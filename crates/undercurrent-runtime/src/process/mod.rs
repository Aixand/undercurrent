// SPDX-License-Identifier: Apache-2.0

//! Bounded native process and pseudo-terminal supervision.

#[cfg(any(unix, windows))]
mod attempt_observer;
#[cfg(any(unix, windows))]
mod attempt_receipts;
mod environment;
#[cfg(any(unix, windows))]
mod host_files;
#[cfg(all(test, target_os = "linux"))]
mod host_receipt_tests;
#[cfg(any(unix, windows))]
mod terminal_exits;
#[cfg(any(unix, windows))]
pub use attempt_observer::HostedOperationAttemptEffectObserver;
#[cfg(any(unix, windows))]
mod execute_host;
#[cfg(any(unix, windows))]
#[doc(hidden)]
pub use execute_host::{
    EXECUTION_SUPERVISOR_ARGUMENT, ExecutionSupervisorOutcome, run_execution_supervisor,
};
#[cfg(any(unix, windows))]
mod session_command;
#[cfg(any(unix, windows))]
pub use session_command::HostedSessionCommandExecutor;
#[cfg(any(unix, windows))]
mod host;
#[cfg(any(unix, windows))]
mod hosted;
#[cfg(target_os = "linux")]
mod linux_observation;
#[cfg(target_os = "macos")]
mod macos_observation;
mod managed;

mod platform;
mod preflight;
mod preparation;
mod reaper;
mod spawn;
#[cfg(all(test, windows))]
mod windows_test_support;

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(target_os = "linux")]
use linux_observation::LinuxProcessObservationCache as ProcessObservationCache;
#[cfg(target_os = "macos")]
use macos_observation::MacosProcessObservationCache as ProcessObservationCache;
use managed::{Abandonment, ExitHandling, ManagedProcess, TerminationMode};
use undercurrent_application::{
    Clock, DiagnosticEventName, DiagnosticLevel, DiagnosticRecord, ExecutionContextResolver,
    ForegroundProcessObserver, MetricName, MetricObservation, OutputAppend, PortError,
    PortErrorKind, PortFuture, PortOperation, ProcessExitObservation, ProcessExitSource,
    ProcessInstance, ProcessRuntime, ProcessTermination, ProcessTruthSource,
    ResolvedExecutionContext, ResolvedSecretEnvironment, SecretResolver, SessionMetricObservation,
    StandardInputClosure, Telemetry, TerminalOutputError, TerminalOutputLimits,
    TerminalOutputStatus, TerminalOutputStore, UnavailableSecretResolver,
    resolve_secret_environment,
};
use undercurrent_domain::{
    InputDeliveryPolicy, InputSequence, LaunchSpec, ProcessIdentity, ProcessInstanceId, SessionId,
    StopPolicy, TerminalSize, TimestampMillis,
};

use crate::{
    CooperativeSignalPressure, CooperativeSignalStore, EncodedTerminalInput, TerminalScreenLimits,
    TerminalScreenPressure, TerminalScreenStoreError, VtTerminalScreenStore,
};
use crate::{RuntimeAdapterFailure, SystemClock};
use preparation::PreparationPool;
use reaper::ReaperHandle;
use spawn::SpawnPool;

const MINIMUM_WORKER_STACK_BYTES: usize = 64 * 1024;

#[cfg(any(unix, windows))]
pub use host::{
    ProcessHostBootstrap, ProcessHostError, capture_shell_environment, run_process_host,
};
#[cfg(any(unix, windows))]
pub use hosted::{
    HostedAgentPeer, HostedAgentPeerError, HostedProcessRuntime, HostedProcessRuntimeConfig,
    HostedProcessRuntimeError,
};

trait ProcessOutputObserver: Send + Sync {
    fn observe(
        &self,
        session_id: &SessionId,
        process_id: &ProcessInstanceId,
        append: OutputAppend,
        bytes: &[u8],
    );

    fn observe_owned(
        &self,
        session_id: &SessionId,
        process_id: &ProcessInstanceId,
        append: OutputAppend,
        bytes: Vec<u8>,
    ) {
        self.observe(session_id, process_id, append, &bytes);
    }

    fn observe_cooperative(
        &self,
        _session_id: &SessionId,
        _process_id: &ProcessInstanceId,
        _state: undercurrent_domain::CooperativeForegroundState,
        _observed_at: TimestampMillis,
    ) {
    }
}

struct IgnoreProcessOutput;

impl ProcessOutputObserver for IgnoreProcessOutput {
    fn observe(
        &self,
        _session_id: &SessionId,
        _process_id: &ProcessInstanceId,
        _append: OutputAppend,
        _bytes: &[u8],
    ) {
    }
}

/// Explicit hard bounds for one native process runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeProcessRuntimeLimits {
    /// Maximum occupied process slots, including unconsumed exit events.
    pub max_processes: usize,
    /// Dedicated workers in each fixed launch-preparation and spawn pool.
    pub spawn_workers: usize,
    /// Requests admitted but not yet executing in each preparation/spawn pool.
    pub spawn_queue_capacity: usize,
    /// Atomic terminal-input sequences queued for one process.
    pub input_queue_sequences: usize,
    /// Encoded terminal-input bytes queued or being written for one process.
    pub input_queue_bytes_per_process: usize,
    /// Bytes read from one PTY in one bounded append.
    pub output_read_bytes: usize,
    /// Maximum ANSI bytes returned by one rendered current screen.
    pub screen_snapshot_bytes: usize,
    /// Maximum bytes read from one environment file.
    pub environment_file_bytes: usize,
    /// Maximum merged environment-file and explicit variable count.
    pub environment_entries: usize,
    /// Maximum merged environment key and value bytes.
    pub environment_bytes: usize,
    /// Stack reserved for each blocking adapter worker thread.
    pub worker_thread_stack_bytes: usize,
}

impl NativeProcessRuntimeLimits {
    /// Validates every hard bound and their worker/process relationship.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed configuration error for the first invalid bound.
    pub fn validate(self) -> Result<Self, NativeProcessRuntimeConfigError> {
        for (field, value) in [
            ("max_processes", self.max_processes),
            ("spawn_workers", self.spawn_workers),
            ("spawn_queue_capacity", self.spawn_queue_capacity),
            ("input_queue_sequences", self.input_queue_sequences),
            (
                "input_queue_bytes_per_process",
                self.input_queue_bytes_per_process,
            ),
            ("output_read_bytes", self.output_read_bytes),
            ("screen_snapshot_bytes", self.screen_snapshot_bytes),
            ("environment_file_bytes", self.environment_file_bytes),
            ("environment_entries", self.environment_entries),
            ("environment_bytes", self.environment_bytes),
            ("worker_thread_stack_bytes", self.worker_thread_stack_bytes),
        ] {
            if value == 0 {
                return Err(NativeProcessRuntimeConfigError::ZeroLimit { field });
            }
        }
        if self.spawn_workers > self.max_processes {
            return Err(NativeProcessRuntimeConfigError::InvalidRelationship {
                rule: "spawn worker count must not exceed process capacity",
            });
        }
        if self.worker_thread_stack_bytes < MINIMUM_WORKER_STACK_BYTES {
            return Err(NativeProcessRuntimeConfigError::InvalidRelationship {
                rule: "worker thread stack must be at least 64 KiB",
            });
        }
        Ok(self)
    }
}

/// Complete native process runtime configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeProcessRuntimeConfig {
    shell_capture_socket: Option<std::path::PathBuf>,
    epoch: String,
    limits: NativeProcessRuntimeLimits,
    output_limits: TerminalOutputLimits,
    scrollback_lines: usize,
    force_confirmation_timeout: Duration,
}

impl NativeProcessRuntimeConfig {
    /// Constructs configuration with an explicit daemon epoch and all hard
    /// process, queue, environment, output, and termination bounds.
    ///
    /// Construction performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed invalid-limit, epoch, output, or relationship error.
    pub fn new(
        epoch: impl Into<String>,
        limits: NativeProcessRuntimeLimits,
        output_limits: TerminalOutputLimits,
        force_confirmation_timeout: Duration,
    ) -> Result<Self, NativeProcessRuntimeConfigError> {
        let limits = limits.validate()?;
        let output_limits = output_limits
            .validate()
            .map_err(NativeProcessRuntimeConfigError::OutputLimits)?;
        if output_limits.sessions < limits.max_processes {
            return Err(NativeProcessRuntimeConfigError::InvalidRelationship {
                rule: "terminal output session capacity must cover process capacity",
            });
        }
        if limits.output_read_bytes > output_limits.append_bytes {
            return Err(NativeProcessRuntimeConfigError::InvalidRelationship {
                rule: "one PTY read must fit in one terminal output append",
            });
        }
        if force_confirmation_timeout.is_zero() {
            return Err(NativeProcessRuntimeConfigError::ZeroForceConfirmationTimeout);
        }
        let epoch = epoch.into();
        if epoch.is_empty() {
            return Err(NativeProcessRuntimeConfigError::EmptyEpoch);
        }
        ProcessInstanceId::new(format!("{epoch}-{}-{}", u32::MAX, u64::MAX))
            .map_err(NativeProcessRuntimeConfigError::InvalidEpoch)?;
        Ok(Self {
            shell_capture_socket: None,
            epoch,
            limits,
            output_limits,
            scrollback_lines: 0,
            force_confirmation_timeout,
        })
    }

    /// Returns the stable daemon/process allocation epoch.
    pub fn epoch(&self) -> &str {
        &self.epoch
    }

    /// Returns validated process runtime limits.
    pub const fn limits(&self) -> NativeProcessRuntimeLimits {
        self.limits
    }

    /// Returns validated terminal output retention limits.
    pub const fn output_limits(&self) -> TerminalOutputLimits {
        self.output_limits
    }

    /// Sets the per-session normal-screen history bound; zero disables history.
    pub fn with_scrollback_lines(mut self, lines: usize) -> Self {
        self.scrollback_lines = lines;
        self
    }

    /// Returns the deadline for confirming forced process termination.
    pub const fn force_confirmation_timeout(&self) -> Duration {
        self.force_confirmation_timeout
    }
}

/// Invalid native process runtime configuration.
#[derive(Debug)]
pub enum NativeProcessRuntimeConfigError {
    /// Daemon epoch was empty.
    EmptyEpoch,
    /// A required hard limit was zero.
    ZeroLimit {
        /// Stable invalid field name.
        field: &'static str,
    },
    /// Configured bounds were individually valid but inconsistent together.
    InvalidRelationship {
        /// Stable violated relationship.
        rule: &'static str,
    },
    /// Daemon epoch could not form a valid process-incarnation identity.
    InvalidEpoch(undercurrent_domain::DomainError),
    /// Terminal output retention limits were invalid.
    OutputLimits(TerminalOutputError),
    /// Forced termination had no confirmation deadline.
    ZeroForceConfirmationTimeout,
}

impl Display for NativeProcessRuntimeConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyEpoch => {
                formatter.write_str("native process runtime epoch must not be empty")
            }
            Self::ZeroLimit { field } => {
                write!(
                    formatter,
                    "native process runtime limit {field} must be non-zero"
                )
            }
            Self::InvalidRelationship { rule } => {
                write!(
                    formatter,
                    "native process runtime limits are inconsistent: {rule}"
                )
            }
            Self::InvalidEpoch(_) => formatter.write_str("native process runtime epoch is invalid"),
            Self::OutputLimits(_) => {
                formatter.write_str("native process terminal output limits are invalid")
            }
            Self::ZeroForceConfirmationTimeout => {
                formatter.write_str("native process force confirmation timeout must be non-zero")
            }
        }
    }
}

impl Error for NativeProcessRuntimeConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidEpoch(error) => Some(error),
            Self::OutputLimits(error) => Some(error),
            Self::EmptyEpoch
            | Self::ZeroLimit { .. }
            | Self::InvalidRelationship { .. }
            | Self::ZeroForceConfirmationTimeout => None,
        }
    }
}

/// Failure while starting native runtime workers.
#[derive(Debug)]
pub enum NativeProcessRuntimeOpenError {
    /// Terminal output store construction failed after configuration validation.
    OutputStore(TerminalOutputError),
    /// Terminal screen store construction failed after configuration validation.
    ScreenStore(TerminalScreenStoreError),
    /// Cooperative signal store construction failed after configuration validation.
    CooperativeSignals(crate::RuntimeLimitError),
    /// Process reaper initialization failed.
    Reaper(reaper::ReaperStartError),
    /// Spawn worker creation failed.
    SpawnWorkers(std::io::Error),
    /// Launch-preparation worker creation failed.
    PreparationWorkers(std::io::Error),
}

impl Display for NativeProcessRuntimeOpenError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutputStore(_) => formatter.write_str("could not create terminal output store"),
            Self::ScreenStore(_) => formatter.write_str("could not create terminal screen store"),
            Self::CooperativeSignals(_) => {
                formatter.write_str("could not create cooperative signal store")
            }
            Self::Reaper(_) => formatter.write_str("could not start native process reaper"),
            Self::SpawnWorkers(_) => {
                formatter.write_str("could not start native process spawn workers")
            }
            Self::PreparationWorkers(_) => {
                formatter.write_str("could not start native launch-preparation workers")
            }
        }
    }
}

impl Error for NativeProcessRuntimeOpenError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::OutputStore(error) => Some(error),
            Self::ScreenStore(error) => Some(error),
            Self::CooperativeSignals(error) => Some(error),
            Self::Reaper(error) => Some(error),
            Self::SpawnWorkers(error) => Some(error),
            Self::PreparationWorkers(error) => Some(error),
        }
    }
}

/// Payload-safe instantaneous native process runtime pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeProcessRuntimePressure {
    /// Whether new process spawns have been closed for daemon shutdown.
    pub closed: bool,
    /// Slots occupied by reservations, live processes, or queued exits.
    pub occupied_process_slots: usize,
    /// Concrete process entries still retained by the runtime.
    pub active_process_entries: usize,
    /// Published exact exits retained until application consumption.
    pub published_process_exits: usize,
    /// Process entries whose exit publication has not been armed.
    pub unarmed_processes: usize,
    /// Unarmed processes whose child already exited.
    pub exited_unarmed_processes: usize,
    /// Lazily started per-process atomic input workers.
    pub input_workers: usize,
    /// Encoded bytes queued or currently being written across processes.
    pub queued_input_bytes: usize,
    /// Spawn requests currently waiting for a worker.
    pub queued_spawns: usize,
    /// Spawn request queue capacity.
    pub spawn_queue_capacity: usize,
    /// Launch preparations currently waiting for a blocking worker.
    pub queued_preparations: usize,
    /// Launch-preparation request queue capacity.
    pub preparation_queue_capacity: usize,
    /// Armed exit observations awaiting application settlement.
    pub queued_exits: usize,
    /// Retained terminal output status.
    pub output: TerminalOutputStatus,
    /// Bounded terminal screen state.
    pub screens: TerminalScreenPressure,
    /// Bounded cooperative signal state and parser outcomes.
    pub cooperative_signals: CooperativeSignalPressure,
}

/// Native process/PTY adapter implementing exact process operations and exits.
#[derive(Clone)]
pub struct NativeProcessRuntime {
    owner: Arc<RuntimeOwner>,
}

struct RuntimeOwner {
    preparation_pool: PreparationPool,
    spawn_pool: SpawnPool,
    _reaper: ReaperHandle,
    core: Arc<RuntimeCore>,
}

struct RuntimeCore {
    config: NativeProcessRuntimeConfig,
    clock: Arc<dyn Clock>,
    telemetry: Arc<dyn Telemetry>,
    secret_resolver: Arc<dyn SecretResolver>,
    output: Arc<TerminalOutputStore>,
    screens: Arc<VtTerminalScreenStore>,
    cooperative_signals: Arc<CooperativeSignalStore>,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    sealed_executables: preflight::SealedExecutableCache,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pinned_directories: preflight::PinnedDirectoryCache,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    process_observations: ProcessObservationCache,
    output_observer: Arc<dyn ProcessOutputObserver>,
    exits: async_channel::Sender<ProcessExitObservation>,
    exit_receiver: async_channel::Receiver<ProcessExitObservation>,
    state: Mutex<RuntimeState>,
    next_process_sequence: AtomicU64,
    closed: AtomicBool,
    exit_confirmation_failed: AtomicBool,
}

#[derive(Default)]
struct RuntimeState {
    occupied_slots: usize,
    reserved_sessions: BTreeSet<SessionId>,
    processes: BTreeMap<SessionId, Arc<ManagedProcess>>,
    published_exits: BTreeMap<ProcessInstanceId, SessionId>,
}

#[derive(Debug, Default, Clone, Copy)]
struct ExitStateEffects {
    publication_conflict: bool,
    slot_underflow: bool,
}

impl NativeProcessRuntime {
    /// Starts the bounded spawn pool, event-driven process reaper, and output
    /// store around explicit clock and telemetry ports.
    ///
    /// This function starts worker threads but no child processes. It does not
    /// panic. Dropping the final runtime handle closes workers and forcibly
    /// terminates children still owned by this adapter.
    ///
    /// # Errors
    ///
    /// Returns a typed output-store, signal, or worker-thread startup failure.
    pub fn start(
        config: NativeProcessRuntimeConfig,
        clock: Arc<dyn Clock>,
        telemetry: Arc<dyn Telemetry>,
    ) -> Result<Self, NativeProcessRuntimeOpenError> {
        Self::start_inner(
            config,
            clock,
            telemetry,
            Arc::new(UnavailableSecretResolver),
            Arc::new(IgnoreProcessOutput),
        )
    }

    /// Starts the native runtime with an authorized ephemeral credential adapter.
    pub fn start_with_secret_resolver(
        config: NativeProcessRuntimeConfig,
        clock: Arc<dyn Clock>,
        telemetry: Arc<dyn Telemetry>,
        secret_resolver: Arc<dyn SecretResolver>,
    ) -> Result<Self, NativeProcessRuntimeOpenError> {
        Self::start_inner(
            config,
            clock,
            telemetry,
            secret_resolver,
            Arc::new(IgnoreProcessOutput),
        )
    }

    fn start_with_output_observer(
        config: NativeProcessRuntimeConfig,
        clock: Arc<dyn Clock>,
        telemetry: Arc<dyn Telemetry>,
        output_observer: Arc<dyn ProcessOutputObserver>,
    ) -> Result<Self, NativeProcessRuntimeOpenError> {
        Self::start_inner(
            config,
            clock,
            telemetry,
            Arc::new(UnavailableSecretResolver),
            output_observer,
        )
    }

    fn start_inner(
        config: NativeProcessRuntimeConfig,
        clock: Arc<dyn Clock>,
        telemetry: Arc<dyn Telemetry>,
        secret_resolver: Arc<dyn SecretResolver>,
        output_observer: Arc<dyn ProcessOutputObserver>,
    ) -> Result<Self, NativeProcessRuntimeOpenError> {
        let output = Arc::new(
            TerminalOutputStore::new(config.output_limits())
                .map_err(NativeProcessRuntimeOpenError::OutputStore)?,
        );
        let screens = Arc::new(
            VtTerminalScreenStore::new(TerminalScreenLimits {
                sessions: config.output_limits().sessions,
                snapshot_bytes: config.limits().screen_snapshot_bytes,
                scrollback_lines: config.scrollback_lines,
            })
            .map_err(NativeProcessRuntimeOpenError::ScreenStore)?,
        );
        let (exits, exit_receiver) = async_channel::bounded(config.limits().max_processes);
        let cooperative_signals = Arc::new(
            CooperativeSignalStore::new(config.limits().max_processes)
                .map_err(NativeProcessRuntimeOpenError::CooperativeSignals)?,
        );
        let core = Arc::new(RuntimeCore {
            config: config.clone(),
            clock,
            telemetry,
            secret_resolver,
            output,
            screens,
            cooperative_signals,
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            sealed_executables: preflight::SealedExecutableCache::new(),
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            pinned_directories: preflight::PinnedDirectoryCache::new(),
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            process_observations: ProcessObservationCache::new(),
            output_observer,
            exits,
            exit_receiver,
            state: Mutex::new(RuntimeState::default()),
            next_process_sequence: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            exit_confirmation_failed: AtomicBool::new(false),
        });
        let reaper = ReaperHandle::start(
            Arc::downgrade(&core),
            config.limits().max_processes,
            config.limits().worker_thread_stack_bytes,
        )
        .map_err(NativeProcessRuntimeOpenError::Reaper)?;
        let preparation_pool = PreparationPool::start(config.limits())
            .map_err(NativeProcessRuntimeOpenError::PreparationWorkers)?;
        let spawn_pool = SpawnPool::start(Arc::clone(&core), reaper.sender(), config.limits())
            .map_err(NativeProcessRuntimeOpenError::SpawnWorkers)?;
        Ok(Self {
            owner: Arc::new(RuntimeOwner {
                preparation_pool,
                spawn_pool,
                _reaper: reaper,
                core,
            }),
        })
    }

    /// Starts a runtime with the system clock and caller-supplied telemetry.
    ///
    /// This convenience performs the same thread and store initialization as
    /// [`Self::start`] and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed runtime startup failure.
    pub fn start_with_system_clock(
        config: NativeProcessRuntimeConfig,
        telemetry: Arc<dyn Telemetry>,
    ) -> Result<Self, NativeProcessRuntimeOpenError> {
        Self::start(config, Arc::new(SystemClock), telemetry)
    }

    /// Returns the bounded terminal output retention shared with stream and
    /// replay adapters.
    pub fn output_store(&self) -> Arc<TerminalOutputStore> {
        Arc::clone(&self.owner.core.output)
    }

    pub(super) fn spawn_with_resolved_secret_environment<'a>(
        &'a self,
        session_id: &'a SessionId,
        launch: &'a LaunchSpec,
        secret_environment: ResolvedSecretEnvironment,
    ) -> PortFuture<'a, Result<ProcessInstance, PortError>> {
        let session_id = session_id.clone();
        let launch = launch.clone();
        Box::pin(async move {
            self.owner.core.reserve_spawn(&session_id)?;
            match self
                .owner
                .spawn_pool
                .submit(session_id.clone(), launch, secret_environment)
            {
                Ok(reply) => match reply.recv().await {
                    Ok(spawn::SpawnReply::Spawned {
                        process,
                        acceptance,
                    }) => {
                        acceptance.accept();
                        Ok(process)
                    }
                    Ok(spawn::SpawnReply::Failed(error)) => Err(error),
                    Err(error) => {
                        self.owner.core.release_reservation(&session_id);
                        Err(PortError::new(
                            PortErrorKind::Unavailable,
                            PortOperation::SpawnProcess,
                            Some(session_id),
                            error,
                        ))
                    }
                },
                Err(error) => {
                    self.owner.core.release_reservation(&session_id);
                    Err(error)
                }
            }
        })
    }

    #[cfg(any(unix, windows))]
    pub(super) fn hosted_process_identity(
        &self,
        session_id: &SessionId,
        process_id: &ProcessInstanceId,
    ) -> Result<(u32, Option<u64>), PortError> {
        self.owner
            .core
            .resolve_process(session_id, process_id, PortOperation::InspectProcessRuntime)
            .map(|process| (process.root_pid(), process.root_start_ticks()))
    }

    /// Returns the bounded parser-consistent screen reader shared with product adapters.
    pub fn screen_store(&self) -> Arc<VtTerminalScreenStore> {
        Arc::clone(&self.owner.core.screens)
    }

    /// Returns the bounded exact-process cooperative signal store shared with
    /// foreground-evidence collection.
    pub fn cooperative_signal_store(&self) -> Arc<CooperativeSignalStore> {
        Arc::clone(&self.owner.core.cooperative_signals)
    }

    /// Returns payload-safe runtime pressure without process, filesystem, or
    /// PTY IO.
    ///
    /// # Errors
    ///
    /// Returns a typed output-store lock or invariant failure.
    pub fn pressure(&self) -> Result<NativeProcessRuntimePressure, PortError> {
        let state = self
            .owner
            .core
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut unarmed_processes = 0;
        let mut exited_unarmed_processes = 0;
        let mut input_workers = 0;
        let mut queued_input_bytes = 0_usize;
        for process in state.processes.values() {
            let status = process.status();
            unarmed_processes += usize::from(!status.exit_armed);
            exited_unarmed_processes += usize::from(!status.exit_armed && status.exited);
            input_workers += usize::from(status.input_worker_active);
            queued_input_bytes = queued_input_bytes.saturating_add(status.queued_input_bytes);
        }
        let output = self.owner.core.output.status().map_err(|error| {
            PortError::new(
                PortErrorKind::Other,
                PortOperation::InspectProcessRuntime,
                None,
                error,
            )
        })?;
        Ok(NativeProcessRuntimePressure {
            closed: self.owner.core.closed.load(Ordering::Acquire),
            occupied_process_slots: state.occupied_slots,
            active_process_entries: state.processes.len(),
            published_process_exits: state.published_exits.len(),
            unarmed_processes,
            exited_unarmed_processes,
            input_workers,
            queued_input_bytes,
            queued_spawns: self.owner.spawn_pool.queued(),
            spawn_queue_capacity: self.owner.core.config.limits().spawn_queue_capacity,
            queued_preparations: self.owner.preparation_pool.queued(),
            preparation_queue_capacity: self.owner.preparation_pool.queue_capacity(),
            queued_exits: self.owner.core.exit_receiver.len(),
            output,
            screens: self.owner.core.screens.pressure(),
            cooperative_signals: self.owner.core.cooperative_signals.pressure(),
        })
    }

    /// Stops new process admission and wakes the exact-exit consumer.
    ///
    /// Existing process ownership is retained until the final runtime handle is
    /// dropped, at which point the runtime force-terminates owned children. A
    /// composition root calls this only after closing request admission, then
    /// joins its exit dispatcher before dropping runtime handles. Repeated calls
    /// are idempotent. This function performs no process or filesystem IO and
    /// does not panic.
    pub fn begin_shutdown(&self) {
        let _state = self
            .owner
            .core
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !self.owner.core.closed.swap(true, Ordering::AcqRel) {
            self.owner.preparation_pool.close();
            self.owner.core.exits.close();
        }
    }

    async fn write_encoded_input(
        &self,
        session_id: &SessionId,
        process: &ProcessInstanceId,
        delivery: EncodedTerminalInput,
    ) -> Result<(), PortError> {
        self.write_encoded_input_attempt(session_id, process, delivery, None)
            .await
    }

    async fn write_encoded_input_attempt(
        &self,
        session_id: &SessionId,
        process: &ProcessInstanceId,
        delivery: EncodedTerminalInput,
        attempt: Option<&undercurrent_domain::OperationAttemptId>,
    ) -> Result<(), PortError> {
        let entry = self
            .owner
            .core
            .resolve_process(session_id, process, PortOperation::WriteInput)
            .map_err(|error| match attempt {
                Some(id) => error.with_attempt_effect(
                    id.clone(),
                    undercurrent_domain::EffectCertainty::KnownAbsent,
                ),
                None => error,
            })?;
        entry.write_encoded_input_attempt(delivery, attempt).await
    }

    fn retire_host_process(&self, session_id: &SessionId, process: &ProcessInstanceId) -> bool {
        self.owner.core.retire_exact(session_id, process)
    }

    fn cancel_host_spawn(&self, session_id: &SessionId, process: &ProcessInstanceId) {
        self.owner.core.abandon_process(session_id, process);
    }

    fn next_exit_blocking(&self) -> Result<ProcessExitObservation, PortError> {
        let observation = self
            .owner
            .core
            .exit_receiver
            .recv_blocking()
            .map_err(|error| {
                PortError::new(
                    PortErrorKind::Unavailable,
                    PortOperation::ReadProcessExit,
                    None,
                    error,
                )
            })?;
        self.owner.core.settle_published_exit(&observation);
        Ok(observation)
    }
}

impl ProcessRuntime for NativeProcessRuntime {
    fn prepare_launch<'a>(
        &'a self,
        launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<LaunchSpec, PortError>> {
        let launch = launch.clone();
        Box::pin(async move {
            if self.owner.core.closed.load(Ordering::Acquire) {
                return Err(PortError::new(
                    PortErrorKind::Unavailable,
                    PortOperation::PrepareLaunch,
                    None,
                    RuntimeAdapterFailure("native process runtime is shutting down"),
                ));
            }
            let reply = self
                .owner
                .preparation_pool
                .submit(launch, self.owner.core.config.limits())?;
            reply.recv().await.map_err(|error| {
                PortError::new(
                    PortErrorKind::Unavailable,
                    PortOperation::PrepareLaunch,
                    None,
                    error,
                )
            })?
        })
    }

    fn spawn<'a>(
        &'a self,
        session_id: &'a SessionId,
        launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<ProcessInstance, PortError>> {
        Box::pin(async move {
            let limits = self.owner.core.config.limits();
            let secret_environment = resolve_secret_environment(
                self.owner.core.secret_resolver.as_ref(),
                launch.secret_environment(),
                session_id,
                limits
                    .environment_entries
                    .saturating_sub(launch.environment().len()),
                limits
                    .environment_bytes
                    .saturating_sub(launch.environment().total_bytes()),
            )
            .await?;
            self.spawn_with_resolved_secret_environment(session_id, launch, secret_environment)
                .await
        })
    }

    fn arm_exit<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move { self.owner.core.arm_exit(session_id, process) })
    }

    fn stop<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        policy: StopPolicy,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        Box::pin(async move {
            self.owner
                .core
                .terminate(session_id, process, TerminationMode::Graceful(policy))
                .await
        })
    }

    fn force_stop<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        Box::pin(async move {
            self.owner
                .core
                .terminate(session_id, process, TerminationMode::Forced)
                .await
        })
    }

    fn write_input<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        input: &'a InputSequence,
        policy: InputDeliveryPolicy,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            let entry =
                self.owner
                    .core
                    .resolve_process(session_id, process, PortOperation::WriteInput)?;
            entry.write_input(input, policy).await
        })
    }

    fn close_input<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<StandardInputClosure, PortError>> {
        Box::pin(async move {
            let entry = match self.owner.core.resolve_process(
                session_id,
                process,
                PortOperation::CloseInput,
            ) {
                Ok(entry) => entry,
                Err(error) => {
                    // EOF can let the child exit before a close retry arrives.
                    // Its exact pending exit still owns the closed input stream.
                    if error.kind() == PortErrorKind::NotFound {
                        let state = self
                            .owner
                            .core
                            .state
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        if state.published_exits.get(process) == Some(session_id) {
                            return Ok(StandardInputClosure::AlreadyClosed);
                        }
                    }
                    return Err(error);
                }
            };
            entry.close_input()
        })
    }

    fn resize<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        size: TerminalSize,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            let entry = self.owner.core.resolve_process(
                session_id,
                process,
                PortOperation::ResizeTerminal,
            )?;
            entry.resize(size)?;
            self.owner.core.screens.resize(session_id, size)
        })
    }
}

impl ExecutionContextResolver for NativeProcessRuntime {
    fn resolve<'a>(
        &'a self,
        launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<ResolvedExecutionContext, PortError>> {
        Box::pin(async move {
            let environment =
                environment::load_environment(launch, self.owner.core.config.limits()).map_err(
                    |source| {
                        PortError::new(
                            PortErrorKind::InvalidData,
                            PortOperation::ResolveExecutionContext,
                            None,
                            source,
                        )
                    },
                )?;
            let working_directory = environment::working_directory(launch)
                .map_err(|source| {
                    PortError::new(
                        PortErrorKind::InvalidData,
                        PortOperation::ResolveExecutionContext,
                        None,
                        source,
                    )
                })?
                .map(|path| path.to_string_lossy().into_owned());
            Ok(ResolvedExecutionContext::new(
                environment,
                working_directory,
            ))
        })
    }
}

impl ProcessExitSource for NativeProcessRuntime {
    fn next_exit(&self) -> PortFuture<'_, Result<ProcessExitObservation, PortError>> {
        Box::pin(async move {
            let observation = self
                .owner
                .core
                .exit_receiver
                .recv()
                .await
                .map_err(|error| {
                    PortError::new(
                        PortErrorKind::Unavailable,
                        PortOperation::ReadProcessExit,
                        None,
                        error,
                    )
                })?;
            self.owner.core.settle_published_exit(&observation);
            Ok(observation)
        })
    }
}

impl ProcessTruthSource for NativeProcessRuntime {
    fn observe<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessIdentity,
    ) -> PortFuture<'a, Result<undercurrent_domain::ProcessTruth, PortError>> {
        Box::pin(async move {
            let observed_at = self.owner.core.clock.now();
            let entry = self
                .owner
                .core
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .processes
                .get(session_id)
                .filter(|entry| entry.instance().matches(process))
                .cloned();
            Ok(match entry {
                Some(entry) => entry.observed_state(observed_at),
                None => undercurrent_domain::ProcessTruth::Missing { observed_at },
            })
        })
    }
}

impl ForegroundProcessObserver for NativeProcessRuntime {
    fn observe_foreground<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<
        'a,
        Result<undercurrent_application::ForegroundProcessGroupObservation, PortError>,
    > {
        Box::pin(async move {
            let entry = self
                .owner
                .core
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .processes
                .get(session_id)
                .filter(|entry| entry.instance().id == *process)
                .cloned()
                .ok_or_else(|| {
                    PortError::new(
                        PortErrorKind::NotFound,
                        PortOperation::ObserveForegroundProcessGroup,
                        Some(session_id.clone()),
                        RuntimeAdapterFailure("native foreground process is unavailable"),
                    )
                })?;
            let cooperative = self
                .owner
                .core
                .cooperative_signals
                .latest_exact(session_id, process)
                .map(|signal| signal.foreground());
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            {
                self.owner.core.process_observations.observe(
                    &entry,
                    process,
                    self.owner.core.clock.now(),
                    cooperative,
                )
            }
            #[cfg(windows)]
            {
                let root = undercurrent_platform_windows::ProcessHandle::open(entry.root_pid())
                    .ok()
                    .filter(|root| {
                        Some(root.identity().created) == entry.root_start_ticks()
                            && root.is_alive().unwrap_or(false)
                    });
                let unavailable = || {
                    PortError::new(
                        PortErrorKind::NotFound,
                        PortOperation::ObserveForegroundProcessGroup,
                        Some(session_id.clone()),
                        RuntimeAdapterFailure("native foreground process is unavailable"),
                    )
                };
                let root = root.ok_or_else(unavailable)?;
                let listener = match root.tcp_listener() {
                    Ok(undercurrent_platform_windows::TcpListenerObservation::Present) => {
                        undercurrent_application::TcpListenerObservation::Present
                    }
                    Ok(undercurrent_platform_windows::TcpListenerObservation::Absent) => {
                        undercurrent_application::TcpListenerObservation::Absent
                    }
                    Ok(undercurrent_platform_windows::TcpListenerObservation::Truncated) => {
                        undercurrent_application::TcpListenerObservation::Truncated
                    }
                    Err(_) => undercurrent_application::TcpListenerObservation::Unavailable,
                };
                let tree_listener =
                    if listener == undercurrent_application::TcpListenerObservation::Present {
                        undercurrent_application::TcpListenerObservation::Unavailable
                    } else {
                        match entry
                            .job_observer()
                            .and_then(|job| job.map(|job| job.tcp_listener(&root)).transpose())
                        {
                            Ok(Some(
                                undercurrent_platform_windows::TcpListenerObservation::Present,
                            )) => undercurrent_application::TcpListenerObservation::Present,
                            Ok(Some(
                                undercurrent_platform_windows::TcpListenerObservation::Absent,
                            )) => undercurrent_application::TcpListenerObservation::Absent,
                            Ok(Some(
                                undercurrent_platform_windows::TcpListenerObservation::Truncated,
                            )) => undercurrent_application::TcpListenerObservation::Truncated,
                            _ => undercurrent_application::TcpListenerObservation::Unavailable,
                        }
                    };
                if !root.is_alive().unwrap_or(false) {
                    return Err(unavailable());
                }
                // ConPTY has no POSIX foreground group. Preserve cooperative
                // shell evidence without inventing group or listener ownership.
                Ok(
                    undercurrent_application::ForegroundProcessGroupObservation::new(
                        process.clone(),
                        self.owner.core.clock.now(),
                        Vec::new(),
                        undercurrent_application::ProcessGroupCompleteness::Unavailable,
                        None,
                        undercurrent_application::TcpListenerObservation::Unavailable,
                        cooperative,
                    )
                    .with_managed_process_tcp_listener(listener)
                    .with_managed_tree_tcp_listener(tree_listener),
                )
            }
            #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
            {
                let _ = (entry, cooperative);
                Err(PortError::new(
                    PortErrorKind::Unsupported,
                    PortOperation::ObserveForegroundProcessGroup,
                    Some(session_id.clone()),
                    RuntimeAdapterFailure("foreground process observation is unsupported"),
                ))
            }
        })
    }
}

impl RuntimeCore {
    fn reserve_spawn(&self, session_id: &SessionId) -> Result<(), PortError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.closed.load(Ordering::Acquire) {
            return Err(PortError::new(
                PortErrorKind::Unavailable,
                PortOperation::SpawnProcess,
                Some(session_id.clone()),
                RuntimeAdapterFailure("native process runtime is shutting down"),
            ));
        }
        if state.reserved_sessions.contains(session_id) || state.processes.contains_key(session_id)
        {
            return Err(PortError::new(
                PortErrorKind::Conflict,
                PortOperation::SpawnProcess,
                Some(session_id.clone()),
                RuntimeAdapterFailure("session already owns or is reserving a native process"),
            ));
        }
        if state.occupied_slots >= self.config.limits().max_processes {
            return Err(PortError::new(
                PortErrorKind::CapacityExceeded,
                PortOperation::SpawnProcess,
                Some(session_id.clone()),
                RuntimeAdapterFailure("native process slot capacity is exhausted"),
            ));
        }
        state.occupied_slots += 1;
        state.reserved_sessions.insert(session_id.clone());
        Ok(())
    }

    fn release_reservation(&self, session_id: &SessionId) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.reserved_sessions.remove(session_id) {
            let underflow = state.occupied_slots == 0;
            state.occupied_slots = state.occupied_slots.saturating_sub(1);
            drop(state);
            if underflow {
                self.emit_invariant_failure(
                    Some(session_id.clone()),
                    PortOperation::InspectProcessRuntime,
                );
            }
        }
    }

    fn install_process(
        &self,
        session_id: &SessionId,
        process: Arc<ManagedProcess>,
    ) -> Result<(), PortError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.processes.contains_key(session_id) {
            return Err(PortError::new(
                PortErrorKind::Conflict,
                PortOperation::SpawnProcess,
                Some(session_id.clone()),
                RuntimeAdapterFailure("native process installation conflicts with an active entry"),
            ));
        }
        if !state.reserved_sessions.remove(session_id) {
            return Err(PortError::new(
                PortErrorKind::InvalidData,
                PortOperation::SpawnProcess,
                Some(session_id.clone()),
                RuntimeAdapterFailure("native process spawn reservation disappeared"),
            ));
        }
        state.processes.insert(session_id.clone(), process);
        Ok(())
    }

    fn next_instance_id(&self, pid: u32) -> Result<ProcessInstanceId, PortError> {
        let sequence = self
            .next_process_sequence
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map(|previous| previous + 1)
            .map_err(|_| {
                PortError::new(
                    PortErrorKind::CapacityExceeded,
                    PortOperation::SpawnProcess,
                    None,
                    RuntimeAdapterFailure("native process identity sequence is exhausted"),
                )
            })?;
        ProcessInstanceId::new(format!("{}-{pid}-{sequence}", self.config.epoch())).map_err(
            |error| {
                PortError::new(
                    PortErrorKind::InvalidData,
                    PortOperation::SpawnProcess,
                    None,
                    error,
                )
            },
        )
    }

    fn resolve_process(
        &self,
        session_id: &SessionId,
        process: &ProcessInstanceId,
        operation: PortOperation,
    ) -> Result<Arc<ManagedProcess>, PortError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = state.processes.get(session_id).ok_or_else(|| {
            PortError::new(
                PortErrorKind::NotFound,
                operation,
                Some(session_id.clone()),
                RuntimeAdapterFailure("session has no native process entry"),
            )
        })?;
        if entry.instance().id != *process {
            return Err(PortError::new(
                PortErrorKind::Conflict,
                operation,
                Some(session_id.clone()),
                RuntimeAdapterFailure("native process incarnation does not match"),
            ));
        }
        Ok(Arc::clone(entry))
    }

    fn arm_exit(
        &self,
        session_id: &SessionId,
        process: &ProcessInstanceId,
    ) -> Result<(), PortError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = {
            if let Some(entry) = state.processes.get(session_id) {
                if entry.instance().id != *process {
                    return Err(PortError::new(
                        PortErrorKind::Conflict,
                        PortOperation::ArmProcessExit,
                        Some(session_id.clone()),
                        RuntimeAdapterFailure("native process incarnation does not match"),
                    ));
                }
                Arc::clone(entry)
            } else if state.published_exits.get(process) == Some(session_id) {
                return Ok(());
            } else {
                return Err(PortError::new(
                    PortErrorKind::NotFound,
                    PortOperation::ArmProcessExit,
                    Some(session_id.clone()),
                    RuntimeAdapterFailure("session has no native process entry"),
                ));
            }
        };
        if entry.arm_exit(&self.exits)? == ExitHandling::Published {
            state.processes.remove(session_id);
            let prior = state
                .published_exits
                .insert(process.clone(), session_id.clone());
            drop(state);
            if prior.is_some_and(|prior| prior != *session_id) {
                self.emit_invariant_failure(
                    Some(session_id.clone()),
                    PortOperation::ReadProcessExit,
                );
            }
        }
        Ok(())
    }

    fn complete_exit(
        &self,
        session_id: &SessionId,
        process: &ProcessInstanceId,
        outcome: undercurrent_domain::ProcessOutcome,
        wait_confirmed: bool,
    ) {
        // Signal exits and failed OS waits both map to SupervisionFailed in the
        // domain. Only the latter prevents proving host retirement, including
        // after the corresponding process entry has been published and removed.
        if !wait_confirmed {
            self.exit_confirmation_failed.store(true, Ordering::Release);
        }
        #[cfg(windows)]
        {
            let entry = self
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .processes
                .get(session_id)
                .filter(|entry| entry.instance().id == *process)
                .cloned();
            if let Some(entry) = entry {
                // ConPTY retains its output pipe until explicitly closed. Do not
                // hold runtime state while closing: the reader must still drain.
                entry.finish_terminal_output();
            }
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(entry) = state.processes.get(session_id).cloned() else {
            return;
        };
        if entry.instance().id != *process {
            return;
        }
        let observed_at = self.clock.now();
        match entry.observe_exit(outcome, observed_at, &self.exits) {
            Ok(handling) => {
                let effects = apply_exit_handling(&mut state, session_id, process, handling);
                drop(state);
                self.emit_exit_effects(session_id, effects);
            }
            Err(error) => {
                drop(state);
                self.emit_port_failure(Some(session_id.clone()), &error);
            }
        }
    }

    fn complete_output(&self, session_id: &SessionId, process: &ProcessInstanceId) {
        self.cooperative_signals.remove_exact(session_id, process);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(entry) = state.processes.get(session_id).cloned() else {
            return;
        };
        if entry.instance().id != *process {
            return;
        }
        match entry.observe_output_complete(&self.exits) {
            Ok(handling) => {
                let effects = apply_exit_handling(&mut state, session_id, process, handling);
                drop(state);
                self.emit_exit_effects(session_id, effects);
            }
            Err(error) => {
                drop(state);
                self.emit_port_failure(Some(session_id.clone()), &error);
            }
        }
    }

    fn retire_exact(&self, session_id: &SessionId, process: &ProcessInstanceId) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let removed = state
            .processes
            .get(session_id)
            .is_some_and(|entry| entry.instance().id == *process)
            .then(|| state.processes.remove(session_id))
            .flatten()
            .is_some();
        let published = state
            .published_exits
            .get(process)
            .is_some_and(|owner| owner == session_id)
            .then(|| state.published_exits.remove(process))
            .flatten()
            .is_some();
        let retired = removed || published;
        let underflow = retired && state.occupied_slots == 0;
        if retired {
            state.occupied_slots = state.occupied_slots.saturating_sub(1);
            self.cooperative_signals.remove_exact(session_id, process);
        }
        drop(state);
        if underflow {
            self.emit_invariant_failure(
                Some(session_id.clone()),
                PortOperation::InspectProcessRuntime,
            );
        }
        retired
    }

    fn retire_unarmed(&self, session_id: &SessionId, process: &ProcessInstanceId) {
        let entry = match self.resolve_process(session_id, process, PortOperation::ForceProcess) {
            Ok(entry) => entry,
            Err(_) => return,
        };
        if !entry.exit_armed()
            && entry.has_exited()
            && entry.abandon() == Abandonment::AlreadyExited
        {
            self.retire_exact(session_id, process);
        }
    }

    fn settle_published_exit(&self, observation: &ProcessExitObservation) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let exact = state.published_exits.remove(&observation.process_id)
            == Some(observation.session_id.clone());
        let underflow = state.occupied_slots == 0;
        state.occupied_slots = state.occupied_slots.saturating_sub(1);
        drop(state);
        if !exact {
            self.emit_invariant_failure(
                Some(observation.session_id.clone()),
                PortOperation::ReadProcessExit,
            );
        }
        if underflow {
            self.emit_invariant_failure(
                Some(observation.session_id.clone()),
                PortOperation::InspectProcessRuntime,
            );
        }
    }

    fn abandon_process(&self, session_id: &SessionId, process: &ProcessInstanceId) {
        self.abandon_process_with_signal(session_id, process, true);
    }

    fn abandon_reaped_process(&self, session_id: &SessionId, process: &ProcessInstanceId) {
        self.abandon_process_with_signal(session_id, process, false);
    }

    fn abandon_process_with_signal(
        &self,
        session_id: &SessionId,
        process: &ProcessInstanceId,
        signal: bool,
    ) {
        let Ok(entry) = self.resolve_process(session_id, process, PortOperation::ForceProcess)
        else {
            return;
        };
        let abandonment = entry.abandon();
        if signal && let Err(error) = entry.force_now() {
            self.emit_port_failure(Some(session_id.clone()), &error);
        }
        if abandonment == Abandonment::AlreadyExited {
            self.retire_exact(session_id, process);
        }
    }

    fn terminal_output_failed(
        &self,
        session_id: &SessionId,
        process: &ProcessInstanceId,
        operation: PortOperation,
        source: impl Error + Send + Sync + 'static,
    ) {
        let error = PortError::new(
            PortErrorKind::Other,
            operation,
            Some(session_id.clone()),
            source,
        );
        self.emit_port_failure(Some(session_id.clone()), &error);
        if let Ok(entry) = self.resolve_process(session_id, process, operation)
            && !entry.has_exited()
            && let Err(force_error) = entry.force_now()
        {
            self.emit_port_failure(Some(session_id.clone()), &force_error);
        }
    }

    fn emit_port_failure(&self, session_id: Option<SessionId>, error: &PortError) {
        self.telemetry.emit_diagnostic(&DiagnosticRecord {
            event: DiagnosticEventName::PortOperationFailed,
            level: DiagnosticLevel::for_port_error(error.kind()),
            timestamp: self.clock.now(),
            session_id,
            application_operation: None,
            application_error: Some(error.category()),
            operation: Some(error.operation()),
            error_kind: Some(error.kind()),
        });
    }

    fn record_output_metrics(
        &self,
        session_id: &SessionId,
        appended_bytes: usize,
        retained_bytes: usize,
    ) {
        self.telemetry
            .record_session_metric(&SessionMetricObservation::new(
                session_id.clone(),
                MetricObservation {
                    name: MetricName::TerminalOutputBytesAppended,
                    value: appended_bytes as f64,
                    operation: None,
                },
            ));
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::TerminalOutputRetainedBytes,
            value: retained_bytes as f64,
            operation: None,
        });
    }

    fn emit_invariant_failure(&self, session_id: Option<SessionId>, operation: PortOperation) {
        self.telemetry.emit_diagnostic(&DiagnosticRecord {
            event: DiagnosticEventName::PortOperationFailed,
            level: DiagnosticLevel::Error,
            timestamp: self.clock.now(),
            session_id,
            application_operation: None,
            application_error: Some(
                undercurrent_application::ApplicationErrorCategory::InvariantViolation,
            ),
            operation: Some(operation),
            error_kind: Some(PortErrorKind::InvalidData),
        });
    }

    fn emit_exit_effects(&self, session_id: &SessionId, effects: ExitStateEffects) {
        if effects.publication_conflict {
            self.emit_invariant_failure(Some(session_id.clone()), PortOperation::ReadProcessExit);
        }
        if effects.slot_underflow {
            self.emit_invariant_failure(
                Some(session_id.clone()),
                PortOperation::InspectProcessRuntime,
            );
        }
    }

    fn force_all_and_confirm(&self, timeout: Duration) -> Result<(), PortError> {
        let processes: Vec<_> = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .processes
            .values()
            .cloned()
            .collect();
        let deadline = std::time::Instant::now() + timeout;
        // Signal every owned process before waiting for any one of them. The
        // confirmation budget is shared, not multiplied by the session count.
        let mut failure = None;
        for process in &processes {
            if let Err(error) = process.force_now() {
                failure.get_or_insert(error);
            }
        }
        for process in processes {
            if let Err(error) = process.confirm_shutdown(deadline) {
                failure.get_or_insert(error);
            }
        }
        if self.exit_confirmation_failed.load(Ordering::Acquire) {
            failure.get_or_insert_with(|| {
                PortError::new(
                    PortErrorKind::Unavailable,
                    PortOperation::ShutdownProcessHost,
                    None,
                    RuntimeAdapterFailure("native child wait did not confirm shutdown"),
                )
            });
        }
        failure.map_or(Ok(()), Err)
    }

    fn force_all(&self) {
        let processes: Vec<_> = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .processes
            .iter()
            .map(|(session_id, process)| (session_id.clone(), Arc::clone(process)))
            .collect();
        for (session_id, process) in processes {
            if let Err(error) = process.force_now() {
                self.emit_port_failure(Some(session_id), &error);
            }
        }
    }

    async fn terminate(
        self: &Arc<Self>,
        session_id: &SessionId,
        process: &ProcessInstanceId,
        mode: TerminationMode,
    ) -> Result<ProcessTermination, PortError> {
        let operation = if matches!(mode, TerminationMode::Forced) {
            PortOperation::ForceProcess
        } else {
            PortOperation::StopProcess
        };
        let entry = self.resolve_process(session_id, process, operation)?;
        let result = entry
            .terminate(
                mode,
                self.config.force_confirmation_timeout(),
                self.config.limits().worker_thread_stack_bytes,
            )
            .await;
        if result
            .as_ref()
            .is_ok_and(|r| r.kind() != undercurrent_application::TerminationKind::GracefulTimedOut)
        {
            self.retire_unarmed(session_id, process);
        }
        result
    }
}

impl Drop for RuntimeOwner {
    fn drop(&mut self) {
        self.preparation_pool.shutdown();
        self.spawn_pool.shutdown();
        self.core.force_all();
    }
}

fn apply_exit_handling(
    state: &mut RuntimeState,
    session_id: &SessionId,
    process: &ProcessInstanceId,
    handling: ExitHandling,
) -> ExitStateEffects {
    match handling {
        ExitHandling::Retained => ExitStateEffects::default(),
        ExitHandling::Published => {
            state.processes.remove(session_id);
            let prior = state
                .published_exits
                .insert(process.clone(), session_id.clone());
            ExitStateEffects {
                publication_conflict: prior.is_some_and(|prior| prior != *session_id),
                slot_underflow: false,
            }
        }
        ExitHandling::Abandoned => {
            state.processes.remove(session_id);
            let slot_underflow = state.occupied_slots == 0;
            state.occupied_slots = state.occupied_slots.saturating_sub(1);
            ExitStateEffects {
                publication_conflict: false,
                slot_underflow,
            }
        }
    }
}
