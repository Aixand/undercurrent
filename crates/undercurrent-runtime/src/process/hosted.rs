// SPDX-License-Identifier: Apache-2.0

//! Daemon-side adapter for the daemon-independent native session host.
//!
//! Reconnection rebuilds bounded local output and screen projections; it does
//! not resend pending mutations. Exact attempt receipts reconcile response loss.
//! Ordinary spawn guards cancel unaccepted processes, while receipted spawns are
//! adopted by the host before their applied receipt is returned.
//!
//! Port futures can block while serializing socket writes. Authentication has an
//! I/O timeout, but ordinary reply waits have no per-request receive deadline.
//! `begin_shutdown` disconnects this adapter without retiring adopted sessions;
//! attempt-aware host retirement occurs only through its settlement/ACK path.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
#[cfg(unix)]
use std::fs;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
#[cfg(windows)]
use undercurrent_platform_windows::BlockingPipeStream as UnixStream;

use undercurrent_application::shutdown_process_host::ProcessHostShutdown;
use undercurrent_application::{
    ApplicationOperation, Clock, DiagnosticEventName, DiagnosticLevel, DiagnosticRecord,
    ExecutionContextResolver, ForegroundObservationWake, ForegroundProcessGroupObservation,
    ForegroundProcessObserver, ForegroundProgramMember, MetricName, MetricObservation,
    ObservedOperationAttemptEffect, OutputOffset, PortError, PortErrorKind, PortFuture,
    PortOperation, ProcessExitObservation, ProcessExitSource, ProcessGroupCompleteness,
    ProcessInstance, ProcessMutationContext, ProcessRuntime, ProcessTermination,
    ProcessTruthSource, ResolvedExecutionContext, SecretResolver, SessionMetricObservation,
    StandardInputClosure, TcpListenerObservation, Telemetry, TerminalOutputError,
    TerminalOutputStore, TerminalScreenSnapshot, UnavailableSecretResolver,
    resolve_secret_environment,
};
use undercurrent_domain::{
    EffectCertainty, EnvironmentName, InputDeliveryPolicy, InputSequence, LaunchSpec,
    OperationAttempt, OutputCaptureMode, PathReference, PayloadDigest, ProcessIdentity,
    ProcessInstanceId, ProcessMutationKind, ProcessOutcome, ProfileMatchTarget, SessionId,
    StopPolicy, TerminalSize, TimestampMillis,
};

use super::preparation::PreparationPool;
use super::{NativeProcessRuntimeConfig, NativeProcessRuntimePressure, ProcessHostBootstrap};
use crate::process_host_protocol::{
    HostAttemptContext, HostAttemptKind, HostAttemptMutation, HostAttemptProbeState, HostCodec,
    HostCommand, HostCooperativeEvent, HostDecodedFrame, HostEffectCertainty, HostExitEvent,
    HostFailure, HostFailureContext, HostGapRecovery, HostInputFrame, HostInputPacing,
    HostLaunchSpec, HostOutputFrame, HostProcessGroupCompleteness, HostProtocolError, HostRequest,
    HostResolvedSecretEnvironment, HostResponseValue, HostTcpListenerObservation,
};
use crate::{
    CooperativeSignalStore, RuntimeAdapterFailure, TerminalScreenLimits, VtTerminalScreenStore,
    encode_terminal_input_delivery,
};

const TOKEN_BYTES: usize = 32;
const MAX_AGENT_PEER_ANCESTORS: usize = 64;
#[cfg(target_os = "linux")]
const MAX_PROC_STAT_BYTES: u64 = 8 * 1024;

/// Validated daemon-side session-host connection and launch settings.
#[derive(Clone)]
pub struct HostedProcessRuntimeConfig {
    native: NativeProcessRuntimeConfig,
    socket: PathBuf,
    journal_base: PathBuf,
    executable: PathBuf,
    connect_timeout: Duration,
    retry_initial: Duration,
    retry_maximum: Duration,
    io_timeout: Duration,
}

impl HostedProcessRuntimeConfig {
    /// Constructs explicit session-host settings around validated native limits.
    ///
    /// Paths must be absolute files. Durations must be non-zero, retry initial
    /// must not exceed retry maximum, and retry maximum must not exceed the
    /// complete connection deadline. This function performs no IO and does not
    /// panic.
    ///
    /// # Errors
    ///
    /// Returns a typed relationship or path validation failure.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        native: NativeProcessRuntimeConfig,
        socket: impl Into<PathBuf>,
        executable: impl Into<PathBuf>,
        connect_timeout: Duration,
        retry_initial: Duration,
        retry_maximum: Duration,
        io_timeout: Duration,
    ) -> Result<Self, HostedProcessRuntimeError> {
        let socket = socket.into();
        let executable = executable.into();
        if !socket.is_absolute() || socket.file_name().is_none() {
            return Err(HostedProcessRuntimeError::InvalidConfiguration(
                "session-host socket must be an absolute file path",
            ));
        }
        if !executable.is_absolute() || executable.file_name().is_none() {
            return Err(HostedProcessRuntimeError::InvalidConfiguration(
                "session-host executable must be an absolute file path",
            ));
        }
        if connect_timeout.is_zero()
            || retry_initial.is_zero()
            || retry_maximum.is_zero()
            || io_timeout.is_zero()
        {
            return Err(HostedProcessRuntimeError::InvalidConfiguration(
                "session-host durations must be non-zero",
            ));
        }
        if retry_initial > retry_maximum || retry_maximum > connect_timeout {
            return Err(HostedProcessRuntimeError::InvalidConfiguration(
                "session-host retry durations exceed the connection deadline",
            ));
        }
        Ok(Self {
            native,
            journal_base: socket.clone(),
            socket,
            executable,
            connect_timeout,
            retry_initial,
            retry_maximum,
            io_timeout,
        })
    }

    /// Separates persistent journals from transient socket storage.
    pub fn with_journal_base(mut self, base: PathBuf) -> Result<Self, HostedProcessRuntimeError> {
        if !base.is_absolute() || base.file_name().is_none() {
            return Err(HostedProcessRuntimeError::InvalidConfiguration(
                "session-host journal base must be an absolute file path",
            ));
        }
        self.journal_base = base;
        Ok(self)
    }

    /// Returns validated native runtime bounds used on both sides of the host boundary.
    pub const fn native(&self) -> &NativeProcessRuntimeConfig {
        &self.native
    }

    /// Returns the private host socket without performing IO.
    pub fn socket(&self) -> &Path {
        &self.socket
    }
}

impl Debug for HostedProcessRuntimeConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HostedProcessRuntimeConfig")
            .field("native", &self.native)
            .field("socket", &"<redacted>")
            .field("executable", &"<redacted>")
            .field("connect_timeout", &self.connect_timeout)
            .field("retry_initial", &self.retry_initial)
            .field("retry_maximum", &self.retry_maximum)
            .field("io_timeout", &self.io_timeout)
            .finish()
    }
}

/// Typed hosted-runtime startup failure without private paths or tokens.
#[derive(Debug)]
pub enum HostedProcessRuntimeError {
    InvalidConfiguration(&'static str),
    OutputStore(undercurrent_application::TerminalOutputError),
    ScreenStore(crate::TerminalScreenStoreError),
    CooperativeSignals(crate::RuntimeLimitError),
    HostClient(HostClientError),
    PreparationWorkers(std::io::Error),
    Worker(std::io::Error),
}

impl Display for HostedProcessRuntimeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidConfiguration(message) => formatter.write_str(message),
            Self::OutputStore(_) => {
                formatter.write_str("hosted terminal output store could not start")
            }
            Self::ScreenStore(_) => {
                formatter.write_str("hosted terminal screen store could not start")
            }
            Self::CooperativeSignals(_) => {
                formatter.write_str("hosted cooperative signal store could not start")
            }
            Self::HostClient(_) => formatter.write_str("session-host client could not start"),
            Self::PreparationWorkers(_) => {
                formatter.write_str("hosted launch-preparation workers could not start")
            }
            Self::Worker(_) => formatter.write_str("hosted runtime worker could not start"),
        }
    }
}

impl Error for HostedProcessRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::OutputStore(source) => Some(source),
            Self::ScreenStore(source) => Some(source),
            Self::CooperativeSignals(source) => Some(source),
            Self::HostClient(source) => Some(source),
            Self::PreparationWorkers(source) => Some(source),
            Self::Worker(source) => Some(source),
            Self::InvalidConfiguration(_) => None,
        }
    }
}

/// Exact hosted workload owning one authenticated local bridge process.
#[derive(Clone, PartialEq, Eq)]
pub struct HostedAgentPeer {
    session_id: SessionId,
    process_id: ProcessInstanceId,
    attempt_id: Option<undercurrent_domain::OperationAttemptId>,
}

impl HostedAgentPeer {
    pub const fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub const fn process_id(&self) -> &ProcessInstanceId {
        &self.process_id
    }

    pub const fn attempt_id(&self) -> Option<&undercurrent_domain::OperationAttemptId> {
        self.attempt_id.as_ref()
    }
}

impl Debug for HostedAgentPeer {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HostedAgentPeer")
            .field("session_id", &self.session_id)
            .field("process_id", &self.process_id)
            .field("attempt_bound", &self.attempt_id.is_some())
            .finish()
    }
}

/// Fail-closed local peer-to-hosted-workload resolution failure.
#[derive(Debug)]
pub enum HostedAgentPeerError {
    UnsupportedPlatform,
    InvalidBridgeExecutable,
    ProcessTableChanged,
    ProcessTable(std::io::Error),
}

impl Display for HostedAgentPeerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedPlatform => {
                formatter.write_str("local agent peer verification is unsupported")
            }
            Self::InvalidBridgeExecutable => {
                formatter.write_str("local agent bridge executable is not trusted")
            }
            Self::ProcessTableChanged => {
                formatter.write_str("local agent peer process identity changed during admission")
            }
            Self::ProcessTable(_) => {
                formatter.write_str("local agent peer process identity could not be read")
            }
        }
    }
}

impl Error for HostedAgentPeerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ProcessTable(source) => Some(source),
            _ => None,
        }
    }
}

/// Daemon-side process adapter whose PTYs remain owned by the independent host.
#[derive(Clone)]
pub struct HostedProcessRuntime {
    owner: Arc<HostedRuntimeOwner>,
}

struct HostedRuntimeOwner {
    core: Arc<HostedRuntimeCore>,
    client: Arc<HostClient>,
    preparation_pool: PreparationPool,
}

struct HostedRuntimeCore {
    config: HostedProcessRuntimeConfig,
    clock: Arc<dyn Clock>,
    telemetry: Arc<dyn Telemetry>,
    secret_resolver: Arc<dyn SecretResolver>,
    output: Arc<TerminalOutputStore>,
    screens: Arc<VtTerminalScreenStore>,
    cooperative_signals: Arc<CooperativeSignalStore>,
    foreground_wake: Option<Arc<dyn ForegroundObservationWake>>,
    exits: async_channel::Sender<ProcessExitObservation>,
    exit_receiver: async_channel::Receiver<ProcessExitObservation>,
    state: Mutex<HostedRuntimeState>,
    closed: AtomicBool,
}

#[derive(Default)]
struct HostedRuntimeState {
    entries: BTreeMap<SessionId, HostedProcessEntry>,
    host_epoch: String,
    dropped_output_frames: u64,
}

struct HostedProcessEntry {
    process: ProcessInstance,
    root_pid: u32,
    root_start_ticks: Option<u64>,
    attempt_id: Option<undercurrent_domain::OperationAttemptId>,
    terminal_size: TerminalSize,
    output_capture_mode: OutputCaptureMode,
    remote_next: Option<u64>,
    pending_output: BTreeMap<u64, Vec<u8>>,
    pending_output_bytes: usize,
    recovery_active: bool,
    armed: bool,
    retained_exit: Option<ProcessExitObservation>,
    exit_published: bool,
}

#[derive(Clone, PartialEq, Eq)]
struct HostedProcessBinding {
    root_pid: u32,
    root_start_ticks: Option<u64>,
    attempt_id: Option<undercurrent_domain::OperationAttemptId>,
}

enum SpawnAttemptProbe {
    Absent,
    Unknown,
    Applied(HostResponseValue),
}

impl HostedProcessRuntime {
    /// Resolves an exact owner-only local bridge peer to its supervised workload.
    ///
    /// Native process incarnations and the bridge executable are reread after
    /// ancestry traversal so PID reuse, reparenting, and executable replacement
    /// fail closed. The model supplies none of these coordinates.
    pub fn resolve_agent_peer(
        &self,
        peer_pid: u32,
        expected_bridge_executable: &Path,
    ) -> Result<Option<HostedAgentPeer>, HostedAgentPeerError> {
        #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
        {
            let _ = (peer_pid, expected_bridge_executable);
            Err(HostedAgentPeerError::UnsupportedPlatform)
        }
        #[cfg(any(target_os = "linux", target_os = "macos", windows))]
        {
            if peer_pid == 0 {
                return Ok(None);
            }
            let roots = {
                let state = self
                    .owner
                    .core
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                state
                    .entries
                    .iter()
                    .map(|(session_id, entry)| {
                        (
                            session_id.clone(),
                            entry.process.id.clone(),
                            entry.root_pid,
                            entry.root_start_ticks,
                            entry.attempt_id.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            };
            let identities = roots
                .iter()
                .map(|root| (root.2, root.3))
                .collect::<Vec<_>>();
            let Some(index) =
                validated_agent_root(peer_pid, expected_bridge_executable, &identities)?
            else {
                return Ok(None);
            };
            let (session_id, process_id, root_pid, root_start_ticks, attempt_id) =
                roots[index].clone();
            let still_current = {
                let state = self
                    .owner
                    .core
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                state.entries.get(&session_id).is_some_and(|entry| {
                    entry.process.id == process_id
                        && entry.root_pid == root_pid
                        && entry.root_start_ticks == root_start_ticks
                        && entry.attempt_id == attempt_id
                })
            };
            if !still_current {
                return Err(HostedAgentPeerError::ProcessTableChanged);
            }
            Ok(Some(HostedAgentPeer {
                session_id,
                process_id,
                attempt_id,
            }))
        }
    }

    /// Connects to or launches the private session host and starts bounded event workers.
    ///
    /// This operation prepares local output, screen, signal, response, and event
    /// queues before opening the authenticated host connection. It performs
    /// filesystem, process, socket, and thread IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed local-store, host connection, launch, or worker failure.
    pub fn start(
        config: HostedProcessRuntimeConfig,
        clock: Arc<dyn Clock>,
        telemetry: Arc<dyn Telemetry>,
    ) -> Result<Self, HostedProcessRuntimeError> {
        Self::start_inner(
            config,
            clock,
            telemetry,
            Arc::new(UnavailableSecretResolver),
            None,
        )
    }

    /// Starts the hosted runtime with an authorized ephemeral credential adapter.
    pub fn start_with_secret_resolver(
        config: HostedProcessRuntimeConfig,
        clock: Arc<dyn Clock>,
        telemetry: Arc<dyn Telemetry>,
        secret_resolver: Arc<dyn SecretResolver>,
    ) -> Result<Self, HostedProcessRuntimeError> {
        Self::start_inner(config, clock, telemetry, secret_resolver, None)
    }

    /// Starts the hosted runtime and wakes the existing bounded observation queue
    /// whenever an exact-process cooperative shell signal changes.
    pub fn start_with_foreground_wake(
        config: HostedProcessRuntimeConfig,
        clock: Arc<dyn Clock>,
        telemetry: Arc<dyn Telemetry>,
        foreground_wake: Arc<dyn ForegroundObservationWake>,
    ) -> Result<Self, HostedProcessRuntimeError> {
        Self::start_inner(
            config,
            clock,
            telemetry,
            Arc::new(UnavailableSecretResolver),
            Some(foreground_wake),
        )
    }

    /// Starts the hosted runtime with both observation wake and credential ports.
    pub fn start_with_foreground_wake_and_secret_resolver(
        config: HostedProcessRuntimeConfig,
        clock: Arc<dyn Clock>,
        telemetry: Arc<dyn Telemetry>,
        foreground_wake: Arc<dyn ForegroundObservationWake>,
        secret_resolver: Arc<dyn SecretResolver>,
    ) -> Result<Self, HostedProcessRuntimeError> {
        Self::start_inner(
            config,
            clock,
            telemetry,
            secret_resolver,
            Some(foreground_wake),
        )
    }

    fn start_inner(
        config: HostedProcessRuntimeConfig,
        clock: Arc<dyn Clock>,
        telemetry: Arc<dyn Telemetry>,
        secret_resolver: Arc<dyn SecretResolver>,
        foreground_wake: Option<Arc<dyn ForegroundObservationWake>>,
    ) -> Result<Self, HostedProcessRuntimeError> {
        let output = Arc::new(
            TerminalOutputStore::new(config.native.output_limits())
                .map_err(HostedProcessRuntimeError::OutputStore)?,
        );
        let screens = Arc::new(
            VtTerminalScreenStore::new(TerminalScreenLimits {
                sessions: config.native.output_limits().sessions,
                snapshot_bytes: config.native.limits().screen_snapshot_bytes,
                scrollback_lines: config.native.scrollback_lines,
            })
            .map_err(HostedProcessRuntimeError::ScreenStore)?,
        );
        let cooperative_signals = Arc::new(
            CooperativeSignalStore::new(config.native.limits().max_processes)
                .map_err(HostedProcessRuntimeError::CooperativeSignals)?,
        );
        let (exits, exit_receiver) = async_channel::bounded(config.native.limits().max_processes);
        let (control_events, control_receiver) = async_channel::bounded(
            config
                .native
                .limits()
                .max_processes
                .saturating_mul(2)
                .max(1),
        );
        let output_event_capacity = config
            .native
            .output_limits()
            .global_bytes
            .div_ceil(config.native.limits().output_read_bytes)
            .max(config.native.limits().max_processes);
        let (output_events, output_receiver) = async_channel::bounded(output_event_capacity);
        let client = HostClient::start(
            config.clone(),
            control_events,
            output_events,
            config.native.limits().worker_thread_stack_bytes,
        )
        .map_err(HostedProcessRuntimeError::HostClient)?;
        let core = Arc::new(HostedRuntimeCore {
            state: Mutex::new(HostedRuntimeState {
                host_epoch: client.host_epoch(),
                ..HostedRuntimeState::default()
            }),
            config,
            clock,
            telemetry,
            secret_resolver,
            output,
            screens,
            cooperative_signals,
            foreground_wake,
            exits,
            exit_receiver,
            closed: AtomicBool::new(false),
        });
        start_control_dispatcher(&core, &client, control_receiver)?;
        start_output_dispatcher(&core, &client, output_receiver)?;
        let preparation_pool = PreparationPool::start(core.config.native.limits())
            .map_err(HostedProcessRuntimeError::PreparationWorkers)?;
        Ok(Self {
            owner: Arc::new(HostedRuntimeOwner {
                core,
                client,
                preparation_pool,
            }),
        })
    }

    /// Returns bounded retained output rebuilt from host replay and live frames.
    pub fn output_store(&self) -> Arc<TerminalOutputStore> {
        Arc::clone(&self.owner.core.output)
    }

    /// Returns the parser-consistent local screen projection.
    pub fn screen_store(&self) -> Arc<VtTerminalScreenStore> {
        Arc::clone(&self.owner.core.screens)
    }

    /// Returns cooperative signals retained locally by the hosted adapter.
    pub fn cooperative_signal_store(&self) -> Arc<CooperativeSignalStore> {
        Arc::clone(&self.owner.core.cooperative_signals)
    }

    /// Returns payload-safe hosted process pressure without remote IO.
    pub fn pressure(&self) -> Result<NativeProcessRuntimePressure, PortError> {
        let state = self
            .owner
            .core
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let output = self.owner.core.output.status().map_err(|source| {
            PortError::new(
                PortErrorKind::Other,
                PortOperation::InspectProcessRuntime,
                None,
                source,
            )
        })?;
        let active = state.entries.len();
        let unarmed = state.entries.values().filter(|entry| !entry.armed).count();
        let retained_exits = state
            .entries
            .values()
            .filter(|entry| entry.retained_exit.is_some())
            .count();
        Ok(NativeProcessRuntimePressure {
            closed: self.owner.core.closed.load(Ordering::Acquire),
            occupied_process_slots: active,
            active_process_entries: active,
            published_process_exits: retained_exits,
            unarmed_processes: unarmed,
            exited_unarmed_processes: state
                .entries
                .values()
                .filter(|entry| !entry.armed && entry.retained_exit.is_some())
                .count(),
            input_workers: 0,
            queued_input_bytes: 0,
            queued_spawns: self.owner.client.pending_count(),
            spawn_queue_capacity: self.owner.core.config.native.limits().spawn_queue_capacity,
            queued_preparations: self.owner.preparation_pool.queued(),
            preparation_queue_capacity: self.owner.preparation_pool.queue_capacity(),
            queued_exits: self.owner.core.exit_receiver.len(),
            output,
            screens: self.owner.core.screens.pressure(),
            cooperative_signals: self.owner.core.cooperative_signals.pressure(),
        })
    }

    /// Stops daemon-side admission and disconnects without terminating host-owned sessions.
    pub fn begin_shutdown(&self) {
        if !self.owner.core.closed.swap(true, Ordering::AcqRel) {
            self.owner.preparation_pool.close();
            self.owner.core.exits.close();
            self.owner.client.close();
        }
    }

    async fn request(
        &self,
        operation: PortOperation,
        session_id: Option<SessionId>,
        command: HostCommand,
    ) -> Result<HostResponseValue, PortError> {
        self.owner
            .client
            .request(command)
            .await
            .map_err(|failure| failure.into_port_error(operation, session_id))
    }

    pub(super) async fn session_command_request(
        &self,
        session_id: &SessionId,
        command: HostCommand,
    ) -> Result<HostResponseValue, PortError> {
        self.request(
            PortOperation::ExecuteInSession,
            Some(session_id.clone()),
            command,
        )
        .await
    }

    pub(super) fn maximum_session_command_output_bytes(&self) -> usize {
        self.owner
            .core
            .config
            .native
            .output_limits()
            .per_session_bytes
            .max(1)
    }

    async fn attempted_mutation_request(
        &self,
        context: &ProcessMutationContext,
        operation: PortOperation,
        session_id: Option<SessionId>,
        mutation: HostAttemptMutation,
    ) -> Result<HostResponseValue, PortError> {
        let attempt_id = context.attempt_id().clone();
        let response = self
            .request(
                operation,
                session_id.clone(),
                HostCommand::AttemptMutation {
                    attempt: host_attempt_context(context),
                    mutation,
                },
            )
            .await
            .map_err(|error| {
                let certainty = if matches!(
                    error.kind(),
                    PortErrorKind::InvalidData
                        | PortErrorKind::Conflict
                        | PortErrorKind::CapacityExceeded
                ) {
                    EffectCertainty::KnownAbsent
                } else {
                    EffectCertainty::Unknown
                };
                error.with_attempt_effect(attempt_id.clone(), certainty)
            })?;
        match response {
            HostResponseValue::AttemptFailed { certainty, failure } => {
                let certainty = effect_certainty(certainty);
                if certainty == EffectCertainty::KnownApplied {
                    return Err(unexpected_response(operation, session_id));
                }
                Err(HostRequestError::Remote(failure)
                    .into_port_error(operation, session_id)
                    .with_attempt_effect(attempt_id, certainty))
            }
            HostResponseValue::AttemptObserved { certainty, .. } => {
                let certainty = effect_certainty(certainty);
                Err(PortError::new(
                    if certainty == EffectCertainty::KnownAbsent {
                        PortErrorKind::Conflict
                    } else {
                        PortErrorKind::Unavailable
                    },
                    operation,
                    session_id,
                    RuntimeAdapterFailure("session host retained a non-replayable attempt receipt"),
                )
                .with_attempt_effect(attempt_id, certainty))
            }
            response => Ok(response),
        }
    }

    async fn probe_attempt(
        &self,
        context: &ProcessMutationContext,
        session_id: &SessionId,
    ) -> Result<SpawnAttemptProbe, PortError> {
        let response = self
            .request(
                PortOperation::SpawnProcess,
                Some(session_id.clone()),
                HostCommand::ProbeAttempt {
                    attempt: host_attempt_context(context),
                },
            )
            .await?;
        match response {
            HostResponseValue::AttemptProbed {
                state: HostAttemptProbeState::Absent,
                response: None,
            } => Ok(SpawnAttemptProbe::Absent),
            HostResponseValue::AttemptProbed {
                state: HostAttemptProbeState::Unknown,
                response: None,
            } => Ok(SpawnAttemptProbe::Unknown),
            HostResponseValue::AttemptProbed {
                state: HostAttemptProbeState::Applied,
                response: Some(response),
            } => Ok(SpawnAttemptProbe::Applied(*response)),
            _ => Err(unexpected_response(
                PortOperation::SpawnProcess,
                Some(session_id.clone()),
            )),
        }
    }

    pub(super) async fn cancel_uncommitted_shutdown(
        &self,
        attempt: &OperationAttempt,
    ) -> Result<bool, PortError> {
        if attempt.kind() != ProcessMutationKind::Shutdown
            || attempt.target_session().is_some()
            || attempt.dispatch_binding().is_some()
            || !attempt.state().requires_reconciliation()
        {
            return Ok(false);
        }
        let response = self
            .request(
                PortOperation::ObserveOperationAttemptEffect,
                None,
                HostCommand::CancelShutdownAttempt {
                    attempt: host_attempt_context_from_attempt(attempt),
                },
            )
            .await?;
        match response {
            HostResponseValue::Completed => Ok(true),
            _ => Err(unexpected_response(
                PortOperation::ObserveOperationAttemptEffect,
                None,
            )),
        }
    }

    pub(super) async fn acknowledge_shutdown_cancellation(
        &self,
        attempt: &OperationAttempt,
    ) -> Result<(), PortError> {
        if attempt.kind() != ProcessMutationKind::Shutdown
            || attempt.target_session().is_some()
            || attempt.dispatch_binding().is_some()
            || !matches!(
                attempt.state(),
                undercurrent_domain::OperationAttemptState::Settled {
                    certainty: EffectCertainty::KnownAbsent | EffectCertainty::Unknown,
                    ..
                }
            )
        {
            return Err(unexpected_response(
                PortOperation::ObserveOperationAttemptEffect,
                None,
            ));
        }
        // Repair passes its pre-commit Unknown snapshot; automatic recovery
        // passes the committed KnownAbsent attempt. Both call only after commit.
        // This acknowledgement only releases a cancellation tombstone. In
        // particular it must not use acknowledge_shutdown_attempt, whose ACK
        // retires the currently connected host and all its processes.
        let response = self
            .request(
                PortOperation::ObserveOperationAttemptEffect,
                None,
                HostCommand::AcknowledgeShutdownCancellation {
                    attempt: host_attempt_context_from_attempt(attempt),
                },
            )
            .await?;
        match response {
            HostResponseValue::Completed => Ok(()),
            _ => Err(unexpected_response(
                PortOperation::ObserveOperationAttemptEffect,
                None,
            )),
        }
    }

    pub(super) async fn observe_input_retirement(
        &self,
        attempt: &OperationAttempt,
    ) -> Result<Option<undercurrent_application::InputAttemptRetirementEvidence>, PortError> {
        let response = self
            .request(
                PortOperation::ObserveOperationAttemptEffect,
                attempt.target_session().cloned(),
                HostCommand::ObserveInputRetirement {
                    attempt: host_attempt_context_from_attempt(attempt),
                },
            )
            .await?;
        match response {
            HostResponseValue::InputRetirementObserved {
                process_id: None,
                exited_at_millis: None,
            } => Ok(None),
            HostResponseValue::InputRetirementObserved {
                process_id: Some(id),
                exited_at_millis: Some(at),
            } => {
                let process_id = ProcessInstanceId::new(id).map_err(|_| {
                    unexpected_response(
                        PortOperation::ObserveOperationAttemptEffect,
                        attempt.target_session().cloned(),
                    )
                })?;
                Ok(Some(
                    undercurrent_application::InputAttemptRetirementEvidence {
                        process_id,
                        exited_at: TimestampMillis::new(at),
                    },
                ))
            }
            _ => Err(unexpected_response(
                PortOperation::ObserveOperationAttemptEffect,
                attempt.target_session().cloned(),
            )),
        }
    }

    pub(super) async fn observe_process_attempt(
        &self,
        attempt: &OperationAttempt,
    ) -> Result<ObservedOperationAttemptEffect, PortError> {
        let response = self
            .request(
                PortOperation::ObserveOperationAttemptEffect,
                attempt.target_session().cloned(),
                HostCommand::ObserveAttempt {
                    attempt: host_attempt_context_from_attempt(attempt),
                },
            )
            .await?;
        let HostResponseValue::AttemptObserved {
            certainty,
            result_digest,
        } = response
        else {
            return Err(unexpected_response(
                PortOperation::ObserveOperationAttemptEffect,
                attempt.target_session().cloned(),
            ));
        };
        let certainty = effect_certainty(certainty);
        let result_digest = result_digest
            .map(|digest| PayloadDigest::from_hex(&digest))
            .transpose()
            .map_err(|source| {
                PortError::new(
                    PortErrorKind::ProtocolViolation,
                    PortOperation::ObserveOperationAttemptEffect,
                    attempt.target_session().cloned(),
                    source,
                )
            })?;
        if (certainty == EffectCertainty::KnownApplied) != result_digest.is_some() {
            return Err(PortError::new(
                PortErrorKind::ProtocolViolation,
                PortOperation::ObserveOperationAttemptEffect,
                attempt.target_session().cloned(),
                RuntimeAdapterFailure("session host returned inconsistent attempt evidence"),
            ));
        }
        Ok(ObservedOperationAttemptEffect {
            certainty,
            result_digest,
        })
    }

    pub(super) async fn acknowledge_process_attempt(
        &self,
        attempt: &OperationAttempt,
    ) -> Result<(), PortError> {
        if attempt.kind() == ProcessMutationKind::Shutdown {
            if matches!(
                attempt.state(),
                undercurrent_domain::OperationAttemptState::Settled {
                    certainty: EffectCertainty::KnownAbsent,
                    ..
                }
            ) {
                return self.acknowledge_shutdown_cancellation(attempt).await;
            }
            return self.acknowledge_shutdown_attempt(attempt).await;
        }
        let response = match self
            .request(
                PortOperation::ObserveOperationAttemptEffect,
                attempt.target_session().cloned(),
                HostCommand::AcknowledgeAttempt {
                    attempt: host_attempt_context_from_attempt(attempt),
                },
            )
            .await
        {
            Ok(response) => response,
            Err(error)
                if matches!(
                    error.kind(),
                    PortErrorKind::Unavailable
                        | PortErrorKind::Timeout
                        | PortErrorKind::Interrupted
                ) =>
            {
                // Receipt release is idempotent. Retry once so loss of the
                // first acknowledgement response cannot turn an already
                // committed product result into an unrecoverable caller error.
                self.request(
                    PortOperation::ObserveOperationAttemptEffect,
                    attempt.target_session().cloned(),
                    HostCommand::AcknowledgeAttempt {
                        attempt: host_attempt_context_from_attempt(attempt),
                    },
                )
                .await?
            }
            Err(error) => return Err(error),
        };
        if matches!(response, HostResponseValue::Completed) {
            Ok(())
        } else {
            Err(unexpected_response(
                PortOperation::ObserveOperationAttemptEffect,
                attempt.target_session().cloned(),
            ))
        }
    }

    /// Requests durable, attempt-aware host shutdown. The retained receipt is
    /// replayable while the host remains live. The host retires only after the
    /// application durably settles the attempt and acknowledges this receipt.
    pub fn shutdown_attempted<'a>(
        &'a self,
        context: &'a ProcessMutationContext,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            let response = self
                .attempted_mutation_request(
                    context,
                    PortOperation::ShutdownProcessHost,
                    None,
                    HostAttemptMutation::Shutdown,
                )
                .await?;
            if matches!(response, HostResponseValue::Completed) {
                Ok(())
            } else {
                Err(unexpected_response(
                    PortOperation::ShutdownProcessHost,
                    None,
                ))
            }
        })
    }

    async fn acknowledge_shutdown_attempt(
        &self,
        attempt: &OperationAttempt,
    ) -> Result<(), PortError> {
        if attempt.kind() != ProcessMutationKind::Shutdown {
            return Err(PortError::new(
                PortErrorKind::ProtocolViolation,
                PortOperation::ShutdownProcessHost,
                attempt.target_session().cloned(),
                RuntimeAdapterFailure("shutdown acknowledgement received a non-shutdown attempt"),
            ));
        }
        if host_retired(&self.owner.client.config) {
            self.begin_shutdown();
            return Ok(());
        }

        self.owner.client.begin_retirement();
        let response = self
            .request(
                PortOperation::ShutdownProcessHost,
                attempt.target_session().cloned(),
                HostCommand::AcknowledgeAttempt {
                    attempt: host_attempt_context_from_attempt(attempt),
                },
            )
            .await;

        match response {
            Ok(HostResponseValue::Completed) => {}
            Ok(_) => {
                self.owner.client.cancel_retirement();
                return Err(unexpected_response(
                    PortOperation::ShutdownProcessHost,
                    attempt.target_session().cloned(),
                ));
            }
            Err(error) => {
                if wait_for_host_retirement(&self.owner.client.config) {
                    self.begin_shutdown();
                    return Ok(());
                }
                self.owner.client.cancel_retirement();
                return Err(error);
            }
        }

        self.begin_shutdown();
        if wait_for_host_retirement(&self.owner.client.config) {
            Ok(())
        } else {
            Err(PortError::new(
                PortErrorKind::Timeout,
                PortOperation::ShutdownProcessHost,
                attempt.target_session().cloned(),
                RuntimeAdapterFailure(
                    "session host did not retire after durable shutdown acknowledgement",
                ),
            ))
        }
    }

    async fn recover_output(
        &self,
        session_id: SessionId,
        process_id: ProcessInstanceId,
    ) -> Result<(), PortError> {
        // Replacement processes continue the session's absolute output stream.
        // Starting at zero invents a retention gap and can discard retained logs
        // while restoring a screen checkpoint from the replacement process.
        let from = self
            .owner
            .core
            .output
            .session_status(&session_id)
            .map_err(|source| {
                hosted_output_port_error(PortOperation::ReadTerminalOutput, &session_id, source)
            })?
            .map_or(0, |status| status.next_offset.value());
        recover_output(
            &self.owner.core,
            &self.owner.client,
            session_id,
            process_id,
            from,
        )
        .await
    }

    async fn retire(&self, session_id: &SessionId, process_id: &ProcessInstanceId) {
        let _ = self
            .request(
                PortOperation::InspectProcessRuntime,
                Some(session_id.clone()),
                HostCommand::Retire {
                    session_id: session_id.as_str().to_owned(),
                    process_id: process_id.as_str().to_owned(),
                },
            )
            .await;
        self.owner.core.remove_entry(session_id, process_id);
    }
}

impl ProcessRuntime for HostedProcessRuntime {
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
                    RuntimeAdapterFailure("hosted process runtime is shutting down"),
                ));
            }
            let reply = self
                .owner
                .preparation_pool
                .submit(launch, self.owner.core.config.native().limits())?;
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
            let limits = self.owner.core.config.native().limits();
            let resolved_secrets = resolve_secret_environment(
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
            let secret_environment =
                HostResolvedSecretEnvironment::from_resolved(&resolved_secrets);
            let spawn_token = generate_token().map_err(|source| {
                PortError::new(
                    PortErrorKind::Unavailable,
                    PortOperation::SpawnProcess,
                    Some(session_id.clone()),
                    source,
                )
            })?;
            let (response, mut acceptance) = self
                .owner
                .client
                .spawn_request(
                    spawn_token.clone(),
                    session_id.as_str().to_owned(),
                    self.owner.core.host_launch(session_id, launch)?,
                    secret_environment,
                )
                .await
                .map_err(|failure| {
                    failure.into_port_error(PortOperation::SpawnProcess, Some(session_id.clone()))
                })?;
            let HostResponseValue::Spawned {
                process_id,
                started_at_millis,
                root_pid,
                root_start_ticks,
            } = response
            else {
                return Err(unexpected_response(
                    PortOperation::SpawnProcess,
                    Some(session_id.clone()),
                ));
            };
            let process = ProcessInstance {
                id: ProcessInstanceId::new(process_id).map_err(|source| {
                    PortError::new(
                        PortErrorKind::ProtocolViolation,
                        PortOperation::SpawnProcess,
                        Some(session_id.clone()),
                        source,
                    )
                })?,
                started_at: TimestampMillis::new(started_at_millis),
            };
            validate_host_process_identity(root_pid, root_start_ticks).map_err(|source| {
                PortError::new(
                    PortErrorKind::ProtocolViolation,
                    PortOperation::SpawnProcess,
                    Some(session_id.clone()),
                    source,
                )
            })?;
            self.owner.core.install_entry(
                session_id.clone(),
                process.clone(),
                HostedProcessBinding {
                    root_pid,
                    root_start_ticks,
                    attempt_id: None,
                },
                launch.terminal_size(),
                launch.output_capture_mode(),
            )?;
            acceptance.track_local(&self.owner.core, session_id.clone(), process.id.clone());
            if let Err(error) = self
                .recover_output(session_id.clone(), process.id.clone())
                .await
            {
                self.owner.core.remove_entry(session_id, &process.id);
                return Err(error);
            }
            let adopted = match self
                .request(
                    PortOperation::SpawnProcess,
                    Some(session_id.clone()),
                    HostCommand::AdoptSpawn { spawn_token },
                )
                .await
            {
                Ok(adopted) => adopted,
                Err(error) => {
                    self.owner.core.remove_entry(session_id, &process.id);
                    return Err(error);
                }
            };
            if let Err(error) = expect_completed(adopted, PortOperation::SpawnProcess, session_id) {
                self.owner.core.remove_entry(session_id, &process.id);
                return Err(error);
            }
            acceptance.accept();
            Ok(process)
        })
    }

    fn arm_exit<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            self.owner.core.arm_exit(session_id, process)?;
            let response = self
                .request(
                    PortOperation::ArmProcessExit,
                    Some(session_id.clone()),
                    HostCommand::ArmExit {
                        session_id: session_id.as_str().to_owned(),
                        process_id: process.as_str().to_owned(),
                    },
                )
                .await?;
            expect_completed(response, PortOperation::ArmProcessExit, session_id)
        })
    }

    fn stop<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        policy: StopPolicy,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        Box::pin(async move {
            let command = HostCommand::stop(session_id, process, policy).map_err(|source| {
                PortError::new(
                    PortErrorKind::InvalidData,
                    PortOperation::StopProcess,
                    Some(session_id.clone()),
                    source,
                )
            })?;
            let response = self
                .request(
                    PortOperation::StopProcess,
                    Some(session_id.clone()),
                    command,
                )
                .await?;
            let termination = decode_termination(response, PortOperation::StopProcess, session_id)?;
            if termination.kind() != undercurrent_application::TerminationKind::GracefulTimedOut {
                self.retire(session_id, process).await;
            }
            Ok(termination)
        })
    }

    fn force_stop<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        Box::pin(async move {
            let response = self
                .request(
                    PortOperation::ForceProcess,
                    Some(session_id.clone()),
                    HostCommand::ForceStop {
                        session_id: session_id.as_str().to_owned(),
                        process_id: process.as_str().to_owned(),
                    },
                )
                .await?;
            let termination =
                decode_termination(response, PortOperation::ForceProcess, session_id)?;
            if termination.kind() != undercurrent_application::TerminationKind::GracefulTimedOut {
                self.retire(session_id, process).await;
            }
            Ok(termination)
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
            let delivery = encode_terminal_input_delivery(input, policy).map_err(|source| {
                PortError::new(
                    PortErrorKind::InvalidData,
                    PortOperation::WriteInput,
                    Some(session_id.clone()),
                    source,
                )
            })?;
            let pacing = delivery
                .pause()
                .map(|pause| {
                    Ok(HostInputPacing {
                        offset: u32::try_from(pause.offset()).map_err(|source| {
                            PortError::new(
                                PortErrorKind::InvalidData,
                                PortOperation::WriteInput,
                                Some(session_id.clone()),
                                source,
                            )
                        })?,
                        delay_millis: u64::try_from(pause.delay().as_millis()).map_err(
                            |source| {
                                PortError::new(
                                    PortErrorKind::InvalidData,
                                    PortOperation::WriteInput,
                                    Some(session_id.clone()),
                                    source,
                                )
                            },
                        )?,
                    })
                })
                .transpose()?;
            let response = self
                .request(
                    PortOperation::WriteInput,
                    Some(session_id.clone()),
                    HostCommand::WriteInput {
                        session_id: session_id.as_str().to_owned(),
                        process_id: process.as_str().to_owned(),
                        bytes: delivery.into_bytes(),
                        pacing,
                    },
                )
                .await?;
            expect_completed(response, PortOperation::WriteInput, session_id)
        })
    }

    fn close_input<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<StandardInputClosure, PortError>> {
        Box::pin(async move {
            let response = self
                .request(
                    PortOperation::CloseInput,
                    Some(session_id.clone()),
                    HostCommand::CloseInput {
                        session_id: session_id.as_str().to_owned(),
                        process_id: process.as_str().to_owned(),
                    },
                )
                .await?;
            match response {
                HostResponseValue::InputClosed {
                    already_closed: false,
                } => Ok(StandardInputClosure::Closed),
                HostResponseValue::InputClosed {
                    already_closed: true,
                } => Ok(StandardInputClosure::AlreadyClosed),
                _ => Err(unexpected_response(
                    PortOperation::CloseInput,
                    Some(session_id.clone()),
                )),
            }
        })
    }

    fn resize<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        size: TerminalSize,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            let response = self
                .request(
                    PortOperation::ResizeTerminal,
                    Some(session_id.clone()),
                    HostCommand::Resize {
                        session_id: session_id.as_str().to_owned(),
                        process_id: process.as_str().to_owned(),
                        columns: size.columns(),
                        rows: size.rows(),
                    },
                )
                .await?;
            expect_completed(response, PortOperation::ResizeTerminal, session_id)?;
            self.owner.core.resize_entry(session_id, process, size)?;
            self.owner.core.screens.resize(session_id, size)
        })
    }

    fn spawn_attempted<'a>(
        &'a self,
        context: &'a ProcessMutationContext,
        session_id: &'a SessionId,
        launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<ProcessInstance, PortError>> {
        Box::pin(async move {
            let response = match self
                .probe_attempt(context, session_id)
                .await
                .map_err(|error| {
                    let certainty = if matches!(
                        error.kind(),
                        PortErrorKind::InvalidData
                            | PortErrorKind::Conflict
                            | PortErrorKind::CapacityExceeded
                    ) {
                        EffectCertainty::KnownAbsent
                    } else {
                        EffectCertainty::Unknown
                    };
                    error.with_attempt_effect(context.attempt_id().clone(), certainty)
                })? {
                SpawnAttemptProbe::Applied(response) => response,
                SpawnAttemptProbe::Unknown => {
                    return Err(PortError::new(
                        PortErrorKind::Unavailable,
                        PortOperation::SpawnProcess,
                        Some(session_id.clone()),
                        RuntimeAdapterFailure(
                            "session host retained an unresolved spawn attempt receipt",
                        ),
                    )
                    .with_attempt_effect(context.attempt_id().clone(), EffectCertainty::Unknown));
                }
                SpawnAttemptProbe::Absent => {
                    let limits = self.owner.core.config.native().limits();
                    let resolved_secrets = resolve_secret_environment(
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
                    .await
                    .map_err(|error| {
                        error.with_attempt_effect(
                            context.attempt_id().clone(),
                            EffectCertainty::KnownAbsent,
                        )
                    })?;
                    self.attempted_mutation_request(
                        context,
                        PortOperation::SpawnProcess,
                        Some(session_id.clone()),
                        HostAttemptMutation::Spawn {
                            session_id: session_id.as_str().to_owned(),
                            launch: Box::new(self.owner.core.host_launch(session_id, launch)?),
                            secret_environment: HostResolvedSecretEnvironment::from_resolved(
                                &resolved_secrets,
                            ),
                        },
                    )
                    .await?
                }
            };
            let HostResponseValue::Spawned {
                process_id,
                started_at_millis,
                root_pid,
                root_start_ticks,
            } = response
            else {
                return Err(unexpected_response(
                    PortOperation::SpawnProcess,
                    Some(session_id.clone()),
                ));
            };
            let process = ProcessInstance {
                id: ProcessInstanceId::new(process_id).map_err(|source| {
                    PortError::new(
                        PortErrorKind::ProtocolViolation,
                        PortOperation::SpawnProcess,
                        Some(session_id.clone()),
                        source,
                    )
                    .with_attempt_effect(
                        context.attempt_id().clone(),
                        EffectCertainty::KnownApplied,
                    )
                })?,
                started_at: TimestampMillis::new(started_at_millis),
            };
            validate_host_process_identity(root_pid, root_start_ticks).map_err(|source| {
                PortError::new(
                    PortErrorKind::ProtocolViolation,
                    PortOperation::SpawnProcess,
                    Some(session_id.clone()),
                    source,
                )
                .with_attempt_effect(context.attempt_id().clone(), EffectCertainty::KnownApplied)
            })?;
            self.owner
                .core
                .install_or_validate_entry(
                    session_id.clone(),
                    process.clone(),
                    HostedProcessBinding {
                        root_pid,
                        root_start_ticks,
                        attempt_id: Some(context.attempt_id().clone()),
                    },
                    launch.terminal_size(),
                    launch.output_capture_mode(),
                )
                .map_err(|error| {
                    error.with_attempt_effect(
                        context.attempt_id().clone(),
                        EffectCertainty::KnownApplied,
                    )
                })?;
            self.recover_output(session_id.clone(), process.id.clone())
                .await
                .map_err(|error| {
                    error.with_attempt_effect(
                        context.attempt_id().clone(),
                        EffectCertainty::KnownApplied,
                    )
                })?;
            Ok(process)
        })
    }

    fn arm_exit_attempted<'a>(
        &'a self,
        context: &'a ProcessMutationContext,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            self.owner
                .core
                .arm_exit(session_id, process)
                .map_err(|error| {
                    error.with_attempt_effect(
                        context.attempt_id().clone(),
                        EffectCertainty::KnownAbsent,
                    )
                })?;
            let response = self
                .attempted_mutation_request(
                    context,
                    PortOperation::ArmProcessExit,
                    Some(session_id.clone()),
                    HostAttemptMutation::ArmExit {
                        session_id: session_id.as_str().to_owned(),
                        process_id: process.as_str().to_owned(),
                    },
                )
                .await?;
            expect_completed(response, PortOperation::ArmProcessExit, session_id)
        })
    }

    fn stop_attempted<'a>(
        &'a self,
        context: &'a ProcessMutationContext,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        policy: StopPolicy,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        Box::pin(async move {
            let command = HostCommand::stop(session_id, process, policy).map_err(|source| {
                PortError::new(
                    PortErrorKind::InvalidData,
                    PortOperation::StopProcess,
                    Some(session_id.clone()),
                    source,
                )
                .with_attempt_effect(context.attempt_id().clone(), EffectCertainty::KnownAbsent)
            })?;
            let HostCommand::Stop {
                session_id: host_session,
                process_id,
                signal,
                force_after_millis,
                no_escalation,
            } = command
            else {
                unreachable!("stop constructor returns stop command")
            };
            let response = self
                .attempted_mutation_request(
                    context,
                    PortOperation::StopProcess,
                    Some(session_id.clone()),
                    HostAttemptMutation::Stop {
                        session_id: host_session,
                        process_id,
                        signal,
                        force_after_millis,
                        no_escalation,
                    },
                )
                .await?;
            let termination = decode_termination(response, PortOperation::StopProcess, session_id)?;
            if termination.kind() != undercurrent_application::TerminationKind::GracefulTimedOut {
                self.retire(session_id, process).await;
            }
            Ok(termination)
        })
    }

    fn force_stop_attempted<'a>(
        &'a self,
        context: &'a ProcessMutationContext,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        Box::pin(async move {
            let response = self
                .attempted_mutation_request(
                    context,
                    PortOperation::ForceProcess,
                    Some(session_id.clone()),
                    HostAttemptMutation::ForceStop {
                        session_id: session_id.as_str().to_owned(),
                        process_id: process.as_str().to_owned(),
                    },
                )
                .await?;
            let termination =
                decode_termination(response, PortOperation::ForceProcess, session_id)?;
            if termination.kind() != undercurrent_application::TerminationKind::GracefulTimedOut {
                self.retire(session_id, process).await;
            }
            Ok(termination)
        })
    }

    fn write_input_attempted<'a>(
        &'a self,
        context: &'a ProcessMutationContext,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        input: &'a InputSequence,
        policy: InputDeliveryPolicy,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            let delivery = encode_terminal_input_delivery(input, policy).map_err(|source| {
                PortError::new(
                    PortErrorKind::InvalidData,
                    PortOperation::WriteInput,
                    Some(session_id.clone()),
                    source,
                )
                .with_attempt_effect(context.attempt_id().clone(), EffectCertainty::KnownAbsent)
            })?;
            let pacing = delivery
                .pause()
                .map(|pause| {
                    Ok(HostInputPacing {
                        offset: u32::try_from(pause.offset()).map_err(|source| {
                            PortError::new(
                                PortErrorKind::InvalidData,
                                PortOperation::WriteInput,
                                Some(session_id.clone()),
                                source,
                            )
                        })?,
                        delay_millis: u64::try_from(pause.delay().as_millis()).map_err(
                            |source| {
                                PortError::new(
                                    PortErrorKind::InvalidData,
                                    PortOperation::WriteInput,
                                    Some(session_id.clone()),
                                    source,
                                )
                            },
                        )?,
                    })
                })
                .transpose()?;
            let response = self
                .attempted_mutation_request(
                    context,
                    PortOperation::WriteInput,
                    Some(session_id.clone()),
                    HostAttemptMutation::WriteInput {
                        session_id: session_id.as_str().to_owned(),
                        process_id: process.as_str().to_owned(),
                        bytes: delivery.into_bytes(),
                        pacing,
                    },
                )
                .await?;
            expect_completed(response, PortOperation::WriteInput, session_id)
        })
    }

    fn resize_attempted<'a>(
        &'a self,
        context: &'a ProcessMutationContext,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        size: TerminalSize,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            let response = self
                .attempted_mutation_request(
                    context,
                    PortOperation::ResizeTerminal,
                    Some(session_id.clone()),
                    HostAttemptMutation::Resize {
                        session_id: session_id.as_str().to_owned(),
                        process_id: process.as_str().to_owned(),
                        columns: size.columns(),
                        rows: size.rows(),
                    },
                )
                .await?;
            expect_completed(response, PortOperation::ResizeTerminal, session_id)?;
            self.owner.core.resize_entry(session_id, process, size)?;
            self.owner.core.screens.resize(session_id, size)
        })
    }

    fn acknowledge_attempt<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move { self.acknowledge_process_attempt(attempt).await })
    }
}

impl ProcessHostShutdown for HostedProcessRuntime {
    fn shutdown_attempted<'a>(
        &'a self,
        context: &'a ProcessMutationContext,
    ) -> PortFuture<'a, Result<(), PortError>> {
        HostedProcessRuntime::shutdown_attempted(self, context)
    }

    fn acknowledge_shutdown<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move { self.acknowledge_shutdown_attempt(attempt).await })
    }
}

impl ForegroundProcessObserver for HostedProcessRuntime {
    fn observe_foreground<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<ForegroundProcessGroupObservation, PortError>> {
        Box::pin(async move {
            let response = self
                .request(
                    PortOperation::ObserveForegroundProcessGroup,
                    Some(session_id.clone()),
                    HostCommand::ObserveForeground {
                        session_id: session_id.as_str().to_owned(),
                        process_id: process.as_str().to_owned(),
                    },
                )
                .await?;
            let HostResponseValue::ForegroundContext { context } = response else {
                return Err(unexpected_response(
                    PortOperation::ObserveForegroundProcessGroup,
                    Some(session_id.clone()),
                ));
            };
            let observed_process =
                ProcessInstanceId::new(context.process_id).map_err(|source| {
                    PortError::new(
                        PortErrorKind::ProtocolViolation,
                        PortOperation::ObserveForegroundProcessGroup,
                        Some(session_id.clone()),
                        source,
                    )
                })?;
            if observed_process != *process {
                return Err(unexpected_response(
                    PortOperation::ObserveForegroundProcessGroup,
                    Some(session_id.clone()),
                ));
            }
            let mut members = Vec::with_capacity(context.members.len());
            for member in context.members {
                let environment_names = member
                    .environment_names
                    .into_iter()
                    .map(|name| EnvironmentName::new(name, 256))
                    .collect::<Result<BTreeSet<_>, _>>()
                    .map_err(|source| {
                        PortError::new(
                            PortErrorKind::ProtocolViolation,
                            PortOperation::ObserveForegroundProcessGroup,
                            Some(session_id.clone()),
                            source,
                        )
                    })?;
                let environment_names_complete = member.environment_names_complete;
                let target = ProfileMatchTarget::new(
                    member.program,
                    member.arguments,
                    environment_names,
                    16 * 1024,
                    64,
                    64,
                )
                .map_err(|source| {
                    PortError::new(
                        PortErrorKind::ProtocolViolation,
                        PortOperation::ObserveForegroundProcessGroup,
                        Some(session_id.clone()),
                        source,
                    )
                })?;
                #[cfg(windows)]
                let target = target
                    .with_match_convention(undercurrent_domain::ProfileMatchConvention::Windows);
                let member = ForegroundProgramMember::new(member.incarnation, target);
                members.push(if environment_names_complete {
                    member
                } else {
                    member.with_truncated_environment_names()
                });
            }
            let current_directory = context
                .current_directory
                .map(|path| PathReference::new(path, 4 * 1024))
                .transpose()
                .map_err(|source| {
                    PortError::new(
                        PortErrorKind::ProtocolViolation,
                        PortOperation::ObserveForegroundProcessGroup,
                        Some(session_id.clone()),
                        source,
                    )
                })?;
            Ok(ForegroundProcessGroupObservation::new(
                observed_process,
                TimestampMillis::new(context.observed_at_millis),
                members,
                match context.completeness {
                    HostProcessGroupCompleteness::Complete => ProcessGroupCompleteness::Complete,
                    HostProcessGroupCompleteness::Truncated => ProcessGroupCompleteness::Truncated,
                    HostProcessGroupCompleteness::Unavailable => {
                        ProcessGroupCompleteness::Unavailable
                    }
                },
                current_directory,
                match context.tcp_listener {
                    HostTcpListenerObservation::Present => TcpListenerObservation::Present,
                    HostTcpListenerObservation::Absent => TcpListenerObservation::Absent,
                    HostTcpListenerObservation::Unavailable => TcpListenerObservation::Unavailable,
                    HostTcpListenerObservation::Truncated => TcpListenerObservation::Truncated,
                },
                context.cooperative.map(Into::into),
            )
            .with_managed_process_tcp_listener(match context.managed_process_tcp_listener {
                Some(HostTcpListenerObservation::Present) => TcpListenerObservation::Present,
                Some(HostTcpListenerObservation::Absent) => TcpListenerObservation::Absent,
                Some(HostTcpListenerObservation::Truncated) => TcpListenerObservation::Truncated,
                Some(HostTcpListenerObservation::Unavailable) | None => {
                    TcpListenerObservation::Unavailable
                }
            })
            .with_managed_tree_tcp_listener(match context.managed_tree_tcp_listener {
                Some(HostTcpListenerObservation::Present) => TcpListenerObservation::Present,
                Some(HostTcpListenerObservation::Absent) => TcpListenerObservation::Absent,
                Some(HostTcpListenerObservation::Truncated) => TcpListenerObservation::Truncated,
                Some(HostTcpListenerObservation::Unavailable) | None => {
                    TcpListenerObservation::Unavailable
                }
            }))
        })
    }
}

impl ProcessTruthSource for HostedProcessRuntime {
    fn observe<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessIdentity,
    ) -> PortFuture<'a, Result<undercurrent_domain::ProcessTruth, PortError>> {
        Box::pin(async move {
            let response = self
                .request(
                    PortOperation::InspectProcessRuntime,
                    Some(session_id.clone()),
                    HostCommand::Observe {
                        session_id: session_id.as_str().to_owned(),
                        process_id: process.id().as_str().to_owned(),
                        started_at_millis: process.started_at().value(),
                    },
                )
                .await?;
            let HostResponseValue::Observed {
                state,
                columns,
                rows,
                output_capture_mode,
                root_pid,
                root_start_ticks,
            } = response
            else {
                return Err(unexpected_response(
                    PortOperation::InspectProcessRuntime,
                    Some(session_id.clone()),
                ));
            };
            let state: undercurrent_domain::ProcessTruth = state.into();
            if !matches!(state, undercurrent_domain::ProcessTruth::Missing { .. }) {
                let (Some(columns), Some(rows)) = (columns, rows) else {
                    return Err(unexpected_response(
                        PortOperation::InspectProcessRuntime,
                        Some(session_id.clone()),
                    ));
                };
                let size =
                    TerminalSize::new(columns, rows, u16::MAX, u16::MAX).map_err(|source| {
                        PortError::new(
                            PortErrorKind::ProtocolViolation,
                            PortOperation::InspectProcessRuntime,
                            Some(session_id.clone()),
                            source,
                        )
                    })?;
                let instance = ProcessInstance {
                    id: process.id().clone(),
                    started_at: process.started_at(),
                };
                let output_capture_mode: OutputCaptureMode = output_capture_mode.into();
                let root_pid = root_pid.ok_or_else(|| {
                    unexpected_response(
                        PortOperation::InspectProcessRuntime,
                        Some(session_id.clone()),
                    )
                })?;
                validate_host_process_identity(root_pid, root_start_ticks).map_err(|source| {
                    PortError::new(
                        PortErrorKind::ProtocolViolation,
                        PortOperation::InspectProcessRuntime,
                        Some(session_id.clone()),
                        source,
                    )
                })?;
                self.owner.core.install_or_validate_entry(
                    session_id.clone(),
                    instance,
                    HostedProcessBinding {
                        root_pid,
                        root_start_ticks,
                        attempt_id: None,
                    },
                    size,
                    output_capture_mode,
                )?;
                match output_capture_mode {
                    OutputCaptureMode::Terminal => {
                        recover_screen_and_output(
                            &self.owner.core,
                            &self.owner.client,
                            session_id.clone(),
                            process.id().clone(),
                        )
                        .await?;
                    }
                    OutputCaptureMode::Opaque => {
                        let from = self
                            .owner
                            .core
                            .output
                            .session_status(session_id)
                            .map_err(|source| {
                                hosted_output_port_error(
                                    PortOperation::ReadTerminalOutput,
                                    session_id,
                                    source,
                                )
                            })?
                            .map_or(0, |status| status.next_offset.value());
                        recover_output(
                            &self.owner.core,
                            &self.owner.client,
                            session_id.clone(),
                            process.id().clone(),
                            from,
                        )
                        .await?;
                    }
                }
                if matches!(state, undercurrent_domain::ProcessTruth::Exited { .. }) {
                    self.retire(session_id, process.id()).await;
                }
            }
            Ok(state)
        })
    }
}

impl ProcessExitSource for HostedProcessRuntime {
    fn next_exit(&self) -> PortFuture<'_, Result<ProcessExitObservation, PortError>> {
        Box::pin(async move {
            let observation = self
                .owner
                .core
                .exit_receiver
                .recv()
                .await
                .map_err(|source| {
                    PortError::new(
                        PortErrorKind::Unavailable,
                        PortOperation::ReadProcessExit,
                        None,
                        source,
                    )
                })?;
            self.retire(&observation.session_id, &observation.process_id)
                .await;
            Ok(observation)
        })
    }
}

impl ExecutionContextResolver for HostedProcessRuntime {
    fn shell_environment_observed_at<'a>(
        &'a self,
        session: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<Option<TimestampMillis>, PortError>> {
        Box::pin(async move {
            match self
                .session_command_request(
                    session,
                    HostCommand::ShellEnvironmentStatus {
                        session_id: session.as_str().to_owned(),
                        process_id: process.as_str().to_owned(),
                    },
                )
                .await?
            {
                HostResponseValue::ShellEnvironmentStatus { observed_at_millis } => {
                    Ok(observed_at_millis.map(TimestampMillis::new))
                }
                _ => Err(PortError::new(
                    PortErrorKind::ProtocolViolation,
                    PortOperation::ResolveExecutionContext,
                    Some(session.clone()),
                    std::io::Error::other("invalid environment status response"),
                )),
            }
        })
    }

    fn resolve_directory<'a>(
        &'a self,
        launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<ResolvedExecutionContext, PortError>> {
        Box::pin(async move {
            let directory = super::environment::working_directory(launch).map_err(|source| {
                PortError::new(
                    PortErrorKind::InvalidData,
                    PortOperation::ResolveExecutionContext,
                    None,
                    source,
                )
            })?;
            Ok(ResolvedExecutionContext::new(
                BTreeMap::new(),
                directory.map(|p| p.to_string_lossy().into_owned()),
            ))
        })
    }

    fn resolve<'a>(
        &'a self,
        launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<ResolvedExecutionContext, PortError>> {
        Box::pin(async move {
            let environment = super::environment::load_environment(
                launch,
                self.owner.core.config.native.limits(),
            )
            .map_err(|source| {
                PortError::new(
                    PortErrorKind::InvalidData,
                    PortOperation::ResolveExecutionContext,
                    None,
                    source,
                )
            })?;
            let working_directory = super::environment::working_directory(launch)
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

impl HostedRuntimeCore {
    fn host_launch(
        &self,
        session_id: &SessionId,
        launch: &LaunchSpec,
    ) -> Result<HostLaunchSpec, PortError> {
        let mut host = HostLaunchSpec::from(launch);
        host.output_offset = self
            .output
            .session_status(session_id)
            .map_err(|source| {
                hosted_output_port_error(PortOperation::SpawnProcess, session_id, source)
            })?
            .map_or(0, |status| status.next_offset.value());
        Ok(host)
    }

    fn install_entry(
        &self,
        session_id: SessionId,
        process: ProcessInstance,
        binding: HostedProcessBinding,
        terminal_size: TerminalSize,
        output_capture_mode: OutputCaptureMode,
    ) -> Result<(), PortError> {
        let output_offset = self
            .output
            .session_status(&session_id)
            .map_err(|source| {
                PortError::new(
                    PortErrorKind::Other,
                    PortOperation::SpawnProcess,
                    Some(session_id.clone()),
                    source,
                )
            })?
            .map_or(OutputOffset::ZERO, |status| status.next_offset);
        self.output
            .begin_session(session_id.clone(), output_capture_mode, output_offset)
            .map_err(|source| {
                hosted_output_port_error(PortOperation::SpawnProcess, &session_id, source)
            })?;
        let screen = self
            .screens
            .prepare_session_with_capture_mode(
                session_id.clone(),
                terminal_size,
                output_offset,
                output_capture_mode,
            )
            .map_err(|source| {
                PortError::new(
                    PortErrorKind::CapacityExceeded,
                    PortOperation::SpawnProcess,
                    Some(session_id.clone()),
                    source,
                )
            })?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.entries.contains_key(&session_id) {
            return Err(PortError::new(
                PortErrorKind::Conflict,
                PortOperation::SpawnProcess,
                Some(session_id),
                RuntimeAdapterFailure("hosted process entry already exists"),
            ));
        }
        state.entries.insert(
            session_id,
            HostedProcessEntry::new(process, binding, terminal_size, output_capture_mode),
        );
        drop(state);
        screen.commit();
        Ok(())
    }

    fn install_or_validate_entry(
        &self,
        session_id: SessionId,
        process: ProcessInstance,
        binding: HostedProcessBinding,
        terminal_size: TerminalSize,
        output_capture_mode: OutputCaptureMode,
    ) -> Result<(), PortError> {
        {
            let state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(entry) = state.entries.get(&session_id) {
                return if entry.process == process
                    && entry.root_pid == binding.root_pid
                    && entry.root_start_ticks == binding.root_start_ticks
                    && (binding.attempt_id.is_none() || entry.attempt_id == binding.attempt_id)
                    && entry.output_capture_mode == output_capture_mode
                {
                    Ok(())
                } else {
                    Err(PortError::new(
                        PortErrorKind::Conflict,
                        PortOperation::InspectProcessRuntime,
                        Some(session_id),
                        RuntimeAdapterFailure("hosted process incarnation conflicts"),
                    ))
                };
            }
        }
        self.install_entry(
            session_id,
            process,
            binding,
            terminal_size,
            output_capture_mode,
        )
    }

    fn resize_entry(
        &self,
        session_id: &SessionId,
        process: &ProcessInstanceId,
        size: TerminalSize,
    ) -> Result<(), PortError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = state.entries.get_mut(session_id).ok_or_else(|| {
            PortError::new(
                PortErrorKind::NotFound,
                PortOperation::ResizeTerminal,
                Some(session_id.clone()),
                RuntimeAdapterFailure("hosted process entry is absent"),
            )
        })?;
        if entry.process.id != *process {
            return Err(PortError::new(
                PortErrorKind::Conflict,
                PortOperation::ResizeTerminal,
                Some(session_id.clone()),
                RuntimeAdapterFailure("hosted process incarnation does not match"),
            ));
        }
        entry.terminal_size = size;
        Ok(())
    }

    fn arm_exit(
        &self,
        session_id: &SessionId,
        process: &ProcessInstanceId,
    ) -> Result<(), PortError> {
        let retained = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let entry = exact_entry_mut(
                &mut state,
                session_id,
                process,
                PortOperation::ArmProcessExit,
            )?;
            if entry.armed {
                return Ok(());
            }
            entry.armed = true;
            let retained = entry.retained_exit.take();
            if retained.is_some() {
                entry.exit_published = true;
            }
            retained
        };
        if let Some(exit) = retained
            && let Err(source) = self.exits.try_send(exit.clone())
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Ok(entry) = exact_entry_mut(
                &mut state,
                session_id,
                process,
                PortOperation::ArmProcessExit,
            ) {
                entry.exit_published = false;
                entry.retained_exit = Some(exit);
            }
            return Err(PortError::new(
                PortErrorKind::CapacityExceeded,
                PortOperation::ArmProcessExit,
                Some(session_id.clone()),
                source,
            ));
        }
        Ok(())
    }

    fn remove_entry(&self, session_id: &SessionId, process: &ProcessInstanceId) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state
            .entries
            .get(session_id)
            .is_some_and(|entry| entry.process.id == *process)
        {
            state.entries.remove(session_id);
            self.cooperative_signals.remove_exact(session_id, process);
        }
    }

    fn observe_output(
        &self,
        output: HostOutputFrame,
    ) -> Option<(SessionId, ProcessInstanceId, u64)> {
        let session_id = SessionId::new(output.session_id).ok()?;
        let process_id = ProcessInstanceId::new(output.process_id).ok()?;
        let key = (session_id.clone(), process_id.clone());
        let mut ready = Vec::new();
        let (recovery_from, output_capture_mode) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let per_session_limit = self.config.native.output_limits().per_session_bytes;
            let Some(entry) = state.entries.get_mut(&session_id) else {
                state.dropped_output_frames = state.dropped_output_frames.saturating_add(1);
                return None;
            };
            if entry.process.id != process_id {
                state.dropped_output_frames = state.dropped_output_frames.saturating_add(1);
                return None;
            }
            entry.insert_output(output.start_offset, output.bytes, per_session_limit);
            entry.drain_ready(&mut ready);
            let recovery = entry
                .remote_next
                .filter(|expected| {
                    entry
                        .pending_output
                        .keys()
                        .next()
                        .is_some_and(|next| next > expected)
                })
                .filter(|_| !entry.recovery_active);
            if recovery.is_some() {
                entry.recovery_active = true;
            }
            (recovery, entry.output_capture_mode)
        };
        for (start, bytes) in ready {
            let Ok(appended) = self.append_ready_output(
                &session_id,
                output_capture_mode,
                OutputOffset::new(start),
                &bytes,
            ) else {
                return None;
            };
            self.record_output_metrics(&session_id, bytes.len(), appended.global_bytes_retained);
        }
        recovery_from.map(|from| (key.0, key.1, from))
    }

    fn observe_cooperative(&self, event: HostCooperativeEvent) {
        let (Ok(session_id), Ok(process_id)) = (
            SessionId::new(event.session_id),
            ProcessInstanceId::new(event.process_id),
        ) else {
            return;
        };
        let exact = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entries
            .get(&session_id)
            .is_some_and(|entry| entry.process.id == process_id);
        if exact {
            let observed_at = TimestampMillis::new(event.observed_at_millis);
            self.cooperative_signals.record(
                session_id.clone(),
                process_id,
                event.state.into(),
                observed_at,
            );
            if let Some(wake) = &self.foreground_wake
                && let Err(error) = wake.wake_foreground(&session_id, observed_at)
            {
                self.telemetry.emit_diagnostic(&DiagnosticRecord {
                    event: DiagnosticEventName::PortOperationFailed,
                    level: DiagnosticLevel::for_port_error(error.kind()),
                    timestamp: self.clock.now(),
                    session_id: Some(session_id),
                    application_operation: Some(ApplicationOperation::ObserveForeground),
                    application_error: Some(error.category()),
                    operation: Some(error.operation()),
                    error_kind: Some(error.kind()),
                });
            }
        }
    }

    fn establish_replay(
        &self,
        session_id: &SessionId,
        process_id: &ProcessInstanceId,
        available_from: u64,
    ) -> Result<Option<u64>, PortError> {
        let output_capture_mode =
            self.capture_mode(session_id, process_id, PortOperation::ReadTerminalOutput)?;
        self.output
            .establish_retention_gap(
                session_id,
                output_capture_mode,
                OutputOffset::new(available_from),
            )
            .map_err(|source| {
                hosted_output_port_error(PortOperation::ReadTerminalOutput, session_id, source)
            })?;
        let local_next = self
            .output
            .session_status(session_id)
            .map_err(|source| {
                hosted_output_port_error(PortOperation::ReadTerminalOutput, session_id, source)
            })?
            .ok_or_else(|| {
                PortError::new(
                    PortErrorKind::NotFound,
                    PortOperation::ReadTerminalOutput,
                    Some(session_id.clone()),
                    RuntimeAdapterFailure("hosted output session is absent"),
                )
            })?
            .next_offset
            .value();
        let mut ready = Vec::new();
        let next_recovery = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let entry = exact_entry_mut(
                &mut state,
                session_id,
                process_id,
                PortOperation::ReadTerminalOutput,
            )?;
            let next = entry
                .remote_next
                .unwrap_or(local_next)
                .max(local_next)
                .max(available_from);
            entry.recovery_active = false;
            entry.rebase_output(next, self.config.native.output_limits().per_session_bytes);
            entry.drain_ready(&mut ready);
            let recovery = entry.remote_next.filter(|expected| {
                entry
                    .pending_output
                    .keys()
                    .next()
                    .is_some_and(|next| next > expected)
            });
            if recovery.is_some() {
                entry.recovery_active = true;
            }
            recovery
        };
        for (start, bytes) in ready {
            let appended = self.append_ready_output(
                session_id,
                output_capture_mode,
                OutputOffset::new(start),
                &bytes,
            )?;
            self.record_output_metrics(session_id, bytes.len(), appended.global_bytes_retained);
        }
        Ok(next_recovery)
    }

    fn capture_mode(
        &self,
        session_id: &SessionId,
        process_id: &ProcessInstanceId,
        operation: PortOperation,
    ) -> Result<OutputCaptureMode, PortError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        exact_entry_mut(&mut state, session_id, process_id, operation)
            .map(|entry| entry.output_capture_mode)
    }

    fn append_ready_output(
        &self,
        session_id: &SessionId,
        output_capture_mode: OutputCaptureMode,
        start: OutputOffset,
        bytes: &[u8],
    ) -> Result<undercurrent_application::OutputAppend, PortError> {
        match output_capture_mode {
            OutputCaptureMode::Terminal => {
                let next = self
                    .output
                    .session_status(session_id)
                    .map_err(|source| {
                        hosted_output_port_error(
                            PortOperation::ReadTerminalOutput,
                            session_id,
                            source,
                        )
                    })?
                    .ok_or_else(|| {
                        PortError::new(
                            PortErrorKind::NotFound,
                            PortOperation::ReadTerminalOutput,
                            Some(session_id.clone()),
                            RuntimeAdapterFailure("hosted output session is absent"),
                        )
                    })?
                    .next_offset;
                if next != start {
                    return Err(PortError::new(
                        PortErrorKind::InvalidData,
                        PortOperation::ReadTerminalOutput,
                        Some(session_id.clone()),
                        RuntimeAdapterFailure("hosted terminal output checkpoint diverged"),
                    ));
                }
                self.screens
                    .observe_output(self.output.as_ref(), session_id, bytes)
            }
            OutputCaptureMode::Opaque => self
                .output
                .append_at_checkpoint(session_id, start, bytes)
                .map_err(|source| {
                    hosted_output_port_error(PortOperation::ReadTerminalOutput, session_id, source)
                }),
        }
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

    fn observe_exit(&self, event: HostExitEvent) {
        let Ok(session_id) = SessionId::new(event.session_id) else {
            return;
        };
        let Ok(process_id) = ProcessInstanceId::new(event.process_id) else {
            return;
        };
        let outcome = if event.supervision_failed {
            ProcessOutcome::SupervisionFailed
        } else if let Some(exit_code) = event.exit_code {
            ProcessOutcome::Exited(exit_code)
        } else {
            return;
        };
        let observation = ProcessExitObservation {
            session_id: session_id.clone(),
            process_id: process_id.clone(),
            outcome,
            observed_at: TimestampMillis::new(event.observed_at_millis),
        };
        let publish = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(entry) = state.entries.get_mut(&session_id) else {
                return;
            };
            if entry.process.id != process_id {
                return;
            }
            if entry.exit_published {
                return;
            }
            if entry.armed {
                entry.exit_published = true;
                true
            } else {
                entry.retained_exit = Some(observation.clone());
                false
            }
        };
        if publish {
            let _ = self.exits.try_send(observation);
        }
    }
}

impl HostedProcessEntry {
    fn new(
        process: ProcessInstance,
        binding: HostedProcessBinding,
        terminal_size: TerminalSize,
        output_capture_mode: OutputCaptureMode,
    ) -> Self {
        Self {
            process,
            root_pid: binding.root_pid,
            root_start_ticks: binding.root_start_ticks,
            attempt_id: binding.attempt_id,
            terminal_size,
            output_capture_mode,
            remote_next: None,
            pending_output: BTreeMap::new(),
            pending_output_bytes: 0,
            recovery_active: false,
            armed: false,
            retained_exit: None,
            exit_published: false,
        }
    }

    fn insert_output(&mut self, start: u64, mut bytes: Vec<u8>, byte_limit: usize) {
        if bytes.is_empty() {
            return;
        }
        if let Some(expected) = self.remote_next {
            let end = start.saturating_add(bytes.len() as u64);
            if end <= expected {
                return;
            }
            if start < expected {
                let skip = usize::try_from(expected - start).unwrap_or(bytes.len());
                bytes.drain(..skip.min(bytes.len()));
                return self.insert_output(expected, bytes, byte_limit);
            }
        }
        if bytes.len() > byte_limit {
            bytes.truncate(byte_limit);
        }
        if let Some(previous) = self.pending_output.insert(start, bytes) {
            self.pending_output_bytes = self.pending_output_bytes.saturating_sub(previous.len());
        }
        self.pending_output_bytes = self
            .pending_output
            .get(&start)
            .map_or(self.pending_output_bytes, |bytes| {
                self.pending_output_bytes.saturating_add(bytes.len())
            });
        while self.pending_output_bytes > byte_limit {
            let Some(last) = self.pending_output.keys().next_back().copied() else {
                break;
            };
            if let Some(removed) = self.pending_output.remove(&last) {
                self.pending_output_bytes = self.pending_output_bytes.saturating_sub(removed.len());
            }
        }
    }

    fn rebase_output(&mut self, next: u64, byte_limit: usize) {
        self.remote_next = Some(next);
        let pending = std::mem::take(&mut self.pending_output);
        self.pending_output_bytes = 0;
        for (start, bytes) in pending {
            self.insert_output(start, bytes, byte_limit);
        }
    }

    fn drain_ready(&mut self, ready: &mut Vec<(u64, Vec<u8>)>) {
        let Some(mut expected) = self.remote_next else {
            return;
        };
        while let Some(bytes) = self.pending_output.remove(&expected) {
            self.pending_output_bytes = self.pending_output_bytes.saturating_sub(bytes.len());
            let start = expected;
            expected = expected.saturating_add(bytes.len() as u64);
            ready.push((start, bytes));
        }
        self.remote_next = Some(expected);
    }
}

fn validate_host_process_identity(
    root_pid: u32,
    root_start_ticks: Option<u64>,
) -> Result<(), RuntimeAdapterFailure> {
    if root_pid == 0 {
        return Err(RuntimeAdapterFailure(
            "session host omitted the workload root process identity",
        ));
    }
    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
    if !matches!(root_start_ticks, Some(value) if value > 0) {
        return Err(RuntimeAdapterFailure(
            "session host omitted the workload incarnation",
        ));
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    let _ = root_start_ticks;
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Clone, Copy, PartialEq, Eq)]
struct AgentProcIdentity {
    pid: u32,
    parent_pid: u32,
    start_ticks: u64,
}

#[cfg(target_os = "linux")]
fn agent_proc_identity(pid: u32) -> Result<AgentProcIdentity, HostedAgentPeerError> {
    let mut bytes = Vec::new();
    fs::File::open(format!("/proc/{pid}/stat"))
        .map_err(HostedAgentPeerError::ProcessTable)?
        .take(MAX_PROC_STAT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(HostedAgentPeerError::ProcessTable)?;
    if bytes.len() as u64 > MAX_PROC_STAT_BYTES {
        return Err(HostedAgentPeerError::ProcessTable(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "process stat exceeded its bound",
        )));
    }
    let stat = std::str::from_utf8(&bytes).map_err(|source| {
        HostedAgentPeerError::ProcessTable(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            source,
        ))
    })?;
    let (prefix, fields) = stat.rsplit_once(')').ok_or_else(|| {
        HostedAgentPeerError::ProcessTable(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "process stat is malformed",
        ))
    })?;
    let observed_pid = prefix
        .split_once('(')
        .map(|(pid, _)| pid.trim())
        .ok_or_else(|| {
            HostedAgentPeerError::ProcessTable(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "process stat identity is malformed",
            ))
        })?
        .parse::<u32>()
        .map_err(|source| {
            HostedAgentPeerError::ProcessTable(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                source,
            ))
        })?;
    let fields = fields.split_whitespace().collect::<Vec<_>>();
    if observed_pid != pid || fields.len() <= 19 {
        return Err(HostedAgentPeerError::ProcessTable(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "process stat fields are incomplete",
        )));
    }
    let parent_pid = fields[1].parse::<u32>().map_err(|source| {
        HostedAgentPeerError::ProcessTable(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            source,
        ))
    })?;
    let start_ticks = fields[19].parse::<u64>().map_err(|source| {
        HostedAgentPeerError::ProcessTable(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            source,
        ))
    })?;
    if start_ticks == 0 {
        return Err(HostedAgentPeerError::ProcessTable(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "process start time is zero",
        )));
    }
    Ok(AgentProcIdentity {
        pid,
        parent_pid,
        start_ticks,
    })
}

fn exact_entry_mut<'a>(
    state: &'a mut HostedRuntimeState,
    session_id: &SessionId,
    process: &ProcessInstanceId,
    operation: PortOperation,
) -> Result<&'a mut HostedProcessEntry, PortError> {
    let entry = state.entries.get_mut(session_id).ok_or_else(|| {
        PortError::new(
            PortErrorKind::NotFound,
            operation,
            Some(session_id.clone()),
            RuntimeAdapterFailure("hosted process entry is absent"),
        )
    })?;
    if entry.process.id != *process {
        return Err(PortError::new(
            PortErrorKind::Conflict,
            operation,
            Some(session_id.clone()),
            RuntimeAdapterFailure("hosted process incarnation does not match"),
        ));
    }
    Ok(entry)
}

async fn recover_output(
    core: &Arc<HostedRuntimeCore>,
    client: &Arc<HostClient>,
    session_id: SessionId,
    process_id: ProcessInstanceId,
    mut from: u64,
) -> Result<(), PortError> {
    loop {
        let response = client
            .request(HostCommand::Replay {
                session_id: session_id.as_str().to_owned(),
                process_id: process_id.as_str().to_owned(),
                from,
                max_bytes: core.config.native.output_limits().replay_bytes,
            })
            .await
            .map_err(|failure| {
                failure.into_port_error(PortOperation::ReadTerminalOutput, Some(session_id.clone()))
            })?;
        let HostResponseValue::Replay {
            available_from,
            delivered_to,
            next_offset,
            recovery,
            has_more,
            ..
        } = response
        else {
            return Err(unexpected_response(
                PortOperation::ReadTerminalOutput,
                Some(session_id),
            ));
        };
        let output_capture_mode =
            core.capture_mode(&session_id, &process_id, PortOperation::ReadTerminalOutput)?;
        if available_from > from
            && matches!(recovery, HostGapRecovery::RequestScreenSnapshot)
            && output_capture_mode == OutputCaptureMode::Terminal
        {
            from = restore_screen_checkpoint(core, client, &session_id, &process_id).await?;
            continue;
        }
        let next_recovery = core.establish_replay(&session_id, &process_id, available_from)?;
        if let Some(next_recovery) = next_recovery {
            from = next_recovery;
            continue;
        }
        if has_more && delivered_to < next_offset {
            if delivered_to == from {
                return Err(PortError::new(
                    PortErrorKind::CapacityExceeded,
                    PortOperation::ReadTerminalOutput,
                    Some(session_id),
                    RuntimeAdapterFailure("session-host replay made no progress"),
                ));
            }
            from = delivered_to;
            continue;
        }
        return Ok(());
    }
}

async fn recover_screen_and_output(
    core: &Arc<HostedRuntimeCore>,
    client: &Arc<HostClient>,
    session_id: SessionId,
    process_id: ProcessInstanceId,
) -> Result<(), PortError> {
    let output_offset = restore_screen_checkpoint(core, client, &session_id, &process_id).await?;
    recover_output(core, client, session_id, process_id, output_offset).await
}

async fn restore_screen_checkpoint(
    core: &Arc<HostedRuntimeCore>,
    client: &Arc<HostClient>,
    session_id: &SessionId,
    process_id: &ProcessInstanceId,
) -> Result<u64, PortError> {
    let response = client
        .request(HostCommand::Snapshot {
            session_id: session_id.as_str().to_owned(),
            process_id: process_id.as_str().to_owned(),
        })
        .await
        .map_err(|failure| {
            failure.into_port_error(PortOperation::ReadTerminalScreen, Some(session_id.clone()))
        })?;
    let HostResponseValue::ScreenSnapshot {
        output_offset,
        columns,
        rows,
        cursor_row,
        cursor_column,
        cursor_visible,
        alternate_screen,
        contents,
    } = response
    else {
        return Err(unexpected_response(
            PortOperation::ReadTerminalScreen,
            Some(session_id.clone()),
        ));
    };
    let snapshot = TerminalScreenSnapshot::new(
        OutputOffset::new(output_offset),
        columns,
        rows,
        cursor_row,
        cursor_column,
        cursor_visible,
        alternate_screen,
        Arc::from(contents.into_bytes()),
    )
    .map_err(|source| {
        PortError::new(
            PortErrorKind::ProtocolViolation,
            PortOperation::ReadTerminalScreen,
            Some(session_id.clone()),
            source,
        )
    })?;
    core.output
        .establish_retention_gap(
            session_id,
            OutputCaptureMode::Terminal,
            OutputOffset::new(output_offset),
        )
        .map_err(|source| {
            hosted_output_port_error(PortOperation::ReadTerminalScreen, session_id, source)
        })?;
    let local_offset = core
        .output
        .session_status(session_id)
        .map_err(|source| {
            PortError::new(
                PortErrorKind::Other,
                PortOperation::ReadTerminalScreen,
                Some(session_id.clone()),
                source,
            )
        })?
        .map_or(OutputOffset::ZERO, |status| status.next_offset);
    // Live output can advance while the snapshot request is in flight. Keep the
    // newer local projection when its checkpoint is already ahead of the snapshot.
    // See [output recovery](../../docs/internals/host/recovery.md#recover-output-using-absolute-checkpoints).
    if local_offset.value() == output_offset {
        core.screens
            .restore_snapshot_at(session_id.clone(), &snapshot, local_offset)
            .map_err(|source| {
                PortError::new(
                    PortErrorKind::InvalidData,
                    PortOperation::ReadTerminalScreen,
                    Some(session_id.clone()),
                    source,
                )
            })?;
    }
    Ok(local_offset.value())
}

fn start_control_dispatcher(
    core: &Arc<HostedRuntimeCore>,
    client: &Arc<HostClient>,
    events: async_channel::Receiver<HostControlEvent>,
) -> Result<(), HostedProcessRuntimeError> {
    let core = Arc::clone(core);
    let client = Arc::clone(client);
    thread::Builder::new()
        .name("undercurrent-host-control".to_owned())
        .stack_size(core.config.native.limits().worker_thread_stack_bytes)
        .spawn(move || {
            while let Ok(event) = events.recv_blocking() {
                match event {
                    HostControlEvent::Exit(exit) => {
                        block_on(observe_exit_after_output(&core, &client, exit));
                    }
                    HostControlEvent::Cooperative(event) => core.observe_cooperative(event),
                    HostControlEvent::Reconnected { host_epoch } => {
                        let changed = {
                            let mut state = core
                                .state
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner());
                            let changed = state.host_epoch != host_epoch;
                            state.host_epoch = host_epoch;
                            changed
                        };
                        recover_entries(&core, &client, changed);
                    }
                }
            }
        })
        .map(|_| ())
        .map_err(HostedProcessRuntimeError::Worker)
}

fn start_output_dispatcher(
    core: &Arc<HostedRuntimeCore>,
    client: &Arc<HostClient>,
    events: async_channel::Receiver<HostOutputEvent>,
) -> Result<(), HostedProcessRuntimeError> {
    let core = Arc::clone(core);
    let client = Arc::clone(client);
    thread::Builder::new()
        .name("undercurrent-host-output".to_owned())
        .stack_size(core.config.native.limits().worker_thread_stack_bytes)
        .spawn(move || {
            while let Ok(event) = events.recv_blocking() {
                match event {
                    HostOutputEvent::Frame(output) => {
                        if let Some((session_id, process_id, from)) = core.observe_output(output) {
                            spawn_recovery(&core, &client, session_id, process_id, from);
                        }
                    }
                    HostOutputEvent::Barrier(ready) => {
                        let _ = ready.try_send(());
                    }
                }
            }
        })
        .map(|_| ())
        .map_err(HostedProcessRuntimeError::Worker)
}

async fn observe_exit_after_output(
    core: &Arc<HostedRuntimeCore>,
    client: &Arc<HostClient>,
    event: HostExitEvent,
) {
    let (Ok(session_id), Ok(process_id)) = (
        SessionId::new(event.session_id.clone()),
        ProcessInstanceId::new(event.process_id.clone()),
    ) else {
        return;
    };
    let Some(mut local_offset) = core
        .output
        .session_status(&session_id)
        .ok()
        .flatten()
        .map(|status| status.next_offset.value())
    else {
        return;
    };
    if local_offset < event.output_offset {
        if recover_output(core, client, session_id.clone(), process_id, local_offset)
            .await
            .is_err()
            || client.drain_output_events().await.is_err()
        {
            return;
        }
        let Some(recovered_offset) = core
            .output
            .session_status(&session_id)
            .ok()
            .flatten()
            .map(|status| status.next_offset.value())
        else {
            return;
        };
        local_offset = recovered_offset;
    }
    if local_offset >= event.output_offset {
        core.observe_exit(event);
    }
}

fn spawn_recovery(
    core: &Arc<HostedRuntimeCore>,
    client: &Arc<HostClient>,
    session_id: SessionId,
    process_id: ProcessInstanceId,
    from: u64,
) {
    let core = Arc::clone(core);
    let client = Arc::clone(client);
    let stack = core.config.native.limits().worker_thread_stack_bytes;
    let _ = thread::Builder::new()
        .name("undercurrent-host-replay".to_owned())
        .stack_size(stack)
        .spawn(move || {
            let _ = block_on(recover_output(&core, &client, session_id, process_id, from));
        });
}

fn recover_entries(core: &Arc<HostedRuntimeCore>, client: &Arc<HostClient>, epoch_changed: bool) {
    let entries: Vec<_> = core
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .entries
        .iter()
        .map(|(session_id, entry)| {
            (
                session_id.clone(),
                entry.process.clone(),
                entry.armed,
                entry.output_capture_mode,
            )
        })
        .collect();
    for (session_id, process, armed, output_capture_mode) in entries {
        if epoch_changed {
            if armed {
                core.observe_exit(HostExitEvent {
                    session_id: session_id.as_str().to_owned(),
                    process_id: process.id.as_str().to_owned(),
                    output_offset: core
                        .output
                        .session_status(&session_id)
                        .ok()
                        .flatten()
                        .map_or(0, |status| status.next_offset.value()),
                    exit_code: None,
                    supervision_failed: true,
                    observed_at_millis: core.clock.now().value(),
                });
            }
            continue;
        }
        let core = Arc::clone(core);
        let client = Arc::clone(client);
        let stack = core.config.native.limits().worker_thread_stack_bytes;
        let _ = thread::Builder::new()
            .name("undercurrent-host-reattach".to_owned())
            .stack_size(stack)
            .spawn(move || {
                let _ = match output_capture_mode {
                    OutputCaptureMode::Terminal => block_on(recover_screen_and_output(
                        &core, &client, session_id, process.id,
                    )),
                    OutputCaptureMode::Opaque => {
                        let from = core
                            .output
                            .session_status(&session_id)
                            .ok()
                            .flatten()
                            .map_or(0, |status| status.next_offset.value());
                        block_on(recover_output(&core, &client, session_id, process.id, from))
                    }
                };
            });
    }
}

fn expect_completed(
    response: HostResponseValue,
    operation: PortOperation,
    session_id: &SessionId,
) -> Result<(), PortError> {
    if matches!(response, HostResponseValue::Completed) {
        Ok(())
    } else {
        Err(unexpected_response(operation, Some(session_id.clone())))
    }
}

fn host_attempt_context(context: &ProcessMutationContext) -> HostAttemptContext {
    HostAttemptContext {
        attempt_id: context.attempt_id().as_str().to_owned(),
        session_id: context
            .target_session()
            .map(|session| session.as_str().to_owned()),
        fencing_scope: Some(process_fencing_scope(
            context.target_session(),
            context
                .dispatch_binding()
                .map(|binding| binding.caller_cell_identity().as_str()),
        )),
        kind: host_attempt_kind(context.kind()),
        fencing_token: context.fencing_token().value(),
        payload_digest: context.payload_digest().to_hex(),
        accepted_at_millis: context.accepted_at().value(),
    }
}

fn host_attempt_context_from_attempt(attempt: &OperationAttempt) -> HostAttemptContext {
    HostAttemptContext {
        attempt_id: attempt.id().as_str().to_owned(),
        session_id: attempt
            .target_session()
            .map(|session| session.as_str().to_owned()),
        fencing_scope: Some(process_fencing_scope(
            attempt.target_session(),
            attempt
                .dispatch_binding()
                .map(|binding| binding.caller_cell_identity().as_str()),
        )),
        kind: host_attempt_kind(attempt.kind()),
        fencing_token: attempt.fencing_token().value(),
        payload_digest: attempt.payload_digest().to_hex(),
        accepted_at_millis: attempt.accepted_at().value(),
    }
}

fn process_fencing_scope(
    target_session: Option<&SessionId>,
    dispatch_cell_identity: Option<&str>,
) -> String {
    if let Some(identity) = dispatch_cell_identity {
        format!("dispatch:{identity}")
    } else if let Some(session) = target_session {
        format!("session:{}", session.as_str())
    } else {
        "host".to_owned()
    }
}

const fn host_attempt_kind(kind: ProcessMutationKind) -> HostAttemptKind {
    match kind {
        ProcessMutationKind::Spawn => HostAttemptKind::Spawn,
        ProcessMutationKind::ArmExit => HostAttemptKind::ArmExit,
        ProcessMutationKind::Stop => HostAttemptKind::Stop,
        ProcessMutationKind::ForceStop => HostAttemptKind::ForceStop,
        ProcessMutationKind::WriteInput => HostAttemptKind::WriteInput,
        ProcessMutationKind::Resize => HostAttemptKind::Resize,
        ProcessMutationKind::Shutdown => HostAttemptKind::Shutdown,
        ProcessMutationKind::ExecuteInSession => HostAttemptKind::ExecuteInSession,
    }
}

const fn effect_certainty(certainty: HostEffectCertainty) -> EffectCertainty {
    match certainty {
        HostEffectCertainty::KnownAbsent => EffectCertainty::KnownAbsent,
        HostEffectCertainty::KnownApplied => EffectCertainty::KnownApplied,
        HostEffectCertainty::Unknown => EffectCertainty::Unknown,
    }
}

fn decode_termination(
    response: HostResponseValue,
    operation: PortOperation,
    session_id: &SessionId,
) -> Result<ProcessTermination, PortError> {
    let HostResponseValue::Terminated {
        kind,
        exit_code,
        observed_at_millis,
    } = response
    else {
        return Err(unexpected_response(operation, Some(session_id.clone())));
    };
    Ok(ProcessTermination::new(
        kind.into(),
        exit_code,
        TimestampMillis::new(observed_at_millis),
    ))
}

fn unexpected_response(operation: PortOperation, session_id: Option<SessionId>) -> PortError {
    PortError::new(
        PortErrorKind::ProtocolViolation,
        operation,
        session_id,
        RuntimeAdapterFailure("session host returned an unexpected response kind"),
    )
}

fn hosted_output_port_error(
    operation: PortOperation,
    session_id: &SessionId,
    source: TerminalOutputError,
) -> PortError {
    let kind = match source {
        TerminalOutputError::SessionCapacity { .. }
        | TerminalOutputError::AppendCapacity { .. }
        | TerminalOutputError::ReplayCapacity { .. }
        | TerminalOutputError::SubscriptionCapacity { .. } => PortErrorKind::CapacityExceeded,
        TerminalOutputError::CheckpointMismatch { .. }
        | TerminalOutputError::CaptureModeMismatch { .. } => PortErrorKind::Conflict,
        _ => PortErrorKind::Other,
    };
    PortError::new(kind, operation, Some(session_id.clone()), source)
}

struct HostClient {
    config: HostedProcessRuntimeConfig,
    codec: HostCodec,
    token: Mutex<String>,
    writer: Mutex<Option<UnixStream>>,
    pending: Mutex<BTreeMap<u64, PendingHostRequest>>,
    next_request: AtomicU64,
    host_epoch: Mutex<String>,
    control_events: async_channel::Sender<HostControlEvent>,
    output_events: async_channel::Sender<HostOutputEvent>,
    spawn_cancellations: async_channel::Sender<String>,
    unaccepted_spawns: Mutex<BTreeSet<String>>,
    retiring: AtomicBool,
    closed: AtomicBool,
}

struct PendingHostRequest {
    reply: async_channel::Sender<Result<HostClientReply, HostRequestError>>,
    spawn: Option<HostSpawnCancellation>,
}

struct HostClientReply {
    value: HostResponseValue,
    spawn: Option<HostSpawnCancellation>,
}

struct HostSpawnCancellation {
    token: String,
    client: std::sync::Weak<HostClient>,
    local: Option<(
        std::sync::Weak<HostedRuntimeCore>,
        SessionId,
        ProcessInstanceId,
    )>,
    accepted: bool,
}

impl HostSpawnCancellation {
    fn track_local(
        &mut self,
        core: &Arc<HostedRuntimeCore>,
        session_id: SessionId,
        process_id: ProcessInstanceId,
    ) {
        self.local = Some((Arc::downgrade(core), session_id, process_id));
    }

    fn accept(mut self) {
        if let Some(client) = self.client.upgrade() {
            client.remove_unaccepted_spawn(&self.token);
        }
        self.accepted = true;
    }
}

impl Drop for HostSpawnCancellation {
    fn drop(&mut self) {
        if !self.accepted {
            if let Some((core, session_id, process_id)) = self.local.take()
                && let Some(core) = core.upgrade()
            {
                core.remove_entry(&session_id, &process_id);
            }
            if let Some(client) = self.client.upgrade() {
                client.remove_unaccepted_spawn(&self.token);
                client.cancel_spawn(self.token.clone());
            }
        }
    }
}

impl HostClient {
    fn start(
        config: HostedProcessRuntimeConfig,
        control_events: async_channel::Sender<HostControlEvent>,
        output_events: async_channel::Sender<HostOutputEvent>,
        stack_bytes: usize,
    ) -> Result<Arc<Self>, HostClientError> {
        let codec = host_codec(&config)?;
        let (stream, token, host_epoch) = connect_or_launch(&config, codec)?;
        let writer = stream.try_clone().map_err(HostClientError::io)?;
        let cancellation_capacity = config
            .native
            .limits()
            .max_processes
            .saturating_add(config.native.limits().spawn_queue_capacity)
            .max(1);
        let (spawn_cancellations, cancellation_receiver) =
            async_channel::bounded(cancellation_capacity);
        let client = Arc::new(Self {
            config,
            codec,
            token: Mutex::new(token),
            writer: Mutex::new(Some(writer)),
            pending: Mutex::new(BTreeMap::new()),
            next_request: AtomicU64::new(0),
            host_epoch: Mutex::new(host_epoch),
            control_events,
            output_events,
            spawn_cancellations,
            unaccepted_spawns: Mutex::new(BTreeSet::new()),
            retiring: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        });
        let reader = Arc::clone(&client);
        thread::Builder::new()
            .name("undercurrent-host-reader".to_owned())
            .stack_size(stack_bytes)
            .spawn(move || reader.read_loop(stream))
            .map_err(HostClientError::io)?;
        let cancellation_client = Arc::downgrade(&client);
        thread::Builder::new()
            .name("undercurrent-host-spawn-cancellations".to_owned())
            .stack_size(stack_bytes)
            .spawn(move || {
                while let Ok(spawn_token) = cancellation_receiver.recv_blocking() {
                    let Some(client) = cancellation_client.upgrade() else {
                        return;
                    };
                    loop {
                        if client.closed.load(Ordering::Acquire) {
                            return;
                        }
                        if block_on(client.request(HostCommand::CancelSpawn {
                            spawn_token: spawn_token.clone(),
                        }))
                        .is_ok()
                        {
                            break;
                        }
                        thread::sleep(client.config.retry_initial);
                    }
                }
            })
            .map_err(HostClientError::io)?;
        Ok(client)
    }

    fn host_epoch(&self) -> String {
        self.host_epoch
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn pending_count(&self) -> usize {
        self.pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    fn begin_retirement(&self) {
        self.retiring.store(true, Ordering::Release);
    }

    fn cancel_retirement(&self) {
        self.retiring.store(false, Ordering::Release);
    }

    async fn request(&self, command: HostCommand) -> Result<HostResponseValue, HostRequestError> {
        Ok(self.request_inner(command, None).await?.value)
    }

    async fn spawn_request(
        self: &Arc<Self>,
        spawn_token: String,
        session_id: String,
        launch: HostLaunchSpec,
        secret_environment: HostResolvedSecretEnvironment,
    ) -> Result<(HostResponseValue, HostSpawnCancellation), HostRequestError> {
        if !self
            .unaccepted_spawns
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(spawn_token.clone())
        {
            return Err(HostRequestError::Capacity);
        }
        let guard = HostSpawnCancellation {
            token: spawn_token.clone(),
            client: Arc::downgrade(self),
            local: None,
            accepted: false,
        };
        let mut reply = self
            .request_inner(
                HostCommand::Spawn {
                    spawn_token,
                    session_id,
                    launch: Box::new(launch),
                    secret_environment,
                },
                Some(guard),
            )
            .await?;
        let guard = reply.spawn.take().ok_or_else(|| {
            HostRequestError::Protocol(Arc::new(HostProtocolError::InvalidDomainValue))
        })?;
        Ok((reply.value, guard))
    }

    async fn request_inner(
        &self,
        command: HostCommand,
        spawn: Option<HostSpawnCancellation>,
    ) -> Result<HostClientReply, HostRequestError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(HostRequestError::Unavailable);
        }
        let request_id = self
            .next_request
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map(|previous| previous + 1)
            .map_err(|_| HostRequestError::Capacity)?;
        let mut encoded = match command {
            HostCommand::WriteInput {
                session_id,
                process_id,
                bytes,
                pacing,
            } => self.codec.encode_input(&HostInputFrame {
                request_id,
                attempt: None,
                session_id,
                process_id,
                bytes,
                pacing,
            }),
            HostCommand::AttemptMutation {
                attempt,
                mutation:
                    HostAttemptMutation::WriteInput {
                        session_id,
                        process_id,
                        bytes,
                        pacing,
                    },
            } => self.codec.encode_input(&HostInputFrame {
                request_id,
                attempt: Some(attempt),
                session_id,
                process_id,
                bytes,
                pacing,
            }),
            command => self.codec.encode_request(&HostRequest {
                request_id,
                command,
            }),
        }
        .map_err(|source| HostRequestError::Protocol(Arc::new(source)))?;
        let (reply, receiver) = async_channel::bounded(1);
        self.pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(request_id, PendingHostRequest { reply, spawn });
        let write = self
            .writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_mut()
            .ok_or(HostRequestError::Unavailable)
            .and_then(|stream| {
                stream
                    .write_all(&encoded)
                    .map_err(|source| HostRequestError::Io(Arc::new(source)))
            });
        encoded.fill(0);
        if let Err(error) = write {
            self.pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&request_id);
            return Err(error);
        }
        receiver
            .recv()
            .await
            .map_err(|_| HostRequestError::Unavailable)?
    }

    fn cancel_spawn(&self, spawn_token: String) {
        if self.spawn_cancellations.try_send(spawn_token).is_err()
            && let Some(stream) = self
                .writer
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_ref()
        {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
    }

    fn remove_unaccepted_spawn(&self, spawn_token: &str) {
        self.unaccepted_spawns
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(spawn_token);
    }

    async fn drain_output_events(&self) -> Result<(), ()> {
        let (ready, drained) = async_channel::bounded(1);
        self.output_events
            .send(HostOutputEvent::Barrier(ready))
            .await
            .map_err(|_| ())?;
        drained.recv().await.map_err(|_| ())
    }

    fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let unaccepted = std::mem::take(
            &mut *self
                .unaccepted_spawns
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        if let Some(stream) = self
            .writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_mut()
        {
            for spawn_token in unaccepted {
                let Ok(request_id) = self
                    .next_request
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                        current.checked_add(1)
                    })
                    .map(|previous| previous + 1)
                else {
                    break;
                };
                let request = HostRequest {
                    request_id,
                    command: HostCommand::CancelSpawn { spawn_token },
                };
                let Ok(encoded) = self.codec.encode_request(&request) else {
                    break;
                };
                if stream.write_all(&encoded).is_err() {
                    break;
                }
            }
        }
        self.control_events.close();
        self.output_events.close();
        self.spawn_cancellations.close();
        if let Some(stream) = self
            .writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
        self.fail_pending(HostRequestError::Unavailable);
    }

    fn read_loop(self: Arc<Self>, mut stream: UnixStream) {
        loop {
            match read_host_frame(&mut stream, self.codec) {
                Ok(HostDecodedFrame::Response(response)) => {
                    if let Some(mut pending) = self
                        .pending
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .remove(&response.request_id)
                    {
                        let result = response
                            .result
                            .map(|value| HostClientReply {
                                value,
                                spawn: pending.spawn.take(),
                            })
                            .map_err(HostRequestError::Remote);
                        let _ = pending.reply.try_send(result);
                    }
                }
                Ok(HostDecodedFrame::Exit(exit)) => {
                    if block_on(self.drain_output_events()).is_err() {
                        return;
                    }
                    if self
                        .control_events
                        .send_blocking(HostControlEvent::Exit(exit))
                        .is_err()
                    {
                        return;
                    }
                }
                Ok(HostDecodedFrame::Output(output)) => {
                    if self
                        .output_events
                        .send_blocking(HostOutputEvent::Frame(output))
                        .is_err()
                    {
                        return;
                    }
                }
                Ok(HostDecodedFrame::Cooperative(event)) => {
                    let _ = self
                        .control_events
                        .try_send(HostControlEvent::Cooperative(event));
                }
                Ok(HostDecodedFrame::Request(_) | HostDecodedFrame::Input(_)) | Err(_) => {
                    if self.closed.load(Ordering::Acquire) {
                        return;
                    }
                    self.writer
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .take();
                    self.fail_pending(HostRequestError::Unavailable);
                    let reconnect = if self.retiring.load(Ordering::Acquire) {
                        reconnect_existing(&self.config, self.codec, &self.token)
                    } else {
                        reconnect(&self.config, self.codec, &self.token)
                    };
                    match reconnect {
                        Ok((reconnected, host_epoch)) => {
                            let Ok(writer) = reconnected.try_clone() else {
                                return;
                            };
                            *self
                                .writer
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(writer);
                            *self
                                .host_epoch
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                                host_epoch.clone();
                            if self
                                .control_events
                                .send_blocking(HostControlEvent::Reconnected { host_epoch })
                                .is_err()
                            {
                                return;
                            }
                            stream = reconnected;
                        }
                        Err(_) => return,
                    }
                }
            }
        }
    }

    fn fail_pending(&self, error: HostRequestError) {
        let pending = std::mem::take(
            &mut *self
                .pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        for (_, pending) in pending {
            let _ = pending.reply.try_send(Err(error.clone()));
        }
    }
}

#[derive(Debug)]
enum HostControlEvent {
    Exit(HostExitEvent),
    Cooperative(HostCooperativeEvent),
    Reconnected { host_epoch: String },
}

enum HostOutputEvent {
    Frame(HostOutputFrame),
    Barrier(async_channel::Sender<()>),
}

#[derive(Debug)]
/// Redacted failure while establishing the private session-host channel.
pub struct HostClientError {
    source: HostClientErrorSource,
}

#[derive(Debug)]
enum HostClientErrorSource {
    Io(std::io::Error),
    Protocol(HostProtocolError),
    Bootstrap(super::ProcessHostError),
    Random(getrandom::Error),
    InvalidResponse,
    Deadline,
}

impl HostClientError {
    fn io(source: std::io::Error) -> Self {
        Self {
            source: HostClientErrorSource::Io(source),
        }
    }

    fn protocol(source: HostProtocolError) -> Self {
        Self {
            source: HostClientErrorSource::Protocol(source),
        }
    }
}

impl From<std::io::Error> for HostClientError {
    fn from(source: std::io::Error) -> Self {
        Self::io(source)
    }
}

impl Display for HostClientError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("session-host connection failed")
    }
}

impl Error for HostClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.source {
            HostClientErrorSource::Io(source) => Some(source),
            HostClientErrorSource::Protocol(source) => Some(source),
            HostClientErrorSource::Bootstrap(source) => Some(source),
            HostClientErrorSource::Random(source) => Some(source),
            HostClientErrorSource::InvalidResponse | HostClientErrorSource::Deadline => None,
        }
    }
}

#[derive(Debug, Clone)]
enum HostRequestError {
    Unavailable,
    Capacity,
    Io(Arc<std::io::Error>),
    Protocol(Arc<HostProtocolError>),
    Remote(HostFailure),
}

impl HostRequestError {
    fn into_port_error(self, operation: PortOperation, session: Option<SessionId>) -> PortError {
        let kind = match &self {
            Self::Unavailable | Self::Io(_) => PortErrorKind::Unavailable,
            Self::Capacity => PortErrorKind::CapacityExceeded,
            Self::Protocol(_) => PortErrorKind::ProtocolViolation,
            Self::Remote(failure) => failure.kind.into(),
        };
        PortError::new(kind, operation, session, self)
    }
}

impl From<std::io::Error> for HostRequestError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl Display for HostRequestError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Remote(failure) => write!(
                formatter,
                "session-host {} operation failed",
                host_failure_context_name(failure.context)
            ),
            _ => formatter.write_str("session-host request failed"),
        }
    }
}

impl Error for HostRequestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(source) => Some(source.as_ref()),
            Self::Protocol(source) => Some(source.as_ref()),
            _ => None,
        }
    }
}

fn host_failure_context_name(context: HostFailureContext) -> &'static str {
    match context {
        HostFailureContext::Execute => "execute",
        HostFailureContext::PollExecution => "poll_execution",
        HostFailureContext::CancelExecution => "cancel_execution",
        HostFailureContext::AcknowledgeExecution => "acknowledge_execution",
        HostFailureContext::AttemptMutation => "attempt_mutation",
        HostFailureContext::ObserveAttempt => "observe_attempt",
        HostFailureContext::AcknowledgeAttempt => "acknowledge_attempt",
        HostFailureContext::Authentication => "authentication",
        HostFailureContext::Spawn => "spawn",
        HostFailureContext::AdoptSpawn => "adopt_spawn",
        HostFailureContext::CancelSpawn => "cancel_spawn",
        HostFailureContext::ArmExit => "arm_exit",
        HostFailureContext::Stop => "stop",
        HostFailureContext::ForceStop => "force_stop",
        HostFailureContext::WriteInput => "write_input",
        HostFailureContext::CloseInput => "close_input",
        HostFailureContext::Resize => "resize",
        HostFailureContext::Observe => "observe",
        HostFailureContext::ObserveForeground => "observe_foreground",
        HostFailureContext::Replay => "replay",
        HostFailureContext::Snapshot => "snapshot",
        HostFailureContext::Retire => "retire",
        HostFailureContext::Shutdown => "shutdown",
        HostFailureContext::Protocol => "protocol",
    }
}

fn host_codec(config: &HostedProcessRuntimeConfig) -> Result<HostCodec, HostClientError> {
    let native = &config.native;
    let control_payload = native
        .limits()
        .input_queue_bytes_per_process
        .max(native.limits().environment_bytes)
        .max(native.limits().environment_file_bytes)
        .max(native.output_limits().append_bytes)
        .max(native.output_limits().replay_bytes);
    let screen_payload =
        native
            .limits()
            .screen_snapshot_bytes
            .checked_mul(6)
            .ok_or(HostClientError {
                source: HostClientErrorSource::InvalidResponse,
            })?;
    let execution_payload = native
        .output_limits()
        .per_session_bytes
        .checked_mul(4)
        .ok_or(HostClientError {
            source: HostClientErrorSource::InvalidResponse,
        })?;
    HostCodec::new(
        control_payload
            .max(screen_payload)
            .max(execution_payload)
            .saturating_add(64 * 1024),
    )
    .map_err(HostClientError::protocol)
}

fn connect_or_launch(
    config: &HostedProcessRuntimeConfig,
    codec: HostCodec,
) -> Result<(UnixStream, String, String), HostClientError> {
    if let Ok(token) = read_authentication(config)
        && let Ok((stream, epoch)) = connect_authenticated(config, codec, &token)
    {
        return Ok((stream, token, epoch));
    }
    let token = generate_token()?;
    launch_host(config, &token)?;
    let (stream, epoch) = connect_until(config, codec, &token)?;
    Ok((stream, token, epoch))
}

fn reconnect(
    config: &HostedProcessRuntimeConfig,
    codec: HostCodec,
    token: &Mutex<String>,
) -> Result<(UnixStream, String), HostClientError> {
    let current = token
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    if let Ok(connection) = connect_until(config, codec, &current) {
        return Ok(connection);
    }
    let replacement = generate_token()?;
    launch_host(config, &replacement)?;
    let connection = connect_until(config, codec, &replacement)?;
    *token
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = replacement;
    Ok(connection)
}

fn reconnect_existing(
    config: &HostedProcessRuntimeConfig,
    codec: HostCodec,
    token: &Mutex<String>,
) -> Result<(UnixStream, String), HostClientError> {
    let current = token
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    connect_until(config, codec, &current)
}

fn host_retired(config: &HostedProcessRuntimeConfig) -> bool {
    !config.socket.exists()
        && !ProcessHostBootstrap::authentication_path(&config.socket).exists()
        && !ProcessHostBootstrap::execution_journal_path(&config.journal_base).exists()
        && !ProcessHostBootstrap::attempt_receipt_journal_path(&config.journal_base).exists()
}

fn wait_for_host_retirement(config: &HostedProcessRuntimeConfig) -> bool {
    let Some(deadline) = Instant::now().checked_add(config.connect_timeout) else {
        return false;
    };
    loop {
        if host_retired(config) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(config.retry_initial);
    }
}

fn connect_until(
    config: &HostedProcessRuntimeConfig,
    codec: HostCodec,
    token: &str,
) -> Result<(UnixStream, String), HostClientError> {
    let deadline = Instant::now()
        .checked_add(config.connect_timeout)
        .ok_or(HostClientError {
            source: HostClientErrorSource::Deadline,
        })?;
    let mut delay = config.retry_initial;
    loop {
        match connect_authenticated(config, codec, token) {
            Ok(connection) => return Ok(connection),
            Err(_) if Instant::now() < deadline => {
                thread::sleep(delay.min(deadline.saturating_duration_since(Instant::now())));
                delay = delay.saturating_mul(2).min(config.retry_maximum);
            }
            Err(error) => return Err(error),
        }
    }
}

fn connect_authenticated(
    config: &HostedProcessRuntimeConfig,
    codec: HostCodec,
    token: &str,
) -> Result<(UnixStream, String), HostClientError> {
    let mut stream = UnixStream::connect(&config.socket).map_err(HostClientError::io)?;
    stream
        .set_read_timeout(Some(config.io_timeout))
        .and_then(|()| stream.set_write_timeout(Some(config.io_timeout)))
        .map_err(HostClientError::io)?;
    let request = HostRequest {
        request_id: 0,
        command: HostCommand::Authenticate {
            token: token.to_owned(),
        },
    };
    stream
        .write_all(
            &codec
                .encode_request(&request)
                .map_err(HostClientError::protocol)?,
        )
        .map_err(HostClientError::io)?;
    let response = read_host_frame(&mut stream, codec).map_err(HostClientError::protocol)?;
    let HostDecodedFrame::Response(response) = response else {
        return Err(HostClientError {
            source: HostClientErrorSource::InvalidResponse,
        });
    };
    let Ok(HostResponseValue::Authenticated { host_epoch }) = response.result else {
        return Err(HostClientError {
            source: HostClientErrorSource::InvalidResponse,
        });
    };
    stream.set_read_timeout(None).map_err(HostClientError::io)?;
    Ok((stream, host_epoch))
}

fn launch_host(config: &HostedProcessRuntimeConfig, token: &str) -> Result<(), HostClientError> {
    let bootstrap = ProcessHostBootstrap::new(config.socket.clone(), token, &config.native)
        .and_then(|bootstrap| bootstrap.with_journal_base(config.journal_base.clone()))
        .map_err(|source| HostClientError {
            source: HostClientErrorSource::Bootstrap(source),
        })?;
    let mut command = Command::new(&config.executable);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000); // CREATE_NO_WINDOW
    }
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(HostClientError::io)?;
    let mut stdin = child.stdin.take().ok_or_else(|| HostClientError {
        source: HostClientErrorSource::InvalidResponse,
    })?;
    bootstrap
        .write_to(&mut stdin)
        .map_err(|source| HostClientError {
            source: HostClientErrorSource::Bootstrap(source),
        })?;
    drop(stdin);
    thread::Builder::new()
        .name("undercurrent-host-child".to_owned())
        .spawn(move || {
            let _ = child.wait();
        })
        .map_err(HostClientError::io)?;
    Ok(())
}

fn read_authentication(config: &HostedProcessRuntimeConfig) -> Result<String, HostClientError> {
    let path = ProcessHostBootstrap::authentication_path(&config.socket);
    let mut value = String::new();
    super::host_files::read(&path)
        .map_err(HostClientError::io)?
        .take(129)
        .read_to_string(&mut value)
        .map_err(HostClientError::io)?;
    if value.is_empty() || value.len() > 128 || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(HostClientError {
            source: HostClientErrorSource::InvalidResponse,
        });
    }
    Ok(value)
}

fn generate_token() -> Result<String, HostClientError> {
    let mut bytes = [0_u8; TOKEN_BYTES];
    getrandom::fill(&mut bytes).map_err(|source| HostClientError {
        source: HostClientErrorSource::Random(source),
    })?;
    let mut token = String::with_capacity(TOKEN_BYTES * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        token.push(HEX[usize::from(byte >> 4)] as char);
        token.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    Ok(token)
}

fn read_host_frame(
    stream: &mut UnixStream,
    codec: HostCodec,
) -> Result<HostDecodedFrame, HostProtocolError> {
    let mut header = [0_u8; 20];
    stream
        .read_exact(&mut header)
        .map_err(|_| HostProtocolError::TruncatedFrame)?;
    let body_len =
        u32::from_be_bytes(header[6..10].try_into().expect("fixed session-host header")) as usize;
    let frame_len = header
        .len()
        .checked_add(body_len)
        .ok_or(HostProtocolError::SizeOverflow)?;
    if frame_len > codec.maximum_frame_bytes() {
        return Err(HostProtocolError::FrameTooLarge {
            actual: frame_len,
            maximum: codec.maximum_frame_bytes(),
        });
    }
    let mut encoded = Vec::with_capacity(frame_len);
    encoded.extend_from_slice(&header);
    encoded.resize(frame_len, 0);
    stream
        .read_exact(&mut encoded[header.len()..])
        .map_err(|_| HostProtocolError::TruncatedFrame)?;
    codec.decode(&encoded)
}

struct ThreadWake(thread::Thread);

impl std::task::Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    let waker = std::task::Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = std::task::Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            std::task::Poll::Ready(output) => return output,
            std::task::Poll::Pending => thread::park(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hosted_config_debug_redacts_paths() {
        let native = NativeProcessRuntimeConfig::new(
            "epoch",
            super::super::NativeProcessRuntimeLimits {
                max_processes: 1,
                spawn_workers: 1,
                spawn_queue_capacity: 1,
                input_queue_sequences: 1,
                input_queue_bytes_per_process: 1024,
                output_read_bytes: 1024,
                screen_snapshot_bytes: 1024,
                environment_file_bytes: 1024,
                environment_entries: 1,
                environment_bytes: 1024,
                worker_thread_stack_bytes: 64 * 1024,
            },
            undercurrent_application::TerminalOutputLimits {
                per_session_bytes: 1024,
                global_bytes: 1024,
                chunk_bytes: 256,
                append_bytes: 1024,
                replay_bytes: 1024,
                sessions: 1,
                subscriptions: 1,
            },
            Duration::from_secs(1),
        )
        .unwrap();
        let private = tempfile::tempdir().unwrap();
        let config = HostedProcessRuntimeConfig::new(
            native,
            private.path().join("host.sock"),
            private.path().join("uc-daemon"),
            Duration::from_secs(1),
            Duration::from_millis(10),
            Duration::from_millis(100),
            Duration::from_millis(100),
        )
        .unwrap();
        let debug = format!("{config:?}");
        assert!(!debug.contains(private.path().to_str().unwrap()));
    }

    #[test]
    fn output_assembler_orders_frames_and_discards_duplicates() {
        let process = ProcessInstance {
            id: ProcessInstanceId::new("process-1").unwrap(),
            started_at: TimestampMillis::new(1),
        };
        let mut entry = HostedProcessEntry::new(
            process,
            HostedProcessBinding {
                root_pid: 42,
                root_start_ticks: Some(7),
                attempt_id: None,
            },
            TerminalSize::new(80, 24, 80, 24).unwrap(),
            OutputCaptureMode::Terminal,
        );
        entry.insert_output(3, b"def".to_vec(), 1024);
        entry.insert_output(0, b"abc".to_vec(), 1024);
        entry.remote_next = Some(0);
        let mut ready = Vec::new();
        entry.drain_ready(&mut ready);
        assert_eq!(ready, vec![(0, b"abc".to_vec()), (3, b"def".to_vec())]);
        assert_eq!(entry.remote_next, Some(6));
        entry.insert_output(0, b"abcdef".to_vec(), 1024);
        entry.drain_ready(&mut ready);
        assert_eq!(ready.len(), 2);
    }

    #[test]
    fn generated_tokens_are_fixed_hex_without_debug_sources() {
        let token = generate_token().unwrap();
        assert_eq!(token.len(), TOKEN_BYTES * 2);
        assert!(token.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }
}

#[cfg(target_os = "linux")]
fn agent_executable_path(pid: u32) -> std::io::Result<PathBuf> {
    fs::read_link(format!("/proc/{pid}/exe"))
}

#[cfg(target_os = "macos")]
fn agent_executable_path(pid: u32) -> std::io::Result<PathBuf> {
    undercurrent_platform_macos::executable_path(pid)
}

#[cfg(target_os = "macos")]
fn agent_proc_identity(pid: u32) -> Result<AgentProcIdentity, HostedAgentPeerError> {
    let info =
        undercurrent_platform_macos::identity(pid).map_err(HostedAgentPeerError::ProcessTable)?;
    if info.uid != nix::unistd::geteuid().as_raw() {
        return Err(HostedAgentPeerError::ProcessTableChanged);
    }
    Ok(AgentProcIdentity {
        pid,
        parent_pid: info.parent,
        start_ticks: info.start_micros,
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn validated_agent_root(
    peer_pid: u32,
    expected_bridge_executable: &Path,
    roots: &[(u32, Option<u64>)],
) -> Result<Option<usize>, HostedAgentPeerError> {
    if roots.is_empty() {
        return Ok(None);
    }
    let expected_executable =
        fs::canonicalize(expected_bridge_executable).map_err(HostedAgentPeerError::ProcessTable)?;
    if agent_executable_path(peer_pid).map_err(HostedAgentPeerError::ProcessTable)?
        != expected_executable
    {
        return Err(HostedAgentPeerError::InvalidBridgeExecutable);
    }
    let initial_peer = agent_proc_identity(peer_pid)?;
    let mut current = initial_peer;
    let mut matched = None;
    let mut visited = BTreeSet::new();
    let mut chain = Vec::new();
    for _ in 0..MAX_AGENT_PEER_ANCESTORS {
        if !visited.insert(current.pid) {
            return Err(HostedAgentPeerError::ProcessTableChanged);
        }
        chain.push(current);
        if roots
            .iter()
            .any(|root| root.0 == current.pid && root.1 != Some(current.start_ticks))
        {
            return Err(HostedAgentPeerError::ProcessTableChanged);
        }
        if let Some(root) = roots
            .iter()
            .enumerate()
            .find(|(_, root)| root.0 == current.pid && root.1 == Some(current.start_ticks))
        {
            matched = Some(root.0);
            break;
        }
        if current.parent_pid <= 1 || current.parent_pid == current.pid {
            break;
        }
        current = agent_proc_identity(current.parent_pid)?;
    }
    let Some(index) = matched else {
        return Ok(None);
    };
    let (root_pid, root_start_ticks) = roots[index];
    let final_peer = agent_proc_identity(peer_pid)?;
    let final_root = agent_proc_identity(root_pid)?;
    let final_executable =
        agent_executable_path(peer_pid).map_err(HostedAgentPeerError::ProcessTable)?;
    if chain
        .iter()
        .any(|info| agent_proc_identity(info.pid).ok() != Some(*info))
        || final_peer != initial_peer
        || final_root.start_ticks != root_start_ticks.unwrap_or_default()
        || final_executable != expected_executable
    {
        return Err(HostedAgentPeerError::ProcessTableChanged);
    }
    Ok(Some(index))
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod agent_ancestry_tests {
    use super::*;

    #[test]
    fn real_child_requires_exact_executable_and_root_incarnation() {
        let root = agent_proc_identity(std::process::id()).unwrap();
        let mut child = std::process::Command::new("/bin/sleep")
            .arg("30")
            .spawn()
            .unwrap();
        // Always reap the child even when an assertion fails.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let roots = [(root.pid, Some(root.start_ticks))];
            // Wait for the child executable to be observable before testing
            // authentication; process creation and kernel path observation can
            // briefly disagree during startup on a loaded Linux runner.
            let expected = fs::canonicalize("/bin/sleep").unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while agent_executable_path(child.id()).ok().as_ref() != Some(&expected) {
                assert!(
                    std::time::Instant::now() < deadline,
                    "child executable did not settle"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            assert_eq!(
                validated_agent_root(child.id(), Path::new("/bin/sleep"), &roots).unwrap_or_else(
                    |error| panic!(
                        "{error:?}: observed {:?}, expected {:?}",
                        agent_executable_path(child.id()),
                        fs::canonicalize("/bin/sleep")
                    )
                ),
                Some(0)
            );
            assert!(matches!(
                validated_agent_root(child.id(), Path::new("/bin/sh"), &roots),
                Err(HostedAgentPeerError::InvalidBridgeExecutable)
            ));
            assert!(matches!(
                validated_agent_root(
                    child.id(),
                    Path::new("/bin/sleep"),
                    &[(root.pid, Some(root.start_ticks + 1))]
                ),
                Err(HostedAgentPeerError::ProcessTableChanged)
            ));
            assert_eq!(
                validated_agent_root(child.id(), Path::new("/bin/sleep"), &[]).unwrap(),
                None
            );
        }));
        let _ = child.kill();
        let _ = child.wait();
        if let Err(error) = result {
            std::panic::resume_unwind(error);
        }
    }
}

#[cfg(windows)]
fn validated_agent_root(
    peer_pid: u32,
    expected_bridge_executable: &Path,
    roots: &[(u32, Option<u64>)],
) -> Result<Option<usize>, HostedAgentPeerError> {
    use undercurrent_platform_windows::{PinnedPath, ProcessHandle};
    if roots.is_empty() {
        return Ok(None);
    }
    let expected = PinnedPath::executable(expected_bridge_executable)
        .map_err(HostedAgentPeerError::ProcessTable)?;
    let peer = ProcessHandle::open(peer_pid).map_err(HostedAgentPeerError::ProcessTable)?;
    let image = PinnedPath::executable(
        &peer
            .executable_path()
            .map_err(HostedAgentPeerError::ProcessTable)?,
    )
    .map_err(HostedAgentPeerError::ProcessTable)?;
    if image
        .identity()
        .map_err(HostedAgentPeerError::ProcessTable)?
        != expected
            .identity()
            .map_err(HostedAgentPeerError::ProcessTable)?
    {
        return Err(HostedAgentPeerError::InvalidBridgeExecutable);
    }
    // Retain every process object until the complete chain is checked. Windows
    // creator PIDs do not change when a parent exits; parent() rejects reuse.
    let mut chain = vec![peer];
    for _ in 0..MAX_AGENT_PEER_ANCESTORS {
        let current = chain.last().expect("peer starts the chain");
        if !current
            .is_alive()
            .map_err(HostedAgentPeerError::ProcessTable)?
            || !current
                .is_current_user()
                .map_err(HostedAgentPeerError::ProcessTable)?
        {
            return Err(HostedAgentPeerError::ProcessTableChanged);
        }
        let identity = current.identity();
        if let Some((index, (_, created))) = roots
            .iter()
            .enumerate()
            .find(|(_, root)| root.0 == identity.pid)
        {
            if *created != Some(identity.created)
                || chain.iter().any(|p| !p.is_alive().unwrap_or(false))
            {
                return Err(HostedAgentPeerError::ProcessTableChanged);
            }
            return Ok(Some(index));
        }
        let Some(parent) = current
            .parent()
            .map_err(HostedAgentPeerError::ProcessTable)?
        else {
            return Ok(None);
        };
        if chain
            .iter()
            .any(|p| p.identity().pid == parent.identity().pid)
        {
            return Err(HostedAgentPeerError::ProcessTableChanged);
        }
        chain.push(parent);
    }
    Err(HostedAgentPeerError::ProcessTable(std::io::Error::other(
        "agent ancestry exceeds bound",
    )))
}

#[cfg(all(test, windows))]
mod windows_agent_tests {
    use super::*;
    use undercurrent_platform_windows::{JobChild, ProcessHandle};

    #[test]
    fn native_bridge_admission_requires_exact_image_and_root_incarnation() {
        let root = ProcessHandle::open(std::process::id()).unwrap();
        let executable = PathBuf::from(std::env::var_os("COMSPEC").unwrap());
        let mut command = Command::new(&executable);
        command
            .args(["/D", "/Q", "/K"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let child = JobChild::spawn(&mut command).unwrap();
        let roots = [(root.identity().pid, Some(root.identity().created))];
        assert_eq!(
            validated_agent_root(child.id(), &executable, &roots).unwrap(),
            Some(0)
        );
        assert!(matches!(
            validated_agent_root(child.id(), &std::env::current_exe().unwrap(), &roots),
            Err(HostedAgentPeerError::InvalidBridgeExecutable)
        ));
        assert!(matches!(
            validated_agent_root(
                child.id(),
                &executable,
                &[(root.identity().pid, Some(root.identity().created + 1))]
            ),
            Err(HostedAgentPeerError::ProcessTableChanged)
        ));
        assert_eq!(
            validated_agent_root(child.id(), &executable, &[]).unwrap(),
            None
        );
    }
}
