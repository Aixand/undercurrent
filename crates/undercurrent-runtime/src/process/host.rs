// SPDX-License-Identifier: Apache-2.0

//! Daemon-independent owner for native PTYs and their isolated process supervisors.

use super::host_files;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::fs;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};
#[cfg(windows)]
use undercurrent_platform_windows::{
    BlockingPipeListener as UnixListener, BlockingPipeStream as UnixStream,
};

use serde::{Deserialize, Serialize};
use undercurrent_application::{
    Clock, ForegroundProcessObserver, OutputGapRecovery, OutputOffset, OutputReplay, PortError,
    ProcessExitObservation, ProcessGroupCompleteness, ProcessInstance, ProcessMutationContext,
    ProcessRuntime, ProcessTermination, ProcessTruthSource, StandardInputClosure,
    TcpListenerObservation, Telemetry, TerminalOutputLimits, TerminalScreenReader,
    process_instance_result_digest, process_termination_result_digest, process_unit_result_digest,
};
use undercurrent_domain::{
    CooperativeForegroundState, FencingToken, OperationAttemptId, OutputCaptureMode, PayloadDigest,
    ProcessIdentity, ProcessInstanceId, ProcessMutationKind, ProcessOutcome, SessionId,
    TimestampMillis,
};

use super::attempt_receipts::{
    HostAttemptReceiptRegistry, ReceiptBegin, ReceiptError, ReceiptProbe,
};
use super::execute_host::{HostExecutionRegistry, HostExecutionSpec};
use super::terminal_exits::TerminalExitRegistry;
use super::{
    NativeProcessRuntime, NativeProcessRuntimeConfig, NativeProcessRuntimeLimits,
    ProcessOutputObserver,
};
use crate::process_host_protocol::{
    HostAttemptContext, HostAttemptKind, HostAttemptMutation, HostAttemptProbeState, HostCodec,
    HostCommand, HostCooperativeEvent, HostDecodedFrame, HostEffectCertainty, HostExitEvent,
    HostFailure, HostFailureContext, HostFailureKind, HostForegroundContext, HostForegroundMember,
    HostObservedProcessState, HostOutputFrame, HostProcessGroupCompleteness, HostProtocolError,
    HostRequest, HostResponse, HostResponseValue, HostTcpListenerObservation,
};
use crate::{BoundedTelemetryRecorder, EncodedInputPause, EncodedTerminalInput, SystemClock};

#[path = "shell_environment.rs"]
mod shell_environment;
pub use shell_environment::capture_shell_environment;

const AUTH_FILE_EXTENSION: &str = "auth";
const EXECUTION_JOURNAL_EXTENSION: &str = "execution-jobs";
const ATTEMPT_RECEIPT_JOURNAL_EXTENSION: &str = "attempt-receipts";
const MAXIMUM_TOKEN_BYTES: usize = 128;
const SPAWN_TOKEN_BYTES: usize = 32;
const CONTROL_FRAME_ALLOWANCE: usize = 64 * 1024;
// This is an enqueue-time aggregation deadline, not a scheduling or socket-write latency bound.
const HOST_OUTPUT_COALESCE_LINGER: Duration = Duration::from_millis(1);

/// Private bootstrap consumed once by a newly launched session host.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessHostBootstrap {
    socket: PathBuf,
    #[serde(default)]
    journal_base: Option<PathBuf>,
    token: String,
    epoch: String,
    limits: HostProcessLimits,
    output: HostOutputLimits,
    #[serde(default)]
    scrollback_lines: usize,
    force_confirmation_timeout_millis: u64,
}

impl Debug for ProcessHostBootstrap {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProcessHostBootstrap")
            .field("socket", &"<redacted>")
            .field("token", &"<redacted>")
            .field("epoch", &self.epoch)
            .field("limits", &self.limits)
            .field("output", &self.output)
            .field(
                "force_confirmation_timeout_millis",
                &self.force_confirmation_timeout_millis,
            )
            .finish()
    }
}

impl ProcessHostBootstrap {
    /// Constructs a payload-redacting bootstrap from validated native settings.
    ///
    /// The token must be non-empty, bounded ASCII hexadecimal text. This
    /// function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed bootstrap failure for an invalid socket, token, or
    /// duration representation.
    pub fn new(
        socket: impl Into<PathBuf>,
        token: impl Into<String>,
        config: &NativeProcessRuntimeConfig,
    ) -> Result<Self, ProcessHostError> {
        let socket = socket.into();
        if !socket.is_absolute() || socket.file_name().is_none() {
            return Err(ProcessHostError::message(
                ProcessHostStage::ValidateBootstrap,
                "session-host socket must be an absolute file path",
            ));
        }
        let token = token.into();
        if token.is_empty()
            || token.len() > MAXIMUM_TOKEN_BYTES
            || !token.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(ProcessHostError::message(
                ProcessHostStage::ValidateBootstrap,
                "session-host token must be bounded hexadecimal text",
            ));
        }
        let force_confirmation_timeout_millis =
            u64::try_from(config.force_confirmation_timeout().as_millis()).map_err(|_| {
                ProcessHostError::message(
                    ProcessHostStage::ValidateBootstrap,
                    "session-host force timeout exceeds protocol representation",
                )
            })?;
        Ok(Self {
            socket,
            journal_base: None,
            token,
            epoch: config.epoch().to_owned(),
            limits: config.limits().into(),
            output: config.output_limits().into(),
            scrollback_lines: config.scrollback_lines,
            force_confirmation_timeout_millis,
        })
    }

    /// Serializes the private bootstrap to a trusted inherited pipe.
    ///
    /// The supplied writer receives the authentication token and must not be a
    /// log or user-visible stream. This function performs bounded IO and does
    /// not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed serialization or write failure.
    pub fn write_to(&self, mut writer: impl Write) -> Result<(), ProcessHostError> {
        serde_json::to_writer(&mut writer, self)
            .map_err(|source| ProcessHostError::new(ProcessHostStage::WriteBootstrap, source))?;
        writer
            .flush()
            .map_err(|source| ProcessHostError::new(ProcessHostStage::WriteBootstrap, source))
    }

    /// Reads one bounded bootstrap from an inherited pipe.
    ///
    /// This function does not log or otherwise expose bootstrap contents and
    /// does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed read, size, parse, or validation failure.
    pub fn read_from(reader: impl Read) -> Result<Self, ProcessHostError> {
        let mut bytes = Vec::new();
        reader
            .take(CONTROL_FRAME_ALLOWANCE as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|source| ProcessHostError::new(ProcessHostStage::ReadBootstrap, source))?;
        if bytes.len() > CONTROL_FRAME_ALLOWANCE {
            return Err(ProcessHostError::message(
                ProcessHostStage::ReadBootstrap,
                "session-host bootstrap exceeds its byte limit",
            ));
        }
        let bootstrap: Self = serde_json::from_slice(&bytes)
            .map_err(|source| ProcessHostError::new(ProcessHostStage::ReadBootstrap, source))?;
        let config = bootstrap.runtime_config()?;
        let mut validated = Self::new(bootstrap.socket, bootstrap.token, &config)?;
        if let Some(base) = bootstrap.journal_base {
            validated = validated.with_journal_base(base)?;
        }
        Ok(validated)
    }

    /// Selects an absolute base for durable journals, independently of the socket.
    pub fn with_journal_base(mut self, base: PathBuf) -> Result<Self, ProcessHostError> {
        if !base.is_absolute() || base.file_name().is_none() {
            return Err(ProcessHostError::message(
                ProcessHostStage::ValidateBootstrap,
                "session-host journal base must be an absolute file path",
            ));
        }
        self.journal_base = Some(base);
        Ok(self)
    }

    fn journal_base(&self) -> &Path {
        self.journal_base.as_deref().unwrap_or(&self.socket)
    }

    /// Returns the private authentication-file path for a host socket.
    pub fn authentication_path(socket: &Path) -> PathBuf {
        socket.with_extension(AUTH_FILE_EXTENSION)
    }

    pub(super) fn execution_journal_path(socket: &Path) -> PathBuf {
        socket.with_extension(EXECUTION_JOURNAL_EXTENSION)
    }

    pub(super) fn attempt_receipt_journal_path(socket: &Path) -> PathBuf {
        socket.with_extension(ATTEMPT_RECEIPT_JOURNAL_EXTENSION)
    }

    fn runtime_config(&self) -> Result<NativeProcessRuntimeConfig, ProcessHostError> {
        NativeProcessRuntimeConfig::new(
            self.epoch.clone(),
            self.limits.into(),
            self.output.into(),
            Duration::from_millis(self.force_confirmation_timeout_millis),
        )
        .map(|config| config.with_scrollback_lines(self.scrollback_lines))
        .map_err(|source| ProcessHostError::new(ProcessHostStage::ValidateBootstrap, source))
    }

    fn codec(&self) -> Result<HostCodec, ProcessHostError> {
        let control_payload = self
            .limits
            .input_queue_bytes_per_process
            .max(
                self.limits
                    .environment_bytes
                    .checked_mul(6)
                    .ok_or_else(|| {
                        ProcessHostError::message(
                            ProcessHostStage::ValidateBootstrap,
                            "session-host environment frame limit overflowed",
                        )
                    })?,
            )
            .max(self.limits.environment_file_bytes)
            .max(self.output.append_bytes)
            .max(self.output.replay_bytes);
        let screen_payload = self
            .limits
            .screen_snapshot_bytes
            .checked_mul(6)
            .ok_or_else(|| {
                ProcessHostError::message(
                    ProcessHostStage::ValidateBootstrap,
                    "session-host screen frame limit overflowed",
                )
            })?;
        let execution_payload = self
            .output
            .per_session_bytes
            .checked_mul(4)
            .ok_or_else(|| {
                ProcessHostError::message(
                    ProcessHostStage::ValidateBootstrap,
                    "session-host execution frame limit overflowed",
                )
            })?;
        let maximum = control_payload
            .max(screen_payload)
            .max(execution_payload)
            .checked_add(CONTROL_FRAME_ALLOWANCE)
            .ok_or_else(|| {
                ProcessHostError::message(
                    ProcessHostStage::ValidateBootstrap,
                    "session-host frame limit overflowed",
                )
            })?;
        HostCodec::new(maximum)
            .map_err(|source| ProcessHostError::new(ProcessHostStage::ValidateBootstrap, source))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HostProcessLimits {
    max_processes: usize,
    spawn_workers: usize,
    spawn_queue_capacity: usize,
    input_queue_sequences: usize,
    input_queue_bytes_per_process: usize,
    output_read_bytes: usize,
    screen_snapshot_bytes: usize,
    environment_file_bytes: usize,
    environment_entries: usize,
    environment_bytes: usize,
    worker_thread_stack_bytes: usize,
}

impl From<NativeProcessRuntimeLimits> for HostProcessLimits {
    fn from(value: NativeProcessRuntimeLimits) -> Self {
        Self {
            max_processes: value.max_processes,
            spawn_workers: value.spawn_workers,
            spawn_queue_capacity: value.spawn_queue_capacity,
            input_queue_sequences: value.input_queue_sequences,
            input_queue_bytes_per_process: value.input_queue_bytes_per_process,
            output_read_bytes: value.output_read_bytes,
            screen_snapshot_bytes: value.screen_snapshot_bytes,
            environment_file_bytes: value.environment_file_bytes,
            environment_entries: value.environment_entries,
            environment_bytes: value.environment_bytes,
            worker_thread_stack_bytes: value.worker_thread_stack_bytes,
        }
    }
}

impl From<HostProcessLimits> for NativeProcessRuntimeLimits {
    fn from(value: HostProcessLimits) -> Self {
        Self {
            max_processes: value.max_processes,
            spawn_workers: value.spawn_workers,
            spawn_queue_capacity: value.spawn_queue_capacity,
            input_queue_sequences: value.input_queue_sequences,
            input_queue_bytes_per_process: value.input_queue_bytes_per_process,
            output_read_bytes: value.output_read_bytes,
            screen_snapshot_bytes: value.screen_snapshot_bytes,
            environment_file_bytes: value.environment_file_bytes,
            environment_entries: value.environment_entries,
            environment_bytes: value.environment_bytes,
            worker_thread_stack_bytes: value.worker_thread_stack_bytes,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HostOutputLimits {
    per_session_bytes: usize,
    global_bytes: usize,
    chunk_bytes: usize,
    append_bytes: usize,
    replay_bytes: usize,
    sessions: usize,
    subscriptions: usize,
}

impl From<TerminalOutputLimits> for HostOutputLimits {
    fn from(value: TerminalOutputLimits) -> Self {
        Self {
            per_session_bytes: value.per_session_bytes,
            global_bytes: value.global_bytes,
            chunk_bytes: value.chunk_bytes,
            append_bytes: value.append_bytes,
            replay_bytes: value.replay_bytes,
            sessions: value.sessions,
            subscriptions: value.subscriptions,
        }
    }
}

impl From<HostOutputLimits> for TerminalOutputLimits {
    fn from(value: HostOutputLimits) -> Self {
        Self {
            per_session_bytes: value.per_session_bytes,
            global_bytes: value.global_bytes,
            chunk_bytes: value.chunk_bytes,
            append_bytes: value.append_bytes,
            replay_bytes: value.replay_bytes,
            sessions: value.sessions,
            subscriptions: value.subscriptions,
        }
    }
}

/// Stable process-host failure stage registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessHostStage {
    ValidateBootstrap,
    ReadBootstrap,
    WriteBootstrap,
    PrepareSocket,
    WriteAuthentication,
    StartRuntime,
    Bind,
    Accept,
    SpawnConnection,
    ConfirmShutdown,
}

impl ProcessHostStage {
    /// Complete stage registry in lifecycle order.
    pub const ALL: [Self; 10] = [
        Self::ValidateBootstrap,
        Self::ReadBootstrap,
        Self::WriteBootstrap,
        Self::PrepareSocket,
        Self::WriteAuthentication,
        Self::StartRuntime,
        Self::Bind,
        Self::Accept,
        Self::SpawnConnection,
        Self::ConfirmShutdown,
    ];

    /// Returns the stable diagnostic name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ValidateBootstrap => "validate_bootstrap",
            Self::ReadBootstrap => "read_bootstrap",
            Self::WriteBootstrap => "write_bootstrap",
            Self::PrepareSocket => "prepare_socket",
            Self::WriteAuthentication => "write_authentication",
            Self::StartRuntime => "start_runtime",
            Self::Bind => "bind",
            Self::Accept => "accept",
            Self::SpawnConnection => "spawn_connection",
            Self::ConfirmShutdown => "confirm_shutdown",
        }
    }
}

/// Payload-redacted session-host infrastructure failure.
pub struct ProcessHostError {
    stage: ProcessHostStage,
    source: Box<dyn Error + Send + Sync>,
}

impl ProcessHostError {
    fn new(stage: ProcessHostStage, source: impl Error + Send + Sync + 'static) -> Self {
        Self {
            stage,
            source: Box::new(source),
        }
    }

    fn message(stage: ProcessHostStage, message: &'static str) -> Self {
        Self::new(stage, HostStaticError(message))
    }

    /// Returns the stable failed stage.
    pub const fn stage(&self) -> ProcessHostStage {
        self.stage
    }
}

impl Debug for ProcessHostError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProcessHostError")
            .field("stage", &self.stage)
            .finish_non_exhaustive()
    }
}

impl Display for ProcessHostError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "session host failed at {}", self.stage.as_str())
    }
}

impl Error for ProcessHostError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

#[derive(Debug)]
struct HostStaticError(&'static str);

impl Display for HostStaticError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for HostStaticError {}

/// Runs one daemon-independent process host until an authenticated shutdown.
///
/// The host owns its socket, authentication file, PTYs, child processes,
/// output retention, and connection workers. It never logs commands,
/// environment values, tokens, or terminal payloads. Clean return removes the
/// socket and token and force-settles remaining children. This function blocks
/// and does not panic.
///
/// # Errors
///
/// Returns a typed bootstrap, filesystem, listener, runtime, or worker failure.
pub fn run_process_host(bootstrap: ProcessHostBootstrap) -> Result<(), ProcessHostError> {
    let journal_base = bootstrap.journal_base().to_path_buf();
    let codec = bootstrap.codec()?;
    let mut runtime_config = bootstrap.runtime_config()?;
    runtime_config.shell_capture_socket = Some(bootstrap.socket.clone());
    prepare_host_socket(&bootstrap.socket)?;
    let listener = UnixListener::bind(&bootstrap.socket)
        .map_err(|source| ProcessHostError::new(ProcessHostStage::Bind, source))?;
    // Restrict the socket before publishing the journals and authentication file.
    // Their presence must not expose a partially initialized socket mode.
    #[cfg(unix)]
    fs::set_permissions(&bootstrap.socket, fs::Permissions::from_mode(0o600))
        .map_err(|source| ProcessHostError::new(ProcessHostStage::PrepareSocket, source))?;
    let executions = HostExecutionRegistry::new(
        bootstrap.limits.max_processes,
        bootstrap
            .limits
            .environment_bytes
            .max(CONTROL_FRAME_ALLOWANCE),
        bootstrap.limits.environment_entries,
        bootstrap.limits.environment_bytes,
        bootstrap.output.per_session_bytes.max(1),
        bootstrap.limits.worker_thread_stack_bytes,
        ProcessHostBootstrap::execution_journal_path(&journal_base),
    )
    .map_err(|source| ProcessHostError::new(ProcessHostStage::StartRuntime, source))?;
    let receipt_capacity = bootstrap
        .limits
        .max_processes
        .saturating_mul(8)
        .saturating_add(bootstrap.limits.spawn_queue_capacity)
        .max(1);
    let attempt_receipts = HostAttemptReceiptRegistry::new_for_host(
        receipt_capacity,
        ProcessHostBootstrap::attempt_receipt_journal_path(&journal_base),
    )
    .map_err(|source| ProcessHostError::new(ProcessHostStage::StartRuntime, source))?;
    let terminal_exits = TerminalExitRegistry::new(
        ProcessHostBootstrap::attempt_receipt_journal_path(&journal_base),
        receipt_capacity,
    )
    .map_err(|source| ProcessHostError::new(ProcessHostStage::StartRuntime, source))?;
    write_authentication(&bootstrap)?;

    let router = Arc::new(HostOutputRouter::new(codec));
    let telemetry: Arc<dyn Telemetry> = Arc::new(
        BoundedTelemetryRecorder::new(1024, 1024)
            .map_err(|source| ProcessHostError::new(ProcessHostStage::StartRuntime, source))?,
    );
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let runtime = NativeProcessRuntime::start_with_output_observer(
        runtime_config,
        clock,
        telemetry,
        router.clone(),
    )
    .map_err(|source| ProcessHostError::new(ProcessHostStage::StartRuntime, source))?;
    let shutdown = Arc::new(HostShutdownSignal::new(bootstrap.socket.clone()));
    let shared = Arc::new(HostShared {
        token: bootstrap.token,
        epoch: bootstrap.epoch,
        socket: bootstrap.socket.clone(),
        codec,
        limits: bootstrap.limits,
        output_limits: bootstrap.output,
        runtime,
        router,
        exits: Mutex::new(BTreeMap::new()),
        metadata: Mutex::new(BTreeMap::new()),
        armed: Mutex::new(BTreeSet::new()),
        spawns: Mutex::new(BTreeMap::new()),
        executions,
        shell_environments: Mutex::new(BTreeMap::new()),
        attempt_receipts,
        terminal_exits,
        next_connection: AtomicU64::new(0),
        shutdown,
    });
    let exit_forwarder = start_exit_forwarder(&shared)?;
    let mut connections: Vec<(Arc<UnixStream>, thread::JoinHandle<()>)> = Vec::new();

    while !shared.shutdown.load(Ordering::Acquire) {
        let (stream, _) = listener
            .accept()
            .map_err(|source| ProcessHostError::new(ProcessHostStage::Accept, source))?;
        if shared.shutdown.load(Ordering::Acquire) {
            break;
        }
        connections.retain(|(_, worker)| !worker.is_finished());
        let shutdown_stream =
            Arc::new(stream.try_clone().map_err(|source| {
                ProcessHostError::new(ProcessHostStage::SpawnConnection, source)
            })?);
        let connection_guard = HostConnectionGuard(shutdown_stream.clone());
        let connection = Arc::clone(&shared);
        let worker = thread::Builder::new()
            .name("undercurrent-host-connection".to_owned())
            .stack_size(shared.limits.worker_thread_stack_bytes)
            .spawn(move || {
                let _guard = connection_guard;
                serve_connection(connection, stream);
            })
            .map_err(|source| ProcessHostError::new(ProcessHostStage::SpawnConnection, source))?;
        connections.push((shutdown_stream, worker));
    }

    shared.runtime.begin_shutdown();
    // Close spawn admission and await its workers before snapshotting ownership; an admitted
    // spawn must not register another child after the force/confirmation pass.
    shared.runtime.owner.preparation_pool.cancel_queued();
    shared.runtime.owner.spawn_pool.close();
    let spawn_deadline =
        Instant::now() + Duration::from_millis(bootstrap.force_confirmation_timeout_millis);
    while !shared.runtime.owner.preparation_pool.is_quiescent()
        || !shared.runtime.owner.spawn_pool.is_quiescent()
    {
        if Instant::now() >= spawn_deadline {
            shared.runtime.owner.core.force_all();
            return Err(ProcessHostError::message(
                ProcessHostStage::ConfirmShutdown,
                "spawn workers did not confirm shutdown",
            ));
        }
        thread::sleep(Duration::from_millis(5));
    }
    shared.executions.cancel_all_and_wait(Duration::from_millis(
        bootstrap.force_confirmation_timeout_millis,
    ));
    shared.router.close();
    for (stream, _) in &connections {
        let _ = stream.shutdown(std::net::Shutdown::Both);
    }
    shared
        .runtime
        .owner
        .core
        .force_all_and_confirm(Duration::from_millis(
            bootstrap.force_confirmation_timeout_millis,
        ))
        .map_err(|error| ProcessHostError::new(ProcessHostStage::ConfirmShutdown, error))?;
    if shared.executions.has_running_jobs() {
        return Err(ProcessHostError::message(
            ProcessHostStage::ConfirmShutdown,
            "execution jobs did not confirm shutdown",
        ));
    }
    // A cancelled mutation may still be committing its host receipt after its
    // process exits. Join connection-owned request workers and the exit journal
    // writer before removing evidence that the CLI uses to authorize deletion.
    let writers_deadline =
        Instant::now() + Duration::from_millis(bootstrap.force_confirmation_timeout_millis);
    while !exit_forwarder.is_finished()
        || connections.iter().any(|(_, worker)| !worker.is_finished())
    {
        if Instant::now() >= writers_deadline {
            return Err(ProcessHostError::message(
                ProcessHostStage::ConfirmShutdown,
                "host writers did not confirm shutdown",
            ));
        }
        thread::sleep(Duration::from_millis(5));
    }
    let _ = exit_forwarder.join();
    for (_, worker) in connections {
        let _ = worker.join();
    }
    // Retirement markers disappear only after owned writers have stopped.
    #[cfg(unix)]
    let _ = fs::remove_file(&bootstrap.socket);
    let _ = fs::remove_file(ProcessHostBootstrap::authentication_path(&bootstrap.socket));
    if shared.shutdown.retires_storage() {
        let _ = fs::remove_dir_all(ProcessHostBootstrap::execution_journal_path(&journal_base));
        let _ = fs::remove_dir_all(ProcessHostBootstrap::attempt_receipt_journal_path(
            &journal_base,
        ));
    }
    Ok(())
}

fn prepare_host_socket(socket: &Path) -> Result<(), ProcessHostError> {
    let parent = socket.parent().ok_or_else(|| {
        ProcessHostError::message(
            ProcessHostStage::PrepareSocket,
            "session-host socket has no parent",
        )
    })?;
    host_files::prepare_directory(parent)
        .map_err(|source| ProcessHostError::new(ProcessHostStage::PrepareSocket, source))?;
    #[cfg(unix)]
    if socket.exists() {
        if UnixStream::connect(socket).is_ok() {
            return Err(ProcessHostError::message(
                ProcessHostStage::PrepareSocket,
                "another session host already owns the socket",
            ));
        }
        fs::remove_file(socket)
            .map_err(|source| ProcessHostError::new(ProcessHostStage::PrepareSocket, source))?;
    }
    Ok(())
}

fn write_authentication(bootstrap: &ProcessHostBootstrap) -> Result<(), ProcessHostError> {
    let path = ProcessHostBootstrap::authentication_path(&bootstrap.socket);
    let mut file = host_files::write_truncated(&path)
        .map_err(|source| ProcessHostError::new(ProcessHostStage::WriteAuthentication, source))?;
    file.write_all(bootstrap.token.as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|source| ProcessHostError::new(ProcessHostStage::WriteAuthentication, source))
}

struct HostShared {
    token: String,
    epoch: String,
    socket: PathBuf,
    codec: HostCodec,
    limits: HostProcessLimits,
    output_limits: HostOutputLimits,
    runtime: NativeProcessRuntime,
    router: Arc<HostOutputRouter>,
    exits: Mutex<BTreeMap<(SessionId, ProcessInstanceId), HostExitEvent>>,
    metadata: Mutex<BTreeMap<(SessionId, ProcessInstanceId), HostProcessMetadata>>,
    armed: Mutex<BTreeSet<(SessionId, ProcessInstanceId)>>,
    spawns: Mutex<BTreeMap<String, HostedSpawnState>>,
    executions: HostExecutionRegistry,
    shell_environments: Mutex<
        BTreeMap<(SessionId, ProcessInstanceId), shell_environment::ShellEnvironmentSnapshot>,
    >,
    attempt_receipts: HostAttemptReceiptRegistry,
    terminal_exits: TerminalExitRegistry,
    next_connection: AtomicU64,
    shutdown: Arc<HostShutdownSignal>,
}

#[derive(Clone, Copy)]
struct HostProcessMetadata {
    terminal_size: undercurrent_domain::TerminalSize,
    output_capture_mode: OutputCaptureMode,
    root_pid: u32,
    root_start_ticks: Option<u64>,
}

struct HostShutdownSignal {
    requested: AtomicBool,
    retire_storage: AtomicBool,
    socket: PathBuf,
}

impl HostShutdownSignal {
    fn new(socket: PathBuf) -> Self {
        Self {
            requested: AtomicBool::new(false),
            retire_storage: AtomicBool::new(false),
            socket,
        }
    }

    fn load(&self, order: Ordering) -> bool {
        self.requested.load(order)
    }

    fn store(&self, value: bool, order: Ordering) {
        self.requested.store(value, order);
    }

    fn mark_storage_retirement(&self) {
        self.retire_storage.store(true, Ordering::Release);
    }

    fn request_retirement(&self) {
        self.mark_storage_retirement();
        self.requested.store(true, Ordering::Release);
        let _ = UnixStream::connect(&self.socket);
    }

    fn retires_storage(&self) -> bool {
        self.retire_storage.load(Ordering::Acquire)
    }
}

enum HostedSpawnState {
    Spawning {
        connection_id: u64,
    },
    Spawned {
        connection_id: u64,
        key: (SessionId, ProcessInstanceId),
        adopted: bool,
    },
    Cancelled {
        connection_id: u64,
    },
}

fn start_exit_forwarder(
    shared: &Arc<HostShared>,
) -> Result<thread::JoinHandle<()>, ProcessHostError> {
    let shared = Arc::clone(shared);
    thread::Builder::new()
        .name("undercurrent-host-exits".to_owned())
        .stack_size(shared.limits.worker_thread_stack_bytes)
        .spawn(move || {
            while let Ok(observation) = shared.runtime.next_exit_blocking() {
                let key = (
                    observation.session_id.clone(),
                    observation.process_id.clone(),
                );
                shared
                    .shell_environments
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .remove(&key);
                let output_offset = match shared
                    .runtime
                    .output_store()
                    .session_status(&observation.session_id)
                {
                    Ok(Some(output_status)) => output_status.next_offset.value(),
                    Ok(None) => 0,
                    Err(_) => continue,
                };
                let mut snapshots = shared
                    .shell_environments
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                snapshots.remove(&key);
                let event = exit_event(&observation, output_offset);
                // Persist exact-incarnation evidence before the daemon can retire
                // the live host entry. Failed persistence must not fabricate proof.
                if !event.supervision_failed {
                    let _ = shared.terminal_exits.record(&event);
                }
                shared
                    .exits
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(key.clone(), event);
                drop(snapshots);
                if let Some(outbound) = shared.router.interested(&key)
                    && let Some(event) = shared
                        .exits
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .get(&key)
                    && let Ok(encoded) = shared.codec.encode_exit(event)
                {
                    let _ = outbound.enqueue_output_ordered_control(
                        (key.0.as_str().to_owned(), key.1.as_str().to_owned()),
                        encoded,
                    );
                }
            }
        })
        .map_err(|source| ProcessHostError::new(ProcessHostStage::SpawnConnection, source))
}

fn exit_event(observation: &ProcessExitObservation, output_offset: u64) -> HostExitEvent {
    let (exit_code, supervision_failed) = match observation.outcome {
        ProcessOutcome::Exited(code) => (Some(code), false),
        ProcessOutcome::SupervisionFailed => (None, true),
    };
    HostExitEvent {
        session_id: observation.session_id.as_str().to_owned(),
        process_id: observation.process_id.as_str().to_owned(),
        output_offset,
        exit_code,
        supervision_failed,
        observed_at_millis: observation.observed_at.value(),
    }
}

struct HostOutputRouter {
    codec: HostCodec,
    current: Mutex<Option<Arc<HostOutbound>>>,
}

impl HostOutputRouter {
    fn new(codec: HostCodec) -> Self {
        Self {
            codec,
            current: Mutex::new(None),
        }
    }

    fn install(&self, outbound: Arc<HostOutbound>) {
        if let Some(previous) = self
            .current
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .replace(outbound)
        {
            previous.close();
        }
    }

    fn interested(&self, key: &(SessionId, ProcessInstanceId)) -> Option<Arc<HostOutbound>> {
        self.current
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .filter(|outbound| outbound.is_interested(key))
            .cloned()
    }

    fn close(&self) {
        if let Some(current) = self
            .current
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            current.close();
        }
    }

    fn clear_if_current(&self, outbound: &Arc<HostOutbound>) -> bool {
        let mut current = self
            .current
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if current
            .as_ref()
            .is_some_and(|candidate| Arc::ptr_eq(candidate, outbound))
        {
            current.take();
            true
        } else {
            false
        }
    }
}

impl ProcessOutputObserver for HostOutputRouter {
    fn observe(
        &self,
        session_id: &SessionId,
        process_id: &ProcessInstanceId,
        append: undercurrent_application::OutputAppend,
        bytes: &[u8],
    ) {
        self.observe_owned(session_id, process_id, append, bytes.to_vec());
    }

    fn observe_owned(
        &self,
        session_id: &SessionId,
        process_id: &ProcessInstanceId,
        append: undercurrent_application::OutputAppend,
        bytes: Vec<u8>,
    ) {
        let key = (session_id.clone(), process_id.clone());
        let Some(outbound) = self.interested(&key) else {
            return;
        };
        let output = owned_host_output_frame(session_id, process_id, append.start.value(), bytes);
        let _ = outbound.enqueue_output(output);
    }

    fn observe_cooperative(
        &self,
        session_id: &SessionId,
        process_id: &ProcessInstanceId,
        state: CooperativeForegroundState,
        observed_at: TimestampMillis,
    ) {
        let key = (session_id.clone(), process_id.clone());
        let Some(outbound) = self.interested(&key) else {
            return;
        };
        let event = HostCooperativeEvent {
            session_id: session_id.as_str().to_owned(),
            process_id: process_id.as_str().to_owned(),
            state: state.into(),
            observed_at_millis: observed_at.value(),
        };
        if let Ok(encoded) = self.codec.encode_cooperative(&event) {
            outbound.enqueue_cooperative(key, encoded);
        }
    }
}

fn owned_host_output_frame(
    session_id: &SessionId,
    process_id: &ProcessInstanceId,
    start_offset: u64,
    bytes: Vec<u8>,
) -> HostOutputFrame {
    HostOutputFrame {
        session_id: session_id.as_str().to_owned(),
        process_id: process_id.as_str().to_owned(),
        start_offset,
        bytes,
    }
}

const fn output_coalesce_limit(output_limits: &HostOutputLimits) -> usize {
    output_limits.append_bytes
}

// The listener retains a cancellation handle until the worker finishes. Close
// the socket on worker return too, so that handle cannot delay peer EOF.
struct HostConnectionGuard(Arc<UnixStream>);

impl Drop for HostConnectionGuard {
    fn drop(&mut self) {
        let _ = self.0.shutdown(std::net::Shutdown::Both);
    }
}

fn serve_connection(shared: Arc<HostShared>, mut stream: UnixStream) {
    let Ok(first) = read_frame(&mut stream, shared.codec) else {
        return;
    };
    let HostDecodedFrame::Request(request) = first else {
        return;
    };
    if let HostCommand::CaptureShellEnvironment {
        token,
        session_id,
        shell_pid,
        environment,
        working_directory,
    } = request.command
    {
        if !constant_time_equal(shared.token.as_bytes(), token.as_bytes())
            || !shell_environment::valid_capture_peer(&stream, shell_pid)
        {
            return;
        }
        let response = HostResponse {
            request_id: request.request_id,
            result: shared.capture_environment(
                session_id,
                shell_pid,
                environment,
                working_directory,
            ),
        };
        if let Ok(encoded) = shared.codec.encode_response(&response) {
            let _ = stream.write_all(&encoded);
        }
        return;
    }
    let HostCommand::Authenticate { token } = request.command else {
        return;
    };
    if !constant_time_equal(shared.token.as_bytes(), token.as_bytes()) {
        return;
    }
    let Ok(connection_id) = shared
        .next_connection
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(1)
        })
        .map(|previous| previous + 1)
    else {
        return;
    };
    let writer = match stream.try_clone() {
        Ok(writer) => writer,
        Err(_) => return,
    };
    let outbound = match HostOutbound::start(
        writer,
        shared.codec,
        shared.limits.max_processes.saturating_mul(4).max(16),
        shared.output_limits.global_bytes,
        output_coalesce_limit(&shared.output_limits),
        shared.limits.worker_thread_stack_bytes,
        Arc::clone(&shared.shutdown),
    ) {
        Ok(outbound) => outbound,
        Err(_) => return,
    };
    let authenticated = HostResponse {
        request_id: request.request_id,
        result: Ok(HostResponseValue::Authenticated {
            host_epoch: shared.epoch.clone(),
        }),
    };
    let Ok(encoded) = shared.codec.encode_response(&authenticated) else {
        return;
    };
    if outbound.enqueue_control(encoded).is_err() {
        return;
    }
    shared.router.install(Arc::clone(&outbound));

    let queue_capacity = shared
        .limits
        .max_processes
        .saturating_mul(2)
        .max(shared.limits.spawn_queue_capacity)
        .max(1);
    let (requests, receiver) = async_channel::bounded(queue_capacity);
    let (force_requests, force_receiver) = async_channel::bounded(queue_capacity);
    let (control_requests, control_receiver) = async_channel::bounded(queue_capacity);
    let mut workers = Vec::with_capacity(shared.limits.spawn_workers * 2 + 2);
    for worker in 0..shared.limits.spawn_workers * 2 + 2 {
        let receiver = if worker < shared.limits.spawn_workers {
            receiver.clone()
        } else if worker < shared.limits.spawn_workers * 2 {
            force_receiver.clone()
        } else {
            control_receiver.clone()
        };
        let worker_shared = Arc::clone(&shared);
        let worker_outbound = Arc::clone(&outbound);
        match thread::Builder::new()
            .name(format!("undercurrent-host-request-{worker}"))
            .stack_size(shared.limits.worker_thread_stack_bytes)
            .spawn(move || request_worker(worker_shared, worker_outbound, receiver, connection_id))
        {
            Ok(worker) => workers.push(worker),
            Err(_) => {
                requests.close();
                force_requests.close();
                control_requests.close();
                outbound.close();
                for worker in workers {
                    let _ = worker.join();
                }
                cleanup_connection_spawns(&shared, connection_id);
                return;
            }
        }
    }

    while !outbound.is_closed() && !shared.shutdown.load(Ordering::Acquire) {
        match read_frame(&mut stream, shared.codec) {
            Ok(HostDecodedFrame::Request(request)) => {
                let request_id = request.request_id;
                let queue = match &request.command {
                    HostCommand::ForceStop { .. }
                    | HostCommand::AttemptMutation {
                        mutation: HostAttemptMutation::ForceStop { .. },
                        ..
                    } => &force_requests,
                    HostCommand::Shutdown
                    | HostCommand::Retire { .. }
                    | HostCommand::AcknowledgeAttempt { .. }
                    | HostCommand::ObserveAttempt { .. }
                    | HostCommand::ProbeAttempt { .. }
                    | HostCommand::CancelShutdownAttempt { .. }
                    | HostCommand::AcknowledgeShutdownCancellation { .. }
                    | HostCommand::AttemptMutation {
                        mutation: HostAttemptMutation::Shutdown,
                        ..
                    } => &control_requests,
                    _ => &requests,
                };
                if queue.try_send(request).is_err() {
                    let response = HostResponse {
                        request_id,
                        result: Err(HostFailure {
                            kind: HostFailureKind::CapacityExceeded,
                            context: HostFailureContext::Protocol,
                        }),
                    };
                    if let Ok(encoded) = shared.codec.encode_response(&response) {
                        let _ = outbound.enqueue_control(encoded);
                    }
                }
            }
            Ok(HostDecodedFrame::Input(input)) => {
                let request_id = input.request_id;
                let mutation = HostAttemptMutation::WriteInput {
                    session_id: input.session_id,
                    process_id: input.process_id,
                    bytes: input.bytes,
                    pacing: input.pacing,
                };
                let command = if let Some(attempt) = input.attempt {
                    HostCommand::AttemptMutation { attempt, mutation }
                } else {
                    let HostAttemptMutation::WriteInput {
                        session_id,
                        process_id,
                        bytes,
                        pacing,
                    } = mutation
                    else {
                        unreachable!("input frame maps to write-input mutation")
                    };
                    HostCommand::WriteInput {
                        session_id,
                        process_id,
                        bytes,
                        pacing,
                    }
                };
                let request = HostRequest {
                    request_id,
                    command,
                };
                if requests.try_send(request).is_err() {
                    let response = HostResponse {
                        request_id,
                        result: Err(HostFailure {
                            kind: HostFailureKind::CapacityExceeded,
                            context: HostFailureContext::WriteInput,
                        }),
                    };
                    if let Ok(encoded) = shared.codec.encode_response(&response) {
                        let _ = outbound.enqueue_control(encoded);
                    }
                }
            }
            Ok(_) => break,
            Err(_) => break,
        }
    }
    requests.close();
    force_requests.close();
    control_requests.close();
    outbound.close();
    for worker in workers {
        let _ = worker.join();
    }
    cleanup_connection_spawns(&shared, connection_id);
    if shared.router.clear_if_current(&outbound)
        && shared
            .runtime
            .pressure()
            .is_ok_and(|pressure| pressure.occupied_process_slots == 0)
    {
        shared.shutdown.store(true, Ordering::Release);
        let _ = UnixStream::connect(&shared.socket);
    }
}

fn request_worker(
    shared: Arc<HostShared>,
    outbound: Arc<HostOutbound>,
    requests: async_channel::Receiver<HostRequest>,
    connection_id: u64,
) {
    while let Ok(request) = requests.recv_blocking() {
        if outbound.is_closed() {
            break;
        }
        let request_id = request.request_id;
        let retires_host = command_retires_host(&request.command);
        let output_barrier = command_output_barrier(&request.command);
        #[cfg(test)]
        let response_loss = test_response_loss_point(&request.command);
        let result = handle_command(&shared, &outbound, connection_id, request.command);
        let retirement_committed = retires_host && result.is_ok();
        #[cfg(test)]
        let result = if result.is_ok()
            && response_loss.is_some_and(|point| claim_test_response_loss(&shared.socket, point))
        {
            Err(failure(
                HostFailureKind::Unavailable,
                match response_loss.expect("response-loss point") {
                    TestResponseLossPoint::Attempt => HostFailureContext::AttemptMutation,
                    TestResponseLossPoint::Acknowledge => HostFailureContext::AcknowledgeAttempt,
                },
            ))
        } else {
            result
        };
        let output_barrier = result.is_ok().then_some(output_barrier).flatten();
        let response = HostResponse { request_id, result };
        let Ok(encoded) = shared.codec.encode_response(&response) else {
            if retirement_committed {
                shared.shutdown.request_retirement();
            }
            outbound.close();
            break;
        };
        let queued = if retirement_committed {
            outbound.enqueue_retirement_control(encoded)
        } else if let Some(key) = output_barrier {
            outbound.enqueue_output_ordered_control(key, encoded)
        } else {
            outbound.enqueue_control(encoded)
        };
        if queued.is_err() {
            if retirement_committed {
                shared.shutdown.request_retirement();
            }
            break;
        }
    }
}

fn command_retires_host(command: &HostCommand) -> bool {
    matches!(command, HostCommand::Shutdown)
        || matches!(
            command,
            HostCommand::AcknowledgeAttempt { attempt }
                if attempt.kind == HostAttemptKind::Shutdown
        )
}

fn command_output_barrier(command: &HostCommand) -> Option<(String, String)> {
    match command {
        HostCommand::Replay {
            session_id,
            process_id,
            ..
        } => Some((session_id.clone(), process_id.clone())),
        _ => None,
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum TestResponseLossPoint {
    Attempt,
    Acknowledge,
}

#[cfg(test)]
fn test_response_loss_point(command: &HostCommand) -> Option<TestResponseLossPoint> {
    if std::env::var("UNDERCURRENT_RUNTIME_TEST_RECEIPT_MODE").as_deref() != Ok("response_loss") {
        return None;
    }
    match command {
        HostCommand::AttemptMutation { .. } => Some(TestResponseLossPoint::Attempt),
        HostCommand::AcknowledgeAttempt { .. } => Some(TestResponseLossPoint::Acknowledge),
        _ => None,
    }
}

#[cfg(test)]
fn claim_test_response_loss(socket: &Path, point: TestResponseLossPoint) -> bool {
    let extension = match point {
        TestResponseLossPoint::Attempt => "test-attempt-response-lost",
        TestResponseLossPoint::Acknowledge => "test-ack-response-lost",
    };
    host_files::create_new(&socket.with_extension(extension)).is_ok()
}

fn handle_command(
    shared: &HostShared,
    outbound: &HostOutbound,
    connection_id: u64,
    command: HostCommand,
) -> Result<HostResponseValue, HostFailure> {
    match command {
        HostCommand::Authenticate { .. } => Err(failure(
            HostFailureKind::ProtocolViolation,
            HostFailureContext::Authentication,
        )),
        HostCommand::AttemptMutation { attempt, mutation } => {
            handle_attempt_mutation(shared, outbound, connection_id, attempt, mutation)
        }
        HostCommand::ObserveInputRetirement { attempt } => {
            validate_attempt_context(&attempt)?;
            let exit = attempt.session_id.as_ref().and_then(|session| {
                let session_id = SessionId::new(session.clone()).ok()?;
                let active = shared
                    .runtime
                    .owner
                    .core
                    .state
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .processes
                    .contains_key(&session_id);
                if active || !shared.attempt_receipts.input_is_quiescent(&attempt) {
                    return None;
                }
                shared.terminal_exits.latest_for_session(session)
            });
            Ok(HostResponseValue::InputRetirementObserved {
                process_id: exit.as_ref().map(|exit| exit.process_id.clone()),
                exited_at_millis: exit.map(|exit| exit.observed_at_millis),
            })
        }
        HostCommand::ObserveAttempt { attempt } => {
            validate_attempt_context(&attempt)?;
            if matches!(
                shared
                    .attempt_receipts
                    .probe(&attempt)
                    .map_err(|error| receipt_failure(&error, HostFailureContext::ObserveAttempt))?,
                ReceiptProbe::Unknown
            ) {
                restore_stop_receipt(shared, &attempt)?;
            }
            let observation = shared
                .attempt_receipts
                .observe(&attempt)
                .map_err(|error| receipt_failure(&error, HostFailureContext::ObserveAttempt))?;
            Ok(HostResponseValue::AttemptObserved {
                certainty: observation.certainty,
                result_digest: observation.result_digest,
            })
        }
        HostCommand::ProbeAttempt { attempt } => {
            validate_attempt_context(&attempt)?;
            let probe = shared
                .attempt_receipts
                .probe(&attempt)
                .map_err(|error| receipt_failure(&error, HostFailureContext::ObserveAttempt))?;
            Ok(match probe {
                ReceiptProbe::Absent => HostResponseValue::AttemptProbed {
                    state: HostAttemptProbeState::Absent,
                    response: None,
                },
                ReceiptProbe::Unknown => HostResponseValue::AttemptProbed {
                    state: HostAttemptProbeState::Unknown,
                    response: None,
                },
                ReceiptProbe::Applied { response } => HostResponseValue::AttemptProbed {
                    state: HostAttemptProbeState::Applied,
                    response: Some(Box::new(response)),
                },
            })
        }
        HostCommand::AcknowledgeAttempt { attempt } => {
            validate_attempt_context(&attempt)?;
            shared
                .attempt_receipts
                .acknowledge(&attempt)
                .map_err(|error| receipt_failure(&error, HostFailureContext::AcknowledgeAttempt))?;
            Ok(HostResponseValue::Completed)
        }
        HostCommand::CancelShutdownAttempt { attempt } => {
            validate_attempt_context(&attempt)?;
            shared
                .attempt_receipts
                .cancel_shutdown(&attempt)
                .map_err(|error| receipt_failure(&error, HostFailureContext::ObserveAttempt))?;
            Ok(HostResponseValue::Completed)
        }
        HostCommand::AcknowledgeShutdownCancellation { attempt } => {
            validate_attempt_context(&attempt)?;
            shared
                .attempt_receipts
                .acknowledge_shutdown_cancellation(&attempt)
                .map_err(|error| receipt_failure(&error, HostFailureContext::AcknowledgeAttempt))?;
            Ok(HostResponseValue::Completed)
        }
        HostCommand::CaptureShellEnvironment { .. } => Err(failure(
            HostFailureKind::InvalidData,
            HostFailureContext::Protocol,
        )),
        HostCommand::ShellEnvironmentStatus {
            session_id,
            process_id,
        } => {
            let key = parse_identity(session_id, process_id, HostFailureContext::Execute)?;
            let snapshots = shared
                .shell_environments
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            Ok(HostResponseValue::ShellEnvironmentStatus {
                observed_at_millis: snapshots.get(&key).map(|s| s.observed_at_millis),
            })
        }
        HostCommand::Execute {
            live_environment_process,
            job_id,
            session_id,
            payload_digest,
            program,
            arguments,
            environment,
            working_directory,
            output_bytes,
            deadline_millis,
        } => {
            let session_id = parse_session(session_id, HostFailureContext::Execute)?;
            // Retries retain the environment selected on the original admission.
            if let Some(result) =
                shared
                    .executions
                    .replay(&job_id, session_id.as_str(), &payload_digest)
            {
                return result;
            }
            let live_environment = if let Some(process) = live_environment_process {
                let process = ProcessInstanceId::new(process).map_err(|_| {
                    failure(HostFailureKind::InvalidData, HostFailureContext::Execute)
                })?;
                Some(
                    shared
                        .shell_environments
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .get(&(session_id.clone(), process))
                        .ok_or_else(|| {
                            failure(HostFailureKind::Unavailable, HostFailureContext::Execute)
                        })?
                        .values
                        .clone(),
                )
            } else {
                None
            };
            shared.executions.start(HostExecutionSpec {
                live_environment,

                job_id,
                session_id: session_id.as_str().to_owned(),
                payload_digest,
                program,
                arguments,
                environment,
                working_directory,
                output_bytes,
                deadline_millis,
            })
        }
        HostCommand::PollExecution {
            job_id,
            wait_millis,
        } => shared
            .executions
            .poll(&job_id, Duration::from_millis(wait_millis)),
        HostCommand::CancelExecution { job_id } => shared.executions.cancel(&job_id),
        HostCommand::AcknowledgeExecution { job_id } => shared.executions.acknowledge(&job_id),
        HostCommand::Spawn {
            spawn_token,
            session_id,
            launch,
            secret_environment,
        } => {
            validate_spawn_token(&spawn_token, HostFailureContext::Spawn)?;
            let session_id = parse_session(session_id, HostFailureContext::Spawn)?;
            let terminal_size = launch
                .terminal_size()
                .map_err(|_| failure(HostFailureKind::InvalidData, HostFailureContext::Spawn))?;
            let output_capture_mode = launch.output_capture_mode();
            let output_offset = launch.output_offset;
            let launch = undercurrent_domain::LaunchSpec::try_from(*launch)
                .map_err(|_| failure(HostFailureKind::InvalidData, HostFailureContext::Spawn))?;
            {
                let mut spawns = shared
                    .spawns
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if let Some(existing) = spawns.remove(&spawn_token) {
                    let kind = if matches!(existing, HostedSpawnState::Cancelled { .. }) {
                        HostFailureKind::Cancelled
                    } else {
                        spawns.insert(spawn_token, existing);
                        HostFailureKind::Conflict
                    };
                    return Err(failure(kind, HostFailureContext::Spawn));
                }
                spawns.insert(
                    spawn_token.clone(),
                    HostedSpawnState::Spawning { connection_id },
                );
            }
            // Retiring the previous process discards its host-side buffer. Carry
            // the daemon's absolute checkpoint into this new buffer so replay
            // and live frames cannot be mistaken for already-consumed bytes.
            if shared
                .runtime
                .output_store()
                .begin_session(
                    session_id.clone(),
                    output_capture_mode,
                    undercurrent_application::OutputOffset::new(output_offset),
                )
                .is_err()
            {
                shared
                    .spawns
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .remove(&spawn_token);
                return Err(failure(
                    HostFailureKind::InvalidData,
                    HostFailureContext::Spawn,
                ));
            }
            let process = match block_on(shared.runtime.spawn_with_resolved_secret_environment(
                &session_id,
                &launch,
                secret_environment.into_resolved(),
            )) {
                Ok(process) => process,
                Err(error) => {
                    shared
                        .spawns
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .remove(&spawn_token);
                    return Err(port_failure(&error, HostFailureContext::Spawn));
                }
            };
            let key = (session_id.clone(), process.id.clone());
            let accepted = {
                let mut spawns = shared
                    .spawns
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                match spawns.remove(&spawn_token) {
                    Some(HostedSpawnState::Spawning {
                        connection_id: owner,
                    }) if owner == connection_id => {
                        spawns.insert(
                            spawn_token,
                            HostedSpawnState::Spawned {
                                connection_id,
                                key: key.clone(),
                                adopted: false,
                            },
                        );
                        true
                    }
                    Some(HostedSpawnState::Cancelled { .. }) | None => false,
                    Some(state) => {
                        spawns.insert(spawn_token, state);
                        false
                    }
                }
            };
            if !accepted {
                shared.runtime.cancel_host_spawn(&session_id, &process.id);
                return Err(failure(
                    HostFailureKind::Cancelled,
                    HostFailureContext::Spawn,
                ));
            }
            outbound.add_interest((session_id.clone(), process.id.clone()));
            let (root_pid, root_start_ticks) = shared
                .runtime
                .hosted_process_identity(&session_id, &process.id)
                .map_err(|error| port_failure(&error, HostFailureContext::Spawn))?;
            shared
                .metadata
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(
                    (session_id.clone(), process.id.clone()),
                    HostProcessMetadata {
                        terminal_size,
                        output_capture_mode,
                        root_pid,
                        root_start_ticks,
                    },
                );
            Ok(HostResponseValue::from_process(
                &process,
                root_pid,
                root_start_ticks,
            ))
        }
        HostCommand::AdoptSpawn { spawn_token } => {
            validate_spawn_token(&spawn_token, HostFailureContext::AdoptSpawn)?;
            let mut spawns = shared
                .spawns
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match spawns.get_mut(&spawn_token) {
                Some(HostedSpawnState::Spawned { adopted, .. }) => {
                    *adopted = true;
                    Ok(HostResponseValue::Completed)
                }
                Some(HostedSpawnState::Cancelled { .. }) => Err(failure(
                    HostFailureKind::Cancelled,
                    HostFailureContext::AdoptSpawn,
                )),
                _ => Err(failure(
                    HostFailureKind::NotFound,
                    HostFailureContext::AdoptSpawn,
                )),
            }
        }
        HostCommand::CancelSpawn { spawn_token } => {
            validate_spawn_token(&spawn_token, HostFailureContext::CancelSpawn)?;
            let spawned = {
                let mut spawns = shared
                    .spawns
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                match spawns.remove(&spawn_token) {
                    Some(HostedSpawnState::Spawning {
                        connection_id: owner,
                    }) => {
                        spawns.insert(
                            spawn_token,
                            HostedSpawnState::Cancelled {
                                connection_id: owner,
                            },
                        );
                        None
                    }
                    Some(HostedSpawnState::Spawned { key, .. }) => Some(key),
                    Some(HostedSpawnState::Cancelled { connection_id }) => {
                        spawns.insert(spawn_token, HostedSpawnState::Cancelled { connection_id });
                        None
                    }
                    None => {
                        let cancellation_limit = shared
                            .limits
                            .max_processes
                            .saturating_add(shared.limits.spawn_queue_capacity);
                        if spawns.len() >= cancellation_limit {
                            return Err(failure(
                                HostFailureKind::CapacityExceeded,
                                HostFailureContext::CancelSpawn,
                            ));
                        }
                        spawns.insert(spawn_token, HostedSpawnState::Cancelled { connection_id });
                        None
                    }
                }
            };
            if let Some((session_id, process_id)) = spawned {
                shared.runtime.cancel_host_spawn(&session_id, &process_id);
                outbound.remove_interest(&(session_id.clone(), process_id.clone()));
                shared
                    .metadata
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(&(session_id, process_id));
            }
            Ok(HostResponseValue::Completed)
        }
        HostCommand::ArmExit {
            session_id,
            process_id,
        } => {
            let (session_id, process_id) =
                parse_identity(session_id, process_id, HostFailureContext::ArmExit)?;
            let key = (session_id.clone(), process_id.clone());
            let first_arm = shared
                .armed
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(key.clone());
            if first_arm
                && let Err(error) = block_on(shared.runtime.arm_exit(&session_id, &process_id))
            {
                shared
                    .armed
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(&key);
                return Err(port_failure(&error, HostFailureContext::ArmExit));
            }
            outbound.add_interest(key.clone());
            if let Some(exit) = shared
                .exits
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&key)
                && let Ok(encoded) = shared.codec.encode_exit(exit)
            {
                let _ = outbound.enqueue_control(encoded);
            }
            Ok(HostResponseValue::Completed)
        }
        HostCommand::Stop {
            session_id,
            process_id,
            signal,
            force_after_millis,
            no_escalation,
        } => {
            let (session_id, process_id) =
                parse_identity(session_id, process_id, HostFailureContext::Stop)?;
            let policy = HostCommand::stop_policy(signal, force_after_millis)
                .map_err(|_| failure(HostFailureKind::InvalidData, HostFailureContext::Stop))?
                .with_no_escalation(no_escalation);
            if let Some(exit) = shared
                .terminal_exits
                .find(session_id.as_str(), process_id.as_str())
            {
                return Ok(termination_from_exit(&exit, HostAttemptKind::Stop));
            }
            let terminated = match block_on(shared.runtime.stop(&session_id, &process_id, policy)) {
                Ok(value) => value,
                Err(error) => {
                    if let Some(exit) = shared
                        .terminal_exits
                        .find(session_id.as_str(), process_id.as_str())
                    {
                        return Ok(termination_from_exit(&exit, HostAttemptKind::Stop));
                    }
                    return Err(port_failure(&error, HostFailureContext::Stop));
                }
            };
            Ok(HostResponseValue::from_termination(terminated))
        }
        HostCommand::ForceStop {
            session_id,
            process_id,
        } => {
            let (session_id, process_id) =
                parse_identity(session_id, process_id, HostFailureContext::ForceStop)?;
            if let Some(exit) = shared
                .terminal_exits
                .find(session_id.as_str(), process_id.as_str())
            {
                return Ok(termination_from_exit(&exit, HostAttemptKind::ForceStop));
            }
            let terminated = match block_on(shared.runtime.force_stop(&session_id, &process_id)) {
                Ok(value) => value,
                Err(error) => {
                    if let Some(exit) = shared
                        .terminal_exits
                        .find(session_id.as_str(), process_id.as_str())
                    {
                        return Ok(termination_from_exit(&exit, HostAttemptKind::ForceStop));
                    }
                    return Err(port_failure(&error, HostFailureContext::ForceStop));
                }
            };
            Ok(HostResponseValue::from_termination(terminated))
        }
        HostCommand::WriteInput {
            session_id,
            process_id,
            bytes,
            pacing,
        } => {
            let (session_id, process_id) =
                parse_identity(session_id, process_id, HostFailureContext::WriteInput)?;
            let pause = pacing.map(|pacing| {
                EncodedInputPause::new(
                    pacing.offset as usize,
                    Duration::from_millis(pacing.delay_millis),
                )
            });
            let delivery = EncodedTerminalInput::from_parts(bytes, pause).map_err(|_| {
                failure(HostFailureKind::InvalidData, HostFailureContext::WriteInput)
            })?;
            block_on(
                shared
                    .runtime
                    .write_encoded_input(&session_id, &process_id, delivery),
            )
            .map_err(|error| port_failure(&error, HostFailureContext::WriteInput))?;
            Ok(HostResponseValue::Completed)
        }
        HostCommand::CloseInput {
            session_id,
            process_id,
        } => {
            let (session_id, process_id) =
                parse_identity(session_id, process_id, HostFailureContext::CloseInput)?;
            let closure = block_on(shared.runtime.close_input(&session_id, &process_id))
                .map_err(|error| port_failure(&error, HostFailureContext::CloseInput))?;
            Ok(HostResponseValue::InputClosed {
                already_closed: matches!(closure, StandardInputClosure::AlreadyClosed),
            })
        }
        HostCommand::Resize {
            session_id,
            process_id,
            columns,
            rows,
        } => {
            let (session_id, process_id) =
                parse_identity(session_id, process_id, HostFailureContext::Resize)?;
            let size = undercurrent_domain::TerminalSize::new(columns, rows, u16::MAX, u16::MAX)
                .map_err(|_| failure(HostFailureKind::InvalidData, HostFailureContext::Resize))?;
            block_on(shared.runtime.resize(&session_id, &process_id, size))
                .map_err(|error| port_failure(&error, HostFailureContext::Resize))?;
            let (root_pid, root_start_ticks) = shared
                .runtime
                .hosted_process_identity(&session_id, &process_id)
                .map_err(|error| port_failure(&error, HostFailureContext::Resize))?;
            shared
                .metadata
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .entry((session_id, process_id))
                .and_modify(|metadata| metadata.terminal_size = size)
                .or_insert(HostProcessMetadata {
                    terminal_size: size,
                    output_capture_mode: OutputCaptureMode::Terminal,
                    root_pid,
                    root_start_ticks,
                });
            Ok(HostResponseValue::Completed)
        }
        HostCommand::Observe {
            session_id,
            process_id,
            started_at_millis,
        } => {
            let (session_id, process_id) =
                parse_identity(session_id, process_id, HostFailureContext::Observe)?;
            let key = (session_id.clone(), process_id.clone());
            outbound.add_interest(key.clone());
            if let Some(exit) = shared
                .exits
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&key)
            {
                let state = match exit.exit_code {
                    Some(exit_code) => HostObservedProcessState::Exited {
                        exit_code,
                        observed_at_millis: exit.observed_at_millis,
                    },
                    None => HostObservedProcessState::Missing {
                        observed_at_millis: exit.observed_at_millis,
                    },
                };
                let metadata = shared
                    .metadata
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .get(&key)
                    .copied();
                return Ok(HostResponseValue::Observed {
                    state,
                    columns: metadata.map(|value| value.terminal_size.columns()),
                    rows: metadata.map(|value| value.terminal_size.rows()),
                    output_capture_mode: metadata
                        .map(|value| value.output_capture_mode.into())
                        .unwrap_or_default(),
                    root_pid: metadata.map(|value| value.root_pid),
                    root_start_ticks: metadata.and_then(|value| value.root_start_ticks),
                });
            }
            let identity = ProcessIdentity::new(
                process_id,
                undercurrent_domain::TimestampMillis::new(started_at_millis),
            );
            let state = block_on(shared.runtime.observe(&session_id, &identity))
                .map_err(|error| port_failure(&error, HostFailureContext::Observe))?;
            let root_identity =
                if matches!(state, undercurrent_domain::ProcessTruth::Missing { .. }) {
                    None
                } else {
                    Some(
                        shared
                            .runtime
                            .hosted_process_identity(&session_id, identity.id())
                            .map_err(|error| port_failure(&error, HostFailureContext::Observe))?,
                    )
                };
            let metadata = shared
                .metadata
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&key)
                .copied();
            Ok(HostResponseValue::Observed {
                state: state.into(),
                columns: metadata.map(|value| value.terminal_size.columns()),
                rows: metadata.map(|value| value.terminal_size.rows()),
                output_capture_mode: metadata
                    .map(|value| value.output_capture_mode.into())
                    .unwrap_or_default(),
                root_pid: root_identity.map(|identity| identity.0),
                root_start_ticks: root_identity.and_then(|identity| identity.1),
            })
        }
        HostCommand::ObserveForeground {
            session_id,
            process_id,
        } => {
            let (session_id, process_id) = parse_identity(
                session_id,
                process_id,
                HostFailureContext::ObserveForeground,
            )?;
            let observation = block_on(shared.runtime.observe_foreground(&session_id, &process_id))
                .map_err(|error| port_failure(&error, HostFailureContext::ObserveForeground))?;
            let members = observation
                .members()
                .iter()
                .map(|member| HostForegroundMember {
                    incarnation: member.incarnation().to_owned(),
                    program: member.target().basename().to_owned(),
                    arguments: member.target().arguments().to_vec(),
                    environment_names: member
                        .target()
                        .environment_names()
                        .iter()
                        .map(|name| name.as_str().to_owned())
                        .collect(),
                    environment_names_complete: member.environment_names_complete(),
                })
                .collect();
            #[cfg(windows)]
            let current_directory = shared
                .shell_environments
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(&(session_id.clone(), process_id.clone()))
                .and_then(|snapshot| snapshot.working_directory.clone());
            #[cfg(not(windows))]
            let current_directory = observation
                .current_directory()
                .map(|path| path.as_str().to_owned());
            Ok(HostResponseValue::ForegroundContext {
                context: HostForegroundContext {
                    process_id: observation.process().as_str().to_owned(),
                    observed_at_millis: observation.observed_at().value(),
                    members,
                    completeness: match observation.completeness() {
                        ProcessGroupCompleteness::Complete => {
                            HostProcessGroupCompleteness::Complete
                        }
                        ProcessGroupCompleteness::Truncated => {
                            HostProcessGroupCompleteness::Truncated
                        }
                        ProcessGroupCompleteness::Unavailable => {
                            HostProcessGroupCompleteness::Unavailable
                        }
                    },
                    current_directory,
                    tcp_listener: match observation.tcp_listener() {
                        TcpListenerObservation::Present => HostTcpListenerObservation::Present,
                        TcpListenerObservation::Absent => HostTcpListenerObservation::Absent,
                        TcpListenerObservation::Unavailable => {
                            HostTcpListenerObservation::Unavailable
                        }
                        TcpListenerObservation::Truncated => HostTcpListenerObservation::Truncated,
                    },
                    managed_process_tcp_listener: match observation.managed_process_tcp_listener() {
                        TcpListenerObservation::Present => {
                            Some(HostTcpListenerObservation::Present)
                        }
                        TcpListenerObservation::Absent => Some(HostTcpListenerObservation::Absent),
                        TcpListenerObservation::Truncated => {
                            Some(HostTcpListenerObservation::Truncated)
                        }
                        TcpListenerObservation::Unavailable => None,
                    },
                    managed_tree_tcp_listener: match observation.managed_tree_tcp_listener() {
                        TcpListenerObservation::Present => {
                            Some(HostTcpListenerObservation::Present)
                        }
                        TcpListenerObservation::Absent => Some(HostTcpListenerObservation::Absent),
                        TcpListenerObservation::Truncated => {
                            Some(HostTcpListenerObservation::Truncated)
                        }
                        TcpListenerObservation::Unavailable => None,
                    },
                    cooperative: observation.cooperative().map(Into::into),
                },
            })
        }
        HostCommand::Replay {
            session_id,
            process_id,
            from,
            max_bytes,
        } => replay_output(shared, outbound, session_id, process_id, from, max_bytes),
        HostCommand::Snapshot {
            session_id,
            process_id,
        } => {
            let (session_id, process_id) =
                parse_identity(session_id, process_id, HostFailureContext::Snapshot)?;
            let key = (session_id.clone(), process_id);
            let metadata = shared
                .metadata
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&key)
                .copied()
                .ok_or_else(|| failure(HostFailureKind::NotFound, HostFailureContext::Snapshot))?;
            if metadata.output_capture_mode == OutputCaptureMode::Opaque {
                return Err(failure(
                    HostFailureKind::Conflict,
                    HostFailureContext::Snapshot,
                ));
            }
            let snapshot = block_on(shared.runtime.screen_store().snapshot(&session_id))
                .map_err(|error| port_failure(&error, HostFailureContext::Snapshot))?
                .ok_or_else(|| failure(HostFailureKind::NotFound, HostFailureContext::Snapshot))?;
            let contents = String::from_utf8(snapshot.contents().to_vec())
                .map_err(|_| failure(HostFailureKind::InvalidData, HostFailureContext::Snapshot))?;
            Ok(HostResponseValue::ScreenSnapshot {
                output_offset: snapshot.output_offset().value(),
                columns: snapshot.columns(),
                rows: snapshot.rows(),
                cursor_row: snapshot.cursor_row(),
                cursor_column: snapshot.cursor_column(),
                cursor_visible: snapshot.cursor_visible(),
                alternate_screen: snapshot.alternate_screen(),
                contents,
            })
        }
        HostCommand::Retire {
            session_id,
            process_id,
        } => {
            let (session_id, process_id) =
                parse_identity(session_id, process_id, HostFailureContext::Retire)?;
            let key = (session_id.clone(), process_id.clone());
            shared
                .spawns
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .retain(|_, state| {
                    !matches!(state, HostedSpawnState::Spawned { key: owned, .. } if owned == &key)
                });
            shared
                .shell_environments
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&key);
            outbound.remove_interest(&key);
            shared
                .exits
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&key);
            shared
                .armed
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&key);
            let mut metadata = shared.metadata.lock().unwrap_or_else(|p| p.into_inner());
            metadata.remove(&key);
            shared.runtime.retire_host_process(&session_id, &process_id);
            // Old-incarnation replay must preserve a replacement's output.
            if !metadata.keys().any(|(owner, _)| owner == &session_id) {
                let _ = shared.runtime.output_store().remove_session(&session_id);
                shared.runtime.screen_store().remove_session(&session_id);
            }
            Ok(HostResponseValue::Completed)
        }
        HostCommand::Shutdown => Ok(HostResponseValue::Completed),
    }
}

fn handle_attempt_mutation(
    shared: &HostShared,
    outbound: &HostOutbound,
    connection_id: u64,
    attempt: HostAttemptContext,
    mutation: HostAttemptMutation,
) -> Result<HostResponseValue, HostFailure> {
    let context = validate_attempt_mutation(&attempt, &mutation)?;
    let stop_process = match &mutation {
        HostAttemptMutation::Stop { process_id, .. }
        | HostAttemptMutation::ForceStop { process_id, .. } => Some(process_id.as_str()),
        _ => None,
    };
    match shared
        .attempt_receipts
        .begin_bound(&attempt, stop_process)
        .map_err(|error| receipt_failure(&error, HostFailureContext::AttemptMutation))?
    {
        ReceiptBegin::Started => {}
        ReceiptBegin::KnownAbsent => {
            return Ok(HostResponseValue::AttemptObserved {
                certainty: HostEffectCertainty::KnownAbsent,
                result_digest: None,
            });
        }
        ReceiptBegin::Applied { response, .. } => return Ok(response),
        ReceiptBegin::Unknown => {
            return Ok(HostResponseValue::AttemptObserved {
                certainty: HostEffectCertainty::Unknown,
                result_digest: None,
            });
        }
        ReceiptBegin::Conflict => {
            return Err(failure(
                HostFailureKind::Conflict,
                HostFailureContext::AttemptMutation,
            ));
        }
    }

    let spawn_token = if matches!(&mutation, HostAttemptMutation::Spawn { .. }) {
        Some(PayloadDigest::of_bytes(attempt.attempt_id.as_bytes()).to_hex())
    } else {
        None
    };
    #[cfg(test)]
    if matches!(
        &mutation,
        HostAttemptMutation::Stop { .. } | HostAttemptMutation::ForceStop { .. }
    ) && std::env::var("UNDERCURRENT_RUNTIME_TEST_RECEIPT_MODE").as_deref()
        == Ok("stop_receipt_unknown")
    {
        shared
            .attempt_receipts
            .mark_unknown(&attempt)
            .map_err(|error| receipt_failure(&error, HostFailureContext::AttemptMutation))?;
        return Ok(HostResponseValue::AttemptObserved {
            certainty: HostEffectCertainty::Unknown,
            result_digest: None,
        });
    }
    #[cfg(test)]
    if matches!(&mutation, HostAttemptMutation::WriteInput { .. })
        && std::env::var("UNDERCURRENT_RUNTIME_TEST_RECEIPT_MODE").as_deref()
            == Ok("input_receipt_unknown")
    {
        shared.attempt_receipts.mark_unknown(&attempt).unwrap();
        return Ok(HostResponseValue::AttemptObserved {
            certainty: HostEffectCertainty::Unknown,
            result_digest: None,
        });
    }
    let response = if let HostAttemptMutation::WriteInput {
        session_id,
        process_id,
        bytes,
        pacing,
    } = mutation
    {
        let (session, process) =
            parse_identity(session_id, process_id, HostFailureContext::WriteInput)?;
        let pause = pacing.map(|p| {
            EncodedInputPause::new(p.offset as usize, Duration::from_millis(p.delay_millis))
        });
        let result = match EncodedTerminalInput::from_parts(bytes, pause) {
            Ok(delivery) => block_on(shared.runtime.write_encoded_input_attempt(
                &session,
                &process,
                delivery,
                Some(context.attempt_id()),
            )),
            Err(error) => Err(PortError::new(
                undercurrent_application::PortErrorKind::InvalidData,
                undercurrent_application::PortOperation::WriteInput,
                Some(session),
                error,
            )
            .with_attempt_effect(
                context.attempt_id().clone(),
                undercurrent_domain::EffectCertainty::KnownAbsent,
            )),
        };
        match result {
            Ok(()) => HostResponseValue::Completed,
            Err(error) => {
                let failure = port_failure(&error, HostFailureContext::WriteInput);
                let certainty = if error.effect_certainty()
                    == Some(undercurrent_domain::EffectCertainty::KnownAbsent)
                {
                    // Commit zero-effect proof before replying, so response loss and
                    // host restart cannot turn a rejected write into recovery debt.
                    if let Err(error) = shared
                        .attempt_receipts
                        .complete_absent(&attempt, failure.clone())
                    {
                        return Ok(HostResponseValue::AttemptFailed {
                            certainty: HostEffectCertainty::Unknown,
                            failure: receipt_failure(&error, HostFailureContext::AttemptMutation),
                        });
                    }
                    HostEffectCertainty::KnownAbsent
                } else {
                    let _ = shared
                        .attempt_receipts
                        .mark_unknown_with_failure(&attempt, Some(failure.clone()));
                    HostEffectCertainty::Unknown
                };
                return Ok(HostResponseValue::AttemptFailed { certainty, failure });
            }
        }
    } else if matches!(&mutation, HostAttemptMutation::Shutdown) {
        // Shutdown is a two-phase host effect. The durable applied receipt is
        // returned while the host remains live; only its post-settlement ACK
        // retires the exact host and its owned process tree.
        HostResponseValue::Completed
    } else {
        let command = mutation.into_command(spawn_token.clone());
        match handle_command(shared, outbound, connection_id, command) {
            Ok(response) => response,
            Err(failure) => {
                let _ = shared
                    .attempt_receipts
                    .mark_unknown_with_failure(&attempt, Some(failure.clone()));
                return Ok(HostResponseValue::AttemptFailed {
                    certainty: HostEffectCertainty::Unknown,
                    failure,
                });
            }
        }
    };
    if let Some(spawn_token) = spawn_token
        && handle_command(
            shared,
            outbound,
            connection_id,
            HostCommand::AdoptSpawn { spawn_token },
        )
        .is_err()
    {
        let _ = shared.attempt_receipts.mark_unknown(&attempt);
        return Ok(HostResponseValue::AttemptObserved {
            certainty: HostEffectCertainty::Unknown,
            result_digest: None,
        });
    }
    let result_digest = match attempt_result_digest(&context, &response) {
        Ok(digest) => digest,
        Err(_) => {
            let _ = shared.attempt_receipts.mark_unknown(&attempt);
            return Ok(HostResponseValue::AttemptObserved {
                certainty: HostEffectCertainty::Unknown,
                result_digest: None,
            });
        }
    };
    if let Err(error) =
        shared
            .attempt_receipts
            .complete_applied(&attempt, result_digest.to_hex(), response.clone())
    {
        // This error follows external IO. Even a capacity/validation failure
        // saving the receipt cannot prove absence of the completed effect.
        return Ok(HostResponseValue::AttemptFailed {
            certainty: HostEffectCertainty::Unknown,
            failure: receipt_failure(&error, HostFailureContext::AttemptMutation),
        });
    }
    Ok(response)
}

fn termination_from_exit(event: &HostExitEvent, kind: HostAttemptKind) -> HostResponseValue {
    HostResponseValue::Terminated {
        kind: if kind == HostAttemptKind::ForceStop {
            crate::process_host_protocol::HostTerminationKind::Forced
        } else {
            crate::process_host_protocol::HostTerminationKind::Graceful
        },
        exit_code: event.exit_code,
        observed_at_millis: event.observed_at_millis,
    }
}

/// Restore only a retained stop receipt whose exact incarnation has durable
/// exit evidence. This proves termination, never attribution of the exit signal.
fn restore_stop_receipt(
    shared: &HostShared,
    attempt: &HostAttemptContext,
) -> Result<(), HostFailure> {
    if !matches!(
        attempt.kind,
        HostAttemptKind::Stop | HostAttemptKind::ForceStop
    ) {
        return Ok(());
    }
    let Some(session) = attempt.session_id.as_deref() else {
        return Ok(());
    };
    let process = shared
        .attempt_receipts
        .stop_process(attempt)
        .map_err(|error| receipt_failure(&error, HostFailureContext::ObserveAttempt))?;
    // Legacy receipts without this independently recorded binding remain unknown.
    // A terminal logical session never identifies the target process incarnation.
    let Some(process) = process else {
        return Ok(());
    };
    let exit = shared.terminal_exits.find(session, &process);
    let Some(exit) = exit else {
        return Ok(());
    };
    let response = termination_from_exit(&exit, attempt.kind);
    let digest = attempt_result_digest(&validate_attempt_context(attempt)?, &response)?;
    shared
        .attempt_receipts
        .complete_applied(attempt, digest.to_hex(), response)
        .map_err(|error| receipt_failure(&error, HostFailureContext::ObserveAttempt))
}

impl HostAttemptMutation {
    fn kind(&self) -> HostAttemptKind {
        match self {
            Self::Spawn { .. } => HostAttemptKind::Spawn,
            Self::ArmExit { .. } => HostAttemptKind::ArmExit,
            Self::Stop { .. } => HostAttemptKind::Stop,
            Self::ForceStop { .. } => HostAttemptKind::ForceStop,
            Self::WriteInput { .. } => HostAttemptKind::WriteInput,
            Self::Resize { .. } => HostAttemptKind::Resize,
            Self::Shutdown => HostAttemptKind::Shutdown,
        }
    }

    fn session_id(&self) -> Option<&str> {
        match self {
            Self::Spawn { session_id, .. }
            | Self::ArmExit { session_id, .. }
            | Self::Stop { session_id, .. }
            | Self::ForceStop { session_id, .. }
            | Self::WriteInput { session_id, .. }
            | Self::Resize { session_id, .. } => Some(session_id),
            Self::Shutdown => None,
        }
    }

    fn into_command(self, spawn_token: Option<String>) -> HostCommand {
        match self {
            Self::Spawn {
                session_id,
                launch,
                secret_environment,
            } => HostCommand::Spawn {
                spawn_token: spawn_token.unwrap_or_default(),
                session_id,
                launch,
                secret_environment,
            },
            Self::ArmExit {
                session_id,
                process_id,
            } => HostCommand::ArmExit {
                session_id,
                process_id,
            },
            Self::Stop {
                session_id,
                process_id,
                signal,
                force_after_millis,
                no_escalation,
            } => HostCommand::Stop {
                session_id,
                process_id,
                signal,
                force_after_millis,
                no_escalation,
            },
            Self::ForceStop {
                session_id,
                process_id,
            } => HostCommand::ForceStop {
                session_id,
                process_id,
            },
            Self::WriteInput {
                session_id,
                process_id,
                bytes,
                pacing,
            } => HostCommand::WriteInput {
                session_id,
                process_id,
                bytes,
                pacing,
            },
            Self::Resize {
                session_id,
                process_id,
                columns,
                rows,
            } => HostCommand::Resize {
                session_id,
                process_id,
                columns,
                rows,
            },
            Self::Shutdown => HostCommand::Shutdown,
        }
    }
}

fn validate_attempt_mutation(
    attempt: &HostAttemptContext,
    mutation: &HostAttemptMutation,
) -> Result<ProcessMutationContext, HostFailure> {
    if attempt.kind != mutation.kind()
        || attempt.session_id.as_deref() != mutation.session_id()
        || attempt.kind == HostAttemptKind::ExecuteInSession
    {
        return Err(failure(
            HostFailureKind::InvalidData,
            HostFailureContext::AttemptMutation,
        ));
    }
    validate_attempt_context(attempt)
}

fn validate_attempt_context(
    attempt: &HostAttemptContext,
) -> Result<ProcessMutationContext, HostFailure> {
    let attempt_id = OperationAttemptId::new(attempt.attempt_id.clone()).map_err(|_| {
        failure(
            HostFailureKind::InvalidData,
            HostFailureContext::AttemptMutation,
        )
    })?;
    let target_session = attempt
        .session_id
        .clone()
        .map(SessionId::new)
        .transpose()
        .map_err(|_| {
            failure(
                HostFailureKind::InvalidData,
                HostFailureContext::AttemptMutation,
            )
        })?;
    let fencing_token = FencingToken::new(attempt.fencing_token).map_err(|_| {
        failure(
            HostFailureKind::InvalidData,
            HostFailureContext::AttemptMutation,
        )
    })?;
    let payload_digest = PayloadDigest::from_hex(&attempt.payload_digest).map_err(|_| {
        failure(
            HostFailureKind::InvalidData,
            HostFailureContext::AttemptMutation,
        )
    })?;
    Ok(ProcessMutationContext::new(
        attempt_id,
        target_session,
        attempt_kind(attempt.kind),
        fencing_token,
        payload_digest,
        TimestampMillis::new(attempt.accepted_at_millis),
    ))
}

const fn attempt_kind(kind: HostAttemptKind) -> ProcessMutationKind {
    match kind {
        HostAttemptKind::Spawn => ProcessMutationKind::Spawn,
        HostAttemptKind::ArmExit => ProcessMutationKind::ArmExit,
        HostAttemptKind::Stop => ProcessMutationKind::Stop,
        HostAttemptKind::ForceStop => ProcessMutationKind::ForceStop,
        HostAttemptKind::WriteInput => ProcessMutationKind::WriteInput,
        HostAttemptKind::Resize => ProcessMutationKind::Resize,
        HostAttemptKind::Shutdown => ProcessMutationKind::Shutdown,
        HostAttemptKind::ExecuteInSession => ProcessMutationKind::ExecuteInSession,
    }
}

fn attempt_result_digest(
    context: &ProcessMutationContext,
    response: &HostResponseValue,
) -> Result<PayloadDigest, HostFailure> {
    match (context.kind(), response) {
        (
            ProcessMutationKind::Spawn,
            HostResponseValue::Spawned {
                process_id,
                started_at_millis,
                ..
            },
        ) => {
            let process = ProcessInstance {
                id: ProcessInstanceId::new(process_id.clone()).map_err(|_| {
                    failure(
                        HostFailureKind::ProtocolViolation,
                        HostFailureContext::AttemptMutation,
                    )
                })?,
                started_at: TimestampMillis::new(*started_at_millis),
            };
            Ok(process_instance_result_digest(&process))
        }
        (
            ProcessMutationKind::Stop | ProcessMutationKind::ForceStop,
            HostResponseValue::Terminated {
                kind,
                exit_code,
                observed_at_millis,
            },
        ) => Ok(process_termination_result_digest(&ProcessTermination::new(
            (*kind).into(),
            *exit_code,
            TimestampMillis::new(*observed_at_millis),
        ))),
        (
            ProcessMutationKind::ArmExit
            | ProcessMutationKind::WriteInput
            | ProcessMutationKind::Resize
            | ProcessMutationKind::Shutdown,
            HostResponseValue::Completed,
        ) => Ok(process_unit_result_digest(context)),
        _ => Err(failure(
            HostFailureKind::ProtocolViolation,
            HostFailureContext::AttemptMutation,
        )),
    }
}

fn receipt_failure(error: &ReceiptError, context: HostFailureContext) -> HostFailure {
    let kind = match error {
        ReceiptError::Capacity => HostFailureKind::CapacityExceeded,
        ReceiptError::Conflict => HostFailureKind::Conflict,
        ReceiptError::Compatibility | ReceiptError::Invalid | ReceiptError::Codec(_) => {
            HostFailureKind::InvalidData
        }
        ReceiptError::Io(_) => HostFailureKind::Unavailable,
    };
    failure(kind, context)
}

fn replay_output(
    shared: &HostShared,
    outbound: &HostOutbound,
    session: String,
    process: String,
    from: u64,
    max_bytes: usize,
) -> Result<HostResponseValue, HostFailure> {
    let (session_id, process_id) = parse_identity(session, process, HostFailureContext::Replay)?;
    outbound.add_interest((session_id.clone(), process_id.clone()));
    if let Some(signal) = shared
        .runtime
        .cooperative_signal_store()
        .latest_exact(&session_id, &process_id)
    {
        let event = HostCooperativeEvent {
            session_id: session_id.as_str().to_owned(),
            process_id: process_id.as_str().to_owned(),
            state: signal.foreground().into(),
            observed_at_millis: signal.observed_at().value(),
        };
        if let Ok(encoded) = shared.codec.encode_cooperative(&event) {
            outbound.enqueue_cooperative((session_id.clone(), process_id.clone()), encoded);
        }
    }
    let replay = shared
        .runtime
        .output_store()
        .replay(
            &session_id,
            OutputOffset::new(from),
            max_bytes,
            OutputGapRecovery::RequestScreenSnapshot,
        )
        .map_err(|_| {
            failure(
                HostFailureKind::ReplayUnavailable,
                HostFailureContext::Replay,
            )
        })?;
    match replay {
        OutputReplay::Gap {
            requested,
            available_from,
            next_offset,
            recovery,
        } => Ok(HostResponseValue::Replay {
            requested: requested.value(),
            available_from: available_from.value(),
            delivered_to: available_from.value(),
            next_offset: next_offset.value(),
            recovery: recovery.into(),
            has_more: available_from != next_offset,
        }),
        OutputReplay::Data {
            slices,
            from,
            to,
            next_offset,
            has_more,
        } => {
            let mut delivered_to = from;
            for slice in slices {
                let frame = HostOutputFrame {
                    session_id: session_id.as_str().to_owned(),
                    process_id: process_id.as_str().to_owned(),
                    start_offset: slice.start().value(),
                    bytes: slice.bytes().to_vec(),
                };
                match outbound.enqueue_output(frame) {
                    Ok(()) => {}
                    Err(HostQueueError::Invalid) => {
                        return Err(failure(HostFailureKind::Other, HostFailureContext::Replay));
                    }
                    Err(HostQueueError::Closed | HostQueueError::Full) => break,
                }
                delivered_to = slice.end();
            }
            Ok(HostResponseValue::Replay {
                requested: from.value(),
                available_from: from.value(),
                delivered_to: delivered_to.value(),
                next_offset: next_offset.value(),
                recovery: OutputGapRecovery::RestartAtAvailableOffset.into(),
                has_more: has_more || delivered_to != to,
            })
        }
    }
}

fn parse_session(
    session_id: String,
    context: HostFailureContext,
) -> Result<SessionId, HostFailure> {
    SessionId::new(session_id).map_err(|_| failure(HostFailureKind::InvalidData, context))
}

fn validate_spawn_token(spawn_token: &str, context: HostFailureContext) -> Result<(), HostFailure> {
    if spawn_token.len() != SPAWN_TOKEN_BYTES * 2
        || !spawn_token.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(failure(HostFailureKind::InvalidData, context));
    }
    Ok(())
}

fn cleanup_connection_spawns(shared: &HostShared, connection_id: u64) {
    let abandoned = {
        let mut spawns = shared
            .spawns
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let tokens = spawns
            .iter()
            .filter_map(|(token, state)| match state {
                HostedSpawnState::Spawning {
                    connection_id: owner,
                } if *owner == connection_id => Some(token.clone()),
                HostedSpawnState::Spawned {
                    connection_id: owner,
                    adopted: false,
                    ..
                } if *owner == connection_id => Some(token.clone()),
                HostedSpawnState::Cancelled {
                    connection_id: owner,
                } if *owner == connection_id => Some(token.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        tokens
            .into_iter()
            .filter_map(|token| match spawns.remove(&token) {
                Some(HostedSpawnState::Spawned { key, .. }) => Some(key),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    for (session_id, process_id) in abandoned {
        shared.runtime.cancel_host_spawn(&session_id, &process_id);
        shared
            .metadata
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&(session_id, process_id));
    }
}

fn parse_identity(
    session_id: String,
    process_id: String,
    context: HostFailureContext,
) -> Result<(SessionId, ProcessInstanceId), HostFailure> {
    let session_id = parse_session(session_id, context)?;
    let process_id = ProcessInstanceId::new(process_id)
        .map_err(|_| failure(HostFailureKind::InvalidData, context))?;
    Ok((session_id, process_id))
}

fn port_failure(error: &PortError, context: HostFailureContext) -> HostFailure {
    HostFailure {
        kind: error.kind().into(),
        context,
    }
}

const fn failure(kind: HostFailureKind, context: HostFailureContext) -> HostFailure {
    HostFailure { kind, context }
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn read_frame(
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
    if stream.read_exact(&mut encoded[header.len()..]).is_err() {
        encoded.fill(0);
        return Err(HostProtocolError::TruncatedFrame);
    }
    let decoded = codec.decode(&encoded);
    encoded.fill(0);
    decoded
}

struct HostOutbound {
    codec: HostCodec,
    control_limit: usize,
    output_byte_limit: usize,
    output_coalesce_limit: usize,
    output_coalesce_linger: Duration,
    state: Mutex<HostOutboundState>,
    changed: Condvar,
    shutdown: Mutex<UnixStream>,
    host_shutdown: Arc<HostShutdownSignal>,
}

struct HostOutboundStartLimits {
    control_limit: usize,
    output_byte_limit: usize,
    output_coalesce_limit: usize,
    output_coalesce_linger: Duration,
    stack_bytes: usize,
}

struct HostOutboundState {
    controls: VecDeque<HostControlFrame>,
    cooperative: BTreeMap<(SessionId, ProcessInstanceId), Vec<u8>>,
    output: VecDeque<HostQueuedOutput>,
    output_bytes: usize,
    interests: BTreeSet<(SessionId, ProcessInstanceId)>,
    closed: bool,
    retirement_queued: bool,
}

struct HostControlFrame {
    bytes: Vec<u8>,
    order: HostControlOrder,
    retires_host: bool,
}

enum HostControlOrder {
    Priority,
    AfterAllOutput,
    AfterProcessOutput((String, String)),
}

struct HostQueuedOutput {
    frame: HostOutputFrame,
    encoded_len: usize,
    linger_deadline: Instant,
}

enum HostPendingFrame {
    Encoded(HostControlFrame),
    Output(HostQueuedOutput),
}

enum HostPendingDecision {
    Ready(HostPendingFrame),
    Wait(Option<Duration>),
    Closed,
}

impl HostOutboundState {
    fn new() -> Self {
        Self {
            controls: VecDeque::new(),
            cooperative: BTreeMap::new(),
            output: VecDeque::new(),
            output_bytes: 0,
            interests: BTreeSet::new(),
            closed: false,
            retirement_queued: false,
        }
    }

    #[cfg(test)]
    fn enqueue_output(
        &mut self,
        codec: HostCodec,
        output_coalesce_limit: usize,
        output_byte_limit: usize,
        frame: HostOutputFrame,
    ) -> Result<(), HostQueueError> {
        self.enqueue_output_at(
            codec,
            output_coalesce_limit,
            output_byte_limit,
            frame,
            Instant::now(),
            HOST_OUTPUT_COALESCE_LINGER,
        )
        .map(|_| ())
    }

    fn enqueue_output_at(
        &mut self,
        codec: HostCodec,
        output_coalesce_limit: usize,
        output_byte_limit: usize,
        frame: HostOutputFrame,
        enqueued_at: Instant,
        linger: Duration,
    ) -> Result<bool, HostQueueError> {
        let previous_len = self.output.len();
        let encoded_len = codec
            .encoded_output_len(&frame)
            .map_err(|_| HostQueueError::Invalid)?;
        let merged_encoded_len = self.output.back().and_then(|previous| {
            let previous_end = previous
                .frame
                .start_offset
                .checked_add(u64::try_from(previous.frame.bytes.len()).ok()?)?;
            let merged_payload_len = previous.frame.bytes.len().checked_add(frame.bytes.len())?;
            (previous.frame.session_id == frame.session_id
                && previous.frame.process_id == frame.process_id
                && previous_end == frame.start_offset
                && enqueued_at < previous.linger_deadline
                && merged_payload_len <= output_coalesce_limit)
                .then(|| previous.encoded_len.checked_add(frame.bytes.len()))
                .flatten()
                .filter(|merged| *merged <= codec.maximum_frame_bytes())
        });
        let next_bytes = match (self.output.back(), merged_encoded_len) {
            (Some(previous), Some(merged)) => self
                .output_bytes
                .checked_sub(previous.encoded_len)
                .and_then(|bytes| bytes.checked_add(merged)),
            _ => self.output_bytes.checked_add(encoded_len),
        }
        .ok_or(HostQueueError::Full)?;
        if next_bytes > output_byte_limit {
            return Err(HostQueueError::Full);
        }
        self.output_bytes = next_bytes;
        if let Some(merged) = merged_encoded_len {
            let Some(previous) = self.output.back_mut() else {
                return Err(HostQueueError::Invalid);
            };
            previous.frame.bytes.extend(frame.bytes);
            previous.encoded_len = merged;
        } else {
            self.output.push_back(HostQueuedOutput {
                frame,
                encoded_len,
                linger_deadline: enqueued_at.checked_add(linger).unwrap_or(enqueued_at),
            });
        }
        let wake_writer = if merged_encoded_len.is_some() {
            self.output.len() == 1
                && self
                    .output
                    .front()
                    .is_some_and(|output| output.frame.bytes.len() >= output_coalesce_limit)
        } else {
            previous_len <= 1
        };
        Ok(wake_writer)
    }

    fn pop_output(&mut self) -> Option<HostQueuedOutput> {
        let frame = self.output.pop_front()?;
        self.output_bytes = self.output_bytes.saturating_sub(frame.encoded_len);
        Some(frame)
    }

    fn next_pending_at(
        &mut self,
        now: Instant,
        output_coalesce_limit: usize,
    ) -> HostPendingDecision {
        let control_waits_for_output =
            self.controls
                .front()
                .is_some_and(|frame| match &frame.order {
                    HostControlOrder::Priority => false,
                    HostControlOrder::AfterAllOutput => !self.output.is_empty(),
                    HostControlOrder::AfterProcessOutput((session_id, process_id)) => {
                        self.output.iter().any(|output| {
                            output.frame.session_id == *session_id
                                && output.frame.process_id == *process_id
                        })
                    }
                });
        if !control_waits_for_output && let Some(frame) = self.controls.pop_front() {
            return HostPendingDecision::Ready(HostPendingFrame::Encoded(frame));
        }
        if let Some(key) = self.cooperative.keys().next().cloned() {
            return HostPendingDecision::Ready(HostPendingFrame::Encoded(HostControlFrame {
                bytes: self
                    .cooperative
                    .remove(&key)
                    .expect("selected cooperative frame remains present"),
                order: HostControlOrder::Priority,
                retires_host: false,
            }));
        }
        let Some(front) = self.output.front() else {
            return if self.closed {
                HostPendingDecision::Closed
            } else {
                HostPendingDecision::Wait(None)
            };
        };
        let wait = if self.closed
            || self.retirement_queued
            || control_waits_for_output
            || self.output.len() > 1
            || front.frame.bytes.len() >= output_coalesce_limit
        {
            None
        } else {
            front
                .linger_deadline
                .checked_duration_since(now)
                .filter(|remaining| !remaining.is_zero())
        };
        if let Some(remaining) = wait {
            HostPendingDecision::Wait(Some(remaining))
        } else {
            HostPendingDecision::Ready(HostPendingFrame::Output(
                self.pop_output()
                    .expect("selected output frame remains present"),
            ))
        }
    }
}

impl HostOutbound {
    fn start(
        stream: UnixStream,
        codec: HostCodec,
        control_limit: usize,
        output_byte_limit: usize,
        output_coalesce_limit: usize,
        stack_bytes: usize,
        host_shutdown: Arc<HostShutdownSignal>,
    ) -> Result<Arc<Self>, std::io::Error> {
        Self::start_with_linger(
            stream,
            codec,
            HostOutboundStartLimits {
                control_limit,
                output_byte_limit,
                output_coalesce_limit,
                output_coalesce_linger: HOST_OUTPUT_COALESCE_LINGER,
                stack_bytes,
            },
            host_shutdown,
        )
    }

    fn start_with_linger(
        stream: UnixStream,
        codec: HostCodec,
        limits: HostOutboundStartLimits,
        host_shutdown: Arc<HostShutdownSignal>,
    ) -> Result<Arc<Self>, std::io::Error> {
        let shutdown = stream.try_clone()?;
        let outbound = Arc::new(Self {
            codec,
            control_limit: limits.control_limit,
            output_byte_limit: limits.output_byte_limit,
            output_coalesce_limit: limits.output_coalesce_limit,
            output_coalesce_linger: limits.output_coalesce_linger,
            state: Mutex::new(HostOutboundState::new()),
            changed: Condvar::new(),
            shutdown: Mutex::new(shutdown),
            host_shutdown,
        });
        let worker = Arc::clone(&outbound);
        thread::Builder::new()
            .name("undercurrent-host-writer".to_owned())
            .stack_size(limits.stack_bytes)
            .spawn(move || worker.write_loop(stream))?;
        Ok(outbound)
    }

    fn enqueue_control(&self, frame: Vec<u8>) -> Result<(), HostQueueError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.closed || state.retirement_queued {
            return Err(HostQueueError::Closed);
        }
        if state.controls.len() >= self.control_limit {
            return Err(HostQueueError::Full);
        }
        state.controls.push_back(HostControlFrame {
            bytes: frame,
            order: HostControlOrder::Priority,
            retires_host: false,
        });
        self.changed.notify_one();
        Ok(())
    }

    fn enqueue_output_ordered_control(
        &self,
        key: (String, String),
        frame: Vec<u8>,
    ) -> Result<(), HostQueueError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.closed || state.retirement_queued {
            return Err(HostQueueError::Closed);
        }
        if state.controls.len() >= self.control_limit {
            return Err(HostQueueError::Full);
        }
        state.controls.push_back(HostControlFrame {
            bytes: frame,
            order: HostControlOrder::AfterProcessOutput(key),
            retires_host: false,
        });
        self.changed.notify_one();
        Ok(())
    }

    fn enqueue_retirement_control(&self, frame: Vec<u8>) -> Result<(), HostQueueError> {
        // Durable shutdown acknowledgement already committed before this call.
        // Mark storage retirement before the response can reach the client: an
        // immediate peer close may otherwise wake the host's ordinary idle-exit
        // path before the writer requests terminal retirement.
        self.host_shutdown.mark_storage_retirement();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.closed || state.retirement_queued {
            return Err(HostQueueError::Closed);
        }
        if state.controls.len() >= self.control_limit {
            return Err(HostQueueError::Full);
        }
        state.retirement_queued = true;
        state.controls.push_back(HostControlFrame {
            bytes: frame,
            order: HostControlOrder::AfterAllOutput,
            retires_host: true,
        });
        self.changed.notify_one();
        Ok(())
    }

    fn enqueue_output(&self, frame: HostOutputFrame) -> Result<(), HostQueueError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.closed || state.retirement_queued {
            return Err(HostQueueError::Closed);
        }
        let wake_writer = state.enqueue_output_at(
            self.codec,
            self.output_coalesce_limit,
            self.output_byte_limit,
            frame,
            Instant::now(),
            self.output_coalesce_linger,
        )?;
        if wake_writer {
            self.changed.notify_one();
        }
        Ok(())
    }

    fn enqueue_cooperative(&self, key: (SessionId, ProcessInstanceId), frame: Vec<u8>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.closed || state.retirement_queued {
            return;
        }
        state.cooperative.insert(key, frame);
        self.changed.notify_one();
    }

    fn add_interest(&self, key: (SessionId, ProcessInstanceId)) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .interests
            .insert(key);
    }

    fn remove_interest(&self, key: &(SessionId, ProcessInstanceId)) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.interests.remove(key);
        state.cooperative.remove(key);
    }

    fn is_interested(&self, key: &(SessionId, ProcessInstanceId)) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .interests
            .contains(key)
    }

    fn is_closed(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .closed
    }

    fn close(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.closed {
            return;
        }
        state.closed = true;
        self.changed.notify_all();
        drop(state);
        if let Ok(stream) = self.shutdown.lock() {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
    }

    fn write_loop(&self, mut stream: UnixStream) {
        loop {
            let frame = {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                loop {
                    match state.next_pending_at(Instant::now(), self.output_coalesce_limit) {
                        HostPendingDecision::Ready(frame) => break Some(frame),
                        HostPendingDecision::Closed => break None,
                        HostPendingDecision::Wait(None) => {
                            state = self
                                .changed
                                .wait(state)
                                .unwrap_or_else(|poisoned| poisoned.into_inner());
                        }
                        HostPendingDecision::Wait(Some(remaining)) => {
                            let (next, _) = self
                                .changed
                                .wait_timeout(state, remaining)
                                .unwrap_or_else(|poisoned| poisoned.into_inner());
                            state = next;
                        }
                    }
                }
            };
            let Some(frame) = frame else {
                return;
            };
            let frame = match frame {
                HostPendingFrame::Encoded(frame) => frame,
                HostPendingFrame::Output(output) => {
                    let Ok(bytes) = self.codec.encode_output(&output.frame) else {
                        self.close();
                        return;
                    };
                    if bytes.len() != output.encoded_len {
                        self.close();
                        return;
                    }
                    HostControlFrame {
                        bytes,
                        order: HostControlOrder::Priority,
                        retires_host: false,
                    }
                }
            };
            // A selected output frame is the only output allowed outside queue accounting. A
            // later control cannot preempt its blocking write, so production head-of-line
            // exposure is bounded to one append/codec-bounded frame rather than by the linger.
            let write = stream.write_all(&frame.bytes);
            if frame.retires_host {
                self.host_shutdown.request_retirement();
            }
            if write.is_err() {
                self.close();
                return;
            }
            if frame.retires_host {
                return;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HostQueueError {
    Closed,
    Full,
    Invalid,
}

struct ThreadWake(thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}

use std::future::Future;

#[cfg(test)]
mod tests {
    use super::*;

    fn stream_pair() -> std::io::Result<(UnixStream, UnixStream)> {
        #[cfg(unix)]
        {
            UnixStream::pair()
        }
        #[cfg(windows)]
        {
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("test-pair");
            let listener = UnixListener::bind(&path)?;
            let client = UnixStream::connect(&path)?;
            let (server, _) = listener.accept()?;
            Ok((server, client))
        }
    }

    fn test_bootstrap(socket: PathBuf) -> ProcessHostBootstrap {
        ProcessHostBootstrap {
            journal_base: None,
            scrollback_lines: 0,
            socket,
            token: "deadbeef".to_owned(),
            epoch: "epoch".to_owned(),
            limits: HostProcessLimits {
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
            output: HostOutputLimits {
                per_session_bytes: 1024,
                global_bytes: 1024,
                chunk_bytes: 256,
                append_bytes: 1024,
                replay_bytes: 1024,
                sessions: 1,
                subscriptions: 1,
            },
            force_confirmation_timeout_millis: 100,
        }
    }
    #[test]
    fn bootstrap_debug_redacts_paths_and_token() {
        let bootstrap = test_bootstrap(PathBuf::from("/private/session-host.sock"));
        let debug = format!("{bootstrap:?}");
        assert!(!debug.contains("deadbeef"));
        assert!(!debug.contains("/private"));
    }

    #[test]
    fn token_comparison_requires_equal_content_and_length() {
        assert!(constant_time_equal(b"abcd", b"abcd"));
        assert!(!constant_time_equal(b"abcd", b"abce"));
        assert!(!constant_time_equal(b"abcd", b"abc"));
    }

    #[cfg(windows)]
    #[test]
    fn windows_host_authenticates_reconnects_and_rejects_duplicate_ownership() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("private");
        drop(undercurrent_platform_windows::prepare_private_directory(&directory).unwrap());
        let socket = directory.join("host.pipe");
        let mut bootstrap = test_bootstrap(socket.clone());
        bootstrap.limits.worker_thread_stack_bytes = 512 * 1024;
        let codec = bootstrap.codec().unwrap();
        let worker = thread::spawn(move || run_process_host(bootstrap));
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut stream = loop {
            if ProcessHostBootstrap::authentication_path(&socket).exists()
                && let Ok(stream) = UnixStream::connect(&socket)
            {
                break stream;
            }
            if worker.is_finished() {
                panic!(
                    "host stopped before accepting connections: {:?}",
                    worker.join().unwrap()
                );
            }
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(5));
        };
        fn request(stream: &mut UnixStream, codec: HostCodec, command: HostCommand) {
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            stream
                .write_all(
                    &codec
                        .encode_request(&HostRequest {
                            request_id: 1,
                            command,
                        })
                        .unwrap(),
                )
                .unwrap();
            let response = read_frame(stream, codec).expect("host response");
            assert!(
                matches!(response, HostDecodedFrame::Response(response) if response.result.is_ok())
            );
        }
        request(
            &mut stream,
            codec,
            HostCommand::Authenticate {
                token: "deadbeef".into(),
            },
        );
        let mut duplicate = test_bootstrap(socket.clone());
        duplicate.token = "bad0bad0".into();
        assert!(run_process_host(duplicate).is_err());
        assert_eq!(
            fs::read_to_string(ProcessHostBootstrap::authentication_path(&socket)).unwrap(),
            "deadbeef"
        );
        let previous = stream;
        let mut stream = UnixStream::connect(&socket).unwrap();
        request(
            &mut stream,
            codec,
            HostCommand::Authenticate {
                token: "deadbeef".into(),
            },
        );
        drop(previous);
        request(&mut stream, codec, HostCommand::Shutdown);
        drop(stream);
        let deadline = Instant::now() + Duration::from_secs(10);
        while !worker.is_finished() {
            assert!(Instant::now() < deadline, "host did not retire");
            thread::sleep(Duration::from_millis(5));
        }
        worker.join().unwrap().unwrap();
        assert!(!ProcessHostBootstrap::authentication_path(&socket).exists());
    }

    #[test]
    fn process_host_stage_registry_is_complete() {
        let names: BTreeSet<_> = ProcessHostStage::ALL
            .iter()
            .map(|stage| stage.as_str())
            .collect();
        assert_eq!(names.len(), ProcessHostStage::ALL.len());
    }

    #[test]
    fn outbound_queue_coalesces_to_the_append_bound_without_losing_later_bytes() {
        let bootstrap = ProcessHostBootstrap {
            journal_base: None,
            scrollback_lines: 0,
            socket: PathBuf::from("/tmp/undercurrent-output-coalesce.sock"),
            token: "token".to_owned(),
            epoch: "epoch".to_owned(),
            limits: HostProcessLimits {
                max_processes: 1,
                spawn_workers: 1,
                spawn_queue_capacity: 1,
                input_queue_sequences: 1,
                input_queue_bytes_per_process: 1_024,
                output_read_bytes: 16 * 1_024,
                screen_snapshot_bytes: 1_024,
                environment_file_bytes: 1_024,
                environment_entries: 1,
                environment_bytes: 1_024,
                worker_thread_stack_bytes: 64 * 1_024,
            },
            output: HostOutputLimits {
                per_session_bytes: 256 * 1_024,
                global_bytes: 256 * 1_024,
                chunk_bytes: 16 * 1_024,
                append_bytes: 64 * 1_024,
                replay_bytes: 256 * 1_024,
                sessions: 1,
                subscriptions: 1,
            },
            force_confirmation_timeout_millis: 100,
        };
        assert_eq!(bootstrap.limits.output_read_bytes, 16 * 1_024);
        let coalesce_limit = output_coalesce_limit(&bootstrap.output);
        assert_eq!(coalesce_limit, 64 * 1_024);

        let codec = HostCodec::new(128 * 1024).expect("codec");
        let mut state = HostOutboundState::new();
        let mut expected_first = Vec::new();
        let mut expected_tail = Vec::new();
        let started = Instant::now();
        let linger = Duration::from_millis(1);
        for index in 0..5_u64 {
            let bytes = (0..16_384)
                .map(|offset| ((u64::from(offset as u32) + index) % 256) as u8)
                .collect::<Vec<_>>();
            if index < 4 {
                expected_first.extend_from_slice(&bytes);
            } else {
                expected_tail.extend_from_slice(&bytes);
            }
            state
                .enqueue_output_at(
                    codec,
                    coalesce_limit,
                    256 * 1_024,
                    test_output("session-1", "process-1", index * 16_384, bytes),
                    started + Duration::from_micros(index * 100),
                    linger,
                )
                .expect("enqueue output");
        }

        assert_eq!(
            state.output.len(),
            2,
            "one append-bound batch is ready while the sparse tail remains exactly queued"
        );
        assert_eq!(
            state.output_bytes,
            state
                .output
                .iter()
                .map(|frame| frame.encoded_len)
                .sum::<usize>()
        );
        let first = state.pop_output().expect("first output");
        assert_eq!(first.frame.bytes.len(), 64 * 1_024);
        let encoded = codec.encode_output(&first.frame).expect("encode output");
        assert_eq!(encoded.len(), first.encoded_len);
        let HostDecodedFrame::Output(decoded) = codec.decode(&encoded).expect("decode output")
        else {
            panic!("decoded wrong frame kind");
        };
        assert_eq!(decoded.start_offset, 0);
        assert_eq!(decoded.bytes, expected_first);
        let tail = state.pop_output().expect("remaining sparse tail");
        assert_eq!(tail.frame.start_offset, 64 * 1_024);
        assert_eq!(tail.frame.bytes, expected_tail);
        assert_eq!(state.output_bytes, 0);
    }

    #[test]
    fn host_owned_output_preserves_proportional_visible_allocation_through_first_queue_admission() {
        let session_id = SessionId::new("session-1").expect("session");
        let process_id = ProcessInstanceId::new("process-1").expect("process");
        let mut raw = Vec::with_capacity(16_384);
        raw.push(b'v');
        raw.extend_from_slice(b"\x1b]9091;uc=running;cmd=");
        raw.resize(16_382, b'x');
        raw.extend_from_slice(b"\x1b\\");
        assert_eq!(raw.len(), 16_384);

        let filtered = crate::cooperative::CooperativeOscFilter::new().filter(&raw);
        assert_eq!(filtered.visible, b"v");
        assert!(
            filtered.visible.capacity() <= 64,
            "one visible byte retained capacity {} from {} filtered input bytes",
            filtered.visible.capacity(),
            raw.len()
        );
        let visible = filtered.visible;
        let visible_capacity = visible.capacity();
        let allocation = visible.as_ptr();
        let output = owned_host_output_frame(&session_id, &process_id, 0, visible);
        assert_eq!(output.bytes.as_ptr(), allocation);
        assert_eq!(output.bytes.capacity(), visible_capacity);

        let codec = HostCodec::new(4_096).expect("codec");
        let mut state = HostOutboundState::new();
        state
            .enqueue_output(codec, 1_024, 4_096, output)
            .expect("first queue admission");
        let queued = state.output.front().expect("queued output");
        assert_eq!(queued.frame.bytes.as_ptr(), allocation);
        assert_eq!(queued.frame.bytes.len(), 1);
        assert_eq!(queued.frame.bytes.capacity(), visible_capacity);
        assert!(queued.frame.bytes.capacity() <= 64);
        assert_eq!(state.output_bytes, queued.encoded_len);
    }

    #[test]
    fn outbound_queue_never_coalesces_across_bound_gap_or_process_identity() {
        let codec = HostCodec::new(4_096).expect("codec");
        let capacity_frame = test_output("capacity", "process-1", 0, vec![0, 1, 2, 3]);
        let capacity = codec
            .encoded_output_len(&capacity_frame)
            .expect("encoded output length");
        let mut capacity_state = HostOutboundState::new();
        assert_eq!(
            capacity_state.enqueue_output(codec, 8, capacity - 1, capacity_frame.clone()),
            Err(HostQueueError::Full)
        );
        assert!(capacity_state.output.is_empty());
        assert_eq!(capacity_state.output_bytes, 0);
        capacity_state
            .enqueue_output(codec, 8, capacity, capacity_frame)
            .expect("exact encoded capacity");
        assert_eq!(capacity_state.output_bytes, capacity);

        let mut state = HostOutboundState::new();
        for frame in [
            test_output("session-1", "process-1", 0, vec![0, 1, 2, 3]),
            test_output("session-1", "process-1", 4, vec![4, 5, 6, 7]),
            test_output("session-1", "process-1", 8, vec![8]),
            test_output("session-1", "process-1", 20, vec![20]),
            test_output("session-2", "process-1", 21, vec![21]),
            test_output("session-2", "process-2", 22, vec![22]),
        ] {
            state
                .enqueue_output(codec, 8, 64 * 1_024, frame)
                .expect("enqueue output");
        }

        assert_eq!(state.output.len(), 5);
        assert_eq!(state.output[0].frame.bytes, vec![0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(state.output[1].frame.start_offset, 8);
        assert_eq!(state.output[2].frame.start_offset, 20);
        assert_eq!(state.output[3].frame.session_id, "session-2");
        assert_eq!(state.output[4].frame.process_id, "process-2");
    }

    #[test]
    fn outbound_queue_keeps_one_remaining_frame_exact_when_production_closes() {
        let codec = HostCodec::new(4_096).expect("codec");
        let mut state = HostOutboundState::new();
        let frame = test_output("session-1", "process-1", 44, vec![0, 255, 1, 0]);
        state
            .enqueue_output(codec, 1_024, 4_096, frame.clone())
            .expect("enqueue output");
        state.closed = true;

        let remaining = state.pop_output().expect("remaining output");
        assert_eq!(remaining.frame, frame);
        assert!(state.pop_output().is_none());
        assert_eq!(state.output_bytes, 0);
    }

    #[test]
    fn outbound_queue_serializes_concurrent_producers_without_loss_or_bad_accounting() {
        let codec = HostCodec::new(4_096).expect("codec");
        let state = Arc::new(Mutex::new(HostOutboundState::new()));
        let barrier = Arc::new(std::sync::Barrier::new(9));
        let mut workers = Vec::new();
        for index in 0..8_u8 {
            let state = Arc::clone(&state);
            let barrier = Arc::clone(&barrier);
            workers.push(thread::spawn(move || {
                barrier.wait();
                state
                    .lock()
                    .expect("output state")
                    .enqueue_output(
                        codec,
                        1_024,
                        64 * 1_024,
                        test_output(
                            &format!("session-{index}"),
                            "process-1",
                            0,
                            vec![index, 0, 255],
                        ),
                    )
                    .expect("enqueue output");
            }));
        }
        barrier.wait();
        for worker in workers {
            worker.join().expect("output producer");
        }

        let mut state = state.lock().expect("output state");
        assert_eq!(state.output.len(), 8);
        assert_eq!(
            state.output_bytes,
            state
                .output
                .iter()
                .map(|frame| frame.encoded_len)
                .sum::<usize>()
        );
        let mut payloads = BTreeSet::new();
        while let Some(output) = state.pop_output() {
            payloads.insert(output.frame.bytes);
        }
        assert_eq!(payloads.len(), 8);
        assert_eq!(state.output_bytes, 0);
    }

    #[test]
    fn outbound_half_bound_releases_before_linger_and_closes_the_batch() {
        let codec = HostCodec::new(4_096).expect("codec");
        let linger = Duration::from_millis(1);
        let started = Instant::now();
        let mut state = HostOutboundState::new();
        assert!(
            state
                .enqueue_output_at(
                    codec,
                    8,
                    4_096,
                    test_output("session-1", "process-1", 0, vec![0, 1, 2, 3]),
                    started,
                    linger,
                )
                .expect("first output")
        );
        assert!(
            state
                .enqueue_output_at(
                    codec,
                    8,
                    4_096,
                    test_output("session-1", "process-1", 4, vec![4, 5, 6, 7]),
                    started + Duration::from_micros(800),
                    linger,
                )
                .expect("merged output")
        );
        assert!(
            state
                .enqueue_output_at(
                    codec,
                    8,
                    4_096,
                    test_output("session-1", "process-1", 8, vec![8, 9]),
                    started + Duration::from_micros(900),
                    linger,
                )
                .expect("ready batch rejects a third merge")
        );
        assert_eq!(state.output.len(), 2);

        let HostPendingDecision::Ready(HostPendingFrame::Output(output)) =
            state.next_pending_at(started + Duration::from_micros(900), 8)
        else {
            panic!("the first contiguous successor releases the merged output immediately");
        };
        assert_eq!(output.frame.bytes, vec![0, 1, 2, 3, 4, 5, 6, 7]);
        assert!(matches!(
            state.next_pending_at(started + Duration::from_micros(999), 8),
            HostPendingDecision::Wait(Some(remaining))
                if remaining == Duration::from_micros(901)
        ));
        let HostPendingDecision::Ready(HostPendingFrame::Output(tail)) =
            state.next_pending_at(started + Duration::from_micros(1_900), 8)
        else {
            panic!("the separate sparse tail releases at its own deadline");
        };
        assert_eq!(tail.frame.bytes, vec![8, 9]);
        assert_eq!(state.output_bytes, 0);
    }

    #[test]
    fn outbound_linger_never_merges_output_admitted_at_or_after_deadline() {
        let codec = HostCodec::new(4_096).expect("codec");
        let linger = Duration::from_millis(1);
        let started = Instant::now();
        for admitted_at in [
            started + linger,
            started + linger + Duration::from_micros(1),
        ] {
            let mut state = HostOutboundState::new();
            state
                .enqueue_output_at(
                    codec,
                    1_024,
                    4_096,
                    test_output("session-1", "process-1", 0, vec![0, 1, 2, 3]),
                    started,
                    linger,
                )
                .expect("first output");
            assert!(
                state
                    .enqueue_output_at(
                        codec,
                        1_024,
                        4_096,
                        test_output("session-1", "process-1", 4, vec![4, 5, 6, 7]),
                        admitted_at,
                        linger,
                    )
                    .expect("late output creates a ready boundary")
            );
            assert_eq!(state.output.len(), 2);
            let HostPendingDecision::Ready(HostPendingFrame::Output(first)) =
                state.next_pending_at(admitted_at, 1_024)
            else {
                panic!("expired front is released without accepting late bytes");
            };
            assert_eq!(first.frame.bytes, vec![0, 1, 2, 3]);
            assert!(matches!(
                state.next_pending_at(admitted_at, 1_024),
                HostPendingDecision::Wait(Some(_))
            ));
        }
    }

    #[test]
    fn outbound_linger_releases_at_append_and_nonmergeable_boundaries() {
        let codec = HostCodec::new(4_096).expect("codec");
        let linger = Duration::from_millis(1);
        let started = Instant::now();
        let mut bounded = HostOutboundState::new();
        bounded
            .enqueue_output_at(
                codec,
                8,
                4_096,
                test_output("session-1", "process-1", 0, vec![0, 1, 2, 3]),
                started,
                linger,
            )
            .expect("first bounded output");
        assert!(
            bounded
                .enqueue_output_at(
                    codec,
                    8,
                    4_096,
                    test_output("session-1", "process-1", 4, vec![4, 5, 6, 7]),
                    started,
                    linger,
                )
                .expect("append-bound output wakes writer")
        );
        let HostPendingDecision::Ready(HostPendingFrame::Output(output)) =
            bounded.next_pending_at(started, 8)
        else {
            panic!("append-bound output is immediately ready");
        };
        assert_eq!(output.frame.bytes, vec![0, 1, 2, 3, 4, 5, 6, 7]);

        for boundary in [
            test_output("session-1", "process-1", 20, vec![8]),
            test_output("session-2", "process-1", 4, vec![8]),
            test_output("session-1", "process-2", 4, vec![8]),
        ] {
            let mut state = HostOutboundState::new();
            state
                .enqueue_output_at(
                    codec,
                    1_024,
                    4_096,
                    test_output("session-1", "process-1", 0, vec![0, 1, 2, 3]),
                    started,
                    linger,
                )
                .expect("first output");
            assert!(
                state
                    .enqueue_output_at(codec, 1_024, 4_096, boundary, started, linger,)
                    .expect("boundary wakes writer")
            );
            let HostPendingDecision::Ready(HostPendingFrame::Output(first)) =
                state.next_pending_at(started, 1_024)
            else {
                panic!("nonmergeable boundary releases front");
            };
            assert_eq!(first.frame.bytes, vec![0, 1, 2, 3]);
            assert!(matches!(
                state.next_pending_at(started, 1_024),
                HostPendingDecision::Wait(Some(_))
            ));
        }

        let mut oversized = HostOutboundState::new();
        oversized
            .enqueue_output_at(
                codec,
                8,
                4_096,
                test_output("session-1", "process-1", 0, vec![0; 9]),
                started,
                linger,
            )
            .expect("oversized singleton remains representable");
        assert!(matches!(
            oversized.next_pending_at(started, 8),
            HostPendingDecision::Ready(HostPendingFrame::Output(_))
        ));
    }

    #[test]
    fn outbound_linger_preserves_priority_close_and_byte_accounting() {
        let codec = HostCodec::new(4_096).expect("codec");
        let linger = Duration::from_millis(1);
        let started = Instant::now();
        let mut state = HostOutboundState::new();
        state
            .enqueue_output_at(
                codec,
                1_024,
                4_096,
                test_output("session-1", "process-1", 0, vec![0, 1, 2, 3]),
                started,
                linger,
            )
            .expect("output");
        let accounted = state.output_bytes;
        assert!(accounted > 0);
        assert!(matches!(
            state.next_pending_at(started, 1_024),
            HostPendingDecision::Wait(Some(_))
        ));
        assert_eq!(state.output_bytes, accounted);

        state.controls.push_back(HostControlFrame {
            bytes: vec![10],
            order: HostControlOrder::Priority,
            retires_host: false,
        });
        let HostPendingDecision::Ready(HostPendingFrame::Encoded(control)) =
            state.next_pending_at(started, 1_024)
        else {
            panic!("control remains highest priority");
        };
        assert_eq!(control.bytes, vec![10]);
        assert_eq!(state.output_bytes, accounted);

        let key = (
            SessionId::new("session-1").expect("session"),
            ProcessInstanceId::new("process-1").expect("process"),
        );
        state.cooperative.insert(key, vec![11]);
        let HostPendingDecision::Ready(HostPendingFrame::Encoded(cooperative)) =
            state.next_pending_at(started, 1_024)
        else {
            panic!("cooperative frame remains ahead of output");
        };
        assert_eq!(cooperative.bytes, vec![11]);
        assert_eq!(state.output_bytes, accounted);

        state.retirement_queued = true;
        state.controls.push_back(HostControlFrame {
            bytes: vec![12],
            order: HostControlOrder::AfterAllOutput,
            retires_host: true,
        });
        let HostPendingDecision::Ready(HostPendingFrame::Output(output)) =
            state.next_pending_at(started, 1_024)
        else {
            panic!("accepted output precedes terminal retirement");
        };
        assert_eq!(output.frame.bytes, vec![0, 1, 2, 3]);
        assert_eq!(state.output_bytes, 0);
        let HostPendingDecision::Ready(HostPendingFrame::Encoded(retirement)) =
            state.next_pending_at(started, 1_024)
        else {
            panic!("retirement becomes ready after accepted output drains");
        };
        assert!(retirement.retires_host);

        let mut closing = HostOutboundState::new();
        closing
            .enqueue_output_at(
                codec,
                1_024,
                4_096,
                test_output("session-1", "process-1", 0, vec![0, 1, 2, 3]),
                started,
                linger,
            )
            .expect("closing output");
        closing.closed = true;
        assert!(matches!(
            closing.next_pending_at(started, 1_024),
            HostPendingDecision::Ready(HostPendingFrame::Output(_))
        ));
        assert_eq!(closing.output_bytes, 0);
    }

    #[test]
    fn outbound_control_waits_behind_at_most_one_bounded_selected_output() {
        let codec = HostCodec::new(128 * 1_024).expect("codec");
        let append_bound = 64 * 1_024;
        let started = Instant::now();
        let mut state = HostOutboundState::new();
        state
            .enqueue_output_at(
                codec,
                append_bound,
                256 * 1_024,
                test_output("session-1", "process-1", 0, vec![0; append_bound]),
                started,
                Duration::from_millis(1),
            )
            .expect("bounded output");
        let HostPendingDecision::Ready(HostPendingFrame::Output(selected)) =
            state.next_pending_at(started, append_bound)
        else {
            panic!("append-bound output is selected");
        };
        assert_eq!(selected.frame.bytes.len(), append_bound);
        assert_eq!(state.output_bytes, 0);

        state
            .enqueue_output_at(
                codec,
                append_bound,
                256 * 1_024,
                test_output(
                    "session-1",
                    "process-1",
                    append_bound as u64,
                    vec![1; append_bound],
                ),
                started,
                Duration::from_millis(1),
            )
            .expect("next output");
        state.controls.push_back(HostControlFrame {
            bytes: vec![13],
            order: HostControlOrder::Priority,
            retires_host: false,
        });
        let HostPendingDecision::Ready(HostPendingFrame::Encoded(control)) =
            state.next_pending_at(started, append_bound)
        else {
            panic!("control is selected before the next output frame");
        };
        assert_eq!(control.bytes, vec![13]);
        assert_eq!(state.output.len(), 1);
    }

    #[test]
    fn outbound_writer_emits_one_exact_coalesced_unix_frame() {
        let codec = HostCodec::new(4_096).expect("codec");
        let (writer, mut reader) = stream_pair().expect("socket pair");
        reader
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("read timeout");
        let outbound = HostOutbound::start_with_linger(
            writer,
            codec,
            HostOutboundStartLimits {
                control_limit: 16,
                output_byte_limit: 4_096,
                output_coalesce_limit: 1_024,
                output_coalesce_linger: Duration::from_millis(40),
                stack_bytes: 64 * 1_024,
            },
            Arc::new(HostShutdownSignal::new(PathBuf::from(
                "/tmp/undercurrent-host-linger-test.sock",
            ))),
        )
        .expect("outbound writer");
        // Control admission timestamps and exclude the writer until both chunks
        // are admitted. A real sleep can overshoot the linger window on CI.
        // Deadline boundaries are covered by the state-level linger tests.
        {
            let mut state = outbound.state.lock().expect("outbound state");
            let started = Instant::now();
            for (offset, bytes, admitted_at) in [
                (0, vec![0, 1, 2, 3], started),
                (4, vec![4, 5, 6, 7], started + Duration::from_millis(5)),
            ] {
                state
                    .enqueue_output_at(
                        codec,
                        outbound.output_coalesce_limit,
                        outbound.output_byte_limit,
                        test_output("session-1", "process-1", offset, bytes),
                        admitted_at,
                        outbound.output_coalesce_linger,
                    )
                    .expect("output admitted within linger window");
            }
            outbound.changed.notify_one();
        }

        let HostDecodedFrame::Output(output) =
            read_frame(&mut reader, codec).expect("output frame")
        else {
            panic!("writer emits output frame");
        };
        assert_eq!(output.start_offset, 0);
        assert_eq!(output.bytes, vec![0, 1, 2, 3, 4, 5, 6, 7]);
        outbound.close();
    }

    #[test]
    fn outbound_writer_drains_accepted_output_before_terminal_retirement() {
        let codec = HostCodec::new(4_096).expect("codec");
        let (writer, mut reader) = stream_pair().expect("socket pair");
        reader
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("read timeout");
        let shutdown = Arc::new(HostShutdownSignal::new(PathBuf::from(
            "/tmp/undercurrent-host-retirement-test.sock",
        )));
        let outbound = HostOutbound::start_with_linger(
            writer,
            codec,
            HostOutboundStartLimits {
                control_limit: 16,
                output_byte_limit: 4_096,
                output_coalesce_limit: 1_024,
                output_coalesce_linger: Duration::from_secs(1),
                stack_bytes: 64 * 1_024,
            },
            Arc::clone(&shutdown),
        )
        .expect("outbound writer");
        outbound
            .enqueue_output(test_output("session-1", "process-1", 0, vec![0, 1, 2, 3]))
            .expect("accepted output");
        let retirement = HostResponse {
            request_id: 41,
            result: Ok(HostResponseValue::Authenticated {
                host_epoch: "retired".to_owned(),
            }),
        };
        outbound
            .enqueue_retirement_control(
                codec
                    .encode_response(&retirement)
                    .expect("retirement response"),
            )
            .expect("retirement control");
        assert!(
            shutdown.retires_storage(),
            "durably acknowledged retirement is marked before peer observation"
        );
        shutdown.store(true, Ordering::Release);
        assert!(
            shutdown.retires_storage(),
            "an immediate peer disconnect cannot downgrade storage retirement"
        );
        assert_eq!(
            outbound.enqueue_output(test_output("session-1", "process-1", 4, vec![4, 5, 6, 7],)),
            Err(HostQueueError::Closed),
            "retirement rejects later output"
        );

        let HostDecodedFrame::Output(output) =
            read_frame(&mut reader, codec).expect("output frame")
        else {
            panic!("accepted output is written first");
        };
        assert_eq!(output.start_offset, 0);
        assert_eq!(output.bytes, vec![0, 1, 2, 3]);
        let HostDecodedFrame::Response(response) =
            read_frame(&mut reader, codec).expect("retirement frame")
        else {
            panic!("terminal retirement is written second");
        };
        assert_eq!(response.request_id, 41);
        let deadline = Instant::now() + Duration::from_secs(1);
        while Arc::strong_count(&outbound) != 1 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(Arc::strong_count(&outbound), 1, "writer terminates");
        assert!(shutdown.load(Ordering::Acquire));
    }

    fn test_output(
        session_id: &str,
        process_id: &str,
        start_offset: u64,
        bytes: Vec<u8>,
    ) -> HostOutputFrame {
        HostOutputFrame {
            session_id: session_id.to_owned(),
            process_id: process_id.to_owned(),
            start_offset,
            bytes,
        }
    }
}
