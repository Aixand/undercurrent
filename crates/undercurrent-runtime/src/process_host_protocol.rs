// SPDX-License-Identifier: Apache-2.0

//! Private bounded protocol between the daemon and its daemon-independent PTY host.
//!
//! UCHS release 0.1.0-beta uses JSON control frames and binary input/output frames under
//! a checked complete-frame limit. It is separate from public client transport
//! and package versions. Request IDs correlate replies without imposing a total
//! order on concurrent effects. Operation-specific responses and retained
//! receipts must be interpreted before inferring completion or retry safety.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use undercurrent_application::{
    EphemeralSecret, OutputGapRecovery, PortErrorKind, ProcessInstance, ProcessTermination,
    ResolvedSecretEnvironment, TerminationKind,
};
use undercurrent_domain::{
    CommandSpec, CooperativeForegroundState, EnvironmentVariables, ExecutionEnvironmentBinding,
    ExecutionLimits, LaunchSpec, OutputCaptureMode, PathReference, PayloadDigest,
    ProcessInstanceId, SecretEnvironmentVariables, SecretReference, SessionId, StopPolicy,
    StopSignal, TerminalSize, TimestampMillis,
};

const MAGIC: [u8; 4] = *b"UCHS";
const VERSION: u8 = undercurrent_foundation::RELEASE_VERSION.len() as u8;
const RELEASE_BYTES: &[u8] = undercurrent_foundation::RELEASE_VERSION.as_bytes();
const HEADER_BYTES: usize = 10 + RELEASE_BYTES.len();

/// Complete private session-host frame registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HostFrameKind {
    Request,
    Response,
    Exit,
    Output,
    Input,
    Cooperative,
}

impl HostFrameKind {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) const ALL: [Self; 6] = [
        Self::Request,
        Self::Response,
        Self::Exit,
        Self::Output,
        Self::Input,
        Self::Cooperative,
    ];

    const fn code(self) -> u8 {
        match self {
            Self::Request => 1,
            Self::Response => 2,
            Self::Exit => 3,
            Self::Output => 4,
            Self::Input => 5,
            Self::Cooperative => 6,
        }
    }

    fn from_code(code: u8) -> Result<Self, HostProtocolError> {
        match code {
            1 => Ok(Self::Request),
            2 => Ok(Self::Response),
            3 => Ok(Self::Exit),
            4 => Ok(Self::Output),
            5 => Ok(Self::Input),
            6 => Ok(Self::Cooperative),
            _ => Err(HostProtocolError::UnknownFrameKind(code)),
        }
    }
}

/// One daemon request routed independently by request ID.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostRequest {
    pub(crate) request_id: u64,
    pub(crate) command: HostCommand,
}

/// Private control operations owned by the process adapter.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum HostCommand {
    Authenticate {
        token: String,
    },
    CaptureShellEnvironment {
        token: String,
        session_id: String,
        shell_pid: u32,
        environment: HostResolvedSecretEnvironment,

        working_directory: Option<String>,
    },
    ShellEnvironmentStatus {
        session_id: String,
        process_id: String,
    },
    Spawn {
        spawn_token: String,
        session_id: String,
        launch: Box<HostLaunchSpec>,

        secret_environment: HostResolvedSecretEnvironment,
    },
    AdoptSpawn {
        spawn_token: String,
    },
    CancelSpawn {
        spawn_token: String,
    },
    ArmExit {
        session_id: String,
        process_id: String,
    },
    Stop {
        session_id: String,
        process_id: String,
        signal: HostStopSignal,
        force_after_millis: u64,

        no_escalation: bool,
    },
    ForceStop {
        session_id: String,
        process_id: String,
    },
    WriteInput {
        session_id: String,
        process_id: String,
        bytes: Vec<u8>,
        pacing: Option<HostInputPacing>,
    },
    CloseInput {
        session_id: String,
        process_id: String,
    },
    Resize {
        session_id: String,
        process_id: String,
        columns: u16,
        rows: u16,
    },
    Observe {
        session_id: String,
        process_id: String,
        started_at_millis: i64,
    },
    ObserveForeground {
        session_id: String,
        process_id: String,
    },
    Replay {
        session_id: String,
        process_id: String,
        from: u64,
        max_bytes: usize,
    },
    Snapshot {
        session_id: String,
        process_id: String,
    },
    Retire {
        session_id: String,
        process_id: String,
    },
    Execute {
        live_environment_process: Option<String>,
        job_id: String,
        session_id: String,
        payload_digest: String,
        program: String,
        arguments: Vec<String>,
        environment: Vec<(String, String)>,
        working_directory: Option<String>,
        output_bytes: u64,
        deadline_millis: Option<i64>,
    },
    PollExecution {
        job_id: String,
        wait_millis: u64,
    },
    CancelExecution {
        job_id: String,
    },
    AcknowledgeExecution {
        job_id: String,
    },
    AttemptMutation {
        attempt: HostAttemptContext,
        mutation: HostAttemptMutation,
    },
    ObserveInputRetirement {
        attempt: HostAttemptContext,
    },
    ObserveAttempt {
        attempt: HostAttemptContext,
    },
    ProbeAttempt {
        attempt: HostAttemptContext,
    },
    AcknowledgeAttempt {
        attempt: HostAttemptContext,
    },
    CancelShutdownAttempt {
        attempt: HostAttemptContext,
    },
    AcknowledgeShutdownCancellation {
        attempt: HostAttemptContext,
    },
    Shutdown,
}

/// Product attempt identity carried only by the private host protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostAttemptContext {
    pub(crate) attempt_id: String,
    pub(crate) session_id: Option<String>,
    /// Identifies the counter domain in which `fencing_token` is ordered.
    ///
    /// `None` selects session-wide fencing for an ordinary session mutation.
    /// Dispatch-bound attempts retain their explicit counter scope.
    pub(crate) fencing_scope: Option<String>,
    pub(crate) kind: HostAttemptKind,
    pub(crate) fencing_token: u64,
    pub(crate) payload_digest: String,
    pub(crate) accepted_at_millis: i64,
}

/// Process mutation classification retained without importing product state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HostAttemptKind {
    Spawn,
    ArmExit,
    Stop,
    ForceStop,
    WriteInput,
    Resize,
    Shutdown,
    ExecuteInSession,
}

/// Attempt-aware process mutation. Execution jobs retain their separate
/// streaming protocol and journal.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "mutation", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum HostAttemptMutation {
    Spawn {
        session_id: String,
        launch: Box<HostLaunchSpec>,

        secret_environment: HostResolvedSecretEnvironment,
    },
    ArmExit {
        session_id: String,
        process_id: String,
    },
    Stop {
        session_id: String,
        process_id: String,
        signal: HostStopSignal,
        force_after_millis: u64,

        no_escalation: bool,
    },
    ForceStop {
        session_id: String,
        process_id: String,
    },
    WriteInput {
        session_id: String,
        process_id: String,
        bytes: Vec<u8>,
        pacing: Option<HostInputPacing>,
    },
    Resize {
        session_id: String,
        process_id: String,
        columns: u16,
        rows: u16,
    },
    Shutdown,
}

/// One response correlated to the exact daemon request.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostResponse {
    pub(crate) request_id: u64,
    pub(crate) result: Result<HostResponseValue, HostFailure>,
}

/// Successful host response values.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum HostResponseValue {
    Authenticated {
        host_epoch: String,
    },
    Spawned {
        process_id: String,
        started_at_millis: i64,

        root_pid: u32,

        root_start_ticks: Option<u64>,
    },
    ShellEnvironmentStatus {
        observed_at_millis: Option<i64>,
    },
    Completed,
    ShellEnvironmentInvalidated,
    InputClosed {
        already_closed: bool,
    },
    Terminated {
        kind: HostTerminationKind,
        exit_code: Option<i32>,
        observed_at_millis: i64,
    },
    Observed {
        state: HostObservedProcessState,
        columns: Option<u16>,
        rows: Option<u16>,

        output_capture_mode: HostOutputCaptureMode,

        root_pid: Option<u32>,

        root_start_ticks: Option<u64>,
    },
    ForegroundContext {
        context: HostForegroundContext,
    },
    Replay {
        requested: u64,
        available_from: u64,
        delivered_to: u64,
        next_offset: u64,
        recovery: HostGapRecovery,
        has_more: bool,
    },
    ScreenSnapshot {
        output_offset: u64,
        columns: u16,
        rows: u16,
        cursor_row: u16,
        cursor_column: u16,
        cursor_visible: bool,
        alternate_screen: bool,
        contents: String,
    },
    ExecutionAccepted,
    ExecutionRunning,
    ExecutionTerminal {
        kind: HostExecutionTerminalKind,
        exit_code: Option<i32>,
        stdout: Vec<u8>,
        stdout_source_bytes: u64,
        stderr: Vec<u8>,
        stderr_source_bytes: u64,
    },
    ExecutionFailed {
        kind: HostFailureKind,
        effect_started: bool,
    },
    InputRetirementObserved {
        process_id: Option<String>,
        exited_at_millis: Option<i64>,
    },
    AttemptFailed {
        certainty: HostEffectCertainty,
        failure: HostFailure,
    },
    AttemptObserved {
        certainty: HostEffectCertainty,
        result_digest: Option<String>,
    },
    AttemptProbed {
        state: HostAttemptProbeState,
        response: Option<Box<HostResponseValue>>,
    },
}

/// Presence-aware receipt state used before resolving a one-use credential handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HostAttemptProbeState {
    Absent,
    Unknown,
    Applied,
}

/// Effect certainty transported without exposing application types to the
/// persisted private journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HostEffectCertainty {
    KnownAbsent,
    KnownApplied,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HostExecutionTerminalKind {
    Completed,
    Cancelled,
    DeadlineExceeded,
    OutputLimitExceeded,
}

#[cfg(test)]
mod execution_protocol_tests {
    use super::*;

    #[test]
    fn execution_commands_and_binary_terminal_results_round_trip_exactly() {
        let command = HostCommand::Execute {
            live_environment_process: None,
            job_id: "attempt-1".to_owned(),
            session_id: "session-1".to_owned(),
            payload_digest: "ab".repeat(32),
            program: "/bin/printf".to_owned(),
            arguments: vec!["%s".to_owned(), "$HOME; untouched".to_owned()],
            environment: vec![("BINARY_SAFE".to_owned(), "yes".to_owned())],
            working_directory: Some("/tmp".to_owned()),
            output_bytes: 4096,
            deadline_millis: Some(1234),
        };
        let encoded = serde_json::to_vec(&command).expect("encode execution command");
        let decoded: HostCommand =
            serde_json::from_slice(&encoded).expect("decode execution command");
        let HostCommand::Execute {
            live_environment_process: None,
            arguments,
            environment,
            working_directory,
            ..
        } = decoded
        else {
            panic!("execution command changed variant");
        };
        assert_eq!(arguments, ["%s", "$HOME; untouched"]);
        assert_eq!(environment, [("BINARY_SAFE".to_owned(), "yes".to_owned())]);
        assert_eq!(working_directory.as_deref(), Some("/tmp"));

        let response = HostResponseValue::ExecutionTerminal {
            kind: HostExecutionTerminalKind::Completed,
            exit_code: Some(7),
            stdout: vec![0, 0xff, b'\n'],
            stdout_source_bytes: 3,
            stderr: vec![0xfe, 0],
            stderr_source_bytes: 2,
        };
        let encoded = serde_json::to_vec(&response).expect("encode terminal result");
        let decoded: HostResponseValue =
            serde_json::from_slice(&encoded).expect("decode terminal result");
        assert!(matches!(
            decoded,
            HostResponseValue::ExecutionTerminal {
                stdout,
                stderr,
                exit_code: Some(7),
                ..
            } if stdout == [0, 0xff, b'\n'] && stderr == [0xfe, 0]
        ));
    }
}

/// Payload-redacted failure returned across the host boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostFailure {
    pub(crate) kind: HostFailureKind,
    pub(crate) context: HostFailureContext,
}

/// Stable private host failure registry.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HostFailureKind {
    Unavailable,
    TimedOut,
    Conflict,
    NotFound,
    PermissionDenied,
    InvalidData,
    CapacityExceeded,
    RateLimited,
    Cancelled,
    Interrupted,
    ReplayUnavailable,
    ProtocolViolation,
    Unsupported,
    Other,
}

impl From<PortErrorKind> for HostFailureKind {
    fn from(value: PortErrorKind) -> Self {
        match value {
            PortErrorKind::Unavailable => Self::Unavailable,
            PortErrorKind::Timeout => Self::TimedOut,
            PortErrorKind::Conflict => Self::Conflict,
            PortErrorKind::NotFound => Self::NotFound,
            PortErrorKind::PermissionDenied => Self::PermissionDenied,
            PortErrorKind::InvalidData => Self::InvalidData,
            PortErrorKind::CapacityExceeded => Self::CapacityExceeded,
            PortErrorKind::RateLimited => Self::RateLimited,
            PortErrorKind::Cancelled => Self::Cancelled,
            PortErrorKind::Interrupted => Self::Interrupted,
            PortErrorKind::ReplayUnavailable => Self::ReplayUnavailable,
            PortErrorKind::ProtocolViolation => Self::ProtocolViolation,
            PortErrorKind::Unsupported => Self::Unsupported,
            PortErrorKind::Other => Self::Other,
        }
    }
}

impl From<HostFailureKind> for PortErrorKind {
    fn from(value: HostFailureKind) -> Self {
        match value {
            HostFailureKind::Unavailable => Self::Unavailable,
            HostFailureKind::TimedOut => Self::Timeout,
            HostFailureKind::Conflict => Self::Conflict,
            HostFailureKind::NotFound => Self::NotFound,
            HostFailureKind::PermissionDenied => Self::PermissionDenied,
            HostFailureKind::InvalidData => Self::InvalidData,
            HostFailureKind::CapacityExceeded => Self::CapacityExceeded,
            HostFailureKind::RateLimited => Self::RateLimited,
            HostFailureKind::Cancelled => Self::Cancelled,
            HostFailureKind::Interrupted => Self::Interrupted,
            HostFailureKind::ReplayUnavailable => Self::ReplayUnavailable,
            HostFailureKind::ProtocolViolation => Self::ProtocolViolation,
            HostFailureKind::Unsupported => Self::Unsupported,
            HostFailureKind::Other => Self::Other,
        }
    }
}

/// Safe operation context without commands, environment, or terminal payloads.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HostFailureContext {
    Execute,
    PollExecution,
    CancelExecution,
    AcknowledgeExecution,
    AttemptMutation,
    ObserveAttempt,
    AcknowledgeAttempt,
    Authentication,
    Spawn,
    AdoptSpawn,
    CancelSpawn,
    ArmExit,
    Stop,
    ForceStop,
    WriteInput,
    CloseInput,
    Resize,
    Observe,
    ObserveForeground,
    Replay,
    Snapshot,
    Retire,
    Shutdown,
    Protocol,
}

/// Exact asynchronous process exit emitted by the host.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostExitEvent {
    pub(crate) session_id: String,
    pub(crate) process_id: String,
    pub(crate) output_offset: u64,
    pub(crate) exit_code: Option<i32>,
    pub(crate) supervision_failed: bool,
    pub(crate) observed_at_millis: i64,
}

/// Raw output frame that avoids JSON or base64 expansion of terminal bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HostOutputFrame {
    pub(crate) session_id: String,
    pub(crate) process_id: String,
    pub(crate) start_offset: u64,
    pub(crate) bytes: Vec<u8>,
}

/// Payload-free cooperative foreground event emitted by a managed shell.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostCooperativeEvent {
    pub(crate) session_id: String,
    pub(crate) process_id: String,
    pub(crate) state: HostCooperativeState,
    pub(crate) observed_at_millis: i64,
}

/// Bounded serializable foreground process-group facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostForegroundContext {
    pub(crate) process_id: String,
    pub(crate) observed_at_millis: i64,
    pub(crate) members: Vec<HostForegroundMember>,
    pub(crate) completeness: HostProcessGroupCompleteness,
    pub(crate) current_directory: Option<String>,
    pub(crate) tcp_listener: HostTcpListenerObservation,

    pub(crate) managed_process_tcp_listener: Option<HostTcpListenerObservation>,

    pub(crate) managed_tree_tcp_listener: Option<HostTcpListenerObservation>,
    pub(crate) cooperative: Option<HostCooperativeState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostForegroundMember {
    pub(crate) incarnation: String,
    pub(crate) program: String,
    pub(crate) arguments: Vec<String>,
    pub(crate) environment_names: Vec<String>,

    pub(crate) environment_names_complete: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HostProcessGroupCompleteness {
    Complete,
    Truncated,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HostTcpListenerObservation {
    Present,
    Absent,
    Unavailable,
    Truncated,
}

#[cfg(test)]
mod listener_scope_tests {
    use super::*;

    #[test]
    fn older_host_contexts_do_not_imply_managed_listener_evidence() {
        let json = r#"{"process_id":"p1","observed_at_millis":1,"members":[],"completeness":"unavailable","current_directory":null,"tcp_listener":"unavailable","cooperative":null}"#;
        let mut context: HostForegroundContext = serde_json::from_str(json).unwrap();
        assert_eq!(context.managed_process_tcp_listener, None);
        assert_eq!(context.managed_tree_tcp_listener, None);
        assert!(
            serde_json::to_value(&context)
                .unwrap()
                .as_object()
                .unwrap()
                .contains_key("managed_process_tcp_listener")
        );
        context.managed_process_tcp_listener = Some(HostTcpListenerObservation::Present);
        context.managed_tree_tcp_listener = Some(HostTcpListenerObservation::Present);
        let decoded: HostForegroundContext =
            serde_json::from_slice(&serde_json::to_vec(&context).unwrap()).unwrap();
        assert_eq!(
            decoded.tcp_listener,
            HostTcpListenerObservation::Unavailable
        );
        assert_eq!(
            decoded.completeness,
            HostProcessGroupCompleteness::Unavailable
        );
        assert_eq!(
            decoded.managed_process_tcp_listener,
            Some(HostTcpListenerObservation::Present)
        );
        assert_eq!(
            decoded.managed_tree_tcp_listener,
            Some(HostTcpListenerObservation::Present)
        );
    }
}

/// Stable private representation of cooperative shell state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HostCooperativeState {
    Ready,
    Running,
}

impl From<CooperativeForegroundState> for HostCooperativeState {
    fn from(value: CooperativeForegroundState) -> Self {
        match value {
            CooperativeForegroundState::Ready => Self::Ready,
            CooperativeForegroundState::Running => Self::Running,
        }
    }
}

impl From<HostCooperativeState> for CooperativeForegroundState {
    fn from(value: HostCooperativeState) -> Self {
        match value {
            HostCooperativeState::Ready => Self::Ready,
            HostCooperativeState::Running => Self::Running,
        }
    }
}

/// Raw input request that avoids JSON expansion of terminal bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HostInputFrame {
    pub(crate) request_id: u64,
    pub(crate) attempt: Option<HostAttemptContext>,
    pub(crate) session_id: String,
    pub(crate) process_id: String,
    pub(crate) bytes: Vec<u8>,
    pub(crate) pacing: Option<HostInputPacing>,
}

/// Payload-independent pacing metadata for one raw input frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostInputPacing {
    /// Internal byte offset flushed before the pause.
    pub(crate) offset: u32,
    /// Whole milliseconds to pause before writing the suffix.
    pub(crate) delay_millis: u64,
}

/// Adapter-owned serializable launch specification.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostLaunchSpec {
    /// Absolute session stream checkpoint supplied by the daemon for a new incarnation.
    pub(crate) output_offset: u64,
    program: String,

    launch_mode: StoredLaunchMode,
    arguments: Vec<String>,
    expected_executable_digest_sha256: Option<String>,
    executable_resolution: Option<HostExecutableResolution>,
    environment: Vec<(String, String)>,

    secret_environment: Vec<(String, String)>,
    environment_file: Option<String>,
    working_directory: Option<String>,

    working_directory_identity_sha256: Option<String>,
    environment_binding: Option<HostExecutionEnvironmentBinding>,
    columns: u16,
    rows: u16,

    output_capture_mode: HostOutputCaptureMode,
}

impl From<&LaunchSpec> for HostLaunchSpec {
    fn from(value: &LaunchSpec) -> Self {
        Self {
            output_offset: 0,
            launch_mode: value.command().launch_mode().into(),
            program: value.command().program().to_owned(),
            arguments: value.command().arguments().to_vec(),
            expected_executable_digest_sha256: value
                .command()
                .expected_executable_digest()
                .map(PayloadDigest::to_hex),
            executable_resolution: value.command().executable_resolution().map(|resolution| {
                HostExecutableResolution {
                    requested_reference: resolution.requested_reference().to_owned(),
                    content_digest_sha256: resolution.content_digest().to_hex(),
                    object_identity_sha256: resolution.object_identity().map(PayloadDigest::to_hex),
                    receipt_digest_sha256: resolution.receipt_digest().to_hex(),
                }
            }),
            environment: value
                .environment()
                .iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect(),
            secret_environment: value
                .secret_environment()
                .iter()
                .map(|(key, reference)| (key.to_owned(), reference.as_str().to_owned()))
                .collect(),
            environment_file: value
                .environment_file()
                .map(|path| path.as_str().to_owned()),
            working_directory: value
                .working_directory()
                .map(|path| path.as_str().to_owned()),
            working_directory_identity_sha256: value
                .working_directory_identity()
                .map(PayloadDigest::to_hex),
            environment_binding: value.environment_binding().map(|binding| {
                HostExecutionEnvironmentBinding {
                    digest_sha256: binding.digest().to_hex(),
                    working_directory_root: binding.working_directory_root().as_str().to_owned(),
                    root_identity_sha256: binding.root_identity().map(PayloadDigest::to_hex),
                }
            }),
            columns: value.terminal_size().columns(),
            rows: value.terminal_size().rows(),
            output_capture_mode: value.output_capture_mode().into(),
        }
    }
}

impl Debug for HostLaunchSpec {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HostLaunchSpec")
            .field("argument_count", &self.arguments.len())
            .field("environment_entry_count", &self.environment.len())
            .field(
                "secret_environment_entry_count",
                &self.secret_environment.len(),
            )
            .field("environment_file_set", &self.environment_file.is_some())
            .field("working_directory_set", &self.working_directory.is_some())
            .finish_non_exhaustive()
    }
}

/// Raw secret bytes carried only across the private authenticated host frame.
#[derive(Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct HostResolvedSecretEnvironment(Vec<(String, Vec<u8>)>);

impl HostResolvedSecretEnvironment {
    pub(crate) fn from_values(values: Vec<(String, Vec<u8>)>) -> Self {
        Self(values)
    }
    pub(crate) fn values(&self) -> &[(String, Vec<u8>)] {
        &self.0
    }

    pub(crate) fn from_resolved(resolved: &ResolvedSecretEnvironment) -> Self {
        let mut values = Vec::with_capacity(resolved.len());
        for (name, secret) in resolved.iter() {
            values.push((name.to_owned(), secret.expose(<[u8]>::to_vec)));
        }
        Self(values)
    }

    pub(crate) fn into_resolved(mut self) -> ResolvedSecretEnvironment {
        let values = std::mem::take(&mut self.0)
            .into_iter()
            .map(|(name, bytes)| (name, EphemeralSecret::new(bytes)))
            .collect::<BTreeMap<_, _>>();
        ResolvedSecretEnvironment::new(values)
    }
}

impl Debug for HostResolvedSecretEnvironment {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HostResolvedSecretEnvironment")
            .field("entry_count", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl Drop for HostResolvedSecretEnvironment {
    fn drop(&mut self) {
        for (_, value) in &mut self.0 {
            value.fill(0);
        }
    }
}

impl HostLaunchSpec {
    pub(crate) fn terminal_size(&self) -> Result<TerminalSize, HostProtocolError> {
        TerminalSize::new(self.columns, self.rows, u16::MAX, u16::MAX)
            .map_err(|_| HostProtocolError::InvalidDomainValue)
    }

    pub(crate) const fn output_capture_mode(&self) -> OutputCaptureMode {
        match self.output_capture_mode {
            HostOutputCaptureMode::Terminal => OutputCaptureMode::Terminal,
            HostOutputCaptureMode::Opaque => OutputCaptureMode::Opaque,
        }
    }
}

impl TryFrom<HostLaunchSpec> for LaunchSpec {
    type Error = HostProtocolError;

    fn try_from(value: HostLaunchSpec) -> Result<Self, Self::Error> {
        let HostLaunchSpec {
            output_offset: _,
            launch_mode,
            program,
            arguments,
            expected_executable_digest_sha256,
            executable_resolution,
            environment,
            secret_environment,
            environment_file,
            working_directory,
            working_directory_identity_sha256,
            environment_binding,
            columns,
            rows,
            output_capture_mode,
        } = value;
        let requested_program = executable_resolution
            .as_ref()
            .map_or(program.as_str(), |resolution| {
                resolution.requested_reference.as_str()
            });
        let command_bytes = arguments.iter().try_fold(
            program.len().max(requested_program.len()),
            |total, item| {
                total
                    .checked_add(item.len())
                    .ok_or(HostProtocolError::InvalidDomainValue)
            },
        )?;
        let environment_bytes = environment
            .iter()
            .chain(secret_environment.iter())
            .try_fold(0_usize, |total, (key, item)| {
                total
                    .checked_add(key.len())
                    .and_then(|total| total.checked_add(item.len()))
                    .ok_or(HostProtocolError::InvalidDomainValue)
            })?;
        let path_bytes = environment_file
            .as_ref()
            .into_iter()
            .chain(working_directory.as_ref())
            .chain(
                environment_binding
                    .as_ref()
                    .map(|binding| &binding.working_directory_root),
            )
            .map(String::len)
            .max()
            .unwrap_or(1);
        let limits = ExecutionLimits {
            max_arguments: arguments.len().max(1),
            max_command_bytes: command_bytes.max(1),
            max_environment_entries: environment
                .len()
                .checked_add(secret_environment.len())
                .ok_or(HostProtocolError::InvalidDomainValue)?
                .max(1),
            max_environment_bytes: environment_bytes.max(1),
            max_path_bytes: path_bytes,
            max_health_rules: 1,
            max_health_rule_bytes: 1,
        };
        let expected_executable_digest = expected_executable_digest_sha256
            .map(|digest| PayloadDigest::from_hex(&digest))
            .transpose()
            .map_err(|_| HostProtocolError::InvalidDomainValue)?;
        let command = CommandSpec::new(requested_program, arguments, limits)
            .map_err(|_| HostProtocolError::InvalidDomainValue)?
            .with_launch_mode(launch_mode.into())
            .with_expected_executable_digest(expected_executable_digest);
        let command = match executable_resolution {
            None => command,
            Some(resolution) => {
                let content_digest = PayloadDigest::from_hex(&resolution.content_digest_sha256)
                    .map_err(|_| HostProtocolError::InvalidDomainValue)?;
                let receipt_digest = PayloadDigest::from_hex(&resolution.receipt_digest_sha256)
                    .map_err(|_| HostProtocolError::InvalidDomainValue)?;
                match resolution.object_identity_sha256 {
                    Some(identity) => command
                        .pin_executable_with_identity(
                            program,
                            content_digest,
                            PayloadDigest::from_hex(&identity)
                                .map_err(|_| HostProtocolError::InvalidDomainValue)?,
                            receipt_digest,
                            limits,
                        )
                        .map_err(|_| HostProtocolError::InvalidDomainValue)?,
                    None => command
                        .pin_executable(program, content_digest, receipt_digest, limits)
                        .map_err(|_| HostProtocolError::InvalidDomainValue)?,
                }
            }
        };
        let environment = EnvironmentVariables::new(environment, limits)
            .map_err(|_| HostProtocolError::InvalidDomainValue)?;
        let secret_environment = SecretEnvironmentVariables::new(
            secret_environment
                .into_iter()
                .map(|(name, reference)| {
                    SecretReference::new(reference)
                        .map(|reference| (name, reference))
                        .map_err(|_| HostProtocolError::InvalidDomainValue)
                })
                .collect::<Result<Vec<_>, _>>()?,
            &environment,
            limits,
        )
        .map_err(|_| HostProtocolError::InvalidDomainValue)?;
        let environment_file = environment_file
            .map(|path| PathReference::new(path, limits.max_path_bytes))
            .transpose()
            .map_err(|_| HostProtocolError::InvalidDomainValue)?;
        let working_directory = working_directory
            .map(|path| PathReference::new(path, limits.max_path_bytes))
            .transpose()
            .map_err(|_| HostProtocolError::InvalidDomainValue)?;
        let working_directory_identity = working_directory_identity_sha256
            .map(|identity| PayloadDigest::from_hex(&identity))
            .transpose()
            .map_err(|_| HostProtocolError::InvalidDomainValue)?;
        let environment_binding = environment_binding
            .map(|binding| {
                Ok::<_, HostProtocolError>(
                    ExecutionEnvironmentBinding::new(
                        PayloadDigest::from_hex(&binding.digest_sha256)
                            .map_err(|_| HostProtocolError::InvalidDomainValue)?,
                        PathReference::new(binding.working_directory_root, limits.max_path_bytes)
                            .map_err(|_| HostProtocolError::InvalidDomainValue)?,
                    )
                    .with_root_identity(
                        binding
                            .root_identity_sha256
                            .map(|identity| PayloadDigest::from_hex(&identity))
                            .transpose()
                            .map_err(|_| HostProtocolError::InvalidDomainValue)?,
                    ),
                )
            })
            .transpose()?;
        let terminal_size = TerminalSize::new(columns, rows, u16::MAX, u16::MAX)
            .map_err(|_| HostProtocolError::InvalidDomainValue)?;
        Ok(Self::new(
            command,
            environment,
            environment_file,
            working_directory,
            terminal_size,
        )
        .with_secret_environment(secret_environment, limits)
        .map_err(|_| HostProtocolError::InvalidDomainValue)?
        .with_environment_binding(environment_binding)
        .with_working_directory_identity(working_directory_identity)
        .with_output_capture_mode(output_capture_mode.into()))
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HostExecutableResolution {
    requested_reference: String,
    content_digest_sha256: String,

    object_identity_sha256: Option<String>,
    receipt_digest_sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HostExecutionEnvironmentBinding {
    digest_sha256: String,
    working_directory_root: String,

    root_identity_sha256: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HostOutputCaptureMode {
    #[default]
    Terminal,
    Opaque,
}

impl From<OutputCaptureMode> for HostOutputCaptureMode {
    fn from(value: OutputCaptureMode) -> Self {
        match value {
            OutputCaptureMode::Terminal => Self::Terminal,
            OutputCaptureMode::Opaque => Self::Opaque,
        }
    }
}

impl From<HostOutputCaptureMode> for OutputCaptureMode {
    fn from(value: HostOutputCaptureMode) -> Self {
        match value {
            HostOutputCaptureMode::Terminal => Self::Terminal,
            HostOutputCaptureMode::Opaque => Self::Opaque,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HostStopSignal {
    Interrupt,
    Terminate,
    Hangup,
    Quit,
    User1,
    User2,
}

impl From<StopSignal> for HostStopSignal {
    fn from(value: StopSignal) -> Self {
        match value {
            StopSignal::Interrupt => Self::Interrupt,
            StopSignal::Terminate => Self::Terminate,
            StopSignal::Hangup => Self::Hangup,
            StopSignal::Quit => Self::Quit,
            StopSignal::User1 => Self::User1,
            StopSignal::User2 => Self::User2,
        }
    }
}

impl From<HostStopSignal> for StopSignal {
    fn from(value: HostStopSignal) -> Self {
        match value {
            HostStopSignal::Interrupt => Self::Interrupt,
            HostStopSignal::Terminate => Self::Terminate,
            HostStopSignal::Hangup => Self::Hangup,
            HostStopSignal::Quit => Self::Quit,
            HostStopSignal::User1 => Self::User1,
            HostStopSignal::User2 => Self::User2,
        }
    }
}

impl HostCommand {
    pub(crate) fn stop(
        session_id: &SessionId,
        process_id: &ProcessInstanceId,
        policy: StopPolicy,
    ) -> Result<Self, HostProtocolError> {
        let force_after_millis = u64::try_from(policy.force_after().as_millis())
            .map_err(|_| HostProtocolError::InvalidDomainValue)?;
        Ok(Self::Stop {
            session_id: session_id.as_str().to_owned(),
            process_id: process_id.as_str().to_owned(),
            signal: policy.signal().into(),
            force_after_millis,
            no_escalation: policy.no_escalation(),
        })
    }

    pub(crate) fn stop_policy(
        signal: HostStopSignal,
        force_after_millis: u64,
    ) -> Result<StopPolicy, HostProtocolError> {
        StopPolicy::new(signal.into(), Duration::from_millis(force_after_millis))
            .map_err(|_| HostProtocolError::InvalidDomainValue)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HostTerminationKind {
    GracefulTimedOut,
    Graceful,
    ForcedAfterTimeout,
    Forced,
}

impl From<TerminationKind> for HostTerminationKind {
    fn from(value: TerminationKind) -> Self {
        match value {
            TerminationKind::Graceful => Self::Graceful,
            TerminationKind::GracefulTimedOut => Self::GracefulTimedOut,
            TerminationKind::ForcedAfterTimeout => Self::ForcedAfterTimeout,
            TerminationKind::Forced => Self::Forced,
        }
    }
}

impl From<HostTerminationKind> for TerminationKind {
    fn from(value: HostTerminationKind) -> Self {
        match value {
            HostTerminationKind::Graceful => Self::Graceful,
            HostTerminationKind::GracefulTimedOut => Self::GracefulTimedOut,
            HostTerminationKind::ForcedAfterTimeout => Self::ForcedAfterTimeout,
            HostTerminationKind::Forced => Self::Forced,
        }
    }
}

impl HostResponseValue {
    pub(crate) fn from_process(
        process: &ProcessInstance,
        root_pid: u32,
        root_start_ticks: Option<u64>,
    ) -> Self {
        Self::Spawned {
            process_id: process.id.as_str().to_owned(),
            started_at_millis: process.started_at.value(),
            root_pid,
            root_start_ticks,
        }
    }

    pub(crate) fn from_termination(termination: ProcessTermination) -> Self {
        Self::Terminated {
            kind: termination.kind().into(),
            exit_code: termination.exit_code(),
            observed_at_millis: termination.observed_at().value(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum HostObservedProcessState {
    Running {
        observed_at_millis: i64,
    },
    Exited {
        exit_code: i32,
        observed_at_millis: i64,
    },
    Missing {
        observed_at_millis: i64,
    },
    Lost {
        observed_at_millis: i64,
    },
    Orphaned {
        observed_at_millis: i64,
    },
    Unknown {
        observed_at_millis: i64,
    },
}

impl From<undercurrent_domain::ProcessTruth> for HostObservedProcessState {
    fn from(value: undercurrent_domain::ProcessTruth) -> Self {
        match value {
            undercurrent_domain::ProcessTruth::Running { observed_at } => Self::Running {
                observed_at_millis: observed_at.value(),
            },
            undercurrent_domain::ProcessTruth::Exited {
                exit_code,
                observed_at,
            } => Self::Exited {
                exit_code,
                observed_at_millis: observed_at.value(),
            },
            undercurrent_domain::ProcessTruth::Missing { observed_at } => Self::Missing {
                observed_at_millis: observed_at.value(),
            },
            undercurrent_domain::ProcessTruth::Lost { observed_at } => Self::Lost {
                observed_at_millis: observed_at.value(),
            },
            undercurrent_domain::ProcessTruth::Orphaned { observed_at } => Self::Orphaned {
                observed_at_millis: observed_at.value(),
            },
            undercurrent_domain::ProcessTruth::Unknown { observed_at } => Self::Unknown {
                observed_at_millis: observed_at.value(),
            },
        }
    }
}

impl From<HostObservedProcessState> for undercurrent_domain::ProcessTruth {
    fn from(value: HostObservedProcessState) -> Self {
        match value {
            HostObservedProcessState::Running { observed_at_millis } => Self::Running {
                observed_at: TimestampMillis::new(observed_at_millis),
            },
            HostObservedProcessState::Exited {
                exit_code,
                observed_at_millis,
            } => Self::Exited {
                exit_code,
                observed_at: TimestampMillis::new(observed_at_millis),
            },
            HostObservedProcessState::Missing { observed_at_millis } => Self::Missing {
                observed_at: TimestampMillis::new(observed_at_millis),
            },
            HostObservedProcessState::Lost { observed_at_millis } => Self::Lost {
                observed_at: TimestampMillis::new(observed_at_millis),
            },
            HostObservedProcessState::Orphaned { observed_at_millis } => Self::Orphaned {
                observed_at: TimestampMillis::new(observed_at_millis),
            },
            HostObservedProcessState::Unknown { observed_at_millis } => Self::Unknown {
                observed_at: TimestampMillis::new(observed_at_millis),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HostGapRecovery {
    RestartAtAvailableOffset,
    RequestScreenSnapshot,
}

impl From<OutputGapRecovery> for HostGapRecovery {
    fn from(value: OutputGapRecovery) -> Self {
        match value {
            OutputGapRecovery::RestartAtAvailableOffset => Self::RestartAtAvailableOffset,
            OutputGapRecovery::RequestScreenSnapshot => Self::RequestScreenSnapshot,
        }
    }
}

/// Allocation-bounded private host frame codec.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HostCodec {
    maximum_frame_bytes: usize,
}

impl HostCodec {
    pub(crate) fn new(maximum_frame_bytes: usize) -> Result<Self, HostProtocolError> {
        if maximum_frame_bytes < HEADER_BYTES {
            return Err(HostProtocolError::InvalidFrameLimit);
        }
        Ok(Self {
            maximum_frame_bytes,
        })
    }

    pub(crate) fn maximum_frame_bytes(self) -> usize {
        self.maximum_frame_bytes
    }

    pub(crate) fn encode_request(
        self,
        request: &HostRequest,
    ) -> Result<Vec<u8>, HostProtocolError> {
        self.encode_json(HostFrameKind::Request, request)
    }

    pub(crate) fn encode_response(
        self,
        response: &HostResponse,
    ) -> Result<Vec<u8>, HostProtocolError> {
        self.encode_json(HostFrameKind::Response, response)
    }

    pub(crate) fn encode_exit(self, event: &HostExitEvent) -> Result<Vec<u8>, HostProtocolError> {
        self.encode_json(HostFrameKind::Exit, event)
    }

    pub(crate) fn encode_cooperative(
        self,
        event: &HostCooperativeEvent,
    ) -> Result<Vec<u8>, HostProtocolError> {
        self.encode_json(HostFrameKind::Cooperative, event)
    }

    pub(crate) fn encode_output(
        self,
        output: &HostOutputFrame,
    ) -> Result<Vec<u8>, HostProtocolError> {
        let session = output.session_id.as_bytes();
        let process = output.process_id.as_bytes();
        let (session_len, process_len, body_len) = output_frame_lengths(output)?;
        let mut encoded = self.frame(HostFrameKind::Output, body_len)?;
        encoded.extend_from_slice(&session_len.to_be_bytes());
        encoded.extend_from_slice(session);
        encoded.extend_from_slice(&process_len.to_be_bytes());
        encoded.extend_from_slice(process);
        encoded.extend_from_slice(&output.start_offset.to_be_bytes());
        encoded.extend_from_slice(&output.bytes);
        Ok(encoded)
    }

    pub(crate) fn encoded_output_len(
        self,
        output: &HostOutputFrame,
    ) -> Result<usize, HostProtocolError> {
        let (_, _, body_len) = output_frame_lengths(output)?;
        self.validated_frame_len(body_len)
    }

    pub(crate) fn encode_input(self, input: &HostInputFrame) -> Result<Vec<u8>, HostProtocolError> {
        let session = input.session_id.as_bytes();
        let process = input.process_id.as_bytes();
        let session_len =
            u16::try_from(session.len()).map_err(|_| HostProtocolError::SizeOverflow)?;
        let process_len =
            u16::try_from(process.len()).map_err(|_| HostProtocolError::SizeOverflow)?;
        let attempt = input
            .attempt
            .as_ref()
            .map(serde_json::to_vec)
            .transpose()
            .map_err(HostProtocolError::Json)?;
        let attempt_bytes = attempt
            .as_ref()
            .map_or(0, |attempt| 2_usize.saturating_add(attempt.len()));
        let pacing_bytes = if input.pacing.is_some() { 12 } else { 0 };
        let body_len = 8_usize
            .checked_add(2)
            .and_then(|value| value.checked_add(session.len()))
            .and_then(|value| value.checked_add(2))
            .and_then(|value| value.checked_add(process.len()))
            .and_then(|value| value.checked_add(1))
            .and_then(|value| value.checked_add(pacing_bytes))
            .and_then(|value| value.checked_add(attempt_bytes))
            .and_then(|value| value.checked_add(input.bytes.len()))
            .ok_or(HostProtocolError::SizeOverflow)?;
        let mut encoded = self.frame(HostFrameKind::Input, body_len)?;
        encoded.extend_from_slice(&input.request_id.to_be_bytes());
        encoded.extend_from_slice(&session_len.to_be_bytes());
        encoded.extend_from_slice(session);
        encoded.extend_from_slice(&process_len.to_be_bytes());
        encoded.extend_from_slice(process);
        encoded.push(match (input.pacing.is_some(), attempt.is_some()) {
            (false, false) => 0,
            (true, false) => 1,
            (false, true) => 2,
            (true, true) => 3,
        });
        if let Some(pacing) = input.pacing {
            encoded.extend_from_slice(&pacing.offset.to_be_bytes());
            encoded.extend_from_slice(&pacing.delay_millis.to_be_bytes());
        }
        if let Some(attempt) = attempt {
            let length =
                u16::try_from(attempt.len()).map_err(|_| HostProtocolError::SizeOverflow)?;
            encoded.extend_from_slice(&length.to_be_bytes());
            encoded.extend_from_slice(&attempt);
        }
        encoded.extend_from_slice(&input.bytes);
        Ok(encoded)
    }

    pub(crate) fn decode(self, encoded: &[u8]) -> Result<HostDecodedFrame, HostProtocolError> {
        if encoded.len() < HEADER_BYTES || encoded[..4] != MAGIC {
            return Err(HostProtocolError::InvalidHeader);
        }
        if encoded[4] != VERSION || &encoded[10..HEADER_BYTES] != RELEASE_BYTES {
            return Err(HostProtocolError::UnsupportedRelease);
        }
        let kind = HostFrameKind::from_code(encoded[5])?;
        let body_len =
            u32::from_be_bytes(encoded[6..10].try_into().expect("fixed header")) as usize;
        let frame_len = HEADER_BYTES
            .checked_add(body_len)
            .ok_or(HostProtocolError::SizeOverflow)?;
        if frame_len > self.maximum_frame_bytes {
            return Err(HostProtocolError::FrameTooLarge {
                actual: frame_len,
                maximum: self.maximum_frame_bytes,
            });
        }
        if encoded.len() != frame_len {
            return Err(HostProtocolError::InvalidLength);
        }
        let body = &encoded[HEADER_BYTES..];
        match kind {
            HostFrameKind::Request => decode_json(body).map(HostDecodedFrame::Request),
            HostFrameKind::Response => decode_json(body).map(HostDecodedFrame::Response),
            HostFrameKind::Exit => decode_json(body).map(HostDecodedFrame::Exit),
            HostFrameKind::Output => decode_output(body).map(HostDecodedFrame::Output),
            HostFrameKind::Input => decode_input(body).map(HostDecodedFrame::Input),
            HostFrameKind::Cooperative => decode_json(body).map(HostDecodedFrame::Cooperative),
        }
    }

    fn encode_json<T: Serialize>(
        self,
        kind: HostFrameKind,
        value: &T,
    ) -> Result<Vec<u8>, HostProtocolError> {
        let mut body = serde_json::to_vec(value).map_err(HostProtocolError::Json)?;
        let result = self.frame(kind, body.len()).map(|mut encoded| {
            encoded.extend_from_slice(&body);
            encoded
        });
        body.fill(0);
        result
    }

    fn frame(self, kind: HostFrameKind, body_len: usize) -> Result<Vec<u8>, HostProtocolError> {
        let frame_len = self.validated_frame_len(body_len)?;
        let body_len = u32::try_from(body_len).map_err(|_| HostProtocolError::SizeOverflow)?;
        let mut encoded = Vec::with_capacity(frame_len);
        encoded.extend_from_slice(&MAGIC);
        encoded.push(VERSION);
        encoded.push(kind.code());
        encoded.extend_from_slice(&body_len.to_be_bytes());
        encoded.extend_from_slice(RELEASE_BYTES);
        Ok(encoded)
    }

    fn validated_frame_len(self, body_len: usize) -> Result<usize, HostProtocolError> {
        let frame_len = HEADER_BYTES
            .checked_add(body_len)
            .ok_or(HostProtocolError::SizeOverflow)?;
        let _ = u32::try_from(body_len).map_err(|_| HostProtocolError::SizeOverflow)?;
        if frame_len > self.maximum_frame_bytes {
            return Err(HostProtocolError::FrameTooLarge {
                actual: frame_len,
                maximum: self.maximum_frame_bytes,
            });
        }
        Ok(frame_len)
    }
}

fn output_frame_lengths(output: &HostOutputFrame) -> Result<(u16, u16, usize), HostProtocolError> {
    let session_len =
        u16::try_from(output.session_id.len()).map_err(|_| HostProtocolError::SizeOverflow)?;
    let process_len =
        u16::try_from(output.process_id.len()).map_err(|_| HostProtocolError::SizeOverflow)?;
    let body_len = 2_usize
        .checked_add(output.session_id.len())
        .and_then(|value| value.checked_add(2))
        .and_then(|value| value.checked_add(output.process_id.len()))
        .and_then(|value| value.checked_add(8))
        .and_then(|value| value.checked_add(output.bytes.len()))
        .ok_or(HostProtocolError::SizeOverflow)?;
    Ok((session_len, process_len, body_len))
}

/// One complete decoded private host frame.
#[derive(Debug)]
pub(crate) enum HostDecodedFrame {
    Request(HostRequest),
    Response(HostResponse),
    Exit(HostExitEvent),
    Output(HostOutputFrame),
    Input(HostInputFrame),
    Cooperative(HostCooperativeEvent),
}

fn decode_json<T: DeserializeOwned>(body: &[u8]) -> Result<T, HostProtocolError> {
    serde_json::from_slice(body).map_err(HostProtocolError::Json)
}

fn decode_output(body: &[u8]) -> Result<HostOutputFrame, HostProtocolError> {
    let mut cursor = 0_usize;
    let session_len = take_u16(body, &mut cursor)?;
    let session_id = take_string(body, &mut cursor, session_len)?;
    let process_len = take_u16(body, &mut cursor)?;
    let process_id = take_string(body, &mut cursor, process_len)?;
    let start_offset = u64::from_be_bytes(
        take(body, &mut cursor, 8)?
            .try_into()
            .expect("fixed output offset"),
    );
    let bytes = body[cursor..].to_vec();
    Ok(HostOutputFrame {
        session_id,
        process_id,
        start_offset,
        bytes,
    })
}

fn decode_input(body: &[u8]) -> Result<HostInputFrame, HostProtocolError> {
    let mut cursor = 0_usize;
    let request_id = u64::from_be_bytes(
        take(body, &mut cursor, 8)?
            .try_into()
            .expect("fixed request ID"),
    );
    let session_len = take_u16(body, &mut cursor)?;
    let session_id = take_string(body, &mut cursor, session_len)?;
    let process_len = take_u16(body, &mut cursor)?;
    let process_id = take_string(body, &mut cursor, process_len)?;
    let tag = take(body, &mut cursor, 1)?[0];
    let pacing = if matches!(tag, 1 | 3) {
        Some(HostInputPacing {
            offset: u32::from_be_bytes(
                take(body, &mut cursor, 4)?
                    .try_into()
                    .expect("fixed pacing offset"),
            ),
            delay_millis: u64::from_be_bytes(
                take(body, &mut cursor, 8)?
                    .try_into()
                    .expect("fixed pacing delay"),
            ),
        })
    } else if matches!(tag, 0 | 2) {
        None
    } else {
        return Err(HostProtocolError::InvalidDomainValue);
    };
    let attempt = if matches!(tag, 2 | 3) {
        let length = take_u16(body, &mut cursor)?;
        Some(
            serde_json::from_slice(take(body, &mut cursor, length)?)
                .map_err(HostProtocolError::Json)?,
        )
    } else {
        None
    };
    let bytes = body[cursor..].to_vec();
    Ok(HostInputFrame {
        request_id,
        attempt,
        session_id,
        process_id,
        bytes,
        pacing,
    })
}

fn take_u16(body: &[u8], cursor: &mut usize) -> Result<usize, HostProtocolError> {
    Ok(u16::from_be_bytes(take(body, cursor, 2)?.try_into().expect("fixed length")) as usize)
}

fn take_string(
    body: &[u8],
    cursor: &mut usize,
    length: usize,
) -> Result<String, HostProtocolError> {
    std::str::from_utf8(take(body, cursor, length)?)
        .map(str::to_owned)
        .map_err(|_| HostProtocolError::InvalidUtf8)
}

fn take<'a>(
    body: &'a [u8],
    cursor: &mut usize,
    length: usize,
) -> Result<&'a [u8], HostProtocolError> {
    let end = cursor
        .checked_add(length)
        .ok_or(HostProtocolError::SizeOverflow)?;
    let value = body
        .get(*cursor..end)
        .ok_or(HostProtocolError::TruncatedFrame)?;
    *cursor = end;
    Ok(value)
}

/// Typed private protocol failure without payload disclosure.
#[derive(Debug)]
pub(crate) enum HostProtocolError {
    InvalidFrameLimit,
    InvalidHeader,
    UnsupportedRelease,
    UnknownFrameKind(u8),
    FrameTooLarge { actual: usize, maximum: usize },
    InvalidLength,
    TruncatedFrame,
    InvalidUtf8,
    InvalidDomainValue,
    SizeOverflow,
    Json(serde_json::Error),
}

impl Display for HostProtocolError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidFrameLimit => formatter.write_str("session-host frame limit is invalid"),
            Self::InvalidHeader => formatter.write_str("session-host frame header is invalid"),
            Self::UnsupportedRelease => write!(
                formatter,
                "session-host release is unsupported; expected {}",
                undercurrent_foundation::RELEASE_VERSION
            ),
            Self::UnknownFrameKind(kind) => {
                write!(formatter, "session-host frame kind {kind} is unknown")
            }
            Self::FrameTooLarge { actual, maximum } => write!(
                formatter,
                "session-host frame size {actual} exceeds limit {maximum}"
            ),
            Self::InvalidLength => formatter.write_str("session-host frame length is invalid"),
            Self::TruncatedFrame => formatter.write_str("session-host frame is truncated"),
            Self::InvalidUtf8 => formatter.write_str("session-host identifier encoding is invalid"),
            Self::InvalidDomainValue => formatter.write_str("session-host domain value is invalid"),
            Self::SizeOverflow => formatter.write_str("session-host frame size overflowed"),
            Self::Json(_) => formatter.write_str("session-host control payload is invalid"),
        }
    }
}

impl Error for HostProtocolError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Json(source) => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMIT: usize = 4096;

    #[test]
    fn shell_capture_directory_is_optional() {
        let command = HostCommand::CaptureShellEnvironment {
            token: "test-token".to_owned(),
            session_id: "session-1".to_owned(),
            shell_pid: 42,
            environment: HostResolvedSecretEnvironment::from_values(Vec::new()),
            working_directory: None,
        };
        let mut encoded = serde_json::to_value(&command).unwrap();
        assert!(encoded["working_directory"].is_null());
        let decoded: HostCommand = serde_json::from_value(encoded.clone()).unwrap();
        assert!(matches!(
            decoded,
            HostCommand::CaptureShellEnvironment {
                working_directory: None,
                ..
            }
        ));
        encoded["working_directory"] = serde_json::json!(r"C:\work with spaces");
        let decoded: HostCommand = serde_json::from_value(encoded).unwrap();
        assert!(
            matches!(decoded, HostCommand::CaptureShellEnvironment { working_directory: Some(path), .. } if path == r"C:\work with spaces")
        );
    }

    #[test]
    fn frame_registry_codes_are_unique_and_complete() {
        let mut codes: Vec<_> = HostFrameKind::ALL.iter().map(|kind| kind.code()).collect();
        codes.sort_unstable();
        codes.dedup();
        assert_eq!(codes.len(), HostFrameKind::ALL.len());
    }

    #[test]
    fn raw_output_round_trips_without_payload_expansion() {
        let codec = HostCodec::new(LIMIT).unwrap();
        let output = HostOutputFrame {
            session_id: "session-1".to_owned(),
            process_id: "host-1-42-1".to_owned(),
            start_offset: 991,
            bytes: vec![0, 1, 2, 0xff],
        };
        let encoded = codec.encode_output(&output).unwrap();
        assert!(encoded.len() < output.bytes.len() + 64);
        let HostDecodedFrame::Output(decoded) = codec.decode(&encoded).unwrap() else {
            panic!("decoded wrong frame kind");
        };
        assert_eq!(decoded, output);
    }

    #[test]
    fn cooperative_event_round_trips_without_terminal_payload() {
        let codec = HostCodec::new(LIMIT).unwrap();
        let event = HostCooperativeEvent {
            session_id: "session-1".to_owned(),
            process_id: "host-1-42-1".to_owned(),
            state: HostCooperativeState::Ready,
            observed_at_millis: 123,
        };
        let encoded = codec.encode_cooperative(&event).unwrap();
        let HostDecodedFrame::Cooperative(decoded) = codec.decode(&encoded).unwrap() else {
            panic!("decoded wrong frame kind");
        };
        assert_eq!(decoded, event);
        assert!(!encoded.windows(4).any(|window| window == b"cmd="));
    }

    #[test]
    fn foreground_member_requires_environment_completeness() {
        let member = HostForegroundMember {
            incarnation: "42:100".to_owned(),
            program: "codex".to_owned(),
            arguments: Vec::new(),
            environment_names: vec!["PATH".to_owned()],
            environment_names_complete: false,
        };
        let encoded = serde_json::to_vec(&member).expect("encode foreground member");
        let decoded: HostForegroundMember =
            serde_json::from_slice(&encoded).expect("decode foreground member");
        assert!(!decoded.environment_names_complete);

        assert!(serde_json::from_str::<HostForegroundMember>(
            r#"{"incarnation":"42:100","program":"codex","arguments":[],"environment_names":["PATH"]}"#
        ).is_err());
    }

    #[test]
    fn raw_input_round_trips_without_payload_expansion() {
        let codec = HostCodec::new(LIMIT).unwrap();
        let input = HostInputFrame {
            request_id: 17,
            attempt: None,
            session_id: "session-1".to_owned(),
            process_id: "host-1-42-1".to_owned(),
            bytes: vec![0, 1, 2, 0xff],
            pacing: Some(HostInputPacing {
                offset: 2,
                delay_millis: 120,
            }),
        };
        let encoded = codec.encode_input(&input).unwrap();
        assert!(encoded.len() < input.bytes.len() + 74);
        let HostDecodedFrame::Input(decoded) = codec.decode(&encoded).unwrap() else {
            panic!("decoded wrong frame kind");
        };
        assert_eq!(decoded, input);
    }

    #[test]
    fn attempted_input_keeps_binary_payload_out_of_json() {
        let codec = HostCodec::new(LIMIT).unwrap();
        let input = HostInputFrame {
            request_id: 18,
            attempt: Some(HostAttemptContext {
                attempt_id: "attempt-18".to_owned(),
                session_id: Some("session-1".to_owned()),
                fencing_scope: Some("dispatch:cell-1".to_owned()),
                kind: HostAttemptKind::WriteInput,
                fencing_token: 4,
                payload_digest: "ab".repeat(32),
                accepted_at_millis: 123,
            }),
            session_id: "session-1".to_owned(),
            process_id: "host-1-42-1".to_owned(),
            bytes: vec![0, 1, 2, 0xff],
            pacing: None,
        };
        let encoded = codec.encode_input(&input).unwrap();
        assert!(encoded.len() < input.bytes.len() + 512);
        assert_eq!(&encoded[encoded.len() - input.bytes.len()..], input.bytes);
        let HostDecodedFrame::Input(decoded) = codec.decode(&encoded).unwrap() else {
            panic!("decoded wrong frame kind");
        };
        assert_eq!(decoded, input);
    }

    #[test]
    fn request_round_trips_and_rejects_trailing_or_oversized_bytes() {
        let codec = HostCodec::new(LIMIT).unwrap();
        let request = HostRequest {
            request_id: 7,
            command: HostCommand::Authenticate {
                token: "private-token".to_owned(),
            },
        };
        let encoded = codec.encode_request(&request).unwrap();
        let HostDecodedFrame::Request(decoded) = codec.decode(&encoded).unwrap() else {
            panic!("decoded wrong frame kind");
        };
        assert_eq!(decoded.request_id, 7);

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(matches!(
            codec.decode(&trailing),
            Err(HostProtocolError::InvalidLength)
        ));
        assert!(matches!(
            HostCodec::new(encoded.len() - 1).unwrap().decode(&encoded),
            Err(HostProtocolError::FrameTooLarge { .. })
        ));
    }

    #[test]
    fn launch_round_trip_revalidates_domain_values() {
        let limits = ExecutionLimits {
            max_arguments: 4,
            max_command_bytes: 128,
            max_environment_entries: 4,
            max_environment_bytes: 128,
            max_path_bytes: 128,
            max_health_rules: 1,
            max_health_rule_bytes: 1,
        };
        let environment =
            EnvironmentVariables::new([("KEY".into(), "value".into())], limits).unwrap();
        let secret_environment = SecretEnvironmentVariables::new(
            [(
                "TOKEN".into(),
                SecretReference::new("opaque-handle").unwrap(),
            )],
            &environment,
            limits,
        )
        .unwrap();
        let launch = LaunchSpec::new(
            CommandSpec::new("sh", vec!["-c".into(), "printf test".into()], limits).unwrap(),
            environment,
            None,
            Some(PathReference::new("/tmp", limits.max_path_bytes).unwrap()),
            TerminalSize::new(80, 24, 200, 100).unwrap(),
        )
        .with_secret_environment(secret_environment, limits)
        .unwrap()
        .with_output_capture_mode(OutputCaptureMode::Opaque);
        for mode in [
            undercurrent_domain::LaunchMode::Native,
            undercurrent_domain::LaunchMode::Sealed,
        ] {
            let command = launch.command().clone().with_launch_mode(mode);
            let selected = launch.clone().with_command(command);
            let bytes = serde_json::to_vec(&HostLaunchSpec::from(&selected)).unwrap();
            let decoded =
                LaunchSpec::try_from(serde_json::from_slice::<HostLaunchSpec>(&bytes).unwrap())
                    .unwrap();
            assert_eq!(decoded, selected);
        }
        let mut incomplete = serde_json::to_value(HostLaunchSpec::from(&launch)).unwrap();
        incomplete.as_object_mut().unwrap().remove("launch_mode");
        assert!(serde_json::from_value::<HostLaunchSpec>(incomplete).is_err());
    }

    #[test]
    fn prepared_launch_round_trip_preserves_all_object_identities() {
        let limits = ExecutionLimits {
            max_arguments: 1,
            max_command_bytes: 256,
            max_environment_entries: 1,
            max_environment_bytes: 16,
            max_path_bytes: 256,
            max_health_rules: 1,
            max_health_rule_bytes: 1,
        };
        let content = PayloadDigest::of_bytes(b"content");
        let executable_identity = PayloadDigest::of_bytes(b"executable object");
        let root_identity = PayloadDigest::of_bytes(b"root object");
        let cwd_identity = PayloadDigest::of_bytes(b"cwd object");
        let command = CommandSpec::new("example-tool", Vec::new(), limits)
            .unwrap()
            .pin_executable_with_identity(
                "/home/bin/example-tool",
                content,
                executable_identity,
                PayloadDigest::of_bytes(b"receipt"),
                limits,
            )
            .unwrap();
        let launch = LaunchSpec::new(
            command,
            EnvironmentVariables::new(Vec::new(), limits).unwrap(),
            None,
            Some(PathReference::new("/home/work", limits.max_path_bytes).unwrap()),
            TerminalSize::new(80, 24, 200, 100).unwrap(),
        )
        .with_environment_binding(Some(
            ExecutionEnvironmentBinding::new(
                PayloadDigest::of_bytes(b"binding"),
                PathReference::new("/home", limits.max_path_bytes).unwrap(),
            )
            .with_root_identity(Some(root_identity)),
        ))
        .with_working_directory_identity(Some(cwd_identity));

        let decoded = LaunchSpec::try_from(HostLaunchSpec::from(&launch)).unwrap();

        assert_eq!(decoded, launch);
        assert_eq!(
            decoded
                .command()
                .executable_resolution()
                .and_then(|resolution| resolution.object_identity()),
            Some(executable_identity)
        );
        assert_eq!(decoded.working_directory_identity(), Some(cwd_identity));
        assert_eq!(
            decoded
                .environment_binding()
                .and_then(ExecutionEnvironmentBinding::root_identity),
            Some(root_identity)
        );
    }

    #[test]
    fn incomplete_private_launch_and_observation_are_rejected() {
        assert!(serde_json::from_str::<HostLaunchSpec>(
            r#"{"program":"sh","arguments":[],"environment":[],"environment_file":null,"working_directory":null,"columns":80,"rows":24}"#
        ).is_err());
        assert!(serde_json::from_str::<HostResponseValue>(
            r#"{"result":"observed","state":{"state":"running","observed_at_millis":1},"columns":80,"rows":24}"#
        ).is_err());
    }

    #[test]
    fn invalid_zero_stop_deadline_is_rejected() {
        assert!(matches!(
            HostCommand::stop_policy(HostStopSignal::Terminate, 0),
            Err(HostProtocolError::InvalidDomainValue)
        ));
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StoredLaunchMode {
    Native,
    #[default]
    Sealed,
}
impl From<undercurrent_domain::LaunchMode> for StoredLaunchMode {
    fn from(mode: undercurrent_domain::LaunchMode) -> Self {
        match mode {
            undercurrent_domain::LaunchMode::Native => Self::Native,
            undercurrent_domain::LaunchMode::Sealed => Self::Sealed,
        }
    }
}
impl From<StoredLaunchMode> for undercurrent_domain::LaunchMode {
    fn from(mode: StoredLaunchMode) -> Self {
        match mode {
            StoredLaunchMode::Native => Self::Native,
            StoredLaunchMode::Sealed => Self::Sealed,
        }
    }
}
