// SPDX-License-Identifier: Apache-2.0

//! Typed CLI boundary failures and stable process exit translation.

use std::error::Error;
use std::fmt::{Display, Formatter};

use crate::cli_support::{CliOutputError, CommonExitCode, OutputShapeError};
use crate::duration::DurationParseError;
use undercurrent_protocol::{ProductErrorCategory, ProductErrorDisposition};
use undercurrent_sdk::{LocalClientConfigError, ProductClientError, ProductClientErrorCategory};

use crate::{
    CliCommand, CliConfigurationError, CliContextError, CliExecutionLimitsError, CliSignalError,
    DetachSequenceError, ResetError, ResetErrorKind, ShellIntegrationError,
    ShellIntegrationErrorKind,
};

/// Stable field categories used by safe CLI input diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliInputField {
    /// Process command selection.
    Command,
    /// Caller current directory.
    CurrentDirectory,
    /// Session working directory.
    WorkingDirectory,
    /// Environment-file reference.
    EnvironmentFile,
    /// Explicit environment entry.
    Environment,
    /// Query-visible label.
    Label,
    /// Private metadata entry.
    Metadata,
    /// Initial automatic restart delay.
    RestartDelay,
    /// Maximum automatic restart delay.
    MaximumRestartDelay,
    /// Stable-uptime threshold.
    MinimumUptime,
    /// Graceful stop timeout.
    StopTimeout,
    /// Graceful daemon drain timeout.
    DrainTimeout,
    /// Health rule declaration.
    HealthRule,
    /// Initial terminal size.
    TerminalSize,
    /// Bounded query page size.
    PageSize,
    /// Ordered terminal input.
    TerminalInput,
    /// Interactive detach sequence.
    DetachKeys,
}

impl CliInputField {
    /// Returns the stable safe field name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Command => "command",
            Self::CurrentDirectory => "current_directory",
            Self::WorkingDirectory => "working_directory",
            Self::EnvironmentFile => "environment_file",
            Self::Environment => "environment",
            Self::Label => "label",
            Self::Metadata => "metadata",
            Self::RestartDelay => "restart_delay",
            Self::MaximumRestartDelay => "maximum_restart_delay",
            Self::MinimumUptime => "minimum_uptime",
            Self::StopTimeout => "stop_timeout",
            Self::DrainTimeout => "drain_timeout",
            Self::HealthRule => "health_rule",
            Self::TerminalSize => "terminal_size",
            Self::PageSize => "page_size",
            Self::TerminalInput => "terminal_input",
            Self::DetachKeys => "detach_keys",
        }
    }
}

/// Payload-safe local input preparation failure.
#[derive(Debug)]
pub enum CliInputError {
    /// Native argument decoding failed without retaining raw argument values.
    InvalidArguments,
    /// A non-interactive start omitted its process command.
    MissingCommand,
    /// Interactive shell creation was combined with a direct process command.
    InteractiveCommandConflict,
    /// A caller-supplied key/value item was structurally invalid.
    InvalidKeyValue {
        /// Safe field category; the original value is not retained.
        field: CliInputField,
        /// Zero-based item index.
        index: usize,
    },
    /// A duration failed the Undercurrent duration grammar.
    InvalidDuration {
        /// Safe duration field.
        field: CliInputField,
        /// Typed parse cause.
        source: DurationParseError,
    },
    /// A parsed duration could not be represented in protocol milliseconds.
    DurationOutOfRange {
        /// Safe duration field.
        field: CliInputField,
    },
    /// A path required by the daemon protocol was not valid UTF-8.
    NonUtf8Path {
        /// Safe path field.
        field: CliInputField,
    },
    /// The supplied caller current directory was not absolute.
    RelativeCurrentDirectory,
    /// Restart backoff was below the domain's 1.0x minimum.
    InvalidRestartBackoff,
    /// Maximum restart delay was shorter than the initial delay.
    InvalidRestartDelayOrder,
    /// Initial terminal dimensions contained zero.
    InvalidTerminalSize,
    /// A bounded query requested zero rows.
    InvalidPageSize,
    /// An explicit named selection cannot fit in its watched page.
    SelectionExceedsPage {
        /// Number of distinct selected sessions.
        selected: usize,
        /// Requested bounded page size.
        page_size: u32,
    },
    /// A health declaration did not match the retained typed grammar.
    InvalidHealthRule {
        /// Zero-based rule index without retaining the rule payload.
        index: usize,
    },
    /// No terminal input item was supplied.
    MissingTerminalInput,
    /// A wait omitted every observable target state.
    MissingWaitCondition,
    /// A programmatically constructed bulk command omitted its required selection.
    MissingSessionSelection,
    /// Stop omitted a selector where the interactive picker cannot be displayed.
    StopPickerRequiresSelection,
    /// One named terminal key was unsupported.
    InvalidTerminalKey {
        /// Zero-based key index without retaining the key text.
        index: usize,
    },
    /// Piped terminal input exceeded the resolved finite request bound.
    StdinTooLarge {
        /// Maximum bytes admitted before request encoding overhead.
        maximum: usize,
    },
    /// Piped terminal input was not valid UTF-8 text.
    StdinNotUtf8,
    /// Interactive attachment requires a real terminal on the selected stream.
    InteractiveTerminalRequired {
        /// Stream that was redirected or unavailable.
        stream: CliTerminalStream,
    },
    /// Interactive detach keys violated the bounded grammar.
    InvalidDetachKeys {
        /// Payload-safe structural cause.
        source: DetachSequenceError,
    },
    /// One translated interactive input item exceeded the daemon request bound.
    TerminalInputTooLarge {
        /// Maximum estimated bytes accepted by one request.
        maximum: usize,
    },
}

impl Display for CliInputError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidArguments => {
                formatter.write_str("invalid command arguments; run `uc --help` for usage")
            }
            Self::MissingCommand => formatter
                .write_str("start requires a command after -- unless --interactive is selected"),
            Self::InteractiveCommandConflict => formatter.write_str(
                "--interactive cannot be combined with a command after --\nhelp: use `uc start NAME --interactive --attach` and run the command inside the shell, or remove `--interactive` to supervise the command directly",
            ),
            Self::InvalidKeyValue { field, index } => write!(
                formatter,
                "{} item {} must use non-empty KEY=VALUE syntax",
                field.as_str(),
                index.saturating_add(1)
            ),
            Self::InvalidDuration { field, .. } => {
                write!(formatter, "{} is not a valid duration", field.as_str())
            }
            Self::DurationOutOfRange { field } => {
                write!(formatter, "{} is too large", field.as_str())
            }
            Self::NonUtf8Path { field } => {
                write!(formatter, "{} must be valid UTF-8", field.as_str())
            }
            Self::RelativeCurrentDirectory => {
                formatter.write_str("current_directory must be absolute")
            }
            Self::InvalidRestartBackoff => {
                formatter.write_str("restart backoff must be at least 10000 basis points")
            }
            Self::InvalidRestartDelayOrder => formatter
                .write_str("maximum restart delay cannot be shorter than initial restart delay"),
            Self::InvalidTerminalSize => {
                formatter.write_str("terminal columns and rows must be non-zero")
            }
            Self::InvalidPageSize => formatter.write_str("page_size must be non-zero"),
            Self::SelectionExceedsPage {
                selected,
                page_size,
            } => write!(
                formatter,
                "{selected} selected sessions cannot fit in a watched page of {page_size}"
            ),
            Self::InvalidHealthRule { index } => write!(
                formatter,
                "health rule {} has an unsupported or invalid typed form",
                index.saturating_add(1)
            ),
            Self::MissingTerminalInput => {
                formatter.write_str("send requires text, stdin content, or at least one --key item")
            }
            Self::MissingWaitCondition => formatter.write_str(
                "wait requires at least one --execution, --interaction, --lifecycle, or --health target",
            ),
            Self::MissingSessionSelection => {
                formatter.write_str("session command requires one explicit selection")
            }
            Self::StopPickerRequiresSelection => {
                formatter.write_str("specify a session name, --all, or --label KEY=VALUE; the stop picker requires terminal stdin/stdout and text output")
            }
            Self::InvalidTerminalKey { index } => write!(
                formatter,
                "terminal key {} has an unsupported typed form",
                index.saturating_add(1)
            ),
            Self::StdinTooLarge { maximum } => {
                write!(
                    formatter,
                    "piped terminal input exceeds the {maximum}-byte limit"
                )
            }
            Self::StdinNotUtf8 => formatter.write_str("piped terminal input must be valid UTF-8"),
            Self::InteractiveTerminalRequired { stream } => write!(
                formatter,
                "attach requires an interactive {} terminal",
                stream.as_str()
            ),
            Self::InvalidDetachKeys { source } => Display::fmt(source, formatter),
            Self::TerminalInputTooLarge { maximum } => write!(
                formatter,
                "interactive terminal input exceeds the {maximum}-byte request limit"
            ),
        }
    }
}

impl Error for CliInputError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidDuration { source, .. } => Some(source),
            Self::InvalidDetachKeys { source } => Some(source),
            Self::InvalidArguments
            | Self::MissingCommand
            | Self::InteractiveCommandConflict
            | Self::InvalidKeyValue { .. }
            | Self::DurationOutOfRange { .. }
            | Self::NonUtf8Path { .. }
            | Self::RelativeCurrentDirectory
            | Self::InvalidRestartBackoff
            | Self::InvalidRestartDelayOrder
            | Self::InvalidTerminalSize
            | Self::InvalidPageSize
            | Self::SelectionExceedsPage { .. }
            | Self::InvalidHealthRule { .. }
            | Self::MissingTerminalInput
            | Self::MissingWaitCondition
            | Self::MissingSessionSelection
            | Self::StopPickerRequiresSelection
            | Self::InvalidTerminalKey { .. }
            | Self::StdinTooLarge { .. }
            | Self::StdinNotUtf8 => None,
            Self::InteractiveTerminalRequired { .. } | Self::TerminalInputTooLarge { .. } => None,
        }
    }
}

/// Native standard stream required for interactive attachment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliTerminalStream {
    /// Process standard input.
    StandardInput,
    /// Process standard output.
    StandardOutput,
}

impl CliTerminalStream {
    /// Returns the stable safe stream name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StandardInput => "standard-input",
            Self::StandardOutput => "standard-output",
        }
    }
}

/// Stable native terminal operation used by payload-safe diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliTerminalOperation {
    /// Read current geometry.
    ReadSize,
    /// Enable raw input mode.
    EnterRawMode,
    /// Restore cooked input mode.
    RestoreRawMode,
    /// Wait for an input event.
    PollEvent,
    /// Decode one available input event.
    ReadEvent,
}

impl CliTerminalOperation {
    /// Returns the stable operation name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadSize => "read_size",
            Self::EnterRawMode => "enter_raw_mode",
            Self::RestoreRawMode => "restore_raw_mode",
            Self::PollEvent => "poll_event",
            Self::ReadEvent => "read_event",
        }
    }
}

/// Concrete native terminal failure with payload-safe presentation.
#[derive(Debug)]
pub struct CliTerminalError {
    operation: CliTerminalOperation,
    source: std::io::Error,
}

impl CliTerminalError {
    /// Wraps one native terminal IO failure without exposing its message publicly.
    pub const fn new(operation: CliTerminalOperation, source: std::io::Error) -> Self {
        Self { operation, source }
    }

    /// Returns the stable failed operation.
    pub const fn operation(&self) -> CliTerminalOperation {
        self.operation
    }
}

impl Display for CliTerminalError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "interactive terminal {} failed",
            self.operation.as_str()
        )
    }
}

impl Error for CliTerminalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

/// Stable native standard-input operation used by payload-safe diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliStandardInputOperation {
    /// Read one bounded piped terminal-input body.
    ReadTerminalInput,
    /// Read one bounded interactive destructive-operation confirmation.
    ReadConfirmation,
}

impl CliStandardInputOperation {
    /// Returns the stable operation name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadTerminalInput => "read_terminal_input",
            Self::ReadConfirmation => "read_confirmation",
        }
    }
}

/// Stable external process operation used by payload-safe CLI diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliProcessOperation {
    /// Resolve the caller's current working directory.
    ResolveCurrentDirectory,
    /// Resolve the currently running executable.
    ResolveCurrentExecutable,
    /// Spawn the detached local daemon process.
    SpawnDaemon,
    /// Probe the OS-released local daemon instance lock.
    ProbeDaemonInstance,
}

impl CliProcessOperation {
    /// Returns the safe operation name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ResolveCurrentDirectory => "resolve_current_directory",
            Self::ResolveCurrentExecutable => "resolve_current_executable",
            Self::SpawnDaemon => "spawn_daemon",
            Self::ProbeDaemonInstance => "probe_daemon_instance",
        }
    }
}

/// Stable daemon process-lifecycle orchestration failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliDaemonLifecycleError {
    /// No daemon owns the configured local instance.
    NotRunning,
    /// The configured daemon retained its instance lock beyond the bound.
    ShutdownTimedOut,
    /// A reported replacement retained the prior daemon's server epoch.
    ReplacementEpochUnchanged,
}

impl Display for CliDaemonLifecycleError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotRunning => formatter.write_str("configured daemon is not running"),
            Self::ShutdownTimedOut => {
                formatter.write_str("timed out waiting for configured daemon shutdown")
            }
            Self::ReplacementEpochUnchanged => {
                formatter.write_str("replacement daemon retained the previous server epoch")
            }
        }
    }
}

impl Error for CliDaemonLifecycleError {}

/// Complete failure model for the native CLI presentation boundary.
#[derive(Debug)]
pub enum CliError {
    /// A support archive could not be written; source paths stay private.
    SupportReport(std::io::Error),
    /// Local workspace metadata, ownership, or terminal UI failure.
    Workspace {
        /// Safe explanation, excluding process payloads and filesystem paths.
        message: String,
        /// Optional underlying filesystem error.
        source: Option<std::io::Error>,
    },
    /// Global presentation or timeout selection was invalid.
    Context(CliContextError),
    /// Command arguments could not be prepared safely.
    Input(CliInputError),
    /// Native CLI orchestration bounds violated their resolved invariant.
    ExecutionLimits(CliExecutionLimitsError),
    /// Selected machine output did not match the command's finite/stream shape.
    OutputShape(OutputShapeError),
    /// Result serialization or presentation writer IO failed.
    Output(CliOutputError),
    /// Raw terminal bytes were requested through a structured output mode.
    RawOutputRequiresText,
    /// An exact utility document was requested through a structured output mode.
    TextDocumentRequiresText {
        /// Utility command whose bytes cannot be wrapped in structured output.
        command: CliCommand,
    },
    /// Effective daemon and client configuration could not be resolved.
    Configuration(CliConfigurationError),
    /// Saved CLI connection selection or publication failed.
    Settings(crate::CliSettingsError),
    /// Concrete local IPC transport construction failed.
    LocalTransport(LocalClientConfigError),
    /// Typed product connection, request, or stream execution failed.
    Product(ProductClientError),
    /// An exact human-facing target name matched no session.
    SessionNotFound {
        /// Safe exact name supplied by the operator.
        name: String,
    },
    /// A caller-owned wait deadline elapsed before its condition was observed.
    WaitTimedOut {
        /// Safe exact session name supplied by the operator.
        name: String,
    },
    /// A complete command activation deadline elapsed between bounded operations.
    CommandTimedOut {
        /// Stable command registry value.
        command: CliCommand,
    },
    /// Daemon execution returned output that requires an unavailable retrieval path.
    ExecOutputUnavailable {
        /// Stable standard stream name.
        channel: &'static str,
    },
    /// Daemon execution output failed its transmitted SHA-256 integrity check.
    ExecOutputIntegrity {
        /// Stable standard stream name.
        channel: &'static str,
    },
    /// A paged catalog response violated continuation invariants.
    PaginationInvariant {
        /// Stable command whose selection could not be completed safely.
        command: CliCommand,
    },
    /// A bulk selection exceeded its configured retained-ID ceiling.
    SelectionCapacity {
        /// Stable command whose selection exceeded the bound.
        command: CliCommand,
        /// Inclusive number of stable session IDs that may be retained.
        limit: usize,
    },
    /// Daemon process location, spawn, or status IO failed.
    Process {
        /// Safe process operation.
        operation: CliProcessOperation,
        /// Concrete source retained for diagnostics.
        source: std::io::Error,
    },
    /// Local daemon lifecycle orchestration failed.
    DaemonLifecycle(CliDaemonLifecycleError),
    /// Confirmed exact local-state removal failed.
    Reset(ResetError),
    /// A destructive command requires explicit interactive or flag confirmation.
    ApprovalRequired {
        /// Stable destructive command registry value.
        command: CliCommand,
    },
    /// Parsed command state violated the complete native routing contract.
    CommandRoutingInvariant {
        /// Stable command registry value.
        command: CliCommand,
    },
    /// Reading bounded native standard input failed.
    StandardInput {
        /// Safe standard-input operation.
        operation: CliStandardInputOperation,
        /// Concrete source retained outside public presentation.
        source: std::io::Error,
    },
    /// Scoped native stream-signal handling failed.
    Signals(CliSignalError),
    /// Native interactive terminal setup, input, or restoration failed.
    Terminal(CliTerminalError),
    /// Native shell-hook inspection or publication failed.
    ShellIntegration(ShellIntegrationError),
}

impl CliError {
    /// Returns the stable shared process exit category.
    pub const fn exit_code(&self) -> CommonExitCode {
        match self {
            Self::Settings(source) => source.exit_code(),
            Self::Context(_)
            | Self::Input(_)
            | Self::Configuration(_)
            | Self::OutputShape(_)
            | Self::RawOutputRequiresText
            | Self::TextDocumentRequiresText { .. } => CommonExitCode::InvalidInput,
            Self::Workspace { .. }
            | Self::ExecutionLimits(_)
            | Self::Output(_)
            | Self::Signals(_)
            | Self::Terminal(_)
            | Self::CommandRoutingInvariant { .. }
            | Self::StandardInput { .. } => CommonExitCode::Runtime,
            Self::ShellIntegration(source) => match source.kind() {
                ShellIntegrationErrorKind::InvalidConfiguration
                | ShellIntegrationErrorKind::UnsafePath
                | ShellIntegrationErrorKind::InvalidStartupFile => CommonExitCode::InvalidInput,
                ShellIntegrationErrorKind::CapacityExceeded => CommonExitCode::Incomplete,
                ShellIntegrationErrorKind::Filesystem => CommonExitCode::Runtime,
            },
            Self::ApprovalRequired { .. } => CommonExitCode::ApprovalRequired,
            Self::DaemonLifecycle(source) => match source {
                CliDaemonLifecycleError::NotRunning => CommonExitCode::Failure,
                CliDaemonLifecycleError::ShutdownTimedOut => CommonExitCode::Incomplete,
                CliDaemonLifecycleError::ReplacementEpochUnchanged => CommonExitCode::Runtime,
            },
            Self::Reset(source) => match source.kind() {
                ResetErrorKind::Busy => CommonExitCode::Incomplete,
                ResetErrorKind::UnsafeArtifact | ResetErrorKind::Filesystem => {
                    CommonExitCode::Runtime
                }
            },
            Self::Product(source) => product_error_exit_code(source),
            Self::SessionNotFound { .. } => CommonExitCode::Failure,
            Self::WaitTimedOut { .. }
            | Self::CommandTimedOut { .. }
            | Self::SelectionCapacity { .. }
            | Self::ExecOutputUnavailable { .. } => CommonExitCode::Incomplete,
            Self::ExecOutputIntegrity { .. } => CommonExitCode::Runtime,
            Self::PaginationInvariant { .. } => CommonExitCode::Runtime,
            Self::LocalTransport(_) | Self::Process { .. } | Self::SupportReport(_) => {
                CommonExitCode::Runtime
            }
        }
    }

    /// Returns the exact server category when a product error supplied one.
    pub const fn product_error_category(&self) -> Option<ProductErrorCategory> {
        match self {
            Self::Product(source) => source.product_error_category(),
            _ => None,
        }
    }

    /// Returns explicit owner evidence for one finite product failure.
    /// Local, aggregate and legacy failures do not invent whole-operation certainty.
    pub const fn error_effect_certainty(
        &self,
    ) -> Option<undercurrent_protocol::EffectCertaintyDto> {
        match self {
            Self::Product(ProductClientError::Server(error)) => error.effect_certainty,
            _ => None,
        }
    }

    /// Returns the exact machine-readable error disposition when known.
    ///
    /// A legacy server error without a disposition remains `None`; it is never inferred from its
    /// category. Local client failures use their typed client or CLI variant.
    pub fn error_disposition(&self) -> Option<ProductErrorDisposition> {
        match self {
            Self::Settings(source) => Some(match source.exit_code() {
                CommonExitCode::InvalidInput => ProductErrorDisposition::SemanticRejection,
                _ => ProductErrorDisposition::InfrastructureFailure,
            }),
            Self::Product(source) => product_client_disposition(source),
            Self::Context(_)
            | Self::Input(_)
            | Self::Configuration(_)
            | Self::OutputShape(_)
            | Self::RawOutputRequiresText
            | Self::TextDocumentRequiresText { .. } => {
                Some(ProductErrorDisposition::SemanticRejection)
            }
            Self::WaitTimedOut { .. } | Self::CommandTimedOut { .. } => {
                Some(ProductErrorDisposition::Timeout)
            }
            Self::ExecOutputUnavailable { .. } => Some(ProductErrorDisposition::Partial),
            Self::ExecOutputIntegrity { .. } | Self::PaginationInvariant { .. } => {
                Some(ProductErrorDisposition::ProtocolFailure)
            }
            Self::Output(_) | Self::Process { .. } | Self::Signals(_) => {
                Some(ProductErrorDisposition::InfrastructureFailure)
            }
            Self::LocalTransport(_) => Some(ProductErrorDisposition::TransportFailure),
            Self::SelectionCapacity { .. } => Some(ProductErrorDisposition::CapacityRejected),
            _ => None,
        }
    }
}

fn product_client_disposition(error: &ProductClientError) -> Option<ProductErrorDisposition> {
    if let Some(disposition) = error.product_error_disposition() {
        return Some(disposition);
    }
    if error.product_error_category().is_some() {
        return None;
    }
    Some(match error.category() {
        ProductClientErrorCategory::InvalidArgument => ProductErrorDisposition::SemanticRejection,
        ProductClientErrorCategory::Overloaded => ProductErrorDisposition::CapacityRejected,
        ProductClientErrorCategory::Timeout => ProductErrorDisposition::Timeout,
        ProductClientErrorCategory::Cancelled => ProductErrorDisposition::Cancellation,
        ProductClientErrorCategory::Unavailable => ProductErrorDisposition::TransportFailure,
        ProductClientErrorCategory::ProtocolViolation => ProductErrorDisposition::ProtocolFailure,
        ProductClientErrorCategory::ServerRejected => return None,
    })
}

pub(crate) const fn product_error_exit_code(error: &ProductClientError) -> CommonExitCode {
    match error.category() {
        ProductClientErrorCategory::InvalidArgument => CommonExitCode::InvalidInput,
        ProductClientErrorCategory::Overloaded | ProductClientErrorCategory::Timeout => {
            CommonExitCode::Incomplete
        }
        ProductClientErrorCategory::Cancelled => CommonExitCode::Interrupted,
        ProductClientErrorCategory::ServerRejected => server_exit_code(error),
        ProductClientErrorCategory::Unavailable | ProductClientErrorCategory::ProtocolViolation => {
            CommonExitCode::Runtime
        }
    }
}

const fn server_exit_code(error: &ProductClientError) -> CommonExitCode {
    let category = match error {
        ProductClientError::Server(error) => Some(error.category),
        ProductClientError::StreamRejected(error) => Some(error.category),
        _ => None,
    };
    match category {
        Some(undercurrent_protocol::ProductErrorCategory::InvalidRequest)
        | Some(undercurrent_protocol::ProductErrorCategory::InvalidConfiguration) => {
            CommonExitCode::InvalidInput
        }
        Some(undercurrent_protocol::ProductErrorCategory::Unauthenticated)
        | Some(undercurrent_protocol::ProductErrorCategory::PermissionDenied) => {
            CommonExitCode::Denied
        }
        Some(undercurrent_protocol::ProductErrorCategory::Cancelled) => CommonExitCode::Interrupted,
        Some(undercurrent_protocol::ProductErrorCategory::TimedOut)
        | Some(undercurrent_protocol::ProductErrorCategory::CapacityExceeded)
        | Some(undercurrent_protocol::ProductErrorCategory::ReplayUnavailable)
        | Some(undercurrent_protocol::ProductErrorCategory::CursorExpired) => {
            CommonExitCode::Incomplete
        }
        Some(undercurrent_protocol::ProductErrorCategory::Transport)
        | Some(undercurrent_protocol::ProductErrorCategory::ProtocolMismatch)
        | Some(undercurrent_protocol::ProductErrorCategory::Internal)
        | None => CommonExitCode::Runtime,
        Some(undercurrent_protocol::ProductErrorCategory::NotFound)
        | Some(undercurrent_protocol::ProductErrorCategory::NameConflict)
        | Some(undercurrent_protocol::ProductErrorCategory::FailedPrecondition)
        | Some(undercurrent_protocol::ProductErrorCategory::RevisionPreconditionFailed)
        | Some(undercurrent_protocol::ProductErrorCategory::Process)
        | Some(undercurrent_protocol::ProductErrorCategory::Terminal)
        | Some(undercurrent_protocol::ProductErrorCategory::Persistence) => CommonExitCode::Failure,
    }
}

impl Display for CliError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Context(source) => Display::fmt(source, formatter),
            Self::Input(source) => Display::fmt(source, formatter),
            Self::ExecutionLimits(source) => Display::fmt(source, formatter),
            Self::OutputShape(source) => Display::fmt(source, formatter),
            Self::Output(source) => Display::fmt(source, formatter),
            Self::RawOutputRequiresText => {
                formatter.write_str("raw terminal output requires text format")
            }
            Self::TextDocumentRequiresText { command } => {
                write!(
                    formatter,
                    "{} output requires text format",
                    command.as_str()
                )
            }
            Self::Configuration(source) => Display::fmt(source, formatter),
            Self::Settings(source) => Display::fmt(source, formatter),
            Self::LocalTransport(source) => Display::fmt(source, formatter),
            Self::Product(source) => Display::fmt(source, formatter),
            Self::SessionNotFound { name } => write!(formatter, "session {name:?} was not found"),
            Self::WaitTimedOut { name } => {
                write!(formatter, "timed out waiting for session {name:?}")
            }
            Self::CommandTimedOut { command } => {
                write!(formatter, "{} command deadline elapsed", command.as_str())
            }
            Self::ExecOutputUnavailable { channel } => {
                write!(formatter, "daemon {channel} output is not available inline")
            }
            Self::ExecOutputIntegrity { channel } => {
                write!(
                    formatter,
                    "daemon {channel} output failed integrity validation"
                )
            }
            Self::PaginationInvariant { command } => write!(
                formatter,
                "{} received an invalid paged catalog continuation",
                command.as_str()
            ),
            Self::SelectionCapacity { command, limit } => write!(
                formatter,
                "{} selection exceeds the configured {limit}-session limit",
                command.as_str()
            ),
            Self::Process { operation, .. } => {
                write!(formatter, "CLI process {} failed", operation.as_str())
            }
            Self::DaemonLifecycle(source) => Display::fmt(source, formatter),
            Self::Reset(source) => Display::fmt(source, formatter),
            Self::ApprovalRequired { command } => write!(
                formatter,
                "{} requires explicit confirmation or --yes",
                command.as_str()
            ),
            Self::CommandRoutingInvariant { command } => write!(
                formatter,
                "{} reached an invalid native command routing state",
                command.as_str()
            ),
            Self::StandardInput { operation, .. } => {
                write!(formatter, "standard input {} failed", operation.as_str())
            }
            Self::Signals(source) => Display::fmt(source, formatter),
            Self::Terminal(source) => Display::fmt(source, formatter),
            Self::ShellIntegration(source) => Display::fmt(source, formatter),
            Self::Workspace { message, .. } => formatter.write_str(message),
            Self::SupportReport(source) => write!(
                formatter,
                "could not write support report ({:?}); choose a new writable output path",
                source.kind()
            ),
        }
    }
}

impl Error for CliError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Context(source) => Some(source),
            Self::SupportReport(source) => Some(source),
            Self::Input(source) => Some(source),
            Self::ExecutionLimits(source) => Some(source),
            Self::OutputShape(source) => Some(source),
            Self::Output(source) => Some(source),
            Self::Configuration(source) => Some(source),
            Self::Settings(source) => Some(source),
            Self::LocalTransport(source) => Some(source),
            Self::Product(source) => Some(source),
            Self::Signals(source) => Some(source),
            Self::Terminal(source) => Some(source),
            Self::ShellIntegration(source) => Some(source),
            Self::DaemonLifecycle(source) => Some(source),
            Self::Reset(source) => Some(source),
            Self::StandardInput { source, .. } => Some(source),
            Self::SessionNotFound { .. }
            | Self::WaitTimedOut { .. }
            | Self::CommandTimedOut { .. }
            | Self::ExecOutputUnavailable { .. }
            | Self::ExecOutputIntegrity { .. }
            | Self::PaginationInvariant { .. }
            | Self::SelectionCapacity { .. }
            | Self::RawOutputRequiresText
            | Self::TextDocumentRequiresText { .. }
            | Self::ApprovalRequired { .. }
            | Self::CommandRoutingInvariant { .. } => None,
            Self::Process { source, .. } => Some(source),
            Self::Workspace { source, .. } => source.as_ref().map(|e| e as &(dyn Error + 'static)),
        }
    }
}

impl From<CliContextError> for CliError {
    fn from(source: CliContextError) -> Self {
        Self::Context(source)
    }
}

impl From<ResetError> for CliError {
    fn from(source: ResetError) -> Self {
        Self::Reset(source)
    }
}

impl From<CliInputError> for CliError {
    fn from(source: CliInputError) -> Self {
        Self::Input(source)
    }
}

impl From<CliExecutionLimitsError> for CliError {
    fn from(source: CliExecutionLimitsError) -> Self {
        Self::ExecutionLimits(source)
    }
}

impl From<CliDaemonLifecycleError> for CliError {
    fn from(source: CliDaemonLifecycleError) -> Self {
        Self::DaemonLifecycle(source)
    }
}

impl From<OutputShapeError> for CliError {
    fn from(source: OutputShapeError) -> Self {
        Self::OutputShape(source)
    }
}

impl From<CliOutputError> for CliError {
    fn from(source: CliOutputError) -> Self {
        Self::Output(source)
    }
}

impl From<CliConfigurationError> for CliError {
    fn from(source: CliConfigurationError) -> Self {
        Self::Configuration(source)
    }
}

impl From<ProductClientError> for CliError {
    fn from(source: ProductClientError) -> Self {
        Self::Product(source)
    }
}

impl From<CliSignalError> for CliError {
    fn from(source: CliSignalError) -> Self {
        Self::Signals(source)
    }
}

impl From<CliTerminalError> for CliError {
    fn from(source: CliTerminalError) -> Self {
        Self::Terminal(source)
    }
}

impl From<ShellIntegrationError> for CliError {
    fn from(source: ShellIntegrationError) -> Self {
        Self::ShellIntegration(source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn presentation_and_runtime_categories_have_stable_exit_codes() {
        assert_eq!(
            CliError::Context(CliContextError::ConflictingVerbosity).exit_code(),
            CommonExitCode::InvalidInput
        );
        assert_eq!(
            CliError::Process {
                operation: CliProcessOperation::SpawnDaemon,
                source: std::io::Error::other("private source"),
            }
            .exit_code(),
            CommonExitCode::Runtime
        );
    }

    #[test]
    fn process_error_display_does_not_expose_source_text() {
        let error = CliError::Process {
            operation: CliProcessOperation::SpawnDaemon,
            source: std::io::Error::other("/private/path"),
        };

        assert!(!error.to_string().contains("/private/path"));
    }
}
