// SPDX-License-Identifier: Apache-2.0

//! Central registry of externally visible product operations and stream kinds.

use serde::{Deserialize, Serialize};

/// Stable finite-operation names used by diagnostics and authorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductOperation {
    /// Start one durable managed session.
    StartSession,
    /// Stop one managed session.
    StopSession,
    /// Restart one managed session.
    RestartSession,
    /// Rename one session without changing its stable identity.
    RenameSession,
    /// Atomically set or remove query-visible labels on one session.
    UpdateSessionLabels,
    /// Remove one terminal session after explicit confirmation.
    RemoveSession,
    /// Resize one live terminal.
    ResizeTerminal,
    /// Deliver one ordered terminal input sequence.
    SendTerminalInput,
    /// Close one managed session's standard-input stream.
    CloseStandardInput,
    /// Return one bounded prepared summary page.
    ListSessions,
    /// Return explicitly projected detail for one session.
    GetSessionDetail,
    /// Return private environment and cwd for authorized one-shot execution.
    GetExecutionContext,
    /// Execute one bounded command under daemon supervision in a session context.
    ExecuteInSession,
    /// Return one bounded retained terminal-output page.
    ReadTerminalOutput,
    /// Return one current terminal screen snapshot.
    ReadTerminalScreen,
    /// Return one bounded update from an issued terminal screen checkpoint.
    ReadTerminalScreenDiff,
    /// Return one bounded durable event page.
    ReadSessionEvents,
    /// Insert or replace one external work declaration.
    UpsertWorkItem,
    /// Remove one external work declaration.
    RemoveWorkItem,
    /// List compact detection-profile summaries.
    ListProfiles,
    /// Return one detection profile.
    GetProfile,
    /// Match a process description against detection profiles.
    MatchProfile,
    /// Validate all configured detection profiles.
    LintProfiles,
    /// Evaluate one profile against a supplied sample.
    TestProfile,
    /// Return bounded daemon and connection diagnostics.
    GetDiagnostics,
    /// Create one bounded privacy-safe diagnostic bundle.
    CreateDiagnosticBundle,
    /// Return daemon identity, health, and pressure.
    GetDaemonStatus,
    /// Begin a bounded graceful daemon shutdown.
    ShutdownDaemon,
    /// Repair one durable Unknown attempt from retained trusted evidence.
    RepairOperationAttempt,
    /// Read one exact caller-bound runtime attempt without changing product state.
    GetRuntimeAttemptStatus,
    /// Durably admit and apply control to one exact attached runtime attempt.
    CancelRuntimeAttempt,
    /// Read one exact durable runtime-cancellation control without redrive.
    GetRuntimeCancellationStatus,
    /// Read one exact unsettled agent event for an attached runtime attempt.
    GetAgentEvent,
    /// Durably reply to one exact attached-runtime agent event.
    ReplyAgentEvent,
}

impl ProductOperation {
    /// Complete finite-operation registry in stable diagnostic order.
    pub const ALL: [Self; 34] = [
        Self::StartSession,
        Self::StopSession,
        Self::RestartSession,
        Self::RenameSession,
        Self::UpdateSessionLabels,
        Self::RemoveSession,
        Self::ResizeTerminal,
        Self::SendTerminalInput,
        Self::CloseStandardInput,
        Self::ListSessions,
        Self::GetSessionDetail,
        Self::GetExecutionContext,
        Self::ExecuteInSession,
        Self::ReadTerminalOutput,
        Self::ReadTerminalScreen,
        Self::ReadTerminalScreenDiff,
        Self::ReadSessionEvents,
        Self::UpsertWorkItem,
        Self::RemoveWorkItem,
        Self::ListProfiles,
        Self::GetProfile,
        Self::MatchProfile,
        Self::LintProfiles,
        Self::TestProfile,
        Self::GetDiagnostics,
        Self::CreateDiagnosticBundle,
        Self::GetDaemonStatus,
        Self::ShutdownDaemon,
        Self::RepairOperationAttempt,
        Self::GetRuntimeAttemptStatus,
        Self::CancelRuntimeAttempt,
        Self::GetRuntimeCancellationStatus,
        Self::GetAgentEvent,
        Self::ReplyAgentEvent,
    ];

    /// Returns the stable snake-case operation name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StartSession => "start_session",
            Self::StopSession => "stop_session",
            Self::RestartSession => "restart_session",
            Self::RenameSession => "rename_session",
            Self::UpdateSessionLabels => "update_session_labels",
            Self::RemoveSession => "remove_session",
            Self::ResizeTerminal => "resize_terminal",
            Self::SendTerminalInput => "send_terminal_input",
            Self::CloseStandardInput => "close_standard_input",
            Self::ListSessions => "list_sessions",
            Self::GetSessionDetail => "get_session_detail",
            Self::GetExecutionContext => "get_execution_context",
            Self::ExecuteInSession => "execute_in_session",
            Self::ReadTerminalOutput => "read_terminal_output",
            Self::ReadTerminalScreen => "read_terminal_screen",
            Self::ReadTerminalScreenDiff => "read_terminal_screen_diff",
            Self::ReadSessionEvents => "read_session_events",
            Self::UpsertWorkItem => "upsert_work_item",
            Self::RemoveWorkItem => "remove_work_item",
            Self::ListProfiles => "list_profiles",
            Self::GetProfile => "get_profile",
            Self::MatchProfile => "match_profile",
            Self::LintProfiles => "lint_profiles",
            Self::TestProfile => "test_profile",
            Self::GetDiagnostics => "get_diagnostics",
            Self::CreateDiagnosticBundle => "create_diagnostic_bundle",
            Self::GetDaemonStatus => "get_daemon_status",
            Self::ShutdownDaemon => "shutdown_daemon",
            Self::RepairOperationAttempt => "repair_operation_attempt",
            Self::GetRuntimeAttemptStatus => "get_runtime_attempt_status",
            Self::CancelRuntimeAttempt => "cancel_runtime_attempt",
            Self::GetRuntimeCancellationStatus => "get_runtime_cancellation_status",
            Self::GetAgentEvent => "get_agent_event",
            Self::ReplyAgentEvent => "reply_agent_event",
        }
    }

    /// Returns whether this operation can mutate product state.
    ///
    /// This classification does not define a uniform retry contract. In
    /// particular, repair is mutating but carries no [`crate::MutationContext`],
    /// and runtime cancellation owns a separate durable control ledger.
    pub const fn is_mutating(self) -> bool {
        matches!(
            self,
            Self::StartSession
                | Self::StopSession
                | Self::RestartSession
                | Self::RenameSession
                | Self::UpdateSessionLabels
                | Self::RemoveSession
                | Self::ResizeTerminal
                | Self::SendTerminalInput
                | Self::CloseStandardInput
                | Self::ExecuteInSession
                | Self::UpsertWorkItem
                | Self::RemoveWorkItem
                | Self::ShutdownDaemon
                | Self::RepairOperationAttempt
                | Self::CancelRuntimeAttempt
                | Self::ReplyAgentEvent
        )
    }
}

/// Stable product-level logical stream kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductStreamKind {
    /// Initial prepared page followed by membership and summary deltas.
    SessionWatch,
    /// Filtered authoritative session events.
    SessionEvents,
    /// Explicitly selected terminal output only.
    TerminalOutput,
    /// Durable agent tool calls for one exact fenced runtime attempt.
    AgentEvents,
}

impl ProductStreamKind {
    /// Complete product stream registry.
    pub const ALL: [Self; 4] = [
        Self::SessionWatch,
        Self::SessionEvents,
        Self::TerminalOutput,
        Self::AgentEvents,
    ];

    /// Returns the stable snake-case stream name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SessionWatch => "session_watch",
            Self::SessionEvents => "session_events",
            Self::TerminalOutput => "terminal_output",
            Self::AgentEvents => "agent_events",
        }
    }
}

/// Stable durable session-event vocabulary exposed by finite reads and streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionEventKindDto {
    /// A durable session record was created.
    SessionCreated,
    /// A session's human-facing name changed.
    SessionRenamed,
    /// Durable session configuration changed.
    SessionConfigurationChanged,
    /// A session was removed from the catalog.
    SessionRemoved,
    /// Authoritative lifecycle changed.
    LifecycleChanged,
    /// Concrete process ownership changed.
    ProcessOwnershipChanged,
    /// Best-effort foreground observation changed.
    ForegroundChanged,
    /// Foreground evidence changed without changing compact observation.
    ForegroundEvidenceChanged,
    /// Compact health status changed.
    HealthChanged,
    /// Detailed health evidence changed.
    HealthEvidenceChanged,
    /// WorkContext content or aggregate status changed.
    WorkContextChanged,
    /// Automatic restart was scheduled.
    RestartScheduled,
    /// A pending automatic restart was cancelled.
    RestartCancelled,
    /// Restart-attempt state changed.
    RestartStateChanged,
    /// Restart policy reached a terminal disposition.
    RestartExhausted,
    /// Terminal dimensions changed.
    TerminalResized,
}

impl SessionEventKindDto {
    /// Complete protocol event registry in stable declaration order.
    pub const ALL: [Self; 16] = [
        Self::SessionCreated,
        Self::SessionRenamed,
        Self::SessionConfigurationChanged,
        Self::SessionRemoved,
        Self::LifecycleChanged,
        Self::ProcessOwnershipChanged,
        Self::ForegroundChanged,
        Self::ForegroundEvidenceChanged,
        Self::HealthChanged,
        Self::HealthEvidenceChanged,
        Self::WorkContextChanged,
        Self::RestartScheduled,
        Self::RestartCancelled,
        Self::RestartStateChanged,
        Self::RestartExhausted,
        Self::TerminalResized,
    ];

    /// Returns the stable snake-case wire and presentation name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SessionCreated => "session_created",
            Self::SessionRenamed => "session_renamed",
            Self::SessionConfigurationChanged => "session_configuration_changed",
            Self::SessionRemoved => "session_removed",
            Self::LifecycleChanged => "lifecycle_changed",
            Self::ProcessOwnershipChanged => "process_ownership_changed",
            Self::ForegroundChanged => "foreground_changed",
            Self::ForegroundEvidenceChanged => "foreground_evidence_changed",
            Self::HealthChanged => "health_changed",
            Self::HealthEvidenceChanged => "health_evidence_changed",
            Self::WorkContextChanged => "work_context_changed",
            Self::RestartScheduled => "restart_scheduled",
            Self::RestartCancelled => "restart_cancelled",
            Self::RestartStateChanged => "restart_state_changed",
            Self::RestartExhausted => "restart_exhausted",
            Self::TerminalResized => "terminal_resized",
        }
    }
}

/// Centrally registered public error categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductErrorCategory {
    /// Request syntax or a typed value is invalid.
    InvalidRequest,
    /// Configuration is invalid or inconsistent.
    InvalidConfiguration,
    /// No matching session or resource exists.
    NotFound,
    /// A unique session name already exists.
    NameConflict,
    /// Current lifecycle or revision rejects the operation.
    FailedPrecondition,
    /// An explicit revision precondition failed under the session mutation
    /// lease before any mutation or process/delivery effect was attempted.
    RevisionPreconditionFailed,
    /// Authentication is absent or invalid.
    Unauthenticated,
    /// Principal is not authorized for the operation.
    PermissionDenied,
    /// Caller cancelled the request.
    Cancelled,
    /// A configured deadline elapsed.
    TimedOut,
    /// A bounded resource or rate limit is exhausted.
    CapacityExceeded,
    /// Requested replay data is no longer retained.
    ReplayUnavailable,
    /// Query cursor no longer represents the current catalog.
    CursorExpired,
    /// Negotiated protocol cannot represent the operation.
    ProtocolMismatch,
    /// Supervised process adapter failed.
    Process,
    /// PTY adapter failed.
    Terminal,
    /// Durable state adapter failed.
    Persistence,
    /// Physical transport failed.
    Transport,
    /// A server invariant failed without exposing internal detail.
    Internal,
}

impl ProductErrorCategory {
    /// Complete error-category registry.
    pub const ALL: [Self; 19] = [
        Self::InvalidRequest,
        Self::InvalidConfiguration,
        Self::NotFound,
        Self::NameConflict,
        Self::FailedPrecondition,
        Self::RevisionPreconditionFailed,
        Self::Unauthenticated,
        Self::PermissionDenied,
        Self::Cancelled,
        Self::TimedOut,
        Self::CapacityExceeded,
        Self::ReplayUnavailable,
        Self::CursorExpired,
        Self::ProtocolMismatch,
        Self::Process,
        Self::Terminal,
        Self::Persistence,
        Self::Transport,
        Self::Internal,
    ];

    /// Returns the canonical product-contract value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::InvalidConfiguration => "invalid_configuration",
            Self::NotFound => "not_found",
            Self::NameConflict => "name_conflict",
            Self::FailedPrecondition => "failed_precondition",
            Self::RevisionPreconditionFailed => "revision_precondition_failed",
            Self::Unauthenticated => "unauthenticated",
            Self::PermissionDenied => "permission_denied",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::CapacityExceeded => "capacity_exceeded",
            Self::ReplayUnavailable => "replay_unavailable",
            Self::CursorExpired => "cursor_expired",
            Self::ProtocolMismatch => "protocol_mismatch",
            Self::Process => "process",
            Self::Terminal => "terminal",
            Self::Persistence => "persistence",
            Self::Transport => "transport",
            Self::Internal => "internal",
        }
    }
}

/// Product-owned optional connection capabilities for the current 0.1.0-beta product contract.
pub mod product_capabilities {
    /// Enables the additive exact error `disposition` field on product error payloads.
    pub const ERROR_DISPOSITION: &str = "undercurrent.error-disposition";
    /// Enables explicit opaque output capture on session start and detail payloads.
    pub const OPAQUE_OUTPUT_CAPTURE: &str = "undercurrent.opaque-output-capture";
    /// Enables atomic expected-revision checks for input and stop, including
    /// the distinct pre-effect `revision_precondition_failed` error category.
    pub const SESSION_REVISION_PRECONDITION: &str = "undercurrent.session-revision-precondition";
    /// Enables explicit whole-operation effect certainty on finite errors.
    pub const ERROR_EFFECT_CERTAINTY: &str = "undercurrent.error-effect-certainty";
}

/// Stable high-level disposition that prevents error-category collapse across boundaries.
///
/// This is additive metadata beside [`ProductErrorCategory`]. It is emitted only when
/// [`product_capabilities::ERROR_DISPOSITION`] was negotiated. Consumers must preserve an absent
/// value rather than infer a possibly incorrect disposition from the category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductErrorDisposition {
    /// Request or current semantic state rejected the operation.
    SemanticRejection,
    /// Authentication or authorization rejected access.
    AccessRejected,
    /// A bounded capacity or rate limit rejected work.
    CapacityRejected,
    /// A supervised process or terminal runtime failed.
    RuntimeFailure,
    /// A durable or internal infrastructure boundary failed.
    InfrastructureFailure,
    /// The physical connection or transport failed.
    TransportFailure,
    /// Protocol negotiation, decoding, or correlation failed.
    ProtocolFailure,
    /// A bounded deadline elapsed without changing the semantic outcome.
    Timeout,
    /// Cancellation interrupted the operation.
    Cancellation,
    /// Some requested data or work remains unavailable or incomplete.
    Partial,
    /// Effect or reconciliation evidence remains unknown or inconclusive.
    UnknownInconclusive,
}

impl ProductErrorDisposition {
    /// Complete current-0.1.0-beta disposition registry.
    pub const ALL: [Self; 11] = [
        Self::SemanticRejection,
        Self::AccessRejected,
        Self::CapacityRejected,
        Self::RuntimeFailure,
        Self::InfrastructureFailure,
        Self::TransportFailure,
        Self::ProtocolFailure,
        Self::Timeout,
        Self::Cancellation,
        Self::Partial,
        Self::UnknownInconclusive,
    ];

    /// Returns the canonical current-0.1.0-beta wire value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SemanticRejection => "semantic_rejection",
            Self::AccessRejected => "access_rejected",
            Self::CapacityRejected => "capacity_rejected",
            Self::RuntimeFailure => "runtime_failure",
            Self::InfrastructureFailure => "infrastructure_failure",
            Self::TransportFailure => "transport_failure",
            Self::ProtocolFailure => "protocol_failure",
            Self::Timeout => "timeout",
            Self::Cancellation => "cancellation",
            Self::Partial => "partial",
            Self::UnknownInconclusive => "unknown_inconclusive",
        }
    }

    /// Returns the ordinary disposition for a category.
    ///
    /// Producers with stronger knowledge, especially unknown-effect or inconclusive
    /// reconciliation outcomes, must explicitly override this default.
    pub const fn default_for_category(category: ProductErrorCategory) -> Self {
        match category {
            ProductErrorCategory::InvalidRequest
            | ProductErrorCategory::InvalidConfiguration
            | ProductErrorCategory::NotFound
            | ProductErrorCategory::NameConflict
            | ProductErrorCategory::FailedPrecondition
            | ProductErrorCategory::RevisionPreconditionFailed => Self::SemanticRejection,
            ProductErrorCategory::Unauthenticated | ProductErrorCategory::PermissionDenied => {
                Self::AccessRejected
            }
            ProductErrorCategory::Cancelled => Self::Cancellation,
            ProductErrorCategory::TimedOut => Self::Timeout,
            ProductErrorCategory::CapacityExceeded => Self::CapacityRejected,
            ProductErrorCategory::ReplayUnavailable | ProductErrorCategory::CursorExpired => {
                Self::Partial
            }
            ProductErrorCategory::ProtocolMismatch => Self::ProtocolFailure,
            ProductErrorCategory::Process | ProductErrorCategory::Terminal => Self::RuntimeFailure,
            ProductErrorCategory::Persistence | ProductErrorCategory::Internal => {
                Self::InfrastructureFailure
            }
            ProductErrorCategory::Transport => Self::TransportFailure,
        }
    }
}
