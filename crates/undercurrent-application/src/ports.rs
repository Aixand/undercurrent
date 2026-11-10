// SPDX-License-Identifier: Apache-2.0

//! Inward-facing ports required by application use cases.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use undercurrent_domain::{
    DeliveryId, DomainEventKind, EffectiveProfileSnapshot, EffectiveProfileTransition,
    ForegroundEvidence, ForegroundObservation, HealthCheckResult, HealthRules, InputDeliveryPolicy,
    InputObservationPolicy, InputSequence, LaunchSpec, LifecycleState, PatternExpression,
    ProcessIdentity, ProcessInstanceId, ProcessOutcome, ProfileName, Revision, Session, SessionId,
    SessionName, StopPolicy, TerminalSize, TimestampMillis,
};

use crate::{
    ApplicationErrorCategory, DiagnosticRecord, ForegroundProcessGroupObservation,
    MetricObservation, ObservationJob, ObservationScheduleOutcome, OutboxAcknowledgeOutcome,
    OutboxSequence, ProcessMutationContext, ProfileCatalogEntry, ProfilePatternError,
    ProfileSummary, SessionEventPublishOutcome, SessionEventReadBatch, SessionEventReadRequest,
    SessionMetricObservation, SessionOutboxRecord, TerminalScreenSnapshot, TerminalScreenUpdate,
};

/// Heap-owned future returned by object-safe asynchronous ports.
pub type PortFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Stable infrastructure operation names used by errors, logs, traces, metrics,
/// and failure injection tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PortOperation {
    /// Evaluate one authenticated caller's permission at the request boundary.
    Authorize,
    /// Generate a stable session ID.
    GenerateSessionId,
    /// Insert a new durable session and outbox event.
    InsertSession,
    /// Load a durable session by ID.
    LoadSession,
    /// Load a durable session by name.
    FindSessionByName,
    /// Resolve and pin launch filesystem bindings before durable acceptance.
    PrepareLaunch,
    /// Resolve private launch environment and working-directory references.
    ResolveExecutionContext,
    /// Resolve opaque launch credential handles into one ephemeral environment overlay.
    ResolveSecretEnvironment,
    /// Execute one bounded command under daemon supervision in a session context.
    ExecuteInSession,
    /// Persist complete command output behind an immutable reference.
    StoreExecutionOutput,
    /// Commit a revision-checked durable session update and outbox events.
    CommitSession,
    /// Delete a durable session.
    DeleteSession,
    /// Spawn a concrete process instance.
    SpawnProcess,
    /// Release retained exit notification after exact supervisor binding.
    ArmProcessExit,
    /// Request graceful process termination.
    StopProcess,
    /// Force process termination.
    ForceProcess,
    /// Write one atomic terminal input sequence.
    WriteInput,
    /// Close one concrete process's standard-input stream.
    CloseInput,
    /// Resize a concrete terminal.
    ResizeTerminal,
    /// Read bounded bytes from one concrete terminal.
    ReadTerminalOutput,
    /// Read one parser-consistent terminal screen snapshot.
    ReadTerminalScreen,
    /// Compute one bounded update between coherent terminal screens.
    DiffTerminalScreen,
    /// Append bounded terminal bytes to replay retention.
    RetainTerminalOutput,
    /// Receive one armed concrete process-exit observation.
    ReadProcessExit,
    /// Inspect payload-safe process runtime pressure.
    InspectProcessRuntime,
    /// Shuts down the isolated process host after durable mutation admission.
    ShutdownProcessHost,
    /// Commits an evidence-backed terminal resolution for an Unknown attempt.
    RepairOperationAttempt,
    /// Reads the exact unresolved-attempt readiness projection.
    InspectOperationAttemptReadiness,
    /// Reads one exact caller-bound runtime attempt and coherent session snapshot.
    GetRuntimeAttemptStatus,
    /// Atomically validates and admits one exact runtime cancellation control.
    AdmitRuntimeCancellation,
    /// Durably marks that one admitted cancellation may cross the process-effect boundary.
    MarkRuntimeCancellationEffectStarted,
    /// Durably settles one admitted runtime cancellation control.
    SettleRuntimeCancellation,
    /// Reads one exact retained runtime cancellation control.
    GetRuntimeCancellationStatus,
    /// Collect typed foreground evidence for one session.
    CollectForegroundEvidence,
    /// Execute configured health checks for one session.
    RunHealthChecks,
    /// Atomically reserve a semantic input delivery ID.
    ReserveInputDelivery,
    /// Settle a reserved semantic input delivery result.
    SettleInputDelivery,
    /// Durably accept one process-affecting operation attempt.
    AcceptOperationAttempt,
    /// Durably settle one accepted operation attempt.
    SettleOperationAttempt,
    /// Prepare exact evidence for superseded dispatch cleanup.
    PrepareOperationAttemptCompensation,
    /// Terminalize one superseded dispatched attempt after exact cleanup.
    CompensateOperationAttempt,
    /// Reconcile one unknown-effect operation attempt.
    ReconcileOperationAttempt,
    /// Load one durable operation attempt.
    LoadOperationAttempt,
    /// Read bounded attempts requiring reconciliation.
    ReadPendingOperationAttempts,
    /// Observe retained runtime truth for one unknown-effect attempt.
    ObserveOperationAttemptEffect,
    /// Read pending durable outbox events.
    ReadOutbox,
    /// Acknowledge published durable outbox events.
    AcknowledgeOutbox,
    /// Idempotently publish one ordered durable session event.
    PublishSessionEvent,
    /// Update the prepared session projection.
    UpdateProjection,
    /// Read one exact-process effective-profile snapshot.
    ReadEffectiveProfile,
    /// Apply one exact-process effective-profile transition.
    UpdateEffectiveProfile,
    /// Bind a running process instance to its session supervisor.
    BindProcess,
    /// Resolve the running process instance owned by a session supervisor.
    LookupProcess,
    /// Remove a process binding after terminal lifecycle or compensation.
    UnbindProcess,
    /// Acquire ordered mutation ownership for one session.
    AcquireSessionMutation,
    /// Durably enqueue one revision-checked automatic restart attempt.
    ScheduleRestart,
    /// Claim a bounded batch of due automatic restart attempts.
    ClaimDueRestarts,
    /// Remove every queued automatic restart for one session.
    CancelSessionRestarts,
    /// Enqueue or revise one session-local observation deadline.
    ScheduleObservation,
    /// Claim a bounded due observation batch.
    ClaimDueObservations,
    /// Remove pending observations for one deleted session.
    CancelSessionObservations,
    /// Atomically apply one revision-aware per-session observation plan.
    ReconcileSessionObservations,
    /// Read one bounded stable-ID page for startup reconciliation.
    ReadRecoverySessions,
    /// Inspect bounded persistence pressure without hydrating aggregates.
    InspectPersistence,
    /// Resolve cached operating-system truth for one durable process identity.
    ObserveProcessTruth,
    /// Observe one exact managed PTY's foreground process group.
    ObserveForegroundProcessGroup,
    /// Read bounded detection-profile summaries or definitions.
    ReadDetectionProfiles,
    /// Evaluate one profile regular expression through the configured engine.
    EvaluateProfilePattern,
    /// Read one bounded filtered retained session-event batch.
    ReadSessionEvents,
    /// Open or continue one selective session-event subscription.
    SubscribeSessionEvents,
    /// Durably enqueue one authenticated local agent call.
    EnqueueAgentEvent,
    /// Read one exact unsettled agent call.
    PollAgentEvent,
    /// Durably retain one exact agent-event reply.
    ReplyAgentEvent,
    /// Read one exact retained agent-event reply.
    ReadAgentEventReply,
    /// Resolve an authenticated hosted peer to one current agent binding.
    ResolveAgentChannelTarget,
}

impl PortOperation {
    /// Complete port-operation registry in stable declaration order.
    pub const ALL: [Self; 75] = [
        Self::Authorize,
        Self::GenerateSessionId,
        Self::InsertSession,
        Self::LoadSession,
        Self::FindSessionByName,
        Self::PrepareLaunch,
        Self::ResolveExecutionContext,
        Self::ResolveSecretEnvironment,
        Self::ExecuteInSession,
        Self::StoreExecutionOutput,
        Self::CommitSession,
        Self::DeleteSession,
        Self::SpawnProcess,
        Self::ArmProcessExit,
        Self::StopProcess,
        Self::ForceProcess,
        Self::WriteInput,
        Self::CloseInput,
        Self::ResizeTerminal,
        Self::ReadTerminalOutput,
        Self::ReadTerminalScreen,
        Self::DiffTerminalScreen,
        Self::RetainTerminalOutput,
        Self::ReadProcessExit,
        Self::InspectProcessRuntime,
        Self::ShutdownProcessHost,
        Self::RepairOperationAttempt,
        Self::InspectOperationAttemptReadiness,
        Self::GetRuntimeAttemptStatus,
        Self::AdmitRuntimeCancellation,
        Self::MarkRuntimeCancellationEffectStarted,
        Self::SettleRuntimeCancellation,
        Self::GetRuntimeCancellationStatus,
        Self::CollectForegroundEvidence,
        Self::RunHealthChecks,
        Self::ReserveInputDelivery,
        Self::SettleInputDelivery,
        Self::AcceptOperationAttempt,
        Self::SettleOperationAttempt,
        Self::PrepareOperationAttemptCompensation,
        Self::CompensateOperationAttempt,
        Self::ReconcileOperationAttempt,
        Self::LoadOperationAttempt,
        Self::ReadPendingOperationAttempts,
        Self::ObserveOperationAttemptEffect,
        Self::ReadOutbox,
        Self::AcknowledgeOutbox,
        Self::PublishSessionEvent,
        Self::UpdateProjection,
        Self::ReadEffectiveProfile,
        Self::UpdateEffectiveProfile,
        Self::BindProcess,
        Self::LookupProcess,
        Self::UnbindProcess,
        Self::AcquireSessionMutation,
        Self::ScheduleRestart,
        Self::ClaimDueRestarts,
        Self::CancelSessionRestarts,
        Self::ScheduleObservation,
        Self::ClaimDueObservations,
        Self::CancelSessionObservations,
        Self::ReconcileSessionObservations,
        Self::ReadRecoverySessions,
        Self::InspectPersistence,
        Self::ObserveProcessTruth,
        Self::ObserveForegroundProcessGroup,
        Self::ReadDetectionProfiles,
        Self::EvaluateProfilePattern,
        Self::ReadSessionEvents,
        Self::SubscribeSessionEvents,
        Self::EnqueueAgentEvent,
        Self::PollAgentEvent,
        Self::ReplyAgentEvent,
        Self::ReadAgentEventReply,
        Self::ResolveAgentChannelTarget,
    ];

    /// Returns stable snake-case name used at adapter boundaries.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Authorize => "authorize",
            Self::GenerateSessionId => "generate_session_id",
            Self::InsertSession => "insert_session",
            Self::LoadSession => "load_session",
            Self::FindSessionByName => "find_session_by_name",
            Self::PrepareLaunch => "prepare_launch",
            Self::ResolveExecutionContext => "resolve_execution_context",
            Self::ResolveSecretEnvironment => "resolve_secret_environment",
            Self::ExecuteInSession => "execute_in_session",
            Self::StoreExecutionOutput => "store_execution_output",
            Self::CommitSession => "commit_session",
            Self::DeleteSession => "delete_session",
            Self::SpawnProcess => "spawn_process",
            Self::ArmProcessExit => "arm_process_exit",
            Self::StopProcess => "stop_process",
            Self::ForceProcess => "force_process",
            Self::WriteInput => "write_input",
            Self::CloseInput => "close_input",
            Self::ResizeTerminal => "resize_terminal",
            Self::ReadTerminalOutput => "read_terminal_output",
            Self::ReadTerminalScreen => "read_terminal_screen",
            Self::DiffTerminalScreen => "diff_terminal_screen",
            Self::RetainTerminalOutput => "retain_terminal_output",
            Self::ReadProcessExit => "read_process_exit",
            Self::InspectProcessRuntime => "inspect_process_runtime",
            Self::ShutdownProcessHost => "shutdown_process_host",
            Self::RepairOperationAttempt => "repair_operation_attempt",
            Self::InspectOperationAttemptReadiness => "inspect_operation_attempt_readiness",
            Self::GetRuntimeAttemptStatus => "get_runtime_attempt_status",
            Self::AdmitRuntimeCancellation => "admit_runtime_cancellation",
            Self::MarkRuntimeCancellationEffectStarted => {
                "mark_runtime_cancellation_effect_started"
            }
            Self::SettleRuntimeCancellation => "settle_runtime_cancellation",
            Self::GetRuntimeCancellationStatus => "get_runtime_cancellation_status",
            Self::CollectForegroundEvidence => "collect_foreground_evidence",
            Self::RunHealthChecks => "run_health_checks",
            Self::ReserveInputDelivery => "reserve_input_delivery",
            Self::SettleInputDelivery => "settle_input_delivery",
            Self::AcceptOperationAttempt => "accept_operation_attempt",
            Self::SettleOperationAttempt => "settle_operation_attempt",
            Self::PrepareOperationAttemptCompensation => "prepare_operation_attempt_compensation",
            Self::CompensateOperationAttempt => "compensate_operation_attempt",
            Self::ReconcileOperationAttempt => "reconcile_operation_attempt",
            Self::LoadOperationAttempt => "load_operation_attempt",
            Self::ReadPendingOperationAttempts => "read_pending_operation_attempts",
            Self::ObserveOperationAttemptEffect => "observe_operation_attempt_effect",
            Self::ReadOutbox => "read_outbox",
            Self::AcknowledgeOutbox => "acknowledge_outbox",
            Self::PublishSessionEvent => "publish_session_event",
            Self::UpdateProjection => "update_projection",
            Self::ReadEffectiveProfile => "read_effective_profile",
            Self::UpdateEffectiveProfile => "update_effective_profile",
            Self::BindProcess => "bind_process",
            Self::LookupProcess => "lookup_process",
            Self::UnbindProcess => "unbind_process",
            Self::AcquireSessionMutation => "acquire_session_mutation",
            Self::ScheduleRestart => "schedule_restart",
            Self::ClaimDueRestarts => "claim_due_restarts",
            Self::CancelSessionRestarts => "cancel_session_restarts",
            Self::ScheduleObservation => "schedule_observation",
            Self::ClaimDueObservations => "claim_due_observations",
            Self::CancelSessionObservations => "cancel_session_observations",
            Self::ReconcileSessionObservations => "reconcile_session_observations",
            Self::ReadRecoverySessions => "read_recovery_sessions",
            Self::InspectPersistence => "inspect_persistence",
            Self::ObserveProcessTruth => "observe_process_truth",
            Self::ObserveForegroundProcessGroup => "observe_foreground_process_group",
            Self::ReadDetectionProfiles => "read_detection_profiles",
            Self::EvaluateProfilePattern => "evaluate_profile_pattern",
            Self::ReadSessionEvents => "read_session_events",
            Self::SubscribeSessionEvents => "subscribe_session_events",
            Self::EnqueueAgentEvent => "enqueue_agent_event",
            Self::PollAgentEvent => "poll_agent_event",
            Self::ReplyAgentEvent => "reply_agent_event",
            Self::ReadAgentEventReply => "read_agent_event_reply",
            Self::ResolveAgentChannelTarget => "resolve_agent_channel_target",
        }
    }
}

/// Centrally registered application actions evaluated independently at the
/// authorization boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AccessAction {
    /// Start a managed session.
    StartSession,
    /// Gracefully or forcibly stop a managed session.
    StopSession,
    /// Restart a managed session.
    RestartSession,
    /// Rename a managed session.
    RenameSession,
    /// Permanently remove a terminal session record.
    RemoveSession,
    /// Send private input to a terminal.
    SendTerminalInput,
    /// Reply to one exact protected local agent event.
    ReplyAgentEvent,
    /// Close a managed process's standard-input stream.
    CloseStandardInput,
    /// Resize a terminal.
    ResizeTerminal,
    /// List compact session summaries.
    ListSessions,
    /// Read an authorized detailed session projection.
    ReadSessionDetail,
    /// Resolve private environment and working directory for one-shot execution.
    ExecuteInSessionContext,
    /// Watch a filtered session query.
    WatchSessions,
    /// Subscribe to private terminal output.
    SubscribeTerminal,
    /// Read a terminal screen snapshot or diff.
    ReadScreen,
    /// Read retained application or session logs.
    ReadLogs,
    /// Read registered domain and diagnostic events.
    ReadEvents,
    /// Read detailed foreground-detection evidence.
    ExplainForeground,
    /// Read detailed health observations.
    ReadHealth,
    /// Read complete WorkContext declarations.
    ReadWorkContext,
    /// Upsert or remove WorkContext declarations.
    WriteWorkContext,
    /// Read or change effective configuration.
    ManageConfiguration,
    /// Read, match, lint, or test detection profiles.
    ReadDetectionProfiles,
    /// Control daemon lifecycle or reset state.
    ControlDaemon,
    /// Accept one authenticated and authorized external connection.
    AcceptConnection,
    /// Read bounded operational diagnostics.
    ReadDiagnostics,
    /// Export explicitly requested terminal content in a diagnostic bundle.
    ExportDiagnosticContent,
}

impl AccessAction {
    /// Complete action registry in stable declaration order.
    pub const ALL: [Self; 27] = [
        Self::StartSession,
        Self::StopSession,
        Self::RestartSession,
        Self::RenameSession,
        Self::RemoveSession,
        Self::SendTerminalInput,
        Self::ReplyAgentEvent,
        Self::CloseStandardInput,
        Self::ResizeTerminal,
        Self::ListSessions,
        Self::ReadSessionDetail,
        Self::ExecuteInSessionContext,
        Self::WatchSessions,
        Self::SubscribeTerminal,
        Self::ReadScreen,
        Self::ReadLogs,
        Self::ReadEvents,
        Self::ExplainForeground,
        Self::ReadHealth,
        Self::ReadWorkContext,
        Self::WriteWorkContext,
        Self::ManageConfiguration,
        Self::ReadDetectionProfiles,
        Self::ControlDaemon,
        Self::AcceptConnection,
        Self::ReadDiagnostics,
        Self::ExportDiagnosticContent,
    ];

    /// Returns the stable action name used by policy adapters and audit logs.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StartSession => "start_session",
            Self::StopSession => "stop_session",
            Self::RestartSession => "restart_session",
            Self::RenameSession => "rename_session",
            Self::RemoveSession => "remove_session",
            Self::SendTerminalInput => "send_terminal_input",
            Self::ReplyAgentEvent => "reply_agent_event",
            Self::CloseStandardInput => "close_standard_input",
            Self::ResizeTerminal => "resize_terminal",
            Self::ListSessions => "list_sessions",
            Self::ReadSessionDetail => "read_session_detail",
            Self::ExecuteInSessionContext => "execute_in_session_context",
            Self::WatchSessions => "watch_sessions",
            Self::SubscribeTerminal => "subscribe_terminal",
            Self::ReadScreen => "read_screen",
            Self::ReadLogs => "read_logs",
            Self::ReadEvents => "read_events",
            Self::ExplainForeground => "explain_foreground",
            Self::ReadHealth => "read_health",
            Self::ReadWorkContext => "read_work_context",
            Self::WriteWorkContext => "write_work_context",
            Self::ManageConfiguration => "manage_configuration",
            Self::ReadDetectionProfiles => "read_detection_profiles",
            Self::ControlDaemon => "control_daemon",
            Self::AcceptConnection => "accept_connection",
            Self::ReadDiagnostics => "read_diagnostics",
            Self::ExportDiagnosticContent => "export_diagnostic_content",
        }
    }
}

/// Resource scope evaluated by an authorization adapter bound to one
/// authenticated connection or request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessResource {
    /// The compact session collection.
    Sessions,
    /// One stable managed session.
    Session(SessionId),
    /// Effective configuration and detection profiles.
    Configuration,
    /// Daemon lifecycle and state.
    Daemon,
    /// Public local-IPC listener admission policy.
    LocalListener,
    /// Operational diagnostics.
    Diagnostics,
}

/// Stable reason an authenticated request was denied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AccessDenialReason {
    /// No valid authentication was established for the request.
    Unauthenticated,
    /// An authenticated principal lacks the requested permission.
    Forbidden,
}

/// Result of one independent authorization decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessDecision {
    /// The use case may access the requested resource.
    Permit,
    /// Access is rejected without leaking policy internals.
    Deny(AccessDenialReason),
}

/// Authorizes application actions for one adapter-established caller context.
///
/// Implementations are expected to be scoped to a connection or request and
/// therefore keep credentials and raw tokens outside the application core.
pub trait AuthorizationPort: Send + Sync {
    /// Evaluates one action and resource independently.
    ///
    /// # Errors
    ///
    /// Returns a typed adapter failure when policy evaluation cannot complete.
    fn authorize<'a>(
        &'a self,
        action: AccessAction,
        resource: &'a AccessResource,
    ) -> PortFuture<'a, Result<AccessDecision, PortError>>;
}

/// Private, fully resolved process context for authorized one-shot execution.
///
/// This value must never be logged, traced, included in compact projections,
/// or returned from a general detail query because it contains environment
/// values.
#[derive(Clone, PartialEq, Eq)]
pub struct ResolvedExecutionContext {
    live_environment_process: Option<ProcessInstanceId>,
    environment: BTreeMap<String, String>,
    working_directory: Option<String>,
}

impl ResolvedExecutionContext {
    /// Constructs a context already bounded and validated by its adapter.
    ///
    /// This function performs no IO, cannot fail, and does not panic.
    pub fn new(environment: BTreeMap<String, String>, working_directory: Option<String>) -> Self {
        Self {
            environment,
            working_directory,
            live_environment_process: None,
        }
    }

    /// Selects the host-private environment snapshot for this exact process.
    pub fn with_live_environment(mut self, process: ProcessInstanceId) -> Self {
        self.environment.clear();
        self.live_environment_process = Some(process);
        self
    }

    /// Returns the process whose live exports must be resolved at the host.
    pub fn live_environment_process(&self) -> Option<&ProcessInstanceId> {
        self.live_environment_process.as_ref()
    }

    /// Returns private variables to an explicitly authorized execution adapter.
    pub const fn environment(&self) -> &BTreeMap<String, String> {
        &self.environment
    }

    /// Returns the resolved absolute working directory when configured.
    pub fn working_directory(&self) -> Option<&str> {
        self.working_directory.as_deref()
    }
}

impl Debug for ResolvedExecutionContext {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResolvedExecutionContext")
            .field("environment_entries", &self.environment.len())
            .field(
                "environment_bytes",
                &self.environment.iter().fold(0usize, |total, (key, value)| {
                    total.saturating_add(key.len()).saturating_add(value.len())
                }),
            )
            .field("working_directory_set", &self.working_directory.is_some())
            .finish_non_exhaustive()
    }
}

/// Resolves adapter-owned path and environment references for one launch spec.
pub trait ExecutionContextResolver: Send + Sync {
    /// Returns only capture time for an exact process; never environment values.
    fn shell_environment_observed_at<'a>(
        &'a self,
        _session: &'a SessionId,
        _process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<Option<TimestampMillis>, PortError>> {
        Box::pin(async { Ok(None) })
    }

    /// Resolves only launch cwd when a live environment will be used.
    fn resolve_directory<'a>(
        &'a self,
        launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<ResolvedExecutionContext, PortError>> {
        Box::pin(async move {
            let context = self.resolve(launch).await?;
            Ok(ResolvedExecutionContext::new(
                BTreeMap::new(),
                context.working_directory().map(str::to_owned),
            ))
        })
    }

    /// Resolves a bounded private context without changing process state.
    ///
    /// Implementations may read an environment file. They must preserve the
    /// same merge and validation semantics used by managed process launch and
    /// must not expose values in errors.
    ///
    /// # Errors
    ///
    /// Returns a typed, payload-safe adapter failure.
    fn resolve<'a>(
        &'a self,
        launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<ResolvedExecutionContext, PortError>>;
}

/// Payload-free readiness notification supplied by an outer stream pump.
///
/// Application and adapter subscriptions invoke this edge port only when a
/// previously clean checkpoint becomes dirty. Implementations must return
/// promptly and must not inspect private stream payloads.
pub trait SubscriptionWake: Send + Sync {
    /// Signals that one registered subscription may now produce bounded work.
    fn wake(&self);
}

/// Stable adapter failure kinds callers can make decisions from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PortErrorKind {
    /// Dependency is temporarily unavailable.
    Unavailable,
    /// Operation exceeded its adapter-enforced deadline.
    Timeout,
    /// Concurrent or unique state conflicts with the operation.
    Conflict,
    /// Requested adapter-owned resource is absent.
    NotFound,
    /// Caller lacks permission at the adapter boundary.
    PermissionDenied,
    /// Persisted or external data violates its contract.
    InvalidData,
    /// A configured infrastructure limit was reached.
    CapacityExceeded,
    /// A configured adapter request or byte rate was exceeded.
    RateLimited,
    /// Operation was explicitly cancelled by its caller.
    Cancelled,
    /// Operation was interrupted before a normal terminal result.
    Interrupted,
    /// Requested replay is outside adapter retention.
    ReplayUnavailable,
    /// A peer violated its negotiated protocol contract.
    ProtocolViolation,
    /// The concrete adapter does not support the requested behavior.
    Unsupported,
    /// Adapter failed for another infrastructure reason.
    Other,
}

impl PortErrorKind {
    /// Complete adapter failure-kind registry in stable declaration order.
    pub const ALL: [Self; 14] = [
        Self::Unavailable,
        Self::Timeout,
        Self::Conflict,
        Self::NotFound,
        Self::PermissionDenied,
        Self::InvalidData,
        Self::CapacityExceeded,
        Self::RateLimited,
        Self::Cancelled,
        Self::Interrupted,
        Self::ReplayUnavailable,
        Self::ProtocolViolation,
        Self::Unsupported,
        Self::Other,
    ];

    /// Returns the stable application category without adapter source details.
    pub const fn application_category(self) -> ApplicationErrorCategory {
        match self {
            Self::Unavailable => ApplicationErrorCategory::Unavailable,
            Self::Timeout => ApplicationErrorCategory::Timeout,
            Self::Conflict => ApplicationErrorCategory::Conflict,
            Self::NotFound => ApplicationErrorCategory::NotFound,
            Self::PermissionDenied => ApplicationErrorCategory::PermissionDenied,
            Self::InvalidData => ApplicationErrorCategory::InvariantViolation,
            Self::CapacityExceeded => ApplicationErrorCategory::CapacityExceeded,
            Self::RateLimited => ApplicationErrorCategory::RateLimited,
            Self::Cancelled => ApplicationErrorCategory::Cancelled,
            Self::Interrupted => ApplicationErrorCategory::Interrupted,
            Self::ReplayUnavailable => ApplicationErrorCategory::ReplayUnavailable,
            Self::ProtocolViolation => ApplicationErrorCategory::ProtocolViolation,
            Self::Unsupported => ApplicationErrorCategory::Unsupported,
            Self::Other => ApplicationErrorCategory::Internal,
        }
    }

    /// Returns stable snake-case failure kind used by diagnostics and envelopes.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unavailable => "unavailable",
            Self::Timeout => "timeout",
            Self::Conflict => "conflict",
            Self::NotFound => "not_found",
            Self::PermissionDenied => "permission_denied",
            Self::InvalidData => "invalid_data",
            Self::CapacityExceeded => "capacity_exceeded",
            Self::RateLimited => "rate_limited",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
            Self::ReplayUnavailable => "replay_unavailable",
            Self::ProtocolViolation => "protocol_violation",
            Self::Unsupported => "unsupported",
            Self::Other => "other",
        }
    }
}

/// Infrastructure failure retaining its concrete source without exposing it in
/// user-facing display or debug output.
pub struct PortError {
    kind: PortErrorKind,
    operation: PortOperation,
    session_id: Option<SessionId>,
    attempt_id: Option<undercurrent_domain::OperationAttemptId>,
    effect_certainty: Option<undercurrent_domain::EffectCertainty>,
    source: Box<dyn Error + Send + Sync>,
}

impl PortError {
    /// Constructs a typed adapter failure with safe operation context.
    ///
    /// Source text remains available through [`Error::source`] for a redacting
    /// telemetry adapter, but is intentionally absent from `Display` and
    /// `Debug`. This function performs no IO and does not panic.
    pub fn new(
        kind: PortErrorKind,
        operation: PortOperation,
        session_id: Option<SessionId>,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            operation,
            session_id,
            attempt_id: None,
            effect_certainty: None,
            source: Box::new(source),
        }
    }

    /// Adds durable attempt and side-effect evidence without exposing payloads.
    pub fn with_attempt_effect(
        mut self,
        attempt_id: undercurrent_domain::OperationAttemptId,
        certainty: undercurrent_domain::EffectCertainty,
    ) -> Self {
        self.attempt_id = Some(attempt_id);
        self.effect_certainty = Some(certainty);
        self
    }

    /// Records a definite refusal before this port operation applied an effect.
    /// This does not prove absence of effects from earlier command stages; an
    /// application boundary must check the complete command before exposing it.
    pub fn with_known_absent_effect(mut self) -> Self {
        self.effect_certainty = Some(undercurrent_domain::EffectCertainty::KnownAbsent);
        self
    }

    /// Returns typed infrastructure failure kind.
    pub const fn kind(&self) -> PortErrorKind {
        self.kind
    }

    /// Returns the stable application category for outer error envelopes.
    pub const fn category(&self) -> ApplicationErrorCategory {
        self.kind.application_category()
    }

    /// Returns failed stable operation.
    pub const fn operation(&self) -> PortOperation {
        self.operation
    }

    /// Returns safe stable session identity when available.
    pub const fn session_id(&self) -> Option<&SessionId> {
        self.session_id.as_ref()
    }

    /// Returns the durable operation-attempt identity when applicable.
    pub const fn attempt_id(&self) -> Option<&undercurrent_domain::OperationAttemptId> {
        self.attempt_id.as_ref()
    }

    /// Returns exact external-effect certainty when applicable.
    pub const fn effect_certainty(&self) -> Option<undercurrent_domain::EffectCertainty> {
        self.effect_certainty
    }
}

impl Display for PortError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} failed with {}",
            self.operation.as_str(),
            self.kind.as_str()
        )?;
        if let Some(session_id) = &self.session_id {
            write!(formatter, " for session {session_id}")?;
        }
        Ok(())
    }
}

impl Debug for PortError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PortError")
            .field("kind", &self.kind)
            .field("operation", &self.operation)
            .field("session_id", &self.session_id)
            .finish_non_exhaustive()
    }
}

impl Error for PortError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// Durable domain event written atomically with a session revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionOutboxEvent {
    /// Stable event kind.
    pub kind: DomainEventKind,
    /// Stable owning session.
    pub session_id: SessionId,
    /// Durable session revision represented by the event.
    pub revision: Revision,
    /// Explicit application-clock event time.
    pub occurred_at: TimestampMillis,
}

/// Result of atomically inserting a new session and its initial outbox event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertSessionOutcome {
    /// New record and event were committed.
    Inserted,
    /// Stable ID already belongs to a durable record.
    IdConflict,
    /// Human-facing name already resolves to another stable ID.
    NameConflict {
        /// Existing stable session identity.
        existing_session_id: SessionId,
    },
}

/// Result of an optimistic revision-checked session commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitSessionOutcome {
    /// Session and outbox events were committed atomically.
    Committed,
    /// Durable record no longer has the expected prior revision.
    RevisionConflict {
        /// Current durable revision observed by the repository.
        current_revision: Revision,
    },
    /// A revised human-facing name belongs to another stable session.
    NameConflict {
        /// Existing stable session identity.
        existing_session_id: SessionId,
    },
    /// Session was removed before the commit.
    NotFound,
}

/// Result of atomically deleting a session and writing its removal event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteSessionOutcome {
    /// Durable session was deleted and its removal event was committed.
    Deleted,
    /// Durable record no longer has the expected prior revision.
    RevisionConflict {
        /// Current durable revision observed by the repository.
        current_revision: Revision,
    },
    /// Session was already absent.
    NotFound,
}

/// One process instance created by the runtime adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessInstance {
    /// Opaque process-instance identity.
    pub id: ProcessInstanceId,
    /// Time at which the adapter established the process as running.
    pub started_at: TimestampMillis,
}

impl ProcessInstance {
    /// Returns whether this live binding exactly matches durable process
    /// incarnation identity and start time.
    ///
    /// This function performs no IO and does not panic.
    pub fn matches(&self, durable: &ProcessIdentity) -> bool {
        self.id == *durable.id() && self.started_at == durable.started_at()
    }
}

/// Outcome of atomically removing one exact supervised process binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessUnbindOutcome {
    /// The requested concrete process binding was removed.
    Unbound(ProcessInstance),
    /// No process was bound to the session.
    Missing,
    /// A different process instance currently owns the session binding.
    ProcessMismatch,
}

/// Durable delayed work item for one automatic restart attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestartJob {
    /// Stable session whose process should be restarted.
    pub session_id: SessionId,
    /// Revision containing the matching durable scheduled restart status.
    pub expected_revision: Revision,
    /// One-based consecutive automatic attempt represented by this job.
    pub attempt: u32,
    /// Earliest time at which a worker may claim the attempt.
    pub not_before: TimestampMillis,
}

/// One bounded stable-ID-ordered page used only during startup recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverySessionBatch {
    /// Complete durable aggregates in strictly increasing stable-ID order.
    pub sessions: Vec<Session>,
    /// Exclusive cursor for the next page, absent when the scan is complete.
    pub next_after: Option<SessionId>,
}

/// One exit observation tied to the exact process instance that produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessExitObservation {
    /// Stable owning session.
    pub session_id: SessionId,
    /// Exact process instance observed by the runtime adapter.
    pub process_id: ProcessInstanceId,
    /// Typed exit or supervision outcome.
    pub outcome: ProcessOutcome,
    /// Time at which the runtime established the outcome.
    pub observed_at: TimestampMillis,
}

/// How a process runtime completed a requested termination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TerminationKind {
    /// Graceful signal was delivered, but its deadline elapsed without confirmed exit.
    GracefulTimedOut,
    /// Configured graceful signal completed before its deadline.
    Graceful,
    /// Graceful termination exceeded its deadline and was escalated.
    ForcedAfterTimeout,
    /// Caller explicitly requested immediate forced termination.
    Forced,
}

/// Termination attempt returned by the runtime adapter; GracefulTimedOut retains ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessTermination {
    kind: TerminationKind,
    exit_code: Option<i32>,
    observed_at: TimestampMillis,
}

/// Idempotent standard-input closure returned by a process adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StandardInputClosure {
    /// This call closed the input stream after all previously accepted writes.
    Closed,
    /// The exact process input stream was already closed.
    AlreadyClosed,
}

/// Result of atomically reserving one semantic terminal-input delivery ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputDeliveryReservation {
    /// ID and exact semantic input were newly reserved before PTY IO.
    Reserved,
    /// A matching prior delivery completed successfully.
    AlreadyDelivered {
        /// Time retained with the completed result.
        delivered_at: TimestampMillis,
    },
    /// A matching reservation has an ambiguous outcome and must not be retried.
    Indeterminate,
    /// The same ID was previously reserved for different semantic input.
    PayloadConflict,
}

/// Terminal-input result durably associated with a reserved delivery ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputDeliveryDisposition {
    /// The complete atomic sequence was accepted by the PTY adapter.
    Delivered {
        /// Time at which application delivery completed.
        delivered_at: TimestampMillis,
    },
    /// Adapter failure left it unsafe to assume that no bytes were delivered.
    Indeterminate {
        /// Time at which ambiguity was recorded.
        observed_at: TimestampMillis,
    },
}

impl ProcessTermination {
    /// Constructs a termination observation, including a graceful timeout.
    ///
    /// For a completed termination kind, `None` represents a confirmed stop
    /// whose portable exit code is unavailable. `GracefulTimedOut` does not
    /// confirm termination; callers must check the kind before using
    /// [`Self::lifecycle`]. This function performs no IO, cannot fail, and does
    /// not panic.
    pub const fn new(
        kind: TerminationKind,
        exit_code: Option<i32>,
        observed_at: TimestampMillis,
    ) -> Self {
        Self {
            kind,
            exit_code,
            observed_at,
        }
    }

    /// Returns how termination completed.
    pub const fn kind(self) -> TerminationKind {
        self.kind
    }

    /// Returns the observed operating-system exit code when portable and known.
    pub const fn exit_code(self) -> Option<i32> {
        self.exit_code
    }

    /// Returns when the runtime confirmed termination.
    pub const fn observed_at(self) -> TimestampMillis {
        self.observed_at
    }

    /// Maps confirmed process truth into the authoritative terminal lifecycle.
    pub const fn lifecycle(self) -> LifecycleState {
        match self.exit_code {
            Some(code) => LifecycleState::Exited(code),
            None => LifecycleState::Stopped,
        }
    }
}

/// Supplies explicit time to deterministic application use cases.
pub trait Clock: Send + Sync {
    /// Returns current application time.
    ///
    /// Implementations may read a system or test clock. They should not panic.
    fn now(&self) -> TimestampMillis;
}

/// Supplies stable session identities outside the pure domain.
pub trait SessionIdSource: Send + Sync {
    /// Generates the next stable session ID.
    ///
    /// # Errors
    ///
    /// Returns a typed adapter failure. Implementations may use randomness,
    /// persistent allocation, or another collision-resistant source.
    fn next_id(&self) -> Result<SessionId, PortError>;
}

/// Drop guard retaining exclusive ordered mutation ownership for one session.
///
/// Implementations release their lease when dropped. The marker deliberately
/// exposes no adapter-specific locking primitive to application code.
pub trait SessionMutationLease: Send {}

/// Serializes mutations for one session without coupling independent sessions.
pub trait SessionMutationCoordinator: Send + Sync {
    /// Acquires exclusive mutation ownership for `session_id`.
    ///
    /// Implementations must honor cancellation while waiting and must not hold
    /// a registry-wide lock after the returned lease is acquired.
    fn acquire<'a>(
        &'a self,
        session_id: &'a SessionId,
    ) -> PortFuture<'a, Result<Box<dyn SessionMutationLease>, PortError>>;
}

/// Persists sessions and domain-event outbox records transactionally.
pub trait SessionRepository: Send + Sync {
    /// Atomically inserts a session and initial outbox event.
    fn insert<'a>(
        &'a self,
        session: &'a Session,
        event: &'a SessionOutboxEvent,
    ) -> PortFuture<'a, Result<InsertSessionOutcome, PortError>>;

    /// Loads one durable session by stable ID.
    fn load<'a>(
        &'a self,
        session_id: &'a SessionId,
    ) -> PortFuture<'a, Result<Option<Session>, PortError>>;

    /// Resolves one durable session by current human-facing name.
    fn find_by_name<'a>(
        &'a self,
        name: &'a SessionName,
    ) -> PortFuture<'a, Result<Option<Session>, PortError>>;

    /// Atomically commits a session and associated outbox events when the
    /// durable prior revision matches `expected_revision`.
    fn commit<'a>(
        &'a self,
        session: &'a Session,
        expected_revision: Revision,
        events: &'a [SessionOutboxEvent],
    ) -> PortFuture<'a, Result<CommitSessionOutcome, PortError>>;

    /// Atomically deletes a terminal session and writes its removal event at an
    /// expected prior revision.
    fn delete<'a>(
        &'a self,
        session_id: &'a SessionId,
        expected_revision: Revision,
        event: &'a SessionOutboxEvent,
    ) -> PortFuture<'a, Result<DeleteSessionOutcome, PortError>>;
}

/// Reads and acknowledges transactionally recorded session events.
pub trait SessionOutbox: Send + Sync {
    /// Reads at most `limit` oldest pending records in strict sequence order.
    ///
    /// Implementations must not remove records until a later acknowledgement.
    fn read_pending<'a>(
        &'a self,
        limit: usize,
    ) -> PortFuture<'a, Result<Vec<SessionOutboxRecord>, PortError>>;

    /// Removes the contiguous pending prefix ending at `through`.
    ///
    /// Implementations must make repeated acknowledgement of an already
    /// delivered checkpoint idempotent and reject unknown future checkpoints.
    fn acknowledge_through<'a>(
        &'a self,
        through: OutboxSequence,
    ) -> PortFuture<'a, Result<OutboxAcknowledgeOutcome, PortError>>;
}

/// Publishes ordered durable session events to live and retained consumers.
pub trait SessionEventPublisher: Send + Sync {
    /// Idempotently publishes one exact outbox sequence and event.
    ///
    /// Implementations must return `AlreadyPublished` only when both sequence
    /// and complete typed event match. Reusing a sequence for different content
    /// must return a typed invalid-data or conflict error.
    fn publish<'a>(
        &'a self,
        record: &'a SessionOutboxRecord,
    ) -> PortFuture<'a, Result<SessionEventPublishOutcome, PortError>>;
}

/// Reads bounded filtered retained session-event batches.
pub trait SessionEventReader: Send + Sync {
    /// Reads one coherent journal snapshot according to the supplied start mode.
    ///
    /// Implementations must return an explicit gap when a resume checkpoint is
    /// older than retained history and must not return unrelated records.
    fn read<'a>(
        &'a self,
        request: &'a SessionEventReadRequest,
    ) -> PortFuture<'a, Result<SessionEventReadBatch, PortError>>;
}

/// Reads parser-consistent terminal screen state without exposing a PTY parser.
pub trait TerminalScreenReader: Send + Sync {
    /// Returns the current screen and exact output offset it represents.
    ///
    /// A missing value means the session has no screen state. Implementations
    /// must keep the returned contents, cursor, dimensions, and output offset
    /// coherent. Private screen contents must not enter logs or diagnostics.
    fn snapshot<'a>(
        &'a self,
        session_id: &'a SessionId,
    ) -> PortFuture<'a, Result<Option<TerminalScreenSnapshot>, PortError>>;
}

/// Computes parser-specific terminal updates behind an application boundary.
pub trait TerminalScreenDiffer: Send + Sync {
    /// Produces a bounded update from `previous` to `current`.
    ///
    /// Implementations must return an authoritative snapshot update when the
    /// states cannot be safely diffed, for example after incompatible geometry
    /// or terminal-buffer changes. Private contents must not enter errors,
    /// logs, or diagnostics.
    fn diff<'a>(
        &'a self,
        session_id: &'a SessionId,
        previous: &'a TerminalScreenSnapshot,
        current: &'a TerminalScreenSnapshot,
    ) -> PortFuture<'a, Result<TerminalScreenUpdate, PortError>>;
}

/// Complete terminal-screen adapter required by screen read use cases.
pub trait TerminalScreenPort: TerminalScreenReader + TerminalScreenDiffer {}

impl<T> TerminalScreenPort for T where T: TerminalScreenReader + TerminalScreenDiffer + ?Sized {}

/// Independently owned checkpoint-only event subscription.
pub trait SessionEventSubscriptionPort: Send {
    /// Reads one pending bounded batch without waiting.
    ///
    /// # Errors
    ///
    /// Returns typed closure, replay, or adapter failures.
    fn try_receive(&self) -> Result<Option<SessionEventReadBatch>, PortError>;

    /// Closes this subscription without affecting any peer subscription.
    fn close(&self);
}

/// Coherent initial event replay and registered live continuation.
pub struct OpenSessionEventSubscription {
    initial: SessionEventReadBatch,
    subscription: Box<dyn SessionEventSubscriptionPort>,
}

impl OpenSessionEventSubscription {
    /// Constructs a coherent adapter result after atomic source registration.
    pub fn new(
        initial: SessionEventReadBatch,
        subscription: Box<dyn SessionEventSubscriptionPort>,
    ) -> Self {
        Self {
            initial,
            subscription,
        }
    }

    /// Consumes the result into its initial batch and continuation ownership.
    pub fn into_parts(self) -> (SessionEventReadBatch, Box<dyn SessionEventSubscriptionPort>) {
        (self.initial, self.subscription)
    }
}

/// Opens selective event subscriptions without exposing adapter types.
pub trait SessionEventSubscriber: Send + Sync {
    /// Atomically reads the requested initial batch and registers continuation.
    ///
    /// # Errors
    ///
    /// Returns typed capacity, identity, replay, or adapter failures.
    fn subscribe(
        &self,
        request: &SessionEventReadRequest,
        wake: Arc<dyn SubscriptionWake>,
    ) -> Result<OpenSessionEventSubscription, PortError>;
}

/// Owns concrete operating-system process and PTY side effects.
pub trait ProcessRuntime: Send + Sync {
    /// Resolves filesystem-bound launch inputs and records executable content
    /// before durable acceptance. Sealed mode pins that content for future spawns;
    /// native mode preserves and revalidates the installation path.
    ///
    /// Adapters must return a command carrying an executable resolution receipt
    /// and must reject any configured working directory that escapes or lacks
    /// its supplied environment binding. Already prepared launches may be
    /// returned unchanged after adapter verification.
    fn prepare_launch<'a>(
        &'a self,
        launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<LaunchSpec, PortError>> {
        if launch.command().executable_is_pinned() {
            let prepared = launch.clone();
            return Box::pin(async move { Ok(prepared) });
        }
        let error = PortError::new(
            PortErrorKind::Unsupported,
            PortOperation::PrepareLaunch,
            None,
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "process runtime cannot resolve and pin executable content",
            ),
        );
        Box::pin(async move { Err(error) })
    }

    /// Spawns one process from an immutable validated launch specification.
    fn spawn<'a>(
        &'a self,
        session_id: &'a SessionId,
        launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<ProcessInstance, PortError>>;

    /// Attempt-aware spawn entry point for durable callers.
    fn spawn_attempted<'a>(
        &'a self,
        context: &'a ProcessMutationContext,
        session_id: &'a SessionId,
        launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<ProcessInstance, PortError>> {
        let _ = context;
        self.spawn(session_id, launch)
    }

    /// Coordinates durable admission and fencing for a spawn attempt.
    ///
    /// The process effect and settlement are separate operations. Externally
    /// dispatched success is settled by `StartSessionService` after activation.
    fn spawn_admitted<'a>(
        &'a self,
        admission: &'a crate::OperationAttemptAdmission,
        session_id: &'a SessionId,
        launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<ProcessInstance, PortError>> {
        let _ = (admission, launch);
        unsupported_admitted_process_mutation(PortOperation::SpawnProcess, session_id)
    }

    /// Arms exit publication only after the exact process is present in the
    /// session supervisor directory.
    ///
    /// Implementations must retain a fast pre-arm exit and publish it exactly
    /// once after this call succeeds. Repeating this call for the same exact
    /// process is idempotent.
    fn arm_exit<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<(), PortError>>;

    /// Attempt-aware exit-arm entry point for durable callers.
    fn arm_exit_attempted<'a>(
        &'a self,
        context: &'a ProcessMutationContext,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<(), PortError>> {
        let _ = context;
        self.arm_exit(session_id, process)
    }

    /// Coordinates durable admission and fencing for exit arming.
    ///
    /// The process effect and settlement are separate operations. See
    /// [`DurableProcessRuntime`](crate::DurableProcessRuntime) for settlement ordering.
    fn arm_exit_admitted<'a>(
        &'a self,
        admission: &'a crate::OperationAttemptAdmission,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<(), PortError>> {
        let _ = (admission, process);
        unsupported_admitted_process_mutation(PortOperation::ArmProcessExit, session_id)
    }

    /// Requests graceful termination according to the session stop policy.
    fn stop<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        policy: StopPolicy,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>>;

    /// Attempt-aware graceful stop entry point for durable callers.
    fn stop_attempted<'a>(
        &'a self,
        context: &'a ProcessMutationContext,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        policy: StopPolicy,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        let _ = context;
        self.stop(session_id, process, policy)
    }

    /// Coordinates durable admission and fencing for graceful termination.
    ///
    /// The process effect and settlement are separate operations. See
    /// [`DurableProcessRuntime`](crate::DurableProcessRuntime) for settlement ordering.
    fn stop_admitted<'a>(
        &'a self,
        admission: &'a crate::OperationAttemptAdmission,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        policy: StopPolicy,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        let _ = (admission, process, policy);
        unsupported_admitted_process_mutation(PortOperation::StopProcess, session_id)
    }

    /// Forces termination during explicit force-stop or compensation.
    fn force_stop<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>>;

    /// Attempt-aware forced stop entry point for durable callers.
    fn force_stop_attempted<'a>(
        &'a self,
        context: &'a ProcessMutationContext,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        let _ = context;
        self.force_stop(session_id, process)
    }

    /// Coordinates durable admission and fencing for forced termination.
    ///
    /// The process effect and settlement are separate operations. See
    /// [`DurableProcessRuntime`](crate::DurableProcessRuntime) for settlement ordering.
    fn force_stop_admitted<'a>(
        &'a self,
        admission: &'a crate::OperationAttemptAdmission,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        let _ = (admission, process);
        unsupported_admitted_process_mutation(PortOperation::ForceProcess, session_id)
    }

    /// Writes one validated sequence atomically relative to other input for the
    /// same session, including any profile-owned internal pacing.
    fn write_input<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        input: &'a InputSequence,
        policy: InputDeliveryPolicy,
    ) -> PortFuture<'a, Result<(), PortError>>;

    /// Attempt-aware terminal input entry point for durable callers.
    fn write_input_attempted<'a>(
        &'a self,
        context: &'a ProcessMutationContext,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        input: &'a InputSequence,
        policy: InputDeliveryPolicy,
    ) -> PortFuture<'a, Result<(), PortError>> {
        let _ = context;
        self.write_input(session_id, process, input, policy)
    }

    /// Coordinates durable admission and fencing for terminal input.
    ///
    /// The process effect and settlement are separate operations. See
    /// [`DurableProcessRuntime`](crate::DurableProcessRuntime) for settlement ordering.
    fn write_input_admitted<'a>(
        &'a self,
        admission: &'a crate::OperationAttemptAdmission,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        input: &'a InputSequence,
        policy: InputDeliveryPolicy,
    ) -> PortFuture<'a, Result<(), PortError>> {
        let _ = (admission, process, input, policy);
        unsupported_admitted_process_mutation(PortOperation::WriteInput, session_id)
    }

    /// Closes standard input after every previously accepted write for the exact process.
    ///
    /// Repeating the call after closure must succeed with
    /// [`StandardInputClosure::AlreadyClosed`].
    fn close_input<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<StandardInputClosure, PortError>> {
        let _ = process;
        unsupported_admitted_process_mutation(PortOperation::CloseInput, session_id)
    }

    /// Resizes the PTY owned by one concrete process instance.
    fn resize<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        size: TerminalSize,
    ) -> PortFuture<'a, Result<(), PortError>>;

    /// Attempt-aware PTY resize entry point for durable callers.
    fn resize_attempted<'a>(
        &'a self,
        context: &'a ProcessMutationContext,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        size: TerminalSize,
    ) -> PortFuture<'a, Result<(), PortError>> {
        let _ = context;
        self.resize(session_id, process, size)
    }

    /// Coordinates durable admission and fencing for a PTY resize.
    ///
    /// The process effect and settlement are separate operations. See
    /// [`DurableProcessRuntime`](crate::DurableProcessRuntime) for settlement ordering.
    fn resize_admitted<'a>(
        &'a self,
        admission: &'a crate::OperationAttemptAdmission,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        size: TerminalSize,
    ) -> PortFuture<'a, Result<(), PortError>> {
        let _ = (admission, process, size);
        unsupported_admitted_process_mutation(PortOperation::ResizeTerminal, session_id)
    }

    /// Releases a retained process-effect receipt after product settlement commits.
    ///
    /// Stateful adapters must make this idempotent. Unknown effects are never
    /// acknowledged by the durable application wrapper.
    fn acknowledge_attempt<'a>(
        &'a self,
        attempt: &'a undercurrent_domain::OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        let _ = attempt;
        Box::pin(async { Ok(()) })
    }
}

fn unsupported_admitted_process_mutation<'a, T: Send + 'a>(
    operation: PortOperation,
    session_id: &'a SessionId,
) -> PortFuture<'a, Result<T, PortError>> {
    let error = PortError::new(
        PortErrorKind::Unsupported,
        operation,
        Some(session_id.clone()),
        std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "process runtime is not wrapped by durable attempt coordination",
        ),
    );
    Box::pin(async move { Err(error) })
}

/// Supplies armed process exits to the application dispatcher.
pub trait ProcessExitSource: Send + Sync {
    /// Waits for the next exact process exit retained by the runtime.
    ///
    /// Cancellation before completion must leave the observation available to
    /// another receiver. Implementations must never publish an exit before
    /// [`ProcessRuntime::arm_exit`] succeeds for that exact incarnation.
    fn next_exit(&self) -> PortFuture<'_, Result<ProcessExitObservation, PortError>>;
}

/// Collects concrete signals and translates them into typed foreground evidence.
///
/// Implementations may inspect PTY, screen, and process state, but must reuse a
/// shared process snapshot rather than refreshing the operating-system process
/// table independently for every session.
pub trait ForegroundEvidenceCollector: Send + Sync {
    /// Collects one bounded evidence cycle for a running process instance.
    ///
    /// This port does not decide committed foreground state. The domain policy
    /// validates freshness, confidence, and precedence after collection.
    fn collect<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        profile: Option<&'a ProfileName>,
        current: &'a ForegroundObservation,
    ) -> PortFuture<'a, Result<Vec<ForegroundEvidence>, PortError>>;
}

/// Executes independently scheduled configured health rules.
pub trait HealthCheckExecutor: Send + Sync {
    /// Returns bounded per-rule results for one running process instance.
    ///
    /// Expected check failures are represented by `passed = false`; this port
    /// returns an error only when infrastructure prevented evaluation.
    fn run<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        rules: &'a HealthRules,
    ) -> PortFuture<'a, Result<Vec<HealthCheckResult>, PortError>>;
}

/// Persists bounded semantic input-delivery reservations and results.
///
/// Implementations compare the supplied [`InputSequence`] and foreground policy
/// exactly without exposing private input through logs or diagnostics. Records
/// must have configured retention and capacity limits.
///
/// The daemon uses SQLite and retains records for the owning session's lifetime:
/// no TTL or capacity eviction, and terminal session deletion removes them.
/// Private typed digests replace raw input in durable comparison records.
pub trait InputDeliveryStore: Send + Sync {
    /// Atomically reserves a delivery ID for one exact semantic input sequence.
    fn reserve<'a>(
        &'a self,
        session_id: &'a SessionId,
        delivery_id: &'a DeliveryId,
        input: &'a InputSequence,
        observation_policy: InputObservationPolicy,
    ) -> PortFuture<'a, Result<InputDeliveryReservation, PortError>>;

    /// Idempotently settles a previously reserved delivery result.
    fn settle<'a>(
        &'a self,
        session_id: &'a SessionId,
        delivery_id: &'a DeliveryId,
        disposition: InputDeliveryDisposition,
    ) -> PortFuture<'a, Result<(), PortError>>;
}

/// Updates rebuildable prepared read models after durable session changes.
pub trait SessionProjectionWriter: Send + Sync {
    /// Inserts or replaces projections derived from one complete aggregate.
    ///
    /// Implementations must retain only the fields required by each prepared
    /// view and must not expose private aggregate content through summaries.
    fn upsert_session(&self, session: &Session) -> Result<(), PortError>;

    /// Removes all prepared views owned by one session.
    fn remove_summary(&self, session_id: &SessionId) -> Result<(), PortError>;
}

/// Owns process-local effective profile state in the authoritative prepared catalog.
pub trait EffectiveProfileDirectory: Send + Sync {
    /// Reads the effective profile only when the prepared session still owns the
    /// supplied exact process incarnation.
    fn get(
        &self,
        session_id: &SessionId,
        process: &ProcessInstanceId,
    ) -> Result<Option<EffectiveProfileSnapshot>, PortError>;

    /// Applies one adaptive transition without mutating durable session state.
    ///
    /// Implementations must reindex profile queries and notify selective
    /// watchers atomically with the transition.
    fn transition(
        &self,
        session_id: &SessionId,
        transition: EffectiveProfileTransition,
    ) -> Result<Option<EffectiveProfileSnapshot>, PortError>;
}

/// Owns the live association between a session supervisor and process instance.
pub trait SessionProcessDirectory: Send + Sync {
    /// Snapshots owned session IDs for shutdown after new mutation admission closes.
    /// The directory's existing process bound limits the returned allocation.
    fn session_ids(&self) -> Result<Vec<SessionId>, PortError> {
        Err(PortError::new(
            PortErrorKind::Unsupported,
            PortOperation::LookupProcess,
            None,
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "process directory cannot enumerate bindings",
            ),
        ))
    }

    /// Binds a newly spawned process, rejecting an existing live binding.
    fn bind(&self, session_id: &SessionId, process: ProcessInstance) -> Result<(), PortError>;

    /// Returns the current process binding, if one exists.
    fn lookup(&self, session_id: &SessionId) -> Result<Option<ProcessInstance>, PortError>;

    /// Atomically removes a binding only when `process` still owns it.
    ///
    /// Implementations must perform comparison and removal under the same
    /// synchronization boundary so a stale observer cannot remove a replacement.
    fn unbind_if(
        &self,
        session_id: &SessionId,
        process: &ProcessInstanceId,
    ) -> Result<ProcessUnbindOutcome, PortError>;
}

/// Queues rebuildable automatic restart work backed by durable aggregate intent.
pub trait RestartScheduler: Send + Sync {
    /// Idempotently schedules one revision-checked restart job.
    ///
    /// The caller must commit scheduled intent to the aggregate before enqueueing.
    /// Acceptance covers this process-local queue; daemon startup must reconstruct
    /// committed jobs before readiness, including an enqueue failure or lost reply.
    /// Implementations deduplicate `(session_id, expected_revision, attempt)`.
    /// Workers revalidate every field against durable intent before process IO.
    fn schedule<'a>(&'a self, job: &'a RestartJob) -> PortFuture<'a, Result<(), PortError>>;

    /// Removes every pending automatic restart for one session.
    ///
    /// Implementations must make repeated cancellation idempotent and return
    /// the number of jobs removed. Work must be proportional to the target
    /// session's jobs rather than all queued restart work.
    fn cancel_session_restarts<'a>(
        &'a self,
        session_id: &'a SessionId,
    ) -> PortFuture<'a, Result<usize, PortError>>;
}

/// Supplies due revision-checked automatic restart jobs to bounded workers.
pub trait DueRestartSource: Send + Sync {
    /// Claims at most `limit` jobs due at or before `now` in stable deadline,
    /// session, revision, and attempt order.
    ///
    /// Claimed jobs are removed from the source. Each job remains safe to
    /// execute once because the application use case revalidates its complete
    /// durable restart disposition before process IO.
    fn claim_due_restarts(
        &self,
        now: TimestampMillis,
        limit: usize,
    ) -> Result<Vec<RestartJob>, PortError>;
}

/// Enqueues revision-aware per-session observation work without global scans.
pub trait ObservationScheduler: Send + Sync {
    /// Inserts or updates one `(session, kind)` deadline idempotently.
    ///
    /// Implementations must not let an older revision or later same-revision
    /// deadline displace newer or earlier work.
    fn schedule_observation<'a>(
        &'a self,
        job: &'a ObservationJob,
    ) -> PortFuture<'a, Result<ObservationScheduleOutcome, PortError>>;

    /// Removes all pending observation kinds for one deleted session.
    fn cancel_session_observations<'a>(
        &'a self,
        session_id: &'a SessionId,
    ) -> PortFuture<'a, Result<usize, PortError>>;

    /// Atomically applies the complete desired schedule for one session.
    ///
    /// Implementations must preserve earlier same-revision work, must not let
    /// an older plan replace or remove newer work, and must reject capacity
    /// before partially applying the plan.
    fn reconcile_session_observations<'a>(
        &'a self,
        plan: &'a crate::SessionObservationPlan,
    ) -> PortFuture<'a, Result<crate::ObservationReconcileOutcome, PortError>>;
}

/// Pulls one active foreground observation deadline forward after runtime evidence changes.
pub trait ForegroundObservationWake: Send + Sync {
    /// Schedules the active foreground plan at `observed_at` without changing its revision.
    ///
    /// `None` means the session has no active foreground observation plan. Implementations
    /// must coalesce repeated wakes through the same bounded `(session, kind)` key.
    fn wake_foreground(
        &self,
        session_id: &SessionId,
        observed_at: TimestampMillis,
    ) -> Result<Option<ObservationScheduleOutcome>, PortError>;
}

/// Supplies due session-local observation jobs to bounded workers.
pub trait DueObservationSource: Send + Sync {
    /// Claims at most `limit` jobs due at or before `now` in stable deadline
    /// order. Claimed jobs are removed and must be explicitly rescheduled.
    fn claim_due_observations(
        &self,
        now: TimestampMillis,
        limit: usize,
    ) -> Result<Vec<ObservationJob>, PortError>;
}

/// Supplies bounded durable session pages for daemon-start reconciliation.
pub trait SessionRecoverySource: Send + Sync {
    /// Reads at most `limit` complete aggregates after the exclusive stable-ID
    /// cursor.
    ///
    /// Implementations must return strictly increasing IDs, no duplicates, and
    /// a cursor that advances. They must not hydrate the entire catalog first.
    fn read_batch<'a>(
        &'a self,
        after: Option<&'a SessionId>,
        limit: usize,
    ) -> PortFuture<'a, Result<RecoverySessionBatch, PortError>>;

    /// Claims the durable runtime-cancellation recovery instruction for one
    /// stopping session at the process-effect boundary.
    ///
    /// `None` means the stopping lifecycle came from the standalone session
    /// command path. Implementations that also own cancellation records must
    /// override this method so returning `Resume` atomically marks the exact
    /// accepted control in flight before process IO, preserves an admitted
    /// force decision, and never redrives an Unknown process effect.
    fn claim_runtime_cancellation_recovery<'a>(
        &'a self,
        _session_id: &'a SessionId,
        _observed_at: TimestampMillis,
    ) -> PortFuture<'a, Result<Option<crate::RuntimeCancellationRecovery>, PortError>> {
        Box::pin(async { Ok(None) })
    }
}

/// Resolves authoritative OS truth for a durable process incarnation.
pub trait ProcessTruthSource: Send + Sync {
    /// Observes one exact process identity without relying on PID alone.
    ///
    /// Implementations must distinguish PID reuse using the opaque incarnation
    /// identity and should serve calls from a shared process snapshot rather
    /// than refreshing the OS process table independently for every session.
    fn observe<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessIdentity,
    ) -> PortFuture<'a, Result<undercurrent_domain::ProcessTruth, PortError>>;
}

/// Observes bounded exact-process foreground program facts for adaptive profiles.
pub trait ForegroundProcessObserver: Send + Sync {
    /// Observes the kernel foreground process group for one exact managed process.
    ///
    /// Implementations must reject stale process identities and share expensive
    /// operating-system snapshots across sessions.
    fn observe_foreground<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<ForegroundProcessGroupObservation, PortError>>;
}

/// Supplies the immutable active detection-profile catalog.
///
/// Implementations may reload configuration atomically, but each individual
/// method must observe one coherent catalog generation. User definitions may
/// override builtins by name before they cross this boundary.
pub trait DetectionProfileCatalog: Send + Sync {
    /// Returns at most `limit` summaries in strictly increasing name order.
    ///
    /// Implementations must return a capacity error rather than a truncated
    /// result when the active catalog contains more than `limit` profiles.
    fn list(&self, limit: usize) -> Result<Vec<ProfileSummary>, PortError>;

    /// Returns one complete active entry by stable profile name.
    fn get(&self, name: &ProfileName) -> Result<Option<ProfileCatalogEntry>, PortError>;

    /// Returns every active entry needed for deterministic ambiguity detection.
    ///
    /// Implementations must return a capacity error rather than silently omit
    /// candidates when the catalog exceeds `limit`.
    fn all(&self, limit: usize) -> Result<Vec<ProfileCatalogEntry>, PortError>;
}

/// Compiles and evaluates profile regular expressions outside the core.
///
/// Implementations should cache compiled expressions by exact value and impose
/// their own execution/resource limits. Invalid syntax is expected profile
/// input and is returned as [`ProfilePatternError`], not infrastructure failure.
pub trait ProfilePatternEngine: Send + Sync {
    /// Validates one expression without evaluating private text.
    fn validate(&self, expression: &PatternExpression) -> Result<(), ProfilePatternError>;

    /// Tests one expression against bounded adapter-supplied text.
    fn is_match(
        &self,
        expression: &PatternExpression,
        text: &str,
    ) -> Result<bool, ProfilePatternError>;
}

/// Receives structured diagnostics and metrics without influencing decisions.
pub trait Telemetry: Send + Sync {
    /// Emits a payload-safe diagnostic. Implementations must not panic.
    fn emit_diagnostic(&self, record: &DiagnosticRecord);

    /// Records a stable numeric metric. Implementations must not panic.
    fn record_metric(&self, observation: MetricObservation);

    /// Records a numeric metric with one safe exact session identity.
    ///
    /// Implementations must retain or export the typed identity and must not
    /// attach terminal bytes or arbitrary labels.
    fn record_session_metric(&self, observation: &SessionMetricObservation);
}
