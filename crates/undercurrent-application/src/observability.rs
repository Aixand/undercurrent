// SPDX-License-Identifier: Apache-2.0

//! Central observability vocabulary and payload-safe diagnostic records.

use undercurrent_domain::{SessionId, TimestampMillis};

use crate::{ApplicationErrorCategory, PortErrorKind, PortOperation};

/// Namespaced protocol prefix for Undercurrent application observability.
pub const UNDERCURRENT_OBSERVABILITY_NAMESPACE: &str = "undercurrent";

/// Stable application use-case names used by metrics and diagnostics.
///
/// These describe user-visible operations and remain separate from
/// [`PortOperation`], which identifies concrete infrastructure boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ApplicationOperation {
    /// Create and start one managed session.
    StartSession,
    /// Gracefully or forcibly stop one managed session.
    StopSession,
    /// Restart one managed session.
    RestartSession,
    /// Rename one durable session.
    RenameSession,
    /// Patch query-visible labels without changing the managed process.
    UpdateSessionLabels,
    /// Remove one terminal durable session.
    RemoveSession,
    /// Send one atomic terminal input sequence.
    SendTerminalInput,
    /// Close one managed process's standard-input stream.
    CloseStandardInput,
    /// Resize one running terminal.
    ResizeTerminal,
    /// Read one bounded prepared summary page.
    ListSessions,
    /// Load one authorized detailed session view.
    GetSessionDetail,
    /// Resolve private launch context for authorized one-shot execution.
    ResolveExecutionContext,
    /// Execute one bounded command under daemon supervision.
    ExecuteInSession,
    /// Repairs one Unknown attempt from retained trusted evidence.
    RepairOperationAttempt,
    /// Reads one exact caller-bound runtime attempt without mutation.
    GetRuntimeAttemptStatus,
    /// Durably cancels one exact attached runtime attempt.
    CancelRuntimeAttempt,
    /// Reads one exact retained runtime-cancellation control.
    GetRuntimeCancellationStatus,
    /// Read one authorized bounded retained terminal-output page.
    ReadTerminalOutput,
    /// Read one authorized current terminal screen.
    ReadTerminalScreen,
    /// Read one authorized bounded terminal screen update.
    ReadTerminalScreenDiff,
    /// Open one bounded query-window watch.
    OpenSessionWatch,
    /// Refresh one bounded query-window watch.
    RefreshSessionWatch,
    /// Insert or replace one declared WorkContext item.
    UpsertWorkContext,
    /// Remove one declared WorkContext item.
    RemoveWorkContext,
    /// Expire eligible WorkContext declarations.
    ExpireWorkContext,
    /// Reconcile durable intent with observed process truth.
    ReconcileSession,
    /// Collect, decide, and commit best-effort foreground evidence.
    ObserveForeground,
    /// Execute and commit one independent health evaluation.
    ObserveHealth,
    /// Claim and dispatch one bounded batch of due observations.
    DispatchObservations,
    /// Publish and acknowledge one bounded transactional outbox batch.
    DispatchOutbox,
    /// Settle one concrete supervised-process exit observation.
    ObserveProcessExit,
    /// Execute one due revision-checked automatic restart job.
    RunScheduledRestart,
    /// Reconcile bounded persisted sessions with OS truth during daemon start.
    ReconcileStartup,
    /// Resolve durable unknown-effect operation attempts before readiness.
    ReconcileOperationAttempts,
    /// List bounded active detection-profile summaries.
    ListDetectionProfiles,
    /// Inspect one complete active detection profile.
    InspectDetectionProfile,
    /// Match explicit command facts against active detection profiles.
    MatchDetectionProfile,
    /// Lint one active detection profile.
    LintDetectionProfile,
    /// Test one active detection profile against explicit screen views.
    TestDetectionProfile,
    /// Read one authorized bounded filtered session-event batch.
    ReadSessionEvents,
}

impl ApplicationOperation {
    /// Complete application-operation registry in stable declaration order.
    pub const ALL: [Self; 40] = [
        Self::StartSession,
        Self::StopSession,
        Self::RestartSession,
        Self::RenameSession,
        Self::UpdateSessionLabels,
        Self::RemoveSession,
        Self::SendTerminalInput,
        Self::CloseStandardInput,
        Self::ResizeTerminal,
        Self::ListSessions,
        Self::GetSessionDetail,
        Self::ResolveExecutionContext,
        Self::ExecuteInSession,
        Self::RepairOperationAttempt,
        Self::GetRuntimeAttemptStatus,
        Self::CancelRuntimeAttempt,
        Self::GetRuntimeCancellationStatus,
        Self::ReadTerminalOutput,
        Self::ReadTerminalScreen,
        Self::ReadTerminalScreenDiff,
        Self::OpenSessionWatch,
        Self::RefreshSessionWatch,
        Self::UpsertWorkContext,
        Self::RemoveWorkContext,
        Self::ExpireWorkContext,
        Self::ReconcileSession,
        Self::ObserveForeground,
        Self::ObserveHealth,
        Self::DispatchObservations,
        Self::DispatchOutbox,
        Self::ObserveProcessExit,
        Self::RunScheduledRestart,
        Self::ReconcileStartup,
        Self::ReconcileOperationAttempts,
        Self::ListDetectionProfiles,
        Self::InspectDetectionProfile,
        Self::MatchDetectionProfile,
        Self::LintDetectionProfile,
        Self::TestDetectionProfile,
        Self::ReadSessionEvents,
    ];

    /// Returns stable snake-case name used at adapter boundaries.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StartSession => "start_session",
            Self::StopSession => "stop_session",
            Self::RestartSession => "restart_session",
            Self::RenameSession => "rename_session",
            Self::UpdateSessionLabels => "update_session_labels",
            Self::RemoveSession => "remove_session",
            Self::SendTerminalInput => "send_terminal_input",
            Self::CloseStandardInput => "close_standard_input",
            Self::ResizeTerminal => "resize_terminal",
            Self::ListSessions => "list_sessions",
            Self::GetSessionDetail => "get_session_detail",
            Self::ResolveExecutionContext => "resolve_execution_context",
            Self::ExecuteInSession => "execute_in_session",
            Self::RepairOperationAttempt => "repair_operation_attempt",
            Self::GetRuntimeAttemptStatus => "get_runtime_attempt_status",
            Self::CancelRuntimeAttempt => "cancel_runtime_attempt",
            Self::GetRuntimeCancellationStatus => "get_runtime_cancellation_status",
            Self::ReadTerminalOutput => "read_terminal_output",
            Self::ReadTerminalScreen => "read_terminal_screen",
            Self::ReadTerminalScreenDiff => "read_terminal_screen_diff",
            Self::OpenSessionWatch => "open_session_watch",
            Self::RefreshSessionWatch => "refresh_session_watch",
            Self::UpsertWorkContext => "upsert_work_context",
            Self::RemoveWorkContext => "remove_work_context",
            Self::ExpireWorkContext => "expire_work_context",
            Self::ReconcileSession => "reconcile_session",
            Self::ObserveForeground => "observe_foreground",
            Self::ObserveHealth => "observe_health",
            Self::DispatchObservations => "dispatch_observations",
            Self::DispatchOutbox => "dispatch_outbox",
            Self::ObserveProcessExit => "observe_process_exit",
            Self::RunScheduledRestart => "run_scheduled_restart",
            Self::ReconcileStartup => "reconcile_startup",
            Self::ReconcileOperationAttempts => "reconcile_operation_attempts",
            Self::ListDetectionProfiles => "list_detection_profiles",
            Self::InspectDetectionProfile => "inspect_detection_profile",
            Self::MatchDetectionProfile => "match_detection_profile",
            Self::LintDetectionProfile => "lint_detection_profile",
            Self::TestDetectionProfile => "test_detection_profile",
            Self::ReadSessionEvents => "read_session_events",
        }
    }

    /// Returns the stable qualified name mapped by an outward protocol adapter.
    pub fn qualified_name(self) -> String {
        observability_name("operation", self.as_str())
    }
}

/// Stable tracing span names used at application and adapter boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TraceSpanName {
    /// Validate and activate editable detection profiles.
    ReloadProfiles,
    /// One bounded due-observation dispatch batch.
    DispatchObservations,
    /// One ordered transactional outbox dispatch batch.
    DispatchOutbox,
    /// One concrete process-exit settlement cycle.
    ObserveProcessExit,
    /// One revision-checked automatic restart attempt.
    RunScheduledRestart,
    /// Bounded daemon startup reconciliation.
    ReconcileStartup,
    /// Bounded durable operation-attempt reconciliation.
    ReconcileOperationAttempts,
}

impl TraceSpanName {
    /// Complete tracing-span registry in stable declaration order.
    pub const ALL: [Self; 7] = [
        Self::ReloadProfiles,
        Self::DispatchObservations,
        Self::DispatchOutbox,
        Self::ObserveProcessExit,
        Self::RunScheduledRestart,
        Self::ReconcileStartup,
        Self::ReconcileOperationAttempts,
    ];

    /// Returns stable snake-case span name used by tracing adapters.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReloadProfiles => "reload_profiles",
            Self::DispatchObservations => "dispatch_observations",
            Self::DispatchOutbox => "dispatch_outbox",
            Self::ObserveProcessExit => "observe_process_exit",
            Self::RunScheduledRestart => "run_scheduled_restart",
            Self::ReconcileStartup => "reconcile_startup",
            Self::ReconcileOperationAttempts => "reconcile_operation_attempts",
        }
    }

    /// Returns the stable qualified name mapped by an outward protocol adapter.
    pub fn qualified_name(self) -> String {
        observability_name("span", self.as_str())
    }
}

/// Stable structured log and trace field names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DiagnosticFieldName {
    /// Stable structured event name.
    EventName,
    /// Physical connection identity.
    ConnectionId,
    /// Logical client identity.
    ClientId,
    /// Finite request correlation identity.
    RequestId,
    /// Logical stream identity.
    StreamId,
    /// Stable managed session identity.
    SessionId,
    /// Server instance epoch.
    ServerEpoch,
    /// Distributed trace identity.
    TraceId,
    /// Caller span identity propagated as the parent of local work.
    ParentSpanId,
    /// End-to-end correlation identity when supplied by the caller.
    CorrelationId,
    /// Stable operation name.
    Operation,
    /// Stable logical stream kind.
    StreamKind,
    /// Stable result category.
    Result,
    /// Typed error category.
    ErrorKind,
    /// Operation duration in milliseconds.
    DurationMs,
    /// Operation duration in microseconds at local adapter boundaries.
    DurationMicros,
    /// Encoded response bytes at the final settlement boundary.
    ResponseBytes,
    /// Queued bytes at an observed boundary.
    QueuedBytes,
    /// Prepared catalog revision.
    CatalogRevision,
    /// Durable session revision.
    SessionRevision,
    /// Query rows returned.
    RowsReturned,
    /// Query index entries examined.
    IndexEntriesExamined,
    /// Stable metric registry name.
    MetricName,
    /// Numeric metric observation.
    MetricValue,
}

impl DiagnosticFieldName {
    /// Complete field-name registry in stable declaration order.
    pub const ALL: [Self; 24] = [
        Self::EventName,
        Self::ConnectionId,
        Self::ClientId,
        Self::RequestId,
        Self::StreamId,
        Self::SessionId,
        Self::ServerEpoch,
        Self::TraceId,
        Self::ParentSpanId,
        Self::CorrelationId,
        Self::Operation,
        Self::StreamKind,
        Self::Result,
        Self::ErrorKind,
        Self::DurationMs,
        Self::DurationMicros,
        Self::ResponseBytes,
        Self::QueuedBytes,
        Self::CatalogRevision,
        Self::SessionRevision,
        Self::RowsReturned,
        Self::IndexEntriesExamined,
        Self::MetricName,
        Self::MetricValue,
    ];

    /// Returns stable snake-case field name used by structured adapters.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EventName => "event_name",
            Self::ConnectionId => "connection_id",
            Self::ClientId => "client_id",
            Self::RequestId => "request_id",
            Self::StreamId => "stream_id",
            Self::SessionId => "session_id",
            Self::ServerEpoch => "server_epoch",
            Self::TraceId => "trace_id",
            Self::ParentSpanId => "parent_span_id",
            Self::CorrelationId => "correlation_id",
            Self::Operation => "operation",
            Self::StreamKind => "stream_kind",
            Self::Result => "result",
            Self::ErrorKind => "error_kind",
            Self::DurationMs => "duration_ms",
            Self::DurationMicros => "duration_micros",
            Self::ResponseBytes => "response_bytes",
            Self::QueuedBytes => "queued_bytes",
            Self::CatalogRevision => "catalog_revision",
            Self::SessionRevision => "session_revision",
            Self::RowsReturned => "rows_returned",
            Self::IndexEntriesExamined => "index_entries_examined",
            Self::MetricName => "metric_name",
            Self::MetricValue => "metric_value",
        }
    }

    /// Returns the stable qualified name mapped by an outward protocol adapter.
    pub fn qualified_name(self) -> String {
        observability_name("field", self.as_str())
    }
}

/// Stable structured diagnostic event names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DiagnosticEventName {
    /// A start-session use case was accepted.
    SessionStartRequested,
    /// Initial durable session record was committed.
    SessionCreated,
    /// Process adapter spawn started.
    ProcessSpawnStarted,
    /// Process adapter spawn succeeded.
    ProcessSpawnSucceeded,
    /// Process adapter spawn failed.
    ProcessSpawnFailed,
    /// Durable lifecycle transition committed.
    LifecycleCommitted,
    /// Compensating termination started after a partial failure.
    CompensationStarted,
    /// Compensating termination succeeded.
    CompensationSucceeded,
    /// Compensating termination failed.
    CompensationFailed,
    /// Prepared summary projection was updated.
    ProjectionUpdated,
    /// A bounded session query completed.
    SessionQueryCompleted,
    /// A bounded session query was rejected or failed.
    SessionQueryFailed,
    /// A bounded query watch opened with its initial page.
    QueryWatchOpened,
    /// A query watch refresh emitted a bounded delta.
    QueryWatchRefreshed,
    /// A query watch operation was rejected or failed.
    QueryWatchFailed,
    /// A port operation failed.
    PortOperationFailed,
    /// A stop-session use case was accepted.
    SessionStopRequested,
    /// Graceful or forced process termination started.
    ProcessStopStarted,
    /// Process termination completed and was confirmed.
    ProcessStopSucceeded,
    /// Process termination could not be confirmed.
    ProcessStopFailed,
    /// Confirmed terminal lifecycle was committed durably.
    TerminalLifecycleCommitted,
    /// An explicit restart-session use case was accepted.
    SessionRestartRequested,
    /// Explicit restart reached a durable running lifecycle.
    SessionRestartSucceeded,
    /// Explicit restart failed or requires reconciliation.
    SessionRestartFailed,
    /// A rename-session use case was accepted.
    SessionRenameRequested,
    /// A revised session name was committed durably.
    SessionRenamed,
    /// A confirmed remove-session use case was accepted.
    SessionRemovalRequested,
    /// A terminal session was deleted durably.
    SessionRemoved,
    /// A WorkContext declaration was inserted or replaced durably.
    WorkContextUpserted,
    /// A WorkContext declaration was removed durably.
    WorkContextRemoved,
    /// Eligible WorkContext declarations were expired durably.
    WorkContextExpired,
    /// A terminal-resize use case was accepted.
    TerminalResizeRequested,
    /// Live PTY resize and durable revision both completed.
    TerminalResizeApplied,
    /// Live PTY dimensions were restored after durable commit failure.
    TerminalResizeRolledBack,
    /// Live PTY dimensions could not be restored after durable commit failure.
    TerminalResizeRollbackFailed,
    /// A semantic input delivery ID was reserved before PTY IO.
    InputDeliveryReserved,
    /// A completed semantic input delivery was returned without duplicate IO.
    InputDeliveryDeduplicated,
    /// A profile-owned pause will precede one trailing submit key.
    InputDeliveryPacingApplied,
    /// A complete atomic input sequence was accepted by the PTY adapter.
    TerminalInputWritten,
    /// A reserved semantic input delivery result was persisted.
    InputDeliverySettled,
    /// A write outcome was ambiguous and duplicate retry was suppressed.
    InputDeliveryIndeterminate,
    /// Programmatic input updated best-effort foreground observation.
    InputForegroundCommitted,
    /// An application action was denied before resource access.
    AuthorizationDenied,
    /// An authorized detailed session projection was loaded.
    SessionDetailLoaded,
    /// An authorized detailed session projection failed to load.
    SessionDetailFailed,
    /// Private one-shot execution context resolved after authorization.
    ExecutionContextResolved,
    /// Private one-shot execution context was denied, absent, or failed.
    ExecutionContextFailed,
    /// One authorized bounded terminal-output page was read.
    TerminalOutputRead,
    /// A terminal-output read was denied, rejected, or failed.
    TerminalOutputReadFailed,
    /// One authorized current terminal screen was read.
    TerminalScreenRead,
    /// A terminal-screen read was denied, absent, or failed.
    TerminalScreenReadFailed,
    /// One authorized bounded terminal screen update was read.
    TerminalScreenDiffRead,
    /// A terminal-screen update was denied, absent, or failed.
    TerminalScreenDiffReadFailed,
    /// A foreground observation cycle was skipped by an expected precondition.
    ForegroundObservationSkipped,
    /// Exact-process foreground ownership and bounded runtime facts were observed.
    ForegroundProcessObserved,
    /// The process-local effective profile selection or evidence origin changed.
    EffectiveProfileChanged,
    /// An effective profile transition deliberately selected no profile.
    EffectiveProfileUnclassified,
    /// Typed foreground evidence was collected outside mutation ownership.
    ForegroundEvidenceCollected,
    /// A durable foreground decision and rationale was committed.
    ForegroundObservationCommitted,
    /// A foreground observation was discarded after concurrent session change.
    ForegroundObservationStale,
    /// A foreground observation cycle failed.
    ForegroundObservationFailed,
    /// A health observation cycle was skipped by an expected precondition.
    HealthObservationSkipped,
    /// Configured health checks completed outside mutation ownership.
    HealthChecksCompleted,
    /// A durable health summary and results were committed.
    HealthObservationCommitted,
    /// A health observation was discarded after concurrent session change.
    HealthObservationStale,
    /// A health observation cycle failed.
    HealthObservationFailed,
    /// One bounded due-observation dispatch batch began.
    ObservationDispatchStarted,
    /// One scheduled observation failed without stopping its peers.
    ObservationJobFailed,
    /// A queued observation was retired after its session disappeared.
    ObservationJobDeactivated,
    /// A periodic/retry deadline or session cancellation could not be persisted.
    ObservationRescheduleFailed,
    /// One bounded due-observation dispatch batch completed.
    ObservationDispatchCompleted,
    /// A due-observation batch could not be claimed.
    ObservationDispatchFailed,
    /// One bounded transactional outbox dispatch began.
    OutboxDispatchStarted,
    /// One durable session event was published or deduplicated.
    OutboxRecordPublished,
    /// One bounded transactional outbox dispatch completed.
    OutboxDispatchCompleted,
    /// Outbox read, publication, scheduling, or acknowledgement failed.
    OutboxDispatchFailed,
    /// A concrete process-exit observation entered ordered settlement.
    ProcessExitObserved,
    /// A stale or duplicate process-exit observation was ignored.
    ProcessExitIgnored,
    /// Terminal lifecycle and restart disposition were committed atomically.
    ProcessExitCommitted,
    /// A durable scheduled restart was handed to the delayed-work boundary.
    RestartEnqueueStarted,
    /// The rebuildable queue accepted a job whose intent is already durable.
    RestartEnqueued,
    /// Restart intent is durable but delayed-work enqueue failed.
    RestartEnqueueFailed,
    /// Explicit stop durably replaced a scheduled automatic restart.
    RestartCancellationCommitted,
    /// Session-local queued restart cleanup completed.
    RestartQueueCancellationCompleted,
    /// Durable cancellation succeeded but queued restart cleanup failed.
    RestartQueueCancellationFailed,
    /// A due automatic restart job entered ordered validation.
    RestartAttemptStarted,
    /// A stale, early, or superseded automatic restart job was ignored.
    RestartAttemptIgnored,
    /// An automatic restart spawned, bound, and committed a running process.
    RestartAttemptSucceeded,
    /// An automatic restart attempt failed or requires reconciliation.
    RestartAttemptFailed,
    /// Daemon startup reconciliation began.
    StartupReconciliationStarted,
    /// One bounded durable recovery page was loaded.
    StartupRecoveryBatchLoaded,
    /// One active session was reconciled to current OS truth.
    StartupSessionReconciled,
    /// One session could not be reconciled and readiness remains incomplete.
    StartupSessionReconciliationFailed,
    /// Startup reconciliation completed without unresolved sessions.
    StartupReconciliationCompleted,
    /// Startup reconciliation stopped on a recovery-source failure.
    StartupReconciliationFailed,
    /// A standalone detection-profile operation completed.
    DetectionProfileOperationCompleted,
    /// A standalone detection-profile operation was rejected as expected.
    DetectionProfileOperationRejected,
    /// A standalone detection-profile operation failed at an outer boundary.
    DetectionProfileOperationFailed,
    /// A bounded filtered session-event read completed.
    SessionEventsRead,
    /// A session-event read was denied, rejected, or failed.
    SessionEventReadFailed,
}

impl DiagnosticEventName {
    /// Complete diagnostic-event registry in stable declaration order.
    pub const ALL: [Self; 100] = [
        Self::SessionStartRequested,
        Self::SessionCreated,
        Self::ProcessSpawnStarted,
        Self::ProcessSpawnSucceeded,
        Self::ProcessSpawnFailed,
        Self::LifecycleCommitted,
        Self::CompensationStarted,
        Self::CompensationSucceeded,
        Self::CompensationFailed,
        Self::ProjectionUpdated,
        Self::SessionQueryCompleted,
        Self::SessionQueryFailed,
        Self::QueryWatchOpened,
        Self::QueryWatchRefreshed,
        Self::QueryWatchFailed,
        Self::PortOperationFailed,
        Self::SessionStopRequested,
        Self::ProcessStopStarted,
        Self::ProcessStopSucceeded,
        Self::ProcessStopFailed,
        Self::TerminalLifecycleCommitted,
        Self::SessionRestartRequested,
        Self::SessionRestartSucceeded,
        Self::SessionRestartFailed,
        Self::SessionRenameRequested,
        Self::SessionRenamed,
        Self::SessionRemovalRequested,
        Self::SessionRemoved,
        Self::WorkContextUpserted,
        Self::WorkContextRemoved,
        Self::WorkContextExpired,
        Self::TerminalResizeRequested,
        Self::TerminalResizeApplied,
        Self::TerminalResizeRolledBack,
        Self::TerminalResizeRollbackFailed,
        Self::InputDeliveryReserved,
        Self::InputDeliveryDeduplicated,
        Self::InputDeliveryPacingApplied,
        Self::TerminalInputWritten,
        Self::InputDeliverySettled,
        Self::InputDeliveryIndeterminate,
        Self::InputForegroundCommitted,
        Self::AuthorizationDenied,
        Self::SessionDetailLoaded,
        Self::SessionDetailFailed,
        Self::ExecutionContextResolved,
        Self::ExecutionContextFailed,
        Self::TerminalOutputRead,
        Self::TerminalOutputReadFailed,
        Self::TerminalScreenRead,
        Self::TerminalScreenReadFailed,
        Self::TerminalScreenDiffRead,
        Self::TerminalScreenDiffReadFailed,
        Self::ForegroundObservationSkipped,
        Self::ForegroundProcessObserved,
        Self::EffectiveProfileChanged,
        Self::EffectiveProfileUnclassified,
        Self::ForegroundEvidenceCollected,
        Self::ForegroundObservationCommitted,
        Self::ForegroundObservationStale,
        Self::ForegroundObservationFailed,
        Self::HealthObservationSkipped,
        Self::HealthChecksCompleted,
        Self::HealthObservationCommitted,
        Self::HealthObservationStale,
        Self::HealthObservationFailed,
        Self::ObservationDispatchStarted,
        Self::ObservationJobFailed,
        Self::ObservationJobDeactivated,
        Self::ObservationRescheduleFailed,
        Self::ObservationDispatchCompleted,
        Self::ObservationDispatchFailed,
        Self::OutboxDispatchStarted,
        Self::OutboxRecordPublished,
        Self::OutboxDispatchCompleted,
        Self::OutboxDispatchFailed,
        Self::ProcessExitObserved,
        Self::ProcessExitIgnored,
        Self::ProcessExitCommitted,
        Self::RestartEnqueueStarted,
        Self::RestartEnqueued,
        Self::RestartEnqueueFailed,
        Self::RestartCancellationCommitted,
        Self::RestartQueueCancellationCompleted,
        Self::RestartQueueCancellationFailed,
        Self::RestartAttemptStarted,
        Self::RestartAttemptIgnored,
        Self::RestartAttemptSucceeded,
        Self::RestartAttemptFailed,
        Self::StartupReconciliationStarted,
        Self::StartupRecoveryBatchLoaded,
        Self::StartupSessionReconciled,
        Self::StartupSessionReconciliationFailed,
        Self::StartupReconciliationCompleted,
        Self::StartupReconciliationFailed,
        Self::DetectionProfileOperationCompleted,
        Self::DetectionProfileOperationRejected,
        Self::DetectionProfileOperationFailed,
        Self::SessionEventsRead,
        Self::SessionEventReadFailed,
    ];

    /// Returns stable snake-case name used by logging adapters.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SessionStartRequested => "session_start_requested",
            Self::SessionCreated => "session_created",
            Self::ProcessSpawnStarted => "process_spawn_started",
            Self::ProcessSpawnSucceeded => "process_spawn_succeeded",
            Self::ProcessSpawnFailed => "process_spawn_failed",
            Self::LifecycleCommitted => "lifecycle_committed",
            Self::CompensationStarted => "compensation_started",
            Self::CompensationSucceeded => "compensation_succeeded",
            Self::CompensationFailed => "compensation_failed",
            Self::ProjectionUpdated => "projection_updated",
            Self::SessionQueryCompleted => "session_query_completed",
            Self::SessionQueryFailed => "session_query_failed",
            Self::QueryWatchOpened => "query_watch_opened",
            Self::QueryWatchRefreshed => "query_watch_refreshed",
            Self::QueryWatchFailed => "query_watch_failed",
            Self::PortOperationFailed => "port_operation_failed",
            Self::SessionStopRequested => "session_stop_requested",
            Self::ProcessStopStarted => "process_stop_started",
            Self::ProcessStopSucceeded => "process_stop_succeeded",
            Self::ProcessStopFailed => "process_stop_failed",
            Self::TerminalLifecycleCommitted => "terminal_lifecycle_committed",
            Self::SessionRestartRequested => "session_restart_requested",
            Self::SessionRestartSucceeded => "session_restart_succeeded",
            Self::SessionRestartFailed => "session_restart_failed",
            Self::SessionRenameRequested => "session_rename_requested",
            Self::SessionRenamed => "session_renamed",
            Self::SessionRemovalRequested => "session_removal_requested",
            Self::SessionRemoved => "session_removed",
            Self::WorkContextUpserted => "work_context_upserted",
            Self::WorkContextRemoved => "work_context_removed",
            Self::WorkContextExpired => "work_context_expired",
            Self::TerminalResizeRequested => "terminal_resize_requested",
            Self::TerminalResizeApplied => "terminal_resize_applied",
            Self::TerminalResizeRolledBack => "terminal_resize_rolled_back",
            Self::TerminalResizeRollbackFailed => "terminal_resize_rollback_failed",
            Self::InputDeliveryReserved => "input_delivery_reserved",
            Self::InputDeliveryDeduplicated => "input_delivery_deduplicated",
            Self::InputDeliveryPacingApplied => "input_delivery_pacing_applied",
            Self::TerminalInputWritten => "terminal_input_written",
            Self::InputDeliverySettled => "input_delivery_settled",
            Self::InputDeliveryIndeterminate => "input_delivery_indeterminate",
            Self::InputForegroundCommitted => "input_foreground_committed",
            Self::AuthorizationDenied => "authorization_denied",
            Self::SessionDetailLoaded => "session_detail_loaded",
            Self::SessionDetailFailed => "session_detail_failed",
            Self::ExecutionContextResolved => "execution_context_resolved",
            Self::ExecutionContextFailed => "execution_context_failed",
            Self::TerminalOutputRead => "terminal_output_read",
            Self::TerminalOutputReadFailed => "terminal_output_read_failed",
            Self::TerminalScreenRead => "terminal_screen_read",
            Self::TerminalScreenReadFailed => "terminal_screen_read_failed",
            Self::TerminalScreenDiffRead => "terminal_screen_diff_read",
            Self::TerminalScreenDiffReadFailed => "terminal_screen_diff_read_failed",
            Self::ForegroundObservationSkipped => "foreground_observation_skipped",
            Self::ForegroundProcessObserved => "foreground_process_observed",
            Self::EffectiveProfileChanged => "effective_profile_changed",
            Self::EffectiveProfileUnclassified => "effective_profile_unclassified",
            Self::ForegroundEvidenceCollected => "foreground_evidence_collected",
            Self::ForegroundObservationCommitted => "foreground_observation_committed",
            Self::ForegroundObservationStale => "foreground_observation_stale",
            Self::ForegroundObservationFailed => "foreground_observation_failed",
            Self::HealthObservationSkipped => "health_observation_skipped",
            Self::HealthChecksCompleted => "health_checks_completed",
            Self::HealthObservationCommitted => "health_observation_committed",
            Self::HealthObservationStale => "health_observation_stale",
            Self::HealthObservationFailed => "health_observation_failed",
            Self::ObservationDispatchStarted => "observation_dispatch_started",
            Self::ObservationJobFailed => "observation_job_failed",
            Self::ObservationJobDeactivated => "observation_job_deactivated",
            Self::ObservationRescheduleFailed => "observation_reschedule_failed",
            Self::ObservationDispatchCompleted => "observation_dispatch_completed",
            Self::ObservationDispatchFailed => "observation_dispatch_failed",
            Self::OutboxDispatchStarted => "outbox_dispatch_started",
            Self::OutboxRecordPublished => "outbox_record_published",
            Self::OutboxDispatchCompleted => "outbox_dispatch_completed",
            Self::OutboxDispatchFailed => "outbox_dispatch_failed",
            Self::ProcessExitObserved => "process_exit_observed",
            Self::ProcessExitIgnored => "process_exit_ignored",
            Self::ProcessExitCommitted => "process_exit_committed",
            Self::RestartEnqueueStarted => "restart_enqueue_started",
            Self::RestartEnqueued => "restart_enqueued",
            Self::RestartEnqueueFailed => "restart_enqueue_failed",
            Self::RestartCancellationCommitted => "restart_cancellation_committed",
            Self::RestartQueueCancellationCompleted => "restart_queue_cancellation_completed",
            Self::RestartQueueCancellationFailed => "restart_queue_cancellation_failed",
            Self::RestartAttemptStarted => "restart_attempt_started",
            Self::RestartAttemptIgnored => "restart_attempt_ignored",
            Self::RestartAttemptSucceeded => "restart_attempt_succeeded",
            Self::RestartAttemptFailed => "restart_attempt_failed",
            Self::StartupReconciliationStarted => "startup_reconciliation_started",
            Self::StartupRecoveryBatchLoaded => "startup_recovery_batch_loaded",
            Self::StartupSessionReconciled => "startup_session_reconciled",
            Self::StartupSessionReconciliationFailed => "startup_session_reconciliation_failed",
            Self::StartupReconciliationCompleted => "startup_reconciliation_completed",
            Self::StartupReconciliationFailed => "startup_reconciliation_failed",
            Self::DetectionProfileOperationCompleted => "detection_profile_operation_completed",
            Self::DetectionProfileOperationRejected => "detection_profile_operation_rejected",
            Self::DetectionProfileOperationFailed => "detection_profile_operation_failed",
            Self::SessionEventsRead => "session_events_read",
            Self::SessionEventReadFailed => "session_event_read_failed",
        }
    }

    /// Returns the stable qualified product event name.
    pub fn qualified_event_name(self) -> String {
        observability_name("event", self.as_str())
    }

    /// Returns the stable qualified product diagnostic code.
    pub fn qualified_diagnostic_code(self) -> String {
        observability_name("diagnostic", self.as_str())
    }
}

/// Stable metric names emitted by application and adapter boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MetricName {
    /// Duration of a finite application operation.
    OperationDurationSeconds,
    /// Number of in-flight finite requests.
    RequestsInFlight,
    /// Time one request waited in a bounded adapter queue.
    RequestQueueWaitSeconds,
    /// End-to-end finite RPC duration at the product adapter boundary.
    RpcDurationSeconds,
    /// Successful finite RPC result count.
    RpcResultsSuccess,
    /// Invalid finite RPC result count.
    RpcResultsInvalidRequest,
    /// Rejected finite RPC result count.
    RpcResultsRejected,
    /// Cancelled finite RPC result count.
    RpcResultsCancelled,
    /// Capacity-rejected finite RPC result count.
    RpcResultsCapacity,
    /// Encoding-failed finite RPC result count.
    RpcResultsEncoding,
    /// Connection-failed finite RPC result count.
    RpcResultsConnection,
    /// Internal-failed finite RPC result count.
    RpcResultsInternal,
    /// Encoded finite RPC response bytes by typed application operation.
    RpcResponseBytes,
    /// Number of prepared query index entries examined.
    QueryIndexEntriesExamined,
    /// Number of summaries returned by a query.
    QueryRowsReturned,
    /// Prepared catalog revision used by one list or watch extraction.
    QueryCatalogRevision,
    /// Compact projection query count.
    QueryProjectionCompact,
    /// Label-bearing projection query count.
    QueryProjectionWithLabels,
    /// Operational projection query count.
    QueryProjectionOperational,
    /// Prepared query cursors rejected because their revision expired.
    QueryCursorExpirations,
    /// Number of active logical streams.
    LogicalStreamsActive,
    /// Number of active physical client connections.
    PhysicalConnections,
    /// Number of pending finite requests on a physical connection.
    PendingRequests,
    /// Bytes queued by a connection priority class.
    ConnectionQueuedBytes,
    /// Messages queued by a connection priority class.
    ConnectionQueuedMessages,
    /// Stream items coalesced before delivery.
    StreamItemsCoalesced,
    /// Stream items dropped with explicit gap semantics.
    StreamItemsDropped,
    /// Resume gaps reported to clients.
    StreamGapsReported,
    /// Retained stream items replayed during resume.
    StreamItemsReplayed,
    /// Current connection-wide flow-control credit.
    ConnectionCreditBytes,
    /// Duration of one physical connection write.
    ConnectionWriteDurationSeconds,
    /// Queued items and pending gap for one sampled logical stream.
    StreamLagItems,
    /// Session-supervisor mailbox depth.
    SupervisorMailboxDepth,
    /// Duration of one session-supervisor mutation acquisition command.
    SupervisorCommandDurationSeconds,
    /// Requests rejected because a bounded product or session queue was full.
    OverloadRejections,
    /// Duration of one foreground evidence collection cycle.
    DetectionDurationSeconds,
    /// Duration of one exact-process foreground ownership observation.
    ForegroundProcessObservationDurationSeconds,
    /// Number of effective profile or evidence-origin transitions.
    EffectiveProfileTransitions,
    /// Number of transitions to a safely unclassified effective profile.
    EffectiveProfileUnclassifiedTransitions,
    /// Age of selected foreground evidence at commit.
    DetectionEvidenceAgeSeconds,
    /// Duration of one configured health evaluation cycle.
    HealthCheckDurationSeconds,
    /// Number of health rule results returned in one evaluation.
    HealthCheckResults,
    /// Number of passing health rule results returned in one evaluation.
    HealthCheckResultsPassed,
    /// Number of failing health rule results returned in one evaluation.
    HealthCheckResultsFailed,
    /// Number of automatic restart attempts accepted for execution.
    RestartAttempts,
    /// Delay between a restart deadline and actual attempt start.
    RestartSchedulingLagSeconds,
    /// Delay between an observation deadline and actual worker claim.
    ObservationSchedulingLagSeconds,
    /// Delay between domain commit and outbox publication attempt.
    OutboxDispatchLagSeconds,
    /// Number of outbox records published in one bounded batch.
    OutboxRecordsPublished,
    /// Number of sessions examined during one startup reconciliation.
    StartupSessionsExamined,
    /// Number of sessions unresolved after startup reconciliation.
    StartupSessionsUnresolved,
    /// Raw terminal-output bytes returned by one authorized bounded read.
    TerminalOutputBytesRead,
    /// Raw terminal-output bytes accepted from supervised PTYs or host replay.
    TerminalOutputBytesAppended,
    /// Current globally retained terminal-output bytes.
    TerminalOutputRetainedBytes,
    /// Durable SQLite mutation or outbox checkpoint completed successfully.
    PersistenceCheckpointSucceeded,
    /// Durable SQLite mutation or outbox checkpoint failed.
    PersistenceCheckpointFailed,
    /// Persistence open or bounded recovery read completed successfully.
    PersistenceRecoverySucceeded,
    /// Persistence open or bounded recovery read failed.
    PersistenceRecoveryFailed,
}

impl MetricName {
    /// Complete metric registry in stable declaration order.
    pub const ALL: [Self; 58] = [
        Self::OperationDurationSeconds,
        Self::RequestsInFlight,
        Self::RequestQueueWaitSeconds,
        Self::RpcDurationSeconds,
        Self::RpcResultsSuccess,
        Self::RpcResultsInvalidRequest,
        Self::RpcResultsRejected,
        Self::RpcResultsCancelled,
        Self::RpcResultsCapacity,
        Self::RpcResultsEncoding,
        Self::RpcResultsConnection,
        Self::RpcResultsInternal,
        Self::RpcResponseBytes,
        Self::QueryIndexEntriesExamined,
        Self::QueryRowsReturned,
        Self::QueryCatalogRevision,
        Self::QueryProjectionCompact,
        Self::QueryProjectionWithLabels,
        Self::QueryProjectionOperational,
        Self::QueryCursorExpirations,
        Self::LogicalStreamsActive,
        Self::PhysicalConnections,
        Self::PendingRequests,
        Self::ConnectionQueuedBytes,
        Self::ConnectionQueuedMessages,
        Self::StreamItemsCoalesced,
        Self::StreamItemsDropped,
        Self::StreamGapsReported,
        Self::StreamItemsReplayed,
        Self::ConnectionCreditBytes,
        Self::ConnectionWriteDurationSeconds,
        Self::StreamLagItems,
        Self::SupervisorMailboxDepth,
        Self::SupervisorCommandDurationSeconds,
        Self::OverloadRejections,
        Self::DetectionDurationSeconds,
        Self::ForegroundProcessObservationDurationSeconds,
        Self::EffectiveProfileTransitions,
        Self::EffectiveProfileUnclassifiedTransitions,
        Self::DetectionEvidenceAgeSeconds,
        Self::HealthCheckDurationSeconds,
        Self::HealthCheckResults,
        Self::HealthCheckResultsPassed,
        Self::HealthCheckResultsFailed,
        Self::RestartAttempts,
        Self::RestartSchedulingLagSeconds,
        Self::ObservationSchedulingLagSeconds,
        Self::OutboxDispatchLagSeconds,
        Self::OutboxRecordsPublished,
        Self::StartupSessionsExamined,
        Self::StartupSessionsUnresolved,
        Self::TerminalOutputBytesRead,
        Self::TerminalOutputBytesAppended,
        Self::TerminalOutputRetainedBytes,
        Self::PersistenceCheckpointSucceeded,
        Self::PersistenceCheckpointFailed,
        Self::PersistenceRecoverySucceeded,
        Self::PersistenceRecoveryFailed,
    ];

    /// Returns stable snake-case name used by metrics adapters.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OperationDurationSeconds => "operation_duration_seconds",
            Self::RequestsInFlight => "requests_in_flight",
            Self::RequestQueueWaitSeconds => "request_queue_wait_seconds",
            Self::RpcDurationSeconds => "rpc_duration_seconds",
            Self::RpcResultsSuccess => "rpc_results_success",
            Self::RpcResultsInvalidRequest => "rpc_results_invalid_request",
            Self::RpcResultsRejected => "rpc_results_rejected",
            Self::RpcResultsCancelled => "rpc_results_cancelled",
            Self::RpcResultsCapacity => "rpc_results_capacity",
            Self::RpcResultsEncoding => "rpc_results_encoding",
            Self::RpcResultsConnection => "rpc_results_connection",
            Self::RpcResultsInternal => "rpc_results_internal",
            Self::RpcResponseBytes => "rpc_response_bytes",
            Self::QueryIndexEntriesExamined => "query_index_entries_examined",
            Self::QueryRowsReturned => "query_rows_returned",
            Self::QueryCatalogRevision => "query_catalog_revision",
            Self::QueryProjectionCompact => "query_projection_compact",
            Self::QueryProjectionWithLabels => "query_projection_with_labels",
            Self::QueryProjectionOperational => "query_projection_operational",
            Self::QueryCursorExpirations => "query_cursor_expirations",
            Self::LogicalStreamsActive => "logical_streams_active",
            Self::PhysicalConnections => "physical_connections",
            Self::PendingRequests => "pending_requests",
            Self::ConnectionQueuedBytes => "connection_queued_bytes",
            Self::ConnectionQueuedMessages => "connection_queued_messages",
            Self::StreamItemsCoalesced => "stream_items_coalesced",
            Self::StreamItemsDropped => "stream_items_dropped",
            Self::StreamGapsReported => "stream_gaps_reported",
            Self::StreamItemsReplayed => "stream_items_replayed",
            Self::ConnectionCreditBytes => "connection_credit_bytes",
            Self::ConnectionWriteDurationSeconds => "connection_write_duration_seconds",
            Self::StreamLagItems => "stream_lag_items",
            Self::SupervisorMailboxDepth => "supervisor_mailbox_depth",
            Self::SupervisorCommandDurationSeconds => "supervisor_command_duration_seconds",
            Self::OverloadRejections => "overload_rejections",
            Self::DetectionDurationSeconds => "detection_duration_seconds",
            Self::ForegroundProcessObservationDurationSeconds => {
                "foreground_process_observation_duration_seconds"
            }
            Self::EffectiveProfileTransitions => "effective_profile_transitions",
            Self::EffectiveProfileUnclassifiedTransitions => {
                "effective_profile_unclassified_transitions"
            }
            Self::DetectionEvidenceAgeSeconds => "detection_evidence_age_seconds",
            Self::HealthCheckDurationSeconds => "health_check_duration_seconds",
            Self::HealthCheckResults => "health_check_results",
            Self::HealthCheckResultsPassed => "health_check_results_passed",
            Self::HealthCheckResultsFailed => "health_check_results_failed",
            Self::RestartAttempts => "restart_attempts",
            Self::RestartSchedulingLagSeconds => "restart_scheduling_lag_seconds",
            Self::ObservationSchedulingLagSeconds => "observation_scheduling_lag_seconds",
            Self::OutboxDispatchLagSeconds => "outbox_dispatch_lag_seconds",
            Self::OutboxRecordsPublished => "outbox_records_published",
            Self::StartupSessionsExamined => "startup_sessions_examined",
            Self::StartupSessionsUnresolved => "startup_sessions_unresolved",
            Self::TerminalOutputBytesRead => "terminal_output_bytes_read",
            Self::TerminalOutputBytesAppended => "terminal_output_bytes_appended",
            Self::TerminalOutputRetainedBytes => "terminal_output_retained_bytes",
            Self::PersistenceCheckpointSucceeded => "persistence_checkpoint_succeeded",
            Self::PersistenceCheckpointFailed => "persistence_checkpoint_failed",
            Self::PersistenceRecoverySucceeded => "persistence_recovery_succeeded",
            Self::PersistenceRecoveryFailed => "persistence_recovery_failed",
        }
    }

    /// Returns the stable qualified product metric name.
    pub fn qualified_name(self) -> String {
        observability_name("metric", self.as_str())
    }
}

/// Severity passed to a concrete structured logging adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DiagnosticLevel {
    /// Detailed expected-path information.
    Debug,
    /// Normal meaningful boundary or decision.
    Info,
    /// Degraded behavior that completed or was compensated.
    Warn,
    /// Infrastructure failure or invariant violation.
    Error,
}

impl DiagnosticLevel {
    /// Selects a consistent payload-safe severity for an application failure.
    pub const fn for_application_error(category: ApplicationErrorCategory) -> Self {
        match category {
            ApplicationErrorCategory::InvalidArgument
            | ApplicationErrorCategory::Unauthenticated
            | ApplicationErrorCategory::PermissionDenied
            | ApplicationErrorCategory::NotFound
            | ApplicationErrorCategory::FailedPrecondition
            | ApplicationErrorCategory::Conflict
            | ApplicationErrorCategory::Cancelled
            | ApplicationErrorCategory::CursorExpired => Self::Debug,
            ApplicationErrorCategory::CapacityExceeded
            | ApplicationErrorCategory::RateLimited
            | ApplicationErrorCategory::Timeout
            | ApplicationErrorCategory::Interrupted
            | ApplicationErrorCategory::Unavailable
            | ApplicationErrorCategory::ReplayUnavailable
            | ApplicationErrorCategory::Unsupported => Self::Warn,
            ApplicationErrorCategory::ProtocolViolation
            | ApplicationErrorCategory::InvariantViolation
            | ApplicationErrorCategory::Internal => Self::Error,
        }
    }

    /// Selects severity for an adapter failure through the same category policy.
    pub const fn for_port_error(kind: PortErrorKind) -> Self {
        Self::for_application_error(kind.application_category())
    }

    /// Returns the stable severity name for outward mapping.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warning",
            Self::Error => "error",
        }
    }
}

/// Payload-safe application diagnostic.
///
/// The record intentionally has no arbitrary string map or private payload
/// field. Adapters can add connection/request/trace context at their boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticRecord {
    /// Stable event name.
    pub event: DiagnosticEventName,
    /// Severity chosen by the use case.
    pub level: DiagnosticLevel,
    /// Explicit application-clock timestamp.
    pub timestamp: TimestampMillis,
    /// Safe stable session identity when applicable.
    pub session_id: Option<SessionId>,
    /// User-visible use case being executed, when applicable.
    pub application_operation: Option<ApplicationOperation>,
    /// Typed application rejection or failure category, when applicable.
    pub application_error: Option<ApplicationErrorCategory>,
    /// Port operation when the event crosses an infrastructure boundary.
    pub operation: Option<PortOperation>,
    /// Typed infrastructure failure kind without a source message.
    pub error_kind: Option<PortErrorKind>,
}

/// Numeric metric observation without high-cardinality free-form labels.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MetricObservation {
    /// Stable metric name.
    pub name: MetricName,
    /// Numeric observation, counter increment, or gauge value.
    pub value: f64,
    /// Optional typed application use-case label.
    pub operation: Option<ApplicationOperation>,
}

impl MetricObservation {
    /// Returns the stable qualified metric name for outward mapping.
    pub fn qualified_name(self) -> String {
        self.name.qualified_name()
    }
}

/// Numeric metric observation scoped to one safe exact session identity.
///
/// This record cannot carry terminal bytes or arbitrary labels. Adapters must
/// preserve the typed session identity when retaining or exporting it.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionMetricObservation {
    session_id: SessionId,
    metric: MetricObservation,
}

impl SessionMetricObservation {
    /// Creates one exact-session metric without performing IO.
    pub const fn new(session_id: SessionId, metric: MetricObservation) -> Self {
        Self { session_id, metric }
    }

    /// Returns the safe exact session identity.
    pub const fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    /// Returns the fixed-cardinality numeric metric.
    pub const fn metric(&self) -> MetricObservation {
        self.metric
    }
}
fn observability_name(kind: &str, local_name: &str) -> String {
    format!("{UNDERCURRENT_OBSERVABILITY_NAMESPACE}.{kind}.{local_name}")
}
