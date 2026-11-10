// SPDX-License-Identifier: Apache-2.0

//! Application-service composition for reconciled daemon background work.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use undercurrent_application::{
    ApplicationErrorCategory, ApplicationOperation, Clock, DetectionProfileCatalog,
    DiagnosticEventName, DiagnosticLevel, DiagnosticRecord, DispatchObservationsService,
    DispatchOutboxService, DueObservationSource, DueRestartSource, EffectiveProfileDirectory,
    ForegroundEvidenceCollector, ForegroundProcessObserver, HealthCheckExecutor,
    ObservationDispatchFailure, ObservationDispatchLimits, ObservationScheduler,
    ObservationServiceRouter, ObserveForegroundService, ObserveHealthService,
    ObserveProcessExitService, OperationAttemptEffectObserver,
    OperationAttemptReconciliationLimits, OperationAttemptReconciliationReport,
    OperationAttemptStore, OutboxDispatchLimits, PortOperation, ProcessExitObservation,
    ProcessExitSource, ProcessRuntime, ProcessTruthSource, ProfilePatternEngine,
    ReconcileOperationAttemptsError, ReconcileOperationAttemptsService, RestartScheduler,
    RunScheduledRestartService, SessionEventPublisher, SessionMutationCoordinator, SessionOutbox,
    SessionProcessDirectory, SessionProjectionWriter, SessionRecoverySource, SessionRepository,
    StartupReconciliationError, StartupReconciliationLimits, StartupReconciliationReport,
    StartupReconciliationService, Telemetry, TraceSpanName,
};
use undercurrent_domain::ForegroundDecisionPolicy;
use undercurrent_runtime::HostedProcessRuntime;

use crate::{
    DaemonBackgroundTask, DaemonBackgroundTaskError, DaemonBackgroundWorkers,
    DaemonBackgroundWorkersCreateError, DaemonDiagnosticField, DaemonDiagnosticRow,
    DaemonDiagnosticSection, DaemonDiagnosticSource, DaemonDiagnosticValue, DaemonOperationsError,
    DaemonWorkerCycle, DaemonWorkerIntervals, DaemonWorkerKind, DaemonWorkerSpec,
    OutboxDispatchWake, ResolvedDaemonConfig, blocking::block_on,
    workers::MINIMUM_WORKER_STACK_BYTES,
};

/// Upper safety-poll delay for idle database-backed workers. Configured base
/// intervals longer than this remain authoritative.
const DATABASE_EMPTY_BACKOFF_CAP: Duration = Duration::from_secs(1);

/// Shared application-core ports used by command and background use cases.
pub struct DaemonCorePorts {
    clock: Arc<dyn Clock>,
    coordinator: Arc<dyn SessionMutationCoordinator>,
    repository: Arc<dyn SessionRepository>,
    runtime: Arc<dyn ProcessRuntime>,
    processes: Arc<dyn SessionProcessDirectory>,
    restarts: DaemonRestartPorts,
    projection: Arc<dyn SessionProjectionWriter>,
    effective_profiles: Arc<dyn EffectiveProfileDirectory>,
    telemetry: Arc<dyn Telemetry>,
    attempts: Arc<dyn OperationAttemptStore>,
    attempt_observer: Arc<dyn OperationAttemptEffectObserver>,
    attempt_readiness: Arc<OperationAttemptReadinessGate>,
}

impl DaemonCorePorts {
    /// Groups the stable inward-facing ports shared by daemon use cases.
    ///
    /// Construction retains handles but performs no IO and does not panic.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        clock: Arc<dyn Clock>,
        coordinator: Arc<dyn SessionMutationCoordinator>,
        repository: Arc<dyn SessionRepository>,
        runtime: Arc<dyn ProcessRuntime>,
        processes: Arc<dyn SessionProcessDirectory>,
        restarts: DaemonRestartPorts,
        projection: Arc<dyn SessionProjectionWriter>,
        effective_profiles: Arc<dyn EffectiveProfileDirectory>,
        telemetry: Arc<dyn Telemetry>,
        attempts: Arc<dyn OperationAttemptStore>,
        attempt_observer: Arc<dyn OperationAttemptEffectObserver>,
        attempt_readiness: Arc<OperationAttemptReadinessGate>,
    ) -> Self {
        Self {
            clock,
            coordinator,
            repository,
            runtime,
            processes,
            restarts,
            projection,
            effective_profiles,
            telemetry,
            attempts,
            attempt_observer,
            attempt_readiness,
        }
    }
}

/// Recovery health and aggregate debt hint; requests still check their resource scope.
#[derive(Debug, Default)]
pub struct OperationAttemptReadinessGate {
    ready: AtomicBool,
    scopes_available: AtomicBool,
    recovery_cursor: Mutex<Option<undercurrent_application::OperationAttemptScanCursor>>,
}

impl OperationAttemptReadinessGate {
    /// Creates closed infrastructure admission until a successful durable recovery scan.
    pub const fn new() -> Self {
        Self {
            ready: AtomicBool::new(false),
            scopes_available: AtomicBool::new(false),
            recovery_cursor: Mutex::new(None),
        }
    }

    /// Reports the aggregate debt-free hint, not an individual request admission decision.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Applies one bounded reconciliation report without guessing about unknown effects.
    pub fn apply_reconciliation(&self, report: &OperationAttemptReconciliationReport) {
        self.scopes_available.store(true, Ordering::Release);
        self.ready.store(
            report.complete && report.unresolved.is_empty(),
            Ordering::Release,
        );
    }

    /// Applies the exact post-repair unresolved count returned by the durable store.
    pub fn apply_unresolved_count(&self, unresolved: usize) {
        self.scopes_available.store(true, Ordering::Release);
        self.ready.store(unresolved == 0, Ordering::Release);
    }

    /// Closes mutation admission after a reconciliation infrastructure failure.
    pub fn close(&self) {
        self.scopes_available.store(false, Ordering::Release);
        self.invalidate();
    }

    pub(crate) fn invalidate(&self) {
        self.ready.store(false, Ordering::Release);
    }

    pub(crate) fn scopes_available(&self) -> bool {
        self.scopes_available.load(Ordering::Acquire)
    }
}

/// One shared automatic-restart store exposed through write and claim ports.
#[derive(Clone)]
pub struct DaemonRestartPorts {
    scheduler: Arc<dyn RestartScheduler>,
    source: Arc<dyn DueRestartSource>,
}

impl DaemonRestartPorts {
    /// Derives both restart port views from the same concrete bounded store.
    ///
    /// Construction retains two handles to one adapter instance, performs no
    /// IO, and does not panic.
    pub fn new<T>(store: Arc<T>) -> Self
    where
        T: RestartScheduler + DueRestartSource + 'static,
    {
        Self {
            scheduler: store.clone(),
            source: store,
        }
    }
}

impl Debug for DaemonRestartPorts {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonRestartPorts")
            .finish_non_exhaustive()
    }
}

/// One shared observation work store exposed through schedule and claim ports.
#[derive(Clone)]
pub struct DaemonObservationPorts {
    scheduler: Arc<dyn ObservationScheduler>,
    source: Arc<dyn DueObservationSource>,
}

impl DaemonObservationPorts {
    /// Derives both observation port views from one deadline-indexed store.
    ///
    /// Construction retains two handles to one adapter instance, performs no
    /// IO, and does not panic.
    pub fn new<T>(store: Arc<T>) -> Self
    where
        T: ObservationScheduler + DueObservationSource + 'static,
    {
        Self {
            scheduler: store.clone(),
            source: store,
        }
    }
}

impl Debug for DaemonObservationPorts {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonObservationPorts")
            .finish_non_exhaustive()
    }
}

impl Debug for DaemonCorePorts {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonCorePorts")
            .finish_non_exhaustive()
    }
}

/// Shutdown edge that wakes the blocking process-exit source.
pub trait ProcessExitInterrupt: Send + Sync {
    /// Permanently interrupts a blocked exit receive during daemon shutdown.
    fn interrupt_process_exits(&self);
}

impl ProcessExitInterrupt for HostedProcessRuntime {
    fn interrupt_process_exits(&self) {
        self.begin_shutdown();
    }
}

/// One shared process-exit adapter exposed through receive and interrupt edges.
#[derive(Clone)]
pub struct DaemonProcessExitPorts {
    source: Arc<dyn ProcessExitSource>,
    interrupt: Arc<dyn ProcessExitInterrupt>,
}

impl DaemonProcessExitPorts {
    /// Derives receive and shutdown views from one concrete exit adapter.
    ///
    /// Construction retains two handles to one adapter instance, performs no
    /// IO, and does not panic.
    pub fn new<T>(source: Arc<T>) -> Self
    where
        T: ProcessExitSource + ProcessExitInterrupt + 'static,
    {
        Self {
            source: source.clone(),
            interrupt: source,
        }
    }
}

impl Debug for DaemonProcessExitPorts {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonProcessExitPorts")
            .finish_non_exhaustive()
    }
}

/// Worker-specific sources and adapters kept outside the application core.
pub struct DaemonWorkerPorts {
    profile_reload: Option<Arc<crate::profile_files::ProfileReload>>,
    outbox: Arc<dyn SessionOutbox>,
    events: Arc<dyn SessionEventPublisher>,
    observations: DaemonObservationPorts,
    foreground: Arc<dyn ForegroundEvidenceCollector>,
    foreground_processes: Arc<dyn ForegroundProcessObserver>,
    profiles: Arc<dyn DetectionProfileCatalog>,
    patterns: Arc<dyn ProfilePatternEngine>,
    health: Arc<dyn HealthCheckExecutor>,
    recovery: Arc<dyn SessionRecoverySource>,
    process_truth: Arc<dyn ProcessTruthSource>,
    process_exits: DaemonProcessExitPorts,
}

impl DaemonWorkerPorts {
    pub(crate) fn with_profile_reload(
        mut self,
        reload: Arc<crate::profile_files::ProfileReload>,
    ) -> Self {
        self.profile_reload = Some(reload);
        self
    }

    /// Groups bounded worker sources and external observation adapters.
    ///
    /// Construction performs no IO and does not panic.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        outbox: Arc<dyn SessionOutbox>,
        events: Arc<dyn SessionEventPublisher>,
        observations: DaemonObservationPorts,
        foreground: Arc<dyn ForegroundEvidenceCollector>,
        foreground_processes: Arc<dyn ForegroundProcessObserver>,
        profiles: Arc<dyn DetectionProfileCatalog>,
        patterns: Arc<dyn ProfilePatternEngine>,
        health: Arc<dyn HealthCheckExecutor>,
        recovery: Arc<dyn SessionRecoverySource>,
        process_truth: Arc<dyn ProcessTruthSource>,
        process_exits: DaemonProcessExitPorts,
    ) -> Self {
        Self {
            profile_reload: None,
            outbox,
            events,
            observations,
            foreground,
            foreground_processes,
            profiles,
            patterns,
            health,
            recovery,
            process_truth,
            process_exits,
        }
    }
}

impl Debug for DaemonWorkerPorts {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonWorkerPorts")
            .finish_non_exhaustive()
    }
}

/// Complete application and adapter port graph needed by daemon background work.
#[derive(Debug)]
pub struct DaemonBackgroundPorts {
    core: DaemonCorePorts,
    workers: DaemonWorkerPorts,
}

impl DaemonBackgroundPorts {
    /// Joins shared use-case ports and worker-specific sources without IO.
    pub const fn new(core: DaemonCorePorts, workers: DaemonWorkerPorts) -> Self {
        Self { core, workers }
    }
}

/// Validated focused settings for startup reconciliation and background work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DaemonApplicationRuntimeConfig {
    outbox: OutboxDispatchLimits,
    restart_batch_size: usize,
    observation: ObservationDispatchLimits,
    reconciliation: StartupReconciliationLimits,
    attempt_reconciliation: OperationAttemptReconciliationLimits,
    foreground: ForegroundDecisionPolicy,
    max_profiles: usize,
    intervals: DaemonWorkerIntervals,
    worker_thread_stack_bytes: usize,
}

impl DaemonApplicationRuntimeConfig {
    /// Validates the remaining primitive daemon worker bounds.
    ///
    /// The application limit types supplied here are already validated by
    /// their owning layers. This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed invalid-limit failure for a zero restart batch, zero
    /// interval, or undersized worker stack.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        outbox: OutboxDispatchLimits,
        restart_batch_size: usize,
        observation: ObservationDispatchLimits,
        reconciliation: StartupReconciliationLimits,
        attempt_reconciliation: OperationAttemptReconciliationLimits,
        foreground: ForegroundDecisionPolicy,
        max_profiles: usize,
        intervals: DaemonWorkerIntervals,
        worker_thread_stack_bytes: usize,
    ) -> Result<Self, DaemonBackgroundWorkersCreateError> {
        if restart_batch_size == 0 {
            return Err(DaemonBackgroundWorkersCreateError::InvalidLimit {
                field: "restart_batch_size",
            });
        }
        if max_profiles == 0 {
            return Err(DaemonBackgroundWorkersCreateError::InvalidLimit {
                field: "max_profiles",
            });
        }
        for (field, interval) in [
            ("outbox_interval", intervals.outbox),
            ("restart_interval", intervals.restart),
            ("observation_interval", intervals.observation),
            ("process_exit_error_interval", intervals.process_exit_error),
        ] {
            if interval.is_zero() {
                return Err(DaemonBackgroundWorkersCreateError::InvalidLimit { field });
            }
        }
        if worker_thread_stack_bytes < MINIMUM_WORKER_STACK_BYTES {
            return Err(DaemonBackgroundWorkersCreateError::InvalidLimit {
                field: "worker_thread_stack_bytes",
            });
        }
        Ok(Self {
            outbox,
            restart_batch_size,
            observation,
            reconciliation,
            attempt_reconciliation,
            foreground,
            max_profiles,
            intervals,
            worker_thread_stack_bytes,
        })
    }

    /// Extracts already validated settings from the resolved daemon config.
    pub fn from_resolved(config: &ResolvedDaemonConfig) -> Self {
        let batch_size = config.reconciliation().batch_size();
        let max_batches = config
            .sqlite()
            .limits()
            .max_sessions()
            .div_ceil(batch_size)
            .max(1);
        Self {
            outbox: config.outbox(),
            restart_batch_size: config.restart_batch_size(),
            observation: config.observation(),
            reconciliation: config.reconciliation(),
            attempt_reconciliation: OperationAttemptReconciliationLimits::new(
                batch_size,
                max_batches,
            )
            .expect("resolved nonzero reconciliation limits"),
            foreground: config.services().foreground(),
            max_profiles: config.services().profile_service().max_profiles,
            intervals: config.worker_intervals(),
            worker_thread_stack_bytes: config.worker_thread_stack_bytes(),
        }
    }
}

/// Failure reconciling startup truth or starting the fixed worker set.
#[derive(Debug)]
pub enum DaemonApplicationRuntimeStartError {
    /// Fatal recovery-source or recovery-page contract failure.
    Reconciliation(StartupReconciliationError),
    /// Durable attempt recovery failed before mutation readiness could be decided.
    AttemptReconciliation(ReconcileOperationAttemptsError),
    /// Full scan completed but at least one session remains unresolved.
    ReadinessIncomplete(Box<StartupReconciliationReport>),
    /// Focused worker settings or thread startup failed.
    Workers(DaemonBackgroundWorkersCreateError),
}

impl DaemonApplicationRuntimeStartError {
    /// Returns the stable safe application category for startup presentation.
    pub const fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::Reconciliation(error) => error.category(),
            Self::AttemptReconciliation(error) => attempt_reconciliation_category(error),
            Self::ReadinessIncomplete(_) => ApplicationErrorCategory::FailedPrecondition,
            Self::Workers(_) => ApplicationErrorCategory::Unavailable,
        }
    }

    /// Returns the incomplete payload-safe readiness report, when available.
    pub const fn readiness_report(&self) -> Option<&StartupReconciliationReport> {
        match self {
            Self::Reconciliation(error) => Some(error.partial_report()),
            Self::ReadinessIncomplete(report) => Some(report),
            Self::AttemptReconciliation(_) | Self::Workers(_) => None,
        }
    }
}

impl Display for DaemonApplicationRuntimeStartError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Reconciliation(_) => formatter.write_str("daemon startup reconciliation failed"),
            Self::AttemptReconciliation(_) => {
                formatter.write_str("daemon operation-attempt reconciliation failed")
            }
            Self::ReadinessIncomplete(report) => write!(
                formatter,
                "daemon startup reconciliation left {} sessions unresolved",
                report.unresolved
            ),
            Self::Workers(_) => formatter.write_str("daemon background workers could not start"),
        }
    }
}

impl Error for DaemonApplicationRuntimeStartError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Reconciliation(error) => Some(error),
            Self::AttemptReconciliation(error) => Some(error),
            Self::Workers(error) => Some(error),
            Self::ReadinessIncomplete(_) => None,
        }
    }
}

/// Reconciled application runtime owning every daemon background worker.
pub struct DaemonApplicationRuntime {
    startup: StartupReconciliationReport,
    attempt_startup: OperationAttemptReconciliationReport,
    attempts: Arc<dyn OperationAttemptStore>,
    diagnostic_attempt_limit: usize,
    workers: DaemonBackgroundWorkers,
}

impl DaemonApplicationRuntime {
    /// Reconciles durable sessions and operation attempts before starting fixed workers.
    ///
    /// No worker is started and readiness must remain closed until the recovery
    /// source reaches its valid end and every durable session is resolved. The
    /// returned owner is the deterministic shutdown and join point for all
    /// background application work. A successful attempt-reconciliation pass
    /// with unresolved effects permits scoped admission checks and background
    /// workers; only mutations conflicting with that recovery debt are rejected.
    ///
    /// # Errors
    ///
    /// Returns a typed fatal reconciliation, incomplete readiness, invalid
    /// worker setting, or worker-thread start failure. Error presentation does
    /// not expose commands, environment, terminal contents, or adapter source
    /// text.
    pub fn start(
        ports: Arc<DaemonBackgroundPorts>,
        config: DaemonApplicationRuntimeConfig,
    ) -> Result<Self, DaemonApplicationRuntimeStartError> {
        let startup = reconcile_startup(&ports, config.reconciliation)
            .map_err(DaemonApplicationRuntimeStartError::Reconciliation)?;
        if !startup.ready() {
            return Err(DaemonApplicationRuntimeStartError::ReadinessIncomplete(
                Box::new(startup),
            ));
        }
        let attempt_startup = reconcile_attempts(&ports, config.attempt_reconciliation)
            .map_err(DaemonApplicationRuntimeStartError::AttemptReconciliation)?;
        ports
            .core
            .attempt_readiness
            .apply_reconciliation(&attempt_startup);
        let attempts = Arc::clone(&ports.core.attempts);
        let specifications = worker_specifications(ports, config)
            .map_err(DaemonApplicationRuntimeStartError::Workers)?;
        let workers =
            DaemonBackgroundWorkers::start(specifications, config.worker_thread_stack_bytes)
                .map_err(DaemonApplicationRuntimeStartError::Workers)?;
        Ok(Self {
            startup,
            attempt_startup,
            attempts,
            diagnostic_attempt_limit: config.attempt_reconciliation.batch_size(),
            workers,
        })
    }

    /// Returns the complete payload-safe startup readiness report.
    pub const fn startup_report(&self) -> &StartupReconciliationReport {
        &self.startup
    }

    /// Returns current payload-free state for every fixed worker.
    pub fn worker_pressure(&self) -> Vec<crate::DaemonWorkerPressure> {
        self.workers.pressure()
    }

    pub(crate) fn request_shutdown(&self) {
        self.workers.request_reset();
    }

    pub(crate) fn is_quiescent(&self) -> bool {
        self.workers.is_quiescent()
    }

    /// Interrupts blocking sources and joins every background worker.
    pub fn shutdown(&self) {
        self.workers.shutdown();
    }
}

impl OutboxDispatchWake for DaemonApplicationRuntime {
    fn wake_outbox_dispatch(&self) {
        self.workers.wake(DaemonWorkerKind::Outbox);
    }

    fn wake_attempt_reconciliation(&self) {
        self.workers.wake(DaemonWorkerKind::AttemptReconciliation);
    }
}

impl Debug for DaemonApplicationRuntime {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonApplicationRuntime")
            .field("startup_ready", &self.startup.ready())
            .field("workers", &self.workers)
            .finish()
    }
}

impl DaemonDiagnosticSource for DaemonApplicationRuntime {
    fn snapshot(
        &self,
        sections: &BTreeSet<DaemonDiagnosticSection>,
    ) -> Result<Vec<DaemonDiagnosticRow>, DaemonOperationsError> {
        let mut rows = self.workers.snapshot(sections)?;
        if sections.contains(&DaemonDiagnosticSection::Reconciliation) {
            let scan = undercurrent_application::OperationAttemptScan::without_exclusions(
                self.diagnostic_attempt_limit,
            )
            .map_err(|_| DaemonOperationsError::SourceInvariant)?;
            let pending = block_on(self.attempts.pending_reconciliation(&scan))
                .map_err(|_| DaemonOperationsError::SourceUnavailable)?;
            for attempt in pending {
                rows.push(DaemonDiagnosticRow {
                    section: DaemonDiagnosticSection::Reconciliation,
                    identity: attempt.id().as_str().to_owned(),
                    fields: BTreeMap::from([
                        (
                            DaemonDiagnosticField::Operation,
                            DaemonDiagnosticValue::Text(attempt.kind().as_str().to_owned()),
                        ),
                        (
                            DaemonDiagnosticField::SessionId,
                            DaemonDiagnosticValue::Text(
                                attempt
                                    .target_session()
                                    .map_or("host", |id| id.as_str())
                                    .to_owned(),
                            ),
                        ),
                        (
                            DaemonDiagnosticField::Unresolved,
                            DaemonDiagnosticValue::Boolean(true),
                        ),
                    ]),
                });
            }
            rows.push(DaemonDiagnosticRow {
                section: DaemonDiagnosticSection::Reconciliation,
                identity: "startup_reconciliation".to_owned(),
                fields: BTreeMap::from([
                    (
                        DaemonDiagnosticField::Items,
                        DaemonDiagnosticValue::Unsigned(
                            u64::try_from(self.startup.examined).unwrap_or(u64::MAX),
                        ),
                    ),
                    (
                        DaemonDiagnosticField::TerminalUnchanged,
                        DaemonDiagnosticValue::Unsigned(
                            u64::try_from(self.startup.terminal_unchanged).unwrap_or(u64::MAX),
                        ),
                    ),
                    (
                        DaemonDiagnosticField::RunningRebound,
                        DaemonDiagnosticValue::Unsigned(
                            u64::try_from(self.startup.running_rebound).unwrap_or(u64::MAX),
                        ),
                    ),
                    (
                        DaemonDiagnosticField::StartingCompleted,
                        DaemonDiagnosticValue::Unsigned(
                            u64::try_from(self.startup.starting_completed).unwrap_or(u64::MAX),
                        ),
                    ),
                    (
                        DaemonDiagnosticField::StoppingResumed,
                        DaemonDiagnosticValue::Unsigned(
                            u64::try_from(self.startup.stopping_resumed).unwrap_or(u64::MAX),
                        ),
                    ),
                    (
                        DaemonDiagnosticField::StoppingUnknownPreserved,
                        DaemonDiagnosticValue::Unsigned(
                            u64::try_from(self.startup.stopping_unknown_preserved)
                                .unwrap_or(u64::MAX),
                        ),
                    ),
                    (
                        DaemonDiagnosticField::ProcessExitsSettled,
                        DaemonDiagnosticValue::Unsigned(
                            u64::try_from(self.startup.process_exits_settled).unwrap_or(u64::MAX),
                        ),
                    ),
                    (
                        DaemonDiagnosticField::RestartJobsReenqueued,
                        DaemonDiagnosticValue::Unsigned(
                            u64::try_from(self.startup.restart_jobs_reenqueued).unwrap_or(u64::MAX),
                        ),
                    ),
                    (
                        DaemonDiagnosticField::Unresolved,
                        DaemonDiagnosticValue::Unsigned(
                            u64::try_from(self.startup.unresolved).unwrap_or(u64::MAX),
                        ),
                    ),
                    (
                        DaemonDiagnosticField::ScanCompleted,
                        DaemonDiagnosticValue::Boolean(self.startup.scan_completed),
                    ),
                ]),
            });
            rows.push(DaemonDiagnosticRow {
                section: DaemonDiagnosticSection::Reconciliation,
                identity: "operation_attempt_reconciliation".to_owned(),
                fields: BTreeMap::from([
                    (
                        DaemonDiagnosticField::Items,
                        DaemonDiagnosticValue::Unsigned(
                            u64::try_from(self.attempt_startup.inspected).unwrap_or(u64::MAX),
                        ),
                    ),
                    (
                        DaemonDiagnosticField::Unresolved,
                        DaemonDiagnosticValue::Unsigned(
                            u64::try_from(self.attempt_startup.unresolved.len())
                                .unwrap_or(u64::MAX),
                        ),
                    ),
                    (
                        DaemonDiagnosticField::ScanCompleted,
                        DaemonDiagnosticValue::Boolean(self.attempt_startup.complete),
                    ),
                ]),
            });
        }
        Ok(rows)
    }
}

fn reconcile_attempts(
    ports: &DaemonBackgroundPorts,
    limits: OperationAttemptReconciliationLimits,
) -> Result<OperationAttemptReconciliationReport, ReconcileOperationAttemptsError> {
    let core = &ports.core;
    block_on(async {
        let service = ReconcileOperationAttemptsService::new(
            core.attempts.as_ref(),
            core.attempt_observer.as_ref(),
            core.clock.as_ref(),
            limits,
        );
        let mut cursor = core
            .attempt_readiness
            .recovery_cursor
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        let mut report = service.execute_from(&mut cursor).await?;
        *core
            .attempt_readiness
            .recovery_cursor
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = cursor;
        let retired = retire_exited_input_attempts(core, &report.unresolved)
            .await
            .map_err(ReconcileOperationAttemptsError::Port)?;
        // Receipt release is post-commit and repeatable. Retry it independently
        // of session existence, including after cleanup or a daemon restart.
        for attempt in core
            .attempts
            .pending_input_retirement_acks(limits.batch_size())
            .await
            .map_err(ReconcileOperationAttemptsError::Port)?
        {
            if core
                .attempt_observer
                .acknowledge_observation(&attempt)
                .await
                .is_ok()
            {
                core.attempts
                    .acknowledge_input_retirement(&attempt)
                    .await
                    .map_err(ReconcileOperationAttemptsError::Port)?;
            }
        }
        if retired {
            report = service.execute().await?;
        }
        Ok(report)
    })
}

async fn retire_exited_input_attempts(
    core: &DaemonCorePorts,
    ids: &[undercurrent_domain::OperationAttemptId],
) -> Result<bool, undercurrent_application::PortError> {
    use undercurrent_domain::{EffectCertainty, OperationAttemptState, ProcessMutationKind};
    let mut changed = false;
    for id in ids {
        let Some(attempt) = core.attempts.load(id).await? else {
            continue;
        };
        if attempt.kind() != ProcessMutationKind::WriteInput
            || attempt.dispatch_binding().is_some()
            || !matches!(
                attempt.state(),
                OperationAttemptState::Settled {
                    certainty: EffectCertainty::Unknown,
                    ..
                }
            )
        {
            continue;
        }
        let Some(target) = attempt.target_session() else {
            continue;
        };
        // Serialize against restart/removal; the store also checks the exact revision.
        let _lease = core.coordinator.acquire(target).await?;
        let Some(session) = core.repository.load(target).await? else {
            continue;
        };
        if !session.lifecycle().is_terminal()
            || session.process().is_some()
            || core.processes.lookup(target)?.is_some()
        {
            continue;
        }
        let Ok(Some(evidence)) = core
            .attempt_observer
            .observe_input_retirement(&attempt)
            .await
        else {
            continue;
        };
        changed |= core
            .attempts
            .retire_input(&undercurrent_application::InputAttemptRetirement {
                attempt_id: attempt.id().clone(),
                fencing_token: attempt.fencing_token(),
                session_revision: session.revision(),
                evidence,
                retired_at: core.clock.now(),
            })
            .await?;
    }
    Ok(changed)
}

const fn attempt_reconciliation_category(
    error: &ReconcileOperationAttemptsError,
) -> ApplicationErrorCategory {
    match error {
        ReconcileOperationAttemptsError::Port(source) => source.category(),
        ReconcileOperationAttemptsError::InvalidBatch
        | ReconcileOperationAttemptsError::ReconciliationRejected => {
            ApplicationErrorCategory::InvariantViolation
        }
    }
}

fn reconcile_startup(
    ports: &DaemonBackgroundPorts,
    limits: StartupReconciliationLimits,
) -> Result<StartupReconciliationReport, StartupReconciliationError> {
    let span = tracing::span!(
        target: "undercurrent.background",
        tracing::Level::INFO,
        TraceSpanName::ReconcileStartup.as_str(),
    );
    let _entered = span.enter();
    let core = &ports.core;
    let workers = &ports.workers;
    let service = StartupReconciliationService::new(
        core.clock.as_ref(),
        workers.recovery.as_ref(),
        workers.process_truth.as_ref(),
        core.coordinator.as_ref(),
        core.repository.as_ref(),
        core.runtime.as_ref(),
        core.processes.as_ref(),
        core.restarts.scheduler.as_ref(),
        workers.observations.scheduler.as_ref(),
        core.projection.as_ref(),
        core.telemetry.as_ref(),
        limits,
    );
    block_on(service.execute())
}

fn worker_specifications(
    ports: Arc<DaemonBackgroundPorts>,
    config: DaemonApplicationRuntimeConfig,
) -> Result<Vec<DaemonWorkerSpec>, DaemonBackgroundWorkersCreateError> {
    let reload = ports.workers.profile_reload.clone();
    let mut specifications = vec![
        DaemonWorkerSpec::new(
            DaemonWorkerKind::AttemptReconciliation,
            config.intervals.observation,
            Arc::new(AttemptReconciliationTask {
                ports: Arc::clone(&ports),
                limits: config.attempt_reconciliation,
            }),
        )?
        .with_adaptive_empty_backoff(
            DATABASE_EMPTY_BACKOFF_CAP.max(config.intervals.observation),
        )?,
        DaemonWorkerSpec::new(
            DaemonWorkerKind::Outbox,
            config.intervals.outbox,
            Arc::new(OutboxTask {
                ports: Arc::clone(&ports),
                limits: config.outbox,
            }),
        )?
        .with_adaptive_empty_backoff(DATABASE_EMPTY_BACKOFF_CAP.max(config.intervals.outbox))?,
        DaemonWorkerSpec::new(
            DaemonWorkerKind::Restart,
            config.intervals.restart,
            Arc::new(RestartTask {
                ports: Arc::clone(&ports),
                batch_size: config.restart_batch_size,
                interrupted: AtomicBool::new(false),
            }),
        )?,
        DaemonWorkerSpec::new(
            DaemonWorkerKind::Observation,
            config.intervals.observation,
            Arc::new(ObservationTask {
                ports: Arc::clone(&ports),
                limits: config.observation,
                foreground: config.foreground,
                max_profiles: config.max_profiles,
            }),
        )?,
        DaemonWorkerSpec::new(
            DaemonWorkerKind::ProcessExit,
            config.intervals.process_exit_error,
            Arc::new(ProcessExitTask {
                ports,
                interrupted: AtomicBool::new(false),
                retry: Mutex::new(None),
            }),
        )?,
    ];
    if let Some(reload) = reload {
        specifications.push(DaemonWorkerSpec::new(
            DaemonWorkerKind::ProfileReload,
            Duration::from_secs(1),
            reload,
        )?);
    }
    Ok(specifications)
}

struct AttemptReconciliationTask {
    ports: Arc<DaemonBackgroundPorts>,
    limits: OperationAttemptReconciliationLimits,
}

impl DaemonBackgroundTask for AttemptReconciliationTask {
    fn execute(&self) -> Result<DaemonWorkerCycle, DaemonBackgroundTaskError> {
        match reconcile_attempts(&self.ports, self.limits) {
            Ok(report) => {
                self.ports
                    .core
                    .attempt_readiness
                    .apply_reconciliation(&report);
                if report.unresolved.is_empty() {
                    Ok(DaemonWorkerCycle::processed(
                        report.inspected,
                        !report.complete && report.resolved > 0,
                    ))
                } else {
                    Ok(DaemonWorkerCycle::with_failures(
                        report.inspected,
                        report.unresolved.len(),
                        Some(ApplicationErrorCategory::FailedPrecondition),
                        false,
                    ))
                }
            }
            Err(error) => {
                if matches!(&error, ReconcileOperationAttemptsError::Port(source)
                    if source.operation() == PortOperation::ObserveOperationAttemptEffect)
                {
                    self.ports.core.attempt_readiness.invalidate();
                } else {
                    self.ports.core.attempt_readiness.close();
                }
                Err(DaemonBackgroundTaskError::new(
                    attempt_reconciliation_category(&error),
                    error,
                ))
            }
        }
    }
}

struct OutboxTask {
    ports: Arc<DaemonBackgroundPorts>,
    limits: OutboxDispatchLimits,
}

impl DaemonBackgroundTask for OutboxTask {
    fn execute(&self) -> Result<DaemonWorkerCycle, DaemonBackgroundTaskError> {
        let core = &self.ports.core;
        let workers = &self.ports.workers;
        let service = DispatchOutboxService::new(
            core.clock.as_ref(),
            workers.outbox.as_ref(),
            core.repository.as_ref(),
            workers.events.as_ref(),
            workers.observations.scheduler.as_ref(),
            core.telemetry.as_ref(),
            self.limits,
        );
        block_on(service.execute())
            .map(|report| {
                DaemonWorkerCycle::processed(
                    report.processed,
                    report.read == self.limits.batch_size(),
                )
            })
            .map_err(|error| DaemonBackgroundTaskError::new(error.category(), error))
    }
}

struct RestartTask {
    ports: Arc<DaemonBackgroundPorts>,
    batch_size: usize,
    interrupted: AtomicBool,
}

impl DaemonBackgroundTask for RestartTask {
    fn interrupt(&self) {
        self.interrupted.store(true, Ordering::Release);
    }

    fn execute(&self) -> Result<DaemonWorkerCycle, DaemonBackgroundTaskError> {
        let _profiles = self
            .ports
            .workers
            .profile_reload
            .as_ref()
            .map(|reload| reload.gate.read().unwrap_or_else(|p| p.into_inner()));
        let core = &self.ports.core;
        let jobs = core
            .restarts
            .source
            .claim_due_restarts(core.clock.now(), self.batch_size)
            .map_err(|error| DaemonBackgroundTaskError::new(error.category(), error))?;
        let claimed = jobs.len();
        let service = RunScheduledRestartService::new(
            core.clock.as_ref(),
            core.coordinator.as_ref(),
            core.repository.as_ref(),
            core.runtime.as_ref(),
            core.processes.as_ref(),
            core.restarts.scheduler.as_ref(),
            core.projection.as_ref(),
            core.telemetry.as_ref(),
        );
        let mut failed = 0_usize;
        let mut last_failure = None;
        for job in jobs {
            let retry = job.clone();
            let Some(result) = crate::blocking::block_on_until(service.execute(job), || {
                self.interrupted.load(Ordering::Acquire)
            }) else {
                break;
            };
            if let Err(error) = result {
                failed = failed.saturating_add(1);
                last_failure = Some(error.category());
                if let Err(requeue) = block_on(core.restarts.scheduler.schedule(&retry)) {
                    last_failure = Some(requeue.category());
                    core.telemetry.emit_diagnostic(&DiagnosticRecord {
                        event: DiagnosticEventName::PortOperationFailed,
                        level: DiagnosticLevel::for_port_error(requeue.kind()),
                        timestamp: core.clock.now(),
                        session_id: Some(retry.session_id),
                        application_operation: Some(ApplicationOperation::RunScheduledRestart),
                        application_error: Some(requeue.category()),
                        operation: Some(PortOperation::ScheduleRestart),
                        error_kind: Some(requeue.kind()),
                    });
                }
            }
        }
        if failed == 0 {
            Ok(DaemonWorkerCycle::processed(
                claimed,
                claimed == self.batch_size,
            ))
        } else {
            Ok(DaemonWorkerCycle::with_failures(
                claimed,
                failed,
                last_failure,
                false,
            ))
        }
    }
}

struct ObservationTask {
    ports: Arc<DaemonBackgroundPorts>,
    limits: ObservationDispatchLimits,
    foreground: ForegroundDecisionPolicy,
    max_profiles: usize,
}

impl DaemonBackgroundTask for ObservationTask {
    fn execute(&self) -> Result<DaemonWorkerCycle, DaemonBackgroundTaskError> {
        let _profiles = self
            .ports
            .workers
            .profile_reload
            .as_ref()
            .map(|reload| reload.gate.read().unwrap_or_else(|p| p.into_inner()));
        let core = &self.ports.core;
        let workers = &self.ports.workers;
        let foreground = ObserveForegroundService::new(
            core.clock.as_ref(),
            core.coordinator.as_ref(),
            core.repository.as_ref(),
            core.projection.as_ref(),
            core.processes.as_ref(),
            core.effective_profiles.as_ref(),
            workers.foreground_processes.as_ref(),
            workers.profiles.as_ref(),
            workers.patterns.as_ref(),
            self.max_profiles,
            workers.foreground.as_ref(),
            core.telemetry.as_ref(),
            self.foreground,
        );
        let health = ObserveHealthService::new(
            core.clock.as_ref(),
            core.coordinator.as_ref(),
            core.repository.as_ref(),
            core.projection.as_ref(),
            core.processes.as_ref(),
            workers.health.as_ref(),
            core.telemetry.as_ref(),
        );
        let router = ObservationServiceRouter::new(&foreground, &health);
        let service = DispatchObservationsService::new(
            core.clock.as_ref(),
            workers.observations.source.as_ref(),
            workers.observations.scheduler.as_ref(),
            &router,
            core.telemetry.as_ref(),
            self.limits,
        );
        block_on(service.execute())
            .map(|report| {
                if report.issue_count == 0 {
                    DaemonWorkerCycle::processed(
                        report.claimed,
                        report.claimed == self.limits.batch_size(),
                    )
                } else {
                    DaemonWorkerCycle::with_failures(
                        report.claimed,
                        report.issue_count,
                        report
                            .issues
                            .last()
                            .map(|issue| observation_failure_category(issue.failure)),
                        report.claimed == self.limits.batch_size(),
                    )
                }
            })
            .map_err(|error| DaemonBackgroundTaskError::new(error.category(), error))
    }
}

fn observation_failure_category(failure: ObservationDispatchFailure) -> ApplicationErrorCategory {
    match failure {
        ObservationDispatchFailure::Application(category) => category,
        ObservationDispatchFailure::Infrastructure { kind, .. } => kind.application_category(),
    }
}

struct ProcessExitTask {
    ports: Arc<DaemonBackgroundPorts>,
    interrupted: AtomicBool,
    retry: Mutex<Option<ProcessExitObservation>>,
}

impl DaemonBackgroundTask for ProcessExitTask {
    fn execute(&self) -> Result<DaemonWorkerCycle, DaemonBackgroundTaskError> {
        // Receiving removes an exit from the source. Keep retrying that exact
        // observation until settlement succeeds, or a transient service failure
        // would discard it when the worker advances to the next receive.
        // See [process-exit work](../docs/internals/lifecycle/background-work.md#fixed-owners-and-independent-work).
        let observation = self
            .retry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let observation = match observation {
            Some(observation) => observation,
            None => match block_on(self.ports.workers.process_exits.source.next_exit()) {
                Ok(observation) => observation,
                Err(_) if self.interrupted.load(Ordering::Acquire) => {
                    return Ok(DaemonWorkerCycle::idle());
                }
                Err(error) => {
                    return Err(DaemonBackgroundTaskError::new(error.category(), error));
                }
            },
        };
        let retry = observation.clone();
        let core = &self.ports.core;
        let service = ObserveProcessExitService::new(
            core.clock.as_ref(),
            core.coordinator.as_ref(),
            core.repository.as_ref(),
            core.processes.as_ref(),
            core.restarts.scheduler.as_ref(),
            core.projection.as_ref(),
            core.telemetry.as_ref(),
        );
        match block_on(service.execute(observation)) {
            Ok(_) => Ok(DaemonWorkerCycle::processed(1, true)),
            Err(error) => {
                *self
                    .retry
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(retry);
                Err(DaemonBackgroundTaskError::new(error.category(), error))
            }
        }
    }

    fn interrupt(&self) {
        self.interrupted.store(true, Ordering::Release);
        self.ports
            .workers
            .process_exits
            .interrupt
            .interrupt_process_exits();
    }
}

#[cfg(test)]
mod attempt_readiness_tests {
    use undercurrent_domain::OperationAttemptId;

    use super::*;

    #[test]
    fn mutation_gate_opens_only_for_complete_reconciliation_or_zero_exact_unresolved_count() {
        let gate = OperationAttemptReadinessGate::new();
        assert!(!gate.is_ready());

        gate.apply_reconciliation(&OperationAttemptReconciliationReport {
            inspected: 1,
            resolved: 0,
            unresolved: vec![OperationAttemptId::new("unknown-1").expect("attempt ID")],
            complete: false,
        });
        assert!(!gate.is_ready());

        gate.apply_reconciliation(&OperationAttemptReconciliationReport {
            inspected: 0,
            resolved: 0,
            unresolved: Vec::new(),
            complete: true,
        });
        assert!(gate.is_ready());

        gate.apply_unresolved_count(1);
        assert!(!gate.is_ready());
        gate.apply_unresolved_count(0);
        assert!(gate.is_ready());
        gate.close();
        assert!(!gate.is_ready());
    }
}
