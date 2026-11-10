// SPDX-License-Identifier: Apache-2.0

//! Bounded daemon-start reconciliation of durable and live process truth.

use std::error::Error;
use std::fmt::{Display, Formatter};

use undercurrent_domain::{
    LifecycleState, ProcessIdentity, ProcessMutationKind, ProcessOutcome, ProcessTruth,
    ProcessTruthKind, Session, SessionId, TimestampMillis,
};

use super::{
    MutationCore, ObserveProcessExitResult, ObserveProcessExitService, SessionMutationError,
    StopMode, StopSessionCommand, StopSessionService, enqueue_restart, not_before,
};
use crate::{
    ApplicationErrorCategory, ApplicationOperation, Clock, DiagnosticEventName, DiagnosticLevel,
    MetricName, MetricObservation, ObservationScheduler, PortError, PortErrorKind, PortOperation,
    ProcessExitObservation, ProcessInstance, ProcessRuntime, RecoverySessionBatch,
    RestartScheduler, RuntimeCancellationMode, RuntimeCancellationRecovery,
    SessionMutationCoordinator, SessionObservationPlan, SessionProcessDirectory,
    SessionProjectionWriter, SessionRecoverySource, SessionRepository, Telemetry,
    process_mutation_admission,
};

const MAX_SESSION_REVALIDATIONS: usize = 3;

/// Validated memory bounds for one startup recovery scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartupReconciliationLimits {
    batch_size: usize,
    max_retained_issues: usize,
}

impl StartupReconciliationLimits {
    /// Validates recovery page and retained-diagnostic bounds.
    ///
    /// `max_retained_issues` may be zero when operators only need aggregate
    /// counters. This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument error when `batch_size` is zero.
    pub const fn new(
        batch_size: usize,
        max_retained_issues: usize,
    ) -> Result<Self, InvalidStartupReconciliationLimits> {
        if batch_size == 0 {
            return Err(InvalidStartupReconciliationLimits);
        }
        Ok(Self {
            batch_size,
            max_retained_issues,
        })
    }

    /// Returns the maximum durable aggregates requested in one source call.
    pub const fn batch_size(self) -> usize {
        self.batch_size
    }

    /// Returns the maximum number of payload-safe per-session issues retained.
    pub const fn max_retained_issues(self) -> usize {
        self.max_retained_issues
    }
}

/// Zero-sized validation failure for startup recovery limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidStartupReconciliationLimits;

impl Display for InvalidStartupReconciliationLimits {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("startup reconciliation batch size must be non-zero")
    }
}

impl Error for InvalidStartupReconciliationLimits {}

/// Stable stage at which one durable session could not be reconciled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupRecoveryStage {
    /// Authoritative process incarnation truth could not be observed.
    ObserveProcessTruth,
    /// Current in-memory process ownership could not be inspected.
    InspectProcessDirectory,
    /// Durable live ownership could not be restored in the process directory.
    RestoreProcessBinding,
    /// Exit publication could not be restored for a live incarnation.
    ArmProcessExit,
    /// An unchanged durable session could not be restored to prepared reads.
    RestoreProjection,
    /// Current session observation work could not be rebuilt after daemon restart.
    RestoreObservationPlan,
    /// A live process could not complete an interrupted starting transition.
    CompleteStarting,
    /// A durable stop intent could not be resumed.
    ResumeStopping,
    /// An observed process exit could not be durably settled.
    SettleProcessExit,
    /// Active durable state without process ownership could not be settled.
    SettleMissingOwnership,
    /// Durable restart intent could not be re-enqueued.
    EnqueueRestart,
    /// State changed during every bounded revalidation attempt.
    RevalidateState,
}

/// Payload-safe reason one session remains unresolved after startup recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupRecoveryFailure {
    /// A concrete adapter operation failed.
    Infrastructure {
        /// Stable failed port operation.
        operation: PortOperation,
        /// Stable infrastructure failure kind.
        kind: PortErrorKind,
    },
    /// An application or domain use case rejected reconciliation.
    Application {
        /// Stable presentation-safe application category.
        category: ApplicationErrorCategory,
    },
    /// Another exact process incarnation already owns the directory slot.
    ProcessBindingConflict,
    /// Process truth was explicitly orphaned or unknown and was not guessed.
    UnresolvedProcessTruth {
        /// Payload-safe truth kind.
        truth: ProcessTruthKind,
    },
    /// An admitted cancellation has an Unknown process effect and cannot be redriven.
    RuntimeCancellationEffectUnknown,
    /// Bounded state revalidation could not reach a stable snapshot.
    StateChangedRepeatedly,
}

/// One bounded payload-safe startup recovery issue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupRecoveryIssue {
    /// Stable affected session identity.
    pub session_id: SessionId,
    /// Stable failed reconciliation stage.
    pub stage: StartupRecoveryStage,
    /// Payload-safe failure classification.
    pub failure: StartupRecoveryFailure,
}

/// Aggregate result of scanning all durable sessions during daemon startup.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StartupReconciliationReport {
    /// Number of durable sessions examined.
    pub examined: usize,
    /// Terminal sessions that required no live-process recovery.
    pub terminal_unchanged: usize,
    /// Running sessions whose exact process binding was restored or retained.
    pub running_rebound: usize,
    /// Starting sessions advanced to running after exact liveness confirmation.
    pub starting_completed: usize,
    /// Stopping sessions whose pending termination was resumed.
    pub stopping_resumed: usize,
    /// Stopping sessions kept operable without redriving an Unknown cancellation effect.
    pub stopping_unknown_preserved: usize,
    /// Active sessions settled from exited, missing, or absent process truth.
    pub process_exits_settled: usize,
    /// Durable scheduled restart jobs accepted by the scheduler.
    pub restart_jobs_reenqueued: usize,
    /// Sessions that remain unresolved and keep readiness closed.
    pub unresolved: usize,
    /// Payload-safe issues retained up to the configured bound.
    pub issues: Vec<StartupRecoveryIssue>,
    /// Whether the recovery source reached a valid end-of-scan page.
    pub scan_completed: bool,
}

impl StartupReconciliationReport {
    /// Returns whether the full scan completed without unresolved sessions.
    ///
    /// This is the startup readiness gate. It performs no IO and does not panic.
    pub const fn ready(&self) -> bool {
        self.scan_completed && self.unresolved == 0
    }

    fn record_action(&mut self, action: ReconciliationAction) {
        match action {
            ReconciliationAction::Terminal { restart_enqueued } => {
                self.terminal_unchanged = self.terminal_unchanged.saturating_add(1);
                if restart_enqueued {
                    self.restart_jobs_reenqueued = self.restart_jobs_reenqueued.saturating_add(1);
                }
            }
            ReconciliationAction::RunningRebound => {
                self.running_rebound = self.running_rebound.saturating_add(1);
            }
            ReconciliationAction::StartingCompleted => {
                self.starting_completed = self.starting_completed.saturating_add(1);
            }
            ReconciliationAction::StoppingResumed => {
                self.stopping_resumed = self.stopping_resumed.saturating_add(1);
            }
            ReconciliationAction::StoppingUnknownPreserved => {
                self.stopping_unknown_preserved = self.stopping_unknown_preserved.saturating_add(1);
            }
            ReconciliationAction::ExitSettled { restart_enqueued } => {
                self.process_exits_settled = self.process_exits_settled.saturating_add(1);
                if restart_enqueued {
                    self.restart_jobs_reenqueued = self.restart_jobs_reenqueued.saturating_add(1);
                }
            }
        }
    }

    fn record_issue(&mut self, issue: StartupRecoveryIssue, max_retained_issues: usize) {
        self.unresolved = self.unresolved.saturating_add(1);
        if self.issues.len() < max_retained_issues {
            self.issues.push(issue);
        }
    }
}

/// Contract violation returned by a recovery-page adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryBatchViolation {
    /// Adapter returned more aggregates than requested.
    LimitExceeded,
    /// Aggregate IDs were not strictly increasing after the supplied cursor.
    SessionOrder,
    /// A continuation cursor was returned without any aggregate.
    EmptyContinuation,
    /// Continuation cursor did not equal the final aggregate ID.
    CursorMismatch,
}

impl Display for RecoveryBatchViolation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::LimitExceeded => "recovery batch exceeded its requested limit",
            Self::SessionOrder => "recovery session IDs were not strictly ordered",
            Self::EmptyContinuation => "empty recovery batch contained a continuation cursor",
            Self::CursorMismatch => "recovery cursor did not match the final session ID",
        })
    }
}

/// Fatal failure that prevents completing the startup readiness scan.
#[derive(Debug)]
pub enum StartupReconciliationError {
    /// Durable recovery source could not supply the next bounded page.
    RecoverySource {
        /// Concrete source failure.
        source: PortError,
        /// Aggregate progress retained before the fatal failure.
        partial: Box<StartupReconciliationReport>,
    },
    /// Recovery source returned a page that violates its ordering contract.
    InvalidBatch {
        /// Stable page-contract violation.
        violation: RecoveryBatchViolation,
        /// Aggregate progress retained before the fatal failure.
        partial: Box<StartupReconciliationReport>,
    },
}

impl StartupReconciliationError {
    /// Returns the incomplete report retained at the fatal scan boundary.
    pub const fn partial_report(&self) -> &StartupReconciliationReport {
        match self {
            Self::RecoverySource { partial, .. } | Self::InvalidBatch { partial, .. } => partial,
        }
    }

    /// Returns the stable presentation category for this fatal failure.
    pub const fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::RecoverySource { source, .. } => source.category(),
            Self::InvalidBatch { .. } => ApplicationErrorCategory::InvariantViolation,
        }
    }
}

impl Display for StartupReconciliationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RecoverySource { source, .. } => {
                write!(formatter, "startup recovery source failed: {source}")
            }
            Self::InvalidBatch { violation, .. } => Display::fmt(violation, formatter),
        }
    }
}

impl Error for StartupReconciliationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::RecoverySource { source, .. } => Some(source),
            Self::InvalidBatch { .. } => None,
        }
    }
}

/// Reconciles durable session state before opening daemon readiness.
pub struct StartupReconciliationService<'a> {
    core: MutationCore<'a>,
    recovery: &'a dyn SessionRecoverySource,
    process_truth: &'a dyn crate::ProcessTruthSource,
    runtime: &'a dyn ProcessRuntime,
    processes: &'a dyn SessionProcessDirectory,
    restarts: &'a dyn RestartScheduler,
    observations: &'a dyn ObservationScheduler,
    coordinator: &'a dyn SessionMutationCoordinator,
    repository: &'a dyn SessionRepository,
    projection: &'a dyn SessionProjectionWriter,
    telemetry: &'a dyn Telemetry,
    limits: StartupReconciliationLimits,
}

impl<'a> StartupReconciliationService<'a> {
    /// Constructs a bounded startup reconciler from explicit inward-facing ports.
    ///
    /// Construction performs no IO and does not panic.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        clock: &'a dyn Clock,
        recovery: &'a dyn SessionRecoverySource,
        process_truth: &'a dyn crate::ProcessTruthSource,
        coordinator: &'a dyn SessionMutationCoordinator,
        repository: &'a dyn SessionRepository,
        runtime: &'a dyn ProcessRuntime,
        processes: &'a dyn SessionProcessDirectory,
        restarts: &'a dyn RestartScheduler,
        observations: &'a dyn ObservationScheduler,
        projection: &'a dyn SessionProjectionWriter,
        telemetry: &'a dyn Telemetry,
        limits: StartupReconciliationLimits,
    ) -> Self {
        Self {
            core: MutationCore::new(
                clock,
                coordinator,
                repository,
                projection,
                telemetry,
                ApplicationOperation::ReconcileStartup,
            ),
            recovery,
            process_truth,
            runtime,
            processes,
            restarts,
            observations,
            coordinator,
            repository,
            projection,
            telemetry,
            limits,
        }
    }

    /// Scans every durable session in bounded stable-ID pages and restores truth.
    ///
    /// The service never opens readiness from a partial scan. Individual
    /// session failures are retained as bounded payload-safe issues while the
    /// remaining sessions continue. Page-source failures and malformed pages
    /// abort with the incomplete aggregate report. Process truth is observed
    /// before taking a session mutation lease, and every later mutation
    /// revalidates durable incarnation identity. This method performs storage,
    /// process, scheduling, projection, and telemetry IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a fatal typed error when the recovery source fails or violates
    /// its bounded ordered-page contract.
    pub async fn execute(&self) -> Result<StartupReconciliationReport, StartupReconciliationError> {
        let started_at = self.core.clock.now();
        self.core.emit(
            DiagnosticEventName::StartupReconciliationStarted,
            DiagnosticLevel::Info,
            started_at,
            None,
            None,
            None,
        );
        let mut report = StartupReconciliationReport::default();
        let mut after = None;

        loop {
            let batch = match self
                .recovery
                .read_batch(after.as_ref(), self.limits.batch_size())
                .await
            {
                Ok(batch) => batch,
                Err(source) => {
                    self.core
                        .emit_port_failure(self.core.clock.now(), None, &source);
                    self.emit_fatal(&report, started_at);
                    return Err(StartupReconciliationError::RecoverySource {
                        source,
                        partial: Box::new(report),
                    });
                }
            };
            if let Err(violation) = validate_batch(&batch, after.as_ref(), self.limits.batch_size())
            {
                self.emit_fatal(&report, started_at);
                return Err(StartupReconciliationError::InvalidBatch {
                    violation,
                    partial: Box::new(report),
                });
            }
            self.core.emit(
                DiagnosticEventName::StartupRecoveryBatchLoaded,
                DiagnosticLevel::Debug,
                self.core.clock.now(),
                None,
                Some(PortOperation::ReadRecoverySessions),
                None,
            );

            for session in batch.sessions {
                report.examined = report.examined.saturating_add(1);
                let session_id = session.id().clone();
                match self.reconcile_session(session).await {
                    Ok(action) => {
                        report.record_action(action);
                        self.core.emit(
                            DiagnosticEventName::StartupSessionReconciled,
                            DiagnosticLevel::Debug,
                            self.core.clock.now(),
                            Some(session_id),
                            None,
                            None,
                        );
                    }
                    Err(failure) => {
                        let issue = failure.into_issue(session_id.clone());
                        self.core.emit(
                            DiagnosticEventName::StartupSessionReconciliationFailed,
                            DiagnosticLevel::Error,
                            self.core.clock.now(),
                            Some(session_id),
                            issue.failure.operation(),
                            issue.failure.error_kind(),
                        );
                        report.record_issue(issue, self.limits.max_retained_issues());
                    }
                }
            }

            match batch.next_after {
                Some(next_after) => after = Some(next_after),
                None => break,
            }
        }

        report.scan_completed = true;
        self.finish_metrics(&report, started_at);
        self.core.emit(
            DiagnosticEventName::StartupReconciliationCompleted,
            if report.ready() {
                DiagnosticLevel::Info
            } else {
                DiagnosticLevel::Warn
            },
            self.core.clock.now(),
            None,
            None,
            None,
        );
        Ok(report)
    }

    async fn reconcile_session(
        &self,
        mut session: Session,
    ) -> Result<ReconciliationAction, SessionRecoveryFailure> {
        for _ in 0..MAX_SESSION_REVALIDATIONS {
            if session.lifecycle().is_terminal() {
                let restart_enqueued = enqueue_restart(&self.core, self.restarts, &session)
                    .await
                    .map_err(|failure| {
                    SessionRecoveryFailure::port(
                        StartupRecoveryStage::EnqueueRestart,
                        failure.source,
                    )
                })?;
                self.restore_projection(&session)?;
                return Ok(ReconciliationAction::Terminal { restart_enqueued });
            }

            let Some(process) = session.process().cloned() else {
                match self.settle_missing_ownership(session.id()).await? {
                    ReconciliationAttempt::Resolved(action) => return Ok(action),
                    ReconciliationAttempt::Retry(current) => {
                        session = *current;
                        continue;
                    }
                }
            };
            let observed = self
                .process_truth
                .observe(session.id(), &process)
                .await
                .map_err(|error| {
                    SessionRecoveryFailure::port(StartupRecoveryStage::ObserveProcessTruth, error)
                })?;
            match self
                .reconcile_observed(session.id(), &process, observed)
                .await?
            {
                ReconciliationAttempt::Resolved(action) => return Ok(action),
                ReconciliationAttempt::Retry(current) => session = *current,
            }
        }
        Err(SessionRecoveryFailure {
            stage: StartupRecoveryStage::RevalidateState,
            failure: StartupRecoveryFailure::StateChangedRepeatedly,
        })
    }

    async fn reconcile_observed(
        &self,
        session_id: &SessionId,
        expected_process: &ProcessIdentity,
        observed: ProcessTruth,
    ) -> Result<ReconciliationAttempt, SessionRecoveryFailure> {
        let (lease, mut session) = self.core.begin(session_id).await.map_err(|error| {
            SessionRecoveryFailure::application(
                StartupRecoveryStage::RevalidateState,
                error.category(),
            )
        })?;
        if session.lifecycle().is_terminal() || session.process() != Some(expected_process) {
            return Ok(ReconciliationAttempt::Retry(Box::new(session)));
        }

        self.restore_binding(&session, expected_process)?;
        match observed {
            ProcessTruth::Running { observed_at } => {
                if matches!(
                    session.lifecycle(),
                    LifecycleState::Starting | LifecycleState::Running
                ) {
                    let arm_payload = format!("{:?}:startup", expected_process.id());
                    let arm_admission = process_mutation_admission(
                        None,
                        session_id.clone(),
                        ProcessMutationKind::ArmExit,
                        arm_payload.as_bytes(),
                        self.core.clock.now(),
                    );
                    self.runtime
                        .arm_exit_admitted(&arm_admission, session_id, expected_process.id())
                        .await
                        .map_err(|error| {
                            SessionRecoveryFailure::port(
                                StartupRecoveryStage::ArmProcessExit,
                                error,
                            )
                        })?;
                }
                match session.lifecycle() {
                    LifecycleState::Starting => {
                        let expected_revision = session.revision();
                        let changed_at = not_before(observed_at, session.updated_at());
                        let change = session
                            .transition_lifecycle(LifecycleState::Running, changed_at)
                            .map_err(SessionMutationError::Domain)
                            .map_err(|error| {
                                SessionRecoveryFailure::application(
                                    StartupRecoveryStage::CompleteStarting,
                                    error.category(),
                                )
                            })?
                            .ok_or(SessionRecoveryFailure {
                                stage: StartupRecoveryStage::CompleteStarting,
                                failure: StartupRecoveryFailure::Application {
                                    category: ApplicationErrorCategory::InvariantViolation,
                                },
                            })?;
                        self.core
                            .commit(&session, expected_revision, change, changed_at)
                            .await
                            .map_err(|error| {
                                SessionRecoveryFailure::application(
                                    StartupRecoveryStage::CompleteStarting,
                                    error.category(),
                                )
                            })?;
                        self.restore_observation_plan(&session).await?;
                        Ok(ReconciliationAttempt::Resolved(
                            ReconciliationAction::StartingCompleted,
                        ))
                    }
                    LifecycleState::Running => {
                        self.restore_projection(&session)?;
                        self.restore_observation_plan(&session).await?;
                        Ok(ReconciliationAttempt::Resolved(
                            ReconciliationAction::RunningRebound,
                        ))
                    }
                    LifecycleState::Stopping => {
                        // A separate retained Force control can authorize force even
                        // when the session carries an earlier no-escalation policy.
                        // Otherwise recovery must preserve cooperative-only stopping.
                        let mode = match self
                            .recovery
                            .claim_runtime_cancellation_recovery(session_id, self.core.clock.now())
                            .await
                            .map_err(|error| {
                                SessionRecoveryFailure::port(
                                    StartupRecoveryStage::ResumeStopping,
                                    error,
                                )
                            })? {
                            Some(RuntimeCancellationRecovery::Resume(
                                RuntimeCancellationMode::Force,
                            )) => StopMode::Force,
                            Some(RuntimeCancellationRecovery::Resume(
                                RuntimeCancellationMode::Graceful,
                            )) if !session.configuration().execution().stop().no_escalation() => {
                                StopMode::GracefulOnly
                            }
                            None if !session.configuration().execution().stop().no_escalation() => {
                                StopMode::Graceful
                            }
                            Some(RuntimeCancellationRecovery::PreserveUnknown)
                            | Some(RuntimeCancellationRecovery::Resume(
                                RuntimeCancellationMode::Graceful,
                            ))
                            | None => {
                                self.restore_projection(&session)?;
                                self.restore_observation_plan(&session).await?;
                                return Ok(ReconciliationAttempt::Resolved(
                                    ReconciliationAction::StoppingUnknownPreserved,
                                ));
                            }
                        };
                        drop(lease);
                        self.stop_service()
                            .execute(StopSessionCommand {
                                expected_revision: None,
                                session_id: session_id.clone(),
                                mode,
                            })
                            .await
                            .map_err(|error| {
                                SessionRecoveryFailure::application(
                                    StartupRecoveryStage::ResumeStopping,
                                    error.category(),
                                )
                            })?;
                        Ok(ReconciliationAttempt::Resolved(
                            ReconciliationAction::StoppingResumed,
                        ))
                    }
                    LifecycleState::Stopped
                    | LifecycleState::Exited(_)
                    | LifecycleState::Failed => Ok(ReconciliationAttempt::Retry(Box::new(session))),
                }
            }
            ProcessTruth::Exited {
                exit_code,
                observed_at,
            } => {
                drop(lease);
                self.settle_observed_exit(session, ProcessOutcome::Exited(exit_code), observed_at)
                    .await
            }
            ProcessTruth::Missing { observed_at } => {
                drop(lease);
                self.settle_observed_exit(session, ProcessOutcome::SupervisionFailed, observed_at)
                    .await
            }
            ProcessTruth::Lost { observed_at } => {
                drop(lease);
                self.settle_observed_exit(session, ProcessOutcome::SupervisionFailed, observed_at)
                    .await
            }
            ProcessTruth::Orphaned { .. } | ProcessTruth::Unknown { .. } => {
                Err(SessionRecoveryFailure {
                    stage: StartupRecoveryStage::ObserveProcessTruth,
                    failure: StartupRecoveryFailure::UnresolvedProcessTruth {
                        truth: observed.kind(),
                    },
                })
            }
        }
    }

    async fn settle_missing_ownership(
        &self,
        session_id: &SessionId,
    ) -> Result<ReconciliationAttempt, SessionRecoveryFailure> {
        let (lease, mut session) = self.core.begin(session_id).await.map_err(|error| {
            SessionRecoveryFailure::application(
                StartupRecoveryStage::SettleMissingOwnership,
                error.category(),
            )
        })?;
        if session.lifecycle().is_terminal() || session.process().is_some() {
            return Ok(ReconciliationAttempt::Retry(Box::new(session)));
        }
        let expected_revision = session.revision();
        let changed_at = not_before(self.core.clock.now(), session.updated_at());
        let lifecycle_change = session
            .transition_lifecycle(LifecycleState::Failed, changed_at)
            .map_err(SessionMutationError::Domain)
            .map_err(|error| {
                SessionRecoveryFailure::application(
                    StartupRecoveryStage::SettleMissingOwnership,
                    error.category(),
                )
            })?
            .ok_or(SessionRecoveryFailure {
                stage: StartupRecoveryStage::SettleMissingOwnership,
                failure: StartupRecoveryFailure::Application {
                    category: ApplicationErrorCategory::InvariantViolation,
                },
            })?;
        let (_, restart_change) = session
            .observe_process_outcome(
                ProcessOutcome::SupervisionFailed,
                Default::default(),
                changed_at,
            )
            .map_err(SessionMutationError::Domain)
            .map_err(|error| {
                SessionRecoveryFailure::application(
                    StartupRecoveryStage::SettleMissingOwnership,
                    error.category(),
                )
            })?;
        let restart_change = restart_change.ok_or(SessionRecoveryFailure {
            stage: StartupRecoveryStage::SettleMissingOwnership,
            failure: StartupRecoveryFailure::Application {
                category: ApplicationErrorCategory::InvariantViolation,
            },
        })?;
        self.core
            .commit_changes(
                &session,
                expected_revision,
                &[lifecycle_change, restart_change],
                changed_at,
            )
            .await
            .map_err(|error| {
                SessionRecoveryFailure::application(
                    StartupRecoveryStage::SettleMissingOwnership,
                    error.category(),
                )
            })?;
        drop(lease);
        let restart_enqueued = enqueue_restart(&self.core, self.restarts, &session)
            .await
            .map_err(|failure| {
                SessionRecoveryFailure::port(StartupRecoveryStage::EnqueueRestart, failure.source)
            })?;
        Ok(ReconciliationAttempt::Resolved(
            ReconciliationAction::ExitSettled { restart_enqueued },
        ))
    }

    async fn settle_observed_exit(
        &self,
        session: Session,
        outcome: ProcessOutcome,
        observed_at: TimestampMillis,
    ) -> Result<ReconciliationAttempt, SessionRecoveryFailure> {
        let process_id = session
            .process()
            .map(|process| process.id().clone())
            .ok_or(SessionRecoveryFailure {
                stage: StartupRecoveryStage::SettleProcessExit,
                failure: StartupRecoveryFailure::Application {
                    category: ApplicationErrorCategory::InvariantViolation,
                },
            })?;
        let result = self
            .exit_service()
            .execute(ProcessExitObservation {
                session_id: session.id().clone(),
                process_id,
                outcome,
                observed_at,
            })
            .await
            .map_err(|error| {
                SessionRecoveryFailure::application(
                    StartupRecoveryStage::SettleProcessExit,
                    error.category(),
                )
            })?;
        match result {
            ObserveProcessExitResult::Committed {
                restart_enqueued, ..
            }
            | ObserveProcessExitResult::Recovered {
                restart_enqueued, ..
            } => Ok(ReconciliationAttempt::Resolved(
                ReconciliationAction::ExitSettled { restart_enqueued },
            )),
            ObserveProcessExitResult::Ignored { .. } => {
                Ok(ReconciliationAttempt::Retry(Box::new(session)))
            }
        }
    }

    fn restore_binding(
        &self,
        session: &Session,
        durable: &ProcessIdentity,
    ) -> Result<(), SessionRecoveryFailure> {
        match self.processes.lookup(session.id()) {
            Ok(Some(process)) if process.matches(durable) => return Ok(()),
            Ok(Some(_)) => {
                return Err(SessionRecoveryFailure {
                    stage: StartupRecoveryStage::InspectProcessDirectory,
                    failure: StartupRecoveryFailure::ProcessBindingConflict,
                });
            }
            Ok(None) => {}
            Err(error) => {
                return Err(SessionRecoveryFailure::port(
                    StartupRecoveryStage::InspectProcessDirectory,
                    error,
                ));
            }
        }

        let process = ProcessInstance {
            id: durable.id().clone(),
            started_at: durable.started_at(),
        };
        if let Err(source) = self.processes.bind(session.id(), process) {
            match self.processes.lookup(session.id()) {
                Ok(Some(current)) if current.matches(durable) => return Ok(()),
                Ok(Some(_)) => {
                    return Err(SessionRecoveryFailure {
                        stage: StartupRecoveryStage::RestoreProcessBinding,
                        failure: StartupRecoveryFailure::ProcessBindingConflict,
                    });
                }
                Ok(None) => {}
                Err(error) => {
                    return Err(SessionRecoveryFailure::port(
                        StartupRecoveryStage::InspectProcessDirectory,
                        error,
                    ));
                }
            }
            return Err(SessionRecoveryFailure::port(
                StartupRecoveryStage::RestoreProcessBinding,
                source,
            ));
        }
        Ok(())
    }

    fn restore_projection(&self, session: &Session) -> Result<(), SessionRecoveryFailure> {
        self.projection.upsert_session(session).map_err(|error| {
            SessionRecoveryFailure::port(StartupRecoveryStage::RestoreProjection, error)
        })
    }

    async fn restore_observation_plan(
        &self,
        session: &Session,
    ) -> Result<(), SessionRecoveryFailure> {
        let running = session.lifecycle() == LifecycleState::Running;
        let now = self.core.clock.now();
        let plan = SessionObservationPlan {
            session_id: session.id().clone(),
            expected_revision: session.revision(),
            foreground_not_before: (running
                && session.configuration().execution().detection_enabled())
            .then_some(now),
            health_not_before: (running && !session.configuration().health_rules().is_empty())
                .then_some(now),
        };
        self.observations
            .reconcile_session_observations(&plan)
            .await
            .map(|_| ())
            .map_err(|error| {
                SessionRecoveryFailure::port(StartupRecoveryStage::RestoreObservationPlan, error)
            })
    }

    fn stop_service(&self) -> StopSessionService<'_> {
        StopSessionService::new(
            self.core.clock,
            self.coordinator,
            self.repository,
            self.runtime,
            self.processes,
            self.restarts,
            self.projection,
            self.telemetry,
        )
    }

    fn exit_service(&self) -> ObserveProcessExitService<'_> {
        ObserveProcessExitService::new(
            self.core.clock,
            self.coordinator,
            self.repository,
            self.processes,
            self.restarts,
            self.projection,
            self.telemetry,
        )
    }

    fn emit_fatal(&self, report: &StartupReconciliationReport, started_at: TimestampMillis) {
        self.finish_metrics(report, started_at);
        self.core.emit(
            DiagnosticEventName::StartupReconciliationFailed,
            DiagnosticLevel::Error,
            self.core.clock.now(),
            None,
            None,
            None,
        );
    }

    fn finish_metrics(&self, report: &StartupReconciliationReport, started_at: TimestampMillis) {
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::StartupSessionsExamined,
            value: report.examined as f64,
            operation: Some(ApplicationOperation::ReconcileStartup),
        });
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::StartupSessionsUnresolved,
            value: report.unresolved as f64,
            operation: Some(ApplicationOperation::ReconcileStartup),
        });
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::OperationDurationSeconds,
            value: self
                .core
                .clock
                .now()
                .saturating_duration_since(started_at)
                .as_secs_f64(),
            operation: Some(ApplicationOperation::ReconcileStartup),
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconciliationAction {
    Terminal { restart_enqueued: bool },
    RunningRebound,
    StartingCompleted,
    StoppingResumed,
    StoppingUnknownPreserved,
    ExitSettled { restart_enqueued: bool },
}

enum ReconciliationAttempt {
    Resolved(ReconciliationAction),
    Retry(Box<Session>),
}

struct SessionRecoveryFailure {
    stage: StartupRecoveryStage,
    failure: StartupRecoveryFailure,
}

impl SessionRecoveryFailure {
    fn port(stage: StartupRecoveryStage, error: PortError) -> Self {
        Self {
            stage,
            failure: StartupRecoveryFailure::Infrastructure {
                operation: error.operation(),
                kind: error.kind(),
            },
        }
    }

    const fn application(stage: StartupRecoveryStage, category: ApplicationErrorCategory) -> Self {
        Self {
            stage,
            failure: StartupRecoveryFailure::Application { category },
        }
    }

    fn into_issue(self, session_id: SessionId) -> StartupRecoveryIssue {
        StartupRecoveryIssue {
            session_id,
            stage: self.stage,
            failure: self.failure,
        }
    }
}

impl StartupRecoveryFailure {
    const fn operation(self) -> Option<PortOperation> {
        match self {
            Self::Infrastructure { operation, .. } => Some(operation),
            Self::Application { .. }
            | Self::ProcessBindingConflict
            | Self::UnresolvedProcessTruth { .. }
            | Self::RuntimeCancellationEffectUnknown
            | Self::StateChangedRepeatedly => None,
        }
    }

    const fn error_kind(self) -> Option<PortErrorKind> {
        match self {
            Self::Infrastructure { kind, .. } => Some(kind),
            Self::Application { .. }
            | Self::ProcessBindingConflict
            | Self::UnresolvedProcessTruth { .. }
            | Self::RuntimeCancellationEffectUnknown
            | Self::StateChangedRepeatedly => None,
        }
    }
}

fn validate_batch(
    batch: &RecoverySessionBatch,
    after: Option<&SessionId>,
    limit: usize,
) -> Result<(), RecoveryBatchViolation> {
    if batch.sessions.len() > limit {
        return Err(RecoveryBatchViolation::LimitExceeded);
    }
    let mut previous = after;
    for session in &batch.sessions {
        if previous.is_some_and(|previous| session.id() <= previous) {
            return Err(RecoveryBatchViolation::SessionOrder);
        }
        previous = Some(session.id());
    }
    match (batch.sessions.last(), batch.next_after.as_ref()) {
        (None, Some(_)) => Err(RecoveryBatchViolation::EmptyContinuation),
        (Some(last), Some(next_after)) if last.id() != next_after => {
            Err(RecoveryBatchViolation::CursorMismatch)
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_reject_zero_batch_size_and_allow_count_only_diagnostics() {
        assert_eq!(
            StartupReconciliationLimits::new(0, 1),
            Err(InvalidStartupReconciliationLimits)
        );
        assert_eq!(
            StartupReconciliationLimits::new(16, 0).expect("valid limits"),
            StartupReconciliationLimits {
                batch_size: 16,
                max_retained_issues: 0,
            }
        );
    }

    #[test]
    fn readiness_requires_complete_scan_without_unresolved_sessions() {
        let mut report = StartupReconciliationReport::default();
        assert!(!report.ready());
        report.scan_completed = true;
        assert!(report.ready());
        report.unresolved = 1;
        assert!(!report.ready());
    }
}
