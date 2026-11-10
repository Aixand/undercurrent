// SPDX-License-Identifier: Apache-2.0

//! Revision-checked execution of durable automatic restart jobs.

use std::error::Error;
use std::fmt::{Display, Formatter};

use undercurrent_domain::{
    LifecycleState, ProcessOutcome, RestartStatus, Session, TimestampMillis,
};

use super::activation::{
    ProcessActivationFailure, ProcessActivationFailureSource, ProcessActivationStage,
    activate_process, compensate_process,
};
use super::{MutationCore, SessionMutationError, not_before};
use crate::{
    ApplicationErrorCategory, ApplicationOperation, Clock, DiagnosticEventName, DiagnosticLevel,
    MetricName, MetricObservation, PortError, PortOperation, ProcessInstance, ProcessRuntime,
    RestartJob, RestartScheduler, SessionMutationCoordinator, SessionProcessDirectory,
    SessionProjectionWriter, SessionRepository, Telemetry,
};

/// Expected reason a delayed restart job performed no process IO.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IgnoredRestartReason {
    /// The delayed-work adapter delivered the job before its durable deadline.
    TooEarly,
    /// Aggregate revision changed after this job was created.
    RevisionChanged,
    /// Durable restart status no longer matches the job attempt and deadline.
    RestartStatusChanged,
    /// Session is no longer in a terminal lifecycle.
    LifecycleChanged,
    /// A process already owns the session binding.
    ProcessAlreadyBound,
}

/// Successful outcome of executing or safely ignoring one restart job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunScheduledRestartResult {
    /// Stale, early, or superseded work was ignored without mutation or spawn.
    Ignored {
        /// Stable validation reason.
        reason: IgnoredRestartReason,
    },
    /// A replacement process is bound and its running lifecycle is durable.
    Restarted {
        /// Current durable session retaining its stable identity/configuration.
        session: Box<Session>,
        /// Newly owned concrete process instance.
        process: ProcessInstance,
    },
}

/// Stage at which process restart activation failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartFailureStage {
    /// Runtime process creation failed.
    Spawn,
    /// Spawned process ownership could not be represented in the aggregate.
    RecordProcessOwnership,
    /// Spawned process ownership could not be committed durably.
    CommitProcessOwnership,
    /// Supervisor directory rejected the new process binding.
    Bind,
    /// Retained exit observation could not be armed after exact binding.
    ArmProcessExit,
    /// The owned process could not enter the running lifecycle.
    EnterRunning,
    /// Durable transition from `starting` to `running` failed.
    CommitRunning,
}

impl RestartFailureStage {
    /// Returns the stable diagnostic name for this activation stage.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Spawn => "spawn",
            Self::RecordProcessOwnership => "record_process_ownership",
            Self::CommitProcessOwnership => "commit_process_ownership",
            Self::Bind => "bind",
            Self::ArmProcessExit => "arm_process_exit",
            Self::EnterRunning => "enter_running",
            Self::CommitRunning => "commit_running",
        }
    }
}

/// Compensation and durable settlement details for a failed restart attempt.
#[derive(Debug, Default)]
pub struct RestartRecoveryReport {
    force_stop_failure: Option<PortError>,
    unbind_failure: Option<PortError>,
    unbind_conflict: bool,
    settlement_failure: Option<SessionMutationError>,
    enqueue_failure: Option<PortError>,
}

impl RestartRecoveryReport {
    /// Returns whether process compensation, durable settlement, and follow-up
    /// scheduling all completed.
    pub const fn completed(&self) -> bool {
        self.force_stop_failure.is_none()
            && self.unbind_failure.is_none()
            && !self.unbind_conflict
            && self.settlement_failure.is_none()
            && self.enqueue_failure.is_none()
    }

    /// Returns forced-termination failure retaining uncertain process truth.
    pub const fn force_stop_failure(&self) -> Option<&PortError> {
        self.force_stop_failure.as_ref()
    }

    /// Returns exact directory-cleanup adapter failure.
    pub const fn unbind_failure(&self) -> Option<&PortError> {
        self.unbind_failure.as_ref()
    }

    /// Returns whether a replacement binding was deliberately preserved.
    pub const fn unbind_conflict(&self) -> bool {
        self.unbind_conflict
    }

    /// Returns durable failed-state settlement failure.
    pub const fn settlement_failure(&self) -> Option<&SessionMutationError> {
        self.settlement_failure.as_ref()
    }

    /// Returns follow-up delayed-work enqueue failure.
    pub const fn enqueue_failure(&self) -> Option<&PortError> {
        self.enqueue_failure.as_ref()
    }
}

/// Typed failure from an accepted automatic restart attempt.
#[derive(Debug)]
pub enum RunScheduledRestartError {
    /// Ordered load or durable `starting` commit failed before process IO.
    Mutation(SessionMutationError),
    /// Supervisor process ownership could not be inspected.
    ProcessDirectory(PortError),
    /// Process activation failed and compensation details are retained.
    AttemptFailed {
        /// Failed activation stage.
        stage: RestartFailureStage,
        /// Primary concrete adapter or durable mutation failure.
        source: RestartAttemptFailure,
        /// Best available aggregate after attempted settlement.
        session: Box<Session>,
        /// Concrete spawned process when the attempt reached process creation.
        /// Callers retain this identity for reconciliation when compensation is
        /// incomplete.
        process: Option<ProcessInstance>,
        /// Compensation and follow-up scheduling results.
        recovery: Box<RestartRecoveryReport>,
    },
    /// Pure lifecycle orchestration reached an inconsistent branch.
    InvariantViolation {
        /// Stable violated rule.
        rule: &'static str,
    },
}

/// Primary process or persistence failure from a restart activation stage.
#[derive(Debug)]
pub enum RestartAttemptFailure {
    /// Concrete process or directory adapter failed.
    Port(PortError),
    /// Ordered durable mutation failed.
    Mutation(SessionMutationError),
}

impl Display for RestartAttemptFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Port(error) => Display::fmt(error, formatter),
            Self::Mutation(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for RestartAttemptFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Port(error) => Some(error),
            Self::Mutation(error) => Some(error),
        }
    }
}

impl RunScheduledRestartError {
    /// Returns stable category used by protocol and presentation adapters.
    pub const fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::Mutation(error) => error.category(),
            Self::ProcessDirectory(error) => error.category(),
            Self::AttemptFailed { source, .. } => match source {
                RestartAttemptFailure::Port(error) => error.category(),
                RestartAttemptFailure::Mutation(error) => error.category(),
            },
            Self::InvariantViolation { .. } => ApplicationErrorCategory::InvariantViolation,
        }
    }
}

impl Display for RunScheduledRestartError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mutation(error) => Display::fmt(error, formatter),
            Self::ProcessDirectory(error) => {
                write!(
                    formatter,
                    "could not inspect restart process ownership: {error}"
                )
            }
            Self::AttemptFailed {
                stage, recovery, ..
            } => {
                write!(formatter, "automatic restart failed at {}", stage.as_str())?;
                if !recovery.completed() {
                    formatter.write_str("; recovery was incomplete")?;
                }
                Ok(())
            }
            Self::InvariantViolation { rule } => {
                write!(formatter, "scheduled-restart invariant failed: {rule}")
            }
        }
    }
}

impl Error for RunScheduledRestartError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Mutation(error) => Some(error),
            Self::ProcessDirectory(error) => Some(error),
            Self::AttemptFailed { source, .. } => Some(source),
            Self::InvariantViolation { .. } => None,
        }
    }
}

/// Validates and executes durable delayed restart jobs one session at a time.
pub struct RunScheduledRestartService<'a> {
    core: MutationCore<'a>,
    runtime: &'a dyn ProcessRuntime,
    processes: &'a dyn SessionProcessDirectory,
    restarts: &'a dyn RestartScheduler,
    telemetry: &'a dyn Telemetry,
}

impl<'a> RunScheduledRestartService<'a> {
    /// Constructs a scheduled-restart worker from explicit inward-facing ports.
    /// Construction performs no IO and does not panic.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        clock: &'a dyn Clock,
        coordinator: &'a dyn SessionMutationCoordinator,
        repository: &'a dyn SessionRepository,
        runtime: &'a dyn ProcessRuntime,
        processes: &'a dyn SessionProcessDirectory,
        restarts: &'a dyn RestartScheduler,
        projection: &'a dyn SessionProjectionWriter,
        telemetry: &'a dyn Telemetry,
    ) -> Self {
        Self {
            core: MutationCore::new(
                clock,
                coordinator,
                repository,
                projection,
                telemetry,
                ApplicationOperation::RunScheduledRestart,
            ),
            runtime,
            processes,
            restarts,
            telemetry,
        }
    }

    /// Executes a due job only while its exact durable revision and disposition
    /// remain current.
    ///
    /// `starting` is committed before spawn. A successful spawn is bound before
    /// `running` is committed. Failures after process creation trigger forced
    /// termination and exact binding cleanup before failed-state settlement.
    /// Spawn and compensated activation failures feed back into bounded restart
    /// policy and may enqueue one revision-checked follow-up job. This method
    /// performs adapter IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed ordering, persistence, process, directory, compensation,
    /// scheduling, or invariant failures. Superseded jobs return an ignored
    /// result without process IO.
    pub async fn execute(
        &self,
        job: RestartJob,
    ) -> Result<RunScheduledRestartResult, RunScheduledRestartError> {
        let operation_started_at = self.core.clock.now();
        let result = self.execute_inner(job).await;
        let elapsed = self
            .core
            .clock
            .now()
            .saturating_duration_since(operation_started_at);
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::OperationDurationSeconds,
            value: elapsed.as_secs_f64(),
            operation: Some(ApplicationOperation::RunScheduledRestart),
        });
        result
    }

    async fn execute_inner(
        &self,
        job: RestartJob,
    ) -> Result<RunScheduledRestartResult, RunScheduledRestartError> {
        let now = self.core.clock.now();
        self.core.emit(
            DiagnosticEventName::RestartAttemptStarted,
            DiagnosticLevel::Info,
            now,
            Some(job.session_id.clone()),
            None,
            None,
        );
        if now < job.not_before {
            return Ok(self.ignored(&job, now, IgnoredRestartReason::TooEarly));
        }
        let (lease, mut session) = self
            .core
            .begin(&job.session_id)
            .await
            .map_err(RunScheduledRestartError::Mutation)?;
        if session.revision() != job.expected_revision {
            return Ok(self.ignored(&job, now, IgnoredRestartReason::RevisionChanged));
        }
        if session.restart_status()
            != (RestartStatus::Scheduled {
                attempt: job.attempt,
                not_before: job.not_before,
            })
        {
            return Ok(self.ignored(&job, now, IgnoredRestartReason::RestartStatusChanged));
        }
        if !session.lifecycle().is_terminal() {
            return Ok(self.ignored(&job, now, IgnoredRestartReason::LifecycleChanged));
        }
        match self.processes.lookup(session.id()) {
            Ok(None) => {}
            Ok(Some(_)) => {
                return Ok(self.ignored(&job, now, IgnoredRestartReason::ProcessAlreadyBound));
            }
            Err(error) => {
                self.core.emit_port_failure(
                    self.core.clock.now(),
                    Some(session.id().clone()),
                    &error,
                );
                return Err(RunScheduledRestartError::ProcessDirectory(error));
            }
        }

        self.telemetry.record_metric(MetricObservation {
            name: MetricName::RestartAttempts,
            value: 1.0,
            operation: Some(ApplicationOperation::RunScheduledRestart),
        });
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::RestartSchedulingLagSeconds,
            value: now.saturating_duration_since(job.not_before).as_secs_f64(),
            operation: Some(ApplicationOperation::RunScheduledRestart),
        });

        let expected_revision = session.revision();
        let starting_at = not_before(now, session.updated_at());
        let starting_change = session
            .transition_lifecycle(LifecycleState::Starting, starting_at)
            .map_err(SessionMutationError::Domain)
            .map_err(RunScheduledRestartError::Mutation)?
            .ok_or(RunScheduledRestartError::InvariantViolation {
                rule: "terminal restart must produce a starting lifecycle change",
            })?;
        self.core
            .commit(&session, expected_revision, starting_change, starting_at)
            .await
            .map_err(RunScheduledRestartError::Mutation)?;

        match activate_process(&self.core, self.runtime, self.processes, session, None).await {
            Ok(activated) => {
                self.core.emit(
                    DiagnosticEventName::RestartAttemptSucceeded,
                    DiagnosticLevel::Info,
                    activated.session.updated_at(),
                    Some(activated.session.id().clone()),
                    Some(PortOperation::CommitSession),
                    None,
                );
                Ok(RunScheduledRestartResult::Restarted {
                    session: Box::new(activated.session),
                    process: activated.process,
                })
            }
            Err(failure) => self.recover_activation_failure(failure, lease).await,
        }
    }

    async fn recover_activation_failure(
        &self,
        failure: ProcessActivationFailure,
        lease: Box<dyn crate::SessionMutationLease>,
    ) -> Result<RunScheduledRestartResult, RunScheduledRestartError> {
        let stage = restart_failure_stage(failure.stage);
        let source = match failure.source {
            ProcessActivationFailureSource::Mutation(source) => {
                RestartAttemptFailure::Mutation(source)
            }
            ProcessActivationFailureSource::Port(source) => RestartAttemptFailure::Port(source),
        };
        let mut session = failure.session;
        let process = failure.process;
        let mut recovery = RestartRecoveryReport::default();
        if let Some(spawned) = process.as_ref() {
            let compensation = compensate_process(
                &self.core,
                self.runtime,
                self.processes,
                &session,
                spawned,
                failure.binding,
            )
            .await;
            let safe_to_settle = compensation.safe_to_settle();
            recovery.force_stop_failure = compensation.force_stop_failure;
            recovery.unbind_failure = compensation.unbind_failure;
            recovery.unbind_conflict = compensation.unbind_conflict;
            if !safe_to_settle {
                return Err(self.attempt_error(stage, source, session, process, recovery));
            }
        }
        self.settle_and_reschedule(&mut session, &mut recovery, lease)
            .await;
        Err(self.attempt_error(stage, source, session, process, recovery))
    }

    async fn settle_and_reschedule(
        &self,
        session: &mut Session,
        recovery: &mut RestartRecoveryReport,
        lease: Box<dyn crate::SessionMutationLease>,
    ) {
        if let Err(error) = self.settle_failed_attempt(session).await {
            recovery.settlement_failure = Some(error);
            return;
        }
        drop(lease);
        if let Err(failure) = enqueue_restart(&self.core, self.restarts, session).await {
            recovery.enqueue_failure = Some(failure.source);
        }
    }

    async fn settle_failed_attempt(
        &self,
        session: &mut Session,
    ) -> Result<(), SessionMutationError> {
        let expected_revision = session.revision();
        let changed_at = not_before(self.core.clock.now(), session.updated_at());
        let failed_change = session
            .transition_lifecycle(LifecycleState::Failed, changed_at)
            .map_err(SessionMutationError::Domain)?
            .ok_or(SessionMutationError::InvariantViolation {
                rule: "starting restart failure must produce failed lifecycle",
            })?;
        let (_, restart_change) = session
            .observe_process_outcome(
                ProcessOutcome::SupervisionFailed,
                Default::default(),
                changed_at,
            )
            .map_err(SessionMutationError::Domain)?;
        let restart_change = restart_change.ok_or(SessionMutationError::InvariantViolation {
            rule: "restart attempt failure must produce restart disposition",
        })?;
        self.core
            .commit_changes(
                session,
                expected_revision,
                &[failed_change, restart_change],
                changed_at,
            )
            .await
    }

    fn ignored(
        &self,
        job: &RestartJob,
        timestamp: TimestampMillis,
        reason: IgnoredRestartReason,
    ) -> RunScheduledRestartResult {
        self.core.emit(
            DiagnosticEventName::RestartAttemptIgnored,
            DiagnosticLevel::Debug,
            timestamp,
            Some(job.session_id.clone()),
            None,
            None,
        );
        RunScheduledRestartResult::Ignored { reason }
    }

    fn attempt_error(
        &self,
        stage: RestartFailureStage,
        source: RestartAttemptFailure,
        session: Session,
        process: Option<ProcessInstance>,
        recovery: RestartRecoveryReport,
    ) -> RunScheduledRestartError {
        self.core.emit(
            DiagnosticEventName::RestartAttemptFailed,
            DiagnosticLevel::Error,
            self.core.clock.now(),
            Some(session.id().clone()),
            None,
            None,
        );
        RunScheduledRestartError::AttemptFailed {
            stage,
            source,
            session: Box::new(session),
            process,
            recovery: Box::new(recovery),
        }
    }
}

pub(crate) fn restart_failure_stage(stage: ProcessActivationStage) -> RestartFailureStage {
    match stage {
        ProcessActivationStage::Spawn => RestartFailureStage::Spawn,
        ProcessActivationStage::RecordProcessOwnership => {
            RestartFailureStage::RecordProcessOwnership
        }
        ProcessActivationStage::CommitProcessOwnership => {
            RestartFailureStage::CommitProcessOwnership
        }
        ProcessActivationStage::BindProcess => RestartFailureStage::Bind,
        ProcessActivationStage::ArmProcessExit => RestartFailureStage::ArmProcessExit,
        ProcessActivationStage::EnterRunning => RestartFailureStage::EnterRunning,
        ProcessActivationStage::CommitRunning => RestartFailureStage::CommitRunning,
    }
}

pub(crate) fn restart_job(session: &Session) -> Option<RestartJob> {
    let RestartStatus::Scheduled {
        attempt,
        not_before,
    } = session.restart_status()
    else {
        return None;
    };
    Some(RestartJob {
        session_id: session.id().clone(),
        expected_revision: session.revision(),
        attempt,
        not_before,
    })
}

pub(crate) struct RestartEnqueueFailure {
    pub(crate) job: RestartJob,
    pub(crate) source: PortError,
}

/// Enqueues current durable restart intent with common redacted diagnostics.
///
/// Absence of scheduled intent is a successful `false` result. This helper
/// performs delayed-work adapter IO and does not panic.
pub(crate) async fn enqueue_restart(
    core: &MutationCore<'_>,
    restarts: &dyn RestartScheduler,
    session: &Session,
) -> Result<bool, RestartEnqueueFailure> {
    let Some(job) = restart_job(session) else {
        return Ok(false);
    };
    core.emit(
        DiagnosticEventName::RestartEnqueueStarted,
        DiagnosticLevel::Debug,
        core.clock.now(),
        Some(session.id().clone()),
        Some(PortOperation::ScheduleRestart),
        None,
    );
    match restarts.schedule(&job).await {
        Ok(()) => {
            core.emit(
                DiagnosticEventName::RestartEnqueued,
                DiagnosticLevel::Info,
                core.clock.now(),
                Some(session.id().clone()),
                Some(PortOperation::ScheduleRestart),
                None,
            );
            Ok(true)
        }
        Err(source) => {
            core.emit_port_failure(core.clock.now(), Some(session.id().clone()), &source);
            core.emit(
                DiagnosticEventName::RestartEnqueueFailed,
                DiagnosticLevel::for_port_error(source.kind()),
                core.clock.now(),
                Some(session.id().clone()),
                Some(PortOperation::ScheduleRestart),
                Some(source.kind()),
            );
            Err(RestartEnqueueFailure { job, source })
        }
    }
}
