// SPDX-License-Identifier: Apache-2.0

//! Ordered settlement of concrete supervised-process exit observations.

use std::error::Error;
use std::fmt::{Display, Formatter};

use undercurrent_domain::{
    LifecycleState, ProcessInstanceId, ProcessOutcome, RestartDecision, Session, TimestampMillis,
};

use super::{MutationCore, SessionMutationError, enqueue_restart, not_before};
use crate::{
    ApplicationErrorCategory, ApplicationOperation, Clock, DiagnosticEventName, DiagnosticLevel,
    MetricName, MetricObservation, PortError, PortOperation, ProcessExitObservation,
    ProcessUnbindOutcome, RestartJob, RestartScheduler, SessionMutationCoordinator,
    SessionProcessDirectory, SessionProjectionWriter, SessionRepository, Telemetry,
};

/// Expected reason an exit notification made no durable change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IgnoredProcessExitReason {
    /// No process is currently owned by the session supervisor.
    BindingMissing,
    /// The session is already owned by another concrete process instance.
    ProcessReplaced,
}

/// Successful result of settling or safely ignoring one process exit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObserveProcessExitResult {
    /// A stale or duplicate notification was ignored without changing state.
    Ignored {
        /// Stable reason the observation was not applicable.
        reason: IgnoredProcessExitReason,
    },
    /// Terminal lifecycle and restart disposition were committed atomically.
    Committed {
        /// Current durable aggregate.
        session: Session,
        /// Deterministic policy decision for the observed outcome.
        decision: RestartDecision,
        /// Whether the rebuildable queue accepted the already committed restart intent.
        restart_enqueued: bool,
    },
    /// Durable terminal truth already existed; stale ownership was cleaned and
    /// any persisted restart job was idempotently re-enqueued.
    Recovered {
        /// Current durable aggregate.
        session: Session,
        /// Whether the rebuildable queue accepted the already committed restart intent.
        restart_enqueued: bool,
    },
}

/// Typed failure from process-exit settlement.
#[derive(Debug)]
pub enum ObserveProcessExitError {
    /// Ordered load, domain mutation, or atomic commit failed.
    Mutation(SessionMutationError),
    /// The supervisor process directory could not be read.
    ProcessDirectory(PortError),
    /// Durable settlement succeeded but exact directory cleanup failed.
    DirectoryCleanup {
        /// Current durable terminal aggregate.
        session: Box<Session>,
        /// Concrete directory adapter failure.
        source: PortError,
    },
    /// A replacement binding appeared after durable settlement and was
    /// deliberately preserved.
    DirectoryChanged {
        /// Current durable terminal aggregate.
        session: Box<Session>,
    },
    /// Restart intent is durable, but the delayed-work adapter did not accept
    /// the idempotent job.
    RestartEnqueue {
        /// Current durable terminal aggregate retaining scheduled intent.
        session: Box<Session>,
        /// Job that may be safely retried by reconciliation.
        job: RestartJob,
        /// Concrete delayed-work adapter failure.
        source: PortError,
    },
    /// Aggregate restart state disagreed with the policy decision just
    /// committed.
    InvariantViolation {
        /// Current durable aggregate requiring operator diagnostics.
        session: Box<Session>,
        /// Stable violated rule.
        rule: &'static str,
    },
}

impl ObserveProcessExitError {
    /// Returns the stable category used by protocol and presentation adapters.
    pub const fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::Mutation(error) => error.category(),
            Self::ProcessDirectory(error) => error.category(),
            Self::DirectoryCleanup { source, .. } | Self::RestartEnqueue { source, .. } => {
                source.category()
            }
            Self::DirectoryChanged { .. } => ApplicationErrorCategory::Conflict,
            Self::InvariantViolation { .. } => ApplicationErrorCategory::InvariantViolation,
        }
    }
}

impl Display for ObserveProcessExitError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mutation(error) => Display::fmt(error, formatter),
            Self::ProcessDirectory(error) => {
                write!(formatter, "could not resolve supervised process: {error}")
            }
            Self::DirectoryCleanup { .. } => formatter
                .write_str("process exit was committed but supervisor-directory cleanup failed"),
            Self::DirectoryChanged { .. } => formatter
                .write_str("process exit was committed but a replacement binding was preserved"),
            Self::RestartEnqueue { .. } => formatter
                .write_str("restart intent was committed but delayed work could not be enqueued"),
            Self::InvariantViolation { rule, .. } => {
                write!(formatter, "process-exit invariant failed: {rule}")
            }
        }
    }
}

impl Error for ObserveProcessExitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Mutation(error) => Some(error),
            Self::ProcessDirectory(error)
            | Self::DirectoryCleanup { source: error, .. }
            | Self::RestartEnqueue { source: error, .. } => Some(error),
            Self::DirectoryChanged { .. } | Self::InvariantViolation { .. } => None,
        }
    }
}

/// Serializes, commits, cleans up, and schedules one concrete process exit.
pub struct ObserveProcessExitService<'a> {
    core: MutationCore<'a>,
    processes: &'a dyn SessionProcessDirectory,
    restarts: &'a dyn RestartScheduler,
    telemetry: &'a dyn Telemetry,
}

impl<'a> ObserveProcessExitService<'a> {
    /// Constructs the service from explicit ordering, persistence, ownership,
    /// delayed-work, projection, time, and telemetry boundaries.
    ///
    /// Construction performs no IO and does not panic.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        clock: &'a dyn Clock,
        coordinator: &'a dyn SessionMutationCoordinator,
        repository: &'a dyn SessionRepository,
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
                ApplicationOperation::ObserveProcessExit,
            ),
            processes,
            restarts,
            telemetry,
        }
    }

    /// Settles an exit only when the observed process still owns the session.
    ///
    /// Lifecycle and restart status are persisted with ordered outbox events in
    /// one optimistic transaction. Directory cleanup is compare-and-remove.
    /// Delayed work is enqueued only after durable truth and cleanup; a failed
    /// enqueue leaves enough durable status for startup reconciliation. This
    /// method performs adapter IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed mutation, directory, cleanup, scheduling, or invariant
    /// failures. Stale and duplicate process notifications are successful
    /// ignored results rather than infrastructure failures.
    pub async fn execute(
        &self,
        observation: ProcessExitObservation,
    ) -> Result<ObserveProcessExitResult, ObserveProcessExitError> {
        let started_at = self.core.clock.now();
        let result = self.execute_inner(observation).await;
        let elapsed = self.core.clock.now().saturating_duration_since(started_at);
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::OperationDurationSeconds,
            value: elapsed.as_secs_f64(),
            operation: Some(ApplicationOperation::ObserveProcessExit),
        });
        result
    }

    async fn execute_inner(
        &self,
        observation: ProcessExitObservation,
    ) -> Result<ObserveProcessExitResult, ObserveProcessExitError> {
        self.core.emit(
            DiagnosticEventName::ProcessExitObserved,
            DiagnosticLevel::Info,
            observation.observed_at,
            Some(observation.session_id.clone()),
            None,
            None,
        );
        let (lease, mut session) = self
            .core
            .begin(&observation.session_id)
            .await
            .map_err(ObserveProcessExitError::Mutation)?;
        let binding = match self.processes.lookup(session.id()) {
            Ok(Some(binding)) => binding,
            Ok(None) => {
                return Ok(self.ignored(
                    &session,
                    observation.observed_at,
                    IgnoredProcessExitReason::BindingMissing,
                ));
            }
            Err(error) => {
                self.core.emit_port_failure(
                    self.core.clock.now(),
                    Some(session.id().clone()),
                    &error,
                );
                return Err(ObserveProcessExitError::ProcessDirectory(error));
            }
        };
        if binding.id != observation.process_id {
            return Ok(self.ignored(
                &session,
                observation.observed_at,
                IgnoredProcessExitReason::ProcessReplaced,
            ));
        }
        if !session.lifecycle().is_terminal()
            && !session
                .process()
                .is_some_and(|durable| binding.matches(durable))
        {
            return Ok(self.ignored(
                &session,
                observation.observed_at,
                IgnoredProcessExitReason::ProcessReplaced,
            ));
        }

        if session.lifecycle().is_terminal() {
            self.cleanup_exact(&session, &binding.id)
                .map_err(|error| error.into_observe_error(&session))?;
            drop(lease);
            let restart_enqueued = self.enqueue_if_scheduled(&session).await?;
            return Ok(ObserveProcessExitResult::Recovered {
                session,
                restart_enqueued,
            });
        }

        let expected_revision = session.revision();
        let changed_at = not_before(observation.observed_at, session.updated_at());
        let uptime = observation
            .observed_at
            .saturating_duration_since(binding.started_at);
        let terminal = match observation.outcome {
            ProcessOutcome::Exited(code) => LifecycleState::Exited(code),
            ProcessOutcome::SupervisionFailed => LifecycleState::Failed,
        };
        let lifecycle_change = session
            .transition_lifecycle(terminal, changed_at)
            .map_err(SessionMutationError::Domain)
            .map_err(ObserveProcessExitError::Mutation)?
            .ok_or_else(|| ObserveProcessExitError::InvariantViolation {
                session: Box::new(session.clone()),
                rule: "active process outcome must produce a terminal lifecycle change",
            })?;
        let (decision, restart_change) = session
            .observe_process_outcome(observation.outcome, uptime, changed_at)
            .map_err(SessionMutationError::Domain)
            .map_err(ObserveProcessExitError::Mutation)?;
        let restart_change =
            restart_change.ok_or_else(|| ObserveProcessExitError::InvariantViolation {
                session: Box::new(session.clone()),
                rule: "new process outcome must produce a durable restart disposition",
            })?;
        self.core
            .commit_changes(
                &session,
                expected_revision,
                &[lifecycle_change, restart_change],
                changed_at,
            )
            .await
            .map_err(ObserveProcessExitError::Mutation)?;
        self.core.emit(
            DiagnosticEventName::ProcessExitCommitted,
            DiagnosticLevel::Info,
            changed_at,
            Some(session.id().clone()),
            Some(PortOperation::CommitSession),
            None,
        );

        self.cleanup_exact(&session, &binding.id)
            .map_err(|error| error.into_observe_error(&session))?;
        drop(lease);
        let restart_enqueued = self.enqueue_if_scheduled(&session).await?;
        Ok(ObserveProcessExitResult::Committed {
            session,
            decision,
            restart_enqueued,
        })
    }

    fn ignored(
        &self,
        session: &Session,
        observed_at: TimestampMillis,
        reason: IgnoredProcessExitReason,
    ) -> ObserveProcessExitResult {
        self.core.emit(
            DiagnosticEventName::ProcessExitIgnored,
            DiagnosticLevel::Debug,
            observed_at,
            Some(session.id().clone()),
            Some(PortOperation::LookupProcess),
            None,
        );
        ObserveProcessExitResult::Ignored { reason }
    }

    fn cleanup_exact(
        &self,
        session: &Session,
        process_id: &ProcessInstanceId,
    ) -> Result<(), ExactDirectoryCleanupError> {
        match self.processes.unbind_if(session.id(), process_id) {
            Ok(ProcessUnbindOutcome::Unbound(_) | ProcessUnbindOutcome::Missing) => Ok(()),
            Ok(ProcessUnbindOutcome::ProcessMismatch) => {
                Err(ExactDirectoryCleanupError::DirectoryChanged)
            }
            Err(source) => {
                self.core.emit_port_failure(
                    self.core.clock.now(),
                    Some(session.id().clone()),
                    &source,
                );
                Err(ExactDirectoryCleanupError::DirectoryCleanup(source))
            }
        }
    }

    async fn enqueue_if_scheduled(
        &self,
        session: &Session,
    ) -> Result<bool, ObserveProcessExitError> {
        enqueue_restart(&self.core, self.restarts, session)
            .await
            .map_err(|failure| ObserveProcessExitError::RestartEnqueue {
                session: Box::new(session.clone()),
                job: failure.job,
                source: failure.source,
            })
    }
}

enum ExactDirectoryCleanupError {
    DirectoryChanged,
    DirectoryCleanup(PortError),
}

impl ExactDirectoryCleanupError {
    fn into_observe_error(self, session: &Session) -> ObserveProcessExitError {
        match self {
            Self::DirectoryChanged => ObserveProcessExitError::DirectoryChanged {
                session: Box::new(session.clone()),
            },
            Self::DirectoryCleanup(source) => ObserveProcessExitError::DirectoryCleanup {
                session: Box::new(session.clone()),
                source,
            },
        }
    }
}
