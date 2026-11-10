// SPDX-License-Identifier: Apache-2.0

//! Explicit stop-then-activate session restart.

use std::error::Error;
use std::fmt::{Display, Formatter};

use undercurrent_domain::{LifecycleState, Revision, Session, SessionId};

use super::activation::{
    ProcessActivationFailure, ProcessActivationFailureSource, activate_process, compensate_process,
};
use super::scheduled_restart::restart_failure_stage;
use super::{
    MutationCore, RestartAttemptFailure, RestartFailureStage, SessionMutationError, StopMode,
    StopSessionCommand, StopSessionError, StopSessionResult, StopSessionService, not_before,
};
use crate::{
    ApplicationErrorCategory, ApplicationOperation, Clock, DiagnosticEventName, DiagnosticLevel,
    MetricName, MetricObservation, PortError, PortOperation, ProcessInstance, ProcessRuntime,
    ProcessTermination, RestartScheduler, SessionMutationCoordinator, SessionProcessDirectory,
    SessionProjectionWriter, SessionRepository, Telemetry,
};

/// Input for explicitly restarting one durable session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestartSessionCommand {
    /// Stable target session identity.
    pub session_id: SessionId,
    /// Termination policy for the old process.
    pub stop_mode: StopMode,
}

/// Successful explicit restart preserving stable identity and configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestartedSession {
    /// Authoritative durable running session.
    pub session: Session,
    /// Exact replacement process incarnation.
    pub process: ProcessInstance,
    /// Confirmed old-process termination when the session was active.
    pub previous_termination: Option<ProcessTermination>,
}

/// Best available recovery state after explicit restart activation failed.
#[derive(Debug, Default)]
pub struct RestartSessionRecoveryReport {
    force_stop_failure: Option<PortError>,
    unbind_failure: Option<PortError>,
    unbind_conflict: bool,
    settlement_failure: Option<SessionMutationError>,
}

impl RestartSessionRecoveryReport {
    /// Returns whether process compensation and durable failed-state settlement
    /// completed. This function performs no IO and does not panic.
    pub const fn completed(&self) -> bool {
        self.force_stop_failure.is_none()
            && self.unbind_failure.is_none()
            && !self.unbind_conflict
            && self.settlement_failure.is_none()
    }

    /// Returns forced-termination failure retaining uncertain process truth.
    pub const fn force_stop_failure(&self) -> Option<&PortError> {
        self.force_stop_failure.as_ref()
    }

    /// Returns exact supervisor-directory cleanup failure.
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
}

/// Typed failure from explicit session restart.
#[derive(Debug)]
pub enum RestartSessionError {
    /// Stop completed, but shutdown cancelled replacement activation.
    Cancelled,
    /// The stop half of restart failed or retained uncertain process truth.
    Stop(StopSessionError),
    /// Ordered load or lifecycle mutation failed.
    Mutation(SessionMutationError),
    /// Session changed after stop completed and before activation ownership was
    /// reacquired. No replacement process was spawned.
    ChangedAfterStop {
        /// Revision returned by the completed stop phase.
        expected_revision: Revision,
        /// Revision loaded under the new activation lease.
        current_revision: Revision,
    },
    /// A terminal session unexpectedly retained a supervisor binding, so no
    /// replacement process was spawned.
    ProcessAlreadyBound {
        /// Stable target session identity.
        session_id: SessionId,
    },
    /// Supervisor process ownership could not be inspected.
    ProcessDirectory(PortError),
    /// Replacement activation failed and best-effort recovery was attempted.
    Activation {
        /// Exact activation stage.
        stage: RestartFailureStage,
        /// Primary adapter or durable-mutation failure.
        source: RestartAttemptFailure,
        /// Last aggregate state known to be durable.
        session: Box<Session>,
        /// Spawned replacement when activation reached process creation.
        process: Option<ProcessInstance>,
        /// Compensation and durable settlement results.
        recovery: Box<RestartSessionRecoveryReport>,
    },
}

impl RestartSessionError {
    /// Returns stable category used by protocol and presentation adapters.
    pub const fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::Cancelled => ApplicationErrorCategory::Cancelled,
            Self::Stop(error) => error.category(),
            Self::Mutation(error) => error.category(),
            Self::ChangedAfterStop { .. } => ApplicationErrorCategory::Conflict,
            Self::ProcessAlreadyBound { .. } => ApplicationErrorCategory::FailedPrecondition,
            Self::ProcessDirectory(error) => error.category(),
            Self::Activation { source, .. } => match source {
                RestartAttemptFailure::Port(error) => error.category(),
                RestartAttemptFailure::Mutation(error) => error.category(),
            },
        }
    }
}

impl Display for RestartSessionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled => formatter.write_str("restart activation cancelled by shutdown"),
            Self::Stop(error) => write!(formatter, "could not stop session for restart: {error}"),
            Self::Mutation(error) => Display::fmt(error, formatter),
            Self::ChangedAfterStop {
                expected_revision,
                current_revision,
            } => write!(
                formatter,
                "session changed after stop from revision {} to {}",
                expected_revision.value(),
                current_revision.value()
            ),
            Self::ProcessAlreadyBound { session_id } => write!(
                formatter,
                "terminal session {session_id} still has a supervised process binding"
            ),
            Self::ProcessDirectory(error) => {
                write!(
                    formatter,
                    "could not inspect restart process ownership: {error}"
                )
            }
            Self::Activation {
                stage, recovery, ..
            } => {
                write!(formatter, "session restart failed at {}", stage.as_str())?;
                if !recovery.completed() {
                    formatter.write_str("; recovery was incomplete")?;
                }
                Ok(())
            }
        }
    }
}

impl Error for RestartSessionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Stop(error) => Some(error),
            Self::Mutation(error) => Some(error),
            Self::ProcessDirectory(error) => Some(error),
            Self::Activation { source, .. } => Some(source),
            Self::Cancelled | Self::ChangedAfterStop { .. } | Self::ProcessAlreadyBound { .. } => {
                None
            }
        }
    }
}

/// Stops and reactivates one session through existing application boundaries.
pub struct RestartSessionService<'a> {
    stop: StopSessionService<'a>,
    core: MutationCore<'a>,
    runtime: &'a dyn ProcessRuntime,
    processes: &'a dyn SessionProcessDirectory,
    telemetry: &'a dyn Telemetry,
}

impl<'a> RestartSessionService<'a> {
    /// Constructs an explicit restart service from ordered mutation, process,
    /// restart scheduling, persistence, projection, clock, and telemetry ports.
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
            stop: StopSessionService::new(
                clock,
                coordinator,
                repository,
                runtime,
                processes,
                restarts,
                projection,
                telemetry,
            ),
            core: MutationCore::new(
                clock,
                coordinator,
                repository,
                projection,
                telemetry,
                ApplicationOperation::RestartSession,
            ),
            runtime,
            processes,
            telemetry,
        }
    }

    /// Gracefully stops the current process when present, verifies that no
    /// intervening mutation changed the stopped revision, and activates a new
    /// process with the same stable identity and configuration.
    ///
    /// Pending automatic restart intent is cancelled by the composed stop use
    /// case. The activation phase reacquires session-local ownership and uses
    /// optimistic revision equality across the deliberate lease boundary. An
    /// intervening change returns conflict before replacement activation and
    /// preserves the state produced by that mutation. The completed stop phase
    /// may already have performed process IO. This method does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed stop, ordering, persistence, concurrency, process,
    /// activation, compensation, or settlement failures.
    pub async fn execute(
        &self,
        command: RestartSessionCommand,
    ) -> Result<RestartedSession, RestartSessionError> {
        self.execute_with_cancellation(command, &|| false).await
    }

    /// Completes stop receipts, then checks shutdown before replacement activation.
    pub async fn execute_with_cancellation(
        &self,
        command: RestartSessionCommand,
        cancelled: &(dyn Fn() -> bool + Sync),
    ) -> Result<RestartedSession, RestartSessionError> {
        let started_at = self.core.clock.now();
        let session_id = command.session_id.clone();
        let result = self.execute_inner(command, cancelled).await;
        let elapsed = self.core.clock.now().saturating_duration_since(started_at);
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::OperationDurationSeconds,
            value: elapsed.as_secs_f64(),
            operation: Some(ApplicationOperation::RestartSession),
        });
        match &result {
            Ok(restarted) => self.core.emit(
                DiagnosticEventName::SessionRestartSucceeded,
                DiagnosticLevel::Info,
                restarted.session.updated_at(),
                Some(restarted.session.id().clone()),
                Some(PortOperation::CommitSession),
                None,
            ),
            Err(error) => self.core.emit(
                DiagnosticEventName::SessionRestartFailed,
                DiagnosticLevel::for_application_error(error.category()),
                self.core.clock.now(),
                Some(session_id),
                None,
                None,
            ),
        }
        result
    }

    async fn execute_inner(
        &self,
        command: RestartSessionCommand,
        cancelled: &(dyn Fn() -> bool + Sync),
    ) -> Result<RestartedSession, RestartSessionError> {
        self.core.emit(
            DiagnosticEventName::SessionRestartRequested,
            DiagnosticLevel::Info,
            self.core.clock.now(),
            Some(command.session_id.clone()),
            None,
            None,
        );
        let stopped = self
            .stop
            .execute(StopSessionCommand {
                expected_revision: None,
                session_id: command.session_id.clone(),
                mode: command.stop_mode,
            })
            .await
            .map_err(RestartSessionError::Stop)?;
        let (stopped_session, previous_termination) = match stopped {
            StopSessionResult::AlreadyTerminal { session } => (session, None),
            StopSessionResult::Terminated {
                session,
                termination,
            } => (session, Some(termination)),
        };
        let stopped_revision = stopped_session.revision();
        let (lease, mut session) = self
            .core
            .begin(&command.session_id)
            .await
            .map_err(RestartSessionError::Mutation)?;
        if cancelled() {
            return Err(RestartSessionError::Cancelled);
        }
        if session.revision() != stopped_revision {
            return Err(RestartSessionError::ChangedAfterStop {
                expected_revision: stopped_revision,
                current_revision: session.revision(),
            });
        }
        if !session.lifecycle().is_terminal() {
            return Err(RestartSessionError::Mutation(
                SessionMutationError::InvariantViolation {
                    rule: "completed restart stop phase must remain terminal",
                },
            ));
        }
        match self.processes.lookup(session.id()) {
            Ok(None) => {}
            Ok(Some(_)) => {
                return Err(RestartSessionError::ProcessAlreadyBound {
                    session_id: session.id().clone(),
                });
            }
            Err(error) => {
                self.core.emit_port_failure(
                    self.core.clock.now(),
                    Some(session.id().clone()),
                    &error,
                );
                return Err(RestartSessionError::ProcessDirectory(error));
            }
        }

        let expected_revision = session.revision();
        let changed_at = not_before(self.core.clock.now(), session.updated_at());
        let starting_change = session
            .transition_lifecycle(LifecycleState::Starting, changed_at)
            .map_err(SessionMutationError::Domain)
            .map_err(RestartSessionError::Mutation)?
            .ok_or(RestartSessionError::Mutation(
                SessionMutationError::InvariantViolation {
                    rule: "terminal explicit restart must produce a starting transition",
                },
            ))?;
        self.core
            .commit(&session, expected_revision, starting_change, changed_at)
            .await
            .map_err(RestartSessionError::Mutation)?;

        let result =
            match activate_process(&self.core, self.runtime, self.processes, session, None).await {
                Ok(activated) => Ok(RestartedSession {
                    session: activated.session,
                    process: activated.process,
                    previous_termination,
                }),
                Err(failure) => Err(self.recover_activation_failure(failure).await),
            };
        drop(lease);
        result
    }

    async fn recover_activation_failure(
        &self,
        failure: ProcessActivationFailure,
    ) -> RestartSessionError {
        let stage = restart_failure_stage(failure.stage);
        let source = match failure.source {
            ProcessActivationFailureSource::Mutation(source) => {
                RestartAttemptFailure::Mutation(source)
            }
            ProcessActivationFailureSource::Port(source) => RestartAttemptFailure::Port(source),
        };
        let mut session = failure.session;
        let process = failure.process;
        let mut recovery = RestartSessionRecoveryReport::default();
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
                return self.activation_error(stage, source, session, process, recovery);
            }
        }
        match self.settle_failed_activation(session).await {
            Ok(settled) => session = settled,
            Err((durable, error)) => {
                session = durable;
                recovery.settlement_failure = Some(error);
            }
        }
        self.activation_error(stage, source, session, process, recovery)
    }

    async fn settle_failed_activation(
        &self,
        mut session: Session,
    ) -> Result<Session, (Session, SessionMutationError)> {
        let durable = session.clone();
        let expected_revision = session.revision();
        let changed_at = not_before(self.core.clock.now(), session.updated_at());
        let change = match session
            .transition_lifecycle(LifecycleState::Failed, changed_at)
            .map_err(SessionMutationError::Domain)
        {
            Ok(Some(change)) => change,
            Ok(None) => return Ok(session),
            Err(error) => return Err((durable, error)),
        };
        match self
            .core
            .commit(&session, expected_revision, change, changed_at)
            .await
        {
            Ok(()) => Ok(session),
            Err(error) => Err((durable, error)),
        }
    }

    fn activation_error(
        &self,
        stage: RestartFailureStage,
        source: RestartAttemptFailure,
        session: Session,
        process: Option<ProcessInstance>,
        recovery: RestartSessionRecoveryReport,
    ) -> RestartSessionError {
        RestartSessionError::Activation {
            stage,
            source,
            session: Box::new(session),
            process,
            recovery: Box::new(recovery),
        }
    }
}
