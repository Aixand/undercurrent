// SPDX-License-Identifier: Apache-2.0

//! Ordered graceful and forced session termination.

use std::error::Error;
use std::fmt::{Display, Formatter};

use undercurrent_domain::{LifecycleState, ProcessMutationKind, Session, SessionId};

use super::{MutationCore, SessionMutationError, not_before};
use crate::{
    ApplicationErrorCategory, ApplicationOperation, Clock, DiagnosticEventName, DiagnosticLevel,
    MetricName, MetricObservation, PortError, PortOperation, ProcessRuntime, ProcessTermination,
    ProcessUnbindOutcome, RestartScheduler, SessionMutationCoordinator, SessionProcessDirectory,
    SessionProjectionWriter, SessionRepository, Telemetry, process_mutation_admission,
};

const MAX_TERMINAL_RECONCILIATION_ATTEMPTS: usize = 3;

/// Operator-selected process termination mode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum StopMode {
    /// Use configured graceful signal and deadline, then allow adapter escalation.
    #[default]
    Graceful,
    /// Immediately force termination.
    Force,
    /// Graceful termination without forced escalation.
    GracefulOnly,
}

/// Input for stopping one session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopSessionCommand {
    /// Stable target session identity.
    pub session_id: SessionId,
    /// Optional caller precondition checked before any stopping mutation.
    pub expected_revision: Option<undercurrent_domain::Revision>,
    /// Requested termination behavior.
    pub mode: StopMode,
}

/// Successful stop result with explicit idempotent and terminated cases.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopSessionResult {
    /// Session was already in an authoritative terminal lifecycle.
    AlreadyTerminal {
        /// Current durable session.
        session: Session,
    },
    /// A live process was terminated and terminal lifecycle was committed.
    Terminated {
        /// Current durable terminal session.
        session: Session,
        /// Confirmed runtime termination observation.
        termination: ProcessTermination,
    },
}

/// Terminal-state commit failed after process termination was confirmed.
#[derive(Debug)]
pub struct TerminalSettlementFailure {
    primary: SessionMutationError,
    reconciliation: Option<SessionMutationError>,
}

impl TerminalSettlementFailure {
    /// Returns the first failed terminal-state commit or reload.
    pub const fn primary(&self) -> &SessionMutationError {
        &self.primary
    }

    /// Returns the final reconciliation failure when it differs from primary.
    pub const fn reconciliation(&self) -> Option<&SessionMutationError> {
        self.reconciliation.as_ref()
    }

    fn from_latest(primary: Option<SessionMutationError>, latest: SessionMutationError) -> Self {
        match primary {
            Some(primary) => Self {
                primary,
                reconciliation: Some(latest),
            },
            None => Self {
                primary: latest,
                reconciliation: None,
            },
        }
    }
}

impl Display for TerminalSettlementFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "could not settle confirmed process termination: {}",
            self.primary
        )?;
        if self.reconciliation.is_some() {
            formatter.write_str("; bounded reconciliation also failed")?;
        }
        Ok(())
    }
}

impl Error for TerminalSettlementFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.primary)
    }
}

/// Typed failure from the stop-session use case.
#[derive(Debug)]
pub enum StopSessionError {
    /// Ordered load or stopping-state commit failed before process termination.
    Mutation(SessionMutationError),
    /// Active lifecycle had no owned process instance.
    ProcessUnavailable {
        /// Stable session identity.
        session_id: SessionId,
        /// Durable lifecycle observed while ownership was missing.
        lifecycle: LifecycleState,
    },
    /// Supervisor process directory failed.
    ProcessDirectory(PortError),
    /// Runtime could not confirm requested termination; durable state remains
    /// nonterminal and process ownership is retained.
    Termination(PortError),
    /// Process terminated but terminal lifecycle could not be reconciled.
    TerminalSettlement(TerminalSettlementFailure),
    /// Process and lifecycle are terminal but stale supervisor ownership could
    /// not be removed.
    DirectoryCleanup {
        /// Successfully committed terminal session.
        session: Box<Session>,
        /// Confirmed termination result.
        termination: ProcessTermination,
        /// Directory adapter failure.
        source: PortError,
    },
    /// A replacement process appeared after termination of the requested
    /// instance; its binding was preserved.
    DirectoryChanged {
        /// Successfully committed terminal session.
        session: Box<Session>,
        /// Confirmed termination result for the old process.
        termination: ProcessTermination,
    },
}

impl StopSessionError {
    /// Returns stable category used by protocol and presentation adapters.
    pub const fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::Mutation(error) => error.category(),
            Self::ProcessUnavailable { .. } => ApplicationErrorCategory::FailedPrecondition,
            Self::ProcessDirectory(error) | Self::Termination(error) => error.category(),
            Self::TerminalSettlement(error) => error.primary().category(),
            Self::DirectoryCleanup { source, .. } => source.category(),
            Self::DirectoryChanged { .. } => ApplicationErrorCategory::Conflict,
        }
    }
}

impl Display for StopSessionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mutation(error) => Display::fmt(error, formatter),
            Self::ProcessUnavailable {
                session_id,
                lifecycle,
            } => write!(
                formatter,
                "session {session_id} is {lifecycle} but has no supervised process"
            ),
            Self::ProcessDirectory(error) => {
                write!(formatter, "could not resolve supervised process: {error}")
            }
            Self::Termination(error) => {
                write!(formatter, "could not confirm process termination: {error}")
            }
            Self::TerminalSettlement(error) => Display::fmt(error, formatter),
            Self::DirectoryCleanup { .. } => {
                formatter.write_str("process terminated but supervisor-directory cleanup failed")
            }
            Self::DirectoryChanged { .. } => formatter
                .write_str("process terminated but a replacement supervisor binding was preserved"),
        }
    }
}

impl Error for StopSessionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Mutation(error) => Some(error),
            Self::ProcessDirectory(error) | Self::Termination(error) => Some(error),
            Self::TerminalSettlement(error) => Some(error),
            Self::DirectoryCleanup { source, .. } => Some(source),
            Self::ProcessUnavailable { .. } | Self::DirectoryChanged { .. } => None,
        }
    }
}

/// Serializes and executes graceful or forced termination for one session.
pub struct StopSessionService<'a> {
    core: MutationCore<'a>,
    runtime: &'a dyn ProcessRuntime,
    processes: &'a dyn SessionProcessDirectory,
    restarts: &'a dyn RestartScheduler,
    telemetry: &'a dyn Telemetry,
}

impl<'a> StopSessionService<'a> {
    /// Constructs a stop service from explicit ordering, persistence, process,
    /// restart scheduling, projection, clock, and telemetry ports. Construction
    /// performs no IO and does not panic.
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
                ApplicationOperation::StopSession,
            ),
            runtime,
            processes,
            restarts,
            telemetry,
        }
    }

    /// Stops one session and commits completion under session-local mutation ownership.
    ///
    /// Unconditional force may signal an already stopping incarnation before
    /// acquiring its lease. Settlement still takes the lease and rejects a
    /// replacement binding. Revision-conditional requests remain fully serialized.
    ///
    /// `stopping` is durable before process IO. A failed or uncertain runtime
    /// operation keeps the process binding and nonterminal lifecycle for later
    /// reconciliation. A pending automatic restart is cancelled durably before
    /// process IO. Confirmed termination is reconciled with bounded optimistic
    /// retries. Queued restart cleanup runs only after mutation ownership is
    /// released; cleanup failure is diagnosed but does not invalidate durable
    /// cancellation. Independent sessions are unaffected. This method does not
    /// panic.
    ///
    /// # Errors
    ///
    /// Returns typed ordering, persistence, ownership, process, cleanup, or
    /// reconciliation failures with redacted adapter sources.
    pub async fn execute(
        &self,
        command: StopSessionCommand,
    ) -> Result<StopSessionResult, StopSessionError> {
        let started_at = self.core.clock.now();
        let result = self.execute_inner(command).await;
        let elapsed = self.core.clock.now().saturating_duration_since(started_at);
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::OperationDurationSeconds,
            value: elapsed.as_secs_f64(),
            operation: Some(ApplicationOperation::StopSession),
        });
        result
    }

    async fn execute_inner(
        &self,
        command: StopSessionCommand,
    ) -> Result<StopSessionResult, StopSessionError> {
        self.core.emit(
            DiagnosticEventName::SessionStopRequested,
            DiagnosticLevel::Info,
            self.core.clock.now(),
            Some(command.session_id.clone()),
            None,
            None,
        );
        // Only unconditional force can run ahead of the mutation lease. Capture
        // the stopping incarnation first; never carry this force into a replacement.
        let escalated = if command.mode == StopMode::Force && command.expected_revision.is_none() {
            let snapshot = self
                .core
                .reload(&command.session_id)
                .await
                .map_err(StopSessionError::Mutation)?;
            if snapshot.lifecycle() == LifecycleState::Stopping {
                let process = self
                    .processes
                    .lookup(&command.session_id)
                    .map_err(StopSessionError::ProcessDirectory)?;
                if let Some(process) = process.filter(|process| {
                    snapshot
                        .process()
                        .is_some_and(|durable| process.matches(durable))
                }) {
                    let admission = process_mutation_admission(
                        None,
                        command.session_id.clone(),
                        ProcessMutationKind::ForceStop,
                        format!("{:?}:{:?}", process.id, StopMode::Force).as_bytes(),
                        self.core.clock.now(),
                    );
                    let termination = self
                        .runtime
                        .force_stop_admitted(&admission, &command.session_id, &process.id)
                        .await
                        .map_err(StopSessionError::Termination)?;
                    Some((process.id, termination))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        let (lease, mut session) = self
            .core
            .begin_at_revision(&command.session_id, command.expected_revision)
            .await
            .map_err(StopSessionError::Mutation)?;
        if let Some((process, _)) = &escalated
            && !session.lifecycle().is_terminal()
            && session
                .process()
                .is_none_or(|current| current.id() != process)
        {
            return Err(StopSessionError::ProcessUnavailable {
                session_id: session.id().clone(),
                lifecycle: session.lifecycle(),
            });
        }
        let result = if let Some((process, termination)) =
            escalated.filter(|_| !session.lifecycle().is_terminal())
        {
            self.commit_pending_restart_cancellation(&mut session)
                .await?;
            self.finish_termination(session, &process, termination)
                .await
        } else {
            self.execute_with_owned_session(command, session).await
        };
        let terminal_session_id = match &result {
            Ok(
                StopSessionResult::AlreadyTerminal { session }
                | StopSessionResult::Terminated { session, .. },
            ) => Some(session.id().clone()),
            Err(
                StopSessionError::DirectoryCleanup { session, .. }
                | StopSessionError::DirectoryChanged { session, .. },
            ) => Some(session.id().clone()),
            Err(_) => None,
        };
        drop(lease);
        if let Some(session_id) = terminal_session_id.as_ref() {
            self.cancel_queued_restarts(session_id).await;
        }
        result
    }

    /// Executes the existing stop mechanics while the caller retains the
    /// session-local mutation lease. This is shared by attached cancellation;
    /// it does not acquire another lease or release queued restarts.
    pub(crate) async fn execute_with_owned_session(
        &self,
        command: StopSessionCommand,
        mut session: Session,
    ) -> Result<StopSessionResult, StopSessionError> {
        self.commit_pending_restart_cancellation(&mut session)
            .await?;
        if session.lifecycle().is_terminal() {
            return Ok(StopSessionResult::AlreadyTerminal { session });
        }

        let process = match self.processes.lookup(session.id()) {
            Ok(Some(process)) => process,
            Ok(None) => {
                return Err(StopSessionError::ProcessUnavailable {
                    session_id: session.id().clone(),
                    lifecycle: session.lifecycle(),
                });
            }
            Err(error) => {
                self.core.emit_port_failure(
                    self.core.clock.now(),
                    Some(session.id().clone()),
                    &error,
                );
                return Err(StopSessionError::ProcessDirectory(error));
            }
        };
        if !session
            .process()
            .is_some_and(|durable| process.matches(durable))
        {
            return Err(StopSessionError::ProcessUnavailable {
                session_id: session.id().clone(),
                lifecycle: session.lifecycle(),
            });
        }

        if command.mode == StopMode::GracefulOnly
            && !session.configuration().execution().stop().no_escalation()
        {
            let expected = session.revision();
            let now = not_before(self.core.clock.now(), session.updated_at());
            let config = session
                .configuration()
                .clone()
                .with_stop_no_escalation(true);
            if let Some(change) = session
                .update_configuration(config, now)
                .map_err(SessionMutationError::Domain)
                .map_err(StopSessionError::Mutation)?
            {
                self.core
                    .commit(&session, expected, change, now)
                    .await
                    .map_err(StopSessionError::Mutation)?;
            }
        }

        if session.lifecycle() != LifecycleState::Stopping {
            let expected_revision = session.revision();
            let changed_at = not_before(self.core.clock.now(), session.updated_at());
            let change = session
                .transition_lifecycle(LifecycleState::Stopping, changed_at)
                .map_err(SessionMutationError::Domain)
                .map_err(StopSessionError::Mutation)?
                .ok_or_else(|| {
                    StopSessionError::Mutation(SessionMutationError::InvariantViolation {
                        rule: "non-stopping active session must produce a stopping transition",
                    })
                })?;
            self.core
                .commit(&session, expected_revision, change, changed_at)
                .await
                .map_err(StopSessionError::Mutation)?;
        }

        let operation = match command.mode {
            StopMode::Graceful | StopMode::GracefulOnly => PortOperation::StopProcess,
            StopMode::Force => PortOperation::ForceProcess,
        };
        self.core.emit(
            DiagnosticEventName::ProcessStopStarted,
            DiagnosticLevel::Info,
            self.core.clock.now(),
            Some(session.id().clone()),
            Some(operation),
            None,
        );
        let mutation_kind = match command.mode {
            StopMode::Graceful | StopMode::GracefulOnly => ProcessMutationKind::Stop,
            StopMode::Force => ProcessMutationKind::ForceStop,
        };
        let termination_payload = format!("{:?}:{:?}", process.id, command.mode);
        let admission = process_mutation_admission(
            None,
            session.id().clone(),
            mutation_kind,
            termination_payload.as_bytes(),
            self.core.clock.now(),
        );
        let termination = match command.mode {
            StopMode::Graceful | StopMode::GracefulOnly => {
                self.runtime
                    .stop_admitted(
                        &admission,
                        session.id(),
                        &process.id,
                        session.configuration().execution().stop(),
                    )
                    .await
            }
            StopMode::Force => {
                self.runtime
                    .force_stop_admitted(&admission, session.id(), &process.id)
                    .await
            }
        };
        let termination = match termination {
            Ok(termination) => termination,
            Err(error) => {
                self.core.emit_port_failure(
                    self.core.clock.now(),
                    Some(session.id().clone()),
                    &error,
                );
                self.core.emit(
                    DiagnosticEventName::ProcessStopFailed,
                    DiagnosticLevel::for_port_error(error.kind()),
                    self.core.clock.now(),
                    Some(session.id().clone()),
                    Some(operation),
                    Some(error.kind()),
                );
                return Err(StopSessionError::Termination(error));
            }
        };
        if termination.kind() == crate::TerminationKind::GracefulTimedOut {
            return Err(StopSessionError::Termination(PortError::new(
                crate::PortErrorKind::Timeout,
                operation,
                Some(session.id().clone()),
                std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "graceful deadline elapsed; process remains owned",
                ),
            )));
        }
        self.core.emit(
            DiagnosticEventName::ProcessStopSucceeded,
            DiagnosticLevel::Info,
            termination.observed_at(),
            Some(session.id().clone()),
            Some(operation),
            None,
        );

        self.finish_termination(session, &process.id, termination)
            .await
    }

    async fn finish_termination(
        &self,
        session: Session,
        process: &undercurrent_domain::ProcessInstanceId,
        termination: ProcessTermination,
    ) -> Result<StopSessionResult, StopSessionError> {
        let cleanup = self.processes.unbind_if(session.id(), process);
        if let Err(error) = &cleanup {
            self.core
                .emit_port_failure(self.core.clock.now(), Some(session.id().clone()), error);
        }
        let session = self
            .settle_terminal(session, termination)
            .await
            .map_err(StopSessionError::TerminalSettlement)?;
        self.core.emit(
            DiagnosticEventName::TerminalLifecycleCommitted,
            DiagnosticLevel::Info,
            self.core.clock.now(),
            Some(session.id().clone()),
            Some(PortOperation::CommitSession),
            None,
        );
        match cleanup {
            Ok(ProcessUnbindOutcome::Unbound(_) | ProcessUnbindOutcome::Missing) => {}
            Ok(ProcessUnbindOutcome::ProcessMismatch) => {
                return Err(StopSessionError::DirectoryChanged {
                    session: Box::new(session),
                    termination,
                });
            }
            Err(source) => {
                return Err(StopSessionError::DirectoryCleanup {
                    session: Box::new(session),
                    termination,
                    source,
                });
            }
        }
        Ok(StopSessionResult::Terminated {
            session,
            termination,
        })
    }

    async fn commit_pending_restart_cancellation(
        &self,
        session: &mut Session,
    ) -> Result<(), StopSessionError> {
        let expected_revision = session.revision();
        let changed_at = not_before(self.core.clock.now(), session.updated_at());
        let Some(change) = session
            .cancel_pending_restart(changed_at)
            .map_err(SessionMutationError::Domain)
            .map_err(StopSessionError::Mutation)?
        else {
            return Ok(());
        };
        self.core
            .commit(session, expected_revision, change, changed_at)
            .await
            .map_err(StopSessionError::Mutation)?;
        self.core.emit(
            DiagnosticEventName::RestartCancellationCommitted,
            DiagnosticLevel::Info,
            changed_at,
            Some(session.id().clone()),
            Some(PortOperation::CommitSession),
            None,
        );
        Ok(())
    }

    pub(crate) async fn cancel_queued_restarts(&self, session_id: &SessionId) {
        match self.restarts.cancel_session_restarts(session_id).await {
            Ok(_) => self.core.emit(
                DiagnosticEventName::RestartQueueCancellationCompleted,
                DiagnosticLevel::Debug,
                self.core.clock.now(),
                Some(session_id.clone()),
                Some(PortOperation::CancelSessionRestarts),
                None,
            ),
            Err(error) => {
                let observed_at = self.core.clock.now();
                self.core
                    .emit_port_failure(observed_at, Some(session_id.clone()), &error);
                self.core.emit(
                    DiagnosticEventName::RestartQueueCancellationFailed,
                    DiagnosticLevel::for_port_error(error.kind()),
                    observed_at,
                    Some(session_id.clone()),
                    Some(PortOperation::CancelSessionRestarts),
                    Some(error.kind()),
                );
            }
        }
    }

    async fn settle_terminal(
        &self,
        mut session: Session,
        termination: ProcessTermination,
    ) -> Result<Session, TerminalSettlementFailure> {
        let mut primary = None;
        for attempt in 0..MAX_TERMINAL_RECONCILIATION_ATTEMPTS {
            if session.lifecycle().is_terminal() {
                self.core.project(&session, self.core.clock.now());
                return Ok(session);
            }
            let expected_revision = session.revision();
            let changed_at = not_before(termination.observed_at(), session.updated_at());
            let change = match session.transition_lifecycle(termination.lifecycle(), changed_at) {
                Ok(Some(change)) => change,
                Ok(None) => return Ok(session),
                Err(error) => {
                    let failure = SessionMutationError::Domain(error);
                    return Err(TerminalSettlementFailure::from_latest(primary, failure));
                }
            };
            match self
                .core
                .commit(&session, expected_revision, change, changed_at)
                .await
            {
                Ok(()) => return Ok(session),
                Err(error) if attempt + 1 == MAX_TERMINAL_RECONCILIATION_ATTEMPTS => {
                    return Err(TerminalSettlementFailure::from_latest(primary, error));
                }
                Err(error) => {
                    if primary.is_none() {
                        primary = Some(error);
                    }
                    match self.core.reload(session.id()).await {
                        Ok(reloaded) => session = reloaded,
                        Err(error) => {
                            return Err(TerminalSettlementFailure::from_latest(primary, error));
                        }
                    }
                }
            }
        }
        Err(TerminalSettlementFailure::from_latest(
            primary,
            SessionMutationError::InvariantViolation {
                rule: "terminal reconciliation exhausted without a final attempt result",
            },
        ))
    }
}
