// SPDX-License-Identifier: Apache-2.0

//! Transactional start-session orchestration and compensation.

use std::error::Error;
use std::fmt::{Display, Formatter};

use undercurrent_domain::{
    DomainChange, DomainError, EffectCertainty, FencingToken, LifecycleState,
    OperationAttemptState, PayloadDigest, ProcessIdentity, Revision, Session, SessionConfiguration,
    SessionId, SessionName, TimestampMillis,
};

use super::activation::{
    ProcessActivationFailure, ProcessActivationFailureSource, ProcessActivationStage,
    ProcessBindingState, activate_process, compensate_process,
};
use super::{MutationCore, SessionMutationError};

use crate::{
    ApplicationErrorCategory, ApplicationOperation, Clock, CommitSessionOutcome,
    DiagnosticEventName, DiagnosticLevel, DiagnosticRecord, InsertSessionOutcome, MetricName,
    MetricObservation, OperationAttemptAdmission, OperationAttemptCompensation,
    OperationAttemptCompensationOutcome, OperationAttemptCompensationPreparation,
    OperationAttemptCompensationPreparationOutcome, OperationAttemptSettlement,
    OperationAttemptSettlementOutcome, OperationAttemptStore, PortError, PortErrorKind,
    PortOperation, ProcessInstance, ProcessRuntime, SessionIdSource, SessionMutationCoordinator,
    SessionOutboxEvent, SessionProcessDirectory, SessionProjectionWriter, SessionRepository,
    Telemetry, process_instance_result_digest,
};

const MAX_RECONCILIATION_ATTEMPTS: usize = 3;

/// Validated inputs for starting one managed session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartSessionCommand {
    /// Unique human-facing name.
    pub name: SessionName,
    /// Durable launch, restart, health, detection, label, and metadata settings.
    pub configuration: SessionConfiguration,
    /// Hard maximum number of explicit WorkContext declarations.
    pub max_work_items: usize,
    /// Complete caller-fenced dispatch admission, absent for standalone starts.
    pub dispatch: Option<OperationAttemptAdmission>,
}

/// Successful start result after the running lifecycle is durable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartedSession {
    /// Authoritative durable session at running revision.
    pub session: Session,
    /// Exact supervised process instance created by this command.
    pub process: ProcessInstance,
}

/// Stage at which a post-spawn failure occurred.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StartFailureStage {
    /// Concrete process ownership could not be represented in the aggregate.
    RecordProcessOwnership,
    /// Concrete process ownership could not be committed durably.
    CommitProcessOwnership,
    /// The process could not be registered with its session supervisor.
    BindProcess,
    /// Retained exit observation could not be armed after exact binding.
    ArmProcessExit,
    /// The process start timestamp violated a domain invariant.
    EnterRunning,
    /// The running lifecycle could not be committed durably.
    CommitRunning,
    /// The fully activated dispatched result could not be committed under its caller fence.
    CommitDispatch,
}

impl StartFailureStage {
    /// Returns the stable diagnostic name for this failure stage.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RecordProcessOwnership => "record_process_ownership",
            Self::CommitProcessOwnership => "commit_process_ownership",
            Self::BindProcess => "bind_process",
            Self::ArmProcessExit => "arm_process_exit",
            Self::EnterRunning => "enter_running",
            Self::CommitRunning => "commit_running",
            Self::CommitDispatch => "commit_dispatch",
        }
    }
}

/// Typed primary failure after a process was spawned.
#[derive(Debug)]
pub enum StartOperationFailure {
    /// Pure domain invariant rejected a transition.
    Domain(DomainError),
    /// An outer adapter failed.
    Port(PortError),
    /// Durable state changed concurrently.
    RevisionConflict {
        /// Current durable revision reported by the repository.
        current_revision: Revision,
    },
    /// Durable name uniqueness changed after initial creation.
    NameConflict {
        /// Existing stable identity that owns the name.
        existing_session_id: SessionId,
    },
    /// An application invariant was violated without an adapter failure.
    InvariantViolation {
        /// Stable, payload-safe violated rule.
        rule: &'static str,
    },
}

impl Display for StartOperationFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Domain(error) => Display::fmt(error, formatter),
            Self::Port(error) => Display::fmt(error, formatter),
            Self::RevisionConflict { current_revision } => write!(
                formatter,
                "session revision changed concurrently to {}",
                current_revision.value()
            ),
            Self::NameConflict {
                existing_session_id,
            } => write!(
                formatter,
                "session name became owned by {existing_session_id} during start"
            ),
            Self::InvariantViolation { rule } => {
                write!(formatter, "start-session invariant failed: {rule}")
            }
        }
    }
}

impl Error for StartOperationFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Domain(error) => Some(error),
            Self::Port(error) => Some(error),
            Self::RevisionConflict { .. }
            | Self::NameConflict { .. }
            | Self::InvariantViolation { .. } => None,
        }
    }
}

/// Failure encountered while settling durable state after a start failure.
#[derive(Debug)]
pub enum SettlementFailure {
    /// Domain state could not transition to failed.
    Domain(DomainError),
    /// Repository or projection infrastructure failed.
    Port(PortError),
    /// Reconciliation exhausted its bounded optimistic retries.
    RevisionConflict {
        /// Most recent durable revision reported by the repository.
        current_revision: Revision,
    },
    /// Durable name uniqueness changed during failed-state settlement.
    NameConflict {
        /// Existing stable identity that owns the name.
        existing_session_id: SessionId,
    },
    /// Durable session was missing during reconciliation.
    SessionMissing,
    /// Exact activated revision, owner, or supervisor binding changed before cleanup.
    OwnershipChanged {
        /// Current durable revision observed before refusing stale cleanup.
        current_revision: Revision,
    },
    /// Exact cleanup completed but its terminal compensation audit was rejected.
    AttemptCompensationRejected,
}

impl Display for SettlementFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Domain(error) => write!(formatter, "failed-state transition rejected: {error}"),
            Self::Port(error) => write!(formatter, "failed-state settlement failed: {error}"),
            Self::RevisionConflict { current_revision } => write!(
                formatter,
                "failed-state settlement conflicted at revision {}",
                current_revision.value()
            ),
            Self::NameConflict {
                existing_session_id,
            } => write!(
                formatter,
                "failed-state settlement found session name owned by {existing_session_id}"
            ),
            Self::SessionMissing => {
                formatter.write_str("session was missing during failed-state settlement")
            }
            Self::OwnershipChanged { current_revision } => write!(
                formatter,
                "exact activated owner changed before cleanup at revision {}",
                current_revision.value()
            ),
            Self::AttemptCompensationRejected => {
                formatter.write_str("exact cleanup completed but compensation audit was rejected")
            }
        }
    }
}

impl Error for SettlementFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Domain(error) => Some(error),
            Self::Port(error) => Some(error),
            Self::RevisionConflict { .. }
            | Self::NameConflict { .. }
            | Self::SessionMissing
            | Self::OwnershipChanged { .. }
            | Self::AttemptCompensationRejected => None,
        }
    }
}

/// Best-effort recovery results retained on a post-spawn failure.
#[derive(Debug, Default)]
pub struct StartRecoveryReport {
    force_stop_failure: Option<PortError>,
    unbind_failure: Option<PortError>,
    unbind_conflict: bool,
    settlement_failure: Option<SettlementFailure>,
    cleanup_required_process: Option<ProcessInstance>,
}

impl StartRecoveryReport {
    /// Returns whether the spawned process was confirmed terminated and durable
    /// lifecycle reconciliation completed.
    pub const fn completed(&self) -> bool {
        self.force_stop_failure.is_none()
            && self.unbind_failure.is_none()
            && !self.unbind_conflict
            && self.settlement_failure.is_none()
            && self.cleanup_required_process.is_none()
    }

    /// Returns forced-termination failure, if process state remains uncertain.
    pub const fn force_stop_failure(&self) -> Option<&PortError> {
        self.force_stop_failure.as_ref()
    }

    /// Returns supervisor-directory cleanup failure, if any.
    pub const fn unbind_failure(&self) -> Option<&PortError> {
        self.unbind_failure.as_ref()
    }

    /// Returns whether a replacement process prevented exact binding cleanup.
    pub const fn unbind_conflict(&self) -> bool {
        self.unbind_conflict
    }

    /// Returns durable failed-state reconciliation failure, if any.
    pub const fn settlement_failure(&self) -> Option<&SettlementFailure> {
        self.settlement_failure.as_ref()
    }

    /// Returns the exact incarnation requiring operator repair when cleanup was incomplete.
    pub const fn cleanup_required_process(&self) -> Option<&ProcessInstance> {
        self.cleanup_required_process.as_ref()
    }
}

/// Typed failure from the start-session use case.
#[derive(Debug)]
pub enum StartSessionError {
    /// A dispatched start was not wired to the durable attempt store.
    DispatchStoreRequired,
    /// Durable dispatch replay state could not be read.
    DispatchPersistence(PortError),
    /// The retry changed immutable dispatch semantics or a known-absent attempt.
    DispatchConflict,
    /// No new effect may begin because the attached runtime policy is absent or expired.
    DispatchPolicyNotCurrent,
    /// The prior attempt is admitted or has unknown effect and must be reconciled.
    DispatchReconciliationRequired,
    /// The durable applied result cannot currently be reconstructed without execution.
    DispatchResultUnavailable,
    /// Settlement outcome is unknown; the exact live process is retained for reconciliation.
    DispatchSettlementUnknown {
        /// Failure that made final settlement inconclusive.
        source: Box<StartOperationFailure>,
        /// Last fully activated canonical session state.
        session: Box<Session>,
        /// Exact live incarnation that must not be destructively guessed away.
        process: ProcessInstance,
    },
    /// Session identity generation failed before any durable mutation.
    Identity(PortError),
    /// Executable or environment-bound paths could not be pinned before acceptance.
    LaunchPreparation(PortError),
    /// Session-local mutation ownership could not be acquired.
    Ordering(PortError),
    /// Session construction rejected validated inputs.
    Domain(DomainError),
    /// Generated stable ID already exists.
    IdConflict {
        /// Conflicting generated identity.
        session_id: SessionId,
    },
    /// Requested name already belongs to another session.
    NameConflict {
        /// Requested name.
        name: SessionName,
        /// Existing owning identity.
        existing_session_id: SessionId,
    },
    /// Initial session and outbox insertion failed.
    InitialCommit(PortError),
    /// Process creation failed; durable settlement may also have failed.
    Spawn {
        /// Process adapter failure.
        source: PortError,
        /// Additional failed-state settlement failure.
        settlement: Option<SettlementFailure>,
    },
    /// A failure after process creation triggered compensating termination.
    PartialStart {
        /// Failed stage.
        stage: StartFailureStage,
        /// Primary failure.
        source: StartOperationFailure,
        /// Results of compensating termination and reconciliation.
        recovery: StartRecoveryReport,
    },
}

impl StartSessionError {
    /// Returns stable category used by protocol and presentation adapters.
    pub fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::DispatchStoreRequired | Self::DispatchResultUnavailable => {
                ApplicationErrorCategory::InvariantViolation
            }
            Self::DispatchSettlementUnknown { source, .. } => match source.as_ref() {
                StartOperationFailure::Domain(error) => {
                    ApplicationErrorCategory::from_domain(error.category())
                }
                StartOperationFailure::RevisionConflict { .. }
                | StartOperationFailure::NameConflict { .. } => ApplicationErrorCategory::Conflict,
                StartOperationFailure::Port(error) => error.category(),
                StartOperationFailure::InvariantViolation { .. } => {
                    ApplicationErrorCategory::InvariantViolation
                }
            },
            Self::DispatchPersistence(error) => error.category(),
            Self::DispatchConflict
            | Self::DispatchPolicyNotCurrent
            | Self::DispatchReconciliationRequired => ApplicationErrorCategory::Conflict,
            Self::Identity(error)
            | Self::LaunchPreparation(error)
            | Self::Ordering(error)
            | Self::InitialCommit(error) => error.category(),
            Self::Spawn { source, .. } => source.category(),
            Self::Domain(error) => ApplicationErrorCategory::from_domain(error.category()),
            Self::IdConflict { .. } | Self::NameConflict { .. } => {
                ApplicationErrorCategory::Conflict
            }
            Self::PartialStart { source, .. } => match source {
                StartOperationFailure::Domain(error) => {
                    ApplicationErrorCategory::from_domain(error.category())
                }
                StartOperationFailure::RevisionConflict { .. } => {
                    ApplicationErrorCategory::Conflict
                }
                StartOperationFailure::NameConflict { .. } => ApplicationErrorCategory::Conflict,
                StartOperationFailure::Port(error) => error.category(),
                StartOperationFailure::InvariantViolation { .. } => {
                    ApplicationErrorCategory::InvariantViolation
                }
            },
        }
    }
}

impl Display for StartSessionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DispatchStoreRequired => formatter
                .write_str("dispatched start requires the product-owned durable attempt store"),
            Self::DispatchPersistence(error) => {
                write!(formatter, "could not read durable dispatch state: {error}")
            }
            Self::DispatchConflict => {
                formatter.write_str("dispatch retry conflicts with durable immutable binding")
            }
            Self::DispatchPolicyNotCurrent => formatter
                .write_str("dispatch runtime policy is absent or expired before new execution"),
            Self::DispatchReconciliationRequired => formatter
                .write_str("dispatch effect is pending or unknown and requires reconciliation"),
            Self::DispatchResultUnavailable => formatter.write_str(
                "durable dispatch result is not currently reconstructable without execution",
            ),
            Self::DispatchSettlementUnknown { source, .. } => write!(
                formatter,
                "dispatch settlement is inconclusive and its exact live process is retained: {source}"
            ),
            Self::Identity(error) => {
                write!(formatter, "could not allocate session identity: {error}")
            }
            Self::LaunchPreparation(error) => {
                write!(
                    formatter,
                    "could not prepare pinned session launch: {error}"
                )
            }
            Self::Ordering(error) => {
                write!(formatter, "could not order session start: {error}")
            }
            Self::Domain(error) => Display::fmt(error, formatter),
            Self::IdConflict { session_id } => {
                write!(formatter, "session identity {session_id} already exists")
            }
            Self::NameConflict {
                name,
                existing_session_id,
            } => write!(
                formatter,
                "session name {name} already belongs to {existing_session_id}"
            ),
            Self::InitialCommit(error) => {
                write!(formatter, "could not create durable session: {error}")
            }
            Self::Spawn { source, settlement } => {
                write!(formatter, "could not spawn session process: {source}")?;
                if settlement.is_some() {
                    formatter.write_str("; durable failed-state settlement was incomplete")?;
                }
                Ok(())
            }
            Self::PartialStart {
                stage,
                source,
                recovery,
            } => {
                write!(
                    formatter,
                    "session start failed at {}: {source}",
                    stage.as_str()
                )?;
                if !recovery.completed() {
                    formatter.write_str("; recovery was incomplete")?;
                }
                Ok(())
            }
        }
    }
}

impl Error for StartSessionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::DispatchPersistence(error) => Some(error),
            Self::DispatchSettlementUnknown { source, .. } => Some(source.as_ref()),
            Self::Identity(error)
            | Self::LaunchPreparation(error)
            | Self::Ordering(error)
            | Self::InitialCommit(error) => Some(error),
            Self::Domain(error) => Some(error),
            Self::Spawn { source, .. } => Some(source),
            Self::PartialStart { source, .. } => Some(source),
            Self::DispatchStoreRequired
            | Self::DispatchConflict
            | Self::DispatchPolicyNotCurrent
            | Self::DispatchReconciliationRequired
            | Self::DispatchResultUnavailable
            | Self::IdConflict { .. }
            | Self::NameConflict { .. } => None,
        }
    }
}

#[derive(Debug)]
enum DispatchSettlementFailure {
    Superseded {
        source: StartOperationFailure,
        current: FencingToken,
    },
    Ambiguous {
        source: StartOperationFailure,
    },
}

/// Orchestrates durable creation, process ownership, lifecycle commit, and
/// compensation through inward-facing ports.
pub struct StartSessionService<'a> {
    clock: &'a dyn Clock,
    ids: &'a dyn SessionIdSource,
    coordinator: &'a dyn SessionMutationCoordinator,
    repository: &'a dyn SessionRepository,
    runtime: &'a dyn ProcessRuntime,
    processes: &'a dyn SessionProcessDirectory,
    projection: &'a dyn SessionProjectionWriter,
    telemetry: &'a dyn Telemetry,
    attempts: Option<&'a dyn OperationAttemptStore>,
}

impl<'a> StartSessionService<'a> {
    /// Constructs a start-session service from explicit boundary ports.
    ///
    /// The service borrows all dependencies, performs no work during
    /// construction, and does not panic.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        clock: &'a dyn Clock,
        ids: &'a dyn SessionIdSource,
        coordinator: &'a dyn SessionMutationCoordinator,
        repository: &'a dyn SessionRepository,
        runtime: &'a dyn ProcessRuntime,
        processes: &'a dyn SessionProcessDirectory,
        projection: &'a dyn SessionProjectionWriter,
        telemetry: &'a dyn Telemetry,
    ) -> Self {
        Self {
            clock,
            ids,
            coordinator,
            repository,
            runtime,
            processes,
            projection,
            telemetry,
            attempts: None,
        }
    }

    /// Attaches product-owned durable attempt state required by dispatched starts.
    pub const fn with_operation_attempt_store(
        mut self,
        attempts: &'a dyn OperationAttemptStore,
    ) -> Self {
        self.attempts = Some(attempts);
        self
    }

    /// Starts one process and returns only after its running lifecycle is
    /// durable and its process instance is owned by the supervisor directory.
    ///
    /// The repository insert and each lifecycle event are atomic through the
    /// repository port. Activation failures after process creation trigger
    /// compensating forced termination; confirmed cleanup permits bounded
    /// optimistic reconciliation to `failed`. An ambiguous final dispatch
    /// settlement instead retains the exact live process for reconciliation.
    /// A confirmed superseding fence permits cleanup only after exact ownership
    /// checks and durable compensation preparation. Receipt-acknowledgement
    /// failure after dispatch settlement also preserves the live process.
    /// Projection updates are cache maintenance: failures are diagnosed but do
    /// not turn an already durable command into a false failure. This method
    /// does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed validation, uniqueness, infrastructure, spawn, commit, or
    /// recovery failures. [`Error::source`] retains concrete adapter failures
    /// while `Display` and `Debug` remain payload-safe.
    pub async fn execute(
        &self,
        command: StartSessionCommand,
    ) -> Result<StartedSession, StartSessionError> {
        let operation_started_at = self.clock.now();
        let result = self.execute_inner(command).await;
        let elapsed = self
            .clock
            .now()
            .saturating_duration_since(operation_started_at);
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::OperationDurationSeconds,
            value: elapsed.as_secs_f64(),
            operation: Some(ApplicationOperation::StartSession),
        });
        result
    }

    async fn execute_inner(
        &self,
        mut command: StartSessionCommand,
    ) -> Result<StartedSession, StartSessionError> {
        let requested_at = self.clock.now();
        // An exact committed start remains replayable after its admission policy
        // expires. Check retained truth first so response-loss recovery does not
        // require renewed admission; only new work must pass the current policy.
        // See ../../docs/internals/sessions/lifecycle.md#dispatched-creation-and-lost-responses.
        if let Some(dispatch) = command.dispatch.as_ref()
            && let Some(replayed) = self.replay_dispatch(dispatch).await?
        {
            return Ok(replayed);
        }
        if !self.dispatch_policy_is_current(command.dispatch.as_ref()) {
            return Err(StartSessionError::DispatchPolicyNotCurrent);
        }
        self.emit(
            DiagnosticEventName::SessionStartRequested,
            DiagnosticLevel::Info,
            requested_at,
            None,
            None,
        );

        let prepared_launch = match self
            .runtime
            .prepare_launch(command.configuration.launch())
            .await
        {
            Ok(prepared_launch) => prepared_launch,
            Err(error) => {
                self.emit_port_failure(requested_at, None, &error);
                return Err(StartSessionError::LaunchPreparation(error));
            }
        };
        command.configuration = command.configuration.with_prepared_launch(prepared_launch);

        let session_id = match self.ids.next_id() {
            Ok(session_id) => session_id,
            Err(error) => {
                self.emit_port_failure(requested_at, None, &error);
                return Err(StartSessionError::Identity(error));
            }
        };
        let _lease = match self.coordinator.acquire(&session_id).await {
            Ok(lease) => lease,
            Err(error) => {
                self.emit_port_failure(requested_at, Some(session_id), &error);
                return Err(StartSessionError::Ordering(error));
            }
        };
        let (session, created) = Session::create(
            session_id.clone(),
            command.name.clone(),
            command.configuration,
            command.max_work_items,
            requested_at,
        )
        .map_err(StartSessionError::Domain)?;
        let created_event = outbox_event(&session, created, requested_at);
        if !self.dispatch_policy_is_current(command.dispatch.as_ref()) {
            return Err(StartSessionError::DispatchPolicyNotCurrent);
        }

        match self.repository.insert(&session, &created_event).await {
            Ok(InsertSessionOutcome::Inserted) => {}
            Ok(InsertSessionOutcome::IdConflict) => {
                return Err(StartSessionError::IdConflict { session_id });
            }
            Ok(InsertSessionOutcome::NameConflict {
                existing_session_id,
            }) => {
                return Err(StartSessionError::NameConflict {
                    name: command.name,
                    existing_session_id,
                });
            }
            Err(error) => {
                self.emit_port_failure(requested_at, Some(session.id().clone()), &error);
                return Err(StartSessionError::InitialCommit(error));
            }
        }
        self.emit(
            DiagnosticEventName::SessionCreated,
            DiagnosticLevel::Info,
            requested_at,
            Some(session.id().clone()),
            Some(PortOperation::InsertSession),
        );
        self.update_projection(&session, requested_at);

        let core = MutationCore::new(
            self.clock,
            self.coordinator,
            self.repository,
            self.projection,
            self.telemetry,
            ApplicationOperation::StartSession,
        );
        match activate_process(
            &core,
            self.runtime,
            self.processes,
            session,
            command.dispatch.as_ref(),
        )
        .await
        {
            Ok(activated) => {
                if let Some(dispatch) = command.dispatch.as_ref() {
                    let committed = match self
                        .settle_dispatch_result(dispatch, &activated.session, &activated.process)
                        .await
                    {
                        Ok(committed) => committed,
                        Err(DispatchSettlementFailure::Superseded { source, current }) => {
                            return Err(self
                                .recover_dispatch_settlement_failure(
                                    dispatch, activated, source, current,
                                )
                                .await);
                        }
                        Err(DispatchSettlementFailure::Ambiguous { source }) => {
                            return Err(StartSessionError::DispatchSettlementUnknown {
                                source: Box::new(source),
                                session: Box::new(activated.session),
                                process: activated.process,
                            });
                        }
                    };
                    if let Err(error) = self.runtime.acknowledge_attempt(&committed).await {
                        // The semantic result is already durable. Preserve the live managed
                        // process so an exact retry can replay it rather than report false loss.
                        return Err(StartSessionError::DispatchPersistence(error));
                    }
                }
                Ok(StartedSession {
                    session: activated.session,
                    process: activated.process,
                })
            }
            Err(failure) => Err(self.recover_activation_failure(failure).await),
        }
    }

    async fn settle_dispatch_result(
        &self,
        dispatch: &OperationAttemptAdmission,
        session: &Session,
        process: &ProcessInstance,
    ) -> Result<undercurrent_domain::OperationAttempt, DispatchSettlementFailure> {
        let attempts = self.attempts.ok_or(DispatchSettlementFailure::Ambiguous {
            source: StartOperationFailure::InvariantViolation {
                rule: "dispatched start lost its durable attempt store after admission",
            },
        })?;
        let fencing_token =
            dispatch
                .supplied_fencing_token()
                .ok_or(DispatchSettlementFailure::Ambiguous {
                    source: StartOperationFailure::InvariantViolation {
                        rule: "dispatched start lost its caller fencing token after admission",
                    },
                })?;
        let settlement = OperationAttemptSettlement {
            attempt_id: dispatch.attempt_id().clone(),
            fencing_token,
            certainty: EffectCertainty::KnownApplied,
            result_digest: Some(process_instance_result_digest(process)),
            settled_at: self.clock.now(),
        };
        let outcome = attempts.settle(&settlement).await;
        match outcome {
            Err(error) => {
                self.resolve_dispatch_settlement(
                    attempts,
                    dispatch,
                    session,
                    process,
                    None,
                    StartOperationFailure::Port(error),
                )
                .await
            }
            Ok(
                OperationAttemptSettlementOutcome::Settled(attempt)
                | OperationAttemptSettlementOutcome::Duplicate(attempt),
            ) => Ok(attempt),
            Ok(OperationAttemptSettlementOutcome::StaleFence { current }) => {
                self.resolve_dispatch_settlement(
                    attempts,
                    dispatch,
                    session,
                    process,
                    Some(current),
                    StartOperationFailure::InvariantViolation {
                        rule: "fully activated dispatch was superseded before settlement",
                    },
                )
                .await
            }
            Ok(
                OperationAttemptSettlementOutcome::NotFound
                | OperationAttemptSettlementOutcome::PayloadConflict,
            ) => {
                self.resolve_dispatch_settlement(
                    attempts,
                    dispatch,
                    session,
                    process,
                    None,
                    StartOperationFailure::InvariantViolation {
                        rule: "fully activated dispatch settlement was rejected by durable state",
                    },
                )
                .await
            }
        }
    }

    async fn resolve_dispatch_settlement(
        &self,
        attempts: &dyn OperationAttemptStore,
        dispatch: &OperationAttemptAdmission,
        session: &Session,
        process: &ProcessInstance,
        confirmed_higher: Option<FencingToken>,
        source: StartOperationFailure,
    ) -> Result<undercurrent_domain::OperationAttempt, DispatchSettlementFailure> {
        let attempt = match attempts.load(dispatch.attempt_id()).await {
            Ok(Some(attempt)) => attempt,
            Ok(None) => return Err(DispatchSettlementFailure::Ambiguous { source }),
            Err(error) => {
                return Err(DispatchSettlementFailure::Ambiguous {
                    source: StartOperationFailure::Port(error),
                });
            }
        };
        if !dispatch.has_same_dispatched_semantics(&attempt)
            || attempt.target_session() != Some(session.id())
        {
            return Err(DispatchSettlementFailure::Ambiguous { source });
        }
        let expected_digest = process_instance_result_digest(process);
        if matches!(
            attempt.state(),
            OperationAttemptState::Settled {
                certainty: EffectCertainty::KnownApplied,
                result_digest: Some(result_digest),
                ..
            } if *result_digest == expected_digest
        ) {
            match self.exact_activation_is_live(session, process).await {
                Ok(true) => return Ok(attempt),
                Ok(false) => return Err(DispatchSettlementFailure::Ambiguous { source }),
                Err(error) => {
                    return Err(DispatchSettlementFailure::Ambiguous {
                        source: StartOperationFailure::Port(error),
                    });
                }
            }
        }
        let original = dispatch
            .supplied_fencing_token()
            .expect("settlement resolution is reached only for a validated dispatch");
        if matches!(attempt.state(), OperationAttemptState::Accepted)
            && confirmed_higher.is_some_and(|current| current > original)
        {
            return Err(DispatchSettlementFailure::Superseded {
                source,
                current: confirmed_higher.expect("checked higher fence"),
            });
        }
        Err(DispatchSettlementFailure::Ambiguous { source })
    }

    async fn exact_activation_is_live(
        &self,
        activated: &Session,
        process: &ProcessInstance,
    ) -> Result<bool, PortError> {
        let Some(current) = self.repository.load(activated.id()).await? else {
            return Ok(false);
        };
        let exact_owner = current.process().is_some_and(|owner| {
            owner.id() == &process.id && owner.started_at() == process.started_at
        });
        if current.revision() != activated.revision()
            || current.lifecycle() != LifecycleState::Running
            || !exact_owner
        {
            return Ok(false);
        }
        Ok(self.processes.lookup(activated.id())?.as_ref() == Some(process))
    }

    async fn recover_dispatch_settlement_failure(
        &self,
        dispatch: &OperationAttemptAdmission,
        activated: super::activation::ActivatedProcess,
        source: StartOperationFailure,
        superseding_fencing_token: FencingToken,
    ) -> StartSessionError {
        let cleanup_target = self.repository.load(activated.session.id()).await;
        let cleanup_target = match cleanup_target {
            Ok(Some(current))
                if current.revision() == activated.session.revision()
                    && current.lifecycle() == LifecycleState::Running
                    && current.process().is_some_and(|owner| {
                        owner.id() == &activated.process.id
                            && owner.started_at() == activated.process.started_at
                    }) =>
            {
                match self.processes.lookup(activated.session.id()) {
                    Ok(Some(bound)) if bound == activated.process => Ok(()),
                    Ok(_) => Err(SettlementFailure::OwnershipChanged {
                        current_revision: current.revision(),
                    }),
                    Err(error) => Err(SettlementFailure::Port(error)),
                }
            }
            Ok(Some(current)) => Err(SettlementFailure::OwnershipChanged {
                current_revision: current.revision(),
            }),
            Ok(None) => Err(SettlementFailure::SessionMissing),
            Err(error) => Err(SettlementFailure::Port(error)),
        };
        if let Err(settlement_failure) = cleanup_target {
            return StartSessionError::PartialStart {
                stage: StartFailureStage::CommitDispatch,
                source,
                recovery: StartRecoveryReport {
                    settlement_failure: Some(settlement_failure),
                    cleanup_required_process: Some(activated.process),
                    ..StartRecoveryReport::default()
                },
            };
        }
        let cleanup_digest = dispatch_cleanup_digest(
            dispatch,
            &activated.session,
            &activated.process,
            superseding_fencing_token,
        );
        if let Err(source) = self
            .prepare_dispatch_compensation(
                dispatch,
                &activated.session,
                &activated.process,
                superseding_fencing_token,
                cleanup_digest,
            )
            .await
        {
            return StartSessionError::DispatchSettlementUnknown {
                source: Box::new(source),
                session: Box::new(activated.session),
                process: activated.process,
            };
        }
        let compensation = compensate_process(
            &MutationCore::new(
                self.clock,
                self.coordinator,
                self.repository,
                self.projection,
                self.telemetry,
                ApplicationOperation::StartSession,
            ),
            self.runtime,
            self.processes,
            &activated.session,
            &activated.process,
            ProcessBindingState::Bound,
        )
        .await;
        let safe_to_settle = compensation.safe_to_settle();
        let mut recovery = StartRecoveryReport {
            force_stop_failure: compensation.force_stop_failure,
            unbind_failure: compensation.unbind_failure,
            unbind_conflict: compensation.unbind_conflict,
            settlement_failure: if safe_to_settle {
                // Commit against the exact activated revision. A concurrently replaced owner or
                // lifecycle is preserved rather than being torn down by stale cleanup.
                self.settle_failed_session(activated.session.clone())
                    .await
                    .err()
            } else {
                None
            },
            cleanup_required_process: None,
        };
        if recovery.completed() {
            recovery.settlement_failure = self
                .terminalize_dispatch_compensation(
                    dispatch,
                    superseding_fencing_token,
                    cleanup_digest,
                )
                .await
                .err();
        }
        if !recovery.completed() {
            recovery.cleanup_required_process = Some(activated.process);
        }
        StartSessionError::PartialStart {
            stage: StartFailureStage::CommitDispatch,
            source,
            recovery,
        }
    }

    async fn prepare_dispatch_compensation(
        &self,
        dispatch: &OperationAttemptAdmission,
        session: &Session,
        process: &ProcessInstance,
        superseding_fencing_token: FencingToken,
        cleanup_digest: PayloadDigest,
    ) -> Result<(), StartOperationFailure> {
        let attempts = self
            .attempts
            .ok_or(StartOperationFailure::InvariantViolation {
                rule: "dispatch compensation lost its durable attempt store",
            })?;
        let fencing_token =
            dispatch
                .supplied_fencing_token()
                .ok_or(StartOperationFailure::InvariantViolation {
                    rule: "dispatch compensation lost its original fence",
                })?;
        let preparation = OperationAttemptCompensationPreparation::new(
            dispatch.attempt_id().clone(),
            fencing_token,
            superseding_fencing_token,
            ProcessIdentity::new(process.id.clone(), process.started_at),
            session.revision(),
            cleanup_digest,
            self.clock.now(),
        );
        let outcome = attempts.prepare_compensation(&preparation).await;
        if matches!(
            outcome,
            Ok(OperationAttemptCompensationPreparationOutcome::Prepared(_)
                | OperationAttemptCompensationPreparationOutcome::Duplicate(_))
        ) {
            return Ok(());
        }
        let source = match outcome {
            Err(error) => StartOperationFailure::Port(error),
            Ok(
                OperationAttemptCompensationPreparationOutcome::StaleFence { .. }
                | OperationAttemptCompensationPreparationOutcome::NotFound
                | OperationAttemptCompensationPreparationOutcome::NotAccepted
                | OperationAttemptCompensationPreparationOutcome::PayloadConflict,
            ) => StartOperationFailure::InvariantViolation {
                rule: "dispatch compensation intent was rejected by durable state",
            },
            Ok(
                OperationAttemptCompensationPreparationOutcome::Prepared(_)
                | OperationAttemptCompensationPreparationOutcome::Duplicate(_),
            ) => unreachable!("successful preparation returned above"),
        };
        let attempt = attempts
            .load(dispatch.attempt_id())
            .await
            .map_err(StartOperationFailure::Port)?
            .ok_or(StartOperationFailure::InvariantViolation {
                rule: "dispatch compensation intent outcome is unknown",
            })?;
        let exact_intent = attempt.compensation_intent().is_some_and(|intent| {
            intent.superseding_fencing_token() == superseding_fencing_token
                && intent.process().id() == &process.id
                && intent.process().started_at() == process.started_at
                && intent.session_revision() == session.revision()
                && intent.cleanup_digest() == cleanup_digest
        });
        if dispatch.has_same_dispatched_semantics(&attempt)
            && attempt.target_session() == Some(session.id())
            && matches!(attempt.state(), OperationAttemptState::Accepted)
            && exact_intent
        {
            Ok(())
        } else {
            Err(source)
        }
    }

    async fn terminalize_dispatch_compensation(
        &self,
        dispatch: &OperationAttemptAdmission,
        superseding_fencing_token: FencingToken,
        cleanup_digest: PayloadDigest,
    ) -> Result<(), SettlementFailure> {
        let attempts = self
            .attempts
            .ok_or(SettlementFailure::AttemptCompensationRejected)?;
        let fencing_token = dispatch
            .supplied_fencing_token()
            .ok_or(SettlementFailure::AttemptCompensationRejected)?;
        let compensation = OperationAttemptCompensation::new(
            dispatch.attempt_id().clone(),
            fencing_token,
            superseding_fencing_token,
            cleanup_digest,
            self.clock.now(),
        );
        match attempts
            .compensate(&compensation)
            .await
            .map_err(SettlementFailure::Port)?
        {
            OperationAttemptCompensationOutcome::Compensated(attempt)
            | OperationAttemptCompensationOutcome::Duplicate(attempt) => self
                .runtime
                .acknowledge_attempt(&attempt)
                .await
                .map_err(SettlementFailure::Port),
            OperationAttemptCompensationOutcome::StaleFence { .. }
            | OperationAttemptCompensationOutcome::NotFound
            | OperationAttemptCompensationOutcome::NotAccepted
            | OperationAttemptCompensationOutcome::PayloadConflict => {
                Err(SettlementFailure::AttemptCompensationRejected)
            }
        }
    }

    async fn replay_dispatch(
        &self,
        dispatch: &OperationAttemptAdmission,
    ) -> Result<Option<StartedSession>, StartSessionError> {
        let attempts = self
            .attempts
            .ok_or(StartSessionError::DispatchStoreRequired)?;
        let Some(attempt) = attempts
            .load(dispatch.attempt_id())
            .await
            .map_err(StartSessionError::DispatchPersistence)?
        else {
            return Ok(None);
        };
        if !dispatch.has_same_dispatched_semantics(&attempt) {
            return Err(StartSessionError::DispatchConflict);
        }
        match attempt.state() {
            OperationAttemptState::Accepted
            | OperationAttemptState::Settled {
                certainty: EffectCertainty::Unknown,
                ..
            } => Err(StartSessionError::DispatchReconciliationRequired),
            OperationAttemptState::Settled {
                certainty: EffectCertainty::KnownAbsent,
                ..
            } => Err(StartSessionError::DispatchConflict),
            OperationAttemptState::Settled {
                certainty: EffectCertainty::KnownApplied,
                ..
            } => {
                let session_id = attempt
                    .target_session()
                    .ok_or(StartSessionError::DispatchResultUnavailable)?;
                let session = self
                    .repository
                    .load(session_id)
                    .await
                    .map_err(StartSessionError::DispatchPersistence)?
                    .ok_or(StartSessionError::DispatchReconciliationRequired)?;
                let process = self
                    .processes
                    .lookup(session_id)
                    .map_err(StartSessionError::DispatchPersistence)?
                    .ok_or(StartSessionError::DispatchReconciliationRequired)?;
                let OperationAttemptState::Settled {
                    result_digest: Some(result_digest),
                    ..
                } = attempt.state()
                else {
                    return Err(StartSessionError::DispatchResultUnavailable);
                };
                let exact_owner = session.process().is_some_and(|owner| {
                    owner.id() == &process.id && owner.started_at() == process.started_at
                });
                if session.lifecycle() != LifecycleState::Running
                    || !exact_owner
                    || process_instance_result_digest(&process) != *result_digest
                {
                    return Err(StartSessionError::DispatchReconciliationRequired);
                }
                // A lost or failed post-commit ACK leaves the same receipt behind.
                // Retry it only after proving this is still the committed process;
                // an ACK failure must not compensate or respawn that live session.
                self.runtime
                    .acknowledge_attempt(&attempt)
                    .await
                    .map_err(StartSessionError::DispatchPersistence)?;
                Ok(Some(StartedSession { session, process }))
            }
            OperationAttemptState::Compensated { .. } => Err(StartSessionError::DispatchConflict),
        }
    }

    async fn recover_activation_failure(
        &self,
        failure: ProcessActivationFailure,
    ) -> StartSessionError {
        let ProcessActivationFailure {
            stage,
            source,
            session,
            process,
            binding,
        } = failure;
        if stage == ProcessActivationStage::Spawn {
            if let ProcessActivationFailureSource::Port(source) = source {
                let settlement = self.settle_failed_session(session).await.err();
                return StartSessionError::Spawn { source, settlement };
            }
            let recovery = StartRecoveryReport {
                settlement_failure: self.settle_failed_session(session).await.err(),
                ..StartRecoveryReport::default()
            };
            return StartSessionError::PartialStart {
                stage: StartFailureStage::RecordProcessOwnership,
                source: StartOperationFailure::InvariantViolation {
                    rule: "spawn failure must retain its process adapter source",
                },
                recovery,
            };
        }
        let stage = start_failure_stage(stage);
        let source = start_operation_failure(source);
        let Some(process) = process else {
            let recovery = StartRecoveryReport {
                settlement_failure: self.settle_failed_session(session).await.err(),
                ..StartRecoveryReport::default()
            };
            return StartSessionError::PartialStart {
                stage,
                source: StartOperationFailure::InvariantViolation {
                    rule: "post-spawn activation failure must retain process identity",
                },
                recovery,
            };
        };
        let recovery = self
            .recover_partial_start(&session, &process, binding)
            .await;
        StartSessionError::PartialStart {
            stage,
            source,
            recovery,
        }
    }

    async fn recover_partial_start(
        &self,
        session: &Session,
        process: &ProcessInstance,
        binding: ProcessBindingState,
    ) -> StartRecoveryReport {
        let core = MutationCore::new(
            self.clock,
            self.coordinator,
            self.repository,
            self.projection,
            self.telemetry,
            ApplicationOperation::StartSession,
        );
        let compensation = compensate_process(
            &core,
            self.runtime,
            self.processes,
            session,
            process,
            binding,
        )
        .await;
        let safe_to_settle = compensation.safe_to_settle();
        let mut report = StartRecoveryReport {
            force_stop_failure: compensation.force_stop_failure,
            unbind_failure: compensation.unbind_failure,
            unbind_conflict: compensation.unbind_conflict,
            settlement_failure: if safe_to_settle {
                self.reconcile_failed_session(session.id()).await.err()
            } else {
                None
            },
            cleanup_required_process: None,
        };
        if !report.completed() {
            report.cleanup_required_process = Some(process.clone());
        }
        report
    }

    async fn settle_failed_session(&self, mut session: Session) -> Result<(), SettlementFailure> {
        let changed_at = not_before(self.clock.now(), session.updated_at());
        let expected_revision = session.revision();
        let change = session
            .transition_lifecycle(LifecycleState::Failed, changed_at)
            .map_err(SettlementFailure::Domain)?;
        let Some(change) = change else {
            self.update_projection(&session, changed_at);
            return Ok(());
        };
        let event = outbox_event(&session, change, changed_at);
        let outcome = match self
            .repository
            .commit(&session, expected_revision, &[event])
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                self.emit_port_failure(changed_at, Some(session.id().clone()), &error);
                return Err(SettlementFailure::Port(error));
            }
        };
        match outcome {
            CommitSessionOutcome::Committed => {
                self.update_projection(&session, changed_at);
                Ok(())
            }
            CommitSessionOutcome::RevisionConflict { current_revision } => {
                Err(SettlementFailure::RevisionConflict { current_revision })
            }
            CommitSessionOutcome::NameConflict {
                existing_session_id,
            } => Err(SettlementFailure::NameConflict {
                existing_session_id,
            }),
            CommitSessionOutcome::NotFound => Err(SettlementFailure::SessionMissing),
        }
    }

    async fn reconcile_failed_session(
        &self,
        session_id: &SessionId,
    ) -> Result<(), SettlementFailure> {
        let mut last_conflict = Revision::INITIAL;
        for attempt in 0..MAX_RECONCILIATION_ATTEMPTS {
            let loaded = match self.repository.load(session_id).await {
                Ok(loaded) => loaded,
                Err(error) => {
                    self.emit_port_failure(self.clock.now(), Some(session_id.clone()), &error);
                    return Err(SettlementFailure::Port(error));
                }
            };
            let Some(mut durable) = loaded else {
                return Err(SettlementFailure::SessionMissing);
            };
            if durable.lifecycle().is_terminal() {
                self.update_projection(&durable, self.clock.now());
                return Ok(());
            }
            let expected_revision = durable.revision();
            let changed_at = not_before(self.clock.now(), durable.updated_at());
            let Some(change) = durable
                .transition_lifecycle(LifecycleState::Failed, changed_at)
                .map_err(SettlementFailure::Domain)?
            else {
                self.update_projection(&durable, changed_at);
                return Ok(());
            };
            let event = outbox_event(&durable, change, changed_at);
            let outcome = match self
                .repository
                .commit(&durable, expected_revision, &[event])
                .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    self.emit_port_failure(changed_at, Some(session_id.clone()), &error);
                    return Err(SettlementFailure::Port(error));
                }
            };
            match outcome {
                CommitSessionOutcome::Committed => {
                    self.update_projection(&durable, changed_at);
                    return Ok(());
                }
                CommitSessionOutcome::RevisionConflict { current_revision }
                    if attempt + 1 == MAX_RECONCILIATION_ATTEMPTS =>
                {
                    return Err(SettlementFailure::RevisionConflict { current_revision });
                }
                CommitSessionOutcome::RevisionConflict { current_revision } => {
                    last_conflict = current_revision;
                }
                CommitSessionOutcome::NameConflict {
                    existing_session_id,
                } => {
                    return Err(SettlementFailure::NameConflict {
                        existing_session_id,
                    });
                }
                CommitSessionOutcome::NotFound => return Err(SettlementFailure::SessionMissing),
            }
        }
        Err(SettlementFailure::RevisionConflict {
            current_revision: last_conflict,
        })
    }

    fn update_projection(&self, session: &Session, timestamp: TimestampMillis) {
        match self.projection.upsert_session(session) {
            Ok(()) => self.emit(
                DiagnosticEventName::ProjectionUpdated,
                DiagnosticLevel::Debug,
                timestamp,
                Some(session.id().clone()),
                Some(PortOperation::UpdateProjection),
            ),
            Err(error) => {
                self.emit_port_failure(timestamp, Some(session.id().clone()), &error);
            }
        }
    }

    fn emit_port_failure(
        &self,
        timestamp: TimestampMillis,
        session_id: Option<SessionId>,
        error: &PortError,
    ) {
        self.emit_operation_failure(timestamp, session_id, error.operation(), error.kind());
    }

    fn emit_operation_failure(
        &self,
        timestamp: TimestampMillis,
        session_id: Option<SessionId>,
        operation: PortOperation,
        error_kind: PortErrorKind,
    ) {
        self.telemetry.emit_diagnostic(&DiagnosticRecord {
            event: DiagnosticEventName::PortOperationFailed,
            level: DiagnosticLevel::for_port_error(error_kind),
            timestamp,
            session_id,
            application_operation: Some(ApplicationOperation::StartSession),
            application_error: Some(error_kind.application_category()),
            operation: Some(operation),
            error_kind: Some(error_kind),
        });
    }

    fn emit(
        &self,
        event: DiagnosticEventName,
        level: DiagnosticLevel,
        timestamp: TimestampMillis,
        session_id: Option<SessionId>,
        operation: Option<PortOperation>,
    ) {
        self.telemetry.emit_diagnostic(&DiagnosticRecord {
            event,
            level,
            timestamp,
            session_id,
            application_operation: Some(ApplicationOperation::StartSession),
            application_error: None,
            operation,
            error_kind: None,
        });
    }

    fn dispatch_policy_is_current(&self, dispatch: Option<&OperationAttemptAdmission>) -> bool {
        let Some(dispatch) = dispatch else {
            return true;
        };
        dispatch
            .dispatch_binding()
            .and_then(|binding| binding.runtime_policy())
            .is_some_and(|policy| policy.validate_at(self.clock.now()).is_ok())
    }
}

fn dispatch_cleanup_digest(
    dispatch: &OperationAttemptAdmission,
    session: &Session,
    process: &ProcessInstance,
    superseding_fencing_token: FencingToken,
) -> PayloadDigest {
    let original_fence = dispatch
        .supplied_fencing_token()
        .map(FencingToken::value)
        .unwrap_or_default()
        .to_string();
    let superseding_fence = superseding_fencing_token.value().to_string();
    let revision = session.revision().value().to_string();
    let started_at = format!("{:?}", process.started_at);
    PayloadDigest::of_slices([
        b"undercurrent:dispatch-compensation:v0.1.0-beta".as_slice(),
        dispatch.attempt_id().as_str().as_bytes(),
        session.id().as_str().as_bytes(),
        process.id.as_str().as_bytes(),
        started_at.as_bytes(),
        revision.as_bytes(),
        original_fence.as_bytes(),
        superseding_fence.as_bytes(),
    ])
}

fn start_failure_stage(stage: ProcessActivationStage) -> StartFailureStage {
    match stage {
        ProcessActivationStage::Spawn | ProcessActivationStage::RecordProcessOwnership => {
            StartFailureStage::RecordProcessOwnership
        }
        ProcessActivationStage::CommitProcessOwnership => StartFailureStage::CommitProcessOwnership,
        ProcessActivationStage::BindProcess => StartFailureStage::BindProcess,
        ProcessActivationStage::ArmProcessExit => StartFailureStage::ArmProcessExit,
        ProcessActivationStage::EnterRunning => StartFailureStage::EnterRunning,
        ProcessActivationStage::CommitRunning => StartFailureStage::CommitRunning,
    }
}

fn start_operation_failure(source: ProcessActivationFailureSource) -> StartOperationFailure {
    match source {
        ProcessActivationFailureSource::Port(source) => StartOperationFailure::Port(source),
        ProcessActivationFailureSource::Mutation(source) => match source {
            SessionMutationError::Domain(source) => StartOperationFailure::Domain(source),
            SessionMutationError::RevisionPreconditionFailed {
                current_revision, ..
            }
            | SessionMutationError::RevisionConflict {
                current_revision, ..
            } => StartOperationFailure::RevisionConflict { current_revision },
            SessionMutationError::NameConflict {
                existing_session_id,
                ..
            } => StartOperationFailure::NameConflict {
                existing_session_id,
            },
            SessionMutationError::Acquire(source)
            | SessionMutationError::Load(source)
            | SessionMutationError::Commit(source)
            | SessionMutationError::Delete(source) => StartOperationFailure::Port(source),
            SessionMutationError::InvariantViolation { rule } => {
                StartOperationFailure::InvariantViolation { rule }
            }
            SessionMutationError::NotFound { .. }
            | SessionMutationError::RemovedDuringCommit { .. } => {
                StartOperationFailure::InvariantViolation {
                    rule: "created session disappeared during process activation",
                }
            }
        },
    }
}

fn not_before(value: TimestampMillis, lower_bound: TimestampMillis) -> TimestampMillis {
    value.max(lower_bound)
}

fn outbox_event(
    session: &Session,
    change: DomainChange,
    occurred_at: TimestampMillis,
) -> SessionOutboxEvent {
    SessionOutboxEvent {
        kind: change.kind(),
        session_id: session.id().clone(),
        revision: change.revision(),
        occurred_at,
    }
}
