// SPDX-License-Identifier: Apache-2.0

//! Ordered terminal mutation orchestration and partial-failure compensation.

mod input_admission;

use std::error::Error;
use std::fmt::{Display, Formatter};

use undercurrent_domain::{
    Confidence, DeliveryId, EvidenceDisposition, EvidenceSource, ExecutionObservation,
    ExecutionState, ForegroundObservation, InputDeliveryPolicy, InputObservationPolicy,
    InputSequence, InteractionObservation, LifecycleState, ObservationBasis, ProcessMutationKind,
    ProfileBindingMode, ProfileName, Revision, Session, SessionId, TerminalSize, TimestampMillis,
};

use super::{MutationCore, SessionMutationError, not_before};
use crate::{
    ApplicationErrorCategory, ApplicationOperation, Clock, DetectionProfileCatalog,
    DiagnosticEventName, DiagnosticLevel, EffectiveProfileDirectory, InputDeliveryDisposition,
    InputDeliveryReservation, InputDeliveryStore, MetricName, MetricObservation, PortError,
    ProcessRuntime, SessionMutationCoordinator, SessionProcessDirectory, SessionProjectionWriter,
    SessionRepository, Telemetry, process_mutation_admission,
};

/// Input for revisioning and applying one terminal size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResizeTerminalCommand {
    /// Stable owning session identity.
    pub session_id: SessionId,
    /// Validated requested terminal dimensions.
    pub size: TerminalSize,
}

/// Result of an idempotent terminal resize.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResizeTerminalResult {
    /// Current durable session after the operation.
    pub session: Session,
    /// Whether live PTY and durable state changed.
    pub changed: bool,
}

/// Failure from live terminal resize or durable settlement.
#[derive(Debug)]
pub enum ResizeTerminalError {
    /// Ordered durable session access failed before PTY IO.
    Mutation(SessionMutationError),
    /// Session has no live PTY in its current lifecycle.
    LifecycleConflict {
        /// Stable target session identity.
        session_id: SessionId,
        /// Current authoritative lifecycle.
        lifecycle: LifecycleState,
    },
    /// Live process ownership could not be resolved.
    ProcessLookup(PortError),
    /// Running lifecycle had no live process binding.
    ProcessUnavailable {
        /// Stable target session identity.
        session_id: SessionId,
    },
    /// PTY adapter rejected the requested dimensions before durable mutation.
    Resize(PortError),
    /// Durable commit failed after PTY resize and compensation was attempted.
    DurableSettlement {
        /// Primary durable mutation failure.
        source: SessionMutationError,
        /// Failure restoring prior live dimensions, when compensation failed.
        rollback: Option<PortError>,
    },
}

impl ResizeTerminalError {
    /// Returns stable category used by protocol and presentation adapters.
    pub const fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::Mutation(error) => error.category(),
            Self::LifecycleConflict { .. } | Self::ProcessUnavailable { .. } => {
                ApplicationErrorCategory::FailedPrecondition
            }
            Self::ProcessLookup(error) | Self::Resize(error) => error.category(),
            Self::DurableSettlement { source, .. } => source.category(),
        }
    }
}

impl Display for ResizeTerminalError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mutation(error) => Display::fmt(error, formatter),
            Self::LifecycleConflict {
                session_id,
                lifecycle,
            } => write!(
                formatter,
                "cannot resize terminal for session {session_id} while lifecycle is {lifecycle}"
            ),
            Self::ProcessLookup(error) => {
                write!(formatter, "could not resolve terminal process: {error}")
            }
            Self::ProcessUnavailable { session_id } => {
                write!(
                    formatter,
                    "session {session_id} has no live terminal process"
                )
            }
            Self::Resize(error) => write!(formatter, "could not resize live terminal: {error}"),
            Self::DurableSettlement {
                source, rollback, ..
            } => {
                write!(formatter, "could not commit terminal resize: {source}")?;
                if rollback.is_some() {
                    formatter.write_str("; live terminal rollback also failed")?;
                }
                Ok(())
            }
        }
    }
}

impl Error for ResizeTerminalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Mutation(error) => Some(error),
            Self::ProcessLookup(error) | Self::Resize(error) => Some(error),
            Self::DurableSettlement { source, .. } => Some(source),
            Self::LifecycleConflict { .. } | Self::ProcessUnavailable { .. } => None,
        }
    }
}

/// Applies live and durable terminal size changes under session-local ordering.
pub struct ResizeTerminalService<'a> {
    core: MutationCore<'a>,
    runtime: &'a dyn ProcessRuntime,
    processes: &'a dyn SessionProcessDirectory,
    telemetry: &'a dyn Telemetry,
}

impl<'a> ResizeTerminalService<'a> {
    /// Constructs a terminal resize service without performing IO.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        clock: &'a dyn Clock,
        coordinator: &'a dyn SessionMutationCoordinator,
        repository: &'a dyn SessionRepository,
        runtime: &'a dyn ProcessRuntime,
        processes: &'a dyn SessionProcessDirectory,
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
                ApplicationOperation::ResizeTerminal,
            ),
            runtime,
            processes,
            telemetry,
        }
    }

    /// Resizes one running PTY and commits its reproducible durable dimensions.
    ///
    /// The live PTY changes before the optimistic durable commit. If that commit
    /// fails, the prior live size is restored while the caller retains its
    /// session-local mutation lease. Repeating the current size is an idempotent
    /// no-op without process IO. This method does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed lifecycle, process ownership, PTY, domain, persistence, or
    /// rollback failures. Adapter sources remain redacted from display/debug.
    pub async fn execute(
        &self,
        command: ResizeTerminalCommand,
    ) -> Result<ResizeTerminalResult, ResizeTerminalError> {
        let started_at = self.core.clock.now();
        let result = self.execute_inner(command).await;
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::OperationDurationSeconds,
            value: self
                .core
                .clock
                .now()
                .saturating_duration_since(started_at)
                .as_secs_f64(),
            operation: Some(ApplicationOperation::ResizeTerminal),
        });
        result
    }

    async fn execute_inner(
        &self,
        command: ResizeTerminalCommand,
    ) -> Result<ResizeTerminalResult, ResizeTerminalError> {
        self.core.emit(
            DiagnosticEventName::TerminalResizeRequested,
            DiagnosticLevel::Debug,
            self.core.clock.now(),
            Some(command.session_id.clone()),
            None,
            None,
        );
        let (_lease, mut session) = self
            .core
            .begin(&command.session_id)
            .await
            .map_err(ResizeTerminalError::Mutation)?;
        if session.lifecycle() != LifecycleState::Running {
            return Err(ResizeTerminalError::LifecycleConflict {
                session_id: session.id().clone(),
                lifecycle: session.lifecycle(),
            });
        }
        let prior_size = session.terminal_size();
        if prior_size == command.size {
            return Ok(ResizeTerminalResult {
                session,
                changed: false,
            });
        }
        let process = match self.processes.lookup(session.id()) {
            Ok(Some(process)) => process,
            Ok(None) => {
                return Err(ResizeTerminalError::ProcessUnavailable {
                    session_id: session.id().clone(),
                });
            }
            Err(error) => {
                self.core.emit_port_failure(
                    self.core.clock.now(),
                    Some(session.id().clone()),
                    &error,
                );
                return Err(ResizeTerminalError::ProcessLookup(error));
            }
        };
        if !session
            .process()
            .is_some_and(|durable| process.matches(durable))
        {
            return Err(ResizeTerminalError::ProcessUnavailable {
                session_id: session.id().clone(),
            });
        }
        let expected_revision = session.revision();
        let changed_at = not_before(self.core.clock.now(), session.updated_at());
        let change = session
            .resize_terminal(command.size, changed_at)
            .map_err(SessionMutationError::Domain)
            .map_err(ResizeTerminalError::Mutation)?
            .ok_or_else(|| {
                ResizeTerminalError::Mutation(SessionMutationError::InvariantViolation {
                    rule: "different terminal dimensions must produce a domain change",
                })
            })?;
        let resize_payload = format!(
            "{:?}:{}x{}",
            process.id,
            command.size.columns(),
            command.size.rows()
        );
        let resize_admission = process_mutation_admission(
            None,
            session.id().clone(),
            ProcessMutationKind::Resize,
            resize_payload.as_bytes(),
            self.core.clock.now(),
        );
        if let Err(error) = self
            .runtime
            .resize_admitted(&resize_admission, session.id(), &process.id, command.size)
            .await
        {
            self.core
                .emit_port_failure(self.core.clock.now(), Some(session.id().clone()), &error);
            return Err(ResizeTerminalError::Resize(error));
        }
        if let Err(source) = self
            .core
            .commit(&session, expected_revision, change, changed_at)
            .await
        {
            let rollback_payload = format!(
                "{:?}:{}x{}:rollback",
                process.id,
                prior_size.columns(),
                prior_size.rows()
            );
            let rollback_admission = process_mutation_admission(
                None,
                session.id().clone(),
                ProcessMutationKind::Resize,
                rollback_payload.as_bytes(),
                self.core.clock.now(),
            );
            let rollback = self
                .runtime
                .resize_admitted(&rollback_admission, session.id(), &process.id, prior_size)
                .await
                .err();
            if let Some(error) = &rollback {
                self.core.emit_port_failure(
                    self.core.clock.now(),
                    Some(session.id().clone()),
                    error,
                );
                self.core.emit(
                    DiagnosticEventName::TerminalResizeRollbackFailed,
                    DiagnosticLevel::Error,
                    self.core.clock.now(),
                    Some(session.id().clone()),
                    None,
                    None,
                );
            } else {
                self.core.emit(
                    DiagnosticEventName::TerminalResizeRolledBack,
                    DiagnosticLevel::Warn,
                    self.core.clock.now(),
                    Some(session.id().clone()),
                    None,
                    None,
                );
            }
            return Err(ResizeTerminalError::DurableSettlement { source, rollback });
        }
        self.core.emit(
            DiagnosticEventName::TerminalResizeApplied,
            DiagnosticLevel::Debug,
            changed_at,
            Some(session.id().clone()),
            None,
            None,
        );
        Ok(ResizeTerminalResult {
            session,
            changed: true,
        })
    }
}

/// Input for an idempotent standard-input closure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseStandardInputCommand {
    /// Stable owning session identity.
    pub session_id: SessionId,
}

/// Confirmed standard-input closure for one exact live process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseStandardInputResult {
    /// Stable target session identity.
    pub session_id: SessionId,
    /// Product-clock time at which closure was confirmed.
    pub closed_at: TimestampMillis,
    /// Whether the exact process input had already been closed.
    pub already_closed: bool,
}

/// Failure from resolving or closing one exact process input stream.
#[derive(Debug)]
pub enum CloseStandardInputError {
    /// Ordered durable session access failed before process IO.
    Mutation(SessionMutationError),
    /// Session has no live process in its current lifecycle.
    LifecycleConflict {
        /// Stable target session identity.
        session_id: SessionId,
        /// Current authoritative lifecycle.
        lifecycle: LifecycleState,
    },
    /// Live process ownership could not be resolved.
    ProcessLookup(PortError),
    /// Running lifecycle had no matching live process binding.
    ProcessUnavailable {
        /// Stable target session identity.
        session_id: SessionId,
    },
    /// The process adapter could not establish input closure.
    Close(PortError),
}

impl CloseStandardInputError {
    /// Returns the stable application error category.
    pub const fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::Mutation(error) => error.category(),
            Self::LifecycleConflict { .. } | Self::ProcessUnavailable { .. } => {
                ApplicationErrorCategory::FailedPrecondition
            }
            Self::ProcessLookup(error) | Self::Close(error) => error.category(),
        }
    }
}

impl Display for CloseStandardInputError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mutation(error) => Display::fmt(error, formatter),
            Self::LifecycleConflict {
                session_id,
                lifecycle,
            } => write!(
                formatter,
                "cannot close standard input for session {session_id} while lifecycle is {lifecycle}"
            ),
            Self::ProcessLookup(error) => {
                write!(
                    formatter,
                    "could not resolve standard-input process: {error}"
                )
            }
            Self::ProcessUnavailable { session_id } => {
                write!(
                    formatter,
                    "session {session_id} has no live standard-input process"
                )
            }
            Self::Close(error) => write!(formatter, "could not close standard input: {error}"),
        }
    }
}

impl Error for CloseStandardInputError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Mutation(error) => Some(error),
            Self::ProcessLookup(error) | Self::Close(error) => Some(error),
            Self::LifecycleConflict { .. } | Self::ProcessUnavailable { .. } => None,
        }
    }
}

/// Closes one exact process input stream under session-local mutation ordering.
pub struct CloseStandardInputService<'a> {
    core: MutationCore<'a>,
    runtime: &'a dyn ProcessRuntime,
    processes: &'a dyn SessionProcessDirectory,
    telemetry: &'a dyn Telemetry,
}

impl<'a> CloseStandardInputService<'a> {
    /// Constructs the service without performing IO.
    pub const fn new(
        clock: &'a dyn Clock,
        coordinator: &'a dyn SessionMutationCoordinator,
        repository: &'a dyn SessionRepository,
        runtime: &'a dyn ProcessRuntime,
        processes: &'a dyn SessionProcessDirectory,
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
                ApplicationOperation::CloseStandardInput,
            ),
            runtime,
            processes,
            telemetry,
        }
    }

    /// Closes stdin after all previously accepted input and confirms completion.
    ///
    /// Repetition is safe and reports `already_closed` without treating EOF as
    /// a byte sequence or terminal key. This method does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed lifecycle, ownership, or process-adapter failures.
    pub async fn execute(
        &self,
        command: CloseStandardInputCommand,
    ) -> Result<CloseStandardInputResult, CloseStandardInputError> {
        let started_at = self.core.clock.now();
        let result = self.execute_inner(command).await;
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::OperationDurationSeconds,
            value: self
                .core
                .clock
                .now()
                .saturating_duration_since(started_at)
                .as_secs_f64(),
            operation: Some(ApplicationOperation::CloseStandardInput),
        });
        result
    }

    async fn execute_inner(
        &self,
        command: CloseStandardInputCommand,
    ) -> Result<CloseStandardInputResult, CloseStandardInputError> {
        let (_lease, session) = self
            .core
            .begin(&command.session_id)
            .await
            .map_err(CloseStandardInputError::Mutation)?;
        if session.lifecycle() != LifecycleState::Running {
            return Err(CloseStandardInputError::LifecycleConflict {
                session_id: session.id().clone(),
                lifecycle: session.lifecycle(),
            });
        }
        let process = match self.processes.lookup(session.id()) {
            Ok(Some(process)) => process,
            Ok(None) => {
                return Err(CloseStandardInputError::ProcessUnavailable {
                    session_id: session.id().clone(),
                });
            }
            Err(error) => {
                self.core.emit_port_failure(
                    self.core.clock.now(),
                    Some(session.id().clone()),
                    &error,
                );
                return Err(CloseStandardInputError::ProcessLookup(error));
            }
        };
        if !session
            .process()
            .is_some_and(|durable| process.matches(durable))
        {
            return Err(CloseStandardInputError::ProcessUnavailable {
                session_id: session.id().clone(),
            });
        }
        let closure = match self.runtime.close_input(session.id(), &process.id).await {
            Ok(closure) => closure,
            Err(error) => {
                self.core.emit_port_failure(
                    self.core.clock.now(),
                    Some(session.id().clone()),
                    &error,
                );
                return Err(CloseStandardInputError::Close(error));
            }
        };
        Ok(CloseStandardInputResult {
            session_id: session.id().clone(),
            closed_at: self.core.clock.now(),
            already_closed: matches!(closure, crate::StandardInputClosure::AlreadyClosed),
        })
    }
}

/// Input for one bounded atomic terminal input delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendTerminalInputCommand {
    /// Stable owning session identity.
    pub session_id: SessionId,
    /// Optional caller precondition checked before delivery reservation or IO.
    pub expected_revision: Option<undercurrent_domain::Revision>,
    /// Complete validated atomic input sequence.
    pub input: InputSequence,
    /// Optional semantic ID used for durable duplicate suppression.
    pub delivery_id: Option<DeliveryId>,
    /// Best-effort foreground-observation behavior after confirmed delivery.
    pub observation_policy: InputObservationPolicy,
}

/// Result of a confirmed or deduplicated terminal input delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendTerminalInputResult {
    /// Stable target session identity.
    pub session_id: SessionId,
    /// Time retained for the confirmed delivery.
    pub delivered_at: TimestampMillis,
    /// Whether no PTY IO occurred because a completed result was retained.
    pub deduplicated: bool,
    /// Foreground revision committed by `force_active`, when one was produced.
    pub foreground_revision: Option<Revision>,
}

/// Failure from terminal input admission, IO, or result settlement.
#[derive(Debug)]
pub enum SendTerminalInputError {
    /// Ordered durable session access failed before PTY IO.
    Mutation(SessionMutationError),
    /// Terminal-relative input is invalid for the leased current dimensions.
    InvalidInput(undercurrent_domain::DomainError),
    /// Session has no live PTY in its current lifecycle.
    LifecycleConflict {
        /// Stable target session identity.
        session_id: SessionId,
        /// Current authoritative lifecycle.
        lifecycle: LifecycleState,
    },
    /// Live process ownership could not be resolved.
    ProcessLookup(PortError),
    /// The selected profile could not be read before input delivery.
    ProfileLookup(PortError),
    /// Exact-process effective profile state could not be read.
    EffectiveProfileLookup(PortError),
    /// The session references a profile absent from the active catalog.
    ProfileUnavailable {
        /// Stable configured profile name.
        profile: ProfileName,
    },
    /// Running lifecycle had no live process binding.
    ProcessUnavailable {
        /// Stable target session identity.
        session_id: SessionId,
    },
    /// Fresh internal identity allocation failed before reserving delivery.
    Admission(PortError),
    /// Semantic delivery reservation infrastructure failed.
    Reservation(PortError),
    /// Delivery ID was previously reserved for different semantic input.
    DeliveryPayloadConflict {
        /// Stable target session identity.
        session_id: SessionId,
        /// Conflicting caller-supplied delivery ID.
        delivery_id: DeliveryId,
    },
    /// A prior attempt has an ambiguous outcome and cannot be duplicated safely.
    DeliveryIndeterminate {
        /// Stable target session identity.
        session_id: SessionId,
        /// Ambiguous caller-supplied delivery ID.
        delivery_id: DeliveryId,
    },
    /// PTY write failed and result settlement was attempted.
    Write {
        /// Primary PTY adapter failure.
        source: PortError,
        /// Additional failure persisting the indeterminate result.
        settlement: Option<PortError>,
    },
    /// PTY accepted the sequence but its completed idempotency record failed.
    DeliveredSettlement {
        /// Result-store failure after confirmed delivery.
        source: PortError,
        /// Time at which the PTY write completed.
        delivered_at: TimestampMillis,
    },
    /// PTY delivery completed but the optional foreground revision failed.
    DeliveredForegroundSettlement {
        /// Durable foreground mutation failure.
        source: SessionMutationError,
        /// Time at which the PTY write completed.
        delivered_at: TimestampMillis,
    },
}

impl SendTerminalInputError {
    /// Returns stable category used by protocol and presentation adapters.
    pub const fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::Mutation(error) => error.category(),
            Self::InvalidInput(error) => ApplicationErrorCategory::from_domain(error.category()),
            Self::LifecycleConflict { .. }
            | Self::ProcessUnavailable { .. }
            | Self::ProfileUnavailable { .. } => ApplicationErrorCategory::FailedPrecondition,
            Self::DeliveryPayloadConflict { .. } | Self::DeliveryIndeterminate { .. } => {
                ApplicationErrorCategory::Conflict
            }
            Self::ProcessLookup(error)
            | Self::ProfileLookup(error)
            | Self::EffectiveProfileLookup(error)
            | Self::Admission(error)
            | Self::Reservation(error) => error.category(),
            Self::Write { source, .. } | Self::DeliveredSettlement { source, .. } => {
                source.category()
            }
            Self::DeliveredForegroundSettlement { source, .. } => source.category(),
        }
    }

    /// Returns whether the PTY adapter confirmed that the input was delivered.
    pub const fn delivery_was_confirmed(&self) -> bool {
        matches!(
            self,
            Self::DeliveredSettlement { .. } | Self::DeliveredForegroundSettlement { .. }
        )
    }
}

impl Display for SendTerminalInputError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mutation(error) => Display::fmt(error, formatter),
            Self::InvalidInput(error) => Display::fmt(error, formatter),
            Self::LifecycleConflict {
                session_id,
                lifecycle,
            } => write!(
                formatter,
                "cannot send terminal input to session {session_id} while lifecycle is {lifecycle}"
            ),
            Self::ProcessLookup(error) => {
                write!(formatter, "could not resolve terminal process: {error}")
            }
            Self::ProfileLookup(error) => {
                write!(
                    formatter,
                    "could not resolve terminal input profile: {error}"
                )
            }
            Self::EffectiveProfileLookup(error) => {
                write!(
                    formatter,
                    "could not resolve effective terminal profile: {error}"
                )
            }
            Self::ProfileUnavailable { profile } => {
                write!(formatter, "terminal input profile {profile} is unavailable")
            }
            Self::ProcessUnavailable { session_id } => {
                write!(
                    formatter,
                    "session {session_id} has no live terminal process"
                )
            }
            Self::Admission(error) => {
                write!(formatter, "could not allocate input attempt: {error}")
            }
            Self::Reservation(error) => {
                write!(
                    formatter,
                    "could not reserve terminal input delivery: {error}"
                )
            }
            Self::DeliveryPayloadConflict {
                session_id,
                delivery_id,
            } => write!(
                formatter,
                "delivery ID {delivery_id} was reused with different input for session {session_id}"
            ),
            Self::DeliveryIndeterminate {
                session_id,
                delivery_id,
            } => write!(
                formatter,
                "delivery ID {delivery_id} has an indeterminate result for session {session_id}"
            ),
            Self::Write { source, settlement } => {
                write!(formatter, "could not write terminal input: {source}")?;
                if settlement.is_some() {
                    formatter.write_str("; indeterminate result persistence also failed")?;
                }
                Ok(())
            }
            Self::DeliveredSettlement { source, .. } => write!(
                formatter,
                "terminal input was delivered but its result could not be persisted: {source}"
            ),
            Self::DeliveredForegroundSettlement { source, .. } => write!(
                formatter,
                "terminal input was delivered but foreground state could not be committed: {source}"
            ),
        }
    }
}

impl Error for SendTerminalInputError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Mutation(error) => Some(error),
            Self::InvalidInput(error) => Some(error),
            Self::ProcessLookup(error)
            | Self::ProfileLookup(error)
            | Self::EffectiveProfileLookup(error)
            | Self::Admission(error)
            | Self::Reservation(error) => Some(error),
            Self::Write { source, .. } | Self::DeliveredSettlement { source, .. } => Some(source),
            Self::DeliveredForegroundSettlement { source, .. } => Some(source),
            Self::LifecycleConflict { .. }
            | Self::ProcessUnavailable { .. }
            | Self::ProfileUnavailable { .. }
            | Self::DeliveryPayloadConflict { .. }
            | Self::DeliveryIndeterminate { .. } => None,
        }
    }
}

/// Delivers atomic terminal input with optional durable duplicate suppression.
pub struct SendTerminalInputService<'a> {
    attempt_ids: &'a dyn crate::OperationAttemptIdSource,
    core: MutationCore<'a>,
    runtime: &'a dyn ProcessRuntime,
    processes: &'a dyn SessionProcessDirectory,
    profiles: &'a dyn DetectionProfileCatalog,
    effective_profiles: &'a dyn EffectiveProfileDirectory,
    deliveries: &'a dyn InputDeliveryStore,
    telemetry: &'a dyn Telemetry,
}

impl<'a> SendTerminalInputService<'a> {
    /// Constructs an input delivery service without performing IO.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        clock: &'a dyn Clock,
        attempt_ids: &'a dyn crate::OperationAttemptIdSource,
        coordinator: &'a dyn SessionMutationCoordinator,
        repository: &'a dyn SessionRepository,
        runtime: &'a dyn ProcessRuntime,
        processes: &'a dyn SessionProcessDirectory,
        profiles: &'a dyn DetectionProfileCatalog,
        effective_profiles: &'a dyn EffectiveProfileDirectory,
        deliveries: &'a dyn InputDeliveryStore,
        projection: &'a dyn SessionProjectionWriter,
        telemetry: &'a dyn Telemetry,
    ) -> Self {
        Self {
            attempt_ids,
            core: MutationCore::new(
                clock,
                coordinator,
                repository,
                projection,
                telemetry,
                ApplicationOperation::SendTerminalInput,
            ),
            runtime,
            processes,
            profiles,
            effective_profiles,
            deliveries,
            telemetry,
        }
    }

    /// Writes one complete sequence atomically relative to session mutations.
    ///
    /// When a delivery ID is supplied, a bounded reservation is created
    /// before PTY IO. Completed results are replayed without IO; ambiguous
    /// results suppress unsafe retry; and payload reuse is rejected. Confirmed
    /// delivery is reported distinctly from later settlement failures. This
    /// method does not panic.
    ///
    /// Retention is owned by the configured [`InputDeliveryStore`]. The daemon's
    /// SQLite adapter preserves reservations through restart until session deletion;
    /// this service does not make a write and its settlement one atomic effect.
    ///
    /// # Errors
    ///
    /// Returns typed lifecycle, ownership, reservation, write, result-settlement,
    /// or foreground-settlement failures. Private input is absent from diagnostics.
    ///
    pub async fn execute(
        &self,
        command: SendTerminalInputCommand,
    ) -> Result<SendTerminalInputResult, SendTerminalInputError> {
        let started_at = self.core.clock.now();
        let result = self.execute_inner(command).await;
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::OperationDurationSeconds,
            value: self
                .core
                .clock
                .now()
                .saturating_duration_since(started_at)
                .as_secs_f64(),
            operation: Some(ApplicationOperation::SendTerminalInput),
        });
        result
    }

    async fn execute_inner(
        &self,
        command: SendTerminalInputCommand,
    ) -> Result<SendTerminalInputResult, SendTerminalInputError> {
        let (_lease, mut session) = self
            .core
            .begin_at_revision(&command.session_id, command.expected_revision)
            .await
            .map_err(SendTerminalInputError::Mutation)?;
        if session.lifecycle() != LifecycleState::Running {
            return Err(SendTerminalInputError::LifecycleConflict {
                session_id: session.id().clone(),
                lifecycle: session.lifecycle(),
            });
        }
        command
            .input
            .validate_for_terminal(session.terminal_size())
            .map_err(SendTerminalInputError::InvalidInput)?;
        let process = match self.processes.lookup(session.id()) {
            Ok(Some(process)) => process,
            Ok(None) => {
                return Err(SendTerminalInputError::ProcessUnavailable {
                    session_id: session.id().clone(),
                });
            }
            Err(error) => {
                self.core.emit_port_failure(
                    self.core.clock.now(),
                    Some(session.id().clone()),
                    &error,
                );
                return Err(SendTerminalInputError::ProcessLookup(error));
            }
        };
        if !session
            .process()
            .is_some_and(|durable| process.matches(durable))
        {
            return Err(SendTerminalInputError::ProcessUnavailable {
                session_id: session.id().clone(),
            });
        }
        // Resolve catalog preconditions before creating an idempotency record;
        // only an attempted PTY delivery is allowed to leave an ambiguous reservation.
        let selected_profile = if session.configuration().profile_binding()
            == ProfileBindingMode::ForegroundAdaptive
        {
            self.effective_profiles
                .get(session.id(), &process.id)
                .map_err(|error| {
                    self.core.emit_port_failure(
                        self.core.clock.now(),
                        Some(session.id().clone()),
                        &error,
                    );
                    SendTerminalInputError::EffectiveProfileLookup(error)
                })?
                .and_then(|effective| effective.selection().profile().cloned())
        } else {
            session.profile().cloned()
        };
        let input_policy = match selected_profile.as_ref() {
            None => InputDeliveryPolicy::immediate(),
            Some(profile) => match self.profiles.get(profile) {
                Ok(Some(entry)) => entry.profile().input_delivery(),
                Ok(None) => {
                    return Err(SendTerminalInputError::ProfileUnavailable {
                        profile: profile.clone(),
                    });
                }
                Err(error) => {
                    self.core.emit_port_failure(
                        self.core.clock.now(),
                        Some(session.id().clone()),
                        &error,
                    );
                    return Err(SendTerminalInputError::ProfileLookup(error));
                }
            },
        };
        let attempt_id = self
            .attempt_ids
            .next_attempt_id()
            .map_err(SendTerminalInputError::Admission)?;
        if let Some(delivery_id) = &command.delivery_id {
            let reservation = self
                .deliveries
                .reserve(
                    session.id(),
                    delivery_id,
                    &command.input,
                    command.observation_policy,
                )
                .await
                .map_err(|error| {
                    self.core.emit_port_failure(
                        self.core.clock.now(),
                        Some(session.id().clone()),
                        &error,
                    );
                    SendTerminalInputError::Reservation(error)
                })?;
            match reservation {
                InputDeliveryReservation::Reserved => self.core.emit(
                    DiagnosticEventName::InputDeliveryReserved,
                    DiagnosticLevel::Debug,
                    self.core.clock.now(),
                    Some(session.id().clone()),
                    None,
                    None,
                ),
                InputDeliveryReservation::AlreadyDelivered { delivered_at } => {
                    self.core.emit(
                        DiagnosticEventName::InputDeliveryDeduplicated,
                        DiagnosticLevel::Debug,
                        self.core.clock.now(),
                        Some(session.id().clone()),
                        None,
                        None,
                    );
                    return Ok(SendTerminalInputResult {
                        session_id: session.id().clone(),
                        delivered_at,
                        deduplicated: true,
                        foreground_revision: None,
                    });
                }
                InputDeliveryReservation::Indeterminate => {
                    self.core.emit(
                        DiagnosticEventName::InputDeliveryIndeterminate,
                        DiagnosticLevel::Warn,
                        self.core.clock.now(),
                        Some(session.id().clone()),
                        None,
                        None,
                    );
                    return Err(SendTerminalInputError::DeliveryIndeterminate {
                        session_id: session.id().clone(),
                        delivery_id: delivery_id.clone(),
                    });
                }
                InputDeliveryReservation::PayloadConflict => {
                    return Err(SendTerminalInputError::DeliveryPayloadConflict {
                        session_id: session.id().clone(),
                        delivery_id: delivery_id.clone(),
                    });
                }
            }
        }

        if input_policy.trailing_submit_delay().is_some()
            && command.input.items().len() > 1
            && command.input.items().last().is_some_and(|item| {
                matches!(
                    item,
                    undercurrent_domain::InputItem::Key(undercurrent_domain::Key::Enter)
                )
            })
        {
            self.core.emit(
                DiagnosticEventName::InputDeliveryPacingApplied,
                DiagnosticLevel::Debug,
                self.core.clock.now(),
                Some(session.id().clone()),
                None,
                None,
            );
        }
        let input_admission = crate::OperationAttemptAdmission::new(
            attempt_id,
            Some(session.id().clone()),
            ProcessMutationKind::WriteInput,
            input_admission::digest(
                session.id(),
                &process.id,
                command.delivery_id.as_ref(),
                &command.input,
                input_policy,
            ),
            self.core.clock.now(),
        );
        if let Err(source) = self
            .runtime
            .write_input_admitted(
                &input_admission,
                session.id(),
                &process.id,
                &command.input,
                input_policy,
            )
            .await
        {
            self.core
                .emit_port_failure(self.core.clock.now(), Some(session.id().clone()), &source);
            let settlement = if let Some(delivery_id) = &command.delivery_id {
                let observed_at = self.core.clock.now();
                let result = self
                    .deliveries
                    .settle(
                        session.id(),
                        delivery_id,
                        InputDeliveryDisposition::Indeterminate { observed_at },
                    )
                    .await;
                if result.is_ok() {
                    self.core.emit(
                        DiagnosticEventName::InputDeliveryIndeterminate,
                        DiagnosticLevel::Warn,
                        observed_at,
                        Some(session.id().clone()),
                        None,
                        None,
                    );
                }
                result.err()
            } else {
                None
            };
            if let Some(error) = &settlement {
                self.core.emit_port_failure(
                    self.core.clock.now(),
                    Some(session.id().clone()),
                    error,
                );
            }
            return Err(SendTerminalInputError::Write { source, settlement });
        }
        let delivered_at = self.core.clock.now();
        self.core.emit(
            DiagnosticEventName::TerminalInputWritten,
            DiagnosticLevel::Debug,
            delivered_at,
            Some(session.id().clone()),
            None,
            None,
        );
        // Record delivery before the separate foreground commit. If that commit
        // fails, an exact delivery-ID retry must not write the input again;
        // AlreadyDelivered returns above without repairing foreground state.
        // See ../../docs/internals/sessions/terminal-io.md#errors-after-confirmed-delivery.
        if let Some(delivery_id) = &command.delivery_id {
            if let Err(source) = self
                .deliveries
                .settle(
                    session.id(),
                    delivery_id,
                    InputDeliveryDisposition::Delivered { delivered_at },
                )
                .await
            {
                self.core.emit_port_failure(
                    self.core.clock.now(),
                    Some(session.id().clone()),
                    &source,
                );
                return Err(SendTerminalInputError::DeliveredSettlement {
                    source,
                    delivered_at,
                });
            }
            self.core.emit(
                DiagnosticEventName::InputDeliverySettled,
                DiagnosticLevel::Debug,
                delivered_at,
                Some(session.id().clone()),
                None,
                None,
            );
        }

        let foreground_revision =
            if command.observation_policy == InputObservationPolicy::ForceActive {
                let changed_at = not_before(delivered_at, session.updated_at());
                let basis = ObservationBasis::new(
                    Confidence::CERTAIN,
                    Some(EvidenceSource::new("input_delivery").map_err(|error| {
                        SendTerminalInputError::DeliveredForegroundSettlement {
                            source: SessionMutationError::Domain(error),
                            delivered_at,
                        }
                    })?),
                    EvidenceDisposition::Affirmative,
                    changed_at,
                    Some(delivered_at),
                )
                .map_err(|error| {
                    SendTerminalInputError::DeliveredForegroundSettlement {
                        source: SessionMutationError::Domain(error),
                        delivered_at,
                    }
                })?;
                let foreground = ForegroundObservation::new(
                    ExecutionObservation::new(ExecutionState::Active, basis).map_err(|error| {
                        SendTerminalInputError::DeliveredForegroundSettlement {
                            source: SessionMutationError::Domain(error),
                            delivered_at,
                        }
                    })?,
                    InteractionObservation::none(changed_at),
                    session.foreground().constraint().clone(),
                );
                let expected_revision = session.revision();
                let change = session.update_foreground(foreground).map_err(|error| {
                    SendTerminalInputError::DeliveredForegroundSettlement {
                        source: SessionMutationError::Domain(error),
                        delivered_at,
                    }
                })?;
                if let Some(change) = change {
                    let revision = change.revision();
                    self.core
                        .commit(&session, expected_revision, change, changed_at)
                        .await
                        .map_err(|source| {
                            SendTerminalInputError::DeliveredForegroundSettlement {
                                source,
                                delivered_at,
                            }
                        })?;
                    self.core.emit(
                        DiagnosticEventName::InputForegroundCommitted,
                        DiagnosticLevel::Debug,
                        changed_at,
                        Some(session.id().clone()),
                        None,
                        None,
                    );
                    Some(revision)
                } else {
                    None
                }
            } else {
                None
            };
        Ok(SendTerminalInputResult {
            session_id: session.id().clone(),
            delivered_at,
            deduplicated: false,
            foreground_revision,
        })
    }
}

/// Fingerprints typed input and observation policy for durable delivery comparison.
///
/// Explicit tags and lengths distinguish text, raw bytes, key events and item
/// boundaries. Stores must also key by session and delivery ID. The digest avoids
/// retaining raw input, but is not encryption and must remain private.
pub fn semantic_input_delivery_digest(
    input: &InputSequence,
    policy: InputObservationPolicy,
) -> undercurrent_domain::PayloadDigest {
    input_admission::semantic_digest(input, policy)
}
