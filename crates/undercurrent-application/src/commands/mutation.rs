// SPDX-License-Identifier: Apache-2.0

//! Shared transaction boundary for ordered session mutations.

use std::error::Error;
use std::fmt::{Display, Formatter};

use undercurrent_domain::{
    DomainChange, DomainError, Revision, Session, SessionId, SessionName, TimestampMillis,
};

use crate::{
    ApplicationErrorCategory, ApplicationOperation, Clock, CommitSessionOutcome,
    DeleteSessionOutcome, DiagnosticEventName, DiagnosticLevel, DiagnosticRecord, PortError,
    PortErrorKind, PortOperation, SessionMutationCoordinator, SessionMutationLease,
    SessionOutboxEvent, SessionProjectionWriter, SessionRepository, Telemetry,
};

/// Typed failure shared by session-local mutation use cases.
#[derive(Debug)]
pub enum SessionMutationError {
    /// Ordered session mutation ownership could not be acquired.
    Acquire(PortError),
    /// Durable session could not be loaded.
    Load(PortError),
    /// Requested durable session does not exist.
    NotFound {
        /// Requested stable session identity.
        session_id: SessionId,
    },
    /// Pure aggregate mutation was rejected.
    Domain(DomainError),
    /// The caller's precondition failed under the session mutation lease,
    /// before any mutation or external effect was attempted.
    RevisionPreconditionFailed {
        /// Revision explicitly required by the caller.
        expected_revision: Revision,
        /// Current durable revision loaded while the lease is held.
        current_revision: Revision,
    },
    /// Atomic session and outbox commit failed.
    Commit(PortError),
    /// Atomic session deletion and removal-event commit failed.
    Delete(PortError),
    /// Durable state changed outside the held mutation lease.
    RevisionConflict {
        /// Expected prior revision.
        expected_revision: Revision,
        /// Current durable revision.
        current_revision: Revision,
    },
    /// Revised human-facing name already belongs to another session.
    NameConflict {
        /// Requested new session name.
        name: SessionName,
        /// Existing stable identity that owns the name.
        existing_session_id: SessionId,
    },
    /// Session disappeared between load and commit.
    RemovedDuringCommit {
        /// Stable session identity.
        session_id: SessionId,
    },
    /// Application orchestration reached an internally inconsistent branch.
    InvariantViolation {
        /// Stable, payload-safe violated rule.
        rule: &'static str,
    },
}

impl SessionMutationError {
    /// Returns stable category used by protocol and presentation adapters.
    pub const fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::NotFound { .. } | Self::RemovedDuringCommit { .. } => {
                ApplicationErrorCategory::NotFound
            }
            Self::Domain(error) => ApplicationErrorCategory::from_domain(error.category()),
            Self::RevisionPreconditionFailed { .. }
            | Self::RevisionConflict { .. }
            | Self::NameConflict { .. } => ApplicationErrorCategory::Conflict,
            Self::InvariantViolation { .. } => ApplicationErrorCategory::InvariantViolation,
            Self::Acquire(error)
            | Self::Load(error)
            | Self::Commit(error)
            | Self::Delete(error) => error.category(),
        }
    }
}

impl Display for SessionMutationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Acquire(error) => write!(formatter, "could not order session mutation: {error}"),
            Self::Load(error) => write!(formatter, "could not load session: {error}"),
            Self::NotFound { session_id } => {
                write!(formatter, "session {session_id} was not found")
            }
            Self::Domain(error) => Display::fmt(error, formatter),
            Self::RevisionPreconditionFailed {
                expected_revision,
                current_revision,
            } => write!(
                formatter,
                "session revision precondition {} differs from current {} before mutation",
                expected_revision.value(),
                current_revision.value()
            ),
            Self::Commit(error) => write!(formatter, "could not commit session mutation: {error}"),
            Self::Delete(error) => write!(formatter, "could not delete durable session: {error}"),
            Self::RevisionConflict {
                expected_revision,
                current_revision,
            } => write!(
                formatter,
                "session revision changed from expected {} to {}",
                expected_revision.value(),
                current_revision.value()
            ),
            Self::NameConflict {
                name,
                existing_session_id,
            } => write!(
                formatter,
                "session name {name} already belongs to {existing_session_id}"
            ),
            Self::RemovedDuringCommit { session_id } => {
                write!(
                    formatter,
                    "session {session_id} was removed during mutation"
                )
            }
            Self::InvariantViolation { rule } => {
                write!(formatter, "session mutation invariant failed: {rule}")
            }
        }
    }
}

impl Error for SessionMutationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Acquire(error)
            | Self::Load(error)
            | Self::Commit(error)
            | Self::Delete(error) => Some(error),
            Self::Domain(error) => Some(error),
            Self::NotFound { .. }
            | Self::RevisionPreconditionFailed { .. }
            | Self::RevisionConflict { .. }
            | Self::NameConflict { .. }
            | Self::RemovedDuringCommit { .. }
            | Self::InvariantViolation { .. } => None,
        }
    }
}

/// Shared ordered-load, optimistic-commit, projection, and diagnostic boundary.
pub(crate) struct MutationCore<'a> {
    /// Explicit application clock shared with concrete command orchestration.
    pub(crate) clock: &'a dyn Clock,
    coordinator: &'a dyn SessionMutationCoordinator,
    repository: &'a dyn SessionRepository,
    projection: &'a dyn SessionProjectionWriter,
    telemetry: &'a dyn Telemetry,
    application_operation: ApplicationOperation,
}

impl<'a> MutationCore<'a> {
    /// Constructs a mutation boundary without performing IO.
    pub(crate) const fn new(
        clock: &'a dyn Clock,
        coordinator: &'a dyn SessionMutationCoordinator,
        repository: &'a dyn SessionRepository,
        projection: &'a dyn SessionProjectionWriter,
        telemetry: &'a dyn Telemetry,
        application_operation: ApplicationOperation,
    ) -> Self {
        Self {
            clock,
            coordinator,
            repository,
            projection,
            telemetry,
            application_operation,
        }
    }

    /// Acquires a session-local lease and loads its current durable aggregate.
    ///
    /// The lease is returned with the aggregate so callers retain ordering
    /// ownership through every subsequent await. Adapter failures are emitted
    /// with safe typed context. This method does not panic.
    pub(crate) async fn begin(
        &self,
        session_id: &SessionId,
    ) -> Result<(Box<dyn SessionMutationLease>, Session), SessionMutationError> {
        let lease = match self.coordinator.acquire(session_id).await {
            Ok(lease) => lease,
            Err(error) => {
                self.emit_port_failure(self.clock.now(), Some(session_id.clone()), &error);
                return Err(SessionMutationError::Acquire(error));
            }
        };
        let session = match self.repository.load(session_id).await {
            Ok(Some(session)) => session,
            Ok(None) => {
                return Err(SessionMutationError::NotFound {
                    session_id: session_id.clone(),
                });
            }
            Err(error) => {
                self.emit_port_failure(self.clock.now(), Some(session_id.clone()), &error);
                return Err(SessionMutationError::Load(error));
            }
        };
        Ok((lease, session))
    }

    /// Loads and checks a caller precondition under the same lease retained by
    /// the mutation. A mismatch releases the lease without changing durable
    /// state or invoking a process/delivery port.
    pub(crate) async fn begin_at_revision(
        &self,
        session_id: &SessionId,
        expected_revision: Option<Revision>,
    ) -> Result<(Box<dyn SessionMutationLease>, Session), SessionMutationError> {
        let (lease, session) = self.begin(session_id).await?;
        if let Some(expected_revision) = expected_revision
            && session.revision() != expected_revision
        {
            return Err(SessionMutationError::RevisionPreconditionFailed {
                expected_revision,
                current_revision: session.revision(),
            });
        }
        Ok((lease, session))
    }

    /// Reloads authoritative durable state while the caller retains its lease.
    ///
    /// Used only for bounded reconciliation after ambiguous or conflicting
    /// commits. This method emits adapter failures and does not panic.
    pub(crate) async fn reload(
        &self,
        session_id: &SessionId,
    ) -> Result<Session, SessionMutationError> {
        match self.repository.load(session_id).await {
            Ok(Some(session)) => Ok(session),
            Ok(None) => Err(SessionMutationError::NotFound {
                session_id: session_id.clone(),
            }),
            Err(error) => {
                self.emit_port_failure(self.clock.now(), Some(session_id.clone()), &error);
                Err(SessionMutationError::Load(error))
            }
        }
    }

    /// Atomically writes one revised aggregate and matching outbox event.
    ///
    /// A successful durable commit updates the rebuildable summary projection;
    /// projection failure is diagnosed but does not falsify durable success.
    /// This method does not panic.
    pub(crate) async fn commit(
        &self,
        session: &Session,
        expected_revision: Revision,
        change: DomainChange,
        occurred_at: TimestampMillis,
    ) -> Result<(), SessionMutationError> {
        self.commit_changes(session, expected_revision, &[change], occurred_at)
            .await
    }

    /// Atomically writes one aggregate revision sequence and matching outbox
    /// events produced by a single ordered use case.
    ///
    /// Changes must already be ordered by revision. Repository adapters enforce
    /// the optimistic prior revision. This method performs persistence and
    /// best-effort projection IO and does not panic.
    pub(crate) async fn commit_changes(
        &self,
        session: &Session,
        expected_revision: Revision,
        changes: &[DomainChange],
        occurred_at: TimestampMillis,
    ) -> Result<(), SessionMutationError> {
        if changes.is_empty() {
            return Err(SessionMutationError::InvariantViolation {
                rule: "a session commit requires at least one domain change",
            });
        }
        let events = changes
            .iter()
            .map(|change| SessionOutboxEvent {
                kind: change.kind(),
                session_id: session.id().clone(),
                revision: change.revision(),
                occurred_at,
            })
            .collect::<Vec<_>>();
        match self
            .repository
            .commit(session, expected_revision, &events)
            .await
        {
            Ok(CommitSessionOutcome::Committed) => {
                self.project(session, occurred_at);
                Ok(())
            }
            Ok(CommitSessionOutcome::RevisionConflict { current_revision }) => {
                self.emit_operation_failure(
                    occurred_at,
                    session.id().clone(),
                    PortOperation::CommitSession,
                    PortErrorKind::Conflict,
                );
                Err(SessionMutationError::RevisionConflict {
                    expected_revision,
                    current_revision,
                })
            }
            Ok(CommitSessionOutcome::NameConflict {
                existing_session_id,
            }) => {
                self.emit_operation_failure(
                    occurred_at,
                    session.id().clone(),
                    PortOperation::CommitSession,
                    PortErrorKind::Conflict,
                );
                Err(SessionMutationError::NameConflict {
                    name: session.name().clone(),
                    existing_session_id,
                })
            }
            Ok(CommitSessionOutcome::NotFound) => {
                self.emit_operation_failure(
                    occurred_at,
                    session.id().clone(),
                    PortOperation::CommitSession,
                    PortErrorKind::NotFound,
                );
                Err(SessionMutationError::RemovedDuringCommit {
                    session_id: session.id().clone(),
                })
            }
            Err(error) => {
                self.emit_port_failure(occurred_at, Some(session.id().clone()), &error);
                Err(SessionMutationError::Commit(error))
            }
        }
    }

    /// Atomically deletes one terminal aggregate and writes its removal event.
    ///
    /// A successful durable deletion removes rebuildable projections on a
    /// best-effort basis. Projection failure is diagnosed but does not falsify
    /// durable success. This method does not panic.
    pub(crate) async fn delete(
        &self,
        session: &Session,
        expected_revision: Revision,
        change: DomainChange,
        occurred_at: TimestampMillis,
    ) -> Result<(), SessionMutationError> {
        let event = SessionOutboxEvent {
            kind: change.kind(),
            session_id: session.id().clone(),
            revision: change.revision(),
            occurred_at,
        };
        match self
            .repository
            .delete(session.id(), expected_revision, &event)
            .await
        {
            Ok(DeleteSessionOutcome::Deleted) => {
                self.remove_projection(session.id(), occurred_at);
                Ok(())
            }
            Ok(DeleteSessionOutcome::RevisionConflict { current_revision }) => {
                self.emit_operation_failure(
                    occurred_at,
                    session.id().clone(),
                    PortOperation::DeleteSession,
                    PortErrorKind::Conflict,
                );
                Err(SessionMutationError::RevisionConflict {
                    expected_revision,
                    current_revision,
                })
            }
            Ok(DeleteSessionOutcome::NotFound) => {
                self.emit_operation_failure(
                    occurred_at,
                    session.id().clone(),
                    PortOperation::DeleteSession,
                    PortErrorKind::NotFound,
                );
                Err(SessionMutationError::RemovedDuringCommit {
                    session_id: session.id().clone(),
                })
            }
            Err(error) => {
                self.emit_port_failure(occurred_at, Some(session.id().clone()), &error);
                Err(SessionMutationError::Delete(error))
            }
        }
    }

    /// Best-effort updates the prepared summary projection after durable truth.
    pub(crate) fn project(&self, session: &Session, timestamp: TimestampMillis) {
        match self.projection.upsert_session(session) {
            Ok(()) => self.emit(
                DiagnosticEventName::ProjectionUpdated,
                DiagnosticLevel::Debug,
                timestamp,
                Some(session.id().clone()),
                Some(PortOperation::UpdateProjection),
                None,
            ),
            Err(error) => {
                self.emit_port_failure(timestamp, Some(session.id().clone()), &error);
            }
        }
    }

    /// Best-effort removes all prepared views after durable deletion.
    pub(crate) fn remove_projection(&self, session_id: &SessionId, timestamp: TimestampMillis) {
        match self.projection.remove_summary(session_id) {
            Ok(()) => self.emit(
                DiagnosticEventName::ProjectionUpdated,
                DiagnosticLevel::Debug,
                timestamp,
                Some(session_id.clone()),
                Some(PortOperation::UpdateProjection),
                None,
            ),
            Err(error) => self.emit_port_failure(timestamp, Some(session_id.clone()), &error),
        }
    }

    /// Emits one redacted adapter failure using its typed operation and kind.
    pub(crate) fn emit_port_failure(
        &self,
        timestamp: TimestampMillis,
        session_id: Option<SessionId>,
        error: &PortError,
    ) {
        self.telemetry.emit_diagnostic(&DiagnosticRecord {
            event: DiagnosticEventName::PortOperationFailed,
            level: DiagnosticLevel::for_port_error(error.kind()),
            timestamp,
            session_id,
            application_operation: Some(self.application_operation),
            application_error: Some(error.category()),
            operation: Some(error.operation()),
            error_kind: Some(error.kind()),
        });
    }

    /// Emits an infrastructure-shaped failure without constructing an error.
    pub(crate) fn emit_operation_failure(
        &self,
        timestamp: TimestampMillis,
        session_id: SessionId,
        operation: PortOperation,
        error_kind: PortErrorKind,
    ) {
        self.telemetry.emit_diagnostic(&DiagnosticRecord {
            event: DiagnosticEventName::PortOperationFailed,
            level: DiagnosticLevel::for_port_error(error_kind),
            timestamp,
            session_id: Some(session_id),
            application_operation: Some(self.application_operation),
            application_error: Some(error_kind.application_category()),
            operation: Some(operation),
            error_kind: Some(error_kind),
        });
    }

    /// Emits one payload-safe diagnostic record.
    pub(crate) fn emit(
        &self,
        event: DiagnosticEventName,
        level: DiagnosticLevel,
        timestamp: TimestampMillis,
        session_id: Option<SessionId>,
        operation: Option<PortOperation>,
        error_kind: Option<PortErrorKind>,
    ) {
        self.telemetry.emit_diagnostic(&DiagnosticRecord {
            event,
            level,
            timestamp,
            session_id,
            application_operation: Some(self.application_operation),
            application_error: None,
            operation,
            error_kind,
        });
    }
}

/// Clamps an operation time so aggregate timestamps remain monotonic despite a
/// wall-clock correction. This function performs no IO and does not panic.
pub(crate) fn not_before(value: TimestampMillis, lower_bound: TimestampMillis) -> TimestampMillis {
    value.max(lower_bound)
}
