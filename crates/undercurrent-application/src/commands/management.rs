// SPDX-License-Identifier: Apache-2.0

//! Ordered durable session metadata and WorkContext mutations.

use undercurrent_domain::{
    LabelPatch, Revision, Session, SessionId, SessionName, TimestampMillis, WorkItem, WorkItemKey,
};

use super::{MutationCore, SessionMutationError, not_before};
use crate::{
    ApplicationOperation, Clock, DiagnosticEventName, DiagnosticLevel, MetricName,
    MetricObservation, SessionMutationCoordinator, SessionProjectionWriter, SessionRepository,
    Telemetry,
};

/// Input for renaming a durable session without changing stable identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenameSessionCommand {
    /// Stable target session identity.
    pub session_id: SessionId,
    /// Requested unique human-facing name.
    pub name: SessionName,
}

/// Result of an idempotent rename operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenameSessionResult {
    /// Current durable session after the operation.
    pub session: Session,
    /// Whether a new revision was committed.
    pub changed: bool,
}

/// Executes ordered rename operations through the durable aggregate boundary.
pub struct RenameSessionService<'a> {
    core: MutationCore<'a>,
    telemetry: &'a dyn Telemetry,
}

impl<'a> RenameSessionService<'a> {
    /// Constructs a rename service without performing IO.
    pub const fn new(
        clock: &'a dyn Clock,
        coordinator: &'a dyn SessionMutationCoordinator,
        repository: &'a dyn SessionRepository,
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
                ApplicationOperation::RenameSession,
            ),
            telemetry,
        }
    }

    /// Renames one session under exclusive session-local mutation ownership.
    ///
    /// Stable ID, process ownership, subscriptions, and ID-based relationships
    /// are unchanged. Repeating the current name is an idempotent no-op. This
    /// method does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed not-found, name-conflict, domain, ordering, or persistence
    /// failures. Projection failure is diagnosed and never falsifies durable
    /// success.
    pub async fn execute(
        &self,
        command: RenameSessionCommand,
    ) -> Result<RenameSessionResult, SessionMutationError> {
        let started_at = self.core.clock.now();
        let result = self.execute_inner(command).await;
        record_duration(
            self.telemetry,
            ApplicationOperation::RenameSession,
            started_at,
            self.core.clock.now(),
        );
        result
    }

    async fn execute_inner(
        &self,
        command: RenameSessionCommand,
    ) -> Result<RenameSessionResult, SessionMutationError> {
        self.core.emit(
            DiagnosticEventName::SessionRenameRequested,
            DiagnosticLevel::Info,
            self.core.clock.now(),
            Some(command.session_id.clone()),
            None,
            None,
        );
        let (_lease, mut session) = self.core.begin(&command.session_id).await?;
        let expected_revision = session.revision();
        let changed_at = not_before(self.core.clock.now(), session.updated_at());
        let Some(change) = session
            .rename(command.name, changed_at)
            .map_err(SessionMutationError::Domain)?
        else {
            return Ok(RenameSessionResult {
                session,
                changed: false,
            });
        };
        self.core
            .commit(&session, expected_revision, change, changed_at)
            .await?;
        self.core.emit(
            DiagnosticEventName::SessionRenamed,
            DiagnosticLevel::Info,
            changed_at,
            Some(session.id().clone()),
            None,
            None,
        );
        Ok(RenameSessionResult {
            session,
            changed: true,
        })
    }
}

/// Atomic partial update of one session's query-visible labels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateSessionLabelsCommand {
    /// Stable session identity.
    pub session_id: SessionId,
    /// Validated insertions and removals; other labels are preserved.
    pub patch: LabelPatch,
}

/// Durable result of a label patch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateSessionLabelsResult {
    /// Session with the committed labels.
    pub session: Session,
    /// Whether the patch committed a new revision.
    pub changed: bool,
}

/// Applies label patches under exclusive session-local mutation ownership.
pub struct UpdateSessionLabelsService<'a> {
    core: MutationCore<'a>,
}

impl<'a> UpdateSessionLabelsService<'a> {
    /// Constructs the service without IO.
    pub const fn new(
        clock: &'a dyn Clock,
        coordinator: &'a dyn SessionMutationCoordinator,
        repository: &'a dyn SessionRepository,
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
                ApplicationOperation::UpdateSessionLabels,
            ),
        }
    }

    /// Commits a label patch and its configuration event without process IO.
    /// Identical patches are idempotent no-ops.
    ///
    /// # Errors
    ///
    /// Returns typed ordering, not-found, domain, or persistence failures.
    pub async fn execute(
        &self,
        command: UpdateSessionLabelsCommand,
    ) -> Result<UpdateSessionLabelsResult, SessionMutationError> {
        let (_lease, mut session) = self.core.begin(&command.session_id).await?;
        let expected_revision = session.revision();
        let labels = command
            .patch
            .apply_to(session.labels())
            .map_err(SessionMutationError::Domain)?;
        let configuration = session.configuration().clone().with_labels(labels);
        let changed_at = not_before(self.core.clock.now(), session.updated_at());
        let Some(change) = session
            .update_configuration(configuration, changed_at)
            .map_err(SessionMutationError::Domain)?
        else {
            return Ok(UpdateSessionLabelsResult {
                session,
                changed: false,
            });
        };
        self.core
            .commit(&session, expected_revision, change, changed_at)
            .await?;
        Ok(UpdateSessionLabelsResult {
            session,
            changed: true,
        })
    }
}

/// Evidence that a destructive session deletion was explicitly authorized by
/// its presentation boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RemovalConfirmation {
    /// An interactive caller confirmed the destructive operation.
    Confirmed,
    /// A non-interactive caller supplied its explicit force option.
    Forced,
}

/// Input for deleting one terminal durable session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoveSessionCommand {
    /// Stable target session identity.
    pub session_id: SessionId,
    /// Explicit presentation-boundary confirmation evidence.
    pub confirmation: RemovalConfirmation,
}

/// Result of a durable session deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoveSessionResult {
    /// Removed stable session identity.
    pub session_id: SessionId,
    /// Revision carried by the durable removal event.
    pub removal_revision: Revision,
    /// Confirmation path used by the caller.
    pub confirmation: RemovalConfirmation,
}

/// Executes confirmed terminal-session deletion and projection cleanup.
pub struct RemoveSessionService<'a> {
    core: MutationCore<'a>,
    telemetry: &'a dyn Telemetry,
}

impl<'a> RemoveSessionService<'a> {
    /// Constructs a removal service without performing IO.
    pub const fn new(
        clock: &'a dyn Clock,
        coordinator: &'a dyn SessionMutationCoordinator,
        repository: &'a dyn SessionRepository,
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
                ApplicationOperation::RemoveSession,
            ),
            telemetry,
        }
    }

    /// Deletes one terminal session and its prepared views under one lease.
    ///
    /// Deletion and the revisioned removal event are one repository operation.
    /// Active sessions are rejected by the domain. This method does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed lifecycle, not-found, ordering, revision, or persistence
    /// failures. Projection cleanup failure is diagnosed after durable success.
    pub async fn execute(
        &self,
        command: RemoveSessionCommand,
    ) -> Result<RemoveSessionResult, SessionMutationError> {
        let started_at = self.core.clock.now();
        let result = self.execute_inner(command).await;
        record_duration(
            self.telemetry,
            ApplicationOperation::RemoveSession,
            started_at,
            self.core.clock.now(),
        );
        result
    }

    async fn execute_inner(
        &self,
        command: RemoveSessionCommand,
    ) -> Result<RemoveSessionResult, SessionMutationError> {
        self.core.emit(
            DiagnosticEventName::SessionRemovalRequested,
            DiagnosticLevel::Info,
            self.core.clock.now(),
            Some(command.session_id.clone()),
            None,
            None,
        );
        let (_lease, session) = self.core.begin(&command.session_id).await?;
        let expected_revision = session.revision();
        let changed_at = not_before(self.core.clock.now(), session.updated_at());
        let change = session
            .prepare_removal(changed_at)
            .map_err(SessionMutationError::Domain)?;
        self.core
            .delete(&session, expected_revision, change, changed_at)
            .await?;
        self.core.emit(
            DiagnosticEventName::SessionRemoved,
            DiagnosticLevel::Info,
            changed_at,
            Some(session.id().clone()),
            None,
            None,
        );
        Ok(RemoveSessionResult {
            session_id: session.id().clone(),
            removal_revision: change.revision(),
            confirmation: command.confirmation,
        })
    }
}

/// Input for idempotently inserting or replacing one WorkContext declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpsertWorkItemCommand {
    /// Stable owning session identity.
    pub session_id: SessionId,
    /// Complete validated declaration.
    pub item: WorkItem,
}

/// Input for idempotently removing one WorkContext declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoveWorkItemCommand {
    /// Stable owning session identity.
    pub session_id: SessionId,
    /// Stable owner-scoped declaration key.
    pub item_key: WorkItemKey,
}

/// Result of an idempotent WorkContext mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkItemMutationResult {
    /// Current durable session after the operation.
    pub session: Session,
    /// Whether a new session and WorkContext revision was committed.
    pub changed: bool,
}

/// Result of expiring eligible WorkContext declarations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpireWorkItemsResult {
    /// Current durable session after the operation.
    pub session: Session,
    /// Stable keys changed to the explicit `expired` status.
    pub expired: Vec<WorkItemKey>,
}

/// Executes ordered, idempotent WorkContext declaration mutations.
pub struct WorkContextService<'a> {
    clock: &'a dyn Clock,
    coordinator: &'a dyn SessionMutationCoordinator,
    repository: &'a dyn SessionRepository,
    projection: &'a dyn SessionProjectionWriter,
    telemetry: &'a dyn Telemetry,
}

impl<'a> WorkContextService<'a> {
    /// Constructs a WorkContext service without performing IO.
    pub const fn new(
        clock: &'a dyn Clock,
        coordinator: &'a dyn SessionMutationCoordinator,
        repository: &'a dyn SessionRepository,
        projection: &'a dyn SessionProjectionWriter,
        telemetry: &'a dyn Telemetry,
    ) -> Self {
        Self {
            clock,
            coordinator,
            repository,
            projection,
            telemetry,
        }
    }

    /// Inserts or replaces one declaration idempotently by owner and item ID.
    ///
    /// Foreground observation and lifecycle remain unchanged. The complete item is
    /// retained only in detail state while prepared summaries receive compact
    /// counts. This method does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed domain, ordering, or persistence failures. Projection
    /// failure is diagnosed after durable success.
    pub async fn upsert(
        &self,
        command: UpsertWorkItemCommand,
    ) -> Result<WorkItemMutationResult, SessionMutationError> {
        let operation = ApplicationOperation::UpsertWorkContext;
        let started_at = self.clock.now();
        let result = async {
            let core = self.core(operation);
            let (_lease, mut session) = core.begin(&command.session_id).await?;
            let expected_revision = session.revision();
            let changed_at = not_before(
                not_before(self.clock.now(), session.updated_at()),
                command.item.updated_at(),
            );
            let Some(change) = session
                .upsert_work_item(command.item, changed_at)
                .map_err(SessionMutationError::Domain)?
            else {
                return Ok(WorkItemMutationResult {
                    session,
                    changed: false,
                });
            };
            core.commit(&session, expected_revision, change, changed_at)
                .await?;
            core.emit(
                DiagnosticEventName::WorkContextUpserted,
                DiagnosticLevel::Info,
                changed_at,
                Some(session.id().clone()),
                None,
                None,
            );
            Ok(WorkItemMutationResult {
                session,
                changed: true,
            })
        }
        .await;
        record_duration(self.telemetry, operation, started_at, self.clock.now());
        result
    }

    /// Removes one declaration idempotently by owner and item ID.
    ///
    /// An absent item is a successful no-op. Foreground observation and lifecycle
    /// remain unchanged. This method does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed domain, ordering, or persistence failures. Projection
    /// failure is diagnosed after durable success.
    pub async fn remove(
        &self,
        command: RemoveWorkItemCommand,
    ) -> Result<WorkItemMutationResult, SessionMutationError> {
        let operation = ApplicationOperation::RemoveWorkContext;
        let started_at = self.clock.now();
        let result = async {
            let core = self.core(operation);
            let (_lease, mut session) = core.begin(&command.session_id).await?;
            if session.work_context().get(&command.item_key).is_none() {
                return Ok(WorkItemMutationResult {
                    session,
                    changed: false,
                });
            }
            let expected_revision = session.revision();
            let changed_at = not_before(self.clock.now(), session.updated_at());
            let (_, change) = session
                .remove_work_item(&command.item_key, changed_at)
                .map_err(SessionMutationError::Domain)?;
            core.commit(&session, expected_revision, change, changed_at)
                .await?;
            core.emit(
                DiagnosticEventName::WorkContextRemoved,
                DiagnosticLevel::Info,
                changed_at,
                Some(session.id().clone()),
                None,
                None,
            );
            Ok(WorkItemMutationResult {
                session,
                changed: true,
            })
        }
        .await;
        record_duration(self.telemetry, operation, started_at, self.clock.now());
        result
    }

    /// Marks eligible active declarations as explicitly expired in one batch.
    ///
    /// No global scan occurs; the caller selects the owning session to check.
    /// This method does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed domain, ordering, or persistence failures. Projection
    /// failure is diagnosed after durable success.
    pub async fn expire(
        &self,
        session_id: SessionId,
    ) -> Result<ExpireWorkItemsResult, SessionMutationError> {
        let operation = ApplicationOperation::ExpireWorkContext;
        let started_at = self.clock.now();
        let result = async {
            let core = self.core(operation);
            let (_lease, mut session) = core.begin(&session_id).await?;
            let expected_revision = session.revision();
            let changed_at = not_before(self.clock.now(), session.updated_at());
            let (expired, change) = session
                .expire_work_items(changed_at)
                .map_err(SessionMutationError::Domain)?;
            let Some(change) = change else {
                return Ok(ExpireWorkItemsResult { session, expired });
            };
            core.commit(&session, expected_revision, change, changed_at)
                .await?;
            core.emit(
                DiagnosticEventName::WorkContextExpired,
                DiagnosticLevel::Info,
                changed_at,
                Some(session.id().clone()),
                None,
                None,
            );
            Ok(ExpireWorkItemsResult { session, expired })
        }
        .await;
        record_duration(self.telemetry, operation, started_at, self.clock.now());
        result
    }

    fn core(&self, operation: ApplicationOperation) -> MutationCore<'_> {
        MutationCore::new(
            self.clock,
            self.coordinator,
            self.repository,
            self.projection,
            self.telemetry,
            operation,
        )
    }
}

fn record_duration(
    telemetry: &dyn Telemetry,
    operation: ApplicationOperation,
    started_at: TimestampMillis,
    finished_at: TimestampMillis,
) {
    telemetry.record_metric(MetricObservation {
        name: MetricName::OperationDurationSeconds,
        value: finished_at
            .saturating_duration_since(started_at)
            .as_secs_f64(),
        operation: Some(operation),
    });
}
