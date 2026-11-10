// SPDX-License-Identifier: Apache-2.0

//! Durable SQLite session repository and transactional outbox adapter.
//!
//! Clones share one worker and connection. Each polled request attempts bounded
//! queue admission without waiting for capacity; admitted work finishes even
//! if its reply future is dropped. Final handle drop closes admission, drains
//! queued work, and joins the worker without a shutdown deadline.
//!
//! Opening performs schema migration and metadata validation synchronously.
//! Record decoding remains part of reads, so successful open is not a complete
//! audit of every aggregate payload. Maintenance opens separate connections.

mod codec;
mod crash;
mod database;
mod input_deliveries;
pub use input_deliveries::SqliteInputDeliveryStore;
mod maintenance;
#[cfg(test)]
mod tests;

pub use crash::*;
pub use maintenance::*;

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use async_channel::{Receiver, Sender};
use database::{Database, DatabaseError, validate_supported_release_schema};
use rusqlite::{Connection, OpenFlags};
use undercurrent_application::{
    AgentChannelRepository, AgentChannelTarget, AgentChannelTargetResolution, AgentEventAdmission,
    AgentEventAdmissionOutcome, AgentEventPollOutcome, AgentEventReplyAdmission,
    AgentEventReplyOutcome, AgentEventReplyRecord, CancelRuntimeAttemptCommand,
    CommitSessionOutcome, DeleteSessionOutcome, GetRuntimeCancellationStatusQuery,
    InsertSessionOutcome, MetricName, MetricObservation, OperationAttemptAcceptance,
    OperationAttemptAdmission, OperationAttemptCompensation, OperationAttemptCompensationOutcome,
    OperationAttemptCompensationPreparation, OperationAttemptCompensationPreparationOutcome,
    OperationAttemptReconciliation, OperationAttemptRepairCommit, OperationAttemptRepairOutcome,
    OperationAttemptRepairRepository, OperationAttemptScan, OperationAttemptSettlement,
    OperationAttemptSettlementOutcome, OperationAttemptStore, OutboxAcknowledgeOutcome,
    OutboxSequence, PortError, PortErrorKind, PortFuture, PortOperation, RecoverySessionBatch,
    RuntimeAttemptStatusLookup, RuntimeAttemptStatusRepository, RuntimeCancellationAdmission,
    RuntimeCancellationEffectStart, RuntimeCancellationRecovery, RuntimeCancellationRepository,
    RuntimeCancellationSettlement, RuntimeCancellationState, RuntimeCancellationStatusLookup,
    SessionOutbox, SessionOutboxEvent, SessionOutboxRecord, SessionRecoverySource,
    SessionRepository, Telemetry, UnresolvedOperationAttempts,
};
use undercurrent_domain::{
    FencingToken, OperationAttempt, OperationAttemptId, ProcessInstanceId, Revision, Session,
    SessionId, SessionName, TimestampMillis,
};

#[derive(Clone)]
pub(super) struct SqliteOperationAttemptScan {
    limit: usize,
    exclusions: Vec<(OperationAttemptId, FencingToken)>,
    scope: undercurrent_application::OperationAttemptScope,
    skip_input: bool,
    after: Option<undercurrent_application::OperationAttemptScanCursor>,
}

impl SqliteOperationAttemptScan {
    fn from_application(scan: &OperationAttemptScan) -> Self {
        Self {
            limit: scan.limit(),
            scope: scan.scope().clone(),
            skip_input: scan.skips_input(),
            after: scan.cursor().cloned(),
            exclusions: scan
                .exclusions()
                .iter()
                .map(|exclusion| (exclusion.attempt_id().clone(), exclusion.fencing_token()))
                .collect(),
        }
    }

    pub(super) fn scope_parameters(&self) -> (bool, Option<&str>) {
        use undercurrent_application::OperationAttemptScope;
        match &self.scope {
            OperationAttemptScope::All => (true, None),
            OperationAttemptScope::Host => (false, None),
            OperationAttemptScope::Session(id) => (false, Some(id.as_str())),
        }
    }

    pub(super) const fn limit(&self) -> usize {
        self.limit
    }

    pub(super) fn exclusions(&self) -> &[(OperationAttemptId, FencingToken)] {
        &self.exclusions
    }
}

/// Hard storage and queue bounds for one SQLite repository.
///
/// These bound sessions, pending outbox rows, session-record bytes, returned
/// batches, and queued requests. They are not a total database byte quota and
/// do not cap retained attempts, controls, or replied agent events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqliteRepositoryLimits {
    max_sessions: usize,
    max_outbox_events: usize,
    max_record_bytes: usize,
    max_read_batch: usize,
    command_queue_capacity: usize,
}

impl SqliteRepositoryLimits {
    /// Validates explicit session, outbox, aggregate-record, read-batch, and
    /// worker-queue bounds. This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed error naming the first zero limit or SQL-bound limit
    /// that cannot be represented by SQLite.
    pub fn new(
        max_sessions: usize,
        max_outbox_events: usize,
        max_record_bytes: usize,
        max_read_batch: usize,
        command_queue_capacity: usize,
    ) -> Result<Self, SqliteRepositoryConfigError> {
        for (field, value) in [
            ("max_sessions", max_sessions),
            ("max_outbox_events", max_outbox_events),
            ("max_record_bytes", max_record_bytes),
            ("max_read_batch", max_read_batch),
        ] {
            if value == 0 {
                return Err(SqliteRepositoryConfigError::ZeroLimit { field });
            }
            if (value as u128) > (i64::MAX as u128) {
                return Err(SqliteRepositoryConfigError::LimitTooLarge { field });
            }
        }
        if command_queue_capacity == 0 {
            return Err(SqliteRepositoryConfigError::ZeroLimit {
                field: "command_queue_capacity",
            });
        }
        Ok(Self {
            max_sessions,
            max_outbox_events,
            max_record_bytes,
            max_read_batch,
            command_queue_capacity,
        })
    }

    /// Returns the durable session capacity.
    pub const fn max_sessions(self) -> usize {
        self.max_sessions
    }

    /// Returns the pending outbox-record capacity.
    pub const fn max_outbox_events(self) -> usize {
        self.max_outbox_events
    }

    /// Returns the maximum encoded aggregate bytes.
    pub const fn max_record_bytes(self) -> usize {
        self.max_record_bytes
    }

    /// Returns the maximum outbox or recovery rows per read.
    pub const fn max_read_batch(self) -> usize {
        self.max_read_batch
    }

    /// Returns the bounded database-worker request capacity.
    pub const fn command_queue_capacity(self) -> usize {
        self.command_queue_capacity
    }
}

/// Complete validated SQLite repository configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteRepositoryConfig {
    path: PathBuf,
    limits: SqliteRepositoryLimits,
    busy_timeout: Duration,
}

impl SqliteRepositoryConfig {
    /// Constructs a repository configuration with an explicit database path,
    /// hard limits, and non-zero SQLite lock wait timeout. This function performs
    /// no IO and does not panic.
    ///
    /// The timeout does not bound total queue delay, query execution, startup,
    /// maintenance, or final worker join.
    ///
    /// # Errors
    ///
    /// Returns a typed error for an empty path or zero busy timeout.
    pub fn new(
        path: impl Into<PathBuf>,
        limits: SqliteRepositoryLimits,
        busy_timeout: Duration,
    ) -> Result<Self, SqliteRepositoryConfigError> {
        let path = path.into();
        if path.as_os_str().is_empty() {
            return Err(SqliteRepositoryConfigError::EmptyPath);
        }
        if busy_timeout.is_zero() {
            return Err(SqliteRepositoryConfigError::ZeroBusyTimeout);
        }
        Ok(Self {
            path,
            limits,
            busy_timeout,
        })
    }

    /// Returns the configured database path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns repository capacity and batch limits.
    pub const fn limits(&self) -> SqliteRepositoryLimits {
        self.limits
    }

    /// Returns the SQLite lock wait timeout, not an end-to-end request deadline.
    pub const fn busy_timeout(&self) -> Duration {
        self.busy_timeout
    }
}

/// Invalid durable repository configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqliteRepositoryConfigError {
    /// Database path was empty.
    EmptyPath,
    /// A required hard limit was zero.
    ZeroLimit {
        /// Stable invalid field name.
        field: &'static str,
    },
    /// A persisted or SQL-bound limit exceeded SQLite's signed integer range.
    LimitTooLarge {
        /// Stable invalid field name.
        field: &'static str,
    },
    /// SQLite lock wait deadline was zero.
    ZeroBusyTimeout,
}

impl Display for SqliteRepositoryConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyPath => formatter.write_str("SQLite repository path must not be empty"),
            Self::ZeroLimit { field } => {
                write!(
                    formatter,
                    "SQLite repository limit {field} must be non-zero"
                )
            }
            Self::LimitTooLarge { field } => write!(
                formatter,
                "SQLite repository limit {field} exceeds the supported integer range"
            ),
            Self::ZeroBusyTimeout => {
                formatter.write_str("SQLite repository busy timeout must be non-zero")
            }
        }
    }
}

impl Error for SqliteRepositoryConfigError {}

/// Failure while creating and validating the durable repository.
#[derive(Debug)]
pub enum SqliteRepositoryOpenError {
    /// Parent state directory could not be created.
    CreateDirectory(std::io::Error),
    /// SQLite database could not be opened.
    Open(rusqlite::Error),
    /// SQLite durability or timeout settings could not be applied.
    Configure(rusqlite::Error),
    /// Database schema or durable metadata was unsupported or corrupt.
    Validate(SqliteRepositoryValidationError),
    /// Restrictive database-file permissions could not be applied.
    Permissions(std::io::Error),
    /// Dedicated database worker could not be created.
    SpawnWorker(std::io::Error),
}

/// Schema, metadata, or durable-record validation failure observed while
/// opening a repository.
#[derive(Debug)]
pub struct SqliteRepositoryValidationError {
    source: DatabaseError,
}

impl SqliteRepositoryValidationError {
    /// Returns the stable adapter failure kind suitable for startup diagnostics.
    /// This function performs no IO and does not panic.
    pub fn kind(&self) -> PortErrorKind {
        self.source.port_kind()
    }
}

impl Display for SqliteRepositoryValidationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.source, formatter)
    }
}

impl Error for SqliteRepositoryValidationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl Display for SqliteRepositoryOpenError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CreateDirectory(_) => {
                formatter.write_str("could not create SQLite repository state directory")
            }
            Self::Open(_) => formatter.write_str("could not open SQLite repository"),
            Self::Configure(_) => formatter.write_str("could not configure SQLite repository"),
            Self::Validate(_) => formatter.write_str("could not validate SQLite repository state"),
            Self::Permissions(_) => {
                formatter.write_str("could not restrict SQLite repository permissions")
            }
            Self::SpawnWorker(_) => formatter.write_str("could not start SQLite repository worker"),
        }
    }
}

impl Error for SqliteRepositoryOpenError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CreateDirectory(error) | Self::Permissions(error) | Self::SpawnWorker(error) => {
                Some(error)
            }
            Self::Open(error) | Self::Configure(error) => Some(error),
            Self::Validate(error) => Some(error),
        }
    }
}

/// Bounded lock-consistent persistent repository pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqliteRepositoryPressure {
    /// Durable session rows.
    pub sessions: usize,
    /// Pending transactional outbox rows.
    pub outbox_events: usize,
    /// Configured session capacity.
    pub session_limit: usize,
    /// Configured pending outbox capacity.
    pub outbox_limit: usize,
    /// Best-effort requests waiting behind the currently executing operation.
    pub queued_requests: usize,
    /// Configured database-worker request capacity.
    pub request_queue_capacity: usize,
}

/// Cloneable durable repository handle backed by one bounded worker.
///
/// Clones share the worker; separate opens create separate connections. SQLite
/// transactions and revision/fence checks arbitrate writes across connections.
/// Dropping the last handle synchronously drains admitted requests and joins
/// the worker. There is no explicit shutdown or request-interruption method.
#[derive(Clone)]
pub struct SqliteSessionRepository {
    inner: Arc<RepositoryWorker>,
}

impl SqliteSessionRepository {
    /// Opens, configures, schema-validates, and permission-hardens one SQLite
    /// database, then moves its connection onto a dedicated bounded worker.
    ///
    /// Existing state is rejected if its schema is unsupported, metadata is
    /// inconsistent, or configured capacities are below durable row counts.
    /// The database uses WAL, full synchronous durability, a configured lock
    /// timeout, and restrictive file permissions on Unix. This function
    /// performs filesystem and SQLite IO and spawns one thread. It does not
    /// panic.
    ///
    /// Recognized historical layouts migrate before the worker starts. This
    /// does not decode every retained session or perform a full integrity scan;
    /// later reads can still report invalid records. Existing Unix parent
    /// directory permissions are preserved; missing parents are owner-only.
    ///
    /// # Errors
    ///
    /// Returns a typed open, configuration, permission, schema, corruption, or
    /// worker-spawn failure.
    pub fn open(config: SqliteRepositoryConfig) -> Result<Self, SqliteRepositoryOpenError> {
        Self::open_inner(config, None, Arc::new(NoopSqliteCrashInjector))
    }

    /// Opens a durable repository and reports open, recovery, and checkpoint
    /// outcomes through the supplied non-blocking telemetry port.
    ///
    /// Telemetry has no result channel and must honor its non-blocking,
    /// non-panicking port contract. This adapter does not isolate a misbehaving
    /// telemetry implementation. This function performs filesystem and SQLite
    /// IO and spawns one thread.
    ///
    /// # Errors
    ///
    /// Returns the same typed failures as [`Self::open`]. A failed open is
    /// reported as a persistence recovery failure before returning.
    pub fn open_with_telemetry(
        config: SqliteRepositoryConfig,
        telemetry: Arc<dyn Telemetry>,
    ) -> Result<Self, SqliteRepositoryOpenError> {
        let result = Self::open_inner(
            config,
            Some(telemetry.clone()),
            Arc::new(NoopSqliteCrashInjector),
        );
        record_persistence_result(
            telemetry.as_ref(),
            result.is_ok(),
            MetricName::PersistenceRecoverySucceeded,
            MetricName::PersistenceRecoveryFailed,
        );
        result
    }

    /// Opens a repository with deterministic transaction failure injection.
    ///
    /// This is intended for restart and rollback conformance tests. Production
    /// composition should use [`Self::open`] or [`Self::open_with_telemetry`].
    pub fn open_with_crash_injector(
        config: SqliteRepositoryConfig,
        crash_injector: Arc<dyn SqliteCrashInjector>,
    ) -> Result<Self, SqliteRepositoryOpenError> {
        Self::open_inner(config, None, crash_injector)
    }

    fn open_inner(
        config: SqliteRepositoryConfig,
        telemetry: Option<Arc<dyn Telemetry>>,
        crash_injector: Arc<dyn SqliteCrashInjector>,
    ) -> Result<Self, SqliteRepositoryOpenError> {
        with_file_owner(|| Self::open_with_file_owner(config, telemetry, crash_injector))
            .map_err(SqliteRepositoryOpenError::Permissions)?
    }

    fn open_with_file_owner(
        config: SqliteRepositoryConfig,
        telemetry: Option<Arc<dyn Telemetry>>,
        crash_injector: Arc<dyn SqliteCrashInjector>,
    ) -> Result<Self, SqliteRepositoryOpenError> {
        create_parent_directory(config.path())?;
        // Reject unknown layouts before permission changes or writable SQLite
        // configuration can alter the existing database. See
        // [schema admission](../../docs/reference/storage-format.md#current-schema-admission).
        preflight_existing_database(config.path())?;
        prepare_database_file(config.path())?;
        let connection = Connection::open_with_flags(
            config.path(),
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(SqliteRepositoryOpenError::Open)?;
        configure_connection(&connection, config.busy_timeout())?;
        let database =
            Database::initialize_with_crash_injector(connection, config.limits(), crash_injector)
                .map_err(|source| {
                SqliteRepositoryOpenError::Validate(SqliteRepositoryValidationError { source })
            })?;
        let (requests, receiver) = async_channel::bounded(config.limits().command_queue_capacity());
        let (ready, initialized) = std::sync::mpsc::sync_channel(1);
        let worker = thread::Builder::new()
            .name("undercurrent-sqlite-repository".to_owned())
            .spawn(move || {
                // SQLite creates/recreates WAL, SHM and rollback journals during
                // requests and connection close, not only during startup.
                let result = with_file_owner(|| {
                    let _ = ready.send(Ok(()));
                    run_worker(database, receiver, telemetry);
                });
                if let Err(error) = result {
                    let _ = ready.send(Err(error));
                }
            })
            .map_err(SqliteRepositoryOpenError::SpawnWorker)?;
        initialized
            .recv()
            .map_err(|_| {
                SqliteRepositoryOpenError::SpawnWorker(std::io::Error::other(
                    "SQLite worker exited before security initialization",
                ))
            })?
            .map_err(SqliteRepositoryOpenError::Permissions)?;
        let inner = Arc::new(RepositoryWorker {
            requests,
            queue_capacity: config.limits().command_queue_capacity(),
            worker: Mutex::new(Some(worker)),
        });
        Ok(Self { inner })
    }

    /// Reads lock-consistent durable row pressure without hydrating aggregate
    /// payloads. This performs bounded worker and SQLite IO and does not panic.
    ///
    /// Session/outbox counters come from one database read. Queue length is a
    /// separate best-effort sample after the reply; it excludes executing work.
    /// This inspection itself requires request-queue admission.
    ///
    /// # Errors
    ///
    /// Returns typed queue, worker, timeout, corruption, or SQLite failures.
    pub async fn pressure(&self) -> Result<SqliteRepositoryPressure, PortError> {
        let mut pressure = self
            .request(PortOperation::InspectPersistence, None, Request::Pressure)
            .await?;
        pressure.queued_requests = self.inner.requests.len();
        pressure.request_queue_capacity = self.inner.queue_capacity;
        Ok(pressure)
    }

    async fn request<T>(
        &self,
        operation: PortOperation,
        session_id: Option<SessionId>,
        build: impl FnOnce(Sender<Result<T, DatabaseError>>) -> Request,
    ) -> Result<T, PortError>
    where
        T: Send + 'static,
    {
        // Once admitted, mutations finish even if their caller is cancelled;
        // abandoning a SQLite transaction mid-flight would create ambiguity.
        let (reply, response) = async_channel::bounded(1);
        match self.inner.requests.try_send(build(reply)) {
            Ok(()) => {}
            Err(async_channel::TrySendError::Full(_)) => {
                let error = PortError::new(
                    PortErrorKind::CapacityExceeded,
                    operation,
                    session_id,
                    WorkerFailure("SQLite request queue is full"),
                );
                return Err(if operation == PortOperation::InsertSession {
                    error.with_known_absent_effect()
                } else {
                    error
                });
            }
            Err(async_channel::TrySendError::Closed(_)) => {
                let error = PortError::new(
                    PortErrorKind::Unavailable,
                    operation,
                    session_id,
                    WorkerFailure("SQLite repository worker is unavailable"),
                );
                return Err(if operation == PortOperation::InsertSession {
                    error.with_known_absent_effect()
                } else {
                    error
                });
            }
        }
        match response.recv().await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => {
                // Insert's capacity checks precede every SQL write and process
                // activation. Other errors may follow an unobserved commit.
                let rejected_insert = operation == PortOperation::InsertSession
                    && matches!(error, DatabaseError::Capacity(_));
                let error = PortError::new(error.port_kind(), operation, session_id, error);
                Err(if rejected_insert {
                    error.with_known_absent_effect()
                } else {
                    error
                })
            }
            Err(_) => Err(PortError::new(
                PortErrorKind::Interrupted,
                operation,
                session_id,
                WorkerFailure("SQLite repository worker ended before replying"),
            )),
        }
    }
}

impl SessionRepository for SqliteSessionRepository {
    fn insert<'a>(
        &'a self,
        session: &'a Session,
        event: &'a SessionOutboxEvent,
    ) -> PortFuture<'a, Result<InsertSessionOutcome, PortError>> {
        let session = session.clone();
        let session_id = session.id().clone();
        let event = event.clone();
        Box::pin(async move {
            self.request(PortOperation::InsertSession, Some(session_id), |reply| {
                Request::Insert {
                    session,
                    event,
                    reply,
                }
            })
            .await
        })
    }

    fn load<'a>(
        &'a self,
        session_id: &'a SessionId,
    ) -> PortFuture<'a, Result<Option<Session>, PortError>> {
        let session_id = session_id.clone();
        Box::pin(async move {
            self.request(
                PortOperation::LoadSession,
                Some(session_id.clone()),
                |reply| Request::Load { session_id, reply },
            )
            .await
        })
    }

    fn find_by_name<'a>(
        &'a self,
        name: &'a SessionName,
    ) -> PortFuture<'a, Result<Option<Session>, PortError>> {
        let name = name.clone();
        Box::pin(async move {
            self.request(PortOperation::FindSessionByName, None, |reply| {
                Request::FindByName { name, reply }
            })
            .await
        })
    }

    fn commit<'a>(
        &'a self,
        session: &'a Session,
        expected_revision: Revision,
        events: &'a [SessionOutboxEvent],
    ) -> PortFuture<'a, Result<CommitSessionOutcome, PortError>> {
        let session = session.clone();
        let session_id = session.id().clone();
        let events = events.to_vec();
        Box::pin(async move {
            self.request(PortOperation::CommitSession, Some(session_id), |reply| {
                Request::Commit {
                    session,
                    expected_revision,
                    events,
                    reply,
                }
            })
            .await
        })
    }

    fn delete<'a>(
        &'a self,
        session_id: &'a SessionId,
        expected_revision: Revision,
        event: &'a SessionOutboxEvent,
    ) -> PortFuture<'a, Result<DeleteSessionOutcome, PortError>> {
        let session_id = session_id.clone();
        let context = session_id.clone();
        let event = event.clone();
        Box::pin(async move {
            self.request(PortOperation::DeleteSession, Some(context), |reply| {
                Request::Delete {
                    session_id,
                    expected_revision,
                    event,
                    reply,
                }
            })
            .await
        })
    }
}

impl SessionOutbox for SqliteSessionRepository {
    fn read_pending<'a>(
        &'a self,
        limit: usize,
    ) -> PortFuture<'a, Result<Vec<SessionOutboxRecord>, PortError>> {
        Box::pin(async move {
            self.request(PortOperation::ReadOutbox, None, |reply| {
                Request::ReadPending { limit, reply }
            })
            .await
        })
    }

    fn acknowledge_through<'a>(
        &'a self,
        through: OutboxSequence,
    ) -> PortFuture<'a, Result<OutboxAcknowledgeOutcome, PortError>> {
        Box::pin(async move {
            self.request(PortOperation::AcknowledgeOutbox, None, |reply| {
                Request::Acknowledge { through, reply }
            })
            .await
        })
    }
}

impl SessionRecoverySource for SqliteSessionRepository {
    fn read_batch<'a>(
        &'a self,
        after: Option<&'a SessionId>,
        limit: usize,
    ) -> PortFuture<'a, Result<RecoverySessionBatch, PortError>> {
        let after = after.cloned();
        Box::pin(async move {
            self.request(PortOperation::ReadRecoverySessions, None, |reply| {
                Request::ReadBatch {
                    after,
                    limit,
                    reply,
                }
            })
            .await
        })
    }

    fn claim_runtime_cancellation_recovery<'a>(
        &'a self,
        session_id: &'a SessionId,
        observed_at: undercurrent_domain::TimestampMillis,
    ) -> PortFuture<'a, Result<Option<RuntimeCancellationRecovery>, PortError>> {
        let session_id = session_id.clone();
        Box::pin(async move {
            self.request(
                PortOperation::MarkRuntimeCancellationEffectStarted,
                Some(session_id.clone()),
                |reply| Request::ClaimRuntimeCancellationRecovery {
                    session_id,
                    observed_at,
                    reply,
                },
            )
            .await
        })
    }
}

impl OperationAttemptStore for SqliteSessionRepository {
    fn retire_input<'a>(
        &'a self,
        retirement: &'a undercurrent_application::InputAttemptRetirement,
    ) -> PortFuture<'a, Result<bool, PortError>> {
        let retirement = retirement.clone();
        Box::pin(async move {
            self.request(PortOperation::ReconcileOperationAttempt, None, |reply| {
                Request::RetireInput { retirement, reply }
            })
            .await
        })
    }
    fn pending_input_retirement_acks(
        &self,
        limit: usize,
    ) -> PortFuture<'_, Result<Vec<OperationAttempt>, PortError>> {
        Box::pin(async move {
            self.request(PortOperation::ReadPendingOperationAttempts, None, |reply| {
                Request::PendingInputRetirementAcks { limit, reply }
            })
            .await
        })
    }
    fn acknowledge_input_retirement<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.request(PortOperation::ReconcileOperationAttempt, None, |reply| {
                Request::AcknowledgeInputRetirement { attempt, reply }
            })
            .await
        })
    }

    fn accept_next<'a>(
        &'a self,
        admission: &'a OperationAttemptAdmission,
    ) -> PortFuture<'a, Result<OperationAttemptAcceptance, PortError>> {
        let admission = admission.clone();
        let session_id = admission.target_session().cloned();
        Box::pin(async move {
            self.request(PortOperation::AcceptOperationAttempt, session_id, |reply| {
                Request::AcceptNextAttempt { admission, reply }
            })
            .await
        })
    }

    fn accept<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<OperationAttemptAcceptance, PortError>> {
        let attempt = attempt.clone();
        let session_id = attempt.target_session().cloned();
        Box::pin(async move {
            self.request(PortOperation::AcceptOperationAttempt, session_id, |reply| {
                Request::AcceptAttempt { attempt, reply }
            })
            .await
        })
    }

    fn settle<'a>(
        &'a self,
        settlement: &'a OperationAttemptSettlement,
    ) -> PortFuture<'a, Result<OperationAttemptSettlementOutcome, PortError>> {
        let settlement = settlement.clone();
        Box::pin(async move {
            self.request(PortOperation::SettleOperationAttempt, None, |reply| {
                Request::SettleAttempt { settlement, reply }
            })
            .await
        })
    }

    fn compensate<'a>(
        &'a self,
        compensation: &'a OperationAttemptCompensation,
    ) -> PortFuture<'a, Result<OperationAttemptCompensationOutcome, PortError>> {
        let compensation = compensation.clone();
        Box::pin(async move {
            self.request(PortOperation::CompensateOperationAttempt, None, |reply| {
                Request::CompensateAttempt {
                    compensation,
                    reply,
                }
            })
            .await
        })
    }

    fn prepare_compensation<'a>(
        &'a self,
        preparation: &'a OperationAttemptCompensationPreparation,
    ) -> PortFuture<'a, Result<OperationAttemptCompensationPreparationOutcome, PortError>> {
        let preparation = preparation.clone();
        Box::pin(async move {
            self.request(
                PortOperation::PrepareOperationAttemptCompensation,
                None,
                |reply| Request::PrepareAttemptCompensation { preparation, reply },
            )
            .await
        })
    }

    fn reconcile<'a>(
        &'a self,
        reconciliation: &'a OperationAttemptReconciliation,
    ) -> PortFuture<'a, Result<OperationAttemptSettlementOutcome, PortError>> {
        let reconciliation = reconciliation.clone();
        Box::pin(async move {
            self.request(PortOperation::ReconcileOperationAttempt, None, |reply| {
                Request::ReconcileAttempt {
                    reconciliation,
                    reply,
                }
            })
            .await
        })
    }

    fn load<'a>(
        &'a self,
        attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<Option<OperationAttempt>, PortError>> {
        let attempt_id = attempt_id.clone();
        Box::pin(async move {
            self.request(PortOperation::LoadOperationAttempt, None, |reply| {
                Request::LoadAttempt { attempt_id, reply }
            })
            .await
        })
    }

    fn pending_reconciliation<'a>(
        &'a self,
        scan: &'a OperationAttemptScan,
    ) -> PortFuture<'a, Result<Vec<OperationAttempt>, PortError>> {
        let scan = SqliteOperationAttemptScan::from_application(scan);
        Box::pin(async move {
            self.request(PortOperation::ReadPendingOperationAttempts, None, |reply| {
                Request::PendingAttemptReconciliation { scan, reply }
            })
            .await
        })
    }
}

impl OperationAttemptRepairRepository for SqliteSessionRepository {
    fn load_repair_candidate<'a>(
        &'a self,
        attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<Option<OperationAttempt>, PortError>> {
        let attempt_id = attempt_id.clone();
        Box::pin(async move {
            self.request(PortOperation::RepairOperationAttempt, None, |reply| {
                Request::LoadRepairCandidate { attempt_id, reply }
            })
            .await
        })
    }

    fn repair_unknown<'a>(
        &'a self,
        commit: &'a OperationAttemptRepairCommit,
    ) -> PortFuture<'a, Result<OperationAttemptRepairOutcome, PortError>> {
        let commit = commit.clone();
        Box::pin(async move {
            self.request(PortOperation::RepairOperationAttempt, None, |reply| {
                Request::RepairUnknownAttempt { commit, reply }
            })
            .await
        })
    }

    fn unresolved_attempts<'a>(
        &'a self,
        scan: &'a OperationAttemptScan,
    ) -> PortFuture<'a, Result<UnresolvedOperationAttempts, PortError>> {
        let scan = SqliteOperationAttemptScan::from_application(scan);
        Box::pin(async move {
            self.request(
                PortOperation::InspectOperationAttemptReadiness,
                None,
                |reply| Request::UnresolvedAttempts { scan, reply },
            )
            .await
        })
    }
}

impl RuntimeAttemptStatusRepository for SqliteSessionRepository {
    fn get_runtime_attempt_status<'a>(
        &'a self,
        attempt_id: &'a OperationAttemptId,
        request_digest: undercurrent_domain::PayloadDigest,
    ) -> PortFuture<'a, Result<RuntimeAttemptStatusLookup, PortError>> {
        let attempt_id = attempt_id.clone();
        Box::pin(async move {
            self.request(PortOperation::GetRuntimeAttemptStatus, None, |reply| {
                Request::GetRuntimeAttemptStatus {
                    attempt_id,
                    request_digest,
                    reply,
                }
            })
            .await
        })
    }
}

impl RuntimeCancellationRepository for SqliteSessionRepository {
    fn admit_runtime_cancellation<'a>(
        &'a self,
        command: &'a CancelRuntimeAttemptCommand,
        observed_at: undercurrent_domain::TimestampMillis,
    ) -> PortFuture<'a, Result<RuntimeCancellationAdmission, PortError>> {
        let command = command.clone();
        let session_id = command.target().session_id().clone();
        Box::pin(async move {
            self.request(
                PortOperation::AdmitRuntimeCancellation,
                Some(session_id),
                |reply| Request::AdmitRuntimeCancellation {
                    command,
                    observed_at,
                    reply,
                },
            )
            .await
        })
    }

    fn settle_runtime_cancellation<'a>(
        &'a self,
        control_id: &'a OperationAttemptId,
        command_digest: undercurrent_domain::PayloadDigest,
        state: RuntimeCancellationState,
    ) -> PortFuture<'a, Result<RuntimeCancellationSettlement, PortError>> {
        let control_id = control_id.clone();
        let session_id = state.session().session_id().clone();
        Box::pin(async move {
            self.request(
                PortOperation::SettleRuntimeCancellation,
                Some(session_id),
                |reply| Request::SettleRuntimeCancellation {
                    control_id,
                    command_digest,
                    state,
                    reply,
                },
            )
            .await
        })
    }

    fn mark_runtime_cancellation_effect_started<'a>(
        &'a self,
        control_id: &'a OperationAttemptId,
        command_digest: undercurrent_domain::PayloadDigest,
        observed_at: undercurrent_domain::TimestampMillis,
    ) -> PortFuture<'a, Result<RuntimeCancellationEffectStart, PortError>> {
        let control_id = control_id.clone();
        Box::pin(async move {
            self.request(
                PortOperation::MarkRuntimeCancellationEffectStarted,
                None,
                |reply| Request::MarkRuntimeCancellationEffectStarted {
                    control_id,
                    command_digest,
                    observed_at,
                    reply,
                },
            )
            .await
        })
    }

    fn get_runtime_cancellation_status<'a>(
        &'a self,
        query: &'a GetRuntimeCancellationStatusQuery,
    ) -> PortFuture<'a, Result<RuntimeCancellationStatusLookup, PortError>> {
        let query = query.clone();
        let session_id = query.session_id().clone();
        Box::pin(async move {
            self.request(
                PortOperation::GetRuntimeCancellationStatus,
                Some(session_id),
                |reply| Request::GetRuntimeCancellationStatus { query, reply },
            )
            .await
        })
    }
}

impl AgentChannelRepository for SqliteSessionRepository {
    fn resolve_agent_channel_target<'a>(
        &'a self,
        session_id: &'a SessionId,
        process_instance_id: &'a ProcessInstanceId,
        observed_at: TimestampMillis,
    ) -> PortFuture<'a, Result<AgentChannelTargetResolution, PortError>> {
        let session_id = session_id.clone();
        let process_instance_id = process_instance_id.clone();
        let operation_session_id = session_id.clone();
        Box::pin(async move {
            self.request(
                PortOperation::ResolveAgentChannelTarget,
                Some(operation_session_id),
                |reply| Request::ResolveAgentChannelTarget {
                    session_id,
                    process_instance_id,
                    observed_at,
                    reply,
                },
            )
            .await
        })
    }

    fn enqueue_agent_event<'a>(
        &'a self,
        admission: &'a AgentEventAdmission,
    ) -> PortFuture<'a, Result<AgentEventAdmissionOutcome, PortError>> {
        let admission = admission.clone();
        let session_id = admission.target.session_id.clone();
        Box::pin(async move {
            self.request(
                PortOperation::EnqueueAgentEvent,
                Some(session_id),
                |reply| Request::EnqueueAgentEvent { admission, reply },
            )
            .await
        })
    }

    fn poll_agent_event<'a>(
        &'a self,
        target: &'a AgentChannelTarget,
        after_sequence: u64,
        observed_at: undercurrent_domain::TimestampMillis,
    ) -> PortFuture<'a, Result<AgentEventPollOutcome, PortError>> {
        let target = target.clone();
        let session_id = target.session_id.clone();
        Box::pin(async move {
            self.request(PortOperation::PollAgentEvent, Some(session_id), |reply| {
                Request::PollAgentEvent {
                    target,
                    after_sequence,
                    observed_at,
                    reply,
                }
            })
            .await
        })
    }

    fn reply_agent_event<'a>(
        &'a self,
        admission: &'a AgentEventReplyAdmission,
    ) -> PortFuture<'a, Result<AgentEventReplyOutcome, PortError>> {
        let admission = admission.clone();
        let session_id = admission.target.session_id.clone();
        Box::pin(async move {
            self.request(PortOperation::ReplyAgentEvent, Some(session_id), |reply| {
                Request::ReplyAgentEvent { admission, reply }
            })
            .await
        })
    }

    fn get_agent_event_reply<'a>(
        &'a self,
        target: &'a AgentChannelTarget,
        event_id: &'a str,
        observed_at: undercurrent_domain::TimestampMillis,
    ) -> PortFuture<'a, Result<Option<AgentEventReplyRecord>, PortError>> {
        let target = target.clone();
        let session_id = target.session_id.clone();
        let event_id = event_id.to_owned();
        Box::pin(async move {
            self.request(
                PortOperation::ReadAgentEventReply,
                Some(session_id),
                |reply| Request::GetAgentEventReply {
                    target,
                    event_id,
                    observed_at,
                    reply,
                },
            )
            .await
        })
    }
}

struct RepositoryWorker {
    requests: Sender<Request>,
    queue_capacity: usize,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for RepositoryWorker {
    fn drop(&mut self) {
        self.requests.close();
        let worker = self
            .worker
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(worker) = worker
            && worker.thread().id() != thread::current().id()
        {
            let _ = worker.join();
        }
    }
}

enum Request {
    ReserveInputDelivery {
        session_id: SessionId,
        delivery_id: undercurrent_domain::DeliveryId,
        digest: undercurrent_domain::PayloadDigest,
        maximum: usize,
        reply: Sender<Result<undercurrent_application::InputDeliveryReservation, DatabaseError>>,
    },
    SettleInputDelivery {
        session_id: SessionId,
        delivery_id: undercurrent_domain::DeliveryId,
        disposition: undercurrent_application::InputDeliveryDisposition,
        reply: Sender<Result<(), DatabaseError>>,
    },
    ResolveAgentChannelTarget {
        session_id: SessionId,
        process_instance_id: ProcessInstanceId,
        observed_at: TimestampMillis,
        reply: Sender<Result<AgentChannelTargetResolution, DatabaseError>>,
    },
    EnqueueAgentEvent {
        admission: AgentEventAdmission,
        reply: Sender<Result<AgentEventAdmissionOutcome, DatabaseError>>,
    },
    PollAgentEvent {
        target: AgentChannelTarget,
        after_sequence: u64,
        observed_at: undercurrent_domain::TimestampMillis,
        reply: Sender<Result<AgentEventPollOutcome, DatabaseError>>,
    },
    ReplyAgentEvent {
        admission: AgentEventReplyAdmission,
        reply: Sender<Result<AgentEventReplyOutcome, DatabaseError>>,
    },
    GetAgentEventReply {
        target: AgentChannelTarget,
        event_id: String,
        observed_at: undercurrent_domain::TimestampMillis,
        reply: Sender<Result<Option<AgentEventReplyRecord>, DatabaseError>>,
    },
    RetireInput {
        retirement: undercurrent_application::InputAttemptRetirement,
        reply: Sender<Result<bool, DatabaseError>>,
    },
    PendingInputRetirementAcks {
        limit: usize,
        reply: Sender<Result<Vec<OperationAttempt>, DatabaseError>>,
    },
    AcknowledgeInputRetirement {
        attempt: OperationAttempt,
        reply: Sender<Result<(), DatabaseError>>,
    },
    AcceptNextAttempt {
        admission: OperationAttemptAdmission,
        reply: Sender<Result<OperationAttemptAcceptance, DatabaseError>>,
    },
    AcceptAttempt {
        attempt: OperationAttempt,
        reply: Sender<Result<OperationAttemptAcceptance, DatabaseError>>,
    },
    SettleAttempt {
        settlement: OperationAttemptSettlement,
        reply: Sender<Result<OperationAttemptSettlementOutcome, DatabaseError>>,
    },
    CompensateAttempt {
        compensation: OperationAttemptCompensation,
        reply: Sender<Result<OperationAttemptCompensationOutcome, DatabaseError>>,
    },
    PrepareAttemptCompensation {
        preparation: OperationAttemptCompensationPreparation,
        reply: Sender<Result<OperationAttemptCompensationPreparationOutcome, DatabaseError>>,
    },
    ReconcileAttempt {
        reconciliation: OperationAttemptReconciliation,
        reply: Sender<Result<OperationAttemptSettlementOutcome, DatabaseError>>,
    },
    LoadAttempt {
        attempt_id: OperationAttemptId,
        reply: Sender<Result<Option<OperationAttempt>, DatabaseError>>,
    },
    PendingAttemptReconciliation {
        scan: SqliteOperationAttemptScan,
        reply: Sender<Result<Vec<OperationAttempt>, DatabaseError>>,
    },
    LoadRepairCandidate {
        attempt_id: OperationAttemptId,
        reply: Sender<Result<Option<OperationAttempt>, DatabaseError>>,
    },
    RepairUnknownAttempt {
        commit: OperationAttemptRepairCommit,
        reply: Sender<Result<OperationAttemptRepairOutcome, DatabaseError>>,
    },
    UnresolvedAttempts {
        scan: SqliteOperationAttemptScan,
        reply: Sender<Result<UnresolvedOperationAttempts, DatabaseError>>,
    },
    GetRuntimeAttemptStatus {
        attempt_id: OperationAttemptId,
        request_digest: undercurrent_domain::PayloadDigest,
        reply: Sender<Result<RuntimeAttemptStatusLookup, DatabaseError>>,
    },
    AdmitRuntimeCancellation {
        command: CancelRuntimeAttemptCommand,
        observed_at: undercurrent_domain::TimestampMillis,
        reply: Sender<Result<RuntimeCancellationAdmission, DatabaseError>>,
    },
    MarkRuntimeCancellationEffectStarted {
        control_id: OperationAttemptId,
        command_digest: undercurrent_domain::PayloadDigest,
        observed_at: undercurrent_domain::TimestampMillis,
        reply: Sender<Result<RuntimeCancellationEffectStart, DatabaseError>>,
    },
    SettleRuntimeCancellation {
        control_id: OperationAttemptId,
        command_digest: undercurrent_domain::PayloadDigest,
        state: RuntimeCancellationState,
        reply: Sender<Result<RuntimeCancellationSettlement, DatabaseError>>,
    },
    GetRuntimeCancellationStatus {
        query: GetRuntimeCancellationStatusQuery,
        reply: Sender<Result<RuntimeCancellationStatusLookup, DatabaseError>>,
    },
    ClaimRuntimeCancellationRecovery {
        session_id: SessionId,
        observed_at: undercurrent_domain::TimestampMillis,
        reply: Sender<Result<Option<RuntimeCancellationRecovery>, DatabaseError>>,
    },
    Insert {
        session: Session,
        event: SessionOutboxEvent,
        reply: Sender<Result<InsertSessionOutcome, DatabaseError>>,
    },
    Load {
        session_id: SessionId,
        reply: Sender<Result<Option<Session>, DatabaseError>>,
    },
    FindByName {
        name: SessionName,
        reply: Sender<Result<Option<Session>, DatabaseError>>,
    },
    Commit {
        session: Session,
        expected_revision: Revision,
        events: Vec<SessionOutboxEvent>,
        reply: Sender<Result<CommitSessionOutcome, DatabaseError>>,
    },
    Delete {
        session_id: SessionId,
        expected_revision: Revision,
        event: SessionOutboxEvent,
        reply: Sender<Result<DeleteSessionOutcome, DatabaseError>>,
    },
    ReadPending {
        limit: usize,
        reply: Sender<Result<Vec<SessionOutboxRecord>, DatabaseError>>,
    },
    Acknowledge {
        through: OutboxSequence,
        reply: Sender<Result<OutboxAcknowledgeOutcome, DatabaseError>>,
    },
    ReadBatch {
        after: Option<SessionId>,
        limit: usize,
        reply: Sender<Result<RecoverySessionBatch, DatabaseError>>,
    },
    Pressure(Sender<Result<SqliteRepositoryPressure, DatabaseError>>),
}

/// Executes admitted commands serially on the only owner of the connection.
fn run_worker(
    mut database: Database,
    requests: Receiver<Request>,
    telemetry: Option<Arc<dyn Telemetry>>,
) {
    while let Ok(request) = requests.recv_blocking() {
        match request {
            Request::ReserveInputDelivery {
                session_id,
                delivery_id,
                digest,
                maximum,
                reply,
            } => {
                reply_result(
                    reply,
                    database.reserve_input_delivery(&session_id, &delivery_id, digest, maximum),
                );
            }
            Request::SettleInputDelivery {
                session_id,
                delivery_id,
                disposition,
                reply,
            } => {
                reply_result(
                    reply,
                    database.settle_input_delivery(&session_id, &delivery_id, disposition),
                );
            }
            Request::ResolveAgentChannelTarget {
                session_id,
                process_instance_id,
                observed_at,
                reply,
            } => reply_result(
                reply,
                database.resolve_agent_channel_target(session_id, process_instance_id, observed_at),
            ),
            Request::EnqueueAgentEvent { admission, reply } => {
                reply_result(reply, database.enqueue_agent_event(admission))
            }
            Request::PollAgentEvent {
                target,
                after_sequence,
                observed_at,
                reply,
            } => reply_result(
                reply,
                database.poll_agent_event(target, after_sequence, observed_at),
            ),
            Request::ReplyAgentEvent { admission, reply } => {
                reply_result(reply, database.reply_agent_event(admission))
            }
            Request::GetAgentEventReply {
                target,
                event_id,
                observed_at,
                reply,
            } => reply_result(
                reply,
                database.get_agent_event_reply(target, event_id, observed_at),
            ),
            Request::RetireInput { retirement, reply } => {
                reply_result(reply, database.retire_input(retirement))
            }
            Request::PendingInputRetirementAcks { limit, reply } => {
                reply_result(reply, database.pending_input_retirement_acks(limit))
            }
            Request::AcknowledgeInputRetirement { attempt, reply } => {
                reply_result(reply, database.acknowledge_input_retirement(attempt))
            }
            Request::AcceptNextAttempt { admission, reply } => {
                reply_result(reply, database.accept_next_attempt(admission))
            }
            Request::AcceptAttempt { attempt, reply } => {
                reply_result(reply, database.accept_attempt(attempt))
            }
            Request::SettleAttempt { settlement, reply } => {
                reply_result(reply, database.settle_attempt(settlement))
            }
            Request::CompensateAttempt {
                compensation,
                reply,
            } => reply_result(reply, database.compensate_attempt(compensation)),
            Request::PrepareAttemptCompensation { preparation, reply } => {
                reply_result(reply, database.prepare_attempt_compensation(preparation))
            }
            Request::ReconcileAttempt {
                reconciliation,
                reply,
            } => reply_result(reply, database.reconcile_attempt(reconciliation)),
            Request::LoadAttempt { attempt_id, reply } => {
                reply_result(reply, database.load_attempt(&attempt_id))
            }
            Request::PendingAttemptReconciliation { scan, reply } => {
                reply_result(reply, database.pending_attempt_reconciliation(&scan))
            }
            Request::LoadRepairCandidate { attempt_id, reply } => {
                reply_result(reply, database.load_attempt(&attempt_id))
            }
            Request::RepairUnknownAttempt { commit, reply } => {
                reply_result(reply, database.repair_unknown_attempt(commit))
            }
            Request::UnresolvedAttempts { scan, reply } => {
                reply_result(reply, database.unresolved_attempts(&scan))
            }
            Request::GetRuntimeAttemptStatus {
                attempt_id,
                request_digest,
                reply,
            } => reply_result(
                reply,
                database.get_runtime_attempt_status(&attempt_id, request_digest),
            ),
            Request::AdmitRuntimeCancellation {
                command,
                observed_at,
                reply,
            } => reply_result(
                reply,
                database.admit_runtime_cancellation(command, observed_at),
            ),
            Request::MarkRuntimeCancellationEffectStarted {
                control_id,
                command_digest,
                observed_at,
                reply,
            } => reply_result(
                reply,
                database.mark_runtime_cancellation_effect_started(
                    control_id,
                    command_digest,
                    observed_at,
                ),
            ),
            Request::SettleRuntimeCancellation {
                control_id,
                command_digest,
                state,
                reply,
            } => reply_result(
                reply,
                database.settle_runtime_cancellation(control_id, command_digest, state),
            ),
            Request::GetRuntimeCancellationStatus { query, reply } => {
                reply_result(reply, database.get_runtime_cancellation_status(query))
            }
            Request::ClaimRuntimeCancellationRecovery {
                session_id,
                observed_at,
                reply,
            } => reply_result(
                reply,
                database.claim_runtime_cancellation_recovery(&session_id, observed_at),
            ),
            Request::Insert {
                session,
                event,
                reply,
            } => reply_observed(
                reply,
                database.insert(session, event),
                telemetry.as_deref(),
                MetricName::PersistenceCheckpointSucceeded,
                MetricName::PersistenceCheckpointFailed,
            ),
            Request::Load { session_id, reply } => reply_result(reply, database.load(&session_id)),
            Request::FindByName { name, reply } => {
                reply_result(reply, database.find_by_name(&name))
            }
            Request::Commit {
                session,
                expected_revision,
                events,
                reply,
            } => reply_observed(
                reply,
                database.commit(session, expected_revision, events),
                telemetry.as_deref(),
                MetricName::PersistenceCheckpointSucceeded,
                MetricName::PersistenceCheckpointFailed,
            ),
            Request::Delete {
                session_id,
                expected_revision,
                event,
                reply,
            } => reply_observed(
                reply,
                database.delete(session_id, expected_revision, event),
                telemetry.as_deref(),
                MetricName::PersistenceCheckpointSucceeded,
                MetricName::PersistenceCheckpointFailed,
            ),
            Request::ReadPending { limit, reply } => {
                reply_result(reply, database.read_pending(limit))
            }
            Request::Acknowledge { through, reply } => reply_observed(
                reply,
                database.acknowledge_through(through),
                telemetry.as_deref(),
                MetricName::PersistenceCheckpointSucceeded,
                MetricName::PersistenceCheckpointFailed,
            ),
            Request::ReadBatch {
                after,
                limit,
                reply,
            } => reply_observed(
                reply,
                database.read_batch(after.as_ref(), limit),
                telemetry.as_deref(),
                MetricName::PersistenceRecoverySucceeded,
                MetricName::PersistenceRecoveryFailed,
            ),
            Request::Pressure(reply) => reply_result(reply, database.pressure()),
        }
    }
}

fn reply_observed<T>(
    reply: Sender<Result<T, DatabaseError>>,
    result: Result<T, DatabaseError>,
    telemetry: Option<&dyn Telemetry>,
    success: MetricName,
    failure: MetricName,
) {
    if let Some(telemetry) = telemetry {
        record_persistence_result(telemetry, result.is_ok(), success, failure);
    }
    reply_result(reply, result);
}

fn record_persistence_result(
    telemetry: &dyn Telemetry,
    succeeded: bool,
    success: MetricName,
    failure: MetricName,
) {
    telemetry.record_metric(MetricObservation {
        name: if succeeded { success } else { failure },
        value: 1.0,
        operation: None,
    });
}

fn reply_result<T>(reply: Sender<Result<T, DatabaseError>>, result: Result<T, DatabaseError>) {
    let _ = reply.send_blocking(result);
}

#[derive(Debug)]
struct WorkerFailure(&'static str);

impl Display for WorkerFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for WorkerFailure {}

/// Applies the durability, lock deadline, and bounded temporary-state policy.
fn configure_connection(
    connection: &Connection,
    busy_timeout: Duration,
) -> Result<(), SqliteRepositoryOpenError> {
    connection
        .busy_timeout(busy_timeout)
        .map_err(SqliteRepositoryOpenError::Configure)?;
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .map_err(SqliteRepositoryOpenError::Configure)?;
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(SqliteRepositoryOpenError::Configure)?;
    connection
        .pragma_update(None, "foreign_keys", true)
        .map_err(SqliteRepositoryOpenError::Configure)?;
    connection
        .pragma_update(None, "temp_store", "MEMORY")
        .map_err(SqliteRepositoryOpenError::Configure)?;
    Ok(())
}

fn preflight_existing_database(path: &Path) -> Result<(), SqliteRepositoryOpenError> {
    if !path.exists() {
        return Ok(());
    }
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(SqliteRepositoryOpenError::Open)?;
    let schema_version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(SqliteRepositoryOpenError::Open)?;
    let application_tables: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
            [],
            |row| row.get(0),
        )
        .map_err(SqliteRepositoryOpenError::Open)?;
    if schema_version == 0 && application_tables == 0 {
        return Ok(());
    }
    validate_supported_release_schema(&connection).map_err(|source| {
        SqliteRepositoryOpenError::Validate(SqliteRepositoryValidationError { source })
    })?;
    Ok(())
}

/// Creates only missing state directories, using owner-only mode on Unix.
fn create_parent_directory(path: &Path) -> Result<(), SqliteRepositoryOpenError> {
    let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    else {
        return Ok(());
    };
    if parent.exists() {
        return Ok(());
    }
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(parent)
        .map_err(SqliteRepositoryOpenError::CreateDirectory)
}

// Keep every synchronous SQLite operation inside one ownership scope on Windows.
// The scope preserves the effective identity and changes no process-wide token.
fn with_file_owner<T>(operation: impl FnOnce() -> T) -> std::io::Result<T> {
    #[cfg(windows)]
    {
        undercurrent_platform_windows::with_current_user_file_owner(operation)
    }
    #[cfg(not(windows))]
    {
        Ok(operation())
    }
}

#[cfg(unix)]
/// Creates or restricts the database file before SQLite can read private state.
fn prepare_database_file(path: &Path) -> Result<(), SqliteRepositoryOpenError> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)
        .map_err(SqliteRepositoryOpenError::Permissions)?;
    let permissions = std::fs::Permissions::from_mode(0o600);
    std::fs::set_permissions(path, permissions).map_err(SqliteRepositoryOpenError::Permissions)
}

#[cfg(not(unix))]
/// Creates the database file before SQLite opens it on non-Unix platforms.
fn prepare_database_file(path: &Path) -> Result<(), SqliteRepositoryOpenError> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map(|_| ())
        .map_err(SqliteRepositoryOpenError::Permissions)
}
