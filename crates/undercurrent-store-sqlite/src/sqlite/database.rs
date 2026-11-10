// SPDX-License-Identifier: Apache-2.0

//! Synchronous SQLite transaction implementation owned by one worker thread.
//!
//! Session writes couple aggregates, outbox snapshots/events, and counters in
//! immediate transactions. Attempts use separate evidence transactions around
//! externally owned effects. The database carries the exact current release
//! identity and complete initial schema; numeric markers describe storage encoding.
//! Only recognized released layouts may migrate, one committed step at a time.

mod agent_channel;
mod input_deliveries;
mod input_retirements;
mod runtime_cancellation;

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use undercurrent_application::{
    CommitSessionOutcome, DeleteSessionOutcome, InsertSessionOutcome, OperationAttemptAcceptance,
    OperationAttemptAdmission, OperationAttemptCompensation, OperationAttemptCompensationOutcome,
    OperationAttemptCompensationPreparation, OperationAttemptCompensationPreparationOutcome,
    OperationAttemptReconciliation, OperationAttemptRepairCommit, OperationAttemptRepairOutcome,
    OperationAttemptSettlement, OperationAttemptSettlementOutcome, OutboxAcknowledgeOutcome,
    OutboxSequence, RecoverySessionBatch, RuntimeAttemptSessionStatus, RuntimeAttemptStatusBinding,
    RuntimeAttemptStatusLookup, RuntimeAttemptStatusResult, RuntimeAttemptStatusSnapshot,
    SessionOutboxEvent, SessionOutboxRecord, UnresolvedOperationAttempts,
};
use undercurrent_domain::{
    AgentRuntimeBinding, DispatchCellIdentity, DomainEventKind, EffectCertainty, FencingToken,
    OperationAttempt, OperationAttemptId, OperationAttemptState, OperationAttemptTransition,
    PayloadDigest, ProcessIdentity, ProcessInstanceId, ProcessMutationKind, Revision,
    RuntimeCallbackRef, RuntimeDispatchBinding, RuntimeDispatchLimits, RuntimeDispatchPolicy,
    RuntimeOutputContract, RuntimeRetentionPolicyRef, RuntimeTelemetryPolicyRef, Session,
    SessionId, SessionName, TimestampMillis,
};
use undercurrent_foundation::Classification;

use super::codec::{
    SESSION_RECORD_VERSION, SessionCodecError, decode_session, encode_session,
    is_supported_session_record_version,
};
use super::{
    SqliteCrashInjector, SqliteCrashPoint, SqliteOperationAttemptScan, SqliteRepositoryLimits,
    SqliteRepositoryPressure,
};

pub(super) const DATABASE_SCHEMA_VERSION: i64 = 1;
const OUTBOX_RECORD_VERSION: i64 = 1;
const ATTEMPT_RECORD_VERSION: i64 = 1;
const RUNTIME_POLICY_RECORD_VERSION: undercurrent_foundation::ReleaseVersion =
    undercurrent_foundation::ReleaseVersion::Current;
const MAX_RUNTIME_POLICY_RECORD_BYTES: usize = 65_536;
const MAX_AGENT_CHANNEL_RECORD_BYTES: usize = 256 * 1024;
const MAX_RELEASE_SCHEMA_OBJECTS: i64 = 128;
const MAX_RELEASE_SCHEMA_SQL_BYTES: i64 = 262_144;
const MAX_RELEASE_SCHEMA_CANONICAL_BYTES: i64 = 1_048_576;
pub(super) const SCHEMA_NAME: &str = "initial-complete-schema";
pub(super) const SCHEMA_CHECKSUM: &str =
    "sha256:3e17e0d2bba3cd723b2b31a87b7ec7e9d8ee178bace3d877ed8527f1ae0a59dd";
const INPUT_DELIVERY_SCHEMA_SQL: &str = "CREATE TABLE input_deliveries (
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    delivery_id TEXT NOT NULL CHECK(length(CAST(delivery_id AS BLOB)) BETWEEN 1 AND 256),
    input_digest BLOB NOT NULL CHECK(length(input_digest) = 32),
    disposition INTEGER NOT NULL CHECK(disposition IN (0, 1, 2)),
    observed_at INTEGER,
    PRIMARY KEY(session_id, delivery_id),
    CHECK((disposition = 0 AND observed_at IS NULL) OR (disposition IN (1, 2) AND observed_at IS NOT NULL))
) STRICT, WITHOUT ROWID";
const EVENT_SNAPSHOT_SCHEMA_SQL: &str = "CREATE TABLE session_event_snapshots (
    sequence BLOB PRIMARY KEY NOT NULL REFERENCES outbox(sequence) ON DELETE CASCADE CHECK(length(sequence) = 8),
    payload BLOB NOT NULL
) STRICT";
const CURRENT_SCHEMA_FINGERPRINT: &str =
    "3e17e0d2bba3cd723b2b31a87b7ec7e9d8ee178bace3d877ed8527f1ae0a59dd";

const RUNTIME_CANCELLATION_SCHEMA_SQL: &str = "
CREATE TABLE runtime_cancellations (
    control_id TEXT PRIMARY KEY NOT NULL CHECK(length(control_id) BETWEEN 1 AND 128),
    command_digest TEXT NOT NULL CHECK(
        length(command_digest) = 64 AND command_digest NOT GLOB '*[^0-9a-f]*'
    ),
    product_request_fingerprint TEXT NOT NULL CHECK(
        length(product_request_fingerprint) = 64 AND
        product_request_fingerprint NOT GLOB '*[^0-9a-f]*'
    ),
    idempotency_key TEXT NOT NULL CHECK(length(idempotency_key) BETWEEN 1 AND 256),
    target_attempt_id TEXT NOT NULL REFERENCES operation_attempts(attempt_id),
    target_request_digest TEXT NOT NULL CHECK(
        length(target_request_digest) = 64 AND
        target_request_digest NOT GLOB '*[^0-9a-f]*'
    ),
    target_fencing_token BLOB NOT NULL CHECK(
        length(target_fencing_token) = 8 AND target_fencing_token != X'0000000000000000'
    ),
    caller_cell_identity TEXT NOT NULL CHECK(length(caller_cell_identity) BETWEEN 1 AND 128),
    session_id TEXT NOT NULL CHECK(length(session_id) BETWEEN 1 AND 256),
    expected_session_revision BLOB NOT NULL CHECK(
        length(expected_session_revision) = 8 AND
        expected_session_revision != X'0000000000000000'
    ),
    expected_lifecycle_revision BLOB NOT NULL CHECK(
        length(expected_lifecycle_revision) = 8 AND
        expected_lifecycle_revision != X'0000000000000000'
    ),
    expected_provider_id TEXT NOT NULL CHECK(
        length(expected_provider_id) = 40 AND
        substr(expected_provider_id, 1, 4) = 'prv_' AND
        substr(expected_provider_id, 5, 8) NOT GLOB '*[^0-9a-f]*' AND
        substr(expected_provider_id, 13, 1) = '-' AND
        substr(expected_provider_id, 14, 4) NOT GLOB '*[^0-9a-f]*' AND
        substr(expected_provider_id, 18, 1) = '-' AND
        substr(expected_provider_id, 19, 1) = '7' AND
        substr(expected_provider_id, 19, 4) NOT GLOB '*[^0-9a-f]*' AND
        substr(expected_provider_id, 23, 1) = '-' AND
        substr(expected_provider_id, 24, 1) GLOB '[89ab]' AND
        substr(expected_provider_id, 24, 4) NOT GLOB '*[^0-9a-f]*' AND
        substr(expected_provider_id, 28, 1) = '-' AND
        substr(expected_provider_id, 29, 12) NOT GLOB '*[^0-9a-f]*'
    ),
    expected_provider_generation BLOB NOT NULL CHECK(
        length(expected_provider_generation) = 8 AND
        expected_provider_generation != X'0000000000000000'
    ),
    mode INTEGER NOT NULL CHECK(mode IN (0, 1)),
    reason TEXT NOT NULL CHECK(length(reason) BETWEEN 1 AND 512),
    propagation_policy INTEGER NOT NULL CHECK(propagation_policy = 0),
    deadline_millis INTEGER NOT NULL CHECK(deadline_millis > 0),
    source_product TEXT NOT NULL CHECK(length(source_product) BETWEEN 3 AND 128),
    tenant_id TEXT NOT NULL CHECK(
        length(tenant_id) = 43 AND
        substr(tenant_id, 1, 7) = 'tenant_' AND
        substr(tenant_id, 8, 8) NOT GLOB '*[^0-9a-f]*' AND
        substr(tenant_id, 16, 1) = '-' AND
        substr(tenant_id, 17, 4) NOT GLOB '*[^0-9a-f]*' AND
        substr(tenant_id, 21, 1) = '-' AND
        substr(tenant_id, 22, 1) = '7' AND
        substr(tenant_id, 22, 4) NOT GLOB '*[^0-9a-f]*' AND
        substr(tenant_id, 26, 1) = '-' AND
        substr(tenant_id, 27, 1) GLOB '[89ab]' AND
        substr(tenant_id, 27, 4) NOT GLOB '*[^0-9a-f]*' AND
        substr(tenant_id, 31, 1) = '-' AND
        substr(tenant_id, 32, 12) NOT GLOB '*[^0-9a-f]*'
    ),
    actor_context_ref TEXT NOT NULL CHECK(length(actor_context_ref) BETWEEN 1 AND 512),
    authority_ref TEXT NOT NULL CHECK(length(authority_ref) BETWEEN 1 AND 512),
    state INTEGER NOT NULL CHECK(state BETWEEN 0 AND 4),
    session_lifecycle INTEGER NOT NULL CHECK(session_lifecycle BETWEEN 0 AND 5),
    session_exit_code INTEGER NULL,
    observed_session_revision BLOB NOT NULL CHECK(
        length(observed_session_revision) = 8 AND
        observed_session_revision != X'0000000000000000'
    ),
    observed_lifecycle_revision BLOB NOT NULL CHECK(
        length(observed_lifecycle_revision) = 8 AND
        observed_lifecycle_revision != X'0000000000000000'
    ),
    observed_at_millis INTEGER NOT NULL,
    admitted_at_millis INTEGER NOT NULL,
    record_version INTEGER NOT NULL CHECK(record_version = 1),
    UNIQUE(source_product, tenant_id, idempotency_key),
    CHECK(control_id != target_attempt_id),
    CHECK(expected_lifecycle_revision <= expected_session_revision),
    CHECK(observed_lifecycle_revision <= observed_session_revision),
    CHECK(
        (session_lifecycle = 4 AND session_exit_code IS NOT NULL) OR
        (session_lifecycle != 4 AND session_exit_code IS NULL)
    ),
    CHECK(
        (state IN (0, 4) AND session_lifecycle = 2) OR
        (state IN (1, 2) AND session_lifecycle IN (3, 4, 5)) OR
        state = 3
    )
) STRICT;
CREATE INDEX runtime_cancellation_target
    ON runtime_cancellations(target_attempt_id, target_request_digest, session_id);
CREATE INDEX runtime_cancellation_pending
    ON runtime_cancellations(session_id, state, admitted_at_millis)
    WHERE state = 0;
";

const AGENT_CHANNEL_SCHEMA_SQL: &str = "
CREATE TABLE agent_runtime_bindings (
    attempt_id TEXT PRIMARY KEY NOT NULL REFERENCES operation_attempts(attempt_id),
    tenant_id TEXT NOT NULL CHECK(length(tenant_id) BETWEEN 1 AND 512),
    source_product TEXT NOT NULL CHECK(length(source_product) BETWEEN 1 AND 512),
    actor_context_ref TEXT NOT NULL CHECK(length(actor_context_ref) BETWEEN 1 AND 512),
    authority_ref TEXT NOT NULL CHECK(length(authority_ref) BETWEEN 1 AND 512),
    undercurrent_provider_id TEXT NOT NULL CHECK(length(undercurrent_provider_id) BETWEEN 1 AND 128),
    undercurrent_provider_generation BLOB NOT NULL CHECK(
        length(undercurrent_provider_generation) = 8 AND
        undercurrent_provider_generation != X'0000000000000000'
    ),
    attempt_revision BLOB NOT NULL CHECK(
        length(attempt_revision) = 8 AND attempt_revision != X'0000000000000000'
    ),
    recovery_epoch BLOB NOT NULL CHECK(
        length(recovery_epoch) = 8 AND recovery_epoch != X'0000000000000000'
    ),
    request_digest TEXT NOT NULL CHECK(
        length(request_digest) = 64 AND request_digest NOT GLOB '*[^0-9a-f]*'
    ),
    expected_result_schema_json BLOB NOT NULL CHECK(
        length(expected_result_schema_json) BETWEEN 2 AND 65536 AND
        json_valid(CAST(expected_result_schema_json AS TEXT))
    ),
    binding_json BLOB NOT NULL CHECK(
        length(binding_json) BETWEEN 2 AND 131072 AND
        json_valid(CAST(binding_json AS TEXT))
    ),
    expires_at INTEGER NOT NULL,
    record_version INTEGER NOT NULL CHECK(record_version = 1)
) STRICT;
CREATE TABLE agent_events (
    event_id TEXT PRIMARY KEY NOT NULL CHECK(length(event_id) BETWEEN 1 AND 256),
    attempt_id TEXT NOT NULL REFERENCES agent_runtime_bindings(attempt_id),
    session_id TEXT NOT NULL CHECK(length(session_id) BETWEEN 1 AND 256),
    process_instance_id TEXT NOT NULL CHECK(length(process_instance_id) BETWEEN 1 AND 128),
    sequence BLOB NOT NULL CHECK(length(sequence) = 8 AND sequence != X'0000000000000000'),
    request_id TEXT NOT NULL CHECK(length(request_id) BETWEEN 1 AND 128),
    call_digest TEXT NOT NULL CHECK(
        length(call_digest) = 64 AND call_digest NOT GLOB '*[^0-9a-f]*'
    ),
    call_json BLOB NOT NULL CHECK(
        length(call_json) BETWEEN 2 AND 262144 AND json_valid(CAST(call_json AS TEXT))
    ),
    received_at INTEGER NOT NULL,
    reply_digest TEXT NULL CHECK(
        reply_digest IS NULL OR (
            length(reply_digest) = 64 AND reply_digest NOT GLOB '*[^0-9a-f]*'
        )
    ),
    reply_json BLOB NULL CHECK(
        reply_json IS NULL OR (
            length(reply_json) BETWEEN 2 AND 262144 AND json_valid(CAST(reply_json AS TEXT))
        )
    ),
    replied_at INTEGER NULL,
    record_version INTEGER NOT NULL CHECK(record_version = 1),
    UNIQUE(attempt_id, sequence),
    UNIQUE(attempt_id, request_id),
    CHECK(
        (reply_digest IS NULL AND reply_json IS NULL AND replied_at IS NULL) OR
        (reply_digest IS NOT NULL AND reply_json IS NOT NULL AND replied_at IS NOT NULL)
    )
) STRICT;
CREATE INDEX agent_events_unsettled
    ON agent_events(attempt_id, sequence) WHERE reply_digest IS NULL;
";

/// Typed internal database failure translated once at the port boundary.
#[derive(Debug)]
pub(super) enum DatabaseError {
    Sqlite(rusqlite::Error),
    Codec(SessionCodecError),
    UnsupportedSchema(i64),
    InvalidData(&'static str),
    Capacity(&'static str),
    Conflict(&'static str),
    NotFound(&'static str),
    InjectedCrash(SqliteCrashPoint),
}

impl DatabaseError {
    fn from_encoding(error: SessionCodecError) -> Self {
        match error {
            SessionCodecError::RecordTooLarge { .. } => Self::Capacity("session record"),
            error => Self::Codec(error),
        }
    }

    pub(super) fn port_kind(&self) -> undercurrent_application::PortErrorKind {
        use undercurrent_application::PortErrorKind;
        match self {
            Self::Codec(_) | Self::UnsupportedSchema(_) | Self::InvalidData(_) => {
                PortErrorKind::InvalidData
            }
            Self::Capacity(_) => PortErrorKind::CapacityExceeded,
            Self::Conflict(_) => PortErrorKind::Conflict,
            Self::NotFound(_) => PortErrorKind::NotFound,
            Self::InjectedCrash(_) => PortErrorKind::Unavailable,
            Self::Sqlite(error) => match error.sqlite_error_code() {
                Some(rusqlite::ErrorCode::DatabaseBusy)
                | Some(rusqlite::ErrorCode::DatabaseLocked) => PortErrorKind::Timeout,
                Some(rusqlite::ErrorCode::CannotOpen)
                | Some(rusqlite::ErrorCode::SystemIoFailure)
                | Some(rusqlite::ErrorCode::ReadOnly) => PortErrorKind::Unavailable,
                Some(rusqlite::ErrorCode::ConstraintViolation)
                | Some(rusqlite::ErrorCode::TypeMismatch)
                | Some(rusqlite::ErrorCode::SchemaChanged) => PortErrorKind::InvalidData,
                _ => PortErrorKind::Other,
            },
        }
    }
}

impl Display for DatabaseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlite(_) => formatter.write_str("SQLite operation failed"),
            Self::Codec(_) => formatter.write_str("durable session record is invalid"),
            Self::UnsupportedSchema(version) => {
                write!(formatter, "SQLite schema version {version} is unsupported")
            }
            Self::InvalidData(rule) => write!(formatter, "SQLite state is invalid: {rule}"),
            Self::Conflict(resource) => write!(formatter, "SQLite {resource} conflict"),
            Self::NotFound(resource) => write!(formatter, "SQLite {resource} was not found"),
            Self::Capacity(resource) => write!(formatter, "SQLite {resource} capacity exhausted"),
            Self::InjectedCrash(point) => write!(formatter, "injected SQLite crash at {point}"),
        }
    }
}

impl Error for DatabaseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Sqlite(error) => Some(error),
            Self::Codec(error) => Some(error),
            Self::UnsupportedSchema(_)
            | Self::InvalidData(_)
            | Self::Capacity(_)
            | Self::Conflict(_)
            | Self::NotFound(_)
            | Self::InjectedCrash(_) => None,
        }
    }
}

impl From<rusqlite::Error> for DatabaseError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

impl From<SessionCodecError> for DatabaseError {
    fn from(error: SessionCodecError) -> Self {
        Self::Codec(error)
    }
}

pub(super) struct Database {
    connection: Connection,
    limits: SqliteRepositoryLimits,
    crash_injector: Arc<dyn SqliteCrashInjector>,
}

impl Database {
    pub(super) fn initialize_with_crash_injector(
        mut connection: Connection,
        limits: SqliteRepositoryLimits,
        crash_injector: Arc<dyn SqliteCrashInjector>,
    ) -> Result<Self, DatabaseError> {
        let schema_version: i64 =
            connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if schema_version == 0 {
            create_schema(&mut connection, crash_injector.as_ref())?;
        } else {
            validate_release_schema(&connection)?;
        }
        validate_schema_identity(&connection)?;
        validate_runtime_policy_consistency(&connection)?;
        validate_request_digest_consistency(&connection)?;
        validate_attempt_repair_consistency(&connection)?;
        validate_input_retirement_consistency(&connection, limits.max_record_bytes)?;
        validate_runtime_cancellation_consistency(&connection)?;
        validate_agent_channel_consistency(&connection)?;
        validate_repository_state(&connection, limits)?;
        Ok(Self {
            connection,
            limits,
            crash_injector,
        })
    }

    /// Inserts one aggregate and its first outbox event in an immediate write
    /// transaction after unique-index and capacity checks.
    pub(super) fn insert(
        &mut self,
        session: Session,
        event: SessionOutboxEvent,
    ) -> Result<InsertSessionOutcome, DatabaseError> {
        validate_insert_event(&session, &event)?;
        let payload = encode_session(&session, self.limits.max_record_bytes)
            .map_err(DatabaseError::from_encoding)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if exists_by_id(&transaction, session.id())? {
            return Ok(InsertSessionOutcome::IdConflict);
        }
        if let Some(existing_session_id) = find_id_by_name(&transaction, session.name())? {
            return Ok(InsertSessionOutcome::NameConflict {
                existing_session_id,
            });
        }
        let state = read_repository_state(&transaction)?;
        if state.session_count >= self.limits.max_sessions {
            return Err(DatabaseError::Capacity("session"));
        }
        if state.outbox_count >= self.limits.max_outbox_events {
            return Err(DatabaseError::Capacity("outbox"));
        }
        let crash_injector = Arc::clone(&self.crash_injector);
        let next_sequence = append_events(&transaction, state.next_sequence, &[event], &payload)?;
        maybe_crash(crash_injector.as_ref(), SqliteCrashPoint::InsertAfterOutbox)?;
        transaction.execute(
            "INSERT INTO sessions \
             (id, name, revision, updated_at, record_version, payload) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                session.id().as_str(),
                session.name().as_str(),
                encode_u64(session.revision().value()),
                session.updated_at().value(),
                i64::from(SESSION_RECORD_VERSION),
                payload,
            ],
        )?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::InsertAfterAggregate,
        )?;
        update_repository_counts(
            &transaction,
            state.session_count.saturating_add(1),
            state.outbox_count.saturating_add(1),
            next_sequence,
            state.last_acknowledged,
        )?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::InsertAfterCounters,
        )?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::InsertBeforeCommit,
        )?;
        transaction.commit()?;
        Ok(InsertSessionOutcome::Inserted)
    }

    /// Loads one indexed aggregate through the bounded validated record codec.
    pub(super) fn load(&self, session_id: &SessionId) -> Result<Option<Session>, DatabaseError> {
        load_session_by_id(&self.connection, session_id, self.limits.max_record_bytes)
    }

    /// Resolves and validates one aggregate through the durable unique name index.
    pub(super) fn find_by_name(
        &self,
        name: &SessionName,
    ) -> Result<Option<Session>, DatabaseError> {
        let record_limit = sql_usize(
            self.limits.max_record_bytes,
            "record byte limit exceeds SQLite range",
        )?;
        let row = self
            .connection
            .query_row(
                "SELECT id, name, revision, record_version, length(payload), \
                 CASE WHEN length(payload) <= ?2 THEN payload ELSE NULL END \
                 FROM sessions WHERE name = ?1",
                params![name.as_str(), record_limit],
                read_session_row,
            )
            .optional()?;
        row.map(|row| decode_session_row(row, self.limits.max_record_bytes))
            .transpose()
    }

    /// Revision-checks and replaces one aggregate with its ordered outbox suffix
    /// under one immediate write transaction.
    pub(super) fn commit(
        &mut self,
        session: Session,
        expected_revision: Revision,
        events: Vec<SessionOutboxEvent>,
    ) -> Result<CommitSessionOutcome, DatabaseError> {
        validate_commit_events(&session, expected_revision, &events)?;
        let payload = encode_session(&session, self.limits.max_record_bytes)
            .map_err(DatabaseError::from_encoding)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some((current_revision, current_name)) =
            find_revision_and_name(&transaction, session.id())?
        else {
            return Ok(CommitSessionOutcome::NotFound);
        };
        if current_revision != expected_revision {
            return Ok(CommitSessionOutcome::RevisionConflict { current_revision });
        }
        if current_name != *session.name()
            && let Some(existing_session_id) = find_id_by_name(&transaction, session.name())?
            && existing_session_id != *session.id()
        {
            return Ok(CommitSessionOutcome::NameConflict {
                existing_session_id,
            });
        }
        let state = read_repository_state(&transaction)?;
        let resulting_outbox = state
            .outbox_count
            .checked_add(events.len())
            .ok_or(DatabaseError::Capacity("outbox count"))?;
        if resulting_outbox > self.limits.max_outbox_events {
            return Err(DatabaseError::Capacity("outbox"));
        }
        let crash_injector = Arc::clone(&self.crash_injector);
        let next_sequence = append_events(&transaction, state.next_sequence, &events, &payload)?;
        maybe_crash(crash_injector.as_ref(), SqliteCrashPoint::CommitAfterOutbox)?;
        let changed = transaction.execute(
            "UPDATE sessions SET name = ?1, revision = ?2, updated_at = ?3, \
             record_version = ?4, payload = ?5 WHERE id = ?6 AND revision = ?7",
            params![
                session.name().as_str(),
                encode_u64(session.revision().value()),
                session.updated_at().value(),
                i64::from(SESSION_RECORD_VERSION),
                payload,
                session.id().as_str(),
                encode_u64(expected_revision.value()),
            ],
        )?;
        if changed != 1 {
            return Err(DatabaseError::InvalidData(
                "revision-checked session update affected an unexpected row count",
            ));
        }
        runtime_cancellation::settle_accepted_for_terminal_session(
            &transaction,
            &session,
            session.updated_at(),
        )?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::CommitAfterAggregate,
        )?;
        update_repository_counts(
            &transaction,
            state.session_count,
            resulting_outbox,
            next_sequence,
            state.last_acknowledged,
        )?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::CommitAfterCounters,
        )?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::CommitBeforeCommit,
        )?;
        transaction.commit()?;
        Ok(CommitSessionOutcome::Committed)
    }

    /// Deletes one validated terminal aggregate and appends its removal event in
    /// the same immediate write transaction.
    pub(super) fn delete(
        &mut self,
        session_id: SessionId,
        expected_revision: Revision,
        event: SessionOutboxEvent,
    ) -> Result<DeleteSessionOutcome, DatabaseError> {
        validate_delete_event(&session_id, expected_revision, &event)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(current) =
            load_session_by_id(&transaction, &session_id, self.limits.max_record_bytes)?
        else {
            return Ok(DeleteSessionOutcome::NotFound);
        };
        let current_revision = current.revision();
        if current_revision != expected_revision {
            return Ok(DeleteSessionOutcome::RevisionConflict { current_revision });
        }
        if !current.lifecycle().is_terminal() {
            return Err(DatabaseError::InvalidData(
                "repository delete requires a terminal durable session",
            ));
        }
        let state = read_repository_state(&transaction)?;
        if state.outbox_count >= self.limits.max_outbox_events {
            return Err(DatabaseError::Capacity("outbox"));
        }
        let crash_injector = Arc::clone(&self.crash_injector);
        let next_sequence = append_events(
            &transaction,
            state.next_sequence,
            &[event],
            &encode_session(&current, self.limits.max_record_bytes)
                .map_err(DatabaseError::from_encoding)?,
        )?;
        maybe_crash(crash_injector.as_ref(), SqliteCrashPoint::DeleteAfterOutbox)?;
        // The product admission scan precedes the session lease. Recheck here
        // so a concurrent unknown settlement cannot erase its recovery context.
        let unresolved: bool = transaction.query_row("SELECT EXISTS(SELECT 1 FROM operation_attempts a
            WHERE a.target_session = ?1 AND a.state IN (0, 3)
              AND NOT EXISTS (SELECT 1 FROM input_attempt_retirements r WHERE r.attempt_id = a.attempt_id))",
            [session_id.as_str()], |row| row.get(0))?;
        if unresolved {
            return Err(DatabaseError::Conflict(
                "session has unresolved operation attempts",
            ));
        }
        let deleted = transaction.execute(
            "DELETE FROM sessions WHERE id = ?1 AND revision = ?2",
            params![session_id.as_str(), encode_u64(expected_revision.value())],
        )?;
        if deleted != 1 {
            return Err(DatabaseError::InvalidData(
                "revision-checked session delete affected an unexpected row count",
            ));
        }
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::DeleteAfterAggregate,
        )?;
        let remaining_sessions =
            state
                .session_count
                .checked_sub(1)
                .ok_or(DatabaseError::InvalidData(
                    "session counter was zero before a durable delete",
                ))?;
        update_repository_counts(
            &transaction,
            remaining_sessions,
            state.outbox_count.saturating_add(1),
            next_sequence,
            state.last_acknowledged,
        )?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::DeleteAfterCounters,
        )?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::DeleteBeforeCommit,
        )?;
        transaction.commit()?;
        Ok(DeleteSessionOutcome::Deleted)
    }

    /// Reads a configured-bounded oldest outbox prefix without acknowledging it.
    pub(super) fn read_pending(
        &self,
        limit: usize,
    ) -> Result<Vec<SessionOutboxRecord>, DatabaseError> {
        validate_batch_limit(limit, self.limits.max_read_batch)?;
        let sql_limit = i64::try_from(limit)
            .map_err(|_| DatabaseError::InvalidData("outbox limit exceeds SQLite range"))?;
        let mut statement = self.connection.prepare(
            "SELECT sequence, session_id, session_revision, event_kind, occurred_at, \
             record_version FROM outbox ORDER BY sequence LIMIT ?1",
        )?;
        let rows = statement.query_map([sql_limit], |row| {
            Ok(EventRow {
                sequence: row.get(0)?,
                session_id: row.get(1)?,
                revision: row.get(2)?,
                kind: row.get(3)?,
                occurred_at: row.get(4)?,
                record_version: row.get(5)?,
            })
        })?;
        rows.map(|row| {
            let mut record = decode_event_row(row?)?;
            let payload: Option<Vec<u8>> = self
                .connection
                .query_row(
                    "SELECT payload FROM session_event_snapshots WHERE sequence = ?1",
                    params![encode_u64(record.sequence.value())],
                    |row| row.get(0),
                )
                .optional()?;
            record.snapshot = payload
                .map(|bytes| {
                    decode_session(&bytes, self.limits.max_record_bytes, SESSION_RECORD_VERSION)
                })
                .transpose()?;
            Ok(record)
        })
        .collect()
    }

    /// Atomically removes a known contiguous outbox prefix and persists its
    /// idempotent acknowledgement checkpoint.
    pub(super) fn acknowledge_through(
        &mut self,
        through: OutboxSequence,
    ) -> Result<OutboxAcknowledgeOutcome, DatabaseError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let state = read_repository_state(&transaction)?;
        if let Some(last_acknowledged) = state.last_acknowledged
            && through <= last_acknowledged
        {
            return Ok(OutboxAcknowledgeOutcome::AlreadyAcknowledged {
                through: last_acknowledged,
            });
        }
        let encoded = encode_u64(through.value());
        let exists = transaction
            .query_row(
                "SELECT 1 FROM outbox WHERE sequence = ?1",
                [&encoded],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !exists {
            return Ok(OutboxAcknowledgeOutcome::UnknownSequence { requested: through });
        }
        let crash_injector = Arc::clone(&self.crash_injector);
        transaction.execute(
            "DELETE FROM session_event_snapshots WHERE sequence <= ?1",
            [&encoded],
        )?;
        let records = transaction.execute("DELETE FROM outbox WHERE sequence <= ?1", [&encoded])?;
        if records == 0 || records > state.outbox_count {
            return Err(DatabaseError::InvalidData(
                "outbox acknowledgement affected an invalid prefix",
            ));
        }
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::AcknowledgeAfterDelete,
        )?;
        update_repository_counts(
            &transaction,
            state.session_count,
            state.outbox_count - records,
            state.next_sequence,
            Some(through),
        )?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::AcknowledgeAfterCounters,
        )?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::AcknowledgeBeforeCommit,
        )?;
        transaction.commit()?;
        Ok(OutboxAcknowledgeOutcome::Acknowledged { records, through })
    }

    /// Reads one stable-ID keyset page, fetching one extra index row only to
    /// determine whether a continuation exists.
    pub(super) fn read_batch(
        &self,
        after: Option<&SessionId>,
        limit: usize,
    ) -> Result<RecoverySessionBatch, DatabaseError> {
        validate_batch_limit(limit, self.limits.max_read_batch)?;
        let fetch = limit
            .checked_add(1)
            .ok_or(DatabaseError::InvalidData("recovery limit overflow"))?;
        let sql_limit = i64::try_from(fetch)
            .map_err(|_| DatabaseError::InvalidData("recovery limit exceeds SQLite range"))?;
        let record_limit = sql_usize(
            self.limits.max_record_bytes,
            "record byte limit exceeds SQLite range",
        )?;
        let mut rows = Vec::with_capacity(fetch);
        if let Some(after) = after {
            let mut statement = self.connection.prepare(
                "SELECT id, name, revision, record_version, length(payload), \
                 CASE WHEN length(payload) <= ?2 THEN payload ELSE NULL END FROM sessions \
                 WHERE id > ?1 ORDER BY id LIMIT ?3",
            )?;
            let mapped = statement.query_map(
                params![after.as_str(), record_limit, sql_limit],
                read_session_row,
            )?;
            for row in mapped {
                rows.push(row?);
            }
        } else {
            let mut statement = self.connection.prepare(
                "SELECT id, name, revision, record_version, length(payload), \
                 CASE WHEN length(payload) <= ?1 THEN payload ELSE NULL END FROM sessions \
                 ORDER BY id LIMIT ?2",
            )?;
            let mapped = statement.query_map(params![record_limit, sql_limit], read_session_row)?;
            for row in mapped {
                rows.push(row?);
            }
        }
        let has_more = rows.len() > limit;
        if has_more {
            rows.pop();
        }
        let sessions = rows
            .into_iter()
            .map(|row| decode_session_row(row, self.limits.max_record_bytes))
            .collect::<Result<Vec<_>, _>>()?;
        let next_after = if has_more {
            sessions.last().map(|session| session.id().clone())
        } else {
            None
        };
        Ok(RecoverySessionBatch {
            sessions,
            next_after,
        })
    }

    /// Reads durable counters without scanning or decoding aggregate payloads.
    pub(super) fn pressure(&self) -> Result<SqliteRepositoryPressure, DatabaseError> {
        let state = read_repository_state(&self.connection)?;
        Ok(SqliteRepositoryPressure {
            sessions: state.session_count,
            outbox_events: state.outbox_count,
            session_limit: self.limits.max_sessions,
            outbox_limit: self.limits.max_outbox_events,
            queued_requests: 0,
            request_queue_capacity: 0,
        })
    }

    pub(super) fn accept_attempt(
        &mut self,
        attempt: OperationAttempt,
    ) -> Result<OperationAttemptAcceptance, DatabaseError> {
        let crash_injector = Arc::clone(&self.crash_injector);
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        // Replay retained semantics before consulting the high-water fence:
        // a newer attempt must not hide this identity's recorded outcome.
        // See [acceptance ordering](../../docs/internals/transactions/attempts.md#acceptance-and-fence-scope).
        if input_retirements::is_retired(&transaction, attempt.id())? {
            return Err(DatabaseError::Conflict(
                "retired input attempt cannot be replayed",
            ));
        }
        if let Some(existing) = load_attempt(&transaction, attempt.id())? {
            if !existing.has_same_semantics(&attempt) {
                return Ok(OperationAttemptAcceptance::PayloadConflict);
            }
            return Ok(match existing.state() {
                OperationAttemptState::Accepted => OperationAttemptAcceptance::Duplicate(existing),
                OperationAttemptState::Settled { .. } => {
                    OperationAttemptAcceptance::AlreadySettled(existing)
                }
                OperationAttemptState::Compensated { .. } => {
                    OperationAttemptAcceptance::AlreadyCompensated(existing)
                }
            });
        }
        if let Some((current_id, current)) = current_fence(
            &transaction,
            attempt.target_session(),
            attempt
                .dispatch_binding()
                .map(RuntimeDispatchBinding::caller_cell_identity),
        )? {
            if attempt.fencing_token() < current {
                return Ok(OperationAttemptAcceptance::StaleFence { current });
            }
            if attempt.fencing_token() == current && current_id != *attempt.id() {
                return Ok(OperationAttemptAcceptance::PayloadConflict);
            }
        }
        insert_attempt(&transaction, &attempt)?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::AttemptAcceptAfterWrite,
        )?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::AttemptAcceptBeforeCommit,
        )?;
        transaction.commit()?;
        Ok(OperationAttemptAcceptance::Accepted(attempt))
    }

    pub(super) fn accept_next_attempt(
        &mut self,
        admission: OperationAttemptAdmission,
    ) -> Result<OperationAttemptAcceptance, DatabaseError> {
        let crash_injector = Arc::clone(&self.crash_injector);
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if input_retirements::is_retired(&transaction, admission.attempt_id())? {
            return Err(DatabaseError::Conflict(
                "retired input attempt cannot be replayed",
            ));
        }
        if let Some(existing) = load_attempt(&transaction, admission.attempt_id())? {
            if !admission.has_same_semantics(&existing) {
                return Ok(OperationAttemptAcceptance::PayloadConflict);
            }
            return Ok(match existing.state() {
                OperationAttemptState::Accepted => OperationAttemptAcceptance::Duplicate(existing),
                OperationAttemptState::Settled { .. } => {
                    OperationAttemptAcceptance::AlreadySettled(existing)
                }
                OperationAttemptState::Compensated { .. } => {
                    OperationAttemptAcceptance::AlreadyCompensated(existing)
                }
            });
        }
        let fencing_token = match current_fence(
            &transaction,
            admission.target_session(),
            admission
                .dispatch_binding()
                .map(RuntimeDispatchBinding::caller_cell_identity),
        )? {
            Some((_, current)) => current
                .next()
                .map_err(|_| DatabaseError::Capacity("operation fencing token"))?,
            None => FencingToken::new(1)
                .map_err(|_| DatabaseError::InvalidData("initial fencing token is invalid"))?,
        };
        let attempt = OperationAttempt::accepted(
            admission.attempt_id().clone(),
            admission.target_session().cloned(),
            admission.kind(),
            fencing_token,
            admission.payload_digest(),
            admission.accepted_at(),
        );
        insert_attempt(&transaction, &attempt)?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::AttemptAcceptAfterWrite,
        )?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::AttemptAcceptBeforeCommit,
        )?;
        transaction.commit()?;
        Ok(OperationAttemptAcceptance::Accepted(attempt))
    }

    pub(super) fn settle_attempt(
        &mut self,
        settlement: OperationAttemptSettlement,
    ) -> Result<OperationAttemptSettlementOutcome, DatabaseError> {
        let crash_injector = Arc::clone(&self.crash_injector);
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if input_retirements::is_retired(&transaction, &settlement.attempt_id)? {
            return Ok(OperationAttemptSettlementOutcome::PayloadConflict);
        }
        let Some(mut attempt) = load_attempt(&transaction, &settlement.attempt_id)? else {
            return Ok(OperationAttemptSettlementOutcome::NotFound);
        };
        if let Some((_, current)) = current_fence(
            &transaction,
            attempt.target_session(),
            attempt
                .dispatch_binding()
                .map(RuntimeDispatchBinding::caller_cell_identity),
        )? && settlement.fencing_token < current
            && !is_shutdown_cancellation(&attempt, settlement.certainty, settlement.result_digest)
            && !completed_stop_before_force(
                &transaction,
                &attempt,
                settlement.certainty,
                settlement.result_digest,
            )?
        {
            return Ok(OperationAttemptSettlementOutcome::StaleFence { current });
        }
        if settlement.fencing_token != attempt.fencing_token() {
            return classify_attempt_fence(&transaction, &attempt, settlement.fencing_token);
        }
        let transition = match attempt.settle(
            settlement.fencing_token,
            settlement.certainty,
            settlement.result_digest,
            settlement.settled_at,
        ) {
            Ok(transition) => transition,
            Err(_) => return Ok(OperationAttemptSettlementOutcome::PayloadConflict),
        };
        if transition == OperationAttemptTransition::Duplicate {
            return Ok(OperationAttemptSettlementOutcome::Duplicate(attempt));
        }
        update_attempt(&transaction, &attempt)?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::AttemptSettleAfterWrite,
        )?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::AttemptSettleBeforeCommit,
        )?;
        transaction.commit()?;
        Ok(OperationAttemptSettlementOutcome::Settled(attempt))
    }

    pub(super) fn compensate_attempt(
        &mut self,
        compensation: OperationAttemptCompensation,
    ) -> Result<OperationAttemptCompensationOutcome, DatabaseError> {
        let crash_injector = Arc::clone(&self.crash_injector);
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(mut attempt) = load_attempt(&transaction, compensation.attempt_id())? else {
            return Ok(OperationAttemptCompensationOutcome::NotFound);
        };
        if compensation.fencing_token() != attempt.fencing_token() {
            return Ok(if compensation.fencing_token() < attempt.fencing_token() {
                OperationAttemptCompensationOutcome::StaleFence {
                    current: attempt.fencing_token(),
                }
            } else {
                OperationAttemptCompensationOutcome::PayloadConflict
            });
        }
        if matches!(attempt.state(), OperationAttemptState::Compensated { .. }) {
            return Ok(
                match attempt.compensate(
                    compensation.fencing_token(),
                    compensation.superseding_fencing_token(),
                    compensation.cleanup_digest(),
                    compensation.compensated_at(),
                ) {
                    Ok(OperationAttemptTransition::Duplicate) => {
                        OperationAttemptCompensationOutcome::Duplicate(attempt)
                    }
                    _ => OperationAttemptCompensationOutcome::PayloadConflict,
                },
            );
        }
        if !matches!(attempt.state(), OperationAttemptState::Accepted) {
            return Ok(OperationAttemptCompensationOutcome::NotAccepted);
        }
        let Some(binding) = attempt.dispatch_binding() else {
            return Ok(OperationAttemptCompensationOutcome::PayloadConflict);
        };
        let Some((_, current)) = current_fence(
            &transaction,
            attempt.target_session(),
            Some(binding.caller_cell_identity()),
        )?
        else {
            return Ok(OperationAttemptCompensationOutcome::PayloadConflict);
        };
        if current < compensation.superseding_fencing_token() {
            return Ok(OperationAttemptCompensationOutcome::PayloadConflict);
        }
        // Preparation pinned the cleanup target under its then-current fence.
        // A later dispatch may advance that fence while the exact cleanup still
        // needs terminal evidence; domain compensation checks the retained intent.
        // See [compensation](../../docs/internals/transactions/attempts.md#superseded-dispatch-compensation).
        if current <= attempt.fencing_token() {
            return Ok(OperationAttemptCompensationOutcome::PayloadConflict);
        }
        let transition = match attempt.compensate(
            compensation.fencing_token(),
            compensation.superseding_fencing_token(),
            compensation.cleanup_digest(),
            compensation.compensated_at(),
        ) {
            Ok(transition) => transition,
            Err(_) => return Ok(OperationAttemptCompensationOutcome::PayloadConflict),
        };
        if transition == OperationAttemptTransition::Duplicate {
            return Ok(OperationAttemptCompensationOutcome::Duplicate(attempt));
        }
        update_attempt(&transaction, &attempt)?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::AttemptCompensateAfterWrite,
        )?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::AttemptCompensateBeforeCommit,
        )?;
        transaction.commit()?;
        Ok(OperationAttemptCompensationOutcome::Compensated(attempt))
    }

    pub(super) fn prepare_attempt_compensation(
        &mut self,
        preparation: OperationAttemptCompensationPreparation,
    ) -> Result<OperationAttemptCompensationPreparationOutcome, DatabaseError> {
        let crash_injector = Arc::clone(&self.crash_injector);
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(mut attempt) = load_attempt(&transaction, preparation.attempt_id())? else {
            return Ok(OperationAttemptCompensationPreparationOutcome::NotFound);
        };
        if preparation.fencing_token() != attempt.fencing_token() {
            return Ok(if preparation.fencing_token() < attempt.fencing_token() {
                OperationAttemptCompensationPreparationOutcome::StaleFence {
                    current: attempt.fencing_token(),
                }
            } else {
                OperationAttemptCompensationPreparationOutcome::PayloadConflict
            });
        }
        if !matches!(attempt.state(), OperationAttemptState::Accepted) {
            return Ok(OperationAttemptCompensationPreparationOutcome::NotAccepted);
        }
        if attempt.compensation_intent().is_some() {
            return Ok(
                match attempt.prepare_compensation(
                    preparation.fencing_token(),
                    preparation.superseding_fencing_token(),
                    preparation.process().clone(),
                    preparation.session_revision(),
                    preparation.cleanup_digest(),
                    preparation.prepared_at(),
                ) {
                    Ok(OperationAttemptTransition::Duplicate) => {
                        OperationAttemptCompensationPreparationOutcome::Duplicate(attempt)
                    }
                    _ => OperationAttemptCompensationPreparationOutcome::PayloadConflict,
                },
            );
        }
        let Some(binding) = attempt.dispatch_binding() else {
            return Ok(OperationAttemptCompensationPreparationOutcome::PayloadConflict);
        };
        let Some((_, current)) = current_fence(
            &transaction,
            attempt.target_session(),
            Some(binding.caller_cell_identity()),
        )?
        else {
            return Ok(OperationAttemptCompensationPreparationOutcome::PayloadConflict);
        };
        if preparation.superseding_fencing_token() != current {
            return Ok(if preparation.superseding_fencing_token() < current {
                OperationAttemptCompensationPreparationOutcome::StaleFence { current }
            } else {
                OperationAttemptCompensationPreparationOutcome::PayloadConflict
            });
        }
        if current <= attempt.fencing_token() {
            return Ok(OperationAttemptCompensationPreparationOutcome::PayloadConflict);
        }
        let transition = match attempt.prepare_compensation(
            preparation.fencing_token(),
            preparation.superseding_fencing_token(),
            preparation.process().clone(),
            preparation.session_revision(),
            preparation.cleanup_digest(),
            preparation.prepared_at(),
        ) {
            Ok(transition) => transition,
            Err(_) => {
                return Ok(OperationAttemptCompensationPreparationOutcome::PayloadConflict);
            }
        };
        if transition == OperationAttemptTransition::Duplicate {
            return Ok(OperationAttemptCompensationPreparationOutcome::Duplicate(
                attempt,
            ));
        }
        update_attempt(&transaction, &attempt)?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::AttemptCompensationPrepareAfterWrite,
        )?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::AttemptCompensationPrepareBeforeCommit,
        )?;
        transaction.commit()?;
        Ok(OperationAttemptCompensationPreparationOutcome::Prepared(
            attempt,
        ))
    }

    pub(super) fn reconcile_attempt(
        &mut self,
        reconciliation: OperationAttemptReconciliation,
    ) -> Result<OperationAttemptSettlementOutcome, DatabaseError> {
        let crash_injector = Arc::clone(&self.crash_injector);
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if input_retirements::is_retired(&transaction, &reconciliation.attempt_id)? {
            return Ok(OperationAttemptSettlementOutcome::PayloadConflict);
        }
        let Some(mut attempt) = load_attempt(&transaction, &reconciliation.attempt_id)? else {
            return Ok(OperationAttemptSettlementOutcome::NotFound);
        };
        if let Some((_, current)) = current_fence(
            &transaction,
            attempt.target_session(),
            attempt
                .dispatch_binding()
                .map(RuntimeDispatchBinding::caller_cell_identity),
        )? && reconciliation.fencing_token < current
            && !is_shutdown_cancellation(
                &attempt,
                reconciliation.certainty,
                reconciliation.result_digest,
            )
            && !completed_stop_before_force(
                &transaction,
                &attempt,
                reconciliation.certainty,
                reconciliation.result_digest,
            )?
        {
            return Ok(OperationAttemptSettlementOutcome::StaleFence { current });
        }
        if reconciliation.fencing_token != attempt.fencing_token() {
            return classify_attempt_fence(&transaction, &attempt, reconciliation.fencing_token);
        }
        let transition = match attempt.reconcile(
            reconciliation.certainty,
            reconciliation.result_digest,
            reconciliation.reconciled_at,
        ) {
            Ok(transition) => transition,
            Err(_) => return Ok(OperationAttemptSettlementOutcome::PayloadConflict),
        };
        if transition == OperationAttemptTransition::Duplicate {
            return Ok(OperationAttemptSettlementOutcome::Duplicate(attempt));
        }
        update_attempt(&transaction, &attempt)?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::AttemptReconcileAfterWrite,
        )?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::AttemptReconcileBeforeCommit,
        )?;
        transaction.commit()?;
        Ok(OperationAttemptSettlementOutcome::Settled(attempt))
    }

    pub(super) fn load_attempt(
        &self,
        attempt_id: &OperationAttemptId,
    ) -> Result<Option<OperationAttempt>, DatabaseError> {
        load_attempt(&self.connection, attempt_id)
    }

    /// Reads one provenance-gated attempt and coherent target session in a
    /// single deferred transaction. No write statement is issued.
    pub(super) fn get_runtime_attempt_status(
        &mut self,
        attempt_id: &OperationAttemptId,
        request_digest: PayloadDigest,
    ) -> Result<RuntimeAttemptStatusLookup, DatabaseError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)?;
        let Some(attempt) = load_attempt(&transaction, attempt_id)? else {
            return Ok(RuntimeAttemptStatusLookup::NotFound);
        };
        if attempt.kind() != ProcessMutationKind::Spawn {
            return Ok(RuntimeAttemptStatusLookup::InconsistentState);
        }
        let Some(dispatch) = attempt.dispatch_binding() else {
            return Ok(RuntimeAttemptStatusLookup::UnverifiableProvenance);
        };
        let Some(retained_request_digest) = dispatch.request_digest() else {
            return Ok(RuntimeAttemptStatusLookup::UnverifiableProvenance);
        };
        if dispatch.runtime_policy().is_none() || retained_request_digest != request_digest {
            return Ok(RuntimeAttemptStatusLookup::UnverifiableProvenance);
        }

        // Provenance is exact before the session table is touched.
        let Some(target_session) = attempt.target_session() else {
            return Ok(RuntimeAttemptStatusLookup::InconsistentState);
        };
        let Some(session) =
            load_session_by_id(&transaction, target_session, self.limits.max_record_bytes)?
        else {
            return Ok(RuntimeAttemptStatusLookup::InconsistentState);
        };
        if session.id() != target_session {
            return Ok(RuntimeAttemptStatusLookup::InconsistentState);
        }
        let session_status = RuntimeAttemptSessionStatus::new(
            session.id().clone(),
            session.lifecycle(),
            session.revision(),
            session.lifecycle_revision(),
            session.updated_at(),
        );
        let snapshot = match attempt.state() {
            OperationAttemptState::Accepted => RuntimeAttemptStatusSnapshot::Unknown {
                observed_at: attempt.accepted_at(),
            },
            OperationAttemptState::Settled {
                certainty: EffectCertainty::Unknown,
                settled_at,
                ..
            } => RuntimeAttemptStatusSnapshot::Unknown {
                observed_at: *settled_at,
            },
            OperationAttemptState::Settled {
                certainty: EffectCertainty::KnownAbsent,
                settled_at,
                ..
            } => RuntimeAttemptStatusSnapshot::KnownAbsent {
                observed_at: *settled_at,
            },
            OperationAttemptState::Settled {
                certainty: EffectCertainty::KnownApplied,
                result_digest: Some(result_digest),
                settled_at,
            } => RuntimeAttemptStatusSnapshot::KnownApplied {
                result_digest: *result_digest,
                observed_at: *settled_at,
                session: session_status,
            },
            OperationAttemptState::Settled {
                certainty: EffectCertainty::KnownApplied,
                result_digest: None,
                ..
            } => return Ok(RuntimeAttemptStatusLookup::InconsistentState),
            OperationAttemptState::Compensated { compensated_at, .. } => {
                RuntimeAttemptStatusSnapshot::Compensated {
                    observed_at: *compensated_at,
                }
            }
        };
        let binding = RuntimeAttemptStatusBinding::new(
            dispatch.caller_cell_identity().clone(),
            retained_request_digest,
            dispatch.launch_profile_digest(),
            dispatch.environment_binding_digest(),
            dispatch.input_digest(),
        );
        let result = RuntimeAttemptStatusResult::new(
            attempt.id().clone(),
            attempt.fencing_token(),
            binding,
            snapshot,
        );
        transaction.commit()?;
        Ok(RuntimeAttemptStatusLookup::Found(Box::new(result)))
    }

    pub(super) fn pending_attempt_reconciliation(
        &mut self,
        scan: &SqliteOperationAttemptScan,
    ) -> Result<Vec<OperationAttempt>, DatabaseError> {
        validate_attempt_scan(scan, self.limits.max_read_batch)?;
        let limit = sqlite_attempt_scan_limit(scan)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)?;
        install_attempt_scan_exclusions(&transaction, scan)?;
        let rows = {
            let mut statement = transaction.prepare(
                "SELECT attempt_id, target_session, mutation_kind, fencing_token, payload_digest,
                    dispatch_cell_identity, launch_profile_digest, environment_binding_digest,
                    input_digest, request_digest, runtime_policy_json, runtime_policy_state,
                    accepted_at, state,
                    result_digest, settled_at,
                    superseding_fencing_token, cleanup_digest, compensation_process_id,
                    compensation_process_started_at, compensation_session_revision,
                    compensation_prepared_at, record_version
             FROM operation_attempts
             WHERE state IN (0, 3)
               AND NOT EXISTS (SELECT 1 FROM input_attempt_retirements retired WHERE retired.attempt_id = operation_attempts.attempt_id)
               AND (?2 OR target_session IS NULL OR target_session = ?3)
               AND (NOT ?4 OR mutation_kind != 'write_input')
               AND (?5 IS NULL OR (accepted_at, attempt_id) > (?5, ?6))
               AND NOT EXISTS (
                    SELECT 1 FROM temp.operation_attempt_scan_exclusions excluded
                    WHERE excluded.attempt_id = operation_attempts.attempt_id
                      AND excluded.fencing_token = operation_attempts.fencing_token
               )
             ORDER BY accepted_at, attempt_id
             LIMIT ?1",
            )?;
            statement
                .query_map(
                    params![
                        limit,
                        scan.scope_parameters().0,
                        scan.scope_parameters().1,
                        scan.skip_input,
                        scan.after.as_ref().map(|c| c.accepted_at.value()),
                        scan.after.as_ref().map(|c| c.attempt_id.as_str())
                    ],
                    read_attempt_row,
                )?
                .collect::<Result<Vec<_>, _>>()?
        };
        let attempts = rows
            .into_iter()
            .map(|row| {
                let mut attempt = decode_attempt_row(row)?;
                if let Some(agent) =
                    load_agent_runtime_binding(&transaction, attempt.id(), attempt.fencing_token())?
                {
                    attempt.restore_agent_binding(agent).map_err(|_| {
                        DatabaseError::InvalidData("agent binding conflicts with its attempt")
                    })?;
                }
                Ok(attempt)
            })
            .collect::<Result<Vec<_>, DatabaseError>>()?;
        transaction.commit()?;
        Ok(attempts)
    }

    pub(super) fn repair_unknown_attempt(
        &mut self,
        commit: OperationAttemptRepairCommit,
    ) -> Result<OperationAttemptRepairOutcome, DatabaseError> {
        let crash_injector = Arc::clone(&self.crash_injector);
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if input_retirements::is_retired(&transaction, commit.attempt_id())? {
            return Ok(OperationAttemptRepairOutcome::NotUnknown);
        }
        let Some(mut attempt) = load_attempt(&transaction, commit.attempt_id())? else {
            return Ok(OperationAttemptRepairOutcome::NotFound);
        };
        if let Some((_, current)) = current_fence(
            &transaction,
            attempt.target_session(),
            attempt
                .dispatch_binding()
                .map(RuntimeDispatchBinding::caller_cell_identity),
        )? && commit.fencing_token() < current
            && !is_shutdown_cancellation(
                &attempt,
                commit.certainty(),
                commit.result_digest().copied(),
            )
        {
            return Ok(OperationAttemptRepairOutcome::StaleFence { current });
        }
        if commit.fencing_token() != attempt.fencing_token() {
            return Ok(if commit.fencing_token() < attempt.fencing_token() {
                OperationAttemptRepairOutcome::StaleFence {
                    current: attempt.fencing_token(),
                }
            } else {
                OperationAttemptRepairOutcome::PayloadConflict
            });
        }
        if let Some(audit) = load_attempt_repair(&transaction, commit.attempt_id())? {
            return Ok(classify_repair_duplicate(&audit, &commit));
        }
        if !matches!(
            attempt.state(),
            OperationAttemptState::Settled {
                certainty: EffectCertainty::Unknown,
                ..
            }
        ) {
            return Ok(OperationAttemptRepairOutcome::NotUnknown);
        }
        attempt
            .reconcile(
                commit.certainty(),
                commit.result_digest().copied(),
                commit.repaired_at(),
            )
            .map_err(|_| DatabaseError::InvalidData("attempt repair transition is invalid"))?;
        update_attempt(&transaction, &attempt)?;
        insert_attempt_repair(&transaction, &commit)?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::AttemptRepairAfterWrite,
        )?;
        maybe_crash(
            crash_injector.as_ref(),
            SqliteCrashPoint::AttemptRepairBeforeCommit,
        )?;
        transaction.commit()?;
        Ok(OperationAttemptRepairOutcome::Repaired)
    }

    pub(super) fn unresolved_attempts(
        &mut self,
        scan: &SqliteOperationAttemptScan,
    ) -> Result<UnresolvedOperationAttempts, DatabaseError> {
        validate_attempt_scan(scan, self.limits.max_read_batch)?;
        let sql_limit = sqlite_attempt_scan_limit(scan)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)?;
        install_attempt_scan_exclusions(&transaction, scan)?;
        let total: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM operation_attempts
             WHERE state IN (0, 3)
               AND NOT EXISTS (SELECT 1 FROM input_attempt_retirements retired WHERE retired.attempt_id = operation_attempts.attempt_id)
               AND (?2 OR target_session IS NULL OR target_session = ?3)
               AND (NOT ?4 OR mutation_kind != 'write_input')
               AND (?5 IS NULL OR (accepted_at, attempt_id) > (?5, ?6))
               AND NOT EXISTS (
                    SELECT 1 FROM temp.operation_attempt_scan_exclusions excluded
                    WHERE excluded.attempt_id = operation_attempts.attempt_id
                      AND excluded.fencing_token = operation_attempts.fencing_token
               )",
            params![sql_limit, scan.scope_parameters().0, scan.scope_parameters().1, scan.skip_input, scan.after.as_ref().map(|c| c.accepted_at.value()), scan.after.as_ref().map(|c| c.attempt_id.as_str())],
            |row| row.get(0),
        )?;
        let total = usize::try_from(total)
            .map_err(|_| DatabaseError::InvalidData("unresolved attempt count is invalid"))?;
        let ids = {
            let mut statement = transaction.prepare(
                    "SELECT attempt_id FROM operation_attempts
                 WHERE state IN (0, 3)
               AND NOT EXISTS (SELECT 1 FROM input_attempt_retirements retired WHERE retired.attempt_id = operation_attempts.attempt_id)
               AND (?2 OR target_session IS NULL OR target_session = ?3)
               AND (NOT ?4 OR mutation_kind != 'write_input')
               AND (?5 IS NULL OR (accepted_at, attempt_id) > (?5, ?6))
                   AND NOT EXISTS (
                        SELECT 1 FROM temp.operation_attempt_scan_exclusions excluded
                        WHERE excluded.attempt_id = operation_attempts.attempt_id
                          AND excluded.fencing_token = operation_attempts.fencing_token
                   )
                 ORDER BY attempt_id LIMIT ?1",
                )?;
            let rows = statement.query_map(
                params![
                    sql_limit,
                    scan.scope_parameters().0,
                    scan.scope_parameters().1,
                    scan.skip_input,
                    scan.after.as_ref().map(|c| c.accepted_at.value()),
                    scan.after.as_ref().map(|c| c.attempt_id.as_str())
                ],
                |row| row.get::<_, String>(0),
            )?;
            let mut ids = Vec::new();
            for row in rows {
                ids.push(
                    OperationAttemptId::new(row?).map_err(|_| {
                        DatabaseError::InvalidData("unresolved attempt ID is invalid")
                    })?,
                );
            }
            ids
        };
        transaction.commit()?;
        UnresolvedOperationAttempts::new(total, ids)
            .map_err(|_| DatabaseError::InvalidData("unresolved attempt page is invalid"))
    }
}

pub(super) fn validate_runtime_cancellation_consistency(
    connection: &Connection,
) -> Result<(), DatabaseError> {
    runtime_cancellation::validate_runtime_cancellation_consistency(connection)
}

pub(super) fn validate_agent_channel_consistency(
    connection: &Connection,
) -> Result<(), DatabaseError> {
    agent_channel::validate_agent_channel_consistency(connection)
}

fn validate_attempt_scan(
    scan: &SqliteOperationAttemptScan,
    maximum: usize,
) -> Result<(), DatabaseError> {
    if scan.limit() == 0 {
        return Err(DatabaseError::InvalidData(
            "operation attempt scan limit is zero",
        ));
    }
    if scan.limit() > maximum || scan.exclusions().len() > maximum {
        return Err(DatabaseError::Capacity("operation attempt scan"));
    }
    if scan.exclusions().len() > scan.limit() {
        return Err(DatabaseError::InvalidData(
            "operation attempt scan exclusions exceed its limit",
        ));
    }
    Ok(())
}

fn sqlite_attempt_scan_limit(scan: &SqliteOperationAttemptScan) -> Result<i64, DatabaseError> {
    i64::try_from(scan.limit()).map_err(|_| DatabaseError::Capacity("operation attempt scan limit"))
}

// Both scans filter these exact identity/fence pairs before LIMIT. Filtering a
// fetched page in Rust could let a full live prefix hide unresolved work beyond
// it. Reset the connection-local table so exclusions cannot leak into later scans.
// See [scan contract](../../docs/internals/transactions/attempts.md#choose-the-scan).
fn install_attempt_scan_exclusions(
    transaction: &Transaction<'_>,
    scan: &SqliteOperationAttemptScan,
) -> Result<(), DatabaseError> {
    transaction.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS operation_attempt_scan_exclusions (
            attempt_id TEXT NOT NULL,
            fencing_token BLOB NOT NULL,
            PRIMARY KEY (attempt_id, fencing_token)
         ) WITHOUT ROWID;
         DELETE FROM temp.operation_attempt_scan_exclusions;",
    )?;
    let mut statement = transaction.prepare(
        "INSERT INTO temp.operation_attempt_scan_exclusions (attempt_id, fencing_token)
         VALUES (?1, ?2)",
    )?;
    for (attempt_id, fencing_token) in scan.exclusions() {
        statement.execute(params![
            attempt_id.as_str(),
            encode_u64(fencing_token.value())
        ])?;
    }
    Ok(())
}

/// Creates the complete current schema and its release identity atomically.
fn create_schema(
    connection: &mut Connection,
    crash_injector: &dyn SqliteCrashInjector,
) -> Result<(), DatabaseError> {
    let transaction = connection.transaction()?;
    transaction.execute_batch(
        "CREATE TABLE sessions (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL UNIQUE,
            revision BLOB NOT NULL CHECK(length(revision) = 8),
            updated_at INTEGER NOT NULL,
            record_version INTEGER NOT NULL,
            payload BLOB NOT NULL
        ) STRICT;
        CREATE TABLE outbox (
            sequence BLOB PRIMARY KEY NOT NULL CHECK(length(sequence) = 8),
            session_id TEXT NOT NULL,
            session_revision BLOB NOT NULL CHECK(length(session_revision) = 8),
            event_kind INTEGER NOT NULL,
            occurred_at INTEGER NOT NULL,
            record_version INTEGER NOT NULL
        ) STRICT;
        CREATE INDEX outbox_session_sequence ON outbox(session_id, sequence);
        CREATE TABLE repository_state (
            singleton INTEGER PRIMARY KEY NOT NULL CHECK(singleton = 1),
            session_count INTEGER NOT NULL CHECK(session_count >= 0),
            outbox_count INTEGER NOT NULL CHECK(outbox_count >= 0),
            next_outbox_sequence BLOB NOT NULL CHECK(length(next_outbox_sequence) = 8),
            last_acknowledged BLOB NULL CHECK(last_acknowledged IS NULL OR length(last_acknowledged) = 8)
        ) STRICT;
        CREATE TABLE schema_identity (
            version INTEGER PRIMARY KEY NOT NULL CHECK(version > 0),
            release_version TEXT NOT NULL,
            name TEXT NOT NULL,
            checksum TEXT NOT NULL,
            applied_at_unix_seconds INTEGER NOT NULL
        ) STRICT;
        CREATE TABLE operation_attempts (
            attempt_id TEXT PRIMARY KEY NOT NULL,
            target_session TEXT NULL,
            mutation_kind TEXT NOT NULL,
            fencing_token BLOB NOT NULL CHECK(length(fencing_token) = 8),
            payload_digest TEXT NOT NULL CHECK(length(payload_digest) = 64),
            dispatch_cell_identity TEXT NULL CHECK(dispatch_cell_identity IS NULL OR (
                length(dispatch_cell_identity) BETWEEN 1 AND 128 AND
                dispatch_cell_identity NOT GLOB '*[^A-Za-z0-9._:-]*'
            )),
            launch_profile_digest TEXT NULL CHECK(launch_profile_digest IS NULL OR length(launch_profile_digest) = 64),
            environment_binding_digest TEXT NULL CHECK(environment_binding_digest IS NULL OR length(environment_binding_digest) = 64),
            input_digest TEXT NULL CHECK(input_digest IS NULL OR length(input_digest) = 64),
            request_digest TEXT NULL CHECK(request_digest IS NULL OR (
                length(request_digest) = 64 AND request_digest NOT GLOB '*[^0-9a-f]*'
            )),
            runtime_policy_json TEXT NULL CHECK(runtime_policy_json IS NULL OR (
                length(runtime_policy_json) BETWEEN 2 AND 65536 AND
                json_valid(runtime_policy_json) AND dispatch_cell_identity IS NOT NULL
            )),
            runtime_policy_state INTEGER NOT NULL DEFAULT 0 CHECK(
                (runtime_policy_state = 0 AND runtime_policy_json IS NULL) OR
                (runtime_policy_state = 1 AND runtime_policy_json IS NOT NULL AND
                 dispatch_cell_identity IS NOT NULL)
            ),
            accepted_at INTEGER NOT NULL,
            state INTEGER NOT NULL CHECK(state BETWEEN 0 AND 4),
            result_digest TEXT NULL CHECK(result_digest IS NULL OR length(result_digest) = 64),
            settled_at INTEGER NULL,
            superseding_fencing_token BLOB NULL CHECK(
                superseding_fencing_token IS NULL OR length(superseding_fencing_token) = 8
            ),
            cleanup_digest TEXT NULL CHECK(cleanup_digest IS NULL OR length(cleanup_digest) = 64),
            compensation_process_id TEXT NULL CHECK(
                compensation_process_id IS NULL OR length(compensation_process_id) BETWEEN 1 AND 128
            ),
            compensation_process_started_at INTEGER NULL,
            compensation_session_revision BLOB NULL CHECK(
                compensation_session_revision IS NULL OR length(compensation_session_revision) = 8
            ),
            compensation_prepared_at INTEGER NULL,
            record_version INTEGER NOT NULL,
            CHECK(
                (state = 0 AND result_digest IS NULL AND settled_at IS NULL) OR
                (state = 1 AND result_digest IS NULL AND settled_at IS NOT NULL AND
                 superseding_fencing_token IS NULL AND cleanup_digest IS NULL) OR
                (state = 2 AND result_digest IS NOT NULL AND settled_at IS NOT NULL AND
                 superseding_fencing_token IS NULL AND cleanup_digest IS NULL) OR
                (state = 3 AND result_digest IS NULL AND settled_at IS NOT NULL AND
                 superseding_fencing_token IS NULL AND cleanup_digest IS NULL) OR
                (state = 4 AND result_digest IS NULL AND settled_at IS NOT NULL AND
                 superseding_fencing_token IS NOT NULL AND cleanup_digest IS NOT NULL AND
                 superseding_fencing_token > fencing_token)
            ),
            CHECK(
                (superseding_fencing_token IS NULL AND cleanup_digest IS NULL AND
                 compensation_process_id IS NULL AND compensation_process_started_at IS NULL AND
                 compensation_session_revision IS NULL AND compensation_prepared_at IS NULL) OR
                (superseding_fencing_token IS NOT NULL AND cleanup_digest IS NOT NULL AND
                 compensation_process_id IS NOT NULL AND compensation_process_started_at IS NOT NULL AND
                 compensation_session_revision IS NOT NULL AND compensation_prepared_at IS NOT NULL AND
                 state IN (0, 4) AND superseding_fencing_token > fencing_token)
            ),
            CHECK(
                (dispatch_cell_identity IS NULL AND launch_profile_digest IS NULL AND
                 environment_binding_digest IS NULL AND input_digest IS NULL AND
                 request_digest IS NULL) OR
                (dispatch_cell_identity IS NOT NULL AND launch_profile_digest IS NOT NULL AND
                 environment_binding_digest IS NOT NULL AND input_digest IS NOT NULL)
            ),
            CHECK(superseding_fencing_token IS NULL OR dispatch_cell_identity IS NOT NULL),
            CHECK(state != 4 OR superseding_fencing_token IS NOT NULL)
        ) STRICT;
        CREATE TABLE operation_attempt_repairs (
            attempt_id TEXT PRIMARY KEY NOT NULL REFERENCES operation_attempts(attempt_id),
            fencing_token BLOB NOT NULL CHECK(length(fencing_token) = 8),
            certainty INTEGER NOT NULL CHECK(certainty IN (1, 2)),
            result_digest TEXT NULL CHECK(result_digest IS NULL OR length(result_digest) = 64),
            evidence_reference TEXT NOT NULL CHECK(length(evidence_reference) BETWEEN 1 AND 256),
            actor_principal TEXT NOT NULL CHECK(length(actor_principal) BETWEEN 1 AND 128),
            actor_tenant TEXT NULL CHECK(actor_tenant IS NULL OR length(actor_tenant) BETWEEN 1 AND 128),
            actor_authority TEXT NOT NULL CHECK(length(actor_authority) BETWEEN 1 AND 128),
            committed_at INTEGER NOT NULL,
            record_version INTEGER NOT NULL CHECK(record_version = 1),
            CHECK((certainty = 1 AND result_digest IS NULL) OR
                  (certainty = 2 AND result_digest IS NOT NULL))
        ) STRICT;
",
    )?;
    transaction.execute_batch(RUNTIME_CANCELLATION_SCHEMA_SQL)?;
    transaction.execute_batch(AGENT_CHANNEL_SCHEMA_SQL)?;
    transaction.execute_batch(EVENT_SNAPSHOT_SCHEMA_SQL)?;
    transaction.execute_batch(INPUT_DELIVERY_SCHEMA_SQL)?;
    transaction.execute_batch(input_retirements::SCHEMA)?;
    transaction.execute_batch(
        "CREATE INDEX operation_attempt_target_fence
            ON operation_attempts(target_session, fencing_token);
         CREATE INDEX operation_attempt_dispatch_fence
            ON operation_attempts(dispatch_cell_identity, fencing_token)
            WHERE dispatch_cell_identity IS NOT NULL;
         CREATE INDEX operation_attempt_pending
            ON operation_attempts(state, accepted_at, attempt_id)
            WHERE state IN (0, 3);",
    )?;
    transaction.execute(
        "INSERT INTO repository_state \
         (singleton, session_count, outbox_count, next_outbox_sequence, last_acknowledged) \
         VALUES (1, 0, 0, ?1, NULL)",
        [encode_u64(OutboxSequence::FIRST.value())],
    )?;
    transaction.execute(
        "INSERT INTO schema_identity (version, release_version, name, checksum, applied_at_unix_seconds)
         VALUES (1, ?1, ?2, ?3, CAST(strftime('%s', 'now') AS INTEGER))",
        params![undercurrent_foundation::RELEASE_VERSION, SCHEMA_NAME, SCHEMA_CHECKSUM],
    )?;
    maybe_crash(crash_injector, SqliteCrashPoint::SchemaCreationAfterSchema)?;
    transaction.pragma_update(None, "user_version", DATABASE_SCHEMA_VERSION)?;
    maybe_crash(crash_injector, SqliteCrashPoint::SchemaCreationBeforeCommit)?;
    transaction.commit()?;
    Ok(())
}

/// Requires the exact current released layout without changing the connection.
pub(super) fn validate_release_schema(connection: &Connection) -> Result<(), DatabaseError> {
    validate_supported_release_schema(connection)
}

/// Verifies only the exact current release layout.
///
/// The release identity and bounded schema fingerprint must both match.
/// Sharing `user_version` with a released database is insufficient evidence.
pub(super) fn validate_supported_release_schema(
    connection: &Connection,
) -> Result<(), DatabaseError> {
    let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version != DATABASE_SCHEMA_VERSION {
        return Err(DatabaseError::UnsupportedSchema(version));
    }
    validate_schema_identity(connection)?;
    if schema_fingerprint(connection)? != CURRENT_SCHEMA_FINGERPRINT {
        return Err(DatabaseError::InvalidData(
            "database layout does not match the current release",
        ));
    }
    Ok(())
}

fn schema_fingerprint(connection: &Connection) -> Result<String, DatabaseError> {
    let object_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master
         WHERE substr(name, 1, 7) != 'sqlite_'",
        [],
        |row| row.get(0),
    )?;
    if !(0..=MAX_RELEASE_SCHEMA_OBJECTS).contains(&object_count) {
        return Err(DatabaseError::InvalidData(
            "schema evidence exceeds the bounded release layout",
        ));
    }
    let (maximum_sql_bytes, component_bytes): (i64, i64) = connection.query_row(
        "SELECT COALESCE(MAX(length(CAST(COALESCE(sql, '') AS BLOB))), 0),
                COALESCE(SUM(
                    length(CAST(type AS BLOB)) +
                    length(CAST(name AS BLOB)) +
                    length(CAST(tbl_name AS BLOB)) +
                    length(CAST(COALESCE(sql, '') AS BLOB))
                ), 0)
         FROM sqlite_master
         WHERE substr(name, 1, 7) != 'sqlite_'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let framing_bytes = object_count
        .checked_mul(32)
        .ok_or(DatabaseError::InvalidData(
            "schema evidence size is invalid",
        ))?;
    let canonical_bytes =
        component_bytes
            .checked_add(framing_bytes)
            .ok_or(DatabaseError::InvalidData(
                "schema evidence size is invalid",
            ))?;
    if maximum_sql_bytes < 0
        || component_bytes < 0
        || maximum_sql_bytes > MAX_RELEASE_SCHEMA_SQL_BYTES
        || canonical_bytes > MAX_RELEASE_SCHEMA_CANONICAL_BYTES
    {
        return Err(DatabaseError::InvalidData(
            "schema evidence exceeds the bounded release layout",
        ));
    }
    let canonical_capacity = usize::try_from(canonical_bytes)
        .map_err(|_| DatabaseError::InvalidData("schema evidence size is invalid"))?;
    let mut statement = connection.prepare(
        "SELECT type, name, tbl_name, COALESCE(sql, '')
         FROM sqlite_master
         WHERE substr(name, 1, 7) != 'sqlite_'
         ORDER BY type, name",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;
    let mut canonical = Vec::with_capacity(canonical_capacity);
    for row in rows {
        let (object_type, name, table_name, sql) = row?;
        for component in [object_type, name, table_name, normalize_schema_sql(&sql)] {
            let length = u64::try_from(component.len())
                .map_err(|_| DatabaseError::InvalidData("schema evidence is too large"))?;
            canonical.extend_from_slice(&length.to_be_bytes());
            canonical.extend_from_slice(component.as_bytes());
        }
    }
    Ok(PayloadDigest::of_bytes(&canonical).to_hex())
}

// Keep token separators and quoted contents significant: deleting whitespace
// would make `INTEGER NOT NULL` collide with `INTEGERNOT NULL`, admitting an
// altered constraint under the released fingerprint.
// See [layout recognition](../../docs/reference/storage-format.md#current-schema-admission).
fn normalize_schema_sql(sql: &str) -> String {
    let mut normalized = String::with_capacity(sql.len());
    let mut quote = None;
    let mut pending_separator = false;
    let mut characters = sql.chars().peekable();
    while let Some(character) = characters.next() {
        if let Some(active_quote) = quote {
            normalized.push(character);
            if character == active_quote {
                if characters.peek() == Some(&active_quote) {
                    normalized.push(active_quote);
                    characters.next();
                } else {
                    quote = None;
                }
            }
        } else if matches!(character, '\'' | '"') {
            if pending_separator && !normalized.is_empty() {
                normalized.push(' ');
            }
            pending_separator = false;
            quote = Some(character);
            normalized.push(character);
        } else if character.is_whitespace() {
            pending_separator = !normalized.is_empty();
        } else {
            if pending_separator {
                normalized.push(' ');
                pending_separator = false;
            }
            normalized.push(character);
        }
    }
    normalized
}

fn validate_schema_identity(connection: &Connection) -> Result<(), DatabaseError> {
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM (SELECT 1 FROM schema_identity LIMIT 2)",
        [],
        |row| row.get(0),
    )?;
    let valid: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM schema_identity
         WHERE version = 1 AND release_version = ?1 AND name = ?2 AND checksum = ?3)",
        params![
            undercurrent_foundation::RELEASE_VERSION,
            SCHEMA_NAME,
            SCHEMA_CHECKSUM
        ],
        |row| row.get(0),
    )?;
    if count != 1 || !valid {
        return Err(DatabaseError::InvalidData(
            "database schema identity is not current",
        ));
    }
    Ok(())
}

pub(super) fn validate_input_retirement_consistency(
    connection: &Connection,
    max_record_bytes: usize,
) -> Result<(), DatabaseError> {
    input_retirements::validate(connection, max_record_bytes)
}

pub(super) fn validate_attempt_repair_consistency(
    connection: &Connection,
) -> Result<(), DatabaseError> {
    let invalid: i64 = connection.query_row(
        "SELECT COUNT(*)
         FROM operation_attempt_repairs r
         LEFT JOIN operation_attempts a ON a.attempt_id = r.attempt_id
         WHERE a.attempt_id IS NULL
            OR a.fencing_token != r.fencing_token
            OR a.state != r.certainty
            OR a.settled_at != r.committed_at
            OR COALESCE(a.result_digest, '') != COALESCE(r.result_digest, '')",
        [],
        |row| row.get(0),
    )?;
    if invalid != 0 {
        return Err(DatabaseError::InvalidData(
            "attempt repair audit does not match durable attempt state",
        ));
    }
    Ok(())
}

pub(super) fn validate_runtime_policy_consistency(
    connection: &Connection,
) -> Result<(), DatabaseError> {
    let invalid: i64 = connection.query_row(
        "SELECT COUNT(*) FROM operation_attempts
         WHERE (dispatch_cell_identity IS NULL AND (runtime_policy_state != 0 OR runtime_policy_json IS NOT NULL))
            OR (dispatch_cell_identity IS NOT NULL AND (runtime_policy_state != 1 OR runtime_policy_json IS NULL))",
        [], |row| row.get(0),
    )?;
    if invalid != 0 {
        return Err(DatabaseError::InvalidData(
            "runtime dispatch policy is required for attached attempts",
        ));
    }
    Ok(())
}

pub(super) fn validate_request_digest_consistency(
    connection: &Connection,
) -> Result<(), DatabaseError> {
    let invalid: i64 = connection.query_row(
        "SELECT COUNT(*) FROM operation_attempts
         WHERE (dispatch_cell_identity IS NULL AND request_digest IS NOT NULL)
            OR (dispatch_cell_identity IS NOT NULL AND request_digest IS NULL)",
        [],
        |row| row.get(0),
    )?;
    if invalid != 0 {
        return Err(DatabaseError::InvalidData(
            "caller request digest is required for attached attempts",
        ));
    }
    Ok(())
}

struct AttemptRepairAudit {
    fencing_token: Vec<u8>,
    certainty: i64,
    result_digest: Option<String>,
    evidence_reference: String,
    actor_principal: String,
    actor_tenant: Option<String>,
    actor_authority: String,
}

fn load_attempt_repair(
    connection: &Connection,
    attempt_id: &OperationAttemptId,
) -> Result<Option<AttemptRepairAudit>, DatabaseError> {
    connection
        .query_row(
            "SELECT fencing_token, certainty, result_digest, evidence_reference,
                    actor_principal, actor_tenant, actor_authority
             FROM operation_attempt_repairs WHERE attempt_id = ?1",
            [attempt_id.as_str()],
            |row| {
                Ok(AttemptRepairAudit {
                    fencing_token: row.get(0)?,
                    certainty: row.get(1)?,
                    result_digest: row.get(2)?,
                    evidence_reference: row.get(3)?,
                    actor_principal: row.get(4)?,
                    actor_tenant: row.get(5)?,
                    actor_authority: row.get(6)?,
                })
            },
        )
        .optional()
        .map_err(DatabaseError::from)
}

fn insert_attempt_repair(
    transaction: &Transaction<'_>,
    commit: &OperationAttemptRepairCommit,
) -> Result<(), DatabaseError> {
    transaction.execute(
        "INSERT INTO operation_attempt_repairs (
            attempt_id, fencing_token, certainty, result_digest, evidence_reference,
            actor_principal, actor_tenant, actor_authority, committed_at, record_version
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1)",
        params![
            commit.attempt_id().as_str(),
            encode_u64(commit.fencing_token().value()),
            encode_certainty(commit.certainty()),
            commit.result_digest().map(|digest| digest.to_hex()),
            commit.evidence_reference().as_str(),
            commit.actor().principal(),
            commit.actor().tenant(),
            commit.actor().authority(),
            commit.repaired_at().value(),
        ],
    )?;
    Ok(())
}

fn classify_repair_duplicate(
    audit: &AttemptRepairAudit,
    commit: &OperationAttemptRepairCommit,
) -> OperationAttemptRepairOutcome {
    if audit.fencing_token != encode_u64(commit.fencing_token().value()) {
        return OperationAttemptRepairOutcome::StaleFence {
            current: commit.fencing_token(),
        };
    }
    let result_digest = commit.result_digest().map(|digest| digest.to_hex());
    if audit.certainty != encode_certainty(commit.certainty())
        || audit.result_digest.as_deref() != result_digest.as_deref()
    {
        return OperationAttemptRepairOutcome::PayloadConflict;
    }
    if audit.evidence_reference != commit.evidence_reference().as_str()
        || audit.actor_principal != commit.actor().principal()
        || audit.actor_tenant.as_deref() != commit.actor().tenant()
        || audit.actor_authority != commit.actor().authority()
    {
        return OperationAttemptRepairOutcome::EvidenceConflict;
    }
    OperationAttemptRepairOutcome::Duplicate
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RuntimeDispatchPolicyRecord {
    record_version: undercurrent_foundation::ReleaseVersion,
    limits: RuntimeDispatchLimitsRecord,
    output_contract: RuntimeOutputContractRecord,
    telemetry_policy_ref: String,
    #[serde(default)]
    retention_policy_ref: Option<String>,
    callback_refs: Vec<String>,
    launch_profile_expires_at_millis: i64,
    environment_binding_expires_at_millis: i64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RuntimeDispatchLimitsRecord {
    wall_time_millis: u64,
    stdout_bytes: u64,
    stderr_bytes: u64,
    #[serde(deserialize_with = "deserialize_required_record_option")]
    memory_bytes: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_record_option")]
    cpu_millis: Option<u32>,
}

fn deserialize_required_record_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RuntimeOutputContractRecord {
    schema: String,
    schema_digest_sha256: String,
    classification: Classification,
}

impl Debug for RuntimeOutputContractRecord {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeOutputContractRecord")
            .field("schema", &self.schema)
            .field("classification", &self.classification)
            .finish_non_exhaustive()
    }
}

impl Debug for RuntimeDispatchPolicyRecord {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeDispatchPolicyRecord")
            .field("record_version", &self.record_version)
            .field("limits", &self.limits)
            .field("output_contract", &self.output_contract)
            .field("callback_count", &self.callback_refs.len())
            .field(
                "launch_profile_expires_at_millis",
                &self.launch_profile_expires_at_millis,
            )
            .field(
                "environment_binding_expires_at_millis",
                &self.environment_binding_expires_at_millis,
            )
            .finish_non_exhaustive()
    }
}

impl RuntimeDispatchPolicyRecord {
    fn from_domain(policy: &RuntimeDispatchPolicy) -> Self {
        let limits = policy.limits();
        let retention_policy_ref = policy.retention_policy_ref();
        Self {
            record_version: RUNTIME_POLICY_RECORD_VERSION,
            limits: RuntimeDispatchLimitsRecord {
                wall_time_millis: limits.wall_time_millis(),
                stdout_bytes: limits.stdout_bytes(),
                stderr_bytes: limits.stderr_bytes(),
                memory_bytes: limits.memory_bytes(),
                cpu_millis: limits.cpu_millis(),
            },
            output_contract: RuntimeOutputContractRecord {
                schema: policy.output_contract().schema().to_owned(),
                schema_digest_sha256: policy.output_contract().schema_digest().to_hex(),
                classification: policy.output_contract().classification(),
            },
            telemetry_policy_ref: policy.telemetry_policy_ref().as_str().to_owned(),
            retention_policy_ref: retention_policy_ref
                .map(|reference| reference.as_str().to_owned()),
            callback_refs: policy
                .callback_refs()
                .iter()
                .map(|reference| reference.as_str().to_owned())
                .collect(),
            launch_profile_expires_at_millis: policy.launch_profile_expires_at().value(),
            environment_binding_expires_at_millis: policy.environment_binding_expires_at().value(),
        }
    }

    fn into_domain(
        self,
        accepted_at: TimestampMillis,
    ) -> Result<RuntimeDispatchPolicy, DatabaseError> {
        if self.record_version != RUNTIME_POLICY_RECORD_VERSION {
            return Err(DatabaseError::InvalidData(
                "runtime dispatch policy record version is unsupported",
            ));
        }
        let limits = RuntimeDispatchLimits::new(
            self.limits.wall_time_millis,
            self.limits.stdout_bytes,
            self.limits.stderr_bytes,
            self.limits.memory_bytes,
            self.limits.cpu_millis,
        )
        .map_err(|_| DatabaseError::InvalidData("runtime dispatch limits are invalid"))?;
        let output_contract = RuntimeOutputContract::new(
            self.output_contract.schema,
            PayloadDigest::from_hex(&self.output_contract.schema_digest_sha256)
                .map_err(|_| DatabaseError::InvalidData("output contract digest is invalid"))?,
            self.output_contract.classification,
        )
        .map_err(|_| DatabaseError::InvalidData("runtime output contract is invalid"))?;
        let telemetry_policy_ref = RuntimeTelemetryPolicyRef::new(self.telemetry_policy_ref)
            .map_err(|_| DatabaseError::InvalidData("runtime telemetry policy ref is invalid"))?;
        let retention_policy_ref = self
            .retention_policy_ref
            .map(RuntimeRetentionPolicyRef::new)
            .transpose()
            .map_err(|_| DatabaseError::InvalidData("runtime retention policy ref is invalid"))?;
        if self.record_version == RUNTIME_POLICY_RECORD_VERSION && retention_policy_ref.is_none() {
            return Err(DatabaseError::InvalidData(
                "current runtime dispatch policy is missing retention provenance",
            ));
        }
        let callback_refs = self
            .callback_refs
            .into_iter()
            .map(RuntimeCallbackRef::new)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| DatabaseError::InvalidData("runtime callback ref is invalid"))?;
        let launch_profile_expires_at = TimestampMillis::new(self.launch_profile_expires_at_millis);
        let environment_binding_expires_at =
            TimestampMillis::new(self.environment_binding_expires_at_millis);
        let retention_policy_ref = retention_policy_ref.ok_or(DatabaseError::InvalidData(
            "runtime retention policy reference is required",
        ))?;
        let policy = RuntimeDispatchPolicy::new(
            limits,
            output_contract,
            telemetry_policy_ref,
            retention_policy_ref,
            callback_refs,
            launch_profile_expires_at,
            environment_binding_expires_at,
        )
        .map_err(|_| DatabaseError::InvalidData("runtime dispatch policy is invalid"))?;
        policy.validate_at(accepted_at).map_err(|_| {
            DatabaseError::InvalidData("runtime dispatch policy was stale at durable acceptance")
        })?;
        Ok(policy)
    }
}

fn encode_runtime_dispatch_policy(
    policy: Option<&RuntimeDispatchPolicy>,
) -> Result<Option<String>, DatabaseError> {
    let Some(policy) = policy else {
        return Ok(None);
    };
    let encoded = serde_json::to_string(&RuntimeDispatchPolicyRecord::from_domain(policy))
        .map_err(|_| DatabaseError::InvalidData("runtime dispatch policy could not be encoded"))?;
    if encoded.len() > MAX_RUNTIME_POLICY_RECORD_BYTES {
        return Err(DatabaseError::Capacity("runtime dispatch policy record"));
    }
    Ok(Some(encoded))
}

fn decode_runtime_dispatch_policy(
    encoded: &str,
    accepted_at: TimestampMillis,
) -> Result<RuntimeDispatchPolicy, DatabaseError> {
    if encoded.len() > MAX_RUNTIME_POLICY_RECORD_BYTES {
        return Err(DatabaseError::InvalidData(
            "runtime dispatch policy record exceeds its hard bound",
        ));
    }
    let record: RuntimeDispatchPolicyRecord = serde_json::from_str(encoded)
        .map_err(|_| DatabaseError::InvalidData("runtime dispatch policy record is invalid"))?;
    let canonical = serde_json::to_string(&record).map_err(|_| {
        DatabaseError::InvalidData("runtime dispatch policy record could not be canonicalized")
    })?;
    if canonical != encoded {
        return Err(DatabaseError::InvalidData(
            "runtime dispatch policy record is not canonical",
        ));
    }
    record.into_domain(accepted_at)
}

fn maybe_crash(
    injector: &dyn SqliteCrashInjector,
    point: SqliteCrashPoint,
) -> Result<(), DatabaseError> {
    if injector.should_crash(point) {
        Err(DatabaseError::InjectedCrash(point))
    } else {
        Ok(())
    }
}

fn insert_attempt(
    transaction: &Transaction<'_>,
    attempt: &OperationAttempt,
) -> Result<(), DatabaseError> {
    if !matches!(attempt.state(), OperationAttemptState::Accepted) {
        return Err(DatabaseError::InvalidData(
            "only an accepted attempt may be inserted",
        ));
    }
    let (request_digest, runtime_policy_state, runtime_policy_json) =
        match attempt.dispatch_binding() {
            None => (None, 0_i64, None),
            Some(binding) => {
                let request_digest = binding.request_digest().ok_or(DatabaseError::InvalidData(
                    "current attached attempts require a retained caller request digest",
                ))?;
                let policy = binding.runtime_policy().ok_or(DatabaseError::InvalidData(
                    "current attached attempts require a retained runtime dispatch policy",
                ))?;
                (
                    Some(request_digest.to_hex()),
                    1_i64,
                    encode_runtime_dispatch_policy(Some(policy))?,
                )
            }
        };
    transaction.execute(
        "INSERT INTO operation_attempts (
            attempt_id, target_session, mutation_kind, fencing_token, payload_digest,
            dispatch_cell_identity, launch_profile_digest, environment_binding_digest, input_digest,
            request_digest, runtime_policy_json, runtime_policy_state, accepted_at, state,
            result_digest, settled_at,
            superseding_fencing_token,
            cleanup_digest, compensation_process_id, compensation_process_started_at,
            compensation_session_revision, compensation_prepared_at, record_version
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, 0, NULL, NULL,
                   NULL, NULL, NULL, NULL, NULL, NULL, ?14)",
        params![
            attempt.id().as_str(),
            attempt.target_session().map(SessionId::as_str),
            attempt.kind().as_str(),
            encode_u64(attempt.fencing_token().value()),
            attempt.payload_digest().to_hex(),
            attempt
                .dispatch_binding()
                .map(|binding| binding.caller_cell_identity().as_str()),
            attempt
                .dispatch_binding()
                .map(|binding| binding.launch_profile_digest().to_hex()),
            attempt
                .dispatch_binding()
                .map(|binding| binding.environment_binding_digest().to_hex()),
            attempt
                .dispatch_binding()
                .map(|binding| binding.input_digest().to_hex()),
            request_digest,
            runtime_policy_json,
            runtime_policy_state,
            attempt.accepted_at().value(),
            ATTEMPT_RECORD_VERSION,
        ],
    )?;
    insert_agent_runtime_binding(transaction, attempt)?;
    Ok(())
}

fn insert_agent_runtime_binding(
    transaction: &Transaction<'_>,
    attempt: &OperationAttempt,
) -> Result<(), DatabaseError> {
    let Some(agent) = attempt
        .dispatch_binding()
        .and_then(RuntimeDispatchBinding::agent)
    else {
        return Ok(());
    };
    transaction.execute(
        "INSERT INTO agent_runtime_bindings (
            attempt_id, tenant_id, source_product, actor_context_ref, authority_ref,
            undercurrent_provider_id, undercurrent_provider_generation,
            attempt_revision, recovery_epoch, request_digest,
            expected_result_schema_json, binding_json, expires_at, record_version
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, 1)",
        params![
            attempt.id().as_str(),
            agent.tenant_id(),
            agent.source_product(),
            agent.actor_context_ref(),
            agent.authority_ref(),
            agent.undercurrent_provider_id(),
            encode_u64(agent.undercurrent_provider_generation()),
            encode_u64(agent.attempt_revision()),
            encode_u64(agent.recovery_epoch()),
            agent.request_digest().to_hex(),
            agent.expected_result_schema_canonical_json(),
            agent.canonical_json(),
            agent.expires_at().value(),
        ],
    )?;
    Ok(())
}

fn update_attempt(
    transaction: &Transaction<'_>,
    attempt: &OperationAttempt,
) -> Result<(), DatabaseError> {
    let (state, result_digest, settled_at) = match attempt.state() {
        OperationAttemptState::Accepted => (0_i64, None, None),
        OperationAttemptState::Settled {
            certainty,
            result_digest,
            settled_at,
        } => (
            encode_certainty(*certainty),
            result_digest.map(PayloadDigest::to_hex),
            Some(settled_at.value()),
        ),
        OperationAttemptState::Compensated { compensated_at, .. } => {
            (4_i64, None, Some(compensated_at.value()))
        }
    };
    let intent = attempt.compensation_intent();
    if let OperationAttemptState::Compensated {
        superseding_fencing_token,
        cleanup_digest,
        ..
    } = attempt.state()
        && intent.is_none_or(|intent| {
            intent.superseding_fencing_token() != *superseding_fencing_token
                || intent.cleanup_digest() != *cleanup_digest
        })
    {
        return Err(DatabaseError::InvalidData(
            "compensated attempt does not match its durable cleanup intent",
        ));
    }
    let changed = transaction.execute(
        "UPDATE operation_attempts
         SET state = ?1, result_digest = ?2, settled_at = ?3,
             superseding_fencing_token = ?4, cleanup_digest = ?5,
             compensation_process_id = ?6, compensation_process_started_at = ?7,
             compensation_session_revision = ?8, compensation_prepared_at = ?9
         WHERE attempt_id = ?10 AND fencing_token = ?11",
        params![
            state,
            result_digest,
            settled_at,
            intent.map(|intent| encode_u64(intent.superseding_fencing_token().value())),
            intent.map(|intent| intent.cleanup_digest().to_hex()),
            intent.map(|intent| intent.process().id().as_str()),
            intent.map(|intent| intent.process().started_at().value()),
            intent.map(|intent| encode_u64(intent.session_revision().value())),
            intent.map(|intent| intent.prepared_at().value()),
            attempt.id().as_str(),
            encode_u64(attempt.fencing_token().value()),
        ],
    )?;
    if changed != 1 {
        return Err(DatabaseError::InvalidData(
            "attempt update affected an unexpected row count",
        ));
    }
    Ok(())
}

fn classify_attempt_fence(
    transaction: &Transaction<'_>,
    attempt: &OperationAttempt,
    requested: FencingToken,
) -> Result<OperationAttemptSettlementOutcome, DatabaseError> {
    let current = current_fence(
        transaction,
        attempt.target_session(),
        attempt
            .dispatch_binding()
            .map(RuntimeDispatchBinding::caller_cell_identity),
    )?
    .map(|(_, fencing_token)| fencing_token)
    .unwrap_or(attempt.fencing_token());
    if requested < current {
        Ok(OperationAttemptSettlementOutcome::StaleFence { current })
    } else {
        Ok(OperationAttemptSettlementOutcome::PayloadConflict)
    }
}

// Cancelling a preparation cannot apply an old shutdown to a newer host. The
// exact attempt fence is still checked by callers; only the global high-water
// mark may be newer (for example after another successful daemon stop).
fn is_shutdown_cancellation(
    attempt: &OperationAttempt,
    certainty: EffectCertainty,
    result_digest: Option<PayloadDigest>,
) -> bool {
    attempt.kind() == ProcessMutationKind::Shutdown
        && attempt.target_session().is_none()
        && attempt.dispatch_binding().is_none()
        && certainty == EffectCertainty::KnownAbsent
        && result_digest.is_none()
}

// A concurrent force can finish an already admitted stop. Its newer fence must
// not erase the older stop's exact applied receipt. Only force attempts may
// intervene: spawn, input, or any other mutation retains the ordinary fence
// barrier. This permits recording evidence, never admitting/redriving an effect.
fn completed_stop_before_force(
    connection: &Connection,
    attempt: &OperationAttempt,
    certainty: EffectCertainty,
    result_digest: Option<PayloadDigest>,
) -> Result<bool, DatabaseError> {
    if attempt.dispatch_binding().is_some()
        || attempt.target_session().is_none()
        || !matches!(
            attempt.kind(),
            ProcessMutationKind::Stop | ProcessMutationKind::ForceStop
        )
        || certainty != EffectCertainty::KnownApplied
        || result_digest.is_none()
    {
        return Ok(false);
    }
    let blocked: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM operation_attempts
         WHERE target_session = ?1 AND dispatch_cell_identity IS NULL
           AND fencing_token > ?2 AND mutation_kind != 'force_stop')",
        params![
            attempt.target_session().map(SessionId::as_str),
            encode_u64(attempt.fencing_token().value())
        ],
        |row| row.get(0),
    )?;
    Ok(!blocked)
}

fn current_fence(
    connection: &Connection,
    target_session: Option<&SessionId>,
    dispatch_cell_identity: Option<&DispatchCellIdentity>,
) -> Result<Option<(OperationAttemptId, FencingToken)>, DatabaseError> {
    let raw = connection
        .query_row(
            "SELECT attempt_id, fencing_token
             FROM operation_attempts
             WHERE (?1 IS NOT NULL AND dispatch_cell_identity = ?1)
                OR (?1 IS NULL AND dispatch_cell_identity IS NULL AND target_session IS ?2)
             ORDER BY fencing_token DESC, attempt_id DESC
             LIMIT 1",
            params![
                dispatch_cell_identity.map(DispatchCellIdentity::as_str),
                target_session.map(SessionId::as_str)
            ],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()?;
    raw.map(|(attempt_id, fencing_token)| {
        Ok((
            OperationAttemptId::new(attempt_id)
                .map_err(|_| DatabaseError::InvalidData("invalid attempt identity index"))?,
            FencingToken::new(decode_u64(fencing_token)?)
                .map_err(|_| DatabaseError::InvalidData("invalid attempt fencing token"))?,
        ))
    })
    .transpose()
}

fn load_attempt(
    connection: &Connection,
    attempt_id: &OperationAttemptId,
) -> Result<Option<OperationAttempt>, DatabaseError> {
    let row = connection
        .query_row(
            "SELECT attempt_id, target_session, mutation_kind, fencing_token, payload_digest,
                    dispatch_cell_identity, launch_profile_digest, environment_binding_digest,
                    input_digest, request_digest, runtime_policy_json, runtime_policy_state,
                    accepted_at, state,
                    result_digest, settled_at,
                    superseding_fencing_token, cleanup_digest, compensation_process_id,
                    compensation_process_started_at, compensation_session_revision,
                    compensation_prepared_at, record_version
             FROM operation_attempts WHERE attempt_id = ?1",
            [attempt_id.as_str()],
            read_attempt_row,
        )
        .optional()?;
    let Some(row) = row else {
        return Ok(None);
    };
    let mut attempt = decode_attempt_row(row)?;
    if let Some(agent) =
        load_agent_runtime_binding(connection, attempt.id(), attempt.fencing_token())?
    {
        attempt
            .restore_agent_binding(agent)
            .map_err(|_| DatabaseError::InvalidData("agent binding conflicts with its attempt"))?;
    }
    Ok(Some(attempt))
}

fn load_agent_runtime_binding(
    connection: &Connection,
    attempt_id: &OperationAttemptId,
    fence: FencingToken,
) -> Result<Option<AgentRuntimeBinding>, DatabaseError> {
    let row = connection
        .query_row(
            "SELECT tenant_id, source_product, actor_context_ref, authority_ref,
                    undercurrent_provider_id, undercurrent_provider_generation,
                    attempt_revision, recovery_epoch, request_digest,
                    expected_result_schema_json, binding_json, expires_at, record_version
             FROM agent_runtime_bindings WHERE attempt_id = ?1",
            [attempt_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, Vec<u8>>(6)?,
                    row.get::<_, Vec<u8>>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, Vec<u8>>(9)?,
                    row.get::<_, Vec<u8>>(10)?,
                    row.get::<_, i64>(11)?,
                    row.get::<_, i64>(12)?,
                ))
            },
        )
        .optional()?;
    row.map(
        |(
            tenant_id,
            source_product,
            actor_context_ref,
            authority_ref,
            undercurrent_provider_id,
            undercurrent_provider_generation,
            attempt_revision,
            recovery_epoch,
            request_digest,
            expected_result_schema,
            binding_json,
            expires_at,
            record_version,
        )| {
            if record_version != 1 {
                return Err(DatabaseError::InvalidData(
                    "agent binding record version is unsupported",
                ));
            }
            AgentRuntimeBinding::new(
                tenant_id,
                source_product,
                actor_context_ref,
                authority_ref,
                undercurrent_provider_id,
                decode_u64(undercurrent_provider_generation)?,
                attempt_id.clone(),
                decode_u64(attempt_revision)?,
                decode_u64(recovery_epoch)?,
                fence,
                PayloadDigest::from_hex(&request_digest).map_err(|_| {
                    DatabaseError::InvalidData("agent binding request digest is invalid")
                })?,
                expected_result_schema,
                binding_json,
                TimestampMillis::new(expires_at),
            )
            .map_err(|_| DatabaseError::InvalidData("agent binding record is invalid"))
        },
    )
    .transpose()
}

struct AttemptRow {
    attempt_id: String,
    target_session: Option<String>,
    mutation_kind: String,
    fencing_token: Vec<u8>,
    payload_digest: String,
    dispatch_cell_identity: Option<String>,
    launch_profile_digest: Option<String>,
    environment_binding_digest: Option<String>,
    input_digest: Option<String>,
    request_digest: Option<String>,
    runtime_policy_json: Option<String>,
    runtime_policy_state: i64,
    accepted_at: i64,
    state: i64,
    result_digest: Option<String>,
    settled_at: Option<i64>,
    superseding_fencing_token: Option<Vec<u8>>,
    cleanup_digest: Option<String>,
    compensation_process_id: Option<String>,
    compensation_process_started_at: Option<i64>,
    compensation_session_revision: Option<Vec<u8>>,
    compensation_prepared_at: Option<i64>,
    record_version: i64,
}

fn read_attempt_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AttemptRow> {
    Ok(AttemptRow {
        attempt_id: row.get(0)?,
        target_session: row.get(1)?,
        mutation_kind: row.get(2)?,
        fencing_token: row.get(3)?,
        payload_digest: row.get(4)?,
        dispatch_cell_identity: row.get(5)?,
        launch_profile_digest: row.get(6)?,
        environment_binding_digest: row.get(7)?,
        input_digest: row.get(8)?,
        request_digest: row.get(9)?,
        runtime_policy_json: row.get(10)?,
        runtime_policy_state: row.get(11)?,
        accepted_at: row.get(12)?,
        state: row.get(13)?,
        result_digest: row.get(14)?,
        settled_at: row.get(15)?,
        superseding_fencing_token: row.get(16)?,
        cleanup_digest: row.get(17)?,
        compensation_process_id: row.get(18)?,
        compensation_process_started_at: row.get(19)?,
        compensation_session_revision: row.get(20)?,
        compensation_prepared_at: row.get(21)?,
        record_version: row.get(22)?,
    })
}

fn decode_attempt_row(row: AttemptRow) -> Result<OperationAttempt, DatabaseError> {
    if row.record_version != ATTEMPT_RECORD_VERSION {
        return Err(DatabaseError::InvalidData(
            "attempt record version is unsupported",
        ));
    }
    let attempt_id = OperationAttemptId::new(row.attempt_id)
        .map_err(|_| DatabaseError::InvalidData("attempt identity is invalid"))?;
    let target_session = row
        .target_session
        .map(SessionId::new)
        .transpose()
        .map_err(|_| DatabaseError::InvalidData("attempt target session is invalid"))?;
    let kind = decode_mutation_kind(&row.mutation_kind).ok_or(DatabaseError::InvalidData(
        "attempt mutation kind is unknown",
    ))?;
    let fencing_token = FencingToken::new(decode_u64(row.fencing_token)?)
        .map_err(|_| DatabaseError::InvalidData("attempt fencing token is invalid"))?;
    let payload_digest = PayloadDigest::from_hex(&row.payload_digest)
        .map_err(|_| DatabaseError::InvalidData("attempt payload digest is invalid"))?;
    let accepted_at = TimestampMillis::new(row.accepted_at);
    let dispatch_binding = match (
        row.dispatch_cell_identity,
        row.launch_profile_digest,
        row.environment_binding_digest,
        row.input_digest,
        row.request_digest,
        row.runtime_policy_json,
        row.runtime_policy_state,
    ) {
        (None, None, None, None, None, None, 0) => None,
        (
            Some(cell),
            Some(launch),
            Some(environment),
            Some(input),
            request,
            policy,
            policy_state,
        ) => {
            let cell = DispatchCellIdentity::new(cell)
                .map_err(|_| DatabaseError::InvalidData("dispatch cell identity is invalid"))?;
            let launch = PayloadDigest::from_hex(&launch)
                .map_err(|_| DatabaseError::InvalidData("launch profile digest is invalid"))?;
            let environment = PayloadDigest::from_hex(&environment)
                .map_err(|_| DatabaseError::InvalidData("environment binding digest is invalid"))?;
            let input = PayloadDigest::from_hex(&input)
                .map_err(|_| DatabaseError::InvalidData("input digest is invalid"))?;
            let request = request
                .as_deref()
                .map(PayloadDigest::from_hex)
                .transpose()
                .map_err(|_| DatabaseError::InvalidData("caller request digest is invalid"))?;
            Some(match (request, policy_state, policy) {
                (Some(request), 1, Some(policy)) => RuntimeDispatchBinding::new(
                    cell,
                    request,
                    launch,
                    environment,
                    input,
                    decode_runtime_dispatch_policy(&policy, accepted_at)?,
                ),
                _ => {
                    return Err(DatabaseError::InvalidData(
                        "runtime dispatch binding provenance is invalid",
                    ));
                }
            })
        }
        _ => return Err(DatabaseError::InvalidData("dispatch binding is incomplete")),
    };
    let mut attempt = OperationAttempt::accepted_with_dispatch(
        attempt_id,
        target_session,
        kind,
        fencing_token,
        payload_digest,
        dispatch_binding,
        accepted_at,
    );
    let compensation_intent = match (
        row.superseding_fencing_token,
        row.cleanup_digest,
        row.compensation_process_id,
        row.compensation_process_started_at,
        row.compensation_session_revision,
        row.compensation_prepared_at,
    ) {
        (None, None, None, None, None, None) => None,
        (
            Some(superseding_fencing_token),
            Some(cleanup_digest),
            Some(process_id),
            Some(process_started_at),
            Some(session_revision),
            Some(prepared_at),
        ) => {
            let superseding_fencing_token =
                FencingToken::new(decode_u64(superseding_fencing_token)?).map_err(|_| {
                    DatabaseError::InvalidData("superseding fencing token is invalid")
                })?;
            let cleanup_digest = PayloadDigest::from_hex(&cleanup_digest)
                .map_err(|_| DatabaseError::InvalidData("cleanup digest is invalid"))?;
            let process = ProcessIdentity::new(
                ProcessInstanceId::new(process_id).map_err(|_| {
                    DatabaseError::InvalidData("compensation process identity is invalid")
                })?,
                TimestampMillis::new(process_started_at),
            );
            let session_revision = Revision::new(decode_u64(session_revision)?);
            attempt
                .prepare_compensation(
                    fencing_token,
                    superseding_fencing_token,
                    process,
                    session_revision,
                    cleanup_digest,
                    TimestampMillis::new(prepared_at),
                )
                .map_err(|_| {
                    DatabaseError::InvalidData("attempt compensation intent is invalid")
                })?;
            Some((superseding_fencing_token, cleanup_digest))
        }
        _ => {
            return Err(DatabaseError::InvalidData(
                "attempt compensation intent is incomplete",
            ));
        }
    };
    match row.state {
        0 if row.result_digest.is_none() && row.settled_at.is_none() => {}
        1..=3 if compensation_intent.is_none() => {
            let certainty = decode_certainty(row.state)
                .ok_or(DatabaseError::InvalidData("attempt certainty is invalid"))?;
            let result_digest = row
                .result_digest
                .as_deref()
                .map(PayloadDigest::from_hex)
                .transpose()
                .map_err(|_| DatabaseError::InvalidData("attempt result digest is invalid"))?;
            let settled_at =
                row.settled_at
                    .map(TimestampMillis::new)
                    .ok_or(DatabaseError::InvalidData(
                        "settled attempt lacks a timestamp",
                    ))?;
            attempt
                .settle(fencing_token, certainty, result_digest, settled_at)
                .map_err(|_| DatabaseError::InvalidData("attempt settlement is invalid"))?;
        }
        4 if row.result_digest.is_none() => {
            let (superseding_fencing_token, cleanup_digest) = compensation_intent.ok_or(
                DatabaseError::InvalidData("compensated attempt lacks a cleanup intent"),
            )?;
            let compensated_at =
                row.settled_at
                    .map(TimestampMillis::new)
                    .ok_or(DatabaseError::InvalidData(
                        "compensated attempt lacks a timestamp",
                    ))?;
            attempt
                .compensate(
                    fencing_token,
                    superseding_fencing_token,
                    cleanup_digest,
                    compensated_at,
                )
                .map_err(|_| DatabaseError::InvalidData("attempt compensation is invalid"))?;
        }
        _ => {
            return Err(DatabaseError::InvalidData(
                "attempt state encoding is invalid",
            ));
        }
    }
    Ok(attempt)
}

const fn encode_certainty(certainty: EffectCertainty) -> i64 {
    match certainty {
        EffectCertainty::KnownAbsent => 1,
        EffectCertainty::KnownApplied => 2,
        EffectCertainty::Unknown => 3,
    }
}

const fn decode_certainty(value: i64) -> Option<EffectCertainty> {
    match value {
        1 => Some(EffectCertainty::KnownAbsent),
        2 => Some(EffectCertainty::KnownApplied),
        3 => Some(EffectCertainty::Unknown),
        _ => None,
    }
}

fn decode_mutation_kind(value: &str) -> Option<ProcessMutationKind> {
    ProcessMutationKind::ALL
        .into_iter()
        .find(|kind| kind.as_str() == value)
}

/// Checks stored counters and outbox checkpoints against physical rows at open.
fn validate_repository_state(
    connection: &Connection,
    limits: SqliteRepositoryLimits,
) -> Result<(), DatabaseError> {
    let (actual_sessions, actual_outbox) = validate_repository_consistency(connection)?;
    if actual_sessions > limits.max_sessions || actual_outbox > limits.max_outbox_events {
        return Err(DatabaseError::Capacity(
            "configured limits are below existing durable state",
        ));
    }
    Ok(())
}

fn validate_repository_consistency(
    connection: &Connection,
) -> Result<(usize, usize), DatabaseError> {
    let state = read_repository_state(connection)?;
    let actual_sessions = count_rows(connection, "sessions")?;
    let actual_outbox = count_rows(connection, "outbox")?;
    if state.session_count != actual_sessions || state.outbox_count != actual_outbox {
        return Err(DatabaseError::InvalidData(
            "repository counters do not match durable rows",
        ));
    }
    let maximum_pending = connection
        .query_row(
            "SELECT sequence FROM outbox ORDER BY sequence DESC LIMIT 1",
            [],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?
        .map(decode_u64)
        .transpose()?;
    if maximum_pending.is_some_and(|maximum| maximum >= state.next_sequence.value())
        || state
            .last_acknowledged
            .is_some_and(|acknowledged| acknowledged >= state.next_sequence)
    {
        return Err(DatabaseError::InvalidData(
            "outbox checkpoints do not precede the next sequence",
        ));
    }
    Ok((actual_sessions, actual_outbox))
}

fn validate_insert_event(
    session: &Session,
    event: &SessionOutboxEvent,
) -> Result<(), DatabaseError> {
    if event.session_id != *session.id() || event.revision != session.revision() {
        return Err(DatabaseError::InvalidData(
            "initial event does not match aggregate identity and revision",
        ));
    }
    Ok(())
}

fn validate_commit_events(
    session: &Session,
    expected_revision: Revision,
    events: &[SessionOutboxEvent],
) -> Result<(), DatabaseError> {
    if events.is_empty()
        || events.iter().any(|event| event.session_id != *session.id())
        || events.last().map(|event| event.revision) != Some(session.revision())
        || events
            .first()
            .is_some_and(|event| event.revision <= expected_revision)
        || events
            .windows(2)
            .any(|pair| pair[0].revision >= pair[1].revision)
    {
        return Err(DatabaseError::InvalidData(
            "commit events do not form an ordered aggregate revision sequence",
        ));
    }
    Ok(())
}

fn validate_delete_event(
    session_id: &SessionId,
    expected_revision: Revision,
    event: &SessionOutboxEvent,
) -> Result<(), DatabaseError> {
    if event.session_id != *session_id || event.revision <= expected_revision {
        return Err(DatabaseError::InvalidData(
            "removal event does not advance the target aggregate revision",
        ));
    }
    Ok(())
}

fn validate_batch_limit(limit: usize, maximum: usize) -> Result<(), DatabaseError> {
    if limit == 0 || limit > maximum {
        return Err(DatabaseError::InvalidData(
            "requested batch is zero or above the configured maximum",
        ));
    }
    Ok(())
}

fn exists_by_id(transaction: &Transaction<'_>, id: &SessionId) -> Result<bool, DatabaseError> {
    Ok(transaction
        .query_row(
            "SELECT 1 FROM sessions WHERE id = ?1",
            [id.as_str()],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

fn find_id_by_name(
    transaction: &Transaction<'_>,
    name: &SessionName,
) -> Result<Option<SessionId>, DatabaseError> {
    let value = transaction
        .query_row(
            "SELECT id FROM sessions WHERE name = ?1",
            [name.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    value
        .map(|value| {
            SessionId::new(value)
                .map_err(|_| DatabaseError::InvalidData("invalid session ID index"))
        })
        .transpose()
}

fn find_revision_and_name(
    transaction: &Transaction<'_>,
    id: &SessionId,
) -> Result<Option<(Revision, SessionName)>, DatabaseError> {
    let raw = transaction
        .query_row(
            "SELECT revision, name FROM sessions WHERE id = ?1",
            [id.as_str()],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    raw.map(|(revision, name)| {
        Ok((
            Revision::new(decode_u64(revision)?),
            SessionName::new(name)
                .map_err(|_| DatabaseError::InvalidData("invalid session name index"))?,
        ))
    })
    .transpose()
}

/// Allocates exact big-endian `u64` event positions without SQLite signed-range
/// truncation; the caller owns the surrounding transaction.
fn append_events(
    transaction: &Transaction<'_>,
    first: OutboxSequence,
    events: &[SessionOutboxEvent],
    payload: &[u8],
) -> Result<OutboxSequence, DatabaseError> {
    // One commit supplies its final aggregate for every event; delete supplies
    // the prior terminal aggregate. Snapshot revision need not equal each event
    // revision, and loading the live session later would lose these observations.
    // See [snapshot meaning](../../docs/internals/transactions/sessions-and-outbox.md#snapshot-meaning-and-retention).
    let mut sequence = first;
    for event in events {
        transaction.execute(
            "INSERT INTO outbox \
             (sequence, session_id, session_revision, event_kind, occurred_at, record_version) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                encode_u64(sequence.value()),
                event.session_id.as_str(),
                encode_u64(event.revision.value()),
                i64::from(encode_event_kind(event.kind)),
                event.occurred_at.value(),
                OUTBOX_RECORD_VERSION,
            ],
        )?;
        transaction.execute(
            "INSERT INTO session_event_snapshots(sequence, payload) VALUES (?1, ?2)",
            params![encode_u64(sequence.value()), payload],
        )?;
        sequence = sequence
            .checked_next()
            .ok_or(DatabaseError::Capacity("outbox sequence"))?;
    }
    Ok(sequence)
}

/// Decodes the singleton repository metadata row with platform-range checks.
fn read_repository_state(connection: &Connection) -> Result<RepositoryState, DatabaseError> {
    let raw = connection.query_row(
        "SELECT session_count, outbox_count, next_outbox_sequence, last_acknowledged \
         FROM repository_state WHERE singleton = 1",
        [],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, Option<Vec<u8>>>(3)?,
            ))
        },
    )?;
    let session_count = usize::try_from(raw.0)
        .map_err(|_| DatabaseError::InvalidData("session count is outside platform range"))?;
    let outbox_count = usize::try_from(raw.1)
        .map_err(|_| DatabaseError::InvalidData("outbox count is outside platform range"))?;
    let next_sequence = OutboxSequence::new(decode_u64(raw.2)?)
        .ok_or(DatabaseError::InvalidData("next outbox sequence is zero"))?;
    let last_acknowledged = raw
        .3
        .map(decode_u64)
        .transpose()?
        .map(|value| {
            OutboxSequence::new(value)
                .ok_or(DatabaseError::InvalidData("acknowledged sequence is zero"))
        })
        .transpose()?;
    Ok(RepositoryState {
        session_count,
        outbox_count,
        next_sequence,
        last_acknowledged,
    })
}

fn update_repository_counts(
    transaction: &Transaction<'_>,
    session_count: usize,
    outbox_count: usize,
    next_sequence: OutboxSequence,
    last_acknowledged: Option<OutboxSequence>,
) -> Result<(), DatabaseError> {
    let session_count =
        i64::try_from(session_count).map_err(|_| DatabaseError::Capacity("session count"))?;
    let outbox_count =
        i64::try_from(outbox_count).map_err(|_| DatabaseError::Capacity("outbox count"))?;
    let updated = transaction.execute(
        "UPDATE repository_state SET session_count = ?1, outbox_count = ?2, \
         next_outbox_sequence = ?3, last_acknowledged = ?4 WHERE singleton = 1",
        params![
            session_count,
            outbox_count,
            encode_u64(next_sequence.value()),
            last_acknowledged.map(|sequence| encode_u64(sequence.value())),
        ],
    )?;
    if updated != 1 {
        return Err(DatabaseError::InvalidData(
            "repository state row was not updated exactly once",
        ));
    }
    Ok(())
}

fn count_rows(connection: &Connection, table: &'static str) -> Result<usize, DatabaseError> {
    let sql = match table {
        "sessions" => "SELECT COUNT(*) FROM sessions",
        "outbox" => "SELECT COUNT(*) FROM outbox",
        _ => return Err(DatabaseError::InvalidData("unknown count table")),
    };
    let count: i64 = connection.query_row(sql, [], |row| row.get(0))?;
    usize::try_from(count).map_err(|_| DatabaseError::InvalidData("row count is invalid"))
}

fn sql_usize(value: usize, field: &'static str) -> Result<i64, DatabaseError> {
    i64::try_from(value).map_err(|_| DatabaseError::InvalidData(field))
}

struct RepositoryState {
    session_count: usize,
    outbox_count: usize,
    next_sequence: OutboxSequence,
    last_acknowledged: Option<OutboxSequence>,
}

struct SessionRow {
    id: String,
    name: String,
    revision: Vec<u8>,
    record_version: i64,
    payload_size: i64,
    payload: Option<Vec<u8>>,
}

fn load_session_by_id(
    connection: &Connection,
    session_id: &SessionId,
    max_record_bytes: usize,
) -> Result<Option<Session>, DatabaseError> {
    let record_limit = sql_usize(max_record_bytes, "record byte limit exceeds SQLite range")?;
    let row = connection
        .query_row(
            "SELECT id, name, revision, record_version, length(payload), \
             CASE WHEN length(payload) <= ?2 THEN payload ELSE NULL END \
             FROM sessions WHERE id = ?1",
            params![session_id.as_str(), record_limit],
            read_session_row,
        )
        .optional()?;
    row.map(|row| decode_session_row(row, max_record_bytes))
        .transpose()
}

fn read_session_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionRow> {
    Ok(SessionRow {
        id: row.get(0)?,
        name: row.get(1)?,
        revision: row.get(2)?,
        record_version: row.get(3)?,
        payload_size: row.get(4)?,
        payload: row.get(5)?,
    })
}

/// Validates redundant indexes and decodes a payload that SQLite returned only
/// after its byte length satisfied the configured bound.
fn decode_session_row(row: SessionRow, max_record_bytes: usize) -> Result<Session, DatabaseError> {
    let record_version = u32::try_from(row.record_version)
        .map_err(|_| DatabaseError::InvalidData("session row record version is invalid"))?;
    if !is_supported_session_record_version(record_version) {
        return Err(DatabaseError::InvalidData(
            "session row record version is unsupported",
        ));
    }
    let payload_size = usize::try_from(row.payload_size)
        .map_err(|_| DatabaseError::InvalidData("session payload size is invalid"))?;
    if payload_size > max_record_bytes {
        return Err(DatabaseError::Codec(SessionCodecError::RecordTooLarge {
            actual: payload_size,
            limit: max_record_bytes,
        }));
    }
    let payload = row.payload.ok_or(DatabaseError::InvalidData(
        "bounded session payload was not returned",
    ))?;
    if payload.len() != payload_size {
        return Err(DatabaseError::InvalidData(
            "session payload length does not match SQLite metadata",
        ));
    }
    let session = decode_session(&payload, max_record_bytes, record_version)?;
    let revision = decode_u64(row.revision)?;
    if session.id().as_str() != row.id
        || session.name().as_str() != row.name
        || session.revision().value() != revision
    {
        return Err(DatabaseError::InvalidData(
            "session indexes do not match aggregate payload",
        ));
    }
    Ok(session)
}

struct EventRow {
    sequence: Vec<u8>,
    session_id: String,
    revision: Vec<u8>,
    kind: i64,
    occurred_at: i64,
    record_version: i64,
}

fn decode_event_row(row: EventRow) -> Result<SessionOutboxRecord, DatabaseError> {
    if row.record_version != OUTBOX_RECORD_VERSION {
        return Err(DatabaseError::InvalidData(
            "outbox record version is unsupported",
        ));
    }
    let sequence = OutboxSequence::new(decode_u64(row.sequence)?)
        .ok_or(DatabaseError::InvalidData("outbox sequence is zero"))?;
    let kind = u16::try_from(row.kind)
        .ok()
        .and_then(decode_event_kind)
        .ok_or(DatabaseError::InvalidData("outbox event kind is unknown"))?;
    Ok(SessionOutboxRecord {
        snapshot: None,
        sequence,
        event: SessionOutboxEvent {
            kind,
            session_id: SessionId::new(row.session_id)
                .map_err(|_| DatabaseError::InvalidData("outbox session ID is invalid"))?,
            revision: Revision::new(decode_u64(row.revision)?),
            occurred_at: undercurrent_domain::TimestampMillis::new(row.occurred_at),
        },
    })
}

const fn encode_event_kind(kind: DomainEventKind) -> u16 {
    match kind {
        DomainEventKind::SessionCreated => 1,
        DomainEventKind::SessionRenamed => 2,
        DomainEventKind::SessionConfigurationChanged => 3,
        DomainEventKind::SessionRemoved => 4,
        DomainEventKind::LifecycleChanged => 5,
        DomainEventKind::ProcessOwnershipChanged => 6,
        DomainEventKind::ForegroundChanged => 7,
        DomainEventKind::ForegroundEvidenceChanged => 8,
        DomainEventKind::HealthChanged => 9,
        DomainEventKind::HealthEvidenceChanged => 10,
        DomainEventKind::WorkContextChanged => 11,
        DomainEventKind::RestartScheduled => 12,
        DomainEventKind::RestartCancelled => 13,
        DomainEventKind::RestartStateChanged => 14,
        DomainEventKind::RestartExhausted => 15,
        DomainEventKind::TerminalResized => 16,
    }
}

const fn decode_event_kind(value: u16) -> Option<DomainEventKind> {
    match value {
        1 => Some(DomainEventKind::SessionCreated),
        2 => Some(DomainEventKind::SessionRenamed),
        3 => Some(DomainEventKind::SessionConfigurationChanged),
        4 => Some(DomainEventKind::SessionRemoved),
        5 => Some(DomainEventKind::LifecycleChanged),
        6 => Some(DomainEventKind::ProcessOwnershipChanged),
        7 => Some(DomainEventKind::ForegroundChanged),
        8 => Some(DomainEventKind::ForegroundEvidenceChanged),
        9 => Some(DomainEventKind::HealthChanged),
        10 => Some(DomainEventKind::HealthEvidenceChanged),
        11 => Some(DomainEventKind::WorkContextChanged),
        12 => Some(DomainEventKind::RestartScheduled),
        13 => Some(DomainEventKind::RestartCancelled),
        14 => Some(DomainEventKind::RestartStateChanged),
        15 => Some(DomainEventKind::RestartExhausted),
        16 => Some(DomainEventKind::TerminalResized),
        _ => None,
    }
}

fn encode_u64(value: u64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

fn decode_u64(bytes: Vec<u8>) -> Result<u64, DatabaseError> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| DatabaseError::InvalidData("unsigned integer blob must have eight bytes"))?;
    Ok(u64::from_be_bytes(bytes))
}

#[cfg(test)]
mod runtime_policy_record_debug_tests {
    use super::*;

    #[test]
    fn schema_normalization_preserves_sql_token_boundaries() {
        assert_eq!(
            normalize_schema_sql("  accepted_at\n INTEGER   NOT NULL  "),
            "accepted_at INTEGER NOT NULL"
        );
        assert_ne!(
            normalize_schema_sql("accepted_at INTEGER NOT NULL"),
            normalize_schema_sql("accepted_at INTEGERNOT NULL")
        );
    }
}
