// SPDX-License-Identifier: Apache-2.0

//! Durable runtime-cancellation admission, effect markers, and status records.
//!
//! Admission couples a control with any required Stopping aggregate/outbox
//! change. Before process I/O, a separate transaction changes raw state 0 to 4;
//! raw 4 decodes as Unknown so restart cannot blindly repeat an uncertain effect.
//! Terminal session commits settle eligible raw 0/4 controls atomically. Raw 3
//! unknown controls remain unknown and are not automatically redriven.
//! Authorization, process termination, and provider selection belong upstream.

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use undercurrent_application::{
    CancelRuntimeAttemptCommand, GetRuntimeCancellationStatusQuery, RuntimeCancellationAdmission,
    RuntimeCancellationControl, RuntimeCancellationEffectStart, RuntimeCancellationMode,
    RuntimeCancellationPropagationPolicy, RuntimeCancellationRecovery, RuntimeCancellationResult,
    RuntimeCancellationSessionSnapshot, RuntimeCancellationSettlement, RuntimeCancellationState,
    RuntimeCancellationStatusLookup, SessionOutboxEvent,
};
use undercurrent_domain::{
    DispatchCellIdentity, EffectCertainty, FencingToken, LifecycleState, OperationAttemptId,
    OperationAttemptState, PayloadDigest, ProcessMutationKind, Revision, Session, SessionId,
    TimestampMillis,
};

use super::*;

const CANCELLATION_RECORD_VERSION: i64 = 1;

impl Database {
    pub(in crate::sqlite) fn admit_runtime_cancellation(
        &mut self,
        command: CancelRuntimeAttemptCommand,
        observed_at: TimestampMillis,
    ) -> Result<RuntimeCancellationAdmission, DatabaseError> {
        let max_record_bytes = self.limits.max_record_bytes;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;

        if let Some(existing) = load_cancellation(&transaction, command.control().control_id())? {
            return if existing.target() == command.target()
                && existing.control() == command.control()
            {
                Ok(RuntimeCancellationAdmission::Replay(Box::new(existing)))
            } else {
                Ok(RuntimeCancellationAdmission::IdentityConflict)
            };
        }

        if command.control().deadline() <= observed_at {
            return Ok(RuntimeCancellationAdmission::DeadlineExpired);
        }
        let idempotency_owner = transaction
            .query_row(
                "SELECT control_id FROM runtime_cancellations
                 WHERE source_product = ?1 AND tenant_id = ?2 AND idempotency_key = ?3",
                params![
                    command.control().source_product(),
                    command.control().tenant_id(),
                    command.control().idempotency_key(),
                ],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if idempotency_owner.is_some() {
            return Ok(RuntimeCancellationAdmission::IdentityConflict);
        }

        let Some(predecessor) = load_attempt(&transaction, command.target().attempt_id())? else {
            return Ok(RuntimeCancellationAdmission::TargetMismatch);
        };
        let predecessor_applied = matches!(
            predecessor.state(),
            OperationAttemptState::Settled {
                certainty: EffectCertainty::KnownApplied,
                ..
            }
        );
        let dispatch_matches = predecessor.dispatch_binding().is_some_and(|binding| {
            binding.request_digest() == Some(command.target().request_digest())
                && binding.caller_cell_identity() == command.target().caller_cell_identity()
        });
        if predecessor.kind() != ProcessMutationKind::Spawn
            || !predecessor_applied
            || predecessor.target_session() != Some(command.target().session_id())
            || predecessor.fencing_token() != command.target().fencing_token()
            || !dispatch_matches
        {
            return Ok(RuntimeCancellationAdmission::TargetMismatch);
        }
        let current = current_fence(
            &transaction,
            Some(command.target().session_id()),
            Some(command.target().caller_cell_identity()),
        )?;
        if !current.is_some_and(|(attempt_id, fence)| {
            attempt_id == *command.target().attempt_id()
                && fence == command.target().fencing_token()
        }) {
            return Ok(RuntimeCancellationAdmission::TargetMismatch);
        }

        let Some(mut session) = load_session_by_id(
            &transaction,
            command.target().session_id(),
            max_record_bytes,
        )?
        else {
            return Ok(RuntimeCancellationAdmission::TargetMismatch);
        };
        if session.revision() != command.target().expected_session_revision()
            || session.lifecycle_revision() != command.target().expected_lifecycle_revision()
        {
            return Ok(RuntimeCancellationAdmission::TargetMismatch);
        }

        let initial_state = if session.lifecycle().is_terminal() {
            RuntimeCancellationState::AlreadyTerminal(
                RuntimeCancellationSessionSnapshot::from_session(&session, observed_at),
            )
        } else {
            let expected_revision = session.revision();
            let changed_at =
                TimestampMillis::new(observed_at.value().max(session.updated_at().value()));
            if let Some(change) = session
                .transition_lifecycle(LifecycleState::Stopping, changed_at)
                .map_err(|_| {
                    DatabaseError::InvalidData(
                        "runtime cancellation could not enter the stopping lifecycle",
                    )
                })?
            {
                let event = SessionOutboxEvent {
                    kind: change.kind(),
                    session_id: session.id().clone(),
                    revision: change.revision(),
                    occurred_at: changed_at,
                };
                persist_session_change(
                    &transaction,
                    &session,
                    expected_revision,
                    &event,
                    self.limits,
                )?;
            }
            RuntimeCancellationState::AcceptedStopping(
                RuntimeCancellationSessionSnapshot::from_session(&session, changed_at),
            )
        };
        initial_state
            .validate()
            .map_err(|_| DatabaseError::InvalidData("initial cancellation state is invalid"))?;
        insert_cancellation(&transaction, &command, &initial_state, observed_at)?;
        transaction.commit()?;
        RuntimeCancellationResult::new(
            command.target().clone(),
            command.control().clone(),
            initial_state,
        )
        .map(|result| RuntimeCancellationAdmission::Admitted(Box::new(result)))
        .map_err(|_| DatabaseError::InvalidData("admitted cancellation result is invalid"))
    }

    pub(in crate::sqlite) fn mark_runtime_cancellation_effect_started(
        &mut self,
        control_id: OperationAttemptId,
        command_digest: PayloadDigest,
        observed_at: TimestampMillis,
    ) -> Result<RuntimeCancellationEffectStart, DatabaseError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(existing) = load_cancellation(&transaction, &control_id)? else {
            return Ok(RuntimeCancellationEffectStart::NotFound);
        };
        if existing.control().command_digest() != command_digest {
            return Ok(RuntimeCancellationEffectStart::Conflict);
        }
        let state_code = load_cancellation_state_code(&transaction, &control_id)?.ok_or(
            DatabaseError::InvalidData("runtime cancellation state disappeared"),
        )?;
        if state_code != 0 {
            return Ok(RuntimeCancellationEffectStart::Replay(Box::new(existing)));
        }
        let marker_observed_at = strictly_advanced_cancellation_observation(
            observed_at,
            existing.state().session().observed_at(),
        )?;
        let changed = transaction.execute(
            "UPDATE runtime_cancellations
             SET state = 4, observed_at_millis = ?1
             WHERE control_id = ?2 AND state = 0",
            params![marker_observed_at.value(), control_id.as_str()],
        )?;
        if changed != 1 {
            return Err(DatabaseError::InvalidData(
                "runtime cancellation effect marker affected an unexpected row count",
            ));
        }
        transaction.commit()?;
        Ok(RuntimeCancellationEffectStart::Started)
    }

    pub(in crate::sqlite) fn settle_runtime_cancellation(
        &mut self,
        control_id: OperationAttemptId,
        command_digest: PayloadDigest,
        mut state: RuntimeCancellationState,
    ) -> Result<RuntimeCancellationSettlement, DatabaseError> {
        state
            .validate()
            .map_err(|_| DatabaseError::InvalidData("cancellation settlement state is invalid"))?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(existing) = load_cancellation(&transaction, &control_id)? else {
            return Ok(RuntimeCancellationSettlement::NotFound);
        };
        let existing_state_code = load_cancellation_state_code(&transaction, &control_id)?.ok_or(
            DatabaseError::InvalidData("runtime cancellation state disappeared"),
        )?;
        if existing.control().command_digest() != command_digest
            || state.session().session_id() != existing.target().session_id()
        {
            return Ok(RuntimeCancellationSettlement::Conflict);
        }
        if existing_state_code != 4 && existing.state() == &state {
            return Ok(RuntimeCancellationSettlement::Duplicate(Box::new(existing)));
        }
        if terminal_snapshots_match(existing.state(), &state) {
            return Ok(RuntimeCancellationSettlement::Duplicate(Box::new(existing)));
        }
        let proposed_snapshot = state.session();
        let existing_snapshot = existing.state().session();
        let revision_regressed = proposed_snapshot.revision() < existing_snapshot.revision()
            || proposed_snapshot.lifecycle_revision() < existing_snapshot.lifecycle_revision();
        let lifecycle_changed = proposed_snapshot.lifecycle() != existing_snapshot.lifecycle();
        let lifecycle_regressed = cancellation_lifecycle_rank(proposed_snapshot.lifecycle())
            < cancellation_lifecycle_rank(existing_snapshot.lifecycle())
            || (lifecycle_changed
                && proposed_snapshot.lifecycle_revision()
                    <= existing_snapshot.lifecycle_revision());
        let observation_did_not_advance =
            proposed_snapshot.observed_at() <= existing_snapshot.observed_at();
        if existing_state_code == 4
            && matches!(state, RuntimeCancellationState::Unknown(_))
            && (revision_regressed || lifecycle_regressed || observation_did_not_advance)
        {
            state = RuntimeCancellationState::Unknown(existing_snapshot.clone());
        } else if revision_regressed || lifecycle_regressed {
            return Ok(RuntimeCancellationSettlement::Conflict);
        } else if existing_state_code == 3
            && matches!(state, RuntimeCancellationState::Unknown(_))
            && proposed_snapshot.observed_at() < existing_snapshot.observed_at()
        {
            return Ok(RuntimeCancellationSettlement::Duplicate(Box::new(existing)));
        }
        let transition_allowed = match existing_state_code {
            0 | 4 => !matches!(state, RuntimeCancellationState::AcceptedStopping(_)),
            3 => matches!(state, RuntimeCancellationState::Unknown(_)),
            1 | 2 => false,
            _ => {
                return Err(DatabaseError::InvalidData(
                    "runtime cancellation state is unknown",
                ));
            }
        };
        if !transition_allowed {
            return Ok(RuntimeCancellationSettlement::Conflict);
        }
        update_cancellation_state(&transaction, &control_id, &state)?;
        let settled = load_cancellation(&transaction, &control_id)?.ok_or(
            DatabaseError::InvalidData("settled cancellation row disappeared"),
        )?;
        transaction.commit()?;
        Ok(RuntimeCancellationSettlement::Settled(Box::new(settled)))
    }

    pub(in crate::sqlite) fn get_runtime_cancellation_status(
        &self,
        query: GetRuntimeCancellationStatusQuery,
    ) -> Result<RuntimeCancellationStatusLookup, DatabaseError> {
        let Some(result) = load_cancellation(&self.connection, query.control_id())? else {
            return Ok(RuntimeCancellationStatusLookup::NotFound);
        };
        let binding_matches = result.control().command_digest() == query.command_digest()
            && result.target().attempt_id() == query.target_attempt_id()
            && result.target().request_digest() == query.target_request_digest()
            && result.target().session_id() == query.session_id()
            && result.target().expected_provider_id() == query.expected_provider_id()
            && result.target().expected_provider_generation()
                == query.expected_provider_generation()
            && result.control().source_product() == query.actor().source_product()
            && result.control().tenant_id() == query.actor().tenant_id();
        if binding_matches {
            Ok(RuntimeCancellationStatusLookup::Found(Box::new(result)))
        } else {
            Ok(RuntimeCancellationStatusLookup::InconsistentBinding)
        }
    }

    pub(in crate::sqlite) fn claim_runtime_cancellation_recovery(
        &mut self,
        session_id: &SessionId,
        observed_at: TimestampMillis,
    ) -> Result<Option<RuntimeCancellationRecovery>, DatabaseError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let unknown_exists = transaction.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM runtime_cancellations
                WHERE session_id = ?1 AND state IN (3, 4)
             )",
            [session_id.as_str()],
            |row| row.get::<_, bool>(0),
        )?;
        if unknown_exists {
            return Ok(Some(RuntimeCancellationRecovery::PreserveUnknown));
        }
        let (mode, prior_observed_at): (Option<i64>, Option<i64>) = transaction.query_row(
            "SELECT MAX(mode), MAX(observed_at_millis)
             FROM runtime_cancellations
             WHERE session_id = ?1 AND state = 0",
            [session_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let (Some(mode), Some(prior_observed_at)) = (mode, prior_observed_at) else {
            return Ok(None);
        };
        let marker_observed_at = strictly_advanced_cancellation_observation(
            observed_at,
            TimestampMillis::new(prior_observed_at),
        )?;
        // Mark every accepted control as potentially affected by the resumed
        // stop. Returning Resume before commit would let another recovery claim
        // the same unstarted controls after response loss.
        // See [restart ordering](../../../docs/internals/transactions/runtime-cancellation.md#restart-and-response-loss).
        let changed = transaction.execute(
            "UPDATE runtime_cancellations
             SET state = 4, observed_at_millis = ?1
             WHERE session_id = ?2 AND state = 0",
            params![marker_observed_at.value(), session_id.as_str()],
        )?;
        if changed == 0 {
            return Err(DatabaseError::InvalidData(
                "runtime cancellation recovery claim affected an unexpected row count",
            ));
        }
        let mode = decode_mode(mode)?;
        transaction.commit()?;
        Ok(Some(RuntimeCancellationRecovery::Resume(mode)))
    }
}

fn strictly_advanced_cancellation_observation(
    observed_at: TimestampMillis,
    prior_observed_at: TimestampMillis,
) -> Result<TimestampMillis, DatabaseError> {
    let next = prior_observed_at
        .value()
        .checked_add(1)
        .ok_or(DatabaseError::InvalidData(
            "runtime cancellation observation timestamp cannot advance",
        ))?;
    Ok(TimestampMillis::new(observed_at.value().max(next)))
}

const fn cancellation_lifecycle_rank(lifecycle: LifecycleState) -> u8 {
    match lifecycle {
        LifecycleState::Starting => 0,
        LifecycleState::Running => 1,
        LifecycleState::Stopping => 2,
        LifecycleState::Stopped | LifecycleState::Exited(_) | LifecycleState::Failed => 3,
    }
}

// A terminal session commit may settle the control before the service replies.
// Match the durable session revisions/lifecycle, allowing a later observation
// time or the other terminal label to acknowledge that same completed state.
// See [settlement ordering](../../../docs/internals/transactions/runtime-cancellation.md#settlement-ordering).
fn terminal_snapshots_match(
    existing: &RuntimeCancellationState,
    proposed: &RuntimeCancellationState,
) -> bool {
    if !matches!(
        existing,
        RuntimeCancellationState::AppliedTerminated(_)
            | RuntimeCancellationState::AlreadyTerminal(_)
    ) || !matches!(
        proposed,
        RuntimeCancellationState::AppliedTerminated(_)
            | RuntimeCancellationState::AlreadyTerminal(_)
    ) {
        return false;
    }
    let existing = existing.session();
    let proposed = proposed.session();
    existing.session_id() == proposed.session_id()
        && existing.lifecycle() == proposed.lifecycle()
        && existing.revision() == proposed.revision()
        && existing.lifecycle_revision() == proposed.lifecycle_revision()
}

fn persist_session_change(
    transaction: &Transaction<'_>,
    session: &Session,
    expected_revision: Revision,
    event: &SessionOutboxEvent,
    limits: SqliteRepositoryLimits,
) -> Result<(), DatabaseError> {
    validate_commit_events(session, expected_revision, std::slice::from_ref(event))?;
    let payload =
        encode_session(session, limits.max_record_bytes).map_err(DatabaseError::from_encoding)?;
    let state = read_repository_state(transaction)?;
    if state.outbox_count >= limits.max_outbox_events {
        return Err(DatabaseError::Capacity("outbox"));
    }
    let next_sequence = append_events(
        transaction,
        state.next_sequence,
        std::slice::from_ref(event),
        &payload,
    )?;
    let changed = transaction.execute(
        "UPDATE sessions SET name = ?1, revision = ?2, updated_at = ?3,
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
            "cancellation session transition affected an unexpected row count",
        ));
    }
    update_repository_counts(
        transaction,
        state.session_count,
        state.outbox_count.saturating_add(1),
        next_sequence,
        state.last_acknowledged,
    )
}

fn insert_cancellation(
    transaction: &Transaction<'_>,
    command: &CancelRuntimeAttemptCommand,
    state: &RuntimeCancellationState,
    admitted_at: TimestampMillis,
) -> Result<(), DatabaseError> {
    let (state_code, snapshot) = encode_state(state);
    let (lifecycle, exit_code) = encode_lifecycle(snapshot.lifecycle());
    transaction.execute(
        "INSERT INTO runtime_cancellations (
            control_id, command_digest, product_request_fingerprint, idempotency_key,
            target_attempt_id, target_request_digest, target_fencing_token,
            caller_cell_identity, session_id, expected_session_revision,
            expected_lifecycle_revision, expected_provider_id, expected_provider_generation,
            mode, reason, propagation_policy, deadline_millis, source_product, tenant_id,
            actor_context_ref, authority_ref, state, session_lifecycle, session_exit_code,
            observed_session_revision, observed_lifecycle_revision, observed_at_millis,
            admitted_at_millis, record_version
         ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15,
            ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29
         )",
        params![
            command.control().control_id().as_str(),
            command.control().command_digest().to_hex(),
            command.control().product_request_fingerprint().to_hex(),
            command.control().idempotency_key(),
            command.target().attempt_id().as_str(),
            command.target().request_digest().to_hex(),
            encode_u64(command.target().fencing_token().value()),
            command.target().caller_cell_identity().as_str(),
            command.target().session_id().as_str(),
            encode_u64(command.target().expected_session_revision().value()),
            encode_u64(command.target().expected_lifecycle_revision().value()),
            command.target().expected_provider_id(),
            encode_u64(command.target().expected_provider_generation()),
            encode_mode(command.control().mode()),
            command.control().reason(),
            encode_propagation(command.control().propagation_policy()),
            command.control().deadline().value(),
            command.control().source_product(),
            command.control().tenant_id(),
            command.control().actor_context_ref(),
            command.control().authority_ref(),
            state_code,
            lifecycle,
            exit_code,
            encode_u64(snapshot.revision().value()),
            encode_u64(snapshot.lifecycle_revision().value()),
            snapshot.observed_at().value(),
            admitted_at.value(),
            CANCELLATION_RECORD_VERSION,
        ],
    )?;
    Ok(())
}

fn update_cancellation_state(
    transaction: &Transaction<'_>,
    control_id: &OperationAttemptId,
    state: &RuntimeCancellationState,
) -> Result<(), DatabaseError> {
    let (state_code, snapshot) = encode_state(state);
    let (lifecycle, exit_code) = encode_lifecycle(snapshot.lifecycle());
    let changed = transaction.execute(
        "UPDATE runtime_cancellations
         SET state = ?1, session_lifecycle = ?2, session_exit_code = ?3,
             observed_session_revision = ?4, observed_lifecycle_revision = ?5,
             observed_at_millis = MAX(observed_at_millis, ?6)
         WHERE control_id = ?7",
        params![
            state_code,
            lifecycle,
            exit_code,
            encode_u64(snapshot.revision().value()),
            encode_u64(snapshot.lifecycle_revision().value()),
            snapshot.observed_at().value(),
            control_id.as_str(),
        ],
    )?;
    if changed != 1 {
        return Err(DatabaseError::InvalidData(
            "cancellation settlement affected an unexpected row count",
        ));
    }
    Ok(())
}

pub(super) fn settle_accepted_for_terminal_session(
    transaction: &Transaction<'_>,
    session: &Session,
    observed_at: TimestampMillis,
) -> Result<(), DatabaseError> {
    if !session.lifecycle().is_terminal() {
        return Ok(());
    }
    let (lifecycle, exit_code) = encode_lifecycle(session.lifecycle());
    transaction.execute(
        "UPDATE runtime_cancellations
         SET state = 1, session_lifecycle = ?1, session_exit_code = ?2,
             observed_session_revision = ?3, observed_lifecycle_revision = ?4,
             observed_at_millis = MAX(observed_at_millis, ?5)
         WHERE session_id = ?6 AND state IN (0, 4)
           AND observed_session_revision <= ?3
           AND observed_lifecycle_revision <= ?4",
        params![
            lifecycle,
            exit_code,
            encode_u64(session.revision().value()),
            encode_u64(session.lifecycle_revision().value()),
            observed_at.value(),
            session.id().as_str(),
        ],
    )?;
    Ok(())
}

#[derive(Debug)]
struct CancellationRow {
    control_id: String,
    command_digest: String,
    product_request_fingerprint: String,
    idempotency_key: String,
    target_attempt_id: String,
    target_request_digest: String,
    target_fencing_token: Vec<u8>,
    caller_cell_identity: String,
    session_id: String,
    expected_session_revision: Vec<u8>,
    expected_lifecycle_revision: Vec<u8>,
    expected_provider_id: String,
    expected_provider_generation: Vec<u8>,
    mode: i64,
    reason: String,
    propagation_policy: i64,
    deadline_millis: i64,
    source_product: String,
    tenant_id: String,
    actor_context_ref: String,
    authority_ref: String,
    state: i64,
    session_lifecycle: i64,
    session_exit_code: Option<i64>,
    observed_session_revision: Vec<u8>,
    observed_lifecycle_revision: Vec<u8>,
    observed_at_millis: i64,
    record_version: i64,
}

fn load_cancellation(
    connection: &Connection,
    control_id: &OperationAttemptId,
) -> Result<Option<RuntimeCancellationResult>, DatabaseError> {
    let row = connection
        .query_row(
            "SELECT control_id, command_digest, product_request_fingerprint, idempotency_key,
                    target_attempt_id, target_request_digest, target_fencing_token,
                    caller_cell_identity, session_id, expected_session_revision,
                    expected_lifecycle_revision, expected_provider_id,
                    expected_provider_generation, mode, reason, propagation_policy,
                    deadline_millis, source_product, tenant_id, actor_context_ref, authority_ref,
                    state, session_lifecycle, session_exit_code, observed_session_revision,
                    observed_lifecycle_revision, observed_at_millis, record_version
             FROM runtime_cancellations WHERE control_id = ?1",
            [control_id.as_str()],
            |row| {
                Ok(CancellationRow {
                    control_id: row.get(0)?,
                    command_digest: row.get(1)?,
                    product_request_fingerprint: row.get(2)?,
                    idempotency_key: row.get(3)?,
                    target_attempt_id: row.get(4)?,
                    target_request_digest: row.get(5)?,
                    target_fencing_token: row.get(6)?,
                    caller_cell_identity: row.get(7)?,
                    session_id: row.get(8)?,
                    expected_session_revision: row.get(9)?,
                    expected_lifecycle_revision: row.get(10)?,
                    expected_provider_id: row.get(11)?,
                    expected_provider_generation: row.get(12)?,
                    mode: row.get(13)?,
                    reason: row.get(14)?,
                    propagation_policy: row.get(15)?,
                    deadline_millis: row.get(16)?,
                    source_product: row.get(17)?,
                    tenant_id: row.get(18)?,
                    actor_context_ref: row.get(19)?,
                    authority_ref: row.get(20)?,
                    state: row.get(21)?,
                    session_lifecycle: row.get(22)?,
                    session_exit_code: row.get(23)?,
                    observed_session_revision: row.get(24)?,
                    observed_lifecycle_revision: row.get(25)?,
                    observed_at_millis: row.get(26)?,
                    record_version: row.get(27)?,
                })
            },
        )
        .optional()?;
    row.map(decode_cancellation).transpose()
}

fn load_cancellation_state_code(
    connection: &Connection,
    control_id: &OperationAttemptId,
) -> Result<Option<i64>, DatabaseError> {
    connection
        .query_row(
            "SELECT state FROM runtime_cancellations WHERE control_id = ?1",
            [control_id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(DatabaseError::from)
}

fn decode_cancellation(row: CancellationRow) -> Result<RuntimeCancellationResult, DatabaseError> {
    if row.record_version != CANCELLATION_RECORD_VERSION {
        return Err(DatabaseError::InvalidData(
            "runtime cancellation record version is unsupported",
        ));
    }
    let session_id = SessionId::new(row.session_id)
        .map_err(|_| DatabaseError::InvalidData("cancellation session ID is invalid"))?;
    if !is_valid_provider_id(&row.expected_provider_id) {
        return Err(DatabaseError::InvalidData(
            "cancellation expected provider ID is invalid",
        ));
    }
    if !is_valid_tenant_id(&row.tenant_id) {
        return Err(DatabaseError::InvalidData(
            "cancellation tenant ID is invalid",
        ));
    }
    let target = undercurrent_application::RuntimeCancellationTarget::new(
        OperationAttemptId::new(row.target_attempt_id)
            .map_err(|_| DatabaseError::InvalidData("cancellation target attempt ID is invalid"))?,
        PayloadDigest::from_hex(&row.target_request_digest)
            .map_err(|_| DatabaseError::InvalidData("cancellation target digest is invalid"))?,
        FencingToken::new(decode_u64(row.target_fencing_token)?)
            .map_err(|_| DatabaseError::InvalidData("cancellation target fence is invalid"))?,
        DispatchCellIdentity::new(row.caller_cell_identity)
            .map_err(|_| DatabaseError::InvalidData("cancellation caller cell is invalid"))?,
        session_id.clone(),
        Revision::new(decode_u64(row.expected_session_revision)?),
        Revision::new(decode_u64(row.expected_lifecycle_revision)?),
        row.expected_provider_id,
        decode_u64(row.expected_provider_generation)?,
    )
    .map_err(|_| DatabaseError::InvalidData("cancellation target binding is invalid"))?;
    let control = RuntimeCancellationControl::new(
        OperationAttemptId::new(row.control_id)
            .map_err(|_| DatabaseError::InvalidData("cancellation control ID is invalid"))?,
        PayloadDigest::from_hex(&row.command_digest)
            .map_err(|_| DatabaseError::InvalidData("cancellation command digest is invalid"))?,
        PayloadDigest::from_hex(&row.product_request_fingerprint).map_err(|_| {
            DatabaseError::InvalidData("cancellation request fingerprint is invalid")
        })?,
        row.idempotency_key,
        decode_mode(row.mode)?,
        row.reason,
        decode_propagation(row.propagation_policy)?,
        TimestampMillis::new(row.deadline_millis),
        row.source_product,
        row.tenant_id,
        row.actor_context_ref,
        row.authority_ref,
    )
    .map_err(|_| DatabaseError::InvalidData("cancellation control binding is invalid"))?;
    let snapshot = RuntimeCancellationSessionSnapshot::new(
        session_id,
        decode_lifecycle(row.session_lifecycle, row.session_exit_code)?,
        Revision::new(decode_u64(row.observed_session_revision)?),
        Revision::new(decode_u64(row.observed_lifecycle_revision)?),
        TimestampMillis::new(row.observed_at_millis),
    )
    .map_err(|_| DatabaseError::InvalidData("cancellation session snapshot is invalid"))?;
    let state = match row.state {
        0 => RuntimeCancellationState::AcceptedStopping(snapshot),
        1 => RuntimeCancellationState::AppliedTerminated(snapshot),
        2 => RuntimeCancellationState::AlreadyTerminal(snapshot),
        3 | 4 => RuntimeCancellationState::Unknown(snapshot),
        _ => return Err(DatabaseError::InvalidData("cancellation state is unknown")),
    };
    RuntimeCancellationResult::new(target, control, state)
        .map_err(|_| DatabaseError::InvalidData("cancellation result is incoherent"))
}

fn is_valid_provider_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 40
        || &bytes[..4] != b"prv_"
        || bytes[12] != b'-'
        || bytes[17] != b'-'
        || bytes[18] != b'7'
        || bytes[22] != b'-'
        || !matches!(bytes[23], b'8' | b'9' | b'a' | b'b')
        || bytes[27] != b'-'
    {
        return false;
    }
    bytes.iter().enumerate().all(|(index, byte)| {
        matches!(index, 0..=3 | 12 | 17 | 22 | 27)
            || byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()
    })
}

fn is_valid_tenant_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 43
        || &bytes[..7] != b"tenant_"
        || bytes[15] != b'-'
        || bytes[20] != b'-'
        || bytes[21] != b'7'
        || bytes[25] != b'-'
        || !matches!(bytes[26], b'8' | b'9' | b'a' | b'b')
        || bytes[30] != b'-'
    {
        return false;
    }
    bytes.iter().enumerate().all(|(index, byte)| {
        matches!(index, 0..=6 | 15 | 20 | 25 | 30)
            || byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()
    })
}

const fn encode_mode(mode: RuntimeCancellationMode) -> i64 {
    match mode {
        RuntimeCancellationMode::Graceful => 0,
        RuntimeCancellationMode::Force => 1,
    }
}

fn decode_mode(value: i64) -> Result<RuntimeCancellationMode, DatabaseError> {
    match value {
        0 => Ok(RuntimeCancellationMode::Graceful),
        1 => Ok(RuntimeCancellationMode::Force),
        _ => Err(DatabaseError::InvalidData("cancellation mode is unknown")),
    }
}

const fn encode_propagation(policy: RuntimeCancellationPropagationPolicy) -> i64 {
    match policy {
        RuntimeCancellationPropagationPolicy::TargetOnly => 0,
    }
}

fn decode_propagation(value: i64) -> Result<RuntimeCancellationPropagationPolicy, DatabaseError> {
    match value {
        0 => Ok(RuntimeCancellationPropagationPolicy::TargetOnly),
        _ => Err(DatabaseError::InvalidData(
            "cancellation propagation policy is unknown",
        )),
    }
}

const fn encode_state(
    state: &RuntimeCancellationState,
) -> (i64, &RuntimeCancellationSessionSnapshot) {
    match state {
        RuntimeCancellationState::AcceptedStopping(snapshot) => (0, snapshot),
        RuntimeCancellationState::AppliedTerminated(snapshot) => (1, snapshot),
        RuntimeCancellationState::AlreadyTerminal(snapshot) => (2, snapshot),
        RuntimeCancellationState::Unknown(snapshot) => (3, snapshot),
    }
}

const fn encode_lifecycle(lifecycle: LifecycleState) -> (i64, Option<i64>) {
    match lifecycle {
        LifecycleState::Starting => (0, None),
        LifecycleState::Running => (1, None),
        LifecycleState::Stopping => (2, None),
        LifecycleState::Stopped => (3, None),
        LifecycleState::Exited(code) => (4, Some(code as i64)),
        LifecycleState::Failed => (5, None),
    }
}

fn decode_lifecycle(
    lifecycle: i64,
    exit_code: Option<i64>,
) -> Result<LifecycleState, DatabaseError> {
    match (lifecycle, exit_code) {
        (0, None) => Ok(LifecycleState::Starting),
        (1, None) => Ok(LifecycleState::Running),
        (2, None) => Ok(LifecycleState::Stopping),
        (3, None) => Ok(LifecycleState::Stopped),
        (4, Some(code)) => i32::try_from(code)
            .map(LifecycleState::Exited)
            .map_err(|_| DatabaseError::InvalidData("cancellation exit code is invalid")),
        (5, None) => Ok(LifecycleState::Failed),
        _ => Err(DatabaseError::InvalidData(
            "cancellation lifecycle encoding is invalid",
        )),
    }
}

pub(super) fn validate_runtime_cancellation_consistency(
    connection: &Connection,
) -> Result<(), DatabaseError> {
    let invalid: i64 = connection.query_row(
        "SELECT COUNT(*)
         FROM runtime_cancellations cancellation
         LEFT JOIN operation_attempts predecessor
           ON predecessor.attempt_id = cancellation.target_attempt_id
         WHERE predecessor.attempt_id IS NULL
            OR predecessor.mutation_kind != 'spawn'
            OR predecessor.target_session != cancellation.session_id
            OR predecessor.fencing_token != cancellation.target_fencing_token
            OR predecessor.request_digest != cancellation.target_request_digest
            OR predecessor.dispatch_cell_identity != cancellation.caller_cell_identity
            OR predecessor.state != 2
            OR cancellation.observed_session_revision
               < cancellation.expected_session_revision
            OR cancellation.observed_lifecycle_revision
               < cancellation.expected_lifecycle_revision
            OR cancellation.state NOT IN (0, 1, 2, 3, 4)
            OR length(cancellation.expected_provider_id) != 40
            OR substr(cancellation.expected_provider_id, 1, 4) != 'prv_'
            OR substr(cancellation.expected_provider_id, 5, 8) GLOB '*[^0-9a-f]*'
            OR substr(cancellation.expected_provider_id, 13, 1) != '-'
            OR substr(cancellation.expected_provider_id, 14, 4) GLOB '*[^0-9a-f]*'
            OR substr(cancellation.expected_provider_id, 18, 1) != '-'
            OR substr(cancellation.expected_provider_id, 19, 1) != '7'
            OR substr(cancellation.expected_provider_id, 19, 4) GLOB '*[^0-9a-f]*'
            OR substr(cancellation.expected_provider_id, 23, 1) != '-'
            OR substr(cancellation.expected_provider_id, 24, 1) NOT GLOB '[89ab]'
            OR substr(cancellation.expected_provider_id, 24, 4) GLOB '*[^0-9a-f]*'
            OR substr(cancellation.expected_provider_id, 28, 1) != '-'
            OR substr(cancellation.expected_provider_id, 29, 12) GLOB '*[^0-9a-f]*'
            OR length(cancellation.tenant_id) != 43
            OR substr(cancellation.tenant_id, 1, 7) != 'tenant_'
            OR substr(cancellation.tenant_id, 8, 8) GLOB '*[^0-9a-f]*'
            OR substr(cancellation.tenant_id, 16, 1) != '-'
            OR substr(cancellation.tenant_id, 17, 4) GLOB '*[^0-9a-f]*'
            OR substr(cancellation.tenant_id, 21, 1) != '-'
            OR substr(cancellation.tenant_id, 22, 1) != '7'
            OR substr(cancellation.tenant_id, 22, 4) GLOB '*[^0-9a-f]*'
            OR substr(cancellation.tenant_id, 26, 1) != '-'
            OR substr(cancellation.tenant_id, 27, 1) NOT GLOB '[89ab]'
            OR substr(cancellation.tenant_id, 27, 4) GLOB '*[^0-9a-f]*'
            OR substr(cancellation.tenant_id, 31, 1) != '-'
            OR substr(cancellation.tenant_id, 32, 12) GLOB '*[^0-9a-f]*'",
        [],
        |row| row.get(0),
    )?;
    if invalid != 0 {
        return Err(DatabaseError::InvalidData(
            "runtime cancellation predecessor binding is inconsistent",
        ));
    }

    let control_ids = {
        let mut statement = connection
            .prepare("SELECT control_id FROM runtime_cancellations ORDER BY control_id")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect::<Result<Vec<_>, _>>()?
    };
    for control_id in control_ids {
        let control_id = OperationAttemptId::new(control_id).map_err(|_| {
            DatabaseError::InvalidData("runtime cancellation control identity is invalid")
        })?;
        let result = load_cancellation(connection, &control_id)?.ok_or(
            DatabaseError::InvalidData("runtime cancellation row disappeared during validation"),
        )?;
        result.validate().map_err(|_| {
            DatabaseError::InvalidData("runtime cancellation retained fields are invalid")
        })?;
    }
    Ok(())
}
