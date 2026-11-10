// SPDX-License-Identifier: Apache-2.0

//! Fenced agent event/reply retention with per-attempt request deduplication.
//!
//! Enqueue and reply are immediate transactions; polls do not consume events.
//! Binding expiry and retained target fields are checked before replay lookup.
//! Payload admission checks JSON and exact byte digests, not canonicalization or
//! semantic result schemas. Upstream owns protected-peer authentication and
//! live process binding; this module does not inspect the process runtime.

use rusqlite::{OptionalExtension, TransactionBehavior, params};
use undercurrent_application::{
    AgentChannelTarget, AgentChannelTargetResolution, AgentEventAdmission,
    AgentEventAdmissionOutcome, AgentEventPollOutcome, AgentEventRecord, AgentEventReplyAdmission,
    AgentEventReplyOutcome, AgentEventReplyRecord, MAX_UNSETTLED_AGENT_EVENTS_PER_ATTEMPT,
    ResolvedAgentChannelBinding,
};
use undercurrent_domain::{
    OperationAttempt, PayloadDigest, ProcessInstanceId, SessionId, TimestampMillis,
};

use super::{
    Database, DatabaseError, MAX_AGENT_CHANNEL_RECORD_BYTES, current_fence, decode_u64, encode_u64,
    load_attempt,
};

impl Database {
    pub(crate) fn resolve_agent_channel_target(
        &mut self,
        session_id: SessionId,
        process_instance_id: ProcessInstanceId,
        observed_at: TimestampMillis,
    ) -> Result<AgentChannelTargetResolution, DatabaseError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)?;
        let candidates = {
            // Historical rows must not consume the ambiguity probe. Match the
            // same cell-wide ordering as current_fence before taking two current
            // candidates; two are sufficient to reject an ambiguous binding.
            let mut statement = transaction.prepare(
                "SELECT binding.attempt_id
                 FROM agent_runtime_bindings binding
                 JOIN operation_attempts attempt ON attempt.attempt_id = binding.attempt_id
                 WHERE attempt.target_session = ?1
                   AND attempt.attempt_id = (
                       SELECT latest.attempt_id FROM operation_attempts latest
                       WHERE (attempt.dispatch_cell_identity IS NOT NULL
                              AND latest.dispatch_cell_identity = attempt.dispatch_cell_identity)
                          OR (attempt.dispatch_cell_identity IS NULL
                              AND latest.dispatch_cell_identity IS NULL
                              AND latest.target_session IS attempt.target_session)
                       ORDER BY latest.fencing_token DESC, latest.attempt_id DESC
                       LIMIT 1
                   )
                 ORDER BY attempt.fencing_token DESC, binding.attempt_id DESC
                 LIMIT 2",
            )?;
            statement
                .query_map([session_id.as_str()], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut current = Vec::new();
        for attempt_id in candidates {
            let attempt_id =
                undercurrent_domain::OperationAttemptId::new(attempt_id).map_err(|_| {
                    DatabaseError::InvalidData("agent binding attempt identity is invalid")
                })?;
            let attempt = load_attempt(&transaction, &attempt_id)?.ok_or(
                DatabaseError::InvalidData("agent binding references a missing attempt"),
            )?;
            let Some(dispatch) = attempt.dispatch_binding() else {
                return Err(DatabaseError::InvalidData(
                    "agent binding attempt lacks a dispatch binding",
                ));
            };
            let Some(binding) = dispatch.agent() else {
                return Err(DatabaseError::InvalidData(
                    "agent binding attempt lacks its retained binding",
                ));
            };
            let latest = current_fence(
                &transaction,
                attempt.target_session(),
                Some(dispatch.caller_cell_identity()),
            )?;
            if latest.as_ref().is_some_and(|(latest_id, latest_fence)| {
                latest_id == attempt.id() && *latest_fence == attempt.fencing_token()
            }) {
                current.push(AgentChannelTarget {
                    tenant_id: binding.tenant_id().to_owned(),
                    source_product: binding.source_product().to_owned(),
                    actor_context_ref: binding.actor_context_ref().to_owned(),
                    authority_ref: binding.authority_ref().to_owned(),
                    undercurrent_provider_id: binding.undercurrent_provider_id().to_owned(),
                    undercurrent_provider_generation: binding.undercurrent_provider_generation(),
                    session_id: session_id.clone(),
                    process_instance_id: Some(process_instance_id.clone()),
                    attempt_id,
                    attempt_revision: binding.attempt_revision(),
                    recovery_epoch: binding.recovery_epoch(),
                    fence: attempt.fencing_token(),
                    request_digest: binding.request_digest(),
                });
            }
        }
        let outcome = match current.len() {
            0 => AgentChannelTargetResolution::NotFound,
            1 => {
                let target = current.pop().ok_or(DatabaseError::InvalidData(
                    "resolved agent target disappeared",
                ))?;
                let attempt = load_attempt(&transaction, &target.attempt_id)?.ok_or(
                    DatabaseError::InvalidData("resolved agent attempt disappeared"),
                )?;
                let expires_at = attempt
                    .dispatch_binding()
                    .and_then(undercurrent_domain::RuntimeDispatchBinding::agent)
                    .map(undercurrent_domain::AgentRuntimeBinding::expires_at)
                    .ok_or(DatabaseError::InvalidData(
                        "resolved agent binding disappeared",
                    ))?;
                if expires_at <= observed_at {
                    AgentChannelTargetResolution::BindingExpired
                } else {
                    let schema = attempt
                        .dispatch_binding()
                        .and_then(undercurrent_domain::RuntimeDispatchBinding::agent)
                        .map(|binding| binding.expected_result_schema_canonical_json().to_vec())
                        .ok_or(DatabaseError::InvalidData(
                            "resolved agent result schema disappeared",
                        ))?;
                    AgentChannelTargetResolution::Current(Box::new(ResolvedAgentChannelBinding {
                        target,
                        expected_result_schema_canonical_json: schema,
                        expires_at,
                    }))
                }
            }
            _ => AgentChannelTargetResolution::Ambiguous,
        };
        transaction.commit()?;
        Ok(outcome)
    }

    pub(crate) fn enqueue_agent_event(
        &mut self,
        admission: AgentEventAdmission,
    ) -> Result<AgentEventAdmissionOutcome, DatabaseError> {
        validate_event_payload(&admission.request_id, &admission.call_canonical_json)?;
        if PayloadDigest::of_bytes(&admission.call_canonical_json) != admission.call_digest {
            return Err(DatabaseError::InvalidData(
                "agent event call digest does not match its canonical payload",
            ));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let attempt = match validate_target(&transaction, &admission.target, admission.received_at)?
        {
            TargetValidation::Current(attempt) => attempt,
            TargetValidation::Stale => return Ok(AgentEventAdmissionOutcome::StaleTarget),
            TargetValidation::Expired => return Ok(AgentEventAdmissionOutcome::BindingExpired),
            TargetValidation::NotFound => return Ok(AgentEventAdmissionOutcome::NotFound),
        };
        let existing = transaction
            .query_row(
                "SELECT event_id, sequence, session_id, process_instance_id, call_digest,
                        call_json, received_at, reply_digest, reply_json, replied_at
                 FROM agent_events WHERE attempt_id = ?1 AND request_id = ?2",
                params![attempt.id().as_str(), admission.request_id],
                read_existing_event,
            )
            .optional()?;
        if let Some(existing) = existing {
            let record = decode_existing_event(&admission.target, existing)?;
            if record.event.call_digest != admission.call_digest
                || record.event.call_canonical_json != admission.call_canonical_json
                || record.event.target.process_instance_id != admission.target.process_instance_id
            {
                return Ok(AgentEventAdmissionOutcome::RequestConflict);
            }
            return Ok(match record.reply {
                Some(reply) => AgentEventAdmissionOutcome::DuplicateReplied(reply),
                None => AgentEventAdmissionOutcome::DuplicatePending(record.event),
            });
        }
        // Replays use retained evidence and consume no slot, so classify them
        // before enforcing capacity for a new request. Replying frees a slot
        // without deleting the event needed for later replay.
        // See [capacity and replay](../../../docs/internals/transactions/agent-channel.md#payload-and-capacity-limits).
        let unsettled: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM agent_events
             WHERE attempt_id = ?1 AND reply_digest IS NULL",
            [attempt.id().as_str()],
            |row| row.get(0),
        )?;
        if usize::try_from(unsettled).unwrap_or(usize::MAX)
            >= MAX_UNSETTLED_AGENT_EVENTS_PER_ATTEMPT
        {
            return Ok(AgentEventAdmissionOutcome::CapacityExceeded);
        }
        let previous: Option<Vec<u8>> = transaction.query_row(
            "SELECT MAX(sequence) FROM agent_events WHERE attempt_id = ?1",
            [attempt.id().as_str()],
            |row| row.get(0),
        )?;
        let sequence = match previous {
            Some(previous) => decode_u64(previous)?
                .checked_add(1)
                .ok_or(DatabaseError::Capacity("agent event sequence"))?,
            None => 1,
        };
        let event_id = format!("agent-event:{}:{sequence}", attempt.id().as_str());
        let process_instance_id =
            admission
                .target
                .process_instance_id
                .as_ref()
                .ok_or(DatabaseError::InvalidData(
                    "local agent event requires a process identity",
                ))?;
        transaction.execute(
            "INSERT INTO agent_events (
                event_id, attempt_id, session_id, process_instance_id, sequence, request_id,
                call_digest, call_json, received_at, reply_digest, reply_json, replied_at,
                record_version
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, NULL, NULL, 1)",
            params![
                event_id,
                attempt.id().as_str(),
                admission.target.session_id.as_str(),
                process_instance_id.as_str(),
                encode_u64(sequence),
                admission.request_id,
                admission.call_digest.to_hex(),
                admission.call_canonical_json,
                admission.received_at.value(),
            ],
        )?;
        let event = AgentEventRecord {
            event_id,
            sequence,
            target: admission.target,
            call_digest: admission.call_digest,
            call_canonical_json: admission.call_canonical_json,
            received_at: admission.received_at,
        };
        transaction.commit()?;
        Ok(AgentEventAdmissionOutcome::Queued(event))
    }

    pub(crate) fn poll_agent_event(
        &mut self,
        target: AgentChannelTarget,
        after_sequence: u64,
        observed_at: TimestampMillis,
    ) -> Result<AgentEventPollOutcome, DatabaseError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)?;
        let attempt = match validate_target(&transaction, &target, observed_at)? {
            TargetValidation::Current(attempt) => attempt,
            TargetValidation::Stale => return Ok(AgentEventPollOutcome::StaleTarget),
            TargetValidation::Expired => return Ok(AgentEventPollOutcome::BindingExpired),
            TargetValidation::NotFound => return Ok(AgentEventPollOutcome::NotFound),
        };
        let event = transaction
            .query_row(
                "SELECT event_id, sequence, session_id, process_instance_id, call_digest,
                        call_json, received_at
                 FROM agent_events
                 WHERE attempt_id = ?1 AND sequence > ?2 AND reply_digest IS NULL
                 ORDER BY sequence LIMIT 1",
                params![attempt.id().as_str(), encode_u64(after_sequence)],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Vec<u8>>(5)?,
                        row.get::<_, i64>(6)?,
                    ))
                },
            )
            .optional()?;
        let outcome =
            if let Some((event_id, sequence, session_id, process_id, digest, call, at)) = event {
                AgentEventPollOutcome::Event(Box::new(AgentEventRecord {
                    event_id,
                    sequence: decode_u64(sequence)?,
                    target: event_target(&target, session_id, process_id)?,
                    call_digest: decode_digest(&digest, "agent event call digest is invalid")?,
                    call_canonical_json: call,
                    received_at: TimestampMillis::new(at),
                }))
            } else {
                let maximum: Option<Vec<u8>> = transaction.query_row(
                    "SELECT MAX(sequence) FROM agent_events WHERE attempt_id = ?1",
                    [attempt.id().as_str()],
                    |row| row.get(0),
                )?;
                let last_sequence = maximum
                    .map(decode_u64)
                    .transpose()?
                    .unwrap_or(after_sequence);
                AgentEventPollOutcome::None {
                    next_sequence: last_sequence.checked_add(1).ok_or(
                        DatabaseError::InvalidData("agent event sequence is exhausted"),
                    )?,
                }
            };
        transaction.commit()?;
        Ok(outcome)
    }

    pub(crate) fn reply_agent_event(
        &mut self,
        admission: AgentEventReplyAdmission,
    ) -> Result<AgentEventReplyOutcome, DatabaseError> {
        validate_json_bytes(&admission.outcome_canonical_json)?;
        if PayloadDigest::of_bytes(&admission.outcome_canonical_json) != admission.reply_digest {
            return Err(DatabaseError::InvalidData(
                "agent event reply digest does not match its canonical payload",
            ));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        match validate_target(&transaction, &admission.target, admission.replied_at)? {
            TargetValidation::Current(_) => {}
            TargetValidation::Stale => return Ok(AgentEventReplyOutcome::StaleTarget),
            TargetValidation::Expired => return Ok(AgentEventReplyOutcome::BindingExpired),
            TargetValidation::NotFound => return Ok(AgentEventReplyOutcome::NotFound),
        }
        let existing = load_reply_target(&transaction, &admission.event_id)?;
        let Some(existing) = existing else {
            return Ok(AgentEventReplyOutcome::NotFound);
        };
        if existing.attempt_id != *admission.target.attempt_id.as_str()
            || existing.sequence != admission.sequence
            || existing.session_id != *admission.target.session_id.as_str()
            || admission
                .target
                .process_instance_id
                .as_ref()
                .map(ProcessInstanceId::as_str)
                != Some(existing.process_instance_id.as_str())
            || existing.call_digest != admission.call_digest
        {
            return Ok(AgentEventReplyOutcome::StaleTarget);
        }
        if let Some(reply) = existing.reply {
            return Ok(
                if reply.reply_digest == admission.reply_digest
                    && reply.outcome_canonical_json == admission.outcome_canonical_json
                {
                    AgentEventReplyOutcome::Duplicate(reply)
                } else {
                    AgentEventReplyOutcome::Conflict
                },
            );
        }
        transaction.execute(
            "UPDATE agent_events
             SET reply_digest = ?1, reply_json = ?2, replied_at = ?3
             WHERE event_id = ?4 AND reply_digest IS NULL",
            params![
                admission.reply_digest.to_hex(),
                admission.outcome_canonical_json,
                admission.replied_at.value(),
                admission.event_id,
            ],
        )?;
        let reply = AgentEventReplyRecord {
            event_id: admission.event_id,
            sequence: admission.sequence,
            reply_digest: admission.reply_digest,
            outcome_canonical_json: admission.outcome_canonical_json,
            replied_at: admission.replied_at,
        };
        transaction.commit()?;
        Ok(AgentEventReplyOutcome::Accepted(reply))
    }

    pub(crate) fn get_agent_event_reply(
        &mut self,
        target: AgentChannelTarget,
        event_id: String,
        observed_at: TimestampMillis,
    ) -> Result<Option<AgentEventReplyRecord>, DatabaseError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)?;
        if !matches!(
            validate_target(&transaction, &target, observed_at)?,
            TargetValidation::Current(_)
        ) {
            return Ok(None);
        }
        let reply = load_reply_target(&transaction, &event_id)?
            .filter(|record| {
                record.attempt_id == target.attempt_id.as_str()
                    && record.session_id == target.session_id.as_str()
            })
            .and_then(|record| record.reply);
        transaction.commit()?;
        Ok(reply)
    }
}

enum TargetValidation {
    Current(Box<OperationAttempt>),
    Stale,
    Expired,
    NotFound,
}

fn validate_target(
    connection: &rusqlite::Connection,
    target: &AgentChannelTarget,
    observed_at: TimestampMillis,
) -> Result<TargetValidation, DatabaseError> {
    let Some(attempt) = load_attempt(connection, &target.attempt_id)? else {
        return Ok(TargetValidation::NotFound);
    };
    let Some(dispatch) = attempt.dispatch_binding() else {
        return Ok(TargetValidation::NotFound);
    };
    let Some(binding) = dispatch.agent() else {
        return Ok(TargetValidation::NotFound);
    };
    let current = current_fence(
        connection,
        attempt.target_session(),
        Some(dispatch.caller_cell_identity()),
    )?;
    if current
        .as_ref()
        .is_some_and(|(_, fence)| *fence != attempt.fencing_token())
        || attempt.fencing_token() != target.fence
        || binding.attempt_revision() != target.attempt_revision
        || binding.recovery_epoch() != target.recovery_epoch
        || binding.request_digest() != target.request_digest
        || binding.tenant_id() != target.tenant_id
        || binding.source_product() != target.source_product
        || binding.actor_context_ref() != target.actor_context_ref
        || binding.authority_ref() != target.authority_ref
        || binding.undercurrent_provider_id() != target.undercurrent_provider_id
        || binding.undercurrent_provider_generation() != target.undercurrent_provider_generation
    {
        return Ok(TargetValidation::Stale);
    }
    if binding.expires_at() <= observed_at {
        return Ok(TargetValidation::Expired);
    }
    Ok(TargetValidation::Current(Box::new(attempt)))
}

fn validate_event_payload(request_id: &str, payload: &[u8]) -> Result<(), DatabaseError> {
    if request_id.is_empty() || request_id.len() > 128 || request_id.chars().any(char::is_control) {
        return Err(DatabaseError::InvalidData(
            "agent event request identity is invalid",
        ));
    }
    validate_json_bytes(payload)
}

fn validate_json_bytes(payload: &[u8]) -> Result<(), DatabaseError> {
    if payload.len() < 2 || payload.len() > MAX_AGENT_CHANNEL_RECORD_BYTES {
        return Err(DatabaseError::Capacity("agent channel record"));
    }
    serde_json::from_slice::<serde_json::Value>(payload)
        .map(|_| ())
        .map_err(|_| DatabaseError::InvalidData("agent channel record is not valid JSON"))
}

fn decode_digest(value: &str, message: &'static str) -> Result<PayloadDigest, DatabaseError> {
    PayloadDigest::from_hex(value).map_err(|_| DatabaseError::InvalidData(message))
}

type ExistingEventRow = (
    String,
    Vec<u8>,
    String,
    String,
    String,
    Vec<u8>,
    i64,
    Option<String>,
    Option<Vec<u8>>,
    Option<i64>,
);

fn read_existing_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<ExistingEventRow> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
        row.get(9)?,
    ))
}

struct ExistingEvent {
    event: AgentEventRecord,
    reply: Option<AgentEventReplyRecord>,
}

fn decode_existing_event(
    target: &AgentChannelTarget,
    row: ExistingEventRow,
) -> Result<ExistingEvent, DatabaseError> {
    let (
        event_id,
        sequence,
        session_id,
        process_id,
        digest,
        call,
        at,
        reply_digest,
        reply,
        reply_at,
    ) = row;
    let sequence = decode_u64(sequence)?;
    let event = AgentEventRecord {
        event_id: event_id.clone(),
        sequence,
        target: event_target(target, session_id, process_id)?,
        call_digest: decode_digest(&digest, "agent event call digest is invalid")?,
        call_canonical_json: call,
        received_at: TimestampMillis::new(at),
    };
    let reply = match (reply_digest, reply, reply_at) {
        (None, None, None) => None,
        (Some(digest), Some(outcome), Some(at)) => Some(AgentEventReplyRecord {
            event_id,
            sequence,
            reply_digest: decode_digest(&digest, "agent event reply digest is invalid")?,
            outcome_canonical_json: outcome,
            replied_at: TimestampMillis::new(at),
        }),
        _ => {
            return Err(DatabaseError::InvalidData(
                "agent event reply state is incomplete",
            ));
        }
    };
    Ok(ExistingEvent { event, reply })
}

fn event_target(
    target: &AgentChannelTarget,
    session_id: String,
    process_id: String,
) -> Result<AgentChannelTarget, DatabaseError> {
    let mut exact = target.clone();
    exact.session_id = SessionId::new(session_id)
        .map_err(|_| DatabaseError::InvalidData("agent event session identity is invalid"))?;
    exact.process_instance_id = Some(
        ProcessInstanceId::new(process_id)
            .map_err(|_| DatabaseError::InvalidData("agent event process identity is invalid"))?,
    );
    Ok(exact)
}

struct ReplyTarget {
    attempt_id: String,
    session_id: String,
    process_instance_id: String,
    sequence: u64,
    call_digest: PayloadDigest,
    reply: Option<AgentEventReplyRecord>,
}

fn load_reply_target(
    connection: &rusqlite::Connection,
    event_id: &str,
) -> Result<Option<ReplyTarget>, DatabaseError> {
    let row = connection
        .query_row(
            "SELECT attempt_id, session_id, process_instance_id, sequence, call_digest,
                    reply_digest, reply_json, replied_at
             FROM agent_events WHERE event_id = ?1",
            [event_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<Vec<u8>>>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                ))
            },
        )
        .optional()?;
    row.map(
        |(attempt_id, session_id, process_instance_id, sequence, call, digest, reply, at)| {
            let sequence = decode_u64(sequence)?;
            let reply = match (digest, reply, at) {
                (None, None, None) => None,
                (Some(digest), Some(outcome), Some(at)) => Some(AgentEventReplyRecord {
                    event_id: event_id.to_owned(),
                    sequence,
                    reply_digest: decode_digest(&digest, "agent event reply digest is invalid")?,
                    outcome_canonical_json: outcome,
                    replied_at: TimestampMillis::new(at),
                }),
                _ => {
                    return Err(DatabaseError::InvalidData(
                        "agent event reply state is incomplete",
                    ));
                }
            };
            Ok(ReplyTarget {
                attempt_id,
                session_id,
                process_instance_id,
                sequence,
                call_digest: decode_digest(&call, "agent event call digest is invalid")?,
                reply,
            })
        },
    )
    .transpose()
}

pub(super) fn validate_agent_channel_consistency(
    connection: &rusqlite::Connection,
) -> Result<(), DatabaseError> {
    let attempt_ids = {
        let mut statement = connection
            .prepare("SELECT attempt_id FROM agent_runtime_bindings ORDER BY attempt_id")?;
        statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?
    };
    for attempt_id in attempt_ids {
        let attempt_id = undercurrent_domain::OperationAttemptId::new(attempt_id)
            .map_err(|_| DatabaseError::InvalidData("agent binding attempt identity is invalid"))?;
        let attempt = load_attempt(connection, &attempt_id)?.ok_or(DatabaseError::InvalidData(
            "agent binding references a missing attempt",
        ))?;
        if attempt
            .dispatch_binding()
            .and_then(undercurrent_domain::RuntimeDispatchBinding::agent)
            .is_none()
        {
            return Err(DatabaseError::InvalidData(
                "agent binding is absent from its dispatch attempt",
            ));
        }
    }

    let events = {
        let mut statement = connection.prepare(
            "SELECT event_id, session_id, process_instance_id, sequence, call_digest,
                    call_json, reply_digest, reply_json
             FROM agent_events ORDER BY event_id",
        )?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<Vec<u8>>>(7)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (event_id, session_id, process_id, sequence, call_digest, call, reply_digest, reply) in
        events
    {
        if event_id.is_empty() || event_id.len() > 256 {
            return Err(DatabaseError::InvalidData(
                "agent event identity is invalid",
            ));
        }
        SessionId::new(session_id)
            .map_err(|_| DatabaseError::InvalidData("agent event session identity is invalid"))?;
        ProcessInstanceId::new(process_id)
            .map_err(|_| DatabaseError::InvalidData("agent event process identity is invalid"))?;
        if decode_u64(sequence)? == 0 {
            return Err(DatabaseError::InvalidData("agent event sequence is zero"));
        }
        decode_digest(&call_digest, "agent event call digest is invalid")?;
        validate_json_bytes(&call)?;
        match (reply_digest, reply) {
            (None, None) => {}
            (Some(digest), Some(reply)) => {
                decode_digest(&digest, "agent event reply digest is invalid")?;
                validate_json_bytes(&reply)?;
            }
            _ => {
                return Err(DatabaseError::InvalidData(
                    "agent event reply state is incomplete",
                ));
            }
        }
    }
    Ok(())
}
