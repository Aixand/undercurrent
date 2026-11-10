// SPDX-License-Identifier: Apache-2.0

//! Quiescent input retirement preserves Unknown certainty and a deletion-safe audit.

use super::*;
use undercurrent_application::InputAttemptRetirement;

pub(super) const SCHEMA: &str = "CREATE TABLE input_attempt_retirements (
    attempt_id TEXT PRIMARY KEY NOT NULL REFERENCES operation_attempts(attempt_id),
    fencing_token BLOB NOT NULL CHECK(length(fencing_token) = 8),
    session_id TEXT NOT NULL,
    session_revision BLOB NOT NULL CHECK(length(session_revision) = 8),
    session_snapshot BLOB NOT NULL,
    process_id TEXT NOT NULL,
    exited_at INTEGER NOT NULL,
    retired_at INTEGER NOT NULL,
    acknowledged INTEGER NOT NULL DEFAULT 0 CHECK(acknowledged IN (0, 1))
) STRICT";

pub(super) fn is_retired(
    connection: &Connection,
    id: &OperationAttemptId,
) -> Result<bool, DatabaseError> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM input_attempt_retirements WHERE attempt_id = ?1)",
        [id.as_str()],
        |row| row.get(0),
    )?)
}

impl Database {
    pub(in crate::sqlite) fn retire_input(
        &mut self,
        retirement: InputAttemptRetirement,
    ) -> Result<bool, DatabaseError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(attempt) = load_attempt(&transaction, &retirement.attempt_id)? else {
            return Ok(false);
        };
        if attempt.fencing_token() != retirement.fencing_token
            || attempt.kind() != ProcessMutationKind::WriteInput
            || attempt.dispatch_binding().is_some()
            || !matches!(
                attempt.state(),
                OperationAttemptState::Settled {
                    certainty: EffectCertainty::Unknown,
                    ..
                }
            )
        {
            return Ok(false);
        }
        if is_retired(&transaction, attempt.id())? {
            return Ok(true);
        }
        let Some(target) = attempt.target_session() else {
            return Ok(false);
        };
        let Some(session) = load_session_by_id(&transaction, target, self.limits.max_record_bytes)?
        else {
            return Ok(false);
        };
        if session.revision() != retirement.session_revision
            || !session.lifecycle().is_terminal()
            || session.process().is_some()
            || retirement.retired_at < retirement.evidence.exited_at
        {
            return Ok(false);
        }
        let snapshot = encode_session(&session, self.limits.max_record_bytes)
            .map_err(DatabaseError::from_encoding)?;
        transaction.execute("INSERT INTO input_attempt_retirements
            (attempt_id, fencing_token, session_id, session_revision, session_snapshot, process_id, exited_at, retired_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)", params![
                attempt.id().as_str(), encode_u64(attempt.fencing_token().value()), target.as_str(),
                encode_u64(session.revision().value()), snapshot, retirement.evidence.process_id.as_str(),
                retirement.evidence.exited_at.value(), retirement.retired_at.value(),
            ])?;
        transaction.commit()?;
        Ok(true)
    }

    pub(in crate::sqlite) fn pending_input_retirement_acks(
        &self,
        limit: usize,
    ) -> Result<Vec<OperationAttempt>, DatabaseError> {
        if limit == 0 || limit > self.limits.max_read_batch {
            return Err(DatabaseError::Capacity("input retirement acknowledgements"));
        }
        let mut query = self.connection.prepare("SELECT attempt_id FROM input_attempt_retirements WHERE acknowledged = 0 ORDER BY attempt_id LIMIT ?1")?;
        let ids = query
            .query_map([limit as i64], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|id| {
                let id = OperationAttemptId::new(id)
                    .map_err(|_| DatabaseError::InvalidData("invalid retired attempt ID"))?;
                load_attempt(&self.connection, &id)?
                    .ok_or(DatabaseError::InvalidData("retired attempt missing"))
            })
            .collect()
    }

    pub(in crate::sqlite) fn acknowledge_input_retirement(
        &mut self,
        attempt: OperationAttempt,
    ) -> Result<(), DatabaseError> {
        let changed = self.connection.execute("UPDATE input_attempt_retirements SET acknowledged = 1 WHERE attempt_id = ?1 AND fencing_token = ?2",
            params![attempt.id().as_str(), encode_u64(attempt.fencing_token().value())])?;
        if changed != 1 {
            return Err(DatabaseError::InvalidData(
                "input retirement acknowledgement mismatch",
            ));
        }
        Ok(())
    }
}

pub(super) fn validate(
    connection: &Connection,
    max_record_bytes: usize,
) -> Result<(), DatabaseError> {
    let invalid: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM input_attempt_retirements r
         LEFT JOIN operation_attempts a ON a.attempt_id = r.attempt_id
         WHERE a.attempt_id IS NULL OR a.fencing_token != r.fencing_token
            OR a.mutation_kind != 'write_input' OR a.state != 3
            OR a.dispatch_cell_identity IS NOT NULL OR a.target_session IS NULL
            OR a.target_session != r.session_id OR r.retired_at < r.exited_at)",
        [],
        |row| row.get(0),
    )?;
    if invalid {
        return Err(DatabaseError::InvalidData(
            "input retirement audit does not match durable attempt",
        ));
    }
    let mut statement = connection.prepare(
        "SELECT session_id, session_revision, CASE WHEN length(session_snapshot) <= ?1 THEN session_snapshot ELSE NULL END, process_id FROM input_attempt_retirements")?;
    let mut rows = statement.query([max_record_bytes.min(i64::MAX as usize) as i64])?;
    while let Some(row) = rows.next()? {
        let id: String = row.get(0)?;
        let revision: Vec<u8> = row.get(1)?;
        let snapshot: Vec<u8> = row
            .get::<_, Option<Vec<u8>>>(2)?
            .ok_or(DatabaseError::Capacity("input retirement snapshot"))?;
        let process: String = row.get(3)?;
        let session = decode_session(&snapshot, max_record_bytes, SESSION_RECORD_VERSION)?;
        if session.id().as_str() != id
            || encode_u64(session.revision().value()) != revision
            || !session.lifecycle().is_terminal()
            || session.process().is_some()
            || ProcessInstanceId::new(process).is_err()
        {
            return Err(DatabaseError::InvalidData(
                "invalid input retirement terminal evidence",
            ));
        }
    }
    Ok(())
}
