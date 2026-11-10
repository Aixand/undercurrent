// SPDX-License-Identifier: Apache-2.0

use super::*;
use undercurrent_application::{InputDeliveryDisposition, InputDeliveryReservation};
use undercurrent_domain::DeliveryId;

impl Database {
    pub(in crate::sqlite) fn reserve_input_delivery(
        &mut self,
        session: &SessionId,
        delivery: &DeliveryId,
        digest: PayloadDigest,
        maximum: usize,
    ) -> Result<InputDeliveryReservation, DatabaseError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let retained: Option<(Vec<u8>, i64, Option<i64>)> = transaction.query_row(
            "SELECT CASE WHEN length(input_digest)=32 THEN input_digest ELSE X'' END, disposition, observed_at FROM input_deliveries WHERE session_id=?1 AND delivery_id=?2",
            params![session.as_str(), delivery.as_str()], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).optional()?;
        if let Some((prior, disposition, observed_at)) = retained {
            if prior.len() != 32 {
                return Err(DatabaseError::InvalidData("invalid input delivery digest"));
            }
            if prior != digest.as_bytes() {
                return Ok(InputDeliveryReservation::PayloadConflict);
            }
            return match (disposition, observed_at) {
                (0, None) | (2, Some(_)) => Ok(InputDeliveryReservation::Indeterminate),
                (1, Some(at)) => Ok(InputDeliveryReservation::AlreadyDelivered {
                    delivered_at: TimestampMillis::new(at),
                }),
                _ => Err(DatabaseError::InvalidData(
                    "invalid input delivery disposition",
                )),
            };
        }
        if !exists_by_id(&transaction, session)? {
            return Err(DatabaseError::NotFound("input delivery session"));
        }
        // No expiry or eviction: an unresolved write may already have reached the
        // terminal. Capacity rejection must not recreate that effect authority.
        let count: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM (SELECT 1 FROM input_deliveries LIMIT ?1)",
            [sql_usize(maximum, "input delivery capacity")?],
            |row| row.get(0),
        )?;
        if count >= sql_usize(maximum, "input delivery capacity")? {
            return Err(DatabaseError::Capacity("input delivery records"));
        }
        transaction.execute(
            "INSERT INTO input_deliveries(session_id, delivery_id, input_digest, disposition, observed_at) VALUES (?1, ?2, ?3, 0, NULL)",
            params![session.as_str(), delivery.as_str(), digest.as_bytes().as_slice()])?;
        transaction.commit()?;
        Ok(InputDeliveryReservation::Reserved)
    }

    pub(in crate::sqlite) fn settle_input_delivery(
        &mut self,
        session: &SessionId,
        delivery: &DeliveryId,
        disposition: InputDeliveryDisposition,
    ) -> Result<(), DatabaseError> {
        let (state, at) = match disposition {
            InputDeliveryDisposition::Delivered { delivered_at } => (1, delivered_at.value()),
            InputDeliveryDisposition::Indeterminate { observed_at } => (2, observed_at.value()),
        };
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let retained: Option<(i64, Option<i64>)> = transaction.query_row(
            "SELECT disposition, observed_at FROM input_deliveries WHERE session_id=?1 AND delivery_id=?2",
            params![session.as_str(), delivery.as_str()], |row| Ok((row.get(0)?, row.get(1)?))).optional()?;
        match retained {
            None => return Err(DatabaseError::NotFound("input delivery reservation")),
            Some((prior, observed)) if prior == state && observed == Some(at) => return Ok(()),
            Some((0, None)) => {}
            Some(_) => return Err(DatabaseError::Conflict("input delivery disposition")),
        }
        transaction.execute(
            "UPDATE input_deliveries SET disposition=?3, observed_at=?4 WHERE session_id=?1 AND delivery_id=?2",
            params![session.as_str(), delivery.as_str(), state, at])?;
        transaction.commit()?;
        Ok(())
    }
}
