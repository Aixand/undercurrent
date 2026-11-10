// SPDX-License-Identifier: Apache-2.0

//! Bounded semantic terminal-input idempotency records.

use std::collections::BTreeMap;
use std::sync::Mutex;

use undercurrent_application::{
    InputDeliveryDisposition, InputDeliveryReservation, InputDeliveryStore, PortError,
    PortErrorKind, PortFuture, PortOperation,
};
use undercurrent_domain::{DeliveryId, InputObservationPolicy, InputSequence, SessionId};

use crate::{RuntimeAdapterFailure, RuntimeLimitError};

/// Current bounded input-delivery record pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InputDeliveryPressure {
    /// Retained semantic delivery records.
    pub records: usize,
    /// Inclusive configured record capacity.
    pub limit: usize,
}

/// In-memory idempotency store that rejects new work instead of evicting
/// ambiguous delivery records silently.
pub struct BoundedInputDeliveryStore {
    maximum: usize,
    records: Mutex<BTreeMap<(SessionId, DeliveryId), InputDeliveryRecord>>,
}

impl BoundedInputDeliveryStore {
    /// Constructs an empty store with an explicit hard record limit.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed zero-limit failure.
    pub fn new(maximum: usize) -> Result<Self, RuntimeLimitError> {
        if maximum == 0 {
            return Err(RuntimeLimitError::zero("input_delivery_records"));
        }
        Ok(Self {
            maximum,
            records: Mutex::new(BTreeMap::new()),
        })
    }

    /// Removes all retained delivery records for one deleted session.
    ///
    /// Returns exact removed count. This function performs no IO and does not
    /// panic.
    pub fn remove_session(&self, session_id: &SessionId) -> usize {
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let before = records.len();
        records.retain(|(candidate, _), _| candidate != session_id);
        before.saturating_sub(records.len())
    }

    /// Removes one record after its externally configured retention expires.
    pub fn remove(&self, session_id: &SessionId, delivery_id: &DeliveryId) -> bool {
        self.records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&(session_id.clone(), delivery_id.clone()))
            .is_some()
    }

    /// Returns lock-consistent record pressure.
    pub fn pressure(&self) -> InputDeliveryPressure {
        InputDeliveryPressure {
            records: self
                .records
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len(),
            limit: self.maximum,
        }
    }
}

impl InputDeliveryStore for BoundedInputDeliveryStore {
    fn reserve<'a>(
        &'a self,
        session_id: &'a SessionId,
        delivery_id: &'a DeliveryId,
        input: &'a InputSequence,
        observation_policy: InputObservationPolicy,
    ) -> PortFuture<'a, Result<InputDeliveryReservation, PortError>> {
        Box::pin(async move {
            let key = (session_id.clone(), delivery_id.clone());
            let mut records = self
                .records
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(record) = records.get(&key) {
                if record.input != *input || record.observation_policy != observation_policy {
                    return Ok(InputDeliveryReservation::PayloadConflict);
                }
                return Ok(match record.disposition {
                    Some(InputDeliveryDisposition::Delivered { delivered_at }) => {
                        InputDeliveryReservation::AlreadyDelivered { delivered_at }
                    }
                    Some(InputDeliveryDisposition::Indeterminate { .. }) | None => {
                        // An unsettled reservation may already have written to
                        // the PTY. Reissuing it could duplicate input after a
                        // lost reply or failed settlement.
                        // See [coordination guide](../docs/internals/coordination/scheduling.md#preserve-uncertainty-in-semantic-input-deliveries).
                        InputDeliveryReservation::Indeterminate
                    }
                });
            }
            if records.len() >= self.maximum {
                return Err(PortError::new(
                    PortErrorKind::CapacityExceeded,
                    PortOperation::ReserveInputDelivery,
                    Some(session_id.clone()),
                    RuntimeAdapterFailure("input delivery record capacity exhausted"),
                ));
            }
            records.insert(
                key,
                InputDeliveryRecord {
                    input: input.clone(),
                    observation_policy,
                    disposition: None,
                },
            );
            Ok(InputDeliveryReservation::Reserved)
        })
    }

    fn settle<'a>(
        &'a self,
        session_id: &'a SessionId,
        delivery_id: &'a DeliveryId,
        disposition: InputDeliveryDisposition,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            let mut records = self
                .records
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let record = records
                .get_mut(&(session_id.clone(), delivery_id.clone()))
                .ok_or_else(|| {
                    PortError::new(
                        PortErrorKind::NotFound,
                        PortOperation::SettleInputDelivery,
                        Some(session_id.clone()),
                        RuntimeAdapterFailure("input delivery reservation was not found"),
                    )
                })?;
            match record.disposition {
                Some(current) if current != disposition => Err(PortError::new(
                    PortErrorKind::Conflict,
                    PortOperation::SettleInputDelivery,
                    Some(session_id.clone()),
                    RuntimeAdapterFailure("input delivery already has another disposition"),
                )),
                Some(_) => Ok(()),
                None => {
                    record.disposition = Some(disposition);
                    Ok(())
                }
            }
        })
    }
}

#[derive(Clone)]
struct InputDeliveryRecord {
    input: InputSequence,
    observation_policy: InputObservationPolicy,
    disposition: Option<InputDeliveryDisposition>,
}
