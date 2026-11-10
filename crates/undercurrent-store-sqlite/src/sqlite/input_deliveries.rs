// SPDX-License-Identifier: Apache-2.0

use super::{Request, SqliteRepositoryConfigError, SqliteSessionRepository};
use undercurrent_application::{
    InputDeliveryDisposition, InputDeliveryReservation, InputDeliveryStore, PortError, PortFuture,
    PortOperation, semantic_input_delivery_digest,
};
use undercurrent_domain::{DeliveryId, InputObservationPolicy, InputSequence, SessionId};

/// Durable semantic delivery reservations sharing the session repository worker.
///
/// Records retain a private typed digest, never raw terminal input. They do not
/// expire or evict, including unresolved effects; deleting their owning session
/// removes them transactionally. Backups include them in the same database.
#[derive(Clone)]
pub struct SqliteInputDeliveryStore {
    repository: SqliteSessionRepository,
    maximum: usize,
}

impl SqliteInputDeliveryStore {
    /// Attaches a non-evicting record capacity to the existing repository.
    ///
    /// Lowering capacity preserves every old record and blocks new IDs until
    /// session deletion frees space. Replay/settlement remain available at capacity.
    ///
    /// # Errors
    /// Returns an error for zero or a limit outside SQLite's signed integer range.
    pub fn new(
        repository: SqliteSessionRepository,
        maximum: usize,
    ) -> Result<Self, SqliteRepositoryConfigError> {
        if maximum == 0 {
            return Err(SqliteRepositoryConfigError::ZeroLimit {
                field: "input_delivery_records",
            });
        }
        if i64::try_from(maximum).is_err() {
            return Err(SqliteRepositoryConfigError::LimitTooLarge {
                field: "input_delivery_records",
            });
        }
        Ok(Self {
            repository,
            maximum,
        })
    }
}

impl InputDeliveryStore for SqliteInputDeliveryStore {
    fn reserve<'a>(
        &'a self,
        session_id: &'a SessionId,
        delivery_id: &'a DeliveryId,
        input: &'a InputSequence,
        observation_policy: InputObservationPolicy,
    ) -> PortFuture<'a, Result<InputDeliveryReservation, PortError>> {
        Box::pin(async move {
            let digest = semantic_input_delivery_digest(input, observation_policy);
            self.repository
                .request(
                    PortOperation::ReserveInputDelivery,
                    Some(session_id.clone()),
                    |reply| Request::ReserveInputDelivery {
                        session_id: session_id.clone(),
                        delivery_id: delivery_id.clone(),
                        digest,
                        maximum: self.maximum,
                        reply,
                    },
                )
                .await
        })
    }
    fn settle<'a>(
        &'a self,
        session_id: &'a SessionId,
        delivery_id: &'a DeliveryId,
        disposition: InputDeliveryDisposition,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            self.repository
                .request(
                    PortOperation::SettleInputDelivery,
                    Some(session_id.clone()),
                    |reply| Request::SettleInputDelivery {
                        session_id: session_id.clone(),
                        delivery_id: delivery_id.clone(),
                        disposition,
                        reply,
                    },
                )
                .await
        })
    }
}
