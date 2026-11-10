// SPDX-License-Identifier: Apache-2.0

//! Bounded client-scoped mutating-request result retention.
//!
//! Exact request bytes identify an entry within a logical client and key scope.
//! Pending records reserve maximum response space and never expire implicitly.
//! Completed results expire only when the owner calls the expiration sweep.
//! This store provides no persistence, authorization or application reconciliation.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use undercurrent_wire::{ClientId, IdempotencyKey};

/// Invalid idempotency-store capacity relationship.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestIdempotencyLimitError {
    /// A required bound was zero.
    ZeroLimit {
        /// Stable invalid field name.
        field: &'static str,
    },
    /// Total retained bytes cannot reserve one maximum-sized record.
    TotalBytesBelowOneRecord,
}

impl Display for RequestIdempotencyLimitError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroLimit { field } => {
                write!(
                    formatter,
                    "request idempotency limit {field} must be non-zero"
                )
            }
            Self::TotalBytesBelowOneRecord => formatter.write_str(
                "request idempotency total bytes cannot reserve one maximum request and response",
            ),
        }
    }
}

impl Error for RequestIdempotencyLimitError {}

/// Hard record and byte limits for mutating-request result retention.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestIdempotencyLimits {
    maximum_records: usize,
    maximum_total_bytes: u64,
    maximum_request_bytes: u32,
    maximum_response_bytes: u32,
}

impl RequestIdempotencyLimits {
    /// Constructs mutually consistent non-zero bounds.
    ///
    /// Each pending reservation accounts for its complete maximum response so
    /// successful mutation results cannot become unrecordable after execution.
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed zero or insufficient-total-bytes failure.
    pub const fn new(
        maximum_records: usize,
        maximum_total_bytes: u64,
        maximum_request_bytes: u32,
        maximum_response_bytes: u32,
    ) -> Result<Self, RequestIdempotencyLimitError> {
        if maximum_records == 0 {
            return Err(RequestIdempotencyLimitError::ZeroLimit {
                field: "maximum_records",
            });
        }
        if maximum_total_bytes == 0 {
            return Err(RequestIdempotencyLimitError::ZeroLimit {
                field: "maximum_total_bytes",
            });
        }
        if maximum_request_bytes == 0 {
            return Err(RequestIdempotencyLimitError::ZeroLimit {
                field: "maximum_request_bytes",
            });
        }
        if maximum_response_bytes == 0 {
            return Err(RequestIdempotencyLimitError::ZeroLimit {
                field: "maximum_response_bytes",
            });
        }
        if maximum_total_bytes < maximum_request_bytes as u64 + maximum_response_bytes as u64 {
            return Err(RequestIdempotencyLimitError::TotalBytesBelowOneRecord);
        }
        Ok(Self {
            maximum_records,
            maximum_total_bytes,
            maximum_request_bytes,
            maximum_response_bytes,
        })
    }

    /// Returns the inclusive record count maximum.
    pub const fn maximum_records(self) -> usize {
        self.maximum_records
    }

    /// Returns bytes reserved across pending and completed records.
    pub const fn maximum_total_bytes(self) -> u64 {
        self.maximum_total_bytes
    }

    /// Returns the inclusive encoded mutating-request maximum.
    pub const fn maximum_request_bytes(self) -> u32 {
        self.maximum_request_bytes
    }

    /// Returns the inclusive encoded result maximum.
    pub const fn maximum_response_bytes(self) -> u32 {
        self.maximum_response_bytes
    }

    const fn pending_reservation_bytes(self, request_bytes: u64) -> u64 {
        request_bytes + self.maximum_response_bytes as u64
    }
}

/// Logical-client and wire idempotency-key scope.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestIdempotencyScope {
    client_id: ClientId,
    key: IdempotencyKey,
}

impl RequestIdempotencyScope {
    /// Constructs an exact retry scope from validated wire identifiers.
    pub const fn new(client_id: ClientId, key: IdempotencyKey) -> Self {
        Self { client_id, key }
    }

    /// Returns the stable logical client owner.
    pub const fn client_id(&self) -> &ClientId {
        &self.client_id
    }

    /// Returns the caller-supplied semantic retry key.
    pub const fn key(&self) -> &IdempotencyKey {
        &self.key
    }
}

/// Failure enforcing idempotency record sizes and hard capacity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestIdempotencyError {
    /// Encoded request is empty or above the configured maximum.
    InvalidRequestSize {
        /// Observed bytes.
        actual: usize,
        /// Inclusive configured maximum.
        maximum: u32,
    },
    /// Encoded response is empty or above the configured maximum.
    InvalidResponseSize {
        /// Observed bytes.
        actual: usize,
        /// Inclusive configured maximum.
        maximum: u32,
    },
    /// Record-count capacity was exhausted.
    RecordCapacityExceeded {
        /// Inclusive configured record maximum.
        limit: usize,
    },
    /// Reserved-byte capacity was exhausted.
    ByteCapacityExceeded {
        /// Current reserved bytes.
        retained: u64,
        /// Additional full-record reservation requested.
        requested: u64,
        /// Inclusive configured byte maximum.
        limit: u64,
    },
    /// Completion referenced no in-flight reservation.
    ReservationNotFound {
        /// Safe logical retry scope.
        scope: RequestIdempotencyScope,
    },
}

impl Display for RequestIdempotencyError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidRequestSize { actual, maximum } => write!(
                formatter,
                "idempotent request size {actual} is outside 1..={maximum}"
            ),
            Self::InvalidResponseSize { actual, maximum } => write!(
                formatter,
                "idempotent response size {actual} is outside 1..={maximum}"
            ),
            Self::RecordCapacityExceeded { limit } => {
                write!(
                    formatter,
                    "idempotency record capacity {limit} is exhausted"
                )
            }
            Self::ByteCapacityExceeded {
                retained,
                requested,
                limit,
            } => write!(
                formatter,
                "idempotency byte capacity {limit} retains {retained}; rejected reservation {requested}"
            ),
            Self::ReservationNotFound { scope } => write!(
                formatter,
                "idempotency reservation {} for client {} was not found",
                scope.key, scope.client_id
            ),
        }
    }
}

impl Error for RequestIdempotencyError {}

/// Result of reserving one exact semantic mutating request.
#[derive(Clone, PartialEq, Eq)]
pub enum RequestIdempotencyReservation {
    /// Caller owns first execution for this key and exact payload.
    Reserved,
    /// Matching execution is still in flight or its result is ambiguous.
    Pending,
    /// Matching completed result can be returned without repeating mutation.
    Completed {
        /// Exact caller-selected canonical response, omitted from debug presentation.
        ///
        /// A product boundary may project this opaque value into a negotiated wire
        /// representation, but the projection is not part of semantic request identity.
        response: Arc<[u8]>,
    },
    /// The same key was already bound to different request bytes.
    PayloadConflict,
}

impl Debug for RequestIdempotencyReservation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Reserved => formatter.write_str("Reserved"),
            Self::Pending => formatter.write_str("Pending"),
            Self::Completed { response } => formatter
                .debug_struct("Completed")
                .field("response_bytes", &response.len())
                .finish(),
            Self::PayloadConflict => formatter.write_str("PayloadConflict"),
        }
    }
}

/// Idempotent completion result for one reserved mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestIdempotencyCompletion {
    /// Pending record retained this response and its deadline.
    Completed,
    /// Exact response was already retained.
    AlreadyCompleted,
    /// A different response was already retained for this key.
    ResponseConflict,
}

/// Result of explicitly abandoning in-flight ambiguous state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestIdempotencyAbandonment {
    /// Pending reservation was removed.
    Abandoned,
    /// Completed results cannot be abandoned before retention expiry.
    AlreadyCompleted,
    /// Scope was not retained.
    Missing,
}

/// Exact resources released by expiry or client retirement.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RequestIdempotencyRelease {
    /// Records removed.
    pub records: usize,
    /// Reserved or retained bytes released.
    pub bytes: u64,
}

/// Current bounded record state without private request or response payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestIdempotencyPressure {
    /// In-flight or ambiguous request records.
    pub pending_records: usize,
    /// Replayable completed request records.
    pub completed_records: usize,
    /// Bytes reserved for pending maxima or retained completed payloads.
    pub retained_bytes: u64,
    /// Inclusive configured record maximum.
    pub record_limit: usize,
    /// Inclusive configured byte maximum.
    pub byte_limit: u64,
}

/// Bounded logical-client-scoped mutating-request result store.
pub struct BoundedRequestIdempotencyStore {
    limits: RequestIdempotencyLimits,
    state: Mutex<RequestIdempotencyState>,
}

impl BoundedRequestIdempotencyStore {
    /// Constructs an empty store from validated bounds.
    pub const fn new(limits: RequestIdempotencyLimits) -> Self {
        Self {
            limits,
            state: Mutex::new(RequestIdempotencyState::new()),
        }
    }

    /// Reserves first execution or returns retained exact retry state.
    ///
    /// Request bytes are compared exactly and omitted from diagnostics. New
    /// pending records reserve the configured maximum response bytes so a
    /// successful mutation can always retain its result. This function performs
    /// bounded allocation, no external IO, and does not panic.
    ///
    /// This method does not sweep expired results; the owner calls
    /// [`Self::expire_before`] explicitly. Absence after expiry or store loss is
    /// not evidence that a previous mutation had no effect.
    ///
    /// # Errors
    ///
    /// Returns typed request-size, record-capacity, or byte-capacity failure.
    pub fn reserve(
        &self,
        scope: RequestIdempotencyScope,
        request: Arc<[u8]>,
    ) -> Result<RequestIdempotencyReservation, RequestIdempotencyError> {
        self.validate_request(&request)?;
        let mut state = self.lock_state();
        // Resolve retained scopes before checking free capacity: an exact retry
        // needs no new reservation, even when unrelated work has filled the store.
        // See [retention rules](../../docs/internals/idempotency.md#identity-and-bounded-retention).
        if let Some(record) = state.records.get(&scope) {
            if record.request != request {
                return Ok(RequestIdempotencyReservation::PayloadConflict);
            }
            return Ok(match &record.result {
                RequestResultState::Pending => RequestIdempotencyReservation::Pending,
                RequestResultState::Completed { response, .. } => {
                    RequestIdempotencyReservation::Completed {
                        response: response.clone(),
                    }
                }
            });
        }
        if state.records.len() >= self.limits.maximum_records {
            return Err(RequestIdempotencyError::RecordCapacityExceeded {
                limit: self.limits.maximum_records,
            });
        }
        let request_bytes = u64::try_from(request.len()).unwrap_or(u64::MAX);
        let reservation = self.limits.pending_reservation_bytes(request_bytes);
        if state.retained_bytes.saturating_add(reservation) > self.limits.maximum_total_bytes {
            return Err(RequestIdempotencyError::ByteCapacityExceeded {
                retained: state.retained_bytes,
                requested: reservation,
                limit: self.limits.maximum_total_bytes,
            });
        }
        state.retained_bytes = state.retained_bytes.saturating_add(reservation);
        state.records.insert(
            scope,
            RequestIdempotencyRecord {
                request,
                result: RequestResultState::Pending,
                retained_bytes: reservation,
            },
        );
        Ok(RequestIdempotencyReservation::Reserved)
    }

    /// Retains one completed response until the caller-supplied deadline.
    ///
    /// Repeating the exact caller-selected canonical bytes is idempotent. A
    /// conflicting canonical response does not overwrite prior truth. Repeating
    /// completion does not refresh the original deadline. The store does not
    /// require these opaque bytes to equal every negotiated
    /// on-wire projection. This function performs bounded allocation, no
    /// external IO, and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed response-size or missing-reservation failure.
    pub fn complete(
        &self,
        scope: &RequestIdempotencyScope,
        response: Arc<[u8]>,
        retained_until: Instant,
    ) -> Result<RequestIdempotencyCompletion, RequestIdempotencyError> {
        self.validate_response(&response)?;
        let mut state = self.lock_state();
        let response_bytes = u64::try_from(response.len()).unwrap_or(u64::MAX);
        let (previous_retained_bytes, retained_bytes) = {
            let record = state.records.get_mut(scope).ok_or_else(|| {
                RequestIdempotencyError::ReservationNotFound {
                    scope: scope.clone(),
                }
            })?;
            match &record.result {
                RequestResultState::Completed {
                    response: current, ..
                } if current == &response => {
                    return Ok(RequestIdempotencyCompletion::AlreadyCompleted);
                }
                RequestResultState::Completed { .. } => {
                    return Ok(RequestIdempotencyCompletion::ResponseConflict);
                }
                RequestResultState::Pending => {}
            }
            let request_bytes = u64::try_from(record.request.len()).unwrap_or(u64::MAX);
            let retained_bytes = request_bytes.saturating_add(response_bytes);
            let previous_retained_bytes = record.retained_bytes;
            record.retained_bytes = retained_bytes;
            record.result = RequestResultState::Completed {
                response,
                retained_until,
            };
            (previous_retained_bytes, retained_bytes)
        };
        state.retained_bytes = state
            .retained_bytes
            .saturating_sub(previous_retained_bytes)
            .saturating_add(retained_bytes);
        state.expirations.insert(
            RequestExpirationKey {
                retained_until,
                scope: scope.clone(),
            },
            (),
        );
        Ok(RequestIdempotencyCompletion::Completed)
    }

    /// Explicitly removes an in-flight record after authoritative reconciliation.
    ///
    /// Completed results remain replayable until expiry. This function performs
    /// no external IO and does not panic.
    pub fn abandon(&self, scope: &RequestIdempotencyScope) -> RequestIdempotencyAbandonment {
        let mut state = self.lock_state();
        let Some(record) = state.records.get(scope) else {
            return RequestIdempotencyAbandonment::Missing;
        };
        if matches!(record.result, RequestResultState::Completed { .. }) {
            return RequestIdempotencyAbandonment::AlreadyCompleted;
        }
        let retained_bytes = record.retained_bytes;
        state.records.remove(scope);
        state.retained_bytes = state.retained_bytes.saturating_sub(retained_bytes);
        RequestIdempotencyAbandonment::Abandoned
    }

    /// Expires completed results whose deadline is not later than `now`.
    ///
    /// Pending and ambiguous mutations are never evicted silently. This
    /// function performs bounded in-memory work proportional to expired
    /// records, no external IO, and does not panic.
    pub fn expire_before(&self, now: Instant) -> RequestIdempotencyRelease {
        let mut state = self.lock_state();
        let expirations = state
            .expirations
            .keys()
            .take_while(|key| key.retained_until <= now)
            .cloned()
            .collect::<Vec<_>>();
        let mut release = RequestIdempotencyRelease::default();
        for expiration in expirations {
            state.expirations.remove(&expiration);
            let removable = state.records.get(&expiration.scope).is_some_and(|record| {
                matches!(
                    record.result,
                    RequestResultState::Completed { retained_until, .. }
                        if retained_until == expiration.retained_until
                )
            });
            if !removable {
                continue;
            }
            if let Some(record) = state.records.remove(&expiration.scope) {
                release.records = release.records.saturating_add(1);
                release.bytes = release.bytes.saturating_add(record.retained_bytes);
                state.retained_bytes = state.retained_bytes.saturating_sub(record.retained_bytes);
            }
        }
        release
    }

    /// Removes every pending and completed record for a retired logical client.
    ///
    /// This explicit administrative operation also cleans expiration entries.
    /// It performs bounded in-memory work, no external IO, and does not panic.
    pub fn remove_client(&self, client_id: &ClientId) -> RequestIdempotencyRelease {
        let mut state = self.lock_state();
        let scopes = state
            .records
            .keys()
            .filter(|scope| &scope.client_id == client_id)
            .cloned()
            .collect::<Vec<_>>();
        let mut release = RequestIdempotencyRelease::default();
        for scope in scopes {
            if let Some(record) = state.records.remove(&scope) {
                release.records = release.records.saturating_add(1);
                release.bytes = release.bytes.saturating_add(record.retained_bytes);
                state.retained_bytes = state.retained_bytes.saturating_sub(record.retained_bytes);
            }
        }
        state
            .expirations
            .retain(|expiration, _| &expiration.scope.client_id != client_id);
        release
    }

    /// Returns lock-consistent bounded store pressure without payloads.
    pub fn pressure(&self) -> RequestIdempotencyPressure {
        let state = self.lock_state();
        let completed_records = state
            .records
            .values()
            .filter(|record| matches!(record.result, RequestResultState::Completed { .. }))
            .count();
        RequestIdempotencyPressure {
            pending_records: state.records.len().saturating_sub(completed_records),
            completed_records,
            retained_bytes: state.retained_bytes,
            record_limit: self.limits.maximum_records,
            byte_limit: self.limits.maximum_total_bytes,
        }
    }

    fn validate_request(&self, request: &[u8]) -> Result<(), RequestIdempotencyError> {
        if request.is_empty() || request.len() > self.limits.maximum_request_bytes as usize {
            return Err(RequestIdempotencyError::InvalidRequestSize {
                actual: request.len(),
                maximum: self.limits.maximum_request_bytes,
            });
        }
        Ok(())
    }

    fn validate_response(&self, response: &[u8]) -> Result<(), RequestIdempotencyError> {
        if response.is_empty() || response.len() > self.limits.maximum_response_bytes as usize {
            return Err(RequestIdempotencyError::InvalidResponseSize {
                actual: response.len(),
                maximum: self.limits.maximum_response_bytes,
            });
        }
        Ok(())
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, RequestIdempotencyState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

struct RequestIdempotencyRecord {
    request: Arc<[u8]>,
    result: RequestResultState,
    retained_bytes: u64,
}

enum RequestResultState {
    Pending,
    Completed {
        response: Arc<[u8]>,
        retained_until: Instant,
    },
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RequestExpirationKey {
    retained_until: Instant,
    scope: RequestIdempotencyScope,
}

struct RequestIdempotencyState {
    records: BTreeMap<RequestIdempotencyScope, RequestIdempotencyRecord>,
    expirations: BTreeMap<RequestExpirationKey, ()>,
    retained_bytes: u64,
}

impl RequestIdempotencyState {
    const fn new() -> Self {
        Self {
            records: BTreeMap::new(),
            expirations: BTreeMap::new(),
            retained_bytes: 0,
        }
    }
}
