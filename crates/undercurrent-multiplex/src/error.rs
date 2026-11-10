// SPDX-License-Identifier: Apache-2.0

//! Typed multiplex runtime failures.
//!
//! Categories describe local validation, ownership, and capacity failures, not
//! remote application effects. Use [`MultiplexError::overload`] for identifier-free
//! resource context. Its retry guidance never authorizes replaying an ambiguous
//! mutation. Replay gaps are successful selection results rather than errors.

use std::error::Error;
use std::fmt::{Display, Formatter};

use undercurrent_wire::{RequestId, StreamId};

/// Stable category for multiplex runtime and presentation adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MultiplexErrorCategory {
    /// Input violates a protocol or configured limit contract.
    InvalidArgument,
    /// An active request or stream conflicts with the operation.
    Conflict,
    /// A bounded queue, registry, or credit window is exhausted.
    Overloaded,
    /// Requested stream replay cannot be satisfied.
    /// No current [`MultiplexError`] variant maps to this category; unavailable
    /// retention is represented by [`crate::ReplayResult::Gap`] or
    /// [`crate::ReplayResult::GapWithItems`].
    ReplayUnavailable,
    /// Physical connection is no longer usable.
    ConnectionClosed,
    /// Monotonic runtime state reached an impossible condition.
    InvariantViolation,
}

/// Local prerequisite for another attempt after a bounded multiplex overload.
///
/// This guidance does not classify remote effects. Queue drain also cannot make
/// an item larger than an empty queue's byte budget fit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MultiplexRetryGuidance {
    /// Retry only after an owned slot or registry entry has been released.
    AfterCapacityAvailable,
    /// Retry only after queued work has made observable progress.
    AfterQueueDrain,
    /// Retry only after the peer has returned flow-control credit.
    AfterWindowUpdate,
    /// The unchanged operation cannot succeed and must not be retried.
    DoNotRetryUnchanged,
}

impl MultiplexRetryGuidance {
    /// Returns the stable payload-safe guidance name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AfterCapacityAvailable => "after_capacity_available",
            Self::AfterQueueDrain => "after_queue_drain",
            Self::AfterWindowUpdate => "after_window_update",
            Self::DoNotRetryUnchanged => "do_not_retry_unchanged",
        }
    }
}

/// Bounded, identifier-free context for one multiplex overload rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultiplexOverload {
    resource: &'static str,
    limit: Option<u64>,
    current: Option<u64>,
    requested: Option<u64>,
    retry: MultiplexRetryGuidance,
}

impl MultiplexOverload {
    /// Returns the stable bounded resource name.
    pub const fn resource(self) -> &'static str {
        self.resource
    }

    /// Returns the inclusive configured limit when the rejection exposes one.
    pub const fn limit(self) -> Option<u64> {
        self.limit
    }

    /// Returns current retained bytes, messages, slots, or credit when known.
    pub const fn current(self) -> Option<u64> {
        self.current
    }

    /// Returns bytes, messages, slots, or credit requested by the rejected work.
    pub const fn requested(self) -> Option<u64> {
        self.requested
    }

    /// Returns the only safe retry action implied by this local rejection.
    pub const fn retry(self) -> MultiplexRetryGuidance {
        self.retry
    }
}

/// Typed failure from multiplex connection operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultiplexError {
    /// A caller-supplied product runtime limit was zero.
    InvalidLimit {
        /// Stable invalid limit name.
        field: &'static str,
    },
    /// Encoded frame is empty or exceeds the negotiated maximum.
    InvalidFrameSize {
        /// Observed encoded bytes.
        actual: u64,
        /// Inclusive negotiated maximum.
        maximum: u64,
    },
    /// Active request ID was registered more than once.
    RequestIdInUse {
        /// Conflicting request ID.
        request_id: RequestId,
    },
    /// Response did not match a pending request.
    UnknownRequest {
        /// Unmatched request ID.
        request_id: RequestId,
    },
    /// Active stream ID was registered more than once.
    StreamIdInUse {
        /// Conflicting stream ID.
        stream_id: StreamId,
    },
    /// Operation referenced an unknown stream.
    UnknownStream {
        /// Unmatched stream ID.
        stream_id: StreamId,
    },
    /// Registry reached its negotiated item limit.
    CapacityExceeded {
        /// Stable bounded resource name.
        resource: &'static str,
        /// Inclusive configured limit.
        limit: usize,
    },
    /// A reliable frame could not enter a bounded queue.
    QueueSaturated {
        /// Stable saturated queue name.
        queue: &'static str,
        /// Inclusive configured byte budget.
        byte_limit: u64,
        /// Bytes already retained in that queue.
        queued_bytes: u64,
        /// Bytes requested by the rejected frame.
        requested_bytes: u64,
    },
    /// A reliable frame could not enter a message-count-bounded queue.
    QueueMessageSaturated {
        /// Stable saturated queue name.
        queue: &'static str,
        /// Inclusive configured message budget.
        message_limit: usize,
        /// Messages already retained in that queue.
        queued_messages: usize,
    },
    /// Flow-control credit is insufficient for the next frame.
    CreditExhausted {
        /// Stream affected by per-stream credit, absent for connection credit.
        stream_id: Option<StreamId>,
        /// Currently available credit.
        available_bytes: u64,
        /// Credit required by the blocked frame.
        required_bytes: u64,
    },
    /// Credit update exceeded the negotiated window maximum.
    CreditUpdateExceeded {
        /// Stable window name.
        window: &'static str,
        /// Requested new credit after applying the update.
        requested: u64,
        /// Inclusive configured maximum.
        maximum: u64,
    },
    /// Stream sequence did not strictly advance.
    StreamSequenceOutOfOrder {
        /// Affected logical stream.
        stream_id: StreamId,
        /// Previous accepted sequence.
        previous: u64,
        /// Rejected sequence.
        received: u64,
    },
    /// Requested resume sequence is later than retained stream truth.
    FutureResumeSequence {
        /// Affected logical stream.
        stream_id: StreamId,
        /// Requested last acknowledged sequence.
        requested: u64,
        /// Latest sequence known to the journal.
        latest: u64,
    },
    /// Connection was closed and released its bounded runtime state.
    ConnectionClosed,
    /// Internal monotonic state cannot advance without wrapping.
    CounterExhausted {
        /// Stable exhausted counter name.
        counter: &'static str,
    },
}

impl MultiplexError {
    /// Returns the stable category used by protocol and presentation adapters.
    pub const fn category(&self) -> MultiplexErrorCategory {
        match self {
            Self::InvalidLimit { .. }
            | Self::InvalidFrameSize { .. }
            | Self::StreamSequenceOutOfOrder { .. }
            | Self::FutureResumeSequence { .. } => MultiplexErrorCategory::InvalidArgument,
            Self::RequestIdInUse { .. }
            | Self::UnknownRequest { .. }
            | Self::StreamIdInUse { .. }
            | Self::UnknownStream { .. } => MultiplexErrorCategory::Conflict,
            Self::CapacityExceeded { .. }
            | Self::QueueSaturated { .. }
            | Self::QueueMessageSaturated { .. }
            | Self::CreditExhausted { .. }
            | Self::CreditUpdateExceeded { .. } => MultiplexErrorCategory::Overloaded,
            Self::ConnectionClosed => MultiplexErrorCategory::ConnectionClosed,
            Self::CounterExhausted { .. } => MultiplexErrorCategory::InvariantViolation,
        }
    }

    /// Returns redacted bounded context and retry guidance for overloads.
    ///
    /// Request IDs, stream IDs, payload bytes, and credentials are deliberately
    /// excluded. Non-overload errors return `None`.
    pub fn overload(&self) -> Option<MultiplexOverload> {
        let overload = match self {
            Self::CapacityExceeded { resource, limit } => MultiplexOverload {
                resource,
                limit: Some(usize_as_u64(*limit)),
                current: Some(usize_as_u64(*limit)),
                requested: Some(1),
                retry: MultiplexRetryGuidance::AfterCapacityAvailable,
            },
            Self::QueueSaturated {
                queue,
                byte_limit,
                queued_bytes,
                requested_bytes,
            } => MultiplexOverload {
                resource: queue,
                limit: Some(*byte_limit),
                current: Some(*queued_bytes),
                requested: Some(*requested_bytes),
                retry: MultiplexRetryGuidance::AfterQueueDrain,
            },
            Self::QueueMessageSaturated {
                queue,
                message_limit,
                queued_messages,
            } => MultiplexOverload {
                resource: queue,
                limit: Some(usize_as_u64(*message_limit)),
                current: Some(usize_as_u64(*queued_messages)),
                requested: Some(1),
                retry: MultiplexRetryGuidance::AfterQueueDrain,
            },
            Self::CreditExhausted {
                stream_id,
                available_bytes,
                required_bytes,
            } => MultiplexOverload {
                resource: if stream_id.is_some() {
                    "stream_credit_bytes"
                } else {
                    "connection_credit_bytes"
                },
                limit: None,
                current: Some(*available_bytes),
                requested: Some(*required_bytes),
                retry: MultiplexRetryGuidance::AfterWindowUpdate,
            },
            Self::CreditUpdateExceeded {
                window,
                requested,
                maximum,
            } => MultiplexOverload {
                resource: window,
                limit: Some(*maximum),
                current: None,
                requested: Some(*requested),
                retry: MultiplexRetryGuidance::DoNotRetryUnchanged,
            },
            Self::InvalidLimit { .. }
            | Self::InvalidFrameSize { .. }
            | Self::RequestIdInUse { .. }
            | Self::UnknownRequest { .. }
            | Self::StreamIdInUse { .. }
            | Self::UnknownStream { .. }
            | Self::StreamSequenceOutOfOrder { .. }
            | Self::FutureResumeSequence { .. }
            | Self::ConnectionClosed
            | Self::CounterExhausted { .. } => return None,
        };
        Some(overload)
    }
}

fn usize_as_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

impl Display for MultiplexError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLimit { field } => write!(formatter, "{field} must be non-zero"),
            Self::InvalidFrameSize { actual, maximum } => {
                write!(
                    formatter,
                    "encoded frame size {actual} is outside 1..={maximum}"
                )
            }
            Self::RequestIdInUse { request_id } => {
                write!(formatter, "request ID {request_id} is already active")
            }
            Self::UnknownRequest { request_id } => {
                write!(formatter, "request ID {request_id} is not pending")
            }
            Self::StreamIdInUse { stream_id } => {
                write!(formatter, "stream ID {stream_id} is already active")
            }
            Self::UnknownStream { stream_id } => {
                write!(formatter, "stream ID {stream_id} is not active")
            }
            Self::CapacityExceeded { resource, limit } => {
                write!(formatter, "{resource} capacity {limit} is exhausted")
            }
            Self::QueueSaturated {
                queue,
                byte_limit,
                queued_bytes,
                requested_bytes,
            } => write!(
                formatter,
                "{queue} queue budget {byte_limit} is saturated at {queued_bytes}; rejected {requested_bytes} bytes"
            ),
            Self::QueueMessageSaturated {
                queue,
                message_limit,
                queued_messages,
            } => write!(
                formatter,
                "{queue} queue message budget {message_limit} is saturated at {queued_messages}"
            ),
            Self::CreditExhausted {
                stream_id,
                available_bytes,
                required_bytes,
            } => match stream_id {
                Some(stream_id) => write!(
                    formatter,
                    "stream {stream_id} has {available_bytes} credit bytes but needs {required_bytes}"
                ),
                None => write!(
                    formatter,
                    "connection has {available_bytes} credit bytes but needs {required_bytes}"
                ),
            },
            Self::CreditUpdateExceeded {
                window,
                requested,
                maximum,
            } => write!(
                formatter,
                "{window} credit update requested {requested} bytes above maximum {maximum}"
            ),
            Self::StreamSequenceOutOfOrder {
                stream_id,
                previous,
                received,
            } => write!(
                formatter,
                "stream {stream_id} sequence {received} did not advance beyond {previous}"
            ),
            Self::FutureResumeSequence {
                stream_id,
                requested,
                latest,
            } => write!(
                formatter,
                "stream {stream_id} resume sequence {requested} exceeds latest {latest}"
            ),
            Self::ConnectionClosed => formatter.write_str("multiplex connection is closed"),
            Self::CounterExhausted { counter } => write!(formatter, "{counter} is exhausted"),
        }
    }
}

impl Error for MultiplexError {}
