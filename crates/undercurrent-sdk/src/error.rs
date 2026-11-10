// SPDX-License-Identifier: Apache-2.0

//! Typed SDK failures independent of concrete socket technology.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use undercurrent_multiplex::{MultiplexError, PendingResponseError};

use crate::TraceContextGenerationError;

/// Safe action an SDK caller may take after a bounded overload rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClientRetryGuidance {
    /// Retry only after an owned request, stream, or connection slot is released.
    AfterCapacityAvailable,
    /// Retry only after queued work has made observable progress.
    AfterQueueDrain,
    /// Retry only after the peer returns flow-control credit.
    AfterWindowUpdate,
    /// Retry connection admission using the configured bounded backoff policy.
    WithBackoff,
    /// The unchanged operation cannot succeed and must not be retried.
    DoNotRetryUnchanged,
}

impl ClientRetryGuidance {
    /// Returns the stable payload-safe guidance name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AfterCapacityAvailable => "after_capacity_available",
            Self::AfterQueueDrain => "after_queue_drain",
            Self::AfterWindowUpdate => "after_window_update",
            Self::WithBackoff => "with_backoff",
            Self::DoNotRetryUnchanged => "do_not_retry_unchanged",
        }
    }
}

/// Identifier- and payload-free overload context exposed by the public SDK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientOverload {
    resource: &'static str,
    limit: Option<u64>,
    current: Option<u64>,
    requested: Option<u64>,
    retry: ClientRetryGuidance,
}

impl ClientOverload {
    pub(crate) const fn new(
        resource: &'static str,
        limit: Option<u64>,
        current: Option<u64>,
        requested: Option<u64>,
        retry: ClientRetryGuidance,
    ) -> Self {
        Self {
            resource,
            limit,
            current,
            requested,
            retry,
        }
    }

    /// Returns the stable bounded resource name.
    pub const fn resource(self) -> &'static str {
        self.resource
    }

    /// Returns the inclusive configured limit when exposed safely.
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

    /// Returns the only safe retry action implied by this rejection.
    pub const fn retry(self) -> ClientRetryGuidance {
        self.retry
    }
}

/// Payload-safe SDK wrapper for a private multiplex-router failure.
#[derive(Debug)]
pub struct ClientMultiplexError {
    category: ClientErrorCategory,
    source: MultiplexError,
}

impl ClientMultiplexError {
    /// Returns the stable caller-facing category.
    pub const fn category(&self) -> ClientErrorCategory {
        self.category
    }
}

impl Display for ClientMultiplexError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.source, formatter)
    }
}

impl Error for ClientMultiplexError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

/// Payload-safe SDK wrapper for a private finite-response wait failure.
#[derive(Debug)]
pub struct ClientPendingError {
    category: ClientErrorCategory,
    source: PendingResponseError,
}

impl ClientPendingError {
    /// Returns the stable caller-facing category.
    pub const fn category(&self) -> ClientErrorCategory {
        self.category
    }
}

impl Display for ClientPendingError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.source, formatter)
    }
}

impl Error for ClientPendingError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

/// Stable category for SDK callers and presentation adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClientErrorCategory {
    /// Request or subscription input violates negotiated limits.
    InvalidArgument,
    /// Active request or stream identity conflicts.
    Conflict,
    /// Bounded connection resources are saturated.
    Overloaded,
    /// Operation was cancelled by its caller.
    Cancelled,
    /// Caller timeout elapsed.
    Timeout,
    /// Physical connection is closed or unavailable.
    Unavailable,
    /// Internal correlation invariant failed.
    InvariantViolation,
}

impl ClientErrorCategory {
    /// Complete stable SDK error category registry.
    pub const ALL: [Self; 7] = [
        Self::InvalidArgument,
        Self::Conflict,
        Self::Overloaded,
        Self::Cancelled,
        Self::Timeout,
        Self::Unavailable,
        Self::InvariantViolation,
    ];

    /// Returns the stable snake-case category name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidArgument => "invalid_argument",
            Self::Conflict => "conflict",
            Self::Overloaded => "overloaded",
            Self::Cancelled => "cancelled",
            Self::Timeout => "timeout",
            Self::Unavailable => "unavailable",
            Self::InvariantViolation => "invariant_violation",
        }
    }
}

/// Failure returned by shared-connection client operations.
#[derive(Debug)]
pub enum ClientError {
    /// Request or subscription payload exceeds negotiated frame bytes.
    InvalidPayloadSize {
        /// Observed product payload bytes.
        actual: usize,
        /// Inclusive negotiated frame maximum before adapter overhead.
        maximum: u32,
    },
    /// Bounded writer command queue is saturated.
    WriterSaturated {
        /// Inclusive configured message capacity.
        capacity: usize,
    },
    /// Multiplex router rejected registration or delivery.
    Multiplex(ClientMultiplexError),
    /// Finite response wait ended without a response payload.
    Pending(ClientPendingError),
    /// Concrete physical transport failure shared by every affected waiter.
    Transport {
        /// Stable category materialized so existing const categorization remains available.
        category: ClientErrorCategory,
        /// Shared redacted failure and concrete internal source chain.
        source: Arc<crate::ClientTransportError>,
    },
    /// Client-owned distributed trace context could not advance.
    TraceContext(TraceContextGenerationError),
}

impl ClientError {
    /// Returns the stable category used by callers and presentation adapters.
    pub const fn category(&self) -> ClientErrorCategory {
        match self {
            Self::InvalidPayloadSize { .. } => ClientErrorCategory::InvalidArgument,
            Self::WriterSaturated { .. } => ClientErrorCategory::Overloaded,
            Self::Multiplex(error) => error.category(),
            Self::Pending(error) => error.category(),
            Self::Transport { category, .. } => *category,
            Self::TraceContext(_) => ClientErrorCategory::InvariantViolation,
        }
    }

    /// Returns bounded redacted overload context with explicit retry guidance.
    pub fn overload(&self) -> Option<ClientOverload> {
        match self {
            Self::WriterSaturated { capacity } => Some(ClientOverload::new(
                "client_writer_queue_messages",
                Some(usize_as_u64(*capacity)),
                Some(usize_as_u64(*capacity)),
                Some(1),
                ClientRetryGuidance::AfterQueueDrain,
            )),
            Self::Multiplex(error) => error.source.overload().map(client_overload_from_multiplex),
            Self::Transport { source, .. } => source.overload(),
            Self::InvalidPayloadSize { .. } | Self::Pending(_) | Self::TraceContext(_) => None,
        }
    }
}

impl Display for ClientError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidPayloadSize { actual, maximum } => write!(
                formatter,
                "client payload size {actual} is outside 1..={maximum}"
            ),
            Self::WriterSaturated { capacity } => {
                write!(
                    formatter,
                    "client writer queue capacity {capacity} is saturated"
                )
            }
            Self::Multiplex(error) => Display::fmt(error, formatter),
            Self::Pending(error) => Display::fmt(error, formatter),
            Self::Transport { source, .. } => Display::fmt(source, formatter),
            Self::TraceContext(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for ClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Multiplex(error) => Some(error),
            Self::Pending(error) => Some(error),
            Self::Transport { source, .. } => Some(source.as_ref()),
            Self::TraceContext(error) => Some(error),
            Self::InvalidPayloadSize { .. } | Self::WriterSaturated { .. } => None,
        }
    }
}

impl ClientError {
    pub(crate) fn transport(source: Arc<crate::ClientTransportError>) -> Self {
        let category = if source.overload().is_some() {
            ClientErrorCategory::Overloaded
        } else {
            ClientErrorCategory::Unavailable
        };
        Self::Transport { category, source }
    }
}

impl From<TraceContextGenerationError> for ClientError {
    fn from(value: TraceContextGenerationError) -> Self {
        Self::TraceContext(value)
    }
}

impl From<MultiplexError> for ClientError {
    fn from(value: MultiplexError) -> Self {
        let category = match value.category() {
            undercurrent_multiplex::MultiplexErrorCategory::InvalidArgument => {
                ClientErrorCategory::InvalidArgument
            }
            undercurrent_multiplex::MultiplexErrorCategory::Conflict => {
                ClientErrorCategory::Conflict
            }
            undercurrent_multiplex::MultiplexErrorCategory::Overloaded => {
                ClientErrorCategory::Overloaded
            }
            undercurrent_multiplex::MultiplexErrorCategory::ReplayUnavailable
            | undercurrent_multiplex::MultiplexErrorCategory::ConnectionClosed => {
                ClientErrorCategory::Unavailable
            }
            undercurrent_multiplex::MultiplexErrorCategory::InvariantViolation => {
                ClientErrorCategory::InvariantViolation
            }
        };
        Self::Multiplex(ClientMultiplexError {
            category,
            source: value,
        })
    }
}

fn client_overload_from_multiplex(
    context: undercurrent_multiplex::MultiplexOverload,
) -> ClientOverload {
    ClientOverload::new(
        context.resource(),
        context.limit(),
        context.current(),
        context.requested(),
        match context.retry() {
            undercurrent_multiplex::MultiplexRetryGuidance::AfterCapacityAvailable => {
                ClientRetryGuidance::AfterCapacityAvailable
            }
            undercurrent_multiplex::MultiplexRetryGuidance::AfterQueueDrain => {
                ClientRetryGuidance::AfterQueueDrain
            }
            undercurrent_multiplex::MultiplexRetryGuidance::AfterWindowUpdate => {
                ClientRetryGuidance::AfterWindowUpdate
            }
            undercurrent_multiplex::MultiplexRetryGuidance::DoNotRetryUnchanged => {
                ClientRetryGuidance::DoNotRetryUnchanged
            }
        },
    )
}

fn usize_as_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

impl From<PendingResponseError> for ClientError {
    fn from(value: PendingResponseError) -> Self {
        let category = match value {
            PendingResponseError::Cancelled => ClientErrorCategory::Cancelled,
            PendingResponseError::Timeout => ClientErrorCategory::Timeout,
            PendingResponseError::ConnectionClosed | PendingResponseError::RouteDisconnected => {
                ClientErrorCategory::Unavailable
            }
        };
        Self::Pending(ClientPendingError {
            category,
            source: value,
        })
    }
}
