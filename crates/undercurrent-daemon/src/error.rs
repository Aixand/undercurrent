// SPDX-License-Identifier: Apache-2.0

//! Payload-redacted daemon dispatch failures.

use std::error::Error;
use std::fmt::{Display, Formatter};

/// Invalid bounded dispatcher configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductDispatcherLimitError {
    /// One required hard bound was zero.
    ZeroLimit {
        /// Stable invalid configuration field.
        field: &'static str,
    },
}

impl Display for ProductDispatcherLimitError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroLimit { field } => {
                write!(
                    formatter,
                    "product dispatcher limit {field} must be non-zero"
                )
            }
        }
    }
}

impl Error for ProductDispatcherLimitError {}

/// Invalid bounded dispatch-telemetry configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductDispatchTelemetryLimitError;

impl Display for ProductDispatchTelemetryLimitError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("product dispatch telemetry retention must be non-zero")
    }
}

impl Error for ProductDispatchTelemetryLimitError {}

/// Payload-free bounded dispatcher failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductDispatchFailure {
    /// Dispatcher shutdown has begun.
    Closed,
    /// Bounded job queue cannot admit another action.
    Saturated {
        /// Inclusive queued job limit.
        limit: usize,
    },
}

impl Display for ProductDispatchFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => formatter.write_str("product dispatcher is closed"),
            Self::Saturated { limit } => {
                write!(
                    formatter,
                    "product dispatcher queue capacity {limit} is exhausted"
                )
            }
        }
    }
}

impl Error for ProductDispatchFailure {}

/// Failure constructing a bounded product dispatcher.
#[derive(Debug)]
pub enum ProductDispatcherCreateError {
    /// Hard limits were invalid.
    Limits(ProductDispatcherLimitError),
    /// Even the fixed internal error response exceeds codec limits.
    Codec(undercurrent_protocol::ProductCodecError),
    /// A named bounded worker thread could not be started.
    Worker {
        /// Zero-based worker index.
        index: usize,
        /// Operating-system thread creation failure.
        source: std::io::Error,
    },
}

impl Display for ProductDispatcherCreateError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Limits(error) => Display::fmt(error, formatter),
            Self::Codec(_) => formatter
                .write_str("product response codec cannot encode the internal failure envelope"),
            Self::Worker { index, .. } => {
                write!(
                    formatter,
                    "product dispatcher worker {index} could not start"
                )
            }
        }
    }
}

impl Error for ProductDispatcherCreateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Limits(error) => Some(error),
            Self::Codec(error) => Some(error),
            Self::Worker { source, .. } => Some(source),
        }
    }
}

impl From<ProductDispatcherLimitError> for ProductDispatcherCreateError {
    fn from(value: ProductDispatcherLimitError) -> Self {
        Self::Limits(value)
    }
}

/// Payload-redacted failure publishing one logical stream item.
pub struct ProductStreamPublishError {
    kind: ProductStreamPublishErrorKind,
}

impl ProductStreamPublishError {
    pub(crate) const fn new(kind: ProductStreamPublishErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable failed publication stage.
    pub const fn stage(&self) -> ProductStreamPublishStage {
        match &self.kind {
            ProductStreamPublishErrorKind::Closed => ProductStreamPublishStage::Connection,
            ProductStreamPublishErrorKind::SequenceExhausted => ProductStreamPublishStage::Sequence,
            ProductStreamPublishErrorKind::Encoding(_) => ProductStreamPublishStage::Encoding,
            ProductStreamPublishErrorKind::Connection(error)
                if matches!(
                    error.category(),
                    undercurrent_transport_core::ServerConnectionErrorCategory::Overloaded
                ) =>
            {
                ProductStreamPublishStage::Backpressure
            }
            ProductStreamPublishErrorKind::Connection(_) => ProductStreamPublishStage::Connection,
        }
    }

    /// Returns whether the exact item may be retried after queue-drain wake.
    pub const fn is_backpressured(&self) -> bool {
        matches!(self.stage(), ProductStreamPublishStage::Backpressure)
    }
}

impl std::fmt::Debug for ProductStreamPublishError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProductStreamPublishError")
            .field("stage", &self.stage())
            .finish_non_exhaustive()
    }
}

impl Display for ProductStreamPublishError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "product stream publication failed at {:?}",
            self.stage()
        )
    }
}

impl Error for ProductStreamPublishError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            ProductStreamPublishErrorKind::Encoding(source) => Some(source),
            ProductStreamPublishErrorKind::Connection(source) => Some(source),
            ProductStreamPublishErrorKind::Closed
            | ProductStreamPublishErrorKind::SequenceExhausted => None,
        }
    }
}

/// Stable publication failure stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductStreamPublishStage {
    /// Product item encoding.
    Encoding,
    /// Monotonic sequence allocation.
    Sequence,
    /// Bounded scheduler capacity is temporarily exhausted.
    Backpressure,
    /// Connection scheduler admission.
    Connection,
}

pub(crate) enum ProductStreamPublishErrorKind {
    Closed,
    SequenceExhausted,
    Encoding(undercurrent_protocol::ProductCodecError),
    Connection(undercurrent_transport_core::ServerConnectionError),
}
