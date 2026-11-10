// SPDX-License-Identifier: Apache-2.0

//! Typed, payload-redacted product SDK failures.

use std::error::Error;
use std::fmt::{Display, Formatter};

use undercurrent_protocol::{
    ProductCodecError, ProductError, ProductErrorCategory, ProductErrorDisposition,
    ProductOperation, ProductStreamError, ProductStreamKind,
};

use crate::{ResilientClientConnectError, ResilientClientError, ResilientSubscriptionError};

/// Invalid relationship between product codecs and the shared client runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductClientLimitError {
    /// Stream-resume operations have no finite deadline.
    ZeroResumeTimeout,
    /// One encoded product payload class exceeds the client payload bound.
    PayloadExceedsClient {
        /// Stable product codec field.
        field: &'static str,
        /// Inclusive SDK payload bound.
        maximum: usize,
    },
}

impl Display for ProductClientLimitError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroResumeTimeout => {
                formatter.write_str("product client resume timeout must be non-zero")
            }
            Self::PayloadExceedsClient { field, maximum } => write!(
                formatter,
                "product codec {field} exceeds client payload limit {maximum}"
            ),
        }
    }
}

impl Error for ProductClientLimitError {}

/// Stable product-client error categories for presentation adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProductClientErrorCategory {
    /// Local typed input or configured limits are invalid.
    InvalidArgument,
    /// Connection setup or the current shared connection is unavailable.
    Unavailable,
    /// Bounded client resources are saturated.
    Overloaded,
    /// A caller-owned timeout elapsed.
    Timeout,
    /// A caller cancelled or independently closed work.
    Cancelled,
    /// The daemon rejected a valid typed request or stream specification.
    ServerRejected,
    /// The peer violated product response or stream correlation.
    ProtocolViolation,
}

impl ProductClientErrorCategory {
    /// Complete stable category registry.
    pub const ALL: [Self; 7] = [
        Self::InvalidArgument,
        Self::Unavailable,
        Self::Overloaded,
        Self::Timeout,
        Self::Cancelled,
        Self::ServerRejected,
        Self::ProtocolViolation,
    ];

    /// Returns the stable snake-case category name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidArgument => "invalid_argument",
            Self::Unavailable => "unavailable",
            Self::Overloaded => "overloaded",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::ServerRejected => "server_rejected",
            Self::ProtocolViolation => "protocol_violation",
        }
    }
}

/// Failure at the typed product SDK boundary.
#[derive(Debug)]
pub enum ProductClientError {
    /// Initial resilient connection setup failed.
    Connect(ResilientClientConnectError),
    /// Negotiation omitted a capability required by the typed product layer.
    MissingCapability {
        /// Stable Undercurrent connection capability name.
        capability: &'static str,
    },
    /// Shared client request or stream-open execution failed.
    Client(ResilientClientError),
    /// A product payload could not be encoded or decoded within bounds.
    Codec(ProductCodecError),
    /// The daemon returned a typed finite-operation rejection.
    Server(ProductError),
    /// The daemon returned a typed logical-stream rejection.
    StreamRejected(ProductStreamError),
    /// A response identified a different operation than its request.
    OperationMismatch {
        /// Operation originally sent by this client.
        expected: ProductOperation,
        /// Operation named by the response, if any.
        actual: Option<ProductOperation>,
    },
    /// Stream activation identified a different stream kind.
    StreamKindMismatch {
        /// Requested stream kind.
        expected: ProductStreamKind,
        /// Activated stream kind.
        actual: ProductStreamKind,
    },
    /// A JSON stream delivered an item owned by another stream kind.
    StreamItemMismatch {
        /// Activated stream kind.
        expected: ProductStreamKind,
        /// Kind implied by the decoded item.
        actual: ProductStreamKind,
    },
    /// A persistent logical stream ended before yielding an item.
    Subscription(ResilientSubscriptionError),
}

impl ProductClientError {
    /// Returns a stable presentation category without inspecting private input.
    pub const fn category(&self) -> ProductClientErrorCategory {
        match self {
            Self::Connect(_) => ProductClientErrorCategory::Unavailable,
            Self::MissingCapability { .. } => ProductClientErrorCategory::ProtocolViolation,
            Self::Client(error) => match error {
                ResilientClientError::Client(client) => match client.category() {
                    crate::ClientErrorCategory::InvalidArgument => {
                        ProductClientErrorCategory::InvalidArgument
                    }
                    crate::ClientErrorCategory::Conflict
                    | crate::ClientErrorCategory::InvariantViolation => {
                        ProductClientErrorCategory::ProtocolViolation
                    }
                    crate::ClientErrorCategory::Overloaded => {
                        ProductClientErrorCategory::Overloaded
                    }
                    crate::ClientErrorCategory::Cancelled => ProductClientErrorCategory::Cancelled,
                    crate::ClientErrorCategory::Timeout => ProductClientErrorCategory::Timeout,
                    crate::ClientErrorCategory::Unavailable => {
                        ProductClientErrorCategory::Unavailable
                    }
                },
                ResilientClientError::Recovering
                | ResilientClientError::RecoveryFailed(_)
                | ResilientClientError::Closed => ProductClientErrorCategory::Unavailable,
            },
            Self::Codec(error) => match error.stage() {
                undercurrent_protocol::ProductCodecStage::Request
                | undercurrent_protocol::ProductCodecStage::StreamSpec => {
                    ProductClientErrorCategory::InvalidArgument
                }
                undercurrent_protocol::ProductCodecStage::Response
                | undercurrent_protocol::ProductCodecStage::StreamItem
                | undercurrent_protocol::ProductCodecStage::TerminalOutput => {
                    ProductClientErrorCategory::ProtocolViolation
                }
            },
            Self::Server(error) => match error.category {
                ProductErrorCategory::Cancelled => ProductClientErrorCategory::Cancelled,
                _ => ProductClientErrorCategory::ServerRejected,
            },
            Self::StreamRejected(error) => match error.category {
                ProductErrorCategory::Cancelled => ProductClientErrorCategory::Cancelled,
                _ => ProductClientErrorCategory::ServerRejected,
            },
            Self::OperationMismatch { .. }
            | Self::StreamKindMismatch { .. }
            | Self::StreamItemMismatch { .. } => ProductClientErrorCategory::ProtocolViolation,
            Self::Subscription(error) => match error {
                ResilientSubscriptionError::Timeout => ProductClientErrorCategory::Timeout,
                ResilientSubscriptionError::Overloaded => ProductClientErrorCategory::Overloaded,
                ResilientSubscriptionError::Unsubscribed => ProductClientErrorCategory::Cancelled,
                ResilientSubscriptionError::ClientClosed
                | ResilientSubscriptionError::RecoveryFailed(_) => {
                    ProductClientErrorCategory::Unavailable
                }
                ResilientSubscriptionError::ResumeActivationRejected(_) => {
                    ProductClientErrorCategory::ProtocolViolation
                }
                ResilientSubscriptionError::ResumeRejected(_) => {
                    ProductClientErrorCategory::ServerRejected
                }
            },
        }
    }

    /// Returns the exact server product category without inferring one for client failures.
    pub const fn product_error_category(&self) -> Option<ProductErrorCategory> {
        match self {
            Self::Server(error) => Some(error.category),
            Self::StreamRejected(error) => Some(error.category),
            _ => None,
        }
    }

    /// Returns the exact server disposition, preserving absent optional metadata.
    ///
    /// Absence is not evidence of a particular effect or retry disposition.
    pub const fn product_error_disposition(&self) -> Option<ProductErrorDisposition> {
        match self {
            Self::Server(error) => error.disposition,
            Self::StreamRejected(error) => error.disposition,
            _ => None,
        }
    }
}

impl Display for ProductClientError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(error) => Display::fmt(error, formatter),
            Self::MissingCapability { capability } => {
                write!(formatter, "product client requires capability {capability}")
            }
            Self::Client(error) => Display::fmt(error, formatter),
            Self::Codec(error) => Display::fmt(error, formatter),
            Self::Server(error) => write!(
                formatter,
                "daemon rejected {:?}: {}",
                error.category, error.message
            ),
            Self::StreamRejected(error) => write!(
                formatter,
                "daemon rejected {:?} stream: {}",
                error.kind, error.message
            ),
            Self::OperationMismatch { expected, actual } => write!(
                formatter,
                "response operation mismatch: expected {}, received {}",
                expected.as_str(),
                actual.map_or("unknown", ProductOperation::as_str)
            ),
            Self::StreamKindMismatch { expected, actual }
            | Self::StreamItemMismatch { expected, actual } => write!(
                formatter,
                "stream kind mismatch: expected {}, received {}",
                expected.as_str(),
                actual.as_str()
            ),
            Self::Subscription(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for ProductClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Connect(error) => Some(error),
            Self::Client(error) => Some(error),
            Self::Codec(error) => Some(error),
            Self::Subscription(error) => Some(error),
            Self::Server(_)
            | Self::MissingCapability { .. }
            | Self::StreamRejected(_)
            | Self::OperationMismatch { .. }
            | Self::StreamKindMismatch { .. }
            | Self::StreamItemMismatch { .. } => None,
        }
    }
}
