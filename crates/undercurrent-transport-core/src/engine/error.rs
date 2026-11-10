// SPDX-License-Identifier: Apache-2.0

//! Typed server connection failures with payload-safe presentation.

use std::error::Error;
use std::fmt::{Display, Formatter};

use undercurrent_multiplex::{MultiplexError, MultiplexErrorCategory, MultiplexOverload};
use undercurrent_wire::core::PaginationValueError;
use undercurrent_wire::{ClientId, ConnectionId, RequestId, StreamId};
use undercurrent_wire::{WireError, WireErrorCategory};

/// Stable server connection failure category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ServerConnectionErrorCategory {
    /// Negotiated or adapter configuration is inconsistent.
    Configuration,
    /// Client input violates the connection state contract.
    Protocol,
    /// A bounded runtime resource is saturated.
    Overloaded,
    /// Physical connection state is closed.
    Unavailable,
    /// Monotonic runtime state is inconsistent.
    InvariantViolation,
}

impl ServerConnectionErrorCategory {
    /// Complete stable server connection error category registry.
    pub const ALL: [Self; 5] = [
        Self::Configuration,
        Self::Protocol,
        Self::Overloaded,
        Self::Unavailable,
        Self::InvariantViolation,
    ];

    /// Returns the stable snake-case category name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Configuration => "configuration",
            Self::Protocol => "protocol",
            Self::Overloaded => "overloaded",
            Self::Unavailable => "unavailable",
            Self::InvariantViolation => "invariant_violation",
        }
    }
}

/// Failure returned by daemon-side connection orchestration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerConnectionError {
    /// A daemon composition limit is zero or otherwise unusable.
    InvalidConfigurationLimit {
        /// Stable configuration field name.
        field: &'static str,
    },
    /// Negotiated agreement omitted a required Undercurrent connection capability.
    MissingCapability {
        /// Stable capability name.
        capability: &'static str,
    },
    /// Two independently validated limits disagree at composition time.
    InconsistentConfiguration {
        /// Stable violated composition rule.
        rule: &'static str,
    },
    /// Request completion used the wrong pending operation kind.
    RequestKindMismatch {
        /// Correlated request identity.
        request_id: RequestId,
        /// Stable expected operation kind.
        expected: &'static str,
    },
    /// Stream activation did not match its reserved open request.
    StreamRequestMismatch {
        /// Correlated request identity.
        request_id: RequestId,
        /// Reserved logical stream identity.
        stream_id: StreamId,
    },
    /// Another physical connection already owns the logical client identity.
    ClientAlreadyConnected {
        /// Conflicting logical client identity.
        client_id: ClientId,
        /// Existing physical connection identity.
        connection_id: ConnectionId,
    },
    /// A physical connection identity is already registered.
    ConnectionIdInUse {
        /// Conflicting physical connection identity.
        connection_id: ConnectionId,
    },
    /// Session stream ownership and scheduler state disagree internally.
    StreamRegistryDiverged {
        /// Logical stream whose state is inconsistent.
        stream_id: StreamId,
    },
    /// Diagnostic pagination value violated its configured hard bound.
    Pagination(PaginationValueError),
    /// Multiplex core rejected state, capacity, scheduling, or replay work.
    Multiplex(MultiplexError),
    /// Binary codec rejected a message without exposing payload bytes.
    Wire(WireError),
}

impl ServerConnectionError {
    /// Returns the stable presentation and telemetry category.
    pub const fn category(&self) -> ServerConnectionErrorCategory {
        match self {
            Self::InvalidConfigurationLimit { .. }
            | Self::MissingCapability { .. }
            | Self::InconsistentConfiguration { .. } => {
                ServerConnectionErrorCategory::Configuration
            }
            Self::RequestKindMismatch { .. }
            | Self::StreamRequestMismatch { .. }
            | Self::ClientAlreadyConnected { .. }
            | Self::ConnectionIdInUse { .. }
            | Self::Pagination(_) => ServerConnectionErrorCategory::Protocol,
            Self::StreamRegistryDiverged { .. } => {
                ServerConnectionErrorCategory::InvariantViolation
            }
            Self::Multiplex(error) => match error.category() {
                MultiplexErrorCategory::InvalidArgument
                | MultiplexErrorCategory::Conflict
                | MultiplexErrorCategory::ReplayUnavailable => {
                    ServerConnectionErrorCategory::Protocol
                }
                MultiplexErrorCategory::Overloaded => ServerConnectionErrorCategory::Overloaded,
                MultiplexErrorCategory::ConnectionClosed => {
                    ServerConnectionErrorCategory::Unavailable
                }
                MultiplexErrorCategory::InvariantViolation => {
                    ServerConnectionErrorCategory::InvariantViolation
                }
            },
            Self::Wire(error) => match error.category() {
                WireErrorCategory::InvalidFrame | WireErrorCategory::FrameTooLarge => {
                    ServerConnectionErrorCategory::Protocol
                }
                WireErrorCategory::InvariantViolation => {
                    ServerConnectionErrorCategory::InvariantViolation
                }
            },
        }
    }

    /// Returns bounded overload context when a multiplex capacity rejected work.
    pub fn overload(&self) -> Option<MultiplexOverload> {
        match self {
            Self::Multiplex(error) => error.overload(),
            _ => None,
        }
    }
}

impl Display for ServerConnectionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidConfigurationLimit { field } => {
                write!(
                    formatter,
                    "server configuration limit {field} must be non-zero"
                )
            }
            Self::MissingCapability { capability } => {
                write!(formatter, "negotiated connection is missing {capability}")
            }
            Self::InconsistentConfiguration { rule } => formatter.write_str(rule),
            Self::RequestKindMismatch {
                request_id,
                expected,
            } => write!(
                formatter,
                "request {request_id} is not pending as {expected}"
            ),
            Self::StreamRequestMismatch {
                request_id,
                stream_id,
            } => write!(
                formatter,
                "request {request_id} does not reserve stream {stream_id}"
            ),
            Self::ClientAlreadyConnected {
                client_id,
                connection_id,
            } => write!(
                formatter,
                "client {client_id} is already connected as {connection_id}"
            ),
            Self::ConnectionIdInUse { connection_id } => {
                write!(formatter, "connection ID {connection_id} is already active")
            }
            Self::StreamRegistryDiverged { stream_id } => write!(
                formatter,
                "logical stream {stream_id} differs between the connection registry and scheduler"
            ),
            Self::Pagination(error) => Display::fmt(error, formatter),
            Self::Multiplex(error) => Display::fmt(error, formatter),
            Self::Wire(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for ServerConnectionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Multiplex(error) => Some(error),
            Self::Wire(error) => Some(error),
            Self::Pagination(error) => Some(error),
            Self::InvalidConfigurationLimit { .. }
            | Self::MissingCapability { .. }
            | Self::InconsistentConfiguration { .. }
            | Self::RequestKindMismatch { .. }
            | Self::StreamRequestMismatch { .. }
            | Self::ClientAlreadyConnected { .. }
            | Self::ConnectionIdInUse { .. }
            | Self::StreamRegistryDiverged { .. } => None,
        }
    }
}

impl From<MultiplexError> for ServerConnectionError {
    fn from(value: MultiplexError) -> Self {
        Self::Multiplex(value)
    }
}

impl From<WireError> for ServerConnectionError {
    fn from(value: WireError) -> Self {
        Self::Wire(value)
    }
}

impl From<PaginationValueError> for ServerConnectionError {
    fn from(value: PaginationValueError) -> Self {
        Self::Pagination(value)
    }
}
