// SPDX-License-Identifier: Apache-2.0

//! Central daemon connection diagnostic registry.

use crate::ServerConnectionError;

/// Validated hard page bounds for payload-free daemon diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerDiagnosticLimits {
    connection_page_size: u32,
    stream_page_size: u32,
}

impl ServerDiagnosticLimits {
    /// Constructs non-zero connection and logical-stream page bounds.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed invalid-limit failure naming the zero bound.
    pub fn new(
        connection_page_size: u32,
        stream_page_size: u32,
    ) -> Result<Self, ServerConnectionError> {
        if connection_page_size == 0 {
            return Err(ServerConnectionError::InvalidConfigurationLimit {
                field: "connection_diagnostic_page_size",
            });
        }
        if stream_page_size == 0 {
            return Err(ServerConnectionError::InvalidConfigurationLimit {
                field: "stream_diagnostic_page_size",
            });
        }
        Ok(Self {
            connection_page_size,
            stream_page_size,
        })
    }

    /// Returns the largest active-connection diagnostic page.
    pub const fn connection_page_size(self) -> u32 {
        self.connection_page_size
    }

    /// Returns the largest logical-stream diagnostic page.
    pub const fn stream_page_size(self) -> u32 {
        self.stream_page_size
    }
}

/// Stable daemon-side connection diagnostic event names.
///
/// This registry defines names only; declaring a name does not install an event
/// sink or cause session/idempotency methods to emit it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ServerConnectionDiagnosticName {
    /// Client request entered the bounded pending registry.
    RequestAccepted,
    /// Pending work was cancelled independently.
    RequestCancelled,
    /// Finite response entered the control scheduler.
    RequestCompleted,
    /// Logical stream was reserved and activated.
    StreamActivated,
    /// Retained logical stream was restored after reconnect.
    StreamResumed,
    /// Logical stream was removed without affecting the connection.
    StreamClosed,
    /// Successful physical write advanced replay retention.
    WriteConfirmed,
    /// Mutating request reserved a bounded idempotency record.
    IdempotencyReserved,
    /// Completed mutating request result was replayed after retry.
    IdempotencyReplayed,
    /// Idempotency key was reused with different request content.
    IdempotencyConflict,
    /// Completed idempotency retention expired.
    IdempotencyExpired,
    /// Connection teardown released all connection-local state.
    ConnectionClosed,
}

impl ServerConnectionDiagnosticName {
    /// Complete daemon connection diagnostic registry.
    pub const ALL: [Self; 12] = [
        Self::RequestAccepted,
        Self::RequestCancelled,
        Self::RequestCompleted,
        Self::StreamActivated,
        Self::StreamResumed,
        Self::StreamClosed,
        Self::WriteConfirmed,
        Self::IdempotencyReserved,
        Self::IdempotencyReplayed,
        Self::IdempotencyConflict,
        Self::IdempotencyExpired,
        Self::ConnectionClosed,
    ];

    /// Returns the stable snake-case event name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RequestAccepted => "request_accepted",
            Self::RequestCancelled => "request_cancelled",
            Self::RequestCompleted => "request_completed",
            Self::StreamActivated => "stream_activated",
            Self::StreamResumed => "stream_resumed",
            Self::StreamClosed => "stream_closed",
            Self::WriteConfirmed => "write_confirmed",
            Self::IdempotencyReserved => "idempotency_reserved",
            Self::IdempotencyReplayed => "idempotency_replayed",
            Self::IdempotencyConflict => "idempotency_conflict",
            Self::IdempotencyExpired => "idempotency_expired",
            Self::ConnectionClosed => "connection_closed",
        }
    }
}
