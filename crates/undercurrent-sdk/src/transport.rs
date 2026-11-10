// SPDX-License-Identifier: Apache-2.0

//! Transport boundary used by the shared-connection SDK runtime.

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use crate::{ClientOverload, ClientTransportInbound, ClientTransportOutbound};

/// Stable client transport operations used by diagnostics and errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClientTransportOperation {
    /// Establish and negotiate one replacement physical connection.
    Connect,
    /// Write one semantic message through the physical connection writer.
    Send,
    /// Read one semantic message through the physical connection reader.
    Receive,
    /// Close the physical connection and unblock pending IO.
    Close,
}

impl ClientTransportOperation {
    /// Complete transport operation registry.
    pub const ALL: [Self; 4] = [Self::Connect, Self::Send, Self::Receive, Self::Close];

    /// Returns the stable snake-case operation name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::Send => "send",
            Self::Receive => "receive",
            Self::Close => "close",
        }
    }
}

/// Redacted client transport failure retaining its concrete source internally.
#[derive(Clone)]
pub struct ClientTransportError {
    operation: ClientTransportOperation,
    overload: Option<ClientOverload>,
    source: Arc<dyn Error + Send + Sync>,
}

impl ClientTransportError {
    /// Wraps one concrete adapter failure at the SDK boundary.
    pub fn new(
        operation: ClientTransportOperation,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            operation,
            overload: None,
            source: Arc::new(source),
        }
    }

    pub(crate) fn with_overload(
        operation: ClientTransportOperation,
        source: impl Error + Send + Sync + 'static,
        overload: ClientOverload,
    ) -> Self {
        Self {
            operation,
            overload: Some(overload),
            source: Arc::new(source),
        }
    }

    /// Wraps one concrete adapter overload with bounded caller guidance.
    #[allow(clippy::too_many_arguments)]
    pub fn overloaded(
        operation: ClientTransportOperation,
        source: impl Error + Send + Sync + 'static,
        resource: &'static str,
        limit: Option<u64>,
        current: Option<u64>,
        requested: Option<u64>,
        retry: crate::ClientRetryGuidance,
    ) -> Self {
        Self::with_overload(
            operation,
            source,
            ClientOverload::new(resource, limit, current, requested, retry),
        )
    }

    /// Returns the stable failed adapter operation.
    pub const fn operation(&self) -> ClientTransportOperation {
        self.operation
    }

    /// Returns bounded redacted capacity context and safe retry guidance.
    pub const fn overload(&self) -> Option<ClientOverload> {
        self.overload
    }
}

impl Debug for ClientTransportError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientTransportError")
            .field("operation", &self.operation)
            .field("overload", &self.overload)
            .finish_non_exhaustive()
    }
}

impl Display for ClientTransportError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "client transport {} operation failed",
            self.operation.as_str()
        )
    }
}

impl Error for ClientTransportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// One negotiated physical connection used by exactly one SDK runtime.
pub trait ClientTransport: Send + Sync {
    /// Writes one semantic message. Calls are serialized by the SDK writer.
    ///
    /// # Errors
    ///
    /// Returns a concrete redacted transport failure.
    fn send(&self, message: ClientTransportOutbound) -> Result<(), ClientTransportError>;

    /// Blocks until one complete semantic server message is decoded.
    ///
    /// `close` must unblock this method. Exactly one SDK reader calls it.
    ///
    /// # Errors
    ///
    /// Returns a concrete redacted transport failure.
    fn receive(&self) -> Result<ClientTransportInbound, ClientTransportError>;

    /// Closes the physical connection and unblocks `send` and `receive`.
    fn close(&self);
}
