// SPDX-License-Identifier: Apache-2.0

//! Product-work dispatch boundary outside connection and socket mechanics.

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use crate::{ServerAction, ServerConnectionSession};

/// Payload-redacted failure accepting product work from a connection.
pub struct ServerDispatchError {
    source: Box<dyn Error + Send + Sync>,
}

impl ServerDispatchError {
    /// Wraps one concrete bounded-dispatch failure.
    ///
    /// The source remains available to daemon diagnostics but is omitted from
    /// `Display` and `Debug`. This function performs no IO and does not panic.
    pub fn new(source: impl Error + Send + Sync + 'static) -> Self {
        Self {
            source: Box::new(source),
        }
    }
}

impl Debug for ServerDispatchError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerDispatchError")
            .finish_non_exhaustive()
    }
}

impl Display for ServerDispatchError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("server product dispatch failed")
    }
}

impl Error for ServerDispatchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// Accepts validated product work without exposing physical transport details.
pub trait ServerActionDispatcher: Send + Sync {
    /// Accepts one connection-scoped action for bounded execution.
    ///
    /// The dispatcher owns product decoding, authorization, application use
    /// case execution, cancellation, and response or stream settlement through
    /// `connection`. It must return only after the action has entered a bounded
    /// execution resource; it must not retain raw private payloads in logs or
    /// diagnostics. Calls for independent requests may be processed
    /// concurrently by the implementation.
    ///
    /// # Errors
    ///
    /// Returns a redacted admission or infrastructure failure. The connection
    /// adapter treats this as fatal because ownership of accepted work would
    /// otherwise be ambiguous.
    fn dispatch(
        &self,
        connection: Arc<ServerConnectionSession>,
        action: ServerAction,
    ) -> Result<(), ServerDispatchError>;
}
