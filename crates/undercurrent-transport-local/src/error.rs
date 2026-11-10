// SPDX-License-Identifier: Apache-2.0

//! Payload- and credential-redacted Local server failures.

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::io;
use std::sync::Arc;

/// Stable transport failure category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalServerErrorCategory {
    /// Static listener or limit configuration is invalid.
    Configuration,
    /// The peer identity check was not authenticated or authorized.
    Authentication,
    /// Endpoint ownership or peer validation failed closed.
    Security,
    /// Peer input violates the Local, UCHS, or UCMP contract.
    Protocol,
    /// A configured time bound elapsed.
    Timeout,
    /// A bounded runtime resource was saturated.
    Capacity,
    /// Local IPC IO failed.
    Transport,
    /// Internal server composition failed.
    Internal,
}

/// Stable server adapter stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalServerStage {
    /// Bind or accept a local listener.
    Listener,
    /// Authenticate the transport session.
    Authenticate,
    /// Exchange UCHS negotiation frames.
    Negotiate,
    /// Compose and register UCMP connection state.
    ComposeConnection,
    /// Decode and dispatch one client frame.
    Dispatch,
    /// Send and confirm one server frame.
    Write,
    /// Stop the connection or listener.
    Shutdown,
    /// Start a bounded runtime worker.
    SpawnWorker,
}

/// Redacted error retaining caller-actionable context and a private source chain.
///
/// `Display` and `Debug` omit source text, but [`Error::source`] exposes the
/// retained concrete error to trusted callers. Recursively formatting that
/// chain can disclose private details. Equality compares category, stage, and
/// optional I/O kind, not the source.
#[derive(Clone)]
pub struct LocalServerError {
    category: LocalServerErrorCategory,
    stage: LocalServerStage,
    io_kind: Option<io::ErrorKind>,
    source: Option<Arc<dyn Error + Send + Sync>>,
}

impl LocalServerError {
    /// Constructs a redacted adapter failure.
    pub const fn new(category: LocalServerErrorCategory, stage: LocalServerStage) -> Self {
        Self {
            category,
            stage,
            io_kind: None,
            source: None,
        }
    }

    /// Retains only the stable, payload-free operating-system error class.
    pub const fn with_io_kind(mut self, io_kind: io::ErrorKind) -> Self {
        self.io_kind = Some(io_kind);
        self
    }

    /// Retains the concrete adapter failure without exposing its display text.
    pub fn with_source(mut self, source: impl Error + Send + Sync + 'static) -> Self {
        self.source = Some(Arc::new(source));
        self
    }

    /// Returns the stable failure category.
    pub const fn category(&self) -> LocalServerErrorCategory {
        self.category
    }

    /// Returns the stable failed stage.
    pub const fn stage(&self) -> LocalServerStage {
        self.stage
    }

    /// Returns the safe operating-system error class, when an IO edge supplied one.
    pub const fn io_kind(&self) -> Option<io::ErrorKind> {
        self.io_kind
    }
}

impl Debug for LocalServerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalServerError")
            .field("category", &self.category)
            .field("stage", &self.stage)
            .field("io_kind", &self.io_kind)
            .field("source", &self.source.as_ref().map(|_| "redacted"))
            .finish()
    }
}

impl PartialEq for LocalServerError {
    fn eq(&self, other: &Self) -> bool {
        self.category == other.category
            && self.stage == other.stage
            && self.io_kind == other.io_kind
    }
}

impl Eq for LocalServerError {}

impl Display for LocalServerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Local server {:?} failure at {:?}",
            self.category, self.stage
        )
    }
}

impl Error for LocalServerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn real_local_sources_are_retained_for_adapter_stages_but_redacted() {
        for stage in [
            LocalServerStage::Authenticate,
            LocalServerStage::Negotiate,
            LocalServerStage::Write,
            LocalServerStage::Dispatch,
        ] {
            let source = io::Error::other("PRIVATE-LOCAL-SOURCE");
            let error = LocalServerError::new(LocalServerErrorCategory::Transport, stage)
                .with_source(source);

            assert_eq!(error.stage(), stage);
            assert!(
                error
                    .source()
                    .and_then(|source| { source.downcast_ref::<io::Error>() })
                    .is_some()
            );
            assert!(!error.to_string().contains("PRIVATE-LOCAL-SOURCE"));
            assert!(!format!("{error:?}").contains("PRIVATE-LOCAL-SOURCE"));
        }
    }
}
