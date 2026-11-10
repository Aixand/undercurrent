// SPDX-License-Identifier: Apache-2.0

//! Blocking Rust client for an external Undercurrent daemon.
//!
//! Start with [`StandaloneClientProfile`], [`LocalClientConfig`], and
//! [`LocalClientConnector`], then construct a [`ProductClient`]. Its clones,
//! finite requests, and [`ProductSubscription`] handles share one connection
//! and recovery supervisor. Product DTOs are owned by `undercurrent-protocol`.
//!
//! Subscriptions retain delivery checkpoints across physical reconnects; finite
//! requests are never automatically replayed. A timeout or lost mutation response
//! does not prove effects absent. Request cancellation, subscription cancellation,
//! and [`ProductClient::cancel_runtime_attempt`] have different scopes.
//!
//! These APIs block even though the local adapter uses an internal async runtime.
//! Use bounded blocking workers when integrating with async Rust. The SDK neither
//! starts a daemon nor stops managed sessions when the client is closed.
//!
//! Linux, macOS, and Windows have local adapters; there is no network fallback.
//! The crate README links task guides, maintained examples, and implementation
//! explanations. SDK-owned boundary values keep private wire/router types out of
//! consumer signatures.

mod boundary;
mod client;
mod error;
mod product;
mod profile;
mod reconnect;
mod registry;
mod resilient;
mod telemetry;
mod trace;
mod transport;

pub use boundary::*;
pub use client::*;
pub use error::*;
pub use product::*;
pub use profile::*;
pub use reconnect::*;
pub use registry::*;
pub use resilient::*;
pub use telemetry::*;
pub use trace::*;
pub use transport::*;

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
compile_error!(
    "Undercurrent local SDK adapter supports Linux, macOS and Windows; no network fallback is provided"
);
#[cfg(any(target_os = "linux", target_os = "macos", windows))]
mod local;
#[cfg(any(target_os = "linux", target_os = "macos", windows))]
pub use local::*;
