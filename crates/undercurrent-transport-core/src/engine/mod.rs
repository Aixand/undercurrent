// SPDX-License-Identifier: Apache-2.0

//! Daemon-side multiplex connection orchestration independent of socket IO.
//!
//! Sessions own request and stream state. Registration leases own active client
//! identity slots independently of session lifetime. Shared replay and mutation
//! result stores may outlive an individual connection but remain in memory.

mod connections;
mod dispatch;
mod error;
mod idempotency;
mod registry;
mod session;

pub use connections::*;
pub use dispatch::*;
pub use error::*;
pub use idempotency::*;
pub use registry::*;
pub use session::*;
