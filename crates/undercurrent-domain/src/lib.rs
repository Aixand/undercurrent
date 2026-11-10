// SPDX-License-Identifier: Apache-2.0

//! Pure domain model for Undercurrent.
//!
//! This crate owns session invariants and deterministic decisions. It performs
//! no filesystem, process, PTY, network, persistence, time, or telemetry IO.
//! Outer layers supply identifiers and timestamps explicitly.
//!
//! # Internal boundaries
//!
//! Rust visibility supports workspace callers; this crate is not a supported
//! external client API. Use the protocol and SDK crates for consumer contracts.
//! [`Session`] owns aggregate invariants, while [`OperationAttempt`] records
//! what is known about an external effect. Neither a [`DomainChange`] nor an
//! accepted attempt performs a durable commit: application services and their
//! repository/runtime adapters own that ordering.
//!
//! [`ForegroundObservation`], [`HealthObservation`], and [`WorkContext`] describe
//! independent evidence and declarations. They cannot establish caller-owned
//! task completion. [`SessionSummary`] also carries rebuildable effective-profile
//! state whose epoch is separate from a durable [`Revision`].
//!
//! Persistence reconstruction must use [`Session::rehydrate`] and the nested
//! reconstruction boundaries. Constructor bounds are explicit; adapter-supplied
//! process identities, canonical bytes, and evidence still require validation
//! at their owning boundaries. See the crate README and its contributor guides
//! for flows, code navigation, and verification commands.

pub use undercurrent_foundation::Classification;

pub mod agent;
pub mod attempt;
pub mod attributes;
pub mod cooperative;
pub mod effective_profile;
pub mod error;
pub mod event;
pub mod execution;
pub mod foreground;
pub mod foreground_decision;
pub mod health;
pub mod identity;
pub mod lifecycle;
pub mod observation;
pub mod output;
pub mod profile;
pub mod query;
pub mod restart;
pub mod revision;
pub mod secret;
pub mod session;
pub mod terminal;
pub mod time;
pub mod work_context;

pub use agent::*;
pub use attempt::*;
pub use attributes::*;
pub use cooperative::*;
pub use effective_profile::*;
pub use error::*;
pub use event::*;
pub use execution::*;
pub use foreground::*;
pub use foreground_decision::*;
pub use health::*;
pub use identity::*;
pub use lifecycle::*;
pub use observation::*;
pub use output::*;
pub use profile::*;
pub use query::*;
pub use restart::*;
pub use revision::*;
pub use secret::*;
pub use session::*;
pub use terminal::*;
pub use time::*;
pub use work_context::*;

/// Exact release marker for domain-owned schemas.
pub use undercurrent_foundation::ReleaseVersion;
