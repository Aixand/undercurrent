// SPDX-License-Identifier: Apache-2.0

//! Application use cases and ports for Undercurrent.
//!
//! This crate orchestrates the pure domain and exposes inward-facing ports. It
//! does not depend on process, PTY, persistence, network, CLI, or telemetry
//! adapters.
//!
//! Public items support internal workspace composition; the supported external
//! Rust client surface lives in the SDK and protocol crates. Command services
//! coordinate durable session state separately from process effects and
//! rebuildable projections. A successful process call is not a database commit,
//! and an infrastructure error is not proof that an effect was absent.
//!
//! Begin with [`commands`] for lifecycle orchestration, [`attempts`] and
//! [`durable_process`] for effect settlement, and [`ports`] for adapter
//! obligations. [`read_model`], [`subscriptions`], and [`terminal_output`] own
//! bounded prepared/read mechanisms. The crate's Markdown guides explain
//! ownership, ordering, recovery, and contributor verification together.

pub mod agent_channel;
pub mod attempts;
pub mod commands;
pub mod detail;
pub mod diagnostic_bundle;
pub mod durable_process;
pub mod error;
pub mod event_feed;
pub mod execute;
pub mod execution_context;
pub mod observability;
pub mod observation;
pub mod outbox;
pub mod outbox_dispatch;
pub mod ports;
pub mod process_observation;
pub mod profiles;
pub mod queries;
pub mod read_model;
pub mod repair_attempt;
pub mod runtime_attempt_status;
pub mod runtime_cancellation;
pub mod scheduling;
pub mod secrets;
pub mod shutdown_process_host;
pub mod subscriptions;
pub mod terminal_output;
pub mod terminal_screen;
pub mod watch;

pub use agent_channel::*;
pub use attempts::*;
pub use commands::*;
pub use detail::*;
pub use diagnostic_bundle::*;
pub use durable_process::*;
pub use error::*;
pub use event_feed::*;
pub use execute::*;
pub use execution_context::*;
pub use observability::*;
pub use observation::*;
pub use outbox::*;
pub use outbox_dispatch::*;
pub use ports::*;
pub use process_observation::*;
pub use profiles::*;
pub use queries::*;
pub use read_model::*;
pub use repair_attempt::*;
pub use runtime_attempt_status::*;
pub use runtime_cancellation::*;
pub use scheduling::*;
pub use secrets::*;
pub use shutdown_process_host::*;
pub use subscriptions::*;
pub use terminal_output::*;
pub use terminal_screen::*;
pub use watch::*;
