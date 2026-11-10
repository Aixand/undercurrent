// SPDX-License-Identifier: Apache-2.0

//! Runtime adapters for the Undercurrent application ports.
//!
//! This crate owns bounded in-process coordination and concrete infrastructure
//! integrations while depending inward on application and domain contracts.
//!
//! It is a daemon-private implementation crate. Its public Rust items connect
//! first-party composition roots and tests; they are not a supported external
//! SDK surface. Public consumers use `undercurrent-sdk` and
//! `undercurrent-protocol`.
//!
//! [`NativeProcessRuntime`] owns local children, PTYs, and exit drainage.
//! The private session-host adapters move that ownership across a process
//! boundary and retain operation-specific receipts. A process observation,
//! accepted request, terminal write, and durable application result are distinct
//! boundaries. In-memory repositories, queues, replay journals, and telemetry
//! do not survive a daemon restart.
//!
//! [`VtTerminalScreenStore`] keeps parser state aligned with retained output;
//! foreground and health adapters derive observations from bounded samples.
//! Futures exposed by application ports are not uniformly nonblocking: mutation
//! lease acquisition and some PTY operations can block the polling thread.

mod clock;
mod cooperative;
mod coordinator;
mod deliveries;
mod directory;
mod error;
mod events;
mod foreground;
mod health_checks;
mod observations;
mod patterns;
mod platform;
mod process;
#[cfg(any(unix, windows))]
mod process_host_protocol;
mod profiles;
mod repository;
mod restarts;
mod telemetry;
mod terminal_input;
mod terminal_screen;

pub use clock::*;
pub use cooperative::*;
pub use coordinator::*;
pub use deliveries::*;
pub use directory::*;
pub use error::*;
pub use events::*;
pub use foreground::*;
pub use health_checks::*;
pub use observations::*;
pub use patterns::*;
pub use platform::*;
pub use process::*;
pub use profiles::*;
pub use repository::*;
pub use restarts::*;
pub use telemetry::*;
pub use terminal_input::*;
pub use terminal_screen::*;
