// SPDX-License-Identifier: Apache-2.0

//! Bounded multiplex state for first-party Undercurrent transport adapters.
//!
//! [`ConnectionRouter`] correlates finite responses and retains independently
//! bounded inbound stream mailboxes. [`OutboundScheduler`] admits encoded frames,
//! applies weighted class/route selection, and spends byte credit at dequeue.
//! [`ReplayJournal`] retains exact stream frames within one server epoch.
//!
//! These components perform no transport I/O and start no worker threads. The
//! adapter owns negotiation, encoding, a single ordered writer, write confirmation,
//! credit return, and explicit teardown. Queue admission does not prove delivery;
//! replay recording must follow a successful complete write and is not durable.
//!
//! This crate is an internal implementation surface, not the supported external
//! Rust SDK. The crate README and its linked contributor guides explain ownership,
//! ordering limitations, recovery, and the relevant contract tests.

mod error;
mod frame;
mod registry;
mod replay;
mod router;
mod scheduler;

pub use error::*;
pub use frame::*;
pub use registry::*;
pub use replay::*;
pub use router::*;
pub use scheduler::*;
pub use undercurrent_wire::{
    ClientMessage, InboundStreamItem, ServerMessage, StreamGap, StreamGapReason,
    StreamRecoveryAction,
};
