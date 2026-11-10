// SPDX-License-Identifier: Apache-2.0

//! Transport-independent bounded server connection orchestration.
//!
//! Internal daemon composition surface: external consumers use the SDK and
//! product protocol crates. The local adapter owns peer verification, negotiation
//! and physical IO; this crate owns connection-local admission and settlement.
//! Product dispatch owns decoding, authorization and application effects.
//!
//! [`ServerConnectionSession`] queues responses and stream frames without IO.
//! Queue admission is distinct from a successful physical write; the adapter
//! confirms writes with [`PendingServerWrite::confirm_written`]. Replay and
//! [`BoundedRequestIdempotencyStore`] are bounded in-memory mechanisms, not
//! durable operation-outcome evidence.

mod admission;
mod engine;
pub use admission::*;
pub use engine::*;
