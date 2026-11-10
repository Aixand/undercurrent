// SPDX-License-Identifier: Apache-2.0

//! Version-neutral product protocol for Undercurrent.
//!
//! Connection negotiation selects the protocol version before this codec is
//! used. Public DTO names therefore describe product concepts rather than a
//! particular encoding generation. Domain validation happens at the server
//! adapter boundary; these types describe transport input, not aggregates.
//!
//! Start with [`ProductRequest`], [`ProductResponse`], and [`ProductStreamSpec`].
//! [`ProductCodec`] enforces JSON byte limits and Serde shape, while
//! [`validate_product_request_contract`] checks the current product descriptor.
//! Dedicated recovery DTOs also expose `validate` methods for decoded values;
//! successful deserialization alone does not establish their invariants.
//!
//! Terminal streams use [`TerminalOutputCodec`] rather than JSON stream items.
//! This crate performs no IO, owns no connection or retained output, and does
//! not retry mutations. The public SDK owns client transport and correlation;
//! the daemon owns authorization and translation into application commands.

mod agent;
mod auth;
mod canonical;
mod codec;
mod compatibility;
mod contract;
mod dto;
mod error;
mod execution_output_digest;
mod observability;
mod registry;
mod repair_attempt;
mod runtime_attempt_status;
mod runtime_cancellation;
mod terminal;

pub use agent::*;
pub use auth::*;
pub use canonical::*;
pub use codec::*;
pub use compatibility::*;
pub use contract::*;
pub use dto::*;
pub use error::*;
pub use execution_output_digest::{
    execution_output_payload_digest_sha256, verify_execution_output_payload_digest,
};
pub use observability::*;
pub use registry::*;
pub use repair_attempt::*;
pub use runtime_attempt_status::*;
pub use runtime_cancellation::*;
pub use terminal::*;

// Shared public scalar used by runtime output contracts.
pub use undercurrent_foundation::Classification;

/// Shared exact release identity for product schemas.
pub use undercurrent_foundation::{RELEASE_VERSION, ReleaseVersion};
