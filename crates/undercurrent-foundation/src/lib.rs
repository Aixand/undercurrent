// SPDX-License-Identifier: Apache-2.0

//! Shared primitives without dependencies on Undercurrent product layers.
//!
//! This crate supplies bounded Serde strings, canonical JSON bytes, SHA-256
//! digests, opaque references, and conservative sensitive-material detection.
//! All operations are synchronous and in-memory; callers own input acquisition,
//! product policy, persistence, transport recovery, and secret resolution.
//!
//! Public exports serve first-party composition. The architecture policy names
//! `undercurrent-protocol` and `undercurrent-sdk` as the supported external Rust
//! surfaces; Rust visibility here does not establish a separate consumer SDK.
//!
//! Canonicalization accepts an already decoded value, [`Digest`] hashes exact
//! supplied bytes, and [`SensitiveMaterialDetector`] reports findings without
//! redacting its input. Use each mechanism at its owning boundary rather than
//! treating successful validation as proof of authorization or completed work.

#![deny(missing_docs)]

mod bounded;
mod canonical;
mod digest;
mod privacy;
mod uri;
mod version;

pub use bounded::BoundedString;
pub use canonical::{CanonicalJsonError, CanonicalJsonLimits, canonicalize_json_value_with_limits};
pub use digest::{ContentDigest, Digest, DigestParseError, DigestVerificationError};
pub use privacy::{
    Classification, SensitiveMaterialDetector, SensitiveMaterialError,
    credential_like_environment_text, credential_like_text, is_sensitive_field_name,
};
pub use uri::{OpaqueUri, OpaqueUriError};
pub use version::{RELEASE_VERSION, ReleaseVersion};
