// SPDX-License-Identifier: Apache-2.0

//! Product-specific canonical request payload rules.

use crate::ProductRequest;
use std::fmt::{Display, Formatter};
use undercurrent_foundation::{CanonicalJsonLimits, Digest, canonicalize_json_value_with_limits};

/// A product payload could not be represented under its canonical JSON limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CanonicalPayloadError {
    /// Serialization or bounded canonicalization failed.
    InvalidCanonicalPayload,
}

impl Display for CanonicalPayloadError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("product request could not be canonicalized")
    }
}

impl std::error::Error for CanonicalPayloadError {}

/// Computes the canonical semantic payload digest, excluding retry metadata.
///
/// Serializes the typed request and removes the entire `input.mutation` member,
/// including its contract descriptor and compatible unknown fields. The operation
/// tag and all other serialized input remain covered. Runtime cancellation uses
/// an Undercurrent fingerprint domain and decimal-string pins/deadline. This is
/// not a digest of the original incoming JSON bytes and does not authorize an
/// ambiguous retry.
pub fn canonical_product_request_digest(
    request: &ProductRequest,
) -> Result<Digest, CanonicalPayloadError> {
    let mut value = serde_json::to_value(request)
        .map_err(|_| CanonicalPayloadError::InvalidCanonicalPayload)?;
    if let Some(input) = value
        .as_object_mut()
        .and_then(|envelope| envelope.get_mut("input"))
        .and_then(serde_json::Value::as_object_mut)
    {
        input.remove("mutation");
    }
    if let ProductRequest::CancelRuntimeAttempt(request) = request {
        value = request
            .fingerprint_material(value)
            .map_err(|_| CanonicalPayloadError::InvalidCanonicalPayload)?;
    }
    let canonical = canonicalize_json_value_with_limits(&value, CanonicalJsonLimits::DEFAULT)
        .map_err(|_| CanonicalPayloadError::InvalidCanonicalPayload)?;
    Ok(Digest::sha256(&canonical))
}
