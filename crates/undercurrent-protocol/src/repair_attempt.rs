// SPDX-License-Identifier: Apache-2.0

//! Digest-bound request and evidence-derived result DTOs for Unknown repair.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::{ContractPayloadKind, ProductContractDto, ProductOperation, validate_product_contract};

const MAX_ATTEMPT_ID_BYTES: usize = 128;
const SHA256_HEX_CHARS: usize = 64;
const MAX_UNRESOLVED_ATTEMPT_IDS: usize = 256;

/// Requests observation and repair of one durable Unknown attempt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RepairOperationAttemptRequest {
    attempt_id: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    request_digest_sha256: Option<String>,
    contract: ProductContractDto,
}

impl RepairOperationAttemptRequest {
    /// Constructs a bounded standalone/operator request containing no dispatch binding.
    pub fn new(attempt_id: String) -> Result<Self, RepairOperationAttemptDtoError> {
        validate_attempt_id(&attempt_id)?;
        Ok(Self {
            attempt_id,
            request_digest_sha256: None,
            contract: ProductContractDto::current(
                ProductOperation::RepairOperationAttempt,
                ContractPayloadKind::Request,
            ),
        })
    }

    /// Constructs an attached repair request bound to the caller's canonical dispatch digest.
    pub fn attached(
        attempt_id: String,
        request_digest_sha256: String,
    ) -> Result<Self, RepairOperationAttemptDtoError> {
        validate_attempt_id(&attempt_id)?;
        validate_request_digest(&request_digest_sha256)?;
        Ok(Self {
            attempt_id,
            request_digest_sha256: Some(request_digest_sha256),
            contract: ProductContractDto::current(
                ProductOperation::RepairOperationAttempt,
                ContractPayloadKind::Request,
            ),
        })
    }

    /// Returns the product attempt identifier.
    pub fn attempt_id(&self) -> &str {
        &self.attempt_id
    }

    /// Returns the caller request digest for attached repair, or `None` for standalone repair.
    pub fn request_digest_sha256(&self) -> Option<&str> {
        self.request_digest_sha256.as_deref()
    }

    /// Returns the exact public product contract required for this repair request.
    pub const fn contract(&self) -> &ProductContractDto {
        &self.contract
    }

    /// Requires the sole current repair-request contract without reading attempt state.
    pub fn validate_contract(&self) -> Result<(), crate::ProductContractError> {
        validate_product_contract(
            &self.contract,
            ProductOperation::RepairOperationAttempt,
            ContractPayloadKind::Request,
        )
    }

    /// Validates a request decoded through a generic transport codec.
    pub fn validate(&self) -> Result<(), RepairOperationAttemptDtoError> {
        self.validate_contract()
            .map_err(|_| RepairOperationAttemptDtoError::InvalidProductContract)?;
        validate_attempt_id(&self.attempt_id)?;
        if let Some(request_digest) = self.request_digest_sha256.as_deref() {
            validate_request_digest(request_digest)?;
        }
        Ok(())
    }
}

/// Evidence-derived terminal resolution; Unknown has no success representation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "certainty", rename_all = "snake_case", deny_unknown_fields)]
pub enum RepairOperationAttemptResolutionDto {
    /// Retained evidence proves no process-affecting side effect occurred.
    KnownAbsent,
    /// Retained evidence proves the effect and its stable result digest.
    KnownApplied {
        /// Canonical lowercase SHA-256 without an algorithm prefix.
        result_digest_sha256: String,
    },
}

impl RepairOperationAttemptResolutionDto {
    /// Constructs KnownApplied only with a canonical result digest.
    pub fn known_applied(
        result_digest_sha256: String,
    ) -> Result<Self, RepairOperationAttemptDtoError> {
        validate_digest(&result_digest_sha256)?;
        Ok(Self::KnownApplied {
            result_digest_sha256,
        })
    }

    /// Validates decoded terminal resolution data.
    pub fn validate(&self) -> Result<(), RepairOperationAttemptDtoError> {
        match self {
            Self::KnownAbsent => Ok(()),
            Self::KnownApplied {
                result_digest_sha256,
            } => validate_digest(result_digest_sha256),
        }
    }

    /// Returns the stable result digest only for KnownApplied.
    pub fn result_digest_sha256(&self) -> Option<&str> {
        match self {
            Self::KnownAbsent => None,
            Self::KnownApplied {
                result_digest_sha256,
            } => Some(result_digest_sha256),
        }
    }
}

/// Successful repair plus the post-commit unresolved readiness projection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RepairOperationAttemptResultDto {
    attempt_id: String,
    resolution: RepairOperationAttemptResolutionDto,
    readiness_complete: bool,
    unresolved_count: u64,
    unresolved_attempt_ids: Vec<String>,
    unresolved_truncated: bool,
}

impl RepairOperationAttemptResultDto {
    /// Constructs a bounded internally consistent repair result.
    pub fn new(
        attempt_id: String,
        resolution: RepairOperationAttemptResolutionDto,
        unresolved_count: u64,
        unresolved_attempt_ids: Vec<String>,
    ) -> Result<Self, RepairOperationAttemptDtoError> {
        validate_attempt_id(&attempt_id)?;
        resolution.validate()?;
        if unresolved_attempt_ids.len() > MAX_UNRESOLVED_ATTEMPT_IDS {
            return Err(RepairOperationAttemptDtoError::TooManyUnresolvedAttemptIds);
        }
        for unresolved_attempt_id in &unresolved_attempt_ids {
            validate_attempt_id(unresolved_attempt_id)?;
        }
        if unresolved_count < unresolved_attempt_ids.len() as u64 {
            return Err(RepairOperationAttemptDtoError::InvalidUnresolvedCount);
        }
        if unresolved_attempt_ids
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(RepairOperationAttemptDtoError::UnorderedUnresolvedAttemptIds);
        }
        let readiness_complete = unresolved_count == 0;
        let unresolved_truncated = unresolved_count > unresolved_attempt_ids.len() as u64;
        Ok(Self {
            attempt_id,
            resolution,
            readiness_complete,
            unresolved_count,
            unresolved_attempt_ids,
            unresolved_truncated,
        })
    }

    /// Returns the repaired attempt identifier.
    pub fn attempt_id(&self) -> &str {
        &self.attempt_id
    }

    /// Returns the evidence-derived terminal resolution.
    pub const fn resolution(&self) -> &RepairOperationAttemptResolutionDto {
        &self.resolution
    }

    /// Returns true only when the exact unresolved count is zero.
    pub const fn readiness_complete(&self) -> bool {
        self.readiness_complete
    }

    /// Returns the exact unresolved count.
    pub const fn unresolved_count(&self) -> u64 {
        self.unresolved_count
    }

    /// Returns the bounded ascending unresolved identifier page.
    pub fn unresolved_attempt_ids(&self) -> &[String] {
        &self.unresolved_attempt_ids
    }

    /// Reports whether unresolved identifiers extend beyond this page.
    pub const fn unresolved_truncated(&self) -> bool {
        self.unresolved_truncated
    }

    /// Validates a result decoded through a generic transport codec.
    pub fn validate(&self) -> Result<(), RepairOperationAttemptDtoError> {
        // Deserialization bypasses new. Recompute both flags from the exact
        // count: even an empty identifier page can be truncated and not ready.
        // See [readiness rules](../../../docs/reference/protocol/recovery.md#check-the-repair-result).
        let canonical = Self::new(
            self.attempt_id.clone(),
            self.resolution.clone(),
            self.unresolved_count,
            self.unresolved_attempt_ids.clone(),
        )?;
        if canonical.readiness_complete != self.readiness_complete
            || canonical.unresolved_truncated != self.unresolved_truncated
        {
            return Err(RepairOperationAttemptDtoError::InvalidReadinessProjection);
        }
        Ok(())
    }
}

/// Stable DTO validation failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepairOperationAttemptDtoError {
    /// Repair requires the exact current capability, schema, and contract versions.
    InvalidProductContract,
    /// Attempt identifiers must be bounded safe references.
    InvalidAttemptId,
    /// Attached repair requires canonical lowercase SHA-256 request identity.
    InvalidRequestDigest,
    /// KnownApplied requires canonical lowercase SHA-256 hex.
    InvalidResultDigest,
    /// The exact unresolved count is smaller than its identifier page.
    InvalidUnresolvedCount,
    /// The unresolved identifier page exceeds the protocol bound.
    TooManyUnresolvedAttemptIds,
    /// The unresolved page must be strictly ascending and duplicate-free.
    UnorderedUnresolvedAttemptIds,
    /// Encoded readiness booleans disagree with the exact count and page.
    InvalidReadinessProjection,
}

impl Display for RepairOperationAttemptDtoError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidProductContract => "invalid repair-operation-attempt product contract",
            Self::InvalidAttemptId => "invalid operation attempt identifier",
            Self::InvalidRequestDigest => "invalid canonical request digest",
            Self::InvalidResultDigest => "invalid canonical result digest",
            Self::InvalidUnresolvedCount => "invalid unresolved attempt count",
            Self::TooManyUnresolvedAttemptIds => "too many unresolved attempt identifiers",
            Self::UnorderedUnresolvedAttemptIds => {
                "unresolved attempt identifiers are not strictly ordered"
            }
            Self::InvalidReadinessProjection => "invalid unresolved readiness projection",
        })
    }
}

impl Error for RepairOperationAttemptDtoError {}

fn validate_attempt_id(value: &str) -> Result<(), RepairOperationAttemptDtoError> {
    if value.is_empty()
        || value.len() > MAX_ATTEMPT_ID_BYTES
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':' | b'/')
        })
    {
        return Err(RepairOperationAttemptDtoError::InvalidAttemptId);
    }
    Ok(())
}

fn validate_digest(value: &str) -> Result<(), RepairOperationAttemptDtoError> {
    if value.len() != SHA256_HEX_CHARS
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(RepairOperationAttemptDtoError::InvalidResultDigest);
    }
    Ok(())
}

fn validate_request_digest(value: &str) -> Result<(), RepairOperationAttemptDtoError> {
    if value.len() != SHA256_HEX_CHARS
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(RepairOperationAttemptDtoError::InvalidRequestDigest);
    }
    Ok(())
}

// Without serde(default), this hook requires the digest member to be present.
// Only explicit null selects the standalone shape; omission must not silently
// discard an [attached repair binding](../../../docs/reference/protocol/recovery.md#repair-an-unknown-attempt).
fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;

    #[test]
    fn repair_contract_is_digest_bound_and_file_backed() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/repair_operation_attempt.json"
        ))
        .expect("repair fixture JSON");
        let request: RepairOperationAttemptRequest =
            serde_json::from_value(fixture["request"].clone()).expect("repair request");
        request.validate().expect("bounded request");
        assert_eq!(request.attempt_id(), "attempt-0001");
        assert_eq!(
            request.request_digest_sha256(),
            Some("7777777777777777777777777777777777777777777777777777777777777777")
        );
        assert_eq!(
            request.contract(),
            &ProductContractDto::current(
                ProductOperation::RepairOperationAttempt,
                ContractPayloadKind::Request,
            )
        );
        let request_json = serde_json::to_value(request).expect("request JSON");
        assert_eq!(request_json.as_object().expect("request object").len(), 3);

        let result: RepairOperationAttemptResultDto =
            serde_json::from_value(fixture["result"].clone()).expect("repair result");
        result.validate().expect("consistent result");
        assert!(!result.readiness_complete());
        assert_eq!(result.unresolved_count(), 2);
        assert!(result.unresolved_truncated());
    }

    #[test]
    fn repair_request_requires_explicit_nullable_digest_and_rejects_noncanonical_values() {
        let standalone = RepairOperationAttemptRequest::new("attempt-standalone".to_owned())
            .expect("standalone request");
        assert_eq!(
            serde_json::to_value(standalone).expect("standalone JSON")["request_digest_sha256"],
            Value::Null
        );

        assert!(
            serde_json::from_value::<RepairOperationAttemptRequest>(
                serde_json::json!({"attempt_id": "attempt-missing"})
            )
            .is_err(),
            "nullable dispatch identity must still be required-present"
        );
        assert!(
            RepairOperationAttemptRequest::attached(
                "attempt-uppercase".to_owned(),
                "A".repeat(64),
            )
            .is_err()
        );
    }
}
