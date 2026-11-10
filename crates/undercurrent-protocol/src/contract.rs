// SPDX-License-Identifier: Apache-2.0

//! Versioned product contract, attempt, secret-reference, and output metadata DTOs.

use std::fmt::{Debug, Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::ProductOperation;

// Stable product metadata; compatibility fixtures pin these serialized values.
const REQUEST_SCHEMA: &str = "undercurrent.request";
const RESPONSE_SCHEMA: &str = "undercurrent.response";
const SCHEMA_VERSION: &str = crate::RELEASE_VERSION;
const CONTRACT_VERSION: &str = crate::RELEASE_VERSION;

/// Schema direction selected for a finite operation payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractPayloadKind {
    /// Request input schema.
    Request,
    /// Response result schema.
    Response,
}

/// Explicit capability and schema versions for additive product payloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProductContractDto {
    /// Stable product capability.
    pub capability: String,
    /// Stable schema identifier.
    pub schema: String,
    /// Schema generation.
    pub schema_version: String,
    /// Product contract version.
    pub contract_version: String,
}

impl ProductContractDto {
    /// Returns the current canonical descriptor for an operation payload.
    pub fn current(operation: ProductOperation, payload: ContractPayloadKind) -> Self {
        Self {
            capability: operation.as_str().to_owned(),
            schema: match payload {
                ContractPayloadKind::Request => REQUEST_SCHEMA,
                ContractPayloadKind::Response => RESPONSE_SCHEMA,
            }
            .to_owned(),
            schema_version: SCHEMA_VERSION.to_owned(),
            contract_version: CONTRACT_VERSION.to_owned(),
        }
    }
}

/// Attempt identity, fence, and exact semantic payload digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationAttemptDto {
    /// Stable product attempt identity.
    pub attempt_id: String,
    /// Non-zero monotonic target fence.
    pub fencing_token: u64,
    /// Canonical lowercase SHA-256 payload digest.
    pub payload_digest_sha256: String,
}

/// Owner evidence about complete operation outcomes, errors or reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectCertaintyDto {
    /// Effect is known absent.
    KnownAbsent,
    /// Effect is known applied.
    KnownApplied,
    /// Effect remains unknown.
    Unknown,
}

/// Optional outward attempt result metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationAttemptOutcomeDto {
    /// Stable attempt identity.
    pub attempt_id: String,
    /// Applied target fence.
    pub fencing_token: u64,
    /// Durable effect certainty.
    pub effect_certainty: EffectCertaintyDto,
    /// Exact known result digest.
    pub result_digest_sha256: Option<String>,
    /// Whether a caller must reconcile rather than retry.
    pub reconciliation_required: bool,
}

/// Explicit process truth used by recovery DTO mappings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "truth", rename_all = "snake_case")]
pub enum ProcessTruthDto {
    /// Exact process is running.
    Running { observed_at_millis: i64 },
    /// Exact process exited.
    Exited {
        exit_code: i32,
        observed_at_millis: i64,
    },
    /// Confirmed process absence observation.
    Missing { observed_at_millis: i64 },
    /// Owned process was lost.
    Lost { observed_at_millis: i64 },
    /// Process exists without attributable ownership.
    Orphaned { observed_at_millis: i64 },
    /// Truth is inconclusive.
    Unknown { observed_at_millis: i64 },
}

/// Opaque secret handle safe to persist instead of raw material.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SecretReferenceDto(String);

impl SecretReferenceDto {
    /// Validates a bounded reference.
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidSecretReferenceDto> {
        let value = value.into();
        if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
            return Err(InvalidSecretReferenceDto);
        }
        Ok(Self(value))
    }

    /// Returns the opaque reference.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Debug for SecretReferenceDto {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecretReferenceDto")
            .field("byte_len", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl TryFrom<String> for SecretReferenceDto {
    type Error = InvalidSecretReferenceDto;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<SecretReferenceDto> for String {
    fn from(value: SecretReferenceDto) -> Self {
        value.0
    }
}

/// Invalid protocol secret reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidSecretReferenceDto;

impl Display for InvalidSecretReferenceDto {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(
            "secret reference must be non-empty, bounded, and contain no control characters",
        )
    }
}

impl std::error::Error for InvalidSecretReferenceDto {}

/// Explicit truncation evidence for one output page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputTruncationDto {
    /// Original available source bytes.
    pub source_bytes: u64,
    /// Bytes actually delivered.
    pub delivered_bytes: u64,
}

/// Bounded inline, immutable-reference, or logical-stream command output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "storage", rename_all = "snake_case")]
pub enum ExecutionOutputDto {
    /// Bounded bytes carried directly by the finite response.
    Inline {
        /// Private output bytes.
        bytes: Vec<u8>,
        /// SHA-256 of exactly `bytes`.
        payload_digest_sha256: String,
        /// Explicit truncation evidence when the inline limit was reached.
        truncation: Option<OutputTruncationDto>,
    },
    /// Immutable content owned by the daemon's configured content adapter.
    ContentReference {
        /// Opaque immutable content URI.
        uri: String,
        /// SHA-256 of complete referenced bytes.
        payload_digest_sha256: String,
        /// Complete referenced size.
        size_bytes: u64,
    },
    /// Bounded resumable logical stream used while output is still available.
    Stream {
        /// Product logical stream identity.
        stream_id: String,
        /// Earliest currently available byte offset.
        available_from: u64,
        /// Current exclusive output offset.
        next_offset: u64,
    },
}

/// Result of one daemon-supervised command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecuteInSessionResultDto {
    /// Stable target session identity.
    pub session_id: String,
    /// Durable attempt result and effect certainty.
    pub attempt: OperationAttemptOutcomeDto,
    /// Portable exit code when the command reached normal process termination.
    pub exit_code: Option<i32>,
    /// Standard output representation.
    pub stdout: ExecutionOutputDto,
    /// Standard error representation.
    pub stderr: ExecutionOutputDto,
}
