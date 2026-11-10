// SPDX-License-Identifier: Apache-2.0

//! Technical agent-channel identities retained with one external runtime.
//!
//! These values fence delivery; they do not interpret workflow progress or
//! decide whether a submitted result satisfies a caller-owned schema.

use crate::{
    DomainError, DomainResult, FencingToken, OperationAttemptId, PayloadDigest, TimestampMillis,
};

/// Maximum canonical caller-owned binding retained for one agent runtime.
pub const MAX_AGENT_BINDING_BYTES: usize = 128 * 1024;
/// Maximum canonical expected-result schema retained for local presentation.
pub const MAX_AGENT_RESULT_SCHEMA_BYTES: usize = 64 * 1024;
const MAX_AGENT_CONTEXT_BYTES: usize = 512;

/// Exact caller-owned identity and schema material for one agent runtime.
#[derive(Clone, PartialEq, Eq)]
pub struct AgentRuntimeBinding {
    tenant_id: String,
    source_product: String,
    actor_context_ref: String,
    authority_ref: String,
    undercurrent_provider_id: String,
    undercurrent_provider_generation: u64,
    attempt_id: OperationAttemptId,
    attempt_revision: u64,
    recovery_epoch: u64,
    fence: FencingToken,
    request_digest: PayloadDigest,
    expected_result_schema_canonical_json: Vec<u8>,
    canonical_json: Vec<u8>,
    expires_at: TimestampMillis,
}

impl AgentRuntimeBinding {
    /// Constructs a bounded technical binding from boundary-canonicalized data.
    ///
    /// This validates string and byte bounds plus nonzero generations. It does
    /// not parse JSON, verify canonicalization, authorize references, evaluate
    /// the result schema, or compare expiry with a clock. Those checks belong
    /// to the boundary supplying this value.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: String,
        source_product: String,
        actor_context_ref: String,
        authority_ref: String,
        undercurrent_provider_id: String,
        undercurrent_provider_generation: u64,
        attempt_id: OperationAttemptId,
        attempt_revision: u64,
        recovery_epoch: u64,
        fence: FencingToken,
        request_digest: PayloadDigest,
        expected_result_schema_canonical_json: Vec<u8>,
        canonical_json: Vec<u8>,
        expires_at: TimestampMillis,
    ) -> DomainResult<Self> {
        for (field, value) in [
            ("agent_tenant_id", &tenant_id),
            ("agent_source_product", &source_product),
            ("agent_actor_context_ref", &actor_context_ref),
            ("agent_authority_ref", &authority_ref),
            ("agent_undercurrent_provider_id", &undercurrent_provider_id),
        ] {
            let reason = if value.is_empty() {
                Some(crate::InvalidValueReason::Empty)
            } else if value.len() > MAX_AGENT_CONTEXT_BYTES {
                Some(crate::InvalidValueReason::TooLong {
                    max: MAX_AGENT_CONTEXT_BYTES,
                })
            } else {
                value
                    .char_indices()
                    .find(|(_, character)| character.is_control())
                    .map(
                        |(index, character)| crate::InvalidValueReason::InvalidCharacter {
                            index,
                            character,
                        },
                    )
            };
            if let Some(reason) = reason {
                return Err(DomainError::InvalidValue { field, reason });
            }
        }
        if attempt_revision == 0 || recovery_epoch == 0 || undercurrent_provider_generation == 0 {
            return Err(DomainError::InvalidValue {
                field: "agent_attempt_revision",
                reason: crate::InvalidValueReason::OutOfRange {
                    min: 1,
                    max: u64::MAX,
                },
            });
        }
        if expected_result_schema_canonical_json.is_empty()
            || expected_result_schema_canonical_json.len() > MAX_AGENT_RESULT_SCHEMA_BYTES
        {
            return Err(DomainError::InvalidValue {
                field: "agent_expected_result_schema",
                reason: crate::InvalidValueReason::OutOfRange {
                    min: 1,
                    max: MAX_AGENT_RESULT_SCHEMA_BYTES as u64,
                },
            });
        }
        if canonical_json.is_empty() || canonical_json.len() > MAX_AGENT_BINDING_BYTES {
            return Err(DomainError::InvalidValue {
                field: "agent_runtime_binding",
                reason: crate::InvalidValueReason::OutOfRange {
                    min: 1,
                    max: MAX_AGENT_BINDING_BYTES as u64,
                },
            });
        }
        Ok(Self {
            tenant_id,
            source_product,
            actor_context_ref,
            authority_ref,
            undercurrent_provider_id,
            undercurrent_provider_generation,
            attempt_id,
            attempt_revision,
            recovery_epoch,
            fence,
            request_digest,
            expected_result_schema_canonical_json,
            canonical_json,
            expires_at,
        })
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    pub fn source_product(&self) -> &str {
        &self.source_product
    }

    pub fn actor_context_ref(&self) -> &str {
        &self.actor_context_ref
    }

    pub fn authority_ref(&self) -> &str {
        &self.authority_ref
    }

    pub fn undercurrent_provider_id(&self) -> &str {
        &self.undercurrent_provider_id
    }

    pub const fn undercurrent_provider_generation(&self) -> u64 {
        self.undercurrent_provider_generation
    }

    pub const fn attempt_id(&self) -> &OperationAttemptId {
        &self.attempt_id
    }

    pub const fn attempt_revision(&self) -> u64 {
        self.attempt_revision
    }

    pub const fn recovery_epoch(&self) -> u64 {
        self.recovery_epoch
    }

    pub const fn fence(&self) -> FencingToken {
        self.fence
    }

    pub const fn request_digest(&self) -> PayloadDigest {
        self.request_digest
    }

    pub fn expected_result_schema_canonical_json(&self) -> &[u8] {
        &self.expected_result_schema_canonical_json
    }

    pub fn canonical_json(&self) -> &[u8] {
        &self.canonical_json
    }

    pub const fn expires_at(&self) -> TimestampMillis {
        self.expires_at
    }
}

impl std::fmt::Debug for AgentRuntimeBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentRuntimeBinding")
            .field("source_product", &self.source_product)
            .field("attempt_id", &self.attempt_id)
            .field("attempt_revision", &self.attempt_revision)
            .field("recovery_epoch", &self.recovery_epoch)
            .field("fence", &self.fence)
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}
