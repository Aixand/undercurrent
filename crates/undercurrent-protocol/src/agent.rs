// SPDX-License-Identifier: Apache-2.0

//! Bounded technical transport for one protected Operation agent attempt.
//!
//! Undercurrent retains and routes these values without deciding whether an
//! agent result is semantically valid. The owning product supplies the exact
//! attempt binding and accepts or rejects each event through its explicit public contract.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use undercurrent_foundation::{CanonicalJsonLimits, canonicalize_json_value_with_limits};

use crate::{CanonicalPayloadError, MutationContext};

/// Current private local bridge framing generation.
pub const AGENT_BRIDGE_PROTOCOL_VERSION: crate::ReleaseVersion = crate::ReleaseVersion::Current;
/// Maximum encoded local bridge or public agent-event frame.
pub const MAX_AGENT_EVENT_FRAME_BYTES: usize = 256 * 1024;
/// Maximum inline semantic result accepted by the bounded bridge.
pub const MAX_AGENT_INLINE_RESULT_BYTES: usize = 192 * 1024;
/// Maximum length of visible progress or question text.
pub const MAX_AGENT_MESSAGE_BYTES: usize = 8 * 1024;
/// Maximum wait applied to a single public event poll.
pub const MAX_AGENT_EVENT_WAIT_MILLIS: u64 = 30_000;

/// Complete caller-owned binding retained with one agent runtime start.
///
/// Values are opaque to Undercurrent except for bounded structural validation
/// and exact equality during delivery/reply fencing.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentRuntimeBindingDto {
    pub tenant_id: String,
    pub source_product: String,
    pub actor_context_ref: String,
    pub authority_ref: String,
    pub undercurrent_provider_id: String,
    pub undercurrent_provider_generation: u64,
    pub dispatch_id: String,
    pub attempt_id: String,
    pub operation_id: String,
    pub operation_revision: u64,
    pub plan_revision: u64,
    pub plan_digest_sha256: String,
    #[serde(default, deserialize_with = "deserialize_required_option")]
    pub grain_id: Option<String>,
    pub cell_id: String,
    pub cell_revision: u64,
    pub attempt_revision: u64,
    pub recovery_epoch: u64,
    pub fence: u64,
    pub request_digest_sha256: String,
    pub action_lock_ref: String,
    pub action_lock_digest_sha256: String,
    pub action_definition_digest_sha256: String,
    pub implementation_digest_sha256: String,
    pub prepared_action_digest_sha256: String,
    pub instruction_digest_sha256: String,
    pub expected_result_schema: String,
    pub expected_result_schema_document: Value,
    pub expected_result_schema_digest_sha256: String,
    pub output_classification: String,
    pub input_manifest: Value,
    pub input_manifest_digest_sha256: String,
    pub maximum_steps: u32,
    pub maximum_tokens: u64,
    pub maximum_cost_micros: u64,
    pub expires_at_millis: i64,
}

/// Model-supplied semantic content from one of the four agent-facing tools, or
/// a caller-supplied explicit turn boundary that is never exposed as a tool.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "tool", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentToolCallDto {
    Begin,
    Progress {
        message: String,
    },
    Submit {
        result: Value,
    },
    NeedInput {
        question: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        answer_schema: Option<Value>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        choices: Vec<Value>,
    },
    TurnBoundary {
        turn_ordinal: u32,
    },
}

/// Exact daemon-injected identity for one queued agent tool call.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentEventDto {
    pub event_id: String,
    pub sequence: u64,
    pub session_id: String,
    pub process_instance_id: String,
    pub attempt_id: String,
    pub attempt_revision: u64,
    pub recovery_epoch: u64,
    pub fence: u64,
    pub request_digest_sha256: String,
    pub call_digest_sha256: String,
    pub call: AgentToolCallDto,
    pub received_at_millis: i64,
}

/// Exact bounded query for the next unsettled event of one agent attempt.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetAgentEventRequest {
    pub tenant_id: String,
    pub source_product: String,
    pub actor_context_ref: String,
    pub authority_ref: String,
    pub undercurrent_provider_id: String,
    pub undercurrent_provider_generation: u64,
    pub session_id: String,
    pub attempt_id: String,
    pub attempt_revision: u64,
    pub recovery_epoch: u64,
    pub fence: u64,
    pub request_digest_sha256: String,
    pub after_sequence: u64,
    pub maximum_wait_millis: u64,
}

/// Bounded result of one agent-event poll.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum GetAgentEventResultDto {
    None { next_sequence: u64 },
    Event { event: Box<AgentEventDto> },
}

/// Caller-owned semantic response carried opaquely back to the bridge.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentEventReplyOutcomeDto {
    Accepted {
        content: Value,
    },
    Rejected {
        code: String,
        safe_message: String,
        retryable: bool,
    },
}

/// Idempotent reply to one exact queued agent event.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplyAgentEventRequest {
    #[serde(default)]
    pub mutation: MutationContext,
    pub tenant_id: String,
    pub source_product: String,
    pub actor_context_ref: String,
    pub authority_ref: String,
    pub undercurrent_provider_id: String,
    pub undercurrent_provider_generation: u64,
    pub session_id: String,
    pub event_id: String,
    pub sequence: u64,
    pub process_instance_id: String,
    pub attempt_id: String,
    pub attempt_revision: u64,
    pub recovery_epoch: u64,
    pub fence: u64,
    pub request_digest_sha256: String,
    pub call_digest_sha256: String,
    pub outcome: AgentEventReplyOutcomeDto,
}

/// Durable acknowledgement that one exact event reply is available locally.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplyAgentEventResultDto {
    pub session_id: String,
    pub event_id: String,
    pub sequence: u64,
    pub reply_digest_sha256: String,
    pub duplicate: bool,
}

/// First request on the protected local bridge transport.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentBridgeDescribeRequestDto {
    pub protocol_version: crate::ReleaseVersion,
}

/// Agent-visible schema material resolved from the authenticated peer process.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentBridgeDescribeResultDto {
    pub protocol_version: crate::ReleaseVersion,
    pub expected_result_schema_document: Value,
}

/// One local call. Attempt routing is deliberately absent and injected by the
/// authenticated local gateway.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentBridgeCallRequestDto {
    pub protocol_version: crate::ReleaseVersion,
    pub request_id: String,
    pub call: AgentToolCallDto,
}

/// One local response with no hidden route coordinates exposed to the model.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentBridgeCallResultDto {
    pub protocol_version: crate::ReleaseVersion,
    pub request_id: String,
    pub outcome: AgentEventReplyOutcomeDto,
}

/// Length-prefixed request on the owner-only local bridge socket.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "operation",
    content = "request",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum AgentBridgeRequestDto {
    Describe(AgentBridgeDescribeRequestDto),
    Call(AgentBridgeCallRequestDto),
}

/// Payload-safe local rejection with no attempt routing coordinates.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentBridgeErrorDto {
    pub code: String,
    pub safe_message: String,
    pub retryable: bool,
}

/// Length-prefixed response on the owner-only local bridge socket.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "operation",
    content = "result",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum AgentBridgeResponseDto {
    Describe(AgentBridgeDescribeResultDto),
    Call(AgentBridgeCallResultDto),
    Error(AgentBridgeErrorDto),
}

/// Canonicalizes one complete agent runtime binding under its protocol bound.
pub fn canonical_agent_runtime_binding(
    binding: &AgentRuntimeBindingDto,
) -> Result<Vec<u8>, CanonicalPayloadError> {
    // Canonicalization is not admission: the daemon subsequently applies the
    // smaller retained-binding bound in domain AgentRuntimeBinding::new and
    // checks schema/attempt pins. See [binding ownership](../../../docs/reference/protocol/agent-events.md#retain-the-complete-binding).
    let value = serde_json::to_value(binding)
        .map_err(|_| CanonicalPayloadError::InvalidCanonicalPayload)?;
    canonicalize_json_value_with_limits(
        &value,
        CanonicalJsonLimits {
            max_document_bytes: crate::MAX_AGENT_EVENT_FRAME_BYTES,
            ..CanonicalJsonLimits::DEFAULT
        },
    )
    .map_err(|_| CanonicalPayloadError::InvalidCanonicalPayload)
}

/// Canonicalizes the dynamically presented expected-result schema.
pub fn canonical_agent_result_schema(
    binding: &AgentRuntimeBindingDto,
) -> Result<Vec<u8>, CanonicalPayloadError> {
    canonical_agent_schema_document(&binding.expected_result_schema_document)
}

/// Canonicalizes an already resolved dynamic result schema under its hard bound.
pub fn canonical_agent_schema_document(schema: &Value) -> Result<Vec<u8>, CanonicalPayloadError> {
    canonicalize_json_value_with_limits(
        schema,
        CanonicalJsonLimits {
            max_document_bytes: undercurrent_domain_result_schema_limit(),
            ..CanonicalJsonLimits::DEFAULT
        },
    )
    .map_err(|_| CanonicalPayloadError::InvalidCanonicalPayload)
}

/// Computes the exact digest of the dynamically presented result schema.
pub fn canonical_agent_result_schema_digest(
    binding: &AgentRuntimeBindingDto,
) -> Result<undercurrent_foundation::Digest, CanonicalPayloadError> {
    canonical_agent_result_schema(binding)
        .map(|canonical| undercurrent_foundation::Digest::sha256(&canonical))
}

/// Canonicalizes one model-visible tool call before durable admission.
pub fn canonical_agent_tool_call(
    call: &AgentToolCallDto,
) -> Result<Vec<u8>, CanonicalPayloadError> {
    canonical_agent_value(call)
}

/// Canonicalizes one caller-owned event outcome before durable reply.
pub fn canonical_agent_event_reply_outcome(
    outcome: &AgentEventReplyOutcomeDto,
) -> Result<Vec<u8>, CanonicalPayloadError> {
    canonical_agent_value(outcome)
}

fn canonical_agent_value<T: Serialize>(value: &T) -> Result<Vec<u8>, CanonicalPayloadError> {
    let value =
        serde_json::to_value(value).map_err(|_| CanonicalPayloadError::InvalidCanonicalPayload)?;
    canonicalize_json_value_with_limits(
        &value,
        CanonicalJsonLimits {
            max_document_bytes: MAX_AGENT_EVENT_FRAME_BYTES,
            ..CanonicalJsonLimits::DEFAULT
        },
    )
    .map_err(|_| CanonicalPayloadError::InvalidCanonicalPayload)
}

// Keep this aligned with domain MAX_AGENT_RESULT_SCHEMA_BYTES: the daemon passes
// these canonical bytes into AgentRuntimeBinding::new. The [dependency policy](../../../tests/architecture/allowed-graph.json)
// excludes a protocol-to-domain import; [schema bounds](../../../docs/reference/protocol/agent-events.md#bounds-and-canonical-material)
// describe the public side of that boundary.
const fn undercurrent_domain_result_schema_limit() -> usize {
    64 * 1024
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}
