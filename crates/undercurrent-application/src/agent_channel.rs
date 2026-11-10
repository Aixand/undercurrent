// SPDX-License-Identifier: Apache-2.0

//! Durable technical queue between an authenticated local agent bridge and
//! an explicitly authorized consumer of the public product contract.
//!
//! These records describe an internal port, not an authentication or JSON
//! canonicalization boundary. Callers must supply validated bindings and
//! canonical payloads. Payload-bearing records derive `Debug`; their output
//! must not be sent to general diagnostics.

use undercurrent_domain::{
    FencingToken, OperationAttemptId, PayloadDigest, ProcessInstanceId, SessionId, TimestampMillis,
};

use crate::{PortError, PortFuture};

/// Maximum unsettled calls retained for one agent attempt.
pub const MAX_UNSETTLED_AGENT_EVENTS_PER_ATTEMPT: usize = 32;

/// Exact runtime and caller fence echoed by every queue operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentChannelTarget {
    pub tenant_id: String,
    pub source_product: String,
    pub actor_context_ref: String,
    pub authority_ref: String,
    pub undercurrent_provider_id: String,
    pub undercurrent_provider_generation: u64,
    pub session_id: SessionId,
    /// Present for local admission/reply and absent for a poll before the event is known.
    pub process_instance_id: Option<ProcessInstanceId>,
    pub attempt_id: OperationAttemptId,
    pub attempt_revision: u64,
    pub recovery_epoch: u64,
    pub fence: FencingToken,
    pub request_digest: PayloadDigest,
}

/// One bounded call admitted from an authenticated descendant process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentEventAdmission {
    pub target: AgentChannelTarget,
    pub request_id: String,
    pub call_digest: PayloadDigest,
    pub call_canonical_json: Vec<u8>,
    pub received_at: TimestampMillis,
}

/// One durable unsettled agent call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentEventRecord {
    pub event_id: String,
    pub sequence: u64,
    pub target: AgentChannelTarget,
    pub call_digest: PayloadDigest,
    pub call_canonical_json: Vec<u8>,
    pub received_at: TimestampMillis,
}

/// Durable reply retained independently from the local bridge connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentEventReplyRecord {
    pub event_id: String,
    pub sequence: u64,
    pub reply_digest: PayloadDigest,
    pub outcome_canonical_json: Vec<u8>,
    pub replied_at: TimestampMillis,
}

/// Admission classification for a local bridge call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentEventAdmissionOutcome {
    Queued(AgentEventRecord),
    DuplicatePending(AgentEventRecord),
    DuplicateReplied(AgentEventReplyRecord),
    RequestConflict,
    CapacityExceeded,
    StaleTarget,
    BindingExpired,
    NotFound,
}

/// Result of one exact bounded consumer poll.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentEventPollOutcome {
    None { next_sequence: u64 },
    Event(Box<AgentEventRecord>),
    StaleTarget,
    BindingExpired,
    NotFound,
}

/// Exact reply mutation after the workflow owner has accepted or rejected the event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentEventReplyAdmission {
    pub target: AgentChannelTarget,
    pub event_id: String,
    pub sequence: u64,
    pub call_digest: PayloadDigest,
    pub reply_digest: PayloadDigest,
    pub outcome_canonical_json: Vec<u8>,
    pub replied_at: TimestampMillis,
}

/// Durable reply classification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentEventReplyOutcome {
    Accepted(AgentEventReplyRecord),
    Duplicate(AgentEventReplyRecord),
    Conflict,
    StaleTarget,
    BindingExpired,
    NotFound,
}

/// Fail-closed resolution of a protected local peer to one current binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedAgentChannelBinding {
    pub target: AgentChannelTarget,
    pub expected_result_schema_canonical_json: Vec<u8>,
    pub expires_at: TimestampMillis,
}

/// Fail-closed resolution of a protected local peer to one current binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentChannelTargetResolution {
    Current(Box<ResolvedAgentChannelBinding>),
    BindingExpired,
    NotFound,
    Ambiguous,
}

/// Durable product-owned queue boundary for agent bridge events and replies.
pub trait AgentChannelRepository: Send + Sync {
    fn resolve_agent_channel_target<'a>(
        &'a self,
        session_id: &'a SessionId,
        process_instance_id: &'a ProcessInstanceId,
        observed_at: TimestampMillis,
    ) -> PortFuture<'a, Result<AgentChannelTargetResolution, PortError>>;

    fn enqueue_agent_event<'a>(
        &'a self,
        admission: &'a AgentEventAdmission,
    ) -> PortFuture<'a, Result<AgentEventAdmissionOutcome, PortError>>;

    fn poll_agent_event<'a>(
        &'a self,
        target: &'a AgentChannelTarget,
        after_sequence: u64,
        observed_at: TimestampMillis,
    ) -> PortFuture<'a, Result<AgentEventPollOutcome, PortError>>;

    fn reply_agent_event<'a>(
        &'a self,
        admission: &'a AgentEventReplyAdmission,
    ) -> PortFuture<'a, Result<AgentEventReplyOutcome, PortError>>;

    fn get_agent_event_reply<'a>(
        &'a self,
        target: &'a AgentChannelTarget,
        event_id: &'a str,
        observed_at: TimestampMillis,
    ) -> PortFuture<'a, Result<Option<AgentEventReplyRecord>, PortError>>;
}
