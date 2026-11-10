// SPDX-License-Identifier: Apache-2.0

//! Durable identities and invariants for process-affecting operation attempts.
//!
//! These values do not execute effects or establish store-wide fence ordering.
//! Acceptance must be persisted before IO, and unknown effects require retained
//! adapter evidence before reconciliation. Local duplicate rules differ between
//! settlement (including time) and compensation (matching cleanup evidence).

use std::fmt::{Debug, Display, Formatter};

use undercurrent_foundation::{ContentDigest, OpaqueUri};

use crate::{
    Classification, DomainError, DomainResult, InvalidValueReason, ProcessIdentity, Revision,
    SessionId, TimestampMillis,
};

const MAX_ATTEMPT_ID_BYTES: usize = 128;
const MAX_DISPATCH_CELL_ID_BYTES: usize = 128;
const MAX_OUTPUT_CONTRACT_SCHEMA_BYTES: usize = 128;

/// Hard maximum number of callbacks retained for one externally dispatched attempt.
pub const MAX_RUNTIME_CALLBACK_REFS: usize = 16;

/// Stable identity for one semantic operation attempt.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OperationAttemptId(String);

impl OperationAttemptId {
    /// Validates a non-empty, bounded, payload-safe identity.
    pub fn new(value: impl Into<String>) -> DomainResult<Self> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_ATTEMPT_ID_BYTES
            || !value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':')
            })
        {
            return Err(invalid_attempt(
                "operation_attempt_id",
                "attempt identity must be non-empty, bounded, and use only safe ASCII characters",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the stable identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Derives a bounded internal identity from an already verified digest.
    ///
    /// Public retries should retain their caller-visible identity; this helper
    /// is for daemon-owned recovery and compensation sub-attempts.
    pub fn derived(kind: ProcessMutationKind, digest: PayloadDigest) -> Self {
        Self(format!("auto-{}-{}", kind.as_str(), digest.to_hex()))
    }
}

impl Debug for OperationAttemptId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("OperationAttemptId")
            .field(&self.0)
            .finish()
    }
}

impl Display for OperationAttemptId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Monotonic token fencing reordered process-affecting attempts for one target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FencingToken(u64);

impl FencingToken {
    /// Constructs a non-zero fencing token.
    pub fn new(value: u64) -> DomainResult<Self> {
        if value == 0 {
            return Err(invalid_attempt(
                "fencing_token",
                "fencing token must be non-zero",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the numeric token.
    pub const fn value(self) -> u64 {
        self.0
    }

    /// Advances the token without wrapping.
    pub fn next(self) -> DomainResult<Self> {
        self.0.checked_add(1).map(Self).ok_or_else(|| {
            invalid_attempt("fencing_token", "fencing token cannot advance beyond u64")
        })
    }
}

/// SHA-256 digest of one exact semantic payload or result.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PayloadDigest(ContentDigest);

impl PayloadDigest {
    /// Computes a digest for one exact byte payload.
    pub fn of_bytes(bytes: &[u8]) -> Self {
        Self(ContentDigest::sha256(bytes))
    }

    /// Computes a digest of concatenated slices using an intermediate byte vector.
    ///
    /// Slice boundaries are not encoded. Callers must canonicalize semantic
    /// fields before hashing and bound the total allocation.
    pub fn of_slices<'a>(slices: impl IntoIterator<Item = &'a [u8]>) -> Self {
        let mut bytes = Vec::new();
        for slice in slices {
            bytes.extend_from_slice(slice);
        }
        Self::of_bytes(&bytes)
    }

    /// Parses a 64-character hexadecimal representation.
    pub fn from_hex(value: &str) -> DomainResult<Self> {
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(invalid_attempt(
                "payload_digest",
                "payload digest must contain exactly 64 hexadecimal characters",
            ));
        }
        ContentDigest::parse(&format!("sha256:{value}"))
            .map(Self)
            .map_err(|_| {
                invalid_attempt("payload_digest", "payload digest is not canonical SHA-256")
            })
    }

    /// Returns digest bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }

    /// Returns canonical lowercase hexadecimal.
    pub fn to_hex(self) -> String {
        self.0
            .to_string()
            .strip_prefix("sha256:")
            .expect("Foundation digest always uses the SHA-256 prefix")
            .to_owned()
    }
}

/// Opaque caller-owned identity whose fencing sequence orders runtime dispatches.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DispatchCellIdentity(String);

impl DispatchCellIdentity {
    /// Validates a bounded payload-safe opaque identity.
    pub fn new(value: impl Into<String>) -> DomainResult<Self> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_DISPATCH_CELL_ID_BYTES
            || !value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':')
            })
        {
            return Err(invalid_attempt(
                "dispatch_cell_identity",
                "dispatch cell identity must be non-empty, bounded, and use only safe ASCII characters",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the opaque caller identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Debug for DispatchCellIdentity {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("DispatchCellIdentity")
            .field(&self.0)
            .finish()
    }
}

impl Display for DispatchCellIdentity {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Positive limits compiled into one externally dispatched runtime start.
///
/// Every value is a compiled/requested ceiling retained exactly for audit and
/// reconciliation. This slice does not prove or claim operating-system enforcement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeDispatchLimits {
    wall_time_millis: u64,
    stdout_bytes: u64,
    stderr_bytes: u64,
    memory_bytes: Option<u64>,
    cpu_millis: Option<u32>,
}

impl RuntimeDispatchLimits {
    /// Constructs a complete positive limit set.
    pub fn new(
        wall_time_millis: u64,
        stdout_bytes: u64,
        stderr_bytes: u64,
        memory_bytes: Option<u64>,
        cpu_millis: Option<u32>,
    ) -> DomainResult<Self> {
        if wall_time_millis == 0 || stdout_bytes == 0 || stderr_bytes == 0 {
            return Err(invalid_attempt(
                "runtime_dispatch_limits",
                "wall time, stdout, and stderr limits must be positive",
            ));
        }
        if memory_bytes == Some(0) || cpu_millis == Some(0) {
            return Err(invalid_attempt(
                "runtime_dispatch_limits",
                "optional memory and CPU limits must be positive when present",
            ));
        }
        Ok(Self {
            wall_time_millis,
            stdout_bytes,
            stderr_bytes,
            memory_bytes,
            cpu_millis,
        })
    }

    pub const fn wall_time_millis(&self) -> u64 {
        self.wall_time_millis
    }

    pub const fn stdout_bytes(&self) -> u64 {
        self.stdout_bytes
    }

    pub const fn stderr_bytes(&self) -> u64 {
        self.stderr_bytes
    }

    pub const fn memory_bytes(&self) -> Option<u64> {
        self.memory_bytes
    }

    pub const fn cpu_millis(&self) -> Option<u32> {
        self.cpu_millis
    }
}

/// Exact schema and Foundation privacy contract for canonical runtime output.
#[derive(Clone, PartialEq, Eq)]
pub struct RuntimeOutputContract {
    schema: String,
    schema_digest: PayloadDigest,
    classification: Classification,
}

impl RuntimeOutputContract {
    /// Constructs a bounded, payload-safe output contract.
    pub fn new(
        schema: impl Into<String>,
        schema_digest: PayloadDigest,
        classification: Classification,
    ) -> DomainResult<Self> {
        if classification == Classification::Secret {
            return Err(invalid_attempt(
                "runtime_output_contract",
                "Secret output requires immutable content-reference transport and cannot use the inline runtime result flow",
            ));
        }
        let schema = schema.into();
        if schema.is_empty()
            || schema.len() > MAX_OUTPUT_CONTRACT_SCHEMA_BYTES
            || !schema.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':' | b'/')
            })
        {
            return Err(invalid_attempt(
                "runtime_output_contract",
                "output schema must be non-empty, bounded, and use only safe ASCII characters",
            ));
        }
        Ok(Self {
            schema,
            schema_digest,
            classification,
        })
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }

    pub const fn schema_digest(&self) -> PayloadDigest {
        self.schema_digest
    }

    pub const fn classification(&self) -> Classification {
        self.classification
    }
}

impl Debug for RuntimeOutputContract {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeOutputContract")
            .field("schema", &self.schema)
            .field("classification", &self.classification)
            .finish_non_exhaustive()
    }
}

/// Structurally validated telemetry policy reference retained with one dispatch.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RuntimeTelemetryPolicyRef(OpaqueUri);

impl RuntimeTelemetryPolicyRef {
    /// Parses a bounded canonical URI through Foundation's credential-material checks.
    pub fn new(value: impl Into<String>) -> DomainResult<Self> {
        OpaqueUri::parse(value.into()).map(Self).map_err(|_| {
            invalid_attempt(
                "runtime_telemetry_policy_ref",
                "telemetry policy reference must be a canonical opaque URI",
            )
        })
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Debug for RuntimeTelemetryPolicyRef {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeTelemetryPolicyRef")
            .finish_non_exhaustive()
    }
}

/// Structurally validated output-retention policy reference retained with one dispatch.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RuntimeRetentionPolicyRef(OpaqueUri);

impl RuntimeRetentionPolicyRef {
    /// Parses a bounded canonical URI through Foundation's credential-material checks.
    pub fn new(value: impl Into<String>) -> DomainResult<Self> {
        OpaqueUri::parse(value.into()).map(Self).map_err(|_| {
            invalid_attempt(
                "runtime_retention_policy_ref",
                "retention policy reference must be a canonical opaque URI",
            )
        })
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Debug for RuntimeRetentionPolicyRef {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeRetentionPolicyRef")
            .finish_non_exhaustive()
    }
}

/// Versioned product-owned callback registration; never an endpoint or authority grant.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RuntimeCallbackRef(OpaqueUri);

impl RuntimeCallbackRef {
    /// Parses the bounded runtime-callback 0.1.0-beta contract without normalizing its identity.
    pub fn new(value: impl Into<String>) -> DomainResult<Self> {
        let value = value.into();
        if !has_runtime_callback_shape(&value) {
            return Err(invalid_attempt(
                "runtime_callback_ref",
                "runtime callback must name a product-owned runtime-callback 0.1.0-beta registration",
            ));
        }
        OpaqueUri::parse(value).map(Self).map_err(|_| {
            invalid_attempt(
                "runtime_callback_ref",
                "runtime callback must be a canonical opaque reference",
            )
        })
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Debug for RuntimeCallbackRef {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeCallbackRef")
            .finish_non_exhaustive()
    }
}

fn has_runtime_callback_shape(value: &str) -> bool {
    if value.len() > 256 {
        return false;
    }
    let Some(path) = value.strip_prefix("runtime-callback://") else {
        return false;
    };
    let mut parts = path.split('/');
    let (Some(scope), Some(product), Some(version), Some(id), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return false;
    };
    (1..=64).contains(&scope.len())
        && scope
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase())
        && scope
            .bytes()
            .last()
            .is_some_and(|byte| byte.is_ascii_alphanumeric())
        && scope
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && version == "v0.1.0-beta"
        && (1..=64).contains(&product.len())
        && product
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_lowercase)
        && product
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
        && product
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && (1..=128).contains(&id.len())
        && id.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
        && id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
}

/// Consequential profile-compiled policy retained with one external dispatch.
#[derive(Clone, PartialEq, Eq)]
pub struct RuntimeDispatchPolicy {
    limits: RuntimeDispatchLimits,
    output_contract: RuntimeOutputContract,
    telemetry_policy_ref: RuntimeTelemetryPolicyRef,
    retention_policy_ref: Option<RuntimeRetentionPolicyRef>,
    callback_refs: Vec<RuntimeCallbackRef>,
    launch_profile_expires_at: TimestampMillis,
    environment_binding_expires_at: TimestampMillis,
}

impl RuntimeDispatchPolicy {
    /// Constructs a strict policy without normalizing caller order or expiry.
    pub fn new(
        limits: RuntimeDispatchLimits,
        output_contract: RuntimeOutputContract,
        telemetry_policy_ref: RuntimeTelemetryPolicyRef,
        retention_policy_ref: RuntimeRetentionPolicyRef,
        callback_refs: Vec<RuntimeCallbackRef>,
        launch_profile_expires_at: TimestampMillis,
        environment_binding_expires_at: TimestampMillis,
    ) -> DomainResult<Self> {
        Self::build(
            limits,
            output_contract,
            telemetry_policy_ref,
            Some(retention_policy_ref),
            callback_refs,
            launch_profile_expires_at,
            environment_binding_expires_at,
        )
    }

    fn build(
        limits: RuntimeDispatchLimits,
        output_contract: RuntimeOutputContract,
        telemetry_policy_ref: RuntimeTelemetryPolicyRef,
        retention_policy_ref: Option<RuntimeRetentionPolicyRef>,
        callback_refs: Vec<RuntimeCallbackRef>,
        launch_profile_expires_at: TimestampMillis,
        environment_binding_expires_at: TimestampMillis,
    ) -> DomainResult<Self> {
        if callback_refs.len() > MAX_RUNTIME_CALLBACK_REFS {
            return Err(invalid_attempt(
                "runtime_callback_refs",
                "runtime callback reference count exceeds the hard limit",
            ));
        }
        if callback_refs.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(invalid_attempt(
                "runtime_callback_refs",
                "runtime callback references must be strictly sorted and duplicate-free",
            ));
        }
        if launch_profile_expires_at.value() <= 0 || environment_binding_expires_at.value() <= 0 {
            return Err(invalid_attempt(
                "runtime_dispatch_expiry",
                "launch-profile and environment-binding expiries must be positive Unix milliseconds",
            ));
        }
        Ok(Self {
            limits,
            output_contract,
            telemetry_policy_ref,
            retention_policy_ref,
            callback_refs,
            launch_profile_expires_at,
            environment_binding_expires_at,
        })
    }

    pub const fn limits(&self) -> RuntimeDispatchLimits {
        self.limits
    }

    pub const fn output_contract(&self) -> &RuntimeOutputContract {
        &self.output_contract
    }

    pub const fn telemetry_policy_ref(&self) -> &RuntimeTelemetryPolicyRef {
        &self.telemetry_policy_ref
    }

    pub const fn retention_policy_ref(&self) -> Option<&RuntimeRetentionPolicyRef> {
        self.retention_policy_ref.as_ref()
    }

    pub fn callback_refs(&self) -> &[RuntimeCallbackRef] {
        &self.callback_refs
    }

    pub const fn launch_profile_expires_at(&self) -> TimestampMillis {
        self.launch_profile_expires_at
    }

    pub const fn environment_binding_expires_at(&self) -> TimestampMillis {
        self.environment_binding_expires_at
    }

    /// Rejects a dispatch whose profile or binding is already expired.
    pub fn validate_at(&self, accepted_at: TimestampMillis) -> DomainResult<()> {
        if self.launch_profile_expires_at <= accepted_at
            || self.environment_binding_expires_at <= accepted_at
        {
            return Err(invalid_attempt(
                "runtime_dispatch_expiry",
                "launch profile and environment binding must be current when a new runtime effect is admitted",
            ));
        }
        Ok(())
    }
}

impl Debug for RuntimeDispatchPolicy {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeDispatchPolicy")
            .field("limits", &self.limits)
            .field("output_contract", &self.output_contract)
            .field("callback_count", &self.callback_refs.len())
            .field("launch_profile_expires_at", &self.launch_profile_expires_at)
            .field(
                "environment_binding_expires_at",
                &self.environment_binding_expires_at,
            )
            .finish_non_exhaustive()
    }
}

/// Immutable opaque identity, content pins, and policy attached to one dispatched attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeDispatchBinding {
    caller_cell_identity: DispatchCellIdentity,
    request_digest: Option<PayloadDigest>,
    launch_profile_digest: PayloadDigest,
    environment_binding_digest: PayloadDigest,
    input_digest: PayloadDigest,
    runtime_policy: Option<RuntimeDispatchPolicy>,
    agent: Option<crate::AgentRuntimeBinding>,
}

impl RuntimeDispatchBinding {
    /// Constructs a complete current dispatch binding from independently validated values.
    pub const fn new(
        caller_cell_identity: DispatchCellIdentity,
        request_digest: PayloadDigest,
        launch_profile_digest: PayloadDigest,
        environment_binding_digest: PayloadDigest,
        input_digest: PayloadDigest,
        runtime_policy: RuntimeDispatchPolicy,
    ) -> Self {
        Self {
            caller_cell_identity,
            request_digest: Some(request_digest),
            launch_profile_digest,
            environment_binding_digest,
            input_digest,
            runtime_policy: Some(runtime_policy),
            agent: None,
        }
    }

    pub const fn caller_cell_identity(&self) -> &DispatchCellIdentity {
        &self.caller_cell_identity
    }

    /// Returns the caller's exact request digest retained by the current binding.
    pub const fn request_digest(&self) -> Option<PayloadDigest> {
        self.request_digest
    }

    pub const fn launch_profile_digest(&self) -> PayloadDigest {
        self.launch_profile_digest
    }

    pub const fn environment_binding_digest(&self) -> PayloadDigest {
        self.environment_binding_digest
    }

    pub const fn input_digest(&self) -> PayloadDigest {
        self.input_digest
    }

    /// Returns the exact policy retained by the current binding.
    pub const fn runtime_policy(&self) -> Option<&RuntimeDispatchPolicy> {
        self.runtime_policy.as_ref()
    }

    /// Attaches the exact caller-owned agent channel identity before admission.
    #[must_use]
    pub fn with_agent(mut self, agent: crate::AgentRuntimeBinding) -> Self {
        self.agent = Some(agent);
        self
    }

    /// Returns the agent channel identity when this is an agent-task runtime.
    pub const fn agent(&self) -> Option<&crate::AgentRuntimeBinding> {
        self.agent.as_ref()
    }
}

impl Debug for PayloadDigest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("PayloadDigest")
            .field(&self.to_hex())
            .finish()
    }
}

/// Durable exact cleanup target prepared before compensating a superseded dispatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchCompensationIntent {
    superseding_fencing_token: FencingToken,
    process: ProcessIdentity,
    session_revision: Revision,
    cleanup_digest: PayloadDigest,
    prepared_at: TimestampMillis,
}

impl DispatchCompensationIntent {
    pub const fn superseding_fencing_token(&self) -> FencingToken {
        self.superseding_fencing_token
    }

    pub const fn process(&self) -> &ProcessIdentity {
        &self.process
    }

    pub const fn session_revision(&self) -> Revision {
        self.session_revision
    }

    pub const fn cleanup_digest(&self) -> PayloadDigest {
        self.cleanup_digest
    }

    pub const fn prepared_at(&self) -> TimestampMillis {
        self.prepared_at
    }
}

impl Display for PayloadDigest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

/// Process-affecting semantic operation protected by an attempt and fence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProcessMutationKind {
    /// Create a new process incarnation.
    Spawn,
    /// Execute one bounded command under daemon supervision.
    ExecuteInSession,
    /// Arm exact-incarnation exit delivery.
    ArmExit,
    /// Request graceful termination.
    Stop,
    /// Force termination.
    ForceStop,
    /// Write one atomic input sequence.
    WriteInput,
    /// Resize one live PTY.
    Resize,
    /// Shut down the external daemon.
    Shutdown,
}

impl ProcessMutationKind {
    /// Complete stable registry.
    pub const ALL: [Self; 8] = [
        Self::Spawn,
        Self::ExecuteInSession,
        Self::ArmExit,
        Self::Stop,
        Self::ForceStop,
        Self::WriteInput,
        Self::Resize,
        Self::Shutdown,
    ];

    /// Returns the stable persistence and diagnostic name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Spawn => "spawn",
            Self::ExecuteInSession => "execute_in_session",
            Self::ArmExit => "arm_exit",
            Self::Stop => "stop",
            Self::ForceStop => "force_stop",
            Self::WriteInput => "write_input",
            Self::Resize => "resize",
            Self::Shutdown => "shutdown",
        }
    }
}

/// What is durably known about an external side effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EffectCertainty {
    /// Side effect is known not to have happened.
    KnownAbsent,
    /// Side effect is known to have happened.
    KnownApplied,
    /// Response loss or interruption prevents a safe conclusion.
    Unknown,
}

/// Explicit operating-system truth for one durable process incarnation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessTruth {
    /// Exact incarnation is alive and owned.
    Running { observed_at: TimestampMillis },
    /// Exact incarnation exited with a known code.
    Exited {
        exit_code: i32,
        observed_at: TimestampMillis,
    },
    /// Exact process incarnation is known to be absent at observation time.
    Missing { observed_at: TimestampMillis },
    /// Owned incarnation was lost and no exit result is recoverable.
    Lost { observed_at: TimestampMillis },
    /// A process exists but ownership cannot safely be attributed.
    Orphaned { observed_at: TimestampMillis },
    /// Adapter could not establish either presence or absence.
    Unknown { observed_at: TimestampMillis },
}

impl ProcessTruth {
    /// Returns the coarse truth kind.
    pub const fn kind(self) -> ProcessTruthKind {
        match self {
            Self::Running { .. } => ProcessTruthKind::Running,
            Self::Exited { .. } => ProcessTruthKind::Exited,
            Self::Missing { .. } => ProcessTruthKind::Missing,
            Self::Lost { .. } => ProcessTruthKind::Lost,
            Self::Orphaned { .. } => ProcessTruthKind::Orphaned,
            Self::Unknown { .. } => ProcessTruthKind::Unknown,
        }
    }

    /// Returns when truth was observed.
    pub const fn observed_at(self) -> TimestampMillis {
        match self {
            Self::Running { observed_at }
            | Self::Missing { observed_at }
            | Self::Lost { observed_at }
            | Self::Orphaned { observed_at }
            | Self::Unknown { observed_at }
            | Self::Exited { observed_at, .. } => observed_at,
        }
    }
}

/// Coarse process-truth kind safe for diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProcessTruthKind {
    /// Exact process is running.
    Running,
    /// Exact process exited.
    Exited,
    /// Exact process incarnation is known to be absent.
    Missing,
    /// Owned process was lost.
    Lost,
    /// Process ownership is orphaned.
    Orphaned,
    /// Truth is inconclusive.
    Unknown,
}

/// Durable state of one operation attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationAttemptState {
    /// Accepted before external IO.
    Accepted,
    /// Settled after return or ambiguity.
    Settled {
        /// Effect certainty.
        certainty: EffectCertainty,
        /// Exact result digest for a known applied effect.
        result_digest: Option<PayloadDigest>,
        /// Settlement time.
        settled_at: TimestampMillis,
    },
    /// A dispatched effect was superseded and its exact spawned incarnation was cleaned up.
    Compensated {
        /// Current same-cell fence that superseded the original dispatch.
        superseding_fencing_token: FencingToken,
        /// Digest of retained evidence for exact process, binding, owner, and lifecycle cleanup.
        cleanup_digest: PayloadDigest,
        /// Time the compensation audit became durable.
        compensated_at: TimestampMillis,
    },
}

impl OperationAttemptState {
    /// Returns whether reconciliation is required before retry.
    pub const fn requires_reconciliation(&self) -> bool {
        matches!(
            self,
            Self::Accepted
                | Self::Settled {
                    certainty: EffectCertainty::Unknown,
                    ..
                }
        )
    }
}

/// Durable product-owned record for one process-affecting attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationAttempt {
    id: OperationAttemptId,
    target_session: Option<SessionId>,
    kind: ProcessMutationKind,
    fencing_token: FencingToken,
    payload_digest: PayloadDigest,
    dispatch_binding: Option<RuntimeDispatchBinding>,
    compensation_intent: Option<DispatchCompensationIntent>,
    accepted_at: TimestampMillis,
    state: OperationAttemptState,
}

impl OperationAttempt {
    /// Creates a record that must commit before the side effect.
    pub const fn accepted(
        id: OperationAttemptId,
        target_session: Option<SessionId>,
        kind: ProcessMutationKind,
        fencing_token: FencingToken,
        payload_digest: PayloadDigest,
        accepted_at: TimestampMillis,
    ) -> Self {
        Self::accepted_with_dispatch(
            id,
            target_session,
            kind,
            fencing_token,
            payload_digest,
            None,
            accepted_at,
        )
    }

    /// Creates a record with an optional immutable external dispatch binding.
    pub const fn accepted_with_dispatch(
        id: OperationAttemptId,
        target_session: Option<SessionId>,
        kind: ProcessMutationKind,
        fencing_token: FencingToken,
        payload_digest: PayloadDigest,
        dispatch_binding: Option<RuntimeDispatchBinding>,
        accepted_at: TimestampMillis,
    ) -> Self {
        Self {
            id,
            target_session,
            kind,
            fencing_token,
            payload_digest,
            dispatch_binding,
            compensation_intent: None,
            accepted_at,
            state: OperationAttemptState::Accepted,
        }
    }

    /// Returns attempt identity.
    pub const fn id(&self) -> &OperationAttemptId {
        &self.id
    }
    /// Returns optional owning session.
    pub const fn target_session(&self) -> Option<&SessionId> {
        self.target_session.as_ref()
    }
    /// Returns mutation kind.
    pub const fn kind(&self) -> ProcessMutationKind {
        self.kind
    }
    /// Returns fencing token.
    pub const fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }
    /// Returns semantic payload digest.
    pub const fn payload_digest(&self) -> PayloadDigest {
        self.payload_digest
    }
    /// Returns the immutable external dispatch binding, if this was dispatched.
    pub const fn dispatch_binding(&self) -> Option<&RuntimeDispatchBinding> {
        self.dispatch_binding.as_ref()
    }

    /// Restores an additively persisted agent binding onto its exact attempt.
    ///
    /// This is intentionally limited to a dispatch that already retains the
    /// same attempt, fence, and caller request digest.
    pub fn restore_agent_binding(&mut self, agent: crate::AgentRuntimeBinding) -> DomainResult<()> {
        let Some(dispatch) = self.dispatch_binding.as_mut() else {
            return Err(invalid_attempt(
                "agent_runtime_binding",
                "agent binding requires an attached runtime dispatch",
            ));
        };
        if dispatch.agent().is_some()
            || agent.attempt_id() != &self.id
            || agent.fence() != self.fencing_token
            || dispatch.request_digest() != Some(agent.request_digest())
        {
            return Err(invalid_attempt(
                "agent_runtime_binding",
                "agent binding does not match its retained runtime attempt",
            ));
        }
        *dispatch = dispatch.clone().with_agent(agent);
        Ok(())
    }
    /// Returns exact durable pre-cleanup intent for a superseded dispatch, if prepared.
    pub const fn compensation_intent(&self) -> Option<&DispatchCompensationIntent> {
        self.compensation_intent.as_ref()
    }
    /// Returns acceptance time.
    pub const fn accepted_at(&self) -> TimestampMillis {
        self.accepted_at
    }
    /// Returns durable state.
    pub const fn state(&self) -> &OperationAttemptState {
        &self.state
    }

    /// Returns whether another record is an exact semantic duplicate.
    pub fn has_same_semantics(&self, other: &Self) -> bool {
        self.id == other.id
            && self.target_session == other.target_session
            && self.kind == other.kind
            && self.fencing_token == other.fencing_token
            && self.payload_digest == other.payload_digest
            && self.dispatch_binding == other.dispatch_binding
    }

    /// Settles an accepted attempt and deduplicates an identical result.
    ///
    /// Duplicates include `settled_at` in equality. This method checks the accepted
    /// fence and digest/certainty combination, but does not compare settlement
    /// time with acceptance time or consult the current fence in a store.
    pub fn settle(
        &mut self,
        fencing_token: FencingToken,
        certainty: EffectCertainty,
        result_digest: Option<PayloadDigest>,
        settled_at: TimestampMillis,
    ) -> DomainResult<OperationAttemptTransition> {
        if fencing_token != self.fencing_token {
            return Err(invalid_attempt(
                "fencing_token",
                "settlement fencing token must equal the accepted attempt token",
            ));
        }
        validate_settlement(certainty, result_digest)?;
        let next = OperationAttemptState::Settled {
            certainty,
            result_digest,
            settled_at,
        };
        // Prepared cleanup must remain recoverable through its exact intent.
        // A normal settlement could otherwise hide unfinished cleanup behind a
        // known effect. See [compensation guards](../docs/internals/attempts-and-recovery.md#prepare-and-confirm-compensation).
        match &self.state {
            OperationAttemptState::Accepted if self.compensation_intent.is_none() => {
                self.state = next;
                Ok(OperationAttemptTransition::Applied)
            }
            OperationAttemptState::Accepted => Err(invalid_attempt(
                "operation_attempt",
                "a prepared compensation cannot be settled as an applied dispatch",
            )),
            current if *current == next => Ok(OperationAttemptTransition::Duplicate),
            OperationAttemptState::Settled { .. } => Err(invalid_attempt(
                "operation_attempt",
                "a settled attempt cannot change except by unknown-effect reconciliation",
            )),
            OperationAttemptState::Compensated { .. } => Err(invalid_attempt(
                "operation_attempt",
                "a compensated attempt cannot be settled",
            )),
        }
    }

    /// Prepares the exact cleanup target for persistence before compensation begins.
    ///
    /// The caller must verify the current cell fence and persist this intent
    /// before destructive cleanup. This method changes only the in-memory
    /// attempt: it retains Accepted state and blocks ordinary settlement so
    /// recovery follows the prepared cleanup. Repeating the same target, revision, digest,
    /// and superseding fence is a no-op even if the preparation time differs.
    ///
    /// # Errors
    ///
    /// Rejects a mismatched original fence, missing dispatch, non-increasing
    /// superseding fence, non-Accepted state, or conflicting cleanup intent.
    pub fn prepare_compensation(
        &mut self,
        fencing_token: FencingToken,
        superseding_fencing_token: FencingToken,
        process: ProcessIdentity,
        session_revision: Revision,
        cleanup_digest: PayloadDigest,
        prepared_at: TimestampMillis,
    ) -> DomainResult<OperationAttemptTransition> {
        if fencing_token != self.fencing_token {
            return Err(invalid_attempt(
                "fencing_token",
                "compensation preparation token must equal the accepted attempt token",
            ));
        }
        if self.dispatch_binding.is_none() || superseding_fencing_token <= fencing_token {
            return Err(invalid_attempt(
                "superseding_fencing_token",
                "compensation preparation requires a dispatched attempt and newer fence",
            ));
        }
        if !matches!(self.state, OperationAttemptState::Accepted) {
            return Err(invalid_attempt(
                "operation_attempt",
                "only an accepted dispatch may prepare compensation",
            ));
        }
        let next = DispatchCompensationIntent {
            superseding_fencing_token,
            process,
            session_revision,
            cleanup_digest,
            prepared_at,
        };
        match &self.compensation_intent {
            None => {
                self.compensation_intent = Some(next);
                Ok(OperationAttemptTransition::Applied)
            }
            Some(current)
                if current.superseding_fencing_token == next.superseding_fencing_token
                    && current.process == next.process
                    && current.session_revision == next.session_revision
                    && current.cleanup_digest == next.cleanup_digest =>
            {
                Ok(OperationAttemptTransition::Duplicate)
            }
            Some(_) => Err(invalid_attempt(
                "operation_attempt",
                "compensation preparation conflicts with the durable cleanup target",
            )),
        }
    }

    /// Terminalizes an accepted dispatched attempt after exact cleanup under a newer cell fence.
    pub fn compensate(
        &mut self,
        fencing_token: FencingToken,
        superseding_fencing_token: FencingToken,
        cleanup_digest: PayloadDigest,
        compensated_at: TimestampMillis,
    ) -> DomainResult<OperationAttemptTransition> {
        if fencing_token != self.fencing_token {
            return Err(invalid_attempt(
                "fencing_token",
                "compensation fencing token must equal the accepted attempt token",
            ));
        }
        if self.dispatch_binding.is_none() {
            return Err(invalid_attempt(
                "dispatch_binding",
                "only an externally dispatched attempt may be compensated",
            ));
        }
        if superseding_fencing_token <= fencing_token {
            return Err(invalid_attempt(
                "superseding_fencing_token",
                "compensation requires a strictly newer same-cell fence",
            ));
        }
        let Some(intent) = self.compensation_intent.as_ref() else {
            return Err(invalid_attempt(
                "operation_attempt",
                "compensation requires a durable exact cleanup intent",
            ));
        };
        if intent.superseding_fencing_token != superseding_fencing_token
            || intent.cleanup_digest != cleanup_digest
        {
            return Err(invalid_attempt(
                "operation_attempt",
                "compensation must match the durable cleanup intent",
            ));
        }
        match &self.state {
            OperationAttemptState::Accepted => {
                self.state = OperationAttemptState::Compensated {
                    superseding_fencing_token,
                    cleanup_digest,
                    compensated_at,
                };
                Ok(OperationAttemptTransition::Applied)
            }
            OperationAttemptState::Compensated {
                superseding_fencing_token: current_fence,
                cleanup_digest: current_digest,
                ..
            } if *current_fence == superseding_fencing_token
                && *current_digest == cleanup_digest =>
            {
                Ok(OperationAttemptTransition::Duplicate)
            }
            _ => Err(invalid_attempt(
                "operation_attempt",
                "only an accepted dispatch may be compensated with exact cleanup evidence",
            )),
        }
    }

    /// Reconciles only an unknown effect into confirmed truth.
    pub fn reconcile(
        &mut self,
        certainty: EffectCertainty,
        result_digest: Option<PayloadDigest>,
        reconciled_at: TimestampMillis,
    ) -> DomainResult<OperationAttemptTransition> {
        if certainty == EffectCertainty::Unknown {
            return Err(invalid_attempt(
                "effect_certainty",
                "reconciliation must establish a known effect",
            ));
        }
        validate_settlement(certainty, result_digest)?;
        let next = OperationAttemptState::Settled {
            certainty,
            result_digest,
            settled_at: reconciled_at,
        };
        match &self.state {
            OperationAttemptState::Settled {
                certainty: EffectCertainty::Unknown,
                settled_at,
                ..
            } if reconciled_at >= *settled_at => {
                self.state = next;
                Ok(OperationAttemptTransition::Applied)
            }
            current if *current == next => Ok(OperationAttemptTransition::Duplicate),
            _ => Err(invalid_attempt(
                "operation_attempt",
                "only an unknown-effect settlement may be reconciled",
            )),
        }
    }
}

/// Whether a requested transition changed durable state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationAttemptTransition {
    /// State changed.
    Applied,
    /// Exact transition was already durable.
    Duplicate,
}

fn validate_settlement(
    certainty: EffectCertainty,
    result_digest: Option<PayloadDigest>,
) -> DomainResult<()> {
    if certainty == EffectCertainty::KnownApplied && result_digest.is_none() {
        return Err(invalid_attempt(
            "result_digest",
            "a known applied effect requires a result digest",
        ));
    }
    if certainty == EffectCertainty::KnownAbsent && result_digest.is_some() {
        return Err(invalid_attempt(
            "result_digest",
            "a known absent effect cannot carry a result digest",
        ));
    }
    Ok(())
}

fn invalid_attempt(field: &'static str, rule: &'static str) -> DomainError {
    DomainError::InvalidValue {
        field,
        reason: InvalidValueReason::InvalidCombination { rule },
    }
}
