// SPDX-License-Identifier: Apache-2.0

//! Exact, durable runtime-attempt cancellation and status DTOs.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};
use serde_json::json;
use undercurrent_foundation::{CanonicalJsonLimits, Digest, canonicalize_json_value_with_limits};

use crate::{
    ContractPayloadKind, LifecycleStateDto, MutationContext, OperationAttemptDto,
    ProductContractDto, ProductOperation, validate_mutation_contract, validate_product_contract,
};

/// Domain discriminator for newly generated exact-integer cancellation digests.
///
/// This Undercurrent-owned namespace is cryptographic input. Product contract
/// 3 accepts only this domain; retained hashes are never rewritten.
pub const RUNTIME_CANCELLATION_COMMAND_DIGEST_DOMAIN: &str =
    "undercurrent.runtime.cancel.command/v0.1.0-beta";

const EXACT_PRODUCT_DIGEST_DOMAIN: &str = "undercurrent.runtime.cancel.product-request/v0.1.0-beta";

const MAX_CONTROL_ID_BYTES: usize = 128;
const MAX_ATTEMPT_ID_BYTES: usize = 128;
const MAX_CELL_ID_BYTES: usize = 128;
const MAX_SESSION_ID_BYTES: usize = 256;
const MAX_SOURCE_PRODUCT_BYTES: usize = 128;
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 256;
const MAX_REASON_BYTES: usize = 512;
const MAX_CONTEXT_REFERENCE_BYTES: usize = 512;
const SHA256_HEX_CHARS: usize = 64;

/// Process-control mode for one exact attached runtime attempt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeCancellationModeDto {
    /// Ask the owned process to stop cooperatively.
    ///
    /// Timeout does not authorize force. The application persists no-escalation
    /// intent; force requires a separately authorized command.
    /// See [ADR 0003](../../../docs/architecture/decisions/0003-runtime-cancellation-graceful-authority.md).
    Graceful,
    /// Apply an independently authorized force-stop command.
    Force,
}

impl RuntimeCancellationModeDto {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Graceful => "graceful",
            Self::Force => "force",
        }
    }
}

/// Runtime cancellation is deliberately limited to its exact target.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeCancellationPropagationPolicyDto {
    /// Never expand the command to a session, cell, grain, or operation.
    TargetOnly,
}

impl RuntimeCancellationPropagationPolicyDto {
    const fn as_str(self) -> &'static str {
        match self {
            Self::TargetOnly => "target_only",
        }
    }
}

/// Complete immutable predecessor binding for a runtime cancellation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeCancellationTargetBindingDto {
    target_attempt_id: String,
    target_request_digest_sha256: String,
    target_fencing_generation: u64,
    caller_cell_identity: String,
    session_id: String,
    expected_session_revision: u64,
    expected_lifecycle_revision: u64,
    expected_provider_id: String,
    expected_provider_generation: u64,
}

impl RuntimeCancellationTargetBindingDto {
    /// Constructs a fully pinned predecessor/session/provider binding.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        target_attempt_id: String,
        target_request_digest_sha256: String,
        target_fencing_generation: u64,
        caller_cell_identity: String,
        session_id: String,
        expected_session_revision: u64,
        expected_lifecycle_revision: u64,
        expected_provider_id: String,
        expected_provider_generation: u64,
    ) -> Result<Self, RuntimeCancellationDtoError> {
        validate_safe_identity(
            &target_attempt_id,
            MAX_ATTEMPT_ID_BYTES,
            RuntimeCancellationDtoError::InvalidTargetAttemptId,
        )?;
        validate_digest(
            &target_request_digest_sha256,
            RuntimeCancellationDtoError::InvalidTargetRequestDigest,
        )?;
        if target_fencing_generation == 0 {
            return Err(RuntimeCancellationDtoError::InvalidTargetFencingGeneration);
        }
        validate_safe_identity(
            &caller_cell_identity,
            MAX_CELL_ID_BYTES,
            RuntimeCancellationDtoError::InvalidCallerCellIdentity,
        )?;
        validate_session_id(
            &session_id,
            MAX_SESSION_ID_BYTES,
            RuntimeCancellationDtoError::InvalidSessionId,
        )?;
        if expected_session_revision == 0
            || expected_lifecycle_revision == 0
            || expected_lifecycle_revision > expected_session_revision
        {
            return Err(RuntimeCancellationDtoError::InvalidExpectedRevision);
        }
        validate_provider_id(&expected_provider_id)?;
        if expected_provider_generation == 0 {
            return Err(RuntimeCancellationDtoError::InvalidProviderGeneration);
        }
        Ok(Self {
            target_attempt_id,
            target_request_digest_sha256,
            target_fencing_generation,
            caller_cell_identity,
            session_id,
            expected_session_revision,
            expected_lifecycle_revision,
            expected_provider_id,
            expected_provider_generation,
        })
    }

    /// Returns the exact predecessor runtime-start attempt identity.
    pub fn target_attempt_id(&self) -> &str {
        &self.target_attempt_id
    }

    /// Returns the canonical digest of the exact predecessor start request.
    pub fn target_request_digest_sha256(&self) -> &str {
        &self.target_request_digest_sha256
    }

    /// Returns the exact predecessor dispatch fencing generation.
    pub const fn target_fencing_generation(&self) -> u64 {
        self.target_fencing_generation
    }

    /// Returns the caller cell whose fence owns the predecessor attempt.
    pub fn caller_cell_identity(&self) -> &str {
        &self.caller_cell_identity
    }

    /// Returns the exact session created for the predecessor attempt.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Returns the exact expected aggregate revision at admission.
    pub const fn expected_session_revision(&self) -> u64 {
        self.expected_session_revision
    }

    /// Returns the exact expected lifecycle revision at admission.
    pub const fn expected_lifecycle_revision(&self) -> u64 {
        self.expected_lifecycle_revision
    }

    /// Returns the exact provider selected for the predecessor start.
    pub fn expected_provider_id(&self) -> &str {
        &self.expected_provider_id
    }

    /// Returns the immutable provider generation selected for the start.
    pub const fn expected_provider_generation(&self) -> u64 {
        self.expected_provider_generation
    }

    /// Validates a target decoded through a generic transport codec.
    pub fn validate(&self) -> Result<(), RuntimeCancellationDtoError> {
        Self::new(
            self.target_attempt_id.clone(),
            self.target_request_digest_sha256.clone(),
            self.target_fencing_generation,
            self.caller_cell_identity.clone(),
            self.session_id.clone(),
            self.expected_session_revision,
            self.expected_lifecycle_revision,
            self.expected_provider_id.clone(),
            self.expected_provider_generation,
        )
        .map(|_| ())
    }
}

/// Complete semantic and envelope context retained for one cancellation command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeCancellationCommandDigestMaterial {
    target: RuntimeCancellationTargetBindingDto,
    control_id: String,
    idempotency_key: String,
    mode: RuntimeCancellationModeDto,
    reason: String,
    propagation_policy: RuntimeCancellationPropagationPolicyDto,
    absolute_deadline_millis: i64,
    source_product: String,
    tenant_id: String,
    actor_context_ref: String,
    authority_ref: String,
}

impl RuntimeCancellationCommandDigestMaterial {
    /// Constructs the complete canonical command material.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        target: RuntimeCancellationTargetBindingDto,
        control_id: String,
        idempotency_key: String,
        mode: RuntimeCancellationModeDto,
        reason: String,
        propagation_policy: RuntimeCancellationPropagationPolicyDto,
        absolute_deadline_millis: i64,
        source_product: String,
        tenant_id: String,
        actor_context_ref: String,
        authority_ref: String,
    ) -> Result<Self, RuntimeCancellationDtoError> {
        target.validate()?;
        validate_safe_identity(
            &control_id,
            MAX_CONTROL_ID_BYTES,
            RuntimeCancellationDtoError::InvalidControlId,
        )?;
        if control_id == target.target_attempt_id {
            return Err(RuntimeCancellationDtoError::ControlTargetIdentityCollision);
        }
        validate_idempotency_key(
            &idempotency_key,
            MAX_IDEMPOTENCY_KEY_BYTES,
            RuntimeCancellationDtoError::InvalidIdempotencyKey,
        )?;
        validate_bounded_plain(
            &reason,
            MAX_REASON_BYTES,
            RuntimeCancellationDtoError::InvalidReason,
        )?;
        // Check representation here, not freshness: exact retained replay can
        // remain valid after this original deadline. The owner checks its clock
        // for [new admission](../../../docs/reference/protocol/cancellation.md#reconcile-a-lost-response).
        if absolute_deadline_millis <= 0 {
            return Err(RuntimeCancellationDtoError::InvalidAbsoluteDeadline);
        }
        validate_product_coordinate(
            &source_product,
            RuntimeCancellationDtoError::InvalidSourceProduct,
        )?;
        validate_tenant_id(&tenant_id)?;
        validate_bounded_plain(
            &actor_context_ref,
            MAX_CONTEXT_REFERENCE_BYTES,
            RuntimeCancellationDtoError::InvalidActorContextRef,
        )?;
        validate_bounded_plain(
            &authority_ref,
            MAX_CONTEXT_REFERENCE_BYTES,
            RuntimeCancellationDtoError::InvalidAuthorityRef,
        )?;
        Ok(Self {
            target,
            control_id,
            idempotency_key,
            mode,
            reason,
            propagation_policy,
            absolute_deadline_millis,
            source_product,
            tenant_id,
            actor_context_ref,
            authority_ref,
        })
    }

    /// Returns the immutable cancellation target.
    pub const fn target(&self) -> &RuntimeCancellationTargetBindingDto {
        &self.target
    }

    /// Returns the distinct control-command identity.
    pub fn control_id(&self) -> &str {
        &self.control_id
    }

    /// Returns the enclosing command's exact idempotency key.
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    /// Returns the independently authorized process-control mode.
    pub const fn mode(&self) -> RuntimeCancellationModeDto {
        self.mode
    }

    /// Returns the bounded cancellation reason.
    pub fn reason(&self) -> &str {
        &self.reason
    }

    /// Returns the exact target-only propagation policy.
    pub const fn propagation_policy(&self) -> RuntimeCancellationPropagationPolicyDto {
        self.propagation_policy
    }

    /// Returns the enclosing command's absolute admission deadline.
    pub const fn absolute_deadline_millis(&self) -> i64 {
        self.absolute_deadline_millis
    }

    /// Returns the authenticated source product asserted by the caller adapter.
    pub fn source_product(&self) -> &str {
        &self.source_product
    }

    /// Returns the exact tenant scope.
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Returns the current actor-context reference.
    pub fn actor_context_ref(&self) -> &str {
        &self.actor_context_ref
    }

    /// Returns the current control-authority reference.
    pub fn authority_ref(&self) -> &str {
        &self.authority_ref
    }

    /// Computes the 0.1.0-beta SHA-256 with exact decimal-string integer pins.
    /// Only the current Undercurrent namespace is accepted.
    pub fn canonical_digest_sha256(&self) -> Result<String, RuntimeCancellationDtoError> {
        canonical_runtime_cancellation_command_digest_sha256(self)
    }
}

/// Computes the domain-separated caller digest for the semantic cancellation command.
///
/// The input is UTF-8 canonical JSON with lexicographically sorted object keys,
/// unsigned integer pins encoded as decimal strings, and no insignificant whitespace. It contains
/// `digest_domain` plus every public Undercurrent runtime-cancel request field except
/// `command_digest_sha256`. Envelope idempotency, deadline, source, tenant,
/// actor, authority, mutation metadata, and product-contract fields are
/// excluded and remain separately integrity-protected and retained.
///
/// Product contract 3 accepts only this exact-integer Undercurrent digest.
/// Older digest domains are rejected, including previously retained controls;
/// never replace a stored hash under the same control identity.
pub fn canonical_runtime_cancellation_command_digest_sha256(
    material: &RuntimeCancellationCommandDigestMaterial,
) -> Result<String, RuntimeCancellationDtoError> {
    command_digest(material)
}

fn command_digest(
    material: &RuntimeCancellationCommandDigestMaterial,
) -> Result<String, RuntimeCancellationDtoError> {
    let target = material.target();
    let mut value = json!({
        "caller_cell_identity": target.caller_cell_identity(),
        "control_id": material.control_id(),
        "digest_domain": RUNTIME_CANCELLATION_COMMAND_DIGEST_DOMAIN,
        "expected_lifecycle_revision": target.expected_lifecycle_revision(),
        "expected_provider_generation": target.expected_provider_generation(),
        "expected_provider_id": target.expected_provider_id(),
        "expected_session_revision": target.expected_session_revision(),
        "mode": material.mode().as_str(),
        "propagation_policy": material.propagation_policy().as_str(),
        "reason": material.reason(),
        "session_id": target.session_id(),
        "target_attempt_id": target.target_attempt_id(),
        "target_fencing_generation": target.target_fencing_generation(),
        "target_request_digest_sha256": target.target_request_digest_sha256(),
    });
    stringify_target_pins(&mut value);
    let canonical = canonicalize_json_value_with_limits(&value, CanonicalJsonLimits::DEFAULT)
        .map_err(|_| RuntimeCancellationDtoError::CanonicalDigestFailure)?;
    let digest = Digest::sha256(&canonical).to_string();
    Ok(digest.strip_prefix("sha256:").unwrap_or(&digest).to_owned())
}

fn stringify_target_pins(value: &mut serde_json::Value) {
    for field in [
        "target_fencing_generation",
        "expected_session_revision",
        "expected_lifecycle_revision",
        "expected_provider_generation",
    ] {
        value[field] = json!(value[field].to_string());
    }
}

fn verify_command_digest(
    material: &RuntimeCancellationCommandDigestMaterial,
    supplied: &str,
) -> Result<(), RuntimeCancellationDtoError> {
    if command_digest(material)? == supplied {
        Ok(())
    } else {
        Err(RuntimeCancellationDtoError::CommandDigestMismatch)
    }
}

/// Exact mutating request for one attached runtime-attempt cancellation.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CancelRuntimeAttemptRequest {
    mutation: MutationContext,
    control_id: String,
    command_digest_sha256: String,
    target_attempt_id: String,
    target_request_digest_sha256: String,
    target_fencing_generation: u64,
    caller_cell_identity: String,
    session_id: String,
    expected_session_revision: u64,
    expected_lifecycle_revision: u64,
    expected_provider_id: String,
    expected_provider_generation: u64,
    mode: RuntimeCancellationModeDto,
    reason: String,
    propagation_policy: RuntimeCancellationPropagationPolicyDto,
    absolute_deadline_millis: i64,
    source_product: String,
    tenant_id: String,
    actor_context_ref: String,
    authority_ref: String,
}

impl fmt::Debug for CancelRuntimeAttemptRequest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CancelRuntimeAttemptRequest")
            .field("control_id", &self.control_id)
            .field("target_attempt_id", &self.target_attempt_id)
            .field("session_id", &self.session_id)
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

impl CancelRuntimeAttemptRequest {
    /// Constructs an exact current-0.1.0-beta request and its exact mutation metadata.
    pub fn attached(
        material: RuntimeCancellationCommandDigestMaterial,
        command_digest_sha256: String,
    ) -> Result<Self, RuntimeCancellationDtoError> {
        validate_digest(
            &command_digest_sha256,
            RuntimeCancellationDtoError::InvalidCommandDigest,
        )?;
        verify_command_digest(&material, &command_digest_sha256)?;
        let target = material.target;
        let mutation = MutationContext {
            idempotency_key: Some(material.idempotency_key.clone()),
            attempt: Some(OperationAttemptDto {
                attempt_id: material.control_id.clone(),
                fencing_token: target.target_fencing_generation,
                // Filled from the complete product request after construction.
                // Mutation metadata is excluded from that fingerprint.
                payload_digest_sha256: String::new(),
            }),
            contract: Some(ProductContractDto::current(
                ProductOperation::CancelRuntimeAttempt,
                ContractPayloadKind::Request,
            )),
            compatible_unknown: Default::default(),
        };
        let mut request = Self {
            mutation,
            control_id: material.control_id,
            command_digest_sha256,
            target_attempt_id: target.target_attempt_id,
            target_request_digest_sha256: target.target_request_digest_sha256,
            target_fencing_generation: target.target_fencing_generation,
            caller_cell_identity: target.caller_cell_identity,
            session_id: target.session_id,
            expected_session_revision: target.expected_session_revision,
            expected_lifecycle_revision: target.expected_lifecycle_revision,
            expected_provider_id: target.expected_provider_id,
            expected_provider_generation: target.expected_provider_generation,
            mode: material.mode,
            reason: material.reason,
            propagation_policy: material.propagation_policy,
            absolute_deadline_millis: material.absolute_deadline_millis,
            source_product: material.source_product,
            tenant_id: material.tenant_id,
            actor_context_ref: material.actor_context_ref,
            authority_ref: material.authority_ref,
        };
        let fingerprint = request.product_request_fingerprint_sha256()?;
        request
            .mutation
            .attempt
            .as_mut()
            .expect("attached cancellation always has attempt metadata")
            .payload_digest_sha256 = fingerprint;
        Ok(request)
    }

    /// Returns the exact mutation context carried on the wire.
    pub const fn mutation(&self) -> &MutationContext {
        &self.mutation
    }

    /// Returns the distinct cancellation-control identity.
    pub fn control_id(&self) -> &str {
        &self.control_id
    }

    /// Returns the canonical digest of all consequential command fields.
    pub fn command_digest_sha256(&self) -> &str {
        &self.command_digest_sha256
    }

    /// Returns the exact predecessor runtime-start attempt identity.
    pub fn target_attempt_id(&self) -> &str {
        &self.target_attempt_id
    }

    /// Returns the exact predecessor start-request digest.
    pub fn target_request_digest_sha256(&self) -> &str {
        &self.target_request_digest_sha256
    }

    /// Returns the exact predecessor dispatch fencing generation.
    pub const fn target_fencing_generation(&self) -> u64 {
        self.target_fencing_generation
    }

    /// Returns the caller cell whose fence owns the predecessor attempt.
    pub fn caller_cell_identity(&self) -> &str {
        &self.caller_cell_identity
    }

    /// Returns the exact target session identity.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Returns the expected target aggregate revision.
    pub const fn expected_session_revision(&self) -> u64 {
        self.expected_session_revision
    }

    /// Returns the expected target lifecycle revision.
    pub const fn expected_lifecycle_revision(&self) -> u64 {
        self.expected_lifecycle_revision
    }

    /// Returns the expected provider identity.
    pub fn expected_provider_id(&self) -> &str {
        &self.expected_provider_id
    }

    /// Returns the expected immutable provider generation.
    pub const fn expected_provider_generation(&self) -> u64 {
        self.expected_provider_generation
    }

    /// Returns the exact process-control mode.
    pub const fn mode(&self) -> RuntimeCancellationModeDto {
        self.mode
    }

    /// Returns the bounded cancellation reason.
    pub fn reason(&self) -> &str {
        &self.reason
    }

    /// Returns the target-only propagation policy.
    pub const fn propagation_policy(&self) -> RuntimeCancellationPropagationPolicyDto {
        self.propagation_policy
    }

    /// Returns the absolute command-admission deadline.
    pub const fn absolute_deadline_millis(&self) -> i64 {
        self.absolute_deadline_millis
    }

    /// Returns the authenticated source-product identity asserted by the caller adapter.
    pub fn source_product(&self) -> &str {
        &self.source_product
    }

    /// Returns the exact tenant scope.
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Returns the current actor-context reference.
    pub fn actor_context_ref(&self) -> &str {
        &self.actor_context_ref
    }

    /// Returns the current control-authority reference.
    pub fn authority_ref(&self) -> &str {
        &self.authority_ref
    }

    /// Returns a validated immutable target binding.
    pub fn target_binding(
        &self,
    ) -> Result<RuntimeCancellationTargetBindingDto, RuntimeCancellationDtoError> {
        RuntimeCancellationTargetBindingDto::new(
            self.target_attempt_id.clone(),
            self.target_request_digest_sha256.clone(),
            self.target_fencing_generation,
            self.caller_cell_identity.clone(),
            self.session_id.clone(),
            self.expected_session_revision,
            self.expected_lifecycle_revision,
            self.expected_provider_id.clone(),
            self.expected_provider_generation,
        )
    }

    /// Returns the semantic command plus separately protected envelope context.
    pub fn digest_material(
        &self,
    ) -> Result<RuntimeCancellationCommandDigestMaterial, RuntimeCancellationDtoError> {
        RuntimeCancellationCommandDigestMaterial::new(
            self.target_binding()?,
            self.control_id.clone(),
            self.mutation
                .idempotency_key
                .clone()
                .ok_or(RuntimeCancellationDtoError::InvalidMutationContext)?,
            self.mode,
            self.reason.clone(),
            self.propagation_policy,
            self.absolute_deadline_millis,
            self.source_product.clone(),
            self.tenant_id.clone(),
            self.actor_context_ref.clone(),
            self.authority_ref.clone(),
        )
    }

    /// Computes the internal canonical product-request fingerprint used by the
    /// durable attempt ledger. It is distinct from the caller command digest.
    pub fn product_request_fingerprint_sha256(
        &self,
    ) -> Result<String, RuntimeCancellationDtoError> {
        let digest = crate::canonical_product_request_digest(
            &crate::ProductRequest::CancelRuntimeAttempt(self.clone()),
        )
        .map_err(|_| RuntimeCancellationDtoError::CanonicalDigestFailure)?
        .to_string();
        Ok(digest.strip_prefix("sha256:").unwrap_or(&digest).to_owned())
    }

    pub(crate) fn fingerprint_material(
        &self,
        mut value: serde_json::Value,
    ) -> Result<serde_json::Value, RuntimeCancellationDtoError> {
        let material = self.digest_material()?;
        verify_command_digest(&material, &self.command_digest_sha256)?;
        // Bind every numeric request field exactly, including the deadline.
        stringify_target_pins(&mut value["input"]);
        value["input"]["absolute_deadline_millis"] =
            json!(self.absolute_deadline_millis.to_string());
        value["digest_domain"] = json!(EXACT_PRODUCT_DIGEST_DOMAIN);
        Ok(value)
    }

    /// Requires the sole current product request contract.
    pub fn validate_contract(&self) -> Result<(), crate::ProductContractError> {
        validate_mutation_contract(&self.mutation, ProductOperation::CancelRuntimeAttempt)
    }

    /// Validates all decoded fields, exact mutation bindings, and command digest.
    pub fn validate(&self) -> Result<(), RuntimeCancellationDtoError> {
        self.validate_contract()
            .map_err(|_| RuntimeCancellationDtoError::InvalidProductContract)?;
        if !self.mutation.compatible_unknown.is_empty() {
            return Err(RuntimeCancellationDtoError::InvalidMutationContext);
        }
        let material = self.digest_material()?;
        validate_digest(
            &self.command_digest_sha256,
            RuntimeCancellationDtoError::InvalidCommandDigest,
        )?;
        verify_command_digest(&material, &self.command_digest_sha256)?;
        let attempt = self
            .mutation
            .attempt
            .as_ref()
            .ok_or(RuntimeCancellationDtoError::InvalidMutationContext)?;
        if attempt.attempt_id != self.control_id
            || attempt.fencing_token != self.target_fencing_generation
            || attempt.payload_digest_sha256 != self.product_request_fingerprint_sha256()?
        {
            return Err(RuntimeCancellationDtoError::InvalidMutationContext);
        }
        Ok(())
    }
}

/// Read-only exact lookup for one durably admitted runtime cancellation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GetRuntimeCancellationStatusRequest {
    control_id: String,
    command_digest_sha256: String,
    target_attempt_id: String,
    target_request_digest_sha256: String,
    session_id: String,
    expected_provider_id: String,
    expected_provider_generation: u64,
    source_product: String,
    tenant_id: String,
    actor_context_ref: String,
    authority_ref: String,
    contract: ProductContractDto,
}

impl GetRuntimeCancellationStatusRequest {
    /// Constructs the sole current, exact cancellation-status lookup.
    #[allow(clippy::too_many_arguments)]
    pub fn attached(
        control_id: String,
        command_digest_sha256: String,
        target_attempt_id: String,
        target_request_digest_sha256: String,
        session_id: String,
        expected_provider_id: String,
        expected_provider_generation: u64,
        source_product: String,
        tenant_id: String,
        actor_context_ref: String,
        authority_ref: String,
    ) -> Result<Self, RuntimeCancellationDtoError> {
        let value = Self {
            control_id,
            command_digest_sha256,
            target_attempt_id,
            target_request_digest_sha256,
            session_id,
            expected_provider_id,
            expected_provider_generation,
            source_product,
            tenant_id,
            actor_context_ref,
            authority_ref,
            contract: ProductContractDto::current(
                ProductOperation::GetRuntimeCancellationStatus,
                ContractPayloadKind::Request,
            ),
        };
        value.validate()?;
        Ok(value)
    }

    /// Returns the exact cancellation-control identity.
    pub fn control_id(&self) -> &str {
        &self.control_id
    }

    /// Returns the exact canonical cancellation-command digest.
    pub fn command_digest_sha256(&self) -> &str {
        &self.command_digest_sha256
    }

    /// Returns the exact predecessor runtime-start attempt identity.
    pub fn target_attempt_id(&self) -> &str {
        &self.target_attempt_id
    }

    /// Returns the exact predecessor start-request digest.
    pub fn target_request_digest_sha256(&self) -> &str {
        &self.target_request_digest_sha256
    }

    /// Returns the exact session identity retained on the control.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Returns the exact predecessor provider identity.
    pub fn expected_provider_id(&self) -> &str {
        &self.expected_provider_id
    }

    /// Returns the exact predecessor provider generation.
    pub const fn expected_provider_generation(&self) -> u64 {
        self.expected_provider_generation
    }

    /// Returns the current authenticated source product asserted by the caller adapter.
    pub fn source_product(&self) -> &str {
        &self.source_product
    }

    /// Returns the current exact tenant scope.
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Returns the current actor-context reference.
    pub fn actor_context_ref(&self) -> &str {
        &self.actor_context_ref
    }

    /// Returns the current status-read authority reference.
    pub fn authority_ref(&self) -> &str {
        &self.authority_ref
    }

    /// Returns the sole current product-contract descriptor.
    pub const fn contract(&self) -> &ProductContractDto {
        &self.contract
    }

    /// Requires the sole current cancellation-status request contract.
    pub fn validate_contract(&self) -> Result<(), crate::ProductContractError> {
        validate_product_contract(
            &self.contract,
            ProductOperation::GetRuntimeCancellationStatus,
            ContractPayloadKind::Request,
        )
    }

    /// Validates a status lookup before authorization or retained-state access.
    pub fn validate(&self) -> Result<(), RuntimeCancellationDtoError> {
        self.validate_contract()
            .map_err(|_| RuntimeCancellationDtoError::InvalidProductContract)?;
        validate_safe_identity(
            &self.control_id,
            MAX_CONTROL_ID_BYTES,
            RuntimeCancellationDtoError::InvalidControlId,
        )?;
        validate_digest(
            &self.command_digest_sha256,
            RuntimeCancellationDtoError::InvalidCommandDigest,
        )?;
        validate_safe_identity(
            &self.target_attempt_id,
            MAX_ATTEMPT_ID_BYTES,
            RuntimeCancellationDtoError::InvalidTargetAttemptId,
        )?;
        if self.control_id == self.target_attempt_id {
            return Err(RuntimeCancellationDtoError::ControlTargetIdentityCollision);
        }
        validate_digest(
            &self.target_request_digest_sha256,
            RuntimeCancellationDtoError::InvalidTargetRequestDigest,
        )?;
        validate_session_id(
            &self.session_id,
            MAX_SESSION_ID_BYTES,
            RuntimeCancellationDtoError::InvalidSessionId,
        )?;
        validate_provider_id(&self.expected_provider_id)?;
        if self.expected_provider_generation == 0 {
            return Err(RuntimeCancellationDtoError::InvalidProviderGeneration);
        }
        validate_product_coordinate(
            &self.source_product,
            RuntimeCancellationDtoError::InvalidSourceProduct,
        )?;
        validate_tenant_id(&self.tenant_id)?;
        validate_bounded_plain(
            &self.actor_context_ref,
            MAX_CONTEXT_REFERENCE_BYTES,
            RuntimeCancellationDtoError::InvalidActorContextRef,
        )?;
        validate_bounded_plain(
            &self.authority_ref,
            MAX_CONTEXT_REFERENCE_BYTES,
            RuntimeCancellationDtoError::InvalidAuthorityRef,
        )
    }
}

/// Complete consequential cancellation binding echoed in every successful result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeCancellationControlBindingDto {
    control_id: String,
    command_digest_sha256: String,
    idempotency_key: String,
    mode: RuntimeCancellationModeDto,
    reason: String,
    propagation_policy: RuntimeCancellationPropagationPolicyDto,
    deadline_millis: i64,
    source_product: String,
    tenant_id: String,
    actor_context_ref: String,
    authority_ref: String,
}

impl RuntimeCancellationControlBindingDto {
    /// Constructs a complete validated retained control binding.
    pub fn new(
        material: &RuntimeCancellationCommandDigestMaterial,
        command_digest_sha256: String,
    ) -> Result<Self, RuntimeCancellationDtoError> {
        validate_digest(
            &command_digest_sha256,
            RuntimeCancellationDtoError::InvalidCommandDigest,
        )?;
        verify_command_digest(material, &command_digest_sha256)?;
        Ok(Self {
            control_id: material.control_id.clone(),
            command_digest_sha256,
            idempotency_key: material.idempotency_key.clone(),
            mode: material.mode,
            reason: material.reason.clone(),
            propagation_policy: material.propagation_policy,
            deadline_millis: material.absolute_deadline_millis,
            source_product: material.source_product.clone(),
            tenant_id: material.tenant_id.clone(),
            actor_context_ref: material.actor_context_ref.clone(),
            authority_ref: material.authority_ref.clone(),
        })
    }

    /// Returns the distinct cancellation-control identity.
    pub fn control_id(&self) -> &str {
        &self.control_id
    }

    /// Returns the exact canonical cancellation-command digest.
    pub fn command_digest_sha256(&self) -> &str {
        &self.command_digest_sha256
    }

    /// Returns the exact enclosing idempotency key.
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    /// Returns the independently authorized process-control mode.
    pub const fn mode(&self) -> RuntimeCancellationModeDto {
        self.mode
    }

    /// Returns the bounded retained reason.
    pub fn reason(&self) -> &str {
        &self.reason
    }

    /// Returns the target-only propagation policy.
    pub const fn propagation_policy(&self) -> RuntimeCancellationPropagationPolicyDto {
        self.propagation_policy
    }

    /// Returns the original absolute command-admission deadline.
    pub const fn deadline_millis(&self) -> i64 {
        self.deadline_millis
    }

    /// Returns the original authenticated source product.
    pub fn source_product(&self) -> &str {
        &self.source_product
    }

    /// Returns the original exact tenant scope.
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Returns the original actor-context reference.
    pub fn actor_context_ref(&self) -> &str {
        &self.actor_context_ref
    }

    /// Returns the original control-authority reference.
    pub fn authority_ref(&self) -> &str {
        &self.authority_ref
    }

    /// Validates a retained control binding decoded through a generic codec.
    pub fn validate(&self) -> Result<(), RuntimeCancellationDtoError> {
        validate_safe_identity(
            &self.control_id,
            MAX_CONTROL_ID_BYTES,
            RuntimeCancellationDtoError::InvalidControlId,
        )?;
        validate_digest(
            &self.command_digest_sha256,
            RuntimeCancellationDtoError::InvalidCommandDigest,
        )?;
        validate_idempotency_key(
            &self.idempotency_key,
            MAX_IDEMPOTENCY_KEY_BYTES,
            RuntimeCancellationDtoError::InvalidIdempotencyKey,
        )?;
        validate_bounded_plain(
            &self.reason,
            MAX_REASON_BYTES,
            RuntimeCancellationDtoError::InvalidReason,
        )?;
        if self.deadline_millis <= 0 {
            return Err(RuntimeCancellationDtoError::InvalidAbsoluteDeadline);
        }
        validate_product_coordinate(
            &self.source_product,
            RuntimeCancellationDtoError::InvalidSourceProduct,
        )?;
        validate_tenant_id(&self.tenant_id)?;
        validate_bounded_plain(
            &self.actor_context_ref,
            MAX_CONTEXT_REFERENCE_BYTES,
            RuntimeCancellationDtoError::InvalidActorContextRef,
        )?;
        validate_bounded_plain(
            &self.authority_ref,
            MAX_CONTEXT_REFERENCE_BYTES,
            RuntimeCancellationDtoError::InvalidAuthorityRef,
        )
    }
}

/// Exact durable session projection attached to each cancellation state.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeCancellationSessionSnapshotDto {
    session_id: String,
    lifecycle: LifecycleStateDto,
    revision: u64,
    lifecycle_revision: u64,
    observed_at_millis: i64,
}

impl RuntimeCancellationSessionSnapshotDto {
    /// Constructs a complete durable session snapshot.
    pub fn new(
        session_id: String,
        lifecycle: LifecycleStateDto,
        revision: u64,
        lifecycle_revision: u64,
        observed_at_millis: i64,
    ) -> Result<Self, RuntimeCancellationDtoError> {
        validate_session_id(
            &session_id,
            MAX_SESSION_ID_BYTES,
            RuntimeCancellationDtoError::InvalidSessionId,
        )?;
        if matches!(lifecycle, LifecycleStateDto::Exited { code: None }) {
            return Err(RuntimeCancellationDtoError::MissingExitCode);
        }
        if revision == 0 || lifecycle_revision == 0 || lifecycle_revision > revision {
            return Err(RuntimeCancellationDtoError::InvalidSessionRevision);
        }
        Ok(Self {
            session_id,
            lifecycle,
            revision,
            lifecycle_revision,
            observed_at_millis,
        })
    }

    /// Returns the exact target session identity.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Returns the exact authoritative durable lifecycle.
    pub const fn lifecycle(&self) -> LifecycleStateDto {
        self.lifecycle
    }

    /// Returns the exact aggregate revision.
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns the exact lifecycle revision.
    pub const fn lifecycle_revision(&self) -> u64 {
        self.lifecycle_revision
    }

    /// Returns the durable observation time represented by the snapshot.
    pub const fn observed_at_millis(&self) -> i64 {
        self.observed_at_millis
    }

    /// Validates a snapshot decoded through a generic codec.
    pub fn validate(&self) -> Result<(), RuntimeCancellationDtoError> {
        Self::new(
            self.session_id.clone(),
            self.lifecycle,
            self.revision,
            self.lifecycle_revision,
            self.observed_at_millis,
        )
        .map(|_| ())
    }
}

/// Provider-owned durable state for one exact cancellation command.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum RuntimeCancellationStateDto {
    /// The command and stopping state are durable; process outcome is pending.
    AcceptedStopping {
        /// Exact durable session snapshot.
        session: RuntimeCancellationSessionSnapshotDto,
    },
    /// Process termination was applied and the terminal session state is durable.
    AppliedTerminated {
        /// Exact durable session snapshot.
        session: RuntimeCancellationSessionSnapshotDto,
    },
    /// The exact session was already terminal when control was admitted.
    AlreadyTerminal {
        /// Exact durable session snapshot.
        session: RuntimeCancellationSessionSnapshotDto,
    },
    /// The command was admitted but retained evidence cannot prove its effect.
    Unknown {
        /// Exact durable session snapshot.
        session: RuntimeCancellationSessionSnapshotDto,
    },
}

impl RuntimeCancellationStateDto {
    /// Returns the exact session snapshot carried by every state.
    pub const fn session(&self) -> &RuntimeCancellationSessionSnapshotDto {
        match self {
            Self::AcceptedStopping { session }
            | Self::AppliedTerminated { session }
            | Self::AlreadyTerminal { session }
            | Self::Unknown { session } => session,
        }
    }

    /// Validates lifecycle/state coherence without inventing an attempt outcome.
    pub fn validate(&self) -> Result<(), RuntimeCancellationDtoError> {
        let session = self.session();
        session.validate()?;
        match self {
            Self::AcceptedStopping { .. } if session.lifecycle() != LifecycleStateDto::Stopping => {
                Err(RuntimeCancellationDtoError::InvalidCancellationState)
            }
            Self::AppliedTerminated { .. } | Self::AlreadyTerminal { .. }
                if !is_terminal(session.lifecycle()) =>
            {
                Err(RuntimeCancellationDtoError::InvalidCancellationState)
            }
            _ => Ok(()),
        }
    }
}

/// Successful exact result for a runtime cancellation command.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CancelRuntimeAttemptResultDto {
    target: RuntimeCancellationTargetBindingDto,
    control: RuntimeCancellationControlBindingDto,
    state: RuntimeCancellationStateDto,
}

impl CancelRuntimeAttemptResultDto {
    /// Constructs a complete, internally coherent cancellation result.
    pub fn new(
        target: RuntimeCancellationTargetBindingDto,
        control: RuntimeCancellationControlBindingDto,
        state: RuntimeCancellationStateDto,
    ) -> Result<Self, RuntimeCancellationDtoError> {
        validate_result(&target, &control, &state)?;
        Ok(Self {
            target,
            control,
            state,
        })
    }

    /// Returns the exact predecessor target binding.
    pub const fn target(&self) -> &RuntimeCancellationTargetBindingDto {
        &self.target
    }

    /// Returns the complete retained control binding.
    pub const fn control(&self) -> &RuntimeCancellationControlBindingDto {
        &self.control
    }

    /// Returns the discriminated durable provider state.
    pub const fn state(&self) -> &RuntimeCancellationStateDto {
        &self.state
    }

    /// Validates a result decoded through a generic codec.
    pub fn validate(&self) -> Result<(), RuntimeCancellationDtoError> {
        validate_result(&self.target, &self.control, &self.state)
    }
}

/// Successful read-only result for an exact runtime cancellation-status lookup.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeCancellationStatusResultDto {
    target: RuntimeCancellationTargetBindingDto,
    control: RuntimeCancellationControlBindingDto,
    state: RuntimeCancellationStateDto,
}

impl RuntimeCancellationStatusResultDto {
    /// Constructs a complete, internally coherent status result.
    pub fn new(
        target: RuntimeCancellationTargetBindingDto,
        control: RuntimeCancellationControlBindingDto,
        state: RuntimeCancellationStateDto,
    ) -> Result<Self, RuntimeCancellationDtoError> {
        validate_result(&target, &control, &state)?;
        Ok(Self {
            target,
            control,
            state,
        })
    }

    /// Returns the exact predecessor target binding.
    pub const fn target(&self) -> &RuntimeCancellationTargetBindingDto {
        &self.target
    }

    /// Returns the complete retained control binding.
    pub const fn control(&self) -> &RuntimeCancellationControlBindingDto {
        &self.control
    }

    /// Returns the discriminated durable provider state.
    pub const fn state(&self) -> &RuntimeCancellationStateDto {
        &self.state
    }

    /// Validates a result decoded through a generic codec.
    pub fn validate(&self) -> Result<(), RuntimeCancellationDtoError> {
        validate_result(&self.target, &self.control, &self.state)
    }
}

/// Stable validation failures for the runtime-cancellation contracts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeCancellationDtoError {
    /// Product contract metadata is not the exact current contract.
    InvalidProductContract,
    /// Control identity is empty, malformed, or over its bound.
    InvalidControlId,
    /// Control and target attempt identities are not distinct.
    ControlTargetIdentityCollision,
    /// Command digest is not canonical lowercase SHA-256.
    InvalidCommandDigest,
    /// Command digest does not match the consequential fields.
    CommandDigestMismatch,
    /// Canonical command material could not be encoded.
    CanonicalDigestFailure,
    /// Target attempt identity is empty, malformed, or over its bound.
    InvalidTargetAttemptId,
    /// Target request digest is not canonical lowercase SHA-256.
    InvalidTargetRequestDigest,
    /// Target fencing generation is zero.
    InvalidTargetFencingGeneration,
    /// Caller cell identity is invalid.
    InvalidCallerCellIdentity,
    /// Target session identity is invalid.
    InvalidSessionId,
    /// Expected session/lifecycle revisions are invalid.
    InvalidExpectedRevision,
    /// Provider identity is invalid.
    InvalidProviderId,
    /// Provider generation is zero.
    InvalidProviderGeneration,
    /// Idempotency key is invalid.
    InvalidIdempotencyKey,
    /// Cancellation reason is invalid.
    InvalidReason,
    /// Absolute admission deadline is invalid.
    InvalidAbsoluteDeadline,
    /// Source-product identity is invalid.
    InvalidSourceProduct,
    /// Tenant identity is invalid.
    InvalidTenantId,
    /// Actor-context reference is invalid.
    InvalidActorContextRef,
    /// Authority reference is invalid.
    InvalidAuthorityRef,
    /// Mutation metadata is absent or not bound exactly to the control.
    InvalidMutationContext,
    /// Durable session revisions are invalid.
    InvalidSessionRevision,
    /// Exited lifecycle omitted its exact exit code.
    MissingExitCode,
    /// Cancellation state and durable session lifecycle conflict.
    InvalidCancellationState,
    /// Result target/session bindings conflict.
    InconsistentResultBinding,
}

impl Display for RuntimeCancellationDtoError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidProductContract => "invalid runtime-cancellation product contract",
            Self::InvalidControlId => "invalid runtime-cancellation control identifier",
            Self::ControlTargetIdentityCollision => {
                "runtime-cancellation control and target identities must be distinct"
            }
            Self::InvalidCommandDigest => "invalid canonical cancellation-command digest",
            Self::CommandDigestMismatch => {
                "cancellation-command digest does not match consequential fields"
            }
            Self::CanonicalDigestFailure => {
                "runtime-cancellation command material could not be canonicalized"
            }
            Self::InvalidTargetAttemptId => "invalid target runtime-attempt identifier",
            Self::InvalidTargetRequestDigest => "invalid target runtime-start request digest",
            Self::InvalidTargetFencingGeneration => {
                "target runtime-attempt fencing generation must be non-zero"
            }
            Self::InvalidCallerCellIdentity => "invalid caller cell identity",
            Self::InvalidSessionId => "invalid runtime session identifier",
            Self::InvalidExpectedRevision => "invalid expected session/lifecycle revisions",
            Self::InvalidProviderId => "invalid expected provider identity",
            Self::InvalidProviderGeneration => "expected provider generation must be non-zero",
            Self::InvalidIdempotencyKey => "invalid runtime-cancellation idempotency key",
            Self::InvalidReason => "invalid runtime-cancellation reason",
            Self::InvalidAbsoluteDeadline => {
                "runtime-cancellation absolute deadline must be positive"
            }
            Self::InvalidSourceProduct => "invalid runtime-cancellation source product",
            Self::InvalidTenantId => "invalid runtime-cancellation tenant identity",
            Self::InvalidActorContextRef => "invalid runtime-cancellation actor-context reference",
            Self::InvalidAuthorityRef => "invalid runtime-cancellation authority reference",
            Self::InvalidMutationContext => "runtime-cancellation mutation metadata is not exact",
            Self::InvalidSessionRevision => "invalid runtime-cancellation session revisions",
            Self::MissingExitCode => "exited runtime session requires an exact exit code",
            Self::InvalidCancellationState => {
                "runtime-cancellation state conflicts with the session lifecycle"
            }
            Self::InconsistentResultBinding => {
                "runtime-cancellation result bindings are inconsistent"
            }
        })
    }
}

impl Error for RuntimeCancellationDtoError {}

fn validate_result(
    target: &RuntimeCancellationTargetBindingDto,
    control: &RuntimeCancellationControlBindingDto,
    state: &RuntimeCancellationStateDto,
) -> Result<(), RuntimeCancellationDtoError> {
    target.validate()?;
    control.validate()?;
    state.validate()?;
    let session = state.session();
    if control.control_id == target.target_attempt_id
        || session.session_id != target.session_id
        || session.revision < target.expected_session_revision
        || session.lifecycle_revision < target.expected_lifecycle_revision
    {
        return Err(RuntimeCancellationDtoError::InconsistentResultBinding);
    }
    // A control binding alone has no target with which to verify its digest.
    // Recombine them here to check that the echoed command digest covers this
    // target as well as the control fields. See [result validation](../../../docs/reference/protocol/cancellation.md#interpret-the-result).
    let material = RuntimeCancellationCommandDigestMaterial::new(
        target.clone(),
        control.control_id.clone(),
        control.idempotency_key.clone(),
        control.mode,
        control.reason.clone(),
        control.propagation_policy,
        control.deadline_millis,
        control.source_product.clone(),
        control.tenant_id.clone(),
        control.actor_context_ref.clone(),
        control.authority_ref.clone(),
    )?;
    verify_command_digest(&material, &control.command_digest_sha256)
        .map_err(|_| RuntimeCancellationDtoError::InconsistentResultBinding)?;
    Ok(())
}

const fn is_terminal(lifecycle: LifecycleStateDto) -> bool {
    matches!(
        lifecycle,
        LifecycleStateDto::Stopped
            | LifecycleStateDto::Exited { code: Some(_) }
            | LifecycleStateDto::Failed
    )
}

fn validate_digest(
    value: &str,
    error: RuntimeCancellationDtoError,
) -> Result<(), RuntimeCancellationDtoError> {
    if value.len() != SHA256_HEX_CHARS
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(error);
    }
    Ok(())
}

fn validate_safe_identity(
    value: &str,
    maximum: usize,
    error: RuntimeCancellationDtoError,
) -> Result<(), RuntimeCancellationDtoError> {
    if value.is_empty()
        || value.len() > maximum
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
    {
        return Err(error);
    }
    Ok(())
}

fn validate_session_id(
    value: &str,
    maximum: usize,
    error: RuntimeCancellationDtoError,
) -> Result<(), RuntimeCancellationDtoError> {
    if value.is_empty()
        || value.len() > maximum
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(error);
    }
    Ok(())
}

fn validate_product_coordinate(
    value: &str,
    error: RuntimeCancellationDtoError,
) -> Result<(), RuntimeCancellationDtoError> {
    if value.len() > MAX_SOURCE_PRODUCT_BYTES {
        return Err(error);
    }
    if value.split('/').enumerate().any(|(index, segment)| {
        index >= 2
            || !(1..=64).contains(&segment.len())
            || !segment
                .bytes()
                .next()
                .is_some_and(|byte| byte.is_ascii_lowercase())
            || !segment
                .bytes()
                .last()
                .is_some_and(|byte| byte.is_ascii_alphanumeric())
            || !segment
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    }) {
        return Err(error);
    }
    Ok(())
}

fn validate_bounded_plain(
    value: &str,
    maximum: usize,
    error: RuntimeCancellationDtoError,
) -> Result<(), RuntimeCancellationDtoError> {
    if value.trim().is_empty() || value.len() > maximum || value.chars().any(char::is_control) {
        return Err(error);
    }
    Ok(())
}

fn validate_idempotency_key(
    value: &str,
    maximum: usize,
    error: RuntimeCancellationDtoError,
) -> Result<(), RuntimeCancellationDtoError> {
    if value.is_empty()
        || value.len() > maximum
        || !value.is_ascii()
        || value.chars().any(char::is_control)
    {
        return Err(error);
    }
    Ok(())
}

fn validate_provider_id(value: &str) -> Result<(), RuntimeCancellationDtoError> {
    if !has_prefixed_uuid_v7(value, "prv") || !has_standard_uuid_variant(value) {
        return Err(RuntimeCancellationDtoError::InvalidProviderId);
    }
    Ok(())
}

fn validate_tenant_id(value: &str) -> Result<(), RuntimeCancellationDtoError> {
    if !has_prefixed_uuid_v7(value, "tenant") || !has_standard_uuid_variant(value) {
        return Err(RuntimeCancellationDtoError::InvalidTenantId);
    }
    Ok(())
}

// Provider and tenant identities are protocol-owned, prefixed UUIDv7 values.
// Preserve the original strict lowercase, hyphenated representation.
fn has_prefixed_uuid_v7(value: &str, prefix: &str) -> bool {
    let Some(uuid_text) = value
        .strip_prefix(prefix)
        .and_then(|rest| rest.strip_prefix('_'))
    else {
        return false;
    };
    if !value.is_ascii()
        || value.bytes().any(|byte| byte.is_ascii_whitespace())
        || uuid_text.len() != 36
        || uuid_text.bytes().any(|byte| byte.is_ascii_uppercase())
    {
        return false;
    }
    uuid::Uuid::parse_str(uuid_text).is_ok_and(|uuid| {
        uuid.hyphenated().to_string() == uuid_text
            && uuid.get_version() == Some(uuid::Version::SortRand)
    })
}

fn has_standard_uuid_variant(value: &str) -> bool {
    value
        .rsplit_once('_')
        .and_then(|(_, uuid)| uuid.as_bytes().get(19))
        .is_some_and(|nibble| matches!(nibble, b'8' | b'9' | b'a' | b'b'))
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;

    const TENANT_ID: &str = "tenant_0198a5bd-2afd-7dd1-b6a3-1fb90c6f0a21";

    fn target() -> RuntimeCancellationTargetBindingDto {
        RuntimeCancellationTargetBindingDto::new(
            "attempt-start-1".to_owned(),
            "1".repeat(64),
            7,
            "cell-1".to_owned(),
            "session-1".to_owned(),
            11,
            9,
            "prv_0198a5bd-2b01-7ae9-9a0c-42532d56e7f7".to_owned(),
            3,
        )
        .expect("target")
    }

    fn target_with_session(
        session_id: String,
    ) -> Result<RuntimeCancellationTargetBindingDto, RuntimeCancellationDtoError> {
        RuntimeCancellationTargetBindingDto::new(
            "attempt-start-1".to_owned(),
            "1".repeat(64),
            7,
            "cell-1".to_owned(),
            session_id,
            11,
            9,
            "prv_0198a5bd-2b01-7ae9-9a0c-42532d56e7f7".to_owned(),
            3,
        )
    }

    fn material(mode: RuntimeCancellationModeDto) -> RuntimeCancellationCommandDigestMaterial {
        material_with_idempotency(mode, "idem-1".to_owned()).expect("material")
    }

    fn material_with_idempotency(
        mode: RuntimeCancellationModeDto,
        idempotency_key: String,
    ) -> Result<RuntimeCancellationCommandDigestMaterial, RuntimeCancellationDtoError> {
        RuntimeCancellationCommandDigestMaterial::new(
            target(),
            "control-1".to_owned(),
            idempotency_key,
            mode,
            "operator requested stop".to_owned(),
            RuntimeCancellationPropagationPolicyDto::TargetOnly,
            2_000_000_000_000,
            "example/workflow".to_owned(),
            TENANT_ID.to_owned(),
            "actor://current".to_owned(),
            "authority://cancel/1".to_owned(),
        )
    }

    fn material_with_text_fields(
        reason: String,
        actor_context_ref: String,
        authority_ref: String,
    ) -> Result<RuntimeCancellationCommandDigestMaterial, RuntimeCancellationDtoError> {
        RuntimeCancellationCommandDigestMaterial::new(
            target(),
            "control-1".to_owned(),
            "idem-1".to_owned(),
            RuntimeCancellationModeDto::Graceful,
            reason,
            RuntimeCancellationPropagationPolicyDto::TargetOnly,
            2_000_000_000_000,
            "example/workflow".to_owned(),
            TENANT_ID.to_owned(),
            actor_context_ref,
            authority_ref,
        )
    }

    #[test]
    fn source_products_are_bounded_without_a_company_scope_registry() {
        for value in ["workflow", "tools/workflow", "custom-scope/client-1"] {
            assert!(
                validate_product_coordinate(
                    value,
                    RuntimeCancellationDtoError::InvalidSourceProduct
                )
                .is_ok(),
                "{value}"
            );
        }
        for value in [
            "",
            "/workflow",
            "tools/",
            "tools/workflow/extra",
            "Tools/workflow",
            "tools/work_flow",
            "1client",
            "client-",
            "é",
            "tools/client\n",
        ] {
            assert!(
                validate_product_coordinate(
                    value,
                    RuntimeCancellationDtoError::InvalidSourceProduct
                )
                .is_err(),
                "{value:?}"
            );
        }
        let long = "a".repeat(65);
        let value = long.as_str();
        assert!(
            validate_product_coordinate(value, RuntimeCancellationDtoError::InvalidSourceProduct)
                .is_err()
        );
    }

    #[test]
    fn adjacent_full_width_pins_have_distinct_digests_and_reject_tampering() {
        for field in [
            "target_fencing_generation",
            "expected_session_revision",
            "expected_lifecycle_revision",
            "expected_provider_generation",
        ] {
            for (left, right) in [
                (9_007_199_254_740_992u64, 9_007_199_254_740_993u64),
                (u64::MAX - 1, u64::MAX),
            ] {
                let build = |value| {
                    let mut material = material(RuntimeCancellationModeDto::Graceful);
                    let mut target = serde_json::to_value(&material.target).unwrap();
                    if field == "expected_lifecycle_revision" {
                        target["expected_session_revision"] = json!(u64::MAX);
                    }
                    target[field] = json!(value);
                    material.target = serde_json::from_value(target).unwrap();
                    material.target.validate().unwrap();
                    material
                };
                let a = build(left);
                let b = build(right);
                let first = a.canonical_digest_sha256().unwrap();
                let second = b.canonical_digest_sha256().unwrap();
                assert_ne!(first, second, "{field}: {left} and {right}");
                let request = CancelRuntimeAttemptRequest::attached(a, first).unwrap();
                let mut changed = serde_json::to_value(&request).unwrap();
                changed[field] = json!(right);
                let changed: CancelRuntimeAttemptRequest = serde_json::from_value(changed).unwrap();
                assert!(
                    changed.validate().is_err(),
                    "changed pin must not reuse the command digest"
                );
                let other = CancelRuntimeAttemptRequest::attached(b, second).unwrap();
                assert_ne!(
                    request.product_request_fingerprint_sha256().unwrap(),
                    other.product_request_fingerprint_sha256().unwrap()
                );
            }
        }
    }

    #[test]
    fn product_fingerprint_preserves_full_width_deadlines() {
        let mut first = material(RuntimeCancellationModeDto::Graceful);
        let mut second = first.clone();
        first.absolute_deadline_millis = 9_007_199_254_740_992;
        second.absolute_deadline_millis = 9_007_199_254_740_993;
        let digest = first.canonical_digest_sha256().unwrap();
        assert_eq!(
            digest,
            second.canonical_digest_sha256().unwrap(),
            "deadline is envelope material"
        );
        let a = CancelRuntimeAttemptRequest::attached(first, digest.clone()).unwrap();
        let b = CancelRuntimeAttemptRequest::attached(second, digest).unwrap();
        assert_ne!(
            a.product_request_fingerprint_sha256().unwrap(),
            b.product_request_fingerprint_sha256().unwrap()
        );
    }

    #[test]
    fn exact_request_binds_mutation_and_recomputes_digest() {
        let material = material(RuntimeCancellationModeDto::Graceful);
        let digest = material.canonical_digest_sha256().expect("digest");
        let request =
            CancelRuntimeAttemptRequest::attached(material, digest.clone()).expect("request");

        request.validate().expect("valid request");
        assert_eq!(request.command_digest_sha256(), digest);
        assert_eq!(
            digest,
            "d696e602e1194d973a1fddbfe01f8a8f241f0fb0b9c1a0a0bd60525d83aa780e"
        );
        let attempt = request.mutation().attempt.as_ref().expect("attempt");
        assert_eq!(attempt.attempt_id, "control-1");
        assert_eq!(attempt.fencing_token, 7);
        assert_eq!(
            attempt.payload_digest_sha256,
            request
                .product_request_fingerprint_sha256()
                .expect("fingerprint")
        );
        assert_ne!(attempt.payload_digest_sha256, digest);
        assert!(request.mutation().compatible_unknown.is_empty());
    }

    #[test]
    fn cancellation_session_identity_matches_the_domain_contract() {
        assert!(target_with_session("session/é".to_owned()).is_ok());
        assert!(target_with_session("s".repeat(MAX_SESSION_ID_BYTES)).is_ok());
        assert!(target_with_session("é".repeat(MAX_SESSION_ID_BYTES / 2)).is_ok());
        assert_eq!(
            target_with_session("s".repeat(MAX_SESSION_ID_BYTES + 1)),
            Err(RuntimeCancellationDtoError::InvalidSessionId)
        );
        assert_eq!(
            target_with_session("é".repeat(MAX_SESSION_ID_BYTES / 2 + 1)),
            Err(RuntimeCancellationDtoError::InvalidSessionId)
        );
        for invalid in ["session with space", "session\nforged"] {
            assert_eq!(
                target_with_session(invalid.to_owned()),
                Err(RuntimeCancellationDtoError::InvalidSessionId)
            );
        }
    }

    #[test]
    fn cancellation_idempotency_matches_the_foundation_envelope_contract() {
        assert!(
            material_with_idempotency(
                RuntimeCancellationModeDto::Graceful,
                "k".repeat(MAX_IDEMPOTENCY_KEY_BYTES),
            )
            .is_ok()
        );
        assert!(
            material_with_idempotency(RuntimeCancellationModeDto::Graceful, "   ".to_owned())
                .is_ok()
        );
        for invalid in [
            "k".repeat(MAX_IDEMPOTENCY_KEY_BYTES + 1),
            "idém".to_owned(),
            "bad\nkey".to_owned(),
        ] {
            assert_eq!(
                material_with_idempotency(RuntimeCancellationModeDto::Graceful, invalid),
                Err(RuntimeCancellationDtoError::InvalidIdempotencyKey)
            );
        }
    }

    #[test]
    fn cancellation_request_status_and_results_are_file_backed_and_strict() {
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/runtime_cancellation.json"))
                .expect("runtime cancellation fixture JSON");
        let request: CancelRuntimeAttemptRequest =
            serde_json::from_value(fixture["request"].clone()).expect("cancel request");
        request.validate().expect("valid cancel request");
        assert_eq!(
            request.command_digest_sha256(),
            "d696e602e1194d973a1fddbfe01f8a8f241f0fb0b9c1a0a0bd60525d83aa780e"
        );
        assert_eq!(
            request
                .mutation()
                .attempt
                .as_ref()
                .expect("cancellation attempt")
                .payload_digest_sha256,
            "1e92a02d2efdc29741e85a0f2c2110a3b296626c753468f8c2df47af2e14215a"
        );

        let status_request: GetRuntimeCancellationStatusRequest =
            serde_json::from_value(fixture["status_request"].clone()).expect("status request");
        status_request.validate().expect("valid status request");
        let cancel_result: CancelRuntimeAttemptResultDto =
            serde_json::from_value(fixture["cancel_result"].clone()).expect("cancel result");
        cancel_result.validate().expect("valid cancel result");
        let status_result: RuntimeCancellationStatusResultDto =
            serde_json::from_value(fixture["status_result"].clone()).expect("status result");
        status_result.validate().expect("valid status result");

        let mut unknown = fixture["request"].clone();
        unknown["unknown"] = json!(true);
        assert!(serde_json::from_value::<CancelRuntimeAttemptRequest>(unknown).is_err());
    }

    #[test]
    fn decoded_results_reject_revision_regression_below_the_target_pins() {
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/runtime_cancellation.json"))
                .expect("runtime cancellation fixture JSON");

        let mut cancel = fixture["cancel_result"].clone();
        cancel["state"]["session"]["revision"] = json!(10);
        cancel["state"]["session"]["lifecycle_revision"] = json!(9);
        let cancel: CancelRuntimeAttemptResultDto =
            serde_json::from_value(cancel).expect("strict shape remains decodable");
        assert_eq!(
            cancel.validate(),
            Err(RuntimeCancellationDtoError::InconsistentResultBinding)
        );

        let mut status = fixture["status_result"].clone();
        status["state"]["session"]["revision"] = json!(11);
        status["state"]["session"]["lifecycle_revision"] = json!(8);
        let status: RuntimeCancellationStatusResultDto =
            serde_json::from_value(status).expect("strict shape remains decodable");
        assert_eq!(
            status.validate(),
            Err(RuntimeCancellationDtoError::InconsistentResultBinding)
        );
    }

    #[test]
    fn current_request_and_retained_results_match_independent_fixture() {
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/runtime_cancellation.json"))
                .unwrap();
        let request: CancelRuntimeAttemptRequest =
            serde_json::from_value(fixture["request"].clone()).unwrap();
        request.validate().unwrap();
        let material = request.digest_material().unwrap();
        assert_eq!(
            material.canonical_digest_sha256().unwrap(),
            request.command_digest_sha256()
        );
        assert_eq!(
            CancelRuntimeAttemptRequest::attached(
                material,
                request.command_digest_sha256().to_owned()
            )
            .unwrap(),
            request
        );
        let result: CancelRuntimeAttemptResultDto =
            serde_json::from_value(fixture["cancel_result"].clone()).unwrap();
        result.validate().unwrap();
        let status: RuntimeCancellationStatusResultDto =
            serde_json::from_value(fixture["status_result"].clone()).unwrap();
        status.validate().unwrap();
    }

    #[test]
    fn retired_digest_domains_are_rejected_even_for_small_integer_pins() {
        // Historical fixture hashes from the retired command domains, with the
        // same semantic command as material(). They must not authorize replay.
        for retired in [
            "c958dbf3accd7b1550f4a0f4826173223b6440173a1abda3b3f5ca6aed586906",
            "bbda0295ef23db01482cf0bfe0dd1549908307429d502fcdbecd5b418107cec3",
        ] {
            let material = material(RuntimeCancellationModeDto::Graceful);
            assert_eq!(
                CancelRuntimeAttemptRequest::attached(material.clone(), retired.to_owned())
                    .unwrap_err(),
                RuntimeCancellationDtoError::CommandDigestMismatch
            );
            assert!(
                RuntimeCancellationControlBindingDto::new(&material, retired.to_owned()).is_err()
            );
            let mut fixture: Value =
                serde_json::from_str(include_str!("../tests/fixtures/runtime_cancellation.json"))
                    .unwrap();
            fixture["status_result"]["control"]["command_digest_sha256"] = json!(retired);
            let result: RuntimeCancellationStatusResultDto =
                serde_json::from_value(fixture["status_result"].clone()).unwrap();
            assert_eq!(
                result.validate(),
                Err(RuntimeCancellationDtoError::InconsistentResultBinding)
            );
        }
    }

    #[test]
    fn force_escalation_has_a_distinct_digest() {
        let graceful = material(RuntimeCancellationModeDto::Graceful)
            .canonical_digest_sha256()
            .expect("graceful digest");
        let force = material(RuntimeCancellationModeDto::Force)
            .canonical_digest_sha256()
            .expect("force digest");

        assert_ne!(graceful, force);
    }

    #[test]
    fn decoded_request_rejects_changed_consequential_field() {
        let material = material(RuntimeCancellationModeDto::Graceful);
        let digest = material.canonical_digest_sha256().expect("digest");
        let request = CancelRuntimeAttemptRequest::attached(material, digest).expect("request");
        let mut value = serde_json::to_value(request).expect("JSON");
        value["reason"] = json!("changed");
        let changed: CancelRuntimeAttemptRequest =
            serde_json::from_value(value).expect("shape remains valid");

        assert_eq!(
            changed.validate(),
            Err(RuntimeCancellationDtoError::CommandDigestMismatch)
        );
    }

    #[test]
    fn control_identity_must_differ_from_target() {
        assert_eq!(
            RuntimeCancellationCommandDigestMaterial::new(
                target(),
                "attempt-start-1".to_owned(),
                "idem-1".to_owned(),
                RuntimeCancellationModeDto::Graceful,
                "reason".to_owned(),
                RuntimeCancellationPropagationPolicyDto::TargetOnly,
                2_000_000_000_000,
                "example/workflow".to_owned(),
                TENANT_ID.to_owned(),
                "actor://current".to_owned(),
                "authority://cancel/1".to_owned(),
            ),
            Err(RuntimeCancellationDtoError::ControlTargetIdentityCollision)
        );
    }

    #[test]
    fn malformed_source_namespace_is_rejected() {
        assert_eq!(
            RuntimeCancellationCommandDigestMaterial::new(
                target(),
                "control-1".to_owned(),
                "idem-1".to_owned(),
                RuntimeCancellationModeDto::Graceful,
                "reason".to_owned(),
                RuntimeCancellationPropagationPolicyDto::TargetOnly,
                2_000_000_000_000,
                "INVALID/workflow".to_owned(),
                TENANT_ID.to_owned(),
                "actor://current".to_owned(),
                "authority://cancel/1".to_owned(),
            ),
            Err(RuntimeCancellationDtoError::InvalidSourceProduct)
        );

        assert_eq!(
            RuntimeCancellationCommandDigestMaterial::new(
                target(),
                "control-1".to_owned(),
                "idem-1".to_owned(),
                RuntimeCancellationModeDto::Graceful,
                "reason".to_owned(),
                RuntimeCancellationPropagationPolicyDto::TargetOnly,
                2_000_000_000_000,
                "example/1workflow".to_owned(),
                TENANT_ID.to_owned(),
                "actor://current".to_owned(),
                "authority://cancel/1".to_owned(),
            ),
            Err(RuntimeCancellationDtoError::InvalidSourceProduct)
        );
    }

    #[test]
    fn current_peer_scope_source_is_accepted() {
        assert!(
            RuntimeCancellationCommandDigestMaterial::new(
                target(),
                "control-1".to_owned(),
                "idem-1".to_owned(),
                RuntimeCancellationModeDto::Graceful,
                "reason".to_owned(),
                RuntimeCancellationPropagationPolicyDto::TargetOnly,
                2_000_000_000_000,
                "trust/x".to_owned(),
                TENANT_ID.to_owned(),
                "actor://current".to_owned(),
                "authority://cancel/1".to_owned(),
            )
            .is_ok()
        );
    }

    #[test]
    fn prefixed_uuid_validation_preserves_canonical_v7_contract() {
        let valid = "0198a5bd-2b01-7ae9-9a0c-42532d56e7f7";
        assert!(validate_provider_id(&format!("prv_{valid}")).is_ok());
        assert!(validate_tenant_id(&format!("tenant_{valid}")).is_ok());
        for malformed in [
            valid.to_ascii_uppercase(),
            valid.replace("-7ae9-", "-4ae9-"),
            valid.replace("-9a0c-", "-0a0c-"),
            valid.replace('-', ""),
            format!("{{{valid}}}"),
            format!("{valid} "),
            "00000000-0000-0000-0000-000000000000".to_owned(),
        ] {
            assert!(validate_provider_id(&format!("prv_{malformed}")).is_err());
            assert!(validate_tenant_id(&format!("tenant_{malformed}")).is_err());
        }
        assert!(validate_provider_id(&format!("tenant_{valid}")).is_err());
        assert!(validate_tenant_id(&format!("prv_{valid}")).is_err());
        assert!(validate_provider_id(valid).is_err());
        assert!(validate_tenant_id(valid).is_err());
    }

    #[test]
    fn target_and_status_require_the_exact_foundation_provider_id_shape() {
        assert_eq!(
            RuntimeCancellationTargetBindingDto::new(
                "attempt-start-1".to_owned(),
                "1".repeat(64),
                7,
                "cell-1".to_owned(),
                "session-1".to_owned(),
                11,
                9,
                "undercurrent-local".to_owned(),
                3,
            ),
            Err(RuntimeCancellationDtoError::InvalidProviderId)
        );

        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/runtime_cancellation.json"))
                .expect("runtime cancellation fixture JSON");
        let mut status = fixture["status_request"].clone();
        status["expected_provider_id"] = json!("prv_not-a-uuid");
        let status: GetRuntimeCancellationStatusRequest =
            serde_json::from_value(status).expect("strict shape remains decodable");
        assert_eq!(
            status.validate(),
            Err(RuntimeCancellationDtoError::InvalidProviderId)
        );

        let mut nonstandard_variant = fixture["status_request"].clone();
        nonstandard_variant["expected_provider_id"] =
            json!("prv_0198a5bd-2b01-7ae9-0a0c-42532d56e7f7");
        let status: GetRuntimeCancellationStatusRequest =
            serde_json::from_value(nonstandard_variant).expect("strict shape remains decodable");
        assert_eq!(
            status.validate(),
            Err(RuntimeCancellationDtoError::InvalidProviderId)
        );
    }

    #[test]
    fn control_and_status_require_the_exact_foundation_tenant_id_shape() {
        assert_eq!(
            RuntimeCancellationCommandDigestMaterial::new(
                target(),
                "control-1".to_owned(),
                "idem-1".to_owned(),
                RuntimeCancellationModeDto::Graceful,
                "operator requested stop".to_owned(),
                RuntimeCancellationPropagationPolicyDto::TargetOnly,
                2_000_000_000_000,
                "example/workflow".to_owned(),
                "tenant-1".to_owned(),
                "actor://current".to_owned(),
                "authority://cancel/1".to_owned(),
            ),
            Err(RuntimeCancellationDtoError::InvalidTenantId)
        );

        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/runtime_cancellation.json"))
                .expect("runtime cancellation fixture JSON");
        let mut status = fixture["status_request"].clone();
        status["tenant_id"] = json!("tenant_not-a-uuid");
        let status: GetRuntimeCancellationStatusRequest =
            serde_json::from_value(status).expect("strict shape remains decodable");
        assert_eq!(
            status.validate(),
            Err(RuntimeCancellationDtoError::InvalidTenantId)
        );

        let mut nonstandard_variant = fixture["status_request"].clone();
        nonstandard_variant["tenant_id"] = json!("tenant_0198a5bd-2afd-7dd1-06a3-1fb90c6f0a21");
        let status: GetRuntimeCancellationStatusRequest =
            serde_json::from_value(nonstandard_variant).expect("strict shape remains decodable");
        assert_eq!(
            status.validate(),
            Err(RuntimeCancellationDtoError::InvalidTenantId)
        );
    }

    #[test]
    fn bounded_cancellation_fields_reject_all_control_characters() {
        assert!(
            material_with_text_fields(
                "é".repeat(MAX_REASON_BYTES / 2),
                "é".repeat(MAX_CONTEXT_REFERENCE_BYTES / 2),
                "é".repeat(MAX_CONTEXT_REFERENCE_BYTES / 2),
            )
            .is_ok()
        );
        assert_eq!(
            material_with_text_fields(
                "é".repeat(MAX_REASON_BYTES / 2 + 1),
                "actor://current".to_owned(),
                "authority://cancel/1".to_owned(),
            ),
            Err(RuntimeCancellationDtoError::InvalidReason)
        );
        assert_eq!(
            material_with_text_fields(
                "reason".to_owned(),
                "é".repeat(MAX_CONTEXT_REFERENCE_BYTES / 2 + 1),
                "authority://cancel/1".to_owned(),
            ),
            Err(RuntimeCancellationDtoError::InvalidActorContextRef)
        );
        assert_eq!(
            material_with_text_fields(
                "reason".to_owned(),
                "actor://current".to_owned(),
                "é".repeat(MAX_CONTEXT_REFERENCE_BYTES / 2 + 1),
            ),
            Err(RuntimeCancellationDtoError::InvalidAuthorityRef)
        );

        for reason in ["operator\tstop", "operator\nstop", "operator\rstop"] {
            assert_eq!(
                RuntimeCancellationCommandDigestMaterial::new(
                    target(),
                    "control-1".to_owned(),
                    "idem-1".to_owned(),
                    RuntimeCancellationModeDto::Graceful,
                    reason.to_owned(),
                    RuntimeCancellationPropagationPolicyDto::TargetOnly,
                    2_000_000_000_000,
                    "example/workflow".to_owned(),
                    TENANT_ID.to_owned(),
                    "actor://current".to_owned(),
                    "authority://cancel/1".to_owned(),
                ),
                Err(RuntimeCancellationDtoError::InvalidReason)
            );
        }

        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/runtime_cancellation.json"))
                .expect("runtime cancellation fixture JSON");
        let mut request = fixture["request"].clone();
        request["actor_context_ref"] = json!("actor://current\nforged");
        let request: CancelRuntimeAttemptRequest =
            serde_json::from_value(request).expect("strict shape remains decodable");
        assert_eq!(
            request.validate(),
            Err(RuntimeCancellationDtoError::InvalidActorContextRef)
        );
    }

    #[test]
    fn terminal_states_require_terminal_session_lifecycle() {
        let snapshot = RuntimeCancellationSessionSnapshotDto::new(
            "session-1".to_owned(),
            LifecycleStateDto::Running,
            11,
            9,
            100,
        )
        .expect("snapshot");
        let state = RuntimeCancellationStateDto::AppliedTerminated { session: snapshot };

        assert_eq!(
            state.validate(),
            Err(RuntimeCancellationDtoError::InvalidCancellationState)
        );
    }

    #[test]
    fn status_request_is_strict_and_exact_current_contract() {
        let request = GetRuntimeCancellationStatusRequest::attached(
            "control-1".to_owned(),
            "2".repeat(64),
            "attempt-start-1".to_owned(),
            "1".repeat(64),
            "session-1".to_owned(),
            "prv_0198a5bd-2b01-7ae9-9a0c-42532d56e7f7".to_owned(),
            3,
            "example/workflow".to_owned(),
            TENANT_ID.to_owned(),
            "actor://current".to_owned(),
            "authority://status/1".to_owned(),
        )
        .expect("status request");
        request.validate().expect("valid status request");

        let mut value = serde_json::to_value(request).expect("JSON");
        value["extra"] = json!(true);
        assert!(serde_json::from_value::<GetRuntimeCancellationStatusRequest>(value).is_err());
    }
}
