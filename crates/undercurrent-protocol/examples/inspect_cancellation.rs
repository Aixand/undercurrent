// SPDX-License-Identifier: Apache-2.0

//! Build and validate synthetic cancellation material without sending a command.

use std::error::Error;

use undercurrent_protocol::{
    CancelRuntimeAttemptRequest, GetRuntimeCancellationStatusRequest,
    RuntimeCancellationCommandDigestMaterial, RuntimeCancellationDtoError,
    RuntimeCancellationModeDto, RuntimeCancellationPropagationPolicyDto,
    RuntimeCancellationTargetBindingDto,
};

fn main() -> Result<(), Box<dyn Error>> {
    // These fixture identities and timestamps describe no live runtime target.
    let target = RuntimeCancellationTargetBindingDto::new(
        "attempt-start-1".to_owned(),
        "1".repeat(64),
        7,
        "cell-1".to_owned(),
        "session-1".to_owned(),
        11,
        9,
        "prv_0198a5bd-2b01-7ae9-9a0c-42532d56e7f7".to_owned(),
        3,
    )?;
    let material = RuntimeCancellationCommandDigestMaterial::new(
        target,
        "control-1".to_owned(),
        "idem-1".to_owned(),
        RuntimeCancellationModeDto::Graceful,
        "operator requested stop".to_owned(),
        RuntimeCancellationPropagationPolicyDto::TargetOnly,
        2_000_000_000_000,
        "example/workflow".to_owned(),
        "tenant_0198a5bd-2afd-7dd1-b6a3-1fb90c6f0a21".to_owned(),
        "actor://current".to_owned(),
        "authority://cancel/1".to_owned(),
    )?;
    let digest = material.canonical_digest_sha256()?;
    let request = CancelRuntimeAttemptRequest::attached(material, digest)?;
    let bytes = serde_json::to_vec(&request)?;
    let decoded: CancelRuntimeAttemptRequest = serde_json::from_slice(&bytes)?;
    decoded.validate()?;
    assert_eq!(decoded, request);
    assert_ne!(
        decoded.command_digest_sha256(),
        decoded.product_request_fingerprint_sha256()?
    );
    println!("cancellation: valid round trip; command and product digests differ");

    let mut changed: serde_json::Value = serde_json::from_slice(&bytes)?;
    changed["reason"] = serde_json::json!("changed reason");
    let changed: CancelRuntimeAttemptRequest = serde_json::from_value(changed)?;
    assert_eq!(
        changed.validate(),
        Err(RuntimeCancellationDtoError::CommandDigestMismatch)
    );
    println!("changed reason: decoded shape, rejected semantic digest");

    let status = GetRuntimeCancellationStatusRequest::attached(
        decoded.control_id().to_owned(),
        decoded.command_digest_sha256().to_owned(),
        decoded.target_attempt_id().to_owned(),
        decoded.target_request_digest_sha256().to_owned(),
        decoded.session_id().to_owned(),
        decoded.expected_provider_id().to_owned(),
        decoded.expected_provider_generation(),
        decoded.source_product().to_owned(),
        decoded.tenant_id().to_owned(),
        decoded.actor_context_ref().to_owned(),
        "authority://status/1".to_owned(),
    )?;
    status.validate()?;
    println!("status: exact lookup constructed; no command was sent");
    Ok(())
}
