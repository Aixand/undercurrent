// SPDX-License-Identifier: Apache-2.0

//! Offline protocol example: no daemon, credentials, or process control.

use std::error::Error;
use std::sync::Arc;

use undercurrent_protocol::{
    MutationContext, ProductCodec, ProductCodecLimits, ProductOperation, ProductRequest,
    StopSessionRequest, TerminalOutputChunk, TerminalOutputCodec, canonical_product_request_digest,
    execution_output_payload_digest_sha256, validate_product_request_contract,
    verify_execution_output_payload_digest,
};

fn main() -> Result<(), Box<dyn Error>> {
    let codec = ProductCodec::new(ProductCodecLimits::new(4096, 4096, 4096, 4096)?);
    let stop = StopSessionRequest {
        graceful_only: false,
        mutation: MutationContext::current(ProductOperation::StopSession),
        session_id: "example-session".to_owned(),
        expected_revision: Some(7),
        force: false,
    };
    let request = ProductRequest::StopSession(stop.clone());
    validate_product_request_contract(&request)?;
    let encoded = codec.encode_request(&request)?;
    assert_eq!(codec.decode_request(&encoded)?, request);
    println!("stop_session: contract valid; JSON round trip matched");

    let original_digest = canonical_product_request_digest(&request)?;
    let mut retry = stop.clone();
    retry.mutation.idempotency_key = Some("example-retry-key".to_owned());
    assert_eq!(
        original_digest,
        canonical_product_request_digest(&ProductRequest::StopSession(retry))?
    );
    let mut changed = stop;
    changed.expected_revision = Some(8);
    assert_ne!(
        original_digest,
        canonical_product_request_digest(&ProductRequest::StopSession(changed))?
    );
    println!("retry metadata: same digest; changed revision: different digest");

    let terminal_codec = TerminalOutputCodec::new(64)?;
    let chunk = TerminalOutputChunk::new(41, Arc::from(b"abc".as_slice()));
    let decoded = terminal_codec.decode(&terminal_codec.encode(&chunk)?)?;
    assert_eq!(decoded, chunk);
    assert_eq!(decoded.end_offset(), Some(44));
    let digest = execution_output_payload_digest_sha256(decoded.bytes());
    assert!(verify_execution_output_payload_digest(
        decoded.bytes(),
        &digest
    ));
    assert!(!verify_execution_output_payload_digest(b"abd", &digest));
    println!("terminal: 3 bytes at offsets 41..44; digest verified");
    Ok(())
}
