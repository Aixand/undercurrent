// SPDX-License-Identifier: Apache-2.0

use undercurrent_domain::*;

fn attempt() -> OperationAttempt {
    OperationAttempt::accepted(
        OperationAttemptId::new("attempt-1").expect("attempt id"),
        Some(SessionId::new("session-1").expect("session id")),
        ProcessMutationKind::Spawn,
        FencingToken::new(2).expect("fence"),
        PayloadDigest::of_bytes(b"request"),
        TimestampMillis::new(10),
    )
}

#[test]
fn duplicate_and_reordered_attempt_settlements_are_distinguished() {
    let mut value = attempt();
    let result = PayloadDigest::of_bytes(b"result");
    assert_eq!(
        value
            .settle(
                FencingToken::new(2).unwrap(),
                EffectCertainty::KnownApplied,
                Some(result),
                TimestampMillis::new(11),
            )
            .unwrap(),
        OperationAttemptTransition::Applied
    );
    assert_eq!(
        value
            .settle(
                FencingToken::new(2).unwrap(),
                EffectCertainty::KnownApplied,
                Some(result),
                TimestampMillis::new(11),
            )
            .unwrap(),
        OperationAttemptTransition::Duplicate
    );
    assert!(
        value
            .settle(
                FencingToken::new(1).unwrap(),
                EffectCertainty::KnownApplied,
                Some(result),
                TimestampMillis::new(12),
            )
            .is_err()
    );
}

#[test]
fn response_loss_remains_unknown_until_explicit_reconciliation() {
    let mut value = attempt();
    value
        .settle(
            FencingToken::new(2).unwrap(),
            EffectCertainty::Unknown,
            None,
            TimestampMillis::new(11),
        )
        .unwrap();
    assert!(value.state().requires_reconciliation());
    value
        .reconcile(
            EffectCertainty::KnownApplied,
            Some(PayloadDigest::of_bytes(b"result")),
            TimestampMillis::new(12),
        )
        .unwrap();
    assert!(!value.state().requires_reconciliation());
}

#[test]
fn output_integrity_detects_tampering_and_records_truncation() {
    let integrity = OutputChunkIntegrity::for_payload(b"abc", 9).expect("metadata");
    assert_eq!(
        integrity.verify(b"abc"),
        OutputIntegrityStatus::Verified { truncated: true }
    );
    assert_eq!(
        integrity.verify(b"abd"),
        OutputIntegrityStatus::DigestMismatch
    );
    assert_eq!(
        integrity.verify(b"ab"),
        OutputIntegrityStatus::LengthMismatch
    );
}
