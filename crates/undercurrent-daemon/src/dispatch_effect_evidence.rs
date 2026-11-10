// SPDX-License-Identifier: Apache-2.0

use super::*;
use undercurrent_protocol::EffectCertaintyDto;

#[test]
fn error_effect_evidence_is_projected_only_for_negotiated_peers() {
    let original = ProductResponse::Error(ProductError {
        operation: Some(ProductOperation::StartSession),
        category: ProductErrorCategory::CapacityExceeded,
        disposition: Some(ProductErrorDisposition::CapacityRejected),
        effect_certainty: Some(EffectCertaintyDto::KnownAbsent),
        message: "capacity refused before insertion".into(),
        session_id: None,
        retryable: false,
        retry_after_millis: None,
        limit: Some(2),
        resynchronization: None,
    });
    for disposition in [false, true] {
        for effect_evidence in [false, true] {
            let ProductResponse::Error(projected) =
                response_for_profile(disposition, effect_evidence, original.clone())
            else {
                panic!("error")
            };
            assert_eq!(
                projected.disposition,
                disposition.then_some(ProductErrorDisposition::CapacityRejected)
            );
            assert_eq!(
                projected.effect_certainty,
                effect_evidence.then_some(EffectCertaintyDto::KnownAbsent)
            );
            let encoded = serde_json::to_value(projected).unwrap();
            assert_eq!(encoded.get("effect_certainty").is_some(), effect_evidence);
        }
    }
    let ProductResponse::Error(canonical) = original else {
        panic!("error")
    };
    assert_eq!(
        canonical.effect_certainty,
        Some(EffectCertaintyDto::KnownAbsent)
    );
}
