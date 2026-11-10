// SPDX-License-Identifier: Apache-2.0

use super::*;
use undercurrent_protocol::EffectCertaintyDto;

fn effect_session(disposition: bool, effect: bool) -> Arc<ServerConnectionSession> {
    Arc::new(session_with_agreement(agreement_for_effect_profile(
        8,
        8,
        disposition,
        disposition,
        effect,
        effect,
    )))
}

fn assert_effect_response(bytes: &[u8], disposition: bool, effect: Option<EffectCertaintyDto>) {
    let document: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    assert_eq!(document["value"].get("disposition").is_some(), disposition);
    assert_eq!(
        document["value"].get("effect_certainty").is_some(),
        effect.is_some()
    );
    let ProductResponse::Error(error) = product_codec().decode_response(bytes).unwrap() else {
        panic!("expected the canonical capacity refusal");
    };
    assert_eq!(error.category, ProductErrorCategory::CapacityExceeded);
    assert_eq!(error.effect_certainty, effect);
    assert_eq!(
        error.disposition,
        disposition.then_some(ProductErrorDisposition::CapacityRejected)
    );
}

#[test]
fn finite_error_effect_evidence_is_projected_independently_on_execution_and_cached_replay() {
    let profiles = [(false, false), (true, false), (false, true), (true, true)];
    for evidence in [
        EffectCertaintyDto::KnownAbsent,
        EffectCertaintyDto::KnownApplied,
        EffectCertaintyDto::Unknown,
    ] {
        for (initial_disposition, initial_effect) in profiles {
            let executor = Arc::new(RecordingExecutor {
                error_message_bytes: AtomicUsize::new(1),
                error_effect_certainty: Some(evidence),
                ..RecordingExecutor::default()
            });
            let dispatcher = dispatcher(executor.clone(), 1, 8);
            let request = ProductRequest::ShutdownDaemon(ShutdownDaemonRequest {
                mode: Default::default(),
                mutation: MutationContext {
                    idempotency_key: Some("independent-effect-capabilities".into()),
                    ..MutationContext::current(ProductOperation::ShutdownDaemon)
                },
                drain_timeout_millis: 1_000,
            });
            let initial = effect_session(initial_disposition, initial_effect);
            dispatch_request(&dispatcher, &initial, request_id(1), &request).unwrap();
            assert_effect_response(
                &next_response(&initial),
                initial_disposition,
                initial_effect.then_some(evidence),
            );
            initial.close();
            for (index, (disposition, effect)) in profiles.into_iter().enumerate() {
                let replay = effect_session(disposition, effect);
                dispatch_request(&dispatcher, &replay, request_id(index + 2), &request).unwrap();
                assert_effect_response(
                    &next_response(&replay),
                    disposition,
                    effect.then_some(evidence),
                );
                assert_eq!(
                    executor.executions.load(Ordering::Acquire),
                    1,
                    "replay must not execute again"
                );
                replay.close();
            }
            dispatcher.shutdown();
        }
    }
}
