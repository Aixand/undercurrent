// SPDX-License-Identifier: Apache-2.0

use serde_json::{Value, json};
use undercurrent_protocol::{
    MutationContext, ProductOperation, ProductRequest, SendTerminalInputRequest,
    StopSessionRequest, canonical_product_request_digest,
};

#[test]
fn stop_request_preserves_the_explicit_session_revision_precondition() {
    let legacy = json!({
        "mutation": MutationContext::current(ProductOperation::StopSession),
        "session_id": "revision-target",
        "force": false
    });
    let legacy_request: StopSessionRequest = serde_json::from_value(legacy).unwrap();
    let mut expected = serde_json::to_value(legacy_request).unwrap();
    assert!(expected.get("expected_revision").is_none());
    expected["expected_revision"] = json!(7);

    let request: StopSessionRequest = serde_json::from_value(expected.clone())
        .expect("stop must carry the caller's revision to the daemon mutation boundary");
    assert_eq!(serde_json::to_value(request).unwrap(), expected);
}

#[test]
fn input_request_preserves_the_explicit_session_revision_precondition() {
    let legacy = json!({
        "mutation": MutationContext::current(ProductOperation::SendTerminalInput),
        "session_id": "revision-target",
        "delivery_id": "revision-delivery",
        "items": [{"kind": "text", "text": "qualified input"}]
    });
    let legacy_request: SendTerminalInputRequest = serde_json::from_value(legacy).unwrap();
    let mut expected: Value = serde_json::to_value(legacy_request).unwrap();
    assert!(expected.get("expected_revision").is_none());
    expected["expected_revision"] = json!(7);

    let request: SendTerminalInputRequest = serde_json::from_value(expected.clone())
        .expect("input must carry the caller's revision to the daemon mutation boundary");
    assert_eq!(serde_json::to_value(request).unwrap(), expected);
}

#[test]
fn session_revision_precondition_is_part_of_the_semantic_attempt_identity() {
    let requests = [
        ProductRequest::StopSession(StopSessionRequest {
            graceful_only: false,
            mutation: MutationContext::current(ProductOperation::StopSession),
            session_id: "revision-target".into(),
            expected_revision: Some(7),
            force: false,
        }),
        ProductRequest::SendTerminalInput(SendTerminalInputRequest {
            mutation: MutationContext::current(ProductOperation::SendTerminalInput),
            session_id: "revision-target".into(),
            expected_revision: Some(7),
            delivery_id: Some("revision-delivery".into()),
            items: vec![undercurrent_protocol::TerminalInputDto::Text {
                text: "input".into(),
            }],
            observation_policy: Default::default(),
        }),
    ];
    for request in requests {
        let exact = canonical_product_request_digest(&request).unwrap();
        assert_eq!(
            canonical_product_request_digest(&request.clone()).unwrap(),
            exact
        );
        for revision in [None, Some(8)] {
            let mut changed = request.clone();
            match &mut changed {
                ProductRequest::StopSession(request) => request.expected_revision = revision,
                ProductRequest::SendTerminalInput(request) => request.expected_revision = revision,
                _ => unreachable!(),
            }
            assert_ne!(canonical_product_request_digest(&changed).unwrap(), exact);
        }
    }
}

#[test]
fn invalid_revision_precondition_types_are_rejected() {
    for revision in [
        json!(-1),
        json!(1.5),
        json!(true),
        json!("7"),
        json!({}),
        json!([]),
    ] {
        let input = json!({
            "mutation": MutationContext::current(ProductOperation::SendTerminalInput),
            "session_id": "revision-target", "delivery_id": null,
            "items": [{"kind": "text", "text": "input"}],
            "expected_revision": revision,
        });
        assert!(serde_json::from_value::<SendTerminalInputRequest>(input).is_err());
        let stop = json!({
            "mutation": MutationContext::current(ProductOperation::StopSession),
            "session_id": "revision-target", "force": false,
            "expected_revision": revision,
        });
        assert!(serde_json::from_value::<StopSessionRequest>(stop).is_err());
    }
}
