// SPDX-License-Identifier: Apache-2.0

use serde_json::{Value, json};
use undercurrent_protocol::ProductError;

fn legacy_error() -> Value {
    json!({
        "operation": "start_session", "category": "capacity_exceeded",
        "disposition": "capacity_rejected", "message": "session capacity reached",
        "session_id": null, "retryable": false, "retry_after_millis": null,
        "limit": 2, "resynchronization": null
    })
}

fn assert_round_trip(certainty: &str) {
    let mut document = legacy_error();
    document["effect_certainty"] = json!(certainty);
    let error: ProductError = serde_json::from_value(document.clone())
        .expect("explicit owner effect evidence must be representable");
    assert_eq!(serde_json::to_value(error).unwrap(), document);
}

#[test]
fn error_known_absence_survives_the_public_boundary() {
    assert_round_trip("known_absent");
}

#[test]
fn error_unknown_effect_remains_explicit() {
    assert_round_trip("unknown");
}

#[test]
fn error_known_application_cannot_be_confused_with_absence() {
    assert_round_trip("known_applied");
}

#[test]
fn legacy_errors_do_not_infer_effect_certainty_from_the_category() {
    let document = legacy_error();
    let error: ProductError = serde_json::from_value(document.clone()).unwrap();
    let encoded = serde_json::to_value(error).unwrap();
    assert_eq!(encoded, document);
    assert!(encoded.get("effect_certainty").is_none());
}

#[test]
fn malformed_effect_evidence_is_not_admitted() {
    for value in [
        json!(false),
        json!(1),
        json!({}),
        json!([]),
        json!("probably_absent"),
    ] {
        let mut document = legacy_error();
        document["effect_certainty"] = value;
        assert!(serde_json::from_value::<ProductError>(document).is_err());
    }
}
