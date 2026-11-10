// SPDX-License-Identifier: Apache-2.0

use undercurrent_protocol::{
    ProductCodec, ProductCodecLimits, ProductError, ProductErrorCategory, ProductErrorDisposition,
    ProductOperation, ProductResponse, ProductStreamError, ProductStreamOpenResponse,
};

fn codec() -> ProductCodec {
    ProductCodec::new(ProductCodecLimits::new(4_096, 4_096, 4_096, 4_096).expect("limits"))
}

#[test]
fn uref_034_disposition_registry_and_category_defaults_are_stable() {
    let expected: Vec<String> =
        serde_json::from_slice(include_bytes!("fixtures/uref-034-error-dispositions.json"))
            .expect("disposition fixture");
    assert_eq!(
        ProductErrorDisposition::ALL
            .map(ProductErrorDisposition::as_str)
            .as_slice(),
        expected.as_slice()
    );

    for (category, disposition) in [
        (
            ProductErrorCategory::InvalidRequest,
            ProductErrorDisposition::SemanticRejection,
        ),
        (
            ProductErrorCategory::Persistence,
            ProductErrorDisposition::InfrastructureFailure,
        ),
        (
            ProductErrorCategory::Process,
            ProductErrorDisposition::RuntimeFailure,
        ),
        (
            ProductErrorCategory::Transport,
            ProductErrorDisposition::TransportFailure,
        ),
        (
            ProductErrorCategory::ProtocolMismatch,
            ProductErrorDisposition::ProtocolFailure,
        ),
        (
            ProductErrorCategory::TimedOut,
            ProductErrorDisposition::Timeout,
        ),
        (
            ProductErrorCategory::Cancelled,
            ProductErrorDisposition::Cancellation,
        ),
        (
            ProductErrorCategory::ReplayUnavailable,
            ProductErrorDisposition::Partial,
        ),
    ] {
        assert_eq!(
            ProductErrorDisposition::default_for_category(category),
            disposition
        );
    }
}

#[test]
fn uref_034_current_error_round_trips_but_legacy_absence_is_not_inferred() {
    let response = ProductResponse::Error(ProductError {
        effect_certainty: None,
        operation: Some(ProductOperation::ShutdownDaemon),
        category: ProductErrorCategory::FailedPrecondition,
        disposition: Some(ProductErrorDisposition::UnknownInconclusive),
        message: "shutdown effect remains unknown".to_owned(),
        session_id: None,
        retryable: false,
        retry_after_millis: None,
        limit: None,
        resynchronization: None,
    });
    let encoded = codec()
        .encode_response(&response)
        .expect("current response");
    assert_eq!(codec().decode_response(&encoded).expect("decode"), response);
    assert!(
        String::from_utf8(encoded)
            .expect("UTF-8")
            .contains("unknown_inconclusive")
    );

    let legacy = br#"{"outcome":"error","value":{"operation":"shutdown_daemon","category":"failed_precondition","message":"legacy rejection","session_id":null,"retryable":false,"retry_after_millis":null,"limit":null,"resynchronization":null}}"#;
    let ProductResponse::Error(error) = codec()
        .decode_response(legacy)
        .expect("legacy 0.1.0-beta decode")
    else {
        panic!("legacy response must remain an error");
    };
    assert_eq!(error.category, ProductErrorCategory::FailedPrecondition);
    assert_eq!(
        error.disposition, None,
        "legacy absence must remain unknown"
    );

    let legacy_stream = br#"{"outcome":"error","value":{"kind":null,"category":"failed_precondition","message":"legacy stream rejection","session_id":null,"retryable":false,"retry_after_millis":null,"limit":null,"resynchronization":null}}"#;
    let ProductStreamOpenResponse::Error(error) = codec()
        .decode_stream_open_response(legacy_stream)
        .expect("legacy 0.1.0-beta stream decode")
    else {
        panic!("legacy stream response must remain an error");
    };
    assert_eq!(error.category, ProductErrorCategory::FailedPrecondition);
    assert_eq!(error.disposition, None);

    let current_stream = ProductStreamOpenResponse::Error(ProductStreamError {
        kind: None,
        category: ProductErrorCategory::FailedPrecondition,
        disposition: Some(ProductErrorDisposition::UnknownInconclusive),
        message: "stream effect remains unknown".to_owned(),
        session_id: None,
        retryable: false,
        retry_after_millis: None,
        limit: None,
        resynchronization: None,
    });
    let encoded = codec()
        .encode_stream_open_response(&current_stream)
        .expect("current stream response");
    assert_eq!(
        codec()
            .decode_stream_open_response(&encoded)
            .expect("current stream decode"),
        current_stream
    );
}
