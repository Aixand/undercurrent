// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;

use undercurrent_wire::core::NamespacedName;
use undercurrent_wire::{
    CURRENT_PROTOCOL_VERSION, ClientId, ConnectionHello, ConnectionId, ConnectionLimits,
    ConnectionPolicy, HeartbeatPolicy, ServerEpoch, connection_capabilities, negotiate_connection,
};
use undercurrent_wire::{
    HandshakeCodec, HandshakeCodecError, HandshakeCodecErrorCategory, HandshakeDecodeStatus,
    HandshakeMessageKind, HandshakeRejection, HandshakeRejectionCategory, HandshakeResponse,
};

#[test]
fn hello_and_both_server_outcomes_round_trip_incrementally() {
    let codec = codec();
    let hello = hello();
    let accepted = HandshakeResponse::Accepted(agreement(&hello));
    let rejected = HandshakeResponse::Rejected(HandshakeRejection::new(
        HandshakeRejectionCategory::CapacityExceeded,
        true,
    ));

    let encoded_hello = codec.encode_hello(&hello).expect("encode hello");
    for length in 0..encoded_hello.len() {
        assert!(matches!(
            codec
                .decode_hello(&encoded_hello[..length])
                .expect("valid partial hello"),
            HandshakeDecodeStatus::NeedMore { minimum_total }
                if minimum_total <= encoded_hello.len()
        ));
    }
    let decoded_hello = complete(
        codec
            .decode_hello(&encoded_hello)
            .expect("decode complete hello"),
    );
    assert_eq!(decoded_hello.message, hello);
    assert_eq!(decoded_hello.consumed, encoded_hello.len());

    for response in [accepted, rejected] {
        let encoded = codec.encode_response(&response).expect("encode response");
        let decoded = complete(codec.decode_response(&encoded).expect("decode response"));
        assert_eq!(decoded.message, response);
        assert_eq!(decoded.consumed, encoded.len());
    }
}

#[test]
fn product_owned_uchs_codec_matches_the_current_rejection_fixture() {
    let fixture = fixture_hex(include_str!("fixtures/uchs-capacity-rejection.hex"));
    let rejection = HandshakeResponse::Rejected(HandshakeRejection::new(
        HandshakeRejectionCategory::CapacityExceeded,
        true,
    ));
    let codec = codec();

    assert_eq!(codec.encode_response(&rejection).expect("encode"), fixture);
    assert_eq!(
        complete(codec.decode_response(&fixture).expect("decode")).message,
        rejection
    );
}

#[test]
fn capacity_handshake_rejection_round_trips_explicit_retry_guidance() {
    let rejection = HandshakeRejection::new(HandshakeRejectionCategory::CapacityExceeded, true);
    let encoded = codec()
        .encode_response(&HandshakeResponse::Rejected(rejection.clone()))
        .expect("encode capacity rejection");
    let decoded = complete(codec().decode_response(&encoded).expect("decode rejection"));
    let HandshakeResponse::Rejected(decoded) = decoded.message else {
        panic!("capacity rejection expected");
    };

    assert_eq!(
        decoded.category(),
        HandshakeRejectionCategory::CapacityExceeded
    );
    assert!(decoded.retryable());
    assert_eq!(decoded, rejection);
}

#[test]
fn declared_and_encoded_sizes_are_rejected_at_the_configured_bound() {
    let codec = HandshakeCodec::new(64).expect("codec");
    let mut header = vec![b'U', b'C', b'H', b'S', 10, 1, 0, 0];
    header.extend_from_slice(&1_000_u32.to_be_bytes());
    header.extend_from_slice(b"0.1.0-beta");

    assert!(matches!(
        codec.decode_hello(&header),
        Err(HandshakeCodecError::FrameTooLarge {
            actual: 1_022,
            maximum: 64
        })
    ));
    assert!(matches!(
        codec.encode_hello(&hello()),
        Err(HandshakeCodecError::FrameTooLarge { maximum: 64, .. })
    ));
}

#[test]
fn direction_format_flags_and_validated_payload_are_enforced() {
    let codec = codec();
    let accepted = codec
        .encode_response(&HandshakeResponse::Accepted(agreement(&hello())))
        .expect("accepted response");
    assert!(matches!(
        codec.decode_hello(&accepted),
        Err(HandshakeCodecError::InvalidMessageKind { code: 2 })
    ));

    let mut invalid_format = codec.encode_hello(&hello()).expect("hello");
    invalid_format[4] = 99;
    assert!(matches!(
        codec.decode_hello(&invalid_format),
        Err(HandshakeCodecError::UnsupportedFormat { received: 99 })
    ));

    invalid_format[4] = 10;
    invalid_format[12] = b'9';
    assert!(codec.decode_hello(&invalid_format).is_err());

    let mut invalid_flags = codec.encode_hello(&hello()).expect("hello");
    invalid_flags[7] = 1;
    assert!(matches!(
        codec.decode_hello(&invalid_flags),
        Err(HandshakeCodecError::UnsupportedFlags { flags: 1 })
    ));

    let malformed = raw_frame(1, br#"{"client_id":""}"#);
    let error = codec
        .decode_hello(&malformed)
        .expect_err("invalid validated hello");
    assert_eq!(error.category(), HandshakeCodecErrorCategory::InvalidFrame);
    assert!(std::error::Error::source(&error).is_some());
    assert!(!format!("{error:?}").contains("client_id"));
}

#[test]
fn handshake_registries_are_unique_and_stable() {
    let fixture = contract_fixture();
    assert_eq!(
        HandshakeMessageKind::ALL
            .map(HandshakeMessageKind::code)
            .into_iter()
            .collect::<BTreeSet<_>>()
            .len(),
        HandshakeMessageKind::ALL.len()
    );
    assert_eq!(
        HandshakeRejectionCategory::ALL
            .map(HandshakeRejectionCategory::as_str)
            .into_iter()
            .collect::<BTreeSet<_>>()
            .len(),
        HandshakeRejectionCategory::ALL.len()
    );
    assert_eq!(
        HandshakeMessageKind::ALL
            .map(|kind| (kind.as_str().to_owned(), kind.code()))
            .as_slice(),
        fixture.message_kinds.as_slice()
    );
    assert_eq!(
        HandshakeRejectionCategory::ALL
            .map(HandshakeRejectionCategory::as_str)
            .as_slice(),
        fixture.rejection_categories.as_slice()
    );
    for (category, name) in HandshakeRejectionCategory::ALL
        .into_iter()
        .zip(fixture.rejection_categories)
    {
        let serialized = serde_json::Value::String(name);
        assert_eq!(
            serde_json::to_value(category).expect("serialize rejection category"),
            serialized
        );
        assert_eq!(
            serde_json::from_value::<HandshakeRejectionCategory>(serialized)
                .expect("decode rejection category"),
            category
        );
    }
    assert!(serde_json::from_str::<HandshakeRejectionCategory>(r#""unknown_rejection""#).is_err());
}

#[test]
fn encoded_handshake_header_matches_fixed_contract_fixture() {
    let fixture = contract_fixture();
    let encoded = codec().encode_hello(&hello()).expect("hello encodes");

    assert_eq!(&encoded[..4], fixture.magic.as_bytes());
    assert_eq!(encoded[4], fixture.version.len() as u8);
    assert_eq!(
        encoded[5],
        fixture
            .message_kinds
            .iter()
            .find(|(name, _)| name == "hello")
            .expect("hello code")
            .1
    );
    assert!(encoded.len() >= fixture.header_bytes);
    assert_eq!(
        u32::from_be_bytes(encoded[8..12].try_into().expect("body length")) as usize
            + fixture.header_bytes,
        encoded.len()
    );
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ContractFixture {
    magic: String,
    version: String,
    header_bytes: usize,
    message_kinds: Vec<(String, u8)>,
    rejection_categories: Vec<String>,
}

fn contract_fixture() -> ContractFixture {
    serde_json::from_str(include_str!("fixtures/uchs-contract.json"))
        .expect("fixed current UCHS contract")
}

fn codec() -> HandshakeCodec {
    HandshakeCodec::new(32_768).expect("valid codec")
}

fn hello() -> ConnectionHello {
    ConnectionHello::new(
        ClientId::new("client-1").expect("client ID"),
        CURRENT_PROTOCOL_VERSION,
        BTreeSet::from([
            capability(connection_capabilities::MULTIPLEX),
            capability(connection_capabilities::FLOW_CONTROL),
        ]),
        BTreeSet::from([capability(connection_capabilities::STREAM_RESUME)]),
        limits(),
    )
    .expect("valid hello")
}

fn agreement(hello: &ConnectionHello) -> undercurrent_wire::ConnectionAgreement {
    let policy = ConnectionPolicy::new(
        CURRENT_PROTOCOL_VERSION,
        BTreeSet::from([
            capability(connection_capabilities::MULTIPLEX),
            capability(connection_capabilities::FLOW_CONTROL),
            capability(connection_capabilities::STREAM_RESUME),
        ]),
        limits(),
        HeartbeatPolicy::new(5_000, 15_000).expect("heartbeat"),
        Some(30_000),
    )
    .expect("policy");
    negotiate_connection(
        hello,
        &policy,
        ConnectionId::new("connection-1").expect("connection ID"),
        ServerEpoch::new("epoch-1").expect("server epoch"),
    )
    .expect("agreement")
}

fn limits() -> ConnectionLimits {
    ConnectionLimits::new(65_536, 512, 128, 8_000_000, 128_000, 64_000).expect("connection limits")
}

fn capability(value: &str) -> NamespacedName {
    NamespacedName::new(value).expect("capability")
}

fn complete<T>(status: HandshakeDecodeStatus<T>) -> undercurrent_wire::DecodedHandshake<T> {
    match status {
        HandshakeDecodeStatus::Complete(frame) => frame,
        HandshakeDecodeStatus::NeedMore { minimum_total } => {
            panic!("expected complete handshake; need {minimum_total} bytes")
        }
    }
}

fn raw_frame(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut frame = vec![b'U', b'C', b'H', b'S', 10, kind, 0, 0];
    frame.extend_from_slice(
        &u32::try_from(body.len())
            .expect("body length")
            .to_be_bytes(),
    );
    frame.extend_from_slice(b"0.1.0-beta");
    frame.extend_from_slice(body);
    frame
}

fn fixture_hex(value: &str) -> Vec<u8> {
    let value = value.trim();
    assert_eq!(
        value.len() % 2,
        0,
        "hex fixture must contain complete bytes"
    );
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).expect("ASCII fixture");
            u8::from_str_radix(pair, 16).expect("hex fixture")
        })
        .collect()
}
