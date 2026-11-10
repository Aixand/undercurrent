// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::sync::Arc;

use undercurrent_wire::{
    ClientMessage, InboundStreamItem, ServerMessage, StreamGap, StreamGapReason,
    StreamRecoveryAction,
};
use undercurrent_wire::{
    CorrelationId, RequestId, ServerEpoch, SpanId, StreamId, StreamSequence, TraceContext, TraceId,
};
use undercurrent_wire::{
    DecodeStatus, MAX_CLIENT_FRAME_ENVELOPE_BYTES, WireCodec, WireDirection, WireError,
    WireMessageKind,
};

#[test]
fn every_client_message_kind_round_trips() {
    let codec = codec();
    let messages = vec![
        ClientMessage::Request {
            request_id: request_id(1),
            trace_context: trace_context(1),
            payload: bytes(&[0, 1, 2, 255]),
        },
        ClientMessage::CancelRequest {
            request_id: request_id(2),
        },
        ClientMessage::OpenStream {
            request_id: request_id(3),
            stream_id: stream_id(1),
            trace_context: trace_context(3),
            payload: bytes(b"query"),
        },
        ClientMessage::ResumeStream {
            request_id: request_id(4),
            stream_id: stream_id(9),
            server_epoch: ServerEpoch::new("epoch-1").expect("server epoch"),
            after: StreamSequence::new(42),
            trace_context: trace_context(4),
            payload: bytes(b"terminal"),
        },
        ClientMessage::CloseStream {
            stream_id: stream_id(2),
        },
        ClientMessage::WindowUpdate {
            stream_id: stream_id(3),
            additional_bytes: 65_536,
        },
        ClientMessage::Ping,
        ClientMessage::Close,
    ];

    for message in messages {
        let encoded = codec.encode_client(&message).expect("client encode");
        let decoded = complete(codec.decode_client(&encoded).expect("client decode"));
        assert_eq!(decoded.message, message);
        assert_eq!(decoded.consumed, encoded.len());
    }
}

#[test]
fn product_owned_ucmp_codec_matches_the_current_ping_fixture() {
    let fixture = fixture_hex(include_str!("fixtures/ucmp-ping.hex"));
    let codec = codec();

    assert_eq!(
        codec.encode_client(&ClientMessage::Ping).expect("encode"),
        fixture
    );
    assert_eq!(
        complete(codec.decode_client(&fixture).expect("decode")).message,
        ClientMessage::Ping
    );
}

#[test]
fn every_server_message_kind_round_trips_with_raw_binary_output() {
    let codec = codec();
    let stream = stream_id(1);
    let messages = vec![
        ServerMessage::Response {
            request_id: request_id(1),
            payload: bytes(b"response"),
        },
        ServerMessage::StreamItem {
            stream_id: stream.clone(),
            item: InboundStreamItem::Data {
                sequence: StreamSequence::new(1),
                bytes: bytes(&[0, 255, 1, 254, 2, 253]),
                flow_controlled_bytes: 0,
            },
        },
        ServerMessage::StreamItem {
            stream_id: stream.clone(),
            item: InboundStreamItem::Gap(StreamGap {
                stream_id: stream,
                first_missing: StreamSequence::new(2),
                last_missing: StreamSequence::new(7),
                reason: StreamGapReason::ReplayEvicted,
                recovery: StreamRecoveryAction::RestartFromAvailableOffset,
            }),
        },
        ServerMessage::Pong,
        ServerMessage::Close,
    ];

    for message in messages {
        let encoded = codec.encode_server(&message).expect("server encode");
        let decoded = complete(codec.decode_server(&encoded).expect("server decode"));
        let mut expected = message;
        if let ServerMessage::StreamItem {
            item:
                InboundStreamItem::Data {
                    flow_controlled_bytes,
                    ..
                },
            ..
        } = &mut expected
        {
            *flow_controlled_bytes = u64::try_from(encoded.len()).expect("encoded length");
        }
        assert_eq!(decoded.message, expected);
        assert_eq!(decoded.consumed, encoded.len());
    }
}

#[test]
fn every_stream_gap_reason_round_trips() {
    let codec = codec();
    let stream = stream_id(1);
    let reasons = [
        StreamGapReason::LiveQueueOverflow,
        StreamGapReason::ReplayEvicted,
        StreamGapReason::ReplayUnavailable,
        StreamGapReason::ServerEpochChanged,
        StreamGapReason::SourceRetentionEvicted,
    ];

    for reason in reasons {
        let message = ServerMessage::StreamItem {
            stream_id: stream.clone(),
            item: InboundStreamItem::Gap(StreamGap {
                stream_id: stream.clone(),
                first_missing: StreamSequence::new(2),
                last_missing: StreamSequence::new(7),
                reason,
                recovery: StreamRecoveryAction::RequestSnapshot,
            }),
        };
        let encoded = codec.encode_server(&message).expect("server encode");
        let decoded = complete(codec.decode_server(&encoded).expect("server decode"));
        assert_eq!(decoded.message, message);
    }
}

#[test]
fn partial_decode_requests_only_the_current_bounded_frame() {
    let codec = codec();
    let first = codec
        .encode_client(&ClientMessage::Request {
            request_id: request_id(1),
            trace_context: trace_context(1),
            payload: bytes(b"payload"),
        })
        .expect("encode");
    let second = codec.encode_client(&ClientMessage::Ping).expect("encode");

    for length in 0..first.len() {
        let status = codec
            .decode_client(&first[..length])
            .expect("partial input is valid");
        assert!(matches!(
            status,
            DecodeStatus::NeedMore { minimum_total } if minimum_total <= first.len()
        ));
    }

    let mut combined = first.clone();
    combined.extend_from_slice(&second);
    let decoded = complete(codec.decode_client(&combined).expect("first decode"));
    assert_eq!(decoded.consumed, first.len());
    let next = complete(
        codec
            .decode_client(&combined[decoded.consumed..])
            .expect("second decode"),
    );
    assert_eq!(next.message, ClientMessage::Ping);
}

#[test]
fn oversized_declared_body_is_rejected_from_header_without_body_allocation() {
    let codec = WireCodec::new(64).expect("codec");
    let mut header = vec![b'U', b'C', b'M', b'P', 10, 1, 0, 0];
    header.extend_from_slice(&1_000_u32.to_be_bytes());
    header.extend_from_slice(b"0.1.0-beta");

    let error = codec
        .decode_client(&header)
        .expect_err("declared size rejected");

    assert!(matches!(
        error,
        WireError::FrameTooLarge {
            actual: 1_022,
            maximum: 64
        }
    ));
}

#[test]
fn direction_flags_identifiers_payloads_and_gap_ranges_are_validated() {
    let codec = codec();
    let mut future = codec
        .encode_client(&ClientMessage::Ping)
        .expect("ping encode");
    future[4] = 2;
    assert!(matches!(
        codec.decode_client(&future),
        Err(WireError::UnsupportedVersion {
            received: 2,
            supported: 10
        })
    ));
    future[4] = 10;
    future[12] = b'9';
    assert!(codec.decode_client(&future).is_err());
    let server = codec
        .encode_server(&ServerMessage::Pong)
        .expect("server encode");
    assert!(matches!(
        codec.decode_client(&server),
        Err(WireError::InvalidMessageKind {
            direction: WireDirection::ClientToServer,
            ..
        })
    ));

    let mut flags = codec
        .encode_client(&ClientMessage::Ping)
        .expect("ping encode");
    flags[7] = 1;
    assert!(matches!(
        codec.decode_client(&flags),
        Err(WireError::UnsupportedFlags { flags: 1 })
    ));

    let empty_request = raw_frame(1, &[0, 0]);
    assert!(matches!(
        codec.decode_client(&empty_request),
        Err(WireError::InvalidIdentifier {
            field: "request_id"
        })
    ));

    assert!(matches!(
        codec.encode_client(&ClientMessage::Request {
            request_id: request_id(1),
            trace_context: trace_context(1),
            payload: bytes(&[]),
        }),
        Err(WireError::EmptyPayload)
    ));

    let stream = stream_id(1);
    assert!(matches!(
        codec.encode_server(&ServerMessage::StreamItem {
            stream_id: stream.clone(),
            item: InboundStreamItem::Gap(StreamGap {
                stream_id: stream,
                first_missing: StreamSequence::new(5),
                last_missing: StreamSequence::new(4),
                reason: StreamGapReason::ReplayUnavailable,
                recovery: StreamRecoveryAction::RequestSnapshot,
            }),
        }),
        Err(WireError::InvalidGapRange)
    ));
}

#[test]
fn maximum_trace_context_is_included_in_the_published_envelope_reservation() {
    let identifier = "x".repeat(undercurrent_wire::MAX_IDENTIFIER_LEN);
    let context = TraceContext::new(
        TraceId::new(identifier.clone()).expect("trace ID"),
        SpanId::new(identifier.clone()).expect("span ID"),
    )
    .with_parent_span_id(SpanId::new(identifier.clone()).expect("parent span ID"))
    .with_correlation_id(CorrelationId::new(identifier.clone()).expect("correlation ID"));
    let message = ClientMessage::ResumeStream {
        request_id: RequestId::new(identifier.clone()).expect("request ID"),
        stream_id: StreamId::new(identifier.clone()).expect("stream ID"),
        server_epoch: ServerEpoch::new(identifier).expect("server epoch"),
        after: StreamSequence::new(u64::MAX),
        trace_context: context,
        payload: bytes(&[1]),
    };

    let exact = WireCodec::new(MAX_CLIENT_FRAME_ENVELOPE_BYTES + 1).expect("exact codec");
    let encoded = exact.encode_client(&message).expect("exact frame fits");
    assert_eq!(encoded.len(), MAX_CLIENT_FRAME_ENVELOPE_BYTES as usize + 1);

    let undersized = WireCodec::new(MAX_CLIENT_FRAME_ENVELOPE_BYTES).expect("undersized codec");
    assert!(matches!(
        undersized.encode_client(&message),
        Err(WireError::FrameTooLarge {
            actual,
            maximum
        }) if actual == u64::from(MAX_CLIENT_FRAME_ENVELOPE_BYTES) + 1
            && maximum == MAX_CLIENT_FRAME_ENVELOPE_BYTES
    ));
}

#[test]
fn message_kind_registry_is_unique_stable_and_directional() {
    let fixture = contract_fixture();
    let codes = WireMessageKind::ALL
        .map(WireMessageKind::code)
        .into_iter()
        .collect::<BTreeSet<_>>();
    let names = WireMessageKind::ALL
        .map(WireMessageKind::as_str)
        .into_iter()
        .collect::<BTreeSet<_>>();

    assert_eq!(codes.len(), WireMessageKind::ALL.len());
    assert_eq!(names.len(), WireMessageKind::ALL.len());
    assert_eq!(
        WireMessageKind::ALL
            .map(|kind| (kind.as_str().to_owned(), kind.code()))
            .as_slice(),
        fixture.message_kinds.as_slice()
    );
    for (name, code) in &fixture.message_kinds {
        let direction = if *code < 128 {
            WireDirection::ClientToServer
        } else {
            WireDirection::ServerToClient
        };
        let kind = WireMessageKind::from_code(*code, direction).expect("registered code");
        assert_eq!(kind.as_str(), name);
        let opposite = match direction {
            WireDirection::ClientToServer => WireDirection::ServerToClient,
            WireDirection::ServerToClient => WireDirection::ClientToServer,
        };
        assert_eq!(WireMessageKind::from_code(*code, opposite), None);
    }
    assert_eq!(
        WireMessageKind::from_code(1, WireDirection::ClientToServer),
        Some(WireMessageKind::Request)
    );
    assert_eq!(
        WireMessageKind::from_code(1, WireDirection::ServerToClient),
        None
    );
}

#[test]
fn encoded_wire_header_matches_fixed_contract_fixture() {
    let fixture = contract_fixture();
    let encoded = codec()
        .encode_client(&ClientMessage::Ping)
        .expect("ping encodes");

    assert_eq!(&encoded[..4], fixture.magic.as_bytes());
    assert_eq!(encoded[4], fixture.version.len() as u8);
    assert_eq!(encoded.len(), fixture.header_bytes);
    assert_eq!(
        u32::from_be_bytes(encoded[8..12].try_into().expect("body length")),
        0
    );
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ContractFixture {
    magic: String,
    version: String,
    header_bytes: usize,
    message_kinds: Vec<(String, u8)>,
}

fn contract_fixture() -> ContractFixture {
    serde_json::from_str(include_str!("fixtures/ucmp-contract.json"))
        .expect("fixed current UCMP contract")
}

fn codec() -> WireCodec {
    WireCodec::new(1_024).expect("codec")
}

fn complete<T>(status: DecodeStatus<T>) -> undercurrent_wire::DecodedFrame<T> {
    match status {
        DecodeStatus::Complete(frame) => frame,
        DecodeStatus::NeedMore { .. } => panic!("expected complete frame"),
    }
}

fn raw_frame(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut frame = vec![b'U', b'C', b'M', b'P', 10, kind, 0, 0];
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(b"0.1.0-beta");
    frame.extend_from_slice(body);
    frame
}

fn request_id(index: usize) -> RequestId {
    RequestId::new(format!("request-{index}")).expect("request ID")
}

fn stream_id(index: usize) -> StreamId {
    StreamId::new(format!("stream-{index}")).expect("stream ID")
}

fn trace_context(index: usize) -> TraceContext {
    TraceContext::new(
        TraceId::new(format!("trace-{index}")).expect("trace ID"),
        SpanId::new(format!("span-{index}")).expect("span ID"),
    )
    .with_parent_span_id(SpanId::new(format!("parent-{index}")).expect("parent span ID"))
    .with_correlation_id(
        CorrelationId::new(format!("correlation-{index}")).expect("correlation ID"),
    )
}

fn bytes(value: &[u8]) -> Arc<[u8]> {
    Arc::from(value)
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
