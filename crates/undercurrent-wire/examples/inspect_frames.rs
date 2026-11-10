// SPDX-License-Identifier: Apache-2.0

//! Exercise negotiation, incremental decoding, and binary credit accounting.
//! Runs entirely in memory; the payloads are opaque codec samples, not product requests.

use std::collections::BTreeSet;
use std::error::Error;
use std::sync::Arc;

use undercurrent_wire::{
    CURRENT_PROTOCOL_VERSION, ClientId, ClientMessage, ConnectionHello, ConnectionId,
    ConnectionLimits, ConnectionPolicy, DecodeStatus, HandshakeCodec, HandshakeDecodeStatus,
    HandshakeResponse, HeartbeatPolicy, InboundStreamItem, NamespacedName, RequestId, ServerEpoch,
    ServerMessage, SpanId, StreamId, StreamSequence, TraceContext, TraceId, WireCodec,
    connection_capabilities, negotiate_connection,
};

fn main() -> Result<(), Box<dyn Error>> {
    let limits = ConnectionLimits::new(4_096, 8, 8, 65_536, 8_192, 4_096)?;
    let capabilities = BTreeSet::from([
        NamespacedName::new(connection_capabilities::MULTIPLEX)?,
        NamespacedName::new(connection_capabilities::FLOW_CONTROL)?,
    ]);
    let hello = ConnectionHello::new(
        ClientId::new("example-client")?,
        CURRENT_PROTOCOL_VERSION,
        capabilities.clone(),
        BTreeSet::new(),
        limits,
    )?;
    let policy = ConnectionPolicy::new(
        CURRENT_PROTOCOL_VERSION,
        capabilities,
        limits,
        HeartbeatPolicy::new(1_000, 3_000)?,
        None,
    )?;
    let handshake = HandshakeCodec::new(8_192)?;
    let hello_bytes = handshake.encode_hello(&hello)?;
    let HandshakeDecodeStatus::Complete(decoded) = handshake.decode_hello(&hello_bytes)? else {
        return Err("complete hello expected".into());
    };
    assert_eq!(decoded.consumed, hello_bytes.len());
    let agreement = negotiate_connection(
        &decoded.message,
        &policy,
        ConnectionId::new("example-connection")?,
        ServerEpoch::new("example-epoch")?,
    )?;
    let accepted = handshake.encode_response(&HandshakeResponse::Accepted(agreement))?;
    let HandshakeDecodeStatus::Complete(decoded) = handshake.decode_response(&accepted)? else {
        return Err("complete acceptance expected".into());
    };
    assert_eq!(decoded.consumed, accepted.len());
    let HandshakeResponse::Accepted(agreement) = decoded.message else {
        return Err("accepted agreement expected".into());
    };
    println!(
        "negotiated protocol {}; maximum frame {} bytes",
        agreement.protocol_version(),
        agreement.limits().max_frame_bytes()
    );

    let codec = WireCodec::new(agreement.limits().max_frame_bytes())?;
    let request = ClientMessage::Request {
        request_id: RequestId::new("example-request")?,
        trace_context: TraceContext::new(TraceId::new("example-trace")?, SpanId::new("span-1")?),
        payload: Arc::from(&b"opaque request bytes"[..]),
    };
    let encoded = codec.encode_client(&request)?;
    let mut input = encoded[..7].to_vec();
    assert_eq!(
        codec.decode_client(&input)?,
        DecodeStatus::NeedMore { minimum_total: 12 }
    );
    input.extend_from_slice(&encoded[7..]);
    input.extend_from_slice(&codec.encode_client(&ClientMessage::Ping)?);
    let DecodeStatus::Complete(first) = codec.decode_client(&input)? else {
        return Err("complete request expected".into());
    };
    assert_eq!(first.message, request);
    let DecodeStatus::Complete(second) = codec.decode_client(&input[first.consumed..])? else {
        return Err("complete ping expected".into());
    };
    assert_eq!(second.message, ClientMessage::Ping);
    assert_eq!(first.consumed + second.consumed, input.len());
    println!("decoded fragmented request and following ping");

    let binary: Arc<[u8]> = Arc::from(&[0_u8, 255, 27, 10][..]);
    let message = ServerMessage::StreamItem {
        stream_id: StreamId::new("example-stream")?,
        item: InboundStreamItem::Data {
            sequence: StreamSequence::INITIAL.next()?,
            bytes: binary.clone(),
            // Decoder computes this metadata from the complete frame length.
            flow_controlled_bytes: 0,
        },
    };
    let encoded = codec.encode_server(&message)?;
    let DecodeStatus::Complete(decoded) = codec.decode_server(&encoded)? else {
        return Err("complete data frame expected".into());
    };
    let ServerMessage::StreamItem { item, .. } = decoded.message else {
        return Err("stream item expected".into());
    };
    assert_eq!(item.encoded_len(), encoded.len() as u64);
    let InboundStreamItem::Data { bytes, .. } = item else {
        return Err("data expected".into());
    };
    assert_eq!(bytes, binary);
    println!("binary payload preserved; credit includes the full frame");
    Ok(())
}
