// SPDX-License-Identifier: Apache-2.0
use futures_util::{SinkExt, StreamExt};
use std::{io::ErrorKind, sync::Arc, time::Duration};
use tokio::io::AsyncWriteExt;
use undercurrent_transport_local::LocalFrameStream;
use undercurrent_wire::{ClientMessage, RequestId, SpanId, TraceContext, TraceId, WireCodec};

#[tokio::test]
async fn fragmented_and_coalesced_frames_survive_cancelled_receive() {
    let (mut peer, stream) = tokio::io::duplex(128);
    let frame = WireCodec::new(128)
        .unwrap()
        .encode_client(&ClientMessage::Request {
            request_id: RequestId::new("r").unwrap(),
            trace_context: TraceContext::new(TraceId::new("t").unwrap(), SpanId::new("s").unwrap()),
            payload: Arc::from(&b"binary\0\xffpayload"[..]),
        })
        .unwrap();
    let mut reader = LocalFrameStream::new(stream, 128).unwrap();
    peer.write_all(&frame[..5]).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(5), reader.next())
            .await
            .is_err()
    );
    peer.write_all(&frame[5..15]).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(5), reader.next())
            .await
            .is_err()
    );
    peer.write_all(&frame[15..]).await.unwrap();
    peer.write_all(&frame).await.unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), frame);
    assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), frame);
    drop(peer);
    assert!(reader.next().await.is_none());
}

#[tokio::test]
async fn oversized_header_is_rejected_before_body_allocation() {
    let (mut peer, stream) = tokio::io::duplex(128);
    let mut reader = LocalFrameStream::new(stream, 128).unwrap();
    peer.write_all(b"UCMP\x0a\x01\x00\x00\xff\xff\xff\xff0.1.0-beta")
        .await
        .unwrap();
    assert_eq!(
        reader.next().await.unwrap().unwrap_err().kind(),
        ErrorKind::InvalidData
    );
    assert!(reader.next().await.is_none());
}

#[tokio::test]
async fn truncated_frame_is_not_a_clean_disconnect() {
    let (mut peer, stream) = tokio::io::duplex(128);
    let mut reader = LocalFrameStream::new(stream, 128).unwrap();
    peer.write_all(b"UCM").await.unwrap();
    drop(peer);
    assert_eq!(
        reader.next().await.unwrap().unwrap_err().kind(),
        ErrorKind::UnexpectedEof
    );
}

#[tokio::test]
async fn writes_cross_a_tiny_pipe_without_frame_corruption() {
    let (a, b) = tokio::io::duplex(1);
    let mut writer = LocalFrameStream::new(a, 128).unwrap();
    let mut reader = LocalFrameStream::new(b, 128).unwrap();
    let expected: Arc<[u8]> = WireCodec::new(128)
        .unwrap()
        .encode_client(&ClientMessage::Ping)
        .unwrap()
        .into();
    let sent = expected.clone();
    let task = tokio::spawn(async move {
        writer.send(sent).await.unwrap();
        writer.close().await.unwrap();
    });
    assert_eq!(reader.next().await.unwrap().unwrap(), expected);
    assert!(reader.next().await.is_none());
    task.await.unwrap();
}
