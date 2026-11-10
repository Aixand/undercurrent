// SPDX-License-Identifier: Apache-2.0

//! Exercise real UCMP encoding, bounded scheduling, and consumed-checkpoint replay.
//! Transport success is simulated by copying a selected frame into a local buffer.
//! Run from the repository root; no daemon, sockets, or persistent state are used.

use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use undercurrent_multiplex::{
    ConnectionRouter, DeliverySemantics, InboundStreamItem, OutboundFrame, OutboundScheduler,
    ReplayJournal, ReplayLimits, ReplayResult, ReplayStreamKey, ScheduledFrame, SchedulerLimits,
    SchedulerMessageLimits, ServerMessage, StreamKind, StreamRecoveryAction, TrafficClass,
};
use undercurrent_wire::{
    ClientId, ConnectionLimits, DecodeStatus, ServerEpoch, StreamId, StreamSequence, WireCodec,
};

fn main() -> Result<(), Box<dyn Error>> {
    let codec = WireCodec::new(512)?;
    let stream_id = StreamId::new("example-output")?;
    let epoch = ServerEpoch::new("example-epoch")?;
    let key = ReplayStreamKey::new(ClientId::new("example-client")?, stream_id.clone());
    let recovery = StreamRecoveryAction::RestartFromAvailableOffset;

    let mut frames = Vec::new();
    for sequence in 1..=2 {
        let sequence = StreamSequence::new(sequence);
        let encoded = codec.encode_server(&ServerMessage::StreamItem {
            stream_id: stream_id.clone(),
            item: InboundStreamItem::Data {
                sequence,
                bytes: Arc::from(&b"hello\n"[..]),
                // The decoder derives the full frame charge from encoded bytes.
                flow_controlled_bytes: 0,
            },
        })?;
        frames.push(OutboundFrame::stream(
            TrafficClass::Bulk,
            stream_id.clone(),
            sequence,
            DeliverySemantics::Reliable,
            Arc::from(encoded),
            512,
        )?);
    }
    let frame_bytes = frames[0].encoded_len();
    assert_eq!(frames[1].encoded_len(), frame_bytes);
    let negotiated = ConnectionLimits::new(512, 2, 2, 4096, 1024, frame_bytes)?;
    let scheduler = OutboundScheduler::new(SchedulerLimits::new(
        negotiated,
        [1024; 4],
        frame_bytes,
        512,
        SchedulerMessageLimits::new(16, 4, [4; 4])?,
    )?);
    let router = ConnectionRouter::new(negotiated);
    let stream = router.open_stream(stream_id.clone(), 4, 1024)?;
    scheduler.open_stream(stream_id.clone(), StreamKind::TerminalOutput, recovery)?;
    let journal = ReplayJournal::new(epoch.clone(), ReplayLimits::new(2, 1024, 2048, 512)?);
    journal.register_stream(key.clone(), recovery)?;
    for frame in frames {
        scheduler.enqueue(frame)?;
    }
    assert_eq!(scheduler.pressure().queued_messages, 2);

    // In a real adapter, record only after a complete successful transport write.
    let first = take_data(&scheduler)?;
    let written = first.bytes().to_vec();
    journal.record(
        &key,
        first.sequence().expect("stream sequence"),
        Arc::from(written.clone()),
    )?;
    route_encoded(&codec, &router, &written)?;
    assert_eq!(stream.last_sequence(), StreamSequence::INITIAL);
    assert_eq!(scheduler.dequeue()?, None);
    println!("second frame waits for consumed credit");

    let consumed = stream.recv_timeout(Duration::from_secs(1))?;
    assert_eq!(consumed.sequence(), StreamSequence::new(1));
    scheduler.grant_consumed_credit(&stream_id, consumed.encoded_len())?;
    println!("consuming sequence 1 returns credit");

    let second = take_data(&scheduler)?;
    let written = second.bytes().to_vec();
    journal.record(
        &key,
        second.sequence().expect("stream sequence"),
        Arc::from(written.clone()),
    )?;
    route_encoded(&codec, &router, &written)?;
    // Disconnect before the consumer receives sequence 2.
    router.close();
    scheduler.close();
    assert_eq!(stream.last_sequence(), StreamSequence::new(1));
    let now = Instant::now();
    assert!(journal.deactivate_stream(&key, now + Duration::from_secs(60)));
    let activation =
        journal.activate_resume(&epoch, key.clone(), stream.last_sequence(), recovery, now)?;
    let ReplayResult::Items(items) = activation.replay else {
        panic!("this retained in-memory example must replay");
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].sequence, StreamSequence::new(2));

    let replacement = ConnectionRouter::new(negotiated);
    let resumed = replacement.open_stream_after(stream_id, 4, 1024, stream.last_sequence())?;
    route_encoded(&codec, &replacement, &items[0].bytes)?;
    assert_eq!(
        resumed.recv_timeout(Duration::from_secs(1))?.sequence(),
        StreamSequence::new(2)
    );
    println!("resume after consumed sequence 1 delivers sequence 2");

    replacement.close();
    journal.remove_stream(&key);
    assert_eq!(journal.pressure().streams, 0);
    println!("all connection and journal entries released");
    Ok(())
}

fn take_data(scheduler: &OutboundScheduler) -> Result<OutboundFrame, Box<dyn Error>> {
    match scheduler.dequeue()? {
        Some(ScheduledFrame::Data(frame)) => Ok(frame),
        _ => Err("expected one writable data frame".into()),
    }
}

fn route_encoded(
    codec: &WireCodec,
    router: &ConnectionRouter,
    bytes: &[u8],
) -> Result<(), Box<dyn Error>> {
    let DecodeStatus::Complete(decoded) = codec.decode_server(bytes)? else {
        return Err("expected a complete frame".into());
    };
    assert_eq!(decoded.consumed, bytes.len());
    let ServerMessage::StreamItem { stream_id, item } = decoded.message else {
        return Err("expected a stream item".into());
    };
    router.route_stream_item(&stream_id, item)?;
    Ok(())
}
