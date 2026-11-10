// SPDX-License-Identifier: Apache-2.0

use super::*;

fn scheduler() -> OutboundScheduler {
    let connection = ConnectionLimits::new(64, 4, 4, 1_024, 256, 64).unwrap();
    OutboundScheduler::new(
        SchedulerLimits::new(
            connection,
            [256; 4],
            64,
            128,
            SchedulerMessageLimits::new(128, 32, [32; 4]).unwrap(),
        )
        .unwrap(),
    )
}

fn open(scheduler: &OutboundScheduler, name: &str) -> StreamId {
    let stream = StreamId::new(name).unwrap();
    scheduler
        .open_stream(
            stream.clone(),
            StreamKind::SessionQuery,
            StreamRecoveryAction::RestartQuery,
        )
        .unwrap();
    stream
}

fn frame(
    stream: &StreamId,
    sequence: u64,
    semantics: DeliverySemantics,
    size: usize,
) -> OutboundFrame {
    OutboundFrame::stream(
        TrafficClass::State,
        stream.clone(),
        StreamSequence::new(sequence),
        semantics,
        Arc::from(vec![0; size]),
        64,
    )
    .unwrap()
}

#[test]
fn closing_a_stream_reclaims_only_its_unreturned_connection_credit() {
    let scheduler = scheduler();
    let initial = scheduler.pressure().connection_credit_bytes;
    let stream = open(&scheduler, "closing");
    scheduler
        .enqueue(frame(&stream, 1, DeliverySemantics::Reliable, 32))
        .unwrap();
    assert!(scheduler.dequeue().unwrap().is_some());
    assert_eq!(scheduler.pressure().connection_credit_bytes, initial - 32);
    scheduler.grant_consumed_credit(&stream, 12).unwrap();
    scheduler.close_stream(&stream).unwrap();
    assert_eq!(scheduler.pressure().connection_credit_bytes, initial);
}

#[test]
fn credit_blocked_replacement_keeps_one_route_entry_and_preserves_peer_service() {
    let scheduler = scheduler();
    let first = open(&scheduler, "first");
    let second = open(&scheduler, "second");
    scheduler
        .enqueue(frame(&first, 1, DeliverySemantics::Reliable, 64))
        .unwrap();
    scheduler.dequeue().unwrap().unwrap();
    assert_eq!(scheduler.pressure().connection_credit_bytes, 0);
    scheduler
        .enqueue(frame(&first, 2, DeliverySemantics::Replaceable, 16))
        .unwrap();
    scheduler
        .enqueue(frame(&second, 1, DeliverySemantics::Reliable, 16))
        .unwrap();
    for sequence in 3..=1_002 {
        scheduler
            .enqueue(frame(&first, sequence, DeliverySemantics::Replaceable, 16))
            .unwrap();
    }
    {
        let state = scheduler.lock_state();
        let class = &state.classes[TrafficClass::State.index()];
        assert_eq!(class.route_order.len(), 2, "one scheduling entry per route");
        assert_eq!(class.routes.len(), 2);
        assert_eq!(class.messages, 2);
        assert_eq!(class.bytes, 32);
    }
    assert!(scheduler.dequeue().unwrap().is_none());
    scheduler.grant_consumed_credit(&first, 32).unwrap();
    for expected in [&first, &second] {
        let ScheduledFrame::Data(frame) = scheduler.dequeue().unwrap().unwrap() else {
            panic!("data");
        };
        assert_eq!(frame.route(), &FrameRoute::Stream(expected.clone()));
    }
    let state = scheduler.lock_state();
    let class = &state.classes[TrafficClass::State.index()];
    assert!(class.routes.is_empty());
    assert!(class.route_order.is_empty());
}

#[test]
fn replacement_preserves_reliable_neighbors_and_close_removes_the_route() {
    let scheduler = scheduler();
    let stream = open(&scheduler, "stream");
    for (sequence, semantics) in [
        (1, DeliverySemantics::Replaceable),
        (2, DeliverySemantics::Reliable),
        (3, DeliverySemantics::Replaceable),
        (4, DeliverySemantics::Reliable),
        (5, DeliverySemantics::Replaceable),
    ] {
        scheduler
            .enqueue(frame(&stream, sequence, semantics, 16))
            .unwrap();
    }
    {
        let state = scheduler.lock_state();
        let class = &state.classes[TrafficClass::State.index()];
        assert_eq!(class.route_order.len(), 1);
        let queue = class
            .routes
            .get(&FrameRoute::Stream(stream.clone()))
            .unwrap();
        assert_eq!(
            queue
                .iter()
                .map(|frame| frame.sequence().unwrap().value())
                .collect::<Vec<_>>(),
            vec![2, 4, 5]
        );
    }
    let released = scheduler.close_stream(&stream).unwrap();
    assert_eq!(released.queued_messages, 3);
    let state = scheduler.lock_state();
    let class = &state.classes[TrafficClass::State.index()];
    assert!(class.routes.is_empty());
    assert!(class.route_order.is_empty());
}

#[test]
fn separated_overflow_gaps_do_not_cover_an_admitted_frame() {
    let scheduler = scheduler();
    let stream = open(&scheduler, "stream");
    for sequence in 1..=4 {
        scheduler
            .enqueue(frame(&stream, sequence, DeliverySemantics::Reliable, 64))
            .unwrap();
    }
    scheduler
        .enqueue(frame(&stream, 5, DeliverySemantics::Droppable, 64))
        .unwrap();
    // Free one slot, admit sequence 6, then overflow again at sequence 7.
    scheduler.dequeue().unwrap().unwrap();
    scheduler
        .enqueue(frame(&stream, 6, DeliverySemantics::Reliable, 64))
        .unwrap();
    scheduler
        .enqueue(frame(&stream, 7, DeliverySemantics::Droppable, 64))
        .unwrap();
    scheduler.grant_consumed_credit(&stream, 256).unwrap();
    for expected in 2..=7 {
        let selected = scheduler.dequeue().unwrap().expect("next ordered item");
        match selected {
            ScheduledFrame::Gap(gap) => {
                assert!([5, 7].contains(&expected));
                assert_eq!(gap.first_missing.value(), expected);
                assert_eq!(gap.last_missing.value(), expected);
            }
            ScheduledFrame::Data(frame) => {
                assert!(![5, 7].contains(&expected));
                assert_eq!(frame.sequence().unwrap().value(), expected);
            }
        }
    }
}

#[test]
fn adjacent_gaps_with_different_reasons_remain_distinct() {
    let scheduler = scheduler();
    let stream = open(&scheduler, "stream");
    for sequence in 1..=4 {
        scheduler
            .enqueue(frame(&stream, sequence, DeliverySemantics::Reliable, 64))
            .unwrap();
    }
    scheduler
        .enqueue(frame(&stream, 5, DeliverySemantics::Droppable, 64))
        .unwrap();
    let source_gap = scheduler
        .record_source_gap(&stream, StreamSequence::new(6))
        .unwrap();
    assert_eq!(source_gap.reason, StreamGapReason::SourceRetentionEvicted);
    assert_eq!(source_gap.first_missing.value(), 6);
}

#[test]
fn gap_capacity_rejection_preserves_sequence_and_releases_on_drain_or_close() {
    let connection = ConnectionLimits::new(64, 4, 4, 1_024, 256, 64).unwrap();
    let scheduler = OutboundScheduler::new(
        SchedulerLimits::new(
            connection,
            [256; 4],
            64,
            128,
            SchedulerMessageLimits::new(4, 2, [1; 4]).unwrap(),
        )
        .unwrap(),
    );
    let first = open(&scheduler, "first");
    let second = open(&scheduler, "second");
    let third = open(&scheduler, "third");
    for stream in [&first, &second] {
        scheduler
            .record_source_gap(stream, StreamSequence::new(1))
            .unwrap();
        scheduler
            .record_source_gap(stream, StreamSequence::new(3))
            .unwrap();
    }
    assert!(matches!(
        scheduler.record_source_gap(&first, StreamSequence::new(5)),
        Err(MultiplexError::QueueMessageSaturated {
            queue: "stream_gaps",
            ..
        })
    ));
    assert_eq!(
        scheduler
            .stream_pressure(&first)
            .unwrap()
            .last_sequence
            .value(),
        3
    );
    assert!(matches!(
        scheduler.record_source_gap(&third, StreamSequence::new(1)),
        Err(MultiplexError::QueueMessageSaturated {
            queue: "connection_gaps",
            ..
        })
    ));
    assert_eq!(
        scheduler.stream_pressure(&third).unwrap().last_sequence,
        StreamSequence::INITIAL
    );
    // Adjacent losses can extend an existing interval even at capacity.
    scheduler
        .record_source_gap(&first, StreamSequence::new(4))
        .unwrap();
    scheduler.dequeue().unwrap().unwrap();
    scheduler
        .record_source_gap(&third, StreamSequence::new(1))
        .unwrap();
    scheduler.close_stream(&second).unwrap();
    scheduler
        .record_source_gap(&third, StreamSequence::new(3))
        .unwrap();
    assert_eq!(scheduler.lock_state().pending_gap_count, 3);
    scheduler.close();
    let state = scheduler.lock_state();
    assert_eq!(state.pending_gap_count, 0);
    assert!(state.pending_gaps.is_empty());
}

#[test]
fn pending_gap_blocks_later_control_stream_data_while_older_data_waits_for_credit() {
    let scheduler = scheduler();
    let stream = open(&scheduler, "stream");
    scheduler
        .enqueue(frame(&stream, 1, DeliverySemantics::Reliable, 64))
        .unwrap();
    scheduler.dequeue().unwrap().unwrap();
    scheduler
        .enqueue(frame(&stream, 2, DeliverySemantics::Reliable, 16))
        .unwrap();
    scheduler
        .record_source_gap(&stream, StreamSequence::new(3))
        .unwrap();
    scheduler
        .enqueue(
            OutboundFrame::stream(
                TrafficClass::Control,
                stream.clone(),
                StreamSequence::new(4),
                DeliverySemantics::Reliable,
                Arc::from(vec![0; 16]),
                64,
            )
            .unwrap(),
        )
        .unwrap();
    assert!(scheduler.dequeue().unwrap().is_none());
    scheduler.grant_consumed_credit(&stream, 16).unwrap();
    for sequence in 2..=4 {
        let actual = match scheduler.dequeue().unwrap().unwrap() {
            ScheduledFrame::Data(frame) => frame.sequence().unwrap(),
            ScheduledFrame::Gap(gap) => gap.last_missing,
        };
        assert_eq!(actual.value(), sequence);
    }
}

#[test]
fn encoded_replay_gap_remains_writable_with_both_credit_windows_empty() {
    let scheduler = scheduler();
    let stream = open(&scheduler, "stream");
    scheduler
        .enqueue(frame(&stream, 1, DeliverySemantics::Reliable, 64))
        .unwrap();
    scheduler.dequeue().unwrap().unwrap();
    let gap = StreamGap {
        stream_id: stream.clone(),
        first_missing: StreamSequence::new(2),
        last_missing: StreamSequence::new(2),
        reason: StreamGapReason::SourceRetentionEvicted,
        recovery: StreamRecoveryAction::RestartQuery,
    };
    let encoded = undercurrent_wire::WireCodec::new(64)
        .unwrap()
        .encode_server(&crate::ServerMessage::StreamItem {
            stream_id: stream.clone(),
            item: crate::InboundStreamItem::Gap(gap),
        })
        .unwrap();
    scheduler
        .enqueue(
            OutboundFrame::gap(
                TrafficClass::State,
                stream.clone(),
                StreamSequence::new(2),
                Arc::from(encoded),
                crate::ReplayDisposition::AlreadyRetained,
                64,
            )
            .unwrap(),
        )
        .unwrap();
    scheduler
        .enqueue(frame(&stream, 3, DeliverySemantics::Reliable, 16))
        .unwrap();
    let ScheduledFrame::Data(frame) = scheduler
        .dequeue()
        .unwrap()
        .expect("gap bypasses empty credit")
    else {
        panic!("encoded gap");
    };
    assert_eq!(frame.sequence().unwrap().value(), 2);
    assert_eq!(frame.flow_controlled_bytes(), 0);
    assert_eq!(scheduler.pressure().connection_credit_bytes, 0);
    assert_eq!(scheduler.stream_pressure(&stream).unwrap().credit_bytes, 0);
    assert!(
        scheduler.dequeue().unwrap().is_none(),
        "ordinary data still requires credit"
    );
}
