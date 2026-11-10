// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::Instant;

use undercurrent_multiplex::{
    ConnectionRouter, DeliverySemantics, EnqueueOutcome, FrameRoute, InboundStreamError,
    InboundStreamItem, MultiplexDiagnosticName, MultiplexError, MultiplexMetricName,
    MultiplexMetricObservation, MultiplexRetryGuidance, MultiplexTelemetry, OutboundFrame,
    OutboundScheduler, PendingResponseError, ReplayJournal, ReplayLimits, ReplayResult,
    ReplayStreamKey, ScheduledFrame, SchedulerLimits, StreamGapReason, StreamKind,
    StreamRecoveryAction, TrafficClass,
};
use undercurrent_wire::{
    ClientId, ConnectionLimits, RequestId, ServerEpoch, StreamId, StreamSequence,
};

#[test]
fn encoded_frames_enforce_the_negotiated_maximum_before_queueing() {
    let error = OutboundFrame::control(bytes(65), 64).expect_err("oversized frame rejected");

    assert!(matches!(
        error,
        MultiplexError::InvalidFrameSize {
            actual: 65,
            maximum: 64
        }
    ));
    assert!(OutboundFrame::control(bytes(0), 64).is_err());
}

#[test]
fn one_scheduler_supports_three_hundred_streams_and_rejects_active_id_reuse() {
    let scheduler = scheduler(400, 64, 256, [128, 256, 256, 384], 256, 128);
    for index in 0..300 {
        scheduler
            .open_stream(
                stream_id(index),
                StreamKind::TerminalOutput,
                StreamRecoveryAction::RestartFromAvailableOffset,
            )
            .expect("stream opens");
    }

    assert_eq!(scheduler.pressure().active_streams, 300);
    assert!(matches!(
        scheduler.open_stream(
            stream_id(0),
            StreamKind::TerminalOutput,
            StreamRecoveryAction::RestartFromAvailableOffset,
        ),
        Err(MultiplexError::StreamIdInUse { .. })
    ));
}

#[test]
fn bulk_saturation_preserves_control_reservation_and_reports_a_gap() {
    let scheduler = scheduler(4, 64, 100, [80, 80, 80, 60], 100, 64);
    let stream = stream_id(1);
    scheduler
        .open_stream(
            stream.clone(),
            StreamKind::TerminalOutput,
            StreamRecoveryAction::RestartFromAvailableOffset,
        )
        .expect("stream opens");
    scheduler
        .enqueue(stream_frame(
            TrafficClass::Bulk,
            &stream,
            1,
            DeliverySemantics::Droppable,
            60,
            64,
        ))
        .expect("first output queued");
    let outcome = scheduler
        .enqueue(stream_frame(
            TrafficClass::Bulk,
            &stream,
            2,
            DeliverySemantics::Droppable,
            60,
            64,
        ))
        .expect("overflow becomes a gap");
    assert!(matches!(outcome, EnqueueOutcome::Dropped { .. }));

    scheduler
        .enqueue(OutboundFrame::response(request_id(1), bytes(40), 64).expect("response frame"))
        .expect("reserved control queue accepts response");
    let pressure = scheduler.pressure();
    assert_eq!(pressure.class_queued_bytes[TrafficClass::Bulk as usize], 60);
    assert_eq!(
        pressure.class_queued_bytes[TrafficClass::Control as usize],
        40
    );
    let stream_pressure = scheduler.stream_pressure(&stream).expect("stream pressure");
    assert_eq!(stream_pressure.queued_messages, 1);
    assert_eq!(stream_pressure.dropped_items, 1);
    assert_eq!(stream_pressure.coalesced_items, 0);
    assert!(stream_pressure.gap_pending);

    let response = scheduler.dequeue().expect("dequeue").expect("response");
    assert!(matches!(
        response,
        ScheduledFrame::Data(ref frame)
            if frame.route() == &undercurrent_multiplex::FrameRoute::Request(request_id(1))
    ));
    let prior_output = scheduler.dequeue().expect("dequeue").expect("prior output");
    assert!(matches!(
        prior_output,
        ScheduledFrame::Data(ref frame)
            if frame.sequence() == Some(StreamSequence::new(1))
    ));
    let gap = scheduler.dequeue().expect("dequeue").expect("gap");
    assert!(matches!(
        gap,
        ScheduledFrame::Gap(ref gap)
            if gap.first_missing == StreamSequence::new(2)
                && gap.last_missing == StreamSequence::new(2)
                && gap.reason == StreamGapReason::LiveQueueOverflow
    ));
}

#[test]
fn saturated_terminal_consumer_is_bounded_and_fair_to_control_lifecycle_and_peer_streams() {
    let scheduler = scheduler(4, 64, 128, [64, 64, 64, 256], 448, 64);
    let slow_terminal = stream_id(1);
    let lifecycle = stream_id(2);
    let peer_terminal = stream_id(3);
    for (stream, kind, recovery) in [
        (
            &slow_terminal,
            StreamKind::TerminalOutput,
            StreamRecoveryAction::RestartFromAvailableOffset,
        ),
        (
            &lifecycle,
            StreamKind::SessionEvents,
            StreamRecoveryAction::RestartQuery,
        ),
        (
            &peer_terminal,
            StreamKind::TerminalOutput,
            StreamRecoveryAction::RestartFromAvailableOffset,
        ),
    ] {
        scheduler
            .open_stream(stream.clone(), kind, recovery)
            .expect("stream opens");
    }

    for sequence in 1..=2 {
        assert_eq!(
            scheduler
                .enqueue(stream_frame(
                    TrafficClass::Bulk,
                    &slow_terminal,
                    sequence,
                    DeliverySemantics::Droppable,
                    64,
                    64,
                ))
                .expect("slow output reaches its exact stream budget"),
            EnqueueOutcome::Queued
        );
    }
    assert!(matches!(
        scheduler
            .enqueue(stream_frame(
                TrafficClass::Bulk,
                &slow_terminal,
                3,
                DeliverySemantics::Droppable,
                64,
                64,
            ))
            .expect("overflow is converted into an explicit gap"),
        EnqueueOutcome::Dropped { ref gap }
            if gap.first_missing == StreamSequence::new(3)
                && gap.last_missing == StreamSequence::new(3)
    ));
    scheduler
        .enqueue(stream_frame(
            TrafficClass::Bulk,
            &peer_terminal,
            1,
            DeliverySemantics::Droppable,
            64,
            64,
        ))
        .expect("peer terminal keeps its independent stream reservation");
    scheduler
        .enqueue(stream_frame(
            TrafficClass::Lifecycle,
            &lifecycle,
            1,
            DeliverySemantics::Reliable,
            8,
            64,
        ))
        .expect("authoritative lifecycle traffic keeps its reservation");
    scheduler
        .enqueue(OutboundFrame::control(bytes(8), 64).expect("connection control frame"))
        .expect("connection control keeps its reservation");
    scheduler
        .enqueue(OutboundFrame::response(request_id(1), bytes(8), 64).expect("control response"))
        .expect("daemon response keeps its reservation");

    let pressure = scheduler.pressure();
    assert_eq!(pressure.queued_bytes, 216);
    assert_eq!(pressure.queued_messages, 6);
    assert_eq!(pressure.class_queued_bytes, [16, 8, 0, 192]);
    assert_eq!(pressure.class_queued_messages, [2, 1, 0, 3]);
    let slow_pressure = scheduler
        .stream_pressure(&slow_terminal)
        .expect("slow terminal pressure");
    assert_eq!(slow_pressure.queued_bytes, 128);
    assert_eq!(slow_pressure.queued_messages, 2);
    assert_eq!(slow_pressure.dropped_items, 1);
    assert!(slow_pressure.gap_pending);

    assert!(matches!(
        scheduler.dequeue().expect("connection control dequeues"),
        Some(ScheduledFrame::Data(ref frame))
            if frame.route() == &FrameRoute::Connection
                && frame.class() == TrafficClass::Control
    ));
    assert!(matches!(
        scheduler.dequeue().expect("daemon response dequeues"),
        Some(ScheduledFrame::Data(ref frame))
            if frame.route() == &FrameRoute::Request(request_id(1))
                && frame.class() == TrafficClass::Control
    ));
    assert!(matches!(
        scheduler.dequeue().expect("lifecycle dequeues"),
        Some(ScheduledFrame::Data(ref frame))
            if frame.route() == &FrameRoute::Stream(lifecycle.clone())
                && frame.class() == TrafficClass::Lifecycle
    ));
    assert!(matches!(
        scheduler.dequeue().expect("first slow frame dequeues"),
        Some(ScheduledFrame::Data(ref frame))
            if frame.route() == &FrameRoute::Stream(slow_terminal.clone())
                && frame.sequence() == Some(StreamSequence::new(1))
    ));
    assert!(matches!(
        scheduler.dequeue().expect("peer terminal receives a fair turn"),
        Some(ScheduledFrame::Data(ref frame))
            if frame.route() == &FrameRoute::Stream(peer_terminal.clone())
                && frame.sequence() == Some(StreamSequence::new(1))
    ));
    assert!(matches!(
        scheduler.dequeue().expect("second slow frame dequeues"),
        Some(ScheduledFrame::Data(ref frame))
            if frame.route() == &FrameRoute::Stream(slow_terminal.clone())
                && frame.sequence() == Some(StreamSequence::new(2))
    ));
    assert!(matches!(
        scheduler.dequeue().expect("visible overflow gap dequeues"),
        Some(ScheduledFrame::Gap(ref gap))
            if gap.first_missing == StreamSequence::new(3)
                && gap.last_missing == StreamSequence::new(3)
                && gap.reason == StreamGapReason::LiveQueueOverflow
    ));
    assert_eq!(scheduler.pressure().queued_bytes, 0);
    assert_eq!(scheduler.pressure().queued_messages, 0);
}

#[test]
fn tiny_reliable_frames_cannot_exceed_the_stream_message_budget() {
    let negotiated =
        ConnectionLimits::new(64, 4, 4, 4_096, 1_024, 1_024).expect("connection limits");
    let scheduler = OutboundScheduler::new(
        SchedulerLimits::new(
            negotiated,
            [1_024; 4],
            1_024,
            64,
            undercurrent_multiplex::SchedulerMessageLimits::new(8, 2, [2; 4])
                .expect("message limits"),
        )
        .expect("scheduler limits"),
    );
    let stream = stream_id(1);
    scheduler
        .open_stream(
            stream.clone(),
            StreamKind::TerminalOutput,
            StreamRecoveryAction::RestartFromAvailableOffset,
        )
        .expect("stream opens");
    for sequence in 1..=2 {
        scheduler
            .enqueue(stream_frame(
                TrafficClass::Bulk,
                &stream,
                sequence,
                DeliverySemantics::Reliable,
                1,
                64,
            ))
            .expect("frame enters message budget");
    }

    assert!(matches!(
        scheduler.enqueue(stream_frame(
            TrafficClass::Bulk,
            &stream,
            3,
            DeliverySemantics::Reliable,
            1,
            64,
        )),
        Err(MultiplexError::QueueMessageSaturated {
            queue: "stream",
            message_limit: 2,
            queued_messages: 2,
        })
    ));
    assert_eq!(scheduler.pressure().queued_messages, 2);
}

#[test]
fn source_retention_loss_advances_sequence_and_reports_configured_recovery() {
    let scheduler = scheduler(4, 64, 256, [128, 256, 256, 384], 256, 128);
    let stream = stream_id(1);
    scheduler
        .open_stream(
            stream.clone(),
            StreamKind::TerminalOutput,
            StreamRecoveryAction::RestartFromAvailableOffset,
        )
        .expect("stream opens");

    let gap = scheduler
        .record_source_gap(&stream, StreamSequence::new(1))
        .expect("source gap records");
    assert_eq!(gap.first_missing, StreamSequence::new(1));
    assert_eq!(gap.last_missing, StreamSequence::new(1));
    assert_eq!(gap.reason, StreamGapReason::SourceRetentionEvicted);
    assert_eq!(
        gap.recovery,
        StreamRecoveryAction::RestartFromAvailableOffset
    );
    assert_eq!(
        scheduler
            .stream_pressure(&stream)
            .expect("stream pressure")
            .last_sequence,
        StreamSequence::new(1)
    );

    let scheduled = scheduler.dequeue().expect("dequeue").expect("gap");
    assert!(matches!(
        scheduled,
        ScheduledFrame::Gap(ref scheduled_gap) if scheduled_gap == &gap
    ));
}

#[test]
fn consecutive_source_retention_gaps_coalesce_before_later_data() {
    let scheduler = scheduler(4, 64, 256, [128, 256, 256, 384], 256, 128);
    let stream = stream_id(1);
    scheduler
        .open_stream(
            stream.clone(),
            StreamKind::SessionEvents,
            StreamRecoveryAction::RequestSnapshot,
        )
        .expect("stream opens");

    scheduler
        .record_source_gap(&stream, StreamSequence::new(1))
        .expect("first gap records");
    let gap = scheduler
        .record_source_gap(&stream, StreamSequence::new(2))
        .expect("second gap coalesces");
    scheduler
        .enqueue(stream_frame(
            TrafficClass::Lifecycle,
            &stream,
            3,
            DeliverySemantics::Reliable,
            16,
            64,
        ))
        .expect("later data queues");

    assert_eq!(gap.first_missing, StreamSequence::new(1));
    assert_eq!(gap.last_missing, StreamSequence::new(2));
    assert!(matches!(
        scheduler.dequeue(),
        Ok(Some(ScheduledFrame::Gap(scheduled_gap)))
            if scheduled_gap.first_missing == StreamSequence::new(1)
                && scheduled_gap.last_missing == StreamSequence::new(2)
                && scheduled_gap.reason == StreamGapReason::SourceRetentionEvicted
    ));
    assert!(matches!(
        scheduler.dequeue(),
        Ok(Some(ScheduledFrame::Data(frame)))
            if frame.sequence() == Some(StreamSequence::new(3))
    ));
}

#[test]
fn queued_control_overtakes_a_pending_output_gap() {
    let scheduler = scheduler(4, 64, 32, [64, 64, 64, 16], 64, 128);
    let stream = stream_id(1);
    scheduler
        .open_stream(
            stream.clone(),
            StreamKind::TerminalOutput,
            StreamRecoveryAction::RestartFromAvailableOffset,
        )
        .expect("stream opens");
    assert!(matches!(
        scheduler
            .enqueue(stream_frame(
                TrafficClass::Bulk,
                &stream,
                1,
                DeliverySemantics::Droppable,
                16,
                64,
            ))
            .expect("first output queues"),
        EnqueueOutcome::Queued
    ));
    assert!(matches!(
        scheduler
            .enqueue(stream_frame(
                TrafficClass::Bulk,
                &stream,
                2,
                DeliverySemantics::Droppable,
                16,
                64,
            ))
            .expect("second output drops"),
        EnqueueOutcome::Dropped { .. }
    ));
    scheduler
        .enqueue(OutboundFrame::control(bytes(8), 64).expect("control frame"))
        .expect("control queues");

    assert!(matches!(
        scheduler.dequeue(),
        Ok(Some(ScheduledFrame::Data(frame)))
            if frame.class() == TrafficClass::Control
                && frame.route() == &FrameRoute::Connection
    ));
}

#[test]
fn replaceable_state_coalesces_only_when_the_replacement_fits() {
    let scheduler = scheduler(4, 64, 128, [64, 64, 50, 78], 128, 64);
    let stream = stream_id(1);
    scheduler
        .open_stream(
            stream.clone(),
            StreamKind::SessionQuery,
            StreamRecoveryAction::RestartQuery,
        )
        .expect("stream opens");
    scheduler
        .enqueue(stream_frame(
            TrafficClass::State,
            &stream,
            1,
            DeliverySemantics::Replaceable,
            20,
            64,
        ))
        .expect("initial state queued");
    let error = scheduler
        .enqueue(stream_frame(
            TrafficClass::State,
            &stream,
            2,
            DeliverySemantics::Replaceable,
            60,
            64,
        ))
        .expect_err("oversized replacement retains old state");
    assert!(matches!(error, MultiplexError::QueueSaturated { .. }));
    assert_eq!(scheduler.pressure().class_queued_bytes[2], 20);
    let old = scheduler.dequeue().expect("dequeue").expect("old state");
    assert!(matches!(
        old,
        ScheduledFrame::Data(ref frame) if frame.sequence() == Some(StreamSequence::new(1))
    ));

    scheduler
        .enqueue(stream_frame(
            TrafficClass::State,
            &stream,
            2,
            DeliverySemantics::Replaceable,
            40,
            64,
        ))
        .expect("sequence can retry after rejection");
    let outcome = scheduler
        .enqueue(stream_frame(
            TrafficClass::State,
            &stream,
            3,
            DeliverySemantics::Replaceable,
            30,
            64,
        ))
        .expect("new state coalesces");
    assert_eq!(outcome, EnqueueOutcome::Coalesced { released_bytes: 40 });
    assert_eq!(scheduler.pressure().queued_messages, 1);
    let pressure = scheduler.stream_pressure(&stream).expect("stream pressure");
    assert_eq!(pressure.queued_messages, 1);
    assert_eq!(pressure.coalesced_items, 1);
    assert_eq!(pressure.dropped_items, 0);
}

#[test]
fn non_replaceable_lifecycle_transitions_are_replayed_or_report_an_explicit_gap() {
    let scheduler = scheduler(1, 64, 64, [64; 4], 64, 64);
    let stream = stream_id(71);
    scheduler
        .open_stream(
            stream.clone(),
            StreamKind::SessionEvents,
            StreamRecoveryAction::RequestSnapshot,
        )
        .expect("lifecycle stream opens");
    for sequence in 1..=3 {
        assert_eq!(
            scheduler
                .enqueue(stream_frame(
                    TrafficClass::Lifecycle,
                    &stream,
                    sequence,
                    DeliverySemantics::Reliable,
                    4,
                    64,
                ))
                .expect("reliable lifecycle transition queues"),
            EnqueueOutcome::Queued
        );
    }
    let pressure = scheduler
        .stream_pressure(&stream)
        .expect("lifecycle stream pressure");
    assert_eq!(pressure.queued_messages, 3);
    assert_eq!(pressure.coalesced_items, 0);

    let epoch = ServerEpoch::new("epoch-1").expect("epoch");
    let key = replay_key(1, 71);
    let journal = ReplayJournal::new(
        epoch.clone(),
        ReplayLimits::new(1, 8, 8, 4).expect("replay limits"),
    );
    journal
        .register_stream(key.clone(), StreamRecoveryAction::RequestSnapshot)
        .expect("lifecycle replay stream registers");
    for expected_sequence in 1..=3 {
        let ScheduledFrame::Data(frame) = scheduler
            .dequeue()
            .expect("lifecycle dequeue succeeds")
            .expect("lifecycle frame is queued")
        else {
            panic!("reliable lifecycle data must not become a live gap");
        };
        assert_eq!(frame.class(), TrafficClass::Lifecycle);
        assert_eq!(frame.semantics(), DeliverySemantics::Reliable);
        assert_eq!(
            frame.sequence(),
            Some(StreamSequence::new(expected_sequence))
        );
        journal
            .record(
                &key,
                StreamSequence::new(expected_sequence),
                frame.shared_bytes(),
            )
            .expect("written lifecycle frame is retained");
    }

    assert!(matches!(
        journal
            .resume(
                &epoch,
                &key,
                StreamSequence::new(1),
                StreamRecoveryAction::RestartQuery,
                Instant::now(),
            )
            .expect("contiguous lifecycle resume"),
        ReplayResult::Items(ref items)
            if items.iter().map(|item| item.sequence.value()).collect::<Vec<_>>() == vec![2, 3]
    ));
    assert!(matches!(
        journal
            .resume(
                &epoch,
                &key,
                StreamSequence::INITIAL,
                StreamRecoveryAction::RestartQuery,
                Instant::now(),
            )
            .expect("evicted lifecycle resume"),
        ReplayResult::GapWithItems { ref gap, ref items }
            if gap.reason == StreamGapReason::ReplayEvicted
                && gap.first_missing == StreamSequence::new(1)
                && gap.last_missing == StreamSequence::new(1)
                && gap.recovery == StreamRecoveryAction::RequestSnapshot
                && items.iter().map(|item| item.sequence.value()).collect::<Vec<_>>() == vec![2, 3]
    ));
}

#[test]
fn flow_credit_blocks_data_but_never_connection_control() {
    let scheduler = scheduler(4, 64, 128, [64, 64, 64, 64], 50, 64);
    let stream = stream_id(1);
    scheduler
        .open_stream(
            stream.clone(),
            StreamKind::TerminalOutput,
            StreamRecoveryAction::RestartFromAvailableOffset,
        )
        .expect("stream opens");
    scheduler
        .enqueue(stream_frame(
            TrafficClass::Bulk,
            &stream,
            1,
            DeliverySemantics::Reliable,
            60,
            64,
        ))
        .expect("data queued");
    assert_eq!(scheduler.dequeue().expect("credit wait is not fatal"), None);
    scheduler
        .enqueue(OutboundFrame::control(bytes(20), 64).expect("control frame"))
        .expect("control queued");
    assert!(matches!(
        scheduler.dequeue().expect("control dequeue"),
        Some(ScheduledFrame::Data(ref frame)) if frame.class() == TrafficClass::Control
    ));
    scheduler
        .grant_connection_credit(10)
        .expect("connection window update");
    scheduler
        .grant_stream_credit(&stream, 10)
        .expect("stream window update");
    assert!(matches!(
        scheduler.dequeue().expect("data dequeue"),
        Some(ScheduledFrame::Data(ref frame)) if frame.sequence() == Some(StreamSequence::new(1))
    ));
}

#[test]
fn consumed_credit_updates_connection_and_stream_atomically() {
    let scheduler = scheduler(4, 64, 35, [1, 1, 1, 32], 35, 128);
    let stream = stream_id(1);
    scheduler
        .open_stream(
            stream.clone(),
            StreamKind::TerminalOutput,
            StreamRecoveryAction::RestartFromAvailableOffset,
        )
        .expect("stream opens");
    scheduler
        .enqueue(stream_frame(
            TrafficClass::Bulk,
            &stream,
            1,
            DeliverySemantics::Reliable,
            32,
            64,
        ))
        .expect("frame queues");
    assert!(matches!(
        scheduler.dequeue(),
        Ok(Some(ScheduledFrame::Data(_)))
    ));

    assert_eq!(
        scheduler
            .grant_consumed_credit(&stream, 32)
            .expect("credit returns"),
        (35, 35)
    );
    let before = scheduler.pressure();
    assert!(scheduler.grant_consumed_credit(&stream, 1).is_err());
    assert_eq!(scheduler.pressure(), before);
}

#[test]
fn stream_burst_cap_resets_without_stalling_the_only_writable_stream() {
    let scheduler = scheduler(4, 64, 192, [64, 64, 64, 128], 192, 64);
    let stream = stream_id(1);
    scheduler
        .open_stream(
            stream.clone(),
            StreamKind::TerminalOutput,
            StreamRecoveryAction::RestartFromAvailableOffset,
        )
        .expect("stream opens");
    for sequence in 1..=2 {
        scheduler
            .enqueue(stream_frame(
                TrafficClass::Bulk,
                &stream,
                sequence,
                DeliverySemantics::Reliable,
                64,
                64,
            ))
            .expect("output queued");
    }
    assert!(scheduler.dequeue().expect("first dequeue").is_some());
    assert!(matches!(
        scheduler.dequeue().expect("second dequeue"),
        Some(ScheduledFrame::Data(ref frame))
            if frame.sequence() == Some(StreamSequence::new(2))
    ));
}

#[test]
fn sustained_control_traffic_obeys_the_declared_weighted_schedule() {
    let scheduler = scheduler(4, 64, 1_024, [1_024; 4], 1_024, 1_024);
    let lifecycle = stream_id(1);
    let state = stream_id(2);
    let bulk = stream_id(3);
    for (stream, kind, recovery) in [
        (
            &lifecycle,
            StreamKind::SessionEvents,
            StreamRecoveryAction::RestartQuery,
        ),
        (
            &state,
            StreamKind::SessionQuery,
            StreamRecoveryAction::RequestSnapshot,
        ),
        (
            &bulk,
            StreamKind::TerminalOutput,
            StreamRecoveryAction::RestartFromAvailableOffset,
        ),
    ] {
        scheduler
            .open_stream(stream.clone(), kind, recovery)
            .expect("stream opens");
    }
    for _ in 0..16 {
        scheduler
            .enqueue(OutboundFrame::control(bytes(1), 64).expect("control frame"))
            .expect("control queues");
    }
    for sequence in 1..=8 {
        scheduler
            .enqueue(stream_frame(
                TrafficClass::Lifecycle,
                &lifecycle,
                sequence,
                DeliverySemantics::Reliable,
                1,
                64,
            ))
            .expect("lifecycle queues");
    }
    for sequence in 1..=4 {
        scheduler
            .enqueue(stream_frame(
                TrafficClass::State,
                &state,
                sequence,
                DeliverySemantics::Reliable,
                1,
                64,
            ))
            .expect("state queues");
    }
    for sequence in 1..=2 {
        scheduler
            .enqueue(stream_frame(
                TrafficClass::Bulk,
                &bulk,
                sequence,
                DeliverySemantics::Reliable,
                1,
                64,
            ))
            .expect("bulk queues");
    }

    let mut selected = [0_usize; 4];
    for _ in 0..15 {
        let ScheduledFrame::Data(frame) = scheduler
            .dequeue()
            .expect("dequeue")
            .expect("weighted frame")
        else {
            panic!("weighted schedule contains no gaps");
        };
        selected[frame.class() as usize] += 1;
    }
    assert_eq!(selected, [8, 4, 2, 1]);
}

#[test]
fn resumed_scheduler_stream_rejects_sequences_already_acknowledged() {
    let scheduler = scheduler(4, 64, 256, [128, 256, 256, 384], 256, 128);
    let stream = stream_id(1);
    scheduler
        .open_stream_after(
            stream.clone(),
            StreamKind::TerminalOutput,
            StreamRecoveryAction::RestartFromAvailableOffset,
            StreamSequence::new(40),
        )
        .expect("resumed stream opens");

    assert!(matches!(
        scheduler.enqueue(stream_frame(
            TrafficClass::Bulk,
            &stream,
            40,
            DeliverySemantics::Reliable,
            8,
            64,
        )),
        Err(MultiplexError::StreamSequenceOutOfOrder { previous: 40, .. })
    ));
    assert!(
        scheduler
            .enqueue(stream_frame(
                TrafficClass::Bulk,
                &stream,
                41,
                DeliverySemantics::Reliable,
                8,
                64,
            ))
            .is_ok()
    );
}

#[test]
fn concurrent_request_responses_route_out_of_order_to_exact_callers() {
    let router = router(8, 8);
    let first_id = request_id(1);
    let second_id = request_id(2);
    let first = router
        .register_request(first_id.clone())
        .expect("first request");
    let second = router
        .register_request(second_id.clone())
        .expect("second request");

    router
        .route_response(&second_id, Arc::from(&b"second"[..]))
        .expect("second routed first");
    router
        .route_response(&first_id, Arc::from(&b"first"[..]))
        .expect("first routed second");

    assert_eq!(&*first.recv().expect("first response"), b"first");
    assert_eq!(&*second.recv().expect("second response"), b"second");
    assert_eq!(router.pressure().pending_requests, 0);
}

#[test]
fn uref_034_product_error_disposition_payload_routes_byte_exactly() {
    let router = router(1, 1);
    let request_id = request_id(34);
    let response = router
        .register_request(request_id.clone())
        .expect("request route");
    let payload: Arc<[u8]> = Arc::from(
        &br#"{"category":"failed_precondition","disposition":"unknown_inconclusive"}"#[..],
    );

    router
        .route_response(&request_id, payload.clone())
        .expect("response route");

    assert_eq!(response.recv().expect("response payload"), payload);
}

#[test]
fn request_response_routes_can_be_rolled_back_without_touching_streams() {
    let scheduler = scheduler(4, 64, 256, [128, 256, 256, 384], 256, 128);
    let request = request_id(1);
    let stream = stream_id(1);
    scheduler
        .open_stream(
            stream.clone(),
            StreamKind::SessionEvents,
            StreamRecoveryAction::RequestSnapshot,
        )
        .expect("stream opens");
    scheduler
        .enqueue(OutboundFrame::response(request.clone(), bytes(16), 64).expect("response frame"))
        .expect("response queues");
    scheduler
        .enqueue(stream_frame(
            TrafficClass::State,
            &stream,
            1,
            DeliverySemantics::Reliable,
            16,
            64,
        ))
        .expect("stream frame queues");

    let release = scheduler
        .remove_request_frames(&request)
        .expect("request route removed");
    assert_eq!(release.queued_messages, 1);
    assert_eq!(release.queued_bytes, 16);
    assert_eq!(scheduler.pressure().queued_messages, 1);
    assert!(matches!(
        scheduler.dequeue(),
        Ok(Some(ScheduledFrame::Data(frame))) if frame.route() == &FrameRoute::Stream(stream)
    ));
}

#[test]
fn request_and_stream_identifiers_use_independent_namespaces() {
    let router = router(2, 2);
    let request_id = RequestId::new("shared-connection-identity").expect("request identity");
    let stream_id = StreamId::new("shared-connection-identity").expect("stream identity");

    let pending = router
        .register_request(request_id.clone())
        .expect("request namespace accepts the identity");
    let stream = router
        .open_stream(stream_id.clone(), 2, 64)
        .expect("stream namespace independently accepts the same lexical identity");
    assert_eq!(router.pressure().pending_requests, 1);
    assert_eq!(router.pressure().active_streams, 1);

    router
        .route_response(&request_id, Arc::from(&b"response"[..]))
        .expect("request route remains independent");
    router
        .route_stream_item(&stream_id, inbound_data(1, 1))
        .expect("stream route remains independent");

    assert_eq!(&*pending.recv().expect("request response"), b"response");
    assert_eq!(stream.recv().expect("stream item").sequence().value(), 1);
}

#[test]
fn router_rejects_active_ids_and_supports_three_hundred_streams_on_one_connection() {
    let router = router(4, 300);
    let request = request_id(1);
    let pending = router
        .register_request(request.clone())
        .expect("request registers");
    assert!(matches!(
        router.register_request(request),
        Err(MultiplexError::RequestIdInUse { .. })
    ));

    let mut streams = Vec::new();
    for index in 0..300 {
        streams.push(
            router
                .open_stream(stream_id(index), 2, 128)
                .expect("stream opens"),
        );
    }
    assert_eq!(router.pressure().active_streams, 300);
    assert!(matches!(
        router.open_stream(stream_id(300), 2, 128),
        Err(MultiplexError::CapacityExceeded {
            resource: "logical_streams",
            limit: 300
        })
    ));
    drop(pending);
    drop(streams);
    assert_eq!(router.pressure().active_streams, 0);
}

#[test]
fn stream_sequences_are_monotonic_per_stream_without_a_global_order() {
    let router = router(4, 4);
    let first_id = stream_id(1);
    let second_id = stream_id(2);
    let first = router
        .open_stream(first_id.clone(), 4, 64)
        .expect("first stream");
    let second = router
        .open_stream(second_id.clone(), 4, 64)
        .expect("second stream");

    router
        .route_stream_item(&first_id, inbound_data(100, 1))
        .expect("first stream starts at its own sequence");
    router
        .route_stream_item(&second_id, inbound_data(1, 1))
        .expect("second stream need not follow the peer sequence");
    router
        .route_stream_item(&first_id, inbound_data(101, 1))
        .expect("first stream advances");
    assert!(matches!(
        router.route_stream_item(&second_id, inbound_data(1, 1)),
        Err(MultiplexError::StreamSequenceOutOfOrder {
            previous: 1,
            received: 1,
            ..
        })
    ));
    router
        .route_stream_item(&first_id, inbound_data(102, 1))
        .expect("peer ordering failure is isolated");

    assert_eq!(first.recv().expect("first item").sequence().value(), 100);
    assert_eq!(first.recv().expect("first item").sequence().value(), 101);
    assert_eq!(first.recv().expect("first item").sequence().value(), 102);
    assert_eq!(second.recv().expect("second item").sequence().value(), 1);
}

#[test]
fn every_overload_has_redacted_limit_context_and_safe_retry_guidance() {
    let cases = [
        (
            MultiplexError::CapacityExceeded {
                resource: "pending_requests",
                limit: 8,
            },
            "pending_requests",
            Some(8),
            Some(8),
            Some(1),
            MultiplexRetryGuidance::AfterCapacityAvailable,
        ),
        (
            MultiplexError::QueueSaturated {
                queue: "control",
                byte_limit: 64,
                queued_bytes: 60,
                requested_bytes: 8,
            },
            "control",
            Some(64),
            Some(60),
            Some(8),
            MultiplexRetryGuidance::AfterQueueDrain,
        ),
        (
            MultiplexError::QueueMessageSaturated {
                queue: "state",
                message_limit: 4,
                queued_messages: 4,
            },
            "state",
            Some(4),
            Some(4),
            Some(1),
            MultiplexRetryGuidance::AfterQueueDrain,
        ),
        (
            MultiplexError::CreditExhausted {
                stream_id: Some(stream_id(77)),
                available_bytes: 3,
                required_bytes: 9,
            },
            "stream_credit_bytes",
            None,
            Some(3),
            Some(9),
            MultiplexRetryGuidance::AfterWindowUpdate,
        ),
        (
            MultiplexError::CreditUpdateExceeded {
                window: "connection_credit_bytes",
                requested: 129,
                maximum: 128,
            },
            "connection_credit_bytes",
            Some(128),
            None,
            Some(129),
            MultiplexRetryGuidance::DoNotRetryUnchanged,
        ),
    ];

    for (error, resource, limit, current, requested, retry) in cases {
        let context = error.overload().expect("overload context");
        assert_eq!(context.resource(), resource);
        assert_eq!(context.limit(), limit);
        assert_eq!(context.current(), current);
        assert_eq!(context.requested(), requested);
        assert_eq!(context.retry(), retry);
        assert!(!format!("{context:?}").contains("stream-77"));
    }
    assert!(MultiplexError::ConnectionClosed.overload().is_none());
}

#[test]
fn unsubscribe_and_slow_mailbox_are_isolated_to_one_logical_stream() {
    let router = router(4, 4);
    let first_id = stream_id(1);
    let second_id = stream_id(2);
    let first = router
        .open_stream(first_id.clone(), 1, 4)
        .expect("first stream");
    let second = router
        .open_stream(second_id.clone(), 2, 8)
        .expect("second stream");
    router
        .route_stream_item(&first_id, inbound_data(1, 4))
        .expect("first item");
    let overflow = router
        .route_stream_item(&first_id, inbound_data(2, 1))
        .expect_err("first stream mailbox is bounded");
    assert!(matches!(overflow, MultiplexError::QueueSaturated { .. }));
    router
        .route_stream_item(&second_id, inbound_data(1, 3))
        .expect("peer remains writable");
    router.close_stream(&first_id).expect("first unsubscribed");
    assert_eq!(first.recv(), Err(InboundStreamError::Unsubscribed));
    assert!(matches!(
        second.recv_timeout(Duration::from_millis(10)),
        Ok(InboundStreamItem::Data { sequence, .. }) if sequence == StreamSequence::new(1)
    ));
}

#[test]
fn teardown_fails_pending_work_releases_queues_and_preserves_resume_sequence() {
    let router = router(4, 4);
    let pending = router
        .register_request(request_id(1))
        .expect("request registers");
    let stream_id = stream_id(1);
    let stream = router
        .open_stream(stream_id.clone(), 4, 32)
        .expect("stream opens");
    router
        .route_stream_item(&stream_id, inbound_data(1, 8))
        .expect("stream item");
    assert!(matches!(
        stream.recv(),
        Ok(InboundStreamItem::Data { sequence, .. })
            if sequence == StreamSequence::new(1)
    ));
    router
        .route_stream_item(&stream_id, inbound_data(2, 8))
        .expect("unconsumed stream item");

    let release = router.close();

    assert_eq!(release.pending_requests, 1);
    assert_eq!(release.streams, 1);
    assert_eq!(release.queued_bytes, 8);
    assert_eq!(pending.recv(), Err(PendingResponseError::ConnectionClosed));
    assert_eq!(stream.last_sequence(), StreamSequence::new(1));
    assert_eq!(stream.recv(), Err(InboundStreamError::ConnectionClosed));
    assert_eq!(router.pressure().stream_queued_bytes, 0);
}

#[test]
fn replay_returns_explicit_eviction_and_epoch_gaps() {
    let journal = ReplayJournal::new(
        ServerEpoch::new("epoch-1").expect("epoch"),
        ReplayLimits::new(4, 8, 16, 4).expect("replay limits"),
    );
    let key = replay_key(1, 1);
    journal
        .register_stream(key.clone(), StreamRecoveryAction::RequestSnapshot)
        .expect("stream registers");
    for sequence in 1..=3 {
        journal
            .record(&key, StreamSequence::new(sequence), bytes(4))
            .expect("frame records");
    }

    let gap = journal
        .resume(
            &ServerEpoch::new("epoch-1").expect("epoch"),
            &key,
            StreamSequence::INITIAL,
            StreamRecoveryAction::RestartQuery,
            Instant::now(),
        )
        .expect("resume result");
    assert!(matches!(
        gap,
        ReplayResult::GapWithItems { ref gap, ref items }
            if gap.reason == StreamGapReason::ReplayEvicted
                && gap.last_missing == StreamSequence::new(1)
                && items.iter().map(|item| item.sequence.value()).collect::<Vec<_>>() == vec![2, 3]
    ));
    let replay = journal
        .resume(
            &ServerEpoch::new("epoch-1").expect("epoch"),
            &key,
            StreamSequence::new(1),
            StreamRecoveryAction::RestartQuery,
            Instant::now(),
        )
        .expect("resume result");
    assert!(matches!(
        replay,
        ReplayResult::Items(ref items)
            if items.iter().map(|item| item.sequence.value()).collect::<Vec<_>>() == vec![2, 3]
    ));
    let epoch_gap = journal
        .resume(
            &ServerEpoch::new("epoch-2").expect("epoch"),
            &key,
            StreamSequence::new(3),
            StreamRecoveryAction::RestartQuery,
            Instant::now(),
        )
        .expect("epoch gap");
    assert!(matches!(
        epoch_gap,
        ReplayResult::Gap(ref gap) if gap.reason == StreamGapReason::ServerEpochChanged
    ));
}

#[test]
fn activated_eviction_suffix_survives_later_global_eviction() {
    let epoch = ServerEpoch::new("epoch-1").unwrap();
    let journal = ReplayJournal::new(epoch.clone(), ReplayLimits::new(2, 8, 8, 8).unwrap());
    let key = replay_key(1, 1);
    journal
        .register_stream(key.clone(), StreamRecoveryAction::RequestSnapshot)
        .unwrap();
    for sequence in 1..=3 {
        journal
            .record(&key, StreamSequence::new(sequence), bytes(4))
            .unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    journal.deactivate_stream(&key, deadline);
    let activation = journal
        .activate_resume(
            &epoch,
            key.clone(),
            StreamSequence::INITIAL,
            StreamRecoveryAction::RestartQuery,
            Instant::now(),
        )
        .unwrap();
    assert!(!activation.created);
    assert_eq!(activation.previous_expiration, Some(deadline));
    let ReplayResult::GapWithItems { gap, items } = activation.replay else {
        panic!("partial eviction must select gap and suffix together");
    };
    assert_eq!(gap.last_missing, StreamSequence::new(1));
    assert_eq!(gap.recovery, StreamRecoveryAction::RequestSnapshot);

    let other = replay_key(1, 2);
    journal
        .register_stream(other.clone(), StreamRecoveryAction::RestartQuery)
        .unwrap();
    journal
        .record(&other, StreamSequence::new(1), bytes(8))
        .unwrap();
    assert_eq!(
        items
            .iter()
            .map(|item| item.sequence.value())
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert!(items.iter().all(|item| item.bytes == bytes(4)));
    assert!(matches!(journal.resume(
        &epoch, &key, StreamSequence::INITIAL, StreamRecoveryAction::RestartQuery, Instant::now(),
    ).unwrap(), ReplayResult::Gap(gap)
        if gap.first_missing.value() == 1 && gap.last_missing.value() == 3
            && gap.reason == StreamGapReason::ReplayEvicted));
}

#[test]
fn replay_bookkeeping_remains_bounded_under_long_stream_churn() {
    let journal = ReplayJournal::new(
        ServerEpoch::new("epoch-1").expect("epoch"),
        ReplayLimits::new(300, 16, 64, 4).expect("replay limits"),
    );
    for index in 0..300 {
        journal
            .register_stream(replay_key(1, index), StreamRecoveryAction::RestartQuery)
            .expect("stream registers");
    }
    let stream = replay_key(1, 0);
    for sequence in 1..=1_000 {
        journal
            .record(&stream, StreamSequence::new(sequence), bytes(4))
            .expect("frame records");
    }

    let pressure = journal.pressure();
    assert_eq!(pressure.streams, 300);
    assert_eq!(pressure.items, 4);
    assert_eq!(pressure.bytes, 16);
}

#[test]
fn rapid_stream_and_request_churn_leaves_no_stale_router_state() {
    const CONCURRENT: usize = 300;
    const WAVES: usize = 100;

    let router = router(CONCURRENT as u32, CONCURRENT as u32);
    for wave in 0..WAVES {
        let mut streams = Vec::with_capacity(CONCURRENT);
        let mut requests = Vec::with_capacity(CONCURRENT);
        for index in 0..CONCURRENT {
            let stream_id =
                StreamId::new(format!("churn-stream-{wave}-{index}")).expect("churn stream ID");
            let stream = router
                .open_stream(stream_id.clone(), 1, 4)
                .expect("churn stream opens");
            router
                .route_stream_item(&stream_id, inbound_data(1, 4))
                .expect("churn stream item routes");
            streams.push(stream);
            requests.push(
                router
                    .register_request(
                        RequestId::new(format!("churn-request-{wave}-{index}"))
                            .expect("churn request ID"),
                    )
                    .expect("churn request registers"),
            );
        }

        assert_eq!(router.pressure().active_streams, CONCURRENT);
        assert_eq!(router.pressure().pending_requests, CONCURRENT);
        assert_eq!(router.pressure().stream_queued_bytes, 4 * CONCURRENT as u64);
        drop(requests);
        drop(streams);
        assert_eq!(
            router.pressure(),
            undercurrent_multiplex::RouterPressure {
                pending_requests: 0,
                active_streams: 0,
                stream_queued_bytes: 0,
            }
        );
    }
}

#[test]
fn replay_stream_identity_is_scoped_to_the_stable_client() {
    let journal = ReplayJournal::new(
        ServerEpoch::new("epoch-1").expect("epoch"),
        ReplayLimits::new(2, 8, 16, 4).expect("replay limits"),
    );
    let first = replay_key(1, 7);
    let second = replay_key(2, 7);

    journal
        .register_stream(first.clone(), StreamRecoveryAction::RestartQuery)
        .expect("first client stream registers");
    journal
        .register_stream(second.clone(), StreamRecoveryAction::RequestSnapshot)
        .expect("same stream ID for second client registers");
    journal
        .record(&first, StreamSequence::new(1), bytes(4))
        .expect("first client frame records");

    assert!(matches!(
        journal
            .resume(
                &ServerEpoch::new("epoch-1").expect("epoch"),
                &second,
                StreamSequence::INITIAL,
                StreamRecoveryAction::RestartQuery,
                Instant::now(),
            )
            .expect("second client resume"),
        ReplayResult::Items(ref items) if items.is_empty()
    ));
    assert_eq!(journal.remove_stream(&first), 4);
    assert_eq!(journal.pressure().streams, 1);
}

#[test]
fn replay_registration_is_idempotent_only_for_reconnect_preparation() {
    let journal = ReplayJournal::new(
        ServerEpoch::new("epoch-1").expect("epoch"),
        ReplayLimits::new(2, 8, 16, 4).expect("replay limits"),
    );
    let key = replay_key(1, 1);

    assert!(
        journal
            .ensure_stream(
                key.clone(),
                StreamRecoveryAction::RestartQuery,
                Instant::now(),
            )
            .expect("new journal created")
    );
    assert!(journal.deactivate_stream(&key, Instant::now() + Duration::from_secs(1)));
    assert!(
        !journal
            .ensure_stream(
                key.clone(),
                StreamRecoveryAction::RequestSnapshot,
                Instant::now(),
            )
            .expect("existing journal retained")
    );
    assert!(matches!(
        journal.register_stream(key, StreamRecoveryAction::RestartQuery),
        Err(MultiplexError::StreamIdInUse { .. })
    ));
}

#[test]
fn inactive_replay_journals_expire_from_the_deadline_index() {
    let journal = ReplayJournal::new(
        ServerEpoch::new("epoch-1").expect("epoch"),
        ReplayLimits::new(4, 8, 32, 4).expect("replay limits"),
    );
    let now = Instant::now();
    let expired = [replay_key(1, 1), replay_key(1, 2)];
    let future = replay_key(1, 3);
    let active = replay_key(1, 4);
    for key in [&expired[0], &expired[1], &future, &active] {
        journal
            .register_stream(key.clone(), StreamRecoveryAction::RestartQuery)
            .expect("journal registers");
        journal
            .record(key, StreamSequence::new(1), bytes(4))
            .expect("frame records");
    }
    for key in &expired {
        assert!(journal.deactivate_stream(key, now));
    }
    assert!(journal.deactivate_stream(&future, now + Duration::from_secs(60)));

    let outcome = journal.expire_before(now);

    assert_eq!(outcome.streams, 2);
    assert_eq!(outcome.items, 2);
    assert_eq!(outcome.bytes, 8);
    let pressure = journal.pressure();
    assert_eq!(pressure.streams, 2);
    assert_eq!(pressure.active_streams, 1);
    assert_eq!(pressure.expiring_streams, 1);
}

#[test]
fn replay_pressure_prunes_due_journals_but_preserves_active_and_future_state() {
    let journal = ReplayJournal::new(
        ServerEpoch::new("epoch-1").expect("epoch"),
        ReplayLimits::new(3, 8, 24, 4).expect("replay limits"),
    );
    let due = replay_key(1, 1);
    let future = replay_key(1, 2);
    let active = replay_key(1, 3);
    for key in [&due, &future, &active] {
        journal
            .register_stream(key.clone(), StreamRecoveryAction::RestartQuery)
            .expect("journal registers");
        journal
            .record(key, StreamSequence::new(1), bytes(4))
            .expect("frame records");
    }
    assert!(journal.deactivate_stream(&due, Instant::now()));
    assert!(journal.deactivate_stream(&future, Instant::now() + Duration::from_secs(60)));

    let pressure = journal.pressure();

    assert_eq!(pressure.streams, 2);
    assert_eq!(pressure.items, 2);
    assert_eq!(pressure.bytes, 8);
    assert_eq!(pressure.active_streams, 1);
    assert_eq!(pressure.expiring_streams, 1);
}

#[test]
fn all_replay_admission_paths_prune_expired_unrelated_capacity() {
    let epoch = ServerEpoch::new("epoch-1").expect("epoch");
    let deadline = Instant::now();

    let registered = ReplayJournal::new(
        epoch.clone(),
        ReplayLimits::new(1, 8, 8, 4).expect("replay limits"),
    );
    let expired = replay_key(1, 1);
    registered
        .register_stream(expired.clone(), StreamRecoveryAction::RestartQuery)
        .expect("expired journal registers");
    assert!(registered.deactivate_stream(&expired, deadline));
    registered
        .register_stream(replay_key(2, 1), StreamRecoveryAction::RestartQuery)
        .expect("register admission prunes expired unrelated journal");

    let ensured = ReplayJournal::new(
        epoch.clone(),
        ReplayLimits::new(1, 8, 8, 4).expect("replay limits"),
    );
    let expired = replay_key(1, 1);
    ensured
        .register_stream(expired.clone(), StreamRecoveryAction::RestartQuery)
        .expect("expired journal registers");
    assert!(ensured.deactivate_stream(&expired, deadline));
    assert!(
        ensured
            .ensure_stream(
                replay_key(2, 1),
                StreamRecoveryAction::RequestSnapshot,
                deadline,
            )
            .expect("ensure admission prunes expired unrelated journal")
    );

    let activated = ReplayJournal::new(
        epoch.clone(),
        ReplayLimits::new(1, 8, 8, 4).expect("replay limits"),
    );
    let expired = replay_key(1, 1);
    activated
        .register_stream(expired.clone(), StreamRecoveryAction::RestartQuery)
        .expect("expired journal registers");
    assert!(activated.deactivate_stream(&expired, deadline));
    let activation = activated
        .activate_resume(
            &epoch,
            replay_key(2, 1),
            StreamSequence::INITIAL,
            StreamRecoveryAction::RequestSnapshot,
            deadline,
        )
        .expect("resume admission prunes expired unrelated journal");
    assert!(activation.created);
    assert!(matches!(activation.replay, ReplayResult::Gap(_)));
}

#[test]
fn replay_is_preserved_immediately_before_the_reconnect_deadline() {
    let epoch = ServerEpoch::new("epoch-1").expect("epoch");
    let journal = ReplayJournal::new(
        epoch.clone(),
        ReplayLimits::new(1, 8, 8, 4).expect("replay limits"),
    );
    let key = replay_key(1, 1);
    let deadline = Instant::now() + Duration::from_secs(60);
    journal
        .register_stream(key.clone(), StreamRecoveryAction::RestartQuery)
        .expect("journal registers");
    journal
        .record(&key, StreamSequence::new(1), bytes(4))
        .expect("frame records");
    assert!(journal.deactivate_stream(&key, deadline));

    let activation = journal
        .activate_resume(
            &epoch,
            key,
            StreamSequence::INITIAL,
            StreamRecoveryAction::RequestSnapshot,
            deadline - Duration::from_nanos(1),
        )
        .expect("pre-deadline resume succeeds");

    assert!(!activation.created);
    assert_eq!(activation.previous_expiration, Some(deadline));
    assert!(matches!(activation.replay, ReplayResult::Items(ref items) if items.len() == 1));
}

#[test]
fn resume_cannot_reactivate_a_journal_at_or_after_its_deadline() {
    let journal = ReplayJournal::new(
        ServerEpoch::new("epoch-1").expect("epoch"),
        ReplayLimits::new(1, 8, 8, 4).expect("replay limits"),
    );
    let key = replay_key(1, 1);
    let deadline = Instant::now();
    journal
        .register_stream(key.clone(), StreamRecoveryAction::RestartQuery)
        .expect("journal registers");
    journal
        .record(&key, StreamSequence::new(1), bytes(4))
        .expect("frame records");
    assert!(journal.deactivate_stream(&key, deadline));

    let activation = journal
        .activate_resume(
            &ServerEpoch::new("epoch-1").expect("epoch"),
            key,
            StreamSequence::INITIAL,
            StreamRecoveryAction::RequestSnapshot,
            deadline,
        )
        .expect("expired resume creates a fresh journal");

    assert!(activation.created);
    assert_eq!(activation.previous_expiration, None);
    assert!(matches!(
        activation.replay,
        ReplayResult::Gap(ref gap)
            if gap.reason == StreamGapReason::ReplayUnavailable
                && gap.recovery == StreamRecoveryAction::RequestSnapshot
    ));
    assert_eq!(
        journal.pressure(),
        undercurrent_multiplex::ReplayPressure {
            streams: 1,
            items: 0,
            bytes: 0,
            active_streams: 1,
            expiring_streams: 0,
        }
    );
}

#[test]
fn multiplex_registries_are_complete_unique_and_stable() {
    let diagnostics = MultiplexDiagnosticName::ALL
        .map(MultiplexDiagnosticName::as_str)
        .into_iter()
        .collect::<BTreeSet<_>>();
    let metrics = MultiplexMetricName::ALL
        .map(MultiplexMetricName::as_str)
        .into_iter()
        .collect::<BTreeSet<_>>();

    assert_eq!(diagnostics.len(), MultiplexDiagnosticName::ALL.len());
    assert_eq!(metrics.len(), MultiplexMetricName::ALL.len());
    assert_eq!(TrafficClass::ALL[0].as_str(), "control");
    assert_eq!(StreamKind::TerminalOutput.as_str(), "terminal_output");
}

#[test]
fn scheduler_emits_class_bounded_metrics_after_state_unlocks() {
    let telemetry = Arc::new(RecordingMultiplexTelemetry::default());
    let negotiated = ConnectionLimits::new(64, 4, 4, 320, 100, 100).expect("connection limits");
    let scheduler = OutboundScheduler::with_telemetry(
        SchedulerLimits::new(
            negotiated,
            [80; 4],
            100,
            64,
            undercurrent_multiplex::SchedulerMessageLimits::new(16, 4, [4; 4])
                .expect("message limits"),
        )
        .expect("scheduler limits"),
        telemetry.clone(),
    );
    let stream = stream_id(90);
    scheduler
        .open_stream(
            stream.clone(),
            StreamKind::TerminalOutput,
            StreamRecoveryAction::RestartFromAvailableOffset,
        )
        .expect("stream opens");
    scheduler
        .enqueue(stream_frame(
            TrafficClass::Bulk,
            &stream,
            1,
            DeliverySemantics::Droppable,
            64,
            64,
        ))
        .expect("bulk queues");
    assert!(matches!(
        scheduler
            .enqueue(stream_frame(
                TrafficClass::Bulk,
                &stream,
                2,
                DeliverySemantics::Droppable,
                64,
                64,
            ))
            .expect("overflow is explicit"),
        EnqueueOutcome::Dropped { .. }
    ));
    let _ = scheduler.dequeue().expect("data dequeue");
    let _ = scheduler.dequeue().expect("gap dequeue");
    scheduler.close_stream(&stream).expect("stream closes");

    let observations = telemetry.observations();
    assert!(observations.iter().any(|observation| {
        observation.name == MultiplexMetricName::QueuedBytes
            && observation.traffic_class == Some(TrafficClass::Bulk)
            && observation.value == 64.0
    }));
    assert!(observations.iter().any(|observation| {
        observation.name == MultiplexMetricName::DroppedItems
            && observation.traffic_class == Some(TrafficClass::Bulk)
    }));
    assert!(
        observations
            .iter()
            .any(|observation| observation.name == MultiplexMetricName::ReportedGaps)
    );
    let logical_streams = observations
        .iter()
        .filter(|observation| observation.name == MultiplexMetricName::LogicalStreams)
        .map(|observation| observation.value)
        .collect::<Vec<_>>();
    assert_eq!(logical_streams.first(), Some(&1.0));
    assert_eq!(logical_streams.last(), Some(&0.0));
    let stream_lag = observations
        .iter()
        .filter(|observation| observation.name == MultiplexMetricName::StreamLagItems)
        .map(|observation| observation.value)
        .collect::<Vec<_>>();
    assert_eq!(stream_lag, vec![0.0, 1.0, 2.0, 1.0, 0.0, 0.0]);
}

#[derive(Default)]
struct RecordingMultiplexTelemetry {
    observations: Mutex<Vec<MultiplexMetricObservation>>,
}

impl RecordingMultiplexTelemetry {
    fn observations(&self) -> Vec<MultiplexMetricObservation> {
        self.observations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl MultiplexTelemetry for RecordingMultiplexTelemetry {
    fn record_metric(&self, observation: MultiplexMetricObservation) {
        self.observations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(observation);
    }
}

fn scheduler(
    max_streams: u32,
    max_frame: u32,
    stream_queue: u64,
    class_budgets: [u64; 4],
    initial_connection_credit: u64,
    max_consecutive_stream_bytes: u64,
) -> OutboundScheduler {
    let connection_queue = class_budgets.iter().copied().sum();
    let negotiated = ConnectionLimits::new(
        max_frame,
        max_streams,
        64,
        connection_queue,
        stream_queue,
        initial_connection_credit.min(stream_queue),
    )
    .expect("connection limits");
    OutboundScheduler::new(
        SchedulerLimits::new(
            negotiated,
            class_budgets,
            initial_connection_credit,
            max_consecutive_stream_bytes,
            undercurrent_multiplex::SchedulerMessageLimits::new(
                16_384,
                4_096,
                [2_048, 4_096, 4_096, 6_144],
            )
            .expect("scheduler message limits"),
        )
        .expect("scheduler limits"),
    )
}

fn router(max_pending_requests: u32, max_streams: u32) -> ConnectionRouter {
    ConnectionRouter::new(
        ConnectionLimits::new(64, max_streams, max_pending_requests, 1_024, 128, 64)
            .expect("connection limits"),
    )
}

fn stream_frame(
    class: TrafficClass,
    stream_id: &StreamId,
    sequence: u64,
    semantics: DeliverySemantics,
    size: usize,
    max_frame: u32,
) -> OutboundFrame {
    OutboundFrame::stream(
        class,
        stream_id.clone(),
        StreamSequence::new(sequence),
        semantics,
        bytes(size),
        max_frame,
    )
    .expect("stream frame")
}

fn inbound_data(sequence: u64, size: usize) -> InboundStreamItem {
    InboundStreamItem::Data {
        sequence: StreamSequence::new(sequence),
        bytes: bytes(size),
        flow_controlled_bytes: u64::try_from(size).expect("test size fits u64"),
    }
}

fn bytes(size: usize) -> Arc<[u8]> {
    Arc::from(vec![b'x'; size])
}

fn stream_id(index: usize) -> StreamId {
    StreamId::new(format!("stream-{index}")).expect("stream ID")
}

fn replay_key(client_index: usize, stream_index: usize) -> ReplayStreamKey {
    ReplayStreamKey::new(
        ClientId::new(format!("client-{client_index}")).expect("client ID"),
        stream_id(stream_index),
    )
}

fn request_id(index: usize) -> RequestId {
    RequestId::new(format!("request-{index}")).expect("request ID")
}
