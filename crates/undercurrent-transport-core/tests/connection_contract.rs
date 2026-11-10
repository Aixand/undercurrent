// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use undercurrent_multiplex::{
    ClientMessage, DeliverySemantics, InboundStreamItem, ReplayJournal, ReplayLimits,
    ReplayStreamKey, SchedulerLimits, ServerMessage, StreamGapReason, StreamKind,
    StreamRecoveryAction, TrafficClass,
};
use undercurrent_transport_core::{
    ActiveConnectionRegistry, ServerAction, ServerConnectionDiagnosticName, ServerConnectionError,
    ServerConnectionErrorCategory, ServerConnectionSession, ServerDiagnosticLimits,
    ServerQueueDrainWake, ServerStreamConfiguration, ServerStreamDiagnosticState,
};
use undercurrent_wire::core::NamespacedName;
use undercurrent_wire::{
    CURRENT_PROTOCOL_VERSION, ClientId, ConnectionAgreement, ConnectionHello, ConnectionId,
    ConnectionLimits, ConnectionPolicy, HeartbeatPolicy, RequestId, ServerEpoch, SpanId, StreamId,
    StreamSequence, TraceContext, TraceId, connection_capabilities, negotiate_connection,
};
use undercurrent_wire::{DecodeStatus, WireCodec};

#[test]
fn physical_writer_waits_for_work_and_teardown_without_polling() {
    let session = Arc::new(session(replay(), 4, 4));
    let (started_sender, started_receiver) = mpsc::sync_channel(1);
    let (write_sender, write_receiver) = mpsc::sync_channel(1);
    let worker_session = session.clone();
    let worker = thread::spawn(move || {
        started_sender.send(()).expect("signal writer start");
        let result = worker_session
            .wait_for_write()
            .expect("wait for queued write")
            .map(|write| decode(write.bytes()));
        write_sender.send(result).expect("return selected write");
    });

    started_receiver.recv().expect("writer started");
    assert!(matches!(
        write_receiver.recv_timeout(Duration::from_millis(25)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    session
        .accept(ClientMessage::Ping)
        .expect("ping queues control response");
    assert_eq!(
        write_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("writer wakes"),
        Some(ServerMessage::Pong)
    );
    worker.join().expect("writer joins");

    let (closed_sender, closed_receiver) = mpsc::sync_channel(1);
    let worker_session = session.clone();
    let worker = thread::spawn(move || {
        closed_sender
            .send(worker_session.wait_for_write())
            .expect("return close result");
    });
    session.close();
    assert!(
        closed_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("close wakes writer")
            .expect("close is successful")
            .is_none()
    );
    worker.join().expect("closing writer joins");
}

#[test]
fn physical_writer_waits_for_window_credit_without_closing_the_connection() {
    let session = Arc::new(credit_limited_session(replay()));
    let request = request_id(1);
    let stream = stream_id(1);
    session
        .accept(ClientMessage::OpenStream {
            request_id: request.clone(),
            stream_id: stream.clone(),
            trace_context: trace_context(1),
            payload: bytes(b"terminal"),
        })
        .expect("open accepted");
    session
        .activate_stream(
            &request,
            &stream,
            terminal_configuration(),
            bytes(b"opened"),
        )
        .expect("stream activates");
    session
        .next_write()
        .expect("dequeue")
        .expect("activation response")
        .confirm_written()
        .expect("activation response confirms");
    session
        .publish_stream(&stream, StreamSequence::new(1), bytes(&vec![7; 512]))
        .expect("flow-blocked output queues");

    let (write_sender, write_receiver) = mpsc::sync_channel(1);
    let worker_session = session.clone();
    let worker = thread::spawn(move || {
        let result = worker_session
            .wait_for_write()
            .expect("credit wait remains healthy")
            .map(|write| decode(write.bytes()));
        write_sender.send(result).expect("return selected write");
    });
    assert!(matches!(
        write_receiver.recv_timeout(Duration::from_millis(25)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    assert!(!session.is_closed());

    session
        .accept(ClientMessage::WindowUpdate {
            stream_id: stream.clone(),
            additional_bytes: 1_024,
        })
        .expect("credit update accepted");
    assert!(matches!(
        write_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("credit update wakes writer"),
        Some(ServerMessage::StreamItem { stream_id, .. }) if stream_id == stream
    ));
    assert!(!session.is_closed());
    worker.join().expect("writer joins");
}

#[test]
fn physical_dequeue_wakes_backpressured_product_work() {
    let session = session(replay(), 4, 4);
    let wake = Arc::new(CountingDrainWake::default());
    session
        .set_queue_drain_wake(wake.clone())
        .expect("drain wake installs");
    session
        .accept(ClientMessage::Ping)
        .expect("ping queues control response");
    assert_eq!(wake.count.load(Ordering::Acquire), 0);

    let write = session.next_write().expect("dequeue").expect("pong");
    assert_eq!(decode(write.bytes()), ServerMessage::Pong);
    assert_eq!(wake.count.load(Ordering::Acquire), 1);
}

#[test]
fn finite_requests_are_bounded_and_complete_out_of_order_by_id() {
    let journal = replay();
    let session = session(journal, 4, 4);
    for index in 1..=2 {
        assert!(matches!(
            session
                .accept(ClientMessage::Request {
                    request_id: request_id(index),
                    trace_context: trace_context(index),
                    payload: bytes(format!("request-{index}").as_bytes()),
                })
                .expect("request accepted")
                .as_slice(),
            [ServerAction::Request { request_id: accepted, .. }] if accepted == &request_id(index)
        ));
    }
    assert!(matches!(
        session.accept(ClientMessage::Request {
            request_id: request_id(1),
            trace_context: trace_context(1),
            payload: bytes(b"duplicate"),
        }),
        Err(ServerConnectionError::Multiplex(
            undercurrent_multiplex::MultiplexError::RequestIdInUse { .. }
        ))
    ));

    session
        .complete_request(&request_id(2), bytes(b"second"))
        .expect("second completes first");
    session
        .complete_request(&request_id(1), bytes(b"first"))
        .expect("first completes second");
    let mut responses = Vec::new();
    for _ in 0..2 {
        let write = session.next_write().expect("dequeue").expect("response");
        responses.push(decode(write.bytes()));
        assert!(write.confirm_written().expect("confirm").is_none());
    }
    assert!(responses.contains(&ServerMessage::Response {
        request_id: request_id(1),
        payload: bytes(b"first"),
    }));
    assert!(responses.contains(&ServerMessage::Response {
        request_id: request_id(2),
        payload: bytes(b"second"),
    }));
    let pressure = session.pressure();
    assert_eq!(pressure.pending_requests, 0);
    assert_eq!(pressure.request_ids_used, 2);
    assert_eq!(
        pressure.request_id_limit,
        undercurrent_transport_core::REQUEST_ID_EPOCH_LEDGER_LIMIT
    );
    assert!(matches!(
        session.accept(ClientMessage::Request {
            request_id: request_id(1),
            trace_context: trace_context(9),
            payload: bytes(b"completed-id-reuse"),
        }),
        Err(ServerConnectionError::Multiplex(
            undercurrent_multiplex::MultiplexError::RequestIdInUse { .. }
        ))
    ));
    assert_eq!(
        undercurrent_transport_core::REQUEST_ID_EPOCH_LEDGER_LIMIT,
        1_048_576
    );
    session.close();
    let pressure = session.pressure();
    assert!(pressure.closed);
    assert_eq!(pressure.request_ids_used, 0);
    assert_eq!(pressure.request_id_limit, 1_048_576);
}

#[test]
fn finite_request_cancel_preserves_kind_and_has_one_correlated_terminal() {
    let session = session(replay(), 4, 4);
    let request = request_id(1);
    session
        .accept(ClientMessage::Request {
            request_id: request.clone(),
            trace_context: trace_context(1),
            payload: bytes(b"request"),
        })
        .expect("request accepted");

    assert_eq!(
        session
            .accept(ClientMessage::CancelRequest {
                request_id: request.clone(),
            })
            .expect("cancel accepted"),
        vec![ServerAction::CancelRequest {
            request_id: request.clone(),
            request_kind: undercurrent_transport_core::ServerRequestKind::FiniteRequest,
        }]
    );
    assert_eq!(session.pressure().pending_requests, 0);
    assert!(
        session
            .accept(ClientMessage::CancelRequest {
                request_id: request.clone(),
            })
            .expect("duplicate cancel is idempotent")
            .is_empty()
    );

    session
        .reject_unaccepted_request(&request, bytes(b"cancelled"))
        .expect("correlated terminal queues");
    let write = session.next_write().expect("dequeue").expect("terminal");
    assert_eq!(
        decode(write.bytes()),
        ServerMessage::Response {
            request_id: request,
            payload: bytes(b"cancelled"),
        }
    );
    write.confirm_written().expect("terminal confirms");
}

#[test]
fn repeated_finite_diagnostics_create_no_journals_or_extend_reconnect_deadlines() {
    let journal = replay();
    let retained = ReplayStreamKey::new(client_id_at(2), stream_id(900));
    journal
        .register_stream(retained.clone(), StreamRecoveryAction::RestartQuery)
        .expect("retained journal registers");
    journal
        .record(&retained, StreamSequence::new(1), bytes(b"keep"))
        .expect("retained frame records");
    let deadline = Instant::now() + Duration::from_secs(1);
    assert!(journal.deactivate_stream(&retained, deadline));
    let session = session(journal.clone(), 16, 4);

    for index in 1..=8 {
        let request = request_id(index);
        session
            .accept(ClientMessage::Request {
                request_id: request.clone(),
                trace_context: trace_context(index),
                payload: bytes(b"get-diagnostics"),
            })
            .expect("finite diagnostic request accepted");
        session
            .complete_request(&request, bytes(b"diagnostic-page"))
            .expect("finite diagnostic request completes");
        session
            .next_write()
            .expect("dequeue")
            .expect("diagnostic response")
            .confirm_written()
            .expect("diagnostic response confirms");
    }

    let expired = journal.expire_before(deadline);
    assert_eq!(expired.streams, 1);
    assert_eq!(expired.items, 1);
    assert_eq!(expired.bytes, 4);
    assert_eq!(journal.pressure().streams, 0);
}

#[test]
fn abrupt_three_hundred_stream_disconnect_retains_then_expires_every_journal() {
    let journal = replay();
    let session = session(journal.clone(), 400, 400);
    for index in 0..300 {
        let request = request_id(index);
        let stream = stream_id(index);
        session
            .accept(ClientMessage::OpenStream {
                request_id: request.clone(),
                stream_id: stream.clone(),
                trace_context: trace_context(index),
                payload: bytes(b"terminal"),
            })
            .expect("stream request accepted");
        session
            .activate_stream(
                &request,
                &stream,
                terminal_configuration(),
                bytes(b"opened"),
            )
            .expect("stream activates");
    }
    let active = journal.pressure();
    assert_eq!(active.streams, 300);
    assert_eq!(active.active_streams, 300);

    session.close();

    let retained = journal.pressure();
    assert_eq!(retained.streams, 300);
    assert_eq!(retained.active_streams, 0);
    assert_eq!(retained.expiring_streams, 300);
    let expired = journal.expire_before(Instant::now() + Duration::from_secs(31));
    assert_eq!(expired.streams, 300);
    assert_eq!(journal.pressure().streams, 0);
}

#[test]
fn one_server_connection_activates_three_hundred_selective_streams() {
    let journal = replay();
    let session = session(journal, 400, 400);
    for index in 0..300 {
        let request = request_id(index);
        let stream = stream_id(index);
        session
            .accept(ClientMessage::OpenStream {
                request_id: request.clone(),
                stream_id: stream.clone(),
                trace_context: trace_context(index),
                payload: bytes(b"terminal"),
            })
            .expect("stream request accepted");
        session
            .activate_stream(
                &request,
                &stream,
                terminal_configuration(),
                bytes(b"opened"),
            )
            .expect("stream activates");
    }

    let pressure = session.pressure();
    assert_eq!(pressure.logical_streams, 300);
    assert_eq!(pressure.scheduler.active_streams, 300);
    assert_eq!(pressure.pending_requests, 0);

    let mut cursor = None;
    let mut streams = BTreeSet::new();
    let mut pages = 0;
    loop {
        let page = session
            .stream_diagnostic_page(cursor.as_ref(), 23)
            .expect("stream diagnostic page");
        assert!(page.items.len() <= 23);
        assert!(page.cost.stream_entries_examined <= 24);
        assert_eq!(page.cost.active_streams_sampled, page.items.len());
        assert_eq!(page.connection.logical_streams, 300);
        for item in &page.items {
            assert!(matches!(
                &item.state,
                ServerStreamDiagnosticState::Active { .. }
            ));
            assert!(streams.insert(item.stream_id.clone()));
        }
        pages += 1;
        let Some(next) = page.next_after else {
            break;
        };
        cursor = Some(next);
    }
    assert_eq!(streams.len(), 300);
    assert_eq!(pages, 14);
}

#[test]
fn opening_stream_diagnostics_are_bounded_and_payload_free() {
    let session = session(replay(), 4, 4);
    session
        .accept(ClientMessage::OpenStream {
            request_id: request_id(1),
            stream_id: stream_id(1),
            trace_context: trace_context(1),
            payload: bytes(b"PRIVATE-SUBSCRIPTION-SPECIFICATION"),
        })
        .expect("stream opens");

    let page = session
        .stream_diagnostic_page(None, 1)
        .expect("diagnostic page");
    assert_eq!(page.cost.stream_entries_examined, 1);
    assert_eq!(page.cost.active_streams_sampled, 0);
    assert_eq!(page.items[0].queue_byte_limit, 8_192);
    assert_eq!(page.items[0].queue_message_limit, 1_024);
    assert!(matches!(
        &page.items[0].state,
        ServerStreamDiagnosticState::Opening { request_id: pending }
            if pending == &request_id(1)
    ));
    assert!(!format!("{page:?}").contains("PRIVATE-SUBSCRIPTION-SPECIFICATION"));
    assert_eq!(
        session
            .stream_diagnostic_page(None, 101)
            .expect_err("configured page bound applies")
            .category(),
        undercurrent_transport_core::ServerConnectionErrorCategory::Protocol
    );
}

#[test]
fn active_stream_diagnostics_report_the_exact_enforced_nonzero_queue_limits() {
    let limits =
        ConnectionLimits::new(1_024, 4, 4, 65_536, 4_096, 1_024).expect("connection limits");
    let agreement = agreement_for_limits(limits, "connection-diagnostic-limits");
    let scheduler_messages =
        undercurrent_multiplex::SchedulerMessageLimits::new(8, 2, [2, 2, 2, 2])
            .expect("scheduler message limits");
    let scheduler = SchedulerLimits::new(
        limits,
        [4_096, 4_096, 4_096, 4_096],
        limits.connection_queue_bytes(),
        4_096,
        scheduler_messages,
    )
    .expect("scheduler limits");
    let session = ServerConnectionSession::new(
        client_id(),
        agreement,
        scheduler,
        replay(),
        diagnostic_limits(4, 4),
    )
    .expect("server connection");
    let request = request_id(1);
    let stream = stream_id(1);
    session
        .accept(ClientMessage::OpenStream {
            request_id: request.clone(),
            stream_id: stream.clone(),
            trace_context: trace_context(1),
            payload: bytes(b"PRIVATE-SUBSCRIPTION-SPECIFICATION"),
        })
        .expect("stream opens");
    session
        .activate_stream(
            &request,
            &stream,
            terminal_configuration(),
            bytes(b"opened"),
        )
        .expect("stream activates");
    session
        .next_write()
        .expect("dequeue")
        .expect("activation response")
        .confirm_written()
        .expect("activation response confirms");
    for sequence in 1..=32 {
        session
            .publish_stream(
                &stream,
                StreamSequence::new(sequence),
                bytes(b"PRIVATE-TERMINAL-PAYLOAD"),
            )
            .expect("droppable stream remains healthy at saturation");
    }

    let page = session
        .stream_diagnostic_page(None, 1)
        .expect("stream diagnostic page");
    let diagnostic = &page.items[0];
    assert_eq!(diagnostic.queue_byte_limit, limits.stream_queue_bytes());
    assert_eq!(
        diagnostic.queue_message_limit,
        u64::try_from(scheduler_messages.stream_queue_messages())
            .expect("scheduler message limit fits the stable diagnostic integer")
    );
    assert!(diagnostic.queue_byte_limit > 0);
    assert!(diagnostic.queue_message_limit > 0);
    let ServerStreamDiagnosticState::Active { pressure, .. } = &diagnostic.state else {
        panic!("stream must be active");
    };
    assert_eq!(
        u64::try_from(pressure.queued_messages).expect("queued message count fits u64"),
        diagnostic.queue_message_limit
    );
    assert!(pressure.queued_bytes <= diagnostic.queue_byte_limit);
    assert!(
        u64::try_from(pressure.queued_messages).expect("queued message count fits u64")
            <= diagnostic.queue_message_limit
    );
    assert!(pressure.gap_pending);
    let debug = format!("{page:?}");
    assert!(!debug.contains("PRIVATE-SUBSCRIPTION-SPECIFICATION"));
    assert!(!debug.contains("PRIVATE-TERMINAL-PAYLOAD"));

    assert!(ConnectionLimits::new(1_024, 4, 4, 65_536, 0, 1).is_err());
    assert!(undercurrent_multiplex::SchedulerMessageLimits::new(8, 0, [2, 2, 2, 2]).is_err());
}

#[test]
fn successful_physical_write_is_the_replay_commit_boundary() {
    let journal = replay();
    let first = session(journal.clone(), 4, 4);
    let request = request_id(1);
    let stream = stream_id(1);
    first
        .accept(ClientMessage::OpenStream {
            request_id: request.clone(),
            stream_id: stream.clone(),
            trace_context: trace_context(1),
            payload: bytes(b"terminal"),
        })
        .expect("open accepted");
    first
        .activate_stream(
            &request,
            &stream,
            terminal_configuration(),
            bytes(b"opened"),
        )
        .expect("stream activates");
    let response = first.next_write().expect("dequeue").expect("response");
    assert!(response.confirm_written().expect("confirm").is_none());
    first
        .publish_stream(&stream, StreamSequence::new(1), bytes(b"output"))
        .expect("output queues");

    let data = first.next_write().expect("dequeue").expect("data");
    assert_eq!(journal.pressure().items, 0);
    assert!(matches!(
        decode(data.bytes()),
        ServerMessage::StreamItem {
            item: InboundStreamItem::Data { sequence, .. },
            ..
        } if sequence == StreamSequence::new(1)
    ));
    assert!(data.confirm_written().expect("write confirms").is_some());
    assert_eq!(journal.pressure().items, 1);
    first.close();
    assert_eq!(journal.pressure().active_streams, 0);
    assert_eq!(journal.pressure().expiring_streams, 1);

    let resumed = session(journal.clone(), 4, 4);
    let resume_request = request_id(2);
    resumed
        .accept(ClientMessage::ResumeStream {
            request_id: resume_request.clone(),
            stream_id: stream.clone(),
            server_epoch: epoch(),
            after: StreamSequence::INITIAL,
            trace_context: trace_context(2),
            payload: bytes(b"terminal"),
        })
        .expect("resume accepted");
    let outcome = resumed
        .activate_stream(
            &resume_request,
            &stream,
            terminal_configuration(),
            bytes(b"resumed"),
        )
        .expect("resume activates");
    assert_eq!(outcome.replayed_items, 1);
    assert_eq!(outcome.last_sequence, StreamSequence::new(1));
    assert!(outcome.gap.is_none());
    assert_eq!(journal.pressure().active_streams, 1);
    assert_eq!(journal.pressure().expiring_streams, 0);

    let response = resumed.next_write().expect("dequeue").expect("response");
    assert!(matches!(
        decode(response.bytes()),
        ServerMessage::Response { .. }
    ));
    response.confirm_written().expect("response confirms");
    let replayed = resumed.next_write().expect("dequeue").expect("replay");
    assert!(matches!(
        decode(replayed.bytes()),
        ServerMessage::StreamItem {
            item: InboundStreamItem::Data { sequence, .. },
            ..
        } if sequence == StreamSequence::new(1)
    ));
    assert!(
        replayed
            .confirm_written()
            .expect("replay confirms")
            .is_none()
    );
    assert_eq!(journal.pressure().items, 1);
}

#[test]
fn partially_evicted_resume_replays_the_suffix_before_confirming_new_output() {
    let journal = replay_with_evicted_prefix();
    let resumed = session(journal.clone(), 4, 4);
    let request = request_id(2);
    let stream = stream_id(1);
    resumed
        .accept(ClientMessage::ResumeStream {
            request_id: request.clone(),
            stream_id: stream.clone(),
            server_epoch: epoch(),
            after: StreamSequence::INITIAL,
            trace_context: trace_context(2),
            payload: bytes(b"terminal"),
        })
        .expect("resume accepted");
    let outcome = resumed
        .activate_stream(
            &request,
            &stream,
            lifecycle_configuration(),
            bytes(b"resumed"),
        )
        .expect("resume activates");
    assert_eq!(outcome.last_sequence, StreamSequence::new(3));
    assert_eq!(outcome.replayed_items, 2);
    let gap = outcome.gap.expect("evicted prefix is reported");
    assert_eq!(gap.reason, StreamGapReason::ReplayEvicted);
    assert_eq!(gap.first_missing, StreamSequence::new(1));
    assert_eq!(gap.last_missing, StreamSequence::new(1));

    // Queue live output before draining activation to check the complete order.
    resumed
        .publish_stream(
            &stream,
            outcome.last_sequence.next().unwrap(),
            bytes(b"new output"),
        )
        .expect("live output queues");
    let retained = journal.pressure();
    for index in 0..5 {
        let write = resumed.next_write().expect("dequeue").expect("frame");
        match (index, decode(write.bytes())) {
            (0, ServerMessage::Response { request_id, .. }) => assert_eq!(request_id, request),
            (
                1,
                ServerMessage::StreamItem {
                    item: InboundStreamItem::Gap(actual),
                    ..
                },
            ) => {
                assert_eq!(actual, gap);
            }
            (
                2..=4,
                ServerMessage::StreamItem {
                    item:
                        InboundStreamItem::Data {
                            sequence,
                            bytes: payload,
                            ..
                        },
                    ..
                },
            ) => {
                assert_eq!(sequence.value(), index);
                let expected = if index == 4 {
                    bytes(b"new output")
                } else {
                    bytes(&[b'x'; 600])
                };
                assert_eq!(payload, expected);
            }
            (_, message) => panic!("unexpected frame at {index}: {message:?}"),
        }
        let recorded = write
            .confirm_written()
            .expect("write confirmation succeeds");
        assert_eq!(recorded.is_some(), index == 4);
        if index < 4 {
            assert_eq!(
                journal.pressure(),
                retained,
                "replay must not be recorded twice"
            );
        }
    }
    assert!(resumed.next_write().expect("dequeue").is_none());
    assert!(!resumed.is_closed());
}

#[test]
fn evicted_resume_rolls_back_if_the_whole_suffix_cannot_be_queued() {
    let journal = replay_with_evicted_prefix();
    let key = ReplayStreamKey::new(client_id(), stream_id(1));
    let deadline = Instant::now() + Duration::from_secs(60);
    journal.deactivate_stream(&key, deadline);
    let retained = journal.pressure();
    let agreement = agreement(4, 4);
    let limits = agreement.limits();
    let scheduler = SchedulerLimits::new(
        limits,
        [64 * 1_024, 128 * 1_024, 256 * 1_024, 512 * 1_024],
        limits.connection_queue_bytes(),
        u64::from(limits.max_frame_bytes()) * 4,
        undercurrent_multiplex::SchedulerMessageLimits::new(32, 2, [8, 8, 8, 8]).unwrap(),
    )
    .unwrap();
    let resumed = ServerConnectionSession::new(
        client_id(),
        agreement,
        scheduler,
        journal.clone(),
        diagnostic_limits(100, 100),
    )
    .unwrap();
    resumed
        .accept(ClientMessage::ResumeStream {
            request_id: request_id(2),
            stream_id: stream_id(1),
            server_epoch: epoch(),
            after: StreamSequence::INITIAL,
            trace_context: trace_context(2),
            payload: bytes(b"terminal"),
        })
        .unwrap();
    // The gap and first retained item fit; the second retained item must fail.
    let error = resumed
        .activate_stream(
            &request_id(2),
            &stream_id(1),
            lifecycle_configuration(),
            bytes(b"resumed"),
        )
        .expect_err("partial activation must not succeed");
    assert_eq!(error.category(), ServerConnectionErrorCategory::Overloaded);
    assert!(
        resumed.next_write().unwrap().is_none(),
        "response and partial replay must be removed"
    );
    assert_eq!(resumed.pressure().pending_requests, 1);
    assert_eq!(journal.pressure(), retained);
    let activation = journal
        .activate_resume(
            &epoch(),
            key,
            StreamSequence::INITIAL,
            StreamRecoveryAction::RestartQuery,
            deadline - Duration::from_nanos(1),
        )
        .expect("another activation can claim the rolled-back journal");
    assert_eq!(activation.previous_expiration, Some(deadline));
    assert!(matches!(activation.replay,
        undercurrent_multiplex::ReplayResult::GapWithItems { items, .. } if items.len() == 2));
}

fn replay_with_evicted_prefix() -> Arc<ReplayJournal> {
    let stream = stream_id(1);
    let payload = bytes(&[b'x'; 600]);
    let frame_bytes = WireCodec::new(1_024)
        .unwrap()
        .encode_server(&ServerMessage::StreamItem {
            stream_id: stream.clone(),
            item: InboundStreamItem::Data {
                sequence: StreamSequence::new(1),
                bytes: payload.clone(),
                flow_controlled_bytes: 0,
            },
        })
        .unwrap()
        .len() as u64;
    let journal = Arc::new(ReplayJournal::new(
        epoch(),
        ReplayLimits::new(4, frame_bytes * 2, frame_bytes * 8, 1_024).unwrap(),
    ));
    let first = session(journal.clone(), 4, 4);
    let request = request_id(1);
    first
        .accept(ClientMessage::OpenStream {
            request_id: request.clone(),
            stream_id: stream.clone(),
            trace_context: trace_context(1),
            payload: bytes(b"terminal"),
        })
        .unwrap();
    first
        .activate_stream(
            &request,
            &stream,
            lifecycle_configuration(),
            bytes(b"opened"),
        )
        .unwrap();
    first
        .next_write()
        .unwrap()
        .unwrap()
        .confirm_written()
        .unwrap();
    for sequence in 1..=3 {
        first
            .publish_stream(&stream, StreamSequence::new(sequence), payload.clone())
            .unwrap();
        first
            .next_write()
            .unwrap()
            .unwrap()
            .confirm_written()
            .unwrap();
    }
    assert_eq!(journal.pressure().items, 2);
    first.close();
    journal
}

#[test]
fn dropping_unconfirmed_pending_write_does_not_commit_replay() {
    let journal = replay();
    let session = session(journal.clone(), 4, 4);
    let request = request_id(1);
    let stream = stream_id(1);
    session
        .accept(ClientMessage::OpenStream {
            request_id: request.clone(),
            stream_id: stream.clone(),
            trace_context: trace_context(1),
            payload: bytes(b"terminal"),
        })
        .expect("open accepted");
    session
        .activate_stream(
            &request,
            &stream,
            terminal_configuration(),
            bytes(b"opened"),
        )
        .expect("stream activates");
    session
        .next_write()
        .expect("dequeue")
        .expect("activation response")
        .confirm_written()
        .expect("activation response confirms");
    session
        .publish_stream(
            &stream,
            StreamSequence::new(1),
            bytes(b"unconfirmed-output"),
        )
        .expect("output queues");

    let pending = session.next_write().expect("dequeue").expect("output");
    assert_eq!(journal.pressure().items, 0);
    drop(pending);
    assert_eq!(journal.pressure().items, 0);
}

#[test]
fn source_retention_loss_is_published_before_later_stream_data() {
    let journal = replay();
    let session = session(journal.clone(), 4, 4);
    let request = request_id(1);
    let stream = stream_id(1);
    session
        .accept(ClientMessage::OpenStream {
            request_id: request.clone(),
            stream_id: stream.clone(),
            trace_context: trace_context(1),
            payload: bytes(b"terminal"),
        })
        .expect("open accepted");
    session
        .activate_stream(
            &request,
            &stream,
            terminal_configuration(),
            bytes(b"opened"),
        )
        .expect("stream activates");
    session
        .next_write()
        .expect("dequeue")
        .expect("response")
        .confirm_written()
        .expect("response confirms");

    let recorded = session
        .publish_source_gap(&stream, StreamSequence::new(1))
        .expect("source gap publishes");
    assert_eq!(recorded.reason, StreamGapReason::SourceRetentionEvicted);
    assert_eq!(
        recorded.recovery,
        StreamRecoveryAction::RestartFromAvailableOffset
    );
    session
        .publish_stream(&stream, StreamSequence::new(2), bytes(b"retained-output"))
        .expect("later output queues");

    let gap = session.next_write().expect("dequeue").expect("gap");
    assert!(matches!(
        decode(gap.bytes()),
        ServerMessage::StreamItem {
            stream_id: ref actual_stream,
            item: InboundStreamItem::Gap(ref actual_gap),
        } if actual_stream == &stream
            && actual_gap.first_missing == StreamSequence::new(1)
            && actual_gap.last_missing == StreamSequence::new(1)
            && actual_gap.reason == StreamGapReason::SourceRetentionEvicted
            && actual_gap.recovery == StreamRecoveryAction::RestartFromAvailableOffset
    ));
    assert!(gap.confirm_written().expect("gap confirms").is_some());

    let data = session.next_write().expect("dequeue").expect("data");
    assert!(matches!(
        decode(data.bytes()),
        ServerMessage::StreamItem {
            item: InboundStreamItem::Data { sequence, .. },
            ..
        } if sequence == StreamSequence::new(2)
    ));
    assert!(data.confirm_written().expect("data confirms").is_some());
    assert_eq!(journal.pressure().items, 2);
}

#[test]
fn separated_source_gaps_preserve_reliable_data_through_confirmation_and_consumption() {
    let journal = replay();
    let session = session(journal.clone(), 4, 4);
    let request = request_id(1);
    let stream = stream_id(1);
    session
        .accept(ClientMessage::OpenStream {
            request_id: request.clone(),
            stream_id: stream.clone(),
            trace_context: trace_context(1),
            payload: bytes(b"events"),
        })
        .unwrap();
    session
        .activate_stream(
            &request,
            &stream,
            lifecycle_configuration(),
            bytes(b"opened"),
        )
        .unwrap();
    session
        .next_write()
        .unwrap()
        .unwrap()
        .confirm_written()
        .unwrap();
    session
        .publish_source_gap(&stream, StreamSequence::new(1))
        .unwrap();
    session
        .publish_stream(&stream, StreamSequence::new(2), bytes(b"reliable event"))
        .unwrap();
    session
        .publish_source_gap(&stream, StreamSequence::new(3))
        .unwrap();

    let router = undercurrent_multiplex::ConnectionRouter::new(session.agreement().limits());
    let subscription = router.open_stream(stream.clone(), 4, 4_096).unwrap();
    for expected in 1..=3 {
        let write = session.next_write().unwrap().expect("ordered gap or data");
        let ServerMessage::StreamItem { item, .. } = decode(write.bytes()) else {
            panic!("stream item");
        };
        router
            .route_stream_item(&stream, item)
            .expect("routing stays ordered");
        write
            .confirm_written()
            .expect("replay confirmation stays ordered");
        let consumed = subscription.recv().unwrap();
        assert_eq!(consumed.sequence().value(), expected);
        assert_eq!(subscription.last_sequence().value(), expected);
        match consumed {
            InboundStreamItem::Gap(gap) => assert_eq!(gap.first_missing, gap.last_missing),
            InboundStreamItem::Data { bytes: payload, .. } => {
                assert_eq!(payload, bytes(b"reliable event"))
            }
        }
    }
    assert_eq!(journal.pressure().items, 3);
    assert!(!session.is_closed());
}

#[test]
fn retained_source_gaps_preserve_credit_and_stream_order() {
    assert_retained_gap_credit(false);
}

#[test]
fn retained_synthetic_resume_gaps_preserve_credit_and_stream_order() {
    assert_retained_gap_credit(true);
}

fn assert_retained_gap_credit(synthetic: bool) {
    let journal = replay();
    let first = session(journal.clone(), 4, 4);
    let stream = stream_id(1);
    let request = request_id(1);
    let payload = bytes(b"terminal");
    let opening = if synthetic {
        ClientMessage::ResumeStream {
            request_id: request.clone(),
            stream_id: stream.clone(),
            server_epoch: epoch(),
            after: StreamSequence::INITIAL,
            trace_context: trace_context(1),
            payload,
        }
    } else {
        ClientMessage::OpenStream {
            request_id: request.clone(),
            stream_id: stream.clone(),
            trace_context: trace_context(1),
            payload,
        }
    };
    first.accept(opening).unwrap();
    let configuration = ServerStreamConfiguration {
        delivery: DeliverySemantics::Reliable,
        ..terminal_configuration()
    };
    first
        .activate_stream(&request, &stream, configuration, bytes(b"opened"))
        .unwrap();
    first
        .next_write()
        .unwrap()
        .unwrap()
        .confirm_written()
        .unwrap();
    let data_sequence = if synthetic { 2 } else { 1 };
    first
        .publish_stream(
            &stream,
            StreamSequence::new(data_sequence),
            bytes(b"before gap"),
        )
        .unwrap();
    first
        .publish_source_gap(&stream, StreamSequence::new(data_sequence + 1))
        .unwrap();
    first
        .publish_stream(
            &stream,
            StreamSequence::new(data_sequence + 2),
            bytes(b"after gap"),
        )
        .unwrap();
    let mut originals = Vec::new();
    while let Some(write) = first.next_write().unwrap() {
        originals.push(write.shared_bytes());
        write.confirm_written().unwrap();
    }
    first.close();
    let resumed = session(journal, 4, 4);
    resumed
        .accept(ClientMessage::ResumeStream {
            request_id: request_id(2),
            stream_id: stream.clone(),
            server_epoch: epoch(),
            after: StreamSequence::INITIAL,
            trace_context: trace_context(2),
            payload: bytes(b"terminal"),
        })
        .unwrap();
    let outcome = resumed
        .activate_stream(&request_id(2), &stream, configuration, bytes(b"resumed"))
        .unwrap();
    assert_eq!(outcome.replayed_items, originals.len());
    resumed
        .next_write()
        .unwrap()
        .unwrap()
        .confirm_written()
        .unwrap();
    let router = undercurrent_multiplex::ConnectionRouter::new(resumed.agreement().limits());
    let subscription = router.open_stream(stream.clone(), 8, 4_096).unwrap();
    for (index, original) in originals.iter().enumerate() {
        let before = credit_windows(&resumed);
        let write = resumed.next_write().unwrap().expect("retained frame");
        assert_eq!(
            write.bytes(),
            original.as_ref(),
            "exact bytes and stream order survive replay"
        );
        let ServerMessage::StreamItem { item, .. } = decode(write.bytes()) else {
            panic!("stream item");
        };
        let charge = item.encoded_len();
        router.route_stream_item(&stream, item).unwrap();
        let consumed = subscription.recv().unwrap();
        assert_eq!(consumed.sequence().value(), index as u64 + 1);
        let after = credit_windows(&resumed);
        assert_eq!(
            after,
            (before.0 - charge, before.1 - charge),
            "server debit must match consumed-item credit"
        );
        assert!(write.confirm_written().unwrap().is_none());
    }
}

fn credit_windows(session: &ServerConnectionSession) -> (u64, u64) {
    let page = session.stream_diagnostic_page(None, 1).unwrap();
    let ServerStreamDiagnosticState::Active { pressure, .. } = &page.items[0].state else {
        panic!("active stream");
    };
    (
        page.connection.scheduler.connection_credit_bytes,
        pressure.credit_bytes,
    )
}

#[test]
fn unavailable_resume_queues_a_typed_gap_and_retains_it_after_write() {
    let journal = replay();
    let session = session(journal.clone(), 4, 4);
    let request = request_id(1);
    let stream = stream_id(1);
    session
        .accept(ClientMessage::ResumeStream {
            request_id: request.clone(),
            stream_id: stream.clone(),
            server_epoch: epoch(),
            after: StreamSequence::INITIAL,
            trace_context: trace_context(1),
            payload: bytes(b"terminal"),
        })
        .expect("resume accepted");
    let outcome = session
        .activate_stream(
            &request,
            &stream,
            terminal_configuration(),
            bytes(b"resumed"),
        )
        .expect("resume activates with gap");
    assert!(matches!(
        outcome.gap,
        Some(ref gap) if gap.reason == StreamGapReason::ReplayUnavailable
    ));
    assert_eq!(outcome.last_sequence, StreamSequence::new(1));

    session
        .next_write()
        .expect("dequeue")
        .expect("response")
        .confirm_written()
        .expect("response confirms");
    let gap = session.next_write().expect("dequeue").expect("gap");
    assert!(matches!(
        decode(gap.bytes()),
        ServerMessage::StreamItem {
            item: InboundStreamItem::Gap(ref gap),
            ..
        } if gap.reason == StreamGapReason::ReplayUnavailable
    ));
    assert!(gap.confirm_written().expect("gap confirms").is_some());
    assert_eq!(journal.pressure().items, 1);
}

#[test]
fn heartbeat_control_overtakes_unrelated_terminal_output() {
    let journal = replay();
    let session = session(journal, 4, 4);
    let request = request_id(1);
    let stream = stream_id(1);
    session
        .accept(ClientMessage::OpenStream {
            request_id: request.clone(),
            stream_id: stream.clone(),
            trace_context: trace_context(1),
            payload: bytes(b"terminal"),
        })
        .expect("open accepted");
    session
        .activate_stream(
            &request,
            &stream,
            terminal_configuration(),
            bytes(b"opened"),
        )
        .expect("stream activates");
    session
        .next_write()
        .expect("dequeue")
        .expect("response")
        .confirm_written()
        .expect("response confirms");
    session
        .publish_stream(&stream, StreamSequence::new(1), bytes(&vec![7; 512]))
        .expect("bulk queues");
    session.accept(ClientMessage::Ping).expect("ping accepted");

    let first = session.next_write().expect("dequeue").expect("pong");
    assert_eq!(decode(first.bytes()), ServerMessage::Pong);
    first.confirm_written().expect("pong confirms");
    let second = session.next_write().expect("dequeue").expect("bulk");
    assert!(matches!(
        decode(second.bytes()),
        ServerMessage::StreamItem { .. }
    ));
}

#[test]
fn terminal_flood_cannot_starve_finite_control_or_lifecycle_traffic() {
    let session = session(replay(), 8, 4);
    let terminal_request = request_id(1);
    let terminal_stream = stream_id(1);
    session
        .accept(ClientMessage::OpenStream {
            request_id: terminal_request.clone(),
            stream_id: terminal_stream.clone(),
            trace_context: trace_context(1),
            payload: bytes(b"terminal-output"),
        })
        .expect("terminal stream accepted");
    session
        .activate_stream(
            &terminal_request,
            &terminal_stream,
            terminal_configuration(),
            bytes(b"opened"),
        )
        .expect("terminal stream activates");

    let lifecycle_request = request_id(2);
    let lifecycle_stream = stream_id(2);
    session
        .accept(ClientMessage::OpenStream {
            request_id: lifecycle_request.clone(),
            stream_id: lifecycle_stream.clone(),
            trace_context: trace_context(2),
            payload: bytes(b"session-events"),
        })
        .expect("lifecycle stream accepted");
    session
        .activate_stream(
            &lifecycle_request,
            &lifecycle_stream,
            lifecycle_configuration(),
            bytes(b"opened"),
        )
        .expect("lifecycle stream activates");
    for _ in 0..2 {
        session
            .next_write()
            .expect("dequeue")
            .expect("stream response")
            .confirm_written()
            .expect("stream response confirms");
    }

    let bulk_payload = bytes(&[7; 900]);
    for sequence in 1..=400_u64 {
        session
            .publish_stream(
                &terminal_stream,
                StreamSequence::new(sequence),
                bulk_payload.clone(),
            )
            .expect("terminal flood queues within its bulk reservation");
    }

    let operations = ["list", "inspect", "input", "stop"];
    for (offset, operation) in operations.iter().enumerate() {
        let index = 100 + offset;
        assert!(matches!(
            session
                .accept(ClientMessage::Request {
                    request_id: request_id(index),
                    trace_context: trace_context(index),
                    payload: bytes(operation.as_bytes()),
                })
                .expect("finite request accepted")
                .as_slice(),
            [ServerAction::Request { .. }]
        ));
    }
    session.accept(ClientMessage::Ping).expect("ping accepted");
    for (offset, operation) in operations.iter().enumerate() {
        session
            .complete_request(&request_id(100 + offset), bytes(operation.as_bytes()))
            .expect("finite response queues");
    }
    session
        .publish_stream(
            &lifecycle_stream,
            StreamSequence::new(1),
            bytes(b"session-stopped"),
        )
        .expect("lifecycle event queues");

    assert_eq!(
        decode(
            session
                .next_write()
                .expect("dequeue")
                .expect("pong")
                .bytes()
        ),
        ServerMessage::Pong
    );
    for (offset, operation) in operations.iter().enumerate() {
        assert_eq!(
            decode(
                session
                    .next_write()
                    .expect("dequeue")
                    .expect("finite response")
                    .bytes()
            ),
            ServerMessage::Response {
                request_id: request_id(100 + offset),
                payload: bytes(operation.as_bytes()),
            }
        );
    }
    assert!(matches!(
        decode(
            session
                .next_write()
                .expect("dequeue")
                .expect("lifecycle event")
                .bytes()
        ),
        ServerMessage::StreamItem {
            stream_id,
            item: InboundStreamItem::Data { sequence, .. },
        } if stream_id == lifecycle_stream && sequence == StreamSequence::new(1)
    ));
    assert!(matches!(
        decode(
            session
                .next_write()
                .expect("dequeue")
                .expect("terminal output")
                .bytes()
        ),
        ServerMessage::StreamItem {
            stream_id,
            item: InboundStreamItem::Data { .. },
        } if stream_id == terminal_stream
    ));
}

#[test]
fn unsubscribe_and_connection_close_release_only_owned_state() {
    let journal = replay();
    let session = session(journal, 4, 4);
    for index in 1..=2 {
        let request = request_id(index);
        let stream = stream_id(index);
        session
            .accept(ClientMessage::OpenStream {
                request_id: request.clone(),
                stream_id: stream.clone(),
                trace_context: trace_context(index),
                payload: bytes(b"terminal"),
            })
            .expect("open accepted");
        session
            .activate_stream(
                &request,
                &stream,
                terminal_configuration(),
                bytes(b"opened"),
            )
            .expect("stream activates");
    }
    let actions = session
        .accept(ClientMessage::CloseStream {
            stream_id: stream_id(1),
        })
        .expect("one stream closes");
    assert_eq!(
        actions,
        vec![ServerAction::CloseStream {
            stream_id: stream_id(1)
        }]
    );
    assert_eq!(session.pressure().logical_streams, 1);

    let release = session.close();
    assert_eq!(release.logical_streams, 1);
    assert!(session.pressure().closed);
    assert_eq!(session.close().logical_streams, 0);
}

#[test]
fn unsubscribe_during_an_in_flight_physical_write_does_not_fail_peer_work() {
    let session = session(replay(), 4, 4);
    let request = request_id(1);
    let stream = stream_id(1);
    session
        .accept(ClientMessage::OpenStream {
            request_id: request.clone(),
            stream_id: stream.clone(),
            trace_context: trace_context(1),
            payload: bytes(b"terminal"),
        })
        .unwrap();
    session
        .activate_stream(
            &request,
            &stream,
            terminal_configuration(),
            bytes(b"opened"),
        )
        .unwrap();
    session
        .next_write()
        .unwrap()
        .unwrap()
        .confirm_written()
        .unwrap();
    session
        .publish_stream(&stream, StreamSequence::new(1), bytes(b"output"))
        .unwrap();
    let write = session.next_write().unwrap().unwrap();
    session
        .accept(ClientMessage::CloseStream { stream_id: stream })
        .unwrap();
    assert!(
        write
            .confirm_written()
            .expect("a closed journal needs no retention")
            .is_none()
    );
    session.accept(ClientMessage::Ping).unwrap();
    assert!(matches!(
        decode(session.next_write().unwrap().unwrap().bytes()),
        ServerMessage::Pong
    ));
}

#[test]
fn server_connection_diagnostic_registry_is_unique_and_stable() {
    let names = ServerConnectionDiagnosticName::ALL
        .map(ServerConnectionDiagnosticName::as_str)
        .into_iter()
        .collect::<BTreeSet<_>>();

    assert_eq!(names.len(), ServerConnectionDiagnosticName::ALL.len());

    let error_categories = ServerConnectionErrorCategory::ALL
        .map(ServerConnectionErrorCategory::as_str)
        .into_iter()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        error_categories.len(),
        ServerConnectionErrorCategory::ALL.len()
    );
}

#[test]
fn active_registry_enforces_one_connection_per_client_and_releases_on_drop() {
    let registry = ActiveConnectionRegistry::new(2, diagnostic_limits(2, 2)).expect("registry");
    let first_agreement = agreement(4, 4);
    let first = registry
        .register(client_id(), &first_agreement)
        .expect("first connection registers");

    assert!(matches!(
        registry.register(client_id(), &first_agreement),
        Err(ServerConnectionError::ClientAlreadyConnected { .. })
    ));
    assert!(matches!(
        registry.register(client_id_at(2), &first_agreement),
        Err(ServerConnectionError::ConnectionIdInUse { .. })
    ));
    let second_agreement = agreement_for(4, 4, "connection-2");
    let second = registry
        .register(client_id_at(2), &second_agreement)
        .expect("second connection registers");
    let third_agreement = agreement_for(4, 4, "connection-3");
    let capacity = match registry.register(client_id_at(3), &third_agreement) {
        Err(error) => error,
        Ok(_) => panic!("third physical connection must exceed capacity"),
    };
    let ServerConnectionError::Multiplex(capacity) = capacity else {
        panic!("multiplex capacity error expected");
    };
    let context = capacity.overload().expect("server overload context");
    assert_eq!(context.resource(), "active_connections");
    assert_eq!(context.limit(), Some(2));
    assert_eq!(context.current(), Some(2));
    assert_eq!(context.requested(), Some(1));
    assert_eq!(
        context.retry(),
        undercurrent_multiplex::MultiplexRetryGuidance::AfterCapacityAvailable
    );
    assert_eq!(registry.pressure().connections, 2);
    drop(first);
    assert_eq!(registry.pressure().connections, 1);
    registry
        .register(client_id(), &first_agreement)
        .expect("client reconnects after teardown");
    drop(second);
}

#[test]
fn active_connection_diagnostics_page_three_hundred_live_sessions_with_bounded_work() {
    let registry = ActiveConnectionRegistry::new(300, diagnostic_limits(17, 17)).expect("registry");
    let replay = replay();
    let mut sessions = Vec::with_capacity(300);
    let mut leases = Vec::with_capacity(300);
    for index in 1..=300 {
        let session = Arc::new(session_for(
            replay.clone(),
            index,
            &format!("connection-{index:03}"),
        ));
        leases.push(
            registry
                .register_session(&session)
                .expect("session registers"),
        );
        sessions.push(session);
    }

    let mut cursor = None;
    let mut clients = BTreeSet::new();
    let mut pages = 0;
    loop {
        let page = registry
            .diagnostic_page(cursor.as_ref(), 17)
            .expect("diagnostic page");
        assert!(page.items.len() <= 17);
        assert!(page.cost.registry_entries_examined <= 18);
        assert_eq!(page.cost.sessions_sampled, page.items.len());
        assert_eq!(page.registry.connections, 300);
        for item in &page.items {
            assert!(item.pressure.is_some());
            assert!(clients.insert(item.client_id.clone()));
        }
        pages += 1;
        let Some(next) = page.next_after else {
            break;
        };
        cursor = Some(next);
    }

    assert_eq!(clients.len(), 300);
    assert_eq!(pages, 18);
    assert_eq!(sessions.len(), 300);
    assert_eq!(leases.len(), 300);
}

#[test]
fn active_connection_diagnostics_do_not_retain_sessions_or_payloads() {
    let registry = ActiveConnectionRegistry::new(2, diagnostic_limits(1, 1)).expect("registry");
    let session = Arc::new(session_for(replay(), 1, "connection-1"));
    session
        .accept(ClientMessage::Request {
            request_id: request_id(1),
            trace_context: trace_context(1),
            payload: bytes(b"PRIVATE-TERMINAL-PAYLOAD"),
        })
        .expect("request accepted");
    let lease = registry
        .register_session(&session)
        .expect("session registers");

    let live = registry
        .diagnostic_page(None, 1)
        .expect("live diagnostic page");
    assert_eq!(live.cost.sessions_sampled, 1);
    assert_eq!(
        live.items[0]
            .pressure
            .expect("live pressure")
            .pending_requests,
        1
    );
    assert!(!format!("{live:?}").contains("PRIVATE-TERMINAL-PAYLOAD"));

    drop(session);
    let detached = registry
        .diagnostic_page(None, 1)
        .expect("detached diagnostic page");
    assert_eq!(detached.registry.connections, 1);
    assert_eq!(detached.cost.sessions_sampled, 0);
    assert_eq!(detached.items[0].pressure, None);
    drop(lease);
    assert_eq!(registry.pressure().connections, 0);
}

#[test]
fn active_registry_delegates_bounded_stream_diagnostics_without_payloads() {
    let registry = ActiveConnectionRegistry::new(1, diagnostic_limits(1, 1)).expect("registry");
    let session = Arc::new(session_for(replay(), 1, "connection-1"));
    session
        .accept(ClientMessage::OpenStream {
            request_id: request_id(1),
            stream_id: stream_id(1),
            trace_context: trace_context(1),
            payload: bytes(b"PRIVATE-SUBSCRIPTION-SPECIFICATION"),
        })
        .expect("stream opens");
    let _lease = registry
        .register_session(&session)
        .expect("session registers");

    let page = registry
        .stream_diagnostic_page(session.client_id(), None, 1)
        .expect("stream diagnostic read")
        .expect("live connection");
    assert_eq!(page.items.len(), 1);
    assert!(matches!(
        page.items[0].state,
        ServerStreamDiagnosticState::Opening { .. }
    ));
    assert!(!format!("{page:?}").contains("PRIVATE-SUBSCRIPTION-SPECIFICATION"));

    drop(session);
    assert_eq!(
        registry
            .stream_diagnostic_page(&client_id_at(1), None, 1)
            .expect("detached diagnostic read"),
        None
    );
}

#[test]
fn active_connection_diagnostic_page_size_is_hard_bounded() {
    let registry = ActiveConnectionRegistry::new(2, diagnostic_limits(1, 1)).expect("registry");

    assert!(matches!(
        registry.diagnostic_page(None, 0),
        Err(undercurrent_wire::core::PaginationValueError::ZeroPageSize)
    ));
    assert!(matches!(
        registry.diagnostic_page(None, 2),
        Err(
            undercurrent_wire::core::PaginationValueError::PageSizeExceeded {
                requested: 2,
                maximum: 1
            }
        )
    ));
    assert!(ServerDiagnosticLimits::new(0, 1).is_err());
    assert!(ServerDiagnosticLimits::new(1, 0).is_err());
    assert_eq!(
        ServerDiagnosticLimits::new(0, 1)
            .expect_err("zero connection page limit is rejected")
            .category(),
        ServerConnectionErrorCategory::Configuration
    );
}

fn session(
    replay: Arc<ReplayJournal>,
    max_pending_requests: u32,
    max_streams: u32,
) -> ServerConnectionSession {
    let agreement = agreement(max_pending_requests, max_streams);
    let limits = agreement.limits();
    let scheduler = SchedulerLimits::new(
        limits,
        [64 * 1_024, 128 * 1_024, 256 * 1_024, 512 * 1_024],
        limits.connection_queue_bytes(),
        u64::from(limits.max_frame_bytes()) * 4,
        message_limits(),
    )
    .expect("scheduler limits");
    ServerConnectionSession::new(
        client_id(),
        agreement,
        scheduler,
        replay,
        diagnostic_limits(100, 100),
    )
    .expect("server connection")
}

fn session_for(
    replay: Arc<ReplayJournal>,
    client_index: usize,
    connection_id: &str,
) -> ServerConnectionSession {
    let agreement = agreement_for(4, 4, connection_id);
    let limits = agreement.limits();
    let scheduler = SchedulerLimits::new(
        limits,
        [64 * 1_024, 128 * 1_024, 256 * 1_024, 512 * 1_024],
        limits.connection_queue_bytes(),
        u64::from(limits.max_frame_bytes()) * 4,
        message_limits(),
    )
    .expect("scheduler limits");
    ServerConnectionSession::new(
        client_id_at(client_index),
        agreement,
        scheduler,
        replay,
        diagnostic_limits(100, 100),
    )
    .expect("server connection")
}

fn credit_limited_session(replay: Arc<ReplayJournal>) -> ServerConnectionSession {
    let limits =
        ConnectionLimits::new(1_024, 4, 4, 1_000_000, 8_192, 256).expect("connection limits");
    let agreement = agreement_for_limits(limits, "connection-credit-limited");
    let scheduler = SchedulerLimits::new(
        limits,
        [64 * 1_024, 128 * 1_024, 256 * 1_024, 512 * 1_024],
        256,
        u64::from(limits.max_frame_bytes()) * 4,
        message_limits(),
    )
    .expect("scheduler limits");
    ServerConnectionSession::new(
        client_id(),
        agreement,
        scheduler,
        replay,
        diagnostic_limits(100, 100),
    )
    .expect("server connection")
}

fn message_limits() -> undercurrent_multiplex::SchedulerMessageLimits {
    undercurrent_multiplex::SchedulerMessageLimits::new(8_192, 1_024, [1_024, 2_048, 2_048, 3_072])
        .expect("scheduler message limits")
}

fn diagnostic_limits(connection_page_size: u32, stream_page_size: u32) -> ServerDiagnosticLimits {
    ServerDiagnosticLimits::new(connection_page_size, stream_page_size).expect("diagnostic limits")
}

fn agreement(max_pending_requests: u32, max_streams: u32) -> ConnectionAgreement {
    agreement_for(max_pending_requests, max_streams, "connection-1")
}

fn agreement_for(
    max_pending_requests: u32,
    max_streams: u32,
    connection_id: &str,
) -> ConnectionAgreement {
    let limits = ConnectionLimits::new(
        1_024,
        max_streams,
        max_pending_requests,
        1_000_000,
        8_192,
        4_096,
    )
    .expect("connection limits");
    agreement_for_limits(limits, connection_id)
}

fn agreement_for_limits(limits: ConnectionLimits, connection_id: &str) -> ConnectionAgreement {
    let capabilities = BTreeSet::from([
        capability(connection_capabilities::MULTIPLEX),
        capability(connection_capabilities::FLOW_CONTROL),
        capability(connection_capabilities::STREAM_RESUME),
        capability(connection_capabilities::BINARY_PAYLOADS),
        capability(connection_capabilities::TRACE_CONTEXT),
    ]);
    let hello = ConnectionHello::new(
        client_id(),
        CURRENT_PROTOCOL_VERSION,
        capabilities.clone(),
        BTreeSet::new(),
        limits,
    )
    .expect("hello");
    let policy = ConnectionPolicy::new(
        CURRENT_PROTOCOL_VERSION,
        capabilities,
        limits,
        HeartbeatPolicy::new(5_000, 15_000).expect("heartbeat"),
        Some(30_000),
    )
    .expect("policy");
    negotiate_connection(
        &hello,
        &policy,
        ConnectionId::new(connection_id).expect("connection ID"),
        epoch(),
    )
    .expect("agreement")
}

fn replay() -> Arc<ReplayJournal> {
    Arc::new(ReplayJournal::new(
        epoch(),
        ReplayLimits::new(1_000, 8_192, 8_192_000, 1_024).expect("replay limits"),
    ))
}

fn terminal_configuration() -> ServerStreamConfiguration {
    ServerStreamConfiguration {
        kind: StreamKind::TerminalOutput,
        class: TrafficClass::Bulk,
        delivery: DeliverySemantics::Droppable,
        recovery: StreamRecoveryAction::RestartFromAvailableOffset,
        retain_for_resume: true,
    }
}

fn lifecycle_configuration() -> ServerStreamConfiguration {
    ServerStreamConfiguration {
        kind: StreamKind::SessionEvents,
        class: TrafficClass::Lifecycle,
        delivery: DeliverySemantics::Reliable,
        recovery: StreamRecoveryAction::RestartQuery,
        retain_for_resume: true,
    }
}

fn decode(bytes: &[u8]) -> ServerMessage {
    match WireCodec::new(1_024)
        .expect("codec")
        .decode_server(bytes)
        .expect("decode")
    {
        DecodeStatus::Complete(frame) => frame.message,
        DecodeStatus::NeedMore { .. } => panic!("expected complete frame"),
    }
}

fn client_id() -> ClientId {
    client_id_at(1)
}

fn client_id_at(index: usize) -> ClientId {
    ClientId::new(format!("client-{index}")).expect("client ID")
}

fn epoch() -> ServerEpoch {
    ServerEpoch::new("epoch-1").expect("server epoch")
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
}

fn capability(value: &str) -> NamespacedName {
    NamespacedName::new(value).expect("capability")
}

fn bytes(value: &[u8]) -> Arc<[u8]> {
    Arc::from(value)
}

#[derive(Default)]
struct CountingDrainWake {
    count: AtomicUsize,
}

impl ServerQueueDrainWake for CountingDrainWake {
    fn wake(&self) {
        self.count.fetch_add(1, Ordering::AcqRel);
    }
}
