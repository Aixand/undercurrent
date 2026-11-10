// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeSet, VecDeque};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Condvar, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use undercurrent_multiplex::{ClientMessage, InboundStreamItem, MultiplexError, ServerMessage};
use undercurrent_sdk::{
    Client, ClientConnectError, ClientConnectionAgreement, ClientConnector, ClientDiagnosticName,
    ClientError, ClientErrorCategory, ClientMetricName, ClientRecoveryCancellation,
    ClientRecoveryError, ClientRecoveryJitter, ClientRecoveryPolicy, ClientRetryGuidance,
    ClientRuntimeLimits, ClientServerEpoch, ClientStreamId, ClientStreamItem, ClientStreamSequence,
    ClientTraceContext, ClientTransport, ClientTransportError, ClientTransportInbound,
    ClientTransportOperation, ClientTransportOutbound, NegotiatedClientTransport,
    PersistentRecoveryFailure, ResilientClient, ResilientClientPhase, ResilientSubscriptionError,
    SubscriptionCheckpoint, recover_client_connection,
};
use undercurrent_wire::core::NamespacedName;
use undercurrent_wire::{
    CURRENT_PROTOCOL_VERSION, ClientId, ConnectionAgreement, ConnectionHello, ConnectionId,
    ConnectionLimits, ConnectionPolicy, DecodeStatus, HandshakeCodec, HandshakeResponse,
    HeartbeatPolicy, ServerEpoch, StreamSequence, WireCodec, connection_capabilities,
    negotiate_connection,
};

#[test]
fn client_requires_multiplex_and_flow_control_agreement() {
    let transport = Arc::new(MemoryTransport::new(false));

    let error = match Client::connect(agreement(false), transport, client_limits(8)) {
        Ok(_) => panic!("missing capability must fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        ClientConnectError::MissingRequiredCapability { .. }
    ));
}

#[test]
fn sdk_boundary_rejects_a_future_protocol_agreement() {
    let limits =
        ConnectionLimits::new(1_024, 8, 8, 16_384, 4_096, 1_024).expect("connection limits");
    let capabilities = BTreeSet::from([
        capability(connection_capabilities::MULTIPLEX),
        capability(connection_capabilities::FLOW_CONTROL),
        capability(connection_capabilities::TRACE_CONTEXT),
    ]);
    let hello = ConnectionHello::new(
        ClientId::new("future-client").expect("client ID"),
        CURRENT_PROTOCOL_VERSION,
        capabilities.clone(),
        BTreeSet::new(),
        limits,
    )
    .expect("future hello");
    let policy = ConnectionPolicy::new(
        CURRENT_PROTOCOL_VERSION,
        capabilities,
        limits,
        HeartbeatPolicy::new(5_000, 15_000).expect("heartbeat"),
        None,
    )
    .expect("future policy");
    let agreement = negotiate_connection(
        &hello,
        &policy,
        ConnectionId::new("future-connection").expect("connection ID"),
        ServerEpoch::new("future-epoch").expect("server epoch"),
    )
    .expect("current agreement");
    let mut bytes = HandshakeCodec::new(TEST_FRAME_BYTES)
        .expect("handshake codec")
        .encode_response(&HandshakeResponse::Accepted(agreement))
        .expect("encode current agreement");
    let current = b"\"protocol_version\":\"0.1.0-beta\"";
    let offset = bytes
        .windows(current.len())
        .position(|window| window == current)
        .expect("encoded protocol version");
    bytes[offset + current.len() - 6] = b'1';

    assert!(
        ClientConnectionAgreement::decode_accepted(&bytes, TEST_FRAME_BYTES).is_err(),
        "SDK must reject future protocol agreements"
    );
}

#[test]
fn cloned_client_routes_concurrent_responses_out_of_order_on_one_transport() {
    let transport = Arc::new(MemoryTransport::new(false));
    let client = Client::connect(agreement(true), transport.clone(), client_limits(8))
        .expect("client connects");
    let first_client = client.clone();
    let second_client = client.clone();
    let first = thread::spawn(move || {
        first_client.request(Arc::from(&b"first"[..]), Duration::from_secs(1))
    });
    let second = thread::spawn(move || {
        second_client.request(Arc::from(&b"second"[..]), Duration::from_secs(1))
    });
    let sent = transport.wait_for_sent(2);
    let requests = sent
        .iter()
        .filter_map(|message| match message {
            ClientMessage::Request {
                request_id,
                payload,
                ..
            } => Some((request_id.clone(), payload.clone())),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(requests.len(), 2);
    for (request_id, payload) in requests.into_iter().rev() {
        transport.inject(ServerMessage::Response {
            request_id,
            payload,
        });
    }

    assert_eq!(
        &*first.join().expect("first thread").expect("response"),
        b"first"
    );
    assert_eq!(
        &*second.join().expect("second thread").expect("response"),
        b"second"
    );
    assert!(!client.is_closed());
    let metrics = client
        .telemetry_metric_series()
        .expect("default telemetry is available");
    let telemetry_pressure = client
        .telemetry_pressure()
        .expect("default telemetry is available");
    let pending_lifecycle_observed = metrics.iter().any(|metric| {
        metric.name == ClientMetricName::PendingRequests
            && metric.maximum >= 1.0
            && metric.latest == 0.0
    });
    assert!(
        pending_lifecycle_observed || telemetry_pressure.dropped_metric_updates > 0,
        "the default recorder must retain pending-request lifecycle samples or account for dropping them"
    );
    let both_durations_observed = metrics.iter().any(|metric| {
        metric.name == ClientMetricName::RequestDurationSeconds && metric.samples == 2
    });
    assert!(
        both_durations_observed || telemetry_pressure.dropped_metric_updates > 0,
        "the default recorder must retain both request durations or account for dropping them"
    );
    let routed_responses = client
        .telemetry_diagnostics()
        .expect("default telemetry is available")
        .iter()
        .filter(|name| **name == ClientDiagnosticName::ResponseRouted)
        .count();
    assert!(
        routed_responses == 2 || telemetry_pressure.dropped_diagnostics > 0,
        "the default recorder must retain both routed-response diagnostics or account for dropping them"
    );
    client.close();
}

#[test]
fn one_client_connection_owns_three_hundred_independent_subscriptions() {
    let transport = Arc::new(MemoryTransport::new(true));
    let client = Client::connect(agreement(true), transport.clone(), client_limits(400))
        .expect("client connects");
    let mut subscriptions = Vec::new();
    for index in 0..300 {
        let opened = client
            .open_subscription(
                Arc::from(format!("terminal-{index}").into_bytes()),
                Duration::from_secs(1),
            )
            .expect("subscription opens");
        assert_eq!(&*opened.response, b"opened");
        subscriptions.push(opened.subscription);
    }
    assert_eq!(
        transport
            .sent()
            .iter()
            .filter(|message| matches!(message, ClientMessage::OpenStream { .. }))
            .count(),
        300
    );
    let sent = transport.sent();
    let request_ids = sent
        .iter()
        .filter_map(|message| match message {
            ClientMessage::OpenStream { request_id, .. } => Some(request_id.as_str()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let stream_ids = sent
        .iter()
        .filter_map(|message| match message {
            ClientMessage::OpenStream { stream_id, .. } => Some(stream_id.as_str()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(request_ids.len(), 300, "request IDs are epoch-unique");
    assert_eq!(stream_ids.len(), 300, "stream IDs are independently unique");
    assert!(request_ids.iter().all(|request| request.starts_with("r-")));
    assert!(stream_ids.iter().all(|stream| stream.starts_with("s-")));
    assert_eq!(
        request_ids
            .first()
            .expect("first request")
            .trim_start_matches("r-"),
        stream_ids
            .first()
            .expect("first stream")
            .trim_start_matches("s-"),
        "independent request and stream namespaces may use the same ordinal"
    );
    let last_id = subscriptions.last().expect("last subscription").stream_id();
    let stream_item = ServerMessage::StreamItem {
        stream_id: raw_stream_id(&last_id),
        item: InboundStreamItem::Data {
            sequence: StreamSequence::new(1),
            bytes: Arc::from(&b"output"[..]),
            flow_controlled_bytes: 32,
        },
    };
    let flow_controlled_frame_bytes = u64::try_from(
        WireCodec::new(TEST_FRAME_BYTES)
            .expect("wire codec")
            .encode_server(&stream_item)
            .expect("encode stream item")
            .len(),
    )
    .expect("encoded frame length fits u64");
    transport.inject(stream_item);
    assert!(matches!(
        subscriptions
            .last()
            .expect("last subscription")
            .recv_timeout(Duration::from_secs(1)),
        Ok(ClientStreamItem::Data { sequence, .. }) if sequence == ClientStreamSequence::new(1)
    ));
    transport.wait_for_message(|message| matches!(message, ClientMessage::WindowUpdate { .. }));
    let returned_credit = transport
        .sent()
        .into_iter()
        .filter_map(|message| match message {
            ClientMessage::WindowUpdate {
                additional_bytes, ..
            } => Some(additional_bytes),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        returned_credit.contains(&flow_controlled_frame_bytes),
        "consuming one stream frame must return its exact encoded bytes of credit: expected={flow_controlled_frame_bytes}, returned={returned_credit:?}"
    );
    let first = subscriptions.remove(0);
    let first_id = first.stream_id();
    first.close();
    let post_close_metrics = client
        .telemetry_metric_series()
        .expect("default telemetry is available");
    let post_close_pressure = client
        .telemetry_pressure()
        .expect("default telemetry is available");
    assert!(
        post_close_metrics.iter().any(|metric| {
            metric.name == ClientMetricName::LogicalSubscriptions && metric.latest == 299.0
        }) || post_close_pressure.dropped_metric_updates > 0,
        "closing one independent stream must synchronously release its logical slot"
    );
    assert!(
        !client.is_closed(),
        "an independently closed stream must not close its physical connection"
    );
    transport.wait_for_message(|message| {
        matches!(message, ClientMessage::CloseStream { stream_id } if stream_id.as_str() == first_id.as_str())
    });
    assert!(!client.is_closed());
    let metrics = client
        .telemetry_metric_series()
        .expect("default telemetry is available");
    let logical_pressure_observed = metrics.iter().any(|metric| {
        metric.name == ClientMetricName::LogicalSubscriptions
            && metric.maximum == 300.0
            && metric.latest == 299.0
    });
    let mailbox_high_water_observed = metrics.iter().any(|metric| {
        metric.name == ClientMetricName::StreamMailboxBytes
            && metric.maximum == flow_controlled_frame_bytes as f64
    });
    let telemetry_pressure = client
        .telemetry_pressure()
        .expect("default telemetry is available");
    assert!(
        logical_pressure_observed || telemetry_pressure.dropped_metric_updates > 0,
        "the default non-blocking recorder must retain logical subscription pressure or account for dropping it"
    );
    assert!(
        mailbox_high_water_observed || telemetry_pressure.dropped_metric_updates > 0,
        "the default non-blocking recorder must retain the exact mailbox high-water sample or account for dropping it"
    );
    client.close();
}

#[test]
fn changing_subscription_interest_reuses_one_physical_connection() {
    let transport = Arc::new(MemoryTransport::new(true));
    let connector = Arc::new(SequenceConnector::new([(
        agreement_for("connection-1", "epoch-1"),
        transport.clone(),
    )]));
    let client = ResilientClient::connect(
        connector.clone(),
        client_limits(8),
        ClientRecoveryPolicy::new(
            Duration::from_millis(1),
            Duration::from_millis(5),
            20_000,
            1_000,
            4,
            16,
        )
        .expect("recovery policy"),
        Arc::new(ZeroJitter),
        Duration::from_secs(1),
    )
    .expect("persistent client connects");

    let first = client
        .open_subscription(Arc::from(&b"query:env=prod"[..]), Duration::from_secs(1))
        .expect("first interest opens")
        .subscription;
    let first_id = first.stream_id();
    first.close();
    transport.wait_for_message(|message| {
        matches!(message, ClientMessage::CloseStream { stream_id } if stream_id.as_str() == first_id.as_str())
    });

    let second = client
        .open_subscription(Arc::from(&b"query:env=dev"[..]), Duration::from_secs(1))
        .expect("changed interest opens")
        .subscription;

    assert_ne!(second.stream_id(), first_id);
    assert_eq!(connector.attempts(), 1);
    assert_eq!(client.pressure().physical_connection_attempts, 1);
    assert_eq!(client.pressure().subscriptions, 1);
    assert_eq!(
        transport
            .sent()
            .iter()
            .filter(|message| matches!(message, ClientMessage::OpenStream { .. }))
            .count(),
        2
    );
    assert!(!transport.state.lock().expect("transport lock").closed);
    client.close();
}

#[test]
fn reconnect_resume_reuses_the_prior_stream_identity_on_one_connection() {
    let transport = Arc::new(MemoryTransport::new(true));
    let client = Client::connect(agreement(true), transport.clone(), client_limits(8))
        .expect("client connects");
    let prior_stream = ClientStreamId::new("prior-stream").expect("stream ID");

    let opened = client
        .resume_subscription(
            prior_stream.clone(),
            ClientServerEpoch::new("epoch-1").expect("server epoch"),
            ClientStreamSequence::new(17),
            Arc::from(&b"terminal"[..]),
            Duration::from_secs(1),
        )
        .expect("subscription resumes");

    assert_eq!(opened.subscription.stream_id(), prior_stream);
    assert_eq!(
        opened.subscription.last_sequence(),
        ClientStreamSequence::new(17)
    );
    assert!(transport.sent().iter().any(|message| matches!(
        message,
        ClientMessage::ResumeStream {
            stream_id,
            after,
            ..
        } if stream_id.as_str() == prior_stream.as_str() && *after == StreamSequence::new(17)
    )));
    client.close();
}

#[test]
fn writer_queue_is_bounded_when_physical_send_stalls() {
    let transport = Arc::new(MemoryTransport::new(false));
    transport.set_send_blocked(true);
    let client = Client::connect(agreement(true), transport.clone(), client_limits(1))
        .expect("client connects");

    client.ping().expect("first ping enters writer");
    transport.wait_for_send_entry();
    client.ping().expect("second ping occupies bounded queue");
    let error = client.ping().expect_err("third ping is rejected");

    assert_eq!(error.category(), ClientErrorCategory::Overloaded);
    let overload = error.overload().expect("writer overload context");
    assert_eq!(overload.resource(), "client_writer_queue_messages");
    assert_eq!(overload.limit(), Some(1));
    assert_eq!(overload.current(), Some(1));
    assert_eq!(overload.requested(), Some(1));
    assert_eq!(overload.retry(), ClientRetryGuidance::AfterQueueDrain);
    assert!(
        client
            .telemetry_metric_series()
            .expect("default telemetry is available")
            .iter()
            .any(|metric| {
                metric.name == ClientMetricName::OverloadRejections && metric.total == 1.0
            })
    );
    assert!(
        client
            .telemetry_diagnostics()
            .expect("default telemetry is available")
            .contains(&ClientDiagnosticName::WriterSaturated)
    );
    transport.set_send_blocked(false);
    transport.wait_for_sent(2);
    client.close();
}

#[test]
fn multiplex_capacity_is_projected_without_private_ids_or_payloads() {
    let error = ClientError::from(MultiplexError::CapacityExceeded {
        resource: "logical_streams",
        limit: 300,
    });
    let overload = error.overload().expect("multiplex overload context");

    assert_eq!(error.category(), ClientErrorCategory::Overloaded);
    assert_eq!(overload.resource(), "logical_streams");
    assert_eq!(overload.limit(), Some(300));
    assert_eq!(overload.current(), Some(300));
    assert_eq!(overload.requested(), Some(1));
    assert_eq!(
        overload.retry(),
        ClientRetryGuidance::AfterCapacityAvailable
    );
    assert_eq!(
        overload.retry().as_str(),
        "after_capacity_available",
        "retry guidance is stable and machine-readable"
    );
}

#[test]
fn writer_transport_overload_reaches_the_correlated_request_caller() {
    let transport = Arc::new(MemoryTransport::new(false));
    transport.set_send_overloaded(true);
    let client = Client::connect(agreement(true), transport, client_limits(8)).expect("client");

    let error = client
        .request(Arc::from(&b"known-unsent"[..]), Duration::from_secs(2))
        .expect_err("writer transport overload must fail the request");
    let overload = error.overload().expect("writer transport overload context");

    assert_eq!(error.category(), ClientErrorCategory::Overloaded);
    assert_eq!(overload.resource(), "test_transport_send_queue");
    assert_eq!(overload.limit(), Some(1));
    assert_eq!(overload.current(), Some(1));
    assert_eq!(overload.requested(), Some(1));
    assert_eq!(overload.retry(), ClientRetryGuidance::WithBackoff);
    let transport_error = error
        .source()
        .and_then(|source| source.downcast_ref::<ClientTransportError>())
        .expect("concrete transport failure is retained");
    assert_eq!(transport_error.operation(), ClientTransportOperation::Send);
    assert_eq!(
        transport_error
            .source()
            .expect("private source retained internally")
            .to_string(),
        "private-transport-detail"
    );
    assert!(!error.to_string().contains("private-transport-detail"));
}

#[test]
fn idle_close_wakes_the_writer_without_polling() {
    let transport = Arc::new(MemoryTransport::new(false));
    let client = Client::connect(agreement(true), transport.clone(), client_limits(1))
        .expect("client connects");
    let (completed_sender, completed_receiver) = mpsc::sync_channel(1);
    let closer = thread::spawn(move || {
        drop(client);
        completed_sender.send(()).expect("report idle close");
    });

    completed_receiver
        .recv_timeout(Duration::from_millis(100))
        .expect("idle writer wakes and joins");
    closer.join().expect("idle closer joins");
    assert!(transport.state.lock().expect("transport lock").closed);
}

#[test]
fn full_writer_queue_close_uses_the_runnable_writer_without_extra_capacity() {
    let transport = Arc::new(MemoryTransport::new(false));
    transport.set_send_blocked(true);
    let client = Client::connect(agreement(true), transport.clone(), client_limits(1))
        .expect("client connects");
    client.ping().expect("first ping enters writer");
    transport.wait_for_send_entry();
    client
        .ping()
        .expect("second ping fills the exact queue bound");

    let (completed_sender, completed_receiver) = mpsc::sync_channel(1);
    let closer = thread::spawn(move || {
        client.close();
        drop(client);
        completed_sender.send(()).expect("report full close");
    });

    completed_receiver
        .recv_timeout(Duration::from_millis(100))
        .expect("full queue close wakes and joins the already-runnable writer");
    closer.join().expect("full queue closer joins");
    assert!(transport.state.lock().expect("transport lock").closed);
}

#[test]
fn concurrent_close_and_send_never_admit_after_close_completes() {
    let transport = Arc::new(MemoryTransport::new(false));
    let client =
        Client::connect(agreement(true), transport, client_limits(8)).expect("client connects");
    let barrier = Arc::new(Barrier::new(3));
    let close_completed = Arc::new(AtomicBool::new(false));
    let sender_client = client.clone();
    let sender_barrier = barrier.clone();
    let sender_closed = close_completed.clone();
    let sender = thread::spawn(move || {
        sender_barrier.wait();
        for _ in 0..10_000 {
            let closed_before_send = sender_closed.load(Ordering::Acquire);
            let result = sender_client.ping();
            assert!(
                !closed_before_send || result.is_err(),
                "a send begun after close completed must not be admitted"
            );
            if closed_before_send {
                break;
            }
        }
    });
    let closer_client = client.clone();
    let closer_barrier = barrier.clone();
    let closer_closed = close_completed.clone();
    let closer = thread::spawn(move || {
        closer_barrier.wait();
        closer_client.close();
        closer_closed.store(true, Ordering::Release);
    });

    barrier.wait();
    closer.join().expect("concurrent closer joins");
    sender.join().expect("concurrent sender joins");
    assert!(client.is_closed());
    assert!(client.ping().is_err());
    drop(client);
}

#[test]
fn repeated_one_shot_teardown_has_no_ten_millisecond_polling_floor() {
    let mut elapsed = Vec::with_capacity(31);
    for _ in 0..31 {
        let transport = Arc::new(MemoryTransport::new(false));
        let client =
            Client::connect(agreement(true), transport, client_limits(1)).expect("client connects");
        let started = Instant::now();
        drop(client);
        elapsed.push(started.elapsed());
    }
    elapsed.sort_unstable();
    let median = elapsed[elapsed.len() / 2];

    assert!(
        median < Duration::from_millis(5),
        "one-shot client teardown median {median:?} retained a polling floor"
    );
}

#[test]
fn negotiated_heartbeat_sends_ping_and_accepts_any_inbound_liveness() {
    let transport = Arc::new(MemoryTransport::new(false));
    let client = Client::connect(
        agreement_with_heartbeat(10, 2_000),
        transport.clone(),
        client_limits(8),
    )
    .expect("client connects");

    transport.wait_until(|state| {
        state
            .sent
            .iter()
            .filter(|message| matches!(message, ClientMessage::Ping))
            .count()
            >= 1
    });
    transport.inject(ServerMessage::Pong);
    transport.wait_until(|state| {
        state
            .sent
            .iter()
            .filter(|message| matches!(message, ClientMessage::Ping))
            .count()
            >= 2
    });
    // Keep supplying inbound liveness beyond the original timeout. The timeout
    // leaves room for hosted runner scheduling, while this duration proves that
    // inbound messages refresh it rather than merely arriving before it expires.
    let live_until = Instant::now() + Duration::from_millis(2_200);
    while Instant::now() < live_until {
        transport.inject(ServerMessage::Pong);
        thread::sleep(Duration::from_millis(20));
        assert!(!client.is_closed());
    }

    assert!(!client.is_closed());
    client.close();
}

#[test]
fn negotiated_heartbeat_closes_a_silent_physical_transport() {
    let transport = Arc::new(MemoryTransport::new(false));
    let client = Client::connect(
        agreement_with_heartbeat(10, 40),
        transport.clone(),
        client_limits(8),
    )
    .expect("client connects");

    // A delayed heartbeat worker can observe the timeout before its first ping.
    // Ping delivery is covered separately; this contract requires silent-peer closure.
    transport.wait_until(|state| state.closed);

    assert!(client.is_closed());
}

#[test]
fn slow_consumer_isolated_without_advancing_past_unconsumed_data() {
    let transport = Arc::new(MemoryTransport::new(true));
    let client = Client::connect(agreement(true), transport.clone(), client_limits(8))
        .expect("client connects");
    let opened = client
        .open_subscription(Arc::from(&b"terminal"[..]), Duration::from_secs(1))
        .expect("subscription opens");
    let stream_id = opened.subscription.stream_id();

    for sequence in 1..=9 {
        transport.inject(ServerMessage::StreamItem {
            stream_id: raw_stream_id(&stream_id),
            item: InboundStreamItem::Data {
                sequence: StreamSequence::new(sequence),
                bytes: Arc::from(&b"x"[..]),
                flow_controlled_bytes: 1,
            },
        });
    }
    transport.wait_for_message(|message| matches!(message, ClientMessage::CloseStream { .. }));

    assert!(!client.is_closed());
    assert_eq!(
        opened.subscription.last_sequence(),
        ClientStreamSequence::INITIAL
    );
    let stream_error = match opened.subscription.recv() {
        Err(error) => error,
        Ok(_) => panic!("mailbox overload must terminate the active stream"),
    };
    let overload = stream_error
        .overload()
        .expect("active stream caller receives typed overload context");
    assert_eq!(overload.resource(), "client_stream_mailbox");
    assert_eq!(
        overload.retry(),
        undercurrent_sdk::ClientRetryGuidance::AfterQueueDrain
    );
    let requester = client.clone();
    let request = thread::spawn(move || {
        requester.request(Arc::from(&b"still-responsive"[..]), Duration::from_secs(1))
    });
    transport.wait_for_message(|m| matches!(m, ClientMessage::Request { .. }));
    for message in transport.sent() {
        if let ClientMessage::Request {
            request_id,
            payload,
            ..
        } = message
        {
            transport.inject(ServerMessage::Response {
                request_id,
                payload,
            });
        }
    }
    assert_eq!(
        request
            .join()
            .unwrap()
            .expect("peer request survives overload"),
        Arc::<[u8]>::from(&b"still-responsive"[..])
    );
    let peer = client
        .open_subscription(Arc::from(&b"other"[..]), Duration::from_secs(1))
        .expect("peer stream opens");
    drop(peer);
    assert!(
        client
            .telemetry_diagnostics()
            .expect("default telemetry is available")
            .contains(&ClientDiagnosticName::SlowConsumerDetected)
    );
    assert!(
        client
            .telemetry_metric_series()
            .expect("default telemetry is available")
            .iter()
            .any(|metric| {
                metric.name == ClientMetricName::OverloadRejections && metric.total == 1.0
            })
    );
}

#[test]
fn connection_teardown_fails_pending_request_without_waiting_for_timeout() {
    let transport = Arc::new(MemoryTransport::new(false));
    let client = Client::connect(agreement(true), transport.clone(), client_limits(8))
        .expect("client connects");
    let request_client = client.clone();
    let pending = thread::spawn(move || {
        request_client.request(Arc::from(&b"wait"[..]), Duration::from_secs(30))
    });
    transport.wait_for_sent(1);

    client.close();
    let error = pending
        .join()
        .expect("request thread")
        .expect_err("request fails on close");

    assert_eq!(error.category(), ClientErrorCategory::Unavailable);
    assert!(client.is_closed());
}

#[test]
fn transport_errors_redact_private_source_text() {
    let error = ClientTransportError::new(ClientTransportOperation::Send, PrivateTransportError);

    assert!(!error.to_string().contains("private-transport-detail"));
    assert!(!format!("{error:?}").contains("private-transport-detail"));
    assert_eq!(
        error.source().expect("source retained").to_string(),
        "private-transport-detail"
    );
}

#[test]
fn client_registries_are_unique_and_stable() {
    assert_eq!(
        ClientDiagnosticName::ALL
            .map(ClientDiagnosticName::as_str)
            .into_iter()
            .collect::<BTreeSet<_>>()
            .len(),
        ClientDiagnosticName::ALL.len()
    );
    assert_eq!(
        ClientMetricName::ALL
            .map(ClientMetricName::as_str)
            .into_iter()
            .collect::<BTreeSet<_>>()
            .len(),
        ClientMetricName::ALL.len()
    );
    assert_eq!(
        ClientErrorCategory::ALL
            .map(ClientErrorCategory::as_str)
            .into_iter()
            .collect::<BTreeSet<_>>()
            .len(),
        ClientErrorCategory::ALL.len()
    );
    assert_eq!(
        PersistentRecoveryFailure::ALL
            .map(PersistentRecoveryFailure::as_str)
            .into_iter()
            .collect::<BTreeSet<_>>()
            .len(),
        PersistentRecoveryFailure::ALL.len()
    );
}

#[test]
fn recovery_uses_bounded_backoff_and_resumes_streams_on_one_replacement_connection() {
    let transport = Arc::new(MemoryTransport::new(true));
    let connector = FlakyConnector::new(2, transport.clone());
    let policy = ClientRecoveryPolicy::new(
        Duration::from_millis(1),
        Duration::from_millis(5),
        20_000,
        1_000,
        4,
        2,
    )
    .expect("recovery policy");
    assert_eq!(
        policy.delay_after_failure(1, 1_000),
        Duration::from_micros(1_100)
    );
    assert_eq!(
        policy.delay_after_failure(10, 1_000),
        Duration::from_millis(5)
    );
    let checkpoints = (0..3)
        .map(|index| {
            SubscriptionCheckpoint::new(
                ClientStreamId::new(format!("prior-{index}")).expect("stream ID"),
                ClientServerEpoch::new("epoch-1").expect("server epoch"),
                ClientStreamSequence::new(index),
                client_trace_context(index),
                Arc::from(format!("terminal-{index}").into_bytes()),
            )
        })
        .collect();

    let recovered = recover_client_connection(
        &connector,
        client_limits(16),
        policy,
        &ZeroJitter,
        &ClientRecoveryCancellation::default(),
        checkpoints,
        Duration::from_secs(1),
    )
    .expect("client recovers");

    assert_eq!(connector.attempts(), 3);
    assert_eq!(recovered.connection_attempts, 3);
    assert_eq!(recovered.subscriptions.len(), 3);
    assert!(
        recovered
            .subscriptions
            .iter()
            .all(|subscription| subscription.result.is_ok())
    );
    assert_eq!(
        transport
            .sent()
            .iter()
            .filter(|message| matches!(message, ClientMessage::ResumeStream { .. }))
            .count(),
        3
    );
    recovered.client.close();
}

#[test]
fn recovery_stops_on_do_not_retry_unchanged_connector_failure() {
    struct Connector {
        calls: AtomicUsize,
        guidance: ClientRetryGuidance,
        initial: Option<Arc<MemoryTransport>>,
    }
    impl ClientConnector for Connector {
        fn connect(
            &self,
            _: &ClientRecoveryCancellation,
        ) -> Result<NegotiatedClientTransport, ClientTransportError> {
            let attempt = self.calls.fetch_add(1, Ordering::AcqRel);
            if attempt == 0
                && let Some(initial) = &self.initial
            {
                return Ok(NegotiatedClientTransport::new(
                    agreement(true),
                    initial.clone(),
                ));
            }
            Err(ClientTransportError::overloaded(
                ClientTransportOperation::Connect,
                PrivateTransportError,
                "connector_admission",
                Some(1),
                Some(1),
                Some(1),
                self.guidance,
            ))
        }
    }
    for (guidance, expected_attempts) in [
        (ClientRetryGuidance::DoNotRetryUnchanged, 1),
        (ClientRetryGuidance::WithBackoff, 4),
    ] {
        let connector = Connector {
            calls: AtomicUsize::new(0),
            guidance,
            initial: None,
        };
        let error = match recover_client_connection(
            &connector,
            client_limits(8),
            recovery_policy(),
            &ZeroJitter,
            &ClientRecoveryCancellation::default(),
            Vec::new(),
            Duration::from_secs(1),
        ) {
            Ok(_) => panic!("connector always rejects"),
            Err(error) => error,
        };
        let (attempts, source) = match &error {
            ClientRecoveryError::ConnectorRejected { attempts, source }
                if expected_attempts == 1 =>
            {
                (attempts, source)
            }
            ClientRecoveryError::AttemptsExhausted { attempts, source }
                if expected_attempts == 4 =>
            {
                (attempts, source)
            }
            other => panic!("wrong recovery failure: {other:?}"),
        };
        assert_eq!(*attempts, expected_attempts);
        assert_eq!(source.overload().unwrap().retry(), guidance);
        assert!(std::error::Error::source(&error).is_some());
        assert!(!format!("{error:?}").contains("private-transport-detail"));
        assert_eq!(
            connector.calls.load(Ordering::Acquire),
            expected_attempts as usize
        );
    }
    let initial = Arc::new(MemoryTransport::new(true));
    let connector = Arc::new(Connector {
        calls: AtomicUsize::new(0),
        guidance: ClientRetryGuidance::DoNotRetryUnchanged,
        initial: Some(initial.clone()),
    });
    let client = ResilientClient::connect(
        connector.clone(),
        client_limits(8),
        recovery_policy(),
        Arc::new(ZeroJitter),
        Duration::from_secs(1),
    )
    .unwrap();
    let subscription = client
        .open_subscription(Arc::from(&b"terminal"[..]), Duration::from_secs(1))
        .unwrap()
        .subscription;
    ClientTransport::close(initial.as_ref());
    assert_eq!(
        subscription
            .recv_timeout(Duration::from_secs(1))
            .unwrap_err(),
        ResilientSubscriptionError::RecoveryFailed(PersistentRecoveryFailure::ConnectorRejected)
    );
    assert_eq!(
        client.pressure().phase,
        ResilientClientPhase::Failed(PersistentRecoveryFailure::ConnectorRejected)
    );
    assert_eq!(connector.calls.load(Ordering::Acquire), 2);
    assert!(matches!(
        client.last_recovery_error().unwrap().as_ref(),
        ClientRecoveryError::ConnectorRejected { attempts: 1, .. }
    ));
    client.close();
}

#[test]
fn cancellation_during_resume_closes_replacement_and_stops_queued_checkpoints() {
    let transport = Arc::new(MemoryTransport::new(false));
    let connector = Arc::new(FlakyConnector::new(0, transport.clone()));
    let cancellation = ClientRecoveryCancellation::default();
    let token = cancellation.clone();
    let worker = thread::spawn(move || {
        let checkpoints = (0..6)
            .map(|index| {
                SubscriptionCheckpoint::new(
                    ClientStreamId::new(format!("cancel-resume-{index}")).unwrap(),
                    ClientServerEpoch::new("epoch-1").unwrap(),
                    ClientStreamSequence::new(0),
                    client_trace_context(index),
                    Arc::from(&b"terminal"[..]),
                )
            })
            .collect();
        recover_client_connection(
            connector.as_ref(),
            client_limits(16),
            recovery_policy(),
            &ZeroJitter,
            &token,
            checkpoints,
            Duration::from_millis(500),
        )
    });
    transport.wait_until(|state| {
        state
            .sent
            .iter()
            .filter(|message| matches!(message, ClientMessage::ResumeStream { .. }))
            .count()
            == 2
    });
    let started = Instant::now();
    cancellation.cancel();
    let result = worker.join().unwrap();
    assert!(
        matches!(result, Err(ClientRecoveryError::Cancelled { attempts: 1 })),
        "resume cancellation must not return an apparently usable replacement"
    );
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(
        transport
            .sent()
            .iter()
            .filter(|message| matches!(message, ClientMessage::ResumeStream { .. }))
            .count(),
        2
    );
    assert!(transport.state.lock().unwrap().closed);
}

fn recovery_policy() -> ClientRecoveryPolicy {
    ClientRecoveryPolicy::new(
        Duration::from_millis(1),
        Duration::from_millis(5),
        20_000,
        1_000,
        4,
        2,
    )
    .unwrap()
}

#[test]
fn persistent_close_and_last_owner_drop_interrupt_replacement_resumes() {
    for explicit_close in [true, false] {
        let first = Arc::new(MemoryTransport::new(true));
        let replacement = Arc::new(MemoryTransport::new(false));
        let connector = Arc::new(SequenceConnector::new([
            (agreement_for("cancel-original", "epoch-1"), first.clone()),
            (
                agreement_for("cancel-replacement", "epoch-1"),
                replacement.clone(),
            ),
        ]));
        let client = ResilientClient::connect(
            connector,
            client_limits(16),
            recovery_policy(),
            Arc::new(ZeroJitter),
            Duration::from_millis(500),
        )
        .unwrap();
        let subscriptions = (0..6)
            .map(|_| {
                client
                    .open_subscription(Arc::from(&b"terminal"[..]), Duration::from_secs(1))
                    .unwrap()
                    .subscription
            })
            .collect::<Vec<_>>();
        ClientTransport::close(first.as_ref());
        replacement.wait_until(|state| {
            state
                .sent
                .iter()
                .filter(|message| matches!(message, ClientMessage::ResumeStream { .. }))
                .count()
                == 2
        });
        let started = Instant::now();
        if explicit_close {
            client.close();
            assert_eq!(client.pressure().phase, ResilientClientPhase::Closed);
        }
        drop(subscriptions);
        drop(client);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "teardown must interrupt activation waits"
        );
        assert!(replacement.state.lock().unwrap().closed);
        assert_eq!(
            replacement
                .sent()
                .iter()
                .filter(|message| matches!(message, ClientMessage::ResumeStream { .. }))
                .count(),
            2
        );
    }
}

#[test]
fn cancellation_interrupts_a_long_reconnect_backoff() {
    let transport = Arc::new(MemoryTransport::new(false));
    let connector = Arc::new(FlakyConnector::new(usize::MAX, transport));
    let cancellation = ClientRecoveryCancellation::default();
    let worker_connector = connector.clone();
    let worker_cancellation = cancellation.clone();
    let started_at = Instant::now();
    let worker = thread::spawn(move || {
        recover_client_connection(
            worker_connector.as_ref(),
            client_limits(8),
            ClientRecoveryPolicy::new(
                Duration::from_secs(30),
                Duration::from_secs(30),
                10_000,
                1,
                3,
                1,
            )
            .expect("recovery policy"),
            &ZeroJitter,
            &worker_cancellation,
            Vec::new(),
            Duration::from_secs(1),
        )
    });
    connector.wait_for_attempts(1);
    cancellation.cancel();

    assert!(matches!(
        worker.join().expect("recovery worker"),
        Err(ClientRecoveryError::Cancelled { attempts: 1 })
    ));
    assert!(started_at.elapsed() < Duration::from_secs(1));
    assert_eq!(connector.attempts(), 1);
}

#[test]
fn initial_probe_budget_does_not_limit_later_recovery() {
    let policy = |attempts| {
        ClientRecoveryPolicy::new(
            Duration::from_millis(1),
            Duration::from_millis(2),
            10_000,
            1,
            attempts,
            2,
        )
        .unwrap()
    };
    let unavailable = Arc::new(FlakyConnector::new(2, Arc::new(MemoryTransport::new(true))));
    assert!(
        ResilientClient::connect_with_initial_policy(
            unavailable.clone(),
            client_limits(16),
            policy(1),
            policy(4),
            Arc::new(ZeroJitter),
            Duration::from_secs(1),
        )
        .is_err()
    );
    assert_eq!(unavailable.attempts(), 1);

    let transport = Arc::new(MemoryTransport::new(true));
    let connector = Arc::new(SequenceConnector::new([(
        agreement_for("probe", "epoch"),
        transport.clone(),
    )]));
    let client = ResilientClient::connect_with_initial_policy(
        connector.clone(),
        client_limits(16),
        policy(1),
        policy(4),
        Arc::new(ZeroJitter),
        Duration::from_secs(1),
    )
    .unwrap();
    let opened = client
        .open_subscription(Arc::from(&b"terminal"[..]), Duration::from_secs(1))
        .unwrap();
    ClientTransport::close(transport.as_ref());
    assert!(
        opened
            .subscription
            .recv_timeout(Duration::from_secs(2))
            .is_err()
    );
    assert_eq!(
        connector.attempts(),
        5,
        "one initial attempt plus four recovery attempts"
    );
    client.close();
}

#[test]
fn persistent_subscription_recovers_across_repeated_physical_connections() {
    let first = Arc::new(MemoryTransport::new(true));
    let second = Arc::new(MemoryTransport::new(true));
    let third = Arc::new(MemoryTransport::new(true));
    let connector = Arc::new(SequenceConnector::new([
        (agreement_for("connection-1", "epoch-1"), first.clone()),
        (agreement_for("connection-2", "epoch-1"), second.clone()),
        (agreement_for("connection-3", "epoch-1"), third.clone()),
    ]));
    let client = ResilientClient::connect(
        connector.clone(),
        client_limits(16),
        ClientRecoveryPolicy::new(
            Duration::from_millis(1),
            Duration::from_millis(5),
            20_000,
            1_000,
            4,
            2,
        )
        .expect("recovery policy"),
        Arc::new(ZeroJitter),
        Duration::from_secs(1),
    )
    .expect("persistent client connects");
    let logical_trace = client_trace_context(77);
    let opened = client
        .open_subscription_with_trace_context(
            Arc::from(&b"terminal"[..]),
            logical_trace.clone(),
            Duration::from_secs(1),
        )
        .expect("persistent subscription opens");
    let subscription = Arc::new(opened.subscription);
    let stream_id = subscription.stream_id();
    first.inject(ServerMessage::StreamItem {
        stream_id: raw_stream_id(&stream_id),
        item: InboundStreamItem::Data {
            sequence: StreamSequence::new(1),
            bytes: Arc::from(&b"first"[..]),
            flow_controlled_bytes: 16,
        },
    });
    assert!(matches!(
        subscription.recv_timeout(Duration::from_secs(1)),
        Ok(ClientStreamItem::Data { sequence, .. }) if sequence == ClientStreamSequence::new(1)
    ));

    let waiting_subscription = subscription.clone();
    let waiting_reader =
        thread::spawn(move || waiting_subscription.recv_timeout(Duration::from_secs(2)));
    ClientTransport::close(first.as_ref());
    second.wait_for_message(|message| {
        matches!(
            message,
            ClientMessage::ResumeStream {
                stream_id: resumed,
                after,
                trace_context: resumed_trace,
                ..
            } if resumed.as_str() == stream_id.as_str()
                && *after == StreamSequence::new(1)
                && resumed_trace.trace_id.as_str() == logical_trace.trace_id()
                && resumed_trace.span_id.as_str() == logical_trace.span_id()
        )
    });
    assert!(client.wait_connected(Duration::from_secs(1)));
    assert_eq!(client.pressure().phase, ResilientClientPhase::Connected);
    assert_eq!(client.pressure().generation, 2);
    assert_eq!(client.pressure().physical_connection_attempts, 2);
    assert_eq!(connector.attempts(), 2);
    let recovery_diagnostics = client
        .telemetry_diagnostics()
        .expect("default telemetry is available");
    assert!(recovery_diagnostics.contains(&ClientDiagnosticName::RecoveryStarted));
    assert!(recovery_diagnostics.contains(&ClientDiagnosticName::RecoverySucceeded));

    ClientTransport::close(second.as_ref());
    third.wait_for_message(|message| {
        matches!(
            message,
            ClientMessage::ResumeStream {
                stream_id: resumed,
                after,
                trace_context: resumed_trace,
                ..
            } if resumed.as_str() == stream_id.as_str()
                && *after == StreamSequence::new(1)
                && resumed_trace.trace_id.as_str() == logical_trace.trace_id()
                && resumed_trace.span_id.as_str() == logical_trace.span_id()
        )
    });
    assert!(client.wait_connected(Duration::from_secs(1)));
    assert_eq!(client.pressure().generation, 3);
    assert_eq!(client.pressure().physical_connection_attempts, 3);
    assert_eq!(connector.attempts(), 3);

    third.inject(ServerMessage::StreamItem {
        stream_id: raw_stream_id(&stream_id),
        item: InboundStreamItem::Data {
            sequence: StreamSequence::new(2),
            bytes: Arc::from(&b"second"[..]),
            flow_controlled_bytes: 16,
        },
    });
    assert!(matches!(
        waiting_reader.join().expect("persistent reader"),
        Ok(ClientStreamItem::Data { sequence, .. }) if sequence == ClientStreamSequence::new(2)
    ));
    assert_eq!(subscription.stream_id(), stream_id);
    assert_eq!(subscription.last_sequence(), ClientStreamSequence::new(2));

    let peer = client
        .open_subscription(Arc::from(&b"events"[..]), Duration::from_secs(1))
        .expect("new stream opens after recovery");
    assert_ne!(peer.subscription.stream_id(), stream_id);
    assert_eq!(
        third
            .sent()
            .iter()
            .filter(|message| matches!(message, ClientMessage::OpenStream { .. }))
            .count(),
        1
    );
    client.close();
}

#[test]
fn persistent_client_resumes_three_hundred_streams_without_fallback_connections() {
    let first = Arc::new(MemoryTransport::new(true));
    let second = Arc::new(MemoryTransport::new(true));
    let connector = Arc::new(SequenceConnector::new([
        (agreement_for("connection-1", "epoch-1"), first.clone()),
        (agreement_for("connection-2", "epoch-1"), second.clone()),
    ]));
    let client = ResilientClient::connect(
        connector.clone(),
        client_limits(400),
        ClientRecoveryPolicy::new(
            Duration::from_millis(1),
            Duration::from_millis(5),
            20_000,
            1_000,
            4,
            16,
        )
        .expect("recovery policy"),
        Arc::new(ZeroJitter),
        Duration::from_secs(1),
    )
    .expect("persistent client connects");
    let mut subscriptions = Vec::with_capacity(300);
    for index in 0..300 {
        subscriptions.push(
            client
                .open_subscription(
                    Arc::from(format!("terminal-{index}").into_bytes()),
                    Duration::from_secs(1),
                )
                .expect("persistent subscription opens")
                .subscription,
        );
    }

    ClientTransport::close(first.as_ref());
    second.wait_until(|state| {
        state
            .sent
            .iter()
            .filter(|message| matches!(message, ClientMessage::ResumeStream { .. }))
            .count()
            == 300
    });
    assert!(client.wait_connected(Duration::from_secs(1)));
    assert_eq!(connector.attempts(), 2);
    assert_eq!(client.pressure().subscriptions, 300);
    assert_eq!(
        subscriptions
            .iter()
            .map(|subscription| subscription.stream_id())
            .collect::<BTreeSet<_>>()
            .len(),
        300
    );
    let metrics = client
        .telemetry_metric_series()
        .expect("default telemetry is available");
    assert!(metrics.iter().any(|metric| {
        metric.name == ClientMetricName::ReconnectAttempts && metric.total == 1.0
    }));
    assert!(metrics.iter().any(|metric| {
        metric.name == ClientMetricName::ResumeAttempts && metric.total == 300.0
    }));
    assert!(metrics.iter().any(|metric| {
        metric.name == ClientMetricName::ResumeSucceeded && metric.total == 300.0
    }));
    assert!(
        metrics
            .iter()
            .any(|metric| { metric.name == ClientMetricName::ResumeFailed && metric.total == 0.0 })
    );
    let diagnostics = client
        .telemetry_diagnostics()
        .expect("default telemetry is available");
    assert!(diagnostics.contains(&ClientDiagnosticName::RecoverySucceeded));
    client.close();
}

#[test]
fn persistent_recovery_exhaustion_wakes_existing_subscription_handles() {
    let first = Arc::new(MemoryTransport::new(true));
    let connector = Arc::new(SequenceConnector::new([(
        agreement_for("connection-1", "epoch-1"),
        first.clone(),
    )]));
    let client = ResilientClient::connect(
        connector,
        client_limits(8),
        ClientRecoveryPolicy::new(
            Duration::from_millis(1),
            Duration::from_millis(2),
            10_000,
            1,
            2,
            1,
        )
        .expect("recovery policy"),
        Arc::new(ZeroJitter),
        Duration::from_secs(1),
    )
    .expect("persistent client connects");
    let subscription = client
        .open_subscription(Arc::from(&b"terminal"[..]), Duration::from_secs(1))
        .expect("persistent subscription opens")
        .subscription;

    ClientTransport::close(first.as_ref());
    assert_eq!(
        subscription
            .recv_timeout(Duration::from_secs(1))
            .expect_err("exhausted recovery terminates the stream"),
        ResilientSubscriptionError::RecoveryFailed(PersistentRecoveryFailure::AttemptsExhausted)
    );
    assert_eq!(
        client.pressure().phase,
        ResilientClientPhase::Failed(PersistentRecoveryFailure::AttemptsExhausted)
    );
    let recovery_error = client
        .last_recovery_error()
        .expect("terminal recovery source is retained");
    assert!(matches!(
        recovery_error.as_ref(),
        ClientRecoveryError::AttemptsExhausted { attempts: 2, .. }
    ));
    assert!(!format!("{recovery_error:?}").contains("private-transport-detail"));
    assert!(
        client
            .telemetry_metric_series()
            .expect("default telemetry is available")
            .iter()
            .any(|metric| {
                metric.name == ClientMetricName::ReconnectAttempts && metric.total == 2.0
            })
    );
    assert!(
        client
            .telemetry_diagnostics()
            .expect("default telemetry is available")
            .contains(&ClientDiagnosticName::RecoveryFailed)
    );
    client.close();
}

fn agreement(required_capabilities: bool) -> ClientConnectionAgreement {
    agreement_with_capabilities(required_capabilities, "connection-1", "epoch-1")
}

fn agreement_for(connection_id: &str, server_epoch: &str) -> ClientConnectionAgreement {
    agreement_with_capabilities(true, connection_id, server_epoch)
}

fn agreement_with_capabilities(
    required_capabilities: bool,
    connection_id: &str,
    server_epoch: &str,
) -> ClientConnectionAgreement {
    agreement_with_capabilities_and_heartbeat(
        required_capabilities,
        connection_id,
        server_epoch,
        HeartbeatPolicy::new(5_000, 15_000).expect("heartbeat"),
    )
}

fn agreement_with_heartbeat(
    interval_millis: u64,
    timeout_millis: u64,
) -> ClientConnectionAgreement {
    agreement_with_capabilities_and_heartbeat(
        true,
        "connection-1",
        "epoch-1",
        HeartbeatPolicy::new(interval_millis, timeout_millis).expect("heartbeat"),
    )
}

fn agreement_with_capabilities_and_heartbeat(
    required_capabilities: bool,
    connection_id: &str,
    server_epoch: &str,
    heartbeat: HeartbeatPolicy,
) -> ClientConnectionAgreement {
    let limits =
        ConnectionLimits::new(1_024, 400, 64, 1_000_000, 8_192, 4_096).expect("connection limits");
    let capabilities = if required_capabilities {
        BTreeSet::from([
            capability(connection_capabilities::MULTIPLEX),
            capability(connection_capabilities::FLOW_CONTROL),
            capability(connection_capabilities::STREAM_RESUME),
            capability(connection_capabilities::TRACE_CONTEXT),
        ])
    } else {
        BTreeSet::new()
    };
    let hello = ConnectionHello::new(
        ClientId::new("client-1").expect("client ID"),
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
        heartbeat,
        required_capabilities.then_some(60_000),
    )
    .expect("policy");
    sdk_agreement(
        negotiate_connection(
            &hello,
            &policy,
            ConnectionId::new(connection_id).expect("connection ID"),
            ServerEpoch::new(server_epoch).expect("server epoch"),
        )
        .expect("agreement"),
    )
}

fn client_limits(writer_queue_messages: usize) -> ClientRuntimeLimits {
    ClientRuntimeLimits::new(900, writer_queue_messages, 8, 4_096).expect("client limits")
}

fn capability(value: &str) -> NamespacedName {
    NamespacedName::new(value).expect("capability")
}

fn client_trace_context(index: u64) -> ClientTraceContext {
    ClientTraceContext::new(format!("trace-{index}"), format!("span-{index}"))
        .expect("client trace context")
}

#[derive(Debug)]
struct PrivateTransportError;

impl Display for PrivateTransportError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("private-transport-detail")
    }
}

impl Error for PrivateTransportError {}

struct ZeroJitter;

impl ClientRecoveryJitter for ZeroJitter {
    fn sample_basis_points(&self, _failed_attempt: u32, _maximum: u16) -> u16 {
        0
    }
}

struct FlakyConnector {
    fail_before: usize,
    attempts: AtomicUsize,
    transport: Arc<MemoryTransport>,
    changed: Condvar,
    wait_lock: Mutex<()>,
}

impl FlakyConnector {
    fn new(fail_before: usize, transport: Arc<MemoryTransport>) -> Self {
        Self {
            fail_before,
            attempts: AtomicUsize::new(0),
            transport,
            changed: Condvar::new(),
            wait_lock: Mutex::new(()),
        }
    }

    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::Acquire)
    }

    fn wait_for_attempts(&self, expected: usize) {
        let started_at = Instant::now();
        let mut guard = self.wait_lock.lock().expect("connector wait lock");
        while self.attempts() < expected {
            let remaining = Duration::from_secs(1).saturating_sub(started_at.elapsed());
            assert!(!remaining.is_zero(), "connector attempt wait timed out");
            let (next, timeout) = self
                .changed
                .wait_timeout(guard, remaining)
                .expect("connector attempt wait");
            guard = next;
            assert!(
                !timeout.timed_out() || self.attempts() >= expected,
                "connector attempt wait timed out"
            );
        }
    }
}

impl ClientConnector for FlakyConnector {
    fn connect(
        &self,
        _cancellation: &ClientRecoveryCancellation,
    ) -> Result<NegotiatedClientTransport, ClientTransportError> {
        let attempt = self.attempts.fetch_add(1, Ordering::AcqRel) + 1;
        self.changed.notify_all();
        if attempt <= self.fail_before {
            return Err(ClientTransportError::new(
                ClientTransportOperation::Connect,
                PrivateTransportError,
            ));
        }
        Ok(NegotiatedClientTransport::new(
            agreement(true),
            self.transport.clone(),
        ))
    }
}

struct SequenceConnector {
    connections: Mutex<VecDeque<(ClientConnectionAgreement, Arc<MemoryTransport>)>>,
    attempts: AtomicUsize,
}

impl SequenceConnector {
    fn new(
        connections: impl IntoIterator<Item = (ClientConnectionAgreement, Arc<MemoryTransport>)>,
    ) -> Self {
        Self {
            connections: Mutex::new(connections.into_iter().collect()),
            attempts: AtomicUsize::new(0),
        }
    }

    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::Acquire)
    }
}

impl ClientConnector for SequenceConnector {
    fn connect(
        &self,
        _cancellation: &ClientRecoveryCancellation,
    ) -> Result<NegotiatedClientTransport, ClientTransportError> {
        self.attempts.fetch_add(1, Ordering::AcqRel);
        let (agreement, transport) = self
            .connections
            .lock()
            .expect("sequence connector lock")
            .pop_front()
            .ok_or_else(|| {
                ClientTransportError::new(ClientTransportOperation::Connect, PrivateTransportError)
            })?;
        Ok(NegotiatedClientTransport::new(agreement, transport))
    }
}

struct MemoryTransport {
    state: Mutex<TransportState>,
    changed: Condvar,
    auto_ack_streams: bool,
    send_entries: AtomicUsize,
    send_overloaded: AtomicBool,
}

#[derive(Default)]
struct TransportState {
    sent: Vec<ClientMessage>,
    inbound: VecDeque<ServerMessage>,
    closed: bool,
    send_blocked: bool,
}

impl MemoryTransport {
    fn new(auto_ack_streams: bool) -> Self {
        Self {
            state: Mutex::new(TransportState::default()),
            changed: Condvar::new(),
            auto_ack_streams,
            send_entries: AtomicUsize::new(0),
            send_overloaded: AtomicBool::new(false),
        }
    }

    fn set_send_overloaded(&self, overloaded: bool) {
        self.send_overloaded.store(overloaded, Ordering::Release);
    }

    fn inject(&self, message: ServerMessage) {
        let mut state = self.state.lock().expect("transport lock");
        state.inbound.push_back(message);
        self.changed.notify_all();
    }

    fn sent(&self) -> Vec<ClientMessage> {
        self.state.lock().expect("transport lock").sent.clone()
    }

    fn wait_for_sent(&self, count: usize) -> Vec<ClientMessage> {
        self.wait_until(|state| state.sent.len() >= count);
        self.sent()
    }

    fn wait_for_message(&self, predicate: impl Fn(&ClientMessage) -> bool) {
        self.wait_until(|state| state.sent.iter().any(&predicate));
    }

    fn set_send_blocked(&self, blocked: bool) {
        let mut state = self.state.lock().expect("transport lock");
        state.send_blocked = blocked;
        self.changed.notify_all();
    }

    fn wait_for_send_entry(&self) {
        self.wait_until(|_| self.send_entries.load(Ordering::Acquire) > 0);
    }

    fn wait_until(&self, predicate: impl Fn(&TransportState) -> bool) {
        let started_at = Instant::now();
        loop {
            let state = self.state.lock().expect("transport lock");
            if predicate(&state) {
                return;
            }
            let snapshot = (
                self.send_entries.load(Ordering::Acquire),
                state.sent.len(),
                state
                    .sent
                    .iter()
                    .filter(|message| matches!(message, ClientMessage::Ping))
                    .count(),
                state
                    .sent
                    .iter()
                    .filter(|message| matches!(message, ClientMessage::Close))
                    .count(),
                state
                    .sent
                    .iter()
                    .filter(|message| matches!(message, ClientMessage::WindowUpdate { .. }))
                    .count(),
                state
                    .sent
                    .iter()
                    .filter(|message| matches!(message, ClientMessage::CloseStream { .. }))
                    .count(),
            );
            drop(state);
            assert!(
                started_at.elapsed() < Duration::from_secs(2),
                "transport wait timed out: entries={}, sent={}, pings={}, closes={}, window_updates={}, close_streams={}",
                snapshot.0,
                snapshot.1,
                snapshot.2,
                snapshot.3,
                snapshot.4,
                snapshot.5
            );
            thread::sleep(Duration::from_millis(1));
        }
    }
}

impl ClientTransport for MemoryTransport {
    fn send(&self, message: ClientTransportOutbound) -> Result<(), ClientTransportError> {
        self.send_entries.fetch_add(1, Ordering::AcqRel);
        if self.send_overloaded.load(Ordering::Acquire) {
            return Err(ClientTransportError::overloaded(
                ClientTransportOperation::Send,
                PrivateTransportError,
                "test_transport_send_queue",
                Some(1),
                Some(1),
                Some(1),
                ClientRetryGuidance::WithBackoff,
            ));
        }
        let message = decode_outbound(message);
        let mut state = self.state.lock().expect("transport lock");
        self.changed.notify_all();
        while state.send_blocked && !state.closed {
            state = self.changed.wait(state).expect("transport wait");
        }
        if state.closed {
            return Err(ClientTransportError::new(
                ClientTransportOperation::Send,
                PrivateTransportError,
            ));
        }
        if self.auto_ack_streams
            && let ClientMessage::OpenStream { request_id, .. }
            | ClientMessage::ResumeStream { request_id, .. } = &message
        {
            state.inbound.push_back(ServerMessage::Response {
                request_id: request_id.clone(),
                payload: Arc::from(&b"opened"[..]),
            });
        }
        state.sent.push(message);
        self.changed.notify_all();
        Ok(())
    }

    fn receive(&self) -> Result<ClientTransportInbound, ClientTransportError> {
        let mut state = self.state.lock().expect("transport lock");
        loop {
            if let Some(message) = state.inbound.pop_front() {
                return Ok(encode_inbound(message));
            }
            if state.closed {
                return Err(ClientTransportError::new(
                    ClientTransportOperation::Receive,
                    PrivateTransportError,
                ));
            }
            state = self.changed.wait(state).expect("transport wait");
        }
    }

    fn close(&self) {
        let mut state = self.state.lock().expect("transport lock");
        state.closed = true;
        self.changed.notify_all();
    }
}

const TEST_FRAME_BYTES: u32 = 1_024;

fn sdk_agreement(value: ConnectionAgreement) -> ClientConnectionAgreement {
    let bytes = HandshakeCodec::new(TEST_FRAME_BYTES)
        .expect("handshake codec")
        .encode_response(&HandshakeResponse::Accepted(value))
        .expect("encode agreement");
    ClientConnectionAgreement::decode_accepted(&bytes, TEST_FRAME_BYTES)
        .expect("decode SDK agreement")
}

fn raw_stream_id(value: &ClientStreamId) -> undercurrent_wire::StreamId {
    undercurrent_wire::StreamId::new(value.as_str()).expect("raw stream ID")
}

fn decode_outbound(value: ClientTransportOutbound) -> ClientMessage {
    let bytes = value.encode(TEST_FRAME_BYTES).expect("encode outbound");
    match WireCodec::new(TEST_FRAME_BYTES)
        .expect("wire codec")
        .decode_client(&bytes)
        .expect("decode outbound")
    {
        DecodeStatus::Complete(frame) => frame.message,
        DecodeStatus::NeedMore { .. } => panic!("complete outbound frame expected"),
    }
}

fn encode_inbound(value: ServerMessage) -> ClientTransportInbound {
    let bytes = WireCodec::new(TEST_FRAME_BYTES)
        .expect("wire codec")
        .encode_server(&value)
        .expect("encode inbound");
    ClientTransportInbound::decode(&bytes, TEST_FRAME_BYTES).expect("decode SDK inbound")
}

#[test]
fn deferred_credit_is_retried_when_writer_drains_without_another_receive() {
    let transport = Arc::new(MemoryTransport::new(true));
    let client = Client::connect(agreement(true), transport.clone(), client_limits(1)).unwrap();
    let opened = client
        .open_subscription(Arc::from(&b"terminal"[..]), Duration::from_secs(1))
        .unwrap();
    transport.set_send_blocked(true);
    client.ping().unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while transport.send_entries.load(Ordering::Acquire) < 2 {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
    client.ping().unwrap();
    transport.inject(ServerMessage::StreamItem {
        stream_id: raw_stream_id(&opened.subscription.stream_id()),
        item: InboundStreamItem::Data {
            sequence: StreamSequence::new(1),
            bytes: Arc::from(&b"last credit-funded item"[..]),
            flow_controlled_bytes: 32,
        },
    });
    opened
        .subscription
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    let deferred = opened.subscription.deferred_credit_bytes();
    assert!(deferred > 0);
    transport.set_send_blocked(false);
    transport.wait_for_sent(3);
    transport.wait_for_message(|m| matches!(m, ClientMessage::WindowUpdate { additional_bytes, .. } if *additional_bytes == deferred));
    assert_eq!(opened.subscription.deferred_credit_bytes(), 0);
    assert!(!client.is_closed());
    client.close();
}
