// SPDX-License-Identifier: Apache-2.0

use std::sync::mpsc;
use std::thread;

use undercurrent_multiplex::{ReplayLimits, SchedulerMessageLimits};
use undercurrent_wire::{
    CURRENT_PROTOCOL_VERSION, ConnectionHello, ConnectionId, ConnectionLimits, ConnectionPolicy,
    HeartbeatPolicy, NamespacedName, SpanId, TraceId, negotiate_connection,
};

use super::*;

const DEADLINE: Duration = Duration::from_secs(2);
type WaitHook = Box<dyn FnOnce() + Send>;

#[derive(Clone, Copy)]
pub(super) enum WaitPoint {
    AfterCheck,
    BeforePark,
}

#[derive(Default)]
pub(super) struct WaitHooks {
    after_check: Mutex<Option<WaitHook>>,
    before_park: Mutex<Option<WaitHook>>,
}

impl WaitHooks {
    fn slot(&self, point: WaitPoint) -> &Mutex<Option<WaitHook>> {
        match point {
            WaitPoint::AfterCheck => &self.after_check,
            WaitPoint::BeforePark => &self.before_park,
        }
    }

    pub(super) fn run(&self, point: WaitPoint) {
        let hook = self.slot(point).lock().expect("wait hook").take();
        if let Some(hook) = hook {
            hook();
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Event {
    Data,
    Credit,
    Close,
}

#[test]
fn writer_wakeup_data_after_queue_check() {
    check_wakeup(Event::Data, WaitPoint::AfterCheck);
}

#[test]
fn writer_wakeup_credit_after_queue_check() {
    check_wakeup(Event::Credit, WaitPoint::AfterCheck);
}

#[test]
fn writer_wakeup_close_after_queue_check() {
    check_wakeup(Event::Close, WaitPoint::AfterCheck);
}

#[test]
fn writer_wakeup_data_before_parking() {
    check_wakeup(Event::Data, WaitPoint::BeforePark);
}

#[test]
fn writer_wakeup_credit_before_parking() {
    check_wakeup(Event::Credit, WaitPoint::BeforePark);
}

#[test]
fn writer_wakeup_close_before_parking() {
    check_wakeup(Event::Close, WaitPoint::BeforePark);
}

fn check_wakeup(event: Event, point: WaitPoint) {
    let session = Arc::new(session(matches!(event, Event::Credit)));
    let stream = StreamId::new("wake-stream").expect("stream ID");
    open_stream(&session, &stream);
    if matches!(event, Event::Credit) {
        session
            .publish_stream(&stream, StreamSequence::new(1), Arc::from(vec![7; 512]))
            .expect("queue data without enough credit");
    }

    let (checked_tx, checked_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    *session
        .write_wait_hooks
        .slot(point)
        .lock()
        .expect("wait hook") = Some(Box::new(move || {
        checked_tx.send(()).expect("writer reached gate");
        resume_rx
            .recv_timeout(DEADLINE)
            .expect("release writer gate");
    }));
    let waiting = session.clone();
    let (write_tx, write_rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        write_tx
            .send(waiting.wait_for_write())
            .expect("writer result");
    });
    checked_rx
        .recv_timeout(DEADLINE)
        .expect("writer checks queue");

    let producing = session.clone();
    let produced_stream = stream.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (produced_tx, produced_rx) = mpsc::channel();
    let producer = thread::spawn(move || {
        started_tx.send(()).expect("producer started");
        match event {
            Event::Data => {
                producing
                    .publish_stream(
                        &produced_stream,
                        StreamSequence::new(1),
                        Arc::from(vec![7; 512]),
                    )
                    .expect("publish data");
            }
            Event::Credit => {
                producing
                    .accept(ClientMessage::WindowUpdate {
                        stream_id: produced_stream,
                        additional_bytes: 1_024,
                    })
                    .expect("grant credit");
            }
            Event::Close => {
                producing.close();
            }
        }
        produced_tx.send(()).expect("producer completed");
    });
    started_rx.recv_timeout(DEADLINE).expect("producer runs");

    // The original notifier completes while the writer holds its final stale
    // predicate. A synchronized notifier waits for enrollment instead. AfterCheck
    // also covers an event that must be retained before the wait mutex is taken.
    let produced_before_enrollment = produced_rx.recv_timeout(Duration::from_millis(100)).is_ok();
    resume_tx.send(()).expect("allow writer to park or recheck");
    if !produced_before_enrollment {
        produced_rx
            .recv_timeout(DEADLINE)
            .expect("producer must not deadlock with writer");
    }
    producer.join().expect("producer joins");

    let first = write_rx.recv_timeout(DEADLINE);
    let woke_without_rescue = first.is_ok();
    let result = match first {
        Ok(result) => result,
        Err(_) => {
            // Rescue a broken implementation before asserting so the regression
            // reports the missed wakeup without leaking its native writer thread.
            let guard = session.write_wait.lock().expect("wake lock");
            session.write_ready.notify_one();
            drop(guard);
            write_rx.recv_timeout(DEADLINE).expect("rescue writer")
        }
    }
    .expect("writer succeeds");
    writer.join().expect("writer joins");

    match event {
        Event::Close => assert!(result.is_none(), "closed writer exits"),
        Event::Data | Event::Credit => {
            let write = result.expect("writer selects queued data");
            let decoded = session
                .codec
                .decode_server(write.bytes())
                .expect("decode data");
            assert!(matches!(
                decoded,
                DecodeStatus::Complete(frame)
                    if matches!(&frame.message, ServerMessage::StreamItem {
                        stream_id,
                        item: undercurrent_multiplex::InboundStreamItem::Data { sequence, bytes, .. },
                    } if stream_id == &stream && *sequence == StreamSequence::new(1) && bytes.as_ref() == [7; 512])
            ));
            write.confirm_written().expect("confirm selected frame");
        }
    }
    session.close();
    assert!(
        woke_without_rescue,
        "{event:?} must wake the writer without another signal (producer finished before enrollment: {produced_before_enrollment})"
    );
}

fn open_stream(session: &ServerConnectionSession, stream: &StreamId) {
    let request = RequestId::new("wake-request").expect("request ID");
    session
        .accept(ClientMessage::OpenStream {
            request_id: request.clone(),
            stream_id: stream.clone(),
            trace_context: TraceContext::new(
                TraceId::new("wake-trace").expect("trace ID"),
                SpanId::new("wake-span").expect("span ID"),
            ),
            payload: Arc::from(&b"terminal"[..]),
        })
        .expect("open stream");
    session
        .activate_stream(
            &request,
            stream,
            ServerStreamConfiguration {
                kind: StreamKind::TerminalOutput,
                class: TrafficClass::Bulk,
                delivery: DeliverySemantics::Reliable,
                recovery: StreamRecoveryAction::RestartFromAvailableOffset,
                retain_for_resume: true,
            },
            Arc::from(&b"opened"[..]),
        )
        .expect("activate stream");
    session
        .next_write()
        .expect("select response")
        .expect("open response")
        .confirm_written()
        .expect("confirm open response");
}

fn session(credit_limited: bool) -> ServerConnectionSession {
    let client_id = ClientId::new("wake-client").expect("client ID");
    let epoch = ServerEpoch::new("wake-epoch").expect("epoch");
    let credit = if credit_limited { 256 } else { 4_096 };
    let limits =
        ConnectionLimits::new(1_024, 4, 4, 1_000_000, 8_192, credit).expect("connection limits");
    let capabilities = [
        connection_capabilities::MULTIPLEX,
        connection_capabilities::FLOW_CONTROL,
        connection_capabilities::STREAM_RESUME,
        connection_capabilities::TRACE_CONTEXT,
    ]
    .into_iter()
    .map(|name| NamespacedName::new(name).expect("capability"))
    .collect::<BTreeSet<_>>();
    let hello = ConnectionHello::new(
        client_id.clone(),
        undercurrent_wire::CURRENT_PROTOCOL_VERSION,
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
    let agreement = negotiate_connection(
        &hello,
        &policy,
        ConnectionId::new("wake-connection").expect("connection ID"),
        epoch.clone(),
    )
    .expect("agreement");
    let scheduler = SchedulerLimits::new(
        limits,
        [64 * 1_024, 128 * 1_024, 256 * 1_024, 512 * 1_024],
        credit,
        4_096,
        SchedulerMessageLimits::new(8_192, 1_024, [1_024, 2_048, 2_048, 3_072])
            .expect("message limits"),
    )
    .expect("scheduler limits");
    let replay = Arc::new(ReplayJournal::new(
        epoch,
        ReplayLimits::new(1_000, 8_192, 8_192_000, 1_024).expect("replay limits"),
    ));
    ServerConnectionSession::new(
        client_id,
        agreement,
        scheduler,
        replay,
        ServerDiagnosticLimits::new(100, 100).expect("diagnostic limits"),
    )
    .expect("session")
}

fn observed_response() -> (ServerConnectionSession, Arc<ServerResponseWrite>) {
    let connection = session(false);
    let request = RequestId::new("reset-response").unwrap();
    connection
        .accept(ClientMessage::Request {
            request_id: request.clone(),
            trace_context: TraceContext::new(
                TraceId::new("reset-trace").unwrap(),
                SpanId::new("reset-span").unwrap(),
            ),
            payload: Arc::from(&b"reset"[..]),
        })
        .unwrap();
    let signal = Arc::new(ServerResponseWrite::default());
    connection
        .complete_request_observed(&request, Arc::from(&b"accepted"[..]), signal.clone())
        .unwrap();
    (connection, signal)
}

#[test]
fn response_completion_requires_physical_write_not_queue_or_selection() {
    let (connection, signal) = observed_response();
    assert!(!signal.is_finished());
    let write = connection.next_write().unwrap().unwrap();
    assert!(!signal.is_finished());
    write.confirm_written().unwrap();
    assert!(signal.was_written());
    connection.close();
    assert!(signal.was_written());
}

#[test]
fn response_completion_observes_disconnect_before_or_during_write() {
    let (connection, signal) = observed_response();
    connection.close();
    assert!(signal.is_finished());
    assert!(!signal.was_written());

    let (connection, signal) = observed_response();
    let write = connection.next_write().unwrap().unwrap();
    connection.close();
    assert!(
        !signal.is_finished(),
        "selected write still owns its completion"
    );
    drop(write);
    assert!(signal.is_finished());
    assert!(!signal.was_written());
}

#[test]
fn response_completion_observes_failed_queue_admission() {
    let connection = session(false);
    let signal = Arc::new(ServerResponseWrite::default());
    assert!(
        connection
            .complete_request_observed(
                &RequestId::new("missing").unwrap(),
                Arc::from(&b"accepted"[..]),
                signal.clone()
            )
            .is_err()
    );
    assert!(signal.is_finished());
    assert!(!signal.was_written());
}
