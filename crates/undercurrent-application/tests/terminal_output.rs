// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;

use undercurrent_application::*;
use undercurrent_domain::{OutputCaptureMode, PayloadDigest, SessionId};

fn limits(
    per_session_bytes: usize,
    global_bytes: usize,
    chunk_bytes: usize,
    sessions: usize,
) -> TerminalOutputLimits {
    TerminalOutputLimits {
        per_session_bytes,
        global_bytes,
        chunk_bytes,
        append_bytes: per_session_bytes.max(chunk_bytes) * 2,
        replay_bytes: global_bytes,
        sessions,
        subscriptions: sessions.max(8),
    }
}

fn session_id(value: impl Into<String>) -> SessionId {
    SessionId::new(value).expect("session ID is valid")
}

fn replay_bytes(replay: &OutputReplay) -> Vec<u8> {
    match replay {
        OutputReplay::Data { slices, .. } => slices
            .iter()
            .flat_map(|slice| slice.bytes().iter().copied())
            .collect(),
        OutputReplay::Gap { .. } => panic!("expected replay data"),
    }
}

fn stream_replay_bytes(replay: &TerminalOutputStreamReplay) -> Vec<u8> {
    match replay {
        TerminalOutputStreamReplay::Data { slices, .. } => slices
            .iter()
            .flat_map(|slice| slice.bytes().iter().copied())
            .collect(),
        TerminalOutputStreamReplay::Gap { .. } => panic!("expected stream replay data"),
    }
}

const fn retained_chunk_digest_remains_const(slice: &OutputSlice) -> PayloadDigest {
    slice.retained_chunk_digest()
}

#[derive(Default)]
struct CountWake {
    calls: AtomicUsize,
}

impl SubscriptionWake for CountWake {
    fn wake(&self) {
        self.calls.fetch_add(1, Ordering::AcqRel);
    }
}

#[test]
fn limits_reject_zero_and_chunks_larger_than_retention() {
    let zero = limits(8, 16, 4, 2);
    let zero = TerminalOutputLimits {
        replay_bytes: 0,
        ..zero
    };
    assert!(matches!(
        zero.validate(),
        Err(TerminalOutputError::InvalidLimits { .. })
    ));

    assert!(matches!(
        limits(3, 16, 4, 2).validate(),
        Err(TerminalOutputError::InvalidLimits { .. })
    ));
}

#[test]
fn session_eviction_reports_gap_and_replays_a_bounded_zero_copy_range() {
    let mut output_limits = limits(8, 32, 4, 4);
    output_limits.replay_bytes = 6;
    let store = TerminalOutputStore::new(output_limits).expect("limits are valid");
    let session_id = session_id("session-1");

    let append = store
        .append(&session_id, b"abcdefghijkl")
        .expect("append succeeds");
    assert_eq!(append.start, OutputOffset::ZERO);
    assert_eq!(append.end, OutputOffset::new(12));
    assert_eq!(append.available_from, OutputOffset::new(4));
    assert_eq!(append.session_bytes_evicted, 4);
    assert_eq!(append.global_bytes_evicted, 4);

    assert_eq!(
        store
            .replay(
                &session_id,
                OutputOffset::ZERO,
                6,
                OutputGapRecovery::RequestScreenSnapshot,
            )
            .expect("gap is returned"),
        OutputReplay::Gap {
            requested: OutputOffset::ZERO,
            available_from: OutputOffset::new(4),
            next_offset: OutputOffset::new(12),
            recovery: OutputGapRecovery::RequestScreenSnapshot,
        }
    );

    let replay = store
        .replay(
            &session_id,
            OutputOffset::new(4),
            6,
            OutputGapRecovery::RestartAtAvailableOffset,
        )
        .expect("replay succeeds");
    assert_eq!(replay_bytes(&replay), b"efghij");
    let OutputReplay::Data {
        from,
        to,
        next_offset,
        has_more,
        ..
    } = replay
    else {
        panic!("expected replay data");
    };
    assert_eq!(from, OutputOffset::new(4));
    assert_eq!(to, OutputOffset::new(10));
    assert_eq!(next_offset, OutputOffset::new(12));
    assert!(has_more);
}

#[test]
fn retained_chunk_digest_covers_backing_chunk_while_integrity_covers_replay_page() {
    let store = TerminalOutputStore::new(limits(32, 32, 4, 1)).expect("limits are valid");
    let session_id = session_id("session-digest");

    assert_eq!(
        store
            .append(&session_id, b"abcdefgh")
            .expect("append succeeds"),
        OutputAppend {
            start: OutputOffset::ZERO,
            end: OutputOffset::new(8),
            available_from: OutputOffset::ZERO,
            session_bytes_evicted: 0,
            global_bytes_evicted: 0,
            global_bytes_retained: 8,
        }
    );

    let replay = store
        .replay(
            &session_id,
            OutputOffset::new(1),
            5,
            OutputGapRecovery::RestartAtAvailableOffset,
        )
        .expect("replay succeeds");
    let OutputReplay::Data {
        slices,
        from,
        to,
        next_offset,
        has_more,
    } = &replay
    else {
        panic!("expected replay data");
    };

    assert_eq!(*from, OutputOffset::new(1));
    assert_eq!(*to, OutputOffset::new(6));
    assert_eq!(*next_offset, OutputOffset::new(8));
    assert!(*has_more);
    assert_eq!(slices.len(), 2);
    assert_eq!(slices[0].bytes(), b"bcd");
    assert_eq!(slices[0].digest(), PayloadDigest::of_bytes(b"bcd"));
    assert_eq!(
        retained_chunk_digest_remains_const(&slices[0]),
        PayloadDigest::of_bytes(b"abcd")
    );
    assert_eq!(slices[1].bytes(), b"ef");
    assert_eq!(slices[1].digest(), PayloadDigest::of_bytes(b"ef"));
    assert_eq!(
        slices[1].retained_chunk_digest(),
        PayloadDigest::of_bytes(b"efgh")
    );

    let integrity = replay
        .integrity()
        .expect("replay integrity is valid")
        .expect("data replay has integrity");
    assert_eq!(integrity.digest(), PayloadDigest::of_bytes(b"bcdef"));
    assert_eq!(integrity.delivered_bytes(), 5);
    assert_eq!(integrity.source_bytes(), 7);
    assert!(integrity.truncated());
    assert_eq!(
        store.session_status(&session_id).expect("status succeeds"),
        Some(SessionOutputStatus {
            output_capture_mode: OutputCaptureMode::Terminal,
            available_from: OutputOffset::ZERO,
            next_offset: OutputOffset::new(8),
            retained_bytes: 8,
            retained_chunks: 2,
        })
    );
    assert_eq!(store.status().expect("status succeeds").retained_bytes, 8);
}

#[test]
fn checkpointed_append_compares_and_advances_under_one_store_lock() {
    let store = TerminalOutputStore::new(limits(32, 64, 8, 2)).expect("limits are valid");
    let session_id = session_id("session-checkpoint");

    let first = store
        .append_at_checkpoint(&session_id, OutputOffset::ZERO, b"abc")
        .expect("matching checkpoint appends");
    assert_eq!(first.start, OutputOffset::ZERO);
    assert_eq!(first.end, OutputOffset::new(3));

    assert_eq!(
        store
            .append_at_checkpoint(&session_id, OutputOffset::ZERO, b"must-not-append")
            .unwrap_err(),
        TerminalOutputError::CheckpointMismatch {
            expected: OutputOffset::ZERO,
            observed: OutputOffset::new(3),
        }
    );
    let status = store
        .session_status(&session_id)
        .unwrap()
        .expect("session status");
    assert_eq!(status.next_offset, OutputOffset::new(3));
    assert_eq!(status.retained_bytes, 3);
}

#[test]
fn output_subscription_coalesces_wakes_and_reports_retention_gap() {
    let store = TerminalOutputStore::new(limits(8, 32, 4, 4)).expect("limits are valid");
    let session_id = session_id("session-1");
    let wake = Arc::new(CountWake::default());
    let opened = store
        .subscribe(
            session_id.clone(),
            OutputOffset::ZERO,
            8,
            OutputGapRecovery::RestartAtAvailableOffset,
            wake.clone(),
        )
        .expect("subscription opens");
    assert!(replay_bytes(opened.initial()).is_empty());
    let (_, subscription) = opened.into_parts();

    store
        .append(&session_id, b"abcdefghijkl")
        .expect("output append succeeds");
    store
        .append(&session_id, b"mn")
        .expect("second output append succeeds");
    assert_eq!(wake.calls.load(Ordering::Acquire), 1);

    let gap = subscription
        .try_receive()
        .expect("subscription read succeeds")
        .expect("gap is pending");
    let OutputReplay::Gap {
        requested,
        available_from,
        next_offset,
        recovery,
    } = gap
    else {
        panic!("expected retention gap")
    };
    assert_eq!(requested, OutputOffset::ZERO);
    assert_eq!(available_from, OutputOffset::new(8));
    assert_eq!(next_offset, OutputOffset::new(14));
    assert_eq!(recovery, OutputGapRecovery::RestartAtAvailableOffset);
    let replay = subscription
        .try_receive()
        .expect("gap continuation succeeds")
        .expect("retained data is pending");
    assert_eq!(replay_bytes(&replay), b"ijklmn");
    assert!(
        subscription
            .try_receive()
            .expect("clean subscription read succeeds")
            .is_none()
    );
    assert_eq!(store.status().expect("status succeeds").subscriptions, 1);

    subscription.close();
    assert_eq!(store.status().expect("status succeeds").subscriptions, 0);
}

#[test]
fn digest_free_stream_subscription_preserves_gaps_offsets_bytes_and_accounting() {
    let store = TerminalOutputStore::new(limits(8, 32, 4, 4)).expect("limits are valid");
    let session_id = session_id("session-stream");
    store
        .append(&session_id, b"abcdefghijkl")
        .expect("output append succeeds");
    let opened = store
        .subscribe_stream(
            session_id.clone(),
            OutputOffset::ZERO,
            6,
            OutputGapRecovery::RestartAtAvailableOffset,
            Arc::new(CountWake::default()),
        )
        .expect("stream subscription opens");
    assert_eq!(
        opened.initial(),
        &TerminalOutputStreamReplay::Gap {
            requested: OutputOffset::ZERO,
            available_from: OutputOffset::new(4),
            next_offset: OutputOffset::new(12),
            recovery: OutputGapRecovery::RestartAtAvailableOffset,
        }
    );
    let (_, subscription) = opened.into_parts();

    let first = subscription
        .try_receive()
        .expect("stream replay succeeds")
        .expect("retained stream data is pending");
    assert_eq!(stream_replay_bytes(&first), b"efghij");
    let TerminalOutputStreamReplay::Data {
        from,
        to,
        next_offset,
        has_more,
        ..
    } = first
    else {
        panic!("expected stream replay data");
    };
    assert_eq!(from, OutputOffset::new(4));
    assert_eq!(to, OutputOffset::new(10));
    assert_eq!(next_offset, OutputOffset::new(12));
    assert!(has_more);

    let second = subscription
        .try_receive()
        .expect("stream continuation succeeds")
        .expect("final stream data is pending");
    assert_eq!(stream_replay_bytes(&second), b"kl");
    assert!(
        subscription
            .try_receive()
            .expect("clean stream read succeeds")
            .is_none()
    );
    assert_eq!(
        store.session_status(&session_id).expect("status succeeds"),
        Some(SessionOutputStatus {
            output_capture_mode: OutputCaptureMode::Terminal,
            available_from: OutputOffset::new(4),
            next_offset: OutputOffset::new(12),
            retained_bytes: 8,
            retained_chunks: 2,
        })
    );
    assert_eq!(store.status().expect("status succeeds").retained_bytes, 8);
}

#[test]
fn global_eviction_is_oldest_append_first_across_sessions() {
    let store = TerminalOutputStore::new(limits(16, 12, 4, 4)).expect("limits are valid");
    let first = session_id("session-1");
    let second = session_id("session-2");
    store.append(&first, b"abcdefgh").expect("append succeeds");

    let append = store.append(&second, b"ijklmnop").expect("append succeeds");
    assert_eq!(append.global_bytes_evicted, 4);
    assert_eq!(append.global_bytes_retained, 12);
    assert_eq!(
        store.session_status(&first).expect("status succeeds"),
        Some(SessionOutputStatus {
            output_capture_mode: OutputCaptureMode::Terminal,
            available_from: OutputOffset::new(4),
            next_offset: OutputOffset::new(8),
            retained_bytes: 4,
            retained_chunks: 1,
        })
    );
    assert_eq!(
        store.session_status(&second).expect("status succeeds"),
        Some(SessionOutputStatus {
            output_capture_mode: OutputCaptureMode::Terminal,
            available_from: OutputOffset::ZERO,
            next_offset: OutputOffset::new(8),
            retained_bytes: 8,
            retained_chunks: 2,
        })
    );
}

#[test]
fn explicit_capture_mode_and_retention_gap_preserve_absolute_offsets() {
    let store = TerminalOutputStore::new(limits(16, 32, 4, 2)).expect("limits are valid");
    let session = session_id("opaque-absolute-offsets");
    store
        .begin_session(
            session.clone(),
            OutputCaptureMode::Opaque,
            OutputOffset::new(40),
        )
        .expect("opaque session begins at its remote offset");
    store
        .append_at_checkpoint(&session, OutputOffset::new(40), b"abcdefgh")
        .expect("opaque bytes append at the absolute checkpoint");
    store
        .establish_retention_gap(&session, OutputCaptureMode::Opaque, OutputOffset::new(64))
        .expect("remote retention gap advances monotonically");

    assert_eq!(
        store
            .replay(
                &session,
                OutputOffset::new(48),
                8,
                OutputGapRecovery::RestartAtAvailableOffset,
            )
            .expect("explicit gap is readable"),
        OutputReplay::Gap {
            requested: OutputOffset::new(48),
            available_from: OutputOffset::new(64),
            next_offset: OutputOffset::new(64),
            recovery: OutputGapRecovery::RestartAtAvailableOffset,
        }
    );
    assert_eq!(
        store.session_status(&session).expect("status succeeds"),
        Some(SessionOutputStatus {
            output_capture_mode: OutputCaptureMode::Opaque,
            available_from: OutputOffset::new(64),
            next_offset: OutputOffset::new(64),
            retained_bytes: 0,
            retained_chunks: 0,
        })
    );
    assert_eq!(
        store.begin_session(session, OutputCaptureMode::Terminal, OutputOffset::new(64),),
        Err(TerminalOutputError::CaptureModeMismatch {
            expected: OutputCaptureMode::Opaque,
            observed: OutputCaptureMode::Terminal,
        })
    );
}

#[test]
fn offsets_continue_after_eviction_and_removed_replay_slices_remain_valid() {
    let store = TerminalOutputStore::new(limits(4, 4, 4, 1)).expect("limits are valid");
    let session_id = session_id("session-1");
    store
        .append(&session_id, b"abcdefgh")
        .expect("append succeeds");
    let append = store.append(&session_id, b"ijkl").expect("append succeeds");
    assert_eq!(append.start, OutputOffset::new(8));
    assert_eq!(append.end, OutputOffset::new(12));

    let replay = store
        .replay(
            &session_id,
            OutputOffset::new(8),
            4,
            OutputGapRecovery::RestartAtAvailableOffset,
        )
        .expect("replay succeeds");
    assert_eq!(
        store.remove_session(&session_id).expect("remove succeeds"),
        4
    );
    assert_eq!(store.status().expect("status succeeds").sessions, 0);
    assert_eq!(replay_bytes(&replay), b"ijkl");
    let OutputReplay::Data { slices, .. } = replay else {
        panic!("expected replay data");
    };
    assert_eq!(
        retained_chunk_digest_remains_const(&slices[0]),
        PayloadDigest::of_bytes(b"ijkl")
    );
}

#[test]
fn capacity_bounds_three_hundred_session_records_without_bulk_reads() {
    let store = TerminalOutputStore::new(limits(1, 300, 1, 300)).expect("limits are valid");
    for index in 0..300 {
        store
            .append(&session_id(format!("session-{index}")), b"x")
            .expect("session append succeeds within capacity");
    }
    assert_eq!(
        store.status().expect("status succeeds"),
        TerminalOutputStatus {
            sessions: 300,
            retained_bytes: 300,
            retained_chunks: 300,
            subscriptions: 0,
            dirty_subscriptions: 0,
            subscription_limit: 300,
        }
    );
    let overflow = session_id("session-overflow");
    assert_eq!(
        store.append(&overflow, b"x"),
        Err(TerminalOutputError::SessionCapacity { limit: 300 })
    );
    assert_eq!(
        store
            .append(&overflow, b"")
            .expect("empty append is a no-op"),
        OutputAppend {
            start: OutputOffset::ZERO,
            end: OutputOffset::ZERO,
            available_from: OutputOffset::ZERO,
            session_bytes_evicted: 0,
            global_bytes_evicted: 0,
            global_bytes_retained: 300,
        }
    );
    assert_eq!(store.status().expect("status succeeds").sessions, 300);
}

#[test]
fn output_producer_diagnostics_are_bounded_payload_free_and_stably_ordered() {
    let store = TerminalOutputStore::new(limits(16, 32, 4, 3)).expect("limits are valid");
    store
        .append(&session_id("session-c"), b"12345678")
        .expect("third append");
    store
        .append(&session_id("session-a"), b"1234")
        .expect("first append");
    store
        .append(&session_id("session-b"), b"abcdefgh")
        .expect("second append");

    let page = store
        .session_status_page(None, 2)
        .expect("bounded session page");
    assert_eq!(
        page.items
            .iter()
            .map(|(session_id, _)| session_id.as_str())
            .collect::<Vec<_>>(),
        ["session-a", "session-b"]
    );
    assert_eq!(page.entries_examined, 3);
    assert_eq!(
        page.next_after.as_ref().map(SessionId::as_str),
        Some("session-b")
    );

    let top = store
        .top_session_statuses(2)
        .expect("bounded top producers");
    assert_eq!(
        top.iter()
            .map(|(session_id, status)| (session_id.as_str(), status.retained_bytes))
            .collect::<Vec<_>>(),
        [("session-b", 8), ("session-c", 8)]
    );
    assert_eq!(
        store.top_session_statuses(4),
        Err(TerminalOutputError::DiagnosticCapacity { limit: 3 })
    );
}

#[test]
fn concurrent_appends_allocate_non_overlapping_monotonic_ranges() {
    let store =
        Arc::new(TerminalOutputStore::new(limits(1_024, 1_024, 8, 1)).expect("limits are valid"));
    let ranges = Arc::new(Mutex::new(Vec::new()));
    let session_id = session_id("session-1");
    let mut threads = Vec::new();
    for index in 0..16_u8 {
        let store = Arc::clone(&store);
        let ranges = Arc::clone(&ranges);
        let session_id = session_id.clone();
        threads.push(thread::spawn(move || {
            let bytes = [index; 8];
            let append = store.append(&session_id, &bytes).expect("append succeeds");
            ranges
                .lock()
                .expect("range lock is available")
                .push((append.start, append.end));
        }));
    }
    for thread in threads {
        thread.join().expect("writer does not panic");
    }

    let mut ranges = ranges.lock().expect("range lock is available").clone();
    ranges.sort_unstable();
    for (index, (start, end)) in ranges.into_iter().enumerate() {
        assert_eq!(start, OutputOffset::new((index * 8) as u64));
        assert_eq!(end, OutputOffset::new(((index + 1) * 8) as u64));
    }
    assert_eq!(
        store
            .session_status(&session_id)
            .expect("status succeeds")
            .expect("session exists")
            .next_offset,
        OutputOffset::new(128)
    );
}

#[test]
fn replay_rejects_unbounded_and_future_reads_without_exposing_payloads() {
    let store = TerminalOutputStore::new(limits(8, 8, 4, 1)).expect("limits are valid");
    let session_id = session_id("session-1");
    store
        .append(&session_id, b"private!")
        .expect("append succeeds");
    assert_eq!(
        store.replay(
            &session_id,
            OutputOffset::ZERO,
            9,
            OutputGapRecovery::RestartAtAvailableOffset,
        ),
        Err(TerminalOutputError::ReplayCapacity { limit: 8 })
    );
    assert_eq!(
        store.replay(
            &session_id,
            OutputOffset::new(9),
            8,
            OutputGapRecovery::RestartAtAvailableOffset,
        ),
        Err(TerminalOutputError::FutureOffset {
            requested: OutputOffset::new(9),
            next_offset: OutputOffset::new(8),
        })
    );

    let replay = store
        .replay(
            &session_id,
            OutputOffset::ZERO,
            8,
            OutputGapRecovery::RestartAtAvailableOffset,
        )
        .expect("replay succeeds");
    assert!(!format!("{replay:?}").contains("private"));
}

#[test]
fn authorized_read_records_payload_free_metrics_and_denial_precedes_retention() {
    let store = TerminalOutputStore::new(limits(16, 16, 4, 1)).expect("limits are valid");
    let session_id = session_id("session-1");
    store
        .append(&session_id, b"private-output")
        .expect("append succeeds");
    let clock = TestClock::default();
    let telemetry = TestTelemetry::default();
    let permit = TestAuthorizer::new(AccessDecision::Permit);
    let service = ReadTerminalOutputService::new(&permit, &store, &clock, &telemetry);

    let replay = block_on(service.read(
        &session_id,
        OutputOffset::ZERO,
        7,
        OutputGapRecovery::RestartAtAvailableOffset,
    ))
    .expect("authorized read succeeds");

    assert_eq!(replay_bytes(&replay), b"private");
    assert_eq!(permit.calls.load(Ordering::Acquire), 1);
    assert!(telemetry.has_event(DiagnosticEventName::TerminalOutputRead));
    assert!(telemetry.has_metric(MetricName::TerminalOutputBytesRead, 7.0));
    assert!(!format!("{:?}", telemetry.diagnostics()).contains("private"));

    let deny = TestAuthorizer::new(AccessDecision::Deny(AccessDenialReason::Forbidden));
    let service = ReadTerminalOutputService::new(&deny, &store, &clock, &telemetry);
    let error = block_on(service.read(
        &session_id,
        OutputOffset::new(999),
        7,
        OutputGapRecovery::RestartAtAvailableOffset,
    ))
    .expect_err("authorization denial precedes invalid offset lookup");
    assert_eq!(error.category(), ApplicationErrorCategory::PermissionDenied);
    assert!(telemetry.has_event(DiagnosticEventName::AuthorizationDenied));
}

struct TestAuthorizer {
    decision: AccessDecision,
    calls: AtomicUsize,
}

impl TestAuthorizer {
    fn new(decision: AccessDecision) -> Self {
        Self {
            decision,
            calls: AtomicUsize::new(0),
        }
    }
}

impl AuthorizationPort for TestAuthorizer {
    fn authorize<'a>(
        &'a self,
        _action: AccessAction,
        _resource: &'a AccessResource,
    ) -> PortFuture<'a, Result<AccessDecision, PortError>> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Box::pin(async move { Ok(self.decision) })
    }
}

#[derive(Default)]
struct TestClock {
    now: AtomicI64,
}

impl Clock for TestClock {
    fn now(&self) -> undercurrent_domain::TimestampMillis {
        undercurrent_domain::TimestampMillis::new(self.now.fetch_add(1, Ordering::AcqRel))
    }
}

#[derive(Default)]
struct TestTelemetry {
    diagnostics: Mutex<Vec<DiagnosticRecord>>,
    metrics: Mutex<Vec<MetricObservation>>,
}

impl TestTelemetry {
    fn diagnostics(&self) -> Vec<DiagnosticRecord> {
        self.diagnostics.lock().expect("diagnostic lock").clone()
    }

    fn has_event(&self, event: DiagnosticEventName) -> bool {
        self.diagnostics()
            .iter()
            .any(|record| record.event == event)
    }

    fn has_metric(&self, name: MetricName, value: f64) -> bool {
        self.metrics
            .lock()
            .expect("metric lock")
            .iter()
            .any(|metric| metric.name == name && metric.value == value)
    }
}

impl Telemetry for TestTelemetry {
    fn emit_diagnostic(&self, record: &DiagnosticRecord) {
        self.diagnostics
            .lock()
            .expect("diagnostic lock")
            .push(record.clone());
    }

    fn record_metric(&self, observation: MetricObservation) {
        self.metrics.lock().expect("metric lock").push(observation);
    }

    fn record_session_metric(&self, observation: &SessionMetricObservation) {
        self.record_metric(observation.metric());
    }
}

fn block_on<T>(future: impl Future<Output = T>) -> T {
    struct NoopWake;
    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    let waker: Waker = Arc::new(NoopWake).into();
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("test future unexpectedly yielded"),
    }
}
