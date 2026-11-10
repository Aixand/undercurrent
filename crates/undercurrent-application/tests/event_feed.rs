// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use undercurrent_application::*;
use undercurrent_domain::*;

#[test]
fn event_filters_and_pages_reject_unbounded_inputs() {
    let sessions = BTreeSet::from([session_id("one"), session_id("two")]);
    let error = SessionEventFilter::new(sessions, BTreeSet::new(), 1, 16)
        .expect_err("session filter exceeds limit");
    assert_eq!(error.field(), "filter_sessions");

    let error = SessionEventReadRequest::new(
        SessionEventFilter::default(),
        SessionEventStart::Earliest,
        11,
        10,
    )
    .expect_err("page exceeds limit");
    assert_eq!(error.field(), "event_page_size");
}

#[test]
fn explicit_sessions_are_authorized_independently_before_journal_access() {
    let reader = TestReader::new(valid_batch(vec![], Some(sequence(10))));
    let authorizer = TestAuthorizer::deny_session("two");
    let telemetry = TestTelemetry::default();
    let clock = TestClock::default();
    let service = ReadSessionEventsService::new(&authorizer, &reader, &clock, &telemetry);
    let request = request(
        SessionEventFilter::new(
            BTreeSet::from([session_id("one"), session_id("two")]),
            BTreeSet::new(),
            8,
            16,
        )
        .expect("filter is valid"),
        SessionEventStart::Earliest,
        10,
    );

    let error = block_on(service.read(&request)).expect_err("second session is denied");

    assert_eq!(error.category(), ApplicationErrorCategory::PermissionDenied);
    assert_eq!(reader.reads.load(Ordering::SeqCst), 0);
    assert_eq!(
        authorizer
            .resources
            .lock()
            .expect("authorizer lock")
            .as_slice(),
        &[
            AccessResource::Session(session_id("one")),
            AccessResource::Session(session_id("two")),
        ]
    );
    assert!(telemetry.has_event(DiagnosticEventName::AuthorizationDenied));
}

#[test]
fn authorized_read_preserves_gap_checkpoint_cost_and_metrics() {
    let gap = SessionEventGap::new(sequence(2), sequence(4), sequence(5));
    let batch = SessionEventReadBatch::new(
        vec![record(5, "one", DomainEventKind::LifecycleChanged)],
        Some(sequence(5)),
        Some(gap),
        true,
        SessionEventQueryCost {
            index_entries_examined: 2,
        },
    );
    let reader = TestReader::new(batch);
    let authorizer = TestAuthorizer::permit();
    let telemetry = TestTelemetry::default();
    let clock = TestClock::default();
    let service = ReadSessionEventsService::new(&authorizer, &reader, &clock, &telemetry);
    let filter = SessionEventFilter::new(
        BTreeSet::from([session_id("one")]),
        BTreeSet::from([DomainEventKind::LifecycleChanged]),
        8,
        16,
    )
    .expect("filter is valid");
    let request = request(filter, SessionEventStart::Resume { after: sequence(1) }, 1);

    let result = block_on(service.read(&request)).expect("read succeeds");

    assert_eq!(result.records().len(), 1);
    assert_eq!(result.gap(), Some(gap));
    assert_eq!(result.checkpoint(), Some(sequence(5)));
    assert!(result.has_more());
    assert!(telemetry.has_event(DiagnosticEventName::SessionEventsRead));
    assert!(telemetry.has_metric(MetricName::QueryRowsReturned, 1.0));
    assert!(telemetry.has_metric(MetricName::QueryIndexEntriesExamined, 2.0));
}

#[test]
fn unrelated_or_reordered_adapter_records_are_invariant_failures() {
    let batch = valid_batch(
        vec![
            record(3, "other", DomainEventKind::ForegroundChanged),
            record(2, "one", DomainEventKind::ForegroundChanged),
        ],
        Some(sequence(3)),
    );
    let reader = TestReader::new(batch);
    let authorizer = TestAuthorizer::permit();
    let telemetry = TestTelemetry::default();
    let clock = TestClock::default();
    let service = ReadSessionEventsService::new(&authorizer, &reader, &clock, &telemetry);
    let filter =
        SessionEventFilter::new(BTreeSet::from([session_id("one")]), BTreeSet::new(), 8, 16)
            .expect("filter is valid");

    let error = block_on(service.read(&request(filter, SessionEventStart::Earliest, 4)))
        .expect_err("invalid adapter order is rejected");

    assert!(matches!(
        error,
        SessionEventReadError::InvalidBatch(SessionEventBatchViolation::SequenceOrder)
    ));
    assert!(telemetry.has_event(DiagnosticEventName::SessionEventReadFailed));
}

#[test]
fn continuation_and_resume_checkpoints_cannot_skip_or_move_backward() {
    let authorizer = TestAuthorizer::permit();
    let telemetry = TestTelemetry::default();
    let clock = TestClock::default();
    let jumping_reader = TestReader::new(SessionEventReadBatch::new(
        vec![record(2, "one", DomainEventKind::ForegroundChanged)],
        Some(sequence(4)),
        None,
        true,
        SessionEventQueryCost::default(),
    ));
    let service = ReadSessionEventsService::new(&authorizer, &jumping_reader, &clock, &telemetry);

    let error = block_on(service.read(&request(
        SessionEventFilter::default(),
        SessionEventStart::Earliest,
        1,
    )))
    .expect_err("continuation cannot skip records through its checkpoint");
    assert!(matches!(
        error,
        SessionEventReadError::InvalidBatch(SessionEventBatchViolation::InvalidCheckpoint)
    ));

    let backward_reader = TestReader::new(valid_batch(vec![], Some(sequence(3))));
    let service = ReadSessionEventsService::new(&authorizer, &backward_reader, &clock, &telemetry);
    let error = block_on(service.read(&request(
        SessionEventFilter::default(),
        SessionEventStart::Resume { after: sequence(4) },
        1,
    )))
    .expect_err("resume cannot move its checkpoint backward");
    assert!(matches!(
        error,
        SessionEventReadError::InvalidBatch(SessionEventBatchViolation::InvalidCheckpoint)
    ));
}

#[test]
fn unfiltered_read_authorizes_the_collection_once() {
    let reader = TestReader::new(valid_batch(vec![], None));
    let authorizer = TestAuthorizer::permit();
    let telemetry = TestTelemetry::default();
    let clock = TestClock::default();
    let service = ReadSessionEventsService::new(&authorizer, &reader, &clock, &telemetry);

    block_on(service.read(&request(
        SessionEventFilter::default(),
        SessionEventStart::Live,
        4,
    )))
    .expect("read succeeds");

    assert_eq!(
        authorizer
            .resources
            .lock()
            .expect("authorizer lock")
            .as_slice(),
        &[AccessResource::Sessions]
    );
}

struct TestReader {
    batch: SessionEventReadBatch,
    reads: AtomicUsize,
}

impl TestReader {
    fn new(batch: SessionEventReadBatch) -> Self {
        Self {
            batch,
            reads: AtomicUsize::new(0),
        }
    }
}

impl SessionEventReader for TestReader {
    fn read<'a>(
        &'a self,
        _request: &'a SessionEventReadRequest,
    ) -> PortFuture<'a, Result<SessionEventReadBatch, PortError>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(self.batch.clone()) })
    }
}

struct TestAuthorizer {
    denied_session: Option<SessionId>,
    resources: Mutex<Vec<AccessResource>>,
}

impl TestAuthorizer {
    fn permit() -> Self {
        Self {
            denied_session: None,
            resources: Mutex::new(Vec::new()),
        }
    }

    fn deny_session(name: &str) -> Self {
        Self {
            denied_session: Some(session_id(name)),
            resources: Mutex::new(Vec::new()),
        }
    }
}

impl AuthorizationPort for TestAuthorizer {
    fn authorize<'a>(
        &'a self,
        action: AccessAction,
        resource: &'a AccessResource,
    ) -> PortFuture<'a, Result<AccessDecision, PortError>> {
        assert_eq!(action, AccessAction::ReadEvents);
        self.resources
            .lock()
            .expect("authorizer lock")
            .push(resource.clone());
        let denied = matches!(resource, AccessResource::Session(id) if Some(id) == self.denied_session.as_ref());
        Box::pin(async move {
            if denied {
                Ok(AccessDecision::Deny(AccessDenialReason::Forbidden))
            } else {
                Ok(AccessDecision::Permit)
            }
        })
    }
}

#[derive(Default)]
struct TestClock(AtomicI64);

impl Clock for TestClock {
    fn now(&self) -> TimestampMillis {
        TimestampMillis::new(self.0.fetch_add(1, Ordering::SeqCst))
    }
}

#[derive(Default)]
struct TestTelemetry {
    diagnostics: Mutex<Vec<DiagnosticRecord>>,
    metrics: Mutex<Vec<MetricObservation>>,
}

impl TestTelemetry {
    fn has_event(&self, event: DiagnosticEventName) -> bool {
        self.diagnostics
            .lock()
            .expect("diagnostic lock")
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

fn request(
    filter: SessionEventFilter,
    start: SessionEventStart,
    limit: usize,
) -> SessionEventReadRequest {
    SessionEventReadRequest::new(filter, start, limit, 64).expect("request is valid")
}

fn valid_batch(
    records: Vec<SessionOutboxRecord>,
    checkpoint: Option<OutboxSequence>,
) -> SessionEventReadBatch {
    SessionEventReadBatch::new(
        records,
        checkpoint,
        None,
        false,
        SessionEventQueryCost::default(),
    )
}

fn record(value: u64, session: &str, kind: DomainEventKind) -> SessionOutboxRecord {
    SessionOutboxRecord {
        snapshot: None,
        sequence: sequence(value),
        event: SessionOutboxEvent {
            kind,
            session_id: session_id(session),
            revision: Revision::new(value),
            occurred_at: TimestampMillis::new(i64::try_from(value).expect("test value fits")),
        },
    }
}

fn session_id(value: &str) -> SessionId {
    SessionId::new(value).expect("session ID is valid")
}

fn sequence(value: u64) -> OutboxSequence {
    OutboxSequence::new(value).expect("sequence is nonzero")
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct NoopWake;
    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
