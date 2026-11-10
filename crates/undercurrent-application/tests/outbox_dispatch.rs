// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};
use std::time::Duration;

use undercurrent_application::*;
use undercurrent_domain::*;

#[test]
fn ordered_dispatch_publishes_activates_and_acknowledges_exact_prefix() {
    let session = running_session(true);
    let records = vec![
        record(1, &session, DomainEventKind::LifecycleChanged, 1),
        record(2, &session, DomainEventKind::HealthChanged, 2),
        record(3, &session, DomainEventKind::SessionRemoved, 3),
    ];
    let outbox = TestOutbox::new(records.clone());
    let repository = TestRepository::new(Some(session.clone()));
    let publisher = TestPublisher::new(Some(records[1].clone()), None);
    let observations = TestObservations::default();
    let telemetry = TestTelemetry::default();
    let clock = FixedClock(TimestampMillis::new(5_000));
    let service = DispatchOutboxService::new(
        &clock,
        &outbox,
        &repository,
        &publisher,
        &observations,
        &telemetry,
        limits(8),
    );

    let report = block_on(service.execute()).expect("outbox dispatch succeeds");

    assert_eq!(report.read, 3);
    assert_eq!(report.published, 2);
    assert_eq!(report.deduplicated, 1);
    assert_eq!(report.processed, 3);
    assert_eq!(report.observation_plans, 1);
    assert_eq!(report.observation_plans_skipped, 1);
    assert_eq!(report.observation_cancellations, 1);
    assert_eq!(report.acknowledged, 3);
    assert_eq!(report.last_acknowledged, Some(sequence(3)));
    assert_eq!(outbox.pending(), 0);
    assert_eq!(repository.loads(), 1);
    assert_eq!(observations.cancelled(), vec![session.id().clone()]);

    let plans = observations.plans();
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].session_id, *session.id());
    assert_eq!(plans[0].expected_revision, session.revision());
    assert_eq!(
        plans[0].foreground_not_before,
        Some(TimestampMillis::new(1_100))
    );
    assert_eq!(
        plans[0].health_not_before,
        Some(TimestampMillis::new(2_100))
    );
    assert!(telemetry.has_event(DiagnosticEventName::OutboxDispatchCompleted));
    assert_eq!(telemetry.metric_count(), 4);
}

#[test]
fn record_failure_acknowledges_only_completed_prefix_and_redacts_source() {
    let session = running_session(false);
    let records = vec![
        record(1, &session, DomainEventKind::LifecycleChanged, 1),
        record(2, &session, DomainEventKind::ForegroundChanged, 2),
        record(3, &session, DomainEventKind::TerminalResized, 3),
    ];
    let outbox = TestOutbox::new(records);
    let repository = TestRepository::new(Some(session));
    let publisher = TestPublisher::new(None, Some(sequence(2)));
    let observations = TestObservations::default();
    let telemetry = TestTelemetry::default();
    let clock = FixedClock(TimestampMillis::new(5_000));
    let service = DispatchOutboxService::new(
        &clock,
        &outbox,
        &repository,
        &publisher,
        &observations,
        &telemetry,
        limits(8),
    );

    let error = block_on(service.execute()).expect_err("second publication fails");

    assert_eq!(error.category(), ApplicationErrorCategory::Unavailable);
    assert!(!error.to_string().contains("private outbox failure"));
    assert!(!format!("{error:?}").contains("private outbox failure"));
    assert!(matches!(
        error,
        OutboxDispatchError::Record {
            sequence: failed,
            stage: OutboxRecordStage::PublishEvent,
            acknowledgement_failure: None,
            ref partial,
            ..
        } if failed == sequence(2)
            && partial.processed == 1
            && partial.acknowledged == 1
            && partial.last_acknowledged == Some(sequence(1))
    ));
    assert_eq!(outbox.pending_sequences(), vec![sequence(2), sequence(3)]);
    assert_eq!(publisher.calls(), 2);
    assert!(telemetry.has_event(DiagnosticEventName::OutboxDispatchFailed));
    let diagnostic = telemetry
        .diagnostics
        .lock()
        .expect("diagnostic lock is available")
        .iter()
        .find(|record| record.event == DiagnosticEventName::OutboxDispatchFailed)
        .cloned()
        .expect("failure diagnostic is emitted");
    assert_eq!(diagnostic.level, DiagnosticLevel::Warn);
    assert_eq!(
        diagnostic.application_error,
        Some(ApplicationErrorCategory::Unavailable)
    );
}

#[test]
fn outbox_dispatch_is_page_bounded_with_three_hundred_pending_records() {
    let session = running_session(false);
    let records = (1..=300).map(|value| {
        record(
            value,
            &session,
            DomainEventKind::ForegroundEvidenceChanged,
            1,
        )
    });
    let outbox = TestOutbox::new(records.collect());
    let repository = TestRepository::new(Some(session));
    let publisher = TestPublisher::new(None, None);
    let observations = TestObservations::default();
    let telemetry = TestTelemetry::default();
    let clock = FixedClock(TimestampMillis::new(5_000));
    let service = DispatchOutboxService::new(
        &clock,
        &outbox,
        &repository,
        &publisher,
        &observations,
        &telemetry,
        limits(37),
    );

    let report = block_on(service.execute()).expect("bounded page dispatches");

    assert_eq!(report.read, 37);
    assert_eq!(report.processed, 37);
    assert_eq!(report.acknowledged, 37);
    assert_eq!(report.observation_plans, 0);
    assert_eq!(report.observation_plans_skipped, 37);
    assert_eq!(publisher.calls(), 37);
    assert_eq!(repository.loads(), 0);
    assert!(observations.plans().is_empty());
    assert_eq!(outbox.pending(), 263);
}

#[test]
fn invalid_batch_stops_before_publication_or_acknowledgement() {
    let session = running_session(false);
    let outbox = TestOutbox::new(vec![
        record(1, &session, DomainEventKind::LifecycleChanged, 1),
        record(3, &session, DomainEventKind::ForegroundChanged, 2),
    ]);
    let repository = TestRepository::new(Some(session));
    let publisher = TestPublisher::new(None, None);
    let observations = TestObservations::default();
    let telemetry = TestTelemetry::default();
    let clock = FixedClock(TimestampMillis::new(5_000));
    let service = DispatchOutboxService::new(
        &clock,
        &outbox,
        &repository,
        &publisher,
        &observations,
        &telemetry,
        limits(8),
    );

    let error = block_on(service.execute()).expect_err("sequence gap is rejected");

    assert!(matches!(
        error,
        OutboxDispatchError::InvalidBatch(OutboxBatchViolation::SequenceGap)
    ));
    assert_eq!(publisher.calls(), 0);
    assert_eq!(outbox.pending(), 2);
    assert_eq!(repository.loads(), 0);
}

#[test]
fn repository_revision_behind_event_is_an_invariant_and_remains_pending() {
    let session = running_session(false);
    let outbox = TestOutbox::new(vec![record(
        1,
        &session,
        DomainEventKind::LifecycleChanged,
        session.revision().value() + 1,
    )]);
    let repository = TestRepository::new(Some(session));
    let publisher = TestPublisher::new(None, None);
    let observations = TestObservations::default();
    let telemetry = TestTelemetry::default();
    let clock = FixedClock(TimestampMillis::new(5_000));
    let service = DispatchOutboxService::new(
        &clock,
        &outbox,
        &repository,
        &publisher,
        &observations,
        &telemetry,
        limits(1),
    );

    let error = block_on(service.execute()).expect_err("repository cannot trail its outbox");

    assert_eq!(
        error.category(),
        ApplicationErrorCategory::InvariantViolation
    );
    assert!(matches!(
        error,
        OutboxDispatchError::Record {
            stage: OutboxRecordStage::ValidateSessionRevision,
            failure: OutboxRecordFailure::RepositoryRevisionBehind { .. },
            ..
        }
    ));
    assert_eq!(outbox.pending(), 1);
    assert!(observations.plans().is_empty());
}

#[test]
fn invalid_acknowledgement_retains_processed_progress_without_deleting_source() {
    let session = running_session(false);
    let outbox = TestOutbox::new(vec![record(
        1,
        &session,
        DomainEventKind::LifecycleChanged,
        1,
    )])
    .with_invalid_acknowledgement();
    let repository = TestRepository::new(Some(session));
    let publisher = TestPublisher::new(None, None);
    let observations = TestObservations::default();
    let telemetry = TestTelemetry::default();
    let clock = FixedClock(TimestampMillis::new(5_000));
    let service = DispatchOutboxService::new(
        &clock,
        &outbox,
        &repository,
        &publisher,
        &observations,
        &telemetry,
        limits(1),
    );

    let error = block_on(service.execute()).expect_err("invalid acknowledgement is rejected");

    assert!(matches!(
        error,
        OutboxDispatchError::Acknowledge {
            through,
            failure: OutboxAcknowledgementFailure::InvalidOutcome(_),
            ref partial,
        } if through == sequence(1)
            && partial.processed == 1
            && partial.acknowledged == 0
    ));
    assert_eq!(outbox.pending(), 1);
}

#[test]
fn outbox_limits_reject_zero_batch_and_cadence() {
    let stages = OutboxRecordStage::ALL
        .iter()
        .copied()
        .map(OutboxRecordStage::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(stages.len(), OutboxRecordStage::ALL.len());

    let valid = Duration::from_secs(1);
    for (result, field) in [
        (OutboxDispatchLimits::new(0, valid, valid), "batch_size"),
        (
            OutboxDispatchLimits::new(1, Duration::ZERO, valid),
            "foreground_delay",
        ),
        (
            OutboxDispatchLimits::new(1, valid, Duration::ZERO),
            "health_delay",
        ),
    ] {
        assert_eq!(result.expect_err("zero field is invalid").field(), field);
    }
}

fn limits(batch_size: usize) -> OutboxDispatchLimits {
    OutboxDispatchLimits::new(batch_size, Duration::from_secs(1), Duration::from_secs(2))
        .expect("outbox limits are valid")
}

fn record(
    sequence_value: u64,
    session: &Session,
    kind: DomainEventKind,
    revision: u64,
) -> SessionOutboxRecord {
    SessionOutboxRecord {
        snapshot: None,
        sequence: sequence(sequence_value),
        event: SessionOutboxEvent {
            kind,
            session_id: session.id().clone(),
            revision: Revision::new(revision),
            occurred_at: TimestampMillis::new(
                i64::try_from(sequence_value).unwrap_or(i64::MAX) * 1_000 - 900,
            ),
        },
    }
}

fn sequence(value: u64) -> OutboxSequence {
    OutboxSequence::new(value).expect("outbox sequence is valid")
}

fn running_session(with_health: bool) -> Session {
    let execution_limits = ExecutionLimits {
        max_arguments: 4,
        max_command_bytes: 128,
        max_environment_entries: 4,
        max_environment_bytes: 128,
        max_path_bytes: 128,
        max_health_rules: 4,
        max_health_rule_bytes: 128,
    };
    let health = if with_health {
        HealthRules::new(
            [HealthRule::tcp_port(80).expect("health rule is valid")],
            execution_limits,
        )
        .expect("health rules are valid")
    } else {
        HealthRules::default()
    };
    let mut session = Session::create(
        SessionId::new("session-1").expect("session ID is valid"),
        SessionName::new("worker").expect("session name is valid"),
        SessionConfiguration::new(
            LaunchSpec::new(
                CommandSpec::new("sh", Vec::new(), execution_limits).expect("command is valid"),
                EnvironmentVariables::default(),
                None,
                None,
                TerminalSize::new(80, 24, 500, 500).expect("terminal size is valid"),
            ),
            ExecutionPolicy::new(
                RestartConfig::never(),
                StopPolicy::new(StopSignal::Terminate, Duration::from_secs(1))
                    .expect("stop policy is valid"),
                true,
                false,
            ),
            health,
            None,
            Labels::default(),
            Metadata::default(),
        ),
        4,
        TimestampMillis::new(1),
    )
    .expect("session is valid")
    .0;
    session
        .record_process_ownership(
            ProcessIdentity::new(
                ProcessInstanceId::new("process-1").expect("process ID is valid"),
                TimestampMillis::new(2),
            ),
            TimestampMillis::new(2),
        )
        .expect("process ownership succeeds");
    session
        .transition_lifecycle(LifecycleState::Running, TimestampMillis::new(3))
        .expect("running transition succeeds");
    session
}

struct FixedClock(TimestampMillis);

impl Clock for FixedClock {
    fn now(&self) -> TimestampMillis {
        self.0
    }
}

struct TestOutbox {
    records: Mutex<Vec<SessionOutboxRecord>>,
    invalid_acknowledgement: bool,
}

impl TestOutbox {
    fn new(records: Vec<SessionOutboxRecord>) -> Self {
        Self {
            records: Mutex::new(records),
            invalid_acknowledgement: false,
        }
    }

    fn with_invalid_acknowledgement(mut self) -> Self {
        self.invalid_acknowledgement = true;
        self
    }

    fn pending(&self) -> usize {
        self.records.lock().expect("outbox lock is available").len()
    }

    fn pending_sequences(&self) -> Vec<OutboxSequence> {
        self.records
            .lock()
            .expect("outbox lock is available")
            .iter()
            .map(|record| record.sequence)
            .collect()
    }
}

impl SessionOutbox for TestOutbox {
    fn read_pending<'a>(
        &'a self,
        limit: usize,
    ) -> PortFuture<'a, Result<Vec<SessionOutboxRecord>, PortError>> {
        Box::pin(async move {
            Ok(self
                .records
                .lock()
                .expect("outbox lock is available")
                .iter()
                .take(limit)
                .cloned()
                .collect())
        })
    }

    fn acknowledge_through<'a>(
        &'a self,
        through: OutboxSequence,
    ) -> PortFuture<'a, Result<OutboxAcknowledgeOutcome, PortError>> {
        Box::pin(async move {
            if self.invalid_acknowledgement {
                return Ok(OutboxAcknowledgeOutcome::UnknownSequence {
                    requested: sequence(999),
                });
            }
            let mut records = self.records.lock().expect("outbox lock is available");
            let Some(position) = records.iter().position(|record| record.sequence == through)
            else {
                return Ok(OutboxAcknowledgeOutcome::UnknownSequence { requested: through });
            };
            let acknowledged = position + 1;
            records.drain(..acknowledged);
            Ok(OutboxAcknowledgeOutcome::Acknowledged {
                records: acknowledged,
                through,
            })
        })
    }
}

struct TestRepository {
    session: Option<Session>,
    loads: AtomicUsize,
}

impl TestRepository {
    fn new(session: Option<Session>) -> Self {
        Self {
            session,
            loads: AtomicUsize::new(0),
        }
    }

    fn loads(&self) -> usize {
        self.loads.load(Ordering::SeqCst)
    }
}

impl SessionRepository for TestRepository {
    fn insert<'a>(
        &'a self,
        _: &'a Session,
        _: &'a SessionOutboxEvent,
    ) -> PortFuture<'a, Result<InsertSessionOutcome, PortError>> {
        Box::pin(async { Err(port_error(PortOperation::InsertSession)) })
    }

    fn load<'a>(&'a self, _: &'a SessionId) -> PortFuture<'a, Result<Option<Session>, PortError>> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(self.session.clone()) })
    }

    fn find_by_name<'a>(
        &'a self,
        _: &'a SessionName,
    ) -> PortFuture<'a, Result<Option<Session>, PortError>> {
        Box::pin(async { Err(port_error(PortOperation::FindSessionByName)) })
    }

    fn commit<'a>(
        &'a self,
        _: &'a Session,
        _: Revision,
        _: &'a [SessionOutboxEvent],
    ) -> PortFuture<'a, Result<CommitSessionOutcome, PortError>> {
        Box::pin(async { Err(port_error(PortOperation::CommitSession)) })
    }

    fn delete<'a>(
        &'a self,
        _: &'a SessionId,
        _: Revision,
        _: &'a SessionOutboxEvent,
    ) -> PortFuture<'a, Result<DeleteSessionOutcome, PortError>> {
        Box::pin(async { Err(port_error(PortOperation::DeleteSession)) })
    }
}

struct TestPublisher {
    published: Mutex<BTreeMap<OutboxSequence, SessionOutboxRecord>>,
    fail_at: Option<OutboxSequence>,
    calls: AtomicUsize,
}

impl TestPublisher {
    fn new(already: Option<SessionOutboxRecord>, fail_at: Option<OutboxSequence>) -> Self {
        let mut published = BTreeMap::new();
        if let Some(record) = already {
            published.insert(record.sequence, record);
        }
        Self {
            published: Mutex::new(published),
            fail_at,
            calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl SessionEventPublisher for TestPublisher {
    fn publish<'a>(
        &'a self,
        record: &'a SessionOutboxRecord,
    ) -> PortFuture<'a, Result<SessionEventPublishOutcome, PortError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if self.fail_at == Some(record.sequence) {
                return Err(port_error(PortOperation::PublishSessionEvent));
            }
            let mut published = self.published.lock().expect("publisher lock is available");
            if let Some(existing) = published.get(&record.sequence) {
                if existing == record {
                    return Ok(SessionEventPublishOutcome::AlreadyPublished);
                }
                return Err(port_error(PortOperation::PublishSessionEvent));
            }
            published.insert(record.sequence, record.clone());
            Ok(SessionEventPublishOutcome::Published)
        })
    }
}

#[derive(Default)]
struct TestObservations {
    plans: Mutex<Vec<SessionObservationPlan>>,
    cancelled: Mutex<Vec<SessionId>>,
}

impl TestObservations {
    fn plans(&self) -> Vec<SessionObservationPlan> {
        self.plans.lock().expect("plan lock is available").clone()
    }

    fn cancelled(&self) -> Vec<SessionId> {
        self.cancelled
            .lock()
            .expect("cancel lock is available")
            .clone()
    }
}

impl ObservationScheduler for TestObservations {
    fn schedule_observation<'a>(
        &'a self,
        _: &'a ObservationJob,
    ) -> PortFuture<'a, Result<ObservationScheduleOutcome, PortError>> {
        Box::pin(async { Ok(ObservationScheduleOutcome::Scheduled) })
    }

    fn cancel_session_observations<'a>(
        &'a self,
        session_id: &'a SessionId,
    ) -> PortFuture<'a, Result<usize, PortError>> {
        Box::pin(async move {
            self.cancelled
                .lock()
                .expect("cancel lock is available")
                .push(session_id.clone());
            Ok(0)
        })
    }

    fn reconcile_session_observations<'a>(
        &'a self,
        plan: &'a SessionObservationPlan,
    ) -> PortFuture<'a, Result<ObservationReconcileOutcome, PortError>> {
        Box::pin(async move {
            self.plans
                .lock()
                .expect("plan lock is available")
                .push(plan.clone());
            Ok(ObservationReconcileOutcome::default())
        })
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
            .expect("diagnostic lock is available")
            .iter()
            .any(|record| record.event == event)
    }

    fn metric_count(&self) -> usize {
        self.metrics.lock().expect("metric lock is available").len()
    }
}

impl Telemetry for TestTelemetry {
    fn emit_diagnostic(&self, record: &DiagnosticRecord) {
        self.diagnostics
            .lock()
            .expect("diagnostic lock is available")
            .push(record.clone());
    }

    fn record_metric(&self, observation: MetricObservation) {
        self.metrics
            .lock()
            .expect("metric lock is available")
            .push(observation);
    }

    fn record_session_metric(&self, observation: &SessionMetricObservation) {
        self.record_metric(observation.metric());
    }
}

#[derive(Debug)]
struct PrivateFailure;

impl Display for PrivateFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("private outbox failure")
    }
}

impl Error for PrivateFailure {}

fn port_error(operation: PortOperation) -> PortError {
    PortError::new(PortErrorKind::Unavailable, operation, None, PrivateFailure)
}

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let parker = ThreadWake(thread::current());
    let waker = Waker::from(std::sync::Arc::new(parker));
    let mut context = Context::from_waker(&waker);
    loop {
        match Pin::as_mut(&mut future).poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}

struct ThreadWake(Thread);

impl Wake for ThreadWake {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        self.0.unpark();
    }
}
