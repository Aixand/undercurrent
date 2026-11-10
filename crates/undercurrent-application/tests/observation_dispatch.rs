// SPDX-License-Identifier: Apache-2.0

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};
use std::time::Duration;

use undercurrent_application::*;
use undercurrent_domain::{DomainEventKind, Revision, SessionId, TimestampMillis};

#[test]
fn dispatch_isolates_jobs_reschedules_by_outcome_and_bounds_diagnostics() {
    let source = TestObservationQueue::new([
        job("commit", ObservationJobKind::Foreground),
        job("unchanged", ObservationJobKind::Health),
        job("stale", ObservationJobKind::Foreground),
        job("disabled", ObservationJobKind::Foreground),
        job("missing", ObservationJobKind::Health),
        job("failure", ObservationJobKind::Foreground),
        job("gone", ObservationJobKind::Health),
        job("schedule-fail", ObservationJobKind::Foreground),
        job("superseded", ObservationJobKind::Health),
    ]);
    let runner = ScriptedRunner::default();
    let telemetry = TestTelemetry::default();
    let clock = FixedClock(TimestampMillis::new(1_000));
    let service = DispatchObservationsService::new(
        &clock,
        &source,
        &source,
        &runner,
        &telemetry,
        limits(32, 2),
    );

    let report = block_on(service.execute()).expect("due batch dispatches");

    assert_eq!(report.claimed, 9);
    assert_eq!(report.committed, 1);
    assert_eq!(report.unchanged, 3);
    assert_eq!(report.stale, 1);
    assert_eq!(report.skipped, 2);
    assert_eq!(report.failed, 1);
    assert_eq!(report.rescheduled, 5);
    assert_eq!(report.deactivated, 2);
    assert_eq!(report.superseded, 1);
    assert_eq!(report.issue_count, 2);
    assert_eq!(report.issues.len(), 2);
    assert_eq!(runner.calls.load(Ordering::SeqCst), 9);
    assert_eq!(source.cancelled(), vec![session_id("gone")]);

    let scheduled = source.scheduled();
    assert_eq!(scheduled.len(), 5);
    assert_job(&scheduled, "commit", Revision::new(2), 11_000);
    assert_job(&scheduled, "unchanged", Revision::new(1), 21_000);
    assert_job(&scheduled, "stale", Revision::new(3), 6_000);
    assert_job(&scheduled, "missing", Revision::new(1), 6_000);
    assert_job(&scheduled, "failure", Revision::new(1), 6_000);
    assert_eq!(telemetry.metric_count(), 9);
    assert!(telemetry.has_event(DiagnosticEventName::ObservationJobFailed));
    assert!(telemetry.has_event(DiagnosticEventName::ObservationJobDeactivated));
    assert!(telemetry.has_event(DiagnosticEventName::ObservationRescheduleFailed));
    assert!(telemetry.has_event(DiagnosticEventName::ObservationDispatchCompleted));
}

#[test]
fn dispatch_claim_is_page_bounded_at_three_hundred_pending_sessions() {
    let source = TestObservationQueue::new((0..300).map(|index| {
        job(
            &format!("session-{index:03}"),
            ObservationJobKind::Foreground,
        )
    }));
    let runner = AlwaysUnchangedRunner::default();
    let telemetry = TestTelemetry::default();
    let clock = FixedClock(TimestampMillis::new(1_000));
    let service = DispatchObservationsService::new(
        &clock,
        &source,
        &source,
        &runner,
        &telemetry,
        limits(37, 0),
    );

    let report = block_on(service.execute()).expect("bounded batch dispatches");

    assert_eq!(report.claimed, 37);
    assert_eq!(report.unchanged, 37);
    assert_eq!(report.rescheduled, 37);
    assert_eq!(runner.calls.load(Ordering::SeqCst), 37);
    assert_eq!(source.pending(), 263);
    assert_eq!(source.scheduled().len(), 37);
}

#[test]
fn claim_failure_is_fatal_redacted_and_observable() {
    let source = TestObservationQueue::new([]);
    source.fail_claim.store(true, Ordering::SeqCst);
    let runner = AlwaysUnchangedRunner::default();
    let telemetry = TestTelemetry::default();
    let clock = FixedClock(TimestampMillis::new(1_000));
    let service = DispatchObservationsService::new(
        &clock,
        &source,
        &source,
        &runner,
        &telemetry,
        limits(1, 1),
    );

    let error = block_on(service.execute()).expect_err("claim failure is fatal");

    assert_eq!(error.category(), ApplicationErrorCategory::Unavailable);
    assert!(!error.to_string().contains("private queue failure"));
    assert!(telemetry.has_event(DiagnosticEventName::ObservationDispatchFailed));
    let diagnostic = telemetry
        .diagnostics
        .lock()
        .expect("diagnostic lock is available")
        .iter()
        .find(|record| record.event == DiagnosticEventName::ObservationDispatchFailed)
        .cloned()
        .expect("failure diagnostic is emitted");
    assert_eq!(diagnostic.level, DiagnosticLevel::Warn);
    assert_eq!(
        diagnostic.application_error,
        Some(ApplicationErrorCategory::Unavailable)
    );
    assert_eq!(runner.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn dispatch_limits_reject_zero_batch_and_intervals() {
    let valid = Duration::from_secs(1);
    for (result, field) in [
        (
            ObservationDispatchLimits::new(0, 0, valid, valid, valid),
            "batch_size",
        ),
        (
            ObservationDispatchLimits::new(1, 0, Duration::ZERO, valid, valid),
            "foreground_interval",
        ),
        (
            ObservationDispatchLimits::new(1, 0, valid, Duration::ZERO, valid),
            "health_interval",
        ),
        (
            ObservationDispatchLimits::new(1, 0, valid, valid, Duration::ZERO),
            "retry_interval",
        ),
    ] {
        assert_eq!(result.expect_err("zero field is invalid").field(), field);
    }
}

fn limits(batch_size: usize, max_retained_issues: usize) -> ObservationDispatchLimits {
    ObservationDispatchLimits::new(
        batch_size,
        max_retained_issues,
        Duration::from_secs(10),
        Duration::from_secs(20),
        Duration::from_secs(5),
    )
    .expect("dispatch limits are valid")
}

fn job(id: &str, kind: ObservationJobKind) -> ObservationJob {
    ObservationJob {
        session_id: session_id(id),
        expected_revision: Revision::new(1),
        kind,
        not_before: TimestampMillis::new(500),
    }
}

fn session_id(value: &str) -> SessionId {
    SessionId::new(value).expect("session ID is valid")
}

fn assert_job(jobs: &[ObservationJob], id: &str, revision: Revision, not_before: i64) {
    let job = jobs
        .iter()
        .find(|job| job.session_id.as_str() == id)
        .expect("scheduled job exists");
    assert_eq!(job.expected_revision, revision);
    assert_eq!(job.not_before, TimestampMillis::new(not_before));
}

struct FixedClock(TimestampMillis);

impl Clock for FixedClock {
    fn now(&self) -> TimestampMillis {
        self.0
    }
}

struct TestObservationQueue {
    due: Mutex<Vec<ObservationJob>>,
    scheduled: Mutex<Vec<ObservationJob>>,
    cancelled: Mutex<Vec<SessionId>>,
    fail_claim: AtomicBool,
}

impl TestObservationQueue {
    fn new(jobs: impl IntoIterator<Item = ObservationJob>) -> Self {
        Self {
            due: Mutex::new(jobs.into_iter().collect()),
            scheduled: Mutex::new(Vec::new()),
            cancelled: Mutex::new(Vec::new()),
            fail_claim: AtomicBool::new(false),
        }
    }

    fn scheduled(&self) -> Vec<ObservationJob> {
        self.scheduled
            .lock()
            .expect("scheduled lock is available")
            .clone()
    }

    fn cancelled(&self) -> Vec<SessionId> {
        self.cancelled
            .lock()
            .expect("cancelled lock is available")
            .clone()
    }

    fn pending(&self) -> usize {
        self.due.lock().expect("due lock is available").len()
    }
}

impl DueObservationSource for TestObservationQueue {
    fn claim_due_observations(
        &self,
        _: TimestampMillis,
        limit: usize,
    ) -> Result<Vec<ObservationJob>, PortError> {
        if self.fail_claim.load(Ordering::SeqCst) {
            return Err(port_error(PortOperation::ClaimDueObservations));
        }
        let mut due = self.due.lock().expect("due lock is available");
        let count = limit.min(due.len());
        Ok(due.drain(..count).collect())
    }
}

impl ObservationScheduler for TestObservationQueue {
    fn schedule_observation<'a>(
        &'a self,
        job: &'a ObservationJob,
    ) -> PortFuture<'a, Result<ObservationScheduleOutcome, PortError>> {
        Box::pin(async move {
            match job.session_id.as_str() {
                "schedule-fail" => Err(port_error(PortOperation::ScheduleObservation)),
                "superseded" => Ok(ObservationScheduleOutcome::EarlierDeadlineRetained),
                _ => {
                    self.scheduled
                        .lock()
                        .expect("scheduled lock is available")
                        .push(job.clone());
                    Ok(ObservationScheduleOutcome::Scheduled)
                }
            }
        })
    }

    fn cancel_session_observations<'a>(
        &'a self,
        session_id: &'a SessionId,
    ) -> PortFuture<'a, Result<usize, PortError>> {
        Box::pin(async move {
            self.cancelled
                .lock()
                .expect("cancelled lock is available")
                .push(session_id.clone());
            Ok(1)
        })
    }

    fn reconcile_session_observations<'a>(
        &'a self,
        _: &'a SessionObservationPlan,
    ) -> PortFuture<'a, Result<ObservationReconcileOutcome, PortError>> {
        Box::pin(async { Ok(ObservationReconcileOutcome::default()) })
    }
}

#[derive(Default)]
struct ScriptedRunner {
    calls: AtomicUsize,
}

impl ScheduledObservationRunner for ScriptedRunner {
    fn run<'a>(
        &'a self,
        job: &'a ObservationJob,
    ) -> PortFuture<'a, Result<ObservationOutcome, ObservationError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            match job.session_id.as_str() {
                "commit" => Ok(ObservationOutcome::Committed {
                    revision: Revision::new(2),
                    event: DomainEventKind::ForegroundChanged,
                }),
                "unchanged" | "schedule-fail" | "superseded" => Ok(ObservationOutcome::Unchanged {
                    revision: Revision::new(1),
                }),
                "stale" => Ok(ObservationOutcome::Stale {
                    reason: StaleObservationReason::SessionChanged,
                    sampled_revision: Revision::new(1),
                    current_revision: Revision::new(3),
                }),
                "disabled" => Ok(ObservationOutcome::Skipped(
                    ObservationSkipReason::DetectionDisabled,
                )),
                "missing" => Ok(ObservationOutcome::Skipped(
                    ObservationSkipReason::MissingProcessBinding,
                )),
                "failure" => Err(ObservationError::ForegroundCollection(port_error(
                    PortOperation::CollectForegroundEvidence,
                ))),
                "gone" => Err(ObservationError::Mutation(SessionMutationError::NotFound {
                    session_id: job.session_id.clone(),
                })),
                _ => unreachable!("test job is scripted"),
            }
        })
    }
}

#[derive(Default)]
struct AlwaysUnchangedRunner {
    calls: AtomicUsize,
}

impl ScheduledObservationRunner for AlwaysUnchangedRunner {
    fn run<'a>(
        &'a self,
        job: &'a ObservationJob,
    ) -> PortFuture<'a, Result<ObservationOutcome, ObservationError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(ObservationOutcome::Unchanged {
                revision: job.expected_revision,
            })
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
        formatter.write_str("private queue failure")
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
