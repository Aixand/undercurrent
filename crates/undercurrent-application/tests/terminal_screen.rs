// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use undercurrent_application::*;
use undercurrent_domain::{SessionId, TimestampMillis};

#[test]
fn screen_snapshot_validates_geometry_and_redacts_debug_contents() {
    let snapshot = TerminalScreenSnapshot::new(
        OutputOffset::new(42),
        80,
        24,
        3,
        12,
        true,
        false,
        Arc::from(b"private-screen".as_slice()),
    )
    .unwrap();

    assert_eq!(snapshot.output_offset(), OutputOffset::new(42));
    assert_eq!((snapshot.columns(), snapshot.rows()), (80, 24));
    assert_eq!((snapshot.cursor_row(), snapshot.cursor_column()), (3, 12));
    assert_eq!(snapshot.contents(), b"private-screen");
    assert!(!format!("{snapshot:?}").contains("private-screen"));
    assert!(
        TerminalScreenSnapshot::new(OutputOffset::ZERO, 0, 24, 0, 0, true, false, Arc::from([]),)
            .is_err()
    );
    assert!(
        TerminalScreenSnapshot::new(
            OutputOffset::ZERO,
            80,
            24,
            24,
            0,
            true,
            false,
            Arc::from([]),
        )
        .is_err()
    );
}

#[test]
fn authorized_screen_read_returns_state_and_payload_safe_telemetry() {
    let session = session();
    let authorizer = TestAuthorizer::new(AccessDecision::Permit);
    let reader = TestReader::with_snapshot(snapshot());
    let clock = TestClock::default();
    let telemetry = TestTelemetry::default();
    let service = ReadTerminalScreenService::new(&authorizer, &reader, &clock, &telemetry);

    let result = block_on(service.read(&session)).unwrap();

    assert_eq!(result.contents(), b"private-screen");
    assert_eq!(reader.calls.load(Ordering::Acquire), 1);
    assert!(telemetry.has_event(DiagnosticEventName::TerminalScreenRead));
    assert!(telemetry.has_operation(ApplicationOperation::ReadTerminalScreen));
    let diagnostics = telemetry.diagnostics.lock().unwrap();
    assert!(!format!("{diagnostics:?}").contains("private-screen"));
}

#[test]
fn authorization_denial_prevents_screen_access() {
    let session = session();
    let authorizer = TestAuthorizer::new(AccessDecision::Deny(AccessDenialReason::Unauthenticated));
    let reader = TestReader::with_snapshot(snapshot());
    let clock = TestClock::default();
    let telemetry = TestTelemetry::default();
    let service = ReadTerminalScreenService::new(&authorizer, &reader, &clock, &telemetry);

    let error = block_on(service.read(&session)).unwrap_err();

    assert_eq!(error.category(), ApplicationErrorCategory::Unauthenticated);
    assert_eq!(reader.calls.load(Ordering::Acquire), 0);
    assert!(telemetry.has_event(DiagnosticEventName::AuthorizationDenied));
}

#[test]
fn absent_screen_is_a_typed_not_found_result() {
    let session = session();
    let authorizer = TestAuthorizer::new(AccessDecision::Permit);
    let reader = TestReader::without_snapshot();
    let clock = TestClock::default();
    let telemetry = TestTelemetry::default();
    let service = ReadTerminalScreenService::new(&authorizer, &reader, &clock, &telemetry);

    let error = block_on(service.read(&session)).unwrap_err();

    assert_eq!(error.category(), ApplicationErrorCategory::NotFound);
    assert_eq!(error.session_id(), Some(&session));
    assert!(telemetry.has_event(DiagnosticEventName::TerminalScreenReadFailed));
}

#[test]
fn opaque_screen_is_a_typed_failed_precondition_after_authorization() {
    let session = session();
    let authorizer = TestAuthorizer::new(AccessDecision::Permit);
    let reader = TestReader::unsupported();
    let clock = TestClock::default();
    let telemetry = TestTelemetry::default();
    let service = ReadTerminalScreenService::new(&authorizer, &reader, &clock, &telemetry);

    let error = block_on(service.read(&session)).unwrap_err();

    assert!(matches!(
        error,
        ReadTerminalScreenError::UnavailableForCaptureMode { ref session_id }
            if session_id == &session
    ));
    assert_eq!(
        error.category(),
        ApplicationErrorCategory::FailedPrecondition
    );
    assert_eq!(reader.calls.load(Ordering::Acquire), 1);
    assert!(telemetry.has_event(DiagnosticEventName::TerminalScreenReadFailed));
}

#[test]
fn authorized_diff_returns_a_bounded_update_and_payload_safe_telemetry() {
    let session = session();
    let authorizer = TestAuthorizer::new(AccessDecision::Permit);
    let reader = TestReader::with_snapshot(snapshot());
    let clock = TestClock::default();
    let telemetry = TestTelemetry::default();
    let service = ReadTerminalScreenService::new(&authorizer, &reader, &clock, &telemetry);
    let previous = TerminalScreenSnapshot::new(
        OutputOffset::new(40),
        80,
        24,
        3,
        10,
        true,
        false,
        Arc::from(b"prior-private-screen".as_slice()),
    )
    .unwrap();

    let update = block_on(service.diff(&session, &previous)).unwrap();

    let TerminalScreenUpdate::Diff(diff) = update else {
        panic!("test differ did not return a diff")
    };
    assert_eq!(diff.from_output_offset(), OutputOffset::new(40));
    assert_eq!(diff.current().output_offset(), OutputOffset::new(42));
    assert_eq!(diff.contents(), b"private-diff");
    assert_eq!(reader.diff_calls.load(Ordering::Acquire), 1);
    assert!(telemetry.has_event(DiagnosticEventName::TerminalScreenDiffRead));
    assert!(telemetry.has_operation(ApplicationOperation::ReadTerminalScreenDiff));
    assert!(!format!("{:?}", telemetry.diagnostics.lock().unwrap()).contains("private-diff"));
}

#[test]
fn screen_diff_rejects_reversed_offsets_and_redacts_private_contents() {
    let error = TerminalScreenDiff::new(
        OutputOffset::new(43),
        snapshot(),
        Arc::from(b"private-diff".as_slice()),
    )
    .unwrap_err();
    assert_eq!(error, TerminalScreenDiffError::OffsetReversed);

    let diff = TerminalScreenDiff::new(
        OutputOffset::new(41),
        snapshot(),
        Arc::from(b"private-diff".as_slice()),
    )
    .unwrap();
    assert!(!format!("{diff:?}").contains("private-diff"));
}

fn snapshot() -> TerminalScreenSnapshot {
    TerminalScreenSnapshot::new(
        OutputOffset::new(42),
        80,
        24,
        3,
        12,
        true,
        false,
        Arc::from(b"private-screen".as_slice()),
    )
    .unwrap()
}

fn session() -> SessionId {
    SessionId::new("screen-session").unwrap()
}

struct TestAuthorizer {
    decision: AccessDecision,
}

impl TestAuthorizer {
    fn new(decision: AccessDecision) -> Self {
        Self { decision }
    }
}

impl AuthorizationPort for TestAuthorizer {
    fn authorize<'a>(
        &'a self,
        action: AccessAction,
        _resource: &'a AccessResource,
    ) -> PortFuture<'a, Result<AccessDecision, PortError>> {
        assert_eq!(action, AccessAction::ReadScreen);
        Box::pin(async move { Ok(self.decision) })
    }
}

struct TestReader {
    snapshot: Option<TerminalScreenSnapshot>,
    unsupported: bool,
    calls: AtomicUsize,
    diff_calls: AtomicUsize,
}

impl TestReader {
    fn with_snapshot(snapshot: TerminalScreenSnapshot) -> Self {
        Self {
            snapshot: Some(snapshot),
            unsupported: false,
            calls: AtomicUsize::new(0),
            diff_calls: AtomicUsize::new(0),
        }
    }

    fn without_snapshot() -> Self {
        Self {
            snapshot: None,
            unsupported: false,
            calls: AtomicUsize::new(0),
            diff_calls: AtomicUsize::new(0),
        }
    }

    fn unsupported() -> Self {
        Self {
            snapshot: None,
            unsupported: true,
            calls: AtomicUsize::new(0),
            diff_calls: AtomicUsize::new(0),
        }
    }
}

impl TerminalScreenDiffer for TestReader {
    fn diff<'a>(
        &'a self,
        _session_id: &'a SessionId,
        previous: &'a TerminalScreenSnapshot,
        current: &'a TerminalScreenSnapshot,
    ) -> PortFuture<'a, Result<TerminalScreenUpdate, PortError>> {
        self.diff_calls.fetch_add(1, Ordering::AcqRel);
        Box::pin(async move {
            TerminalScreenDiff::new(
                previous.output_offset(),
                current.clone(),
                Arc::from(b"private-diff".as_slice()),
            )
            .map(TerminalScreenUpdate::Diff)
            .map_err(|source| {
                PortError::new(
                    PortErrorKind::InvalidData,
                    PortOperation::DiffTerminalScreen,
                    None,
                    source,
                )
            })
        })
    }
}

impl TerminalScreenReader for TestReader {
    fn snapshot<'a>(
        &'a self,
        _session_id: &'a SessionId,
    ) -> PortFuture<'a, Result<Option<TerminalScreenSnapshot>, PortError>> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Box::pin(async move {
            if self.unsupported {
                Err(PortError::new(
                    PortErrorKind::Unsupported,
                    PortOperation::ReadTerminalScreen,
                    None,
                    std::io::Error::other("opaque capture has no screen"),
                ))
            } else {
                Ok(self.snapshot.clone())
            }
        })
    }
}

#[derive(Default)]
struct TestClock(AtomicI64);

impl Clock for TestClock {
    fn now(&self) -> TimestampMillis {
        TimestampMillis::new(self.0.fetch_add(1, Ordering::AcqRel))
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
            .unwrap()
            .iter()
            .any(|record| record.event == event)
    }

    fn has_operation(&self, operation: ApplicationOperation) -> bool {
        self.metrics
            .lock()
            .unwrap()
            .iter()
            .any(|metric| metric.operation == Some(operation))
    }
}

impl Telemetry for TestTelemetry {
    fn emit_diagnostic(&self, record: &DiagnosticRecord) {
        self.diagnostics.lock().unwrap().push(record.clone());
    }

    fn record_metric(&self, observation: MetricObservation) {
        self.metrics.lock().unwrap().push(observation);
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
