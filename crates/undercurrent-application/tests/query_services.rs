// SPDX-License-Identifier: Apache-2.0

use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use undercurrent_application::*;
use undercurrent_domain::*;

struct IncrementingClock(AtomicI64);

impl Clock for IncrementingClock {
    fn now(&self) -> TimestampMillis {
        TimestampMillis::new(self.0.fetch_add(10, Ordering::SeqCst))
    }
}

#[derive(Default)]
struct CapturingTelemetry {
    diagnostics: Mutex<Vec<DiagnosticRecord>>,
    metrics: Mutex<Vec<MetricObservation>>,
}

impl Telemetry for CapturingTelemetry {
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

#[test]
fn list_sessions_is_constructed_only_from_prepared_catalog_clock_and_telemetry() {
    fn prepared_only<'a>(
        clock: &'a dyn Clock,
        catalog: &'a PreparedSessionCatalog,
        telemetry: &'a dyn Telemetry,
    ) -> SessionQueryService<'a> {
        SessionQueryService::new(clock, catalog, telemetry)
    }

    let catalog = PreparedSessionCatalog::new();
    catalog
        .upsert(session(42).summary())
        .expect("prepared summary upsert succeeds");
    let clock = IncrementingClock(AtomicI64::new(100));
    let telemetry = CapturingTelemetry::default();
    let page = prepared_only(&clock, &catalog, &telemetry)
        .list(&query(10), None)
        .expect("prepared-only list succeeds");

    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].name().as_str(), "worker-42");
}

#[test]
fn list_service_records_bounded_cost_rows_duration_and_operation() {
    let catalog = PreparedSessionCatalog::new();
    for index in 0..3 {
        catalog
            .upsert(session(index).summary())
            .expect("catalog upsert succeeds");
    }
    let clock = IncrementingClock(AtomicI64::new(100));
    let telemetry = CapturingTelemetry::default();
    let service = SessionQueryService::new(&clock, &catalog, &telemetry);

    let page = service.list(&query(2), None).expect("query succeeds");
    assert_eq!(page.items.len(), 2);
    assert!(page.query_cost.index_entries_examined <= 3);

    let diagnostics = telemetry.diagnostics.lock().expect("diagnostic lock");
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(
        diagnostics[0].event,
        DiagnosticEventName::SessionQueryCompleted
    );
    assert_eq!(
        diagnostics[0].application_operation,
        Some(ApplicationOperation::ListSessions)
    );
    assert_eq!(diagnostics[0].application_error, None);
    drop(diagnostics);

    let metrics = telemetry.metrics.lock().expect("metric lock");
    assert!(metrics.iter().any(|metric| {
        metric.name == MetricName::OperationDurationSeconds
            && metric.value == 0.01
            && metric.operation == Some(ApplicationOperation::ListSessions)
    }));
    assert!(metrics.iter().any(|metric| {
        metric.name == MetricName::QueryRowsReturned
            && metric.value == 2.0
            && metric.operation == Some(ApplicationOperation::ListSessions)
    }));
    assert!(metrics.iter().any(|metric| {
        metric.name == MetricName::QueryIndexEntriesExamined
            && metric.value == page.query_cost.index_entries_examined as f64
    }));
    assert!(metrics.iter().any(|metric| {
        metric.name == MetricName::QueryCatalogRevision
            && metric.value == page.snapshot_revision.value() as f64
    }));
    assert!(metrics.iter().any(|metric| {
        metric.name == MetricName::QueryProjectionCompact && metric.value == 1.0
    }));
}

#[test]
fn exact_name_query_examines_only_its_index_candidate_at_scale() {
    let catalog = PreparedSessionCatalog::new();
    for index in 0..300 {
        catalog
            .upsert(session(index).summary())
            .expect("catalog upsert succeeds");
    }
    let clock = IncrementingClock(AtomicI64::new(100));
    let telemetry = CapturingTelemetry::default();
    let service = SessionQueryService::new(&clock, &catalog, &telemetry);
    let mut exact = query(1);
    exact.filter.name = Some(SessionName::new("worker-299").expect("exact name"));
    exact.sort.field = SessionSortField::UpdatedAt;

    let page = service.list(&exact, None).expect("exact query succeeds");

    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].name().as_str(), "worker-299");
    assert_eq!(page.query_cost.index_entries_examined, 1);
}

#[test]
fn expected_cursor_expiry_is_diagnosed_without_infrastructure_error() {
    let catalog = PreparedSessionCatalog::new();
    catalog
        .upsert(session(1).summary())
        .expect("catalog upsert succeeds");
    catalog
        .upsert(session(2).summary())
        .expect("catalog upsert succeeds");
    let clock = IncrementingClock(AtomicI64::new(100));
    let telemetry = CapturingTelemetry::default();
    let service = SessionQueryService::new(&clock, &catalog, &telemetry);
    let first = service.list(&query(1), None).expect("query succeeds");
    let cursor = first.next_cursor.expect("next cursor exists");
    catalog
        .upsert(session(3).summary())
        .expect("catalog upsert succeeds");

    let error = service
        .list(&query(1), Some(&cursor))
        .expect_err("cursor must expire");
    assert_eq!(error.category(), ApplicationErrorCategory::CursorExpired);

    let diagnostics = telemetry.diagnostics.lock().expect("diagnostic lock");
    let failure = diagnostics.last().expect("failure diagnostic exists");
    assert_eq!(failure.event, DiagnosticEventName::SessionQueryFailed);
    assert_eq!(failure.level, DiagnosticLevel::Debug);
    assert_eq!(
        failure.application_error,
        Some(ApplicationErrorCategory::CursorExpired)
    );
    assert_eq!(failure.error_kind, None);
    drop(diagnostics);
    assert!(
        telemetry
            .metrics
            .lock()
            .expect("metric lock")
            .iter()
            .any(|metric| metric.name == MetricName::QueryCursorExpirations)
    );
}

#[test]
fn watch_service_observes_initial_and_coalesced_refresh_costs() {
    let catalog = PreparedSessionCatalog::new();
    catalog
        .upsert(session(1).summary())
        .expect("catalog upsert succeeds");
    let clock = IncrementingClock(AtomicI64::new(100));
    let telemetry = CapturingTelemetry::default();
    let service = SessionQueryService::new(&clock, &catalog, &telemetry);
    let mut watch = service.open_watch(query(10)).expect("watch opens");
    assert_eq!(watch.items().len(), 1);

    catalog
        .upsert(session(2).summary())
        .expect("catalog upsert succeeds");
    let batch = service
        .refresh_watch(&mut watch)
        .expect("watch refresh succeeds")
        .expect("bounded delta exists");
    assert_eq!(batch.deltas.len(), 1);
    assert_eq!(watch.items().len(), 2);

    let diagnostics = telemetry.diagnostics.lock().expect("diagnostic lock");
    assert_eq!(diagnostics[0].event, DiagnosticEventName::QueryWatchOpened);
    assert_eq!(
        diagnostics[1].event,
        DiagnosticEventName::QueryWatchRefreshed
    );
    drop(diagnostics);

    let metrics = telemetry.metrics.lock().expect("metric lock");
    assert!(metrics.iter().any(|metric| {
        metric.name == MetricName::QueryRowsReturned
            && metric.operation == Some(ApplicationOperation::OpenSessionWatch)
    }));
    assert!(metrics.iter().any(|metric| {
        metric.name == MetricName::QueryRowsReturned
            && metric.operation == Some(ApplicationOperation::RefreshSessionWatch)
    }));
}

fn query(page_size: usize) -> SessionQuery {
    SessionQuery {
        filter: SessionFilter::default(),
        sort: SessionSort::default(),
        projection: SessionProjection::Compact,
        page_size: PageSize::new(page_size, 100).expect("page size is valid"),
        include_total: false,
    }
}

fn session(index: usize) -> Session {
    Session::create(
        SessionId::new(format!("session-{index}")).expect("session ID is valid"),
        SessionName::new(format!("worker-{index}")).expect("session name is valid"),
        SessionConfiguration::new(
            LaunchSpec::new(
                CommandSpec::new("sh", Vec::new(), limits()).expect("command is valid"),
                EnvironmentVariables::default(),
                None,
                None,
                TerminalSize::new(80, 24, 500, 200).expect("terminal size is valid"),
            ),
            ExecutionPolicy::new(
                RestartConfig::never(),
                StopPolicy::new(StopSignal::Terminate, Duration::from_secs(5))
                    .expect("stop policy is valid"),
                true,
                false,
            ),
            HealthRules::default(),
            None,
            Labels::default(),
            Metadata::default(),
        ),
        8,
        TimestampMillis::new(1),
    )
    .expect("session creation succeeds")
    .0
}

fn limits() -> ExecutionLimits {
    ExecutionLimits {
        max_arguments: 8,
        max_command_bytes: 1_024,
        max_environment_entries: 8,
        max_environment_bytes: 2_048,
        max_path_bytes: 512,
        max_health_rules: 8,
        max_health_rule_bytes: 512,
    }
}

#[test]
fn explicit_missing_ids_are_absent_not_catalog_corruption() {
    let catalog = PreparedSessionCatalog::new();
    let present = session(42);
    catalog.upsert(present.summary()).unwrap();
    let clock = IncrementingClock(AtomicI64::new(100));
    let telemetry = CapturingTelemetry::default();
    let service = SessionQueryService::new(&clock, &catalog, &telemetry);
    let mut selection = query(10);
    selection.include_total = true;
    selection.filter.ids.insert(present.id().clone());
    selection
        .filter
        .ids
        .insert(SessionId::new("missing").unwrap());
    let page = service.list(&selection, None).unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.total, Some(1));
    catalog.remove(present.id()).unwrap();
    let page = service.list(&selection, None).unwrap();
    assert!(page.items.is_empty());
    assert_eq!(page.total, Some(0));
}
