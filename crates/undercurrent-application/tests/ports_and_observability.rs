// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{Display, Formatter};

use undercurrent_application::*;
use undercurrent_domain::{LifecycleState, SessionId, TimestampMillis};

#[derive(Debug)]
struct SensitiveSource;

impl Display for SensitiveSource {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("private-token-value")
    }
}

impl Error for SensitiveSource {}

#[test]
fn observability_and_port_registries_have_unique_stable_names() {
    let actions: BTreeSet<_> = AccessAction::ALL
        .iter()
        .copied()
        .map(AccessAction::as_str)
        .collect();
    assert_eq!(actions.len(), AccessAction::ALL.len());

    let diagnostics: BTreeSet<_> = DiagnosticEventName::ALL
        .iter()
        .copied()
        .map(DiagnosticEventName::as_str)
        .collect();
    assert_eq!(diagnostics.len(), DiagnosticEventName::ALL.len());

    let application_operations: BTreeSet<_> = ApplicationOperation::ALL
        .iter()
        .copied()
        .map(ApplicationOperation::as_str)
        .collect();
    assert_eq!(
        application_operations.len(),
        ApplicationOperation::ALL.len()
    );

    let metrics: BTreeSet<_> = MetricName::ALL
        .iter()
        .copied()
        .map(MetricName::as_str)
        .collect();
    assert_eq!(metrics.len(), MetricName::ALL.len());

    let operations: BTreeSet<_> = PortOperation::ALL
        .iter()
        .copied()
        .map(PortOperation::as_str)
        .collect();
    assert_eq!(operations.len(), PortOperation::ALL.len());

    let application_errors: BTreeSet<_> = ApplicationErrorCategory::ALL
        .iter()
        .copied()
        .map(ApplicationErrorCategory::as_str)
        .collect();
    assert_eq!(
        application_errors.len(),
        ApplicationErrorCategory::ALL.len()
    );

    let port_errors: BTreeSet<_> = PortErrorKind::ALL
        .iter()
        .copied()
        .map(PortErrorKind::as_str)
        .collect();
    assert_eq!(port_errors.len(), PortErrorKind::ALL.len());

    let observation_jobs: BTreeSet<_> = ObservationJobKind::ALL
        .iter()
        .copied()
        .map(ObservationJobKind::as_str)
        .collect();
    assert_eq!(observation_jobs.len(), ObservationJobKind::ALL.len());

    let spans: BTreeSet<_> = TraceSpanName::ALL
        .iter()
        .copied()
        .map(TraceSpanName::as_str)
        .collect();
    assert_eq!(spans.len(), TraceSpanName::ALL.len());
    assert_eq!(
        TraceSpanName::ReconcileOperationAttempts.as_str(),
        "reconcile_operation_attempts"
    );
    assert_eq!(
        TraceSpanName::ReconcileOperationAttempts.qualified_name(),
        "undercurrent.span.reconcile_operation_attempts"
    );

    let fields: BTreeSet<_> = DiagnosticFieldName::ALL
        .iter()
        .copied()
        .map(DiagnosticFieldName::as_str)
        .collect();
    assert_eq!(fields.len(), DiagnosticFieldName::ALL.len());
}

#[test]
fn observability_registry_values_expose_transport_neutral_qualified_names() {
    for event in DiagnosticEventName::ALL {
        assert_eq!(
            event.qualified_event_name(),
            format!("undercurrent.event.{}", event.as_str())
        );
        assert_eq!(
            event.qualified_diagnostic_code(),
            format!("undercurrent.diagnostic.{}", event.as_str())
        );
    }

    for metric in MetricName::ALL {
        assert_eq!(
            metric.qualified_name(),
            format!("undercurrent.metric.{}", metric.as_str())
        );
    }

    for operation in ApplicationOperation::ALL {
        assert!(
            operation
                .qualified_name()
                .starts_with("undercurrent.operation.")
        );
    }

    for span in TraceSpanName::ALL {
        assert!(span.qualified_name().starts_with("undercurrent.span."));
    }

    for field in DiagnosticFieldName::ALL {
        assert!(field.qualified_name().starts_with("undercurrent.field."));
    }
}

#[test]
fn telemetry_records_remain_payload_safe_and_transport_neutral() {
    let record = DiagnosticRecord {
        event: DiagnosticEventName::SessionQueryFailed,
        level: DiagnosticLevel::Warn,
        timestamp: TimestampMillis::new(10),
        session_id: None,
        application_operation: Some(ApplicationOperation::ListSessions),
        application_error: Some(ApplicationErrorCategory::Unavailable),
        operation: None,
        error_kind: None,
    };

    assert_eq!(
        record.event.qualified_diagnostic_code(),
        "undercurrent.diagnostic.session_query_failed"
    );
    assert_eq!(record.level.as_str(), "warning");

    let metric = MetricObservation {
        name: MetricName::QueryRowsReturned,
        value: 2.0,
        operation: Some(ApplicationOperation::ListSessions),
    };
    assert_eq!(
        metric.qualified_name(),
        "undercurrent.metric.query_rows_returned"
    );
}

#[test]
fn port_error_preserves_source_but_redacts_it_from_display_and_debug() {
    let session_id = SessionId::new("session-1").expect("session ID is valid");
    let error = PortError::new(
        PortErrorKind::Unavailable,
        PortOperation::SpawnProcess,
        Some(session_id.clone()),
        SensitiveSource,
    );

    assert_eq!(error.kind(), PortErrorKind::Unavailable);
    assert_eq!(error.category(), ApplicationErrorCategory::Unavailable);
    assert_eq!(error.operation(), PortOperation::SpawnProcess);
    assert_eq!(error.session_id(), Some(&session_id));
    assert_eq!(
        error.source().expect("source is preserved").to_string(),
        "private-token-value"
    );
    assert!(!error.to_string().contains("private-token-value"));
    assert!(!format!("{error:?}").contains("private-token-value"));
}

#[test]
fn error_categories_preserve_domain_and_port_decisions() {
    assert_eq!(
        ApplicationErrorCategory::from_domain(
            undercurrent_domain::ErrorCategory::FailedPrecondition
        ),
        ApplicationErrorCategory::FailedPrecondition
    );
    assert_eq!(
        ApplicationErrorCategory::from_domain(undercurrent_domain::ErrorCategory::CapacityExceeded),
        ApplicationErrorCategory::CapacityExceeded
    );
    assert_eq!(
        PortErrorKind::Timeout.application_category(),
        ApplicationErrorCategory::Timeout
    );
    assert_eq!(
        PortErrorKind::ReplayUnavailable.application_category(),
        ApplicationErrorCategory::ReplayUnavailable
    );
    assert_eq!(
        DiagnosticLevel::for_application_error(ApplicationErrorCategory::CapacityExceeded),
        DiagnosticLevel::Warn
    );
    assert_eq!(
        DiagnosticLevel::for_application_error(ApplicationErrorCategory::ProtocolViolation),
        DiagnosticLevel::Error
    );
}

#[test]
fn asynchronous_ports_remain_object_safe_for_adapter_composition() {
    fn accepts_repository(_: &dyn SessionRepository) {}
    fn accepts_outbox(_: &dyn SessionOutbox) {}
    fn accepts_session_event_publisher(_: &dyn SessionEventPublisher) {}
    fn accepts_runtime(_: &dyn ProcessRuntime) {}
    fn accepts_projection(_: &dyn SessionProjectionWriter) {}
    fn accepts_telemetry(_: &dyn Telemetry) {}
    fn accepts_mutation_coordinator(_: &dyn SessionMutationCoordinator) {}
    fn accepts_input_delivery_store(_: &dyn InputDeliveryStore) {}
    fn accepts_authorizer(_: &dyn AuthorizationPort) {}
    fn accepts_foreground_collector(_: &dyn ForegroundEvidenceCollector) {}
    fn accepts_health_executor(_: &dyn HealthCheckExecutor) {}
    fn accepts_observation_scheduler(_: &dyn ObservationScheduler) {}
    fn accepts_due_observation_source(_: &dyn DueObservationSource) {}
    fn accepts_scheduled_observation_runner(_: &dyn ScheduledObservationRunner) {}
    fn accepts_detection_profile_catalog(_: &dyn DetectionProfileCatalog) {}
    fn accepts_profile_pattern_engine(_: &dyn ProfilePatternEngine) {}
    fn accepts_session_event_reader(_: &dyn SessionEventReader) {}

    let _ = accepts_repository;
    let _ = accepts_outbox;
    let _ = accepts_session_event_publisher;
    let _ = accepts_runtime;
    let _ = accepts_projection;
    let _ = accepts_telemetry;
    let _ = accepts_mutation_coordinator;
    let _ = accepts_input_delivery_store;
    let _ = accepts_authorizer;
    let _ = accepts_foreground_collector;
    let _ = accepts_health_executor;
    let _ = accepts_observation_scheduler;
    let _ = accepts_due_observation_source;
    let _ = accepts_scheduled_observation_runner;
    let _ = accepts_detection_profile_catalog;
    let _ = accepts_profile_pattern_engine;
    let _ = accepts_session_event_reader;
}

#[test]
fn process_termination_preserves_method_time_and_authoritative_lifecycle() {
    let exited = ProcessTermination::new(
        TerminationKind::ForcedAfterTimeout,
        Some(137),
        TimestampMillis::new(42),
    );
    assert_eq!(exited.kind(), TerminationKind::ForcedAfterTimeout);
    assert_eq!(exited.exit_code(), Some(137));
    assert_eq!(exited.observed_at(), TimestampMillis::new(42));
    assert_eq!(exited.lifecycle(), LifecycleState::Exited(137));

    let stopped = ProcessTermination::new(TerminationKind::Forced, None, TimestampMillis::new(43));
    assert_eq!(stopped.lifecycle(), LifecycleState::Stopped);
}
