// SPDX-License-Identifier: Apache-2.0

//! Bounded structured logging and application-telemetry fan-out adapters.

use std::path::Path;
use std::sync::Arc;

use crate::structured_logging::install_jsonl_logging;
pub use crate::structured_logging::{
    StructuredLogPressure as DaemonLogPressure, StructuredLoggingError as DaemonLoggingError,
    StructuredLoggingGuard as DaemonLoggingGuard, StructuredLoggingStage as DaemonLoggingStage,
};
use tracing::{Level, Span};
use tracing_subscriber::filter::LevelFilter;
use undercurrent_application::{
    DiagnosticLevel, DiagnosticRecord, MetricObservation, SessionMetricObservation, Telemetry,
};
use undercurrent_protocol::{ProductOperation, ProductStreamKind};
use undercurrent_runtime::BoundedTelemetryRecorder;
use undercurrent_wire::{ClientId, ConnectionId, RequestId, ServerEpoch, StreamId, TraceContext};

use crate::{DaemonLoggingSettings, ProductDispatchResultCategory};

/// Stable daemon-process events emitted outside application use cases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DaemonLogEventName {
    /// Structured logging became active before daemon composition.
    LoggingStarted,
    /// Daemon composition is beginning.
    ProcessStarting,
    /// The reconciled listener is ready for clients.
    ProcessReady,
    /// A process signal requested graceful shutdown.
    ShutdownRequested,
    /// A second signal or shutdown failure forced teardown.
    ShutdownForced,
    /// Ordered daemon teardown completed.
    ProcessStopped,
    /// Daemon startup or runtime ended with a typed failure.
    ProcessFailed,
}

impl DaemonLogEventName {
    /// Complete process-event registry in lifecycle order.
    pub const ALL: [Self; 7] = [
        Self::LoggingStarted,
        Self::ProcessStarting,
        Self::ProcessReady,
        Self::ShutdownRequested,
        Self::ShutdownForced,
        Self::ProcessStopped,
        Self::ProcessFailed,
    ];

    /// Returns the stable snake-case event name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LoggingStarted => "daemon_logging_started",
            Self::ProcessStarting => "daemon_process_starting",
            Self::ProcessReady => "daemon_process_ready",
            Self::ShutdownRequested => "daemon_shutdown_requested",
            Self::ShutdownForced => "daemon_shutdown_forced",
            Self::ProcessStopped => "daemon_process_stopped",
            Self::ProcessFailed => "daemon_process_failed",
        }
    }
}

/// Stable tracing spans owned by daemon transport adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DaemonTraceSpanName {
    /// Decode, authorize, execute, and settle one finite product request.
    ProductRequest,
    /// Decode, authorize, prepare, and activate one logical product stream.
    ProductStream,
}

impl DaemonTraceSpanName {
    /// Complete daemon-adapter span registry.
    pub const ALL: [Self; 2] = [Self::ProductRequest, Self::ProductStream];

    /// Returns the stable snake-case span name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProductRequest => "product_request",
            Self::ProductStream => "product_stream",
        }
    }
}

/// Installs one bounded non-blocking JSONL subscriber for the daemon process.
///
/// The configured path is never logged or included in public errors. The file
/// is owner-readable/writable on Unix, rotation keeps a fixed total file count,
/// and a saturated queue drops log lines instead of delaying request or process
/// workers. This operation performs directory, file, and thread IO.
///
/// # Errors
///
/// Returns a typed redacted directory, file, or subscriber-installation error.
pub fn install_daemon_logging(
    path: &Path,
    settings: DaemonLoggingSettings,
    debug_enabled: bool,
) -> Result<DaemonLoggingGuard, DaemonLoggingError> {
    let maximum_level = if debug_enabled {
        LevelFilter::DEBUG
    } else {
        LevelFilter::INFO
    };
    let guard = install_jsonl_logging(
        path,
        settings.structured(),
        maximum_level,
        "undercurrent-log-writer",
    )?;
    emit_daemon_log_event(DaemonLogEventName::LoggingStarted, None);
    Ok(guard)
}

/// Creates one payload-free finite-request span before product decoding.
pub(crate) fn product_request_span(
    connection_id: &ConnectionId,
    server_epoch: &ServerEpoch,
    client_id: &ClientId,
    request_id: &RequestId,
    trace_context: &TraceContext,
) -> Span {
    let correlation_id = trace_context
        .correlation_id
        .as_ref()
        .map(|value| value.as_str());
    tracing::span!(
        target: "undercurrent.dispatch",
        Level::INFO,
        DaemonTraceSpanName::ProductRequest.as_str(),
        connection_id = connection_id.as_str(),
        server_epoch = server_epoch.as_str(),
        client_id = client_id.as_str(),
        request_id = request_id.as_str(),
        trace_id = trace_context.trace_id.as_str(),
        parent_span_id = trace_context.span_id.as_str(),
        correlation_id,
        operation = tracing::field::Empty,
        session_id = tracing::field::Empty,
        result = tracing::field::Empty,
        duration_micros = tracing::field::Empty,
        response_bytes = tracing::field::Empty,
    )
}

/// Records a decoded operation from the central product registry.
pub(crate) fn record_product_operation(span: &Span, operation: ProductOperation) {
    span.record("operation", operation.as_str());
}

/// Records validated target context and the final request settlement.
pub(crate) fn record_product_request_outcome(
    span: &Span,
    session_id: Option<&str>,
    result: ProductDispatchResultCategory,
    duration_micros: u64,
    response_bytes: Option<usize>,
) {
    if let Some(session_id) = session_id {
        span.record("session_id", session_id);
    }
    span.record("result", result.as_str());
    span.record("duration_micros", duration_micros);
    if let Some(response_bytes) = response_bytes {
        span.record("response_bytes", response_bytes);
    }
}

/// Creates one payload-free logical-stream preparation span before decoding.
pub(crate) fn product_stream_span(
    connection_id: &ConnectionId,
    server_epoch: &ServerEpoch,
    client_id: &ClientId,
    request_id: &RequestId,
    stream_id: &StreamId,
    trace_context: &TraceContext,
) -> Span {
    let correlation_id = trace_context
        .correlation_id
        .as_ref()
        .map(|value| value.as_str());
    tracing::span!(
        target: "undercurrent.dispatch",
        Level::INFO,
        DaemonTraceSpanName::ProductStream.as_str(),
        connection_id = connection_id.as_str(),
        server_epoch = server_epoch.as_str(),
        client_id = client_id.as_str(),
        request_id = request_id.as_str(),
        stream_id = stream_id.as_str(),
        trace_id = trace_context.trace_id.as_str(),
        parent_span_id = trace_context.span_id.as_str(),
        correlation_id,
        stream_kind = tracing::field::Empty,
        session_id = tracing::field::Empty,
        result = tracing::field::Empty,
        duration_micros = tracing::field::Empty,
    )
}

/// Records a decoded stream kind from the central product registry.
pub(crate) fn record_product_stream_kind(span: &Span, kind: ProductStreamKind) {
    span.record("stream_kind", kind.as_str());
}

/// Records validated target context and the final stream-open settlement.
pub(crate) fn record_product_stream_outcome(
    span: &Span,
    session_id: Option<&str>,
    result: ProductDispatchResultCategory,
    duration_micros: u64,
) {
    if let Some(session_id) = session_id {
        span.record("session_id", session_id);
    }
    span.record("result", result.as_str());
    span.record("duration_micros", duration_micros);
}

/// Emits one payload-free daemon-process event through the installed adapter.
///
/// `error_kind` must be a stable typed category, never a source message. This
/// function performs no direct IO and does not panic.
pub fn emit_daemon_log_event(name: DaemonLogEventName, error_kind: Option<&'static str>) {
    match name {
        DaemonLogEventName::ProcessFailed | DaemonLogEventName::ShutdownForced => {
            tracing::event!(
                target: "undercurrent.daemon",
                Level::ERROR,
                event_name = name.as_str(),
                error_kind,
                "daemon process event"
            );
        }
        DaemonLogEventName::ShutdownRequested => {
            tracing::event!(
                target: "undercurrent.daemon",
                Level::WARN,
                event_name = name.as_str(),
                error_kind,
                "daemon process event"
            );
        }
        _ => {
            tracing::event!(
                target: "undercurrent.daemon",
                Level::INFO,
                event_name = name.as_str(),
                error_kind,
                "daemon process event"
            );
        }
    }
}

/// Telemetry adapter that fans safe records into bounded inspection and logs.
pub struct DaemonTelemetry {
    retained: Arc<BoundedTelemetryRecorder>,
    diagnostics_enabled: bool,
}

impl DaemonTelemetry {
    /// Creates a fan-out adapter without performing IO.
    pub const fn new(retained: Arc<BoundedTelemetryRecorder>, diagnostics_enabled: bool) -> Self {
        Self {
            retained,
            diagnostics_enabled,
        }
    }

    /// Returns the bounded recorder used by operational diagnostic views.
    pub fn retained(&self) -> &Arc<BoundedTelemetryRecorder> {
        &self.retained
    }
}

impl Telemetry for DaemonTelemetry {
    fn emit_diagnostic(&self, record: &DiagnosticRecord) {
        if self.diagnostics_enabled {
            self.retained.emit_diagnostic(record);
        }
        emit_application_diagnostic(record);
    }

    fn record_metric(&self, observation: MetricObservation) {
        self.retained.record_metric(observation);
        if !observation.value.is_finite() {
            return;
        }
        tracing::event!(
            target: "undercurrent.metric",
            Level::DEBUG,
            event_name = "metric_observed",
            metric_name = observation.name.as_str(),
            metric_value = observation.value,
            operation = observation.operation.map(|value| value.as_str()),
            "application metric"
        );
    }

    fn record_session_metric(&self, observation: &SessionMetricObservation) {
        self.retained.record_session_metric(observation);
        let metric = observation.metric();
        if !metric.value.is_finite() {
            return;
        }
        tracing::event!(
            target: "undercurrent.metric",
            Level::DEBUG,
            event_name = "metric_observed",
            metric_name = metric.name.as_str(),
            metric_value = metric.value,
            operation = metric.operation.map(|value| value.as_str()),
            session_id = observation.session_id().as_str(),
            "application session metric"
        );
    }
}

fn emit_application_diagnostic(record: &DiagnosticRecord) {
    let operation = record
        .application_operation
        .map(|value| value.as_str())
        .or_else(|| record.operation.map(|value| value.as_str()));
    let error_kind = record
        .application_error
        .map(|value| value.as_str())
        .or_else(|| record.error_kind.map(|value| value.as_str()));
    let session_id = record.session_id.as_ref().map(|value| value.as_str());
    macro_rules! emit {
        ($level:expr) => {
            tracing::event!(
                target: "undercurrent.application",
                $level,
                event_name = record.event.as_str(),
                session_id,
                operation,
                error_kind,
                "application diagnostic"
            )
        };
    }
    match record.level {
        DiagnosticLevel::Debug => emit!(Level::DEBUG),
        DiagnosticLevel::Info => emit!(Level::INFO),
        DiagnosticLevel::Warn => emit!(Level::WARN),
        DiagnosticLevel::Error => emit!(Level::ERROR),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::io::{self, Write};
    use std::process::Command;
    use std::sync::Mutex;
    use tracing_subscriber::fmt::MakeWriter;
    use undercurrent_application::{
        ApplicationOperation, DiagnosticEventName, PortError, PortErrorKind, PortOperation,
    };
    use undercurrent_domain::{SessionId, TimestampMillis};

    #[test]
    fn daemon_log_event_registry_is_complete_and_unique() {
        let names = DaemonLogEventName::ALL
            .into_iter()
            .map(DaemonLogEventName::as_str)
            .collect::<BTreeSet<_>>();
        let spans = DaemonTraceSpanName::ALL
            .into_iter()
            .map(DaemonTraceSpanName::as_str)
            .collect::<BTreeSet<_>>();

        assert_eq!(names.len(), DaemonLogEventName::ALL.len());
        assert_eq!(spans.len(), DaemonTraceSpanName::ALL.len());
    }

    #[test]
    fn telemetry_emits_only_typed_safe_fields_and_honors_capture_policy() {
        const CREDENTIAL_CANARY: &str = "private-token-adapter-source-canary";
        const TEST_NAME: &str =
            "logging::tests::telemetry_emits_only_typed_safe_fields_and_honors_capture_policy";
        if run_isolated_capture_test(TEST_NAME) {
            return;
        }
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_ansi(false)
            .with_max_level(LevelFilter::DEBUG)
            .with_writer(BufferMakeWriter(bytes.clone()))
            .finish();
        let retained = Arc::new(BoundedTelemetryRecorder::new(4, 4).unwrap());
        let telemetry = DaemonTelemetry::new(retained.clone(), true);
        let sensitive_error = PortError::new(
            PortErrorKind::Unavailable,
            PortOperation::InsertSession,
            Some(SessionId::new("session-safe").unwrap()),
            io::Error::other(CREDENTIAL_CANARY),
        );
        assert_eq!(
            std::error::Error::source(&sensitive_error)
                .expect("private adapter source is retained inwardly")
                .to_string(),
            CREDENTIAL_CANARY
        );
        let record = DiagnosticRecord {
            event: DiagnosticEventName::ProcessSpawnFailed,
            level: DiagnosticLevel::Error,
            timestamp: TimestampMillis::new(1),
            session_id: Some(SessionId::new("session-safe").unwrap()),
            application_operation: Some(ApplicationOperation::StartSession),
            application_error: Some(sensitive_error.category()),
            operation: Some(PortOperation::InsertSession),
            error_kind: Some(sensitive_error.kind()),
        };

        tracing::subscriber::with_default(subscriber, || {
            telemetry.emit_diagnostic(&record);
            telemetry.record_metric(MetricObservation {
                name: undercurrent_application::MetricName::OperationDurationSeconds,
                value: 0.5,
                operation: Some(ApplicationOperation::StartSession),
            });
            telemetry.record_session_metric(&SessionMetricObservation::new(
                SessionId::new("session-metric").unwrap(),
                MetricObservation {
                    name: undercurrent_application::MetricName::TerminalOutputBytesAppended,
                    value: 4096.0,
                    operation: None,
                },
            ));
        });

        assert_eq!(retained.diagnostics(), vec![record]);
        assert_eq!(retained.metrics().len(), 2);
        assert_eq!(retained.session_metrics().len(), 1);
        let output = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert!(output.contains("process_spawn_failed"));
        assert!(output.contains("session-safe"));
        assert!(output.contains("session-metric"));
        assert!(output.contains("terminal_output_bytes_appended"));
        assert!(output.contains("operation_duration_seconds"));
        assert!(output.contains("unavailable"));
        assert!(!output.contains(CREDENTIAL_CANARY));
        assert!(!format!("{:?}", retained.diagnostics()).contains(CREDENTIAL_CANARY));
        mark_isolated_capture_test_complete(TEST_NAME);
    }

    #[test]
    fn request_and_stream_spans_emit_only_typed_safe_context() {
        const TEST_NAME: &str =
            "logging::tests::request_and_stream_spans_emit_only_typed_safe_context";
        if run_isolated_capture_test(TEST_NAME) {
            return;
        }
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_ansi(false)
            .with_current_span(true)
            .with_max_level(LevelFilter::INFO)
            .with_writer(BufferMakeWriter(bytes.clone()))
            .finish();
        let client_id = ClientId::new("client-safe").unwrap();
        let connection_id = ConnectionId::new("connection-safe").unwrap();
        let server_epoch = ServerEpoch::new("epoch-safe").unwrap();
        let request_id = RequestId::new("request-safe").unwrap();
        let stream_id = StreamId::new("stream-safe").unwrap();
        let trace_context = TraceContext::new(
            undercurrent_wire::TraceId::new("trace-safe").unwrap(),
            undercurrent_wire::SpanId::new("parent-safe").unwrap(),
        )
        .with_correlation_id(undercurrent_wire::CorrelationId::new("correlation-safe").unwrap());

        tracing::subscriber::with_default(subscriber, || {
            let request = product_request_span(
                &connection_id,
                &server_epoch,
                &client_id,
                &request_id,
                &trace_context,
            );
            record_product_operation(&request, ProductOperation::ListSessions);
            record_product_request_outcome(
                &request,
                Some("session-safe"),
                ProductDispatchResultCategory::Success,
                42,
                Some(384),
            );
            let entered = request.enter();
            tracing::info!(event_name = "request_test", "test event");
            drop(entered);

            let stream = product_stream_span(
                &connection_id,
                &server_epoch,
                &client_id,
                &request_id,
                &stream_id,
                &trace_context,
            );
            record_product_stream_kind(&stream, ProductStreamKind::TerminalOutput);
            record_product_stream_outcome(
                &stream,
                Some("session-safe"),
                ProductDispatchResultCategory::Rejected,
                43,
            );
            let _entered = stream.enter();
            tracing::info!(event_name = "stream_test", "test event");
        });

        let output = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert!(output.contains("product_request"));
        assert!(output.contains("connection-safe"));
        assert!(output.contains("epoch-safe"));
        assert!(output.contains("list_sessions"));
        assert!(output.contains("product_stream"));
        assert!(output.contains("terminal_output"));
        assert!(output.contains("client-safe"));
        assert!(output.contains("request-safe"));
        assert!(output.contains("stream-safe"));
        assert!(output.contains("trace-safe"));
        assert!(output.contains("parent-safe"));
        assert!(output.contains("correlation-safe"));
        assert!(output.contains("session-safe"));
        assert!(output.contains("success"));
        assert!(output.contains("rejected"));
        assert!(output.contains("384"));
        assert!(!output.contains("private"));
        mark_isolated_capture_test_complete(TEST_NAME);
    }

    fn run_isolated_capture_test(test_name: &str) -> bool {
        const ISOLATED_TEST: &str = "UNDERCURRENT_ISOLATED_LOGGING_CAPTURE_TEST";
        const COMPLETION_MARKER: &str = "UNDERCURRENT_LOGGING_CAPTURE_TEST_COMPLETED:";
        if std::env::var(ISOLATED_TEST).as_deref() == Ok(test_name) {
            return false;
        }

        // Tracing callsite interest is process-global and outlives scoped subscribers.
        let output = Command::new(std::env::current_exe().expect("current test executable"))
            .args(["--exact", test_name, "--test-threads=1", "--nocapture"])
            .env(ISOLATED_TEST, test_name)
            .output()
            .expect("isolated logging capture test starts");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success() && stdout.contains(&format!("{COMPLETION_MARKER}{test_name}")),
            "isolated logging capture test failed\nstdout:\n{}\nstderr:\n{}",
            stdout,
            String::from_utf8_lossy(&output.stderr)
        );
        true
    }

    fn mark_isolated_capture_test_complete(test_name: &str) {
        println!("UNDERCURRENT_LOGGING_CAPTURE_TEST_COMPLETED:{test_name}");
    }

    #[derive(Clone)]
    struct BufferMakeWriter(Arc<Mutex<Vec<u8>>>);

    impl<'writer> MakeWriter<'writer> for BufferMakeWriter {
        type Writer = BufferWriter;

        fn make_writer(&'writer self) -> Self::Writer {
            BufferWriter(self.0.clone())
        }
    }

    struct BufferWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for BufferWriter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
}
