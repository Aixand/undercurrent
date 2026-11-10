// SPDX-License-Identifier: Apache-2.0

//! Bounded payload-free product-dispatch telemetry adapter.
//!
//! Event counters count observations, not unique requests. Timed events can
//! overlap; use operation-specific RPC metrics for finite-request latency.
//! Retention and try-lock collection may lose observations under pressure, so
//! this adapter is not durable evidence of whether an external effect occurred.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, TryLockError};

use tracing::Level;
use undercurrent_application::{ApplicationOperation, MetricName, MetricObservation, Telemetry};
use undercurrent_protocol::ProductOperation;

use crate::{
    ProductDispatchEvent, ProductDispatchEventName, ProductDispatchMetricName,
    ProductDispatchResultCategory, ProductDispatchTelemetry, ProductDispatchTelemetryLimitError,
};

/// Fixed-cardinality aggregate for one product dispatch metric series.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProductDispatchMetricSeries {
    /// Stable metric identity.
    pub name: ProductDispatchMetricName,
    /// Typed finite RPC operation when known.
    pub operation: Option<ProductOperation>,
    /// Typed terminal result dimension for result counters.
    pub result: Option<ProductDispatchResultCategory>,
    /// Finite samples observed since process start.
    pub samples: u64,
    /// Saturating finite sum of observed values.
    pub total: f64,
    /// Smallest observed value.
    pub minimum: f64,
    /// Largest observed value.
    pub maximum: f64,
    /// Most recently observed value.
    pub latest: f64,
}

/// Aggregate dispatch telemetry suitable for fixed-cardinality diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductDispatchTelemetryPressure {
    /// Payload-free events currently retained for inspection.
    pub retained_events: usize,
    /// Inclusive retained-event bound.
    pub event_limit: usize,
    /// Events evicted or skipped because the recorder lock was contended.
    pub dropped_events: u64,
    /// Every dispatch event observed since process start.
    pub observed_events: u64,
    /// Observed events whose result was neither success nor cancellation.
    pub unsuccessful_events: u64,
    /// Observed events carrying a duration.
    pub timed_events: u64,
    /// Saturating total duration across timed events.
    pub total_duration_micros: u64,
    /// Largest observed event duration.
    pub maximum_duration_micros: u64,
    /// Fixed-cardinality product metric series currently populated.
    pub metric_series: usize,
    /// Metric updates rejected as non-finite or skipped on lock contention.
    pub dropped_metric_updates: u64,
    /// Finite and stream-open actions currently executing.
    pub requests_in_flight: u64,
    /// Largest observed concurrent executing-action count.
    pub maximum_requests_in_flight: u64,
}

/// Non-blocking bounded recorder for payload-free dispatcher decisions.
pub struct BoundedProductDispatchTelemetry {
    event_limit: usize,
    diagnostics_enabled: bool,
    debug_enabled: bool,
    retained: Mutex<VecDeque<ProductDispatchEvent>>,
    dropped_events: AtomicU64,
    observed_events: AtomicU64,
    unsuccessful_events: AtomicU64,
    timed_events: AtomicU64,
    total_duration_micros: AtomicU64,
    maximum_duration_micros: AtomicU64,
    metrics: Mutex<BTreeMap<ProductDispatchMetricKey, ProductDispatchMetricValue>>,
    dropped_metric_updates: AtomicU64,
    requests_in_flight: AtomicU64,
    maximum_requests_in_flight: AtomicU64,
    application_telemetry: Option<Arc<dyn Telemetry>>,
}

impl BoundedProductDispatchTelemetry {
    /// Constructs a recorder with explicit capture policy and a hard event bound.
    ///
    /// Aggregate counters remain active when diagnostic retention is disabled.
    /// Emission performs no IO and uses a non-waiting lock attempt for retained
    /// records, so telemetry cannot delay dispatcher workers.
    ///
    /// # Errors
    ///
    /// Returns a typed limit error when `event_limit` is zero.
    pub fn new(
        event_limit: usize,
        diagnostics_enabled: bool,
        debug_enabled: bool,
    ) -> Result<Self, ProductDispatchTelemetryLimitError> {
        Self::build(event_limit, diagnostics_enabled, debug_enabled, None)
    }

    /// Constructs a recorder that also publishes typed application metrics.
    ///
    /// Metric publication carries only fixed registry dimensions and performs
    /// no application decision-making. The supplied telemetry port must remain
    /// non-blocking because dispatcher workers call it directly.
    ///
    /// # Errors
    ///
    /// Returns a typed limit error when `event_limit` is zero.
    pub fn with_application_metrics(
        event_limit: usize,
        diagnostics_enabled: bool,
        debug_enabled: bool,
        application_telemetry: Arc<dyn Telemetry>,
    ) -> Result<Self, ProductDispatchTelemetryLimitError> {
        Self::build(
            event_limit,
            diagnostics_enabled,
            debug_enabled,
            Some(application_telemetry),
        )
    }

    fn build(
        event_limit: usize,
        diagnostics_enabled: bool,
        debug_enabled: bool,
        application_telemetry: Option<Arc<dyn Telemetry>>,
    ) -> Result<Self, ProductDispatchTelemetryLimitError> {
        if event_limit == 0 {
            return Err(ProductDispatchTelemetryLimitError);
        }
        Ok(Self {
            event_limit,
            diagnostics_enabled,
            debug_enabled,
            retained: Mutex::new(VecDeque::new()),
            dropped_events: AtomicU64::new(0),
            observed_events: AtomicU64::new(0),
            unsuccessful_events: AtomicU64::new(0),
            timed_events: AtomicU64::new(0),
            total_duration_micros: AtomicU64::new(0),
            maximum_duration_micros: AtomicU64::new(0),
            metrics: Mutex::new(BTreeMap::new()),
            dropped_metric_updates: AtomicU64::new(0),
            requests_in_flight: AtomicU64::new(0),
            maximum_requests_in_flight: AtomicU64::new(0),
            application_telemetry,
        })
    }

    /// Returns a bounded oldest-to-newest payload-free event snapshot.
    pub fn events(&self) -> Vec<ProductDispatchEvent> {
        self.lock_retained().iter().cloned().collect()
    }

    /// Returns bounded retention and atomic aggregate pressure.
    pub fn pressure(&self) -> ProductDispatchTelemetryPressure {
        let metric_series = self
            .metrics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len();
        ProductDispatchTelemetryPressure {
            retained_events: self.lock_retained().len(),
            event_limit: self.event_limit,
            dropped_events: self.dropped_events.load(Ordering::Acquire),
            observed_events: self.observed_events.load(Ordering::Acquire),
            unsuccessful_events: self.unsuccessful_events.load(Ordering::Acquire),
            timed_events: self.timed_events.load(Ordering::Acquire),
            total_duration_micros: self.total_duration_micros.load(Ordering::Acquire),
            maximum_duration_micros: self.maximum_duration_micros.load(Ordering::Acquire),
            metric_series,
            dropped_metric_updates: self.dropped_metric_updates.load(Ordering::Acquire),
            requests_in_flight: self.requests_in_flight.load(Ordering::Acquire),
            maximum_requests_in_flight: self.maximum_requests_in_flight.load(Ordering::Acquire),
        }
    }

    /// Returns populated product metric series in stable registry order.
    pub fn metric_series(&self) -> Vec<ProductDispatchMetricSeries> {
        self.metrics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|(key, value)| value.snapshot(*key))
            .collect()
    }

    fn lock_retained(&self) -> std::sync::MutexGuard<'_, VecDeque<ProductDispatchEvent>> {
        self.retained
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    const fn should_retain(&self, name: ProductDispatchEventName) -> bool {
        self.diagnostics_enabled
            && (self.debug_enabled
                || matches!(
                    name,
                    ProductDispatchEventName::AdmissionRejected
                        | ProductDispatchEventName::RequestFailed
                        | ProductDispatchEventName::StreamRejected
                        | ProductDispatchEventName::ExecutorPanicked
                ))
    }

    fn retain_without_waiting(&self, event: ProductDispatchEvent) {
        let mut retained = match self.retained.try_lock() {
            Ok(retained) => retained,
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => {
                self.dropped_events.fetch_add(1, Ordering::AcqRel);
                return;
            }
        };
        if retained.len() >= self.event_limit {
            retained.pop_front();
            self.dropped_events.fetch_add(1, Ordering::AcqRel);
        }
        retained.push_back(event);
    }

    fn observe_metrics(&self, event: &ProductDispatchEvent) {
        match event.name {
            ProductDispatchEventName::ActionStarted => {
                let in_flight = self
                    .requests_in_flight
                    .fetch_add(1, Ordering::AcqRel)
                    .saturating_add(1);
                self.maximum_requests_in_flight
                    .fetch_max(in_flight, Ordering::AcqRel);
                self.record_metric(
                    ProductDispatchMetricName::RequestsInFlight,
                    None,
                    None,
                    in_flight as f64,
                );
                self.record_application_metric(
                    MetricName::RequestsInFlight,
                    in_flight as f64,
                    None,
                );
                if let Some(duration_micros) = event.duration_micros {
                    let seconds = duration_micros as f64 / 1_000_000.0;
                    self.record_metric(
                        ProductDispatchMetricName::RequestQueueWaitSeconds,
                        None,
                        None,
                        seconds,
                    );
                    self.record_application_metric(
                        MetricName::RequestQueueWaitSeconds,
                        seconds,
                        None,
                    );
                }
            }
            ProductDispatchEventName::ActionFinished => {
                let in_flight = self
                    .requests_in_flight
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                        Some(value.saturating_sub(1))
                    })
                    .unwrap_or(0)
                    .saturating_sub(1);
                self.record_metric(
                    ProductDispatchMetricName::RequestsInFlight,
                    None,
                    None,
                    in_flight as f64,
                );
                self.record_application_metric(
                    MetricName::RequestsInFlight,
                    in_flight as f64,
                    None,
                );
            }
            ProductDispatchEventName::RequestCompleted
            | ProductDispatchEventName::RequestFailed => {
                let application_operation = event.operation.and_then(application_operation);
                if let Some(duration_micros) = event.duration_micros {
                    let seconds = duration_micros as f64 / 1_000_000.0;
                    self.record_metric(
                        ProductDispatchMetricName::RpcDurationSeconds,
                        event.operation,
                        None,
                        seconds,
                    );
                    self.record_application_metric(
                        MetricName::RpcDurationSeconds,
                        seconds,
                        application_operation,
                    );
                }
                self.record_metric(
                    ProductDispatchMetricName::RpcResults,
                    event.operation,
                    Some(event.result),
                    1.0,
                );
                self.record_application_metric(
                    result_metric_name(event.result),
                    1.0,
                    application_operation,
                );
                if let Some(response_bytes) = event.response_bytes {
                    self.record_metric(
                        ProductDispatchMetricName::RpcResponseBytes,
                        event.operation,
                        None,
                        response_bytes as f64,
                    );
                    self.record_application_metric(
                        MetricName::RpcResponseBytes,
                        response_bytes as f64,
                        application_operation,
                    );
                }
            }
            ProductDispatchEventName::AdmissionRejected => {
                self.record_application_metric(MetricName::OverloadRejections, 1.0, None);
            }
            _ => {}
        }
    }

    fn record_metric(
        &self,
        name: ProductDispatchMetricName,
        operation: Option<ProductOperation>,
        result: Option<ProductDispatchResultCategory>,
        value: f64,
    ) {
        if !value.is_finite() {
            self.dropped_metric_updates.fetch_add(1, Ordering::AcqRel);
            return;
        }
        let mut metrics = match self.metrics.try_lock() {
            Ok(metrics) => metrics,
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => {
                self.dropped_metric_updates.fetch_add(1, Ordering::AcqRel);
                return;
            }
        };
        metrics
            .entry(ProductDispatchMetricKey {
                name,
                operation,
                result,
            })
            .and_modify(|metric| metric.record(value))
            .or_insert_with(|| ProductDispatchMetricValue::new(value));
    }

    fn record_application_metric(
        &self,
        name: MetricName,
        value: f64,
        operation: Option<ApplicationOperation>,
    ) {
        if let Some(telemetry) = self.application_telemetry.as_ref() {
            telemetry.record_metric(MetricObservation {
                name,
                value,
                operation,
            });
        }
    }
}

impl ProductDispatchTelemetry for BoundedProductDispatchTelemetry {
    fn emit(&self, event: ProductDispatchEvent) {
        emit_structured_dispatch_event(&event);
        self.observe_metrics(&event);
        self.observed_events.fetch_add(1, Ordering::AcqRel);
        if !matches!(
            event.result,
            ProductDispatchResultCategory::Success | ProductDispatchResultCategory::Cancelled
        ) {
            self.unsuccessful_events.fetch_add(1, Ordering::AcqRel);
        }
        if let Some(duration_micros) = event.duration_micros {
            self.timed_events.fetch_add(1, Ordering::AcqRel);
            let _ = self.total_duration_micros.fetch_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |total| Some(total.saturating_add(duration_micros)),
            );
            self.maximum_duration_micros
                .fetch_max(duration_micros, Ordering::AcqRel);
        }
        if self.should_retain(event.name) {
            self.retain_without_waiting(event);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ProductDispatchMetricKey {
    name: ProductDispatchMetricName,
    operation: Option<ProductOperation>,
    result: Option<ProductDispatchResultCategory>,
}

#[derive(Debug, Clone, Copy)]
struct ProductDispatchMetricValue {
    samples: u64,
    total: f64,
    minimum: f64,
    maximum: f64,
    latest: f64,
}

impl ProductDispatchMetricValue {
    fn new(value: f64) -> Self {
        Self {
            samples: 1,
            total: value,
            minimum: value,
            maximum: value,
            latest: value,
        }
    }

    fn record(&mut self, value: f64) {
        self.samples = self.samples.saturating_add(1);
        self.total = finite_saturating_add(self.total, value);
        self.minimum = self.minimum.min(value);
        self.maximum = self.maximum.max(value);
        self.latest = value;
    }

    fn snapshot(self, key: ProductDispatchMetricKey) -> ProductDispatchMetricSeries {
        ProductDispatchMetricSeries {
            name: key.name,
            operation: key.operation,
            result: key.result,
            samples: self.samples,
            total: self.total,
            minimum: self.minimum,
            maximum: self.maximum,
            latest: self.latest,
        }
    }
}

fn finite_saturating_add(left: f64, right: f64) -> f64 {
    let total = left + right;
    if total.is_finite() {
        total
    } else if total.is_sign_negative() {
        -f64::MAX
    } else {
        f64::MAX
    }
}

const fn result_metric_name(result: ProductDispatchResultCategory) -> MetricName {
    match result {
        ProductDispatchResultCategory::Success => MetricName::RpcResultsSuccess,
        ProductDispatchResultCategory::InvalidRequest => MetricName::RpcResultsInvalidRequest,
        ProductDispatchResultCategory::Rejected => MetricName::RpcResultsRejected,
        ProductDispatchResultCategory::Cancelled => MetricName::RpcResultsCancelled,
        ProductDispatchResultCategory::Capacity => MetricName::RpcResultsCapacity,
        ProductDispatchResultCategory::Encoding => MetricName::RpcResultsEncoding,
        ProductDispatchResultCategory::Connection => MetricName::RpcResultsConnection,
        ProductDispatchResultCategory::Internal => MetricName::RpcResultsInternal,
    }
}

const fn application_operation(operation: ProductOperation) -> Option<ApplicationOperation> {
    match operation {
        ProductOperation::StartSession => Some(ApplicationOperation::StartSession),
        ProductOperation::StopSession => Some(ApplicationOperation::StopSession),
        ProductOperation::RestartSession => Some(ApplicationOperation::RestartSession),
        ProductOperation::RenameSession => Some(ApplicationOperation::RenameSession),
        ProductOperation::UpdateSessionLabels => Some(ApplicationOperation::UpdateSessionLabels),
        ProductOperation::RemoveSession => Some(ApplicationOperation::RemoveSession),
        ProductOperation::ResizeTerminal => Some(ApplicationOperation::ResizeTerminal),
        ProductOperation::SendTerminalInput => Some(ApplicationOperation::SendTerminalInput),
        ProductOperation::CloseStandardInput => Some(ApplicationOperation::CloseStandardInput),
        ProductOperation::ListSessions => Some(ApplicationOperation::ListSessions),
        ProductOperation::GetSessionDetail => Some(ApplicationOperation::GetSessionDetail),
        ProductOperation::GetExecutionContext => {
            Some(ApplicationOperation::ResolveExecutionContext)
        }
        ProductOperation::ExecuteInSession => Some(ApplicationOperation::ExecuteInSession),
        ProductOperation::RepairOperationAttempt => {
            Some(ApplicationOperation::RepairOperationAttempt)
        }
        ProductOperation::GetRuntimeAttemptStatus => {
            Some(ApplicationOperation::GetRuntimeAttemptStatus)
        }
        ProductOperation::CancelRuntimeAttempt => Some(ApplicationOperation::CancelRuntimeAttempt),
        ProductOperation::GetRuntimeCancellationStatus => {
            Some(ApplicationOperation::GetRuntimeCancellationStatus)
        }
        ProductOperation::ReadTerminalOutput => Some(ApplicationOperation::ReadTerminalOutput),
        ProductOperation::ReadTerminalScreen => Some(ApplicationOperation::ReadTerminalScreen),
        ProductOperation::ReadTerminalScreenDiff => {
            Some(ApplicationOperation::ReadTerminalScreenDiff)
        }
        ProductOperation::ReadSessionEvents => Some(ApplicationOperation::ReadSessionEvents),
        ProductOperation::UpsertWorkItem => Some(ApplicationOperation::UpsertWorkContext),
        ProductOperation::RemoveWorkItem => Some(ApplicationOperation::RemoveWorkContext),
        ProductOperation::ListProfiles => Some(ApplicationOperation::ListDetectionProfiles),
        ProductOperation::GetProfile => Some(ApplicationOperation::InspectDetectionProfile),
        ProductOperation::MatchProfile => Some(ApplicationOperation::MatchDetectionProfile),
        ProductOperation::LintProfiles => Some(ApplicationOperation::LintDetectionProfile),
        ProductOperation::TestProfile => Some(ApplicationOperation::TestDetectionProfile),
        ProductOperation::GetDiagnostics
        | ProductOperation::CreateDiagnosticBundle
        | ProductOperation::GetDaemonStatus
        | ProductOperation::ShutdownDaemon
        | ProductOperation::GetAgentEvent
        | ProductOperation::ReplyAgentEvent => None,
    }
}

fn emit_structured_dispatch_event(event: &ProductDispatchEvent) {
    let connection_id = event.connection_id.as_ref().map(|value| value.as_str());
    let server_epoch = event.server_epoch.as_ref().map(|value| value.as_str());
    let client_id = event.client_id.as_ref().map(|value| value.as_str());
    let request_id = event.request_id.as_ref().map(|value| value.as_str());
    let stream_id = event.stream_id.as_ref().map(|value| value.as_str());
    let trace_id = event
        .trace_context
        .as_ref()
        .map(|value| value.trace_id.as_str());
    let parent_span_id = event
        .trace_context
        .as_ref()
        .map(|value| value.span_id.as_str());
    let correlation_id = event
        .trace_context
        .as_ref()
        .and_then(|value| value.correlation_id.as_ref())
        .map(|value| value.as_str());
    let operation = event.operation.map(|value| value.as_str());
    let stream_kind = event.stream_kind.map(|value| value.as_str());
    macro_rules! emit {
        ($level:expr) => {
            tracing::event!(
                target: "undercurrent.dispatch",
                $level,
                event_name = event.name.as_str(),
                connection_id,
                server_epoch,
                client_id,
                request_id,
                stream_id,
                trace_id,
                parent_span_id,
                correlation_id,
                operation,
                stream_kind,
                result = event.result.as_str(),
                duration_micros = event.duration_micros,
                response_bytes = event.response_bytes,
                "product dispatch event"
            )
        };
    }
    match event.result {
        ProductDispatchResultCategory::Internal => emit!(Level::ERROR),
        ProductDispatchResultCategory::Capacity
        | ProductDispatchResultCategory::Encoding
        | ProductDispatchResultCategory::Connection => emit!(Level::WARN),
        ProductDispatchResultCategory::Success
        | ProductDispatchResultCategory::InvalidRequest
        | ProductDispatchResultCategory::Rejected
        | ProductDispatchResultCategory::Cancelled => emit!(Level::DEBUG),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(
        name: ProductDispatchEventName,
        result: ProductDispatchResultCategory,
        duration_micros: Option<u64>,
    ) -> ProductDispatchEvent {
        ProductDispatchEvent {
            name,
            connection_id: None,
            server_epoch: None,
            client_id: None,
            request_id: None,
            stream_id: None,
            trace_context: None,
            operation: None,
            stream_kind: None,
            result,
            duration_micros,
            response_bytes: None,
        }
    }

    #[test]
    fn default_capture_retains_failures_and_aggregates_every_event() {
        let recorder = BoundedProductDispatchTelemetry::new(2, true, false).unwrap();
        recorder.emit(event(
            ProductDispatchEventName::RequestCompleted,
            ProductDispatchResultCategory::Success,
            Some(10),
        ));
        recorder.emit(event(
            ProductDispatchEventName::RequestFailed,
            ProductDispatchResultCategory::Connection,
            Some(20),
        ));

        assert_eq!(recorder.events().len(), 1);
        assert_eq!(
            recorder.pressure(),
            ProductDispatchTelemetryPressure {
                retained_events: 1,
                event_limit: 2,
                dropped_events: 0,
                observed_events: 2,
                unsuccessful_events: 1,
                timed_events: 2,
                total_duration_micros: 30,
                maximum_duration_micros: 20,
                metric_series: 3,
                dropped_metric_updates: 0,
                requests_in_flight: 0,
                maximum_requests_in_flight: 0,
            }
        );
    }

    #[test]
    fn worker_and_rpc_events_populate_typed_bounded_metric_series() {
        let retained = Arc::new(
            undercurrent_runtime::BoundedTelemetryRecorder::new(1, 16).expect("telemetry"),
        );
        let recorder = BoundedProductDispatchTelemetry::with_application_metrics(
            2,
            false,
            false,
            retained.clone(),
        )
        .expect("dispatch telemetry");
        recorder.emit(event(
            ProductDispatchEventName::ActionStarted,
            ProductDispatchResultCategory::Success,
            Some(2_000),
        ));
        let mut completed = event(
            ProductDispatchEventName::RequestCompleted,
            ProductDispatchResultCategory::Success,
            Some(4_000),
        );
        completed.operation = Some(ProductOperation::ListSessions);
        completed.response_bytes = Some(384);
        recorder.emit(completed);
        recorder.emit(event(
            ProductDispatchEventName::ActionFinished,
            ProductDispatchResultCategory::Success,
            Some(5_000),
        ));
        recorder.emit(event(
            ProductDispatchEventName::AdmissionRejected,
            ProductDispatchResultCategory::Capacity,
            None,
        ));

        assert_eq!(recorder.pressure().requests_in_flight, 0);
        assert_eq!(recorder.pressure().maximum_requests_in_flight, 1);
        assert_eq!(recorder.metric_series().len(), 5);
        assert!(recorder.metric_series().iter().any(|series| {
            series.name == ProductDispatchMetricName::RpcResults
                && series.operation == Some(ProductOperation::ListSessions)
                && series.result == Some(ProductDispatchResultCategory::Success)
                && series.total == 1.0
        }));
        assert!(retained.metric_aggregates().iter().any(|aggregate| {
            aggregate.name == MetricName::RpcDurationSeconds
                && aggregate.operation == Some(ApplicationOperation::ListSessions)
                && aggregate.latest == 0.004
        }));
        assert!(recorder.metric_series().iter().any(|series| {
            series.name == ProductDispatchMetricName::RpcResponseBytes
                && series.operation == Some(ProductOperation::ListSessions)
                && series.latest == 384.0
        }));
        assert!(retained.metric_aggregates().iter().any(|aggregate| {
            aggregate.name == MetricName::RpcResponseBytes
                && aggregate.operation == Some(ApplicationOperation::ListSessions)
                && aggregate.latest == 384.0
        }));
        assert!(retained.metric_aggregates().iter().any(|aggregate| {
            aggregate.name == MetricName::OverloadRejections && aggregate.total == 1.0
        }));
    }

    #[test]
    fn debug_capture_evicts_oldest_records_at_the_hard_bound() {
        let recorder = BoundedProductDispatchTelemetry::new(1, true, true).unwrap();
        recorder.emit(event(
            ProductDispatchEventName::ActionAdmitted,
            ProductDispatchResultCategory::Success,
            None,
        ));
        recorder.emit(event(
            ProductDispatchEventName::RequestDecoded,
            ProductDispatchResultCategory::Success,
            None,
        ));

        assert_eq!(
            recorder.events()[0].name,
            ProductDispatchEventName::RequestDecoded
        );
        assert_eq!(recorder.pressure().dropped_events, 1);
    }

    #[test]
    fn disabled_capture_keeps_only_fixed_aggregate_counters() {
        let recorder = BoundedProductDispatchTelemetry::new(1, false, true).unwrap();
        recorder.emit(event(
            ProductDispatchEventName::ExecutorPanicked,
            ProductDispatchResultCategory::Internal,
            None,
        ));

        assert!(recorder.events().is_empty());
        assert_eq!(recorder.pressure().observed_events, 1);
        assert_eq!(recorder.pressure().unsuccessful_events, 1);
    }
}
