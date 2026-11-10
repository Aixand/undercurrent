// SPDX-License-Identifier: Apache-2.0

//! Bounded payload-safe diagnostic and metric recorder.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, TryLockError};

use undercurrent_application::{
    ApplicationOperation, DiagnosticRecord, MetricName, MetricObservation,
    SessionMetricObservation, Telemetry,
};
use undercurrent_domain::SessionId;

use crate::RuntimeLimitError;

/// Bounded telemetry recorder pressure and loss accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TelemetryPressure {
    /// Retained diagnostic records.
    pub diagnostics: usize,
    /// Retained metric observations.
    pub metrics: usize,
    /// Fixed-cardinality metric aggregate series.
    pub aggregate_series: usize,
    /// Diagnostic records evicted from retention or dropped on producer lock contention.
    pub dropped_diagnostics: u64,
    /// Metric observations evicted, rejected as non-finite, or lost to contention.
    pub dropped_metrics: u64,
}

/// Fixed-cardinality aggregate for one metric and optional typed operation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MetricAggregate {
    /// Stable metric identity.
    pub name: MetricName,
    /// Optional stable application operation dimension.
    pub operation: Option<ApplicationOperation>,
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

/// In-process telemetry sink for diagnostics, debug views, and tests.
pub struct BoundedTelemetryRecorder {
    diagnostic_limit: usize,
    metric_limit: usize,
    state: Mutex<TelemetryState>,
    dropped_diagnostics: AtomicU64,
    dropped_metrics: AtomicU64,
}

impl BoundedTelemetryRecorder {
    /// Constructs an empty recorder with independent hard record limits.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed zero-limit failure.
    pub fn new(diagnostic_limit: usize, metric_limit: usize) -> Result<Self, RuntimeLimitError> {
        if diagnostic_limit == 0 {
            return Err(RuntimeLimitError::zero("diagnostic_records"));
        }
        if metric_limit == 0 {
            return Err(RuntimeLimitError::zero("metric_observations"));
        }
        Ok(Self {
            diagnostic_limit,
            metric_limit,
            state: Mutex::new(TelemetryState::default()),
            dropped_diagnostics: AtomicU64::new(0),
            dropped_metrics: AtomicU64::new(0),
        })
    }

    /// Returns a bounded oldest-to-newest diagnostic snapshot.
    pub fn diagnostics(&self) -> Vec<DiagnosticRecord> {
        self.lock_state().diagnostics.iter().cloned().collect()
    }

    /// Returns a bounded oldest-to-newest metric snapshot.
    pub fn metrics(&self) -> Vec<MetricObservation> {
        self.lock_state()
            .metrics
            .iter()
            .map(|retained| retained.observation)
            .collect()
    }

    /// Returns bounded exact-session metrics in oldest-to-newest order.
    pub fn session_metrics(&self) -> Vec<SessionMetricObservation> {
        self.lock_state()
            .metrics
            .iter()
            .filter_map(|retained| {
                retained.session_id.as_ref().map(|session_id| {
                    SessionMetricObservation::new(session_id.clone(), retained.observation)
                })
            })
            .collect()
    }

    /// Returns every observed fixed-cardinality aggregate in registry order.
    pub fn metric_aggregates(&self) -> Vec<MetricAggregate> {
        self.lock_state()
            .aggregates
            .iter()
            .map(|(key, value)| value.snapshot(*key))
            .collect()
    }

    /// Returns retained counts and monotonic eviction or contention loss totals.
    pub fn pressure(&self) -> TelemetryPressure {
        let state = self.lock_state();
        TelemetryPressure {
            diagnostics: state.diagnostics.len(),
            metrics: state.metrics.len(),
            aggregate_series: state.aggregates.len(),
            dropped_diagnostics: self.dropped_diagnostics.load(Ordering::Acquire),
            dropped_metrics: self.dropped_metrics.load(Ordering::Acquire),
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, TelemetryState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn try_lock_state(&self) -> Option<std::sync::MutexGuard<'_, TelemetryState>> {
        // Inspection uses the same mutex. Producers drop and count a sample on
        // contention so reading diagnostics cannot make supervised work wait.
        // See [coordination guide](../docs/internals/coordination/scheduling.md#observe-pressure-without-blocking-producers).
        match self.state.try_lock() {
            Ok(state) => Some(state),
            Err(TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
            Err(TryLockError::WouldBlock) => None,
        }
    }

    fn record_metric_inner(&self, observation: MetricObservation, session_id: Option<SessionId>) {
        if !observation.value.is_finite() {
            self.dropped_metrics.fetch_add(1, Ordering::AcqRel);
            return;
        }
        let Some(mut state) = self.try_lock_state() else {
            self.dropped_metrics.fetch_add(1, Ordering::AcqRel);
            return;
        };
        if state.metrics.len() >= self.metric_limit {
            state.metrics.pop_front();
            self.dropped_metrics.fetch_add(1, Ordering::AcqRel);
        }
        state.metrics.push_back(RetainedMetricObservation {
            observation,
            session_id,
        });
        state
            .aggregates
            .entry(MetricAggregateKey {
                name: observation.name,
                operation: observation.operation,
            })
            .and_modify(|aggregate| aggregate.record(observation.value))
            .or_insert_with(|| MetricAggregateValue::new(observation.value));
    }
}

impl Telemetry for BoundedTelemetryRecorder {
    fn emit_diagnostic(&self, record: &DiagnosticRecord) {
        let Some(mut state) = self.try_lock_state() else {
            self.dropped_diagnostics.fetch_add(1, Ordering::AcqRel);
            return;
        };
        if state.diagnostics.len() >= self.diagnostic_limit {
            state.diagnostics.pop_front();
            self.dropped_diagnostics.fetch_add(1, Ordering::AcqRel);
        }
        state.diagnostics.push_back(record.clone());
    }

    fn record_metric(&self, observation: MetricObservation) {
        self.record_metric_inner(observation, None);
    }

    fn record_session_metric(&self, observation: &SessionMetricObservation) {
        self.record_metric_inner(observation.metric(), Some(observation.session_id().clone()));
    }
}

struct RetainedMetricObservation {
    observation: MetricObservation,
    session_id: Option<SessionId>,
}

#[derive(Default)]
struct TelemetryState {
    diagnostics: VecDeque<DiagnosticRecord>,
    metrics: VecDeque<RetainedMetricObservation>,
    aggregates: BTreeMap<MetricAggregateKey, MetricAggregateValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct MetricAggregateKey {
    name: MetricName,
    operation: Option<ApplicationOperation>,
}

#[derive(Debug, Clone, Copy)]
struct MetricAggregateValue {
    samples: u64,
    total: f64,
    minimum: f64,
    maximum: f64,
    latest: f64,
}

impl MetricAggregateValue {
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

    fn snapshot(self, key: MetricAggregateKey) -> MetricAggregate {
        MetricAggregate {
            name: key.name,
            operation: key.operation,
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

#[cfg(test)]
mod tests {
    use super::*;
    use undercurrent_application::{DiagnosticEventName, DiagnosticLevel, MetricName};
    use undercurrent_domain::{SessionId, TimestampMillis};

    #[test]
    fn producer_paths_drop_and_count_instead_of_waiting_for_inspection() {
        let recorder = BoundedTelemetryRecorder::new(1, 1).expect("recorder");
        let state = recorder.state.lock().expect("state lock");

        recorder.emit_diagnostic(&DiagnosticRecord {
            event: DiagnosticEventName::SessionCreated,
            level: DiagnosticLevel::Info,
            timestamp: TimestampMillis::new(1),
            session_id: None,
            application_operation: None,
            application_error: None,
            operation: None,
            error_kind: None,
        });
        recorder.record_metric(MetricObservation {
            name: MetricName::OperationDurationSeconds,
            value: 1.0,
            operation: None,
        });

        drop(state);
        assert_eq!(
            recorder.pressure(),
            TelemetryPressure {
                diagnostics: 0,
                metrics: 0,
                aggregate_series: 0,
                dropped_diagnostics: 1,
                dropped_metrics: 1,
            }
        );
    }

    #[test]
    fn metric_aggregates_are_typed_finite_and_independent_of_raw_eviction() {
        let recorder = BoundedTelemetryRecorder::new(1, 1).expect("recorder");
        for value in [1.0, 3.0, 2.0] {
            recorder.record_metric(MetricObservation {
                name: MetricName::OperationDurationSeconds,
                value,
                operation: Some(ApplicationOperation::ListSessions),
            });
        }
        recorder.record_metric(MetricObservation {
            name: MetricName::OperationDurationSeconds,
            value: f64::NAN,
            operation: Some(ApplicationOperation::ListSessions),
        });

        assert_eq!(recorder.metrics().len(), 1);
        assert_eq!(
            recorder.metric_aggregates(),
            [MetricAggregate {
                name: MetricName::OperationDurationSeconds,
                operation: Some(ApplicationOperation::ListSessions),
                samples: 3,
                total: 6.0,
                minimum: 1.0,
                maximum: 3.0,
                latest: 2.0,
            }]
        );
        assert_eq!(recorder.pressure().aggregate_series, 1);
        assert_eq!(recorder.pressure().dropped_metrics, 3);
    }

    #[test]
    fn session_metric_retains_exact_identity_under_the_shared_bound() {
        let recorder = BoundedTelemetryRecorder::new(1, 2).expect("recorder");
        let session_id = SessionId::new("session-metric").expect("session id");
        recorder.record_session_metric(&SessionMetricObservation::new(
            session_id.clone(),
            MetricObservation {
                name: MetricName::TerminalOutputBytesAppended,
                value: 4096.0,
                operation: None,
            },
        ));

        assert_eq!(recorder.metrics().len(), 1);
        assert_eq!(recorder.session_metrics().len(), 1);
        assert_eq!(recorder.session_metrics()[0].session_id(), &session_id);
        assert_eq!(
            recorder.session_metrics()[0].metric().name,
            MetricName::TerminalOutputBytesAppended
        );
        assert_eq!(recorder.pressure().metrics, 1);
    }
}
