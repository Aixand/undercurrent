// SPDX-License-Identifier: Apache-2.0

//! Bounded payload-free SDK diagnostics and fixed-cardinality metrics.

use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, TryLockError};

use crate::{ClientDiagnosticName, ClientMetricName};

/// One fixed-cardinality SDK metric series.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClientMetricSeries {
    /// Stable metric identity.
    pub name: ClientMetricName,
    /// Finite samples accepted since recorder construction.
    pub samples: u64,
    /// Saturating finite sum of accepted values.
    pub total: f64,
    /// Smallest accepted value.
    pub minimum: f64,
    /// Largest accepted value.
    pub maximum: f64,
    /// Most recently accepted value.
    pub latest: f64,
}

/// Current bounded SDK telemetry pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientTelemetryPressure {
    /// Payload-free diagnostics currently retained.
    pub retained_diagnostics: usize,
    /// Inclusive diagnostic retention bound.
    pub diagnostic_limit: usize,
    /// Diagnostics evicted or skipped on recorder contention.
    pub dropped_diagnostics: u64,
    /// Fixed-cardinality metric series currently populated.
    pub metric_series: usize,
    /// Invalid or contended metric updates that were skipped.
    pub dropped_metric_updates: u64,
}

/// Invalid SDK telemetry retention configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientTelemetryLimitError;

impl Display for ClientTelemetryLimitError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("client telemetry diagnostic limit must be non-zero")
    }
}

impl Error for ClientTelemetryLimitError {}

/// Non-blocking payload-safe SDK observability boundary.
pub trait ClientTelemetry: Send + Sync {
    /// Emits one stable diagnostic without client, stream, or payload labels.
    fn emit_diagnostic(&self, name: ClientDiagnosticName);

    /// Records one stable finite metric observation.
    fn record_metric(&self, name: ClientMetricName, value: f64);
}

/// No-op adapter for explicitly disabled SDK observability.
#[derive(Debug, Default)]
pub struct NoopClientTelemetry;

impl ClientTelemetry for NoopClientTelemetry {
    fn emit_diagnostic(&self, _name: ClientDiagnosticName) {}

    fn record_metric(&self, _name: ClientMetricName, _value: f64) {}
}

/// In-process bounded SDK telemetry recorder used by default clients.
pub struct BoundedClientTelemetry {
    diagnostic_limit: usize,
    diagnostics: Mutex<VecDeque<ClientDiagnosticName>>,
    dropped_diagnostics: AtomicU64,
    metrics: Mutex<BTreeMap<ClientMetricName, ClientMetricValue>>,
    dropped_metric_updates: AtomicU64,
}

impl BoundedClientTelemetry {
    /// Constructs an empty recorder with an explicit diagnostic bound.
    ///
    /// Metric series remain bounded by [`ClientMetricName::ALL`]. Recording
    /// uses non-waiting locks and performs no external IO.
    ///
    /// # Errors
    ///
    /// Returns [`ClientTelemetryLimitError`] when `diagnostic_limit` is zero.
    pub fn new(diagnostic_limit: usize) -> Result<Self, ClientTelemetryLimitError> {
        if diagnostic_limit == 0 {
            return Err(ClientTelemetryLimitError);
        }
        Ok(Self {
            diagnostic_limit,
            diagnostics: Mutex::new(VecDeque::new()),
            dropped_diagnostics: AtomicU64::new(0),
            metrics: Mutex::new(BTreeMap::new()),
            dropped_metric_updates: AtomicU64::new(0),
        })
    }

    /// Returns retained diagnostic names from oldest to newest.
    pub fn diagnostics(&self) -> Vec<ClientDiagnosticName> {
        self.diagnostics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .copied()
            .collect()
    }

    /// Returns populated metric series in registry order.
    pub fn metric_series(&self) -> Vec<ClientMetricSeries> {
        self.metrics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|(name, value)| value.snapshot(*name))
            .collect()
    }

    /// Returns recorder retention and loss pressure.
    pub fn pressure(&self) -> ClientTelemetryPressure {
        ClientTelemetryPressure {
            retained_diagnostics: self
                .diagnostics
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len(),
            diagnostic_limit: self.diagnostic_limit,
            dropped_diagnostics: self.dropped_diagnostics.load(Ordering::Acquire),
            metric_series: self
                .metrics
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len(),
            dropped_metric_updates: self.dropped_metric_updates.load(Ordering::Acquire),
        }
    }
}

impl Default for BoundedClientTelemetry {
    fn default() -> Self {
        Self::new(128).expect("non-zero static client telemetry limit")
    }
}

impl ClientTelemetry for BoundedClientTelemetry {
    fn emit_diagnostic(&self, name: ClientDiagnosticName) {
        let mut diagnostics = match self.diagnostics.try_lock() {
            Ok(diagnostics) => diagnostics,
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => {
                self.dropped_diagnostics.fetch_add(1, Ordering::AcqRel);
                return;
            }
        };
        if diagnostics.len() >= self.diagnostic_limit {
            diagnostics.pop_front();
            self.dropped_diagnostics.fetch_add(1, Ordering::AcqRel);
        }
        diagnostics.push_back(name);
    }

    fn record_metric(&self, name: ClientMetricName, value: f64) {
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
            .entry(name)
            .and_modify(|metric| metric.record(value))
            .or_insert_with(|| ClientMetricValue::new(value));
    }
}

#[derive(Debug, Clone, Copy)]
struct ClientMetricValue {
    samples: u64,
    total: f64,
    minimum: f64,
    maximum: f64,
    latest: f64,
}

impl ClientMetricValue {
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

    fn snapshot(self, name: ClientMetricName) -> ClientMetricSeries {
        ClientMetricSeries {
            name,
            samples: self.samples,
            total: self.total,
            minimum: self.minimum,
            maximum: self.maximum,
            latest: self.latest,
        }
    }
}

fn finite_saturating_add(left: f64, right: f64) -> f64 {
    let sum = left + right;
    if sum.is_finite() {
        sum
    } else if sum.is_sign_negative() {
        -f64::MAX
    } else {
        f64::MAX
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recorder_bounds_diagnostics_and_rejects_non_finite_metrics() {
        let telemetry = BoundedClientTelemetry::new(1).expect("telemetry");
        telemetry.emit_diagnostic(ClientDiagnosticName::RecoveryStarted);
        telemetry.emit_diagnostic(ClientDiagnosticName::RecoverySucceeded);
        telemetry.record_metric(ClientMetricName::ReconnectAttempts, 2.0);
        telemetry.record_metric(ClientMetricName::ReconnectAttempts, f64::NAN);

        assert_eq!(
            telemetry.diagnostics(),
            vec![ClientDiagnosticName::RecoverySucceeded]
        );
        assert_eq!(telemetry.metric_series()[0].total, 2.0);
        assert_eq!(telemetry.pressure().dropped_diagnostics, 1);
        assert_eq!(telemetry.pressure().dropped_metric_updates, 1);
    }
}
