// SPDX-License-Identifier: Apache-2.0

//! Bounded aggregation adapter for transport-owned multiplex metrics.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, TryLockError};

use undercurrent_application::{MetricName, MetricObservation, Telemetry};
use undercurrent_multiplex::{
    MultiplexMetricName, MultiplexMetricObservation, MultiplexTelemetry, TrafficClass,
};

/// Fixed-cardinality aggregate for one multiplex metric and priority class.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MultiplexMetricSeries {
    /// Stable transport metric identity.
    pub name: MultiplexMetricName,
    /// Typed traffic priority when the metric is class-specific.
    pub traffic_class: Option<TrafficClass>,
    /// Finite observations received since daemon start.
    pub samples: u64,
    /// Saturating finite sum of observations.
    pub total: f64,
    /// Smallest observed value.
    pub minimum: f64,
    /// Largest observed value.
    pub maximum: f64,
    /// Most recently observed value.
    pub latest: f64,
}

/// Current multiplex telemetry aggregation pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultiplexTelemetryPressure {
    /// Fixed-cardinality series currently populated.
    pub metric_series: usize,
    /// Updates rejected as non-finite or skipped on lock contention.
    pub dropped_metric_updates: u64,
}

/// Non-blocking fixed-cardinality adapter for every physical scheduler.
pub struct BoundedMultiplexTelemetry {
    metrics: Mutex<BTreeMap<MultiplexMetricKey, MultiplexMetricValue>>,
    dropped_metric_updates: AtomicU64,
    application_telemetry: Arc<dyn Telemetry>,
}

impl BoundedMultiplexTelemetry {
    /// Constructs an empty adapter around the application telemetry boundary.
    ///
    /// The number of possible aggregate series is fixed by the central metric
    /// and traffic-class registries. This function performs no IO and does not
    /// panic.
    pub fn new(application_telemetry: Arc<dyn Telemetry>) -> Self {
        Self {
            metrics: Mutex::new(BTreeMap::new()),
            dropped_metric_updates: AtomicU64::new(0),
            application_telemetry,
        }
    }

    /// Returns populated series in stable registry order.
    pub fn metric_series(&self) -> Vec<MultiplexMetricSeries> {
        self.metrics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|(key, value)| value.snapshot(*key))
            .collect()
    }

    /// Returns bounded series and producer-loss pressure.
    pub fn pressure(&self) -> MultiplexTelemetryPressure {
        MultiplexTelemetryPressure {
            metric_series: self
                .metrics
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len(),
            dropped_metric_updates: self.dropped_metric_updates.load(Ordering::Acquire),
        }
    }
}

impl MultiplexTelemetry for BoundedMultiplexTelemetry {
    fn record_metric(&self, observation: MultiplexMetricObservation) {
        if !observation.value.is_finite() {
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
            .entry(MultiplexMetricKey {
                name: observation.name,
                traffic_class: observation.traffic_class,
            })
            .and_modify(|metric| metric.record(observation.value))
            .or_insert_with(|| MultiplexMetricValue::new(observation.value));
        drop(metrics);

        if let Some(name) = application_metric_name(observation.name) {
            self.application_telemetry.record_metric(MetricObservation {
                name,
                value: observation.value,
                operation: None,
            });
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct MultiplexMetricKey {
    name: MultiplexMetricName,
    traffic_class: Option<TrafficClass>,
}

#[derive(Debug, Clone, Copy)]
struct MultiplexMetricValue {
    samples: u64,
    total: f64,
    minimum: f64,
    maximum: f64,
    latest: f64,
}

impl MultiplexMetricValue {
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

    fn snapshot(self, key: MultiplexMetricKey) -> MultiplexMetricSeries {
        MultiplexMetricSeries {
            name: key.name,
            traffic_class: key.traffic_class,
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

const fn application_metric_name(name: MultiplexMetricName) -> Option<MetricName> {
    match name {
        MultiplexMetricName::PhysicalConnections => Some(MetricName::PhysicalConnections),
        MultiplexMetricName::PendingRequests => Some(MetricName::PendingRequests),
        MultiplexMetricName::QueuedBytes => Some(MetricName::ConnectionQueuedBytes),
        MultiplexMetricName::QueuedMessages => Some(MetricName::ConnectionQueuedMessages),
        MultiplexMetricName::CoalescedItems => Some(MetricName::StreamItemsCoalesced),
        MultiplexMetricName::DroppedItems => Some(MetricName::StreamItemsDropped),
        MultiplexMetricName::ReplayedItems => Some(MetricName::StreamItemsReplayed),
        MultiplexMetricName::ReportedGaps => Some(MetricName::StreamGapsReported),
        MultiplexMetricName::ConnectionCreditBytes => Some(MetricName::ConnectionCreditBytes),
        MultiplexMetricName::ConnectionWriteDurationSeconds => {
            Some(MetricName::ConnectionWriteDurationSeconds)
        }
        MultiplexMetricName::StreamLagItems => Some(MetricName::StreamLagItems),
        MultiplexMetricName::LogicalStreams => None,
    }
}

#[cfg(test)]
mod tests {
    use undercurrent_runtime::BoundedTelemetryRecorder;

    use super::*;

    #[test]
    fn typed_series_are_finite_bounded_and_fan_out_registered_metrics() {
        let application = Arc::new(BoundedTelemetryRecorder::new(1, 4).expect("telemetry"));
        let telemetry = BoundedMultiplexTelemetry::new(application.clone());
        telemetry.record_metric(MultiplexMetricObservation {
            name: MultiplexMetricName::QueuedBytes,
            value: 32.0,
            traffic_class: Some(TrafficClass::Bulk),
        });
        telemetry.record_metric(MultiplexMetricObservation {
            name: MultiplexMetricName::QueuedBytes,
            value: 0.0,
            traffic_class: Some(TrafficClass::Bulk),
        });
        telemetry.record_metric(MultiplexMetricObservation {
            name: MultiplexMetricName::QueuedBytes,
            value: f64::NAN,
            traffic_class: Some(TrafficClass::Bulk),
        });
        telemetry.record_metric(MultiplexMetricObservation {
            name: MultiplexMetricName::StreamLagItems,
            value: 3.0,
            traffic_class: None,
        });

        assert_eq!(telemetry.pressure().metric_series, 2);
        assert_eq!(telemetry.pressure().dropped_metric_updates, 1);
        assert_eq!(telemetry.metric_series()[0].maximum, 32.0);
        assert_eq!(telemetry.metric_series()[0].latest, 0.0);
        assert!(application.metric_aggregates().iter().any(|aggregate| {
            aggregate.name == MetricName::ConnectionQueuedBytes && aggregate.samples == 2
        }));
        assert!(application.metric_aggregates().iter().any(|aggregate| {
            aggregate.name == MetricName::StreamLagItems && aggregate.latest == 3.0
        }));
    }
}
