// SPDX-License-Identifier: Apache-2.0

//! Central multiplex diagnostic and metric name registries.
//!
//! Registry membership does not imply emission by every component. The scheduler
//! emits queue, stream, gap, and credit observations; outer adapters supply
//! connection, request, replay, and write-duration observations. The telemetry
//! trait exposes metrics only, not a diagnostic event callback.

use crate::TrafficClass;

/// Stable multiplex diagnostic events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MultiplexDiagnosticName {
    /// Physical connection was negotiated and opened.
    ConnectionOpened,
    /// Physical connection teardown released bounded state.
    ConnectionClosed,
    /// Logical stream was registered.
    StreamOpened,
    /// Logical stream was removed independently.
    StreamClosed,
    /// Queue admission rejected a reliable item.
    QueueSaturated,
    /// Replaceable state superseded an older queued item.
    ItemCoalesced,
    /// Droppable stream data produced a visible gap.
    ItemDropped,
    /// Retained stream items satisfied a resume request.
    StreamReplayed,
    /// Resume required an explicit recovery action.
    ReplayGapReported,
    /// Flow-control credit prevented one route from writing.
    CreditBlocked,
}

impl MultiplexDiagnosticName {
    /// Complete diagnostic registry.
    pub const ALL: [Self; 10] = [
        Self::ConnectionOpened,
        Self::ConnectionClosed,
        Self::StreamOpened,
        Self::StreamClosed,
        Self::QueueSaturated,
        Self::ItemCoalesced,
        Self::ItemDropped,
        Self::StreamReplayed,
        Self::ReplayGapReported,
        Self::CreditBlocked,
    ];

    /// Returns the stable snake-case event name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConnectionOpened => "connection_opened",
            Self::ConnectionClosed => "connection_closed",
            Self::StreamOpened => "stream_opened",
            Self::StreamClosed => "stream_closed",
            Self::QueueSaturated => "queue_saturated",
            Self::ItemCoalesced => "item_coalesced",
            Self::ItemDropped => "item_dropped",
            Self::StreamReplayed => "stream_replayed",
            Self::ReplayGapReported => "replay_gap_reported",
            Self::CreditBlocked => "credit_blocked",
        }
    }
}

/// Stable multiplex metric names without high-cardinality labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MultiplexMetricName {
    /// Active physical connections.
    PhysicalConnections,
    /// Active logical streams.
    LogicalStreams,
    /// Pending finite requests.
    PendingRequests,
    /// Exact outbound encoded bytes retained by a traffic class.
    QueuedBytes,
    /// Outbound messages retained by a traffic class.
    QueuedMessages,
    /// Replaceable items removed before writing.
    CoalescedItems,
    /// Droppable stream frames not retained.
    DroppedItems,
    /// Retained frames returned for resume.
    ReplayedItems,
    /// Typed replay or live gaps reported.
    ReportedGaps,
    /// Current connection-wide flow credit.
    ConnectionCreditBytes,
    /// Time spent writing one selected frame.
    ConnectionWriteDurationSeconds,
    /// Queued items and pending gap for one sampled logical stream.
    StreamLagItems,
}

impl MultiplexMetricName {
    /// Complete metric registry.
    pub const ALL: [Self; 12] = [
        Self::PhysicalConnections,
        Self::LogicalStreams,
        Self::PendingRequests,
        Self::QueuedBytes,
        Self::QueuedMessages,
        Self::CoalescedItems,
        Self::DroppedItems,
        Self::ReplayedItems,
        Self::ReportedGaps,
        Self::ConnectionCreditBytes,
        Self::ConnectionWriteDurationSeconds,
        Self::StreamLagItems,
    ];

    /// Returns the stable snake-case metric name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PhysicalConnections => "physical_connections",
            Self::LogicalStreams => "logical_streams",
            Self::PendingRequests => "pending_requests",
            Self::QueuedBytes => "queued_bytes",
            Self::QueuedMessages => "queued_messages",
            Self::CoalescedItems => "coalesced_items",
            Self::DroppedItems => "dropped_items",
            Self::ReplayedItems => "replayed_items",
            Self::ReportedGaps => "reported_gaps",
            Self::ConnectionCreditBytes => "connection_credit_bytes",
            Self::ConnectionWriteDurationSeconds => "connection_write_duration_seconds",
            Self::StreamLagItems => "stream_lag_items",
        }
    }
}

/// One fixed-cardinality multiplex metric observation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MultiplexMetricObservation {
    /// Stable metric identity.
    pub name: MultiplexMetricName,
    /// Numeric counter increment or gauge value.
    pub value: f64,
    /// Priority-class dimension when the metric is class-specific.
    pub traffic_class: Option<TrafficClass>,
}

/// Telemetry boundary supplied by transport adapters, which must return promptly.
///
/// Scheduler callbacks run synchronously after releasing its state lock. The
/// trait does not enforce nonblocking behavior or isolate callback panics. Values
/// from concurrent producers can arrive out of order, and stream-lag samples
/// deliberately carry no stream identifier.
pub trait MultiplexTelemetry: Send + Sync {
    /// Records one payload-free, fixed-cardinality metric observation.
    fn record_metric(&self, observation: MultiplexMetricObservation);
}

/// No-op adapter used by standalone schedulers and tests without telemetry.
#[derive(Debug, Default)]
pub struct NoopMultiplexTelemetry;

impl MultiplexTelemetry for NoopMultiplexTelemetry {
    fn record_metric(&self, _observation: MultiplexMetricObservation) {}
}
