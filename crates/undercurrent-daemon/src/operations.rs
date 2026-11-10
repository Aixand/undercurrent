// SPDX-License-Identifier: Apache-2.0

//! Bounded daemon status, diagnostics, and graceful-shutdown control plane.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

use undercurrent_application::{
    AuthorizedDiagnosticBundleRequest, Clock, OperationAttemptStore, ProcessHostShutdown,
    ShutdownProcessHostCommand, ShutdownProcessHostError, ShutdownProcessHostService,
};
use undercurrent_domain::{OperationAttemptId, PayloadDigest, ProcessMutationKind};
use undercurrent_wire::{ClientId, ServerEpoch};

const DIAGNOSTIC_TOKEN_PREFIX: char = 'd';
const DIAGNOSTIC_TOKEN_HEX_BYTES: usize = 16;
const DIAGNOSTIC_TOKEN_BYTES: usize = 1 + DIAGNOSTIC_TOKEN_HEX_BYTES;
/// Hard bound for explicitly authorized terminal content in one diagnostic bundle.
pub const DIAGNOSTIC_BUNDLE_TERMINAL_CONTENT_BYTES: usize = 64 * 1_024;

/// Stable daemon diagnostic sections in public presentation order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DaemonDiagnosticSection {
    /// Physical connections and their negotiated resource bounds.
    Connections,
    /// Logical stream registrations and shared stream-pump pressure.
    Streams,
    /// Consumers currently delayed by bounded downstream capacity.
    SlowConsumers,
    /// Terminal-output producers and retained output pressure.
    OutputProducers,
    /// Dispatcher, process-supervisor, and background worker queues.
    SupervisorQueues,
    /// Payload-free foreground evidence and decision counts.
    DetectionEvidence,
    /// Durable repository, outbox, event-journal, and telemetry-retention state.
    Persistence,
    /// Payload-safe startup recovery outcomes retained for this daemon epoch.
    Reconciliation,
    /// Effective non-secret configuration and configured limits.
    Configuration,
}

impl DaemonDiagnosticSection {
    /// Complete section registry in stable presentation order.
    pub const ALL: [Self; 9] = [
        Self::Connections,
        Self::Streams,
        Self::SlowConsumers,
        Self::OutputProducers,
        Self::SupervisorQueues,
        Self::DetectionEvidence,
        Self::Persistence,
        Self::Reconciliation,
        Self::Configuration,
    ];

    /// Returns the stable snake-case section name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Connections => "connections",
            Self::Streams => "streams",
            Self::SlowConsumers => "slow_consumers",
            Self::OutputProducers => "output_producers",
            Self::SupervisorQueues => "supervisor_queues",
            Self::DetectionEvidence => "detection_evidence",
            Self::Persistence => "persistence",
            Self::Reconciliation => "reconciliation",
            Self::Configuration => "configuration",
        }
    }
}

/// Central registry of safe scalar field names used by daemon diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DaemonDiagnosticField {
    /// Whether new work is currently accepted.
    Accepting,
    /// Current active item count.
    Active,
    /// Current backpressured item count.
    Backpressured,
    /// Current logical byte count.
    Bytes,
    /// Inclusive logical byte bound.
    ByteLimit,
    /// Current logical client count.
    Clients,
    /// Inclusive logical client bound.
    ClientLimit,
    /// Whether the component has closed.
    Closed,
    /// Current physical connection count.
    Connections,
    /// Inclusive physical connection bound.
    ConnectionLimit,
    /// Generic current item count when no narrower field applies.
    Count,
    /// Current retained cursor count.
    Cursors,
    /// Inclusive retained cursor bound.
    CursorLimit,
    /// Current dirty or ready-to-recompute count.
    Dirty,
    /// Cumulative dropped item count.
    Dropped,
    /// Saturating accumulated operation duration in microseconds.
    DurationMicros,
    /// Cumulative oldest-first eviction count.
    Evictions,
    /// Cumulative failed operation count.
    Failed,
    /// Current retained item count.
    Items,
    /// Inclusive retained item bound.
    ItemLimit,
    /// Generic inclusive bound when no narrower field applies.
    Limit,
    /// Largest observed operation duration in microseconds.
    MaximumDurationMicros,
    /// Current pending work count.
    Pending,
    /// Current work still preparing for activation.
    Preparing,
    /// Current queued work count.
    Queued,
    /// Current work ready for bounded processing.
    Ready,
    /// Number of observations contributing to an aggregate measurement.
    Samples,
    /// Current monotonic state revision.
    Revision,
    /// Current managed session count.
    Sessions,
    /// Current logical stream count.
    Streams,
    /// Current worker count.
    Workers,
    /// Whether a session-local supervisor lease is currently held.
    Held,
    /// Current session-local supervisor waiter count.
    WaitingCallers,
    /// Whether the next session-local command would exceed its queue bound.
    AtLimit,
    /// Independent foreground axis.
    ForegroundAxis,
    /// Committed execution state.
    Execution,
    /// Committed interaction state.
    Interaction,
    /// Mechanical interaction modality.
    InteractionModality,
    /// Committed runtime-constraint state.
    Constraint,
    /// Profile-owned namespaced observation semantic.
    Semantic,
    /// Foreground-axis confidence in basis points.
    ConfidenceBasisPoints,
    /// Stable foreground evidence source.
    EvidenceSource,
    /// Semantic foreground evidence priority.
    EvidencePriority,
    /// Affirmative, inferred, or unavailable evidence disposition.
    EvidenceDisposition,
    /// Selected or typed ignored evidence assessment.
    EvidenceAssessment,
    /// Evidence observation time in Unix epoch milliseconds.
    ObservedAtMillis,
    /// Decision commit time in Unix epoch milliseconds.
    CommittedAtMillis,
    /// Stable logical client identity.
    ClientId,
    /// Stable physical connection identity.
    ConnectionId,
    /// Stable logical stream identity.
    StreamId,
    /// Stable managed session identity.
    SessionId,
    /// Finite request identity associated with pending work.
    RequestId,
    /// Server process epoch governing resume validity.
    ServerEpoch,
    /// Negotiated Undercurrent connection protocol version.
    ProtocolVersion,
    /// Negotiated maximum encoded frame bytes.
    MaxFrameBytes,
    /// Negotiated maximum pending finite requests.
    MaxPendingRequests,
    /// Current queued encoded message count.
    QueuedMessages,
    /// Inclusive encoded-byte queue bound for one logical stream.
    QueueByteLimit,
    /// Inclusive queued-message bound for one logical stream.
    QueueMessageLimit,
    /// Current flow-control credit bytes.
    CreditBytes,
    /// Current or configured stream kind.
    StreamKind,
    /// Semantic outbound priority class.
    TrafficClass,
    /// Cumulative coalesced state item count.
    Coalesced,
    /// Whether a visible stream gap is pending.
    GapPending,
    /// Negotiated resume retention in milliseconds, or zero when disabled.
    ResumeRetentionMillis,
    /// Latest accepted logical stream sequence.
    StreamSequence,
    /// Current exclusive terminal output offset.
    OutputOffset,
    /// Stable metric registry name.
    MetricName,
    /// Fixed-cardinality metric series currently populated.
    MetricSeries,
    /// Stable application operation dimension.
    Operation,
    /// Stable terminal RPC result dimension.
    Result,
    /// Aggregate metric sum as a canonical finite decimal.
    MetricTotal,
    /// Aggregate metric minimum as a canonical finite decimal.
    MetricMinimum,
    /// Aggregate metric maximum as a canonical finite decimal.
    MetricMaximum,
    /// Latest metric value as a canonical finite decimal.
    MetricLatest,
    /// Whether a complete bounded scan reached its source end.
    ScanCompleted,
    /// Durable terminal sessions requiring no recovery action.
    TerminalUnchanged,
    /// Live process bindings restored during startup.
    RunningRebound,
    /// Starting sessions advanced after liveness confirmation.
    StartingCompleted,
    /// Pending termination flows resumed during startup.
    StoppingResumed,
    /// Unknown cancellation effects preserved without process redrive during startup.
    StoppingUnknownPreserved,
    /// Missing or exited processes settled durably during startup.
    ProcessExitsSettled,
    /// Durable restart jobs restored into the live scheduler.
    RestartJobsReenqueued,
    /// Sessions that could not be resolved during startup.
    Unresolved,
}

impl DaemonDiagnosticField {
    /// Complete diagnostic field registry in declaration order.
    pub const ALL: [Self; 84] = [
        Self::Accepting,
        Self::Active,
        Self::Backpressured,
        Self::Bytes,
        Self::ByteLimit,
        Self::Clients,
        Self::ClientLimit,
        Self::Closed,
        Self::Connections,
        Self::ConnectionLimit,
        Self::Count,
        Self::Cursors,
        Self::CursorLimit,
        Self::Dirty,
        Self::Dropped,
        Self::DurationMicros,
        Self::Evictions,
        Self::Failed,
        Self::Items,
        Self::ItemLimit,
        Self::Limit,
        Self::MaximumDurationMicros,
        Self::Pending,
        Self::Preparing,
        Self::Queued,
        Self::Ready,
        Self::Samples,
        Self::Revision,
        Self::Sessions,
        Self::Streams,
        Self::Workers,
        Self::Held,
        Self::WaitingCallers,
        Self::AtLimit,
        Self::ForegroundAxis,
        Self::Execution,
        Self::Interaction,
        Self::InteractionModality,
        Self::Constraint,
        Self::Semantic,
        Self::ConfidenceBasisPoints,
        Self::EvidenceSource,
        Self::EvidencePriority,
        Self::EvidenceDisposition,
        Self::EvidenceAssessment,
        Self::ObservedAtMillis,
        Self::CommittedAtMillis,
        Self::ClientId,
        Self::ConnectionId,
        Self::StreamId,
        Self::SessionId,
        Self::RequestId,
        Self::ServerEpoch,
        Self::ProtocolVersion,
        Self::MaxFrameBytes,
        Self::MaxPendingRequests,
        Self::QueuedMessages,
        Self::QueueByteLimit,
        Self::QueueMessageLimit,
        Self::CreditBytes,
        Self::StreamKind,
        Self::TrafficClass,
        Self::Coalesced,
        Self::GapPending,
        Self::ResumeRetentionMillis,
        Self::StreamSequence,
        Self::OutputOffset,
        Self::MetricName,
        Self::MetricSeries,
        Self::Operation,
        Self::Result,
        Self::MetricTotal,
        Self::MetricMinimum,
        Self::MetricMaximum,
        Self::MetricLatest,
        Self::ScanCompleted,
        Self::TerminalUnchanged,
        Self::RunningRebound,
        Self::StartingCompleted,
        Self::StoppingResumed,
        Self::StoppingUnknownPreserved,
        Self::ProcessExitsSettled,
        Self::RestartJobsReenqueued,
        Self::Unresolved,
    ];

    /// Returns the stable snake-case field name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepting => "accepting",
            Self::Active => "active",
            Self::Backpressured => "backpressured",
            Self::Bytes => "bytes",
            Self::ByteLimit => "byte_limit",
            Self::Clients => "clients",
            Self::ClientLimit => "client_limit",
            Self::Closed => "closed",
            Self::Connections => "connections",
            Self::ConnectionLimit => "connection_limit",
            Self::Count => "count",
            Self::Cursors => "cursors",
            Self::CursorLimit => "cursor_limit",
            Self::Dirty => "dirty",
            Self::Dropped => "dropped",
            Self::DurationMicros => "duration_micros",
            Self::Evictions => "evictions",
            Self::Failed => "failed",
            Self::Items => "items",
            Self::ItemLimit => "item_limit",
            Self::Limit => "limit",
            Self::MaximumDurationMicros => "maximum_duration_micros",
            Self::Pending => "pending",
            Self::Preparing => "preparing",
            Self::Queued => "queued",
            Self::Ready => "ready",
            Self::Samples => "samples",
            Self::Revision => "revision",
            Self::Sessions => "sessions",
            Self::Streams => "streams",
            Self::Workers => "workers",
            Self::Held => "held",
            Self::WaitingCallers => "waiting_callers",
            Self::AtLimit => "at_limit",
            Self::ForegroundAxis => "foreground_axis",
            Self::Execution => "execution",
            Self::Interaction => "interaction",
            Self::InteractionModality => "interaction_modality",
            Self::Constraint => "constraint",
            Self::Semantic => "semantic",
            Self::ConfidenceBasisPoints => "confidence_basis_points",
            Self::EvidenceSource => "evidence_source",
            Self::EvidencePriority => "evidence_priority",
            Self::EvidenceDisposition => "evidence_disposition",
            Self::EvidenceAssessment => "evidence_assessment",
            Self::ObservedAtMillis => "observed_at_millis",
            Self::CommittedAtMillis => "committed_at_millis",
            Self::ClientId => "client_id",
            Self::ConnectionId => "connection_id",
            Self::StreamId => "stream_id",
            Self::SessionId => "session_id",
            Self::RequestId => "request_id",
            Self::ServerEpoch => "server_epoch",
            Self::ProtocolVersion => "protocol_version",
            Self::MaxFrameBytes => "max_frame_bytes",
            Self::MaxPendingRequests => "max_pending_requests",
            Self::QueuedMessages => "queued_messages",
            Self::QueueByteLimit => "queue_byte_limit",
            Self::QueueMessageLimit => "queue_message_limit",
            Self::CreditBytes => "credit_bytes",
            Self::StreamKind => "stream_kind",
            Self::TrafficClass => "traffic_class",
            Self::Coalesced => "coalesced",
            Self::GapPending => "gap_pending",
            Self::ResumeRetentionMillis => "resume_retention_millis",
            Self::StreamSequence => "stream_sequence",
            Self::OutputOffset => "output_offset",
            Self::MetricName => "metric_name",
            Self::MetricSeries => "metric_series",
            Self::Operation => "operation",
            Self::Result => "result",
            Self::MetricTotal => "metric_total",
            Self::MetricMinimum => "metric_minimum",
            Self::MetricMaximum => "metric_maximum",
            Self::MetricLatest => "metric_latest",
            Self::ScanCompleted => "scan_completed",
            Self::TerminalUnchanged => "terminal_unchanged",
            Self::RunningRebound => "running_rebound",
            Self::StartingCompleted => "starting_completed",
            Self::StoppingResumed => "stopping_resumed",
            Self::StoppingUnknownPreserved => "stopping_unknown_preserved",
            Self::ProcessExitsSettled => "process_exits_settled",
            Self::RestartJobsReenqueued => "restart_jobs_reenqueued",
            Self::Unresolved => "unresolved",
        }
    }
}

/// Bounded non-secret diagnostic scalar retained by the control plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DaemonDiagnosticValue {
    /// Boolean state or decision.
    Boolean(bool),
    /// Non-negative count, limit, duration, or revision.
    Unsigned(u64),
    /// Signed timestamp or value.
    Signed(i64),
    /// Bounded payload-safe label or state name.
    Text(String),
}

/// One payload-safe daemon diagnostic row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonDiagnosticRow {
    /// Stable diagnostic section.
    pub section: DaemonDiagnosticSection,
    /// Payload-safe stable identity within the section.
    pub identity: String,
    /// Typed fields keyed exclusively by the central field registry.
    pub fields: BTreeMap<DaemonDiagnosticField, DaemonDiagnosticValue>,
}

/// Aggregate state needed by the compact daemon status operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DaemonOperationalPressure {
    /// Whether the listener currently admits new physical connections.
    pub accepting_connections: bool,
    /// Current physical connection count.
    pub physical_connections: usize,
    /// Current logical stream count across physical connections.
    pub logical_streams: usize,
    /// Current durable managed-session count.
    pub managed_sessions: usize,
}

/// Complete compact daemon status before protocol presentation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonStatusSnapshot {
    /// Stable process epoch invalidating replay across restarts.
    pub server_epoch: ServerEpoch,
    /// Daemon start time as Unix epoch milliseconds.
    pub started_at_millis: i64,
    /// Whether new physical connections are admitted.
    pub accepting_connections: bool,
    /// Current physical connection count.
    pub physical_connections: usize,
    /// Current logical stream count.
    pub logical_streams: usize,
    /// Current durable managed-session count.
    pub managed_sessions: usize,
}

/// One stable diagnostic query independent of protocol DTOs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonDiagnosticQuery {
    /// Selected sections; an empty set selects every registered section.
    pub sections: BTreeSet<DaemonDiagnosticSection>,
    /// Maximum rows to return in this page.
    pub page_size: usize,
    /// Client-owned opaque continuation from a prior captured snapshot.
    pub cursor: Option<String>,
}

/// One captured page from a bounded, point-in-time diagnostic snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonDiagnosticPage {
    /// Bounded rows in stable section and identity order.
    pub items: Vec<DaemonDiagnosticRow>,
    /// Opaque continuation when the captured snapshot has more rows.
    pub next_cursor: Option<String>,
    /// Time at which the first page captured its snapshot.
    pub captured_at_millis: i64,
}

/// One bounded diagnostic export with terminal content redacted by default.
#[derive(Clone, PartialEq, Eq)]
pub struct DaemonDiagnosticBundle {
    items: Vec<DaemonDiagnosticRow>,
    captured_at_millis: i64,
    terminal_content: Option<Arc<[u8]>>,
    terminal_content_redacted: bool,
}

impl DaemonDiagnosticBundle {
    /// Returns payload-safe rows in stable section and identity order.
    pub fn items(&self) -> &[DaemonDiagnosticRow] {
        &self.items
    }

    /// Returns the common capture time for this point-in-time bundle.
    pub const fn captured_at_millis(&self) -> i64 {
        self.captured_at_millis
    }

    /// Returns terminal content only after an explicit authorized detail request.
    pub fn terminal_content(&self) -> Option<&[u8]> {
        self.terminal_content.as_deref()
    }

    /// Returns whether terminal content was deliberately omitted.
    pub const fn terminal_content_redacted(&self) -> bool {
        self.terminal_content_redacted
    }
}

impl Debug for DaemonDiagnosticBundle {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonDiagnosticBundle")
            .field("item_count", &self.items.len())
            .field("captured_at_millis", &self.captured_at_millis)
            .field(
                "terminal_content_bytes",
                &self.terminal_content.as_ref().map(|content| content.len()),
            )
            .field("terminal_content_redacted", &self.terminal_content_redacted)
            .finish()
    }
}

/// Validated bounds for daemon diagnostic retention and shutdown admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DaemonOperationsLimits {
    clients: usize,
    cursors_per_client: usize,
    maximum_page_size: usize,
    maximum_snapshot_rows: usize,
    maximum_drain_timeout: Duration,
}

impl DaemonOperationsLimits {
    /// Validates every control-plane memory and duration bound.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns the first zero or internally inconsistent limit.
    pub const fn new(
        clients: usize,
        cursors_per_client: usize,
        maximum_page_size: usize,
        maximum_snapshot_rows: usize,
        maximum_drain_timeout: Duration,
    ) -> Result<Self, DaemonOperationsError> {
        if clients == 0 {
            return Err(DaemonOperationsError::InvalidLimit { field: "clients" });
        }
        if cursors_per_client == 0 {
            return Err(DaemonOperationsError::InvalidLimit {
                field: "cursors_per_client",
            });
        }
        if maximum_page_size == 0 {
            return Err(DaemonOperationsError::InvalidLimit {
                field: "maximum_page_size",
            });
        }
        if maximum_snapshot_rows < maximum_page_size {
            return Err(DaemonOperationsError::InvalidLimit {
                field: "maximum_snapshot_rows",
            });
        }
        if maximum_drain_timeout.is_zero() {
            return Err(DaemonOperationsError::InvalidLimit {
                field: "maximum_drain_timeout",
            });
        }
        Ok(Self {
            clients,
            cursors_per_client,
            maximum_page_size,
            maximum_snapshot_rows,
            maximum_drain_timeout,
        })
    }

    /// Returns the inclusive diagnostic page-size maximum.
    pub const fn maximum_page_size(self) -> usize {
        self.maximum_page_size
    }

    /// Returns the inclusive diagnostic snapshot row maximum.
    pub const fn maximum_snapshot_rows(self) -> usize {
        self.maximum_snapshot_rows
    }

    /// Returns the inclusive graceful drain timeout maximum.
    pub const fn maximum_drain_timeout(self) -> Duration {
        self.maximum_drain_timeout
    }
}

/// Safe failure category for daemon operational requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DaemonOperationsError {
    /// One daemon composition bound is invalid.
    InvalidLimit {
        /// Stable invalid field name.
        field: &'static str,
    },
    /// The requested diagnostic page size is zero or excessive.
    InvalidPageSize {
        /// Requested row count.
        requested: usize,
        /// Inclusive configured maximum.
        maximum: usize,
    },
    /// The graceful drain timeout is zero or excessive.
    InvalidDrainTimeout {
        /// Requested drain duration.
        requested: Duration,
        /// Inclusive configured maximum.
        maximum: Duration,
    },
    /// The opaque diagnostic cursor has an invalid format or query selection.
    InvalidCursor,
    /// The cursor is absent, evicted, closed, or owned by another client.
    CursorExpired,
    /// A new cursor-owning client would exceed the configured bound.
    ClientCapacity {
        /// Inclusive distinct-client bound.
        limit: usize,
    },
    /// Diagnostic sources returned more rows than one snapshot may retain.
    SnapshotCapacity {
        /// Inclusive captured row bound.
        limit: usize,
    },
    /// The process-local diagnostic cursor identity space is exhausted.
    TokenExhausted,
    /// A diagnostic or aggregate source cannot currently produce state.
    SourceUnavailable,
    /// A source returned invalid, unselected, or unrepresentable state.
    SourceInvariant,
    /// A semantic mutation arrived after authenticated shutdown began draining.
    Draining,
    /// Explicit terminal-content inclusion did not supply bounded content.
    TerminalContentRequired,
    /// Explicit terminal content exceeded the diagnostic-bundle hard bound.
    TerminalContentCapacity {
        /// Inclusive byte bound.
        limit: usize,
    },
}

impl Display for DaemonOperationsError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLimit { field } => {
                write!(formatter, "daemon operation limit {field} is invalid")
            }
            Self::InvalidPageSize { requested, maximum } => write!(
                formatter,
                "diagnostic page size {requested} must be within 1..={maximum}"
            ),
            Self::InvalidDrainTimeout { requested, maximum } => write!(
                formatter,
                "drain timeout {}ms must be within 1..={}ms",
                requested.as_millis(),
                maximum.as_millis()
            ),
            Self::InvalidCursor => formatter.write_str("diagnostic cursor is malformed"),
            Self::CursorExpired => formatter.write_str("diagnostic cursor is absent or expired"),
            Self::ClientCapacity { limit } => {
                write!(
                    formatter,
                    "diagnostic cursor client capacity of {limit} is exhausted"
                )
            }
            Self::SnapshotCapacity { limit } => {
                write!(
                    formatter,
                    "diagnostic snapshot exceeds its {limit}-row limit"
                )
            }
            Self::TokenExhausted => formatter.write_str("diagnostic cursor identity is exhausted"),
            Self::SourceUnavailable => {
                formatter.write_str("daemon diagnostic source is unavailable")
            }
            Self::SourceInvariant => {
                formatter.write_str("daemon diagnostic source violated its contract")
            }
            Self::Draining => formatter.write_str("daemon is draining semantic mutations"),
            Self::TerminalContentRequired => {
                formatter.write_str("authorized diagnostic terminal content was not supplied")
            }
            Self::TerminalContentCapacity { limit } => write!(
                formatter,
                "diagnostic terminal content exceeds its {limit}-byte limit"
            ),
        }
    }
}

impl Error for DaemonOperationsError {}

/// Supplies the current cheap aggregate state without process or bulk IO.
pub trait DaemonStatusSource: Send + Sync {
    /// Returns a payload-free pressure snapshot.
    ///
    /// # Errors
    ///
    /// Returns a typed source failure when aggregate state cannot be read.
    fn pressure(&self) -> Result<DaemonOperationalPressure, DaemonOperationsError>;
}

/// Supplies bounded safe rows for selected operational diagnostic sections.
pub trait DaemonDiagnosticSource: Send + Sync {
    /// Captures rows only for the selected sections.
    ///
    /// Implementations must bound their own work, return no secrets or private
    /// payloads, and use stable identities and typed fields.
    ///
    /// # Errors
    ///
    /// Returns a typed source failure when a consistent snapshot cannot be read.
    fn snapshot(
        &self,
        sections: &BTreeSet<DaemonDiagnosticSection>,
    ) -> Result<Vec<DaemonDiagnosticRow>, DaemonOperationsError>;
}

/// Narrow control-plane port consumed by the product request adapter.
pub trait DaemonOperationsPort: Send + Sync {
    /// Returns a bounded, point-in-time diagnostic page for one logical client.
    ///
    /// # Errors
    ///
    /// Returns typed validation, ownership, capacity, or source failures.
    fn diagnostics(
        &self,
        client_id: &ClientId,
        query: DaemonDiagnosticQuery,
    ) -> Result<DaemonDiagnosticPage, DaemonOperationsError>;

    /// Returns compact current daemon state without enumerating sessions.
    ///
    /// # Errors
    ///
    /// Returns a typed source failure when aggregate state is unavailable.
    fn status(&self) -> Result<DaemonStatusSnapshot, DaemonOperationsError>;

    /// Idempotently requests graceful shutdown and returns its fixed deadline.
    ///
    /// # Errors
    ///
    /// Returns a typed validation failure for a zero or excessive duration.
    fn begin_shutdown(&self, drain_timeout: Duration) -> Result<i64, DaemonOperationsError>;

    /// Acquires one semantic-mutation permit while the daemon is accepting work.
    fn admit_mutation(&self) -> Result<DaemonMutationPermit, DaemonOperationsError>;

    /// Atomically drains mutations and durably terminates the private process host.
    fn begin_product_shutdown(
        &self,
        drain_timeout: Duration,
    ) -> Result<i64, DaemonProductShutdownError>;

    /// Cancels other work before receipt-backed host retirement and immediate client closure.
    fn begin_reset_shutdown(
        &self,
        _timeout: Duration,
        _context: &crate::ProductRequestContext,
    ) -> Result<i64, DaemonProductShutdownError> {
        Err(DaemonProductShutdownError::NotConfigured)
    }

    /// Closes admission, interrupts non-stop work, settles forced stops, then retires the host.
    fn begin_immediate_shutdown(
        &self,
        _timeout: Duration,
        _context: &crate::ProductRequestContext,
        _stop_sessions: &mut dyn FnMut(Duration) -> Result<(), DaemonProductShutdownError>,
    ) -> Result<i64, DaemonProductShutdownError> {
        Err(DaemonProductShutdownError::NotConfigured)
    }

    /// Reset may abandon stop futures; state-preserving shutdown must settle them.
    fn abandon_stops_requested(&self) -> bool {
        self.reset_requested()
    }

    /// Whether accepted work should abandon further effects for a confirmed reset.
    fn reset_requested(&self) -> bool {
        false
    }

    /// Releases all private diagnostic snapshots owned by one logical client.
    fn close_client(&self, client_id: &ClientId);
}

/// Process-local graceful-shutdown signal shared with the composition root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DaemonShutdownPhase {
    /// Connections and semantic mutations remain admitted.
    Running,
    /// New mutations are closed while queries, recovery, and host shutdown remain reachable.
    Draining,
    /// Host termination is proven and listener teardown may finish.
    Finalizing(i64),
}

#[derive(Debug)]
enum DaemonShutdownPhaseState {
    Running,
    ProductDraining {
        attempt_id: OperationAttemptId,
        drain_timeout: Duration,
        operation_active: bool,
    },
    Finalizing(i64),
}

#[derive(Debug)]
struct DaemonShutdownState {
    phase: DaemonShutdownPhaseState,
    active_mutations: usize,
}

impl Default for DaemonShutdownState {
    fn default() -> Self {
        Self {
            phase: DaemonShutdownPhaseState::Running,
            active_mutations: 0,
        }
    }
}

#[derive(Debug, Default)]
pub struct DaemonShutdownSignal {
    resetting: AtomicBool,
    stopping_immediately: AtomicBool,
    reset_response: Mutex<Option<Arc<undercurrent_transport_core::ServerResponseWrite>>>,
    state: Mutex<DaemonShutdownState>,
    changed: Condvar,
}

impl DaemonShutdownSignal {
    pub(crate) fn reset_response_finished(&self) -> bool {
        self.reset_response
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .is_some_and(|response| response.is_finished())
    }

    /// Returns whether any authorized request has begun graceful shutdown.
    pub fn is_requested(&self) -> bool {
        !matches!(
            self.state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .phase,
            DaemonShutdownPhaseState::Running
        )
    }

    /// Returns the first accepted drain deadline, if shutdown has begun.
    pub fn deadline_millis(&self) -> Option<i64> {
        match self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .phase
        {
            DaemonShutdownPhaseState::Finalizing(deadline) => Some(deadline),
            DaemonShutdownPhaseState::Running
            | DaemonShutdownPhaseState::ProductDraining { .. } => None,
        }
    }

    /// Waits up to `timeout` for the first shutdown request.
    ///
    /// This function blocks only the calling supervisor thread and performs no
    /// IO. Poisoned synchronization state is recovered without panicking.
    pub fn wait_timeout(&self, timeout: Duration) -> Option<i64> {
        match self.wait_phase_timeout(timeout) {
            DaemonShutdownPhase::Finalizing(deadline) => Some(deadline),
            DaemonShutdownPhase::Running | DaemonShutdownPhase::Draining => None,
        }
    }

    fn request(&self, deadline_millis: i64) -> i64 {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match &state.phase {
            DaemonShutdownPhaseState::Finalizing(existing) => return *existing,
            DaemonShutdownPhaseState::ProductDraining { .. } => return deadline_millis,
            DaemonShutdownPhaseState::Running => {}
        }
        state.phase = DaemonShutdownPhaseState::Finalizing(deadline_millis);
        self.changed.notify_all();
        deadline_millis
    }

    /// Waits up to the timeout while Running, then returns the observed phase.
    /// Returns immediately if already Draining or Finalizing; use
    /// [`Self::wait_for_finalization`] to wait through Draining.
    pub fn wait_phase_timeout(&self, timeout: Duration) -> DaemonShutdownPhase {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (state, _) = self
            .changed
            .wait_timeout_while(state, timeout, |state| {
                matches!(state.phase, DaemonShutdownPhaseState::Running)
            })
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        shutdown_phase(&state.phase)
    }

    /// Waits for durable host shutdown to permit final listener teardown.
    pub fn wait_for_finalization(&self, timeout: Duration) -> DaemonShutdownPhase {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (state, _) = self
            .changed
            .wait_timeout_while(state, timeout, |state| {
                matches!(
                    state.phase,
                    DaemonShutdownPhaseState::ProductDraining { .. }
                )
            })
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        shutdown_phase(&state.phase)
    }

    fn admit_mutation(self: &Arc<Self>) -> Result<DaemonMutationPermit, DaemonOperationsError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !matches!(state.phase, DaemonShutdownPhaseState::Running) {
            return Err(DaemonOperationsError::Draining);
        }
        state.active_mutations = state
            .active_mutations
            .checked_add(1)
            .ok_or(DaemonOperationsError::SourceInvariant)?;
        Ok(DaemonMutationPermit {
            shutdown: self.clone(),
            active: true,
        })
    }

    fn begin_product_drain(
        self: &Arc<Self>,
        attempt_id: OperationAttemptId,
        drain_timeout: Duration,
    ) -> Result<DaemonProductShutdownPermit, DaemonProductShutdownError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match &mut state.phase {
            DaemonShutdownPhaseState::Running => {
                state.phase = DaemonShutdownPhaseState::ProductDraining {
                    attempt_id: attempt_id.clone(),
                    drain_timeout,
                    operation_active: true,
                };
                self.changed.notify_all();
            }
            DaemonShutdownPhaseState::ProductDraining {
                attempt_id: existing,
                drain_timeout: existing_timeout,
                operation_active,
            } if existing == &attempt_id
                && *existing_timeout == drain_timeout
                && !*operation_active =>
            {
                *operation_active = true;
            }
            DaemonShutdownPhaseState::ProductDraining { .. }
            | DaemonShutdownPhaseState::Finalizing(_) => {
                return Err(DaemonProductShutdownError::AlreadyInProgress);
            }
        }
        Ok(DaemonProductShutdownPermit {
            shutdown: self.clone(),
            attempt_id,
            drain_timeout,
            active: true,
        })
    }

    fn release_mutation(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.active_mutations = state.active_mutations.saturating_sub(1);
        self.changed.notify_all();
    }

    fn release_product_shutdown(&self, attempt_id: &OperationAttemptId) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let DaemonShutdownPhaseState::ProductDraining {
            attempt_id: existing,
            operation_active,
            ..
        } = &mut state.phase
            && existing == attempt_id
        {
            *operation_active = false;
            self.changed.notify_all();
        }
    }

    fn abort_product_shutdown(&self, attempt_id: &OperationAttemptId) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if matches!(
            &state.phase,
            DaemonShutdownPhaseState::ProductDraining {
                attempt_id: existing,
                operation_active: true,
                ..
            } if existing == attempt_id
        ) {
            state.phase = DaemonShutdownPhaseState::Running;
            self.changed.notify_all();
        }
    }
}

fn shutdown_phase(state: &DaemonShutdownPhaseState) -> DaemonShutdownPhase {
    match state {
        DaemonShutdownPhaseState::Running => DaemonShutdownPhase::Running,
        DaemonShutdownPhaseState::ProductDraining { .. } => DaemonShutdownPhase::Draining,
        DaemonShutdownPhaseState::Finalizing(deadline) => {
            DaemonShutdownPhase::Finalizing(*deadline)
        }
    }
}

/// RAII proof that one semantic mutation entered before draining began.
pub struct DaemonMutationPermit {
    shutdown: Arc<DaemonShutdownSignal>,
    active: bool,
}

impl Drop for DaemonMutationPermit {
    fn drop(&mut self) {
        if self.active {
            self.shutdown.release_mutation();
            self.active = false;
        }
    }
}

/// Exclusive authenticated product-shutdown attempt within a draining epoch.
pub struct DaemonProductShutdownPermit {
    shutdown: Arc<DaemonShutdownSignal>,
    attempt_id: OperationAttemptId,
    drain_timeout: Duration,
    active: bool,
}

impl DaemonProductShutdownPermit {
    fn wait_for_mutations(&self, timeout: Duration) -> bool {
        let state = self
            .shutdown
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (state, _) = self
            .shutdown
            .changed
            .wait_timeout_while(state, timeout, |state| state.active_mutations != 0)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.active_mutations == 0
    }

    fn complete(mut self, deadline_millis: i64) -> Result<(), DaemonProductShutdownError> {
        let mut state = self
            .shutdown
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match &state.phase {
            DaemonShutdownPhaseState::ProductDraining {
                attempt_id,
                drain_timeout,
                operation_active: true,
            } if attempt_id == &self.attempt_id && *drain_timeout == self.drain_timeout => {}
            DaemonShutdownPhaseState::Running
            | DaemonShutdownPhaseState::ProductDraining { .. }
            | DaemonShutdownPhaseState::Finalizing(_) => {
                return Err(DaemonProductShutdownError::StateInvariant);
            }
        }
        state.phase = DaemonShutdownPhaseState::Finalizing(deadline_millis);
        self.active = false;
        self.shutdown.changed.notify_all();
        Ok(())
    }

    fn abort(mut self) {
        self.shutdown.abort_product_shutdown(&self.attempt_id);
        self.active = false;
    }
}

impl Drop for DaemonProductShutdownPermit {
    fn drop(&mut self) {
        if self.active {
            self.shutdown.release_product_shutdown(&self.attempt_id);
            self.active = false;
        }
    }
}

/// Failure while coordinating authenticated product shutdown.
#[derive(Debug)]
pub enum DaemonProductShutdownError {
    /// The requested drain duration violates configured bounds.
    Operations(DaemonOperationsError),
    /// Another shutdown attempt is currently active or already finalizing.
    AlreadyInProgress,
    /// Accepted semantic mutations did not finish before the drain timeout.
    MutationDrainTimedOut,
    /// Durable private-host shutdown ports were not composed.
    NotConfigured,
    /// The application-owned durable shutdown use case failed.
    Host(ShutdownProcessHostError),
    /// Owned sessions could not be settled before state-preserving shutdown.
    SessionTermination,
    /// Process-local phase ownership changed unexpectedly.
    StateInvariant,
}

impl Display for DaemonProductShutdownError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Operations(error) => Display::fmt(error, formatter),
            Self::AlreadyInProgress => {
                formatter.write_str("daemon shutdown is already in progress")
            }
            Self::MutationDrainTimedOut => {
                formatter.write_str("semantic mutations did not drain before the shutdown deadline")
            }
            Self::NotConfigured => formatter.write_str("process-host shutdown is not configured"),
            Self::Host(error) => Display::fmt(error, formatter),
            Self::SessionTermination => formatter
                .write_str("session termination could not be settled before daemon shutdown"),
            Self::StateInvariant => {
                formatter.write_str("daemon shutdown state changed unexpectedly")
            }
        }
    }
}

impl Error for DaemonProductShutdownError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Operations(error) => Some(error),
            Self::Host(error) => Some(error),
            Self::AlreadyInProgress
            | Self::MutationDrainTimedOut
            | Self::NotConfigured
            | Self::StateInvariant
            | Self::SessionTermination => None,
        }
    }
}

/// Bounded implementation retaining only captured diagnostic continuations.
pub struct BoundedDaemonOperations {
    epoch: ServerEpoch,
    started_at_millis: i64,
    clock: Arc<dyn Clock>,
    status: Arc<dyn DaemonStatusSource>,
    diagnostics: Vec<Arc<dyn DaemonDiagnosticSource>>,
    shutdown: Arc<DaemonShutdownSignal>,
    process_host_shutdown: Option<ProcessHostShutdownPorts>,
    reset_participants: Mutex<(
        Weak<crate::BoundedProductDispatcher>,
        Weak<crate::DaemonApplicationRuntime>,
    )>,
    limits: DaemonOperationsLimits,
    cursors: DiagnosticCursorStore,
}

impl BoundedDaemonOperations {
    /// Composes daemon operational sources without reading them or performing IO.
    ///
    /// The supplied limits must already have passed [`DaemonOperationsLimits::new`].
    /// Construction retains only ports and allocates an empty cursor registry; it
    /// does not panic.
    pub fn new(
        epoch: ServerEpoch,
        started_at_millis: i64,
        clock: Arc<dyn Clock>,
        status: Arc<dyn DaemonStatusSource>,
        diagnostics: Vec<Arc<dyn DaemonDiagnosticSource>>,
        shutdown: Arc<DaemonShutdownSignal>,
        limits: DaemonOperationsLimits,
    ) -> Self {
        Self {
            epoch,
            started_at_millis,
            clock,
            status,
            diagnostics,
            shutdown,
            process_host_shutdown: None,
            reset_participants: Mutex::default(),
            limits,
            cursors: DiagnosticCursorStore::new(limits),
        }
    }

    pub(crate) fn attach_reset_participants(
        &self,
        dispatcher: &Arc<crate::BoundedProductDispatcher>,
        background: &Arc<crate::DaemonApplicationRuntime>,
    ) {
        *self
            .reset_participants
            .lock()
            .unwrap_or_else(|p| p.into_inner()) =
            (Arc::downgrade(dispatcher), Arc::downgrade(background));
    }

    /// Enables durable private-host termination for authenticated product shutdown.
    pub fn with_process_host_shutdown(
        mut self,
        attempts: Arc<dyn OperationAttemptStore>,
        host: Arc<dyn ProcessHostShutdown>,
    ) -> Self {
        self.process_host_shutdown = Some(ProcessHostShutdownPorts { attempts, host });
        self
    }

    /// Creates one bounded diagnostic bundle after application authorization.
    ///
    /// The safe authorization proof drops any supplied terminal content. An
    /// explicitly authorized proof requires content and enforces the fixed byte
    /// bound before retaining it. Debug output never formats the bytes.
    pub fn create_diagnostic_bundle(
        &self,
        authorization: AuthorizedDiagnosticBundleRequest,
        sections: BTreeSet<DaemonDiagnosticSection>,
        terminal_content: Option<Arc<[u8]>>,
    ) -> Result<DaemonDiagnosticBundle, DaemonOperationsError> {
        let sections = normalized_sections(sections);
        let captured_at_millis = self.clock.now().value();
        let mut items = Vec::new();
        for source in &self.diagnostics {
            let source_items = source.snapshot(&sections)?;
            if items.len().saturating_add(source_items.len()) > self.limits.maximum_snapshot_rows {
                return Err(DaemonOperationsError::SnapshotCapacity {
                    limit: self.limits.maximum_snapshot_rows,
                });
            }
            if source_items
                .iter()
                .any(|item| item.identity.is_empty() || !sections.contains(&item.section))
            {
                return Err(DaemonOperationsError::SourceInvariant);
            }
            items.extend(source_items);
        }
        items.sort_by(|left, right| {
            (left.section, left.identity.as_str()).cmp(&(right.section, right.identity.as_str()))
        });
        if items
            .windows(2)
            .any(|pair| pair[0].section == pair[1].section && pair[0].identity == pair[1].identity)
        {
            return Err(DaemonOperationsError::SourceInvariant);
        }

        let (terminal_content, terminal_content_redacted) =
            if authorization.includes_terminal_content() {
                let terminal_content =
                    terminal_content.ok_or(DaemonOperationsError::TerminalContentRequired)?;
                if terminal_content.len() > DIAGNOSTIC_BUNDLE_TERMINAL_CONTENT_BYTES {
                    return Err(DaemonOperationsError::TerminalContentCapacity {
                        limit: DIAGNOSTIC_BUNDLE_TERMINAL_CONTENT_BYTES,
                    });
                }
                (Some(terminal_content), false)
            } else {
                (None, true)
            };
        Ok(DaemonDiagnosticBundle {
            items,
            captured_at_millis,
            terminal_content,
            terminal_content_redacted,
        })
    }

    /// Returns payload-free diagnostic cursor retention pressure.
    pub fn cursor_pressure(&self) -> DaemonDiagnosticCursorPressure {
        self.cursors.pressure()
    }

    /// Returns the shared shutdown signal observed by the composition root.
    pub fn shutdown_signal(&self) -> &Arc<DaemonShutdownSignal> {
        &self.shutdown
    }

    fn first_page(
        &self,
        client_id: &ClientId,
        query: DaemonDiagnosticQuery,
    ) -> Result<DaemonDiagnosticPage, DaemonOperationsError> {
        self.validate_page_size(query.page_size)?;
        let sections = normalized_sections(query.sections);
        let captured_at_millis = self.clock.now().value();
        let mut items = Vec::new();
        for source in &self.diagnostics {
            let source_items = source.snapshot(&sections)?;
            if items.len().saturating_add(source_items.len()) > self.limits.maximum_snapshot_rows {
                return Err(DaemonOperationsError::SnapshotCapacity {
                    limit: self.limits.maximum_snapshot_rows,
                });
            }
            if source_items
                .iter()
                .any(|item| item.identity.is_empty() || !sections.contains(&item.section))
            {
                return Err(DaemonOperationsError::SourceInvariant);
            }
            items.extend(source_items);
        }
        items.sort_by(|left, right| {
            (left.section, left.identity.as_str()).cmp(&(right.section, right.identity.as_str()))
        });
        if items
            .windows(2)
            .any(|pair| pair[0].section == pair[1].section && pair[0].identity == pair[1].identity)
        {
            return Err(DaemonOperationsError::SourceInvariant);
        }
        let end = query.page_size.min(items.len());
        let page_items = items[..end].to_vec();
        let next_cursor = if end < items.len() {
            Some(self.cursors.register(
                client_id.clone(),
                DiagnosticCursor {
                    sections,
                    items: Arc::from(items),
                    offset: end,
                    captured_at_millis,
                },
            )?)
        } else {
            None
        };
        Ok(DaemonDiagnosticPage {
            items: page_items,
            next_cursor,
            captured_at_millis,
        })
    }

    fn continuation_page(
        &self,
        client_id: &ClientId,
        query: DaemonDiagnosticQuery,
        token: &str,
    ) -> Result<DaemonDiagnosticPage, DaemonOperationsError> {
        self.validate_page_size(query.page_size)?;
        let cursor = self.cursors.resolve(client_id, token)?;
        if cursor.sections != normalized_sections(query.sections) {
            return Err(DaemonOperationsError::InvalidCursor);
        }
        let end = cursor
            .offset
            .saturating_add(query.page_size)
            .min(cursor.items.len());
        let items = cursor.items[cursor.offset..end].to_vec();
        let next_cursor = if end < cursor.items.len() {
            Some(self.cursors.register(
                client_id.clone(),
                DiagnosticCursor {
                    offset: end,
                    ..cursor.clone()
                },
            )?)
        } else {
            None
        };
        Ok(DaemonDiagnosticPage {
            items,
            next_cursor,
            captured_at_millis: cursor.captured_at_millis,
        })
    }

    fn validate_page_size(&self, requested: usize) -> Result<(), DaemonOperationsError> {
        if requested == 0 || requested > self.limits.maximum_page_size {
            return Err(DaemonOperationsError::InvalidPageSize {
                requested,
                maximum: self.limits.maximum_page_size,
            });
        }
        Ok(())
    }
}

impl DaemonOperationsPort for BoundedDaemonOperations {
    fn diagnostics(
        &self,
        client_id: &ClientId,
        query: DaemonDiagnosticQuery,
    ) -> Result<DaemonDiagnosticPage, DaemonOperationsError> {
        match query.cursor.clone() {
            Some(cursor) => self.continuation_page(client_id, query, &cursor),
            None => self.first_page(client_id, query),
        }
    }

    fn status(&self) -> Result<DaemonStatusSnapshot, DaemonOperationsError> {
        let pressure = self.status.pressure()?;
        Ok(DaemonStatusSnapshot {
            server_epoch: self.epoch.clone(),
            started_at_millis: self.started_at_millis,
            accepting_connections: pressure.accepting_connections,
            physical_connections: pressure.physical_connections,
            logical_streams: pressure.logical_streams,
            managed_sessions: pressure.managed_sessions,
        })
    }

    fn begin_shutdown(&self, drain_timeout: Duration) -> Result<i64, DaemonOperationsError> {
        if drain_timeout.is_zero() || drain_timeout > self.limits.maximum_drain_timeout {
            return Err(DaemonOperationsError::InvalidDrainTimeout {
                requested: drain_timeout,
                maximum: self.limits.maximum_drain_timeout,
            });
        }
        let now = self.clock.now().value();
        let timeout = i64::try_from(drain_timeout.as_millis()).unwrap_or(i64::MAX);
        Ok(self.shutdown.request(now.saturating_add(timeout)))
    }

    fn admit_mutation(&self) -> Result<DaemonMutationPermit, DaemonOperationsError> {
        self.shutdown.admit_mutation()
    }

    fn begin_product_shutdown(&self, timeout: Duration) -> Result<i64, DaemonProductShutdownError> {
        self.shutdown_product(timeout, None, None)
    }

    fn begin_reset_shutdown(
        &self,
        timeout: Duration,
        context: &crate::ProductRequestContext,
    ) -> Result<i64, DaemonProductShutdownError> {
        self.shutdown_product(timeout, Some(context), None)
    }

    fn begin_immediate_shutdown(
        &self,
        timeout: Duration,
        context: &crate::ProductRequestContext,
        stop_sessions: &mut dyn FnMut(Duration) -> Result<(), DaemonProductShutdownError>,
    ) -> Result<i64, DaemonProductShutdownError> {
        self.shutdown_product(timeout, Some(context), Some(stop_sessions))
    }

    fn abandon_stops_requested(&self) -> bool {
        self.shutdown.resetting.load(Ordering::Acquire)
    }

    fn reset_requested(&self) -> bool {
        self.shutdown.resetting.load(Ordering::Acquire)
            || self.shutdown.stopping_immediately.load(Ordering::Acquire)
    }

    fn close_client(&self, client_id: &ClientId) {
        self.cursors.close_client(client_id);
    }
}

impl BoundedDaemonOperations {
    fn shutdown_product(
        &self,
        drain_timeout: Duration,
        reset: Option<&crate::ProductRequestContext>,
        mut stop_sessions: Option<
            &mut dyn FnMut(Duration) -> Result<(), DaemonProductShutdownError>,
        >,
    ) -> Result<i64, DaemonProductShutdownError> {
        if drain_timeout.is_zero() || drain_timeout > self.limits.maximum_drain_timeout {
            return Err(DaemonProductShutdownError::Operations(
                DaemonOperationsError::InvalidDrainTimeout {
                    requested: drain_timeout,
                    maximum: self.limits.maximum_drain_timeout,
                },
            ));
        }
        let started = Instant::now();
        let identity_digest = PayloadDigest::of_slices([
            if reset.is_some() {
                b"undercurrent.daemon-reset-shutdown.v0.1.0-beta".as_slice()
            } else {
                b"undercurrent.daemon-product-shutdown.v0.1.0-beta".as_slice()
            },
            self.epoch.as_str().as_bytes(),
        ]);
        let attempt_id =
            OperationAttemptId::derived(ProcessMutationKind::Shutdown, identity_digest);
        // Reset has no graceful drain policy. A retry may extend its waiting
        // budget without changing the exact host effect already admitted.
        let semantic_timeout = if reset.is_some() {
            Duration::ZERO
        } else {
            drain_timeout
        };
        let permit = self
            .shutdown
            .begin_product_drain(attempt_id.clone(), semantic_timeout)?;
        if let Some(context) = reset {
            if stop_sessions.is_some() {
                self.shutdown
                    .stopping_immediately
                    .store(true, Ordering::Release);
            } else {
                self.shutdown.resetting.store(true, Ordering::Release);
            }
            *self
                .shutdown
                .reset_response
                .lock()
                .unwrap_or_else(|p| p.into_inner()) = Some(context.response_write.clone());
            let (dispatcher, background) = self
                .reset_participants
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            if let Some(dispatcher) = dispatcher.upgrade() {
                dispatcher.cancel_for_reset(&context.client_id, &context.request_id);
            }
            let background = background.upgrade();
            if let Some(background) = &background {
                background.request_shutdown();
            }
            if let Some(stop_sessions) = stop_sessions.as_mut() {
                stop_sessions(drain_timeout.saturating_sub(started.elapsed()))?;
            }
            if let Some(background) = background {
                while !background.is_quiescent() {
                    let remaining = drain_timeout.saturating_sub(started.elapsed());
                    if remaining.is_zero() {
                        return Err(DaemonProductShutdownError::MutationDrainTimedOut);
                    }
                    std::thread::sleep(Duration::from_millis(5).min(remaining));
                }
            }
        }
        let mutation_timeout = if reset.is_some() {
            drain_timeout.saturating_sub(started.elapsed())
        } else {
            drain_timeout
        };
        if !permit.wait_for_mutations(mutation_timeout) {
            if reset.is_none() {
                permit.abort();
            }
            return Err(DaemonProductShutdownError::MutationDrainTimedOut);
        }
        let Some(ports) = self.process_host_shutdown.as_ref() else {
            if reset.is_none() {
                permit.abort();
            }
            return Err(DaemonProductShutdownError::NotConfigured);
        };
        let timeout_millis = u64::try_from(drain_timeout.as_millis()).unwrap_or(u64::MAX);
        let command = ShutdownProcessHostCommand::new(Some(attempt_id), timeout_millis);
        let command = if reset.is_some() {
            command.for_reset()
        } else {
            command
        };
        let shutdown = crate::blocking::block_on(
            ShutdownProcessHostService::new(
                ports.attempts.as_ref(),
                ports.host.as_ref(),
                self.clock.as_ref(),
            )
            .execute(command),
        );
        if let Err(source) = shutdown {
            // Only affirmative absence can reopen mutation admission. Otherwise
            // dropping the permit releases this invocation but keeps Draining,
            // allowing the exact shutdown attempt to be inspected or resumed.
            // See [shutdown recovery](../docs/internals/lifecycle/startup-and-shutdown.md#graceful-product-shutdown).
            if reset.is_none() && shutdown_failure_is_known_absent(&source) {
                permit.abort();
            }
            return Err(DaemonProductShutdownError::Host(source));
        }
        // Responses follow durable host settlement and acknowledgement. Graceful
        // stop gets its full drain window; reset shares its remaining budget.
        let response_timeout = if reset.is_some() {
            // A non-reading requester cannot hold reset open. Successful writes
            // wake finalization immediately; this is only the delivery fallback.
            Duration::from_millis(250).min(drain_timeout.saturating_sub(started.elapsed()))
        } else {
            drain_timeout
        };
        let timeout = i64::try_from(response_timeout.as_millis()).unwrap_or(i64::MAX);
        let deadline = self.clock.now().value().saturating_add(timeout);
        permit.complete(deadline)?;
        Ok(deadline)
    }
}

fn shutdown_failure_is_known_absent(error: &ShutdownProcessHostError) -> bool {
    matches!(
        error,
        ShutdownProcessHostError::Admission(_)
            | ShutdownProcessHostError::PayloadConflict
            | ShutdownProcessHostError::StaleFence { .. }
            | ShutdownProcessHostError::KnownAbsent { .. }
            | ShutdownProcessHostError::RejectedKnownAbsent { .. }
            | ShutdownProcessHostError::KnownAbsentSettlement { .. }
            | ShutdownProcessHostError::KnownAbsentSettlementRejected { .. }
    )
}

struct ProcessHostShutdownPorts {
    attempts: Arc<dyn OperationAttemptStore>,
    host: Arc<dyn ProcessHostShutdown>,
}

fn normalized_sections(
    sections: BTreeSet<DaemonDiagnosticSection>,
) -> BTreeSet<DaemonDiagnosticSection> {
    if sections.is_empty() {
        DaemonDiagnosticSection::ALL.into_iter().collect()
    } else {
        sections
    }
}

/// Current payload-free diagnostic cursor retention pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DaemonDiagnosticCursorPressure {
    /// Clients retaining at least one captured continuation.
    pub clients: usize,
    /// Inclusive client bound.
    pub client_limit: usize,
    /// Current captured continuation count.
    pub cursors: usize,
    /// Inclusive aggregate continuation bound.
    pub cursor_limit: usize,
    /// Logical rows referenced across retained continuations.
    pub retained_rows: usize,
    /// Inclusive worst-case logical row bound.
    pub retained_row_limit: usize,
    /// Continuations evicted oldest-first within their owning client.
    pub evictions: u64,
}

#[derive(Debug, Clone)]
struct DiagnosticCursor {
    sections: BTreeSet<DaemonDiagnosticSection>,
    items: Arc<[DaemonDiagnosticRow]>,
    offset: usize,
    captured_at_millis: i64,
}

struct DiagnosticCursorStore {
    limits: DaemonOperationsLimits,
    next_token: AtomicU64,
    state: Mutex<DiagnosticCursorState>,
}

impl DiagnosticCursorStore {
    fn new(limits: DaemonOperationsLimits) -> Self {
        Self {
            limits,
            next_token: AtomicU64::new(1),
            state: Mutex::new(DiagnosticCursorState::default()),
        }
    }

    fn register(
        &self,
        client_id: ClientId,
        cursor: DiagnosticCursor,
    ) -> Result<String, DaemonOperationsError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !state.clients.contains_key(&client_id) && state.clients.len() == self.limits.clients {
            return Err(DaemonOperationsError::ClientCapacity {
                limit: self.limits.clients,
            });
        }
        let token = self
            .next_token
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| DaemonOperationsError::TokenExhausted)?;
        let evicted = {
            let cursors = state.clients.entry(client_id).or_default();
            let evicted = cursors.entries.len() == self.limits.cursors_per_client
                && cursors
                    .order
                    .pop_front()
                    .is_some_and(|expired| cursors.entries.remove(&expired).is_some());
            cursors.order.push_back(token);
            cursors.entries.insert(token, cursor);
            evicted
        };
        if evicted {
            state.evictions = state.evictions.saturating_add(1);
        }
        Ok(encode_diagnostic_token(token))
    }

    fn resolve(
        &self,
        client_id: &ClientId,
        token: &str,
    ) -> Result<DiagnosticCursor, DaemonOperationsError> {
        let token = decode_diagnostic_token(token)?;
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clients
            .get(client_id)
            .and_then(|client| client.entries.get(&token))
            .cloned()
            .ok_or(DaemonOperationsError::CursorExpired)
    }

    fn close_client(&self, client_id: &ClientId) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clients
            .remove(client_id);
    }

    fn pressure(&self) -> DaemonDiagnosticCursorPressure {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let cursors = state
            .clients
            .values()
            .map(|client| client.entries.len())
            .sum();
        let retained_rows = state
            .clients
            .values()
            .flat_map(|client| client.entries.values())
            .map(|cursor| cursor.items.len())
            .sum();
        DaemonDiagnosticCursorPressure {
            clients: state.clients.len(),
            client_limit: self.limits.clients,
            cursors,
            cursor_limit: self
                .limits
                .clients
                .saturating_mul(self.limits.cursors_per_client),
            retained_rows,
            retained_row_limit: self
                .limits
                .clients
                .saturating_mul(self.limits.cursors_per_client)
                .saturating_mul(self.limits.maximum_snapshot_rows),
            evictions: state.evictions,
        }
    }
}

#[derive(Default)]
struct DiagnosticCursorState {
    clients: BTreeMap<ClientId, ClientDiagnosticCursors>,
    evictions: u64,
}

#[derive(Default)]
struct ClientDiagnosticCursors {
    entries: BTreeMap<u64, DiagnosticCursor>,
    order: VecDeque<u64>,
}

fn encode_diagnostic_token(token: u64) -> String {
    format!("{DIAGNOSTIC_TOKEN_PREFIX}{token:016x}")
}

fn decode_diagnostic_token(value: &str) -> Result<u64, DaemonOperationsError> {
    if value.len() != DIAGNOSTIC_TOKEN_BYTES || !value.starts_with(DIAGNOSTIC_TOKEN_PREFIX) {
        return Err(DaemonOperationsError::InvalidCursor);
    }
    u64::from_str_radix(&value[1..], 16).map_err(|_| DaemonOperationsError::InvalidCursor)
}
