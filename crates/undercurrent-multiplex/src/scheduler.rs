// SPDX-License-Identifier: Apache-2.0

//! Bounded priority and flow-control scheduler for one physical connection.
//!
//! Admission enforces byte and message caps independently. Selection spends
//! credit and releases queue accounting before I/O; the outer adapter owns a
//! single writer and any replay confirmation. FIFO ordering is per route within
//! each class, not a general merge of one stream's frames across classes.

use std::array;
use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex};

use undercurrent_wire::{ConnectionLimits, StreamId, StreamSequence};

use crate::{
    DeliverySemantics, FrameRoute, MultiplexError, MultiplexMetricName, MultiplexMetricObservation,
    MultiplexTelemetry, NoopMultiplexTelemetry, OutboundFrame, ScheduledFrame, StreamGap,
    StreamGapReason, StreamKind, StreamRecoveryAction, TrafficClass,
};

#[cfg(test)]
mod tests;

const CLASS_SCHEDULE: [TrafficClass; 15] = [
    TrafficClass::Control,
    TrafficClass::Control,
    TrafficClass::Control,
    TrafficClass::Control,
    TrafficClass::Control,
    TrafficClass::Control,
    TrafficClass::Control,
    TrafficClass::Control,
    TrafficClass::Lifecycle,
    TrafficClass::Lifecycle,
    TrafficClass::Lifecycle,
    TrafficClass::Lifecycle,
    TrafficClass::State,
    TrafficClass::State,
    TrafficClass::Bulk,
];

/// Invalid relationship between negotiated and product scheduler limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerLimitError {
    /// A required scheduler limit was zero.
    ZeroLimit {
        /// Stable invalid field.
        field: &'static str,
    },
    /// Traffic-class reservations exceed the connection queue.
    ClassBudgetsExceeded,
    /// Traffic-class message reservations exceed the connection maximum.
    ClassMessageBudgetsExceeded,
    /// Per-stream message retention exceeds the connection maximum.
    StreamMessageBudgetExceeded,
    /// Initial connection credit exceeds its negotiated maximum.
    ConnectionCreditExceeded,
    /// Fairness burst cap is smaller than one negotiated frame.
    BurstBelowFrame,
}

impl Display for SchedulerLimitError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroLimit { field } => {
                write!(formatter, "scheduler limit {field} must be non-zero")
            }
            Self::ClassBudgetsExceeded => {
                formatter.write_str("traffic-class byte budgets exceed the connection queue budget")
            }
            Self::ClassMessageBudgetsExceeded => formatter
                .write_str("traffic-class message budgets exceed the connection queue budget"),
            Self::StreamMessageBudgetExceeded => {
                formatter.write_str("stream message budget exceeds the connection queue budget")
            }
            Self::ConnectionCreditExceeded => {
                formatter.write_str("initial connection credit exceeds the connection queue budget")
            }
            Self::BurstBelowFrame => formatter
                .write_str("maximum consecutive stream bytes cannot be smaller than a frame"),
        }
    }
}

impl Error for SchedulerLimitError {}

/// Independent hard message-count bounds for one outbound scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerMessageLimits {
    connection_queue_messages: usize,
    stream_queue_messages: usize,
    class_queue_messages: [usize; 4],
}

impl SchedulerMessageLimits {
    /// Validates connection, stream, and traffic-class message reservations.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed zero or nested-budget validation failure.
    pub const fn new(
        connection_queue_messages: usize,
        stream_queue_messages: usize,
        class_queue_messages: [usize; 4],
    ) -> Result<Self, SchedulerLimitError> {
        if connection_queue_messages == 0 {
            return Err(SchedulerLimitError::ZeroLimit {
                field: "connection_queue_messages",
            });
        }
        if stream_queue_messages == 0 {
            return Err(SchedulerLimitError::ZeroLimit {
                field: "stream_queue_messages",
            });
        }
        if stream_queue_messages > connection_queue_messages {
            return Err(SchedulerLimitError::StreamMessageBudgetExceeded);
        }
        let mut index = 0;
        let mut class_total = 0_usize;
        while index < class_queue_messages.len() {
            if class_queue_messages[index] == 0 {
                return Err(SchedulerLimitError::ZeroLimit {
                    field: "class_queue_messages",
                });
            }
            class_total = class_total.saturating_add(class_queue_messages[index]);
            index += 1;
        }
        if class_total > connection_queue_messages {
            return Err(SchedulerLimitError::ClassMessageBudgetsExceeded);
        }
        Ok(Self {
            connection_queue_messages,
            stream_queue_messages,
            class_queue_messages,
        })
    }

    /// Returns the connection-wide retained message maximum.
    pub const fn connection_queue_messages(self) -> usize {
        self.connection_queue_messages
    }

    /// Returns the retained message maximum for one logical stream.
    pub const fn stream_queue_messages(self) -> usize {
        self.stream_queue_messages
    }

    /// Returns the retained message reservation for one traffic class.
    pub const fn class_queue_messages(self, class: TrafficClass) -> usize {
        self.class_queue_messages[class.index()]
    }
}

/// Validated hard bounds for one outbound connection scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerLimits {
    negotiated: ConnectionLimits,
    class_queue_bytes: [u64; 4],
    initial_connection_window_bytes: u64,
    max_consecutive_stream_bytes: u64,
    messages: SchedulerMessageLimits,
}

impl SchedulerLimits {
    /// Constructs scheduler limits within the negotiated connection contract.
    ///
    /// Each traffic class receives an independent non-zero reservation. Their
    /// sum may be below the connection maximum, leaving deliberately unused
    /// headroom. This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed validation failure for zero values or invalid nested
    /// byte-budget relationships.
    pub const fn new(
        negotiated: ConnectionLimits,
        class_queue_bytes: [u64; 4],
        initial_connection_window_bytes: u64,
        max_consecutive_stream_bytes: u64,
        messages: SchedulerMessageLimits,
    ) -> Result<Self, SchedulerLimitError> {
        let mut index = 0;
        let mut class_total = 0_u64;
        while index < class_queue_bytes.len() {
            if class_queue_bytes[index] == 0 {
                return Err(SchedulerLimitError::ZeroLimit {
                    field: "class_queue_bytes",
                });
            }
            class_total = class_total.saturating_add(class_queue_bytes[index]);
            index += 1;
        }
        if initial_connection_window_bytes == 0 {
            return Err(SchedulerLimitError::ZeroLimit {
                field: "initial_connection_window_bytes",
            });
        }
        if max_consecutive_stream_bytes == 0 {
            return Err(SchedulerLimitError::ZeroLimit {
                field: "max_consecutive_stream_bytes",
            });
        }
        if class_total > negotiated.connection_queue_bytes() {
            return Err(SchedulerLimitError::ClassBudgetsExceeded);
        }
        if initial_connection_window_bytes > negotiated.connection_queue_bytes() {
            return Err(SchedulerLimitError::ConnectionCreditExceeded);
        }
        if max_consecutive_stream_bytes < negotiated.max_frame_bytes() as u64 {
            return Err(SchedulerLimitError::BurstBelowFrame);
        }
        Ok(Self {
            negotiated,
            class_queue_bytes,
            initial_connection_window_bytes,
            max_consecutive_stream_bytes,
            messages,
        })
    }

    /// Returns negotiated Undercurrent connection limits.
    pub const fn negotiated(self) -> ConnectionLimits {
        self.negotiated
    }

    /// Returns reserved bytes for one semantic traffic class.
    pub const fn class_queue_bytes(self, class: TrafficClass) -> u64 {
        self.class_queue_bytes[class.index()]
    }

    /// Returns initial connection-wide data credit.
    pub const fn initial_connection_window_bytes(self) -> u64 {
        self.initial_connection_window_bytes
    }

    /// Returns the consecutive stream-byte threshold for yielding selection
    /// opportunities. A sole writable stream can continue after a fairness reset;
    /// this value does not cause an operating-system thread yield.
    pub const fn max_consecutive_stream_bytes(self) -> u64 {
        self.max_consecutive_stream_bytes
    }

    /// Returns independent queue message-count bounds.
    pub const fn messages(self) -> SchedulerMessageLimits {
        self.messages
    }
}

/// Queue-admission result for one outbound frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnqueueOutcome {
    /// Frame entered its exact bounded class and route queues.
    Queued,
    /// Older replaceable state was removed and this frame was retained.
    Coalesced {
        /// Exact encoded bytes released from the older frame.
        released_bytes: u64,
    },
    /// Droppable data was not retained and a visible gap was recorded.
    Dropped {
        /// Current inclusive missing interval after this drop.
        gap: StreamGap,
    },
}

/// Bounded scheduler pressure snapshot for diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerPressure {
    /// Total exact encoded bytes retained.
    pub queued_bytes: u64,
    /// Total retained frames.
    pub queued_messages: usize,
    /// Active logical streams.
    pub active_streams: usize,
    /// Current connection-wide flow credit.
    pub connection_credit_bytes: u64,
    /// Exact queued bytes by [`TrafficClass::ALL`] order.
    pub class_queued_bytes: [u64; 4],
    /// Retained message counts by [`TrafficClass::ALL`] order.
    pub class_queued_messages: [usize; 4],
    /// Total replaceable frames removed.
    pub coalesced_items: u64,
    /// Total droppable frames rejected with gaps.
    pub dropped_items: u64,
}

/// Resources released when a stream is independently closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamRelease {
    /// Exact queued encoded bytes released.
    pub queued_bytes: u64,
    /// Queued frames released.
    pub queued_messages: usize,
}

/// Queued response resources released when stream activation rolls back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestRelease {
    /// Exact queued encoded bytes released.
    pub queued_bytes: u64,
    /// Queued frames released.
    pub queued_messages: usize,
}

/// Bounded pressure for one logical scheduler stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamPressure {
    /// Product stream kind.
    pub kind: StreamKind,
    /// Exact encoded bytes queued for this stream.
    pub queued_bytes: u64,
    /// Retained frames queued for this stream.
    pub queued_messages: usize,
    /// Current per-stream flow credit.
    pub credit_bytes: u64,
    /// Latest accepted sequence, including explicitly dropped items.
    pub last_sequence: StreamSequence,
    /// Whether a live queue gap remains to be delivered.
    pub gap_pending: bool,
    /// Total older replaceable frames removed for this stream.
    pub coalesced_items: u64,
    /// Total droppable frames rejected for this stream.
    pub dropped_items: u64,
}

/// Resources released by physical connection teardown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerRelease {
    /// Active streams removed.
    pub streams: usize,
    /// Exact encoded bytes released.
    pub queued_bytes: u64,
    /// Frames released.
    pub queued_messages: usize,
}

/// Thread-safe bounded outbound scheduler owned by one connection writer.
///
/// # Callback contract
///
/// The scheduler itself performs no transport IO. Supplied telemetry executes
/// synchronously after state mutation and scheduler lock release; blocking and
/// panics are not isolated. A panic does not roll back the preceding mutation.
pub struct OutboundScheduler {
    limits: SchedulerLimits,
    state: Mutex<SchedulerState>,
    telemetry: Arc<dyn MultiplexTelemetry>,
}

impl OutboundScheduler {
    /// Constructs an empty scheduler with negotiated credit and resource limits.
    ///
    /// Construction allocates only fixed scheduler metadata, performs no IO,
    /// and does not panic.
    pub fn new(limits: SchedulerLimits) -> Self {
        Self::with_telemetry(limits, Arc::new(NoopMultiplexTelemetry))
    }

    /// Constructs an empty scheduler with caller-supplied synchronous telemetry.
    ///
    /// Observations carry only typed registry dimensions and are emitted after
    /// scheduler locks are released. Construction performs no IO and does not
    /// panic.
    pub fn with_telemetry(limits: SchedulerLimits, telemetry: Arc<dyn MultiplexTelemetry>) -> Self {
        Self {
            limits,
            state: Mutex::new(SchedulerState::new(limits)),
            telemetry,
        }
    }

    /// Registers one independently cancellable logical stream.
    ///
    /// # Errors
    ///
    /// Returns connection-closed, active-ID conflict, or negotiated stream
    /// capacity failure.
    ///
    /// # Panics
    ///
    /// Supplied callbacks may block or panic; see the
    /// [`OutboundScheduler` callback contract](OutboundScheduler#callback-contract).
    pub fn open_stream(
        &self,
        stream_id: StreamId,
        kind: StreamKind,
        recovery: StreamRecoveryAction,
    ) -> Result<(), MultiplexError> {
        self.open_stream_after(stream_id, kind, recovery, StreamSequence::INITIAL)
    }

    /// Registers a resumed logical stream after its acknowledged sequence.
    ///
    /// Replayed or new items must advance beyond `after`.
    ///
    /// # Errors
    ///
    /// Returns connection-closed, active-ID conflict, or negotiated stream
    /// capacity failure.
    ///
    /// # Panics
    ///
    /// Supplied callbacks may block or panic; see the
    /// [`OutboundScheduler` callback contract](OutboundScheduler#callback-contract).
    pub fn open_stream_after(
        &self,
        stream_id: StreamId,
        kind: StreamKind,
        recovery: StreamRecoveryAction,
        after: StreamSequence,
    ) -> Result<(), MultiplexError> {
        let mut state = self.lock_state();
        state.ensure_open()?;
        if state.streams.contains_key(&stream_id) {
            return Err(MultiplexError::StreamIdInUse { stream_id });
        }
        let limit = self.limits.negotiated().max_logical_streams() as usize;
        if state.streams.len() >= limit {
            return Err(MultiplexError::CapacityExceeded {
                resource: "logical_streams",
                limit,
            });
        }
        state.streams.insert(
            stream_id,
            StreamState::new(
                kind,
                recovery,
                self.limits.negotiated().initial_stream_window_bytes(),
                after,
            ),
        );
        let metrics = state.metric_snapshot();
        drop(state);
        self.emit_logical_streams(metrics.active_streams);
        self.emit_stream_lag(0);
        Ok(())
    }

    /// Removes one stream and all of its queued frames without affecting peers.
    ///
    /// # Errors
    ///
    /// Returns connection-closed or unknown-stream failure.
    ///
    /// # Panics
    ///
    /// Supplied callbacks may block or panic; see the
    /// [`OutboundScheduler` callback contract](OutboundScheduler#callback-contract).
    pub fn close_stream(&self, stream_id: &StreamId) -> Result<StreamRelease, MultiplexError> {
        let mut state = self.lock_state();
        state.ensure_open()?;
        let removed =
            state
                .streams
                .remove(stream_id)
                .ok_or_else(|| MultiplexError::UnknownStream {
                    stream_id: stream_id.clone(),
                })?;
        state.pending_gap_count -= removed.pending_gaps.len();
        state
            .pending_gaps
            .retain(|candidate| candidate != stream_id);
        let route = FrameRoute::Stream(stream_id.clone());
        let mut released_bytes = 0_u64;
        let mut released_messages = 0_usize;
        for class in &mut state.classes {
            let released = class.remove_route(&route);
            released_bytes = released_bytes.saturating_add(released.0);
            released_messages = released_messages.saturating_add(released.1);
        }
        state.queued_bytes = state.queued_bytes.saturating_sub(released_bytes);
        state.queued_messages = state.queued_messages.saturating_sub(released_messages);
        // This interest can no longer return credit for its delivered, unread
        // data. Reclaim that charge so repeated unsubscribe/overload does not
        // gradually exhaust the shared connection window.
        state.connection_credit = state
            .connection_credit
            .saturating_add(removed.outstanding_credit)
            .min(self.limits.negotiated().connection_queue_bytes());
        let metrics = state.metric_snapshot();
        drop(state);
        self.emit_pressure(metrics);
        self.emit_stream_lag(0);
        Ok(StreamRelease {
            queued_bytes: released_bytes,
            queued_messages: released_messages,
        })
    }

    /// Records one stream sequence whose application source is no longer
    /// retained and schedules a credit-exempt gap after queued predecessors.
    ///
    /// No payload frame is allocated. Only adjacent losses with the same reason
    /// and recovery action share an interval. Interval counts are separately
    /// bounded by the configured stream and connection message limits, allowing
    /// loss reporting when payload queues are full.
    ///
    /// # Errors
    ///
    /// Returns connection-closed, unknown-stream, non-monotonic-sequence, or gap
    /// capacity failure. Rejection preserves the prior accepted sequence.
    ///
    /// # Panics
    ///
    /// Supplied callbacks may block or panic; see the
    /// [`OutboundScheduler` callback contract](OutboundScheduler#callback-contract).
    pub fn record_source_gap(
        &self,
        stream_id: &StreamId,
        sequence: StreamSequence,
    ) -> Result<StreamGap, MultiplexError> {
        let mut state = self.lock_state();
        state.ensure_open()?;
        let gap = state.record_gap(
            stream_id,
            sequence,
            StreamGapReason::SourceRetentionEvicted,
            self.limits,
        )?;
        let stream_lag = state.stream_lag_items(stream_id).unwrap_or(0);
        drop(state);
        self.emit_counter(MultiplexMetricName::DroppedItems, None);
        self.emit_stream_lag(stream_lag);
        Ok(gap)
    }

    /// Removes queued frames for one request route during application rollback.
    ///
    /// An unknown or already-written request route is an idempotent no-op.
    ///
    /// # Errors
    ///
    /// Returns connection-closed failure.
    ///
    /// # Panics
    ///
    /// Supplied callbacks may block or panic; see the
    /// [`OutboundScheduler` callback contract](OutboundScheduler#callback-contract).
    pub fn remove_request_frames(
        &self,
        request_id: &undercurrent_wire::RequestId,
    ) -> Result<RequestRelease, MultiplexError> {
        let mut state = self.lock_state();
        state.ensure_open()?;
        let route = FrameRoute::Request(request_id.clone());
        let mut released_bytes = 0_u64;
        let mut released_messages = 0_usize;
        for class in &mut state.classes {
            let released = class.remove_route(&route);
            released_bytes = released_bytes.saturating_add(released.0);
            released_messages = released_messages.saturating_add(released.1);
        }
        state.queued_bytes = state.queued_bytes.saturating_sub(released_bytes);
        state.queued_messages = state.queued_messages.saturating_sub(released_messages);
        let metrics = state.metric_snapshot();
        drop(state);
        self.emit_pressure(metrics);
        Ok(RequestRelease {
            queued_bytes: released_bytes,
            queued_messages: released_messages,
        })
    }

    /// Admits one encoded frame according to exact class and stream budgets.
    ///
    /// Stream sequence advances only for queued, coalesced, or explicitly
    /// dropped items. Reliable admission failure may therefore be retried with
    /// the same sequence.
    ///
    /// # Errors
    ///
    /// Returns typed connection, frame, stream, sequence, or bounded reliable
    /// queue failure.
    ///
    /// # Panics
    ///
    /// Supplied callbacks may block or panic; see the
    /// [`OutboundScheduler` callback contract](OutboundScheduler#callback-contract).
    pub fn enqueue(&self, frame: OutboundFrame) -> Result<EnqueueOutcome, MultiplexError> {
        let mut state = self.lock_state();
        state.ensure_open()?;
        let frame_bytes = frame.encoded_len();
        let traffic_class = frame.class();
        if frame_bytes > u64::from(self.limits.negotiated().max_frame_bytes()) {
            return Err(MultiplexError::InvalidFrameSize {
                actual: frame_bytes,
                maximum: u64::from(self.limits.negotiated().max_frame_bytes()),
            });
        }

        let stream_id = match frame.route() {
            FrameRoute::Stream(stream_id) => Some(stream_id.clone()),
            FrameRoute::Connection | FrameRoute::Request(_) => None,
        };
        if let Some(stream_id) = &stream_id {
            let stream =
                state
                    .streams
                    .get(stream_id)
                    .ok_or_else(|| MultiplexError::UnknownStream {
                        stream_id: stream_id.clone(),
                    })?;
            let sequence =
                frame
                    .sequence()
                    .ok_or_else(|| MultiplexError::StreamSequenceOutOfOrder {
                        stream_id: stream_id.clone(),
                        previous: stream.last_sequence.value(),
                        received: stream.last_sequence.value(),
                    })?;
            if sequence <= stream.last_sequence {
                return Err(MultiplexError::StreamSequenceOutOfOrder {
                    stream_id: stream_id.clone(),
                    previous: stream.last_sequence.value(),
                    received: sequence.value(),
                });
            }
        }

        let replaceable_bytes = if frame.semantics() == DeliverySemantics::Replaceable {
            state.replaceable_bytes(&frame)
        } else {
            0
        };
        // Check the replacement's final byte charge before removing its predecessor;
        // rejection must preserve the old state and allow the new sequence to retry.
        // See [replacement and saturation](../docs/internals/scheduling.md#replacement-and-saturation).
        if replaceable_bytes > 0
            && state.can_admit_after_release(&frame, replaceable_bytes, self.limits)
        {
            let released = state.remove_replaceable(&frame);
            state.push_frame(frame, stream_id.as_ref());
            state.coalesced_items = state.coalesced_items.saturating_add(1);
            let metrics = state.metric_snapshot();
            let stream_lag = stream_id
                .as_ref()
                .and_then(|stream_id| state.stream_lag_items(stream_id));
            drop(state);
            self.emit_class_pressure(metrics, traffic_class);
            if let Some(stream_lag) = stream_lag {
                self.emit_stream_lag(stream_lag);
            }
            self.emit_counter(MultiplexMetricName::CoalescedItems, Some(traffic_class));
            return Ok(EnqueueOutcome::Coalesced {
                released_bytes: released,
            });
        }
        if state.can_admit(&frame, self.limits) {
            state.push_frame(frame, stream_id.as_ref());
            let metrics = state.metric_snapshot();
            let stream_lag = stream_id
                .as_ref()
                .and_then(|stream_id| state.stream_lag_items(stream_id));
            drop(state);
            self.emit_class_pressure(metrics, traffic_class);
            if let Some(stream_lag) = stream_lag {
                self.emit_stream_lag(stream_lag);
            }
            return Ok(EnqueueOutcome::Queued);
        }

        if frame.semantics() == DeliverySemantics::Droppable {
            let stream_id = stream_id.ok_or_else(|| MultiplexError::QueueSaturated {
                queue: "connection",
                byte_limit: self.limits.negotiated().connection_queue_bytes(),
                queued_bytes: state.queued_bytes,
                requested_bytes: frame_bytes,
            })?;
            let sequence =
                frame
                    .sequence()
                    .ok_or_else(|| MultiplexError::StreamSequenceOutOfOrder {
                        stream_id: stream_id.clone(),
                        previous: 0,
                        received: 0,
                    })?;
            let gap = state.record_gap(
                &stream_id,
                sequence,
                StreamGapReason::LiveQueueOverflow,
                self.limits,
            )?;
            let stream_lag = state.stream_lag_items(&stream_id).unwrap_or(0);
            drop(state);
            self.emit_counter(MultiplexMetricName::DroppedItems, Some(traffic_class));
            self.emit_stream_lag(stream_lag);
            return Ok(EnqueueOutcome::Dropped { gap });
        }

        Err(state.saturation_error(&frame, self.limits))
    }

    /// Selects the next weighted-fair frame whose flow credit is available.
    ///
    /// Selection removes queued bytes and spends non-control credit immediately.
    /// Selection does not physically write, record replay, or roll back automatically.
    ///
    /// Control traffic and typed gaps bypass data credit to prevent heartbeat
    /// and window-update deadlock. A `None` result with queued data means every
    /// candidate is waiting for additional flow credit.
    ///
    /// # Errors
    ///
    /// Returns connection-closed or scheduler-invariant failure. Exhausted flow
    /// credit is temporary backpressure and therefore returns `Ok(None)`.
    ///
    /// # Panics
    ///
    /// Supplied callbacks may block or panic; see the
    /// [`OutboundScheduler` callback contract](OutboundScheduler#callback-contract).
    pub fn dequeue(&self) -> Result<Option<ScheduledFrame>, MultiplexError> {
        let mut state = self.lock_state();
        state.ensure_open()?;
        let pending_gap_count = state.pending_gaps.len();
        for _ in 0..pending_gap_count {
            let Some(stream_id) = state.pending_gaps.pop_front() else {
                break;
            };
            let gap = state
                .streams
                .get(&stream_id)
                .and_then(|stream| stream.pending_gaps.front())
                .cloned();
            let Some(gap) = gap else {
                continue;
            };
            if state.has_queued_before(&stream_id, gap.first_missing) {
                state.pending_gaps.push_back(stream_id);
                continue;
            }
            if let Some(stream) = state.streams.get_mut(&stream_id) {
                stream.pending_gaps.pop_front();
                if !stream.pending_gaps.is_empty() {
                    state.pending_gaps.push_back(stream_id.clone());
                }
            }
            state.pending_gap_count -= 1;
            let stream_lag = state.stream_lag_items(&stream_id).unwrap_or(0);
            drop(state);
            self.emit_counter(MultiplexMetricName::ReportedGaps, None);
            self.emit_stream_lag(stream_lag);
            return Ok(Some(ScheduledFrame::Gap(gap)));
        }

        let mut fairness_reset = false;
        loop {
            let mut fairness_blocked = false;
            for _ in 0..CLASS_SCHEDULE.len() {
                let class = CLASS_SCHEDULE[state.class_cursor];
                state.class_cursor = (state.class_cursor + 1) % CLASS_SCHEDULE.len();
                let route_count = state.classes[class.index()].route_order.len();
                for _ in 0..route_count {
                    let Some(route) = state.classes[class.index()].route_order.pop_front() else {
                        break;
                    };
                    let Some(frame) = state.classes[class.index()]
                        .routes
                        .get(&route)
                        .and_then(|queue| queue.front())
                        .cloned()
                    else {
                        state.classes[class.index()].routes.remove(&route);
                        continue;
                    };
                    if state.must_yield_for_stream(&frame, self.limits) {
                        fairness_blocked = true;
                        state.classes[class.index()].route_order.push_back(route);
                        continue;
                    }
                    if state.has_gap_before(&frame) || state.check_credit(&frame).is_err() {
                        state.classes[class.index()].route_order.push_back(route);
                        continue;
                    }

                    let frame = state.classes[class.index()].pop_front(&route).ok_or(
                        MultiplexError::CounterExhausted {
                            counter: "scheduler_route_queue",
                        },
                    )?;
                    if state.classes[class.index()].routes.contains_key(&route) {
                        state.classes[class.index()].route_order.push_back(route);
                    }
                    state.consume_frame(&frame);
                    let metrics = state.metric_snapshot();
                    let stream_lag = match frame.route() {
                        FrameRoute::Stream(stream_id) => state.stream_lag_items(stream_id),
                        FrameRoute::Connection | FrameRoute::Request(_) => None,
                    };
                    drop(state);
                    self.emit_class_pressure(metrics, class);
                    if let Some(stream_lag) = stream_lag {
                        self.emit_stream_lag(stream_lag);
                    }
                    if class != TrafficClass::Control {
                        self.emit_connection_credit(metrics.connection_credit_bytes);
                    }
                    return Ok(Some(ScheduledFrame::Data(frame)));
                }
            }

            if fairness_blocked && !fairness_reset {
                // The burst limit offers peers a turn; it must not stall the only
                // writable stream when there is no credit update to wake the writer.
                // See [selection and fairness](../docs/internals/scheduling.md#selection-ordering-and-fairness).
                state.last_stream = None;
                state.consecutive_stream_bytes = 0;
                fairness_reset = true;
                continue;
            }
            return Ok(None);
        }
    }

    /// Adds bounded connection-wide flow-control credit.
    ///
    /// # Errors
    ///
    /// Returns connection-closed or an update above the negotiated window.
    ///
    /// # Panics
    ///
    /// Supplied callbacks may block or panic; see the
    /// [`OutboundScheduler` callback contract](OutboundScheduler#callback-contract).
    pub fn grant_connection_credit(&self, additional_bytes: u64) -> Result<u64, MultiplexError> {
        let mut state = self.lock_state();
        state.ensure_open()?;
        let requested = state.connection_credit.saturating_add(additional_bytes);
        let maximum = self.limits.negotiated().connection_queue_bytes();
        if requested > maximum {
            return Err(MultiplexError::CreditUpdateExceeded {
                window: "connection",
                requested,
                maximum,
            });
        }
        state.connection_credit = requested;
        drop(state);
        self.emit_gauge(
            MultiplexMetricName::ConnectionCreditBytes,
            requested as f64,
            None,
        );
        Ok(requested)
    }

    /// Adds bounded flow-control credit for one logical stream.
    ///
    /// # Errors
    ///
    /// Returns connection-closed, unknown-stream, or update-above-window failure.
    pub fn grant_stream_credit(
        &self,
        stream_id: &StreamId,
        additional_bytes: u64,
    ) -> Result<u64, MultiplexError> {
        let mut state = self.lock_state();
        state.ensure_open()?;
        let stream =
            state
                .streams
                .get_mut(stream_id)
                .ok_or_else(|| MultiplexError::UnknownStream {
                    stream_id: stream_id.clone(),
                })?;
        let requested = stream.credit.saturating_add(additional_bytes);
        let maximum = self.limits.negotiated().stream_queue_bytes();
        if requested > maximum {
            return Err(MultiplexError::CreditUpdateExceeded {
                window: "stream",
                requested,
                maximum,
            });
        }
        stream.credit = requested;
        Ok(requested)
    }

    /// Atomically returns consumed bytes to connection and stream windows.
    ///
    /// Neither window changes when either resulting value would exceed its
    /// configured maximum.
    ///
    /// # Errors
    ///
    /// Returns connection-closed, unknown-stream, zero-update, or either
    /// update-above-window failure.
    ///
    /// # Panics
    ///
    /// Supplied callbacks may block or panic; see the
    /// [`OutboundScheduler` callback contract](OutboundScheduler#callback-contract).
    pub fn grant_consumed_credit(
        &self,
        stream_id: &StreamId,
        additional_bytes: u64,
    ) -> Result<(u64, u64), MultiplexError> {
        if additional_bytes == 0 {
            return Err(MultiplexError::InvalidLimit {
                field: "additional_credit_bytes",
            });
        }
        let mut state = self.lock_state();
        state.ensure_open()?;
        let stream_credit = state
            .streams
            .get(stream_id)
            .map(|stream| stream.credit)
            .ok_or_else(|| MultiplexError::UnknownStream {
                stream_id: stream_id.clone(),
            })?;
        let requested_stream = stream_credit.saturating_add(additional_bytes);
        let stream_maximum = self.limits.negotiated().stream_queue_bytes();
        if requested_stream > stream_maximum {
            return Err(MultiplexError::CreditUpdateExceeded {
                window: "stream",
                requested: requested_stream,
                maximum: stream_maximum,
            });
        }
        let requested_connection = state.connection_credit.saturating_add(additional_bytes);
        let connection_maximum = self.limits.negotiated().connection_queue_bytes();
        if requested_connection > connection_maximum {
            return Err(MultiplexError::CreditUpdateExceeded {
                window: "connection",
                requested: requested_connection,
                maximum: connection_maximum,
            });
        }
        state.connection_credit = requested_connection;
        if let Some(stream) = state.streams.get_mut(stream_id) {
            stream.credit = requested_stream;
            stream.outstanding_credit = stream.outstanding_credit.saturating_sub(additional_bytes);
        }
        drop(state);
        self.emit_gauge(
            MultiplexMetricName::ConnectionCreditBytes,
            requested_connection as f64,
            None,
        );
        Ok((requested_connection, requested_stream))
    }

    /// Returns a lock-consistent bounded pressure snapshot.
    pub fn pressure(&self) -> SchedulerPressure {
        let state = self.lock_state();
        SchedulerPressure {
            queued_bytes: state.queued_bytes,
            queued_messages: state.queued_messages,
            active_streams: state.streams.len(),
            connection_credit_bytes: state.connection_credit,
            class_queued_bytes: array::from_fn(|index| state.classes[index].bytes),
            class_queued_messages: array::from_fn(|index| state.classes[index].messages),
            coalesced_items: state.coalesced_items,
            dropped_items: state.dropped_items,
        }
    }

    /// Returns bounded pressure for one active logical stream.
    ///
    /// # Errors
    ///
    /// Returns unknown-stream failure. This function performs no IO and does
    /// not panic.
    pub fn stream_pressure(&self, stream_id: &StreamId) -> Result<StreamPressure, MultiplexError> {
        let state = self.lock_state();
        let stream = state
            .streams
            .get(stream_id)
            .ok_or_else(|| MultiplexError::UnknownStream {
                stream_id: stream_id.clone(),
            })?;
        Ok(StreamPressure {
            kind: stream.kind,
            queued_bytes: stream.queued_bytes,
            queued_messages: stream.queued_messages,
            credit_bytes: stream.credit,
            last_sequence: stream.last_sequence,
            gap_pending: !stream.pending_gaps.is_empty(),
            coalesced_items: stream.coalesced_items,
            dropped_items: stream.dropped_items,
        })
    }

    /// Closes the scheduler and atomically releases all retained queue memory.
    ///
    /// Repeating close is idempotent and returns an empty release report.
    ///
    /// # Panics
    ///
    /// Supplied callbacks may block or panic; see the
    /// [`OutboundScheduler` callback contract](OutboundScheduler#callback-contract).
    pub fn close(&self) -> SchedulerRelease {
        let mut state = self.lock_state();
        if state.closed {
            return SchedulerRelease {
                streams: 0,
                queued_bytes: 0,
                queued_messages: 0,
            };
        }
        let release = SchedulerRelease {
            streams: state.streams.len(),
            queued_bytes: state.queued_bytes,
            queued_messages: state.queued_messages,
        };
        state.closed = true;
        state.streams.clear();
        state.pending_gaps.clear();
        state.pending_gap_count = 0;
        state.classes = array::from_fn(|_| ClassQueue::default());
        state.queued_bytes = 0;
        state.queued_messages = 0;
        let metrics = state.metric_snapshot();
        drop(state);
        self.emit_pressure(metrics);
        release
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, SchedulerState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn emit_pressure(&self, snapshot: SchedulerMetricSnapshot) {
        self.emit_logical_streams(snapshot.active_streams);
        self.emit_connection_credit(snapshot.connection_credit_bytes);
        for class in TrafficClass::ALL {
            self.emit_class_pressure(snapshot, class);
        }
    }

    fn emit_logical_streams(&self, active_streams: usize) {
        self.emit_gauge(
            MultiplexMetricName::LogicalStreams,
            active_streams as f64,
            None,
        );
    }

    fn emit_connection_credit(&self, connection_credit_bytes: u64) {
        self.emit_gauge(
            MultiplexMetricName::ConnectionCreditBytes,
            connection_credit_bytes as f64,
            None,
        );
    }

    fn emit_stream_lag(&self, lag_items: usize) {
        self.emit_gauge(MultiplexMetricName::StreamLagItems, lag_items as f64, None);
    }

    fn emit_class_pressure(&self, snapshot: SchedulerMetricSnapshot, class: TrafficClass) {
        self.emit_gauge(
            MultiplexMetricName::QueuedBytes,
            snapshot.class_queued_bytes[class.index()] as f64,
            Some(class),
        );
        self.emit_gauge(
            MultiplexMetricName::QueuedMessages,
            snapshot.class_queued_messages[class.index()] as f64,
            Some(class),
        );
    }

    fn emit_counter(&self, name: MultiplexMetricName, traffic_class: Option<TrafficClass>) {
        self.telemetry.record_metric(MultiplexMetricObservation {
            name,
            value: 1.0,
            traffic_class,
        });
    }

    fn emit_gauge(
        &self,
        name: MultiplexMetricName,
        value: f64,
        traffic_class: Option<TrafficClass>,
    ) {
        self.telemetry.record_metric(MultiplexMetricObservation {
            name,
            value,
            traffic_class,
        });
    }
}

#[derive(Debug, Clone, Copy)]
struct SchedulerMetricSnapshot {
    active_streams: usize,
    connection_credit_bytes: u64,
    class_queued_bytes: [u64; 4],
    class_queued_messages: [usize; 4],
}

struct SchedulerState {
    closed: bool,
    streams: BTreeMap<StreamId, StreamState>,
    classes: [ClassQueue; 4],
    pending_gaps: VecDeque<StreamId>,
    pending_gap_count: usize,
    queued_bytes: u64,
    queued_messages: usize,
    connection_credit: u64,
    class_cursor: usize,
    last_stream: Option<StreamId>,
    consecutive_stream_bytes: u64,
    coalesced_items: u64,
    dropped_items: u64,
}

impl SchedulerState {
    fn new(limits: SchedulerLimits) -> Self {
        Self {
            closed: false,
            streams: BTreeMap::new(),
            classes: array::from_fn(|_| ClassQueue::default()),
            pending_gaps: VecDeque::new(),
            pending_gap_count: 0,
            queued_bytes: 0,
            queued_messages: 0,
            connection_credit: limits.initial_connection_window_bytes(),
            class_cursor: 0,
            last_stream: None,
            consecutive_stream_bytes: 0,
            coalesced_items: 0,
            dropped_items: 0,
        }
    }

    fn ensure_open(&self) -> Result<(), MultiplexError> {
        if self.closed {
            Err(MultiplexError::ConnectionClosed)
        } else {
            Ok(())
        }
    }

    fn metric_snapshot(&self) -> SchedulerMetricSnapshot {
        SchedulerMetricSnapshot {
            active_streams: self.streams.len(),
            connection_credit_bytes: self.connection_credit,
            class_queued_bytes: array::from_fn(|index| self.classes[index].bytes),
            class_queued_messages: array::from_fn(|index| self.classes[index].messages),
        }
    }

    fn can_admit(&self, frame: &OutboundFrame, limits: SchedulerLimits) -> bool {
        let bytes = frame.encoded_len();
        if self.queued_messages.saturating_add(1) > limits.messages().connection_queue_messages() {
            return false;
        }
        if self.queued_bytes.saturating_add(bytes) > limits.negotiated().connection_queue_bytes() {
            return false;
        }
        let class = &self.classes[frame.class().index()];
        if class.messages.saturating_add(1) > limits.messages().class_queue_messages(frame.class())
        {
            return false;
        }
        if class.bytes.saturating_add(bytes) > limits.class_queue_bytes(frame.class()) {
            return false;
        }
        match frame.route() {
            FrameRoute::Stream(stream_id) => self.streams.get(stream_id).is_some_and(|stream| {
                stream.queued_messages.saturating_add(1)
                    <= limits.messages().stream_queue_messages()
                    && stream.queued_bytes.saturating_add(bytes)
                        <= limits.negotiated().stream_queue_bytes()
            }),
            FrameRoute::Connection | FrameRoute::Request(_) => true,
        }
    }

    fn can_admit_after_release(
        &self,
        frame: &OutboundFrame,
        released_bytes: u64,
        limits: SchedulerLimits,
    ) -> bool {
        // enqueue holds the lock and replaces one existing frame on this exact
        // route and class. Message counts therefore stay unchanged at every scope;
        // only the replacement's byte charge can exceed an admission bound.
        let bytes = frame.encoded_len();
        if self
            .queued_bytes
            .saturating_sub(released_bytes)
            .saturating_add(bytes)
            > limits.negotiated().connection_queue_bytes()
        {
            return false;
        }
        let class = &self.classes[frame.class().index()];
        if class
            .bytes
            .saturating_sub(released_bytes)
            .saturating_add(bytes)
            > limits.class_queue_bytes(frame.class())
        {
            return false;
        }
        match frame.route() {
            FrameRoute::Stream(stream_id) => self.streams.get(stream_id).is_some_and(|stream| {
                stream
                    .queued_bytes
                    .saturating_sub(released_bytes)
                    .saturating_add(bytes)
                    <= limits.negotiated().stream_queue_bytes()
            }),
            FrameRoute::Connection | FrameRoute::Request(_) => true,
        }
    }

    fn push_frame(&mut self, frame: OutboundFrame, stream_id: Option<&StreamId>) {
        let bytes = frame.encoded_len();
        if let Some(stream_id) = stream_id
            && let Some(stream) = self.streams.get_mut(stream_id)
        {
            stream.queued_bytes = stream.queued_bytes.saturating_add(bytes);
            stream.queued_messages = stream.queued_messages.saturating_add(1);
            stream.last_sequence = frame.sequence().unwrap_or(stream.last_sequence);
        }
        let class = frame.class().index();
        self.classes[class].push(frame);
        self.queued_bytes = self.queued_bytes.saturating_add(bytes);
        self.queued_messages = self.queued_messages.saturating_add(1);
    }

    fn replaceable_bytes(&self, frame: &OutboundFrame) -> u64 {
        self.classes[frame.class().index()]
            .routes
            .get(frame.route())
            .and_then(|queue| {
                queue
                    .iter()
                    .rfind(|queued| queued.semantics() == DeliverySemantics::Replaceable)
            })
            .map_or(0, OutboundFrame::encoded_len)
    }

    fn remove_replaceable(&mut self, frame: &OutboundFrame) -> u64 {
        let class_index = frame.class().index();
        let Some(queue) = self.classes[class_index].routes.get_mut(frame.route()) else {
            return 0;
        };
        let Some(index) = queue
            .iter()
            .rposition(|queued| queued.semantics() == DeliverySemantics::Replaceable)
        else {
            return 0;
        };
        let Some(removed) = queue.remove(index) else {
            return 0;
        };
        let bytes = removed.encoded_len();
        self.classes[class_index].bytes = self.classes[class_index].bytes.saturating_sub(bytes);
        self.classes[class_index].messages = self.classes[class_index].messages.saturating_sub(1);
        self.queued_bytes = self.queued_bytes.saturating_sub(bytes);
        self.queued_messages = self.queued_messages.saturating_sub(1);
        if let FrameRoute::Stream(stream_id) = frame.route()
            && let Some(stream) = self.streams.get_mut(stream_id)
        {
            stream.queued_bytes = stream.queued_bytes.saturating_sub(bytes);
            stream.queued_messages = stream.queued_messages.saturating_sub(1);
            stream.coalesced_items = stream.coalesced_items.saturating_add(1);
        }
        bytes
    }

    fn record_gap(
        &mut self,
        stream_id: &StreamId,
        sequence: StreamSequence,
        reason: StreamGapReason,
        limits: SchedulerLimits,
    ) -> Result<StreamGap, MultiplexError> {
        let stream =
            self.streams
                .get_mut(stream_id)
                .ok_or_else(|| MultiplexError::UnknownStream {
                    stream_id: stream_id.clone(),
                })?;
        if sequence <= stream.last_sequence {
            return Err(MultiplexError::StreamSequenceOutOfOrder {
                stream_id: stream_id.clone(),
                previous: stream.last_sequence.value(),
                received: sequence.value(),
            });
        }
        let extends_last = stream.pending_gaps.back().is_some_and(|gap| {
            gap.last_missing.next().ok() == Some(sequence)
                && gap.reason == reason
                && gap.recovery == stream.recovery
        });
        if extends_last {
            if let Some(gap) = stream.pending_gaps.back_mut() {
                gap.last_missing = sequence;
            }
        } else {
            // Gap metadata needs its own bounded reserve: a full payload queue
            // must still report loss, but must never widen a gap over kept data.
            if stream.pending_gaps.len() >= limits.messages().stream_queue_messages() {
                return Err(MultiplexError::QueueMessageSaturated {
                    queue: "stream_gaps",
                    message_limit: limits.messages().stream_queue_messages(),
                    queued_messages: stream.pending_gaps.len(),
                });
            }
            if self.pending_gap_count >= limits.messages().connection_queue_messages() {
                return Err(MultiplexError::QueueMessageSaturated {
                    queue: "connection_gaps",
                    message_limit: limits.messages().connection_queue_messages(),
                    queued_messages: self.pending_gap_count,
                });
            }
            if stream.pending_gaps.is_empty() {
                self.pending_gaps.push_back(stream_id.clone());
            }
            stream.pending_gaps.push_back(StreamGap {
                stream_id: stream_id.clone(),
                first_missing: sequence,
                last_missing: sequence,
                reason,
                recovery: stream.recovery,
            });
            self.pending_gap_count += 1;
        }
        stream.last_sequence = sequence;
        self.dropped_items = self.dropped_items.saturating_add(1);
        stream.dropped_items = stream.dropped_items.saturating_add(1);
        stream
            .pending_gaps
            .back()
            .cloned()
            .ok_or(MultiplexError::CounterExhausted {
                counter: "pending_stream_gap",
            })
    }

    fn saturation_error(&self, frame: &OutboundFrame, limits: SchedulerLimits) -> MultiplexError {
        let requested_bytes = frame.encoded_len();
        if let FrameRoute::Stream(stream_id) = frame.route()
            && let Some(stream) = self.streams.get(stream_id)
        {
            if stream.queued_messages.saturating_add(1) > limits.messages().stream_queue_messages()
            {
                return MultiplexError::QueueMessageSaturated {
                    queue: "stream",
                    message_limit: limits.messages().stream_queue_messages(),
                    queued_messages: stream.queued_messages,
                };
            }
            if stream.queued_bytes.saturating_add(requested_bytes)
                > limits.negotiated().stream_queue_bytes()
            {
                return MultiplexError::QueueSaturated {
                    queue: "stream",
                    byte_limit: limits.negotiated().stream_queue_bytes(),
                    queued_bytes: stream.queued_bytes,
                    requested_bytes,
                };
            }
        }
        let class = &self.classes[frame.class().index()];
        if class.messages.saturating_add(1) > limits.messages().class_queue_messages(frame.class())
        {
            return MultiplexError::QueueMessageSaturated {
                queue: frame.class().as_str(),
                message_limit: limits.messages().class_queue_messages(frame.class()),
                queued_messages: class.messages,
            };
        }
        if class.bytes.saturating_add(requested_bytes) > limits.class_queue_bytes(frame.class()) {
            return MultiplexError::QueueSaturated {
                queue: frame.class().as_str(),
                byte_limit: limits.class_queue_bytes(frame.class()),
                queued_bytes: class.bytes,
                requested_bytes,
            };
        }
        if self.queued_messages.saturating_add(1) > limits.messages().connection_queue_messages() {
            MultiplexError::QueueMessageSaturated {
                queue: "connection",
                message_limit: limits.messages().connection_queue_messages(),
                queued_messages: self.queued_messages,
            }
        } else {
            MultiplexError::QueueSaturated {
                queue: "connection",
                byte_limit: limits.negotiated().connection_queue_bytes(),
                queued_bytes: self.queued_bytes,
                requested_bytes,
            }
        }
    }

    fn check_credit(&self, frame: &OutboundFrame) -> Result<(), MultiplexError> {
        let required = frame.flow_controlled_bytes();
        if required == 0 {
            return Ok(());
        }
        if self.connection_credit < required {
            return Err(MultiplexError::CreditExhausted {
                stream_id: None,
                available_bytes: self.connection_credit,
                required_bytes: required,
            });
        }
        if let FrameRoute::Stream(stream_id) = frame.route() {
            let available = self
                .streams
                .get(stream_id)
                .map_or(0, |stream| stream.credit);
            if available < required {
                return Err(MultiplexError::CreditExhausted {
                    stream_id: Some(stream_id.clone()),
                    available_bytes: available,
                    required_bytes: required,
                });
            }
        }
        Ok(())
    }

    fn has_queued_before(&self, stream_id: &StreamId, sequence: StreamSequence) -> bool {
        let route = FrameRoute::Stream(stream_id.clone());
        self.classes.iter().any(|class| {
            class.routes.get(&route).is_some_and(|queue| {
                queue
                    .iter()
                    .any(|frame| frame.sequence().is_some_and(|queued| queued < sequence))
            })
        })
    }

    fn has_gap_before(&self, frame: &OutboundFrame) -> bool {
        let (FrameRoute::Stream(stream_id), Some(sequence)) = (frame.route(), frame.sequence())
        else {
            return false;
        };
        // A gap waiting on older data cannot let newer frames overtake it,
        // including control-class stream frames that do not need data credit.
        self.streams
            .get(stream_id)
            .and_then(|stream| stream.pending_gaps.front())
            .is_some_and(|gap| gap.first_missing < sequence)
    }

    fn stream_lag_items(&self, stream_id: &StreamId) -> Option<usize> {
        self.streams.get(stream_id).map(|stream| {
            stream
                .queued_messages
                .saturating_add(stream.pending_gaps.len())
        })
    }

    fn must_yield_for_stream(&self, frame: &OutboundFrame, limits: SchedulerLimits) -> bool {
        let FrameRoute::Stream(stream_id) = frame.route() else {
            return false;
        };
        self.last_stream.as_ref() == Some(stream_id)
            && self
                .consecutive_stream_bytes
                .saturating_add(frame.encoded_len())
                > limits.max_consecutive_stream_bytes()
    }

    fn consume_frame(&mut self, frame: &OutboundFrame) {
        let bytes = frame.encoded_len();
        self.queued_bytes = self.queued_bytes.saturating_sub(bytes);
        self.queued_messages = self.queued_messages.saturating_sub(1);
        let credit = frame.flow_controlled_bytes();
        self.connection_credit = self.connection_credit.saturating_sub(credit);
        match frame.route() {
            FrameRoute::Stream(stream_id) => {
                if let Some(stream) = self.streams.get_mut(stream_id) {
                    stream.queued_bytes = stream.queued_bytes.saturating_sub(bytes);
                    stream.queued_messages = stream.queued_messages.saturating_sub(1);
                    stream.credit = stream.credit.saturating_sub(credit);
                    stream.outstanding_credit = stream.outstanding_credit.saturating_add(credit);
                }
                if self.last_stream.as_ref() == Some(stream_id) {
                    self.consecutive_stream_bytes =
                        self.consecutive_stream_bytes.saturating_add(bytes);
                } else {
                    self.last_stream = Some(stream_id.clone());
                    self.consecutive_stream_bytes = bytes;
                }
            }
            FrameRoute::Connection | FrameRoute::Request(_) => {
                self.last_stream = None;
                self.consecutive_stream_bytes = 0;
            }
        }
    }
}

struct StreamState {
    kind: StreamKind,
    recovery: StreamRecoveryAction,
    queued_bytes: u64,
    queued_messages: usize,
    credit: u64,
    outstanding_credit: u64,
    last_sequence: StreamSequence,
    pending_gaps: VecDeque<StreamGap>,
    coalesced_items: u64,
    dropped_items: u64,
}

impl StreamState {
    const fn new(
        kind: StreamKind,
        recovery: StreamRecoveryAction,
        credit: u64,
        last_sequence: StreamSequence,
    ) -> Self {
        Self {
            kind,
            recovery,
            queued_bytes: 0,
            queued_messages: 0,
            credit,
            outstanding_credit: 0,
            last_sequence,
            pending_gaps: VecDeque::new(),
            coalesced_items: 0,
            dropped_items: 0,
        }
    }
}

#[derive(Default)]
struct ClassQueue {
    routes: BTreeMap<FrameRoute, VecDeque<OutboundFrame>>,
    route_order: VecDeque<FrameRoute>,
    bytes: u64,
    messages: usize,
}

impl ClassQueue {
    fn push(&mut self, frame: OutboundFrame) {
        let route = frame.route().clone();
        let bytes = frame.encoded_len();
        // Replacement can temporarily empty an existing queue while its route
        // keeps its turn. Only a new route needs another scheduling entry.
        let queue = self.routes.entry(route.clone()).or_insert_with(|| {
            self.route_order.push_back(route);
            VecDeque::new()
        });
        queue.push_back(frame);
        self.bytes = self.bytes.saturating_add(bytes);
        self.messages = self.messages.saturating_add(1);
    }

    fn pop_front(&mut self, route: &FrameRoute) -> Option<OutboundFrame> {
        let queue = self.routes.get_mut(route)?;
        let frame = queue.pop_front()?;
        self.bytes = self.bytes.saturating_sub(frame.encoded_len());
        self.messages = self.messages.saturating_sub(1);
        if queue.is_empty() {
            self.routes.remove(route);
        }
        Some(frame)
    }

    fn remove_route(&mut self, route: &FrameRoute) -> (u64, usize) {
        let Some(queue) = self.routes.remove(route) else {
            return (0, 0);
        };
        self.route_order.retain(|candidate| candidate != route);
        let messages = queue.len();
        let bytes = queue.iter().fold(0_u64, |total, frame| {
            total.saturating_add(frame.encoded_len())
        });
        self.bytes = self.bytes.saturating_sub(bytes);
        self.messages = self.messages.saturating_sub(messages);
        (bytes, messages)
    }
}
