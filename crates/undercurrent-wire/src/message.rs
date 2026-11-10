// SPDX-License-Identifier: Apache-2.0

//! Semantic UCMP 0.1.0-beta messages shared by codecs and multiplex runtimes.
//!
//! These are values, not operations: encoding cancellation, close, or resume
//! does not execute it. Product bytes remain opaque, and runtime owners enforce
//! request correlation, stream ordering, replay, and flow-control policy.

use std::sync::Arc;

use crate::{RequestId, ServerEpoch, StreamId, StreamSequence, TraceContext};

/// Client action required when replay or live delivery contains a gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamRecoveryAction {
    /// Request an authoritative state or screen snapshot.
    RequestSnapshot,
    /// Restart the filtered query from a new snapshot revision.
    RestartQuery,
    /// Restart terminal output from the earliest available byte offset.
    RestartFromAvailableOffset,
}

/// Why one logical stream cannot provide a contiguous sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamGapReason {
    /// Droppable live data exceeded a bounded scheduler queue.
    LiveQueueOverflow,
    /// Requested sequence was evicted from bounded replay retention.
    ReplayEvicted,
    /// No retained journal exists for the requested stream.
    ReplayUnavailable,
    /// Server process epoch changed and invalidated in-memory replay.
    ServerEpochChanged,
    /// The application source evicted data before its checkpoint was consumed.
    SourceRetentionEvicted,
}

/// Inclusive missing sequence interval for one logical stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamGap {
    /// Affected logical stream.
    pub stream_id: StreamId,
    /// First missing sequence.
    pub first_missing: StreamSequence,
    /// Last missing sequence.
    pub last_missing: StreamSequence,
    /// Stable reason data is unavailable.
    pub reason: StreamGapReason,
    /// Required deterministic client recovery action.
    pub recovery: StreamRecoveryAction,
}

/// One routed logical stream item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboundStreamItem {
    /// Ordered encoded stream frame.
    Data {
        /// Monotonic stream-local sequence.
        sequence: StreamSequence,
        /// Exact product payload bytes retained by the client mailbox.
        bytes: Arc<[u8]>,
        /// Full flow-controlled frame bytes consumed from server windows.
        /// The wire decoder computes this from frame length; encoding ignores it.
        flow_controlled_bytes: u64,
    },
    /// Explicit live or replay discontinuity and recovery action.
    Gap(StreamGap),
}

impl InboundStreamItem {
    /// Returns the data sequence or the last missing sequence of a gap.
    pub const fn sequence(&self) -> StreamSequence {
        match self {
            Self::Data { sequence, .. } => *sequence,
            // Mailbox consumption advances the resume checkpoint past reported
            // loss; it does not restore the missing product state or bytes.
            // See [gap recovery](../docs/internals/flows.md#recover-after-disconnection-or-a-gap).
            Self::Gap(gap) => gap.last_missing,
        }
    }

    /// Returns data frame credit, or zero for a gap control item.
    pub const fn encoded_len(&self) -> u64 {
        // This is the client's data-credit charge, not the serialized size.
        // Gaps still occupy a mailbox message slot even though they add no bytes
        // to its credit accounting or to a returned WindowUpdate.
        // See [message accounting](../docs/reference/wire-format.md#ucmp-messages).
        match self {
            Self::Data {
                flow_controlled_bytes,
                ..
            } => *flow_controlled_bytes,
            Self::Gap(_) => 0,
        }
    }
}

/// Semantic client-to-server message encoded by a transport adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientMessage {
    /// Finite correlated request.
    Request {
        request_id: RequestId,
        trace_context: TraceContext,
        payload: Arc<[u8]>,
    },
    /// Best-effort cancellation for no-longer-useful work.
    CancelRequest { request_id: RequestId },
    /// Opens one logical stream.
    OpenStream {
        request_id: RequestId,
        stream_id: StreamId,
        trace_context: TraceContext,
        payload: Arc<[u8]>,
    },
    /// Requests replay after the supplied stream sequence in a prior server epoch.
    /// Replay availability and gap recovery are decided by the runtime owners.
    ResumeStream {
        request_id: RequestId,
        stream_id: StreamId,
        server_epoch: ServerEpoch,
        after: StreamSequence,
        trace_context: TraceContext,
        payload: Arc<[u8]>,
    },
    /// Independently unsubscribes one stream.
    CloseStream { stream_id: StreamId },
    /// Returns consumed connection and stream credit.
    WindowUpdate {
        stream_id: StreamId,
        additional_bytes: u64,
    },
    /// Application heartbeat probe.
    Ping,
    /// Graceful connection close request.
    Close,
}

/// Semantic server-to-client message decoded by a transport adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerMessage {
    /// Finite correlated response.
    Response {
        request_id: RequestId,
        payload: Arc<[u8]>,
    },
    /// One ordered item for an open logical stream.
    StreamItem {
        stream_id: StreamId,
        item: InboundStreamItem,
    },
    /// Heartbeat response.
    Pong,
    /// Server-initiated terminal notification.
    Close,
}
