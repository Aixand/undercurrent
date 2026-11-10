// SPDX-License-Identifier: Apache-2.0

//! Product-specific semantic frames scheduled over one ordered transport.
//!
//! Frames share already encoded bytes; constructors validate size, not payload
//! syntax or agreement between encoded IDs and semantic route metadata. Adapters
//! choose class, delivery policy, and replay disposition. Stream kind alone does
//! not select policy or expose a public product subscription.

use std::sync::Arc;

use undercurrent_wire::{RequestId, StreamId, StreamSequence};

use crate::{MultiplexError, StreamGap};

/// Semantic outbound traffic classes in descending operational importance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TrafficClass {
    /// Protocol control, RPC responses, errors, close, heartbeat, and credit.
    Control,
    /// Authoritative lifecycle transitions and input/control results.
    Lifecycle,
    /// Replaceable foreground, health, WorkContext, and query state.
    State,
    /// Terminal output and bulk diagnostic payloads.
    Bulk,
}

impl TrafficClass {
    /// Complete traffic-class registry in scheduling priority order.
    pub const ALL: [Self; 4] = [Self::Control, Self::Lifecycle, Self::State, Self::Bulk];

    /// Returns the stable snake-case class name used by diagnostics and metrics.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Control => "control",
            Self::Lifecycle => "lifecycle",
            Self::State => "state",
            Self::Bulk => "bulk",
        }
    }

    pub(crate) const fn index(self) -> usize {
        match self {
            Self::Control => 0,
            Self::Lifecycle => 1,
            Self::State => 2,
            Self::Bulk => 3,
        }
    }
}

/// Undercurrent logical stream kinds registered at the product boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StreamKind {
    /// Filtered prepared session-page changes.
    SessionQuery,
    /// Authoritative lifecycle and compact session changes.
    SessionEvents,
    /// Selective terminal output for one explicitly opened session.
    TerminalOutput,
    /// Retained application or session logs.
    Logs,
    /// Bounded operational diagnostic events.
    Diagnostics,
}

impl StreamKind {
    /// Complete product stream-kind registry.
    pub const ALL: [Self; 5] = [
        Self::SessionQuery,
        Self::SessionEvents,
        Self::TerminalOutput,
        Self::Logs,
        Self::Diagnostics,
    ];

    /// Returns the stable protocol name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SessionQuery => "session_query",
            Self::SessionEvents => "session_events",
            Self::TerminalOutput => "terminal_output",
            Self::Logs => "logs",
            Self::Diagnostics => "diagnostics",
        }
    }
}

/// Admission and replacement policy for one outbound item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeliverySemantics {
    /// Saturation rejects the item before admission instead of silently dropping it.
    /// This does not guarantee delivery after admission or across connection loss.
    Reliable,
    /// The last replaceable item on the same route and class may be replaced,
    /// including before saturation, when the replacement fits.
    Replaceable,
    /// Item may be dropped when saturated, with an explicit stream gap.
    Droppable,
}

/// Whether a successfully written stream frame advances replay retention.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReplayDisposition {
    /// Record this newly produced frame after physical transport write succeeds.
    RecordAfterWrite,
    /// Skip recording against the existing journal. Used for replayed bytes or
    /// an adapter-generated recovery gap against an already known boundary.
    AlreadyRetained,
}

/// Wire semantics needed to preserve flow credit through retained stream replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamFrameKind {
    /// Product data whose complete encoded length consumes data credit.
    Data,
    /// A loss notification for which consumers return no data credit.
    Gap,
}

/// Semantic destination used for per-route fairness and correlation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FrameRoute {
    /// Connection-scoped protocol traffic.
    Connection,
    /// Finite RPC response correlated to one pending request.
    Request(RequestId),
    /// Ordered logical stream traffic.
    Stream(StreamId),
}

/// One already encoded, size-accounted outbound transport frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundFrame {
    class: TrafficClass,
    route: FrameRoute,
    sequence: Option<StreamSequence>,
    semantics: DeliverySemantics,
    replay: Option<ReplayDisposition>,
    kind: StreamFrameKind,
    bytes: Arc<[u8]>,
}

impl OutboundFrame {
    /// Constructs connection-scoped control traffic.
    ///
    /// # Errors
    ///
    /// Returns a typed size failure for an empty frame or bytes above the
    /// negotiated frame maximum. This function performs no IO and does not panic.
    pub fn control(bytes: Arc<[u8]>, max_frame_bytes: u32) -> Result<Self, MultiplexError> {
        validate_frame_size(bytes.len(), max_frame_bytes)?;
        Ok(Self {
            class: TrafficClass::Control,
            route: FrameRoute::Connection,
            sequence: None,
            semantics: DeliverySemantics::Reliable,
            replay: None,
            kind: StreamFrameKind::Data,
            bytes,
        })
    }

    /// Constructs a reliable finite response frame.
    ///
    /// # Errors
    ///
    /// Returns a typed size failure for an empty or oversized frame. This
    /// function performs no IO and does not panic.
    pub fn response(
        request_id: RequestId,
        bytes: Arc<[u8]>,
        max_frame_bytes: u32,
    ) -> Result<Self, MultiplexError> {
        validate_frame_size(bytes.len(), max_frame_bytes)?;
        Ok(Self {
            class: TrafficClass::Control,
            route: FrameRoute::Request(request_id),
            sequence: None,
            semantics: DeliverySemantics::Reliable,
            replay: None,
            kind: StreamFrameKind::Data,
            bytes,
        })
    }

    /// Constructs one ordered logical stream frame.
    ///
    /// # Errors
    ///
    /// Returns a typed size failure for an empty or oversized frame. Sequence
    /// ordering is validated when the frame enters a registered scheduler
    /// stream. This function performs no IO and does not panic.
    pub fn stream(
        class: TrafficClass,
        stream_id: StreamId,
        sequence: StreamSequence,
        semantics: DeliverySemantics,
        bytes: Arc<[u8]>,
        max_frame_bytes: u32,
    ) -> Result<Self, MultiplexError> {
        validate_frame_size(bytes.len(), max_frame_bytes)?;
        Ok(Self {
            class,
            route: FrameRoute::Stream(stream_id),
            sequence: Some(sequence),
            semantics,
            replay: Some(ReplayDisposition::RecordAfterWrite),
            kind: StreamFrameKind::Data,
            bytes,
        })
    }

    /// Constructs a reliable stream frame that must not be recorded again.
    ///
    /// Typically these are exact replay bytes. An adapter can also use this for
    /// a recovery gap against an existing journal; provenance is not validated.
    ///
    /// # Errors
    ///
    /// Returns a typed size failure for an empty or oversized frame. This
    /// function performs no IO and does not panic.
    pub fn replayed_stream(
        class: TrafficClass,
        stream_id: StreamId,
        sequence: StreamSequence,
        bytes: Arc<[u8]>,
        max_frame_bytes: u32,
    ) -> Result<Self, MultiplexError> {
        validate_frame_size(bytes.len(), max_frame_bytes)?;
        Ok(Self {
            class,
            route: FrameRoute::Stream(stream_id),
            sequence: Some(sequence),
            semantics: DeliverySemantics::Reliable,
            replay: Some(ReplayDisposition::AlreadyRetained),
            kind: StreamFrameKind::Data,
            bytes,
        })
    }

    /// Constructs a reliable encoded gap that consumes no data credit.
    ///
    /// Bytes must encode a gap with the supplied stream ID and ending sequence;
    /// this constructor trusts the adapter's encoding. Retained gaps should use
    /// the same class as neighboring replay frames to preserve their FIFO order.
    /// The replay disposition determines whether successful writes are recorded.
    ///
    /// # Errors
    ///
    /// Returns a typed size failure for an empty or oversized frame.
    pub fn gap(
        class: TrafficClass,
        stream_id: StreamId,
        sequence: StreamSequence,
        bytes: Arc<[u8]>,
        disposition: ReplayDisposition,
        max_frame_bytes: u32,
    ) -> Result<Self, MultiplexError> {
        let mut frame = Self::replayed_stream(class, stream_id, sequence, bytes, max_frame_bytes)?;
        frame.kind = StreamFrameKind::Gap;
        frame.replay = Some(disposition);
        Ok(frame)
    }

    /// Returns the stream wire kind; only meaningful for a stream route.
    pub const fn stream_frame_kind(&self) -> StreamFrameKind {
        self.kind
    }

    /// Returns the complete encoded charge, or zero for control traffic and gaps.
    pub fn flow_controlled_bytes(&self) -> u64 {
        if self.class == TrafficClass::Control || self.kind == StreamFrameKind::Gap {
            0
        } else {
            self.encoded_len()
        }
    }

    /// Returns the semantic traffic class.
    pub const fn class(&self) -> TrafficClass {
        self.class
    }

    /// Returns the connection, request, or stream fairness route.
    pub const fn route(&self) -> &FrameRoute {
        &self.route
    }

    /// Returns stream-local sequence for logical stream traffic.
    pub const fn sequence(&self) -> Option<StreamSequence> {
        self.sequence
    }

    /// Returns configured saturation behavior.
    pub const fn semantics(&self) -> DeliverySemantics {
        self.semantics
    }

    /// Returns replay retention behavior for stream frames.
    pub const fn replay_disposition(&self) -> Option<ReplayDisposition> {
        self.replay
    }

    /// Returns exact encoded bytes retained by the bounded scheduler.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns shared exact encoded bytes without copying the payload.
    pub fn shared_bytes(&self) -> Arc<[u8]> {
        self.bytes.clone()
    }

    /// Returns exact encoded frame size charged to queues and flow control.
    pub fn encoded_len(&self) -> u64 {
        u64::try_from(self.bytes.len()).unwrap_or(u64::MAX)
    }
}

/// Next semantic item selected by the outbound scheduler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduledFrame {
    /// Already encoded transport frame.
    Data(OutboundFrame),
    /// Typed gap selected after queued predecessors before its first missing
    /// sequence. The adapter must encode it and preserve selection order.
    Gap(StreamGap),
}

fn validate_frame_size(actual: usize, maximum: u32) -> Result<(), MultiplexError> {
    if actual == 0 || actual > maximum as usize {
        return Err(MultiplexError::InvalidFrameSize {
            actual: u64::try_from(actual).unwrap_or(u64::MAX),
            maximum: u64::from(maximum),
        });
    }
    Ok(())
}
