// SPDX-License-Identifier: Apache-2.0

//! SDK-owned public values that isolate consumers from private UCMP machinery.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use undercurrent_multiplex::{
    ClientMessage, InboundStreamError, InboundStreamItem, ServerMessage, StreamGap,
    StreamGapReason, StreamRecoveryAction,
};
use undercurrent_wire::{
    ClientId, ConnectionAgreement, ConnectionHello, ConnectionLimits, DecodeStatus, HandshakeCodec,
    HandshakeDecodeStatus, HandshakeResponse, HeartbeatPolicy, NamespacedName, ProtocolVersion,
    RequestId, ServerEpoch, StreamId, StreamSequence, TraceContext, WireCodec,
};

/// Invalid SDK-owned connection, identity, or frame value.
pub struct ClientBoundaryError {
    field: &'static str,
    source: Box<dyn Error + Send + Sync>,
}

impl ClientBoundaryError {
    pub(crate) fn new(field: &'static str, source: impl Error + Send + Sync + 'static) -> Self {
        Self {
            field,
            source: Box::new(source),
        }
    }

    /// Returns the stable invalid component name.
    pub const fn field(&self) -> &'static str {
        self.field
    }
}

impl Debug for ClientBoundaryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientBoundaryError")
            .field("field", &self.field)
            .finish_non_exhaustive()
    }
}

impl Display for ClientBoundaryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "SDK client {} is invalid", self.field)
    }
}

impl Error for ClientBoundaryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// SDK-owned protocol version value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientProtocolVersion {
    /// Semantic major version.
    pub major: u16,
    /// Semantic minor version.
    pub minor: u16,
    /// Semantic patch version.
    pub patch: u16,
    /// Beta release marker.
    pub beta: bool,
}

impl ClientProtocolVersion {
    /// Sole release implemented by this SDK.
    pub const CURRENT: Self = Self::from_raw(undercurrent_wire::CURRENT_PROTOCOL_VERSION);

    /// Constructs an explicit stable version for compatibility checks.
    pub const fn new(major: u16, minor: u16) -> Self {
        Self {
            major,
            minor,
            patch: 0,
            beta: false,
        }
    }

    const fn into_raw(self) -> ProtocolVersion {
        ProtocolVersion {
            major: self.major,
            minor: self.minor,
            patch: self.patch,
            beta: self.beta,
        }
    }

    const fn from_raw(value: ProtocolVersion) -> Self {
        Self {
            major: value.major,
            minor: value.minor,
            patch: value.patch,
            beta: value.beta,
        }
    }
}

/// SDK-owned negotiated connection resource limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientConnectionLimits {
    max_frame_bytes: u32,
    max_logical_streams: u32,
    max_pending_requests: u32,
    connection_queue_bytes: u64,
    stream_queue_bytes: u64,
    initial_stream_window_bytes: u64,
}

impl ClientConnectionLimits {
    /// Constructs validated hard limits.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        max_frame_bytes: u32,
        max_logical_streams: u32,
        max_pending_requests: u32,
        connection_queue_bytes: u64,
        stream_queue_bytes: u64,
        initial_stream_window_bytes: u64,
    ) -> Result<Self, ClientBoundaryError> {
        let raw = ConnectionLimits::new(
            max_frame_bytes,
            max_logical_streams,
            max_pending_requests,
            connection_queue_bytes,
            stream_queue_bytes,
            initial_stream_window_bytes,
        )
        .map_err(|source| ClientBoundaryError::new("connection_limits", source))?;
        Ok(Self::from_raw(raw))
    }

    /// Returns maximum complete encoded frame bytes.
    pub const fn max_frame_bytes(self) -> u32 {
        self.max_frame_bytes
    }

    /// Returns maximum active logical streams.
    pub const fn max_logical_streams(self) -> u32 {
        self.max_logical_streams
    }

    /// Returns maximum pending finite requests.
    pub const fn max_pending_requests(self) -> u32 {
        self.max_pending_requests
    }

    /// Returns the connection-wide queued-byte budget.
    pub const fn connection_queue_bytes(self) -> u64 {
        self.connection_queue_bytes
    }

    /// Returns one stream's queued-byte budget.
    pub const fn stream_queue_bytes(self) -> u64 {
        self.stream_queue_bytes
    }

    /// Returns initial per-stream flow-control credit.
    pub const fn initial_stream_window_bytes(self) -> u64 {
        self.initial_stream_window_bytes
    }

    pub(crate) fn into_raw(self) -> ConnectionLimits {
        ConnectionLimits::new(
            self.max_frame_bytes,
            self.max_logical_streams,
            self.max_pending_requests,
            self.connection_queue_bytes,
            self.stream_queue_bytes,
            self.initial_stream_window_bytes,
        )
        .expect("SDK connection limits preserve their construction invariant")
    }

    pub(crate) const fn from_raw(value: ConnectionLimits) -> Self {
        Self {
            max_frame_bytes: value.max_frame_bytes(),
            max_logical_streams: value.max_logical_streams(),
            max_pending_requests: value.max_pending_requests(),
            connection_queue_bytes: value.connection_queue_bytes(),
            stream_queue_bytes: value.stream_queue_bytes(),
            initial_stream_window_bytes: value.initial_stream_window_bytes(),
        }
    }
}

/// SDK-owned heartbeat terms selected by the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientHeartbeatPolicy {
    interval_millis: u64,
    timeout_millis: u64,
}

impl ClientHeartbeatPolicy {
    /// Constructs validated heartbeat terms.
    pub fn new(interval_millis: u64, timeout_millis: u64) -> Result<Self, ClientBoundaryError> {
        let raw = HeartbeatPolicy::new(interval_millis, timeout_millis)
            .map_err(|source| ClientBoundaryError::new("heartbeat", source))?;
        Ok(Self::from_raw(raw))
    }

    /// Returns the heartbeat interval in milliseconds.
    pub const fn interval_millis(self) -> u64 {
        self.interval_millis
    }

    /// Returns the dead-peer timeout in milliseconds.
    pub const fn timeout_millis(self) -> u64 {
        self.timeout_millis
    }

    pub(crate) const fn from_raw(value: HeartbeatPolicy) -> Self {
        Self {
            interval_millis: value.interval_millis(),
            timeout_millis: value.timeout_millis(),
        }
    }
}

/// Validated SDK-owned client handshake offer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConnectionHello(ConnectionHello);

impl ClientConnectionHello {
    /// Constructs a complete bounded connection offer.
    pub fn new(
        client_id: impl Into<String>,
        protocol_version: ClientProtocolVersion,
        required_capabilities: impl IntoIterator<Item = String>,
        optional_capabilities: impl IntoIterator<Item = String>,
        limits: ClientConnectionLimits,
    ) -> Result<Self, ClientBoundaryError> {
        if protocol_version != ClientProtocolVersion::CURRENT {
            return Err(ClientBoundaryError::new(
                "protocol_version",
                UnsupportedSdkProtocolVersion,
            ));
        }
        let client_id = ClientId::new(client_id.into())
            .map_err(|source| ClientBoundaryError::new("client_id", source))?;
        let protocol_version = protocol_version.into_raw();
        let required_capabilities = capability_set(required_capabilities, "required_capabilities")?;
        let optional_capabilities = capability_set(optional_capabilities, "optional_capabilities")?;
        ConnectionHello::new(
            client_id,
            protocol_version,
            required_capabilities,
            optional_capabilities,
            limits.into_raw(),
        )
        .map(Self)
        .map_err(|source| ClientBoundaryError::new("hello", source))
    }

    /// Returns the logical client identity.
    pub fn client_id(&self) -> &str {
        self.0.client_id().as_str()
    }

    /// Returns the sole UCMP wire version offered by this client.
    pub fn protocol_version(&self) -> ClientProtocolVersion {
        ClientProtocolVersion::from_raw(self.0.protocol_version())
    }

    /// Returns required capability names in deterministic order.
    pub fn required_capabilities(&self) -> Vec<&str> {
        self.0
            .required_capabilities()
            .iter()
            .map(NamespacedName::as_str)
            .collect()
    }

    /// Returns optional capability names in deterministic order.
    pub fn optional_capabilities(&self) -> Vec<&str> {
        self.0
            .optional_capabilities()
            .iter()
            .map(NamespacedName::as_str)
            .collect()
    }

    /// Returns client-side hard resource maxima.
    pub const fn limits(&self) -> ClientConnectionLimits {
        ClientConnectionLimits::from_raw(self.0.limits())
    }

    pub(crate) const fn as_raw(&self) -> &ConnectionHello {
        &self.0
    }
}

/// Immutable negotiated connection terms safe for SDK consumers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConnectionAgreement(ConnectionAgreement);

impl ClientConnectionAgreement {
    /// Decodes one complete accepted UCHS response without exposing handshake internals.
    pub fn decode_accepted(
        bytes: &[u8],
        maximum_frame_bytes: u32,
    ) -> Result<Self, ClientBoundaryError> {
        let codec = HandshakeCodec::new(maximum_frame_bytes)
            .map_err(|source| ClientBoundaryError::new("handshake_limit", source))?;
        match codec
            .decode_response(bytes)
            .map_err(|source| ClientBoundaryError::new("agreement", source))?
        {
            HandshakeDecodeStatus::Complete(frame) if frame.consumed == bytes.len() => {
                match frame.message {
                    HandshakeResponse::Accepted(agreement) => Self::try_from_raw(agreement),
                    HandshakeResponse::Rejected(_) => Err(ClientBoundaryError::new(
                        "agreement",
                        IncompleteTransportFrame,
                    )),
                }
            }
            _ => Err(ClientBoundaryError::new(
                "agreement",
                IncompleteTransportFrame,
            )),
        }
    }

    /// Returns the physical connection identity.
    pub fn connection_id(&self) -> &str {
        self.0.connection_id().as_str()
    }

    /// Returns the server process epoch governing replay validity.
    pub fn server_epoch(&self) -> &str {
        self.0.server_epoch().as_str()
    }

    /// Returns the selected protocol version.
    pub const fn protocol_version(&self) -> ClientProtocolVersion {
        ClientProtocolVersion::from_raw(self.0.protocol_version())
    }

    /// Returns enabled capability names in deterministic order.
    pub fn capabilities(&self) -> Vec<&str> {
        self.0
            .capabilities()
            .iter()
            .map(NamespacedName::as_str)
            .collect()
    }

    /// Returns whether one capability was negotiated.
    pub fn has_capability(&self, capability: &str) -> bool {
        self.0
            .capabilities()
            .iter()
            .any(|enabled| enabled.as_str() == capability)
    }

    /// Returns negotiated hard limits.
    pub const fn limits(&self) -> ClientConnectionLimits {
        ClientConnectionLimits::from_raw(self.0.limits())
    }

    /// Returns server-selected heartbeat terms.
    pub const fn heartbeat(&self) -> ClientHeartbeatPolicy {
        ClientHeartbeatPolicy::from_raw(self.0.heartbeat())
    }

    /// Returns replay retention when resume is enabled.
    pub const fn resume_retention_millis(&self) -> Option<u64> {
        self.0.resume_retention_millis()
    }

    pub(crate) fn into_raw(self) -> ConnectionAgreement {
        self.0
    }

    pub(crate) fn try_from_raw(value: ConnectionAgreement) -> Result<Self, ClientBoundaryError> {
        if value.protocol_version() != ClientProtocolVersion::CURRENT.into_raw() {
            return Err(ClientBoundaryError::new(
                "protocol_version",
                UnsupportedSdkProtocolVersion,
            ));
        }
        Ok(Self(value))
    }

    pub(crate) const fn from_raw(value: ConnectionAgreement) -> Self {
        Self(value)
    }
}

/// Validated client-scoped logical stream identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientStreamId(StreamId);

impl ClientStreamId {
    /// Constructs a bounded stream identity.
    pub fn new(value: impl Into<String>) -> Result<Self, ClientBoundaryError> {
        StreamId::new(value.into())
            .map(Self)
            .map_err(|source| ClientBoundaryError::new("stream_id", source))
    }

    /// Returns the encoded identity.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub(crate) fn into_raw(self) -> StreamId {
        self.0
    }

    pub(crate) const fn from_raw(value: StreamId) -> Self {
        Self(value)
    }
}

/// Validated server process epoch.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientServerEpoch(ServerEpoch);

impl ClientServerEpoch {
    /// Constructs a bounded server epoch.
    pub fn new(value: impl Into<String>) -> Result<Self, ClientBoundaryError> {
        ServerEpoch::new(value.into())
            .map(Self)
            .map_err(|source| ClientBoundaryError::new("server_epoch", source))
    }

    /// Returns the encoded epoch.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub(crate) fn into_raw(self) -> ServerEpoch {
        self.0
    }

    pub(crate) const fn from_raw(value: ServerEpoch) -> Self {
        Self(value)
    }
}

/// Monotonic sequence within one SDK logical stream.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientStreamSequence(u64);

impl ClientStreamSequence {
    /// Initial sequence before the first item.
    pub const INITIAL: Self = Self(0);

    /// Reconstructs a sequence from its numeric value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric sequence.
    pub const fn value(self) -> u64 {
        self.0
    }

    pub(crate) const fn into_raw(self) -> StreamSequence {
        StreamSequence::new(self.0)
    }

    pub(crate) const fn from_raw(value: StreamSequence) -> Self {
        Self(value.value())
    }
}

/// Validated distributed context owned by the SDK API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientTraceContext(TraceContext);

impl ClientTraceContext {
    /// Constructs a trace and current span context.
    pub fn new(
        trace_id: impl Into<String>,
        span_id: impl Into<String>,
    ) -> Result<Self, ClientBoundaryError> {
        let trace_id = undercurrent_wire::TraceId::new(trace_id.into())
            .map_err(|source| ClientBoundaryError::new("trace_id", source))?;
        let span_id = undercurrent_wire::SpanId::new(span_id.into())
            .map_err(|source| ClientBoundaryError::new("span_id", source))?;
        Ok(Self(TraceContext::new(trace_id, span_id)))
    }

    /// Adds a validated parent span identity.
    pub fn with_parent_span_id(
        mut self,
        parent_span_id: impl Into<String>,
    ) -> Result<Self, ClientBoundaryError> {
        let parent_span_id = undercurrent_wire::SpanId::new(parent_span_id.into())
            .map_err(|source| ClientBoundaryError::new("parent_span_id", source))?;
        self.0 = self.0.with_parent_span_id(parent_span_id);
        Ok(self)
    }

    /// Adds a validated correlation identity.
    pub fn with_correlation_id(
        mut self,
        correlation_id: impl Into<String>,
    ) -> Result<Self, ClientBoundaryError> {
        let correlation_id = undercurrent_wire::CorrelationId::new(correlation_id.into())
            .map_err(|source| ClientBoundaryError::new("correlation_id", source))?;
        self.0 = self.0.with_correlation_id(correlation_id);
        Ok(self)
    }

    /// Returns the trace identity.
    pub fn trace_id(&self) -> &str {
        self.0.trace_id.as_str()
    }

    /// Returns the current span identity.
    pub fn span_id(&self) -> &str {
        self.0.span_id.as_str()
    }

    /// Returns the optional parent span identity.
    pub fn parent_span_id(&self) -> Option<&str> {
        self.0
            .parent_span_id
            .as_ref()
            .map(undercurrent_wire::SpanId::as_str)
    }

    /// Returns the optional correlation identity.
    pub fn correlation_id(&self) -> Option<&str> {
        self.0
            .correlation_id
            .as_ref()
            .map(undercurrent_wire::CorrelationId::as_str)
    }

    pub(crate) fn into_raw(self) -> TraceContext {
        self.0
    }

    pub(crate) const fn from_raw(value: TraceContext) -> Self {
        Self(value)
    }
}

/// Deterministic recovery action for an SDK stream gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClientStreamRecovery {
    /// Request an authoritative snapshot.
    RequestSnapshot,
    /// Restart the filtered query.
    RestartQuery,
    /// Restart terminal output at the earliest available offset.
    RestartFromAvailableOffset,
}

/// Stable cause of a non-contiguous SDK stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClientStreamGapReason {
    /// Droppable live data exceeded a queue.
    LiveQueueOverflow,
    /// Requested sequence was evicted.
    ReplayEvicted,
    /// No replay journal was available.
    ReplayUnavailable,
    /// A daemon epoch change invalidated replay.
    ServerEpochChanged,
    /// The source evicted data before consumption.
    SourceRetentionEvicted,
}

/// Inclusive missing interval in one SDK logical stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientStreamGap {
    /// Affected logical stream.
    pub stream_id: ClientStreamId,
    /// First missing sequence.
    pub first_missing: ClientStreamSequence,
    /// Last missing sequence.
    pub last_missing: ClientStreamSequence,
    /// Stable reason for the gap.
    pub reason: ClientStreamGapReason,
    /// Required deterministic recovery action.
    pub recovery: ClientStreamRecovery,
}

/// One ordered SDK logical-stream item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientStreamItem {
    /// Ordered encoded product payload.
    Data {
        /// Stream-local sequence.
        sequence: ClientStreamSequence,
        /// Exact product payload bytes.
        bytes: Arc<[u8]>,
        /// Full flow-controlled frame bytes.
        flow_controlled_bytes: u64,
    },
    /// Explicit discontinuity and recovery action.
    Gap(ClientStreamGap),
}

impl ClientStreamItem {
    /// Returns the sequence consumed by this item.
    pub const fn sequence(&self) -> ClientStreamSequence {
        match self {
            Self::Data { sequence, .. } => *sequence,
            Self::Gap(gap) => gap.last_missing,
        }
    }

    pub(crate) fn from_raw(value: InboundStreamItem) -> Self {
        match value {
            InboundStreamItem::Data {
                sequence,
                bytes,
                flow_controlled_bytes,
            } => Self::Data {
                sequence: ClientStreamSequence::from_raw(sequence),
                bytes,
                flow_controlled_bytes,
            },
            InboundStreamItem::Gap(gap) => Self::Gap(ClientStreamGap::from_raw(gap)),
        }
    }
}

impl ClientStreamGap {
    fn from_raw(value: StreamGap) -> Self {
        Self {
            stream_id: ClientStreamId::from_raw(value.stream_id),
            first_missing: ClientStreamSequence::from_raw(value.first_missing),
            last_missing: ClientStreamSequence::from_raw(value.last_missing),
            reason: match value.reason {
                StreamGapReason::LiveQueueOverflow => ClientStreamGapReason::LiveQueueOverflow,
                StreamGapReason::ReplayEvicted => ClientStreamGapReason::ReplayEvicted,
                StreamGapReason::ReplayUnavailable => ClientStreamGapReason::ReplayUnavailable,
                StreamGapReason::ServerEpochChanged => ClientStreamGapReason::ServerEpochChanged,
                StreamGapReason::SourceRetentionEvicted => {
                    ClientStreamGapReason::SourceRetentionEvicted
                }
            },
            recovery: match value.recovery {
                StreamRecoveryAction::RequestSnapshot => ClientStreamRecovery::RequestSnapshot,
                StreamRecoveryAction::RestartQuery => ClientStreamRecovery::RestartQuery,
                StreamRecoveryAction::RestartFromAvailableOffset => {
                    ClientStreamRecovery::RestartFromAvailableOffset
                }
            },
        }
    }
}

/// Caller-visible logical-stream receive failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientStreamReceiveError {
    /// Caller wait deadline elapsed.
    Timeout,
    /// Stream was independently closed.
    Unsubscribed,
    /// Stream was cancelled.
    Cancelled,
    /// The connection terminated because a bounded resource was overloaded.
    ///
    /// Callers should follow the embedded retry guidance rather than treating
    /// this as an ordinary connection closure.
    Overloaded {
        /// Safe bounded resource context and retry guidance.
        overload: crate::ClientOverload,
    },
    /// Physical connection closed.
    ConnectionClosed,
}

impl ClientStreamReceiveError {
    /// Returns typed overload context when overload terminated the stream.
    #[must_use]
    pub const fn overload(&self) -> Option<crate::ClientOverload> {
        match self {
            Self::Overloaded { overload } => Some(*overload),
            Self::Timeout | Self::Unsubscribed | Self::Cancelled | Self::ConnectionClosed => None,
        }
    }
}

impl Display for ClientStreamReceiveError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Timeout => "SDK stream receive timed out",
            Self::Unsubscribed => "SDK stream is unsubscribed",
            Self::Cancelled => "SDK stream was cancelled",
            Self::Overloaded { .. } => {
                "SDK stream connection closed by overload; use retry guidance"
            }
            Self::ConnectionClosed => "SDK stream connection closed",
        })
    }
}

impl Error for ClientStreamReceiveError {}

impl From<InboundStreamError> for ClientStreamReceiveError {
    fn from(value: InboundStreamError) -> Self {
        match value {
            InboundStreamError::Timeout => Self::Timeout,
            InboundStreamError::Unsubscribed => Self::Unsubscribed,
            InboundStreamError::Overloaded => Self::Overloaded {
                overload: crate::ClientOverload::new(
                    "client_stream_mailbox",
                    None,
                    None,
                    None,
                    crate::ClientRetryGuidance::AfterQueueDrain,
                ),
            },
            InboundStreamError::Cancelled => Self::Cancelled,
            InboundStreamError::ConnectionClosed => Self::ConnectionClosed,
        }
    }
}

/// Semantic view of one outbound transport command for adapters and tests.
///
/// Request and stream variants contain exact payload bytes. Derived `Debug`
/// includes those bytes and must not be used as a payload-redacted diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientTransportCommand {
    /// Finite request.
    Request {
        /// Request correlation identity.
        request_id: String,
        /// Distributed trace context.
        trace_context: ClientTraceContext,
        /// Exact product payload.
        payload: Arc<[u8]>,
    },
    /// Best-effort request cancellation.
    CancelRequest { request_id: String },
    /// Logical stream open.
    OpenStream {
        request_id: String,
        stream_id: ClientStreamId,
        trace_context: ClientTraceContext,
        payload: Arc<[u8]>,
    },
    /// Logical stream resume.
    ResumeStream {
        request_id: String,
        stream_id: ClientStreamId,
        server_epoch: ClientServerEpoch,
        after: ClientStreamSequence,
        trace_context: ClientTraceContext,
        payload: Arc<[u8]>,
    },
    /// Independent stream close.
    CloseStream { stream_id: ClientStreamId },
    /// Returned flow-control credit.
    WindowUpdate {
        stream_id: ClientStreamId,
        additional_bytes: u64,
    },
    /// Heartbeat probe.
    Ping,
    /// Graceful connection close.
    Close,
}

/// Opaque outbound transport frame owned by the SDK.
///
/// Its `Debug` implementation uses [`ClientTransportCommand`] and can expose
/// payload bytes. Prefer stable diagnostic names when recording transport activity.
#[derive(Clone)]
pub struct ClientTransportOutbound(ClientMessage);

impl ClientTransportOutbound {
    /// Returns an SDK-owned semantic view for adapters and test ports.
    pub fn command(&self) -> ClientTransportCommand {
        match &self.0 {
            ClientMessage::Request {
                request_id,
                trace_context,
                payload,
            } => ClientTransportCommand::Request {
                request_id: request_id.as_str().to_owned(),
                trace_context: ClientTraceContext::from_raw(trace_context.clone()),
                payload: payload.clone(),
            },
            ClientMessage::CancelRequest { request_id } => ClientTransportCommand::CancelRequest {
                request_id: request_id.as_str().to_owned(),
            },
            ClientMessage::OpenStream {
                request_id,
                stream_id,
                trace_context,
                payload,
            } => ClientTransportCommand::OpenStream {
                request_id: request_id.as_str().to_owned(),
                stream_id: ClientStreamId::from_raw(stream_id.clone()),
                trace_context: ClientTraceContext::from_raw(trace_context.clone()),
                payload: payload.clone(),
            },
            ClientMessage::ResumeStream {
                request_id,
                stream_id,
                server_epoch,
                after,
                trace_context,
                payload,
            } => ClientTransportCommand::ResumeStream {
                request_id: request_id.as_str().to_owned(),
                stream_id: ClientStreamId::from_raw(stream_id.clone()),
                server_epoch: ClientServerEpoch::from_raw(server_epoch.clone()),
                after: ClientStreamSequence::from_raw(*after),
                trace_context: ClientTraceContext::from_raw(trace_context.clone()),
                payload: payload.clone(),
            },
            ClientMessage::CloseStream { stream_id } => ClientTransportCommand::CloseStream {
                stream_id: ClientStreamId::from_raw(stream_id.clone()),
            },
            ClientMessage::WindowUpdate {
                stream_id,
                additional_bytes,
            } => ClientTransportCommand::WindowUpdate {
                stream_id: ClientStreamId::from_raw(stream_id.clone()),
                additional_bytes: *additional_bytes,
            },
            ClientMessage::Ping => ClientTransportCommand::Ping,
            ClientMessage::Close => ClientTransportCommand::Close,
        }
    }

    /// Encodes one complete private UCMP frame for a byte transport.
    pub fn encode(&self, maximum_frame_bytes: u32) -> Result<Vec<u8>, ClientBoundaryError> {
        let codec = WireCodec::new(maximum_frame_bytes)
            .map_err(|source| ClientBoundaryError::new("transport_frame_limit", source))?;
        codec
            .encode_client(&self.0)
            .map_err(|source| ClientBoundaryError::new("outbound_transport_frame", source))
    }

    /// Returns whether the frame requests graceful physical close.
    pub const fn closes_connection(&self) -> bool {
        matches!(self.0, ClientMessage::Close)
    }

    pub(crate) const fn from_raw(value: ClientMessage) -> Self {
        Self(value)
    }
}

impl Debug for ClientTransportOutbound {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientTransportOutbound")
            .field("command", &self.command())
            .finish()
    }
}

/// Opaque inbound transport frame validated by the SDK codec.
#[derive(Clone)]
pub struct ClientTransportInbound(ServerMessage);

impl ClientTransportInbound {
    /// Decodes exactly one complete private UCMP server frame.
    pub fn decode(bytes: &[u8], maximum_frame_bytes: u32) -> Result<Self, ClientBoundaryError> {
        let codec = WireCodec::new(maximum_frame_bytes)
            .map_err(|source| ClientBoundaryError::new("transport_frame_limit", source))?;
        match codec
            .decode_server(bytes)
            .map_err(|source| ClientBoundaryError::new("inbound_transport_frame", source))?
        {
            DecodeStatus::Complete(frame) if frame.consumed == bytes.len() => {
                Ok(Self(frame.message))
            }
            _ => Err(ClientBoundaryError::new(
                "inbound_transport_frame",
                IncompleteTransportFrame,
            )),
        }
    }

    /// Constructs one finite response for a custom connector or test port.
    pub fn response(
        request_id: impl Into<String>,
        payload: Arc<[u8]>,
    ) -> Result<Self, ClientBoundaryError> {
        let request_id = RequestId::new(request_id.into())
            .map_err(|source| ClientBoundaryError::new("request_id", source))?;
        Ok(Self(ServerMessage::Response {
            request_id,
            payload,
        }))
    }

    /// Constructs one ordered stream data item.
    pub fn stream_data(
        stream_id: ClientStreamId,
        sequence: ClientStreamSequence,
        bytes: Arc<[u8]>,
        flow_controlled_bytes: u64,
    ) -> Self {
        Self(ServerMessage::StreamItem {
            stream_id: stream_id.0,
            item: InboundStreamItem::Data {
                sequence: sequence.into_raw(),
                bytes,
                flow_controlled_bytes,
            },
        })
    }

    /// Constructs a heartbeat response.
    pub const fn pong() -> Self {
        Self(ServerMessage::Pong)
    }

    /// Constructs a server-initiated close notification.
    pub const fn close() -> Self {
        Self(ServerMessage::Close)
    }

    /// Returns whether the frame closes the physical connection.
    pub const fn closes_connection(&self) -> bool {
        matches!(self.0, ServerMessage::Close)
    }

    pub(crate) fn into_raw(self) -> ServerMessage {
        self.0
    }
}

impl Debug for ClientTransportInbound {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ClientTransportInbound([REDACTED])")
    }
}

#[derive(Debug)]
struct IncompleteTransportFrame;

#[derive(Debug)]
struct UnsupportedSdkProtocolVersion;

impl Display for UnsupportedSdkProtocolVersion {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .write_str("the SDK supports Undercurrent connection protocol version 0.1.0-beta only")
    }
}

impl Error for UnsupportedSdkProtocolVersion {}

impl Display for IncompleteTransportFrame {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("transport frame is incomplete or contains trailing bytes")
    }
}

impl Error for IncompleteTransportFrame {}

fn capability_set(
    values: impl IntoIterator<Item = String>,
    field: &'static str,
) -> Result<BTreeSet<NamespacedName>, ClientBoundaryError> {
    values
        .into_iter()
        .map(NamespacedName::new)
        .collect::<Result<_, _>>()
        .map_err(|source| ClientBoundaryError::new(field, source))
}
