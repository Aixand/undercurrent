// SPDX-License-Identifier: Apache-2.0

//! Binary encoder and stateless incremental decoder with a full-frame size bound.
//!
//! UCMP uses a 22-byte header and direction-specific message kinds. The caller
//! retains partial input; complete results consume one frame even when later
//! frames are already buffered. Decoded payloads are owned copies in `Arc`.
//! Frame-size bounds do not bound caller buffers or aggregate allocator usage.

use std::sync::Arc;

use crate::{
    ClientMessage, InboundStreamItem, ServerMessage, StreamGap, StreamGapReason,
    StreamRecoveryAction,
};
use crate::{
    CorrelationId, RequestId, ServerEpoch, SpanId, StreamId, StreamSequence, TraceContext, TraceId,
};

use crate::{WireDirection, WireError, WireMessageKind};

const MAGIC: [u8; 4] = *b"UCMP";
const WIRE_VERSION: u8 = undercurrent_foundation::RELEASE_VERSION.len() as u8;
const RELEASE_BYTES: &[u8] = undercurrent_foundation::RELEASE_VERSION.as_bytes();
const HEADER_BYTES: usize = 12 + RELEASE_BYTES.len();

/// Maximum non-payload bytes in the largest client frame at wire ID limits.
///
/// Configuration uses this reservation so every valid request identifier and
/// trace context can coexist with the advertised product payload maximum.
pub const MAX_CLIENT_FRAME_ENVELOPE_BYTES: u32 =
    (HEADER_BYTES + 7 * (2 + crate::MAX_IDENTIFIER_LEN) + 2 + size_of::<u64>()) as u32;

/// One complete decoded frame and bytes consumed from the caller buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedFrame<T> {
    /// Semantic decoded message.
    pub message: T,
    /// Exact bytes consumed, allowing a caller buffer to retain later frames.
    pub consumed: usize,
}

/// Incremental decode result without partial-frame allocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeStatus<T> {
    /// One complete frame was decoded.
    Complete(DecodedFrame<T>),
    /// More bytes are required to complete the header or declared frame.
    NeedMore {
        /// Minimum total caller-buffer bytes required for the current frame.
        minimum_total: usize,
    },
}

/// Compact binary codec constrained by one negotiated full-frame maximum.
///
/// This value owns no I/O or connection state. Capability permissions, unique
/// request IDs, stream ordering, credit validation, and deadlines are enforced
/// by callers, not by successful encoding or decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireCodec {
    max_frame_bytes: u32,
}

impl WireCodec {
    /// Constructs a codec that can contain the fixed protocol header.
    ///
    /// This function performs no allocation or IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed limit error when the maximum is smaller than the header.
    pub const fn new(max_frame_bytes: u32) -> Result<Self, WireError> {
        if max_frame_bytes < HEADER_BYTES as u32 {
            return Err(WireError::MaximumBelowHeader {
                maximum: max_frame_bytes,
                header: HEADER_BYTES,
            });
        }
        Ok(Self { max_frame_bytes })
    }

    /// Returns negotiated full encoded-frame maximum.
    pub const fn max_frame_bytes(self) -> u32 {
        self.max_frame_bytes
    }

    /// Encodes one client message with exact binary payload bytes.
    ///
    /// Prospective body length is checked before each append. The limit bounds
    /// encoded length, not allocator capacity or the combined memory occupied
    /// by the temporary body and returned frame. This function performs
    /// allocation and no IO.
    ///
    /// # Errors
    ///
    /// Returns typed size, identifier-length, or required-payload failure.
    pub fn encode_client(&self, message: &ClientMessage) -> Result<Vec<u8>, WireError> {
        let mut body = BodyWriter::new(self.max_body_bytes());
        let kind = match message {
            ClientMessage::Request {
                request_id,
                trace_context,
                payload,
            } => {
                body.identifier(request_id.as_str())?;
                body.trace_context(trace_context)?;
                body.required_payload(payload)?;
                WireMessageKind::Request
            }
            ClientMessage::CancelRequest { request_id } => {
                body.identifier(request_id.as_str())?;
                WireMessageKind::CancelRequest
            }
            ClientMessage::OpenStream {
                request_id,
                stream_id,
                trace_context,
                payload,
            } => {
                body.identifier(request_id.as_str())?;
                body.identifier(stream_id.as_str())?;
                body.trace_context(trace_context)?;
                body.required_payload(payload)?;
                WireMessageKind::OpenStream
            }
            ClientMessage::ResumeStream {
                request_id,
                stream_id,
                server_epoch,
                after,
                trace_context,
                payload,
            } => {
                body.identifier(request_id.as_str())?;
                body.identifier(stream_id.as_str())?;
                body.identifier(server_epoch.as_str())?;
                body.u64(after.value())?;
                body.trace_context(trace_context)?;
                body.required_payload(payload)?;
                WireMessageKind::ResumeStream
            }
            ClientMessage::CloseStream { stream_id } => {
                body.identifier(stream_id.as_str())?;
                WireMessageKind::CloseStream
            }
            ClientMessage::WindowUpdate {
                stream_id,
                additional_bytes,
            } => {
                body.identifier(stream_id.as_str())?;
                body.u64(*additional_bytes)?;
                WireMessageKind::WindowUpdate
            }
            ClientMessage::Ping => WireMessageKind::Ping,
            ClientMessage::Close => WireMessageKind::ClientClose,
        };
        self.finish(kind, body)
    }

    /// Encodes one server message, retaining stream payload as binary bytes.
    ///
    /// Prospective body length is checked before each append. Terminal output
    /// is not base64 encoded or wrapped in JSON. Data credit metadata is ignored
    /// on encode and recomputed from full frame length on decode. This function
    /// performs allocation and no IO.
    ///
    /// # Errors
    ///
    /// Returns typed size, identifier-length, payload, or gap-range failure.
    pub fn encode_server(&self, message: &ServerMessage) -> Result<Vec<u8>, WireError> {
        let mut body = BodyWriter::new(self.max_body_bytes());
        let kind = match message {
            ServerMessage::Response {
                request_id,
                payload,
            } => {
                body.identifier(request_id.as_str())?;
                body.bytes(payload)?;
                WireMessageKind::Response
            }
            ServerMessage::StreamItem { stream_id, item } => match item {
                InboundStreamItem::Data {
                    sequence, bytes, ..
                } => {
                    body.identifier(stream_id.as_str())?;
                    body.u64(sequence.value())?;
                    body.required_payload(bytes)?;
                    WireMessageKind::StreamData
                }
                InboundStreamItem::Gap(gap) => {
                    if gap.stream_id != *stream_id || gap.first_missing > gap.last_missing {
                        return Err(WireError::InvalidGapRange);
                    }
                    body.identifier(stream_id.as_str())?;
                    body.u64(gap.first_missing.value())?;
                    body.u64(gap.last_missing.value())?;
                    body.u8(gap_reason_code(gap.reason))?;
                    body.u8(recovery_action_code(gap.recovery))?;
                    WireMessageKind::StreamGap
                }
            },
            ServerMessage::Pong => WireMessageKind::Pong,
            ServerMessage::Close => WireMessageKind::ServerClose,
        };
        self.finish(kind, body)
    }

    /// Decodes at most one client frame from a possibly partial caller buffer.
    ///
    /// Declared size is validated before payload allocation. Later complete
    /// frames remain unconsumed. This function performs bounded allocation, no
    /// IO, and does not panic.
    /// `NeedMore` reports a total buffer requirement; the caller must retain
    /// the prefix and decide how EOF or a transport deadline ends the wait.
    ///
    /// # Errors
    ///
    /// Returns typed header, size, identifier, payload, or layout failure.
    pub fn decode_client(&self, input: &[u8]) -> Result<DecodeStatus<ClientMessage>, WireError> {
        let Some(frame) = self.frame(input, WireDirection::ClientToServer)? else {
            return Ok(DecodeStatus::NeedMore {
                minimum_total: required_frame_bytes(input, self.max_frame_bytes)?,
            });
        };
        let mut body = BodyReader::new(frame.body);
        let message = match frame.kind {
            WireMessageKind::Request => ClientMessage::Request {
                request_id: body.request_id("request_id")?,
                trace_context: body.trace_context()?,
                payload: body.required_payload()?,
            },
            WireMessageKind::CancelRequest => ClientMessage::CancelRequest {
                request_id: body.request_id("request_id")?,
            },
            WireMessageKind::OpenStream => ClientMessage::OpenStream {
                request_id: body.request_id("request_id")?,
                stream_id: body.stream_id("stream_id")?,
                trace_context: body.trace_context()?,
                payload: body.required_payload()?,
            },
            WireMessageKind::ResumeStream => ClientMessage::ResumeStream {
                request_id: body.request_id("request_id")?,
                stream_id: body.stream_id("stream_id")?,
                server_epoch: body.server_epoch("server_epoch")?,
                after: StreamSequence::new(body.u64("after")?),
                trace_context: body.trace_context()?,
                payload: body.required_payload()?,
            },
            WireMessageKind::CloseStream => ClientMessage::CloseStream {
                stream_id: body.stream_id("stream_id")?,
            },
            WireMessageKind::WindowUpdate => ClientMessage::WindowUpdate {
                stream_id: body.stream_id("stream_id")?,
                additional_bytes: body.u64("additional_bytes")?,
            },
            WireMessageKind::Ping => ClientMessage::Ping,
            WireMessageKind::ClientClose => ClientMessage::Close,
            WireMessageKind::Response
            | WireMessageKind::StreamData
            | WireMessageKind::StreamGap
            | WireMessageKind::Pong
            | WireMessageKind::ServerClose => {
                return Err(WireError::InvalidMessageKind {
                    code: frame.kind.code(),
                    direction: WireDirection::ClientToServer,
                });
            }
        };
        body.finish()?;
        Ok(DecodeStatus::Complete(DecodedFrame {
            message,
            consumed: frame.consumed,
        }))
    }

    /// Decodes at most one server frame from a possibly partial caller buffer.
    ///
    /// Declared size is validated before payload allocation. Terminal data
    /// remains exact binary bytes. This function performs bounded allocation,
    /// no IO, and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed header, size, identifier, sequence, payload, gap, or
    /// layout failure.
    pub fn decode_server(&self, input: &[u8]) -> Result<DecodeStatus<ServerMessage>, WireError> {
        let Some(frame) = self.frame(input, WireDirection::ServerToClient)? else {
            return Ok(DecodeStatus::NeedMore {
                minimum_total: required_frame_bytes(input, self.max_frame_bytes)?,
            });
        };
        let mut body = BodyReader::new(frame.body);
        let message = match frame.kind {
            WireMessageKind::Response => ServerMessage::Response {
                request_id: body.request_id("request_id")?,
                payload: body.payload(),
            },
            WireMessageKind::StreamData => {
                let stream_id = body.stream_id("stream_id")?;
                let sequence = StreamSequence::new(body.u64("stream_sequence")?);
                let bytes = body.required_payload()?;
                ServerMessage::StreamItem {
                    stream_id,
                    item: InboundStreamItem::Data {
                        sequence,
                        bytes,
                        flow_controlled_bytes: u64::try_from(frame.consumed).unwrap_or(u64::MAX),
                    },
                }
            }
            WireMessageKind::StreamGap => {
                let stream_id = body.stream_id("stream_id")?;
                let first_missing = StreamSequence::new(body.u64("first_missing")?);
                let last_missing = StreamSequence::new(body.u64("last_missing")?);
                if first_missing > last_missing {
                    return Err(WireError::InvalidGapRange);
                }
                let reason = decode_gap_reason(body.u8("gap_reason")?)?;
                let recovery = decode_recovery_action(body.u8("recovery_action")?)?;
                ServerMessage::StreamItem {
                    stream_id: stream_id.clone(),
                    item: InboundStreamItem::Gap(StreamGap {
                        stream_id,
                        first_missing,
                        last_missing,
                        reason,
                        recovery,
                    }),
                }
            }
            WireMessageKind::Pong => ServerMessage::Pong,
            WireMessageKind::ServerClose => ServerMessage::Close,
            WireMessageKind::Request
            | WireMessageKind::CancelRequest
            | WireMessageKind::OpenStream
            | WireMessageKind::ResumeStream
            | WireMessageKind::CloseStream
            | WireMessageKind::WindowUpdate
            | WireMessageKind::Ping
            | WireMessageKind::ClientClose => {
                return Err(WireError::InvalidMessageKind {
                    code: frame.kind.code(),
                    direction: WireDirection::ServerToClient,
                });
            }
        };
        body.finish()?;
        Ok(DecodeStatus::Complete(DecodedFrame {
            message,
            consumed: frame.consumed,
        }))
    }

    fn max_body_bytes(self) -> usize {
        self.max_frame_bytes as usize - HEADER_BYTES
    }

    fn finish(&self, kind: WireMessageKind, body: BodyWriter) -> Result<Vec<u8>, WireError> {
        let body = body.into_inner();
        let body_len = u32::try_from(body.len()).map_err(|_| WireError::SizeOverflow)?;
        let total = HEADER_BYTES
            .checked_add(body.len())
            .ok_or(WireError::SizeOverflow)?;
        if total > self.max_frame_bytes as usize {
            return Err(WireError::FrameTooLarge {
                actual: u64::try_from(total).unwrap_or(u64::MAX),
                maximum: self.max_frame_bytes,
            });
        }
        let mut frame = Vec::with_capacity(total);
        frame.extend_from_slice(&MAGIC);
        frame.push(WIRE_VERSION);
        frame.push(kind.code());
        frame.extend_from_slice(&0_u16.to_be_bytes());
        frame.extend_from_slice(&body_len.to_be_bytes());
        frame.extend_from_slice(RELEASE_BYTES);
        frame.extend_from_slice(&body);
        Ok(frame)
    }

    fn frame<'a>(
        &self,
        input: &'a [u8],
        direction: WireDirection,
    ) -> Result<Option<FrameView<'a>>, WireError> {
        if input.len() < HEADER_BYTES {
            return Ok(None);
        }
        if input[..4] != MAGIC {
            return Err(WireError::InvalidMagic);
        }
        if input[4] != WIRE_VERSION || &input[12..HEADER_BYTES] != RELEASE_BYTES {
            return Err(WireError::UnsupportedVersion {
                received: input[4],
                supported: WIRE_VERSION,
            });
        }
        let kind = WireMessageKind::from_code(input[5], direction).ok_or(
            WireError::InvalidMessageKind {
                code: input[5],
                direction,
            },
        )?;
        let flags = u16::from_be_bytes([input[6], input[7]]);
        if flags != 0 {
            return Err(WireError::UnsupportedFlags { flags });
        }
        let body_len = u32::from_be_bytes([input[8], input[9], input[10], input[11]]) as usize;
        let consumed = HEADER_BYTES
            .checked_add(body_len)
            .ok_or(WireError::SizeOverflow)?;
        if consumed > self.max_frame_bytes as usize {
            return Err(WireError::FrameTooLarge {
                actual: u64::try_from(consumed).unwrap_or(u64::MAX),
                maximum: self.max_frame_bytes,
            });
        }
        if input.len() < consumed {
            return Ok(None);
        }
        Ok(Some(FrameView {
            kind,
            body: &input[HEADER_BYTES..consumed],
            consumed,
        }))
    }
}

struct FrameView<'a> {
    kind: WireMessageKind,
    body: &'a [u8],
    consumed: usize,
}

struct BodyWriter {
    bytes: Vec<u8>,
    maximum: usize,
}

impl BodyWriter {
    fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
        }
    }

    fn identifier(&mut self, value: &str) -> Result<(), WireError> {
        let length = u16::try_from(value.len()).map_err(|_| WireError::SizeOverflow)?;
        self.ensure(2_usize.saturating_add(value.len()))?;
        self.bytes.extend_from_slice(&length.to_be_bytes());
        self.bytes.extend_from_slice(value.as_bytes());
        Ok(())
    }

    fn required_payload(&mut self, value: &[u8]) -> Result<(), WireError> {
        if value.is_empty() {
            return Err(WireError::EmptyPayload);
        }
        self.bytes(value)
    }

    fn trace_context(&mut self, context: &TraceContext) -> Result<(), WireError> {
        self.identifier(context.trace_id.as_str())?;
        self.identifier(context.span_id.as_str())?;
        self.optional_identifier(context.parent_span_id.as_ref().map(SpanId::as_str))?;
        self.optional_identifier(context.correlation_id.as_ref().map(CorrelationId::as_str))
    }

    fn optional_identifier(&mut self, value: Option<&str>) -> Result<(), WireError> {
        match value {
            Some(value) => {
                self.u8(1)?;
                self.identifier(value)
            }
            None => self.u8(0),
        }
    }

    fn bytes(&mut self, value: &[u8]) -> Result<(), WireError> {
        self.ensure(value.len())?;
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    fn u64(&mut self, value: u64) -> Result<(), WireError> {
        self.bytes(&value.to_be_bytes())
    }

    fn u8(&mut self, value: u8) -> Result<(), WireError> {
        self.ensure(1)?;
        self.bytes.push(value);
        Ok(())
    }

    fn ensure(&self, additional: usize) -> Result<(), WireError> {
        let prospective = self
            .bytes
            .len()
            .checked_add(additional)
            .ok_or(WireError::SizeOverflow)?;
        if prospective > self.maximum {
            return Err(WireError::FrameTooLarge {
                actual: u64::try_from(HEADER_BYTES)
                    .unwrap_or(u64::MAX)
                    .saturating_add(u64::try_from(prospective).unwrap_or(u64::MAX)),
                maximum: u32::try_from(HEADER_BYTES.saturating_add(self.maximum))
                    .unwrap_or(u32::MAX),
            });
        }
        Ok(())
    }

    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

struct BodyReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> BodyReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn request_id(&mut self, field: &'static str) -> Result<RequestId, WireError> {
        let value = self.identifier(field)?;
        RequestId::new(value).map_err(|_| WireError::InvalidIdentifier { field })
    }

    fn stream_id(&mut self, field: &'static str) -> Result<StreamId, WireError> {
        let value = self.identifier(field)?;
        StreamId::new(value).map_err(|_| WireError::InvalidIdentifier { field })
    }

    fn server_epoch(&mut self, field: &'static str) -> Result<ServerEpoch, WireError> {
        let value = self.identifier(field)?;
        ServerEpoch::new(value).map_err(|_| WireError::InvalidIdentifier { field })
    }

    fn identifier(&mut self, field: &'static str) -> Result<&'a str, WireError> {
        let length = usize::from(self.u16(field)?);
        let bytes = self.take(length, field)?;
        std::str::from_utf8(bytes).map_err(|_| WireError::InvalidIdentifier { field })
    }

    fn u16(&mut self, field: &'static str) -> Result<u16, WireError> {
        let bytes = self.take(2, field)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, WireError> {
        let bytes = self.take(8, field)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn u8(&mut self, field: &'static str) -> Result<u8, WireError> {
        Ok(self.take(1, field)?[0])
    }

    fn required_payload(&mut self) -> Result<Arc<[u8]>, WireError> {
        if self.position == self.bytes.len() {
            return Err(WireError::EmptyPayload);
        }
        Ok(self.payload())
    }

    fn trace_context(&mut self) -> Result<TraceContext, WireError> {
        Ok(TraceContext {
            trace_id: TraceId::new(self.identifier("trace_id")?)
                .map_err(|_| WireError::InvalidIdentifier { field: "trace_id" })?,
            span_id: SpanId::new(self.identifier("span_id")?)
                .map_err(|_| WireError::InvalidIdentifier { field: "span_id" })?,
            parent_span_id: self.optional_span_id("parent_span_id")?,
            correlation_id: self.optional_correlation_id("correlation_id")?,
        })
    }

    fn optional_span_id(&mut self, field: &'static str) -> Result<Option<SpanId>, WireError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => SpanId::new(self.identifier(field)?)
                .map(Some)
                .map_err(|_| WireError::InvalidIdentifier { field }),
            code => Err(WireError::InvalidFieldCode { field, code }),
        }
    }

    fn optional_correlation_id(
        &mut self,
        field: &'static str,
    ) -> Result<Option<CorrelationId>, WireError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => CorrelationId::new(self.identifier(field)?)
                .map(Some)
                .map_err(|_| WireError::InvalidIdentifier { field }),
            code => Err(WireError::InvalidFieldCode { field, code }),
        }
    }

    fn payload(&mut self) -> Arc<[u8]> {
        let payload = Arc::from(&self.bytes[self.position..]);
        self.position = self.bytes.len();
        payload
    }

    fn take(&mut self, length: usize, field: &'static str) -> Result<&'a [u8], WireError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(WireError::SizeOverflow)?;
        if end > self.bytes.len() {
            // This reader only sees a complete declared body. More transport bytes
            // belong to a later frame and cannot repair a short field here.
            // See [decode stages](../docs/reference/wire-format.md#incremental-decoding).
            return Err(WireError::TruncatedField { field });
        }
        let value = &self.bytes[self.position..end];
        self.position = end;
        Ok(value)
    }

    fn finish(&self) -> Result<(), WireError> {
        let remaining = self.bytes.len().saturating_sub(self.position);
        if remaining == 0 {
            Ok(())
        } else {
            Err(WireError::TrailingBytes { remaining })
        }
    }
}

fn required_frame_bytes(input: &[u8], maximum: u32) -> Result<usize, WireError> {
    if input.len() < HEADER_BYTES {
        return Ok(HEADER_BYTES);
    }
    let body_len = u32::from_be_bytes([input[8], input[9], input[10], input[11]]) as usize;
    let total = HEADER_BYTES
        .checked_add(body_len)
        .ok_or(WireError::SizeOverflow)?;
    if total > maximum as usize {
        return Err(WireError::FrameTooLarge {
            actual: u64::try_from(total).unwrap_or(u64::MAX),
            maximum,
        });
    }
    Ok(total)
}

const fn gap_reason_code(reason: StreamGapReason) -> u8 {
    match reason {
        StreamGapReason::LiveQueueOverflow => 1,
        StreamGapReason::ReplayEvicted => 2,
        StreamGapReason::ReplayUnavailable => 3,
        StreamGapReason::ServerEpochChanged => 4,
        StreamGapReason::SourceRetentionEvicted => 5,
    }
}

fn decode_gap_reason(code: u8) -> Result<StreamGapReason, WireError> {
    match code {
        1 => Ok(StreamGapReason::LiveQueueOverflow),
        2 => Ok(StreamGapReason::ReplayEvicted),
        3 => Ok(StreamGapReason::ReplayUnavailable),
        4 => Ok(StreamGapReason::ServerEpochChanged),
        5 => Ok(StreamGapReason::SourceRetentionEvicted),
        _ => Err(WireError::InvalidGapCode {
            field: "reason",
            code,
        }),
    }
}

const fn recovery_action_code(action: StreamRecoveryAction) -> u8 {
    match action {
        StreamRecoveryAction::RequestSnapshot => 1,
        StreamRecoveryAction::RestartQuery => 2,
        StreamRecoveryAction::RestartFromAvailableOffset => 3,
    }
}

fn decode_recovery_action(code: u8) -> Result<StreamRecoveryAction, WireError> {
    match code {
        1 => Ok(StreamRecoveryAction::RequestSnapshot),
        2 => Ok(StreamRecoveryAction::RestartQuery),
        3 => Ok(StreamRecoveryAction::RestartFromAvailableOffset),
        _ => Err(WireError::InvalidGapCode {
            field: "recovery_action",
            code,
        }),
    }
}
