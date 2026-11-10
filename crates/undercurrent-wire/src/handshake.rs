// SPDX-License-Identifier: Apache-2.0

//! Bounded framing for physical-connection negotiation.
//!
//! Client/daemon UCHS frames have a 12-byte header and a JSON body. This codec
//! is unrelated to the runtime's private session-host protocol with the same
//! magic. Decoding validates value construction, not admission or compatibility
//! with a previous offer. The caller supplies setup deadlines and the frame
//! bound used before a negotiated UCMP bound exists.

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

use crate::{ConnectionAgreement, ConnectionHello};
use serde::{Deserialize, Serialize};

const MAGIC: [u8; 4] = *b"UCHS";
const FORMAT_VERSION: u8 = undercurrent_foundation::RELEASE_VERSION.len() as u8;
const RELEASE_BYTES: &[u8] = undercurrent_foundation::RELEASE_VERSION.as_bytes();
const HEADER_BYTES: usize = 12 + RELEASE_BYTES.len();

/// Stable handshake message discriminator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HandshakeMessageKind {
    /// Client connection offer.
    Hello,
    /// Successful server negotiation result.
    Accepted,
    /// Typed server negotiation rejection.
    Rejected,
}

impl HandshakeMessageKind {
    /// Complete discriminator registry in encoded order.
    pub const ALL: [Self; 3] = [Self::Hello, Self::Accepted, Self::Rejected];

    /// Returns the stable one-byte discriminator.
    pub const fn code(self) -> u8 {
        match self {
            Self::Hello => 1,
            Self::Accepted => 2,
            Self::Rejected => 3,
        }
    }

    /// Returns the stable snake-case diagnostic name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hello => "hello",
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
        }
    }

    const fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Hello),
            2 => Some(Self::Accepted),
            3 => Some(Self::Rejected),
            _ => None,
        }
    }
}

/// Stable reason a server rejected connection setup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandshakeRejectionCategory {
    /// The handshake frame or payload was invalid.
    InvalidHandshake,
    /// Client or server does not select the sole supported protocol version.
    IncompatibleVersion,
    /// A capability required by the client is unavailable.
    MissingCapability,
    /// The server cannot admit another physical connection.
    CapacityExceeded,
    /// The logical client or physical connection identity conflicts.
    Conflict,
    /// Server setup failed without a client-actionable detail.
    Unavailable,
}

impl HandshakeRejectionCategory {
    /// Complete stable rejection-category registry.
    pub const ALL: [Self; 6] = [
        Self::InvalidHandshake,
        Self::IncompatibleVersion,
        Self::MissingCapability,
        Self::CapacityExceeded,
        Self::Conflict,
        Self::Unavailable,
    ];

    /// Returns the stable snake-case category name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidHandshake => "invalid_handshake",
            Self::IncompatibleVersion => "incompatible_version",
            Self::MissingCapability => "missing_capability",
            Self::CapacityExceeded => "capacity_exceeded",
            Self::Conflict => "conflict",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Payload-safe server rejection returned before multiplex activation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandshakeRejection {
    category: HandshakeRejectionCategory,
    retryable: bool,
}

impl HandshakeRejection {
    /// Constructs a rejection containing only stable, payload-safe fields.
    ///
    /// This function performs no IO, cannot fail, and does not panic.
    pub const fn new(category: HandshakeRejectionCategory, retryable: bool) -> Self {
        Self {
            category,
            retryable,
        }
    }

    /// Returns the stable rejection category.
    pub const fn category(&self) -> HandshakeRejectionCategory {
        self.category
    }

    /// Returns whether reconnect policy may retry this rejection.
    pub const fn retryable(&self) -> bool {
        self.retryable
    }
}

/// Complete server response to one client handshake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandshakeResponse {
    /// Negotiation succeeded with these immutable connection terms.
    Accepted(ConnectionAgreement),
    /// Setup was rejected; admission and resource cleanup belong to the caller.
    Rejected(HandshakeRejection),
}

/// One complete decoded handshake value and its consumed frame bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedHandshake<T> {
    /// Validated semantic handshake value.
    pub message: T,
    /// Exact bytes consumed from the caller buffer.
    pub consumed: usize,
}

/// Incremental handshake decode result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandshakeDecodeStatus<T> {
    /// One complete frame was decoded.
    Complete(DecodedHandshake<T>),
    /// More bytes are required for the current bounded frame.
    NeedMore {
        /// Minimum total caller-buffer bytes required.
        minimum_total: usize,
    },
}

/// Stable category for handshake framing and payload failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HandshakeCodecErrorCategory {
    /// Codec configuration is not usable.
    Configuration,
    /// Peer input violates the handshake contract.
    InvalidFrame,
    /// A frame exceeds its configured byte maximum.
    FrameTooLarge,
    /// Local serialization or size state failed.
    InvariantViolation,
}

/// Bounded handshake codec failure retaining serializer sources internally.
pub enum HandshakeCodecError {
    /// Configured maximum cannot contain the fixed header.
    MaximumBelowHeader {
        /// Configured full-frame maximum.
        maximum: u32,
    },
    /// Fixed magic bytes do not identify a handshake frame.
    InvalidMagic,
    /// The handshake release marker is unsupported.
    UnsupportedFormat {
        /// Received release marker length.
        received: u8,
    },
    /// Message discriminator is unknown or invalid for the expected side.
    InvalidMessageKind {
        /// Received discriminator.
        code: u8,
    },
    /// Reserved flags were non-zero.
    UnsupportedFlags {
        /// Received flag bits.
        flags: u16,
    },
    /// Declared or encoded frame exceeds the configured bound.
    FrameTooLarge {
        /// Observed or declared full frame bytes.
        actual: u64,
        /// Inclusive configured maximum.
        maximum: u32,
    },
    /// JSON did not encode the semantic value required by the frame kind.
    InvalidPayload {
        /// Stable semantic payload name.
        payload: &'static str,
        /// Concrete parser or serializer source, omitted from presentation.
        source: serde_json::Error,
    },
    /// A frame size calculation overflowed its representation.
    SizeOverflow,
}

impl HandshakeCodecError {
    /// Returns the stable presentation and telemetry category.
    pub const fn category(&self) -> HandshakeCodecErrorCategory {
        match self {
            Self::MaximumBelowHeader { .. } => HandshakeCodecErrorCategory::Configuration,
            Self::InvalidMagic
            | Self::UnsupportedFormat { .. }
            | Self::InvalidMessageKind { .. }
            | Self::UnsupportedFlags { .. }
            | Self::InvalidPayload { .. } => HandshakeCodecErrorCategory::InvalidFrame,
            Self::FrameTooLarge { .. } => HandshakeCodecErrorCategory::FrameTooLarge,
            Self::SizeOverflow => HandshakeCodecErrorCategory::InvariantViolation,
        }
    }
}

impl Debug for HandshakeCodecError {
    // Parser errors can include peer-supplied values, so presentation omits the
    // source. Error::source still exposes it; source-chain logging is not redacted.
    // See [diagnostics](../docs/reference/wire-format.md#error-reference-and-safe-diagnostics).
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MaximumBelowHeader { maximum } => formatter
                .debug_struct("MaximumBelowHeader")
                .field("maximum", maximum)
                .finish(),
            Self::InvalidMagic => formatter.write_str("InvalidMagic"),
            Self::UnsupportedFormat { received } => formatter
                .debug_struct("UnsupportedFormat")
                .field("received", received)
                .finish(),
            Self::InvalidMessageKind { code } => formatter
                .debug_struct("InvalidMessageKind")
                .field("code", code)
                .finish(),
            Self::UnsupportedFlags { flags } => formatter
                .debug_struct("UnsupportedFlags")
                .field("flags", flags)
                .finish(),
            Self::FrameTooLarge { actual, maximum } => formatter
                .debug_struct("FrameTooLarge")
                .field("actual", actual)
                .field("maximum", maximum)
                .finish(),
            Self::InvalidPayload { payload, .. } => formatter
                .debug_struct("InvalidPayload")
                .field("payload", payload)
                .finish_non_exhaustive(),
            Self::SizeOverflow => formatter.write_str("SizeOverflow"),
        }
    }
}

impl Display for HandshakeCodecError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MaximumBelowHeader { maximum } => write!(
                formatter,
                "handshake frame maximum {maximum} is smaller than fixed header {HEADER_BYTES}"
            ),
            Self::InvalidMagic => formatter.write_str("invalid Undercurrent handshake magic"),
            Self::UnsupportedFormat { .. } => write!(
                formatter,
                "handshake release is unsupported; expected {}",
                undercurrent_foundation::RELEASE_VERSION
            ),
            Self::InvalidMessageKind { code } => {
                write!(formatter, "handshake message kind {code} is invalid")
            }
            Self::UnsupportedFlags { flags } => {
                write!(formatter, "handshake flags {flags:#06x} are unsupported")
            }
            Self::FrameTooLarge { actual, maximum } => write!(
                formatter,
                "handshake frame bytes {actual} exceed maximum {maximum}"
            ),
            Self::InvalidPayload { payload, .. } => {
                write!(formatter, "handshake {payload} payload is invalid")
            }
            Self::SizeOverflow => formatter.write_str("handshake frame size overflowed"),
        }
    }
}

impl Error for HandshakeCodecError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidPayload { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Codec for pre-multiplex negotiation frames with bounded encoded length.
///
/// The codec keeps no partial input and performs no transport IO. JSON semantic
/// allocations and encoder buffer capacity are not an exact memory ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandshakeCodec {
    max_frame_bytes: u32,
}

impl HandshakeCodec {
    /// Constructs a codec with an explicit complete-frame byte bound.
    ///
    /// This function performs no allocation or IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the maximum cannot contain a header.
    pub const fn new(max_frame_bytes: u32) -> Result<Self, HandshakeCodecError> {
        if max_frame_bytes < HEADER_BYTES as u32 {
            return Err(HandshakeCodecError::MaximumBelowHeader {
                maximum: max_frame_bytes,
            });
        }
        Ok(Self { max_frame_bytes })
    }

    /// Returns the configured full-frame byte maximum.
    pub const fn max_frame_bytes(self) -> u32 {
        self.max_frame_bytes
    }

    /// Encodes one validated client offer.
    ///
    /// Serialization is bounded by the codec limit and performs no IO.
    ///
    /// # Errors
    ///
    /// Returns typed serialization, capacity, or size failures.
    pub fn encode_hello(&self, hello: &ConnectionHello) -> Result<Vec<u8>, HandshakeCodecError> {
        self.encode_json(HandshakeMessageKind::Hello, "hello", hello)
    }

    /// Encodes one successful or rejected server result.
    ///
    /// Serialization is bounded by the codec limit and performs no IO.
    ///
    /// # Errors
    ///
    /// Returns typed serialization, capacity, or size failures.
    pub fn encode_response(
        &self,
        response: &HandshakeResponse,
    ) -> Result<Vec<u8>, HandshakeCodecError> {
        match response {
            HandshakeResponse::Accepted(agreement) => {
                self.encode_json(HandshakeMessageKind::Accepted, "agreement", agreement)
            }
            HandshakeResponse::Rejected(rejection) => {
                self.encode_json(HandshakeMessageKind::Rejected, "rejection", rejection)
            }
        }
    }

    /// Incrementally decodes one client offer and rejects server message kinds.
    ///
    /// Declared size is checked before parsing or allocating payload-owned
    /// semantic values. Later frames remain unconsumed. This function performs
    /// no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed framing, direction, payload, or size failures.
    pub fn decode_hello(
        &self,
        input: &[u8],
    ) -> Result<HandshakeDecodeStatus<ConnectionHello>, HandshakeCodecError> {
        let Some(frame) = self.frame(input)? else {
            return Ok(HandshakeDecodeStatus::NeedMore {
                minimum_total: self.minimum_total(input)?,
            });
        };
        if frame.kind != HandshakeMessageKind::Hello {
            return Err(HandshakeCodecError::InvalidMessageKind {
                code: frame.kind.code(),
            });
        }
        let message = serde_json::from_slice(frame.body).map_err(|source| {
            HandshakeCodecError::InvalidPayload {
                payload: "hello",
                source,
            }
        })?;
        Ok(HandshakeDecodeStatus::Complete(DecodedHandshake {
            message,
            consumed: frame.consumed,
        }))
    }

    /// Incrementally decodes one server negotiation result.
    ///
    /// Declared size is checked before payload parsing. Later frames remain
    /// unconsumed. This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed framing, direction, payload, or size failures.
    pub fn decode_response(
        &self,
        input: &[u8],
    ) -> Result<HandshakeDecodeStatus<HandshakeResponse>, HandshakeCodecError> {
        let Some(frame) = self.frame(input)? else {
            return Ok(HandshakeDecodeStatus::NeedMore {
                minimum_total: self.minimum_total(input)?,
            });
        };
        let message = match frame.kind {
            HandshakeMessageKind::Accepted => {
                HandshakeResponse::Accepted(serde_json::from_slice(frame.body).map_err(
                    |source| HandshakeCodecError::InvalidPayload {
                        payload: "agreement",
                        source,
                    },
                )?)
            }
            HandshakeMessageKind::Rejected => {
                HandshakeResponse::Rejected(serde_json::from_slice(frame.body).map_err(
                    |source| HandshakeCodecError::InvalidPayload {
                        payload: "rejection",
                        source,
                    },
                )?)
            }
            HandshakeMessageKind::Hello => {
                return Err(HandshakeCodecError::InvalidMessageKind {
                    code: frame.kind.code(),
                });
            }
        };
        Ok(HandshakeDecodeStatus::Complete(DecodedHandshake {
            message,
            consumed: frame.consumed,
        }))
    }

    fn encode_json<T: Serialize>(
        &self,
        kind: HandshakeMessageKind,
        payload: &'static str,
        value: &T,
    ) -> Result<Vec<u8>, HandshakeCodecError> {
        let maximum_body = self.max_frame_bytes as usize - HEADER_BYTES;
        let mut body = BoundedJsonBody::new(maximum_body);
        if let Err(source) = serde_json::to_writer(&mut body, value) {
            // Serde wraps writer failures as serialization errors. Preserve the
            // size category using the writer's state; attempted ends at the failing
            // write and need not be the full hypothetical JSON length.
            // See [bounded JSON bodies](../docs/reference/wire-format.md#uchs-bodies).
            if body.exceeded {
                return Err(HandshakeCodecError::FrameTooLarge {
                    actual: u64::try_from(HEADER_BYTES)
                        .unwrap_or(u64::MAX)
                        .saturating_add(u64::try_from(body.attempted).unwrap_or(u64::MAX)),
                    maximum: self.max_frame_bytes,
                });
            }
            return Err(HandshakeCodecError::InvalidPayload { payload, source });
        }
        let body = body.bytes;
        let body_len = u32::try_from(body.len()).map_err(|_| HandshakeCodecError::SizeOverflow)?;
        let total = HEADER_BYTES
            .checked_add(body.len())
            .ok_or(HandshakeCodecError::SizeOverflow)?;
        let mut encoded = Vec::with_capacity(total);
        encoded.extend_from_slice(&MAGIC);
        encoded.push(FORMAT_VERSION);
        encoded.push(kind.code());
        encoded.extend_from_slice(&0_u16.to_be_bytes());
        encoded.extend_from_slice(&body_len.to_be_bytes());
        encoded.extend_from_slice(RELEASE_BYTES);
        encoded.extend_from_slice(&body);
        Ok(encoded)
    }

    fn frame<'a>(
        &self,
        input: &'a [u8],
    ) -> Result<Option<HandshakeFrame<'a>>, HandshakeCodecError> {
        if input.len() < HEADER_BYTES {
            return Ok(None);
        }
        if input[..4] != MAGIC {
            return Err(HandshakeCodecError::InvalidMagic);
        }
        if input[4] != FORMAT_VERSION || &input[12..HEADER_BYTES] != RELEASE_BYTES {
            return Err(HandshakeCodecError::UnsupportedFormat { received: input[4] });
        }
        let kind = HandshakeMessageKind::from_code(input[5])
            .ok_or(HandshakeCodecError::InvalidMessageKind { code: input[5] })?;
        let flags = u16::from_be_bytes([input[6], input[7]]);
        if flags != 0 {
            return Err(HandshakeCodecError::UnsupportedFlags { flags });
        }
        let body_len = u32::from_be_bytes([input[8], input[9], input[10], input[11]]) as usize;
        let consumed = HEADER_BYTES
            .checked_add(body_len)
            .ok_or(HandshakeCodecError::SizeOverflow)?;
        if consumed > self.max_frame_bytes as usize {
            return Err(HandshakeCodecError::FrameTooLarge {
                actual: u64::try_from(consumed).unwrap_or(u64::MAX),
                maximum: self.max_frame_bytes,
            });
        }
        if input.len() < consumed {
            return Ok(None);
        }
        Ok(Some(HandshakeFrame {
            kind,
            body: &input[HEADER_BYTES..consumed],
            consumed,
        }))
    }

    fn minimum_total(&self, input: &[u8]) -> Result<usize, HandshakeCodecError> {
        if input.len() < HEADER_BYTES {
            return Ok(HEADER_BYTES);
        }
        let body_len = u32::from_be_bytes([input[8], input[9], input[10], input[11]]) as usize;
        let total = HEADER_BYTES
            .checked_add(body_len)
            .ok_or(HandshakeCodecError::SizeOverflow)?;
        if total > self.max_frame_bytes as usize {
            return Err(HandshakeCodecError::FrameTooLarge {
                actual: u64::try_from(total).unwrap_or(u64::MAX),
                maximum: self.max_frame_bytes,
            });
        }
        Ok(total)
    }
}

struct HandshakeFrame<'a> {
    kind: HandshakeMessageKind,
    body: &'a [u8],
    consumed: usize,
}

struct BoundedJsonBody {
    bytes: Vec<u8>,
    maximum: usize,
    attempted: usize,
    exceeded: bool,
}

impl BoundedJsonBody {
    fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
            attempted: 0,
            exceeded: false,
        }
    }
}

impl std::io::Write for BoundedJsonBody {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.attempted = self.bytes.len().saturating_add(bytes.len());
        if self.attempted > self.maximum {
            self.exceeded = true;
            return Err(std::io::Error::other("handshake body capacity exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
