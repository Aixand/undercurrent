// SPDX-License-Identifier: Apache-2.0

//! Typed binary framing failures with payload-safe diagnostics.

use std::error::Error;
use std::fmt::{Display, Formatter};

use crate::WireDirection;

/// Stable wire codec failure category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WireErrorCategory {
    /// Frame input violates the binary protocol contract.
    InvalidFrame,
    /// Declared or encoded frame exceeds the negotiated byte maximum.
    FrameTooLarge,
    /// A monotonic size calculation exhausted its representation.
    InvariantViolation,
}

/// Payload-redacted binary frame failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireError {
    /// Configured frame maximum cannot contain the fixed header.
    MaximumBelowHeader {
        /// Configured maximum bytes.
        maximum: u32,
        /// Fixed protocol header bytes.
        header: usize,
    },
    /// Fixed magic bytes did not identify the Undercurrent protocol.
    InvalidMagic,
    /// The frame release marker is not supported.
    UnsupportedVersion {
        /// Received release marker length.
        received: u8,
        /// Supported release marker length.
        supported: u8,
    },
    /// Message discriminator is unknown or invalid for the read direction.
    InvalidMessageKind {
        /// Received discriminator.
        code: u8,
        /// Expected connection direction.
        direction: WireDirection,
    },
    /// Reserved header flags were non-zero.
    UnsupportedFlags {
        /// Received flag bits.
        flags: u16,
    },
    /// Declared or encoded frame bytes exceed the negotiated maximum.
    FrameTooLarge {
        /// Observed or declared full frame bytes.
        actual: u64,
        /// Inclusive negotiated maximum.
        maximum: u32,
    },
    /// Required field ended before its complete fixed or length-prefixed value.
    TruncatedField {
        /// Stable field name.
        field: &'static str,
    },
    /// Identifier bytes were not valid UTF-8 or violated wire validation.
    InvalidIdentifier {
        /// Stable identifier field.
        field: &'static str,
    },
    /// A compact field discriminator was outside its declared registry.
    InvalidFieldCode {
        /// Stable invalid field.
        field: &'static str,
        /// Received code.
        code: u8,
    },
    /// Required binary payload was empty.
    EmptyPayload,
    /// Fixed-layout frame retained unexpected bytes.
    TrailingBytes {
        /// Unexpected remaining byte count.
        remaining: usize,
    },
    /// The gap stream ID differs from the enclosing stream ID, or its inclusive
    /// sequence interval is reversed.
    InvalidGapRange,
    /// Gap reason or recovery action code is unknown.
    InvalidGapCode {
        /// Stable invalid field.
        field: &'static str,
        /// Received code.
        code: u8,
    },
    /// Size calculation overflowed its representation.
    SizeOverflow,
}

impl WireError {
    /// Returns stable presentation and telemetry category.
    pub const fn category(&self) -> WireErrorCategory {
        match self {
            Self::FrameTooLarge { .. } => WireErrorCategory::FrameTooLarge,
            Self::SizeOverflow => WireErrorCategory::InvariantViolation,
            Self::MaximumBelowHeader { .. }
            | Self::InvalidMagic
            | Self::UnsupportedVersion { .. }
            | Self::InvalidMessageKind { .. }
            | Self::UnsupportedFlags { .. }
            | Self::TruncatedField { .. }
            | Self::InvalidIdentifier { .. }
            | Self::InvalidFieldCode { .. }
            | Self::EmptyPayload
            | Self::TrailingBytes { .. }
            | Self::InvalidGapRange
            | Self::InvalidGapCode { .. } => WireErrorCategory::InvalidFrame,
        }
    }
}

impl Display for WireError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MaximumBelowHeader { maximum, header } => write!(
                formatter,
                "frame maximum {maximum} is smaller than fixed header {header}"
            ),
            Self::InvalidMagic => formatter.write_str("invalid Undercurrent frame magic"),
            Self::UnsupportedVersion { .. } => write!(
                formatter,
                "wire release is unsupported; expected {}",
                undercurrent_foundation::RELEASE_VERSION
            ),
            Self::InvalidMessageKind { code, direction } => {
                write!(
                    formatter,
                    "message kind {code} is invalid for {direction:?}"
                )
            }
            Self::UnsupportedFlags { flags } => {
                write!(formatter, "frame flags {flags:#06x} are unsupported")
            }
            Self::FrameTooLarge { actual, maximum } => {
                write!(formatter, "frame bytes {actual} exceed maximum {maximum}")
            }
            Self::TruncatedField { field } => write!(formatter, "frame field {field} is truncated"),
            Self::InvalidIdentifier { field } => {
                write!(formatter, "frame identifier {field} is invalid")
            }
            Self::InvalidFieldCode { field, code } => {
                write!(formatter, "frame field {field} code {code} is invalid")
            }
            Self::EmptyPayload => formatter.write_str("required frame payload is empty"),
            Self::TrailingBytes { remaining } => {
                write!(formatter, "fixed frame contains {remaining} trailing bytes")
            }
            Self::InvalidGapRange => formatter.write_str("stream gap range is reversed"),
            Self::InvalidGapCode { field, code } => {
                write!(formatter, "stream gap {field} code {code} is invalid")
            }
            Self::SizeOverflow => formatter.write_str("frame size calculation overflowed"),
        }
    }
}

impl Error for WireError {}
