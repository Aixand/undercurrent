// SPDX-License-Identifier: Apache-2.0

//! Payload-redacted product codec and validation failures.

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

/// Stable stage at which a product payload was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductCodecStage {
    /// Client finite request payload.
    Request,
    /// Server finite response payload.
    Response,
    /// Client logical-stream specification.
    StreamSpec,
    /// Non-terminal logical-stream item.
    StreamItem,
    /// Binary terminal output item.
    TerminalOutput,
}

/// Bounded product payload encoding or decoding failure.
pub struct ProductCodecError {
    stage: ProductCodecStage,
    kind: ProductCodecErrorKind,
}

impl ProductCodecError {
    pub(crate) const fn new(stage: ProductCodecStage, kind: ProductCodecErrorKind) -> Self {
        Self { stage, kind }
    }

    /// Returns the stable failed codec stage.
    pub const fn stage(&self) -> ProductCodecStage {
        self.stage
    }

    /// Returns the stable payload-safe failure category.
    pub const fn category(&self) -> ProductCodecErrorCategory {
        match self.kind {
            ProductCodecErrorKind::Empty => ProductCodecErrorCategory::Malformed,
            ProductCodecErrorKind::Capacity { .. } => ProductCodecErrorCategory::Capacity,
            ProductCodecErrorKind::Malformed(_) => ProductCodecErrorCategory::Malformed,
            ProductCodecErrorKind::UnsupportedTerminalKind { .. } => {
                ProductCodecErrorCategory::UnsupportedKind
            }
            ProductCodecErrorKind::TerminalLengthMismatch { .. } => {
                ProductCodecErrorCategory::Malformed
            }
            ProductCodecErrorKind::InvalidTerminalScreen => ProductCodecErrorCategory::Malformed,
        }
    }

    /// Returns the configured byte limit for a capacity failure.
    pub const fn limit(&self) -> Option<usize> {
        match self.kind {
            ProductCodecErrorKind::Capacity { limit, .. } => Some(limit),
            _ => None,
        }
    }

    /// Returns the observed byte count for a capacity or length failure.
    pub const fn observed_bytes(&self) -> Option<usize> {
        match self.kind {
            ProductCodecErrorKind::Capacity { observed, .. }
            | ProductCodecErrorKind::TerminalLengthMismatch { observed, .. } => Some(observed),
            _ => None,
        }
    }

    /// Returns an unsupported terminal item discriminator when present.
    pub const fn unsupported_kind_code(&self) -> Option<u8> {
        match self.kind {
            ProductCodecErrorKind::UnsupportedTerminalKind { code } => Some(code),
            _ => None,
        }
    }

    /// Returns a declared terminal data length when it disagreed with input.
    pub const fn declared_bytes(&self) -> Option<usize> {
        match self.kind {
            ProductCodecErrorKind::TerminalLengthMismatch { declared, .. } => Some(declared),
            _ => None,
        }
    }
}

impl Debug for ProductCodecError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProductCodecError")
            .field("stage", &self.stage)
            .field("category", &self.category())
            .field("limit", &self.limit())
            .field("observed_bytes", &self.observed_bytes())
            .field("unsupported_kind_code", &self.unsupported_kind_code())
            .field("declared_bytes", &self.declared_bytes())
            .finish_non_exhaustive()
    }
}

impl Display for ProductCodecError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "product {:?} payload is {}",
            self.stage,
            match self.category() {
                ProductCodecErrorCategory::Capacity => "over its byte limit",
                ProductCodecErrorCategory::Malformed => "malformed",
                ProductCodecErrorCategory::UnsupportedKind => "an unsupported kind",
            }
        )
    }
}

impl Error for ProductCodecError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        // Serde's diagnostic can include rejected input values. Redaction covers
        // this wrapper's Display/Debug, not recursive source formatting; outward
        // logs need the [stable accessors](../../../docs/reference/protocol/encoding.md#codec-errors-and-safe-diagnostics).
        match &self.kind {
            ProductCodecErrorKind::Malformed(source) => Some(source),
            _ => None,
        }
    }
}

/// Stable codec-error classification safe for protocol diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductCodecErrorCategory {
    /// Payload exceeded a configured hard byte limit.
    Capacity,
    /// Payload could not be decoded or had inconsistent lengths.
    Malformed,
    /// Binary item used an unknown discriminator.
    UnsupportedKind,
}

pub(crate) enum ProductCodecErrorKind {
    Empty,
    Capacity { observed: usize, limit: usize },
    Malformed(serde_json::Error),
    UnsupportedTerminalKind { code: u8 },
    TerminalLengthMismatch { declared: usize, observed: usize },
    InvalidTerminalScreen,
}

impl ProductCodecErrorKind {
    pub(crate) const fn capacity(observed: usize, limit: usize) -> Self {
        Self::Capacity { observed, limit }
    }
}
