// SPDX-License-Identifier: Apache-2.0

//! Integrity and explicit truncation metadata for bounded runtime output.

use crate::{DomainError, DomainResult, InvalidValueReason, PayloadDigest};

/// Digest and length evidence for one bounded output chunk or page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputChunkIntegrity {
    digest: PayloadDigest,
    delivered_bytes: u64,
    source_bytes: u64,
    truncated: bool,
}

impl OutputChunkIntegrity {
    /// Builds metadata from delivered bytes and original size.
    pub fn for_payload(payload: &[u8], source_bytes: u64) -> DomainResult<Self> {
        let delivered_bytes = u64::try_from(payload.len()).map_err(|_| invalid_output())?;
        if source_bytes < delivered_bytes {
            return Err(invalid_output());
        }
        Ok(Self {
            digest: PayloadDigest::of_bytes(payload),
            delivered_bytes,
            source_bytes,
            truncated: delivered_bytes < source_bytes,
        })
    }

    /// Restores validated persisted or protocol metadata.
    pub fn from_metadata(
        digest: PayloadDigest,
        delivered_bytes: u64,
        source_bytes: u64,
        truncated: bool,
    ) -> DomainResult<Self> {
        if source_bytes < delivered_bytes || truncated != (delivered_bytes < source_bytes) {
            return Err(invalid_output());
        }
        Ok(Self {
            digest,
            delivered_bytes,
            source_bytes,
            truncated,
        })
    }

    /// Returns payload digest.
    pub const fn digest(self) -> PayloadDigest {
        self.digest
    }
    /// Returns delivered bytes.
    pub const fn delivered_bytes(self) -> u64 {
        self.delivered_bytes
    }
    /// Returns original source bytes.
    pub const fn source_bytes(self) -> u64 {
        self.source_bytes
    }
    /// Returns whether output was truncated.
    pub const fn truncated(self) -> bool {
        self.truncated
    }

    /// Verifies exact delivered bytes without interpreting private output.
    pub fn verify(self, payload: &[u8]) -> OutputIntegrityStatus {
        if u64::try_from(payload.len()).ok() != Some(self.delivered_bytes) {
            return OutputIntegrityStatus::LengthMismatch;
        }
        if PayloadDigest::of_bytes(payload) != self.digest {
            return OutputIntegrityStatus::DigestMismatch;
        }
        OutputIntegrityStatus::Verified {
            truncated: self.truncated,
        }
    }
}

/// Result of verifying one bounded output payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputIntegrityStatus {
    /// Length and digest match.
    Verified { truncated: bool },
    /// Length differs from metadata.
    LengthMismatch,
    /// Digest differs from metadata.
    DigestMismatch,
}

fn invalid_output() -> DomainError {
    DomainError::InvalidValue {
        field: "output_chunk_integrity",
        reason: InvalidValueReason::InvalidCombination {
            rule: "source size, delivered size, and truncation flag must agree",
        },
    }
}
