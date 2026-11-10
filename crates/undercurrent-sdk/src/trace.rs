// SPDX-License-Identifier: Apache-2.0

//! Secure, allocation-bounded client trace-context generation.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};

use undercurrent_wire::{CorrelationId, SpanId, TraceContext, TraceId};

const TRACE_SEED_BYTES: usize = 8;

/// Failure preparing or advancing client-owned distributed trace context.
#[derive(Debug)]
pub struct ClientGeneratedIdentifierError {
    field: &'static str,
    source: undercurrent_wire::core::IdentifierParseError,
}

impl ClientGeneratedIdentifierError {
    /// Returns the stable generated field name.
    pub const fn field(&self) -> &'static str {
        self.field
    }
}

impl Display for ClientGeneratedIdentifierError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "generated client {} is invalid", self.field)
    }
}

impl Error for ClientGeneratedIdentifierError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

/// Failure preparing or advancing client-owned distributed trace context.
#[derive(Debug)]
pub enum TraceContextGenerationError {
    /// Operating-system entropy was unavailable during client construction.
    Entropy(getrandom::Error),
    /// The per-physical-client trace sequence was exhausted.
    SequenceExhausted,
    /// A fixed-format generated identifier violated the Undercurrent wire contract.
    InvalidIdentifier(ClientGeneratedIdentifierError),
}

impl Display for TraceContextGenerationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Entropy(_) => formatter.write_str("could not initialize client trace context"),
            Self::SequenceExhausted => formatter.write_str("client trace sequence is exhausted"),
            Self::InvalidIdentifier(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for TraceContextGenerationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Entropy(error) => Some(error),
            Self::InvalidIdentifier(error) => Some(error),
            Self::SequenceExhausted => None,
        }
    }
}

pub(crate) struct TraceContextGenerator {
    seed: [u8; TRACE_SEED_BYTES],
    sequence: AtomicU64,
}

impl TraceContextGenerator {
    /// Seeds one physical client from operating-system entropy.
    pub(crate) fn secure() -> Result<Self, TraceContextGenerationError> {
        let mut seed = [0_u8; TRACE_SEED_BYTES];
        getrandom::fill(&mut seed).map_err(TraceContextGenerationError::Entropy)?;
        Ok(Self {
            seed,
            sequence: AtomicU64::new(0),
        })
    }

    /// Allocates the next unique trace and span pair without further IO.
    pub(crate) fn next(&self) -> Result<TraceContext, TraceContextGenerationError> {
        let sequence = self
            .sequence
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map(|previous| previous + 1)
            .map_err(|_| TraceContextGenerationError::SequenceExhausted)?;
        let mut trace_bytes = [0_u8; 16];
        trace_bytes[..TRACE_SEED_BYTES].copy_from_slice(&self.seed);
        trace_bytes[TRACE_SEED_BYTES..].copy_from_slice(&sequence.to_be_bytes());
        let trace_id = TraceId::new(hex(&trace_bytes)).map_err(|source| {
            TraceContextGenerationError::InvalidIdentifier(ClientGeneratedIdentifierError {
                field: "trace_id",
                source,
            })
        })?;
        let seed = u64::from_be_bytes(self.seed);
        let span_value = (seed ^ sequence.rotate_left(17)) | 1;
        let span_id = SpanId::new(hex(&span_value.to_be_bytes())).map_err(|source| {
            TraceContextGenerationError::InvalidIdentifier(ClientGeneratedIdentifierError {
                field: "span_id",
                source,
            })
        })?;
        let correlation_id = CorrelationId::new(trace_id.as_str()).map_err(|source| {
            TraceContextGenerationError::InvalidIdentifier(ClientGeneratedIdentifierError {
                field: "correlation_id",
                source,
            })
        })?;
        Ok(TraceContext::new(trace_id, span_id).with_correlation_id(correlation_id))
    }

    #[cfg(test)]
    const fn from_seed(seed: [u8; TRACE_SEED_BYTES]) -> Self {
        Self {
            seed,
            sequence: AtomicU64::new(0),
        }
    }
}

/// Encodes fixed-size identity bytes without an intermediate allocation.
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_contexts_are_fixed_width_unique_and_correlated() {
        let generator = TraceContextGenerator::from_seed([0xab; TRACE_SEED_BYTES]);

        let first = generator.next().expect("first context");
        let second = generator.next().expect("second context");

        assert_eq!(first.trace_id.as_str(), "abababababababab0000000000000001");
        assert_eq!(first.trace_id.as_str().len(), 32);
        assert_eq!(first.span_id.as_str().len(), 16);
        assert_eq!(
            first.correlation_id.as_ref().map(CorrelationId::as_str),
            Some(first.trace_id.as_str())
        );
        assert_ne!(first.trace_id, second.trace_id);
        assert_ne!(first.span_id, second.span_id);
    }
}
