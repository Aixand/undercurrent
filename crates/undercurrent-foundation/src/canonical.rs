// SPDX-License-Identifier: Apache-2.0
// See ../NOTICE for source attribution.

//! Bounded RFC 8785 canonicalization of decoded JSON values.
//!
//! Callers choose the semantic document and bound encoded input before parsing.
//! This module preflights the resulting value before serializing through a
//! bounded writer; it does not parse JSON or enforce a product schema.

use serde_json::Value;
use std::fmt::{self, Display, Formatter};
use std::io::{self, Write};
use thiserror::Error;

/// Resource limits for canonicalization, also reusable by a caller's parser.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalJsonLimits {
    /// Maximum canonical output bytes and aggregate visited-value nodes.
    ///
    /// A caller may also apply this bound to encoded input before parsing;
    /// canonicalizing an already decoded value cannot enforce that earlier bound.
    pub max_document_bytes: usize,
    /// Maximum root-inclusive JSON nesting depth.
    pub max_depth: usize,
    /// Maximum UTF-8 byte length of one string or object key.
    pub max_string_bytes: usize,
    /// Maximum number of values in one array.
    pub max_array_items: usize,
    /// Maximum number of entries in one object.
    pub max_object_entries: usize,
}

impl CanonicalJsonLimits {
    /// Conservative limits suitable for control documents.
    pub const DEFAULT: Self = Self {
        max_document_bytes: 1_048_576,
        max_depth: 64,
        max_string_bytes: 262_144,
        max_array_items: 65_536,
        max_object_entries: 65_536,
    };

    /// Absolute safe ceiling accepted by public canonical JSON mechanisms.
    ///
    /// This is deliberately separate from [`Self::DEFAULT`]: the default is a
    /// conservative general-purpose profile, while bounded internal document
    /// formats may require larger limits without opting out of a hard ceiling.
    pub const HARD_MAX: Self = Self {
        max_document_bytes: 16 * 1024 * 1024,
        max_depth: 128,
        max_string_bytes: 1024 * 1024,
        max_array_items: 131_072,
        max_object_entries: 131_072,
    };

    /// Validates that every limit is nonzero and does not exceed the hard safe profile.
    pub fn validate(self) -> Result<Self, CanonicalJsonError> {
        let maximum = Self::HARD_MAX;
        if self.max_document_bytes == 0
            || self.max_depth == 0
            || self.max_string_bytes == 0
            || self.max_array_items == 0
            || self.max_object_entries == 0
            || self.max_document_bytes > maximum.max_document_bytes
            || self.max_depth > maximum.max_depth
            || self.max_string_bytes > maximum.max_string_bytes
            || self.max_array_items > maximum.max_array_items
            || self.max_object_entries > maximum.max_object_entries
        {
            return Err(CanonicalJsonError::InvalidLimits);
        }
        Ok(self)
    }
}

impl Default for CanonicalJsonLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Canonicalizes an already decoded JSON value using explicit resource limits.
///
/// Preflight checks canonical size and aggregate node count before allocating
/// the document output writer. Measuring individual numbers can allocate small
/// temporary buffers. The writer separately enforces the output byte limit.
///
/// The root has depth one. String/key limits measure decoded UTF-8 bytes;
/// document size includes JSON punctuation and escapes. This function does not
/// bound prior parsing allocations, detect duplicate keys lost during decoding,
/// remove fields, or provide a whole-process memory quota. Failures return no
/// partial output and leave the input unchanged.
pub fn canonicalize_json_value_with_limits(
    value: &Value,
    limits: CanonicalJsonLimits,
) -> Result<Vec<u8>, CanonicalJsonError> {
    canonicalize_validated_value(value, limits.validate()?)
}

fn canonicalize_validated_value(
    value: &Value,
    limits: CanonicalJsonLimits,
) -> Result<Vec<u8>, CanonicalJsonError> {
    let mut budget = CanonicalValueBudget::new(limits.max_document_bytes);
    preflight_value(value, limits, 1, &mut budget)?;

    // Keep the writer's independent byte check: preflight avoids allocating the
    // document output for rejected input, but must not be the only limit if its
    // accounting drifts from the serializer. See the allocation sequence in
    // [the value contract](../docs/reference/values.md#preflight-and-allocation).
    let mut writer = BoundedCanonicalWriter::new(limits.max_document_bytes);
    if serde_json_canonicalizer::to_writer(value, &mut writer).is_err() {
        if let Some(actual) = writer.exceeded_at {
            return Err(CanonicalJsonError::DocumentTooLarge {
                actual,
                limit: limits.max_document_bytes,
            });
        }
        return Err(CanonicalJsonError::CanonicalizationFailed);
    }
    debug_assert_eq!(writer.bytes.len(), budget.encoded_bytes);
    Ok(writer.bytes)
}

fn preflight_value(
    value: &Value,
    limits: CanonicalJsonLimits,
    depth: usize,
    budget: &mut CanonicalValueBudget,
) -> Result<(), CanonicalJsonError> {
    if depth > limits.max_depth {
        return Err(CanonicalJsonError::DepthLimitExceeded {
            limit: limits.max_depth,
        });
    }
    budget.charge_node()?;
    match value {
        Value::String(value) => {
            validate_string(value, limits)?;
            budget.charge_string(value)
        }
        Value::Array(values) => {
            if values.len() > limits.max_array_items {
                return Err(CanonicalJsonError::ArrayLimitExceeded {
                    limit: limits.max_array_items,
                });
            }
            budget.charge(2usize.saturating_add(values.len().saturating_sub(1)))?;
            for value in values {
                preflight_value(value, limits, depth + 1, budget)?;
            }
            Ok(())
        }
        Value::Object(entries) => {
            if entries.len() > limits.max_object_entries {
                return Err(CanonicalJsonError::ObjectLimitExceeded {
                    limit: limits.max_object_entries,
                });
            }
            budget.charge(
                2usize
                    .saturating_add(entries.len().saturating_sub(1))
                    .saturating_add(entries.len()),
            )?;
            for (key, value) in entries {
                validate_string(key, limits)?;
                budget.charge_string(key)?;
                preflight_value(value, limits, depth + 1, budget)?;
            }
            Ok(())
        }
        Value::Null => budget.charge(4),
        Value::Bool(value) => budget.charge(if *value { 4 } else { 5 }),
        Value::Number(value) => {
            // Measure with the output encoder: ordinary JSON number formatting
            // can have a different length, making an exact byte budget reject
            // valid canonical output or undercount it.
            let canonical = serde_json_canonicalizer::to_vec(value)
                .map_err(|_| CanonicalJsonError::CanonicalizationFailed)?;
            budget.charge(canonical.len())
        }
    }
}

struct CanonicalValueBudget {
    encoded_bytes: usize,
    nodes: usize,
    maximum: usize,
}

impl CanonicalValueBudget {
    const fn new(maximum: usize) -> Self {
        Self {
            encoded_bytes: 0,
            nodes: 0,
            maximum,
        }
    }

    fn charge_node(&mut self) -> Result<(), CanonicalJsonError> {
        self.nodes = self.nodes.saturating_add(1);
        if self.nodes > self.maximum {
            return Err(self.too_large(self.maximum.saturating_add(1)));
        }
        Ok(())
    }

    fn charge_string(&mut self, value: &str) -> Result<(), CanonicalJsonError> {
        self.charge(2)?;
        for character in value.chars() {
            let encoded = match character {
                '"' | '\\' | '\u{0008}' | '\u{000c}' | '\n' | '\r' | '\t' => 2,
                '\u{0000}'..='\u{001f}' => 6,
                _ => character.len_utf8(),
            };
            self.charge(encoded)?;
        }
        Ok(())
    }

    fn charge(&mut self, additional: usize) -> Result<(), CanonicalJsonError> {
        let next = self.encoded_bytes.saturating_add(additional);
        if next > self.maximum {
            return Err(self.too_large(next));
        }
        self.encoded_bytes = next;
        Ok(())
    }

    const fn too_large(&self, actual: usize) -> CanonicalJsonError {
        CanonicalJsonError::DocumentTooLarge {
            actual,
            limit: self.maximum,
        }
    }
}

struct BoundedCanonicalWriter {
    bytes: Vec<u8>,
    maximum: usize,
    exceeded_at: Option<usize>,
}

impl BoundedCanonicalWriter {
    fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(maximum.min(8_192)),
            maximum,
            exceeded_at: None,
        }
    }
}

impl Write for BoundedCanonicalWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let next = self.bytes.len().saturating_add(buffer.len());
        if next > self.maximum {
            self.exceeded_at = Some(next);
            return Err(io::Error::other("canonical JSON output limit exceeded"));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn validate_string(value: &str, limits: CanonicalJsonLimits) -> Result<(), CanonicalJsonError> {
    if value.len() > limits.max_string_bytes {
        Err(CanonicalJsonError::StringLimitExceeded {
            limit: limits.max_string_bytes,
        })
    } else {
        Ok(())
    }
}

/// Failure to validate limits or canonicalize a decoded JSON value.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CanonicalJsonError {
    /// One or more configured limits were zero or exceeded the hard safe profile.
    #[error("canonical JSON limits must be nonzero and no greater than the hard safe profile")]
    InvalidLimits,
    /// Preflight byte/node accounting or canonical output exceeded its budget.
    #[error("JSON document contains {actual} bytes, exceeding the {limit}-byte limit")]
    DocumentTooLarge {
        /// Budget or write size at overflow, not necessarily the complete length.
        /// The aggregate node-budget path reports one more than the limit.
        actual: usize,
        /// Configured maximum length.
        limit: usize,
    },
    /// The nesting limit was exceeded.
    #[error("JSON nesting exceeds the configured depth limit of {limit}")]
    DepthLimitExceeded {
        /// Configured root-inclusive depth limit.
        limit: usize,
    },
    /// A string or key exceeded its UTF-8 byte limit.
    #[error("JSON string exceeds the configured byte limit of {limit}")]
    StringLimitExceeded {
        /// Configured per-string byte limit.
        limit: usize,
    },
    /// An array contained too many values.
    #[error("JSON array exceeds the configured item limit of {limit}")]
    ArrayLimitExceeded {
        /// Configured per-array item limit.
        limit: usize,
    },
    /// An object contained too many entries.
    #[error("JSON object exceeds the configured entry limit of {limit}")]
    ObjectLimitExceeded {
        /// Configured per-object entry limit.
        limit: usize,
    },
    /// A decoded value could not be represented by the canonical profile.
    #[error("JSON value is outside the canonicalization profile")]
    CanonicalizationFailed,
}

impl Display for CanonicalJsonLimits {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "bytes={}, depth={}, string={}, array={}, object={}",
            self.max_document_bytes,
            self.max_depth,
            self.max_string_bytes,
            self.max_array_items,
            self.max_object_entries
        )
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use serde_json::json;

    use super::*;

    fn canonical(value: &Value) -> Vec<u8> {
        canonicalize_json_value_with_limits(value, CanonicalJsonLimits::DEFAULT).unwrap()
    }

    #[test]
    fn canonical_json_matches_number_unicode_and_order_fixture() {
        let input = serde_json::from_str(
            r#"{
            "unicode":"€$\u000f\nA'B\"\\\"/",
            "numbers":[333333333.33333329,1E30,4.50,2e-3,0.000000000000000000000000001],
            "a":true
        }"#,
        )
        .unwrap();
        let expected = r#"{"a":true,"numbers":[333333333.3333333,1e+30,4.5,0.002,1e-27],"unicode":"€$\u000f\nA'B\"\\\"/"}"#;
        assert_eq!(canonical(&input), expected.as_bytes());
    }

    #[test]
    fn canonical_digest_has_a_byte_exact_golden_value() {
        assert_eq!(
            crate::Digest::sha256(&canonical(&json!({"b": 2, "a": 1}))).to_string(),
            "sha256:43258cff783fe7036d8a43033f830adfc60ec037382473548ac742b888292777"
        );
    }

    #[test]
    fn limits_accept_expanded_profiles_but_reject_zero_and_excessive_limits() {
        let defaults = CanonicalJsonLimits::DEFAULT;
        let expanded = CanonicalJsonLimits {
            max_document_bytes: defaults.max_document_bytes + 1,
            max_depth: defaults.max_depth + 1,
            max_string_bytes: defaults.max_string_bytes + 1,
            max_array_items: defaults.max_array_items + 1,
            max_object_entries: defaults.max_object_entries + 1,
        };
        assert_eq!(
            canonicalize_json_value_with_limits(&Value::Null, expanded).unwrap(),
            b"null"
        );
        let maximum = CanonicalJsonLimits::HARD_MAX;
        for limits in [
            CanonicalJsonLimits {
                max_document_bytes: 0,
                ..defaults
            },
            CanonicalJsonLimits {
                max_depth: 0,
                ..defaults
            },
            CanonicalJsonLimits {
                max_string_bytes: 0,
                ..defaults
            },
            CanonicalJsonLimits {
                max_array_items: 0,
                ..defaults
            },
            CanonicalJsonLimits {
                max_object_entries: 0,
                ..defaults
            },
            CanonicalJsonLimits {
                max_document_bytes: maximum.max_document_bytes + 1,
                ..maximum
            },
            CanonicalJsonLimits {
                max_depth: maximum.max_depth + 1,
                ..maximum
            },
            CanonicalJsonLimits {
                max_string_bytes: maximum.max_string_bytes + 1,
                ..maximum
            },
            CanonicalJsonLimits {
                max_array_items: maximum.max_array_items + 1,
                ..maximum
            },
            CanonicalJsonLimits {
                max_object_entries: maximum.max_object_entries + 1,
                ..maximum
            },
        ] {
            assert_eq!(
                canonicalize_json_value_with_limits(&Value::Null, limits),
                Err(CanonicalJsonError::InvalidLimits)
            );
        }
    }

    #[test]
    fn byte_depth_string_array_and_object_limits_fail_closed() {
        let limits = CanonicalJsonLimits {
            max_document_bytes: 32,
            max_depth: 2,
            max_string_bytes: 3,
            max_array_items: 2,
            max_object_entries: 1,
        };
        for (value, expected) in [
            (
                json!([[0]]),
                CanonicalJsonError::DepthLimitExceeded { limit: 2 },
            ),
            (
                json!("four"),
                CanonicalJsonError::StringLimitExceeded { limit: 3 },
            ),
            (
                json!({"four": 0}),
                CanonicalJsonError::StringLimitExceeded { limit: 3 },
            ),
            (
                json!([0, 1, 2]),
                CanonicalJsonError::ArrayLimitExceeded { limit: 2 },
            ),
            (
                json!({"a": 0, "b": 0}),
                CanonicalJsonError::ObjectLimitExceeded { limit: 1 },
            ),
        ] {
            assert_eq!(
                canonicalize_json_value_with_limits(&value, limits),
                Err(expected)
            );
        }
    }

    #[test]
    fn decoded_values_are_preflighted_before_bounded_canonical_output() {
        let limits = CanonicalJsonLimits {
            max_document_bytes: 32,
            max_depth: 4,
            max_string_bytes: 16,
            max_array_items: 64,
            max_object_entries: 8,
        };
        let wide = Value::Array((0..32).map(|_| Value::Null).collect());
        assert!(matches!(
            canonicalize_json_value_with_limits(&wide, limits),
            Err(CanonicalJsonError::DocumentTooLarge { limit: 32, .. })
        ));
        let deep = json!({"one": {"two": {"three": {"four": true}}}});
        assert_eq!(
            canonicalize_json_value_with_limits(
                &deep,
                CanonicalJsonLimits {
                    max_document_bytes: 128,
                    ..limits
                }
            ),
            Err(CanonicalJsonError::DepthLimitExceeded { limit: 4 })
        );
        let exact = json!({"escaped": "\u{0000}\n\\\""});
        let expected = br#"{"escaped":"\u0000\n\\\""}"#;
        let exact_limits = CanonicalJsonLimits {
            max_document_bytes: expected.len(),
            max_depth: 2,
            max_string_bytes: 16,
            max_array_items: 1,
            max_object_entries: 1,
        };
        assert_eq!(
            canonicalize_json_value_with_limits(&exact, exact_limits).unwrap(),
            expected
        );
        assert!(matches!(
            canonicalize_json_value_with_limits(
                &exact,
                CanonicalJsonLimits {
                    max_document_bytes: expected.len() - 1,
                    ..exact_limits
                }
            ),
            Err(CanonicalJsonError::DocumentTooLarge { .. })
        ));
    }

    proptest! {
        #[test]
        fn object_insertion_order_never_changes_canonical_bytes(first in any::<i64>(), second in any::<i64>()) {
            let left = json!({"alpha": first, "beta": second});
            let right = json!({"beta": second, "alpha": first});
            prop_assert_eq!(canonical(&left), canonical(&right));
        }
    }
}
