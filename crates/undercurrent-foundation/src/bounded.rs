// SPDX-License-Identifier: Apache-2.0
// See ../NOTICE for source attribution.

//! Allocation-aware bounded string decoding for public wire scalars.
//!
//! The visitor checks decoded UTF-8 bytes before making its own copy. It cannot
//! bound allocations already performed by the deserializer or transport.

use std::fmt;

use serde::{Deserialize, Deserializer, de};

/// A string accepted from Serde only when its UTF-8 byte length is bounded.
///
/// Domain scalar deserializers use this wrapper before running their stricter
/// canonical parser, avoiding an intermediate unconstrained `String` API.
/// Empty strings are accepted, including when `MAX_BYTES` is zero. This wrapper
/// does not normalize text or apply domain syntax or sensitivity checks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundedString<const MAX_BYTES: usize>(String);

impl<const MAX_BYTES: usize> BoundedString<MAX_BYTES> {
    /// Borrows the decoded string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the wrapper and returns the bounded string.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl<'de, const MAX_BYTES: usize> Deserialize<'de> for BoundedString<MAX_BYTES> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor<const MAX_BYTES: usize>;

        impl<const MAX_BYTES: usize> de::Visitor<'_> for Visitor<MAX_BYTES> {
            type Value = BoundedString<MAX_BYTES>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "a string of at most {MAX_BYTES} UTF-8 bytes")
            }

            fn visit_borrowed_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_str(value)
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value.len() > MAX_BYTES {
                    return Err(E::invalid_length(value.len(), &self));
                }
                Ok(BoundedString(value.to_owned()))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value.len() > MAX_BYTES {
                    return Err(E::invalid_length(value.len(), &self));
                }
                Ok(BoundedString(value))
            }
        }

        deserializer.deserialize_str(Visitor::<MAX_BYTES>)
    }
}

#[cfg(test)]
mod tests {
    use super::BoundedString;
    use crate::{Digest, OpaqueUri};

    #[test]
    fn bounded_string_rejects_plain_and_escaped_oversized_values() {
        assert!(serde_json::from_str::<BoundedString<4>>(r#""test""#).is_ok());
        assert!(serde_json::from_str::<BoundedString<4>>(r#""tests""#).is_err());
        assert!(
            serde_json::from_str::<BoundedString<4>>(r#""\u0061\u0061\u0061\u0061\u0061""#)
                .is_err()
        );
    }

    #[test]
    fn public_scalar_deserializers_reject_oversized_strings_before_domain_parsing() {
        let oversized = serde_json::to_string(&"x".repeat(4_096)).unwrap();

        assert!(serde_json::from_str::<Digest>(&oversized).is_err());
        assert!(serde_json::from_str::<OpaqueUri>(&oversized).is_err());
    }
}
