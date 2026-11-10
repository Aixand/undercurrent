// SPDX-License-Identifier: Apache-2.0
// See ../NOTICE for source attribution.

//! SHA-256 content digests and constant-time verification.
//!
//! Hashing uses exact caller-provided bytes. Canonicalization, domain separation,
//! payload limits, and the trustworthiness of an expected digest belong to the
//! caller. A matching digest is not authentication or proof of execution.

use std::borrow::Cow;
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use sha2::{Digest as _, Sha256};
use subtle::ConstantTimeEq;
use thiserror::Error;

const DIGEST_PREFIX: &str = "sha256:";
const DIGEST_BYTES: usize = 32;
const DIGEST_HEX_CHARS: usize = DIGEST_BYTES * 2;

/// A SHA-256 digest serialized as `sha256:<lowercase-hex>`.
///
/// Parsing accepts exactly 64 lowercase hexadecimal digits after the prefix;
/// Serde first bounds the complete decoded string to 71 UTF-8 bytes. Derived
/// equality and ordering do not promise constant-time behavior: use
/// [`Self::constant_time_eq`] or [`Self::verify`] where that property is needed.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Digest([u8; DIGEST_BYTES]);

/// Semantic alias used when a digest addresses content bytes.
pub type ContentDigest = Digest;

impl Digest {
    /// Computes SHA-256 over the exact supplied bytes.
    #[must_use]
    pub fn sha256(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    /// Parses the strict lowercase wire representation.
    pub fn parse(input: &str) -> Result<Self, DigestParseError> {
        let hex = input
            .strip_prefix(DIGEST_PREFIX)
            .ok_or(DigestParseError::InvalidAlgorithm)?;
        if hex.len() != DIGEST_HEX_CHARS {
            return Err(DigestParseError::InvalidLength);
        }
        if !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(DigestParseError::NonCanonicalHex);
        }

        let mut bytes = [0_u8; DIGEST_BYTES];
        for (index, pair) in hex.as_bytes().chunks_exact(2).enumerate() {
            bytes[index] = (decode_nibble(pair[0]) << 4) | decode_nibble(pair[1]);
        }
        Ok(Self(bytes))
    }

    /// Returns the 32 digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; DIGEST_BYTES] {
        &self.0
    }

    /// Verifies bytes against this digest using constant-time digest comparison.
    ///
    /// Hashing time depends on payload length. A mismatch returns
    /// [`DigestVerificationError`] without including either digest or payload.
    pub fn verify(&self, bytes: &[u8]) -> Result<(), DigestVerificationError> {
        let actual = Self::sha256(bytes);
        if self.0.ct_eq(&actual.0).into() {
            Ok(())
        } else {
            Err(DigestVerificationError)
        }
    }

    /// Compares two digest values in constant time.
    #[must_use]
    pub fn constant_time_eq(&self, other: &Self) -> bool {
        self.0.ct_eq(&other.0).into()
    }
}

impl Display for Digest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(DIGEST_PREFIX)?;
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for Digest {
    type Err = DigestParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Self::parse(input)
    }
}

impl Serialize for Digest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Digest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = crate::BoundedString::<71>::deserialize(deserializer)?;
        Self::parse(value.as_str()).map_err(de::Error::custom)
    }
}

impl JsonSchema for Digest {
    fn schema_name() -> Cow<'static, str> {
        "Digest".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        schemars::json_schema!({
            "type": "string",
            "pattern": "^sha256:[0-9a-f]{64}$",
            "minLength": 71,
            "maxLength": 71,
        })
    }
}

/// Failure to parse a SHA-256 digest.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum DigestParseError {
    /// The supported `sha256:` algorithm marker was absent.
    #[error("digest must use the `sha256:` algorithm prefix")]
    InvalidAlgorithm,
    /// The SHA-256 value did not contain exactly 64 hexadecimal characters.
    #[error("SHA-256 digest must contain exactly 64 hexadecimal characters")]
    InvalidLength,
    /// The value contained uppercase or non-hexadecimal characters.
    #[error("SHA-256 digest must use lowercase hexadecimal characters")]
    NonCanonicalHex,
}

/// Content did not match its declared digest.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("content digest verification failed")]
pub struct DigestVerificationError;

const fn decode_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_matches_public_golden_vector() {
        let digest = Digest::sha256(b"abc");
        assert_eq!(
            digest.to_string(),
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(Digest::parse(&digest.to_string()).unwrap(), digest);
        assert!(digest.verify(b"abc").is_ok());
        assert_eq!(digest.verify(b"abd"), Err(DigestVerificationError));
    }

    #[test]
    fn digest_parser_rejects_noncanonical_forms() {
        assert_eq!(
            Digest::parse(
                "SHA256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
            ),
            Err(DigestParseError::InvalidAlgorithm)
        );
        assert_eq!(
            Digest::parse(
                "sha256:BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD"
            ),
            Err(DigestParseError::NonCanonicalHex)
        );
    }

    #[test]
    fn generated_schema_enforces_the_exact_digest_wire_shape() {
        let schema = serde_json::to_value(schemars::schema_for!(Digest)).unwrap();
        let validator = jsonschema::validator_for(&schema).unwrap();
        let valid = Digest::sha256(b"schema").to_string();
        assert!(validator.is_valid(&serde_json::json!(valid)));
        assert!(!validator.is_valid(&serde_json::json!(valid.to_uppercase())));
        assert!(!validator.is_valid(&serde_json::json!("sha256:00")));
    }
}
