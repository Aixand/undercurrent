// SPDX-License-Identifier: Apache-2.0
// See ../NOTICE for source attribution.

//! Structurally validated opaque URIs with selected credential checks.
//!
//! These values preserve caller-owned reference text without resolving it.
//! Validation rejects userinfo and selected query-name forms; it is not a
//! general secret scanner, URL normalizer, or authority grant. The generated
//! schema mirrors the current constructor checks; neither defines a general URL grammar.

use std::borrow::Cow;
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

const MAX_URI_BYTES: usize = 2_048;

/// An opaque URI with structural and selected credential checks, never resolved here.
///
/// Accepted text is preserved exactly. Equality and ordering use that text,
/// without host/path normalization. Display and serialization expose the full
/// accepted string; this is not a redacting wrapper.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OpaqueUri(String);

impl OpaqueUri {
    /// Parses an ASCII URI of at most 2,048 bytes with a lowercase scheme.
    ///
    /// Requires `://` and a nonempty authority; rejects whitespace, controls,
    /// backslashes, authority userinfo, and selected credential-like query names.
    /// Query values and paths are not scanned for arbitrary secrets. Conversion
    /// to an owned string occurs before validation; Serde decoding uses a bounded
    /// string visitor first. No I/O or dereferencing occurs.
    pub fn parse(input: impl Into<String>) -> Result<Self, OpaqueUriError> {
        let input = input.into();
        validate_uri(&input)?;
        Ok(Self(input))
    }

    /// Returns the validated URI text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for OpaqueUri {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for OpaqueUri {
    type Err = OpaqueUriError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Self::parse(input)
    }
}

impl Serialize for OpaqueUri {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for OpaqueUri {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = crate::BoundedString::<MAX_URI_BYTES>::deserialize(deserializer)?;
        Self::parse(value.into_string()).map_err(de::Error::custom)
    }
}

impl JsonSchema for OpaqueUri {
    fn schema_name() -> Cow<'static, str> {
        "OpaqueUri".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        schemars::json_schema!({
            "type": "string",
            "pattern": "^[a-z][a-z0-9+.-]*://[^\\s\\\\/?#@]+(?:[/?#][^\\s\\\\]*)?$",
            "minLength": 5,
            "maxLength": MAX_URI_BYTES,
            "not": {
                "anyOf": [
                    {"pattern": "[^\\u0021-\\u005B\\u005D-\\u007E]"},
                    {"pattern": "^[^?#]*[?](?:[^#]*&)?[^=&#]*%"},
                    {
                        "pattern": "^[^?#]*[?](?:[^#]*&)?(?:[aA][cC][cC][eE][sS][sS]_[tT][oO][kK][eE][nN]|[aA][pP][iI]_[kK][eE][yY]|[aA][uU][tT][hH][oO][rR][iI][zZ][aA][tT][iI][oO][nN]|[bB][eE][aA][rR][eE][rR]|[cC][rR][eE][dD][eE][nN][tT][iI][aA][lL]|[pP][aA][sS][sS][wW][oO][rR][dD]|[sS][eE][cC][rR][eE][tT]|[sS][iI][gG]|[sS][iI][gG][nN][aA][tT][uU][rR][eE]|[tT][oO][kK][eE][nN]|[xX]-[aA][mM][zZ]-[cC][rR][eE][dD][eE][nN][tT][iI][aA][lL]|[xX]-[aA][mM][zZ]-[sS][iI][gG][nN][aA][tT][uU][rR][eE])(?:=|&|#|$)"
                    }
                ]
            }
        })
    }
}

/// Structural failure in an opaque URI.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum OpaqueUriError {
    /// The URI was empty or exceeded its byte limit.
    #[error("opaque URI length is outside the supported range")]
    InvalidLength,
    /// The URI contained non-ASCII, whitespace, a control character, or a backslash.
    #[error("opaque URI contains a noncanonical character")]
    NonCanonicalCharacter,
    /// The URI did not contain a canonical lowercase scheme and nonempty authority.
    #[error("opaque URI must contain a lowercase scheme and nonempty authority")]
    InvalidShape,
    /// The URI contained user information or a credential-like query parameter.
    #[error("opaque URI must not contain credential material")]
    CredentialMaterial,
}

fn validate_uri(input: &str) -> Result<(), OpaqueUriError> {
    if input.is_empty() || input.len() > MAX_URI_BYTES {
        return Err(OpaqueUriError::InvalidLength);
    }
    if !input.is_ascii()
        || input
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace() || byte == b'\\')
    {
        return Err(OpaqueUriError::NonCanonicalCharacter);
    }
    let (scheme, remainder) = input
        .split_once("://")
        .ok_or(OpaqueUriError::InvalidShape)?;
    if scheme.is_empty()
        || !scheme.as_bytes()[0].is_ascii_lowercase()
        || !scheme.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'+' | b'.' | b'-')
        })
        || remainder.is_empty()
    {
        return Err(OpaqueUriError::InvalidShape);
    }
    let authority_end = remainder.find(['/', '?', '#']).unwrap_or(remainder.len());
    let authority = &remainder[..authority_end];
    if authority.is_empty() {
        return Err(OpaqueUriError::InvalidShape);
    }
    if authority.contains('@') || contains_sensitive_query_parameter(input) {
        return Err(OpaqueUriError::CredentialMaterial);
    }
    Ok(())
}

fn contains_sensitive_query_parameter(uri: &str) -> bool {
    // A question mark inside the fragment does not start a query. Preserve
    // the opaque spelling, scanning only names before the first fragment.
    let before_fragment = uri.split('#').next().unwrap_or(uri);
    let Some(query) = before_fragment.split_once('?').map(|(_, query)| query) else {
        return false;
    };
    query.split('&').any(|pair| {
        let raw_name = pair.split('=').next().unwrap_or(pair);
        // The denylist compares undecoded names; allowing escapes would let
        // spellings such as `%74oken` evade it. Reject even malformed escapes
        // instead of decoding or rewriting the caller's reference. See
        // [query-name rejection](../docs/reference/values.md#query-name-rejection).
        if raw_name.contains('%') {
            return true;
        }
        let name = raw_name.to_ascii_lowercase();
        [
            "access_token",
            "api_key",
            "authorization",
            "bearer",
            "credential",
            "password",
            "secret",
            "sig",
            "signature",
            "token",
            "x-amz-credential",
            "x-amz-signature",
        ]
        .contains(&name.as_str())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opaque_uri_rejects_userinfo_and_signed_query_material() {
        for safe in [
            "trust://passport/subjects/sub_01",
            "https://example.test/data?view=summary&page=2",
        ] {
            assert!(OpaqueUri::parse(safe).is_ok());
        }
        assert_eq!(
            OpaqueUri::parse("https://user:password@example.test/data"),
            Err(OpaqueUriError::CredentialMaterial)
        );
        assert_eq!(
            OpaqueUri::parse("https://example.test/data?X-Amz-Signature=secret"),
            Err(OpaqueUriError::CredentialMaterial)
        );
        for unsafe_uri in [
            "https://example.test/data?BeArEr=secret",
            "https://example.test/data?%74oken=secret",
            "https://example.test/data?to%6ben=secret",
            "https://example.test/data?access%5ftoken=secret",
            "https://example.test/data?view%26token=secret",
            "https://example.test/data?malformed%=value",
        ] {
            assert_eq!(
                OpaqueUri::parse(unsafe_uri),
                Err(OpaqueUriError::CredentialMaterial)
            );
        }
    }

    #[test]
    fn fragment_question_marks_do_not_start_queries() {
        for uri in [
            "x://host#?token=x",
            "x://host#?%74oken=x",
            "x://host?ok=x#?token=y",
            "x://host?ok=%23%3Ftoken=x",
            "x://host/path??view=x",
        ] {
            let parsed = OpaqueUri::parse(uri).unwrap();
            assert_eq!(parsed.as_str(), uri);
            assert_eq!(
                serde_json::from_str::<OpaqueUri>(&serde_json::to_string(&parsed).unwrap())
                    .unwrap(),
                parsed
            );
            let schema = serde_json::to_value(schemars::schema_for!(OpaqueUri)).unwrap();
            assert!(
                jsonschema::validator_for(&schema)
                    .unwrap()
                    .is_valid(&serde_json::json!(uri))
            );
        }
        assert!(OpaqueUri::parse("x://host?token=x#?ok=y").is_err());
        assert!(OpaqueUri::parse("x://host?%74oken=x#fragment").is_err());
    }

    #[test]
    fn generated_schema_matches_current_parser_across_uri_components() {
        let schema = serde_json::to_value(schemars::schema_for!(OpaqueUri)).unwrap();
        let validator = jsonschema::validator_for(&schema).unwrap();
        let check = |uri: &str| {
            assert_eq!(
                validator.is_valid(&serde_json::json!(uri)),
                OpaqueUri::parse(uri).is_ok(),
                "schema/parser mismatch for {uri:?}"
            );
        };
        for uri in [
            "https://user@example.test",
            "trust://é",
            "trust://host/path&token=x",
            "x://host?token=x",
            "x://host?ok=token",
            "x://host?ok=a%20b",
            "x://host?%74oken=x",
            "x://host/path%25",
            "x://host#fragment&token=x",
            "x://host?ok=x#fragment&token=x",
            "x://host?ok=x&ToKeN=y",
            "x://host?x?token=y",
            "x://host?ok=x&token",
            "x://host?ok=x&token#fragment",
        ] {
            check(uri);
        }
        for ch in (0..=127)
            .map(|code| char::from_u32(code).unwrap())
            .chain(['é', '€', '😀'])
        {
            for uri in [
                format!("x://a{ch}b"),
                format!("x://host/path{ch}tail"),
                format!("x://host?na{ch}me=value"),
                format!("x://host?name=va{ch}lue"),
                format!("x://host#fragment{ch}tail"),
                format!("x://host?ok=1&na{ch}me=x"),
            ] {
                check(&uri);
            }
        }
        for bytes in [MAX_URI_BYTES - 1, MAX_URI_BYTES, MAX_URI_BYTES + 1] {
            check(&format!("x://{}", "a".repeat(bytes - 4)));
        }
    }

    #[test]
    fn generated_schema_bounds_and_shapes_opaque_uris() {
        let schema = serde_json::to_value(schemars::schema_for!(OpaqueUri)).unwrap();
        let validator = jsonschema::validator_for(&schema).unwrap();
        assert!(validator.is_valid(&serde_json::json!("trust://passport/subjects/sub_01")));
        for invalid in [
            "Trust://passport/subjects/sub_01",
            "trust:/missing",
            "trust://",
            "https://example.test/data?token=secret",
            "https://example.test/data?X-Amz-Signature=secret",
            "https://example.test/data?to%6ben=secret",
        ] {
            assert!(OpaqueUri::parse(invalid).is_err());
            assert!(!validator.is_valid(&serde_json::json!(invalid)));
        }
        assert!(!validator.is_valid(&serde_json::json!(format!(
            "trust://{}",
            "a".repeat(MAX_URI_BYTES)
        ))));
    }
}
