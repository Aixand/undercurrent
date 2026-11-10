// SPDX-License-Identifier: Apache-2.0
// See ../NOTICE for source attribution.

//! Shared classification and default sensitive-material detection policy.
//!
//! Detection does not redact, resolve secrets, or establish that arbitrary data
//! is safe to disclose. Callers enforce the result before crossing a boundary
//! and apply their own input-size limits; only traversal depth is bounded here.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};
use thiserror::Error;

const MAX_REDACTION_DEPTH: usize = 64;
const MAX_FIELD_NAME_BYTES: usize = 128;
const MAX_DIAGNOSTIC_TEXT_BYTES: usize = 8_192;

/// Base information classifications, from least to most sensitive.
///
/// Lowercase Serde values are persisted compatibility data; variant order is
/// sensitivity order. Labels express policy; this enum does not itself enforce
/// access or serialization.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Classification {
    /// Intended for unrestricted disclosure.
    Public,
    /// Intended for normal use inside the deployment boundary.
    Internal,
    /// Disclosure requires a defined business need.
    Confidential,
    /// Disclosure is tightly limited and audited.
    Restricted,
    /// Raw material must never enter ordinary diagnostics or serialization.
    Secret,
}

impl Classification {
    /// Returns the more protective of two classifications.
    #[must_use]
    pub fn max(self, other: Self) -> Self {
        std::cmp::max(self, other)
    }
}

impl Display for Classification {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Public => "public",
            Self::Internal => "internal",
            Self::Confidential => "confidential",
            Self::Restricted => "restricted",
            Self::Secret => "secret",
        })
    }
}

/// Failure to inspect structured values under the sensitivity policy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SensitiveMaterialError {
    /// Input nesting exceeded the structured inspection limit.
    #[error("structured redaction depth limit exceeded")]
    DepthLimitExceeded,
}

/// Keyless scanner for structured values that must contain no secret material.
///
/// The default field-name policy is fixed. Reusing a detector reuses its private
/// field set; inspection borrows the input and performs no mutation or redaction.
#[derive(Clone, Debug)]
pub struct SensitiveMaterialDetector {
    sensitive_fields: BTreeSet<String>,
}

impl SensitiveMaterialDetector {
    /// Creates a detector with the default sensitive-field policy.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sensitive_fields: default_sensitive_fields(),
        }
    }

    /// Returns whether the structured value contains classified, keyed, or
    /// credential-like secret material.
    ///
    /// Inspection starts at depth one and errors beyond depth 64. `Secret`
    /// classification is immediately sensitive; other classifications use the
    /// same field/text rules. Traversal stops on the first match or error, so a
    /// match can leave deeper branches unvisited. `Ok(false)` means no rule
    /// matched, not that every possible credential format was excluded. Callers
    /// must not treat an inspection error as permission to serialize.
    pub fn contains_sensitive_material(
        &self,
        value: &Value,
        classification: Classification,
    ) -> Result<bool, SensitiveMaterialError> {
        self.inspect_value(value, classification, 1)
    }

    fn inspect_value(
        &self,
        value: &Value,
        classification: Classification,
        depth: usize,
    ) -> Result<bool, SensitiveMaterialError> {
        if depth > MAX_REDACTION_DEPTH {
            return Err(SensitiveMaterialError::DepthLimitExceeded);
        }
        if classification == Classification::Secret {
            return Ok(true);
        }
        match value {
            Value::Object(entries) => {
                for (key, value) in entries {
                    if sensitive_field_matches(&self.sensitive_fields, key)
                        || self.inspect_value(value, classification, depth + 1)?
                    {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Value::Array(values) => {
                for value in values {
                    if self.inspect_value(value, classification, depth + 1)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Value::String(value) => Ok(credential_like_text(value)),
            Value::Null | Value::Bool(_) | Value::Number(_) => Ok(false),
        }
    }
}

impl Default for SensitiveMaterialDetector {
    fn default() -> Self {
        Self::new()
    }
}

fn default_sensitive_fields() -> BTreeSet<String> {
    [
        "access_key",
        "api_key",
        "authorization",
        "bearer",
        "client_secret",
        "cookie",
        "credential",
        "password",
        "private_key",
        "secret",
        "set_cookie",
        "token",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

/// Returns whether a field name denotes credential or secret material.
///
/// ASCII camel case and separator variants normalize to the same canonical
/// snake-case name, so `clientSecret`, `client-secret`, and `client_secret`
/// receive identical treatment.
#[must_use]
pub fn is_sensitive_field_name(name: &str) -> bool {
    sensitive_field_matches(&default_sensitive_fields(), name)
}

fn sensitive_field_matches(sensitive_fields: &BTreeSet<String>, name: &str) -> bool {
    // Names outside this policy's supported shape remain findings even when
    // their values look harmless: the environment serializer relies on this
    // result to reject the raw entry. See
    // [caller enforcement](../docs/internals/privacy.md#use-the-result-at-a-boundary).
    if name.is_empty()
        || name.len() > MAX_FIELD_NAME_BYTES
        || !name.is_ascii()
        || name.bytes().any(|byte| byte.is_ascii_control())
    {
        return true;
    }
    let normalized = normalize_sensitive_field_name(name.trim_start_matches('-'));
    sensitive_fields.contains(&normalized)
        || normalized.ends_with("_token")
        || normalized.ends_with("_secret")
        || normalized.ends_with("_password")
        || normalized.ends_with("_credential")
}

/// Returns whether a text value resembles raw credential material.
///
/// This conservative policy also flags text over 8,192 UTF-8 bytes, non-ASCII
/// text, and any ASCII control character. It is not a general secret detector.
#[must_use]
pub fn credential_like_text(value: &str) -> bool {
    if value.len() > MAX_DIAGNOSTIC_TEXT_BYTES
        || !value.is_ascii()
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return true;
    }
    credential_patterns(value)
}

/// Detects credential content in a private environment value.
///
/// Ordinary Unicode, tabs and line breaks are supported here. NUL and other
/// control characters remain invalid. Credential prefixes are checked on every
/// whitespace-delimited token, including multiline values. This does not make
/// the value safe to log; diagnostics must use [`credential_like_text`].
#[must_use]
pub fn credential_like_environment_text(value: &str) -> bool {
    value
        .chars()
        .any(|c| c.is_control() && !matches!(c, '\t' | '\n' | '\r'))
        || credential_patterns(value)
        || value.split_whitespace().any(|token| {
            token.eq_ignore_ascii_case("bearer")
                || token.eq_ignore_ascii_case("basic")
                || credential_patterns(token)
        })
}

fn credential_patterns(value: &str) -> bool {
    let lowercase = value.to_ascii_lowercase();
    lowercase.starts_with("bearer ")
        || lowercase.starts_with("basic ")
        || lowercase.starts_with("sk-")
        || lowercase.starts_with("ghp_")
        || value.starts_with("AKIA")
        || lowercase.contains(concat!("-----", "begin ", "private ", "key-----"))
        || lowercase.contains(concat!("-----", "begin rsa ", "private ", "key-----"))
        || lowercase.contains("password=")
        || lowercase.contains("access_token=")
        || lowercase.contains("bearer=")
        || lowercase.contains("client_secret=")
        || lowercase.contains("clientsecret=")
        || lowercase.contains("x-amz-signature=")
}

fn normalize_sensitive_field_name(name: &str) -> String {
    let mut normalized = String::with_capacity(name.len());
    let characters = name.chars().collect::<Vec<_>>();
    for (index, character) in characters.iter().copied().enumerate() {
        if character.is_ascii_uppercase() {
            let previous_is_lower_or_digit = index
                .checked_sub(1)
                .and_then(|previous| characters.get(previous))
                .is_some_and(|previous| previous.is_ascii_lowercase() || previous.is_ascii_digit());
            let acronym_boundary = index > 0
                && characters
                    .get(index + 1)
                    .is_some_and(char::is_ascii_lowercase);
            if (previous_is_lower_or_digit || acronym_boundary) && !normalized.ends_with('_') {
                normalized.push('_');
            }
            normalized.push(character.to_ascii_lowercase());
        } else if matches!(character, '-' | '.' | ' ') {
            if !normalized.ends_with('_') {
                normalized.push('_');
            }
        } else {
            normalized.push(character.to_ascii_lowercase());
        }
    }
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn classification_preserves_stored_values_and_ordering() {
        let variants = [
            Classification::Public,
            Classification::Internal,
            Classification::Confidential,
            Classification::Restricted,
            Classification::Secret,
        ];
        for (variant, text) in
            variants
                .into_iter()
                .zip(["public", "internal", "confidential", "restricted", "secret"])
        {
            assert_eq!(serde_json::to_value(variant).unwrap(), json!(text));
            assert_eq!(
                serde_json::from_value::<Classification>(json!(text)).unwrap(),
                variant
            );
            assert_eq!(variant.to_string(), text);
        }
        for pair in variants.windows(2) {
            assert!(pair[0] < pair[1]);
            assert_eq!(pair[0].max(pair[1]), pair[1]);
        }
        assert!(serde_json::from_value::<Classification>(json!("Restricted")).is_err());
    }

    #[test]
    fn sensitive_field_detection_normalizes_camel_case_and_bearer_names() {
        for field in [
            "bearer",
            "clientSecret",
            "client-secret",
            "client_secret",
            "APIKey",
            "nested.apiToken",
        ] {
            assert!(is_sensitive_field_name(field), "did not recognize {field}");
        }
        assert!(!is_sensitive_field_name("publicStatus"));
        assert!(is_sensitive_field_name(
            &"x".repeat(MAX_FIELD_NAME_BYTES + 1)
        ));
        assert!(credential_like_text(
            &"x".repeat(MAX_DIAGNOSTIC_TEXT_BYTES + 1)
        ));
    }

    #[test]
    fn keyless_sensitive_material_detector_preserves_default_policy() {
        let detector = SensitiveMaterialDetector::new();
        assert!(
            !detector
                .contains_sensitive_material(
                    &json!({"status": "ready", "count": 2}),
                    Classification::Internal
                )
                .unwrap()
        );
        for value in [
            json!({"clientSecret": "opaque-value"}),
            json!({"message": "Bearer opaque-value"}),
            json!({"nested": [{"API_KEY": "opaque-value"}]}),
            json!({"message": "ghp_opaque-value"}),
            json!({"message": "AKIAopaque-value"}),
            json!({"message": "sk-opaque-value"}),
            json!({"message": "credential\ntext"}),
            json!({"message": "non-ascii-\u{20ac}"}),
        ] {
            assert!(
                detector
                    .contains_sensitive_material(&value, Classification::Internal)
                    .unwrap()
            );
        }
        assert!(
            detector
                .contains_sensitive_material(&json!({"status": "ready"}), Classification::Secret)
                .unwrap()
        );
        let mut nested = Value::Null;
        for _ in 0..MAX_REDACTION_DEPTH {
            nested = json!([nested]);
        }
        assert_eq!(
            detector.contains_sensitive_material(&nested, Classification::Internal),
            Err(SensitiveMaterialError::DepthLimitExceeded)
        );
    }
}
