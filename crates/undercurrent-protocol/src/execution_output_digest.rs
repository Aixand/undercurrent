// SPDX-License-Identifier: Apache-2.0

//! Canonical integrity helpers for bounded inline execution output.

use undercurrent_foundation::ContentDigest;

const FOUNDATION_SHA256_PREFIX: &str = "sha256:";
const SHA256_HEX_CHARS: usize = 64;

/// Computes the canonical lowercase SHA-256 hex carried by execution output DTOs.
///
/// The product protocol intentionally omits Foundation's `sha256:` display prefix
/// because the DTO field already fixes the digest algorithm.
#[must_use]
pub fn execution_output_payload_digest_sha256(bytes: &[u8]) -> String {
    ContentDigest::sha256(bytes)
        .to_string()
        .trim_start_matches(FOUNDATION_SHA256_PREFIX)
        .to_owned()
}

/// Verifies bounded inline execution output against its canonical DTO digest.
///
/// Non-canonical uppercase, prefixed, truncated, or otherwise malformed values
/// fail closed. Digest comparison is delegated to Foundation's constant-time
/// verification mechanism.
#[must_use]
pub fn verify_execution_output_payload_digest(bytes: &[u8], expected_hex: &str) -> bool {
    if expected_hex.len() != SHA256_HEX_CHARS
        || !expected_hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return false;
    }

    let encoded = format!("{FOUNDATION_SHA256_PREFIX}{expected_hex}");
    ContentDigest::parse(&encoded).is_ok_and(|digest| digest.verify(bytes).is_ok())
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;

    #[test]
    fn execution_output_digest_matches_golden_and_verifies_constant_time() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/execution_output_digest.json"
        ))
        .expect("digest fixture must be valid JSON");
        let payload = fixture["payload_utf8"]
            .as_str()
            .expect("fixture payload must be text")
            .as_bytes();
        let expected = fixture["digest_hex"]
            .as_str()
            .expect("fixture digest must be text");

        assert_eq!(execution_output_payload_digest_sha256(payload), expected);
        assert!(verify_execution_output_payload_digest(payload, expected));
        assert!(!verify_execution_output_payload_digest(b"abd", expected));
        assert!(!verify_execution_output_payload_digest(
            payload,
            &format!("sha256:{expected}")
        ));
        assert!(!verify_execution_output_payload_digest(
            payload,
            &expected.to_uppercase()
        ));
    }
}
