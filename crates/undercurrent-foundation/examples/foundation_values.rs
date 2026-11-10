// SPDX-License-Identifier: Apache-2.0

//! Exercise Foundation values using only in-memory, synthetic data.

use serde_json::json;
use undercurrent_foundation::{
    BoundedString, CanonicalJsonLimits, Classification, Digest, OpaqueUri, OpaqueUriError,
    SensitiveMaterialDetector, canonicalize_json_value_with_limits,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let value = json!({"b": 2, "a": 1});
    let canonical = canonicalize_json_value_with_limits(&value, CanonicalJsonLimits::DEFAULT)?;
    assert_eq!(canonical, br#"{"a":1,"b":2}"#);
    let digest = Digest::sha256(&canonical);
    assert_eq!(
        digest.to_string(),
        "sha256:43258cff783fe7036d8a43033f830adfc60ec037382473548ac742b888292777"
    );
    let decoded_digest: Digest = serde_json::from_str(&serde_json::to_string(&digest)?)?;
    decoded_digest.verify(&canonical)?;
    assert!(decoded_digest.verify(b"changed content").is_err());
    println!("canonical: {}", std::str::from_utf8(&canonical)?);
    println!("digest: {digest}");

    let bounded: BoundedString<5> = serde_json::from_str(r#""ready""#)?;
    assert!(serde_json::from_str::<BoundedString<5>>(r#""longer""#).is_err());
    println!("bounded string: {}", bounded.as_str());

    let uri = OpaqueUri::parse("trust://passport/subjects/sub_01")?;
    assert_eq!(
        OpaqueUri::parse("https://example.test/data?token=synthetic"),
        Err(OpaqueUriError::CredentialMaterial)
    );
    println!("opaque URI: {uri}");

    let detector = SensitiveMaterialDetector::new();
    assert!(!detector.contains_sensitive_material(
        &json!({"status": "ready", "count": 2}),
        Classification::Internal,
    )?);
    assert!(detector.contains_sensitive_material(
        &json!({"clientSecret": "synthetic-value"}),
        Classification::Internal,
    )?);
    println!("privacy: safe value accepted; sensitive value rejected");
    Ok(())
}
