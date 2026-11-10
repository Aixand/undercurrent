// SPDX-License-Identifier: Apache-2.0

//! Durable opaque references to adapter-resolved secrets.

use std::fmt::{Debug, Formatter};

use crate::{DomainError, DomainResult, InvalidValueReason};

/// Maximum persisted secret-reference length.
pub const MAX_SECRET_REFERENCE_BYTES: usize = 512;

const PRIVATE_KEY_PEM_MARKER: &str = concat!("-----BEGIN ", "PRIVATE KEY-----");
const RSA_PRIVATE_KEY_PEM_MARKER: &str = concat!("-----BEGIN ", "RSA PRIVATE KEY-----");

/// Opaque durable handle resolved only by an authorized outer adapter.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SecretReference(String);

impl SecretReference {
    /// Validates a bounded non-secret reference without resolving it.
    pub fn new(value: impl Into<String>) -> DomainResult<Self> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_SECRET_REFERENCE_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(DomainError::InvalidValue {
                field: "secret_reference",
                reason: InvalidValueReason::InvalidCombination {
                    rule: "secret reference must be non-empty, bounded, and contain no control characters",
                },
            });
        }
        Ok(Self(value))
    }

    /// Exposes the reference, never resolved material, to an adapter.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Classification result for a raw environment entry at an adapter boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawEnvironmentClassification {
    /// Entry is eligible for explicit non-secret persistence.
    NonSecret,
    /// Entry resembles credential material and must move behind a secret reference.
    CredentialClassified,
}

/// Classifies raw environment material without exposing it in diagnostics.
pub fn classify_raw_environment_entry(name: &str, value: &str) -> RawEnvironmentClassification {
    let normalized: String = name
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect();
    let sensitive_name = [
        "accesstoken",
        "apikey",
        "authorization",
        "clientsecret",
        "credential",
        "password",
        "passwd",
        "privatekey",
        "secret",
        "sessioncookie",
        "token",
    ]
    .iter()
    .any(|marker| normalized.contains(marker));
    let trimmed = value.trim();
    let lowercase = trimmed.to_ascii_lowercase();
    let sensitive_value = lowercase.starts_with("bearer ")
        || lowercase.starts_with("basic ")
        || lowercase.starts_with("sk-")
        || lowercase.starts_with("ghp_")
        || trimmed.starts_with("AKIA")
        || trimmed.contains(PRIVATE_KEY_PEM_MARKER)
        || trimmed.contains(RSA_PRIVATE_KEY_PEM_MARKER);
    if sensitive_name || sensitive_value {
        RawEnvironmentClassification::CredentialClassified
    } else {
        RawEnvironmentClassification::NonSecret
    }
}

impl Debug for SecretReference {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecretReference")
            .field("byte_len", &self.0.len())
            .finish_non_exhaustive()
    }
}
