// SPDX-License-Identifier: Apache-2.0

//! Exact release identities for connection negotiation.

use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Sole protocol implemented by this release.
pub const CURRENT_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion {
    major: 0,
    minor: 1,
    patch: 0,
    beta: true,
};

/// Full release identity used in the connection handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ProtocolVersion {
    /// Semantic major version.
    pub major: u16,
    /// Semantic minor version.
    pub minor: u16,
    /// Semantic patch version.
    pub patch: u16,
    /// Whether this is the beta release.
    pub beta: bool,
}

impl ProtocolVersion {
    /// Constructs a stable version for explicit compatibility checks.
    pub const fn new(major: u16, minor: u16) -> Self {
        Self {
            major,
            minor,
            patch: 0,
            beta: false,
        }
    }

    /// Both peers must select the exact current release.
    pub const fn can_read(self, produced: Self) -> bool {
        self.major == CURRENT_PROTOCOL_VERSION.major
            && self.minor == CURRENT_PROTOCOL_VERSION.minor
            && self.patch == CURRENT_PROTOCOL_VERSION.patch
            && self.beta == CURRENT_PROTOCOL_VERSION.beta
            && produced.major == CURRENT_PROTOCOL_VERSION.major
            && produced.minor == CURRENT_PROTOCOL_VERSION.minor
            && produced.patch == CURRENT_PROTOCOL_VERSION.patch
            && produced.beta == CURRENT_PROTOCOL_VERSION.beta
    }
}

impl Display for ProtocolVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{}.{}{}",
            self.major,
            self.minor,
            self.patch,
            if self.beta { "-beta" } else { "" }
        )
    }
}

impl From<ProtocolVersion> for String {
    fn from(value: ProtocolVersion) -> Self {
        value.to_string()
    }
}

impl TryFrom<String> for ProtocolVersion {
    type Error = &'static str;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        let (numeric, beta) = value
            .strip_suffix("-beta")
            .map_or((value.as_str(), false), |v| (v, true));
        let parts: Vec<_> = numeric.split('.').collect();
        if parts.len() != 3
            || parts
                .iter()
                .any(|v| v.is_empty() || (v.len() > 1 && v.starts_with('0')))
        {
            return Err("invalid release version");
        }
        let version = Self {
            major: parts[0].parse().map_err(|_| "invalid major version")?,
            minor: parts[1].parse().map_err(|_| "invalid minor version")?,
            patch: parts[2].parse().map_err(|_| "invalid patch version")?,
            beta,
        };
        if version.to_string() != value {
            return Err("noncanonical release version");
        }
        Ok(version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn full_release_identity_is_exact() {
        assert_eq!(
            CURRENT_PROTOCOL_VERSION.to_string(),
            undercurrent_foundation::RELEASE_VERSION
        );
        assert_eq!(
            serde_json::to_value(CURRENT_PROTOCOL_VERSION).unwrap(),
            undercurrent_foundation::RELEASE_VERSION
        );
        assert!(CURRENT_PROTOCOL_VERSION.can_read(CURRENT_PROTOCOL_VERSION));
        for other in ["0.1.0", "0.1.1-beta", "99.0.0"] {
            let other = ProtocolVersion::try_from(other.to_owned()).unwrap();
            assert!(!CURRENT_PROTOCOL_VERSION.can_read(other));
            assert!(!other.can_read(other));
        }
        assert!(serde_json::from_str::<ProtocolVersion>(r#"{"major":0,"minor":1}"#).is_err());
    }
}
