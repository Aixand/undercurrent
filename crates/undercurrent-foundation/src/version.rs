// SPDX-License-Identifier: Apache-2.0

//! The single supported Undercurrent release identity.

use serde::{Deserialize, Serialize};

/// Release identity shared by packages, schemas and protocol metadata.
pub const RELEASE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Exact release marker. Deserialization rejects every other release.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReleaseVersion {
    /// The current Undercurrent release.
    #[default]
    #[serde(rename = "0.1.0-beta")]
    Current,
}

impl std::fmt::Display for ReleaseVersion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(RELEASE_VERSION)
    }
}

impl From<ReleaseVersion> for serde_json::Value {
    fn from(_: ReleaseVersion) -> Self {
        Self::String(RELEASE_VERSION.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_marker_matches_package_and_rejects_other_versions() {
        assert_eq!(
            serde_json::to_value(ReleaseVersion::Current).unwrap(),
            RELEASE_VERSION
        );
        for other in ["0.1.0", "0.1.0-beta.1", "99.0.0", "v0.1.0-beta"] {
            assert!(serde_json::from_value::<ReleaseVersion>(serde_json::json!(other)).is_err());
        }
    }
}
