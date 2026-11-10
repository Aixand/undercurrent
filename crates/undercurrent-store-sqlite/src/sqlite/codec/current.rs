// SPDX-License-Identifier: Apache-2.0

//! Sole durable 0.1.0-beta session aggregate envelope.
//!
//! Profile origin and binding mode wrap the complete current state record.
//! Decoding checks the byte bound before JSON parsing and requires both header
//! and full-envelope validity. Encoding checks size after serialization, so the
//! configured limit is not an encoding-allocation ceiling.

use serde::{Deserialize, Serialize};
use undercurrent_domain::{ProfileBindingMode, ProfileSelectionOrigin, Session};

use super::aggregate::SessionCodecError;
use super::state::SessionStateRecord;

pub(crate) const SESSION_RECORD_VERSION: u32 = 1;

/// Encodes durable profile-binding policy without persisting effective runtime state.
pub(super) fn encode_session(
    session: &Session,
    max_record_bytes: usize,
) -> Result<Vec<u8>, SessionCodecError> {
    let configuration = session.configuration();
    let record = Envelope {
        schema_version: undercurrent_foundation::ReleaseVersion::Current,
        profile: ProfileBindingRecord {
            origin: configuration.profile_origin().map(Into::into),
            mode: configuration.profile_binding().into(),
        },
        state: super::state::encode_state(session)?,
    };
    let bytes = serde_json::to_vec(&record)?;
    validate_record_size(bytes.len(), max_record_bytes)?;
    Ok(bytes)
}

/// Decodes a 0.1.0-beta envelope and revalidates the complete domain aggregate.
pub(super) fn decode_session(
    bytes: &[u8],
    max_record_bytes: usize,
) -> Result<Session, SessionCodecError> {
    validate_record_size(bytes.len(), max_record_bytes)?;
    // Read the version before decoding version-specific fields so a foreign
    // envelope is reported as unsupported, even when its state shape differs.
    // See [decode order](../../../docs/reference/storage-format.md#encoding-and-decoding-order).
    let header: RecordHeader = serde_json::from_slice(bytes)?;
    if header.schema_version != undercurrent_foundation::ReleaseVersion::Current {
        return Err(SessionCodecError::UnsupportedVersion(
            SESSION_RECORD_VERSION,
        ));
    }
    let record: Envelope = serde_json::from_slice(bytes)?;
    let session = super::state::decode_state(record.state)?;
    let mut state = session.state();
    state.configuration.restore_profile_binding(
        record.profile.origin.map(Into::into),
        record.profile.mode.into(),
    )?;
    Session::rehydrate(state).map_err(Into::into)
}

fn validate_record_size(actual: usize, limit: usize) -> Result<(), SessionCodecError> {
    if limit == 0 || actual > limit {
        return Err(SessionCodecError::RecordTooLarge { actual, limit });
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    schema_version: undercurrent_foundation::ReleaseVersion,
    profile: ProfileBindingRecord,
    state: SessionStateRecord,
}

#[derive(Debug, Deserialize)]
struct RecordHeader {
    schema_version: undercurrent_foundation::ReleaseVersion,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileBindingRecord {
    origin: Option<ProfileSelectionOriginRecord>,
    mode: ProfileBindingModeRecord,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProfileSelectionOriginRecord {
    Explicit,
    LaunchCommand,
}

impl From<ProfileSelectionOrigin> for ProfileSelectionOriginRecord {
    fn from(value: ProfileSelectionOrigin) -> Self {
        match value {
            ProfileSelectionOrigin::Explicit => Self::Explicit,
            ProfileSelectionOrigin::LaunchCommand => Self::LaunchCommand,
        }
    }
}

impl From<ProfileSelectionOriginRecord> for ProfileSelectionOrigin {
    fn from(value: ProfileSelectionOriginRecord) -> Self {
        match value {
            ProfileSelectionOriginRecord::Explicit => Self::Explicit,
            ProfileSelectionOriginRecord::LaunchCommand => Self::LaunchCommand,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProfileBindingModeRecord {
    Fixed,
    ForegroundAdaptive,
}

impl From<ProfileBindingMode> for ProfileBindingModeRecord {
    fn from(value: ProfileBindingMode) -> Self {
        match value {
            ProfileBindingMode::Fixed => Self::Fixed,
            ProfileBindingMode::ForegroundAdaptive => Self::ForegroundAdaptive,
        }
    }
}

impl From<ProfileBindingModeRecord> for ProfileBindingMode {
    fn from(value: ProfileBindingModeRecord) -> Self {
        match value {
            ProfileBindingModeRecord::Fixed => Self::Fixed,
            ProfileBindingModeRecord::ForegroundAdaptive => Self::ForegroundAdaptive,
        }
    }
}
