// SPDX-License-Identifier: Apache-2.0

//! Validated identities and names owned by the Undercurrent domain.

use std::fmt::{Display, Formatter};
use std::str::FromStr;

use crate::{DomainError, DomainResult, InvalidValueReason, TimestampMillis};

const MAX_IDENTIFIER_BYTES: usize = 256;
const MAX_SESSION_NAME_BYTES: usize = 128;

fn validate_identifier(field: &'static str, value: &str) -> DomainResult<()> {
    if value.is_empty() {
        return Err(DomainError::InvalidValue {
            field,
            reason: InvalidValueReason::Empty,
        });
    }
    if value.len() > MAX_IDENTIFIER_BYTES {
        return Err(DomainError::InvalidValue {
            field,
            reason: InvalidValueReason::TooLong {
                max: MAX_IDENTIFIER_BYTES,
            },
        });
    }
    if let Some((index, character)) = value
        .char_indices()
        .find(|(_, character)| character.is_control() || character.is_whitespace())
    {
        return Err(DomainError::InvalidValue {
            field,
            reason: InvalidValueReason::InvalidCharacter { index, character },
        });
    }
    Ok(())
}

macro_rules! define_identifier {
    ($(#[$meta:meta])* $name:ident, $field:literal) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            /// Validates and constructs the identifier.
            ///
            /// Returns a typed validation error for empty, oversized, whitespace,
            /// or control-character input. It performs no IO and does not panic.
            pub fn new(value: impl Into<String>) -> DomainResult<Self> {
                let value = value.into();
                validate_identifier($field, &value)?;
                Ok(Self(value))
            }

            /// Returns the validated identifier as a borrowed string.
            ///
            /// This function performs no IO and does not panic.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consumes the identifier and returns its owned string.
            ///
            /// This function performs no IO and does not panic.
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = DomainError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }
    };
}

define_identifier!(
    /// Stable product-specific identifier for a managed session.
    SessionId,
    "session_id"
);
define_identifier!(
    /// Stable identifier assigned to a declared WorkContext item.
    WorkItemId,
    "work_item_id"
);
define_identifier!(
    /// Stable identifier for the system that declared a WorkContext item.
    WorkOwner,
    "work_owner"
);
define_identifier!(
    /// Stable name of a foreground evidence source.
    EvidenceSource,
    "evidence_source"
);
define_identifier!(
    /// Stable name of a detection profile.
    ProfileName,
    "profile_name"
);
define_identifier!(
    /// Stable identifier for one semantic input delivery.
    DeliveryId,
    "delivery_id"
);
define_identifier!(
    /// Opaque identity of one concrete supervised process instance.
    ProcessInstanceId,
    "process_instance_id"
);

/// Durable identity of one concrete process owned by a session.
///
/// The opaque instance ID must identify process incarnation, not only a reused
/// operating-system PID. The adapter-supplied start time supports uptime and
/// startup reconciliation without embedding platform process types in domain
/// state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessIdentity {
    id: ProcessInstanceId,
    started_at: TimestampMillis,
}

impl ProcessIdentity {
    /// Constructs durable identity from an already validated opaque ID and the
    /// time the adapter established the process as started.
    ///
    /// This function performs no IO, cannot fail, and does not panic.
    pub const fn new(id: ProcessInstanceId, started_at: TimestampMillis) -> Self {
        Self { id, started_at }
    }

    /// Returns the opaque process-incarnation identity.
    pub const fn id(&self) -> &ProcessInstanceId {
        &self.id
    }

    /// Returns the adapter-observed process start time.
    pub const fn started_at(&self) -> TimestampMillis {
        self.started_at
    }
}

/// Human-facing selector with bounded portable syntax.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionName(String);

impl SessionName {
    /// Validates and constructs a session name.
    ///
    /// Names are nonempty and bounded, without control characters, surrounding
    /// whitespace, path separators, literal dot, or a leading double-dot.
    /// This does not establish platform filename validity or authorize filesystem
    /// use. The function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::InvalidValue`] when any invariant is violated.
    pub fn new(value: impl Into<String>) -> DomainResult<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(DomainError::InvalidValue {
                field: "session_name",
                reason: InvalidValueReason::Empty,
            });
        }
        if value.len() > MAX_SESSION_NAME_BYTES {
            return Err(DomainError::InvalidValue {
                field: "session_name",
                reason: InvalidValueReason::TooLong {
                    max: MAX_SESSION_NAME_BYTES,
                },
            });
        }
        if value != value.trim()
            || value.contains('/')
            || value.contains('\\')
            || value == "."
            || value.starts_with("..")
        {
            return Err(DomainError::InvalidValue {
                field: "session_name",
                reason: InvalidValueReason::PathLike,
            });
        }
        if let Some((index, character)) = value
            .char_indices()
            .find(|(_, character)| character.is_control())
        {
            return Err(DomainError::InvalidValue {
                field: "session_name",
                reason: InvalidValueReason::InvalidCharacter { index, character },
            });
        }
        Ok(Self(value))
    }

    /// Returns the validated name as a borrowed string.
    ///
    /// This function performs no IO and does not panic.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the name and returns its owned string.
    ///
    /// This function performs no IO and does not panic.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl Display for SessionName {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for SessionName {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}
