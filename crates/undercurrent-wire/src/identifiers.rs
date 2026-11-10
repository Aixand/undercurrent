// SPDX-License-Identifier: Apache-2.0

//! Typed identifiers used by protocol envelopes, events, traces, and resources.
//!
//! All identifier types share a byte limit and reject ASCII controls and
//! Unicode whitespace. They do not enforce UUID/hex syntax, uniqueness,
//! existence, authority, or idempotency. Parse errors retain rejected values
//! and their derived `Debug` output is not redacted.

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as DeError};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

/// Maximum number of bytes allowed in a protocol identifier.
pub const MAX_IDENTIFIER_LEN: usize = 256;

/// Explains why a protocol identifier could not be accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentifierParseErrorReason {
    /// The supplied identifier was empty.
    Empty,
    /// The supplied identifier exceeded the maximum length.
    TooLong {
        /// Maximum accepted byte length.
        max: usize,
    },
    /// The supplied identifier contained a character that is unsafe for the wire contract.
    InvalidCharacter {
        /// The rejected character.
        character: char,
        /// The byte index where the rejected character starts.
        index: usize,
    },
}

/// Error returned when a protocol identifier fails validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentifierParseError {
    value: String,
    reason: IdentifierParseErrorReason,
}

impl IdentifierParseError {
    /// Creates an identifier parse error.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn new(value: impl Into<String>, reason: IdentifierParseErrorReason) -> Self {
        Self {
            value: value.into(),
            reason,
        }
    }

    /// Returns the rejected identifier value.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Returns the validation failure reason.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn reason(&self) -> &IdentifierParseErrorReason {
        &self.reason
    }
}

impl Display for IdentifierParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.reason {
            IdentifierParseErrorReason::Empty => write!(f, "protocol identifier is empty"),
            IdentifierParseErrorReason::TooLong { max } => {
                write!(f, "protocol identifier exceeds {max} bytes")
            }
            IdentifierParseErrorReason::InvalidCharacter { character, index } => write!(
                f,
                "protocol identifier contains invalid character {character:?} at byte {index}"
            ),
        }
    }
}

impl Error for IdentifierParseError {}

macro_rules! define_identifier {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            /// Creates a typed protocol identifier after validating the wire-safe format.
            ///
            /// # Errors
            ///
            /// Returns [`IdentifierParseError`] when the identifier is empty, too long,
            /// or contains Unicode whitespace or ASCII control characters.
            ///
            /// # Panics
            ///
            /// Does not panic.
            pub fn new(value: impl Into<String>) -> Result<Self, IdentifierParseError> {
                let value = value.into();
                validate_identifier(&value)?;
                Ok(Self(value))
            }

            /// Returns the identifier as a borrowed string.
            ///
            /// # Panics
            ///
            /// Does not panic.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consumes the typed identifier and returns the owned string.
            ///
            /// # Panics
            ///
            /// Does not panic.
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdentifierParseError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(D::Error::custom)
            }
        }
    };
}

define_identifier!(
    /// Unique identifier for a protocol message envelope.
    MessageId
);
define_identifier!(
    /// Idempotency label whose scope, retention, and retry semantics belong to callers.
    IdempotencyKey
);
define_identifier!(
    /// Identifier used to correlate related messages, events, traces, and diagnostics.
    CorrelationId
);
define_identifier!(
    /// Identifier that points to the message or event that caused the current item.
    CausationId
);
define_identifier!(
    /// Unique identifier for an event.
    EventId
);
define_identifier!(
    /// Identifier for a workflow, command, execution, or test run.
    RunId
);
define_identifier!(
    /// Identifier for the actor responsible for an operation.
    ActorId
);
define_identifier!(
    /// Identifier for an authority decision, grant, approval, or enforcement result.
    AuthorityId
);
define_identifier!(
    /// Identifier for a project boundary.
    ProjectId
);
define_identifier!(
    /// Identifier for a domain, protocol, or infrastructure resource.
    ResourceId
);
define_identifier!(
    /// Identifier for the source component that emitted a protocol item.
    SourceId
);
define_identifier!(
    /// Identifier for a tenant or isolation boundary.
    TenantId
);
define_identifier!(
    /// Identifier for a distributed trace.
    TraceId
);
define_identifier!(
    /// Identifier for a span inside a distributed trace.
    SpanId
);
define_identifier!(
    /// Stable identity of one physical protocol connection.
    ConnectionId
);
define_identifier!(
    /// Stable identity supplied by one logical client instance.
    ClientId
);
define_identifier!(
    /// Correlates one finite request and response within a connection epoch.
    RequestId
);
define_identifier!(
    /// Correlates one long-lived logical stream within a connection epoch.
    StreamId
);
define_identifier!(
    /// Identifies one server process epoch for replay invalidation.
    ServerEpoch
);

fn validate_identifier(value: &str) -> Result<(), IdentifierParseError> {
    if value.is_empty() {
        return Err(IdentifierParseError::new(
            value,
            IdentifierParseErrorReason::Empty,
        ));
    }

    if value.len() > MAX_IDENTIFIER_LEN {
        return Err(IdentifierParseError::new(
            value,
            IdentifierParseErrorReason::TooLong {
                max: MAX_IDENTIFIER_LEN,
            },
        ));
    }

    for (index, character) in value.char_indices() {
        if character.is_ascii_control() || character.is_whitespace() {
            return Err(IdentifierParseError::new(
                value,
                IdentifierParseErrorReason::InvalidCharacter { character, index },
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifier_accepts_wire_safe_values() {
        let id = CorrelationId::new("run:echo/import-1").expect("valid id");

        assert_eq!(id.as_str(), "run:echo/import-1");
    }

    #[test]
    fn identifier_rejects_empty_values() {
        let error = MessageId::new("").expect_err("empty id should fail");

        assert_eq!(error.reason(), &IdentifierParseErrorReason::Empty);
    }

    #[test]
    fn identifier_rejects_whitespace() {
        let error = EventId::new("event 1").expect_err("whitespace should fail");

        assert_eq!(
            error.reason(),
            &IdentifierParseErrorReason::InvalidCharacter {
                character: ' ',
                index: 5
            }
        );
    }

    #[test]
    fn identifier_deserialization_validates_input() {
        let error = serde_json::from_str::<RunId>(r#""bad run""#)
            .expect_err("invalid serialized id should fail");

        assert!(error.to_string().contains("invalid character"));
    }
}
