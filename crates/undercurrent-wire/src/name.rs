// SPDX-License-Identifier: Apache-2.0

//! Namespaced protocol names for events, metrics, fields, and diagnostic codes.

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as DeError};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

/// Maximum number of bytes allowed in a namespaced protocol name.
pub const MAX_NAME_LEN: usize = 192;

/// Explains why a namespaced protocol name could not be accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NameParseErrorReason {
    /// The supplied name was empty.
    Empty,
    /// The supplied name contained an empty dot-separated segment.
    EmptySegment,
    /// The supplied name exceeded the maximum length.
    TooLong {
        /// Maximum accepted byte length.
        max: usize,
    },
    /// The supplied name contained an uppercase ASCII character.
    NotLowercase {
        /// The rejected character.
        character: char,
        /// The byte index where the rejected character starts.
        index: usize,
    },
    /// The supplied name contained a character outside the stable protocol alphabet.
    InvalidCharacter {
        /// The rejected character.
        character: char,
        /// The byte index where the rejected character starts.
        index: usize,
    },
}

/// Error returned when a namespaced protocol name fails validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameParseError {
    value: String,
    reason: NameParseErrorReason,
}

impl NameParseError {
    /// Creates a namespaced name parse error.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn new(value: impl Into<String>, reason: NameParseErrorReason) -> Self {
        Self {
            value: value.into(),
            reason,
        }
    }

    /// Returns the rejected name value.
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
    pub fn reason(&self) -> &NameParseErrorReason {
        &self.reason
    }
}

impl Display for NameParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.reason {
            NameParseErrorReason::Empty => write!(f, "namespaced protocol name is empty"),
            NameParseErrorReason::EmptySegment => {
                write!(f, "namespaced protocol name contains an empty segment")
            }
            NameParseErrorReason::TooLong { max } => {
                write!(f, "namespaced protocol name exceeds {max} bytes")
            }
            NameParseErrorReason::NotLowercase { character, index } => write!(
                f,
                "namespaced protocol name contains uppercase character {character:?} at byte {index}"
            ),
            NameParseErrorReason::InvalidCharacter { character, index } => write!(
                f,
                "namespaced protocol name contains invalid character {character:?} at byte {index}"
            ),
        }
    }
}

impl Error for NameParseError {}

/// Dot-separated, lowercase name used for protocol-visible concepts.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NamespacedName(String);

impl NamespacedName {
    /// Creates a namespaced protocol name after validating the wire format.
    ///
    /// Valid names use lowercase ASCII letters, digits, underscores, hyphens,
    /// and dots. Dots separate non-empty segments.
    ///
    /// # Errors
    ///
    /// Returns [`NameParseError`] when the name is empty, too long, contains an
    /// empty segment, contains uppercase letters, or uses unsupported
    /// characters.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn new(value: impl Into<String>) -> Result<Self, NameParseError> {
        let value = value.into();
        validate_name(&value)?;
        Ok(Self(value))
    }

    /// Returns the namespaced name as a borrowed string.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the name and returns the owned string.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl Display for NamespacedName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for NamespacedName {
    type Err = NameParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl Serialize for NamespacedName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for NamespacedName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

fn validate_name(value: &str) -> Result<(), NameParseError> {
    if value.is_empty() {
        return Err(NameParseError::new(value, NameParseErrorReason::Empty));
    }

    if value.len() > MAX_NAME_LEN {
        return Err(NameParseError::new(
            value,
            NameParseErrorReason::TooLong { max: MAX_NAME_LEN },
        ));
    }

    if value.split('.').any(str::is_empty) {
        return Err(NameParseError::new(
            value,
            NameParseErrorReason::EmptySegment,
        ));
    }

    for (index, character) in value.char_indices() {
        if character.is_ascii_uppercase() {
            return Err(NameParseError::new(
                value,
                NameParseErrorReason::NotLowercase { character, index },
            ));
        }

        if !(character.is_ascii_lowercase()
            || character.is_ascii_digit()
            || matches!(character, '.' | '_' | '-'))
        {
            return Err(NameParseError::new(
                value,
                NameParseErrorReason::InvalidCharacter { character, index },
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespaced_name_accepts_stable_protocol_names() {
        let name = NamespacedName::new("intent.echo.import_started").expect("valid name");

        assert_eq!(name.as_str(), "intent.echo.import_started");
    }

    #[test]
    fn namespaced_name_rejects_empty_segments() {
        let error = NamespacedName::new("intent..echo").expect_err("empty segment should fail");

        assert_eq!(error.reason(), &NameParseErrorReason::EmptySegment);
    }

    #[test]
    fn namespaced_name_rejects_uppercase() {
        let error = NamespacedName::new("Intent.echo").expect_err("uppercase should fail");

        assert_eq!(
            error.reason(),
            &NameParseErrorReason::NotLowercase {
                character: 'I',
                index: 0
            }
        );
    }

    #[test]
    fn namespaced_name_deserialization_validates_input() {
        let error = serde_json::from_str::<NamespacedName>(r#""bad name""#)
            .expect_err("invalid serialized name should fail");

        assert!(error.to_string().contains("invalid character"));
    }
}
