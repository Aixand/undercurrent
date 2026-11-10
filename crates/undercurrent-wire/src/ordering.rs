// SPDX-License-Identifier: Apache-2.0

//! Monotonic ordering and reusable pagination primitives.
//!
//! Counters advance without wrapping but do not validate a stream's history or
//! persist commits. Pagination scalars validate representation; product owners
//! enforce page limits after deserialization and authenticate cursor content.

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as DeError};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

/// Maximum bytes accepted in a generic opaque page cursor.
pub const MAX_OPAQUE_CURSOR_BYTES: usize = 4_096;

/// Error returned when a monotonic protocol counter cannot advance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CounterExhausted {
    counter: &'static str,
}

impl CounterExhausted {
    /// Returns the stable counter name.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub const fn counter(self) -> &'static str {
        self.counter
    }
}

impl Display for CounterExhausted {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{} counter is exhausted", self.counter)
    }
}

impl Error for CounterExhausted {}

macro_rules! define_counter {
    ($(#[$meta:meta])* $name:ident, $counter:literal) => {
        $(#[$meta])*
        #[derive(
            Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize,
            Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(u64);

        impl $name {
            /// Initial value before the first ordered item is committed.
            pub const INITIAL: Self = Self(0);

            /// Reconstructs a counter from a validated wire or persistence value.
            ///
            /// # Panics
            ///
            /// Does not panic.
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            /// Returns the numeric counter value.
            ///
            /// # Panics
            ///
            /// Does not panic.
            pub const fn value(self) -> u64 {
                self.0
            }

            /// Returns the next value without mutating this counter.
            ///
            /// # Errors
            ///
            /// Returns [`CounterExhausted`] instead of wrapping at `u64::MAX`.
            ///
            /// # Panics
            ///
            /// Does not panic.
            pub const fn next(self) -> Result<Self, CounterExhausted> {
                match self.0.checked_add(1) {
                    Some(value) => Ok(Self(value)),
                    None => Err(CounterExhausted { counter: $counter }),
                }
            }
        }
    };
}

define_counter!(
    /// Monotonic revision of a durable or prepared protocol entity.
    Revision,
    "revision"
);
define_counter!(
    /// Monotonic sequence within one logical protocol stream.
    StreamSequence,
    "stream_sequence"
);

/// Error returned by reusable pagination value validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaginationValueError {
    /// Page size or configured maximum was zero.
    ZeroPageSize,
    /// Page size exceeded the caller-supplied product limit.
    PageSizeExceeded {
        /// Requested page size.
        requested: u32,
        /// Inclusive configured maximum.
        maximum: u32,
    },
    /// Opaque cursor was empty.
    EmptyCursor,
    /// Opaque cursor exceeded the Undercurrent wire limit.
    CursorTooLong {
        /// Observed byte length.
        actual: usize,
        /// Inclusive maximum byte length.
        maximum: usize,
    },
    /// Opaque cursor contained a control character.
    CursorControlCharacter {
        /// Byte index of the rejected character.
        index: usize,
    },
}

impl Display for PaginationValueError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroPageSize => formatter.write_str("page size and maximum must be non-zero"),
            Self::PageSizeExceeded { requested, maximum } => {
                write!(formatter, "page size {requested} exceeds maximum {maximum}")
            }
            Self::EmptyCursor => formatter.write_str("opaque cursor must not be empty"),
            Self::CursorTooLong { actual, maximum } => write!(
                formatter,
                "opaque cursor length {actual} exceeds maximum {maximum}"
            ),
            Self::CursorControlCharacter { index } => write!(
                formatter,
                "opaque cursor contains a control character at byte {index}"
            ),
        }
    }
}

impl Error for PaginationValueError {}

/// Non-zero page size validated against a product-supplied hard maximum.
///
/// Deserialization checks against `u32::MAX`, because the product maximum is
/// not encoded in this scalar. Reapply the product limit at the owning boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct PageSize(u32);

impl PageSize {
    /// Constructs a page size within `maximum`.
    ///
    /// # Errors
    ///
    /// Returns [`PaginationValueError`] for zero values or a request above the
    /// product-supplied maximum.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub const fn new(value: u32, maximum: u32) -> Result<Self, PaginationValueError> {
        if value == 0 || maximum == 0 {
            return Err(PaginationValueError::ZeroPageSize);
        }
        if value > maximum {
            return Err(PaginationValueError::PageSizeExceeded {
                requested: value,
                maximum,
            });
        }
        Ok(Self(value))
    }

    /// Returns the non-zero page size.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl<'de> Deserialize<'de> for PageSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u32::deserialize(deserializer)?;
        Self::new(value, u32::MAX).map_err(D::Error::custom)
    }
}

/// Bounded transport-opaque cursor.
///
/// Product protocols own signing and internal cursor content. Debug output
/// reports length only so accidental diagnostics do not make the value a
/// compatibility contract.
/// `Display` and serialization expose the actual cursor value.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OpaqueCursor(String);

impl OpaqueCursor {
    /// Validates and constructs a transport cursor.
    ///
    /// # Errors
    ///
    /// Returns [`PaginationValueError`] for empty, oversized, or control-byte
    /// input. This function performs no decoding and does not panic.
    pub fn new(value: impl Into<String>) -> Result<Self, PaginationValueError> {
        let value = value.into();
        if value.is_empty() {
            return Err(PaginationValueError::EmptyCursor);
        }
        if value.len() > MAX_OPAQUE_CURSOR_BYTES {
            return Err(PaginationValueError::CursorTooLong {
                actual: value.len(),
                maximum: MAX_OPAQUE_CURSOR_BYTES,
            });
        }
        if let Some((index, _)) = value
            .char_indices()
            .find(|(_, character)| character.is_control())
        {
            return Err(PaginationValueError::CursorControlCharacter { index });
        }
        Ok(Self(value))
    }

    /// Returns the opaque wire value to an authorized codec.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the cursor and returns its opaque wire value.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl Debug for OpaqueCursor {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpaqueCursor")
            .field("byte_len", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl Display for OpaqueCursor {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for OpaqueCursor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for OpaqueCursor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_advance_and_never_wrap() {
        assert_eq!(
            Revision::INITIAL.next().expect("advances"),
            Revision::new(1)
        );
        assert_eq!(
            StreamSequence::new(u64::MAX)
                .next()
                .expect_err("must not wrap")
                .counter(),
            "stream_sequence"
        );
    }

    #[test]
    fn page_size_applies_product_limit_during_construction_and_rejects_zero_on_wire() {
        assert_eq!(PageSize::new(25, 100).expect("valid").get(), 25);
        assert!(matches!(
            PageSize::new(101, 100),
            Err(PaginationValueError::PageSizeExceeded { .. })
        ));
        assert!(serde_json::from_str::<PageSize>("0").is_err());
    }

    #[test]
    fn opaque_cursor_is_bounded_validated_and_redacted_in_debug() {
        let cursor = OpaqueCursor::new("signed.cursor.value").expect("valid cursor");
        assert_eq!(cursor.as_str(), "signed.cursor.value");
        assert!(!format!("{cursor:?}").contains("signed.cursor.value"));
        assert!(OpaqueCursor::new("line\nbreak").is_err());
        assert!(OpaqueCursor::new("x".repeat(MAX_OPAQUE_CURSOR_BYTES + 1)).is_err());
    }
}
