// SPDX-License-Identifier: Apache-2.0

//! Bounded labels and metadata used by sessions and WorkContext declarations.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};

use crate::{DomainError, DomainResult, InvalidValueReason};

/// Maximum labels carried by one domain entity.
pub const MAX_LABELS: usize = 64;
/// Maximum metadata entries carried by one domain entity.
pub const MAX_METADATA_ENTRIES: usize = 64;
/// Maximum byte length of a label or metadata key.
pub const MAX_ATTRIBUTE_KEY_BYTES: usize = 64;
/// Maximum byte length of a label value.
pub const MAX_LABEL_VALUE_BYTES: usize = 256;
/// Maximum byte length of one metadata value.
pub const MAX_METADATA_VALUE_BYTES: usize = 4_096;

fn validate_text(
    field: &'static str,
    value: &str,
    max: usize,
    allow_empty: bool,
) -> DomainResult<()> {
    if !allow_empty && value.is_empty() {
        return Err(DomainError::InvalidValue {
            field,
            reason: InvalidValueReason::Empty,
        });
    }
    if value.len() > max {
        return Err(DomainError::InvalidValue {
            field,
            reason: InvalidValueReason::TooLong { max },
        });
    }
    if let Some((index, character)) = value
        .char_indices()
        .find(|(_, character)| character.is_control())
    {
        return Err(DomainError::InvalidValue {
            field,
            reason: InvalidValueReason::InvalidCharacter { index, character },
        });
    }
    Ok(())
}

/// Bounded, deterministic label map used for selection and display.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Labels(BTreeMap<String, String>);

impl Labels {
    /// Validates and constructs labels from key/value pairs.
    ///
    /// Duplicate keys use the last supplied value. Keys are non-empty and all
    /// values are bounded. The function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed validation or capacity error for invalid input.
    pub fn new(entries: impl IntoIterator<Item = (String, String)>) -> DomainResult<Self> {
        let mut labels = BTreeMap::new();
        for (key, value) in entries {
            validate_text("label_key", &key, MAX_ATTRIBUTE_KEY_BYTES, false)?;
            validate_text("label_value", &value, MAX_LABEL_VALUE_BYTES, true)?;
            labels.insert(key, value);
            if labels.len() > MAX_LABELS {
                return Err(DomainError::CapacityExceeded {
                    resource: "labels",
                    limit: MAX_LABELS,
                });
            }
        }
        Ok(Self(labels))
    }

    /// Returns the number of labels.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether no labels are present.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the value for a label key.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    /// Iterates over labels in deterministic key order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }

    /// Returns whether every label in `required` is present with the same value.
    pub fn contains_all(&self, required: &Self) -> bool {
        required
            .0
            .iter()
            .all(|(key, value)| self.0.get(key) == Some(value))
    }
}

/// Validated partial change to query-visible labels.
///
/// Unmentioned keys are preserved. Removal is applied before insertion so a
/// full label map can exchange keys without exceeding its capacity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelPatch {
    set: Labels,
    remove: BTreeSet<String>,
}

impl LabelPatch {
    /// Constructs a bounded patch without performing IO.
    ///
    /// # Errors
    ///
    /// Rejects invalid removal keys, more than 64 distinct removals, or a key
    /// present in both the set and remove portions.
    pub fn new(set: Labels, remove: impl IntoIterator<Item = String>) -> DomainResult<Self> {
        let mut keys = BTreeSet::new();
        for key in remove {
            validate_text("label_key", &key, MAX_ATTRIBUTE_KEY_BYTES, false)?;
            if set.get(&key).is_some() {
                return Err(DomainError::InvalidValue {
                    field: "label_patch",
                    reason: InvalidValueReason::InvalidCombination {
                        rule: "a label cannot be set and removed by the same patch",
                    },
                });
            }
            keys.insert(key);
            if keys.len() > MAX_LABELS {
                return Err(DomainError::CapacityExceeded {
                    resource: "label_removals",
                    limit: MAX_LABELS,
                });
            }
        }
        Ok(Self { set, remove: keys })
    }

    /// Applies a patch to a copy of the current labels.
    ///
    /// # Errors
    ///
    /// Rejects a resulting map that exceeds label capacity. The supplied map
    /// remains unchanged on both success and failure.
    pub fn apply_to(&self, current: &Labels) -> DomainResult<Labels> {
        let mut next = current.0.clone();
        for key in &self.remove {
            next.remove(key);
        }
        next.extend(self.set.0.clone());
        Labels::new(next)
    }
}

/// Bounded metadata kept out of default summary projections.
///
/// Metadata values may be private. This type intentionally implements neither
/// `Display` nor custom diagnostics that could expose them.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Metadata(BTreeMap<String, String>);

impl Metadata {
    /// Validates and constructs metadata from key/value pairs.
    ///
    /// Duplicate keys use the last supplied value. This function performs no
    /// IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed validation or capacity error for invalid input.
    pub fn new(entries: impl IntoIterator<Item = (String, String)>) -> DomainResult<Self> {
        let mut metadata = BTreeMap::new();
        for (key, value) in entries {
            validate_text("metadata_key", &key, MAX_ATTRIBUTE_KEY_BYTES, false)?;
            validate_text("metadata_value", &value, MAX_METADATA_VALUE_BYTES, true)?;
            metadata.insert(key, value);
            if metadata.len() > MAX_METADATA_ENTRIES {
                return Err(DomainError::CapacityExceeded {
                    resource: "metadata",
                    limit: MAX_METADATA_ENTRIES,
                });
            }
        }
        Ok(Self(metadata))
    }

    /// Returns the number of metadata entries without exposing their values.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether no metadata entries are present.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns a metadata value to an explicitly authorized caller.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    /// Iterates over metadata in deterministic key order.
    ///
    /// Callers are responsible for keeping these values out of logs, traces,
    /// and summary DTOs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }
}

impl Debug for Metadata {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Metadata")
            .field("entry_count", &self.0.len())
            .finish_non_exhaustive()
    }
}
