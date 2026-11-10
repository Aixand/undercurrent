// SPDX-License-Identifier: Apache-2.0

//! Monotonic revisions used to reject stale domain updates.

use crate::{DomainError, DomainResult};

/// Monotonic revision of a durable domain entity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Revision(u64);

impl Revision {
    /// Initial revision before any durable change has been recorded.
    pub const INITIAL: Self = Self(0);

    /// Constructs a revision from a persisted or protocol value.
    ///
    /// This function performs no IO, cannot fail, and does not panic.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric revision.
    ///
    /// This function performs no IO and does not panic.
    pub const fn value(self) -> u64 {
        self.0
    }

    /// Returns the next revision without mutating the current value.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::RevisionExhausted`] at `u64::MAX`.
    /// This function performs no IO and does not panic.
    pub fn next(self, scope: &'static str) -> DomainResult<Self> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(DomainError::RevisionExhausted { scope })
    }
}

/// Monotonic revision of the session catalog and query membership.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CatalogRevision(Revision);

impl CatalogRevision {
    /// Initial catalog revision before the first catalog change.
    pub const INITIAL: Self = Self(Revision::INITIAL);

    /// Constructs a catalog revision from a persisted or protocol value.
    ///
    /// This function performs no IO, cannot fail, and does not panic.
    pub const fn new(value: u64) -> Self {
        Self(Revision::new(value))
    }

    /// Returns the numeric catalog revision.
    ///
    /// This function performs no IO and does not panic.
    pub const fn value(self) -> u64 {
        self.0.value()
    }

    /// Returns the next catalog revision.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::RevisionExhausted`] at `u64::MAX`.
    /// This function performs no IO and does not panic.
    pub fn next(self) -> DomainResult<Self> {
        self.0.next("catalog").map(Self)
    }
}
