// SPDX-License-Identifier: Apache-2.0

//! Stable checkpoints for transactionally recorded session events.

use crate::SessionOutboxEvent;

/// Monotonic repository-local position of one transactional outbox record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OutboxSequence(u64);

impl OutboxSequence {
    /// First valid outbox position in a new repository.
    pub const FIRST: Self = Self(1);

    /// Restores a non-zero position from persistence or a test fixture.
    ///
    /// This function performs no IO and does not panic.
    pub const fn new(value: u64) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    /// Returns the persisted numeric position.
    pub const fn value(self) -> u64 {
        self.0
    }

    /// Returns the next position, or `None` after `u64::MAX`.
    ///
    /// This function performs no IO and does not panic.
    pub const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the preceding non-zero position, or `None` at the first record.
    ///
    /// This function performs no IO and does not panic.
    pub const fn checked_previous(self) -> Option<Self> {
        match self.0.checked_sub(1) {
            Some(0) | None => None,
            Some(value) => Some(Self(value)),
        }
    }
}

/// One ordered durable domain event read from a transactional outbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionOutboxRecord {
    /// Event-time aggregate captured in the same transaction; absent for legacy events.
    pub snapshot: Option<undercurrent_domain::Session>,
    /// Stable repository-local acknowledgement checkpoint.
    pub sequence: OutboxSequence,
    /// Domain event committed atomically with its owning aggregate revision.
    pub event: SessionOutboxEvent,
}

/// Result of acknowledging one delivered contiguous outbox prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxAcknowledgeOutcome {
    /// The requested record and every older pending record were removed.
    Acknowledged {
        /// Exact number of records removed.
        records: usize,
        /// New durable acknowledgement checkpoint.
        through: OutboxSequence,
    },
    /// The requested checkpoint was already acknowledged by an earlier retry.
    AlreadyAcknowledged {
        /// Previously durable acknowledgement checkpoint.
        through: OutboxSequence,
    },
    /// The requested checkpoint was neither pending nor previously delivered.
    UnknownSequence {
        /// Unrecognized checkpoint supplied by the caller.
        requested: OutboxSequence,
    },
}

/// Result of idempotently publishing one transactional outbox record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEventPublishOutcome {
    /// This exact record was published for the first time.
    Published,
    /// This exact sequence and event were already published.
    AlreadyPublished,
}
