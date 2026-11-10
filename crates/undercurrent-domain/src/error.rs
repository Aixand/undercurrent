// SPDX-License-Identifier: Apache-2.0

//! Typed domain failures and their stable categories.

use std::error::Error;
use std::fmt::{Display, Formatter};

use crate::LifecycleState;

/// Stable categories callers use to decide how a domain rejection is handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCategory {
    /// A supplied value violates a domain constraint.
    InvalidArgument,
    /// The requested domain entity does not exist.
    NotFound,
    /// The request conflicts with current domain state.
    Conflict,
    /// The request is valid but cannot run in the current state.
    FailedPrecondition,
    /// A configured domain capacity would be exceeded.
    CapacityExceeded,
    /// An internal monotonic value or invariant cannot progress safely.
    InvariantViolation,
}

impl ErrorCategory {
    /// Complete domain rejection-category registry in stable declaration order.
    pub const ALL: [Self; 6] = [
        Self::InvalidArgument,
        Self::NotFound,
        Self::Conflict,
        Self::FailedPrecondition,
        Self::CapacityExceeded,
        Self::InvariantViolation,
    ];

    /// Returns the stable snake-case representation used by outer translators.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidArgument => "invalid_argument",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::FailedPrecondition => "failed_precondition",
            Self::CapacityExceeded => "capacity_exceeded",
            Self::InvariantViolation => "invariant_violation",
        }
    }
}

/// Structured reasons a value can fail domain validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvalidValueReason {
    /// A required value was empty.
    Empty,
    /// A value exceeded its maximum byte length.
    TooLong {
        /// Maximum accepted byte length.
        max: usize,
    },
    /// A value contained a character forbidden by its contract.
    InvalidCharacter {
        /// Byte index of the rejected character.
        index: usize,
        /// Rejected character.
        character: char,
    },
    /// A human-facing name could be interpreted as a filesystem path.
    PathLike,
    /// A numeric value fell outside its inclusive range.
    OutOfRange {
        /// Inclusive lower bound.
        min: u64,
        /// Inclusive upper bound.
        max: u64,
    },
    /// Two otherwise valid fields form an invalid combination.
    InvalidCombination {
        /// Stable, non-sensitive explanation of the violated invariant.
        rule: &'static str,
    },
}

/// A deterministic rejection produced by domain validation or state changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainError {
    /// A named input field contains an invalid value.
    InvalidValue {
        /// Stable field name safe for diagnostics.
        field: &'static str,
        /// Structured validation failure.
        reason: InvalidValueReason,
    },
    /// A lifecycle transition is not permitted by the session state machine.
    InvalidLifecycleTransition {
        /// Current lifecycle.
        from: LifecycleState,
        /// Requested lifecycle.
        to: LifecycleState,
    },
    /// An operation requires a different current lifecycle.
    LifecyclePrecondition {
        /// Stable operation name safe for diagnostics.
        operation: &'static str,
        /// Lifecycle that rejected the operation.
        lifecycle: LifecycleState,
    },
    /// A monotonic revision reached its maximum value.
    RevisionExhausted {
        /// Stable name of the entity whose revision was exhausted.
        scope: &'static str,
    },
    /// Persisted or internally reconstructed state violates an aggregate invariant.
    CorruptState {
        /// Stable state boundary safe for diagnostics.
        scope: &'static str,
        /// Stable, payload-free violated invariant.
        rule: &'static str,
    },
    /// A WorkContext item was requested but is not present.
    WorkItemNotFound,
    /// A configured collection limit would be exceeded.
    CapacityExceeded {
        /// Stable resource name safe for diagnostics.
        resource: &'static str,
        /// Configured hard limit.
        limit: usize,
    },
}

impl DomainError {
    /// Returns the stable category outer layers use for error translation.
    ///
    /// This function performs no IO, cannot fail, and does not panic.
    pub const fn category(&self) -> ErrorCategory {
        match self {
            Self::InvalidValue { .. } => ErrorCategory::InvalidArgument,
            Self::InvalidLifecycleTransition { .. } | Self::LifecyclePrecondition { .. } => {
                ErrorCategory::FailedPrecondition
            }
            Self::RevisionExhausted { .. } | Self::CorruptState { .. } => {
                ErrorCategory::InvariantViolation
            }
            Self::WorkItemNotFound => ErrorCategory::NotFound,
            Self::CapacityExceeded { .. } => ErrorCategory::CapacityExceeded,
        }
    }
}

impl Display for InvalidValueReason {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => formatter.write_str("must not be empty"),
            Self::TooLong { max } => write!(formatter, "must not exceed {max} bytes"),
            Self::InvalidCharacter { index, character } => write!(
                formatter,
                "contains invalid character {character:?} at byte {index}"
            ),
            Self::PathLike => formatter.write_str("must not contain a path or traversal"),
            Self::OutOfRange { min, max } => {
                write!(formatter, "must be between {min} and {max}, inclusive")
            }
            Self::InvalidCombination { rule } => formatter.write_str(rule),
        }
    }
}

impl Display for DomainError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidValue { field, reason } => {
                write!(formatter, "invalid {field}: {reason}")
            }
            Self::InvalidLifecycleTransition { from, to } => {
                write!(formatter, "cannot transition lifecycle from {from} to {to}")
            }
            Self::LifecyclePrecondition {
                operation,
                lifecycle,
            } => write!(
                formatter,
                "cannot perform {operation} while lifecycle is {lifecycle}"
            ),
            Self::RevisionExhausted { scope } => {
                write!(formatter, "{scope} revision is exhausted")
            }
            Self::CorruptState { scope, rule } => {
                write!(formatter, "corrupt {scope} state: {rule}")
            }
            Self::WorkItemNotFound => formatter.write_str("work context item was not found"),
            Self::CapacityExceeded { resource, limit } => {
                write!(formatter, "{resource} exceeds configured limit of {limit}")
            }
        }
    }
}

impl Error for DomainError {}

/// Result alias for pure domain operations.
pub type DomainResult<T> = Result<T, DomainError>;
