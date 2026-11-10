// SPDX-License-Identifier: Apache-2.0

//! Typed application failures and stable presentation categories.

use std::error::Error;
use std::fmt::{Display, Formatter};

use undercurrent_domain::{CatalogRevision, DomainError, ErrorCategory};

/// Stable categories used by protocol and presentation adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ApplicationErrorCategory {
    /// Invalid use-case input.
    InvalidArgument,
    /// No authenticated principal is available for the action.
    Unauthenticated,
    /// The authenticated principal is not permitted to perform the action.
    PermissionDenied,
    /// Requested entity does not exist.
    NotFound,
    /// Request is valid but current state does not permit it.
    FailedPrecondition,
    /// Request conflicts with current state.
    Conflict,
    /// A configured item or byte capacity was exhausted.
    CapacityExceeded,
    /// A configured request or byte rate was exceeded.
    RateLimited,
    /// The caller explicitly cancelled the operation.
    Cancelled,
    /// An operation exceeded its enforced deadline.
    Timeout,
    /// An operation was interrupted before a normal terminal result.
    Interrupted,
    /// A required adapter or external dependency is unavailable.
    Unavailable,
    /// Requested replay is outside retained history.
    ReplayUnavailable,
    /// A page cursor no longer identifies a valid catalog snapshot.
    CursorExpired,
    /// A peer violated framing, sequencing, correlation, or negotiation rules.
    ProtocolViolation,
    /// The requested behavior is not supported by this implementation.
    Unsupported,
    /// A domain or application monotonic invariant was exhausted.
    InvariantViolation,
    /// Application infrastructure failed unexpectedly.
    Internal,
}

impl ApplicationErrorCategory {
    /// Complete stable category registry used by diagnostics and presentation.
    pub const ALL: [Self; 18] = [
        Self::InvalidArgument,
        Self::Unauthenticated,
        Self::PermissionDenied,
        Self::NotFound,
        Self::FailedPrecondition,
        Self::Conflict,
        Self::CapacityExceeded,
        Self::RateLimited,
        Self::Cancelled,
        Self::Timeout,
        Self::Interrupted,
        Self::Unavailable,
        Self::ReplayUnavailable,
        Self::CursorExpired,
        Self::ProtocolViolation,
        Self::Unsupported,
        Self::InvariantViolation,
        Self::Internal,
    ];

    /// Translates one pure domain rejection without discarding its semantics.
    pub const fn from_domain(category: ErrorCategory) -> Self {
        match category {
            ErrorCategory::InvalidArgument => Self::InvalidArgument,
            ErrorCategory::NotFound => Self::NotFound,
            ErrorCategory::Conflict => Self::Conflict,
            ErrorCategory::FailedPrecondition => Self::FailedPrecondition,
            ErrorCategory::CapacityExceeded => Self::CapacityExceeded,
            ErrorCategory::InvariantViolation => Self::InvariantViolation,
        }
    }

    /// Returns whether this category represents an access-control rejection.
    pub const fn is_access_denial(self) -> bool {
        matches!(self, Self::Unauthenticated | Self::PermissionDenied)
    }

    /// Returns the stable snake-case representation used at outer boundaries.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidArgument => "invalid_argument",
            Self::Unauthenticated => "unauthenticated",
            Self::PermissionDenied => "permission_denied",
            Self::NotFound => "not_found",
            Self::FailedPrecondition => "failed_precondition",
            Self::Conflict => "conflict",
            Self::CapacityExceeded => "capacity_exceeded",
            Self::RateLimited => "rate_limited",
            Self::Cancelled => "cancelled",
            Self::Timeout => "timeout",
            Self::Interrupted => "interrupted",
            Self::Unavailable => "unavailable",
            Self::ReplayUnavailable => "replay_unavailable",
            Self::CursorExpired => "cursor_expired",
            Self::ProtocolViolation => "protocol_violation",
            Self::Unsupported => "unsupported",
            Self::InvariantViolation => "invariant_violation",
            Self::Internal => "internal",
        }
    }
}

/// Error returned by prepared session catalog operations.
#[derive(Debug)]
pub enum CatalogError {
    /// A domain revision or validation invariant failed.
    Domain(DomainError),
    /// Cursor was issued for a catalog revision that is no longer current.
    CursorExpired {
        /// Revision carried by the cursor.
        cursor_revision: CatalogRevision,
        /// Current prepared catalog revision.
        current_revision: CatalogRevision,
    },
    /// Cursor belongs to a different query specification.
    CursorQueryMismatch,
    /// The in-memory catalog lock was poisoned by a panic while held.
    LockPoisoned,
    /// Prepared read-model invariants were internally inconsistent.
    InvariantViolation {
        /// Stable, non-sensitive invariant description.
        rule: &'static str,
    },
}

impl CatalogError {
    /// Returns the stable category outer adapters use for error envelopes.
    ///
    /// This function performs no IO and does not panic.
    pub const fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::Domain(error) => ApplicationErrorCategory::from_domain(error.category()),
            Self::CursorExpired { .. } | Self::CursorQueryMismatch => {
                ApplicationErrorCategory::CursorExpired
            }
            Self::LockPoisoned => ApplicationErrorCategory::Internal,
            Self::InvariantViolation { .. } => ApplicationErrorCategory::InvariantViolation,
        }
    }
}

impl Display for CatalogError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Domain(error) => Display::fmt(error, formatter),
            Self::CursorExpired {
                cursor_revision,
                current_revision,
            } => write!(
                formatter,
                "session cursor revision {} expired at catalog revision {}",
                cursor_revision.value(),
                current_revision.value()
            ),
            Self::CursorQueryMismatch => {
                formatter.write_str("session cursor belongs to a different query")
            }
            Self::LockPoisoned => formatter.write_str("prepared session catalog lock is poisoned"),
            Self::InvariantViolation { rule } => {
                write!(
                    formatter,
                    "prepared session catalog invariant failed: {rule}"
                )
            }
        }
    }
}

impl Error for CatalogError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Domain(error) => Some(error),
            Self::CursorExpired { .. }
            | Self::CursorQueryMismatch
            | Self::LockPoisoned
            | Self::InvariantViolation { .. } => None,
        }
    }
}

impl From<DomainError> for CatalogError {
    fn from(error: DomainError) -> Self {
        Self::Domain(error)
    }
}

/// Result alias for prepared session catalog operations.
pub type CatalogResult<T> = Result<T, CatalogError>;
