// SPDX-License-Identifier: Apache-2.0

//! Authoritative managed-process lifecycle state machine.

use std::fmt::{Display, Formatter};

use crate::{DomainError, DomainResult};

/// Coarse lifecycle kind used by filters without discarding an exit code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LifecycleKind {
    /// Process creation is in progress.
    Starting,
    /// The managed process is alive.
    Running,
    /// Graceful or forced termination is in progress.
    Stopping,
    /// The durable session exists without a running process.
    Stopped,
    /// The process exited and an exit code is known.
    Exited,
    /// Process creation or supervision failed.
    Failed,
}

/// Authoritative lifecycle of a managed process session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LifecycleState {
    /// Process creation is in progress.
    Starting,
    /// The managed process is alive.
    Running,
    /// Graceful or forced termination is in progress.
    Stopping,
    /// The durable session exists without a running process.
    Stopped,
    /// The process exited with the contained operating-system exit code.
    Exited(i32),
    /// Process creation or supervision failed.
    Failed,
}

impl LifecycleState {
    /// Returns the coarse lifecycle kind used for filtering and aggregation.
    ///
    /// This function performs no IO and does not panic.
    pub const fn kind(self) -> LifecycleKind {
        match self {
            Self::Starting => LifecycleKind::Starting,
            Self::Running => LifecycleKind::Running,
            Self::Stopping => LifecycleKind::Stopping,
            Self::Stopped => LifecycleKind::Stopped,
            Self::Exited(_) => LifecycleKind::Exited,
            Self::Failed => LifecycleKind::Failed,
        }
    }

    /// Returns whether the lifecycle represents a live or transitioning process.
    ///
    /// This function performs no IO and does not panic.
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Starting | Self::Running | Self::Stopping)
    }

    /// Returns whether no live process is expected for this session.
    ///
    /// This function performs no IO and does not panic.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Stopped | Self::Exited(_) | Self::Failed)
    }

    /// Returns whether the state machine permits `next` from this lifecycle.
    ///
    /// Repeating the same lifecycle is accepted as an idempotent no-op. The
    /// function performs no IO and does not panic.
    pub const fn can_transition_to(self, next: Self) -> bool {
        if lifecycle_equal(self, next) {
            return true;
        }
        matches!(
            (self, next),
            (
                Self::Starting,
                Self::Running | Self::Stopping | Self::Stopped | Self::Exited(_) | Self::Failed
            ) | (
                Self::Running,
                Self::Stopping | Self::Exited(_) | Self::Failed
            ) | (
                Self::Stopping,
                Self::Stopped | Self::Exited(_) | Self::Failed
            ) | (
                Self::Stopped | Self::Exited(_) | Self::Failed,
                Self::Starting
            ) | (Self::Failed, Self::Stopped)
        )
    }

    /// Validates a requested transition without mutating a session.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::InvalidLifecycleTransition`] when the transition
    /// violates the state machine. It performs no IO and does not panic.
    pub fn validate_transition(self, next: Self) -> DomainResult<()> {
        if self.can_transition_to(next) {
            Ok(())
        } else {
            Err(DomainError::InvalidLifecycleTransition {
                from: self,
                to: next,
            })
        }
    }
}

const fn lifecycle_equal(left: LifecycleState, right: LifecycleState) -> bool {
    match (left, right) {
        (LifecycleState::Exited(left_code), LifecycleState::Exited(right_code)) => {
            left_code == right_code
        }
        (LifecycleState::Starting, LifecycleState::Starting)
        | (LifecycleState::Running, LifecycleState::Running)
        | (LifecycleState::Stopping, LifecycleState::Stopping)
        | (LifecycleState::Stopped, LifecycleState::Stopped)
        | (LifecycleState::Failed, LifecycleState::Failed) => true,
        _ => false,
    }
}

impl Display for LifecycleKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::Exited => "exited",
            Self::Failed => "failed",
        })
    }
}

impl Display for LifecycleState {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exited(code) => write!(formatter, "exited({code})"),
            state => Display::fmt(&state.kind(), formatter),
        }
    }
}
