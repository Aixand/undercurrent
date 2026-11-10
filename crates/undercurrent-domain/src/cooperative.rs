// SPDX-License-Identifier: Apache-2.0

//! Stable cooperative foreground contract shared by shell and PTY adapters.

use crate::{ExecutionState, InteractionModality, InteractionState};

/// Registered OSC number for Undercurrent cooperative foreground signals.
pub const COOPERATIVE_OSC_NUMBER: u16 = 9091;

/// Exact byte prefix beginning one cooperative OSC record.
pub const COOPERATIVE_OSC_PREFIX: &[u8] = b"\x1b]9091;";

/// Stable environment variables owned by cooperative runtime integration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CooperativeEnvironmentVariable {
    /// Exact managed-session identifier injected into supervised processes.
    Session,
}

impl CooperativeEnvironmentVariable {
    /// Complete cooperative environment-variable registry.
    pub const ALL: [Self; 1] = [Self::Session];

    /// Returns the stable process environment name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Session => "UC_SESSION",
        }
    }
}

/// Payload-free cooperative foreground states accepted from managed shells.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CooperativeForegroundState {
    /// The shell is ready to receive a command.
    Ready,
    /// The shell is executing a command.
    Running,
}

impl CooperativeForegroundState {
    /// Complete state registry accepted from the private OSC contract.
    pub const ALL: [Self; 2] = [Self::Ready, Self::Running];

    /// Returns the stable wire value after the `uc=` key.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Running => "running",
        }
    }

    /// Returns the execution state claimed by this cooperative signal.
    pub const fn execution(self) -> ExecutionState {
        match self {
            Self::Ready => ExecutionState::Quiescent,
            Self::Running => ExecutionState::Active,
        }
    }

    /// Returns the interaction availability claimed by this signal.
    pub const fn interaction(self) -> InteractionState {
        match self {
            Self::Ready => InteractionState::Available,
            Self::Running => InteractionState::None,
        }
    }

    /// Returns the mechanical input modality claimed by this signal.
    pub const fn modality(self) -> Option<InteractionModality> {
        match self {
            Self::Ready => Some(InteractionModality::Text),
            Self::Running => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cooperative_registries_are_complete_and_consistent() {
        assert_eq!(COOPERATIVE_OSC_NUMBER, 9091);
        assert!(COOPERATIVE_OSC_PREFIX.ends_with(b"9091;"));
        assert_eq!(CooperativeEnvironmentVariable::ALL.len(), 1);
        assert_eq!(CooperativeForegroundState::ALL.len(), 2);
        assert_eq!(
            CooperativeForegroundState::Ready.execution(),
            ExecutionState::Quiescent
        );
        assert_eq!(
            CooperativeForegroundState::Ready.interaction(),
            InteractionState::Available
        );
        assert_eq!(
            CooperativeForegroundState::Running.execution(),
            ExecutionState::Active
        );
    }
}
