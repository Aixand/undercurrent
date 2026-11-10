// SPDX-License-Identifier: Apache-2.0

//! Central registry of domain event kinds.

/// Every durable domain change that outer layers may publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DomainEventKind {
    /// A durable session record was created.
    SessionCreated,
    /// A session's human-facing name changed.
    SessionRenamed,
    /// Labels, metadata, profile, or another durable session setting changed.
    SessionConfigurationChanged,
    /// A session was removed from the catalog.
    SessionRemoved,
    /// Authoritative lifecycle changed.
    LifecycleChanged,
    /// Durable concrete process ownership was established while starting.
    ProcessOwnershipChanged,
    /// Best-effort foreground observation changed.
    ForegroundChanged,
    /// Foreground evidence or rationale changed without a compact-state change.
    ForegroundEvidenceChanged,
    /// Health summary changed.
    HealthChanged,
    /// Detailed health results changed without a compact-status change.
    HealthEvidenceChanged,
    /// WorkContext content or aggregate status changed.
    WorkContextChanged,
    /// A restart was scheduled by policy.
    RestartScheduled,
    /// An operator explicitly cancelled a pending automatic restart.
    RestartCancelled,
    /// Persisted restart-attempt tracking changed without scheduling a restart.
    RestartStateChanged,
    /// A restart policy reached a terminal disposition.
    RestartExhausted,
    /// Terminal dimensions changed.
    TerminalResized,
}

impl DomainEventKind {
    /// Complete event registry in stable declaration order.
    pub const ALL: [Self; 16] = [
        Self::SessionCreated,
        Self::SessionRenamed,
        Self::SessionConfigurationChanged,
        Self::SessionRemoved,
        Self::LifecycleChanged,
        Self::ProcessOwnershipChanged,
        Self::ForegroundChanged,
        Self::ForegroundEvidenceChanged,
        Self::HealthChanged,
        Self::HealthEvidenceChanged,
        Self::WorkContextChanged,
        Self::RestartScheduled,
        Self::RestartCancelled,
        Self::RestartStateChanged,
        Self::RestartExhausted,
        Self::TerminalResized,
    ];

    /// Returns the stable snake-case event name used by adapters and registries.
    ///
    /// This function performs no IO and does not panic.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SessionCreated => "session_created",
            Self::SessionRenamed => "session_renamed",
            Self::SessionConfigurationChanged => "session_configuration_changed",
            Self::SessionRemoved => "session_removed",
            Self::LifecycleChanged => "lifecycle_changed",
            Self::ProcessOwnershipChanged => "process_ownership_changed",
            Self::ForegroundChanged => "foreground_changed",
            Self::ForegroundEvidenceChanged => "foreground_evidence_changed",
            Self::HealthChanged => "health_changed",
            Self::HealthEvidenceChanged => "health_evidence_changed",
            Self::WorkContextChanged => "work_context_changed",
            Self::RestartScheduled => "restart_scheduled",
            Self::RestartCancelled => "restart_cancelled",
            Self::RestartStateChanged => "restart_state_changed",
            Self::RestartExhausted => "restart_exhausted",
            Self::TerminalResized => "terminal_resized",
        }
    }

    /// Resolves one stable adapter-facing event name.
    ///
    /// Unknown names return `None`. This function performs no IO and does not
    /// panic.
    pub fn from_name(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|candidate| candidate.as_str() == value)
    }

    /// Returns whether this change invalidates the session's observation plan.
    ///
    /// Only changes to lifecycle, process ownership, or observation-relevant
    /// configuration require immediate reconciliation. Observation results and
    /// unrelated session changes must preserve the independently configured
    /// foreground and health cadences. This function performs no IO and does not
    /// panic.
    pub const fn invalidates_observation_plan(self) -> bool {
        matches!(
            self,
            Self::SessionCreated
                | Self::SessionConfigurationChanged
                | Self::LifecycleChanged
                | Self::ProcessOwnershipChanged
        )
    }
}
