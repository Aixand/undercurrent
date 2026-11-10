// SPDX-License-Identifier: Apache-2.0

//! Bounded adapter-neutral foreground process-group observations.

use undercurrent_domain::{
    CooperativeForegroundState, PathReference, ProcessInstanceId, ProfileMatchTarget,
    TimestampMillis,
};

/// Whether every foreground member and its command identity were observed.
/// Member-local evidence such as environment-name projection reports its own
/// completeness independently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessGroupCompleteness {
    /// Every bounded foreground member and command identity was observed.
    Complete,
    /// Hard limits omitted a foreground member or its command identity.
    Truncated,
    /// The platform could not provide authoritative foreground ownership.
    Unavailable,
}

/// Bounded ownership result for foreground TCP listeners.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpListenerObservation {
    /// At least one foreground member owns a host-namespace TCP listener.
    Present,
    /// All bounded foreground file descriptors were inspected without a listener.
    Absent,
    /// Permission or platform limitations prevented inspection.
    Unavailable,
    /// File-descriptor limits prevented a complete negative result.
    Truncated,
}

/// One bounded foreground process-group member suitable for profile matching.
#[derive(Clone, PartialEq, Eq)]
pub struct ForegroundProgramMember {
    incarnation: String,
    target: ProfileMatchTarget,
    environment_names_complete: bool,
}

impl ForegroundProgramMember {
    /// Constructs a member from an adapter-owned PID-reuse-safe identity and
    /// bounded private command facts. This function performs no IO and does not panic.
    pub fn new(incarnation: impl Into<String>, target: ProfileMatchTarget) -> Self {
        Self {
            incarnation: incarnation.into(),
            target,
            environment_names_complete: true,
        }
    }

    /// Marks the bounded environment-name projection as incomplete while
    /// retaining the member's executable and argument evidence.
    pub fn with_truncated_environment_names(mut self) -> Self {
        self.environment_names_complete = false;
        self
    }

    /// Returns the adapter-owned PID-reuse-safe member identity.
    pub fn incarnation(&self) -> &str {
        &self.incarnation
    }

    /// Returns bounded private command facts for the profile selector.
    pub const fn target(&self) -> &ProfileMatchTarget {
        &self.target
    }

    /// Returns whether every environment-variable name was observed.
    pub const fn environment_names_complete(&self) -> bool {
        self.environment_names_complete
    }
}

impl std::fmt::Debug for ForegroundProgramMember {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ForegroundProgramMember")
            .field("incarnation", &self.incarnation)
            .field("target", &self.target)
            .field(
                "environment_names_complete",
                &self.environment_names_complete,
            )
            .finish()
    }
}

/// One exact managed process's current foreground process-group facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForegroundProcessGroupObservation {
    process: ProcessInstanceId,
    observed_at: TimestampMillis,
    members: Vec<ForegroundProgramMember>,
    completeness: ProcessGroupCompleteness,
    current_directory: Option<PathReference>,
    tcp_listener: TcpListenerObservation,
    managed_process_tcp_listener: TcpListenerObservation,
    managed_tree_tcp_listener: TcpListenerObservation,
    cooperative: Option<CooperativeForegroundState>,
}

impl ForegroundProcessGroupObservation {
    /// Constructs one already-bounded adapter observation.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        process: ProcessInstanceId,
        observed_at: TimestampMillis,
        members: Vec<ForegroundProgramMember>,
        completeness: ProcessGroupCompleteness,
        current_directory: Option<PathReference>,
        tcp_listener: TcpListenerObservation,
        cooperative: Option<CooperativeForegroundState>,
    ) -> Self {
        Self {
            process,
            observed_at,
            members,
            completeness,
            current_directory,
            tcp_listener,
            managed_process_tcp_listener: TcpListenerObservation::Unavailable,
            managed_tree_tcp_listener: TcpListenerObservation::Unavailable,
            cooperative,
        }
    }

    /// Returns the exact managed process incarnation sampled by the adapter.
    pub const fn process(&self) -> &ProcessInstanceId {
        &self.process
    }

    /// Returns when the process-group snapshot was captured.
    pub const fn observed_at(&self) -> TimestampMillis {
        self.observed_at
    }

    /// Returns bounded foreground members in stable adapter order.
    pub fn members(&self) -> &[ForegroundProgramMember] {
        &self.members
    }

    /// Returns whether missing facts forbid a positive classification.
    pub const fn completeness(&self) -> ProcessGroupCompleteness {
        self.completeness
    }

    /// Returns the live managed-shell working directory when available.
    pub const fn current_directory(&self) -> Option<&PathReference> {
        self.current_directory.as_ref()
    }

    /// Returns whether a foreground member owns a TCP listener in the host namespace.
    pub const fn tcp_listener(&self) -> TcpListenerObservation {
        self.tcp_listener
    }

    /// Adds listener evidence for the exact managed root process. This does not
    /// assert foreground membership or change foreground matching completeness.
    pub fn with_managed_process_tcp_listener(mut self, listener: TcpListenerObservation) -> Self {
        self.managed_process_tcp_listener = listener;
        self
    }

    /// Returns host TCP ownership of the exact managed root, independently of
    /// whether the platform can identify a foreground process group.
    pub const fn managed_process_tcp_listener(&self) -> TcpListenerObservation {
        self.managed_process_tcp_listener
    }

    /// Adds kernel-verified listener ownership within the managed process tree.
    /// Background descendants are included; foreground ownership is not implied.
    pub fn with_managed_tree_tcp_listener(mut self, listener: TcpListenerObservation) -> Self {
        self.managed_tree_tcp_listener = listener;
        self
    }

    /// Returns listener ownership in the managed tree, including background
    /// descendants; this does not establish foreground-group membership.
    pub const fn managed_tree_tcp_listener(&self) -> TcpListenerObservation {
        self.managed_tree_tcp_listener
    }

    /// Returns the latest exact-process payload-free cooperative shell state.
    pub const fn cooperative(&self) -> Option<CooperativeForegroundState> {
        self.cooperative
    }
}
