// SPDX-License-Identifier: Apache-2.0

//! Exact-process runtime selection of one effective detection profile.

use crate::{
    DomainError, DomainResult, PathReference, ProcessInstanceId, ProfileName, TimestampMillis,
};

/// Payload-free evidence for one matched profile across a bounded foreground group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileMatchEvidence {
    profile: ProfileName,
    matching_fallback: bool,
    process_incarnations: Vec<String>,
}

impl ProfileMatchEvidence {
    /// Constructs evidence from validated profile metadata and bounded observer identities.
    ///
    /// The caller enforces vector and identity-string bounds; this constructor
    /// stores the supplied values without further validation or process lookup.
    pub fn new(
        profile: ProfileName,
        matching_fallback: bool,
        process_incarnations: Vec<String>,
    ) -> Self {
        Self {
            profile,
            matching_fallback,
            process_incarnations,
        }
    }

    /// Returns the matching profile name.
    pub const fn profile(&self) -> &ProfileName {
        &self.profile
    }

    /// Returns whether this match is a fallback candidate.
    pub const fn matching_fallback(&self) -> bool {
        self.matching_fallback
    }

    /// Returns opaque process identities, without executable paths or arguments.
    pub fn process_incarnations(&self) -> &[String] {
        &self.process_incarnations
    }
}

/// Per-session runtime generation of effective-profile evidence.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EffectiveProfileEpoch(u64);

impl EffectiveProfileEpoch {
    /// Initial generation before adaptive process evidence is available.
    pub const INITIAL: Self = Self(0);

    /// Reconstructs a process-local epoch from a trusted prepared projection.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the process-local numeric generation.
    pub const fn value(self) -> u64 {
        self.0
    }

    /// Advances the generation for one effective-profile transition.
    ///
    /// # Errors
    ///
    /// Returns a typed exhaustion failure instead of wrapping.
    pub fn next(self) -> DomainResult<Self> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(DomainError::RevisionExhausted {
                scope: "effective_profile",
            })
    }
}

/// Why an adaptive foreground could not select one profile safely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UnclassifiedProfileReason {
    /// The exact running process has not yet been observed.
    AwaitingObservation,
    /// Complete foreground evidence matched no enabled profile.
    NoMatch,
    /// Complete evidence matched more than one enabled profile.
    Ambiguous,
    /// A bounded observation omitted facts required for safe matching.
    IncompleteEvidence,
    /// Operating-system foreground evidence was unavailable.
    Unavailable,
}

impl UnclassifiedProfileReason {
    /// Complete stable reason registry.
    pub const ALL: [Self; 5] = [
        Self::AwaitingObservation,
        Self::NoMatch,
        Self::Ambiguous,
        Self::IncompleteEvidence,
        Self::Unavailable,
    ];

    /// Returns the stable protocol and diagnostic name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AwaitingObservation => "awaiting_observation",
            Self::NoMatch => "no_match",
            Self::Ambiguous => "ambiguous",
            Self::IncompleteEvidence => "incomplete_evidence",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Profile or explicit safe unclassified state selected for one foreground epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectiveProfileSelection {
    /// Exactly one enabled profile matched complete evidence.
    Profile(ProfileName),
    /// No profile was selected because the evidence was not decisive.
    Unclassified(UnclassifiedProfileReason),
}

impl EffectiveProfileSelection {
    /// Returns the selected profile, if evidence was decisive.
    pub const fn profile(&self) -> Option<&ProfileName> {
        match self {
            Self::Profile(profile) => Some(profile),
            Self::Unclassified(_) => None,
        }
    }

    /// Returns the safe unclassified reason, if no profile was selected.
    pub const fn unclassified_reason(&self) -> Option<UnclassifiedProfileReason> {
        match self {
            Self::Profile(_) => None,
            Self::Unclassified(reason) => Some(*reason),
        }
    }
}

/// Evidence family responsible for one effective-profile selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EffectiveProfileOrigin {
    /// Durable fixed base-profile configuration.
    BaseProfile,
    /// Exact operating-system foreground process-group facts.
    ForegroundProcess,
    /// Payload-free cooperative shell state restored the base profile.
    CooperativeShell,
}

impl EffectiveProfileOrigin {
    /// Complete stable origin registry.
    pub const ALL: [Self; 3] = [
        Self::BaseProfile,
        Self::ForegroundProcess,
        Self::CooperativeShell,
    ];

    /// Returns the stable protocol and diagnostic name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BaseProfile => "base_profile",
            Self::ForegroundProcess => "foreground_process",
            Self::CooperativeShell => "cooperative_shell",
        }
    }
}

/// Evidence that conservatively identifies a running process as a server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ServerRoleOrigin {
    /// The selected effective profile has the server archetype.
    Profile,
    /// A foreground process-group member owns a host TCP listener.
    TcpListener,
    /// Both independent evidence families identify a server.
    ProfileAndTcpListener,
    /// The exact managed root process owns a host TCP listener; foreground
    /// membership is not implied by this evidence.
    ManagedProcessTcpListener,
    /// Server profile plus listener ownership by the exact managed root.
    ProfileAndManagedProcessTcpListener,
    /// A kernel-verified managed tree member owns a host TCP listener.
    ManagedTreeTcpListener,
    /// Server profile plus listener ownership within the managed process tree.
    ProfileAndManagedTreeTcpListener,
}

impl ServerRoleOrigin {
    /// Complete stable origin registry.
    pub const ALL: [Self; 7] = [
        Self::Profile,
        Self::TcpListener,
        Self::ProfileAndTcpListener,
        Self::ManagedProcessTcpListener,
        Self::ProfileAndManagedProcessTcpListener,
        Self::ManagedTreeTcpListener,
        Self::ProfileAndManagedTreeTcpListener,
    ];

    /// Returns the stable protocol and diagnostic name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Profile => "profile",
            Self::TcpListener => "tcp_listener",
            Self::ProfileAndTcpListener => "profile_and_tcp_listener",
            Self::ManagedProcessTcpListener => "managed_process_tcp_listener",
            Self::ProfileAndManagedProcessTcpListener => "profile_and_managed_process_tcp_listener",
            Self::ManagedTreeTcpListener => "managed_tree_tcp_listener",
            Self::ProfileAndManagedTreeTcpListener => "profile_and_managed_tree_tcp_listener",
        }
    }
}

/// One exact-process effective-profile snapshot in the prepared catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveProfileSnapshot {
    process: ProcessInstanceId,
    epoch: EffectiveProfileEpoch,
    selection: EffectiveProfileSelection,
    origin: EffectiveProfileOrigin,
    current_directory: Option<PathReference>,
    server_role: Option<ServerRoleOrigin>,
    matching_evidence: Vec<ProfileMatchEvidence>,
    observed_at: TimestampMillis,
}

impl EffectiveProfileSnapshot {
    /// Constructs one already-bounded process-local snapshot.
    pub const fn new(
        process: ProcessInstanceId,
        epoch: EffectiveProfileEpoch,
        selection: EffectiveProfileSelection,
        origin: EffectiveProfileOrigin,
        observed_at: TimestampMillis,
    ) -> Self {
        Self {
            process,
            epoch,
            selection,
            origin,
            current_directory: None,
            server_role: None,
            matching_evidence: Vec::new(),
            observed_at,
        }
    }

    /// Attaches matching evidence from the same bounded foreground observation.
    pub fn with_matching_evidence(mut self, evidence: Vec<ProfileMatchEvidence>) -> Self {
        self.matching_evidence = evidence;
        self
    }

    /// Returns profile matches before fallback filtering.
    pub fn matching_evidence(&self) -> &[ProfileMatchEvidence] {
        &self.matching_evidence
    }

    /// Attaches already-bounded live directory and conservative server-role facts.
    pub fn with_runtime_context(
        mut self,
        current_directory: Option<PathReference>,
        server_role: Option<ServerRoleOrigin>,
    ) -> Self {
        self.current_directory = current_directory;
        self.server_role = server_role;
        self
    }

    /// Returns the exact managed process incarnation.
    pub const fn process(&self) -> &ProcessInstanceId {
        &self.process
    }

    /// Returns the per-session runtime generation.
    pub const fn epoch(&self) -> EffectiveProfileEpoch {
        self.epoch
    }

    /// Returns the selected profile or safe unclassified state.
    pub const fn selection(&self) -> &EffectiveProfileSelection {
        &self.selection
    }

    /// Returns the evidence family responsible for the selection.
    pub const fn origin(&self) -> EffectiveProfileOrigin {
        self.origin
    }

    /// Returns the live managed-shell working directory observed for this process.
    pub const fn current_directory(&self) -> Option<&PathReference> {
        self.current_directory.as_ref()
    }

    /// Returns conservative server-role evidence when present.
    pub const fn server_role(&self) -> Option<ServerRoleOrigin> {
        self.server_role
    }

    /// Returns when the selection evidence was observed.
    pub const fn observed_at(&self) -> TimestampMillis {
        self.observed_at
    }
}

/// Requested adaptive selection before the directory assigns its next epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveProfileTransition {
    process: ProcessInstanceId,
    selection: EffectiveProfileSelection,
    origin: EffectiveProfileOrigin,
    current_directory: Option<PathReference>,
    server_role: Option<ServerRoleOrigin>,
    matching_evidence: Vec<ProfileMatchEvidence>,
    observed_at: TimestampMillis,
}

impl EffectiveProfileTransition {
    /// Constructs a transition for one exact managed process.
    pub const fn new(
        process: ProcessInstanceId,
        selection: EffectiveProfileSelection,
        origin: EffectiveProfileOrigin,
        observed_at: TimestampMillis,
    ) -> Self {
        Self {
            process,
            selection,
            origin,
            current_directory: None,
            server_role: None,
            matching_evidence: Vec::new(),
            observed_at,
        }
    }

    /// Attaches matching evidence from the same bounded foreground observation.
    pub fn with_matching_evidence(mut self, evidence: Vec<ProfileMatchEvidence>) -> Self {
        self.matching_evidence = evidence;
        self
    }

    /// Returns profile matches before fallback filtering.
    pub fn matching_evidence(&self) -> &[ProfileMatchEvidence] {
        &self.matching_evidence
    }

    /// Attaches already-bounded live directory and conservative server-role facts.
    pub fn with_runtime_context(
        mut self,
        current_directory: Option<PathReference>,
        server_role: Option<ServerRoleOrigin>,
    ) -> Self {
        self.current_directory = current_directory;
        self.server_role = server_role;
        self
    }

    /// Returns the exact process incarnation being observed.
    pub const fn process(&self) -> &ProcessInstanceId {
        &self.process
    }

    /// Returns the proposed selection.
    pub const fn selection(&self) -> &EffectiveProfileSelection {
        &self.selection
    }

    /// Returns the proposed evidence origin.
    pub const fn origin(&self) -> EffectiveProfileOrigin {
        self.origin
    }

    /// Returns the proposed live managed-shell working directory.
    pub const fn current_directory(&self) -> Option<&PathReference> {
        self.current_directory.as_ref()
    }

    /// Returns proposed conservative server-role evidence.
    pub const fn server_role(&self) -> Option<ServerRoleOrigin> {
        self.server_role
    }

    /// Returns when the proposed evidence was observed.
    pub const fn observed_at(&self) -> TimestampMillis {
        self.observed_at
    }
}
