// SPDX-License-Identifier: Apache-2.0

//! Bounded parser-backed foreground-evidence collection.

use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use undercurrent_application::{
    Clock, DetectionProfileCatalog, ForegroundEvidenceCollector, ForegroundProcessObserver,
    PortError, PortErrorKind, PortFuture, PortOperation, ProfilePatternEngine,
    ProfileRuleEvaluator, ProfileServiceError,
};
use undercurrent_domain::{
    Confidence, DetectionArchetype, DomainError, EvidenceDisposition, EvidencePriority,
    EvidenceSource, ExecutionState, ForegroundAxis, ForegroundClaim, ForegroundEvidence,
    ForegroundObservation, InteractionClaim, OutputExclusions, ProcessInstanceId, ProfileName,
    SessionId, TimestampMillis,
};

use crate::{
    CooperativeSignalStore, RuntimeLimitError, TerminalMotionSnapshot, VtTerminalScreenStore,
};

/// Stable runtime-owned evidence source names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RuntimeEvidenceSource {
    /// Exact-process state emitted through the cooperative shell contract.
    CooperativeShell,
    /// Current-screen prompt rule selected from the active profile.
    ProfilePrompt,
    /// Meaningful watched-row change since the prior scheduled observation.
    TerminalMotion,
    /// Profile-declared server remained quiet after producing output.
    ServerSilence,
}

impl RuntimeEvidenceSource {
    /// Complete source registry in evidence-output order.
    pub const ALL: [Self; 4] = [
        Self::CooperativeShell,
        Self::ProfilePrompt,
        Self::TerminalMotion,
        Self::ServerSilence,
    ];

    /// Returns the stable persisted and diagnostic source name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CooperativeShell => "cooperative_shell",
            Self::ProfilePrompt => "profile_prompt",
            Self::TerminalMotion => "terminal_motion",
            Self::ServerSilence => "server_silence",
        }
    }
}

/// Hard state, extraction, and claim-confidence bounds for foreground collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalForegroundEvidenceLimits {
    /// Maximum process sessions with retained motion checkpoints.
    pub tracked_sessions: usize,
    /// Maximum distinct prompt regions extracted in one cycle.
    pub profile_samples: usize,
    /// Maximum private bytes in one extracted prompt sample.
    pub profile_sample_bytes: usize,
    /// Maximum rows fingerprinted for terminal motion.
    pub motion_rows: usize,
    /// Confidence assigned to meaningful terminal motion.
    pub motion_confidence: Confidence,
    /// Confidence assigned to quiet-server inference.
    pub server_idle_confidence: Confidence,
}

impl TerminalForegroundEvidenceLimits {
    /// Validates every state and evidence bound.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns the first stable zero-valued field.
    pub fn validate(self) -> Result<Self, RuntimeLimitError> {
        for (field, value) in [
            ("foreground_tracked_sessions", self.tracked_sessions),
            ("foreground_profile_samples", self.profile_samples),
            ("foreground_profile_sample_bytes", self.profile_sample_bytes),
            ("foreground_motion_rows", self.motion_rows),
            (
                "foreground_motion_confidence",
                usize::from(self.motion_confidence.basis_points()),
            ),
            (
                "foreground_server_idle_confidence",
                usize::from(self.server_idle_confidence.basis_points()),
            ),
        ] {
            if value == 0 {
                return Err(RuntimeLimitError::zero(field));
            }
        }
        Ok(self)
    }
}

/// Payload-free pressure and cumulative collection outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalForegroundEvidencePressure {
    /// Currently retained per-session motion checkpoints.
    pub tracked_sessions: usize,
    /// Inclusive checkpoint capacity.
    pub tracked_session_limit: usize,
    /// Successfully completed evidence cycles.
    pub cycles: u64,
    /// Selected profile prompt rules; one cycle can select multiple claims.
    pub prompt_matches: u64,
    /// Cycles selecting an exact-process cooperative shell signal.
    pub cooperative_matches: u64,
    /// Cycles observing meaningful terminal motion.
    pub motion_matches: u64,
    /// Cycles inferring a quiet server at rest.
    pub server_idle_matches: u64,
    /// Least-recently-used checkpoints removed on admission.
    pub evictions: u64,
}

/// Construction failure for the terminal foreground adapter.
#[derive(Debug)]
pub enum TerminalForegroundEvidenceBuildError {
    /// One configured hard bound is zero.
    Limit(RuntimeLimitError),
    /// One centrally registered evidence source violates domain identity rules.
    InvalidEvidenceSource(DomainError),
}

impl Display for TerminalForegroundEvidenceBuildError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Limit(error) => Display::fmt(error, formatter),
            Self::InvalidEvidenceSource(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for TerminalForegroundEvidenceBuildError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Limit(error) => Some(error),
            Self::InvalidEvidenceSource(error) => Some(error),
        }
    }
}

/// Thread-safe foreground collector over one shared screen store and profile catalog.
pub struct TerminalForegroundEvidenceCollector {
    clock: Arc<dyn Clock>,
    catalog: Arc<dyn DetectionProfileCatalog>,
    patterns: Arc<dyn ProfilePatternEngine>,
    screens: Arc<VtTerminalScreenStore>,
    cooperative_signals: Arc<CooperativeSignalStore>,
    foreground_processes: Option<Arc<dyn ForegroundProcessObserver>>,
    sources: EvidenceSources,
    limits: TerminalForegroundEvidenceLimits,
    motion: Mutex<MotionState>,
    cycles: AtomicU64,
    prompt_matches: AtomicU64,
    cooperative_matches: AtomicU64,
    motion_matches: AtomicU64,
    server_idle_matches: AtomicU64,
    evictions: AtomicU64,
}

impl Debug for TerminalForegroundEvidenceCollector {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TerminalForegroundEvidenceCollector")
            .field("limits", &self.limits)
            .field("pressure", &self.pressure())
            .finish_non_exhaustive()
    }
}

impl TerminalForegroundEvidenceCollector {
    /// Constructs an empty bounded collector without starting workers or IO.
    ///
    /// Dependencies are shared because the daemon composition root owns their
    /// lifetimes. Construction validates all source identifiers and does not
    /// panic.
    ///
    /// # Errors
    ///
    /// Returns a typed limit or source-identity failure.
    pub fn new(
        clock: Arc<dyn Clock>,
        catalog: Arc<dyn DetectionProfileCatalog>,
        patterns: Arc<dyn ProfilePatternEngine>,
        screens: Arc<VtTerminalScreenStore>,
        cooperative_signals: Arc<CooperativeSignalStore>,
        limits: TerminalForegroundEvidenceLimits,
    ) -> Result<Self, TerminalForegroundEvidenceBuildError> {
        let limits = limits
            .validate()
            .map_err(TerminalForegroundEvidenceBuildError::Limit)?;
        let sources = EvidenceSources::new()
            .map_err(TerminalForegroundEvidenceBuildError::InvalidEvidenceSource)?;
        Ok(Self {
            clock,
            catalog,
            patterns,
            screens,
            cooperative_signals,
            foreground_processes: None,
            sources,
            limits,
            motion: Mutex::new(MotionState::default()),
            cycles: AtomicU64::new(0),
            prompt_matches: AtomicU64::new(0),
            cooperative_matches: AtomicU64::new(0),
            motion_matches: AtomicU64::new(0),
            server_idle_matches: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        })
    }

    /// Enables live confirmation of cooperative shell readiness. Without an
    /// observer, shell signals retain their original evidence timestamps.
    pub fn with_foreground_processes(
        mut self,
        observer: Arc<dyn ForegroundProcessObserver>,
    ) -> Self {
        self.foreground_processes = Some(observer);
        self
    }

    /// Returns payload-free occupancy and cumulative evidence outcomes.
    ///
    /// This function performs no external IO, retains no terminal content in
    /// its result, and does not panic.
    pub fn pressure(&self) -> TerminalForegroundEvidencePressure {
        TerminalForegroundEvidencePressure {
            tracked_sessions: self.lock_motion().entries.len(),
            tracked_session_limit: self.limits.tracked_sessions,
            cycles: self.cycles.load(Ordering::Acquire),
            prompt_matches: self.prompt_matches.load(Ordering::Acquire),
            cooperative_matches: self.cooperative_matches.load(Ordering::Acquire),
            motion_matches: self.motion_matches.load(Ordering::Acquire),
            server_idle_matches: self.server_idle_matches.load(Ordering::Acquire),
            evictions: self.evictions.load(Ordering::Acquire),
        }
    }

    fn collect_now(
        &self,
        session_id: &SessionId,
        process: &ProcessInstanceId,
        profile_name: Option<&ProfileName>,
        cooperative: Option<(crate::CooperativeForegroundSignal, TimestampMillis)>,
    ) -> Result<Vec<ForegroundEvidence>, PortError> {
        let profile = self.resolve_profile(session_id, profile_name)?;
        let regions = profile
            .as_ref()
            .into_iter()
            .flat_map(|profile| profile.foreground_rules())
            .map(|rule| rule.region().cloned());
        let exclusions = profile
            .as_ref()
            .map_or_else(OutputExclusions::default, |profile| {
                profile.output_exclusions()
            });
        let samples = self.screens.foreground_samples(
            session_id,
            regions,
            exclusions,
            self.limits.profile_samples,
            self.limits.profile_sample_bytes,
            self.limits.motion_rows,
        )?;
        let selected_foreground = profile
            .as_ref()
            .map(|profile| {
                ProfileRuleEvaluator::new(self.patterns.as_ref())
                    .select_foreground(profile, samples.samples())
            })
            .transpose()
            .map_err(|error| profile_evaluation_error(session_id, error))?
            .unwrap_or_default();
        let motion = self.observe_motion(
            session_id,
            process,
            profile_name.cloned(),
            exclusions,
            samples.motion(),
        );
        let observed_at = self.clock.now();

        let mut evidence = Vec::new();
        match cooperative {
            Some((signal, confirmed_at)) => {
                increment_counter(&self.cooperative_matches);
                evidence.push(evidence_claim(
                    self.sources.cooperative_shell.clone(),
                    EvidencePriority::Cooperative,
                    EvidenceDisposition::Affirmative,
                    ForegroundClaim::Execution(signal.foreground().execution()),
                    Confidence::CERTAIN,
                    confirmed_at,
                    session_id,
                )?);
                let interaction = InteractionClaim::new(
                    signal.foreground().interaction(),
                    signal.foreground().modality(),
                    None,
                )
                .map_err(|error| invalid_evidence(session_id, error))?;
                evidence.push(evidence_claim(
                    self.sources.cooperative_shell.clone(),
                    EvidencePriority::Cooperative,
                    EvidenceDisposition::Affirmative,
                    ForegroundClaim::Interaction(interaction),
                    Confidence::CERTAIN,
                    confirmed_at,
                    session_id,
                )?);
            }
            None => {
                evidence.push(ForegroundEvidence::unavailable(
                    self.sources.cooperative_shell.clone(),
                    EvidencePriority::Cooperative,
                    ForegroundAxis::Execution,
                    observed_at,
                ));
                evidence.push(ForegroundEvidence::unavailable(
                    self.sources.cooperative_shell.clone(),
                    EvidencePriority::Cooperative,
                    ForegroundAxis::Interaction,
                    observed_at,
                ));
            }
        }

        let selected_interaction = selected_foreground
            .iter()
            .any(|selected| selected.claim.axis() == ForegroundAxis::Interaction);
        for selected in selected_foreground {
            increment_counter(&self.prompt_matches);
            evidence.push(evidence_claim(
                self.sources.profile_prompt.clone(),
                EvidencePriority::Terminal,
                EvidenceDisposition::Affirmative,
                selected.claim,
                selected.confidence,
                observed_at,
                session_id,
            )?);
        }
        if !selected_interaction {
            if let Some(baseline) = profile
                .as_ref()
                .and_then(|profile| profile.baseline_interaction())
            {
                evidence.push(evidence_claim(
                    self.sources.profile_prompt.clone(),
                    EvidencePriority::Inference,
                    EvidenceDisposition::Inference,
                    ForegroundClaim::Interaction(baseline.clone()),
                    self.limits.server_idle_confidence,
                    observed_at,
                    session_id,
                )?);
            } else {
                evidence.push(ForegroundEvidence::unavailable(
                    self.sources.profile_prompt.clone(),
                    EvidencePriority::Terminal,
                    ForegroundAxis::Interaction,
                    observed_at,
                ));
            }
        }
        let motion_evidence = if motion.changed {
            increment_counter(&self.motion_matches);
            evidence_claim(
                self.sources.terminal_motion.clone(),
                EvidencePriority::TerminalHistory,
                EvidenceDisposition::Affirmative,
                ForegroundClaim::Execution(ExecutionState::Active),
                self.limits.motion_confidence,
                observed_at,
                session_id,
            )?
        } else {
            ForegroundEvidence::unavailable(
                self.sources.terminal_motion.clone(),
                EvidencePriority::TerminalHistory,
                ForegroundAxis::Execution,
                observed_at,
            )
        };
        let quiet_server = profile.as_ref().is_some_and(|profile| {
            profile.archetype() == DetectionArchetype::Server
                && motion.baseline_available
                && !motion.changed
                && motion.has_process_output
        });
        let server_evidence = if quiet_server {
            increment_counter(&self.server_idle_matches);
            evidence_claim(
                self.sources.server_silence.clone(),
                EvidencePriority::Inference,
                EvidenceDisposition::Inference,
                ForegroundClaim::Execution(ExecutionState::Quiescent),
                self.limits.server_idle_confidence,
                observed_at,
                session_id,
            )?
        } else {
            ForegroundEvidence::unavailable(
                self.sources.server_silence.clone(),
                EvidencePriority::Inference,
                ForegroundAxis::Execution,
                observed_at,
            )
        };
        increment_counter(&self.cycles);
        evidence.push(motion_evidence);
        evidence.push(server_evidence);
        Ok(evidence)
    }

    fn resolve_profile(
        &self,
        session_id: &SessionId,
        profile_name: Option<&ProfileName>,
    ) -> Result<Option<Arc<undercurrent_domain::DetectionProfile>>, PortError> {
        let Some(profile_name) = profile_name else {
            return Ok(None);
        };
        let entry = self
            .catalog
            .get(profile_name)
            .map_err(|error| dependency_error(session_id, error))?
            .ok_or_else(|| {
                PortError::new(
                    PortErrorKind::InvalidData,
                    PortOperation::CollectForegroundEvidence,
                    Some(session_id.clone()),
                    ForegroundCollectionFailure::MissingProfile(profile_name.clone()),
                )
            })?;
        Ok(Some(entry.shared_profile()))
    }

    fn observe_motion(
        &self,
        session_id: &SessionId,
        process: &ProcessInstanceId,
        profile: Option<ProfileName>,
        exclusions: OutputExclusions,
        current: &TerminalMotionSnapshot,
    ) -> MotionObservation {
        let mut state = self.lock_motion();
        let access = state.next_access();
        if let Some(entry) = state.entries.get_mut(session_id) {
            if entry.process == *process
                && entry.profile == profile
                && entry.exclusions == exclusions
            {
                let observation = MotionObservation {
                    baseline_available: true,
                    changed: current.changed_since(&entry.snapshot),
                    has_process_output: current.has_process_output(),
                };
                entry.snapshot = current.clone();
                entry.last_access = access;
                return observation;
            }
            *entry = MotionEntry {
                process: process.clone(),
                profile,
                exclusions,
                snapshot: current.clone(),
                last_access: access,
            };
            return MotionObservation {
                baseline_available: false,
                changed: false,
                has_process_output: current.has_process_output(),
            };
        }
        if state.entries.len() >= self.limits.tracked_sessions {
            let oldest = state
                .entries
                .iter()
                .min_by(|(left_id, left), (right_id, right)| {
                    (left.last_access, *left_id).cmp(&(right.last_access, *right_id))
                })
                .map(|(session_id, _)| session_id.clone());
            if let Some(oldest) = oldest {
                state.entries.remove(&oldest);
                increment_counter(&self.evictions);
            }
        }
        state.entries.insert(
            session_id.clone(),
            MotionEntry {
                process: process.clone(),
                profile,
                exclusions,
                snapshot: current.clone(),
                last_access: access,
            },
        );
        MotionObservation {
            baseline_available: false,
            changed: false,
            has_process_output: current.has_process_output(),
        }
    }

    fn lock_motion(&self) -> std::sync::MutexGuard<'_, MotionState> {
        self.motion
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl ForegroundEvidenceCollector for TerminalForegroundEvidenceCollector {
    fn collect<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        profile: Option<&'a ProfileName>,
        _current: &'a ForegroundObservation,
    ) -> PortFuture<'a, Result<Vec<ForegroundEvidence>, PortError>> {
        Box::pin(async move {
            let signal = self.cooperative_signals.latest_exact(session_id, process);
            let cooperative = match (signal, self.foreground_processes.as_ref()) {
                (Some(signal), Some(observer))
                    if signal.foreground() == crate::CooperativeForegroundState::Ready =>
                {
                    // A prompt is persistent state, but only while the live
                    // observer confirms the same shell still owns the foreground.
                    // Never refresh a cached signal merely because it was read.
                    observer
                        .observe_foreground(session_id, process)
                        .await
                        .ok()
                        .filter(|observation| {
                            observation.process() == process
                                && observation.cooperative()
                                    == Some(crate::CooperativeForegroundState::Ready)
                                && observation.observed_at() >= signal.observed_at()
                                && self
                                    .cooperative_signals
                                    .latest_exact(session_id, process)
                                    .is_some_and(|current| {
                                        current.foreground() == signal.foreground()
                                            && current.observed_at() == signal.observed_at()
                                    })
                        })
                        .map(|observation| (signal, observation.observed_at()))
                }
                (Some(signal), _) => {
                    let observed_at = signal.observed_at();
                    Some((signal, observed_at))
                }
                (None, _) => None,
            };
            self.collect_now(session_id, process, profile, cooperative)
        })
    }
}

#[derive(Debug)]
enum ForegroundCollectionFailure {
    MissingProfile(ProfileName),
    InvalidProfile(ProfileServiceError),
    InvalidEvidence(DomainError),
}

impl Display for ForegroundCollectionFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingProfile(profile) => {
                write!(formatter, "active detection profile {profile} is missing")
            }
            Self::InvalidProfile(error) => Display::fmt(error, formatter),
            Self::InvalidEvidence(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for ForegroundCollectionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::MissingProfile(_) => None,
            Self::InvalidProfile(error) => Some(error),
            Self::InvalidEvidence(error) => Some(error),
        }
    }
}

struct EvidenceSources {
    cooperative_shell: EvidenceSource,
    profile_prompt: EvidenceSource,
    terminal_motion: EvidenceSource,
    server_silence: EvidenceSource,
}

impl EvidenceSources {
    fn new() -> Result<Self, DomainError> {
        Ok(Self {
            cooperative_shell: EvidenceSource::new(
                RuntimeEvidenceSource::CooperativeShell.as_str(),
            )?,
            profile_prompt: EvidenceSource::new(RuntimeEvidenceSource::ProfilePrompt.as_str())?,
            terminal_motion: EvidenceSource::new(RuntimeEvidenceSource::TerminalMotion.as_str())?,
            server_silence: EvidenceSource::new(RuntimeEvidenceSource::ServerSilence.as_str())?,
        })
    }
}

#[derive(Default)]
struct MotionState {
    access_sequence: u64,
    entries: HashMap<SessionId, MotionEntry>,
}

impl MotionState {
    fn next_access(&mut self) -> u64 {
        if let Some(next) = self.access_sequence.checked_add(1) {
            self.access_sequence = next;
            return next;
        }
        let mut entries = self.entries.values_mut().collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.last_access);
        for (index, entry) in entries.into_iter().enumerate() {
            entry.last_access = u64::try_from(index).unwrap_or(u64::MAX);
        }
        self.access_sequence = u64::try_from(self.entries.len()).unwrap_or(u64::MAX);
        self.access_sequence
    }
}

struct MotionEntry {
    process: ProcessInstanceId,
    profile: Option<ProfileName>,
    exclusions: OutputExclusions,
    snapshot: TerminalMotionSnapshot,
    last_access: u64,
}

#[derive(Debug, Clone, Copy)]
struct MotionObservation {
    baseline_available: bool,
    changed: bool,
    has_process_output: bool,
}

#[allow(clippy::too_many_arguments)]
fn evidence_claim(
    source: EvidenceSource,
    priority: EvidencePriority,
    disposition: EvidenceDisposition,
    claim: ForegroundClaim,
    confidence: Confidence,
    observed_at: TimestampMillis,
    session_id: &SessionId,
) -> Result<ForegroundEvidence, PortError> {
    ForegroundEvidence::new(
        source,
        priority,
        disposition,
        claim,
        confidence,
        observed_at,
    )
    .map_err(|error| invalid_evidence(session_id, error))
}

fn invalid_evidence(session_id: &SessionId, error: DomainError) -> PortError {
    PortError::new(
        PortErrorKind::InvalidData,
        PortOperation::CollectForegroundEvidence,
        Some(session_id.clone()),
        ForegroundCollectionFailure::InvalidEvidence(error),
    )
}

fn dependency_error(session_id: &SessionId, error: PortError) -> PortError {
    PortError::new(
        error.kind(),
        PortOperation::CollectForegroundEvidence,
        Some(session_id.clone()),
        error,
    )
}

fn profile_evaluation_error(session_id: &SessionId, error: ProfileServiceError) -> PortError {
    PortError::new(
        PortErrorKind::InvalidData,
        PortOperation::CollectForegroundEvidence,
        Some(session_id.clone()),
        ForegroundCollectionFailure::InvalidProfile(error),
    )
}

fn increment_counter(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(1))
    });
}
