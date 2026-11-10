// SPDX-License-Identifier: Apache-2.0

//! Independently scheduled foreground and health observation use cases.

use std::error::Error;
use std::fmt::{Display, Formatter};

use undercurrent_domain::{
    CooperativeForegroundState, DetectionArchetype, DomainError, DomainEventKind,
    EffectiveProfileOrigin, EffectiveProfileSelection, EffectiveProfileSnapshot,
    EffectiveProfileTransition, ForegroundDecisionPolicy, HealthObservation, LifecycleState,
    ProfileBindingMode, Revision, ServerRoleOrigin, Session, SessionId,
};

use crate::commands::{MutationCore, SessionMutationError, not_before};
use crate::{
    ApplicationErrorCategory, ApplicationOperation, Clock, DetectionProfileCatalog,
    DetectionProfileSelector, DiagnosticEventName, DiagnosticLevel, EffectiveProfileDirectory,
    ForegroundEvidenceCollector, ForegroundProcessObserver, HealthCheckExecutor, MetricName,
    MetricObservation, PortError, PortOperation, ProcessGroupCompleteness, ProfileMatchOutcome,
    ProfilePatternEngine, ProfileServiceError, SessionMutationCoordinator, SessionProcessDirectory,
    SessionProjectionWriter, SessionRepository, TcpListenerObservation, Telemetry,
};

/// Expected reason an independently scheduled observation did no work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObservationSkipReason {
    /// Session does not currently own a running process lifecycle.
    NotRunning,
    /// Foreground detection is disabled by durable execution policy.
    DetectionDisabled,
    /// No health rules are configured for the session.
    NoHealthRules,
    /// Durable state says running but no live process binding is available.
    MissingProcessBinding,
    /// Adaptive runtime profile state is not prepared for the exact process.
    MissingEffectiveProfile,
}

/// Why collected data was discarded rather than committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StaleObservationReason {
    /// Durable session revision changed while the adapter was collecting data.
    SessionChanged,
    /// Live process ownership changed without a matching observation result.
    ProcessChanged,
    /// Effective profile generation changed while evidence was collected.
    ProfileChanged,
}

/// Result of one bounded foreground or health observation cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservationOutcome {
    /// Expected preconditions made this cycle unnecessary.
    Skipped(ObservationSkipReason),
    /// Collected data was discarded after concurrent state change.
    Stale {
        /// Stable reason the observation no longer matched current state.
        reason: StaleObservationReason,
        /// Revision from which adapter inputs were captured.
        sampled_revision: Revision,
        /// Current durable revision when the commit was considered.
        current_revision: Revision,
    },
    /// The observation matched already durable content.
    Unchanged {
        /// Current durable revision.
        revision: Revision,
    },
    /// New observation detail and compact state were committed.
    Committed {
        /// Durable revision after the commit.
        revision: Revision,
        /// Stable domain event describing summary or evidence change.
        event: DomainEventKind,
    },
}

/// Failure from independently scheduled foreground or health observation.
#[derive(Debug)]
pub enum ObservationError {
    /// Ordered snapshot or durable commit orchestration failed.
    Mutation(SessionMutationError),
    /// Live process binding could not be inspected.
    ProcessDirectory(PortError),
    /// Exact-process effective profile state could not be read.
    EffectiveProfileDirectory(PortError),
    /// Active profile selection or expression evaluation failed.
    ProfileSelection(ProfileServiceError),
    /// Active profile catalog access failed while determining the runtime role.
    ProfileCatalog(PortError),
    /// Foreground signal collection failed at an adapter boundary.
    ForegroundCollection(PortError),
    /// Health rule execution failed at an adapter boundary.
    HealthExecution(PortError),
    /// Collected typed data violated domain invariants or configured bounds.
    Domain(DomainError),
}

impl ObservationError {
    /// Returns the stable category used by protocol and presentation adapters.
    pub const fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::Mutation(error) => error.category(),
            Self::Domain(error) => ApplicationErrorCategory::from_domain(error.category()),
            Self::ProfileSelection(error) => error.category(),
            Self::ProcessDirectory(error)
            | Self::EffectiveProfileDirectory(error)
            | Self::ProfileCatalog(error)
            | Self::ForegroundCollection(error)
            | Self::HealthExecution(error) => error.category(),
        }
    }
}

impl Display for ObservationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mutation(error) => Display::fmt(error, formatter),
            Self::ProcessDirectory(error) => {
                write!(
                    formatter,
                    "could not inspect session process binding: {error}"
                )
            }
            Self::EffectiveProfileDirectory(error) => {
                write!(
                    formatter,
                    "could not inspect effective profile state: {error}"
                )
            }
            Self::ProfileSelection(error) => {
                write!(formatter, "could not select an effective profile: {error}")
            }
            Self::ProfileCatalog(error) => {
                write!(formatter, "could not inspect the active profile: {error}")
            }
            Self::ForegroundCollection(error) => {
                write!(formatter, "could not collect foreground evidence: {error}")
            }
            Self::HealthExecution(error) => {
                write!(formatter, "could not execute health checks: {error}")
            }
            Self::Domain(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for ObservationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Mutation(error) => Some(error),
            Self::ProfileSelection(error) => Some(error),
            Self::ProcessDirectory(error)
            | Self::EffectiveProfileDirectory(error)
            | Self::ProfileCatalog(error)
            | Self::ForegroundCollection(error)
            | Self::HealthExecution(error) => Some(error),
            Self::Domain(error) => Some(error),
        }
    }
}

impl From<SessionMutationError> for ObservationError {
    fn from(error: SessionMutationError) -> Self {
        Self::Mutation(error)
    }
}

impl From<DomainError> for ObservationError {
    fn from(error: DomainError) -> Self {
        Self::Domain(error)
    }
}

/// Collects and commits one best-effort foreground decision for one session.
pub struct ObserveForegroundService<'a> {
    core: MutationCore<'a>,
    processes: &'a dyn SessionProcessDirectory,
    effective_profiles: &'a dyn EffectiveProfileDirectory,
    foreground_processes: &'a dyn ForegroundProcessObserver,
    profiles: &'a dyn DetectionProfileCatalog,
    patterns: &'a dyn ProfilePatternEngine,
    max_profiles: usize,
    collector: &'a dyn ForegroundEvidenceCollector,
    telemetry: &'a dyn Telemetry,
    policy: ForegroundDecisionPolicy,
}

enum EffectiveContextResolution {
    Current(Option<EffectiveProfileSnapshot>),
    Stale {
        reason: StaleObservationReason,
        current_revision: Revision,
    },
}

impl<'a> ObserveForegroundService<'a> {
    /// Constructs an foreground observer without starting a timer or global scan.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        clock: &'a dyn Clock,
        coordinator: &'a dyn SessionMutationCoordinator,
        repository: &'a dyn SessionRepository,
        projection: &'a dyn SessionProjectionWriter,
        processes: &'a dyn SessionProcessDirectory,
        effective_profiles: &'a dyn EffectiveProfileDirectory,
        foreground_processes: &'a dyn ForegroundProcessObserver,
        profiles: &'a dyn DetectionProfileCatalog,
        patterns: &'a dyn ProfilePatternEngine,
        max_profiles: usize,
        collector: &'a dyn ForegroundEvidenceCollector,
        telemetry: &'a dyn Telemetry,
        policy: ForegroundDecisionPolicy,
    ) -> Self {
        Self {
            core: MutationCore::new(
                clock,
                coordinator,
                repository,
                projection,
                telemetry,
                ApplicationOperation::ObserveForeground,
            ),
            processes,
            effective_profiles,
            foreground_processes,
            profiles,
            patterns,
            max_profiles,
            collector,
            telemetry,
            policy,
        }
    }

    /// Collects foreground evidence outside mutation ownership and conditionally
    /// commits a pure domain decision.
    ///
    /// The initial lease is dropped before adapter IO. A second lease protects
    /// the revision/process comparison and commit. This method does not start a
    /// timer, scan unrelated sessions, or panic.
    ///
    /// # Errors
    ///
    /// Returns typed mutation, process-directory, collection, or domain errors.
    pub async fn execute(
        &self,
        session_id: &SessionId,
    ) -> Result<ObservationOutcome, ObservationError> {
        self.execute_inner(session_id, None).await
    }

    /// Executes a queued foreground observation only when its source revision is
    /// still current.
    ///
    /// Revision validation occurs before process lookup and evidence
    /// collection, so superseded jobs do not perform expensive adapter IO. This
    /// method does not start a timer, scan unrelated sessions, or panic.
    ///
    /// # Errors
    ///
    /// Returns typed mutation, process-directory, collection, or domain errors.
    pub async fn execute_scheduled(
        &self,
        session_id: &SessionId,
        expected_revision: Revision,
    ) -> Result<ObservationOutcome, ObservationError> {
        self.execute_inner(session_id, Some(expected_revision))
            .await
    }

    async fn resolve_effective_context(
        &self,
        session_id: &SessionId,
        session: &mut Session,
        process: &crate::ProcessInstance,
    ) -> Result<EffectiveContextResolution, ObservationError> {
        let observation_started = self.core.clock.now();
        let observation_result = self
            .foreground_processes
            .observe_foreground(session_id, &process.id)
            .await;
        let observation_finished = self.core.clock.now();
        self.record_metric(
            MetricName::ForegroundProcessObservationDurationSeconds,
            observation_finished
                .saturating_duration_since(observation_started)
                .as_secs_f64(),
            ApplicationOperation::ObserveForeground,
        );
        let observation = match observation_result {
            Ok(observation) => {
                self.core.emit(
                    DiagnosticEventName::ForegroundProcessObserved,
                    DiagnosticLevel::Debug,
                    observation_finished,
                    Some(session_id.clone()),
                    Some(PortOperation::ObserveForegroundProcessGroup),
                    None,
                );
                Some(observation)
            }
            Err(error) => {
                self.core
                    .emit_port_failure(observation_finished, Some(session_id.clone()), &error);
                None
            }
        };
        let binding = session.configuration().profile_binding();
        let mut matching_evidence = Vec::new();
        let (selection, origin) = match (binding, observation.as_ref()) {
            (ProfileBindingMode::Fixed, _) => (
                session.profile().cloned().map_or(
                    EffectiveProfileSelection::Unclassified(
                        undercurrent_domain::UnclassifiedProfileReason::NoMatch,
                    ),
                    EffectiveProfileSelection::Profile,
                ),
                EffectiveProfileOrigin::BaseProfile,
            ),
            (ProfileBindingMode::ForegroundAdaptive, None) => (
                EffectiveProfileSelection::Unclassified(
                    undercurrent_domain::UnclassifiedProfileReason::Unavailable,
                ),
                EffectiveProfileOrigin::ForegroundProcess,
            ),
            (ProfileBindingMode::ForegroundAdaptive, Some(observation))
                if observation.cooperative() == Some(CooperativeForegroundState::Ready) =>
            {
                (
                    session.profile().cloned().map_or(
                        EffectiveProfileSelection::Unclassified(
                            undercurrent_domain::UnclassifiedProfileReason::NoMatch,
                        ),
                        EffectiveProfileSelection::Profile,
                    ),
                    EffectiveProfileOrigin::CooperativeShell,
                )
            }
            (ProfileBindingMode::ForegroundAdaptive, Some(observation)) => (
                {
                    let (selection, evidence) = self.select_foreground_profile(observation)?;
                    matching_evidence = evidence;
                    selection
                },
                EffectiveProfileOrigin::ForegroundProcess,
            ),
        };
        let server_profile = match selection.profile() {
            Some(profile) => self
                .profiles
                .get(profile)
                .map_err(ObservationError::ProfileCatalog)?
                .is_some_and(|entry| entry.profile().archetype() == DetectionArchetype::Server),
            None => false,
        };
        let tcp_listener = observation.as_ref().is_some_and(|observation| {
            observation.tcp_listener() == TcpListenerObservation::Present
        });
        let managed_listener = observation.as_ref().is_some_and(|observation| {
            observation.managed_process_tcp_listener() == TcpListenerObservation::Present
        });
        let tree_listener = observation.as_ref().is_some_and(|observation| {
            observation.managed_tree_tcp_listener() == TcpListenerObservation::Present
        });
        let server_role = match (
            server_profile,
            tcp_listener,
            managed_listener,
            tree_listener,
        ) {
            (true, true, _, _) => Some(ServerRoleOrigin::ProfileAndTcpListener),
            (false, true, _, _) => Some(ServerRoleOrigin::TcpListener),
            (true, false, true, _) => Some(ServerRoleOrigin::ProfileAndManagedProcessTcpListener),
            (false, false, true, _) => Some(ServerRoleOrigin::ManagedProcessTcpListener),
            (true, false, false, true) => Some(ServerRoleOrigin::ProfileAndManagedTreeTcpListener),
            (false, false, false, true) => Some(ServerRoleOrigin::ManagedTreeTcpListener),
            (true, false, false, false) => Some(ServerRoleOrigin::Profile),
            (false, false, false, false) => None,
        };
        let observed_at = observation.as_ref().map_or_else(
            || self.core.clock.now(),
            |observation| observation.observed_at(),
        );
        let transition =
            EffectiveProfileTransition::new(process.id.clone(), selection, origin, observed_at)
                .with_matching_evidence(matching_evidence)
                .with_runtime_context(
                    observation
                        .as_ref()
                        .and_then(|observation| observation.current_directory().cloned()),
                    server_role,
                );
        let (lease, mut current) = self.core.begin(session_id).await?;
        if let Some(reason) = self.staleness(session, process, &current)? {
            drop(lease);
            return Ok(EffectiveContextResolution::Stale {
                reason,
                current_revision: current.revision(),
            });
        }
        let expected_revision = current.revision();
        let committed_at = not_before(observed_at, current.updated_at());
        if let Some(change) = current.retain_restoration_directory(
            observation.as_ref().and_then(|o| o.current_directory()),
            committed_at,
        )? {
            self.core
                .commit(&current, expected_revision, change, committed_at)
                .await?;
            // Continue foreground collection against the revision just committed under
            // this same lease, retaining the exact-process staleness checks above.
            *session = current.clone();
        }
        let previous = self
            .effective_profiles
            .get(session_id, &process.id)
            .map_err(|error| {
                self.core.emit_port_failure(
                    self.core.clock.now(),
                    Some(session_id.clone()),
                    &error,
                );
                ObservationError::EffectiveProfileDirectory(error)
            })?;
        let snapshot = self
            .effective_profiles
            .transition(session_id, transition)
            .map_err(|error| {
                self.core.emit_port_failure(
                    self.core.clock.now(),
                    Some(session_id.clone()),
                    &error,
                );
                ObservationError::EffectiveProfileDirectory(error)
            })?;
        drop(lease);
        let selection_changed = match (&previous, &snapshot) {
            (Some(previous), Some(current)) => {
                previous.selection() != current.selection() || previous.origin() != current.origin()
            }
            (None, Some(_)) => true,
            _ => false,
        };
        if selection_changed {
            self.core.emit(
                DiagnosticEventName::EffectiveProfileChanged,
                DiagnosticLevel::Info,
                observed_at,
                Some(session_id.clone()),
                None,
                None,
            );
            self.record_metric(
                MetricName::EffectiveProfileTransitions,
                1.0,
                ApplicationOperation::ObserveForeground,
            );
            if let Some(reason) = snapshot
                .as_ref()
                .and_then(|current| current.selection().unclassified_reason())
            {
                let level = match reason {
                    undercurrent_domain::UnclassifiedProfileReason::NoMatch => {
                        DiagnosticLevel::Debug
                    }
                    undercurrent_domain::UnclassifiedProfileReason::AwaitingObservation
                    | undercurrent_domain::UnclassifiedProfileReason::Ambiguous
                    | undercurrent_domain::UnclassifiedProfileReason::IncompleteEvidence
                    | undercurrent_domain::UnclassifiedProfileReason::Unavailable => {
                        DiagnosticLevel::Warn
                    }
                };
                self.core.emit(
                    DiagnosticEventName::EffectiveProfileUnclassified,
                    level,
                    observed_at,
                    Some(session_id.clone()),
                    None,
                    None,
                );
                self.record_metric(
                    MetricName::EffectiveProfileUnclassifiedTransitions,
                    1.0,
                    ApplicationOperation::ObserveForeground,
                );
            }
        }
        Ok(EffectiveContextResolution::Current(snapshot))
    }

    fn select_foreground_profile(
        &self,
        observation: &crate::ForegroundProcessGroupObservation,
    ) -> Result<
        (
            EffectiveProfileSelection,
            Vec<undercurrent_domain::ProfileMatchEvidence>,
        ),
        ObservationError,
    > {
        use undercurrent_domain::UnclassifiedProfileReason;
        let incomplete_reason = match observation.completeness() {
            ProcessGroupCompleteness::Unavailable => Some(UnclassifiedProfileReason::Unavailable),
            ProcessGroupCompleteness::Truncated => {
                Some(UnclassifiedProfileReason::IncompleteEvidence)
            }
            ProcessGroupCompleteness::Complete => None,
        };
        if let Some(reason) = incomplete_reason {
            return Ok((EffectiveProfileSelection::Unclassified(reason), Vec::new()));
        }
        let selector =
            DetectionProfileSelector::new(self.profiles, self.patterns, self.max_profiles)
                .map_err(ObservationError::ProfileSelection)?;
        let (outcome, evidence) = selector
            .select_foreground_group(observation.members())
            .map_err(ObservationError::ProfileSelection)?;
        let selection = match outcome {
            crate::profiles::ForegroundProfileMatchOutcome::Determinate(outcome) => match outcome {
                ProfileMatchOutcome::Matched { profile, .. } => {
                    EffectiveProfileSelection::Profile(profile)
                }
                ProfileMatchOutcome::Ambiguous { .. } => {
                    EffectiveProfileSelection::Unclassified(UnclassifiedProfileReason::Ambiguous)
                }
                ProfileMatchOutcome::NoMatch => {
                    EffectiveProfileSelection::Unclassified(UnclassifiedProfileReason::NoMatch)
                }
            },
            crate::profiles::ForegroundProfileMatchOutcome::IncompleteEvidence => {
                EffectiveProfileSelection::Unclassified(
                    UnclassifiedProfileReason::IncompleteEvidence,
                )
            }
        };
        Ok((selection, evidence))
    }

    async fn execute_inner(
        &self,
        session_id: &SessionId,
        expected_revision: Option<Revision>,
    ) -> Result<ObservationOutcome, ObservationError> {
        let (lease, mut sampled) = self.core.begin(session_id).await?;
        drop(lease);
        if let Some(expected_revision) = expected_revision
            && expected_revision != sampled.revision()
        {
            return Ok(self.stale(
                session_id,
                StaleObservationReason::SessionChanged,
                expected_revision,
                sampled.revision(),
                DiagnosticEventName::ForegroundObservationStale,
            ));
        }
        if !sampled.configuration().execution().detection_enabled() {
            return Ok(self.skip(
                session_id,
                ObservationSkipReason::DetectionDisabled,
                DiagnosticEventName::ForegroundObservationSkipped,
            ));
        }
        if sampled.lifecycle() != LifecycleState::Running {
            return Ok(self.skip(
                session_id,
                ObservationSkipReason::NotRunning,
                DiagnosticEventName::ForegroundObservationSkipped,
            ));
        }
        let Some(sampled_process) = self.lookup_process(&sampled)? else {
            return Ok(self.skip(
                session_id,
                ObservationSkipReason::MissingProcessBinding,
                DiagnosticEventName::ForegroundObservationSkipped,
            ));
        };

        let effective = match self
            .resolve_effective_context(session_id, &mut sampled, &sampled_process)
            .await?
        {
            EffectiveContextResolution::Current(effective) => effective,
            EffectiveContextResolution::Stale {
                reason,
                current_revision,
            } => {
                return Ok(self.stale(
                    session_id,
                    reason,
                    sampled.revision(),
                    current_revision,
                    DiagnosticEventName::ForegroundObservationStale,
                ));
            }
        };
        let effective_profile = effective
            .as_ref()
            .and_then(|snapshot| snapshot.selection().profile().cloned())
            .or_else(|| {
                (sampled.configuration().profile_binding() == ProfileBindingMode::Fixed)
                    .then(|| sampled.profile().cloned())
                    .flatten()
            });
        let sampled_profile_epoch = effective.as_ref().map(EffectiveProfileSnapshot::epoch);
        if sampled.configuration().profile_binding() == ProfileBindingMode::ForegroundAdaptive
            && effective.is_none()
        {
            return Ok(self.skip(
                session_id,
                ObservationSkipReason::MissingEffectiveProfile,
                DiagnosticEventName::ForegroundObservationSkipped,
            ));
        }
        let collection_started = self.core.clock.now();
        let evidence = match self
            .collector
            .collect(
                session_id,
                &sampled_process.id,
                effective_profile.as_ref(),
                sampled.foreground(),
            )
            .await
        {
            Ok(evidence) => evidence,
            Err(error) => {
                self.core.emit_port_failure(
                    self.core.clock.now(),
                    Some(session_id.clone()),
                    &error,
                );
                self.core.emit(
                    DiagnosticEventName::ForegroundObservationFailed,
                    DiagnosticLevel::for_port_error(error.kind()),
                    self.core.clock.now(),
                    Some(session_id.clone()),
                    Some(error.operation()),
                    Some(error.kind()),
                );
                return Err(ObservationError::ForegroundCollection(error));
            }
        };
        let collected_at = not_before(self.core.clock.now(), sampled.updated_at());
        self.record_metric(
            MetricName::DetectionDurationSeconds,
            collected_at
                .saturating_duration_since(collection_started)
                .as_secs_f64(),
            ApplicationOperation::ObserveForeground,
        );
        self.core.emit(
            DiagnosticEventName::ForegroundEvidenceCollected,
            DiagnosticLevel::Debug,
            collected_at,
            Some(session_id.clone()),
            Some(PortOperation::CollectForegroundEvidence),
            None,
        );
        let decision = self
            .policy
            .decide(evidence, collected_at)
            .map_err(|error| {
                let level = DiagnosticLevel::for_application_error(
                    ApplicationErrorCategory::from_domain(error.category()),
                );
                self.core.emit(
                    DiagnosticEventName::ForegroundObservationFailed,
                    level,
                    collected_at,
                    Some(session_id.clone()),
                    None,
                    None,
                );
                ObservationError::Domain(error)
            })?;
        if let Some(age) = decision.observation().evidence_age(collected_at) {
            self.record_metric(
                MetricName::DetectionEvidenceAgeSeconds,
                age.as_secs_f64(),
                ApplicationOperation::ObserveForeground,
            );
        }

        let (lease, mut current) = self.core.begin(session_id).await?;
        let stale = self.staleness(&sampled, &sampled_process, &current)?;
        if let Some(reason) = stale {
            drop(lease);
            return Ok(self.stale(
                session_id,
                reason,
                sampled.revision(),
                current.revision(),
                DiagnosticEventName::ForegroundObservationStale,
            ));
        }
        // Effective-profile transitions can change prepared state without a
        // durable session revision change. The revision/process check above
        // therefore cannot detect evidence collected under a superseded profile.
        // See ../docs/internals/observation/observations.md#sample-collect-revalidate.
        if let Some(sampled_profile_epoch) = sampled_profile_epoch {
            let current_profile_epoch = self
                .effective_profiles
                .get(session_id, &sampled_process.id)
                .map_err(ObservationError::EffectiveProfileDirectory)?
                .map(|effective| effective.epoch());
            if current_profile_epoch != Some(sampled_profile_epoch) {
                drop(lease);
                return Ok(self.stale(
                    session_id,
                    StaleObservationReason::ProfileChanged,
                    sampled.revision(),
                    current.revision(),
                    DiagnosticEventName::ForegroundObservationStale,
                ));
            }
        }
        let expected_revision = current.revision();
        let Some(change) = current.update_foreground_observation(decision)? else {
            drop(lease);
            return Ok(ObservationOutcome::Unchanged {
                revision: current.revision(),
            });
        };
        self.core
            .commit(&current, expected_revision, change, collected_at)
            .await?;
        drop(lease);
        self.core.emit(
            DiagnosticEventName::ForegroundObservationCommitted,
            DiagnosticLevel::Info,
            collected_at,
            Some(session_id.clone()),
            None,
            None,
        );
        Ok(ObservationOutcome::Committed {
            revision: current.revision(),
            event: change.kind(),
        })
    }

    fn lookup_process(
        &self,
        session: &Session,
    ) -> Result<Option<crate::ProcessInstance>, ObservationError> {
        self.processes
            .lookup(session.id())
            .map(|binding| {
                binding.filter(|process| {
                    session
                        .process()
                        .is_some_and(|durable| process.matches(durable))
                })
            })
            .map_err(|error| {
                self.core.emit_port_failure(
                    self.core.clock.now(),
                    Some(session.id().clone()),
                    &error,
                );
                ObservationError::ProcessDirectory(error)
            })
    }

    fn staleness(
        &self,
        sampled: &Session,
        sampled_process: &crate::ProcessInstance,
        current: &Session,
    ) -> Result<Option<StaleObservationReason>, ObservationError> {
        if sampled.revision() != current.revision() {
            return Ok(Some(StaleObservationReason::SessionChanged));
        }
        let current_process = self.lookup_process(current)?;
        if current_process.as_ref() != Some(sampled_process) {
            return Ok(Some(StaleObservationReason::ProcessChanged));
        }
        Ok(None)
    }

    fn skip(
        &self,
        session_id: &SessionId,
        reason: ObservationSkipReason,
        event: DiagnosticEventName,
    ) -> ObservationOutcome {
        self.core.emit(
            event,
            if matches!(
                reason,
                ObservationSkipReason::MissingProcessBinding
                    | ObservationSkipReason::MissingEffectiveProfile
            ) {
                DiagnosticLevel::Warn
            } else {
                DiagnosticLevel::Debug
            },
            self.core.clock.now(),
            Some(session_id.clone()),
            None,
            None,
        );
        ObservationOutcome::Skipped(reason)
    }

    fn stale(
        &self,
        session_id: &SessionId,
        reason: StaleObservationReason,
        sampled_revision: Revision,
        current_revision: Revision,
        event: DiagnosticEventName,
    ) -> ObservationOutcome {
        self.core.emit(
            event,
            DiagnosticLevel::Debug,
            self.core.clock.now(),
            Some(session_id.clone()),
            None,
            None,
        );
        ObservationOutcome::Stale {
            reason,
            sampled_revision,
            current_revision,
        }
    }

    fn record_metric(&self, name: MetricName, value: f64, operation: ApplicationOperation) {
        self.telemetry.record_metric(MetricObservation {
            name,
            value,
            operation: Some(operation),
        });
    }
}

/// Executes and commits one independent health observation for one session.
pub struct ObserveHealthService<'a> {
    core: MutationCore<'a>,
    processes: &'a dyn SessionProcessDirectory,
    executor: &'a dyn HealthCheckExecutor,
    telemetry: &'a dyn Telemetry,
}

impl<'a> ObserveHealthService<'a> {
    /// Constructs a health observer without starting a timer or global scan.
    pub const fn new(
        clock: &'a dyn Clock,
        coordinator: &'a dyn SessionMutationCoordinator,
        repository: &'a dyn SessionRepository,
        projection: &'a dyn SessionProjectionWriter,
        processes: &'a dyn SessionProcessDirectory,
        executor: &'a dyn HealthCheckExecutor,
        telemetry: &'a dyn Telemetry,
    ) -> Self {
        Self {
            core: MutationCore::new(
                clock,
                coordinator,
                repository,
                projection,
                telemetry,
                ApplicationOperation::ObserveHealth,
            ),
            processes,
            executor,
            telemetry,
        }
    }

    /// Runs configured health checks outside mutation ownership and commits
    /// their typed aggregate only when sampled state remains current.
    ///
    /// This method does not start a timer, scan unrelated sessions, or panic.
    ///
    /// # Errors
    ///
    /// Returns typed mutation, process-directory, health-execution, or domain
    /// errors.
    pub async fn execute(
        &self,
        session_id: &SessionId,
    ) -> Result<ObservationOutcome, ObservationError> {
        self.execute_inner(session_id, None).await
    }

    /// Executes a queued health observation only when its source revision is
    /// still current.
    ///
    /// Revision validation occurs before process lookup and rule execution, so
    /// superseded jobs do not perform expensive adapter IO. This method does
    /// not start a timer, scan unrelated sessions, or panic.
    ///
    /// # Errors
    ///
    /// Returns typed mutation, process-directory, health-execution, or domain
    /// errors.
    pub async fn execute_scheduled(
        &self,
        session_id: &SessionId,
        expected_revision: Revision,
    ) -> Result<ObservationOutcome, ObservationError> {
        self.execute_inner(session_id, Some(expected_revision))
            .await
    }

    async fn execute_inner(
        &self,
        session_id: &SessionId,
        expected_revision: Option<Revision>,
    ) -> Result<ObservationOutcome, ObservationError> {
        let (lease, sampled) = self.core.begin(session_id).await?;
        drop(lease);
        if let Some(expected_revision) = expected_revision
            && expected_revision != sampled.revision()
        {
            return Ok(self.stale(
                session_id,
                StaleObservationReason::SessionChanged,
                expected_revision,
                sampled.revision(),
            ));
        }
        if sampled.configuration().health_rules().is_empty() {
            return Ok(self.skip(session_id, ObservationSkipReason::NoHealthRules));
        }
        if sampled.lifecycle() != LifecycleState::Running {
            return Ok(self.skip(session_id, ObservationSkipReason::NotRunning));
        }
        let Some(sampled_process) = self.lookup_process(&sampled)? else {
            return Ok(self.skip(session_id, ObservationSkipReason::MissingProcessBinding));
        };

        let check_started = self.core.clock.now();
        let results = match self
            .executor
            .run(
                session_id,
                &sampled_process.id,
                sampled.configuration().health_rules(),
            )
            .await
        {
            Ok(results) => results,
            Err(error) => {
                self.core.emit_port_failure(
                    self.core.clock.now(),
                    Some(session_id.clone()),
                    &error,
                );
                self.core.emit(
                    DiagnosticEventName::HealthObservationFailed,
                    DiagnosticLevel::for_port_error(error.kind()),
                    self.core.clock.now(),
                    Some(session_id.clone()),
                    Some(error.operation()),
                    Some(error.kind()),
                );
                return Err(ObservationError::HealthExecution(error));
            }
        };
        let observed_at = not_before(self.core.clock.now(), sampled.updated_at());
        self.record_metric(
            MetricName::HealthCheckDurationSeconds,
            observed_at
                .saturating_duration_since(check_started)
                .as_secs_f64(),
        );
        let passed_results = results.iter().filter(|result| result.passed()).count();
        self.record_metric(MetricName::HealthCheckResults, results.len() as f64);
        self.record_metric(MetricName::HealthCheckResultsPassed, passed_results as f64);
        self.record_metric(
            MetricName::HealthCheckResultsFailed,
            results.len().saturating_sub(passed_results) as f64,
        );
        self.core.emit(
            DiagnosticEventName::HealthChecksCompleted,
            DiagnosticLevel::Debug,
            observed_at,
            Some(session_id.clone()),
            Some(PortOperation::RunHealthChecks),
            None,
        );
        let configured_checks = u16::try_from(sampled.configuration().health_rules().len())
            .map_err(|_| DomainError::CapacityExceeded {
                resource: "health_rules",
                limit: usize::from(u16::MAX),
            })?;
        let observation = HealthObservation::evaluate(configured_checks, results, observed_at)
            .map_err(|error| {
                let level = DiagnosticLevel::for_application_error(
                    ApplicationErrorCategory::from_domain(error.category()),
                );
                self.core.emit(
                    DiagnosticEventName::HealthObservationFailed,
                    level,
                    observed_at,
                    Some(session_id.clone()),
                    None,
                    None,
                );
                ObservationError::Domain(error)
            })?;

        let (lease, mut current) = self.core.begin(session_id).await?;
        if sampled.revision() != current.revision() {
            drop(lease);
            return Ok(self.stale(
                session_id,
                StaleObservationReason::SessionChanged,
                sampled.revision(),
                current.revision(),
            ));
        }
        if self.lookup_process(&current)?.as_ref() != Some(&sampled_process) {
            drop(lease);
            return Ok(self.stale(
                session_id,
                StaleObservationReason::ProcessChanged,
                sampled.revision(),
                current.revision(),
            ));
        }
        let expected_revision = current.revision();
        let Some(change) = current.update_health_observation(observation, observed_at)? else {
            drop(lease);
            return Ok(ObservationOutcome::Unchanged {
                revision: current.revision(),
            });
        };
        self.core
            .commit(&current, expected_revision, change, observed_at)
            .await?;
        drop(lease);
        self.core.emit(
            DiagnosticEventName::HealthObservationCommitted,
            DiagnosticLevel::Info,
            observed_at,
            Some(session_id.clone()),
            None,
            None,
        );
        Ok(ObservationOutcome::Committed {
            revision: current.revision(),
            event: change.kind(),
        })
    }

    fn lookup_process(
        &self,
        session: &Session,
    ) -> Result<Option<crate::ProcessInstance>, ObservationError> {
        self.processes
            .lookup(session.id())
            .map(|binding| {
                binding.filter(|process| {
                    session
                        .process()
                        .is_some_and(|durable| process.matches(durable))
                })
            })
            .map_err(|error| {
                self.core.emit_port_failure(
                    self.core.clock.now(),
                    Some(session.id().clone()),
                    &error,
                );
                ObservationError::ProcessDirectory(error)
            })
    }

    fn skip(&self, session_id: &SessionId, reason: ObservationSkipReason) -> ObservationOutcome {
        self.core.emit(
            DiagnosticEventName::HealthObservationSkipped,
            if reason == ObservationSkipReason::MissingProcessBinding {
                DiagnosticLevel::Warn
            } else {
                DiagnosticLevel::Debug
            },
            self.core.clock.now(),
            Some(session_id.clone()),
            None,
            None,
        );
        ObservationOutcome::Skipped(reason)
    }

    fn stale(
        &self,
        session_id: &SessionId,
        reason: StaleObservationReason,
        sampled_revision: Revision,
        current_revision: Revision,
    ) -> ObservationOutcome {
        self.core.emit(
            DiagnosticEventName::HealthObservationStale,
            DiagnosticLevel::Debug,
            self.core.clock.now(),
            Some(session_id.clone()),
            None,
            None,
        );
        ObservationOutcome::Stale {
            reason,
            sampled_revision,
            current_revision,
        }
    }

    fn record_metric(&self, name: MetricName, value: f64) {
        self.telemetry.record_metric(MetricObservation {
            name,
            value,
            operation: Some(ApplicationOperation::ObserveHealth),
        });
    }
}
