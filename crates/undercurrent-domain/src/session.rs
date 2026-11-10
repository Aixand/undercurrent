// SPDX-License-Identifier: Apache-2.0

//! Managed session aggregate and compact read projection.

use std::time::Duration;

use crate::{
    DomainError, DomainEventKind, DomainResult, EffectiveProfileEpoch, EffectiveProfileSelection,
    EffectiveProfileSnapshot, EffectiveProfileTransition, ForegroundDecision,
    ForegroundObservation, HealthObservation, HealthSummary, Labels, LifecycleState, Metadata,
    NoRestartReason, PathReference, ProcessIdentity, ProcessInstanceId, ProcessOutcome,
    ProfileBindingMode, ProfileName, ProfileSelectionOrigin, RestartDecision, RestartStatus,
    RestartTracker, Revision, SessionConfiguration, SessionId, SessionName, TerminalSize,
    TimestampMillis, UnclassifiedProfileReason, WorkContext, WorkItem, WorkItemKey, WorkSummary,
};

/// Description of an in-memory aggregate change intended for durable commit.
///
/// The application must persist the revised aggregate and matching outbox event.
/// Constructing or receiving this value does not establish persistence success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DomainChange {
    kind: DomainEventKind,
    revision: Revision,
}

impl DomainChange {
    /// Constructs a change emitted by a validated aggregate mutation.
    ///
    /// This function performs no IO, cannot fail, and does not panic.
    pub const fn new(kind: DomainEventKind, revision: Revision) -> Self {
        Self { kind, revision }
    }

    /// Returns the centrally registered event kind.
    pub const fn kind(self) -> DomainEventKind {
        self.kind
    }

    /// Returns the session revision after the change.
    pub const fn revision(self) -> Revision {
        self.revision
    }
}

/// Complete typed state required to reconstruct one durable session aggregate.
///
/// Persistence adapters may encode these already-validated domain values, but
/// must pass decoded state through [`Session::rehydrate`] before exposing it to
/// application code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionState {
    /// Stable session identity.
    pub id: SessionId,
    /// Current human-facing name.
    pub name: SessionName,
    /// Authoritative process lifecycle.
    pub lifecycle: LifecycleState,
    /// Compact committed foreground.
    pub foreground: ForegroundObservation,
    /// Complete bounded foreground rationale.
    pub foreground_decision: ForegroundDecision,
    /// Compact committed health state.
    pub health: HealthSummary,
    /// Complete bounded health results.
    pub health_observation: HealthObservation,
    /// Complete bounded declared work context.
    pub work_context: WorkContext,
    /// Durable process launch and observation configuration.
    pub configuration: SessionConfiguration,
    /// Last observed root directory retained only for group-created terminals.
    pub restoration_directory: Option<PathReference>,
    /// Exact owned process incarnation when lifecycle expects one.
    pub process: Option<ProcessIdentity>,
    /// Most recent aggregate commit time.
    pub updated_at: TimestampMillis,
    /// Consecutive automatic restart history.
    pub restart_tracker: RestartTracker,
    /// Durable next automatic restart disposition.
    pub restart_status: RestartStatus,
    /// Current monotonic aggregate revision.
    pub revision: Revision,
    /// Revision of the most recent authoritative lifecycle transition.
    pub lifecycle_revision: Revision,
}

/// Aggregate root for one durable managed-process session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    id: SessionId,
    name: SessionName,
    lifecycle: LifecycleState,
    foreground: ForegroundObservation,
    foreground_decision: ForegroundDecision,
    health: HealthSummary,
    health_observation: HealthObservation,
    work_context: WorkContext,
    configuration: SessionConfiguration,
    restoration_directory: Option<PathReference>,
    process: Option<ProcessIdentity>,
    updated_at: TimestampMillis,
    restart_tracker: RestartTracker,
    restart_status: RestartStatus,
    revision: Revision,
    lifecycle_revision: Revision,
}

impl Session {
    /// Creates a session value in `starting` lifecycle at revision one.
    ///
    /// The caller supplies identity and time through application ports. This
    /// function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns validation failure when the WorkContext limit is zero or the
    /// initial revision cannot be represented.
    pub fn create(
        id: SessionId,
        name: SessionName,
        configuration: SessionConfiguration,
        max_work_items: usize,
        created_at: TimestampMillis,
    ) -> DomainResult<(Self, DomainChange)> {
        let revision = Revision::INITIAL.next("session")?;
        let health = initial_health(&configuration)?;
        let health_observation = HealthObservation::from_summary(health.clone());
        let session = Self {
            id,
            name,
            lifecycle: LifecycleState::Starting,
            foreground: ForegroundObservation::unknown(created_at),
            foreground_decision: ForegroundDecision::unknown(created_at),
            health,
            health_observation,
            work_context: WorkContext::new(max_work_items)?,
            configuration,
            restoration_directory: None,
            process: None,
            updated_at: created_at,
            restart_tracker: RestartTracker::default(),
            restart_status: RestartStatus::Idle,
            revision,
            lifecycle_revision: revision,
        };
        Ok((
            session,
            DomainChange::new(DomainEventKind::SessionCreated, revision),
        ))
    }

    /// Rehydrates a complete aggregate from typed persisted state.
    ///
    /// This is the only supported persistence reconstruction boundary. It
    /// validates cross-field lifecycle, observation, WorkContext, restart,
    /// timestamp, and revision invariants before returning the aggregate. This
    /// function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns corrupt-state failure when independently valid fields cannot
    /// represent a reachable session aggregate.
    pub fn rehydrate(state: SessionState) -> DomainResult<Self> {
        validate_session_state(&state)?;
        Ok(Self {
            id: state.id,
            name: state.name,
            lifecycle: state.lifecycle,
            foreground: state.foreground,
            foreground_decision: state.foreground_decision,
            health: state.health,
            health_observation: state.health_observation,
            work_context: state.work_context,
            configuration: state.configuration,
            restoration_directory: state.restoration_directory,
            process: state.process,
            updated_at: state.updated_at,
            restart_tracker: state.restart_tracker,
            restart_status: state.restart_status,
            revision: state.revision,
            lifecycle_revision: state.lifecycle_revision,
        })
    }

    /// Clones the complete typed durable state for a persistence adapter.
    ///
    /// Private environment and metadata values remain protected by their
    /// redacted `Debug` implementations. This function performs no IO and does
    /// not panic.
    pub fn state(&self) -> SessionState {
        SessionState {
            id: self.id.clone(),
            name: self.name.clone(),
            lifecycle: self.lifecycle,
            foreground: self.foreground.clone(),
            foreground_decision: self.foreground_decision.clone(),
            health: self.health.clone(),
            health_observation: self.health_observation.clone(),
            work_context: self.work_context.clone(),
            configuration: self.configuration.clone(),
            restoration_directory: self.restoration_directory.clone(),
            process: self.process.clone(),
            updated_at: self.updated_at,
            restart_tracker: self.restart_tracker,
            restart_status: self.restart_status,
            revision: self.revision,
            lifecycle_revision: self.lifecycle_revision,
        }
    }

    /// Returns the stable session identity.
    pub const fn id(&self) -> &SessionId {
        &self.id
    }

    /// Returns the human-facing session name.
    pub const fn name(&self) -> &SessionName {
        &self.name
    }

    /// Returns authoritative process lifecycle.
    pub const fn lifecycle(&self) -> LifecycleState {
        self.lifecycle
    }

    /// Returns the current best-effort foreground observation.
    pub const fn foreground(&self) -> &ForegroundObservation {
        &self.foreground
    }

    /// Returns the complete bounded foreground decision and evidence rationale.
    pub const fn foreground_decision(&self) -> &ForegroundDecision {
        &self.foreground_decision
    }

    /// Returns compact health state.
    pub const fn health(&self) -> &HealthSummary {
        &self.health
    }

    /// Returns complete bounded results from the latest health evaluation.
    pub const fn health_observation(&self) -> &HealthObservation {
        &self.health_observation
    }

    /// Returns complete WorkContext to an authorized detail use case.
    pub const fn work_context(&self) -> &WorkContext {
        &self.work_context
    }

    /// Returns the selected detection profile, if present.
    pub const fn profile(&self) -> Option<&ProfileName> {
        self.configuration.profile()
    }

    /// Returns session labels.
    pub const fn labels(&self) -> &Labels {
        self.configuration.labels()
    }

    /// Returns private metadata to an explicitly authorized detail caller.
    pub const fn metadata(&self) -> &Metadata {
        self.configuration.metadata()
    }

    /// Returns complete durable launch and execution configuration.
    pub const fn configuration(&self) -> &SessionConfiguration {
        &self.configuration
    }

    /// Returns the current durable PTY dimensions used by future restarts.
    pub const fn terminal_size(&self) -> TerminalSize {
        self.configuration.launch().terminal_size()
    }

    /// Returns durable concrete process identity while one is owned.
    pub const fn process(&self) -> Option<&ProcessIdentity> {
        self.process.as_ref()
    }

    /// Returns when the current process instance was established, if owned.
    pub const fn started_at(&self) -> Option<TimestampMillis> {
        match &self.process {
            Some(process) => Some(process.started_at()),
            None => None,
        }
    }

    /// Returns the most recent durable domain update time.
    pub const fn updated_at(&self) -> TimestampMillis {
        self.updated_at
    }

    /// Returns automatic restart tracking state.
    pub const fn restart_tracker(&self) -> RestartTracker {
        self.restart_tracker
    }

    /// Returns the durable disposition of the latest restart decision.
    pub const fn restart_status(&self) -> RestartStatus {
        self.restart_status
    }

    /// Returns the current durable session revision.
    pub const fn revision(&self) -> Revision {
        self.revision
    }

    /// Returns the revision of the latest authoritative lifecycle transition.
    pub const fn lifecycle_revision(&self) -> Revision {
        self.lifecycle_revision
    }

    /// Transitions authoritative lifecycle and returns a revisioned change.
    ///
    /// Repeating the current state is an idempotent no-op. Entering `running`
    /// or `stopping` requires ownership established by
    /// [`Self::record_process_ownership`]; it does not set the process start time.
    /// Terminal states clear ownership. Entering `starting` resets the restart
    /// disposition and the prior stop's no-escalation constraint. This function
    /// performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns an invalid transition, missing-ownership precondition, backward
    /// commit timestamp, or revision exhaustion.
    pub fn transition_lifecycle(
        &mut self,
        next: LifecycleState,
        changed_at: TimestampMillis,
    ) -> DomainResult<Option<DomainChange>> {
        self.lifecycle.validate_transition(next)?;
        if matches!(next, LifecycleState::Running | LifecycleState::Stopping)
            && self.process.is_none()
        {
            return Err(crate::DomainError::LifecyclePrecondition {
                operation: "enter_active_lifecycle_without_process_ownership",
                lifecycle: self.lifecycle,
            });
        }
        if self.lifecycle == next {
            return Ok(None);
        }
        let revision = self.next_revision_at(changed_at)?;
        self.lifecycle = next;
        if next.is_terminal() {
            self.process = None;
        }
        if next == LifecycleState::Starting {
            self.restart_status = RestartStatus::Idle;
            // The no-escalation constraint belongs to the previous stop intent,
            // not to subsequent process incarnations.
            self.configuration = self.configuration.clone().with_stop_no_escalation(false);
        }
        self.updated_at = changed_at;
        self.revision = revision;
        self.lifecycle_revision = revision;
        Ok(Some(DomainChange::new(
            DomainEventKind::LifecycleChanged,
            revision,
        )))
    }

    /// Records ownership of one concrete process while the session is starting.
    ///
    /// Repeating the exact identity is idempotent. A different identity cannot
    /// replace an owned process implicitly. Process start time remains the
    /// adapter observation and may precede `changed_at`. This function performs
    /// no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a lifecycle precondition outside `starting` or when a
    /// different process is already owned, stale timestamp rejection, or
    /// revision exhaustion.
    pub fn record_process_ownership(
        &mut self,
        process: ProcessIdentity,
        changed_at: TimestampMillis,
    ) -> DomainResult<Option<DomainChange>> {
        if self.lifecycle != LifecycleState::Starting {
            return Err(crate::DomainError::LifecyclePrecondition {
                operation: "record_process_ownership",
                lifecycle: self.lifecycle,
            });
        }
        if self.process.as_ref() == Some(&process) {
            return Ok(None);
        }
        if self.process.is_some() {
            return Err(crate::DomainError::LifecyclePrecondition {
                operation: "replace_process_ownership",
                lifecycle: self.lifecycle,
            });
        }
        if process.started_at() > changed_at {
            return Err(crate::DomainError::InvalidValue {
                field: "process_started_at",
                reason: crate::InvalidValueReason::InvalidCombination {
                    rule: "process start time cannot follow its ownership commit",
                },
            });
        }
        let revision = self.next_revision_at(changed_at)?;
        self.process = Some(process);
        self.updated_at = changed_at;
        self.revision = revision;
        Ok(Some(DomainChange::new(
            DomainEventKind::ProcessOwnershipChanged,
            revision,
        )))
    }

    /// Renames the session without changing its stable ID.
    ///
    /// Repeating the name is an idempotent no-op. This function performs no IO
    /// and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a backward commit timestamp rejection or revision exhaustion.
    pub fn rename(
        &mut self,
        name: SessionName,
        changed_at: TimestampMillis,
    ) -> DomainResult<Option<DomainChange>> {
        if self.name == name {
            return Ok(None);
        }
        let revision = self.next_revision_at(changed_at)?;
        self.name = name;
        self.updated_at = changed_at;
        self.revision = revision;
        Ok(Some(DomainChange::new(
            DomainEventKind::SessionRenamed,
            revision,
        )))
    }

    /// Replaces the committed foreground observation.
    ///
    /// Identical content is an idempotent no-op. WorkContext is intentionally
    /// untouched. This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a backward foreground commit timestamp rejection or revision
    /// exhaustion.
    pub fn update_foreground(
        &mut self,
        foreground: ForegroundObservation,
    ) -> DomainResult<Option<DomainChange>> {
        if self.foreground == foreground {
            return Ok(None);
        }
        let revision = self.next_revision_at(foreground.committed_at())?;
        self.updated_at = foreground.committed_at();
        self.foreground_decision = ForegroundDecision::from_observation(foreground.clone());
        self.foreground = foreground;
        self.revision = revision;
        Ok(Some(DomainChange::new(
            DomainEventKind::ForegroundChanged,
            revision,
        )))
    }

    /// Replaces foreground and its complete bounded evidence rationale together.
    ///
    /// Evidence-only changes advance the durable revision without pretending
    /// the foreground observation transitioned. WorkContext remains untouched.
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns revision exhaustion or a stale commit timestamp.
    pub fn update_foreground_observation(
        &mut self,
        decision: ForegroundDecision,
    ) -> DomainResult<Option<DomainChange>> {
        if self.foreground_decision == decision {
            return Ok(None);
        }
        let changed_at = decision.observation().committed_at();
        let revision = self.next_revision_at(changed_at)?;
        let event = if self.foreground == *decision.observation() {
            DomainEventKind::ForegroundEvidenceChanged
        } else {
            DomainEventKind::ForegroundChanged
        };
        self.foreground = decision.observation().clone();
        self.foreground_decision = decision;
        self.updated_at = changed_at;
        self.revision = revision;
        Ok(Some(DomainChange::new(event, revision)))
    }

    /// Replaces compact health state without changing lifecycle or foreground.
    ///
    /// Identical content is an idempotent no-op. This function performs no IO
    /// and does not panic.
    ///
    /// # Errors
    ///
    /// Returns validation failure when the check count disagrees with the
    /// configuration or the observation follows `changed_at`, a backward
    /// commit timestamp rejection, or revision exhaustion.
    pub fn update_health(
        &mut self,
        health: HealthSummary,
        changed_at: TimestampMillis,
    ) -> DomainResult<Option<DomainChange>> {
        validate_health_commit(&self.configuration, &health, changed_at)?;
        if self.health == health {
            return Ok(None);
        }
        let revision = self.next_revision_at(changed_at)?;
        self.health_observation = HealthObservation::from_summary(health.clone());
        self.health = health;
        self.updated_at = changed_at;
        self.revision = revision;
        Ok(Some(DomainChange::new(
            DomainEventKind::HealthChanged,
            revision,
        )))
    }

    /// Replaces compact health and complete bounded check results together.
    ///
    /// Detail-only changes advance the durable revision using a distinct event.
    /// Lifecycle and foreground remain unchanged. This function performs no IO
    /// and does not panic.
    ///
    /// # Errors
    ///
    /// Returns validation failure when the check count disagrees with the
    /// configuration or the observation follows `changed_at`, a backward
    /// commit timestamp rejection, or revision exhaustion.
    pub fn update_health_observation(
        &mut self,
        observation: HealthObservation,
        changed_at: TimestampMillis,
    ) -> DomainResult<Option<DomainChange>> {
        validate_health_commit(&self.configuration, observation.summary(), changed_at)?;
        if self.health_observation == observation {
            return Ok(None);
        }
        let revision = self.next_revision_at(changed_at)?;
        let event = if self.health == *observation.summary() {
            DomainEventKind::HealthEvidenceChanged
        } else {
            DomainEventKind::HealthChanged
        };
        self.health = observation.summary().clone();
        self.health_observation = observation;
        self.updated_at = changed_at;
        self.revision = revision;
        Ok(Some(DomainChange::new(event, revision)))
    }

    /// Replaces the complete configuration as one in-memory domain change.
    ///
    /// Changing health rules resets compact and detailed health to initial state.
    /// Persistence and any effects of the new configuration belong to callers.
    ///
    /// Identical content is an idempotent no-op. This function performs no IO
    /// and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a backward commit timestamp rejection or revision exhaustion.
    pub fn update_configuration(
        &mut self,
        configuration: SessionConfiguration,
        changed_at: TimestampMillis,
    ) -> DomainResult<Option<DomainChange>> {
        if self.configuration == configuration {
            return Ok(None);
        }
        let revision = self.next_revision_at(changed_at)?;
        if self.configuration.health_rules() != configuration.health_rules() {
            self.health = initial_health(&configuration)?;
            self.health_observation = HealthObservation::from_summary(self.health.clone());
        }
        self.configuration = configuration;
        self.updated_at = changed_at;
        self.revision = revision;
        Ok(Some(DomainChange::new(
            DomainEventKind::SessionConfigurationChanged,
            revision,
        )))
    }

    /// Last root working directory observed for a persistent-group terminal.
    pub const fn restoration_directory(&self) -> Option<&PathReference> {
        self.restoration_directory.as_ref()
    }

    /// Retains observed directory evidence only for group-created terminal slots.
    /// Missing observations and identical directories leave durable state unchanged.
    ///
    /// # Errors
    /// Returns invalid timestamp or revision overflow errors.
    pub fn retain_restoration_directory(
        &mut self,
        directory: Option<&PathReference>,
        observed_at: TimestampMillis,
    ) -> DomainResult<Option<DomainChange>> {
        if self.labels().get("uc.group.slot").is_none() {
            return Ok(None);
        }
        let Some(directory) = directory else {
            return Ok(None);
        };
        if self.restoration_directory.as_ref() == Some(directory) {
            return Ok(None);
        }
        let revision = self.next_revision_at(observed_at)?;
        self.restoration_directory = Some(directory.clone());
        self.updated_at = observed_at;
        self.revision = revision;
        Ok(Some(DomainChange::new(
            DomainEventKind::ForegroundEvidenceChanged,
            revision,
        )))
    }

    /// Changes current PTY dimensions and advances the session revision.
    ///
    /// Identical dimensions are an idempotent no-op. Accepted dimensions also
    /// update the launch specification so a later restart reproduces the
    /// current terminal. This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a stale-timestamp rejection or revision exhaustion. Dimension
    /// bounds are enforced when [`TerminalSize`] is constructed.
    pub fn resize_terminal(
        &mut self,
        terminal_size: TerminalSize,
        changed_at: TimestampMillis,
    ) -> DomainResult<Option<DomainChange>> {
        if self.terminal_size() == terminal_size {
            return Ok(None);
        }
        let revision = self.next_revision_at(changed_at)?;
        self.configuration.set_terminal_size(terminal_size);
        self.updated_at = changed_at;
        self.revision = revision;
        Ok(Some(DomainChange::new(
            DomainEventKind::TerminalResized,
            revision,
        )))
    }

    /// Inserts or replaces one WorkContext declaration.
    ///
    /// Identical content is an idempotent no-op. Foreground remains
    /// unchanged. This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns WorkContext validation/capacity failures, a backward commit
    /// timestamp rejection, an item update later than its commit, or revision
    /// exhaustion. Rejection leaves both session and WorkContext unchanged.
    pub fn upsert_work_item(
        &mut self,
        item: WorkItem,
        changed_at: TimestampMillis,
    ) -> DomainResult<Option<DomainChange>> {
        if item.updated_at() > changed_at {
            return Err(crate::DomainError::InvalidValue {
                field: "work_updated_at",
                reason: crate::InvalidValueReason::InvalidCombination {
                    rule: "work item update time cannot follow its session commit",
                },
            });
        }
        if self.work_context.get(item.key()) == Some(&item) {
            return Ok(None);
        }
        let revision = self.next_revision_at(changed_at)?;
        if !self.work_context.upsert(item)? {
            return Ok(None);
        }
        self.updated_at = changed_at;
        self.revision = revision;
        Ok(Some(DomainChange::new(
            DomainEventKind::WorkContextChanged,
            revision,
        )))
    }

    /// Removes one WorkContext declaration and returns it with the change.
    ///
    /// Foreground remains unchanged. This function performs no IO and
    /// does not panic.
    ///
    /// # Errors
    ///
    /// Returns `WorkItemNotFound`, a backward commit timestamp rejection, or
    /// revision exhaustion.
    pub fn remove_work_item(
        &mut self,
        key: &WorkItemKey,
        changed_at: TimestampMillis,
    ) -> DomainResult<(WorkItem, DomainChange)> {
        if self.work_context.get(key).is_none() {
            return Err(crate::DomainError::WorkItemNotFound);
        }
        let revision = self.next_revision_at(changed_at)?;
        let removed = self.work_context.remove(key)?;
        self.updated_at = changed_at;
        self.revision = revision;
        Ok((
            removed,
            DomainChange::new(DomainEventKind::WorkContextChanged, revision),
        ))
    }

    /// Expires eligible WorkContext declarations at the supplied time.
    ///
    /// Returns changed keys and an optional session change. Foreground
    /// remains unchanged. This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a backward commit timestamp rejection or revision exhaustion
    /// when an expiry change would occur.
    pub fn expire_work_items(
        &mut self,
        now: TimestampMillis,
    ) -> DomainResult<(Vec<WorkItemKey>, Option<DomainChange>)> {
        if !self.work_context.has_expired_items(now) {
            return Ok((Vec::new(), None));
        }
        let revision = self.next_revision_at(now)?;
        let expired = self.work_context.expire(now)?;
        self.updated_at = now;
        self.revision = revision;
        Ok((
            expired,
            Some(DomainChange::new(
                DomainEventKind::WorkContextChanged,
                revision,
            )),
        ))
    }

    /// Prepares a revisioned removal event for a terminal session.
    ///
    /// The aggregate is not mutated because a successful repository operation
    /// removes it atomically with the returned outbox event. This function
    /// performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a failed precondition for an active lifecycle, a stale timestamp
    /// rejection, or revision exhaustion.
    pub fn prepare_removal(&self, changed_at: TimestampMillis) -> DomainResult<DomainChange> {
        if !self.lifecycle.is_terminal() {
            return Err(crate::DomainError::LifecyclePrecondition {
                operation: "remove_session",
                lifecycle: self.lifecycle,
            });
        }
        let revision = self.next_revision_at(changed_at)?;
        Ok(DomainChange::new(DomainEventKind::SessionRemoved, revision))
    }

    /// Returns uptime of the current running process at `now`.
    ///
    /// Non-running sessions return `None`; reversed timestamps saturate to zero.
    /// This function performs no IO and does not panic.
    pub fn uptime(&self, now: TimestampMillis) -> Option<Duration> {
        if self.lifecycle != LifecycleState::Running {
            return None;
        }
        self.process
            .as_ref()
            .map(|process| now.saturating_duration_since(process.started_at()))
    }

    /// Applies restart policy to an observed process outcome.
    ///
    /// Requires terminal lifecycle without process ownership. The caller supplies
    /// verified outcome and uptime and must avoid evaluating the same exit twice:
    /// another eligible evaluation can consume another restart attempt.
    ///
    /// The restart tracker and next-action status are changed atomically with
    /// the session revision. This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a lifecycle precondition, backward commit timestamp, or revision
    /// exhaustion when changed restart state cannot be represented.
    pub fn observe_process_outcome(
        &mut self,
        outcome: ProcessOutcome,
        uptime: Duration,
        changed_at: TimestampMillis,
    ) -> DomainResult<(RestartDecision, Option<DomainChange>)> {
        if !self.lifecycle.is_terminal() || self.process.is_some() {
            return Err(crate::DomainError::LifecyclePrecondition {
                operation: "observe_process_outcome",
                lifecycle: self.lifecycle,
            });
        }
        let mut tracker = self.restart_tracker;
        let decision = tracker.observe(self.configuration.execution().restart(), outcome, uptime);
        let status = RestartStatus::from_decision(decision, changed_at);
        if tracker == self.restart_tracker && status == self.restart_status {
            return Ok((decision, None));
        }
        let revision = self.next_revision_at(changed_at)?;
        self.restart_tracker = tracker;
        self.restart_status = status;
        self.updated_at = changed_at;
        self.revision = revision;
        let kind = match decision {
            RestartDecision::RestartAfter { .. } => DomainEventKind::RestartScheduled,
            RestartDecision::Exhausted { .. } => DomainEventKind::RestartExhausted,
            RestartDecision::DoNotRestart(_) => DomainEventKind::RestartStateChanged,
        };
        Ok((decision, Some(DomainChange::new(kind, revision))))
    }

    /// Cancels a pending automatic restart after an explicit operator stop.
    ///
    /// Only a scheduled disposition changes. Repeated cancellation and every
    /// other restart disposition are idempotent no-ops. Consecutive-attempt
    /// history is retained for diagnostics. This function performs no IO and
    /// does not panic.
    ///
    /// # Errors
    ///
    /// Returns a stale timestamp rejection or revision exhaustion.
    pub fn cancel_pending_restart(
        &mut self,
        changed_at: TimestampMillis,
    ) -> DomainResult<Option<DomainChange>> {
        if !matches!(self.restart_status, RestartStatus::Scheduled { .. }) {
            return Ok(None);
        }
        let revision = self.next_revision_at(changed_at)?;
        self.restart_status = RestartStatus::NotScheduled {
            reason: NoRestartReason::ExplicitStop,
        };
        self.updated_at = changed_at;
        self.revision = revision;
        Ok(Some(DomainChange::new(
            DomainEventKind::RestartCancelled,
            revision,
        )))
    }

    /// Builds an owned compact read projection without detail hydration or IO.
    pub fn summary(&self) -> SessionSummary {
        let base_profile = self.configuration.profile().cloned();
        let profile_binding = self.configuration.profile_binding();
        SessionSummary {
            id: self.id.clone(),
            name: self.name.clone(),
            lifecycle: self.lifecycle,
            foreground: self.foreground.clone(),
            health: self.health.clone(),
            work: self.work_context.summary(),
            profile: (profile_binding == ProfileBindingMode::Fixed)
                .then(|| base_profile.clone())
                .flatten(),
            base_profile,
            profile_origin: self.configuration.profile_origin(),
            profile_binding,
            effective_profile: None,
            process: self.process.as_ref().map(|process| process.id().clone()),
            labels: self.configuration.labels().clone(),
            started_at: self.started_at(),
            updated_at: self.updated_at,
            restart_count: self.restart_tracker.consecutive_attempts(),
            restart_status: self.restart_status,
            revision: self.revision,
            lifecycle_revision: self.lifecycle_revision,
        }
    }

    fn next_revision_at(&self, changed_at: TimestampMillis) -> DomainResult<Revision> {
        if changed_at < self.updated_at {
            return Err(crate::DomainError::InvalidValue {
                field: "changed_at",
                reason: crate::InvalidValueReason::InvalidCombination {
                    rule: "session updates must not move backward in time",
                },
            });
        }
        self.revision.next("session")
    }
}

fn initial_health(configuration: &SessionConfiguration) -> DomainResult<HealthSummary> {
    let configured_checks = configuration.health_rules().len();
    if configured_checks == 0 {
        return Ok(HealthSummary::unconfigured());
    }
    HealthSummary::new(
        crate::HealthStatus::Unknown,
        u16::try_from(configured_checks).map_err(|_| crate::DomainError::CapacityExceeded {
            resource: "health_rules",
            limit: usize::from(u16::MAX),
        })?,
        None,
    )
}

fn validate_session_state(state: &SessionState) -> DomainResult<()> {
    if state.revision == Revision::INITIAL {
        return Err(corrupt_session("session revision must be non-zero"));
    }
    if state.lifecycle_revision == Revision::INITIAL {
        return Err(corrupt_session("lifecycle revision must be non-zero"));
    }
    if state.lifecycle_revision > state.revision {
        return Err(corrupt_session(
            "lifecycle revision cannot exceed session revision",
        ));
    }
    if state.foreground != *state.foreground_decision.observation() {
        return Err(corrupt_session(
            "compact foreground must match the detailed foreground decision",
        ));
    }
    if state.health != *state.health_observation.summary() {
        return Err(corrupt_session(
            "compact health must match the detailed health observation",
        ));
    }
    validate_health_commit(&state.configuration, &state.health, state.updated_at)
        .map_err(|_| corrupt_session("health state must match configuration and commit time"))?;
    if state.work_context.revision() > state.revision {
        return Err(corrupt_session(
            "WorkContext revision cannot exceed session revision",
        ));
    }
    if state
        .work_context
        .items()
        .any(|item| item.updated_at() > state.updated_at)
    {
        return Err(corrupt_session(
            "WorkContext item update cannot follow the session commit",
        ));
    }
    if state.foreground.committed_at() > state.updated_at {
        return Err(corrupt_session(
            "foreground commit cannot follow the session commit",
        ));
    }
    if state
        .process
        .as_ref()
        .is_some_and(|process| process.started_at() > state.updated_at)
    {
        return Err(corrupt_session(
            "process start cannot follow the session commit",
        ));
    }
    match state.lifecycle {
        LifecycleState::Running | LifecycleState::Stopping if state.process.is_none() => {
            return Err(corrupt_session(
                "running and stopping lifecycle require process ownership",
            ));
        }
        lifecycle if lifecycle.is_terminal() && state.process.is_some() => {
            return Err(corrupt_session(
                "terminal lifecycle cannot retain process ownership",
            ));
        }
        _ => {}
    }
    if state.lifecycle.is_active() && state.restart_status != RestartStatus::Idle {
        return Err(corrupt_session(
            "active lifecycle cannot retain a terminal restart disposition",
        ));
    }
    match state.restart_status {
        RestartStatus::Scheduled { attempt, .. } => {
            if !state.lifecycle.is_terminal()
                || attempt == 0
                || attempt != state.restart_tracker.consecutive_attempts()
            {
                return Err(corrupt_session(
                    "scheduled restart must match terminal lifecycle and attempt history",
                ));
            }
        }
        RestartStatus::Exhausted { attempts } => {
            if !state.lifecycle.is_terminal()
                || attempts != state.restart_tracker.consecutive_attempts()
            {
                return Err(corrupt_session(
                    "exhausted restart must match terminal lifecycle and attempt history",
                ));
            }
        }
        RestartStatus::NotScheduled { .. } if !state.lifecycle.is_terminal() => {
            return Err(corrupt_session(
                "final restart disposition requires terminal lifecycle",
            ));
        }
        RestartStatus::Idle | RestartStatus::NotScheduled { .. } => {}
    }
    Ok(())
}

fn validate_health_commit(
    configuration: &SessionConfiguration,
    health: &HealthSummary,
    changed_at: TimestampMillis,
) -> DomainResult<()> {
    let configured_checks = u16::try_from(configuration.health_rules().len()).map_err(|_| {
        crate::DomainError::CapacityExceeded {
            resource: "health_rules",
            limit: usize::from(u16::MAX),
        }
    })?;
    if health.configured_checks() != configured_checks {
        return Err(crate::DomainError::InvalidValue {
            field: "health_summary",
            reason: crate::InvalidValueReason::InvalidCombination {
                rule: "health summary must match configured health rule count",
            },
        });
    }
    if health
        .observed_at()
        .is_some_and(|observed| observed > changed_at)
    {
        return Err(crate::DomainError::InvalidValue {
            field: "health_observed_at",
            reason: crate::InvalidValueReason::InvalidCombination {
                rule: "health observation cannot follow its session commit",
            },
        });
    }
    Ok(())
}

fn corrupt_session(rule: &'static str) -> crate::DomainError {
    crate::DomainError::CorruptState {
        scope: "session",
        rule,
    }
}

/// Prepared compact projection used by list and query subscriptions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSummary {
    id: SessionId,
    name: SessionName,
    lifecycle: LifecycleState,
    foreground: ForegroundObservation,
    health: HealthSummary,
    work: WorkSummary,
    profile: Option<ProfileName>,
    base_profile: Option<ProfileName>,
    profile_origin: Option<ProfileSelectionOrigin>,
    profile_binding: ProfileBindingMode,
    effective_profile: Option<EffectiveProfileSnapshot>,
    process: Option<ProcessInstanceId>,
    labels: Labels,
    started_at: Option<TimestampMillis>,
    updated_at: TimestampMillis,
    restart_count: u32,
    restart_status: RestartStatus,
    revision: Revision,
    lifecycle_revision: Revision,
}

impl SessionSummary {
    /// Returns the stable session identity.
    pub const fn id(&self) -> &SessionId {
        &self.id
    }

    /// Returns the human-facing session name.
    pub const fn name(&self) -> &SessionName {
        &self.name
    }

    /// Returns authoritative lifecycle.
    pub const fn lifecycle(&self) -> LifecycleState {
        self.lifecycle
    }

    /// Returns compact foreground observation.
    pub const fn foreground(&self) -> &ForegroundObservation {
        &self.foreground
    }

    /// Returns compact health state.
    pub const fn health(&self) -> &HealthSummary {
        &self.health
    }

    /// Returns compact WorkContext counts and aggregate status.
    pub const fn work(&self) -> &WorkSummary {
        &self.work
    }

    /// Returns the selected detection profile, if present.
    pub const fn profile(&self) -> Option<&ProfileName> {
        self.profile.as_ref()
    }

    /// Returns the durable launch-selected base profile, if present.
    pub const fn base_profile(&self) -> Option<&ProfileName> {
        self.base_profile.as_ref()
    }

    /// Returns the durable base-profile selection provenance.
    pub const fn profile_origin(&self) -> Option<ProfileSelectionOrigin> {
        self.profile_origin
    }

    /// Returns whether effective selection is fixed or foreground-adaptive.
    pub const fn profile_binding(&self) -> ProfileBindingMode {
        self.profile_binding
    }

    /// Returns the exact adaptive runtime snapshot when evidence is available.
    pub const fn effective_profile(&self) -> Option<&EffectiveProfileSnapshot> {
        self.effective_profile.as_ref()
    }

    /// Returns the exact managed process represented by this prepared summary.
    pub const fn process(&self) -> Option<&ProcessInstanceId> {
        self.process.as_ref()
    }

    /// Returns the current effective state for the supplied exact process.
    ///
    /// Adaptive sessions without evidence return an explicit unclassified
    /// initial generation. A process mismatch returns no state. This function
    /// performs no IO and does not panic.
    pub fn effective_profile_for(
        &self,
        process: &ProcessInstanceId,
    ) -> Option<EffectiveProfileSnapshot> {
        if self.process.as_ref() != Some(process) {
            return None;
        }
        match self.profile_binding {
            ProfileBindingMode::Fixed => self.effective_profile.clone().or_else(|| {
                Some(EffectiveProfileSnapshot::new(
                    process.clone(),
                    EffectiveProfileEpoch::INITIAL,
                    self.base_profile.clone().map_or(
                        EffectiveProfileSelection::Unclassified(UnclassifiedProfileReason::NoMatch),
                        EffectiveProfileSelection::Profile,
                    ),
                    crate::EffectiveProfileOrigin::BaseProfile,
                    self.updated_at,
                ))
            }),
            ProfileBindingMode::ForegroundAdaptive => {
                self.effective_profile.clone().or_else(|| {
                    Some(EffectiveProfileSnapshot::new(
                        process.clone(),
                        EffectiveProfileEpoch::INITIAL,
                        EffectiveProfileSelection::Unclassified(
                            UnclassifiedProfileReason::AwaitingObservation,
                        ),
                        crate::EffectiveProfileOrigin::ForegroundProcess,
                        self.updated_at,
                    ))
                })
            }
        }
    }

    /// Preserves adaptive runtime state across a matching durable projection upsert.
    ///
    /// Process replacement, binding changes, or base-profile changes deliberately
    /// reset the effective state. This function performs no IO and does not panic.
    pub fn retain_effective_profile_from(&mut self, previous: &Self) {
        if previous.profile_binding == self.profile_binding
            && previous.base_profile == self.base_profile
            && previous.process == self.process
        {
            self.profile = previous.profile.clone();
            self.effective_profile = previous.effective_profile.clone();
        }
    }

    /// Applies one exact-process profile/context transition and advances its epoch.
    ///
    /// This mutates only a rebuildable prepared summary. It never changes a
    /// durable session revision or emits a durable domain event.
    ///
    /// # Errors
    ///
    /// Fixed bindings accept only their base selection and base-profile origin.
    /// Returns invalid-value errors for incompatible bindings, stale process
    /// identities, or backward evidence time, and revision exhaustion at the
    /// epoch bound. A later timestamp alone is a content no-op.
    pub fn apply_effective_profile(
        &mut self,
        transition: EffectiveProfileTransition,
    ) -> DomainResult<bool> {
        if self.profile_binding == ProfileBindingMode::Fixed
            && (transition.origin() != crate::EffectiveProfileOrigin::BaseProfile
                || transition.selection().profile() != self.base_profile.as_ref())
        {
            return Err(DomainError::InvalidValue {
                field: "profile_binding",
                reason: crate::InvalidValueReason::InvalidCombination {
                    rule: "fixed profile context must retain its durable base selection",
                },
            });
        }
        if self.process.as_ref() != Some(transition.process()) {
            return Err(DomainError::InvalidValue {
                field: "effective_profile_process",
                reason: crate::InvalidValueReason::InvalidCombination {
                    rule: "effective profile transition must match the prepared process",
                },
            });
        }
        if self
            .effective_profile
            .as_ref()
            .is_some_and(|current| transition.observed_at() < current.observed_at())
        {
            return Err(DomainError::InvalidValue {
                field: "effective_profile_observed_at",
                reason: crate::InvalidValueReason::InvalidCombination {
                    rule: "effective profile evidence cannot move backward in time",
                },
            });
        }
        // A different selection or origin changes how foreground evidence is
        // interpreted. Clear that observation before publishing the new profile;
        // directory/server context alone does not invalidate its interpretation.
        // See [effective snapshot changes](../docs/internals/observations-and-profiles.md#apply-or-retain-an-effective-snapshot).
        let invalidate_foreground = self
            .effective_profile_for(transition.process())
            .is_some_and(|current| {
                current.selection() != transition.selection()
                    || current.origin() != transition.origin()
            });
        if self.effective_profile.as_ref().is_some_and(|current| {
            current.process() == transition.process()
                && current.selection() == transition.selection()
                && current.origin() == transition.origin()
                && current.current_directory() == transition.current_directory()
                && current.server_role() == transition.server_role()
                && current.matching_evidence() == transition.matching_evidence()
        }) {
            return Ok(false);
        }
        let epoch = self
            .effective_profile
            .as_ref()
            .map_or(Ok(EffectiveProfileEpoch::new(1)), |current| {
                current.epoch().next()
            })?;
        self.profile = transition.selection().profile().cloned();
        if invalidate_foreground {
            self.foreground = ForegroundObservation::unknown(transition.observed_at());
        }
        self.effective_profile = Some(
            EffectiveProfileSnapshot::new(
                transition.process().clone(),
                epoch,
                transition.selection().clone(),
                transition.origin(),
                transition.observed_at(),
            )
            .with_runtime_context(
                transition.current_directory().cloned(),
                transition.server_role(),
            )
            .with_matching_evidence(transition.matching_evidence().to_vec()),
        );
        Ok(true)
    }

    /// Returns labels retained by the internal prepared projection.
    pub const fn labels(&self) -> &Labels {
        &self.labels
    }

    /// Returns current process start time, if running.
    pub const fn started_at(&self) -> Option<TimestampMillis> {
        self.started_at
    }

    /// Returns most recent durable domain update time.
    pub const fn updated_at(&self) -> TimestampMillis {
        self.updated_at
    }

    /// Returns the consecutive automatic restart count.
    pub const fn restart_count(&self) -> u32 {
        self.restart_count
    }

    /// Returns the durable disposition of the latest restart decision.
    pub const fn restart_status(&self) -> RestartStatus {
        self.restart_status
    }

    /// Returns the durable session revision represented by this projection.
    pub const fn revision(&self) -> Revision {
        self.revision
    }

    /// Returns the revision of the latest authoritative lifecycle transition.
    pub const fn lifecycle_revision(&self) -> Revision {
        self.lifecycle_revision
    }
}
