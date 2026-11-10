// SPDX-License-Identifier: Apache-2.0

//! Bounded deadline-indexed per-session observation work.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use undercurrent_application::{
    DueObservationSource, ForegroundObservationWake, ObservationJob, ObservationJobKind,
    ObservationReconcileOutcome, ObservationScheduleOutcome, ObservationScheduler, PortError,
    PortErrorKind, PortFuture, PortOperation, SessionObservationPlan,
};
use undercurrent_domain::{SessionId, TimestampMillis};

use crate::{RuntimeAdapterFailure, RuntimeLimitError};

type ObservationKey = (SessionId, ObservationJobKind);
type ObservationDeadlineKey = (TimestampMillis, ObservationJobKind, SessionId);

/// Current bounded observation queue state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservationQueuePressure {
    /// Pending distinct `(session, kind)` jobs.
    pub jobs: usize,
    /// Distinct sessions with at least one pending job.
    pub sessions: usize,
    /// Sessions retaining a latest-plan revision, including inactive plans.
    pub planned_sessions: usize,
    /// Inclusive configured job maximum.
    pub limit: usize,
}

/// In-memory observation queue with deadline and direct session indexes.
pub struct BoundedObservationQueue {
    maximum: usize,
    state: Mutex<ObservationQueueState>,
}

impl BoundedObservationQueue {
    /// Constructs an empty queue with a hard distinct-job maximum.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed zero-limit failure.
    pub fn new(maximum: usize) -> Result<Self, RuntimeLimitError> {
        if maximum == 0 {
            return Err(RuntimeLimitError::zero("observation_jobs"));
        }
        Ok(Self {
            maximum,
            state: Mutex::new(ObservationQueueState::default()),
        })
    }

    /// Returns lock-consistent queue and session counts.
    pub fn pressure(&self) -> ObservationQueuePressure {
        let state = self.lock_state();
        let mut previous = None;
        let mut sessions = 0_usize;
        for session_id in state.jobs.keys().map(|(session_id, _)| session_id) {
            if previous != Some(session_id) {
                sessions = sessions.saturating_add(1);
                previous = Some(session_id);
            }
        }
        ObservationQueuePressure {
            jobs: state.jobs.len(),
            sessions,
            planned_sessions: state.plan_revisions.len(),
            limit: self.maximum,
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, ObservationQueueState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn deadline_key(job: &ObservationJob) -> ObservationDeadlineKey {
        (job.not_before, job.kind, job.session_id.clone())
    }

    fn capacity_error(
        operation: PortOperation,
        session_id: &SessionId,
        message: &'static str,
    ) -> PortError {
        PortError::new(
            PortErrorKind::CapacityExceeded,
            operation,
            Some(session_id.clone()),
            RuntimeAdapterFailure(message),
        )
    }
}

impl ObservationScheduler for BoundedObservationQueue {
    fn schedule_observation<'a>(
        &'a self,
        job: &'a ObservationJob,
    ) -> PortFuture<'a, Result<ObservationScheduleOutcome, PortError>> {
        Box::pin(async move {
            let mut state = self.lock_state();
            if state
                .plan_revisions
                .get(&job.session_id)
                .is_some_and(|revision| *revision > job.expected_revision)
            {
                return Ok(ObservationScheduleOutcome::StaleIgnored);
            }
            if !state.plan_revisions.contains_key(&job.session_id)
                && state.plan_revisions.len() >= self.maximum
            {
                return Err(Self::capacity_error(
                    PortOperation::ScheduleObservation,
                    &job.session_id,
                    "observation session plan capacity exhausted",
                ));
            }
            if !state.jobs.contains_key(&(job.session_id.clone(), job.kind))
                && state.jobs.len() >= self.maximum
            {
                return Err(Self::capacity_error(
                    PortOperation::ScheduleObservation,
                    &job.session_id,
                    "observation job capacity exhausted",
                ));
            }
            let outcome = state.apply_job(job);
            if outcome != ObservationScheduleOutcome::StaleIgnored {
                state
                    .plan_revisions
                    .entry(job.session_id.clone())
                    .and_modify(|revision| {
                        *revision = (*revision).max(job.expected_revision);
                    })
                    .or_insert(job.expected_revision);
            }
            Ok(outcome)
        })
    }

    fn cancel_session_observations<'a>(
        &'a self,
        session_id: &'a SessionId,
    ) -> PortFuture<'a, Result<usize, PortError>> {
        Box::pin(async move {
            let mut state = self.lock_state();
            let mut removed = 0_usize;
            for kind in ObservationJobKind::ALL {
                if let Some(job) = state.jobs.remove(&(session_id.clone(), kind)) {
                    state.deadlines.remove(&Self::deadline_key(&job));
                    removed = removed.saturating_add(1);
                }
            }
            state.plan_revisions.remove(session_id);
            state.foreground_active.remove(session_id);
            Ok(removed)
        })
    }

    fn reconcile_session_observations<'a>(
        &'a self,
        plan: &'a SessionObservationPlan,
    ) -> PortFuture<'a, Result<ObservationReconcileOutcome, PortError>> {
        Box::pin(async move {
            let mut state = self.lock_state();
            if state
                .plan_revisions
                .get(&plan.session_id)
                .is_some_and(|revision| *revision > plan.expected_revision)
            {
                return Ok(ObservationReconcileOutcome {
                    stale_ignored: ObservationJobKind::ALL.len(),
                    ..ObservationReconcileOutcome::default()
                });
            }
            if !state.plan_revisions.contains_key(&plan.session_id)
                && state.plan_revisions.len() >= self.maximum
            {
                return Err(Self::capacity_error(
                    PortOperation::ReconcileSessionObservations,
                    &plan.session_id,
                    "observation session plan capacity exhausted",
                ));
            }
            let projected = state.projected_len(plan);
            if projected > self.maximum {
                return Err(Self::capacity_error(
                    PortOperation::ReconcileSessionObservations,
                    &plan.session_id,
                    "observation plan exceeds job capacity",
                ));
            }
            let mut outcome = ObservationReconcileOutcome::default();
            for kind in ObservationJobKind::ALL {
                if plan.deadline(kind).is_none() {
                    match state.remove_if_not_newer(&plan.session_id, kind, plan.expected_revision)
                    {
                        RemovalOutcome::Removed => {
                            outcome.removed = outcome.removed.saturating_add(1)
                        }
                        RemovalOutcome::StaleIgnored => {
                            outcome.stale_ignored = outcome.stale_ignored.saturating_add(1)
                        }
                        RemovalOutcome::Missing => {}
                    }
                }
            }
            for kind in ObservationJobKind::ALL {
                let Some(job) = plan.job(kind) else {
                    continue;
                };
                match state.apply_job(&job) {
                    ObservationScheduleOutcome::Scheduled => {
                        outcome.scheduled = outcome.scheduled.saturating_add(1)
                    }
                    ObservationScheduleOutcome::Replaced => {
                        outcome.replaced = outcome.replaced.saturating_add(1)
                    }
                    ObservationScheduleOutcome::Unchanged
                    | ObservationScheduleOutcome::EarlierDeadlineRetained => {
                        outcome.retained = outcome.retained.saturating_add(1)
                    }
                    ObservationScheduleOutcome::StaleIgnored => {
                        outcome.stale_ignored = outcome.stale_ignored.saturating_add(1)
                    }
                }
            }
            state
                .plan_revisions
                .insert(plan.session_id.clone(), plan.expected_revision);
            if plan.foreground_not_before.is_some() {
                state.foreground_active.insert(plan.session_id.clone());
            } else {
                state.foreground_active.remove(&plan.session_id);
            }
            Ok(outcome)
        })
    }
}

impl ForegroundObservationWake for BoundedObservationQueue {
    fn wake_foreground(
        &self,
        session_id: &SessionId,
        observed_at: TimestampMillis,
    ) -> Result<Option<ObservationScheduleOutcome>, PortError> {
        let mut state = self.lock_state();
        if !state.foreground_active.contains(session_id) {
            return Ok(None);
        }
        let Some(expected_revision) = state.plan_revisions.get(session_id).copied() else {
            return Ok(None);
        };
        let job = ObservationJob {
            session_id: session_id.clone(),
            expected_revision,
            kind: ObservationJobKind::Foreground,
            not_before: observed_at,
        };
        if !state
            .jobs
            .contains_key(&(session_id.clone(), ObservationJobKind::Foreground))
            && state.jobs.len() >= self.maximum
        {
            return Err(Self::capacity_error(
                PortOperation::ScheduleObservation,
                session_id,
                "foreground wake exceeds job capacity",
            ));
        }
        Ok(Some(state.apply_job(&job)))
    }
}

impl DueObservationSource for BoundedObservationQueue {
    fn claim_due_observations(
        &self,
        now: TimestampMillis,
        limit: usize,
    ) -> Result<Vec<ObservationJob>, PortError> {
        if limit == 0 {
            return Err(PortError::new(
                PortErrorKind::InvalidData,
                PortOperation::ClaimDueObservations,
                None,
                RuntimeAdapterFailure("observation claim limit must be non-zero"),
            ));
        }
        let mut state = self.lock_state();
        let deadlines = state
            .deadlines
            .iter()
            .take_while(|(not_before, _, _)| *not_before <= now)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        let mut jobs = Vec::with_capacity(deadlines.len());
        // Claiming transfers jobs to the worker, but keeps plan revisions so a
        // delayed older activation cannot recreate superseded work. An active
        // foreground plan also remains eligible for a new output wake.
        // See [coordination guide](../docs/internals/coordination/scheduling.md#schedule-observations-without-reviving-stale-work).
        for deadline in deadlines {
            state.deadlines.remove(&deadline);
            let (_, kind, session_id) = deadline;
            if let Some(job) = state.jobs.remove(&(session_id, kind)) {
                jobs.push(job);
            }
        }
        Ok(jobs)
    }
}

#[derive(Default)]
struct ObservationQueueState {
    jobs: BTreeMap<ObservationKey, ObservationJob>,
    deadlines: BTreeSet<ObservationDeadlineKey>,
    plan_revisions: BTreeMap<SessionId, undercurrent_domain::Revision>,
    foreground_active: BTreeSet<SessionId>,
}

impl ObservationQueueState {
    fn apply_job(&mut self, job: &ObservationJob) -> ObservationScheduleOutcome {
        let key = (job.session_id.clone(), job.kind);
        if let Some(current) = self.jobs.get(&key) {
            if job.expected_revision < current.expected_revision {
                return ObservationScheduleOutcome::StaleIgnored;
            }
            if job.expected_revision == current.expected_revision {
                if job.not_before == current.not_before {
                    return ObservationScheduleOutcome::Unchanged;
                }
                if job.not_before > current.not_before {
                    return ObservationScheduleOutcome::EarlierDeadlineRetained;
                }
            }
            let previous_deadline = BoundedObservationQueue::deadline_key(current);
            self.deadlines.remove(&previous_deadline);
            self.jobs.insert(key, job.clone());
            self.deadlines
                .insert(BoundedObservationQueue::deadline_key(job));
            return ObservationScheduleOutcome::Replaced;
        }
        self.jobs.insert(key, job.clone());
        self.deadlines
            .insert(BoundedObservationQueue::deadline_key(job));
        ObservationScheduleOutcome::Scheduled
    }

    fn projected_len(&self, plan: &SessionObservationPlan) -> usize {
        let mut projected = self.jobs.len();
        for kind in ObservationJobKind::ALL {
            let current = self.jobs.get(&(plan.session_id.clone(), kind));
            match (current, plan.deadline(kind)) {
                (None, Some(_)) => projected = projected.saturating_add(1),
                (Some(current), None) if current.expected_revision <= plan.expected_revision => {
                    projected = projected.saturating_sub(1);
                }
                _ => {}
            }
        }
        projected
    }

    fn remove_if_not_newer(
        &mut self,
        session_id: &SessionId,
        kind: ObservationJobKind,
        expected_revision: undercurrent_domain::Revision,
    ) -> RemovalOutcome {
        let key = (session_id.clone(), kind);
        let Some(current) = self.jobs.get(&key) else {
            return RemovalOutcome::Missing;
        };
        if current.expected_revision > expected_revision {
            return RemovalOutcome::StaleIgnored;
        }
        let deadline = BoundedObservationQueue::deadline_key(current);
        self.jobs.remove(&key);
        self.deadlines.remove(&deadline);
        RemovalOutcome::Removed
    }
}

enum RemovalOutcome {
    Removed,
    StaleIgnored,
    Missing,
}
