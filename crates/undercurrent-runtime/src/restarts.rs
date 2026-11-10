// SPDX-License-Identifier: Apache-2.0

//! Bounded deduplicated automatic restart work queue.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use undercurrent_application::{
    DueRestartSource, PortError, PortErrorKind, PortFuture, PortOperation, RestartJob,
    RestartScheduler,
};
use undercurrent_domain::{Revision, SessionId, TimestampMillis};

use crate::{RuntimeAdapterFailure, RuntimeLimitError};

type RestartKey = (TimestampMillis, SessionId, Revision, u32);

/// In-memory restart queue with stable due ordering and exact deduplication.
pub struct BoundedRestartQueue {
    maximum: usize,
    state: Mutex<RestartQueueState>,
}

#[derive(Default)]
struct RestartQueueState {
    jobs: BTreeMap<RestartKey, RestartJob>,
    keys_by_session: BTreeMap<SessionId, BTreeSet<RestartKey>>,
}

impl BoundedRestartQueue {
    /// Constructs an empty queue with a hard pending-job limit.
    ///
    /// # Errors
    ///
    /// Returns a typed zero-limit failure. This function performs no IO and
    /// does not panic.
    pub fn new(maximum: usize) -> Result<Self, RuntimeLimitError> {
        if maximum == 0 {
            return Err(RuntimeLimitError::zero("restart_jobs"));
        }
        Ok(Self {
            maximum,
            state: Mutex::new(RestartQueueState::default()),
        })
    }

    /// Claims at most `limit` jobs whose deadline is not later than `now`.
    ///
    /// Jobs are returned by deadline, stable session ID, revision, then attempt.
    /// A zero limit returns an empty batch. This function performs no IO and
    /// does not panic.
    pub fn claim_due(&self, now: TimestampMillis, limit: usize) -> Vec<RestartJob> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let keys = state
            .jobs
            .iter()
            .take_while(|(key, _)| key.0 <= now)
            .take(limit)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        keys.into_iter()
            .filter_map(|key| remove_job(&mut state, &key))
            .collect()
    }

    /// Returns pending restart job count.
    pub fn len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .jobs
            .len()
    }

    /// Returns whether no restart job is pending.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl RestartScheduler for BoundedRestartQueue {
    fn schedule<'a>(&'a self, job: &'a RestartJob) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            let key = restart_key(job);
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.jobs.contains_key(&key) {
                return Ok(());
            }
            if state.jobs.len() >= self.maximum {
                return Err(PortError::new(
                    PortErrorKind::CapacityExceeded,
                    PortOperation::ScheduleRestart,
                    Some(job.session_id.clone()),
                    RuntimeAdapterFailure("restart job capacity exhausted"),
                ));
            }
            state.jobs.insert(key.clone(), job.clone());
            state
                .keys_by_session
                .entry(job.session_id.clone())
                .or_default()
                .insert(key);
            Ok(())
        })
    }

    fn cancel_session_restarts<'a>(
        &'a self,
        session_id: &'a SessionId,
    ) -> PortFuture<'a, Result<usize, PortError>> {
        Box::pin(async move {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(keys) = state.keys_by_session.remove(session_id) else {
                return Ok(0);
            };
            let removed = keys
                .into_iter()
                .filter(|key| state.jobs.remove(key).is_some())
                .count();
            Ok(removed)
        })
    }
}

impl DueRestartSource for BoundedRestartQueue {
    fn claim_due_restarts(
        &self,
        now: TimestampMillis,
        limit: usize,
    ) -> Result<Vec<RestartJob>, PortError> {
        if limit == 0 {
            return Err(PortError::new(
                PortErrorKind::InvalidData,
                PortOperation::ClaimDueRestarts,
                None,
                RuntimeAdapterFailure("restart claim limit must be non-zero"),
            ));
        }
        Ok(self.claim_due(now, limit))
    }
}

fn remove_job(state: &mut RestartQueueState, key: &RestartKey) -> Option<RestartJob> {
    let job = state.jobs.remove(key)?;
    let remove_session = state
        .keys_by_session
        .get_mut(&job.session_id)
        .is_some_and(|keys| {
            keys.remove(key);
            keys.is_empty()
        });
    if remove_session {
        state.keys_by_session.remove(&job.session_id);
    }
    Some(job)
}

fn restart_key(job: &RestartJob) -> RestartKey {
    (
        job.not_before,
        job.session_id.clone(),
        job.expected_revision,
        job.attempt,
    )
}
