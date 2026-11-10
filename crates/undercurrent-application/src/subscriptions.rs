// SPDX-License-Identifier: Apache-2.0

//! Event-driven, bounded session-query subscription ownership.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

use undercurrent_domain::{
    CatalogRevision, EffectiveProfileSnapshot, EffectiveProfileTransition, Session, SessionId,
    SessionQuery, SessionSummary,
};

use crate::{
    CatalogError, EffectiveProfileDirectory, PortError, PortErrorKind, PortOperation,
    PreparedReadModels, PreparedSessionCatalog, QueryCost, QueryWatchBatch,
    SessionProjectionWriter, SessionQueryWatch, SessionSummaryView, SubscriptionWake,
};

/// Stable in-process identity for one query subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionSubscriptionId(u64);

impl SessionSubscriptionId {
    /// Returns the monotonic process-local numeric value.
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl Display for SessionSubscriptionId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Failure from bounded prepared-query subscription operations.
#[derive(Debug)]
pub enum SessionSubscriptionError {
    /// Configured subscription capacity was zero.
    InvalidLimit,
    /// Active subscription capacity was exhausted.
    CapacityExceeded {
        /// Inclusive configured capacity.
        limit: usize,
    },
    /// Process-local subscription identities were exhausted.
    IdentityExhausted,
    /// Prepared catalog query or revision failed.
    Catalog(CatalogError),
    /// Subscription was explicitly closed or dropped.
    Closed,
    /// Caller-supplied receive timeout elapsed.
    Timeout,
}

impl Display for SessionSubscriptionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLimit => {
                formatter.write_str("session subscription limit must be non-zero")
            }
            Self::CapacityExceeded { limit } => {
                write!(
                    formatter,
                    "session subscription capacity {limit} is exhausted"
                )
            }
            Self::IdentityExhausted => {
                formatter.write_str("session subscription identity counter is exhausted")
            }
            Self::Catalog(error) => Display::fmt(error, formatter),
            Self::Closed => formatter.write_str("session query subscription is closed"),
            Self::Timeout => formatter.write_str("session query subscription wait timed out"),
        }
    }
}

impl Error for SessionSubscriptionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Catalog(error) => Some(error),
            Self::InvalidLimit
            | Self::CapacityExceeded { .. }
            | Self::IdentityExhausted
            | Self::Closed
            | Self::Timeout => None,
        }
    }
}

impl From<CatalogError> for SessionSubscriptionError {
    fn from(value: CatalogError) -> Self {
        Self::Catalog(value)
    }
}

/// Current bounded hub state without complete query or summary payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionSubscriptionPressure {
    /// Live logical query subscriptions.
    pub subscriptions: usize,
    /// Subscriptions with at least one coalesced catalog change pending.
    pub dirty_subscriptions: usize,
    /// Inclusive configured live-subscription maximum.
    pub limit: usize,
}

/// Clone of the latest query window already observed by a subscriber.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionWatchSnapshot {
    /// Current bounded page in authoritative order.
    pub items: Vec<SessionSummaryView>,
    /// Catalog revision represented by the snapshot.
    pub revision: CatalogRevision,
    /// Exact total only when explicitly requested.
    pub total: Option<usize>,
    /// Bounded prepared-query extraction cost.
    pub query_cost: QueryCost,
}

/// Event-driven query hub and rebuildable projection adapter.
///
/// One changed summary marks only subscriptions whose filter matched the prior
/// or replacement summary, or whose current page already contains that ID.
/// Slow consumers retain one dirty bit and recompute one bounded latest-page
/// delta when they next receive, so intermediate replaceable state cannot grow
/// memory.
pub struct SessionSubscriptionHub {
    read_models: Arc<PreparedReadModels>,
    maximum: usize,
    next_id: AtomicU64,
    projection_gate: Mutex<()>,
    subscriptions: Arc<Mutex<BTreeMap<SessionSubscriptionId, Weak<SubscriptionState>>>>,
}

impl SessionSubscriptionHub {
    /// Constructs an empty hub around the supplied prepared read models.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed error when `maximum` is zero.
    pub fn new(
        read_models: PreparedReadModels,
        maximum: usize,
    ) -> Result<Self, SessionSubscriptionError> {
        if maximum == 0 {
            return Err(SessionSubscriptionError::InvalidLimit);
        }
        Ok(Self {
            read_models: Arc::new(read_models),
            maximum,
            next_id: AtomicU64::new(0),
            projection_gate: Mutex::new(()),
            subscriptions: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    /// Returns the prepared catalog serving finite session queries.
    ///
    /// Projection mutations must still enter through this hub so logical
    /// subscriptions are notified.
    pub fn catalog(&self) -> &PreparedSessionCatalog {
        self.read_models.catalog()
    }

    /// Opens one bounded first-page watch and registers its selective interest.
    ///
    /// The initial snapshot and registration are serialized with projection
    /// writes, preventing a change from falling between those steps. This
    /// function reads prepared state only, performs no external IO, and does
    /// not panic.
    ///
    /// # Errors
    ///
    /// Returns catalog, subscription-capacity, or identity-exhaustion failure.
    pub fn subscribe(
        &self,
        query: SessionQuery,
    ) -> Result<SessionQuerySubscription, SessionSubscriptionError> {
        self.subscribe_inner(query, None)
    }

    /// Opens a watch whose clean-to-dirty transition wakes one outer pump.
    ///
    /// The wake carries no query or session payload. Initial snapshot and
    /// registration retain the same atomicity as [`Self::subscribe`]. This
    /// function performs prepared-state work only and does not panic.
    ///
    /// # Errors
    ///
    /// Returns catalog, subscription-capacity, or identity-exhaustion failure.
    pub fn subscribe_with_wake(
        &self,
        query: SessionQuery,
        wake: Arc<dyn SubscriptionWake>,
    ) -> Result<SessionQuerySubscription, SessionSubscriptionError> {
        self.subscribe_inner(query, Some(wake))
    }

    fn subscribe_inner(
        &self,
        query: SessionQuery,
        wake: Option<Arc<dyn SubscriptionWake>>,
    ) -> Result<SessionQuerySubscription, SessionSubscriptionError> {
        let _projection = self
            .projection_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let watch = self.read_models.catalog().open_watch(query)?;
        let id = self.next_subscription_id()?;
        let state = Arc::new(SubscriptionState {
            wake,
            watch: Mutex::new(SubscriptionWatchState {
                watch,
                dirty: false,
                closed: false,
            }),
            changed: Condvar::new(),
        });
        let mut subscriptions = self.lock_subscriptions();
        subscriptions.retain(|_, state| state.strong_count() > 0);
        if subscriptions.len() >= self.maximum {
            return Err(SessionSubscriptionError::CapacityExceeded {
                limit: self.maximum,
            });
        }
        subscriptions.insert(id, Arc::downgrade(&state));
        Ok(SessionQuerySubscription {
            id,
            catalog: self.read_models.clone(),
            state,
            subscriptions: Arc::downgrade(&self.subscriptions),
        })
    }

    /// Returns lock-consistent live and dirty subscription counts.
    ///
    /// Dead weak registrations are cleaned as part of the snapshot. This
    /// function performs no external IO and does not panic.
    pub fn pressure(&self) -> SessionSubscriptionPressure {
        let mut subscriptions = self.lock_subscriptions();
        let mut dirty_subscriptions = 0;
        subscriptions.retain(|_, candidate| {
            let Some(state) = candidate.upgrade() else {
                return false;
            };
            let watch = state
                .watch
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            dirty_subscriptions += usize::from(watch.dirty && !watch.closed);
            !watch.closed
        });
        SessionSubscriptionPressure {
            subscriptions: subscriptions.len(),
            dirty_subscriptions,
            limit: self.maximum,
        }
    }

    fn next_subscription_id(&self) -> Result<SessionSubscriptionId, SessionSubscriptionError> {
        self.next_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map(|previous| SessionSubscriptionId(previous + 1))
            .map_err(|_| SessionSubscriptionError::IdentityExhausted)
    }

    fn notify_change(
        &self,
        session_id: &SessionId,
        previous: Option<&SessionSummary>,
        current: Option<&SessionSummary>,
    ) {
        let mut subscriptions = self.lock_subscriptions();
        subscriptions.retain(|_, candidate| {
            let Some(state) = candidate.upgrade() else {
                return false;
            };
            let mut watch = state
                .watch
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if watch.closed {
                return false;
            }
            let interested = previous
                .is_some_and(|summary| watch.watch.query().filter.matches(summary))
                || current.is_some_and(|summary| watch.watch.query().filter.matches(summary))
                || watch
                    .watch
                    .items()
                    .iter()
                    .any(|summary| summary.id() == session_id);
            if interested {
                let wake = (!watch.dirty)
                    .then(|| state.wake.as_ref())
                    .flatten()
                    .cloned();
                watch.dirty = true;
                state.changed.notify_one();
                drop(watch);
                if let Some(wake) = wake {
                    wake.wake();
                }
            }
            true
        });
    }

    fn lock_subscriptions(
        &self,
    ) -> std::sync::MutexGuard<'_, BTreeMap<SessionSubscriptionId, Weak<SubscriptionState>>> {
        self.subscriptions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn projection_error(session_id: Option<SessionId>, error: CatalogError) -> PortError {
        PortError::new(
            PortErrorKind::Other,
            PortOperation::UpdateProjection,
            session_id,
            error,
        )
    }
}

impl SessionProjectionWriter for SessionSubscriptionHub {
    fn upsert_session(&self, session: &Session) -> Result<(), PortError> {
        let _projection = self
            .projection_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = self
            .read_models
            .catalog()
            .get(session.id())
            .map_err(|error| Self::projection_error(Some(session.id().clone()), error))?;
        let mut current = session.summary();
        if let Some(previous) = &previous {
            current.retain_effective_profile_from(previous);
        }
        self.read_models
            .catalog()
            .upsert(current.clone())
            .map_err(|error| Self::projection_error(Some(session.id().clone()), error))?;
        if previous.as_ref() != Some(&current) {
            self.notify_change(session.id(), previous.as_ref(), Some(&current));
        }
        Ok(())
    }

    fn remove_summary(&self, session_id: &SessionId) -> Result<(), PortError> {
        let _projection = self
            .projection_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = self
            .read_models
            .catalog()
            .get(session_id)
            .map_err(|error| Self::projection_error(Some(session_id.clone()), error))?;
        self.read_models.remove_summary(session_id)?;
        if previous.is_some() {
            self.notify_change(session_id, previous.as_ref(), None);
        }
        Ok(())
    }
}

impl EffectiveProfileDirectory for SessionSubscriptionHub {
    fn get(
        &self,
        session_id: &SessionId,
        process: &undercurrent_domain::ProcessInstanceId,
    ) -> Result<Option<EffectiveProfileSnapshot>, PortError> {
        self.read_models
            .catalog()
            .get(session_id)
            .map(|summary| summary.and_then(|summary| summary.effective_profile_for(process)))
            .map_err(|error| {
                PortError::new(
                    PortErrorKind::Other,
                    PortOperation::ReadEffectiveProfile,
                    Some(session_id.clone()),
                    error,
                )
            })
    }

    fn transition(
        &self,
        session_id: &SessionId,
        transition: EffectiveProfileTransition,
    ) -> Result<Option<EffectiveProfileSnapshot>, PortError> {
        let _projection = self
            .projection_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(previous) = self
            .read_models
            .catalog()
            .get(session_id)
            .map_err(|error| Self::projection_error(Some(session_id.clone()), error))?
        else {
            return Ok(None);
        };
        let mut current = previous.clone();
        let changed = current
            .apply_effective_profile(transition)
            .map_err(|error| {
                PortError::new(
                    PortErrorKind::Conflict,
                    PortOperation::UpdateEffectiveProfile,
                    Some(session_id.clone()),
                    error,
                )
            })?;
        let snapshot = current.effective_profile().cloned();
        if changed {
            self.read_models
                .catalog()
                .upsert(current.clone())
                .map_err(|error| Self::projection_error(Some(session_id.clone()), error))?;
            self.notify_change(session_id, Some(&previous), Some(&current));
        }
        Ok(snapshot)
    }
}

/// One independently owned query subscription with a coalescing mailbox.
pub struct SessionQuerySubscription {
    id: SessionSubscriptionId,
    catalog: Arc<PreparedReadModels>,
    state: Arc<SubscriptionState>,
    subscriptions: Weak<Mutex<BTreeMap<SessionSubscriptionId, Weak<SubscriptionState>>>>,
}

impl SessionQuerySubscription {
    /// Returns the process-local subscription identity.
    pub const fn id(&self) -> SessionSubscriptionId {
        self.id
    }

    /// Returns the latest query window already delivered to this subscriber.
    ///
    /// Pending dirty state is not applied until a receive operation. This
    /// function clones at most one configured page, performs no external IO,
    /// and does not panic.
    ///
    /// # Errors
    ///
    /// Returns closed-subscription failure.
    pub fn snapshot(&self) -> Result<SessionWatchSnapshot, SessionSubscriptionError> {
        let state = self.lock_state();
        if state.closed {
            return Err(SessionSubscriptionError::Closed);
        }
        Ok(snapshot(&state.watch))
    }

    /// Returns a pending latest-page delta without waiting.
    ///
    /// Multiple changes since the last receive are coalesced. This function
    /// performs a prepared query only when selectively marked dirty and does
    /// not panic.
    ///
    /// # Errors
    ///
    /// Returns catalog or closed-subscription failure.
    pub fn try_recv(&self) -> Result<Option<QueryWatchBatch>, SessionSubscriptionError> {
        let mut state = self.lock_state();
        if state.closed {
            return Err(SessionSubscriptionError::Closed);
        }
        self.refresh_dirty(&mut state)
    }

    /// Blocks until one coalesced latest-page delta is available.
    ///
    /// No hub or catalog lock is retained while waiting. This function performs
    /// prepared-query and blocking synchronization work but no external IO, and
    /// does not panic.
    ///
    /// # Errors
    ///
    /// Returns catalog or closed-subscription failure.
    pub fn recv(&self) -> Result<QueryWatchBatch, SessionSubscriptionError> {
        let mut state = self.lock_state();
        loop {
            if state.closed {
                return Err(SessionSubscriptionError::Closed);
            }
            if let Some(batch) = self.refresh_dirty(&mut state)? {
                return Ok(batch);
            }
            state = self
                .state
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    /// Waits up to `timeout` for one coalesced latest-page delta.
    ///
    /// No hub or catalog lock is retained while waiting. This function performs
    /// prepared-query and blocking synchronization work but no external IO, and
    /// does not panic.
    ///
    /// # Errors
    ///
    /// Returns timeout, catalog, or closed-subscription failure.
    pub fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<QueryWatchBatch, SessionSubscriptionError> {
        if timeout.is_zero() {
            return self.try_recv()?.ok_or(SessionSubscriptionError::Timeout);
        }
        let started_at = Instant::now();
        let mut state = self.lock_state();
        loop {
            if state.closed {
                return Err(SessionSubscriptionError::Closed);
            }
            if let Some(batch) = self.refresh_dirty(&mut state)? {
                return Ok(batch);
            }
            let remaining = timeout.saturating_sub(started_at.elapsed());
            if remaining.is_zero() {
                return Err(SessionSubscriptionError::Timeout);
            }
            let (next, timed_out) = self
                .state
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next;
            if timed_out.timed_out() && !state.dirty {
                return Err(SessionSubscriptionError::Timeout);
            }
        }
    }

    /// Closes this query stream without affecting its peers.
    ///
    /// Repeated close is idempotent. This function performs no external IO and
    /// does not panic.
    pub fn close(&self) {
        self.close_inner();
    }

    fn refresh_dirty(
        &self,
        state: &mut SubscriptionWatchState,
    ) -> Result<Option<QueryWatchBatch>, SessionSubscriptionError> {
        while state.dirty {
            state.dirty = false;
            match self.catalog.catalog().refresh_watch(&mut state.watch) {
                Ok(Some(batch)) => return Ok(Some(batch)),
                Ok(None) => {}
                Err(error) => {
                    state.dirty = true;
                    return Err(error.into());
                }
            }
        }
        Ok(None)
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, SubscriptionWatchState> {
        self.state
            .watch
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn close_inner(&self) {
        let mut state = self.lock_state();
        if state.closed {
            return;
        }
        state.closed = true;
        state.dirty = false;
        self.state.changed.notify_all();
        // Notification and pressure scans hold the registry before locking each
        // watch. Release this watch before removing its registration to avoid
        // taking those two locks in the opposite order.
        drop(state);
        let Some(subscriptions) = self.subscriptions.upgrade() else {
            return;
        };
        subscriptions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.id);
    }
}

impl Drop for SessionQuerySubscription {
    fn drop(&mut self) {
        self.close_inner();
    }
}

struct SubscriptionState {
    wake: Option<Arc<dyn SubscriptionWake>>,
    watch: Mutex<SubscriptionWatchState>,
    changed: Condvar,
}

struct SubscriptionWatchState {
    watch: SessionQueryWatch,
    dirty: bool,
    closed: bool,
}

fn snapshot(watch: &SessionQueryWatch) -> SessionWatchSnapshot {
    SessionWatchSnapshot {
        items: watch.items().to_vec(),
        revision: watch.snapshot_revision(),
        total: watch.total(),
        query_cost: watch.query_cost(),
    }
}
