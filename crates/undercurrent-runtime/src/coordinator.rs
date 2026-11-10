// SPDX-License-Identifier: Apache-2.0

//! Bounded per-session mutation ownership and pressure diagnostics.

use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;

use undercurrent_application::{
    MetricName, MetricObservation, PortError, PortErrorKind, PortFuture, PortOperation,
    SessionMutationCoordinator, SessionMutationLease, Telemetry,
};
use undercurrent_domain::SessionId;

use crate::{RuntimeAdapterFailure, RuntimeLimitError};

/// Explicit hard bounds for session-local mutation coordination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionMutationCoordinatorLimits {
    max_active_sessions: usize,
    max_waiters_per_session: usize,
    max_diagnostic_page_size: usize,
}

impl SessionMutationCoordinatorLimits {
    /// Constructs coordinator bounds without selecting product defaults.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed error when any required hard limit is zero.
    pub fn new(
        max_active_sessions: usize,
        max_waiters_per_session: usize,
        max_diagnostic_page_size: usize,
    ) -> Result<Self, RuntimeLimitError> {
        if max_active_sessions == 0 {
            return Err(RuntimeLimitError::zero("mutation_active_sessions"));
        }
        if max_waiters_per_session == 0 {
            return Err(RuntimeLimitError::zero("mutation_waiters_per_session"));
        }
        if max_diagnostic_page_size == 0 {
            return Err(RuntimeLimitError::zero("mutation_diagnostic_page_size"));
        }
        Ok(Self {
            max_active_sessions,
            max_waiters_per_session,
            max_diagnostic_page_size,
        })
    }

    /// Returns the maximum session keys held or contended concurrently.
    pub const fn max_active_sessions(self) -> usize {
        self.max_active_sessions
    }

    /// Returns the maximum queued callers behind one held session lease.
    pub const fn max_waiters_per_session(self) -> usize {
        self.max_waiters_per_session
    }

    /// Returns the maximum rows accepted by one diagnostic page request.
    pub const fn max_diagnostic_page_size(self) -> usize {
        self.max_diagnostic_page_size
    }
}

/// Invalid caller-supplied mutation diagnostic page size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionMutationPageError {
    requested: usize,
    maximum: usize,
}

impl SessionMutationPageError {
    /// Returns the rejected requested row count.
    pub const fn requested(self) -> usize {
        self.requested
    }

    /// Returns the configured inclusive maximum row count.
    pub const fn maximum(self) -> usize {
        self.maximum
    }
}

impl Display for SessionMutationPageError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "mutation diagnostic page size {} must be within 1..={}",
            self.requested, self.maximum
        )
    }
}

impl Error for SessionMutationPageError {}

/// Lock-consistent aggregate mutation coordination pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionMutationPressure {
    /// Session keys currently held or contended.
    pub active_sessions: usize,
    /// Session keys with an issued exclusive lease.
    pub held_sessions: usize,
    /// Callers waiting behind held session leases.
    pub waiting_callers: usize,
    /// Requests rejected by active-session or per-session waiter bounds.
    pub capacity_rejections: u64,
}

/// Payload-free pressure for one active session mutation key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMutationDiagnostic {
    /// Stable session identity.
    pub session_id: SessionId,
    /// Whether one caller currently owns the mutation lease.
    pub held: bool,
    /// Callers queued behind the current lease.
    pub waiting_callers: usize,
    /// Whether another waiter would be rejected by the configured bound.
    pub at_waiter_limit: bool,
}

/// Exact bounded work used to build one mutation diagnostic page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionMutationDiagnosticCost {
    /// Ordered registry entries examined, including at most one look-ahead.
    pub session_entries_examined: usize,
    /// Bounded active entries sampled to construct aggregate pressure.
    pub pressure_entries_sampled: usize,
}

/// Stable session-ID-ordered page of mutation coordination pressure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMutationDiagnosticPage {
    /// Payload-free diagnostic rows.
    pub items: Vec<SessionMutationDiagnostic>,
    /// Exclusive session-ID cursor for the next page when more rows exist.
    pub next_after: Option<SessionId>,
    /// Aggregate pressure captured under the same registry lock.
    pub pressure: SessionMutationPressure,
    /// Exact bounded page construction work.
    pub cost: SessionMutationDiagnosticCost,
}

/// Dynamically keyed coordinator serializing only same-session mutations.
///
/// Contended acquisition yields its future. Dropping that future removes its
/// waiter; dropping a lease wakes the first remaining registered waiter.
pub struct KeyedSessionMutationCoordinator {
    limits: SessionMutationCoordinatorLimits,
    entries: Arc<Mutex<BTreeMap<SessionId, Arc<MutationEntry>>>>,
    capacity_rejections: AtomicU64,
    waiting_callers: AtomicUsize,
    telemetry: Option<Arc<dyn Telemetry>>,
}

impl KeyedSessionMutationCoordinator {
    /// Constructs an empty coordinator with explicit hard bounds.
    ///
    /// Construction performs no IO and does not panic.
    pub fn new(limits: SessionMutationCoordinatorLimits) -> Self {
        Self::build(limits, None)
    }

    /// Constructs an empty coordinator that publishes bounded pressure metrics.
    ///
    /// Metric emission contains no session identifiers or command payloads and
    /// must be non-blocking. Construction performs no IO and does not panic.
    pub fn with_telemetry(
        limits: SessionMutationCoordinatorLimits,
        telemetry: Arc<dyn Telemetry>,
    ) -> Self {
        Self::build(limits, Some(telemetry))
    }

    fn build(
        limits: SessionMutationCoordinatorLimits,
        telemetry: Option<Arc<dyn Telemetry>>,
    ) -> Self {
        Self {
            limits,
            entries: Arc::new(Mutex::new(BTreeMap::new())),
            capacity_rejections: AtomicU64::new(0),
            waiting_callers: AtomicUsize::new(0),
            telemetry,
        }
    }

    /// Returns lock-consistent aggregate pressure without waiting for leases.
    pub fn pressure(&self) -> SessionMutationPressure {
        let entries = self.lock_entries();
        self.pressure_from_entries(&entries)
    }

    /// Reads one stable, hard-bounded page of active session mutation keys.
    ///
    /// The page contains no command or payload data. Work is proportional to
    /// `limit + 1`, while aggregate pressure examines only the already-bounded
    /// active registry. This function performs no external IO and does not
    /// panic.
    ///
    /// # Errors
    ///
    /// Returns a typed error when `limit` is zero or above the configured page
    /// maximum.
    pub fn diagnostic_page(
        &self,
        after: Option<&SessionId>,
        limit: usize,
    ) -> Result<SessionMutationDiagnosticPage, SessionMutationPageError> {
        if limit == 0 || limit > self.limits.max_diagnostic_page_size {
            return Err(SessionMutationPageError {
                requested: limit,
                maximum: self.limits.max_diagnostic_page_size,
            });
        }
        let entries = self.lock_entries();
        let pressure = self.pressure_from_entries(&entries);
        let range = match after {
            Some(after) => entries.range((Excluded(after), Unbounded)),
            None => entries.range::<SessionId, _>((
                std::ops::Bound::<&SessionId>::Unbounded,
                std::ops::Bound::<&SessionId>::Unbounded,
            )),
        };
        let mut examined = 0_usize;
        let mut rows = Vec::with_capacity(limit.saturating_add(1));
        for (session_id, entry) in range.take(limit.saturating_add(1)) {
            examined = examined.saturating_add(1);
            let state = entry.lock_state();
            rows.push(SessionMutationDiagnostic {
                session_id: session_id.clone(),
                held: state.held,
                waiting_callers: state.waiters.len(),
                at_waiter_limit: state.waiters.len() >= self.limits.max_waiters_per_session,
            });
        }
        let has_more = rows.len() > limit;
        if has_more {
            rows.pop();
        }
        let next_after = has_more
            .then(|| rows.last().map(|row| row.session_id.clone()))
            .flatten();
        Ok(SessionMutationDiagnosticPage {
            items: rows,
            next_after,
            pressure,
            cost: SessionMutationDiagnosticCost {
                session_entries_examined: examined,
                pressure_entries_sampled: entries.len(),
            },
        })
    }

    fn pressure_from_entries(
        &self,
        entries: &BTreeMap<SessionId, Arc<MutationEntry>>,
    ) -> SessionMutationPressure {
        let (held_sessions, waiting_callers) =
            entries
                .values()
                .fold((0_usize, 0_usize), |(held_total, waiting_total), entry| {
                    let state = entry.lock_state();
                    (
                        held_total.saturating_add(usize::from(state.held)),
                        waiting_total.saturating_add(state.waiters.len()),
                    )
                });
        SessionMutationPressure {
            active_sessions: entries.len(),
            held_sessions,
            waiting_callers,
            capacity_rejections: self.capacity_rejections.load(Ordering::Acquire),
        }
    }

    fn lock_entries(&self) -> std::sync::MutexGuard<'_, BTreeMap<SessionId, Arc<MutationEntry>>> {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn capacity_error(
        &self,
        session_id: &SessionId,
        reason: &'static str,
        started_at: Instant,
    ) -> PortError {
        self.capacity_rejections.fetch_add(1, Ordering::AcqRel);
        self.record_metric(MetricName::OverloadRejections, 1.0);
        self.record_command_duration(started_at);
        PortError::new(
            PortErrorKind::CapacityExceeded,
            PortOperation::AcquireSessionMutation,
            Some(session_id.clone()),
            RuntimeAdapterFailure(reason),
        )
    }

    fn record_waiter_depth(&self) {
        self.record_metric(
            MetricName::SupervisorMailboxDepth,
            self.waiting_callers.load(Ordering::Acquire) as f64,
        );
    }

    fn record_command_duration(&self, started_at: Instant) {
        self.record_metric(
            MetricName::SupervisorCommandDurationSeconds,
            started_at.elapsed().as_secs_f64(),
        );
    }

    fn record_metric(&self, name: MetricName, value: f64) {
        if let Some(telemetry) = self.telemetry.as_ref() {
            telemetry.record_metric(MetricObservation {
                name,
                value,
                operation: None,
            });
        }
    }
}

impl SessionMutationCoordinator for KeyedSessionMutationCoordinator {
    fn acquire<'a>(
        &'a self,
        session_id: &'a SessionId,
    ) -> PortFuture<'a, Result<Box<dyn SessionMutationLease>, PortError>> {
        Box::pin(async move {
            let started_at = Instant::now();
            let (entry, waiter) = {
                let mut entries = self.lock_entries();
                let entry = if let Some(entry) = entries.get(session_id) {
                    entry.clone()
                } else {
                    if entries.len() >= self.limits.max_active_sessions {
                        drop(entries);
                        return Err(self.capacity_error(
                            session_id,
                            "active session mutation key capacity reached",
                            started_at,
                        ));
                    }
                    let entry = Arc::new(MutationEntry::default());
                    entries.insert(session_id.clone(), entry.clone());
                    entry
                };
                let mut state = entry.lock_state();
                if !state.held && state.waiters.is_empty() {
                    state.held = true;
                    drop(state);
                    drop(entries);
                    (entry, None)
                } else {
                    if state.waiters.len() >= self.limits.max_waiters_per_session {
                        drop(state);
                        drop(entries);
                        return Err(self.capacity_error(
                            session_id,
                            "session mutation waiter capacity reached",
                            started_at,
                        ));
                    }
                    let waiter = Arc::new(MutationWaiter::default());
                    state.waiters.push_back(waiter.clone());
                    self.waiting_callers.fetch_add(1, Ordering::AcqRel);
                    drop(state);
                    drop(entries);
                    self.record_waiter_depth();
                    (entry, Some(waiter))
                }
            };
            if let Some(waiter) = waiter {
                let mut pending = PendingMutationWaiter {
                    coordinator: self,
                    session_id,
                    entry: &entry,
                    waiter: &waiter,
                    acquired: false,
                };
                loop {
                    {
                        let mut state = entry.lock_state();
                        if !state.held
                            && state
                                .waiters
                                .front()
                                .is_some_and(|front| Arc::ptr_eq(front, &waiter))
                        {
                            state.waiters.pop_front();
                            state.held = true;
                            pending.acquired = true;
                            break;
                        }
                    }
                    let _ = waiter.notification.recv().await;
                }
            }
            self.record_command_duration(started_at);
            Ok(Box::new(KeyedSessionMutationLease {
                session_id: session_id.clone(),
                entry,
                entries: Arc::downgrade(&self.entries),
            }) as Box<dyn SessionMutationLease>)
        })
    }
}

#[derive(Default)]
struct MutationEntry {
    state: Mutex<MutationEntryState>,
}

impl MutationEntry {
    fn lock_state(&self) -> std::sync::MutexGuard<'_, MutationEntryState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[derive(Default)]
struct MutationEntryState {
    held: bool,
    waiters: VecDeque<Arc<MutationWaiter>>,
}

struct MutationWaiter {
    available: async_channel::Sender<()>,
    notification: async_channel::Receiver<()>,
}

impl Default for MutationWaiter {
    fn default() -> Self {
        let (available, notification) = async_channel::bounded(1);
        Self {
            available,
            notification,
        }
    }
}

struct PendingMutationWaiter<'a> {
    coordinator: &'a KeyedSessionMutationCoordinator,
    session_id: &'a SessionId,
    entry: &'a Arc<MutationEntry>,
    waiter: &'a Arc<MutationWaiter>,
    acquired: bool,
}

impl Drop for PendingMutationWaiter<'_> {
    fn drop(&mut self) {
        if !self.acquired {
            {
                let mut state = self.entry.lock_state();
                state
                    .waiters
                    .retain(|waiter| !Arc::ptr_eq(waiter, self.waiter));
                if !state.held
                    && let Some(waiter) = state.waiters.front()
                {
                    let _ = waiter.available.try_send(());
                }
            }
            remove_unused_entry(self.session_id, self.entry, &self.coordinator.entries);
        }
        self.coordinator
            .waiting_callers
            .fetch_sub(1, Ordering::AcqRel);
        self.coordinator.record_waiter_depth();
    }
}

struct KeyedSessionMutationLease {
    session_id: SessionId,
    entry: Arc<MutationEntry>,
    entries: Weak<Mutex<BTreeMap<SessionId, Arc<MutationEntry>>>>,
}

impl SessionMutationLease for KeyedSessionMutationLease {}

impl Drop for KeyedSessionMutationLease {
    fn drop(&mut self) {
        // Release the entry lock before registry cleanup: acquisition takes the
        // registry lock first, so holding both in reverse order could deadlock.
        // See [coordination guide](../docs/internals/coordination/scheduling.md#keep-mutation-ownership-separate-from-process-ownership).
        {
            let mut state = self.entry.lock_state();
            state.held = false;
            if let Some(waiter) = state.waiters.front() {
                let _ = waiter.available.try_send(());
            }
        }
        let Some(entries) = self.entries.upgrade() else {
            return;
        };
        remove_unused_entry(&self.session_id, &self.entry, &entries);
    }
}

fn remove_unused_entry(
    session_id: &SessionId,
    entry: &Arc<MutationEntry>,
    entries: &Mutex<BTreeMap<SessionId, Arc<MutationEntry>>>,
) {
    let mut entries = entries
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if !entries
        .get(session_id)
        .is_some_and(|registered| Arc::ptr_eq(registered, entry))
    {
        return;
    }
    let state = entry.lock_state();
    if !state.held && state.waiters.is_empty() {
        drop(state);
        entries.remove(session_id);
    }
}
