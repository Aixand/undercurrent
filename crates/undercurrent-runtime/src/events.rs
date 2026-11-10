// SPDX-License-Identifier: Apache-2.0

//! Bounded indexed retention and selective subscriptions for session events.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

use undercurrent_application::{
    InvalidSessionEventRequest, OpenSessionEventSubscription, OutboxSequence, PortError,
    PortErrorKind, PortFuture, PortOperation, SessionEventFilter, SessionEventGap,
    SessionEventPublishOutcome, SessionEventPublisher, SessionEventQueryCost,
    SessionEventReadBatch, SessionEventReadRequest, SessionEventReader, SessionEventStart,
    SessionEventSubscriber, SessionEventSubscriptionPort, SessionOutboxRecord, SubscriptionWake,
};
use undercurrent_domain::{DomainEventKind, SessionId};

use crate::{RuntimeAdapterFailure, RuntimeLimitError};

/// Current bounded retained event-journal and subscription state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionEventJournalPressure {
    /// Complete records currently retained for replay.
    pub retained_records: usize,
    /// Inclusive configured record limit.
    pub record_limit: usize,
    /// Oldest retained sequence, when non-empty.
    pub oldest_retained: Option<OutboxSequence>,
    /// Latest sequence ever accepted by this journal instance.
    pub latest_published: Option<OutboxSequence>,
    /// Records evicted since this journal instance started.
    pub evicted_records: u64,
    /// Live selective subscriptions.
    pub subscriptions: usize,
    /// Subscriptions with matching retained work pending.
    pub dirty_subscriptions: usize,
    /// Inclusive configured subscription limit.
    pub subscription_limit: usize,
}

/// Stable process-local identity for one journal subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionEventSubscriptionId(u64);

impl SessionEventSubscriptionId {
    /// Returns the monotonic process-local numeric value.
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl Display for SessionEventSubscriptionId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Failure while opening or receiving a selective journal subscription.
#[derive(Debug)]
pub enum SessionEventSubscriptionError {
    /// Live subscription capacity is exhausted.
    CapacityExceeded {
        /// Inclusive configured capacity.
        limit: usize,
    },
    /// Process-local subscription identity cannot advance.
    IdentityExhausted,
    /// The internally derived resume request was invalid.
    InvalidRequest(InvalidSessionEventRequest),
    /// Retained journal read failed.
    Read(PortError),
    /// Subscription was explicitly closed, dropped, or lost its journal.
    Closed,
    /// Caller-supplied receive timeout elapsed.
    Timeout,
}

impl Display for SessionEventSubscriptionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CapacityExceeded { limit } => {
                write!(
                    formatter,
                    "session event subscription capacity {limit} is exhausted"
                )
            }
            Self::IdentityExhausted => {
                formatter.write_str("session event subscription identity counter is exhausted")
            }
            Self::InvalidRequest(error) => Display::fmt(error, formatter),
            Self::Read(error) => Display::fmt(error, formatter),
            Self::Closed => formatter.write_str("session event subscription is closed"),
            Self::Timeout => formatter.write_str("session event subscription wait timed out"),
        }
    }
}

impl Error for SessionEventSubscriptionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidRequest(error) => Some(error),
            Self::Read(error) => Some(error),
            Self::CapacityExceeded { .. }
            | Self::IdentityExhausted
            | Self::Closed
            | Self::Timeout => None,
        }
    }
}

/// Initial coherent replay batch and its registered live continuation.
pub struct SessionEventSubscriptionOpen {
    initial: SessionEventReadBatch,
    subscription: SessionEventSubscription,
}

impl SessionEventSubscriptionOpen {
    /// Returns the coherent initial replay batch.
    pub const fn initial(&self) -> &SessionEventReadBatch {
        &self.initial
    }

    /// Consumes the open result into replay and live continuation ownership.
    pub fn into_parts(self) -> (SessionEventReadBatch, SessionEventSubscription) {
        (self.initial, self.subscription)
    }
}

/// In-memory idempotent event sink with hard retention and subscription bounds.
pub struct BoundedSessionEventJournal {
    maximum_records: usize,
    maximum_subscriptions: usize,
    next_subscription_id: AtomicU64,
    state: Arc<Mutex<SessionEventJournalState>>,
}

impl BoundedSessionEventJournal {
    /// Constructs an empty event journal with independent hard bounds.
    ///
    /// The record bound must be at least the configured outbox dispatch batch
    /// so an ambiguous acknowledgement retry remains replayable. Construction
    /// performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed zero-limit failure.
    pub fn new(
        maximum_records: usize,
        maximum_subscriptions: usize,
    ) -> Result<Self, RuntimeLimitError> {
        if maximum_records == 0 {
            return Err(RuntimeLimitError::zero("session_event_records"));
        }
        if maximum_subscriptions == 0 {
            return Err(RuntimeLimitError::zero("session_event_subscriptions"));
        }
        Ok(Self {
            maximum_records,
            maximum_subscriptions,
            next_subscription_id: AtomicU64::new(0),
            state: Arc::new(Mutex::new(SessionEventJournalState::default())),
        })
    }

    /// Returns a bounded oldest-first copy of all currently retained records.
    ///
    /// This diagnostic method cannot exceed the configured journal bound,
    /// performs no external IO, and does not panic.
    pub fn retained_records(&self) -> Vec<SessionOutboxRecord> {
        let state = self.lock_state();
        state
            .order
            .iter()
            .filter_map(|sequence| state.records.get(sequence))
            .cloned()
            .collect()
    }

    /// Opens replay and live continuation under one journal lock.
    ///
    /// A publication cannot fall between the initial snapshot and registration.
    /// The subscription retains no event payload queue; it stores a checkpoint
    /// and one coalesced dirty bit, then rereads bounded retained records. This
    /// function performs no external IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns capacity, identity, or retained-read failures.
    pub fn subscribe(
        &self,
        request: &SessionEventReadRequest,
    ) -> Result<SessionEventSubscriptionOpen, SessionEventSubscriptionError> {
        self.subscribe_inner(request, None)
    }

    /// Opens replay and live continuation with a payload-free outer wake.
    ///
    /// The callback runs only on a clean-to-dirty transition and carries no
    /// event data. Snapshot and registration remain atomic. This function
    /// performs no external IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns capacity, identity, or retained-read failures.
    pub fn subscribe_with_wake(
        &self,
        request: &SessionEventReadRequest,
        wake: Arc<dyn SubscriptionWake>,
    ) -> Result<SessionEventSubscriptionOpen, SessionEventSubscriptionError> {
        self.subscribe_inner(request, Some(wake))
    }

    fn subscribe_inner(
        &self,
        request: &SessionEventReadRequest,
        wake: Option<Arc<dyn SubscriptionWake>>,
    ) -> Result<SessionEventSubscriptionOpen, SessionEventSubscriptionError> {
        let mut journal = self.lock_state();
        journal
            .subscriptions
            .retain(|_, subscription| subscription.strong_count() > 0);
        if journal.subscriptions.len() >= self.maximum_subscriptions {
            return Err(SessionEventSubscriptionError::CapacityExceeded {
                limit: self.maximum_subscriptions,
            });
        }
        let initial =
            Self::read_locked(&journal, request).map_err(SessionEventSubscriptionError::Read)?;
        let id = self.next_subscription_id()?;
        let state = Arc::new(SubscriptionState {
            filter: request.filter().clone(),
            wake,
            watch: Mutex::new(SubscriptionWatch {
                checkpoint: initial.checkpoint(),
                dirty: initial.has_more(),
                closed: false,
            }),
            changed: Condvar::new(),
        });
        journal.subscriptions.insert(id, Arc::downgrade(&state));
        Ok(SessionEventSubscriptionOpen {
            initial,
            subscription: SessionEventSubscription {
                id,
                page_limit: request.limit(),
                state,
                journal: Arc::downgrade(&self.state),
            },
        })
    }

    /// Returns lock-consistent retention, loss, and subscription pressure.
    pub fn pressure(&self) -> SessionEventJournalPressure {
        let mut state = self.lock_state();
        let mut dirty_subscriptions = 0;
        state.subscriptions.retain(|_, candidate| {
            let Some(subscription) = candidate.upgrade() else {
                return false;
            };
            let watch = subscription
                .watch
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            dirty_subscriptions += usize::from(watch.dirty && !watch.closed);
            !watch.closed
        });
        SessionEventJournalPressure {
            retained_records: state.records.len(),
            record_limit: self.maximum_records,
            oldest_retained: state.order.front().copied(),
            latest_published: state.latest_published,
            evicted_records: state.evicted_records,
            subscriptions: state.subscriptions.len(),
            dirty_subscriptions,
            subscription_limit: self.maximum_subscriptions,
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, SessionEventJournalState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn next_subscription_id(
        &self,
    ) -> Result<SessionEventSubscriptionId, SessionEventSubscriptionError> {
        self.next_subscription_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map(|previous| SessionEventSubscriptionId(previous + 1))
            .map_err(|_| SessionEventSubscriptionError::IdentityExhausted)
    }

    fn read_locked(
        state: &SessionEventJournalState,
        request: &SessionEventReadRequest,
    ) -> Result<SessionEventReadBatch, PortError> {
        if let SessionEventStart::Resume { after } = request.start()
            && state.latest_published.is_some_and(|latest| after > latest)
        {
            return Err(Self::read_error(
                PortErrorKind::InvalidData,
                "session event checkpoint is ahead of journal head",
            ));
        }

        let gap = retention_gap(state, request.start());
        if request.start() == SessionEventStart::Live {
            return Ok(SessionEventReadBatch::new(
                Vec::new(),
                state.latest_published,
                None,
                false,
                SessionEventQueryCost::default(),
            ));
        }

        let (sequences, has_more, cost) = match request.start() {
            SessionEventStart::Tail => {
                let (mut sequences, cost) =
                    collect_descending(state, request.filter(), request.limit());
                sequences.reverse();
                (sequences, false, cost)
            }
            SessionEventStart::Earliest | SessionEventStart::Resume { .. } => {
                collect_ascending(state, request.filter(), request.start(), request.limit())
            }
            SessionEventStart::Live => (Vec::new(), false, 0),
        };
        let records = sequences
            .iter()
            .filter_map(|sequence| state.records.get(sequence))
            .cloned()
            .collect::<Vec<_>>();
        let checkpoint = if has_more {
            records.last().map(|record| record.sequence)
        } else {
            state.latest_published.or_else(|| match request.start() {
                SessionEventStart::Resume { after } => Some(after),
                SessionEventStart::Earliest | SessionEventStart::Live | SessionEventStart::Tail => {
                    None
                }
            })
        };
        Ok(SessionEventReadBatch::new(
            records,
            checkpoint,
            gap,
            has_more,
            SessionEventQueryCost {
                index_entries_examined: cost,
            },
        ))
    }

    fn publication_error(
        record: &SessionOutboxRecord,
        kind: PortErrorKind,
        rule: &'static str,
    ) -> PortError {
        PortError::new(
            kind,
            PortOperation::PublishSessionEvent,
            Some(record.event.session_id.clone()),
            RuntimeAdapterFailure(rule),
        )
    }

    fn read_error(kind: PortErrorKind, rule: &'static str) -> PortError {
        PortError::new(
            kind,
            PortOperation::ReadSessionEvents,
            None,
            RuntimeAdapterFailure(rule),
        )
    }
}

impl SessionEventPublisher for BoundedSessionEventJournal {
    fn publish<'a>(
        &'a self,
        record: &'a SessionOutboxRecord,
    ) -> PortFuture<'a, Result<SessionEventPublishOutcome, PortError>> {
        Box::pin(async move {
            let mut state = self.lock_state();
            if let Some(existing) = state.records.get(&record.sequence) {
                if existing == record {
                    return Ok(SessionEventPublishOutcome::AlreadyPublished);
                }
                return Err(Self::publication_error(
                    record,
                    PortErrorKind::Conflict,
                    "session event sequence was reused for different content",
                ));
            }
            if let Some(latest) = state.latest_published {
                if record.sequence <= latest {
                    return Err(Self::publication_error(
                        record,
                        PortErrorKind::ReplayUnavailable,
                        "session event idempotency record is no longer retained",
                    ));
                }
                if latest.checked_next() != Some(record.sequence) {
                    return Err(Self::publication_error(
                        record,
                        PortErrorKind::InvalidData,
                        "session event publication sequence is not contiguous",
                    ));
                }
            }

            state.order.push_back(record.sequence);
            state.records.insert(record.sequence, record.clone());
            state
                .by_session
                .entry(record.event.session_id.clone())
                .or_default()
                .insert(record.sequence);
            state
                .by_kind
                .entry(record.event.kind)
                .or_default()
                .insert(record.sequence);
            state.latest_published = Some(record.sequence);

            if state.records.len() > self.maximum_records
                && let Some(evicted) = state.order.pop_front()
                && let Some(evicted_record) = state.records.remove(&evicted)
            {
                remove_index_sequence(
                    &mut state.by_session,
                    &evicted_record.event.session_id,
                    evicted,
                );
                remove_index_sequence(&mut state.by_kind, &evicted_record.event.kind, evicted);
                state.evicted_records = state.evicted_records.saturating_add(1);
            }

            let mut wakes = Vec::new();
            state.subscriptions.retain(|_, candidate| {
                let Some(subscription) = candidate.upgrade() else {
                    return false;
                };
                let mut watch = subscription
                    .watch
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if watch.closed {
                    return false;
                }
                if subscription.filter.matches(record) {
                    if !watch.dirty
                        && let Some(wake) = &subscription.wake
                    {
                        wakes.push(Arc::clone(wake));
                    }
                    watch.dirty = true;
                    subscription.changed.notify_one();
                }
                true
            });
            drop(state);
            for wake in wakes {
                wake.wake();
            }
            Ok(SessionEventPublishOutcome::Published)
        })
    }
}

impl SessionEventReader for BoundedSessionEventJournal {
    fn read<'a>(
        &'a self,
        request: &'a SessionEventReadRequest,
    ) -> PortFuture<'a, Result<SessionEventReadBatch, PortError>> {
        Box::pin(async move { Self::read_locked(&self.lock_state(), request) })
    }
}

impl SessionEventSubscriber for BoundedSessionEventJournal {
    fn subscribe(
        &self,
        request: &SessionEventReadRequest,
        wake: Arc<dyn SubscriptionWake>,
    ) -> Result<OpenSessionEventSubscription, PortError> {
        let opened = self
            .subscribe_with_wake(request, wake)
            .map_err(subscription_port_error)?;
        let (initial, subscription) = opened.into_parts();
        Ok(OpenSessionEventSubscription::new(
            initial,
            Box::new(subscription),
        ))
    }
}

/// Independently owned selective subscription with checkpoint-only buffering.
pub struct SessionEventSubscription {
    id: SessionEventSubscriptionId,
    page_limit: usize,
    state: Arc<SubscriptionState>,
    journal: Weak<Mutex<SessionEventJournalState>>,
}

impl SessionEventSubscription {
    /// Returns the process-local subscription identity.
    pub const fn id(&self) -> SessionEventSubscriptionId {
        self.id
    }

    /// Reads one pending retained batch without waiting.
    ///
    /// This function retains only a checkpoint between calls, performs no
    /// external IO, and does not panic.
    ///
    /// # Errors
    ///
    /// Returns closure, invalid derived request, or journal read failure.
    pub fn try_receive(
        &self,
    ) -> Result<Option<SessionEventReadBatch>, SessionEventSubscriptionError> {
        let mut watch = self.lock_watch();
        if watch.closed {
            return Err(SessionEventSubscriptionError::Closed);
        }
        if !watch.dirty {
            return Ok(None);
        }
        watch.dirty = false;
        let checkpoint = watch.checkpoint;
        drop(watch);
        self.read_after(checkpoint).map(Some)
    }

    /// Waits for matching publication and reads one bounded retained batch.
    ///
    /// The wait retains no event payloads and wakes only for matching interest.
    /// A slow receiver observes an explicit gap after journal eviction. This
    /// function may block the calling thread for `timeout`, performs no external
    /// IO, and does not panic.
    ///
    /// # Errors
    ///
    /// Returns timeout, closure, invalid derived request, or journal read failure.
    pub fn receive_timeout(
        &self,
        timeout: Duration,
    ) -> Result<SessionEventReadBatch, SessionEventSubscriptionError> {
        let deadline = Instant::now().checked_add(timeout);
        let mut watch = self.lock_watch();
        loop {
            if watch.closed {
                return Err(SessionEventSubscriptionError::Closed);
            }
            if watch.dirty {
                break;
            }
            let Some(deadline) = deadline else {
                return Err(SessionEventSubscriptionError::Timeout);
            };
            let now = Instant::now();
            if now >= deadline {
                return Err(SessionEventSubscriptionError::Timeout);
            }
            let remaining = deadline.saturating_duration_since(now);
            let waited = self
                .state
                .changed
                .wait_timeout(watch, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            watch = waited.0;
            if waited.1.timed_out() && !watch.dirty {
                return Err(SessionEventSubscriptionError::Timeout);
            }
        }

        watch.dirty = false;
        let checkpoint = watch.checkpoint;
        drop(watch);

        self.read_after(checkpoint)
    }

    fn read_after(
        &self,
        checkpoint: Option<OutboxSequence>,
    ) -> Result<SessionEventReadBatch, SessionEventSubscriptionError> {
        let filter = self.state.filter.clone();
        let start = checkpoint.map_or(SessionEventStart::Earliest, |after| {
            SessionEventStart::Resume { after }
        });
        let request = SessionEventReadRequest::new(filter, start, self.page_limit, self.page_limit)
            .map_err(SessionEventSubscriptionError::InvalidRequest)?;
        let journal = self
            .journal
            .upgrade()
            .ok_or(SessionEventSubscriptionError::Closed)?;
        let batch = {
            let journal = journal
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            BoundedSessionEventJournal::read_locked(&journal, &request)
                .map_err(SessionEventSubscriptionError::Read)?
        };

        let mut watch = self.lock_watch();
        if watch.closed {
            return Err(SessionEventSubscriptionError::Closed);
        }
        watch.checkpoint = batch.checkpoint();
        // Publication can mark this watch dirty after the journal read and
        // before this lock is reacquired. Preserve that wake even if this batch
        // exhausted the earlier snapshot; retained pages also need another drain.
        // See [coordination guide](../docs/internals/coordination/events.md#own-subscriptions-and-their-cancellation-explicitly).
        watch.dirty |= batch.has_more();
        Ok(batch)
    }

    /// Closes only this logical subscription and wakes a blocked receiver.
    ///
    /// Repeated closure is idempotent. This function performs no external IO
    /// and does not panic.
    pub fn close(&self) {
        {
            let mut watch = self.lock_watch();
            if watch.closed {
                return;
            }
            watch.closed = true;
            self.state.changed.notify_all();
        }
        if let Some(journal) = self.journal.upgrade() {
            journal
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .subscriptions
                .remove(&self.id);
        }
    }

    fn lock_watch(&self) -> std::sync::MutexGuard<'_, SubscriptionWatch> {
        self.state
            .watch
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Drop for SessionEventSubscription {
    fn drop(&mut self) {
        self.close();
    }
}

impl SessionEventSubscriptionPort for SessionEventSubscription {
    fn try_receive(&self) -> Result<Option<SessionEventReadBatch>, PortError> {
        SessionEventSubscription::try_receive(self).map_err(subscription_port_error)
    }

    fn close(&self) {
        SessionEventSubscription::close(self);
    }
}

#[derive(Default)]
struct SessionEventJournalState {
    order: VecDeque<OutboxSequence>,
    records: BTreeMap<OutboxSequence, SessionOutboxRecord>,
    by_session: BTreeMap<SessionId, BTreeSet<OutboxSequence>>,
    by_kind: BTreeMap<DomainEventKind, BTreeSet<OutboxSequence>>,
    latest_published: Option<OutboxSequence>,
    evicted_records: u64,
    subscriptions: BTreeMap<SessionEventSubscriptionId, Weak<SubscriptionState>>,
}

struct SubscriptionState {
    filter: SessionEventFilter,
    wake: Option<Arc<dyn SubscriptionWake>>,
    watch: Mutex<SubscriptionWatch>,
    changed: Condvar,
}

struct SubscriptionWatch {
    checkpoint: Option<OutboxSequence>,
    dirty: bool,
    closed: bool,
}

fn subscription_port_error(error: SessionEventSubscriptionError) -> PortError {
    let (kind, session_id) = match &error {
        SessionEventSubscriptionError::CapacityExceeded { .. } => {
            (PortErrorKind::CapacityExceeded, None)
        }
        SessionEventSubscriptionError::IdentityExhausted => (PortErrorKind::Other, None),
        SessionEventSubscriptionError::InvalidRequest(_) => (PortErrorKind::InvalidData, None),
        SessionEventSubscriptionError::Read(source) => {
            (source.kind(), source.session_id().cloned())
        }
        SessionEventSubscriptionError::Closed => (PortErrorKind::Interrupted, None),
        SessionEventSubscriptionError::Timeout => (PortErrorKind::Timeout, None),
    };
    PortError::new(
        kind,
        PortOperation::SubscribeSessionEvents,
        session_id,
        error,
    )
}

fn retention_gap(
    state: &SessionEventJournalState,
    start: SessionEventStart,
) -> Option<SessionEventGap> {
    let SessionEventStart::Resume { after } = start else {
        return None;
    };
    let oldest = state.order.front().copied()?;
    let missing_from = after.checked_next()?;
    let missing_through = oldest.checked_previous()?;
    (missing_from <= missing_through)
        .then(|| SessionEventGap::new(missing_from, missing_through, oldest))
}

fn collect_ascending(
    state: &SessionEventJournalState,
    filter: &SessionEventFilter,
    start: SessionEventStart,
    limit: usize,
) -> (Vec<OutboxSequence>, bool, usize) {
    let after = match start {
        SessionEventStart::Resume { after } => Some(after),
        SessionEventStart::Earliest => None,
        SessionEventStart::Live | SessionEventStart::Tail => None,
    };
    let mut sequences = Vec::new();
    let mut cost = 0usize;
    if let Some(indexes) = selected_indexes(state, filter) {
        let mut iterators: Vec<Box<dyn Iterator<Item = OutboxSequence> + '_>> = indexes
            .into_iter()
            .map(|index| match after {
                Some(after) => Box::new(index.range((Excluded(after), Unbounded)).copied())
                    as Box<dyn Iterator<Item = OutboxSequence>>,
                None => Box::new(index.iter().copied()),
            })
            .collect();
        let mut heap = BinaryHeap::new();
        for (index, iterator) in iterators.iter_mut().enumerate() {
            if let Some(sequence) = iterator.next() {
                heap.push(Reverse((sequence, index)));
            }
        }
        while let Some(Reverse((sequence, iterator_index))) = heap.pop() {
            cost = cost.saturating_add(1);
            if state
                .records
                .get(&sequence)
                .is_some_and(|record| filter.matches(record))
            {
                sequences.push(sequence);
                if sequences.len() > limit {
                    break;
                }
            }
            if let Some(next) = iterators[iterator_index].next() {
                heap.push(Reverse((next, iterator_index)));
            }
        }
    } else {
        let iterator: Box<dyn Iterator<Item = (&OutboxSequence, &SessionOutboxRecord)>> =
            match after {
                Some(after) => Box::new(state.records.range((Excluded(after), Unbounded))),
                None => Box::new(state.records.iter()),
            };
        for (sequence, record) in iterator {
            cost = cost.saturating_add(1);
            if filter.matches(record) {
                sequences.push(*sequence);
                if sequences.len() > limit {
                    break;
                }
            }
        }
    }
    let has_more = sequences.len() > limit;
    if has_more {
        sequences.pop();
    }
    (sequences, has_more, cost)
}

fn collect_descending(
    state: &SessionEventJournalState,
    filter: &SessionEventFilter,
    limit: usize,
) -> (Vec<OutboxSequence>, usize) {
    let mut sequences = Vec::new();
    let mut cost = 0usize;
    if let Some(indexes) = selected_indexes(state, filter) {
        let mut iterators: Vec<Box<dyn Iterator<Item = OutboxSequence> + '_>> = indexes
            .into_iter()
            .map(|index| {
                Box::new(index.iter().rev().copied()) as Box<dyn Iterator<Item = OutboxSequence>>
            })
            .collect();
        let mut heap = BinaryHeap::new();
        for (index, iterator) in iterators.iter_mut().enumerate() {
            if let Some(sequence) = iterator.next() {
                heap.push((sequence, index));
            }
        }
        while let Some((sequence, iterator_index)) = heap.pop() {
            cost = cost.saturating_add(1);
            if state
                .records
                .get(&sequence)
                .is_some_and(|record| filter.matches(record))
            {
                sequences.push(sequence);
                if sequences.len() == limit {
                    break;
                }
            }
            if let Some(next) = iterators[iterator_index].next() {
                heap.push((next, iterator_index));
            }
        }
    } else {
        for (sequence, record) in state.records.iter().rev() {
            cost = cost.saturating_add(1);
            if filter.matches(record) {
                sequences.push(*sequence);
                if sequences.len() == limit {
                    break;
                }
            }
        }
    }
    (sequences, cost)
}

fn selected_indexes<'a>(
    state: &'a SessionEventJournalState,
    filter: &SessionEventFilter,
) -> Option<Vec<&'a BTreeSet<OutboxSequence>>> {
    let sessions = (!filter.sessions().is_empty()).then(|| {
        filter
            .sessions()
            .iter()
            .filter_map(|session_id| state.by_session.get(session_id))
            .collect::<Vec<_>>()
    });
    let kinds = (!filter.kinds().is_empty()).then(|| {
        filter
            .kinds()
            .iter()
            .filter_map(|kind| state.by_kind.get(kind))
            .collect::<Vec<_>>()
    });
    match (sessions, kinds) {
        (None, None) => None,
        (Some(indexes), None) | (None, Some(indexes)) => Some(indexes),
        (Some(sessions), Some(kinds)) => {
            let session_entries = sessions.iter().map(|index| index.len()).sum::<usize>();
            let kind_entries = kinds.iter().map(|index| index.len()).sum::<usize>();
            Some(if session_entries <= kind_entries {
                sessions
            } else {
                kinds
            })
        }
    }
}

fn remove_index_sequence<K: Ord>(
    index: &mut BTreeMap<K, BTreeSet<OutboxSequence>>,
    key: &K,
    sequence: OutboxSequence,
) {
    let remove_key = index.get_mut(key).is_some_and(|sequences| {
        sequences.remove(&sequence);
        sequences.is_empty()
    });
    if remove_key {
        index.remove(key);
    }
}
