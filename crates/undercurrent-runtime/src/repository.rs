// SPDX-License-Identifier: Apache-2.0

//! Bounded transactional in-memory session and outbox repository.

use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Mutex;

use undercurrent_application::{
    CommitSessionOutcome, DeleteSessionOutcome, InsertSessionOutcome, OutboxAcknowledgeOutcome,
    OutboxSequence, PortError, PortErrorKind, PortFuture, PortOperation, RecoverySessionBatch,
    SessionOutbox, SessionOutboxEvent, SessionOutboxRecord, SessionRecoverySource,
    SessionRepository,
};
use undercurrent_domain::{Revision, Session, SessionId, SessionName};

use crate::{RuntimeAdapterFailure, RuntimeLimitError};

/// Bounded repository pressure without hydrating aggregate copies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InMemoryRepositoryPressure {
    /// Session records retained in this process; not persistent across restart.
    pub sessions: usize,
    /// Pending transactional outbox records.
    pub outbox_events: usize,
    /// Inclusive configured session capacity.
    pub session_limit: usize,
    /// Inclusive configured outbox capacity.
    pub outbox_limit: usize,
}

/// Transactional repository used by embedded runtimes and cross-boundary tests.
///
/// Session, name-index, and outbox changes are atomic under one mutex, but are
/// not crash-durable. Production persistence belongs to a durable repository
/// adapter, such as the daemon's SQLite implementation.
pub struct InMemorySessionRepository {
    max_sessions: usize,
    max_outbox_events: usize,
    state: Mutex<RepositoryState>,
}

impl InMemorySessionRepository {
    /// Constructs an empty repository with hard aggregate and outbox limits.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed zero-limit failure.
    pub fn new(max_sessions: usize, max_outbox_events: usize) -> Result<Self, RuntimeLimitError> {
        if max_sessions == 0 {
            return Err(RuntimeLimitError::zero("max_sessions"));
        }
        if max_outbox_events == 0 {
            return Err(RuntimeLimitError::zero("max_outbox_events"));
        }
        Ok(Self {
            max_sessions,
            max_outbox_events,
            state: Mutex::new(RepositoryState::default()),
        })
    }

    /// Returns lock-consistent repository pressure.
    pub fn pressure(&self) -> InMemoryRepositoryPressure {
        let state = self.lock_state();
        InMemoryRepositoryPressure {
            sessions: state.sessions.len(),
            outbox_events: state.outbox.len(),
            session_limit: self.max_sessions,
            outbox_limit: self.max_outbox_events,
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, RepositoryState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn capacity_error(
        operation: PortOperation,
        session_id: Option<SessionId>,
        resource: &'static str,
    ) -> PortError {
        PortError::new(
            PortErrorKind::CapacityExceeded,
            operation,
            session_id,
            RuntimeAdapterFailure(resource),
        )
    }

    fn invalid_event(
        operation: PortOperation,
        session_id: &SessionId,
        rule: &'static str,
    ) -> PortError {
        PortError::new(
            PortErrorKind::InvalidData,
            operation,
            Some(session_id.clone()),
            RuntimeAdapterFailure(rule),
        )
    }
}

impl SessionRepository for InMemorySessionRepository {
    fn insert<'a>(
        &'a self,
        session: &'a Session,
        event: &'a SessionOutboxEvent,
    ) -> PortFuture<'a, Result<InsertSessionOutcome, PortError>> {
        Box::pin(async move {
            if event.session_id != *session.id() || event.revision != session.revision() {
                return Err(Self::invalid_event(
                    PortOperation::InsertSession,
                    session.id(),
                    "initial outbox event does not match session identity and revision",
                ));
            }
            let mut state = self.lock_state();
            if state.sessions.contains_key(session.id()) {
                return Ok(InsertSessionOutcome::IdConflict);
            }
            if let Some(existing_session_id) = state.names.get(session.name()) {
                return Ok(InsertSessionOutcome::NameConflict {
                    existing_session_id: existing_session_id.clone(),
                });
            }
            if state.sessions.len() >= self.max_sessions {
                return Err(Self::capacity_error(
                    PortOperation::InsertSession,
                    Some(session.id().clone()),
                    "session repository capacity exhausted",
                ));
            }
            if state.outbox.len() >= self.max_outbox_events {
                return Err(Self::capacity_error(
                    PortOperation::InsertSession,
                    Some(session.id().clone()),
                    "session outbox capacity exhausted",
                ));
            }
            state
                .append_outbox(std::slice::from_ref(event), session)
                .map_err(|()| {
                    Self::capacity_error(
                        PortOperation::InsertSession,
                        Some(session.id().clone()),
                        "session outbox sequence exhausted",
                    )
                })?;
            state
                .names
                .insert(session.name().clone(), session.id().clone());
            state.sessions.insert(session.id().clone(), session.clone());
            Ok(InsertSessionOutcome::Inserted)
        })
    }

    fn load<'a>(
        &'a self,
        session_id: &'a SessionId,
    ) -> PortFuture<'a, Result<Option<Session>, PortError>> {
        Box::pin(async move { Ok(self.lock_state().sessions.get(session_id).cloned()) })
    }

    fn find_by_name<'a>(
        &'a self,
        name: &'a SessionName,
    ) -> PortFuture<'a, Result<Option<Session>, PortError>> {
        Box::pin(async move {
            let state = self.lock_state();
            Ok(state
                .names
                .get(name)
                .and_then(|session_id| state.sessions.get(session_id))
                .cloned())
        })
    }

    fn commit<'a>(
        &'a self,
        session: &'a Session,
        expected_revision: Revision,
        events: &'a [SessionOutboxEvent],
    ) -> PortFuture<'a, Result<CommitSessionOutcome, PortError>> {
        Box::pin(async move {
            if events.iter().any(|event| event.session_id != *session.id())
                || events
                    .last()
                    .is_none_or(|event| event.revision != session.revision())
            {
                return Err(Self::invalid_event(
                    PortOperation::CommitSession,
                    session.id(),
                    "committed outbox events do not end at the aggregate revision",
                ));
            }
            let mut state = self.lock_state();
            let Some(current) = state.sessions.get(session.id()) else {
                return Ok(CommitSessionOutcome::NotFound);
            };
            if current.revision() != expected_revision {
                return Ok(CommitSessionOutcome::RevisionConflict {
                    current_revision: current.revision(),
                });
            }
            if let Some(existing_session_id) = state.names.get(session.name())
                && existing_session_id != session.id()
            {
                return Ok(CommitSessionOutcome::NameConflict {
                    existing_session_id: existing_session_id.clone(),
                });
            }
            let previous_name = current.name().clone();
            if state.outbox.len().saturating_add(events.len()) > self.max_outbox_events {
                return Err(Self::capacity_error(
                    PortOperation::CommitSession,
                    Some(session.id().clone()),
                    "session outbox capacity exhausted",
                ));
            }
            state.append_outbox(events, session).map_err(|()| {
                Self::capacity_error(
                    PortOperation::CommitSession,
                    Some(session.id().clone()),
                    "session outbox sequence exhausted",
                )
            })?;
            if &previous_name != session.name() {
                state.names.remove(&previous_name);
                state
                    .names
                    .insert(session.name().clone(), session.id().clone());
            }
            state.sessions.insert(session.id().clone(), session.clone());
            Ok(CommitSessionOutcome::Committed)
        })
    }

    fn delete<'a>(
        &'a self,
        session_id: &'a SessionId,
        expected_revision: Revision,
        event: &'a SessionOutboxEvent,
    ) -> PortFuture<'a, Result<DeleteSessionOutcome, PortError>> {
        Box::pin(async move {
            if event.session_id != *session_id {
                return Err(Self::invalid_event(
                    PortOperation::DeleteSession,
                    session_id,
                    "removal outbox event does not match session identity",
                ));
            }
            let mut state = self.lock_state();
            let Some(current) = state.sessions.get(session_id) else {
                return Ok(DeleteSessionOutcome::NotFound);
            };
            if current.revision() != expected_revision {
                return Ok(DeleteSessionOutcome::RevisionConflict {
                    current_revision: current.revision(),
                });
            }
            if state.outbox.len() >= self.max_outbox_events {
                return Err(Self::capacity_error(
                    PortOperation::DeleteSession,
                    Some(session_id.clone()),
                    "session outbox capacity exhausted",
                ));
            }
            let snapshot = current.clone();
            state
                .append_outbox(std::slice::from_ref(event), &snapshot)
                .map_err(|()| {
                    Self::capacity_error(
                        PortOperation::DeleteSession,
                        Some(session_id.clone()),
                        "session outbox sequence exhausted",
                    )
                })?;
            let removed = state.sessions.remove(session_id).ok_or_else(|| {
                Self::invalid_event(
                    PortOperation::DeleteSession,
                    session_id,
                    "session disappeared inside repository transaction",
                )
            })?;
            state.names.remove(removed.name());
            Ok(DeleteSessionOutcome::Deleted)
        })
    }
}

impl SessionOutbox for InMemorySessionRepository {
    fn read_pending<'a>(
        &'a self,
        limit: usize,
    ) -> PortFuture<'a, Result<Vec<SessionOutboxRecord>, PortError>> {
        Box::pin(async move {
            if limit == 0 {
                return Err(PortError::new(
                    PortErrorKind::InvalidData,
                    PortOperation::ReadOutbox,
                    None,
                    RuntimeAdapterFailure("outbox read limit must be non-zero"),
                ));
            }
            Ok(self
                .lock_state()
                .outbox
                .iter()
                .take(limit)
                .cloned()
                .collect())
        })
    }

    fn acknowledge_through<'a>(
        &'a self,
        through: OutboxSequence,
    ) -> PortFuture<'a, Result<OutboxAcknowledgeOutcome, PortError>> {
        Box::pin(async move {
            let mut state = self.lock_state();
            if let Some(last_acknowledged) = state.last_acknowledged
                && through <= last_acknowledged
            {
                return Ok(OutboxAcknowledgeOutcome::AlreadyAcknowledged {
                    through: last_acknowledged,
                });
            }
            let Some(position) = state
                .outbox
                .iter()
                .position(|record| record.sequence == through)
            else {
                return Ok(OutboxAcknowledgeOutcome::UnknownSequence { requested: through });
            };
            let records = position.saturating_add(1);
            state.outbox.drain(..records);
            state.last_acknowledged = Some(through);
            Ok(OutboxAcknowledgeOutcome::Acknowledged { records, through })
        })
    }
}

impl SessionRecoverySource for InMemorySessionRepository {
    fn read_batch<'a>(
        &'a self,
        after: Option<&'a SessionId>,
        limit: usize,
    ) -> PortFuture<'a, Result<RecoverySessionBatch, PortError>> {
        Box::pin(async move {
            if limit == 0 {
                return Err(PortError::new(
                    PortErrorKind::InvalidData,
                    PortOperation::ReadRecoverySessions,
                    None,
                    RuntimeAdapterFailure("recovery batch limit must be non-zero"),
                ));
            }
            let state = self.lock_state();
            let lower = after.map_or(Unbounded, Excluded);
            let mut sessions = state.sessions.range((lower, Unbounded));
            let batch = sessions
                .by_ref()
                .take(limit)
                .map(|(_, session)| session.clone())
                .collect::<Vec<_>>();
            let has_more = sessions.next().is_some();
            let next_after = if has_more {
                batch.last().map(|session| session.id().clone())
            } else {
                None
            };
            Ok(RecoverySessionBatch {
                sessions: batch,
                next_after,
            })
        })
    }
}

struct RepositoryState {
    sessions: BTreeMap<SessionId, Session>,
    names: BTreeMap<SessionName, SessionId>,
    outbox: VecDeque<SessionOutboxRecord>,
    next_outbox_sequence: OutboxSequence,
    last_acknowledged: Option<OutboxSequence>,
}

impl RepositoryState {
    fn append_outbox(
        &mut self,
        events: &[SessionOutboxEvent],
        session: &Session,
    ) -> Result<(), ()> {
        // Stage every checked sequence advance before mutating the outbox.
        // Callers update the aggregate only after this succeeds, so exhaustion
        // must leave both sides of their transaction unchanged.
        // See [coordination guide](../docs/internals/coordination/events.md#transactional-does-not-mean-persistent).
        let mut sequence = self.next_outbox_sequence;
        let mut records = Vec::with_capacity(events.len());
        for event in events {
            let next = sequence.checked_next().ok_or(())?;
            records.push(SessionOutboxRecord {
                snapshot: Some(session.clone()),
                sequence,
                event: event.clone(),
            });
            sequence = next;
        }
        self.outbox.extend(records);
        self.next_outbox_sequence = sequence;
        Ok(())
    }
}

impl Default for RepositoryState {
    fn default() -> Self {
        Self {
            sessions: BTreeMap::new(),
            names: BTreeMap::new(),
            outbox: VecDeque::new(),
            next_outbox_sequence: OutboxSequence::FIRST,
            last_acknowledged: None,
        }
    }
}
