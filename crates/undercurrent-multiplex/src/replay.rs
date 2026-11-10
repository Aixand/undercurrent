// SPDX-License-Identifier: Apache-2.0

//! Bounded per-stream replay journal with server-epoch invalidation.
//!
//! Retention is volatile and keyed by stable client identity plus stream ID.
//! The adapter records complete writes and supplies reconnect deadlines; no timer
//! or transport acknowledgement is owned here. Byte eviction removes its ordering
//! metadata too, and atomic resume activation prevents expiration races during
//! ownership transfer. Returned byte references can outlive journal eviction.

use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use undercurrent_wire::{ClientId, ServerEpoch, StreamId, StreamSequence};

use crate::{MultiplexError, StreamFrameKind, StreamGap, StreamGapReason, StreamRecoveryAction};

/// Invalid replay retention bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayLimitError {
    /// A required limit was zero.
    ZeroLimit {
        /// Stable invalid field.
        field: &'static str,
    },
    /// Per-stream retention exceeded total retention.
    StreamBudgetExceeded,
    /// One replay item maximum exceeded per-stream retention.
    ItemBudgetExceeded,
}

impl Display for ReplayLimitError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroLimit { field } => write!(formatter, "replay limit {field} must be non-zero"),
            Self::StreamBudgetExceeded => {
                formatter.write_str("per-stream replay bytes exceed total replay bytes")
            }
            Self::ItemBudgetExceeded => {
                formatter.write_str("replay item bytes exceed per-stream replay bytes")
            }
        }
    }
}

impl Error for ReplayLimitError {}

/// Validated bounded replay retention shared within one server process epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayLimits {
    max_streams: usize,
    per_stream_bytes: u64,
    total_bytes: u64,
    max_item_bytes: u64,
}

impl ReplayLimits {
    /// Constructs explicit stream-count, per-stream, total, and item bounds.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed zero or nested-budget validation failure.
    pub const fn new(
        max_streams: usize,
        per_stream_bytes: u64,
        total_bytes: u64,
        max_item_bytes: u64,
    ) -> Result<Self, ReplayLimitError> {
        if max_streams == 0 {
            return Err(ReplayLimitError::ZeroLimit {
                field: "max_streams",
            });
        }
        if per_stream_bytes == 0 {
            return Err(ReplayLimitError::ZeroLimit {
                field: "per_stream_bytes",
            });
        }
        if total_bytes == 0 {
            return Err(ReplayLimitError::ZeroLimit {
                field: "total_bytes",
            });
        }
        if max_item_bytes == 0 {
            return Err(ReplayLimitError::ZeroLimit {
                field: "max_item_bytes",
            });
        }
        if per_stream_bytes > total_bytes {
            return Err(ReplayLimitError::StreamBudgetExceeded);
        }
        if max_item_bytes > per_stream_bytes {
            return Err(ReplayLimitError::ItemBudgetExceeded);
        }
        Ok(Self {
            max_streams,
            per_stream_bytes,
            total_bytes,
            max_item_bytes,
        })
    }

    /// Returns maximum retained stream journals.
    pub const fn max_streams(self) -> usize {
        self.max_streams
    }

    /// Returns maximum retained bytes for one stream.
    pub const fn per_stream_bytes(self) -> u64 {
        self.per_stream_bytes
    }

    /// Returns maximum retained bytes across all streams.
    pub const fn total_bytes(self) -> u64 {
        self.total_bytes
    }

    /// Returns maximum bytes accepted in one replay item.
    pub const fn max_item_bytes(self) -> u64 {
        self.max_item_bytes
    }
}

/// Bounded retention evicted while accepting one replay item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayRecordOutcome {
    /// Older items evicted.
    pub evicted_items: usize,
    /// Exact older encoded bytes released.
    pub evicted_bytes: u64,
}

/// One retained encoded stream frame returned during resume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayedFrame {
    /// Monotonic stream sequence.
    pub sequence: StreamSequence,
    /// Wire kind retained so replay preserves gap credit exemption.
    pub kind: StreamFrameKind,
    /// Shared exact encoded frame bytes.
    pub bytes: Arc<[u8]>,
}

/// Result of one stream resume request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayResult {
    /// All retained frames strictly after the acknowledged sequence.
    Items(Vec<ReplayedFrame>),
    /// The journal requires explicit recovery and has no suffix to replay.
    Gap(StreamGap),
    /// An evicted prefix followed by the retained suffix from the same snapshot.
    ///
    /// Deliver the gap before these frames and continue new publication after
    /// the final retained sequence. The suffix does not repair the missing data.
    GapWithItems {
        /// Missing prefix and its required application recovery.
        gap: StreamGap,
        /// Retained frames in sequence order, all after the gap.
        items: Vec<ReplayedFrame>,
    },
}

/// Atomic replay selection and retained-stream ownership outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayActivation {
    /// Whether activation created a new empty journal.
    pub created: bool,
    /// Prior reconnect deadline restored if outer activation rolls back.
    pub previous_expiration: Option<Instant>,
    /// Retained frames, an explicit recovery gap, or a gap followed by frames.
    pub replay: ReplayResult,
}

/// Current bounded replay retention pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayPressure {
    /// Registered retained stream journals.
    pub streams: usize,
    /// Retained encoded frames.
    pub items: usize,
    /// Exact retained encoded bytes.
    pub bytes: u64,
    /// Journals currently owned by live connection streams.
    pub active_streams: usize,
    /// Disconnected journals awaiting their retention deadline.
    pub expiring_streams: usize,
}

/// Replay journals and retained data released by one expiration pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayExpirationOutcome {
    /// Inactive stream journals removed.
    pub streams: usize,
    /// Retained encoded frames removed.
    pub items: usize,
    /// Exact retained encoded bytes released.
    pub bytes: u64,
}

/// Stable replay identity across physical client connection epochs.
///
/// Stream IDs are only unique inside one client connection. Pairing them with
/// the stable client ID prevents unrelated clients from sharing replay state.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReplayStreamKey {
    client_id: ClientId,
    stream_id: StreamId,
}

impl ReplayStreamKey {
    /// Constructs a replay key for one client's logical stream.
    ///
    /// This function performs no IO and does not panic.
    pub const fn new(client_id: ClientId, stream_id: StreamId) -> Self {
        Self {
            client_id,
            stream_id,
        }
    }

    /// Returns the stable client identity.
    pub const fn client_id(&self) -> &ClientId {
        &self.client_id
    }

    /// Returns the client-scoped logical stream identity.
    pub const fn stream_id(&self) -> &StreamId {
        &self.stream_id
    }
}

/// Thread-safe replay journal scoped to one server process epoch.
pub struct ReplayJournal {
    server_epoch: ServerEpoch,
    limits: ReplayLimits,
    state: Mutex<ReplayState>,
}

impl ReplayJournal {
    /// Constructs an empty bounded journal for one server epoch.
    ///
    /// Construction performs no IO and does not panic.
    pub fn new(server_epoch: ServerEpoch, limits: ReplayLimits) -> Self {
        Self {
            server_epoch,
            limits,
            state: Mutex::new(ReplayState::default()),
        }
    }

    /// Registers one resumable stream and its deterministic recovery action.
    ///
    /// # Errors
    ///
    /// Returns active-ID conflict or configured stream capacity failure.
    pub fn register_stream(
        &self,
        key: ReplayStreamKey,
        recovery: StreamRecoveryAction,
    ) -> Result<(), MultiplexError> {
        let mut state = self.lock_state();
        expire_due_streams(&mut state, Instant::now());
        if state.streams.contains_key(&key) {
            return Err(MultiplexError::StreamIdInUse {
                stream_id: key.stream_id,
            });
        }
        if state.streams.len() >= self.limits.max_streams() {
            return Err(MultiplexError::CapacityExceeded {
                resource: "replay_streams",
                limit: self.limits.max_streams(),
            });
        }
        state.streams.insert(key, ReplayStream::new(recovery));
        Ok(())
    }

    /// Activates a retained stream journal for reconnect processing.
    ///
    /// Existing inactive state and its original recovery action are preserved.
    /// A still-active journal is rejected so two physical connections cannot
    /// own the same client stream. This function performs no IO and does not
    /// panic.
    ///
    /// # Errors
    ///
    /// Returns active-ID conflict or configured stream capacity failure when a
    /// new journal is needed.
    pub fn ensure_stream(
        &self,
        key: ReplayStreamKey,
        recovery: StreamRecoveryAction,
        now: Instant,
    ) -> Result<bool, MultiplexError> {
        let mut state = self.lock_state();
        expire_due_streams(&mut state, now);
        if let Some(stream) = state.streams.get_mut(&key) {
            if stream.active {
                return Err(MultiplexError::StreamIdInUse {
                    stream_id: key.stream_id,
                });
            }
            let expiration = stream.expires_at.take();
            stream.active = true;
            if let Some(expiration) = expiration {
                state.expirations.remove(&(expiration, key));
            }
            return Ok(false);
        }
        if state.streams.len() >= self.limits.max_streams() {
            return Err(MultiplexError::CapacityExceeded {
                resource: "replay_streams",
                limit: self.limits.max_streams(),
            });
        }
        state.streams.insert(key, ReplayStream::new(recovery));
        Ok(true)
    }

    /// Marks a retained stream inactive until its reconnect deadline.
    ///
    /// Repeating deactivation replaces the prior deadline without duplicating
    /// expiration-index entries. Unknown streams are idempotent no-ops. This
    /// function performs no IO and does not panic.
    pub fn deactivate_stream(&self, key: &ReplayStreamKey, expires_at: Instant) -> bool {
        let mut state = self.lock_state();
        let previous = match state.streams.get_mut(key) {
            Some(stream) => {
                stream.active = false;
                stream.expires_at.replace(expires_at)
            }
            None => return false,
        };
        if let Some(previous) = previous {
            state.expirations.remove(&(previous, key.clone()));
        }
        state.expirations.insert((expires_at, key.clone()), ());
        true
    }

    /// Removes inactive journals whose reconnect deadline has elapsed.
    ///
    /// Expiration uses an ordered deadline index and therefore visits only due
    /// journals rather than scanning every retained stream. This function
    /// performs no IO and does not panic.
    pub fn expire_before(&self, now: Instant) -> ReplayExpirationOutcome {
        let mut state = self.lock_state();
        expire_due_streams(&mut state, now)
    }

    /// Records one strictly ordered encoded data frame with bounded eviction.
    ///
    /// Call only after a complete successful write for newly produced data.
    /// This method trusts encoded metadata, permits increasing sequence jumps,
    /// and does not check active ownership or sweep expiration deadlines.
    ///
    /// Per-stream and total oldest items are evicted before insertion. Global
    /// ordering metadata is removed with every eviction, so stable retained
    /// byte use cannot hide unbounded bookkeeping growth. This function
    /// performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns unknown-stream, invalid-size, out-of-order sequence, or ordinal
    /// exhaustion failure.
    pub fn record(
        &self,
        key: &ReplayStreamKey,
        sequence: StreamSequence,
        bytes: Arc<[u8]>,
    ) -> Result<ReplayRecordOutcome, MultiplexError> {
        self.record_frame(key, sequence, StreamFrameKind::Data, bytes)
    }

    /// Records a confirmed stream frame together with its wire kind.
    ///
    /// The adapter must supply a kind matching the encoded bytes. In particular,
    /// gaps must retain their credit exemption even when replayed in a data class.
    /// Write ordering, ownership, and eviction obligations are the same as
    /// [`record`](Self::record); this method does not parse or validate wire bytes.
    ///
    /// # Errors
    ///
    /// Returns the same size, stream, sequence, or ordinal failures as `record`.
    pub fn record_frame(
        &self,
        key: &ReplayStreamKey,
        sequence: StreamSequence,
        kind: StreamFrameKind,
        bytes: Arc<[u8]>,
    ) -> Result<ReplayRecordOutcome, MultiplexError> {
        let item_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if item_bytes == 0 || item_bytes > self.limits.max_item_bytes() {
            return Err(MultiplexError::InvalidFrameSize {
                actual: item_bytes,
                maximum: self.limits.max_item_bytes(),
            });
        }
        let mut state = self.lock_state();
        let stream = state
            .streams
            .get(key)
            .ok_or_else(|| MultiplexError::UnknownStream {
                stream_id: key.stream_id.clone(),
            })?;
        if sequence <= stream.latest {
            return Err(MultiplexError::StreamSequenceOutOfOrder {
                stream_id: key.stream_id.clone(),
                previous: stream.latest.value(),
                received: sequence.value(),
            });
        }
        // Reserve the next insertion identity before eviction so counter exhaustion
        // cannot discard retained history for a frame we cannot insert.
        let ordinal =
            state
                .next_ordinal
                .checked_add(1)
                .ok_or(MultiplexError::CounterExhausted {
                    counter: "replay_ordinal",
                })?;

        let mut outcome = ReplayRecordOutcome {
            evicted_items: 0,
            evicted_bytes: 0,
        };
        while state.streams.get(key).is_some_and(|stream| {
            stream.bytes.saturating_add(item_bytes) > self.limits.per_stream_bytes()
        }) {
            let removed = state
                .streams
                .get_mut(key)
                .and_then(ReplayStream::pop_front)
                .ok_or(MultiplexError::CounterExhausted {
                    counter: "per_stream_replay_eviction",
                })?;
            state.global_order.remove(&removed.ordinal);
            state.total_bytes = state.total_bytes.saturating_sub(removed.bytes_len());
            outcome.evicted_items = outcome.evicted_items.saturating_add(1);
            outcome.evicted_bytes = outcome.evicted_bytes.saturating_add(removed.bytes_len());
        }
        while state.total_bytes.saturating_add(item_bytes) > self.limits.total_bytes() {
            let (&oldest, global) =
                state
                    .global_order
                    .first_key_value()
                    .ok_or(MultiplexError::CounterExhausted {
                        counter: "global_replay_eviction",
                    })?;
            let owner = global.key.clone();
            let removed = state
                .streams
                .get_mut(&owner)
                .and_then(|stream| stream.remove_ordinal(oldest))
                .ok_or(MultiplexError::CounterExhausted {
                    counter: "global_replay_index",
                })?;
            state.global_order.remove(&oldest);
            state.total_bytes = state.total_bytes.saturating_sub(removed.bytes_len());
            outcome.evicted_items = outcome.evicted_items.saturating_add(1);
            outcome.evicted_bytes = outcome.evicted_bytes.saturating_add(removed.bytes_len());
        }

        let entry = ReplayEntry {
            ordinal,
            sequence,
            kind,
            bytes,
        };
        state
            .streams
            .get_mut(key)
            .ok_or_else(|| MultiplexError::UnknownStream {
                stream_id: key.stream_id.clone(),
            })?
            .push(entry.clone());
        state
            .global_order
            .insert(ordinal, GlobalReplayEntry { key: key.clone() });
        state.next_ordinal = ordinal;
        state.total_bytes = state.total_bytes.saturating_add(item_bytes);
        Ok(outcome)
    }

    /// Resumes one stream after its last acknowledged sequence.
    ///
    /// A different server epoch always returns a typed gap rather than
    /// pretending in-memory state survived. Unknown and evicted streams also
    /// return explicit recovery actions. Partial eviction returns the missing
    /// prefix together with its retained suffix so live publication can continue
    /// after the journal's latest sequence. This function performs no IO and
    /// does not panic.
    ///
    /// # Errors
    ///
    /// Returns a future-sequence failure.
    pub fn resume(
        &self,
        requested_epoch: &ServerEpoch,
        key: &ReplayStreamKey,
        after: StreamSequence,
        fallback_recovery: StreamRecoveryAction,
        now: Instant,
    ) -> Result<ReplayResult, MultiplexError> {
        let mut state = self.lock_state();
        expire_due_streams(&mut state, now);
        replay_result(
            &self.server_epoch,
            requested_epoch,
            key,
            after,
            fallback_recovery,
            state.streams.get(key),
        )
    }

    /// Selects replay and activates its retained journal under one lock.
    ///
    /// A due inactive journal is removed before replay selection. The selected
    /// result and ownership transition are therefore consistent at `now`, even
    /// when another connection concurrently sweeps expired journals. This
    /// function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns active-ID conflict, future-sequence, or configured stream
    /// capacity failure.
    pub fn activate_resume(
        &self,
        requested_epoch: &ServerEpoch,
        key: ReplayStreamKey,
        after: StreamSequence,
        fallback_recovery: StreamRecoveryAction,
        now: Instant,
    ) -> Result<ReplayActivation, MultiplexError> {
        let mut state = self.lock_state();
        expire_due_streams(&mut state, now);
        // Select against the old state before creating an empty journal. Otherwise
        // an absent/expired key resumed from INITIAL would appear caught up rather
        // than requiring recovery. Keep selection and activation under this lock.
        // See [resume activation](../docs/internals/replay.md#activate-and-roll-back-a-reconnect).
        let replay = replay_result(
            &self.server_epoch,
            requested_epoch,
            &key,
            after,
            fallback_recovery,
            state.streams.get(&key),
        )?;
        let (created, previous_expiration) = if let Some(stream) = state.streams.get_mut(&key) {
            if stream.active {
                return Err(MultiplexError::StreamIdInUse {
                    stream_id: key.stream_id,
                });
            }
            let expiration = stream.expires_at.take();
            stream.active = true;
            if let Some(expiration) = expiration {
                state.expirations.remove(&(expiration, key));
            }
            (false, expiration)
        } else {
            if state.streams.len() >= self.limits.max_streams() {
                return Err(MultiplexError::CapacityExceeded {
                    resource: "replay_streams",
                    limit: self.limits.max_streams(),
                });
            }
            state
                .streams
                .insert(key, ReplayStream::new(fallback_recovery));
            (true, None)
        };
        Ok(ReplayActivation {
            created,
            previous_expiration,
            replay,
        })
    }

    /// Explicitly removes one retained journal, whether active or inactive.
    ///
    /// Returns exact bytes released. Unknown streams are idempotent no-ops.
    /// This function performs no IO and does not panic.
    pub fn remove_stream(&self, key: &ReplayStreamKey) -> u64 {
        let mut state = self.lock_state();
        let Some(stream) = remove_stream_from_state(&mut state, key) else {
            return 0;
        };
        stream.bytes
    }

    /// Expires every due inactive journal and returns a lock-consistent bounded
    /// replay pressure snapshot.
    pub fn pressure(&self) -> ReplayPressure {
        let mut state = self.lock_state();
        expire_due_streams(&mut state, Instant::now());
        ReplayPressure {
            streams: state.streams.len(),
            items: state.global_order.len(),
            bytes: state.total_bytes,
            active_streams: state
                .streams
                .values()
                .filter(|stream| stream.active)
                .count(),
            expiring_streams: state.expirations.len(),
        }
    }

    /// Returns the server process epoch governing all retained state.
    pub const fn server_epoch(&self) -> &ServerEpoch {
        &self.server_epoch
    }

    /// Returns immutable replay retention limits for adapter validation.
    pub const fn limits(&self) -> ReplayLimits {
        self.limits
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, ReplayState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[derive(Default)]
struct ReplayState {
    streams: BTreeMap<ReplayStreamKey, ReplayStream>,
    global_order: BTreeMap<u64, GlobalReplayEntry>,
    expirations: BTreeMap<(Instant, ReplayStreamKey), ()>,
    next_ordinal: u64,
    total_bytes: u64,
}

struct ReplayStream {
    recovery: StreamRecoveryAction,
    items: VecDeque<ReplayEntry>,
    bytes: u64,
    latest: StreamSequence,
    evicted_through: Option<StreamSequence>,
    active: bool,
    expires_at: Option<Instant>,
}

impl ReplayStream {
    fn new(recovery: StreamRecoveryAction) -> Self {
        Self {
            recovery,
            items: VecDeque::new(),
            bytes: 0,
            latest: StreamSequence::INITIAL,
            evicted_through: None,
            active: true,
            expires_at: None,
        }
    }

    fn push(&mut self, entry: ReplayEntry) {
        self.bytes = self.bytes.saturating_add(entry.bytes_len());
        self.latest = entry.sequence;
        self.items.push_back(entry);
    }

    fn pop_front(&mut self) -> Option<ReplayEntry> {
        let entry = self.items.pop_front()?;
        self.bytes = self.bytes.saturating_sub(entry.bytes_len());
        self.evicted_through = Some(entry.sequence);
        Some(entry)
    }

    fn remove_ordinal(&mut self, ordinal: u64) -> Option<ReplayEntry> {
        let index = self
            .items
            .iter()
            .position(|entry| entry.ordinal == ordinal)?;
        let entry = self.items.remove(index)?;
        self.bytes = self.bytes.saturating_sub(entry.bytes_len());
        self.evicted_through = Some(
            self.evicted_through
                .map_or(entry.sequence, |current| current.max(entry.sequence)),
        );
        Some(entry)
    }
}

#[derive(Clone)]
struct ReplayEntry {
    ordinal: u64,
    sequence: StreamSequence,
    kind: StreamFrameKind,
    bytes: Arc<[u8]>,
}

impl ReplayEntry {
    fn bytes_len(&self) -> u64 {
        u64::try_from(self.bytes.len()).unwrap_or(u64::MAX)
    }
}

struct GlobalReplayEntry {
    key: ReplayStreamKey,
}

fn remove_stream_from_state(
    state: &mut ReplayState,
    key: &ReplayStreamKey,
) -> Option<ReplayStream> {
    let stream = state.streams.remove(key)?;
    if let Some(expiration) = stream.expires_at {
        state.expirations.remove(&(expiration, key.clone()));
    }
    for item in &stream.items {
        state.global_order.remove(&item.ordinal);
    }
    state.total_bytes = state.total_bytes.saturating_sub(stream.bytes);
    Some(stream)
}

fn expire_due_streams(state: &mut ReplayState, now: Instant) -> ReplayExpirationOutcome {
    let mut outcome = ReplayExpirationOutcome {
        streams: 0,
        items: 0,
        bytes: 0,
    };
    while let Some(((deadline, key), ())) = state.expirations.first_key_value() {
        if *deadline > now {
            break;
        }
        let deadline = *deadline;
        let key = key.clone();
        state.expirations.remove(&(deadline, key.clone()));
        let due = state
            .streams
            .get(&key)
            .is_some_and(|stream| !stream.active && stream.expires_at == Some(deadline));
        if !due {
            continue;
        }
        if let Some(removed) = remove_stream_from_state(state, &key) {
            outcome.streams = outcome.streams.saturating_add(1);
            outcome.items = outcome.items.saturating_add(removed.items.len());
            outcome.bytes = outcome.bytes.saturating_add(removed.bytes);
        }
    }
    outcome
}

fn replay_result(
    server_epoch: &ServerEpoch,
    requested_epoch: &ServerEpoch,
    key: &ReplayStreamKey,
    after: StreamSequence,
    fallback_recovery: StreamRecoveryAction,
    stream: Option<&ReplayStream>,
) -> Result<ReplayResult, MultiplexError> {
    let first_requested = after.next().unwrap_or(after);
    // Sequence comparisons only establish continuity within the same epoch.
    // Check the epoch first so an old checkpoint yields recovery even when it
    // exceeds this journal's latest sequence or the key is absent.
    // See [resume decisions](../docs/internals/replay.md#resume-decision-reference).
    if requested_epoch != server_epoch {
        let latest = stream.map_or(first_requested, |stream| stream.latest.max(first_requested));
        return Ok(ReplayResult::Gap(StreamGap {
            stream_id: key.stream_id.clone(),
            first_missing: first_requested,
            last_missing: latest,
            reason: StreamGapReason::ServerEpochChanged,
            recovery: stream.map_or(fallback_recovery, |stream| stream.recovery),
        }));
    }
    let Some(stream) = stream else {
        return Ok(ReplayResult::Gap(StreamGap {
            stream_id: key.stream_id.clone(),
            first_missing: first_requested,
            last_missing: first_requested,
            reason: StreamGapReason::ReplayUnavailable,
            recovery: fallback_recovery,
        }));
    };
    if after > stream.latest {
        return Err(MultiplexError::FutureResumeSequence {
            stream_id: key.stream_id.clone(),
            requested: after.value(),
            latest: stream.latest.value(),
        });
    }
    if after == stream.latest {
        return Ok(ReplayResult::Items(Vec::new()));
    }
    // Select the suffix under the same journal lock as the eviction watermark.
    // Returning only the gap would rewind the publisher below retained history;
    // a later lookup could lose more history to concurrent global eviction.
    let items: Vec<_> = stream
        .items
        .iter()
        .filter(|item| item.sequence > after)
        .map(|item| ReplayedFrame {
            sequence: item.sequence,
            kind: item.kind,
            bytes: item.bytes.clone(),
        })
        .collect();
    if let Some(evicted) = stream.evicted_through.filter(|evicted| after < *evicted) {
        let gap = StreamGap {
            stream_id: key.stream_id.clone(),
            first_missing: first_requested,
            last_missing: evicted,
            reason: StreamGapReason::ReplayEvicted,
            recovery: stream.recovery,
        };
        return Ok(if items.is_empty() {
            ReplayResult::Gap(gap)
        } else {
            ReplayResult::GapWithItems { gap, items }
        });
    }
    Ok(ReplayResult::Items(items))
}
