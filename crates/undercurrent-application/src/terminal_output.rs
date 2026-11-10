// SPDX-License-Identifier: Apache-2.0

//! Bounded, offset-addressed terminal output retention and replay.

use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};

use undercurrent_domain::{OutputCaptureMode, OutputChunkIntegrity, PayloadDigest, SessionId};

use crate::{
    AccessAction, AccessDecision, AccessDenialReason, AccessResource, ApplicationErrorCategory,
    ApplicationOperation, AuthorizationPort, Clock, DiagnosticEventName, DiagnosticLevel,
    DiagnosticRecord, MetricName, MetricObservation, PortError, SubscriptionWake, Telemetry,
};

/// Hard limits for retained and replayed terminal output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalOutputLimits {
    /// Maximum retained bytes for one session.
    pub per_session_bytes: usize,
    /// Maximum retained bytes across every session.
    pub global_bytes: usize,
    /// Maximum bytes copied into one reference-counted retained chunk.
    pub chunk_bytes: usize,
    /// Maximum bytes accepted by one append call.
    pub append_bytes: usize,
    /// Maximum bytes returned by one bounded replay extraction.
    pub replay_bytes: usize,
    /// Maximum distinct session offset records retained by the store.
    pub sessions: usize,
    /// Maximum concurrently registered checkpoint-only output subscriptions.
    pub subscriptions: usize,
}

impl TerminalOutputLimits {
    /// Validates non-zero, internally consistent output retention limits.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed invalid-limit error when a chunk cannot fit within both
    /// byte budgets or any configured limit is zero.
    pub fn validate(self) -> Result<Self, TerminalOutputError> {
        if self.per_session_bytes == 0
            || self.global_bytes == 0
            || self.chunk_bytes == 0
            || self.append_bytes == 0
            || self.replay_bytes == 0
            || self.sessions == 0
            || self.subscriptions == 0
        {
            return Err(TerminalOutputError::InvalidLimits {
                rule: "terminal output limits must be non-zero",
            });
        }
        if self.chunk_bytes > self.per_session_bytes || self.chunk_bytes > self.global_bytes {
            return Err(TerminalOutputError::InvalidLimits {
                rule: "one retained chunk must fit within session and global byte limits",
            });
        }
        Ok(self)
    }
}

/// Monotonic byte position within one session's terminal output.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OutputOffset(u64);

impl OutputOffset {
    /// Initial byte offset before any output is appended.
    pub const ZERO: Self = Self(0);

    /// Constructs an offset supplied by a protocol or persistence adapter.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric byte offset.
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Recovery action attached to an explicit replay gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OutputGapRecovery {
    /// Continue from the earliest retained output offset.
    RestartAtAvailableOffset,
    /// Request a screen snapshot before continuing from retained output.
    RequestScreenSnapshot,
}

/// One zero-copy slice of a retained terminal-output chunk.
#[derive(Clone, PartialEq, Eq)]
pub struct OutputSlice {
    chunk: Arc<[u8]>,
    range: Range<usize>,
    start: OutputOffset,
    retained_chunk_digest: PayloadDigest,
}

impl OutputSlice {
    /// Returns the session-relative starting byte offset.
    pub const fn start(&self) -> OutputOffset {
        self.start
    }

    /// Returns the exclusive ending byte offset.
    pub fn end(&self) -> OutputOffset {
        OutputOffset(self.start.0 + self.len() as u64)
    }

    /// Returns the retained byte count in this slice.
    pub fn len(&self) -> usize {
        self.range.len()
    }

    /// Returns whether this slice contains no bytes.
    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }

    /// Returns private terminal bytes to an authorized output adapter.
    ///
    /// Callers must not log, trace, or include this payload in diagnostics.
    pub fn bytes(&self) -> &[u8] {
        &self.chunk[self.range.clone()]
    }

    /// Computes the digest of exactly the returned slice.
    pub fn digest(&self) -> PayloadDigest {
        PayloadDigest::of_bytes(self.bytes())
    }

    /// Returns the digest of the complete retained chunk containing this slice.
    pub const fn retained_chunk_digest(&self) -> PayloadDigest {
        self.retained_chunk_digest
    }
}

impl Debug for OutputSlice {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OutputSlice")
            .field("start", &self.start)
            .field("byte_len", &self.len())
            .field("digest", &self.digest())
            .finish_non_exhaustive()
    }
}

/// Result of one bounded terminal-output replay read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputReplay {
    /// Requested offset predates retained history.
    Gap {
        /// Caller-requested offset.
        requested: OutputOffset,
        /// Earliest currently retained offset.
        available_from: OutputOffset,
        /// Current exclusive stream end.
        next_offset: OutputOffset,
        /// Explicit caller recovery action.
        recovery: OutputGapRecovery,
    },
    /// Available bounded data, possibly empty at the current stream end.
    Data {
        /// Zero-copy retained slices in session byte order.
        slices: Vec<OutputSlice>,
        /// Offset represented by the first returned byte or requested empty read.
        from: OutputOffset,
        /// Exclusive ending offset represented by this result.
        to: OutputOffset,
        /// Current exclusive stream end at extraction time.
        next_offset: OutputOffset,
        /// Whether additional retained bytes remain after `to`.
        has_more: bool,
    },
}

impl OutputReplay {
    /// Returns digest and explicit truncation evidence for a data result.
    pub fn integrity(&self) -> Result<Option<OutputChunkIntegrity>, TerminalOutputError> {
        let Self::Data {
            slices,
            from,
            to,
            next_offset,
            ..
        } = self
        else {
            return Ok(None);
        };
        let delivered = to.value().saturating_sub(from.value());
        let source = next_offset.value().saturating_sub(from.value());
        let digest = PayloadDigest::of_slices(slices.iter().map(OutputSlice::bytes));
        OutputChunkIntegrity::from_metadata(digest, delivered, source, delivered < source)
            .map(Some)
            .map_err(|_| TerminalOutputError::InvariantViolation {
                rule: "output replay integrity metadata is inconsistent",
            })
    }
}

/// One zero-copy slice used only by the digest-free terminal stream adapter.
///
/// This type deliberately exposes no retained-chunk digest. Finite replay
/// callers must use [`OutputSlice`] through [`OutputReplay`].
#[doc(hidden)]
#[derive(Clone, PartialEq, Eq)]
pub struct TerminalOutputStreamSlice {
    chunk: Arc<[u8]>,
    range: Range<usize>,
    start: OutputOffset,
}

impl TerminalOutputStreamSlice {
    /// Returns the session-relative starting byte offset.
    pub const fn start(&self) -> OutputOffset {
        self.start
    }

    /// Returns the exclusive ending byte offset.
    pub fn end(&self) -> OutputOffset {
        OutputOffset(self.start.0 + self.len() as u64)
    }

    /// Returns the retained byte count in this slice.
    pub fn len(&self) -> usize {
        self.range.len()
    }

    /// Returns whether this slice contains no bytes.
    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }

    /// Returns private terminal bytes to the terminal stream adapter.
    ///
    /// Callers must not log, trace, or include this payload in diagnostics.
    pub fn bytes(&self) -> &[u8] {
        &self.chunk[self.range.clone()]
    }
}

impl Debug for TerminalOutputStreamSlice {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TerminalOutputStreamSlice")
            .field("start", &self.start)
            .field("byte_len", &self.len())
            .finish_non_exhaustive()
    }
}

/// Digest-free bounded replay used only by the terminal stream adapter.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalOutputStreamReplay {
    /// Requested offset predates retained history.
    Gap {
        /// Caller-requested offset.
        requested: OutputOffset,
        /// Earliest currently retained offset.
        available_from: OutputOffset,
        /// Current exclusive stream end.
        next_offset: OutputOffset,
        /// Explicit caller recovery action.
        recovery: OutputGapRecovery,
    },
    /// Available bounded data, possibly empty at the current stream end.
    Data {
        /// Zero-copy retained slices in session byte order.
        slices: Vec<TerminalOutputStreamSlice>,
        /// Offset represented by the first returned byte or requested empty read.
        from: OutputOffset,
        /// Exclusive ending offset represented by this result.
        to: OutputOffset,
        /// Current exclusive stream end at extraction time.
        next_offset: OutputOffset,
        /// Whether additional retained bytes remain after `to`.
        has_more: bool,
    },
}

trait SubscriptionReplay {
    fn checkpoint_and_dirty(&self) -> (OutputOffset, bool);
}

impl SubscriptionReplay for OutputReplay {
    fn checkpoint_and_dirty(&self) -> (OutputOffset, bool) {
        match self {
            Self::Data { to, has_more, .. } => (*to, *has_more),
            Self::Gap {
                available_from,
                next_offset,
                ..
            } => (*available_from, available_from < next_offset),
        }
    }
}

impl SubscriptionReplay for TerminalOutputStreamReplay {
    fn checkpoint_and_dirty(&self) -> (OutputOffset, bool) {
        match self {
            Self::Data { to, has_more, .. } => (*to, *has_more),
            Self::Gap {
                available_from,
                next_offset,
                ..
            } => (*available_from, available_from < next_offset),
        }
    }
}

/// Result of one terminal-output append and any resulting eviction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputAppend {
    /// Starting offset assigned to the supplied bytes.
    pub start: OutputOffset,
    /// Exclusive ending offset after the append.
    pub end: OutputOffset,
    /// Earliest retained offset for this session after eviction.
    pub available_from: OutputOffset,
    /// Bytes evicted from this session during this append.
    pub session_bytes_evicted: usize,
    /// Bytes evicted from all sessions during this append.
    pub global_bytes_evicted: usize,
    /// Total bytes retained globally after this append.
    pub global_bytes_retained: usize,
}

/// Payload-safe status for one session's retained output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionOutputStatus {
    /// Interpretation policy fixed when the session output record is created.
    pub output_capture_mode: OutputCaptureMode,
    /// Earliest retained byte offset.
    pub available_from: OutputOffset,
    /// Current exclusive stream end.
    pub next_offset: OutputOffset,
    /// Bytes retained for this session.
    pub retained_bytes: usize,
    /// Reference-counted retained chunk count.
    pub retained_chunks: usize,
}

/// Payload-safe aggregate output-store status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalOutputStatus {
    /// Session offset records retained by the store.
    pub sessions: usize,
    /// Bytes retained across all sessions.
    pub retained_bytes: usize,
    /// Chunks retained across all sessions.
    pub retained_chunks: usize,
    /// Live checkpoint-only output subscriptions.
    pub subscriptions: usize,
    /// Subscriptions with retained output waiting at their checkpoint.
    pub dirty_subscriptions: usize,
    /// Inclusive configured subscription capacity.
    pub subscription_limit: usize,
}

/// Stable session-ID-ordered page of payload-safe output producer pressure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionOutputStatusPage {
    /// Returned session identities and their retained output pressure.
    pub items: Vec<(SessionId, SessionOutputStatus)>,
    /// Exclusive session-ID cursor for the next page when more rows exist.
    pub next_after: Option<SessionId>,
    /// Ordered session entries examined, including one look-ahead entry.
    pub entries_examined: usize,
}

/// Failure from bounded terminal-output retention or replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalOutputError {
    /// Configured limits are zero or internally inconsistent.
    InvalidLimits {
        /// Stable non-sensitive violated rule.
        rule: &'static str,
    },
    /// A new session would exceed the configured session-record limit.
    SessionCapacity {
        /// Configured session-record limit.
        limit: usize,
    },
    /// A new output subscription would exceed configured capacity.
    SubscriptionCapacity {
        /// Configured live-subscription limit.
        limit: usize,
    },
    /// Process-local output subscription identities cannot advance.
    SubscriptionIdentityExhausted,
    /// An output subscription was explicitly closed or lost its store.
    SubscriptionClosed,
    /// One append exceeds the configured input byte limit.
    AppendCapacity {
        /// Configured append byte limit.
        limit: usize,
    },
    /// One replay request exceeds the configured output byte limit.
    ReplayCapacity {
        /// Configured replay byte limit.
        limit: usize,
    },
    /// A diagnostic page or top-producer request exceeds its configured bound.
    DiagnosticCapacity {
        /// Configured diagnostic row limit.
        limit: usize,
    },
    /// A caller requested bytes beyond the current stream end.
    FutureOffset {
        /// Caller-requested offset.
        requested: OutputOffset,
        /// Current exclusive stream end.
        next_offset: OutputOffset,
    },
    /// Session output byte offsets cannot advance safely.
    OffsetExhausted,
    /// An atomic append expected a different current output checkpoint.
    CheckpointMismatch {
        /// Parser-side expected output checkpoint.
        expected: OutputOffset,
        /// Current retained-output checkpoint.
        observed: OutputOffset,
    },
    /// An existing output record was opened under a different capture policy.
    CaptureModeMismatch {
        /// Capture policy fixed on the existing output record.
        expected: OutputCaptureMode,
        /// Capture policy supplied by the conflicting caller.
        observed: OutputCaptureMode,
    },
    /// Global chunk ordering cannot advance safely.
    ChunkSequenceExhausted,
    /// A thread panicked while holding the output-store lock.
    LockPoisoned,
    /// Internal retained-byte or ordering state became inconsistent.
    InvariantViolation {
        /// Stable non-sensitive violated rule.
        rule: &'static str,
    },
}

impl Display for TerminalOutputError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLimits { rule } => write!(formatter, "invalid output limits: {rule}"),
            Self::SessionCapacity { limit } => {
                write!(
                    formatter,
                    "terminal output session limit of {limit} was reached"
                )
            }
            Self::SubscriptionCapacity { limit } => {
                write!(
                    formatter,
                    "terminal output subscription limit of {limit} was reached"
                )
            }
            Self::SubscriptionIdentityExhausted => {
                formatter.write_str("terminal output subscription identity is exhausted")
            }
            Self::SubscriptionClosed => {
                formatter.write_str("terminal output subscription is closed")
            }
            Self::AppendCapacity { limit } => {
                write!(
                    formatter,
                    "terminal output append exceeds limit of {limit} bytes"
                )
            }
            Self::ReplayCapacity { limit } => {
                write!(
                    formatter,
                    "terminal output replay exceeds limit of {limit} bytes"
                )
            }
            Self::DiagnosticCapacity { limit } => {
                write!(
                    formatter,
                    "terminal output diagnostic limit is {limit} rows"
                )
            }
            Self::FutureOffset {
                requested,
                next_offset,
            } => write!(
                formatter,
                "terminal output offset {} is beyond stream end {}",
                requested.value(),
                next_offset.value()
            ),
            Self::OffsetExhausted => formatter.write_str("terminal output offset is exhausted"),
            Self::CheckpointMismatch { expected, observed } => write!(
                formatter,
                "terminal output checkpoint {} does not match expected {}",
                observed.value(),
                expected.value()
            ),
            Self::CaptureModeMismatch { expected, observed } => write!(
                formatter,
                "terminal output capture mode {observed:?} does not match existing mode {expected:?}"
            ),
            Self::ChunkSequenceExhausted => {
                formatter.write_str("terminal output chunk sequence is exhausted")
            }
            Self::LockPoisoned => formatter.write_str("terminal output store lock is poisoned"),
            Self::InvariantViolation { rule } => {
                write!(formatter, "terminal output store invariant failed: {rule}")
            }
        }
    }
}

impl Error for TerminalOutputError {}

/// Failure from an authorized bounded terminal-output read.
#[derive(Debug)]
pub enum ReadTerminalOutputError {
    /// No valid authentication was established for the request.
    Unauthenticated { session_id: SessionId },
    /// The authenticated principal cannot read this terminal.
    PermissionDenied { session_id: SessionId },
    /// Authorization infrastructure failed.
    Infrastructure(PortError),
    /// Retained output rejected or could not satisfy the read.
    Retention(TerminalOutputError),
}

impl ReadTerminalOutputError {
    /// Returns the stable category used by presentation adapters.
    pub const fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::Unauthenticated { .. } => ApplicationErrorCategory::Unauthenticated,
            Self::PermissionDenied { .. } => ApplicationErrorCategory::PermissionDenied,
            Self::Infrastructure(error) => error.category(),
            Self::Retention(error) => match error {
                TerminalOutputError::ReplayCapacity { .. }
                | TerminalOutputError::FutureOffset { .. }
                | TerminalOutputError::DiagnosticCapacity { .. }
                | TerminalOutputError::InvalidLimits { .. } => {
                    ApplicationErrorCategory::InvalidArgument
                }
                TerminalOutputError::SessionCapacity { .. }
                | TerminalOutputError::SubscriptionCapacity { .. }
                | TerminalOutputError::AppendCapacity { .. } => {
                    ApplicationErrorCategory::CapacityExceeded
                }
                TerminalOutputError::OffsetExhausted
                | TerminalOutputError::CheckpointMismatch { .. }
                | TerminalOutputError::CaptureModeMismatch { .. }
                | TerminalOutputError::ChunkSequenceExhausted
                | TerminalOutputError::SubscriptionIdentityExhausted
                | TerminalOutputError::InvariantViolation { .. } => {
                    ApplicationErrorCategory::InvariantViolation
                }
                TerminalOutputError::LockPoisoned | TerminalOutputError::SubscriptionClosed => {
                    ApplicationErrorCategory::Internal
                }
            },
        }
    }

    /// Returns the safe target session identity when available.
    pub fn session_id(&self) -> Option<&SessionId> {
        match self {
            Self::Unauthenticated { session_id } | Self::PermissionDenied { session_id } => {
                Some(session_id)
            }
            Self::Infrastructure(error) => error.session_id(),
            Self::Retention(_) => None,
        }
    }
}

impl Display for ReadTerminalOutputError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unauthenticated { session_id } => write!(
                formatter,
                "authentication is required to read terminal output for session {session_id}"
            ),
            Self::PermissionDenied { session_id } => write!(
                formatter,
                "permission denied to read terminal output for session {session_id}"
            ),
            Self::Infrastructure(error) => Display::fmt(error, formatter),
            Self::Retention(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for ReadTerminalOutputError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Infrastructure(error) => Some(error),
            Self::Retention(error) => Some(error),
            Self::Unauthenticated { .. } | Self::PermissionDenied { .. } => None,
        }
    }
}

/// Process-local identity for one checkpoint-only output subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TerminalOutputSubscriptionId(u64);

impl TerminalOutputSubscriptionId {
    /// Returns the monotonic process-local numeric value.
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Coherent initial output replay and registered live continuation.
pub struct TerminalOutputSubscriptionOpen {
    initial: OutputReplay,
    subscription: TerminalOutputSubscription,
}

/// Coherent digest-free initial replay and live stream continuation.
#[doc(hidden)]
pub struct TerminalOutputStreamSubscriptionOpen {
    initial: TerminalOutputStreamReplay,
    subscription: TerminalOutputStreamSubscription,
}

impl TerminalOutputStreamSubscriptionOpen {
    /// Returns the initial replay captured atomically with registration.
    pub const fn initial(&self) -> &TerminalOutputStreamReplay {
        &self.initial
    }

    /// Consumes the result into replay and continuation ownership.
    pub fn into_parts(self) -> (TerminalOutputStreamReplay, TerminalOutputStreamSubscription) {
        (self.initial, self.subscription)
    }
}

impl TerminalOutputSubscriptionOpen {
    /// Returns the initial replay captured atomically with registration.
    pub const fn initial(&self) -> &OutputReplay {
        &self.initial
    }

    /// Consumes the result into replay and continuation ownership.
    pub fn into_parts(self) -> (OutputReplay, TerminalOutputSubscription) {
        (self.initial, self.subscription)
    }
}

/// Independently owned terminal-output subscription retaining only a byte
/// checkpoint and payload-free wake callback.
pub struct TerminalOutputSubscription {
    id: TerminalOutputSubscriptionId,
    session_id: SessionId,
    max_bytes: usize,
    recovery: OutputGapRecovery,
    state: Arc<Mutex<TerminalOutputSubscriptionState>>,
    store: Weak<RwLock<TerminalOutputState>>,
}

impl TerminalOutputSubscription {
    /// Returns the process-local subscription identity.
    pub const fn id(&self) -> TerminalOutputSubscriptionId {
        self.id
    }

    /// Reads one pending bounded replay without waiting.
    ///
    /// Serialize receive calls for each subscription. Replay extraction releases
    /// the subscription mutex, so concurrent receives are not one ordered
    /// consumption stream. See the [subscription guide](../docs/internals/sessions/terminal-io.md#subscribe-and-recover-a-screen).
    ///
    /// The result retains zero-copy output slices only for the duration chosen
    /// by its caller. This function performs no external IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns closure, replay-capacity, offset, lock, or invariant failures.
    pub fn try_receive(&self) -> Result<Option<OutputReplay>, TerminalOutputError> {
        self.try_receive_with(replay_locked)
    }

    fn try_receive_with<R: SubscriptionReplay>(
        &self,
        replay_locked: fn(
            &TerminalOutputState,
            &SessionId,
            OutputOffset,
            usize,
            OutputGapRecovery,
        ) -> Result<R, TerminalOutputError>,
    ) -> Result<Option<R>, TerminalOutputError> {
        let mut subscription = self.lock_state();
        if subscription.closed {
            return Err(TerminalOutputError::SubscriptionClosed);
        }
        if !subscription.dirty {
            return Ok(None);
        }
        subscription.dirty = false;
        let checkpoint = subscription.checkpoint;
        // Append and status paths lock the store before subscription state.
        // Release this lock before reading the store to preserve that order.
        drop(subscription);

        let store = self
            .store
            .upgrade()
            .ok_or(TerminalOutputError::SubscriptionClosed)?;
        let replay = {
            let state = store
                .read()
                .map_err(|_| TerminalOutputError::LockPoisoned)?;
            replay_locked(
                &state,
                &self.session_id,
                checkpoint,
                self.max_bytes,
                self.recovery,
            )?
        };

        let mut subscription = self.lock_state();
        if subscription.closed {
            return Err(TerminalOutputError::SubscriptionClosed);
        }
        let (checkpoint, dirty) = replay.checkpoint_and_dirty();
        subscription.checkpoint = checkpoint;
        // An append may have marked us dirty after replay released the store.
        // Preserve that notification as well as any unread part of this replay.
        subscription.dirty |= dirty;
        Ok(Some(replay))
    }

    /// Closes this output subscription without affecting peer streams.
    ///
    /// Repeated closure is idempotent. This function performs no external IO
    /// and does not panic.
    pub fn close(&self) {
        if let Some(store) = self.store.upgrade()
            && let Ok(mut store) = store.write()
        {
            store.remove_subscription(&self.session_id, self.id);
        }
        let mut subscription = self.lock_state();
        subscription.closed = true;
        subscription.dirty = false;
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, TerminalOutputSubscriptionState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Drop for TerminalOutputSubscription {
    fn drop(&mut self) {
        self.close();
    }
}

/// Digest-free checkpoint subscription used only by terminal streaming.
#[doc(hidden)]
pub struct TerminalOutputStreamSubscription {
    subscription: TerminalOutputSubscription,
}

impl TerminalOutputStreamSubscription {
    /// Returns the process-local subscription identity.
    pub const fn id(&self) -> TerminalOutputSubscriptionId {
        self.subscription.id()
    }

    /// Reads one pending digest-free bounded stream replay without waiting.
    ///
    /// Serialize receives on this subscription, as required by
    /// [`TerminalOutputSubscription::try_receive`].
    pub fn try_receive(&self) -> Result<Option<TerminalOutputStreamReplay>, TerminalOutputError> {
        self.subscription.try_receive_with(stream_replay_locked)
    }

    /// Closes this output stream subscription without affecting peers.
    pub fn close(&self) {
        self.subscription.close();
    }
}

struct TerminalOutputSubscriptionState {
    checkpoint: OutputOffset,
    dirty: bool,
    closed: bool,
    wake: Arc<dyn SubscriptionWake>,
}

/// Thread-safe bounded terminal output retention shared by session adapters.
///
/// Retained chunks are reference-counted so replay extraction does not copy
/// output while holding the store lock. Connection and stream mailboxes must
/// apply their own negotiated byte limits to bound outstanding references held
/// after store eviction.
pub struct TerminalOutputStore {
    limits: TerminalOutputLimits,
    state: Arc<RwLock<TerminalOutputState>>,
    next_subscription_id: AtomicU64,
}

impl TerminalOutputStore {
    /// Creates an empty output store with validated hard limits.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed invalid-limit failure.
    pub fn new(limits: TerminalOutputLimits) -> Result<Self, TerminalOutputError> {
        Ok(Self {
            limits: limits.validate()?,
            state: Arc::new(RwLock::new(TerminalOutputState::default())),
            next_subscription_id: AtomicU64::new(0),
        })
    }

    /// Returns the validated retention and subscription limits.
    ///
    /// This function performs no IO, cannot fail, and does not panic.
    pub const fn limits(&self) -> TerminalOutputLimits {
        self.limits
    }

    /// Creates one mode-bound absolute output checkpoint before producer admission.
    ///
    /// Repeating the exact mode and checkpoint is idempotent. Existing retained
    /// bytes cannot be relabelled or rebased. This function performs no external
    /// IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns session capacity, mode conflict, checkpoint conflict, or lock
    /// failures.
    pub fn begin_session(
        &self,
        session_id: SessionId,
        output_capture_mode: OutputCaptureMode,
        output_offset: OutputOffset,
    ) -> Result<(), TerminalOutputError> {
        let mut state = self
            .state
            .write()
            .map_err(|_| TerminalOutputError::LockPoisoned)?;
        if let Some(buffer) = state.sessions.get(&session_id) {
            if buffer.output_capture_mode != output_capture_mode {
                return Err(TerminalOutputError::CaptureModeMismatch {
                    expected: buffer.output_capture_mode,
                    observed: output_capture_mode,
                });
            }
            if buffer.next_offset != output_offset {
                return Err(TerminalOutputError::CheckpointMismatch {
                    expected: output_offset,
                    observed: buffer.next_offset,
                });
            }
            return Ok(());
        }
        if state.sessions.len() == self.limits.sessions {
            return Err(TerminalOutputError::SessionCapacity {
                limit: self.limits.sessions,
            });
        }
        state.sessions.insert(
            session_id.clone(),
            SessionOutputBuffer::new(output_offset, output_capture_mode),
        );
        let wakes = state.mark_output_dirty(&session_id);
        drop(state);
        for wake in wakes {
            wake.wake();
        }
        Ok(())
    }

    /// Advances one session to an explicit remote-retention boundary.
    ///
    /// Advancing discards the older retained prefix because this store models
    /// one contiguous retained suffix. Subsequent reads from an older offset
    /// therefore return an explicit gap rather than silently rebasing remote
    /// bytes. Equal or older boundaries are idempotent and never regress the
    /// stream. This function performs no external IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns session capacity, mode conflict, lock, or invariant failures.
    pub fn establish_retention_gap(
        &self,
        session_id: &SessionId,
        output_capture_mode: OutputCaptureMode,
        available_from: OutputOffset,
    ) -> Result<(), TerminalOutputError> {
        let mut state = self
            .state
            .write()
            .map_err(|_| TerminalOutputError::LockPoisoned)?;
        if !state.sessions.contains_key(session_id) {
            if state.sessions.len() == self.limits.sessions {
                return Err(TerminalOutputError::SessionCapacity {
                    limit: self.limits.sessions,
                });
            }
            state.sessions.insert(
                session_id.clone(),
                SessionOutputBuffer::new(available_from, output_capture_mode),
            );
            let wakes = state.mark_output_dirty(session_id);
            drop(state);
            for wake in wakes {
                wake.wake();
            }
            return Ok(());
        }
        let (sequences, evicted_bytes) = {
            let buffer = state.sessions.get_mut(session_id).ok_or(
                TerminalOutputError::InvariantViolation {
                    rule: "output session disappeared while establishing a retention gap",
                },
            )?;
            if buffer.output_capture_mode != output_capture_mode {
                return Err(TerminalOutputError::CaptureModeMismatch {
                    expected: buffer.output_capture_mode,
                    observed: output_capture_mode,
                });
            }
            if available_from <= buffer.next_offset {
                return Ok(());
            }
            let sequences = buffer
                .chunks
                .iter()
                .map(|chunk| chunk.sequence)
                .collect::<Vec<_>>();
            let evicted_bytes = buffer.retained_bytes;
            buffer.chunks.clear();
            buffer.retained_bytes = 0;
            buffer.next_offset = available_from;
            (sequences, evicted_bytes)
        };
        for sequence in sequences {
            if state.global_order.remove(&sequence).is_none() {
                return Err(TerminalOutputError::InvariantViolation {
                    rule: "retention gap removed a chunk absent from global output order",
                });
            }
        }
        state.retained_bytes = state.retained_bytes.checked_sub(evicted_bytes).ok_or(
            TerminalOutputError::InvariantViolation {
                rule: "retention gap exceeded globally retained output bytes",
            },
        )?;
        let wakes = state.mark_output_dirty(session_id);
        drop(state);
        for wake in wakes {
            wake.wake();
        }
        Ok(())
    }

    /// Atomically captures initial replay and registers a selected-session
    /// continuation with one payload-free wake callback.
    ///
    /// The subscription retains a byte checkpoint rather than a private output
    /// queue. Slow consumers therefore remain bounded by the shared retention
    /// policy and receive an explicit replay gap after eviction. This function
    /// performs no external IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns capacity, identity, replay, offset, lock, or invariant failures.
    pub fn subscribe(
        &self,
        session_id: SessionId,
        from: OutputOffset,
        max_bytes: usize,
        recovery: OutputGapRecovery,
        wake: Arc<dyn SubscriptionWake>,
    ) -> Result<TerminalOutputSubscriptionOpen, TerminalOutputError> {
        let (initial, subscription) =
            self.subscribe_with_replay(session_id, from, max_bytes, recovery, wake, replay_locked)?;
        Ok(TerminalOutputSubscriptionOpen {
            initial,
            subscription,
        })
    }

    /// Registers the digest-free continuation used by terminal streaming.
    #[doc(hidden)]
    pub fn subscribe_stream(
        &self,
        session_id: SessionId,
        from: OutputOffset,
        max_bytes: usize,
        recovery: OutputGapRecovery,
        wake: Arc<dyn SubscriptionWake>,
    ) -> Result<TerminalOutputStreamSubscriptionOpen, TerminalOutputError> {
        let (initial, subscription) = self.subscribe_with_replay(
            session_id,
            from,
            max_bytes,
            recovery,
            wake,
            stream_replay_locked,
        )?;
        Ok(TerminalOutputStreamSubscriptionOpen {
            initial,
            subscription: TerminalOutputStreamSubscription { subscription },
        })
    }

    fn subscribe_with_replay<R: SubscriptionReplay>(
        &self,
        session_id: SessionId,
        from: OutputOffset,
        max_bytes: usize,
        recovery: OutputGapRecovery,
        wake: Arc<dyn SubscriptionWake>,
        replay_locked: fn(
            &TerminalOutputState,
            &SessionId,
            OutputOffset,
            usize,
            OutputGapRecovery,
        ) -> Result<R, TerminalOutputError>,
    ) -> Result<(R, TerminalOutputSubscription), TerminalOutputError> {
        if max_bytes == 0 || max_bytes > self.limits.replay_bytes {
            return Err(TerminalOutputError::ReplayCapacity {
                limit: self.limits.replay_bytes,
            });
        }
        let mut state = self
            .state
            .write()
            .map_err(|_| TerminalOutputError::LockPoisoned)?;
        state.remove_dead_subscriptions();
        if state.subscription_count() >= self.limits.subscriptions {
            return Err(TerminalOutputError::SubscriptionCapacity {
                limit: self.limits.subscriptions,
            });
        }
        let id = self
            .next_subscription_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map(|previous| TerminalOutputSubscriptionId(previous + 1))
            .map_err(|_| TerminalOutputError::SubscriptionIdentityExhausted)?;
        let initial = replay_locked(&state, &session_id, from, max_bytes, recovery)?;
        let (checkpoint, dirty) = initial.checkpoint_and_dirty();
        let subscription_state = Arc::new(Mutex::new(TerminalOutputSubscriptionState {
            checkpoint,
            dirty,
            closed: false,
            wake,
        }));
        state
            .subscriptions
            .entry(session_id.clone())
            .or_default()
            .insert(id, Arc::downgrade(&subscription_state));
        Ok((
            initial,
            TerminalOutputSubscription {
                id,
                session_id,
                max_bytes,
                recovery,
                state: subscription_state,
                store: Arc::downgrade(&self.state),
            },
        ))
    }

    /// Appends bounded output and evicts oldest chunks to satisfy both budgets.
    ///
    /// Empty output is an idempotent no-op. Non-empty output receives monotonic
    /// session byte offsets. Eviction is deterministic by append order across
    /// sessions and never scans session details. This function performs no
    /// external IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns capacity, offset exhaustion, lock, or internal invariant errors.
    pub fn append(
        &self,
        session_id: &SessionId,
        bytes: &[u8],
    ) -> Result<OutputAppend, TerminalOutputError> {
        self.append_inner(session_id, None, bytes)
    }

    /// Atomically appends output only at an exact parser-side checkpoint.
    ///
    /// The checkpoint comparison and append share one output-store write lock,
    /// so a concurrent or bypassing writer cannot create an unobserved interval.
    /// Successful eviction and subscription wake behavior is identical to
    /// [`Self::append`]. This method performs no external IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns checkpoint mismatch, capacity, offset exhaustion, lock, or
    /// internal invariant failures.
    pub fn append_at_checkpoint(
        &self,
        session_id: &SessionId,
        expected: OutputOffset,
        bytes: &[u8],
    ) -> Result<OutputAppend, TerminalOutputError> {
        self.append_inner(session_id, Some(expected), bytes)
    }

    fn append_inner(
        &self,
        session_id: &SessionId,
        expected: Option<OutputOffset>,
        bytes: &[u8],
    ) -> Result<OutputAppend, TerminalOutputError> {
        if bytes.len() > self.limits.append_bytes {
            return Err(TerminalOutputError::AppendCapacity {
                limit: self.limits.append_bytes,
            });
        }
        let mut state = self
            .state
            .write()
            .map_err(|_| TerminalOutputError::LockPoisoned)?;
        let current_offset = state
            .sessions
            .get(session_id)
            .map_or(OutputOffset::ZERO, |buffer| buffer.next_offset);
        if let Some(expected) = expected
            && expected != current_offset
        {
            return Err(TerminalOutputError::CheckpointMismatch {
                expected,
                observed: current_offset,
            });
        }
        let byte_count =
            u64::try_from(bytes.len()).map_err(|_| TerminalOutputError::OffsetExhausted)?;
        let end = current_offset
            .0
            .checked_add(byte_count)
            .map(OutputOffset)
            .ok_or(TerminalOutputError::OffsetExhausted)?;
        if bytes.is_empty() {
            let available_from = state
                .sessions
                .get(session_id)
                .map_or(current_offset, SessionOutputBuffer::available_from);
            return Ok(OutputAppend {
                start: current_offset,
                end,
                available_from,
                session_bytes_evicted: 0,
                global_bytes_evicted: 0,
                global_bytes_retained: state.retained_bytes,
            });
        }
        if !state.sessions.contains_key(session_id) && state.sessions.len() == self.limits.sessions
        {
            return Err(TerminalOutputError::SessionCapacity {
                limit: self.limits.sessions,
            });
        }
        let chunk_count = bytes.len().div_ceil(self.limits.chunk_bytes);
        let chunk_count =
            u64::try_from(chunk_count).map_err(|_| TerminalOutputError::ChunkSequenceExhausted)?;
        state
            .next_chunk_sequence
            .checked_add(chunk_count)
            .ok_or(TerminalOutputError::ChunkSequenceExhausted)?;
        state.sessions.entry(session_id.clone()).or_insert_with(|| {
            SessionOutputBuffer::new(current_offset, OutputCaptureMode::Terminal)
        });

        let mut next_offset = current_offset;
        for bytes in bytes.chunks(self.limits.chunk_bytes) {
            let chunk_end = OutputOffset(next_offset.0 + bytes.len() as u64);
            let sequence = state.next_chunk_sequence;
            state.next_chunk_sequence += 1;
            let chunk = RetainedOutputChunk {
                sequence,
                start: next_offset,
                end: chunk_end,
                bytes: Arc::from(bytes),
            };
            state
                .global_order
                .insert(sequence, (session_id.clone(), next_offset));
            let buffer = state.sessions.get_mut(session_id).ok_or(
                TerminalOutputError::InvariantViolation {
                    rule: "new output session disappeared during append",
                },
            )?;
            buffer.retained_bytes += bytes.len();
            buffer.next_offset = chunk_end;
            buffer.chunks.push_back(chunk);
            state.retained_bytes += bytes.len();
            next_offset = chunk_end;
        }

        let mut session_bytes_evicted = 0;
        loop {
            let over_limit = state
                .sessions
                .get(session_id)
                .is_some_and(|buffer| buffer.retained_bytes > self.limits.per_session_bytes);
            if !over_limit {
                break;
            }
            session_bytes_evicted += state.evict_session_front(session_id)?;
        }
        let mut global_bytes_evicted = session_bytes_evicted;
        while state.retained_bytes > self.limits.global_bytes {
            global_bytes_evicted += state.evict_global_front()?;
        }
        let available_from = state
            .sessions
            .get(session_id)
            .ok_or(TerminalOutputError::InvariantViolation {
                rule: "output session disappeared after eviction",
            })?
            .available_from();
        let appended = OutputAppend {
            start: current_offset,
            end,
            available_from,
            session_bytes_evicted,
            global_bytes_evicted,
            global_bytes_retained: state.retained_bytes,
        };
        let wakes = state.mark_output_dirty(session_id);
        drop(state);
        for wake in wakes {
            wake.wake();
        }
        Ok(appended)
    }

    /// Extracts a bounded zero-copy replay from one session byte offset.
    ///
    /// Reads before retained history return an explicit gap. Reads at the stream
    /// end return empty data. This function performs no external IO and does not
    /// panic.
    ///
    /// # Errors
    ///
    /// Returns replay-capacity, future-offset, lock, or invariant failures.
    pub fn replay(
        &self,
        session_id: &SessionId,
        from: OutputOffset,
        max_bytes: usize,
        gap_recovery: OutputGapRecovery,
    ) -> Result<OutputReplay, TerminalOutputError> {
        if max_bytes == 0 || max_bytes > self.limits.replay_bytes {
            return Err(TerminalOutputError::ReplayCapacity {
                limit: self.limits.replay_bytes,
            });
        }
        let state = self
            .state
            .read()
            .map_err(|_| TerminalOutputError::LockPoisoned)?;
        replay_locked(&state, session_id, from, max_bytes, gap_recovery)
    }

    /// Removes retained chunks and offset state for one session.
    ///
    /// Missing sessions are idempotent no-ops. Existing zero-copy replay slices
    /// remain valid through reference counting and must stay within their owning
    /// stream budgets. This function performs no external IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a poisoned-lock failure.
    pub fn remove_session(&self, session_id: &SessionId) -> Result<usize, TerminalOutputError> {
        let mut state = self
            .state
            .write()
            .map_err(|_| TerminalOutputError::LockPoisoned)?;
        let retained_bytes = if let Some(buffer) = state.sessions.remove(session_id) {
            for chunk in &buffer.chunks {
                state.global_order.remove(&chunk.sequence);
            }
            state.retained_bytes -= buffer.retained_bytes;
            buffer.retained_bytes
        } else {
            0
        };
        let subscriptions = state.take_subscriptions(session_id);
        drop(state);
        for subscription in subscriptions {
            let wake = {
                let mut subscription = subscription
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                subscription.closed = true;
                subscription.dirty = false;
                Arc::clone(&subscription.wake)
            };
            wake.wake();
        }
        Ok(retained_bytes)
    }

    /// Returns payload-safe status for one session when its offset record exists.
    ///
    /// # Errors
    ///
    /// Returns a poisoned-lock failure.
    pub fn session_status(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<SessionOutputStatus>, TerminalOutputError> {
        let state = self
            .state
            .read()
            .map_err(|_| TerminalOutputError::LockPoisoned)?;
        Ok(state
            .sessions
            .get(session_id)
            .map(|buffer| SessionOutputStatus {
                output_capture_mode: buffer.output_capture_mode,
                available_from: buffer.available_from(),
                next_offset: buffer.next_offset,
                retained_bytes: buffer.retained_bytes,
                retained_chunks: buffer.chunks.len(),
            }))
    }

    /// Reads one bounded stable page of payload-safe output producer pressure.
    ///
    /// The read examines at most one more session than requested, copies no
    /// terminal bytes, and performs no external IO. Concurrent appends may be
    /// reflected between pages because this is an operational snapshot.
    ///
    /// # Errors
    ///
    /// Returns replay-capacity failure for a zero or excessive page size, or a
    /// poisoned-lock failure.
    pub fn session_status_page(
        &self,
        after: Option<&SessionId>,
        requested_page_size: usize,
    ) -> Result<SessionOutputStatusPage, TerminalOutputError> {
        if requested_page_size == 0 || requested_page_size > self.limits.sessions {
            return Err(TerminalOutputError::DiagnosticCapacity {
                limit: self.limits.sessions,
            });
        }
        let state = self
            .state
            .read()
            .map_err(|_| TerminalOutputError::LockPoisoned)?;
        let mut selected: Vec<_> = match after {
            Some(after) => state
                .sessions
                .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
                .take(requested_page_size.saturating_add(1))
                .map(|(session_id, buffer)| (session_id.clone(), session_output_status(buffer)))
                .collect(),
            None => state
                .sessions
                .iter()
                .take(requested_page_size.saturating_add(1))
                .map(|(session_id, buffer)| (session_id.clone(), session_output_status(buffer)))
                .collect(),
        };
        let entries_examined = selected.len();
        let has_more = selected.len() > requested_page_size;
        if has_more {
            selected.pop();
        }
        let next_after = has_more
            .then(|| selected.last().map(|(session_id, _)| session_id.clone()))
            .flatten();
        Ok(SessionOutputStatusPage {
            items: selected,
            next_after,
            entries_examined,
        })
    }

    /// Returns the highest-retained-byte session producers within a hard limit.
    ///
    /// The source session registry is already bounded by configuration. This
    /// method copies payload-safe counters only, orders largest producers first
    /// with session ID as a stable tie-breaker, and never reads terminal bytes.
    /// It performs no external IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns replay-capacity failure for a zero or excessive result limit, or
    /// a poisoned-lock failure.
    pub fn top_session_statuses(
        &self,
        requested_limit: usize,
    ) -> Result<Vec<(SessionId, SessionOutputStatus)>, TerminalOutputError> {
        if requested_limit == 0 || requested_limit > self.limits.sessions {
            return Err(TerminalOutputError::DiagnosticCapacity {
                limit: self.limits.sessions,
            });
        }
        let state = self
            .state
            .read()
            .map_err(|_| TerminalOutputError::LockPoisoned)?;
        let mut producers: Vec<_> = state
            .sessions
            .iter()
            .filter(|(_, buffer)| buffer.retained_bytes > 0)
            .map(|(session_id, buffer)| (session_id.clone(), session_output_status(buffer)))
            .collect();
        producers.sort_by(|left, right| {
            right
                .1
                .retained_bytes
                .cmp(&left.1.retained_bytes)
                .then_with(|| left.0.cmp(&right.0))
        });
        producers.truncate(requested_limit);
        Ok(producers)
    }

    /// Returns payload-safe global retention status.
    ///
    /// # Errors
    ///
    /// Returns a poisoned-lock failure.
    pub fn status(&self) -> Result<TerminalOutputStatus, TerminalOutputError> {
        let mut state = self
            .state
            .write()
            .map_err(|_| TerminalOutputError::LockPoisoned)?;
        state.remove_dead_subscriptions();
        let mut dirty_subscriptions = 0;
        for subscriptions in state.subscriptions.values() {
            for subscription in subscriptions.values().filter_map(Weak::upgrade) {
                let subscription = subscription
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                dirty_subscriptions += usize::from(subscription.dirty && !subscription.closed);
            }
        }
        Ok(TerminalOutputStatus {
            sessions: state.sessions.len(),
            retained_bytes: state.retained_bytes,
            retained_chunks: state.global_order.len(),
            subscriptions: state.subscription_count(),
            dirty_subscriptions,
            subscription_limit: self.limits.subscriptions,
        })
    }
}

fn session_output_status(buffer: &SessionOutputBuffer) -> SessionOutputStatus {
    SessionOutputStatus {
        output_capture_mode: buffer.output_capture_mode,
        available_from: buffer.available_from(),
        next_offset: buffer.next_offset,
        retained_bytes: buffer.retained_bytes,
        retained_chunks: buffer.chunks.len(),
    }
}

/// Authorizes and extracts one bounded retained terminal-output page.
pub struct ReadTerminalOutputService<'a> {
    authorizer: &'a dyn AuthorizationPort,
    output: &'a TerminalOutputStore,
    clock: &'a dyn Clock,
    telemetry: &'a dyn Telemetry,
}

impl<'a> ReadTerminalOutputService<'a> {
    /// Constructs the terminal-output read boundary without performing IO.
    pub const fn new(
        authorizer: &'a dyn AuthorizationPort,
        output: &'a TerminalOutputStore,
        clock: &'a dyn Clock,
        telemetry: &'a dyn Telemetry,
    ) -> Self {
        Self {
            authorizer,
            output,
            clock,
            telemetry,
        }
    }

    /// Authorizes and returns one bounded zero-copy replay result.
    ///
    /// Authorization completes before retained output is inspected. Raw output
    /// bytes are returned only through the result and never enter telemetry.
    /// This method performs no process or PTY IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed access, authorization-infrastructure, capacity, offset, or
    /// retained-state failures.
    pub async fn read(
        &self,
        session_id: &SessionId,
        from: OutputOffset,
        max_bytes: usize,
        gap_recovery: OutputGapRecovery,
    ) -> Result<OutputReplay, ReadTerminalOutputError> {
        let started_at = self.clock.now();
        let result = async {
            let decision = self
                .authorizer
                .authorize(
                    AccessAction::SubscribeTerminal,
                    &AccessResource::Session(session_id.clone()),
                )
                .await
                .map_err(ReadTerminalOutputError::Infrastructure)?;
            match decision {
                AccessDecision::Permit => {}
                AccessDecision::Deny(AccessDenialReason::Unauthenticated) => {
                    return Err(ReadTerminalOutputError::Unauthenticated {
                        session_id: session_id.clone(),
                    });
                }
                AccessDecision::Deny(AccessDenialReason::Forbidden) => {
                    return Err(ReadTerminalOutputError::PermissionDenied {
                        session_id: session_id.clone(),
                    });
                }
            }
            self.output
                .replay(session_id, from, max_bytes, gap_recovery)
                .map_err(ReadTerminalOutputError::Retention)
        }
        .await;
        self.finish(started_at, session_id, result.as_ref());
        result
    }

    fn finish(
        &self,
        started_at: undercurrent_domain::TimestampMillis,
        session_id: &SessionId,
        result: Result<&OutputReplay, &ReadTerminalOutputError>,
    ) {
        let finished_at = self.clock.now();
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::OperationDurationSeconds,
            value: finished_at
                .saturating_duration_since(started_at)
                .as_secs_f64(),
            operation: Some(ApplicationOperation::ReadTerminalOutput),
        });
        if let Ok(replay) = result {
            self.telemetry.record_metric(MetricObservation {
                name: MetricName::TerminalOutputBytesRead,
                value: replay_byte_len(replay) as f64,
                operation: Some(ApplicationOperation::ReadTerminalOutput),
            });
        }
        let (event, level, application_error, operation, error_kind) = match result {
            Ok(_) => (
                DiagnosticEventName::TerminalOutputRead,
                DiagnosticLevel::Debug,
                None,
                None,
                None,
            ),
            Err(error) => {
                let category = error.category();
                let event = if category.is_access_denial() {
                    DiagnosticEventName::AuthorizationDenied
                } else {
                    DiagnosticEventName::TerminalOutputReadFailed
                };
                let port = match error {
                    ReadTerminalOutputError::Infrastructure(error) => Some(error),
                    _ => None,
                };
                (
                    event,
                    port.map_or_else(
                        || DiagnosticLevel::for_application_error(category),
                        |error| DiagnosticLevel::for_port_error(error.kind()),
                    ),
                    Some(category),
                    port.map(PortError::operation),
                    port.map(PortError::kind),
                )
            }
        };
        self.telemetry.emit_diagnostic(&DiagnosticRecord {
            event,
            level,
            timestamp: finished_at,
            session_id: Some(session_id.clone()),
            application_operation: Some(ApplicationOperation::ReadTerminalOutput),
            application_error,
            operation,
            error_kind,
        });
    }
}

fn replay_byte_len(replay: &OutputReplay) -> usize {
    match replay {
        OutputReplay::Data { slices, .. } => slices.iter().map(OutputSlice::len).sum(),
        OutputReplay::Gap { .. } => 0,
    }
}

fn replay_locked(
    state: &TerminalOutputState,
    session_id: &SessionId,
    from: OutputOffset,
    max_bytes: usize,
    gap_recovery: OutputGapRecovery,
) -> Result<OutputReplay, TerminalOutputError> {
    match replay_parts_locked(
        state,
        session_id,
        from,
        max_bytes,
        gap_recovery,
        |chunk, range, start| OutputSlice {
            retained_chunk_digest: PayloadDigest::of_bytes(&chunk),
            chunk,
            range,
            start,
        },
    )? {
        ReplayParts::Gap {
            requested,
            available_from,
            next_offset,
            recovery,
        } => Ok(OutputReplay::Gap {
            requested,
            available_from,
            next_offset,
            recovery,
        }),
        ReplayParts::Data {
            slices,
            from,
            to,
            next_offset,
            has_more,
        } => Ok(OutputReplay::Data {
            slices,
            from,
            to,
            next_offset,
            has_more,
        }),
    }
}

fn stream_replay_locked(
    state: &TerminalOutputState,
    session_id: &SessionId,
    from: OutputOffset,
    max_bytes: usize,
    gap_recovery: OutputGapRecovery,
) -> Result<TerminalOutputStreamReplay, TerminalOutputError> {
    match replay_parts_locked(
        state,
        session_id,
        from,
        max_bytes,
        gap_recovery,
        |chunk, range, start| TerminalOutputStreamSlice {
            chunk,
            range,
            start,
        },
    )? {
        ReplayParts::Gap {
            requested,
            available_from,
            next_offset,
            recovery,
        } => Ok(TerminalOutputStreamReplay::Gap {
            requested,
            available_from,
            next_offset,
            recovery,
        }),
        ReplayParts::Data {
            slices,
            from,
            to,
            next_offset,
            has_more,
        } => Ok(TerminalOutputStreamReplay::Data {
            slices,
            from,
            to,
            next_offset,
            has_more,
        }),
    }
}

enum ReplayParts<S> {
    Gap {
        requested: OutputOffset,
        available_from: OutputOffset,
        next_offset: OutputOffset,
        recovery: OutputGapRecovery,
    },
    Data {
        slices: Vec<S>,
        from: OutputOffset,
        to: OutputOffset,
        next_offset: OutputOffset,
        has_more: bool,
    },
}

fn replay_parts_locked<S>(
    state: &TerminalOutputState,
    session_id: &SessionId,
    from: OutputOffset,
    max_bytes: usize,
    gap_recovery: OutputGapRecovery,
    mut make_slice: impl FnMut(Arc<[u8]>, Range<usize>, OutputOffset) -> S,
) -> Result<ReplayParts<S>, TerminalOutputError> {
    let Some(buffer) = state.sessions.get(session_id) else {
        if from == OutputOffset::ZERO {
            return Ok(ReplayParts::Data {
                slices: Vec::new(),
                from,
                to: from,
                next_offset: OutputOffset::ZERO,
                has_more: false,
            });
        }
        return Err(TerminalOutputError::FutureOffset {
            requested: from,
            next_offset: OutputOffset::ZERO,
        });
    };
    let available_from = buffer.available_from();
    if from < available_from {
        return Ok(ReplayParts::Gap {
            requested: from,
            available_from,
            next_offset: buffer.next_offset,
            recovery: gap_recovery,
        });
    }
    if from > buffer.next_offset {
        return Err(TerminalOutputError::FutureOffset {
            requested: from,
            next_offset: buffer.next_offset,
        });
    }
    let mut slices = Vec::new();
    let mut remaining = max_bytes;
    let mut to = from;
    for chunk in &buffer.chunks {
        if remaining == 0 {
            break;
        }
        if chunk.end <= from {
            continue;
        }
        let local_start = if from > chunk.start {
            usize::try_from(from.0 - chunk.start.0).map_err(|_| {
                TerminalOutputError::InvariantViolation {
                    rule: "output replay local start does not fit memory index",
                }
            })?
        } else {
            0
        };
        let available = chunk.bytes.len() - local_start;
        let take = available.min(remaining);
        let start = OutputOffset(chunk.start.0 + local_start as u64);
        slices.push(make_slice(
            Arc::clone(&chunk.bytes),
            local_start..local_start + take,
            start,
        ));
        remaining -= take;
        to = OutputOffset(start.0 + take as u64);
    }
    Ok(ReplayParts::Data {
        slices,
        from,
        to,
        next_offset: buffer.next_offset,
        has_more: to < buffer.next_offset,
    })
}

#[derive(Default)]
struct TerminalOutputState {
    sessions: BTreeMap<SessionId, SessionOutputBuffer>,
    subscriptions: BTreeMap<
        SessionId,
        BTreeMap<TerminalOutputSubscriptionId, Weak<Mutex<TerminalOutputSubscriptionState>>>,
    >,
    global_order: BTreeMap<u64, (SessionId, OutputOffset)>,
    retained_bytes: usize,
    next_chunk_sequence: u64,
}

impl TerminalOutputState {
    fn subscription_count(&self) -> usize {
        self.subscriptions.values().map(BTreeMap::len).sum()
    }

    fn remove_dead_subscriptions(&mut self) {
        self.subscriptions.retain(|_, subscriptions| {
            subscriptions.retain(|_, subscription| subscription.strong_count() > 0);
            !subscriptions.is_empty()
        });
    }

    fn remove_subscription(
        &mut self,
        session_id: &SessionId,
        subscription_id: TerminalOutputSubscriptionId,
    ) {
        let Some(subscriptions) = self.subscriptions.get_mut(session_id) else {
            return;
        };
        subscriptions.remove(&subscription_id);
        if subscriptions.is_empty() {
            self.subscriptions.remove(session_id);
        }
    }

    fn take_subscriptions(
        &mut self,
        session_id: &SessionId,
    ) -> Vec<Arc<Mutex<TerminalOutputSubscriptionState>>> {
        self.subscriptions
            .remove(session_id)
            .into_iter()
            .flat_map(|subscriptions| subscriptions.into_values())
            .filter_map(|subscription| subscription.upgrade())
            .collect()
    }

    fn mark_output_dirty(&mut self, session_id: &SessionId) -> Vec<Arc<dyn SubscriptionWake>> {
        let Some(subscriptions) = self.subscriptions.get_mut(session_id) else {
            return Vec::new();
        };
        let mut wakes = Vec::new();
        subscriptions.retain(|_, candidate| {
            let Some(subscription) = candidate.upgrade() else {
                return false;
            };
            let mut subscription = subscription
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if subscription.closed {
                return false;
            }
            if !subscription.dirty {
                subscription.dirty = true;
                wakes.push(Arc::clone(&subscription.wake));
            }
            true
        });
        wakes
    }

    fn evict_session_front(
        &mut self,
        session_id: &SessionId,
    ) -> Result<usize, TerminalOutputError> {
        let buffer =
            self.sessions
                .get_mut(session_id)
                .ok_or(TerminalOutputError::InvariantViolation {
                    rule: "session eviction target is absent",
                })?;
        let chunk = buffer
            .chunks
            .pop_front()
            .ok_or(TerminalOutputError::InvariantViolation {
                rule: "session exceeds retention without a retained chunk",
            })?;
        let bytes = chunk.bytes.len();
        buffer.retained_bytes -= bytes;
        self.retained_bytes -= bytes;
        if self.global_order.remove(&chunk.sequence).is_none() {
            return Err(TerminalOutputError::InvariantViolation {
                rule: "session chunk is absent from global output order",
            });
        }
        Ok(bytes)
    }

    fn evict_global_front(&mut self) -> Result<usize, TerminalOutputError> {
        let sequence = self
            .global_order
            .first_key_value()
            .map(|(sequence, _)| *sequence)
            .ok_or(TerminalOutputError::InvariantViolation {
                rule: "global retention exceeds limit without a retained chunk",
            })?;
        let (session_id, expected_start) =
            self.global_order
                .remove(&sequence)
                .ok_or(TerminalOutputError::InvariantViolation {
                    rule: "oldest global output chunk disappeared",
                })?;
        let buffer =
            self.sessions
                .get_mut(&session_id)
                .ok_or(TerminalOutputError::InvariantViolation {
                    rule: "global output chunk references an absent session",
                })?;
        let chunk = buffer
            .chunks
            .pop_front()
            .ok_or(TerminalOutputError::InvariantViolation {
                rule: "global output chunk references an empty session",
            })?;
        if chunk.sequence != sequence || chunk.start != expected_start {
            return Err(TerminalOutputError::InvariantViolation {
                rule: "global and session output ordering disagree",
            });
        }
        let bytes = chunk.bytes.len();
        buffer.retained_bytes -= bytes;
        self.retained_bytes -= bytes;
        Ok(bytes)
    }
}

struct SessionOutputBuffer {
    chunks: VecDeque<RetainedOutputChunk>,
    retained_bytes: usize,
    next_offset: OutputOffset,
    output_capture_mode: OutputCaptureMode,
}

impl SessionOutputBuffer {
    fn new(next_offset: OutputOffset, output_capture_mode: OutputCaptureMode) -> Self {
        Self {
            chunks: VecDeque::new(),
            retained_bytes: 0,
            next_offset,
            output_capture_mode,
        }
    }

    fn available_from(&self) -> OutputOffset {
        self.chunks
            .front()
            .map_or(self.next_offset, |chunk| chunk.start)
    }
}

struct RetainedOutputChunk {
    sequence: u64,
    start: OutputOffset,
    end: OutputOffset,
    bytes: Arc<[u8]>,
}
