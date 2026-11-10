// SPDX-License-Identifier: Apache-2.0

//! Bounded cooperative shell foreground signals extracted from PTY output.

use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

pub use undercurrent_domain::{
    COOPERATIVE_OSC_NUMBER, CooperativeEnvironmentVariable, CooperativeForegroundState,
};
use undercurrent_domain::{COOPERATIVE_OSC_PREFIX, ProcessInstanceId, SessionId, TimestampMillis};

const ESCAPE: u8 = 0x1b;
const BELL: u8 = 0x07;
/// Maximum private payload bytes accepted in one cooperative OSC record.
pub const COOPERATIVE_PAYLOAD_BYTES: usize = 4_096;

/// One exact-process cooperative foreground observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CooperativeForegroundSignal {
    process: ProcessInstanceId,
    foreground: CooperativeForegroundState,
    observed_at: TimestampMillis,
}

impl CooperativeForegroundSignal {
    /// Returns the exact process incarnation that emitted the signal.
    pub const fn process(&self) -> &ProcessInstanceId {
        &self.process
    }

    /// Returns the payload-free cooperative state.
    pub const fn foreground(&self) -> CooperativeForegroundState {
        self.foreground
    }

    /// Returns the runtime time at which the complete signal was observed.
    pub const fn observed_at(&self) -> TimestampMillis {
        self.observed_at
    }
}

/// Payload-free occupancy and parser outcomes for cooperative signals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CooperativeSignalPressure {
    /// Session entries currently retaining one signal.
    pub retained_signals: usize,
    /// Inclusive retained-session limit.
    pub retained_signal_limit: usize,
    /// Valid cooperative records accepted since startup.
    pub accepted_signals: u64,
    /// Complete but invalid or incomplete cooperative records.
    pub malformed_signals: u64,
    /// Cooperative records discarded after exceeding the payload bound.
    pub oversized_signals: u64,
    /// Oldest retained entries removed on bounded admission.
    pub evictions: u64,
}

/// Bounded exact-process store for the latest cooperative foreground signal.
pub struct CooperativeSignalStore {
    limit: usize,
    signals: Mutex<BTreeMap<SessionId, CooperativeForegroundSignal>>,
    accepted: AtomicU64,
    malformed: AtomicU64,
    oversized: AtomicU64,
    evictions: AtomicU64,
}

impl Debug for CooperativeSignalStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CooperativeSignalStore")
            .field("pressure", &self.pressure())
            .finish_non_exhaustive()
    }
}

impl CooperativeSignalStore {
    /// Creates an empty store with one hard retained-session limit.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed limit failure when the retained-session limit is zero.
    pub fn new(retained_signal_limit: usize) -> Result<Self, crate::RuntimeLimitError> {
        if retained_signal_limit == 0 {
            return Err(crate::RuntimeLimitError::zero(
                "cooperative_retained_signals",
            ));
        }
        Ok(Self {
            limit: retained_signal_limit,
            signals: Mutex::new(BTreeMap::new()),
            accepted: AtomicU64::new(0),
            malformed: AtomicU64::new(0),
            oversized: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        })
    }

    /// Retains one payload-free state for an exact managed-process incarnation.
    ///
    /// Admission evicts the oldest observation when the bounded session map is
    /// full. This function performs no external IO and does not panic.
    pub fn record(
        &self,
        session_id: SessionId,
        process: ProcessInstanceId,
        foreground: CooperativeForegroundState,
        observed_at: TimestampMillis,
    ) {
        let signal = CooperativeForegroundSignal {
            process,
            foreground,
            observed_at,
        };
        let mut signals = self.lock_signals();
        if !signals.contains_key(&session_id) && signals.len() >= self.limit {
            let oldest = signals
                .iter()
                .min_by(|(left_id, left), (right_id, right)| {
                    (left.observed_at, *left_id).cmp(&(right.observed_at, *right_id))
                })
                .map(|(session_id, _)| session_id.clone());
            if let Some(oldest) = oldest {
                signals.remove(&oldest);
                increment(&self.evictions);
            }
        }
        signals.insert(session_id, signal);
        increment(&self.accepted);
    }

    /// Returns the latest signal only when it belongs to the requested process.
    ///
    /// This exact-incarnation check prevents a restarted session from using a
    /// predecessor's foreground. This function performs no external IO and does
    /// not panic.
    pub fn latest_exact(
        &self,
        session_id: &SessionId,
        process: &ProcessInstanceId,
    ) -> Option<CooperativeForegroundSignal> {
        self.lock_signals()
            .get(session_id)
            .filter(|signal| signal.process == *process)
            .cloned()
    }

    /// Removes a retained signal only when its process incarnation matches.
    ///
    /// This function performs no external IO and does not panic.
    pub fn remove_exact(&self, session_id: &SessionId, process: &ProcessInstanceId) {
        let mut signals = self.lock_signals();
        if signals
            .get(session_id)
            .is_some_and(|signal| signal.process == *process)
        {
            signals.remove(session_id);
        }
    }

    /// Returns payload-free occupancy and cumulative parser outcomes.
    ///
    /// This function performs no external IO and does not panic.
    pub fn pressure(&self) -> CooperativeSignalPressure {
        CooperativeSignalPressure {
            retained_signals: self.lock_signals().len(),
            retained_signal_limit: self.limit,
            accepted_signals: self.accepted.load(Ordering::Acquire),
            malformed_signals: self.malformed.load(Ordering::Acquire),
            oversized_signals: self.oversized.load(Ordering::Acquire),
            evictions: self.evictions.load(Ordering::Acquire),
        }
    }

    pub(crate) fn record_filter_outcomes(&self, malformed: u64, oversized: u64) {
        saturating_add(&self.malformed, malformed);
        saturating_add(&self.oversized, oversized);
    }

    fn lock_signals(
        &self,
    ) -> std::sync::MutexGuard<'_, BTreeMap<SessionId, CooperativeForegroundSignal>> {
        self.signals
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

pub(crate) struct FilteredTerminalOutput {
    pub(crate) visible: Vec<u8>,
    pub(crate) signals: Vec<CooperativeForegroundState>,
    pub(crate) malformed: u64,
    pub(crate) oversized: u64,
}

pub(crate) struct CooperativeOscFilter {
    state: FilterState,
    #[cfg(test)]
    payload_storage_activations: usize,
}

enum FilterState {
    Ground,
    Prefix {
        matched: usize,
    },
    PrivatePayload {
        payload: Vec<u8>,
        pending_escape: bool,
    },
    PassThrough {
        prior_escape: bool,
    },
    Discard {
        prior_escape: bool,
    },
}

enum PrivatePayloadTransition {
    Continue,
    Complete,
    OversizedTerminated,
    Oversized { prior_escape: bool },
}

impl CooperativeOscFilter {
    pub(crate) fn new() -> Self {
        debug_assert_eq!(COOPERATIVE_OSC_PREFIX.first(), Some(&ESCAPE));
        Self {
            state: FilterState::Ground,
            #[cfg(test)]
            payload_storage_activations: 0,
        }
    }

    pub(crate) fn filter(&mut self, bytes: &[u8]) -> FilteredTerminalOutput {
        let mut output = FilteredTerminalOutput {
            visible: Vec::new(),
            signals: Vec::new(),
            malformed: 0,
            oversized: 0,
        };
        let mut cursor = 0;
        while cursor < bytes.len() {
            if matches!(self.state, FilterState::Ground) {
                let Some(relative_escape) = bytes[cursor..].iter().position(|byte| *byte == ESCAPE)
                else {
                    output.visible.extend_from_slice(&bytes[cursor..]);
                    break;
                };
                let escape = cursor + relative_escape;
                output.visible.extend_from_slice(&bytes[cursor..escape]);
                self.accept_byte(ESCAPE, &mut output);
                cursor = escape + 1;
            } else {
                self.accept_byte(bytes[cursor], &mut output);
                cursor += 1;
            }
        }
        output
    }

    #[cfg(test)]
    fn filter_bytewise(&mut self, bytes: &[u8]) -> FilteredTerminalOutput {
        let mut output = FilteredTerminalOutput {
            visible: Vec::new(),
            signals: Vec::new(),
            malformed: 0,
            oversized: 0,
        };
        for &byte in bytes {
            self.accept_byte(byte, &mut output);
        }
        output
    }

    pub(crate) fn finish(&mut self) -> FilteredTerminalOutput {
        let mut output = FilteredTerminalOutput {
            visible: Vec::new(),
            signals: Vec::new(),
            malformed: 0,
            oversized: 0,
        };
        match std::mem::replace(&mut self.state, FilterState::Ground) {
            FilterState::Prefix { matched } => {
                output
                    .visible
                    .extend_from_slice(&COOPERATIVE_OSC_PREFIX[..matched]);
            }
            FilterState::PrivatePayload { .. } => output.malformed = 1,
            FilterState::Discard { .. } => {}
            FilterState::Ground | FilterState::PassThrough { .. } => {}
        }
        output
    }

    #[cfg(test)]
    fn payload_storage_activations(&self) -> usize {
        self.payload_storage_activations
    }

    fn accept_byte(&mut self, byte: u8, output: &mut FilteredTerminalOutput) {
        match &mut self.state {
            FilterState::Ground => {
                if byte == ESCAPE {
                    self.state = FilterState::Prefix { matched: 1 };
                } else {
                    output.visible.push(byte);
                }
            }
            FilterState::Prefix { matched } => {
                let completes_sequence = byte == BELL
                    || (byte == b'\\'
                        && COOPERATIVE_OSC_PREFIX.get(matched.saturating_sub(1)) == Some(&ESCAPE));
                if completes_sequence {
                    output
                        .visible
                        .extend_from_slice(&COOPERATIVE_OSC_PREFIX[..*matched]);
                    output.visible.push(byte);
                    self.state = FilterState::Ground;
                } else if COOPERATIVE_OSC_PREFIX.get(*matched) == Some(&byte) {
                    *matched += 1;
                    if *matched == COOPERATIVE_OSC_PREFIX.len() {
                        self.state = FilterState::PrivatePayload {
                            payload: Vec::new(),
                            pending_escape: false,
                        };
                    }
                } else {
                    let is_osc = COOPERATIVE_OSC_PREFIX[..*matched].starts_with(b"\x1b]");
                    output
                        .visible
                        .extend_from_slice(&COOPERATIVE_OSC_PREFIX[..*matched]);
                    output.visible.push(byte);
                    let prior_escape = byte == ESCAPE;
                    self.state = if is_osc {
                        FilterState::PassThrough { prior_escape }
                    } else {
                        FilterState::Ground
                    };
                }
            }
            FilterState::PrivatePayload {
                payload,
                pending_escape,
            } => {
                let (transition, _activated) =
                    accept_private_payload_byte(payload, pending_escape, byte);
                #[cfg(test)]
                if _activated {
                    self.payload_storage_activations += 1;
                }
                match transition {
                    PrivatePayloadTransition::Continue => {}
                    PrivatePayloadTransition::Complete => {
                        let payload = std::mem::take(payload);
                        self.state = FilterState::Ground;
                        self.complete_payload(payload, output);
                    }
                    PrivatePayloadTransition::OversizedTerminated => {
                        self.state = FilterState::Ground;
                        output.oversized = output.oversized.saturating_add(1);
                    }
                    PrivatePayloadTransition::Oversized { prior_escape } => {
                        self.state = FilterState::Discard { prior_escape };
                        output.oversized = output.oversized.saturating_add(1);
                    }
                }
            }
            FilterState::PassThrough { prior_escape } => {
                output.visible.push(byte);
                let complete = byte == BELL || (*prior_escape && byte == b'\\');
                *prior_escape = byte == ESCAPE;
                if complete {
                    self.state = FilterState::Ground;
                }
            }
            FilterState::Discard { prior_escape } => {
                let complete = byte == BELL || (*prior_escape && byte == b'\\');
                *prior_escape = byte == ESCAPE;
                if complete {
                    self.state = FilterState::Ground;
                }
            }
        }
    }

    fn complete_payload(&self, payload: Vec<u8>, output: &mut FilteredTerminalOutput) {
        if payload.len() > COOPERATIVE_PAYLOAD_BYTES {
            output.oversized = output.oversized.saturating_add(1);
            return;
        }
        match parse_foreground(&payload) {
            Some(foreground) => output.signals.push(foreground),
            None => output.malformed = output.malformed.saturating_add(1),
        }
    }
}

fn accept_private_payload_byte(
    payload: &mut Vec<u8>,
    pending_escape: &mut bool,
    byte: u8,
) -> (PrivatePayloadTransition, bool) {
    let mut activated = false;
    if byte == BELL {
        if *pending_escape {
            *pending_escape = false;
            if payload.len() == COOPERATIVE_PAYLOAD_BYTES {
                return (PrivatePayloadTransition::OversizedTerminated, false);
            }
            activated |= retain_payload_byte(payload, ESCAPE);
        }
        return (PrivatePayloadTransition::Complete, activated);
    }

    if *pending_escape {
        *pending_escape = false;
        if byte == b'\\' {
            return (PrivatePayloadTransition::Complete, false);
        }
        if payload.len() == COOPERATIVE_PAYLOAD_BYTES {
            return (
                PrivatePayloadTransition::Oversized {
                    prior_escape: byte == ESCAPE,
                },
                false,
            );
        }
        activated |= retain_payload_byte(payload, ESCAPE);
    }

    if byte == ESCAPE {
        *pending_escape = true;
    } else if payload.len() == COOPERATIVE_PAYLOAD_BYTES {
        return (
            PrivatePayloadTransition::Oversized {
                prior_escape: false,
            },
            activated,
        );
    } else {
        activated |= retain_payload_byte(payload, byte);
    }
    (PrivatePayloadTransition::Continue, activated)
}

#[inline]
fn retain_payload_byte(payload: &mut Vec<u8>, byte: u8) -> bool {
    #[cfg(test)]
    let activated = payload.capacity() == 0;
    payload.push(byte);
    #[cfg(test)]
    {
        activated
    }
    #[cfg(not(test))]
    {
        false
    }
}

fn parse_foreground(payload: &[u8]) -> Option<CooperativeForegroundState> {
    let mut selected = None;
    for field in payload.split(|byte| *byte == b';') {
        let Some(value) = field.strip_prefix(b"uc=") else {
            continue;
        };
        let foreground = match value {
            b"ready" | b"done" => CooperativeForegroundState::Ready,
            b"running" => CooperativeForegroundState::Running,
            _ => return None,
        };
        if selected.replace(foreground).is_some() {
            return None;
        }
    }
    selected
}

fn increment(counter: &AtomicU64) {
    saturating_add(counter, 1);
}

fn saturating_add(counter: &AtomicU64, amount: u64) {
    let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        Some(current.saturating_add(amount))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default, PartialEq, Eq)]
    struct FilterOutcome {
        visible: Vec<u8>,
        signals: Vec<CooperativeForegroundState>,
        malformed: u64,
        oversized: u64,
    }

    impl FilterOutcome {
        fn extend(&mut self, output: FilteredTerminalOutput) {
            self.visible.extend(output.visible);
            self.signals.extend(output.signals);
            self.malformed = self.malformed.saturating_add(output.malformed);
            self.oversized = self.oversized.saturating_add(output.oversized);
        }
    }

    fn filtered_in_chunks(bytes: &[u8], chunk_bytes: usize, bytewise: bool) -> FilterOutcome {
        let mut filter = CooperativeOscFilter::new();
        let mut outcome = FilterOutcome::default();
        for chunk in bytes.chunks(chunk_bytes) {
            outcome.extend(if bytewise {
                filter.filter_bytewise(chunk)
            } else {
                filter.filter(chunk)
            });
        }
        outcome.extend(filter.finish());
        outcome
    }

    struct LegacyCooperativeOscFilter {
        state: LegacyFilterState,
    }

    enum LegacyFilterState {
        Ground,
        Candidate(Vec<u8>),
        PassThrough { prior_escape: bool },
        Discard { prior_escape: bool },
    }

    impl LegacyCooperativeOscFilter {
        fn new() -> Self {
            Self {
                state: LegacyFilterState::Ground,
            }
        }

        fn filter(&mut self, bytes: &[u8]) -> FilteredTerminalOutput {
            let mut output = FilteredTerminalOutput {
                visible: Vec::new(),
                signals: Vec::new(),
                malformed: 0,
                oversized: 0,
            };
            for &byte in bytes {
                self.accept_byte(byte, &mut output);
            }
            output
        }

        fn finish(&mut self) -> FilteredTerminalOutput {
            let mut output = FilteredTerminalOutput {
                visible: Vec::new(),
                signals: Vec::new(),
                malformed: 0,
                oversized: 0,
            };
            match std::mem::replace(&mut self.state, LegacyFilterState::Ground) {
                LegacyFilterState::Candidate(candidate)
                    if candidate.starts_with(COOPERATIVE_OSC_PREFIX) =>
                {
                    output.malformed = 1;
                }
                LegacyFilterState::Candidate(candidate) => output.visible = candidate,
                LegacyFilterState::Discard { .. } => {}
                LegacyFilterState::Ground | LegacyFilterState::PassThrough { .. } => {}
            }
            output
        }

        fn accept_byte(&mut self, byte: u8, output: &mut FilteredTerminalOutput) {
            match &mut self.state {
                LegacyFilterState::Ground => {
                    if byte == ESCAPE {
                        self.state = LegacyFilterState::Candidate(vec![byte]);
                    } else {
                        output.visible.push(byte);
                    }
                }
                LegacyFilterState::Candidate(candidate) => {
                    candidate.push(byte);
                    if legacy_sequence_complete(candidate) {
                        let sequence = std::mem::take(candidate);
                        self.state = LegacyFilterState::Ground;
                        self.complete_candidate(sequence, output);
                    } else if !legacy_is_cooperative_candidate(candidate) {
                        output.visible.extend_from_slice(candidate);
                        let prior_escape = byte == ESCAPE;
                        self.state = if candidate.starts_with(b"\x1b]") {
                            LegacyFilterState::PassThrough { prior_escape }
                        } else {
                            LegacyFilterState::Ground
                        };
                    } else if legacy_cooperative_payload_is_oversized(candidate) {
                        self.state = LegacyFilterState::Discard {
                            prior_escape: byte == ESCAPE,
                        };
                        output.oversized = output.oversized.saturating_add(1);
                    }
                }
                LegacyFilterState::PassThrough { prior_escape } => {
                    output.visible.push(byte);
                    let complete = byte == BELL || (*prior_escape && byte == b'\\');
                    *prior_escape = byte == ESCAPE;
                    if complete {
                        self.state = LegacyFilterState::Ground;
                    }
                }
                LegacyFilterState::Discard { prior_escape } => {
                    let complete = byte == BELL || (*prior_escape && byte == b'\\');
                    *prior_escape = byte == ESCAPE;
                    if complete {
                        self.state = LegacyFilterState::Ground;
                    }
                }
            }
        }

        fn complete_candidate(&self, sequence: Vec<u8>, output: &mut FilteredTerminalOutput) {
            if !sequence.starts_with(COOPERATIVE_OSC_PREFIX) {
                output.visible.extend_from_slice(&sequence);
                return;
            }
            let terminator_bytes = if sequence.ends_with(&[ESCAPE, b'\\']) {
                2
            } else {
                1
            };
            let payload_end = sequence.len().saturating_sub(terminator_bytes);
            let payload = &sequence[COOPERATIVE_OSC_PREFIX.len()..payload_end];
            if payload.len() > COOPERATIVE_PAYLOAD_BYTES {
                output.oversized = output.oversized.saturating_add(1);
                return;
            }
            match parse_foreground(payload) {
                Some(foreground) => output.signals.push(foreground),
                None => output.malformed = output.malformed.saturating_add(1),
            }
        }
    }

    fn legacy_is_cooperative_candidate(candidate: &[u8]) -> bool {
        COOPERATIVE_OSC_PREFIX.starts_with(candidate)
            || candidate.starts_with(COOPERATIVE_OSC_PREFIX)
    }

    fn legacy_sequence_complete(candidate: &[u8]) -> bool {
        candidate.ends_with(&[BELL]) || candidate.ends_with(&[ESCAPE, b'\\'])
    }

    fn legacy_cooperative_payload_is_oversized(candidate: &[u8]) -> bool {
        let payload_bytes = candidate.len().saturating_sub(COOPERATIVE_OSC_PREFIX.len());
        payload_bytes > COOPERATIVE_PAYLOAD_BYTES
            && !(payload_bytes == COOPERATIVE_PAYLOAD_BYTES + 1
                && candidate.last() == Some(&ESCAPE))
    }

    fn filtered_at_split(bytes: &[u8], split: usize) -> FilterOutcome {
        let mut filter = CooperativeOscFilter::new();
        let mut outcome = FilterOutcome::default();
        outcome.extend(filter.filter(&bytes[..split]));
        outcome.extend(filter.filter(&bytes[split..]));
        outcome.extend(filter.finish());
        outcome
    }

    fn legacy_filtered_at_split(bytes: &[u8], split: usize) -> FilterOutcome {
        let mut filter = LegacyCooperativeOscFilter::new();
        let mut outcome = FilterOutcome::default();
        outcome.extend(filter.filter(&bytes[..split]));
        outcome.extend(filter.filter(&bytes[split..]));
        outcome.extend(filter.finish());
        outcome
    }

    fn assert_legacy_equivalent_at_every_split(bytes: &[u8], label: &str) {
        for split in 0..=bytes.len() {
            assert_eq!(
                filtered_at_split(bytes, split),
                legacy_filtered_at_split(bytes, split),
                "{label} at split {split}"
            );
        }
    }

    fn private_record(payload: &[u8], terminator: &[u8]) -> Vec<u8> {
        let mut record = Vec::from(COOPERATIVE_OSC_PREFIX);
        record.extend_from_slice(payload);
        record.extend_from_slice(terminator);
        record
    }

    #[test]
    fn ground_bulk_path_is_bytewise_equivalent_across_every_short_chunk_boundary() {
        let mut bytes = Vec::from(
            &b"plain\x1b[31mred\x1b]0;title\x07before\x1b]9091;uc=running;cmd=private\x1b\\after\x1b]9091;uc=unknown\x07tail"[..],
        );
        bytes.extend_from_slice(b"\x1b]9091;uc=running;cmd=incomplete");
        for chunk_bytes in 1..=bytes.len().min(31) {
            assert_eq!(
                filtered_in_chunks(&bytes, chunk_bytes, false),
                filtered_in_chunks(&bytes, chunk_bytes, true),
                "chunk size {chunk_bytes}"
            );
        }
    }

    #[test]
    fn ground_bulk_path_preserves_large_ordinary_payload_exactly() {
        let bytes = (0..1024 * 1024)
            .map(|index| b'a' + (index % 26) as u8)
            .collect::<Vec<_>>();
        let output = CooperativeOscFilter::new().filter(&bytes);
        assert_eq!(output.visible, bytes);
        assert!(output.signals.is_empty());
        assert_eq!(output.malformed, 0);
        assert_eq!(output.oversized, 0);
    }

    #[test]
    fn explicit_prefix_matcher_is_legacy_equivalent_for_every_byte_and_split_point() {
        for matched in 1..COOPERATIVE_OSC_PREFIX.len() {
            for byte in u8::MIN..=u8::MAX {
                let mut bytes = b"lead".to_vec();
                bytes.extend_from_slice(&COOPERATIVE_OSC_PREFIX[..matched]);
                bytes.push(byte);
                bytes.extend_from_slice(b"payload\x1b\\tail");
                assert_legacy_equivalent_at_every_split(
                    &bytes,
                    &format!("matched prefix bytes {matched}, next byte {byte}"),
                );
            }
        }
    }

    #[test]
    fn escape_osc_and_private_records_are_legacy_equivalent_at_every_split_point() {
        let cases = [
            ("empty", Vec::new()),
            ("escape only", Vec::from(&b"before\x1b"[..])),
            ("csi", Vec::from(&b"\x1b[31mred\x1b[0m"[..])),
            ("osc bell", Vec::from(&b"\x1b]0;title\x07tail"[..])),
            (
                "osc string terminator",
                Vec::from(&b"\x1b]2;title\x1b\\tail"[..]),
            ),
            (
                "private bell",
                private_record(b"uc=ready;cmd=private", &[BELL]),
            ),
            (
                "private string terminator",
                private_record(b"uc=running;cmd=private", &[ESCAPE, b'\\']),
            ),
            (
                "ordered private records",
                Vec::from(&b"a\x1b]9091;uc=running\x07b\x1b]9091;uc=ready\x1b\\c"[..]),
            ),
            ("malformed state", private_record(b"uc=unknown", &[BELL])),
            (
                "duplicate state",
                private_record(b"uc=ready;uc=running", &[BELL]),
            ),
            (
                "embedded escape",
                private_record(b"x=left\x1bX;uc=ready", &[ESCAPE, b'\\']),
            ),
            (
                "incomplete prefix",
                COOPERATIVE_OSC_PREFIX[..COOPERATIVE_OSC_PREFIX.len() - 1].to_vec(),
            ),
            (
                "incomplete private payload",
                Vec::from(&b"\x1b]9091;uc=running"[..]),
            ),
            (
                "escape string terminator",
                Vec::from(&b"left\x1b\\right"[..]),
            ),
        ];
        for (label, bytes) in cases {
            assert_legacy_equivalent_at_every_split(&bytes, label);
        }
    }

    #[test]
    fn payload_bounds_are_legacy_equivalent_across_terminators_and_split_points() {
        let mut exact_payload = b"uc=running;pad=".to_vec();
        exact_payload.resize(COOPERATIVE_PAYLOAD_BYTES, b'x');
        let exact_st = private_record(&exact_payload, &[ESCAPE, b'\\']);
        assert_legacy_equivalent_at_every_split(&exact_st, "exact bound plus ST");

        let mut oversized_payload = exact_payload.clone();
        oversized_payload.push(b'x');
        let oversized_bell = private_record(&oversized_payload, &[BELL]);
        assert_legacy_equivalent_at_every_split(&oversized_bell, "oversized plus BEL");

        let mut deferred_escape = private_record(&exact_payload, &[ESCAPE]);
        deferred_escape.extend_from_slice(b"Xdiscarded\x07tail");
        for split in [
            0,
            COOPERATIVE_OSC_PREFIX.len(),
            deferred_escape.len() / 2,
            COOPERATIVE_OSC_PREFIX.len() + COOPERATIVE_PAYLOAD_BYTES,
            COOPERATIVE_OSC_PREFIX.len() + COOPERATIVE_PAYLOAD_BYTES + 1,
            deferred_escape.len(),
        ] {
            assert_eq!(
                filtered_at_split(&deferred_escape, split),
                legacy_filtered_at_split(&deferred_escape, split),
                "deferred oversize escape at split {split}"
            );
        }

        let escape_then_bell = private_record(&exact_payload, &[ESCAPE, BELL]);
        for split in [
            0,
            COOPERATIVE_OSC_PREFIX.len(),
            COOPERATIVE_OSC_PREFIX.len() + COOPERATIVE_PAYLOAD_BYTES,
            COOPERATIVE_OSC_PREFIX.len() + COOPERATIVE_PAYLOAD_BYTES + 1,
            escape_then_bell.len(),
        ] {
            assert_eq!(
                filtered_at_split(&escape_then_bell, split),
                legacy_filtered_at_split(&escape_then_bell, split),
                "oversized pending escape plus BEL at split {split}"
            );
        }
    }

    #[test]
    fn ordinary_escape_candidates_do_not_activate_private_payload_storage() {
        let ordinary = b"\x1b[31mred\x1b[0m\x1b]0;title\x07\x1b]2;other\x1b\\\x1bPdata\x1b\\";
        let mut filter = CooperativeOscFilter::new();
        for chunk in ordinary.chunks(1) {
            let _ = filter.filter(chunk);
        }
        let _ = filter.finish();
        assert_eq!(filter.payload_storage_activations(), 0);

        let empty_bell = private_record(b"", &[BELL]);
        let empty_st = private_record(b"", &[ESCAPE, b'\\']);
        let _ = filter.filter(&empty_bell);
        let _ = filter.filter(&empty_st);
        assert_eq!(filter.payload_storage_activations(), 0);

        let private = private_record(b"uc=ready", &[BELL]);
        let _ = filter.filter(&private);
        assert_eq!(filter.payload_storage_activations(), 1);
    }

    #[test]
    fn cooperative_records_are_stripped_across_chunks() {
        let mut filter = CooperativeOscFilter::new();
        let first = filter.filter(b"before\x1b]9091;uc=run");
        let second = filter.filter(b"ning;cmd=private\x1b\\after");

        assert_eq!(first.visible, b"before");
        assert!(first.signals.is_empty());
        assert_eq!(second.visible, b"after");
        assert_eq!(second.signals, vec![CooperativeForegroundState::Running]);
        assert_eq!(second.malformed, 0);
    }

    #[test]
    fn unrelated_osc_and_escape_sequences_pass_through_exactly() {
        let bytes = b"a\x1b[31mred\x1b]0;private title\x07z";
        let mut filter = CooperativeOscFilter::new();
        let mut visible = Vec::new();
        for chunk in bytes.chunks(2) {
            visible.extend(filter.filter(chunk).visible);
        }
        visible.extend(filter.finish().visible);

        assert_eq!(visible, bytes);
    }

    #[test]
    fn malformed_and_oversized_records_are_bounded_and_payload_free() {
        let mut filter = CooperativeOscFilter::new();
        let malformed = filter.filter(b"\x1b]9091;uc=unknown\x07visible");
        assert_eq!(malformed.visible, b"visible");
        assert_eq!(malformed.malformed, 1);

        let mut oversized = Vec::from(COOPERATIVE_OSC_PREFIX);
        oversized.extend(std::iter::repeat_n(b'x', COOPERATIVE_PAYLOAD_BYTES + 3));
        oversized.extend_from_slice(b"\x07tail");
        let discarded = filter.filter(&oversized);
        assert_eq!(discarded.visible, b"tail");
        assert_eq!(discarded.oversized, 1);
        assert!(format!("{:?}", discarded.signals).find("xxxx").is_none());

        let mut boundary = Vec::from(COOPERATIVE_OSC_PREFIX);
        boundary.extend(std::iter::repeat_n(b'x', COOPERATIVE_PAYLOAD_BYTES + 1));
        boundary.push(BELL);
        assert_eq!(filter.filter(&boundary).oversized, 1);
        assert_eq!(
            CooperativeSignalStore::new(0)
                .expect_err("zero signal limit")
                .field(),
            "cooperative_retained_signals"
        );
    }

    #[test]
    fn exact_process_lookup_rejects_stale_session_signal() {
        let store = CooperativeSignalStore::new(1).expect("signal limit");
        let session = SessionId::new("session").expect("session");
        let first = ProcessInstanceId::new("process-1").expect("process");
        let second = ProcessInstanceId::new("process-2").expect("process");
        store.record(
            session.clone(),
            first.clone(),
            CooperativeForegroundState::Ready,
            TimestampMillis::new(10),
        );

        assert!(store.latest_exact(&session, &second).is_none());
        assert_eq!(
            store
                .latest_exact(&session, &first)
                .map(|signal| signal.foreground),
            Some(CooperativeForegroundState::Ready)
        );
    }
}
