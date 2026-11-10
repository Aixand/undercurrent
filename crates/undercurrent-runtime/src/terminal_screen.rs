// SPDX-License-Identifier: Apache-2.0

//! Bounded terminal capture state synchronized with retained terminal output.

mod health;
mod history;
mod profile;

pub use health::*;
pub use profile::*;

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex, RwLock};

use undercurrent_application::{
    OutputAppend, OutputOffset, PortError, PortErrorKind, PortFuture, PortOperation,
    TerminalOutputStore, TerminalScreenDiff, TerminalScreenDiffer, TerminalScreenReader,
    TerminalScreenSnapshot, TerminalScreenUpdate,
};
use undercurrent_domain::{DomainError, OutputCaptureMode, SessionId, TerminalSize};

/// Hard memory and representation bounds for live terminal screens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalScreenLimits {
    /// Maximum distinct session screens retained at once.
    pub sessions: usize,
    /// Maximum ANSI bytes returned by one current-screen render.
    pub snapshot_bytes: usize,
    /// Maximum normal-screen history rows per session; zero disables history.
    pub scrollback_lines: usize,
}

impl TerminalScreenLimits {
    /// Validates non-zero live-screen bounds.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed configuration error when the session capacity or snapshot
    /// byte limit is zero. Zero scrollback rows disables history.
    pub const fn validate(self) -> Result<Self, TerminalScreenStoreError> {
        if self.sessions == 0 {
            return Err(TerminalScreenStoreError::InvalidLimit { field: "sessions" });
        }
        if self.snapshot_bytes == 0 {
            return Err(TerminalScreenStoreError::InvalidLimit {
                field: "snapshot_bytes",
            });
        }
        Ok(self)
    }
}

/// Payload-safe pressure for the live terminal screen adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalScreenPressure {
    /// Current session screen records.
    pub sessions: usize,
    /// Configured inclusive session capacity.
    pub session_limit: usize,
    /// Configured maximum rendered snapshot bytes.
    pub snapshot_byte_limit: usize,
}

/// Failure from bounded VT screen state.
#[derive(Debug)]
pub enum TerminalScreenStoreError {
    /// One configured hard limit was zero.
    InvalidLimit { field: &'static str },
    /// A new session would exceed the screen-record capacity.
    SessionCapacity { limit: usize },
    /// One rendered screen exceeded its configured byte limit.
    SnapshotCapacity { observed: usize, limit: usize },
    /// One observation requested too many distinct screen regions.
    SampleCapacity { observed: usize, limit: usize },
    /// One screen exceeded the bounded motion-fingerprint row count.
    MotionRowCapacity { observed: usize, limit: usize },
    /// One screen exceeded the bounded health-check cell count.
    ScreenCellCapacity { observed: usize, limit: usize },
    /// A parser-backed sample violated the domain sample contract.
    InvalidSample(DomainError),
    /// Retained terminal output failed while applying one screen update.
    Output(undercurrent_application::TerminalOutputError),
    /// A concrete parser returned invalid screen geometry.
    InvalidSnapshot(undercurrent_application::TerminalScreenSnapshotError),
    /// A concrete parser produced an invalid screen update.
    InvalidDiff(undercurrent_application::TerminalScreenDiffError),
    /// A checkpoint supplied to the differ was newer than the current screen.
    FutureCheckpoint {
        requested: OutputOffset,
        current: OutputOffset,
    },
}

impl Display for TerminalScreenStoreError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLimit { field } => {
                write!(formatter, "terminal screen limit {field} must be non-zero")
            }
            Self::SessionCapacity { limit } => {
                write!(
                    formatter,
                    "terminal screen session limit of {limit} was reached"
                )
            }
            Self::SnapshotCapacity { observed, limit } => write!(
                formatter,
                "rendered terminal screen bytes {observed} exceed limit {limit}"
            ),
            Self::SampleCapacity { observed, limit } => write!(
                formatter,
                "terminal screen sample count {observed} exceeds limit {limit}"
            ),
            Self::MotionRowCapacity { observed, limit } => write!(
                formatter,
                "terminal motion rows {observed} exceed limit {limit}"
            ),
            Self::ScreenCellCapacity { observed, limit } => write!(
                formatter,
                "terminal screen cells {observed} exceed limit {limit}"
            ),
            Self::InvalidSample(error) => Display::fmt(error, formatter),
            Self::Output(error) => Display::fmt(error, formatter),
            Self::InvalidSnapshot(error) => Display::fmt(error, formatter),
            Self::InvalidDiff(error) => Display::fmt(error, formatter),
            Self::FutureCheckpoint { requested, current } => write!(
                formatter,
                "terminal screen checkpoint {} is newer than current checkpoint {}",
                requested.value(),
                current.value()
            ),
        }
    }
}

impl Error for TerminalScreenStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Output(error) => Some(error),
            Self::InvalidSnapshot(error) => Some(error),
            Self::InvalidDiff(error) => Some(error),
            Self::InvalidSample(error) => Some(error),
            Self::InvalidLimit { .. }
            | Self::SessionCapacity { .. }
            | Self::SnapshotCapacity { .. }
            | Self::SampleCapacity { .. }
            | Self::MotionRowCapacity { .. }
            | Self::ScreenCellCapacity { .. }
            | Self::FutureCheckpoint { .. } => None,
        }
    }
}

/// Concrete bounded terminal-screen adapter backed by `vt100`.
pub struct VtTerminalScreenStore {
    limits: TerminalScreenLimits,
    screens: RwLock<BTreeMap<SessionId, TerminalCaptureEntry>>,
}

/// Reversible installation of a new process incarnation's blank screen.
pub struct TerminalScreenSessionUpdate<'a> {
    store: &'a VtTerminalScreenStore,
    session_id: SessionId,
    installed: TerminalCaptureEntry,
    prior: Option<TerminalCaptureEntry>,
    committed: bool,
}

#[derive(Clone)]
enum TerminalCaptureEntry {
    Terminal(Arc<Mutex<VtTerminalScreen>>),
    Opaque(Arc<Mutex<OpaqueOutputCapture>>),
}

struct VtTerminalScreen {
    parser: vt100::Parser,
    process_start_offset: OutputOffset,
    output_offset: OutputOffset,
}

struct OpaqueOutputCapture {
    output_offset: OutputOffset,
}

impl TerminalCaptureEntry {
    const fn capture_mode(&self) -> OutputCaptureMode {
        match self {
            Self::Terminal(_) => OutputCaptureMode::Terminal,
            Self::Opaque(_) => OutputCaptureMode::Opaque,
        }
    }

    fn ptr_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Terminal(left), Self::Terminal(right)) => Arc::ptr_eq(left, right),
            (Self::Opaque(left), Self::Opaque(right)) => Arc::ptr_eq(left, right),
            (Self::Terminal(_), Self::Opaque(_)) | (Self::Opaque(_), Self::Terminal(_)) => false,
        }
    }
}

impl VtTerminalScreenStore {
    /// Creates an empty screen store without starting threads or doing IO.
    ///
    /// # Errors
    ///
    /// Returns a typed invalid-limit error.
    pub fn new(limits: TerminalScreenLimits) -> Result<Self, TerminalScreenStoreError> {
        Ok(Self {
            limits: limits.validate()?,
            screens: RwLock::new(BTreeMap::new()),
        })
    }

    /// Starts or resets one session screen at an exact retained-output offset.
    ///
    /// Replacing an existing session is the restart behavior: the new process
    /// begins from a blank parser while the raw-output offset remains monotonic.
    /// This method allocates parser state, performs no external IO, and does not
    /// panic.
    ///
    /// # Errors
    ///
    /// Returns a typed capacity failure for a new session beyond the limit.
    pub fn begin_session(
        &self,
        session_id: SessionId,
        size: TerminalSize,
        output_offset: OutputOffset,
    ) -> Result<(), TerminalScreenStoreError> {
        self.begin_session_with_capture_mode(
            session_id,
            size,
            output_offset,
            OutputCaptureMode::Terminal,
        )
    }

    /// Starts or resets one session capture at an exact retained-output offset.
    ///
    /// Terminal capture allocates a blank VT parser. Opaque capture retains only
    /// the exact output checkpoint and never constructs a parser. This method
    /// performs no external IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed capacity failure for a new session beyond the limit.
    pub fn begin_session_with_capture_mode(
        &self,
        session_id: SessionId,
        size: TerminalSize,
        output_offset: OutputOffset,
        capture_mode: OutputCaptureMode,
    ) -> Result<(), TerminalScreenStoreError> {
        self.prepare_session_with_capture_mode(session_id, size, output_offset, capture_mode)?
            .commit();
        Ok(())
    }

    /// Replaces one parser with an authoritative bounded screen checkpoint.
    ///
    /// The snapshot rendering is rehydrated without reading raw history. The
    /// supplied local output offset becomes the next append checkpoint for
    /// this store and is independent of any remote snapshot offset. This
    /// method performs no external IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed session-capacity or snapshot-size failure.
    pub fn restore_snapshot_at(
        &self,
        session_id: SessionId,
        snapshot: &TerminalScreenSnapshot,
        local_output_offset: OutputOffset,
    ) -> Result<(), TerminalScreenStoreError> {
        if snapshot.contents().len() > self.limits.snapshot_bytes {
            return Err(TerminalScreenStoreError::SnapshotCapacity {
                observed: snapshot.contents().len(),
                limit: self.limits.snapshot_bytes,
            });
        }
        let mut screens = self
            .screens
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !screens.contains_key(&session_id) && screens.len() >= self.limits.sessions {
            return Err(TerminalScreenStoreError::SessionCapacity {
                limit: self.limits.sessions,
            });
        }
        screens.insert(
            session_id,
            TerminalCaptureEntry::Terminal(Arc::new(Mutex::new(VtTerminalScreen {
                parser: parser_from_snapshot(snapshot, self.limits.scrollback_lines),
                process_start_offset: local_output_offset,
                output_offset: local_output_offset,
            }))),
        );
        Ok(())
    }

    /// Reversibly installs one new process incarnation's blank screen.
    ///
    /// Dropping the returned update before [`TerminalScreenSessionUpdate::commit`]
    /// restores the prior screen, if any. This lets process spawning fail after
    /// parser allocation without destroying the last observable screen.
    ///
    /// # Errors
    ///
    /// Returns a typed capacity failure for a new session beyond the limit.
    pub fn prepare_session(
        &self,
        session_id: SessionId,
        size: TerminalSize,
        output_offset: OutputOffset,
    ) -> Result<TerminalScreenSessionUpdate<'_>, TerminalScreenStoreError> {
        self.prepare_session_with_capture_mode(
            session_id,
            size,
            output_offset,
            OutputCaptureMode::Terminal,
        )
    }

    /// Reversibly installs capture state for one new process incarnation.
    ///
    /// Terminal capture installs a blank parser. Opaque capture installs only
    /// an exact raw-output checkpoint, so arbitrary bytes cannot reach `vt100`.
    /// Dropping the returned update before commit restores the prior entry.
    ///
    /// # Errors
    ///
    /// Returns a typed capacity failure for a new session beyond the limit.
    pub fn prepare_session_with_capture_mode(
        &self,
        session_id: SessionId,
        size: TerminalSize,
        output_offset: OutputOffset,
        capture_mode: OutputCaptureMode,
    ) -> Result<TerminalScreenSessionUpdate<'_>, TerminalScreenStoreError> {
        let mut screens = self
            .screens
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !screens.contains_key(&session_id) && screens.len() >= self.limits.sessions {
            return Err(TerminalScreenStoreError::SessionCapacity {
                limit: self.limits.sessions,
            });
        }
        let installed = match capture_mode {
            OutputCaptureMode::Terminal => {
                TerminalCaptureEntry::Terminal(Arc::new(Mutex::new(VtTerminalScreen {
                    parser: vt100::Parser::new(
                        size.rows(),
                        size.columns(),
                        self.limits.scrollback_lines,
                    ),
                    process_start_offset: output_offset,
                    output_offset,
                })))
            }
            OutputCaptureMode::Opaque => {
                TerminalCaptureEntry::Opaque(Arc::new(Mutex::new(OpaqueOutputCapture {
                    output_offset,
                })))
            }
        };
        let prior = screens.insert(session_id.clone(), installed.clone());
        Ok(TerminalScreenSessionUpdate {
            store: self,
            session_id,
            installed,
            prior,
            committed: false,
        })
    }

    /// Retains raw output and advances its screen parser under one session lock.
    ///
    /// Successful output retention happens before parser advancement, and the
    /// parser checkpoint is then set to the exact appended end offset. A screen
    /// snapshot cannot observe either half of that transition. This method may
    /// wake output subscriptions through `output` and does not panic.
    ///
    /// # Errors
    ///
    /// Returns absence, checkpoint divergence, or retained-output failures.
    pub fn observe_output(
        &self,
        output: &TerminalOutputStore,
        session_id: &SessionId,
        bytes: &[u8],
    ) -> Result<OutputAppend, PortError> {
        match self.resolve_entry(session_id, PortOperation::RetainTerminalOutput)? {
            TerminalCaptureEntry::Terminal(screen) => {
                let mut screen = screen
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let appended =
                    append_at_checkpoint(output, session_id, screen.output_offset, bytes)?;
                screen.parser.process(bytes);
                screen.output_offset = appended.end;
                Ok(appended)
            }
            TerminalCaptureEntry::Opaque(capture) => {
                let mut capture = capture
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let appended =
                    append_at_checkpoint(output, session_id, capture.output_offset, bytes)?;
                capture.output_offset = appended.end;
                Ok(appended)
            }
        }
    }

    /// Drains bounded parser-generated terminal query responses.
    pub(crate) fn take_replies(&self, session_id: &SessionId) -> Vec<u8> {
        match self.resolve_entry(session_id, PortOperation::RetainTerminalOutput) {
            Ok(TerminalCaptureEntry::Terminal(screen)) => screen
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .parser
                .take_replies(),
            _ => Vec::new(),
        }
    }

    /// Applies dimensions to one existing parser after its PTY was resized.
    ///
    /// This method performs no external IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed missing-screen failure.
    pub fn resize(&self, session_id: &SessionId, size: TerminalSize) -> Result<(), PortError> {
        match self.resolve_entry(session_id, PortOperation::ResizeTerminal)? {
            TerminalCaptureEntry::Terminal(screen) => screen
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .parser
                .set_size(size.rows(), size.columns()),
            TerminalCaptureEntry::Opaque(_) => {}
        }
        Ok(())
    }

    /// Returns the installed capture mode without exposing parser or payload state.
    pub fn capture_mode(&self, session_id: &SessionId) -> Option<OutputCaptureMode> {
        self.screens
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(session_id)
            .map(TerminalCaptureEntry::capture_mode)
    }

    /// Removes one screen record without affecting retained output.
    ///
    /// Missing sessions are idempotent. This method performs no external IO and
    /// does not panic.
    pub fn remove_session(&self, session_id: &SessionId) -> bool {
        self.screens
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(session_id)
            .is_some()
    }

    /// Returns payload-free live-screen pressure.
    pub fn pressure(&self) -> TerminalScreenPressure {
        TerminalScreenPressure {
            sessions: self
                .screens
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len(),
            session_limit: self.limits.sessions,
            snapshot_byte_limit: self.limits.snapshot_bytes,
        }
    }

    fn resolve(
        &self,
        session_id: &SessionId,
        operation: PortOperation,
    ) -> Result<Arc<Mutex<VtTerminalScreen>>, PortError> {
        match self.resolve_entry(session_id, operation)? {
            TerminalCaptureEntry::Terminal(screen) => Ok(screen),
            TerminalCaptureEntry::Opaque(_) => Err(opaque_screen_port_error(session_id, operation)),
        }
    }

    fn resolve_entry(
        &self,
        session_id: &SessionId,
        operation: PortOperation,
    ) -> Result<TerminalCaptureEntry, PortError> {
        self.screens
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(session_id)
            .cloned()
            .ok_or_else(|| {
                PortError::new(
                    PortErrorKind::NotFound,
                    operation,
                    Some(session_id.clone()),
                    TerminalScreenMissing,
                )
            })
    }

    fn snapshot_now(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<TerminalScreenSnapshot>, PortError> {
        let entry = {
            self.screens
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(session_id)
                .cloned()
        };
        let Some(entry) = entry else {
            return Ok(None);
        };
        let TerminalCaptureEntry::Terminal(screen) = entry else {
            return Err(opaque_screen_port_error(
                session_id,
                PortOperation::ReadTerminalScreen,
            ));
        };
        let mut screen = screen
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let rendered = screen.parser.screen();
        let (rows, columns) = rendered.size();
        let (cursor_row, cursor_column) = rendered.cursor_position();
        // vt100 represents a pending autowrap with column == columns. It is
        // still visually on the last cell, not outside the terminal. Preserve
        // the pending wrap in contents_formatted(), but expose an in-bounds
        // cursor in the parser-independent snapshot contract.
        let cursor_column = cursor_column.min(columns - 1);
        let cursor_visible = !rendered.hide_cursor();
        let alternate = rendered.alternate_screen();
        let mut contents = rendered.contents_formatted();
        // A late attach has not seen the child's startup escapes. Restore input
        // modes as well as cells, or mouse events and paste framing are lost.
        contents.extend(rendered.input_mode_formatted());
        if contents.len() > self.limits.snapshot_bytes {
            return Err(screen_port_error(
                session_id,
                PortOperation::ReadTerminalScreen,
                TerminalScreenStoreError::SnapshotCapacity {
                    observed: contents.len(),
                    limit: self.limits.snapshot_bytes,
                },
            ));
        }
        let contents =
            history::with_history(&mut screen.parser, contents, self.limits.snapshot_bytes);
        TerminalScreenSnapshot::new(
            screen.output_offset,
            columns,
            rows,
            cursor_row,
            cursor_column,
            cursor_visible,
            alternate,
            Arc::from(contents),
        )
        .map(Some)
        .map_err(TerminalScreenStoreError::InvalidSnapshot)
        .map_err(|error| screen_port_error(session_id, PortOperation::ReadTerminalScreen, error))
    }

    fn diff_now(
        &self,
        session_id: &SessionId,
        previous: &TerminalScreenSnapshot,
        current: &TerminalScreenSnapshot,
    ) -> Result<TerminalScreenUpdate, PortError> {
        if previous.output_offset() > current.output_offset() {
            return Err(screen_port_error(
                session_id,
                PortOperation::DiffTerminalScreen,
                TerminalScreenStoreError::FutureCheckpoint {
                    requested: previous.output_offset(),
                    current: current.output_offset(),
                },
            ));
        }
        if previous.contents().len() > self.limits.snapshot_bytes {
            return Err(screen_port_error(
                session_id,
                PortOperation::DiffTerminalScreen,
                TerminalScreenStoreError::SnapshotCapacity {
                    observed: previous.contents().len(),
                    limit: self.limits.snapshot_bytes,
                },
            ));
        }
        if previous.columns() != current.columns()
            || previous.rows() != current.rows()
            || previous.alternate_screen() != current.alternate_screen()
        {
            return Ok(TerminalScreenUpdate::Snapshot(current.clone()));
        }

        let previous_parser = parser_from_snapshot(previous, 0);
        let current_parser = parser_from_snapshot(current, 0);
        let mut contents = current_parser
            .screen()
            .contents_diff(previous_parser.screen());
        contents.extend(
            current_parser
                .screen()
                .input_mode_diff(previous_parser.screen()),
        );
        if contents.len() > self.limits.snapshot_bytes {
            return Ok(TerminalScreenUpdate::Snapshot(current.clone()));
        }
        TerminalScreenDiff::new(
            previous.output_offset(),
            current.clone(),
            Arc::from(contents),
        )
        .map(TerminalScreenUpdate::Diff)
        .map_err(TerminalScreenStoreError::InvalidDiff)
        .map_err(|error| screen_port_error(session_id, PortOperation::DiffTerminalScreen, error))
    }
}

/// Reconstructs a parser from a bounded visible snapshot for diffing or recovery.
///
/// This creates a new parser without reading retained logs or accessing the
/// existing live parser. The restore caller may install the result as the live
/// projection. The snapshot's alternate-buffer bit selects the target grid
/// before applying its complete ANSI rendering. This performs no external IO
/// and does not panic.
fn parser_from_snapshot(
    snapshot: &TerminalScreenSnapshot,
    scrollback_lines: usize,
) -> vt100::Parser {
    let mut parser = vt100::Parser::new(snapshot.rows(), snapshot.columns(), scrollback_lines);
    if snapshot.alternate_screen() {
        parser.process(b"\x1b[?1049h");
    }
    parser.process(snapshot.contents());
    parser
}

impl TerminalScreenSessionUpdate<'_> {
    /// Keeps the already-installed capture entry by disarming rollback.
    ///
    /// Installation occurs during preparation and is visible to concurrent
    /// readers before this call. This is an ownership guard, not an isolated
    /// transaction spanning process spawn and screen reads.
    ///
    /// This method performs no IO, cannot fail, and does not panic.
    pub fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for TerminalScreenSessionUpdate<'_> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let mut screens = self
            .store
            .screens
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if screens
            .get(&self.session_id)
            .is_some_and(|screen| screen.ptr_eq(&self.installed))
        {
            if let Some(prior) = self.prior.take() {
                screens.insert(self.session_id.clone(), prior);
            } else {
                screens.remove(&self.session_id);
            }
        }
    }
}

impl TerminalScreenReader for VtTerminalScreenStore {
    fn snapshot<'a>(
        &'a self,
        session_id: &'a SessionId,
    ) -> PortFuture<'a, Result<Option<TerminalScreenSnapshot>, PortError>> {
        Box::pin(async move { self.snapshot_now(session_id) })
    }
}

impl TerminalScreenDiffer for VtTerminalScreenStore {
    fn diff<'a>(
        &'a self,
        session_id: &'a SessionId,
        previous: &'a TerminalScreenSnapshot,
        current: &'a TerminalScreenSnapshot,
    ) -> PortFuture<'a, Result<TerminalScreenUpdate, PortError>> {
        Box::pin(async move { self.diff_now(session_id, previous, current) })
    }
}

#[derive(Debug)]
struct TerminalScreenMissing;

impl Display for TerminalScreenMissing {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("session has no terminal screen state")
    }
}

impl Error for TerminalScreenMissing {}

#[derive(Debug)]
struct OpaqueTerminalScreenUnavailable;

impl Display for OpaqueTerminalScreenUnavailable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("opaque output capture has no terminal screen state")
    }
}

impl Error for OpaqueTerminalScreenUnavailable {}

fn opaque_screen_port_error(session_id: &SessionId, operation: PortOperation) -> PortError {
    PortError::new(
        PortErrorKind::Unsupported,
        operation,
        Some(session_id.clone()),
        OpaqueTerminalScreenUnavailable,
    )
}

fn append_at_checkpoint(
    output: &TerminalOutputStore,
    session_id: &SessionId,
    checkpoint: OutputOffset,
    bytes: &[u8],
) -> Result<OutputAppend, PortError> {
    output
        .append_at_checkpoint(session_id, checkpoint, bytes)
        .map_err(TerminalScreenStoreError::Output)
        .map_err(|error| screen_port_error(session_id, PortOperation::RetainTerminalOutput, error))
}

fn screen_port_error(
    session_id: &SessionId,
    operation: PortOperation,
    error: TerminalScreenStoreError,
) -> PortError {
    let kind = match error {
        TerminalScreenStoreError::SessionCapacity { .. }
        | TerminalScreenStoreError::SnapshotCapacity { .. }
        | TerminalScreenStoreError::SampleCapacity { .. }
        | TerminalScreenStoreError::MotionRowCapacity { .. }
        | TerminalScreenStoreError::ScreenCellCapacity { .. } => PortErrorKind::CapacityExceeded,
        TerminalScreenStoreError::Output(ref output) => match output {
            undercurrent_application::TerminalOutputError::SessionCapacity { .. }
            | undercurrent_application::TerminalOutputError::SubscriptionCapacity { .. }
            | undercurrent_application::TerminalOutputError::AppendCapacity { .. }
            | undercurrent_application::TerminalOutputError::ReplayCapacity { .. } => {
                PortErrorKind::CapacityExceeded
            }
            undercurrent_application::TerminalOutputError::CheckpointMismatch { .. } => {
                PortErrorKind::InvalidData
            }
            _ => PortErrorKind::Other,
        },
        TerminalScreenStoreError::InvalidLimit { .. }
        | TerminalScreenStoreError::InvalidSample(_)
        | TerminalScreenStoreError::InvalidSnapshot(_)
        | TerminalScreenStoreError::InvalidDiff(_)
        | TerminalScreenStoreError::FutureCheckpoint { .. } => PortErrorKind::InvalidData,
    };
    PortError::new(kind, operation, Some(session_id.clone()), error)
}
