// SPDX-License-Identifier: Apache-2.0

//! Authorized terminal screen snapshots and updates independent of concrete parsers.

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use undercurrent_domain::SessionId;

use crate::{
    AccessAction, AccessDecision, AccessDenialReason, AccessResource, ApplicationErrorCategory,
    ApplicationOperation, AuthorizationPort, Clock, DiagnosticEventName, DiagnosticLevel,
    DiagnosticRecord, MetricName, MetricObservation, OutputOffset, PortError, PortErrorKind,
    Telemetry, TerminalScreenPort,
};

/// Parser-independent current terminal state and the output checkpoint it covers.
#[derive(Clone, PartialEq, Eq)]
pub struct TerminalScreenSnapshot {
    output_offset: OutputOffset,
    columns: u16,
    rows: u16,
    cursor_row: u16,
    cursor_column: u16,
    cursor_visible: bool,
    alternate_screen: bool,
    contents: Arc<[u8]>,
}

impl TerminalScreenSnapshot {
    /// Constructs one coherent bounded screen result supplied by an adapter.
    ///
    /// `contents` is a complete ANSI terminal rendering, not retained raw
    /// output. Construction retains that allocation, performs no IO, and does
    /// not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed invariant error for zero dimensions or a cursor outside
    /// the declared screen.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        output_offset: OutputOffset,
        columns: u16,
        rows: u16,
        cursor_row: u16,
        cursor_column: u16,
        cursor_visible: bool,
        alternate_screen: bool,
        contents: Arc<[u8]>,
    ) -> Result<Self, TerminalScreenSnapshotError> {
        if columns == 0 || rows == 0 {
            return Err(TerminalScreenSnapshotError::ZeroDimensions);
        }
        if cursor_row >= rows || cursor_column >= columns {
            return Err(TerminalScreenSnapshotError::CursorOutOfBounds);
        }
        Ok(Self {
            output_offset,
            columns,
            rows,
            cursor_row,
            cursor_column,
            cursor_visible,
            alternate_screen,
            contents,
        })
    }

    /// Returns the exclusive raw-output offset represented by this screen.
    pub const fn output_offset(&self) -> OutputOffset {
        self.output_offset
    }

    /// Returns terminal columns.
    pub const fn columns(&self) -> u16 {
        self.columns
    }

    /// Returns terminal rows.
    pub const fn rows(&self) -> u16 {
        self.rows
    }

    /// Returns the zero-based cursor row.
    pub const fn cursor_row(&self) -> u16 {
        self.cursor_row
    }

    /// Returns the zero-based cursor column.
    pub const fn cursor_column(&self) -> u16 {
        self.cursor_column
    }

    /// Returns whether the cursor should be rendered.
    pub const fn cursor_visible(&self) -> bool {
        self.cursor_visible
    }

    /// Returns whether the terminal is currently using its alternate screen.
    pub const fn alternate_screen(&self) -> bool {
        self.alternate_screen
    }

    /// Returns the complete ANSI screen rendering.
    ///
    /// The returned bytes are private terminal content and must not be logged.
    pub fn contents(&self) -> &[u8] {
        &self.contents
    }
}

impl Debug for TerminalScreenSnapshot {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TerminalScreenSnapshot")
            .field("output_offset", &self.output_offset)
            .field("columns", &self.columns)
            .field("rows", &self.rows)
            .field("cursor_row", &self.cursor_row)
            .field("cursor_column", &self.cursor_column)
            .field("cursor_visible", &self.cursor_visible)
            .field("alternate_screen", &self.alternate_screen)
            .field("content_bytes", &self.contents.len())
            .finish_non_exhaustive()
    }
}

/// Minimal ANSI update and the authoritative screen checkpoint it reaches.
#[derive(Clone, PartialEq, Eq)]
pub struct TerminalScreenDiff {
    from_output_offset: OutputOffset,
    current: TerminalScreenSnapshot,
    contents: Arc<[u8]>,
}

impl TerminalScreenDiff {
    /// Constructs a bounded update between monotonic output checkpoints.
    ///
    /// The complete current screen is retained so an outer adapter can issue a
    /// new opaque checkpoint without reconstructing or rereading the parser.
    /// Construction performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed invariant error when the baseline is newer than the
    /// current screen.
    pub fn new(
        from_output_offset: OutputOffset,
        current: TerminalScreenSnapshot,
        contents: Arc<[u8]>,
    ) -> Result<Self, TerminalScreenDiffError> {
        if from_output_offset > current.output_offset() {
            return Err(TerminalScreenDiffError::OffsetReversed);
        }
        Ok(Self {
            from_output_offset,
            current,
            contents,
        })
    }

    /// Returns the baseline screen's exclusive raw-output checkpoint.
    pub const fn from_output_offset(&self) -> OutputOffset {
        self.from_output_offset
    }

    /// Returns the complete current checkpoint retained for subsequent diffs.
    pub const fn current(&self) -> &TerminalScreenSnapshot {
        &self.current
    }

    /// Returns the private ANSI update from the baseline to `current`.
    pub fn contents(&self) -> &[u8] {
        &self.contents
    }
}

impl Debug for TerminalScreenDiff {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TerminalScreenDiff")
            .field("from_output_offset", &self.from_output_offset)
            .field("current", &self.current)
            .field("content_bytes", &self.contents.len())
            .finish_non_exhaustive()
    }
}

/// Invalid terminal screen update supplied by an outer parser adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalScreenDiffError {
    /// The requested baseline offset was newer than the current screen.
    OffsetReversed,
}

impl Display for TerminalScreenDiffError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OffsetReversed => {
                formatter.write_str("terminal screen diff offsets must advance monotonically")
            }
        }
    }
}

impl Error for TerminalScreenDiffError {}

/// Bounded terminal update or authoritative replacement after incompatibility.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalScreenUpdate {
    /// ANSI update that advances an existing compatible screen.
    Diff(TerminalScreenDiff),
    /// Complete replacement when a safe diff cannot be produced.
    Snapshot(TerminalScreenSnapshot),
}

impl TerminalScreenUpdate {
    /// Returns the complete current checkpoint represented by this update.
    pub const fn current(&self) -> &TerminalScreenSnapshot {
        match self {
            Self::Diff(diff) => diff.current(),
            Self::Snapshot(snapshot) => snapshot,
        }
    }
}

/// Invalid screen state returned by an outer parser adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalScreenSnapshotError {
    /// A terminal dimension was zero.
    ZeroDimensions,
    /// The cursor was outside the declared grid.
    CursorOutOfBounds,
}

impl Display for TerminalScreenSnapshotError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroDimensions => {
                formatter.write_str("terminal screen dimensions must be non-zero")
            }
            Self::CursorOutOfBounds => {
                formatter.write_str("terminal screen cursor is outside the declared grid")
            }
        }
    }
}

impl Error for TerminalScreenSnapshotError {}

/// Failure from one authorized current-screen read.
#[derive(Debug)]
pub enum ReadTerminalScreenError {
    /// No valid authentication was established for the request.
    Unauthenticated { session_id: SessionId },
    /// The authenticated principal cannot read this screen.
    PermissionDenied { session_id: SessionId },
    /// The session has no current terminal screen state.
    NotFound { session_id: SessionId },
    /// The session intentionally captures opaque output and has no screen model.
    UnavailableForCaptureMode { session_id: SessionId },
    /// Authorization or screen infrastructure failed.
    Infrastructure(PortError),
}

impl ReadTerminalScreenError {
    /// Returns the stable category used by presentation adapters.
    pub const fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::Unauthenticated { .. } => ApplicationErrorCategory::Unauthenticated,
            Self::PermissionDenied { .. } => ApplicationErrorCategory::PermissionDenied,
            Self::NotFound { .. } => ApplicationErrorCategory::NotFound,
            Self::UnavailableForCaptureMode { .. } => ApplicationErrorCategory::FailedPrecondition,
            Self::Infrastructure(error) => error.category(),
        }
    }

    /// Returns the safe target session identity.
    pub fn session_id(&self) -> Option<&SessionId> {
        match self {
            Self::Unauthenticated { session_id }
            | Self::PermissionDenied { session_id }
            | Self::NotFound { session_id }
            | Self::UnavailableForCaptureMode { session_id } => Some(session_id),
            Self::Infrastructure(error) => error.session_id(),
        }
    }
}

impl Display for ReadTerminalScreenError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unauthenticated { session_id } => write!(
                formatter,
                "authentication is required to read the terminal screen for session {session_id}"
            ),
            Self::PermissionDenied { session_id } => write!(
                formatter,
                "permission denied to read the terminal screen for session {session_id}"
            ),
            Self::NotFound { session_id } => {
                write!(
                    formatter,
                    "terminal screen for session {session_id} was not found"
                )
            }
            Self::UnavailableForCaptureMode { session_id } => write!(
                formatter,
                "terminal screen for session {session_id} is unavailable for its output capture mode"
            ),
            Self::Infrastructure(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for ReadTerminalScreenError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Infrastructure(error) => Some(error),
            Self::Unauthenticated { .. }
            | Self::PermissionDenied { .. }
            | Self::NotFound { .. }
            | Self::UnavailableForCaptureMode { .. } => None,
        }
    }
}

/// Authorizes and reads one parser-consistent current screen.
pub struct ReadTerminalScreenService<'a> {
    authorizer: &'a dyn AuthorizationPort,
    screens: &'a dyn TerminalScreenPort,
    clock: &'a dyn Clock,
    telemetry: &'a dyn Telemetry,
}

impl<'a> ReadTerminalScreenService<'a> {
    /// Constructs the terminal-screen read boundary without performing IO.
    pub const fn new(
        authorizer: &'a dyn AuthorizationPort,
        screens: &'a dyn TerminalScreenPort,
        clock: &'a dyn Clock,
        telemetry: &'a dyn Telemetry,
    ) -> Self {
        Self {
            authorizer,
            screens,
            clock,
            telemetry,
        }
    }

    /// Authorizes before reading one current screen from its adapter.
    ///
    /// Private contents appear only in the successful result. This method may
    /// cross authorization and screen-reader boundaries, never logs payloads,
    /// and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed access denial, absence, or infrastructure failure.
    pub async fn read(
        &self,
        session_id: &SessionId,
    ) -> Result<TerminalScreenSnapshot, ReadTerminalScreenError> {
        let started_at = self.clock.now();
        let result = async {
            self.authorize(session_id).await?;
            self.screens
                .snapshot(session_id)
                .await
                .map_err(|error| map_screen_error(session_id, error))?
                .ok_or_else(|| ReadTerminalScreenError::NotFound {
                    session_id: session_id.clone(),
                })
        }
        .await;
        self.finish(started_at, session_id, result.as_ref());
        result
    }

    /// Authorizes, reads one current screen, and computes a bounded update.
    ///
    /// Both input and result contain private terminal state. This method never
    /// logs payloads and delegates parser-specific work through the screen
    /// adapter port. It may cross authorization and screen boundaries and does
    /// not panic.
    ///
    /// # Errors
    ///
    /// Returns typed access denial, absence, or screen infrastructure failure.
    pub async fn diff(
        &self,
        session_id: &SessionId,
        previous: &TerminalScreenSnapshot,
    ) -> Result<TerminalScreenUpdate, ReadTerminalScreenError> {
        let started_at = self.clock.now();
        let result = async {
            self.authorize(session_id).await?;
            let current = self
                .screens
                .snapshot(session_id)
                .await
                .map_err(|error| map_screen_error(session_id, error))?
                .ok_or_else(|| ReadTerminalScreenError::NotFound {
                    session_id: session_id.clone(),
                })?;
            self.screens
                .diff(session_id, previous, &current)
                .await
                .map_err(|error| map_screen_error(session_id, error))
        }
        .await;
        self.finish_operation(
            started_at,
            session_id,
            result.as_ref().map(|_| ()),
            ApplicationOperation::ReadTerminalScreenDiff,
            DiagnosticEventName::TerminalScreenDiffRead,
            DiagnosticEventName::TerminalScreenDiffReadFailed,
        );
        result
    }

    async fn authorize(&self, session_id: &SessionId) -> Result<(), ReadTerminalScreenError> {
        let decision = self
            .authorizer
            .authorize(
                AccessAction::ReadScreen,
                &AccessResource::Session(session_id.clone()),
            )
            .await
            .map_err(ReadTerminalScreenError::Infrastructure)?;
        match decision {
            AccessDecision::Permit => Ok(()),
            AccessDecision::Deny(AccessDenialReason::Unauthenticated) => {
                Err(ReadTerminalScreenError::Unauthenticated {
                    session_id: session_id.clone(),
                })
            }
            AccessDecision::Deny(AccessDenialReason::Forbidden) => {
                Err(ReadTerminalScreenError::PermissionDenied {
                    session_id: session_id.clone(),
                })
            }
        }
    }

    fn finish(
        &self,
        started_at: undercurrent_domain::TimestampMillis,
        session_id: &SessionId,
        result: Result<&TerminalScreenSnapshot, &ReadTerminalScreenError>,
    ) {
        self.finish_operation(
            started_at,
            session_id,
            result.map(|_| ()),
            ApplicationOperation::ReadTerminalScreen,
            DiagnosticEventName::TerminalScreenRead,
            DiagnosticEventName::TerminalScreenReadFailed,
        );
    }

    fn finish_operation(
        &self,
        started_at: undercurrent_domain::TimestampMillis,
        session_id: &SessionId,
        result: Result<(), &ReadTerminalScreenError>,
        application_operation: ApplicationOperation,
        success_event: DiagnosticEventName,
        failure_event: DiagnosticEventName,
    ) {
        let finished_at = self.clock.now();
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::OperationDurationSeconds,
            value: finished_at
                .saturating_duration_since(started_at)
                .as_secs_f64(),
            operation: Some(application_operation),
        });
        let (event, level, application_error, operation, error_kind) = match result {
            Ok(_) => (success_event, DiagnosticLevel::Debug, None, None, None),
            Err(error) => {
                let category = error.category();
                let event = if category.is_access_denial() {
                    DiagnosticEventName::AuthorizationDenied
                } else {
                    failure_event
                };
                let port = match error {
                    ReadTerminalScreenError::Infrastructure(error) => Some(error),
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
            application_operation: Some(application_operation),
            application_error,
            operation,
            error_kind,
        });
    }
}

fn map_screen_error(session_id: &SessionId, error: PortError) -> ReadTerminalScreenError {
    if error.kind() == PortErrorKind::Unsupported {
        ReadTerminalScreenError::UnavailableForCaptureMode {
            session_id: session_id.clone(),
        }
    } else {
        ReadTerminalScreenError::Infrastructure(error)
    }
}
