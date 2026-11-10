// SPDX-License-Identifier: Apache-2.0

//! Compact binary terminal-output stream payloads.
//!
//! These product items are carried inside transport frames. Output and screen
//! headers occupy 17 and 26 bytes respectively, in addition to the configured
//! raw-data limit. Use [`TerminalOutputCodec::decode_item`] for streams that can
//! begin with a screen; [`TerminalOutputCodec::decode`] accepts output only.

use std::sync::Arc;

use crate::{ProductCodecError, ProductCodecErrorKind, ProductCodecStage};

const MAGIC: [u8; 4] = *b"UCTO";
const DATA_KIND: u8 = 1;
const SCREEN_KIND: u8 = 2;
const HEADER_BYTES: usize = 4 + 1 + 8 + 4;
const SCREEN_HEADER_BYTES: usize = 4 + 1 + 8 + 2 + 2 + 2 + 2 + 1 + 4;
const CURSOR_VISIBLE_FLAG: u8 = 1 << 0;
const ALTERNATE_SCREEN_FLAG: u8 = 1 << 1;
const SCREEN_FLAGS: u8 = CURSOR_VISIBLE_FLAG | ALTERNATE_SCREEN_FLAG;

/// One retained or live terminal byte interval.
#[derive(Clone, PartialEq, Eq)]
pub struct TerminalOutputChunk {
    start_offset: u64,
    bytes: Arc<[u8]>,
}

impl TerminalOutputChunk {
    /// Constructs a terminal output interval.
    ///
    /// This function retains the supplied bounded byte allocation, performs no
    /// IO, cannot fail, and does not panic.
    ///
    /// It does not enforce a byte limit or validate offset addition. Encoding
    /// performs those checks; [`Self::end_offset`] returns `None` on overflow.
    pub const fn new(start_offset: u64, bytes: Arc<[u8]>) -> Self {
        Self {
            start_offset,
            bytes,
        }
    }

    /// Returns the inclusive start byte offset.
    pub const fn start_offset(&self) -> u64 {
        self.start_offset
    }

    /// Returns the exclusive end byte offset when representable.
    pub fn end_offset(&self) -> Option<u64> {
        u64::try_from(self.bytes.len())
            .ok()
            .and_then(|length| self.start_offset.checked_add(length))
    }

    /// Returns the raw terminal bytes without base64 expansion.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl std::fmt::Debug for TerminalOutputChunk {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TerminalOutputChunk")
            .field("start_offset", &self.start_offset)
            .field("byte_count", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

/// Complete current terminal state used to seed an output stream.
#[derive(Clone, PartialEq, Eq)]
pub struct TerminalScreenSnapshot {
    output_offset: u64,
    columns: u16,
    rows: u16,
    cursor_row: u16,
    cursor_column: u16,
    cursor_visible: bool,
    alternate_screen: bool,
    contents: Arc<[u8]>,
}

impl TerminalScreenSnapshot {
    /// Constructs one binary screen seed after validating its grid geometry.
    ///
    /// Construction retains the bounded ANSI allocation, performs no IO, and
    /// does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed geometry error for zero dimensions or an out-of-grid
    /// cursor.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        output_offset: u64,
        columns: u16,
        rows: u16,
        cursor_row: u16,
        cursor_column: u16,
        cursor_visible: bool,
        alternate_screen: bool,
        contents: Arc<[u8]>,
    ) -> Result<Self, InvalidTerminalScreenSnapshot> {
        if columns == 0 || rows == 0 {
            return Err(InvalidTerminalScreenSnapshot::ZeroDimensions);
        }
        if cursor_row >= rows || cursor_column >= columns {
            return Err(InvalidTerminalScreenSnapshot::CursorOutOfBounds);
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

    /// Returns the exclusive raw-output checkpoint represented by the screen.
    pub const fn output_offset(&self) -> u64 {
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

    /// Returns whether the alternate screen is active.
    pub const fn alternate_screen(&self) -> bool {
        self.alternate_screen
    }

    /// Returns the complete ANSI screen rendering.
    pub fn contents(&self) -> &[u8] {
        &self.contents
    }
}

impl std::fmt::Debug for TerminalScreenSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

/// Invalid geometry for one terminal screen seed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidTerminalScreenSnapshot {
    /// A terminal dimension was zero.
    ZeroDimensions,
    /// The cursor was outside the declared grid.
    CursorOutOfBounds,
}

impl std::fmt::Display for InvalidTerminalScreenSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

impl std::error::Error for InvalidTerminalScreenSnapshot {}

/// One tagged compact binary item on a selected terminal stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalStreamItem {
    /// Raw terminal output occupying one offset interval.
    Output(TerminalOutputChunk),
    /// Complete terminal state at an exact output checkpoint.
    Screen(TerminalScreenSnapshot),
}

/// Binary terminal-output codec with a hard raw-byte bound.
#[derive(Debug, Clone, Copy)]
pub struct TerminalOutputCodec {
    max_data_bytes: usize,
}

impl TerminalOutputCodec {
    /// Constructs a non-zero raw terminal byte limit.
    ///
    /// # Errors
    ///
    /// Returns an invalid codec-limit error when `max_data_bytes` is zero or
    /// cannot be represented by the binary length field.
    pub fn new(max_data_bytes: usize) -> Result<Self, InvalidTerminalOutputLimit> {
        if max_data_bytes == 0 || u32::try_from(max_data_bytes).is_err() {
            return Err(InvalidTerminalOutputLimit { max_data_bytes });
        }
        Ok(Self { max_data_bytes })
    }

    /// Returns the maximum raw terminal bytes per stream item.
    pub const fn max_data_bytes(self) -> usize {
        self.max_data_bytes
    }

    /// Returns exact binary overhead before raw terminal bytes.
    pub const fn header_bytes() -> usize {
        HEADER_BYTES
    }

    /// Returns exact binary overhead before screen rendering bytes.
    pub const fn screen_header_bytes() -> usize {
        SCREEN_HEADER_BYTES
    }

    /// Encodes one terminal interval without JSON or base64 conversion.
    ///
    /// # Errors
    ///
    /// Returns a redacted capacity or offset-exhaustion failure.
    pub fn encode(&self, chunk: &TerminalOutputChunk) -> Result<Vec<u8>, ProductCodecError> {
        self.encode_output(chunk)
    }

    /// Encodes one tagged terminal output or screen item.
    ///
    /// # Errors
    ///
    /// Returns a redacted capacity or offset-exhaustion failure.
    pub fn encode_item(&self, item: &TerminalStreamItem) -> Result<Vec<u8>, ProductCodecError> {
        match item {
            TerminalStreamItem::Output(chunk) => self.encode_output(chunk),
            TerminalStreamItem::Screen(snapshot) => self.encode_screen(snapshot),
        }
    }

    fn encode_output(&self, chunk: &TerminalOutputChunk) -> Result<Vec<u8>, ProductCodecError> {
        self.encode_output_parts(
            chunk.start_offset,
            chunk.bytes.len(),
            std::iter::once(chunk.bytes()),
        )
    }

    /// Encodes one terminal interval directly from ordered borrowed byte parts.
    ///
    /// This is an implementation seam for zero-copy retained-output adapters.
    /// The declared length must equal the checked sum of every part.
    ///
    /// # Errors
    ///
    /// Returns the same redacted capacity, length, or offset-exhaustion failure
    /// as encoding one contiguous [`TerminalOutputChunk`].
    #[doc(hidden)]
    pub fn encode_output_parts<'a>(
        &self,
        start_offset: u64,
        declared_len: usize,
        parts: impl IntoIterator<Item = &'a [u8]>,
    ) -> Result<Vec<u8>, ProductCodecError> {
        ensure_data_size(declared_len, self.max_data_bytes)?;
        let length = u32::try_from(declared_len).map_err(|_| {
            ProductCodecError::new(
                ProductCodecStage::TerminalOutput,
                ProductCodecErrorKind::capacity(declared_len, self.max_data_bytes),
            )
        })?;
        if start_offset.checked_add(u64::from(length)).is_none() {
            return Err(ProductCodecError::new(
                ProductCodecStage::TerminalOutput,
                ProductCodecErrorKind::TerminalLengthMismatch {
                    declared: declared_len,
                    observed: declared_len,
                },
            ));
        }
        let capacity = HEADER_BYTES.checked_add(declared_len).ok_or_else(|| {
            ProductCodecError::new(
                ProductCodecStage::TerminalOutput,
                ProductCodecErrorKind::capacity(usize::MAX, self.max_data_bytes),
            )
        })?;
        let mut encoded = Vec::with_capacity(capacity);
        encoded.extend_from_slice(&MAGIC);
        encoded.push(DATA_KIND);
        encoded.extend_from_slice(&start_offset.to_be_bytes());
        encoded.extend_from_slice(&length.to_be_bytes());
        let mut observed = 0usize;
        for part in parts {
            // The declared length does not bound the iterator. Check each part
            // before appending so an inconsistent source cannot grow the encoded
            // buffer past the [validated payload size](../../../docs/reference/protocol/streams.md#allocation-and-integrity-boundaries).
            observed = observed.checked_add(part.len()).ok_or_else(|| {
                ProductCodecError::new(
                    ProductCodecStage::TerminalOutput,
                    ProductCodecErrorKind::capacity(usize::MAX, self.max_data_bytes),
                )
            })?;
            ensure_data_size(observed, self.max_data_bytes)?;
            if observed > declared_len {
                return Err(malformed_length(declared_len, observed));
            }
            encoded.extend_from_slice(part);
        }
        if observed != declared_len {
            return Err(malformed_length(declared_len, observed));
        }
        Ok(encoded)
    }

    fn encode_screen(
        &self,
        snapshot: &TerminalScreenSnapshot,
    ) -> Result<Vec<u8>, ProductCodecError> {
        ensure_data_size(snapshot.contents.len(), self.max_data_bytes)?;
        let length = u32::try_from(snapshot.contents.len()).map_err(|_| {
            ProductCodecError::new(
                ProductCodecStage::TerminalOutput,
                ProductCodecErrorKind::capacity(snapshot.contents.len(), self.max_data_bytes),
            )
        })?;
        let mut flags = 0;
        if snapshot.cursor_visible {
            flags |= CURSOR_VISIBLE_FLAG;
        }
        if snapshot.alternate_screen {
            flags |= ALTERNATE_SCREEN_FLAG;
        }
        let mut encoded = Vec::with_capacity(SCREEN_HEADER_BYTES + snapshot.contents.len());
        encoded.extend_from_slice(&MAGIC);
        encoded.push(SCREEN_KIND);
        // Rendering bytes describe state at this checkpoint; they do not occupy
        // a raw-output interval or advance the offset by their own length.
        // See [screen seeding](../../../docs/reference/protocol/streams.md#seed-a-live-terminal-view).
        encoded.extend_from_slice(&snapshot.output_offset.to_be_bytes());
        encoded.extend_from_slice(&snapshot.columns.to_be_bytes());
        encoded.extend_from_slice(&snapshot.rows.to_be_bytes());
        encoded.extend_from_slice(&snapshot.cursor_row.to_be_bytes());
        encoded.extend_from_slice(&snapshot.cursor_column.to_be_bytes());
        encoded.push(flags);
        encoded.extend_from_slice(&length.to_be_bytes());
        encoded.extend_from_slice(&snapshot.contents);
        Ok(encoded)
    }

    /// Decodes exactly one bounded terminal interval.
    ///
    /// # Errors
    ///
    /// Returns a redacted malformed, unsupported-kind, capacity, or offset
    /// exhaustion failure.
    pub fn decode(&self, bytes: &[u8]) -> Result<TerminalOutputChunk, ProductCodecError> {
        match self.decode_item(bytes)? {
            TerminalStreamItem::Output(chunk) => Ok(chunk),
            TerminalStreamItem::Screen(_) => Err(unsupported_kind(SCREEN_KIND)),
        }
    }

    /// Decodes exactly one bounded tagged terminal item.
    ///
    /// # Errors
    ///
    /// Returns a redacted malformed, unsupported-kind, capacity, geometry, or
    /// offset-exhaustion failure.
    pub fn decode_item(&self, bytes: &[u8]) -> Result<TerminalStreamItem, ProductCodecError> {
        if bytes.len() < 5 || bytes[..4] != MAGIC {
            return Err(malformed_length(5, bytes.len()));
        }
        match bytes[4] {
            DATA_KIND => self.decode_output(bytes).map(TerminalStreamItem::Output),
            SCREEN_KIND => self.decode_screen(bytes).map(TerminalStreamItem::Screen),
            code => Err(unsupported_kind(code)),
        }
    }

    fn decode_output(&self, bytes: &[u8]) -> Result<TerminalOutputChunk, ProductCodecError> {
        if bytes.len() < HEADER_BYTES {
            return Err(malformed_length(HEADER_BYTES, bytes.len()));
        }
        let start_offset = u64::from_be_bytes(
            bytes[5..13]
                .try_into()
                .map_err(|_| malformed_length(HEADER_BYTES, bytes.len()))?,
        );
        let declared = u32::from_be_bytes(
            bytes[13..17]
                .try_into()
                .map_err(|_| malformed_length(HEADER_BYTES, bytes.len()))?,
        ) as usize;
        ensure_data_size(declared, self.max_data_bytes)?;
        let observed = bytes.len() - HEADER_BYTES;
        if observed != declared {
            return Err(malformed_length(declared, observed));
        }
        if start_offset
            .checked_add(u64::try_from(declared).unwrap_or(u64::MAX))
            .is_none()
        {
            return Err(malformed_length(declared, observed));
        }
        Ok(TerminalOutputChunk::new(
            start_offset,
            Arc::from(&bytes[HEADER_BYTES..]),
        ))
    }

    fn decode_screen(&self, bytes: &[u8]) -> Result<TerminalScreenSnapshot, ProductCodecError> {
        if bytes.len() < SCREEN_HEADER_BYTES {
            return Err(malformed_length(SCREEN_HEADER_BYTES, bytes.len()));
        }
        let output_offset = read_u64(bytes, 5)?;
        let columns = read_u16(bytes, 13)?;
        let rows = read_u16(bytes, 15)?;
        let cursor_row = read_u16(bytes, 17)?;
        let cursor_column = read_u16(bytes, 19)?;
        let flags = bytes[21];
        if flags & !SCREEN_FLAGS != 0 {
            return Err(invalid_screen());
        }
        let declared = u32::from_be_bytes(
            bytes[22..26]
                .try_into()
                .map_err(|_| malformed_length(SCREEN_HEADER_BYTES, bytes.len()))?,
        ) as usize;
        ensure_data_size(declared, self.max_data_bytes)?;
        let observed = bytes.len() - SCREEN_HEADER_BYTES;
        if declared != observed {
            return Err(malformed_length(declared, observed));
        }
        TerminalScreenSnapshot::new(
            output_offset,
            columns,
            rows,
            cursor_row,
            cursor_column,
            flags & CURSOR_VISIBLE_FLAG != 0,
            flags & ALTERNATE_SCREEN_FLAG != 0,
            Arc::from(&bytes[SCREEN_HEADER_BYTES..]),
        )
        .map_err(|_| invalid_screen())
    }
}

/// Invalid raw terminal output item limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidTerminalOutputLimit {
    max_data_bytes: usize,
}

impl InvalidTerminalOutputLimit {
    /// Returns the rejected value.
    pub const fn value(self) -> usize {
        self.max_data_bytes
    }
}

impl std::fmt::Display for InvalidTerminalOutputLimit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("terminal output item limit must fit a non-zero 32-bit length")
    }
}

impl std::error::Error for InvalidTerminalOutputLimit {}

fn ensure_data_size(observed: usize, limit: usize) -> Result<(), ProductCodecError> {
    if observed > limit {
        return Err(ProductCodecError::new(
            ProductCodecStage::TerminalOutput,
            ProductCodecErrorKind::capacity(observed, limit),
        ));
    }
    Ok(())
}

fn malformed_length(declared: usize, observed: usize) -> ProductCodecError {
    ProductCodecError::new(
        ProductCodecStage::TerminalOutput,
        ProductCodecErrorKind::TerminalLengthMismatch { declared, observed },
    )
}

fn unsupported_kind(code: u8) -> ProductCodecError {
    ProductCodecError::new(
        ProductCodecStage::TerminalOutput,
        ProductCodecErrorKind::UnsupportedTerminalKind { code },
    )
}

fn invalid_screen() -> ProductCodecError {
    ProductCodecError::new(
        ProductCodecStage::TerminalOutput,
        ProductCodecErrorKind::InvalidTerminalScreen,
    )
}

fn read_u64(bytes: &[u8], start: usize) -> Result<u64, ProductCodecError> {
    bytes[start..start + 8]
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| malformed_length(start + 8, bytes.len()))
}

fn read_u16(bytes: &[u8], start: usize) -> Result<u16, ProductCodecError> {
    bytes[start..start + 2]
        .try_into()
        .map(u16::from_be_bytes)
        .map_err(|_| malformed_length(start + 2, bytes.len()))
}
