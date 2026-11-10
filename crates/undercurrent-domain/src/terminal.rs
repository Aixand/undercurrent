// SPDX-License-Identifier: Apache-2.0

//! Bounded terminal dimensions and typed input delivery vocabulary.

use std::fmt::{Debug, Formatter};

use crate::{DomainError, DomainResult, InvalidValueReason};

/// Validated terminal dimensions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TerminalSize {
    columns: u16,
    rows: u16,
}

impl TerminalSize {
    /// Constructs dimensions within caller-supplied hard maxima.
    ///
    /// # Errors
    ///
    /// Returns a validation error for zero dimensions or a capacity error above
    /// either maximum. This function performs no IO and does not panic.
    pub fn new(columns: u16, rows: u16, max_columns: u16, max_rows: u16) -> DomainResult<Self> {
        if columns == 0 || rows == 0 || max_columns == 0 || max_rows == 0 {
            return Err(DomainError::InvalidValue {
                field: "terminal_size",
                reason: InvalidValueReason::InvalidCombination {
                    rule: "terminal dimensions and configured maxima must be non-zero",
                },
            });
        }
        if columns > max_columns {
            return Err(DomainError::CapacityExceeded {
                resource: "terminal_columns",
                limit: usize::from(max_columns),
            });
        }
        if rows > max_rows {
            return Err(DomainError::CapacityExceeded {
                resource: "terminal_rows",
                limit: usize::from(max_rows),
            });
        }
        Ok(Self { columns, rows })
    }

    /// Returns terminal columns.
    pub const fn columns(self) -> u16 {
        self.columns
    }

    /// Returns terminal rows.
    pub const fn rows(self) -> u16 {
        self.rows
    }
}

/// Keyboard modifiers used with a named key.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct KeyModifiers {
    /// Shift modifier.
    pub shift: bool,
    /// Control modifier.
    pub control: bool,
    /// Alt or Option modifier.
    pub alt: bool,
}

/// Validated function-key number in the inclusive range F1..=F12.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FunctionKey(u8);

impl FunctionKey {
    /// Validates and constructs a function-key number.
    ///
    /// # Errors
    ///
    /// Returns a validation error outside F1..=F12. This function performs no
    /// IO and does not panic.
    pub fn new(number: u8) -> DomainResult<Self> {
        if !(1..=12).contains(&number) {
            return Err(DomainError::InvalidValue {
                field: "function_key",
                reason: InvalidValueReason::OutOfRange { min: 1, max: 12 },
            });
        }
        Ok(Self(number))
    }

    /// Returns function-key number.
    pub const fn number(self) -> u8 {
        self.0
    }
}

/// Validated lowercase ASCII character used by a control chord.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ControlKey(char);

impl ControlKey {
    /// Validates and constructs an ASCII control chord character.
    ///
    /// # Errors
    ///
    /// Returns a validation error for a non-ASCII alphabetic character. This
    /// function performs no IO and does not panic.
    pub fn new(character: char) -> DomainResult<Self> {
        if !character.is_ascii_alphabetic() {
            return Err(DomainError::InvalidValue {
                field: "control_key",
                reason: InvalidValueReason::InvalidCharacter {
                    index: 0,
                    character,
                },
            });
        }
        Ok(Self(character.to_ascii_lowercase()))
    }

    /// Returns normalized lowercase ASCII character.
    pub const fn character(self) -> char {
        self.0
    }
}

/// Non-recursive named key that supports xterm modifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ModifiableKey {
    /// Up arrow.
    Up,
    /// Down arrow.
    Down,
    /// Left arrow.
    Left,
    /// Right arrow.
    Right,
    /// Home.
    Home,
    /// End.
    End,
    /// Page up.
    PageUp,
    /// Page down.
    PageDown,
    /// Validated function key.
    Function(FunctionKey),
}

/// Named terminal key resolved to bytes by a PTY adapter.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Key {
    /// Enter or Return.
    Enter,
    /// Horizontal tab.
    Tab,
    /// Reverse tab.
    BackTab,
    /// Escape.
    Escape,
    /// Backspace.
    Backspace,
    /// Forward delete.
    Delete,
    /// Space.
    Space,
    /// Up arrow.
    Up,
    /// Down arrow.
    Down,
    /// Left arrow.
    Left,
    /// Right arrow.
    Right,
    /// Home.
    Home,
    /// End.
    End,
    /// Page up.
    PageUp,
    /// Page down.
    PageDown,
    /// Function key in the inclusive range F1..=F12.
    Function(FunctionKey),
    /// Control chord with an ASCII alphabetic character.
    Control(ControlKey),
    /// A named base key with explicit modifiers.
    Modified {
        /// Base key before modifiers are applied.
        base: ModifiableKey,
        /// Active modifiers.
        modifiers: KeyModifiers,
    },
}

impl Key {
    /// Validates key-specific invariants and constructs a function key.
    ///
    /// # Errors
    ///
    /// Returns a validation error outside F1..=F12. This function performs no
    /// IO and does not panic.
    pub fn function(number: u8) -> DomainResult<Self> {
        FunctionKey::new(number).map(Self::Function)
    }

    /// Validates and constructs an ASCII control chord.
    ///
    /// # Errors
    ///
    /// Returns a validation error for a non-ASCII alphabetic character. This
    /// function performs no IO and does not panic.
    pub fn control(character: char) -> DomainResult<Self> {
        ControlKey::new(character).map(Self::Control)
    }
}

/// Mouse button reported through an SGR-compatible terminal sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MouseButton {
    /// Left button.
    Left,
    /// Middle button.
    Middle,
    /// Right button.
    Right,
    /// Scroll wheel up.
    ScrollUp,
    /// Scroll wheel down.
    ScrollDown,
}

/// Mouse event kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MouseEventKind {
    /// Button press.
    Press,
    /// Button release.
    Release,
    /// Pointer movement.
    Move,
}

/// Validated one-based terminal mouse event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MouseEvent {
    column: u16,
    row: u16,
    button: MouseButton,
    kind: MouseEventKind,
}

impl MouseEvent {
    /// Constructs an event within the current terminal bounds.
    ///
    /// # Errors
    ///
    /// Returns a validation error for zero or out-of-bounds coordinates. This
    /// function performs no IO and does not panic.
    pub fn new(
        column: u16,
        row: u16,
        button: MouseButton,
        kind: MouseEventKind,
        terminal: TerminalSize,
    ) -> DomainResult<Self> {
        if column == 0 || row == 0 || column > terminal.columns || row > terminal.rows {
            return Err(DomainError::InvalidValue {
                field: "mouse_coordinates",
                reason: InvalidValueReason::InvalidCombination {
                    rule: "mouse coordinates must be one-based and within terminal bounds",
                },
            });
        }
        Ok(Self {
            column,
            row,
            button,
            kind,
        })
    }

    /// Returns one-based terminal column.
    pub const fn column(self) -> u16 {
        self.column
    }

    /// Returns one-based terminal row.
    pub const fn row(self) -> u16 {
        self.row
    }

    /// Returns mouse button.
    pub const fn button(self) -> MouseButton {
        self.button
    }

    /// Returns event kind.
    pub const fn kind(self) -> MouseEventKind {
        self.kind
    }
}

/// One typed item in an atomic terminal input sequence.
#[derive(Clone, PartialEq, Eq)]
pub enum InputItem {
    /// UTF-8 prompt text encoded using the effective profile.
    AutoText(String),
    /// Literal UTF-8 text.
    Text(String),
    /// Named terminal key.
    Key(Key),
    /// Raw bytes for protocols not representable by named keys.
    Bytes(Vec<u8>),
    /// UTF-8 text delivered using terminal bracketed-paste framing.
    Paste(String),
    /// SGR-compatible mouse event.
    Mouse(MouseEvent),
}

impl InputItem {
    fn encoded_len_upper_bound(&self) -> Option<usize> {
        match self {
            Self::Text(value) => Some(value.len()),
            Self::Bytes(value) => Some(value.len()),
            // AutoText's profile is chosen at delivery, after admission. Reserve
            // both six-byte paste delimiters even if that profile uses raw text.
            // See [input byte estimates](../docs/reference/values-and-io.md#terminal-input-and-resize).
            Self::Paste(value) | Self::AutoText(value) => value.len().checked_add(12),
            Self::Key(_) => Some(16),
            Self::Mouse(_) => Some(18),
        }
    }
}

impl Debug for InputItem {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AutoText(value) => formatter
                .debug_struct("AutoText")
                .field("byte_len", &value.len())
                .finish(),
            Self::Text(value) => formatter
                .debug_struct("Text")
                .field("byte_len", &value.len())
                .finish(),
            Self::Key(key) => formatter.debug_tuple("Key").field(key).finish(),
            Self::Bytes(value) => formatter
                .debug_struct("Bytes")
                .field("byte_len", &value.len())
                .finish(),
            Self::Paste(value) => formatter
                .debug_struct("Paste")
                .field("byte_len", &value.len())
                .finish(),
            Self::Mouse(event) => formatter.debug_tuple("Mouse").field(event).finish(),
        }
    }
}

/// Validated non-empty input sequence written atomically per session.
#[derive(Clone, PartialEq, Eq)]
pub struct InputSequence {
    items: Vec<InputItem>,
    payload_bytes: usize,
}

impl InputSequence {
    /// Constructs a sequence within caller-supplied item and byte limits.
    ///
    /// The estimated payload includes a conservative allowance for encoded
    /// named keys and mouse events. This function performs no IO and does not
    /// panic.
    ///
    /// # Errors
    ///
    /// Returns a validation error for empty input or a capacity error when a
    /// configured limit is exceeded.
    pub fn new(
        items: Vec<InputItem>,
        max_items: usize,
        max_payload_bytes: usize,
    ) -> DomainResult<Self> {
        if max_items == 0 || max_payload_bytes == 0 {
            return Err(DomainError::InvalidValue {
                field: "input_limits",
                reason: InvalidValueReason::InvalidCombination {
                    rule: "input item and byte limits must be non-zero",
                },
            });
        }
        if items.is_empty() {
            return Err(DomainError::InvalidValue {
                field: "input_items",
                reason: InvalidValueReason::Empty,
            });
        }
        if items.len() > max_items {
            return Err(DomainError::CapacityExceeded {
                resource: "input_items",
                limit: max_items,
            });
        }
        let payload_bytes = items.iter().try_fold(0usize, |total, item| {
            item.encoded_len_upper_bound()
                .and_then(|item_bytes| total.checked_add(item_bytes))
                .ok_or(DomainError::CapacityExceeded {
                    resource: "input_payload_bytes",
                    limit: max_payload_bytes,
                })
        })?;
        if payload_bytes > max_payload_bytes {
            return Err(DomainError::CapacityExceeded {
                resource: "input_payload_bytes",
                limit: max_payload_bytes,
            });
        }
        Ok(Self {
            items,
            payload_bytes,
        })
    }

    /// Returns the typed items for an authorized input adapter.
    pub fn items(&self) -> &[InputItem] {
        &self.items
    }

    /// Returns the estimated encoded payload size used for admission control.
    pub const fn payload_bytes(&self) -> usize {
        self.payload_bytes
    }

    /// Revalidates terminal-relative items against the current terminal size.
    ///
    /// This check is intended to run under session-local mutation ownership so
    /// a resize cannot race mouse-coordinate validation. Non-mouse items are
    /// unaffected. The function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a validation error when a mouse coordinate is outside the
    /// supplied current terminal dimensions.
    pub fn validate_for_terminal(&self, terminal: TerminalSize) -> DomainResult<()> {
        for item in &self.items {
            if let InputItem::Mouse(event) = item {
                MouseEvent::new(
                    event.column(),
                    event.row(),
                    event.button(),
                    event.kind(),
                    terminal,
                )?;
            }
        }
        Ok(())
    }
}

impl Debug for InputSequence {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InputSequence")
            .field("item_count", &self.items.len())
            .field("payload_bytes", &self.payload_bytes)
            .finish_non_exhaustive()
    }
}

/// How delivered input affects best-effort foreground observation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum InputObservationPolicy {
    /// Mark execution active and clear the consumed interaction request.
    #[default]
    ForceActive,
    /// Preserve committed foreground state for direct human interaction.
    Preserve,
}
