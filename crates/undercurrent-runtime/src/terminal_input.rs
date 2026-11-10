// SPDX-License-Identifier: Apache-2.0

//! Canonical xterm encoding for validated terminal input.

use std::error::Error;
use std::fmt::{Debug, Display, Formatter, Write};
use std::time::Duration;

use undercurrent_domain::{
    FunctionKey, InputDeliveryPolicy, InputItem, InputSequence, Key, KeyModifiers, ModifiableKey,
    MouseButton, MouseEvent, MouseEventKind,
};

const PASTE_START: &[u8] = b"\x1b[200~";
const PASTE_END: &[u8] = b"\x1b[201~";

/// Payload-safe failure while encoding already validated terminal input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalInputEncodingError {
    /// The admitted output buffer could not be reserved.
    AllocationFailed {
        /// Bytes admitted by the domain and requested from the allocator.
        requested_bytes: usize,
    },
    /// Concrete encoding exceeded the domain's conservative admitted bound.
    BoundViolated {
        /// Domain-admitted encoded byte upper bound.
        admitted_bytes: usize,
        /// Concrete encoded byte count.
        encoded_bytes: usize,
    },
    /// Formatting into the preallocated byte buffer unexpectedly failed.
    FormattingFailed,
    /// Adapter-supplied pacing metadata did not name an internal byte boundary.
    InvalidPacingBoundary {
        /// Requested split offset.
        offset: usize,
        /// Complete encoded byte length.
        encoded_bytes: usize,
    },
    /// Adapter-supplied pacing delay was zero or not millisecond-aligned.
    InvalidPacingDelay,
}

impl Display for TerminalInputEncodingError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AllocationFailed { requested_bytes } => write!(
                formatter,
                "could not reserve {requested_bytes} bytes for terminal input"
            ),
            Self::BoundViolated {
                admitted_bytes,
                encoded_bytes,
            } => write!(
                formatter,
                "terminal input encoded to {encoded_bytes} bytes above admitted bound {admitted_bytes}"
            ),
            Self::FormattingFailed => {
                formatter.write_str("could not format terminal input into its admitted buffer")
            }
            Self::InvalidPacingBoundary {
                offset,
                encoded_bytes,
            } => write!(
                formatter,
                "terminal input pacing offset {offset} is invalid for {encoded_bytes} encoded bytes"
            ),
            Self::InvalidPacingDelay => {
                formatter.write_str("terminal input pacing delay is invalid")
            }
        }
    }
}

impl Error for TerminalInputEncodingError {}

/// One intentional pause inside an otherwise atomic encoded input delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncodedInputPause {
    offset: usize,
    delay: Duration,
}

impl EncodedInputPause {
    /// Constructs unchecked host-decoded pacing metadata for validation by the
    /// enclosing delivery. This function performs no IO and does not panic.
    pub(crate) const fn new(offset: usize, delay: Duration) -> Self {
        Self { offset, delay }
    }

    /// Returns the byte offset written and flushed before the pause.
    pub const fn offset(self) -> usize {
        self.offset
    }

    /// Returns the bounded pause duration.
    pub const fn delay(self) -> Duration {
        self.delay
    }
}

/// Encoded bytes and optional internal pacing retained as one atomic request.
#[derive(Clone, PartialEq, Eq)]
pub struct EncodedTerminalInput {
    bytes: Vec<u8>,
    pause: Option<EncodedInputPause>,
}

impl Debug for EncodedTerminalInput {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EncodedTerminalInput")
            .field("bytes", &self.bytes.len())
            .field("pause", &self.pause)
            .finish()
    }
}

impl EncodedTerminalInput {
    /// Constructs an encoded delivery while validating an optional internal
    /// pacing boundary. Payload bytes are retained privately and no IO occurs.
    ///
    /// # Errors
    ///
    /// Returns a payload-safe error for an edge/out-of-bounds offset, zero
    /// delay, or delay that is not aligned to whole milliseconds.
    pub(crate) fn from_parts(
        bytes: Vec<u8>,
        pause: Option<EncodedInputPause>,
    ) -> Result<Self, TerminalInputEncodingError> {
        if let Some(pause) = pause {
            if pause.offset == 0 || pause.offset >= bytes.len() {
                return Err(TerminalInputEncodingError::InvalidPacingBoundary {
                    offset: pause.offset,
                    encoded_bytes: bytes.len(),
                });
            }
            if pause.delay.is_zero() || !pause.delay.subsec_nanos().is_multiple_of(1_000_000) {
                return Err(TerminalInputEncodingError::InvalidPacingDelay);
            }
        }
        Ok(Self { bytes, pause })
    }

    /// Returns the complete payload without exposing it through debug output.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Consumes the delivery into its complete payload.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Returns the optional internal write boundary and delay.
    pub const fn pause(&self) -> Option<EncodedInputPause> {
        self.pause
    }
}

/// Encodes one complete validated input sequence into canonical xterm bytes.
///
/// Items retain caller order and are returned in one contiguous buffer so a
/// process adapter can serialize the entire sequence as one atomic delivery.
/// This function performs no IO and does not expose text, paste, or raw byte
/// content in diagnostics.
///
/// # Errors
///
/// Returns a payload-safe allocation, formatting, or domain/adapter bound
/// mismatch error. It does not panic.
pub fn encode_terminal_input(input: &InputSequence) -> Result<Vec<u8>, TerminalInputEncodingError> {
    encode_terminal_input_delivery(input, InputDeliveryPolicy::immediate())
        .map(EncodedTerminalInput::into_bytes)
}

/// Encodes one validated sequence with profile-owned trailing-submit pacing.
///
/// Pacing is applied only when the final typed item is Enter and the preceding
/// items encode at least one byte. The returned value remains one atomic
/// delivery so process adapters can prevent input interleaving across the
/// internal pause. This function performs no IO and does not panic.
///
/// # Errors
///
/// Returns a payload-safe allocation, formatting, or invariant failure.
pub fn encode_terminal_input_delivery(
    input: &InputSequence,
    policy: InputDeliveryPolicy,
) -> Result<EncodedTerminalInput, TerminalInputEncodingError> {
    let admitted_bytes = input.payload_bytes();
    let mut encoded = BoundedInputBuffer::new(admitted_bytes)?;
    let paced_item = policy.trailing_submit_delay().and_then(|delay| {
        input
            .items()
            .last()
            .is_some_and(|item| matches!(item, InputItem::Key(Key::Enter)))
            .then_some((input.items().len().saturating_sub(1), delay))
    });
    let mut pause = None;

    for (index, item) in input.items().iter().enumerate() {
        if let Some((paced_index, delay)) = paced_item
            && index == paced_index
            && !encoded.is_empty()
        {
            pause = Some(EncodedInputPause {
                offset: encoded.len(),
                delay,
            });
        }
        match item {
            InputItem::AutoText(value) => {
                let paste = policy.text_mode() == undercurrent_domain::InputTextMode::Paste;
                if paste {
                    encoded.extend(PASTE_START)?;
                }
                encoded.extend(value.as_bytes())?;
                if paste {
                    encoded.extend(PASTE_END)?;
                }
            }
            InputItem::Text(value) => encoded.extend(value.as_bytes())?,
            InputItem::Bytes(value) => encoded.extend(value)?,
            InputItem::Key(key) => encode_key(&mut encoded, key)?,
            InputItem::Paste(value) => {
                encoded.extend(PASTE_START)?;
                encoded.extend(value.as_bytes())?;
                encoded.extend(PASTE_END)?;
            }
            InputItem::Mouse(event) => encode_mouse(&mut encoded, *event)?,
        }
    }

    EncodedTerminalInput::from_parts(encoded.into_bytes(), pause)
}

struct BoundedInputBuffer {
    bytes: Vec<u8>,
    admitted_bytes: usize,
    violation: Option<usize>,
}

impl BoundedInputBuffer {
    fn new(admitted_bytes: usize) -> Result<Self, TerminalInputEncodingError> {
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(admitted_bytes).map_err(|_| {
            TerminalInputEncodingError::AllocationFailed {
                requested_bytes: admitted_bytes,
            }
        })?;
        Ok(Self {
            bytes,
            admitted_bytes,
            violation: None,
        })
    }

    fn extend(&mut self, item: &[u8]) -> Result<(), TerminalInputEncodingError> {
        let encoded_bytes = self.bytes.len().saturating_add(item.len());
        if encoded_bytes > self.admitted_bytes {
            self.violation = Some(encoded_bytes);
            return Err(self.failure());
        }
        self.bytes.extend_from_slice(item);
        Ok(())
    }

    fn push(&mut self, byte: u8) -> Result<(), TerminalInputEncodingError> {
        self.extend(std::slice::from_ref(&byte))
    }

    fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    fn len(&self) -> usize {
        self.bytes.len()
    }

    fn failure(&self) -> TerminalInputEncodingError {
        self.violation.map_or(
            TerminalInputEncodingError::FormattingFailed,
            |encoded_bytes| TerminalInputEncodingError::BoundViolated {
                admitted_bytes: self.admitted_bytes,
                encoded_bytes,
            },
        )
    }

    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for BoundedInputBuffer {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.extend(value.as_bytes()).map_err(|_| std::fmt::Error)
    }
}

fn encode_key(
    encoded: &mut BoundedInputBuffer,
    key: &Key,
) -> Result<(), TerminalInputEncodingError> {
    match key {
        Key::Enter => encoded.push(b'\r')?,
        Key::Tab => encoded.push(b'\t')?,
        Key::BackTab => encoded.extend(b"\x1b[Z")?,
        Key::Escape => encoded.push(0x1b)?,
        Key::Backspace => encoded.push(0x7f)?,
        Key::Delete => encoded.extend(b"\x1b[3~")?,
        Key::Space => encoded.push(b' ')?,
        Key::Up => encoded.extend(b"\x1b[A")?,
        Key::Down => encoded.extend(b"\x1b[B")?,
        Key::Left => encoded.extend(b"\x1b[D")?,
        Key::Right => encoded.extend(b"\x1b[C")?,
        Key::Home => encoded.extend(b"\x1b[H")?,
        Key::End => encoded.extend(b"\x1b[F")?,
        Key::PageUp => encoded.extend(b"\x1b[5~")?,
        Key::PageDown => encoded.extend(b"\x1b[6~")?,
        Key::Function(function) => encode_function(encoded, *function)?,
        Key::Control(control) => encoded.push((control.character() as u8) & 0x1f)?,
        Key::Modified { base, modifiers } => encode_modified(encoded, *base, *modifiers)?,
    }
    Ok(())
}

fn encode_function(
    encoded: &mut BoundedInputBuffer,
    function: FunctionKey,
) -> Result<(), TerminalInputEncodingError> {
    match function.number() {
        1 => encoded.extend(b"\x1bOP")?,
        2 => encoded.extend(b"\x1bOQ")?,
        3 => encoded.extend(b"\x1bOR")?,
        4 => encoded.extend(b"\x1bOS")?,
        5 => encoded.extend(b"\x1b[15~")?,
        6 => encoded.extend(b"\x1b[17~")?,
        7 => encoded.extend(b"\x1b[18~")?,
        8 => encoded.extend(b"\x1b[19~")?,
        9 => encoded.extend(b"\x1b[20~")?,
        10 => encoded.extend(b"\x1b[21~")?,
        11 => encoded.extend(b"\x1b[23~")?,
        12 => encoded.extend(b"\x1b[24~")?,
        _ => return Err(TerminalInputEncodingError::FormattingFailed),
    }
    Ok(())
}

fn encode_modified(
    encoded: &mut BoundedInputBuffer,
    base: ModifiableKey,
    modifiers: KeyModifiers,
) -> Result<(), TerminalInputEncodingError> {
    let parameter = 1
        + u8::from(modifiers.shift)
        + 2 * u8::from(modifiers.alt)
        + 4 * u8::from(modifiers.control);
    if parameter == 1 {
        return encode_key(encoded, &plain_key(base));
    }

    if let Some(final_byte) = csi_final_byte(base) {
        write!(encoded, "\x1b[1;{parameter}{final_byte}").map_err(|_| encoded.failure())?;
        return Ok(());
    }
    if let Some(code) = csi_tilde_code(base) {
        write!(encoded, "\x1b[{code};{parameter}~").map_err(|_| encoded.failure())?;
        return Ok(());
    }
    Err(TerminalInputEncodingError::FormattingFailed)
}

const fn plain_key(base: ModifiableKey) -> Key {
    match base {
        ModifiableKey::Up => Key::Up,
        ModifiableKey::Down => Key::Down,
        ModifiableKey::Left => Key::Left,
        ModifiableKey::Right => Key::Right,
        ModifiableKey::Home => Key::Home,
        ModifiableKey::End => Key::End,
        ModifiableKey::PageUp => Key::PageUp,
        ModifiableKey::PageDown => Key::PageDown,
        ModifiableKey::Function(function) => Key::Function(function),
    }
}

const fn csi_final_byte(base: ModifiableKey) -> Option<char> {
    match base {
        ModifiableKey::Up => Some('A'),
        ModifiableKey::Down => Some('B'),
        ModifiableKey::Right => Some('C'),
        ModifiableKey::Left => Some('D'),
        ModifiableKey::Home => Some('H'),
        ModifiableKey::End => Some('F'),
        ModifiableKey::Function(function) => match function.number() {
            1 => Some('P'),
            2 => Some('Q'),
            3 => Some('R'),
            4 => Some('S'),
            _ => None,
        },
        ModifiableKey::PageUp | ModifiableKey::PageDown => None,
    }
}

const fn csi_tilde_code(base: ModifiableKey) -> Option<u8> {
    match base {
        ModifiableKey::PageUp => Some(5),
        ModifiableKey::PageDown => Some(6),
        ModifiableKey::Function(function) => match function.number() {
            5 => Some(15),
            6 => Some(17),
            7 => Some(18),
            8 => Some(19),
            9 => Some(20),
            10 => Some(21),
            11 => Some(23),
            12 => Some(24),
            _ => None,
        },
        ModifiableKey::Up
        | ModifiableKey::Down
        | ModifiableKey::Left
        | ModifiableKey::Right
        | ModifiableKey::Home
        | ModifiableKey::End => None,
    }
}

fn encode_mouse(
    encoded: &mut BoundedInputBuffer,
    event: MouseEvent,
) -> Result<(), TerminalInputEncodingError> {
    let button = match event.button() {
        MouseButton::Left => 0,
        MouseButton::Middle => 1,
        MouseButton::Right => 2,
        MouseButton::ScrollUp => 64,
        MouseButton::ScrollDown => 65,
    };
    let code = if event.kind() == MouseEventKind::Move {
        button + 32
    } else {
        button
    };
    let final_byte = if event.kind() == MouseEventKind::Release {
        'm'
    } else {
        'M'
    };
    write!(
        encoded,
        "\x1b[<{code};{};{}{final_byte}",
        event.column(),
        event.row()
    )
    .map_err(|_| encoded.failure())
}

#[cfg(test)]
mod tests {
    use undercurrent_domain::{ControlKey, MouseEventKind, TerminalSize};

    use super::*;

    #[test]
    fn profile_text_encoding_preserves_bounds_overrides_and_submit_boundary() {
        use undercurrent_domain::InputTextMode;
        for size in [1024, 1025, 4096, 16384] {
            let body = "a\nb".repeat(size / 3 + 1)[..size].to_owned();
            for paste in [false, true] {
                for submit in [false, true] {
                    let policy = InputDeliveryPolicy::paced_trailing_submit(
                        Duration::from_millis(120),
                        Duration::from_secs(2),
                    )
                    .unwrap()
                    .with_text_mode(if paste {
                        InputTextMode::Paste
                    } else {
                        InputTextMode::Raw
                    });
                    for (item, framed) in [
                        (InputItem::AutoText(body.clone()), paste),
                        (InputItem::Text(body.clone()), false),
                        (InputItem::Paste(body.clone()), true),
                    ] {
                        let mut items = vec![item];
                        if submit {
                            items.push(InputItem::Key(Key::Enter));
                        }
                        let input = InputSequence::new(items, 2, size + 28).unwrap();
                        let result = encode_terminal_input_delivery(&input, policy).unwrap();
                        let mut expected = Vec::new();
                        if framed {
                            expected.extend_from_slice(PASTE_START);
                        }
                        expected.extend_from_slice(body.as_bytes());
                        if framed {
                            expected.extend_from_slice(PASTE_END);
                        }
                        let boundary = expected.len();
                        if submit {
                            expected.push(b'\r');
                        }
                        assert_eq!(result.bytes(), expected);
                        assert_eq!(
                            result.pause().map(|p| p.offset()),
                            submit.then_some(boundary)
                        );
                    }
                }
            }
        }
        assert!(InputSequence::new(vec![InputItem::AutoText("x".repeat(1024))], 1, 1035).is_err());
    }

    #[test]
    fn complete_mixed_sequence_is_encoded_once_in_caller_order() {
        let terminal = TerminalSize::new(120, 40, 120, 40).expect("terminal");
        let input = InputSequence::new(
            vec![
                InputItem::Text("hello".to_owned()),
                InputItem::Key(Key::Enter),
                InputItem::Bytes(vec![0, 0xff]),
                InputItem::Paste("private".to_owned()),
                InputItem::Mouse(
                    MouseEvent::new(120, 40, MouseButton::Left, MouseEventKind::Press, terminal)
                        .expect("mouse"),
                ),
            ],
            8,
            256,
        )
        .expect("input");

        assert_eq!(
            encode_terminal_input(&input).expect("encode"),
            b"hello\r\0\xff\x1b[200~private\x1b[201~\x1b[<0;120;40M"
        );
        assert!(!format!("{input:?}").contains("private"));
    }

    #[test]
    fn named_keys_and_every_modifier_family_match_xterm_sequences() {
        let input = InputSequence::new(
            vec![
                InputItem::Key(Key::BackTab),
                InputItem::Key(Key::Delete),
                InputItem::Key(Key::Control(ControlKey::new('C').expect("control"))),
                InputItem::Key(Key::Function(FunctionKey::new(1).expect("F1"))),
                InputItem::Key(Key::Function(FunctionKey::new(12).expect("F12"))),
                InputItem::Key(Key::Modified {
                    base: ModifiableKey::Up,
                    modifiers: KeyModifiers {
                        shift: false,
                        control: true,
                        alt: false,
                    },
                }),
                InputItem::Key(Key::Modified {
                    base: ModifiableKey::Function(FunctionKey::new(5).expect("F5")),
                    modifiers: KeyModifiers {
                        shift: true,
                        control: false,
                        alt: true,
                    },
                }),
            ],
            8,
            256,
        )
        .expect("input");

        assert_eq!(
            encode_terminal_input(&input).expect("encode"),
            b"\x1b[Z\x1b[3~\x03\x1bOP\x1b[24~\x1b[1;5A\x1b[15;4~"
        );
    }

    #[test]
    fn maximum_mouse_encoding_stays_within_the_admitted_bound() {
        let terminal = TerminalSize::new(u16::MAX, u16::MAX, u16::MAX, u16::MAX).expect("terminal");
        let input = InputSequence::new(
            vec![InputItem::Mouse(
                MouseEvent::new(
                    u16::MAX,
                    u16::MAX,
                    MouseButton::ScrollDown,
                    MouseEventKind::Move,
                    terminal,
                )
                .expect("mouse"),
            )],
            1,
            18,
        )
        .expect("input");
        let encoded = encode_terminal_input(&input).expect("encode");

        assert_eq!(encoded, b"\x1b[<97;65535;65535M");
        assert_eq!(encoded.len(), input.payload_bytes());
    }

    #[test]
    fn bounded_buffer_rejects_before_mutating_or_reallocating() {
        let mut buffer = BoundedInputBuffer::new(1).expect("buffer");

        assert_eq!(
            buffer.extend(b"ab"),
            Err(TerminalInputEncodingError::BoundViolated {
                admitted_bytes: 1,
                encoded_bytes: 2,
            })
        );
        assert!(buffer.into_bytes().is_empty());
    }

    #[test]
    fn paced_trailing_enter_retains_one_atomic_delivery_with_an_internal_boundary() {
        let input = InputSequence::new(
            vec![
                InputItem::Text("status".to_owned()),
                InputItem::Key(Key::Enter),
            ],
            2,
            64,
        )
        .expect("input");
        let policy = InputDeliveryPolicy::paced_trailing_submit(
            Duration::from_millis(120),
            Duration::from_secs(2),
        )
        .expect("policy");

        let delivery = encode_terminal_input_delivery(&input, policy).expect("encode");

        assert_eq!(delivery.bytes(), b"status\r");
        assert_eq!(
            delivery.pause(),
            Some(EncodedInputPause::new(6, Duration::from_millis(120)))
        );
        let debug = format!("{delivery:?}");
        assert!(debug.contains("bytes: 7"));
        assert!(!debug.contains("status"));
    }

    #[test]
    fn pacing_is_not_applied_without_a_body_and_final_enter() {
        let policy = InputDeliveryPolicy::paced_trailing_submit(
            Duration::from_millis(120),
            Duration::from_secs(2),
        )
        .expect("policy");
        let enter_only =
            InputSequence::new(vec![InputItem::Key(Key::Enter)], 1, 16).expect("enter input");
        let enter_then_tab = InputSequence::new(
            vec![InputItem::Key(Key::Enter), InputItem::Key(Key::Tab)],
            2,
            64,
        )
        .expect("non-submit input");

        assert_eq!(
            encode_terminal_input_delivery(&enter_only, policy)
                .expect("encode")
                .pause(),
            None
        );
        assert_eq!(
            encode_terminal_input_delivery(&enter_then_tab, policy)
                .expect("encode")
                .pause(),
            None
        );
    }

    #[test]
    fn host_decoded_pacing_metadata_must_be_internal_and_millisecond_aligned() {
        assert!(matches!(
            EncodedTerminalInput::from_parts(
                b"a\r".to_vec(),
                Some(EncodedInputPause::new(0, Duration::from_millis(1)))
            ),
            Err(TerminalInputEncodingError::InvalidPacingBoundary { .. })
        ));
        assert!(matches!(
            EncodedTerminalInput::from_parts(
                b"a\r".to_vec(),
                Some(EncodedInputPause::new(1, Duration::from_nanos(1)))
            ),
            Err(TerminalInputEncodingError::InvalidPacingDelay)
        ));
    }
}
