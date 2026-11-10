// SPDX-License-Identifier: Apache-2.0

//! Interactive terminal port and native Crossterm adapter.

use std::time::Duration;

use crossterm::event::{
    self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent,
    MouseEventKind,
};
use undercurrent_protocol::{
    KeyDto, KeyModifiersDto, ModifiableKeyDto, MouseButtonDto, MouseEventDto, MouseEventKindDto,
    TerminalInputDto, TerminalSizeDto,
};

use crate::{CliTerminalError, CliTerminalOperation, DetachKey, InteractiveInput};

/// One local interactive terminal event translated to the product vocabulary.
pub enum InteractiveTerminalEvent {
    /// One input item that may be forwarded or consumed by detachment.
    Input(InteractiveInput),
    /// One live terminal geometry change.
    Resize(TerminalSizeDto),
}

/// Interactive terminal behavior required by attach orchestration.
pub trait InteractiveTerminalPort {
    /// Reads current terminal geometry.
    ///
    /// # Errors
    ///
    /// Returns a typed native terminal failure.
    fn size(&mut self) -> Result<TerminalSizeDto, CliTerminalError>;

    /// Enables raw input mode before remote terminal interaction.
    ///
    /// # Errors
    ///
    /// Returns a typed native terminal failure.
    fn enter_raw_mode(&mut self) -> Result<(), CliTerminalError>;

    /// Restores the terminal mode after remote terminal interaction.
    ///
    /// # Errors
    ///
    /// Returns a typed native terminal failure.
    fn restore(&mut self) -> Result<(), CliTerminalError>;

    /// Polls for one translated input or resize event up to a bounded duration.
    ///
    /// Unsupported native events are consumed and reported as `None`.
    ///
    /// # Errors
    ///
    /// Returns a typed native terminal poll or decode failure.
    fn poll_event(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<InteractiveTerminalEvent>, CliTerminalError>;
}

/// Native Crossterm implementation of the interactive terminal port.
#[derive(Default)]
pub struct NativeInteractiveTerminal {
    raw_enabled: bool,
}

impl NativeInteractiveTerminal {
    /// Creates a terminal adapter without changing process terminal state.
    pub const fn new() -> Self {
        Self { raw_enabled: false }
    }
}

impl InteractiveTerminalPort for NativeInteractiveTerminal {
    fn size(&mut self) -> Result<TerminalSizeDto, CliTerminalError> {
        let (columns, rows) = crossterm::terminal::size()
            .map_err(|source| CliTerminalError::new(CliTerminalOperation::ReadSize, source))?;
        Ok(TerminalSizeDto { columns, rows })
    }

    fn enter_raw_mode(&mut self) -> Result<(), CliTerminalError> {
        crossterm::terminal::enable_raw_mode()
            .map_err(|source| CliTerminalError::new(CliTerminalOperation::EnterRawMode, source))?;
        self.raw_enabled = true;
        Ok(())
    }

    fn restore(&mut self) -> Result<(), CliTerminalError> {
        if self.raw_enabled {
            crossterm::terminal::disable_raw_mode().map_err(|source| {
                CliTerminalError::new(CliTerminalOperation::RestoreRawMode, source)
            })?;
            self.raw_enabled = false;
        }
        Ok(())
    }

    fn poll_event(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<InteractiveTerminalEvent>, CliTerminalError> {
        if !event::poll(timeout)
            .map_err(|source| CliTerminalError::new(CliTerminalOperation::PollEvent, source))?
        {
            return Ok(None);
        }
        let event = event::read()
            .map_err(|source| CliTerminalError::new(CliTerminalOperation::ReadEvent, source))?;
        Ok(translate_event(event))
    }
}

impl Drop for NativeInteractiveTerminal {
    fn drop(&mut self) {
        if self.raw_enabled {
            let _ = crossterm::terminal::disable_raw_mode();
            self.raw_enabled = false;
        }
    }
}

fn translate_event(event: Event) -> Option<InteractiveTerminalEvent> {
    match event {
        Event::Key(event) if matches!(event.kind, KeyEventKind::Press | KeyEventKind::Repeat) => {
            translate_key(event).map(InteractiveTerminalEvent::Input)
        }
        Event::Mouse(event) => translate_mouse(event).map(InteractiveTerminalEvent::Input),
        Event::Paste(text) => Some(InteractiveTerminalEvent::Input(InteractiveInput {
            item: TerminalInputDto::Paste { text },
            detach_key: None,
        })),
        Event::Resize(columns, rows) if columns != 0 && rows != 0 => {
            Some(InteractiveTerminalEvent::Resize(TerminalSizeDto {
                columns,
                rows,
            }))
        }
        Event::FocusGained | Event::FocusLost | Event::Key(_) | Event::Resize(_, _) => None,
    }
}

fn translate_key(event: KeyEvent) -> Option<InteractiveInput> {
    let modifiers = event.modifiers;
    match event.code {
        KeyCode::Char(character) => translate_character(character, modifiers),
        KeyCode::Enter => named(KeyDto::Enter),
        KeyCode::Tab => named(KeyDto::Tab),
        KeyCode::BackTab => named(KeyDto::BackTab),
        KeyCode::Esc => named(KeyDto::Escape),
        KeyCode::Backspace => named(KeyDto::Backspace),
        KeyCode::Delete => named(KeyDto::Delete),
        KeyCode::Up => modifiable(KeyDto::Up, ModifiableKeyDto::Up, modifiers),
        KeyCode::Down => modifiable(KeyDto::Down, ModifiableKeyDto::Down, modifiers),
        KeyCode::Left => modifiable(KeyDto::Left, ModifiableKeyDto::Left, modifiers),
        KeyCode::Right => modifiable(KeyDto::Right, ModifiableKeyDto::Right, modifiers),
        KeyCode::Home => modifiable(KeyDto::Home, ModifiableKeyDto::Home, modifiers),
        KeyCode::End => modifiable(KeyDto::End, ModifiableKeyDto::End, modifiers),
        KeyCode::PageUp => modifiable(KeyDto::PageUp, ModifiableKeyDto::PageUp, modifiers),
        KeyCode::PageDown => modifiable(KeyDto::PageDown, ModifiableKeyDto::PageDown, modifiers),
        KeyCode::F(number) if (1..=12).contains(&number) => modifiable(
            KeyDto::Function { number },
            ModifiableKeyDto::Function { number },
            modifiers,
        ),
        KeyCode::Insert => raw(b"\x1b[2~".to_vec()),
        KeyCode::Null => raw(vec![0]),
        KeyCode::F(_)
        | KeyCode::CapsLock
        | KeyCode::ScrollLock
        | KeyCode::NumLock
        | KeyCode::PrintScreen
        | KeyCode::Pause
        | KeyCode::Menu
        | KeyCode::KeypadBegin
        | KeyCode::Media(_)
        | KeyCode::Modifier(_) => None,
    }
}

fn translate_character(character: char, modifiers: KeyModifiers) -> Option<InteractiveInput> {
    let control = modifiers.contains(KeyModifiers::CONTROL);
    let alt = modifiers.contains(KeyModifiers::ALT);
    if control {
        let byte = control_byte(character)?;
        if character.is_ascii_alphabetic() && !alt {
            let character = character.to_ascii_lowercase();
            return Some(InteractiveInput {
                item: TerminalInputDto::Key {
                    key: KeyDto::Control { character },
                },
                detach_key: Some(DetachKey::Control(character)),
            });
        }
        let mut bytes = Vec::with_capacity(2);
        if alt {
            bytes.push(0x1b);
        }
        bytes.push(byte);
        return raw(bytes);
    }

    if alt {
        let mut bytes = vec![0x1b];
        let mut encoded = [0; 4];
        bytes.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
        return Some(InteractiveInput {
            item: TerminalInputDto::Raw { bytes },
            detach_key: character.is_ascii().then_some(DetachKey::Alt(character)),
        });
    }

    if character == ' ' {
        return Some(InteractiveInput {
            item: TerminalInputDto::Key { key: KeyDto::Space },
            detach_key: Some(DetachKey::Literal(character)),
        });
    }
    Some(InteractiveInput {
        item: TerminalInputDto::Text {
            text: character.to_string(),
        },
        detach_key: character
            .is_ascii()
            .then_some(DetachKey::Literal(character)),
    })
}

fn named(key: KeyDto) -> Option<InteractiveInput> {
    Some(InteractiveInput {
        item: TerminalInputDto::Key { key },
        detach_key: None,
    })
}

fn raw(bytes: Vec<u8>) -> Option<InteractiveInput> {
    Some(InteractiveInput {
        item: TerminalInputDto::Raw { bytes },
        detach_key: None,
    })
}

fn modifiable(
    plain: KeyDto,
    base: ModifiableKeyDto,
    modifiers: KeyModifiers,
) -> Option<InteractiveInput> {
    if modifiers.is_empty() {
        named(plain)
    } else {
        named(KeyDto::Modified {
            base,
            modifiers: KeyModifiersDto {
                shift: modifiers.contains(KeyModifiers::SHIFT),
                control: modifiers.contains(KeyModifiers::CONTROL),
                alt: modifiers.contains(KeyModifiers::ALT),
            },
        })
    }
}

fn control_byte(character: char) -> Option<u8> {
    match character {
        'a'..='z' => Some(character as u8 - b'a' + 1),
        'A'..='Z' => Some(character as u8 - b'A' + 1),
        '@' | ' ' => Some(0),
        '[' => Some(27),
        '\\' => Some(28),
        ']' => Some(29),
        '^' => Some(30),
        '_' => Some(31),
        '?' => Some(127),
        _ => None,
    }
}

fn translate_mouse(event: MouseEvent) -> Option<InteractiveInput> {
    let column = event.column.checked_add(1)?;
    let row = event.row.checked_add(1)?;
    let (button, kind) = match event.kind {
        MouseEventKind::Down(button) => (mouse_button(button), MouseEventKindDto::Press),
        MouseEventKind::Up(button) => (mouse_button(button), MouseEventKindDto::Release),
        MouseEventKind::Drag(button) => (mouse_button(button), MouseEventKindDto::Move),
        MouseEventKind::ScrollUp => (Some(MouseButtonDto::ScrollUp), MouseEventKindDto::Press),
        MouseEventKind::ScrollDown => (Some(MouseButtonDto::ScrollDown), MouseEventKindDto::Press),
        MouseEventKind::Moved | MouseEventKind::ScrollLeft | MouseEventKind::ScrollRight => {
            return None;
        }
    };
    Some(InteractiveInput {
        item: TerminalInputDto::Mouse {
            event: MouseEventDto {
                column,
                row,
                button: button?,
                event: kind,
            },
        },
        detach_key: None,
    })
}

const fn mouse_button(button: MouseButton) -> Option<MouseButtonDto> {
    match button {
        MouseButton::Left => Some(MouseButtonDto::Left),
        MouseButton::Right => Some(MouseButtonDto::Right),
        MouseButton::Middle => Some(MouseButtonDto::Middle),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_and_control_characters_preserve_detach_identity() {
        let plain = translate_key(KeyEvent::new(KeyCode::Char('d'), KeyModifiers::NONE))
            .expect("plain character");
        assert_eq!(plain.detach_key, Some(DetachKey::Literal('d')));

        let control = translate_key(KeyEvent::new(KeyCode::Char('B'), KeyModifiers::CONTROL))
            .expect("control character");
        assert_eq!(control.detach_key, Some(DetachKey::Control('b')));
        assert!(matches!(
            control.item,
            TerminalInputDto::Key {
                key: KeyDto::Control { character: 'b' }
            }
        ));
    }

    #[test]
    fn alt_and_non_alphabetic_control_keys_use_exact_raw_bytes() {
        let alt = translate_key(KeyEvent::new(KeyCode::Char('x'), KeyModifiers::ALT))
            .expect("alt character");
        assert_eq!(alt.detach_key, Some(DetachKey::Alt('x')));
        assert!(matches!(
            alt.item,
            TerminalInputDto::Raw { bytes } if bytes == b"\x1bx"
        ));

        let control = translate_key(KeyEvent::new(KeyCode::Char('['), KeyModifiers::CONTROL))
            .expect("control punctuation");
        assert!(matches!(
            control.item,
            TerminalInputDto::Raw { bytes } if bytes == vec![27]
        ));
    }

    #[test]
    fn resize_and_mouse_coordinates_are_validated_and_one_based() {
        assert!(matches!(
            translate_event(Event::Resize(120, 40)),
            Some(InteractiveTerminalEvent::Resize(TerminalSizeDto {
                columns: 120,
                rows: 40
            }))
        ));
        let mouse = translate_mouse(MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: 0,
            row: 1,
            modifiers: KeyModifiers::NONE,
        })
        .expect("mouse event");
        assert!(matches!(
            mouse.item,
            TerminalInputDto::Mouse {
                event: MouseEventDto {
                    column: 1,
                    row: 2,
                    ..
                }
            }
        ));
    }

    #[test]
    fn release_and_unsupported_events_do_not_generate_input() {
        assert!(
            translate_event(Event::Key(KeyEvent::new_with_kind(
                KeyCode::Char('x'),
                KeyModifiers::NONE,
                KeyEventKind::Release,
            )))
            .is_none()
        );
        assert!(translate_event(Event::FocusGained).is_none());
    }
}
