// SPDX-License-Identifier: Apache-2.0

//! A confined cell renderer: child escape sequences never reach the outer terminal.

use crate::{CliError, CliOutputError};

mod selection;
use crossterm::{
    cursor::MoveTo,
    queue,
    style::{Attribute, Color, Print, SetAttribute, SetBackgroundColor, SetForegroundColor},
};
pub(crate) use selection::clipboard;
use selection::{Selection, SelectionResult};
use std::io::Write;
use undercurrent_protocol::{
    KeyDto, ModifiableKeyDto, MouseButtonDto, MouseEventKindDto, TerminalInputDto, TerminalSizeDto,
};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

#[cfg(test)]
const SCROLLBACK_ROWS: usize = 2_000;

pub(crate) struct Renderer {
    scrollback: undercurrent_config::ScrollbackConfig,
    pub parser: vt100::Parser,
    query_parser: vte::Parser,
    // Diff only visible cells; cloning Screen would copy the entire history per frame.
    previous: Option<Vec<Option<vt100::Cell>>>,
    previous_bar: String,
    history_view: Option<vt100::Screen>,
    selection: Option<Selection>,
    pending_drag: Option<TerminalInputDto>,
    copied: Option<String>,
    history_notice: bool,
    history_partial: bool,
    pub size: TerminalSizeDto,
}

impl Renderer {
    #[cfg(test)]
    pub fn new(size: TerminalSizeDto) -> Self {
        Self::with_scrollback(size, Default::default())
    }

    pub fn with_scrollback(
        size: TerminalSizeDto,
        scrollback: undercurrent_config::ScrollbackConfig,
    ) -> Self {
        Self {
            scrollback,
            parser: vt100::Parser::new(size.rows, size.columns, scrollback.memory_lines),
            query_parser: vte::Parser::new(),
            previous: None,
            previous_bar: String::new(),
            history_view: None,
            selection: None,
            pending_drag: None,
            copied: None,
            history_notice: false,
            history_partial: false,
            size,
        }
    }

    pub fn resize(&mut self, size: TerminalSizeDto) {
        self.size = size;
        self.parser.set_size(size.rows, size.columns);
        if let Some(selection) = &mut self.selection {
            selection.clamp(size);
        }
        self.refresh_history_view();
        self.invalidate();
    }

    pub fn invalidate(&mut self) {
        self.previous = None;
        self.previous_bar.clear();
    }

    pub fn reset(&mut self, contents: &[u8], alternate: bool) {
        self.parser = vt100::Parser::new(
            self.size.rows,
            self.size.columns,
            self.scrollback.memory_lines,
        );
        self.history_view = None;
        // A stream checkpoint repairs the live parser, not the user's frozen
        // selection. Keep copying stable even if output needs resynchronizing.
        self.pending_drag = None;
        self.history_notice = false;
        // Snapshot history is bounded by the sender's row and byte budgets.
        self.history_partial = true;
        self.query_parser = vte::Parser::new();
        if alternate {
            self.parser.process(b"\x1b[?1049h");
        }
        self.parser.process(contents);
        self.invalidate();
    }

    pub fn process(&mut self, bytes: &[u8]) -> Vec<u8> {
        let alternate = self.parser.screen().alternate_screen();
        let mut capture = Queries::default();
        let mut last = 0;
        let mut replies = Vec::new();
        for (index, byte) in bytes.iter().enumerate() {
            self.query_parser.advance(&mut capture, *byte);
            if let Some(query) = capture.query.take() {
                // Answer at the query's position in the byte stream. Processing
                // the whole chunk first could include later cursor movement in
                // the reply; the frozen history/copy view is not child state.
                // See [renderer boundaries](../../docs/internals/terminal-and-streams.md#preserve-input-order-and-renderer-boundaries).
                self.parser.process(&bytes[last..=index]);
                last = index + 1;
                // Bound terminal reply traffic per received output chunk.
                if replies.len() < 4096 {
                    replies.extend(query.reply(self.parser.screen()));
                }
            }
        }
        self.parser.process(&bytes[last..]);
        if alternate != self.parser.screen().alternate_screen()
            || (self.history_view.is_some() && self.parser.screen().scrollback() == 0)
        {
            self.leave_history();
        }
        replies
    }

    fn refresh_history_view(&mut self) {
        self.history_view =
            (self.parser.screen().scrollback() > 0).then(|| self.parser.screen().clone());
    }

    fn leave_history(&mut self) {
        self.parser.set_scrollback(0);
        self.history_view = None;
        self.history_notice = false;
    }

    /// Captures a frozen terminal view for local copy selection and requests a redraw.
    pub fn enter_copy(&mut self) {
        self.pending_drag = None;
        self.selection = Some(Selection::capture(&mut self.parser));
        self.invalidate();
    }

    pub fn take_copy(&mut self) -> Option<String> {
        self.copied.take()
    }

    /// Handle local navigation before the read-only gate or child input encoding.
    /// Shift+PageUp/PageDown override application mouse capture; Shift+End returns live.
    pub fn history_input(&mut self, input: &TerminalInputDto) -> bool {
        if self.selection.is_none() {
            match input {
                TerminalInputDto::Mouse { event }
                    if event.button == MouseButtonDto::Left
                        && event.row > 0
                        && event.row <= self.size.rows
                        && event.column > 0
                        && event.column <= self.size.columns
                        && (self.history_view.is_some()
                            || self.parser.screen().mouse_protocol_mode()
                                == vt100::MouseProtocolMode::None) =>
                {
                    match event.event {
                        MouseEventKindDto::Press => self.pending_drag = Some(input.clone()),
                        MouseEventKindDto::Move => {
                            if let Some(press) = self.pending_drag.take() {
                                self.enter_copy();
                                if let Some(selection) = &mut self.selection {
                                    selection.input(&press, self.size);
                                }
                            }
                        }
                        MouseEventKindDto::Release => self.pending_drag = None,
                    }
                }
                _ => self.pending_drag = None,
            }
        }
        if let Some(selection) = &mut self.selection {
            match selection.input(input, self.size) {
                SelectionResult::Continue => {}
                SelectionResult::Cancel => {
                    self.selection = None;
                    self.leave_history();
                }
                SelectionResult::Copy(text) => {
                    self.copied = Some(text);
                }
            }
            self.invalidate();
            return true;
        }
        let screen = self.parser.screen();
        let browsing = self.history_view.is_some();
        let page = usize::from(self.size.rows.saturating_sub(1).max(1));
        let (up, amount) = match input {
            TerminalInputDto::Key {
                key: KeyDto::Modified { base, modifiers },
            } if modifiers.shift && !modifiers.control && !modifiers.alt => match base {
                ModifiableKeyDto::PageUp => (true, page),
                ModifiableKeyDto::PageDown => (false, page),
                ModifiableKeyDto::End => {
                    self.leave_history();
                    return true;
                }
                _ => {
                    self.leave_history();
                    return false;
                }
            },
            TerminalInputDto::Key {
                key: KeyDto::Escape | KeyDto::End,
            } if browsing || self.history_notice => {
                self.leave_history();
                return true;
            }
            TerminalInputDto::Key {
                key: KeyDto::PageUp,
            } if browsing => (true, page),
            TerminalInputDto::Key {
                key: KeyDto::PageDown,
            } if browsing => (false, page),
            TerminalInputDto::Mouse { event }
                if event.event == MouseEventKindDto::Press
                    && event.row > 0
                    && event.row <= self.size.rows
                    && event.column > 0
                    && event.column <= self.size.columns
                    && (browsing
                        || (!screen.alternate_screen()
                            && screen.mouse_protocol_mode() == vt100::MouseProtocolMode::None)) =>
            {
                match event.button {
                    MouseButtonDto::ScrollUp => (true, 3),
                    MouseButtonDto::ScrollDown => (false, 3),
                    _ => return browsing,
                }
            }
            TerminalInputDto::Mouse { .. } => return browsing,
            _ => {
                self.leave_history();
                return false;
            }
        };
        let offset = screen.scrollback();
        self.parser.set_scrollback(if up {
            offset.saturating_add(amount)
        } else {
            offset.saturating_sub(amount)
        });
        self.history_notice = up || self.parser.screen().scrollback() > 0;
        self.refresh_history_view();
        true
    }

    pub fn input(&self, input: TerminalInputDto) -> Option<TerminalInputDto> {
        let screen = self.parser.screen();
        match input {
            TerminalInputDto::Key { key } if screen.application_cursor() => {
                let suffix = match key {
                    KeyDto::Up => Some(b'A'),
                    KeyDto::Down => Some(b'B'),
                    KeyDto::Right => Some(b'C'),
                    KeyDto::Left => Some(b'D'),
                    KeyDto::Home => Some(b'H'),
                    KeyDto::End => Some(b'F'),
                    _ => None,
                };
                Some(
                    suffix.map_or(TerminalInputDto::Key { key }, |c| TerminalInputDto::Raw {
                        bytes: vec![27, b'O', c],
                    }),
                )
            }
            TerminalInputDto::Paste { text } => {
                let bytes = if screen.bracketed_paste() {
                    [b"\x1b[200~".as_slice(), text.as_bytes(), b"\x1b[201~"].concat()
                } else {
                    text.into_bytes()
                };
                Some(TerminalInputDto::Raw { bytes })
            }
            TerminalInputDto::Mouse { event } => {
                use vt100::{MouseProtocolEncoding as Encoding, MouseProtocolMode as Mode};
                let mode = screen.mouse_protocol_mode();
                if event.row == 0
                    || event.column == 0
                    || event.row > self.size.rows
                    || event.column > self.size.columns
                    || mode == Mode::None
                    || (mode == Mode::Press && event.event != MouseEventKindDto::Press)
                    || (mode == Mode::PressRelease && event.event == MouseEventKindDto::Move)
                {
                    return None;
                }
                let mut code = match event.button {
                    MouseButtonDto::Left => 0,
                    MouseButtonDto::Middle => 1,
                    MouseButtonDto::Right => 2,
                    MouseButtonDto::ScrollUp => 64,
                    MouseButtonDto::ScrollDown => 65,
                };
                if event.event == MouseEventKindDto::Move {
                    code += 32;
                }
                let released = event.event == MouseEventKindDto::Release;
                let bytes = match screen.mouse_protocol_encoding() {
                    Encoding::Sgr => format!(
                        "\x1b[<{code};{};{}{}",
                        event.column,
                        event.row,
                        if released { 'm' } else { 'M' }
                    )
                    .into_bytes(),
                    encoding => {
                        if released {
                            code = 3;
                        }
                        let mut bytes = b"\x1b[M".to_vec();
                        for value in [
                            code + 32,
                            u32::from(event.column) + 32,
                            u32::from(event.row) + 32,
                        ] {
                            if encoding == Encoding::Default {
                                if value > 255 {
                                    return None;
                                }
                                bytes.push(value as u8);
                            } else {
                                let character = char::from_u32(value)?;
                                let mut encoded = [0; 4];
                                bytes.extend_from_slice(
                                    character.encode_utf8(&mut encoded).as_bytes(),
                                );
                            }
                        }
                        bytes
                    }
                };
                Some(TerminalInputDto::Raw { bytes })
            }
            other => Some(other),
        }
    }

    pub fn draw(&mut self, writer: &mut impl Write, bar: &str) -> Result<(), CliError> {
        self.draw_at(writer, bar, self.size.rows)
    }

    pub fn draw_at(
        &mut self,
        writer: &mut impl Write,
        bar: &str,
        bar_row: u16,
    ) -> Result<(), CliError> {
        self.draw_region(writer, bar, bar_row, (0, 0), true)
    }

    pub fn draw_region(
        &mut self,
        writer: &mut impl Write,
        bar: &str,
        bar_row: u16,
        origin: (u16, u16),
        focused: bool,
    ) -> Result<(), CliError> {
        let history_bar;
        let bar = if let Some(selection) = &self.selection {
            selection.bar()
        } else if self.history_notice {
            history_bar = if self.parser.screen().alternate_screen() {
                "UC · application controls history · Esc: live".to_owned()
            } else {
                format!(
                    "UC · history {} · {} · Esc: live",
                    self.parser.screen().scrollback(),
                    if self.history_partial {
                        "earlier history unavailable".to_owned()
                    } else {
                        format!("up to {} lines", self.scrollback.memory_lines)
                    },
                )
            };
            history_bar.as_str()
        } else {
            bar
        };
        let mut frame = Vec::new();
        frame.extend_from_slice(b"\x1b[?25l\x1b[?7l");
        let screen = self
            .history_view
            .as_ref()
            .unwrap_or_else(|| self.parser.screen());
        let cell_at = |row, col| match &self.selection {
            Some(selection) => selection.cell(row, col),
            None => screen.cell(row, col),
        };
        for row in 0..self.size.rows {
            let changed = self.previous.as_ref().is_none_or(|prev| {
                (0..self.size.columns).any(|col| {
                    let index =
                        usize::from(row) * usize::from(self.size.columns) + usize::from(col);
                    cell_at(row, col) != prev[index].as_ref()
                })
            });
            if !changed {
                continue;
            }
            queue!(
                frame,
                MoveTo(origin.0, origin.1 + row),
                SetAttribute(Attribute::Reset)
            )
            .map_err(CliOutputError::from)?;
            let mut previous_cell: Option<&vt100::Cell> = None;
            let mut previous_highlight = false;
            for col in 0..self.size.columns {
                let Some(cell) = cell_at(row, col) else {
                    queue!(frame, SetAttribute(Attribute::Reset), Print(" "))
                        .map_err(CliOutputError::from)?;
                    previous_cell = None;
                    continue;
                };
                if cell.is_wide_continuation() {
                    continue;
                }
                if cell.is_wide() && col + 1 >= self.size.columns {
                    queue!(frame, SetAttribute(Attribute::Reset), Print(" "))
                        .map_err(CliOutputError::from)?;
                    previous_cell = None;
                    continue;
                }
                let highlight = self
                    .selection
                    .as_ref()
                    .is_some_and(|s| s.highlighted(row, col));
                if highlight != previous_highlight
                    || previous_cell.is_none_or(|p| {
                        p.fgcolor() != cell.fgcolor()
                            || p.bgcolor() != cell.bgcolor()
                            || p.bold() != cell.bold()
                            || p.italic() != cell.italic()
                            || p.underline() != cell.underline()
                            || p.inverse() != cell.inverse()
                    })
                {
                    queue!(
                        frame,
                        SetAttribute(Attribute::Reset),
                        SetForegroundColor(color(cell.fgcolor())),
                        SetBackgroundColor(color(cell.bgcolor()))
                    )
                    .map_err(CliOutputError::from)?;
                    for (enabled, attr) in [
                        (cell.bold(), Attribute::Bold),
                        (cell.italic(), Attribute::Italic),
                        (cell.underline(), Attribute::Underlined),
                        (cell.inverse() ^ highlight, Attribute::Reverse),
                    ] {
                        if enabled {
                            queue!(frame, SetAttribute(attr)).map_err(CliOutputError::from)?;
                        }
                    }
                }
                let contents = cell.contents();
                frame.extend_from_slice(if contents.is_empty() {
                    b" "
                } else {
                    contents.as_bytes()
                });
                previous_cell = Some(cell);
                previous_highlight = highlight;
            }
        }
        if self.previous.is_none() || self.previous_bar != bar {
            let width = usize::from(self.size.columns);
            let text = fit(bar, width);
            queue!(
                frame,
                MoveTo(origin.0, origin.1 + bar_row),
                SetAttribute(Attribute::Reset),
                SetAttribute(Attribute::Reverse),
                Print(&text),
                // Erasing the remainder does not preserve reverse video on
                // every terminal. Paint spaces, including the last column;
                // automatic wrapping is disabled for the whole frame.
                Print(" ".repeat(width.saturating_sub(text.width())))
            )
            .map_err(CliOutputError::from)?;
            frame.extend_from_slice(b"\x1b[0m");
        }
        let (row, col) = self.selection.as_ref().map_or_else(
            || screen.cursor_position(),
            |s| ((s.cursor.0 - s.top) as u16, s.cursor.1),
        );
        queue!(
            frame,
            SetAttribute(Attribute::Reset),
            MoveTo(
                origin.0 + col.min(self.size.columns - 1),
                origin.1 + row.min(self.size.rows - 1)
            )
        )
        .map_err(CliOutputError::from)?;
        if focused
            && (self.selection.is_some() || (self.history_view.is_none() && !screen.hide_cursor()))
        {
            frame.extend_from_slice(b"\x1b[?25h");
        }
        writer
            .write_all(&frame)
            .and_then(|()| writer.flush())
            .map_err(CliOutputError::from)?;
        self.previous = Some(
            (0..self.size.rows)
                .flat_map(|row| (0..self.size.columns).map(move |col| cell_at(row, col).cloned()))
                .collect(),
        );
        self.previous_bar = bar.to_owned();
        Ok(())
    }
}

pub(crate) fn fit(text: &str, width: usize) -> String {
    let mut columns = 0;
    text.chars()
        .filter(|c| !c.is_control())
        .take_while(|c| {
            columns += c.width().unwrap_or(0);
            columns <= width
        })
        .collect()
}

fn color(c: vt100::Color) -> Color {
    match c {
        vt100::Color::Default => Color::Reset,
        vt100::Color::Idx(i) => Color::AnsiValue(i),
        vt100::Color::Rgb(r, g, b) => Color::Rgb { r, g, b },
    }
}

#[derive(Default)]
struct Queries {
    query: Option<Query>,
}
enum Query {
    PrivateCursor,
    Secondary,
    Size,
    Foreground,
    Background,
}
impl Query {
    fn reply(self, screen: &vt100::Screen) -> Vec<u8> {
        match self {
            Self::PrivateCursor => {
                let (r, c) = screen.cursor_position();
                format!("\x1b[?{};{}R", r + 1, c + 1).into_bytes()
            }
            Self::Secondary => b"\x1b[>0;1;0c".to_vec(),
            Self::Size => {
                let (r, c) = screen.size();
                format!("\x1b[8;{r};{c}t").into_bytes()
            }
            Self::Foreground => b"\x1b]10;rgb:ffff/ffff/ffff\x1b\\".to_vec(),
            Self::Background => b"\x1b]11;rgb:0000/0000/0000\x1b\\".to_vec(),
        }
    }
}
impl vte::Perform for Queries {
    fn csi_dispatch(
        &mut self,
        params: &vte::Params,
        intermediates: &[u8],
        ignore: bool,
        action: char,
    ) {
        if ignore {
            return;
        }
        let first = params
            .iter()
            .next()
            .and_then(|p| p.first())
            .copied()
            .unwrap_or(0);
        self.query = match (action, first, intermediates) {
            // The runtime screen already answers primary attributes, status,
            // and ordinary cursor reports before publishing output. Answering
            // again here injects duplicate input; ConPTY can interpret a second
            // cursor report as F3 and recall the previous input line.
            ('n', 6, b"?") => Some(Query::PrivateCursor),
            ('c', 0, b">") => Some(Query::Secondary),
            ('t', 18, b"") => Some(Query::Size),
            _ => None,
        };
    }
    fn osc_dispatch(&mut self, params: &[&[u8]], _bell: bool) {
        self.query = match params {
            [b"10", b"?"] => Some(Query::Foreground),
            [b"11", b"?"] => Some(Query::Background),
            _ => None,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wheel(button: MouseButtonDto) -> TerminalInputDto {
        TerminalInputDto::Mouse {
            event: undercurrent_protocol::MouseEventDto {
                row: 1,
                column: 1,
                button,
                event: MouseEventKindDto::Press,
            },
        }
    }

    fn history_key(base: ModifiableKeyDto) -> TerminalInputDto {
        TerminalInputDto::Key {
            key: KeyDto::Modified {
                base,
                modifiers: undercurrent_protocol::KeyModifiersDto {
                    shift: true,
                    ..Default::default()
                },
            },
        }
    }

    fn history_renderer() -> Renderer {
        let mut view = Renderer::new(TerminalSizeDto {
            columns: 80,
            rows: 3,
        });
        for index in 0..10 {
            view.process(format!("line {index}\r\n").as_bytes());
        }
        view
    }

    fn rendered(view: &mut Renderer) -> vt100::Screen {
        let mut bytes = Vec::new();
        view.invalidate();
        view.draw(&mut bytes, "attached").unwrap();
        let mut outer = vt100::Parser::new(view.size.rows + 1, view.size.columns, 0);
        outer.process(&bytes);
        outer.screen().clone()
    }

    #[test]
    fn top_anchored_transcript_region_retains_history_above_a_fixed_composer() {
        let mut view = Renderer::new(TerminalSizeDto {
            rows: 6,
            columns: 80,
        });
        view.process(b"before application\r\n\x1b[5;1Hcomposer\x1b[6;1Hstatus");
        // Codex writes completed transcript rows through a top-anchored DECSTBM
        // region while keeping the composer below it. No mouse capture is enabled.
        view.process(b"\x1b[1;4r\x1b[H");
        for index in 0..12 {
            view.process(format!("transcript {index}\r\n").as_bytes());
        }
        view.process(b"\x1b[r");
        let live = view.parser.screen().contents();
        assert!(live.contains("composer"));
        assert!(live.contains("status"));
        assert!(view.history_input(&wheel(MouseButtonDto::ScrollUp)));
        let history = rendered(&mut view).contents();
        assert!(history.contains("transcript 6"), "{history}");
        assert!(!history.contains("before application"));
        assert!(view.history_input(&history_key(ModifiableKeyDto::End)));
        assert_eq!(view.parser.screen().contents(), live);
    }

    #[test]
    fn interior_and_alternate_regions_do_not_pollute_primary_history() {
        for prefix in [b"\x1b[2;4r".as_slice(), b"\x1b[?1049h\x1b[1;4r"] {
            let mut view = Renderer::new(TerminalSizeDto {
                rows: 6,
                columns: 80,
            });
            view.process(b"shell");
            view.process(prefix);
            for _ in 0..12 {
                view.process(b"transient\r\n");
            }
            view.process(b"\x1b[?1049l\x1b[r");
            view.history_input(&wheel(MouseButtonDto::ScrollUp));
            assert_eq!(view.parser.screen().scrollback(), 0);
        }
    }

    #[test]
    fn wheel_scrolls_multiple_pages_and_freezes_history_while_output_redraws() {
        let mut view = history_renderer();
        assert!(view.history_input(&wheel(MouseButtonDto::ScrollUp)));
        assert!(view.history_input(&wheel(MouseButtonDto::ScrollUp)));
        let before = rendered(&mut view);
        assert_eq!(before.rows(0, 80).next().unwrap().trim_end(), "line 2");
        assert!(before.hide_cursor());
        // Queries must still report the live cursor, not the historical view.
        let replies = view.process(b"new output\r\n\x1b[Hredrawn\x1b[?6n");
        assert_eq!(replies, b"\x1b[?1;8R");
        let after = rendered(&mut view);
        assert_eq!(
            before.rows(0, 80).take(3).collect::<Vec<_>>(),
            after.rows(0, 80).take(3).collect::<Vec<_>>()
        );
        assert!(view.history_input(&history_key(ModifiableKeyDto::End)));
        let live = rendered(&mut view);
        assert!(live.contents().contains("redrawn"));
        assert!(!live.hide_cursor());
    }

    #[test]
    fn redraws_do_not_create_history_and_checkpoint_history_is_explicitly_partial() {
        let mut view = Renderer::new(TerminalSizeDto {
            columns: 80,
            rows: 3,
        });
        for index in 0..100 {
            view.process(format!("\x1b[Hanswer {index}\x1b[K").as_bytes());
        }
        view.history_input(&history_key(ModifiableKeyDto::PageUp));
        assert_eq!(view.parser.screen().scrollback(), 0);
        assert!(view.history_view.is_none());
        assert!(rendered(&mut view).contents().contains("answer 99"));
        view.reset(b"restored viewport", false);
        view.history_input(&history_key(ModifiableKeyDto::PageUp));
        assert!(
            rendered(&mut view)
                .contents()
                .contains("earlier history unavailable")
        );
    }

    #[test]
    fn mouse_capture_and_alternate_screen_keep_application_scrolling() {
        let mut view = history_renderer();
        view.process(b"\x1b[?1000h\x1b[?1006h");
        let up = wheel(MouseButtonDto::ScrollUp);
        assert!(!view.history_input(&up));
        assert!(
            matches!(view.input(up.clone()), Some(TerminalInputDto::Raw { bytes }) if bytes == b"\x1b[<64;1;1M")
        );
        assert!(view.history_input(&history_key(ModifiableKeyDto::PageUp)));
        assert!(view.history_input(&up));
        view.process(b"\x1b[?1049h\x1b[Happlication");
        assert!(view.history_view.is_none());
        assert!(!view.history_input(&up));
        assert!(!view.history_input(&TerminalInputDto::Key {
            key: KeyDto::PageUp
        }));
        view.history_input(&history_key(ModifiableKeyDto::PageUp));
        assert!(
            rendered(&mut view)
                .contents()
                .contains("application controls history")
        );
        view.process(b"\x1b[?1049l");
        view.history_input(&history_key(ModifiableKeyDto::PageUp));
        assert!(view.history_view.is_some());
    }

    #[test]
    fn wrapped_history_pages_back_to_live_and_typing_exits_history() {
        let mut view = Renderer::new(TerminalSizeDto {
            columns: 4,
            rows: 2,
        });
        view.process(b"abcdEFGHijkl");
        assert!(view.history_input(&history_key(ModifiableKeyDto::PageUp)));
        assert_eq!(rendered(&mut view).rows(0, 4).next().unwrap(), "abcd");
        assert!(view.history_input(&TerminalInputDto::Key {
            key: KeyDto::PageDown
        }));
        assert!(view.history_view.is_none());
        assert_eq!(rendered(&mut view).rows(0, 4).next().unwrap(), "EFGH");
        view.history_input(&history_key(ModifiableKeyDto::PageUp));
        let text = TerminalInputDto::Text { text: "x".into() };
        assert!(!view.history_input(&text));
        assert!(view.history_view.is_none());
        assert!(view.input(text.clone()) == Some(text));
    }

    #[test]
    fn history_ignores_mouse_clicks_and_clears_on_a_terminal_reset() {
        let mut view = history_renderer();
        view.process(b"\x1b[?1003h\x1b[?1006h");
        view.history_input(&history_key(ModifiableKeyDto::PageUp));
        for event in [
            MouseEventKindDto::Press,
            MouseEventKindDto::Release,
            MouseEventKindDto::Move,
        ] {
            assert!(view.history_input(&TerminalInputDto::Mouse {
                event: undercurrent_protocol::MouseEventDto {
                    row: 1,
                    column: 1,
                    button: MouseButtonDto::Left,
                    event,
                },
            }));
        }
        view.process(b"\x1bcnew screen");
        assert!(view.history_view.is_none());
        assert_eq!(
            rendered(&mut view).rows(0, 80).next().unwrap().trim_end(),
            "new screen"
        );
    }

    #[test]
    fn configured_capacity_survives_reset_and_zero_disables_history() {
        for memory_lines in [0, 2, 17] {
            let mut view = Renderer::with_scrollback(
                TerminalSizeDto {
                    rows: 3,
                    columns: 80,
                },
                undercurrent_config::ScrollbackConfig { memory_lines },
            );
            for reset in [false, true] {
                if reset {
                    view.reset(b"", false);
                }
                for _ in 0..30 {
                    view.process(b"line\r\n");
                }
                view.parser.set_scrollback(usize::MAX);
                assert_eq!(view.parser.screen().scrollback(), memory_lines);
            }
        }
        eprintln!("vt100 cell bytes: {}", std::mem::size_of::<vt100::Cell>());
    }

    #[test]
    fn history_is_bounded_and_resizing_paints_all_columns() {
        let mut view = history_renderer();
        for _ in 0..SCROLLBACK_ROWS + 5 {
            view.process("界e\u{301}\r\n".as_bytes());
        }
        view.parser.set_scrollback(usize::MAX);
        assert_eq!(view.parser.screen().scrollback(), SCROLLBACK_ROWS);
        view.refresh_history_view();
        assert_eq!(view.parser.screen().rows(0, 80).count(), 3);
        for columns in [2, 90] {
            view.resize(TerminalSizeDto { rows: 5, columns });
            let screen = rendered(&mut view);
            assert!(screen.cell(0, columns - 1).is_some());
            assert_eq!(view.parser.screen().rows(0, columns).count(), 5);
        }
        view.reset(b"reconnected", false);
        view.history_input(&history_key(ModifiableKeyDto::PageUp));
        assert_eq!(view.parser.screen().scrollback(), 0);
    }

    #[test]
    fn status_background_fills_the_width_after_shorter_text_and_resize() {
        let mut view = Renderer::new(TerminalSizeDto {
            columns: 40,
            rows: 4,
        });
        let mut outer = vt100::Parser::new(5, 40, 0);
        for (columns, bar) in [
            (40, "uc | group | [1:界] | help"),
            (40, "uc | group"),
            (60, "uc | group"),
        ] {
            if view.size.columns != columns {
                view.resize(TerminalSizeDto { columns, rows: 4 });
            }
            outer.set_size(5, columns);
            let mut output = Vec::new();
            view.draw(&mut output, bar).unwrap();
            outer.process(&output);
            assert_eq!(
                outer.screen().rows(0, columns).last().unwrap().trim_end(),
                bar
            );
            // Erased blanks may lose reverse video in real terminals, even
            // when the virtual terminal model retains that attribute.
            assert_eq!(outer.screen().cell(4, columns - 1).unwrap().contents(), " ");
            for column in 0..columns {
                let cell = outer.screen().cell(4, column).unwrap();
                assert!(
                    cell.is_wide_continuation() || cell.inverse(),
                    "status background missing at column {column} of {columns}"
                );
            }
        }
    }

    #[test]
    fn hostile_escape_sequences_cannot_erase_status_and_unicode_fits() {
        let mut view = Renderer::new(TerminalSizeDto {
            columns: 20,
            rows: 4,
        });
        view.process(b"\x1b[2J\x1b[999;1Hoverflow\x1b]52;c;clipboard\x07");
        let mut bytes = Vec::new();
        view.draw(&mut bytes, "uc | group").unwrap();
        let mut outer = vt100::Parser::new(5, 20, 0);
        outer.process(&bytes);
        assert_eq!(
            outer.screen().rows(0, 20).last().unwrap().trim_end(),
            "uc | group"
        );
        assert!(!bytes.windows(4).any(|b| b == b"]52;"));
        assert_eq!(fit("界abc", 3), "界a");
        view.process(b"\x1b[?1049hALT\x1b[?1049l");
        bytes.clear();
        view.draw(&mut bytes, "uc | group").unwrap();
        outer.process(&bytes);
        assert_eq!(
            outer.screen().rows(0, 20).last().unwrap().trim_end(),
            "uc | group"
        );
    }
    #[test]
    fn mouse_encoding_follows_child_mode_and_excludes_status_row() {
        use undercurrent_protocol::{MouseButtonDto, MouseEventDto, MouseEventKindDto};
        let mut view = Renderer::new(TerminalSizeDto {
            columns: 80,
            rows: 23,
        });
        let mut event = MouseEventDto {
            column: 10,
            row: 5,
            button: MouseButtonDto::Left,
            event: MouseEventKindDto::Press,
        };
        assert!(view.input(TerminalInputDto::Mouse { event }).is_none());
        view.process(b"\x1b[?1000h\x1b[?1006h");
        assert!(
            matches!(view.input(TerminalInputDto::Mouse { event }), Some(TerminalInputDto::Raw { bytes }) if bytes == b"\x1b[<0;10;5M")
        );
        event.row = 24;
        assert!(view.input(TerminalInputDto::Mouse { event }).is_none());
        event.row = 5;
        view.process(b"\x1b[?1006l");
        assert!(
            matches!(view.input(TerminalInputDto::Mouse { event }), Some(TerminalInputDto::Raw { bytes }) if bytes == [27, 91, 77, 32, 42, 37])
        );
    }

    #[test]
    fn runtime_owned_queries_are_not_answered_again_by_attached_views() {
        for _ in 0..2 {
            let mut view = Renderer::new(TerminalSizeDto {
                columns: 80,
                rows: 23,
            });
            // Include split requests and later cursor movement, as delivered
            // by live streams and replay to multiple attached views.
            for byte in b"\x1b[c\x1b[0c\x1b[5n\x1b[3;4H\x1b[6n\x1b[8;9H" {
                assert!(view.process(&[*byte]).is_empty());
            }
            assert_eq!(view.parser.screen().cursor_position(), (7, 8));
            assert_eq!(view.process(b"\x1b[?6n"), b"\x1b[?8;9R");
        }
    }

    #[test]
    fn queries_and_application_input_use_child_geometry_and_modes() {
        let mut view = Renderer::new(TerminalSizeDto {
            columns: 80,
            rows: 23,
        });
        assert_eq!(view.process(b"\x1b[3;4H\x1b[?6n"), b"\x1b[?3;4R");
        assert_eq!(view.process(b"\x1b[18t"), b"\x1b[8;23;80t");
        view.process(b"\x1b[?1h\x1b[?2004h");
        assert!(
            matches!(view.input(TerminalInputDto::Key{key:KeyDto::Up}),Some(TerminalInputDto::Raw{bytes}) if bytes==b"\x1bOA")
        );
        assert!(
            matches!(view.input(TerminalInputDto::Paste{text:"x".into()}),Some(TerminalInputDto::Raw{bytes}) if bytes==b"\x1b[200~x\x1b[201~")
        );
    }
}
