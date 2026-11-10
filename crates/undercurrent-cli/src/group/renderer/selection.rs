// SPDX-License-Identifier: Apache-2.0

//! A frozen, bounded copy view, independent of incoming application output.

use super::*;

struct Line {
    cells: Vec<vt100::Cell>,
    wrapped: bool,
}

pub(super) struct Selection {
    lines: Vec<Line>,
    pub top: usize,
    pub cursor: (usize, u16),
    anchor: Option<(usize, u16)>,
    linewise: bool,
    copied: bool,
}

pub(super) enum SelectionResult {
    Continue,
    Cancel,
    Copy(String),
}

impl Selection {
    pub fn capture(parser: &mut vt100::Parser) -> Self {
        let original = parser.screen().scrollback();
        let cursor = parser.screen().cursor_position();
        let (rows, columns) = parser.screen().size();
        parser.set_scrollback(usize::MAX);
        let history = parser.screen().scrollback();
        let total = history + usize::from(rows);
        let mut lines = Vec::with_capacity(total);
        while lines.len() < total {
            let start = lines.len();
            parser.set_scrollback(history.saturating_sub(start));
            for row in start.saturating_sub(history) as u16..rows {
                lines.push(Line {
                    cells: (0..columns)
                        .filter_map(|col| parser.screen().cell(row, col).cloned())
                        .collect(),
                    wrapped: parser.screen().row_wrapped(row),
                });
            }
        }
        parser.set_scrollback(original);
        let top = history.saturating_sub(original);
        let mut selection = Self {
            lines,
            top,
            cursor: (
                top + usize::from(cursor.0.min(rows - 1)),
                cursor.1.min(columns - 1),
            ),
            anchor: None,
            linewise: false,
            copied: false,
        };
        selection.clamp(TerminalSizeDto { rows, columns });
        selection
    }

    pub fn cell(&self, row: u16, col: u16) -> Option<&vt100::Cell> {
        self.lines
            .get(self.top + usize::from(row))?
            .cells
            .get(usize::from(col))
    }

    fn bounds(&self) -> Option<((usize, u16), (usize, u16))> {
        let anchor = self.anchor?;
        Some((anchor.min(self.cursor), anchor.max(self.cursor)))
    }

    pub fn highlighted(&self, row: u16, col: u16) -> bool {
        let point = (self.top + usize::from(row), col);
        self.bounds().is_some_and(|(start, end)| {
            if self.linewise {
                (start.0..=end.0).contains(&point.0)
            } else {
                start <= point && point <= end
            }
        })
    }

    pub fn bar(&self) -> &'static str {
        if self.copied {
            "UC · COPY · copied · drag to select again · Esc: live"
        } else if self.anchor.is_some() {
            "UC · VISUAL · arrows/hjkl: move · y/Enter: copy · Esc: cancel"
        } else {
            "UC · COPY · arrows/hjkl: move · v/Space: select · V: line · Esc: live"
        }
    }

    fn text(&self) -> String {
        let Some((start, end)) = self.bounds() else {
            return String::new();
        };
        let mut text = String::new();
        for row in start.0..=end.0 {
            let line = &self.lines[row];
            let first = if self.linewise || row != start.0 {
                0
            } else {
                usize::from(start.1)
            };
            let last = if self.linewise || row != end.0 {
                line.cells.len()
            } else {
                usize::from(end.1) + 1
            };
            let mut segment = String::new();
            for cell in line.cells.iter().take(last).skip(first) {
                if !cell.is_wide_continuation() {
                    let contents = cell.contents();
                    segment.push_str(if contents.is_empty() { " " } else { &contents });
                }
            }
            text.push_str(if row != end.0 && line.wrapped && !self.linewise {
                &segment
            } else {
                segment.trim_end_matches(' ')
            });
            if row != end.0 && (self.linewise || !line.wrapped) {
                text.push('\n');
            }
        }
        text
    }

    pub fn clamp(&mut self, size: TerminalSizeDto) {
        self.cursor.0 = self.cursor.0.min(self.lines.len() - 1);
        self.cursor.1 = self
            .cursor
            .1
            .min(size.columns - 1)
            .min(self.lines[self.cursor.0].cells.len().saturating_sub(1) as u16);
        if self.lines[self.cursor.0].cells[usize::from(self.cursor.1)].is_wide_continuation() {
            self.cursor.1 = self.cursor.1.saturating_sub(1);
        }
        self.top = self
            .top
            .min(self.cursor.0)
            .max(self.cursor.0.saturating_sub(usize::from(size.rows - 1)));
    }

    pub fn input(&mut self, input: &TerminalInputDto, size: TerminalSizeDto) -> SelectionResult {
        self.copied = false;
        let mut copy = false;
        match input {
            TerminalInputDto::Key {
                key: KeyDto::Escape,
            } => return SelectionResult::Cancel,
            TerminalInputDto::Text { text } if text == "q" => return SelectionResult::Cancel,
            TerminalInputDto::Text { text } if text == "v" || text == "V" => {
                self.anchor = if self.anchor.is_some() {
                    None
                } else {
                    Some(self.cursor)
                };
                self.linewise = text == "V";
            }
            TerminalInputDto::Key { key: KeyDto::Space } => {
                self.anchor = Some(self.cursor);
                self.linewise = false;
            }
            TerminalInputDto::Text { text } if text == "y" => copy = true,
            TerminalInputDto::Key { key: KeyDto::Enter } => copy = true,
            TerminalInputDto::Mouse { event } => {
                if event.row == 0
                    || event.column == 0
                    || event.row > size.rows
                    || event.column > size.columns
                {
                    return SelectionResult::Continue;
                }
                match event.button {
                    MouseButtonDto::ScrollUp | MouseButtonDto::ScrollDown => {
                        // Wheel scrolling moves the viewport immediately, rather
                        // than walking the cursor to its edge first.
                        let maximum = self.lines.len().saturating_sub(usize::from(size.rows));
                        let top = if event.button == MouseButtonDto::ScrollUp {
                            self.top.saturating_sub(3)
                        } else {
                            self.top.saturating_add(3).min(maximum)
                        };
                        self.cursor.0 = top + self.cursor.0.saturating_sub(self.top);
                        self.top = top;
                    }
                    MouseButtonDto::Left => {
                        self.cursor = (self.top + usize::from(event.row - 1), event.column - 1);
                        self.clamp(size);
                        if event.event == MouseEventKindDto::Press {
                            self.anchor = Some(self.cursor);
                            self.linewise = false;
                        }
                        copy = event.event == MouseEventKindDto::Release;
                    }
                    _ => {}
                }
            }
            _ => {
                let movement = match input {
                    TerminalInputDto::Text { text } => text.as_str(),
                    TerminalInputDto::Key { key } => match key {
                        KeyDto::Up => "k",
                        KeyDto::Down => "j",
                        KeyDto::Left => "h",
                        KeyDto::Right => "l",
                        KeyDto::Home => "0",
                        KeyDto::End => "$",
                        KeyDto::PageUp => "page-up",
                        KeyDto::PageDown => "page-down",
                        KeyDto::Modified {
                            base: ModifiableKeyDto::PageUp,
                            ..
                        } => "page-up",
                        KeyDto::Modified {
                            base: ModifiableKeyDto::PageDown,
                            ..
                        } => "page-down",
                        _ => "",
                    },
                    _ => "",
                };
                match movement {
                    "k" => self.cursor.0 = self.cursor.0.saturating_sub(1),
                    "j" => self.cursor.0 = self.cursor.0.saturating_add(1),
                    "h" => self.cursor.1 = self.cursor.1.saturating_sub(1),
                    "l" => {
                        self.cursor.1 = self.cursor.1.saturating_add(
                            if self.lines[self.cursor.0].cells[usize::from(self.cursor.1)].is_wide()
                            {
                                2
                            } else {
                                1
                            },
                        )
                    }
                    "0" => self.cursor.1 = 0,
                    "$" => {
                        self.cursor.1 =
                            self.lines[self.cursor.0].cells.len().saturating_sub(1) as u16
                    }
                    "g" => self.cursor = (0, 0),
                    "G" => self.cursor = (self.lines.len() - 1, 0),
                    "page-up" => {
                        self.cursor.0 = self.cursor.0.saturating_sub(usize::from(size.rows))
                    }
                    "page-down" => {
                        self.cursor.0 = self.cursor.0.saturating_add(usize::from(size.rows))
                    }
                    _ => {}
                }
            }
        }
        self.clamp(size);
        if copy && self.anchor.is_some() {
            self.copied = true;
            SelectionResult::Copy(self.text())
        } else {
            SelectionResult::Continue
        }
    }
}

/// Only explicit user copies reach the outer clipboard; child OSC sequences remain confined.
pub(crate) fn clipboard(writer: &mut impl Write, text: &str) -> Result<(), CliError> {
    use base64::Engine;
    let encoded = base64::engine::general_purpose::STANDARD.encode(text);
    write!(writer, "\x1b]52;c;{encoded}\x07")
        .and_then(|()| writer.flush())
        .map_err(CliOutputError::from)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(text: &str) -> TerminalInputDto {
        TerminalInputDto::Text { text: text.into() }
    }

    #[test]
    fn copy_is_frozen_across_output_and_highlights_only_the_selected_cells() {
        let mut view = Renderer::new(TerminalSizeDto {
            rows: 3,
            columns: 80,
        });
        view.process(b"hello world\r\nsecond\x1b[H");
        view.enter_copy();
        for input in ["v", "l", "l", "l", "l"] {
            assert!(view.history_input(&key(input)));
        }
        view.process(b"\x1b[2J\x1b[Hnew output");
        let mut frame = vec![];
        view.draw(&mut frame, "attached").unwrap();
        let mut outer = vt100::Parser::new(4, 80, 0);
        outer.process(&frame);
        assert!(outer.screen().contents().starts_with("hello world"));
        assert!(outer.screen().cell(0, 0).unwrap().inverse());
        assert!(outer.screen().cell(0, 4).unwrap().inverse());
        assert!(!outer.screen().cell(0, 5).unwrap().inverse());
        assert!(outer.screen().contents().contains("VISUAL"));
        view.history_input(&key("y"));
        assert_eq!(view.take_copy().as_deref(), Some("hello"));
        frame.clear();
        view.draw(&mut frame, "attached").unwrap();
        outer.process(&frame);
        assert!(outer.screen().contents().starts_with("hello world"));
        assert!(outer.screen().contents().contains("copied"));
        view.history_input(&TerminalInputDto::Key {
            key: KeyDto::Escape,
        });
        frame.clear();
        view.draw(&mut frame, "attached").unwrap();
        outer.process(&frame);
        assert!(outer.screen().contents().starts_with("new output"));
    }

    #[test]
    fn scrollback_copy_preserves_soft_wraps_spaces_unicode_and_hard_newlines() {
        let mut view = Renderer::new(TerminalSizeDto {
            rows: 2,
            columns: 6,
        });
        view.process("abc   界e\u{301}\r\nlast\r\n".as_bytes());
        view.enter_copy();
        for input in ["g", "v", "j", "j", "l", "l", "l", "y"] {
            view.history_input(&key(input));
        }
        assert_eq!(view.take_copy().as_deref(), Some("abc   界e\u{301}\nlast"));
    }

    #[test]
    fn wide_character_navigation_reverse_selection_and_resize_remain_valid() {
        let mut view = Renderer::new(TerminalSizeDto {
            rows: 2,
            columns: 10,
        });
        view.process("界ab\x1b[H".as_bytes());
        view.enter_copy();
        for input in ["l", "v", "h", "y"] {
            view.history_input(&key(input));
        }
        assert_eq!(view.take_copy().as_deref(), Some("界a"));
        view.enter_copy();
        view.resize(TerminalSizeDto {
            rows: 1,
            columns: 1,
        });
        for input in ["V", "y"] {
            view.history_input(&key(input));
        }
        assert_eq!(view.take_copy().as_deref(), Some("界ab"));
    }

    #[test]
    fn mouse_drag_copies_and_paste_is_consumed_in_copy_mode() {
        let mut view = Renderer::new(TerminalSizeDto {
            rows: 2,
            columns: 10,
        });
        view.process(b"hello\x1b[H");
        view.enter_copy();
        assert!(view.history_input(&TerminalInputDto::Paste {
            text: "do not send".into()
        }));
        for (column, event) in [
            (1, MouseEventKindDto::Press),
            (5, MouseEventKindDto::Move),
            (5, MouseEventKindDto::Release),
        ] {
            view.history_input(&TerminalInputDto::Mouse {
                event: undercurrent_protocol::MouseEventDto {
                    column,
                    row: 1,
                    button: MouseButtonDto::Left,
                    event,
                },
            });
        }
        assert_eq!(view.take_copy().as_deref(), Some("hello"));
        let mut bytes = vec![];
        clipboard(&mut bytes, "hello").unwrap();
        assert_eq!(bytes, b"\x1b]52;c;aGVsbG8=\x07");
    }

    fn mouse(
        column: u16,
        row: u16,
        button: MouseButtonDto,
        event: MouseEventKindDto,
    ) -> TerminalInputDto {
        TerminalInputDto::Mouse {
            event: undercurrent_protocol::MouseEventDto {
                column,
                row,
                button,
                event,
            },
        }
    }

    #[test]
    fn normal_drag_enters_copy_but_clicks_and_application_mouse_input_do_not() {
        let mut view = Renderer::new(TerminalSizeDto {
            rows: 3,
            columns: 30,
        });
        view.process(b"hello world\x1b[H");
        let press = mouse(1, 1, MouseButtonDto::Left, MouseEventKindDto::Press);
        let drag = mouse(5, 1, MouseButtonDto::Left, MouseEventKindDto::Move);
        let release = mouse(5, 1, MouseButtonDto::Left, MouseEventKindDto::Release);
        view.history_input(&press);
        view.history_input(&release);
        assert!(view.selection.is_none());
        view.history_input(&press);
        assert!(view.history_input(&drag));
        view.history_input(&release);
        assert_eq!(view.take_copy().as_deref(), Some("hello"));
        assert!(view.selection.is_some());
        for event in [
            MouseEventKindDto::Press,
            MouseEventKindDto::Move,
            MouseEventKindDto::Release,
        ] {
            view.history_input(&mouse(
                if event == MouseEventKindDto::Press {
                    7
                } else {
                    11
                },
                1,
                MouseButtonDto::Left,
                event,
            ));
        }
        assert_eq!(view.take_copy().as_deref(), Some("world"));
        view.reset(b"recovered live screen", false);
        assert_eq!(
            view.selection
                .as_ref()
                .unwrap()
                .cell(0, 0)
                .unwrap()
                .contents(),
            "h"
        );
        view.history_input(&TerminalInputDto::Key {
            key: KeyDto::Escape,
        });
        assert!(view.selection.is_none());
        assert!(
            view.parser
                .screen()
                .contents()
                .starts_with("recovered live")
        );
        view.process(b"\x1b[?1003h\x1b[?1006h");
        for item in [press, drag, release] {
            assert!(!view.history_input(&item));
            assert!(view.input(item).is_some());
        }
        assert!(view.selection.is_none());
    }

    #[test]
    fn copy_wheel_scrolls_the_viewport_immediately_and_stays_frozen() {
        let mut view = Renderer::new(TerminalSizeDto {
            rows: 4,
            columns: 30,
        });
        for line in 0..25 {
            view.process(format!("line {line}\r\n").as_bytes());
        }
        view.enter_copy();
        let before = view.selection.as_ref().unwrap().top;
        let wheel = mouse(2, 2, MouseButtonDto::ScrollUp, MouseEventKindDto::Press);
        assert!(view.history_input(&wheel));
        assert_eq!(view.selection.as_ref().unwrap().top, before - 3);
        let frozen = view.selection.as_ref().unwrap().cell(0, 0).cloned();
        view.process(b"\x1b[2J\x1b[Hnew live output");
        assert_eq!(view.selection.as_ref().unwrap().cell(0, 0), frozen.as_ref());
        for _ in 0..30 {
            view.history_input(&wheel);
        }
        assert_eq!(view.selection.as_ref().unwrap().top, 0);
        let down = mouse(2, 2, MouseButtonDto::ScrollDown, MouseEventKindDto::Press);
        for _ in 0..30 {
            view.history_input(&down);
        }
        assert!(view.selection.is_some());
        view.history_input(&TerminalInputDto::Key {
            key: KeyDto::Escape,
        });
        assert!(view.parser.screen().contents().contains("new live output"));
    }

    #[test]
    fn offset_renderer_confines_fullscreen_output_and_status_to_its_pane() {
        let mut view = Renderer::new(TerminalSizeDto {
            rows: 2,
            columns: 5,
        });
        view.process(b"\x1b[2J\x1b[H12345\r\nabcde\x1b]52;c;bad\x07");
        let mut bytes = vec![];
        view.draw_region(&mut bytes, "pane", 2, (6, 1), false)
            .unwrap();
        let mut outer = vt100::Parser::new(5, 12, 0);
        outer.process(b"left\r\nleft\r\nleft\r\nleft");
        outer.process(&bytes);
        let rows: Vec<_> = outer.screen().rows(0, 12).collect();
        assert_eq!(rows[1], "left  12345");
        assert_eq!(rows[2], "left  abcde");
        assert_eq!(rows[3].trim_end(), "left  pane");
        assert!(outer.screen().hide_cursor());
        assert!(!String::from_utf8(bytes).unwrap().contains("]52"));
    }
}
