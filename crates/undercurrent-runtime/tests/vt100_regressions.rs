// SPDX-License-Identifier: Apache-2.0

use vt100::{Cell, Parser, Screen};
use vt100_reference as reference;

fn contents_at(screen: &Screen, row: u16, col: u16) -> String {
    screen
        .cell(row, col)
        .expect("cell must be in bounds")
        .contents()
}

#[derive(Debug, PartialEq, Eq)]
struct CellSnapshot {
    contents: String,
    has_contents: bool,
    is_wide: bool,
    is_wide_continuation: bool,
    foreground: String,
    background: String,
    bold: bool,
    italic: bool,
    underline: bool,
    inverse: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct ScreenSnapshot {
    contents: String,
    contents_formatted: Vec<u8>,
    state_formatted: Vec<u8>,
    input_mode_formatted: Vec<u8>,
    title_formatted: Vec<u8>,
    attributes_formatted: Vec<u8>,
    cursor_state_formatted: Vec<u8>,
    cursor: (u16, u16),
    size: (u16, u16),
    scrollback: usize,
    title: String,
    icon_name: String,
    audible_bells: usize,
    visual_bells: usize,
    errors: usize,
    alternate: bool,
    application_keypad: bool,
    application_cursor: bool,
    hidden_cursor: bool,
    bracketed_paste: bool,
    mouse_mode: String,
    mouse_encoding: String,
    foreground: String,
    background: String,
    bold: bool,
    italic: bool,
    underline: bool,
    inverse: bool,
    row_wrapped: Vec<bool>,
    cells: Vec<Option<CellSnapshot>>,
}

fn reference_snapshot(parser: &reference::Parser) -> ScreenSnapshot {
    let screen = parser.screen();
    let (rows, cols) = screen.size();
    let mut cells = Vec::with_capacity(usize::from(rows) * usize::from(cols));
    for row in 0..rows {
        for col in 0..cols {
            cells.push(screen.cell(row, col).map(|cell| CellSnapshot {
                contents: cell.contents(),
                has_contents: cell.has_contents(),
                is_wide: cell.is_wide(),
                is_wide_continuation: cell.is_wide_continuation(),
                foreground: format!("{:?}", cell.fgcolor()),
                background: format!("{:?}", cell.bgcolor()),
                bold: cell.bold(),
                italic: cell.italic(),
                underline: cell.underline(),
                inverse: cell.inverse(),
            }));
        }
    }
    ScreenSnapshot {
        contents: screen.contents(),
        contents_formatted: screen.contents_formatted(),
        state_formatted: screen.state_formatted(),
        input_mode_formatted: screen.input_mode_formatted(),
        title_formatted: screen.title_formatted(),
        attributes_formatted: screen.attributes_formatted(),
        cursor_state_formatted: screen.cursor_state_formatted(),
        cursor: screen.cursor_position(),
        size: screen.size(),
        scrollback: screen.scrollback(),
        title: screen.title().to_owned(),
        icon_name: screen.icon_name().to_owned(),
        audible_bells: screen.audible_bell_count(),
        visual_bells: screen.visual_bell_count(),
        errors: screen.errors(),
        alternate: screen.alternate_screen(),
        application_keypad: screen.application_keypad(),
        application_cursor: screen.application_cursor(),
        hidden_cursor: screen.hide_cursor(),
        bracketed_paste: screen.bracketed_paste(),
        mouse_mode: format!("{:?}", screen.mouse_protocol_mode()),
        mouse_encoding: format!("{:?}", screen.mouse_protocol_encoding()),
        foreground: format!("{:?}", screen.fgcolor()),
        background: format!("{:?}", screen.bgcolor()),
        bold: screen.bold(),
        italic: screen.italic(),
        underline: screen.underline(),
        inverse: screen.inverse(),
        row_wrapped: (0..rows).map(|row| screen.row_wrapped(row)).collect(),
        cells,
    }
}

fn vendored_snapshot(parser: &Parser) -> ScreenSnapshot {
    let screen = parser.screen();
    let (rows, cols) = screen.size();
    let mut cells = Vec::with_capacity(usize::from(rows) * usize::from(cols));
    for row in 0..rows {
        for col in 0..cols {
            cells.push(screen.cell(row, col).map(|cell| CellSnapshot {
                contents: cell.contents(),
                has_contents: cell.has_contents(),
                is_wide: cell.is_wide(),
                is_wide_continuation: cell.is_wide_continuation(),
                foreground: format!("{:?}", cell.fgcolor()),
                background: format!("{:?}", cell.bgcolor()),
                bold: cell.bold(),
                italic: cell.italic(),
                underline: cell.underline(),
                inverse: cell.inverse(),
            }));
        }
    }
    ScreenSnapshot {
        contents: screen.contents(),
        contents_formatted: screen.contents_formatted(),
        state_formatted: screen.state_formatted(),
        input_mode_formatted: screen.input_mode_formatted(),
        title_formatted: screen.title_formatted(),
        attributes_formatted: screen.attributes_formatted(),
        cursor_state_formatted: screen.cursor_state_formatted(),
        cursor: screen.cursor_position(),
        size: screen.size(),
        scrollback: screen.scrollback(),
        title: screen.title().to_owned(),
        icon_name: screen.icon_name().to_owned(),
        audible_bells: screen.audible_bell_count(),
        visual_bells: screen.visual_bell_count(),
        errors: screen.errors(),
        alternate: screen.alternate_screen(),
        application_keypad: screen.application_keypad(),
        application_cursor: screen.application_cursor(),
        hidden_cursor: screen.hide_cursor(),
        bracketed_paste: screen.bracketed_paste(),
        mouse_mode: format!("{:?}", screen.mouse_protocol_mode()),
        mouse_encoding: format!("{:?}", screen.mouse_protocol_encoding()),
        foreground: format!("{:?}", screen.fgcolor()),
        background: format!("{:?}", screen.bgcolor()),
        bold: screen.bold(),
        italic: screen.italic(),
        underline: screen.underline(),
        inverse: screen.inverse(),
        row_wrapped: (0..rows).map(|row| screen.row_wrapped(row)).collect(),
        cells,
    }
}

fn chunks<'a>(bytes: &'a [u8], pattern: &[usize]) -> Vec<&'a [u8]> {
    let mut result = Vec::new();
    let mut offset = 0;
    let mut index = 0;
    while offset < bytes.len() {
        let length = pattern[index % pattern.len()].min(bytes.len() - offset);
        result.push(&bytes[offset..offset + length]);
        offset += length;
        index += 1;
    }
    result
}

fn compare_with_reference(
    name: &str,
    rows: u16,
    cols: u16,
    scrollback_capacity: usize,
    bytes: &[u8],
    pattern: &[usize],
    view_scrollback: Option<usize>,
) {
    let mut reference = reference::Parser::new(rows, cols, scrollback_capacity);
    let mut vendored = Parser::new(rows, cols, scrollback_capacity);
    for chunk in chunks(bytes, pattern) {
        reference.process(chunk);
        vendored.process(chunk);
    }
    assert_eq!(
        reference_snapshot(&reference),
        vendored_snapshot(&vendored),
        "{name} at scrollback zero"
    );
    if let Some(scrollback) = view_scrollback {
        reference.set_scrollback(scrollback);
        vendored.set_scrollback(scrollback);
        assert_eq!(
            reference.screen().scrollback(),
            scrollback,
            "{name} did not establish the required reference scrollback"
        );
        assert_eq!(
            reference_snapshot(&reference),
            vendored_snapshot(&vendored),
            "{name} at scrollback {scrollback}"
        );
    }
}

#[test]
fn recycled_rows_reset_cells_and_wrapped_state() {
    let mut parser = Parser::new(2, 3, 0);
    parser.process(b"\x1b[31mabc");

    for col in 0..3 {
        let cell = parser.screen().cell(0, col).expect("cell must exist");
        assert!(!cell.is_wide());
        assert!(!cell.is_wide_continuation());
    }
    assert!(!parser.screen().row_wrapped(0));

    parser.process(b"\x1b[0md");
    assert!(parser.screen().row_wrapped(0));
    assert_eq!(contents_at(parser.screen(), 1, 0), "d");

    parser.process(b"efg");
    assert_eq!(contents_at(parser.screen(), 0, 0), "d");
    assert_eq!(contents_at(parser.screen(), 0, 1), "e");
    assert_eq!(contents_at(parser.screen(), 0, 2), "f");
    assert_eq!(contents_at(parser.screen(), 1, 0), "g");
    assert!(!parser.screen().row_wrapped(1));
    for col in 1..3 {
        assert_eq!(
            format!(
                "{:?}",
                parser.screen().cell(1, col).expect("cell must exist")
            ),
            format!("{:?}", Cell::default())
        );
    }
}

#[test]
fn wide_overwrites_and_alternate_grid_preserve_primary_state() {
    let mut parser = Parser::new(2, 6, 0);
    parser.process(b"main");
    let primary_cursor = parser.screen().cursor_position();

    parser.process("\x1b[?1049h界界".as_bytes());
    assert!(
        parser
            .screen()
            .cell(0, 0)
            .expect("cell must exist")
            .is_wide()
    );
    assert!(
        parser
            .screen()
            .cell(0, 1)
            .expect("cell must exist")
            .is_wide_continuation()
    );

    parser.process(b"\x1b[1;2Hx\x1b[1;3Hy");
    assert_eq!(contents_at(parser.screen(), 0, 0), "");
    assert_eq!(contents_at(parser.screen(), 0, 1), "x");
    assert_eq!(contents_at(parser.screen(), 0, 2), "y");
    assert_eq!(contents_at(parser.screen(), 0, 3), " ");

    parser.process(b"\x1b[?1049l");
    assert_eq!(parser.screen().contents(), "main");
    assert_eq!(parser.screen().cursor_position(), primary_cursor);
}

#[test]
fn scrollback_view_stays_stable_while_output_advances() {
    let mut parser = Parser::new(2, 3, 2);
    parser.process(b"aaa\r\nbbb\r\nccc");
    assert_eq!(parser.screen().contents(), "bbb\nccc");

    parser.set_scrollback(1);
    assert_eq!(parser.screen().contents(), "aaa\nbbb");

    parser.process(b"\r\nddd");
    assert_eq!(parser.screen().scrollback(), 2);
    assert_eq!(parser.screen().contents(), "aaa\nbbb");

    parser.set_scrollback(0);
    assert_eq!(parser.screen().contents(), "ccc\nddd");
}

#[test]
fn vendored_hotpaths_match_registry_vt100_full_public_state() {
    let partial_scroll = b"h0\r\nh1\r\nh2\r\nh3\r\nh4\r\nh5\r\nh6\r\nh7\
\x1b[H\x1b[2J\
\x1b[1;1HTOP-OUTSIDE\
\x1b[2;1HINNER-A\
\x1b[3;1HINNER-B\
\x1b[4;1HINNER-C\
\x1b[5;1HINNER-D\
\x1b[6;1HBELOW-OUTSIDE\
\x1b[2;5r\x1b[5;1H\r\nNEW-INNER";
    let combining = "e\u{301} normal\x1b[2;1H12345678A\x08\u{301}\x1b[4;4Hz\u{301}".as_bytes();
    let wide_final_columns = "\x1b[1;7H界\x1b[1;8Hx\x1b[2;8H界\x1b[3;7H界\x1b[3;7Hy\
\x1b[4;7H界\x1b[4;8Hz"
        .as_bytes();
    let patterns: &[(&str, &[usize])] = &[
        ("whole", &[usize::MAX]),
        ("bytewise", &[1]),
        ("ragged", &[2, 3, 5, 1, 8]),
        ("escape-boundaries", &[7, 1, 4, 2, 11]),
    ];

    for (pattern_name, pattern) in patterns {
        compare_with_reference(
            &format!("partial-scroll/no-history/{pattern_name}"),
            6,
            14,
            0,
            partial_scroll,
            pattern,
            None,
        );
        compare_with_reference(
            &format!("partial-scroll/history-view/{pattern_name}"),
            6,
            14,
            8,
            partial_scroll,
            pattern,
            Some(2),
        );
        compare_with_reference(
            &format!("combining/{pattern_name}"),
            4,
            8,
            0,
            combining,
            pattern,
            None,
        );
        compare_with_reference(
            &format!("wide-final-columns/{pattern_name}"),
            5,
            8,
            0,
            wide_final_columns,
            pattern,
            None,
        );
    }
}
