// SPDX-License-Identifier: Apache-2.0

//! Finite binary terminal result presentation.

use std::io::Write;

use crate::cli_support::{CliOutputError, OutputFormat, OutputShape, Verbosity};
use serde::Serialize;
use undercurrent_protocol::{
    ResynchronizationDto, TerminalOutputPageDto, TerminalScreenSnapshotDto, TerminalScreenUpdateDto,
};

use crate::{CliError, CliPresenter};

/// Stable JSON Lines record for a finite page that seeds `logs --follow`.
#[derive(Serialize)]
#[serde(tag = "record", rename_all = "snake_case")]
pub enum CliTerminalOutputPageRecord<'a> {
    /// One retained terminal byte interval.
    TerminalOutput {
        /// Inclusive raw-output start offset.
        start_offset: u64,
        /// Exclusive raw-output end offset.
        end_offset: u64,
        /// Authoritative continuation offset.
        next_offset: u64,
        /// Whether more retained bytes existed when the page was read.
        has_more: bool,
        /// Exact terminal bytes.
        bytes: &'a [u8],
    },
    /// Explicit retained-output discontinuity and recovery instruction.
    TerminalOutputGap {
        /// Requested byte offset that is no longer retained.
        requested_offset: u64,
        /// First currently retained byte offset.
        available_from: u64,
        /// Authoritative continuation offset.
        next_offset: u64,
        /// Stable recovery action.
        recovery: ResynchronizationDto,
    },
}

impl<'a> From<&'a TerminalOutputPageDto> for CliTerminalOutputPageRecord<'a> {
    fn from(page: &'a TerminalOutputPageDto) -> Self {
        match page {
            TerminalOutputPageDto::Data {
                from_offset,
                to_offset,
                next_offset,
                has_more,
                bytes,
                ..
            } => Self::TerminalOutput {
                start_offset: *from_offset,
                end_offset: *to_offset,
                next_offset: *next_offset,
                has_more: *has_more,
                bytes,
            },
            TerminalOutputPageDto::Gap {
                requested_offset,
                available_from,
                next_offset,
                recovery,
            } => Self::TerminalOutputGap {
                requested_offset: *requested_offset,
                available_from: *available_from,
                next_offset: *next_offset,
                recovery: *recovery,
            },
        }
    }
}

impl CliPresenter {
    /// Writes one retained terminal-output page without decoding its bytes.
    ///
    /// Data pages are emitted byte-for-byte in text mode and as their typed DTO
    /// in JSON mode. Gap pages remain structured in JSON and receive an
    /// actionable payload-free text diagnostic. This method performs writer IO
    /// and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a finite-output shape, serialization, or writer failure.
    pub fn write_terminal_output_page<W>(
        self,
        writer: &mut W,
        page: &TerminalOutputPageDto,
    ) -> Result<(), CliError>
    where
        W: Write,
    {
        match (self.context.output, page) {
            (OutputFormat::Text, TerminalOutputPageDto::Data { bytes, .. }) => {
                self.write_raw_finite(writer, bytes)
            }
            _ => self.write_finite(writer, page, |_| terminal_output_page_text(page)),
        }
    }

    /// Writes the finite retained page that precedes a live terminal stream.
    ///
    /// Text mode preserves terminal data bytes exactly. JSON Lines emits one
    /// independently consumable typed record. Finite JSON is rejected because
    /// `--follow` has a stream output contract. This method performs writer IO
    /// and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a stream-output shape, serialization, or writer failure.
    pub fn write_terminal_output_stream_page<W>(
        self,
        writer: &mut W,
        page: &TerminalOutputPageDto,
    ) -> Result<(), CliError>
    where
        W: Write,
    {
        self.context.validate_output_shape(OutputShape::Stream)?;
        if self.context.output == OutputFormat::Text {
            if self.context.verbosity == Verbosity::Quiet {
                return Ok(());
            }
            return match page {
                TerminalOutputPageDto::Data { bytes, .. } => writer.write_all(bytes),
                TerminalOutputPageDto::Gap { .. } => {
                    writeln!(writer, "{}", terminal_output_page_text(page))
                }
            }
            .map_err(CliOutputError::from)
            .map_err(Into::into);
        }
        self.context
            .output
            .write(
                writer,
                &CliTerminalOutputPageRecord::from(page),
                String::new,
            )
            .map_err(Into::into)
    }

    /// Writes one complete terminal screen as plain text by default.
    ///
    /// Text mode reconstructs the screen without emitting terminal controls.
    /// Explicit raw mode preserves ANSI bytes; JSON retains typed metadata.
    /// Raw mode rejects structured output. This method
    /// performs writer IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a finite-output shape, raw-output mode, serialization, or writer failure.
    pub fn write_terminal_screen<W>(
        self,
        writer: &mut W,
        screen: &TerminalScreenSnapshotDto,
        raw: bool,
    ) -> Result<(), CliError>
    where
        W: Write,
    {
        if raw {
            self.write_raw_finite(writer, &screen.contents)
        } else {
            self.write_finite(writer, screen, |_| {
                let mut parser = vt100::Parser::new(screen.rows, screen.columns, 0);
                parser.process(&screen.contents);
                parser.screen().contents()
            })
        }
    }

    /// Writes one bounded terminal-screen update while preserving exact ANSI bytes.
    ///
    /// Text mode writes only the bytes that transform or replace the prior
    /// screen. JSON mode retains the typed diff-versus-snapshot discriminator.
    /// This method performs writer IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a finite-output shape, serialization, or writer failure.
    pub fn write_terminal_screen_update<W>(
        self,
        writer: &mut W,
        update: &TerminalScreenUpdateDto,
        raw: bool,
    ) -> Result<(), CliError>
    where
        W: Write,
    {
        if raw || self.context.output == OutputFormat::Text {
            self.write_raw_finite(writer, screen_update_bytes(update))
        } else {
            self.write_finite(writer, update, |_| String::new())
        }
    }
}

/// Renders one retained-output replay gap without exposing terminal bytes.
///
/// This function performs no IO and does not panic.
pub fn terminal_output_page_text(page: &TerminalOutputPageDto) -> String {
    match page {
        TerminalOutputPageDto::Data {
            from_offset,
            to_offset,
            has_more,
            ..
        } => format!(
            "terminal output bytes {from_offset}..{to_offset}{}",
            if *has_more { " (more available)" } else { "" }
        ),
        TerminalOutputPageDto::Gap {
            requested_offset,
            available_from,
            recovery,
            ..
        } => format!(
            "terminal output offset {requested_offset} is no longer retained; resume from {available_from} ({})",
            recovery_text(*recovery)
        ),
    }
}

fn screen_update_bytes(update: &TerminalScreenUpdateDto) -> &[u8] {
    match update {
        TerminalScreenUpdateDto::Diff(diff) => &diff.contents,
        TerminalScreenUpdateDto::Snapshot(snapshot) => &snapshot.contents,
    }
}

const fn recovery_text(recovery: ResynchronizationDto) -> &'static str {
    match recovery {
        ResynchronizationDto::RequestSnapshot => "request_snapshot",
        ResynchronizationDto::RestartQuery => "restart_query",
        ResynchronizationDto::RestartFromAvailableOffset => "restart_from_available_offset",
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::cli_support::{CliContext, CommandTimeout, OutputFormat, Verbosity};
    use undercurrent_protocol::TerminalScreenDiffDto;

    use super::*;

    #[test]
    fn text_terminal_pages_preserve_invalid_utf8_exactly() {
        let page = TerminalOutputPageDto::Data {
            from_offset: 4,
            to_offset: 7,
            next_offset: 7,
            has_more: false,
            bytes: vec![0xff, 0x00, b'x'],
            payload_digest_sha256: undercurrent_protocol::execution_output_payload_digest_sha256(
                &[0xff, 0x00, b'x'],
            ),
            truncation: None,
        };
        let mut output = Vec::new();

        presenter(OutputFormat::Text)
            .write_terminal_output_page(&mut output, &page)
            .expect("terminal output");

        assert_eq!(output, [0xff, 0x00, b'x']);
    }

    #[test]
    fn replay_gaps_are_actionable_in_text_and_structured_in_json() {
        let page = TerminalOutputPageDto::Gap {
            requested_offset: 10,
            available_from: 20,
            next_offset: 20,
            recovery: ResynchronizationDto::RestartFromAvailableOffset,
        };
        let mut text = Vec::new();
        presenter(OutputFormat::Text)
            .write_terminal_output_page(&mut text, &page)
            .expect("text gap");
        assert_eq!(
            String::from_utf8(text).expect("UTF-8"),
            "terminal output offset 10 is no longer retained; resume from 20 (restart_from_available_offset)\n"
        );

        let mut json = Vec::new();
        presenter(OutputFormat::Json)
            .write_terminal_output_page(&mut json, &page)
            .expect("JSON gap");
        let value: serde_json::Value = serde_json::from_slice(&json).expect("JSON");
        assert_eq!(value["result"], "gap");
        assert_eq!(value["available_from"], 20);
    }

    #[test]
    fn follow_seed_preserves_text_bytes_and_uses_one_jsonl_record() {
        let page = TerminalOutputPageDto::Data {
            from_offset: 4,
            to_offset: 6,
            next_offset: 6,
            has_more: true,
            bytes: vec![0xff, b'x'],
            payload_digest_sha256: undercurrent_protocol::execution_output_payload_digest_sha256(
                &[0xff, b'x'],
            ),
            truncation: None,
        };
        let mut text = Vec::new();
        presenter(OutputFormat::Text)
            .write_terminal_output_stream_page(&mut text, &page)
            .expect("text stream seed");
        assert_eq!(text, [0xff, b'x']);

        let mut jsonl = Vec::new();
        presenter(OutputFormat::Jsonl)
            .write_terminal_output_stream_page(&mut jsonl, &page)
            .expect("JSONL stream seed");
        assert_eq!(jsonl.iter().filter(|byte| **byte == b'\n').count(), 1);
        let value: serde_json::Value = serde_json::from_slice(&jsonl).expect("JSON record");
        assert_eq!(value["record"], "terminal_output");
        assert_eq!(value["start_offset"], 4);
        assert_eq!(value["next_offset"], 6);
        assert_eq!(value["bytes"], serde_json::json!([255, 120]));
    }

    #[test]
    fn screen_snapshots_and_diffs_preserve_bytes_or_typed_json() {
        let snapshot = screen(vec![0xff, b's']);
        let mut raw = Vec::new();
        presenter(OutputFormat::Text)
            .write_terminal_screen(&mut raw, &snapshot, true)
            .expect("screen bytes");
        assert_eq!(raw, [0xff, b's']);

        let update = TerminalScreenUpdateDto::Diff(TerminalScreenDiffDto {
            checkpoint: "next".to_owned(),
            from_output_offset: 1,
            to_output_offset: 2,
            columns: 80,
            rows: 24,
            cursor_row: 0,
            cursor_column: 0,
            cursor_visible: true,
            alternate_screen: false,
            contents: vec![0xfe, b'd'],
        });
        let mut diff = Vec::new();
        presenter(OutputFormat::Text)
            .write_terminal_screen_update(&mut diff, &update, false)
            .expect("diff bytes");
        assert_eq!(diff, [0xfe, b'd']);

        let mut json = Vec::new();
        presenter(OutputFormat::Json)
            .write_terminal_screen(&mut json, &snapshot, false)
            .expect("screen JSON");
        let value: serde_json::Value = serde_json::from_slice(&json).expect("JSON");
        assert_eq!(value["contents"], serde_json::json!([255, 115]));
    }

    #[test]
    fn screen_text_preserves_existing_terminal_content() {
        let snapshot = screen(b"\x1b[H\x1b[J\x1b[31mhello\x1b[0m\r\nworld\x1b[?25l".to_vec());
        let mut output = Vec::new();
        presenter(OutputFormat::Text)
            .write_terminal_screen(&mut output, &snapshot, false)
            .expect("plain screen");
        assert_eq!(output, b"hello\nworld\n");

        let mut terminal = vt100::Parser::new(24, 80, 0);
        terminal.process(b"existing shell output\r\n");
        terminal.process(&output);
        assert!(
            terminal
                .screen()
                .contents()
                .starts_with("existing shell output\nhello")
        );
        assert!(!terminal.screen().hide_cursor());
    }

    #[test]
    fn raw_screen_rejects_json_before_writing() {
        let mut output = Vec::new();
        let error = presenter(OutputFormat::Json)
            .write_terminal_screen(&mut output, &screen(vec![b'x']), true)
            .expect_err("raw JSON rejection");

        assert!(matches!(error, CliError::RawOutputRequiresText));
        assert!(output.is_empty());
    }

    fn screen(contents: Vec<u8>) -> TerminalScreenSnapshotDto {
        TerminalScreenSnapshotDto {
            checkpoint: "checkpoint".to_owned(),
            output_offset: 1,
            columns: 80,
            rows: 24,
            cursor_row: 0,
            cursor_column: 0,
            cursor_visible: true,
            alternate_screen: false,
            contents,
        }
    }

    fn presenter(output: OutputFormat) -> CliPresenter {
        CliPresenter::new(CliContext::new(
            output,
            Verbosity::Normal,
            CommandTimeout::new(Duration::from_secs(1), Duration::from_secs(1)).expect("timeout"),
        ))
    }
}
