// SPDX-License-Identifier: Apache-2.0

//! Typed JSON Lines envelopes and exact-byte text rendering for product streams.

use std::fmt::Write as _;
use std::io::Write;

use crate::cli_support::{CliOutputError, OutputFormat, OutputShape, Verbosity};
use serde::Serialize;
use undercurrent_protocol::{
    AgentEventDto, ProductStreamItem, SessionDeltaBatchDto, SessionDeltaDto, SessionEventDto,
    SessionPageDto, TerminalOutputChunk, TerminalScreenSnapshot, TerminalStreamItem,
};
use undercurrent_sdk::{ProductStreamGap, ProductSubscriptionItem};

use crate::CliError;

use super::{CliPresenter, safe_text, session_page_text, session_summary_text};

/// Borrowed stable record emitted for one decoded logical-stream item.
#[derive(Serialize)]
#[serde(tag = "record", rename_all = "snake_case")]
pub enum CliStreamRecord<'a> {
    /// Initial or reconnect query snapshot.
    SessionSnapshot {
        /// Stream-local sequence.
        sequence: u64,
        /// Bounded authoritative page.
        page: &'a SessionPageDto,
    },
    /// Coalesced watched-page transition.
    SessionDeltaBatch {
        /// Stream-local sequence.
        sequence: u64,
        /// Authoritative ordered transition.
        batch: &'a SessionDeltaBatchDto,
    },
    /// Durable session event.
    SessionEvent {
        /// Stream-local sequence.
        sequence: u64,
        /// Safe durable event.
        event: &'a SessionEventDto,
    },
    /// Durable agent tool call for one exact attempt.
    AgentEvent {
        /// Stream-local sequence.
        sequence: u64,
        /// Exact authenticated agent event.
        event: &'a AgentEventDto,
    },
    /// Binary terminal output interval represented as bytes in JSON Lines.
    TerminalOutput {
        /// Stream-local sequence.
        sequence: u64,
        /// Inclusive raw-output start offset.
        start_offset: u64,
        /// Exclusive raw-output end offset when representable.
        end_offset: Option<u64>,
        /// Exact terminal bytes.
        bytes: &'a [u8],
    },
    /// Complete binary terminal screen seed.
    TerminalScreen {
        /// Stream-local sequence.
        sequence: u64,
        /// Exclusive raw-output checkpoint.
        output_offset: u64,
        /// Terminal columns.
        columns: u16,
        /// Terminal rows.
        rows: u16,
        /// Zero-based cursor row.
        cursor_row: u16,
        /// Zero-based cursor column.
        cursor_column: u16,
        /// Whether the cursor is visible.
        cursor_visible: bool,
        /// Whether the alternate screen is active.
        alternate_screen: bool,
        /// Exact ANSI rendering bytes.
        contents: &'a [u8],
    },
    /// Explicit sequence discontinuity with deterministic recovery.
    Gap {
        /// Affected logical stream identity.
        stream_id: &'a str,
        /// First missing stream-local sequence.
        first_missing: u64,
        /// Last missing stream-local sequence.
        last_missing: u64,
        /// Stable gap reason.
        reason: &'static str,
        /// Required recovery action.
        recovery: &'static str,
    },
}

impl<'a> From<&'a ProductSubscriptionItem> for CliStreamRecord<'a> {
    fn from(value: &'a ProductSubscriptionItem) -> Self {
        match value {
            ProductSubscriptionItem::Json { sequence, item } => match item {
                ProductStreamItem::SessionSnapshot(page) => Self::SessionSnapshot {
                    sequence: *sequence,
                    page,
                },
                ProductStreamItem::SessionDeltaBatch(batch) => Self::SessionDeltaBatch {
                    sequence: *sequence,
                    batch,
                },
                ProductStreamItem::SessionEvent(event) => Self::SessionEvent {
                    sequence: *sequence,
                    event,
                },
                ProductStreamItem::AgentEvent(event) => Self::AgentEvent {
                    sequence: *sequence,
                    event,
                },
            },
            ProductSubscriptionItem::Terminal { sequence, item } => match item {
                TerminalStreamItem::Output(output) => terminal_output_record(*sequence, output),
                TerminalStreamItem::Screen(screen) => terminal_screen_record(*sequence, screen),
            },
            ProductSubscriptionItem::Gap(gap) => gap_record(gap),
        }
    }
}

impl CliPresenter {
    /// Writes one decoded product stream item in exact text bytes or JSON Lines.
    ///
    /// Terminal output and screen items are written without UTF-8 conversion in
    /// text mode. JSON Lines uses a borrowed stable envelope with sequence and
    /// recovery metadata. Quiet text suppresses the item; quiet JSON Lines does
    /// not.
    ///
    /// # Errors
    ///
    /// Returns a typed shape, serialization, or writer failure. This method
    /// performs writer IO and does not panic.
    pub fn write_product_stream_item<W>(
        self,
        writer: &mut W,
        item: &ProductSubscriptionItem,
    ) -> Result<(), CliError>
    where
        W: Write,
    {
        self.context.validate_output_shape(OutputShape::Stream)?;
        if self.context.output == OutputFormat::Text {
            if self.context.verbosity == Verbosity::Quiet {
                return Ok(());
            }
            return write_product_text(writer, item, self.context.verbosity);
        }
        let record = CliStreamRecord::from(item);
        self.context
            .output
            .write(writer, &record, String::new)
            .map_err(Into::into)
    }
}

fn write_product_text<W>(
    writer: &mut W,
    item: &ProductSubscriptionItem,
    verbosity: Verbosity,
) -> Result<(), CliError>
where
    W: Write,
{
    match item {
        ProductSubscriptionItem::Terminal { item, .. } => match item {
            TerminalStreamItem::Output(output) => writer.write_all(output.bytes()),
            TerminalStreamItem::Screen(screen) => writer.write_all(screen.contents()),
        },
        ProductSubscriptionItem::Json { item, .. } => {
            let text = product_item_text(item, verbosity);
            writer.write_all(text.as_bytes()).and_then(|()| {
                if text.ends_with('\n') {
                    Ok(())
                } else {
                    writer.write_all(b"\n")
                }
            })
        }
        ProductSubscriptionItem::Gap(gap) => writeln!(
            writer,
            "stream gap {}..{}: {}; recovery={}",
            gap.first_missing,
            gap.last_missing,
            gap.reason.as_str(),
            gap.recovery.as_str()
        ),
    }
    .map_err(CliOutputError::from)?;
    Ok(())
}

fn product_item_text(item: &ProductStreamItem, verbosity: Verbosity) -> String {
    match item {
        ProductStreamItem::SessionSnapshot(page) => session_page_text(page, verbosity),
        ProductStreamItem::SessionDeltaBatch(batch) => delta_batch_text(batch, verbosity),
        ProductStreamItem::SessionEvent(event) => format!(
            "event {} session={} revision={} sequence={}",
            event.kind.as_str(),
            safe_text(&event.session_id),
            event.session_revision,
            event.sequence
        ),
        ProductStreamItem::AgentEvent(event) => format!(
            "agent event {} attempt={} session={} sequence={}",
            safe_text(&event.event_id),
            safe_text(&event.attempt_id),
            safe_text(&event.session_id),
            event.sequence
        ),
    }
}

fn delta_batch_text(batch: &SessionDeltaBatchDto, verbosity: Verbosity) -> String {
    let mut output = format!(
        "revision {} -> {} ({} changes)",
        batch.from_revision,
        batch.to_revision,
        batch.deltas.len()
    );
    for delta in &batch.deltas {
        let _ = write!(output, "\n{}", delta_text(delta, verbosity));
    }
    output
}

fn delta_text(delta: &SessionDeltaDto, verbosity: Verbosity) -> String {
    let kind = match delta.kind {
        undercurrent_protocol::SessionDeltaKindDto::Added => "added",
        undercurrent_protocol::SessionDeltaKindDto::Updated => "updated",
        undercurrent_protocol::SessionDeltaKindDto::Removed => "removed",
    };
    delta.summary.as_ref().map_or_else(
        || format!("{kind} {}", safe_text(&delta.session_id)),
        |summary| format!("{kind}\n{}", session_summary_text(summary, verbosity)),
    )
}

fn terminal_output_record(sequence: u64, output: &TerminalOutputChunk) -> CliStreamRecord<'_> {
    CliStreamRecord::TerminalOutput {
        sequence,
        start_offset: output.start_offset(),
        end_offset: output.end_offset(),
        bytes: output.bytes(),
    }
}

fn terminal_screen_record(sequence: u64, screen: &TerminalScreenSnapshot) -> CliStreamRecord<'_> {
    CliStreamRecord::TerminalScreen {
        sequence,
        output_offset: screen.output_offset(),
        columns: screen.columns(),
        rows: screen.rows(),
        cursor_row: screen.cursor_row(),
        cursor_column: screen.cursor_column(),
        cursor_visible: screen.cursor_visible(),
        alternate_screen: screen.alternate_screen(),
        contents: screen.contents(),
    }
}

fn gap_record(gap: &ProductStreamGap) -> CliStreamRecord<'_> {
    CliStreamRecord::Gap {
        stream_id: gap.stream_id.as_str(),
        first_missing: gap.first_missing,
        last_missing: gap.last_missing,
        reason: gap.reason.as_str(),
        recovery: gap.recovery.as_str(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::cli_support::{CliContext, CommandTimeout};
    use undercurrent_sdk::{ProductStreamGapReason, ProductStreamRecovery};

    use super::*;

    #[test]
    fn text_terminal_output_preserves_exact_non_utf8_bytes() {
        let item = ProductSubscriptionItem::Terminal {
            sequence: 4,
            item: TerminalStreamItem::Output(TerminalOutputChunk::new(
                10,
                Arc::from([0xff, 0x00, b'a']),
            )),
        };
        let mut bytes = Vec::new();

        presenter(OutputFormat::Text)
            .write_product_stream_item(&mut bytes, &item)
            .expect("text output");

        assert_eq!(bytes, [0xff, 0x00, b'a']);
    }

    #[test]
    fn jsonl_gap_record_retains_stable_recovery_metadata() {
        let item = ProductSubscriptionItem::Gap(ProductStreamGap {
            stream_id: "stream-1".to_owned(),
            first_missing: 7,
            last_missing: 9,
            reason: ProductStreamGapReason::ReplayEvicted,
            recovery: ProductStreamRecovery::RequestSnapshot,
        });
        let mut bytes = Vec::new();

        presenter(OutputFormat::Jsonl)
            .write_product_stream_item(&mut bytes, &item)
            .expect("JSON Lines gap");

        let value: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON record");
        assert_eq!(value["record"], "gap");
        assert_eq!(value["reason"], "replay_evicted");
        assert_eq!(value["recovery"], "request_snapshot");
        assert_eq!(value["first_missing"], 7);
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);
    }

    #[test]
    fn pretty_json_is_rejected_for_a_stream_before_writing() {
        let item = ProductSubscriptionItem::Terminal {
            sequence: 1,
            item: TerminalStreamItem::Output(TerminalOutputChunk::new(0, Arc::from(*b"private"))),
        };
        let mut bytes = Vec::new();

        let error = presenter(OutputFormat::Json)
            .write_product_stream_item(&mut bytes, &item)
            .expect_err("JSON is finite-only");

        assert!(matches!(error, CliError::OutputShape(_)));
        assert!(bytes.is_empty());
    }

    fn presenter(output: OutputFormat) -> CliPresenter {
        CliPresenter::new(CliContext::new(
            output,
            Verbosity::Normal,
            CommandTimeout::new(Duration::from_secs(1), Duration::from_secs(1)).expect("timeout"),
        ))
    }
}
