// SPDX-License-Identifier: Apache-2.0

//! Coherent bounded visible-screen extraction for health checks.

use std::fmt::{Debug, Formatter};

use undercurrent_application::{OutputOffset, PortError, PortOperation};
use undercurrent_domain::SessionId;

use super::{TerminalScreenStoreError, VtTerminalScreenStore, screen_port_error};

/// One private visible-screen sample and payload-free red-cell count.
#[derive(Clone)]
pub struct TerminalHealthSample {
    output_offset: OutputOffset,
    text: String,
    red_cells: u32,
}

impl TerminalHealthSample {
    /// Returns the coherent raw-output checkpoint represented by this sample.
    pub const fn output_offset(&self) -> OutputOffset {
        self.output_offset
    }

    /// Returns private plain visible-screen text for pattern evaluation.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Returns nonblank cells using standard or bright indexed red foreground.
    pub const fn red_cells(&self) -> u32 {
        self.red_cells
    }
}

impl Debug for TerminalHealthSample {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TerminalHealthSample")
            .field("output_offset", &self.output_offset)
            .field("text_bytes", &self.text.len())
            .field("red_cells", &self.red_cells)
            .finish_non_exhaustive()
    }
}

impl VtTerminalScreenStore {
    /// Extracts one bounded visible-screen health sample under the parser lock.
    ///
    /// Text and cell colors represent the same output checkpoint. Private text
    /// is returned only to the health adapter and is redacted from `Debug` and
    /// all errors. This method performs no external IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed absence, zero-limit, text-byte, or screen-cell capacity
    /// failures.
    pub fn health_sample(
        &self,
        session_id: &SessionId,
        maximum_text_bytes: usize,
        maximum_cells: usize,
    ) -> Result<TerminalHealthSample, PortError> {
        if maximum_text_bytes == 0 || maximum_cells == 0 {
            return Err(screen_port_error(
                session_id,
                PortOperation::RunHealthChecks,
                TerminalScreenStoreError::InvalidLimit {
                    field: if maximum_text_bytes == 0 {
                        "maximum_health_text_bytes"
                    } else {
                        "maximum_health_cells"
                    },
                },
            ));
        }
        let screen = self.resolve(session_id, PortOperation::RunHealthChecks)?;
        let screen = screen
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let rendered = screen.parser.screen();
        let (rows, columns) = rendered.size();
        let cells = usize::from(rows).saturating_mul(usize::from(columns));
        if cells > maximum_cells {
            return Err(screen_port_error(
                session_id,
                PortOperation::RunHealthChecks,
                TerminalScreenStoreError::ScreenCellCapacity {
                    observed: cells,
                    limit: maximum_cells,
                },
            ));
        }
        let mut text = String::new();
        for (index, row) in rendered.rows(0, columns).enumerate() {
            let required = text
                .len()
                .checked_add(row.len())
                .and_then(|value| value.checked_add(usize::from(index > 0)))
                .unwrap_or(usize::MAX);
            if required > maximum_text_bytes {
                return Err(screen_port_error(
                    session_id,
                    PortOperation::RunHealthChecks,
                    TerminalScreenStoreError::SnapshotCapacity {
                        observed: required,
                        limit: maximum_text_bytes,
                    },
                ));
            }
            if index > 0 {
                text.push('\n');
            }
            text.push_str(&row);
        }
        let red_cells = (0..rows)
            .flat_map(|row| (0..columns).map(move |column| (row, column)))
            .filter(|(row, column)| {
                rendered.cell(*row, *column).is_some_and(|cell| {
                    cell.has_contents()
                        && matches!(cell.fgcolor(), vt100::Color::Idx(1) | vt100::Color::Idx(9))
                })
            })
            .count();
        Ok(TerminalHealthSample {
            output_offset: screen.output_offset,
            text,
            red_cells: u32::try_from(red_cells).unwrap_or(u32::MAX),
        })
    }
}
