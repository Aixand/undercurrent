// SPDX-License-Identifier: Apache-2.0

//! Coherent bounded screen-region extraction for detection profiles.

use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};

use undercurrent_application::{OutputOffset, PortError, PortOperation};
use undercurrent_domain::{
    OutputExclusions, ProfileRegion, ProfileSampleView, ScreenAnchor, SessionId,
};

use super::{TerminalScreenStoreError, VtTerminalScreenStore, screen_port_error};

/// One coherent output checkpoint and bounded set of private screen samples.
#[derive(Clone)]
pub struct TerminalProfileSamples {
    output_offset: OutputOffset,
    samples: Vec<ProfileSampleView>,
}

/// One payload-free fingerprint of the profile-watched visible terminal rows.
#[derive(Clone, PartialEq, Eq)]
pub struct TerminalMotionSnapshot {
    output_offset: OutputOffset,
    has_process_output: bool,
    columns: u16,
    rows: u16,
    alternate_screen: bool,
    row_hashes: Vec<u64>,
}

impl TerminalMotionSnapshot {
    /// Returns the coherent raw-output checkpoint represented by this screen.
    pub const fn output_offset(&self) -> OutputOffset {
        self.output_offset
    }

    /// Returns whether this process incarnation emitted any retained output.
    pub const fn has_process_output(&self) -> bool {
        self.has_process_output
    }

    /// Returns whether watched visible state changed after the prior checkpoint.
    ///
    /// Output must have advanced as well as the filtered visible state changing;
    /// a resize alone is not process foreground. This method performs no IO and
    /// does not panic.
    pub fn changed_since(&self, prior: &Self) -> bool {
        self.output_offset > prior.output_offset
            && (self.columns != prior.columns
                || self.rows != prior.rows
                || self.alternate_screen != prior.alternate_screen
                || self.row_hashes != prior.row_hashes)
    }
}

impl Debug for TerminalMotionSnapshot {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TerminalMotionSnapshot")
            .field("output_offset", &self.output_offset)
            .field("has_process_output", &self.has_process_output)
            .field("columns", &self.columns)
            .field("rows", &self.rows)
            .field("alternate_screen", &self.alternate_screen)
            .field("watched_rows", &self.row_hashes.len())
            .finish_non_exhaustive()
    }
}

/// Parser-consistent prompt samples and watched-row motion fingerprint.
#[derive(Clone)]
pub struct TerminalForegroundSamples {
    profile: TerminalProfileSamples,
    motion: TerminalMotionSnapshot,
}

impl TerminalForegroundSamples {
    /// Returns the raw-output checkpoint represented by every extracted view.
    pub const fn output_offset(&self) -> OutputOffset {
        self.profile.output_offset()
    }

    /// Returns private prompt-region samples in stable region order.
    pub fn samples(&self) -> &[ProfileSampleView] {
        self.profile.samples()
    }

    /// Returns the payload-free watched-row fingerprint.
    pub const fn motion(&self) -> &TerminalMotionSnapshot {
        &self.motion
    }
}

impl Debug for TerminalForegroundSamples {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TerminalForegroundSamples")
            .field("profile", &self.profile)
            .field("motion", &self.motion)
            .finish()
    }
}

impl TerminalProfileSamples {
    /// Returns the raw-output checkpoint represented by every sample.
    pub const fn output_offset(&self) -> OutputOffset {
        self.output_offset
    }

    /// Returns samples in stable region order.
    ///
    /// Sample text is private terminal content and must not be logged.
    pub fn samples(&self) -> &[ProfileSampleView] {
        &self.samples
    }

    /// Returns the sample for one exact region.
    pub fn get(&self, region: Option<&ProfileRegion>) -> Option<&ProfileSampleView> {
        self.samples.iter().find(|sample| sample.region() == region)
    }
}

impl Debug for TerminalProfileSamples {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TerminalProfileSamples")
            .field("output_offset", &self.output_offset)
            .field("sample_count", &self.samples.len())
            .finish_non_exhaustive()
    }
}

impl VtTerminalScreenStore {
    /// Extracts multiple profile regions from one coherent parser checkpoint.
    ///
    /// Exact duplicate regions are read once. Text remains in the returned
    /// private domain samples and is never included in errors or `Debug`.
    /// This method performs no external IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed absence, sample-count, sample-byte, or domain-invariant
    /// failures. `maximum_samples` and `maximum_sample_bytes` must be non-zero.
    pub fn profile_samples(
        &self,
        session_id: &SessionId,
        regions: impl IntoIterator<Item = Option<ProfileRegion>>,
        maximum_samples: usize,
        maximum_sample_bytes: usize,
    ) -> Result<TerminalProfileSamples, PortError> {
        let regions =
            validated_regions(session_id, regions, maximum_samples, maximum_sample_bytes)?;
        let screen = self.resolve(session_id, PortOperation::CollectForegroundEvidence)?;
        let screen = screen
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        extract_profile_samples(session_id, &screen, regions, maximum_sample_bytes)
    }

    /// Extracts prompt regions and terminal-motion state at one parser checkpoint.
    ///
    /// Motion retains only per-row hashes after applying profile-declared top
    /// and bottom exclusions. Neither private terminal text nor expressions are
    /// retained in the fingerprint or exposed through `Debug`. This method
    /// performs no external IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed absence, sample, row-count, or domain-invariant failures.
    /// All maximum values must be non-zero.
    #[allow(clippy::too_many_arguments)]
    pub fn foreground_samples(
        &self,
        session_id: &SessionId,
        regions: impl IntoIterator<Item = Option<ProfileRegion>>,
        exclusions: OutputExclusions,
        maximum_samples: usize,
        maximum_sample_bytes: usize,
        maximum_motion_rows: usize,
    ) -> Result<TerminalForegroundSamples, PortError> {
        let regions =
            validated_regions(session_id, regions, maximum_samples, maximum_sample_bytes)?;
        if maximum_motion_rows == 0 {
            return Err(screen_port_error(
                session_id,
                PortOperation::CollectForegroundEvidence,
                TerminalScreenStoreError::InvalidLimit {
                    field: "maximum_motion_rows",
                },
            ));
        }
        let screen = self.resolve(session_id, PortOperation::CollectForegroundEvidence)?;
        let screen = screen
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let profile = extract_profile_samples(session_id, &screen, regions, maximum_sample_bytes)?;
        let motion = motion_snapshot(
            screen.parser.screen(),
            screen.process_start_offset,
            screen.output_offset,
            exclusions,
            maximum_motion_rows,
        )
        .map_err(|error| {
            screen_port_error(session_id, PortOperation::CollectForegroundEvidence, error)
        })?;
        Ok(TerminalForegroundSamples { profile, motion })
    }
}

fn validated_regions(
    session_id: &SessionId,
    regions: impl IntoIterator<Item = Option<ProfileRegion>>,
    maximum_samples: usize,
    maximum_sample_bytes: usize,
) -> Result<BTreeSet<Option<ProfileRegion>>, PortError> {
    if maximum_samples == 0 || maximum_sample_bytes == 0 {
        return Err(screen_port_error(
            session_id,
            PortOperation::CollectForegroundEvidence,
            TerminalScreenStoreError::InvalidLimit {
                field: if maximum_samples == 0 {
                    "maximum_samples"
                } else {
                    "maximum_sample_bytes"
                },
            },
        ));
    }
    let regions = regions.into_iter().collect::<BTreeSet<_>>();
    if regions.len() > maximum_samples {
        return Err(screen_port_error(
            session_id,
            PortOperation::CollectForegroundEvidence,
            TerminalScreenStoreError::SampleCapacity {
                observed: regions.len(),
                limit: maximum_samples,
            },
        ));
    }
    Ok(regions)
}

fn extract_profile_samples(
    session_id: &SessionId,
    screen: &super::VtTerminalScreen,
    regions: BTreeSet<Option<ProfileRegion>>,
    maximum_sample_bytes: usize,
) -> Result<TerminalProfileSamples, PortError> {
    let rendered = screen.parser.screen();
    let samples = regions
        .into_iter()
        .map(|region| profile_sample(rendered, region, maximum_sample_bytes))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            screen_port_error(session_id, PortOperation::CollectForegroundEvidence, error)
        })?;
    Ok(TerminalProfileSamples {
        output_offset: screen.output_offset,
        samples,
    })
}

fn motion_snapshot(
    screen: &vt100::Screen,
    process_start_offset: OutputOffset,
    output_offset: OutputOffset,
    exclusions: OutputExclusions,
    maximum_rows: usize,
) -> Result<TerminalMotionSnapshot, TerminalScreenStoreError> {
    let (rows, columns) = screen.size();
    if usize::from(rows) > maximum_rows {
        return Err(TerminalScreenStoreError::MotionRowCapacity {
            observed: usize::from(rows),
            limit: maximum_rows,
        });
    }
    let start = exclusions.top_rows.min(rows);
    let end = rows.saturating_sub(exclusions.bottom_rows.min(rows));
    let row_hashes = if start >= end {
        Vec::new()
    } else {
        screen
            .rows(0, columns)
            .enumerate()
            .filter(|(row, _)| *row >= usize::from(start) && *row < usize::from(end))
            .map(|(_, row)| fnv_hash(row.trim_end().as_bytes()))
            .collect()
    };
    Ok(TerminalMotionSnapshot {
        output_offset,
        has_process_output: output_offset > process_start_offset,
        columns,
        rows,
        alternate_screen: screen.alternate_screen(),
        row_hashes,
    })
}

fn fnv_hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

fn profile_sample(
    screen: &vt100::Screen,
    region: Option<ProfileRegion>,
    maximum_bytes: usize,
) -> Result<ProfileSampleView, TerminalScreenStoreError> {
    let (rows, columns) = screen.size();
    let (cursor_row, cursor_column) = screen.cursor_position();
    let selected = selected_rows(
        screen,
        region.as_ref(),
        rows,
        columns,
        cursor_row,
        cursor_column,
    );
    let text_bytes = selected.iter().try_fold(0usize, |total, (_, line)| {
        total
            .checked_add(line.len())
            .and_then(|value| value.checked_add(1))
    });
    let Some(text_bytes) = text_bytes else {
        return Err(TerminalScreenStoreError::SnapshotCapacity {
            observed: usize::MAX,
            limit: maximum_bytes,
        });
    };
    if text_bytes.saturating_sub(usize::from(!selected.is_empty())) > maximum_bytes {
        return Err(TerminalScreenStoreError::SnapshotCapacity {
            observed: text_bytes.saturating_sub(usize::from(!selected.is_empty())),
            limit: maximum_bytes,
        });
    }
    let last_line = selected
        .last()
        .map(|(_, line)| line.clone())
        .unwrap_or_default();
    let text = selected
        .into_iter()
        .map(|(_, line)| line)
        .collect::<Vec<_>>()
        .join("\n");
    let cursor_line_length = screen
        .rows(0, columns)
        .nth(usize::from(cursor_row))
        .map(|line| line.trim_end().chars().count())
        .unwrap_or(0);
    let cursor_at_end = usize::from(cursor_column) >= cursor_line_length;
    ProfileSampleView::new(region, text, last_line, cursor_at_end, maximum_bytes)
        .map_err(TerminalScreenStoreError::InvalidSample)
}

fn selected_rows(
    screen: &vt100::Screen,
    region: Option<&ProfileRegion>,
    total_rows: u16,
    columns: u16,
    cursor_row: u16,
    cursor_column: u16,
) -> Vec<(u16, String)> {
    let all_rows = || {
        screen
            .rows(0, columns)
            .enumerate()
            .filter_map(|(row, line)| u16::try_from(row).ok().map(|row| (row, line)))
            .collect::<Vec<_>>()
    };
    let Some(region) = region else {
        let rows = all_rows();
        let last_non_empty = rows
            .iter()
            .rposition(|(_, line)| !line.trim().is_empty())
            .map_or(0, |index| index.saturating_add(1));
        return rows.into_iter().take(last_non_empty).collect();
    };
    let maximum_row = total_rows.saturating_sub(1);
    let (row_range, column_clip, anchor) = match region {
        ProfileRegion::BottomRows {
            rows,
            anchor,
            columns,
        } => (
            Some((total_rows.saturating_sub(rows.get()), maximum_row)),
            columns.map(std::num::NonZeroU16::get),
            Some(*anchor),
        ),
        ProfileRegion::AboveCursor { rows, columns } => {
            let range = cursor_row
                .checked_sub(1)
                .map(|end| (cursor_row.saturating_sub(rows.get()), end));
            (range, columns.map(std::num::NonZeroU16::get), None)
        }
        ProfileRegion::BelowCursor { rows, columns } => {
            let range = cursor_row
                .checked_add(1)
                .filter(|start| *start <= maximum_row)
                .map(|start| {
                    (
                        start,
                        cursor_row.saturating_add(rows.get()).min(maximum_row),
                    )
                });
            (range, columns.map(std::num::NonZeroU16::get), None)
        }
    };
    let clip = |line: String| match column_clip {
        Some(columns) => line.chars().take(usize::from(columns)).collect(),
        None => line,
    };
    let rows = all_rows();
    let selected = row_range.map_or_else(Vec::new, |(start, end)| {
        rows.iter()
            .filter(|(row, _)| *row >= start && *row <= end)
            .map(|(row, line)| (*row, clip(line.clone())))
            .collect::<Vec<_>>()
    });
    match anchor {
        None | Some(ScreenAnchor::Any) => selected,
        Some(ScreenAnchor::Last) => selected
            .into_iter()
            .rev()
            .find(|(_, line)| !line.trim().is_empty())
            .into_iter()
            .collect(),
        Some(ScreenAnchor::Cursor) => {
            let line = rows
                .get(usize::from(cursor_row))
                .map(|(_, line)| clip(line.clone()));
            line.map(|line| {
                let cursor_end = usize::from(cursor_column)
                    .saturating_add(1)
                    .min(line.chars().count());
                (cursor_row, line.chars().take(cursor_end).collect())
            })
            .into_iter()
            .collect()
        }
        Some(ScreenAnchor::NearCursor { distance }) => {
            let minimum = cursor_row.saturating_sub(distance);
            let maximum = cursor_row.saturating_add(distance).min(maximum_row);
            rows.into_iter()
                .filter(|(row, _)| *row >= minimum && *row <= maximum)
                .map(|(row, line)| (row, clip(line)))
                .collect()
        }
    }
}
