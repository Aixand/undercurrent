// SPDX-License-Identifier: Apache-2.0

//! Validated native CLI orchestration bounds derived from daemon configuration.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::num::{NonZeroU16, NonZeroU32, NonZeroUsize};

use crate::ResolvedCliConfig;

/// Unvalidated scalar values used to construct native CLI orchestration bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CliExecutionLimitValues {
    /// Maximum stable IDs returned by one catalog page.
    pub catalog_page_size: u32,
    /// Maximum stable session IDs retained by one bulk command.
    pub bulk_selection_sessions: usize,
    /// Maximum concurrent requests issued by one bulk command.
    pub bulk_parallel_requests: usize,
    /// Maximum terminal columns admitted by interactive operations.
    pub terminal_columns: u16,
    /// Maximum terminal rows admitted by interactive operations.
    pub terminal_rows: u16,
    /// Maximum typed input items admitted by one interactive request.
    pub terminal_input_items: usize,
    /// Maximum estimated input bytes admitted by one interactive request.
    pub terminal_input_bytes: usize,
    /// Maximum terminal output bytes read by one finite drain.
    pub terminal_output_page_bytes: u32,
}

/// Hard bounds for paged selection and concurrent finite bulk requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CliExecutionLimits {
    catalog_page_size: NonZeroU32,
    bulk_selection_sessions: NonZeroUsize,
    bulk_parallel_requests: NonZeroUsize,
    terminal_columns: NonZeroU16,
    terminal_rows: NonZeroU16,
    terminal_input_items: NonZeroUsize,
    terminal_input_bytes: NonZeroUsize,
    terminal_output_page_bytes: NonZeroU32,
}

impl CliExecutionLimits {
    /// Validates explicit protocol page and bulk concurrency limits.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns the first zero bound.
    pub fn new(values: CliExecutionLimitValues) -> Result<Self, CliExecutionLimitsError> {
        Ok(Self {
            catalog_page_size: NonZeroU32::new(values.catalog_page_size)
                .ok_or(CliExecutionLimitsError::ZeroCatalogPageSize)?,
            bulk_selection_sessions: NonZeroUsize::new(values.bulk_selection_sessions)
                .ok_or(CliExecutionLimitsError::ZeroBulkSelectionSessions)?,
            bulk_parallel_requests: NonZeroUsize::new(values.bulk_parallel_requests)
                .ok_or(CliExecutionLimitsError::ZeroBulkParallelRequests)?,
            terminal_columns: NonZeroU16::new(values.terminal_columns)
                .ok_or(CliExecutionLimitsError::ZeroTerminalColumns)?,
            terminal_rows: NonZeroU16::new(values.terminal_rows)
                .ok_or(CliExecutionLimitsError::ZeroTerminalRows)?,
            terminal_input_items: NonZeroUsize::new(values.terminal_input_items)
                .ok_or(CliExecutionLimitsError::ZeroTerminalInputItems)?,
            terminal_input_bytes: NonZeroUsize::new(values.terminal_input_bytes)
                .ok_or(CliExecutionLimitsError::ZeroTerminalInputBytes)?,
            terminal_output_page_bytes: NonZeroU32::new(values.terminal_output_page_bytes)
                .ok_or(CliExecutionLimitsError::ZeroTerminalOutputPageBytes)?,
        })
    }

    /// Projects already validated daemon settings into native CLI bounds.
    ///
    /// Values above the protocol's `u32` page representation are capped at
    /// `u32::MAX`; all smaller configured maxima are preserved exactly. This
    /// function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns an invariant failure if a supposedly resolved daemon setting is zero.
    pub fn from_daemon(configuration: &ResolvedCliConfig) -> Result<Self, CliExecutionLimitsError> {
        let catalog_page_size = configuration.max_query_page_size();
        let terminal_output_page_bytes = configuration.terminal_replay_bytes();
        Self::new(CliExecutionLimitValues {
            catalog_page_size,
            bulk_selection_sessions: configuration.max_sessions(),
            bulk_parallel_requests: configuration.bulk_parallel_requests(),
            terminal_columns: configuration.max_terminal_columns(),
            terminal_rows: configuration.max_terminal_rows(),
            terminal_input_items: configuration.max_input_items(),
            terminal_input_bytes: configuration.max_input_bytes(),
            terminal_output_page_bytes,
        })
    }

    /// Returns the maximum IDs collected by one server-side query page.
    pub const fn catalog_page_size(self) -> u32 {
        self.catalog_page_size.get()
    }

    /// Returns the maximum stable session IDs retained by one bulk command.
    pub const fn bulk_selection_sessions(self) -> usize {
        self.bulk_selection_sessions.get()
    }

    /// Returns the maximum concurrent finite requests issued by one bulk command.
    pub const fn bulk_parallel_requests(self) -> usize {
        self.bulk_parallel_requests.get()
    }

    /// Returns the maximum terminal columns sent to the daemon.
    pub const fn terminal_columns(self) -> u16 {
        self.terminal_columns.get()
    }

    /// Returns the maximum terminal rows sent to the daemon.
    pub const fn terminal_rows(self) -> u16 {
        self.terminal_rows.get()
    }

    /// Returns the maximum typed items in one interactive input batch.
    pub const fn terminal_input_items(self) -> usize {
        self.terminal_input_items.get()
    }

    /// Returns the maximum estimated bytes in one interactive input batch.
    pub const fn terminal_input_bytes(self) -> usize {
        self.terminal_input_bytes.get()
    }

    /// Returns the maximum bytes read by one terminal output drain request.
    pub const fn terminal_output_page_bytes(self) -> u32 {
        self.terminal_output_page_bytes.get()
    }
}

/// Invalid native CLI orchestration limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliExecutionLimitsError {
    /// Query pagination has no positive page bound.
    ZeroCatalogPageSize,
    /// Bulk selection retention has no positive session bound.
    ZeroBulkSelectionSessions,
    /// Bulk mutation execution has no worker bound.
    ZeroBulkParallelRequests,
    /// Terminal geometry has no positive column bound.
    ZeroTerminalColumns,
    /// Terminal geometry has no positive row bound.
    ZeroTerminalRows,
    /// Interactive terminal input has no positive item bound.
    ZeroTerminalInputItems,
    /// Interactive terminal input has no positive byte bound.
    ZeroTerminalInputBytes,
    /// Final terminal output drain has no positive page bound.
    ZeroTerminalOutputPageBytes,
}

impl Display for CliExecutionLimitsError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroCatalogPageSize => {
                formatter.write_str("CLI catalog page size must be non-zero")
            }
            Self::ZeroBulkSelectionSessions => {
                formatter.write_str("CLI bulk selection sessions must be non-zero")
            }
            Self::ZeroBulkParallelRequests => {
                formatter.write_str("CLI bulk parallel requests must be non-zero")
            }
            Self::ZeroTerminalColumns => {
                formatter.write_str("CLI terminal columns must be non-zero")
            }
            Self::ZeroTerminalRows => formatter.write_str("CLI terminal rows must be non-zero"),
            Self::ZeroTerminalInputItems => {
                formatter.write_str("CLI terminal input items must be non-zero")
            }
            Self::ZeroTerminalInputBytes => {
                formatter.write_str("CLI terminal input bytes must be non-zero")
            }
            Self::ZeroTerminalOutputPageBytes => {
                formatter.write_str("CLI terminal output page bytes must be non-zero")
            }
        }
    }
}

impl Error for CliExecutionLimitsError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn values() -> CliExecutionLimitValues {
        CliExecutionLimitValues {
            catalog_page_size: 1,
            bulk_selection_sessions: 1,
            bulk_parallel_requests: 1,
            terminal_columns: 1,
            terminal_rows: 1,
            terminal_input_items: 1,
            terminal_input_bytes: 1,
            terminal_output_page_bytes: 1,
        }
    }

    #[test]
    fn zero_limits_are_rejected_independently() {
        for (invalid, expected) in [
            (
                CliExecutionLimitValues {
                    catalog_page_size: 0,
                    ..values()
                },
                CliExecutionLimitsError::ZeroCatalogPageSize,
            ),
            (
                CliExecutionLimitValues {
                    bulk_selection_sessions: 0,
                    ..values()
                },
                CliExecutionLimitsError::ZeroBulkSelectionSessions,
            ),
            (
                CliExecutionLimitValues {
                    bulk_parallel_requests: 0,
                    ..values()
                },
                CliExecutionLimitsError::ZeroBulkParallelRequests,
            ),
            (
                CliExecutionLimitValues {
                    terminal_columns: 0,
                    ..values()
                },
                CliExecutionLimitsError::ZeroTerminalColumns,
            ),
            (
                CliExecutionLimitValues {
                    terminal_rows: 0,
                    ..values()
                },
                CliExecutionLimitsError::ZeroTerminalRows,
            ),
            (
                CliExecutionLimitValues {
                    terminal_input_items: 0,
                    ..values()
                },
                CliExecutionLimitsError::ZeroTerminalInputItems,
            ),
            (
                CliExecutionLimitValues {
                    terminal_input_bytes: 0,
                    ..values()
                },
                CliExecutionLimitsError::ZeroTerminalInputBytes,
            ),
            (
                CliExecutionLimitValues {
                    terminal_output_page_bytes: 0,
                    ..values()
                },
                CliExecutionLimitsError::ZeroTerminalOutputPageBytes,
            ),
        ] {
            assert_eq!(CliExecutionLimits::new(invalid), Err(expected));
        }
    }
}
