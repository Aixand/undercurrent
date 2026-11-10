// SPDX-License-Identifier: Apache-2.0

//! One total CLI command deadline shared across bounded sub-operations.

use std::time::{Duration, Instant};

use crate::{CliCommand, CliError};

/// Monotonic deadline for one command composed of several requests or streams.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CliCommandDeadline {
    command: CliCommand,
    deadline: Instant,
}

impl CliCommandDeadline {
    /// Starts one total command deadline without performing IO.
    ///
    /// # Errors
    ///
    /// Returns a typed timeout when the duration cannot be represented by the
    /// platform monotonic clock. This function does not panic.
    pub(crate) fn start(command: CliCommand, timeout: Duration) -> Result<Self, CliError> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(CliError::CommandTimedOut { command })?;
        Ok(Self { command, deadline })
    }

    /// Returns the non-zero time remaining for the next bounded operation.
    ///
    /// # Errors
    ///
    /// Returns a typed timeout once the total command deadline has elapsed.
    /// This function performs no IO and does not panic.
    pub(crate) fn remaining(self) -> Result<Duration, CliError> {
        self.deadline
            .checked_duration_since(Instant::now())
            .filter(|duration| !duration.is_zero())
            .ok_or(CliError::CommandTimedOut {
                command: self.command,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deadline_returns_a_nonzero_bounded_remainder() {
        let timeout = Duration::from_secs(1);
        let deadline = CliCommandDeadline::start(CliCommand::Ls, timeout).expect("deadline");
        let remaining = deadline.remaining().expect("remaining");

        assert!(!remaining.is_zero());
        assert!(remaining <= timeout);
    }

    #[test]
    fn zero_duration_is_already_elapsed() {
        let error = CliCommandDeadline::start(CliCommand::Wait, Duration::ZERO)
            .expect("representable deadline")
            .remaining()
            .expect_err("zero timeout is elapsed");

        assert!(matches!(
            error,
            CliError::CommandTimedOut {
                command: CliCommand::Wait
            }
        ));
    }
}
