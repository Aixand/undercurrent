// SPDX-License-Identifier: Apache-2.0

//! Deterministic restart-policy decisions.

use std::time::Duration;

use crate::{DomainError, DomainResult, InvalidValueReason, TimestampMillis};

/// Policy controlling whether a terminated process is restarted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum RestartPolicy {
    /// Never restart automatically.
    #[default]
    Never,
    /// Restart only after an unsuccessful exit or supervision failure.
    OnFailure,
    /// Restart after every exit while attempts remain.
    Always,
}

/// Validated restart and exponential-backoff configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestartConfig {
    policy: RestartPolicy,
    max_restarts: u32,
    initial_delay: Duration,
    backoff_basis_points: u32,
    max_delay: Duration,
    min_uptime: Duration,
}

impl RestartConfig {
    /// Constructs validated restart configuration.
    ///
    /// A multiplier of 10,000 basis points is 1.0x. The maximum delay cannot be
    /// less than the initial delay. The function performs no IO and does not
    /// panic.
    ///
    /// # Errors
    ///
    /// Returns a validation error for a multiplier below 1.0x or inconsistent
    /// delay bounds.
    pub fn new(
        policy: RestartPolicy,
        max_restarts: u32,
        initial_delay: Duration,
        backoff_basis_points: u32,
        max_delay: Duration,
        min_uptime: Duration,
    ) -> DomainResult<Self> {
        if backoff_basis_points < 10_000 {
            return Err(DomainError::InvalidValue {
                field: "restart_backoff_basis_points",
                reason: InvalidValueReason::OutOfRange {
                    min: 10_000,
                    max: u64::from(u32::MAX),
                },
            });
        }
        if max_delay < initial_delay {
            return Err(DomainError::InvalidValue {
                field: "restart_max_delay",
                reason: InvalidValueReason::InvalidCombination {
                    rule: "maximum restart delay cannot be less than initial delay",
                },
            });
        }
        Ok(Self {
            policy,
            max_restarts,
            initial_delay,
            backoff_basis_points,
            max_delay,
            min_uptime,
        })
    }

    /// Returns configuration that never restarts.
    pub const fn never() -> Self {
        Self {
            policy: RestartPolicy::Never,
            max_restarts: 0,
            initial_delay: Duration::ZERO,
            backoff_basis_points: 10_000,
            max_delay: Duration::ZERO,
            min_uptime: Duration::ZERO,
        }
    }

    /// Returns the restart policy.
    pub const fn policy(&self) -> RestartPolicy {
        self.policy
    }

    /// Returns the maximum consecutive automatic restart attempts.
    pub const fn max_restarts(&self) -> u32 {
        self.max_restarts
    }

    /// Returns the initial backoff delay.
    pub const fn initial_delay(&self) -> Duration {
        self.initial_delay
    }

    /// Returns the exponential multiplier in basis points.
    pub const fn backoff_basis_points(&self) -> u32 {
        self.backoff_basis_points
    }

    /// Returns the maximum backoff delay.
    pub const fn max_delay(&self) -> Duration {
        self.max_delay
    }

    /// Returns uptime that resets the consecutive restart counter.
    pub const fn min_uptime(&self) -> Duration {
        self.min_uptime
    }
}

impl Default for RestartConfig {
    fn default() -> Self {
        Self::never()
    }
}

/// Observed reason a process stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProcessOutcome {
    /// Process exited with an operating-system exit code.
    Exited(i32),
    /// Process supervision failed without a trustworthy exit code.
    SupervisionFailed,
}

impl ProcessOutcome {
    /// Returns whether the outcome represents success.
    pub const fn is_success(self) -> bool {
        matches!(self, Self::Exited(0))
    }
}

/// Reason automatic restart will not occur.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NoRestartReason {
    /// Automatic restart is disabled.
    PolicyDisabled,
    /// `on_failure` policy observed a successful exit.
    SuccessfulExit,
    /// An operator explicitly stopped the session before the pending restart.
    ExplicitStop,
}

/// Deterministic result of applying restart policy to a process outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartDecision {
    /// The session remains terminal for the contained reason.
    DoNotRestart(NoRestartReason),
    /// A restart should occur after the delay.
    RestartAfter {
        /// One-based consecutive automatic restart attempt.
        attempt: u32,
        /// Bounded exponential backoff delay.
        delay: Duration,
    },
    /// Policy permits restart but the configured attempt limit was reached.
    Exhausted {
        /// Number of consecutive attempts already consumed.
        attempts: u32,
    },
}

/// Durable disposition of the most recently evaluated restart decision.
///
/// This state exposes automatic-restart intent without requiring callers to
/// reconstruct it from events or an in-memory timer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RestartStatus {
    /// No automatic restart is currently pending.
    #[default]
    Idle,
    /// A bounded automatic restart attempt is due at or after `not_before`.
    Scheduled {
        /// One-based consecutive automatic restart attempt.
        attempt: u32,
        /// Earliest time at which the attempt may begin.
        not_before: TimestampMillis,
    },
    /// Policy made the last observed terminal outcome final.
    NotScheduled {
        /// Stable reason no automatic restart will occur.
        reason: NoRestartReason,
    },
    /// Policy permitted restart but its bounded attempt count was exhausted.
    Exhausted {
        /// Number of consecutive automatic attempts already consumed.
        attempts: u32,
    },
}

impl RestartStatus {
    /// Converts a policy decision into durable status at `decided_at`.
    ///
    /// Delay addition saturates at the timestamp representation boundary. This
    /// function performs no IO and does not panic.
    pub fn from_decision(decision: RestartDecision, decided_at: TimestampMillis) -> Self {
        match decision {
            RestartDecision::DoNotRestart(reason) => Self::NotScheduled { reason },
            RestartDecision::RestartAfter { attempt, delay } => Self::Scheduled {
                attempt,
                not_before: decided_at.saturating_add(delay),
            },
            RestartDecision::Exhausted { attempts } => Self::Exhausted { attempts },
        }
    }
}

/// Consecutive automatic restart state for one session.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RestartTracker {
    consecutive_attempts: u32,
}

impl RestartTracker {
    /// Reconstructs a tracker from persisted consecutive attempts.
    ///
    /// This function performs no IO, cannot fail, and does not panic.
    pub const fn new(consecutive_attempts: u32) -> Self {
        Self {
            consecutive_attempts,
        }
    }

    /// Returns attempts consumed since the last stable uptime interval.
    pub const fn consecutive_attempts(self) -> u32 {
        self.consecutive_attempts
    }

    /// Applies restart policy to an observed outcome and advances the tracker.
    ///
    /// A non-zero uptime threshold reached by the process resets the prior
    /// consecutive count before this outcome is considered. A zero threshold
    /// disables uptime-based reset. Delay calculation is saturating and always
    /// bounded by `max_delay`. This function performs no IO and does not panic.
    pub fn observe(
        &mut self,
        config: &RestartConfig,
        outcome: ProcessOutcome,
        uptime: Duration,
    ) -> RestartDecision {
        if config.policy == RestartPolicy::Never {
            return RestartDecision::DoNotRestart(NoRestartReason::PolicyDisabled);
        }
        if config.policy == RestartPolicy::OnFailure && outcome.is_success() {
            self.consecutive_attempts = 0;
            return RestartDecision::DoNotRestart(NoRestartReason::SuccessfulExit);
        }
        if !config.min_uptime.is_zero() && uptime >= config.min_uptime {
            self.consecutive_attempts = 0;
        }
        if self.consecutive_attempts >= config.max_restarts {
            return RestartDecision::Exhausted {
                attempts: self.consecutive_attempts,
            };
        }
        let delay = delay_for_attempt(config, self.consecutive_attempts);
        self.consecutive_attempts += 1;
        RestartDecision::RestartAfter {
            attempt: self.consecutive_attempts,
            delay,
        }
    }

    /// Clears consecutive attempts after an explicitly observed stable run.
    ///
    /// This function performs no IO and does not panic.
    pub fn reset(&mut self) {
        self.consecutive_attempts = 0;
    }
}

fn delay_for_attempt(config: &RestartConfig, zero_based_attempt: u32) -> Duration {
    const SCALE: u128 = 10_000;

    let initial_millis = config.initial_delay.as_millis();
    let max_millis = config.max_delay.as_millis();
    if zero_based_attempt == 0 || initial_millis == 0 || config.backoff_basis_points == SCALE as u32
    {
        return config.initial_delay.min(config.max_delay);
    }

    let maximum_factor = max_millis.saturating_mul(SCALE).div_ceil(initial_millis);
    let factor = scaled_power(
        u128::from(config.backoff_basis_points),
        zero_based_attempt,
        maximum_factor,
    );
    let delay_millis = initial_millis
        .saturating_mul(factor)
        .div_ceil(SCALE)
        .min(max_millis);
    let bounded = delay_millis.min(u128::from(u64::MAX));
    Duration::from_millis(u64::try_from(bounded).unwrap_or(u64::MAX))
}

// Persisted attempt counts can approach u32::MAX. Exponentiation by squaring
// keeps work bounded by the exponent's bits instead of the number of retries;
// factors beyond the delay cap cannot change the scheduled delay.
// See [restart bounds](../docs/internals/session-lifecycle.md#restart-intent-survives-the-process).
fn scaled_power(mut base: u128, mut exponent: u32, cap: u128) -> u128 {
    const SCALE: u128 = 10_000;

    let mut result = SCALE.min(cap);
    while exponent > 0 {
        if exponent & 1 == 1 {
            result = scaled_multiply(result, base, cap);
            if result >= cap {
                return cap;
            }
        }
        exponent >>= 1;
        if exponent > 0 {
            base = scaled_multiply(base, base, cap);
        }
    }
    result
}

fn scaled_multiply(left: u128, right: u128, cap: u128) -> u128 {
    const SCALE: u128 = 10_000;

    left.saturating_mul(right).div_ceil(SCALE).min(cap)
}
