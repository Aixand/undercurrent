// SPDX-License-Identifier: Apache-2.0

//! Explicit timestamp values supplied to the domain by an outer clock port.

use std::time::Duration;

/// Milliseconds from the Unix epoch.
///
/// The domain never reads the system clock. Application code supplies this
/// value through a clock port so decisions remain deterministic in tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimestampMillis(i64);

impl TimestampMillis {
    /// Constructs a timestamp from Unix epoch milliseconds.
    ///
    /// Negative values represent instants before the Unix epoch. The function
    /// performs no IO, cannot fail, and does not panic.
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    /// Returns the Unix epoch millisecond value.
    ///
    /// This function performs no IO and does not panic.
    pub const fn value(self) -> i64 {
        self.0
    }

    /// Returns elapsed time since `earlier`, saturating at zero for reversed
    /// timestamps. Subtraction saturates in the signed millisecond representation,
    /// so a positive difference is bounded by `i64::MAX` milliseconds.
    ///
    /// This function performs no IO and does not panic.
    pub fn saturating_duration_since(self, earlier: Self) -> Duration {
        let millis = self.0.saturating_sub(earlier.0);
        if millis <= 0 {
            return Duration::ZERO;
        }
        u64::try_from(millis)
            .map(Duration::from_millis)
            .unwrap_or(Duration::MAX)
    }

    /// Adds a duration and saturates the final millisecond timestamp at `i64::MAX`.
    ///
    /// Sub-millisecond precision is discarded. A wider intermediate preserves
    /// the full duration when a negative starting timestamp offsets part of it;
    /// see the [time boundary](../docs/reference/values-and-io.md#names-time-and-revisions).
    /// This function performs no IO and does not panic.
    pub fn saturating_add(self, duration: Duration) -> Self {
        // Even Duration's maximum u64 seconds fit in i128 milliseconds. Adding
        // a nonnegative duration cannot underflow the original i64 timestamp.
        let millis = i128::from(duration.as_secs()) * 1_000 + i128::from(duration.subsec_millis());
        Self(i64::try_from(i128::from(self.0) + millis).unwrap_or(i64::MAX))
    }
}
