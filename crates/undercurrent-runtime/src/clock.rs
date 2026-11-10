// SPDX-License-Identifier: Apache-2.0

//! System time and monotonic session identity adapters.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use undercurrent_application::{Clock, PortError, PortErrorKind, PortOperation, SessionIdSource};
use undercurrent_domain::{SessionId, TimestampMillis};

use crate::RuntimeAdapterFailure;

/// Wall-clock adapter returning signed Unix epoch milliseconds.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> TimestampMillis {
        let now = SystemTime::now();
        match now.duration_since(UNIX_EPOCH) {
            Ok(duration) => {
                TimestampMillis::new(i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
            }
            Err(error) => TimestampMillis::new(
                -i64::try_from(error.duration().as_millis()).unwrap_or(i64::MAX),
            ),
        }
    }
}

/// Process-epoch-prefixed monotonic session identity source.
pub struct MonotonicSessionIdSource {
    prefix: String,
    next: AtomicU64,
}

impl MonotonicSessionIdSource {
    /// Constructs a source whose caller-provided prefix identifies one durable
    /// allocator or daemon epoch.
    ///
    /// Construction performs no IO and does not panic. Prefix validation is
    /// applied to the complete first generated ID.
    ///
    /// # Errors
    ///
    /// Returns typed invalid-data failure for an unsafe or oversized prefix.
    pub fn new(prefix: impl Into<String>) -> Result<Self, PortError> {
        let prefix = prefix.into();
        SessionId::new(format!("{prefix}-1")).map_err(|error| {
            PortError::new(
                PortErrorKind::InvalidData,
                PortOperation::GenerateSessionId,
                None,
                error,
            )
        })?;
        Ok(Self {
            prefix,
            next: AtomicU64::new(0),
        })
    }
}

impl SessionIdSource for MonotonicSessionIdSource {
    fn next_id(&self) -> Result<SessionId, PortError> {
        let value = self
            .next
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map(|previous| previous + 1)
            .map_err(|_| {
                PortError::new(
                    PortErrorKind::CapacityExceeded,
                    PortOperation::GenerateSessionId,
                    None,
                    RuntimeAdapterFailure("session identity counter exhausted"),
                )
            })?;
        SessionId::new(format!("{}-{value}", self.prefix)).map_err(|error| {
            PortError::new(
                PortErrorKind::InvalidData,
                PortOperation::GenerateSessionId,
                None,
                error,
            )
        })
    }
}

/// Operating-system randomness for independent internal mutation identities.
#[derive(Debug, Clone, Copy, Default)]
pub struct RandomOperationAttemptIdSource;

impl undercurrent_application::OperationAttemptIdSource for RandomOperationAttemptIdSource {
    fn next_attempt_id(&self) -> Result<undercurrent_domain::OperationAttemptId, PortError> {
        let mut nonce = [0u8; 32];
        getrandom::fill(&mut nonce).map_err(|error| {
            PortError::new(
                PortErrorKind::Unavailable,
                PortOperation::AcceptOperationAttempt,
                None,
                std::io::Error::other(error.to_string()),
            )
        })?;
        // The namespace is internal; the admission separately records the kind.
        Ok(undercurrent_domain::OperationAttemptId::derived(
            undercurrent_domain::ProcessMutationKind::WriteInput,
            undercurrent_domain::PayloadDigest::of_bytes(&nonce),
        ))
    }
}
