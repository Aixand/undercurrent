// SPDX-License-Identifier: Apache-2.0

//! Durable coordination for shutting down the isolated process host.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use undercurrent_domain::{
    EffectCertainty, FencingToken, OperationAttempt, OperationAttemptId, OperationAttemptState,
    PayloadDigest, ProcessMutationKind,
};

use crate::{
    Clock, OperationAttemptAcceptance, OperationAttemptAdmission, OperationAttemptSettlement,
    OperationAttemptSettlementOutcome, OperationAttemptStore, PortError, PortFuture,
    ProcessMutationContext, process_unit_result_digest,
};

const SHUTDOWN_PAYLOAD_DOMAIN: &[u8] = b"undercurrent.shutdown-process-host.v0.1.0-beta";

/// Trusted daemon input for one durable private-host shutdown attempt.
///
/// The optional retry identity can originate at an authenticated request
/// boundary. The application owns every other admission field, including the
/// operation kind, target, payload digest, acceptance time, and allocated
/// fence. Graceful shutdown binds its drain timeout into the semantic digest.
/// Reset has no graceful drain policy; its timeout only bounds the daemon's
/// caller wait, so extending that wait must not create a different host effect.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShutdownProcessHostCommand {
    requested_attempt_id: Option<OperationAttemptId>,
    drain_timeout_millis: u64,
    reset: bool,
}

impl ShutdownProcessHostCommand {
    /// Constructs server-owned shutdown admission input.
    pub const fn new(
        requested_attempt_id: Option<OperationAttemptId>,
        drain_timeout_millis: u64,
    ) -> Self {
        Self {
            requested_attempt_id,
            drain_timeout_millis,
            reset: false,
        }
    }

    /// Binds reset cancellation policy into the durable shutdown identity.
    pub const fn for_reset(mut self) -> Self {
        self.reset = true;
        self
    }

    /// Returns the stable retry identity supplied by the authenticated boundary.
    pub const fn requested_attempt_id(&self) -> Option<&OperationAttemptId> {
        self.requested_attempt_id.as_ref()
    }

    /// Returns the daemon wait timeout, bound into graceful shutdown semantics.
    pub const fn drain_timeout_millis(&self) -> u64 {
        self.drain_timeout_millis
    }

    fn admission(
        &self,
        accepted_at: undercurrent_domain::TimestampMillis,
    ) -> OperationAttemptAdmission {
        let timeout = if self.reset {
            0
        } else {
            self.drain_timeout_millis
        }
        .to_be_bytes();
        let payload_digest = PayloadDigest::of_slices([
            if self.reset {
                b"undercurrent.reset-process-host.v0.1.0-beta"
            } else {
                SHUTDOWN_PAYLOAD_DOMAIN
            },
            ProcessMutationKind::Shutdown.as_str().as_bytes(),
            &timeout,
        ]);
        let accepted_at_bytes = accepted_at.value().to_be_bytes();
        let identity_digest =
            PayloadDigest::of_slices([&payload_digest.as_bytes()[..], &accepted_at_bytes]);
        let attempt_id = self.requested_attempt_id.clone().unwrap_or_else(|| {
            OperationAttemptId::derived(ProcessMutationKind::Shutdown, identity_digest)
        });
        OperationAttemptAdmission::new(
            attempt_id,
            None,
            ProcessMutationKind::Shutdown,
            payload_digest,
            accepted_at,
        )
    }
}

/// Adapter capability for one receipt-backed private-host shutdown effect.
pub trait ProcessHostShutdown: Send + Sync {
    /// Invokes or replays the exact admitted shutdown attempt.
    ///
    /// Implementations must retain an idempotent receipt for the supplied
    /// attempt and must not replace the selected host after a known-applied
    /// shutdown. A lost response must be returned as an error rather than
    /// guessed as success.
    fn shutdown_attempted<'a>(
        &'a self,
        context: &'a ProcessMutationContext,
    ) -> PortFuture<'a, Result<(), PortError>>;

    /// Releases the retained shutdown receipt after durable settlement.
    ///
    /// Implementations must make acknowledgement idempotent. For a
    /// known-applied shutdown, this acknowledgement may terminate the retained
    /// private host; it is therefore never called before product settlement.
    fn acknowledge_shutdown<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>>;
}

/// Whether this invocation first applied or durably replayed the shutdown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownProcessHostDisposition {
    /// This invocation began from a newly accepted attempt.
    Applied,
    /// This invocation resumed an accepted or settled exact retry.
    Replayed,
}

/// Durable result returned only after the shutdown settlement and ACK succeed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShutdownProcessHostResult {
    attempt_id: OperationAttemptId,
    fencing_token: FencingToken,
    disposition: ShutdownProcessHostDisposition,
}

impl ShutdownProcessHostResult {
    fn from_attempt(
        attempt: &OperationAttempt,
        disposition: ShutdownProcessHostDisposition,
    ) -> Self {
        Self {
            attempt_id: attempt.id().clone(),
            fencing_token: attempt.fencing_token(),
            disposition,
        }
    }

    /// Returns the exact durable shutdown-attempt identity.
    pub const fn attempt_id(&self) -> &OperationAttemptId {
        &self.attempt_id
    }

    /// Returns the server-allocated fence used by the private host.
    pub const fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }

    /// Returns whether this call applied or replayed the attempt.
    pub const fn disposition(&self) -> ShutdownProcessHostDisposition {
        self.disposition
    }
}

/// Typed failure from durable private-host shutdown coordination.
#[derive(Debug)]
pub enum ShutdownProcessHostError {
    /// Atomic attempt admission failed before private-host IO.
    Admission(Box<PortError>),
    /// The retry identity is already bound to different shutdown semantics.
    PayloadConflict,
    /// Durable target state has advanced past the returned fence.
    StaleFence {
        /// Current durable fence.
        current: FencingToken,
    },
    /// The attempt store returned a value outside its application contract.
    StoreContractViolation,
    /// This exact attempt is durably unresolved and cannot be executed again.
    UnresolvedEffect {
        /// Blocked attempt identity.
        attempt_id: OperationAttemptId,
    },
    /// This exact attempt was terminalized as known absent and needs a new ID.
    KnownAbsent {
        /// Terminal attempt identity.
        attempt_id: OperationAttemptId,
    },
    /// The host proved that this newly admitted attempt had no side effect.
    RejectedKnownAbsent {
        /// Terminal attempt identity.
        attempt_id: OperationAttemptId,
        /// Typed host rejection carrying affirmative effect certainty.
        source: Box<PortError>,
    },
    /// The host proved no effect, but durable KnownAbsent settlement failed.
    KnownAbsentSettlement {
        /// Typed host rejection carrying affirmative effect certainty.
        effect: Box<PortError>,
        /// Durable settlement failure.
        settlement: Box<PortError>,
    },
    /// The host proved no effect, but the store rejected its settlement.
    KnownAbsentSettlementRejected {
        /// Typed host rejection carrying affirmative effect certainty.
        effect: Box<PortError>,
    },
    /// The host response was inconclusive and Unknown was durably committed.
    EffectUnknown(Box<PortError>),
    /// The host response was inconclusive and committing Unknown also failed.
    UnknownSettlement {
        /// Original private-host failure.
        effect: Box<PortError>,
        /// Durable settlement failure.
        settlement: Box<PortError>,
    },
    /// The host response was inconclusive but the store rejected settlement.
    UnknownSettlementRejected {
        /// Original private-host failure.
        effect: Box<PortError>,
    },
    /// Shutdown was known applied, but its durable settlement call failed.
    SettlementAfterEffect(Box<PortError>),
    /// Shutdown was known applied, but the store rejected its settlement.
    SettlementRejected,
    /// Settlement committed, but retained receipt acknowledgement must retry.
    AcknowledgementAfterCommit(Box<PortError>),
}

impl Display for ShutdownProcessHostError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Admission(_) => "process-host shutdown admission failed",
            Self::PayloadConflict => "process-host shutdown retry conflicts with durable input",
            Self::StaleFence { .. } => "process-host shutdown fence is stale",
            Self::StoreContractViolation => {
                "process-host shutdown store violated its application contract"
            }
            Self::UnresolvedEffect { .. } => {
                "process-host shutdown effect is unresolved and blocks retry"
            }
            Self::KnownAbsent { .. } => "process-host shutdown attempt is terminally known absent",
            Self::RejectedKnownAbsent { .. } => {
                "process-host shutdown was rejected before any side effect"
            }
            Self::KnownAbsentSettlement { .. } => {
                "process-host shutdown was rejected but KnownAbsent settlement failed"
            }
            Self::KnownAbsentSettlementRejected { .. } => {
                "process-host shutdown KnownAbsent settlement was rejected"
            }
            Self::EffectUnknown(_) => "process-host shutdown effect is durably unknown",
            Self::UnknownSettlement { .. } => {
                "process-host shutdown effect and Unknown settlement both failed"
            }
            Self::UnknownSettlementRejected { .. } => {
                "process-host shutdown Unknown settlement was rejected"
            }
            Self::SettlementAfterEffect(_) => "process-host shutdown applied but settlement failed",
            Self::SettlementRejected => "process-host shutdown settlement was rejected",
            Self::AcknowledgementAfterCommit(_) => {
                "process-host shutdown committed but acknowledgement is pending"
            }
        })
    }
}

impl Error for ShutdownProcessHostError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Admission(source)
            | Self::EffectUnknown(source)
            | Self::SettlementAfterEffect(source)
            | Self::AcknowledgementAfterCommit(source) => Some(source.as_ref()),
            Self::RejectedKnownAbsent { source, .. } => Some(source.as_ref()),
            Self::KnownAbsentSettlement { settlement, .. } => Some(settlement.as_ref()),
            Self::KnownAbsentSettlementRejected { effect } => Some(effect.as_ref()),
            Self::UnknownSettlement { settlement, .. } => Some(settlement.as_ref()),
            Self::UnknownSettlementRejected { effect } => Some(effect.as_ref()),
            Self::PayloadConflict
            | Self::StaleFence { .. }
            | Self::StoreContractViolation
            | Self::UnresolvedEffect { .. }
            | Self::KnownAbsent { .. }
            | Self::SettlementRejected => None,
        }
    }
}

/// Durably admits, applies, settles, and acknowledges private-host shutdown.
pub struct ShutdownProcessHostService<'a> {
    attempts: &'a dyn OperationAttemptStore,
    host: &'a dyn ProcessHostShutdown,
    clock: &'a dyn Clock,
}

impl<'a> ShutdownProcessHostService<'a> {
    /// Constructs the application use case from narrow inward-facing ports.
    pub const fn new(
        attempts: &'a dyn OperationAttemptStore,
        host: &'a dyn ProcessHostShutdown,
        clock: &'a dyn Clock,
    ) -> Self {
        Self {
            attempts,
            host,
            clock,
        }
    }

    /// Executes an exact durable shutdown attempt.
    ///
    /// Admission always commits before private-host IO. Success commits
    /// `KnownApplied` before ACK. An inconclusive adapter failure commits
    /// `Unknown`, does not ACK, and prevents this exact attempt from executing
    /// again until explicit reconciliation establishes terminal truth.
    pub async fn execute(
        &self,
        command: ShutdownProcessHostCommand,
    ) -> Result<ShutdownProcessHostResult, ShutdownProcessHostError> {
        let admission = command.admission(self.clock.now());
        let acceptance = self
            .attempts
            .accept_next(&admission)
            .await
            .map_err(|source| ShutdownProcessHostError::Admission(Box::new(source)))?;
        match acceptance {
            OperationAttemptAcceptance::Accepted(attempt) => {
                self.apply(&admission, attempt, ShutdownProcessHostDisposition::Applied)
                    .await
            }
            OperationAttemptAcceptance::Duplicate(attempt) => {
                self.apply(
                    &admission,
                    attempt,
                    ShutdownProcessHostDisposition::Replayed,
                )
                .await
            }
            OperationAttemptAcceptance::AlreadySettled(attempt) => {
                self.replay_settled(&admission, attempt).await
            }
            OperationAttemptAcceptance::AlreadyCompensated(_) => {
                Err(ShutdownProcessHostError::PayloadConflict)
            }
            OperationAttemptAcceptance::StaleFence { current } => {
                Err(ShutdownProcessHostError::StaleFence { current })
            }
            OperationAttemptAcceptance::PayloadConflict => {
                Err(ShutdownProcessHostError::PayloadConflict)
            }
        }
    }

    async fn apply(
        &self,
        admission: &OperationAttemptAdmission,
        attempt: OperationAttempt,
        disposition: ShutdownProcessHostDisposition,
    ) -> Result<ShutdownProcessHostResult, ShutdownProcessHostError> {
        if !valid_attempt(admission, &attempt)
            || !matches!(attempt.state(), OperationAttemptState::Accepted)
        {
            return Err(ShutdownProcessHostError::StoreContractViolation);
        }
        let context = ProcessMutationContext::from_attempt(&attempt);
        match self.host.shutdown_attempted(&context).await {
            Ok(()) => {
                let result_digest = process_unit_result_digest(&context);
                let settlement = OperationAttemptSettlement {
                    attempt_id: context.attempt_id().clone(),
                    fencing_token: context.fencing_token(),
                    certainty: EffectCertainty::KnownApplied,
                    result_digest: Some(result_digest),
                    settled_at: self.clock.now(),
                };
                let outcome = self.attempts.settle(&settlement).await.map_err(|source| {
                    ShutdownProcessHostError::SettlementAfterEffect(Box::new(source))
                })?;
                let (committed, disposition) = match outcome {
                    OperationAttemptSettlementOutcome::Settled(committed) => {
                        (committed, disposition)
                    }
                    OperationAttemptSettlementOutcome::Duplicate(committed) => {
                        (committed, ShutdownProcessHostDisposition::Replayed)
                    }
                    OperationAttemptSettlementOutcome::StaleFence { .. }
                    | OperationAttemptSettlementOutcome::NotFound
                    | OperationAttemptSettlementOutcome::PayloadConflict => {
                        return Err(ShutdownProcessHostError::SettlementRejected);
                    }
                };
                if !valid_known_applied(admission, &committed, result_digest) {
                    return Err(ShutdownProcessHostError::StoreContractViolation);
                }
                self.acknowledge(&committed).await?;
                Ok(ShutdownProcessHostResult::from_attempt(
                    &committed,
                    disposition,
                ))
            }
            Err(effect) if effect.effect_certainty() == Some(EffectCertainty::KnownAbsent) => {
                self.settle_known_absent(&context, effect).await
            }
            Err(effect) => self.settle_unknown(&context, effect).await,
        }
    }

    async fn replay_settled(
        &self,
        admission: &OperationAttemptAdmission,
        attempt: OperationAttempt,
    ) -> Result<ShutdownProcessHostResult, ShutdownProcessHostError> {
        if !valid_attempt(admission, &attempt) {
            return Err(ShutdownProcessHostError::StoreContractViolation);
        }
        let context = ProcessMutationContext::from_attempt(&attempt);
        match attempt.state() {
            OperationAttemptState::Settled {
                certainty: EffectCertainty::KnownApplied,
                result_digest: Some(result_digest),
                ..
            } if *result_digest == process_unit_result_digest(&context) => {
                self.acknowledge(&attempt).await?;
                Ok(ShutdownProcessHostResult::from_attempt(
                    &attempt,
                    ShutdownProcessHostDisposition::Replayed,
                ))
            }
            OperationAttemptState::Settled {
                certainty: EffectCertainty::Unknown,
                ..
            } => Err(ShutdownProcessHostError::UnresolvedEffect {
                attempt_id: attempt.id().clone(),
            }),
            OperationAttemptState::Settled {
                certainty: EffectCertainty::KnownAbsent,
                ..
            } => Err(ShutdownProcessHostError::KnownAbsent {
                attempt_id: attempt.id().clone(),
            }),
            OperationAttemptState::Accepted
            | OperationAttemptState::Compensated { .. }
            | OperationAttemptState::Settled {
                certainty: EffectCertainty::KnownApplied,
                ..
            } => Err(ShutdownProcessHostError::StoreContractViolation),
        }
    }

    async fn settle_unknown(
        &self,
        context: &ProcessMutationContext,
        effect: PortError,
    ) -> Result<ShutdownProcessHostResult, ShutdownProcessHostError> {
        let settlement = OperationAttemptSettlement {
            attempt_id: context.attempt_id().clone(),
            fencing_token: context.fencing_token(),
            certainty: EffectCertainty::Unknown,
            result_digest: None,
            settled_at: self.clock.now(),
        };
        let outcome = match self.attempts.settle(&settlement).await {
            Ok(outcome) => outcome,
            Err(settlement) => {
                return Err(ShutdownProcessHostError::UnknownSettlement {
                    effect: Box::new(effect),
                    settlement: Box::new(settlement),
                });
            }
        };
        match outcome {
            OperationAttemptSettlementOutcome::Settled(committed)
            | OperationAttemptSettlementOutcome::Duplicate(committed)
                if matches!(
                    committed.state(),
                    OperationAttemptState::Settled {
                        certainty: EffectCertainty::Unknown,
                        result_digest: None,
                        ..
                    }
                ) =>
            {
                Err(ShutdownProcessHostError::EffectUnknown(Box::new(effect)))
            }
            OperationAttemptSettlementOutcome::Settled(_)
            | OperationAttemptSettlementOutcome::Duplicate(_) => {
                Err(ShutdownProcessHostError::StoreContractViolation)
            }
            OperationAttemptSettlementOutcome::StaleFence { .. }
            | OperationAttemptSettlementOutcome::NotFound
            | OperationAttemptSettlementOutcome::PayloadConflict => {
                Err(ShutdownProcessHostError::UnknownSettlementRejected {
                    effect: Box::new(effect),
                })
            }
        }
    }

    async fn settle_known_absent(
        &self,
        context: &ProcessMutationContext,
        source: PortError,
    ) -> Result<ShutdownProcessHostResult, ShutdownProcessHostError> {
        let settlement = OperationAttemptSettlement {
            attempt_id: context.attempt_id().clone(),
            fencing_token: context.fencing_token(),
            certainty: EffectCertainty::KnownAbsent,
            result_digest: None,
            settled_at: self.clock.now(),
        };
        let outcome = match self.attempts.settle(&settlement).await {
            Ok(outcome) => outcome,
            Err(settlement) => {
                return Err(ShutdownProcessHostError::KnownAbsentSettlement {
                    effect: Box::new(source),
                    settlement: Box::new(settlement),
                });
            }
        };
        match outcome {
            OperationAttemptSettlementOutcome::Settled(committed)
            | OperationAttemptSettlementOutcome::Duplicate(committed)
                if matches!(
                    committed.state(),
                    OperationAttemptState::Settled {
                        certainty: EffectCertainty::KnownAbsent,
                        result_digest: None,
                        ..
                    }
                ) =>
            {
                Err(ShutdownProcessHostError::RejectedKnownAbsent {
                    attempt_id: committed.id().clone(),
                    source: Box::new(source),
                })
            }
            OperationAttemptSettlementOutcome::Settled(_)
            | OperationAttemptSettlementOutcome::Duplicate(_) => {
                Err(ShutdownProcessHostError::StoreContractViolation)
            }
            OperationAttemptSettlementOutcome::StaleFence { .. }
            | OperationAttemptSettlementOutcome::NotFound
            | OperationAttemptSettlementOutcome::PayloadConflict => {
                Err(ShutdownProcessHostError::KnownAbsentSettlementRejected {
                    effect: Box::new(source),
                })
            }
        }
    }

    async fn acknowledge(
        &self,
        attempt: &OperationAttempt,
    ) -> Result<(), ShutdownProcessHostError> {
        self.host
            .acknowledge_shutdown(attempt)
            .await
            .map_err(|source| {
                ShutdownProcessHostError::AcknowledgementAfterCommit(Box::new(source))
            })
    }
}

fn valid_attempt(admission: &OperationAttemptAdmission, attempt: &OperationAttempt) -> bool {
    admission.has_same_semantics(attempt)
        && attempt.target_session().is_none()
        && attempt.kind() == ProcessMutationKind::Shutdown
}

fn valid_known_applied(
    admission: &OperationAttemptAdmission,
    attempt: &OperationAttempt,
    expected_result_digest: PayloadDigest,
) -> bool {
    valid_attempt(admission, attempt)
        && matches!(
            attempt.state(),
            OperationAttemptState::Settled {
                certainty: EffectCertainty::KnownApplied,
                result_digest: Some(result_digest),
                ..
            } if *result_digest == expected_result_digest
        )
}

#[cfg(test)]
mod tests {
    use std::fmt::{Display, Formatter};
    use std::future::Future;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Wake, Waker};

    use undercurrent_domain::{
        EffectCertainty, FencingToken, OperationAttempt, OperationAttemptId, OperationAttemptState,
        OperationAttemptTransition, TimestampMillis,
    };

    use super::*;
    use crate::{PortErrorKind, PortOperation};

    struct FixedClock(TimestampMillis);

    impl Clock for FixedClock {
        fn now(&self) -> TimestampMillis {
            self.0
        }
    }

    struct MemoryAttemptStore {
        state: Mutex<Option<OperationAttempt>>,
        last_admission: Mutex<Option<OperationAttemptAdmission>>,
        stale_next: AtomicBool,
        trace: Arc<Mutex<Vec<&'static str>>>,
    }

    impl MemoryAttemptStore {
        fn new(trace: Arc<Mutex<Vec<&'static str>>>) -> Self {
            Self {
                state: Mutex::new(None),
                last_admission: Mutex::new(None),
                stale_next: AtomicBool::new(false),
                trace,
            }
        }

        fn seed_accepted(
            &self,
            command: &ShutdownProcessHostCommand,
            accepted_at: TimestampMillis,
        ) {
            let admission = command.admission(accepted_at);
            let attempt = accepted(&admission);
            *self.state.lock().expect("state lock") = Some(attempt);
        }

        fn state(&self) -> OperationAttempt {
            self.state
                .lock()
                .expect("state lock")
                .clone()
                .expect("durable attempt")
        }

        fn last_admission(&self) -> OperationAttemptAdmission {
            self.last_admission
                .lock()
                .expect("admission lock")
                .clone()
                .expect("recorded admission")
        }
    }

    impl OperationAttemptStore for MemoryAttemptStore {
        fn accept_next<'a>(
            &'a self,
            admission: &'a OperationAttemptAdmission,
        ) -> PortFuture<'a, Result<OperationAttemptAcceptance, PortError>> {
            self.trace.lock().expect("trace lock").push("accept");
            *self.last_admission.lock().expect("admission lock") = Some(admission.clone());
            let outcome = if self.stale_next.swap(false, Ordering::SeqCst) {
                OperationAttemptAcceptance::StaleFence { current: fence(9) }
            } else {
                let mut state = self.state.lock().expect("state lock");
                match state.as_ref() {
                    Some(current) if !admission.has_same_semantics(current) => {
                        OperationAttemptAcceptance::PayloadConflict
                    }
                    Some(current) if matches!(current.state(), OperationAttemptState::Accepted) => {
                        OperationAttemptAcceptance::Duplicate(current.clone())
                    }
                    Some(current) => OperationAttemptAcceptance::AlreadySettled(current.clone()),
                    None => {
                        let attempt = accepted(admission);
                        *state = Some(attempt.clone());
                        OperationAttemptAcceptance::Accepted(attempt)
                    }
                }
            };
            Box::pin(async move { Ok(outcome) })
        }

        fn accept<'a>(
            &'a self,
            _attempt: &'a OperationAttempt,
        ) -> PortFuture<'a, Result<OperationAttemptAcceptance, PortError>> {
            Box::pin(async { Err(test_port_error()) })
        }

        fn settle<'a>(
            &'a self,
            settlement: &'a OperationAttemptSettlement,
        ) -> PortFuture<'a, Result<OperationAttemptSettlementOutcome, PortError>> {
            self.trace.lock().expect("trace lock").push("settle");
            let outcome = match self.state.lock().expect("state lock").as_mut() {
                None => OperationAttemptSettlementOutcome::NotFound,
                Some(attempt) if attempt.fencing_token() != settlement.fencing_token => {
                    OperationAttemptSettlementOutcome::StaleFence {
                        current: attempt.fencing_token(),
                    }
                }
                Some(attempt) if attempt.id() != &settlement.attempt_id => {
                    OperationAttemptSettlementOutcome::PayloadConflict
                }
                Some(attempt) => match attempt.settle(
                    settlement.fencing_token,
                    settlement.certainty,
                    settlement.result_digest,
                    settlement.settled_at,
                ) {
                    Ok(OperationAttemptTransition::Applied) => {
                        OperationAttemptSettlementOutcome::Settled(attempt.clone())
                    }
                    Ok(OperationAttemptTransition::Duplicate) => {
                        OperationAttemptSettlementOutcome::Duplicate(attempt.clone())
                    }
                    Err(_) => OperationAttemptSettlementOutcome::PayloadConflict,
                },
            };
            Box::pin(async move { Ok(outcome) })
        }

        fn reconcile<'a>(
            &'a self,
            _reconciliation: &'a crate::OperationAttemptReconciliation,
        ) -> PortFuture<'a, Result<OperationAttemptSettlementOutcome, PortError>> {
            Box::pin(async { Err(test_port_error()) })
        }

        fn load<'a>(
            &'a self,
            _attempt_id: &'a OperationAttemptId,
        ) -> PortFuture<'a, Result<Option<OperationAttempt>, PortError>> {
            let attempt = self.state.lock().expect("state lock").clone();
            Box::pin(async move { Ok(attempt) })
        }

        fn pending_reconciliation<'a>(
            &'a self,
            _scan: &'a crate::OperationAttemptScan,
        ) -> PortFuture<'a, Result<Vec<OperationAttempt>, PortError>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    struct FakeHost {
        effect_failures: AtomicUsize,
        effect_failure_certainty: EffectCertainty,
        acknowledgement_failures: AtomicUsize,
        effect_calls: AtomicUsize,
        acknowledgement_calls: AtomicUsize,
        trace: Arc<Mutex<Vec<&'static str>>>,
    }

    impl FakeHost {
        fn new(
            trace: Arc<Mutex<Vec<&'static str>>>,
            effect_failures: usize,
            acknowledgement_failures: usize,
        ) -> Self {
            Self {
                effect_failures: AtomicUsize::new(effect_failures),
                effect_failure_certainty: EffectCertainty::Unknown,
                acknowledgement_failures: AtomicUsize::new(acknowledgement_failures),
                effect_calls: AtomicUsize::new(0),
                acknowledgement_calls: AtomicUsize::new(0),
                trace,
            }
        }

        fn with_effect_failure_certainty(mut self, certainty: EffectCertainty) -> Self {
            self.effect_failure_certainty = certainty;
            self
        }
    }

    impl ProcessHostShutdown for FakeHost {
        fn shutdown_attempted<'a>(
            &'a self,
            context: &'a ProcessMutationContext,
        ) -> PortFuture<'a, Result<(), PortError>> {
            self.trace.lock().expect("trace lock").push("effect");
            self.effect_calls.fetch_add(1, Ordering::SeqCst);
            let fails = decrement_if_positive(&self.effect_failures);
            let attempt_id = context.attempt_id().clone();
            let certainty = self.effect_failure_certainty;
            Box::pin(async move {
                if fails {
                    Err(test_port_error().with_attempt_effect(attempt_id, certainty))
                } else {
                    Ok(())
                }
            })
        }

        fn acknowledge_shutdown<'a>(
            &'a self,
            _attempt: &'a OperationAttempt,
        ) -> PortFuture<'a, Result<(), PortError>> {
            self.trace.lock().expect("trace lock").push("ack");
            self.acknowledgement_calls.fetch_add(1, Ordering::SeqCst);
            let fails = decrement_if_positive(&self.acknowledgement_failures);
            Box::pin(async move {
                if fails {
                    Err(test_port_error())
                } else {
                    Ok(())
                }
            })
        }
    }

    #[test]
    fn admission_effect_settlement_and_acknowledgement_are_strictly_ordered() {
        let trace = Arc::new(Mutex::new(Vec::new()));
        let store = MemoryAttemptStore::new(trace.clone());
        let host = FakeHost::new(trace.clone(), 0, 0);
        let clock = FixedClock(timestamp(100));
        let command = command("shutdown-order", 5_000);

        let result =
            block_on(ShutdownProcessHostService::new(&store, &host, &clock).execute(command))
                .expect("durable shutdown");

        assert_eq!(
            result.disposition(),
            ShutdownProcessHostDisposition::Applied
        );
        assert_eq!(
            *trace.lock().expect("trace lock"),
            ["accept", "effect", "settle", "ack"]
        );
        assert!(matches!(
            store.state().state(),
            OperationAttemptState::Settled {
                certainty: EffectCertainty::KnownApplied,
                result_digest: Some(_),
                ..
            }
        ));
        let admission = store.last_admission();
        assert_eq!(admission.kind(), ProcessMutationKind::Shutdown);
        assert!(admission.target_session().is_none());
        assert_eq!(admission.accepted_at(), timestamp(100));
    }

    #[test]
    fn duplicate_in_flight_attempt_replays_effect_but_settled_retry_only_acknowledges() {
        let trace = Arc::new(Mutex::new(Vec::new()));
        let store = MemoryAttemptStore::new(trace.clone());
        let host = FakeHost::new(trace.clone(), 0, 0);
        let clock = FixedClock(timestamp(100));
        let command = command("shutdown-duplicate", 5_000);
        store.seed_accepted(&command, clock.now());
        let service = ShutdownProcessHostService::new(&store, &host, &clock);

        let first = block_on(service.execute(command.clone())).expect("accepted retry");
        let second = block_on(service.execute(command)).expect("settled retry");

        assert_eq!(
            first.disposition(),
            ShutdownProcessHostDisposition::Replayed
        );
        assert_eq!(
            second.disposition(),
            ShutdownProcessHostDisposition::Replayed
        );
        assert_eq!(host.effect_calls.load(Ordering::SeqCst), 1);
        assert_eq!(host.acknowledgement_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            *trace.lock().expect("trace lock"),
            ["accept", "effect", "settle", "ack", "accept", "ack"]
        );
    }

    #[test]
    fn acknowledgement_loss_observes_known_applied_settlement_before_retry() {
        let trace = Arc::new(Mutex::new(Vec::new()));
        let store = MemoryAttemptStore::new(trace.clone());
        let host = FakeHost::new(trace.clone(), 0, 1);
        let clock = FixedClock(timestamp(100));
        let command = command("shutdown-ack-loss", 5_000);
        let service = ShutdownProcessHostService::new(&store, &host, &clock);

        let error = block_on(service.execute(command.clone())).expect_err("first ACK is lost");
        assert!(matches!(
            error,
            ShutdownProcessHostError::AcknowledgementAfterCommit(_)
        ));
        assert!(matches!(
            store.state().state(),
            OperationAttemptState::Settled {
                certainty: EffectCertainty::KnownApplied,
                ..
            }
        ));
        let retry = block_on(service.execute(command)).expect("retry only ACKs");
        assert_eq!(
            retry.disposition(),
            ShutdownProcessHostDisposition::Replayed
        );
        assert_eq!(host.effect_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            *trace.lock().expect("trace lock"),
            ["accept", "effect", "settle", "ack", "accept", "ack"]
        );
    }

    #[test]
    fn unknown_effect_is_durable_and_blocks_effect_and_acknowledgement_on_retry() {
        let trace = Arc::new(Mutex::new(Vec::new()));
        let store = MemoryAttemptStore::new(trace.clone());
        let host = FakeHost::new(trace.clone(), 1, 0);
        let clock = FixedClock(timestamp(100));
        let command = command("shutdown-unknown", 5_000);
        let service = ShutdownProcessHostService::new(&store, &host, &clock);

        let first = block_on(service.execute(command.clone())).expect_err("effect is unknown");
        assert!(matches!(first, ShutdownProcessHostError::EffectUnknown(_)));
        assert!(matches!(
            store.state().state(),
            OperationAttemptState::Settled {
                certainty: EffectCertainty::Unknown,
                result_digest: None,
                ..
            }
        ));
        let retry = block_on(service.execute(command)).expect_err("Unknown fails closed");
        assert!(matches!(
            retry,
            ShutdownProcessHostError::UnresolvedEffect { .. }
        ));
        assert_eq!(host.effect_calls.load(Ordering::SeqCst), 1);
        assert_eq!(host.acknowledgement_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            *trace.lock().expect("trace lock"),
            ["accept", "effect", "settle", "accept"]
        );
    }

    #[test]
    fn known_absent_host_rejection_is_terminal_without_blocking_recovery() {
        let trace = Arc::new(Mutex::new(Vec::new()));
        let store = MemoryAttemptStore::new(trace.clone());
        let host = FakeHost::new(trace.clone(), 1, 0)
            .with_effect_failure_certainty(EffectCertainty::KnownAbsent);
        let clock = FixedClock(timestamp(100));
        let command = command("shutdown-known-absent", 5_000);
        let service = ShutdownProcessHostService::new(&store, &host, &clock);

        let first = block_on(service.execute(command.clone()))
            .expect_err("host rejected the attempt before any effect");
        assert!(matches!(
            first,
            ShutdownProcessHostError::RejectedKnownAbsent { .. }
        ));
        assert!(matches!(
            store.state().state(),
            OperationAttemptState::Settled {
                certainty: EffectCertainty::KnownAbsent,
                result_digest: None,
                ..
            }
        ));

        let retry = block_on(service.execute(command)).expect_err("known absent retry is terminal");
        assert!(matches!(
            retry,
            ShutdownProcessHostError::KnownAbsent { .. }
        ));
        assert_eq!(host.effect_calls.load(Ordering::SeqCst), 1);
        assert_eq!(host.acknowledgement_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            *trace.lock().expect("trace lock"),
            ["accept", "effect", "settle", "accept"]
        );
    }

    #[test]
    fn payload_and_stale_fence_admission_conflicts_stop_before_host_io() {
        let trace = Arc::new(Mutex::new(Vec::new()));
        let store = MemoryAttemptStore::new(trace.clone());
        let host = FakeHost::new(trace.clone(), 0, 0);
        let clock = FixedClock(timestamp(100));
        let original = command("shutdown-conflict", 5_000);
        store.seed_accepted(&original, clock.now());
        let service = ShutdownProcessHostService::new(&store, &host, &clock);

        let payload_conflict = block_on(service.execute(command("shutdown-conflict", 5_001)))
            .expect_err("changed timeout conflicts");
        assert!(matches!(
            payload_conflict,
            ShutdownProcessHostError::PayloadConflict
        ));

        let stale_trace = Arc::new(Mutex::new(Vec::new()));
        let stale_store = MemoryAttemptStore::new(stale_trace.clone());
        stale_store.stale_next.store(true, Ordering::SeqCst);
        let stale_host = FakeHost::new(stale_trace.clone(), 0, 0);
        let stale = block_on(
            ShutdownProcessHostService::new(&stale_store, &stale_host, &clock)
                .execute(command("shutdown-stale", 5_000)),
        )
        .expect_err("stale fence conflicts");
        assert!(matches!(
            stale,
            ShutdownProcessHostError::StaleFence { current } if current == fence(9)
        ));
        assert_eq!(host.effect_calls.load(Ordering::SeqCst), 0);
        assert_eq!(stale_host.effect_calls.load(Ordering::SeqCst), 0);
        assert_eq!(*trace.lock().expect("trace lock"), ["accept"]);
        assert_eq!(*stale_trace.lock().expect("trace lock"), ["accept"]);
    }

    #[test]
    fn reset_mode_cannot_replay_a_graceful_shutdown_attempt() {
        let trace = Arc::new(Mutex::new(Vec::new()));
        let store = MemoryAttemptStore::new(trace.clone());
        let host = FakeHost::new(trace.clone(), 0, 0);
        let clock = FixedClock(timestamp(100));
        store.seed_accepted(&command("mode-conflict", 5_000), clock.now());
        let error = block_on(
            ShutdownProcessHostService::new(&store, &host, &clock)
                .execute(command("mode-conflict", 5_000).for_reset()),
        )
        .unwrap_err();
        assert!(matches!(error, ShutdownProcessHostError::PayloadConflict));
        assert_eq!(host.effect_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn reset_retry_with_more_wait_time_only_acknowledges_the_settled_effect() {
        let trace = Arc::new(Mutex::new(Vec::new()));
        let store = MemoryAttemptStore::new(trace.clone());
        let host = FakeHost::new(trace, 0, 1);
        let clock = FixedClock(timestamp(100));
        let service = ShutdownProcessHostService::new(&store, &host, &clock);
        assert!(matches!(
            block_on(service.execute(command("reset-retry", 100).for_reset())),
            Err(ShutdownProcessHostError::AcknowledgementAfterCommit(_))
        ));
        let retry = block_on(service.execute(command("reset-retry", 5_000).for_reset())).unwrap();
        assert_eq!(
            retry.disposition(),
            ShutdownProcessHostDisposition::Replayed
        );
        assert_eq!(host.effect_calls.load(Ordering::SeqCst), 1);
        assert_eq!(host.acknowledgement_calls.load(Ordering::SeqCst), 2);
    }

    fn accepted(admission: &OperationAttemptAdmission) -> OperationAttempt {
        OperationAttempt::accepted(
            admission.attempt_id().clone(),
            admission.target_session().cloned(),
            admission.kind(),
            fence(7),
            admission.payload_digest(),
            admission.accepted_at(),
        )
    }

    fn command(id: &str, drain_timeout_millis: u64) -> ShutdownProcessHostCommand {
        ShutdownProcessHostCommand::new(
            Some(OperationAttemptId::new(id).expect("valid attempt ID")),
            drain_timeout_millis,
        )
    }

    fn timestamp(value: i64) -> TimestampMillis {
        TimestampMillis::new(value)
    }

    fn fence(value: u64) -> FencingToken {
        FencingToken::new(value).expect("nonzero fence")
    }

    fn decrement_if_positive(counter: &AtomicUsize) -> bool {
        counter
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_sub(1)
            })
            .is_ok()
    }

    fn test_port_error() -> PortError {
        PortError::new(
            PortErrorKind::Unavailable,
            PortOperation::ShutdownProcessHost,
            None,
            TestFailure,
        )
    }

    #[derive(Debug)]
    struct TestFailure;

    impl Display for TestFailure {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
            formatter.write_str("test failure")
        }
    }

    impl Error for TestFailure {}

    fn block_on<F: Future>(future: F) -> F::Output {
        let mut future = Box::pin(future);
        let waker = Waker::from(Arc::new(NoopWake));
        let mut context = Context::from_waker(&waker);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => return output,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }
}
