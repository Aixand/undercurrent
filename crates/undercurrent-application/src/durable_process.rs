// SPDX-License-Identifier: Apache-2.0

//! Durable attempt coordination around process-only runtime adapters.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use undercurrent_domain::{
    EffectCertainty, InputDeliveryPolicy, InputSequence, LaunchSpec, OperationAttempt,
    OperationAttemptState, PayloadDigest, ProcessInstanceId, SessionId, StopPolicy, TerminalSize,
};

use crate::{
    Clock, OperationAttemptAcceptance, OperationAttemptAdmission, OperationAttemptReconciliation,
    OperationAttemptSettlement, OperationAttemptSettlementOutcome, OperationAttemptStore,
    PortError, PortErrorKind, PortFuture, PortOperation, ProcessInstance, ProcessMutationContext,
    ProcessRuntime, ProcessTermination, StandardInputClosure, TerminationKind,
};

struct AdmittedProcessMutation {
    context: ProcessMutationContext,
    reconcile_unknown: bool,
}

/// Coordinates durable attempts for the admitted process-mutation entry points.
///
/// The `*_admitted` methods accept before calling the inner attempt-aware port.
/// Most settle before acknowledging known effects; externally dispatched spawn
/// success remains Accepted until `StartSessionService` commits activation.
/// These steps are not one atomic transaction. Plain process methods and
/// standard-input closure delegate directly, bypassing attempt coordination. Safe continuation of an internal
/// Accepted or Unknown attempt therefore also requires an inner runtime that
/// preserves exact attempt receipts; the trait's default `*_attempted` methods
/// alone do not provide replay protection.
pub struct DurableProcessRuntime {
    inner: Arc<dyn ProcessRuntime>,
    attempts: Arc<dyn OperationAttemptStore>,
    clock: Arc<dyn Clock>,
}

impl DurableProcessRuntime {
    /// Wraps owned process, attempt-store, and clock ports for safe long-lived composition.
    ///
    /// Shared owned ports let composition roots retain the decorator without
    /// leaks or self-referential owners. Construction performs no IO and does
    /// not panic.
    pub fn new(
        inner: Arc<dyn ProcessRuntime>,
        attempts: Arc<dyn OperationAttemptStore>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            inner,
            attempts,
            clock,
        }
    }

    async fn admit(
        &self,
        admission: &OperationAttemptAdmission,
        session_id: &SessionId,
    ) -> Result<AdmittedProcessMutation, PortError> {
        let externally_fenced = admission.supplied_fencing_token().is_some();
        if externally_fenced {
            let current_policy = admission
                .dispatch_binding()
                .and_then(|binding| binding.runtime_policy());
            if current_policy.is_none_or(|policy| policy.validate_at(self.clock.now()).is_err()) {
                return Err(attempt_rejected(
                    session_id,
                    admission.attempt_id().clone(),
                    EffectCertainty::KnownAbsent,
                ));
            }
        }
        let accepted = if let Some(attempt) = admission.supplied_attempt() {
            self.attempts.accept(&attempt).await?
        } else {
            self.attempts.accept_next(admission).await?
        };
        match accepted {
            OperationAttemptAcceptance::Accepted(attempt) => {
                let admitted = AdmittedProcessMutation {
                    context: ProcessMutationContext::from_attempt(&attempt),
                    reconcile_unknown: false,
                };
                if externally_fenced {
                    let accepted_policy = attempt
                        .dispatch_binding()
                        .and_then(|binding| binding.runtime_policy());
                    if accepted_policy
                        .is_none_or(|policy| policy.validate_at(self.clock.now()).is_err())
                    {
                        self.settle(&admitted.context, EffectCertainty::KnownAbsent, None)
                            .await?;
                        return Err(attempt_rejected(
                            session_id,
                            attempt.id().clone(),
                            EffectCertainty::KnownAbsent,
                        ));
                    }
                }
                Ok(admitted)
            }
            OperationAttemptAcceptance::Duplicate(attempt) if !externally_fenced => {
                Ok(AdmittedProcessMutation {
                    context: ProcessMutationContext::from_attempt(&attempt),
                    reconcile_unknown: false,
                })
            }
            OperationAttemptAcceptance::Duplicate(attempt) => Err(attempt_rejected(
                session_id,
                attempt.id().clone(),
                EffectCertainty::Unknown,
            )),
            OperationAttemptAcceptance::AlreadySettled(attempt)
                if !externally_fenced
                    && matches!(
                        attempt.state(),
                        OperationAttemptState::Settled {
                            certainty: EffectCertainty::Unknown,
                            ..
                        }
                    ) =>
            {
                Ok(AdmittedProcessMutation {
                    reconcile_unknown: true,
                    context: ProcessMutationContext::from_attempt(&attempt),
                })
            }
            OperationAttemptAcceptance::AlreadySettled(attempt) => Err(attempt_rejected(
                session_id,
                attempt.id().clone(),
                EffectCertainty::KnownAbsent,
            )),
            OperationAttemptAcceptance::AlreadyCompensated(attempt) => Err(attempt_rejected(
                session_id,
                attempt.id().clone(),
                EffectCertainty::KnownAbsent,
            )),
            OperationAttemptAcceptance::StaleFence { .. }
            | OperationAttemptAcceptance::PayloadConflict => Err(attempt_rejected(
                session_id,
                admission.attempt_id().clone(),
                EffectCertainty::KnownAbsent,
            )),
        }
    }

    async fn settle(
        &self,
        context: &ProcessMutationContext,
        certainty: EffectCertainty,
        result_digest: Option<PayloadDigest>,
    ) -> Result<OperationAttempt, PortError> {
        let settlement = OperationAttemptSettlement {
            attempt_id: context.attempt_id().clone(),
            fencing_token: context.fencing_token(),
            certainty,
            result_digest,
            settled_at: self.clock.now(),
        };
        match self.attempts.settle(&settlement).await? {
            OperationAttemptSettlementOutcome::Settled(attempt)
            | OperationAttemptSettlementOutcome::Duplicate(attempt) => Ok(attempt),
            OperationAttemptSettlementOutcome::StaleFence { .. }
            | OperationAttemptSettlementOutcome::NotFound
            | OperationAttemptSettlementOutcome::PayloadConflict => Err(PortError::new(
                PortErrorKind::Conflict,
                PortOperation::SettleOperationAttempt,
                context.target_session().cloned(),
                DurableSettlementRejected,
            )
            .with_attempt_effect(context.attempt_id().clone(), certainty)),
        }
    }

    async fn reconcile(
        &self,
        context: &ProcessMutationContext,
        certainty: EffectCertainty,
        result_digest: Option<PayloadDigest>,
    ) -> Result<OperationAttempt, PortError> {
        let reconciliation = OperationAttemptReconciliation {
            attempt_id: context.attempt_id().clone(),
            fencing_token: context.fencing_token(),
            certainty,
            result_digest,
            reconciled_at: self.clock.now(),
        };
        match self.attempts.reconcile(&reconciliation).await? {
            OperationAttemptSettlementOutcome::Settled(attempt)
            | OperationAttemptSettlementOutcome::Duplicate(attempt) => Ok(attempt),
            OperationAttemptSettlementOutcome::StaleFence { .. }
            | OperationAttemptSettlementOutcome::NotFound
            | OperationAttemptSettlementOutcome::PayloadConflict => Err(PortError::new(
                PortErrorKind::Conflict,
                PortOperation::ReconcileOperationAttempt,
                context.target_session().cloned(),
                DurableSettlementRejected,
            )
            .with_attempt_effect(context.attempt_id().clone(), certainty)),
        }
    }

    async fn complete<T, F>(
        &self,
        admitted: &AdmittedProcessMutation,
        effect: Result<T, PortError>,
        applied_digest: F,
    ) -> Result<T, PortError>
    where
        F: FnOnce(&T) -> PayloadDigest,
    {
        let context = &admitted.context;
        match effect {
            Ok(value) => {
                let result_digest = Some(applied_digest(&value));
                let committed = if admitted.reconcile_unknown {
                    self.reconcile(context, EffectCertainty::KnownApplied, result_digest)
                        .await?
                } else {
                    self.settle(context, EffectCertainty::KnownApplied, result_digest)
                        .await?
                };
                self.inner.acknowledge_attempt(&committed).await?;
                Ok(value)
            }
            Err(error) => {
                // PortError carries certainty but no result digest. Even an
                // applied claim cannot satisfy KnownApplied's digest invariant,
                // so retain Unknown and its receipt until the result is recovered.
                // See ../docs/internals/execution/attempts.md#the-durable-process-decorator.
                let certainty = match error.effect_certainty() {
                    Some(EffectCertainty::KnownAbsent) => EffectCertainty::KnownAbsent,
                    Some(EffectCertainty::KnownApplied | EffectCertainty::Unknown) | None => {
                        EffectCertainty::Unknown
                    }
                };
                let settlement = if admitted.reconcile_unknown {
                    if certainty == EffectCertainty::Unknown {
                        None
                    } else {
                        Some(self.reconcile(context, certainty, None).await?)
                    }
                } else {
                    Some(self.settle(context, certainty, None).await?)
                };
                if certainty != EffectCertainty::Unknown
                    && let Some(committed) = settlement
                {
                    self.inner.acknowledge_attempt(&committed).await?;
                }
                Err(error.with_attempt_effect(context.attempt_id().clone(), certainty))
            }
        }
    }
}

impl ProcessRuntime for DurableProcessRuntime {
    fn acknowledge_attempt<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        // The start service settles dispatched spawns outside this decorator.
        // Its post-commit ACK must reach the runtime that owns the receipt.
        self.inner.acknowledge_attempt(attempt)
    }

    fn prepare_launch<'a>(
        &'a self,
        launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<LaunchSpec, PortError>> {
        self.inner.prepare_launch(launch)
    }

    fn spawn<'a>(
        &'a self,
        session_id: &'a SessionId,
        launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<ProcessInstance, PortError>> {
        self.inner.spawn(session_id, launch)
    }

    fn arm_exit<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<(), PortError>> {
        self.inner.arm_exit(session_id, process)
    }

    fn stop<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        policy: StopPolicy,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        self.inner.stop(session_id, process, policy)
    }

    fn force_stop<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        self.inner.force_stop(session_id, process)
    }

    fn write_input<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        input: &'a InputSequence,
        policy: InputDeliveryPolicy,
    ) -> PortFuture<'a, Result<(), PortError>> {
        self.inner.write_input(session_id, process, input, policy)
    }

    fn close_input<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<StandardInputClosure, PortError>> {
        self.inner.close_input(session_id, process)
    }

    fn resize<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        size: TerminalSize,
    ) -> PortFuture<'a, Result<(), PortError>> {
        self.inner.resize(session_id, process, size)
    }

    fn spawn_admitted<'a>(
        &'a self,
        admission: &'a OperationAttemptAdmission,
        session_id: &'a SessionId,
        launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<ProcessInstance, PortError>> {
        Box::pin(async move {
            let admitted = self.admit(admission, session_id).await?;
            let effect = self
                .inner
                .spawn_attempted(&admitted.context, session_id, launch)
                .await;
            if admission.supplied_fencing_token().is_some()
                && let Ok(instance) = effect
            {
                // A dispatched start is not semantically applied until the start service has
                // durably committed ownership, binding, and Running. Leaving the attempt
                // accepted also makes an overlapping retry fail closed without another spawn.
                return Ok(instance);
            }
            self.complete(&admitted, effect, process_instance_result_digest)
                .await
        })
    }

    fn arm_exit_admitted<'a>(
        &'a self,
        admission: &'a OperationAttemptAdmission,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            let admitted = self.admit(admission, session_id).await?;
            let effect = self
                .inner
                .arm_exit_attempted(&admitted.context, session_id, process)
                .await;
            self.complete(&admitted, effect, |_| {
                process_unit_result_digest(&admitted.context)
            })
            .await
        })
    }

    fn stop_admitted<'a>(
        &'a self,
        admission: &'a OperationAttemptAdmission,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        policy: StopPolicy,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        Box::pin(async move {
            let admitted = self.admit(admission, session_id).await?;
            let effect = self
                .inner
                .stop_attempted(&admitted.context, session_id, process, policy)
                .await;
            self.complete(&admitted, effect, process_termination_result_digest)
                .await
        })
    }

    fn force_stop_admitted<'a>(
        &'a self,
        admission: &'a OperationAttemptAdmission,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        Box::pin(async move {
            let admitted = self.admit(admission, session_id).await?;
            let effect = self
                .inner
                .force_stop_attempted(&admitted.context, session_id, process)
                .await;
            self.complete(&admitted, effect, process_termination_result_digest)
                .await
        })
    }

    fn write_input_admitted<'a>(
        &'a self,
        admission: &'a OperationAttemptAdmission,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        input: &'a InputSequence,
        policy: InputDeliveryPolicy,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            let admitted = self.admit(admission, session_id).await?;
            let effect = self
                .inner
                .write_input_attempted(&admitted.context, session_id, process, input, policy)
                .await;
            self.complete(&admitted, effect, |_| {
                process_unit_result_digest(&admitted.context)
            })
            .await
        })
    }

    fn resize_admitted<'a>(
        &'a self,
        admission: &'a OperationAttemptAdmission,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        size: TerminalSize,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            let admitted = self.admit(admission, session_id).await?;
            let effect = self
                .inner
                .resize_attempted(&admitted.context, session_id, process, size)
                .await;
            self.complete(&admitted, effect, |_| {
                process_unit_result_digest(&admitted.context)
            })
            .await
        })
    }
}

/// Computes the canonical receipt digest for a successful unit process effect.
pub fn process_unit_result_digest(context: &ProcessMutationContext) -> PayloadDigest {
    PayloadDigest::of_slices([
        b"undercurrent.process-result.v0.1.0-beta".as_slice(),
        context.kind().as_str().as_bytes(),
        context.attempt_id().as_str().as_bytes(),
        b"unit".as_slice(),
    ])
}

/// Computes the canonical receipt digest for a spawned process instance.
pub fn process_instance_result_digest(instance: &ProcessInstance) -> PayloadDigest {
    let started_at = instance.started_at.value().to_be_bytes();
    PayloadDigest::of_slices([
        b"undercurrent.process-result.v0.1.0-beta.spawn".as_slice(),
        instance.id.as_str().as_bytes(),
        &started_at,
    ])
}

/// Computes the canonical receipt digest for a process-termination response.
///
/// The digest also represents `GracefulTimedOut`; a known response does not
/// necessarily establish that the process exited.
pub fn process_termination_result_digest(termination: &ProcessTermination) -> PayloadDigest {
    let kind = match termination.kind() {
        TerminationKind::Graceful => b"graceful".as_slice(),
        TerminationKind::GracefulTimedOut => b"graceful_timed_out".as_slice(),
        TerminationKind::ForcedAfterTimeout => b"forced_after_timeout".as_slice(),
        TerminationKind::Forced => b"forced".as_slice(),
    };
    let exit_code = termination.exit_code().unwrap_or(i32::MIN).to_be_bytes();
    let observed_at = termination.observed_at().value().to_be_bytes();
    PayloadDigest::of_slices([
        b"undercurrent.process-result.v0.1.0-beta.stop".as_slice(),
        kind,
        &exit_code,
        &observed_at,
    ])
}

fn attempt_rejected(
    session_id: &SessionId,
    attempt_id: undercurrent_domain::OperationAttemptId,
    certainty: EffectCertainty,
) -> PortError {
    PortError::new(
        PortErrorKind::Conflict,
        PortOperation::AcceptOperationAttempt,
        Some(session_id.clone()),
        DurableAdmissionRejected,
    )
    .with_attempt_effect(attempt_id, certainty)
}

#[derive(Debug)]
struct DurableAdmissionRejected;

impl Display for DurableAdmissionRejected {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("durable process mutation admission was rejected")
    }
}

impl Error for DurableAdmissionRejected {}

#[derive(Debug)]
struct DurableSettlementRejected;

impl Display for DurableSettlementRejected {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("durable process mutation settlement was rejected")
    }
}

impl Error for DurableSettlementRejected {}
