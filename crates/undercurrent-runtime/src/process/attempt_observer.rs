// SPDX-License-Identifier: Apache-2.0

//! Composite hosted effect observer used by daemon reconciliation.

use undercurrent_application::{
    ObservedOperationAttemptEffect, ObservedOperationAttemptRepair, OperationAttemptEffectObserver,
    OperationAttemptRepairEvidenceRef, OperationAttemptRepairObserver, PortError, PortErrorKind,
    PortFuture, PortOperation, SessionCommandExecutor,
};
use undercurrent_domain::{
    EffectCertainty, OperationAttempt, OperationAttemptState, PayloadDigest, ProcessMutationKind,
};

use crate::RuntimeAdapterFailure;

use super::{HostedProcessRuntime, HostedSessionCommandExecutor};

/// Routes durable attempt observation to the exact private host evidence owner.
#[derive(Clone)]
pub struct HostedOperationAttemptEffectObserver {
    runtime: HostedProcessRuntime,
    commands: HostedSessionCommandExecutor,
}

impl HostedOperationAttemptEffectObserver {
    /// Constructs the single daemon-injected observer without exposing private
    /// UCHS protocol or journal types.
    pub const fn new(
        runtime: HostedProcessRuntime,
        commands: HostedSessionCommandExecutor,
    ) -> Self {
        Self { runtime, commands }
    }
}

impl OperationAttemptEffectObserver for HostedOperationAttemptEffectObserver {
    fn observe_input_retirement<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<
        'a,
        Result<Option<undercurrent_application::InputAttemptRetirementEvidence>, PortError>,
    > {
        Box::pin(async move { self.runtime.observe_input_retirement(attempt).await })
    }

    fn cancel_uncommitted_shutdown<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<bool, PortError>> {
        Box::pin(async move { self.runtime.cancel_uncommitted_shutdown(attempt).await })
    }

    fn acknowledge_shutdown_cancellation<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            self.runtime
                .acknowledge_shutdown_cancellation(attempt)
                .await
        })
    }

    fn observe<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<ObservedOperationAttemptEffect, PortError>> {
        match attempt.kind() {
            ProcessMutationKind::ExecuteInSession => self.commands.observe(attempt),
            ProcessMutationKind::Spawn
            | ProcessMutationKind::ArmExit
            | ProcessMutationKind::Stop
            | ProcessMutationKind::ForceStop
            | ProcessMutationKind::WriteInput
            | ProcessMutationKind::Resize
            | ProcessMutationKind::Shutdown => {
                Box::pin(async move { self.runtime.observe_process_attempt(attempt).await })
            }
        }
    }

    fn acknowledge_observation<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        match attempt.kind() {
            ProcessMutationKind::ExecuteInSession => self.commands.acknowledge(attempt),
            ProcessMutationKind::Spawn
            | ProcessMutationKind::ArmExit
            | ProcessMutationKind::Stop
            | ProcessMutationKind::ForceStop
            | ProcessMutationKind::WriteInput
            | ProcessMutationKind::Resize
            | ProcessMutationKind::Shutdown => {
                Box::pin(async move { self.runtime.acknowledge_process_attempt(attempt).await })
            }
        }
    }
}

impl OperationAttemptRepairObserver for HostedOperationAttemptEffectObserver {
    fn observe_repair<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<ObservedOperationAttemptRepair, PortError>> {
        Box::pin(async move {
            // Use the same cancellation outcome as automatic recovery. An
            // Unknown shutdown never authorized the destructive acknowledgement.
            if attempt.kind() == ProcessMutationKind::Shutdown
                && matches!(
                    attempt.state(),
                    OperationAttemptState::Settled {
                        certainty: EffectCertainty::Unknown,
                        ..
                    }
                )
            {
                self.runtime.cancel_uncommitted_shutdown(attempt).await?;
            }
            let observation = self.observe(attempt).await?;
            if observation.certainty == EffectCertainty::Unknown {
                return Err(PortError::new(
                    PortErrorKind::Unavailable,
                    PortOperation::RepairOperationAttempt,
                    attempt.target_session().cloned(),
                    RuntimeAdapterFailure("session host has no terminal repair evidence"),
                ));
            }
            let evidence_reference = repair_evidence_reference(attempt, &observation)?;
            ObservedOperationAttemptRepair::new(
                attempt.id().clone(),
                attempt.fencing_token(),
                observation.certainty,
                observation.result_digest,
                evidence_reference,
            )
            .map_err(|source| {
                PortError::new(
                    PortErrorKind::InvalidData,
                    PortOperation::RepairOperationAttempt,
                    attempt.target_session().cloned(),
                    source,
                )
            })
        })
    }

    fn acknowledge_repair<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
        evidence_reference: &'a OperationAttemptRepairEvidenceRef,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            let observation = self.observe(attempt).await?;
            if observation.certainty != EffectCertainty::Unknown {
                let current = repair_evidence_reference(attempt, &observation)?;
                if &current != evidence_reference {
                    return Err(PortError::new(
                        PortErrorKind::Conflict,
                        PortOperation::RepairOperationAttempt,
                        attempt.target_session().cloned(),
                        RuntimeAdapterFailure(
                            "repair evidence reference does not match retained receipt",
                        ),
                    ));
                }
            }
            if attempt.kind() == ProcessMutationKind::Shutdown
                && matches!(
                    attempt.state(),
                    OperationAttemptState::Settled {
                        certainty: EffectCertainty::Unknown | EffectCertainty::KnownAbsent,
                        ..
                    }
                )
            {
                return self
                    .runtime
                    .acknowledge_shutdown_cancellation(attempt)
                    .await;
            }
            self.acknowledge_observation(attempt).await
        })
    }
}

fn repair_evidence_reference(
    attempt: &OperationAttempt,
    observation: &ObservedOperationAttemptEffect,
) -> Result<OperationAttemptRepairEvidenceRef, PortError> {
    let mut evidence = Vec::with_capacity(256);
    evidence.extend_from_slice(b"undercurrent-attempt-receipt-v0.1.0-beta\0");
    evidence.extend_from_slice(attempt.id().as_str().as_bytes());
    evidence.extend_from_slice(&attempt.fencing_token().value().to_be_bytes());
    evidence.extend_from_slice(attempt.kind().as_str().as_bytes());
    evidence.extend_from_slice(attempt.payload_digest().as_bytes());
    evidence.push(match observation.certainty {
        EffectCertainty::KnownAbsent => 1,
        EffectCertainty::KnownApplied => 2,
        EffectCertainty::Unknown => 3,
    });
    if let Some(digest) = observation.result_digest {
        evidence.extend_from_slice(digest.as_bytes());
    }
    let reference = format!(
        "undercurrent-receipt:v0.1.0-beta:{}",
        PayloadDigest::of_bytes(&evidence).to_hex()
    );
    OperationAttemptRepairEvidenceRef::new(reference).map_err(|source| {
        PortError::new(
            PortErrorKind::InvalidData,
            PortOperation::RepairOperationAttempt,
            attempt.target_session().cloned(),
            source,
        )
    })
}
