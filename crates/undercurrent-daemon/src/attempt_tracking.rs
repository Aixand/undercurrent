// SPDX-License-Identifier: Apache-2.0

//! Exact live-attempt tracking around the durable repository boundary.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex};

use undercurrent_application::{
    InvalidOperationAttemptScan, ObservedOperationAttemptCompensation,
    ObservedOperationAttemptEffect, OperationAttemptAcceptance, OperationAttemptAdmission,
    OperationAttemptCompensation, OperationAttemptCompensationOutcome,
    OperationAttemptCompensationPreparation, OperationAttemptCompensationPreparationOutcome,
    OperationAttemptEffectObserver, OperationAttemptReconciliation, OperationAttemptRepairCommit,
    OperationAttemptRepairOutcome, OperationAttemptRepairRepository, OperationAttemptScan,
    OperationAttemptScanExclusion, OperationAttemptSettlement, OperationAttemptSettlementOutcome,
    OperationAttemptStore, PortError, PortErrorKind, PortFuture, PortOperation, ProcessTruthSource,
    SessionProcessDirectory, SessionRepository, UnresolvedOperationAttempts,
};
use undercurrent_domain::{
    EffectCertainty, FencingToken, LifecycleState, OperationAttempt, OperationAttemptId,
    OperationAttemptState, ProcessMutationKind, ProcessTruth, Revision,
};

use crate::OperationAttemptReadinessGate;

#[derive(Clone, PartialEq, Eq)]
struct LiveAttempt {
    attempt_id: OperationAttemptId,
    fencing_token: FencingToken,
    leases: usize,
    target: Option<undercurrent_domain::SessionId>,
}

/// Process-epoch proof that an exact accepted attempt is actively executing.
#[derive(Default)]
pub(crate) struct LiveOperationAttemptRegistry {
    entries: Mutex<Vec<LiveAttempt>>,
}

impl LiveOperationAttemptRegistry {
    fn register(&self, attempt: &OperationAttempt) {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(entry) = entries.iter_mut().find(|entry| {
            &entry.attempt_id == attempt.id() && entry.fencing_token == attempt.fencing_token()
        }) {
            entry.leases = entry.leases.saturating_add(1);
            return;
        }
        entries.push(LiveAttempt {
            attempt_id: attempt.id().clone(),
            fencing_token: attempt.fencing_token(),
            leases: 1,
            target: attempt.target_session().cloned(),
        });
    }

    fn unregister(&self, attempt_id: &OperationAttemptId, fencing_token: FencingToken) {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(index) = entries.iter().position(|entry| {
            &entry.attempt_id == attempt_id && entry.fencing_token == fencing_token
        }) else {
            return;
        };
        if entries[index].leases > 1 {
            entries[index].leases -= 1;
        } else {
            entries.swap_remove(index);
        }
    }

    #[cfg(test)]
    fn contains(&self, attempt: &OperationAttempt) -> bool {
        self.contains_exact(attempt.id(), attempt.fencing_token())
    }

    #[cfg(test)]
    fn contains_exact(&self, attempt_id: &OperationAttemptId, fencing_token: FencingToken) -> bool {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .any(|entry| &entry.attempt_id == attempt_id && entry.fencing_token == fencing_token)
    }

    fn scoped_exclusions(
        &self,
        scope: &undercurrent_application::OperationAttemptScope,
    ) -> Vec<OperationAttemptScanExclusion> {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|entry| scope.includes_target(entry.target.as_ref()))
            .map(|entry| {
                OperationAttemptScanExclusion::new(entry.attempt_id.clone(), entry.fencing_token)
            })
            .collect()
    }

    #[cfg(test)]
    fn unique_count(&self) -> usize {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }
}

/// Store adapter that atomically pairs durable acceptance with live epoch proof.
pub(crate) struct TrackedOperationAttemptRepository {
    attempts: Arc<dyn OperationAttemptStore>,
    repairs: Arc<dyn OperationAttemptRepairRepository>,
    registry: Arc<LiveOperationAttemptRegistry>,
    // Scans must not see acceptance before live proof is registered, or
    // settlement before that proof is removed. Hold this across each durable
    // call and its registry update so in-flight work cannot appear abandoned.
    // See [live attempt proof](../docs/internals/requests/effects-and-recovery.md#why-an-active-attempt-is-not-automatically-recovery-debt).
    coordination: tokio::sync::Mutex<()>,
    readiness: Arc<OperationAttemptReadinessGate>,
}

impl TrackedOperationAttemptRepository {
    pub(crate) fn new(
        attempts: Arc<dyn OperationAttemptStore>,
        repairs: Arc<dyn OperationAttemptRepairRepository>,
        readiness: Arc<OperationAttemptReadinessGate>,
    ) -> Self {
        Self {
            attempts,
            repairs,
            registry: Arc::new(LiveOperationAttemptRegistry::default()),
            coordination: tokio::sync::Mutex::new(()),
            readiness,
        }
    }

    fn register_acceptance(&self, acceptance: &OperationAttemptAcceptance) {
        match acceptance {
            OperationAttemptAcceptance::Accepted(attempt)
            | OperationAttemptAcceptance::Duplicate(attempt)
                if matches!(attempt.state(), OperationAttemptState::Accepted) =>
            {
                self.registry.register(attempt);
            }
            OperationAttemptAcceptance::Accepted(_)
            | OperationAttemptAcceptance::Duplicate(_)
            | OperationAttemptAcceptance::AlreadySettled(_)
            | OperationAttemptAcceptance::AlreadyCompensated(_)
            | OperationAttemptAcceptance::StaleFence { .. }
            | OperationAttemptAcceptance::PayloadConflict => {}
        }
    }

    fn settlement_finished(
        &self,
        attempt_id: &OperationAttemptId,
        fencing_token: FencingToken,
        certainty: EffectCertainty,
        succeeded: bool,
    ) {
        self.registry.unregister(attempt_id, fencing_token);
        if !succeeded {
            self.readiness.close();
        } else if certainty == EffectCertainty::Unknown {
            self.readiness.invalidate();
        }
    }

    async fn check_admission(
        &self,
        id: &OperationAttemptId,
        target: Option<&undercurrent_domain::SessionId>,
        kind: ProcessMutationKind,
    ) -> Result<(), PortError> {
        use undercurrent_application::OperationAttemptScope;
        let scope = target
            .cloned()
            .map_or(OperationAttemptScope::Host, OperationAttemptScope::Session);
        let exclusions = self.registry.scoped_exclusions(&scope);
        let mut scan = OperationAttemptScan::new(exclusions.len().max(1), exclusions)
            .map_err(invalid_tracking_scan)?
            .with_scope(scope);
        if matches!(
            kind,
            ProcessMutationKind::Stop | ProcessMutationKind::ForceStop
        ) {
            scan = scan.without_input_attempts();
        }
        // Recheck under the same coordination lock as acceptance and settlement.
        // A product-level read alone can race an earlier write settling Unknown.
        for _ in 0..2 {
            let pending = self.attempts.pending_reconciliation(&scan).await?;
            if pending.len() > scan.limit() || pending.iter().any(|attempt| !scan.includes(attempt))
            {
                self.readiness.close();
                return Err(tracking_invariant());
            }
            if pending.iter().any(|attempt| attempt.id() != id) {
                return Err(PortError::new(
                    PortErrorKind::Conflict,
                    PortOperation::AcceptOperationAttempt,
                    target.cloned(),
                    RecoveryAdmissionBlocked,
                ));
            }
            let Some(own) = pending.last() else {
                return Ok(());
            };
            // An exact replay may occupy a one-row page. Inspect the next page
            // without raising the store's configured read bound or hiding other debt.
            scan = scan.after(Some(undercurrent_application::OperationAttemptScanCursor {
                accepted_at: own.accepted_at(),
                attempt_id: own.id().clone(),
            }));
        }
        Ok(())
    }

    fn merged_scan(&self, scan: &OperationAttemptScan) -> Result<OperationAttemptScan, PortError> {
        // Exclude live attempts in the repository query, before its page limit.
        // Filtering the returned page could let live work hide unresolved debt
        // and make an empty filtered page look like a complete recovery scan.
        let mut exclusions = scan.exclusions().to_vec();
        for live in self.registry.scoped_exclusions(scan.scope()) {
            if !exclusions.contains(&live) {
                exclusions.push(live);
            }
        }
        OperationAttemptScan::new(scan.limit(), exclusions)
            .map(|merged| {
                let merged = merged
                    .with_scope(scan.scope().clone())
                    .after(scan.cursor().cloned());
                if scan.skips_input() {
                    merged.without_input_attempts()
                } else {
                    merged
                }
            })
            .map_err(|source| {
                self.readiness.close();
                invalid_tracking_scan(source)
            })
    }
}

impl OperationAttemptStore for TrackedOperationAttemptRepository {
    fn retire_input<'a>(
        &'a self,
        retirement: &'a undercurrent_application::InputAttemptRetirement,
    ) -> PortFuture<'a, Result<bool, PortError>> {
        Box::pin(async move {
            let _guard = self.coordination.lock().await;
            self.attempts.retire_input(retirement).await
        })
    }
    fn pending_input_retirement_acks(
        &self,
        limit: usize,
    ) -> PortFuture<'_, Result<Vec<OperationAttempt>, PortError>> {
        self.attempts.pending_input_retirement_acks(limit)
    }
    fn acknowledge_input_retirement<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        self.attempts.acknowledge_input_retirement(attempt)
    }

    fn accept_next<'a>(
        &'a self,
        admission: &'a OperationAttemptAdmission,
    ) -> PortFuture<'a, Result<OperationAttemptAcceptance, PortError>> {
        Box::pin(async move {
            let _guard = self.coordination.lock().await;
            self.check_admission(
                admission.attempt_id(),
                admission.target_session(),
                admission.kind(),
            )
            .await?;
            let acceptance = self.attempts.accept_next(admission).await?;
            self.register_acceptance(&acceptance);
            Ok(acceptance)
        })
    }

    fn accept<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<OperationAttemptAcceptance, PortError>> {
        Box::pin(async move {
            let _guard = self.coordination.lock().await;
            self.check_admission(attempt.id(), attempt.target_session(), attempt.kind())
                .await?;
            let acceptance = self.attempts.accept(attempt).await?;
            self.register_acceptance(&acceptance);
            Ok(acceptance)
        })
    }

    fn settle<'a>(
        &'a self,
        settlement: &'a OperationAttemptSettlement,
    ) -> PortFuture<'a, Result<OperationAttemptSettlementOutcome, PortError>> {
        Box::pin(async move {
            let _guard = self.coordination.lock().await;
            let outcome = self.attempts.settle(settlement).await;
            self.settlement_finished(
                &settlement.attempt_id,
                settlement.fencing_token,
                settlement.certainty,
                outcome.is_ok(),
            );
            outcome
        })
    }

    fn prepare_compensation<'a>(
        &'a self,
        preparation: &'a OperationAttemptCompensationPreparation,
    ) -> PortFuture<'a, Result<OperationAttemptCompensationPreparationOutcome, PortError>> {
        Box::pin(async move {
            let _guard = self.coordination.lock().await;
            self.attempts.prepare_compensation(preparation).await
        })
    }

    fn compensate<'a>(
        &'a self,
        compensation: &'a OperationAttemptCompensation,
    ) -> PortFuture<'a, Result<OperationAttemptCompensationOutcome, PortError>> {
        Box::pin(async move {
            let _guard = self.coordination.lock().await;
            self.attempts.compensate(compensation).await
        })
    }

    fn reconcile<'a>(
        &'a self,
        reconciliation: &'a OperationAttemptReconciliation,
    ) -> PortFuture<'a, Result<OperationAttemptSettlementOutcome, PortError>> {
        Box::pin(async move {
            let _guard = self.coordination.lock().await;
            let outcome = self.attempts.reconcile(reconciliation).await;
            self.settlement_finished(
                &reconciliation.attempt_id,
                reconciliation.fencing_token,
                reconciliation.certainty,
                outcome.is_ok(),
            );
            outcome
        })
    }

    fn load<'a>(
        &'a self,
        attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<Option<OperationAttempt>, PortError>> {
        self.attempts.load(attempt_id)
    }

    fn pending_reconciliation<'a>(
        &'a self,
        scan: &'a OperationAttemptScan,
    ) -> PortFuture<'a, Result<Vec<OperationAttempt>, PortError>> {
        Box::pin(async move {
            let _guard = self.coordination.lock().await;
            let merged = self.merged_scan(scan)?;
            let attempts = self.attempts.pending_reconciliation(&merged).await?;
            if attempts.len() > scan.limit()
                || attempts.iter().any(|attempt| {
                    !merged.includes(attempt)
                        || merged.exclusions().iter().any(|exclusion| {
                            exclusion.attempt_id() == attempt.id()
                                && exclusion.fencing_token() == attempt.fencing_token()
                        })
                })
            {
                self.readiness.close();
                return Err(tracking_invariant());
            }
            Ok(attempts)
        })
    }
}

/// Startup observer that terminalizes prepared compensation only from exact durable absence.
pub(crate) struct CompensationRecoveryAttemptObserver {
    inner: Arc<dyn OperationAttemptEffectObserver>,
    repository: Arc<dyn SessionRepository>,
    processes: Arc<dyn SessionProcessDirectory>,
    process_truth: Arc<dyn ProcessTruthSource>,
}

impl CompensationRecoveryAttemptObserver {
    pub(crate) fn new(
        inner: Arc<dyn OperationAttemptEffectObserver>,
        repository: Arc<dyn SessionRepository>,
        processes: Arc<dyn SessionProcessDirectory>,
        process_truth: Arc<dyn ProcessTruthSource>,
    ) -> Self {
        Self {
            inner,
            repository,
            processes,
            process_truth,
        }
    }
}

impl OperationAttemptEffectObserver for CompensationRecoveryAttemptObserver {
    fn observe_input_retirement<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<
        'a,
        Result<Option<undercurrent_application::InputAttemptRetirementEvidence>, PortError>,
    > {
        self.inner.observe_input_retirement(attempt)
    }

    fn cancel_uncommitted_shutdown<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<bool, PortError>> {
        self.inner.cancel_uncommitted_shutdown(attempt)
    }

    fn acknowledge_shutdown_cancellation<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        self.inner.acknowledge_shutdown_cancellation(attempt)
    }

    fn observe<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<ObservedOperationAttemptEffect, PortError>> {
        self.inner.observe(attempt)
    }

    fn acknowledge_observation<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        self.inner.acknowledge_observation(attempt)
    }

    fn observe_compensation<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<Option<ObservedOperationAttemptCompensation>, PortError>> {
        Box::pin(async move {
            if attempt.kind() != ProcessMutationKind::Spawn
                || attempt.dispatch_binding().is_none()
                || !matches!(attempt.state(), OperationAttemptState::Accepted)
            {
                return Ok(None);
            }
            let (Some(session_id), Some(intent)) =
                (attempt.target_session(), attempt.compensation_intent())
            else {
                return Ok(None);
            };
            let Some(expected_revision) = intent.session_revision().value().checked_add(1) else {
                return Ok(None);
            };
            let Some(session) = self.repository.load(session_id).await? else {
                return Ok(None);
            };
            if session.revision() != Revision::new(expected_revision)
                || session.lifecycle() != LifecycleState::Failed
                || session.process().is_some()
                || self.processes.lookup(session_id)?.is_some()
            {
                return Ok(None);
            }
            if !matches!(
                self.process_truth
                    .observe(session_id, intent.process())
                    .await?,
                ProcessTruth::Missing { .. } | ProcessTruth::Exited { .. }
            ) {
                return Ok(None);
            }
            Ok(Some(ObservedOperationAttemptCompensation::new(
                attempt.id().clone(),
                attempt.fencing_token(),
                intent.superseding_fencing_token(),
                intent.cleanup_digest(),
            )))
        })
    }

    fn acknowledge_compensation<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        self.inner.acknowledge_observation(attempt)
    }
}

impl OperationAttemptRepairRepository for TrackedOperationAttemptRepository {
    fn load_repair_candidate<'a>(
        &'a self,
        attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<Option<OperationAttempt>, PortError>> {
        self.repairs.load_repair_candidate(attempt_id)
    }

    fn repair_unknown<'a>(
        &'a self,
        commit: &'a OperationAttemptRepairCommit,
    ) -> PortFuture<'a, Result<OperationAttemptRepairOutcome, PortError>> {
        self.repairs.repair_unknown(commit)
    }

    fn unresolved_attempts<'a>(
        &'a self,
        scan: &'a OperationAttemptScan,
    ) -> PortFuture<'a, Result<UnresolvedOperationAttempts, PortError>> {
        Box::pin(async move {
            let _guard = self.coordination.lock().await;
            let merged = self.merged_scan(scan)?;
            let unresolved = self.repairs.unresolved_attempts(&merged).await?;
            if unresolved.attempt_ids().len() > scan.limit() {
                self.readiness.close();
                return Err(tracking_invariant());
            }
            Ok(unresolved)
        })
    }
}

#[derive(Debug)]
struct AttemptTrackingInvariant;

impl Display for AttemptTrackingInvariant {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("live operation-attempt tracking invariant failed")
    }
}

impl Error for AttemptTrackingInvariant {}

fn tracking_invariant() -> PortError {
    PortError::new(
        PortErrorKind::InvalidData,
        PortOperation::InspectOperationAttemptReadiness,
        None,
        AttemptTrackingInvariant,
    )
}

fn invalid_tracking_scan(source: InvalidOperationAttemptScan) -> PortError {
    PortError::new(
        PortErrorKind::InvalidData,
        PortOperation::InspectOperationAttemptReadiness,
        None,
        source,
    )
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Waker};

    use tokio::sync::Barrier;
    use undercurrent_application::{
        OperationAttemptRepairCommit, OperationAttemptRepairOutcome,
        OperationAttemptRepairRepository, OperationAttemptSettlement,
        OperationAttemptSettlementOutcome, OperationAttemptStore, UnresolvedOperationAttempts,
    };
    use undercurrent_domain::{
        EffectCertainty, FencingToken, OperationAttempt, OperationAttemptId, OperationAttemptState,
        PayloadDigest, ProcessMutationKind, TimestampMillis,
    };

    use super::*;

    struct CommitWindow {
        committed: Barrier,
        release: Barrier,
    }

    impl CommitWindow {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                committed: Barrier::new(2),
                release: Barrier::new(2),
            })
        }

        async fn pause_after_commit(&self) {
            self.committed.wait().await;
            self.release.wait().await;
        }

        async fn wait_for_commit(&self) {
            self.committed.wait().await;
        }

        async fn release_return(&self) {
            self.release.wait().await;
        }
    }

    struct ControllableAttemptRepository {
        durable: Mutex<Vec<OperationAttempt>>,
        acceptance_window: Option<Arc<CommitWindow>>,
        settlement_window: Option<Arc<CommitWindow>>,
        pending_reads: AtomicUsize,
        readiness_reads: AtomicUsize,
        hard_read_limit: AtomicUsize,
    }

    impl ControllableAttemptRepository {
        fn new(
            acceptance_window: Option<Arc<CommitWindow>>,
            settlement_window: Option<Arc<CommitWindow>>,
        ) -> Arc<Self> {
            Arc::new(Self {
                durable: Mutex::new(Vec::new()),
                acceptance_window,
                settlement_window,
                pending_reads: AtomicUsize::new(0),
                readiness_reads: AtomicUsize::new(0),
                hard_read_limit: AtomicUsize::new(usize::MAX),
            })
        }

        fn set_hard_read_limit(&self, limit: usize) {
            self.hard_read_limit.store(limit, Ordering::SeqCst);
        }

        fn enforce_read_limit(&self, requested: usize) -> Result<(), PortError> {
            let maximum = self.hard_read_limit.load(Ordering::SeqCst);
            if requested > maximum {
                return Err(backing_read_limit_exceeded(requested, maximum));
            }
            Ok(())
        }

        fn durable_attempt(&self, attempt_id: &OperationAttemptId) -> OperationAttempt {
            self.durable
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .iter()
                .find(|attempt| attempt.id() == attempt_id)
                .cloned()
                .expect("durable attempt")
        }

        fn seed_attempt(&self, attempt: OperationAttempt) {
            self.durable
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(attempt);
        }

        async fn commit_acceptance(
            &self,
            accepted: OperationAttempt,
        ) -> Result<OperationAttemptAcceptance, PortError> {
            {
                let mut durable = self
                    .durable
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if let Some(current) = durable
                    .iter_mut()
                    .find(|attempt| attempt.id() == accepted.id())
                {
                    *current = accepted.clone();
                } else {
                    durable.push(accepted.clone());
                }
            }
            if let Some(window) = &self.acceptance_window {
                window.pause_after_commit().await;
            }
            Ok(OperationAttemptAcceptance::Accepted(accepted))
        }
    }

    impl OperationAttemptStore for ControllableAttemptRepository {
        fn accept_next<'a>(
            &'a self,
            admission: &'a OperationAttemptAdmission,
        ) -> PortFuture<'a, Result<OperationAttemptAcceptance, PortError>> {
            Box::pin(async move {
                let accepted = OperationAttempt::accepted(
                    admission.attempt_id().clone(),
                    admission.target_session().cloned(),
                    admission.kind(),
                    FencingToken::new(1).expect("fence"),
                    admission.payload_digest(),
                    admission.accepted_at(),
                );
                self.commit_acceptance(accepted).await
            })
        }

        fn accept<'a>(
            &'a self,
            attempt: &'a OperationAttempt,
        ) -> PortFuture<'a, Result<OperationAttemptAcceptance, PortError>> {
            Box::pin(async move { self.commit_acceptance(attempt.clone()).await })
        }

        fn settle<'a>(
            &'a self,
            settlement: &'a OperationAttemptSettlement,
        ) -> PortFuture<'a, Result<OperationAttemptSettlementOutcome, PortError>> {
            Box::pin(async move {
                let settled = {
                    let mut durable = self
                        .durable
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    let attempt = durable
                        .iter_mut()
                        .find(|attempt| attempt.id() == &settlement.attempt_id)
                        .expect("accepted attempt");
                    attempt
                        .settle(
                            settlement.fencing_token,
                            settlement.certainty,
                            settlement.result_digest,
                            settlement.settled_at,
                        )
                        .map_err(|_| tracking_invariant())?;
                    attempt.clone()
                };
                if let Some(window) = &self.settlement_window {
                    window.pause_after_commit().await;
                }
                Ok(OperationAttemptSettlementOutcome::Settled(settled))
            })
        }

        fn reconcile<'a>(
            &'a self,
            _reconciliation: &'a OperationAttemptReconciliation,
        ) -> PortFuture<'a, Result<OperationAttemptSettlementOutcome, PortError>> {
            Box::pin(async { Err(tracking_invariant()) })
        }

        fn load<'a>(
            &'a self,
            attempt_id: &'a OperationAttemptId,
        ) -> PortFuture<'a, Result<Option<OperationAttempt>, PortError>> {
            Box::pin(async move {
                Ok(self
                    .durable
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .iter()
                    .find(|attempt| attempt.id() == attempt_id)
                    .cloned())
            })
        }

        fn pending_reconciliation<'a>(
            &'a self,
            scan: &'a OperationAttemptScan,
        ) -> PortFuture<'a, Result<Vec<OperationAttempt>, PortError>> {
            Box::pin(async move {
                self.enforce_read_limit(scan.limit())?;
                self.pending_reads.fetch_add(1, Ordering::SeqCst);
                let mut pending = self
                    .durable
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .iter()
                    .filter(|attempt| attempt.state().requires_reconciliation())
                    .filter(|attempt| scan.includes(attempt))
                    .filter(|attempt| !scan_excludes(scan, attempt))
                    .cloned()
                    .collect::<Vec<_>>();
                pending.sort_by(|left, right| left.id().cmp(right.id()));
                pending.truncate(scan.limit());
                Ok(pending)
            })
        }
    }

    impl OperationAttemptRepairRepository for ControllableAttemptRepository {
        fn load_repair_candidate<'a>(
            &'a self,
            attempt_id: &'a OperationAttemptId,
        ) -> PortFuture<'a, Result<Option<OperationAttempt>, PortError>> {
            self.load(attempt_id)
        }

        fn repair_unknown<'a>(
            &'a self,
            _commit: &'a OperationAttemptRepairCommit,
        ) -> PortFuture<'a, Result<OperationAttemptRepairOutcome, PortError>> {
            Box::pin(async { Err(tracking_invariant()) })
        }

        fn unresolved_attempts<'a>(
            &'a self,
            scan: &'a OperationAttemptScan,
        ) -> PortFuture<'a, Result<UnresolvedOperationAttempts, PortError>> {
            Box::pin(async move {
                self.enforce_read_limit(scan.limit())?;
                self.readiness_reads.fetch_add(1, Ordering::SeqCst);
                let mut attempt_ids = self
                    .durable
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .iter()
                    .filter(|attempt| attempt.state().requires_reconciliation())
                    .filter(|attempt| scan.includes(attempt))
                    .filter(|attempt| !scan_excludes(scan, attempt))
                    .map(|attempt| attempt.id().clone())
                    .collect::<Vec<_>>();
                attempt_ids.sort();
                let total = attempt_ids.len();
                attempt_ids.truncate(scan.limit());
                UnresolvedOperationAttempts::new(total, attempt_ids)
                    .map_err(|_| tracking_invariant())
            })
        }
    }

    fn scan_excludes(scan: &OperationAttemptScan, attempt: &OperationAttempt) -> bool {
        scan.exclusions().iter().any(|exclusion| {
            exclusion.attempt_id() == attempt.id()
                && exclusion.fencing_token() == attempt.fencing_token()
        })
    }

    #[derive(Debug)]
    struct BackingReadLimitExceeded {
        requested: usize,
        maximum: usize,
    }

    impl Display for BackingReadLimitExceeded {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            write!(
                formatter,
                "backing attempt read limit exceeded: requested {}, maximum {}",
                self.requested, self.maximum
            )
        }
    }

    impl Error for BackingReadLimitExceeded {}

    fn backing_read_limit_exceeded(requested: usize, maximum: usize) -> PortError {
        PortError::new(
            PortErrorKind::InvalidData,
            PortOperation::InspectOperationAttemptReadiness,
            None,
            BackingReadLimitExceeded { requested, maximum },
        )
    }

    fn attempt(id: &str, fence: u64) -> OperationAttempt {
        OperationAttempt::accepted(
            OperationAttemptId::new(id).expect("attempt ID"),
            None,
            ProcessMutationKind::Spawn,
            FencingToken::new(fence).expect("fence"),
            PayloadDigest::of_bytes(b"payload"),
            TimestampMillis::new(1),
        )
    }

    fn unknown_attempt(id: &str, fence: u64) -> OperationAttempt {
        let mut attempt = attempt(id, fence);
        attempt
            .settle(
                attempt.fencing_token(),
                EffectCertainty::Unknown,
                None,
                TimestampMillis::new(2),
            )
            .expect("unknown settlement");
        attempt
    }

    #[test]
    fn live_proof_requires_exact_attempt_and_fence_and_is_reference_counted() {
        let registry = LiveOperationAttemptRegistry::default();
        let first = attempt("attempt-1", 1);
        let stale = attempt("attempt-1", 2);

        registry.register(&first);
        registry.register(&first);
        assert!(registry.contains(&first));
        assert!(!registry.contains(&stale));
        assert_eq!(registry.unique_count(), 1);

        registry.unregister(first.id(), first.fencing_token());
        assert!(registry.contains(&first));
        registry.unregister(first.id(), first.fencing_token());
        assert!(!registry.contains(&first));
    }

    #[test]
    fn live_epoch_proof_preserves_readiness_but_never_survives_restart() {
        let gate = OperationAttemptReadinessGate::new();
        gate.apply_unresolved_count(0);
        let accepted = attempt("attempt-live", 7);
        let live = LiveOperationAttemptRegistry::default();
        live.register(&accepted);

        assert!(gate.is_ready(), "a proven live effect is not recovery debt");
        assert!(live.contains(&accepted));

        let restarted_epoch = LiveOperationAttemptRegistry::default();
        assert!(
            !restarted_epoch.contains(&accepted),
            "a preexisting Accepted attempt has no proof in a new daemon epoch"
        );
        gate.apply_unresolved_count(1);
        assert!(!gate.is_ready());
    }

    #[tokio::test]
    async fn exact_retry_cannot_hide_other_debt_behind_a_one_row_page() {
        let store = ControllableAttemptRepository::new(None, None);
        store.set_hard_read_limit(1);
        let own = unknown_attempt("a-retry", 1);
        store.seed_attempt(own.clone());
        store.seed_attempt(unknown_attempt("b-other-debt", 2));
        let repository = TrackedOperationAttemptRepository::new(
            store.clone(),
            store.clone(),
            Arc::new(OperationAttemptReadinessGate::new()),
        );
        assert!(repository.accept(&own).await.is_err());
        assert_eq!(store.pending_reads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn unknown_input_blocks_only_its_session_and_allows_termination() {
        let store = ControllableAttemptRepository::new(None, None);
        let gate = Arc::new(OperationAttemptReadinessGate::new());
        gate.apply_unresolved_count(0);
        let repository =
            TrackedOperationAttemptRepository::new(store.clone(), store.clone(), gate.clone());
        let make = |id: &str, target: &str, kind| {
            OperationAttempt::accepted(
                OperationAttemptId::new(id).unwrap(),
                Some(undercurrent_domain::SessionId::new(target).unwrap()),
                kind,
                FencingToken::new(1).unwrap(),
                PayloadDigest::of_bytes(id.as_bytes()),
                TimestampMillis::new(1),
            )
        };
        let input = make(
            "uncertain-input",
            "affected",
            ProcessMutationKind::WriteInput,
        );
        repository.accept(&input).await.unwrap();
        repository
            .settle(&OperationAttemptSettlement {
                attempt_id: input.id().clone(),
                fencing_token: input.fencing_token(),
                certainty: EffectCertainty::Unknown,
                result_digest: None,
                settled_at: TimestampMillis::new(2),
            })
            .await
            .unwrap();
        assert!(gate.scopes_available());
        assert!(!gate.is_ready());
        assert!(
            repository
                .accept(&make(
                    "blocked",
                    "affected",
                    ProcessMutationKind::WriteInput
                ))
                .await
                .is_err()
        );
        assert!(
            repository
                .accept(&make(
                    "healthy",
                    "unaffected",
                    ProcessMutationKind::WriteInput
                ))
                .await
                .is_ok()
        );
        assert!(
            repository
                .accept(&make("terminate", "affected", ProcessMutationKind::Stop))
                .await
                .is_ok()
        );
        let global = unknown_attempt("host-uncertain", 99);
        store.seed_attempt(global);
        assert!(
            repository
                .accept(&make(
                    "host-blocked",
                    "unaffected",
                    ProcessMutationKind::Resize
                ))
                .await
                .is_err()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn accept_commit_window_blocks_scans_until_live_registration() {
        let acceptance_window = CommitWindow::new();
        let store = ControllableAttemptRepository::new(Some(acceptance_window.clone()), None);
        let gate = Arc::new(OperationAttemptReadinessGate::new());
        let repository = Arc::new(TrackedOperationAttemptRepository::new(
            store.clone(),
            store.clone(),
            gate,
        ));
        let accepted = attempt("attempt-accept-window", 11);

        let accept_task = tokio::spawn({
            let repository = repository.clone();
            let accepted = accepted.clone();
            async move { repository.accept(&accepted).await }
        });
        acceptance_window.wait_for_commit().await;

        assert!(matches!(
            store.durable_attempt(accepted.id()).state(),
            OperationAttemptState::Accepted
        ));
        assert!(!repository.registry.contains(&accepted));

        let scan = OperationAttemptScan::without_exclusions(8).expect("bounded scan");
        let mut pending_scan = Box::pin(repository.pending_reconciliation(&scan));
        let mut readiness_scan = Box::pin(repository.unresolved_attempts(&scan));
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(matches!(
            Future::poll(pending_scan.as_mut(), &mut context),
            Poll::Pending
        ));
        assert!(matches!(
            Future::poll(readiness_scan.as_mut(), &mut context),
            Poll::Pending
        ));
        assert_eq!(
            store.pending_reads.load(Ordering::SeqCst),
            1,
            "only the pre-accept admission scan may have reached storage"
        );
        assert_eq!(store.readiness_reads.load(Ordering::SeqCst), 0);

        acceptance_window.release_return().await;
        let acceptance = accept_task.await.expect("accept task").expect("acceptance");
        assert!(matches!(
            acceptance,
            OperationAttemptAcceptance::Accepted(_)
        ));
        assert!(repository.registry.contains(&accepted));

        let pending = pending_scan.await.expect("pending scan");
        let unresolved = readiness_scan.await.expect("readiness scan");
        assert!(pending.is_empty(), "live Accepted must not be pending debt");
        assert_eq!(unresolved.total(), 0);
        assert!(unresolved.attempt_ids().is_empty());
        assert_eq!(store.pending_reads.load(Ordering::SeqCst), 2);
        assert_eq!(store.readiness_reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn settlement_commit_window_blocks_scans_until_live_proof_removal() {
        let settlement_window = CommitWindow::new();
        let store = ControllableAttemptRepository::new(None, Some(settlement_window.clone()));
        let gate = Arc::new(OperationAttemptReadinessGate::new());
        let repository = Arc::new(TrackedOperationAttemptRepository::new(
            store.clone(),
            store.clone(),
            gate,
        ));
        let accepted = attempt("attempt-settlement-window", 13);
        repository.accept(&accepted).await.expect("acceptance");
        assert!(repository.registry.contains(&accepted));

        let settlement = OperationAttemptSettlement {
            attempt_id: accepted.id().clone(),
            fencing_token: accepted.fencing_token(),
            certainty: EffectCertainty::KnownApplied,
            result_digest: Some(PayloadDigest::of_bytes(b"settled-result")),
            settled_at: TimestampMillis::new(2),
        };
        let settlement_task = tokio::spawn({
            let repository = repository.clone();
            async move { repository.settle(&settlement).await }
        });
        settlement_window.wait_for_commit().await;

        assert!(matches!(
            store.durable_attempt(accepted.id()).state(),
            OperationAttemptState::Settled {
                certainty: EffectCertainty::KnownApplied,
                ..
            }
        ));
        assert!(repository.registry.contains(&accepted));

        let scan = OperationAttemptScan::without_exclusions(8).expect("bounded scan");
        let mut pending_scan = Box::pin(repository.pending_reconciliation(&scan));
        let mut readiness_scan = Box::pin(repository.unresolved_attempts(&scan));
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(matches!(
            Future::poll(pending_scan.as_mut(), &mut context),
            Poll::Pending
        ));
        assert!(matches!(
            Future::poll(readiness_scan.as_mut(), &mut context),
            Poll::Pending
        ));
        assert_eq!(
            store.pending_reads.load(Ordering::SeqCst),
            1,
            "only the pre-accept admission scan may have reached storage"
        );
        assert_eq!(store.readiness_reads.load(Ordering::SeqCst), 0);

        settlement_window.release_return().await;
        let outcome = settlement_task
            .await
            .expect("settlement task")
            .expect("settlement");
        assert!(matches!(
            outcome,
            OperationAttemptSettlementOutcome::Settled(_)
        ));
        assert!(!repository.registry.contains(&accepted));

        let pending = pending_scan.await.expect("pending scan");
        let unresolved = readiness_scan.await.expect("readiness scan");
        assert!(pending.is_empty());
        assert_eq!(unresolved.total(), 0);
        assert!(unresolved.attempt_ids().is_empty());
        assert_eq!(store.pending_reads.load(Ordering::SeqCst), 2);
        assert_eq!(store.readiness_reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn one_live_attempt_with_no_unresolved_debt_reports_ready() {
        let store = ControllableAttemptRepository::new(None, None);
        let gate = Arc::new(OperationAttemptReadinessGate::new());
        let repository = TrackedOperationAttemptRepository::new(store.clone(), store, gate);
        let live = attempt("attempt-live-only", 19);
        repository.accept(&live).await.expect("acceptance");
        let scan = OperationAttemptScan::without_exclusions(8).expect("bounded scan");

        let pending = repository
            .pending_reconciliation(&scan)
            .await
            .expect("pending scan");
        let unresolved = repository
            .unresolved_attempts(&scan)
            .await
            .expect("readiness scan");

        assert!(pending.is_empty());
        assert_eq!(unresolved.total(), 0);
        assert!(unresolved.attempt_ids().is_empty());
    }

    #[tokio::test]
    async fn one_live_attempt_does_not_consume_a_full_256_unresolved_page() {
        const LIMIT: usize = 256;

        let store = ControllableAttemptRepository::new(None, None);
        let gate = Arc::new(OperationAttemptReadinessGate::new());
        let repository = TrackedOperationAttemptRepository::new(store.clone(), store.clone(), gate);
        let live = attempt("attempt-live-page", 300);
        repository.accept(&live).await.expect("acceptance");
        let expected = (0..LIMIT)
            .map(|index| {
                let id = format!("attempt-unresolved-{index:03}");
                let attempt = unknown_attempt(&id, index as u64 + 1);
                let attempt_id = attempt.id().clone();
                store.seed_attempt(attempt);
                attempt_id
            })
            .collect::<Vec<_>>();
        let scan = OperationAttemptScan::without_exclusions(LIMIT).expect("bounded scan");

        let pending = repository
            .pending_reconciliation(&scan)
            .await
            .expect("pending scan");
        let unresolved = repository
            .unresolved_attempts(&scan)
            .await
            .expect("readiness scan");

        assert_eq!(pending.len(), LIMIT);
        assert_eq!(
            pending
                .iter()
                .map(|attempt| attempt.id().clone())
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(unresolved.total(), LIMIT);
        assert_eq!(unresolved.attempt_ids(), expected);
        assert!(!unresolved.truncated());
    }

    #[tokio::test]
    async fn two_hundred_fifty_six_live_attempts_do_not_hide_unresolved_debt() {
        const LIMIT: usize = 256;

        let store = ControllableAttemptRepository::new(None, None);
        let gate = Arc::new(OperationAttemptReadinessGate::new());
        let repository = TrackedOperationAttemptRepository::new(store.clone(), store.clone(), gate);
        for index in 0..LIMIT {
            let id = format!("attempt-live-{index:03}");
            let live = attempt(&id, index as u64 + 1);
            repository.accept(&live).await.expect("acceptance");
        }
        let hidden = unknown_attempt("attempt-unresolved-hidden", 400);
        let hidden_id = hidden.id().clone();
        store.seed_attempt(hidden);
        let scan = OperationAttemptScan::without_exclusions(LIMIT).expect("bounded scan");

        let pending = repository
            .pending_reconciliation(&scan)
            .await
            .expect("pending scan");
        let unresolved = repository
            .unresolved_attempts(&scan)
            .await
            .expect("readiness scan");

        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id(), &hidden_id);
        assert_eq!(unresolved.total(), 1);
        assert_eq!(unresolved.attempt_ids(), [hidden_id]);
        assert!(!unresolved.truncated());
    }

    #[tokio::test]
    async fn merged_exclusion_overflow_is_typed_and_closes_readiness() {
        let store = ControllableAttemptRepository::new(None, None);
        let gate = Arc::new(OperationAttemptReadinessGate::new());
        gate.apply_unresolved_count(0);
        let repository =
            TrackedOperationAttemptRepository::new(store.clone(), store.clone(), gate.clone());
        let live = attempt("attempt-live-overflow", 23);
        repository.accept(&live).await.expect("acceptance");
        let caller_exclusion = OperationAttemptScanExclusion::new(
            OperationAttemptId::new("attempt-caller-excluded").expect("attempt ID"),
            FencingToken::new(1).expect("fence"),
        );
        let scan = OperationAttemptScan::new(1, vec![caller_exclusion]).expect("bounded scan");

        let error = repository
            .unresolved_attempts(&scan)
            .await
            .expect_err("merged exclusions exceed the representable bound");
        let invalid = error
            .source()
            .and_then(|source| source.downcast_ref::<InvalidOperationAttemptScan>());

        assert_eq!(
            invalid,
            Some(&InvalidOperationAttemptScan::TooManyExclusions {
                limit: 1,
                exclusions: 2,
            })
        );
        assert!(!gate.is_ready());
        assert_eq!(store.readiness_reads.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn live_attempt_readiness_scan_respects_backing_store_limit() {
        const RECONCILIATION_LIMIT: usize = 1;

        let store = ControllableAttemptRepository::new(None, None);
        store.set_hard_read_limit(RECONCILIATION_LIMIT);
        let gate = Arc::new(OperationAttemptReadinessGate::new());
        let repository = TrackedOperationAttemptRepository::new(store.clone(), store.clone(), gate);
        let accepted = attempt("attempt-bounded-readiness", 17);
        repository.accept(&accepted).await.expect("acceptance");

        let scan =
            OperationAttemptScan::without_exclusions(RECONCILIATION_LIMIT).expect("bounded scan");
        let unresolved = repository
            .unresolved_attempts(&scan)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "live attempt filtering must preserve the backing store read bound: {error}; source: {}",
                    error.source().expect("backing read limit source")
                )
            });

        assert_eq!(unresolved.total(), 0);
        assert!(unresolved.attempt_ids().is_empty());
    }
}

#[derive(Debug)]
struct RecoveryAdmissionBlocked;
impl Display for RecoveryAdmissionBlocked {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "unresolved operation attempt blocks this session; inspect recovery diagnostics",
        )
    }
}
impl Error for RecoveryAdmissionBlocked {}
