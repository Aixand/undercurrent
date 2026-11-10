// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

use undercurrent_application::{
    Clock, DurableProcessRuntime, ObservedOperationAttemptEffect, OperationAttemptAcceptance,
    OperationAttemptAdmission, OperationAttemptEffectObserver, OperationAttemptReconciliation,
    OperationAttemptReconciliationLimits, OperationAttemptSettlement,
    OperationAttemptSettlementOutcome, OperationAttemptStore, PortError, PortErrorKind, PortFuture,
    PortOperation, ProcessInstance, ProcessMutationContext, ProcessRuntime, ProcessTermination,
    ReconcileOperationAttemptsService, TerminationKind,
};
use undercurrent_domain::{
    Classification, CommandSpec, DispatchCellIdentity, EffectCertainty, EnvironmentVariables,
    ExecutionLimits, FencingToken, InputDeliveryPolicy, InputSequence, LaunchSpec,
    OperationAttempt, OperationAttemptId, OperationAttemptState, PayloadDigest, ProcessInstanceId,
    ProcessMutationKind, RuntimeCallbackRef, RuntimeDispatchBinding, RuntimeDispatchLimits,
    RuntimeDispatchPolicy, RuntimeOutputContract, RuntimeTelemetryPolicyRef, SessionId, StopPolicy,
    TerminalSize, TimestampMillis,
};

#[test]
fn durable_runtime_settles_committed_not_started_and_unknown_outcomes() {
    for (raw_outcome, expected) in [
        (RawOutcome::Success, EffectCertainty::KnownApplied),
        (RawOutcome::KnownAbsent, EffectCertainty::KnownAbsent),
        (RawOutcome::ResponseLost, EffectCertainty::Unknown),
    ] {
        let store = Arc::new(MemoryAttemptStore::default());
        let raw = Arc::new(ScriptedRuntime::new([raw_outcome]));
        let clock = Arc::new(FixedClock(TimestampMillis::new(200)));
        let runtime = DurableProcessRuntime::new(raw, store.clone(), clock);
        let admission = admission("outcome-attempt", b"same-effect", 100);
        let result = block_on(runtime.force_stop_admitted(&admission, &session(), &process()));

        match expected {
            EffectCertainty::KnownApplied => assert!(result.is_ok()),
            EffectCertainty::KnownAbsent | EffectCertainty::Unknown => {
                let error = result.expect_err("scripted runtime must fail");
                assert_eq!(error.effect_certainty(), Some(expected));
            }
        }
        let attempt = store.load_sync(admission.attempt_id());
        assert!(matches!(
            attempt.state(),
            OperationAttemptState::Settled { certainty, .. } if *certainty == expected
        ));
        if expected == EffectCertainty::KnownApplied {
            assert!(matches!(
                attempt.state(),
                OperationAttemptState::Settled {
                    result_digest: Some(_),
                    ..
                }
            ));
        }
    }
}

#[test]
fn duplicate_reordered_and_pid_reuse_attempts_do_not_repeat_effects() {
    let store = Arc::new(MemoryAttemptStore::default());
    let raw = Arc::new(ScriptedRuntime::new([RawOutcome::Success]));
    let clock = Arc::new(FixedClock(TimestampMillis::new(200)));
    let runtime = DurableProcessRuntime::new(raw.clone(), store.clone(), clock);
    let original = admission("stable-attempt", b"first-payload", 100);
    store.seed(accepted(&original, 7));

    block_on(runtime.force_stop_admitted(&original, &session(), &process()))
        .expect("accepted duplicate resumes with its original fence");
    let already_applied = block_on(runtime.force_stop_admitted(&original, &session(), &process()))
        .expect_err("a settled effect must not execute twice");
    assert_eq!(
        already_applied.effect_certainty(),
        Some(EffectCertainty::KnownAbsent)
    );

    let reused = admission("stable-attempt", b"different-payload", 101);
    let conflict = block_on(runtime.force_stop_admitted(&reused, &session(), &process()))
        .expect_err("PID reuse with different semantics must conflict");
    assert_eq!(
        conflict.effect_certainty(),
        Some(EffectCertainty::KnownAbsent)
    );

    store.reject_next_as_stale();
    let reordered = admission("older-attempt", b"older", 50);
    let stale = block_on(runtime.force_stop_admitted(&reordered, &session(), &process()))
        .expect_err("stale fence must be rejected before runtime IO");
    assert_eq!(stale.effect_certainty(), Some(EffectCertainty::KnownAbsent));
    assert_eq!(raw.calls(), vec![FencingToken::new(7).unwrap()]);
}

#[test]
fn response_loss_retry_reconciles_retained_success() {
    let store = Arc::new(MemoryAttemptStore::default());
    let admission = admission("lost-response", b"effect", 100);
    let mut unknown = accepted(&admission, 4);
    unknown
        .settle(
            FencingToken::new(4).unwrap(),
            EffectCertainty::Unknown,
            None,
            TimestampMillis::new(150),
        )
        .unwrap();
    store.seed(unknown);
    let raw = Arc::new(ScriptedRuntime::new([RawOutcome::Success]));
    let clock = Arc::new(FixedClock(TimestampMillis::new(200)));
    let runtime = DurableProcessRuntime::new(raw, store.clone(), clock);

    block_on(runtime.force_stop_admitted(&admission, &session(), &process()))
        .expect("retained result resolves response loss");
    let attempt = store.load_sync(admission.attempt_id());
    assert!(matches!(
        attempt.state(),
        OperationAttemptState::Settled {
            certainty: EffectCertainty::KnownApplied,
            result_digest: Some(_),
            ..
        }
    ));
    assert_eq!(store.reconciliations(), vec![EffectCertainty::KnownApplied]);
    assert!(store.settlements().is_empty());
}

#[test]
fn concurrent_attempts_receive_distinct_monotonic_fences() {
    let store = Arc::new(MemoryAttemptStore::default());
    let raw = Arc::new(ScriptedRuntime::new([
        RawOutcome::Success,
        RawOutcome::Success,
    ]));
    let clock = Arc::new(FixedClock(TimestampMillis::new(200)));
    let runtime = DurableProcessRuntime::new(raw.clone(), store, clock);
    let first = admission("concurrent-a", b"a", 100);
    let second = admission("concurrent-b", b"b", 100);

    thread::scope(|scope| {
        let one = scope.spawn(|| {
            block_on(runtime.force_stop_admitted(&first, &session(), &process())).unwrap();
        });
        let two = scope.spawn(|| {
            block_on(runtime.force_stop_admitted(&second, &session(), &process())).unwrap();
        });
        one.join().unwrap();
        two.join().unwrap();
    });

    let mut fences = raw.calls();
    fences.sort_unstable();
    assert_eq!(
        fences,
        vec![FencingToken::new(1).unwrap(), FencingToken::new(2).unwrap()]
    );
}

#[test]
fn attached_policy_is_revalidated_after_delay_immediately_before_durable_admission() {
    let store = Arc::new(MemoryAttemptStore::default());
    let raw = Arc::new(ScriptedRuntime::new([]));
    let clock = Arc::new(MutableClock::new(199));
    let runtime = DurableProcessRuntime::new(raw.clone(), store.clone(), clock.clone());
    let admission = dispatched_admission("expiring-dispatch", runtime_binding(200));

    assert!(
        admission
            .dispatch_binding()
            .and_then(RuntimeDispatchBinding::runtime_policy)
            .expect("runtime policy")
            .validate_at(clock.now())
            .is_ok(),
        "the boundary-time snapshot is initially current"
    );
    clock.set(200);

    let error = block_on(runtime.spawn_admitted(&admission, &session(), &launch()))
        .expect_err("policy expired during pre-admission work");
    assert_eq!(error.kind(), PortErrorKind::Conflict);
    assert_eq!(error.effect_certainty(), Some(EffectCertainty::KnownAbsent));
    assert_eq!(store.accept_calls(), 0);
    assert!(
        raw.calls().is_empty(),
        "expired dispatch must perform no runtime IO"
    );
}

#[test]
fn attached_policy_expiring_during_store_accept_is_settled_known_absent_without_runtime_io() {
    let store = Arc::new(MemoryAttemptStore::default());
    let raw = Arc::new(ScriptedRuntime::new([]));
    let clock = Arc::new(MutableClock::new(199));
    store.advance_clock_on_accept(clock.clone(), 200);
    let runtime = DurableProcessRuntime::new(raw.clone(), store.clone(), clock);
    let admission = dispatched_admission("store-delayed-expiry", runtime_binding(200));

    let error = block_on(runtime.spawn_admitted(&admission, &session(), &launch()))
        .expect_err("policy expired while durable acceptance was queued");

    assert_eq!(error.kind(), PortErrorKind::Conflict);
    assert_eq!(error.effect_certainty(), Some(EffectCertainty::KnownAbsent));
    assert_eq!(store.accept_calls(), 1);
    assert_eq!(store.settlements(), vec![EffectCertainty::KnownAbsent]);
    assert!(matches!(
        store.load_sync(admission.attempt_id()).state(),
        OperationAttemptState::Settled {
            certainty: EffectCertainty::KnownAbsent,
            result_digest: None,
            ..
        }
    ));
    assert!(
        raw.calls().is_empty(),
        "post-accept expiry must be terminalized before process IO"
    );
}

#[test]
fn shutdown_recovery_requires_durable_cancellation_and_preserves_other_unknown_effects() {
    for cancellation_supported in [false, true] {
        let store = MemoryAttemptStore::default();
        let shutdown = OperationAttempt::accepted(
            OperationAttemptId::new("shutdown-before-settlement").unwrap(),
            None,
            ProcessMutationKind::Shutdown,
            FencingToken::new(1).unwrap(),
            PayloadDigest::of_bytes(b"shutdown"),
            TimestampMillis::new(10),
        );
        store.seed(shutdown.clone());
        let other = unknown_attempt(&admission("unknown-input", b"input", 10), 1);
        store.seed(other.clone());
        let mut observer = ScriptedObserver::new([(
            other.id().clone(),
            ObservedOperationAttemptEffect {
                certainty: EffectCertainty::Unknown,
                result_digest: None,
            },
        )]);
        observer.cancel_shutdown = cancellation_supported;
        let clock = FixedClock(TimestampMillis::new(30));
        let report = block_on(
            ReconcileOperationAttemptsService::new(
                &store,
                &observer,
                &clock,
                OperationAttemptReconciliationLimits::new(8, 3).unwrap(),
            )
            .execute(),
        )
        .unwrap();
        assert!(
            !report.complete,
            "unrelated unknown effects must still block readiness"
        );
        assert!(report.unresolved.contains(other.id()));
        if cancellation_supported {
            assert_eq!(report.resolved, 1);
            assert_eq!(observer.acknowledged(), [shutdown.id().clone()]);
            assert!(matches!(
                store.load_sync(shutdown.id()).state(),
                OperationAttemptState::Settled {
                    certainty: EffectCertainty::KnownAbsent,
                    ..
                }
            ));
        } else {
            assert_eq!(report.resolved, 0);
            assert!(report.unresolved.contains(shutdown.id()));
            assert!(observer.acknowledged().is_empty());
            assert!(matches!(
                store.load_sync(shutdown.id()).state(),
                OperationAttemptState::Accepted
            ));
        }
    }
}

#[test]
fn reconciliation_acknowledges_only_after_durable_commit() {
    let store = MemoryAttemptStore::default();
    let known_admission = admission("known-after-restart", b"known", 100);
    let unknown_admission = admission("still-unknown", b"unknown", 100);
    store.seed(unknown_attempt(&known_admission, 1));
    store.seed(unknown_attempt(&unknown_admission, 2));
    let observer = ScriptedObserver::new([
        (
            known_admission.attempt_id().clone(),
            ObservedOperationAttemptEffect {
                certainty: EffectCertainty::KnownApplied,
                result_digest: Some(PayloadDigest::of_bytes(b"retained-terminal-result")),
            },
        ),
        (
            unknown_admission.attempt_id().clone(),
            ObservedOperationAttemptEffect {
                certainty: EffectCertainty::Unknown,
                result_digest: None,
            },
        ),
    ]);
    let clock = FixedClock(TimestampMillis::new(300));
    let service = ReconcileOperationAttemptsService::new(
        &store,
        &observer,
        &clock,
        OperationAttemptReconciliationLimits::new(8, 2).unwrap(),
    );

    let report = block_on(service.execute()).unwrap();
    assert_eq!(report.resolved, 1);
    assert_eq!(
        report.unresolved,
        vec![unknown_admission.attempt_id().clone()]
    );
    assert!(!report.complete);
    assert_eq!(
        observer.acknowledged(),
        vec![known_admission.attempt_id().clone()]
    );
    assert!(matches!(
        store.load_sync(known_admission.attempt_id()).state(),
        OperationAttemptState::Settled {
            certainty: EffectCertainty::KnownApplied,
            ..
        }
    ));
}

fn session() -> SessionId {
    SessionId::new("session-1").unwrap()
}

fn process() -> ProcessInstanceId {
    ProcessInstanceId::new("pid-reused-1").unwrap()
}

fn admission(id: &str, payload: &[u8], accepted_at: i64) -> OperationAttemptAdmission {
    OperationAttemptAdmission::new(
        OperationAttemptId::new(id).unwrap(),
        Some(session()),
        ProcessMutationKind::ForceStop,
        PayloadDigest::of_bytes(payload),
        TimestampMillis::new(accepted_at),
    )
}

fn dispatched_admission(id: &str, binding: RuntimeDispatchBinding) -> OperationAttemptAdmission {
    OperationAttemptAdmission::dispatched(
        OperationAttemptId::new(id).expect("attempt ID"),
        Some(session()),
        ProcessMutationKind::Spawn,
        FencingToken::new(7).expect("fence"),
        PayloadDigest::of_bytes(b"canonical dispatched start"),
        binding,
        TimestampMillis::new(100),
    )
}

fn runtime_binding(expires_at: i64) -> RuntimeDispatchBinding {
    RuntimeDispatchBinding::new(
        DispatchCellIdentity::new("cell:build").expect("cell"),
        PayloadDigest::of_bytes(b"caller dispatch request"),
        PayloadDigest::of_bytes(b"launch"),
        PayloadDigest::of_bytes(b"environment"),
        PayloadDigest::of_bytes(b"input"),
        RuntimeDispatchPolicy::new(
            RuntimeDispatchLimits::new(30_000, 1_024, 2_048, None, None).expect("limits"),
            RuntimeOutputContract::new(
                "application-test/0.1.0-beta",
                PayloadDigest::of_bytes(b"output schema"),
                Classification::Internal,
            )
            .expect("output contract"),
            RuntimeTelemetryPolicyRef::new("trust://telemetry/policies/application-test")
                .expect("telemetry ref"),
            undercurrent_domain::RuntimeRetentionPolicyRef::new(
                "trust://retention/policies/application-test",
            )
            .expect("retention ref"),
            vec![
                RuntimeCallbackRef::new(
                    "runtime-callback://example/workflow/v0.1.0-beta/application-test",
                )
                .expect("callback ref"),
            ],
            TimestampMillis::new(expires_at),
            TimestampMillis::new(expires_at + 100),
        )
        .expect("runtime policy"),
    )
}

fn launch() -> LaunchSpec {
    let limits = ExecutionLimits {
        max_arguments: 8,
        max_command_bytes: 1_024,
        max_environment_entries: 8,
        max_environment_bytes: 2_048,
        max_path_bytes: 256,
        max_health_rules: 8,
        max_health_rule_bytes: 256,
    };
    LaunchSpec::new(
        CommandSpec::new("sh", Vec::new(), limits).expect("command"),
        EnvironmentVariables::default(),
        None,
        None,
        TerminalSize::new(80, 24, 400, 200).expect("terminal size"),
    )
}

fn accepted(admission: &OperationAttemptAdmission, fence: u64) -> OperationAttempt {
    OperationAttempt::accepted(
        admission.attempt_id().clone(),
        admission.target_session().cloned(),
        admission.kind(),
        FencingToken::new(fence).unwrap(),
        admission.payload_digest(),
        admission.accepted_at(),
    )
}

fn unknown_attempt(admission: &OperationAttemptAdmission, fence: u64) -> OperationAttempt {
    let mut attempt = accepted(admission, fence);
    attempt
        .settle(
            attempt.fencing_token(),
            EffectCertainty::Unknown,
            None,
            TimestampMillis::new(150),
        )
        .unwrap();
    attempt
}

#[derive(Default)]
struct MemoryAttemptStore {
    state: Mutex<StoreState>,
}

#[derive(Default)]
struct StoreState {
    attempts: BTreeMap<OperationAttemptId, OperationAttempt>,
    next_fence: u64,
    reject_stale: bool,
    settlements: Vec<EffectCertainty>,
    reconciliations: Vec<EffectCertainty>,
    accept_calls: usize,
    advance_clock_on_accept: Option<(Arc<MutableClock>, i64)>,
}

impl MemoryAttemptStore {
    fn advance_clock_on_accept(&self, clock: Arc<MutableClock>, value: i64) {
        self.state.lock().unwrap().advance_clock_on_accept = Some((clock, value));
    }

    fn seed(&self, attempt: OperationAttempt) {
        let mut state = self.state.lock().unwrap();
        state.next_fence = state.next_fence.max(attempt.fencing_token().value());
        state.attempts.insert(attempt.id().clone(), attempt);
    }

    fn reject_next_as_stale(&self) {
        self.state.lock().unwrap().reject_stale = true;
    }

    fn load_sync(&self, id: &OperationAttemptId) -> OperationAttempt {
        self.state.lock().unwrap().attempts.get(id).unwrap().clone()
    }

    fn settlements(&self) -> Vec<EffectCertainty> {
        self.state.lock().unwrap().settlements.clone()
    }

    fn reconciliations(&self) -> Vec<EffectCertainty> {
        self.state.lock().unwrap().reconciliations.clone()
    }

    fn accept_calls(&self) -> usize {
        self.state.lock().unwrap().accept_calls
    }
}

impl OperationAttemptStore for MemoryAttemptStore {
    fn accept_next<'a>(
        &'a self,
        admission: &'a OperationAttemptAdmission,
    ) -> PortFuture<'a, Result<OperationAttemptAcceptance, PortError>> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap();
            state.accept_calls += 1;
            if state.reject_stale {
                state.reject_stale = false;
                return Ok(OperationAttemptAcceptance::StaleFence {
                    current: FencingToken::new(state.next_fence.max(1)).unwrap(),
                });
            }
            if let Some(existing) = state.attempts.get(admission.attempt_id()) {
                if !admission.has_same_semantics(existing) {
                    return Ok(OperationAttemptAcceptance::PayloadConflict);
                }
                return Ok(match existing.state() {
                    OperationAttemptState::Accepted => {
                        OperationAttemptAcceptance::Duplicate(existing.clone())
                    }
                    OperationAttemptState::Settled { .. } => {
                        OperationAttemptAcceptance::AlreadySettled(existing.clone())
                    }
                    OperationAttemptState::Compensated { .. } => {
                        OperationAttemptAcceptance::AlreadyCompensated(existing.clone())
                    }
                });
            }
            state.next_fence = state.next_fence.checked_add(1).unwrap();
            let attempt = accepted(admission, state.next_fence);
            state
                .attempts
                .insert(admission.attempt_id().clone(), attempt.clone());
            Ok(OperationAttemptAcceptance::Accepted(attempt))
        })
    }

    fn accept<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<OperationAttemptAcceptance, PortError>> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap();
            state.accept_calls += 1;
            if let Some(existing) = state.attempts.get(attempt.id()) {
                return Ok(if existing.has_same_semantics(attempt) {
                    OperationAttemptAcceptance::Duplicate(existing.clone())
                } else {
                    OperationAttemptAcceptance::PayloadConflict
                });
            }
            state.attempts.insert(attempt.id().clone(), attempt.clone());
            if let Some((clock, value)) = state.advance_clock_on_accept.take() {
                clock.set(value);
            }
            Ok(OperationAttemptAcceptance::Accepted(attempt.clone()))
        })
    }

    fn settle<'a>(
        &'a self,
        settlement: &'a OperationAttemptSettlement,
    ) -> PortFuture<'a, Result<OperationAttemptSettlementOutcome, PortError>> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap();
            let Some(mut attempt) = state.attempts.remove(&settlement.attempt_id) else {
                return Ok(OperationAttemptSettlementOutcome::NotFound);
            };
            if attempt
                .settle(
                    settlement.fencing_token,
                    settlement.certainty,
                    settlement.result_digest,
                    settlement.settled_at,
                )
                .is_err()
            {
                state.attempts.insert(attempt.id().clone(), attempt);
                return Ok(OperationAttemptSettlementOutcome::PayloadConflict);
            }
            state.settlements.push(settlement.certainty);
            state.attempts.insert(attempt.id().clone(), attempt.clone());
            Ok(OperationAttemptSettlementOutcome::Settled(attempt))
        })
    }

    fn reconcile<'a>(
        &'a self,
        reconciliation: &'a OperationAttemptReconciliation,
    ) -> PortFuture<'a, Result<OperationAttemptSettlementOutcome, PortError>> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap();
            let Some(mut attempt) = state.attempts.remove(&reconciliation.attempt_id) else {
                return Ok(OperationAttemptSettlementOutcome::NotFound);
            };
            if attempt
                .reconcile(
                    reconciliation.certainty,
                    reconciliation.result_digest,
                    reconciliation.reconciled_at,
                )
                .is_err()
            {
                state.attempts.insert(attempt.id().clone(), attempt);
                return Ok(OperationAttemptSettlementOutcome::PayloadConflict);
            }
            state.reconciliations.push(reconciliation.certainty);
            state.attempts.insert(attempt.id().clone(), attempt.clone());
            Ok(OperationAttemptSettlementOutcome::Settled(attempt))
        })
    }

    fn load<'a>(
        &'a self,
        attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<Option<OperationAttempt>, PortError>> {
        Box::pin(async move { Ok(self.state.lock().unwrap().attempts.get(attempt_id).cloned()) })
    }

    fn pending_reconciliation<'a>(
        &'a self,
        scan: &'a undercurrent_application::OperationAttemptScan,
    ) -> PortFuture<'a, Result<Vec<OperationAttempt>, PortError>> {
        Box::pin(async move {
            Ok(self
                .state
                .lock()
                .unwrap()
                .attempts
                .values()
                .filter(|attempt| attempt.state().requires_reconciliation())
                .filter(|attempt| {
                    !scan.exclusions().iter().any(|exclusion| {
                        exclusion.attempt_id() == attempt.id()
                            && exclusion.fencing_token() == attempt.fencing_token()
                    })
                })
                .take(scan.limit())
                .cloned()
                .collect())
        })
    }
}

#[derive(Debug, Clone, Copy)]
enum RawOutcome {
    Success,
    KnownAbsent,
    ResponseLost,
}

struct ScriptedRuntime {
    outcomes: Mutex<VecDeque<RawOutcome>>,
    calls: Mutex<Vec<FencingToken>>,
}

impl ScriptedRuntime {
    fn new(outcomes: impl IntoIterator<Item = RawOutcome>) -> Self {
        Self {
            outcomes: Mutex::new(outcomes.into_iter().collect()),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<FencingToken> {
        self.calls.lock().unwrap().clone()
    }

    fn next_stop(&self, context: &ProcessMutationContext) -> Result<ProcessTermination, PortError> {
        self.calls.lock().unwrap().push(context.fencing_token());
        match self.outcomes.lock().unwrap().pop_front().unwrap() {
            RawOutcome::Success => Ok(ProcessTermination::new(
                TerminationKind::Forced,
                Some(137),
                TimestampMillis::new(175),
            )),
            RawOutcome::KnownAbsent => Err(test_port_error(PortOperation::ForceProcess)
                .with_attempt_effect(context.attempt_id().clone(), EffectCertainty::KnownAbsent)),
            RawOutcome::ResponseLost => Err(test_port_error(PortOperation::ForceProcess)),
        }
    }
}

impl ProcessRuntime for ScriptedRuntime {
    fn spawn<'a>(
        &'a self,
        session_id: &'a SessionId,
        _launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<ProcessInstance, PortError>> {
        unsupported(PortOperation::SpawnProcess, session_id)
    }

    fn spawn_attempted<'a>(
        &'a self,
        context: &'a ProcessMutationContext,
        _session_id: &'a SessionId,
        _launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<ProcessInstance, PortError>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(context.fencing_token());
            Ok(ProcessInstance {
                id: ProcessInstanceId::new("spawned-process").expect("process ID"),
                started_at: TimestampMillis::new(175),
            })
        })
    }

    fn arm_exit<'a>(
        &'a self,
        session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<(), PortError>> {
        unsupported(PortOperation::ArmProcessExit, session_id)
    }

    fn stop<'a>(
        &'a self,
        session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
        _policy: StopPolicy,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        unsupported(PortOperation::StopProcess, session_id)
    }

    fn force_stop<'a>(
        &'a self,
        session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        unsupported(PortOperation::ForceProcess, session_id)
    }

    fn force_stop_attempted<'a>(
        &'a self,
        context: &'a ProcessMutationContext,
        _session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        Box::pin(async move { self.next_stop(context) })
    }

    fn write_input<'a>(
        &'a self,
        session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
        _input: &'a InputSequence,
        _policy: InputDeliveryPolicy,
    ) -> PortFuture<'a, Result<(), PortError>> {
        unsupported(PortOperation::WriteInput, session_id)
    }

    fn resize<'a>(
        &'a self,
        session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
        _size: TerminalSize,
    ) -> PortFuture<'a, Result<(), PortError>> {
        unsupported(PortOperation::ResizeTerminal, session_id)
    }
}

struct ScriptedObserver {
    cancel_shutdown: bool,
    effects: BTreeMap<OperationAttemptId, ObservedOperationAttemptEffect>,
    acknowledged: Mutex<Vec<OperationAttemptId>>,
}

impl ScriptedObserver {
    fn new(
        effects: impl IntoIterator<Item = (OperationAttemptId, ObservedOperationAttemptEffect)>,
    ) -> Self {
        Self {
            cancel_shutdown: false,
            effects: effects.into_iter().collect(),
            acknowledged: Mutex::new(Vec::new()),
        }
    }

    fn acknowledged(&self) -> Vec<OperationAttemptId> {
        self.acknowledged.lock().unwrap().clone()
    }
}

impl OperationAttemptEffectObserver for ScriptedObserver {
    fn cancel_uncommitted_shutdown<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<bool, PortError>> {
        assert_eq!(attempt.kind(), ProcessMutationKind::Shutdown);
        Box::pin(async move { Ok(self.cancel_shutdown) })
    }

    fn acknowledge_shutdown_cancellation<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        assert!(
            matches!(
                attempt.state(),
                OperationAttemptState::Settled {
                    certainty: EffectCertainty::KnownAbsent,
                    ..
                }
            ),
            "only release the receipt after committing cancellation"
        );
        self.acknowledge_observation(attempt)
    }

    fn observe<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<ObservedOperationAttemptEffect, PortError>> {
        Box::pin(async move { Ok(*self.effects.get(attempt.id()).unwrap()) })
    }

    fn acknowledge_observation<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            self.acknowledged.lock().unwrap().push(attempt.id().clone());
            Ok(())
        })
    }
}

struct FixedClock(TimestampMillis);

impl Clock for FixedClock {
    fn now(&self) -> TimestampMillis {
        self.0
    }
}

struct MutableClock(AtomicI64);

impl MutableClock {
    fn new(value: i64) -> Self {
        Self(AtomicI64::new(value))
    }

    fn set(&self, value: i64) {
        self.0.store(value, Ordering::SeqCst);
    }
}

impl Clock for MutableClock {
    fn now(&self) -> TimestampMillis {
        TimestampMillis::new(self.0.load(Ordering::SeqCst))
    }
}

fn unsupported<'a, T: Send + 'a>(
    operation: PortOperation,
    session_id: &'a SessionId,
) -> PortFuture<'a, Result<T, PortError>> {
    let error = PortError::new(
        PortErrorKind::Unsupported,
        operation,
        Some(session_id.clone()),
        TestFailure,
    );
    Box::pin(async move { Err(error) })
}

fn test_port_error(operation: PortOperation) -> PortError {
    PortError::new(
        PortErrorKind::Unavailable,
        operation,
        Some(session()),
        TestFailure,
    )
}

#[derive(Debug)]
struct TestFailure;

impl Display for TestFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("test failure")
    }
}

impl Error for TestFailure {}

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let parker = ThreadWake(thread::current());
    let waker = Waker::from(Arc::new(parker));
    let mut context = Context::from_waker(&waker);
    loop {
        match Pin::as_mut(&mut future).poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}

struct ThreadWake(Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}
