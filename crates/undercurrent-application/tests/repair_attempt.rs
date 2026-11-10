// SPDX-License-Identifier: Apache-2.0

use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use undercurrent_application::{
    Clock, ObservedOperationAttemptRepair, OperationAttemptReadinessService,
    OperationAttemptReadinessStatus, OperationAttemptRepairActor,
    OperationAttemptRepairAuthorization, OperationAttemptRepairAuthorizer,
    OperationAttemptRepairCommit, OperationAttemptRepairDisposition,
    OperationAttemptRepairEvidenceRef, OperationAttemptRepairObserver,
    OperationAttemptRepairOutcome, OperationAttemptRepairRepository, PortError, PortErrorKind,
    PortFuture, PortOperation, RepairOperationAttemptCommand, RepairOperationAttemptError,
    RepairOperationAttemptService, UnresolvedOperationAttempts,
};
use undercurrent_domain::{
    Classification, DispatchCellIdentity, EffectCertainty, FencingToken, OperationAttempt,
    OperationAttemptId, PayloadDigest, ProcessMutationKind, RuntimeCallbackRef,
    RuntimeDispatchBinding, RuntimeDispatchLimits, RuntimeDispatchPolicy, RuntimeOutputContract,
    RuntimeTelemetryPolicyRef, SessionId, TimestampMillis,
};

#[derive(Debug)]
struct TestPortFailure;

impl Display for TestPortFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("test port failure")
    }
}

impl Error for TestPortFailure {}

fn port_error() -> PortError {
    PortError::new(
        PortErrorKind::Unavailable,
        PortOperation::RepairOperationAttempt,
        None,
        TestPortFailure,
    )
}

#[derive(Clone)]
struct FixedClock(TimestampMillis);

impl Clock for FixedClock {
    fn now(&self) -> TimestampMillis {
        self.0
    }
}

struct FakeAuthorizer {
    decision: OperationAttemptRepairAuthorization,
    trace: Arc<Mutex<Vec<&'static str>>>,
}

impl OperationAttemptRepairAuthorizer for FakeAuthorizer {
    fn authorize_repair<'a>(
        &'a self,
        _actor: &'a OperationAttemptRepairActor,
        _attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<OperationAttemptRepairAuthorization, PortError>> {
        self.trace.lock().expect("trace lock").push("authorize");
        Box::pin(async move { Ok(self.decision) })
    }
}

struct FakeRepository {
    attempt: Mutex<Option<OperationAttempt>>,
    outcomes: Mutex<VecDeque<OperationAttemptRepairOutcome>>,
    unresolved: Mutex<UnresolvedOperationAttempts>,
    commits: Mutex<Vec<OperationAttemptRepairCommit>>,
    trace: Arc<Mutex<Vec<&'static str>>>,
}

impl OperationAttemptRepairRepository for FakeRepository {
    fn load_repair_candidate<'a>(
        &'a self,
        _attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<Option<OperationAttempt>, PortError>> {
        self.trace.lock().expect("trace lock").push("load");
        let attempt = self.attempt.lock().expect("attempt lock").clone();
        Box::pin(async move { Ok(attempt) })
    }

    fn repair_unknown<'a>(
        &'a self,
        commit: &'a OperationAttemptRepairCommit,
    ) -> PortFuture<'a, Result<OperationAttemptRepairOutcome, PortError>> {
        self.trace.lock().expect("trace lock").push("commit");
        self.commits
            .lock()
            .expect("commit lock")
            .push(commit.clone());
        let outcome = self
            .outcomes
            .lock()
            .expect("outcome lock")
            .pop_front()
            .expect("configured repair outcome");
        Box::pin(async move { Ok(outcome) })
    }

    fn unresolved_attempts<'a>(
        &'a self,
        scan: &'a undercurrent_application::OperationAttemptScan,
    ) -> PortFuture<'a, Result<UnresolvedOperationAttempts, PortError>> {
        self.trace.lock().expect("trace lock").push("readiness");
        let unresolved = self.unresolved.lock().expect("readiness lock").clone();
        let attempt = self.attempt.lock().expect("attempt lock").clone();
        Box::pin(async move {
            let excluded = attempt.as_ref().is_some_and(|attempt| {
                scan.exclusions().iter().any(|exclusion| {
                    exclusion.attempt_id() == attempt.id()
                        && exclusion.fencing_token() == attempt.fencing_token()
                })
            });
            let total = unresolved.total().saturating_sub(usize::from(excluded));
            let mut attempt_ids = unresolved
                .attempt_ids()
                .iter()
                .filter(|attempt_id| {
                    !excluded
                        || attempt
                            .as_ref()
                            .is_none_or(|attempt| attempt.id() != *attempt_id)
                })
                .cloned()
                .collect::<Vec<_>>();
            attempt_ids.truncate(scan.limit());
            UnresolvedOperationAttempts::new(total, attempt_ids).map_err(|source| {
                PortError::new(
                    undercurrent_application::PortErrorKind::InvalidData,
                    undercurrent_application::PortOperation::InspectOperationAttemptReadiness,
                    None,
                    source,
                )
            })
        })
    }
}

struct FakeObserver {
    observation: ObservedOperationAttemptRepair,
    acknowledgement_failures: AtomicUsize,
    trace: Arc<Mutex<Vec<&'static str>>>,
}

impl OperationAttemptRepairObserver for FakeObserver {
    fn observe_repair<'a>(
        &'a self,
        _attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<ObservedOperationAttemptRepair, PortError>> {
        self.trace.lock().expect("trace lock").push("observe");
        let observation = self.observation.clone();
        Box::pin(async move { Ok(observation) })
    }

    fn acknowledge_repair<'a>(
        &'a self,
        _attempt: &'a OperationAttempt,
        _evidence_reference: &'a OperationAttemptRepairEvidenceRef,
    ) -> PortFuture<'a, Result<(), PortError>> {
        self.trace.lock().expect("trace lock").push("ack");
        let should_fail = self
            .acknowledgement_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok();
        Box::pin(async move {
            if should_fail {
                Err(port_error())
            } else {
                Ok(())
            }
        })
    }
}

fn timestamp(value: i64) -> TimestampMillis {
    TimestampMillis::new(value)
}

fn fence(value: u64) -> FencingToken {
    FencingToken::new(value).expect("nonzero fence")
}

fn attempt_id(value: &str) -> OperationAttemptId {
    OperationAttemptId::new(value).expect("valid attempt id")
}

fn unknown_attempt(id: &str) -> OperationAttempt {
    let mut attempt = OperationAttempt::accepted(
        attempt_id(id),
        Some(SessionId::new("session-repair").expect("valid session id")),
        ProcessMutationKind::Resize,
        fence(7),
        PayloadDigest::of_bytes(b"resize:80x24"),
        timestamp(10),
    );
    attempt
        .settle(
            attempt.fencing_token(),
            EffectCertainty::Unknown,
            None,
            timestamp(20),
        )
        .expect("Unknown settlement");
    attempt
}

fn unknown_attached_attempt(id: &str, request_digest: Option<PayloadDigest>) -> OperationAttempt {
    let policy = RuntimeDispatchPolicy::new(
        RuntimeDispatchLimits::new(30_000, 1_024, 2_048, None, None).expect("limits"),
        RuntimeOutputContract::new(
            "repair-test/0.1.0-beta",
            PayloadDigest::of_bytes(b"repair output schema"),
            Classification::Internal,
        )
        .expect("output contract"),
        RuntimeTelemetryPolicyRef::new("trust://telemetry/policies/repair-test")
            .expect("telemetry ref"),
        undercurrent_domain::RuntimeRetentionPolicyRef::new(
            "trust://retention/policies/repair-test",
        )
        .expect("retention ref"),
        vec![
            RuntimeCallbackRef::new("runtime-callback://example/workflow/v0.1.0-beta/repair-test")
                .expect("callback ref"),
        ],
        timestamp(1_000),
        timestamp(2_000),
    )
    .expect("runtime policy");
    let cell = DispatchCellIdentity::new("cell:repair").expect("cell identity");
    let binding = RuntimeDispatchBinding::new(
        cell,
        request_digest.unwrap_or_else(|| PayloadDigest::of_bytes(b"unsupported request")),
        PayloadDigest::of_bytes(b"launch"),
        PayloadDigest::of_bytes(b"environment"),
        PayloadDigest::of_bytes(b"input"),
        policy,
    );
    let mut attempt = OperationAttempt::accepted_with_dispatch(
        attempt_id(id),
        Some(SessionId::new("session-repair-attached").expect("session")),
        ProcessMutationKind::Spawn,
        fence(7),
        PayloadDigest::of_bytes(b"undercurrent canonical start"),
        Some(binding),
        timestamp(10),
    );
    attempt
        .settle(
            attempt.fencing_token(),
            EffectCertainty::Unknown,
            None,
            timestamp(20),
        )
        .expect("Unknown settlement");
    attempt
}

fn actor() -> OperationAttemptRepairActor {
    OperationAttemptRepairActor::new(
        "operator-1".to_owned(),
        Some("tenant-1".to_owned()),
        "operation-admin".to_owned(),
    )
    .expect("valid actor references")
}

fn evidence() -> OperationAttemptRepairEvidenceRef {
    OperationAttemptRepairEvidenceRef::new("uchs:receipt/attempt-repair".to_owned())
        .expect("valid evidence reference")
}

fn observation(
    attempt: &OperationAttempt,
    fencing_token: FencingToken,
    certainty: EffectCertainty,
) -> ObservedOperationAttemptRepair {
    let result_digest = (certainty == EffectCertainty::KnownApplied)
        .then(|| PayloadDigest::of_bytes(b"stable-result"));
    ObservedOperationAttemptRepair::new(
        attempt.id().clone(),
        fencing_token,
        certainty,
        result_digest,
        evidence(),
    )
    .expect("valid terminal observation")
}

fn fake_repository(
    attempt: OperationAttempt,
    outcomes: impl IntoIterator<Item = OperationAttemptRepairOutcome>,
    unresolved: UnresolvedOperationAttempts,
    trace: Arc<Mutex<Vec<&'static str>>>,
) -> FakeRepository {
    FakeRepository {
        attempt: Mutex::new(Some(attempt)),
        outcomes: Mutex::new(outcomes.into_iter().collect()),
        unresolved: Mutex::new(unresolved),
        commits: Mutex::new(Vec::new()),
        trace,
    }
}

fn service<'a>(
    authorizer: &'a FakeAuthorizer,
    repository: &'a FakeRepository,
    observer: &'a FakeObserver,
    clock: &'a FixedClock,
) -> RepairOperationAttemptService<'a> {
    RepairOperationAttemptService::new(authorizer, repository, observer, clock, 32)
        .expect("bounded service")
}

#[test]
fn denial_precedes_attempt_and_evidence_access() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let attempt = unknown_attempt("attempt-denied");
    let authorizer = FakeAuthorizer {
        decision: OperationAttemptRepairAuthorization::Denied,
        trace: trace.clone(),
    };
    let repository = fake_repository(
        attempt.clone(),
        [OperationAttemptRepairOutcome::Repaired],
        UnresolvedOperationAttempts::new(0, Vec::new()).expect("empty readiness"),
        trace.clone(),
    );
    let observer = FakeObserver {
        observation: observation(
            &attempt,
            attempt.fencing_token(),
            EffectCertainty::KnownAbsent,
        ),
        acknowledgement_failures: AtomicUsize::new(0),
        trace: trace.clone(),
    };

    let error = block_on(
        service(
            &authorizer,
            &repository,
            &observer,
            &FixedClock(timestamp(30)),
        )
        .execute(RepairOperationAttemptCommand::new(
            attempt.id().clone(),
            actor(),
        )),
    )
    .expect_err("denied repair");

    assert!(matches!(error, RepairOperationAttemptError::Denied));
    assert_eq!(*trace.lock().expect("trace lock"), ["authorize"]);
}

#[test]
fn stale_observation_and_durable_fence_are_rejected_without_acknowledgement() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let attempt = unknown_attempt("attempt-stale-observation");
    let authorizer = FakeAuthorizer {
        decision: OperationAttemptRepairAuthorization::Authorized,
        trace: trace.clone(),
    };
    let repository = fake_repository(
        attempt.clone(),
        [OperationAttemptRepairOutcome::Repaired],
        UnresolvedOperationAttempts::new(1, vec![attempt.id().clone()]).expect("readiness"),
        trace.clone(),
    );
    let observer = FakeObserver {
        observation: observation(&attempt, fence(8), EffectCertainty::KnownAbsent),
        acknowledgement_failures: AtomicUsize::new(0),
        trace: trace.clone(),
    };
    let error = block_on(
        service(
            &authorizer,
            &repository,
            &observer,
            &FixedClock(timestamp(30)),
        )
        .execute(RepairOperationAttemptCommand::new(
            attempt.id().clone(),
            actor(),
        )),
    )
    .expect_err("stale observation");
    assert!(matches!(error, RepairOperationAttemptError::StaleEvidence));
    assert_eq!(
        *trace.lock().expect("trace lock"),
        ["authorize", "load", "observe"]
    );

    let trace = Arc::new(Mutex::new(Vec::new()));
    let attempt = unknown_attempt("attempt-stale-store");
    let authorizer = FakeAuthorizer {
        decision: OperationAttemptRepairAuthorization::Authorized,
        trace: trace.clone(),
    };
    let repository = fake_repository(
        attempt.clone(),
        [OperationAttemptRepairOutcome::StaleFence { current: fence(9) }],
        UnresolvedOperationAttempts::new(1, vec![attempt.id().clone()]).expect("readiness"),
        trace.clone(),
    );
    let observer = FakeObserver {
        observation: observation(
            &attempt,
            attempt.fencing_token(),
            EffectCertainty::KnownAbsent,
        ),
        acknowledgement_failures: AtomicUsize::new(0),
        trace: trace.clone(),
    };
    let error = block_on(
        service(
            &authorizer,
            &repository,
            &observer,
            &FixedClock(timestamp(30)),
        )
        .execute(RepairOperationAttemptCommand::new(
            attempt.id().clone(),
            actor(),
        )),
    )
    .expect_err("stale durable fence");
    assert!(matches!(
        error,
        RepairOperationAttemptError::StaleFence { current } if current == fence(9)
    ));
    assert!(!trace.lock().expect("trace lock").contains(&"ack"));
}

#[test]
fn dispatch_digest_mismatch_and_legacy_absence_fail_before_runtime_evidence() {
    let expected_digest = PayloadDigest::of_bytes(b"caller dispatch request");
    let cases = [
        (
            unknown_attached_attempt("attempt-missing-request-digest", Some(expected_digest)),
            RepairOperationAttemptCommand::new(
                attempt_id("attempt-missing-request-digest"),
                actor(),
            ),
        ),
        (
            unknown_attached_attempt("attempt-mismatched-request-digest", Some(expected_digest)),
            RepairOperationAttemptCommand::attached(
                attempt_id("attempt-mismatched-request-digest"),
                PayloadDigest::of_bytes(b"another caller request"),
                actor(),
            ),
        ),
        (
            unknown_attached_attempt("attempt-legacy-request-digest", None),
            RepairOperationAttemptCommand::attached(
                attempt_id("attempt-legacy-request-digest"),
                expected_digest,
                actor(),
            ),
        ),
        (
            unknown_attached_attempt("attempt-legacy-request-digest-null", None),
            RepairOperationAttemptCommand::new(
                attempt_id("attempt-legacy-request-digest-null"),
                actor(),
            ),
        ),
        (
            unknown_attempt("attempt-standalone-with-digest"),
            RepairOperationAttemptCommand::attached(
                attempt_id("attempt-standalone-with-digest"),
                expected_digest,
                actor(),
            ),
        ),
    ];

    for (attempt, command) in cases {
        let trace = Arc::new(Mutex::new(Vec::new()));
        let authorizer = FakeAuthorizer {
            decision: OperationAttemptRepairAuthorization::Authorized,
            trace: trace.clone(),
        };
        let repository = fake_repository(
            attempt.clone(),
            [OperationAttemptRepairOutcome::Repaired],
            UnresolvedOperationAttempts::new(1, vec![attempt.id().clone()]).expect("readiness"),
            trace.clone(),
        );
        let observer = FakeObserver {
            observation: observation(
                &attempt,
                attempt.fencing_token(),
                EffectCertainty::KnownAbsent,
            ),
            acknowledgement_failures: AtomicUsize::new(0),
            trace: trace.clone(),
        };

        let error = block_on(
            service(
                &authorizer,
                &repository,
                &observer,
                &FixedClock(timestamp(30)),
            )
            .execute(command),
        )
        .expect_err("dispatch binding conflict");
        assert!(matches!(
            error,
            RepairOperationAttemptError::PayloadConflict
        ));
        assert_eq!(*trace.lock().expect("trace lock"), ["authorize", "load"]);
        assert!(repository.commits.lock().expect("commit lock").is_empty());
    }
}

#[test]
fn exact_attached_request_digest_allows_evidence_observation() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let request_digest = PayloadDigest::of_bytes(b"caller dispatch request");
    let attempt = unknown_attached_attempt("attempt-exact-request-digest", Some(request_digest));
    let authorizer = FakeAuthorizer {
        decision: OperationAttemptRepairAuthorization::Authorized,
        trace: trace.clone(),
    };
    let repository = fake_repository(
        attempt.clone(),
        [OperationAttemptRepairOutcome::Repaired],
        UnresolvedOperationAttempts::new(0, Vec::new()).expect("readiness"),
        trace.clone(),
    );
    let observer = FakeObserver {
        observation: observation(
            &attempt,
            attempt.fencing_token(),
            EffectCertainty::KnownAbsent,
        ),
        acknowledgement_failures: AtomicUsize::new(0),
        trace: trace.clone(),
    };

    block_on(
        service(
            &authorizer,
            &repository,
            &observer,
            &FixedClock(timestamp(30)),
        )
        .execute(RepairOperationAttemptCommand::attached(
            attempt.id().clone(),
            request_digest,
            actor(),
        )),
    )
    .expect("digest-bound attached repair");
    assert_eq!(
        *trace.lock().expect("trace lock"),
        ["authorize", "load", "observe", "commit", "ack", "readiness"]
    );
}

#[test]
fn known_absent_and_known_applied_commit_only_valid_terminal_shapes() {
    for certainty in [EffectCertainty::KnownAbsent, EffectCertainty::KnownApplied] {
        let trace = Arc::new(Mutex::new(Vec::new()));
        let attempt = unknown_attempt(if certainty == EffectCertainty::KnownAbsent {
            "attempt-absent"
        } else {
            "attempt-applied"
        });
        let authorizer = FakeAuthorizer {
            decision: OperationAttemptRepairAuthorization::Authorized,
            trace: trace.clone(),
        };
        let repository = fake_repository(
            attempt.clone(),
            [OperationAttemptRepairOutcome::Repaired],
            UnresolvedOperationAttempts::new(0, Vec::new()).expect("ready"),
            trace.clone(),
        );
        let observer = FakeObserver {
            observation: observation(&attempt, attempt.fencing_token(), certainty),
            acknowledgement_failures: AtomicUsize::new(0),
            trace: trace.clone(),
        };

        let result = block_on(
            service(
                &authorizer,
                &repository,
                &observer,
                &FixedClock(timestamp(30)),
            )
            .execute(RepairOperationAttemptCommand::new(
                attempt.id().clone(),
                actor(),
            )),
        )
        .expect("terminal repair");
        assert_eq!(result.certainty(), certainty);
        assert_eq!(
            result.result_digest().is_some(),
            certainty == EffectCertainty::KnownApplied
        );
        assert!(result.readiness().complete());
        let commits = repository.commits.lock().expect("commit lock");
        assert_eq!(commits[0].certainty(), certainty);
        assert_eq!(commits[0].actor().principal(), "operator-1");
        assert_eq!(commits[0].evidence_reference(), result.evidence_reference());
    }
}

#[test]
fn durable_commit_precedes_observer_acknowledgement() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let attempt = unknown_attempt("attempt-ordering");
    let authorizer = FakeAuthorizer {
        decision: OperationAttemptRepairAuthorization::Authorized,
        trace: trace.clone(),
    };
    let repository = fake_repository(
        attempt.clone(),
        [OperationAttemptRepairOutcome::Repaired],
        UnresolvedOperationAttempts::new(0, Vec::new()).expect("ready"),
        trace.clone(),
    );
    let observer = FakeObserver {
        observation: observation(
            &attempt,
            attempt.fencing_token(),
            EffectCertainty::KnownApplied,
        ),
        acknowledgement_failures: AtomicUsize::new(0),
        trace: trace.clone(),
    };

    block_on(
        service(
            &authorizer,
            &repository,
            &observer,
            &FixedClock(timestamp(30)),
        )
        .execute(RepairOperationAttemptCommand::new(
            attempt.id().clone(),
            actor(),
        )),
    )
    .expect("repair");

    assert_eq!(
        *trace.lock().expect("trace lock"),
        ["authorize", "load", "observe", "commit", "ack", "readiness"]
    );
}

#[test]
fn acknowledgement_loss_retries_exact_committed_repair_without_reexecution() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let attempt = unknown_attempt("attempt-ack-retry");
    let authorizer = FakeAuthorizer {
        decision: OperationAttemptRepairAuthorization::Authorized,
        trace: trace.clone(),
    };
    let repository = fake_repository(
        attempt.clone(),
        [
            OperationAttemptRepairOutcome::Repaired,
            OperationAttemptRepairOutcome::Duplicate,
        ],
        UnresolvedOperationAttempts::new(0, Vec::new()).expect("ready"),
        trace.clone(),
    );
    let observer = FakeObserver {
        observation: observation(
            &attempt,
            attempt.fencing_token(),
            EffectCertainty::KnownApplied,
        ),
        acknowledgement_failures: AtomicUsize::new(1),
        trace: trace.clone(),
    };
    let clock = FixedClock(timestamp(30));
    let service = service(&authorizer, &repository, &observer, &clock);
    let command = RepairOperationAttemptCommand::new(attempt.id().clone(), actor());

    let error = block_on(service.execute(command.clone())).expect_err("first ACK is lost");
    assert!(matches!(
        error,
        RepairOperationAttemptError::AcknowledgementAfterCommit(_)
    ));
    let result = block_on(service.execute(command)).expect("retry ACKs retained evidence");
    assert_eq!(
        result.disposition(),
        OperationAttemptRepairDisposition::Duplicate
    );
    let trace = trace.lock().expect("trace lock");
    assert_eq!(trace.iter().filter(|entry| **entry == "commit").count(), 2);
    assert_eq!(trace.iter().filter(|entry| **entry == "ack").count(), 2);
    assert_eq!(
        trace.iter().filter(|entry| **entry == "readiness").count(),
        1
    );
}

#[test]
fn unresolved_attempts_block_readiness_with_exact_count_and_bounded_ids() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let attempt = unknown_attempt("attempt-readiness");
    let repository = fake_repository(
        attempt.clone(),
        [],
        UnresolvedOperationAttempts::new(3, vec![attempt.id().clone()])
            .expect("bounded unresolved page"),
        trace,
    );
    let readiness = block_on(
        OperationAttemptReadinessService::new(&repository, 1)
            .expect("bounded readiness service")
            .inspect(),
    )
    .expect("readiness projection");

    assert_eq!(
        readiness.status(),
        OperationAttemptReadinessStatus::BlockedUnresolvedAttempts
    );
    assert!(!readiness.complete());
    assert_eq!(readiness.unresolved().total(), 3);
    assert_eq!(readiness.unresolved().attempt_ids(), [attempt.id().clone()]);
    assert!(readiness.unresolved().truncated());
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
