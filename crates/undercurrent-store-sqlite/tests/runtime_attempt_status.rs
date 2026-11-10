// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use undercurrent_application::{
    InsertSessionOutcome, OperationAttemptAcceptance, OperationAttemptCompensation,
    OperationAttemptCompensationOutcome, OperationAttemptCompensationPreparation,
    OperationAttemptCompensationPreparationOutcome, OperationAttemptSettlement,
    OperationAttemptSettlementOutcome, OperationAttemptStore, RuntimeAttemptStatusLookup,
    RuntimeAttemptStatusRepository, RuntimeAttemptStatusSnapshot, SessionOutbox,
    SessionOutboxEvent, SessionRepository,
};
use undercurrent_domain::{
    CommandSpec, DispatchCellIdentity, DomainEventKind, EffectCertainty, EnvironmentVariables,
    ExecutionLimits, ExecutionPolicy, FencingToken, HealthRules, Labels, LaunchSpec,
    LifecycleState, Metadata, OperationAttempt, OperationAttemptId, PayloadDigest, ProcessIdentity,
    ProcessInstanceId, ProcessMutationKind, ProfileName, RestartConfig, Revision,
    RuntimeCallbackRef, RuntimeDispatchBinding, RuntimeDispatchLimits, RuntimeDispatchPolicy,
    RuntimeOutputContract, RuntimeTelemetryPolicyRef, Session, SessionConfiguration, SessionId,
    SessionName, StopPolicy, StopSignal, TerminalSize, TimestampMillis,
};
use undercurrent_foundation::Classification;
use undercurrent_store_sqlite::{
    SqliteRepositoryConfig, SqliteRepositoryLimits, SqliteSessionRepository,
};

const REQUEST_BYTES: &[u8] = b"caller dispatch request";

#[test]
fn status_reads_every_retained_state_without_mutating_attempt_session_or_outbox() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory.path().join("runtime-attempt-status.sqlite3");
    let repository = open(&database_path);

    let accepted = seed(&repository, "attempt-accepted", "session-accepted", 1);
    let unknown = seed(&repository, "attempt-unknown", "session-unknown", 2);
    settle(&repository, &unknown, EffectCertainty::Unknown, None, 20);
    let absent = seed(&repository, "attempt-absent", "session-absent", 3);
    settle(&repository, &absent, EffectCertainty::KnownAbsent, None, 21);
    let applied = seed(&repository, "attempt-applied", "session-applied", 4);
    let result_digest = PayloadDigest::of_bytes(b"exact start result");
    settle(
        &repository,
        &applied,
        EffectCertainty::KnownApplied,
        Some(result_digest),
        22,
    );
    let compensated = seed(&repository, "attempt-compensated", "session-compensated", 5);
    let superseding = attempt("attempt-superseding", "session-compensated", 6);
    accept(&repository, &superseding);
    settle(
        &repository,
        &superseding,
        EffectCertainty::KnownApplied,
        Some(PayloadDigest::of_bytes(b"superseding start result")),
        25,
    );
    compensate(&repository, &compensated, &superseding);

    let tracked_attempts = [
        accepted.clone(),
        unknown.clone(),
        absent.clone(),
        applied.clone(),
        compensated.clone(),
        superseding.clone(),
    ];
    let attempts_before = load_attempts(&repository, &tracked_attempts);
    let sessions_before = [
        load_session(&repository, "session-accepted"),
        load_session(&repository, "session-unknown"),
        load_session(&repository, "session-absent"),
        load_session(&repository, "session-applied"),
        load_session(&repository, "session-compensated"),
    ];
    let outbox_before = block_on(SessionOutbox::read_pending(&repository, 100))
        .expect("read outbox before status queries");
    let pressure_before = block_on(repository.pressure()).expect("pressure before status queries");

    assert!(matches!(
        status(&repository, accepted.id(), request_digest()),
        RuntimeAttemptStatusLookup::Found(result)
            if matches!(result.snapshot(), RuntimeAttemptStatusSnapshot::Unknown {
                observed_at
            } if *observed_at == TimestampMillis::new(10))
    ));
    assert!(matches!(
        status(&repository, unknown.id(), request_digest()),
        RuntimeAttemptStatusLookup::Found(result)
            if matches!(result.snapshot(), RuntimeAttemptStatusSnapshot::Unknown {
                observed_at
            } if *observed_at == TimestampMillis::new(20))
    ));
    assert!(matches!(
        status(&repository, absent.id(), request_digest()),
        RuntimeAttemptStatusLookup::Found(result)
            if matches!(result.snapshot(), RuntimeAttemptStatusSnapshot::KnownAbsent {
                observed_at
            } if *observed_at == TimestampMillis::new(21))
    ));
    let applied_status = status(&repository, applied.id(), request_digest());
    let RuntimeAttemptStatusLookup::Found(applied_result) = &applied_status else {
        panic!("known-applied attempt must return a result: {applied_status:?}");
    };
    assert_eq!(applied_result.attempt_id(), applied.id());
    assert_eq!(applied_result.fencing_token(), applied.fencing_token());
    assert_eq!(
        applied_result.binding().caller_cell_identity().as_str(),
        "cell:workflow"
    );
    assert_eq!(applied_result.binding().request_digest(), request_digest());
    assert_eq!(
        applied_result.binding().launch_profile_digest(),
        PayloadDigest::of_bytes(b"launch profile")
    );
    assert_eq!(
        applied_result.binding().environment_binding_digest(),
        PayloadDigest::of_bytes(b"environment binding")
    );
    assert_eq!(
        applied_result.binding().input_digest(),
        PayloadDigest::of_bytes(b"input set")
    );
    assert!(matches!(
        applied_result.snapshot(),
        RuntimeAttemptStatusSnapshot::KnownApplied {
            result_digest: retained_result,
            observed_at,
            session,
        } if *retained_result == result_digest
            && *observed_at == TimestampMillis::new(22)
            && session.session_id().as_str() == "session-applied"
            && session.lifecycle() == LifecycleState::Starting
            && session.revision() == Revision::new(1)
            && session.lifecycle_revision() == Revision::new(1)
            && session.observed_at() == TimestampMillis::new(1)
    ));
    assert!(matches!(
        status(&repository, compensated.id(), request_digest()),
        RuntimeAttemptStatusLookup::Found(result)
            if matches!(result.snapshot(), RuntimeAttemptStatusSnapshot::Compensated {
                observed_at
            } if *observed_at == TimestampMillis::new(40))
    ));
    assert!(matches!(
        status(
            &repository,
            &OperationAttemptId::new("attempt-missing").expect("attempt ID"),
            request_digest(),
        ),
        RuntimeAttemptStatusLookup::NotFound
    ));

    assert_eq!(
        load_attempts(&repository, &tracked_attempts),
        attempts_before
    );
    assert_eq!(
        [
            load_session(&repository, "session-accepted"),
            load_session(&repository, "session-unknown"),
            load_session(&repository, "session-absent"),
            load_session(&repository, "session-applied"),
            load_session(&repository, "session-compensated"),
        ],
        sessions_before
    );
    assert_eq!(
        block_on(SessionOutbox::read_pending(&repository, 100))
            .expect("read outbox after status queries"),
        outbox_before
    );
    assert_eq!(
        block_on(repository.pressure()).expect("pressure after status queries"),
        pressure_before
    );

    drop(repository);
    let repository = open(&database_path);
    assert_eq!(
        status(&repository, applied.id(), request_digest()),
        applied_status
    );
}

#[test]
fn digest_and_attached_provenance_preflight_before_session_existence() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory.path().join("runtime-attempt-preflight.sqlite3");
    let repository = open(&database_path);

    let attached = attempt("attached-missing-session", "missing-session", 1);
    accept(&repository, &attached);
    assert!(matches!(
        status(
            &repository,
            attached.id(),
            PayloadDigest::of_bytes(b"wrong caller request"),
        ),
        RuntimeAttemptStatusLookup::UnverifiableProvenance
    ));
    assert!(matches!(
        status(&repository, attached.id(), request_digest()),
        RuntimeAttemptStatusLookup::InconsistentState
    ));

    let standalone = OperationAttempt::accepted(
        OperationAttemptId::new("standalone-attempt").expect("attempt ID"),
        Some(SessionId::new("missing-standalone-session").expect("session ID")),
        ProcessMutationKind::Spawn,
        FencingToken::new(1).expect("fencing token"),
        PayloadDigest::of_bytes(b"standalone payload"),
        TimestampMillis::new(10),
    );
    accept(&repository, &standalone);
    assert!(matches!(
        status(&repository, standalone.id(), request_digest()),
        RuntimeAttemptStatusLookup::UnverifiableProvenance
    ));
}

fn seed(
    repository: &SqliteSessionRepository,
    attempt_id: &str,
    session_id: &str,
    fence: u64,
) -> OperationAttempt {
    let session = session(session_id);
    let event = SessionOutboxEvent {
        kind: DomainEventKind::SessionCreated,
        session_id: session.id().clone(),
        revision: session.revision(),
        occurred_at: session.updated_at(),
    };
    assert_eq!(
        block_on(SessionRepository::insert(repository, &session, &event))
            .expect("insert status session"),
        InsertSessionOutcome::Inserted
    );
    let attempt = attempt(attempt_id, session_id, fence);
    accept(repository, &attempt);
    attempt
}

fn accept(repository: &SqliteSessionRepository, attempt: &OperationAttempt) {
    assert!(matches!(
        block_on(OperationAttemptStore::accept(repository, attempt)).expect("accept attempt"),
        OperationAttemptAcceptance::Accepted(accepted) if accepted.id() == attempt.id()
    ));
}

fn settle(
    repository: &SqliteSessionRepository,
    attempt: &OperationAttempt,
    certainty: EffectCertainty,
    result_digest: Option<PayloadDigest>,
    settled_at: i64,
) {
    assert!(matches!(
        block_on(OperationAttemptStore::settle(
            repository,
            &OperationAttemptSettlement {
                attempt_id: attempt.id().clone(),
                fencing_token: attempt.fencing_token(),
                certainty,
                result_digest,
                settled_at: TimestampMillis::new(settled_at),
            },
        ))
        .expect("settle attempt"),
        OperationAttemptSettlementOutcome::Settled(_)
    ));
}

fn compensate(
    repository: &SqliteSessionRepository,
    attempt: &OperationAttempt,
    superseding: &OperationAttempt,
) {
    let cleanup_digest = PayloadDigest::of_bytes(b"exact cleanup evidence");
    let preparation = OperationAttemptCompensationPreparation::new(
        attempt.id().clone(),
        attempt.fencing_token(),
        superseding.fencing_token(),
        ProcessIdentity::new(
            ProcessInstanceId::new("compensated-process").expect("process ID"),
            TimestampMillis::new(30),
        ),
        Revision::new(1),
        cleanup_digest,
        TimestampMillis::new(35),
    );
    assert!(matches!(
        block_on(OperationAttemptStore::prepare_compensation(
            repository,
            &preparation,
        ))
        .expect("prepare compensation"),
        OperationAttemptCompensationPreparationOutcome::Prepared(_)
    ));
    let compensation = OperationAttemptCompensation::new(
        attempt.id().clone(),
        attempt.fencing_token(),
        superseding.fencing_token(),
        cleanup_digest,
        TimestampMillis::new(40),
    );
    assert!(matches!(
        block_on(OperationAttemptStore::compensate(repository, &compensation))
            .expect("compensate attempt"),
        OperationAttemptCompensationOutcome::Compensated(_)
    ));
}

fn status(
    repository: &SqliteSessionRepository,
    attempt_id: &OperationAttemptId,
    request_digest: PayloadDigest,
) -> RuntimeAttemptStatusLookup {
    block_on(RuntimeAttemptStatusRepository::get_runtime_attempt_status(
        repository,
        attempt_id,
        request_digest,
    ))
    .expect("read runtime-attempt status")
}

fn load_attempts(
    repository: &SqliteSessionRepository,
    attempts: &[OperationAttempt],
) -> Vec<OperationAttempt> {
    attempts
        .iter()
        .map(|attempt| {
            block_on(OperationAttemptStore::load(repository, attempt.id()))
                .expect("load attempt")
                .expect("attempt exists")
        })
        .collect()
}

fn load_session(repository: &SqliteSessionRepository, session_id: &str) -> Session {
    block_on(SessionRepository::load(
        repository,
        &SessionId::new(session_id).expect("session ID"),
    ))
    .expect("load session")
    .expect("session exists")
}

fn attempt(id: &str, session_id: &str, fence: u64) -> OperationAttempt {
    OperationAttempt::accepted_with_dispatch(
        OperationAttemptId::new(id).expect("attempt ID"),
        Some(SessionId::new(session_id).expect("session ID")),
        ProcessMutationKind::Spawn,
        FencingToken::new(fence).expect("fencing token"),
        PayloadDigest::of_bytes(b"canonical start payload"),
        Some(RuntimeDispatchBinding::new(
            DispatchCellIdentity::new("cell:workflow").expect("cell identity"),
            request_digest(),
            PayloadDigest::of_bytes(b"launch profile"),
            PayloadDigest::of_bytes(b"environment binding"),
            PayloadDigest::of_bytes(b"input set"),
            runtime_policy(),
        )),
        TimestampMillis::new(10),
    )
}

fn request_digest() -> PayloadDigest {
    PayloadDigest::of_bytes(REQUEST_BYTES)
}

fn runtime_policy() -> RuntimeDispatchPolicy {
    RuntimeDispatchPolicy::new(
        RuntimeDispatchLimits::new(30_000, 1_024, 2_048, None, None).expect("limits"),
        RuntimeOutputContract::new(
            "runtime-attempt-status-test/0.1.0-beta",
            PayloadDigest::of_bytes(b"result schema"),
            Classification::Internal,
        )
        .expect("output contract"),
        RuntimeTelemetryPolicyRef::new("trust://telemetry/policies/runtime-attempt-status")
            .expect("telemetry policy"),
        undercurrent_domain::RuntimeRetentionPolicyRef::new(
            "trust://retention/policies/runtime-attempt-status",
        )
        .expect("retention policy"),
        vec![
            RuntimeCallbackRef::new(
                "runtime-callback://example/workflow/v0.1.0-beta/runtime-attempt-status",
            )
            .expect("callback"),
        ],
        TimestampMillis::new(10_000),
        TimestampMillis::new(20_000),
    )
    .expect("runtime dispatch policy")
}

fn session(id: &str) -> Session {
    let limits = execution_limits();
    let configuration = SessionConfiguration::new(
        LaunchSpec::new(
            CommandSpec::new("true", Vec::new(), limits).expect("command"),
            EnvironmentVariables::new(Vec::new(), limits).expect("environment"),
            None,
            None,
            TerminalSize::new(80, 24, 400, 200).expect("terminal"),
        ),
        ExecutionPolicy::new(
            RestartConfig::never(),
            StopPolicy::new(StopSignal::Terminate, Duration::from_secs(1)).expect("stop"),
            true,
            false,
        ),
        HealthRules::new(Vec::new(), limits).expect("health rules"),
        Some(ProfileName::new("runtime").expect("profile")),
        Labels::default(),
        Metadata::default(),
    );
    Session::create(
        SessionId::new(id).expect("session ID"),
        SessionName::new(format!("name-{id}")).expect("session name"),
        configuration,
        4,
        TimestampMillis::new(1),
    )
    .expect("session")
    .0
}

const fn execution_limits() -> ExecutionLimits {
    ExecutionLimits {
        max_arguments: 8,
        max_command_bytes: 1_024,
        max_environment_entries: 8,
        max_environment_bytes: 4_096,
        max_path_bytes: 1_024,
        max_health_rules: 8,
        max_health_rule_bytes: 1_024,
    }
}

fn open(path: &Path) -> SqliteSessionRepository {
    SqliteSessionRepository::open(
        SqliteRepositoryConfig::new(
            path,
            SqliteRepositoryLimits::new(100, 100, 1_048_576, 100, 32).expect("limits"),
            Duration::from_secs(1),
        )
        .expect("configuration"),
    )
    .expect("open repository")
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct ThreadWake(thread::Thread);
    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match Future::poll(future.as_mut(), &mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}
