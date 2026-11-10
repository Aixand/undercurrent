// SPDX-License-Identifier: Apache-2.0
use std::{
    future::Future,
    path::Path,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    thread,
    time::Duration,
};
use undercurrent_application::{
    DeleteSessionOutcome, InputAttemptRetirement, InputAttemptRetirementEvidence,
    OperationAttemptScan, OperationAttemptScope, OperationAttemptSettlement, OperationAttemptStore,
    SessionOutboxEvent, SessionRepository,
};
use undercurrent_domain::*;
use undercurrent_store_sqlite::{
    SqliteRepositoryConfig, SqliteRepositoryLimits, SqliteSessionRepository, verify_sqlite_database,
};

fn input(
    repository: &SqliteSessionRepository,
    id: &str,
    target: Option<&str>,
    kind: ProcessMutationKind,
) -> OperationAttempt {
    let attempt = OperationAttempt::accepted(
        OperationAttemptId::new(id).unwrap(),
        target.map(|id| SessionId::new(id).unwrap()),
        kind,
        FencingToken::new(1).unwrap(),
        PayloadDigest::of_bytes(id.as_bytes()),
        TimestampMillis::new(2),
    );
    block_on(repository.accept(&attempt)).unwrap();
    block_on(repository.settle(&OperationAttemptSettlement {
        attempt_id: attempt.id().clone(),
        fencing_token: attempt.fencing_token(),
        certainty: EffectCertainty::Unknown,
        result_digest: None,
        settled_at: TimestampMillis::new(3),
    }))
    .unwrap();
    block_on(OperationAttemptStore::load(repository, attempt.id()))
        .unwrap()
        .unwrap()
}

fn insert(repository: &SqliteSessionRepository, session: &Session) {
    block_on(repository.insert(
        session,
        &SessionOutboxEvent {
            kind: DomainEventKind::SessionCreated,
            session_id: session.id().clone(),
            revision: session.revision(),
            occurred_at: session.updated_at(),
        },
    ))
    .unwrap();
}

#[test]
fn retirement_preserves_unknown_and_survives_cleanup_restart_and_lost_ack() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("retirement.sqlite3");
    let repository = open(&path);
    let mut session = session("exited");
    session
        .transition_lifecycle(LifecycleState::Failed, TimestampMillis::new(4))
        .unwrap();
    insert(&repository, &session);
    let attempt = input(
        &repository,
        "unknown-input",
        Some("exited"),
        ProcessMutationKind::WriteInput,
    );
    let retirement = InputAttemptRetirement {
        attempt_id: attempt.id().clone(),
        fencing_token: attempt.fencing_token(),
        session_revision: session.revision(),
        evidence: InputAttemptRetirementEvidence {
            process_id: ProcessInstanceId::new("exited-process").unwrap(),
            exited_at: TimestampMillis::new(4),
        },
        retired_at: TimestampMillis::new(5),
    };
    let removal = session.prepare_removal(TimestampMillis::new(6)).unwrap();
    let event = SessionOutboxEvent {
        kind: DomainEventKind::SessionRemoved,
        session_id: session.id().clone(),
        revision: removal.revision(),
        occurred_at: TimestampMillis::new(6),
    };
    assert!(
        block_on(repository.delete(session.id(), session.revision(), &event)).is_err(),
        "unretired evidence blocks deletion"
    );
    let mut stale = retirement.clone();
    stale.session_revision = Revision::new(99);
    assert!(!block_on(repository.retire_input(&stale)).unwrap());
    stale = retirement.clone();
    stale.fencing_token = FencingToken::new(9).unwrap();
    assert!(!block_on(repository.retire_input(&stale)).unwrap());
    assert!(block_on(repository.retire_input(&retirement)).unwrap());
    assert!(
        block_on(
            repository
                .pending_reconciliation(&OperationAttemptScan::without_exclusions(1).unwrap())
        )
        .unwrap()
        .is_empty()
    );
    assert_eq!(
        block_on(OperationAttemptStore::load(&repository, attempt.id())).unwrap(),
        Some(attempt.clone())
    );
    assert_eq!(
        block_on(repository.delete(session.id(), session.revision(), &event)).unwrap(),
        DeleteSessionOutcome::Deleted
    );
    assert!(
        block_on(repository.accept(&attempt)).is_err(),
        "retirement must not enable replay"
    );
    drop(repository);
    let repository = open(&path);
    assert_eq!(
        block_on(repository.pending_input_retirement_acks(8)).unwrap(),
        vec![attempt.clone()]
    );
    block_on(repository.acknowledge_input_retirement(&attempt)).unwrap();
    drop(repository);
    let repository = open(&path);
    assert!(
        block_on(repository.pending_input_retirement_acks(8))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        block_on(OperationAttemptStore::load(&repository, attempt.id())).unwrap(),
        Some(attempt)
    );
    drop(repository);
    verify_sqlite_database(&path).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute("UPDATE input_attempt_retirements SET process_id = ''", [])
        .unwrap();
    drop(connection);
    assert!(verify_sqlite_database(&path).is_err());
    assert!(
        SqliteSessionRepository::open(
            SqliteRepositoryConfig::new(
                &path,
                SqliteRepositoryLimits::new(100, 100, 1_048_576, 100, 32).unwrap(),
                Duration::from_secs(1)
            )
            .unwrap()
        )
        .is_err()
    );
}

#[test]
fn active_session_cannot_retire_uncertain_input() {
    let dir = tempfile::tempdir().unwrap();
    let repository = open(&dir.path().join("active.sqlite3"));
    let session = session("active");
    insert(&repository, &session);
    let attempt = input(
        &repository,
        "active-input",
        Some("active"),
        ProcessMutationKind::WriteInput,
    );
    assert!(
        !block_on(repository.retire_input(&InputAttemptRetirement {
            attempt_id: attempt.id().clone(),
            fencing_token: attempt.fencing_token(),
            session_revision: session.revision(),
            evidence: InputAttemptRetirementEvidence {
                process_id: ProcessInstanceId::new("process").unwrap(),
                exited_at: TimestampMillis::new(4)
            },
            retired_at: TimestampMillis::new(5)
        }))
        .unwrap()
    );
}

#[test]
fn scope_filters_precede_page_limits_and_always_include_host_debt() {
    let dir = tempfile::tempdir().unwrap();
    let repository = open(&dir.path().join("scopes.sqlite3"));
    input(
        &repository,
        "a-unrelated",
        Some("other"),
        ProcessMutationKind::WriteInput,
    );
    let local = input(
        &repository,
        "b-local",
        Some("target"),
        ProcessMutationKind::WriteInput,
    );
    let scope = OperationAttemptScope::Session(SessionId::new("target").unwrap());
    let scan = OperationAttemptScan::without_exclusions(1)
        .unwrap()
        .with_scope(scope);
    assert_eq!(
        block_on(repository.pending_reconciliation(&scan)).unwrap(),
        vec![local]
    );
    assert!(
        block_on(repository.pending_reconciliation(&scan.clone().without_input_attempts()))
            .unwrap()
            .is_empty()
    );
    let host = input(&repository, "z-host", None, ProcessMutationKind::Shutdown);
    assert_eq!(
        block_on(repository.pending_reconciliation(&scan.without_input_attempts())).unwrap(),
        vec![host.clone()]
    );
    assert_eq!(
        block_on(
            repository.pending_reconciliation(
                &OperationAttemptScan::without_exclusions(1)
                    .unwrap()
                    .with_scope(OperationAttemptScope::Host)
            )
        )
        .unwrap(),
        vec![host]
    );
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
