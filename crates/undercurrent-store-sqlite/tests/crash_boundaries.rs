// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use undercurrent_application::{SessionOutbox, SessionOutboxEvent, SessionRepository};
use undercurrent_domain::{
    CommandSpec, DomainChange, DomainEventKind, EnvironmentVariables, ExecutionLimits,
    ExecutionPolicy, HealthRules, Labels, LaunchSpec, LifecycleState, Metadata, RestartConfig,
    RestartPolicy, Session, SessionConfiguration, SessionId, SessionName, StopPolicy, StopSignal,
    TerminalSize, TimestampMillis,
};
use undercurrent_store_sqlite::{
    SqliteCrashInjector, SqliteCrashPoint, SqliteRepositoryConfig, SqliteRepositoryLimits,
    SqliteSessionRepository,
};

#[test]
fn insert_commit_delete_and_ack_crash_points_roll_back_after_restart() {
    let directory = tempfile::tempdir().expect("temporary directory");

    for (index, point) in [
        SqliteCrashPoint::InsertAfterOutbox,
        SqliteCrashPoint::InsertAfterAggregate,
        SqliteCrashPoint::InsertAfterCounters,
        SqliteCrashPoint::InsertBeforeCommit,
    ]
    .into_iter()
    .enumerate()
    {
        let path = directory.path().join(format!("insert-{index}.sqlite3"));
        let repository = open_with_crash(&path, point);
        let (session, change) = session(&format!("insert-{index}"));
        let event = event(&session, change);
        assert!(block_on(SessionRepository::insert(&repository, &session, &event)).is_err());
        drop(repository);

        let repository = open(&path);
        assert!(
            block_on(SessionRepository::load(&repository, session.id()))
                .expect("load after insert crash")
                .is_none()
        );
        assert!(
            block_on(SessionOutbox::read_pending(&repository, 10))
                .expect("outbox after insert crash")
                .is_empty()
        );
        let pressure = block_on(repository.pressure()).expect("pressure after insert crash");
        assert_eq!((pressure.sessions, pressure.outbox_events), (0, 0));
    }

    for (index, point) in [
        SqliteCrashPoint::CommitAfterOutbox,
        SqliteCrashPoint::CommitAfterAggregate,
        SqliteCrashPoint::CommitAfterCounters,
        SqliteCrashPoint::CommitBeforeCommit,
    ]
    .into_iter()
    .enumerate()
    {
        let path = directory.path().join(format!("commit-{index}.sqlite3"));
        let repository = open(&path);
        let (mut session, change) = session(&format!("commit-{index}"));
        let initial_name = session.name().clone();
        let initial_revision = session.revision();
        block_on(SessionRepository::insert(
            &repository,
            &session,
            &event(&session, change),
        ))
        .expect("insert session");
        acknowledge_all(&repository);
        drop(repository);

        let change = session
            .rename(
                SessionName::new(format!("renamed-{index}")).expect("renamed session"),
                TimestampMillis::new(2),
            )
            .expect("rename")
            .expect("rename change");
        let repository = open_with_crash(&path, point);
        assert!(
            block_on(SessionRepository::commit(
                &repository,
                &session,
                initial_revision,
                &[event(&session, change)],
            ))
            .is_err()
        );
        drop(repository);

        let repository = open(&path);
        let recovered = block_on(SessionRepository::load(&repository, session.id()))
            .expect("load after commit crash")
            .expect("session retained");
        assert_eq!(recovered.name(), &initial_name);
        assert_eq!(recovered.revision(), initial_revision);
        assert!(
            block_on(SessionOutbox::read_pending(&repository, 10))
                .expect("outbox after commit crash")
                .is_empty()
        );
    }

    for (index, point) in [
        SqliteCrashPoint::DeleteAfterOutbox,
        SqliteCrashPoint::DeleteAfterAggregate,
        SqliteCrashPoint::DeleteAfterCounters,
        SqliteCrashPoint::DeleteBeforeCommit,
    ]
    .into_iter()
    .enumerate()
    {
        let path = directory.path().join(format!("delete-{index}.sqlite3"));
        let repository = open(&path);
        let (mut session, created) = session(&format!("delete-{index}"));
        block_on(SessionRepository::insert(
            &repository,
            &session,
            &event(&session, created),
        ))
        .expect("insert session");
        let expected_revision = session.revision();
        let stopped = session
            .transition_lifecycle(LifecycleState::Failed, TimestampMillis::new(2))
            .expect("terminal transition")
            .expect("terminal change");
        block_on(SessionRepository::commit(
            &repository,
            &session,
            expected_revision,
            &[event(&session, stopped)],
        ))
        .expect("commit terminal session");
        acknowledge_all(&repository);
        drop(repository);

        let repository = open_with_crash(&path, point);
        let removal = SessionOutboxEvent {
            kind: DomainEventKind::SessionRemoved,
            session_id: session.id().clone(),
            revision: session
                .revision()
                .next("test removal")
                .expect("removal revision"),
            occurred_at: TimestampMillis::new(3),
        };
        assert!(
            block_on(SessionRepository::delete(
                &repository,
                session.id(),
                session.revision(),
                &removal,
            ))
            .is_err()
        );
        drop(repository);

        let repository = open(&path);
        assert!(
            block_on(SessionRepository::load(&repository, session.id()))
                .expect("load after delete crash")
                .is_some()
        );
        assert!(
            block_on(SessionOutbox::read_pending(&repository, 10))
                .expect("outbox after delete crash")
                .is_empty()
        );
    }

    for (index, point) in [
        SqliteCrashPoint::AcknowledgeAfterDelete,
        SqliteCrashPoint::AcknowledgeAfterCounters,
        SqliteCrashPoint::AcknowledgeBeforeCommit,
    ]
    .into_iter()
    .enumerate()
    {
        let path = directory.path().join(format!("ack-{index}.sqlite3"));
        let repository = open(&path);
        let (session, change) = session(&format!("ack-{index}"));
        block_on(SessionRepository::insert(
            &repository,
            &session,
            &event(&session, change),
        ))
        .expect("insert session");
        let pending =
            block_on(SessionOutbox::read_pending(&repository, 10)).expect("pending outbox");
        let through = pending[0].sequence;
        drop(repository);

        let repository = open_with_crash(&path, point);
        assert!(block_on(SessionOutbox::acknowledge_through(&repository, through)).is_err());
        drop(repository);

        let repository = open(&path);
        let pending = block_on(SessionOutbox::read_pending(&repository, 10))
            .expect("outbox after acknowledgement crash");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].sequence, through);
        assert_eq!(
            block_on(repository.pressure())
                .expect("pressure after acknowledgement crash")
                .outbox_events,
            1
        );
    }
}

fn session(suffix: &str) -> (Session, DomainChange) {
    let limits = ExecutionLimits {
        max_arguments: 8,
        max_command_bytes: 1024,
        max_environment_entries: 8,
        max_environment_bytes: 1024,
        max_path_bytes: 1024,
        max_health_rules: 8,
        max_health_rule_bytes: 1024,
    };
    let command = CommandSpec::new("sh", Vec::new(), limits).expect("command");
    let launch = LaunchSpec::new(
        command,
        EnvironmentVariables::default(),
        None,
        None,
        TerminalSize::new(80, 24, 512, 512).expect("terminal size"),
    );
    let restart = RestartConfig::new(
        RestartPolicy::Never,
        0,
        Duration::from_secs(1),
        10_000,
        Duration::from_secs(1),
        Duration::from_secs(1),
    )
    .expect("restart configuration");
    let policy = ExecutionPolicy::new(
        restart,
        StopPolicy::new(StopSignal::Terminate, Duration::from_secs(1)).expect("stop policy"),
        false,
        false,
    );
    let configuration = SessionConfiguration::new(
        launch,
        policy,
        HealthRules::default(),
        None,
        Labels::default(),
        Metadata::default(),
    );
    Session::create(
        SessionId::new(format!("session-{suffix}")).expect("session ID"),
        SessionName::new(format!("name-{suffix}")).expect("session name"),
        configuration,
        8,
        TimestampMillis::new(1),
    )
    .expect("create session")
}

fn event(session: &Session, change: DomainChange) -> SessionOutboxEvent {
    SessionOutboxEvent {
        kind: change.kind(),
        session_id: session.id().clone(),
        revision: change.revision(),
        occurred_at: session.updated_at(),
    }
}

fn acknowledge_all(repository: &SqliteSessionRepository) {
    let pending = block_on(SessionOutbox::read_pending(repository, 10)).expect("pending outbox");
    let through = pending.last().expect("outbox record").sequence;
    block_on(SessionOutbox::acknowledge_through(repository, through)).expect("acknowledge outbox");
}

fn open(path: &Path) -> SqliteSessionRepository {
    SqliteSessionRepository::open(config(path)).expect("open repository")
}

fn open_with_crash(path: &Path, point: SqliteCrashPoint) -> SqliteSessionRepository {
    SqliteSessionRepository::open_with_crash_injector(config(path), Arc::new(CrashAt(point)))
        .expect("open crash-injected repository")
}

fn config(path: &Path) -> SqliteRepositoryConfig {
    SqliteRepositoryConfig::new(
        path,
        SqliteRepositoryLimits::new(100, 100, 1_048_576, 100, 32).expect("limits"),
        Duration::from_secs(1),
    )
    .expect("configuration")
}

#[derive(Debug)]
struct CrashAt(SqliteCrashPoint);

impl SqliteCrashInjector for CrashAt {
    fn should_crash(&self, point: SqliteCrashPoint) -> bool {
        self.0 == point
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct ThreadWake(std::thread::Thread);

    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::park(),
        }
    }
}
