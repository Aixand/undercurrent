// SPDX-License-Identifier: Apache-2.0

//! Exercise a disposable repository without a daemon or process activation.
//!
//! Run from the workspace root with:
//! `cargo run --locked -p undercurrent-store-sqlite --example repository_roundtrip`.

use std::error::Error;
use std::future::Future;
use std::path::Path;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use undercurrent_application::{
    CommitSessionOutcome, InsertSessionOutcome, OutboxAcknowledgeOutcome, SessionOutbox,
    SessionOutboxEvent, SessionRepository,
};
use undercurrent_domain::{
    CommandSpec, DomainChange, EnvironmentVariables, ExecutionLimits, ExecutionPolicy, HealthRules,
    Labels, LaunchSpec, Metadata, RestartConfig, Session, SessionConfiguration, SessionId,
    SessionName, StopPolicy, StopSignal, TerminalSize, TimestampMillis,
};
use undercurrent_store_sqlite::{
    SqliteRepositoryConfig, SqliteRepositoryLimits, SqliteSessionRepository,
    backup_sqlite_database, restore_sqlite_database, sqlite_schema_evidence,
    verify_sqlite_database,
};

fn main() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("source.sqlite3");
    let backup_path = directory.path().join("backup.sqlite3");
    let repository = SqliteSessionRepository::open(config(&path)?)?;
    let (mut session, created) = session()?;
    let original = session.clone();
    assert_eq!(
        block_on(repository.insert(&session, &event(&session, created)))?,
        InsertSessionOutcome::Inserted
    );

    let expected_revision = session.revision();
    let renamed = session
        .rename(SessionName::new("renamed")?, TimestampMillis::new(2))?
        .ok_or("rename should produce a domain change")?;
    assert_eq!(
        block_on(repository.commit(&session, expected_revision, &[event(&session, renamed)]))?,
        CommitSessionOutcome::Committed
    );
    drop(repository);

    let repository = SqliteSessionRepository::open(config(&path)?)?;
    assert_eq!(
        block_on(repository.load(session.id()))?,
        Some(session.clone())
    );
    let pending = block_on(repository.read_pending(8))?;
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].snapshot.as_ref(), Some(&original));
    assert_eq!(pending[1].snapshot.as_ref(), Some(&session));
    let pressure = block_on(repository.pressure())?;
    println!(
        "reopened: name={}, sessions={}, pending_events={}",
        session.name().as_str(),
        pressure.sessions,
        pressure.outbox_events
    );

    // Use the SQLite backup API while the source is open, including its WAL.
    let backup = backup_sqlite_database(&path, &backup_path)?;
    let identity = sqlite_schema_evidence(&backup_path)?;
    assert_eq!(backup.verification.schema_version, 1);
    assert_eq!(identity.len(), 1);
    println!(
        "verified backup: release={}, identities={}",
        identity[0].release_version,
        identity.len()
    );

    // In this isolated example, inspection is the only event consumer.
    // A daemon must publish and activate observations before acknowledging.
    let through = pending[1].sequence;
    assert_eq!(
        block_on(repository.acknowledge_through(through))?,
        OutboxAcknowledgeOutcome::Acknowledged {
            records: 2,
            through
        }
    );
    assert!(block_on(repository.read_pending(8))?.is_empty());
    drop(repository); // Restore requires all target handles to be closed.

    let restored = restore_sqlite_database(&backup_path, &path)?;
    assert!(restored.replaced_existing_database);
    assert_eq!(restored.source, restored.installed);
    let repository = SqliteSessionRepository::open(config(&path)?)?;
    assert_eq!(block_on(repository.load(session.id()))?, Some(session));
    assert_eq!(block_on(repository.read_pending(8))?.len(), 2);
    drop(repository);
    let verified = verify_sqlite_database(&path)?;
    assert_eq!(verified.session_count, 1);
    assert_eq!(verified.outbox_count, 2);
    assert_eq!(verified.next_outbox_sequence, 3);
    println!(
        "restored: sessions={}, pending_events={}, next_sequence={}",
        verified.session_count, verified.outbox_count, verified.next_outbox_sequence
    );

    directory.close()?;
    println!("temporary repository removed");
    Ok(())
}

fn config(path: &Path) -> Result<SqliteRepositoryConfig, Box<dyn Error>> {
    Ok(SqliteRepositoryConfig::new(
        path,
        SqliteRepositoryLimits::new(4, 8, 64 * 1024, 8, 4)?,
        Duration::from_secs(1),
    )?)
}

fn session() -> Result<(Session, DomainChange), Box<dyn Error>> {
    let limits = ExecutionLimits {
        max_arguments: 4,
        max_command_bytes: 128,
        max_environment_entries: 4,
        max_environment_bytes: 128,
        max_path_bytes: 128,
        max_health_rules: 4,
        max_health_rule_bytes: 128,
    };
    let configuration = SessionConfiguration::new(
        LaunchSpec::new(
            // This is stored configuration only; the example never spawns it.
            CommandSpec::new("example-program", Vec::new(), limits)?,
            EnvironmentVariables::new(Vec::new(), limits)?,
            None,
            None,
            TerminalSize::new(80, 24, 400, 200)?,
        ),
        ExecutionPolicy::new(
            RestartConfig::never(),
            StopPolicy::new(StopSignal::Terminate, Duration::from_secs(1))?,
            true,
            false,
        ),
        HealthRules::new(Vec::new(), limits)?,
        None,
        Labels::default(),
        Metadata::default(),
    );
    Ok(Session::create(
        SessionId::new("example-session")?,
        SessionName::new("initial")?,
        configuration,
        4,
        TimestampMillis::new(1),
    )?)
}

fn event(session: &Session, change: DomainChange) -> SessionOutboxEvent {
    SessionOutboxEvent {
        kind: change.kind(),
        session_id: session.id().clone(),
        revision: change.revision(),
        occurred_at: session.updated_at(),
    }
}

// Minimal thread-based executor for these worker-backed port futures. This is
// an example helper, not a replacement for application runtime composition.
struct ThreadWake(thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => thread::park(),
        }
    }
}
