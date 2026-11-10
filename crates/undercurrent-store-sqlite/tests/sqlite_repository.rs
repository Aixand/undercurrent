// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use rusqlite::{Connection, params};
use tempfile::TempDir;
use undercurrent_application::{
    CommitSessionOutcome, InsertSessionOutcome, MetricName, OutboxAcknowledgeOutcome,
    OutboxSequence, PortErrorKind, SessionOutbox, SessionOutboxEvent, SessionRecoverySource,
    SessionRepository,
};
use undercurrent_domain::{
    CommandSpec, Confidence, DomainEventKind, EnvironmentVariables, EvidenceDisposition,
    EvidencePriority, EvidenceSource, ExecutionLimits, ExecutionPolicy, ForegroundClaim,
    ForegroundDecisionPolicy, ForegroundEvidence, HealthCheckResult, HealthObservation, HealthRule,
    HealthRules, InteractionClaim, InteractionModality, InteractionState, Labels, LaunchSpec,
    LifecycleState, Metadata, ObservationSemanticId, ProcessIdentity, ProcessInstanceId,
    ProfileName, RestartConfig, RestartPolicy, SecretEnvironmentVariables, SecretReference,
    Session, SessionConfiguration, SessionId, SessionName, StopPolicy, StopSignal, TerminalSize,
    TimestampMillis, WorkItem, WorkItemId, WorkItemKey, WorkOwner, WorkProvenance,
    WorkRelationKind, WorkStatus, WorkTarget,
};
use undercurrent_store_sqlite::{
    SqliteRepositoryConfig, SqliteRepositoryConfigError, SqliteRepositoryLimits,
    SqliteRepositoryOpenError, SqliteSessionRepository,
};

#[derive(Default)]
struct TestTelemetry {
    metrics: std::sync::Mutex<Vec<undercurrent_application::MetricObservation>>,
    session_metrics: std::sync::Mutex<Vec<undercurrent_application::SessionMetricObservation>>,
}

impl TestTelemetry {
    fn metric_aggregates(&self) -> Vec<TestMetricAggregate> {
        let metrics = self
            .metrics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let session_metrics = self
            .session_metrics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut aggregates: Vec<TestMetricAggregate> = Vec::new();
        let mut record = |observation: &undercurrent_application::MetricObservation| {
            if let Some(aggregate) = aggregates
                .iter_mut()
                .find(|aggregate| aggregate.name == observation.name)
            {
                aggregate.total += observation.value;
            } else {
                aggregates.push(TestMetricAggregate {
                    name: observation.name,
                    total: observation.value,
                });
            }
        };
        for observation in metrics.iter() {
            record(observation);
        }
        for observation in session_metrics.iter() {
            record(&observation.metric());
        }
        aggregates
    }
}

impl undercurrent_application::Telemetry for TestTelemetry {
    fn emit_diagnostic(&self, _record: &undercurrent_application::DiagnosticRecord) {}

    fn record_metric(&self, observation: undercurrent_application::MetricObservation) {
        self.metrics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(observation);
    }

    fn record_session_metric(
        &self,
        observation: &undercurrent_application::SessionMetricObservation,
    ) {
        self.session_metrics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(observation.clone());
    }
}

struct TestMetricAggregate {
    name: undercurrent_application::MetricName,
    total: f64,
}

#[test]
fn sqlite_configuration_rejects_unbounded_or_missing_operational_limits() {
    assert!(matches!(
        SqliteRepositoryLimits::new(0, 1, 1, 1, 1),
        Err(SqliteRepositoryConfigError::ZeroLimit {
            field: "max_sessions"
        })
    ));
    let limits = SqliteRepositoryLimits::new(1, 1, 1, 1, 1).expect("limits");
    assert!(matches!(
        SqliteRepositoryConfig::new("", limits, Duration::from_secs(1)),
        Err(SqliteRepositoryConfigError::EmptyPath)
    ));
    assert!(matches!(
        SqliteRepositoryConfig::new("state.sqlite", limits, Duration::ZERO),
        Err(SqliteRepositoryConfigError::ZeroBusyTimeout)
    ));

    #[cfg(target_pointer_width = "64")]
    assert!(matches!(
        SqliteRepositoryLimits::new(usize::MAX, 1, 1, 1, 1),
        Err(SqliteRepositoryConfigError::LimitTooLarge {
            field: "max_sessions"
        })
    ));
}

#[test]
fn credential_classified_environment_is_rejected_before_sqlite_admission_and_store_remains_clean() {
    const CREDENTIAL_CANARY: &str = "private-token-sqlite-canary";
    let fixture = RepositoryFixture::new(2, 2);
    let repository = fixture.open();

    let error = EnvironmentVariables::new(
        [("API_TOKEN".to_owned(), CREDENTIAL_CANARY.to_owned())],
        execution_limits(),
    )
    .expect_err("credential-classified environment is rejected before persistence");
    assert!(!format!("{error}").contains(CREDENTIAL_CANARY));
    assert!(!format!("{error:?}").contains(CREDENTIAL_CANARY));
    let pressure = block_on(repository.pressure()).expect("empty repository pressure");
    assert_eq!(pressure.sessions, 0);
    assert_eq!(pressure.outbox_events, 0);

    let safe_session = simple_session("safe-session", "safe");
    let created = event(&safe_session, DomainEventKind::SessionCreated);
    assert_eq!(
        block_on(repository.insert(&safe_session, &created)).expect("safe insert"),
        InsertSessionOutcome::Inserted
    );
    drop(repository);

    let repository = fixture.open();
    let loaded = block_on(repository.load(safe_session.id()))
        .expect("reopen load")
        .expect("safe session remains durable");
    assert!(
        loaded
            .configuration()
            .launch()
            .environment()
            .iter()
            .next()
            .is_none()
    );
    assert!(!format!("{loaded:?}").contains(CREDENTIAL_CANARY));
    let pressure = block_on(repository.pressure()).expect("reopened repository pressure");
    assert_eq!(pressure.sessions, 1);
    assert_eq!(pressure.outbox_events, 1);
}

#[test]
fn sqlite_restart_retains_only_opaque_secret_references() {
    const OPAQUE_REFERENCE: &str = "passport-handle-0e7a";
    const RESOLVED_SECRET_CANARY: &str = "resolved-secret-must-never-be-durable-6d91";
    let fixture = RepositoryFixture::new(2, 2);
    let repository = fixture.open();
    let session = secret_session("secret-session", "secret", OPAQUE_REFERENCE);
    let created = event(&session, DomainEventKind::SessionCreated);

    assert_eq!(
        block_on(repository.insert(&session, &created)).expect("secret-reference insert"),
        InsertSessionOutcome::Inserted
    );
    drop(repository);

    let repository = fixture.open();
    let loaded = block_on(repository.load(session.id()))
        .expect("restart load")
        .expect("durable session");
    let bindings = loaded
        .configuration()
        .launch()
        .secret_environment()
        .iter()
        .map(|(name, reference)| (name, reference.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(bindings, [("SERVICE_CREDENTIAL", OPAQUE_REFERENCE)]);
    drop(repository);

    let durable_bytes = std::fs::read(&fixture.path).expect("durable SQLite bytes");
    assert!(
        durable_bytes
            .windows(OPAQUE_REFERENCE.len())
            .any(|window| window == OPAQUE_REFERENCE.as_bytes())
    );
    assert!(
        !durable_bytes
            .windows(RESOLVED_SECRET_CANARY.len())
            .any(|window| window == RESOLVED_SECRET_CANARY.as_bytes())
    );
}

#[test]
fn complete_session_and_outbox_survive_reopen_without_losing_private_state() {
    let fixture = RepositoryFixture::new(8, 16);
    let repository = fixture.open();
    let mut session = rich_session("session-1", "build");
    let created = event(&session, DomainEventKind::LifecycleChanged);

    assert_eq!(
        block_on(repository.insert(&session, &created)).expect("insert"),
        InsertSessionOutcome::Inserted
    );
    let pressure = block_on(repository.pressure()).expect("pressure");
    assert_eq!(pressure.sessions, 1);
    assert_eq!(pressure.queued_requests, 0);
    assert_eq!(pressure.request_queue_capacity, 32);
    drop(repository);

    let repository = fixture.open();
    let loaded = block_on(repository.load(session.id()))
        .expect("load")
        .expect("durable session");
    assert_eq!(loaded, session);
    assert_eq!(
        loaded
            .work_context()
            .items()
            .next()
            .expect("work item")
            .provenance()
            .as_str(),
        "example://workflow/tasks/compile"
    );
    assert_eq!(
        loaded
            .configuration()
            .launch()
            .environment()
            .iter()
            .collect::<Vec<_>>(),
        vec![("PRIVATE_SETTING", "do-not-log")]
    );
    assert_eq!(
        block_on(repository.find_by_name(session.name()))
            .expect("find by name")
            .expect("named session"),
        session
    );
    let pending = block_on(repository.read_pending(4)).expect("pending outbox");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].sequence, OutboxSequence::FIRST);
    assert_eq!(pending[0].event, created);

    let prior = session.revision();
    let change = session
        .rename(
            SessionName::new("renamed").expect("name"),
            TimestampMillis::new(50),
        )
        .expect("rename")
        .expect("changed");
    let renamed = SessionOutboxEvent {
        kind: change.kind(),
        session_id: session.id().clone(),
        revision: change.revision(),
        occurred_at: TimestampMillis::new(50),
    };
    assert_eq!(
        block_on(repository.commit(&session, prior, std::slice::from_ref(&renamed)))
            .expect("commit"),
        CommitSessionOutcome::Committed
    );
    assert_eq!(
        block_on(repository.acknowledge_through(OutboxSequence::FIRST)).expect("acknowledge"),
        OutboxAcknowledgeOutcome::Acknowledged {
            records: 1,
            through: OutboxSequence::FIRST,
        }
    );
    drop(repository);

    let repository = fixture.open();
    assert_eq!(
        block_on(repository.load(session.id()))
            .expect("reopen load")
            .expect("session"),
        session
    );
    let pending = block_on(repository.read_pending(4)).expect("remaining event");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].sequence.value(), 2);
    assert_eq!(pending[0].event, renamed);
    assert_eq!(
        block_on(repository.acknowledge_through(OutboxSequence::FIRST))
            .expect("idempotent acknowledgement"),
        OutboxAcknowledgeOutcome::AlreadyAcknowledged {
            through: OutboxSequence::FIRST,
        }
    );
}

#[test]
fn outbox_capacity_rejects_the_complete_session_transaction() {
    let fixture = RepositoryFixture::new(2, 1);
    let repository = fixture.open();
    let mut session = simple_session("session-1", "build");
    let created = event(&session, DomainEventKind::SessionCreated);
    assert_eq!(
        block_on(repository.insert(&session, &created)).expect("insert"),
        InsertSessionOutcome::Inserted
    );
    let durable_before = session.clone();
    let prior = session.revision();
    let change = session
        .rename(
            SessionName::new("renamed").expect("name"),
            TimestampMillis::new(20),
        )
        .expect("rename")
        .expect("changed");
    let renamed = SessionOutboxEvent {
        kind: change.kind(),
        session_id: session.id().clone(),
        revision: change.revision(),
        occurred_at: TimestampMillis::new(20),
    };

    let error = block_on(repository.commit(&session, prior, &[renamed]))
        .expect_err("full outbox rejects transaction");
    assert_eq!(error.kind(), PortErrorKind::CapacityExceeded);
    assert_eq!(
        error.effect_certainty(),
        None,
        "A later commit cannot prove whole-command absence"
    );
    assert_eq!(
        block_on(repository.load(session.id()))
            .expect("load")
            .expect("session"),
        durable_before
    );
    assert_eq!(
        block_on(repository.pressure())
            .expect("pressure")
            .outbox_events,
        1
    );
}

#[test]
fn encoded_record_limit_reports_capacity_without_partial_insert() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let limits = SqliteRepositoryLimits::new(2, 2, 1, 2, 2).expect("limits");
    let config = SqliteRepositoryConfig::new(
        directory.path().join("state.sqlite"),
        limits,
        Duration::from_secs(1),
    )
    .expect("configuration");
    let repository = SqliteSessionRepository::open(config).expect("repository");
    let session = simple_session("session-1", "build");

    let error =
        block_on(repository.insert(&session, &event(&session, DomainEventKind::SessionCreated)))
            .expect_err("record limit rejects insert");
    assert_eq!(error.kind(), PortErrorKind::CapacityExceeded);
    assert_eq!(
        error.effect_certainty(),
        Some(undercurrent_domain::EffectCertainty::KnownAbsent)
    );
    assert!(
        block_on(repository.load(session.id()))
            .expect("load")
            .is_none()
    );
    assert_eq!(
        block_on(repository.pressure())
            .expect("pressure")
            .outbox_events,
        0
    );
}

#[test]
fn initial_insert_capacity_proves_absence_without_creating_state() {
    for (sessions, outbox) in [(1, 4), (2, 1)] {
        let fixture = RepositoryFixture::new(sessions, outbox);
        let repository = fixture.open();
        let first = simple_session("session-first", "first");
        block_on(repository.insert(&first, &event(&first, DomainEventKind::SessionCreated)))
            .expect("first insert");
        let rejected = simple_session("session-rejected", "rejected");
        let error = block_on(repository.insert(
            &rejected,
            &event(&rejected, DomainEventKind::SessionCreated),
        ))
        .expect_err("session or outbox capacity must refuse insert");
        assert_eq!(error.kind(), PortErrorKind::CapacityExceeded);
        assert_eq!(
            error.operation(),
            undercurrent_application::PortOperation::InsertSession
        );
        assert_eq!(
            error.effect_certainty(),
            Some(undercurrent_domain::EffectCertainty::KnownAbsent)
        );
        assert!(
            error.attempt_id().is_none(),
            "Refusal must not invent an attempt identity"
        );
        assert!(block_on(repository.load(rejected.id())).unwrap().is_none());
        assert_eq!(block_on(repository.load(first.id())).unwrap(), Some(first));
        assert_eq!(block_on(repository.pressure()).unwrap().outbox_events, 1);
    }
}

#[test]
fn independent_repository_instances_enforce_the_same_optimistic_revision() {
    let fixture = RepositoryFixture::new(4, 8);
    let first = fixture.open();
    let second = fixture.open();
    let session = simple_session("session-1", "build");
    block_on(first.insert(&session, &event(&session, DomainEventKind::SessionCreated)))
        .expect("insert");
    let mut first_copy = block_on(first.load(session.id()))
        .expect("first load")
        .expect("session");
    let mut second_copy = block_on(second.load(session.id()))
        .expect("second load")
        .expect("session");
    let expected = first_copy.revision();
    let first_change = first_copy
        .rename(
            SessionName::new("first-name").expect("name"),
            TimestampMillis::new(20),
        )
        .expect("rename")
        .expect("change");
    let second_change = second_copy
        .rename(
            SessionName::new("second-name").expect("name"),
            TimestampMillis::new(20),
        )
        .expect("rename")
        .expect("change");
    let first_event = SessionOutboxEvent {
        kind: first_change.kind(),
        session_id: first_copy.id().clone(),
        revision: first_change.revision(),
        occurred_at: TimestampMillis::new(20),
    };
    let second_event = SessionOutboxEvent {
        kind: second_change.kind(),
        session_id: second_copy.id().clone(),
        revision: second_change.revision(),
        occurred_at: TimestampMillis::new(20),
    };

    assert_eq!(
        block_on(first.commit(&first_copy, expected, &[first_event])).expect("first commit"),
        CommitSessionOutcome::Committed
    );
    assert_eq!(
        block_on(second.commit(&second_copy, expected, &[second_event])).expect("second conflict"),
        CommitSessionOutcome::RevisionConflict {
            current_revision: first_copy.revision(),
        }
    );
    assert_eq!(
        block_on(second.load(session.id()))
            .expect("load winner")
            .expect("session")
            .name(),
        first_copy.name()
    );
}

#[test]
fn terminal_delete_and_removal_event_commit_atomically_across_reopen() {
    let fixture = RepositoryFixture::new(2, 8);
    let repository = fixture.open();
    let mut session = simple_session("session-1", "build");
    block_on(repository.insert(&session, &event(&session, DomainEventKind::SessionCreated)))
        .expect("insert");
    let active_revision = session.revision();
    let removal = SessionOutboxEvent {
        kind: DomainEventKind::SessionRemoved,
        session_id: session.id().clone(),
        revision: active_revision.next("test").expect("next revision"),
        occurred_at: TimestampMillis::new(2),
    };
    let error = block_on(repository.delete(session.id(), active_revision, &removal))
        .expect_err("active session cannot be deleted");
    assert_eq!(error.kind(), PortErrorKind::InvalidData);

    let expected = session.revision();
    let stopped = session
        .transition_lifecycle(LifecycleState::Stopped, TimestampMillis::new(3))
        .expect("stop")
        .expect("change");
    let stopped_event = SessionOutboxEvent {
        kind: stopped.kind(),
        session_id: session.id().clone(),
        revision: stopped.revision(),
        occurred_at: TimestampMillis::new(3),
    };
    assert_eq!(
        block_on(repository.commit(&session, expected, &[stopped_event])).expect("stop commit"),
        CommitSessionOutcome::Committed
    );
    let expected = session.revision();
    let removal_change = session
        .prepare_removal(TimestampMillis::new(4))
        .expect("removal change");
    let removal_event = SessionOutboxEvent {
        kind: removal_change.kind(),
        session_id: session.id().clone(),
        revision: removal_change.revision(),
        occurred_at: TimestampMillis::new(4),
    };
    assert_eq!(
        block_on(repository.delete(session.id(), expected, &removal_event)).expect("delete"),
        undercurrent_application::DeleteSessionOutcome::Deleted
    );
    drop(repository);

    let repository = fixture.open();
    assert!(
        block_on(repository.load(session.id()))
            .expect("load deleted")
            .is_none()
    );
    let pending = block_on(repository.read_pending(8)).expect("outbox");
    assert_eq!(pending.last().expect("removal event").event, removal_event);
}

#[test]
fn recovery_pages_three_hundred_sessions_without_hydrating_the_catalog_at_once() {
    let fixture = RepositoryFixture::new(300, 300);
    let repository = fixture.open();
    for index in 0..300 {
        let session = simple_session(&format!("session-{index:03}"), &format!("task-{index:03}"));
        let created = event(&session, DomainEventKind::SessionCreated);
        assert_eq!(
            block_on(repository.insert(&session, &created)).expect("insert"),
            InsertSessionOutcome::Inserted
        );
    }
    drop(repository);

    let repository = fixture.open();
    let mut after = None;
    let mut observed = Vec::new();
    loop {
        let page = block_on(repository.read_batch(after.as_ref(), 37)).expect("recovery page");
        assert!(page.sessions.len() <= 37);
        observed.extend(page.sessions.iter().map(|session| session.id().clone()));
        let Some(next) = page.next_after else {
            break;
        };
        after = Some(next);
    }
    assert_eq!(observed.len(), 300);
    assert!(observed.windows(2).all(|ids| ids[0] < ids[1]));
    assert!(matches!(
        block_on(repository.read_batch(None, 38)),
        Err(error) if error.kind() == PortErrorKind::InvalidData
    ));
}

#[test]
fn tampered_aggregate_index_is_rejected_as_invalid_data() {
    let fixture = RepositoryFixture::new(2, 2);
    let repository = fixture.open();
    let session = simple_session("session-1", "build");
    let created = event(&session, DomainEventKind::SessionCreated);
    block_on(repository.insert(&session, &created)).expect("insert");
    drop(repository);

    let connection = Connection::open(&fixture.path).expect("tamper connection");
    connection
        .execute(
            "UPDATE sessions SET name = ?1 WHERE id = ?2",
            params!["tampered", session.id().as_str()],
        )
        .expect("tamper name index");
    drop(connection);

    let repository = fixture.open();
    let error = block_on(repository.load(session.id())).expect_err("tampering is rejected");
    assert_eq!(error.kind(), PortErrorKind::InvalidData);
    assert_eq!(error.session_id(), Some(session.id()));
}

#[test]
fn oversized_corrupt_payload_is_rejected_before_codec_allocation() {
    let fixture = RepositoryFixture::new(2, 2);
    let repository = fixture.open();
    let session = simple_session("session-1", "build");
    block_on(repository.insert(&session, &event(&session, DomainEventKind::SessionCreated)))
        .expect("insert");
    drop(repository);

    let connection = Connection::open(&fixture.path).expect("tamper connection");
    connection
        .execute(
            "UPDATE sessions SET payload = zeroblob(?1) WHERE id = ?2",
            params![600 * 1_024, session.id().as_str()],
        )
        .expect("oversize payload");
    drop(connection);

    let repository = fixture.open();
    let error = block_on(repository.load(session.id())).expect_err("oversize is rejected");
    assert_eq!(error.kind(), PortErrorKind::InvalidData);
}

#[test]
fn unsupported_database_schema_is_rejected_during_open() {
    let fixture = RepositoryFixture::new(2, 2);
    drop(fixture.open());
    let connection = Connection::open(&fixture.path).expect("schema connection");
    connection
        .pragma_update(None, "user_version", 99)
        .expect("set unsupported version");
    drop(connection);

    let Err(SqliteRepositoryOpenError::Validate(error)) =
        SqliteSessionRepository::open(fixture.config())
    else {
        panic!("unsupported schema must fail validation");
    };
    assert_eq!(error.kind(), PortErrorKind::InvalidData);
}

#[test]
fn persistence_boundaries_report_checkpoint_and_recovery_results() {
    let fixture = RepositoryFixture::new(2, 2);
    let telemetry = Arc::new(TestTelemetry::default());
    let repository =
        SqliteSessionRepository::open_with_telemetry(fixture.config(), telemetry.clone())
            .expect("repository");
    let session = simple_session("session-1", "build");

    block_on(repository.insert(&session, &event(&session, DomainEventKind::SessionCreated)))
        .expect("insert");
    block_on(repository.read_batch(None, 1)).expect("recovery batch");

    let metrics = telemetry.metric_aggregates();
    assert!(metrics.iter().any(|metric| {
        metric.name == MetricName::PersistenceCheckpointSucceeded && metric.total == 1.0
    }));
    assert!(metrics.iter().any(|metric| {
        metric.name == MetricName::PersistenceRecoverySucceeded && metric.total == 2.0
    }));
}

#[test]
fn persistence_open_reports_schema_recovery_failure() {
    let fixture = RepositoryFixture::new(2, 2);
    drop(fixture.open());
    let connection = Connection::open(&fixture.path).expect("schema connection");
    connection
        .pragma_update(None, "user_version", 99)
        .expect("set unsupported version");
    drop(connection);
    let telemetry = Arc::new(TestTelemetry::default());

    assert!(matches!(
        SqliteSessionRepository::open_with_telemetry(fixture.config(), telemetry.clone()),
        Err(SqliteRepositoryOpenError::Validate(_))
    ));
    assert!(telemetry.metric_aggregates().iter().any(|metric| {
        metric.name == MetricName::PersistenceRecoveryFailed && metric.total == 1.0
    }));
}

#[test]
fn inconsistent_repository_counters_are_rejected_during_open() {
    let fixture = RepositoryFixture::new(2, 2);
    let repository = fixture.open();
    let session = simple_session("session-1", "build");
    block_on(repository.insert(&session, &event(&session, DomainEventKind::SessionCreated)))
        .expect("insert");
    drop(repository);
    let connection = Connection::open(&fixture.path).expect("counter connection");
    connection
        .execute("UPDATE repository_state SET session_count = 0", [])
        .expect("corrupt counter");
    drop(connection);

    assert!(matches!(
        SqliteSessionRepository::open(fixture.config()),
        Err(SqliteRepositoryOpenError::Validate(_))
    ));
}

#[cfg(unix)]
#[test]
fn database_file_permissions_exclude_group_and_other_access() {
    use std::os::unix::fs::PermissionsExt;

    let fixture = RepositoryFixture::new(1, 1);
    drop(fixture.open());
    let mode = std::fs::metadata(&fixture.path)
        .expect("database metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);
}

struct RepositoryFixture {
    _directory: TempDir,
    path: std::path::PathBuf,
    limits: SqliteRepositoryLimits,
}

impl RepositoryFixture {
    fn new(max_sessions: usize, max_outbox_events: usize) -> Self {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("state").join("undercurrent.sqlite");
        Self {
            _directory: directory,
            path,
            limits: SqliteRepositoryLimits::new(
                max_sessions,
                max_outbox_events,
                512 * 1_024,
                37,
                32,
            )
            .expect("limits"),
        }
    }

    fn config(&self) -> SqliteRepositoryConfig {
        SqliteRepositoryConfig::new(&self.path, self.limits, Duration::from_secs(1))
            .expect("configuration")
    }

    fn open(&self) -> SqliteSessionRepository {
        SqliteSessionRepository::open(self.config()).expect("open repository")
    }
}

fn rich_session(id: &str, name: &str) -> Session {
    let limits = execution_limits();
    let command = CommandSpec::new(
        "sh",
        vec!["-lc".to_owned(), "printf ready".to_owned()],
        limits,
    )
    .expect("command");
    let environment = EnvironmentVariables::new(
        [("PRIVATE_SETTING".to_owned(), "do-not-log".to_owned())],
        limits,
    )
    .expect("environment");
    let health_rules = HealthRules::new(
        [
            HealthRule::tcp_port(8080).expect("port rule"),
            HealthRule::output_contains("ready", limits.max_health_rule_bytes)
                .expect("output rule"),
            HealthRule::command(
                CommandSpec::new("true", Vec::new(), limits).expect("health command"),
            ),
        ],
        limits,
    )
    .expect("health rules");
    let configuration = SessionConfiguration::new(
        LaunchSpec::new(
            command,
            environment,
            None,
            None,
            TerminalSize::new(120, 40, 400, 200).expect("terminal size"),
        ),
        ExecutionPolicy::new(
            RestartConfig::new(
                RestartPolicy::OnFailure,
                4,
                Duration::from_millis(25),
                20_000,
                Duration::from_secs(3),
                Duration::from_secs(10),
            )
            .expect("restart"),
            StopPolicy::new(StopSignal::Interrupt, Duration::from_secs(2)).expect("stop policy"),
            true,
            false,
        ),
        health_rules,
        Some(ProfileName::new("shell").expect("profile")),
        Labels::new([("team".to_owned(), "platform".to_owned())]).expect("labels"),
        Metadata::new([("private_note".to_owned(), "retained".to_owned())]).expect("metadata"),
    );
    let (mut session, _) = Session::create(
        SessionId::new(id).expect("session ID"),
        SessionName::new(name).expect("session name"),
        configuration,
        8,
        TimestampMillis::new(1),
    )
    .expect("session");
    session
        .upsert_work_item(
            WorkItem::new_with_provenance(
                WorkItemKey::new(
                    WorkOwner::new("orchestrator").expect("owner"),
                    WorkItemId::new("compile").expect("work ID"),
                ),
                WorkRelationKind::DependsOn,
                "Compile release",
                Some("Wait for the build artifact".to_owned()),
                WorkStatus::Blocked,
                [
                    WorkTarget::Session(SessionId::new("dependency").expect("target")),
                    WorkTarget::Labels(
                        Labels::new([("kind".to_owned(), "build".to_owned())])
                            .expect("target labels"),
                    ),
                    WorkTarget::External {
                        system: WorkOwner::new("ci").expect("external system"),
                        id: WorkItemId::new("run-42").expect("external ID"),
                    },
                ],
                WorkProvenance::new("example://workflow/tasks/compile").expect("provenance"),
                Metadata::new([("ticket".to_owned(), "INT-42".to_owned())]).expect("work metadata"),
                TimestampMillis::new(2),
                TimestampMillis::new(2),
                Some(TimestampMillis::new(10_000)),
            )
            .expect("work item"),
            TimestampMillis::new(2),
        )
        .expect("work update");
    let decision = ForegroundDecisionPolicy::new(
        4,
        Duration::from_secs(30),
        Confidence::from_basis_points(5_000).expect("minimum confidence"),
    )
    .expect("foreground policy")
    .decide(
        vec![
            ForegroundEvidence::new(
                EvidenceSource::new("terminal").expect("source"),
                EvidencePriority::Terminal,
                EvidenceDisposition::Affirmative,
                ForegroundClaim::Interaction(
                    InteractionClaim::new(
                        InteractionState::Available,
                        Some(InteractionModality::Text),
                        Some(ObservationSemanticId::new("shell.command").expect("semantic")),
                    )
                    .expect("interaction claim"),
                ),
                Confidence::from_basis_points(9_000).expect("confidence"),
                TimestampMillis::new(3),
            )
            .expect("evidence"),
        ],
        TimestampMillis::new(3),
    )
    .expect("foreground decision");
    session
        .update_foreground_observation(decision)
        .expect("foreground update");
    let health = HealthObservation::evaluate(
        3,
        vec![
            HealthCheckResult::new(
                0,
                true,
                Duration::from_micros(250),
                Some("connected".to_owned()),
                128,
            )
            .expect("health result"),
            HealthCheckResult::new(1, true, Duration::from_millis(1), None, 128)
                .expect("health result"),
            HealthCheckResult::new(2, true, Duration::from_millis(2), None, 128)
                .expect("health result"),
        ],
        TimestampMillis::new(4),
    )
    .expect("health observation");
    session
        .update_health_observation(health, TimestampMillis::new(4))
        .expect("health update");
    session
        .record_process_ownership(
            ProcessIdentity::new(
                ProcessInstanceId::new("pid-42-start-4").expect("process ID"),
                TimestampMillis::new(5),
            ),
            TimestampMillis::new(5),
        )
        .expect("process ownership");
    session
        .transition_lifecycle(LifecycleState::Running, TimestampMillis::new(6))
        .expect("running");
    session
}

fn simple_session(id: &str, name: &str) -> Session {
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
        None,
        Labels::default(),
        Metadata::default(),
    );
    Session::create(
        SessionId::new(id).expect("session ID"),
        SessionName::new(name).expect("session name"),
        configuration,
        4,
        TimestampMillis::new(1),
    )
    .expect("session")
    .0
}

fn secret_session(id: &str, name: &str, reference: &str) -> Session {
    let limits = execution_limits();
    let ordinary = EnvironmentVariables::new(Vec::new(), limits).expect("environment");
    let secret_environment = SecretEnvironmentVariables::new(
        [(
            "SERVICE_CREDENTIAL".to_owned(),
            SecretReference::new(reference).expect("opaque reference"),
        )],
        &ordinary,
        limits,
    )
    .expect("secret environment");
    let configuration = SessionConfiguration::new(
        LaunchSpec::new(
            CommandSpec::new("true", Vec::new(), limits).expect("command"),
            ordinary,
            None,
            None,
            TerminalSize::new(80, 24, 400, 200).expect("terminal"),
        )
        .with_secret_environment(secret_environment, limits)
        .expect("secret launch"),
        ExecutionPolicy::new(
            RestartConfig::never(),
            StopPolicy::new(StopSignal::Terminate, Duration::from_secs(1)).expect("stop"),
            true,
            false,
        ),
        HealthRules::new(Vec::new(), limits).expect("health rules"),
        None,
        Labels::default(),
        Metadata::default(),
    );
    Session::create(
        SessionId::new(id).expect("session ID"),
        SessionName::new(name).expect("session name"),
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

fn event(session: &Session, kind: DomainEventKind) -> SessionOutboxEvent {
    SessionOutboxEvent {
        kind,
        session_id: session.id().clone(),
        revision: session.revision(),
        occurred_at: session.updated_at(),
    }
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

    let mut future = Box::pin(future);
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}

#[test]
fn runtime_event_snapshot_preserves_the_committed_revision_after_later_changes() {
    let fixture = RepositoryFixture::new(8, 16);
    let repository = fixture.open();
    let mut session = simple_session("runtime-event", "first-name");
    let original = session.clone();
    let created = event(&session, DomainEventKind::SessionCreated);
    block_on(repository.insert(&session, &created)).unwrap();
    let previous = session.revision();
    let change = session
        .rename(
            SessionName::new("second-name").unwrap(),
            TimestampMillis::new(50),
        )
        .unwrap()
        .unwrap();
    let changed = SessionOutboxEvent {
        kind: change.kind(),
        session_id: session.id().clone(),
        revision: change.revision(),
        occurred_at: TimestampMillis::new(50),
    };
    block_on(repository.commit(&session, previous, &[changed])).unwrap();
    drop(repository);
    let reopened = fixture.open();
    let events = block_on(reopened.read_pending(4)).unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].snapshot.as_ref(), Some(&original));
    assert_eq!(events[1].snapshot.as_ref(), Some(&session));
}

#[test]
fn semantic_deliveries_survive_restart_without_reauthorizing_unknown_io() {
    use undercurrent_application::{
        InputDeliveryDisposition as Disposition, InputDeliveryReservation as Reservation,
        InputDeliveryStore,
    };
    use undercurrent_domain::{
        DeliveryId, InputItem, InputObservationPolicy as Policy, InputSequence,
    };
    use undercurrent_store_sqlite::{
        SqliteInputDeliveryStore, backup_sqlite_database, verify_sqlite_database,
    };
    let fixture = RepositoryFixture::new(8, 16);
    let session = simple_session("delivery-session", "delivery-session");
    let input = InputSequence::new(
        vec![InputItem::Text("private-delivery-canary".into())],
        8,
        1024,
    )
    .unwrap();
    let changed = InputSequence::new(
        vec![InputItem::Bytes(b"private-delivery-canary".to_vec())],
        8,
        1024,
    )
    .unwrap();
    let first = DeliveryId::new("first").unwrap();
    let second = DeliveryId::new("second").unwrap();
    {
        let repository = fixture.open();
        block_on(repository.insert(&session, &event(&session, DomainEventKind::SessionCreated)))
            .unwrap();
        let store = SqliteInputDeliveryStore::new(repository, 2).unwrap();
        assert_eq!(
            block_on(store.reserve(session.id(), &first, &input, Policy::ForceActive)).unwrap(),
            Reservation::Reserved
        );
        // The process can disappear before writing, after writing, or before settlement.
        // Every window has the same retained reservation and must preserve uncertainty.
    }
    for _ in 0..3 {
        let store = SqliteInputDeliveryStore::new(fixture.open(), 2).unwrap();
        assert_eq!(
            block_on(store.reserve(session.id(), &first, &input, Policy::ForceActive)).unwrap(),
            Reservation::Indeterminate
        );
        assert_eq!(
            block_on(store.reserve(session.id(), &first, &changed, Policy::ForceActive)).unwrap(),
            Reservation::PayloadConflict
        );
        assert_eq!(
            block_on(store.reserve(session.id(), &first, &input, Policy::Preserve)).unwrap(),
            Reservation::PayloadConflict
        );
    }
    let backup = fixture.path.with_extension("backup");
    {
        let store = SqliteInputDeliveryStore::new(fixture.open(), 2).unwrap();
        let unknown = Disposition::Indeterminate {
            observed_at: TimestampMillis::new(10),
        };
        block_on(store.settle(session.id(), &first, unknown)).unwrap();
        block_on(store.settle(session.id(), &first, unknown)).unwrap();
        let delivered = Disposition::Delivered {
            delivered_at: TimestampMillis::new(20),
        };
        assert_eq!(
            block_on(store.settle(session.id(), &first, delivered))
                .unwrap_err()
                .kind(),
            PortErrorKind::Conflict
        );
        assert_eq!(
            block_on(store.reserve(session.id(), &second, &input, Policy::ForceActive)).unwrap(),
            Reservation::Reserved
        );
        block_on(store.settle(session.id(), &second, delivered)).unwrap();
        block_on(store.settle(session.id(), &second, delivered)).unwrap();
        let third = DeliveryId::new("third").unwrap();
        assert_eq!(
            block_on(store.reserve(session.id(), &third, &input, Policy::ForceActive))
                .unwrap_err()
                .kind(),
            PortErrorKind::CapacityExceeded
        );
        backup_sqlite_database(&fixture.path, &backup).unwrap();
    }
    verify_sqlite_database(&backup).unwrap();
    let config =
        SqliteRepositoryConfig::new(&backup, fixture.limits, Duration::from_secs(1)).unwrap();
    // A reduced capacity must keep old IDs replayable, including unresolved ones.
    let store =
        SqliteInputDeliveryStore::new(SqliteSessionRepository::open(config).unwrap(), 1).unwrap();
    assert_eq!(
        block_on(store.reserve(session.id(), &first, &input, Policy::ForceActive)).unwrap(),
        Reservation::Indeterminate
    );
    assert_eq!(
        block_on(store.reserve(session.id(), &second, &input, Policy::ForceActive)).unwrap(),
        Reservation::AlreadyDelivered {
            delivered_at: TimestampMillis::new(20)
        }
    );
    let connection = Connection::open(&backup).unwrap();
    let digests: Vec<Vec<u8>> = connection
        .prepare("SELECT input_digest FROM input_deliveries")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(digests.len(), 2);
    assert!(digests.iter().all(|digest| digest.len() == 32));
    assert!(
        !std::fs::read(&backup)
            .unwrap()
            .windows(b"private-delivery-canary".len())
            .any(|bytes| bytes == b"private-delivery-canary")
    );
    // A malformed on-disk digest is rejected without hydrating its full blob.
    connection
        .execute_batch("PRAGMA ignore_check_constraints=ON;")
        .unwrap();
    connection
        .execute(
            "UPDATE input_deliveries SET input_digest=zeroblob(1048576) WHERE delivery_id='first'",
            [],
        )
        .unwrap();
    assert_eq!(
        block_on(store.reserve(session.id(), &first, &input, Policy::ForceActive))
            .unwrap_err()
            .kind(),
        PortErrorKind::InvalidData
    );
}
