// SPDX-License-Identifier: Apache-2.0

#![cfg(unix)]

use std::fs;
use std::future::Future;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use undercurrent_application::{
    CommitSessionOutcome, InsertSessionOutcome, OperationAttemptAcceptance,
    OperationAttemptCompensationPreparation, OperationAttemptCompensationPreparationOutcome,
    OperationAttemptRepairRepository, OperationAttemptScan, OperationAttemptSettlement,
    OperationAttemptSettlementOutcome, OperationAttemptStore, SessionOutboxEvent,
    SessionRepository,
};
use undercurrent_daemon::{ConfigDiscovery, PathEnvironment};
use undercurrent_daemon::{DaemonConfigOverrides, DaemonProcess, load_daemon_config};
use undercurrent_domain::{
    CommandSpec, DispatchCellIdentity, EffectCertainty, EnvironmentVariables, ExecutionLimits,
    ExecutionPolicy, FencingToken, HealthRules, Labels, LaunchSpec, LifecycleState, Metadata,
    NoRestartReason, OperationAttempt, OperationAttemptId, OperationAttemptState, PayloadDigest,
    ProcessIdentity, ProcessInstanceId, ProcessMutationKind, RestartConfig, RestartStatus,
    RuntimeCallbackRef, RuntimeDispatchBinding, RuntimeDispatchLimits, RuntimeDispatchPolicy,
    RuntimeOutputContract, RuntimeTelemetryPolicyRef, Session, SessionConfiguration, SessionId,
    SessionName, StopPolicy, StopSignal, TerminalSize, TimestampMillis,
};
use undercurrent_store_sqlite::SqliteSessionRepository;

#[test]
fn abandoned_shutdowns_recover_after_restart_and_allow_session_cleanup() {
    let temporary = physical_tempdir().expect("temporary product home");
    let discovery = ConfigDiscovery::from_path_env(
        PathEnvironment::with_home_dir(temporary.path()).with_undercurrent_home(temporary.path()),
    );
    let config = load_daemon_config(&discovery, DaemonConfigOverrides::new()).expect("config");
    undercurrent_daemon::prepare_private_directory(&temporary.path().join("state"))
        .expect("private state directory");
    let repository = SqliteSessionRepository::open(config.sqlite().clone()).expect("fixture store");
    let session = persisted_running_session();
    block_on(repository.insert(
        &session,
        &SessionOutboxEvent {
            kind: undercurrent_domain::DomainEventKind::LifecycleChanged,
            session_id: session.id().clone(),
            revision: session.revision(),
            occurred_at: session.updated_at(),
        },
    ))
    .expect("persist session");

    // Reproduce both crash windows, then the user's successful stop/restart:
    // a newer shutdown must not prevent cancellation of either older attempt.
    let mut abandoned = Vec::new();
    for (index, state) in [
        None,
        Some(EffectCertainty::Unknown),
        Some(EffectCertainty::KnownApplied),
    ]
    .into_iter()
    .enumerate()
    {
        let attempt = OperationAttempt::accepted(
            OperationAttemptId::new(format!("old-shutdown-{index}")).unwrap(),
            None,
            ProcessMutationKind::Shutdown,
            FencingToken::new(index as u64 + 1).unwrap(),
            PayloadDigest::of_bytes(b"shutdown"),
            TimestampMillis::new(10),
        );
        block_on(repository.accept(&attempt)).expect("persist shutdown acceptance");
        if let Some(certainty) = state {
            block_on(
                repository.settle(&OperationAttemptSettlement {
                    attempt_id: attempt.id().clone(),
                    fencing_token: attempt.fencing_token(),
                    certainty,
                    result_digest: (certainty == EffectCertainty::KnownApplied)
                        .then(|| PayloadDigest::of_bytes(b"completed shutdown")),
                    settled_at: TimestampMillis::new(20),
                }),
            )
            .expect("persist shutdown result");
        }
        if state != Some(EffectCertainty::KnownApplied) {
            abandoned.push(attempt);
        }
    }
    drop(repository);

    let process = DaemonProcess::compose(config.clone()).expect("recover abandoned shutdowns");
    let repository = SqliteSessionRepository::open(config.sqlite().clone()).expect("reopen store");
    for attempt in &abandoned {
        assert!(matches!(
            block_on(OperationAttemptStore::load(&repository, attempt.id()))
                .unwrap()
                .unwrap()
                .state(),
            OperationAttemptState::Settled {
                certainty: EffectCertainty::KnownAbsent,
                ..
            }
        ));
    }
    assert_eq!(
        block_on(
            repository.unresolved_attempts(&OperationAttemptScan::without_exclusions(8).unwrap())
        )
        .unwrap()
        .total(),
        0
    );

    let control = process.control();
    let worker = thread::spawn(move || process.run());
    let profile =
        undercurrent_sdk::StandaloneClientProfile::standard("shutdown-recovery-test").unwrap();
    let local = undercurrent_sdk::LocalClientConfig::new(
        config.local_endpoint().path().to_path_buf(),
        profile.hello().clone(),
        profile.local(),
    )
    .unwrap();
    let client = undercurrent_sdk::ProductClient::connect(
        Arc::new(undercurrent_sdk::LocalClientConnector::new(local)),
        profile.product(),
        Arc::new(NoJitter),
    )
    .expect("connect to recovered daemon");
    let result = client.request(
        undercurrent_protocol::ProductRequest::RemoveSession(
            undercurrent_protocol::RemoveSessionRequest {
                mutation: undercurrent_protocol::MutationContext::current(
                    undercurrent_protocol::ProductOperation::RemoveSession,
                ),
                session_id: session.id().as_str().to_owned(),
                confirmation: undercurrent_protocol::RemovalConfirmationDto::Forced,
            },
        ),
        Duration::from_secs(5),
    );
    drop(client);
    control
        .begin_shutdown(Duration::from_secs(2))
        .expect("stop test daemon");
    worker.join().expect("daemon thread").expect("daemon stops");
    assert!(matches!(
        result.expect("cleanup must pass recovery gate"),
        undercurrent_protocol::ProductSuccess::RemoveSession(_)
    ));
    assert!(
        block_on(SessionRepository::load(&repository, session.id()))
            .unwrap()
            .is_none()
    );
}

struct NoJitter;

impl undercurrent_sdk::ClientRecoveryJitter for NoJitter {
    fn sample_basis_points(&self, _failed_attempt: u32, _maximum: u16) -> u16 {
        0
    }
}

#[test]
fn daemon_reconciles_persisted_running_intent_before_opening_readiness() {
    let temporary = physical_tempdir().expect("temporary directory");
    fs::set_permissions(temporary.path(), fs::Permissions::from_mode(0o700))
        .expect("private runtime directory");
    let database = temporary.path().join("undercurrent.db");
    let instance_lock = temporary.path().join("daemon.lock");
    let discovery = ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(
        temporary.path().to_path_buf(),
    ));
    let config = load_daemon_config(
        &discovery,
        DaemonConfigOverrides::new()
            .with_database_path(&database)
            .with_instance_lock_path(&instance_lock),
    )
    .expect("resolved daemon config");
    let repository =
        SqliteSessionRepository::open(config.sqlite().clone()).expect("open fixture repository");
    let session = persisted_running_session();
    let event = SessionOutboxEvent {
        kind: undercurrent_domain::DomainEventKind::LifecycleChanged,
        session_id: session.id().clone(),
        revision: session.revision(),
        occurred_at: session.updated_at(),
    };
    assert_eq!(
        block_on(repository.insert(&session, &event)).expect("insert running fixture"),
        InsertSessionOutcome::Inserted
    );
    drop(repository);

    let process = DaemonProcess::compose(config).expect("compose reconciled daemon");
    assert!(
        !instance_lock.exists(),
        "instance lock must be released and removed after shutdown"
    );
    let repository = SqliteSessionRepository::open(
        load_daemon_config(
            &discovery,
            DaemonConfigOverrides::new()
                .with_database_path(&database)
                .with_instance_lock_path(&instance_lock),
        )
        .expect("reopen config")
        .sqlite()
        .clone(),
    )
    .expect("reopen reconciled repository");
    let recovered = block_on(SessionRepository::load(&repository, session.id()))
        .expect("load reconciled session")
        .expect("persisted session");
    assert_eq!(recovered.lifecycle(), LifecycleState::Failed);
    assert_eq!(recovered.process(), None);
    assert_eq!(
        recovered.restart_status(),
        RestartStatus::NotScheduled {
            reason: NoRestartReason::PolicyDisabled,
        }
    );
    assert!(recovered.revision() > session.revision());
    drop(repository);

    process
        .control()
        .begin_shutdown(Duration::from_millis(100))
        .expect("request daemon shutdown");
    let report = process.run().expect("run reconciled daemon");
    assert_eq!(report.accepted, 0);
    assert!(!instance_lock.exists());
}

#[test]
fn daemon_reopens_cleanup_commit_window_and_terminalizes_exact_prepared_dispatch() {
    let temporary = physical_tempdir().expect("temporary directory");
    fs::set_permissions(temporary.path(), fs::Permissions::from_mode(0o700))
        .expect("private runtime directory");
    let database = temporary.path().join("undercurrent.db");
    let instance_lock = temporary.path().join("daemon.lock");
    let discovery = ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(
        temporary.path().to_path_buf(),
    ));
    let config = load_daemon_config(
        &discovery,
        DaemonConfigOverrides::new()
            .with_database_path(&database)
            .with_instance_lock_path(&instance_lock),
    )
    .expect("resolved daemon config");
    let repository =
        SqliteSessionRepository::open(config.sqlite().clone()).expect("open fixture repository");

    let mut low_session = persisted_running_session();
    let activated_revision = low_session.revision();
    let low_process = low_session.process().expect("running owner").clone();
    let running_event = SessionOutboxEvent {
        kind: undercurrent_domain::DomainEventKind::LifecycleChanged,
        session_id: low_session.id().clone(),
        revision: low_session.revision(),
        occurred_at: low_session.updated_at(),
    };
    assert_eq!(
        block_on(repository.insert(&low_session, &running_event)).expect("insert running fixture"),
        InsertSessionOutcome::Inserted
    );

    let cell = DispatchCellIdentity::new("cell:cleanup-recovery").expect("cell");
    let dispatch_binding = |input: &'static [u8]| {
        RuntimeDispatchBinding::new(
            cell.clone(),
            PayloadDigest::of_bytes(b"caller dispatch request"),
            PayloadDigest::of_bytes(b"launch"),
            PayloadDigest::of_bytes(b"environment"),
            PayloadDigest::of_bytes(input),
            runtime_policy(),
        )
    };
    let low_attempt_id = OperationAttemptId::new("cleanup:low:7").expect("low attempt");
    let high_attempt_id = OperationAttemptId::new("cleanup:high:8").expect("high attempt");
    let latest_attempt_id = OperationAttemptId::new("cleanup:latest:9").expect("latest attempt");
    let low_attempt = OperationAttempt::accepted_with_dispatch(
        low_attempt_id.clone(),
        Some(low_session.id().clone()),
        ProcessMutationKind::Spawn,
        FencingToken::new(7).expect("low fence"),
        PayloadDigest::of_bytes(b"low request"),
        Some(dispatch_binding(b"low input")),
        TimestampMillis::new(10),
    );
    let high_attempt = OperationAttempt::accepted_with_dispatch(
        high_attempt_id.clone(),
        Some(SessionId::new("high-live-session").expect("high session")),
        ProcessMutationKind::Spawn,
        FencingToken::new(8).expect("high fence"),
        PayloadDigest::of_bytes(b"high request"),
        Some(dispatch_binding(b"high input")),
        TimestampMillis::new(11),
    );
    assert!(matches!(
        block_on(repository.accept(&low_attempt)).expect("accept low"),
        OperationAttemptAcceptance::Accepted(_)
    ));
    assert!(matches!(
        block_on(repository.accept(&high_attempt)).expect("accept high"),
        OperationAttemptAcceptance::Accepted(_)
    ));
    assert!(matches!(
        block_on(repository.settle(&OperationAttemptSettlement {
            attempt_id: high_attempt_id.clone(),
            fencing_token: FencingToken::new(8).expect("high fence"),
            certainty: EffectCertainty::KnownApplied,
            result_digest: Some(PayloadDigest::of_bytes(b"high process result")),
            settled_at: TimestampMillis::new(12),
        }))
        .expect("settle high"),
        OperationAttemptSettlementOutcome::Settled(_)
    ));
    let cleanup_digest = PayloadDigest::of_bytes(b"exact cleanup evidence");
    assert!(matches!(
        block_on(
            repository.prepare_compensation(&OperationAttemptCompensationPreparation::new(
                low_attempt_id.clone(),
                FencingToken::new(7).expect("low fence"),
                FencingToken::new(8).expect("high fence"),
                low_process,
                activated_revision,
                cleanup_digest,
                TimestampMillis::new(13),
            ))
        )
        .expect("prepare exact cleanup"),
        OperationAttemptCompensationPreparationOutcome::Prepared(_)
    ));
    let latest_attempt = OperationAttempt::accepted_with_dispatch(
        latest_attempt_id.clone(),
        Some(SessionId::new("latest-live-session").expect("latest session")),
        ProcessMutationKind::Spawn,
        FencingToken::new(9).expect("latest fence"),
        PayloadDigest::of_bytes(b"latest request"),
        Some(dispatch_binding(b"latest input")),
        TimestampMillis::new(14),
    );
    assert!(matches!(
        block_on(repository.accept(&latest_attempt)).expect("accept latest"),
        OperationAttemptAcceptance::Accepted(_)
    ));
    assert!(matches!(
        block_on(repository.settle(&OperationAttemptSettlement {
            attempt_id: latest_attempt_id.clone(),
            fencing_token: FencingToken::new(9).expect("latest fence"),
            certainty: EffectCertainty::KnownApplied,
            result_digest: Some(PayloadDigest::of_bytes(b"latest process result")),
            settled_at: TimestampMillis::new(15),
        }))
        .expect("settle latest"),
        OperationAttemptSettlementOutcome::Settled(_)
    ));

    low_session
        .transition_lifecycle(LifecycleState::Failed, TimestampMillis::new(16))
        .expect("failed transition")
        .expect("cleanup changes lifecycle");
    let cleanup_event = SessionOutboxEvent {
        kind: undercurrent_domain::DomainEventKind::LifecycleChanged,
        session_id: low_session.id().clone(),
        revision: low_session.revision(),
        occurred_at: low_session.updated_at(),
    };
    assert!(matches!(
        block_on(repository.commit(&low_session, activated_revision, &[cleanup_event]))
            .expect("commit exact cleanup"),
        CommitSessionOutcome::Committed
    ));
    let unresolved = block_on(
        repository.unresolved_attempts(&OperationAttemptScan::without_exclusions(8).expect("scan")),
    )
    .expect("unresolved before crash");
    assert_eq!(unresolved.total(), 1);
    drop(repository);

    // Reopening at this point models process loss after cleanup committed but before the
    // terminal attempt audit. Startup must prove exact absence rather than guessing.
    let process = DaemonProcess::compose(config).expect("compose recovery daemon");
    let reopened = SqliteSessionRepository::open(
        load_daemon_config(
            &discovery,
            DaemonConfigOverrides::new()
                .with_database_path(&database)
                .with_instance_lock_path(&instance_lock),
        )
        .expect("reopen config")
        .sqlite()
        .clone(),
    )
    .expect("reopen recovered repository");
    let recovered_low = block_on(OperationAttemptStore::load(&reopened, &low_attempt_id))
        .expect("load low attempt")
        .expect("low attempt");
    let OperationAttemptState::Compensated {
        superseding_fencing_token,
        cleanup_digest: durable_digest,
        ..
    } = recovered_low.state()
    else {
        panic!("low attempt must be terminally compensated");
    };
    assert_eq!(
        *superseding_fencing_token,
        FencingToken::new(8).expect("high fence")
    );
    assert_eq!(*durable_digest, cleanup_digest);
    assert!(matches!(
        block_on(OperationAttemptStore::load(&reopened, &high_attempt_id))
            .expect("load high attempt")
            .expect("high attempt")
            .state(),
        OperationAttemptState::Settled {
            certainty: EffectCertainty::KnownApplied,
            ..
        }
    ));
    assert!(matches!(
        block_on(OperationAttemptStore::load(&reopened, &latest_attempt_id))
            .expect("load latest attempt")
            .expect("latest attempt")
            .state(),
        OperationAttemptState::Settled {
            certainty: EffectCertainty::KnownApplied,
            ..
        }
    ));
    assert_eq!(
        block_on(
            reopened
                .unresolved_attempts(&OperationAttemptScan::without_exclusions(8).expect("scan"),)
        )
        .expect("readiness after recovery")
        .total(),
        0
    );
    drop(reopened);

    process
        .control()
        .begin_shutdown(Duration::from_millis(100))
        .expect("request daemon shutdown");
    process.run().expect("run recovered daemon");
}

fn runtime_policy() -> RuntimeDispatchPolicy {
    RuntimeDispatchPolicy::new(
        RuntimeDispatchLimits::new(30_000, 1_024, 2_048, None, None).expect("limits"),
        RuntimeOutputContract::new(
            "startup-recovery/0.1.0-beta",
            PayloadDigest::of_bytes(b"startup recovery output schema"),
            serde_json::from_str("\"internal\"").expect("Internal classification"),
        )
        .expect("output contract"),
        RuntimeTelemetryPolicyRef::new("trust://telemetry/policies/startup-recovery")
            .expect("telemetry ref"),
        undercurrent_domain::RuntimeRetentionPolicyRef::new(
            "trust://retention/policies/startup-recovery",
        )
        .expect("retention ref"),
        vec![
            RuntimeCallbackRef::new(
                "runtime-callback://example/workflow/v0.1.0-beta/startup-recovery",
            )
            .expect("callback ref"),
        ],
        TimestampMillis::new(10_000),
        TimestampMillis::new(20_000),
    )
    .expect("runtime dispatch policy")
}

fn persisted_running_session() -> Session {
    let limits = execution_limits();
    let (mut session, _) = Session::create(
        SessionId::new("persisted-running").expect("session ID"),
        SessionName::new("persisted-running").expect("session name"),
        SessionConfiguration::new(
            LaunchSpec::new(
                CommandSpec::new("sh", vec!["-lc".to_owned(), "sleep 30".to_owned()], limits)
                    .expect("command"),
                EnvironmentVariables::default(),
                None,
                None,
                TerminalSize::new(80, 24, 400, 200).expect("terminal size"),
            ),
            ExecutionPolicy::new(
                RestartConfig::never(),
                StopPolicy::new(StopSignal::Terminate, Duration::from_secs(1))
                    .expect("stop policy"),
                true,
                false,
            ),
            HealthRules::default(),
            None,
            Labels::default(),
            Metadata::default(),
        ),
        16,
        TimestampMillis::new(1),
    )
    .expect("session");
    session
        .record_process_ownership(
            ProcessIdentity::new(
                ProcessInstanceId::new("prior-daemon-process").expect("process ID"),
                TimestampMillis::new(2),
            ),
            TimestampMillis::new(2),
        )
        .expect("process ownership");
    session
        .transition_lifecycle(LifecycleState::Running, TimestampMillis::new(3))
        .expect("running lifecycle");
    session
}

fn execution_limits() -> ExecutionLimits {
    ExecutionLimits {
        max_arguments: 8,
        max_command_bytes: 1_024,
        max_environment_entries: 8,
        max_environment_bytes: 2_048,
        max_path_bytes: 256,
        max_health_rules: 8,
        max_health_rule_bytes: 256,
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
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

struct ThreadWake(thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

// macOS's default temp directory can exceed AF_UNIX limits once the product's
// run paths are appended. Keep IPC fixtures short and free of symlink ancestors.
fn physical_tempdir() -> std::io::Result<tempfile::TempDir> {
    #[cfg(target_os = "macos")]
    let base = std::path::PathBuf::from("/tmp");
    #[cfg(not(target_os = "macos"))]
    let base = std::env::temp_dir();
    tempfile::tempdir_in(std::fs::canonicalize(base)?)
}
