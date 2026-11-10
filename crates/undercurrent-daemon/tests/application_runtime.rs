// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeSet, VecDeque};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use undercurrent_application::{
    ApplicationErrorCategory, Clock, DetectionProfileCatalog, DiagnosticEventName,
    ForegroundEvidenceCollector, ForegroundProcessObserver, HealthCheckExecutor,
    InsertSessionOutcome, ObservationDispatchLimits, ObservedOperationAttemptEffect,
    OperationAttemptAcceptance, OperationAttemptEffectObserver, OperationAttemptReconciliation,
    OperationAttemptReconciliationLimits, OperationAttemptSettlement,
    OperationAttemptSettlementOutcome, OperationAttemptStore, PortError, PortErrorKind, PortFuture,
    PortOperation, PreparedSessionCatalog, ProcessExitObservation, ProcessExitSource,
    ProcessInstance, ProcessRuntime, ProcessTermination, ProcessTruthSource, ProfileCatalogEntry,
    ProfilePatternEngine, ProfilePatternError, ProfileSummary, RecoverySessionBatch, RestartJob,
    RestartScheduler, RuntimeCancellationActor, RuntimeCancellationAdmission,
    RuntimeCancellationControl, RuntimeCancellationMode, RuntimeCancellationPropagationPolicy,
    RuntimeCancellationRepository, RuntimeCancellationState, RuntimeCancellationStatusLookup,
    RuntimeCancellationTarget, SessionOutboxEvent, SessionProjectionWriter, SessionRecoverySource,
    SessionRepository, StartupReconciliationLimits,
};
use undercurrent_daemon::{
    DaemonApplicationRuntime, DaemonApplicationRuntimeConfig, DaemonApplicationRuntimeStartError,
    DaemonBackgroundPorts, DaemonBackgroundWorkersCreateError, DaemonCorePorts,
    DaemonDiagnosticField, DaemonDiagnosticSection, DaemonDiagnosticSource, DaemonObservationPorts,
    DaemonProcessExitPorts, DaemonRestartPorts, DaemonWorkerIntervals, DaemonWorkerKind,
    DaemonWorkerPorts, ProcessExitInterrupt,
};
use undercurrent_domain::{
    CommandSpec, Confidence, DispatchCellIdentity, EffectCertainty, EnvironmentVariables,
    ExecutionLimits, ExecutionPolicy, FencingToken, ForegroundDecisionPolicy, ForegroundEvidence,
    ForegroundObservation, HealthCheckResult, HealthRules, InputSequence, Labels, LaunchSpec,
    LifecycleState, Metadata, OperationAttempt, OperationAttemptId, PayloadDigest, ProcessIdentity,
    ProcessInstanceId, ProcessMutationKind, ProcessOutcome, ProfileName, RestartConfig, Revision,
    RuntimeCallbackRef, RuntimeDispatchBinding, RuntimeDispatchLimits, RuntimeDispatchPolicy,
    RuntimeOutputContract, RuntimeTelemetryPolicyRef, Session, SessionConfiguration, SessionId,
    SessionName, StopPolicy, StopSignal, TerminalSize, TimestampMillis,
};
use undercurrent_runtime::{
    BoundedObservationQueue, BoundedRestartQueue, BoundedSessionEventJournal,
    BoundedTelemetryRecorder, InMemoryProcessDirectory, InMemorySessionRepository,
    KeyedSessionMutationCoordinator, SessionMutationCoordinatorLimits,
};
use undercurrent_store_sqlite::{
    SqliteRepositoryConfig, SqliteRepositoryLimits, SqliteSessionRepository,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(2);

#[test]
fn focused_runtime_config_rejects_invalid_primitive_worker_bounds() {
    let components = config_components();
    assert!(matches!(
        DaemonApplicationRuntimeConfig::new(
            components.outbox,
            0,
            components.observation,
            components.reconciliation,
            OperationAttemptReconciliationLimits::new(8, 4).expect("attempt limits"),
            components.foreground,
            8,
            components.intervals,
            256 * 1_024,
        ),
        Err(DaemonBackgroundWorkersCreateError::InvalidLimit {
            field: "restart_batch_size"
        })
    ));
    assert!(matches!(
        DaemonApplicationRuntimeConfig::new(
            components.outbox,
            4,
            components.observation,
            components.reconciliation,
            OperationAttemptReconciliationLimits::new(8, 4).expect("attempt limits"),
            components.foreground,
            8,
            DaemonWorkerIntervals {
                observation: Duration::ZERO,
                ..components.intervals
            },
            256 * 1_024,
        ),
        Err(DaemonBackgroundWorkersCreateError::InvalidLimit {
            field: "observation_interval"
        })
    ));
    assert!(matches!(
        DaemonApplicationRuntimeConfig::new(
            components.outbox,
            4,
            components.observation,
            components.reconciliation,
            OperationAttemptReconciliationLimits::new(8, 4).expect("attempt limits"),
            components.foreground,
            8,
            components.intervals,
            32 * 1_024,
        ),
        Err(DaemonBackgroundWorkersCreateError::InvalidLimit {
            field: "worker_thread_stack_bytes"
        })
    ));
}

#[test]
fn reconciled_runtime_owns_retries_diagnostics_and_shutdown() {
    let harness = Harness::new(None, Arc::new(PreparedProjection::default()));
    block_on(harness.restarts.schedule(&RestartJob {
        session_id: session_id("missing-restart"),
        expected_revision: Revision::new(1),
        attempt: 1,
        not_before: TimestampMillis::new(100),
    }))
    .expect("schedule missing restart");
    harness.process.push_exit(ProcessExitObservation {
        session_id: session_id("missing-exit"),
        process_id: ProcessInstanceId::new("process-1").expect("process ID"),
        outcome: ProcessOutcome::Exited(7),
        observed_at: TimestampMillis::new(100),
    });

    let runtime = DaemonApplicationRuntime::start(harness.ports.clone(), runtime_config())
        .expect("reconciled runtime");
    assert!(runtime.startup_report().ready());
    assert_eq!(runtime.startup_report().examined, 0);

    wait_until(|| {
        let pressure = runtime.worker_pressure();
        pressure.iter().any(|worker| {
            worker.kind == DaemonWorkerKind::Restart
                && worker.failures >= 1
                && worker.last_failure == Some(ApplicationErrorCategory::NotFound)
        }) && pressure.iter().any(|worker| {
            worker.kind == DaemonWorkerKind::ProcessExit
                && worker.failures >= 2
                && worker.last_failure == Some(ApplicationErrorCategory::NotFound)
        })
    });

    let pressure = runtime.worker_pressure();
    // This fixture supplies application ports only; disk reload is installed by daemon composition.
    let application_workers = DaemonWorkerKind::ALL
        .into_iter()
        .filter(|kind| *kind != DaemonWorkerKind::ProfileReload)
        .count();
    assert_eq!(pressure.len(), application_workers);
    assert_eq!(harness.restarts.len(), 1);
    assert_eq!(harness.process.receive_calls.load(Ordering::Acquire), 1);
    let rows = runtime
        .snapshot(&BTreeSet::from([
            DaemonDiagnosticSection::SupervisorQueues,
            DaemonDiagnosticSection::Persistence,
            DaemonDiagnosticSection::Reconciliation,
        ]))
        .expect("worker diagnostics");
    assert_eq!(rows.len(), application_workers + 2);
    assert!(
        rows.iter()
            .filter(|row| row.section == DaemonDiagnosticSection::SupervisorQueues)
            .all(|row| {
                row.fields.contains_key(&DaemonDiagnosticField::Count)
                    && row.fields.contains_key(&DaemonDiagnosticField::Failed)
            })
    );
    let startup = rows
        .iter()
        .find(|row| row.identity == "startup_reconciliation")
        .expect("startup diagnostic");
    assert_eq!(startup.section, DaemonDiagnosticSection::Reconciliation);
    assert_eq!(
        startup.fields.get(&DaemonDiagnosticField::Items),
        Some(&undercurrent_daemon::DaemonDiagnosticValue::Unsigned(0))
    );
    assert_eq!(
        startup.fields.get(&DaemonDiagnosticField::ScanCompleted),
        Some(&undercurrent_daemon::DaemonDiagnosticValue::Boolean(true))
    );

    runtime.shutdown();
    assert_eq!(harness.process.interrupts.load(Ordering::Acquire), 1);
    assert!(
        runtime
            .worker_pressure()
            .iter()
            .all(|worker| worker.stopped)
    );
    assert!(
        harness
            .telemetry
            .diagnostics()
            .iter()
            .any(|record| { record.event == DiagnosticEventName::StartupReconciliationCompleted })
    );
}

#[test]
fn fatal_reconciliation_stops_before_any_worker_or_exit_receive() {
    let recovery = Arc::new(FailingRecoverySource);
    let harness = Harness::new(Some(recovery), Arc::new(PreparedProjection::default()));

    let error = DaemonApplicationRuntime::start(harness.ports, runtime_config())
        .expect_err("fatal recovery must keep readiness closed");

    assert!(matches!(
        error,
        DaemonApplicationRuntimeStartError::Reconciliation(_)
    ));
    assert_eq!(error.category(), ApplicationErrorCategory::Unavailable);
    assert_eq!(
        error.readiness_report().expect("partial report").examined,
        0
    );
    assert!(!format!("{error}").contains("private-recovery"));
    assert!(!format!("{error:?}").contains("private-recovery"));
    assert_eq!(harness.process.receive_calls.load(Ordering::Acquire), 0);
    assert_eq!(harness.process.interrupts.load(Ordering::Acquire), 0);
}

#[test]
fn unresolved_session_keeps_readiness_closed_and_starts_no_workers() {
    let harness = Harness::new(None, Arc::new(PreparedProjection::default()));
    harness.process.fail_truth.store(true, Ordering::Release);
    let mut session = starting_session("unresolved-session");
    session
        .record_process_ownership(
            ProcessIdentity::new(
                ProcessInstanceId::new("unresolved-process").expect("process ID"),
                TimestampMillis::new(11),
            ),
            TimestampMillis::new(12),
        )
        .expect("record ownership")
        .expect("ownership change");
    let event = SessionOutboxEvent {
        kind: undercurrent_domain::DomainEventKind::ProcessOwnershipChanged,
        session_id: session.id().clone(),
        revision: session.revision(),
        occurred_at: session.updated_at(),
    };
    assert_eq!(
        block_on(harness.repository.insert(&session, &event)).expect("insert session"),
        InsertSessionOutcome::Inserted
    );

    let error = DaemonApplicationRuntime::start(harness.ports, runtime_config())
        .expect_err("unresolved recovery must keep readiness closed");

    assert!(matches!(
        error,
        DaemonApplicationRuntimeStartError::ReadinessIncomplete(_)
    ));
    assert_eq!(
        error.category(),
        ApplicationErrorCategory::FailedPrecondition
    );
    let report = error.readiness_report().expect("readiness report");
    assert!(report.scan_completed);
    assert_eq!(report.examined, 1);
    assert_eq!(report.unresolved, 1);
    assert_eq!(harness.process.receive_calls.load(Ordering::Acquire), 0);
    assert_eq!(harness.process.interrupts.load(Ordering::Acquire), 0);
}

#[test]
fn daemon_starts_with_in_flight_cancellation_and_status_stays_unknown_without_redrive() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory.path().join("in-flight-cancellation.sqlite3");
    let repository = SqliteSessionRepository::open(
        SqliteRepositoryConfig::new(
            &database_path,
            SqliteRepositoryLimits::new(32, 64, 1_048_576, 64, 16).expect("SQLite limits"),
            Duration::from_secs(1),
        )
        .expect("SQLite config"),
    )
    .expect("open fixture repository");
    let mut session = starting_session("in-flight-cancellation-session");
    let created = SessionOutboxEvent {
        kind: undercurrent_domain::DomainEventKind::SessionCreated,
        session_id: session.id().clone(),
        revision: session.revision(),
        occurred_at: session.updated_at(),
    };
    assert_eq!(
        block_on(SessionRepository::insert(&repository, &session, &created))
            .expect("insert cancellation session"),
        InsertSessionOutcome::Inserted
    );
    let initial_revision = session.revision();
    let ownership = session
        .record_process_ownership(
            ProcessIdentity::new(
                ProcessInstanceId::new("in-flight-cancellation-process").expect("process ID"),
                TimestampMillis::new(20),
            ),
            TimestampMillis::new(20),
        )
        .expect("record ownership")
        .expect("ownership changes");
    let running = session
        .transition_lifecycle(LifecycleState::Running, TimestampMillis::new(21))
        .expect("enter running")
        .expect("lifecycle changes");
    let events = [
        SessionOutboxEvent {
            kind: ownership.kind(),
            session_id: session.id().clone(),
            revision: ownership.revision(),
            occurred_at: TimestampMillis::new(20),
        },
        SessionOutboxEvent {
            kind: running.kind(),
            session_id: session.id().clone(),
            revision: running.revision(),
            occurred_at: TimestampMillis::new(21),
        },
    ];
    assert_eq!(
        block_on(SessionRepository::commit(
            &repository,
            &session,
            initial_revision,
            &events,
        ))
        .expect("commit running cancellation session"),
        undercurrent_application::CommitSessionOutcome::Committed
    );

    let target_attempt_id =
        OperationAttemptId::new("in-flight-cancellation-start").expect("attempt ID");
    let target_request_digest = PayloadDigest::of_bytes(b"in-flight caller start request");
    let dispatch_cell = DispatchCellIdentity::new("cell:workflow").expect("dispatch cell");
    let attempt = OperationAttempt::accepted_with_dispatch(
        target_attempt_id.clone(),
        Some(session.id().clone()),
        ProcessMutationKind::Spawn,
        FencingToken::new(7).expect("fence"),
        PayloadDigest::of_bytes(b"in-flight internal start request"),
        Some(RuntimeDispatchBinding::new(
            dispatch_cell.clone(),
            target_request_digest,
            PayloadDigest::of_bytes(b"launch profile"),
            PayloadDigest::of_bytes(b"environment binding"),
            PayloadDigest::of_bytes(b"input set"),
            cancellation_runtime_policy(),
        )),
        TimestampMillis::new(22),
    );
    assert!(matches!(
        block_on(OperationAttemptStore::accept(&repository, &attempt)).expect("accept start"),
        OperationAttemptAcceptance::Accepted(_)
    ));
    assert!(matches!(
        block_on(OperationAttemptStore::settle(
            &repository,
            &OperationAttemptSettlement {
                attempt_id: target_attempt_id.clone(),
                fencing_token: FencingToken::new(7).expect("fence"),
                certainty: EffectCertainty::KnownApplied,
                result_digest: Some(PayloadDigest::of_bytes(b"start result")),
                settled_at: TimestampMillis::new(23),
            },
        ))
        .expect("settle start"),
        OperationAttemptSettlementOutcome::Settled(_)
    ));
    let actor = RuntimeCancellationActor::new(
        "principal-1".to_owned(),
        Some("tenant_0198a5bd-2afd-7dd1-b6a3-1fb90c6f0a21".to_owned()),
        "transport-authority-1".to_owned(),
        "example/workflow".to_owned(),
        "tenant_0198a5bd-2afd-7dd1-b6a3-1fb90c6f0a21".to_owned(),
        "actor-context-1".to_owned(),
        "authority-1".to_owned(),
    )
    .expect("actor");
    let target = RuntimeCancellationTarget::new(
        target_attempt_id,
        target_request_digest,
        FencingToken::new(7).expect("fence"),
        dispatch_cell,
        session.id().clone(),
        session.revision(),
        session.lifecycle_revision(),
        "prv_0198a5bd-2b01-7ae9-9a0c-42532d56e7f7".to_owned(),
        1,
    )
    .expect("cancellation target");
    let control = RuntimeCancellationControl::new(
        OperationAttemptId::new("in-flight-cancellation-control").expect("control ID"),
        PayloadDigest::of_bytes(b"cancellation command"),
        PayloadDigest::of_bytes(b"cancellation product request"),
        "idempotency-in-flight-cancellation".to_owned(),
        RuntimeCancellationMode::Force,
        "restart crash-window regression".to_owned(),
        RuntimeCancellationPropagationPolicy::TargetOnly,
        TimestampMillis::new(1_000),
        actor.source_product().to_owned(),
        actor.tenant_id().to_owned(),
        actor.actor_context_ref().to_owned(),
        actor.authority_ref().to_owned(),
    )
    .expect("cancellation control");
    let command =
        undercurrent_application::CancelRuntimeAttemptCommand::new(target, control, actor.clone())
            .expect("cancellation command");
    assert!(matches!(
        block_on(repository.admit_runtime_cancellation(&command, TimestampMillis::new(30)))
            .expect("admit cancellation"),
        RuntimeCancellationAdmission::Admitted(_)
    ));
    assert_eq!(
        block_on(repository.mark_runtime_cancellation_effect_started(
            command.control().control_id(),
            command.control().command_digest(),
            TimestampMillis::new(31),
        ))
        .expect("mark effect started"),
        undercurrent_application::RuntimeCancellationEffectStart::Started
    );
    let query = undercurrent_application::GetRuntimeCancellationStatusQuery::new(
        command.control().control_id().clone(),
        command.control().command_digest(),
        command.target().attempt_id().clone(),
        command.target().request_digest(),
        command.target().session_id().clone(),
        command.target().expected_provider_id().to_owned(),
        command.target().expected_provider_generation(),
        actor,
    )
    .expect("status query");
    drop(repository);

    let repository = Arc::new(
        SqliteSessionRepository::open(
            SqliteRepositoryConfig::new(
                &database_path,
                SqliteRepositoryLimits::new(32, 64, 1_048_576, 64, 16).expect("SQLite limits"),
                Duration::from_secs(1),
            )
            .expect("SQLite config"),
        )
        .expect("reopen fixture repository"),
    );
    let clock = Arc::new(FixedClock(TimestampMillis::new(100)));
    let coordinator = Arc::new(KeyedSessionMutationCoordinator::new(
        SessionMutationCoordinatorLimits::new(32, 8, 16).expect("coordinator limits"),
    ));
    let process = Arc::new(TestProcessAdapter::default());
    process.running_truth.store(true, Ordering::Release);
    let processes = Arc::new(InMemoryProcessDirectory::default());
    let restarts = Arc::new(BoundedRestartQueue::new(32).expect("restart queue"));
    let observations = Arc::new(BoundedObservationQueue::new(64).expect("observation queue"));
    let events = Arc::new(BoundedSessionEventJournal::new(128, 32).expect("event journal"));
    let telemetry = Arc::new(BoundedTelemetryRecorder::new(512, 512).expect("telemetry limits"));
    let core = DaemonCorePorts::new(
        clock,
        coordinator,
        repository.clone(),
        process.clone(),
        processes,
        DaemonRestartPorts::new(restarts),
        Arc::new(PreparedProjection::default()),
        Arc::new(PreparedSessionCatalog::new()),
        telemetry,
        repository.clone(),
        Arc::new(NoopAttemptObserver),
        Arc::new(undercurrent_daemon::OperationAttemptReadinessGate::new()),
    );
    let workers = DaemonWorkerPorts::new(
        repository.clone(),
        events,
        DaemonObservationPorts::new(observations),
        Arc::new(NoopForegroundCollector),
        process.clone(),
        Arc::new(EmptyProfiles),
        Arc::new(NoopPatterns),
        Arc::new(NoopHealthExecutor),
        repository.clone(),
        process.clone(),
        DaemonProcessExitPorts::new(process.clone()),
    );
    let runtime = DaemonApplicationRuntime::start(
        Arc::new(DaemonBackgroundPorts::new(core, workers)),
        runtime_config(),
    )
    .expect("Unknown cancellation must not block daemon readiness");

    assert!(runtime.startup_report().ready());
    assert_eq!(runtime.startup_report().stopping_unknown_preserved, 1);
    assert_eq!(process.stop_calls.load(Ordering::Acquire), 0);
    assert_eq!(process.force_calls.load(Ordering::Acquire), 0);
    assert!(matches!(
        block_on(repository.get_runtime_cancellation_status(&query))
            .expect("read retained cancellation status"),
        RuntimeCancellationStatusLookup::Found(result)
            if matches!(result.state(), RuntimeCancellationState::Unknown(_))
    ));
    runtime.shutdown();
}

#[test]
fn recovery_retires_exited_input_behind_an_unresolved_page_and_reports_live_debt() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("input-recovery.sqlite3");
    let repository = Arc::new(
        SqliteSessionRepository::open(
            SqliteRepositoryConfig::new(
                &database_path,
                SqliteRepositoryLimits::new(32, 64, 1_048_576, 64, 16).unwrap(),
                Duration::from_secs(1),
            )
            .unwrap(),
        )
        .unwrap(),
    );
    let mut session = starting_session("exited-input");
    session
        .transition_lifecycle(LifecycleState::Failed, TimestampMillis::new(30))
        .unwrap();
    block_on(repository.insert(
        &session,
        &SessionOutboxEvent {
            kind: undercurrent_domain::DomainEventKind::LifecycleChanged,
            session_id: session.id().clone(),
            revision: session.revision(),
            occurred_at: session.updated_at(),
        },
    ))
    .unwrap();
    let mut retired_id = None;
    for index in 0..41 {
        let (id, target, kind) = if index == 40 {
            (
                "z-retire-input".to_owned(),
                session.id().clone(),
                ProcessMutationKind::WriteInput,
            )
        } else {
            (
                format!("a-unknown-{index:02}"),
                session_id("other"),
                ProcessMutationKind::Resize,
            )
        };
        let attempt = OperationAttempt::accepted(
            OperationAttemptId::new(id).unwrap(),
            Some(target),
            kind,
            FencingToken::new(index + 1).unwrap(),
            PayloadDigest::of_bytes(b"input-recovery"),
            TimestampMillis::new(20),
        );
        block_on(repository.accept(&attempt)).unwrap();
        block_on(repository.settle(&OperationAttemptSettlement {
            attempt_id: attempt.id().clone(),
            fencing_token: attempt.fencing_token(),
            certainty: EffectCertainty::Unknown,
            result_digest: None,
            settled_at: TimestampMillis::new(21),
        }))
        .unwrap();
        if index == 40 {
            retired_id = Some(attempt.id().clone());
        }
    }
    let clock = Arc::new(FixedClock(TimestampMillis::new(100)));
    let coordinator = Arc::new(KeyedSessionMutationCoordinator::new(
        SessionMutationCoordinatorLimits::new(32, 8, 16).expect("coordinator limits"),
    ));
    let process = Arc::new(TestProcessAdapter::default());
    process.running_truth.store(true, Ordering::Release);
    let processes = Arc::new(InMemoryProcessDirectory::default());
    let restarts = Arc::new(BoundedRestartQueue::new(32).expect("restart queue"));
    let observations = Arc::new(BoundedObservationQueue::new(64).expect("observation queue"));
    let events = Arc::new(BoundedSessionEventJournal::new(128, 32).expect("event journal"));
    let telemetry = Arc::new(BoundedTelemetryRecorder::new(512, 512).expect("telemetry limits"));
    let core = DaemonCorePorts::new(
        clock,
        coordinator,
        repository.clone(),
        process.clone(),
        processes,
        DaemonRestartPorts::new(restarts),
        Arc::new(PreparedProjection::default()),
        Arc::new(PreparedSessionCatalog::new()),
        telemetry,
        repository.clone(),
        Arc::new(QuiescentInputObserver),
        Arc::new(undercurrent_daemon::OperationAttemptReadinessGate::new()),
    );
    let workers = DaemonWorkerPorts::new(
        repository.clone(),
        events,
        DaemonObservationPorts::new(observations),
        Arc::new(NoopForegroundCollector),
        process.clone(),
        Arc::new(EmptyProfiles),
        Arc::new(NoopPatterns),
        Arc::new(NoopHealthExecutor),
        repository.clone(),
        process.clone(),
        DaemonProcessExitPorts::new(process.clone()),
    );
    let runtime = DaemonApplicationRuntime::start(
        Arc::new(DaemonBackgroundPorts::new(core, workers)),
        runtime_config(),
    )
    .expect("uncertain input permits daemon startup");

    let retired_id = retired_id.unwrap();
    wait_until(|| {
        let pending = block_on(repository.pending_reconciliation(
            &undercurrent_application::OperationAttemptScan::without_exclusions(64).unwrap(),
        ))
        .unwrap();
        pending.len() == 40 && pending.iter().all(|attempt| attempt.id() != &retired_id)
    });
    let retained = block_on(OperationAttemptStore::load(
        repository.as_ref(),
        &retired_id,
    ))
    .unwrap()
    .unwrap();
    assert!(matches!(
        retained.state(),
        undercurrent_domain::OperationAttemptState::Settled {
            certainty: EffectCertainty::Unknown,
            ..
        }
    ));
    let rows = runtime
        .snapshot(&BTreeSet::from([DaemonDiagnosticSection::Reconciliation]))
        .unwrap();
    assert!(rows.iter().any(|row| row.identity == "a-unknown-00"
        && row.fields.contains_key(&DaemonDiagnosticField::SessionId)));
    assert!(rows.iter().all(|row| row.identity != retired_id.as_str()));
    runtime.shutdown();
    drop(repository);
    undercurrent_store_sqlite::verify_sqlite_database(&database_path).unwrap();
}

struct QuiescentInputObserver;
impl OperationAttemptEffectObserver for QuiescentInputObserver {
    fn observe<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<ObservedOperationAttemptEffect, PortError>> {
        NoopAttemptObserver.observe(attempt)
    }
    fn observe_input_retirement<'a>(
        &'a self,
        _attempt: &'a OperationAttempt,
    ) -> PortFuture<
        'a,
        Result<Option<undercurrent_application::InputAttemptRetirementEvidence>, PortError>,
    > {
        Box::pin(async {
            Ok(Some(
                undercurrent_application::InputAttemptRetirementEvidence {
                    process_id: ProcessInstanceId::new("exited-process").unwrap(),
                    exited_at: TimestampMillis::new(30),
                },
            ))
        })
    }
    fn acknowledge_observation<'a>(
        &'a self,
        _attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async { Ok(()) })
    }
}

struct Harness {
    ports: Arc<DaemonBackgroundPorts>,
    repository: Arc<InMemorySessionRepository>,
    restarts: Arc<BoundedRestartQueue>,
    process: Arc<TestProcessAdapter>,
    telemetry: Arc<BoundedTelemetryRecorder>,
}

impl Harness {
    fn new(
        recovery: Option<Arc<dyn SessionRecoverySource>>,
        projection: Arc<dyn SessionProjectionWriter>,
    ) -> Self {
        let clock = Arc::new(FixedClock(TimestampMillis::new(100)));
        let repository =
            Arc::new(InMemorySessionRepository::new(32, 64).expect("in-memory repository limits"));
        let coordinator = Arc::new(KeyedSessionMutationCoordinator::new(
            SessionMutationCoordinatorLimits::new(32, 8, 16).expect("coordinator limits"),
        ));
        let process = Arc::new(TestProcessAdapter::default());
        let processes = Arc::new(InMemoryProcessDirectory::default());
        let restarts = Arc::new(BoundedRestartQueue::new(32).expect("restart queue"));
        let observations = Arc::new(BoundedObservationQueue::new(64).expect("observation queue"));
        let events = Arc::new(BoundedSessionEventJournal::new(128, 32).expect("event journal"));
        let telemetry =
            Arc::new(BoundedTelemetryRecorder::new(512, 512).expect("telemetry recorder limits"));
        let attempts = Arc::new(NoPendingAttempts);
        let recovery: Arc<dyn SessionRecoverySource> =
            recovery.unwrap_or_else(|| repository.clone());
        let core = DaemonCorePorts::new(
            clock,
            coordinator,
            repository.clone(),
            process.clone(),
            processes,
            DaemonRestartPorts::new(restarts.clone()),
            projection,
            Arc::new(PreparedSessionCatalog::new()),
            telemetry.clone(),
            attempts,
            Arc::new(NoopAttemptObserver),
            Arc::new(undercurrent_daemon::OperationAttemptReadinessGate::new()),
        );
        let workers = DaemonWorkerPorts::new(
            repository.clone(),
            events,
            DaemonObservationPorts::new(observations),
            Arc::new(NoopForegroundCollector),
            process.clone(),
            Arc::new(EmptyProfiles),
            Arc::new(NoopPatterns),
            Arc::new(NoopHealthExecutor),
            recovery,
            process.clone(),
            DaemonProcessExitPorts::new(process.clone()),
        );
        Self {
            ports: Arc::new(DaemonBackgroundPorts::new(core, workers)),
            repository,
            restarts,
            process,
            telemetry,
        }
    }
}

#[derive(Clone, Copy)]
struct ConfigComponents {
    outbox: undercurrent_application::OutboxDispatchLimits,
    observation: ObservationDispatchLimits,
    reconciliation: StartupReconciliationLimits,
    foreground: ForegroundDecisionPolicy,
    intervals: DaemonWorkerIntervals,
}

fn config_components() -> ConfigComponents {
    ConfigComponents {
        outbox: undercurrent_application::OutboxDispatchLimits::new(
            8,
            Duration::from_millis(1),
            Duration::from_millis(1),
        )
        .expect("outbox limits"),
        observation: ObservationDispatchLimits::new(
            8,
            8,
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_millis(50),
        )
        .expect("observation limits"),
        reconciliation: StartupReconciliationLimits::new(8, 8).expect("reconciliation limits"),
        foreground: ForegroundDecisionPolicy::new(
            8,
            Duration::from_secs(5),
            Confidence::from_basis_points(5_000).expect("confidence"),
        )
        .expect("foreground policy"),
        intervals: DaemonWorkerIntervals {
            outbox: Duration::from_millis(20),
            restart: Duration::from_secs(5),
            observation: Duration::from_millis(20),
            process_exit_error: Duration::from_millis(20),
        },
    }
}

fn runtime_config() -> DaemonApplicationRuntimeConfig {
    let components = config_components();
    DaemonApplicationRuntimeConfig::new(
        components.outbox,
        4,
        components.observation,
        components.reconciliation,
        OperationAttemptReconciliationLimits::new(8, 4).expect("attempt reconciliation limits"),
        components.foreground,
        8,
        components.intervals,
        256 * 1_024,
    )
    .expect("runtime config")
}

struct NoPendingAttempts;

impl OperationAttemptStore for NoPendingAttempts {
    fn accept_next<'a>(
        &'a self,
        _admission: &'a undercurrent_application::OperationAttemptAdmission,
    ) -> PortFuture<'a, Result<OperationAttemptAcceptance, PortError>> {
        Box::pin(async { Err(test_port_error(PortOperation::AcceptOperationAttempt, None)) })
    }

    fn accept<'a>(
        &'a self,
        _attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<OperationAttemptAcceptance, PortError>> {
        Box::pin(async { Err(test_port_error(PortOperation::AcceptOperationAttempt, None)) })
    }

    fn settle<'a>(
        &'a self,
        _settlement: &'a OperationAttemptSettlement,
    ) -> PortFuture<'a, Result<OperationAttemptSettlementOutcome, PortError>> {
        Box::pin(async { Err(test_port_error(PortOperation::SettleOperationAttempt, None)) })
    }

    fn reconcile<'a>(
        &'a self,
        _reconciliation: &'a OperationAttemptReconciliation,
    ) -> PortFuture<'a, Result<OperationAttemptSettlementOutcome, PortError>> {
        Box::pin(async {
            Err(test_port_error(
                PortOperation::ReconcileOperationAttempt,
                None,
            ))
        })
    }

    fn load<'a>(
        &'a self,
        _attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<Option<OperationAttempt>, PortError>> {
        Box::pin(async { Ok(None) })
    }

    fn pending_reconciliation<'a>(
        &'a self,
        _scan: &'a undercurrent_application::OperationAttemptScan,
    ) -> PortFuture<'a, Result<Vec<OperationAttempt>, PortError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

struct NoopAttemptObserver;

impl OperationAttemptEffectObserver for NoopAttemptObserver {
    fn observe<'a>(
        &'a self,
        _attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<ObservedOperationAttemptEffect, PortError>> {
        Box::pin(async {
            Ok(ObservedOperationAttemptEffect {
                certainty: undercurrent_domain::EffectCertainty::Unknown,
                result_digest: None,
            })
        })
    }

    fn acknowledge_observation<'a>(
        &'a self,
        _attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async { Ok(()) })
    }
}

fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + TEST_TIMEOUT;
    while !condition() {
        assert!(Instant::now() < deadline, "condition timed out");
        thread::sleep(Duration::from_millis(2));
    }
}

#[derive(Default)]
struct ExitState {
    closed: bool,
    exits: VecDeque<ProcessExitObservation>,
}

#[derive(Default)]
struct TestProcessAdapter {
    state: Mutex<ExitState>,
    changed: Condvar,
    receive_calls: AtomicUsize,
    interrupts: AtomicUsize,
    fail_truth: AtomicBool,
    running_truth: AtomicBool,
    stop_calls: AtomicUsize,
    force_calls: AtomicUsize,
}

impl TestProcessAdapter {
    fn push_exit(&self, observation: ProcessExitObservation) {
        self.state
            .lock()
            .expect("exit state")
            .exits
            .push_back(observation);
        self.changed.notify_all();
    }
}

impl ProcessRuntime for TestProcessAdapter {
    fn spawn<'a>(
        &'a self,
        session_id: &'a SessionId,
        _launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<ProcessInstance, PortError>> {
        Box::pin(async move {
            Err(test_port_error(
                PortOperation::SpawnProcess,
                Some(session_id),
            ))
        })
    }

    fn arm_exit<'a>(
        &'a self,
        session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            Err(test_port_error(
                PortOperation::ArmProcessExit,
                Some(session_id),
            ))
        })
    }

    fn stop<'a>(
        &'a self,
        session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
        _policy: StopPolicy,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        self.stop_calls.fetch_add(1, Ordering::AcqRel);
        Box::pin(async move {
            Err(test_port_error(
                PortOperation::StopProcess,
                Some(session_id),
            ))
        })
    }

    fn force_stop<'a>(
        &'a self,
        session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        self.force_calls.fetch_add(1, Ordering::AcqRel);
        Box::pin(async move {
            Err(test_port_error(
                PortOperation::ForceProcess,
                Some(session_id),
            ))
        })
    }

    fn write_input<'a>(
        &'a self,
        session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
        _input: &'a InputSequence,
        _policy: undercurrent_domain::InputDeliveryPolicy,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move { Err(test_port_error(PortOperation::WriteInput, Some(session_id))) })
    }

    fn resize<'a>(
        &'a self,
        session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
        _size: TerminalSize,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            Err(test_port_error(
                PortOperation::ResizeTerminal,
                Some(session_id),
            ))
        })
    }
}

impl ForegroundProcessObserver for TestProcessAdapter {
    fn observe_foreground<'a>(
        &'a self,
        session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
    ) -> PortFuture<
        'a,
        Result<undercurrent_application::ForegroundProcessGroupObservation, PortError>,
    > {
        Box::pin(async move {
            Err(test_port_error(
                PortOperation::ObserveForegroundProcessGroup,
                Some(session_id),
            ))
        })
    }
}

impl ProcessTruthSource for TestProcessAdapter {
    fn observe<'a>(
        &'a self,
        session_id: &'a SessionId,
        _process: &'a ProcessIdentity,
    ) -> PortFuture<'a, Result<undercurrent_domain::ProcessTruth, PortError>> {
        Box::pin(async move {
            if self.fail_truth.load(Ordering::Acquire) {
                Err(test_port_error(
                    PortOperation::ObserveProcessTruth,
                    Some(session_id),
                ))
            } else if self.running_truth.load(Ordering::Acquire) {
                Ok(undercurrent_domain::ProcessTruth::Running {
                    observed_at: TimestampMillis::new(100),
                })
            } else {
                Ok(undercurrent_domain::ProcessTruth::Missing {
                    observed_at: TimestampMillis::new(100),
                })
            }
        })
    }
}

impl ProcessExitSource for TestProcessAdapter {
    fn next_exit(&self) -> PortFuture<'_, Result<ProcessExitObservation, PortError>> {
        Box::pin(async move {
            self.receive_calls.fetch_add(1, Ordering::AcqRel);
            let mut state = self.state.lock().expect("exit state");
            while state.exits.is_empty() && !state.closed {
                state = self.changed.wait(state).expect("exit wait");
            }
            state
                .exits
                .pop_front()
                .ok_or_else(|| test_port_error(PortOperation::ReadProcessExit, None))
        })
    }
}

impl ProcessExitInterrupt for TestProcessAdapter {
    fn interrupt_process_exits(&self) {
        self.interrupts.fetch_add(1, Ordering::AcqRel);
        self.state.lock().expect("exit state").closed = true;
        self.changed.notify_all();
    }
}

struct NoopForegroundCollector;

struct EmptyProfiles;

impl DetectionProfileCatalog for EmptyProfiles {
    fn list(&self, _limit: usize) -> Result<Vec<ProfileSummary>, PortError> {
        Ok(Vec::new())
    }

    fn get(&self, _name: &ProfileName) -> Result<Option<ProfileCatalogEntry>, PortError> {
        Ok(None)
    }

    fn all(&self, _limit: usize) -> Result<Vec<ProfileCatalogEntry>, PortError> {
        Ok(Vec::new())
    }
}

struct NoopPatterns;

impl ProfilePatternEngine for NoopPatterns {
    fn validate(
        &self,
        _expression: &undercurrent_domain::PatternExpression,
    ) -> Result<(), ProfilePatternError> {
        Ok(())
    }

    fn is_match(
        &self,
        _expression: &undercurrent_domain::PatternExpression,
        _text: &str,
    ) -> Result<bool, ProfilePatternError> {
        Ok(false)
    }
}

impl ForegroundEvidenceCollector for NoopForegroundCollector {
    fn collect<'a>(
        &'a self,
        _session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
        _profile: Option<&'a ProfileName>,
        _current: &'a ForegroundObservation,
    ) -> PortFuture<'a, Result<Vec<ForegroundEvidence>, PortError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

struct NoopHealthExecutor;

impl HealthCheckExecutor for NoopHealthExecutor {
    fn run<'a>(
        &'a self,
        _session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
        _rules: &'a HealthRules,
    ) -> PortFuture<'a, Result<Vec<HealthCheckResult>, PortError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

struct FailingRecoverySource;

impl SessionRecoverySource for FailingRecoverySource {
    fn read_batch<'a>(
        &'a self,
        _after: Option<&'a SessionId>,
        _limit: usize,
    ) -> PortFuture<'a, Result<RecoverySessionBatch, PortError>> {
        Box::pin(async {
            Err(PortError::new(
                PortErrorKind::Unavailable,
                PortOperation::ReadRecoverySessions,
                None,
                PrivateFailure("private-recovery"),
            ))
        })
    }
}

#[derive(Default)]
struct PreparedProjection(undercurrent_application::PreparedReadModels);

impl SessionProjectionWriter for PreparedProjection {
    fn upsert_session(&self, session: &Session) -> Result<(), PortError> {
        self.0.upsert_session(session)
    }

    fn remove_summary(&self, session_id: &SessionId) -> Result<(), PortError> {
        self.0.remove_summary(session_id)
    }
}

fn test_port_error(operation: PortOperation, session_id: Option<&SessionId>) -> PortError {
    PortError::new(
        PortErrorKind::Unavailable,
        operation,
        session_id.cloned(),
        PrivateFailure("private-adapter"),
    )
}

#[derive(Debug)]
struct PrivateFailure(&'static str);

impl Display for PrivateFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for PrivateFailure {}

#[derive(Clone, Copy)]
struct FixedClock(TimestampMillis);

impl Clock for FixedClock {
    fn now(&self) -> TimestampMillis {
        self.0
    }
}

fn starting_session(id: &str) -> Session {
    Session::create(
        session_id(id),
        SessionName::new(format!("name-{id}")).expect("session name"),
        session_configuration(),
        8,
        TimestampMillis::new(10),
    )
    .expect("session")
    .0
}

fn session_configuration() -> SessionConfiguration {
    let limits = ExecutionLimits {
        max_arguments: 8,
        max_command_bytes: 4_096,
        max_environment_entries: 8,
        max_environment_bytes: 4_096,
        max_path_bytes: 1_024,
        max_health_rules: 4,
        max_health_rule_bytes: 1_024,
    };
    SessionConfiguration::new(
        LaunchSpec::new(
            CommandSpec::new("test", Vec::new(), limits).expect("command"),
            EnvironmentVariables::new(Vec::new(), limits).expect("environment"),
            None,
            None,
            TerminalSize::new(80, 24, 500, 200).expect("terminal size"),
        ),
        ExecutionPolicy::new(
            RestartConfig::never(),
            StopPolicy::new(StopSignal::Terminate, Duration::from_secs(1)).expect("stop policy"),
            true,
            false,
        ),
        HealthRules::new(Vec::new(), limits).expect("health rules"),
        None,
        Labels::default(),
        Metadata::default(),
    )
}

fn cancellation_runtime_policy() -> RuntimeDispatchPolicy {
    RuntimeDispatchPolicy::new(
        RuntimeDispatchLimits::new(30_000, 1_024, 2_048, None, None).expect("runtime limits"),
        RuntimeOutputContract::new(
            "runtime-cancellation-startup/0.1.0-beta",
            PayloadDigest::of_bytes(b"runtime cancellation startup result schema"),
            undercurrent_domain::Classification::Internal,
        )
        .expect("output contract"),
        RuntimeTelemetryPolicyRef::new("trust://telemetry/policies/runtime-cancellation-startup")
            .expect("telemetry policy"),
        undercurrent_domain::RuntimeRetentionPolicyRef::new(
            "trust://retention/policies/runtime-cancellation-startup",
        )
        .expect("retention policy"),
        vec![
            RuntimeCallbackRef::new(
                "runtime-callback://example/workflow/v0.1.0-beta/runtime-cancellation-startup",
            )
            .expect("callback"),
        ],
        TimestampMillis::new(10_000),
        TimestampMillis::new(20_000),
    )
    .expect("runtime dispatch policy")
}

fn session_id(value: &str) -> SessionId {
    SessionId::new(value).expect("session ID")
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
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
