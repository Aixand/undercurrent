// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use rusqlite::Connection;
use undercurrent_application::{
    CancelRuntimeAttemptCommand, CancelRuntimeAttemptError, CancelRuntimeAttemptService, Clock,
    CommitSessionOutcome, DeleteSessionOutcome, DiagnosticRecord,
    GetRuntimeCancellationStatusQuery, InsertSessionOutcome, MetricObservation,
    OperationAttemptAcceptance, OperationAttemptSettlement, OperationAttemptSettlementOutcome,
    OperationAttemptStore, PortError, PortErrorKind, PortFuture, PortOperation, ProcessInstance,
    ProcessRuntime, ProcessTermination, ProcessUnbindOutcome, RestartJob, RestartScheduler,
    RuntimeCancellationActor, RuntimeCancellationAdmission, RuntimeCancellationAuthorization,
    RuntimeCancellationAuthorizer, RuntimeCancellationControl, RuntimeCancellationEffectStart,
    RuntimeCancellationMode, RuntimeCancellationPropagationPolicy, RuntimeCancellationRecovery,
    RuntimeCancellationRepository, RuntimeCancellationSessionSnapshot,
    RuntimeCancellationSettlement, RuntimeCancellationState, RuntimeCancellationStatusLookup,
    SessionMetricObservation, SessionMutationCoordinator, SessionMutationLease, SessionOutbox,
    SessionOutboxEvent, SessionProcessDirectory, SessionProjectionWriter, SessionRecoverySource,
    SessionRepository, TerminationKind,
};
use undercurrent_domain::{
    CommandSpec, DispatchCellIdentity, DomainEventKind, EffectCertainty, EnvironmentVariables,
    ExecutionLimits, ExecutionPolicy, FencingToken, HealthRules, InputDeliveryPolicy,
    InputSequence, Labels, LaunchSpec, LifecycleState, Metadata, OperationAttempt,
    OperationAttemptId, PayloadDigest, ProcessIdentity, ProcessInstanceId, ProcessMutationKind,
    ProfileName, RestartConfig, Revision, RuntimeCallbackRef, RuntimeDispatchBinding,
    RuntimeDispatchLimits, RuntimeDispatchPolicy, RuntimeOutputContract, RuntimeTelemetryPolicyRef,
    Session, SessionConfiguration, SessionId, SessionName, StopPolicy, StopSignal, TerminalSize,
    TimestampMillis,
};
use undercurrent_foundation::Classification;
use undercurrent_store_sqlite::{
    SqliteMaintenanceError, SqliteRepositoryConfig, SqliteRepositoryLimits,
    SqliteSessionRepository, verify_sqlite_database,
};

#[test]
fn admission_is_atomic_exactly_replayable_and_stale_targets_are_known_absent() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory
        .path()
        .join("runtime-cancellation-admission.sqlite3");
    let repository = open(&database_path);
    let session = seed_running_attempt(&repository);
    let cancel_command = command("cancel-control-1", b"cancel-1", "operator stop", &session);

    let admitted =
        block_on(repository.admit_runtime_cancellation(&cancel_command, TimestampMillis::new(10)))
            .expect("admit cancellation");
    let RuntimeCancellationAdmission::Admitted(admitted) = admitted else {
        panic!("new command must be admitted")
    };
    assert!(matches!(
        admitted.state(),
        RuntimeCancellationState::AcceptedStopping(snapshot)
            if snapshot.lifecycle() == LifecycleState::Stopping
                && snapshot.revision() == Revision::new(4)
                && snapshot.lifecycle_revision() == Revision::new(4)
    ));
    let durable = load_session(&repository);
    assert_eq!(durable.lifecycle(), LifecycleState::Stopping);
    assert_eq!(durable.revision(), Revision::new(4));
    let outbox_after_admission = block_on(SessionOutbox::read_pending(&repository, 100))
        .expect("read outbox after admission");

    drop(repository);
    let repository = open(&database_path);
    assert!(matches!(
        block_on(repository.admit_runtime_cancellation(
            &cancel_command,
            TimestampMillis::new(1_001),
        ))
        .expect("exact replay after restart"),
        RuntimeCancellationAdmission::Replay(replay) if replay == admitted
    ));
    assert_eq!(
        block_on(SessionOutbox::read_pending(&repository, 100)).expect("read outbox after replay"),
        outbox_after_admission
    );

    let conflict = command(
        "cancel-control-1",
        b"changed-cancel",
        "forcefully changed",
        &session,
    );
    assert!(matches!(
        block_on(repository.admit_runtime_cancellation(&conflict, TimestampMillis::new(12),))
            .expect("classify identity conflict"),
        RuntimeCancellationAdmission::IdentityConflict
    ));
    let stale = command("cancel-control-2", b"cancel-2", "stale revision", &session);
    assert!(matches!(
        block_on(repository.admit_runtime_cancellation(&stale, TimestampMillis::new(13),))
            .expect("classify stale target"),
        RuntimeCancellationAdmission::TargetMismatch
    ));
    assert_eq!(
        block_on(SessionOutbox::read_pending(&repository, 100))
            .expect("read outbox after rejections"),
        outbox_after_admission
    );
}

#[test]
fn admission_accepts_the_complete_domain_session_identity_bound() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory
        .path()
        .join("runtime-cancellation-long-session.sqlite3");
    let repository = open(&database_path);
    let session = seed_running_attempt_with_session(&repository, session_with_id(&"s".repeat(256)));
    let command = command(
        "cancel-control-long-session",
        b"cancel-long-session",
        "bounded session identity",
        &session,
    );

    assert!(matches!(
        block_on(repository.admit_runtime_cancellation(&command, TimestampMillis::new(10)))
            .expect("admit cancellation at the domain session-ID bound"),
        RuntimeCancellationAdmission::Admitted(_)
    ));
    drop(repository);
    let repository = open(&database_path);
    assert!(matches!(
        status(&repository, &command),
        RuntimeCancellationStatusLookup::Found(_)
    ));
}

#[test]
fn exact_live_retry_after_session_read_failure_resumes_raw_accepted_control() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory
        .path()
        .join("runtime-cancellation-retry-read.sqlite3");
    let repository = open(&database_path);
    let session = seed_running_attempt(&repository);
    let command = command_with_mode(
        "cancel-control-retry-read",
        b"cancel-retry-read",
        "retry after session read failure",
        RuntimeCancellationMode::Force,
        &session,
    );
    let session_repository = FailFirstSessionRead::new(&repository);
    let runtime = SuccessfulStopRuntime::default();
    let processes = TestProcessDirectory::from_session(&session);
    let first_clock = FixedClock(TimestampMillis::new(100));
    let service = cancellation_service(
        &repository,
        &session_repository,
        &runtime,
        &processes,
        &first_clock,
    );

    assert!(matches!(
        block_on(service.execute(command.clone())),
        Err(CancelRuntimeAttemptError::SessionRead(_))
    ));
    assert_eq!(runtime.calls.load(Ordering::Acquire), 0);
    assert!(matches!(
        status(&repository, &command),
        RuntimeCancellationStatusLookup::Found(result)
            if matches!(result.state(), RuntimeCancellationState::AcceptedStopping(_))
    ));

    let retry_clock = FixedClock(TimestampMillis::new(2_000));
    let retry = cancellation_service(
        &repository,
        &session_repository,
        &runtime,
        &processes,
        &retry_clock,
    );
    let result = block_on(retry.execute(command.clone()))
        .expect("exact accepted replay resumes after the original deadline");
    assert!(matches!(
        result.state(),
        RuntimeCancellationState::AppliedTerminated(_)
    ));
    assert_eq!(runtime.calls.load(Ordering::Acquire), 1);
    assert!(matches!(
        status(&repository, &command),
        RuntimeCancellationStatusLookup::Found(result)
            if matches!(result.state(), RuntimeCancellationState::AppliedTerminated(_))
    ));
}

#[test]
fn exact_live_retry_after_marker_failure_performs_one_eventual_process_effect() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory
        .path()
        .join("runtime-cancellation-retry-marker.sqlite3");
    let repository = open(&database_path);
    let session = seed_running_attempt(&repository);
    let command = command_with_mode(
        "cancel-control-retry-marker",
        b"cancel-retry-marker",
        "retry after marker failure",
        RuntimeCancellationMode::Force,
        &session,
    );
    let cancellation_repository = FailFirstEffectMarker::new(&repository);
    let runtime = SuccessfulStopRuntime::default();
    let processes = TestProcessDirectory::from_session(&session);
    let first_clock = FixedClock(TimestampMillis::new(100));
    let service = cancellation_service(
        &cancellation_repository,
        &repository,
        &runtime,
        &processes,
        &first_clock,
    );

    assert!(matches!(
        block_on(service.execute(command.clone())),
        Err(CancelRuntimeAttemptError::EffectStart(_))
    ));
    assert_eq!(runtime.calls.load(Ordering::Acquire), 0);
    assert!(matches!(
        status(&repository, &command),
        RuntimeCancellationStatusLookup::Found(result)
            if matches!(result.state(), RuntimeCancellationState::AcceptedStopping(_))
    ));

    let retry_clock = FixedClock(TimestampMillis::new(2_000));
    let retry = cancellation_service(
        &cancellation_repository,
        &repository,
        &runtime,
        &processes,
        &retry_clock,
    );
    let result = block_on(retry.execute(command.clone()))
        .expect("exact accepted replay resumes after the original deadline");
    assert!(matches!(
        result.state(),
        RuntimeCancellationState::AppliedTerminated(_)
    ));
    assert_eq!(runtime.calls.load(Ordering::Acquire), 1);
    assert!(matches!(
        status(&repository, &command),
        RuntimeCancellationStatusLookup::Found(result)
            if matches!(result.state(), RuntimeCancellationState::AppliedTerminated(_))
    ));
}

#[test]
fn exact_live_retry_accepts_monotonic_non_lifecycle_revision_advance() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory
        .path()
        .join("runtime-cancellation-retry-after-rename.sqlite3");
    let repository = open(&database_path);
    let session = seed_running_attempt(&repository);
    let command = command_with_mode(
        "cancel-control-retry-after-rename",
        b"cancel-retry-after-rename",
        "retry after a concurrent non-lifecycle commit",
        RuntimeCancellationMode::Force,
        &session,
    );
    let cancellation_repository = FailFirstEffectMarker::new(&repository);
    let runtime = SuccessfulStopRuntime::default();
    let processes = TestProcessDirectory::from_session(&session);
    let first_clock = FixedClock(TimestampMillis::new(100));
    let service = cancellation_service(
        &cancellation_repository,
        &repository,
        &runtime,
        &processes,
        &first_clock,
    );

    assert!(matches!(
        block_on(service.execute(command.clone())),
        Err(CancelRuntimeAttemptError::EffectStart(_))
    ));
    assert_eq!(runtime.calls.load(Ordering::Acquire), 0);

    let mut stopping = load_session(&repository);
    let admitted_revision = stopping.revision();
    let admitted_lifecycle_revision = stopping.lifecycle_revision();
    let change = stopping
        .rename(
            SessionName::new("renamed-while-stopping").expect("renamed session"),
            TimestampMillis::new(101),
        )
        .expect("rename stopping session")
        .expect("name changes");
    let renamed = outbox_event(&stopping, change.kind(), change.revision());
    assert_eq!(
        block_on(SessionRepository::commit(
            &repository,
            &stopping,
            admitted_revision,
            &[renamed],
        ))
        .expect("commit non-lifecycle revision advance"),
        CommitSessionOutcome::Committed
    );
    assert!(stopping.revision() > admitted_revision);
    assert_eq!(stopping.lifecycle_revision(), admitted_lifecycle_revision);
    assert_eq!(stopping.lifecycle(), LifecycleState::Stopping);

    let retry_clock = FixedClock(TimestampMillis::new(2_000));
    let retry = cancellation_service(
        &cancellation_repository,
        &repository,
        &runtime,
        &processes,
        &retry_clock,
    );
    let result = block_on(retry.execute(command.clone()))
        .expect("exact replay resumes with monotonic stopping evidence after its deadline");
    assert!(matches!(
        result.state(),
        RuntimeCancellationState::AppliedTerminated(snapshot)
            if snapshot.revision() > admitted_revision
                && snapshot.lifecycle_revision() > admitted_lifecycle_revision
    ));
    assert_eq!(runtime.calls.load(Ordering::Acquire), 1);
    assert!(matches!(
        status(&repository, &command),
        RuntimeCancellationStatusLookup::Found(result)
            if matches!(result.state(), RuntimeCancellationState::AppliedTerminated(_))
    ));
}

#[test]
fn restart_terminal_commit_settles_accepted_control_and_later_clock_is_duplicate() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory
        .path()
        .join("runtime-cancellation-recovery.sqlite3");
    let repository = open(&database_path);
    let session = seed_running_attempt(&repository);
    let command = command_with_mode(
        "cancel-control-recovery",
        b"cancel-recovery",
        "recover stop",
        RuntimeCancellationMode::Force,
        &session,
    );
    assert!(matches!(
        block_on(repository.admit_runtime_cancellation(
            &command,
            TimestampMillis::new(10),
        ))
        .expect("admit cancellation"),
        RuntimeCancellationAdmission::Admitted(result)
            if matches!(result.state(), RuntimeCancellationState::AcceptedStopping(_))
    ));

    // Simulate process death after the atomic stopping/control commit and before
    // process IO. Startup recovery sees the durable Stopping session after reopen.
    drop(repository);
    let repository = open(&database_path);
    assert_eq!(
        block_on(SessionRecoverySource::claim_runtime_cancellation_recovery(
            &repository,
            command.target().session_id(),
            TimestampMillis::new(9),
        ))
        .expect("read retained cancellation recovery mode"),
        Some(RuntimeCancellationRecovery::Resume(
            RuntimeCancellationMode::Force
        ))
    );
    assert!(matches!(
        status(&repository, &command),
        RuntimeCancellationStatusLookup::Found(result)
            if matches!(result.state(), RuntimeCancellationState::Unknown(snapshot)
                if snapshot.observed_at() == TimestampMillis::new(11))
    ));
    let mut stopping = load_session(&repository);
    let expected_revision = stopping.revision();
    let change = stopping
        .transition_lifecycle(LifecycleState::Stopped, TimestampMillis::new(20))
        .expect("terminal transition")
        .expect("terminal lifecycle changes");
    let event = outbox_event(&stopping, change.kind(), change.revision());
    assert_eq!(
        block_on(SessionRepository::commit(
            &repository,
            &stopping,
            expected_revision,
            &[event],
        ))
        .expect("commit recovery result"),
        CommitSessionOutcome::Committed
    );

    let RuntimeCancellationStatusLookup::Found(settled) = status(&repository, &command) else {
        panic!("recovered control must remain status-queryable")
    };
    assert!(matches!(
        settled.state(),
        RuntimeCancellationState::AppliedTerminated(snapshot)
            if snapshot.lifecycle() == LifecycleState::Stopped
                && snapshot.revision() == stopping.revision()
                && snapshot.lifecycle_revision() == stopping.lifecycle_revision()
                && snapshot.observed_at() == TimestampMillis::new(20)
    ));

    let later_observation = RuntimeCancellationState::AppliedTerminated(
        RuntimeCancellationSessionSnapshot::new(
            stopping.id().clone(),
            stopping.lifecycle(),
            stopping.revision(),
            stopping.lifecycle_revision(),
            TimestampMillis::new(99),
        )
        .expect("later terminal snapshot"),
    );
    assert!(matches!(
        block_on(repository.settle_runtime_cancellation(
            command.control().control_id(),
            command.control().command_digest(),
            later_observation,
        ))
        .expect("settlement acknowledgement"),
        RuntimeCancellationSettlement::Duplicate(existing)
            if existing == settled
    ));
}

#[test]
fn in_flight_terminal_commit_atomically_settles_and_later_settlement_is_duplicate() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory
        .path()
        .join("runtime-cancellation-in-flight-terminal.sqlite3");
    let repository = open(&database_path);
    let session = seed_running_attempt(&repository);
    let command = command_with_mode(
        "cancel-control-in-flight-terminal",
        b"cancel-in-flight-terminal",
        "terminal after effect marker",
        RuntimeCancellationMode::Force,
        &session,
    );
    assert!(matches!(
        block_on(repository.admit_runtime_cancellation(&command, TimestampMillis::new(10),))
            .expect("admit cancellation"),
        RuntimeCancellationAdmission::Admitted(_)
    ));
    assert_eq!(
        block_on(repository.mark_runtime_cancellation_effect_started(
            command.control().control_id(),
            command.control().command_digest(),
            TimestampMillis::new(50),
        ))
        .expect("mark process effect started"),
        RuntimeCancellationEffectStart::Started
    );
    assert!(matches!(
        status(&repository, &command),
        RuntimeCancellationStatusLookup::Found(result)
            if matches!(result.state(), RuntimeCancellationState::Unknown(snapshot)
                if snapshot.observed_at() == TimestampMillis::new(50))
    ));

    let mut stopping = load_session(&repository);
    let expected_revision = stopping.revision();
    let change = stopping
        .transition_lifecycle(LifecycleState::Stopped, TimestampMillis::new(20))
        .expect("terminal transition")
        .expect("terminal lifecycle changes");
    let event = outbox_event(&stopping, change.kind(), change.revision());
    assert_eq!(
        block_on(SessionRepository::commit(
            &repository,
            &stopping,
            expected_revision,
            &[event],
        ))
        .expect("commit terminal session and cancellation"),
        CommitSessionOutcome::Committed
    );
    let RuntimeCancellationStatusLookup::Found(terminal) = status(&repository, &command) else {
        panic!("terminal control must remain status-queryable")
    };
    assert!(matches!(
        terminal.state(),
        RuntimeCancellationState::AppliedTerminated(snapshot)
            if snapshot.lifecycle() == LifecycleState::Stopped
                && snapshot.revision() == stopping.revision()
                && snapshot.lifecycle_revision() == stopping.lifecycle_revision()
                && snapshot.observed_at() == TimestampMillis::new(50)
    ));
    assert_eq!(
        block_on(SessionRecoverySource::claim_runtime_cancellation_recovery(
            &repository,
            command.target().session_id(),
            TimestampMillis::new(21),
        ))
        .expect("terminal control has no recovery instruction"),
        None
    );

    let later_terminal = RuntimeCancellationState::AppliedTerminated(
        RuntimeCancellationSessionSnapshot::new(
            stopping.id().clone(),
            stopping.lifecycle(),
            stopping.revision(),
            stopping.lifecycle_revision(),
            TimestampMillis::new(99),
        )
        .expect("later terminal snapshot"),
    );
    assert!(matches!(
        block_on(repository.settle_runtime_cancellation(
            command.control().control_id(),
            command.control().command_digest(),
            later_terminal,
        ))
        .expect("acknowledge terminal settlement"),
        RuntimeCancellationSettlement::Duplicate(existing) if existing == terminal
    ));
}

#[test]
fn unresolved_in_flight_force_restarts_as_unknown_and_is_never_redriven() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory
        .path()
        .join("runtime-cancellation-in-flight-unknown.sqlite3");
    let repository = open(&database_path);
    let session = seed_running_attempt(&repository);
    let command = command_with_mode(
        "cancel-control-in-flight-unknown",
        b"cancel-in-flight-unknown",
        "ambiguous force effect",
        RuntimeCancellationMode::Force,
        &session,
    );
    assert!(matches!(
        block_on(repository.admit_runtime_cancellation(&command, TimestampMillis::new(10),))
            .expect("admit cancellation"),
        RuntimeCancellationAdmission::Admitted(_)
    ));
    assert_eq!(
        block_on(repository.mark_runtime_cancellation_effect_started(
            command.control().control_id(),
            command.control().command_digest(),
            TimestampMillis::new(11),
        ))
        .expect("mark process effect started"),
        RuntimeCancellationEffectStart::Started
    );
    drop(repository);

    let repository = open(&database_path);
    assert_eq!(
        block_on(SessionRecoverySource::claim_runtime_cancellation_recovery(
            &repository,
            command.target().session_id(),
            TimestampMillis::new(12),
        ))
        .expect("read in-flight recovery instruction"),
        Some(RuntimeCancellationRecovery::PreserveUnknown)
    );
    assert!(matches!(
        status(&repository, &command),
        RuntimeCancellationStatusLookup::Found(result)
            if matches!(result.state(), RuntimeCancellationState::Unknown(snapshot)
                if snapshot.observed_at() == TimestampMillis::new(11))
    ));
    assert!(matches!(
        block_on(repository.admit_runtime_cancellation(
            &command,
            TimestampMillis::new(10_000),
        ))
        .expect("exact retry after deadline"),
        RuntimeCancellationAdmission::Replay(result)
            if matches!(result.state(), RuntimeCancellationState::Unknown(_))
    ));
    assert!(matches!(
        block_on(repository.mark_runtime_cancellation_effect_started(
            command.control().control_id(),
            command.control().command_digest(),
            TimestampMillis::new(12),
        ))
        .expect("repeat marker remains read-only"),
        RuntimeCancellationEffectStart::Replay(result)
            if matches!(result.state(), RuntimeCancellationState::Unknown(_))
    ));
}

#[test]
fn in_flight_unknown_settlement_preserves_the_newer_marker_snapshot() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory
        .path()
        .join("runtime-cancellation-in-flight-monotonic.sqlite3");
    let repository = open(&database_path);
    let session = seed_running_attempt(&repository);
    let command = command(
        "cancel-control-in-flight-monotonic",
        b"cancel-in-flight-monotonic",
        "ambiguous effect with a backward clock",
        &session,
    );
    let RuntimeCancellationAdmission::Admitted(admitted) =
        block_on(repository.admit_runtime_cancellation(&command, TimestampMillis::new(10)))
            .expect("admit cancellation")
    else {
        panic!("cancellation must be admitted")
    };
    assert_eq!(
        block_on(repository.mark_runtime_cancellation_effect_started(
            command.control().control_id(),
            command.control().command_digest(),
            TimestampMillis::new(50),
        ))
        .expect("mark process effect started"),
        RuntimeCancellationEffectStart::Started
    );

    let stale_unknown = RuntimeCancellationState::Unknown(
        RuntimeCancellationSessionSnapshot::new(
            admitted.state().session().session_id().clone(),
            admitted.state().session().lifecycle(),
            admitted.state().session().revision(),
            admitted.state().session().lifecycle_revision(),
            TimestampMillis::new(20),
        )
        .expect("stale unknown snapshot"),
    );
    assert!(matches!(
        block_on(repository.settle_runtime_cancellation(
            command.control().control_id(),
            command.control().command_digest(),
            stale_unknown,
        ))
        .expect("settle unknown effect"),
        RuntimeCancellationSettlement::Settled(result)
            if matches!(result.state(), RuntimeCancellationState::Unknown(snapshot)
                if snapshot.observed_at() == TimestampMillis::new(50))
    ));
    assert!(matches!(
        status(&repository, &command),
        RuntimeCancellationStatusLookup::Found(result)
            if matches!(result.state(), RuntimeCancellationState::Unknown(snapshot)
                if snapshot.observed_at() == TimestampMillis::new(50))
    ));
}

#[test]
fn in_flight_unknown_settlement_rejects_lifecycle_regression() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory
        .path()
        .join("runtime-cancellation-in-flight-lifecycle.sqlite3");
    let repository = open(&database_path);
    let session = seed_running_attempt(&repository);
    let command = command(
        "cancel-control-in-flight-lifecycle",
        b"cancel-in-flight-lifecycle",
        "ambiguous effect with regressed lifecycle",
        &session,
    );
    let RuntimeCancellationAdmission::Admitted(admitted) =
        block_on(repository.admit_runtime_cancellation(&command, TimestampMillis::new(10)))
            .expect("admit cancellation")
    else {
        panic!("cancellation must be admitted")
    };
    assert_eq!(
        block_on(repository.mark_runtime_cancellation_effect_started(
            command.control().control_id(),
            command.control().command_digest(),
            TimestampMillis::new(50),
        ))
        .expect("mark process effect started"),
        RuntimeCancellationEffectStart::Started
    );

    let regressed = RuntimeCancellationState::Unknown(
        RuntimeCancellationSessionSnapshot::new(
            admitted.state().session().session_id().clone(),
            LifecycleState::Running,
            Revision::new(admitted.state().session().revision().value() + 1),
            Revision::new(admitted.state().session().lifecycle_revision().value() + 1),
            TimestampMillis::new(60),
        )
        .expect("otherwise coherent regressed snapshot"),
    );
    assert!(matches!(
        block_on(repository.settle_runtime_cancellation(
            command.control().control_id(),
            command.control().command_digest(),
            regressed,
        ))
        .expect("preserve marker snapshot"),
        RuntimeCancellationSettlement::Settled(result)
            if matches!(result.state(), RuntimeCancellationState::Unknown(snapshot)
                if snapshot.lifecycle() == LifecycleState::Stopping
                    && snapshot.revision() == admitted.state().session().revision()
                    && snapshot.lifecycle_revision()
                        == admitted.state().session().lifecycle_revision()
                    && snapshot.observed_at() == TimestampMillis::new(50))
    ));
}

#[test]
fn durable_terminal_unknown_cannot_regress_to_a_restart_lifecycle() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory
        .path()
        .join("runtime-cancellation-terminal-unknown-lifecycle.sqlite3");
    let repository = open(&database_path);
    let session = seed_running_attempt(&repository);
    let command = command(
        "cancel-control-terminal-unknown-lifecycle",
        b"cancel-terminal-unknown-lifecycle",
        "terminal unknown must not restart",
        &session,
    );
    let RuntimeCancellationAdmission::Admitted(admitted) =
        block_on(repository.admit_runtime_cancellation(&command, TimestampMillis::new(10)))
            .expect("admit cancellation")
    else {
        panic!("cancellation must be admitted")
    };
    assert_eq!(
        block_on(repository.mark_runtime_cancellation_effect_started(
            command.control().control_id(),
            command.control().command_digest(),
            TimestampMillis::new(50),
        ))
        .expect("mark process effect started"),
        RuntimeCancellationEffectStart::Started
    );
    let terminal_unknown = RuntimeCancellationState::Unknown(
        RuntimeCancellationSessionSnapshot::new(
            admitted.state().session().session_id().clone(),
            LifecycleState::Stopped,
            Revision::new(admitted.state().session().revision().value() + 1),
            Revision::new(admitted.state().session().lifecycle_revision().value() + 1),
            TimestampMillis::new(60),
        )
        .expect("terminal unknown snapshot"),
    );
    assert!(matches!(
        block_on(repository.settle_runtime_cancellation(
            command.control().control_id(),
            command.control().command_digest(),
            terminal_unknown,
        ))
        .expect("settle terminal unknown"),
        RuntimeCancellationSettlement::Settled(_)
    ));
    let restarted = RuntimeCancellationState::Unknown(
        RuntimeCancellationSessionSnapshot::new(
            admitted.state().session().session_id().clone(),
            LifecycleState::Starting,
            Revision::new(admitted.state().session().revision().value() + 2),
            Revision::new(admitted.state().session().lifecycle_revision().value() + 2),
            TimestampMillis::new(70),
        )
        .expect("restart snapshot"),
    );
    assert_eq!(
        block_on(repository.settle_runtime_cancellation(
            command.control().control_id(),
            command.control().command_digest(),
            restarted,
        ))
        .expect("reject lifecycle regression"),
        RuntimeCancellationSettlement::Conflict
    );
    assert!(matches!(
        status(&repository, &command),
        RuntimeCancellationStatusLookup::Found(result)
            if matches!(result.state(), RuntimeCancellationState::Unknown(snapshot)
                if snapshot.lifecycle() == LifecycleState::Stopped
                    && snapshot.observed_at() == TimestampMillis::new(60))
    ));
}

#[test]
fn startup_claim_marks_every_accepted_control_before_a_second_restart() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory
        .path()
        .join("runtime-cancellation-two-restarts.sqlite3");
    let repository = open(&database_path);
    let running = seed_running_attempt(&repository);
    let graceful = command_with_mode(
        "cancel-control-before-restart-graceful",
        b"cancel-before-restart-graceful",
        "graceful before restart",
        RuntimeCancellationMode::Graceful,
        &running,
    );
    assert!(matches!(
        block_on(repository.admit_runtime_cancellation(&graceful, TimestampMillis::new(10)))
            .expect("admit graceful cancellation"),
        RuntimeCancellationAdmission::Admitted(_)
    ));
    let stopping = load_session(&repository);
    let force = command_with_mode(
        "cancel-control-before-restart-force",
        b"cancel-before-restart-force",
        "force before restart",
        RuntimeCancellationMode::Force,
        &stopping,
    );
    assert!(matches!(
        block_on(repository.admit_runtime_cancellation(&force, TimestampMillis::new(12)))
            .expect("admit force escalation"),
        RuntimeCancellationAdmission::Admitted(_)
    ));
    drop(repository);

    let repository = open(&database_path);
    assert_eq!(
        block_on(SessionRecoverySource::claim_runtime_cancellation_recovery(
            &repository,
            running.id(),
            TimestampMillis::new(11),
        ))
        .expect("first startup atomically claims accepted controls"),
        Some(RuntimeCancellationRecovery::Resume(
            RuntimeCancellationMode::Force
        ))
    );
    for command in [&graceful, &force] {
        assert!(matches!(
            status(&repository, command),
            RuntimeCancellationStatusLookup::Found(result)
                if matches!(result.state(), RuntimeCancellationState::Unknown(snapshot)
                    if snapshot.observed_at() == TimestampMillis::new(13))
        ));
    }
    drop(repository);

    let repository = open(&database_path);
    assert_eq!(
        block_on(SessionRecoverySource::claim_runtime_cancellation_recovery(
            &repository,
            running.id(),
            TimestampMillis::new(14),
        ))
        .expect("second startup preserves the unresolved effect"),
        Some(RuntimeCancellationRecovery::PreserveUnknown)
    );
}

#[test]
fn startup_and_maintenance_reject_corrupt_state_lifecycle_discriminator() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory
        .path()
        .join("runtime-cancellation-corrupt.sqlite3");
    let repository = open(&database_path);
    let session = seed_running_attempt(&repository);
    let command = command(
        "cancel-control-corrupt",
        b"cancel-corrupt",
        "corrupt row",
        &session,
    );
    assert!(matches!(
        block_on(repository.admit_runtime_cancellation(&command, TimestampMillis::new(10),))
            .expect("admit cancellation"),
        RuntimeCancellationAdmission::Admitted(_)
    ));
    drop(repository);

    let connection = Connection::open(&database_path).expect("open raw database");
    connection
        .pragma_update(None, "ignore_check_constraints", true)
        .expect("enable corruption fixture");
    connection
        .execute(
            "UPDATE runtime_cancellations SET session_lifecycle = 1 WHERE control_id = ?1",
            [command.control().control_id().as_str()],
        )
        .expect("corrupt cancellation lifecycle");
    drop(connection);

    assert!(matches!(
        verify_sqlite_database(&database_path),
        Err(SqliteMaintenanceError::IntegrityCheckFailed(_)
            | SqliteMaintenanceError::RepositoryInvariant(_))
    ));
    let config = SqliteRepositoryConfig::new(
        &database_path,
        SqliteRepositoryLimits::new(100, 100, 1_048_576, 100, 32).expect("limits"),
        Duration::from_secs(1),
    )
    .expect("configuration");
    assert!(
        SqliteSessionRepository::open(config).is_err(),
        "startup must reject accepted-stopping state with a running lifecycle"
    );
}

#[test]
fn maintenance_rejects_an_unknown_internal_cancellation_state_code() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory
        .path()
        .join("runtime-cancellation-unknown-state-code.sqlite3");
    let repository = open(&database_path);
    let session = seed_running_attempt(&repository);
    let command = command(
        "cancel-control-unknown-state-code",
        b"cancel-unknown-state-code",
        "tampered state code",
        &session,
    );
    assert!(matches!(
        block_on(repository.admit_runtime_cancellation(&command, TimestampMillis::new(10),))
            .expect("admit cancellation"),
        RuntimeCancellationAdmission::Admitted(_)
    ));
    drop(repository);

    let connection = Connection::open(&database_path).expect("open raw database");
    connection
        .pragma_update(None, "ignore_check_constraints", true)
        .expect("enable corruption fixture");
    connection
        .execute(
            "UPDATE runtime_cancellations SET state = 9 WHERE control_id = ?1",
            [command.control().control_id().as_str()],
        )
        .expect("corrupt cancellation state code");
    drop(connection);

    assert!(matches!(
        verify_sqlite_database(&database_path),
        Err(SqliteMaintenanceError::IntegrityCheckFailed(_)
            | SqliteMaintenanceError::RepositoryInvariant(_))
    ));
}

#[test]
fn startup_and_maintenance_reject_tampered_non_foundation_provider_id() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory
        .path()
        .join("runtime-cancellation-invalid-provider.sqlite3");
    let repository = open(&database_path);
    let session = seed_running_attempt(&repository);
    let command = command(
        "cancel-control-invalid-provider",
        b"cancel-invalid-provider",
        "tampered provider binding",
        &session,
    );
    assert!(matches!(
        block_on(repository.admit_runtime_cancellation(&command, TimestampMillis::new(10),))
            .expect("admit cancellation"),
        RuntimeCancellationAdmission::Admitted(_)
    ));
    drop(repository);

    let connection = Connection::open(&database_path).expect("open raw database");
    connection
        .pragma_update(None, "ignore_check_constraints", true)
        .expect("enable corruption fixture");
    connection
        .execute(
            "UPDATE runtime_cancellations SET expected_provider_id = 'undercurrent-local'
             WHERE control_id = ?1",
            [command.control().control_id().as_str()],
        )
        .expect("corrupt provider binding");
    drop(connection);

    assert!(matches!(
        verify_sqlite_database(&database_path),
        Err(SqliteMaintenanceError::IntegrityCheckFailed(_)
            | SqliteMaintenanceError::RepositoryInvariant(_))
    ));
    let config = SqliteRepositoryConfig::new(
        &database_path,
        SqliteRepositoryLimits::new(100, 100, 1_048_576, 100, 32).expect("limits"),
        Duration::from_secs(1),
    )
    .expect("configuration");
    assert!(
        SqliteSessionRepository::open(config).is_err(),
        "startup must fail closed for a malformed retained provider identity"
    );
}

#[test]
fn startup_and_maintenance_reject_tampered_non_foundation_tenant_id() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory
        .path()
        .join("runtime-cancellation-invalid-tenant.sqlite3");
    let repository = open(&database_path);
    let session = seed_running_attempt(&repository);
    let command = command(
        "cancel-control-invalid-tenant",
        b"cancel-invalid-tenant",
        "tampered tenant binding",
        &session,
    );
    assert!(matches!(
        block_on(repository.admit_runtime_cancellation(&command, TimestampMillis::new(10),))
            .expect("admit cancellation"),
        RuntimeCancellationAdmission::Admitted(_)
    ));
    drop(repository);

    let connection = Connection::open(&database_path).expect("open raw database");
    connection
        .pragma_update(None, "ignore_check_constraints", true)
        .expect("enable corruption fixture");
    connection
        .execute(
            "UPDATE runtime_cancellations SET tenant_id = 'tenant-1'
             WHERE control_id = ?1",
            [command.control().control_id().as_str()],
        )
        .expect("corrupt tenant binding");
    drop(connection);

    assert!(matches!(
        verify_sqlite_database(&database_path),
        Err(SqliteMaintenanceError::IntegrityCheckFailed(_)
            | SqliteMaintenanceError::RepositoryInvariant(_))
    ));
    let config = SqliteRepositoryConfig::new(
        &database_path,
        SqliteRepositoryLimits::new(100, 100, 1_048_576, 100, 32).expect("limits"),
        Duration::from_secs(1),
    )
    .expect("configuration");
    assert!(
        SqliteSessionRepository::open(config).is_err(),
        "startup must fail closed for a malformed retained tenant identity"
    );
}

#[test]
fn startup_and_maintenance_reject_cancellation_revision_regression_below_target_pins() {
    for dimension in ["session", "lifecycle"] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let database_path = directory.path().join(format!(
            "runtime-cancellation-regressed-{dimension}-revision.sqlite3"
        ));
        let repository = open(&database_path);
        let session = seed_running_attempt(&repository);
        let command = command(
            "cancel-control-regressed-revision",
            b"cancel-regressed-revision",
            "tampered retained revision",
            &session,
        );
        assert!(matches!(
            block_on(repository.admit_runtime_cancellation(&command, TimestampMillis::new(10)))
                .expect("admit cancellation"),
            RuntimeCancellationAdmission::Admitted(_)
        ));
        drop(repository);

        let connection = Connection::open(&database_path).expect("open raw database");
        let (statement, revision) = match dimension {
            "session" => (
                "UPDATE runtime_cancellations SET expected_session_revision = ?1
                 WHERE control_id = ?2",
                command.target().expected_session_revision().value() + 2,
            ),
            "lifecycle" => (
                "UPDATE runtime_cancellations SET observed_lifecycle_revision = ?1
                 WHERE control_id = ?2",
                command.target().expected_lifecycle_revision().value() - 1,
            ),
            _ => unreachable!("fixed revision dimension"),
        };
        connection
            .execute(
                statement,
                rusqlite::params![
                    revision.to_be_bytes().to_vec(),
                    command.control().control_id().as_str()
                ],
            )
            .expect("install revision-regressed retained cancellation");
        let integrity: String = connection
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect("physical integrity check");
        assert_eq!(integrity, "ok", "fixture must pass SQLite constraints");
        drop(connection);

        assert!(matches!(
            verify_sqlite_database(&database_path),
            Err(SqliteMaintenanceError::RepositoryInvariant(_))
        ));
        let config = SqliteRepositoryConfig::new(
            &database_path,
            SqliteRepositoryLimits::new(100, 100, 1_048_576, 100, 32).expect("limits"),
            Duration::from_secs(1),
        )
        .expect("configuration");
        assert!(
            SqliteSessionRepository::open(config).is_err(),
            "startup must fail closed for a retained {dimension} revision below its target pin"
        );
    }
}

#[test]
fn startup_and_maintenance_revalidate_all_retained_cancellation_field_grammars() {
    for scenario in [
        "control-identity",
        "session-byte-bound",
        "idempotency-grammar",
        "reason-plain-text",
        "reason-byte-bound",
        "source-product",
        "actor-context-byte-bound",
        "authority-plain-text",
    ] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let database_path = directory
            .path()
            .join(format!("runtime-cancellation-invalid-{scenario}.sqlite3"));
        let repository = open(&database_path);
        let session = seed_running_attempt(&repository);
        let command = command(
            "cancel-control-invalid-retained-field",
            b"cancel-invalid-retained-field",
            "tampered retained field",
            &session,
        );
        assert!(matches!(
            block_on(repository.admit_runtime_cancellation(&command, TimestampMillis::new(10)))
                .expect("admit cancellation"),
            RuntimeCancellationAdmission::Admitted(_)
        ));
        drop(repository);

        let connection = Connection::open(&database_path).expect("open raw database");
        let control_id = command.control().control_id().as_str();
        match scenario {
            "control-identity" => {
                connection
                    .execute(
                        "UPDATE runtime_cancellations SET control_id = 'control/invalid'
                         WHERE control_id = ?1",
                        [control_id],
                    )
                    .expect("tamper control identity");
            }
            "session-byte-bound" => {
                let oversized_session = "é".repeat(129);
                connection
                    .execute(
                        "UPDATE operation_attempts SET target_session = ?1
                         WHERE attempt_id = ?2",
                        rusqlite::params![
                            oversized_session,
                            command.target().attempt_id().as_str()
                        ],
                    )
                    .expect("tamper predecessor session identity");
                connection
                    .execute(
                        "UPDATE runtime_cancellations SET session_id = ?1
                         WHERE control_id = ?2",
                        rusqlite::params![oversized_session, control_id],
                    )
                    .expect("tamper cancellation session identity");
            }
            "idempotency-grammar" => {
                connection
                    .execute(
                        "UPDATE runtime_cancellations SET idempotency_key = 'idém'
                         WHERE control_id = ?1",
                        [control_id],
                    )
                    .expect("tamper idempotency key");
            }
            "reason-plain-text" => {
                connection
                    .execute(
                        "UPDATE runtime_cancellations SET reason = '   '
                         WHERE control_id = ?1",
                        [control_id],
                    )
                    .expect("tamper cancellation reason");
            }
            "reason-byte-bound" => {
                connection
                    .execute(
                        "UPDATE runtime_cancellations SET reason = ?1 WHERE control_id = ?2",
                        rusqlite::params!["é".repeat(257), control_id],
                    )
                    .expect("tamper cancellation reason byte bound");
            }
            "source-product" => {
                connection
                    .execute(
                        "UPDATE runtime_cancellations SET source_product = 'INVALID/workflow'
                         WHERE control_id = ?1",
                        [control_id],
                    )
                    .expect("tamper source product");
            }
            "actor-context-byte-bound" => {
                connection
                    .execute(
                        "UPDATE runtime_cancellations SET actor_context_ref = ?1
                         WHERE control_id = ?2",
                        rusqlite::params!["é".repeat(257), control_id],
                    )
                    .expect("tamper actor-context byte bound");
            }
            "authority-plain-text" => {
                connection
                    .execute(
                        "UPDATE runtime_cancellations SET authority_ref = '   '
                         WHERE control_id = ?1",
                        [control_id],
                    )
                    .expect("tamper authority reference");
            }
            _ => unreachable!("fixed retained-field scenario"),
        }
        let integrity: String = connection
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect("physical integrity check");
        assert_eq!(
            integrity, "ok",
            "{scenario} fixture must pass SQLite constraints"
        );
        drop(connection);

        assert!(matches!(
            verify_sqlite_database(&database_path),
            Err(SqliteMaintenanceError::RepositoryInvariant(_))
        ));
        let config = SqliteRepositoryConfig::new(
            &database_path,
            SqliteRepositoryLimits::new(100, 100, 1_048_576, 100, 32).expect("limits"),
            Duration::from_secs(1),
        )
        .expect("configuration");
        assert!(
            SqliteSessionRepository::open(config).is_err(),
            "startup must fail closed for the retained {scenario} violation"
        );
    }
}

#[test]
fn unrepresentable_effect_observation_fails_before_marking_or_recovery_io() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory
        .path()
        .join("runtime-cancellation-observation-overflow.sqlite3");
    let repository = open(&database_path);
    let session = seed_running_attempt(&repository);
    let command = command(
        "cancel-control-observation-overflow",
        b"cancel-observation-overflow",
        "observation timestamp overflow",
        &session,
    );
    assert!(matches!(
        block_on(repository.admit_runtime_cancellation(&command, TimestampMillis::new(10)))
            .expect("admit cancellation"),
        RuntimeCancellationAdmission::Admitted(_)
    ));
    drop(repository);

    let connection = Connection::open(&database_path).expect("open raw database");
    connection
        .execute(
            "UPDATE runtime_cancellations SET observed_at_millis = ?1 WHERE control_id = ?2",
            rusqlite::params![i64::MAX, command.control().control_id().as_str()],
        )
        .expect("install maximum retained observation");
    drop(connection);

    let repository = open(&database_path);
    let marker_error = block_on(repository.mark_runtime_cancellation_effect_started(
        command.control().control_id(),
        command.control().command_digest(),
        TimestampMillis::new(i64::MAX),
    ))
    .expect_err("effect marker must fail before process IO");
    assert_eq!(marker_error.kind(), PortErrorKind::InvalidData);
    assert!(matches!(
        status(&repository, &command),
        RuntimeCancellationStatusLookup::Found(result)
            if matches!(result.state(), RuntimeCancellationState::AcceptedStopping(snapshot)
                if snapshot.observed_at() == TimestampMillis::new(i64::MAX))
    ));
    let recovery_error = block_on(SessionRecoverySource::claim_runtime_cancellation_recovery(
        &repository,
        command.target().session_id(),
        TimestampMillis::new(i64::MAX),
    ))
    .expect_err("startup claim must fail before stop IO");
    assert_eq!(recovery_error.kind(), PortErrorKind::InvalidData);
}

#[test]
fn restart_recovery_preserves_unknown_without_process_redrive_instruction() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory
        .path()
        .join("runtime-cancellation-unknown.sqlite3");
    let repository = open(&database_path);
    let session = seed_running_attempt(&repository);
    let command = command(
        "cancel-control-unknown",
        b"cancel-unknown",
        "unknown process effect",
        &session,
    );
    let RuntimeCancellationAdmission::Admitted(admitted) =
        block_on(repository.admit_runtime_cancellation(&command, TimestampMillis::new(10)))
            .expect("admit cancellation")
    else {
        panic!("cancellation must be admitted")
    };
    let admitted_session = admitted.state().session();
    let unknown = RuntimeCancellationState::Unknown(
        RuntimeCancellationSessionSnapshot::new(
            admitted_session.session_id().clone(),
            admitted_session.lifecycle(),
            admitted_session.revision(),
            admitted_session.lifecycle_revision(),
            TimestampMillis::new(20),
        )
        .expect("unknown snapshot"),
    );
    assert!(matches!(
        block_on(repository.settle_runtime_cancellation(
            command.control().control_id(),
            command.control().command_digest(),
            unknown,
        ))
        .expect("settle unknown effect"),
        RuntimeCancellationSettlement::Settled(_)
    ));
    drop(repository);

    let repository = open(&database_path);
    assert_eq!(
        block_on(SessionRecoverySource::claim_runtime_cancellation_recovery(
            &repository,
            command.target().session_id(),
            TimestampMillis::new(21),
        ))
        .expect("read retained unknown recovery instruction"),
        Some(RuntimeCancellationRecovery::PreserveUnknown)
    );
}

#[test]
fn graceful_cancellation_persists_no_force_and_replay_does_not_repeat_it() {
    struct GracefulOnlyAuthorizer;
    impl RuntimeCancellationAuthorizer for GracefulOnlyAuthorizer {
        fn authorize_cancel<'a>(
            &'a self,
            _: &'a RuntimeCancellationActor,
            _: &'a OperationAttemptId,
            _: &'a OperationAttemptId,
            mode: RuntimeCancellationMode,
        ) -> PortFuture<'a, Result<RuntimeCancellationAuthorization, PortError>> {
            Box::pin(async move {
                Ok(if mode == RuntimeCancellationMode::Graceful {
                    RuntimeCancellationAuthorization::Authorized
                } else {
                    RuntimeCancellationAuthorization::Denied
                })
            })
        }
        fn authorize_status<'a>(
            &'a self,
            _: &'a RuntimeCancellationActor,
            _: &'a OperationAttemptId,
            _: &'a OperationAttemptId,
        ) -> PortFuture<'a, Result<RuntimeCancellationAuthorization, PortError>> {
            Box::pin(async { Ok(RuntimeCancellationAuthorization::Authorized) })
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("graceful-only.sqlite3");
    let repository = open(&path);
    let running = seed_running_attempt(&repository);
    let graceful = command(
        "graceful-control",
        b"graceful",
        "cooperative stop",
        &running,
    );
    let runtime = SuccessfulStopRuntime {
        refuse_stop: true,
        ..Default::default()
    };
    let processes = TestProcessDirectory::from_session(&running);
    let clock = FixedClock(TimestampMillis::new(100));
    let service = CancelRuntimeAttemptService::new(
        &GracefulOnlyAuthorizer,
        &repository,
        &repository,
        &ImmediateCoordinator,
        &runtime,
        &processes,
        &NoopRestarts,
        &NoopProjection,
        &NoopTelemetry,
        &clock,
    );
    assert!(matches!(
        block_on(service.execute(graceful.clone())),
        Err(CancelRuntimeAttemptError::EffectUnknown(_))
    ));
    assert!(runtime.stop_policies.lock().unwrap()[0].no_escalation());
    assert!(
        load_session(&repository)
            .configuration()
            .execution()
            .stop()
            .no_escalation()
    );
    let force = command_with_mode(
        "denied-force",
        b"force",
        "force stop",
        RuntimeCancellationMode::Force,
        &load_session(&repository),
    );
    assert!(matches!(
        block_on(service.execute(force)),
        Err(CancelRuntimeAttemptError::Denied)
    ));
    assert!(matches!(
        block_on(service.execute(graceful.clone())).unwrap().state(),
        RuntimeCancellationState::Unknown(_)
    ));
    assert_eq!(runtime.calls.load(Ordering::Acquire), 1);
    assert_eq!(runtime.force_calls.load(Ordering::Acquire), 0);
    drop(repository);
    let repository = open(&path);
    assert!(
        load_session(&repository)
            .configuration()
            .execution()
            .stop()
            .no_escalation()
    );
    assert_eq!(
        block_on(SessionRecoverySource::claim_runtime_cancellation_recovery(
            &repository,
            running.id(),
            TimestampMillis::new(101)
        ))
        .unwrap(),
        Some(RuntimeCancellationRecovery::PreserveUnknown)
    );
    assert!(
        matches!(status(&repository, &graceful), RuntimeCancellationStatusLookup::Found(result) if matches!(result.state(), RuntimeCancellationState::Unknown(_)))
    );
}

fn cancellation_service<'a>(
    cancellation_repository: &'a dyn RuntimeCancellationRepository,
    session_repository: &'a dyn SessionRepository,
    runtime: &'a dyn ProcessRuntime,
    processes: &'a dyn SessionProcessDirectory,
    clock: &'a dyn Clock,
) -> CancelRuntimeAttemptService<'a> {
    static AUTHORIZER: AllowRuntimeCancellation = AllowRuntimeCancellation;
    static COORDINATOR: ImmediateCoordinator = ImmediateCoordinator;
    static RESTARTS: NoopRestarts = NoopRestarts;
    static PROJECTION: NoopProjection = NoopProjection;
    static TELEMETRY: NoopTelemetry = NoopTelemetry;
    CancelRuntimeAttemptService::new(
        &AUTHORIZER,
        cancellation_repository,
        session_repository,
        &COORDINATOR,
        runtime,
        processes,
        &RESTARTS,
        &PROJECTION,
        &TELEMETRY,
        clock,
    )
}

struct FailFirstSessionRead<'a> {
    inner: &'a SqliteSessionRepository,
    remaining: AtomicUsize,
}

impl<'a> FailFirstSessionRead<'a> {
    fn new(inner: &'a SqliteSessionRepository) -> Self {
        Self {
            inner,
            remaining: AtomicUsize::new(1),
        }
    }
}

impl SessionRepository for FailFirstSessionRead<'_> {
    fn insert<'a>(
        &'a self,
        session: &'a Session,
        event: &'a SessionOutboxEvent,
    ) -> PortFuture<'a, Result<InsertSessionOutcome, PortError>> {
        self.inner.insert(session, event)
    }

    fn load<'a>(
        &'a self,
        session_id: &'a SessionId,
    ) -> PortFuture<'a, Result<Option<Session>, PortError>> {
        if self
            .remaining
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            let error = test_port_error(PortOperation::LoadSession, Some(session_id));
            Box::pin(async move { Err(error) })
        } else {
            SessionRepository::load(self.inner, session_id)
        }
    }

    fn find_by_name<'a>(
        &'a self,
        name: &'a SessionName,
    ) -> PortFuture<'a, Result<Option<Session>, PortError>> {
        self.inner.find_by_name(name)
    }

    fn commit<'a>(
        &'a self,
        session: &'a Session,
        expected_revision: Revision,
        events: &'a [SessionOutboxEvent],
    ) -> PortFuture<'a, Result<CommitSessionOutcome, PortError>> {
        self.inner.commit(session, expected_revision, events)
    }

    fn delete<'a>(
        &'a self,
        session_id: &'a SessionId,
        expected_revision: Revision,
        event: &'a SessionOutboxEvent,
    ) -> PortFuture<'a, Result<DeleteSessionOutcome, PortError>> {
        self.inner.delete(session_id, expected_revision, event)
    }
}

struct FailFirstEffectMarker<'a> {
    inner: &'a SqliteSessionRepository,
    remaining: AtomicUsize,
}

impl<'a> FailFirstEffectMarker<'a> {
    fn new(inner: &'a SqliteSessionRepository) -> Self {
        Self {
            inner,
            remaining: AtomicUsize::new(1),
        }
    }
}

impl RuntimeCancellationRepository for FailFirstEffectMarker<'_> {
    fn admit_runtime_cancellation<'a>(
        &'a self,
        command: &'a CancelRuntimeAttemptCommand,
        observed_at: TimestampMillis,
    ) -> PortFuture<'a, Result<RuntimeCancellationAdmission, PortError>> {
        self.inner.admit_runtime_cancellation(command, observed_at)
    }

    fn mark_runtime_cancellation_effect_started<'a>(
        &'a self,
        control_id: &'a OperationAttemptId,
        command_digest: PayloadDigest,
        observed_at: TimestampMillis,
    ) -> PortFuture<'a, Result<RuntimeCancellationEffectStart, PortError>> {
        if self
            .remaining
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            let error = test_port_error(PortOperation::MarkRuntimeCancellationEffectStarted, None);
            Box::pin(async move { Err(error) })
        } else {
            self.inner.mark_runtime_cancellation_effect_started(
                control_id,
                command_digest,
                observed_at,
            )
        }
    }

    fn settle_runtime_cancellation<'a>(
        &'a self,
        control_id: &'a OperationAttemptId,
        command_digest: PayloadDigest,
        state: RuntimeCancellationState,
    ) -> PortFuture<'a, Result<RuntimeCancellationSettlement, PortError>> {
        self.inner
            .settle_runtime_cancellation(control_id, command_digest, state)
    }

    fn get_runtime_cancellation_status<'a>(
        &'a self,
        query: &'a GetRuntimeCancellationStatusQuery,
    ) -> PortFuture<'a, Result<RuntimeCancellationStatusLookup, PortError>> {
        self.inner.get_runtime_cancellation_status(query)
    }
}

struct AllowRuntimeCancellation;

impl RuntimeCancellationAuthorizer for AllowRuntimeCancellation {
    fn authorize_cancel<'a>(
        &'a self,
        _actor: &'a RuntimeCancellationActor,
        _control_id: &'a OperationAttemptId,
        _target_attempt_id: &'a OperationAttemptId,
        _mode: RuntimeCancellationMode,
    ) -> PortFuture<'a, Result<RuntimeCancellationAuthorization, PortError>> {
        Box::pin(async { Ok(RuntimeCancellationAuthorization::Authorized) })
    }

    fn authorize_status<'a>(
        &'a self,
        _actor: &'a RuntimeCancellationActor,
        _control_id: &'a OperationAttemptId,
        _target_attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<RuntimeCancellationAuthorization, PortError>> {
        Box::pin(async { Ok(RuntimeCancellationAuthorization::Authorized) })
    }
}

struct ImmediateCoordinator;
struct ImmediateLease;
impl SessionMutationLease for ImmediateLease {}

impl SessionMutationCoordinator for ImmediateCoordinator {
    fn acquire<'a>(
        &'a self,
        _session_id: &'a SessionId,
    ) -> PortFuture<'a, Result<Box<dyn SessionMutationLease>, PortError>> {
        Box::pin(async { Ok(Box::new(ImmediateLease) as Box<dyn SessionMutationLease>) })
    }
}

#[derive(Default)]
struct SuccessfulStopRuntime {
    calls: AtomicUsize,
    force_calls: AtomicUsize,
    stop_policies: Mutex<Vec<StopPolicy>>,
    refuse_stop: bool,
}

impl ProcessRuntime for SuccessfulStopRuntime {
    fn spawn<'a>(
        &'a self,
        session_id: &'a SessionId,
        _launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<ProcessInstance, PortError>> {
        let error = test_port_error(PortOperation::SpawnProcess, Some(session_id));
        Box::pin(async move { Err(error) })
    }

    fn arm_exit<'a>(
        &'a self,
        session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<(), PortError>> {
        let error = test_port_error(PortOperation::ArmProcessExit, Some(session_id));
        Box::pin(async move { Err(error) })
    }

    fn stop<'a>(
        &'a self,
        _session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
        policy: StopPolicy,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        self.stop_policies.lock().unwrap().push(policy);
        if self.refuse_stop {
            return Box::pin(async { Err(test_port_error(PortOperation::StopProcess, None)) });
        }
        Box::pin(async {
            Ok(ProcessTermination::new(
                TerminationKind::Graceful,
                None,
                TimestampMillis::new(2_100),
            ))
        })
    }

    fn stop_admitted<'a>(
        &'a self,
        _admission: &'a undercurrent_application::OperationAttemptAdmission,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        policy: StopPolicy,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        self.stop(session_id, process, policy)
    }

    fn force_stop<'a>(
        &'a self,
        _session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        self.force_calls.fetch_add(1, Ordering::AcqRel);
        Box::pin(async {
            Ok(ProcessTermination::new(
                TerminationKind::Forced,
                None,
                TimestampMillis::new(2_100),
            ))
        })
    }

    fn force_stop_admitted<'a>(
        &'a self,
        _admission: &'a undercurrent_application::OperationAttemptAdmission,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        self.force_stop(session_id, process)
    }

    fn write_input<'a>(
        &'a self,
        session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
        _input: &'a InputSequence,
        _policy: InputDeliveryPolicy,
    ) -> PortFuture<'a, Result<(), PortError>> {
        let error = test_port_error(PortOperation::WriteInput, Some(session_id));
        Box::pin(async move { Err(error) })
    }

    fn resize<'a>(
        &'a self,
        session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
        _size: TerminalSize,
    ) -> PortFuture<'a, Result<(), PortError>> {
        let error = test_port_error(PortOperation::ResizeTerminal, Some(session_id));
        Box::pin(async move { Err(error) })
    }
}

struct TestProcessDirectory(Mutex<Option<ProcessInstance>>);

impl TestProcessDirectory {
    fn from_session(session: &Session) -> Self {
        let process = session.process().expect("running process ownership");
        Self(Mutex::new(Some(ProcessInstance {
            id: process.id().clone(),
            started_at: process.started_at(),
        })))
    }
}

impl SessionProcessDirectory for TestProcessDirectory {
    fn bind(&self, _session_id: &SessionId, process: ProcessInstance) -> Result<(), PortError> {
        *self.0.lock().expect("process directory") = Some(process);
        Ok(())
    }

    fn lookup(&self, _session_id: &SessionId) -> Result<Option<ProcessInstance>, PortError> {
        Ok(self.0.lock().expect("process directory").clone())
    }

    fn unbind_if(
        &self,
        _session_id: &SessionId,
        process: &ProcessInstanceId,
    ) -> Result<ProcessUnbindOutcome, PortError> {
        let mut current = self.0.lock().expect("process directory");
        match current.as_ref() {
            None => Ok(ProcessUnbindOutcome::Missing),
            Some(bound) if &bound.id != process => Ok(ProcessUnbindOutcome::ProcessMismatch),
            Some(_) => Ok(ProcessUnbindOutcome::Unbound(
                current.take().expect("matching process"),
            )),
        }
    }
}

struct NoopRestarts;

impl RestartScheduler for NoopRestarts {
    fn schedule<'a>(&'a self, _job: &'a RestartJob) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async { Ok(()) })
    }

    fn cancel_session_restarts<'a>(
        &'a self,
        _session_id: &'a SessionId,
    ) -> PortFuture<'a, Result<usize, PortError>> {
        Box::pin(async { Ok(0) })
    }
}

struct NoopProjection;

impl SessionProjectionWriter for NoopProjection {
    fn upsert_session(&self, _session: &Session) -> Result<(), PortError> {
        Ok(())
    }

    fn remove_summary(&self, _session_id: &SessionId) -> Result<(), PortError> {
        Ok(())
    }
}

struct NoopTelemetry;

impl undercurrent_application::Telemetry for NoopTelemetry {
    fn emit_diagnostic(&self, _record: &DiagnosticRecord) {}

    fn record_metric(&self, _observation: MetricObservation) {}

    fn record_session_metric(&self, _observation: &SessionMetricObservation) {}
}

struct FixedClock(TimestampMillis);

impl Clock for FixedClock {
    fn now(&self) -> TimestampMillis {
        self.0
    }
}

fn test_port_error(operation: PortOperation, session_id: Option<&SessionId>) -> PortError {
    PortError::new(
        PortErrorKind::Unavailable,
        operation,
        session_id.cloned(),
        io::Error::other("private retry fixture failure"),
    )
}

fn seed_running_attempt(repository: &SqliteSessionRepository) -> Session {
    seed_running_attempt_with_session(repository, session())
}

fn seed_running_attempt_with_session(
    repository: &SqliteSessionRepository,
    mut session: Session,
) -> Session {
    let created = SessionOutboxEvent {
        kind: DomainEventKind::SessionCreated,
        session_id: session.id().clone(),
        revision: session.revision(),
        occurred_at: session.updated_at(),
    };
    assert_eq!(
        block_on(SessionRepository::insert(repository, &session, &created))
            .expect("insert session"),
        InsertSessionOutcome::Inserted
    );
    let initial_revision = session.revision();
    let ownership = session
        .record_process_ownership(
            ProcessIdentity::new(
                ProcessInstanceId::new("process-1").expect("process ID"),
                TimestampMillis::new(2),
            ),
            TimestampMillis::new(2),
        )
        .expect("record process")
        .expect("ownership changes");
    let ownership_event = outbox_event(&session, ownership.kind(), ownership.revision());
    let running = session
        .transition_lifecycle(LifecycleState::Running, TimestampMillis::new(3))
        .expect("running transition")
        .expect("lifecycle changes");
    let running_event = outbox_event(&session, running.kind(), running.revision());
    assert_eq!(
        block_on(SessionRepository::commit(
            repository,
            &session,
            initial_revision,
            &[ownership_event, running_event],
        ))
        .expect("commit running session"),
        CommitSessionOutcome::Committed
    );

    let attempt = OperationAttempt::accepted_with_dispatch(
        OperationAttemptId::new("runtime-start-1").expect("attempt ID"),
        Some(session.id().clone()),
        ProcessMutationKind::Spawn,
        FencingToken::new(7).expect("fence"),
        PayloadDigest::of_bytes(b"internal start payload"),
        Some(RuntimeDispatchBinding::new(
            DispatchCellIdentity::new("cell:workflow").expect("cell"),
            start_request_digest(),
            PayloadDigest::of_bytes(b"launch profile"),
            PayloadDigest::of_bytes(b"environment binding"),
            PayloadDigest::of_bytes(b"input set"),
            runtime_policy(),
        )),
        TimestampMillis::new(4),
    );
    assert!(matches!(
        block_on(OperationAttemptStore::accept(repository, &attempt)).expect("accept attempt"),
        OperationAttemptAcceptance::Accepted(_)
    ));
    assert!(matches!(
        block_on(OperationAttemptStore::settle(
            repository,
            &OperationAttemptSettlement {
                attempt_id: attempt.id().clone(),
                fencing_token: attempt.fencing_token(),
                certainty: EffectCertainty::KnownApplied,
                result_digest: Some(PayloadDigest::of_bytes(b"start result")),
                settled_at: TimestampMillis::new(5),
            },
        ))
        .expect("settle attempt"),
        OperationAttemptSettlementOutcome::Settled(_)
    ));
    session
}

fn command(
    control_id: &str,
    digest_material: &[u8],
    reason: &str,
    session: &Session,
) -> CancelRuntimeAttemptCommand {
    command_with_mode(
        control_id,
        digest_material,
        reason,
        RuntimeCancellationMode::Graceful,
        session,
    )
}

fn command_with_mode(
    control_id: &str,
    digest_material: &[u8],
    reason: &str,
    mode: RuntimeCancellationMode,
    session: &Session,
) -> CancelRuntimeAttemptCommand {
    let actor = actor();
    let target = undercurrent_application::RuntimeCancellationTarget::new(
        OperationAttemptId::new("runtime-start-1").expect("attempt ID"),
        start_request_digest(),
        FencingToken::new(7).expect("fence"),
        DispatchCellIdentity::new("cell:workflow").expect("cell"),
        session.id().clone(),
        session.revision(),
        session.lifecycle_revision(),
        "prv_0198a5bd-2b01-7ae9-9a0c-42532d56e7f7".to_owned(),
        3,
    )
    .expect("target");
    let control = RuntimeCancellationControl::new(
        OperationAttemptId::new(control_id).expect("control ID"),
        PayloadDigest::of_bytes(digest_material),
        PayloadDigest::of_bytes(&[b"product request ".as_slice(), digest_material].concat()),
        format!("idempotency-{control_id}"),
        mode,
        reason.to_owned(),
        RuntimeCancellationPropagationPolicy::TargetOnly,
        TimestampMillis::new(1_000),
        actor.source_product().to_owned(),
        actor.tenant_id().to_owned(),
        actor.actor_context_ref().to_owned(),
        actor.authority_ref().to_owned(),
    )
    .expect("control");
    CancelRuntimeAttemptCommand::new(target, control, actor).expect("command")
}

fn status(
    repository: &SqliteSessionRepository,
    command: &CancelRuntimeAttemptCommand,
) -> RuntimeCancellationStatusLookup {
    let query = status_query(command);
    block_on(repository.get_runtime_cancellation_status(&query)).expect("status lookup")
}

fn status_query(command: &CancelRuntimeAttemptCommand) -> GetRuntimeCancellationStatusQuery {
    GetRuntimeCancellationStatusQuery::new(
        command.control().control_id().clone(),
        command.control().command_digest(),
        command.target().attempt_id().clone(),
        command.target().request_digest(),
        command.target().session_id().clone(),
        command.target().expected_provider_id().to_owned(),
        command.target().expected_provider_generation(),
        actor(),
    )
    .expect("status query")
}

fn actor() -> RuntimeCancellationActor {
    RuntimeCancellationActor::new(
        "principal-1".to_owned(),
        Some("tenant_0198a5bd-2afd-7dd1-b6a3-1fb90c6f0a21".to_owned()),
        "transport-authority-1".to_owned(),
        "example/workflow".to_owned(),
        "tenant_0198a5bd-2afd-7dd1-b6a3-1fb90c6f0a21".to_owned(),
        "actor-context-1".to_owned(),
        "authority-1".to_owned(),
    )
    .expect("actor")
}

fn load_session(repository: &SqliteSessionRepository) -> Session {
    block_on(SessionRepository::load(
        repository,
        &SessionId::new("session-1").expect("session ID"),
    ))
    .expect("load session")
    .expect("session exists")
}

fn outbox_event(
    session: &Session,
    kind: DomainEventKind,
    revision: Revision,
) -> SessionOutboxEvent {
    SessionOutboxEvent {
        kind,
        session_id: session.id().clone(),
        revision,
        occurred_at: session.updated_at(),
    }
}

fn start_request_digest() -> PayloadDigest {
    PayloadDigest::of_bytes(b"canonical caller start request")
}

fn runtime_policy() -> RuntimeDispatchPolicy {
    RuntimeDispatchPolicy::new(
        RuntimeDispatchLimits::new(30_000, 1_024, 2_048, None, None).expect("limits"),
        RuntimeOutputContract::new(
            "runtime-cancellation-test/0.1.0-beta",
            PayloadDigest::of_bytes(b"result schema"),
            Classification::Internal,
        )
        .expect("output contract"),
        RuntimeTelemetryPolicyRef::new("trust://telemetry/policies/runtime-cancellation")
            .expect("telemetry policy"),
        undercurrent_domain::RuntimeRetentionPolicyRef::new(
            "trust://retention/policies/runtime-cancellation",
        )
        .expect("retention policy"),
        vec![
            RuntimeCallbackRef::new(
                "runtime-callback://example/workflow/v0.1.0-beta/runtime-cancellation",
            )
            .expect("callback"),
        ],
        TimestampMillis::new(10_000),
        TimestampMillis::new(20_000),
    )
    .expect("runtime dispatch policy")
}

fn session() -> Session {
    session_with_id("session-1")
}

fn session_with_id(session_id: &str) -> Session {
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
        SessionId::new(session_id).expect("session ID"),
        SessionName::new("runtime-cancellation-session").expect("session name"),
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
