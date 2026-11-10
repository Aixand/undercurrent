// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};
use std::time::Duration;

use undercurrent_domain::{
    CommandSpec, DeliveryId, DetectionArchetype, DetectionProfile, DetectionProfileLimits,
    DomainEventKind, EffectiveProfileOrigin, EffectiveProfileSelection, EffectiveProfileTransition,
    EnvironmentVariables, ExecutionLimits, ExecutionPolicy, ExecutionState, ForegroundObservation,
    HealthRules, InputDeliveryPolicy, InputItem, InputObservationPolicy, InputSequence, Key,
    Labels, LaunchSpec, LifecycleState, Metadata, MouseButton, MouseEvent, MouseEventKind,
    NoRestartReason, OutputExclusions, PayloadDigest, ProcessIdentity, ProcessInstanceId,
    ProcessOutcome, ProcessTruth, ProfileBindingMode, ProfileMatchRules, ProfileName,
    RestartConfig, RestartDecision, RestartPolicy, RestartStatus, Revision, Session,
    SessionConfiguration, SessionId, SessionName, StopPolicy, StopSignal, TerminalSize,
    TimestampMillis, WorkItem, WorkItemId, WorkItemKey, WorkOwner, WorkRelationKind, WorkStatus,
};

use super::*;
use crate::{
    Clock, CommitSessionOutcome, DeleteSessionOutcome, DetectionProfileCatalog,
    DiagnosticEventName, DiagnosticRecord, EffectiveProfileDirectory, InputDeliveryDisposition,
    InputDeliveryReservation, InputDeliveryStore, InsertSessionOutcome, MetricObservation,
    ObservationJob, ObservationReconcileOutcome, ObservationScheduleOutcome, ObservationScheduler,
    PortError, PortErrorKind, PortFuture, PortOperation, PreparedReadModels,
    PreparedSessionCatalog, ProcessExitObservation, ProcessInstance, ProcessRuntime,
    ProcessTermination, ProcessTruthSource, ProcessUnbindOutcome, ProfileCatalogEntry,
    ProfileSourceKind, ProfileSummary, RecoverySessionBatch, RestartJob, RestartScheduler,
    RuntimeCancellationMode, RuntimeCancellationRecovery, SessionIdSource,
    SessionMutationCoordinator, SessionMutationLease, SessionObservationPlan, SessionOutboxEvent,
    SessionProcessDirectory, SessionProjectionWriter, SessionRecoverySource, SessionRepository,
    SessionSubscriptionHub, StandardInputClosure, Telemetry, TerminationKind,
};

#[test]
fn start_commits_running_session_binds_process_and_projects_summary() {
    let harness = Harness::new();

    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");

    assert_eq!(started.session.lifecycle(), LifecycleState::Running);
    assert_eq!(harness.runtime.arm_calls(), 1);
    assert!(started.session.updated_at() >= started.process.started_at);
    assert_eq!(started.session.revision(), Revision::new(3));
    assert_eq!(
        started
            .session
            .process()
            .expect("durable process ownership")
            .id(),
        &started.process.id
    );
    assert_eq!(
        harness
            .repository
            .load_sync(started.session.id())
            .lifecycle(),
        LifecycleState::Running
    );
    assert_eq!(
        harness
            .catalog
            .get(started.session.id())
            .expect("catalog readable")
            .expect("summary present")
            .lifecycle(),
        LifecycleState::Running
    );
    assert_eq!(
        harness
            .processes
            .lookup(started.session.id())
            .expect("lookup"),
        Some(started.process)
    );
    assert_eq!(
        harness.repository.event_kinds(),
        vec![
            DomainEventKind::SessionCreated,
            DomainEventKind::ProcessOwnershipChanged,
            DomainEventKind::LifecycleChanged
        ]
    );
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::ProcessSpawnSucceeded)
    );
    assert_eq!(harness.telemetry.metric_count(), 1);
}

#[test]
fn start_returns_stable_id_and_initial_client_revision_without_subscription() {
    let harness = Harness::new();

    let started = block_on(harness.service().execute(command("receipt")))
        .expect("start returns its durable receipt");
    let returned_id = started.session.id().clone();
    let returned_revision = started.session.revision();

    assert_eq!(returned_id.as_str(), "session-1");
    assert_eq!(returned_revision, Revision::new(3));
    assert_eq!(
        harness.repository.load_sync(&returned_id).revision(),
        returned_revision
    );
    assert_eq!(
        harness
            .catalog
            .get(&returned_id)
            .expect("catalog lookup succeeds")
            .expect("start projects the returned identity")
            .revision(),
        returned_revision
    );
}

#[test]
fn launch_preparation_failure_happens_before_identity_persistence_or_spawn() {
    let harness = Harness::new();
    harness.runtime.fail_prepare.store(1, Ordering::SeqCst);

    let error = block_on(harness.service().execute(command("unprepared")))
        .expect_err("unprepared launch must fail");

    assert!(matches!(error, StartSessionError::LaunchPreparation(_)));
    assert_eq!(harness.repository.session_count(), 0);
    assert_eq!(harness.runtime.spawn_calls(), 0);
}

#[test]
fn dispatched_start_replays_after_expiry_without_second_accept_or_spawn() {
    let harness = Harness::new();
    let raw_runtime = Arc::new(FakeRuntime::new());
    let attempts = Arc::new(DispatchAttemptStore::default());
    let durable_clock = Arc::new(IncrementingClock::new(10_000));
    let durable_runtime = crate::DurableProcessRuntime::new(
        raw_runtime.clone(),
        attempts.clone(),
        durable_clock.clone(),
    );
    let binding = runtime_dispatch_binding(
        "cell:build",
        b"launch-profile",
        b"environment-binding",
        b"input",
        10_010,
    );
    let mut request = command("dispatched-build");
    request.dispatch = Some(crate::OperationAttemptAdmission::dispatched(
        undercurrent_domain::OperationAttemptId::new("attempt:build:7").expect("attempt"),
        None,
        undercurrent_domain::ProcessMutationKind::Spawn,
        undercurrent_domain::FencingToken::new(7).expect("fence"),
        undercurrent_domain::PayloadDigest::of_bytes(b"canonical-start-request"),
        binding,
        TimestampMillis::new(100),
    ));

    let service = StartSessionService::new(
        &harness.clock,
        &harness.ids,
        &harness.coordinator,
        &harness.repository,
        &durable_runtime,
        &harness.processes,
        &harness.catalog,
        &harness.telemetry,
    )
    .with_operation_attempt_store(attempts.as_ref());
    let first = block_on(service.execute(request.clone())).expect("first dispatch");
    assert_eq!(attempts.accept_calls(), 1);
    assert_eq!(harness.repository.insert_calls(), 1);
    harness.clock.set(10_010);
    durable_clock.set(10_010);

    // A fresh service represents response loss followed by executor reconstruction.
    let restarted_service = StartSessionService::new(
        &harness.clock,
        &harness.ids,
        &harness.coordinator,
        &harness.repository,
        &durable_runtime,
        &harness.processes,
        &harness.catalog,
        &harness.telemetry,
    )
    .with_operation_attempt_store(attempts.as_ref());
    let replayed = block_on(restarted_service.execute(request.clone())).expect("durable replay");

    assert_eq!(replayed, first);
    assert_eq!(raw_runtime.spawn_calls(), 1);

    let mut changed_binding = request.clone();
    changed_binding.dispatch = Some(crate::OperationAttemptAdmission::dispatched(
        undercurrent_domain::OperationAttemptId::new("attempt:build:7").expect("attempt"),
        None,
        undercurrent_domain::ProcessMutationKind::Spawn,
        undercurrent_domain::FencingToken::new(7).expect("fence"),
        undercurrent_domain::PayloadDigest::of_bytes(b"canonical-start-request"),
        runtime_dispatch_binding(
            "cell:build",
            b"launch-profile",
            b"environment-binding",
            b"changed-input",
            10_010,
        ),
        TimestampMillis::new(100),
    ));
    assert!(matches!(
        block_on(restarted_service.execute(changed_binding)),
        Err(StartSessionError::DispatchConflict)
    ));
    assert_eq!(raw_runtime.spawn_calls(), 1);

    let stale_ids = FixedIds::new("session-stale");
    let mut stale = command("stale-dispatch");
    stale.dispatch = Some(crate::OperationAttemptAdmission::dispatched(
        undercurrent_domain::OperationAttemptId::new("attempt:build:6").expect("attempt"),
        None,
        undercurrent_domain::ProcessMutationKind::Spawn,
        undercurrent_domain::FencingToken::new(6).expect("fence"),
        undercurrent_domain::PayloadDigest::of_bytes(b"stale-canonical-start-request"),
        runtime_dispatch_binding(
            "cell:build",
            b"launch-profile",
            b"environment-binding",
            b"stale-input",
            10_010,
        ),
        TimestampMillis::new(101),
    ));
    let stale_service = StartSessionService::new(
        &harness.clock,
        &stale_ids,
        &harness.coordinator,
        &harness.repository,
        &durable_runtime,
        &harness.processes,
        &harness.catalog,
        &harness.telemetry,
    )
    .with_operation_attempt_store(attempts.as_ref());
    assert!(matches!(
        block_on(stale_service.execute(stale)),
        Err(StartSessionError::DispatchPolicyNotCurrent)
    ));
    assert_eq!(
        raw_runtime.spawn_calls(),
        1,
        "changed and stale dispatches must be rejected before raw runtime IO"
    );
    assert_eq!(
        attempts.accept_calls(),
        1,
        "an expired new dispatch must fail before durable attempt acceptance"
    );
    assert_eq!(
        harness.repository.insert_calls(),
        1,
        "an expired new dispatch must fail before a second canonical session insert"
    );
    assert_eq!(harness.repository.session_count(), 1);
}

#[test]
fn dispatched_start_acknowledges_committed_receipt_through_durable_wrapper() {
    assert_dispatched_receipt_acknowledgement(false);
}

#[test]
fn dispatched_start_ack_failure_keeps_session_and_replay_retries_receipt() {
    assert_dispatched_receipt_acknowledgement(true);
}

fn assert_dispatched_receipt_acknowledgement(fail_first_ack: bool) {
    let harness = Harness::new();
    let raw_runtime = Arc::new(FakeRuntime::new());
    let attempts = Arc::new(DispatchAttemptStore::default());
    let id = undercurrent_domain::OperationAttemptId::new("attempt:receipt:7").unwrap();
    *raw_runtime.receipt_probe.lock().unwrap() = Some(ReceiptProbe {
        target: id.clone(),
        attempts: attempts.clone(),
        fail_acks: usize::from(fail_first_ack),
        pending: BTreeSet::new(),
        acknowledgements: Vec::new(),
    });
    let durable_runtime = crate::DurableProcessRuntime::new(
        raw_runtime.clone(),
        attempts.clone(),
        Arc::new(IncrementingClock::new(10_000)),
    );
    let mut request = command("receipt-test");
    request.dispatch = Some(crate::OperationAttemptAdmission::dispatched(
        id.clone(),
        None,
        undercurrent_domain::ProcessMutationKind::Spawn,
        undercurrent_domain::FencingToken::new(7).unwrap(),
        PayloadDigest::of_bytes(b"receipt-request"),
        runtime_dispatch_binding(
            "cell:receipt",
            b"launch-profile",
            b"environment",
            b"input",
            10_010,
        ),
        TimestampMillis::new(100),
    ));
    let service = StartSessionService::new(
        &harness.clock,
        &harness.ids,
        &harness.coordinator,
        &harness.repository,
        &durable_runtime,
        &harness.processes,
        &harness.catalog,
        &harness.telemetry,
    )
    .with_operation_attempt_store(attempts.as_ref());
    let first = block_on(service.execute(request.clone()));
    if fail_first_ack {
        assert!(matches!(
            first,
            Err(StartSessionError::DispatchPersistence(_))
        ));
    } else {
        first.expect("successful start acknowledgement");
    }
    let committed = attempts.attempt(&id).unwrap();
    let session_id = committed.target_session().unwrap();
    let live = block_on(harness.repository.load(session_id))
        .unwrap()
        .unwrap();
    assert_eq!(live.lifecycle(), LifecycleState::Running);
    assert!(harness.processes.lookup(session_id).unwrap().is_some());
    {
        let guard = raw_runtime.receipt_probe.lock().unwrap();
        let probe = guard.as_ref().unwrap();
        assert_eq!(probe.acknowledgements, vec![committed.clone()]);
        assert_eq!(probe.pending.contains(&id), fail_first_ack);
    }
    let replayed = block_on(service.execute(request.clone()))
        .expect("exact retry acknowledges the same receipt");
    assert_eq!(replayed.session, live);
    block_on(service.execute(request)).expect("repeated acknowledgement is idempotent");
    assert_eq!(raw_runtime.spawn_calls(), 1);
    assert_eq!(raw_runtime.stop_calls(), 0);
    assert_eq!(raw_runtime.force_calls(), 0);
    assert_eq!(attempts.accept_calls(), 1);
    assert_eq!(harness.repository.session_count(), 1);
    let guard = raw_runtime.receipt_probe.lock().unwrap();
    let probe = guard.as_ref().unwrap();
    assert_eq!(probe.acknowledgements, vec![committed; 3]);
    assert!(probe.pending.is_empty());
}

#[test]
fn dispatched_start_expiring_during_preparation_writes_no_session_attempt_or_process_effect() {
    let harness = Harness::new();
    let service_clock = Arc::new(IncrementingClock::new(190));
    let raw_runtime = Arc::new(FakeRuntime::new());
    raw_runtime.advance_clock_during_prepare(service_clock.clone(), 200);
    let attempts = Arc::new(DispatchAttemptStore::default());
    let durable_runtime = crate::DurableProcessRuntime::new(
        raw_runtime.clone(),
        attempts.clone(),
        service_clock.clone(),
    );
    let mut request = command("preparation-expiry");
    request.dispatch = Some(crate::OperationAttemptAdmission::dispatched(
        undercurrent_domain::OperationAttemptId::new("attempt:preparation-expiry:7")
            .expect("attempt"),
        None,
        undercurrent_domain::ProcessMutationKind::Spawn,
        undercurrent_domain::FencingToken::new(7).expect("fence"),
        undercurrent_domain::PayloadDigest::of_bytes(b"preparation-expiry-request"),
        runtime_dispatch_binding(
            "cell:preparation-expiry",
            b"launch-profile",
            b"environment-binding",
            b"input",
            200,
        ),
        TimestampMillis::new(100),
    ));
    let service = StartSessionService::new(
        service_clock.as_ref(),
        &harness.ids,
        &harness.coordinator,
        &harness.repository,
        &durable_runtime,
        &harness.processes,
        &harness.catalog,
        &harness.telemetry,
    )
    .with_operation_attempt_store(attempts.as_ref());

    assert!(matches!(
        block_on(service.execute(request)),
        Err(StartSessionError::DispatchPolicyNotCurrent)
    ));
    assert_eq!(harness.repository.insert_calls(), 0);
    assert_eq!(harness.repository.session_count(), 0);
    assert_eq!(attempts.accept_calls(), 0);
    assert_eq!(raw_runtime.spawn_calls(), 0);
}

#[test]
fn dispatched_start_recovers_response_loss_after_settlement_commit_without_cleanup_or_respawn() {
    let harness = Harness::new();
    let raw_runtime = Arc::new(FakeRuntime::new());
    let attempts = Arc::new(DispatchAttemptStore::default());
    let attempt_id =
        undercurrent_domain::OperationAttemptId::new("attempt:settle-loss:7").expect("attempt");
    attempts.fail_after_settlement_commit_once(attempt_id.clone());
    let durable_runtime = crate::DurableProcessRuntime::new(
        raw_runtime.clone(),
        attempts.clone(),
        Arc::new(IncrementingClock::new(15_000)),
    );
    let mut request = command("settle-response-loss");
    request.dispatch = Some(crate::OperationAttemptAdmission::dispatched(
        attempt_id,
        None,
        undercurrent_domain::ProcessMutationKind::Spawn,
        undercurrent_domain::FencingToken::new(7).expect("fence"),
        undercurrent_domain::PayloadDigest::of_bytes(b"canonical-settle-loss"),
        runtime_dispatch_binding(
            "cell:settle-loss",
            b"launch",
            b"environment",
            b"input",
            1_000_000,
        ),
        TimestampMillis::new(100),
    ));
    let service = StartSessionService::new(
        &harness.clock,
        &harness.ids,
        &harness.coordinator,
        &harness.repository,
        &durable_runtime,
        &harness.processes,
        &harness.catalog,
        &harness.telemetry,
    )
    .with_operation_attempt_store(attempts.as_ref());

    let started = block_on(service.execute(request.clone())).expect("commit was recovered");
    let replayed = block_on(service.execute(request)).expect("exact retry replays");

    assert_eq!(replayed, started);
    assert_eq!(raw_runtime.spawn_calls(), 1);
    assert_eq!(raw_runtime.force_calls(), 0);
    assert!(raw_runtime.is_active(started.session.id(), &started.process.id));
}

#[test]
fn higher_dispatch_survives_exact_low_fence_compensation_without_orphan_process() {
    let harness = Harness::new();
    let raw_runtime = Arc::new(FakeRuntime::new());
    let attempts = Arc::new(DispatchAttemptStore::default());
    let durable_runtime = crate::DurableProcessRuntime::new(
        raw_runtime.clone(),
        attempts.clone(),
        Arc::new(IncrementingClock::new(20_000)),
    );
    let low_attempt_id =
        undercurrent_domain::OperationAttemptId::new("overlap:low:7").expect("low attempt");
    let high_attempt_id =
        undercurrent_domain::OperationAttemptId::new("overlap:high:8").expect("high attempt");
    let low_session_id = SessionId::new("session-low").expect("low session");
    let high_session_id = SessionId::new("session-high").expect("high session");
    let low_ids = FixedIds::new(low_session_id.as_str());
    let high_ids = FixedIds::new(high_session_id.as_str());
    let binding = |input: &'static [u8]| {
        runtime_dispatch_binding(
            "cell:overlap",
            b"launch-profile",
            b"environment-binding",
            input,
            1_000_000,
        )
    };
    let mut low_command = command("overlap-low");
    low_command.dispatch = Some(crate::OperationAttemptAdmission::dispatched(
        low_attempt_id.clone(),
        None,
        undercurrent_domain::ProcessMutationKind::Spawn,
        undercurrent_domain::FencingToken::new(7).expect("low fence"),
        undercurrent_domain::PayloadDigest::of_bytes(b"low canonical request"),
        binding(b"low input"),
        TimestampMillis::new(100),
    ));
    let low_replay = low_command.clone();
    let mut high_command = command("overlap-high");
    high_command.dispatch = Some(crate::OperationAttemptAdmission::dispatched(
        high_attempt_id,
        None,
        undercurrent_domain::ProcessMutationKind::Spawn,
        undercurrent_domain::FencingToken::new(8).expect("high fence"),
        undercurrent_domain::PayloadDigest::of_bytes(b"high canonical request"),
        binding(b"high input"),
        TimestampMillis::new(101),
    ));
    attempts.block_settlement(low_attempt_id.clone());

    let (low_result, high_started) = thread::scope(|scope| {
        let low_service = StartSessionService::new(
            &harness.clock,
            &low_ids,
            &harness.coordinator,
            &harness.repository,
            &durable_runtime,
            &harness.processes,
            &harness.catalog,
            &harness.telemetry,
        )
        .with_operation_attempt_store(attempts.as_ref());
        let low = scope.spawn(move || block_on(low_service.execute(low_command)).map_err(Box::new));
        attempts.wait_until_settlement_is_blocked();

        let high_service = StartSessionService::new(
            &harness.clock,
            &high_ids,
            &harness.coordinator,
            &harness.repository,
            &durable_runtime,
            &harness.processes,
            &harness.catalog,
            &harness.telemetry,
        )
        .with_operation_attempt_store(attempts.as_ref());
        let high_started = block_on(high_service.execute(high_command)).expect("higher dispatch");
        attempts.release_blocked_settlement();
        let low_result = low.join().expect("low dispatch thread");
        (low_result, high_started)
    });

    let Err(error) = low_result else {
        panic!("low dispatch must be compensated after stale settlement");
    };
    let StartSessionError::PartialStart {
        stage: StartFailureStage::CommitDispatch,
        recovery,
        ..
    } = *error
    else {
        panic!("low dispatch must be compensated after stale settlement");
    };
    assert!(recovery.completed());
    assert!(recovery.cleanup_required_process().is_none());
    let low_durable = harness.repository.load_sync(&low_session_id);
    assert_eq!(low_durable.lifecycle(), LifecycleState::Failed);
    assert!(low_durable.process().is_none());
    assert!(
        harness
            .processes
            .lookup(&low_session_id)
            .expect("low binding lookup")
            .is_none()
    );
    assert!(!raw_runtime.is_active(&low_session_id, &high_started.process.id));

    let high_durable = harness.repository.load_sync(&high_session_id);
    assert_eq!(high_durable.lifecycle(), LifecycleState::Running);
    assert_eq!(
        high_durable.process().expect("high owner").id(),
        &high_started.process.id
    );
    assert_eq!(
        harness
            .processes
            .lookup(&high_session_id)
            .expect("high binding lookup"),
        Some(high_started.process.clone())
    );
    assert!(raw_runtime.is_active(&high_session_id, &high_started.process.id));
    assert_eq!(raw_runtime.spawn_calls(), 2);
    assert_eq!(raw_runtime.force_calls(), 1);
    assert!(matches!(
        attempts
            .attempt(&low_attempt_id)
            .expect("low attempt remains audited")
            .state(),
        undercurrent_domain::OperationAttemptState::Compensated {
            superseding_fencing_token,
            ..
        } if *superseding_fencing_token
            == undercurrent_domain::FencingToken::new(8).expect("high fence")
    ));

    let replay_service = StartSessionService::new(
        &harness.clock,
        &low_ids,
        &harness.coordinator,
        &harness.repository,
        &durable_runtime,
        &harness.processes,
        &harness.catalog,
        &harness.telemetry,
    )
    .with_operation_attempt_store(attempts.as_ref());
    assert!(matches!(
        block_on(replay_service.execute(low_replay)),
        Err(StartSessionError::DispatchConflict)
    ));
    assert_eq!(raw_runtime.spawn_calls(), 2, "low replay must not respawn");
}

#[test]
fn final_dispatch_compensation_failure_retains_exact_cleanup_required_process() {
    let harness = Harness::new();
    let raw_runtime = Arc::new(FakeRuntime::new());
    let attempts = Arc::new(DispatchAttemptStore::default());
    attempts.fail_compensation_once();
    let cell = undercurrent_domain::DispatchCellIdentity::new("cell:audit-failure").expect("cell");
    let binding = runtime_dispatch_binding(
        cell.as_str(),
        b"launch",
        b"environment",
        b"high-input",
        1_000_000,
    );
    attempts.supersede_on_settlement(
        undercurrent_domain::OperationAttempt::accepted_with_dispatch(
            undercurrent_domain::OperationAttemptId::new("audit:high:8").expect("high attempt"),
            Some(SessionId::new("session-high-audit").expect("high session")),
            undercurrent_domain::ProcessMutationKind::Spawn,
            undercurrent_domain::FencingToken::new(8).expect("high fence"),
            undercurrent_domain::PayloadDigest::of_bytes(b"high request"),
            Some(binding),
            TimestampMillis::new(101),
        ),
    );
    let durable_runtime = crate::DurableProcessRuntime::new(
        raw_runtime.clone(),
        attempts.clone(),
        Arc::new(IncrementingClock::new(25_000)),
    );
    let low_attempt_id =
        undercurrent_domain::OperationAttemptId::new("audit:low:7").expect("low attempt");
    let mut request = command("audit-low");
    request.dispatch = Some(crate::OperationAttemptAdmission::dispatched(
        low_attempt_id.clone(),
        None,
        undercurrent_domain::ProcessMutationKind::Spawn,
        undercurrent_domain::FencingToken::new(7).expect("low fence"),
        undercurrent_domain::PayloadDigest::of_bytes(b"low request"),
        runtime_dispatch_binding(
            cell.as_str(),
            b"launch",
            b"environment",
            b"low-input",
            1_000_000,
        ),
        TimestampMillis::new(100),
    ));
    let service = StartSessionService::new(
        &harness.clock,
        &harness.ids,
        &harness.coordinator,
        &harness.repository,
        &durable_runtime,
        &harness.processes,
        &harness.catalog,
        &harness.telemetry,
    )
    .with_operation_attempt_store(attempts.as_ref());

    let error = block_on(service.execute(request)).expect_err("audit commit fails");
    let StartSessionError::PartialStart {
        stage: StartFailureStage::CommitDispatch,
        recovery,
        ..
    } = error
    else {
        panic!("expected final dispatch recovery failure");
    };
    let retained = recovery
        .cleanup_required_process()
        .expect("exact cleanup evidence retained");
    assert_eq!(
        retained.id,
        ProcessInstanceId::new("process-1").expect("process")
    );
    assert!(recovery.settlement_failure().is_some());
    assert!(!raw_runtime.is_active(&SessionId::new("session-1").expect("session"), &retained.id));
    assert!(matches!(
        attempts
            .attempt(&low_attempt_id)
            .expect("low attempt")
            .state(),
        undercurrent_domain::OperationAttemptState::Accepted
    ));
}

#[test]
fn name_conflict_does_not_spawn_a_process() {
    let harness = Harness::new();
    harness.repository.reserve_name(
        SessionName::new("build").expect("name"),
        SessionId::new("existing").expect("id"),
    );

    let error = block_on(harness.service().execute(command("build"))).expect_err("must conflict");

    assert!(matches!(error, StartSessionError::NameConflict { .. }));
    assert_eq!(harness.runtime.spawn_calls(), 0);
    assert_eq!(harness.repository.session_count(), 0);
}

#[test]
fn start_ordering_failure_stops_before_durable_or_process_io() {
    let harness = Harness::new();
    let coordinator = FailingMutationCoordinator;
    let service = StartSessionService::new(
        &harness.clock,
        &harness.ids,
        &coordinator,
        &harness.repository,
        &harness.runtime,
        &harness.processes,
        &harness.catalog,
        &harness.telemetry,
    );

    let error = block_on(service.execute(command("build"))).expect_err("ordering must fail");

    assert!(matches!(error, StartSessionError::Ordering(_)));
    assert_eq!(harness.repository.session_count(), 0);
    assert_eq!(harness.runtime.spawn_calls(), 0);
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::PortOperationFailed)
    );
}

#[test]
fn spawn_failure_is_settled_as_failed_without_exposing_source_text() {
    let harness = Harness::new();
    harness.runtime.fail_spawn.store(1, Ordering::SeqCst);

    let error = block_on(harness.service().execute(command("build"))).expect_err("spawn fails");

    let StartSessionError::Spawn { source, settlement } = &error else {
        panic!("unexpected error: {error}");
    };
    assert!(settlement.is_none());
    assert!(!format!("{source}").contains("private-adapter-detail"));
    assert!(!format!("{source:?}").contains("private-adapter-detail"));
    assert!(source.source().is_some());
    let durable = harness.repository.only_session();
    assert_eq!(durable.lifecycle(), LifecycleState::Failed);
    assert_eq!(
        harness
            .catalog
            .get(durable.id())
            .expect("catalog readable")
            .expect("summary")
            .lifecycle(),
        LifecycleState::Failed
    );
}

#[test]
fn running_commit_failure_kills_unbinds_and_reconciles_the_process() {
    let harness = Harness::new();
    harness.repository.fail_on_commit_from_now(2);

    let error = block_on(harness.service().execute(command("build"))).expect_err("commit fails");

    let StartSessionError::PartialStart {
        stage, recovery, ..
    } = error
    else {
        panic!("unexpected start result");
    };
    assert_eq!(stage, StartFailureStage::CommitRunning);
    assert!(recovery.completed());
    assert_eq!(harness.runtime.force_calls(), 1);
    let durable = harness.repository.only_session();
    assert_eq!(durable.lifecycle(), LifecycleState::Failed);
    assert_eq!(
        harness.processes.lookup(durable.id()).expect("lookup"),
        None
    );
    assert_eq!(
        harness.repository.event_kinds(),
        vec![
            DomainEventKind::SessionCreated,
            DomainEventKind::ProcessOwnershipChanged,
            DomainEventKind::LifecycleChanged
        ]
    );
}

#[test]
fn exit_observation_is_armed_only_after_binding_and_arm_failure_is_compensated() {
    let harness = Harness::new();
    harness.runtime.fail_arm.store(1, Ordering::SeqCst);

    let error =
        block_on(harness.service().execute(command("build"))).expect_err("arming must fail");

    let StartSessionError::PartialStart {
        stage, recovery, ..
    } = error
    else {
        panic!("unexpected start result");
    };
    assert_eq!(stage, StartFailureStage::ArmProcessExit);
    assert!(recovery.completed());
    assert_eq!(harness.runtime.arm_calls(), 1);
    assert_eq!(harness.runtime.force_calls(), 1);
    let durable = harness.repository.only_session();
    assert_eq!(durable.lifecycle(), LifecycleState::Failed);
    assert_eq!(
        harness.processes.lookup(durable.id()).expect("lookup"),
        None
    );
}

#[test]
fn failed_compensation_cleanup_retains_durable_process_truth() {
    let harness = Harness::new();
    harness.repository.fail_on_commit_from_now(2);
    harness
        .processes
        .fail_next_unbind
        .store(1, Ordering::SeqCst);

    let error = block_on(harness.service().execute(command("build"))).expect_err("commit fails");

    let StartSessionError::PartialStart { recovery, .. } = error else {
        panic!("unexpected start result");
    };
    assert!(recovery.unbind_failure().is_some());
    assert!(recovery.settlement_failure().is_none());
    assert!(!recovery.completed());
    let durable = harness.repository.only_session();
    assert_eq!(durable.lifecycle(), LifecycleState::Starting);
    assert!(durable.process().is_some());
    assert!(
        harness
            .processes
            .lookup(durable.id())
            .expect("lookup")
            .is_some()
    );
}

#[test]
fn uncertain_termination_preserves_process_ownership_and_nonterminal_state() {
    let harness = Harness::new();
    harness.repository.fail_on_commit_from_now(2);
    harness.runtime.fail_force.store(1, Ordering::SeqCst);

    let error = block_on(harness.service().execute(command("build"))).expect_err("commit fails");

    let StartSessionError::PartialStart { recovery, .. } = error else {
        panic!("unexpected start result");
    };
    assert!(recovery.force_stop_failure().is_some());
    assert!(recovery.cleanup_required_process().is_some());
    assert!(!recovery.completed());
    let durable = harness.repository.only_session();
    assert_eq!(durable.lifecycle(), LifecycleState::Starting);
    assert_eq!(
        durable
            .process()
            .expect("uncertain process remains durable")
            .id(),
        &ProcessInstanceId::new("process-1").expect("process ID")
    );
    assert!(
        harness
            .processes
            .lookup(durable.id())
            .expect("lookup")
            .is_some()
    );
}

#[test]
fn projection_failure_does_not_report_false_failure_after_durable_start() {
    let harness = Harness::new();
    let projection = FailingProjection;
    let service = StartSessionService::new(
        &harness.clock,
        &harness.ids,
        &harness.coordinator,
        &harness.repository,
        &harness.runtime,
        &harness.processes,
        &projection,
        &harness.telemetry,
    );

    let started = block_on(service.execute(command("build"))).expect("durable start succeeds");

    assert_eq!(
        harness
            .repository
            .load_sync(started.session.id())
            .lifecycle(),
        LifecycleState::Running
    );
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::PortOperationFailed)
    );
}

#[test]
fn graceful_stop_commits_stopping_and_confirmed_exit_under_one_lease() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");

    let result = block_on(harness.stop_service().execute(StopSessionCommand {
        expected_revision: None,
        session_id: started.session.id().clone(),
        mode: StopMode::Graceful,
    }))
    .expect("stop succeeds");

    let StopSessionResult::Terminated {
        session,
        termination,
    } = result
    else {
        panic!("live session must terminate");
    };
    assert_eq!(termination.kind(), TerminationKind::Graceful);
    assert_eq!(session.lifecycle(), LifecycleState::Exited(0));
    assert!(session.process().is_none());
    assert_eq!(session.revision(), Revision::new(5));
    assert_eq!(harness.runtime.stop_calls(), 1);
    assert_eq!(
        harness.processes.lookup(session.id()).expect("lookup"),
        None
    );
    assert_eq!(
        harness.repository.event_kinds(),
        vec![
            DomainEventKind::SessionCreated,
            DomainEventKind::ProcessOwnershipChanged,
            DomainEventKind::LifecycleChanged,
            DomainEventKind::LifecycleChanged,
            DomainEventKind::LifecycleChanged,
        ]
    );
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::TerminalLifecycleCommitted)
    );
}

#[test]
fn failed_stop_keeps_stopping_state_and_process_ownership_for_recovery() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    harness.runtime.fail_stop.store(1, Ordering::SeqCst);

    let error = block_on(harness.stop_service().execute(StopSessionCommand {
        expected_revision: None,
        session_id: started.session.id().clone(),
        mode: StopMode::Graceful,
    }))
    .expect_err("termination remains uncertain");

    let StopSessionError::Termination(source) = &error else {
        panic!("unexpected stop error: {error}");
    };
    assert!(!source.to_string().contains("private-adapter-detail"));
    let durable = harness.repository.load_sync(started.session.id());
    assert_eq!(durable.lifecycle(), LifecycleState::Stopping);
    assert!(
        harness
            .processes
            .lookup(started.session.id())
            .expect("lookup")
            .is_some()
    );
}

#[test]
fn explicit_restart_replaces_active_process_and_preserves_session_configuration() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    harness
        .runtime
        .spawn_started_at
        .store(500, Ordering::SeqCst);

    let restarted = block_on(
        harness
            .manual_restart_service()
            .execute(RestartSessionCommand {
                stop_mode: StopMode::Graceful,
                session_id: started.session.id().clone(),
            }),
    )
    .expect("restart succeeds");

    assert_eq!(restarted.session.id(), started.session.id());
    assert_eq!(restarted.session.name(), started.session.name());
    assert_eq!(
        restarted.session.configuration(),
        started.session.configuration()
    );
    assert_eq!(restarted.session.lifecycle(), LifecycleState::Running);
    assert_eq!(restarted.session.restart_status(), RestartStatus::Idle);
    assert_eq!(restarted.session.revision(), Revision::new(8));
    assert_eq!(restarted.process.started_at, TimestampMillis::new(500));
    assert!(restarted.previous_termination.is_some());
    assert_eq!(harness.runtime.spawn_calls(), 2);
    assert_eq!(harness.runtime.stop_calls(), 1);
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::SessionRestartSucceeded)
    );
}

#[test]
fn explicit_restart_starts_terminal_session_without_second_stop() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    block_on(harness.stop_service().execute(StopSessionCommand {
        expected_revision: None,
        session_id: started.session.id().clone(),
        mode: StopMode::Graceful,
    }))
    .expect("stop succeeds");

    let restarted = block_on(
        harness
            .manual_restart_service()
            .execute(RestartSessionCommand {
                stop_mode: StopMode::Graceful,
                session_id: started.session.id().clone(),
            }),
    )
    .expect("restart succeeds");

    assert_eq!(restarted.session.lifecycle(), LifecycleState::Running);
    assert!(restarted.previous_termination.is_none());
    assert_eq!(harness.runtime.spawn_calls(), 2);
    assert_eq!(harness.runtime.stop_calls(), 1);
}

#[test]
fn explicit_restart_cancels_pending_automatic_restart_before_activation() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command_with_restart(
        "build",
        RestartPolicy::Always,
        2,
    )))
    .expect("start succeeds");
    block_on(harness.exit_service().execute(ProcessExitObservation {
        session_id: started.session.id().clone(),
        process_id: started.process.id,
        outcome: ProcessOutcome::Exited(1),
        observed_at: TimestampMillis::new(1_000),
    }))
    .expect("exit schedules restart");

    let restarted = block_on(
        harness
            .manual_restart_service()
            .execute(RestartSessionCommand {
                stop_mode: StopMode::Graceful,
                session_id: started.session.id().clone(),
            }),
    )
    .expect("restart succeeds");

    assert_eq!(restarted.session.lifecycle(), LifecycleState::Running);
    assert_eq!(restarted.session.restart_status(), RestartStatus::Idle);
    assert!(harness.restarts.jobs().is_empty());
    assert_eq!(harness.restarts.cancel_calls(), 1);
    assert_eq!(harness.runtime.stop_calls(), 0);
}

#[test]
fn explicit_restart_spawn_failure_settles_failed_without_auto_reschedule() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    harness.runtime.fail_spawn.store(1, Ordering::SeqCst);

    let error = block_on(
        harness
            .manual_restart_service()
            .execute(RestartSessionCommand {
                stop_mode: StopMode::Graceful,
                session_id: started.session.id().clone(),
            }),
    )
    .expect_err("replacement spawn fails");

    let RestartSessionError::Activation {
        stage,
        session,
        recovery,
        ..
    } = error
    else {
        panic!("unexpected restart error");
    };
    assert_eq!(stage, RestartFailureStage::Spawn);
    assert!(recovery.completed());
    assert_eq!(session.lifecycle(), LifecycleState::Failed);
    assert!(session.process().is_none());
    assert!(harness.restarts.jobs().is_empty());
    assert_eq!(harness.runtime.spawn_calls(), 2);
    assert_eq!(harness.runtime.stop_calls(), 1);
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::SessionRestartFailed)
    );
}

#[test]
fn explicit_restart_stop_failure_does_not_spawn_replacement() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    harness.runtime.fail_stop.store(1, Ordering::SeqCst);

    let error = block_on(
        harness
            .manual_restart_service()
            .execute(RestartSessionCommand {
                stop_mode: StopMode::Graceful,
                session_id: started.session.id().clone(),
            }),
    )
    .expect_err("stop fails");

    assert!(matches!(
        error,
        RestartSessionError::Stop(StopSessionError::Termination(_))
    ));
    assert_eq!(harness.runtime.spawn_calls(), 1);
    let durable = harness.repository.load_sync(started.session.id());
    assert_eq!(durable.lifecycle(), LifecycleState::Stopping);
    assert!(durable.process().is_some());
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::SessionRestartFailed)
    );
}

#[test]
fn explicit_restart_rejects_stale_terminal_binding_without_spawning() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    block_on(harness.stop_service().execute(StopSessionCommand {
        expected_revision: None,
        session_id: started.session.id().clone(),
        mode: StopMode::Graceful,
    }))
    .expect("stop succeeds");
    harness
        .processes
        .bind(
            started.session.id(),
            ProcessInstance {
                id: ProcessInstanceId::new("stale-process").expect("process ID"),
                started_at: TimestampMillis::new(500),
            },
        )
        .expect("stale binding fixture");

    let error = block_on(
        harness
            .manual_restart_service()
            .execute(RestartSessionCommand {
                stop_mode: StopMode::Graceful,
                session_id: started.session.id().clone(),
            }),
    )
    .expect_err("stale binding blocks restart");

    assert!(matches!(
        error,
        RestartSessionError::ProcessAlreadyBound { .. }
    ));
    assert_eq!(harness.runtime.spawn_calls(), 1);
    assert_eq!(
        harness
            .repository
            .load_sync(started.session.id())
            .lifecycle(),
        LifecycleState::Exited(0)
    );
}

#[test]
fn explicit_restart_rejects_intervening_mutation_after_stop() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    let coordinator = InterveningMutationCoordinator {
        repository: &harness.repository,
        acquisitions: AtomicUsize::new(0),
    };
    let service = RestartSessionService::new(
        &harness.clock,
        &coordinator,
        &harness.repository,
        &harness.runtime,
        &harness.processes,
        &harness.restarts,
        &harness.catalog,
        &harness.telemetry,
    );

    let error = block_on(service.execute(RestartSessionCommand {
        stop_mode: StopMode::Graceful,
        session_id: started.session.id().clone(),
    }))
    .expect_err("intervening mutation conflicts");

    assert!(matches!(
        error,
        RestartSessionError::ChangedAfterStop { .. }
    ));
    assert_eq!(harness.runtime.spawn_calls(), 1);
    assert_eq!(harness.runtime.stop_calls(), 1);
    let durable = harness.repository.load_sync(started.session.id());
    assert!(durable.lifecycle().is_terminal());
    assert!(durable.process().is_none());
}

#[test]
fn repeated_stop_of_terminal_session_is_idempotent_without_process_io() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    let command = StopSessionCommand {
        expected_revision: None,
        session_id: started.session.id().clone(),
        mode: StopMode::Graceful,
    };
    block_on(harness.stop_service().execute(command.clone())).expect("first stop succeeds");

    let repeated = block_on(harness.stop_service().execute(command)).expect("repeat succeeds");

    assert!(matches!(
        repeated,
        StopSessionResult::AlreadyTerminal { .. }
    ));
    assert_eq!(harness.runtime.stop_calls(), 1);
    assert_eq!(harness.restarts.cancel_calls(), 2);
}

#[test]
fn explicit_stop_cancels_durable_and_queued_automatic_restart() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command_with_restart(
        "build",
        RestartPolicy::Always,
        2,
    )))
    .expect("start succeeds");
    block_on(harness.exit_service().execute(ProcessExitObservation {
        session_id: started.session.id().clone(),
        process_id: started.process.id,
        outcome: ProcessOutcome::Exited(1),
        observed_at: TimestampMillis::new(1_000),
    }))
    .expect("exit schedules restart");
    assert_eq!(harness.restarts.jobs().len(), 1);

    let result = block_on(harness.stop_service().execute(StopSessionCommand {
        expected_revision: None,
        session_id: started.session.id().clone(),
        mode: StopMode::Graceful,
    }))
    .expect("explicit stop succeeds");

    let StopSessionResult::AlreadyTerminal { session } = result else {
        panic!("exited session is already terminal");
    };
    assert_eq!(session.revision(), Revision::new(6));
    assert_eq!(session.restart_tracker().consecutive_attempts(), 1);
    assert_eq!(
        session.restart_status(),
        RestartStatus::NotScheduled {
            reason: NoRestartReason::ExplicitStop,
        }
    );
    assert!(harness.restarts.jobs().is_empty());
    assert_eq!(harness.restarts.cancel_calls(), 1);
    assert_eq!(harness.runtime.stop_calls(), 0);
    assert_eq!(
        harness.repository.event_kinds().last(),
        Some(&DomainEventKind::RestartCancelled)
    );
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::RestartCancellationCommitted)
    );
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::RestartQueueCancellationCompleted)
    );
}

#[test]
fn explicit_stop_survives_restart_queue_cleanup_failure_and_retries_cleanup() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command_with_restart(
        "build",
        RestartPolicy::Always,
        2,
    )))
    .expect("start succeeds");
    block_on(harness.exit_service().execute(ProcessExitObservation {
        session_id: started.session.id().clone(),
        process_id: started.process.id,
        outcome: ProcessOutcome::Exited(1),
        observed_at: TimestampMillis::new(1_000),
    }))
    .expect("exit schedules restart");
    harness.restarts.fail_next_cancel.store(1, Ordering::SeqCst);
    let command = StopSessionCommand {
        expected_revision: None,
        session_id: started.session.id().clone(),
        mode: StopMode::Graceful,
    };

    let first = block_on(harness.stop_service().execute(command.clone()))
        .expect("durable cancellation is authoritative");

    let StopSessionResult::AlreadyTerminal { session } = first else {
        panic!("exited session is already terminal");
    };
    assert_eq!(
        session.restart_status(),
        RestartStatus::NotScheduled {
            reason: NoRestartReason::ExplicitStop,
        }
    );
    assert_eq!(harness.restarts.jobs().len(), 1);
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::RestartQueueCancellationFailed)
    );

    block_on(harness.stop_service().execute(command)).expect("cleanup retry succeeds");
    assert!(harness.restarts.jobs().is_empty());
    assert_eq!(harness.restarts.cancel_calls(), 2);
}

#[test]
fn force_stop_uses_immediate_force_port_and_commits_stopped_without_exit_code() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");

    let result = block_on(harness.stop_service().execute(StopSessionCommand {
        expected_revision: None,
        session_id: started.session.id().clone(),
        mode: StopMode::Force,
    }))
    .expect("force stop succeeds");

    let StopSessionResult::Terminated { session, .. } = result else {
        panic!("live session must terminate");
    };
    assert_eq!(session.lifecycle(), LifecycleState::Stopped);
    assert_eq!(harness.runtime.force_calls(), 1);
    assert_eq!(harness.runtime.stop_calls(), 0);
}

#[test]
fn terminal_settlement_retains_primary_and_final_reconciliation_failures() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    harness
        .repository
        .fail_session_revision
        .store(5, Ordering::SeqCst);

    let error = block_on(harness.stop_service().execute(StopSessionCommand {
        expected_revision: None,
        session_id: started.session.id().clone(),
        mode: StopMode::Graceful,
    }))
    .expect_err("terminal commits fail");

    let StopSessionError::TerminalSettlement(settlement) = error else {
        panic!("unexpected stop result");
    };
    assert!(matches!(
        settlement.primary(),
        SessionMutationError::Commit(_)
    ));
    assert!(matches!(
        settlement.reconciliation(),
        Some(SessionMutationError::Commit(_))
    ));
    assert_eq!(
        harness
            .repository
            .load_sync(started.session.id())
            .lifecycle(),
        LifecycleState::Stopping
    );
    assert_eq!(
        harness
            .processes
            .lookup(started.session.id())
            .expect("lookup"),
        None
    );
}

#[test]
fn rename_preserves_stable_identity_and_is_idempotent() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    let original_id = started.session.id().clone();
    let rename = RenameSessionCommand {
        session_id: original_id.clone(),
        name: SessionName::new("compile").expect("name is valid"),
    };

    let renamed =
        block_on(harness.rename_service().execute(rename.clone())).expect("rename succeeds");
    assert!(renamed.changed);
    assert_eq!(renamed.session.id(), &original_id);
    assert_eq!(renamed.session.name().as_str(), "compile");
    assert_eq!(
        harness.repository.load_sync(&original_id).name().as_str(),
        "compile"
    );
    assert_eq!(
        harness
            .catalog
            .get(&original_id)
            .expect("catalog lookup succeeds")
            .expect("summary exists")
            .name()
            .as_str(),
        "compile"
    );

    let repeated =
        block_on(harness.rename_service().execute(rename)).expect("repeat rename succeeds");
    assert!(!repeated.changed);
    assert_eq!(repeated.session.revision(), renamed.session.revision());
}

#[test]
fn label_updates_persist_without_changing_process_configuration_or_identity() {
    let harness = Harness::new();
    let mut start = command("labels");
    start.configuration = start.configuration.with_labels(
        Labels::new([
            ("app".into(), "example-app".into()),
            ("workflow.run_id".into(), "run-1".into()),
        ])
        .unwrap(),
    );
    let started = block_on(harness.service().execute(start)).unwrap();
    let service = UpdateSessionLabelsService::new(
        &harness.clock,
        &harness.coordinator,
        &harness.repository,
        &harness.catalog,
        &harness.telemetry,
    );
    let mark = UpdateSessionLabelsCommand {
        session_id: started.session.id().clone(),
        patch: undercurrent_domain::LabelPatch::new(
            Labels::new([("aixand.my_terminal".into(), "true".into())]).unwrap(),
            [],
        )
        .unwrap(),
    };
    let updated = block_on(service.execute(mark.clone())).unwrap();
    assert!(updated.changed);
    assert_eq!(updated.session.id(), started.session.id());
    assert_eq!(updated.session.process(), started.session.process());
    assert_eq!(
        updated.session.lifecycle_revision(),
        started.session.lifecycle_revision()
    );
    assert_eq!(
        updated.session.configuration().launch(),
        started.session.configuration().launch()
    );
    assert_eq!(
        updated.session.configuration().metadata(),
        started.session.configuration().metadata()
    );
    assert_eq!(updated.session.labels().get("app"), Some("example-app"));
    assert_eq!(
        updated.session.labels().get("workflow.run_id"),
        Some("run-1")
    );
    assert_eq!(
        harness.repository.load_sync(started.session.id()).labels(),
        updated.session.labels()
    );
    assert_eq!(
        harness
            .catalog
            .get(started.session.id())
            .unwrap()
            .unwrap()
            .labels(),
        updated.session.labels()
    );
    let repeated = block_on(service.execute(mark)).unwrap();
    assert!(!repeated.changed);
    assert_eq!(repeated.session.revision(), updated.session.revision());
    let unmarked = block_on(
        service.execute(UpdateSessionLabelsCommand {
            session_id: started.session.id().clone(),
            patch: undercurrent_domain::LabelPatch::new(
                Labels::default(),
                ["aixand.my_terminal".into()],
            )
            .unwrap(),
        }),
    )
    .unwrap();
    assert!(unmarked.changed);
    assert_eq!(
        unmarked.session.configuration(),
        started.session.configuration()
    );
    assert_eq!(harness.runtime.arm_calls(), 1);
    assert_eq!(
        harness.repository.event_kinds().last(),
        Some(&DomainEventKind::SessionConfigurationChanged)
    );
}

#[test]
fn overflowing_label_patch_does_not_commit_or_change_the_process() {
    let harness = Harness::new();
    let mut start = command("full-labels");
    start.configuration = start.configuration.with_labels(
        Labels::new(
            (0..undercurrent_domain::MAX_LABELS).map(|n| (format!("key-{n}"), "value".into())),
        )
        .unwrap(),
    );
    let started = block_on(harness.service().execute(start)).unwrap();
    let service = UpdateSessionLabelsService::new(
        &harness.clock,
        &harness.coordinator,
        &harness.repository,
        &harness.catalog,
        &harness.telemetry,
    );
    assert!(
        block_on(
            service.execute(UpdateSessionLabelsCommand {
                session_id: started.session.id().clone(),
                patch: undercurrent_domain::LabelPatch::new(
                    Labels::new([("extra".into(), "value".into())]).unwrap(),
                    [],
                )
                .unwrap(),
            })
        )
        .is_err()
    );
    assert_eq!(
        harness.repository.load_sync(started.session.id()),
        started.session
    );
    assert_eq!(harness.runtime.arm_calls(), 1);
}

#[test]
fn rename_preserves_identity_active_subscription_and_work_context_session_targets() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    let original_id = started.session.id().clone();
    let item_key = WorkItemKey::new(
        WorkOwner::new("workflow").expect("owner is valid"),
        WorkItemId::new("self-reference").expect("work ID is valid"),
    );
    let item = WorkItem::new(
        item_key.clone(),
        WorkRelationKind::Related,
        "session identity reference",
        None,
        WorkStatus::Running,
        [undercurrent_domain::WorkTarget::Session(
            original_id.clone(),
        )],
        Metadata::default(),
        TimestampMillis::new(100),
        TimestampMillis::new(100),
        None,
    )
    .expect("work item is valid");
    let with_work = block_on(harness.work_service().upsert(UpsertWorkItemCommand {
        session_id: original_id.clone(),
        item,
    }))
    .expect("work context upsert succeeds")
    .session;
    let work_revision = with_work.work_context().revision();

    let subscriptions = SessionSubscriptionHub::new(PreparedReadModels::new(), 1)
        .expect("subscription hub is valid");
    subscriptions
        .upsert_session(&with_work)
        .expect("initial projection succeeds");
    let filter = undercurrent_domain::SessionFilter {
        ids: BTreeSet::from([original_id.clone()]),
        ..undercurrent_domain::SessionFilter::default()
    };
    let watch = subscriptions
        .subscribe(undercurrent_domain::SessionQuery {
            filter,
            sort: undercurrent_domain::SessionSort::default(),
            projection: undercurrent_domain::SessionProjection::Compact,
            page_size: undercurrent_domain::PageSize::new(1, 1).expect("page size is valid"),
            include_total: false,
        })
        .expect("subscription opens before rename");
    let initial = watch.snapshot().expect("initial subscription snapshot");

    let service = RenameSessionService::new(
        &harness.clock,
        &harness.coordinator,
        &harness.repository,
        &subscriptions,
        &harness.telemetry,
    );
    let renamed = block_on(service.execute(RenameSessionCommand {
        session_id: original_id.clone(),
        name: SessionName::new("compile").expect("name is valid"),
    }))
    .expect("rename succeeds through the subscribed projection");

    assert_eq!(renamed.session.id(), &original_id);
    assert_eq!(renamed.session.work_context().revision(), work_revision);
    assert_eq!(
        renamed.session.work_context().direct_session_targets(),
        BTreeSet::from([original_id.clone()])
    );
    assert_eq!(
        renamed
            .session
            .work_context()
            .get(&item_key)
            .expect("ID-based WorkContext item remains")
            .key(),
        &item_key
    );
    assert_eq!(subscriptions.pressure().subscriptions, 1);

    let batch = watch
        .try_recv()
        .expect("subscription remains readable")
        .expect("rename publishes one subscription delta");
    assert_eq!(batch.from_revision, initial.revision);
    assert!(batch.to_revision > batch.from_revision);
    assert_eq!(batch.deltas.len(), 1);
    assert_eq!(batch.deltas[0].session_id(), &original_id);
    assert_eq!(
        batch.deltas[0]
            .summary()
            .expect("rename is an updated summary")
            .name()
            .as_str(),
        "compile"
    );
}

#[test]
fn rename_reports_the_existing_owner_of_a_unique_name() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    let existing = SessionId::new("existing").expect("ID is valid");
    harness.repository.reserve_name(
        SessionName::new("taken").expect("name is valid"),
        existing.clone(),
    );

    let error = block_on(harness.rename_service().execute(RenameSessionCommand {
        session_id: started.session.id().clone(),
        name: SessionName::new("taken").expect("name is valid"),
    }))
    .expect_err("rename must conflict");

    assert!(matches!(
        error,
        SessionMutationError::NameConflict {
            existing_session_id,
            ..
        } if existing_session_id == existing
    ));
    assert_eq!(
        harness
            .repository
            .load_sync(started.session.id())
            .name()
            .as_str(),
        "build"
    );
}

#[test]
fn removal_requires_terminal_lifecycle_and_commits_event_with_deletion() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    let session_id = started.session.id().clone();
    let active_error = block_on(harness.remove_service().execute(RemoveSessionCommand {
        session_id: session_id.clone(),
        confirmation: RemovalConfirmation::Confirmed,
    }))
    .expect_err("running session cannot be removed");
    assert!(matches!(active_error, SessionMutationError::Domain(_)));
    assert_eq!(harness.repository.session_count(), 1);

    let stopped = block_on(harness.stop_service().execute(StopSessionCommand {
        expected_revision: None,
        session_id: session_id.clone(),
        mode: StopMode::Graceful,
    }))
    .expect("stop succeeds");
    let StopSessionResult::Terminated { session, .. } = stopped else {
        panic!("running session must terminate");
    };
    let removed = block_on(harness.remove_service().execute(RemoveSessionCommand {
        session_id: session_id.clone(),
        confirmation: RemovalConfirmation::Forced,
    }))
    .expect("terminal removal succeeds");

    assert_eq!(removed.session_id, session_id);
    assert_eq!(
        removed.removal_revision,
        Revision::new(session.revision().value() + 1)
    );
    assert_eq!(removed.confirmation, RemovalConfirmation::Forced);
    assert_eq!(harness.repository.session_count(), 0);
    assert!(
        harness
            .catalog
            .get(&removed.session_id)
            .expect("catalog lookup succeeds")
            .is_none()
    );
    assert_eq!(
        harness.repository.event_kinds().last(),
        Some(&DomainEventKind::SessionRemoved)
    );
}

#[test]
fn work_context_mutations_are_idempotent_and_preserve_foreground_observation() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    let session_id = started.session.id().clone();
    let foreground = started.session.foreground().clone();
    let item = work_item();
    let key = item.key().clone();

    let inserted = block_on(harness.work_service().upsert(UpsertWorkItemCommand {
        session_id: session_id.clone(),
        item: item.clone(),
    }))
    .expect("work upsert succeeds");
    assert!(inserted.changed);
    assert_eq!(inserted.session.foreground(), &foreground);
    assert_eq!(inserted.session.summary().work().active_item_count, 1);

    let repeated = block_on(harness.work_service().upsert(UpsertWorkItemCommand {
        session_id: session_id.clone(),
        item,
    }))
    .expect("repeated work upsert succeeds");
    assert!(!repeated.changed);
    assert_eq!(repeated.session.revision(), inserted.session.revision());

    let removed = block_on(harness.work_service().remove(RemoveWorkItemCommand {
        session_id: session_id.clone(),
        item_key: key.clone(),
    }))
    .expect("work removal succeeds");
    assert!(removed.changed);
    assert_eq!(removed.session.foreground(), &foreground);

    let repeated_remove = block_on(harness.work_service().remove(RemoveWorkItemCommand {
        session_id,
        item_key: key,
    }))
    .expect("repeated work removal succeeds");
    assert!(!repeated_remove.changed);
}

#[test]
fn work_context_expiry_is_session_local_batched_and_idempotent() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    let session_id = started.session.id().clone();
    let item = WorkItem::new(
        WorkItemKey::new(
            WorkOwner::new("workflow").expect("owner is valid"),
            WorkItemId::new("expiring-task").expect("work ID is valid"),
        ),
        WorkRelationKind::Related,
        "expiring task",
        None,
        WorkStatus::Running,
        Vec::new(),
        Metadata::default(),
        TimestampMillis::new(105),
        TimestampMillis::new(105),
        Some(TimestampMillis::new(106)),
    )
    .expect("work item is valid");
    block_on(harness.work_service().upsert(UpsertWorkItemCommand {
        session_id: session_id.clone(),
        item,
    }))
    .expect("work upsert succeeds");

    let expired =
        block_on(harness.work_service().expire(session_id.clone())).expect("work expiry succeeds");
    assert_eq!(expired.expired.len(), 1);
    assert_eq!(
        expired
            .session
            .work_context()
            .get(&expired.expired[0])
            .expect("expired item remains visible")
            .status(),
        WorkStatus::Expired
    );

    let repeated =
        block_on(harness.work_service().expire(session_id)).expect("repeat expiry succeeds");
    assert!(repeated.expired.is_empty());
    assert_eq!(repeated.session.revision(), expired.session.revision());
}

#[test]
fn terminal_resize_updates_live_pty_durable_restart_size_and_event_once() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    let size = TerminalSize::new(132, 40, 500, 500).expect("size is valid");
    let command = ResizeTerminalCommand {
        session_id: started.session.id().clone(),
        size,
    };

    let resized =
        block_on(harness.resize_service().execute(command.clone())).expect("resize succeeds");
    assert!(resized.changed);
    assert_eq!(resized.session.terminal_size(), size);
    assert_eq!(
        resized.session.configuration().launch().terminal_size(),
        size
    );
    assert_eq!(harness.runtime.resize_sizes(), vec![size]);
    assert_eq!(
        harness.repository.event_kinds().last(),
        Some(&DomainEventKind::TerminalResized)
    );

    let repeated =
        block_on(harness.resize_service().execute(command)).expect("repeat resize succeeds");
    assert!(!repeated.changed);
    assert_eq!(harness.runtime.resize_sizes(), vec![size]);
}

#[test]
fn terminal_resize_restores_live_size_when_durable_commit_fails() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    let prior_size = started.session.terminal_size();
    let requested = TerminalSize::new(132, 40, 500, 500).expect("size is valid");
    harness
        .repository
        .fail_next_commit
        .store(1, Ordering::SeqCst);

    let error = block_on(harness.resize_service().execute(ResizeTerminalCommand {
        session_id: started.session.id().clone(),
        size: requested,
    }))
    .expect_err("durable commit must fail");

    assert!(matches!(
        error,
        ResizeTerminalError::DurableSettlement { rollback: None, .. }
    ));
    assert_eq!(harness.runtime.resize_sizes(), vec![requested, prior_size]);
    assert_eq!(
        harness
            .repository
            .load_sync(started.session.id())
            .terminal_size(),
        prior_size
    );
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::TerminalResizeRolledBack)
    );
}

#[test]
fn terminal_resize_adapter_failure_does_not_change_durable_size() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    let prior_size = started.session.terminal_size();
    let requested = TerminalSize::new(132, 40, 500, 500).expect("size is valid");
    harness
        .runtime
        .fail_resize_on_call
        .store(1, Ordering::SeqCst);

    let error = block_on(harness.resize_service().execute(ResizeTerminalCommand {
        session_id: started.session.id().clone(),
        size: requested,
    }))
    .expect_err("PTY resize must fail");

    assert!(matches!(error, ResizeTerminalError::Resize(_)));
    assert_eq!(harness.runtime.resize_sizes(), vec![requested]);
    assert_eq!(
        harness
            .repository
            .load_sync(started.session.id())
            .terminal_size(),
        prior_size
    );
    assert_ne!(
        harness.repository.event_kinds().last(),
        Some(&DomainEventKind::TerminalResized)
    );
}

#[test]
fn standard_input_close_is_typed_ordered_and_idempotent() {
    let harness = Harness::new();
    let started =
        block_on(harness.service().execute(command("stdin-owner"))).expect("start succeeds");
    let command = CloseStandardInputCommand {
        session_id: started.session.id().clone(),
    };

    let first = block_on(harness.close_input_service().execute(command.clone()))
        .expect("first close succeeds");
    let repeated =
        block_on(harness.close_input_service().execute(command)).expect("repeated close succeeds");

    assert!(!first.already_closed);
    assert!(repeated.already_closed);
    assert_eq!(first.session_id, started.session.id().clone());
    assert_eq!(harness.runtime.close_calls(), 2);
}

#[test]
fn semantic_terminal_input_retry_replays_result_without_duplicate_pty_io() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    let command = input_command(started.session.id().clone(), "delivery-1", "compile");

    let delivered = block_on(harness.input_service().execute(command.clone()))
        .expect("input delivery succeeds");
    assert!(!delivered.deduplicated);
    assert!(delivered.foreground_revision.is_some());
    assert_eq!(harness.runtime.write_calls(), 1);
    assert_eq!(
        harness
            .repository
            .load_sync(started.session.id())
            .foreground()
            .execution()
            .state(),
        ExecutionState::Active
    );

    let repeated =
        block_on(harness.input_service().execute(command)).expect("retry is deduplicated");
    assert!(repeated.deduplicated);
    assert_eq!(repeated.delivered_at, delivered.delivered_at);
    assert_eq!(harness.runtime.write_calls(), 1);
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::InputDeliveryDeduplicated)
    );
}

#[test]
fn terminal_input_uses_the_selected_profiles_delivery_policy() {
    let harness = Harness::new();
    let profile_name = ProfileName::new("paced-agent").expect("profile name");
    let policy = InputDeliveryPolicy::paced_trailing_submit(
        Duration::from_millis(120),
        Duration::from_secs(2),
    )
    .expect("bounded pacing policy");
    harness
        .profiles
        .insert(test_input_profile(profile_name.clone(), policy));
    let mut start = command("agent");
    assert!(start.configuration.select_profile_if_absent(profile_name));
    let started = block_on(harness.service().execute(start)).expect("start succeeds");
    let input = InputSequence::new(
        vec![InputItem::Text("status".into()), InputItem::Key(Key::Enter)],
        8,
        1_024,
    )
    .expect("input is bounded");

    block_on(harness.input_service().execute(SendTerminalInputCommand {
        expected_revision: None,
        session_id: started.session.id().clone(),
        input,
        delivery_id: None,
        observation_policy: InputObservationPolicy::ForceActive,
    }))
    .expect("input delivery succeeds");

    assert_eq!(harness.runtime.input_policies(), vec![policy]);
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::InputDeliveryPacingApplied)
    );
}

#[test]
fn terminal_input_uses_the_exact_process_effective_profile_policy() {
    let harness = Harness::new();
    let effective_profile = ProfileName::new("codex").expect("profile name");
    let policy = InputDeliveryPolicy::paced_trailing_submit(
        Duration::from_millis(120),
        Duration::from_secs(2),
    )
    .expect("bounded pacing policy");
    harness
        .profiles
        .insert(test_input_profile(effective_profile.clone(), policy));
    let mut start = command("adaptive-agent");
    assert!(
        start
            .configuration
            .select_profile_if_absent(ProfileName::new("zsh").expect("base profile"))
    );
    start
        .configuration
        .set_profile_binding(ProfileBindingMode::ForegroundAdaptive);
    let started = block_on(harness.service().execute(start)).expect("start succeeds");
    harness
        .catalog
        .transition(
            started.session.id(),
            EffectiveProfileTransition::new(
                started.process.id.clone(),
                EffectiveProfileSelection::Profile(effective_profile),
                EffectiveProfileOrigin::ForegroundProcess,
                TimestampMillis::new(110),
            ),
        )
        .expect("effective transition")
        .expect("effective snapshot");

    block_on(harness.input_service().execute(input_command(
        started.session.id().clone(),
        "delivery-adaptive",
        "status",
    )))
    .expect("input delivery succeeds");

    assert_eq!(harness.runtime.input_policies(), vec![policy]);
}

#[test]
fn terminal_input_rejects_a_selected_profile_missing_from_the_active_catalog() {
    let harness = Harness::new();
    let mut start = command("agent");
    let profile = ProfileName::new("removed-agent").expect("profile name");
    assert!(
        start
            .configuration
            .select_profile_if_absent(profile.clone())
    );
    let started = block_on(harness.service().execute(start)).expect("start succeeds");

    let error = block_on(harness.input_service().execute(input_command(
        started.session.id().clone(),
        "delivery-missing-profile",
        "status",
    )))
    .expect_err("missing profile must not silently change input semantics");

    assert!(matches!(
        error,
        SendTerminalInputError::ProfileUnavailable { profile: missing } if missing == profile
    ));
    assert_eq!(harness.runtime.write_calls(), 0);
}

#[test]
fn terminal_input_revalidates_mouse_coordinates_under_the_session_lease() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    let larger = TerminalSize::new(100, 40, 500, 500).expect("larger terminal");
    let input = InputSequence::new(
        vec![InputItem::Mouse(
            MouseEvent::new(90, 24, MouseButton::Left, MouseEventKind::Press, larger)
                .expect("mouse is valid for admission maximum"),
        )],
        8,
        1_024,
    )
    .expect("input is bounded");

    let error = block_on(harness.input_service().execute(SendTerminalInputCommand {
        expected_revision: None,
        session_id: started.session.id().clone(),
        input,
        delivery_id: None,
        observation_policy: InputObservationPolicy::Preserve,
    }))
    .expect_err("current 80-column terminal must reject coordinate 90");

    assert!(matches!(error, SendTerminalInputError::InvalidInput(_)));
    assert_eq!(harness.runtime.write_calls(), 0);
}

#[test]
fn delivery_id_reuse_with_different_input_is_rejected_without_second_write() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    block_on(harness.input_service().execute(input_command(
        started.session.id().clone(),
        "delivery-1",
        "one",
    )))
    .expect("first delivery succeeds");

    let error = block_on(harness.input_service().execute(input_command(
        started.session.id().clone(),
        "delivery-1",
        "two",
    )))
    .expect_err("payload reuse must conflict");

    assert!(matches!(
        error,
        SendTerminalInputError::DeliveryPayloadConflict { .. }
    ));
    let mut policy_changed = input_command(started.session.id().clone(), "delivery-1", "one");
    policy_changed.observation_policy = InputObservationPolicy::Preserve;
    assert!(matches!(
        block_on(harness.input_service().execute(policy_changed))
            .expect_err("observation policy is part of semantic delivery"),
        SendTerminalInputError::DeliveryPayloadConflict { .. }
    ));
    assert_eq!(harness.runtime.write_calls(), 1);
}

#[test]
fn ambiguous_input_write_is_retained_and_never_retried_implicitly() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    harness
        .runtime
        .fail_write_on_call
        .store(1, Ordering::SeqCst);
    let command = input_command(started.session.id().clone(), "delivery-1", "compile");

    let first =
        block_on(harness.input_service().execute(command.clone())).expect_err("write is ambiguous");
    assert!(matches!(
        first,
        SendTerminalInputError::Write {
            settlement: None,
            ..
        }
    ));
    assert_eq!(harness.runtime.write_calls(), 1);

    let repeated = block_on(harness.input_service().execute(command))
        .expect_err("ambiguous result suppresses retry");
    assert!(matches!(
        repeated,
        SendTerminalInputError::DeliveryIndeterminate { .. }
    ));
    assert_eq!(harness.runtime.write_calls(), 1);
}

#[test]
fn confirmed_input_is_reported_when_result_settlement_fails() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    harness
        .deliveries
        .fail_next_settle
        .store(1, Ordering::SeqCst);
    let command = input_command(started.session.id().clone(), "delivery-1", "compile");

    let error = block_on(harness.input_service().execute(command.clone()))
        .expect_err("result settlement fails");
    assert!(error.delivery_was_confirmed());
    assert!(matches!(
        error,
        SendTerminalInputError::DeliveredSettlement { .. }
    ));
    assert_eq!(harness.runtime.write_calls(), 1);

    let repeated = block_on(harness.input_service().execute(command))
        .expect_err("pending reservation suppresses retry");
    assert!(matches!(
        repeated,
        SendTerminalInputError::DeliveryIndeterminate { .. }
    ));
    assert_eq!(harness.runtime.write_calls(), 1);
}

#[test]
fn direct_human_input_can_preserve_committed_foreground() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    let original_foreground = started.session.foreground().clone();
    let mut command = input_command(started.session.id().clone(), "delivery-1", "compile");
    command.observation_policy = InputObservationPolicy::Preserve;

    let delivered =
        block_on(harness.input_service().execute(command)).expect("input delivery succeeds");

    assert!(delivered.foreground_revision.is_none());
    assert_eq!(
        harness
            .repository
            .load_sync(started.session.id())
            .foreground(),
        &original_foreground
    );
}

#[test]
fn failed_process_exit_commits_restart_intent_and_enqueues_exact_revision() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command_with_restart(
        "build",
        RestartPolicy::OnFailure,
        2,
    )))
    .expect("start succeeds");
    let observed_at = TimestampMillis::new(1_000);

    let result = block_on(harness.exit_service().execute(ProcessExitObservation {
        session_id: started.session.id().clone(),
        process_id: started.process.id.clone(),
        outcome: ProcessOutcome::Exited(7),
        observed_at,
    }))
    .expect("exit settles");

    let ObserveProcessExitResult::Committed {
        session,
        decision,
        restart_enqueued,
    } = result
    else {
        panic!("exit must commit");
    };
    assert_eq!(
        decision,
        RestartDecision::RestartAfter {
            attempt: 1,
            delay: Duration::from_millis(25),
        }
    );
    assert!(restart_enqueued);
    assert_eq!(session.lifecycle(), LifecycleState::Exited(7));
    assert_eq!(session.revision(), Revision::new(5));
    assert_eq!(
        session.restart_status(),
        RestartStatus::Scheduled {
            attempt: 1,
            not_before: TimestampMillis::new(1_025),
        }
    );
    assert_eq!(
        harness.processes.lookup(session.id()).expect("lookup"),
        None
    );
    assert_eq!(
        harness.restarts.jobs(),
        vec![RestartJob {
            session_id: session.id().clone(),
            expected_revision: Revision::new(5),
            attempt: 1,
            not_before: TimestampMillis::new(1_025),
        }]
    );
    assert_eq!(
        &harness.repository.event_kinds()[3..],
        &[
            DomainEventKind::LifecycleChanged,
            DomainEventKind::RestartScheduled,
        ]
    );
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::RestartEnqueued)
    );
}

#[test]
fn stale_old_process_exit_cannot_unbind_or_terminate_replacement() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");
    let removed = harness
        .processes
        .unbind_if(started.session.id(), &started.process.id)
        .expect("old binding is removable");
    assert!(matches!(removed, ProcessUnbindOutcome::Unbound(_)));
    let replacement = ProcessInstance {
        id: ProcessInstanceId::new("process-2").expect("replacement ID"),
        started_at: TimestampMillis::new(500),
    };
    harness
        .processes
        .bind(started.session.id(), replacement.clone())
        .expect("replacement binds");

    let result = block_on(harness.exit_service().execute(ProcessExitObservation {
        session_id: started.session.id().clone(),
        process_id: started.process.id,
        outcome: ProcessOutcome::Exited(1),
        observed_at: TimestampMillis::new(600),
    }))
    .expect("stale exit is expected control flow");

    assert_eq!(
        result,
        ObserveProcessExitResult::Ignored {
            reason: IgnoredProcessExitReason::ProcessReplaced,
        }
    );
    assert_eq!(
        harness
            .repository
            .load_sync(started.session.id())
            .lifecycle(),
        LifecycleState::Running
    );
    assert_eq!(
        harness
            .processes
            .lookup(started.session.id())
            .expect("lookup"),
        Some(replacement)
    );
    assert!(harness.restarts.jobs().is_empty());
}

#[test]
fn disabled_restart_policy_persists_terminal_reason_without_scheduling() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("build"))).expect("start succeeds");

    let result = block_on(harness.exit_service().execute(ProcessExitObservation {
        session_id: started.session.id().clone(),
        process_id: started.process.id,
        outcome: ProcessOutcome::Exited(0),
        observed_at: TimestampMillis::new(800),
    }))
    .expect("exit settles");

    let ObserveProcessExitResult::Committed {
        session,
        decision,
        restart_enqueued,
    } = result
    else {
        panic!("exit must commit");
    };
    assert_eq!(
        decision,
        RestartDecision::DoNotRestart(NoRestartReason::PolicyDisabled)
    );
    assert!(!restart_enqueued);
    assert_eq!(
        session.restart_status(),
        RestartStatus::NotScheduled {
            reason: NoRestartReason::PolicyDisabled,
        }
    );
    assert!(harness.restarts.jobs().is_empty());
}

#[test]
fn scheduler_failure_leaves_durable_restart_intent_for_reconciliation() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command_with_restart(
        "build",
        RestartPolicy::Always,
        1,
    )))
    .expect("start succeeds");
    harness.restarts.fail_next.store(1, Ordering::SeqCst);

    let error = block_on(harness.exit_service().execute(ProcessExitObservation {
        session_id: started.session.id().clone(),
        process_id: started.process.id,
        outcome: ProcessOutcome::Exited(0),
        observed_at: TimestampMillis::new(900),
    }))
    .expect_err("scheduler fails after durable settlement");

    let ObserveProcessExitError::RestartEnqueue { session, job, .. } = error else {
        panic!("unexpected exit error");
    };
    assert_eq!(job.expected_revision, session.revision());
    assert!(matches!(
        session.restart_status(),
        RestartStatus::Scheduled { attempt: 1, .. }
    ));
    assert_eq!(
        harness
            .repository
            .load_sync(started.session.id())
            .restart_status(),
        session.restart_status()
    );
    assert_eq!(
        harness
            .processes
            .lookup(started.session.id())
            .expect("lookup"),
        None
    );
    assert!(harness.restarts.jobs().is_empty());
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::RestartEnqueueFailed)
    );
}

#[test]
fn startup_rebuilds_committed_restart_after_queue_loss_and_duplicate_exit() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command_with_restart(
        "rebuild",
        RestartPolicy::Always,
        1,
    )))
    .unwrap();
    let exit = ProcessExitObservation {
        session_id: started.session.id().clone(),
        process_id: started.process.id,
        outcome: ProcessOutcome::Exited(1),
        observed_at: TimestampMillis::new(900),
    };
    block_on(harness.exit_service().execute(exit.clone())).unwrap();
    let committed = harness.repository.only_session();
    harness.restarts.jobs.lock().unwrap().clear();
    // The durable aggregate is unchanged whether the crash loses an accepted
    // queue entry or happens between its commit and enqueue.
    block_on(harness.exit_service().execute(exit)).unwrap();
    assert_eq!(
        harness.repository.only_session().revision(),
        committed.revision()
    );
    harness.restarts.jobs.lock().unwrap().clear();
    let report = block_on(
        harness
            .reconciliation_service(StartupReconciliationLimits::new(8, 8).unwrap())
            .execute(),
    )
    .unwrap();
    assert!(report.ready());
    assert_eq!(report.restart_jobs_reenqueued, 1);
    let job = harness.restarts.jobs()[0].clone();
    assert_eq!(job.expected_revision, committed.revision());
    harness.clock.set(2000);
    harness
        .runtime
        .spawn_started_at
        .store(2005, Ordering::SeqCst);
    block_on(harness.restart_service().execute(job.clone())).unwrap();
    let restarted = harness.repository.only_session().revision();
    block_on(harness.restart_service().execute(job)).unwrap();
    assert_eq!(harness.repository.only_session().revision(), restarted);
    assert_eq!(harness.runtime.spawn_calls(), 2);
}

#[test]
fn due_restart_job_preserves_session_identity_and_commits_running() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command_with_restart(
        "build",
        RestartPolicy::Always,
        2,
    )))
    .expect("start succeeds");
    block_on(harness.exit_service().execute(ProcessExitObservation {
        session_id: started.session.id().clone(),
        process_id: started.process.id,
        outcome: ProcessOutcome::Exited(1),
        observed_at: TimestampMillis::new(1_000),
    }))
    .expect("exit schedules restart");
    let job = harness.restarts.jobs().last().expect("restart job").clone();
    harness.clock.set(2_000);
    harness
        .runtime
        .spawn_started_at
        .store(2_005, Ordering::SeqCst);

    let result = block_on(harness.restart_service().execute(job)).expect("restart succeeds");

    let RunScheduledRestartResult::Restarted { session, process } = result else {
        panic!("due job must restart");
    };
    assert_eq!(session.id(), started.session.id());
    assert_eq!(session.name(), started.session.name());
    assert_eq!(session.configuration(), started.session.configuration());
    assert_eq!(session.lifecycle(), LifecycleState::Running);
    assert_eq!(session.restart_status(), RestartStatus::Idle);
    assert_eq!(session.revision(), Revision::new(8));
    assert_eq!(process.started_at, TimestampMillis::new(2_005));
    assert!(session.updated_at() >= process.started_at);
    assert_eq!(harness.runtime.spawn_calls(), 2);
    assert_eq!(
        harness
            .processes
            .lookup(session.id())
            .expect("lookup")
            .expect("replacement binding"),
        process
    );
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::RestartAttemptSucceeded)
    );
}

#[test]
fn stale_restart_job_performs_no_process_io() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command_with_restart(
        "build",
        RestartPolicy::Always,
        2,
    )))
    .expect("start succeeds");
    block_on(harness.exit_service().execute(ProcessExitObservation {
        session_id: started.session.id().clone(),
        process_id: started.process.id,
        outcome: ProcessOutcome::Exited(1),
        observed_at: TimestampMillis::new(1_000),
    }))
    .expect("exit schedules restart");
    let job = harness.restarts.jobs().last().expect("restart job").clone();
    block_on(harness.rename_service().execute(RenameSessionCommand {
        session_id: started.session.id().clone(),
        name: SessionName::new("renamed").expect("name"),
    }))
    .expect("rename supersedes job");
    harness.clock.set(2_000);

    let result = block_on(harness.restart_service().execute(job)).expect("stale job is ignored");

    assert_eq!(
        result,
        RunScheduledRestartResult::Ignored {
            reason: IgnoredRestartReason::RevisionChanged,
        }
    );
    assert_eq!(harness.runtime.spawn_calls(), 1);
    assert_eq!(
        harness
            .repository
            .load_sync(started.session.id())
            .lifecycle(),
        LifecycleState::Exited(1)
    );
}

#[test]
fn early_restart_job_is_ignored_before_repository_or_process_mutation() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command_with_restart(
        "build",
        RestartPolicy::Always,
        2,
    )))
    .expect("start succeeds");
    block_on(harness.exit_service().execute(ProcessExitObservation {
        session_id: started.session.id().clone(),
        process_id: started.process.id,
        outcome: ProcessOutcome::Exited(1),
        observed_at: TimestampMillis::new(10_000),
    }))
    .expect("exit schedules restart");
    let job = harness.restarts.jobs().last().expect("restart job").clone();

    let result = block_on(harness.restart_service().execute(job)).expect("early job is ignored");

    assert_eq!(
        result,
        RunScheduledRestartResult::Ignored {
            reason: IgnoredRestartReason::TooEarly,
        }
    );
    assert_eq!(harness.runtime.spawn_calls(), 1);
    assert_eq!(
        harness
            .repository
            .load_sync(started.session.id())
            .revision(),
        Revision::new(5)
    );
}

#[test]
fn failed_restart_spawn_commits_failure_and_schedules_next_bounded_attempt() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command_with_restart(
        "build",
        RestartPolicy::Always,
        2,
    )))
    .expect("start succeeds");
    block_on(harness.exit_service().execute(ProcessExitObservation {
        session_id: started.session.id().clone(),
        process_id: started.process.id,
        outcome: ProcessOutcome::Exited(1),
        observed_at: TimestampMillis::new(1_000),
    }))
    .expect("exit schedules restart");
    let job = harness.restarts.jobs().last().expect("restart job").clone();
    harness.clock.set(2_000);
    harness.runtime.fail_spawn.store(1, Ordering::SeqCst);

    let error = block_on(harness.restart_service().execute(job)).expect_err("spawn fails");

    let RunScheduledRestartError::AttemptFailed {
        stage,
        session,
        recovery,
        ..
    } = error
    else {
        panic!("unexpected restart error");
    };
    assert_eq!(stage, RestartFailureStage::Spawn);
    assert!(recovery.completed());
    assert_eq!(session.lifecycle(), LifecycleState::Failed);
    assert!(matches!(
        session.restart_status(),
        RestartStatus::Scheduled { attempt: 2, .. }
    ));
    let jobs = harness.restarts.jobs();
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[1].attempt, 2);
    assert_eq!(jobs[1].expected_revision, Revision::new(8));
    assert_eq!(harness.runtime.force_calls(), 0);
}

#[test]
fn failed_restart_bind_forces_process_and_schedules_next_attempt() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command_with_restart(
        "build",
        RestartPolicy::Always,
        2,
    )))
    .expect("start succeeds");
    block_on(harness.exit_service().execute(ProcessExitObservation {
        session_id: started.session.id().clone(),
        process_id: started.process.id,
        outcome: ProcessOutcome::Exited(1),
        observed_at: TimestampMillis::new(1_000),
    }))
    .expect("exit schedules restart");
    let job = harness.restarts.jobs().last().expect("restart job").clone();
    harness.clock.set(2_000);
    harness.processes.fail_next_bind.store(1, Ordering::SeqCst);

    let error = block_on(harness.restart_service().execute(job)).expect_err("bind fails");

    let RunScheduledRestartError::AttemptFailed {
        stage,
        process,
        session,
        recovery,
        ..
    } = error
    else {
        panic!("unexpected restart error");
    };
    assert_eq!(stage, RestartFailureStage::Bind);
    assert!(process.is_some());
    assert!(recovery.completed());
    assert_eq!(session.lifecycle(), LifecycleState::Failed);
    assert!(matches!(
        session.restart_status(),
        RestartStatus::Scheduled { attempt: 2, .. }
    ));
    assert_eq!(harness.runtime.force_calls(), 1);
    assert_eq!(
        harness
            .processes
            .lookup(started.session.id())
            .expect("lookup"),
        None
    );
}

#[test]
fn failed_running_commit_compensates_exact_process_before_rescheduling() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command_with_restart(
        "build",
        RestartPolicy::Always,
        2,
    )))
    .expect("start succeeds");
    block_on(harness.exit_service().execute(ProcessExitObservation {
        session_id: started.session.id().clone(),
        process_id: started.process.id,
        outcome: ProcessOutcome::Exited(1),
        observed_at: TimestampMillis::new(1_000),
    }))
    .expect("exit schedules restart");
    let job = harness.restarts.jobs().last().expect("restart job").clone();
    harness.clock.set(2_000);
    harness.repository.fail_on_commit_from_now(3);

    let error = block_on(harness.restart_service().execute(job)).expect_err("running commit fails");

    let RunScheduledRestartError::AttemptFailed {
        stage,
        session,
        recovery,
        ..
    } = error
    else {
        panic!("unexpected restart error");
    };
    assert_eq!(stage, RestartFailureStage::CommitRunning);
    assert!(recovery.completed());
    assert_eq!(session.lifecycle(), LifecycleState::Failed);
    assert_eq!(session.revision(), Revision::new(9));
    assert_eq!(harness.runtime.force_calls(), 1);
    assert_eq!(
        harness
            .processes
            .lookup(started.session.id())
            .expect("lookup"),
        None
    );
    assert_eq!(harness.restarts.jobs().len(), 2);
}

#[test]
fn failed_restart_cleanup_retains_ownership_without_rescheduling() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command_with_restart(
        "build",
        RestartPolicy::Always,
        2,
    )))
    .expect("start succeeds");
    block_on(harness.exit_service().execute(ProcessExitObservation {
        session_id: started.session.id().clone(),
        process_id: started.process.id,
        outcome: ProcessOutcome::Exited(1),
        observed_at: TimestampMillis::new(1_000),
    }))
    .expect("exit schedules restart");
    let job = harness.restarts.jobs().last().expect("restart job").clone();
    harness.clock.set(2_000);
    harness.repository.fail_on_commit_from_now(3);
    harness
        .processes
        .fail_next_unbind
        .store(1, Ordering::SeqCst);

    let error = block_on(harness.restart_service().execute(job)).expect_err("running commit fails");

    let RunScheduledRestartError::AttemptFailed {
        session, recovery, ..
    } = error
    else {
        panic!("unexpected restart error");
    };
    assert!(recovery.unbind_failure().is_some());
    assert!(recovery.settlement_failure().is_none());
    assert!(recovery.enqueue_failure().is_none());
    assert!(!recovery.completed());
    assert_eq!(session.lifecycle(), LifecycleState::Starting);
    assert!(session.process().is_some());
    assert!(
        harness
            .processes
            .lookup(session.id())
            .expect("lookup")
            .is_some()
    );
    assert_eq!(harness.restarts.jobs().len(), 1);
}

#[test]
fn startup_reconciliation_scans_pages_and_restores_mixed_session_truth() {
    let harness = Harness::new();
    let (running, running_process) = recovery_session(
        "session-001",
        "running",
        RecoveryFixtureState::Running,
        false,
    );
    let (starting, starting_process) = recovery_session(
        "session-002",
        "starting",
        RecoveryFixtureState::StartingOwned,
        false,
    );
    let (unowned, _) = recovery_session(
        "session-003",
        "unowned",
        RecoveryFixtureState::StartingUnowned,
        false,
    );
    let (stopped, _) = recovery_session(
        "session-004",
        "stopped",
        RecoveryFixtureState::Stopped,
        false,
    );
    for session in [running, starting, unowned, stopped] {
        harness.repository.seed(session);
    }
    let running_process = running_process.expect("running process");
    let starting_process = starting_process.expect("starting process");
    harness.process_truth.set(
        running_process.id().clone(),
        ProcessTruth::Running {
            observed_at: TimestampMillis::new(500),
        },
    );
    harness.process_truth.set(
        starting_process.id().clone(),
        ProcessTruth::Running {
            observed_at: TimestampMillis::new(500),
        },
    );

    let report = block_on(
        harness
            .reconciliation_service(
                StartupReconciliationLimits::new(2, 8).expect("valid recovery limits"),
            )
            .execute(),
    )
    .expect("reconciliation succeeds");

    assert!(report.ready());
    assert_eq!(report.examined, 4);
    assert_eq!(report.terminal_unchanged, 1);
    assert_eq!(report.running_rebound, 1);
    assert_eq!(report.starting_completed, 1);
    assert_eq!(report.process_exits_settled, 1);
    assert_eq!(report.unresolved, 0);
    assert_eq!(harness.repository.recovery_calls().len(), 2);
    assert_eq!(harness.process_truth.call_count(), 2);
    assert_eq!(harness.runtime.arm_calls(), 2);
    assert_eq!(harness.catalog.status().expect("catalog status").1, 4);
    let plans = harness.observations.plans();
    assert_eq!(plans.len(), 2);
    assert!(
        plans
            .iter()
            .all(|plan| plan.foreground_not_before.is_some())
    );
    assert_eq!(
        harness
            .repository
            .load_sync(&SessionId::new("session-002").expect("session ID"))
            .lifecycle(),
        LifecycleState::Running
    );
    assert_eq!(
        harness
            .repository
            .load_sync(&SessionId::new("session-003").expect("session ID"))
            .lifecycle(),
        LifecycleState::Failed
    );
    assert!(
        harness
            .processes
            .lookup(&SessionId::new("session-001").expect("session ID"))
            .expect("directory lookup")
            .is_some()
    );
}

#[test]
fn startup_reconciliation_settles_exit_and_reenqueues_durable_restart() {
    let harness = Harness::new();
    let (session, process) = recovery_session(
        "session-restart",
        "restart",
        RecoveryFixtureState::Running,
        true,
    );
    let process = process.expect("running process");
    harness.repository.seed(session);
    harness.process_truth.set(
        process.id().clone(),
        ProcessTruth::Exited {
            exit_code: 17,
            observed_at: TimestampMillis::new(500),
        },
    );

    let report = block_on(
        harness
            .reconciliation_service(
                StartupReconciliationLimits::new(8, 8).expect("valid recovery limits"),
            )
            .execute(),
    )
    .expect("reconciliation succeeds");

    assert!(report.ready());
    assert_eq!(report.process_exits_settled, 1);
    assert_eq!(report.restart_jobs_reenqueued, 1);
    let durable = harness.repository.only_session();
    assert_eq!(durable.lifecycle(), LifecycleState::Exited(17));
    assert!(matches!(
        durable.restart_status(),
        RestartStatus::Scheduled { attempt: 1, .. }
    ));
    assert_eq!(harness.restarts.jobs().len(), 1);
    assert_eq!(
        harness
            .processes
            .lookup(durable.id())
            .expect("directory lookup"),
        None
    );
}

#[test]
fn startup_reconciliation_settles_missing_process_as_supervision_failure() {
    let harness = Harness::new();
    let (session, process) = recovery_session(
        "session-missing",
        "missing",
        RecoveryFixtureState::Running,
        false,
    );
    let process = process.expect("running process");
    harness.repository.seed(session);
    harness.process_truth.set(
        process.id().clone(),
        ProcessTruth::Missing {
            observed_at: TimestampMillis::new(500),
        },
    );

    let report = block_on(
        harness
            .reconciliation_service(
                StartupReconciliationLimits::new(8, 8).expect("valid recovery limits"),
            )
            .execute(),
    )
    .expect("reconciliation succeeds");

    assert!(report.ready());
    assert_eq!(report.process_exits_settled, 1);
    let durable = harness.repository.only_session();
    assert_eq!(durable.lifecycle(), LifecycleState::Failed);
    assert_eq!(
        durable.restart_status(),
        RestartStatus::NotScheduled {
            reason: NoRestartReason::PolicyDisabled
        }
    );
}

#[test]
fn startup_reconciliation_resumes_durable_stopping_intent() {
    let harness = Harness::new();
    let (session, process) = recovery_session(
        "session-stopping",
        "stopping",
        RecoveryFixtureState::Stopping,
        false,
    );
    let process = process.expect("stopping process");
    harness.repository.seed(session);
    harness.process_truth.set(
        process.id().clone(),
        ProcessTruth::Running {
            observed_at: TimestampMillis::new(500),
        },
    );

    let report = block_on(
        harness
            .reconciliation_service(
                StartupReconciliationLimits::new(8, 8).expect("valid recovery limits"),
            )
            .execute(),
    )
    .expect("reconciliation succeeds");

    assert!(report.ready());
    assert_eq!(report.stopping_resumed, 1);
    assert_eq!(
        harness.repository.only_session().lifecycle(),
        LifecycleState::Exited(0)
    );
    assert_eq!(harness.runtime.stop_calls(), 1);
}

#[test]
fn startup_reconciliation_preserves_durable_force_cancellation_mode() {
    let harness = Harness::new();
    let (session, process) = recovery_session(
        "session-force-cancellation",
        "force-cancellation",
        RecoveryFixtureState::Stopping,
        false,
    );
    let process = process.expect("stopping process");
    harness.repository.retain_cancellation_recovery(
        session.id().clone(),
        RuntimeCancellationRecovery::Resume(RuntimeCancellationMode::Force),
    );
    harness.repository.seed(session);
    harness.process_truth.set(
        process.id().clone(),
        ProcessTruth::Running {
            observed_at: TimestampMillis::new(500),
        },
    );

    let report = block_on(
        harness
            .reconciliation_service(
                StartupReconciliationLimits::new(8, 8).expect("valid recovery limits"),
            )
            .execute(),
    )
    .expect("force recovery succeeds");

    assert!(report.ready());
    assert_eq!(report.stopping_resumed, 1);
    assert_eq!(harness.runtime.force_calls(), 1);
    assert_eq!(harness.runtime.stop_calls(), 0);
}

#[test]
fn startup_cancellation_keeps_graceful_only_and_allows_independent_force() {
    for mode in [
        RuntimeCancellationMode::Graceful,
        RuntimeCancellationMode::Force,
    ] {
        let harness = Harness::new();
        let (mut session, process) = recovery_session(
            "session-control",
            "control",
            RecoveryFixtureState::Stopping,
            false,
        );
        if mode == RuntimeCancellationMode::Force {
            let config = session
                .configuration()
                .clone()
                .with_stop_no_escalation(true);
            session
                .update_configuration(config, TimestampMillis::new(105))
                .unwrap();
        }
        let process = process.unwrap();
        harness.repository.retain_cancellation_recovery(
            session.id().clone(),
            RuntimeCancellationRecovery::Resume(mode),
        );
        harness.repository.seed(session);
        harness.process_truth.set(
            process.id().clone(),
            ProcessTruth::Running {
                observed_at: TimestampMillis::new(500),
            },
        );
        let report = block_on(
            harness
                .reconciliation_service(StartupReconciliationLimits::new(8, 8).unwrap())
                .execute(),
        )
        .unwrap();
        assert!(report.ready());
        assert_eq!(report.stopping_resumed, 1);
        match mode {
            RuntimeCancellationMode::Graceful => {
                assert_eq!(harness.runtime.force_calls(), 0);
                assert!(harness.runtime.stop_policies.lock().unwrap()[0].no_escalation());
                assert!(
                    harness
                        .repository
                        .only_session()
                        .configuration()
                        .execution()
                        .stop()
                        .no_escalation()
                );
            }
            RuntimeCancellationMode::Force => assert_eq!(harness.runtime.force_calls(), 1),
        }
    }
}

#[test]
fn startup_reconciliation_never_redrives_unknown_cancellation_effect() {
    let harness = Harness::new();
    let (session, process) = recovery_session(
        "session-unknown-cancellation",
        "unknown-cancellation",
        RecoveryFixtureState::Stopping,
        false,
    );
    let process = process.expect("stopping process");
    harness.repository.retain_cancellation_recovery(
        session.id().clone(),
        RuntimeCancellationRecovery::PreserveUnknown,
    );
    harness.repository.seed(session);
    harness.process_truth.set(
        process.id().clone(),
        ProcessTruth::Running {
            observed_at: TimestampMillis::new(500),
        },
    );

    let report = block_on(
        harness
            .reconciliation_service(
                StartupReconciliationLimits::new(8, 8).expect("valid recovery limits"),
            )
            .execute(),
    )
    .expect("scan preserves the status-queryable cancellation");

    assert!(report.ready());
    assert_eq!(report.unresolved, 0);
    assert_eq!(report.stopping_unknown_preserved, 1);
    assert!(report.issues.is_empty());
    assert_eq!(harness.runtime.force_calls(), 0);
    assert_eq!(harness.runtime.stop_calls(), 0);
}

#[test]
fn second_startup_never_redrives_a_previously_claimed_force_cancellation() {
    let harness = Harness::new();
    let (session, process) = recovery_session(
        "session-force-crash-window",
        "force-crash-window",
        RecoveryFixtureState::Stopping,
        false,
    );
    let process = process.expect("stopping process");
    harness.repository.retain_cancellation_recovery(
        session.id().clone(),
        RuntimeCancellationRecovery::Resume(RuntimeCancellationMode::Force),
    );
    assert_eq!(
        block_on(SessionRecoverySource::claim_runtime_cancellation_recovery(
            &harness.repository,
            session.id(),
            TimestampMillis::new(499),
        ))
        .expect("first startup commits its pre-effect claim"),
        Some(RuntimeCancellationRecovery::Resume(
            RuntimeCancellationMode::Force
        ))
    );
    harness.repository.seed(session);
    harness.process_truth.set(
        process.id().clone(),
        ProcessTruth::Running {
            observed_at: TimestampMillis::new(500),
        },
    );

    let report = block_on(
        harness
            .reconciliation_service(
                StartupReconciliationLimits::new(8, 8).expect("valid recovery limits"),
            )
            .execute(),
    )
    .expect("second startup remains operable for status reconciliation");

    assert!(report.ready());
    assert_eq!(report.unresolved, 0);
    assert_eq!(report.stopping_unknown_preserved, 1);
    assert_eq!(harness.runtime.force_calls(), 0);
    assert_eq!(harness.runtime.stop_calls(), 0);
}

#[test]
fn startup_reconciliation_bounds_issues_and_continues_after_session_failures() {
    let harness = Harness::new();
    for index in 0..3 {
        let (session, process) = recovery_session(
            &format!("session-failure-{index:03}"),
            &format!("failure-{index:03}"),
            RecoveryFixtureState::Running,
            false,
        );
        let process = process.expect("running process");
        harness.process_truth.fail(process.id().clone());
        harness.repository.seed(session);
    }

    let report = block_on(
        harness
            .reconciliation_service(
                StartupReconciliationLimits::new(2, 1).expect("valid recovery limits"),
            )
            .execute(),
    )
    .expect("scan completes");

    assert!(!report.ready());
    assert!(report.scan_completed);
    assert_eq!(report.examined, 3);
    assert_eq!(report.unresolved, 3);
    assert_eq!(report.issues.len(), 1);
    assert_eq!(harness.process_truth.call_count(), 3);
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::StartupSessionReconciliationFailed)
    );
}

#[test]
fn startup_reconciliation_keeps_three_hundred_terminal_sessions_page_bounded() {
    let harness = Harness::new();
    for index in 0..300 {
        let (session, _) = recovery_session(
            &format!("session-{index:03}"),
            &format!("terminal-{index:03}"),
            RecoveryFixtureState::Stopped,
            false,
        );
        harness.repository.seed(session);
    }

    let report = block_on(
        harness
            .reconciliation_service(
                StartupReconciliationLimits::new(32, 4).expect("valid recovery limits"),
            )
            .execute(),
    )
    .expect("reconciliation succeeds");

    assert!(report.ready());
    assert_eq!(report.examined, 300);
    assert_eq!(report.terminal_unchanged, 300);
    assert_eq!(harness.process_truth.call_count(), 0);
    assert_eq!(harness.catalog.status().expect("catalog status").1, 300);
    let calls = harness.repository.recovery_calls();
    assert_eq!(calls.len(), 10);
    assert!(calls.iter().all(|(_, limit)| *limit == 32));
}

#[test]
fn startup_reconciliation_source_failure_retains_incomplete_report() {
    let harness = Harness::new();
    harness
        .repository
        .fail_next_recovery
        .store(1, Ordering::SeqCst);

    let error = block_on(
        harness
            .reconciliation_service(
                StartupReconciliationLimits::new(8, 8).expect("valid recovery limits"),
            )
            .execute(),
    )
    .expect_err("source failure is fatal");

    assert_eq!(
        error.category(),
        crate::ApplicationErrorCategory::Unavailable
    );
    assert_eq!(error.partial_report().examined, 0);
    assert!(!error.partial_report().scan_completed);
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::StartupReconciliationFailed)
    );
}

#[test]
fn startup_reconciliation_rejects_nonadvancing_empty_page() {
    let harness = Harness::new();
    let malformed = EmptyContinuationRecovery;
    let service = StartupReconciliationService::new(
        &harness.clock,
        &malformed,
        &harness.process_truth,
        &harness.coordinator,
        &harness.repository,
        &harness.runtime,
        &harness.processes,
        &harness.restarts,
        &harness.observations,
        &harness.catalog,
        &harness.telemetry,
        StartupReconciliationLimits::new(8, 8).expect("valid recovery limits"),
    );

    let error = block_on(service.execute()).expect_err("malformed page is fatal");

    assert!(matches!(
        &error,
        StartupReconciliationError::InvalidBatch {
            violation: RecoveryBatchViolation::EmptyContinuation,
            ..
        }
    ));
    assert!(!error.partial_report().ready());
}

#[test]
fn stop_revision_precondition_observes_a_change_during_lease_acquisition() {
    for mode in [StopMode::Graceful, StopMode::Force] {
        let harness = Harness::new();
        let started = block_on(harness.service().execute(command("guarded-stop"))).unwrap();
        let events = harness.repository.event_kinds();
        let coordinator = InterveningMutationCoordinator {
            repository: &harness.repository,
            acquisitions: AtomicUsize::new(1),
        };
        let service = StopSessionService::new(
            &harness.clock,
            &coordinator,
            &harness.repository,
            &harness.runtime,
            &harness.processes,
            &harness.restarts,
            &harness.catalog,
            &harness.telemetry,
        );
        let error = block_on(service.execute(StopSessionCommand {
            session_id: started.session.id().clone(),
            expected_revision: Some(started.session.revision()),
            mode,
        }))
        .expect_err("an intervening observation must prevent stopping");
        assert!(matches!(error, StopSessionError::Mutation(
            SessionMutationError::RevisionPreconditionFailed { expected_revision, current_revision }
        ) if expected_revision == Revision::new(3) && current_revision == Revision::new(4)));
        let retained = harness.repository.load_sync(started.session.id());
        assert_eq!(retained.lifecycle(), LifecycleState::Running);
        assert_eq!(retained.process(), started.session.process());
        assert_eq!(harness.repository.event_kinds(), events);
        assert_eq!(harness.runtime.stop_calls(), 0);
        assert_eq!(harness.runtime.force_calls.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn same_timestamp_input_calls_have_distinct_attempts_and_exact_digests() {
    struct FixedClock;
    impl Clock for FixedClock {
        fn now(&self) -> TimestampMillis {
            TimestampMillis::new(10_000)
        }
    }
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("input-identities"))).unwrap();
    let inner = Arc::new(FakeRuntime::new());
    let attempts = Arc::new(DispatchAttemptStore::default());
    let durable =
        crate::DurableProcessRuntime::new(inner.clone(), attempts.clone(), Arc::new(FixedClock));
    let attempt_ids = TestAttemptIds::default();
    let service = SendTerminalInputService::new(
        &FixedClock,
        &attempt_ids,
        &harness.coordinator,
        &harness.repository,
        &durable,
        &harness.processes,
        &harness.profiles,
        &harness.catalog,
        &harness.deliveries,
        &harness.catalog,
        &harness.telemetry,
    );
    let mut digests = Vec::new();
    for text in ["one", "two", "one"] {
        let mut request = input_command(started.session.id().clone(), "unused", text);
        request.delivery_id = None;
        block_on(service.execute(request)).expect("each intentional write must reach the PTY");
        let records = attempts.attempts.lock().unwrap();
        digests.push(
            records
                .values()
                .max_by_key(|attempt| attempt.fencing_token().value())
                .unwrap()
                .payload_digest(),
        );
    }
    assert_eq!(inner.write_calls(), 3);
    assert_eq!(attempts.attempts.lock().unwrap().len(), 3);
    assert_ne!(
        digests[0], digests[1],
        "same-length input differs semantically"
    );
    assert_eq!(
        digests[0], digests[2],
        "identity nonce is not semantic payload"
    );
    let explicit = input_command(started.session.id().clone(), "retry-delivery", "one");
    assert!(
        !block_on(service.execute(explicit.clone()))
            .unwrap()
            .deduplicated
    );
    assert!(block_on(service.execute(explicit)).unwrap().deduplicated);
    assert_eq!(inner.write_calls(), 4);
    assert_eq!(attempts.attempts.lock().unwrap().len(), 4);
}

#[test]
fn input_identity_failure_precedes_delivery_reservation_and_io() {
    struct UnavailableIds;
    impl crate::OperationAttemptIdSource for UnavailableIds {
        fn next_attempt_id(&self) -> Result<undercurrent_domain::OperationAttemptId, PortError> {
            Err(port_error(PortOperation::AcceptOperationAttempt, None))
        }
    }
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("identity-failure"))).unwrap();
    let service = SendTerminalInputService::new(
        &harness.clock,
        &UnavailableIds,
        &harness.coordinator,
        &harness.repository,
        &harness.runtime,
        &harness.processes,
        &harness.profiles,
        &harness.catalog,
        &harness.deliveries,
        &harness.catalog,
        &harness.telemetry,
    );
    let result = block_on(service.execute(input_command(
        started.session.id().clone(),
        "delivery",
        "secret",
    )));
    assert!(matches!(result, Err(SendTerminalInputError::Admission(_))));
    assert!(harness.deliveries.records.lock().unwrap().is_empty());
    assert_eq!(harness.runtime.write_calls(), 0);
}

#[test]
fn input_revision_precondition_observes_a_change_before_delivery_reservation() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("guarded-input"))).unwrap();
    let events = harness.repository.event_kinds();
    let coordinator = InterveningMutationCoordinator {
        repository: &harness.repository,
        acquisitions: AtomicUsize::new(1),
    };
    let attempt_ids = TestAttemptIds::default();
    let service = SendTerminalInputService::new(
        &harness.clock,
        &attempt_ids,
        &coordinator,
        &harness.repository,
        &harness.runtime,
        &harness.processes,
        &harness.profiles,
        &harness.catalog,
        &harness.deliveries,
        &harness.catalog,
        &harness.telemetry,
    );
    let mut request = input_command(started.session.id().clone(), "guarded-delivery", "input");
    request.expected_revision = Some(started.session.revision());
    let error = block_on(service.execute(request))
        .expect_err("an intervening observation must prevent input reservation and IO");
    assert!(matches!(error, SendTerminalInputError::Mutation(
        SessionMutationError::RevisionPreconditionFailed { expected_revision, current_revision }
    ) if expected_revision == Revision::new(3) && current_revision == Revision::new(4)));
    assert!(harness.deliveries.records.lock().unwrap().is_empty());
    assert_eq!(harness.runtime.write_calls(), 0);
    assert_eq!(harness.repository.event_kinds(), events);
    assert_eq!(
        harness
            .repository
            .load_sync(started.session.id())
            .lifecycle(),
        LifecycleState::Running
    );
}

#[test]
fn matching_revision_preconditions_allow_input_and_stop() {
    let harness = Harness::new();
    let started = block_on(harness.service().execute(command("guarded-success"))).unwrap();
    let mut input = input_command(started.session.id().clone(), "guarded-delivery", "input");
    input.expected_revision = Some(started.session.revision());
    block_on(harness.input_service().execute(input)).expect("matching input revision");
    let current = harness.repository.load_sync(started.session.id());
    assert!(current.revision() > started.session.revision());
    block_on(harness.stop_service().execute(StopSessionCommand {
        session_id: current.id().clone(),
        expected_revision: Some(current.revision()),
        mode: StopMode::Graceful,
    }))
    .expect("matching stop revision");
    assert_eq!(harness.runtime.write_calls(), 1);
    assert_eq!(harness.runtime.stop_calls(), 1);
    assert!(
        harness
            .repository
            .load_sync(current.id())
            .lifecycle()
            .is_terminal()
    );
}

struct Harness {
    clock: IncrementingClock,
    ids: FixedIds,
    repository: MemoryRepository,
    runtime: FakeRuntime,
    processes: MemoryProcessDirectory,
    profiles: TestDetectionProfiles,
    catalog: PreparedSessionCatalog,
    telemetry: MemoryTelemetry,
    coordinator: NoopMutationCoordinator,
    deliveries: MemoryInputDeliveries,
    restarts: MemoryRestartScheduler,
    observations: MemoryObservationScheduler,
    process_truth: MemoryProcessTruth,
}

#[derive(Default)]
struct TestDetectionProfiles {
    entries: Mutex<BTreeMap<ProfileName, ProfileCatalogEntry>>,
}

impl TestDetectionProfiles {
    fn insert(&self, profile: DetectionProfile) {
        self.entries.lock().expect("profile lock").insert(
            profile.name().clone(),
            ProfileCatalogEntry::new(Arc::new(profile), ProfileSourceKind::BuiltIn),
        );
    }

    fn bounded_entries(&self, limit: usize) -> Result<Vec<ProfileCatalogEntry>, PortError> {
        let entries = self.entries.lock().expect("profile lock");
        if entries.len() > limit {
            return Err(port_error(PortOperation::ReadDetectionProfiles, None));
        }
        Ok(entries.values().cloned().collect())
    }
}

impl DetectionProfileCatalog for TestDetectionProfiles {
    fn list(&self, limit: usize) -> Result<Vec<ProfileSummary>, PortError> {
        Ok(self
            .bounded_entries(limit)?
            .iter()
            .map(ProfileSummary::from_entry)
            .collect())
    }

    fn get(&self, name: &ProfileName) -> Result<Option<ProfileCatalogEntry>, PortError> {
        Ok(self
            .entries
            .lock()
            .expect("profile lock")
            .get(name)
            .cloned())
    }

    fn all(&self, limit: usize) -> Result<Vec<ProfileCatalogEntry>, PortError> {
        self.bounded_entries(limit)
    }
}

impl Harness {
    fn new() -> Self {
        Self {
            clock: IncrementingClock::new(100),
            ids: FixedIds::new("session-1"),
            repository: MemoryRepository::default(),
            runtime: FakeRuntime::new(),
            processes: MemoryProcessDirectory::default(),
            profiles: TestDetectionProfiles::default(),
            catalog: PreparedSessionCatalog::new(),
            telemetry: MemoryTelemetry::default(),
            coordinator: NoopMutationCoordinator,
            deliveries: MemoryInputDeliveries::default(),
            restarts: MemoryRestartScheduler::default(),
            observations: MemoryObservationScheduler::default(),
            process_truth: MemoryProcessTruth::default(),
        }
    }

    fn service(&self) -> StartSessionService<'_> {
        StartSessionService::new(
            &self.clock,
            &self.ids,
            &self.coordinator,
            &self.repository,
            &self.runtime,
            &self.processes,
            &self.catalog,
            &self.telemetry,
        )
    }

    fn stop_service(&self) -> StopSessionService<'_> {
        StopSessionService::new(
            &self.clock,
            &self.coordinator,
            &self.repository,
            &self.runtime,
            &self.processes,
            &self.restarts,
            &self.catalog,
            &self.telemetry,
        )
    }

    fn manual_restart_service(&self) -> RestartSessionService<'_> {
        RestartSessionService::new(
            &self.clock,
            &self.coordinator,
            &self.repository,
            &self.runtime,
            &self.processes,
            &self.restarts,
            &self.catalog,
            &self.telemetry,
        )
    }

    fn rename_service(&self) -> RenameSessionService<'_> {
        RenameSessionService::new(
            &self.clock,
            &self.coordinator,
            &self.repository,
            &self.catalog,
            &self.telemetry,
        )
    }

    fn remove_service(&self) -> RemoveSessionService<'_> {
        RemoveSessionService::new(
            &self.clock,
            &self.coordinator,
            &self.repository,
            &self.catalog,
            &self.telemetry,
        )
    }

    fn work_service(&self) -> WorkContextService<'_> {
        WorkContextService::new(
            &self.clock,
            &self.coordinator,
            &self.repository,
            &self.catalog,
            &self.telemetry,
        )
    }

    fn resize_service(&self) -> ResizeTerminalService<'_> {
        ResizeTerminalService::new(
            &self.clock,
            &self.coordinator,
            &self.repository,
            &self.runtime,
            &self.processes,
            &self.catalog,
            &self.telemetry,
        )
    }

    fn input_service(&self) -> SendTerminalInputService<'_> {
        SendTerminalInputService::new(
            &self.clock,
            &TEST_ATTEMPT_IDS,
            &self.coordinator,
            &self.repository,
            &self.runtime,
            &self.processes,
            &self.profiles,
            &self.catalog,
            &self.deliveries,
            &self.catalog,
            &self.telemetry,
        )
    }

    fn close_input_service(&self) -> CloseStandardInputService<'_> {
        CloseStandardInputService::new(
            &self.clock,
            &self.coordinator,
            &self.repository,
            &self.runtime,
            &self.processes,
            &self.catalog,
            &self.telemetry,
        )
    }

    fn exit_service(&self) -> ObserveProcessExitService<'_> {
        ObserveProcessExitService::new(
            &self.clock,
            &self.coordinator,
            &self.repository,
            &self.processes,
            &self.restarts,
            &self.catalog,
            &self.telemetry,
        )
    }

    fn restart_service(&self) -> RunScheduledRestartService<'_> {
        RunScheduledRestartService::new(
            &self.clock,
            &self.coordinator,
            &self.repository,
            &self.runtime,
            &self.processes,
            &self.restarts,
            &self.catalog,
            &self.telemetry,
        )
    }

    fn reconciliation_service(
        &self,
        limits: StartupReconciliationLimits,
    ) -> StartupReconciliationService<'_> {
        StartupReconciliationService::new(
            &self.clock,
            &self.repository,
            &self.process_truth,
            &self.coordinator,
            &self.repository,
            &self.runtime,
            &self.processes,
            &self.restarts,
            &self.observations,
            &self.catalog,
            &self.telemetry,
            limits,
        )
    }
}

struct NoopMutationLease;

struct EmptyContinuationRecovery;

impl SessionRecoverySource for EmptyContinuationRecovery {
    fn read_batch<'a>(
        &'a self,
        _after: Option<&'a SessionId>,
        _limit: usize,
    ) -> PortFuture<'a, Result<RecoverySessionBatch, PortError>> {
        Box::pin(async {
            Ok(RecoverySessionBatch {
                sessions: Vec::new(),
                next_after: Some(SessionId::new("nonadvancing").expect("session ID")),
            })
        })
    }
}

impl SessionMutationLease for NoopMutationLease {}

struct NoopMutationCoordinator;

impl SessionMutationCoordinator for NoopMutationCoordinator {
    fn acquire<'a>(
        &'a self,
        _session_id: &'a SessionId,
    ) -> PortFuture<'a, Result<Box<dyn SessionMutationLease>, PortError>> {
        Box::pin(async { Ok(Box::new(NoopMutationLease) as Box<dyn SessionMutationLease>) })
    }
}

struct FailingMutationCoordinator;

impl SessionMutationCoordinator for FailingMutationCoordinator {
    fn acquire<'a>(
        &'a self,
        session_id: &'a SessionId,
    ) -> PortFuture<'a, Result<Box<dyn SessionMutationLease>, PortError>> {
        Box::pin(async move {
            Err(port_error(
                PortOperation::AcquireSessionMutation,
                Some(session_id.clone()),
            ))
        })
    }
}

struct InterveningMutationCoordinator<'a> {
    repository: &'a MemoryRepository,
    acquisitions: AtomicUsize,
}

impl SessionMutationCoordinator for InterveningMutationCoordinator<'_> {
    fn acquire<'a>(
        &'a self,
        session_id: &'a SessionId,
    ) -> PortFuture<'a, Result<Box<dyn SessionMutationLease>, PortError>> {
        Box::pin(async move {
            if self.acquisitions.fetch_add(1, Ordering::SeqCst) == 1 {
                self.repository.advance_revision(session_id);
            }
            Ok(Box::new(NoopMutationLease) as Box<dyn SessionMutationLease>)
        })
    }
}

struct IncrementingClock(AtomicI64);

impl IncrementingClock {
    const fn new(initial: i64) -> Self {
        Self(AtomicI64::new(initial))
    }

    fn set(&self, value: i64) {
        self.0.store(value, Ordering::SeqCst);
    }
}

impl Clock for IncrementingClock {
    fn now(&self) -> TimestampMillis {
        TimestampMillis::new(self.0.fetch_add(1, Ordering::SeqCst))
    }
}

struct FixedIds(SessionId);

impl FixedIds {
    fn new(value: &str) -> Self {
        Self(SessionId::new(value).expect("valid fixture ID"))
    }
}

impl SessionIdSource for FixedIds {
    fn next_id(&self) -> Result<SessionId, PortError> {
        Ok(self.0.clone())
    }
}

#[derive(Default)]
struct MemoryRepository {
    state: Mutex<RepositoryState>,
    fail_next_commit: AtomicUsize,
    fail_session_revision: AtomicUsize,
    commit_calls: AtomicUsize,
    insert_calls: AtomicUsize,
    fail_commit_call: AtomicUsize,
    recovery_calls: Mutex<Vec<(Option<SessionId>, usize)>>,
    cancellation_recovery: Mutex<BTreeMap<SessionId, RuntimeCancellationRecovery>>,
    fail_next_recovery: AtomicUsize,
}

#[derive(Default)]
struct RepositoryState {
    sessions: BTreeMap<SessionId, Session>,
    names: BTreeMap<SessionName, SessionId>,
    events: Vec<SessionOutboxEvent>,
}

impl MemoryRepository {
    fn fail_on_commit_from_now(&self, offset: usize) {
        let current = self.commit_calls.load(Ordering::SeqCst);
        self.fail_commit_call
            .store(current.saturating_add(offset), Ordering::SeqCst);
    }

    fn reserve_name(&self, name: SessionName, session_id: SessionId) {
        self.state
            .lock()
            .expect("repository lock")
            .names
            .insert(name, session_id);
    }

    fn load_sync(&self, session_id: &SessionId) -> Session {
        self.state
            .lock()
            .expect("repository lock")
            .sessions
            .get(session_id)
            .expect("session")
            .clone()
    }

    fn only_session(&self) -> Session {
        let state = self.state.lock().expect("repository lock");
        assert_eq!(state.sessions.len(), 1);
        state.sessions.values().next().expect("session").clone()
    }

    fn session_count(&self) -> usize {
        self.state.lock().expect("repository lock").sessions.len()
    }

    fn insert_calls(&self) -> usize {
        self.insert_calls.load(Ordering::SeqCst)
    }

    fn event_kinds(&self) -> Vec<DomainEventKind> {
        self.state
            .lock()
            .expect("repository lock")
            .events
            .iter()
            .map(|event| event.kind)
            .collect()
    }

    fn seed(&self, session: Session) {
        let mut state = self.state.lock().expect("repository lock");
        state
            .names
            .insert(session.name().clone(), session.id().clone());
        state.sessions.insert(session.id().clone(), session);
    }

    fn recovery_calls(&self) -> Vec<(Option<SessionId>, usize)> {
        self.recovery_calls
            .lock()
            .expect("recovery call lock")
            .clone()
    }

    fn retain_cancellation_recovery(
        &self,
        session_id: SessionId,
        recovery: RuntimeCancellationRecovery,
    ) {
        self.cancellation_recovery
            .lock()
            .expect("cancellation-recovery lock")
            .insert(session_id, recovery);
    }

    fn advance_revision(&self, session_id: &SessionId) {
        let mut state = self.state.lock().expect("repository lock");
        let session = state.sessions.get_mut(session_id).expect("session");
        session
            .update_foreground(ForegroundObservation::unknown(TimestampMillis::new(10_000)))
            .expect("foreground mutation")
            .expect("foreground revision changes");
    }
}

impl SessionRecoverySource for MemoryRepository {
    fn read_batch<'a>(
        &'a self,
        after: Option<&'a SessionId>,
        limit: usize,
    ) -> PortFuture<'a, Result<RecoverySessionBatch, PortError>> {
        Box::pin(async move {
            self.recovery_calls
                .lock()
                .expect("recovery call lock")
                .push((after.cloned(), limit));
            if self
                .fail_next_recovery
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(port_error(PortOperation::ReadRecoverySessions, None));
            }
            let state = self.state.lock().expect("repository lock");
            let lower_bound = match after {
                Some(after) => Excluded(after),
                None => Unbounded,
            };
            let mut range = state.sessions.range((lower_bound, Unbounded));
            let sessions = range
                .by_ref()
                .take(limit)
                .map(|(_, session)| session.clone())
                .collect::<Vec<_>>();
            let has_more = range.next().is_some();
            let next_after = has_more
                .then(|| sessions.last().map(|session| session.id().clone()))
                .flatten();
            Ok(RecoverySessionBatch {
                sessions,
                next_after,
            })
        })
    }

    fn claim_runtime_cancellation_recovery<'a>(
        &'a self,
        session_id: &'a SessionId,
        _observed_at: TimestampMillis,
    ) -> PortFuture<'a, Result<Option<RuntimeCancellationRecovery>, PortError>> {
        Box::pin(async move {
            let mut recovery = self
                .cancellation_recovery
                .lock()
                .expect("cancellation-recovery lock");
            let instruction = recovery.get(session_id).copied();
            if matches!(instruction, Some(RuntimeCancellationRecovery::Resume(_))) {
                recovery.insert(
                    session_id.clone(),
                    RuntimeCancellationRecovery::PreserveUnknown,
                );
            }
            Ok(instruction)
        })
    }
}

impl SessionRepository for MemoryRepository {
    fn insert<'a>(
        &'a self,
        session: &'a Session,
        event: &'a SessionOutboxEvent,
    ) -> PortFuture<'a, Result<InsertSessionOutcome, PortError>> {
        Box::pin(async move {
            self.insert_calls.fetch_add(1, Ordering::SeqCst);
            let mut state = self.state.lock().expect("repository lock");
            if state.sessions.contains_key(session.id()) {
                return Ok(InsertSessionOutcome::IdConflict);
            }
            if let Some(existing_session_id) = state.names.get(session.name()) {
                return Ok(InsertSessionOutcome::NameConflict {
                    existing_session_id: existing_session_id.clone(),
                });
            }
            state
                .names
                .insert(session.name().clone(), session.id().clone());
            state.sessions.insert(session.id().clone(), session.clone());
            state.events.push(event.clone());
            Ok(InsertSessionOutcome::Inserted)
        })
    }

    fn load<'a>(
        &'a self,
        session_id: &'a SessionId,
    ) -> PortFuture<'a, Result<Option<Session>, PortError>> {
        Box::pin(async move {
            Ok(self
                .state
                .lock()
                .expect("repository lock")
                .sessions
                .get(session_id)
                .cloned())
        })
    }

    fn find_by_name<'a>(
        &'a self,
        name: &'a SessionName,
    ) -> PortFuture<'a, Result<Option<Session>, PortError>> {
        Box::pin(async move {
            let state = self.state.lock().expect("repository lock");
            Ok(state
                .names
                .get(name)
                .and_then(|session_id| state.sessions.get(session_id))
                .cloned())
        })
    }

    fn commit<'a>(
        &'a self,
        session: &'a Session,
        expected_revision: Revision,
        events: &'a [SessionOutboxEvent],
    ) -> PortFuture<'a, Result<CommitSessionOutcome, PortError>> {
        Box::pin(async move {
            let commit_call = self.commit_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if self.fail_commit_call.load(Ordering::SeqCst) == commit_call {
                return Err(port_error(
                    PortOperation::CommitSession,
                    Some(session.id().clone()),
                ));
            }
            if self.fail_session_revision.load(Ordering::SeqCst)
                == usize::try_from(session.revision().value()).unwrap_or(usize::MAX)
            {
                return Err(port_error(
                    PortOperation::CommitSession,
                    Some(session.id().clone()),
                ));
            }
            if self
                .fail_next_commit
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(port_error(
                    PortOperation::CommitSession,
                    Some(session.id().clone()),
                ));
            }
            let mut state = self.state.lock().expect("repository lock");
            let Some(current) = state.sessions.get(session.id()) else {
                return Ok(CommitSessionOutcome::NotFound);
            };
            if current.revision() != expected_revision {
                return Ok(CommitSessionOutcome::RevisionConflict {
                    current_revision: current.revision(),
                });
            }
            if let Some(existing_session_id) = state.names.get(session.name())
                && existing_session_id != session.id()
            {
                return Ok(CommitSessionOutcome::NameConflict {
                    existing_session_id: existing_session_id.clone(),
                });
            }
            if current.name() != session.name() {
                let previous_name = current.name().clone();
                state.names.remove(&previous_name);
                state
                    .names
                    .insert(session.name().clone(), session.id().clone());
            }
            state.sessions.insert(session.id().clone(), session.clone());
            state.events.extend_from_slice(events);
            Ok(CommitSessionOutcome::Committed)
        })
    }

    fn delete<'a>(
        &'a self,
        session_id: &'a SessionId,
        expected_revision: Revision,
        event: &'a SessionOutboxEvent,
    ) -> PortFuture<'a, Result<DeleteSessionOutcome, PortError>> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("repository lock");
            let Some(current) = state.sessions.get(session_id) else {
                return Ok(DeleteSessionOutcome::NotFound);
            };
            if current.revision() != expected_revision {
                return Ok(DeleteSessionOutcome::RevisionConflict {
                    current_revision: current.revision(),
                });
            }
            if let Some(removed) = state.sessions.remove(session_id) {
                state.names.remove(removed.name());
            }
            state.events.push(event.clone());
            Ok(DeleteSessionOutcome::Deleted)
        })
    }
}

#[derive(Default)]
struct MemoryInputDeliveries {
    records: Mutex<BTreeMap<(SessionId, DeliveryId), InputDeliveryRecord>>,
    fail_next_settle: AtomicUsize,
}

#[derive(Clone)]
struct InputDeliveryRecord {
    input: InputSequence,
    observation_policy: InputObservationPolicy,
    disposition: Option<InputDeliveryDisposition>,
}

impl InputDeliveryStore for MemoryInputDeliveries {
    fn reserve<'a>(
        &'a self,
        session_id: &'a SessionId,
        delivery_id: &'a DeliveryId,
        input: &'a InputSequence,
        observation_policy: InputObservationPolicy,
    ) -> PortFuture<'a, Result<InputDeliveryReservation, PortError>> {
        Box::pin(async move {
            let key = (session_id.clone(), delivery_id.clone());
            let mut records = self.records.lock().expect("delivery lock");
            let Some(record) = records.get(&key) else {
                records.insert(
                    key,
                    InputDeliveryRecord {
                        input: input.clone(),
                        observation_policy,
                        disposition: None,
                    },
                );
                return Ok(InputDeliveryReservation::Reserved);
            };
            if record.input != *input || record.observation_policy != observation_policy {
                return Ok(InputDeliveryReservation::PayloadConflict);
            }
            Ok(match record.disposition {
                Some(InputDeliveryDisposition::Delivered { delivered_at }) => {
                    InputDeliveryReservation::AlreadyDelivered { delivered_at }
                }
                Some(InputDeliveryDisposition::Indeterminate { .. }) | None => {
                    InputDeliveryReservation::Indeterminate
                }
            })
        })
    }

    fn settle<'a>(
        &'a self,
        session_id: &'a SessionId,
        delivery_id: &'a DeliveryId,
        disposition: InputDeliveryDisposition,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            if self
                .fail_next_settle
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(port_error(
                    PortOperation::SettleInputDelivery,
                    Some(session_id.clone()),
                ));
            }
            let mut records = self.records.lock().expect("delivery lock");
            let record = records
                .get_mut(&(session_id.clone(), delivery_id.clone()))
                .ok_or_else(|| {
                    port_error(PortOperation::SettleInputDelivery, Some(session_id.clone()))
                })?;
            record.disposition = Some(disposition);
            Ok(())
        })
    }
}

struct ReceiptProbe {
    target: undercurrent_domain::OperationAttemptId,
    attempts: Arc<DispatchAttemptStore>,
    fail_acks: usize,
    pending: BTreeSet<undercurrent_domain::OperationAttemptId>,
    acknowledgements: Vec<undercurrent_domain::OperationAttempt>,
}

struct FakeRuntime {
    receipt_probe: Mutex<Option<ReceiptProbe>>,
    fail_prepare: AtomicUsize,
    fail_spawn: AtomicUsize,
    fail_arm: AtomicUsize,
    fail_stop: AtomicUsize,
    fail_force: AtomicUsize,
    spawn_calls: AtomicUsize,
    arm_calls: AtomicUsize,
    stop_calls: AtomicUsize,
    stop_policies: Mutex<Vec<StopPolicy>>,
    force_calls: AtomicUsize,
    resize_calls: AtomicUsize,
    fail_resize_on_call: AtomicUsize,
    resize_sizes: Mutex<Vec<TerminalSize>>,
    write_calls: AtomicUsize,
    close_calls: AtomicUsize,
    input_closed: AtomicUsize,
    input_policies: Mutex<Vec<InputDeliveryPolicy>>,
    fail_write_on_call: AtomicUsize,
    spawn_started_at: AtomicI64,
    active_processes: Mutex<BTreeMap<SessionId, ProcessInstanceId>>,
    prepare_clock_advance: Mutex<Option<(Arc<IncrementingClock>, i64)>>,
}

impl FakeRuntime {
    const fn new() -> Self {
        Self {
            receipt_probe: Mutex::new(None),
            fail_prepare: AtomicUsize::new(0),
            fail_spawn: AtomicUsize::new(0),
            fail_arm: AtomicUsize::new(0),
            fail_stop: AtomicUsize::new(0),
            fail_force: AtomicUsize::new(0),
            spawn_calls: AtomicUsize::new(0),
            arm_calls: AtomicUsize::new(0),
            stop_calls: AtomicUsize::new(0),
            stop_policies: Mutex::new(Vec::new()),
            force_calls: AtomicUsize::new(0),
            resize_calls: AtomicUsize::new(0),
            fail_resize_on_call: AtomicUsize::new(usize::MAX),
            resize_sizes: Mutex::new(Vec::new()),
            write_calls: AtomicUsize::new(0),
            close_calls: AtomicUsize::new(0),
            input_closed: AtomicUsize::new(0),
            input_policies: Mutex::new(Vec::new()),
            fail_write_on_call: AtomicUsize::new(usize::MAX),
            spawn_started_at: AtomicI64::new(105),
            active_processes: Mutex::new(BTreeMap::new()),
            prepare_clock_advance: Mutex::new(None),
        }
    }

    fn advance_clock_during_prepare(&self, clock: Arc<IncrementingClock>, value: i64) {
        *self
            .prepare_clock_advance
            .lock()
            .expect("prepare clock hook") = Some((clock, value));
    }

    fn spawn_calls(&self) -> usize {
        self.spawn_calls.load(Ordering::SeqCst)
    }

    fn force_calls(&self) -> usize {
        self.force_calls.load(Ordering::SeqCst)
    }

    fn arm_calls(&self) -> usize {
        self.arm_calls.load(Ordering::SeqCst)
    }

    fn stop_calls(&self) -> usize {
        self.stop_calls.load(Ordering::SeqCst)
    }

    fn resize_sizes(&self) -> Vec<TerminalSize> {
        self.resize_sizes.lock().expect("resize lock").clone()
    }

    fn write_calls(&self) -> usize {
        self.write_calls.load(Ordering::SeqCst)
    }

    fn close_calls(&self) -> usize {
        self.close_calls.load(Ordering::SeqCst)
    }

    fn input_policies(&self) -> Vec<InputDeliveryPolicy> {
        self.input_policies.lock().expect("policy lock").clone()
    }

    fn is_active(&self, session_id: &SessionId, process: &ProcessInstanceId) -> bool {
        self.active_processes
            .lock()
            .expect("active process lock")
            .get(session_id)
            .is_some_and(|active| active == process)
    }
}

impl ProcessRuntime for FakeRuntime {
    fn spawn_attempted<'a>(
        &'a self,
        context: &'a crate::ProcessMutationContext,
        session_id: &'a SessionId,
        launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<ProcessInstance, PortError>> {
        Box::pin(async move {
            let process = self.spawn(session_id, launch).await?;
            if let Some(probe) = self.receipt_probe.lock().unwrap().as_mut() {
                probe.pending.insert(context.attempt_id().clone());
            }
            Ok(process)
        })
    }

    fn acknowledge_attempt<'a>(
        &'a self,
        attempt: &'a undercurrent_domain::OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            let mut guard = self.receipt_probe.lock().unwrap();
            let Some(probe) = guard.as_mut() else {
                return Ok(());
            };
            if attempt.id() != &probe.target {
                return Ok(());
            }
            assert_eq!(
                probe.attempts.attempt(attempt.id()).as_ref(),
                Some(attempt),
                "ACK follows committed settlement"
            );
            assert!(matches!(
                attempt.state(),
                undercurrent_domain::OperationAttemptState::Settled {
                    certainty: undercurrent_domain::EffectCertainty::KnownApplied,
                    ..
                }
            ));
            probe.acknowledgements.push(attempt.clone());
            if probe.fail_acks > 0 {
                probe.fail_acks -= 1;
                return Err(port_error(
                    PortOperation::SettleOperationAttempt,
                    attempt.target_session().cloned(),
                ));
            }
            probe.pending.remove(attempt.id());
            Ok(())
        })
    }

    fn prepare_launch<'a>(
        &'a self,
        launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<LaunchSpec, PortError>> {
        Box::pin(async move {
            if self
                .fail_prepare
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(port_error(PortOperation::PrepareLaunch, None));
            }
            if let Some((clock, value)) = self
                .prepare_clock_advance
                .lock()
                .expect("prepare clock hook")
                .take()
            {
                clock.set(value);
            }
            let argument_bytes = launch
                .command()
                .arguments()
                .iter()
                .map(String::len)
                .sum::<usize>();
            let limits = ExecutionLimits {
                max_arguments: launch.command().arguments().len().max(1),
                max_command_bytes: argument_bytes.saturating_add(64).max(1),
                max_environment_entries: launch.environment().len().max(1),
                max_environment_bytes: launch.environment().total_bytes().max(1),
                max_path_bytes: 1_024,
                max_health_rules: 1,
                max_health_rule_bytes: 1,
            };
            let command = launch
                .command()
                .clone()
                .pin_executable(
                    "/test/bin/runtime",
                    PayloadDigest::of_bytes(b"fake executable"),
                    PayloadDigest::of_bytes(b"fake executable resolution"),
                    limits,
                )
                .map_err(|source| {
                    PortError::new(
                        PortErrorKind::InvalidData,
                        PortOperation::PrepareLaunch,
                        None,
                        source,
                    )
                })?;
            Ok(launch.clone().with_command(command))
        })
    }

    fn spawn<'a>(
        &'a self,
        session_id: &'a SessionId,
        _launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<ProcessInstance, PortError>> {
        Box::pin(async move {
            self.spawn_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_spawn.load(Ordering::SeqCst) > 0 {
                return Err(port_error(
                    PortOperation::SpawnProcess,
                    Some(session_id.clone()),
                ));
            }
            let process = ProcessInstance {
                id: ProcessInstanceId::new("process-1").expect("process ID"),
                started_at: TimestampMillis::new(self.spawn_started_at.load(Ordering::SeqCst)),
            };
            self.active_processes
                .lock()
                .expect("active process lock")
                .insert(session_id.clone(), process.id.clone());
            Ok(process)
        })
    }

    fn arm_exit<'a>(
        &'a self,
        session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            self.arm_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_arm.load(Ordering::SeqCst) > 0 {
                return Err(port_error(
                    PortOperation::ArmProcessExit,
                    Some(session_id.clone()),
                ));
            }
            Ok(())
        })
    }

    fn stop<'a>(
        &'a self,
        session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
        policy: StopPolicy,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        Box::pin(async move {
            self.stop_policies.lock().unwrap().push(policy);
            self.stop_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_stop.load(Ordering::SeqCst) > 0 {
                return Err(port_error(
                    PortOperation::StopProcess,
                    Some(session_id.clone()),
                ));
            }
            Ok(ProcessTermination::new(
                TerminationKind::Graceful,
                Some(0),
                TimestampMillis::new(110),
            ))
        })
    }

    fn force_stop<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        Box::pin(async move {
            self.force_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_force.load(Ordering::SeqCst) > 0 {
                return Err(port_error(
                    PortOperation::ForceProcess,
                    Some(session_id.clone()),
                ));
            }
            let mut active = self.active_processes.lock().expect("active process lock");
            if active.get(session_id) == Some(process) {
                active.remove(session_id);
            }
            Ok(ProcessTermination::new(
                TerminationKind::Forced,
                None,
                TimestampMillis::new(110),
            ))
        })
    }

    fn write_input<'a>(
        &'a self,
        session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
        _input: &'a InputSequence,
        policy: InputDeliveryPolicy,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            self.input_policies
                .lock()
                .expect("policy lock")
                .push(policy);
            let call = self.write_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if self.fail_write_on_call.load(Ordering::SeqCst) == call {
                return Err(port_error(
                    PortOperation::WriteInput,
                    Some(session_id.clone()),
                ));
            }
            Ok(())
        })
    }

    fn close_input<'a>(
        &'a self,
        _session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<StandardInputClosure, PortError>> {
        Box::pin(async move {
            self.close_calls.fetch_add(1, Ordering::SeqCst);
            Ok(if self.input_closed.swap(1, Ordering::SeqCst) == 0 {
                StandardInputClosure::Closed
            } else {
                StandardInputClosure::AlreadyClosed
            })
        })
    }

    fn resize<'a>(
        &'a self,
        session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
        size: TerminalSize,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            let call = self.resize_calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.resize_sizes.lock().expect("resize lock").push(size);
            if self.fail_resize_on_call.load(Ordering::SeqCst) == call {
                return Err(port_error(
                    PortOperation::ResizeTerminal,
                    Some(session_id.clone()),
                ));
            }
            Ok(())
        })
    }

    fn spawn_admitted<'a>(
        &'a self,
        _admission: &'a crate::OperationAttemptAdmission,
        session_id: &'a SessionId,
        launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<ProcessInstance, PortError>> {
        self.spawn(session_id, launch)
    }

    fn arm_exit_admitted<'a>(
        &'a self,
        _admission: &'a crate::OperationAttemptAdmission,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<(), PortError>> {
        self.arm_exit(session_id, process)
    }

    fn stop_admitted<'a>(
        &'a self,
        _admission: &'a crate::OperationAttemptAdmission,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        policy: StopPolicy,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        self.stop(session_id, process, policy)
    }

    fn force_stop_admitted<'a>(
        &'a self,
        _admission: &'a crate::OperationAttemptAdmission,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<ProcessTermination, PortError>> {
        self.force_stop(session_id, process)
    }

    fn write_input_admitted<'a>(
        &'a self,
        _admission: &'a crate::OperationAttemptAdmission,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        input: &'a InputSequence,
        policy: InputDeliveryPolicy,
    ) -> PortFuture<'a, Result<(), PortError>> {
        self.write_input(session_id, process, input, policy)
    }

    fn resize_admitted<'a>(
        &'a self,
        _admission: &'a crate::OperationAttemptAdmission,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        size: TerminalSize,
    ) -> PortFuture<'a, Result<(), PortError>> {
        self.resize(session_id, process, size)
    }
}

#[derive(Default)]
struct MemoryProcessDirectory {
    processes: Mutex<BTreeMap<SessionId, ProcessInstance>>,
    fail_next_bind: AtomicUsize,
    fail_next_unbind: AtomicUsize,
}

impl SessionProcessDirectory for MemoryProcessDirectory {
    fn bind(&self, session_id: &SessionId, process: ProcessInstance) -> Result<(), PortError> {
        if self
            .fail_next_bind
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(port_error(
                PortOperation::BindProcess,
                Some(session_id.clone()),
            ));
        }
        let mut processes = self.processes.lock().expect("process lock");
        if processes.contains_key(session_id) {
            return Err(port_error(
                PortOperation::BindProcess,
                Some(session_id.clone()),
            ));
        }
        processes.insert(session_id.clone(), process);
        Ok(())
    }

    fn lookup(&self, session_id: &SessionId) -> Result<Option<ProcessInstance>, PortError> {
        Ok(self
            .processes
            .lock()
            .expect("process lock")
            .get(session_id)
            .cloned())
    }

    fn unbind_if(
        &self,
        session_id: &SessionId,
        process: &ProcessInstanceId,
    ) -> Result<ProcessUnbindOutcome, PortError> {
        if self
            .fail_next_unbind
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(port_error(
                PortOperation::UnbindProcess,
                Some(session_id.clone()),
            ));
        }
        let mut processes = self.processes.lock().expect("process lock");
        match processes.get(session_id) {
            None => Ok(ProcessUnbindOutcome::Missing),
            Some(current) if &current.id != process => Ok(ProcessUnbindOutcome::ProcessMismatch),
            Some(_) => Ok(ProcessUnbindOutcome::Unbound(
                processes
                    .remove(session_id)
                    .expect("matching process exists"),
            )),
        }
    }
}

#[derive(Default)]
struct MemoryRestartScheduler {
    jobs: Mutex<Vec<RestartJob>>,
    fail_next: AtomicUsize,
    fail_next_cancel: AtomicUsize,
    cancel_calls: AtomicUsize,
}

impl MemoryRestartScheduler {
    fn jobs(&self) -> Vec<RestartJob> {
        self.jobs.lock().expect("restart job lock").clone()
    }

    fn cancel_calls(&self) -> usize {
        self.cancel_calls.load(Ordering::SeqCst)
    }
}

impl RestartScheduler for MemoryRestartScheduler {
    fn schedule<'a>(&'a self, job: &'a RestartJob) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            if self
                .fail_next
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(port_error(
                    PortOperation::ScheduleRestart,
                    Some(job.session_id.clone()),
                ));
            }
            let mut jobs = self.jobs.lock().expect("restart job lock");
            if !jobs.contains(job) {
                jobs.push(job.clone());
            }
            Ok(())
        })
    }

    fn cancel_session_restarts<'a>(
        &'a self,
        session_id: &'a SessionId,
    ) -> PortFuture<'a, Result<usize, PortError>> {
        Box::pin(async move {
            self.cancel_calls.fetch_add(1, Ordering::SeqCst);
            if self
                .fail_next_cancel
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(port_error(
                    PortOperation::CancelSessionRestarts,
                    Some(session_id.clone()),
                ));
            }
            let mut jobs = self.jobs.lock().expect("restart job lock");
            let prior_len = jobs.len();
            jobs.retain(|job| &job.session_id != session_id);
            Ok(prior_len - jobs.len())
        })
    }
}

#[derive(Default)]
struct MemoryObservationScheduler {
    plans: Mutex<Vec<SessionObservationPlan>>,
}

impl MemoryObservationScheduler {
    fn plans(&self) -> Vec<SessionObservationPlan> {
        self.plans.lock().expect("observation plan lock").clone()
    }
}

impl ObservationScheduler for MemoryObservationScheduler {
    fn schedule_observation<'a>(
        &'a self,
        _job: &'a ObservationJob,
    ) -> PortFuture<'a, Result<ObservationScheduleOutcome, PortError>> {
        Box::pin(async { Ok(ObservationScheduleOutcome::Scheduled) })
    }

    fn cancel_session_observations<'a>(
        &'a self,
        _session_id: &'a SessionId,
    ) -> PortFuture<'a, Result<usize, PortError>> {
        Box::pin(async { Ok(0) })
    }

    fn reconcile_session_observations<'a>(
        &'a self,
        plan: &'a SessionObservationPlan,
    ) -> PortFuture<'a, Result<ObservationReconcileOutcome, PortError>> {
        self.plans
            .lock()
            .expect("observation plan lock")
            .push(plan.clone());
        Box::pin(async { Ok(ObservationReconcileOutcome::default()) })
    }
}

#[derive(Default)]
struct MemoryProcessTruth {
    states: Mutex<BTreeMap<ProcessInstanceId, ProcessTruth>>,
    failures: Mutex<Vec<ProcessInstanceId>>,
    calls: Mutex<Vec<ProcessInstanceId>>,
}

impl MemoryProcessTruth {
    fn set(&self, process_id: ProcessInstanceId, state: ProcessTruth) {
        self.states
            .lock()
            .expect("process truth lock")
            .insert(process_id, state);
    }

    fn fail(&self, process_id: ProcessInstanceId) {
        self.failures
            .lock()
            .expect("process truth failure lock")
            .push(process_id);
    }

    fn call_count(&self) -> usize {
        self.calls.lock().expect("process truth call lock").len()
    }
}

impl ProcessTruthSource for MemoryProcessTruth {
    fn observe<'a>(
        &'a self,
        session_id: &'a SessionId,
        process: &'a undercurrent_domain::ProcessIdentity,
    ) -> PortFuture<'a, Result<ProcessTruth, PortError>> {
        Box::pin(async move {
            self.calls
                .lock()
                .expect("process truth call lock")
                .push(process.id().clone());
            if self
                .failures
                .lock()
                .expect("process truth failure lock")
                .contains(process.id())
            {
                return Err(port_error(
                    PortOperation::ObserveProcessTruth,
                    Some(session_id.clone()),
                ));
            }
            self.states
                .lock()
                .expect("process truth lock")
                .get(process.id())
                .copied()
                .ok_or_else(|| {
                    port_error(PortOperation::ObserveProcessTruth, Some(session_id.clone()))
                })
        })
    }
}

#[derive(Default)]
struct MemoryTelemetry {
    diagnostics: Mutex<Vec<DiagnosticRecord>>,
    metrics: Mutex<Vec<MetricObservation>>,
}

impl MemoryTelemetry {
    fn has_event(&self, event: DiagnosticEventName) -> bool {
        self.diagnostics
            .lock()
            .expect("telemetry lock")
            .iter()
            .any(|record| record.event == event)
    }

    fn metric_count(&self) -> usize {
        self.metrics.lock().expect("telemetry lock").len()
    }
}

impl Telemetry for MemoryTelemetry {
    fn emit_diagnostic(&self, record: &DiagnosticRecord) {
        self.diagnostics
            .lock()
            .expect("telemetry lock")
            .push(record.clone());
    }

    fn record_metric(&self, observation: MetricObservation) {
        self.metrics
            .lock()
            .expect("telemetry lock")
            .push(observation);
    }

    fn record_session_metric(&self, observation: &crate::SessionMetricObservation) {
        self.record_metric(observation.metric());
    }
}

struct FailingProjection;

impl SessionProjectionWriter for FailingProjection {
    fn upsert_session(&self, session: &Session) -> Result<(), PortError> {
        Err(port_error(
            PortOperation::UpdateProjection,
            Some(session.id().clone()),
        ))
    }

    fn remove_summary(&self, session_id: &SessionId) -> Result<(), PortError> {
        Err(port_error(
            PortOperation::UpdateProjection,
            Some(session_id.clone()),
        ))
    }
}

#[derive(Debug)]
struct PrivateAdapterError;

impl Display for PrivateAdapterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("private-adapter-detail")
    }
}

impl Error for PrivateAdapterError {}

fn port_error(operation: PortOperation, session_id: Option<SessionId>) -> PortError {
    PortError::new(
        PortErrorKind::Unavailable,
        operation,
        session_id,
        PrivateAdapterError,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryFixtureState {
    StartingUnowned,
    StartingOwned,
    Running,
    Stopping,
    Stopped,
}

fn recovery_session(
    id: &str,
    name: &str,
    state: RecoveryFixtureState,
    restart: bool,
) -> (Session, Option<ProcessIdentity>) {
    let configuration = if restart {
        command_with_restart(name, RestartPolicy::Always, 3).configuration
    } else {
        command(name).configuration
    };
    let (mut session, _) = Session::create(
        SessionId::new(id).expect("session ID"),
        SessionName::new(name).expect("session name"),
        configuration,
        32,
        TimestampMillis::new(100),
    )
    .expect("session fixture");
    let process = ProcessIdentity::new(
        ProcessInstanceId::new(format!("process-{id}")).expect("process ID"),
        TimestampMillis::new(101),
    );
    match state {
        RecoveryFixtureState::StartingUnowned => (session, None),
        RecoveryFixtureState::StartingOwned => {
            session
                .record_process_ownership(process.clone(), TimestampMillis::new(102))
                .expect("ownership transition")
                .expect("ownership change");
            (session, Some(process))
        }
        RecoveryFixtureState::Running | RecoveryFixtureState::Stopping => {
            session
                .record_process_ownership(process.clone(), TimestampMillis::new(102))
                .expect("ownership transition")
                .expect("ownership change");
            session
                .transition_lifecycle(LifecycleState::Running, TimestampMillis::new(103))
                .expect("running transition")
                .expect("running change");
            if state == RecoveryFixtureState::Stopping {
                session
                    .transition_lifecycle(LifecycleState::Stopping, TimestampMillis::new(104))
                    .expect("stopping transition")
                    .expect("stopping change");
            }
            (session, Some(process))
        }
        RecoveryFixtureState::Stopped => {
            session
                .transition_lifecycle(LifecycleState::Stopped, TimestampMillis::new(101))
                .expect("stopped transition")
                .expect("stopped change");
            (session, None)
        }
    }
}

#[derive(Default)]
struct DispatchAttemptStore {
    attempts: Mutex<
        BTreeMap<undercurrent_domain::OperationAttemptId, undercurrent_domain::OperationAttempt>,
    >,
    settlement_gate: Mutex<DispatchSettlementGate>,
    settlement_ready: Condvar,
    fail_after_settlement_commit: Mutex<Option<undercurrent_domain::OperationAttemptId>>,
    fail_compensation: AtomicUsize,
    settlement_superseder: Mutex<Option<undercurrent_domain::OperationAttempt>>,
    accept_calls: AtomicUsize,
}

#[derive(Default)]
struct DispatchSettlementGate {
    blocked_attempt: Option<undercurrent_domain::OperationAttemptId>,
    waiting: bool,
    released: bool,
}

impl DispatchAttemptStore {
    fn accept_calls(&self) -> usize {
        self.accept_calls.load(Ordering::SeqCst)
    }

    fn fail_after_settlement_commit_once(
        &self,
        attempt_id: undercurrent_domain::OperationAttemptId,
    ) {
        *self
            .fail_after_settlement_commit
            .lock()
            .expect("settlement response-loss injection") = Some(attempt_id);
    }

    fn fail_compensation_once(&self) {
        self.fail_compensation.store(1, Ordering::SeqCst);
    }

    fn supersede_on_settlement(&self, attempt: undercurrent_domain::OperationAttempt) {
        *self
            .settlement_superseder
            .lock()
            .expect("settlement superseder") = Some(attempt);
    }

    fn attempt(
        &self,
        attempt_id: &undercurrent_domain::OperationAttemptId,
    ) -> Option<undercurrent_domain::OperationAttempt> {
        self.attempts
            .lock()
            .expect("attempt lock")
            .get(attempt_id)
            .cloned()
    }

    fn block_settlement(&self, attempt_id: undercurrent_domain::OperationAttemptId) {
        let mut gate = self.settlement_gate.lock().expect("settlement gate");
        *gate = DispatchSettlementGate {
            blocked_attempt: Some(attempt_id),
            waiting: false,
            released: false,
        };
    }

    fn wait_until_settlement_is_blocked(&self) {
        let mut gate = self.settlement_gate.lock().expect("settlement gate");
        while !gate.waiting {
            gate = self
                .settlement_ready
                .wait(gate)
                .expect("settlement gate wait");
        }
    }

    fn release_blocked_settlement(&self) {
        let mut gate = self.settlement_gate.lock().expect("settlement gate");
        gate.released = true;
        self.settlement_ready.notify_all();
    }

    fn await_settlement_release(&self, attempt_id: &undercurrent_domain::OperationAttemptId) {
        let mut gate = self.settlement_gate.lock().expect("settlement gate");
        if gate.blocked_attempt.as_ref() != Some(attempt_id) {
            return;
        }
        gate.waiting = true;
        self.settlement_ready.notify_all();
        while !gate.released {
            gate = self
                .settlement_ready
                .wait(gate)
                .expect("settlement release wait");
        }
    }
}

impl crate::OperationAttemptStore for DispatchAttemptStore {
    fn accept_next<'a>(
        &'a self,
        admission: &'a crate::OperationAttemptAdmission,
    ) -> PortFuture<'a, Result<crate::OperationAttemptAcceptance, PortError>> {
        Box::pin(async move {
            let mut records = self.attempts.lock().expect("attempt lock");
            if let Some(existing) = records.get(admission.attempt_id()) {
                return Ok(if admission.has_same_semantics(existing) {
                    match existing.state() {
                        undercurrent_domain::OperationAttemptState::Accepted => {
                            crate::OperationAttemptAcceptance::Duplicate(existing.clone())
                        }
                        undercurrent_domain::OperationAttemptState::Settled { .. } => {
                            crate::OperationAttemptAcceptance::AlreadySettled(existing.clone())
                        }
                        undercurrent_domain::OperationAttemptState::Compensated { .. } => {
                            crate::OperationAttemptAcceptance::AlreadyCompensated(existing.clone())
                        }
                    }
                } else {
                    crate::OperationAttemptAcceptance::PayloadConflict
                });
            }
            let next = records
                .values()
                .filter(|attempt| {
                    attempt.dispatch_binding().is_none()
                        && attempt.target_session() == admission.target_session()
                })
                .map(|attempt| attempt.fencing_token().value())
                .max()
                .unwrap_or(0)
                .checked_add(1)
                .expect("test fence");
            let attempt = undercurrent_domain::OperationAttempt::accepted(
                admission.attempt_id().clone(),
                admission.target_session().cloned(),
                admission.kind(),
                undercurrent_domain::FencingToken::new(next).expect("test fence"),
                admission.payload_digest(),
                admission.accepted_at(),
            );
            records.insert(attempt.id().clone(), attempt.clone());
            Ok(crate::OperationAttemptAcceptance::Accepted(attempt))
        })
    }

    fn accept<'a>(
        &'a self,
        attempt: &'a undercurrent_domain::OperationAttempt,
    ) -> PortFuture<'a, Result<crate::OperationAttemptAcceptance, PortError>> {
        Box::pin(async move {
            self.accept_calls.fetch_add(1, Ordering::SeqCst);
            let mut records = self.attempts.lock().expect("attempt lock");
            if let Some(existing) = records.get(attempt.id()) {
                return Ok(if existing.has_same_semantics(attempt) {
                    match existing.state() {
                        undercurrent_domain::OperationAttemptState::Accepted => {
                            crate::OperationAttemptAcceptance::Duplicate(existing.clone())
                        }
                        undercurrent_domain::OperationAttemptState::Settled { .. } => {
                            crate::OperationAttemptAcceptance::AlreadySettled(existing.clone())
                        }
                        undercurrent_domain::OperationAttemptState::Compensated { .. } => {
                            crate::OperationAttemptAcceptance::AlreadyCompensated(existing.clone())
                        }
                    }
                } else {
                    crate::OperationAttemptAcceptance::PayloadConflict
                });
            }
            let current = records
                .values()
                .filter(|existing| {
                    match (existing.dispatch_binding(), attempt.dispatch_binding()) {
                        (Some(existing), Some(requested)) => {
                            existing.caller_cell_identity() == requested.caller_cell_identity()
                        }
                        (None, None) => existing.target_session() == attempt.target_session(),
                        _ => false,
                    }
                })
                .map(|existing| existing.fencing_token())
                .max();
            if let Some(current) = current {
                if attempt.fencing_token() < current {
                    return Ok(crate::OperationAttemptAcceptance::StaleFence { current });
                }
                if attempt.fencing_token() == current {
                    return Ok(crate::OperationAttemptAcceptance::PayloadConflict);
                }
            }
            records.insert(attempt.id().clone(), attempt.clone());
            Ok(crate::OperationAttemptAcceptance::Accepted(attempt.clone()))
        })
    }

    fn settle<'a>(
        &'a self,
        settlement: &'a crate::OperationAttemptSettlement,
    ) -> PortFuture<'a, Result<crate::OperationAttemptSettlementOutcome, PortError>> {
        Box::pin(async move {
            self.await_settlement_release(&settlement.attempt_id);
            let mut records = self.attempts.lock().expect("attempt lock");
            if let Some(superseder) = self
                .settlement_superseder
                .lock()
                .expect("settlement superseder")
                .take()
            {
                records.insert(superseder.id().clone(), superseder);
            }
            let Some(requested) = records.get(&settlement.attempt_id).cloned() else {
                return Ok(crate::OperationAttemptSettlementOutcome::NotFound);
            };
            let current = records
                .values()
                .filter(|existing| {
                    match (existing.dispatch_binding(), requested.dispatch_binding()) {
                        (Some(existing), Some(requested)) => {
                            existing.caller_cell_identity() == requested.caller_cell_identity()
                        }
                        (None, None) => existing.target_session() == requested.target_session(),
                        _ => false,
                    }
                })
                .map(|existing| existing.fencing_token())
                .max()
                .unwrap_or(requested.fencing_token());
            if settlement.fencing_token < current {
                return Ok(crate::OperationAttemptSettlementOutcome::StaleFence { current });
            }
            if settlement.fencing_token != requested.fencing_token() {
                return Ok(crate::OperationAttemptSettlementOutcome::PayloadConflict);
            }
            let attempt = records
                .get_mut(&settlement.attempt_id)
                .expect("attempt retained under lock");
            let was_settled = matches!(
                attempt.state(),
                undercurrent_domain::OperationAttemptState::Settled { .. }
            );
            if attempt
                .settle(
                    settlement.fencing_token,
                    settlement.certainty,
                    settlement.result_digest,
                    settlement.settled_at,
                )
                .is_err()
            {
                return Ok(crate::OperationAttemptSettlementOutcome::PayloadConflict);
            }
            let outcome = if was_settled {
                crate::OperationAttemptSettlementOutcome::Duplicate(attempt.clone())
            } else {
                crate::OperationAttemptSettlementOutcome::Settled(attempt.clone())
            };
            let mut response_loss = self
                .fail_after_settlement_commit
                .lock()
                .expect("settlement response-loss injection");
            if response_loss.as_ref() == Some(&settlement.attempt_id) {
                response_loss.take();
                return Err(port_error(
                    PortOperation::SettleOperationAttempt,
                    requested.target_session().cloned(),
                ));
            }
            Ok(outcome)
        })
    }

    fn prepare_compensation<'a>(
        &'a self,
        preparation: &'a crate::OperationAttemptCompensationPreparation,
    ) -> PortFuture<'a, Result<crate::OperationAttemptCompensationPreparationOutcome, PortError>>
    {
        Box::pin(async move {
            let mut records = self.attempts.lock().expect("attempt lock");
            let Some(requested) = records.get(preparation.attempt_id()).cloned() else {
                return Ok(crate::OperationAttemptCompensationPreparationOutcome::NotFound);
            };
            if !matches!(
                requested.state(),
                undercurrent_domain::OperationAttemptState::Accepted
            ) {
                return Ok(crate::OperationAttemptCompensationPreparationOutcome::NotAccepted);
            }
            if requested.fencing_token() != preparation.fencing_token() {
                return Ok(crate::OperationAttemptCompensationPreparationOutcome::PayloadConflict);
            }
            let current = records
                .values()
                .filter(|existing| {
                    matches!(
                        (existing.dispatch_binding(), requested.dispatch_binding()),
                        (Some(existing), Some(requested))
                            if existing.caller_cell_identity() == requested.caller_cell_identity()
                    )
                })
                .map(|existing| existing.fencing_token())
                .max();
            let Some(current) = current else {
                return Ok(crate::OperationAttemptCompensationPreparationOutcome::PayloadConflict);
            };
            if current > preparation.superseding_fencing_token() {
                return Ok(
                    crate::OperationAttemptCompensationPreparationOutcome::StaleFence { current },
                );
            }
            if current != preparation.superseding_fencing_token()
                || current <= preparation.fencing_token()
            {
                return Ok(crate::OperationAttemptCompensationPreparationOutcome::PayloadConflict);
            }
            let attempt = records
                .get_mut(preparation.attempt_id())
                .expect("attempt retained");
            let duplicate = attempt.compensation_intent().is_some();
            if attempt
                .prepare_compensation(
                    preparation.fencing_token(),
                    preparation.superseding_fencing_token(),
                    preparation.process().clone(),
                    preparation.session_revision(),
                    preparation.cleanup_digest(),
                    preparation.prepared_at(),
                )
                .is_err()
            {
                return Ok(crate::OperationAttemptCompensationPreparationOutcome::PayloadConflict);
            }
            Ok(if duplicate {
                crate::OperationAttemptCompensationPreparationOutcome::Duplicate(attempt.clone())
            } else {
                crate::OperationAttemptCompensationPreparationOutcome::Prepared(attempt.clone())
            })
        })
    }

    fn compensate<'a>(
        &'a self,
        compensation: &'a crate::OperationAttemptCompensation,
    ) -> PortFuture<'a, Result<crate::OperationAttemptCompensationOutcome, PortError>> {
        Box::pin(async move {
            if self.fail_compensation.swap(0, Ordering::SeqCst) > 0 {
                return Err(port_error(PortOperation::CompensateOperationAttempt, None));
            }
            let mut records = self.attempts.lock().expect("attempt lock");
            let Some(requested) = records.get(compensation.attempt_id()).cloned() else {
                return Ok(crate::OperationAttemptCompensationOutcome::NotFound);
            };
            if matches!(
                requested.state(),
                undercurrent_domain::OperationAttemptState::Compensated { .. }
            ) {
                let attempt = records
                    .get_mut(compensation.attempt_id())
                    .expect("attempt retained");
                return Ok(
                    if attempt
                        .compensate(
                            compensation.fencing_token(),
                            compensation.superseding_fencing_token(),
                            compensation.cleanup_digest(),
                            compensation.compensated_at(),
                        )
                        .is_ok()
                    {
                        crate::OperationAttemptCompensationOutcome::Duplicate(attempt.clone())
                    } else {
                        crate::OperationAttemptCompensationOutcome::PayloadConflict
                    },
                );
            }
            if !matches!(
                requested.state(),
                undercurrent_domain::OperationAttemptState::Accepted
            ) {
                return Ok(crate::OperationAttemptCompensationOutcome::NotAccepted);
            }
            if requested.fencing_token() != compensation.fencing_token() {
                return Ok(crate::OperationAttemptCompensationOutcome::PayloadConflict);
            }
            let current = records
                .values()
                .filter(|existing| {
                    matches!(
                        (existing.dispatch_binding(), requested.dispatch_binding()),
                        (Some(existing), Some(requested))
                            if existing.caller_cell_identity() == requested.caller_cell_identity()
                    )
                })
                .map(|existing| existing.fencing_token())
                .max();
            let Some(current) = current else {
                return Ok(crate::OperationAttemptCompensationOutcome::PayloadConflict);
            };
            if current < compensation.superseding_fencing_token()
                || compensation.superseding_fencing_token() <= compensation.fencing_token()
            {
                return Ok(crate::OperationAttemptCompensationOutcome::PayloadConflict);
            }
            let attempt = records
                .get_mut(compensation.attempt_id())
                .expect("attempt retained");
            if attempt
                .compensate(
                    compensation.fencing_token(),
                    compensation.superseding_fencing_token(),
                    compensation.cleanup_digest(),
                    compensation.compensated_at(),
                )
                .is_err()
            {
                return Ok(crate::OperationAttemptCompensationOutcome::PayloadConflict);
            }
            Ok(crate::OperationAttemptCompensationOutcome::Compensated(
                attempt.clone(),
            ))
        })
    }

    fn reconcile<'a>(
        &'a self,
        _reconciliation: &'a crate::OperationAttemptReconciliation,
    ) -> PortFuture<'a, Result<crate::OperationAttemptSettlementOutcome, PortError>> {
        Box::pin(async { Ok(crate::OperationAttemptSettlementOutcome::NotFound) })
    }

    fn load<'a>(
        &'a self,
        attempt_id: &'a undercurrent_domain::OperationAttemptId,
    ) -> PortFuture<'a, Result<Option<undercurrent_domain::OperationAttempt>, PortError>> {
        Box::pin(async move {
            Ok(self
                .attempts
                .lock()
                .expect("attempt lock")
                .get(attempt_id)
                .cloned())
        })
    }

    fn pending_reconciliation<'a>(
        &'a self,
        _scan: &'a crate::OperationAttemptScan,
    ) -> PortFuture<'a, Result<Vec<undercurrent_domain::OperationAttempt>, PortError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

fn command(name: &str) -> StartSessionCommand {
    let limits = ExecutionLimits {
        max_arguments: 8,
        max_command_bytes: 1_024,
        max_environment_entries: 8,
        max_environment_bytes: 1_024,
        max_path_bytes: 1_024,
        max_health_rules: 8,
        max_health_rule_bytes: 1_024,
    };
    let launch = LaunchSpec::new(
        CommandSpec::new("sh", vec!["-lc".into(), "sleep 60".into()], limits).expect("command"),
        EnvironmentVariables::new([], limits).expect("environment"),
        None,
        None,
        TerminalSize::new(80, 24, 500, 500).expect("terminal size"),
    );
    let execution = ExecutionPolicy::new(
        RestartConfig::never(),
        StopPolicy::new(StopSignal::Terminate, Duration::from_secs(5)).expect("stop policy"),
        true,
        false,
    );
    StartSessionCommand {
        name: SessionName::new(name).expect("session name"),
        configuration: SessionConfiguration::new(
            launch,
            execution,
            HealthRules::default(),
            None,
            Labels::default(),
            Metadata::new([("token".into(), "private".into())]).expect("metadata"),
        ),
        max_work_items: 32,
        dispatch: None,
    }
}

fn runtime_dispatch_binding(
    cell: &str,
    launch_profile: &[u8],
    environment_binding: &[u8],
    input: &[u8],
    expires_at: i64,
) -> undercurrent_domain::RuntimeDispatchBinding {
    undercurrent_domain::RuntimeDispatchBinding::new(
        undercurrent_domain::DispatchCellIdentity::new(cell).expect("dispatch cell"),
        undercurrent_domain::PayloadDigest::of_bytes(b"caller dispatch request"),
        undercurrent_domain::PayloadDigest::of_bytes(launch_profile),
        undercurrent_domain::PayloadDigest::of_bytes(environment_binding),
        undercurrent_domain::PayloadDigest::of_bytes(input),
        undercurrent_domain::RuntimeDispatchPolicy::new(
            undercurrent_domain::RuntimeDispatchLimits::new(30_000, 1_024, 2_048, None, None)
                .expect("runtime limits"),
            undercurrent_domain::RuntimeOutputContract::new(
                "application-test/0.1.0-beta",
                undercurrent_domain::PayloadDigest::of_bytes(b"application-test-output-schema"),
                undercurrent_domain::Classification::Internal,
            )
            .expect("output contract"),
            undercurrent_domain::RuntimeTelemetryPolicyRef::new(
                "trust://telemetry/policies/application-test",
            )
            .expect("telemetry policy reference"),
            undercurrent_domain::RuntimeRetentionPolicyRef::new(
                "trust://retention/policies/application-test",
            )
            .expect("retention policy reference"),
            vec![
                undercurrent_domain::RuntimeCallbackRef::new(
                    "runtime-callback://example/workflow/v0.1.0-beta/application-test",
                )
                .expect("callback reference"),
            ],
            TimestampMillis::new(expires_at),
            TimestampMillis::new(expires_at + 100),
        )
        .expect("runtime policy"),
    )
}

fn command_with_restart(
    name: &str,
    policy: RestartPolicy,
    max_restarts: u32,
) -> StartSessionCommand {
    let limits = ExecutionLimits {
        max_arguments: 8,
        max_command_bytes: 1_024,
        max_environment_entries: 8,
        max_environment_bytes: 1_024,
        max_path_bytes: 1_024,
        max_health_rules: 8,
        max_health_rule_bytes: 1_024,
    };
    let launch = LaunchSpec::new(
        CommandSpec::new("sh", vec!["-lc".into(), "sleep 60".into()], limits).expect("command"),
        EnvironmentVariables::new([], limits).expect("environment"),
        None,
        None,
        TerminalSize::new(80, 24, 500, 500).expect("terminal size"),
    );
    let restart = RestartConfig::new(
        policy,
        max_restarts,
        Duration::from_millis(25),
        10_000,
        Duration::from_millis(25),
        Duration::from_secs(30),
    )
    .expect("restart policy");
    StartSessionCommand {
        name: SessionName::new(name).expect("session name"),
        configuration: SessionConfiguration::new(
            launch,
            ExecutionPolicy::new(
                restart,
                StopPolicy::new(StopSignal::Terminate, Duration::from_secs(5))
                    .expect("stop policy"),
                true,
                false,
            ),
            HealthRules::default(),
            None,
            Labels::default(),
            Metadata::default(),
        ),
        max_work_items: 32,
        dispatch: None,
    }
}

fn work_item() -> WorkItem {
    WorkItem::new(
        WorkItemKey::new(
            WorkOwner::new("workflow").expect("owner is valid"),
            WorkItemId::new("task-1").expect("work ID is valid"),
        ),
        WorkRelationKind::DependsOn,
        "dependent task",
        None,
        WorkStatus::Waiting,
        [undercurrent_domain::WorkTarget::Session(
            SessionId::new("dependency").expect("target ID is valid"),
        )],
        Metadata::default(),
        TimestampMillis::new(110),
        TimestampMillis::new(110),
        None,
    )
    .expect("work item is valid")
}

fn input_command(session_id: SessionId, delivery_id: &str, text: &str) -> SendTerminalInputCommand {
    SendTerminalInputCommand {
        expected_revision: None,
        session_id,
        input: InputSequence::new(vec![InputItem::Text(text.to_owned())], 8, 1_024)
            .expect("input is valid"),
        delivery_id: Some(DeliveryId::new(delivery_id).expect("delivery ID is valid")),
        observation_policy: InputObservationPolicy::ForceActive,
    }
}

fn test_input_profile(name: ProfileName, policy: InputDeliveryPolicy) -> DetectionProfile {
    DetectionProfile::new(
        name,
        None,
        false,
        ProfileMatchRules::default(),
        DetectionArchetype::AnimatedAgent,
        policy,
        None,
        Vec::new(),
        Vec::new(),
        true,
        false,
        OutputExclusions::default(),
        DetectionProfileLimits {
            max_description_bytes: 256,
            max_binary_patterns: 8,
            max_argument_patterns: 8,
            max_environment_names: 8,
            max_environment_name_bytes: 128,
            max_foreground_rules: 8,
            max_background_rules: 8,
            max_patterns_per_rule: 8,
            max_pattern_bytes: 256,
            max_trait_needles: 8,
            max_trait_text_bytes: 256,
            max_input_submission_delay_millis: 2_000,
        },
    )
    .expect("test profile")
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

fn block_on<T>(future: impl Future<Output = T>) -> T {
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

#[derive(Default)]
struct TestAttemptIds(AtomicUsize);
static TEST_ATTEMPT_IDS: TestAttemptIds = TestAttemptIds(AtomicUsize::new(0));
impl crate::OperationAttemptIdSource for TestAttemptIds {
    fn next_attempt_id(&self) -> Result<undercurrent_domain::OperationAttemptId, PortError> {
        Ok(undercurrent_domain::OperationAttemptId::new(format!(
            "input-{}",
            self.0.fetch_add(1, Ordering::SeqCst)
        ))
        .unwrap())
    }
}
