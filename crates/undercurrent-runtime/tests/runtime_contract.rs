// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::sync::Arc;
use std::sync::mpsc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use undercurrent_application::{
    ApplicationOperation, Clock, CommitSessionOutcome, DiagnosticEventName, DiagnosticLevel,
    DiagnosticRecord, DispatchOutboxService, DueObservationSource, DueRestartSource,
    ForegroundObservationWake, InputDeliveryDisposition, InputDeliveryReservation,
    InputDeliveryStore, InsertSessionOutcome, MetricName, MetricObservation, ObservationJob,
    ObservationJobKind, ObservationReconcileOutcome, ObservationScheduleOutcome,
    ObservationScheduler, OutboxAcknowledgeOutcome, OutboxDispatchLimits, OutboxSequence,
    PortErrorKind, ProcessInstance, RestartJob, RestartScheduler, SessionMutationCoordinator,
    SessionObservationPlan, SessionOutbox, SessionOutboxEvent, SessionProcessDirectory,
    SessionRecoverySource, SessionRepository, Telemetry,
};
use undercurrent_domain::{
    CommandSpec, DeliveryId, EnvironmentVariables, ExecutionLimits, ExecutionPolicy, HealthRules,
    InputItem, InputObservationPolicy, InputSequence, Labels, LaunchSpec, LifecycleState, Metadata,
    ProcessIdentity, ProcessInstanceId, ProfileName, RestartConfig, Revision, Session,
    SessionConfiguration, SessionId, SessionName, StopPolicy, StopSignal, TerminalSize,
    TimestampMillis,
};
use undercurrent_runtime::{
    BoundedInputDeliveryStore, BoundedObservationQueue, BoundedRestartQueue,
    BoundedSessionEventJournal, BoundedTelemetryRecorder, InMemoryProcessDirectory,
    InMemorySessionRepository, KeyedSessionMutationCoordinator, MonotonicSessionIdSource,
    SessionMutationCoordinatorLimits, SystemClock,
};

#[test]
fn repository_commits_session_and_outbox_atomically_under_capacity() {
    let repository = InMemorySessionRepository::new(2, 1).expect("repository");
    let mut session = session(1);
    let created = event(
        &session,
        undercurrent_domain::DomainEventKind::SessionCreated,
    );
    assert_eq!(
        block_on(repository.insert(&session, &created)).expect("insert"),
        InsertSessionOutcome::Inserted
    );
    let prior = session.revision();
    let change = session
        .rename(session_name("renamed"), TimestampMillis::new(20))
        .expect("rename")
        .expect("changed");
    let renamed = SessionOutboxEvent {
        kind: change.kind(),
        session_id: session.id().clone(),
        revision: change.revision(),
        occurred_at: TimestampMillis::new(20),
    };

    let error = block_on(repository.commit(&session, prior, &[renamed]))
        .expect_err("full outbox rejects entire transaction");
    assert_eq!(error.kind(), PortErrorKind::CapacityExceeded);
    let durable = block_on(repository.load(session.id()))
        .expect("load")
        .expect("session");
    assert_ne!(durable.name(), session.name());
    assert_eq!(repository.pressure().outbox_events, 1);

    let pending = block_on(repository.read_pending(1)).expect("pending outbox record");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].sequence, OutboxSequence::FIRST);
    assert_eq!(pending[0].event, created);
    assert_eq!(
        block_on(repository.acknowledge_through(OutboxSequence::FIRST))
            .expect("outbox acknowledgement"),
        OutboxAcknowledgeOutcome::Acknowledged {
            records: 1,
            through: OutboxSequence::FIRST,
        }
    );
    assert_eq!(
        block_on(repository.acknowledge_through(OutboxSequence::FIRST))
            .expect("repeated acknowledgement"),
        OutboxAcknowledgeOutcome::AlreadyAcknowledged {
            through: OutboxSequence::FIRST,
        }
    );
    assert_eq!(
        block_on(
            repository.acknowledge_through(OutboxSequence::new(99).expect("sequence is valid"))
        )
        .expect("unknown acknowledgement is typed"),
        OutboxAcknowledgeOutcome::UnknownSequence {
            requested: OutboxSequence::new(99).expect("sequence is valid"),
        }
    );
    let renamed = SessionOutboxEvent {
        kind: change.kind(),
        session_id: session.id().clone(),
        revision: change.revision(),
        occurred_at: TimestampMillis::new(20),
    };
    assert_eq!(
        block_on(repository.commit(&session, prior, &[renamed])).expect("commit"),
        CommitSessionOutcome::Committed
    );
    let pending = block_on(repository.read_pending(1)).expect("renamed outbox record");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].sequence.value(), 2);
    assert!(matches!(
        block_on(repository.read_pending(0)),
        Err(error) if error.kind() == PortErrorKind::InvalidData
    ));
}

#[test]
fn outbox_dispatch_connects_repository_event_retention_and_observation_activation() {
    let repository = InMemorySessionRepository::new(1, 4).expect("repository");
    let mut session = session(1);
    session
        .record_process_ownership(
            ProcessIdentity::new(
                ProcessInstanceId::new("process-1").expect("process ID"),
                TimestampMillis::new(11),
            ),
            TimestampMillis::new(11),
        )
        .expect("process ownership");
    session
        .transition_lifecycle(LifecycleState::Running, TimestampMillis::new(12))
        .expect("running lifecycle");
    let lifecycle = event(
        &session,
        undercurrent_domain::DomainEventKind::LifecycleChanged,
    );
    assert_eq!(
        block_on(repository.insert(&session, &lifecycle)).expect("session insert"),
        InsertSessionOutcome::Inserted
    );
    let journal = BoundedSessionEventJournal::new(4, 4).expect("event journal");
    let observations = BoundedObservationQueue::new(2).expect("observation queue");
    let telemetry = BoundedTelemetryRecorder::new(32, 32).expect("telemetry");
    let clock = FixedClock(TimestampMillis::new(20));
    let service = DispatchOutboxService::new(
        &clock,
        &repository,
        &repository,
        &journal,
        &observations,
        &telemetry,
        OutboxDispatchLimits::new(4, Duration::from_secs(1), Duration::from_secs(2))
            .expect("dispatch limits"),
    );

    let report = block_on(service.execute()).expect("outbox dispatch");

    assert_eq!(report.processed, 1);
    assert_eq!(report.acknowledged, 1);
    assert_eq!(repository.pressure().outbox_events, 0);
    assert_eq!(journal.pressure().retained_records, 1);
    assert_eq!(observations.pressure().jobs, 1);
    let due = observations
        .claim_due_observations(TimestampMillis::new(1_012), 2)
        .expect("foreground deadline");
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].kind, ObservationJobKind::Foreground);
}

#[test]
fn recovery_reads_three_hundred_sessions_in_stable_bounded_pages() {
    let repository = InMemorySessionRepository::new(300, 300).expect("repository");
    for index in 0..300 {
        let session = session(index);
        let created = event(
            &session,
            undercurrent_domain::DomainEventKind::SessionCreated,
        );
        assert_eq!(
            block_on(repository.insert(&session, &created)).expect("insert"),
            InsertSessionOutcome::Inserted
        );
    }

    let mut after = None;
    let mut observed = Vec::new();
    loop {
        let page = block_on(repository.read_batch(after.as_ref(), 37)).expect("recovery page");
        observed.extend(page.sessions.iter().map(|session| session.id().clone()));
        let Some(next) = page.next_after else {
            break;
        };
        assert!(page.sessions.len() <= 37);
        after = Some(next);
    }

    assert_eq!(observed.len(), 300);
    assert!(observed.windows(2).all(|ids| ids[0] < ids[1]));
}

#[test]
fn keyed_mutation_lease_blocks_only_the_same_session_and_cleans_registry() {
    let coordinator = Arc::new(KeyedSessionMutationCoordinator::new(
        SessionMutationCoordinatorLimits::new(4, 2, 4).expect("coordinator limits"),
    ));
    let target = session_id("session-1");
    let first = block_on(coordinator.acquire(&target)).expect("first lease");
    let independent = block_on(coordinator.acquire(&session_id("session-2")))
        .expect("independent session does not wait");
    drop(independent);
    let (acquired_tx, acquired_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let waiting = coordinator.clone();
    let waiting_target = target.clone();
    let worker = thread::spawn(move || {
        let second = block_on(waiting.acquire(&waiting_target)).expect("second lease");
        acquired_tx.send(()).expect("acquired signal");
        release_rx.recv().expect("release signal");
        drop(second);
    });

    assert!(acquired_rx.recv_timeout(Duration::from_millis(20)).is_err());
    drop(first);
    acquired_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("second lease acquires after release");
    release_tx.send(()).expect("release second");
    worker.join().expect("worker");
    assert_eq!(
        coordinator.pressure(),
        undercurrent_runtime::SessionMutationPressure {
            active_sessions: 0,
            held_sessions: 0,
            waiting_callers: 0,
            capacity_rejections: 0,
        }
    );
}

#[test]
fn cancelled_mutation_waiter_releases_capacity_and_does_not_block_successors() {
    let coordinator = KeyedSessionMutationCoordinator::new(
        SessionMutationCoordinatorLimits::new(1, 1, 1).unwrap(),
    );
    let target = session_id("cancelled-waiter");
    let held = block_on(coordinator.acquire(&target)).unwrap();
    let mut pending = coordinator.acquire(&target);
    let mut context = Context::from_waker(Waker::noop());
    assert!(pending.as_mut().poll(&mut context).is_pending());
    assert_eq!(coordinator.pressure().waiting_callers, 1);
    drop(pending);
    assert_eq!(coordinator.pressure().waiting_callers, 0);
    let mut successor = coordinator.acquire(&target);
    assert!(successor.as_mut().poll(&mut context).is_pending());
    drop(held);
    drop(block_on(successor).unwrap());
    assert_eq!(coordinator.pressure().active_sessions, 0);
}

#[test]
fn mutation_coordinator_rejects_active_key_and_waiter_capacity() {
    let (metric_tx, metric_rx) = mpsc::channel();
    let telemetry = Arc::new(SignalingTelemetry {
        recorder: BoundedTelemetryRecorder::new(1, 8).expect("telemetry"),
        metric_tx,
    });
    let coordinator = Arc::new(KeyedSessionMutationCoordinator::with_telemetry(
        SessionMutationCoordinatorLimits::new(1, 1, 1).expect("coordinator limits"),
        telemetry.clone(),
    ));
    let target = session_id("session-1");
    let first = block_on(coordinator.acquire(&target)).expect("first lease");
    let active_error = match block_on(coordinator.acquire(&session_id("session-2"))) {
        Ok(_) => panic!("active key capacity must be enforced"),
        Err(error) => error,
    };
    assert_eq!(active_error.kind(), PortErrorKind::CapacityExceeded);
    assert_eq!(
        active_error.operation(),
        undercurrent_application::PortOperation::AcquireSessionMutation
    );

    let (acquired_tx, acquired_rx) = mpsc::channel();
    let waiting = coordinator.clone();
    let waiting_target = target.clone();
    let worker = thread::spawn(move || {
        let lease = block_on(waiting.acquire(&waiting_target)).expect("bounded waiter acquires");
        acquired_tx.send(()).expect("acquired signal");
        drop(lease);
    });
    loop {
        let metric = metric_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter depth metric was not published");
        if metric.name == MetricName::SupervisorMailboxDepth && metric.value == 1.0 {
            break;
        }
    }
    assert_eq!(coordinator.pressure().waiting_callers, 1);
    assert!(telemetry.recorder.metric_aggregates().iter().any(|metric| {
        metric.name == MetricName::SupervisorMailboxDepth && metric.latest == 1.0
    }));
    let waiter_error = match block_on(coordinator.acquire(&target)) {
        Ok(_) => panic!("per-session waiter capacity must be enforced"),
        Err(error) => error,
    };
    assert_eq!(waiter_error.kind(), PortErrorKind::CapacityExceeded);
    assert_eq!(waiter_error.session_id(), Some(&target));
    assert_eq!(coordinator.pressure().capacity_rejections, 2);

    drop(first);
    acquired_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("bounded waiter acquires after release");
    worker.join().expect("waiter worker");
    assert_eq!(coordinator.pressure().active_sessions, 0);
    assert!(telemetry.recorder.metric_aggregates().iter().any(|metric| {
        metric.name == MetricName::SupervisorMailboxDepth && metric.latest == 0.0
    }));
    assert!(
        telemetry
            .recorder
            .metric_aggregates()
            .iter()
            .any(|metric| { metric.name == MetricName::OverloadRejections && metric.total == 2.0 })
    );
    assert!(telemetry.recorder.metric_aggregates().iter().any(|metric| {
        metric.name == MetricName::SupervisorCommandDurationSeconds && metric.samples >= 4
    }));
}

#[test]
fn mutation_waiters_acquire_in_registration_order() {
    let coordinator = Arc::new(KeyedSessionMutationCoordinator::new(
        SessionMutationCoordinatorLimits::new(1, 2, 1).expect("coordinator limits"),
    ));
    let target = session_id("session-1");
    let first = block_on(coordinator.acquire(&target)).expect("first lease");
    let (acquired_tx, acquired_rx) = mpsc::channel();
    let (release_first_tx, release_first_rx) = mpsc::channel();
    let first_waiter_coordinator = coordinator.clone();
    let first_waiter_target = target.clone();
    let first_waiter_tx = acquired_tx.clone();
    let first_waiter = thread::spawn(move || {
        let lease = block_on(first_waiter_coordinator.acquire(&first_waiter_target))
            .expect("first waiter acquires");
        first_waiter_tx.send(1).expect("first waiter signal");
        release_first_rx.recv().expect("first waiter release");
        drop(lease);
    });
    wait_for_mutation_waiters(coordinator.as_ref(), 1);

    let second_waiter_coordinator = coordinator.clone();
    let second_waiter_target = target.clone();
    let second_waiter = thread::spawn(move || {
        let lease = block_on(second_waiter_coordinator.acquire(&second_waiter_target))
            .expect("second waiter acquires");
        acquired_tx.send(2).expect("second waiter signal");
        drop(lease);
    });
    wait_for_mutation_waiters(coordinator.as_ref(), 2);

    drop(first);
    assert_eq!(
        acquired_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first waiter acquires"),
        1
    );
    assert!(acquired_rx.recv_timeout(Duration::from_millis(20)).is_err());
    release_first_tx.send(()).expect("release first waiter");
    assert_eq!(
        acquired_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second waiter acquires"),
        2
    );
    first_waiter.join().expect("first waiter worker");
    second_waiter.join().expect("second waiter worker");
    assert_eq!(coordinator.pressure().active_sessions, 0);
}

#[test]
fn three_hundred_mutation_keys_have_page_bounded_diagnostics() {
    assert_eq!(
        SessionMutationCoordinatorLimits::new(0, 1, 1)
            .expect_err("active session bound is required")
            .field(),
        "mutation_active_sessions"
    );
    assert_eq!(
        SessionMutationCoordinatorLimits::new(1, 0, 1)
            .expect_err("waiter bound is required")
            .field(),
        "mutation_waiters_per_session"
    );
    assert_eq!(
        SessionMutationCoordinatorLimits::new(1, 1, 0)
            .expect_err("diagnostic page bound is required")
            .field(),
        "mutation_diagnostic_page_size"
    );
    let coordinator = KeyedSessionMutationCoordinator::new(
        SessionMutationCoordinatorLimits::new(300, 1, 23).expect("coordinator limits"),
    );
    let mut leases = Vec::with_capacity(300);
    for index in 0..300 {
        leases.push(
            block_on(coordinator.acquire(&session_id(&format!("session-{index:03}"))))
                .expect("session lease"),
        );
    }

    let mut after = None;
    let mut observed = Vec::with_capacity(300);
    let mut pages = 0;
    loop {
        let page = coordinator
            .diagnostic_page(after.as_ref(), 23)
            .expect("diagnostic page");
        assert!(page.items.len() <= 23);
        assert!(page.cost.session_entries_examined <= 24);
        assert_eq!(page.cost.pressure_entries_sampled, 300);
        assert_eq!(page.pressure.active_sessions, 300);
        assert_eq!(page.pressure.held_sessions, 300);
        assert_eq!(page.pressure.waiting_callers, 0);
        observed.extend(page.items.iter().map(|item| item.session_id.clone()));
        pages += 1;
        let Some(next) = page.next_after else {
            break;
        };
        after = Some(next);
    }
    assert_eq!(observed.len(), 300);
    assert!(observed.windows(2).all(|ids| ids[0] < ids[1]));
    assert_eq!(pages, 14);
    let zero_page = coordinator
        .diagnostic_page(None, 0)
        .expect_err("zero page is invalid");
    assert_eq!(zero_page.requested(), 0);
    assert_eq!(zero_page.maximum(), 23);
    let oversized_page = coordinator
        .diagnostic_page(None, 24)
        .expect_err("oversized page is invalid");
    assert_eq!(oversized_page.requested(), 24);
    assert_eq!(oversized_page.maximum(), 23);

    drop(leases);
    assert_eq!(coordinator.pressure().active_sessions, 0);
}

#[test]
fn process_directory_and_delivery_store_use_exact_incarnation_and_payload_identity() {
    let directory = InMemoryProcessDirectory::default();
    let session_id = session_id("session-1");
    let process = ProcessInstance {
        id: undercurrent_domain::ProcessInstanceId::new("process-1").expect("process ID"),
        started_at: TimestampMillis::new(10),
    };
    directory
        .bind(&session_id, process.clone())
        .expect("process binds");
    assert!(directory.bind(&session_id, process.clone()).is_err());
    assert!(matches!(
        directory
            .unbind_if(
                &session_id,
                &undercurrent_domain::ProcessInstanceId::new("other").expect("process ID")
            )
            .expect("unbind result"),
        undercurrent_application::ProcessUnbindOutcome::ProcessMismatch
    ));

    let deliveries = BoundedInputDeliveryStore::new(1).expect("deliveries");
    let delivery_id = DeliveryId::new("delivery-1").expect("delivery ID");
    let input =
        InputSequence::new(vec![InputItem::Text("hello".to_owned())], 4, 64).expect("input");
    assert_eq!(
        block_on(deliveries.reserve(
            &session_id,
            &delivery_id,
            &input,
            InputObservationPolicy::ForceActive,
        ))
        .expect("reserve"),
        InputDeliveryReservation::Reserved
    );
    block_on(deliveries.settle(
        &session_id,
        &delivery_id,
        InputDeliveryDisposition::Delivered {
            delivered_at: TimestampMillis::new(30),
        },
    ))
    .expect("settle");
    assert_eq!(
        block_on(deliveries.reserve(
            &session_id,
            &delivery_id,
            &input,
            InputObservationPolicy::ForceActive,
        ))
        .expect("deduplicate"),
        InputDeliveryReservation::AlreadyDelivered {
            delivered_at: TimestampMillis::new(30)
        }
    );
}

#[test]
fn restart_and_telemetry_queues_are_bounded_and_observable() {
    let restarts = BoundedRestartQueue::new(1).expect("restart queue");
    let job = RestartJob {
        session_id: session_id("session-1"),
        expected_revision: Revision::new(4),
        attempt: 1,
        not_before: TimestampMillis::new(50),
    };
    block_on(restarts.schedule(&job)).expect("schedule");
    block_on(restarts.schedule(&job)).expect("deduplicated schedule");
    assert_eq!(restarts.len(), 1);
    assert!(
        restarts
            .claim_due_restarts(TimestampMillis::new(49), 1)
            .expect("claim before deadline")
            .is_empty()
    );
    assert_eq!(
        restarts
            .claim_due_restarts(TimestampMillis::new(50), 1)
            .expect("claim due restart"),
        vec![job]
    );
    let invalid = restarts
        .claim_due_restarts(TimestampMillis::new(50), 0)
        .expect_err("zero claim is invalid");
    assert_eq!(invalid.kind(), PortErrorKind::InvalidData);
    assert_eq!(
        invalid.operation(),
        undercurrent_application::PortOperation::ClaimDueRestarts
    );

    let telemetry = BoundedTelemetryRecorder::new(1, 1).expect("telemetry");
    for timestamp in [1, 2] {
        telemetry.emit_diagnostic(&DiagnosticRecord {
            event: DiagnosticEventName::SessionCreated,
            level: DiagnosticLevel::Info,
            timestamp: TimestampMillis::new(timestamp),
            session_id: None,
            application_operation: Some(ApplicationOperation::StartSession),
            application_error: None,
            operation: None,
            error_kind: None,
        });
        telemetry.record_metric(MetricObservation {
            name: MetricName::OperationDurationSeconds,
            value: timestamp as f64,
            operation: Some(ApplicationOperation::StartSession),
        });
    }
    let pressure = telemetry.pressure();
    assert_eq!(pressure.dropped_diagnostics, 1);
    assert_eq!(pressure.dropped_metrics, 1);
    assert_eq!(
        telemetry.diagnostics()[0].timestamp,
        TimestampMillis::new(2)
    );
}

#[test]
fn restart_queue_cancellation_is_session_local_and_releases_capacity() {
    let restarts = BoundedRestartQueue::new(3).expect("restart queue");
    let first_session = session_id("session-1");
    let second_session = session_id("session-2");
    let jobs = [
        RestartJob {
            session_id: first_session.clone(),
            expected_revision: Revision::new(4),
            attempt: 1,
            not_before: TimestampMillis::new(40),
        },
        RestartJob {
            session_id: first_session.clone(),
            expected_revision: Revision::new(5),
            attempt: 2,
            not_before: TimestampMillis::new(50),
        },
        RestartJob {
            session_id: second_session.clone(),
            expected_revision: Revision::new(4),
            attempt: 1,
            not_before: TimestampMillis::new(45),
        },
    ];
    for job in &jobs {
        block_on(restarts.schedule(job)).expect("schedule");
    }

    assert_eq!(
        block_on(restarts.cancel_session_restarts(&first_session)).expect("cancel"),
        2
    );
    assert_eq!(restarts.len(), 1);
    assert_eq!(
        block_on(restarts.cancel_session_restarts(&first_session)).expect("repeat cancel"),
        0
    );
    let replacement = RestartJob {
        session_id: session_id("session-3"),
        expected_revision: Revision::new(1),
        attempt: 1,
        not_before: TimestampMillis::new(43),
    };
    block_on(restarts.schedule(&replacement)).expect("released capacity accepts work");
    assert_eq!(
        restarts
            .claim_due_restarts(TimestampMillis::new(45), 3)
            .expect("claim due restarts"),
        vec![replacement, jobs[2].clone()]
    );
    assert_eq!(
        block_on(restarts.cancel_session_restarts(&second_session)).expect("claimed job absent"),
        0
    );
    assert!(restarts.is_empty());
}

#[test]
fn observations_are_revision_aware_deadline_indexed_and_page_bounded() {
    let observations = BoundedObservationQueue::new(600).expect("observation queue");
    for index in 0..300 {
        for (kind, not_before) in [
            (ObservationJobKind::Foreground, 10),
            (ObservationJobKind::Health, 20),
        ] {
            assert_eq!(
                block_on(observations.schedule_observation(&ObservationJob {
                    session_id: session_id(&format!("session-{index:03}")),
                    expected_revision: Revision::new(1),
                    kind,
                    not_before: TimestampMillis::new(not_before),
                }))
                .expect("observation schedules"),
                ObservationScheduleOutcome::Scheduled
            );
        }
    }
    assert_eq!(observations.pressure().jobs, 600);
    assert_eq!(observations.pressure().sessions, 300);
    assert_eq!(observations.pressure().planned_sessions, 300);

    let target = session_id("session-000");
    assert_eq!(
        block_on(observations.schedule_observation(&ObservationJob {
            session_id: target.clone(),
            expected_revision: Revision::INITIAL,
            kind: ObservationJobKind::Foreground,
            not_before: TimestampMillis::new(1),
        }))
        .expect("stale observation is ignored"),
        ObservationScheduleOutcome::StaleIgnored
    );
    assert_eq!(
        block_on(observations.schedule_observation(&ObservationJob {
            session_id: target.clone(),
            expected_revision: Revision::new(1),
            kind: ObservationJobKind::Foreground,
            not_before: TimestampMillis::new(15),
        }))
        .expect("later deadline is ignored"),
        ObservationScheduleOutcome::EarlierDeadlineRetained
    );
    assert_eq!(
        block_on(observations.schedule_observation(&ObservationJob {
            session_id: target,
            expected_revision: Revision::new(1),
            kind: ObservationJobKind::Foreground,
            not_before: TimestampMillis::new(5),
        }))
        .expect("earlier deadline replaces"),
        ObservationScheduleOutcome::Replaced
    );

    let early = observations
        .claim_due_observations(TimestampMillis::new(9), 37)
        .expect("early observation claim");
    assert_eq!(early.len(), 1);
    assert_eq!(early[0].session_id.as_str(), "session-000");
    let mut foreground_jobs = 0;
    loop {
        let batch = observations
            .claim_due_observations(TimestampMillis::new(10), 37)
            .expect("bounded observation claim");
        if batch.is_empty() {
            break;
        }
        assert!(batch.len() <= 37);
        assert!(
            batch
                .iter()
                .all(|job| job.kind == ObservationJobKind::Foreground)
        );
        foreground_jobs += batch.len();
    }
    assert_eq!(foreground_jobs, 299);
    assert_eq!(observations.pressure().jobs, 300);
    assert_eq!(
        block_on(observations.cancel_session_observations(&session_id("session-299")))
            .expect("session observations cancel"),
        1
    );
    assert_eq!(observations.pressure().jobs, 299);
    assert!(matches!(
        observations.claim_due_observations(TimestampMillis::new(20), 0),
        Err(error) if error.kind() == PortErrorKind::InvalidData
    ));
}

#[test]
fn observation_plans_apply_atomically_and_reject_stale_reactivation() {
    let observations = BoundedObservationQueue::new(2).expect("observation queue");
    let target = session_id("planned-session");
    let initial = SessionObservationPlan {
        session_id: target.clone(),
        expected_revision: Revision::new(1),
        foreground_not_before: Some(TimestampMillis::new(10)),
        health_not_before: Some(TimestampMillis::new(10)),
    };
    assert_eq!(
        block_on(observations.reconcile_session_observations(&initial)).expect("initial plan"),
        ObservationReconcileOutcome {
            scheduled: 2,
            ..ObservationReconcileOutcome::default()
        }
    );

    let later_retry = SessionObservationPlan {
        foreground_not_before: Some(TimestampMillis::new(20)),
        health_not_before: Some(TimestampMillis::new(20)),
        ..initial.clone()
    };
    assert_eq!(
        block_on(observations.reconcile_session_observations(&later_retry))
            .expect("same-revision retry"),
        ObservationReconcileOutcome {
            retained: 2,
            ..ObservationReconcileOutcome::default()
        }
    );

    let replacement = SessionObservationPlan {
        session_id: target.clone(),
        expected_revision: Revision::new(2),
        foreground_not_before: None,
        health_not_before: Some(TimestampMillis::new(30)),
    };
    assert_eq!(
        block_on(observations.reconcile_session_observations(&replacement)).expect("newer plan"),
        ObservationReconcileOutcome {
            replaced: 1,
            removed: 1,
            ..ObservationReconcileOutcome::default()
        }
    );

    assert_eq!(
        block_on(observations.reconcile_session_observations(&initial)).expect("stale plan"),
        ObservationReconcileOutcome {
            stale_ignored: 2,
            ..ObservationReconcileOutcome::default()
        }
    );
    assert!(
        observations
            .claim_due_observations(TimestampMillis::new(29), 2)
            .expect("early claim")
            .is_empty()
    );
    let due = observations
        .claim_due_observations(TimestampMillis::new(30), 2)
        .expect("due claim");
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].kind, ObservationJobKind::Health);
    assert_eq!(due[0].expected_revision, Revision::new(2));

    let capacity = BoundedObservationQueue::new(1).expect("small queue");
    assert!(matches!(
        block_on(capacity.reconcile_session_observations(&initial)),
        Err(error) if error.kind() == PortErrorKind::CapacityExceeded
    ));
    assert_eq!(capacity.pressure().jobs, 0);
    assert_eq!(capacity.pressure().planned_sessions, 0);

    let health_only = SessionObservationPlan {
        session_id: target.clone(),
        expected_revision: Revision::new(1),
        foreground_not_before: None,
        health_not_before: Some(TimestampMillis::new(10)),
    };
    block_on(capacity.reconcile_session_observations(&health_only)).expect("health-only plan");
    let foreground_only = SessionObservationPlan {
        session_id: target,
        expected_revision: Revision::new(2),
        foreground_not_before: Some(TimestampMillis::new(20)),
        health_not_before: None,
    };
    block_on(capacity.reconcile_session_observations(&foreground_only))
        .expect("replacement remains within capacity");
    assert_eq!(capacity.pressure().jobs, 1);
}

#[test]
fn cooperative_foreground_wakes_pull_only_active_plans_forward() {
    let observations = BoundedObservationQueue::new(2).expect("observation queue");
    let active = session_id("active");
    let inactive = session_id("inactive");
    block_on(
        observations.reconcile_session_observations(&SessionObservationPlan {
            session_id: active.clone(),
            expected_revision: Revision::new(3),
            foreground_not_before: Some(TimestampMillis::new(50)),
            health_not_before: None,
        }),
    )
    .expect("active plan");
    block_on(
        observations.reconcile_session_observations(&SessionObservationPlan {
            session_id: inactive.clone(),
            expected_revision: Revision::new(4),
            foreground_not_before: None,
            health_not_before: None,
        }),
    )
    .expect("inactive plan");

    assert_eq!(
        observations
            .wake_foreground(&active, TimestampMillis::new(10))
            .expect("active wake"),
        Some(ObservationScheduleOutcome::Replaced)
    );
    assert_eq!(
        observations
            .wake_foreground(&inactive, TimestampMillis::new(10))
            .expect("inactive wake"),
        None
    );
    let due = observations
        .claim_due_observations(TimestampMillis::new(10), 2)
        .expect("due wake");
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].session_id, active);
    assert_eq!(due[0].expected_revision, Revision::new(3));
}

#[test]
fn clock_and_monotonic_id_source_are_explicit_adapters() {
    let source = MonotonicSessionIdSource::new("epoch-a").expect("ID source");
    assert_eq!(
        undercurrent_application::SessionIdSource::next_id(&source)
            .expect("first ID")
            .as_str(),
        "epoch-a-1"
    );
    assert!(SystemClock.now().value() > 0);
}

fn session(index: usize) -> Session {
    Session::create(
        session_id(&format!("session-{index:03}")),
        session_name(&format!("worker-{index:03}")),
        configuration(),
        8,
        TimestampMillis::new(10),
    )
    .expect("session")
    .0
}

fn configuration() -> SessionConfiguration {
    let limits = execution_limits();
    SessionConfiguration::new(
        LaunchSpec::new(
            CommandSpec::new("sh", vec!["-lc".to_owned(), "sleep 10".to_owned()], limits)
                .expect("command"),
            EnvironmentVariables::new(Vec::new(), limits).expect("environment"),
            None,
            None,
            TerminalSize::new(80, 24, 500, 200).expect("terminal size"),
        ),
        ExecutionPolicy::new(
            RestartConfig::never(),
            StopPolicy::new(StopSignal::Terminate, Duration::from_secs(5)).expect("stop policy"),
            true,
            false,
        ),
        HealthRules::new(Vec::new(), limits).expect("health rules"),
        Some(ProfileName::new("agent").expect("profile")),
        Labels::default(),
        Metadata::default(),
    )
}

fn execution_limits() -> ExecutionLimits {
    ExecutionLimits {
        max_arguments: 32,
        max_command_bytes: 8_192,
        max_environment_entries: 32,
        max_environment_bytes: 16_384,
        max_path_bytes: 1_024,
        max_health_rules: 16,
        max_health_rule_bytes: 2_048,
    }
}

fn event(session: &Session, kind: undercurrent_domain::DomainEventKind) -> SessionOutboxEvent {
    SessionOutboxEvent {
        kind,
        session_id: session.id().clone(),
        revision: session.revision(),
        occurred_at: session.updated_at(),
    }
}

fn session_id(value: &str) -> SessionId {
    SessionId::new(value).expect("session ID")
}

fn session_name(value: &str) -> SessionName {
    SessionName::new(value).expect("session name")
}

fn wait_for_mutation_waiters(coordinator: &KeyedSessionMutationCoordinator, expected: usize) {
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while coordinator.pressure().waiting_callers != expected {
        assert!(
            std::time::Instant::now() < deadline,
            "waiters did not enter coordinator"
        );
        thread::yield_now();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::yield_now(),
        }
    }
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

struct FixedClock(TimestampMillis);

impl Clock for FixedClock {
    fn now(&self) -> TimestampMillis {
        self.0
    }
}

struct SignalingTelemetry {
    recorder: BoundedTelemetryRecorder,
    metric_tx: mpsc::Sender<MetricObservation>,
}

impl Telemetry for SignalingTelemetry {
    fn emit_diagnostic(&self, record: &DiagnosticRecord) {
        self.recorder.emit_diagnostic(record);
    }

    fn record_metric(&self, observation: MetricObservation) {
        self.recorder.record_metric(observation);
        let _ = self.metric_tx.send(observation);
    }

    fn record_session_metric(
        &self,
        observation: &undercurrent_application::SessionMetricObservation,
    ) {
        self.recorder.record_session_metric(observation);
        let _ = self.metric_tx.send(observation.metric());
    }
}
