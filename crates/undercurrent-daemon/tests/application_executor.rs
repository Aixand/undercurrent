// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use undercurrent_application::{
    AccessAction, AccessDecision, AccessResource, AuthorizationPort, ExecuteInSessionPortError,
    ExecuteInSessionResult, ExecutionContextResolver, InsertSessionOutcome,
    ObservedOperationAttemptRepair, OperationAttemptAcceptance, OperationAttemptAdmission,
    OperationAttemptReconciliation, OperationAttemptRepairCommit,
    OperationAttemptRepairEvidenceRef, OperationAttemptRepairObserver,
    OperationAttemptRepairOutcome, OperationAttemptRepairRepository, OperationAttemptScan,
    OperationAttemptSettlement, OperationAttemptSettlementOutcome, OperationAttemptStore,
    OutboxSequence, PortError, PortErrorKind, PortFuture, PortOperation, ProcessInstance,
    ProcessRuntime, ProcessTermination, ProfilePatternEngine, ProfilePatternError,
    ProfileServiceLimits, ResolvedExecutionContext, RuntimeAttemptSessionStatus,
    RuntimeAttemptStatusActor, RuntimeAttemptStatusAuthorization, RuntimeAttemptStatusAuthorizer,
    RuntimeAttemptStatusBinding, RuntimeAttemptStatusLookup, RuntimeAttemptStatusRepository,
    RuntimeAttemptStatusResult, RuntimeAttemptStatusSnapshot, SessionCommandExecution,
    SessionCommandExecutor, SessionEventPublisher, SessionOutboxEvent, SessionOutboxRecord,
    SessionProjectionWriter, SessionRepository, TerminalOutputLimits, TerminalOutputStore,
    UnresolvedOperationAttempts,
};
use undercurrent_daemon::{
    ApplicationProductExecutor, BoundedDaemonOperations, BoundedProfileCursorStore,
    BoundedScreenCheckpointStore, BoundedSessionCursorStore, CursorStoreLimits,
    DaemonOperationalPressure, DaemonOperationsError, DaemonOperationsLimits, DaemonOperationsPort,
    DaemonShutdownSignal, DaemonStatusSource, OutboxDispatchWake, ProductActionExecutor,
    ProductApplicationPorts, ProductBoundary, ProductBoundaryLimits, ProductCancellation,
    ProductCursorStores, ProductEventBoundaryLimits, ProductEventPorts, ProductExecutionPorts,
    ProductProfileBoundaryLimits, ProductReadPorts, ProductRequestContext, ProductStreamContext,
    ProductStreamPolicy, ProductStreamPublisher, ProductStreamPump, ProductStreamPumpLimits,
    ProductTerminalPorts, ScreenCheckpointLimits,
};
use undercurrent_domain::{
    BinaryPattern, CommandSpec, DetectionArchetype, DetectionProfile, DetectionProfileLimits,
    DispatchCellIdentity, EffectCertainty, EnvironmentVariables, ExecutionLimits, ExecutionPolicy,
    FencingToken, HealthRules, InputSequence, Labels, LaunchSpec, LifecycleState, Metadata,
    OperationAttempt, OperationAttemptId, OutputCaptureMode, OutputExclusions, PayloadDigest,
    ProcessInstanceId, ProcessMutationKind, ProfileMatchRules, ProfileName, RestartConfig,
    Revision, Session, SessionConfiguration, SessionId, SessionName, StopPolicy, StopSignal,
    TerminalSize, TimestampMillis,
};
use undercurrent_multiplex::{
    ClientMessage, InboundStreamItem, ReplayJournal, ReplayLimits, SchedulerLimits, ServerMessage,
};
use undercurrent_protocol::{
    AgentEventsSpec, AuthenticatedPrincipalRef, AuthenticatedRequestContext, AuthorityRefDto,
    CommandSpecDto, CreateDiagnosticBundleRequest, DiagnosticBundleTerminalContentRequestDto,
    DiagnosticSectionDto, EnvironmentSpecDto, ExecutionPolicyDto, GetDiagnosticsRequest,
    GetExecutionContextRequest, GetProfileRequest, GetRuntimeAttemptStatusRequest,
    GetSessionDetailRequest, HealthRulesDto, LintProfilesRequest, ListProfilesRequest,
    ListSessionsRequest, MatchProfileRequest, MutationContext, ProductCodec, ProductCodecLimits,
    ProductErrorCategory, ProductOperation, ProductRequest, ProductResponse, ProductStreamItem,
    ProductStreamKind, ProductStreamSpec, ProductSuccess, ReadSessionEventsRequest,
    ReadTerminalOutputRequest, ReadTerminalScreenDiffRequest, ReadTerminalScreenRequest,
    RepairOperationAttemptRequest, ReplyAgentEventRequest, RuntimeAttemptStatusSnapshotDto,
    SessionDetailProjectionDto, SessionEventKindDto, SessionEventsSpec, SessionFilterDto,
    SessionProjectionDto, SessionSortDto, SessionWatchSpec, ShutdownDaemonRequest,
    StartSessionRequest, TerminalInitialModeDto, TerminalOutputCodec, TerminalOutputPageDto,
    TerminalOutputSpec, TerminalScreenUpdateDto, TerminalSizeDto, TerminalStreamItem,
    TestProfileRequest,
};
use undercurrent_runtime::{
    BoundedInputDeliveryStore, BoundedRestartQueue, BoundedSessionEventJournal,
    BoundedTelemetryRecorder, InMemoryDetectionProfileCatalog, InMemoryProcessDirectory,
    InMemorySessionRepository, KeyedSessionMutationCoordinator, MonotonicSessionIdSource,
    SessionMutationCoordinatorLimits, SystemClock, TerminalScreenLimits, VtTerminalScreenStore,
};
use undercurrent_transport_local::{
    ServerConnectionSession, ServerDiagnosticLimits, ServerStreamConfiguration,
};
use undercurrent_wire::core::NamespacedName;
use undercurrent_wire::{
    CURRENT_PROTOCOL_VERSION, ClientId, ConnectionHello, ConnectionId, ConnectionLimits,
    ConnectionPolicy, HeartbeatPolicy, RequestId, ServerEpoch, SpanId, StreamId, TraceContext,
    TraceId, connection_capabilities, negotiate_connection,
};
use undercurrent_wire::{DecodeStatus, WireCodec};

#[derive(Default)]
struct NoQueryProcessIo {
    calls: AtomicUsize,
    allow_spawn: bool,
    fail_spawn_unknown: bool,
}

impl NoQueryProcessIo {
    fn start_capable() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            allow_spawn: true,
            fail_spawn_unknown: false,
        }
    }

    fn failing_spawn_unknown() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            allow_spawn: false,
            fail_spawn_unknown: true,
        }
    }
}

impl ProcessRuntime for NoQueryProcessIo {
    fn prepare_launch<'a>(
        &'a self,
        launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<LaunchSpec, undercurrent_application::PortError>> {
        Box::pin(async move {
            let content_digest = launch
                .command()
                .expected_executable_digest()
                .unwrap_or_else(|| PayloadDigest::of_bytes(launch.command().program().as_bytes()));
            let command = launch
                .command()
                .clone()
                .pin_executable(
                    launch.command().program(),
                    content_digest,
                    PayloadDigest::of_slices([
                        b"test-executable-resolution\0".as_slice(),
                        launch.command().program().as_bytes(),
                    ]),
                    ExecutionLimits {
                        max_arguments: launch.command().arguments().len().max(1),
                        max_command_bytes: launch.command().total_bytes().max(1),
                        max_environment_entries: launch.environment().len().max(1),
                        max_environment_bytes: launch.environment().total_bytes().max(1),
                        max_path_bytes: launch.command().program().len().max(1),
                        max_health_rules: 1,
                        max_health_rule_bytes: 1,
                    },
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
        _session_id: &'a SessionId,
        _launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<ProcessInstance, undercurrent_application::PortError>> {
        let call = self.calls.fetch_add(1, Ordering::AcqRel);
        assert!(self.allow_spawn, "list path performed process spawn");
        Box::pin(async move {
            Ok(ProcessInstance {
                id: ProcessInstanceId::new(format!("process-{call}")).expect("process ID is valid"),
                started_at: undercurrent_application::Clock::now(&SystemClock),
            })
        })
    }

    fn spawn_admitted<'a>(
        &'a self,
        admission: &'a undercurrent_application::OperationAttemptAdmission,
        session_id: &'a SessionId,
        launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<ProcessInstance, undercurrent_application::PortError>> {
        if self.fail_spawn_unknown {
            let attempt_id = admission.attempt_id().clone();
            let session_id = session_id.clone();
            return Box::pin(async move {
                Err(PortError::new(
                    PortErrorKind::Interrupted,
                    PortOperation::SpawnProcess,
                    Some(session_id),
                    std::io::Error::other("synthetic response loss"),
                )
                .with_attempt_effect(attempt_id, EffectCertainty::Unknown))
            });
        }
        self.spawn(session_id, launch)
    }

    fn arm_exit<'a>(
        &'a self,
        _session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<(), undercurrent_application::PortError>> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        assert!(self.allow_spawn, "list path armed process exit");
        Box::pin(async { Ok(()) })
    }

    fn arm_exit_admitted<'a>(
        &'a self,
        _admission: &'a undercurrent_application::OperationAttemptAdmission,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<(), undercurrent_application::PortError>> {
        self.arm_exit(session_id, process)
    }

    fn stop<'a>(
        &'a self,
        _session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
        _policy: StopPolicy,
    ) -> PortFuture<'a, Result<ProcessTermination, undercurrent_application::PortError>> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        panic!("list path stopped process")
    }

    fn stop_admitted<'a>(
        &'a self,
        _admission: &'a undercurrent_application::OperationAttemptAdmission,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        policy: StopPolicy,
    ) -> PortFuture<'a, Result<ProcessTermination, undercurrent_application::PortError>> {
        self.stop(session_id, process, policy)
    }

    fn force_stop<'a>(
        &'a self,
        _session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<ProcessTermination, undercurrent_application::PortError>> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        panic!("list path force-stopped process")
    }

    fn force_stop_admitted<'a>(
        &'a self,
        _admission: &'a undercurrent_application::OperationAttemptAdmission,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<ProcessTermination, undercurrent_application::PortError>> {
        self.force_stop(session_id, process)
    }

    fn write_input<'a>(
        &'a self,
        _session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
        _input: &'a InputSequence,
        _policy: undercurrent_domain::InputDeliveryPolicy,
    ) -> PortFuture<'a, Result<(), undercurrent_application::PortError>> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        panic!("list path wrote process input")
    }

    fn write_input_admitted<'a>(
        &'a self,
        _admission: &'a undercurrent_application::OperationAttemptAdmission,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        input: &'a InputSequence,
        policy: undercurrent_domain::InputDeliveryPolicy,
    ) -> PortFuture<'a, Result<(), undercurrent_application::PortError>> {
        self.write_input(session_id, process, input, policy)
    }

    fn resize<'a>(
        &'a self,
        _session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
        _size: TerminalSize,
    ) -> PortFuture<'a, Result<(), undercurrent_application::PortError>> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        panic!("list path resized process terminal")
    }

    fn resize_admitted<'a>(
        &'a self,
        _admission: &'a undercurrent_application::OperationAttemptAdmission,
        session_id: &'a SessionId,
        process: &'a ProcessInstanceId,
        size: TerminalSize,
    ) -> PortFuture<'a, Result<(), undercurrent_application::PortError>> {
        self.resize(session_id, process, size)
    }
}

impl ExecutionContextResolver for NoQueryProcessIo {
    fn resolve<'a>(
        &'a self,
        _launch: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<ResolvedExecutionContext, PortError>> {
        Box::pin(async { Ok(ResolvedExecutionContext::new(BTreeMap::new(), None)) })
    }
}

struct ExecutorHarness {
    executor: ApplicationProductExecutor,
    operations: Arc<BoundedDaemonOperations>,
    projections: Arc<undercurrent_application::SessionSubscriptionHub>,
    streams: Arc<ProductStreamPump>,
    processes: Arc<NoQueryProcessIo>,
    repository: Arc<InMemorySessionRepository>,
    output: Arc<TerminalOutputStore>,
    screens: Arc<VtTerminalScreenStore>,
    screen_checkpoints: Arc<BoundedScreenCheckpointStore>,
    events: Arc<BoundedSessionEventJournal>,
    profiles: Arc<InMemoryDetectionProfileCatalog>,
    daemon_status_calls: Arc<AtomicUsize>,
    outbox_wakes: Arc<AtomicUsize>,
    attempt_reconciliation_wakes: Arc<AtomicUsize>,
    required_commits_before_outbox_wake: Arc<Mutex<Option<Arc<AtomicUsize>>>>,
}

fn executor() -> ExecutorHarness {
    executor_with_authorizer(Arc::new(PermitAll))
}

fn executor_with_authorizer(authorizer: Arc<dyn AuthorizationPort>) -> ExecutorHarness {
    executor_with_processes(authorizer, Arc::new(NoQueryProcessIo::default()))
}

fn start_executor() -> ExecutorHarness {
    executor_with_processes(
        Arc::new(PermitAll),
        Arc::new(NoQueryProcessIo::start_capable()),
    )
}

fn executor_with_processes(
    authorizer: Arc<dyn AuthorizationPort>,
    processes: Arc<NoQueryProcessIo>,
) -> ExecutorHarness {
    executor_with_processes_and_codec(authorizer, processes, finite_product_codec())
}

fn executor_with_processes_and_codec(
    authorizer: Arc<dyn AuthorizationPort>,
    processes: Arc<NoQueryProcessIo>,
    response_codec: ProductCodec,
) -> ExecutorHarness {
    let daemon_status_calls = Arc::new(AtomicUsize::new(0));
    let status: Arc<dyn DaemonStatusSource> =
        Arc::new(HarnessDaemonStatus(daemon_status_calls.clone()));
    executor_with_processes_codec_and_status(
        authorizer,
        processes,
        response_codec,
        status,
        daemon_status_calls,
        None,
        None,
    )
}

fn executor_with_processes_codec_and_status(
    authorizer: Arc<dyn AuthorizationPort>,
    processes: Arc<NoQueryProcessIo>,
    response_codec: ProductCodec,
    daemon_status: Arc<dyn DaemonStatusSource>,
    daemon_status_calls: Arc<AtomicUsize>,
    process_host_shutdown: Option<(
        Arc<dyn undercurrent_application::OperationAttemptStore>,
        Arc<dyn undercurrent_application::ProcessHostShutdown>,
    )>,
    execution: Option<ProductExecutionPorts>,
) -> ExecutorHarness {
    let projections = Arc::new(
        undercurrent_application::SessionSubscriptionHub::new(
            undercurrent_application::PreparedReadModels::new(),
            1_024,
        )
        .expect("projection hub"),
    );
    for index in 0..300 {
        projections
            .upsert_session(&session(index))
            .expect("prepared session");
    }
    let clock = Arc::new(SystemClock);
    let telemetry = Arc::new(BoundedTelemetryRecorder::new(2_048, 2_048).expect("telemetry"));
    let repository = Arc::new(InMemorySessionRepository::new(512, 2_048).expect("repository"));
    let output = Arc::new(
        TerminalOutputStore::new(TerminalOutputLimits {
            per_session_bytes: 64 * 1_024,
            global_bytes: 1024 * 1024,
            chunk_bytes: 4 * 1_024,
            append_bytes: 64 * 1_024,
            replay_bytes: 64 * 1_024,
            sessions: 512,
            subscriptions: 1_024,
        })
        .expect("output store"),
    );
    let events = Arc::new(BoundedSessionEventJournal::new(2_048, 1_024).expect("event journal"));
    let screens = Arc::new(
        VtTerminalScreenStore::new(TerminalScreenLimits {
            scrollback_lines: 0,
            sessions: 512,
            snapshot_bytes: 64 * 1_024,
        })
        .expect("screen store"),
    );
    let profiles = Arc::new(InMemoryDetectionProfileCatalog::new(512).expect("profile catalog"));
    let reads = ProductReadPorts::new(
        authorizer,
        ProductEventPorts::new(events.clone(), events.clone()),
        ProductTerminalPorts::new(output.clone(), screens.clone(), 8 * 1_024)
            .expect("terminal ports"),
        profiles.clone(),
        Arc::new(TestPatterns),
        ProfileServiceLimits {
            max_profiles: 512,
            max_test_samples: 8,
        },
    )
    .expect("read ports");
    let ports = ProductApplicationPorts::new(
        clock.clone(),
        Arc::new(MonotonicSessionIdSource::new("session").expect("ID source")),
        Arc::new(KeyedSessionMutationCoordinator::new(
            SessionMutationCoordinatorLimits::new(512, 8, 100).expect("coordinator limits"),
        )),
        repository.clone(),
        processes.clone(),
        processes.clone(),
        Arc::new(InMemoryProcessDirectory::default()),
        Arc::new(BoundedRestartQueue::new(512).expect("restart queue")),
        Arc::new(BoundedInputDeliveryStore::new(1_024).expect("delivery store")),
        projections.clone(),
        telemetry,
        reads,
    );
    let ports = match execution {
        Some(execution) => ports.with_execution(execution),
        None => ports,
    };
    let boundary = ProductBoundary::new(ProductBoundaryLimits {
        execution: execution_limits(),
        max_terminal_columns: 400,
        max_terminal_rows: 200,
        max_input_items: 32,
        max_input_bytes: 64 * 1_024,
        max_query_page_size: 100,
        max_work_items: 64,
        events: ProductEventBoundaryLimits {
            max_filter_sessions: 64,
            max_filter_kinds: 16,
            max_page_size: 100,
        },
        profiles: ProductProfileBoundaryLimits {
            max_page_size: 100,
            max_environment_name_bytes: 128,
            max_sample_bytes: 64 * 1_024,
        },
    })
    .expect("boundary");
    let streams = Arc::new(
        ProductStreamPump::new(ProductStreamPumpLimits::new(1_024).unwrap()).expect("stream pump"),
    );
    let screen_checkpoints = Arc::new(BoundedScreenCheckpointStore::new(
        ScreenCheckpointLimits::new(512, 8, 512 * 1_024).expect("screen checkpoint limits"),
    ));
    let mut operations = BoundedDaemonOperations::new(
        ServerEpoch::new("application-test-epoch").expect("epoch"),
        0,
        clock,
        daemon_status,
        Vec::new(),
        Arc::new(DaemonShutdownSignal::default()),
        DaemonOperationsLimits::new(512, 8, 100, 1_024, Duration::from_secs(60))
            .expect("daemon operation limits"),
    );
    if let Some((attempts, host)) = process_host_shutdown {
        operations = operations.with_process_host_shutdown(attempts, host);
    }
    let operations = Arc::new(operations);
    let outbox_wakes = Arc::new(AtomicUsize::new(0));
    let attempt_reconciliation_wakes = Arc::new(AtomicUsize::new(0));
    let required_commits_before_outbox_wake = Arc::new(Mutex::new(None));
    ExecutorHarness {
        executor: ApplicationProductExecutor::new(
            boundary,
            response_codec,
            ports,
            ProductCursorStores::new(
                Arc::new(BoundedSessionCursorStore::new(
                    CursorStoreLimits::new(512, 8).expect("session cursor limits"),
                )),
                Arc::new(BoundedProfileCursorStore::new(
                    CursorStoreLimits::new(512, 8).expect("profile cursor limits"),
                )),
                screen_checkpoints.clone(),
            ),
            streams.clone(),
            operations.clone(),
            Arc::new(CountingOutboxWake {
                outbox: outbox_wakes.clone(),
                attempt_reconciliation: attempt_reconciliation_wakes.clone(),
                required_commits_before_outbox_wake: required_commits_before_outbox_wake.clone(),
            }),
        ),
        operations,
        projections,
        streams,
        processes,
        repository,
        output,
        screens,
        screen_checkpoints,
        events,
        profiles,
        daemon_status_calls,
        outbox_wakes,
        attempt_reconciliation_wakes,
        required_commits_before_outbox_wake,
    }
}

struct CountingOutboxWake {
    outbox: Arc<AtomicUsize>,
    attempt_reconciliation: Arc<AtomicUsize>,
    required_commits_before_outbox_wake: Arc<Mutex<Option<Arc<AtomicUsize>>>>,
}

struct SuccessfulShutdownHost;

impl undercurrent_application::ProcessHostShutdown for SuccessfulShutdownHost {
    fn shutdown_attempted<'a>(
        &'a self,
        _context: &'a undercurrent_application::ProcessMutationContext,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async { Ok(()) })
    }

    fn acknowledge_shutdown<'a>(
        &'a self,
        _attempt: &'a undercurrent_domain::OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async { Ok(()) })
    }
}

impl OutboxDispatchWake for CountingOutboxWake {
    fn wake_outbox_dispatch(&self) {
        let required_commits = self
            .required_commits_before_outbox_wake
            .lock()
            .expect("repair wake precondition lock")
            .clone();
        if let Some(commits) = required_commits {
            assert_eq!(
                commits.load(Ordering::Acquire),
                1,
                "outbox wake must follow the canonical repair commit"
            );
        }
        self.outbox.fetch_add(1, Ordering::AcqRel);
    }

    fn wake_attempt_reconciliation(&self) {
        self.attempt_reconciliation.fetch_add(1, Ordering::AcqRel);
    }
}

struct UnusedAttemptStore;

impl OperationAttemptStore for UnusedAttemptStore {
    fn accept_next<'a>(
        &'a self,
        _admission: &'a OperationAttemptAdmission,
    ) -> PortFuture<'a, Result<OperationAttemptAcceptance, PortError>> {
        Box::pin(async { Ok(OperationAttemptAcceptance::PayloadConflict) })
    }

    fn accept<'a>(
        &'a self,
        _attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<OperationAttemptAcceptance, PortError>> {
        Box::pin(async { Ok(OperationAttemptAcceptance::PayloadConflict) })
    }

    fn settle<'a>(
        &'a self,
        _settlement: &'a OperationAttemptSettlement,
    ) -> PortFuture<'a, Result<OperationAttemptSettlementOutcome, PortError>> {
        Box::pin(async { Ok(OperationAttemptSettlementOutcome::NotFound) })
    }

    fn reconcile<'a>(
        &'a self,
        _reconciliation: &'a OperationAttemptReconciliation,
    ) -> PortFuture<'a, Result<OperationAttemptSettlementOutcome, PortError>> {
        Box::pin(async { Ok(OperationAttemptSettlementOutcome::NotFound) })
    }

    fn load<'a>(
        &'a self,
        _attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<Option<OperationAttempt>, PortError>> {
        Box::pin(async { Ok(None) })
    }

    fn pending_reconciliation<'a>(
        &'a self,
        _scan: &'a OperationAttemptScan,
    ) -> PortFuture<'a, Result<Vec<OperationAttempt>, PortError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

struct UnusedSessionCommandExecutor;

struct UnusedRuntimeAttemptStatusRepository;

impl RuntimeAttemptStatusRepository for UnusedRuntimeAttemptStatusRepository {
    fn get_runtime_attempt_status<'a>(
        &'a self,
        _attempt_id: &'a OperationAttemptId,
        _request_digest: PayloadDigest,
    ) -> PortFuture<'a, Result<RuntimeAttemptStatusLookup, PortError>> {
        Box::pin(async { Ok(RuntimeAttemptStatusLookup::NotFound) })
    }
}

struct RecordingRuntimeAttemptStatusRepository {
    calls: Arc<AtomicUsize>,
    lookup: RuntimeAttemptStatusLookup,
}

impl RuntimeAttemptStatusRepository for RecordingRuntimeAttemptStatusRepository {
    fn get_runtime_attempt_status<'a>(
        &'a self,
        _attempt_id: &'a OperationAttemptId,
        _request_digest: PayloadDigest,
    ) -> PortFuture<'a, Result<RuntimeAttemptStatusLookup, PortError>> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        let lookup = self.lookup.clone();
        Box::pin(async move { Ok(lookup) })
    }
}

struct FixedRuntimeAttemptStatusAuthorizer {
    calls: Arc<AtomicUsize>,
    decision: RuntimeAttemptStatusAuthorization,
}

impl RuntimeAttemptStatusAuthorizer for FixedRuntimeAttemptStatusAuthorizer {
    fn authorize_status<'a>(
        &'a self,
        _actor: &'a RuntimeAttemptStatusActor,
        _attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<RuntimeAttemptStatusAuthorization, PortError>> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        let decision = self.decision;
        Box::pin(async move { Ok(decision) })
    }
}

struct UnusedRepairRepository;

impl OperationAttemptRepairRepository for UnusedRepairRepository {
    fn load_repair_candidate<'a>(
        &'a self,
        _attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<Option<OperationAttempt>, PortError>> {
        Box::pin(async { Ok(None) })
    }

    fn repair_unknown<'a>(
        &'a self,
        _commit: &'a OperationAttemptRepairCommit,
    ) -> PortFuture<'a, Result<OperationAttemptRepairOutcome, PortError>> {
        Box::pin(async { Ok(OperationAttemptRepairOutcome::NotFound) })
    }

    fn unresolved_attempts<'a>(
        &'a self,
        _scan: &'a OperationAttemptScan,
    ) -> PortFuture<'a, Result<UnresolvedOperationAttempts, PortError>> {
        Box::pin(async {
            Ok(UnresolvedOperationAttempts::new(0, Vec::new()).expect("empty readiness"))
        })
    }
}

impl SessionCommandExecutor for UnusedSessionCommandExecutor {
    fn execute<'a>(
        &'a self,
        _execution: SessionCommandExecution<'a>,
    ) -> PortFuture<'a, Result<ExecuteInSessionResult, ExecuteInSessionPortError>> {
        Box::pin(async {
            Err(ExecuteInSessionPortError::new(
                PortError::new(
                    PortErrorKind::Other,
                    PortOperation::ExecuteInSession,
                    None,
                    std::io::Error::other("unused repair-test execution port"),
                ),
                EffectCertainty::KnownAbsent,
                None,
            ))
        })
    }

    fn acknowledge<'a>(
        &'a self,
        _attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async { Ok(()) })
    }
}

struct SuccessfulRepairRepository {
    attempt: OperationAttempt,
    commits: Arc<AtomicUsize>,
}

impl OperationAttemptRepairRepository for SuccessfulRepairRepository {
    fn load_repair_candidate<'a>(
        &'a self,
        _attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<Option<OperationAttempt>, PortError>> {
        let attempt = self.attempt.clone();
        Box::pin(async move { Ok(Some(attempt)) })
    }

    fn repair_unknown<'a>(
        &'a self,
        _commit: &'a OperationAttemptRepairCommit,
    ) -> PortFuture<'a, Result<OperationAttemptRepairOutcome, PortError>> {
        self.commits.fetch_add(1, Ordering::AcqRel);
        Box::pin(async { Ok(OperationAttemptRepairOutcome::Repaired) })
    }

    fn unresolved_attempts<'a>(
        &'a self,
        _scan: &'a OperationAttemptScan,
    ) -> PortFuture<'a, Result<UnresolvedOperationAttempts, PortError>> {
        Box::pin(async {
            Ok(UnresolvedOperationAttempts::new(0, Vec::new()).expect("empty repair readiness"))
        })
    }
}

struct KnownAbsentRepairObserver;

impl OperationAttemptRepairObserver for KnownAbsentRepairObserver {
    fn observe_repair<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<ObservedOperationAttemptRepair, PortError>> {
        let attempt_id = attempt.id().clone();
        let fencing_token = attempt.fencing_token();
        Box::pin(async move {
            Ok(ObservedOperationAttemptRepair::new(
                attempt_id,
                fencing_token,
                EffectCertainty::KnownAbsent,
                None,
                OperationAttemptRepairEvidenceRef::new(
                    "uchs:repair-evidence/application-executor".to_owned(),
                )
                .expect("repair evidence reference"),
            )
            .expect("known-absent repair observation"))
        })
    }

    fn acknowledge_repair<'a>(
        &'a self,
        _attempt: &'a OperationAttempt,
        _evidence_reference: &'a OperationAttemptRepairEvidenceRef,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async { Ok(()) })
    }
}

fn repair_executor(attempt_id: &str) -> (ExecutorHarness, Arc<AtomicUsize>) {
    let attempt_id = OperationAttemptId::new(attempt_id).expect("repair attempt ID");
    let mut attempt = OperationAttempt::accepted(
        attempt_id,
        Some(SessionId::new("repair-session").expect("repair session ID")),
        ProcessMutationKind::Resize,
        FencingToken::new(1).expect("repair fence"),
        PayloadDigest::of_bytes(b"repair-test-payload"),
        TimestampMillis::new(1),
    );
    attempt
        .settle(
            attempt.fencing_token(),
            EffectCertainty::Unknown,
            None,
            TimestampMillis::new(2),
        )
        .expect("Unknown repair candidate");
    let commits = Arc::new(AtomicUsize::new(0));
    let repository = Arc::new(SuccessfulRepairRepository {
        attempt,
        commits: commits.clone(),
    });
    let readiness = Arc::new(undercurrent_daemon::OperationAttemptReadinessGate::new());
    let execution = ProductExecutionPorts::new(
        Arc::new(UnusedAttemptStore),
        Arc::new(UnusedSessionCommandExecutor),
        repository,
        Arc::new(KnownAbsentRepairObserver),
        Arc::new(undercurrent_daemon::AuthenticatedTransportAuthorization::new()),
        Arc::new(UnusedRuntimeAttemptStatusRepository),
        Arc::new(undercurrent_daemon::AuthenticatedTransportAuthorization::new()),
        readiness,
        32,
        1_024,
    )
    .expect("repair execution ports");
    let daemon_status_calls = Arc::new(AtomicUsize::new(0));
    let status: Arc<dyn DaemonStatusSource> =
        Arc::new(HarnessDaemonStatus(daemon_status_calls.clone()));
    let harness = executor_with_processes_codec_and_status(
        Arc::new(PermitAll),
        Arc::new(NoQueryProcessIo::default()),
        finite_product_codec(),
        status,
        daemon_status_calls,
        None,
        Some(execution),
    );
    *harness
        .required_commits_before_outbox_wake
        .lock()
        .expect("repair wake precondition lock") = Some(commits.clone());
    (harness, commits)
}

fn runtime_attempt_status_executor(
    decision: RuntimeAttemptStatusAuthorization,
) -> (ExecutorHarness, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let repository_calls = Arc::new(AtomicUsize::new(0));
    let authorization_calls = Arc::new(AtomicUsize::new(0));
    let repository = Arc::new(RecordingRuntimeAttemptStatusRepository {
        calls: repository_calls.clone(),
        lookup: RuntimeAttemptStatusLookup::Found(Box::new(RuntimeAttemptStatusResult::new(
            OperationAttemptId::new("status-attempt").expect("attempt ID"),
            FencingToken::new(7).expect("fencing token"),
            RuntimeAttemptStatusBinding::new(
                DispatchCellIdentity::new("cell:workflow").expect("cell identity"),
                PayloadDigest::of_bytes(b"status caller request"),
                PayloadDigest::of_bytes(b"status launch profile"),
                PayloadDigest::of_bytes(b"status environment binding"),
                PayloadDigest::of_bytes(b"status input"),
            ),
            RuntimeAttemptStatusSnapshot::KnownApplied {
                result_digest: PayloadDigest::of_bytes(b"status start result"),
                observed_at: TimestampMillis::new(12),
                session: RuntimeAttemptSessionStatus::new(
                    SessionId::new("status-session").expect("session ID"),
                    LifecycleState::Running,
                    Revision::new(5),
                    Revision::new(4),
                    TimestampMillis::new(13),
                ),
            },
        ))),
    });
    let authorizer = Arc::new(FixedRuntimeAttemptStatusAuthorizer {
        calls: authorization_calls.clone(),
        decision,
    });
    let execution = ProductExecutionPorts::new(
        Arc::new(UnusedAttemptStore),
        Arc::new(UnusedSessionCommandExecutor),
        Arc::new(UnusedRepairRepository),
        Arc::new(KnownAbsentRepairObserver),
        Arc::new(undercurrent_daemon::AuthenticatedTransportAuthorization::new()),
        repository,
        authorizer,
        Arc::new(undercurrent_daemon::OperationAttemptReadinessGate::new()),
        32,
        1_024,
    )
    .expect("runtime-attempt status execution ports");
    let daemon_status_calls = Arc::new(AtomicUsize::new(0));
    let status: Arc<dyn DaemonStatusSource> =
        Arc::new(HarnessDaemonStatus(daemon_status_calls.clone()));
    let harness = executor_with_processes_codec_and_status(
        Arc::new(PermitAll),
        Arc::new(NoQueryProcessIo::default()),
        finite_product_codec(),
        status,
        daemon_status_calls,
        None,
        Some(execution),
    );
    (harness, repository_calls, authorization_calls)
}

struct HarnessDaemonStatus(Arc<AtomicUsize>);

struct LiveLocalDaemonStatus {
    listener: std::sync::OnceLock<std::sync::Weak<undercurrent_transport_local::LocalServer>>,
    projections:
        std::sync::OnceLock<std::sync::Weak<undercurrent_application::SessionSubscriptionHub>>,
    streams: std::sync::OnceLock<std::sync::Weak<ProductStreamPump>>,
    private_values: [&'static str; 7],
}

impl LiveLocalDaemonStatus {
    const fn new(private_values: [&'static str; 7]) -> Self {
        Self {
            listener: std::sync::OnceLock::new(),
            projections: std::sync::OnceLock::new(),
            streams: std::sync::OnceLock::new(),
            private_values,
        }
    }

    fn attach(
        &self,
        listener: &Arc<undercurrent_transport_local::LocalServer>,
        projections: &Arc<undercurrent_application::SessionSubscriptionHub>,
        streams: &Arc<ProductStreamPump>,
    ) {
        assert!(
            self.listener.set(Arc::downgrade(listener)).is_ok(),
            "live listener status is attached exactly once"
        );
        assert!(
            self.projections.set(Arc::downgrade(projections)).is_ok(),
            "live projection status is attached exactly once"
        );
        assert!(
            self.streams.set(Arc::downgrade(streams)).is_ok(),
            "live stream status is attached exactly once"
        );
    }

    const fn private_values(&self) -> &[&'static str; 7] {
        &self.private_values
    }
}

impl DaemonStatusSource for LiveLocalDaemonStatus {
    fn pressure(&self) -> Result<DaemonOperationalPressure, DaemonOperationsError> {
        let listener = self
            .listener
            .get()
            .and_then(std::sync::Weak::upgrade)
            .ok_or(DaemonOperationsError::SourceUnavailable)?;
        let projections = self
            .projections
            .get()
            .and_then(std::sync::Weak::upgrade)
            .ok_or(DaemonOperationsError::SourceUnavailable)?;
        let streams = self
            .streams
            .get()
            .and_then(std::sync::Weak::upgrade)
            .ok_or(DaemonOperationsError::SourceUnavailable)?;
        let listener = listener.pressure();
        let stream_pressure = streams.pressure();
        let logical_streams = stream_pressure
            .preparing_streams
            .checked_add(stream_pressure.active_streams)
            .ok_or(DaemonOperationsError::SourceInvariant)?;
        let (_, managed_sessions) = projections
            .catalog()
            .status()
            .map_err(|_| DaemonOperationsError::SourceUnavailable)?;
        Ok(DaemonOperationalPressure {
            accepting_connections: listener.accepting,
            physical_connections: listener.connections.connections,
            logical_streams,
            managed_sessions,
        })
    }
}

struct LiveProductZeroJitter;

impl undercurrent_sdk::ClientRecoveryJitter for LiveProductZeroJitter {
    fn sample_basis_points(&self, _failed_attempt: u32, _maximum: u16) -> u16 {
        0
    }
}

fn unix_now_millis() -> i64 {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time after Unix epoch")
        .as_millis();
    i64::try_from(millis).expect("current Unix milliseconds fit i64")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn daemon_control_public_values_are_exact_bounded_and_private_free_over_real_sdk_local_lifecycle()
 {
    const PRIVATE_HOME: &str = "/private/uref-025/home";
    const PRIVATE_TEMP: &str = "/private/uref-025/temp";
    const PRIVATE_SOCKET: &str = "/private/uref-025/undercurrent.sock";
    const PRIVATE_LOCK: &str = "/private/uref-025/undercurrent.lock";
    const PRIVATE_TOKEN: &str = "uref-025-private-token";
    const PRIVATE_CREDENTIAL: &str = "uref-025-private-credential";
    const PRIVATE_ENDPOINT: &str = "wss://private.invalid/undercurrent";
    const PUBLIC_DTO_BYTES: usize = 4_096;

    let live_status = Arc::new(LiveLocalDaemonStatus::new([
        PRIVATE_HOME,
        PRIVATE_TEMP,
        PRIVATE_SOCKET,
        PRIVATE_LOCK,
        PRIVATE_TOKEN,
        PRIVATE_CREDENTIAL,
        PRIVATE_ENDPOINT,
    ]));
    let shutdown_directory = physical_tempdir().expect("shutdown attempt directory");
    let shutdown_attempts = Arc::new(
        undercurrent_store_sqlite::SqliteSessionRepository::open(
            undercurrent_store_sqlite::SqliteRepositoryConfig::new(
                shutdown_directory.path().join("shutdown-attempts.sqlite"),
                undercurrent_store_sqlite::SqliteRepositoryLimits::new(8, 32, 512 * 1_024, 32, 32)
                    .expect("shutdown attempt store limits"),
                Duration::from_secs(1),
            )
            .expect("shutdown attempt store configuration"),
        )
        .expect("shutdown attempt store"),
    );
    let harness = executor_with_processes_codec_and_status(
        Arc::new(PermitAll),
        Arc::new(NoQueryProcessIo::default()),
        finite_product_codec(),
        live_status.clone(),
        Arc::new(AtomicUsize::new(0)),
        Some((shutdown_attempts, Arc::new(SuccessfulShutdownHost))),
        None,
    );
    let dispatcher = Arc::new(
        undercurrent_daemon::BoundedProductDispatcher::new(
            finite_product_codec(),
            undercurrent_protocol::TerminalOutputCodec::new(131_072)
                .expect("terminal output codec"),
            Arc::new(harness.executor),
            Arc::new(
                undercurrent_transport_local::BoundedRequestIdempotencyStore::new(
                    undercurrent_transport_local::RequestIdempotencyLimits::new(
                        16,
                        32 * 1_048_576,
                        1_048_576,
                        1_048_576,
                    )
                    .expect("request idempotency limits"),
                ),
            ),
            Arc::new(
                undercurrent_daemon::BoundedProductDispatchTelemetry::new(32, false, false)
                    .expect("product dispatch telemetry"),
            ),
            undercurrent_daemon::ProductDispatcherLimits::new(1, 4, Duration::from_secs(30))
                .expect("product dispatcher limits"),
        )
        .expect("product dispatcher"),
    );

    let profile =
        undercurrent_sdk::StandaloneClientProfile::standard("uref-025-real-daemon-control-client")
            .expect("standard product client profile");
    let capabilities = profile
        .hello()
        .required_capabilities()
        .into_iter()
        .chain(profile.hello().optional_capabilities())
        .map(capability)
        .collect::<std::collections::BTreeSet<_>>();
    let supports_resume = capabilities.iter().any(|capability| {
        capability.as_str() == undercurrent_wire::connection_capabilities::STREAM_RESUME
    });
    let offered_limits = profile.hello().limits();
    let connection_limits = undercurrent_wire::ConnectionLimits::new(
        offered_limits.max_frame_bytes(),
        offered_limits.max_logical_streams(),
        offered_limits.max_pending_requests(),
        offered_limits.connection_queue_bytes(),
        offered_limits.stream_queue_bytes(),
        offered_limits.initial_stream_window_bytes(),
    )
    .expect("server connection limits");
    let policy = undercurrent_wire::ConnectionPolicy::new(
        undercurrent_wire::CURRENT_PROTOCOL_VERSION,
        capabilities,
        connection_limits,
        undercurrent_wire::HeartbeatPolicy::new(5_000, 15_000).expect("heartbeat policy"),
        supports_resume.then_some(60_000),
    )
    .expect("server connection policy");
    let frame_bytes = offered_limits.max_frame_bytes();
    let frame_bytes_u64 = u64::from(frame_bytes);
    let server_limits = undercurrent_transport_local::LocalServerLimits::new(
        frame_bytes,
        Duration::from_secs(2),
        Duration::from_secs(2),
        usize::try_from(frame_bytes).expect("frame bytes fit usize"),
        8,
        16,
        64,
        undercurrent_multiplex::SchedulerMessageLimits::new(64, 16, [16, 16, 16, 16])
            .expect("scheduler message limits"),
        undercurrent_multiplex::ReplayLimits::new(
            64,
            frame_bytes_u64 * 2,
            frame_bytes_u64 * 64,
            frame_bytes_u64,
        )
        .expect("replay limits"),
        undercurrent_transport_local::ServerDiagnosticLimits::new(16, 16)
            .expect("server diagnostic limits"),
        &policy,
    )
    .expect("Local server limits");
    let transport_home = physical_tempdir().expect("transport home");
    let endpoint = undercurrent_transport_local::LocalEndpoint::new(
        transport_home.path().join("run/control.sock"),
    )
    .expect("endpoint");
    let server = Arc::new(
        undercurrent_transport_local::LocalServer::bind(
            endpoint,
            ServerEpoch::new("application-test-epoch").expect("server epoch"),
            policy,
            server_limits,
            dispatcher.clone(),
        )
        .expect("live product Local server"),
    );
    live_status.attach(&server, &harness.projections, &harness.streams);
    let endpoint = server.endpoint().path().to_path_buf();
    let running = server.clone();
    let server_worker = tokio::spawn(async move { running.run().await });
    assert!(server.wait_until_running(Duration::from_secs(2)).await);

    let product_runtime = profile.product();
    let local = undercurrent_sdk::LocalClientConfig::new(
        endpoint,
        profile.hello().clone(),
        profile.local(),
    )
    .expect("live product client configuration");
    let client = tokio::task::spawn_blocking(move || {
        undercurrent_sdk::ProductClient::connect(
            Arc::new(undercurrent_sdk::LocalClientConnector::new(local)),
            product_runtime,
            Arc::new(LiveProductZeroJitter),
        )
    })
    .await
    .expect("product client connector task")
    .expect("live product client");

    let undercurrent_protocol::ProductSuccess::GetDaemonStatus(status) = client
        .request(
            undercurrent_protocol::ProductRequest::GetDaemonStatus,
            Duration::from_secs(2),
        )
        .expect("typed daemon status over Local")
    else {
        panic!("daemon status returned the wrong success variant");
    };
    let status_json = serde_json::to_value(&status).expect("serialize daemon status DTO");
    let status_object = status_json.as_object().expect("daemon status JSON object");
    assert_eq!(status_object.len(), 6);
    assert_eq!(
        status_object
            .keys()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from([
            "accepting_connections",
            "logical_streams",
            "managed_sessions",
            "physical_connections",
            "server_epoch",
            "started_at_millis",
        ])
    );
    let encoded_status = serde_json::to_vec(&status).expect("encode daemon status DTO");
    assert!(encoded_status.len() <= PUBLIC_DTO_BYTES);
    assert_eq!(status.server_epoch, "application-test-epoch");
    assert_eq!(status.started_at_millis, 0);
    assert!(status.accepting_connections);
    assert_eq!(status.physical_connections, 1);
    assert_eq!(status.logical_streams, 0);
    assert_eq!(status.managed_sessions, 300);
    let status_text = String::from_utf8(encoded_status).expect("daemon status JSON is UTF-8");
    for private in live_status.private_values() {
        assert!(
            !status_text.contains(private),
            "daemon status exposed private sentinel {private}"
        );
    }

    let mut mutation = undercurrent_protocol::MutationContext::current(
        undercurrent_protocol::ProductOperation::ShutdownDaemon,
    );
    mutation.idempotency_key = Some("uref-025-real-shutdown-once".to_owned());
    let before_now = unix_now_millis();
    let undercurrent_protocol::ProductSuccess::ShutdownDaemon(shutdown) = client
        .request(
            undercurrent_protocol::ProductRequest::ShutdownDaemon(
                undercurrent_protocol::ShutdownDaemonRequest {
                    mode: Default::default(),
                    mutation,
                    drain_timeout_millis: 1_000,
                },
            ),
            Duration::from_secs(2),
        )
        .expect("typed daemon shutdown over Local")
    else {
        panic!("daemon shutdown returned the wrong success variant");
    };
    let after_now = unix_now_millis();
    let shutdown_json = serde_json::to_value(shutdown).expect("serialize shutdown DTO");
    let shutdown_object = shutdown_json.as_object().expect("shutdown JSON object");
    assert_eq!(shutdown_object.len(), 1);
    assert_eq!(
        shutdown_object
            .keys()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from(["drain_deadline_millis"])
    );
    let encoded_shutdown = serde_json::to_vec(&shutdown).expect("encode shutdown DTO");
    assert!(encoded_shutdown.len() <= PUBLIC_DTO_BYTES);
    assert_ne!(shutdown.drain_deadline_millis, 1_000);
    assert!(
        shutdown.drain_deadline_millis >= before_now.saturating_add(1_000),
        "shutdown deadline must be an absolute timestamp at least now + timeout"
    );
    assert!(
        shutdown.drain_deadline_millis <= after_now.saturating_add(1_000),
        "shutdown deadline must be sampled no later than response completion"
    );
    let shutdown_text =
        String::from_utf8(encoded_shutdown).expect("shutdown acknowledgement JSON is UTF-8");
    for private in live_status.private_values() {
        assert!(
            !shutdown_text.contains(private),
            "shutdown acknowledgement exposed private sentinel {private}"
        );
    }

    client.close();
    server.shutdown();
    let report = server_worker
        .await
        .expect("Local server task")
        .expect("Local server report");
    assert_eq!(report.accepted, 1);
    assert_eq!(report.rejected, 0);
    assert_eq!(report.failed, 0);
    dispatcher.shutdown();
}

impl DaemonStatusSource for HarnessDaemonStatus {
    fn pressure(&self) -> Result<DaemonOperationalPressure, DaemonOperationsError> {
        self.0.fetch_add(1, Ordering::AcqRel);
        Ok(DaemonOperationalPressure {
            accepting_connections: true,
            physical_connections: 0,
            logical_streams: 0,
            managed_sessions: 300,
        })
    }
}

struct PermitAll;

impl AuthorizationPort for PermitAll {
    fn authorize<'a>(
        &'a self,
        _action: AccessAction,
        _resource: &'a AccessResource,
    ) -> PortFuture<'a, Result<AccessDecision, PortError>> {
        Box::pin(async { Ok(AccessDecision::Permit) })
    }
}

struct DenyAll;

impl AuthorizationPort for DenyAll {
    fn authorize<'a>(
        &'a self,
        _action: AccessAction,
        _resource: &'a AccessResource,
    ) -> PortFuture<'a, Result<AccessDecision, PortError>> {
        Box::pin(async {
            Ok(AccessDecision::Deny(
                undercurrent_application::AccessDenialReason::Forbidden,
            ))
        })
    }
}

struct RecordingDeny {
    actions: Mutex<Vec<AccessAction>>,
}

impl RecordingDeny {
    fn new() -> Self {
        Self {
            actions: Mutex::new(Vec::new()),
        }
    }

    fn actions(&self) -> Vec<AccessAction> {
        self.actions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl AuthorizationPort for RecordingDeny {
    fn authorize<'a>(
        &'a self,
        action: AccessAction,
        _resource: &'a AccessResource,
    ) -> PortFuture<'a, Result<AccessDecision, PortError>> {
        self.actions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(action);
        Box::pin(async {
            Ok(AccessDecision::Deny(
                undercurrent_application::AccessDenialReason::Forbidden,
            ))
        })
    }
}

struct TestPatterns;

impl ProfilePatternEngine for TestPatterns {
    fn validate(
        &self,
        _expression: &undercurrent_domain::PatternExpression,
    ) -> Result<(), ProfilePatternError> {
        Ok(())
    }

    fn is_match(
        &self,
        expression: &undercurrent_domain::PatternExpression,
        text: &str,
    ) -> Result<bool, ProfilePatternError> {
        Ok(text.contains(expression.as_str()))
    }
}

#[test]
fn three_hundred_session_list_is_page_bounded_and_performs_no_process_io() {
    let harness = executor();
    let owner = client("client-1");
    let first = harness.executor.execute(
        &context(owner.clone(), 1),
        ProductRequest::ListSessions(list_request(None)),
    );
    let ProductResponse::Success(first) = first else {
        panic!("first list failed")
    };
    let ProductSuccess::ListSessions(first) = *first else {
        panic!("wrong first result")
    };
    assert_eq!(first.items.len(), 25);
    assert_eq!(first.total, None);
    let cursor = first.next_cursor.expect("next cursor");

    let second = harness.executor.execute(
        &context(owner, 2),
        ProductRequest::ListSessions(list_request(Some(cursor.clone()))),
    );
    let ProductResponse::Success(second) = second else {
        panic!("second list failed")
    };
    let ProductSuccess::ListSessions(second) = *second else {
        panic!("wrong second result")
    };
    assert_eq!(second.items.len(), 25);
    assert_ne!(first.items[0].id, second.items[0].id);
    assert_eq!(harness.processes.calls.load(Ordering::Acquire), 0);

    let foreign = harness.executor.execute(
        &context(client("client-2"), 3),
        ProductRequest::ListSessions(list_request(Some(cursor))),
    );
    assert!(matches!(
        foreign,
        ProductResponse::Error(error) if error.category == ProductErrorCategory::CursorExpired
    ));
}

#[test]
fn session_pages_shrink_to_response_budget_without_skipping_cursor_rows() {
    let codec = ProductCodec::new(
        ProductCodecLimits::new(8 * 1_024, 16 * 1_024, 8 * 1_024, 8 * 1_024)
            .expect("product codec limits"),
    );
    let harness = executor_with_processes_and_codec(
        Arc::new(PermitAll),
        Arc::new(NoQueryProcessIo::default()),
        codec,
    );
    let owner = client("bounded-page-client");
    let mut cursor = None;
    let mut identities = BTreeSet::new();
    let mut pages = 0usize;

    loop {
        let mut request = list_request(cursor.take());
        request.include_total = true;
        let response = harness.executor.execute(
            &context(owner.clone(), pages as u64 + 1),
            ProductRequest::ListSessions(request),
        );
        codec
            .encode_response(&response)
            .expect("every returned page fits the configured response budget");
        let ProductResponse::Success(success) = response else {
            panic!("bounded list failed")
        };
        let ProductSuccess::ListSessions(page) = *success else {
            panic!("wrong bounded list result")
        };
        pages += 1;
        assert_eq!(page.total, Some(300));
        assert!(!page.items.is_empty());
        for item in page.items {
            assert!(identities.insert(item.id));
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
        assert!(pages < 300);
    }

    assert_eq!(identities.len(), 300);
    assert!(pages > 12, "the constrained codec must force smaller pages");
    assert_eq!(harness.processes.calls.load(Ordering::Acquire), 0);
}

#[test]
fn background_wakes_target_only_the_relevant_mutation_completions() {
    let successful = start_executor();
    let success = successful.executor.execute(
        &context(client("wake-success"), 1),
        ProductRequest::StartSession(start_request("wake-success", "codex", None, true)),
    );
    assert!(matches!(success, ProductResponse::Success(_)));
    assert_eq!(successful.outbox_wakes.load(Ordering::Acquire), 1);
    assert_eq!(
        successful
            .attempt_reconciliation_wakes
            .load(Ordering::Acquire),
        0
    );

    let uncertain = executor_with_processes(
        Arc::new(PermitAll),
        Arc::new(NoQueryProcessIo::failing_spawn_unknown()),
    );
    let error = uncertain.executor.execute(
        &context(client("wake-error"), 2),
        ProductRequest::StartSession(start_request("wake-error", "codex", None, true)),
    );
    assert!(matches!(
        error,
        ProductResponse::Error(error)
            if error.disposition
                == Some(undercurrent_protocol::ProductErrorDisposition::UnknownInconclusive)
    ));
    assert_eq!(uncertain.outbox_wakes.load(Ordering::Acquire), 1);
    assert_eq!(
        uncertain
            .attempt_reconciliation_wakes
            .load(Ordering::Acquire),
        1
    );

    let read = executor();
    assert!(matches!(
        read.executor.execute(
            &context(client("wake-read"), 3),
            ProductRequest::ListSessions(list_request(None)),
        ),
        ProductResponse::Success(_)
    ));
    assert_eq!(read.outbox_wakes.load(Ordering::Acquire), 0);
    assert_eq!(read.attempt_reconciliation_wakes.load(Ordering::Acquire), 0);

    let denied = executor_with_authorizer(Arc::new(DenyAll));
    let rejection = denied.executor.execute(
        &context(client("wake-denied"), 4),
        ProductRequest::StartSession(start_request("wake-denied", "codex", None, true)),
    );
    assert!(matches!(
        rejection,
        ProductResponse::Error(error)
            if error.category == ProductErrorCategory::PermissionDenied
    ));
    assert_eq!(denied.outbox_wakes.load(Ordering::Acquire), 0);
    assert_eq!(
        denied.attempt_reconciliation_wakes.load(Ordering::Acquire),
        0
    );
}

#[test]
fn repair_uses_semantic_mutation_admission_and_post_commit_outbox_wake() {
    let (successful, commits) = repair_executor("repair-admission-success");
    let request = ProductRequest::RepairOperationAttempt(
        RepairOperationAttemptRequest::new("repair-admission-success".to_owned())
            .expect("current repair request"),
    );
    assert!(request.operation().is_mutating());
    assert!(
        request.mutation().is_none(),
        "repair must stay outside process-attempt mutation metadata"
    );
    let response = successful
        .executor
        .execute(&context(client("repair-success"), 50), request);
    assert!(matches!(
        response,
        ProductResponse::Success(success)
            if matches!(*success, ProductSuccess::RepairOperationAttempt(_))
    ));
    assert_eq!(commits.load(Ordering::Acquire), 1);
    assert_eq!(successful.outbox_wakes.load(Ordering::Acquire), 1);
    assert_eq!(
        successful
            .attempt_reconciliation_wakes
            .load(Ordering::Acquire),
        0
    );

    let (invalid, invalid_commits) = repair_executor("repair-contract-rejected");
    let mut invalid_contract = serde_json::to_value(ProductRequest::RepairOperationAttempt(
        RepairOperationAttemptRequest::new("repair-contract-rejected".to_owned())
            .expect("current repair request"),
    ))
    .expect("current repair JSON");
    invalid_contract["input"]["contract"]["contract_version"] = serde_json::json!("99.0.0");
    let invalid_contract: ProductRequest =
        serde_json::from_value(invalid_contract).expect("structurally valid retired contract");
    assert!(matches!(
        invalid.executor.execute(
            &context(client("repair-invalid-contract"), 51),
            invalid_contract,
        ),
        ProductResponse::Error(error)
            if error.category == ProductErrorCategory::InvalidRequest
    ));
    assert_eq!(invalid_commits.load(Ordering::Acquire), 0);
    assert_eq!(invalid.outbox_wakes.load(Ordering::Acquire), 0);

    let (draining, draining_commits) = repair_executor("repair-draining");
    draining
        .operations
        .begin_shutdown(Duration::from_secs(1))
        .expect("begin daemon drain");
    let response = draining.executor.execute(
        &context(client("repair-draining"), 52),
        ProductRequest::RepairOperationAttempt(
            RepairOperationAttemptRequest::new("repair-draining".to_owned())
                .expect("current repair request"),
        ),
    );
    assert!(matches!(
        response,
        ProductResponse::Error(error)
            if error.category == ProductErrorCategory::FailedPrecondition
                && error.message.contains("draining")
    ));
    assert_eq!(draining_commits.load(Ordering::Acquire), 0);
    assert_eq!(draining.outbox_wakes.load(Ordering::Acquire), 0);
}

#[test]
fn runtime_attempt_status_is_authorization_first_read_only_and_strictly_mapped() {
    let (allowed, repository_calls, authorization_calls) =
        runtime_attempt_status_executor(RuntimeAttemptStatusAuthorization::Authorized);
    let request = ProductRequest::GetRuntimeAttemptStatus(
        GetRuntimeAttemptStatusRequest::attached(
            "status-attempt".to_owned(),
            PayloadDigest::of_bytes(b"status caller request").to_hex(),
        )
        .expect("attached status request"),
    );
    assert!(!request.operation().is_mutating());
    assert!(request.mutation().is_none());
    let response = allowed
        .executor
        .execute(&context(client("status-allowed"), 53), request);
    let ProductResponse::Success(success) = response else {
        panic!("status query failed: {response:?}");
    };
    let ProductSuccess::RuntimeAttemptStatus(result) = *success else {
        panic!("wrong status success variant")
    };
    result.validate().expect("mapped status result is strict");
    assert!(matches!(
        result.snapshot(),
        RuntimeAttemptStatusSnapshotDto::KnownApplied { session, .. }
            if session.session_id() == "status-session"
                && session.revision() == 5
                && session.lifecycle_revision() == 4
    ));
    assert_eq!(authorization_calls.load(Ordering::Acquire), 1);
    assert_eq!(repository_calls.load(Ordering::Acquire), 1);
    assert_eq!(allowed.processes.calls.load(Ordering::Acquire), 0);
    assert_eq!(allowed.outbox_wakes.load(Ordering::Acquire), 0);
    assert_eq!(
        allowed.attempt_reconciliation_wakes.load(Ordering::Acquire),
        0
    );

    let (denied, denied_repository_calls, denied_authorization_calls) =
        runtime_attempt_status_executor(RuntimeAttemptStatusAuthorization::Denied);
    let response = denied.executor.execute(
        &context(client("status-denied"), 54),
        ProductRequest::GetRuntimeAttemptStatus(
            GetRuntimeAttemptStatusRequest::attached(
                "status-attempt".to_owned(),
                PayloadDigest::of_bytes(b"status caller request").to_hex(),
            )
            .expect("attached status request"),
        ),
    );
    assert!(matches!(
        response,
        ProductResponse::Error(error)
            if error.category == ProductErrorCategory::PermissionDenied
    ));
    assert_eq!(denied_authorization_calls.load(Ordering::Acquire), 1);
    assert_eq!(denied_repository_calls.load(Ordering::Acquire), 0);

    let (invalid, invalid_repository_calls, invalid_authorization_calls) =
        runtime_attempt_status_executor(RuntimeAttemptStatusAuthorization::Authorized);
    let mut request = serde_json::to_value(ProductRequest::GetRuntimeAttemptStatus(
        GetRuntimeAttemptStatusRequest::attached(
            "status-attempt".to_owned(),
            PayloadDigest::of_bytes(b"status caller request").to_hex(),
        )
        .expect("attached status request"),
    ))
    .expect("status request JSON");
    request["input"]["contract"]["contract_version"] = serde_json::json!("99.0.0");
    let request: ProductRequest =
        serde_json::from_value(request).expect("structurally valid retired contract");
    assert!(matches!(
        invalid
            .executor
            .execute(&context(client("status-invalid"), 55), request),
        ProductResponse::Error(error) if error.category == ProductErrorCategory::InvalidRequest
    ));
    assert_eq!(invalid_authorization_calls.load(Ordering::Acquire), 0);
    assert_eq!(invalid_repository_calls.load(Ordering::Acquire), 0);
}

#[test]
fn cancelled_request_stops_before_query_execution() {
    let harness = executor();
    let context = context(client("client-1"), 1);
    context.cancellation.cancel();

    let response = harness
        .executor
        .execute(&context, ProductRequest::ListSessions(list_request(None)));

    assert!(matches!(
        response,
        ProductResponse::Error(error) if error.category == ProductErrorCategory::Cancelled
    ));
}

#[test]
fn one_shared_pump_serves_three_hundred_selective_watches_on_one_connection() {
    let harness = executor();
    let connection = Arc::new(stream_connection(400));
    let codec = product_codec();

    for index in 0..300 {
        let specification = ProductStreamSpec::SessionWatch(SessionWatchSpec {
            query: ListSessionsRequest {
                filter: SessionFilterDto {
                    ids: vec![format!("session-{index:03}")],
                    ..SessionFilterDto::default()
                },
                sort: SessionSortDto::default(),
                projection: SessionProjectionDto::Compact,
                page_size: 1,
                cursor: None,
                include_total: true,
            },
        });
        prepare_and_activate_stream(&harness, &connection, index, specification);
    }

    let initial = collect_stream_data(&connection, 300, Duration::from_secs(5));
    assert_eq!(initial.len(), 300);
    let mut stream_ids = BTreeSet::new();
    for (stream_id, payload) in initial {
        assert!(stream_ids.insert(stream_id));
        assert!(matches!(
            codec.decode_stream_item(&payload).expect("query item"),
            ProductStreamItem::SessionSnapshot(snapshot)
                if snapshot.items.len() == 1 && snapshot.total == Some(1)
        ));
    }
    assert_eq!(harness.streams.pressure().active_streams, 300);
    assert_eq!(connection.pressure().logical_streams, 300);

    let mut changed = session(42);
    changed
        .rename(
            SessionName::new("renamed-session-042").expect("session name"),
            TimestampMillis::new(1_000),
        )
        .expect("rename");
    harness
        .projections
        .upsert_session(&changed)
        .expect("projection update");
    let deltas = collect_stream_data(&connection, 1, Duration::from_secs(2));
    assert_eq!(deltas[0].0, stream_id(42));
    assert!(matches!(
        codec.decode_stream_item(&deltas[0].1).expect("delta item"),
        ProductStreamItem::SessionDeltaBatch(batch)
            if batch.deltas.len() == 1
                && batch.order == vec!["session-042".to_owned()]
                && batch.total == Some(1)
    ));

    harness
        .executor
        .close_stream(connection.client_id(), &stream_id(42));
    connection
        .accept(ClientMessage::CloseStream {
            stream_id: stream_id(42),
        })
        .expect("transport stream closes");
    assert_eq!(harness.streams.pressure().active_streams, 299);
    harness.executor.close_connection(connection.client_id());
    assert_eq!(harness.streams.pressure().active_streams, 0);
    connection.close();
}

#[test]
fn shared_pump_publishes_live_events_and_raw_terminal_bytes() {
    let harness = executor();
    let connection = Arc::new(stream_connection(8));
    let event_stream = ProductStreamSpec::SessionEvents(SessionEventsSpec {
        session_ids: vec!["session-000".to_owned()],
        kinds: Vec::new(),
        after_sequence: None,
    });
    prepare_and_activate_stream(&harness, &connection, 1, event_stream);
    drain_responses(&connection, 1, Duration::from_secs(1));

    let (session, change) = create_session(0);
    block_on(harness.events.publish(&SessionOutboxRecord {
        snapshot: None,
        sequence: OutboxSequence::FIRST,
        event: SessionOutboxEvent {
            kind: change.kind(),
            session_id: session.id().clone(),
            revision: change.revision(),
            occurred_at: TimestampMillis::new(1_000),
        },
    }))
    .expect("event publishes");
    let events = collect_stream_data(&connection, 1, Duration::from_secs(2));
    assert_eq!(events[0].0, stream_id(1));
    assert!(matches!(
        product_codec()
            .decode_stream_item(&events[0].1)
            .expect("event item"),
        ProductStreamItem::SessionEvent(event)
            if event.session_id == "session-000" && event.sequence == 1
    ));

    harness
        .output
        .append(session.id(), b"initial-output")
        .expect("initial output");
    let output_stream = ProductStreamSpec::TerminalOutput(TerminalOutputSpec {
        session_id: session.id().as_str().to_owned(),
        initial: TerminalInitialModeDto::Tail { max_bytes: 64 },
    });
    prepare_and_activate_stream(&harness, &connection, 2, output_stream);
    let initial = collect_stream_data_and_responses(&connection, 1, 1, Duration::from_secs(2));
    assert_eq!(initial[0].0, stream_id(2));
    assert_eq!(
        TerminalOutputCodec::new(8 * 1_024)
            .unwrap()
            .decode(&initial[0].1)
            .expect("terminal chunk")
            .bytes(),
        b"initial-output"
    );

    harness
        .output
        .append(session.id(), b"-live")
        .expect("live output");
    let live = collect_stream_data(&connection, 1, Duration::from_secs(2));
    let chunk = TerminalOutputCodec::new(8 * 1_024)
        .unwrap()
        .decode(&live[0].1)
        .expect("terminal chunk");
    assert_eq!(chunk.start_offset(), 14);
    assert_eq!(chunk.bytes(), b"-live");
    harness.executor.close_connection(connection.client_id());
    connection.close();
}

#[test]
fn terminal_output_reaches_only_the_visible_subset_of_three_hundred_sessions() {
    let harness = executor();
    let connection = Arc::new(stream_connection(8));
    let visible = [2_usize, 40, 299];
    for index in visible {
        prepare_and_activate_stream(
            &harness,
            &connection,
            index,
            ProductStreamSpec::TerminalOutput(TerminalOutputSpec {
                session_id: format!("session-{index:03}"),
                initial: TerminalInitialModeDto::None,
            }),
        );
    }
    drain_responses(&connection, visible.len(), Duration::from_secs(1));

    for index in 0..300 {
        harness
            .output
            .append(
                &SessionId::new(format!("session-{index:03}")).expect("session ID"),
                format!("output-{index:03}").as_bytes(),
            )
            .expect("session output appends");
    }

    let items = collect_stream_data(&connection, visible.len(), Duration::from_secs(2));
    let terminal_codec = TerminalOutputCodec::new(8 * 1_024).expect("terminal codec");
    let mut received = BTreeMap::new();
    for (stream_id, payload) in items {
        let chunk = terminal_codec.decode(&payload).expect("terminal chunk");
        received.insert(stream_id, chunk.bytes().to_vec());
    }
    for index in visible {
        assert_eq!(
            received.get(&stream_id(index)).map(Vec::as_slice),
            Some(format!("output-{index:03}").as_bytes())
        );
    }
    assert_eq!(received.len(), visible.len());
    assert_eq!(
        harness
            .output
            .status()
            .expect("output status")
            .subscriptions,
        3
    );

    for index in 0..300 {
        if !visible.contains(&index) {
            harness
                .output
                .append(
                    &SessionId::new(format!("session-{index:03}")).expect("session ID"),
                    b"-unrelated",
                )
                .expect("unrelated output appends");
        }
    }
    let silence_deadline = Instant::now() + Duration::from_millis(50);
    while Instant::now() < silence_deadline {
        assert!(
            connection.next_write().expect("next write").is_none(),
            "unselected terminal output must not enter the client connection"
        );
        thread::sleep(Duration::from_millis(1));
    }

    harness.executor.close_connection(connection.client_id());
    connection.close();
    assert_eq!(harness.streams.pressure().active_streams, 0);
    assert_eq!(
        harness
            .output
            .status()
            .expect("output status")
            .subscriptions,
        0
    );
}

#[test]
fn screen_read_and_screen_seed_preserve_the_exact_live_output_checkpoint() {
    let harness = executor();
    let connection = Arc::new(stream_connection(4));
    let (session, _) = create_session(0);
    harness
        .screens
        .begin_session(
            session.id().clone(),
            TerminalSize::new(80, 24, 400, 200).unwrap(),
            undercurrent_application::OutputOffset::ZERO,
        )
        .expect("screen begins");
    harness
        .screens
        .observe_output(harness.output.as_ref(), session.id(), b"prompt")
        .expect("initial screen output");

    let finite = harness.executor.execute(
        &context(connection.client_id().clone(), 90),
        ProductRequest::ReadTerminalScreen(ReadTerminalScreenRequest {
            session_id: session.id().as_str().to_owned(),
        }),
    );
    let ProductResponse::Success(finite) = finite else {
        panic!("screen read failed")
    };
    let ProductSuccess::ReadTerminalScreen(finite) = *finite else {
        panic!("unexpected screen result")
    };
    assert_eq!(finite.output_offset, 6);
    assert!(finite.contents.windows(6).any(|window| window == b"prompt"));
    let checkpoint = finite.checkpoint.clone();

    let request_id = RequestId::new("screen-open").unwrap();
    let output_stream_id = stream_id(3);
    connection
        .accept(ClientMessage::OpenStream {
            request_id: request_id.clone(),
            stream_id: output_stream_id.clone(),
            trace_context: trace_context(3),
            payload: Arc::from(b"prepared-directly".as_slice()),
        })
        .unwrap();
    let stream_context = ProductStreamContext {
        authentication: authentication(),
        connection_id: connection.agreement().connection_id().clone(),
        server_epoch: connection.agreement().server_epoch().clone(),
        client_id: connection.client_id().clone(),
        request_id: request_id.clone(),
        stream_id: output_stream_id.clone(),
        trace_context: trace_context(3),
        resume: None,
        cancellation: ProductCancellation::new(),
    };
    let specification = ProductStreamSpec::TerminalOutput(TerminalOutputSpec {
        session_id: session.id().as_str().to_owned(),
        initial: TerminalInitialModeDto::Screen,
    });
    let policy = ProductStreamPolicy::for_specification(&specification);
    assert_eq!(
        policy.configuration.recovery,
        undercurrent_multiplex::StreamRecoveryAction::RequestSnapshot
    );
    let prepared = harness
        .executor
        .prepare_stream(&stream_context, specification)
        .expect("screen stream prepares");

    harness
        .screens
        .observe_output(harness.output.as_ref(), session.id(), b"-live")
        .expect("output produced before activation is retained");
    let diff = harness.executor.execute(
        &context(connection.client_id().clone(), 91),
        ProductRequest::ReadTerminalScreenDiff(ReadTerminalScreenDiffRequest {
            session_id: session.id().as_str().to_owned(),
            checkpoint: checkpoint.clone(),
        }),
    );
    let ProductResponse::Success(diff) = diff else {
        panic!("screen diff failed")
    };
    let ProductSuccess::ReadTerminalScreenDiff(TerminalScreenUpdateDto::Diff(diff)) = *diff else {
        panic!("unexpected screen diff result")
    };
    assert_eq!(diff.from_output_offset, 6);
    assert_eq!(diff.to_output_offset, 11);
    assert!(!diff.contents.is_empty());
    assert_ne!(diff.checkpoint, checkpoint);

    let peer_response = harness.executor.execute(
        &context(client("screen-peer"), 92),
        ProductRequest::ReadTerminalScreenDiff(ReadTerminalScreenDiffRequest {
            session_id: session.id().as_str().to_owned(),
            checkpoint,
        }),
    );
    let ProductResponse::Error(peer_error) = peer_response else {
        panic!("cross-client checkpoint unexpectedly resolved")
    };
    assert_eq!(peer_error.category, ProductErrorCategory::CursorExpired);
    assert_eq!(
        peer_error.resynchronization,
        Some(undercurrent_protocol::ResynchronizationDto::RequestSnapshot)
    );
    let outcome = connection
        .activate_stream(
            &request_id,
            &output_stream_id,
            policy.configuration,
            Arc::from(b"opened".as_slice()),
        )
        .unwrap();
    prepared.activate(ProductStreamPublisher::new(
        connection.clone(),
        output_stream_id,
        outcome.last_sequence,
        product_codec(),
        TerminalOutputCodec::new(8 * 1_024).unwrap(),
    ));
    let items = collect_stream_data_and_responses(&connection, 2, 1, Duration::from_secs(2));
    let codec = TerminalOutputCodec::new(8 * 1_024).unwrap();
    let TerminalStreamItem::Screen(seed) = codec.decode_item(&items[0].1).unwrap() else {
        panic!("first terminal item was not a screen seed")
    };
    assert_eq!(seed.output_offset(), 6);
    assert!(seed.contents().windows(6).any(|window| window == b"prompt"));
    let TerminalStreamItem::Output(live) = codec.decode_item(&items[1].1).unwrap() else {
        panic!("second terminal item was not live output")
    };
    assert_eq!(live.start_offset(), 6);
    assert_eq!(live.bytes(), b"-live");

    harness.executor.close_connection(connection.client_id());
    assert_eq!(harness.screen_checkpoints.pressure().clients, 0);
    connection.close();
}

#[test]
fn opaque_capture_rejects_finite_and_initial_screen_as_failed_precondition() {
    let harness = executor();
    let connection = Arc::new(stream_connection(4));
    let (session, _) = create_session(0);
    harness
        .screens
        .begin_session_with_capture_mode(
            session.id().clone(),
            TerminalSize::new(80, 24, 400, 200).unwrap(),
            undercurrent_application::OutputOffset::ZERO,
            OutputCaptureMode::Opaque,
        )
        .expect("opaque capture begins");
    harness
        .screens
        .observe_output(harness.output.as_ref(), session.id(), b"opaque-bytes")
        .expect("opaque output is retained without parsing");

    let finite = harness.executor.execute(
        &context(connection.client_id().clone(), 190),
        ProductRequest::ReadTerminalScreen(ReadTerminalScreenRequest {
            session_id: session.id().as_str().to_owned(),
        }),
    );
    assert!(matches!(
        finite,
        ProductResponse::Error(error)
            if error.category == ProductErrorCategory::FailedPrecondition && !error.retryable
    ));

    let request_id = RequestId::new("opaque-screen-open").unwrap();
    let stream_context = ProductStreamContext {
        authentication: authentication(),
        connection_id: connection.agreement().connection_id().clone(),
        server_epoch: connection.agreement().server_epoch().clone(),
        client_id: connection.client_id().clone(),
        request_id,
        stream_id: stream_id(13),
        trace_context: trace_context(13),
        resume: None,
        cancellation: ProductCancellation::new(),
    };
    let error = match harness.executor.prepare_stream(
        &stream_context,
        ProductStreamSpec::TerminalOutput(TerminalOutputSpec {
            session_id: session.id().as_str().to_owned(),
            initial: TerminalInitialModeDto::Screen,
        }),
    ) {
        Err(error) => error,
        Ok(_) => panic!("opaque screen stream unexpectedly prepared"),
    };
    assert_eq!(error.category, ProductErrorCategory::FailedPrecondition);
    assert!(!error.retryable);
}

#[test]
fn shared_pump_retries_reliable_events_only_after_writer_drain() {
    let harness = executor();
    let connection = Arc::new(stream_connection_with_stream_queue(8, 512));
    prepare_and_activate_stream(
        &harness,
        &connection,
        1,
        ProductStreamSpec::SessionEvents(SessionEventsSpec {
            session_ids: vec!["session-000".to_owned()],
            kinds: Vec::new(),
            after_sequence: None,
        }),
    );
    drain_responses(&connection, 1, Duration::from_secs(1));

    let (session, change) = create_session(0);
    for sequence in 1..=20 {
        block_on(harness.events.publish(&SessionOutboxRecord {
            snapshot: None,
            sequence: OutboxSequence::new(sequence).expect("outbox sequence"),
            event: SessionOutboxEvent {
                kind: change.kind(),
                session_id: session.id().clone(),
                revision: change.revision(),
                occurred_at: TimestampMillis::new(sequence as i64),
            },
        }))
        .expect("event publishes");
    }
    thread::sleep(Duration::from_millis(10));

    let items = collect_stream_data(&connection, 20, Duration::from_secs(3));
    assert_eq!(
        items.len(),
        20,
        "pump pressure: {:?}; connection pressure: {:?}",
        harness.streams.pressure(),
        connection.pressure()
    );
    let sequences = items
        .iter()
        .map(|(_, payload)| {
            match product_codec()
                .decode_stream_item(payload)
                .expect("event stream item")
            {
                ProductStreamItem::SessionEvent(event) => event.sequence,
                _ => panic!("expected event stream item"),
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(sequences, (1..=20).collect::<Vec<_>>());
    assert_eq!(harness.streams.pressure().backpressured_streams, 0);
    assert_eq!(harness.streams.pressure().failed_streams, 0);
    harness.executor.close_connection(connection.client_id());
    connection.close();
}

#[test]
fn saturated_terminal_retry_preserves_one_sequence_and_exact_payload() {
    let harness = executor();
    let connection = Arc::new(stream_connection_with_stream_queue(8, 512));
    let specification = ProductStreamSpec::TerminalOutput(TerminalOutputSpec {
        session_id: "session-000".to_owned(),
        initial: TerminalInitialModeDto::Tail { max_bytes: 128 },
    });
    prepare_and_activate_stream(&harness, &connection, 7, specification);
    drain_responses(&connection, 1, Duration::from_secs(1));

    let expected = (0..20)
        .flat_map(|index| std::iter::repeat_n(index as u8, 128))
        .collect::<Vec<_>>();
    harness
        .output
        .append(
            &SessionId::new("session-000").expect("session ID"),
            &expected,
        )
        .expect("terminal output appends");

    let pressure_deadline = Instant::now() + Duration::from_secs(1);
    while harness.streams.pressure().backpressured_streams == 0
        && Instant::now() < pressure_deadline
    {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(harness.streams.pressure().backpressured_streams, 1);

    let codec = TerminalOutputCodec::new(128).expect("terminal codec");
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut sequences = Vec::new();
    let mut observed = Vec::new();
    while observed.len() < expected.len() && Instant::now() < deadline {
        if let Some(write) = connection.next_write().expect("next write") {
            let message = decode_server(write.bytes());
            write.confirm_written().expect("write confirms");
            match message {
                ServerMessage::StreamItem {
                    stream_id,
                    item:
                        InboundStreamItem::Data {
                            sequence,
                            bytes,
                            flow_controlled_bytes,
                        },
                } => {
                    connection
                        .accept(ClientMessage::WindowUpdate {
                            stream_id,
                            additional_bytes: flow_controlled_bytes,
                        })
                        .expect("stream credit replenishes");
                    let chunk = codec.decode(&bytes).expect("terminal payload decodes");
                    assert_eq!(chunk.start_offset(), observed.len() as u64);
                    sequences.push(sequence.value());
                    observed.extend_from_slice(chunk.bytes());
                }
                ServerMessage::StreamItem {
                    item: InboundStreamItem::Gap(_),
                    ..
                } => panic!("reliable retry must not create a stream gap"),
                _ => {}
            }
        } else {
            thread::sleep(Duration::from_millis(1));
        }
    }

    assert_eq!(observed, expected);
    assert_eq!(sequences, (1..=20).collect::<Vec<_>>());
    assert_eq!(harness.streams.pressure().backpressured_streams, 0);
    assert_eq!(harness.streams.pressure().failed_streams, 0);
    harness.executor.close_connection(connection.client_id());
    connection.close();
}

#[test]
fn session_revision_precondition_rejection_is_typed_and_precedes_process_io() {
    let harness = executor();
    let (session, change) = create_session(999);
    let event = SessionOutboxEvent {
        kind: change.kind(),
        session_id: session.id().clone(),
        revision: change.revision(),
        occurred_at: TimestampMillis::new(999),
    };
    assert_eq!(
        block_on(harness.repository.insert(&session, &event)).unwrap(),
        InsertSessionOutcome::Inserted
    );
    let requests = [
        ProductRequest::StopSession(undercurrent_protocol::StopSessionRequest {
            graceful_only: false,
            mutation: MutationContext::current(ProductOperation::StopSession),
            session_id: session.id().as_str().into(),
            expected_revision: Some(999),
            force: true,
        }),
        ProductRequest::SendTerminalInput(undercurrent_protocol::SendTerminalInputRequest {
            mutation: MutationContext::current(ProductOperation::SendTerminalInput),
            session_id: session.id().as_str().into(),
            expected_revision: Some(999),
            delivery_id: Some("guarded-delivery".into()),
            items: vec![undercurrent_protocol::TerminalInputDto::Text {
                text: "input".into(),
            }],
            observation_policy: Default::default(),
        }),
    ];
    for (index, request) in requests.into_iter().enumerate() {
        let operation = request.operation();
        let response = harness.executor.execute(
            &context(client("guarded"), index as u64 + 1),
            request.clone(),
        );
        let ProductResponse::Error(error) = response else {
            panic!("stale revision must be refused");
        };
        assert_eq!(error.operation, Some(operation));
        assert_eq!(
            error.category,
            ProductErrorCategory::RevisionPreconditionFailed
        );
        assert_eq!(
            error.disposition,
            Some(undercurrent_protocol::ProductErrorDisposition::SemanticRejection)
        );
        assert_eq!(error.session_id.as_deref(), Some(session.id().as_str()));
        assert!(!error.retryable);
        assert_eq!(harness.processes.calls.load(Ordering::Acquire), 0);
        assert_eq!(
            block_on(harness.repository.load(session.id()))
                .unwrap()
                .unwrap()
                .revision(),
            session.revision()
        );

        let denied = executor_with_authorizer(Arc::new(DenyAll));
        assert!(
            matches!(denied.executor.execute(&context(client("denied-guard"), index as u64 + 1), request),
            ProductResponse::Error(error) if error.category == ProductErrorCategory::PermissionDenied)
        );
    }
}

#[test]
fn finite_read_operations_present_bounded_authorized_results() {
    let harness = executor();
    let (session, change) = create_session(999);
    let event = SessionOutboxEvent {
        kind: change.kind(),
        session_id: session.id().clone(),
        revision: change.revision(),
        occurred_at: TimestampMillis::new(999),
    };
    assert_eq!(
        block_on(harness.repository.insert(&session, &event)).expect("session insert"),
        InsertSessionOutcome::Inserted
    );
    harness
        .output
        .append(session.id(), b"hello")
        .expect("output append");
    let record = SessionOutboxRecord {
        snapshot: None,
        sequence: OutboxSequence::FIRST,
        event,
    };
    block_on(harness.events.publish(&record)).expect("event publish");
    let owner = client("reader");

    let detail = harness.executor.execute(
        &context(owner.clone(), 10),
        ProductRequest::GetSessionDetail(GetSessionDetailRequest {
            session_id: session.id().as_str().to_owned(),
            projection: SessionDetailProjectionDto {
                configuration: true,
                work_context: true,
                process: true,
                foreground_evidence: true,
                health_results: true,
            },
        }),
    );
    let ProductResponse::Success(detail) = detail else {
        panic!("detail read failed")
    };
    let ProductSuccess::GetSessionDetail(detail) = *detail else {
        panic!("wrong detail result")
    };
    assert_eq!(detail.summary.id, session.id().as_str());
    assert!(detail.configuration.is_some());
    assert!(detail.work_context.is_some());

    let execution_context = harness.executor.execute(
        &context(owner.clone(), 13),
        ProductRequest::GetExecutionContext(GetExecutionContextRequest {
            session_id: session.id().as_str().to_owned(),
        }),
    );
    let ProductResponse::Success(execution_context) = execution_context else {
        panic!("execution context read failed")
    };
    let ProductSuccess::GetExecutionContext(execution_context) = *execution_context else {
        panic!("wrong execution context result")
    };
    assert!(execution_context.environment.is_empty());
    assert!(execution_context.working_directory.is_none());

    let output = harness.executor.execute(
        &context(owner.clone(), 11),
        ProductRequest::ReadTerminalOutput(ReadTerminalOutputRequest {
            session_id: session.id().as_str().to_owned(),
            from_offset: 0,
            max_bytes: 4,
        }),
    );
    let ProductResponse::Success(output) = output else {
        panic!("output read failed")
    };
    assert!(matches!(
        *output,
        ProductSuccess::ReadTerminalOutput(TerminalOutputPageDto::Data {
            from_offset: 0,
            to_offset: 4,
            next_offset: 5,
            has_more: true,
            ref bytes,
            ..
        }) if bytes == b"hell"
    ));

    let events = harness.executor.execute(
        &context(owner, 12),
        ProductRequest::ReadSessionEvents(ReadSessionEventsRequest {
            tail: false,
            include_observation: false,
            session_ids: vec![session.id().as_str().to_owned()],
            kinds: vec![SessionEventKindDto::SessionCreated],
            after_sequence: None,
            limit: 10,
        }),
    );
    let ProductResponse::Success(events) = events else {
        panic!("event read failed")
    };
    let ProductSuccess::ReadSessionEvents(events) = *events else {
        panic!("wrong event result")
    };
    assert_eq!(events.items.len(), 1);
    assert_eq!(events.items[0].session_id, session.id().as_str());
    assert_eq!(events.checkpoint, Some(OutboxSequence::FIRST.value()));
    assert_eq!(harness.processes.calls.load(Ordering::Acquire), 0);
}

#[test]
fn start_rejects_a_profile_removed_before_creation() {
    let harness = start_executor();
    let result = harness.executor.execute(
        &context(client("missing-profile"), 16),
        ProductRequest::StartSession(start_request("missing", "codex", Some("removed"), true)),
    );
    assert!(matches!(result, ProductResponse::Error(ref error)
        if error.category == undercurrent_protocol::ProductErrorCategory::FailedPrecondition
        && error.effect_certainty == Some(undercurrent_protocol::EffectCertaintyDto::KnownAbsent)));
    assert!(
        block_on(
            harness
                .repository
                .find_by_name(&SessionName::new("missing").unwrap())
        )
        .unwrap()
        .is_none()
    );
}

#[test]
fn start_selects_one_profile_and_preserves_explicit_or_safe_unselected_states() {
    let harness = start_executor();
    harness
        .profiles
        .replace(
            vec![profile("codex", "codex"), profile("opencode", "opencode")],
            Vec::new(),
            &TestPatterns,
        )
        .expect("profile activation");
    let owner = client("profile-starter");

    let automatic = harness.executor.execute(
        &context(owner.clone(), 16),
        ProductRequest::StartSession(start_request("automatic", "codex", None, true)),
    );
    let ProductResponse::Success(automatic) = automatic else {
        panic!("automatic start failed")
    };
    let ProductSuccess::StartSession(automatic) = *automatic else {
        panic!("wrong automatic start result")
    };
    assert_eq!(automatic.session.profile.as_deref(), Some("codex"));
    let resolution = automatic
        .executable_resolution
        .expect("start returns the durable executable resolution receipt");
    assert_eq!(resolution.requested_reference, "codex");
    assert_eq!(resolution.resolved_program, "codex");
    assert_eq!(resolution.content_digest_sha256.len(), 64);
    assert_eq!(resolution.receipt_digest_sha256.len(), 64);

    let explicit = harness.executor.execute(
        &context(owner.clone(), 17),
        ProductRequest::StartSession(start_request("explicit", "opencode", Some("codex"), true)),
    );
    assert!(matches!(
        explicit,
        ProductResponse::Success(result)
            if matches!(*result, ProductSuccess::StartSession(ref started)
                if started.session.profile.as_deref() == Some("codex"))
    ));

    let disabled = harness.executor.execute(
        &context(owner.clone(), 18),
        ProductRequest::StartSession(start_request("disabled", "opencode", None, false)),
    );
    assert!(matches!(
        disabled,
        ProductResponse::Success(result)
            if matches!(*result, ProductSuccess::StartSession(ref started)
                if started.session.profile.is_none())
    ));

    harness
        .profiles
        .replace(
            vec![profile("agent-a", "agent*"), profile("agent-b", "agent?")],
            Vec::new(),
            &TestPatterns,
        )
        .expect("ambiguous profiles activate");
    let ambiguous = harness.executor.execute(
        &context(owner, 19),
        ProductRequest::StartSession(start_request("ambiguous", "agent1", None, true)),
    );
    assert!(matches!(
        ambiguous,
        ProductResponse::Success(result)
            if matches!(*result, ProductSuccess::StartSession(ref started)
                if started.session.profile.is_none())
    ));
}

#[test]
fn profile_operations_share_the_active_catalog_and_client_scoped_pages() {
    let harness = executor();
    harness
        .profiles
        .replace(
            vec![
                profile("alpha", "alpha"),
                profile("beta", "beta"),
                profile("gamma", "gamma"),
            ],
            Vec::new(),
            &TestPatterns,
        )
        .expect("profile activation");
    let owner = client("profile-reader");

    let first = harness.executor.execute(
        &context(owner.clone(), 20),
        ProductRequest::ListProfiles(ListProfilesRequest {
            page_size: 2,
            cursor: None,
        }),
    );
    let ProductResponse::Success(first) = first else {
        panic!("profile list failed")
    };
    let ProductSuccess::ListProfiles(first) = *first else {
        panic!("wrong profile list result")
    };
    assert_eq!(
        first
            .items
            .iter()
            .map(|profile| profile.name.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "beta"]
    );

    let second = harness.executor.execute(
        &context(owner.clone(), 21),
        ProductRequest::ListProfiles(ListProfilesRequest {
            page_size: 2,
            cursor: first.next_cursor,
        }),
    );
    let ProductResponse::Success(second) = second else {
        panic!("profile continuation failed")
    };
    let ProductSuccess::ListProfiles(second) = *second else {
        panic!("wrong profile continuation result")
    };
    assert_eq!(second.items.len(), 1);
    assert_eq!(second.items[0].name, "gamma");
    assert!(second.next_cursor.is_none());

    let detail = harness.executor.execute(
        &context(owner.clone(), 22),
        ProductRequest::GetProfile(GetProfileRequest {
            name: "alpha".to_owned(),
        }),
    );
    assert!(matches!(
        detail,
        ProductResponse::Success(result)
            if matches!(*result, ProductSuccess::GetProfile(ref detail)
                if detail.summary.name == "alpha" && detail.binary_patterns == ["alpha"])
    ));

    let matched = harness.executor.execute(
        &context(owner.clone(), 23),
        ProductRequest::MatchProfile(MatchProfileRequest {
            executable: "/usr/bin/alpha".to_owned(),
            arguments: Vec::new(),
            environment_names: Vec::new(),
        }),
    );
    assert!(matches!(
        matched,
        ProductResponse::Success(result)
            if matches!(*result, ProductSuccess::MatchProfile(ref matched)
                if matched.selected.as_deref() == Some("alpha"))
    ));

    let linted = harness.executor.execute(
        &context(owner.clone(), 24),
        ProductRequest::LintProfiles(LintProfilesRequest { name: None }),
    );
    assert!(matches!(
        linted,
        ProductResponse::Success(result)
            if matches!(*result, ProductSuccess::LintProfiles(ref linted) if linted.valid)
    ));

    let tested = harness.executor.execute(
        &context(owner, 25),
        ProductRequest::TestProfile(TestProfileRequest {
            name: "alpha".to_owned(),
            screen_text: "ready".to_owned(),
            last_line: "ready".to_owned(),
            cursor_at_end: true,
        }),
    );
    assert!(matches!(
        tested,
        ProductResponse::Success(result)
            if matches!(*result, ProductSuccess::TestProfile(ref tested)
                if tested.profile == "alpha" && tested.evaluations.is_empty())
    ));
}

#[test]
fn denied_session_list_stops_before_query_or_process_access() {
    let harness = executor_with_authorizer(Arc::new(DenyAll));

    let response = harness.executor.execute(
        &context(client("denied"), 30),
        ProductRequest::ListSessions(list_request(None)),
    );

    assert!(matches!(
        response,
        ProductResponse::Error(error)
            if error.category == ProductErrorCategory::PermissionDenied
    ));
    assert_eq!(harness.processes.calls.load(Ordering::Acquire), 0);
}

#[test]
fn denied_screen_diff_stops_before_private_checkpoint_lookup() {
    let harness = executor_with_authorizer(Arc::new(DenyAll));

    let response = harness.executor.execute(
        &context(client("denied-screen"), 31),
        ProductRequest::ReadTerminalScreenDiff(ReadTerminalScreenDiffRequest {
            session_id: "session-denied".into(),
            checkpoint: "not-a-checkpoint".into(),
        }),
    );

    assert!(matches!(
        response,
        ProductResponse::Error(error)
            if error.category == ProductErrorCategory::PermissionDenied
    ));
}

// Finite polling and subscriptions must consult the same per-session policy
// before the channel repository can reveal whether a binding or event exists.
#[test]
fn agent_event_reads_authorize_before_repository_access() {
    use undercurrent_application::{
        AccessDenialReason, AgentChannelRepository, AgentChannelTarget,
        AgentChannelTargetResolution, AgentEventAdmission, AgentEventAdmissionOutcome,
        AgentEventPollOutcome, AgentEventReplyAdmission, AgentEventReplyOutcome,
        AgentEventReplyRecord,
    };
    use undercurrent_domain::ProcessInstanceId;

    struct Channel(AtomicUsize);
    impl AgentChannelRepository for Channel {
        fn resolve_agent_channel_target<'a>(
            &'a self,
            _: &'a SessionId,
            _: &'a ProcessInstanceId,
            _: TimestampMillis,
        ) -> PortFuture<'a, Result<AgentChannelTargetResolution, PortError>> {
            panic!("read must not resolve another target")
        }
        fn enqueue_agent_event<'a>(
            &'a self,
            _: &'a AgentEventAdmission,
        ) -> PortFuture<'a, Result<AgentEventAdmissionOutcome, PortError>> {
            panic!("read must not enqueue")
        }
        fn poll_agent_event<'a>(
            &'a self,
            _: &'a AgentChannelTarget,
            _: u64,
            _: TimestampMillis,
        ) -> PortFuture<'a, Result<AgentEventPollOutcome, PortError>> {
            self.0.fetch_add(1, Ordering::AcqRel);
            Box::pin(async { Ok(AgentEventPollOutcome::None { next_sequence: 0 }) })
        }
        fn reply_agent_event<'a>(
            &'a self,
            _: &'a AgentEventReplyAdmission,
        ) -> PortFuture<'a, Result<AgentEventReplyOutcome, PortError>> {
            panic!("read must not reply")
        }
        fn get_agent_event_reply<'a>(
            &'a self,
            _: &'a AgentChannelTarget,
            _: &'a str,
            _: TimestampMillis,
        ) -> PortFuture<'a, Result<Option<AgentEventReplyRecord>, PortError>> {
            panic!("read must not retrieve reply")
        }
    }
    struct Policy {
        decision: AccessDecision,
        calls: AtomicUsize,
    }
    impl AuthorizationPort for Policy {
        fn authorize<'a>(
            &'a self,
            action: AccessAction,
            resource: &'a AccessResource,
        ) -> PortFuture<'a, Result<AccessDecision, PortError>> {
            assert_eq!(action, AccessAction::ReadEvents);
            assert_eq!(
                resource,
                &AccessResource::Session(SessionId::new("session-agent").unwrap())
            );
            self.calls.fetch_add(1, Ordering::AcqRel);
            Box::pin(async { Ok(self.decision) })
        }
    }
    for (decision, expected_error) in [
        (AccessDecision::Permit, None),
        (
            AccessDecision::Deny(AccessDenialReason::Forbidden),
            Some(ProductErrorCategory::PermissionDenied),
        ),
        (
            AccessDecision::Deny(AccessDenialReason::Unauthenticated),
            Some(ProductErrorCategory::Unauthenticated),
        ),
    ] {
        let policy = Arc::new(Policy {
            decision,
            calls: AtomicUsize::new(0),
        });
        let channel = Arc::new(Channel(AtomicUsize::new(0)));
        let authority = Arc::new(undercurrent_daemon::AuthenticatedTransportAuthorization::new());
        let execution = ProductExecutionPorts::new(
            Arc::new(UnusedAttemptStore),
            Arc::new(UnusedSessionCommandExecutor),
            Arc::new(UnusedRepairRepository),
            Arc::new(KnownAbsentRepairObserver),
            authority.clone(),
            Arc::new(UnusedRuntimeAttemptStatusRepository),
            authority,
            Arc::new(undercurrent_daemon::OperationAttemptReadinessGate::new()),
            32,
            1_024,
        )
        .unwrap()
        .with_agent_channel(channel.clone());
        let status_calls = Arc::new(AtomicUsize::new(0));
        let harness = executor_with_processes_codec_and_status(
            policy.clone(),
            Arc::new(NoQueryProcessIo::default()),
            finite_product_codec(),
            Arc::new(HarnessDaemonStatus(status_calls.clone())),
            status_calls,
            None,
            Some(execution),
        );
        let request = undercurrent_protocol::GetAgentEventRequest {
            tenant_id: "tenant-test".into(),
            source_product: "example/workflow".into(),
            actor_context_ref: "actor:test".into(),
            authority_ref: "authority:test".into(),
            undercurrent_provider_id: "provider-test".into(),
            undercurrent_provider_generation: 1,
            session_id: "session-agent".into(),
            attempt_id: "attempt:test".into(),
            attempt_revision: 1,
            recovery_epoch: 1,
            fence: 1,
            request_digest_sha256: "a".repeat(64),
            after_sequence: 0,
            maximum_wait_millis: 0,
        };
        let response = harness.executor.execute(
            &context(client("agent-reader"), 1),
            ProductRequest::GetAgentEvent(request.clone()),
        );
        match expected_error {
            Some(category) => assert!(
                matches!(response, ProductResponse::Error(error) if error.category == category)
            ),
            None => assert!(matches!(response, ProductResponse::Success(_))),
        }
        assert_eq!(policy.calls.load(Ordering::Acquire), 1);
        let specification = ProductStreamSpec::AgentEvents(AgentEventsSpec {
            tenant_id: request.tenant_id,
            source_product: request.source_product,
            actor_context_ref: request.actor_context_ref,
            authority_ref: request.authority_ref,
            undercurrent_provider_id: request.undercurrent_provider_id,
            undercurrent_provider_generation: request.undercurrent_provider_generation,
            session_id: request.session_id,
            attempt_id: request.attempt_id,
            attempt_revision: request.attempt_revision,
            recovery_epoch: request.recovery_epoch,
            fence: request.fence,
            request_digest_sha256: request.request_digest_sha256,
            after_sequence: 0,
        });
        let context = ProductStreamContext {
            authentication: authentication(),
            connection_id: ConnectionId::new("agent-read-connection").unwrap(),
            server_epoch: ServerEpoch::new("agent-read-epoch").unwrap(),
            client_id: client("agent-reader"),
            request_id: RequestId::new("agent-read-request").unwrap(),
            stream_id: StreamId::new("agent-read-stream").unwrap(),
            trace_context: trace_context(0),
            resume: None,
            cancellation: ProductCancellation::new(),
        };
        let prepared = harness.executor.prepare_stream(&context, specification);
        match expected_error {
            Some(category) => match prepared {
                Err(error) => assert_eq!(error.category, category),
                Ok(_) => panic!("denied stream must not register a source"),
            },
            None => assert!(prepared.is_ok()),
        }
        assert_eq!(policy.calls.load(Ordering::Acquire), 2);
        assert_eq!(
            channel.0.load(Ordering::Acquire),
            if expected_error.is_none() { 2 } else { 0 }
        );
        harness.executor.close_connection(&context.client_id);
    }
}

#[test]
fn denied_agent_event_reply_stops_before_agent_repository_access() {
    let authorizer = Arc::new(RecordingDeny::new());
    let harness = executor_with_authorizer(authorizer.clone());
    let response = harness.executor.execute(
        &context(client("denied-agent-reply"), 32),
        ProductRequest::ReplyAgentEvent(ReplyAgentEventRequest {
            mutation: MutationContext::default(),
            tenant_id: "tenant-1".to_owned(),
            source_product: "example/workflow".to_owned(),
            actor_context_ref: "actor://example/test".to_owned(),
            authority_ref: "authority://operation/agent/reply".to_owned(),
            undercurrent_provider_id: "undercurrent-provider-1".to_owned(),
            undercurrent_provider_generation: 1,
            session_id: "session-agent-1".to_owned(),
            event_id: "agent-event-1".to_owned(),
            sequence: 1,
            process_instance_id: "process-agent-1".to_owned(),
            attempt_id: "attempt-agent-1".to_owned(),
            attempt_revision: 1,
            recovery_epoch: 1,
            fence: 1,
            request_digest_sha256: PayloadDigest::of_bytes(b"agent request").to_hex(),
            call_digest_sha256: PayloadDigest::of_bytes(b"agent call").to_hex(),
            outcome: undercurrent_protocol::AgentEventReplyOutcomeDto::Accepted {
                content: serde_json::json!({"accepted": true}),
            },
        }),
    );

    assert!(matches!(
        response,
        ProductResponse::Error(error)
            if error.category == ProductErrorCategory::PermissionDenied
    ));
    assert_eq!(authorizer.actions(), [AccessAction::ReplyAgentEvent]);
}

#[test]
fn mutation_and_subscription_authorization_registries_are_complete() {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum MutationAuthorizationBoundary {
        Application(AccessAction),
        RetainedAttemptEvidence,
        RuntimeCancellationControl,
    }

    let mutation_authorization = BTreeMap::from([
        (
            ProductOperation::StartSession,
            MutationAuthorizationBoundary::Application(AccessAction::StartSession),
        ),
        (
            ProductOperation::StopSession,
            MutationAuthorizationBoundary::Application(AccessAction::StopSession),
        ),
        (
            ProductOperation::RestartSession,
            MutationAuthorizationBoundary::Application(AccessAction::RestartSession),
        ),
        (
            ProductOperation::RenameSession,
            MutationAuthorizationBoundary::Application(AccessAction::RenameSession),
        ),
        (
            ProductOperation::UpdateSessionLabels,
            MutationAuthorizationBoundary::Application(AccessAction::ManageConfiguration),
        ),
        (
            ProductOperation::RemoveSession,
            MutationAuthorizationBoundary::Application(AccessAction::RemoveSession),
        ),
        (
            ProductOperation::ResizeTerminal,
            MutationAuthorizationBoundary::Application(AccessAction::ResizeTerminal),
        ),
        (
            ProductOperation::SendTerminalInput,
            MutationAuthorizationBoundary::Application(AccessAction::SendTerminalInput),
        ),
        (
            ProductOperation::CloseStandardInput,
            MutationAuthorizationBoundary::Application(AccessAction::CloseStandardInput),
        ),
        (
            ProductOperation::ExecuteInSession,
            MutationAuthorizationBoundary::Application(AccessAction::ExecuteInSessionContext),
        ),
        (
            ProductOperation::UpsertWorkItem,
            MutationAuthorizationBoundary::Application(AccessAction::WriteWorkContext),
        ),
        (
            ProductOperation::RemoveWorkItem,
            MutationAuthorizationBoundary::Application(AccessAction::WriteWorkContext),
        ),
        (
            ProductOperation::ShutdownDaemon,
            MutationAuthorizationBoundary::Application(AccessAction::ControlDaemon),
        ),
        (
            ProductOperation::RepairOperationAttempt,
            MutationAuthorizationBoundary::RetainedAttemptEvidence,
        ),
        (
            ProductOperation::CancelRuntimeAttempt,
            MutationAuthorizationBoundary::RuntimeCancellationControl,
        ),
        (
            ProductOperation::ReplyAgentEvent,
            MutationAuthorizationBoundary::Application(AccessAction::ReplyAgentEvent),
        ),
    ]);
    let registered_mutations = ProductOperation::ALL
        .into_iter()
        .filter(|operation| operation.is_mutating())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        mutation_authorization
            .keys()
            .copied()
            .collect::<BTreeSet<_>>(),
        registered_mutations,
        "every mutation must select an independent authorization boundary"
    );

    let stream_authorization = BTreeMap::from([
        (ProductStreamKind::SessionWatch, AccessAction::WatchSessions),
        (ProductStreamKind::SessionEvents, AccessAction::ReadEvents),
        (ProductStreamKind::AgentEvents, AccessAction::ReadEvents),
        (
            ProductStreamKind::TerminalOutput,
            AccessAction::SubscribeTerminal,
        ),
    ]);
    assert_eq!(
        stream_authorization
            .keys()
            .copied()
            .collect::<BTreeSet<_>>(),
        ProductStreamKind::ALL.into_iter().collect::<BTreeSet<_>>(),
        "every subscription kind must select an independent authorization action"
    );

    let authorizer = Arc::new(RecordingDeny::new());
    let harness = executor_with_authorizer(authorizer.clone());
    let specifications = [
        ProductStreamSpec::SessionWatch(SessionWatchSpec {
            query: list_request(None),
        }),
        ProductStreamSpec::SessionEvents(SessionEventsSpec {
            session_ids: Vec::new(),
            kinds: Vec::new(),
            after_sequence: None,
        }),
        ProductStreamSpec::TerminalOutput(TerminalOutputSpec {
            session_id: "session-000".to_owned(),
            initial: TerminalInitialModeDto::None,
        }),
        ProductStreamSpec::AgentEvents(AgentEventsSpec {
            tenant_id: "tenant-test".to_owned(),
            source_product: "example/workflow".to_owned(),
            actor_context_ref: "actor:test".to_owned(),
            authority_ref: "authority:test".to_owned(),
            undercurrent_provider_id: "provider-test".to_owned(),
            undercurrent_provider_generation: 1,
            session_id: "session-000".to_owned(),
            attempt_id: "attempt:test".to_owned(),
            attempt_revision: 1,
            recovery_epoch: 1,
            fence: 1,
            request_digest_sha256: "a".repeat(64),
            after_sequence: 0,
        }),
    ];
    for (index, specification) in specifications.into_iter().enumerate() {
        let kind = specification.kind();
        let context = ProductStreamContext {
            authentication: authentication(),
            connection_id: ConnectionId::new(format!("authorization-connection-{index}"))
                .expect("connection ID"),
            server_epoch: ServerEpoch::new("authorization-epoch").expect("server epoch"),
            client_id: client("authorization-client"),
            request_id: RequestId::new(format!("authorization-request-{index}"))
                .expect("request ID"),
            stream_id: StreamId::new(format!("authorization-stream-{index}")).expect("stream ID"),
            trace_context: trace_context(index),
            resume: None,
            cancellation: ProductCancellation::new(),
        };
        let error = match harness.executor.prepare_stream(&context, specification) {
            Err(error) => error,
            Ok(_) => panic!("denied subscription must stop before source registration"),
        };
        assert_eq!(error.kind, Some(kind));
        assert_eq!(error.category, ProductErrorCategory::PermissionDenied);
        assert_eq!(harness.streams.pressure().active_streams, 0);
    }
    assert_eq!(
        authorizer.actions(),
        ProductStreamKind::ALL
            .into_iter()
            .map(|kind| stream_authorization[&kind])
            .collect::<Vec<_>>()
    );
}

#[test]
fn daemon_operations_are_finite_authorized_and_configured() {
    let harness = executor();
    let owner = client("operator");

    let diagnostics = harness.executor.execute(
        &context(owner.clone(), 32),
        ProductRequest::GetDiagnostics(GetDiagnosticsRequest {
            sections: vec![DiagnosticSectionDto::Streams],
            page_size: 10,
            cursor: None,
        }),
    );
    assert!(matches!(
        diagnostics,
        ProductResponse::Success(result)
            if matches!(*result, ProductSuccess::GetDiagnostics(ref page)
                if page.items.is_empty() && page.next_cursor.is_none())
    ));

    let diagnostic_session = SessionId::new("diagnostic-session").expect("diagnostic session");
    let private_terminal = b"private-diagnostic-terminal";
    harness
        .output
        .append(&diagnostic_session, private_terminal)
        .expect("diagnostic terminal output appends");
    let safe_bundle = harness.executor.execute(
        &context(owner.clone(), 35),
        ProductRequest::CreateDiagnosticBundle(CreateDiagnosticBundleRequest {
            sections: DiagnosticSectionDto::ALL.to_vec(),
            page_size: 10,
            cursor: None,
            terminal_content: None,
        }),
    );
    let safe_debug = format!("{safe_bundle:?}");
    assert!(!safe_debug.contains("private-diagnostic-terminal"));
    assert!(matches!(
        safe_bundle,
        ProductResponse::Success(result)
            if matches!(*result, ProductSuccess::CreateDiagnosticBundle(ref bundle)
                if bundle.terminal_content_redacted && bundle.terminal_content.is_none())
    ));

    let explicit_bundle = harness.executor.execute(
        &context(owner.clone(), 36),
        ProductRequest::CreateDiagnosticBundle(CreateDiagnosticBundleRequest {
            sections: vec![DiagnosticSectionDto::Streams],
            page_size: 10,
            cursor: None,
            terminal_content: Some(DiagnosticBundleTerminalContentRequestDto {
                session_id: diagnostic_session.as_str().to_owned(),
                from_offset: 0,
                max_bytes: 1024,
            }),
        }),
    );
    let explicit_debug = format!("{explicit_bundle:?}");
    assert!(!explicit_debug.contains("private-diagnostic-terminal"));
    let ProductResponse::Success(explicit_bundle) = explicit_bundle else {
        panic!("explicit diagnostic bundle failed")
    };
    let ProductSuccess::CreateDiagnosticBundle(explicit_bundle) = *explicit_bundle else {
        panic!("unexpected diagnostic bundle result")
    };
    assert!(!explicit_bundle.terminal_content_redacted);
    let terminal = explicit_bundle
        .terminal_content
        .expect("explicit terminal content");
    assert_eq!(terminal.session_id, diagnostic_session.as_str());
    assert!(matches!(
        terminal.page,
        TerminalOutputPageDto::Data { ref bytes, .. } if bytes == private_terminal
    ));

    let status = harness
        .executor
        .execute(&context(owner.clone(), 33), ProductRequest::GetDaemonStatus);
    assert!(matches!(
        status,
        ProductResponse::Success(result)
            if matches!(*result, ProductSuccess::GetDaemonStatus(ref status)
                if status.managed_sessions == 300 && status.accepting_connections)
    ));
    assert_eq!(harness.daemon_status_calls.load(Ordering::Acquire), 1);

    let shutdown = harness.executor.execute(
        &context(owner, 34),
        ProductRequest::ShutdownDaemon(ShutdownDaemonRequest {
            mode: Default::default(),
            mutation: MutationContext::default(),
            drain_timeout_millis: 1_000,
        }),
    );
    assert!(matches!(
        shutdown,
        ProductResponse::Error(error)
            if error.category == ProductErrorCategory::InvalidConfiguration
    ));
}

#[test]
fn denied_daemon_operation_stops_before_control_plane_access() {
    let harness = executor_with_authorizer(Arc::new(DenyAll));
    let response = harness.executor.execute(
        &context(client("denied-operator"), 35),
        ProductRequest::GetDaemonStatus,
    );

    assert!(matches!(
        response,
        ProductResponse::Error(error)
            if error.category == ProductErrorCategory::PermissionDenied
    ));
    assert_eq!(harness.daemon_status_calls.load(Ordering::Acquire), 0);
}

fn prepare_and_activate_stream(
    harness: &ExecutorHarness,
    connection: &Arc<ServerConnectionSession>,
    index: usize,
    specification: ProductStreamSpec,
) {
    let configuration = ProductStreamPolicy::for_specification(&specification).configuration;
    prepare_and_activate_stream_with_configuration(
        harness,
        connection,
        index,
        specification,
        configuration,
    );
}

fn prepare_and_activate_stream_with_configuration(
    harness: &ExecutorHarness,
    connection: &Arc<ServerConnectionSession>,
    index: usize,
    specification: ProductStreamSpec,
    configuration: ServerStreamConfiguration,
) {
    let request_id = RequestId::new(format!("stream-request-{index}")).expect("request ID");
    let stream_id = stream_id(index);
    connection
        .accept(ClientMessage::OpenStream {
            request_id: request_id.clone(),
            stream_id: stream_id.clone(),
            trace_context: trace_context(index),
            payload: Arc::from(b"prepared-directly".as_slice()),
        })
        .expect("stream reservation");
    let context = ProductStreamContext {
        authentication: authentication(),
        connection_id: connection.agreement().connection_id().clone(),
        server_epoch: connection.agreement().server_epoch().clone(),
        client_id: connection.client_id().clone(),
        request_id: request_id.clone(),
        stream_id: stream_id.clone(),
        trace_context: trace_context(index),
        resume: None,
        cancellation: ProductCancellation::new(),
    };
    let prepared = harness
        .executor
        .prepare_stream(&context, specification)
        .expect("product stream prepares");
    let outcome = connection
        .activate_stream(
            &request_id,
            &stream_id,
            configuration,
            Arc::from(b"opened".as_slice()),
        )
        .expect("transport stream activates");
    prepared.activate(ProductStreamPublisher::new(
        connection.clone(),
        stream_id,
        outcome.last_sequence,
        product_codec(),
        TerminalOutputCodec::new(8 * 1_024).expect("terminal codec"),
    ));
}

fn collect_stream_data(
    connection: &ServerConnectionSession,
    expected: usize,
    timeout: Duration,
) -> Vec<(StreamId, Arc<[u8]>)> {
    collect_stream_data_and_responses(connection, expected, 0, timeout)
}

// Weighted scheduling can deliver initial data before the open response. Keep
// both, as the SDK does with its pre-registered stream mailbox.
fn collect_stream_data_and_responses(
    connection: &ServerConnectionSession,
    expected: usize,
    expected_responses: usize,
    timeout: Duration,
) -> Vec<(StreamId, Arc<[u8]>)> {
    let deadline = Instant::now() + timeout;
    let mut items = Vec::with_capacity(expected);
    let mut responses = 0;
    while (items.len() < expected || responses < expected_responses) && Instant::now() < deadline {
        if let Some(write) = connection.next_write().expect("next write") {
            let message = decode_server(write.bytes());
            write.confirm_written().expect("write confirms");
            responses += usize::from(matches!(message, ServerMessage::Response { .. }));
            if let ServerMessage::StreamItem {
                stream_id,
                item:
                    InboundStreamItem::Data {
                        bytes,
                        flow_controlled_bytes,
                        ..
                    },
            } = message
            {
                connection
                    .accept(ClientMessage::WindowUpdate {
                        stream_id: stream_id.clone(),
                        additional_bytes: flow_controlled_bytes,
                    })
                    .expect("stream credit replenishes");
                items.push((stream_id, bytes));
            }
        } else {
            thread::sleep(Duration::from_millis(1));
        }
    }
    assert_eq!(
        items.len(),
        expected,
        "timed out waiting for stream data; connection pressure: {:?}",
        connection.pressure()
    );
    assert!(
        responses >= expected_responses,
        "timed out waiting for open responses"
    );
    items
}

fn drain_responses(connection: &ServerConnectionSession, expected: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    let mut responses = 0;
    while responses < expected && Instant::now() < deadline {
        if let Some(write) = connection.next_write().expect("next write") {
            let message = decode_server(write.bytes());
            assert!(
                !matches!(message, ServerMessage::StreamItem { .. }),
                "response-only helper must not discard stream items"
            );
            write.confirm_written().expect("write confirms");
            responses += usize::from(matches!(message, ServerMessage::Response { .. }));
        } else {
            thread::sleep(Duration::from_millis(1));
        }
    }
    assert_eq!(responses, expected, "timed out waiting for responses");
}

fn stream_connection(max_streams: u32) -> ServerConnectionSession {
    stream_connection_with_stream_queue(max_streams, 64 * 1_024)
}

fn stream_connection_with_stream_queue(
    max_streams: u32,
    stream_queue_bytes: u64,
) -> ServerConnectionSession {
    let limits = ConnectionLimits::new(
        8 * 1_024,
        max_streams,
        max_streams,
        8 * 1_024 * 1_024,
        stream_queue_bytes,
        stream_queue_bytes,
    )
    .expect("connection limits");
    let capabilities = BTreeSet::from([
        capability(connection_capabilities::MULTIPLEX),
        capability(connection_capabilities::FLOW_CONTROL),
        capability(connection_capabilities::STREAM_RESUME),
        capability(connection_capabilities::BINARY_PAYLOADS),
        capability(connection_capabilities::TRACE_CONTEXT),
    ]);
    let hello = ConnectionHello::new(
        client("stream-client"),
        CURRENT_PROTOCOL_VERSION,
        capabilities.clone(),
        BTreeSet::new(),
        limits,
    )
    .expect("hello");
    let policy = ConnectionPolicy::new(
        CURRENT_PROTOCOL_VERSION,
        capabilities,
        limits,
        HeartbeatPolicy::new(5_000, 15_000).expect("heartbeat"),
        Some(30_000),
    )
    .expect("policy");
    let epoch = ServerEpoch::new("stream-epoch").expect("epoch");
    let agreement = negotiate_connection(
        &hello,
        &policy,
        ConnectionId::new("stream-connection").expect("connection ID"),
        epoch.clone(),
    )
    .expect("agreement");
    let scheduler = SchedulerLimits::new(
        limits,
        [
            512 * 1_024,
            1024 * 1_024,
            2 * 1_024 * 1_024,
            2 * 1_024 * 1_024,
        ],
        limits.connection_queue_bytes(),
        64 * 1_024,
        undercurrent_multiplex::SchedulerMessageLimits::new(
            8_192,
            1_024,
            [1_024, 2_048, 2_048, 3_072],
        )
        .expect("scheduler message limits"),
    )
    .expect("scheduler limits");
    ServerConnectionSession::new(
        client("stream-client"),
        agreement,
        scheduler,
        Arc::new(ReplayJournal::new(
            epoch,
            ReplayLimits::new(
                max_streams as usize,
                64 * 1_024,
                8 * 1_024 * 1_024,
                8 * 1_024,
            )
            .expect("replay limits"),
        )),
        ServerDiagnosticLimits::new(max_streams, max_streams).expect("diagnostic limits"),
    )
    .expect("server connection")
}

fn product_codec() -> ProductCodec {
    ProductCodec::new(
        ProductCodecLimits::new(8 * 1_024, 8 * 1_024, 8 * 1_024, 8 * 1_024)
            .expect("product codec limits"),
    )
}

fn finite_product_codec() -> ProductCodec {
    ProductCodec::new(
        ProductCodecLimits::new(131_072, 196_608, 65_536, 131_072)
            .expect("finite product codec limits"),
    )
}

fn decode_server(bytes: &[u8]) -> ServerMessage {
    match WireCodec::new(8 * 1_024)
        .expect("wire codec")
        .decode_server(bytes)
        .expect("server decode")
    {
        DecodeStatus::Complete(frame) => frame.message,
        DecodeStatus::NeedMore { .. } => panic!("expected complete frame"),
    }
}

fn capability(value: &str) -> NamespacedName {
    NamespacedName::new(value).expect("capability")
}

fn stream_id(index: usize) -> StreamId {
    StreamId::new(format!("stream-{index}")).expect("stream ID")
}

fn context(client_id: ClientId, index: u64) -> ProductRequestContext {
    ProductRequestContext {
        response_write: Arc::default(),
        authentication: authentication(),
        connection_id: ConnectionId::new(format!("request-connection-{index}"))
            .expect("connection ID"),
        server_epoch: ServerEpoch::new("application-test-epoch").expect("server epoch"),
        client_id,
        request_id: RequestId::new(format!("request-{index}")).expect("request ID"),
        trace_context: trace_context(index as usize),
        cancellation: ProductCancellation::new(),
    }
}

fn authentication() -> Arc<AuthenticatedRequestContext> {
    Arc::new(AuthenticatedRequestContext::new(
        AuthenticatedPrincipalRef::new("application-test").expect("principal"),
        None,
        AuthorityRefDto::new("local").expect("authority"),
    ))
}

fn client(value: &str) -> ClientId {
    ClientId::new(value).expect("client ID")
}

fn trace_context(index: usize) -> TraceContext {
    TraceContext::new(
        TraceId::new(format!("trace-{index}")).expect("trace ID"),
        SpanId::new(format!("span-{index}")).expect("span ID"),
    )
}

fn start_request(
    name: &str,
    program: &str,
    profile: Option<&str>,
    detection_enabled: bool,
) -> StartSessionRequest {
    StartSessionRequest {
        launch_mode: undercurrent_protocol::LaunchModeDto::Sealed,
        mutation: MutationContext::default(),
        dispatch: None,
        name: name.to_owned(),
        command: CommandSpecDto {
            program: program.to_owned(),
            arguments: Vec::new(),
        },
        working_directory: None,
        environment: EnvironmentSpecDto::default(),
        terminal: TerminalSizeDto {
            columns: 80,
            rows: 24,
        },
        output_capture_mode: undercurrent_protocol::OutputCaptureModeDto::Terminal,
        execution: ExecutionPolicyDto {
            detection_enabled,
            ..ExecutionPolicyDto::default()
        },
        health: HealthRulesDto::default(),
        profile: profile.map(str::to_owned),
        profile_binding: undercurrent_protocol::ProfileBindingModeDto::Fixed,
        labels: BTreeMap::new(),
        metadata: BTreeMap::new(),
    }
}

fn list_request(cursor: Option<String>) -> ListSessionsRequest {
    ListSessionsRequest {
        filter: SessionFilterDto::default(),
        sort: SessionSortDto::default(),
        projection: SessionProjectionDto::Compact,
        page_size: 25,
        cursor,
        include_total: false,
    }
}

fn session(index: usize) -> Session {
    create_session(index).0
}

fn create_session(index: usize) -> (Session, undercurrent_domain::DomainChange) {
    Session::create(
        SessionId::new(format!("session-{index:03}")).expect("session ID"),
        SessionName::new(format!("session-{index:03}")).expect("session name"),
        SessionConfiguration::new(
            LaunchSpec::new(
                CommandSpec::new("sh", Vec::new(), execution_limits()).expect("command"),
                EnvironmentVariables::default(),
                None,
                None,
                TerminalSize::new(80, 24, 400, 200).expect("terminal"),
            ),
            ExecutionPolicy::new(
                RestartConfig::never(),
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
        64,
        TimestampMillis::new(index as i64),
    )
    .expect("session")
}

fn profile(name: &str, binary: &str) -> DetectionProfile {
    let limits = detection_profile_limits();
    let matching = ProfileMatchRules::new(
        vec![BinaryPattern::new(binary, limits.max_pattern_bytes).expect("binary pattern")],
        Vec::new(),
        BTreeSet::new(),
        limits,
    )
    .expect("profile matching");
    DetectionProfile::new(
        ProfileName::new(name).expect("profile name"),
        Some(format!("{name} profile")),
        false,
        matching,
        DetectionArchetype::Unclassified,
        undercurrent_domain::InputDeliveryPolicy::immediate(),
        None,
        Vec::new(),
        Vec::new(),
        false,
        false,
        OutputExclusions::default(),
        limits,
    )
    .expect("profile")
}

fn detection_profile_limits() -> DetectionProfileLimits {
    DetectionProfileLimits {
        max_description_bytes: 1_024,
        max_binary_patterns: 32,
        max_argument_patterns: 32,
        max_environment_names: 32,
        max_environment_name_bytes: 128,
        max_foreground_rules: 32,
        max_background_rules: 32,
        max_patterns_per_rule: 32,
        max_pattern_bytes: 1_024,
        max_trait_needles: 32,
        max_trait_text_bytes: 256,
        max_input_submission_delay_millis: 2_000,
    }
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
