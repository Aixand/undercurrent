// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};
use std::time::Duration;

use undercurrent_application::*;
use undercurrent_domain::*;

#[test]
fn execution_selects_host_private_exports_and_explicit_launch_fallback() {
    for available in [false, true] {
        let session = session();
        let repository = TestRepository::new(Some(session.clone()));
        let authorizer = TestAuthorizer::new(AccessDecision::Permit);
        let resolver = TestExecutionContextResolver {
            live: available,
            ..Default::default()
        };
        let clock = TestClock::default();
        let telemetry = TestTelemetry::default();
        let catalog = PreparedSessionCatalog::new();
        let service = ResolveExecutionContextService::new(
            &authorizer,
            &repository,
            &resolver,
            &clock,
            &telemetry,
        );
        let result = block_on(service.resolve_for_execution(session.id(), &catalog, true, false));
        if available {
            let context = result.unwrap();
            assert!(context.environment().is_empty());
            assert_eq!(
                context.live_environment_process().unwrap().as_str(),
                "process-1"
            );
        } else {
            assert!(result.unwrap_err().to_string().contains("--env-launch"));
        }
        let launch =
            block_on(service.resolve_for_execution(session.id(), &catalog, true, true)).unwrap();
        assert!(launch.live_environment_process().is_none());
        assert_eq!(
            launch.environment().get("API_TOKEN").unwrap(),
            "private-resolved-value"
        );
    }
}

#[test]
fn execution_uses_live_directory_unless_launch_is_requested() {
    for binding in [
        ProfileBindingMode::Fixed,
        ProfileBindingMode::ForegroundAdaptive,
    ] {
        for observed_process in [Some("process-1"), Some("previous-process"), None] {
            let session = session_with_process("process-1", binding);
            let authorizer = TestAuthorizer::new(AccessDecision::Permit);
            let repository = TestRepository::new(Some(session.clone()));
            let resolver = TestExecutionContextResolver::default();
            let clock = TestClock::default();
            let telemetry = TestTelemetry::default();
            let catalog = PreparedSessionCatalog::new();
            if let Some(process) = observed_process {
                let mut prepared = session_with_process(process, binding).summary();
                prepared
                    .apply_effective_profile(
                        EffectiveProfileTransition::new(
                            ProcessInstanceId::new(process).unwrap(),
                            EffectiveProfileSelection::Profile(ProfileName::new("agent").unwrap()),
                            if binding == ProfileBindingMode::Fixed {
                                EffectiveProfileOrigin::BaseProfile
                            } else {
                                EffectiveProfileOrigin::CooperativeShell
                            },
                            TimestampMillis::new(3),
                        )
                        .with_runtime_context(
                            Some(PathReference::new("/live/project", 1024).unwrap()),
                            None,
                        ),
                    )
                    .unwrap();
                catalog.upsert(prepared).unwrap();
            }
            let service = ResolveExecutionContextService::new(
                &authorizer,
                &repository,
                &resolver,
                &clock,
                &telemetry,
            );
            let live = block_on(service.resolve_for_execution(session.id(), &catalog, false, true));
            if observed_process == Some("process-1") {
                let live = live.unwrap();
                assert_eq!(live.working_directory(), Some("/live/project"));
                assert_eq!(
                    live.environment().get("API_TOKEN").map(String::as_str),
                    Some("private-resolved-value")
                );
            } else {
                let error = live.unwrap_err();
                assert_eq!(error.category(), ApplicationErrorCategory::Unavailable);
                assert!(error.to_string().contains("--dir-launch"));
            }
            let launch =
                block_on(service.resolve_for_execution(session.id(), &catalog, true, true))
                    .unwrap();
            assert_eq!(
                launch.working_directory(),
                Some("/private/working-directory")
            );
        }
    }
}

#[test]
fn detail_uses_live_directory_only_for_the_matching_process() {
    for binding in [
        ProfileBindingMode::Fixed,
        ProfileBindingMode::ForegroundAdaptive,
    ] {
        for prepared_process in ["process-1", "previous-process"] {
            let session = session_with_process("process-1", binding);
            let authorizer = TestAuthorizer::new(AccessDecision::Permit);
            let repository = TestRepository::new(Some(session.clone()));
            let processes = TestProcesses::default();
            let clock = TestClock::default();
            let telemetry = TestTelemetry::default();
            let catalog = PreparedSessionCatalog::new();
            let mut prepared = session_with_process(prepared_process, binding).summary();
            prepared
                .apply_effective_profile(
                    EffectiveProfileTransition::new(
                        ProcessInstanceId::new(prepared_process).unwrap(),
                        EffectiveProfileSelection::Profile(ProfileName::new("agent").unwrap()),
                        if binding == ProfileBindingMode::Fixed {
                            EffectiveProfileOrigin::BaseProfile
                        } else {
                            EffectiveProfileOrigin::CooperativeShell
                        },
                        TimestampMillis::new(3),
                    )
                    .with_runtime_context(
                        Some(PathReference::new("/live/project", 1024).unwrap()),
                        None,
                    ),
                )
                .unwrap();
            let expected = prepared.effective_profile().cloned();
            catalog.upsert(prepared).unwrap();
            let service = GetSessionDetailService::new(
                &authorizer,
                &repository,
                &processes,
                &catalog,
                &clock,
                &telemetry,
            );
            let detail =
                block_on(service.get(session.id(), SessionDetailProjection::COMPLETE)).unwrap();
            assert_eq!(
                detail
                    .configuration()
                    .unwrap()
                    .working_directory()
                    .unwrap()
                    .as_str(),
                "working-directory"
            );
            if prepared_process == "process-1" {
                assert_eq!(detail.summary().effective_profile(), expected.as_ref());
            } else {
                assert!(detail.summary().effective_profile().is_none());
            }
        }
    }
}

#[test]
fn denial_happens_before_session_or_process_access() {
    let session = session();
    let authorizer = TestAuthorizer::new(AccessDecision::Deny(AccessDenialReason::Unauthenticated));
    let repository = TestRepository::new(Some(session.clone()));
    let processes = TestProcesses::default();
    let clock = TestClock::default();
    let telemetry = TestTelemetry::default();
    let catalog = PreparedSessionCatalog::new();
    let service = GetSessionDetailService::new(
        &authorizer,
        &repository,
        &processes,
        &catalog,
        &clock,
        &telemetry,
    );

    let error = block_on(service.get(session.id(), SessionDetailProjection::OVERVIEW))
        .expect_err("unauthenticated read is denied");

    assert_eq!(error.category(), ApplicationErrorCategory::Unauthenticated);
    assert_eq!(repository.load_calls(), 0);
    assert_eq!(processes.lookup_calls(), 0);
    assert_eq!(
        authorizer.requests(),
        vec![(
            AccessAction::ReadSessionDetail,
            AccessResource::Session(session.id().clone())
        )]
    );
    assert!(telemetry.has_event(DiagnosticEventName::AuthorizationDenied));
}

#[test]
fn complete_detail_excludes_environment_values_and_redacts_private_debug_data() {
    let session = session();
    let process = ProcessInstance {
        id: ProcessInstanceId::new("process-1").expect("process ID is valid"),
        started_at: TimestampMillis::new(2),
    };
    let authorizer = TestAuthorizer::new(AccessDecision::Permit);
    let repository = TestRepository::new(Some(session.clone()));
    let processes = TestProcesses::new(Some(process.clone()));
    let clock = TestClock::default();
    let telemetry = TestTelemetry::default();
    let catalog = PreparedSessionCatalog::new();
    let service = GetSessionDetailService::new(
        &authorizer,
        &repository,
        &processes,
        &catalog,
        &clock,
        &telemetry,
    );

    let detail = block_on(service.get(session.id(), SessionDetailProjection::COMPLETE))
        .expect("authorized detail succeeds");
    let configuration = detail.configuration().expect("configuration is projected");

    assert_eq!(configuration.command().program(), "private-program");
    assert_eq!(
        configuration.command().arguments(),
        &["--token".to_owned(), "private-argument".to_owned()]
    );
    assert_eq!(
        configuration.environment().variable_names(),
        &["MODE".to_owned(), "REGION".to_owned()]
    );
    assert_eq!(
        configuration.output_capture_mode(),
        OutputCaptureMode::Opaque
    );
    assert_eq!(detail.process(), Some(&process));
    assert!(detail.foreground_decision().is_some());
    assert!(detail.health_observation().is_some());
    assert_eq!(
        detail
            .work_context()
            .expect("work context is projected")
            .items()
            .len(),
        1
    );
    assert_eq!(detail.persistence(), SessionPersistenceStatus::Persisted);
    let debug = format!("{detail:?}");
    assert!(!debug.contains("private-environment-value"));
    assert!(!debug.contains("private-argument"));
    assert!(!debug.contains("private-metadata-value"));
    assert_eq!(repository.load_calls(), 1);
    assert_eq!(processes.lookup_calls(), 1);
    assert!(telemetry.has_event(DiagnosticEventName::SessionDetailLoaded));
}

#[test]
fn overview_projection_does_not_read_process_directory_or_allocate_detail_sections() {
    let session = session();
    let authorizer = TestAuthorizer::new(AccessDecision::Permit);
    let repository = TestRepository::new(Some(session.clone()));
    let processes = TestProcesses::default();
    let clock = TestClock::default();
    let telemetry = TestTelemetry::default();
    let catalog = PreparedSessionCatalog::new();
    let service = GetSessionDetailService::new(
        &authorizer,
        &repository,
        &processes,
        &catalog,
        &clock,
        &telemetry,
    );

    let detail = block_on(service.get(session.id(), SessionDetailProjection::OVERVIEW))
        .expect("overview succeeds");

    assert!(detail.configuration().is_none());
    assert!(detail.work_context().is_none());
    assert!(detail.process().is_none());
    assert!(detail.foreground_decision().is_none());
    assert!(detail.health_observation().is_none());
    assert_eq!(processes.lookup_calls(), 0);
}

#[test]
fn work_context_permission_is_evaluated_independently_before_loading_session() {
    let session = session();
    let authorizer = TestAuthorizer::denying(AccessAction::ReadWorkContext);
    let repository = TestRepository::new(Some(session.clone()));
    let processes = TestProcesses::default();
    let clock = TestClock::default();
    let telemetry = TestTelemetry::default();
    let catalog = PreparedSessionCatalog::new();
    let service = GetSessionDetailService::new(
        &authorizer,
        &repository,
        &processes,
        &catalog,
        &clock,
        &telemetry,
    );

    let projection = SessionDetailProjection::new(false, true, false, false, false);
    let error = block_on(service.get(session.id(), projection))
        .expect_err("WorkContext permission is required");

    assert_eq!(error.category(), ApplicationErrorCategory::PermissionDenied);
    assert_eq!(repository.load_calls(), 0);
    assert_eq!(
        authorizer.requests(),
        vec![
            (
                AccessAction::ReadSessionDetail,
                AccessResource::Session(session.id().clone())
            ),
            (
                AccessAction::ReadWorkContext,
                AccessResource::Session(session.id().clone())
            ),
        ]
    );
}

#[test]
fn missing_and_infrastructure_failures_are_typed_and_observable() {
    let session_id = SessionId::new("session-1").expect("session ID is valid");
    let authorizer = TestAuthorizer::new(AccessDecision::Permit);
    let repository = TestRepository::new(None);
    let processes = TestProcesses::default();
    let clock = TestClock::default();
    let telemetry = TestTelemetry::default();
    let catalog = PreparedSessionCatalog::new();
    let service = GetSessionDetailService::new(
        &authorizer,
        &repository,
        &processes,
        &catalog,
        &clock,
        &telemetry,
    );

    let missing = block_on(service.get(&session_id, SessionDetailProjection::OVERVIEW))
        .expect_err("missing session is rejected");
    assert_eq!(missing.category(), ApplicationErrorCategory::NotFound);
    assert!(telemetry.has_event(DiagnosticEventName::SessionDetailFailed));

    let failing_authorizer = TestAuthorizer::failing();
    let telemetry = TestTelemetry::default();
    let service = GetSessionDetailService::new(
        &failing_authorizer,
        &repository,
        &processes,
        &catalog,
        &clock,
        &telemetry,
    );
    let failure = block_on(service.get(&session_id, SessionDetailProjection::OVERVIEW))
        .expect_err("authorization infrastructure failure is returned");
    assert_eq!(failure.category(), ApplicationErrorCategory::Unavailable);
    assert!(!failure.to_string().contains("private-policy-detail"));
    let diagnostic = telemetry
        .diagnostics()
        .into_iter()
        .find(|record| record.event == DiagnosticEventName::SessionDetailFailed)
        .expect("failure diagnostic is emitted");
    assert_eq!(diagnostic.operation, Some(PortOperation::Authorize));
    assert_eq!(diagnostic.error_kind, Some(PortErrorKind::Unavailable));
    assert_eq!(
        diagnostic.application_error,
        Some(ApplicationErrorCategory::Unavailable)
    );
    assert_eq!(diagnostic.level, DiagnosticLevel::Warn);
}

#[test]
fn execution_context_is_authorized_before_private_resolution() {
    let session = session();
    let authorizer = TestAuthorizer::new(AccessDecision::Deny(AccessDenialReason::Forbidden));
    let repository = TestRepository::new(Some(session.clone()));
    let resolver = TestExecutionContextResolver::default();
    let clock = TestClock::default();
    let telemetry = TestTelemetry::default();
    let service = ResolveExecutionContextService::new(
        &authorizer,
        &repository,
        &resolver,
        &clock,
        &telemetry,
    );

    let error = block_on(service.resolve(session.id())).expect_err("permission is required");

    assert_eq!(error.category(), ApplicationErrorCategory::PermissionDenied);
    assert_eq!(repository.load_calls(), 0);
    assert_eq!(resolver.calls(), 0);
    assert_eq!(
        authorizer.requests(),
        vec![(
            AccessAction::ExecuteInSessionContext,
            AccessResource::Session(session.id().clone())
        )]
    );
}

#[test]
fn execution_context_returns_private_values_without_debug_exposure() {
    let session = session();
    let authorizer = TestAuthorizer::new(AccessDecision::Permit);
    let repository = TestRepository::new(Some(session.clone()));
    let resolver = TestExecutionContextResolver::default();
    let clock = TestClock::default();
    let telemetry = TestTelemetry::default();
    let service = ResolveExecutionContextService::new(
        &authorizer,
        &repository,
        &resolver,
        &clock,
        &telemetry,
    );

    let context = block_on(service.resolve(session.id())).expect("context resolves");

    assert_eq!(
        context.environment().get("API_TOKEN").map(String::as_str),
        Some("private-resolved-value")
    );
    assert_eq!(
        context.working_directory(),
        Some("/private/working-directory")
    );
    assert!(!format!("{context:?}").contains("private-resolved-value"));
    assert!(!format!("{context:?}").contains("working-directory"));
    assert_eq!(resolver.calls(), 1);
    assert!(telemetry.has_event(DiagnosticEventName::ExecutionContextResolved));
}

fn limits() -> ExecutionLimits {
    ExecutionLimits {
        max_arguments: 8,
        max_command_bytes: 1_024,
        max_environment_entries: 8,
        max_environment_bytes: 1_024,
        max_path_bytes: 1_024,
        max_health_rules: 8,
        max_health_rule_bytes: 1_024,
    }
}

fn session() -> Session {
    session_with_process("process-1", ProfileBindingMode::Fixed)
}

fn session_with_process(process_id: &str, binding: ProfileBindingMode) -> Session {
    let limits = limits();
    let mut configuration = SessionConfiguration::new(
        LaunchSpec::new(
            CommandSpec::new(
                "private-program",
                vec!["--token".to_owned(), "private-argument".to_owned()],
                limits,
            )
            .expect("command is valid"),
            EnvironmentVariables::new(
                [
                    ("REGION".to_owned(), "us-east-1".to_owned()),
                    ("MODE".to_owned(), "production".to_owned()),
                ],
                limits,
            )
            .expect("environment is valid"),
            Some(PathReference::new("environment.ref", 1_024).expect("path is valid")),
            Some(PathReference::new("working-directory", 1_024).expect("path is valid")),
            TerminalSize::new(80, 24, 500, 500).expect("terminal size is valid"),
        )
        .with_output_capture_mode(OutputCaptureMode::Opaque),
        ExecutionPolicy::new(
            RestartConfig::never(),
            StopPolicy::new(StopSignal::Terminate, Duration::from_secs(5))
                .expect("stop policy is valid"),
            true,
            false,
        ),
        HealthRules::default(),
        Some(ProfileName::new("agent").expect("profile is valid")),
        Labels::new([("environment".to_owned(), "test".to_owned())]).expect("labels are valid"),
        Metadata::new([(
            "private-key".to_owned(),
            "private-metadata-value".to_owned(),
        )])
        .expect("metadata is valid"),
    );
    configuration.set_profile_binding(binding);
    let mut session = Session::create(
        SessionId::new("session-1").expect("session ID is valid"),
        SessionName::new("worker").expect("session name is valid"),
        configuration,
        4,
        TimestampMillis::new(1),
    )
    .expect("session is valid")
    .0;
    session
        .upsert_work_item(
            WorkItem::new(
                WorkItemKey::new(
                    WorkOwner::new("workflow").expect("owner is valid"),
                    WorkItemId::new("work-1").expect("work ID is valid"),
                ),
                WorkRelationKind::DependsOn,
                "related work",
                Some("safe description".to_owned()),
                WorkStatus::Waiting,
                Vec::new(),
                Metadata::default(),
                TimestampMillis::new(1),
                TimestampMillis::new(1),
                None,
            )
            .expect("work item is valid"),
            TimestampMillis::new(1),
        )
        .expect("work item is added");
    session
        .record_process_ownership(
            ProcessIdentity::new(
                ProcessInstanceId::new(process_id).expect("process ID is valid"),
                TimestampMillis::new(2),
            ),
            TimestampMillis::new(2),
        )
        .expect("process ownership is durable");
    session
}

#[derive(Default)]
struct TestClock(AtomicI64);

impl Clock for TestClock {
    fn now(&self) -> TimestampMillis {
        TimestampMillis::new(self.0.fetch_add(1, Ordering::SeqCst))
    }
}

struct TestAuthorizer {
    decision: AccessDecision,
    denied_action: Option<AccessAction>,
    requests: Mutex<Vec<(AccessAction, AccessResource)>>,
    fail: bool,
}

impl TestAuthorizer {
    fn new(decision: AccessDecision) -> Self {
        Self {
            decision,
            denied_action: None,
            requests: Mutex::new(Vec::new()),
            fail: false,
        }
    }

    fn failing() -> Self {
        Self {
            decision: AccessDecision::Permit,
            denied_action: None,
            requests: Mutex::new(Vec::new()),
            fail: true,
        }
    }

    fn denying(action: AccessAction) -> Self {
        Self {
            decision: AccessDecision::Permit,
            denied_action: Some(action),
            requests: Mutex::new(Vec::new()),
            fail: false,
        }
    }

    fn requests(&self) -> Vec<(AccessAction, AccessResource)> {
        self.requests
            .lock()
            .expect("request lock is available")
            .clone()
    }
}

impl AuthorizationPort for TestAuthorizer {
    fn authorize<'a>(
        &'a self,
        action: AccessAction,
        resource: &'a AccessResource,
    ) -> PortFuture<'a, Result<AccessDecision, PortError>> {
        self.requests
            .lock()
            .expect("request lock is available")
            .push((action, resource.clone()));
        Box::pin(async move {
            if self.fail {
                return Err(port_error(PortOperation::Authorize, None));
            }
            Ok(if self.denied_action == Some(action) {
                AccessDecision::Deny(AccessDenialReason::Forbidden)
            } else {
                self.decision
            })
        })
    }
}

struct TestRepository {
    session: Option<Session>,
    load_calls: AtomicUsize,
}

impl TestRepository {
    fn new(session: Option<Session>) -> Self {
        Self {
            session,
            load_calls: AtomicUsize::new(0),
        }
    }

    fn load_calls(&self) -> usize {
        self.load_calls.load(Ordering::SeqCst)
    }
}

impl SessionRepository for TestRepository {
    fn insert<'a>(
        &'a self,
        _: &'a Session,
        _: &'a SessionOutboxEvent,
    ) -> PortFuture<'a, Result<InsertSessionOutcome, PortError>> {
        Box::pin(async { Err(port_error(PortOperation::InsertSession, None)) })
    }

    fn load<'a>(&'a self, _: &'a SessionId) -> PortFuture<'a, Result<Option<Session>, PortError>> {
        self.load_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(self.session.clone()) })
    }

    fn find_by_name<'a>(
        &'a self,
        _: &'a SessionName,
    ) -> PortFuture<'a, Result<Option<Session>, PortError>> {
        Box::pin(async { Err(port_error(PortOperation::FindSessionByName, None)) })
    }

    fn commit<'a>(
        &'a self,
        _: &'a Session,
        _: Revision,
        _: &'a [SessionOutboxEvent],
    ) -> PortFuture<'a, Result<CommitSessionOutcome, PortError>> {
        Box::pin(async { Err(port_error(PortOperation::CommitSession, None)) })
    }

    fn delete<'a>(
        &'a self,
        _: &'a SessionId,
        _: Revision,
        _: &'a SessionOutboxEvent,
    ) -> PortFuture<'a, Result<DeleteSessionOutcome, PortError>> {
        Box::pin(async { Err(port_error(PortOperation::DeleteSession, None)) })
    }
}

#[derive(Default)]
struct TestProcesses {
    process: Mutex<Option<ProcessInstance>>,
    lookup_calls: AtomicUsize,
}

impl TestProcesses {
    fn new(process: Option<ProcessInstance>) -> Self {
        Self {
            process: Mutex::new(process),
            lookup_calls: AtomicUsize::new(0),
        }
    }

    fn lookup_calls(&self) -> usize {
        self.lookup_calls.load(Ordering::SeqCst)
    }
}

impl SessionProcessDirectory for TestProcesses {
    fn bind(&self, _: &SessionId, process: ProcessInstance) -> Result<(), PortError> {
        *self.process.lock().expect("process lock is available") = Some(process);
        Ok(())
    }

    fn lookup(&self, _: &SessionId) -> Result<Option<ProcessInstance>, PortError> {
        self.lookup_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .process
            .lock()
            .expect("process lock is available")
            .clone())
    }

    fn unbind_if(
        &self,
        _: &SessionId,
        process: &ProcessInstanceId,
    ) -> Result<ProcessUnbindOutcome, PortError> {
        let mut current = self.process.lock().expect("process lock is available");
        match current.as_ref() {
            None => Ok(ProcessUnbindOutcome::Missing),
            Some(binding) if &binding.id != process => Ok(ProcessUnbindOutcome::ProcessMismatch),
            Some(_) => Ok(ProcessUnbindOutcome::Unbound(
                current.take().expect("matching process exists"),
            )),
        }
    }
}

#[derive(Default)]
struct TestExecutionContextResolver {
    live: bool,
    calls: AtomicUsize,
}

impl TestExecutionContextResolver {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl ExecutionContextResolver for TestExecutionContextResolver {
    fn shell_environment_observed_at<'a>(
        &'a self,
        _: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<Option<TimestampMillis>, PortError>> {
        Box::pin(async move {
            Ok((self.live && process.as_str() == "process-1").then_some(TimestampMillis::new(3)))
        })
    }

    fn resolve<'a>(
        &'a self,
        _: &'a LaunchSpec,
    ) -> PortFuture<'a, Result<ResolvedExecutionContext, PortError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(ResolvedExecutionContext::new(
                BTreeMap::from([("API_TOKEN".to_owned(), "private-resolved-value".to_owned())]),
                Some("/private/working-directory".to_owned()),
            ))
        })
    }
}

#[derive(Default)]
struct TestTelemetry {
    diagnostics: Mutex<Vec<DiagnosticRecord>>,
    metrics: Mutex<Vec<MetricObservation>>,
}

impl TestTelemetry {
    fn has_event(&self, event: DiagnosticEventName) -> bool {
        self.diagnostics
            .lock()
            .expect("diagnostic lock is available")
            .iter()
            .any(|record| record.event == event)
    }

    fn diagnostics(&self) -> Vec<DiagnosticRecord> {
        self.diagnostics
            .lock()
            .expect("diagnostic lock is available")
            .clone()
    }
}

impl Telemetry for TestTelemetry {
    fn emit_diagnostic(&self, record: &DiagnosticRecord) {
        self.diagnostics
            .lock()
            .expect("diagnostic lock is available")
            .push(record.clone());
    }

    fn record_metric(&self, observation: MetricObservation) {
        self.metrics
            .lock()
            .expect("metric lock is available")
            .push(observation);
    }

    fn record_session_metric(&self, observation: &SessionMetricObservation) {
        self.record_metric(observation.metric());
    }
}

#[derive(Debug)]
struct PrivatePolicyError;

impl Display for PrivatePolicyError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("private-policy-detail")
    }
}

impl Error for PrivatePolicyError {}

fn port_error(operation: PortOperation, session_id: Option<SessionId>) -> PortError {
    PortError::new(
        PortErrorKind::Unavailable,
        operation,
        session_id,
        PrivatePolicyError,
    )
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
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => thread::park(),
        }
    }
}
