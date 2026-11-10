// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::time::Duration;

use undercurrent_domain::*;

#[test]
fn domain_error_categories_have_unique_stable_names() {
    let names = ErrorCategory::ALL
        .iter()
        .copied()
        .map(ErrorCategory::as_str)
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(names.len(), ErrorCategory::ALL.len());
}

fn timestamp(value: i64) -> TimestampMillis {
    TimestampMillis::new(value)
}

fn session_id(value: &str) -> SessionId {
    SessionId::new(value).expect("test session ID is valid")
}

fn session_name(value: &str) -> SessionName {
    SessionName::new(value).expect("test session name is valid")
}

#[test]
fn work_context_scopes_same_item_run_by_owner_and_preserves_distinct_outcomes() {
    let mut context = WorkContext::new(8).expect("work context limits are valid");
    context
        .upsert(work_item(
            "workflow",
            "run-42-item-7",
            WorkStatus::Completed,
            Vec::new(),
            None,
        ))
        .expect("workflow completion is retained");
    context
        .upsert(work_item(
            "external-app",
            "run-42-item-7",
            WorkStatus::Failed,
            Vec::new(),
            None,
        ))
        .expect("external app failure with the same opaque ID is retained separately");
    for (item_id, status) in [
        ("cancelled", WorkStatus::Cancelled),
        ("delivery-failed", WorkStatus::DeliveryFailed),
        ("expired", WorkStatus::Expired),
    ] {
        context
            .upsert(work_item("workflow", item_id, status, Vec::new(), None))
            .expect("distinct outcome is retained");
    }

    assert_eq!(
        context
            .get(&work_key("workflow", "run-42-item-7"))
            .expect("workflow item")
            .status(),
        WorkStatus::Completed
    );
    assert_eq!(
        context
            .get(&work_key("external-app", "run-42-item-7"))
            .expect("external app item")
            .status(),
        WorkStatus::Failed
    );
    assert_eq!(
        [
            context
                .get(&work_key("workflow", "run-42-item-7"))
                .expect("completed item")
                .status(),
            context
                .get(&work_key("external-app", "run-42-item-7"))
                .expect("failed item")
                .status(),
            context
                .get(&work_key("workflow", "cancelled"))
                .expect("cancelled item")
                .status(),
            context
                .get(&work_key("workflow", "delivery-failed"))
                .expect("delivery-failed item")
                .status(),
            context
                .get(&work_key("workflow", "expired"))
                .expect("expired item")
                .status(),
        ],
        [
            WorkStatus::Completed,
            WorkStatus::Failed,
            WorkStatus::Cancelled,
            WorkStatus::DeliveryFailed,
            WorkStatus::Expired,
        ]
    );

    let summary = context.summary();
    assert_eq!(summary.item_count, 5);
    assert_eq!(summary.active_item_count, 1);
    assert_eq!(summary.aggregate_status, Some(WorkStatus::DeliveryFailed));
}

fn work_key(owner: &str, id: &str) -> WorkItemKey {
    WorkItemKey::new(
        WorkOwner::new(owner).expect("test owner is valid"),
        WorkItemId::new(id).expect("test work ID is valid"),
    )
}

fn work_item(
    owner: &str,
    id: &str,
    status: WorkStatus,
    targets: Vec<WorkTarget>,
    expires_at: Option<TimestampMillis>,
) -> WorkItem {
    WorkItem::new(
        work_key(owner, id),
        WorkRelationKind::DependsOn,
        format!("work {id}"),
        None,
        status,
        targets,
        Metadata::default(),
        timestamp(10),
        timestamp(10),
        expires_at,
    )
    .expect("test work item is valid")
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

fn session_configuration() -> SessionConfiguration {
    let limits = execution_limits();
    let launch = LaunchSpec::new(
        CommandSpec::new("sh", vec!["-lc".to_owned(), "sleep 10".to_owned()], limits)
            .expect("valid command"),
        EnvironmentVariables::new([("MODE".to_owned(), "test".to_owned())], limits)
            .expect("valid environment"),
        None,
        None,
        TerminalSize::new(80, 24, 500, 200).expect("valid terminal size"),
    );
    SessionConfiguration::new(
        launch,
        ExecutionPolicy::new(
            RestartConfig::never(),
            StopPolicy::new(StopSignal::Terminate, Duration::from_secs(5))
                .expect("valid stop policy"),
            true,
            false,
        ),
        HealthRules::new(Vec::new(), limits).expect("valid health rules"),
        Some(ProfileName::new("agent").expect("valid profile")),
        Labels::new([
            ("env".to_owned(), "test".to_owned()),
            ("team".to_owned(), "execution".to_owned()),
        ])
        .expect("valid labels"),
        Metadata::new([("private".to_owned(), "secret-value".to_owned())]).expect("valid metadata"),
    )
}

fn create_session() -> Session {
    Session::create(
        session_id("session-1"),
        session_name("worker-1"),
        session_configuration(),
        8,
        timestamp(10),
    )
    .expect("session creation succeeds")
    .0
}

fn running_session_with_state() -> Session {
    let mut session = create_session();
    record_test_process(&mut session, "process-state", timestamp(11));
    session
        .transition_lifecycle(LifecycleState::Running, timestamp(11))
        .expect("session enters running")
        .expect("running changes lifecycle");
    session
        .update_foreground(ForegroundObservation::new(
            ExecutionObservation::new(
                ExecutionState::Active,
                ObservationBasis::new(
                    Confidence::from_basis_points(7_500).expect("confidence"),
                    Some(EvidenceSource::new("process").expect("source")),
                    EvidenceDisposition::Inference,
                    timestamp(12),
                    Some(timestamp(12)),
                )
                .expect("basis"),
            )
            .expect("execution"),
            InteractionObservation::none(timestamp(12)),
            RuntimeConstraintObservation::clear(timestamp(12)),
        ))
        .expect("foreground updates")
        .expect("foreground changes");
    session
        .upsert_work_item(
            work_item("workflow", "build", WorkStatus::Running, Vec::new(), None),
            timestamp(13),
        )
        .expect("work item updates")
        .expect("work item changes");
    session
}

fn record_test_process(session: &mut Session, id: &str, started_at: TimestampMillis) {
    session
        .record_process_ownership(
            ProcessIdentity::new(
                ProcessInstanceId::new(id).expect("process identity is valid"),
                started_at,
            ),
            started_at.max(session.updated_at()),
        )
        .expect("starting session accepts process ownership")
        .expect("new process ownership changes the session");
}

#[test]
fn identifiers_reject_unsafe_values() {
    let whitespace = SessionId::new("session 1").expect_err("whitespace must fail");
    assert_eq!(whitespace.category(), ErrorCategory::InvalidArgument);

    assert!(SessionName::new("../escape").is_err());
    assert!(SessionName::new("nested/name").is_err());
    assert!(SessionName::new(" trailing ").is_err());
    assert!(SessionName::new("worker-1").is_ok());
}

#[test]
fn attributes_are_bounded_and_metadata_debug_is_redacted() {
    let excessive = (0..=MAX_LABELS)
        .map(|index| (format!("key-{index}"), "value".to_owned()))
        .collect::<Vec<_>>();
    let error = Labels::new(excessive).expect_err("label limit must be enforced");
    assert_eq!(error.category(), ErrorCategory::CapacityExceeded);

    let metadata = Metadata::new([("token".to_owned(), "do-not-print".to_owned())])
        .expect("metadata is valid");
    let diagnostic = format!("{metadata:?}");
    assert!(diagnostic.contains("entry_count"));
    assert!(!diagnostic.contains("token"));
    assert!(!diagnostic.contains("do-not-print"));
}

#[test]
fn launch_configuration_is_bounded_and_diagnostics_are_redacted() {
    let limits = execution_limits();
    let command = CommandSpec::new(
        "secret-program",
        vec!["--token".to_owned(), "private-token".to_owned()],
        limits,
    )
    .expect("command is valid");
    let environment =
        EnvironmentVariables::new([("REGION".to_owned(), "us-east-1".to_owned())], limits)
            .expect("environment is valid");

    let command_debug = format!("{command:?}");
    assert!(!command_debug.contains("secret-program"));
    assert!(!command_debug.contains("private-token"));
    let environment_debug = format!("{environment:?}");
    assert!(!environment_debug.contains("REGION"));
    assert!(!environment_debug.contains("us-east-1"));

    assert!(CommandSpec::new("", Vec::new(), limits).is_err());
    assert!(CommandSpec::new("sh\0bad", Vec::new(), limits).is_err());
    assert!(EnvironmentVariables::new([("BAD=KEY".to_owned(), String::new())], limits).is_err());
    assert!(
        EnvironmentVariables::new([("GOOD".to_owned(), "bad\0value".to_owned())], limits).is_err()
    );
}

#[test]
fn complete_session_debug_output_redacts_private_configuration() {
    let session = create_session();
    let diagnostic = format!("{session:?}");
    assert!(!diagnostic.contains("sleep 10"));
    assert!(!diagnostic.contains("MODE"));
    assert!(!diagnostic.contains("secret-value"));
}

#[test]
fn terminal_values_reject_invalid_dimensions_keys_and_coordinates() {
    assert!(TerminalSize::new(0, 24, 500, 200).is_err());
    assert!(TerminalSize::new(501, 24, 500, 200).is_err());
    let terminal = TerminalSize::new(80, 24, 500, 200).expect("terminal is valid");
    assert_eq!(terminal.columns(), 80);
    assert_eq!(terminal.rows(), 24);

    assert!(Key::function(0).is_err());
    assert!(Key::function(13).is_err());
    assert_eq!(
        Key::control('C').expect("control key is valid"),
        Key::Control(ControlKey::new('c').expect("control key is valid"))
    );
    assert!(Key::control('1').is_err());

    assert!(MouseEvent::new(0, 1, MouseButton::Left, MouseEventKind::Press, terminal,).is_err());
    let event = MouseEvent::new(80, 24, MouseButton::Left, MouseEventKind::Press, terminal)
        .expect("edge coordinate is valid");
    assert_eq!(event.column(), 80);
    assert_eq!(event.row(), 24);
}

#[test]
fn input_sequences_are_bounded_atomic_units_with_redacted_debug() {
    let sequence = InputSequence::new(
        vec![
            InputItem::Text("private prompt".to_owned()),
            InputItem::Key(Key::Enter),
        ],
        4,
        1_024,
    )
    .expect("input is valid");
    assert_eq!(sequence.items().len(), 2);
    assert_eq!(sequence.payload_bytes(), "private prompt".len() + 16);
    assert!(!format!("{sequence:?}").contains("private prompt"));

    assert!(InputSequence::new(Vec::new(), 4, 1_024).is_err());
    assert!(
        InputSequence::new(vec![InputItem::Bytes(vec![0; 10])], 4, 9)
            .expect_err("byte limit must be enforced")
            .category()
            == ErrorCategory::CapacityExceeded
    );

    assert!(InputSequence::new(vec![InputItem::Paste("x".to_owned())], 1, 12).is_err());
    assert_eq!(
        InputSequence::new(vec![InputItem::Paste("x".to_owned())], 1, 13)
            .expect("paste framing fits exactly")
            .payload_bytes(),
        13
    );

    let maximum_terminal =
        TerminalSize::new(u16::MAX, u16::MAX, u16::MAX, u16::MAX).expect("terminal");
    let maximum_mouse = MouseEvent::new(
        u16::MAX,
        u16::MAX,
        MouseButton::ScrollDown,
        MouseEventKind::Move,
        maximum_terminal,
    )
    .expect("mouse event");
    assert!(InputSequence::new(vec![InputItem::Mouse(maximum_mouse)], 1, 17).is_err());
    assert_eq!(
        InputSequence::new(vec![InputItem::Mouse(maximum_mouse)], 1, 18)
            .expect("maximum SGR mouse event fits exactly")
            .payload_bytes(),
        18
    );
}

#[test]
fn health_rules_are_bounded_deduplicated_and_redacted() {
    let limits = execution_limits();
    let endpoint = HealthRule::http_endpoint(
        "http://user:private-token@localhost/health",
        limits.max_health_rule_bytes,
    )
    .expect("endpoint is bounded");
    let rules = HealthRules::new(
        [
            endpoint.clone(),
            endpoint,
            HealthRule::tcp_port(8080).expect("port is valid"),
        ],
        limits,
    )
    .expect("rules are valid");
    assert_eq!(rules.len(), 2);
    assert!(!format!("{rules:?}").contains("private-token"));
    assert!(HealthRule::tcp_port(0).is_err());
    assert!(HealthRule::red_cell_threshold(0).is_err());

    let result = HealthCheckResult::new(
        0,
        false,
        Duration::from_millis(12),
        Some("credential leaked upstream".to_owned()),
        1_024,
    )
    .expect("health result is bounded");
    assert!(!format!("{result:?}").contains("credential"));
    assert_eq!(result.detail(), Some("credential leaked upstream"));
    assert!(
        HealthCheckResult::new(0, false, Duration::ZERO, Some("too long".to_owned()), 3).is_err()
    );
}

#[test]
fn configured_health_starts_unknown_without_executing_checks() {
    let limits = execution_limits();
    let configuration = SessionConfiguration::new(
        session_configuration().launch().clone(),
        session_configuration().execution().clone(),
        HealthRules::new([HealthRule::tcp_port(8080).expect("valid port")], limits)
            .expect("valid health rules"),
        None,
        Labels::default(),
        Metadata::default(),
    );
    let session = Session::create(
        session_id("health-session"),
        session_name("health-session"),
        configuration,
        4,
        timestamp(10),
    )
    .expect("session creation succeeds")
    .0;

    assert_eq!(session.health().status(), HealthStatus::Unknown);
    assert_eq!(session.health().configured_checks(), 1);
    assert_eq!(session.health().observed_at(), None);
}

#[test]
fn automatic_profile_selection_never_replaces_an_explicit_profile() {
    let explicit = ProfileName::new("explicit").expect("valid profile");
    let automatic = ProfileName::new("automatic").expect("valid profile");
    let mut configured = session_configuration();

    assert!(!configured.select_profile_if_absent(automatic.clone()));
    assert_eq!(
        configured.profile_origin(),
        Some(ProfileSelectionOrigin::Explicit)
    );
    assert_eq!(
        configured.profile(),
        Some(&ProfileName::new("agent").unwrap())
    );

    let mut unconfigured = SessionConfiguration::new(
        configured.launch().clone(),
        configured.execution().clone(),
        configured.health_rules().clone(),
        None,
        configured.labels().clone(),
        configured.metadata().clone(),
    );
    assert!(unconfigured.select_profile_if_absent(automatic.clone()));
    assert_eq!(unconfigured.profile(), Some(&automatic));
    assert_eq!(
        unconfigured.profile_origin(),
        Some(ProfileSelectionOrigin::LaunchCommand)
    );
    assert_eq!(unconfigured.profile_binding(), ProfileBindingMode::Fixed);
    unconfigured.set_profile_binding(ProfileBindingMode::ForegroundAdaptive);
    assert_eq!(
        unconfigured.profile_binding(),
        ProfileBindingMode::ForegroundAdaptive
    );
    assert!(!unconfigured.select_profile_if_absent(explicit));
    assert_eq!(unconfigured.profile(), Some(&automatic));
}

#[test]
fn changing_health_rules_resets_summary_without_running_adapter_io() {
    let limits = execution_limits();
    let mut session = create_session();
    assert_eq!(session.health().status(), HealthStatus::NoCheck);
    let configuration = SessionConfiguration::new(
        session.configuration().launch().clone(),
        session.configuration().execution().clone(),
        HealthRules::new([HealthRule::tcp_port(8080).expect("valid port")], limits)
            .expect("valid health rules"),
        session.configuration().profile().cloned(),
        session.configuration().labels().clone(),
        session.configuration().metadata().clone(),
    );

    session
        .update_configuration(configuration, timestamp(11))
        .expect("configuration update succeeds")
        .expect("configuration changed");
    assert_eq!(session.health().status(), HealthStatus::Unknown);
    assert_eq!(session.health().configured_checks(), 1);
    assert_eq!(session.health().observed_at(), None);
}

#[test]
fn stop_and_path_policies_reject_zero_limits() {
    assert!(StopPolicy::new(StopSignal::Terminate, Duration::ZERO).is_err());
    assert!(PathReference::new("/tmp/work", 0).is_err());
    let path = PathReference::new("/tmp/work", 128).expect("path reference is valid");
    assert_eq!(path.as_str(), "/tmp/work");
    assert!(!format!("{path:?}").contains("/tmp/work"));

    let mut limits = execution_limits();
    limits.max_command_bytes = 0;
    assert!(limits.validate().is_err());
}

#[test]
fn lifecycle_rejects_skipped_and_backward_transitions() {
    assert!(
        LifecycleState::Starting
            .validate_transition(LifecycleState::Running)
            .is_ok()
    );
    assert!(
        LifecycleState::Running
            .validate_transition(LifecycleState::Stopping)
            .is_ok()
    );
    assert!(
        LifecycleState::Stopping
            .validate_transition(LifecycleState::Stopped)
            .is_ok()
    );

    let error = LifecycleState::Running
        .validate_transition(LifecycleState::Stopped)
        .expect_err("running cannot skip directly to stopped");
    assert_eq!(error.category(), ErrorCategory::FailedPrecondition);
    assert!(
        LifecycleState::Exited(1)
            .validate_transition(LifecycleState::Running)
            .is_err()
    );
}

#[test]
fn requested_interaction_enforces_semantic_and_evidence_invariants() {
    let confidence = Confidence::from_basis_points(9_500).expect("confidence is bounded");
    let inferred_basis = ObservationBasis::new(
        confidence,
        Some(EvidenceSource::new("profile:codex").expect("valid source")),
        EvidenceDisposition::Inference,
        timestamp(20),
        Some(timestamp(18)),
    )
    .expect("valid inferred basis");
    assert!(
        InteractionObservation::new(
            InteractionState::Requested,
            None,
            Some(ObservationSemanticId::new("codex.permission").expect("valid semantic")),
            inferred_basis.clone(),
        )
        .is_err()
    );
    assert!(
        InteractionObservation::new(
            InteractionState::Requested,
            Some(InteractionModality::Options),
            None,
            inferred_basis.clone(),
        )
        .is_err()
    );
    assert!(
        InteractionObservation::new(
            InteractionState::Requested,
            Some(InteractionModality::Options),
            Some(ObservationSemanticId::new("codex.permission").expect("valid semantic")),
            inferred_basis,
        )
        .is_err()
    );

    let available = InteractionObservation::new(
        InteractionState::Available,
        Some(InteractionModality::Text),
        Some(ObservationSemanticId::new("codex.composer").expect("valid semantic")),
        ObservationBasis::new(
            confidence,
            Some(EvidenceSource::new("profile:codex").expect("valid source")),
            EvidenceDisposition::Inference,
            timestamp(20),
            Some(timestamp(18)),
        )
        .expect("valid inferred basis"),
    )
    .expect("non-actionable availability is representable");
    assert!(!available.is_actionable());

    let requested = InteractionObservation::new(
        InteractionState::Requested,
        Some(InteractionModality::Options),
        Some(ObservationSemanticId::new("codex.permission").expect("valid semantic")),
        ObservationBasis::new(
            confidence,
            Some(EvidenceSource::new("screen-pattern").expect("valid source")),
            EvidenceDisposition::Affirmative,
            timestamp(20),
            Some(timestamp(18)),
        )
        .expect("valid affirmative basis"),
    )
    .expect("affirmative requested interaction is valid");
    assert!(requested.is_actionable());
}

#[test]
fn health_requires_configuration_to_match_status() {
    assert!(HealthSummary::new(HealthStatus::Healthy, 0, Some(timestamp(10))).is_err());
    assert!(HealthSummary::new(HealthStatus::NoCheck, 1, None).is_err());

    let healthy = HealthSummary::new(HealthStatus::Healthy, 2, Some(timestamp(10)))
        .expect("configured health is valid");
    assert_eq!(healthy.status(), HealthStatus::Healthy);
    assert_eq!(healthy.configured_checks(), 2);
}

#[test]
fn revisions_are_monotonic_and_exhaustion_is_typed() {
    let first = Revision::INITIAL.next("test").expect("revision advances");
    assert_eq!(first.value(), 1);

    let error = Revision::new(u64::MAX)
        .next("test")
        .expect_err("maximum revision must not wrap");
    assert_eq!(error.category(), ErrorCategory::InvariantViolation);
}

#[test]
fn timestamp_addition_is_bounded_and_discards_sub_milliseconds() {
    assert_eq!(
        timestamp(10).saturating_add(Duration::from_millis(25)),
        timestamp(35)
    );
    assert_eq!(
        timestamp(10).saturating_add(Duration::from_nanos(999_999)),
        timestamp(10)
    );
    assert_eq!(
        timestamp(i64::MAX - 1).saturating_add(Duration::from_millis(2)),
        timestamp(i64::MAX)
    );
}

#[test]
fn timestamp_addition_saturates_the_final_sum_for_negative_epochs_and_huge_durations() {
    for (start, duration, expected) in [
        (-1, Duration::from_millis(i64::MAX as u64 + 1), i64::MAX),
        (i64::MIN, Duration::from_millis(i64::MAX as u64 + 1), 0),
        (i64::MIN, Duration::from_millis(u64::MAX), i64::MAX),
        (i64::MIN, Duration::MAX, i64::MAX),
        (i64::MIN, Duration::ZERO, i64::MIN),
        (i64::MAX, Duration::MAX, i64::MAX),
        (i64::MIN, Duration::from_nanos(999_999), i64::MIN),
    ] {
        assert_eq!(
            timestamp(start).saturating_add(duration),
            timestamp(expected)
        );
    }
}

#[test]
fn restart_backoff_is_bounded_and_attempts_exhaust() {
    let config = RestartConfig::new(
        RestartPolicy::Always,
        3,
        Duration::from_millis(100),
        20_000,
        Duration::from_millis(250),
        Duration::from_secs(10),
    )
    .expect("restart config is valid");
    let mut tracker = RestartTracker::default();

    assert_eq!(
        tracker.observe(&config, ProcessOutcome::Exited(1), Duration::from_secs(1)),
        RestartDecision::RestartAfter {
            attempt: 1,
            delay: Duration::from_millis(100)
        }
    );
    assert_eq!(
        tracker.observe(&config, ProcessOutcome::Exited(1), Duration::from_secs(1)),
        RestartDecision::RestartAfter {
            attempt: 2,
            delay: Duration::from_millis(200)
        }
    );
    assert_eq!(
        tracker.observe(
            &config,
            ProcessOutcome::SupervisionFailed,
            Duration::from_secs(1)
        ),
        RestartDecision::RestartAfter {
            attempt: 3,
            delay: Duration::from_millis(250)
        }
    );
    assert_eq!(
        tracker.observe(&config, ProcessOutcome::Exited(2), Duration::from_secs(1)),
        RestartDecision::Exhausted { attempts: 3 }
    );
}

#[test]
fn successful_on_failure_exit_resets_restart_tracking() {
    let config = RestartConfig::new(
        RestartPolicy::OnFailure,
        5,
        Duration::from_millis(10),
        10_000,
        Duration::from_millis(10),
        Duration::from_secs(30),
    )
    .expect("restart config is valid");
    let mut tracker = RestartTracker::new(4);

    assert_eq!(
        tracker.observe(&config, ProcessOutcome::Exited(0), Duration::from_secs(1)),
        RestartDecision::DoNotRestart(NoRestartReason::SuccessfulExit)
    );
    assert_eq!(tracker.consecutive_attempts(), 0);
}

#[test]
fn session_persists_restart_deadline_and_terminal_disposition() {
    let limits = execution_limits();
    let launch = LaunchSpec::new(
        CommandSpec::new("false", Vec::new(), limits).expect("valid command"),
        EnvironmentVariables::new(Vec::<(String, String)>::new(), limits)
            .expect("valid environment"),
        None,
        None,
        TerminalSize::new(80, 24, 500, 200).expect("valid terminal size"),
    );
    let restart = RestartConfig::new(
        RestartPolicy::OnFailure,
        1,
        Duration::from_millis(250),
        10_000,
        Duration::from_millis(250),
        Duration::ZERO,
    )
    .expect("valid restart policy");
    let configuration = SessionConfiguration::new(
        launch,
        ExecutionPolicy::new(
            restart,
            StopPolicy::new(StopSignal::Terminate, Duration::from_secs(1))
                .expect("valid stop policy"),
            true,
            false,
        ),
        HealthRules::new(Vec::new(), limits).expect("valid health rules"),
        None,
        Labels::default(),
        Metadata::default(),
    );
    let (mut session, _) = Session::create(
        session_id("restart-status"),
        session_name("restart-status"),
        configuration,
        1,
        timestamp(10),
    )
    .expect("session creation succeeds");
    record_test_process(&mut session, "process-restart-status", timestamp(11));
    session
        .transition_lifecycle(LifecycleState::Running, timestamp(11))
        .expect("session enters running")
        .expect("running changes lifecycle");
    session
        .transition_lifecycle(LifecycleState::Exited(1), timestamp(20))
        .expect("session records process exit")
        .expect("exit changes lifecycle");

    let (decision, change) = session
        .observe_process_outcome(
            ProcessOutcome::Exited(1),
            Duration::from_millis(10),
            timestamp(20),
        )
        .expect("failed exit is observed");
    assert_eq!(
        decision,
        RestartDecision::RestartAfter {
            attempt: 1,
            delay: Duration::from_millis(250),
        }
    );
    assert!(change.is_some());
    assert_eq!(
        session.restart_status(),
        RestartStatus::Scheduled {
            attempt: 1,
            not_before: timestamp(270),
        }
    );
    assert_eq!(session.summary().restart_status(), session.restart_status());

    assert!(
        session
            .transition_lifecycle(LifecycleState::Starting, timestamp(21))
            .expect("terminal lifecycle permits restart")
            .is_some()
    );
    assert_eq!(session.restart_status(), RestartStatus::Idle);
}

#[test]
fn no_restart_decision_is_durable_even_when_attempt_count_is_unchanged() {
    let mut session = create_session();
    session
        .transition_lifecycle(LifecycleState::Stopped, timestamp(11))
        .expect("unstarted session becomes terminal")
        .expect("terminal lifecycle changes");
    let (decision, change) = session
        .observe_process_outcome(
            ProcessOutcome::Exited(0),
            Duration::from_secs(1),
            timestamp(11),
        )
        .expect("successful exit is observed");

    assert_eq!(
        decision,
        RestartDecision::DoNotRestart(NoRestartReason::PolicyDisabled)
    );
    assert!(change.is_some());
    assert_eq!(
        session.restart_status(),
        RestartStatus::NotScheduled {
            reason: NoRestartReason::PolicyDisabled,
        }
    );
    assert_eq!(session.summary().restart_status(), session.restart_status());
}

#[test]
fn complete_session_state_round_trips_without_losing_private_or_detailed_state() {
    let session = running_session_with_state();
    let state = session.state();

    let restored = Session::rehydrate(state.clone()).expect("valid state rehydrates");

    assert_eq!(restored, session);
    assert_eq!(restored.state(), state);
    let debug = format!("{:?}", restored.state());
    assert!(!debug.contains("secret-value"));
    assert!(!debug.contains("sleep 10"));
}

#[test]
fn lifecycle_revision_changes_only_with_authoritative_lifecycle() {
    let mut session = create_session();
    assert_eq!(session.lifecycle_revision(), Revision::new(1));

    session
        .rename(session_name("renamed"), timestamp(11))
        .expect("rename succeeds")
        .expect("rename changes aggregate");
    assert_eq!(session.revision(), Revision::new(2));
    assert_eq!(session.lifecycle_revision(), Revision::new(1));

    record_test_process(&mut session, "process-lifecycle", timestamp(12));
    assert_eq!(session.revision(), Revision::new(3));
    assert_eq!(session.lifecycle_revision(), Revision::new(1));

    session
        .transition_lifecycle(LifecycleState::Running, timestamp(12))
        .expect("running transition succeeds")
        .expect("running changes lifecycle");
    assert_eq!(session.lifecycle_revision(), Revision::new(4));
    assert_eq!(
        session.summary().lifecycle_revision(),
        session.lifecycle_revision()
    );
}

#[test]
fn session_rehydration_rejects_cross_field_corruption() {
    let state = running_session_with_state().state();

    let mut zero_revision = state.clone();
    zero_revision.revision = Revision::INITIAL;
    assert!(matches!(
        Session::rehydrate(zero_revision),
        Err(DomainError::CorruptState {
            scope: "session",
            ..
        })
    ));

    let mut zero_lifecycle_revision = state.clone();
    zero_lifecycle_revision.lifecycle_revision = Revision::INITIAL;
    assert!(matches!(
        Session::rehydrate(zero_lifecycle_revision),
        Err(DomainError::CorruptState { .. })
    ));

    let mut future_lifecycle_revision = state.clone();
    future_lifecycle_revision.lifecycle_revision = Revision::new(state.revision.value() + 1);
    assert!(matches!(
        Session::rehydrate(future_lifecycle_revision),
        Err(DomainError::CorruptState { .. })
    ));

    let mut missing_process = state.clone();
    missing_process.process = None;
    assert!(matches!(
        Session::rehydrate(missing_process),
        Err(DomainError::CorruptState { .. })
    ));

    let mut terminal_with_process = state.clone();
    terminal_with_process.lifecycle = LifecycleState::Stopped;
    assert!(matches!(
        Session::rehydrate(terminal_with_process),
        Err(DomainError::CorruptState { .. })
    ));

    let mut mismatched_foreground = state.clone();
    mismatched_foreground.foreground = ForegroundObservation::unknown(state.updated_at);
    assert!(matches!(
        Session::rehydrate(mismatched_foreground),
        Err(DomainError::CorruptState { .. })
    ));

    let mut mismatched_health = state.clone();
    mismatched_health.health =
        HealthSummary::new(HealthStatus::Unknown, 1, None).expect("independently valid health");
    mismatched_health.health_observation =
        HealthObservation::from_summary(mismatched_health.health.clone());
    assert!(matches!(
        Session::rehydrate(mismatched_health),
        Err(DomainError::CorruptState { .. })
    ));

    let mut impossible_restart = state;
    impossible_restart.lifecycle = LifecycleState::Stopped;
    impossible_restart.process = None;
    impossible_restart.restart_tracker = RestartTracker::new(1);
    impossible_restart.restart_status = RestartStatus::Scheduled {
        attempt: 2,
        not_before: timestamp(100),
    };
    assert!(matches!(
        Session::rehydrate(impossible_restart),
        Err(DomainError::CorruptState { .. })
    ));
}

#[test]
fn work_context_rehydration_preserves_revision_and_rejects_impossible_state() {
    let first = work_item("workflow", "first", WorkStatus::Running, Vec::new(), None);
    let second = work_item("workflow", "second", WorkStatus::Waiting, Vec::new(), None);
    let restored = WorkContext::rehydrate(2, Revision::new(4), vec![second.clone(), first.clone()])
        .expect("valid WorkContext rehydrates");

    assert_eq!(restored.revision(), Revision::new(4));
    assert_eq!(
        restored
            .items()
            .map(|item| item.key().clone())
            .collect::<Vec<_>>(),
        vec![first.key().clone(), second.key().clone()]
    );
    assert!(matches!(
        WorkContext::rehydrate(2, Revision::new(2), vec![first.clone(), first.clone()]),
        Err(DomainError::CorruptState {
            scope: "work_context",
            ..
        })
    ));
    assert!(matches!(
        WorkContext::rehydrate(2, Revision::new(1), vec![first.clone(), second.clone()]),
        Err(DomainError::CorruptState { .. })
    ));
    assert!(matches!(
        WorkContext::rehydrate(
            1,
            Revision::new(2),
            vec![
                work_item("workflow", "a", WorkStatus::Running, Vec::new(), None),
                work_item("workflow", "b", WorkStatus::Running, Vec::new(), None),
            ]
        ),
        Err(DomainError::CorruptState { .. })
    ));
}

#[test]
fn live_mutations_reject_state_that_rehydration_would_reject() {
    let mut session = create_session();
    let revision = session.revision();
    let future_process = ProcessIdentity::new(
        ProcessInstanceId::new("future-process").expect("process ID"),
        timestamp(20),
    );

    assert!(matches!(
        session.record_process_ownership(future_process, timestamp(11)),
        Err(DomainError::InvalidValue {
            field: "process_started_at",
            ..
        })
    ));
    assert!(matches!(
        session.transition_lifecycle(LifecycleState::Stopping, timestamp(11)),
        Err(DomainError::LifecyclePrecondition { .. })
    ));
    assert!(matches!(
        session.observe_process_outcome(ProcessOutcome::Exited(1), Duration::ZERO, timestamp(11),),
        Err(DomainError::LifecyclePrecondition { .. })
    ));
    let mismatched_health =
        HealthSummary::new(HealthStatus::Unknown, 1, None).expect("health summary");
    assert!(matches!(
        session.update_health(mismatched_health, timestamp(11)),
        Err(DomainError::InvalidValue {
            field: "health_summary",
            ..
        })
    ));
    let future_work = WorkItem::new(
        work_key("workflow", "future"),
        WorkRelationKind::DependsOn,
        "future work",
        None,
        WorkStatus::Running,
        Vec::new(),
        Metadata::default(),
        timestamp(10),
        timestamp(20),
        None,
    )
    .expect("work item is independently valid");
    assert!(matches!(
        session.upsert_work_item(future_work, timestamp(11)),
        Err(DomainError::InvalidValue {
            field: "work_updated_at",
            ..
        })
    ));
    assert_eq!(session.revision(), revision);
}

#[test]
fn explicit_stop_cancels_only_pending_restart_and_retains_attempt_history() {
    let limits = execution_limits();
    let launch = LaunchSpec::new(
        CommandSpec::new("build", Vec::new(), limits).expect("valid command"),
        EnvironmentVariables::new(Vec::<(String, String)>::new(), limits)
            .expect("valid environment"),
        None,
        None,
        TerminalSize::new(80, 24, 500, 200).expect("valid terminal size"),
    );
    let restart = RestartConfig::new(
        RestartPolicy::Always,
        2,
        Duration::from_millis(250),
        10_000,
        Duration::from_secs(1),
        Duration::from_secs(1),
    )
    .expect("valid restart policy");
    let configuration = SessionConfiguration::new(
        launch,
        ExecutionPolicy::new(
            restart,
            StopPolicy::new(StopSignal::Terminate, Duration::from_secs(1))
                .expect("valid stop policy"),
            true,
            false,
        ),
        HealthRules::new(Vec::new(), limits).expect("valid health rules"),
        None,
        Labels::default(),
        Metadata::default(),
    );
    let (mut session, _) = Session::create(
        session_id("restart-cancellation"),
        session_name("restart-cancellation"),
        configuration,
        1,
        timestamp(10),
    )
    .expect("session creation succeeds");
    record_test_process(&mut session, "process-restart-cancellation", timestamp(11));
    session
        .transition_lifecycle(LifecycleState::Running, timestamp(11))
        .expect("session enters running");
    session
        .transition_lifecycle(LifecycleState::Exited(1), timestamp(20))
        .expect("session records process exit");
    session
        .observe_process_outcome(
            ProcessOutcome::Exited(1),
            Duration::from_millis(10),
            timestamp(20),
        )
        .expect("failed exit schedules restart");
    let attempts = session.restart_tracker();
    let prior_revision = session.revision();

    let change = session
        .cancel_pending_restart(timestamp(21))
        .expect("pending restart cancels")
        .expect("scheduled restart changes");

    assert_eq!(change.kind(), DomainEventKind::RestartCancelled);
    assert_eq!(
        change.revision(),
        prior_revision.next("test").expect("revision")
    );
    assert_eq!(session.restart_tracker(), attempts);
    assert_eq!(
        session.restart_status(),
        RestartStatus::NotScheduled {
            reason: NoRestartReason::ExplicitStop,
        }
    );
    assert!(
        session
            .cancel_pending_restart(timestamp(22))
            .expect("repeated cancellation is valid")
            .is_none()
    );
    assert_eq!(session.revision(), change.revision());
}

#[test]
fn restart_backoff_handles_large_attempt_numbers_in_bounded_work() {
    let config = RestartConfig::new(
        RestartPolicy::Always,
        u32::MAX,
        Duration::from_millis(10),
        10_000,
        Duration::from_secs(1),
        Duration::from_secs(30),
    )
    .expect("restart config is valid");
    let mut tracker = RestartTracker::new(u32::MAX - 1);

    assert_eq!(
        tracker.observe(&config, ProcessOutcome::Exited(1), Duration::from_secs(1),),
        RestartDecision::RestartAfter {
            attempt: u32::MAX,
            delay: Duration::from_millis(10),
        }
    );
}

#[test]
fn work_context_upsert_is_idempotent_and_summary_is_compact() {
    let target = session_id("child-1");
    let item = work_item(
        "workflow",
        "delegation-1",
        WorkStatus::Waiting,
        vec![WorkTarget::Session(target.clone())],
        None,
    );
    let mut context = WorkContext::new(4).expect("positive capacity");

    assert!(context.upsert(item.clone()).expect("first upsert succeeds"));
    let revision = context.revision();
    assert!(!context.upsert(item).expect("identical upsert succeeds"));
    assert_eq!(context.revision(), revision);
    assert_eq!(context.direct_session_targets(), BTreeSet::from([target]));

    let summary = context.summary();
    assert_eq!(summary.item_count, 1);
    assert_eq!(summary.target_count, 1);
    assert_eq!(summary.active_item_count, 1);
    assert_eq!(summary.active_target_count, 1);
    assert_eq!(summary.aggregate_status, Some(WorkStatus::Waiting));
    assert_eq!(summary.revision, revision);
}

#[test]
fn work_context_targets_are_bounded_opaque_and_target_changes_do_not_resolve_declarations() {
    let targets = |count: usize| {
        (0..count)
            .map(|index| WorkTarget::External {
                system: WorkOwner::new("workflow").expect("target system is valid"),
                id: WorkItemId::new(format!("target-{index}")).expect("opaque target ID is valid"),
            })
            .collect::<Vec<_>>()
    };
    let bounded = WorkItem::new(
        work_key("workflow", "bounded-targets"),
        WorkRelationKind::Related,
        "bounded targets",
        None,
        WorkStatus::Waiting,
        targets(64),
        Metadata::default(),
        timestamp(10),
        timestamp(10),
        None,
    )
    .expect("the exact target bound is accepted");
    assert_eq!(bounded.targets().count(), 64);

    let oversized = WorkItem::new(
        work_key("workflow", "oversized-targets"),
        WorkRelationKind::Related,
        "oversized targets",
        None,
        WorkStatus::Waiting,
        targets(65),
        Metadata::default(),
        timestamp(10),
        timestamp(10),
        None,
    )
    .expect_err("one target beyond the hard bound is rejected");
    assert!(matches!(
        oversized,
        DomainError::CapacityExceeded {
            resource: "work_targets",
            limit: 64,
        }
    ));

    let target_id = session_id("opaque-target");
    let declaration = work_item(
        "workflow",
        "waiting-on-target",
        WorkStatus::Waiting,
        vec![WorkTarget::Session(target_id.clone())],
        None,
    );
    let mut context = WorkContext::new(1).expect("work context bound is valid");
    context
        .upsert(declaration)
        .expect("opaque declaration is retained");
    let unchanged = context.clone();

    let (mut target, _) = Session::create(
        target_id,
        session_name("opaque-target"),
        session_configuration(),
        1,
        timestamp(10),
    )
    .expect("target session is valid");
    target
        .transition_lifecycle(LifecycleState::Failed, timestamp(20))
        .expect("target lifecycle can change")
        .expect("target lifecycle changed");

    assert_eq!(context, unchanged);
    assert_eq!(
        context
            .get(&work_key("workflow", "waiting-on-target"))
            .expect("declaration remains present")
            .status(),
        WorkStatus::Waiting
    );
}

#[test]
fn targetless_active_work_remains_visible_and_priority_is_deterministic() {
    let mut context = WorkContext::new(4).expect("positive capacity");
    context
        .upsert(work_item(
            "workflow",
            "running",
            WorkStatus::Running,
            Vec::new(),
            None,
        ))
        .expect("upsert succeeds");
    context
        .upsert(work_item(
            "workflow",
            "blocked",
            WorkStatus::Blocked,
            Vec::new(),
            None,
        ))
        .expect("upsert succeeds");

    let summary = context.summary();
    assert_eq!(summary.active_target_count, 2);
    assert_eq!(summary.aggregate_status, Some(WorkStatus::Blocked));
}

#[test]
fn expiry_is_distinct_and_advances_revision_once_per_batch() {
    let mut context = WorkContext::new(4).expect("positive capacity");
    context
        .upsert(work_item(
            "workflow",
            "expiring",
            WorkStatus::Running,
            Vec::new(),
            Some(timestamp(20)),
        ))
        .expect("upsert succeeds");
    let before = context.revision();

    assert!(
        context
            .expire(timestamp(19))
            .expect("expiry succeeds")
            .is_empty()
    );
    let expired = context.expire(timestamp(20)).expect("expiry succeeds");
    assert_eq!(expired, vec![work_key("workflow", "expiring")]);
    assert_eq!(context.revision().value(), before.value() + 1);
    assert_eq!(
        context
            .get(&work_key("workflow", "expiring"))
            .expect("item remains visible")
            .status(),
        WorkStatus::Expired
    );
}

#[test]
fn session_lifecycle_is_revisioned_and_idempotent() {
    let mut session = create_session();
    assert_eq!(session.revision().value(), 1);
    assert_eq!(session.lifecycle(), LifecycleState::Starting);

    assert!(
        session
            .transition_lifecycle(LifecycleState::Running, timestamp(20))
            .is_err()
    );
    record_test_process(&mut session, "process-session-lifecycle", timestamp(20));
    let owned_revision = session.revision();
    let owned = session.process().expect("process ownership is durable");
    assert_eq!(owned.id().as_str(), "process-session-lifecycle");
    assert_eq!(owned.started_at(), timestamp(20));
    assert!(
        session
            .record_process_ownership(owned.clone(), timestamp(21))
            .expect("exact ownership repeat is valid")
            .is_none()
    );
    assert_eq!(session.revision(), owned_revision);
    assert!(
        session
            .record_process_ownership(
                ProcessIdentity::new(
                    ProcessInstanceId::new("replacement").expect("process ID is valid"),
                    timestamp(21),
                ),
                timestamp(21),
            )
            .is_err()
    );
    let change = session
        .transition_lifecycle(LifecycleState::Running, timestamp(20))
        .expect("valid transition")
        .expect("state changed");
    assert_eq!(change.kind(), DomainEventKind::LifecycleChanged);
    assert_eq!(session.revision().value(), 3);
    assert_eq!(
        session.uptime(timestamp(25)),
        Some(Duration::from_millis(5))
    );

    assert!(
        session
            .transition_lifecycle(LifecycleState::Running, timestamp(30))
            .expect("idempotent transition")
            .is_none()
    );
    assert_eq!(session.revision().value(), 3);
    assert!(
        session
            .transition_lifecycle(LifecycleState::Stopped, timestamp(30))
            .is_err()
    );
    session
        .transition_lifecycle(LifecycleState::Exited(0), timestamp(30))
        .expect("running process may exit")
        .expect("exit changes lifecycle");
    assert!(session.process().is_none());
    assert!(session.started_at().is_none());
}

#[test]
fn terminal_resize_is_revisioned_idempotent_and_used_by_restarts() {
    let mut session = create_session();
    let original_revision = session.revision();
    let original_size = session.terminal_size();
    assert!(
        session
            .resize_terminal(original_size, timestamp(11))
            .expect("identical resize is valid")
            .is_none()
    );
    assert_eq!(session.revision(), original_revision);

    let resized = TerminalSize::new(132, 40, 500, 200).expect("valid terminal size");
    let change = session
        .resize_terminal(resized, timestamp(12))
        .expect("resize succeeds")
        .expect("dimensions changed");
    assert_eq!(change.kind(), DomainEventKind::TerminalResized);
    assert_eq!(session.revision(), Revision::new(2));
    assert_eq!(session.terminal_size(), resized);
    assert_eq!(session.configuration().launch().terminal_size(), resized);

    let revision = session.revision();
    assert!(
        session
            .resize_terminal(original_size, timestamp(9))
            .is_err()
    );
    assert_eq!(session.revision(), revision);
    assert_eq!(session.terminal_size(), resized);
}

#[test]
fn work_context_never_rewrites_foreground_observation() {
    let mut session = create_session();
    let basis = ObservationBasis::new(
        Confidence::from_basis_points(8_500).expect("valid confidence"),
        Some(EvidenceSource::new("profile:shell").expect("valid source")),
        EvidenceDisposition::Inference,
        timestamp(20),
        Some(timestamp(20)),
    )
    .expect("valid basis");
    let observed_wait = ForegroundObservation::new(
        ExecutionObservation::new(ExecutionState::Quiescent, basis.clone())
            .expect("valid execution"),
        InteractionObservation::new(
            InteractionState::Available,
            Some(InteractionModality::Text),
            Some(ObservationSemanticId::new("shell.command").expect("valid semantic")),
            basis,
        )
        .expect("valid interaction"),
        RuntimeConstraintObservation::clear(timestamp(20)),
    );
    session
        .update_foreground(observed_wait.clone())
        .expect("foreground update succeeds");

    let work = work_item(
        "workflow",
        "background-1",
        WorkStatus::Blocked,
        Vec::new(),
        None,
    );
    session
        .upsert_work_item(work, timestamp(21))
        .expect("work update succeeds");

    assert_eq!(session.foreground(), &observed_wait);
    assert_eq!(
        session.summary().work().aggregate_status,
        Some(WorkStatus::Blocked)
    );
}

#[test]
fn work_context_retains_bounded_explicit_provenance() {
    let item = WorkItem::new_with_provenance(
        work_key("workflow", "background-1"),
        WorkRelationKind::Related,
        "background work",
        None,
        WorkStatus::Running,
        Vec::new(),
        WorkProvenance::new("example://workflow/tasks/background-1").expect("provenance is valid"),
        Metadata::default(),
        timestamp(10),
        timestamp(10),
        None,
    )
    .expect("work item is valid");

    assert_eq!(
        item.provenance().as_str(),
        "example://workflow/tasks/background-1"
    );
    assert!(WorkProvenance::new("").is_err());
    assert!(WorkProvenance::new("invalid\nprovenance").is_err());
}

#[test]
fn work_context_provenance_identifies_declaring_product_without_credentials_or_private_payloads() {
    const CREDENTIAL_CANARY: &str = "credential-canary-must-not-enter-provenance";
    const PRIVATE_PAYLOAD_CANARY: &str = "private-payload-must-not-enter-provenance";
    let item = WorkItem::new(
        work_key("workflow", "private-declaration"),
        WorkRelationKind::Related,
        "private declaration",
        Some(PRIVATE_PAYLOAD_CANARY.to_owned()),
        WorkStatus::Running,
        Vec::new(),
        Metadata::new([
            ("credential_handle".to_owned(), CREDENTIAL_CANARY.to_owned()),
            (
                "private_payload".to_owned(),
                PRIVATE_PAYLOAD_CANARY.to_owned(),
            ),
        ])
        .expect("bounded declaration metadata is valid"),
        timestamp(10),
        timestamp(10),
        None,
    )
    .expect("declaration is valid");

    assert_eq!(item.key().owner().as_str(), "workflow");
    assert_eq!(item.provenance().as_str(), "workflow");
    assert_eq!(item.description(), Some(PRIVATE_PAYLOAD_CANARY));
    let provenance = format!("{:?}", item.provenance());
    assert!(!provenance.contains(CREDENTIAL_CANARY));
    assert!(!provenance.contains(PRIVATE_PAYLOAD_CANARY));
}

#[test]
fn removal_is_prepared_only_for_terminal_sessions_without_mutating_the_aggregate() {
    let mut session = create_session();
    let active_error = session
        .prepare_removal(timestamp(11))
        .expect_err("starting session cannot be removed");
    assert_eq!(active_error.category(), ErrorCategory::FailedPrecondition);

    session
        .transition_lifecycle(LifecycleState::Failed, timestamp(11))
        .expect("failure transition succeeds");
    let prior_revision = session.revision();
    let removal = session
        .prepare_removal(timestamp(12))
        .expect("terminal session can be removed");
    assert_eq!(removal.kind(), DomainEventKind::SessionRemoved);
    assert_eq!(
        removal.revision(),
        Revision::new(prior_revision.value() + 1)
    );
    assert_eq!(session.revision(), prior_revision);
    assert_eq!(session.lifecycle(), LifecycleState::Failed);
}

#[test]
fn session_query_matches_prepared_summary_without_detail_access() {
    let session = create_session();
    let summary = session.summary();
    let mut filter = SessionFilter {
        name_contains: Some(NameContains::new("worker").expect("valid name filter")),
        profile: Some(ProfileName::new("agent").expect("valid profile")),
        labels: Labels::new([("env".to_owned(), "test".to_owned())]).expect("valid filter labels"),
        has_active_work: Some(false),
        ..SessionFilter::default()
    };
    filter.lifecycles.insert(LifecycleKind::Starting);
    filter.executions.insert(ExecutionState::Unknown);
    filter.interactions.insert(InteractionState::None);
    filter.health.insert(HealthStatus::NoCheck);

    assert!(filter.matches(&summary));
    filter.ids.insert(session_id("another-session"));
    assert!(!filter.matches(&summary));

    filter.ids.clear();
    filter.name = Some(SessionName::new("another-worker").expect("exact name"));
    assert!(!filter.matches(&summary));
}

#[test]
fn page_size_uses_the_configured_hard_limit() {
    assert!(PageSize::new(1, 0).is_err());
    assert!(PageSize::new(0, 100).is_err());
    assert_eq!(PageSize::new(25, 100).expect("valid page").get(), 25);
    let error = PageSize::new(101, 100).expect_err("limit must be enforced");
    assert_eq!(error.category(), ErrorCategory::CapacityExceeded);
}

#[test]
fn query_name_filter_is_bounded() {
    assert!(NameContains::new("").is_err());
    assert!(NameContains::new("worker").is_ok());
    assert!(NameContains::new("x".repeat(129)).is_err());
}

#[test]
fn stale_updates_are_rejected_without_advancing_session_revision() {
    let mut session = create_session();
    let initial_revision = session.revision();
    assert!(
        session
            .rename(session_name("new-name"), timestamp(9))
            .is_err()
    );
    assert_eq!(session.revision(), initial_revision);
    assert_eq!(session.name().as_str(), "worker-1");

    session
        .upsert_work_item(
            work_item(
                "workflow",
                "delegation-1",
                WorkStatus::Running,
                Vec::new(),
                None,
            ),
            timestamp(11),
        )
        .expect("initial work update succeeds");
    let work_revision = session.revision();
    let stale_replacement = WorkItem::new(
        work_key("workflow", "delegation-1"),
        WorkRelationKind::DependsOn,
        "stale replacement",
        None,
        WorkStatus::Blocked,
        Vec::new(),
        Metadata::default(),
        timestamp(9),
        timestamp(9),
        None,
    )
    .expect("replacement is internally valid");
    assert!(
        session
            .upsert_work_item(stale_replacement, timestamp(12))
            .is_err()
    );
    assert_eq!(session.revision(), work_revision);
    assert_eq!(
        session
            .work_context()
            .get(&work_key("workflow", "delegation-1"))
            .expect("original remains")
            .status(),
        WorkStatus::Running
    );
}

#[test]
fn domain_event_names_are_unique_and_stable() {
    let names: BTreeSet<_> = DomainEventKind::ALL
        .iter()
        .copied()
        .map(DomainEventKind::as_str)
        .collect();
    assert_eq!(names.len(), DomainEventKind::ALL.len());
    assert!(names.contains("work_context_changed"));
    assert!(names.contains("lifecycle_changed"));
    for kind in DomainEventKind::ALL {
        assert_eq!(DomainEventKind::from_name(kind.as_str()), Some(kind));
    }
    assert_eq!(DomainEventKind::from_name("not_an_event"), None);
}

#[test]
fn health_rule_kind_names_are_complete_unique_and_stable() {
    let names = HealthRuleKind::ALL
        .into_iter()
        .map(HealthRuleKind::as_str)
        .collect::<BTreeSet<_>>();

    assert_eq!(names.len(), HealthRuleKind::ALL.len());
    assert!(names.contains("tcp_port"));
    assert!(names.contains("red_cell_threshold"));
    assert!(names.contains("command"));
}

#[test]
fn only_observation_input_changes_invalidate_observation_plans() {
    let invalidating: BTreeSet<_> = DomainEventKind::ALL
        .into_iter()
        .filter(|kind| kind.invalidates_observation_plan())
        .collect();

    assert_eq!(
        invalidating,
        BTreeSet::from([
            DomainEventKind::SessionCreated,
            DomainEventKind::SessionConfigurationChanged,
            DomainEventKind::LifecycleChanged,
            DomainEventKind::ProcessOwnershipChanged,
        ])
    );
}

#[test]
fn timestamps_saturate_reversed_durations_at_zero() {
    assert_eq!(
        timestamp(10).saturating_duration_since(timestamp(20)),
        Duration::ZERO
    );
}

#[test]
fn restoration_retains_only_changed_group_directory_observations() {
    let directory = PathReference::new("/projects/current", 4096).unwrap();
    let mut ordinary = create_session();
    let revision = ordinary.revision();
    assert!(
        ordinary
            .retain_restoration_directory(Some(&directory), timestamp(11))
            .unwrap()
            .is_none()
    );
    assert_eq!(ordinary.revision(), revision);
    assert!(ordinary.restoration_directory().is_none());

    let config = ordinary.configuration();
    let config = SessionConfiguration::new(
        config.launch().clone(),
        config.execution().clone(),
        config.health_rules().clone(),
        config.profile().cloned(),
        Labels::new([("uc.group.slot".into(), "slot-1".into())]).unwrap(),
        config.metadata().clone(),
    );
    ordinary
        .update_configuration(config, timestamp(12))
        .unwrap();
    let change = ordinary
        .retain_restoration_directory(Some(&directory), timestamp(13))
        .unwrap()
        .unwrap();
    assert!(!change.kind().invalidates_observation_plan());
    assert_eq!(ordinary.restoration_directory(), Some(&directory));
    let saved_revision = ordinary.revision();
    assert!(
        ordinary
            .retain_restoration_directory(Some(&directory), timestamp(14))
            .unwrap()
            .is_none()
    );
    assert!(
        ordinary
            .retain_restoration_directory(None, timestamp(15))
            .unwrap()
            .is_none()
    );
    assert_eq!(ordinary.revision(), saved_revision);
    let restored = Session::rehydrate(ordinary.state()).unwrap();
    assert_eq!(restored.restoration_directory(), Some(&directory));
    assert_ne!(
        restored.configuration().launch().working_directory(),
        Some(&directory)
    );
}
