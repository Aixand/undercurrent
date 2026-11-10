// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
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
fn foreground_collection_runs_without_a_mutation_lease_and_commits_typed_evidence() {
    let harness = Harness::new(true);

    let outcome = block_on(harness.foreground_service().execute(&harness.session_id))
        .expect("foreground observation succeeds");

    assert!(matches!(
        outcome,
        ObservationOutcome::Committed {
            event: DomainEventKind::ForegroundChanged,
            ..
        }
    ));
    assert_eq!(harness.collector.active_leases_seen(), vec![0]);
    let session = harness.repository.session();
    assert_eq!(
        session.foreground().execution().state(),
        ExecutionState::Active
    );
    assert_eq!(session.foreground_decision().evidence().len(), 1);
    assert_eq!(harness.repository.commit_calls(), 1);
    assert_eq!(harness.projection.upserts(), 1);
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::ForegroundObservationCommitted)
    );
    assert!(
        harness
            .telemetry
            .has_metric(MetricName::DetectionDurationSeconds)
    );
}

#[test]
fn concurrent_session_change_discards_foreground_observation_without_commit() {
    let harness = Harness::new(true);
    harness
        .collector
        .mutate_durable_session
        .store(1, Ordering::SeqCst);

    let outcome = block_on(harness.foreground_service().execute(&harness.session_id))
        .expect("stale observation is an expected result");

    assert!(matches!(
        outcome,
        ObservationOutcome::Stale {
            reason: StaleObservationReason::SessionChanged,
            ..
        }
    ));
    assert_eq!(harness.repository.commit_calls(), 0);
    assert_eq!(
        harness
            .repository
            .session()
            .foreground()
            .execution()
            .state(),
        ExecutionState::Unknown
    );
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::ForegroundObservationStale)
    );
}

#[test]
fn superseded_scheduled_observations_skip_process_and_evidence_io() {
    let harness = Harness::new(true);
    let expected_revision = Revision::INITIAL;

    let foreground = block_on(
        harness
            .foreground_service()
            .execute_scheduled(&harness.session_id, expected_revision),
    )
    .expect("superseded foreground job is an expected result");
    let health = block_on(
        harness
            .health_service()
            .execute_scheduled(&harness.session_id, expected_revision),
    )
    .expect("superseded health job is an expected result");

    assert!(matches!(
        foreground,
        ObservationOutcome::Stale {
            reason: StaleObservationReason::SessionChanged,
            sampled_revision,
            ..
        } if sampled_revision == expected_revision
    ));
    assert!(matches!(
        health,
        ObservationOutcome::Stale {
            reason: StaleObservationReason::SessionChanged,
            sampled_revision,
            ..
        } if sampled_revision == expected_revision
    ));
    assert_eq!(harness.processes.lookup_calls(), 0);
    assert!(harness.collector.active_leases_seen().is_empty());
    assert_eq!(harness.health.calls(), 0);
}

#[test]
fn health_checks_run_without_a_mutation_lease_and_commit_ordered_detail() {
    let harness = Harness::new(true);

    let outcome = block_on(harness.health_service().execute(&harness.session_id))
        .expect("health observation succeeds");

    assert!(matches!(
        outcome,
        ObservationOutcome::Committed {
            event: DomainEventKind::HealthChanged,
            ..
        }
    ));
    assert_eq!(harness.health.active_leases_seen(), vec![0]);
    let session = harness.repository.session();
    assert_eq!(session.health().status(), HealthStatus::Healthy);
    assert_eq!(session.health_observation().results()[0].rule_index(), 0);
    assert!(
        harness
            .telemetry
            .has_metric(MetricName::HealthCheckDurationSeconds)
    );
    assert_eq!(
        harness
            .telemetry
            .metric_values(MetricName::HealthCheckResults),
        vec![1.0]
    );
    assert_eq!(
        harness
            .telemetry
            .metric_values(MetricName::HealthCheckResultsPassed),
        vec![1.0]
    );
    assert_eq!(
        harness
            .telemetry
            .metric_values(MetricName::HealthCheckResultsFailed),
        vec![0.0]
    );
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::HealthObservationCommitted)
    );
}

#[test]
fn unconfigured_health_skips_process_and_health_adapters() {
    let harness = Harness::new(false);

    let outcome = block_on(harness.health_service().execute(&harness.session_id))
        .expect("unconfigured health is skipped");

    assert_eq!(
        outcome,
        ObservationOutcome::Skipped(ObservationSkipReason::NoHealthRules)
    );
    assert_eq!(harness.processes.lookup_calls(), 0);
    assert_eq!(harness.health.calls(), 0);
}

#[test]
fn expected_observation_absence_and_uncertainty_never_emit_infrastructure_errors() {
    let absent = Harness::new(false);
    let skipped = block_on(absent.health_service().execute(&absent.session_id))
        .expect("expected absence is a typed skip");
    assert_eq!(
        skipped,
        ObservationOutcome::Skipped(ObservationSkipReason::NoHealthRules)
    );
    assert_expected_non_error_diagnostics(
        "no health rules",
        &absent.telemetry,
        &[(DiagnosticEventName::HealthObservationSkipped, None)],
    );

    let unavailable = Harness::new(true);
    unavailable.foreground_processes.replace(observation(
        "agent",
        Vec::new(),
        None,
        TcpListenerObservation::Absent,
        "/workspace/project",
    ));
    unavailable.collector.unavailable.store(1, Ordering::SeqCst);
    block_on(
        unavailable
            .foreground_service()
            .execute(&unavailable.session_id),
    )
    .expect("unavailable evidence is a successful typed observation");
    assert_expected_non_error_diagnostics(
        "unavailable foreground evidence",
        &unavailable.telemetry,
        &[
            (DiagnosticEventName::ForegroundProcessObserved, None),
            (DiagnosticEventName::ForegroundEvidenceCollected, None),
            (DiagnosticEventName::ProjectionUpdated, None),
            (DiagnosticEventName::ForegroundObservationCommitted, None),
        ],
    );

    let partial = Harness::new(true);
    partial.health.partial.store(1, Ordering::SeqCst);
    block_on(partial.health_service().execute(&partial.session_id))
        .expect("partial health evidence is a successful typed observation");
    assert_eq!(
        partial.repository.session().health().status(),
        HealthStatus::Unknown
    );
    assert_expected_non_error_diagnostics(
        "partial health evidence",
        &partial.telemetry,
        &[
            (DiagnosticEventName::HealthChecksCompleted, None),
            (DiagnosticEventName::ProjectionUpdated, None),
            (DiagnosticEventName::HealthObservationCommitted, None),
        ],
    );
}

fn assert_expected_non_error_diagnostics(
    case: &str,
    telemetry: &TestTelemetry,
    expected: &[(DiagnosticEventName, Option<ApplicationErrorCategory>)],
) {
    let diagnostics = telemetry
        .diagnostics
        .lock()
        .expect("diagnostic lock is available");
    let actual = diagnostics
        .iter()
        .map(|record| (record.event, record.application_error))
        .collect::<Vec<_>>();
    assert_eq!(
        actual, expected,
        "{case} must retain its exact non-infrastructure category mapping"
    );
    assert!(
        diagnostics
            .iter()
            .all(|record| record.level != DiagnosticLevel::Error),
        "{case} must never be logged at Error"
    );
    assert!(
        !diagnostics.iter().any(|record| matches!(
            record.event,
            DiagnosticEventName::PortOperationFailed
                | DiagnosticEventName::ForegroundObservationFailed
                | DiagnosticEventName::HealthObservationFailed
        )),
        "{case} must not emit infrastructure-failure telemetry"
    );
}

#[test]
fn foreground_adapter_failure_is_redacted_typed_and_observable() {
    let harness = Harness::new(true);
    harness.collector.fail.store(1, Ordering::SeqCst);

    let error = block_on(harness.foreground_service().execute(&harness.session_id))
        .expect_err("collector failure is returned");

    assert_eq!(error.category(), ApplicationErrorCategory::Unavailable);
    assert!(!error.to_string().contains("private-adapter-detail"));
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::ForegroundObservationFailed)
    );
    let diagnostic = harness
        .telemetry
        .diagnostics
        .lock()
        .expect("diagnostic lock is available")
        .iter()
        .find(|record| record.event == DiagnosticEventName::PortOperationFailed)
        .cloned()
        .expect("typed port failure diagnostic is emitted");
    assert_eq!(diagnostic.level, DiagnosticLevel::Warn);
    assert_eq!(
        diagnostic.application_error,
        Some(ApplicationErrorCategory::Unavailable)
    );
}

#[test]
fn adaptive_profile_tracks_foreground_program_and_shell_ready_without_durable_profile_mutation() {
    let harness = Harness::new(true);
    harness.enable_adaptive_profile();
    harness.profiles.replace(vec![test_profile(
        "codex",
        "codex",
        DetectionArchetype::AnimatedAgent,
    )]);
    harness.foreground_processes.replace(observation(
        "codex",
        Vec::new(),
        None,
        TcpListenerObservation::Absent,
        "/workspace/project",
    ));

    block_on(harness.foreground_service().execute(&harness.session_id))
        .expect("adaptive observation");
    let process = ProcessInstanceId::new("process-1").expect("process ID");
    let selected =
        EffectiveProfileDirectory::get(&harness.effective_profiles, &harness.session_id, &process)
            .expect("effective profile")
            .expect("prepared profile");
    assert_eq!(
        selected.selection().profile().map(ProfileName::as_str),
        Some("codex")
    );
    assert_eq!(
        selected.current_directory().map(PathReference::as_str),
        Some("/workspace/project")
    );
    assert_eq!(
        harness.repository.session().profile().unwrap().as_str(),
        "agent"
    );
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::ForegroundProcessObserved)
    );
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::EffectiveProfileChanged)
    );
    assert!(
        harness
            .telemetry
            .has_metric(MetricName::ForegroundProcessObservationDurationSeconds)
    );
    assert_eq!(
        harness
            .telemetry
            .metric_values(MetricName::EffectiveProfileTransitions),
        vec![1.0]
    );

    harness.foreground_processes.replace(observation(
        "sh",
        Vec::new(),
        Some(CooperativeForegroundState::Ready),
        TcpListenerObservation::Absent,
        "/workspace/next",
    ));
    block_on(harness.foreground_service().execute(&harness.session_id))
        .expect("shell-ready observation");
    let restored =
        EffectiveProfileDirectory::get(&harness.effective_profiles, &harness.session_id, &process)
            .expect("effective profile")
            .expect("prepared profile");
    assert_eq!(
        restored.selection().profile().map(ProfileName::as_str),
        Some("agent")
    );
    assert_eq!(restored.origin(), EffectiveProfileOrigin::CooperativeShell);
    assert_eq!(
        harness.repository.session().profile().unwrap().as_str(),
        "agent"
    );
}

#[test]
fn foreground_fallbacks_resolve_wrappers_without_hiding_application_conflicts() {
    let cases: &[(&[&str], Option<&str>, Option<UnclassifiedProfileReason>)] = &[
        (&["python", "codex"], Some("codex"), None),
        (&["codex", "python"], Some("codex"), None),
        (&["python"], Some("python"), None),
        (&["node"], Some("node"), None),
        (&["python", "codex", "codex"], Some("codex"), None),
        (
            &["python", "codex", "claude"],
            None,
            Some(UnclassifiedProfileReason::Ambiguous),
        ),
        (
            &["python", "node"],
            None,
            Some(UnclassifiedProfileReason::Ambiguous),
        ),
        (&["unknown"], None, Some(UnclassifiedProfileReason::NoMatch)),
    ];
    for (programs, expected, reason) in cases {
        let harness = Harness::new(true);
        harness.enable_adaptive_profile();
        harness.profiles.replace(vec![
            test_profile("python", "python", DetectionArchetype::Unclassified)
                .with_matching_fallback(true),
            test_profile("node", "node", DetectionArchetype::Unclassified)
                .with_matching_fallback(true),
            test_profile("codex", "codex", DetectionArchetype::AnimatedAgent),
            test_profile("claude", "claude", DetectionArchetype::AnimatedAgent),
        ]);
        harness.foreground_processes.replace(group_observation(
            programs,
            false,
            ProcessGroupCompleteness::Complete,
        ));
        block_on(harness.foreground_service().execute(&harness.session_id)).unwrap();
        let selected = EffectiveProfileDirectory::get(
            &harness.effective_profiles,
            &harness.session_id,
            &ProcessInstanceId::new("process-1").unwrap(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            selected.selection().profile().map(ProfileName::as_str),
            *expected,
            "{programs:?}"
        );
        assert_eq!(
            selected.selection().unclassified_reason(),
            *reason,
            "{programs:?}"
        );
        if programs.contains(&"python") && programs.contains(&"codex") {
            let python = selected
                .matching_evidence()
                .iter()
                .find(|e| e.profile().as_str() == "python")
                .unwrap();
            assert!(python.matching_fallback());
            assert_eq!(python.process_incarnations().len(), 1);
            let codex = selected
                .matching_evidence()
                .iter()
                .find(|e| e.profile().as_str() == "codex")
                .unwrap();
            assert!(!codex.matching_fallback());
            assert_eq!(
                codex.process_incarnations().len(),
                programs.iter().filter(|p| **p == "codex").count()
            );
        }
    }
}

#[test]
fn group_selection_resolves_member_local_fallback_ambiguity_after_gathering_all_matches() {
    let harness = Harness::new(true);
    harness.enable_adaptive_profile();
    harness.profiles.replace(vec![
        test_profile("python", "python", DetectionArchetype::Unclassified)
            .with_matching_fallback(true),
        test_profile("python-other", "python", DetectionArchetype::Unclassified)
            .with_matching_fallback(true),
        test_profile("codex", "codex", DetectionArchetype::AnimatedAgent),
    ]);
    harness.foreground_processes.replace(group_observation(
        &["python", "codex"],
        false,
        ProcessGroupCompleteness::Complete,
    ));
    block_on(harness.foreground_service().execute(&harness.session_id)).unwrap();
    let selected = EffectiveProfileDirectory::get(
        &harness.effective_profiles,
        &harness.session_id,
        &ProcessInstanceId::new("process-1").unwrap(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        selected.selection().profile().map(ProfileName::as_str),
        Some("codex")
    );
    assert_eq!(selected.matching_evidence().len(), 3);
}

#[test]
fn possible_application_match_prevents_committing_fallback_ambiguity() {
    let harness = Harness::new(true);
    harness.enable_adaptive_profile();
    harness.profiles.replace(vec![
        test_profile("python", "python", DetectionArchetype::Unclassified)
            .with_matching_fallback(true),
        test_profile("node", "node", DetectionArchetype::Unclassified).with_matching_fallback(true),
        test_profile_with_environment(
            "possible-agent",
            "python",
            "CI",
            DetectionArchetype::AnimatedAgent,
        ),
    ]);
    harness.foreground_processes.replace(group_observation(
        &["python", "node"],
        true,
        ProcessGroupCompleteness::Complete,
    ));
    block_on(harness.foreground_service().execute(&harness.session_id)).unwrap();
    let selected = EffectiveProfileDirectory::get(
        &harness.effective_profiles,
        &harness.session_id,
        &ProcessInstanceId::new("process-1").unwrap(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        selected.selection().unclassified_reason(),
        Some(UnclassifiedProfileReason::IncompleteEvidence)
    );
}

#[test]
fn fallback_resolution_respects_incomplete_evidence_and_fixed_profiles() {
    for (fallback_requirement, completeness, adaptive, expected, reason) in [
        (
            true,
            ProcessGroupCompleteness::Complete,
            true,
            Some("codex"),
            None,
        ),
        (
            false,
            ProcessGroupCompleteness::Complete,
            true,
            None,
            Some(UnclassifiedProfileReason::IncompleteEvidence),
        ),
        (
            true,
            ProcessGroupCompleteness::Truncated,
            true,
            None,
            Some(UnclassifiedProfileReason::IncompleteEvidence),
        ),
        (
            true,
            ProcessGroupCompleteness::Unavailable,
            true,
            None,
            Some(UnclassifiedProfileReason::Unavailable),
        ),
        (
            true,
            ProcessGroupCompleteness::Complete,
            false,
            Some("agent"),
            None,
        ),
    ] {
        let harness = Harness::new(true);
        if adaptive {
            harness.enable_adaptive_profile();
        }
        harness.profiles.replace(vec![
            test_profile("agent", "agent", DetectionArchetype::AnimatedAgent),
            test_profile("python", "python", DetectionArchetype::Unclassified)
                .with_matching_fallback(true),
            test_profile("codex", "codex", DetectionArchetype::AnimatedAgent),
            test_profile_with_environment(
                "possible",
                "python",
                "CI",
                DetectionArchetype::Unclassified,
            )
            .with_matching_fallback(fallback_requirement),
        ]);
        harness.foreground_processes.replace(group_observation(
            &["python", "codex"],
            true,
            completeness,
        ));
        block_on(harness.foreground_service().execute(&harness.session_id)).unwrap();
        let selected = EffectiveProfileDirectory::get(
            &harness.effective_profiles,
            &harness.session_id,
            &ProcessInstanceId::new("process-1").unwrap(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            selected.selection().profile().map(ProfileName::as_str),
            expected
        );
        assert_eq!(selected.selection().unclassified_reason(), reason);
        if !adaptive {
            assert!(selected.matching_evidence().is_empty());
        }
    }
}

fn group_observation(
    programs: &[&str],
    incomplete_environment: bool,
    completeness: ProcessGroupCompleteness,
) -> ForegroundProcessGroupObservation {
    ForegroundProcessGroupObservation::new(
        ProcessInstanceId::new("process-1").unwrap(),
        TimestampMillis::new(10),
        programs
            .iter()
            .enumerate()
            .map(|(index, program)| {
                let target =
                    ProfileMatchTarget::new(*program, Vec::new(), BTreeSet::new(), 4096, 16, 16)
                        .unwrap();
                let member = ForegroundProgramMember::new(format!("{}:100", 42 + index), target);
                if incomplete_environment {
                    member.with_truncated_environment_names()
                } else {
                    member
                }
            })
            .collect(),
        completeness,
        None,
        TcpListenerObservation::Absent,
        None,
    )
}

#[test]
fn adaptive_profile_keeps_binary_match_when_only_environment_names_are_truncated() {
    let harness = Harness::new(true);
    harness.enable_adaptive_profile();
    harness.profiles.replace(vec![test_profile(
        "codex",
        "codex",
        DetectionArchetype::AnimatedAgent,
    )]);
    harness
        .foreground_processes
        .replace(observation_with_truncated_environment("codex"));

    block_on(harness.foreground_service().execute(&harness.session_id))
        .expect("adaptive observation");
    let selected = EffectiveProfileDirectory::get(
        &harness.effective_profiles,
        &harness.session_id,
        &ProcessInstanceId::new("process-1").expect("process ID"),
    )
    .expect("effective profile")
    .expect("prepared profile");

    assert_eq!(
        selected.selection().profile().map(ProfileName::as_str),
        Some("codex")
    );
}

#[test]
fn adaptive_profile_fails_closed_when_truncated_environment_can_change_selection() {
    let harness = Harness::new(true);
    harness.enable_adaptive_profile();
    harness.profiles.replace(vec![
        test_profile("codex", "codex", DetectionArchetype::AnimatedAgent),
        test_profile_with_environment("codex-ci", "codex", "CI", DetectionArchetype::AnimatedAgent),
    ]);
    harness
        .foreground_processes
        .replace(observation_with_truncated_environment("codex"));

    block_on(harness.foreground_service().execute(&harness.session_id))
        .expect("adaptive observation");
    let selected = EffectiveProfileDirectory::get(
        &harness.effective_profiles,
        &harness.session_id,
        &ProcessInstanceId::new("process-1").expect("process ID"),
    )
    .expect("effective profile")
    .expect("prepared profile");

    assert_eq!(
        selected.selection().unclassified_reason(),
        Some(UnclassifiedProfileReason::IncompleteEvidence)
    );
}

#[test]
fn listener_ownership_marks_server_role_without_selecting_a_server_profile() {
    let harness = Harness::new(true);
    harness.enable_adaptive_profile();
    harness.foreground_processes.replace(observation(
        "custom-service",
        Vec::new(),
        None,
        TcpListenerObservation::Present,
        "/srv/app",
    ));

    block_on(harness.foreground_service().execute(&harness.session_id))
        .expect("listener observation");
    let selected = EffectiveProfileDirectory::get(
        &harness.effective_profiles,
        &harness.session_id,
        &ProcessInstanceId::new("process-1").expect("process ID"),
    )
    .expect("effective profile")
    .expect("prepared profile");
    assert_eq!(
        selected.selection().unclassified_reason(),
        Some(UnclassifiedProfileReason::NoMatch)
    );
    assert_eq!(selected.server_role(), Some(ServerRoleOrigin::TcpListener));
    assert!(
        harness
            .telemetry
            .has_event(DiagnosticEventName::EffectiveProfileUnclassified)
    );
    assert_eq!(
        harness
            .telemetry
            .metric_values(MetricName::EffectiveProfileUnclassifiedTransitions),
        vec![1.0]
    );
}

#[test]
fn managed_listener_role_does_not_claim_foreground_membership_or_select_a_profile() {
    for (listener, tree_listener, role) in [
        (
            TcpListenerObservation::Present,
            TcpListenerObservation::Present,
            Some(ServerRoleOrigin::ManagedProcessTcpListener),
        ),
        (
            TcpListenerObservation::Unavailable,
            TcpListenerObservation::Present,
            Some(ServerRoleOrigin::ManagedTreeTcpListener),
        ),
        (
            TcpListenerObservation::Absent,
            TcpListenerObservation::Absent,
            None,
        ),
        (
            TcpListenerObservation::Truncated,
            TcpListenerObservation::Truncated,
            None,
        ),
        (
            TcpListenerObservation::Unavailable,
            TcpListenerObservation::Unavailable,
            None,
        ),
    ] {
        let harness = Harness::new(true);
        harness.enable_adaptive_profile();
        harness.foreground_processes.replace(
            ForegroundProcessGroupObservation::new(
                ProcessInstanceId::new("process-1").unwrap(),
                TimestampMillis::new(1000),
                Vec::new(),
                ProcessGroupCompleteness::Unavailable,
                None,
                TcpListenerObservation::Unavailable,
                None,
            )
            .with_managed_process_tcp_listener(listener)
            .with_managed_tree_tcp_listener(tree_listener),
        );
        block_on(harness.foreground_service().execute(&harness.session_id)).unwrap();
        let selected = EffectiveProfileDirectory::get(
            &harness.effective_profiles,
            &harness.session_id,
            &ProcessInstanceId::new("process-1").unwrap(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(selected.server_role(), role);
        assert_eq!(
            selected.selection().unclassified_reason(),
            Some(UnclassifiedProfileReason::Unavailable)
        );
    }
}

struct Harness {
    session_id: SessionId,
    clock: TestClock,
    coordinator: TestCoordinator,
    repository: Arc<TestRepository>,
    projection: TestProjection,
    effective_profiles: undercurrent_application::PreparedSessionCatalog,
    foreground_processes: TestForegroundProcesses,
    profiles: TestProfiles,
    patterns: TestPatterns,
    processes: TestProcesses,
    collector: TestCollector,
    health: TestHealthExecutor,
    telemetry: TestTelemetry,
}

impl Harness {
    fn new(with_health: bool) -> Self {
        let session = session(with_health);
        let session_id = session.id().clone();
        let active_leases = Arc::new(AtomicUsize::new(0));
        let repository = Arc::new(TestRepository::new(session));
        Self {
            session_id,
            clock: TestClock(AtomicI64::new(10)),
            coordinator: TestCoordinator {
                active: Arc::clone(&active_leases),
            },
            projection: TestProjection::default(),
            effective_profiles: {
                let catalog = undercurrent_application::PreparedSessionCatalog::new();
                catalog
                    .upsert(repository.session().summary())
                    .expect("prepared summary");
                catalog
            },
            foreground_processes: TestForegroundProcesses::default(),
            profiles: TestProfiles::default(),
            patterns: TestPatterns,
            processes: TestProcesses::new(ProcessInstance {
                id: ProcessInstanceId::new("process-1").expect("process ID is valid"),
                started_at: TimestampMillis::new(2),
            }),
            collector: TestCollector {
                active_leases: Arc::clone(&active_leases),
                repository: Arc::clone(&repository),
                observations: Mutex::new(Vec::new()),
                mutate_durable_session: AtomicUsize::new(0),
                unavailable: AtomicUsize::new(0),
                fail: AtomicUsize::new(0),
            },
            health: TestHealthExecutor {
                active_leases,
                observations: Mutex::new(Vec::new()),
                partial: AtomicUsize::new(0),
                calls: AtomicUsize::new(0),
            },
            repository,
            telemetry: TestTelemetry::default(),
        }
    }

    fn foreground_service(&self) -> ObserveForegroundService<'_> {
        ObserveForegroundService::new(
            &self.clock,
            &self.coordinator,
            self.repository.as_ref(),
            &self.projection,
            &self.processes,
            &self.effective_profiles,
            &self.foreground_processes,
            &self.profiles,
            &self.patterns,
            8,
            &self.collector,
            &self.telemetry,
            ForegroundDecisionPolicy::new(
                8,
                Duration::from_secs(10),
                Confidence::from_basis_points(1_000).expect("confidence is valid"),
            )
            .expect("policy is valid"),
        )
    }

    fn health_service(&self) -> ObserveHealthService<'_> {
        ObserveHealthService::new(
            &self.clock,
            &self.coordinator,
            self.repository.as_ref(),
            &self.projection,
            &self.processes,
            &self.health,
            &self.telemetry,
        )
    }

    fn enable_adaptive_profile(&self) {
        let mut session = self.repository.session();
        let mut configuration = session.configuration().clone();
        configuration.set_profile_binding(ProfileBindingMode::ForegroundAdaptive);
        session
            .update_configuration(configuration, TimestampMillis::new(9))
            .expect("adaptive configuration");
        *self.repository.session.lock().expect("session lock") = session.clone();
        self.effective_profiles
            .upsert(session.summary())
            .expect("adaptive prepared summary");
    }
}

fn session(with_health: bool) -> Session {
    let limits = ExecutionLimits {
        max_arguments: 4,
        max_command_bytes: 128,
        max_environment_entries: 4,
        max_environment_bytes: 128,
        max_path_bytes: 128,
        max_health_rules: 4,
        max_health_rule_bytes: 128,
    };
    let health = if with_health {
        HealthRules::new([HealthRule::tcp_port(80).expect("rule is valid")], limits)
            .expect("health rules are valid")
    } else {
        HealthRules::default()
    };
    let mut session = Session::create(
        SessionId::new("session-1").expect("session ID is valid"),
        SessionName::new("worker").expect("session name is valid"),
        SessionConfiguration::new(
            LaunchSpec::new(
                CommandSpec::new("sh", Vec::new(), limits).expect("command is valid"),
                EnvironmentVariables::default(),
                None,
                None,
                TerminalSize::new(80, 24, 500, 500).expect("terminal size is valid"),
            ),
            ExecutionPolicy::new(
                RestartConfig::never(),
                StopPolicy::new(StopSignal::Terminate, Duration::from_secs(1))
                    .expect("stop policy is valid"),
                true,
                false,
            ),
            health,
            Some(ProfileName::new("agent").expect("profile is valid")),
            Labels::default(),
            Metadata::default(),
        ),
        4,
        TimestampMillis::new(1),
    )
    .expect("session is valid")
    .0;
    session
        .record_process_ownership(
            ProcessIdentity::new(
                ProcessInstanceId::new("process-1").expect("process ID is valid"),
                TimestampMillis::new(2),
            ),
            TimestampMillis::new(2),
        )
        .expect("process ownership succeeds");
    session
        .transition_lifecycle(LifecycleState::Running, TimestampMillis::new(2))
        .expect("running transition succeeds");
    session
}

fn observation(
    program: &str,
    arguments: Vec<&str>,
    cooperative: Option<CooperativeForegroundState>,
    tcp_listener: TcpListenerObservation,
    current_directory: &str,
) -> ForegroundProcessGroupObservation {
    let target = ProfileMatchTarget::new(
        program,
        arguments.into_iter().map(str::to_owned).collect(),
        BTreeSet::new(),
        4_096,
        16,
        16,
    )
    .expect("match target");
    ForegroundProcessGroupObservation::new(
        ProcessInstanceId::new("process-1").expect("process ID"),
        TimestampMillis::new(10),
        vec![ForegroundProgramMember::new("42:100", target)],
        ProcessGroupCompleteness::Complete,
        Some(PathReference::new(current_directory, 4_096).expect("directory")),
        tcp_listener,
        cooperative,
    )
}

fn observation_with_truncated_environment(program: &str) -> ForegroundProcessGroupObservation {
    let target = ProfileMatchTarget::new(program, Vec::new(), BTreeSet::new(), 4_096, 16, 16)
        .expect("match target");
    ForegroundProcessGroupObservation::new(
        ProcessInstanceId::new("process-1").expect("process ID"),
        TimestampMillis::new(10),
        vec![ForegroundProgramMember::new("42:100", target).with_truncated_environment_names()],
        ProcessGroupCompleteness::Complete,
        Some(PathReference::new("/workspace/project", 4_096).expect("directory")),
        TcpListenerObservation::Absent,
        None,
    )
}

fn test_profile(name: &str, binary: &str, archetype: DetectionArchetype) -> DetectionProfile {
    test_profile_with_environment(name, binary, "", archetype)
}

fn test_profile_with_environment(
    name: &str,
    binary: &str,
    required_environment: &str,
    archetype: DetectionArchetype,
) -> DetectionProfile {
    let limits = DetectionProfileLimits {
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
        max_input_submission_delay_millis: 1_000,
    };
    DetectionProfile::new(
        ProfileName::new(name).expect("profile name"),
        None,
        false,
        ProfileMatchRules::new(
            vec![BinaryPattern::new(binary, limits.max_pattern_bytes).expect("binary pattern")],
            Vec::new(),
            if required_environment.is_empty() {
                BTreeSet::new()
            } else {
                [
                    EnvironmentName::new(required_environment, limits.max_environment_name_bytes)
                        .expect("environment name"),
                ]
                .into_iter()
                .collect()
            },
            limits,
        )
        .expect("matching rules"),
        archetype,
        InputDeliveryPolicy::immediate(),
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

struct TestClock(AtomicI64);

impl Clock for TestClock {
    fn now(&self) -> TimestampMillis {
        TimestampMillis::new(self.0.fetch_add(1, Ordering::SeqCst))
    }
}

struct TestCoordinator {
    active: Arc<AtomicUsize>,
}

struct TestLease(Arc<AtomicUsize>);

impl Drop for TestLease {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl SessionMutationLease for TestLease {}

impl SessionMutationCoordinator for TestCoordinator {
    fn acquire<'a>(
        &'a self,
        _: &'a SessionId,
    ) -> PortFuture<'a, Result<Box<dyn SessionMutationLease>, PortError>> {
        self.active.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(Box::new(TestLease(Arc::clone(&self.active))) as Box<dyn SessionMutationLease>)
        })
    }
}

struct TestRepository {
    session: Mutex<Session>,
    events: Mutex<Vec<SessionOutboxEvent>>,
    commit_calls: AtomicUsize,
}

impl TestRepository {
    fn new(session: Session) -> Self {
        Self {
            session: Mutex::new(session),
            events: Mutex::new(Vec::new()),
            commit_calls: AtomicUsize::new(0),
        }
    }

    fn session(&self) -> Session {
        self.session
            .lock()
            .expect("session lock is available")
            .clone()
    }

    fn commit_calls(&self) -> usize {
        self.commit_calls.load(Ordering::SeqCst)
    }

    fn rename_directly(&self) {
        let mut session = self.session.lock().expect("session lock is available");
        session
            .rename(
                SessionName::new("changed-worker").expect("name is valid"),
                TimestampMillis::new(3),
            )
            .expect("rename succeeds");
    }
}

impl SessionRepository for TestRepository {
    fn insert<'a>(
        &'a self,
        _: &'a Session,
        _: &'a SessionOutboxEvent,
    ) -> PortFuture<'a, Result<InsertSessionOutcome, PortError>> {
        Box::pin(async { Err(port_error(PortOperation::InsertSession)) })
    }

    fn load<'a>(&'a self, _: &'a SessionId) -> PortFuture<'a, Result<Option<Session>, PortError>> {
        Box::pin(async { Ok(Some(self.session())) })
    }

    fn find_by_name<'a>(
        &'a self,
        _: &'a SessionName,
    ) -> PortFuture<'a, Result<Option<Session>, PortError>> {
        Box::pin(async { Err(port_error(PortOperation::FindSessionByName)) })
    }

    fn commit<'a>(
        &'a self,
        session: &'a Session,
        expected_revision: Revision,
        events: &'a [SessionOutboxEvent],
    ) -> PortFuture<'a, Result<CommitSessionOutcome, PortError>> {
        self.commit_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let mut current = self.session.lock().expect("session lock is available");
            if current.revision() != expected_revision {
                return Ok(CommitSessionOutcome::RevisionConflict {
                    current_revision: current.revision(),
                });
            }
            *current = session.clone();
            self.events
                .lock()
                .expect("event lock is available")
                .extend_from_slice(events);
            Ok(CommitSessionOutcome::Committed)
        })
    }

    fn delete<'a>(
        &'a self,
        _: &'a SessionId,
        _: Revision,
        _: &'a SessionOutboxEvent,
    ) -> PortFuture<'a, Result<DeleteSessionOutcome, PortError>> {
        Box::pin(async { Err(port_error(PortOperation::DeleteSession)) })
    }
}

#[derive(Default)]
struct TestProjection(AtomicUsize);

impl TestProjection {
    fn upserts(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl SessionProjectionWriter for TestProjection {
    fn upsert_session(&self, _: &Session) -> Result<(), PortError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn remove_summary(&self, _: &SessionId) -> Result<(), PortError> {
        Ok(())
    }
}

struct TestProcesses {
    process: Mutex<Option<ProcessInstance>>,
    lookups: AtomicUsize,
}

impl TestProcesses {
    fn new(process: ProcessInstance) -> Self {
        Self {
            process: Mutex::new(Some(process)),
            lookups: AtomicUsize::new(0),
        }
    }

    fn lookup_calls(&self) -> usize {
        self.lookups.load(Ordering::SeqCst)
    }
}

impl SessionProcessDirectory for TestProcesses {
    fn bind(&self, _: &SessionId, process: ProcessInstance) -> Result<(), PortError> {
        *self.process.lock().expect("process lock is available") = Some(process);
        Ok(())
    }

    fn lookup(&self, _: &SessionId) -> Result<Option<ProcessInstance>, PortError> {
        self.lookups.fetch_add(1, Ordering::SeqCst);
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
struct TestForegroundProcesses(Mutex<Option<ForegroundProcessGroupObservation>>);

impl TestForegroundProcesses {
    fn replace(&self, observation: ForegroundProcessGroupObservation) {
        *self.0.lock().expect("foreground process lock") = Some(observation);
    }
}

impl ForegroundProcessObserver for TestForegroundProcesses {
    fn observe_foreground<'a>(
        &'a self,
        session_id: &'a SessionId,
        _: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<ForegroundProcessGroupObservation, PortError>> {
        let observation = self.0.lock().expect("foreground process lock").clone();
        Box::pin(async move {
            observation.ok_or_else(|| {
                PortError::new(
                    PortErrorKind::Unavailable,
                    PortOperation::ObserveForegroundProcessGroup,
                    Some(session_id.clone()),
                    PrivateAdapterError,
                )
            })
        })
    }
}

#[derive(Default)]
struct TestProfiles(Mutex<Vec<ProfileCatalogEntry>>);

impl TestProfiles {
    fn replace(&self, profiles: Vec<DetectionProfile>) {
        *self.0.lock().expect("profile lock") = profiles
            .into_iter()
            .map(|profile| ProfileCatalogEntry::new(Arc::new(profile), ProfileSourceKind::BuiltIn))
            .collect();
    }
}

impl DetectionProfileCatalog for TestProfiles {
    fn list(&self, _: usize) -> Result<Vec<ProfileSummary>, PortError> {
        Ok(self
            .0
            .lock()
            .expect("profile lock")
            .iter()
            .map(ProfileSummary::from_entry)
            .collect())
    }

    fn get(&self, name: &ProfileName) -> Result<Option<ProfileCatalogEntry>, PortError> {
        Ok(self
            .0
            .lock()
            .expect("profile lock")
            .iter()
            .find(|entry| entry.profile().name() == name)
            .cloned())
    }

    fn all(&self, _: usize) -> Result<Vec<ProfileCatalogEntry>, PortError> {
        Ok(self.0.lock().expect("profile lock").clone())
    }
}

struct TestPatterns;

impl ProfilePatternEngine for TestPatterns {
    fn validate(&self, _: &PatternExpression) -> Result<(), ProfilePatternError> {
        Ok(())
    }

    fn is_match(&self, _: &PatternExpression, _: &str) -> Result<bool, ProfilePatternError> {
        Ok(false)
    }
}

struct TestCollector {
    active_leases: Arc<AtomicUsize>,
    repository: Arc<TestRepository>,
    observations: Mutex<Vec<usize>>,
    mutate_durable_session: AtomicUsize,
    unavailable: AtomicUsize,
    fail: AtomicUsize,
}

impl TestCollector {
    fn active_leases_seen(&self) -> Vec<usize> {
        self.observations
            .lock()
            .expect("observation lock is available")
            .clone()
    }
}

impl ForegroundEvidenceCollector for TestCollector {
    fn collect<'a>(
        &'a self,
        _: &'a SessionId,
        _: &'a ProcessInstanceId,
        _: Option<&'a ProfileName>,
        _: &'a ForegroundObservation,
    ) -> PortFuture<'a, Result<Vec<ForegroundEvidence>, PortError>> {
        self.observations
            .lock()
            .expect("observation lock is available")
            .push(self.active_leases.load(Ordering::SeqCst));
        Box::pin(async move {
            if self.fail.swap(0, Ordering::SeqCst) != 0 {
                return Err(port_error(PortOperation::CollectForegroundEvidence));
            }
            if self.unavailable.swap(0, Ordering::SeqCst) != 0 {
                return Ok(vec![ForegroundEvidence::unavailable(
                    EvidenceSource::new("expected-absence").expect("source is valid"),
                    EvidencePriority::Inference,
                    undercurrent_domain::ForegroundAxis::Execution,
                    TimestampMillis::new(10),
                )]);
            }
            if self.mutate_durable_session.swap(0, Ordering::SeqCst) != 0 {
                self.repository.rename_directly();
            }
            Ok(vec![
                ForegroundEvidence::new(
                    EvidenceSource::new("terminal-output").expect("source is valid"),
                    EvidencePriority::Terminal,
                    EvidenceDisposition::Affirmative,
                    ForegroundClaim::Execution(ExecutionState::Active),
                    Confidence::from_basis_points(8_000).expect("confidence is valid"),
                    TimestampMillis::new(10),
                )
                .expect("evidence is valid"),
            ])
        })
    }
}

struct TestHealthExecutor {
    active_leases: Arc<AtomicUsize>,
    observations: Mutex<Vec<usize>>,
    partial: AtomicUsize,
    calls: AtomicUsize,
}

impl TestHealthExecutor {
    fn active_leases_seen(&self) -> Vec<usize> {
        self.observations
            .lock()
            .expect("observation lock is available")
            .clone()
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl HealthCheckExecutor for TestHealthExecutor {
    fn run<'a>(
        &'a self,
        _: &'a SessionId,
        _: &'a ProcessInstanceId,
        _: &'a HealthRules,
    ) -> PortFuture<'a, Result<Vec<HealthCheckResult>, PortError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.observations
            .lock()
            .expect("observation lock is available")
            .push(self.active_leases.load(Ordering::SeqCst));
        if self.partial.swap(0, Ordering::SeqCst) != 0 {
            return Box::pin(async { Ok(Vec::new()) });
        }
        Box::pin(async {
            Ok(vec![
                HealthCheckResult::new(
                    0,
                    true,
                    Duration::from_millis(2),
                    Some("private-health-detail".to_owned()),
                    1_024,
                )
                .expect("health result is valid"),
            ])
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

    fn has_metric(&self, metric: MetricName) -> bool {
        self.metrics
            .lock()
            .expect("metric lock is available")
            .iter()
            .any(|observation| observation.name == metric)
    }

    fn metric_values(&self, metric: MetricName) -> Vec<f64> {
        self.metrics
            .lock()
            .expect("metric lock is available")
            .iter()
            .filter_map(|observation| (observation.name == metric).then_some(observation.value))
            .collect()
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
struct PrivateAdapterError;

impl Display for PrivateAdapterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("private-adapter-detail")
    }
}

impl Error for PrivateAdapterError {}

fn port_error(operation: PortOperation) -> PortError {
    PortError::new(
        PortErrorKind::Unavailable,
        operation,
        None,
        PrivateAdapterError,
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

#[test]
fn group_directory_is_retained_from_the_same_runtime_observation_as_inspect() {
    for grouped in [false, true] {
        let harness = Harness::new(true);
        if grouped {
            let mut session = harness.repository.session.lock().unwrap();
            let config = session.configuration();
            let config = SessionConfiguration::new(
                config.launch().clone(),
                config.execution().clone(),
                config.health_rules().clone(),
                config.profile().cloned(),
                Labels::new([("uc.group.slot".into(), "slot-1".into())]).unwrap(),
                config.metadata().clone(),
            );
            let now = session.updated_at();
            session.update_configuration(config, now).unwrap();
        }
        harness.foreground_processes.replace(observation(
            "bash",
            Vec::new(),
            None,
            TcpListenerObservation::Absent,
            "/workspace/latest",
        ));
        block_on(harness.foreground_service().execute(&harness.session_id)).unwrap();
        let session = harness.repository.session();
        assert_eq!(
            session.restoration_directory().map(PathReference::as_str),
            grouped.then_some("/workspace/latest")
        );
        let process = ProcessInstanceId::new("process-1").unwrap();
        let live = EffectiveProfileDirectory::get(
            &harness.effective_profiles,
            &harness.session_id,
            &process,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            live.current_directory().map(PathReference::as_str),
            Some("/workspace/latest")
        );
    }
}
