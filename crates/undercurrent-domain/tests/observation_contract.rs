// SPDX-License-Identifier: Apache-2.0

use std::any::TypeId;
use std::time::Duration;

use undercurrent_domain::*;

#[test]
fn lifecycle_execution_interaction_constraint_and_health_are_distinct_typed_concepts() {
    let axes = [
        ("lifecycle", TypeId::of::<LifecycleState>()),
        ("execution", TypeId::of::<ExecutionState>()),
        ("interaction", TypeId::of::<InteractionState>()),
        ("runtime_constraint", TypeId::of::<RuntimeConstraintState>()),
        ("health", TypeId::of::<HealthStatus>()),
    ];

    for (index, (left_name, left)) in axes.iter().enumerate() {
        for (right_name, right) in &axes[index + 1..] {
            assert_ne!(
                left, right,
                "{left_name} and {right_name} must remain separate domain types"
            );
        }
    }
}

#[test]
fn requested_interaction_requires_affirmative_profile_semantics() {
    assert!(InteractionClaim::new(InteractionState::Requested, None, None).is_err());
    let requested = interaction_claim(
        InteractionState::Requested,
        InteractionModality::Options,
        "codex.permission",
    );
    assert!(
        evidence(
            "profile:codex",
            EvidencePriority::Terminal,
            EvidenceDisposition::Inference,
            ForegroundClaim::Interaction(requested),
            9_000,
            1,
        )
        .is_err()
    );
}

#[test]
fn foreground_axes_are_selected_independently() {
    let decision = policy(8, 100, 1_000)
        .decide(
            vec![
                claim(
                    "terminal-motion",
                    EvidencePriority::TerminalHistory,
                    EvidenceDisposition::Affirmative,
                    ForegroundClaim::Execution(ExecutionState::Active),
                    9_500,
                    99,
                ),
                claim(
                    "profile:codex",
                    EvidencePriority::Terminal,
                    EvidenceDisposition::Affirmative,
                    ForegroundClaim::Interaction(interaction_claim(
                        InteractionState::Requested,
                        InteractionModality::Options,
                        "codex.permission",
                    )),
                    8_000,
                    100,
                ),
                claim(
                    "profile:codex",
                    EvidencePriority::Terminal,
                    EvidenceDisposition::Affirmative,
                    ForegroundClaim::Constraint(constraint_claim("codex.rate_limit")),
                    8_500,
                    100,
                ),
            ],
            TimestampMillis::new(100),
        )
        .expect("decision succeeds");

    assert_eq!(
        decision.observation().execution().state(),
        ExecutionState::Active
    );
    assert_eq!(
        decision.observation().interaction().state(),
        InteractionState::Requested
    );
    assert!(decision.observation().interaction().is_actionable());
    assert_eq!(
        decision.observation().constraint().state(),
        RuntimeConstraintState::Present
    );
    assert!(
        decision
            .evidence()
            .iter()
            .all(|item| item.assessment() == EvidenceAssessment::Selected)
    );
}

#[test]
fn precedence_and_confidence_apply_within_each_axis_only() {
    let decision = policy(8, 100, 1_000)
        .decide(
            vec![
                claim(
                    "motion",
                    EvidencePriority::TerminalHistory,
                    EvidenceDisposition::Affirmative,
                    ForegroundClaim::Execution(ExecutionState::Active),
                    9_500,
                    95,
                ),
                claim(
                    "cooperative",
                    EvidencePriority::Cooperative,
                    EvidenceDisposition::Affirmative,
                    ForegroundClaim::Execution(ExecutionState::Quiescent),
                    8_000,
                    96,
                ),
                claim(
                    "profile:shell",
                    EvidencePriority::Terminal,
                    EvidenceDisposition::Inference,
                    ForegroundClaim::Interaction(interaction_claim(
                        InteractionState::Available,
                        InteractionModality::Text,
                        "shell.command",
                    )),
                    7_000,
                    97,
                ),
            ],
            TimestampMillis::new(100),
        )
        .expect("decision succeeds");

    assert_eq!(
        decision.observation().execution().state(),
        ExecutionState::Quiescent
    );
    assert_eq!(
        decision.observation().interaction().state(),
        InteractionState::Available
    );
    assert!(!decision.observation().interaction().is_actionable());
    assert_eq!(
        decision
            .evidence()
            .iter()
            .map(AssessedForegroundEvidence::assessment)
            .collect::<Vec<_>>(),
        vec![
            EvidenceAssessment::Ignored(IgnoredEvidenceReason::LowerPrecedence),
            EvidenceAssessment::Selected,
            EvidenceAssessment::Selected,
        ]
    );
}

#[test]
fn stale_future_unavailable_and_weak_evidence_keep_safe_axis_defaults() {
    let decision = policy(8, 10, 5_000)
        .decide(
            vec![
                claim(
                    "stale",
                    EvidencePriority::Cooperative,
                    EvidenceDisposition::Affirmative,
                    ForegroundClaim::Execution(ExecutionState::Active),
                    9_000,
                    80,
                ),
                claim(
                    "future",
                    EvidencePriority::Terminal,
                    EvidenceDisposition::Affirmative,
                    ForegroundClaim::Execution(ExecutionState::Active),
                    9_000,
                    101,
                ),
                ForegroundEvidence::unavailable(
                    source("unavailable"),
                    EvidencePriority::ForegroundProcess,
                    ForegroundAxis::Interaction,
                    TimestampMillis::new(99),
                ),
                claim(
                    "weak",
                    EvidencePriority::Inference,
                    EvidenceDisposition::Inference,
                    ForegroundClaim::Execution(ExecutionState::Quiescent),
                    4_999,
                    99,
                ),
            ],
            TimestampMillis::new(100),
        )
        .expect("decision succeeds");

    assert_eq!(
        decision.observation(),
        &ForegroundObservation::unknown(TimestampMillis::new(100))
    );
    assert_eq!(
        decision
            .evidence()
            .iter()
            .map(AssessedForegroundEvidence::assessment)
            .collect::<Vec<_>>(),
        vec![
            EvidenceAssessment::Ignored(IgnoredEvidenceReason::Stale),
            EvidenceAssessment::Ignored(IgnoredEvidenceReason::Future),
            EvidenceAssessment::Ignored(IgnoredEvidenceReason::Unavailable),
            EvidenceAssessment::Ignored(IgnoredEvidenceReason::BelowConfidence),
        ]
    );
}

#[test]
fn source_identity_is_unique_per_axis_and_evidence_is_bounded() {
    let duplicate_axis = vec![
        claim(
            "same-source",
            EvidencePriority::Terminal,
            EvidenceDisposition::Affirmative,
            ForegroundClaim::Execution(ExecutionState::Active),
            8_000,
            1,
        ),
        claim(
            "same-source",
            EvidencePriority::Terminal,
            EvidenceDisposition::Inference,
            ForegroundClaim::Execution(ExecutionState::Quiescent),
            7_000,
            1,
        ),
    ];
    assert!(matches!(
        policy(2, 10, 0).decide(duplicate_axis, TimestampMillis::new(1)),
        Err(DomainError::InvalidValue {
            field: "foreground_evidence_source",
            ..
        })
    ));

    let too_many = vec![
        ForegroundEvidence::unavailable(
            source("one"),
            EvidencePriority::Inference,
            ForegroundAxis::Execution,
            TimestampMillis::new(1),
        ),
        ForegroundEvidence::unavailable(
            source("two"),
            EvidencePriority::Inference,
            ForegroundAxis::Interaction,
            TimestampMillis::new(1),
        ),
    ];
    assert!(matches!(
        policy(1, 10, 0).decide(too_many, TimestampMillis::new(1)),
        Err(DomainError::CapacityExceeded {
            resource: "foreground_evidence_items",
            limit: 1
        })
    ));
}

#[test]
fn foreground_decision_rehydration_validates_selected_axis_rationale() {
    let decision = policy(4, 100, 1_000)
        .decide(
            vec![claim(
                "motion",
                EvidencePriority::TerminalHistory,
                EvidenceDisposition::Affirmative,
                ForegroundClaim::Execution(ExecutionState::Active),
                9_000,
                90,
            )],
            TimestampMillis::new(100),
        )
        .expect("decision succeeds");
    let persisted = decision
        .evidence()
        .iter()
        .map(|item| (item.evidence().clone(), item.assessment()))
        .collect::<Vec<_>>();

    assert_eq!(
        ForegroundDecision::rehydrate(decision.observation().clone(), persisted.clone())
            .expect("valid rationale rehydrates"),
        decision
    );
    assert!(matches!(
        ForegroundDecision::rehydrate(
            ForegroundObservation::unknown(TimestampMillis::new(100)),
            persisted,
        ),
        Err(DomainError::CorruptState {
            scope: "foreground_decision",
            ..
        })
    ));
}

#[test]
fn health_observation_distinguishes_partial_failure_and_complete_success() {
    let partial = HealthObservation::evaluate(
        2,
        vec![health_result(0, true, "first")],
        TimestampMillis::new(10),
    )
    .expect("partial observation is valid");
    assert_eq!(partial.summary().status(), HealthStatus::Unknown);

    let failed = HealthObservation::evaluate(
        2,
        vec![
            health_result(0, true, "first"),
            health_result(1, false, "private failure detail"),
        ],
        TimestampMillis::new(11),
    )
    .expect("failed observation is valid");
    assert_eq!(failed.summary().status(), HealthStatus::Unhealthy);
    assert!(!format!("{failed:?}").contains("private failure detail"));

    let healthy = HealthObservation::evaluate(
        2,
        vec![
            health_result(1, true, "second"),
            health_result(0, true, "first"),
        ],
        TimestampMillis::new(12),
    )
    .expect("healthy observation is valid");
    assert_eq!(healthy.summary().status(), HealthStatus::Healthy);
    assert_eq!(healthy.results()[0].rule_index(), 0);
    assert_eq!(healthy.results()[1].rule_index(), 1);
}

#[test]
fn health_observation_rehydration_recomputes_and_validates_summary() {
    let observation = HealthObservation::evaluate(
        2,
        vec![
            health_result(1, true, "second"),
            health_result(0, true, "first"),
        ],
        TimestampMillis::new(20),
    )
    .expect("health observation");

    let restored = HealthObservation::rehydrate(
        observation.summary().clone(),
        observation.results().iter().cloned().rev().collect(),
    )
    .expect("valid health state rehydrates");

    assert_eq!(restored, observation);
    let mismatched = HealthSummary::new(HealthStatus::Unhealthy, 2, Some(TimestampMillis::new(20)))
        .expect("independently valid summary");
    assert!(matches!(
        HealthObservation::rehydrate(mismatched, observation.results().to_vec()),
        Err(DomainError::CorruptState {
            scope: "health_observation",
            ..
        })
    ));
}

#[test]
fn health_observation_rejects_duplicate_or_unconfigured_results() {
    assert!(
        HealthObservation::evaluate(
            1,
            vec![health_result(0, true, "one"), health_result(0, true, "two")],
            TimestampMillis::new(1),
        )
        .is_err()
    );
    assert!(
        HealthObservation::evaluate(
            0,
            vec![health_result(0, true, "one")],
            TimestampMillis::new(1),
        )
        .is_err()
    );
}

#[test]
fn session_persists_foreground_evidence_only_changes_with_distinct_events() {
    let mut session = session_with_health_rule();
    let first = policy(2, 10, 1)
        .decide(
            vec![claim(
                "motion",
                EvidencePriority::TerminalHistory,
                EvidenceDisposition::Affirmative,
                ForegroundClaim::Execution(ExecutionState::Active),
                8_000,
                2,
            )],
            TimestampMillis::new(3),
        )
        .expect("decision succeeds");
    let observation = first.observation().clone();
    let change = session
        .update_foreground_observation(first)
        .expect("foreground update succeeds")
        .expect("foreground changed");
    assert_eq!(change.kind(), DomainEventKind::ForegroundChanged);

    let evidence_only = policy(2, 1_000, 0)
        .decide(
            vec![
                claim(
                    "motion",
                    EvidencePriority::TerminalHistory,
                    EvidenceDisposition::Affirmative,
                    ForegroundClaim::Execution(ExecutionState::Active),
                    8_000,
                    2,
                ),
                ForegroundEvidence::unavailable(
                    source("process"),
                    EvidencePriority::Process,
                    ForegroundAxis::Interaction,
                    TimestampMillis::new(2),
                ),
            ],
            observation.committed_at(),
        )
        .expect("decision succeeds");
    assert_eq!(evidence_only.observation(), &observation);
    let change = session
        .update_foreground_observation(evidence_only)
        .expect("evidence update succeeds")
        .expect("evidence changed");
    assert_eq!(change.kind(), DomainEventKind::ForegroundEvidenceChanged);

    let first_health = HealthObservation::evaluate(
        1,
        vec![health_result(0, true, "first detail")],
        TimestampMillis::new(4),
    )
    .expect("health observation is valid");
    let health_summary = first_health.summary().clone();
    assert_eq!(
        session
            .update_health_observation(first_health, TimestampMillis::new(4))
            .expect("health update succeeds")
            .expect("health changed")
            .kind(),
        DomainEventKind::HealthChanged
    );
    let second_health = HealthObservation::evaluate(
        1,
        vec![health_result(0, true, "second detail")],
        TimestampMillis::new(4),
    )
    .expect("health observation is valid");
    assert_eq!(second_health.summary(), &health_summary);
    assert_eq!(
        session
            .update_health_observation(second_health, TimestampMillis::new(4))
            .expect("health evidence update succeeds")
            .expect("health evidence changed")
            .kind(),
        DomainEventKind::HealthEvidenceChanged
    );
}

fn source(value: &str) -> EvidenceSource {
    EvidenceSource::new(value).expect("evidence source is valid")
}

fn confidence(value: u16) -> Confidence {
    Confidence::from_basis_points(value).expect("confidence is valid")
}

fn policy(items: usize, age_millis: u64, minimum_confidence: u16) -> ForegroundDecisionPolicy {
    ForegroundDecisionPolicy::new(
        items,
        Duration::from_millis(age_millis),
        confidence(minimum_confidence),
    )
    .expect("policy is valid")
}

fn evidence(
    source_name: &str,
    priority: EvidencePriority,
    disposition: EvidenceDisposition,
    claim: ForegroundClaim,
    confidence_basis_points: u16,
    observed_at: i64,
) -> DomainResult<ForegroundEvidence> {
    ForegroundEvidence::new(
        source(source_name),
        priority,
        disposition,
        claim,
        confidence(confidence_basis_points),
        TimestampMillis::new(observed_at),
    )
}

fn claim(
    source_name: &str,
    priority: EvidencePriority,
    disposition: EvidenceDisposition,
    claim: ForegroundClaim,
    confidence_basis_points: u16,
    observed_at: i64,
) -> ForegroundEvidence {
    evidence(
        source_name,
        priority,
        disposition,
        claim,
        confidence_basis_points,
        observed_at,
    )
    .expect("foreground evidence is valid")
}

fn interaction_claim(
    state: InteractionState,
    modality: InteractionModality,
    semantic: &str,
) -> InteractionClaim {
    InteractionClaim::new(
        state,
        Some(modality),
        Some(ObservationSemanticId::new(semantic).expect("semantic is valid")),
    )
    .expect("interaction claim is valid")
}

fn constraint_claim(semantic: &str) -> RuntimeConstraintClaim {
    RuntimeConstraintClaim::new(
        RuntimeConstraintState::Present,
        Some(ObservationSemanticId::new(semantic).expect("semantic is valid")),
    )
    .expect("constraint claim is valid")
}

fn health_result(index: u16, passed: bool, detail: &str) -> HealthCheckResult {
    HealthCheckResult::new(
        index,
        passed,
        Duration::from_millis(5),
        Some(detail.to_owned()),
        1_024,
    )
    .expect("health result is valid")
}

fn session_with_health_rule() -> Session {
    let limits = ExecutionLimits {
        max_arguments: 4,
        max_command_bytes: 128,
        max_environment_entries: 4,
        max_environment_bytes: 128,
        max_path_bytes: 128,
        max_health_rules: 4,
        max_health_rule_bytes: 128,
    };
    Session::create(
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
            HealthRules::new(
                [HealthRule::tcp_port(80).expect("health rule is valid")],
                limits,
            )
            .expect("health rules are valid"),
            None,
            Labels::default(),
            Metadata::default(),
        ),
        4,
        TimestampMillis::new(1),
    )
    .expect("session is valid")
    .0
}
