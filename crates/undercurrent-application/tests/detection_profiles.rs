// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::num::NonZeroU16;
use std::pin::pin;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use undercurrent_application::*;
use undercurrent_domain::*;

#[test]
fn authorization_precedes_every_catalog_read() {
    let catalog = TestCatalog::new(vec![profile_entry("codex", false, vec![], vec![], vec![])]);
    let authorizer = TestAuthorizer::new(AccessDecision::Deny(AccessDenialReason::Unauthenticated));
    let telemetry = TestTelemetry::default();
    let patterns = TestPatterns;
    let clock = TestClock::default();
    let service = service(&authorizer, &catalog, &patterns, &clock, &telemetry, 300);

    let error = block_on(service.list()).expect_err("unauthenticated list is denied");

    assert_eq!(error.category(), ApplicationErrorCategory::Unauthenticated);
    assert_eq!(catalog.reads.load(Ordering::SeqCst), 0);
    assert_eq!(authorizer.calls.load(Ordering::SeqCst), 1);
    assert!(telemetry.has_event(DiagnosticEventName::AuthorizationDenied));
}

#[test]
fn list_and_inspect_return_resolved_bounded_profiles() {
    let catalog = TestCatalog::new(vec![
        profile_entry("zsh", false, vec!["zsh"], vec![], vec![]),
        profile_entry("codex", true, vec!["codex"], vec![], vec![]),
    ]);
    let authorizer = TestAuthorizer::permit();
    let telemetry = TestTelemetry::default();
    let patterns = TestPatterns;
    let clock = TestClock::default();
    let service = service(&authorizer, &catalog, &patterns, &clock, &telemetry, 300);

    let summaries = block_on(service.list()).expect("list succeeds");
    let inspected = block_on(service.inspect(&profile_name("codex"))).expect("inspect succeeds");

    assert_eq!(
        summaries
            .iter()
            .map(|summary| summary.name().as_str())
            .collect::<Vec<_>>(),
        vec!["codex", "zsh"]
    );
    assert!(summaries[0].disabled());
    assert_eq!(inspected.profile().name().as_str(), "codex");
    assert_eq!(inspected.source(), ProfileSourceKind::BuiltIn);
}

#[test]
fn matching_uses_explicit_all_of_dimensions_and_reports_ambiguity() {
    let catalog = TestCatalog::new(vec![
        profile_entry(
            "agent-a",
            false,
            vec!["agent*"],
            vec!["contains:run"],
            vec!["CI"],
        ),
        profile_entry(
            "agent-b",
            false,
            vec!["agent?"],
            vec!["contains:run"],
            vec!["CI"],
        ),
        profile_entry("disabled", true, vec!["agent*"], vec![], vec![]),
        profile_entry("wrong-env", false, vec!["agent*"], vec![], vec!["MISSING"]),
    ]);
    let authorizer = TestAuthorizer::permit();
    let telemetry = TestTelemetry::default();
    let patterns = TestPatterns;
    let clock = TestClock::default();
    let service = service(&authorizer, &catalog, &patterns, &clock, &telemetry, 300);
    let target = match_target("/usr/bin/agent1", &["run", "build"], &["CI"]);

    let outcome = block_on(service.match_profile(&target)).expect("match succeeds");

    assert_eq!(
        outcome,
        ProfileMatchOutcome::Ambiguous {
            profiles: vec![profile_name("agent-a"), profile_name("agent-b")]
        }
    );
}

#[test]
fn application_match_supersedes_runtime_fallback_on_the_same_process() {
    let fallback = ProfileCatalogEntry::new(
        Arc::new(
            build_profile(
                "python",
                false,
                vec!["python*"],
                vec![],
                vec![],
                vec![],
                vec![],
            )
            .with_matching_fallback(true),
        ),
        ProfileSourceKind::BuiltIn,
    );
    let catalog = TestCatalog::new(vec![
        fallback,
        profile_entry(
            "wrapped-agent",
            false,
            vec!["python*"],
            vec!["contains:agent.py"],
            vec![],
        ),
    ]);
    let selector = DetectionProfileSelector::new(&catalog, &TestPatterns, 32).unwrap();
    assert_eq!(
        selector
            .select(&match_target("python3", &["agent.py"], &[]))
            .unwrap(),
        ProfileMatchOutcome::Matched {
            profile: profile_name("wrapped-agent"),
            reason: ProfileMatchReason::BinaryAndArguments
        }
    );
    assert_eq!(
        selector.select(&match_target("python3", &[], &[])).unwrap(),
        ProfileMatchOutcome::Matched {
            profile: profile_name("python"),
            reason: ProfileMatchReason::Binary
        }
    );
}

#[test]
fn grouped_command_signatures_do_not_match_accidental_cross_pairs() {
    let catalog = TestCatalog::new(vec![profile_entry_with_matching(
        "dev-server",
        ProfileMatchRules::from_signatures(
            vec![
                match_signature(vec!["next"], vec!["contains:dev"], vec![]),
                match_signature(vec!["nest"], vec!["contains:start"], vec![]),
            ],
            profile_limits(),
        )
        .expect("grouped match rules are valid"),
    )]);
    let authorizer = TestAuthorizer::permit();
    let telemetry = TestTelemetry::default();
    let patterns = TestPatterns;
    let clock = TestClock::default();
    let service = service(&authorizer, &catalog, &patterns, &clock, &telemetry, 300);

    for (program, argument) in [("next", "dev"), ("nest", "start")] {
        assert_eq!(
            block_on(service.match_profile(&match_target(program, &[argument], &[])))
                .expect("intended signature matches"),
            ProfileMatchOutcome::Matched {
                profile: profile_name("dev-server"),
                reason: ProfileMatchReason::BinaryAndArguments,
            }
        );
    }
    for (program, argument) in [("next", "start"), ("nest", "dev")] {
        assert_eq!(
            block_on(service.match_profile(&match_target(program, &[argument], &[])))
                .expect("cross-pair evaluation succeeds"),
            ProfileMatchOutcome::NoMatch
        );
    }
}

#[test]
fn matching_signatures_collapse_to_one_profile_with_the_strongest_reason() {
    let catalog = TestCatalog::new(vec![profile_entry_with_matching(
        "next",
        ProfileMatchRules::from_signatures(
            vec![
                match_signature(vec![], vec!["contains:dev"], vec![]),
                match_signature(vec!["next"], vec!["contains:dev"], vec![]),
                match_signature(vec!["next"], vec![], vec![]),
            ],
            profile_limits(),
        )
        .expect("grouped match rules are valid"),
    )]);
    let authorizer = TestAuthorizer::permit();
    let telemetry = TestTelemetry::default();
    let patterns = TestPatterns;
    let clock = TestClock::default();
    let service = service(&authorizer, &catalog, &patterns, &clock, &telemetry, 300);

    let outcome = block_on(service.match_profile(&match_target("next", &["dev"], &[])))
        .expect("matching alternatives collapse deterministically");

    assert_eq!(
        outcome,
        ProfileMatchOutcome::Matched {
            profile: profile_name("next"),
            reason: ProfileMatchReason::BinaryAndArguments,
        }
    );
}

#[test]
fn internal_selector_reuses_matching_without_a_profile_read_authorization() {
    let catalog = TestCatalog::new(vec![profile_entry(
        "codex",
        false,
        vec!["codex"],
        vec![],
        vec![],
    )]);
    let patterns = TestPatterns;
    let selector =
        DetectionProfileSelector::new(&catalog, &patterns, 300).expect("selector is valid");

    let outcome = selector
        .select(&match_target("/usr/bin/codex", &[], &[]))
        .expect("selection succeeds");

    assert_eq!(
        outcome,
        ProfileMatchOutcome::Matched {
            profile: profile_name("codex"),
            reason: ProfileMatchReason::Binary,
        }
    );
    assert_eq!(catalog.reads.load(Ordering::SeqCst), 1);
    assert_eq!(catalog.last_all_limit.load(Ordering::SeqCst), 300);
}

#[test]
fn matching_rejects_invalid_active_pattern_as_catalog_invariant() {
    let catalog = TestCatalog::new(vec![profile_entry(
        "broken",
        false,
        vec!["broken"],
        vec!["invalid:(["],
        vec![],
    )]);
    let authorizer = TestAuthorizer::permit();
    let telemetry = TestTelemetry::default();
    let patterns = TestPatterns;
    let clock = TestClock::default();
    let service = service(&authorizer, &catalog, &patterns, &clock, &telemetry, 300);

    let error = block_on(service.match_profile(&match_target("broken", &["x"], &[])))
        .expect_err("invalid active expression is rejected");

    assert_eq!(
        error.category(),
        ApplicationErrorCategory::InvariantViolation
    );
    let ProfileServiceError::InvalidActiveProfile {
        profile,
        location,
        error,
    } = error
    else {
        panic!("expected invalid active profile");
    };
    assert_eq!(profile.as_str(), "broken");
    assert_eq!(location, ProfileIssueLocation::MatchArgument { index: 0 });
    assert_eq!(error.kind(), ProfilePatternErrorKind::InvalidSyntax);
}

#[test]
fn grouped_matching_reports_the_exact_invalid_signature_argument() {
    let catalog = TestCatalog::new(vec![profile_entry_with_matching(
        "broken",
        ProfileMatchRules::from_signatures(
            vec![
                match_signature(vec!["other"], vec!["invalid:first"], vec!["MISSING"]),
                match_signature(vec!["broken"], vec!["invalid:second"], vec![]),
            ],
            profile_limits(),
        )
        .expect("grouped match rules are structurally valid"),
    )]);
    let authorizer = TestAuthorizer::permit();
    let telemetry = TestTelemetry::default();
    let patterns = TestPatterns;
    let clock = TestClock::default();
    let service = service(&authorizer, &catalog, &patterns, &clock, &telemetry, 300);

    let error = block_on(service.match_profile(&match_target("broken", &["dev"], &[])))
        .expect_err("invalid matching signature is rejected");

    let ProfileServiceError::InvalidActiveProfile { location, .. } = error else {
        panic!("expected invalid active profile");
    };
    assert_eq!(location, ProfileIssueLocation::MatchArgument { index: 1 });
}

#[test]
fn lint_returns_typed_errors_and_nonblocking_selection_warnings() {
    let prompt = ForegroundRule::new(
        available_claim("lint.command"),
        vec![pattern("invalid:prompt")],
        vec![],
        PromptTraits::default(),
        confidence(8_000),
        None,
        profile_limits(),
    )
    .expect("structurally valid prompt");
    let profile = profile_with_rules(
        "lint-me",
        true,
        vec![],
        vec!["contains:dev"],
        vec![prompt],
        vec![],
    );
    let catalog = TestCatalog::new(vec![ProfileCatalogEntry::new(
        Arc::new(profile),
        ProfileSourceKind::User,
    )]);
    let authorizer = TestAuthorizer::permit();
    let telemetry = TestTelemetry::default();
    let patterns = TestPatterns;
    let clock = TestClock::default();
    let service = service(&authorizer, &catalog, &patterns, &clock, &telemetry, 300);

    let report = block_on(service.lint(&profile_name("lint-me"))).expect("lint succeeds");
    let all_reports = block_on(service.lint_all()).expect("bounded lint-all succeeds");

    assert!(!report.is_valid());
    assert_eq!(all_reports, vec![report.clone()]);
    assert_eq!(
        report
            .issues()
            .iter()
            .map(ProfileLintIssue::code)
            .collect::<Vec<_>>(),
        vec![
            ProfileIssueCode::Disabled,
            ProfileIssueCode::ArgumentsWithoutBinary,
            ProfileIssueCode::InvalidPatternSyntax,
        ]
    );
    assert_eq!(
        report.issues()[2].location(),
        &ProfileIssueLocation::PromptPattern { rule: 0, index: 0 }
    );
}

#[test]
fn profile_test_reports_regions_exclusions_background_and_declaration_order_winner() {
    let bottom = ProfileRegion::BottomRows {
        rows: NonZeroU16::new(2).expect("nonzero"),
        anchor: ScreenAnchor::Any,
        columns: None,
    };
    let command = prompt_rule(
        available_claim("test.command"),
        "contains:ready",
        None,
        confidence(9_500),
    );
    let permission = prompt_rule(
        requested_claim("test.permission"),
        "contains:Allow",
        Some(bottom.clone()),
        confidence(7_000),
    );
    let excluded = ForegroundRule::new(
        constraint_claim("test.error"),
        vec![pattern("contains:error")],
        vec![pattern("contains:stale")],
        PromptTraits::default(),
        confidence(10_000),
        None,
        profile_limits(),
    )
    .expect("prompt is valid");
    let background = BackgroundObservationRule::new(
        BackgroundWorkKind::Agent,
        vec![pattern("contains:2 agents")],
        vec![],
        vec![pattern("contains:builder")],
        confidence(9_000),
        Some("agent".into()),
        Some(bottom.clone()),
        profile_limits(),
    )
    .expect("background rule is valid");
    let profile = profile_with_rules(
        "test-me",
        false,
        vec!["test-me"],
        vec![],
        vec![command, permission, excluded],
        vec![background],
    );
    let catalog = TestCatalog::new(vec![ProfileCatalogEntry::new(
        Arc::new(profile),
        ProfileSourceKind::User,
    )]);
    let authorizer = TestAuthorizer::permit();
    let telemetry = TestTelemetry::default();
    let patterns = TestPatterns;
    let clock = TestClock::default();
    let service = service(&authorizer, &catalog, &patterns, &clock, &telemetry, 300);
    let samples = vec![
        ProfileSampleView::new(None, "ready\nstale error", "stale error", true, 1_024)
            .expect("sample is valid"),
        ProfileSampleView::new(
            Some(bottom),
            "Allow this?\n2 agents: builder",
            "2 agents: builder",
            true,
            1_024,
        )
        .expect("sample is valid"),
    ];

    let report =
        block_on(service.test(&profile_name("test-me"), &samples)).expect("profile test succeeds");

    assert_eq!(report.evaluations().len(), 4);
    assert_eq!(
        report.evaluations()[0].disposition(),
        ProfileTestDisposition::Matched
    );
    assert_eq!(
        report.evaluations()[2].disposition(),
        ProfileTestDisposition::Excluded
    );
    assert_eq!(report.evaluations()[2].excluded_by(), Some(0));
    assert_eq!(
        report.evaluations()[3].disposition(),
        ProfileTestDisposition::Matched
    );
    assert_eq!(report.evaluations()[3].matched_name_patterns(), &[0]);
    let selected = report
        .selected_foreground()
        .first()
        .expect("a foreground rule wins");
    assert_eq!(selected.rule, 0);
    assert_eq!(selected.claim, available_claim("test.command"));
}

#[test]
fn prompt_selection_does_not_evaluate_unrelated_background_patterns() {
    let prompt = prompt_rule(
        available_claim("test.command"),
        "contains:ready",
        None,
        confidence(8_000),
    );
    let background = BackgroundObservationRule::new(
        BackgroundWorkKind::Agent,
        vec![pattern("invalid:background")],
        vec![],
        vec![],
        confidence(8_000),
        None,
        None,
        profile_limits(),
    )
    .expect("background rule is structurally valid");
    let profile = profile_with_rules(
        "focused-evaluation",
        false,
        vec![],
        vec![],
        vec![prompt],
        vec![background],
    );
    let sample =
        ProfileSampleView::new(None, "ready", "ready", true, 1_024).expect("sample is valid");
    let evaluator = ProfileRuleEvaluator::new(&TestPatterns);

    let selected = evaluator
        .select_foreground(&profile, std::slice::from_ref(&sample))
        .expect("prompt-only selection ignores background rules")
        .into_iter()
        .next()
        .expect("prompt matches");

    assert_eq!(selected.claim, available_claim("test.command"));
    assert!(matches!(
        evaluator.evaluate(&profile, &[sample]),
        Err(ProfileServiceError::InvalidActiveProfile {
            location: ProfileIssueLocation::BackgroundPattern { .. },
            ..
        })
    ));
}

#[test]
fn profile_test_reports_missing_spatial_view_instead_of_using_wrong_text() {
    let region = ProfileRegion::AboveCursor {
        rows: NonZeroU16::new(1).expect("nonzero"),
        columns: None,
    };
    let profile = profile_with_rules(
        "spatial",
        false,
        vec!["spatial"],
        vec![],
        vec![prompt_rule(
            available_claim("test.command"),
            "contains:ready",
            Some(region),
            confidence(8_000),
        )],
        vec![],
    );
    let catalog = TestCatalog::new(vec![ProfileCatalogEntry::new(
        Arc::new(profile),
        ProfileSourceKind::BuiltIn,
    )]);
    let authorizer = TestAuthorizer::permit();
    let telemetry = TestTelemetry::default();
    let patterns = TestPatterns;
    let clock = TestClock::default();
    let service = service(&authorizer, &catalog, &patterns, &clock, &telemetry, 300);
    let wrong_region =
        ProfileSampleView::new(None, "ready", "ready", true, 1_024).expect("sample is valid");

    let report =
        block_on(service.test(&profile_name("spatial"), &[wrong_region])).expect("test succeeds");

    assert_eq!(report.evaluations().len(), 1);
    assert_eq!(
        report.evaluations()[0].disposition(),
        ProfileTestDisposition::SampleUnavailable
    );
    assert!(report.selected_foreground().is_empty());
}

#[test]
fn three_hundred_profile_match_is_bounded_and_deterministic() {
    let profiles = (0..300)
        .map(|index| {
            let name = format!("tool-{index:03}");
            ProfileCatalogEntry::new(
                Arc::new(build_profile_owned(
                    &name,
                    false,
                    vec![name.clone()],
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                )),
                ProfileSourceKind::BuiltIn,
            )
        })
        .collect();
    let catalog = TestCatalog::new(profiles);
    let authorizer = TestAuthorizer::permit();
    let telemetry = TestTelemetry::default();
    let patterns = TestPatterns;
    let clock = TestClock::default();
    let service = service(&authorizer, &catalog, &patterns, &clock, &telemetry, 300);

    let result = block_on(service.match_profile(&match_target("tool-299", &[], &[])))
        .expect("bounded match succeeds");

    assert_eq!(
        result,
        ProfileMatchOutcome::Matched {
            profile: profile_name("tool-299"),
            reason: ProfileMatchReason::Binary,
        }
    );
    assert_eq!(catalog.last_all_limit.load(Ordering::SeqCst), 300);
}

struct TestCatalog {
    entries: BTreeMap<ProfileName, ProfileCatalogEntry>,
    reads: AtomicUsize,
    last_all_limit: AtomicUsize,
}

impl TestCatalog {
    fn new(entries: Vec<ProfileCatalogEntry>) -> Self {
        Self {
            entries: entries
                .into_iter()
                .map(|entry| (entry.profile().name().clone(), entry))
                .collect(),
            reads: AtomicUsize::new(0),
            last_all_limit: AtomicUsize::new(0),
        }
    }

    fn capacity(&self, limit: usize) -> Result<(), PortError> {
        if self.entries.len() <= limit {
            Ok(())
        } else {
            Err(port_error(PortErrorKind::CapacityExceeded))
        }
    }
}

impl DetectionProfileCatalog for TestCatalog {
    fn list(&self, limit: usize) -> Result<Vec<ProfileSummary>, PortError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.capacity(limit)?;
        Ok(self
            .entries
            .values()
            .map(ProfileSummary::from_entry)
            .collect())
    }

    fn get(&self, name: &ProfileName) -> Result<Option<ProfileCatalogEntry>, PortError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(self.entries.get(name).cloned())
    }

    fn all(&self, limit: usize) -> Result<Vec<ProfileCatalogEntry>, PortError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.last_all_limit.store(limit, Ordering::SeqCst);
        self.capacity(limit)?;
        Ok(self.entries.values().cloned().collect())
    }
}

struct TestPatterns;

impl ProfilePatternEngine for TestPatterns {
    fn validate(&self, expression: &PatternExpression) -> Result<(), ProfilePatternError> {
        if expression.as_str().starts_with("invalid:") {
            Err(ProfilePatternError::new(
                ProfilePatternErrorKind::InvalidSyntax,
                Some(8),
            ))
        } else {
            Ok(())
        }
    }

    fn is_match(
        &self,
        expression: &PatternExpression,
        text: &str,
    ) -> Result<bool, ProfilePatternError> {
        self.validate(expression)?;
        let needle = expression
            .as_str()
            .strip_prefix("contains:")
            .unwrap_or(expression.as_str());
        Ok(text.contains(needle))
    }
}

struct TestAuthorizer {
    decision: AccessDecision,
    calls: AtomicUsize,
}

impl TestAuthorizer {
    fn new(decision: AccessDecision) -> Self {
        Self {
            decision,
            calls: AtomicUsize::new(0),
        }
    }

    fn permit() -> Self {
        Self::new(AccessDecision::Permit)
    }
}

impl AuthorizationPort for TestAuthorizer {
    fn authorize<'a>(
        &'a self,
        action: AccessAction,
        resource: &'a AccessResource,
    ) -> PortFuture<'a, Result<AccessDecision, PortError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(action, AccessAction::ReadDetectionProfiles);
        assert_eq!(resource, &AccessResource::Configuration);
        Box::pin(async move { Ok(self.decision) })
    }
}

#[derive(Default)]
struct TestClock(AtomicI64);

impl Clock for TestClock {
    fn now(&self) -> TimestampMillis {
        TimestampMillis::new(self.0.fetch_add(1, Ordering::SeqCst))
    }
}

#[derive(Default)]
struct TestTelemetry {
    diagnostics: Mutex<Vec<DiagnosticRecord>>,
}

impl TestTelemetry {
    fn has_event(&self, event: DiagnosticEventName) -> bool {
        self.diagnostics
            .lock()
            .expect("telemetry lock")
            .iter()
            .any(|record| record.event == event)
    }
}

impl Telemetry for TestTelemetry {
    fn emit_diagnostic(&self, record: &DiagnosticRecord) {
        self.diagnostics
            .lock()
            .expect("telemetry lock")
            .push(record.clone());
    }

    fn record_metric(&self, _observation: MetricObservation) {}

    fn record_session_metric(&self, _observation: &SessionMetricObservation) {}
}

fn service<'a>(
    authorizer: &'a TestAuthorizer,
    catalog: &'a TestCatalog,
    patterns: &'a TestPatterns,
    clock: &'a TestClock,
    telemetry: &'a TestTelemetry,
    max_profiles: usize,
) -> DetectionProfileService<'a> {
    DetectionProfileService::new(
        authorizer,
        catalog,
        patterns,
        clock,
        telemetry,
        ProfileServiceLimits {
            max_profiles,
            max_test_samples: 32,
        },
    )
    .expect("service limits are valid")
}

fn profile_entry(
    name: &str,
    disabled: bool,
    binary: Vec<&str>,
    arguments: Vec<&str>,
    environment: Vec<&str>,
) -> ProfileCatalogEntry {
    ProfileCatalogEntry::new(
        Arc::new(build_profile(
            name,
            disabled,
            binary,
            arguments,
            environment,
            vec![],
            vec![],
        )),
        ProfileSourceKind::BuiltIn,
    )
}

fn profile_entry_with_matching(name: &str, matching: ProfileMatchRules) -> ProfileCatalogEntry {
    ProfileCatalogEntry::new(
        Arc::new(build_profile_with_matching(
            name,
            false,
            matching,
            vec![],
            vec![],
        )),
        ProfileSourceKind::BuiltIn,
    )
}

fn match_signature(
    binary: Vec<&str>,
    arguments: Vec<&str>,
    environment: Vec<&str>,
) -> ProfileMatchSignature {
    ProfileMatchSignature::new(
        binary
            .into_iter()
            .map(|value| BinaryPattern::new(value, 128).expect("binary pattern is valid"))
            .collect(),
        arguments.into_iter().map(pattern).collect(),
        environment
            .into_iter()
            .map(|value| EnvironmentName::new(value, 128).expect("environment name is valid"))
            .collect(),
        profile_limits(),
    )
    .expect("match signature is valid")
}

fn profile_with_rules(
    name: &str,
    disabled: bool,
    binary: Vec<&str>,
    arguments: Vec<&str>,
    prompts: Vec<ForegroundRule>,
    background: Vec<BackgroundObservationRule>,
) -> DetectionProfile {
    build_profile(
        name,
        disabled,
        binary,
        arguments,
        vec![],
        prompts,
        background,
    )
}

fn build_profile(
    name: &str,
    disabled: bool,
    binary: Vec<&str>,
    arguments: Vec<&str>,
    environment: Vec<&str>,
    prompts: Vec<ForegroundRule>,
    background: Vec<BackgroundObservationRule>,
) -> DetectionProfile {
    build_profile_owned(
        name,
        disabled,
        binary.into_iter().map(str::to_owned).collect(),
        arguments.into_iter().map(str::to_owned).collect(),
        environment.into_iter().map(str::to_owned).collect(),
        prompts,
        background,
    )
}

fn build_profile_owned(
    name: &str,
    disabled: bool,
    binary: Vec<String>,
    arguments: Vec<String>,
    environment: Vec<String>,
    prompts: Vec<ForegroundRule>,
    background: Vec<BackgroundObservationRule>,
) -> DetectionProfile {
    let matching = ProfileMatchRules::new(
        binary
            .into_iter()
            .map(|value| BinaryPattern::new(value, 128).expect("binary pattern is valid"))
            .collect(),
        arguments.iter().map(|value| pattern(value)).collect(),
        environment
            .into_iter()
            .map(|value| EnvironmentName::new(value, 128).expect("environment name is valid"))
            .collect(),
        profile_limits(),
    )
    .expect("match rules are valid");
    build_profile_with_matching(name, disabled, matching, prompts, background)
}

fn build_profile_with_matching(
    name: &str,
    disabled: bool,
    matching: ProfileMatchRules,
    prompts: Vec<ForegroundRule>,
    background: Vec<BackgroundObservationRule>,
) -> DetectionProfile {
    DetectionProfile::new(
        profile_name(name),
        Some(format!("{name} profile")),
        disabled,
        matching,
        DetectionArchetype::Unclassified,
        undercurrent_domain::InputDeliveryPolicy::immediate(),
        None,
        prompts,
        background,
        false,
        false,
        OutputExclusions::default(),
        profile_limits(),
    )
    .expect("profile is valid")
}

fn prompt_rule(
    claim: ForegroundClaim,
    expression: &str,
    region: Option<ProfileRegion>,
    confidence: Confidence,
) -> ForegroundRule {
    ForegroundRule::new(
        claim,
        vec![pattern(expression)],
        vec![],
        PromptTraits::default(),
        confidence,
        region,
        profile_limits(),
    )
    .expect("prompt is valid")
}

fn available_claim(semantic: &str) -> ForegroundClaim {
    ForegroundClaim::Interaction(
        InteractionClaim::new(
            InteractionState::Available,
            Some(InteractionModality::Text),
            Some(ObservationSemanticId::new(semantic).expect("semantic is valid")),
        )
        .expect("interaction claim is valid"),
    )
}

fn requested_claim(semantic: &str) -> ForegroundClaim {
    ForegroundClaim::Interaction(
        InteractionClaim::new(
            InteractionState::Requested,
            Some(InteractionModality::Options),
            Some(ObservationSemanticId::new(semantic).expect("semantic is valid")),
        )
        .expect("interaction claim is valid"),
    )
}

fn constraint_claim(semantic: &str) -> ForegroundClaim {
    ForegroundClaim::Constraint(
        RuntimeConstraintClaim::new(
            RuntimeConstraintState::Present,
            Some(ObservationSemanticId::new(semantic).expect("semantic is valid")),
        )
        .expect("constraint claim is valid"),
    )
}

fn match_target(program: &str, arguments: &[&str], environment: &[&str]) -> ProfileMatchTarget {
    ProfileMatchTarget::new(
        program,
        arguments.iter().map(|value| (*value).to_owned()).collect(),
        environment
            .iter()
            .map(|value| EnvironmentName::new(*value, 128).expect("environment name is valid"))
            .collect::<BTreeSet<_>>(),
        4_096,
        64,
        64,
    )
    .expect("match target is valid")
}

fn pattern(value: &str) -> PatternExpression {
    PatternExpression::new(value, profile_limits().max_pattern_bytes).expect("pattern is valid")
}

fn profile_name(value: &str) -> ProfileName {
    ProfileName::new(value).expect("profile name is valid")
}

fn confidence(value: u16) -> Confidence {
    Confidence::from_basis_points(value).expect("confidence is valid")
}

fn profile_limits() -> DetectionProfileLimits {
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

#[derive(Debug)]
struct TestPortSource;

impl std::fmt::Display for TestPortSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("test source")
    }
}

impl std::error::Error for TestPortSource {}

fn port_error(kind: PortErrorKind) -> PortError {
    PortError::new(
        kind,
        PortOperation::ReadDetectionProfiles,
        None,
        TestPortSource,
    )
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct NoopWake;
    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
