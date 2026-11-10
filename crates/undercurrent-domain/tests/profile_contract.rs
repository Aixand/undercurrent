// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::num::NonZeroU16;
use std::time::Duration;

use undercurrent_domain::*;

#[test]
fn binary_wildcards_match_paths_without_recursive_backtracking() {
    let exact = BinaryPattern::new("python?", 64).expect("pattern is valid");
    let wildcard = BinaryPattern::new("cargo-*test", 64).expect("pattern is valid");

    assert!(exact.is_match("python3"));
    assert!(!exact.is_match("python33"));
    assert!(wildcard.is_match("cargo-nextest"));
    assert!(!wildcard.is_match("cargo-nextest-run"));

    let adversarial = BinaryPattern::new(format!("{}z", "*a".repeat(64)), 256)
        .expect("bounded adversarial pattern is valid");
    assert!(!adversarial.is_match(&"a".repeat(256)));
}

#[test]
fn windows_names_match_suffixes_without_changing_literal_matching() {
    for (pattern, program, expected) in [
        ("codex", r"C:\tools\CoDeX.CMD", true),
        ("node", "NODE.EXE", true),
        ("tool", "tool.com", true),
        ("tool", "tool.BAT", true),
        ("tool.cmd", "TOOL.CMD", true),
        ("tool.cmd", "tool.exe", false),
        ("tool", "tool.exe.cmd", false),
        ("tool", "tool.txt", false),
        ("tool", "tool.cmd.other", false),
        ("node*", "NODE-22.EXE", true),
        ("tool", "TOOL", true),
    ] {
        let target =
            ProfileMatchTarget::new(program, vec![], BTreeSet::new(), 1024, 16, 16).unwrap();
        let binary = BinaryPattern::new(pattern, 64).unwrap();
        let rules = ProfileMatchRules::new(vec![binary.clone()], vec![], BTreeSet::new(), limits())
            .unwrap();
        assert_eq!(
            rules.binary_matches(&target),
            binary.is_match(target.basename())
        );
        let windows = target.with_match_convention(ProfileMatchConvention::Windows);
        assert_eq!(
            rules.binary_matches(&windows),
            expected,
            "{pattern} / {program}"
        );
        assert_eq!(rules.signatures()[0].binary_matches(&windows), expected);
        assert_eq!(
            windows.basename(),
            program.rsplit(['/', '\\']).next().unwrap()
        );
    }
}

#[test]
fn windows_environment_names_preserve_all_of_requirements() {
    let names = |names: &[&str]| {
        names
            .iter()
            .map(|name| EnvironmentName::new(*name, 64).unwrap())
            .collect()
    };
    let rules = ProfileMatchRules::new(
        vec![BinaryPattern::new("tool", 64).unwrap()],
        vec![],
        names(&["CI", "TOOL_HOME"]),
        limits(),
    )
    .unwrap();
    let target =
        ProfileMatchTarget::new("tool", vec![], names(&["ci", "Tool_Home"]), 1024, 16, 16).unwrap();
    assert!(!rules.environment_matches(&target));
    let target = target.with_match_convention(ProfileMatchConvention::Windows);
    assert!(rules.environment_matches(&target));
    assert!(rules.signatures()[0].environment_matches(&target));
    let missing = ProfileMatchTarget::new("tool", vec![], names(&["ci"]), 1024, 16, 16)
        .unwrap()
        .with_match_convention(ProfileMatchConvention::Windows);
    assert!(!rules.environment_matches(&missing));
    assert!(!rules.signatures()[0].environment_matches(&missing));
}

#[test]
fn match_target_uses_explicit_environment_names_and_redacts_arguments() {
    let target = ProfileMatchTarget::new(
        "/usr/bin/codex",
        vec!["--token=private-value".into()],
        BTreeSet::from([EnvironmentName::new("CI", 64).expect("environment name is valid")]),
        1_024,
        16,
        16,
    )
    .expect("target is valid");
    let rules = ProfileMatchRules::new(
        vec![BinaryPattern::new("codex", 64).expect("pattern is valid")],
        vec![],
        BTreeSet::from([EnvironmentName::new("CI", 64).expect("environment name is valid")]),
        limits(),
    )
    .expect("rules are valid");

    assert_eq!(target.basename(), "codex");
    assert!(rules.binary_matches(&target));
    assert!(rules.environment_matches(&target));
    assert!(!format!("{target:?}").contains("private-value"));
}

#[test]
fn grouped_match_rules_preserve_signature_boundaries() {
    let ci = EnvironmentName::new("CI", 64).expect("environment name is valid");
    let local = EnvironmentName::new("LOCAL", 64).expect("environment name is valid");
    let signatures = vec![
        ProfileMatchSignature::new(
            vec![BinaryPattern::new("next", 64).expect("pattern is valid")],
            vec![pattern("contains:dev")],
            BTreeSet::from([ci]),
            limits(),
        )
        .expect("signature is valid"),
        ProfileMatchSignature::new(
            vec![BinaryPattern::new("nest", 64).expect("pattern is valid")],
            vec![pattern("contains:start")],
            BTreeSet::from([local]),
            limits(),
        )
        .expect("signature is valid"),
    ];

    let rules = ProfileMatchRules::from_signatures(signatures.clone(), limits())
        .expect("grouped rules are valid");

    assert_eq!(rules.signatures(), signatures);
    assert_eq!(
        rules
            .binary()
            .iter()
            .map(BinaryPattern::as_str)
            .collect::<Vec<_>>(),
        vec!["next", "nest"]
    );
    assert!(rules.required_environment().is_empty());
}

#[test]
fn command_signature_requires_an_automatic_selector() {
    let error = ProfileMatchSignature::new(
        vec![],
        vec![],
        BTreeSet::from([EnvironmentName::new("CI", 64).expect("environment name is valid")]),
        limits(),
    )
    .expect_err("environment alone cannot select a command signature");

    assert_eq!(error.category(), ErrorCategory::InvalidArgument);
    assert!(!ProfileMatchRules::default().is_selectable());
}

#[test]
fn foreground_rules_require_evidence_and_encode_valid_regions() {
    let empty_traits = PromptTraits::default();
    assert!(
        ForegroundRule::new(
            available_claim("shell.command"),
            vec![],
            vec![],
            empty_traits,
            confidence(8_000),
            None,
            limits(),
        )
        .is_err()
    );

    let traits = PromptTraits::new(
        Some(">".into()),
        None,
        vec!["ready".into()],
        vec!["busy".into()],
        Some(true),
        limits(),
    )
    .expect("traits are valid");
    let region = ProfileRegion::BottomRows {
        rows: NonZeroU16::new(3).expect("nonzero"),
        anchor: ScreenAnchor::NearCursor { distance: 2 },
        columns: NonZeroU16::new(120),
    };
    let rule = ForegroundRule::new(
        available_claim("shell.command"),
        vec![],
        vec![],
        traits,
        confidence(8_000),
        Some(region.clone()),
        limits(),
    )
    .expect("trait-only prompt is valid");
    let sample = ProfileSampleView::new(Some(region), "agent ready\n>", ">", true, 1_024)
        .expect("sample is valid");

    assert!(rule.traits().matches(&sample));
}

#[test]
fn detection_profile_owns_observation_semantics_while_domain_only_validates_shape() {
    let profile = DetectionProfile::new(
        ProfileName::new("custom-agent").expect("profile name is valid"),
        None,
        false,
        ProfileMatchRules::default(),
        DetectionArchetype::AnimatedAgent,
        InputDeliveryPolicy::immediate(),
        None,
        vec![
            ForegroundRule::new(
                available_claim("vendor_alpha.prompt"),
                vec![pattern("contains:prompt")],
                vec![],
                PromptTraits::default(),
                confidence(8_000),
                None,
                limits(),
            )
            .expect("interaction rule is valid"),
            ForegroundRule::new(
                constraint_claim("vendor_alpha.rate_limit"),
                vec![pattern("contains:limited")],
                vec![],
                PromptTraits::default(),
                confidence(8_000),
                None,
                limits(),
            )
            .expect("constraint rule is valid"),
        ],
        vec![],
        false,
        false,
        OutputExclusions::default(),
        limits(),
    )
    .expect("profile is valid");

    let ForegroundClaim::Interaction(interaction) = profile.foreground_rules()[0].claim() else {
        panic!("first profile rule must retain its interaction claim");
    };
    assert_eq!(
        interaction
            .semantic()
            .expect("interaction semantic")
            .as_str(),
        "vendor_alpha.prompt"
    );
    let ForegroundClaim::Constraint(constraint) = profile.foreground_rules()[1].claim() else {
        panic!("second profile rule must retain its constraint claim");
    };
    assert_eq!(
        constraint.semantic().expect("constraint semantic").as_str(),
        "vendor_alpha.rate_limit"
    );

    assert!(ObservationSemanticId::new("another_vendor.custom_meaning").is_ok());
    assert!(ObservationSemanticId::new("not_namespaced").is_err());
    assert!(ObservationSemanticId::new("Vendor.prompt").is_err());
}

#[test]
fn background_rules_are_separate_from_foreground_prompt_rules() {
    let background = BackgroundObservationRule::new(
        BackgroundWorkKind::Agent,
        vec![pattern("(?P<count>\\d+) background agents")],
        vec![],
        vec![pattern("(?P<name>build-[a-z]+)")],
        confidence(9_000),
        Some("background agent".into()),
        None,
        limits(),
    )
    .expect("background rule is valid");
    let profile = DetectionProfile::new(
        ProfileName::new("codex").expect("name is valid"),
        Some("Codex terminal foreground".into()),
        false,
        ProfileMatchRules::default(),
        DetectionArchetype::AnimatedAgent,
        undercurrent_domain::InputDeliveryPolicy::immediate(),
        None,
        vec![],
        vec![background],
        false,
        false,
        OutputExclusions {
            top_rows: 0,
            bottom_rows: 2,
        },
        limits(),
    )
    .expect("profile is valid");

    assert!(profile.foreground_rules().is_empty());
    assert_eq!(profile.background_rules().len(), 1);
    assert_eq!(
        profile.background_rules()[0].kind(),
        BackgroundWorkKind::Agent
    );
    assert!(profile.archetype().cpu_is_ambient());
}

#[test]
fn profile_collections_are_hard_bounded() {
    let patterns = (0..=limits().max_binary_patterns)
        .map(|index| BinaryPattern::new(format!("tool-{index}"), 64).expect("pattern is valid"))
        .collect();
    let error = ProfileMatchRules::new(patterns, vec![], BTreeSet::new(), limits())
        .expect_err("collection exceeds limit");

    assert_eq!(error.category(), ErrorCategory::CapacityExceeded);
}

#[test]
fn final_profile_boundary_revalidates_nested_values_against_its_limits() {
    let prompt = ForegroundRule::new(
        available_claim("test.input"),
        vec![PatternExpression::new("12345678", 32).expect("loose expression is valid")],
        vec![],
        PromptTraits::default(),
        confidence(8_000),
        None,
        limits(),
    )
    .expect("rule is valid under loose limits");
    let mut tight = limits();
    tight.max_pattern_bytes = 4;

    let error = DetectionProfile::new(
        ProfileName::new("tight").expect("name is valid"),
        None,
        false,
        ProfileMatchRules::default(),
        DetectionArchetype::Unclassified,
        undercurrent_domain::InputDeliveryPolicy::immediate(),
        None,
        vec![prompt],
        vec![],
        false,
        false,
        OutputExclusions::default(),
        tight,
    )
    .expect_err("final profile limits reject the nested expression");

    assert_eq!(error.category(), ErrorCategory::InvalidArgument);
}

#[test]
fn trailing_submit_pacing_is_nonzero_millisecond_aligned_and_bounded() {
    let maximum = Duration::from_millis(2_000);
    let policy = InputDeliveryPolicy::paced_trailing_submit(Duration::from_millis(120), maximum)
        .expect("policy is valid");

    assert_eq!(
        policy.trailing_submit_delay(),
        Some(Duration::from_millis(120))
    );
    assert!(InputDeliveryPolicy::paced_trailing_submit(Duration::ZERO, maximum).is_err());
    assert!(InputDeliveryPolicy::paced_trailing_submit(Duration::from_nanos(1), maximum).is_err());
    assert!(
        InputDeliveryPolicy::paced_trailing_submit(Duration::from_millis(2_001), maximum).is_err()
    );
}

#[test]
fn profile_boundary_revalidates_its_input_delivery_policy() {
    let policy = InputDeliveryPolicy::paced_trailing_submit(
        Duration::from_millis(120),
        Duration::from_secs(5),
    )
    .expect("policy is valid under a loose limit");
    let mut tight = limits();
    tight.max_input_submission_delay_millis = 100;

    let error = DetectionProfile::new(
        ProfileName::new("paced").expect("name is valid"),
        None,
        false,
        ProfileMatchRules::default(),
        DetectionArchetype::AnimatedAgent,
        policy,
        None,
        vec![],
        vec![],
        true,
        false,
        OutputExclusions::default(),
        tight,
    )
    .expect_err("profile limit rejects the delay");

    assert_eq!(error.category(), ErrorCategory::InvalidArgument);
}

#[test]
fn sample_debug_never_exposes_terminal_text() {
    let sample = ProfileSampleView::new(None, "private terminal payload", "private", false, 128)
        .expect("sample is valid");
    let debug = format!("{sample:?}");

    assert!(!debug.contains("private terminal payload"));
    assert!(debug.contains("text_bytes"));
}

#[test]
fn match_target_rejects_a_path_without_an_executable_basename() {
    let error = ProfileMatchTarget::new("/usr/bin/", vec![], BTreeSet::new(), 1_024, 16, 16)
        .expect_err("directory path is not an executable target");

    assert_eq!(error.category(), ErrorCategory::InvalidArgument);
}

fn pattern(value: &str) -> PatternExpression {
    PatternExpression::new(value, limits().max_pattern_bytes).expect("pattern is valid")
}

fn confidence(value: u16) -> Confidence {
    Confidence::from_basis_points(value).expect("confidence is valid")
}

fn available_claim(semantic: &str) -> ForegroundClaim {
    ForegroundClaim::Interaction(
        InteractionClaim::new(
            InteractionState::Available,
            Some(InteractionModality::Text),
            Some(ObservationSemanticId::new(semantic).expect("semantic is valid")),
        )
        .expect("interaction is valid"),
    )
}

fn constraint_claim(semantic: &str) -> ForegroundClaim {
    ForegroundClaim::Constraint(
        RuntimeConstraintClaim::new(
            RuntimeConstraintState::Present,
            Some(ObservationSemanticId::new(semantic).expect("semantic is valid")),
        )
        .expect("constraint is valid"),
    )
}

fn limits() -> DetectionProfileLimits {
    DetectionProfileLimits {
        max_description_bytes: 1_024,
        max_binary_patterns: 16,
        max_argument_patterns: 16,
        max_environment_names: 16,
        max_environment_name_bytes: 128,
        max_foreground_rules: 32,
        max_background_rules: 16,
        max_patterns_per_rule: 16,
        max_pattern_bytes: 1_024,
        max_trait_needles: 16,
        max_trait_text_bytes: 256,
        max_input_submission_delay_millis: 2_000,
    }
}
