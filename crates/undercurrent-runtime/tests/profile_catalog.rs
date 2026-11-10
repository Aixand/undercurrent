// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;

use undercurrent_application::*;
use undercurrent_domain::*;
use undercurrent_runtime::*;

#[test]
fn user_profiles_override_builtins_and_catalog_reads_are_sorted() {
    let catalog = InMemoryDetectionProfileCatalog::new(8).expect("limit is valid");
    let activation = catalog
        .replace(
            vec![
                profile("zsh", "zsh", false),
                profile("codex", "codex-old", false),
            ],
            vec![profile("codex", "codex", false)],
            &TestPatterns,
        )
        .expect("activation succeeds");

    assert_eq!(activation.generation().value(), 1);
    assert_eq!(activation.profiles(), 2);
    assert_eq!(activation.overridden_builtins(), 1);
    assert!(activation.warnings().is_empty());
    let summaries = catalog.list(8).expect("list succeeds");
    assert_eq!(
        summaries
            .iter()
            .map(|summary| summary.name().as_str())
            .collect::<Vec<_>>(),
        vec!["codex", "zsh"]
    );
    assert_eq!(summaries[0].source(), ProfileSourceKind::User);
    let codex = catalog
        .get(&profile_name("codex"))
        .expect("read succeeds")
        .expect("profile exists");
    assert_eq!(codex.profile().matching().binary()[0].as_str(), "codex");
}

#[test]
fn invalid_reload_is_atomic_and_preserves_previous_generation() {
    let catalog = InMemoryDetectionProfileCatalog::new(8).expect("limit is valid");
    catalog
        .replace(vec![profile("good", "good", false)], vec![], &TestPatterns)
        .expect("initial activation succeeds");
    let error = catalog
        .replace(
            vec![profile_with_argument("broken", "invalid:([")],
            vec![],
            &TestPatterns,
        )
        .expect_err("invalid replacement is rejected");

    let ProfileCatalogActivationError::InvalidProfiles { reports } = error else {
        panic!("expected invalid profiles");
    };
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].profile().as_str(), "broken");
    assert_eq!(catalog.pressure().generation.value(), 1);
    assert!(
        catalog
            .get(&profile_name("good"))
            .expect("read succeeds")
            .is_some()
    );
    assert!(
        catalog
            .get(&profile_name("broken"))
            .expect("read succeeds")
            .is_none()
    );
}

#[test]
fn duplicate_names_within_one_source_are_rejected() {
    let catalog = InMemoryDetectionProfileCatalog::new(8).expect("limit is valid");
    let error = catalog
        .replace(
            vec![profile("same", "one", false), profile("same", "two", false)],
            vec![],
            &TestPatterns,
        )
        .expect_err("duplicate builtin name is rejected");

    assert_eq!(
        error,
        ProfileCatalogActivationError::DuplicateName {
            profile: profile_name("same"),
            source: ProfileSourceKind::BuiltIn,
        }
    );
    assert_eq!(catalog.pressure().generation.value(), 0);
}

#[test]
fn capacity_is_enforced_after_valid_override_resolution() {
    let catalog = InMemoryDetectionProfileCatalog::new(2).expect("limit is valid");
    let activation = catalog
        .replace(
            vec![profile("one", "one", false), profile("two", "two", false)],
            vec![profile("one", "one-user", false)],
            &TestPatterns,
        )
        .expect("override keeps active count at two");
    assert_eq!(activation.profiles(), 2);

    let error = catalog
        .replace(
            vec![profile("one", "one", false), profile("two", "two", false)],
            vec![profile("three", "three", false)],
            &TestPatterns,
        )
        .expect_err("third active name exceeds capacity");
    assert_eq!(
        error,
        ProfileCatalogActivationError::CapacityExceeded {
            attempted: 3,
            limit: 2
        }
    );
    assert_eq!(catalog.pressure().generation.value(), 1);
}

#[test]
fn three_hundred_profiles_remain_bounded_and_name_indexed() {
    let catalog = InMemoryDetectionProfileCatalog::new(300).expect("limit is valid");
    let profiles = (0..300)
        .map(|index| {
            let name = format!("tool-{index:03}");
            profile(&name, &name, false)
        })
        .collect();
    catalog
        .replace(profiles, vec![], &TestPatterns)
        .expect("300-profile catalog activates");

    assert_eq!(catalog.pressure().profiles, 300);
    assert_eq!(catalog.all(300).expect("bounded read succeeds").len(), 300);
    let error = catalog.all(299).expect_err("truncation is never silent");
    assert_eq!(error.kind(), PortErrorKind::CapacityExceeded);
    assert_eq!(error.operation(), PortOperation::ReadDetectionProfiles);
}

#[test]
fn disabled_and_explicit_only_profiles_activate_with_typed_warnings() {
    let catalog = InMemoryDetectionProfileCatalog::new(4).expect("limit is valid");
    let profile = DetectionProfile::new(
        profile_name("manual"),
        None,
        true,
        ProfileMatchRules::default(),
        DetectionArchetype::Unclassified,
        undercurrent_domain::InputDeliveryPolicy::immediate(),
        None,
        vec![],
        vec![],
        false,
        false,
        OutputExclusions::default(),
        limits(),
    )
    .expect("profile is structurally valid");

    let activation = catalog
        .replace(vec![profile], vec![], &TestPatterns)
        .expect("warnings do not block activation");

    assert_eq!(activation.warnings().len(), 1);
    assert_eq!(
        activation.warnings()[0]
            .issues()
            .iter()
            .map(ProfileLintIssue::code)
            .collect::<Vec<_>>(),
        vec![
            ProfileIssueCode::Disabled,
            ProfileIssueCode::NotAutomaticallySelectable
        ]
    );
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
        Ok(text.contains(expression.as_str()))
    }
}

fn profile(name: &str, binary: &str, disabled: bool) -> DetectionProfile {
    let matching = ProfileMatchRules::new(
        vec![BinaryPattern::new(binary, 128).expect("binary is valid")],
        vec![],
        BTreeSet::new(),
        limits(),
    )
    .expect("match rules are valid");
    DetectionProfile::new(
        profile_name(name),
        Some(format!("{name} profile")),
        disabled,
        matching,
        DetectionArchetype::Unclassified,
        undercurrent_domain::InputDeliveryPolicy::immediate(),
        None,
        vec![],
        vec![],
        false,
        false,
        OutputExclusions::default(),
        limits(),
    )
    .expect("profile is valid")
}

fn profile_with_argument(name: &str, argument: &str) -> DetectionProfile {
    let matching = ProfileMatchRules::new(
        vec![BinaryPattern::new(name, 128).expect("binary is valid")],
        vec![PatternExpression::new(argument, 128).expect("expression is bounded")],
        BTreeSet::new(),
        limits(),
    )
    .expect("match rules are valid");
    DetectionProfile::new(
        profile_name(name),
        None,
        false,
        matching,
        DetectionArchetype::Unclassified,
        undercurrent_domain::InputDeliveryPolicy::immediate(),
        None,
        vec![],
        vec![],
        false,
        false,
        OutputExclusions::default(),
        limits(),
    )
    .expect("profile is structurally valid")
}

fn profile_name(value: &str) -> ProfileName {
    ProfileName::new(value).expect("profile name is valid")
}

fn limits() -> DetectionProfileLimits {
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
