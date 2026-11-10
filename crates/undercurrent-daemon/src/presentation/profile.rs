// SPDX-License-Identifier: Apache-2.0

//! Profile presentation without retaining or echoing standalone test samples.
//!
//! Lint reports preserve typed locations and safe messages. Test reports expose
//! rule indices, dispositions, claims, and confidence; checked integer narrowing
//! fails presentation rather than truncating a source index.

use undercurrent_application::{
    ProfileCatalogEntry, ProfileIssueCode, ProfileIssueLocation, ProfileIssueSeverity,
    ProfileLintReport, ProfileMatchOutcome, ProfileSourceKind, ProfileSummary,
    ProfileTestDisposition, ProfileTestReport, ProfileTestRuleKind,
};
use undercurrent_domain::{
    BackgroundWorkKind, DETECTION_PROFILE_SCHEMA_VERSION, DetectionArchetype, InputDeliveryPolicy,
};
use undercurrent_protocol::{
    ProfileDetailDto, ProfileDiagnosticDto, ProfileInputDeliveryDto, ProfileLintDto,
    ProfileMatchDto, ProfileSummaryDto, ProfileTestDispositionDto, ProfileTestDto,
    ProfileTestEvaluationDto, ProfileTestRuleKindDto, ProfileTestSelectionDto,
};

use super::{ProductPresentation, ProductPresentationError, foreground_axis, foreground_claim};

impl ProductPresentation {
    /// Formats one compact profile summary.
    pub fn profile_summary(value: &ProfileSummary) -> ProfileSummaryDto {
        ProfileSummaryDto {
            name: value.name().as_str().to_owned(),
            source: profile_source(value.source()).to_owned(),
            description: value.description().map(str::to_owned),
            disabled: value.disabled(),
        }
    }

    /// Formats one complete authorized active profile.
    pub fn profile_detail(
        entry: &ProfileCatalogEntry,
    ) -> Result<ProfileDetailDto, ProductPresentationError> {
        let profile = entry.profile();
        Ok(ProfileDetailDto {
            matching_fallback: profile.matching_fallback(),
            summary: Self::profile_summary(&ProfileSummary::from_entry(entry)),
            schema_version: DETECTION_PROFILE_SCHEMA_VERSION,
            archetype: archetype(profile.archetype()).to_owned(),
            input_delivery: input_delivery(profile.input_delivery())?,
            binary_patterns: profile
                .matching()
                .binary()
                .iter()
                .map(|pattern| pattern.as_str().to_owned())
                .collect(),
            argument_patterns: profile
                .matching()
                .arguments()
                .iter()
                .map(|pattern| pattern.as_str().to_owned())
                .collect(),
            required_environment: profile
                .matching()
                .required_environment()
                .iter()
                .map(|name| name.as_str().to_owned())
                .collect(),
            foreground_rule_count: u32::try_from(profile.foreground_rules().len()).map_err(
                |_| ProductPresentationError {
                    field: "profile_foreground_rules",
                },
            )?,
            background_rule_count: u32::try_from(profile.background_rules().len()).map_err(
                |_| ProductPresentationError {
                    field: "profile_background_rules",
                },
            )?,
        })
    }

    /// Formats deterministic detection-profile selection.
    pub fn profile_match(value: &ProfileMatchOutcome) -> ProfileMatchDto {
        match value {
            ProfileMatchOutcome::Matched { profile, .. } => ProfileMatchDto {
                selected: Some(profile.as_str().to_owned()),
                candidates: vec![profile.as_str().to_owned()],
            },
            ProfileMatchOutcome::NoMatch => ProfileMatchDto {
                selected: None,
                candidates: Vec::new(),
            },
            ProfileMatchOutcome::Ambiguous { profiles } => ProfileMatchDto {
                selected: None,
                candidates: profiles
                    .iter()
                    .map(|profile| profile.as_str().to_owned())
                    .collect(),
            },
        }
    }

    /// Formats one or more profile lint reports into flat stable diagnostics.
    pub fn profile_lint(reports: &[ProfileLintReport]) -> ProfileLintDto {
        ProfileLintDto {
            valid: reports.iter().all(ProfileLintReport::is_valid),
            diagnostics: reports
                .iter()
                .flat_map(|report| {
                    report.issues().iter().map(|issue| ProfileDiagnosticDto {
                        profile: Some(report.profile().as_str().to_owned()),
                        severity: issue_severity(issue.severity()).to_owned(),
                        field: Some(issue_location(issue.location())),
                        message: issue_code(issue.code()).to_owned(),
                    })
                })
                .collect(),
        }
    }

    /// Formats a rule-level standalone profile test without sample text.
    pub fn profile_test(
        report: &ProfileTestReport,
    ) -> Result<ProfileTestDto, ProductPresentationError> {
        Ok(ProfileTestDto {
            profile: report.profile().as_str().to_owned(),
            evaluations: report
                .evaluations()
                .iter()
                .map(|evaluation| {
                    Ok(ProfileTestEvaluationDto {
                        rule: index(evaluation.rule(), "profile_test_rule")?,
                        kind: match evaluation.kind() {
                            ProfileTestRuleKind::Foreground { axis } => {
                                ProfileTestRuleKindDto::Foreground {
                                    axis: foreground_axis(axis),
                                }
                            }
                            ProfileTestRuleKind::Background { kind } => {
                                ProfileTestRuleKindDto::Background {
                                    work_kind: background_kind(kind).to_owned(),
                                }
                            }
                        },
                        sample: evaluation
                            .sample()
                            .map(|value| index(value, "profile_test_sample"))
                            .transpose()?,
                        disposition: match evaluation.disposition() {
                            ProfileTestDisposition::Matched => ProfileTestDispositionDto::Matched,
                            ProfileTestDisposition::NotMatched => {
                                ProfileTestDispositionDto::NotMatched
                            }
                            ProfileTestDisposition::Excluded => ProfileTestDispositionDto::Excluded,
                            ProfileTestDisposition::TraitsRejected => {
                                ProfileTestDispositionDto::TraitsRejected
                            }
                            ProfileTestDisposition::SampleUnavailable => {
                                ProfileTestDispositionDto::SampleUnavailable
                            }
                        },
                        matched_patterns: evaluation
                            .matched_patterns()
                            .iter()
                            .map(|value| index(*value, "profile_test_matched_pattern"))
                            .collect::<Result<Vec<_>, _>>()?,
                        excluded_by: evaluation
                            .excluded_by()
                            .map(|value| index(value, "profile_test_excluded_pattern"))
                            .transpose()?,
                        matched_name_patterns: evaluation
                            .matched_name_patterns()
                            .iter()
                            .map(|value| index(*value, "profile_test_matched_name_pattern"))
                            .collect::<Result<Vec<_>, _>>()?,
                    })
                })
                .collect::<Result<Vec<_>, ProductPresentationError>>()?,
            selected_foreground: report
                .selected_foreground()
                .iter()
                .map(|selection| {
                    Ok(ProfileTestSelectionDto {
                        rule: index(selection.rule, "profile_test_selected_rule")?,
                        sample: index(selection.sample, "profile_test_selected_sample")?,
                        claim: foreground_claim(&selection.claim),
                        confidence_basis_points: selection.confidence.basis_points(),
                    })
                })
                .collect::<Result<Vec<_>, ProductPresentationError>>()?,
        })
    }
}

/// Projects a domain pacing policy without exposing adapter state.
fn input_delivery(
    value: InputDeliveryPolicy,
) -> Result<ProfileInputDeliveryDto, ProductPresentationError> {
    let text_mode = match value.text_mode() {
        undercurrent_domain::InputTextMode::Raw => undercurrent_protocol::ProfileTextModeDto::Raw,
        undercurrent_domain::InputTextMode::Paste => {
            undercurrent_protocol::ProfileTextModeDto::Paste
        }
    };
    match value.trailing_submit_delay() {
        None => Ok(ProfileInputDeliveryDto::Immediate { text_mode }),
        Some(delay) => Ok(ProfileInputDeliveryDto::PacedTrailingSubmit {
            text_mode,
            delay_millis: u64::try_from(delay.as_millis()).map_err(|_| {
                ProductPresentationError {
                    field: "profile_input_submission_delay",
                }
            })?,
        }),
    }
}

const fn profile_source(value: ProfileSourceKind) -> &'static str {
    match value {
        ProfileSourceKind::BuiltIn => "built_in",
        ProfileSourceKind::User => "user",
    }
}

const fn archetype(value: DetectionArchetype) -> &'static str {
    match value {
        DetectionArchetype::Unclassified => "unclassified",
        DetectionArchetype::LineOriented => "line_oriented",
        DetectionArchetype::AnimatedAgent => "animated_agent",
        DetectionArchetype::Server => "server",
    }
}

const fn background_kind(value: BackgroundWorkKind) -> &'static str {
    match value {
        BackgroundWorkKind::ProcessGroup => "process_group",
        BackgroundWorkKind::Agent => "agent",
    }
}

const fn issue_severity(value: ProfileIssueSeverity) -> &'static str {
    match value {
        ProfileIssueSeverity::Error => "error",
        ProfileIssueSeverity::Warning => "warning",
    }
}

const fn issue_code(value: ProfileIssueCode) -> &'static str {
    match value {
        ProfileIssueCode::Disabled => "profile is disabled for automatic matching",
        ProfileIssueCode::NotAutomaticallySelectable => "profile has no automatic selection rule",
        ProfileIssueCode::ArgumentsWithoutBinary => {
            "argument-only matching may select unrelated executables"
        }
        ProfileIssueCode::InvalidPatternSyntax => "profile pattern syntax is invalid",
        ProfileIssueCode::PatternResourceLimit => "profile pattern exceeds engine resource limits",
    }
}

fn issue_location(value: &ProfileIssueLocation) -> String {
    match value {
        ProfileIssueLocation::Profile => "profile".to_owned(),
        ProfileIssueLocation::MatchArgument { index } => format!("matching.arguments[{index}]"),
        ProfileIssueLocation::PromptPattern { rule, index } => {
            format!("foreground_rules[{rule}].patterns[{index}]")
        }
        ProfileIssueLocation::PromptExclude { rule, index } => {
            format!("foreground_rules[{rule}].excludes[{index}]")
        }
        ProfileIssueLocation::BackgroundPattern { rule, index } => {
            format!("background_rules[{rule}].patterns[{index}]")
        }
        ProfileIssueLocation::BackgroundExclude { rule, index } => {
            format!("background_rules[{rule}].excludes[{index}]")
        }
        ProfileIssueLocation::BackgroundNamePattern { rule, index } => {
            format!("background_rules[{rule}].name_patterns[{index}]")
        }
    }
}

fn index(value: usize, field: &'static str) -> Result<u32, ProductPresentationError> {
    u32::try_from(value).map_err(|_| ProductPresentationError { field })
}
