// SPDX-License-Identifier: Apache-2.0

//! Authorized standalone detection-profile use cases.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use undercurrent_domain::{
    BackgroundObservationRule, BackgroundWorkKind, Confidence, DetectionProfile, ForegroundAxis,
    ForegroundClaim, ForegroundRule, PatternExpression, ProfileMatchSignature, ProfileMatchTarget,
    ProfileName, ProfileSampleView, TimestampMillis,
};

use crate::{
    AccessAction, AccessDecision, AccessDenialReason, AccessResource, ApplicationErrorCategory,
    ApplicationOperation, AuthorizationPort, Clock, DetectionProfileCatalog, DiagnosticEventName,
    DiagnosticLevel, DiagnosticRecord, MetricName, MetricObservation, PortError,
    ProfilePatternEngine, Telemetry,
};

/// Origin of one active profile after override resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProfileSourceKind {
    /// Definition embedded by the distribution.
    BuiltIn,
    /// Definition loaded from operator-controlled configuration.
    User,
}

/// Compact profile projection used by list operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileSummary {
    name: ProfileName,
    source: ProfileSourceKind,
    description: Option<String>,
    disabled: bool,
}

impl ProfileSummary {
    /// Projects a complete catalog entry into a bounded summary.
    ///
    /// This function performs no IO and does not panic.
    pub fn from_entry(entry: &ProfileCatalogEntry) -> Self {
        Self {
            name: entry.profile.name().clone(),
            source: entry.source,
            description: entry.profile.description().map(str::to_owned),
            disabled: entry.profile.disabled(),
        }
    }

    /// Returns the stable profile name.
    pub const fn name(&self) -> &ProfileName {
        &self.name
    }

    /// Returns whether the active definition is builtin or user supplied.
    pub const fn source(&self) -> ProfileSourceKind {
        self.source
    }

    /// Returns the optional human-facing description.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Returns whether automatic matching ignores this profile.
    pub const fn disabled(&self) -> bool {
        self.disabled
    }
}

/// Complete active profile paired with its resolved source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileCatalogEntry {
    profile: Arc<DetectionProfile>,
    source: ProfileSourceKind,
}

impl ProfileCatalogEntry {
    /// Constructs a catalog entry without performing IO.
    pub fn new(profile: Arc<DetectionProfile>, source: ProfileSourceKind) -> Self {
        Self { profile, source }
    }

    /// Returns the immutable active definition.
    pub fn profile(&self) -> &DetectionProfile {
        &self.profile
    }

    /// Returns a shared immutable active definition.
    pub fn shared_profile(&self) -> Arc<DetectionProfile> {
        Arc::clone(&self.profile)
    }

    /// Returns the resolved source.
    pub const fn source(&self) -> ProfileSourceKind {
        self.source
    }
}

/// Stable profile-expression rejection kinds returned by pattern adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProfilePatternErrorKind {
    /// Expression is not valid for the configured syntax.
    InvalidSyntax,
    /// Compiling or evaluating the expression exceeded an engine limit.
    ResourceLimit,
}

/// Expected failure to compile or evaluate one profile expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfilePatternError {
    kind: ProfilePatternErrorKind,
    offset: Option<usize>,
}

impl ProfilePatternError {
    /// Constructs a typed expression failure without retaining private input.
    pub const fn new(kind: ProfilePatternErrorKind, offset: Option<usize>) -> Self {
        Self { kind, offset }
    }

    /// Returns the stable failure kind.
    pub const fn kind(&self) -> ProfilePatternErrorKind {
        self.kind
    }

    /// Returns an optional byte offset supplied by the pattern engine.
    pub const fn offset(&self) -> Option<usize> {
        self.offset
    }
}

impl Display for ProfilePatternError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match (self.kind, self.offset) {
            (ProfilePatternErrorKind::InvalidSyntax, Some(offset)) => {
                write!(formatter, "invalid pattern syntax at byte {offset}")
            }
            (ProfilePatternErrorKind::InvalidSyntax, None) => {
                formatter.write_str("invalid pattern syntax")
            }
            (ProfilePatternErrorKind::ResourceLimit, Some(offset)) => {
                write!(formatter, "pattern resource limit reached at byte {offset}")
            }
            (ProfilePatternErrorKind::ResourceLimit, None) => {
                formatter.write_str("pattern resource limit reached")
            }
        }
    }
}

impl Error for ProfilePatternError {}

/// Stable location of a lintable expression or profile-level rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfileIssueLocation {
    /// Whole profile.
    Profile,
    /// Argument selector at zero-based index.
    MatchArgument { index: usize },
    /// Prompt include expression.
    PromptPattern { rule: usize, index: usize },
    /// Prompt exclusion expression.
    PromptExclude { rule: usize, index: usize },
    /// Background presence/count expression.
    BackgroundPattern { rule: usize, index: usize },
    /// Background exclusion expression.
    BackgroundExclude { rule: usize, index: usize },
    /// Background work-name expression.
    BackgroundNamePattern { rule: usize, index: usize },
}

/// Stable machine-readable lint issue code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileIssueCode {
    /// Profile is valid but excluded from automatic matching.
    Disabled,
    /// Profile has no binary or argument selection dimension.
    NotAutomaticallySelectable,
    /// Profile selects only by arguments and may be overly broad.
    ArgumentsWithoutBinary,
    /// One expression has invalid syntax.
    InvalidPatternSyntax,
    /// One expression exceeds pattern-engine limits.
    PatternResourceLimit,
}

/// Severity of one profile lint issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileIssueSeverity {
    /// Blocks profile activation.
    Error,
    /// Profile remains valid but deserves operator attention.
    Warning,
}

/// One typed, presentation-independent lint issue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileLintIssue {
    severity: ProfileIssueSeverity,
    code: ProfileIssueCode,
    location: ProfileIssueLocation,
    offset: Option<usize>,
}

impl ProfileLintIssue {
    /// Returns issue severity.
    pub const fn severity(&self) -> ProfileIssueSeverity {
        self.severity
    }

    /// Returns stable machine-readable code.
    pub const fn code(&self) -> ProfileIssueCode {
        self.code
    }

    /// Returns the exact profile field location.
    pub const fn location(&self) -> &ProfileIssueLocation {
        &self.location
    }

    /// Returns an optional pattern byte offset.
    pub const fn offset(&self) -> Option<usize> {
        self.offset
    }
}

/// Complete lint outcome for one active profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileLintReport {
    profile: ProfileName,
    issues: Vec<ProfileLintIssue>,
}

impl ProfileLintReport {
    /// Returns the linted profile name.
    pub const fn profile(&self) -> &ProfileName {
        &self.profile
    }

    /// Returns issues in deterministic field order.
    pub fn issues(&self) -> &[ProfileLintIssue] {
        &self.issues
    }

    /// Returns whether no blocking issue was found.
    pub fn is_valid(&self) -> bool {
        self.issues
            .iter()
            .all(|issue| issue.severity != ProfileIssueSeverity::Error)
    }
}

/// Successful reason one profile matched a command target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileMatchReason {
    /// Executable basename matched.
    Binary,
    /// Joined argument vector matched.
    Arguments,
    /// Executable basename and joined arguments both matched.
    BinaryAndArguments,
}

/// Complete deterministic profile-selection outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfileMatchOutcome {
    /// Exactly one enabled active profile matched.
    Matched {
        /// Stable selected profile name.
        profile: ProfileName,
        /// Selection dimensions that matched.
        reason: ProfileMatchReason,
    },
    /// No enabled profile matched every configured dimension.
    NoMatch,
    /// Multiple enabled profiles matched; none is selected implicitly.
    Ambiguous {
        /// All matching profile names in stable name order.
        profiles: Vec<ProfileName>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ForegroundProfileMatchOutcome {
    Determinate(ProfileMatchOutcome),
    IncompleteEvidence,
}

/// Profile rule category in standalone test diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileTestRuleKind {
    /// Profile-owned foreground-observation classifier.
    Foreground { axis: ForegroundAxis },
    /// Background WorkContext evidence rule.
    Background { kind: BackgroundWorkKind },
}

/// Result of evaluating one rule against one supplied region view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileTestDisposition {
    /// Rule matched and was not vetoed.
    Matched,
    /// Include expressions did not match.
    NotMatched,
    /// An exclusion expression vetoed the rule.
    Excluded,
    /// Structural prompt traits rejected the view.
    TraitsRejected,
    /// Caller supplied no view for the rule's exact region.
    SampleUnavailable,
}

/// One bounded rule/sample evaluation with expression indices, not text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileTestEvaluation {
    rule: usize,
    kind: ProfileTestRuleKind,
    sample: Option<usize>,
    disposition: ProfileTestDisposition,
    matched_patterns: Vec<usize>,
    excluded_by: Option<usize>,
    matched_name_patterns: Vec<usize>,
}

impl ProfileTestEvaluation {
    /// Returns the zero-based rule index within its rule kind.
    pub const fn rule(&self) -> usize {
        self.rule
    }

    /// Returns the evaluated rule kind.
    pub const fn kind(&self) -> ProfileTestRuleKind {
        self.kind
    }

    /// Returns the supplied sample index, or `None` when unavailable.
    pub const fn sample(&self) -> Option<usize> {
        self.sample
    }

    /// Returns the typed evaluation result.
    pub const fn disposition(&self) -> ProfileTestDisposition {
        self.disposition
    }

    /// Returns all include-expression indices that matched.
    pub fn matched_patterns(&self) -> &[usize] {
        &self.matched_patterns
    }

    /// Returns the first exclusion-expression index that vetoed the rule.
    pub const fn excluded_by(&self) -> Option<usize> {
        self.excluded_by
    }

    /// Returns background work-name expression indices that matched.
    ///
    /// Prompt evaluations always return an empty slice.
    pub fn matched_name_patterns(&self) -> &[usize] {
        &self.matched_name_patterns
    }
}

/// Winning foreground classification from a standalone profile test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileTestSelection {
    /// Zero-based foreground rule index.
    pub rule: usize,
    /// Supplied sample index.
    pub sample: usize,
    /// Selected profile-owned foreground claim.
    pub claim: ForegroundClaim,
    /// Profile-declared confidence.
    pub confidence: Confidence,
}

/// Complete standalone profile-test report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileTestReport {
    profile: ProfileName,
    evaluations: Vec<ProfileTestEvaluation>,
    selected_foreground: Vec<ProfileTestSelection>,
}

impl ProfileTestReport {
    /// Returns the tested profile name.
    pub const fn profile(&self) -> &ProfileName {
        &self.profile
    }

    /// Returns bounded evaluations in rule then sample order.
    pub fn evaluations(&self) -> &[ProfileTestEvaluation] {
        &self.evaluations
    }

    /// Returns at most one winning foreground classification per independent axis.
    pub fn selected_foreground(&self) -> &[ProfileTestSelection] {
        &self.selected_foreground
    }
}

/// Hard bounds for standalone profile use cases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProfileServiceLimits {
    /// Maximum active profiles examined by list or match.
    pub max_profiles: usize,
    /// Maximum pre-extracted screen views accepted by one test.
    pub max_test_samples: usize,
}

impl ProfileServiceLimits {
    /// Validates nonzero use-case limits.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns [`ProfileServiceError::InvalidArgument`] for zero limits.
    pub fn validate(self) -> Result<Self, ProfileServiceError> {
        if self.max_profiles == 0 {
            return Err(ProfileServiceError::InvalidArgument {
                field: "max_profiles",
                rule: "must be greater than zero",
            });
        }
        if self.max_test_samples == 0 {
            return Err(ProfileServiceError::InvalidArgument {
                field: "max_test_samples",
                rule: "must be greater than zero",
            });
        }
        Ok(self)
    }
}

/// Deterministically selects one active profile from bounded command facts.
///
/// This application-core service performs no authorization because it is also
/// used inside already-authorized session creation. Callers exposing profile
/// matching directly must authorize before invoking it. Catalog and pattern
/// access occur through inward-facing ports, and the service does not panic.
pub struct DetectionProfileSelector<'a> {
    catalog: &'a dyn DetectionProfileCatalog,
    patterns: &'a dyn ProfilePatternEngine,
    max_profiles: usize,
}

impl<'a> DetectionProfileSelector<'a> {
    /// Constructs a selector with a hard active-profile bound.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed invalid-argument failure when `max_profiles` is zero.
    pub fn new(
        catalog: &'a dyn DetectionProfileCatalog,
        patterns: &'a dyn ProfilePatternEngine,
        max_profiles: usize,
    ) -> Result<Self, ProfileServiceError> {
        if max_profiles == 0 {
            return Err(ProfileServiceError::InvalidArgument {
                field: "max_profiles",
                rule: "must be greater than zero",
            });
        }
        Ok(Self {
            catalog,
            patterns,
            max_profiles,
        })
    }

    /// Matches every enabled profile and selects only one unambiguous result.
    ///
    /// The catalog read is bounded by `max_profiles`. No match and ambiguity
    /// are successful, explicit outcomes; invalid active expressions and
    /// catalog failures remain typed errors. This method does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed infrastructure or invalid-active-profile failure.
    pub fn select(
        &self,
        target: &ProfileMatchTarget,
    ) -> Result<ProfileMatchOutcome, ProfileServiceError> {
        match self.select_targets(&[(target, true, None)])?.0 {
            ForegroundProfileMatchOutcome::Determinate(outcome) => Ok(outcome),
            ForegroundProfileMatchOutcome::IncompleteEvidence => Ok(ProfileMatchOutcome::NoMatch),
        }
    }

    /// Resolves fallback priority once, across every foreground member.
    pub(crate) fn select_foreground_group(
        &self,
        members: &[crate::ForegroundProgramMember],
    ) -> Result<
        (
            ForegroundProfileMatchOutcome,
            Vec<undercurrent_domain::ProfileMatchEvidence>,
        ),
        ProfileServiceError,
    > {
        let targets: Vec<_> = members
            .iter()
            .map(|member| {
                (
                    member.target(),
                    member.environment_names_complete(),
                    Some(member.incarnation()),
                )
            })
            .collect();
        self.select_targets(&targets)
    }

    fn select_targets(
        &self,
        targets: &[(&ProfileMatchTarget, bool, Option<&str>)],
    ) -> Result<
        (
            ForegroundProfileMatchOutcome,
            Vec<undercurrent_domain::ProfileMatchEvidence>,
        ),
        ProfileServiceError,
    > {
        let entries = self
            .catalog
            .all(self.max_profiles)
            .map_err(ProfileServiceError::Infrastructure)?;
        let arguments: Vec<_> = targets
            .iter()
            .map(|(target, _, _)| target.arguments().join(" "))
            .collect();
        let mut matched = Vec::new();
        let mut evidence = Vec::new();
        let mut incomplete_primary = false;
        let mut incomplete_fallback = false;
        for entry in entries {
            let profile = entry.profile();
            if profile.disabled() || !profile.matching().is_selectable() {
                continue;
            }
            let mut reason = None;
            let mut profile_incomplete = false;
            let mut incarnations = std::collections::BTreeSet::new();
            for ((target, environment_complete, incarnation), arguments) in
                targets.iter().zip(&arguments)
            {
                let mut argument_offset = 0;
                for signature in profile.matching().signatures() {
                    let candidate = match_signature(
                        self.patterns,
                        profile.name(),
                        signature,
                        argument_offset,
                        target,
                        arguments,
                        *environment_complete,
                    )?;
                    argument_offset += signature.arguments().len();
                    match candidate {
                        SignatureMatchOutcome::Matched(candidate) => {
                            reason = Some(stronger_match_reason(reason, candidate));
                            if let Some(incarnation) = incarnation {
                                incarnations.insert((*incarnation).to_owned());
                            }
                        }
                        SignatureMatchOutcome::NoMatch => {}
                        SignatureMatchOutcome::IncompleteEnvironment => profile_incomplete = true,
                    }
                }
            }
            if let Some(reason) = reason {
                matched.push((profile.name().clone(), reason, profile.matching_fallback()));
                evidence.push(undercurrent_domain::ProfileMatchEvidence::new(
                    profile.name().clone(),
                    profile.matching_fallback(),
                    incarnations.into_iter().collect(),
                ));
            } else if profile_incomplete {
                if profile.matching_fallback() {
                    incomplete_fallback = true;
                } else {
                    incomplete_primary = true;
                }
            }
        }
        evidence.sort_by(|left, right| left.profile().cmp(right.profile()));
        let has_primary = matched.iter().any(|(_, _, fallback)| !fallback);
        if has_primary {
            matched.retain(|(_, _, fallback)| !fallback);
        }
        matched.sort_by(|left, right| left.0.cmp(&right.0));
        let outcome = match matched.as_slice() {
            [] => ProfileMatchOutcome::NoMatch,
            [(profile, reason, _)] => ProfileMatchOutcome::Matched {
                profile: profile.clone(),
                reason: *reason,
            },
            _ => ProfileMatchOutcome::Ambiguous {
                profiles: matched.into_iter().map(|(profile, _, _)| profile).collect(),
            },
        };
        let incomplete = incomplete_primary || (!has_primary && incomplete_fallback);
        // Missing primary evidence can supersede an apparent fallback ambiguity.
        let decisive_ambiguity = matches!(outcome, ProfileMatchOutcome::Ambiguous { .. })
            && (has_primary || !incomplete_primary);
        let outcome = if incomplete && !decisive_ambiguity {
            ForegroundProfileMatchOutcome::IncompleteEvidence
        } else {
            ForegroundProfileMatchOutcome::Determinate(outcome)
        };
        Ok((outcome, evidence))
    }
}

enum SignatureMatchOutcome {
    Matched(ProfileMatchReason),
    NoMatch,
    IncompleteEnvironment,
}

/// Evaluates one conjunctive signature against bounded command facts.
///
/// Missing environment requirements short-circuit argument evaluation when the
/// environment-name set is complete. An incomplete set evaluates the command
/// dimensions so a potentially hidden match remains explicitly inconclusive.
/// Pattern failures retain their exact flattened expression location. This
/// function performs no IO and does not panic.
fn match_signature(
    patterns: &dyn ProfilePatternEngine,
    profile: &ProfileName,
    signature: &ProfileMatchSignature,
    argument_offset: usize,
    target: &ProfileMatchTarget,
    arguments: &str,
    environment_names_complete: bool,
) -> Result<SignatureMatchOutcome, ProfileServiceError> {
    let environment_matches = signature.environment_matches(target);
    if !environment_matches && environment_names_complete {
        return Ok(SignatureMatchOutcome::NoMatch);
    }

    let has_binary = !signature.binary().is_empty();
    let binary_matches = has_binary && signature.binary_matches(target);

    let has_arguments = !signature.arguments().is_empty();
    let arguments_match = if has_arguments && !arguments.is_empty() {
        any_expression_matches(
            patterns,
            profile,
            signature.arguments(),
            arguments,
            |index| ProfileIssueLocation::MatchArgument {
                index: argument_offset + index,
            },
        )?
    } else {
        false
    };

    let reason = match (has_binary, has_arguments, binary_matches, arguments_match) {
        (true, false, true, _) => Some(ProfileMatchReason::Binary),
        (false, true, _, true) => Some(ProfileMatchReason::Arguments),
        (true, true, true, true) => Some(ProfileMatchReason::BinaryAndArguments),
        _ => None,
    };
    Ok(match (reason, environment_matches) {
        (Some(reason), true) => SignatureMatchOutcome::Matched(reason),
        (Some(_), false) => SignatureMatchOutcome::IncompleteEnvironment,
        (None, _) => SignatureMatchOutcome::NoMatch,
    })
}

/// Chooses a stable, evidence-strength-ordered reason for one matched profile.
fn stronger_match_reason(
    current: Option<ProfileMatchReason>,
    candidate: ProfileMatchReason,
) -> ProfileMatchReason {
    let strength = |reason| match reason {
        ProfileMatchReason::Arguments => 1,
        ProfileMatchReason::Binary => 2,
        ProfileMatchReason::BinaryAndArguments => 3,
    };
    current
        .filter(|current| strength(*current) >= strength(candidate))
        .unwrap_or(candidate)
}

/// Failure from a standalone profile use case.
pub enum ProfileServiceError {
    /// Invalid direct use-case input.
    InvalidArgument {
        /// Stable field name.
        field: &'static str,
        /// Stable non-sensitive rule.
        rule: &'static str,
    },
    /// No valid authentication was established.
    Unauthenticated,
    /// Authenticated caller lacks profile-read permission.
    PermissionDenied,
    /// Requested profile is absent.
    NotFound {
        /// Stable requested profile name.
        profile: ProfileName,
    },
    /// An active catalog profile contains an expression that cannot run.
    InvalidActiveProfile {
        /// Stable affected profile name.
        profile: ProfileName,
        /// Exact expression location.
        location: ProfileIssueLocation,
        /// Typed pattern failure.
        error: ProfilePatternError,
    },
    /// Authorization or catalog infrastructure failed.
    Infrastructure(PortError),
}

/// Evaluates active profile rules against already bounded private samples.
///
/// This application-core helper performs no authorization or catalog IO. It is
/// reused by authorized standalone profile testing and trusted background
/// observation workers after their respective boundaries have supplied an
/// active profile and parser-consistent samples.
pub struct ProfileRuleEvaluator<'a> {
    patterns: &'a dyn ProfilePatternEngine,
}

impl<'a> ProfileRuleEvaluator<'a> {
    /// Constructs a stateless evaluator over one external pattern-engine port.
    pub const fn new(patterns: &'a dyn ProfilePatternEngine) -> Self {
        Self { patterns }
    }

    /// Evaluates every prompt and background rule in stable declaration order.
    ///
    /// Private sample text is inspected only through the pattern port and is
    /// not retained in the result. This function performs no IO itself and does
    /// not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed invalid-active-profile failure when one configured
    /// expression cannot be compiled or evaluated.
    pub fn evaluate(
        &self,
        profile: &DetectionProfile,
        samples: &[ProfileSampleView],
    ) -> Result<ProfileTestReport, ProfileServiceError> {
        let (mut evaluations, selected_foreground) =
            self.evaluate_foreground_rules(profile, samples)?;
        self.evaluate_background_rules(profile, samples, &mut evaluations)?;
        Ok(ProfileTestReport {
            profile: profile.name().clone(),
            evaluations,
            selected_foreground,
        })
    }

    /// Selects the winning current-screen foreground rule without background rules.
    ///
    /// This is the bounded foreground-observation path. It shares exact rule
    /// semantics with [`Self::evaluate`] while avoiding unrelated background
    /// regular-expression work. Private sample text is not retained in the
    /// result. This function performs no IO itself and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed invalid-active-profile failure when one foreground
    /// expression cannot be compiled or evaluated.
    pub fn select_foreground(
        &self,
        profile: &DetectionProfile,
        samples: &[ProfileSampleView],
    ) -> Result<Vec<ProfileTestSelection>, ProfileServiceError> {
        self.evaluate_foreground_rules(profile, samples)
            .map(|(_, selected)| selected)
    }

    fn evaluate_foreground_rules(
        &self,
        profile: &DetectionProfile,
        samples: &[ProfileSampleView],
    ) -> Result<(Vec<ProfileTestEvaluation>, Vec<ProfileTestSelection>), ProfileServiceError> {
        let mut evaluations = Vec::new();
        let mut selected_foreground = BTreeMap::new();
        for (rule_index, rule) in profile.foreground_rules().iter().enumerate() {
            let matching_samples: Vec<_> = samples
                .iter()
                .enumerate()
                .filter(|(_, sample)| sample.region() == rule.region())
                .collect();
            if matching_samples.is_empty() {
                evaluations.push(ProfileTestEvaluation {
                    rule: rule_index,
                    kind: ProfileTestRuleKind::Foreground {
                        axis: rule.claim().axis(),
                    },
                    sample: None,
                    disposition: ProfileTestDisposition::SampleUnavailable,
                    matched_patterns: Vec::new(),
                    excluded_by: None,
                    matched_name_patterns: Vec::new(),
                });
                continue;
            }
            for (sample_index, sample) in matching_samples {
                let evaluation = self.evaluate_foreground(
                    profile.name(),
                    rule_index,
                    rule,
                    sample_index,
                    sample,
                )?;
                if evaluation.disposition == ProfileTestDisposition::Matched {
                    let candidate = ProfileTestSelection {
                        rule: rule_index,
                        sample: sample_index,
                        claim: rule.claim().clone(),
                        confidence: rule.confidence(),
                    };
                    let axis = candidate.claim.axis();
                    if selected_foreground
                        .get(&axis)
                        .is_none_or(|selected| foreground_candidate_precedes(&candidate, selected))
                    {
                        selected_foreground.insert(axis, candidate);
                    }
                }
                evaluations.push(evaluation);
            }
        }
        Ok((evaluations, selected_foreground.into_values().collect()))
    }

    fn evaluate_background_rules(
        &self,
        profile: &DetectionProfile,
        samples: &[ProfileSampleView],
        evaluations: &mut Vec<ProfileTestEvaluation>,
    ) -> Result<(), ProfileServiceError> {
        for (rule_index, rule) in profile.background_rules().iter().enumerate() {
            let matching_samples: Vec<_> = samples
                .iter()
                .enumerate()
                .filter(|(_, sample)| sample.region() == rule.region())
                .collect();
            if matching_samples.is_empty() {
                evaluations.push(ProfileTestEvaluation {
                    rule: rule_index,
                    kind: ProfileTestRuleKind::Background { kind: rule.kind() },
                    sample: None,
                    disposition: ProfileTestDisposition::SampleUnavailable,
                    matched_patterns: Vec::new(),
                    excluded_by: None,
                    matched_name_patterns: Vec::new(),
                });
                continue;
            }
            for (sample_index, sample) in matching_samples {
                evaluations.push(self.evaluate_background(
                    profile.name(),
                    rule_index,
                    rule,
                    sample_index,
                    sample,
                )?);
            }
        }
        Ok(())
    }

    fn evaluate_foreground(
        &self,
        profile: &ProfileName,
        rule_index: usize,
        rule: &ForegroundRule,
        sample_index: usize,
        sample: &ProfileSampleView,
    ) -> Result<ProfileTestEvaluation, ProfileServiceError> {
        let excluded_by =
            self.first_expression_match(profile, rule.excludes(), sample, |index| {
                ProfileIssueLocation::PromptExclude {
                    rule: rule_index,
                    index,
                }
            })?;
        let (disposition, matched_patterns) = if excluded_by.is_some() {
            (ProfileTestDisposition::Excluded, Vec::new())
        } else if !rule.traits().matches(sample) {
            (ProfileTestDisposition::TraitsRejected, Vec::new())
        } else if rule.patterns().is_empty() {
            (ProfileTestDisposition::Matched, Vec::new())
        } else {
            let matched = self.expression_matches(profile, rule.patterns(), sample, |index| {
                ProfileIssueLocation::PromptPattern {
                    rule: rule_index,
                    index,
                }
            })?;
            if matched.is_empty() {
                (ProfileTestDisposition::NotMatched, matched)
            } else {
                (ProfileTestDisposition::Matched, matched)
            }
        };
        Ok(ProfileTestEvaluation {
            rule: rule_index,
            kind: ProfileTestRuleKind::Foreground {
                axis: rule.claim().axis(),
            },
            sample: Some(sample_index),
            disposition,
            matched_patterns,
            excluded_by,
            matched_name_patterns: Vec::new(),
        })
    }

    fn evaluate_background(
        &self,
        profile: &ProfileName,
        rule_index: usize,
        rule: &BackgroundObservationRule,
        sample_index: usize,
        sample: &ProfileSampleView,
    ) -> Result<ProfileTestEvaluation, ProfileServiceError> {
        let excluded_by =
            self.first_expression_match(profile, rule.excludes(), sample, |index| {
                ProfileIssueLocation::BackgroundExclude {
                    rule: rule_index,
                    index,
                }
            })?;
        let matched_patterns = if excluded_by.is_some() {
            Vec::new()
        } else {
            self.expression_matches(profile, rule.patterns(), sample, |index| {
                ProfileIssueLocation::BackgroundPattern {
                    rule: rule_index,
                    index,
                }
            })?
        };
        let disposition = if excluded_by.is_some() {
            ProfileTestDisposition::Excluded
        } else if matched_patterns.is_empty() {
            ProfileTestDisposition::NotMatched
        } else {
            ProfileTestDisposition::Matched
        };
        let matched_name_patterns = if disposition == ProfileTestDisposition::Matched {
            self.expression_matches(profile, rule.name_patterns(), sample, |index| {
                ProfileIssueLocation::BackgroundNamePattern {
                    rule: rule_index,
                    index,
                }
            })?
        } else {
            Vec::new()
        };
        Ok(ProfileTestEvaluation {
            rule: rule_index,
            kind: ProfileTestRuleKind::Background { kind: rule.kind() },
            sample: Some(sample_index),
            disposition,
            matched_patterns,
            excluded_by,
            matched_name_patterns,
        })
    }

    fn first_expression_match(
        &self,
        profile: &ProfileName,
        expressions: &[PatternExpression],
        sample: &ProfileSampleView,
        location: impl Fn(usize) -> ProfileIssueLocation,
    ) -> Result<Option<usize>, ProfileServiceError> {
        for (index, expression) in expressions.iter().enumerate() {
            if self.expression_matches_view(profile, expression, sample, location(index))? {
                return Ok(Some(index));
            }
        }
        Ok(None)
    }

    fn expression_matches(
        &self,
        profile: &ProfileName,
        expressions: &[PatternExpression],
        sample: &ProfileSampleView,
        location: impl Fn(usize) -> ProfileIssueLocation,
    ) -> Result<Vec<usize>, ProfileServiceError> {
        let mut matched = Vec::new();
        for (index, expression) in expressions.iter().enumerate() {
            if self.expression_matches_view(profile, expression, sample, location(index))? {
                matched.push(index);
            }
        }
        Ok(matched)
    }

    fn expression_matches_view(
        &self,
        profile: &ProfileName,
        expression: &PatternExpression,
        sample: &ProfileSampleView,
        location: ProfileIssueLocation,
    ) -> Result<bool, ProfileServiceError> {
        let on_last_line = self
            .patterns
            .is_match(expression, sample.last_line())
            .map_err(|error| invalid_profile(profile, location.clone(), error))?;
        if on_last_line {
            return Ok(true);
        }
        self.patterns
            .is_match(expression, sample.text())
            .map_err(|error| invalid_profile(profile, location, error))
    }
}

impl ProfileServiceError {
    /// Returns the stable category used by protocol and presentation adapters.
    pub const fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::InvalidArgument { .. } => ApplicationErrorCategory::InvalidArgument,
            Self::Unauthenticated => ApplicationErrorCategory::Unauthenticated,
            Self::PermissionDenied => ApplicationErrorCategory::PermissionDenied,
            Self::NotFound { .. } => ApplicationErrorCategory::NotFound,
            Self::InvalidActiveProfile { .. } => ApplicationErrorCategory::InvariantViolation,
            Self::Infrastructure(error) => error.category(),
        }
    }

    fn port_error(&self) -> Option<&PortError> {
        match self {
            Self::Infrastructure(error) => Some(error),
            Self::InvalidArgument { .. }
            | Self::Unauthenticated
            | Self::PermissionDenied
            | Self::NotFound { .. }
            | Self::InvalidActiveProfile { .. } => None,
        }
    }
}

impl Debug for ProfileServiceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, formatter)
    }
}

impl Display for ProfileServiceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidArgument { field, rule } => write!(formatter, "invalid {field}: {rule}"),
            Self::Unauthenticated => {
                formatter.write_str("authentication is required to read detection profiles")
            }
            Self::PermissionDenied => {
                formatter.write_str("permission denied to read detection profiles")
            }
            Self::NotFound { profile } => write!(formatter, "profile {profile} was not found"),
            Self::InvalidActiveProfile {
                profile,
                location,
                error,
            } => {
                write!(
                    formatter,
                    "active profile {profile} has an invalid expression at {location:?}: {error}"
                )
            }
            Self::Infrastructure(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for ProfileServiceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidActiveProfile { error, .. } => Some(error),
            Self::Infrastructure(error) => Some(error),
            Self::InvalidArgument { .. }
            | Self::Unauthenticated
            | Self::PermissionDenied
            | Self::NotFound { .. } => None,
        }
    }
}

/// Authorized use cases for listing, inspecting, matching, linting, and testing
/// detection profiles without starting or polling a session.
pub struct DetectionProfileService<'a> {
    authorizer: &'a dyn AuthorizationPort,
    catalog: &'a dyn DetectionProfileCatalog,
    patterns: &'a dyn ProfilePatternEngine,
    clock: &'a dyn Clock,
    telemetry: &'a dyn Telemetry,
    limits: ProfileServiceLimits,
}

impl<'a> DetectionProfileService<'a> {
    /// Constructs the service after validating hard limits.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed invalid-argument error for zero limits.
    pub fn new(
        authorizer: &'a dyn AuthorizationPort,
        catalog: &'a dyn DetectionProfileCatalog,
        patterns: &'a dyn ProfilePatternEngine,
        clock: &'a dyn Clock,
        telemetry: &'a dyn Telemetry,
        limits: ProfileServiceLimits,
    ) -> Result<Self, ProfileServiceError> {
        Ok(Self {
            authorizer,
            catalog,
            patterns,
            clock,
            telemetry,
            limits: limits.validate()?,
        })
    }

    /// Lists every active profile as a bounded, name-sorted summary.
    ///
    /// This method authorizes before catalog access, starts no session, and
    /// performs no process, PTY, environment, or filesystem IO itself.
    ///
    /// # Errors
    ///
    /// Returns typed authorization or catalog failures.
    pub async fn list(&self) -> Result<Vec<ProfileSummary>, ProfileServiceError> {
        let operation = ApplicationOperation::ListDetectionProfiles;
        self.run(operation, async {
            self.authorize().await?;
            self.catalog
                .list(self.limits.max_profiles)
                .map_err(ProfileServiceError::Infrastructure)
        })
        .await
    }

    /// Loads one complete active profile by stable name.
    ///
    /// This method authorizes before catalog access and performs no unrelated
    /// session work.
    ///
    /// # Errors
    ///
    /// Returns typed authorization, absence, or catalog failures.
    pub async fn inspect(
        &self,
        name: &ProfileName,
    ) -> Result<ProfileCatalogEntry, ProfileServiceError> {
        let operation = ApplicationOperation::InspectDetectionProfile;
        self.run(operation, async {
            self.authorize().await?;
            self.load(name)
        })
        .await
    }

    /// Matches explicit bounded command facts against every enabled profile.
    ///
    /// Environment names come from `target`; this method never reads the daemon
    /// environment. All candidates are evaluated so ambiguity is explicit.
    ///
    /// # Errors
    ///
    /// Returns typed authorization, catalog, or invalid-active-profile failures.
    pub async fn match_profile(
        &self,
        target: &ProfileMatchTarget,
    ) -> Result<ProfileMatchOutcome, ProfileServiceError> {
        let operation = ApplicationOperation::MatchDetectionProfile;
        self.run(operation, async {
            self.authorize().await?;
            DetectionProfileSelector::new(self.catalog, self.patterns, self.limits.max_profiles)?
                .select(target)
        })
        .await
    }

    /// Lints every expression and emits typed warnings for selection behavior.
    ///
    /// Invalid profile syntax is returned in the report, not as infrastructure
    /// failure. This method performs no session or process work.
    ///
    /// # Errors
    ///
    /// Returns typed authorization, absence, or catalog failures.
    pub async fn lint(&self, name: &ProfileName) -> Result<ProfileLintReport, ProfileServiceError> {
        let operation = ApplicationOperation::LintDetectionProfile;
        self.run(operation, async {
            self.authorize().await?;
            let entry = self.load(name)?;
            Ok(lint_detection_profile(entry.profile(), self.patterns))
        })
        .await
    }

    /// Lints every active profile after one authorization and bounded catalog read.
    ///
    /// Reports remain in stable profile-name order. Invalid syntax is represented
    /// in each report rather than failing the complete operation. This method
    /// performs no session, process, PTY, or environment IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed authorization or catalog infrastructure failures.
    pub async fn lint_all(&self) -> Result<Vec<ProfileLintReport>, ProfileServiceError> {
        let operation = ApplicationOperation::LintDetectionProfile;
        self.run(operation, async {
            self.authorize().await?;
            let entries = self
                .catalog
                .all(self.limits.max_profiles)
                .map_err(ProfileServiceError::Infrastructure)?;
            Ok(entries
                .iter()
                .map(|entry| lint_detection_profile(entry.profile(), self.patterns))
                .collect())
        })
        .await
    }

    /// Tests one profile against explicit pre-extracted region views.
    ///
    /// The caller must provide the exact view required by each spatial rule;
    /// unavailable regions are reported explicitly. Terminal text is not logged
    /// or retained in the result. This method starts no session.
    ///
    /// # Errors
    ///
    /// Returns typed authorization, absence, capacity, catalog, or invalid
    /// active-profile failures.
    pub async fn test(
        &self,
        name: &ProfileName,
        samples: &[ProfileSampleView],
    ) -> Result<ProfileTestReport, ProfileServiceError> {
        let operation = ApplicationOperation::TestDetectionProfile;
        self.run(operation, async {
            self.authorize().await?;
            if samples.len() > self.limits.max_test_samples {
                return Err(ProfileServiceError::InvalidArgument {
                    field: "profile_test_samples",
                    rule: "sample count exceeds the configured limit",
                });
            }
            let entry = self.load(name)?;
            self.test_entry(entry.profile(), samples)
        })
        .await
    }

    async fn authorize(&self) -> Result<(), ProfileServiceError> {
        let decision = self
            .authorizer
            .authorize(
                AccessAction::ReadDetectionProfiles,
                &AccessResource::Configuration,
            )
            .await
            .map_err(ProfileServiceError::Infrastructure)?;
        match decision {
            AccessDecision::Permit => Ok(()),
            AccessDecision::Deny(AccessDenialReason::Unauthenticated) => {
                Err(ProfileServiceError::Unauthenticated)
            }
            AccessDecision::Deny(AccessDenialReason::Forbidden) => {
                Err(ProfileServiceError::PermissionDenied)
            }
        }
    }

    fn load(&self, name: &ProfileName) -> Result<ProfileCatalogEntry, ProfileServiceError> {
        self.catalog
            .get(name)
            .map_err(ProfileServiceError::Infrastructure)?
            .ok_or_else(|| ProfileServiceError::NotFound {
                profile: name.clone(),
            })
    }

    fn test_entry(
        &self,
        profile: &DetectionProfile,
        samples: &[ProfileSampleView],
    ) -> Result<ProfileTestReport, ProfileServiceError> {
        ProfileRuleEvaluator::new(self.patterns).evaluate(profile, samples)
    }

    async fn run<T>(
        &self,
        operation: ApplicationOperation,
        future: impl Future<Output = Result<T, ProfileServiceError>>,
    ) -> Result<T, ProfileServiceError> {
        let started_at = self.clock.now();
        let result = future.await;
        self.finish(operation, started_at, result.as_ref().err());
        result
    }

    fn finish(
        &self,
        operation: ApplicationOperation,
        started_at: TimestampMillis,
        error: Option<&ProfileServiceError>,
    ) {
        let finished_at = self.clock.now();
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::OperationDurationSeconds,
            value: finished_at
                .saturating_duration_since(started_at)
                .as_secs_f64(),
            operation: Some(operation),
        });
        let (event, level, application_error, port_operation, port_error_kind) = match error {
            None => (
                DiagnosticEventName::DetectionProfileOperationCompleted,
                DiagnosticLevel::Debug,
                None,
                None,
                None,
            ),
            Some(error) => {
                let category = error.category();
                if let Some(port) = error.port_error() {
                    (
                        DiagnosticEventName::DetectionProfileOperationFailed,
                        DiagnosticLevel::for_port_error(port.kind()),
                        Some(category),
                        Some(port.operation()),
                        Some(port.kind()),
                    )
                } else {
                    let event = if category.is_access_denial() {
                        DiagnosticEventName::AuthorizationDenied
                    } else {
                        DiagnosticEventName::DetectionProfileOperationRejected
                    };
                    (
                        event,
                        DiagnosticLevel::for_application_error(category),
                        Some(category),
                        None,
                        None,
                    )
                }
            }
        };
        self.telemetry.emit_diagnostic(&DiagnosticRecord {
            event,
            level,
            timestamp: finished_at,
            session_id: None,
            application_operation: Some(operation),
            application_error,
            operation: port_operation,
            error_kind: port_error_kind,
        });
    }
}

fn any_expression_matches(
    patterns: &dyn ProfilePatternEngine,
    profile: &ProfileName,
    expressions: &[PatternExpression],
    text: &str,
    location: impl Fn(usize) -> ProfileIssueLocation,
) -> Result<bool, ProfileServiceError> {
    for (index, expression) in expressions.iter().enumerate() {
        let matched = patterns
            .is_match(expression, text)
            .map_err(|error| invalid_profile(profile, location(index), error))?;
        if matched {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Lints one structurally valid profile through the configured pattern engine.
///
/// The returned report contains expected syntax/resource issues rather than
/// converting them to infrastructure failures. This function performs no IO,
/// starts no session, and does not panic.
pub fn lint_detection_profile(
    profile: &DetectionProfile,
    patterns: &dyn ProfilePatternEngine,
) -> ProfileLintReport {
    let mut issues = Vec::new();
    if profile.disabled() {
        issues.push(ProfileLintIssue {
            severity: ProfileIssueSeverity::Warning,
            code: ProfileIssueCode::Disabled,
            location: ProfileIssueLocation::Profile,
            offset: None,
        });
    }
    if !profile.matching().is_selectable() {
        issues.push(ProfileLintIssue {
            severity: ProfileIssueSeverity::Warning,
            code: ProfileIssueCode::NotAutomaticallySelectable,
            location: ProfileIssueLocation::Profile,
            offset: None,
        });
    } else if profile
        .matching()
        .signatures()
        .iter()
        .any(|signature| signature.binary().is_empty())
    {
        issues.push(ProfileLintIssue {
            severity: ProfileIssueSeverity::Warning,
            code: ProfileIssueCode::ArgumentsWithoutBinary,
            location: ProfileIssueLocation::Profile,
            offset: None,
        });
    }
    let mut argument_offset = 0;
    for command in profile.matching().signatures() {
        lint_expressions(
            patterns,
            command.arguments(),
            |index| ProfileIssueLocation::MatchArgument {
                index: argument_offset + index,
            },
            &mut issues,
        );
        argument_offset += command.arguments().len();
    }
    for (rule, prompt) in profile.foreground_rules().iter().enumerate() {
        lint_expressions(
            patterns,
            prompt.patterns(),
            |index| ProfileIssueLocation::PromptPattern { rule, index },
            &mut issues,
        );
        lint_expressions(
            patterns,
            prompt.excludes(),
            |index| ProfileIssueLocation::PromptExclude { rule, index },
            &mut issues,
        );
    }
    for (rule, background) in profile.background_rules().iter().enumerate() {
        lint_expressions(
            patterns,
            background.patterns(),
            |index| ProfileIssueLocation::BackgroundPattern { rule, index },
            &mut issues,
        );
        lint_expressions(
            patterns,
            background.excludes(),
            |index| ProfileIssueLocation::BackgroundExclude { rule, index },
            &mut issues,
        );
        lint_expressions(
            patterns,
            background.name_patterns(),
            |index| ProfileIssueLocation::BackgroundNamePattern { rule, index },
            &mut issues,
        );
    }
    ProfileLintReport {
        profile: profile.name().clone(),
        issues,
    }
}

fn lint_expressions(
    patterns: &dyn ProfilePatternEngine,
    expressions: &[PatternExpression],
    location: impl Fn(usize) -> ProfileIssueLocation,
    issues: &mut Vec<ProfileLintIssue>,
) {
    for (index, expression) in expressions.iter().enumerate() {
        let Err(error) = patterns.validate(expression) else {
            continue;
        };
        issues.push(ProfileLintIssue {
            severity: ProfileIssueSeverity::Error,
            code: issue_code(error.kind()),
            location: location(index),
            offset: error.offset(),
        });
    }
}

fn issue_code(kind: ProfilePatternErrorKind) -> ProfileIssueCode {
    match kind {
        ProfilePatternErrorKind::InvalidSyntax => ProfileIssueCode::InvalidPatternSyntax,
        ProfilePatternErrorKind::ResourceLimit => ProfileIssueCode::PatternResourceLimit,
    }
}

fn invalid_profile(
    profile: &ProfileName,
    location: ProfileIssueLocation,
    error: ProfilePatternError,
) -> ProfileServiceError {
    ProfileServiceError::InvalidActiveProfile {
        profile: profile.clone(),
        location,
        error,
    }
}

fn foreground_candidate_precedes(
    candidate: &ProfileTestSelection,
    selected: &ProfileTestSelection,
) -> bool {
    candidate.rule < selected.rule
        || (candidate.rule == selected.rule && candidate.confidence > selected.confidence)
}
