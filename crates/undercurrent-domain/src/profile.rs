// SPDX-License-Identifier: Apache-2.0

//! Focused detection-profile vocabulary and deterministic matching invariants.
//!
//! Profiles describe how concrete adapters recognize a program and translate
//! terminal evidence. They do not read the environment, inspect a process, or
//! compile regular expressions. Outer adapters supply environment names and a
//! bounded pattern engine explicitly.

use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};
use std::num::NonZeroU16;
use std::time::Duration;

use crate::{
    Confidence, DomainError, DomainResult, ForegroundClaim, InteractionClaim, InteractionState,
    InvalidValueReason, ProfileName,
};

/// Version of the focused detection-profile schema accepted by this domain.
pub const DETECTION_PROFILE_SCHEMA_VERSION: crate::ReleaseVersion = crate::ReleaseVersion::Current;

/// Admission limits applied when constructing one detection profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DetectionProfileLimits {
    /// Maximum UTF-8 bytes in the optional human-facing description.
    pub max_description_bytes: usize,
    /// Maximum binary-name wildcard expressions.
    pub max_binary_patterns: usize,
    /// Maximum argument regular expressions.
    pub max_argument_patterns: usize,
    /// Maximum required environment-variable names.
    pub max_environment_names: usize,
    /// Maximum UTF-8 bytes in one environment-variable name.
    pub max_environment_name_bytes: usize,
    /// Maximum foreground-observation rules.
    pub max_foreground_rules: usize,
    /// Maximum background-work observation rules.
    pub max_background_rules: usize,
    /// Maximum include, exclude, or name expressions in one rule collection.
    pub max_patterns_per_rule: usize,
    /// Maximum UTF-8 bytes in one pattern expression.
    pub max_pattern_bytes: usize,
    /// Maximum structural trait needles in one collection.
    pub max_trait_needles: usize,
    /// Maximum UTF-8 bytes in one structural trait needle or label.
    pub max_trait_text_bytes: usize,
    /// Maximum delay before a profile-paced trailing submit key.
    pub max_input_submission_delay_millis: u64,
}

impl DetectionProfileLimits {
    /// Validates that every configured profile limit is nonzero.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed invalid-value error when any limit is zero.
    pub fn validate(self) -> DomainResult<Self> {
        let values = [
            ("max_description_bytes", self.max_description_bytes),
            ("max_binary_patterns", self.max_binary_patterns),
            ("max_argument_patterns", self.max_argument_patterns),
            ("max_environment_names", self.max_environment_names),
            (
                "max_environment_name_bytes",
                self.max_environment_name_bytes,
            ),
            ("max_foreground_rules", self.max_foreground_rules),
            ("max_background_rules", self.max_background_rules),
            ("max_patterns_per_rule", self.max_patterns_per_rule),
            ("max_pattern_bytes", self.max_pattern_bytes),
            ("max_trait_needles", self.max_trait_needles),
            ("max_trait_text_bytes", self.max_trait_text_bytes),
            (
                "max_input_submission_delay_millis",
                usize::try_from(self.max_input_submission_delay_millis).unwrap_or(usize::MAX),
            ),
        ];
        if let Some((field, _)) = values.into_iter().find(|(_, value)| *value == 0) {
            return Err(DomainError::InvalidValue {
                field,
                reason: InvalidValueReason::OutOfRange {
                    min: 1,
                    max: u64::MAX,
                },
            });
        }
        Ok(self)
    }
}

/// Default encoding for profile-selected prompt text.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum InputTextMode {
    /// Literal UTF-8 bytes.
    #[default]
    Raw,
    /// Explicit terminal bracketed paste.
    Paste,
}

/// Profile-owned text encoding and trailing-submit pacing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InputDeliveryPolicy {
    delay: Option<Duration>,
    text_mode: InputTextMode,
}

impl InputDeliveryPolicy {
    /// Returns immediate delivery without allocating or performing IO.
    pub const fn immediate() -> Self {
        Self {
            delay: None,
            text_mode: InputTextMode::Raw,
        }
    }

    /// Constructs a bounded nonzero trailing-submit pacing policy.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed validation error when `delay` is zero, exceeds
    /// `maximum`, or cannot be represented as whole milliseconds.
    pub fn paced_trailing_submit(delay: Duration, maximum: Duration) -> DomainResult<Self> {
        if delay.is_zero() {
            return Err(DomainError::InvalidValue {
                field: "input_submission_delay",
                reason: InvalidValueReason::OutOfRange {
                    min: 1,
                    max: duration_millis(maximum),
                },
            });
        }
        if delay > maximum || !delay.subsec_nanos().is_multiple_of(1_000_000) {
            return Err(DomainError::InvalidValue {
                field: "input_submission_delay",
                reason: InvalidValueReason::OutOfRange {
                    min: 1,
                    max: duration_millis(maximum),
                },
            });
        }
        Ok(Self {
            delay: Some(delay),
            text_mode: InputTextMode::Raw,
        })
    }

    /// Returns the intentional delay before a trailing Enter, when configured.
    pub const fn trailing_submit_delay(self) -> Option<Duration> {
        self.delay
    }

    /// Selects the encoding used only by automatic text items.
    pub const fn with_text_mode(mut self, mode: InputTextMode) -> Self {
        self.text_mode = mode;
        self
    }

    /// Returns the default encoding for automatic text items.
    pub const fn text_mode(self) -> InputTextMode {
        self.text_mode
    }

    fn validate(self, limits: DetectionProfileLimits) -> DomainResult<Self> {
        if let Some(delay) = self.delay {
            Self::paced_trailing_submit(
                delay,
                Duration::from_millis(limits.max_input_submission_delay_millis),
            )?;
        }
        Ok(self)
    }
}

/// Converts a duration to a saturating millisecond error-bound value.
fn duration_millis(value: Duration) -> u64 {
    u64::try_from(value.as_millis()).unwrap_or(u64::MAX)
}

/// Bounded opaque expression compiled by an outer regular-expression adapter.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PatternExpression(String);

impl PatternExpression {
    /// Constructs a non-empty, NUL-free expression within `max_bytes`.
    ///
    /// Syntax is deliberately not checked here because the domain does not
    /// depend on a concrete regular-expression implementation. This function
    /// performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed validation errors for empty, oversized, or NUL input.
    pub fn new(value: impl Into<String>, max_bytes: usize) -> DomainResult<Self> {
        let value = value.into();
        validate_bounded_text("pattern_expression", &value, max_bytes, false)?;
        Ok(Self(value))
    }

    /// Returns the expression for an authorized pattern adapter.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn byte_len(&self) -> usize {
        self.0.len()
    }
}

/// Bounded wildcard applied only to an executable basename.
///
/// `*` matches zero or more Unicode scalar values and `?` matches exactly one.
/// Matching uses bounded dynamic programming rather than recursive backtracking.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BinaryPattern(String);

impl BinaryPattern {
    /// Constructs one bounded binary wildcard expression.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed validation errors for empty, oversized, NUL, or path-like
    /// input. Binary rules intentionally match basenames, not paths.
    pub fn new(value: impl Into<String>, max_bytes: usize) -> DomainResult<Self> {
        let value = value.into();
        validate_bounded_text("binary_pattern", &value, max_bytes, false)?;
        if value.contains('/') || value.contains('\\') {
            return Err(DomainError::InvalidValue {
                field: "binary_pattern",
                reason: InvalidValueReason::PathLike,
            });
        }
        Ok(Self(value))
    }

    /// Returns the wildcard expression.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn byte_len(&self) -> usize {
        self.0.len()
    }

    /// Tests a basename with work proportional to pattern times input length.
    ///
    /// This function performs no IO and does not panic.
    pub fn is_match(&self, basename: &str) -> bool {
        self.is_match_with_case(basename, false)
    }

    fn is_match_with_case(&self, basename: &str, ignore_ascii_case: bool) -> bool {
        let pattern: Vec<char> = self.0.chars().collect();
        let input: Vec<char> = basename.chars().collect();
        let mut previous = vec![false; input.len() + 1];
        previous[0] = true;

        for token in pattern {
            let mut current = vec![false; input.len() + 1];
            match token {
                '*' => {
                    current[0] = previous[0];
                    for index in 1..=input.len() {
                        current[index] = previous[index] || current[index - 1];
                    }
                }
                '?' => {
                    current[1..].copy_from_slice(&previous[..input.len()]);
                }
                literal => {
                    for index in 1..=input.len() {
                        current[index] = previous[index - 1]
                            && (input[index - 1] == literal
                                || (ignore_ascii_case
                                    && input[index - 1].eq_ignore_ascii_case(&literal)));
                    }
                }
            }
            previous = current;
        }

        previous[input.len()]
    }
}

/// Validated environment-variable name used only as a presence predicate.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EnvironmentName(String);

impl EnvironmentName {
    /// Constructs an environment name without reading its value.
    ///
    /// Names cannot be empty, contain `=`, NUL, control characters, or
    /// whitespace. This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed validation error when the name is unsafe or oversized.
    pub fn new(value: impl Into<String>, max_bytes: usize) -> DomainResult<Self> {
        let value = value.into();
        validate_bounded_text("environment_name", &value, max_bytes, false)?;
        if let Some((index, character)) = value.char_indices().find(|(_, character)| {
            *character == '=' || character.is_control() || character.is_whitespace()
        }) {
            return Err(DomainError::InvalidValue {
                field: "environment_name",
                reason: InvalidValueReason::InvalidCharacter { index, character },
            });
        }
        Ok(Self(value))
    }

    /// Returns the validated name without exposing any environment value.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn byte_len(&self) -> usize {
        self.0.len()
    }
}

/// Platform-supplied executable and environment-name matching semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileMatchConvention {
    /// Exact, case-sensitive basename wildcard and environment-name matching.
    Literal,
    /// ASCII case-insensitive matching, also trying the name without one
    /// recognized Windows executable suffix (.exe, .com, .cmd, or .bat).
    /// Required environment names also ignore ASCII case. Argument matching
    /// remains unchanged.
    Windows,
}

/// Pure command facts supplied to standalone profile matching.
#[derive(Clone, PartialEq, Eq)]
pub struct ProfileMatchTarget {
    program: String,
    arguments: Vec<String>,
    environment_names: BTreeSet<EnvironmentName>,
    argument_bytes: usize,
    match_convention: ProfileMatchConvention,
}

impl ProfileMatchTarget {
    /// Constructs a bounded command target from adapter-supplied facts.
    ///
    /// The target never reads the current process environment. Argument values
    /// are intentionally redacted from `Debug`. This function performs no IO
    /// and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed validation or capacity errors for invalid or oversized
    /// program, arguments, or environment-name collections.
    pub fn new(
        program: impl Into<String>,
        arguments: Vec<String>,
        environment_names: BTreeSet<EnvironmentName>,
        max_command_bytes: usize,
        max_arguments: usize,
        max_environment_names: usize,
    ) -> DomainResult<Self> {
        let program = program.into();
        validate_bounded_text("profile_match_program", &program, max_command_bytes, false)?;
        if program.rsplit(['/', '\\']).next().is_none_or(str::is_empty) {
            return Err(DomainError::InvalidValue {
                field: "profile_match_program",
                reason: InvalidValueReason::PathLike,
            });
        }
        if arguments.len() > max_arguments {
            return Err(DomainError::CapacityExceeded {
                resource: "profile_match_arguments",
                limit: max_arguments,
            });
        }
        if environment_names.len() > max_environment_names {
            return Err(DomainError::CapacityExceeded {
                resource: "profile_match_environment_names",
                limit: max_environment_names,
            });
        }
        let argument_bytes = arguments.iter().try_fold(0usize, |total, argument| {
            if argument.contains('\0') {
                return Err(DomainError::InvalidValue {
                    field: "profile_match_argument",
                    reason: InvalidValueReason::InvalidCharacter {
                        index: argument.find('\0').unwrap_or(0),
                        character: '\0',
                    },
                });
            }
            total
                .checked_add(argument.len())
                .and_then(|value| value.checked_add(1))
                .ok_or(DomainError::CapacityExceeded {
                    resource: "profile_match_command_bytes",
                    limit: max_command_bytes,
                })
        })?;
        let total_bytes =
            program
                .len()
                .checked_add(argument_bytes)
                .ok_or(DomainError::CapacityExceeded {
                    resource: "profile_match_command_bytes",
                    limit: max_command_bytes,
                })?;
        if total_bytes > max_command_bytes {
            return Err(DomainError::CapacityExceeded {
                resource: "profile_match_command_bytes",
                limit: max_command_bytes,
            });
        }
        Ok(Self {
            program,
            arguments,
            environment_names,
            argument_bytes,
            match_convention: ProfileMatchConvention::Literal,
        })
    }

    /// Selects name semantics from adapter-supplied platform facts. This
    /// never changes the executable, arguments, environment, or launch behavior.
    pub fn with_match_convention(mut self, convention: ProfileMatchConvention) -> Self {
        self.match_convention = convention;
        self
    }

    fn matches_binary(&self, pattern: &BinaryPattern) -> bool {
        let basename = self.basename();
        if self.match_convention == ProfileMatchConvention::Literal {
            return pattern.is_match(basename);
        }
        pattern.is_match_with_case(basename, true)
            || basename.rsplit_once('.').is_some_and(|(stem, suffix)| {
                !stem.is_empty()
                    && ["exe", "com", "cmd", "bat"]
                        .iter()
                        .any(|ext| suffix.eq_ignore_ascii_case(ext))
                    && pattern.is_match_with_case(stem, true)
            })
    }

    fn matches_environment(&self, required: &BTreeSet<EnvironmentName>) -> bool {
        match self.match_convention {
            ProfileMatchConvention::Literal => required.is_subset(&self.environment_names),
            ProfileMatchConvention::Windows => required.iter().all(|required| {
                self.environment_names
                    .iter()
                    .any(|present| present.as_str().eq_ignore_ascii_case(required.as_str()))
            }),
        }
    }

    /// Returns the executable basename using both Unix and Windows separators.
    pub fn basename(&self) -> &str {
        self.program
            .rsplit(['/', '\\'])
            .next()
            .unwrap_or(&self.program)
    }

    /// Returns arguments for an authorized pattern adapter.
    pub fn arguments(&self) -> &[String] {
        &self.arguments
    }

    /// Returns present environment names without reading values.
    pub const fn environment_names(&self) -> &BTreeSet<EnvironmentName> {
        &self.environment_names
    }

    /// Returns the bounded argument byte count for diagnostics.
    pub const fn argument_bytes(&self) -> usize {
        self.argument_bytes
    }
}

impl Debug for ProfileMatchTarget {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProfileMatchTarget")
            .field("basename", &self.basename())
            .field("match_convention", &self.match_convention)
            .field("argument_count", &self.arguments.len())
            .field("argument_bytes", &self.argument_bytes)
            .field("environment_name_count", &self.environment_names.len())
            .finish()
    }
}

/// One conjunctive command signature used to select a profile.
///
/// Binary and argument expressions are alternatives within their respective
/// dimensions. When both dimensions are present, each must match. Required
/// environment names are an all-of condition. Profiles express unrelated
/// command shapes as separate signatures so their dimensions cannot combine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileMatchSignature {
    binary: Vec<BinaryPattern>,
    arguments: Vec<PatternExpression>,
    required_environment: BTreeSet<EnvironmentName>,
}

impl ProfileMatchSignature {
    /// Constructs one bounded, automatically selectable command signature.
    ///
    /// At least one binary or argument selector is required. This function
    /// performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed validation error for an unselectable signature or a
    /// capacity error when a collection exceeds configured limits.
    pub fn new(
        binary: Vec<BinaryPattern>,
        arguments: Vec<PatternExpression>,
        required_environment: BTreeSet<EnvironmentName>,
        limits: DetectionProfileLimits,
    ) -> DomainResult<Self> {
        let value = Self {
            binary,
            arguments,
            required_environment,
        };
        if !value.is_selectable() {
            return Err(DomainError::InvalidValue {
                field: "profile_match_signature",
                reason: InvalidValueReason::InvalidCombination {
                    rule: "a command signature requires a binary or argument selector",
                },
            });
        }
        value.validate_limits(limits)?;
        Ok(value)
    }

    /// Returns executable-basename rules.
    pub fn binary(&self) -> &[BinaryPattern] {
        &self.binary
    }

    /// Returns argument regular expressions.
    pub fn arguments(&self) -> &[PatternExpression] {
        &self.arguments
    }

    /// Returns environment names that must all be present.
    pub const fn required_environment(&self) -> &BTreeSet<EnvironmentName> {
        &self.required_environment
    }

    /// Returns whether the rule has a binary or argument selection dimension.
    pub fn is_selectable(&self) -> bool {
        !self.binary.is_empty() || !self.arguments.is_empty()
    }

    /// Tests the binary dimension when it is configured.
    pub fn binary_matches(&self, target: &ProfileMatchTarget) -> bool {
        self.binary
            .iter()
            .any(|pattern| target.matches_binary(pattern))
    }

    /// Tests all-of environment presence without reading any value.
    pub fn environment_matches(&self, target: &ProfileMatchTarget) -> bool {
        target.matches_environment(&self.required_environment)
    }

    fn validate_limits(&self, limits: DetectionProfileLimits) -> DomainResult<()> {
        let limits = limits.validate()?;
        check_capacity(
            "profile_binary_patterns",
            self.binary.len(),
            limits.max_binary_patterns,
        )?;
        check_capacity(
            "profile_argument_patterns",
            self.arguments.len(),
            limits.max_argument_patterns,
        )?;
        check_capacity(
            "profile_environment_names",
            self.required_environment.len(),
            limits.max_environment_names,
        )?;
        check_byte_lengths(
            "binary_pattern",
            self.binary.iter().map(BinaryPattern::byte_len),
            limits.max_pattern_bytes,
        )?;
        check_byte_lengths(
            "pattern_expression",
            self.arguments.iter().map(PatternExpression::byte_len),
            limits.max_pattern_bytes,
        )?;
        check_byte_lengths(
            "environment_name",
            self.required_environment
                .iter()
                .map(EnvironmentName::byte_len),
            limits.max_environment_name_bytes,
        )
    }
}

/// Alternative command signatures used to select a profile.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProfileMatchRules {
    signatures: Vec<ProfileMatchSignature>,
    flattened_binary: Vec<BinaryPattern>,
    flattened_arguments: Vec<PatternExpression>,
    common_required_environment: BTreeSet<EnvironmentName>,
}

impl ProfileMatchRules {
    /// Constructs rules from one command signature.
    ///
    /// Binary expressions remain alternatives, argument expressions remain
    /// alternatives, and configured dimensions are conjunctive. Empty rules
    /// are accepted for profiles selected explicitly by name. Definitions with
    /// unrelated command shapes use [`Self::from_signatures`]. This function
    /// performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns capacity errors when a rule collection exceeds configured limits.
    pub fn new(
        binary: Vec<BinaryPattern>,
        arguments: Vec<PatternExpression>,
        required_environment: BTreeSet<EnvironmentName>,
        limits: DetectionProfileLimits,
    ) -> DomainResult<Self> {
        if binary.is_empty() && arguments.is_empty() {
            let value = Self {
                signatures: Vec::new(),
                flattened_binary: binary,
                flattened_arguments: arguments,
                common_required_environment: required_environment,
            };
            value.validate_limits(limits)?;
            return Ok(value);
        }

        Self::from_signatures(
            vec![ProfileMatchSignature::new(
                binary,
                arguments,
                required_environment,
                limits,
            )?],
            limits,
        )
    }

    /// Constructs bounded alternative command signatures.
    ///
    /// Each signature is evaluated independently. A profile matches when at
    /// least one complete signature matches, preventing binary and argument
    /// dimensions from unrelated signatures from combining. An empty
    /// collection is accepted for profiles selected explicitly by name. This
    /// function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns capacity errors when the combined binary or argument
    /// declarations exceed the configured profile limits, or a nested
    /// signature exceeds its per-collection limits.
    pub fn from_signatures(
        signatures: Vec<ProfileMatchSignature>,
        limits: DetectionProfileLimits,
    ) -> DomainResult<Self> {
        let flattened_binary = signatures
            .iter()
            .flat_map(|signature| signature.binary.iter().cloned())
            .collect();
        let flattened_arguments = signatures
            .iter()
            .flat_map(|signature| signature.arguments.iter().cloned())
            .collect();
        let common_required_environment = signatures
            .first()
            .map(|first| {
                first
                    .required_environment
                    .iter()
                    .filter(|name| {
                        signatures
                            .iter()
                            .all(|signature| signature.required_environment.contains(*name))
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let value = Self {
            signatures,
            flattened_binary,
            flattened_arguments,
            common_required_environment,
        };
        value.validate_limits(limits)?;
        Ok(value)
    }

    /// Returns alternative command signatures in declaration order.
    pub fn signatures(&self) -> &[ProfileMatchSignature] {
        &self.signatures
    }

    /// Returns every executable-basename rule in signature declaration order.
    ///
    /// This bounded presentation view does not encode signature group
    /// boundaries and must not be used for matching.
    pub fn binary(&self) -> &[BinaryPattern] {
        &self.flattened_binary
    }

    /// Returns every argument expression in signature declaration order.
    ///
    /// This bounded presentation view does not encode signature group
    /// boundaries and must not be used for matching.
    pub fn arguments(&self) -> &[PatternExpression] {
        &self.flattened_arguments
    }

    /// Returns environment names required by every signature.
    ///
    /// Single-signature and explicit-only rules return their complete
    /// environment set. Multi-signature callers must inspect
    /// [`Self::signatures`] for exact per-signature requirements.
    pub const fn required_environment(&self) -> &BTreeSet<EnvironmentName> {
        &self.common_required_environment
    }

    /// Returns whether at least one command signature can select the profile.
    pub fn is_selectable(&self) -> bool {
        !self.signatures.is_empty()
    }

    /// Tests whether any declared binary selector matches the target.
    ///
    /// This dimension-only query does not evaluate complete signatures and
    /// must not be used by automatic profile selection.
    pub fn binary_matches(&self, target: &ProfileMatchTarget) -> bool {
        self.flattened_binary
            .iter()
            .any(|pattern| target.matches_binary(pattern))
    }

    /// Tests whether at least one signature's environment condition is met.
    ///
    /// Empty explicit-only rules evaluate their configured all-of environment
    /// set. This query does not evaluate the other dimensions of a signature.
    pub fn environment_matches(&self, target: &ProfileMatchTarget) -> bool {
        if self.signatures.is_empty() {
            return target.matches_environment(&self.common_required_environment);
        }
        self.signatures
            .iter()
            .any(|signature| signature.environment_matches(target))
    }

    fn validate_limits(&self, limits: DetectionProfileLimits) -> DomainResult<()> {
        let limits = limits.validate()?;
        check_capacity(
            "profile_binary_patterns",
            self.flattened_binary.len(),
            limits.max_binary_patterns,
        )?;
        check_capacity(
            "profile_argument_patterns",
            self.flattened_arguments.len(),
            limits.max_argument_patterns,
        )?;
        check_capacity(
            "profile_environment_names",
            self.common_required_environment.len(),
            limits.max_environment_names,
        )?;
        check_byte_lengths(
            "binary_pattern",
            self.flattened_binary.iter().map(BinaryPattern::byte_len),
            limits.max_pattern_bytes,
        )?;
        check_byte_lengths(
            "pattern_expression",
            self.flattened_arguments
                .iter()
                .map(PatternExpression::byte_len),
            limits.max_pattern_bytes,
        )?;
        check_byte_lengths(
            "environment_name",
            self.common_required_environment
                .iter()
                .map(EnvironmentName::byte_len),
            limits.max_environment_name_bytes,
        )?;
        for signature in &self.signatures {
            signature.validate_limits(limits)?;
        }
        Ok(())
    }
}

/// Program category that determines the conservative meaning of silence.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DetectionArchetype {
    /// Silence alone produces no foreground claim.
    #[default]
    Unclassified,
    /// POSIX foreground ownership distinguishes shell prompt from child work.
    LineOriented,
    /// Visible motion is meaningful while helper-process CPU may be ambient.
    AnimatedAgent,
    /// Quiet indicates a live service at rest rather than an input turn.
    Server,
}

impl DetectionArchetype {
    /// Returns whether process CPU is considered ambient for this category.
    pub const fn cpu_is_ambient(self) -> bool {
        match self {
            Self::AnimatedAgent | Self::Server => true,
            Self::LineOriented | Self::Unclassified => false,
        }
    }

    /// Returns whether kernel foreground ownership provides prompt evidence.
    pub const fn kernel_foreground_is_meaningful(self) -> bool {
        match self {
            Self::LineOriented => true,
            Self::AnimatedAgent | Self::Server | Self::Unclassified => false,
        }
    }
}

/// Screen-relative anchor for prompt and background-work evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ScreenAnchor {
    /// Restrict evidence to the cursor row.
    Cursor,
    /// Restrict evidence to rows within the supplied cursor distance.
    NearCursor { distance: u16 },
    /// Restrict evidence to the final non-empty row.
    Last,
    /// Accept evidence anywhere in the selected row range.
    Any,
}

/// Valid screen region with mutually exclusive row modes encoded by variants.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProfileRegion {
    /// Bottom-most rows with an optional column clip and explicit anchor.
    BottomRows {
        /// Nonzero row count.
        rows: NonZeroU16,
        /// Anchor applied within the selected rows.
        anchor: ScreenAnchor,
        /// Optional nonzero maximum columns.
        columns: Option<NonZeroU16>,
    },
    /// Rows immediately above the cursor.
    AboveCursor {
        /// Nonzero row count.
        rows: NonZeroU16,
        /// Optional nonzero maximum columns.
        columns: Option<NonZeroU16>,
    },
    /// Rows immediately below the cursor.
    BelowCursor {
        /// Nonzero row count.
        rows: NonZeroU16,
        /// Optional nonzero maximum columns.
        columns: Option<NonZeroU16>,
    },
}

/// Literal structural predicates evaluated in addition to regular expressions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PromptTraits {
    last_line_ends_with: Option<String>,
    last_line_starts_with: Option<String>,
    screen_contains: Vec<String>,
    screen_excludes: Vec<String>,
    cursor_at_end: Option<bool>,
}

impl PromptTraits {
    /// Constructs bounded all-of structural predicates.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns validation or capacity errors for oversized or empty literals.
    pub fn new(
        last_line_ends_with: Option<String>,
        last_line_starts_with: Option<String>,
        screen_contains: Vec<String>,
        screen_excludes: Vec<String>,
        cursor_at_end: Option<bool>,
        limits: DetectionProfileLimits,
    ) -> DomainResult<Self> {
        let value = Self {
            last_line_ends_with,
            last_line_starts_with,
            screen_contains,
            screen_excludes,
            cursor_at_end,
        };
        value.validate_limits(limits)?;
        Ok(value)
    }

    /// Returns whether no structural predicate is configured.
    pub fn is_empty(&self) -> bool {
        self.last_line_ends_with.is_none()
            && self.last_line_starts_with.is_none()
            && self.screen_contains.is_empty()
            && self.screen_excludes.is_empty()
            && self.cursor_at_end.is_none()
    }

    /// Tests all structural predicates against a pre-extracted bounded view.
    pub fn matches(&self, sample: &ProfileSampleView) -> bool {
        self.last_line_ends_with
            .as_ref()
            .is_none_or(|suffix| sample.last_line.ends_with(suffix))
            && self
                .last_line_starts_with
                .as_ref()
                .is_none_or(|prefix| sample.last_line.starts_with(prefix))
            && self
                .screen_contains
                .iter()
                .all(|needle| sample.text.contains(needle))
            && self
                .screen_excludes
                .iter()
                .all(|needle| !sample.text.contains(needle))
            && self
                .cursor_at_end
                .is_none_or(|expected| sample.cursor_at_end == expected)
    }

    fn validate_limits(&self, limits: DetectionProfileLimits) -> DomainResult<()> {
        let limits = limits.validate()?;
        check_capacity(
            "profile_trait_screen_contains",
            self.screen_contains.len(),
            limits.max_trait_needles,
        )?;
        check_capacity(
            "profile_trait_screen_excludes",
            self.screen_excludes.len(),
            limits.max_trait_needles,
        )?;
        for literal in self
            .last_line_ends_with
            .iter()
            .chain(self.last_line_starts_with.iter())
            .chain(self.screen_contains.iter())
            .chain(self.screen_excludes.iter())
        {
            validate_bounded_text(
                "profile_trait_literal",
                literal,
                limits.max_trait_text_bytes,
                false,
            )?;
        }
        Ok(())
    }
}

/// One profile-owned foreground-observation rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForegroundRule {
    claim: ForegroundClaim,
    patterns: Vec<PatternExpression>,
    excludes: Vec<PatternExpression>,
    traits: PromptTraits,
    confidence: Confidence,
    region: Option<ProfileRegion>,
}

impl ForegroundRule {
    /// Constructs one bounded foreground-observation rule.
    ///
    /// A rule requires at least one expression or structural predicate. This
    /// function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed validation or capacity errors for empty or oversized rules.
    pub fn new(
        claim: ForegroundClaim,
        patterns: Vec<PatternExpression>,
        excludes: Vec<PatternExpression>,
        traits: PromptTraits,
        confidence: Confidence,
        region: Option<ProfileRegion>,
        limits: DetectionProfileLimits,
    ) -> DomainResult<Self> {
        if patterns.is_empty() && traits.is_empty() {
            return Err(DomainError::InvalidValue {
                field: "foreground_rule",
                reason: InvalidValueReason::InvalidCombination {
                    rule: "a foreground rule requires patterns or structural traits",
                },
            });
        }
        let value = Self {
            claim,
            patterns,
            excludes,
            traits,
            confidence,
            region,
        };
        value.validate_limits(limits)?;
        Ok(value)
    }

    /// Returns the profile-owned typed foreground claim.
    pub const fn claim(&self) -> &ForegroundClaim {
        &self.claim
    }

    /// Returns positive-match expressions, any one of which may match.
    pub fn patterns(&self) -> &[PatternExpression] {
        &self.patterns
    }

    /// Returns exclusion expressions, any one of which vetoes the rule.
    pub fn excludes(&self) -> &[PatternExpression] {
        &self.excludes
    }

    /// Returns structural all-of predicates.
    pub const fn traits(&self) -> &PromptTraits {
        &self.traits
    }

    /// Returns the rule confidence.
    pub const fn confidence(&self) -> Confidence {
        self.confidence
    }

    /// Returns the required extracted screen region, if any.
    pub const fn region(&self) -> Option<&ProfileRegion> {
        self.region.as_ref()
    }

    fn validate_limits(&self, limits: DetectionProfileLimits) -> DomainResult<()> {
        let limits = limits.validate()?;
        check_capacity(
            "foreground_rule_patterns",
            self.patterns.len(),
            limits.max_patterns_per_rule,
        )?;
        check_capacity(
            "foreground_rule_excludes",
            self.excludes.len(),
            limits.max_patterns_per_rule,
        )?;
        check_byte_lengths(
            "pattern_expression",
            self.patterns
                .iter()
                .chain(self.excludes.iter())
                .map(PatternExpression::byte_len),
            limits.max_pattern_bytes,
        )?;
        self.traits.validate_limits(limits)
    }
}

/// Kind of TUI-visible background work a profile can surface as evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BackgroundWorkKind {
    /// One or more background process groups or shells.
    ProcessGroup,
    /// One or more delegated/background agents.
    Agent,
}

/// Profile-owned evidence rule for declared background work.
///
/// These rules produce WorkContext evidence only. They never override the
/// canonical foreground decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundObservationRule {
    kind: BackgroundWorkKind,
    patterns: Vec<PatternExpression>,
    excludes: Vec<PatternExpression>,
    name_patterns: Vec<PatternExpression>,
    confidence: Confidence,
    label: Option<String>,
    region: Option<ProfileRegion>,
}

impl BackgroundObservationRule {
    /// Constructs a bounded background-work evidence rule.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed validation or capacity errors. At least one count/presence
    /// pattern is required.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        kind: BackgroundWorkKind,
        patterns: Vec<PatternExpression>,
        excludes: Vec<PatternExpression>,
        name_patterns: Vec<PatternExpression>,
        confidence: Confidence,
        label: Option<String>,
        region: Option<ProfileRegion>,
        limits: DetectionProfileLimits,
    ) -> DomainResult<Self> {
        if patterns.is_empty() {
            return Err(DomainError::InvalidValue {
                field: "background_observation_patterns",
                reason: InvalidValueReason::Empty,
            });
        }
        let value = Self {
            kind,
            patterns,
            excludes,
            name_patterns,
            confidence,
            label,
            region,
        };
        value.validate_limits(limits)?;
        Ok(value)
    }

    /// Returns the background-work category.
    pub const fn kind(&self) -> BackgroundWorkKind {
        self.kind
    }

    /// Returns count or presence expressions.
    pub fn patterns(&self) -> &[PatternExpression] {
        &self.patterns
    }

    /// Returns exclusion expressions.
    pub fn excludes(&self) -> &[PatternExpression] {
        &self.excludes
    }

    /// Returns optional expressions that extract stable work names.
    pub fn name_patterns(&self) -> &[PatternExpression] {
        &self.name_patterns
    }

    /// Returns evidence confidence.
    pub const fn confidence(&self) -> Confidence {
        self.confidence
    }

    /// Returns an optional display label.
    pub fn label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    /// Returns the required extracted screen region, if any.
    pub const fn region(&self) -> Option<&ProfileRegion> {
        self.region.as_ref()
    }

    fn validate_limits(&self, limits: DetectionProfileLimits) -> DomainResult<()> {
        let limits = limits.validate()?;
        check_capacity(
            "background_observation_patterns",
            self.patterns.len(),
            limits.max_patterns_per_rule,
        )?;
        check_capacity(
            "background_observation_excludes",
            self.excludes.len(),
            limits.max_patterns_per_rule,
        )?;
        check_capacity(
            "background_observation_name_patterns",
            self.name_patterns.len(),
            limits.max_patterns_per_rule,
        )?;
        check_byte_lengths(
            "pattern_expression",
            self.patterns
                .iter()
                .chain(self.excludes.iter())
                .chain(self.name_patterns.iter())
                .map(PatternExpression::byte_len),
            limits.max_pattern_bytes,
        )?;
        if let Some(label) = &self.label {
            validate_bounded_text(
                "background_observation_label",
                label,
                limits.max_trait_text_bytes,
                false,
            )?;
        }
        Ok(())
    }
}

/// Rows excluded from terminal-motion evidence.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OutputExclusions {
    /// Rows ignored from the top edge.
    pub top_rows: u16,
    /// Rows ignored from the bottom edge.
    pub bottom_rows: u16,
}

/// One complete validated detection profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectionProfile {
    name: ProfileName,
    description: Option<String>,
    disabled: bool,
    matching_fallback: bool,
    matching: ProfileMatchRules,
    archetype: DetectionArchetype,
    input_delivery: InputDeliveryPolicy,
    baseline_interaction: Option<InteractionClaim>,
    foreground_rules: Vec<ForegroundRule>,
    background_rules: Vec<BackgroundObservationRule>,
    alternate_screen_is_normal: bool,
    silent_until_input: bool,
    output_exclusions: OutputExclusions,
}

impl DetectionProfile {
    /// Constructs a complete bounded detection profile.
    ///
    /// This function performs no IO and does not compile expressions. The
    /// application lint use case checks expression syntax through a pattern
    /// port before an adapter activates a profile. It does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed validation or capacity errors for oversized fields.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: ProfileName,
        description: Option<String>,
        disabled: bool,
        matching: ProfileMatchRules,
        archetype: DetectionArchetype,
        input_delivery: InputDeliveryPolicy,
        baseline_interaction: Option<InteractionClaim>,
        foreground_rules: Vec<ForegroundRule>,
        background_rules: Vec<BackgroundObservationRule>,
        alternate_screen_is_normal: bool,
        silent_until_input: bool,
        output_exclusions: OutputExclusions,
        limits: DetectionProfileLimits,
    ) -> DomainResult<Self> {
        let limits = limits.validate()?;
        // Nested values may have been built with looser limits. Revalidate at
        // this final boundary so composing valid pieces cannot bypass the
        // receiving profile's budget. See [profile admission](../docs/internals/observations-and-profiles.md#validate-rules-and-input-policy).
        let input_delivery = input_delivery.validate(limits)?;
        if let Some(description) = &description {
            validate_bounded_text(
                "profile_description",
                description,
                limits.max_description_bytes,
                true,
            )?;
        }
        if baseline_interaction
            .as_ref()
            .is_some_and(|claim| claim.state() != InteractionState::Available)
        {
            return Err(DomainError::InvalidValue {
                field: "profile_baseline_interaction",
                reason: InvalidValueReason::InvalidCombination {
                    rule: "a profile baseline may make input available but never requested",
                },
            });
        }
        check_capacity(
            "profile_foreground_rules",
            foreground_rules.len(),
            limits.max_foreground_rules,
        )?;
        check_capacity(
            "profile_background_rules",
            background_rules.len(),
            limits.max_background_rules,
        )?;
        matching.validate_limits(limits)?;
        for rule in &foreground_rules {
            rule.validate_limits(limits)?;
        }
        for background in &background_rules {
            background.validate_limits(limits)?;
        }
        Ok(Self {
            name,
            description,
            disabled,
            matching_fallback: false,
            matching,
            archetype,
            input_delivery,
            baseline_interaction,
            foreground_rules,
            background_rules,
            alternate_screen_is_normal,
            silent_until_input,
            output_exclusions,
        })
    }

    /// Returns the stable profile name.
    pub const fn name(&self) -> &ProfileName {
        &self.name
    }

    /// Returns the optional human-facing description.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Returns whether automatic matching must ignore the profile.
    pub const fn disabled(&self) -> bool {
        self.disabled
    }

    /// Marks this profile as eligible only when no non-fallback profile matches.
    /// Explicit profile selection is unaffected.
    pub fn with_matching_fallback(mut self, matching_fallback: bool) -> Self {
        self.matching_fallback = matching_fallback;
        self
    }

    /// Returns whether automatic selection treats this profile as a fallback.
    pub const fn matching_fallback(&self) -> bool {
        self.matching_fallback
    }

    /// Returns command-selection rules.
    pub const fn matching(&self) -> &ProfileMatchRules {
        &self.matching
    }

    /// Returns the conservative program category.
    pub const fn archetype(&self) -> DetectionArchetype {
        self.archetype
    }

    /// Returns the profile-owned terminal input delivery policy.
    pub const fn input_delivery(&self) -> InputDeliveryPolicy {
        self.input_delivery
    }

    /// Returns the conservative interaction assumed while this profile is active.
    ///
    /// Baselines are restricted to non-actionable availability. They never
    /// replace a matching affirmative foreground rule.
    pub const fn baseline_interaction(&self) -> Option<&InteractionClaim> {
        self.baseline_interaction.as_ref()
    }

    /// Returns profile-owned foreground-observation rules.
    pub fn foreground_rules(&self) -> &[ForegroundRule] {
        &self.foreground_rules
    }

    /// Returns background WorkContext evidence rules.
    pub fn background_rules(&self) -> &[BackgroundObservationRule] {
        &self.background_rules
    }

    /// Returns whether alternate-screen mode is expected for this program.
    pub const fn alternate_screen_is_normal(&self) -> bool {
        self.alternate_screen_is_normal
    }

    /// Returns whether the program may legitimately emit nothing before input.
    pub const fn silent_until_input(&self) -> bool {
        self.silent_until_input
    }

    /// Returns output rows excluded from motion evidence.
    pub const fn output_exclusions(&self) -> OutputExclusions {
        self.output_exclusions
    }
}

/// Pre-extracted bounded screen view used by standalone profile testing.
#[derive(Clone, PartialEq, Eq)]
pub struct ProfileSampleView {
    region: Option<ProfileRegion>,
    text: String,
    last_line: String,
    cursor_at_end: bool,
}

impl ProfileSampleView {
    /// Constructs a pre-extracted screen view for one exact profile region.
    ///
    /// Raw text is intentionally redacted from `Debug`. This function performs
    /// no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed validation errors for NUL or oversized text.
    pub fn new(
        region: Option<ProfileRegion>,
        text: impl Into<String>,
        last_line: impl Into<String>,
        cursor_at_end: bool,
        max_bytes: usize,
    ) -> DomainResult<Self> {
        let text = text.into();
        let last_line = last_line.into();
        validate_bounded_text("profile_sample_text", &text, max_bytes, true)?;
        validate_bounded_text("profile_sample_last_line", &last_line, max_bytes, true)?;
        Ok(Self {
            region,
            text,
            last_line,
            cursor_at_end,
        })
    }

    /// Returns the exact region represented by this view.
    pub const fn region(&self) -> Option<&ProfileRegion> {
        self.region.as_ref()
    }

    /// Returns complete extracted text for an authorized pattern adapter.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Returns the final extracted line for an authorized pattern adapter.
    pub fn last_line(&self) -> &str {
        &self.last_line
    }

    /// Returns whether the cursor is at the trimmed end of its row.
    pub const fn cursor_at_end(&self) -> bool {
        self.cursor_at_end
    }
}

impl Debug for ProfileSampleView {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProfileSampleView")
            .field("region", &self.region)
            .field("text_bytes", &self.text.len())
            .field("last_line_bytes", &self.last_line.len())
            .field("cursor_at_end", &self.cursor_at_end)
            .finish()
    }
}

fn validate_bounded_text(
    field: &'static str,
    value: &str,
    max_bytes: usize,
    allow_empty: bool,
) -> DomainResult<()> {
    if max_bytes == 0 {
        return Err(DomainError::InvalidValue {
            field,
            reason: InvalidValueReason::OutOfRange {
                min: 1,
                max: u64::MAX,
            },
        });
    }
    if !allow_empty && value.is_empty() {
        return Err(DomainError::InvalidValue {
            field,
            reason: InvalidValueReason::Empty,
        });
    }
    if value.len() > max_bytes {
        return Err(DomainError::InvalidValue {
            field,
            reason: InvalidValueReason::TooLong { max: max_bytes },
        });
    }
    if let Some(index) = value.find('\0') {
        return Err(DomainError::InvalidValue {
            field,
            reason: InvalidValueReason::InvalidCharacter {
                index,
                character: '\0',
            },
        });
    }
    Ok(())
}

fn check_capacity(resource: &'static str, actual: usize, limit: usize) -> DomainResult<()> {
    if actual > limit {
        return Err(DomainError::CapacityExceeded { resource, limit });
    }
    Ok(())
}

fn check_byte_lengths(
    field: &'static str,
    lengths: impl IntoIterator<Item = usize>,
    limit: usize,
) -> DomainResult<()> {
    if lengths.into_iter().any(|length| length > limit) {
        return Err(DomainError::InvalidValue {
            field,
            reason: InvalidValueReason::TooLong { max: limit },
        });
    }
    Ok(())
}
