// SPDX-License-Identifier: Apache-2.0

//! Strict, bounded YAML boundary. Domain constructors remain the validation authority.

use serde::Deserialize;
use std::{num::NonZeroU16, time::Duration};
use undercurrent_domain::*;

pub(crate) const MAX_PROFILE_BYTES: usize = 256 * 1024;
pub(crate) type ProfileResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(crate) fn decode(
    source: &str,
    limits: DetectionProfileLimits,
) -> ProfileResult<DetectionProfile> {
    if source.len() > MAX_PROFILE_BYTES {
        return Err("profile exceeds 256 KiB".into());
    }
    // The serde layer intentionally ignores some unknown tags. Reject them at the
    // event boundary, before deserialization can discard that information.
    use granit_parser::{Event, Parser};
    let mut parser = Parser::new_from_str(source);
    let mut depth = 0usize;
    let mut events = 0usize;
    let mut documents = 0usize;
    while let Some(event) = parser.next_event() {
        let (event, _) = event?;
        events += 1;
        if events > 32_768 {
            return Err("profile exceeds YAML event limit".into());
        }
        match &event {
            Event::Scalar(_, _, anchor, tag)
            | Event::SequenceStart(_, anchor, tag)
            | Event::MappingStart(_, anchor, tag)
                if *anchor != 0 || tag.is_some() =>
            {
                return Err("YAML tags and anchors are not supported".into());
            }
            Event::Alias(_) => return Err("YAML aliases are not supported".into()),
            _ => {}
        }
        match event {
            Event::DocumentStart(_) => {
                documents += 1;
                if documents > 1 {
                    return Err("expected exactly one YAML document".into());
                }
            }
            Event::SequenceStart(..) | Event::MappingStart(..) => {
                depth += 1;
                if depth > 32 {
                    return Err("profile exceeds YAML depth limit".into());
                }
            }
            Event::SequenceEnd | Event::MappingEnd => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    let options = serde_saphyr::options! {
        duplicate_keys: serde_saphyr::DuplicateKeyPolicy::Error,
        strict_booleans: true,
        with_snippet: false,
        budget: serde_saphyr::budget! {
            max_depth: 32,
            max_events: 32_768,
            max_nodes: 16_384,
            max_total_scalar_bytes: MAX_PROFILE_BYTES,
            max_aliases: 0,
            max_anchors: 0,
            max_merge_keys: 0,
            max_documents: 1,
        },
    };
    let dto: ProfileYaml = serde_saphyr::from_str_with_options(source, options)?;
    if dto.schema_version != undercurrent_protocol::ReleaseVersion::Current {
        return Err("unsupported profile schema_version (expected 0.1.0-beta)".into());
    }
    Ok(dto.build(limits)?)
}

macro_rules! vocabulary {
    ($name:ident => $domain:ident { $($variant:ident),+ $(,)? }) => {
        #[derive(Deserialize)]
        #[serde(rename_all = "snake_case")]
        enum $name { $($variant),+ }
        impl From<$name> for $domain {
            fn from(value: $name) -> Self {
                match value { $($name::$variant => Self::$variant),+ }
            }
        }
    };
}
vocabulary!(Archetype => DetectionArchetype { Unclassified, LineOriented, AnimatedAgent, Server });
vocabulary!(Execution => ExecutionState { Active, Quiescent, Unknown });
vocabulary!(Interaction => InteractionState { None, Available, Requested });
vocabulary!(Modality => InteractionModality { Text, Options, Secret, Navigation });
vocabulary!(Constraint => RuntimeConstraintState { Clear, Present });
vocabulary!(WorkKind => BackgroundWorkKind { ProcessGroup, Agent });

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileYaml {
    schema_version: undercurrent_protocol::ReleaseVersion,
    name: String,
    description: Option<String>,
    #[serde(default)]
    disabled: bool,
    #[serde(default)]
    matching_fallback: bool,
    #[serde(default)]
    matching: Vec<Signature>,
    archetype: Archetype,
    input_delivery: Delivery,
    baseline_interaction: Option<InteractionYaml>,
    #[serde(default)]
    foreground_rules: Vec<Foreground>,
    #[serde(default)]
    background_rules: Vec<Background>,
    #[serde(default)]
    alternate_screen_is_normal: bool,
    #[serde(default)]
    silent_until_input: bool,
    #[serde(default)]
    output_exclusions: Exclusions,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Signature {
    #[serde(default)]
    binary: Vec<String>,
    #[serde(default)]
    arguments: Vec<String>,
    #[serde(default)]
    required_environment: Vec<String>,
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Delivery {
    Immediate {
        #[serde(default)]
        text_mode: undercurrent_protocol::ProfileTextModeDto,
    },
    PacedTrailingSubmit {
        delay_millis: u64,
        #[serde(default)]
        text_mode: undercurrent_protocol::ProfileTextModeDto,
    },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InteractionYaml {
    state: Interaction,
    modality: Option<Modality>,
    semantic: Option<String>,
}
impl InteractionYaml {
    fn build(self) -> DomainResult<InteractionClaim> {
        InteractionClaim::new(
            self.state.into(),
            self.modality.map(Into::into),
            self.semantic.map(ObservationSemanticId::new).transpose()?,
        )
    }
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Claim {
    Execution {
        state: Execution,
    },
    Interaction {
        state: Interaction,
        modality: Option<Modality>,
        semantic: Option<String>,
    },
    Constraint {
        state: Constraint,
        semantic: Option<String>,
    },
}
impl Claim {
    fn build(self) -> DomainResult<ForegroundClaim> {
        Ok(match self {
            Self::Execution { state } => ForegroundClaim::Execution(state.into()),
            Self::Interaction {
                state,
                modality,
                semantic,
            } => ForegroundClaim::Interaction(
                InteractionYaml {
                    state,
                    modality,
                    semantic,
                }
                .build()?,
            ),
            Self::Constraint { state, semantic } => {
                ForegroundClaim::Constraint(RuntimeConstraintClaim::new(
                    state.into(),
                    semantic.map(ObservationSemanticId::new).transpose()?,
                )?)
            }
        })
    }
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Region {
    BottomRows {
        rows: NonZeroU16,
        anchor: Anchor,
        columns: Option<NonZeroU16>,
    },
    AboveCursor {
        rows: NonZeroU16,
        columns: Option<NonZeroU16>,
    },
    BelowCursor {
        rows: NonZeroU16,
        columns: Option<NonZeroU16>,
    },
}
impl From<Region> for ProfileRegion {
    fn from(value: Region) -> Self {
        match value {
            Region::BottomRows {
                rows,
                anchor,
                columns,
            } => Self::BottomRows {
                rows,
                anchor: anchor.into(),
                columns,
            },
            Region::AboveCursor { rows, columns } => Self::AboveCursor { rows, columns },
            Region::BelowCursor { rows, columns } => Self::BelowCursor { rows, columns },
        }
    }
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Anchor {
    Any {},
    Cursor {},
    Last {},
    NearCursor { distance: u16 },
}
impl From<Anchor> for ScreenAnchor {
    fn from(value: Anchor) -> Self {
        match value {
            Anchor::Any {} => Self::Any,
            Anchor::Cursor {} => Self::Cursor,
            Anchor::Last {} => Self::Last,
            Anchor::NearCursor { distance } => Self::NearCursor { distance },
        }
    }
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Traits {
    last_line_ends_with: Option<String>,
    last_line_starts_with: Option<String>,
    screen_contains: Vec<String>,
    screen_excludes: Vec<String>,
    cursor_at_end: Option<bool>,
}
impl Traits {
    fn build(self, limits: DetectionProfileLimits) -> DomainResult<PromptTraits> {
        PromptTraits::new(
            self.last_line_ends_with,
            self.last_line_starts_with,
            self.screen_contains,
            self.screen_excludes,
            self.cursor_at_end,
            limits,
        )
    }
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Exclusions {
    top_rows: u16,
    bottom_rows: u16,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Foreground {
    claim: Claim,
    #[serde(default)]
    patterns: Vec<String>,
    #[serde(default)]
    excludes: Vec<String>,
    #[serde(default)]
    traits: Traits,
    confidence_basis_points: u16,
    region: Option<Region>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Background {
    kind: WorkKind,
    patterns: Vec<String>,
    #[serde(default)]
    excludes: Vec<String>,
    #[serde(default)]
    name_patterns: Vec<String>,
    confidence_basis_points: u16,
    label: Option<String>,
    region: Option<Region>,
}
fn expressions(
    values: Vec<String>,
    limits: DetectionProfileLimits,
) -> DomainResult<Vec<PatternExpression>> {
    values
        .into_iter()
        .map(|value| PatternExpression::new(value, limits.max_pattern_bytes))
        .collect()
}
impl ProfileYaml {
    fn build(self, limits: DetectionProfileLimits) -> DomainResult<DetectionProfile> {
        let signatures = self
            .matching
            .into_iter()
            .map(|s| {
                ProfileMatchSignature::new(
                    s.binary
                        .into_iter()
                        .map(|v| BinaryPattern::new(v, limits.max_pattern_bytes))
                        .collect::<DomainResult<_>>()?,
                    expressions(s.arguments, limits)?,
                    s.required_environment
                        .into_iter()
                        .map(|v| EnvironmentName::new(v, limits.max_environment_name_bytes))
                        .collect::<DomainResult<_>>()?,
                    limits,
                )
            })
            .collect::<DomainResult<_>>()?;
        let foreground = self
            .foreground_rules
            .into_iter()
            .map(|r| {
                ForegroundRule::new(
                    r.claim.build()?,
                    expressions(r.patterns, limits)?,
                    expressions(r.excludes, limits)?,
                    r.traits.build(limits)?,
                    Confidence::from_basis_points(r.confidence_basis_points)?,
                    r.region.map(Into::into),
                    limits,
                )
            })
            .collect::<DomainResult<_>>()?;
        let background = self
            .background_rules
            .into_iter()
            .map(|r| {
                BackgroundObservationRule::new(
                    r.kind.into(),
                    expressions(r.patterns, limits)?,
                    expressions(r.excludes, limits)?,
                    expressions(r.name_patterns, limits)?,
                    Confidence::from_basis_points(r.confidence_basis_points)?,
                    r.label,
                    r.region.map(Into::into),
                    limits,
                )
            })
            .collect::<DomainResult<_>>()?;
        DetectionProfile::new(
            ProfileName::new(self.name)?,
            self.description,
            self.disabled,
            ProfileMatchRules::from_signatures(signatures, limits)?,
            self.archetype.into(),
            match self.input_delivery {
                Delivery::Immediate { text_mode } => {
                    InputDeliveryPolicy::immediate().with_text_mode(text_mode_domain(text_mode))
                }
                Delivery::PacedTrailingSubmit {
                    delay_millis,
                    text_mode,
                } => InputDeliveryPolicy::paced_trailing_submit(
                    Duration::from_millis(delay_millis),
                    Duration::from_millis(limits.max_input_submission_delay_millis),
                )?
                .with_text_mode(text_mode_domain(text_mode)),
            },
            self.baseline_interaction
                .map(InteractionYaml::build)
                .transpose()?,
            foreground,
            background,
            self.alternate_screen_is_normal,
            self.silent_until_input,
            OutputExclusions {
                top_rows: self.output_exclusions.top_rows,
                bottom_rows: self.output_exclusions.bottom_rows,
            },
            limits,
        )
        .map(|profile| profile.with_matching_fallback(self.matching_fallback))
    }
}

fn text_mode_domain(mode: undercurrent_protocol::ProfileTextModeDto) -> InputTextMode {
    match mode {
        undercurrent_protocol::ProfileTextModeDto::Raw => InputTextMode::Raw,
        undercurrent_protocol::ProfileTextModeDto::Paste => InputTextMode::Paste,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile_builtins::tests::limits;
    const SIMPLE: &str = "schema_version: 0.1.0-beta\nname: example\narchetype: unclassified\ninput_delivery: {type: immediate}\n";

    #[test]
    fn text_mode_defaults_raw_and_accepts_explicit_paste() {
        assert_eq!(
            decode(SIMPLE, limits())
                .unwrap()
                .input_delivery()
                .text_mode(),
            InputTextMode::Raw
        );
        for delivery in [
            "{type: immediate, text_mode: paste}",
            "{type: paced_trailing_submit, delay_millis: 120, text_mode: paste}",
        ] {
            let profile = decode(&SIMPLE.replace("{type: immediate}", delivery), limits()).unwrap();
            assert_eq!(profile.input_delivery().text_mode(), InputTextMode::Paste);
        }
        assert!(
            decode(
                &SIMPLE.replace("{type: immediate}", "{type: immediate, text_mode: guess}"),
                limits()
            )
            .is_err()
        );
    }

    #[test]
    fn fallback_metadata_defaults_false_and_requires_a_boolean() {
        assert!(!decode(SIMPLE, limits()).unwrap().matching_fallback());
        assert!(
            decode(&format!("{SIMPLE}matching_fallback: true\n"), limits())
                .unwrap()
                .matching_fallback()
        );
        assert!(decode(&format!("{SIMPLE}matching_fallback: sometimes\n"), limits()).is_err());
    }

    #[test]
    fn rejects_ambiguous_or_unsupported_yaml() {
        for source in [
            SIMPLE.replace("schema_version: 0.1.0-beta", "schema_version: 99"),
            format!("{SIMPLE}unexpected: true\n"),
            format!("{SIMPLE}name: duplicate\n"),
            SIMPLE.replace("example", "!custom example"),
            SIMPLE.replace("example", "&anchor example"),
            format!("{SIMPLE}description: *missing\n"),
            format!("{SIMPLE}<<: {{disabled: true}}\n"),
            format!("{SIMPLE}---\n{SIMPLE}"),
            SIMPLE.replace("immediate", "immediate, typo: true"),
            SIMPLE.replace("unclassified", "typo"),
            format!("{SIMPLE}disabled: yes\n"),
        ] {
            assert!(decode(&source, limits()).is_err(), "accepted: {source}");
        }
    }
    #[test]
    fn validates_domain_values_and_resource_bounds() {
        assert!(decode(SIMPLE, limits()).is_ok());
        for source in [
            SIMPLE.replace("example", "bad name"),
            SIMPLE.replace(
                "{type: immediate}",
                "{type: paced_trailing_submit, delay_millis: 0}",
            ),
            SIMPLE.replace(
                "{type: immediate}",
                "{type: paced_trailing_submit, delay_millis: 2001}",
            ),
            format!(
                "{SIMPLE}baseline_interaction: {{state: requested, modality: text, semantic: example.prompt}}\n"
            ),
            format!(
                "{SIMPLE}description: {}value{}\n",
                "[".repeat(100),
                "]".repeat(100)
            ),
            " ".repeat(MAX_PROFILE_BYTES + 1),
        ] {
            assert!(decode(&source, limits()).is_err());
        }
    }
}
