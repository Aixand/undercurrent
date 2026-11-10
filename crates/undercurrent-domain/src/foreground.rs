// SPDX-License-Identifier: Apache-2.0

//! Independent best-effort foreground observations and safety invariants.

use std::fmt::{Display, Formatter};
use std::time::Duration;

use crate::{
    DomainError, DomainResult, EvidenceDisposition, EvidenceSource, InvalidValueReason,
    TimestampMillis,
};

const MAX_SEMANTIC_ID_BYTES: usize = 128;

/// Confidence represented as basis points in the inclusive range 0..=10,000.
///
/// Integer representation keeps equality, persistence, and protocol conversion
/// deterministic while retaining two decimal places of percentage precision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Confidence(u16);

impl Confidence {
    /// Complete absence of confidence.
    pub const NONE: Self = Self(0);
    /// Maximum confidence.
    pub const CERTAIN: Self = Self(10_000);

    /// Constructs confidence from basis points.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::InvalidValue`] above 10,000. This function
    /// performs no IO and does not panic.
    pub fn from_basis_points(value: u16) -> DomainResult<Self> {
        if value > Self::CERTAIN.0 {
            return Err(DomainError::InvalidValue {
                field: "confidence_basis_points",
                reason: InvalidValueReason::OutOfRange {
                    min: 0,
                    max: u64::from(Self::CERTAIN.0),
                },
            });
        }
        Ok(Self(value))
    }

    /// Returns confidence as basis points.
    ///
    /// This function performs no IO and does not panic.
    pub const fn basis_points(self) -> u16 {
        self.0
    }
}

/// What the foreground process appears to be doing mechanically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ExecutionState {
    /// Observable work or terminal motion is occurring.
    Active,
    /// The process is alive without current evidence of work.
    Quiescent,
    /// Available evidence cannot establish execution state.
    Unknown,
}

impl Display for ExecutionState {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Active => "active",
            Self::Quiescent => "quiescent",
            Self::Unknown => "unknown",
        })
    }
}

/// Mechanical availability of an input surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum InteractionState {
    /// No current interaction surface has been observed.
    None,
    /// Input can be delivered, but the process has not affirmatively requested it.
    Available,
    /// Fresh affirmative evidence says the process is requesting input now.
    Requested,
}

impl Display for InteractionState {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::None => "none",
            Self::Available => "available",
            Self::Requested => "requested",
        })
    }
}

/// Mechanical form of input accepted by an observed interaction surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum InteractionModality {
    /// Free-form non-secret text.
    Text,
    /// One choice from a presented set of options.
    Options,
    /// Secret text whose contents must not be observed or retained.
    Secret,
    /// Pager, focus, or cursor navigation.
    Navigation,
}

impl Display for InteractionModality {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Text => "text",
            Self::Options => "options",
            Self::Secret => "secret",
            Self::Navigation => "navigation",
        })
    }
}

/// Whether a profile-owned runtime constraint is currently present.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RuntimeConstraintState {
    /// No current constraint has been observed.
    Clear,
    /// A profile-owned constraint currently limits progress.
    Present,
}

/// Validated profile-owned semantic identifier.
///
/// IDs have at least two lowercase ASCII segments, for example
/// `codex.workspace_trust`. The domain validates their shape but never assigns
/// product meaning to them.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObservationSemanticId(String);

impl ObservationSemanticId {
    /// Constructs a bounded namespaced semantic identifier.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns validation failure for an oversized identifier or for segments
    /// outside the lowercase ASCII identifier grammar.
    pub fn new(value: impl Into<String>) -> DomainResult<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(DomainError::InvalidValue {
                field: "observation_semantic_id",
                reason: InvalidValueReason::Empty,
            });
        }
        if value.len() > MAX_SEMANTIC_ID_BYTES {
            return Err(DomainError::InvalidValue {
                field: "observation_semantic_id",
                reason: InvalidValueReason::TooLong {
                    max: MAX_SEMANTIC_ID_BYTES,
                },
            });
        }
        let segments = value.split('.').collect::<Vec<_>>();
        if segments.len() < 2 || segments.iter().any(|segment| !valid_segment(segment)) {
            return Err(DomainError::InvalidValue {
                field: "observation_semantic_id",
                reason: InvalidValueReason::InvalidCombination {
                    rule: "semantic IDs require two or more lowercase ASCII identifier segments",
                },
            });
        }
        Ok(Self(value))
    }

    /// Returns the stable profile-owned value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for ObservationSemanticId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Confidence, provenance, and timing shared by one committed observation axis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservationBasis {
    confidence: Confidence,
    source: Option<EvidenceSource>,
    disposition: EvidenceDisposition,
    committed_at: TimestampMillis,
    evidence_observed_at: Option<TimestampMillis>,
}

impl ObservationBasis {
    /// Constructs validated observation provenance.
    ///
    /// Affirmative and inferred observations require a source and an evidence
    /// timestamp. An unavailable basis carries neither and has zero confidence.
    /// Evidence cannot postdate the commit. This function performs no IO and
    /// does not panic.
    ///
    /// # Errors
    ///
    /// Returns validation failure for inconsistent provenance or timestamps.
    pub fn new(
        confidence: Confidence,
        source: Option<EvidenceSource>,
        disposition: EvidenceDisposition,
        committed_at: TimestampMillis,
        evidence_observed_at: Option<TimestampMillis>,
    ) -> DomainResult<Self> {
        match disposition {
            EvidenceDisposition::Affirmative | EvidenceDisposition::Inference
                if source.is_none() || evidence_observed_at.is_none() =>
            {
                return Err(DomainError::InvalidValue {
                    field: "observation_basis",
                    reason: InvalidValueReason::InvalidCombination {
                        rule: "available evidence requires a source and observation time",
                    },
                });
            }
            EvidenceDisposition::Unavailable
                if source.is_some()
                    || evidence_observed_at.is_some()
                    || confidence != Confidence::NONE =>
            {
                return Err(DomainError::InvalidValue {
                    field: "observation_basis",
                    reason: InvalidValueReason::InvalidCombination {
                        rule: "unavailable observations carry no source, timestamp, or confidence",
                    },
                });
            }
            EvidenceDisposition::Affirmative
            | EvidenceDisposition::Inference
            | EvidenceDisposition::Unavailable => {}
        }
        if evidence_observed_at.is_some_and(|observed_at| observed_at > committed_at) {
            return Err(DomainError::InvalidValue {
                field: "observation_basis",
                reason: InvalidValueReason::InvalidCombination {
                    rule: "observation evidence cannot follow its commit",
                },
            });
        }
        Ok(Self {
            confidence,
            source,
            disposition,
            committed_at,
            evidence_observed_at,
        })
    }

    /// Returns an evidence-free basis for an unknown or absent observation.
    pub const fn unavailable(committed_at: TimestampMillis) -> Self {
        Self {
            confidence: Confidence::NONE,
            source: None,
            disposition: EvidenceDisposition::Unavailable,
            committed_at,
            evidence_observed_at: None,
        }
    }

    /// Returns confidence in this observation axis.
    pub const fn confidence(&self) -> Confidence {
        self.confidence
    }

    /// Returns contributing source when evidence was available.
    pub const fn source(&self) -> Option<&EvidenceSource> {
        self.source.as_ref()
    }

    /// Returns affirmative, inferred, or unavailable disposition.
    pub const fn disposition(&self) -> EvidenceDisposition {
        self.disposition
    }

    /// Returns when this observation axis was committed.
    pub const fn committed_at(&self) -> TimestampMillis {
        self.committed_at
    }

    /// Returns when the selected evidence was observed.
    pub const fn evidence_observed_at(&self) -> Option<TimestampMillis> {
        self.evidence_observed_at
    }
}

/// One committed best-effort execution observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionObservation {
    state: ExecutionState,
    basis: ObservationBasis,
}

impl ExecutionObservation {
    /// Constructs a validated execution observation.
    ///
    /// Unknown execution uses an unavailable or inferred basis; affirmative
    /// evidence must make an active or quiescent claim. This function performs
    /// no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns validation failure for an affirmative unknown state.
    pub fn new(state: ExecutionState, basis: ObservationBasis) -> DomainResult<Self> {
        if state == ExecutionState::Unknown
            && basis.disposition() == EvidenceDisposition::Affirmative
        {
            return Err(DomainError::InvalidValue {
                field: "execution_observation",
                reason: InvalidValueReason::InvalidCombination {
                    rule: "affirmative execution evidence must claim active or quiescent",
                },
            });
        }
        Ok(Self { state, basis })
    }

    /// Returns an evidence-free unknown execution observation.
    pub const fn unknown(committed_at: TimestampMillis) -> Self {
        Self {
            state: ExecutionState::Unknown,
            basis: ObservationBasis::unavailable(committed_at),
        }
    }

    /// Returns the committed execution state.
    pub const fn state(&self) -> ExecutionState {
        self.state
    }

    /// Returns confidence, provenance, and timing.
    pub const fn basis(&self) -> &ObservationBasis {
        &self.basis
    }
}

/// One committed best-effort interaction observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InteractionObservation {
    state: InteractionState,
    modality: Option<InteractionModality>,
    semantic: Option<ObservationSemanticId>,
    basis: ObservationBasis,
}

impl InteractionObservation {
    /// Constructs a validated interaction observation.
    ///
    /// Available and requested interactions require a modality. Requested
    /// interaction additionally requires affirmative evidence and a profile-owned
    /// semantic ID. This constructor does not enforce a maximum evidence age;
    /// [`crate::ForegroundDecisionPolicy`] applies freshness during selection.
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns validation failure for inconsistent mechanics or unsafe
    /// requested interaction.
    pub fn new(
        state: InteractionState,
        modality: Option<InteractionModality>,
        semantic: Option<ObservationSemanticId>,
        basis: ObservationBasis,
    ) -> DomainResult<Self> {
        match state {
            InteractionState::None if modality.is_some() || semantic.is_some() => {
                return Err(DomainError::InvalidValue {
                    field: "interaction_observation",
                    reason: InvalidValueReason::InvalidCombination {
                        rule: "absent interaction cannot carry modality or semantics",
                    },
                });
            }
            InteractionState::Available | InteractionState::Requested if modality.is_none() => {
                return Err(DomainError::InvalidValue {
                    field: "interaction_modality",
                    reason: InvalidValueReason::InvalidCombination {
                        rule: "available and requested interaction require a modality",
                    },
                });
            }
            InteractionState::Requested
                if semantic.is_none()
                    || basis.disposition() != EvidenceDisposition::Affirmative =>
            {
                return Err(DomainError::InvalidValue {
                    field: "interaction_request",
                    reason: InvalidValueReason::InvalidCombination {
                        rule: "requested interaction requires affirmative evidence and semantics",
                    },
                });
            }
            InteractionState::None | InteractionState::Available | InteractionState::Requested => {}
        }
        Ok(Self {
            state,
            modality,
            semantic,
            basis,
        })
    }

    /// Returns an evidence-free absent interaction observation.
    pub const fn none(committed_at: TimestampMillis) -> Self {
        Self {
            state: InteractionState::None,
            modality: None,
            semantic: None,
            basis: ObservationBasis::unavailable(committed_at),
        }
    }

    /// Returns the committed interaction state.
    pub const fn state(&self) -> InteractionState {
        self.state
    }

    /// Returns the input mechanics when an interaction surface is present.
    pub const fn modality(&self) -> Option<InteractionModality> {
        self.modality
    }

    /// Returns profile-owned semantics without interpreting them.
    pub const fn semantic(&self) -> Option<&ObservationSemanticId> {
        self.semantic.as_ref()
    }

    /// Returns confidence, provenance, and timing.
    pub const fn basis(&self) -> &ObservationBasis {
        &self.basis
    }

    /// Returns whether an orchestrator may consider responding to this claim.
    ///
    /// This checks requested state and affirmative disposition only. Callers
    /// still need current process/profile evidence and their own authorization
    /// and response policy; no clock is consulted here.
    pub const fn is_actionable(&self) -> bool {
        matches!(self.state, InteractionState::Requested)
            && matches!(self.basis.disposition, EvidenceDisposition::Affirmative)
    }
}

/// One committed best-effort runtime-constraint observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConstraintObservation {
    state: RuntimeConstraintState,
    semantic: Option<ObservationSemanticId>,
    basis: ObservationBasis,
}

impl RuntimeConstraintObservation {
    /// Constructs a validated runtime-constraint observation.
    ///
    /// Present constraints require profile-owned semantics. Clear constraints
    /// carry no semantic ID. This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns validation failure for inconsistent state and semantics.
    pub fn new(
        state: RuntimeConstraintState,
        semantic: Option<ObservationSemanticId>,
        basis: ObservationBasis,
    ) -> DomainResult<Self> {
        if (state == RuntimeConstraintState::Present) != semantic.is_some() {
            return Err(DomainError::InvalidValue {
                field: "runtime_constraint_observation",
                reason: InvalidValueReason::InvalidCombination {
                    rule: "present constraints require semantics and clear constraints omit them",
                },
            });
        }
        Ok(Self {
            state,
            semantic,
            basis,
        })
    }

    /// Returns an evidence-free clear constraint observation.
    pub const fn clear(committed_at: TimestampMillis) -> Self {
        Self {
            state: RuntimeConstraintState::Clear,
            semantic: None,
            basis: ObservationBasis::unavailable(committed_at),
        }
    }

    /// Returns whether a runtime constraint is present.
    pub const fn state(&self) -> RuntimeConstraintState {
        self.state
    }

    /// Returns profile-owned semantics without interpreting them.
    pub const fn semantic(&self) -> Option<&ObservationSemanticId> {
        self.semantic.as_ref()
    }

    /// Returns confidence, provenance, and timing.
    pub const fn basis(&self) -> &ObservationBasis {
        &self.basis
    }
}

/// Compact committed foreground state carried by session summaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForegroundObservation {
    execution: ExecutionObservation,
    interaction: InteractionObservation,
    constraint: RuntimeConstraintObservation,
}

impl ForegroundObservation {
    /// Combines independently validated observation axes without additional IO.
    pub const fn new(
        execution: ExecutionObservation,
        interaction: InteractionObservation,
        constraint: RuntimeConstraintObservation,
    ) -> Self {
        Self {
            execution,
            interaction,
            constraint,
        }
    }

    /// Returns initial evidence-free foreground state.
    pub const fn unknown(committed_at: TimestampMillis) -> Self {
        Self::new(
            ExecutionObservation::unknown(committed_at),
            InteractionObservation::none(committed_at),
            RuntimeConstraintObservation::clear(committed_at),
        )
    }

    /// Returns the independent execution observation.
    pub const fn execution(&self) -> &ExecutionObservation {
        &self.execution
    }

    /// Returns the independent interaction observation.
    pub const fn interaction(&self) -> &InteractionObservation {
        &self.interaction
    }

    /// Returns the independent runtime-constraint observation.
    pub const fn constraint(&self) -> &RuntimeConstraintObservation {
        &self.constraint
    }

    /// Returns the newest axis commit represented by this compact observation.
    ///
    /// Independent axes may retain different commit times when one axis is
    /// preserved across an update. This function performs no IO and does not
    /// panic.
    pub fn committed_at(&self) -> TimestampMillis {
        [
            self.execution.basis().committed_at(),
            self.interaction.basis().committed_at(),
            self.constraint.basis().committed_at(),
        ]
        .into_iter()
        .max()
        .expect("foreground observations always contain three axes")
    }

    /// Returns the age of the oldest selected axis evidence, when any.
    ///
    /// Reversed timestamps saturate to zero. This function performs no IO and
    /// does not panic.
    pub fn evidence_age(&self, now: TimestampMillis) -> Option<Duration> {
        [
            self.execution.basis().evidence_observed_at(),
            self.interaction.basis().evidence_observed_at(),
            self.constraint.basis().evidence_observed_at(),
        ]
        .into_iter()
        .flatten()
        .map(|observed_at| now.saturating_duration_since(observed_at))
        .max()
    }
}

fn valid_segment(segment: &str) -> bool {
    let mut characters = segment.chars();
    characters
        .next()
        .is_some_and(|character| character.is_ascii_lowercase())
        && characters.all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || matches!(character, '_' | '-')
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source() -> EvidenceSource {
        EvidenceSource::new("profile:codex").expect("valid source")
    }

    fn basis(disposition: EvidenceDisposition) -> ObservationBasis {
        ObservationBasis::new(
            Confidence::from_basis_points(9_000).expect("valid confidence"),
            Some(source()),
            disposition,
            TimestampMillis::new(20),
            Some(TimestampMillis::new(19)),
        )
        .expect("valid basis")
    }

    #[test]
    fn semantic_ids_are_namespaced_and_bounded() {
        assert_eq!(
            ObservationSemanticId::new("codex.workspace_trust")
                .expect("valid semantic")
                .as_str(),
            "codex.workspace_trust"
        );
        assert!(ObservationSemanticId::new("permission").is_err());
        assert!(ObservationSemanticId::new("Codex.permission").is_err());
        assert!(ObservationSemanticId::new("codex..permission").is_err());
    }

    #[test]
    fn available_interaction_is_not_actionable() {
        let observation = InteractionObservation::new(
            InteractionState::Available,
            Some(InteractionModality::Text),
            Some(ObservationSemanticId::new("codex.composer").expect("valid semantic")),
            basis(EvidenceDisposition::Inference),
        )
        .expect("valid availability");

        assert!(!observation.is_actionable());
    }

    #[test]
    fn requested_interaction_requires_affirmative_named_evidence() {
        assert!(
            InteractionObservation::new(
                InteractionState::Requested,
                Some(InteractionModality::Options),
                Some(ObservationSemanticId::new("codex.workspace_trust").expect("valid semantic")),
                basis(EvidenceDisposition::Inference),
            )
            .is_err()
        );
        let requested = InteractionObservation::new(
            InteractionState::Requested,
            Some(InteractionModality::Options),
            Some(ObservationSemanticId::new("codex.workspace_trust").expect("valid semantic")),
            basis(EvidenceDisposition::Affirmative),
        )
        .expect("valid request");
        assert!(requested.is_actionable());
    }

    #[test]
    fn constraint_semantics_are_independent_from_interaction() {
        let foreground = ForegroundObservation::new(
            ExecutionObservation::new(
                ExecutionState::Quiescent,
                basis(EvidenceDisposition::Inference),
            )
            .expect("valid execution"),
            InteractionObservation::none(TimestampMillis::new(20)),
            RuntimeConstraintObservation::new(
                RuntimeConstraintState::Present,
                Some(ObservationSemanticId::new("codex.rate_limit").expect("valid semantic")),
                basis(EvidenceDisposition::Affirmative),
            )
            .expect("valid constraint"),
        );

        assert_eq!(foreground.interaction().state(), InteractionState::None);
        assert_eq!(
            foreground.constraint().state(),
            RuntimeConstraintState::Present
        );
    }
}
