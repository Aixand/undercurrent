// SPDX-License-Identifier: Apache-2.0

//! Pure multi-axis foreground-evidence selection and rationale.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use crate::EvidenceSource;
use crate::{
    Confidence, DomainError, DomainResult, EvidenceAssessment, EvidenceDisposition,
    EvidencePriority, ExecutionObservation, ExecutionState, ForegroundObservation,
    IgnoredEvidenceReason, InteractionModality, InteractionObservation, InteractionState,
    InvalidValueReason, ObservationBasis, ObservationSemanticId, RuntimeConstraintObservation,
    RuntimeConstraintState, TimestampMillis,
};

/// Independent foreground axis claimed by one evidence item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ForegroundAxis {
    /// Mechanical work versus quiescence.
    Execution,
    /// Mechanical input availability or request.
    Interaction,
    /// Profile-owned explanation for limited progress.
    Constraint,
}

impl ForegroundAxis {
    /// Complete foreground-axis registry.
    pub const ALL: [Self; 3] = [Self::Execution, Self::Interaction, Self::Constraint];

    /// Returns the stable diagnostic and persisted name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Execution => "execution",
            Self::Interaction => "interaction",
            Self::Constraint => "constraint",
        }
    }
}

/// Mechanical interaction fields supplied before evidence provenance is known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InteractionClaim {
    state: InteractionState,
    modality: Option<InteractionModality>,
    semantic: Option<ObservationSemanticId>,
}

impl InteractionClaim {
    /// Constructs mechanically consistent interaction fields.
    ///
    /// Requested actionability is validated later together with the evidence
    /// disposition. This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns validation failure for missing or unexpected mechanics.
    pub fn new(
        state: InteractionState,
        modality: Option<InteractionModality>,
        semantic: Option<ObservationSemanticId>,
    ) -> DomainResult<Self> {
        match state {
            InteractionState::None if modality.is_some() || semantic.is_some() => {
                return Err(invalid_claim(
                    "absent interaction cannot carry modality or semantics",
                ));
            }
            InteractionState::Available | InteractionState::Requested if modality.is_none() => {
                return Err(invalid_claim(
                    "available and requested interaction require a modality",
                ));
            }
            InteractionState::Requested if semantic.is_none() => {
                return Err(invalid_claim(
                    "requested interaction requires profile-owned semantics",
                ));
            }
            InteractionState::None | InteractionState::Available | InteractionState::Requested => {}
        }
        Ok(Self {
            state,
            modality,
            semantic,
        })
    }

    /// Returns the mechanical interaction state.
    pub const fn state(&self) -> InteractionState {
        self.state
    }

    /// Returns required input mechanics when present.
    pub const fn modality(&self) -> Option<InteractionModality> {
        self.modality
    }

    /// Returns profile-owned semantics without interpreting them.
    pub const fn semantic(&self) -> Option<&ObservationSemanticId> {
        self.semantic.as_ref()
    }
}

/// Runtime-constraint fields supplied before evidence provenance is known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConstraintClaim {
    state: RuntimeConstraintState,
    semantic: Option<ObservationSemanticId>,
}

impl RuntimeConstraintClaim {
    /// Constructs mechanically consistent constraint fields.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns validation failure when state and semantics disagree.
    pub fn new(
        state: RuntimeConstraintState,
        semantic: Option<ObservationSemanticId>,
    ) -> DomainResult<Self> {
        if (state == RuntimeConstraintState::Present) != semantic.is_some() {
            return Err(invalid_claim(
                "present constraints require semantics and clear constraints omit them",
            ));
        }
        Ok(Self { state, semantic })
    }

    /// Returns whether the constraint is present.
    pub const fn state(&self) -> RuntimeConstraintState {
        self.state
    }

    /// Returns profile-owned semantics without interpreting them.
    pub const fn semantic(&self) -> Option<&ObservationSemanticId> {
        self.semantic.as_ref()
    }
}

/// One typed foreground claim before confidence and provenance are applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForegroundClaim {
    /// Mechanical execution state.
    Execution(ExecutionState),
    /// Mechanical interaction state and profile semantics.
    Interaction(InteractionClaim),
    /// Profile-owned runtime constraint.
    Constraint(RuntimeConstraintClaim),
}

impl ForegroundClaim {
    /// Returns the independent axis this claim may affect.
    pub const fn axis(&self) -> ForegroundAxis {
        match self {
            Self::Execution(_) => ForegroundAxis::Execution,
            Self::Interaction(_) => ForegroundAxis::Interaction,
            Self::Constraint(_) => ForegroundAxis::Constraint,
        }
    }
}

/// One bounded, payload-free foreground claim produced by an outer adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForegroundEvidence {
    source: EvidenceSource,
    priority: EvidencePriority,
    disposition: EvidenceDisposition,
    axis: ForegroundAxis,
    claim: Option<ForegroundClaim>,
    confidence: Confidence,
    observed_at: TimestampMillis,
}

impl ForegroundEvidence {
    /// Constructs one typed foreground claim.
    ///
    /// Unavailable evidence carries no claim or confidence. Requested
    /// interaction requires affirmative evidence. This function performs no IO
    /// and does not panic.
    ///
    /// # Errors
    ///
    /// Returns validation failure for inconsistent evidence fields.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source: EvidenceSource,
        priority: EvidencePriority,
        disposition: EvidenceDisposition,
        claim: ForegroundClaim,
        confidence: Confidence,
        observed_at: TimestampMillis,
    ) -> DomainResult<Self> {
        if disposition == EvidenceDisposition::Unavailable {
            return Err(DomainError::InvalidValue {
                field: "foreground_evidence",
                reason: InvalidValueReason::InvalidCombination {
                    rule: "unavailable evidence must use the unavailable constructor",
                },
            });
        }
        if matches!(
            &claim,
            ForegroundClaim::Interaction(interaction)
                if interaction.state() == InteractionState::Requested
        ) && disposition != EvidenceDisposition::Affirmative
        {
            return Err(DomainError::InvalidValue {
                field: "foreground_evidence",
                reason: InvalidValueReason::InvalidCombination {
                    rule: "requested interaction requires affirmative evidence",
                },
            });
        }
        let axis = claim.axis();
        Ok(Self {
            source,
            priority,
            disposition,
            axis,
            claim: Some(claim),
            confidence,
            observed_at,
        })
    }

    /// Constructs an explicit unavailable-source observation for one axis.
    pub fn unavailable(
        source: EvidenceSource,
        priority: EvidencePriority,
        axis: ForegroundAxis,
        observed_at: TimestampMillis,
    ) -> Self {
        Self {
            source,
            priority,
            disposition: EvidenceDisposition::Unavailable,
            axis,
            claim: None,
            confidence: Confidence::NONE,
            observed_at,
        }
    }

    /// Returns the stable evidence-source name.
    pub const fn source(&self) -> &EvidenceSource {
        &self.source
    }

    /// Returns semantic source precedence.
    pub const fn priority(&self) -> EvidencePriority {
        self.priority
    }

    /// Returns affirmative, inferred, or unavailable disposition.
    pub const fn disposition(&self) -> EvidenceDisposition {
        self.disposition
    }

    /// Returns the independent target axis.
    pub const fn axis(&self) -> ForegroundAxis {
        self.axis
    }

    /// Returns the claimed foreground mechanics when available.
    pub const fn claim(&self) -> Option<&ForegroundClaim> {
        self.claim.as_ref()
    }

    /// Returns source confidence.
    pub const fn confidence(&self) -> Confidence {
        self.confidence
    }

    /// Returns when the adapter observed this evidence.
    pub const fn observed_at(&self) -> TimestampMillis {
        self.observed_at
    }
}

/// One evidence item and the decision made about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssessedForegroundEvidence {
    evidence: ForegroundEvidence,
    assessment: EvidenceAssessment,
}

impl AssessedForegroundEvidence {
    /// Returns the original typed evidence.
    pub const fn evidence(&self) -> &ForegroundEvidence {
        &self.evidence
    }

    /// Returns how the decision treated this evidence.
    pub const fn assessment(&self) -> EvidenceAssessment {
        self.assessment
    }
}

/// Hard bounds and acceptance thresholds for one foreground decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForegroundDecisionPolicy {
    max_evidence_items: usize,
    max_evidence_age: Duration,
    minimum_confidence: Confidence,
}

impl ForegroundDecisionPolicy {
    /// Constructs a bounded foreground-decision policy.
    ///
    /// # Errors
    ///
    /// Returns validation failure for a zero item limit or freshness window.
    /// This function performs no IO and does not panic.
    pub fn new(
        max_evidence_items: usize,
        max_evidence_age: Duration,
        minimum_confidence: Confidence,
    ) -> DomainResult<Self> {
        if max_evidence_items == 0 || max_evidence_age.is_zero() {
            return Err(DomainError::InvalidValue {
                field: "foreground_decision_policy",
                reason: InvalidValueReason::InvalidCombination {
                    rule: "evidence item and freshness limits must be non-zero",
                },
            });
        }
        Ok(Self {
            max_evidence_items,
            max_evidence_age,
            minimum_confidence,
        })
    }

    /// Returns the maximum evidence items accepted per evaluation.
    pub const fn max_evidence_items(self) -> usize {
        self.max_evidence_items
    }

    /// Returns the maximum accepted age of evidence.
    pub const fn max_evidence_age(self) -> Duration {
        self.max_evidence_age
    }

    /// Returns the minimum confidence accepted for selection.
    pub const fn minimum_confidence(self) -> Confidence {
        self.minimum_confidence
    }

    /// Selects evidence independently for execution, interaction, and constraint.
    ///
    /// Priority wins within one axis; confidence and stable source identity
    /// break ties. Missing usable evidence produces safe defaults. Every item is
    /// retained with a payload-free assessment. This function performs no IO
    /// and does not panic.
    ///
    /// # Errors
    ///
    /// Returns capacity or duplicate source-axis failures.
    pub fn decide(
        self,
        evidence: Vec<ForegroundEvidence>,
        committed_at: TimestampMillis,
    ) -> DomainResult<ForegroundDecision> {
        if evidence.len() > self.max_evidence_items {
            return Err(DomainError::CapacityExceeded {
                resource: "foreground_evidence_items",
                limit: self.max_evidence_items,
            });
        }
        let distinct_sources = evidence
            .iter()
            .map(|item| (item.source(), item.axis()))
            .collect::<BTreeSet<_>>();
        if distinct_sources.len() != evidence.len() {
            return Err(DomainError::InvalidValue {
                field: "foreground_evidence_source",
                reason: InvalidValueReason::InvalidCombination {
                    rule: "each evidence source may report once per foreground axis",
                },
            });
        }

        let preliminary = evidence
            .iter()
            .map(|item| self.preliminary_assessment(item, committed_at))
            .collect::<Vec<_>>();
        let mut selected = BTreeMap::new();
        for axis in ForegroundAxis::ALL {
            if let Some((index, _)) = evidence
                .iter()
                .enumerate()
                .filter(|(index, item)| item.axis == axis && preliminary[*index].is_none())
                .min_by(|(_, left), (_, right)| {
                    (left.priority, Reverse(left.confidence), &left.source).cmp(&(
                        right.priority,
                        Reverse(right.confidence),
                        &right.source,
                    ))
                })
            {
                selected.insert(axis, index);
            }
        }

        let observation = ForegroundObservation::new(
            selected_axis_observation(
                &evidence,
                selected.get(&ForegroundAxis::Execution).copied(),
                committed_at,
            )?,
            selected_interaction_observation(
                &evidence,
                selected.get(&ForegroundAxis::Interaction).copied(),
                committed_at,
            )?,
            selected_constraint_observation(
                &evidence,
                selected.get(&ForegroundAxis::Constraint).copied(),
                committed_at,
            )?,
        );
        let assessed = evidence
            .into_iter()
            .enumerate()
            .map(|(index, evidence)| AssessedForegroundEvidence {
                assessment: if selected.get(&evidence.axis()) == Some(&index) {
                    EvidenceAssessment::Selected
                } else {
                    EvidenceAssessment::Ignored(
                        preliminary[index].unwrap_or(IgnoredEvidenceReason::LowerPrecedence),
                    )
                },
                evidence,
            })
            .collect();
        Ok(ForegroundDecision {
            observation,
            evidence: assessed,
        })
    }

    fn preliminary_assessment(
        self,
        evidence: &ForegroundEvidence,
        committed_at: TimestampMillis,
    ) -> Option<IgnoredEvidenceReason> {
        if evidence.disposition == EvidenceDisposition::Unavailable {
            return Some(IgnoredEvidenceReason::Unavailable);
        }
        // Check direction before measuring age: reversed timestamps saturate
        // to zero and would otherwise make future evidence look fresh.
        // See [eligibility ordering](../docs/internals/observations-and-profiles.md#selection-is-deterministic-within-each-axis).
        if evidence.observed_at > committed_at {
            return Some(IgnoredEvidenceReason::Future);
        }
        if committed_at.saturating_duration_since(evidence.observed_at) > self.max_evidence_age {
            return Some(IgnoredEvidenceReason::Stale);
        }
        if evidence.confidence < self.minimum_confidence {
            return Some(IgnoredEvidenceReason::BelowConfidence);
        }
        None
    }
}

/// Committed foreground observation and complete bounded rationale.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForegroundDecision {
    observation: ForegroundObservation,
    evidence: Vec<AssessedForegroundEvidence>,
}

impl ForegroundDecision {
    /// Constructs initial evidence-free state.
    pub const fn unknown(committed_at: TimestampMillis) -> Self {
        Self {
            observation: ForegroundObservation::unknown(committed_at),
            evidence: Vec::new(),
        }
    }

    /// Wraps already-validated foreground state without external rationale.
    pub const fn from_observation(observation: ForegroundObservation) -> Self {
        Self {
            observation,
            evidence: Vec::new(),
        }
    }

    /// Rehydrates a committed observation and its persisted evidence rationale.
    ///
    /// Source-axis pairs must be unique, each axis may select at most one item,
    /// and every selected claim must exactly support the corresponding compact
    /// observation. Axes without a selection retain their canonical safe
    /// default. Evidence-free explicit observations remain valid. This function
    /// performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns corrupt-state failure when persisted rationale and compact state
    /// disagree.
    pub fn rehydrate(
        observation: ForegroundObservation,
        evidence: Vec<(ForegroundEvidence, EvidenceAssessment)>,
    ) -> DomainResult<Self> {
        if evidence.is_empty() {
            return Ok(Self::from_observation(observation));
        }
        let mut sources = BTreeSet::new();
        let mut selected = BTreeMap::new();
        for (index, (item, assessment)) in evidence.iter().enumerate() {
            if !sources.insert((item.source(), item.axis())) {
                return Err(corrupt_decision(
                    "foreground evidence source-axis pairs must be unique",
                ));
            }
            if *assessment == EvidenceAssessment::Selected
                && selected.insert(item.axis(), index).is_some()
            {
                return Err(corrupt_decision(
                    "each foreground axis may select at most one evidence item",
                ));
            }
        }
        for axis in ForegroundAxis::ALL {
            match selected.get(&axis).copied() {
                Some(index) if !selected_evidence_matches(&observation, &evidence[index].0)? => {
                    return Err(corrupt_decision(
                        "selected foreground evidence must exactly support its observation axis",
                    ));
                }
                None if !axis_has_safe_default(&observation, axis) => {
                    return Err(corrupt_decision(
                        "an unselected foreground axis must retain its safe default",
                    ));
                }
                Some(_) | None => {}
            }
        }
        Ok(Self {
            observation,
            evidence: evidence
                .into_iter()
                .map(|(evidence, assessment)| AssessedForegroundEvidence {
                    evidence,
                    assessment,
                })
                .collect(),
        })
    }

    /// Returns compact committed foreground state.
    pub const fn observation(&self) -> &ForegroundObservation {
        &self.observation
    }

    /// Returns every assessed evidence item in adapter-supplied order.
    pub fn evidence(&self) -> &[AssessedForegroundEvidence] {
        &self.evidence
    }
}

fn selected_evidence_matches(
    observation: &ForegroundObservation,
    evidence: &ForegroundEvidence,
) -> DomainResult<bool> {
    let basis = match evidence.axis() {
        ForegroundAxis::Execution => observation.execution().basis(),
        ForegroundAxis::Interaction => observation.interaction().basis(),
        ForegroundAxis::Constraint => observation.constraint().basis(),
    };
    let expected_basis = selected_basis(evidence, basis.committed_at())?;
    if *basis != expected_basis {
        return Ok(false);
    }
    Ok(match evidence.claim() {
        Some(ForegroundClaim::Execution(state)) => observation.execution().state() == *state,
        Some(ForegroundClaim::Interaction(claim)) => {
            observation.interaction().state() == claim.state()
                && observation.interaction().modality() == claim.modality()
                && observation.interaction().semantic() == claim.semantic()
        }
        Some(ForegroundClaim::Constraint(claim)) => {
            observation.constraint().state() == claim.state()
                && observation.constraint().semantic() == claim.semantic()
        }
        None => false,
    })
}

fn axis_has_safe_default(observation: &ForegroundObservation, axis: ForegroundAxis) -> bool {
    match axis {
        ForegroundAxis::Execution => {
            *observation.execution()
                == ExecutionObservation::unknown(observation.execution().basis().committed_at())
        }
        ForegroundAxis::Interaction => {
            *observation.interaction()
                == InteractionObservation::none(observation.interaction().basis().committed_at())
        }
        ForegroundAxis::Constraint => {
            *observation.constraint()
                == RuntimeConstraintObservation::clear(
                    observation.constraint().basis().committed_at(),
                )
        }
    }
}

fn selected_axis_observation(
    evidence: &[ForegroundEvidence],
    selected: Option<usize>,
    committed_at: TimestampMillis,
) -> DomainResult<ExecutionObservation> {
    let Some(item) = selected.map(|index| &evidence[index]) else {
        return Ok(ExecutionObservation::unknown(committed_at));
    };
    let Some(ForegroundClaim::Execution(state)) = item.claim() else {
        return Err(corrupt_decision(
            "selected execution evidence has wrong axis",
        ));
    };
    ExecutionObservation::new(*state, selected_basis(item, committed_at)?)
}

fn selected_interaction_observation(
    evidence: &[ForegroundEvidence],
    selected: Option<usize>,
    committed_at: TimestampMillis,
) -> DomainResult<InteractionObservation> {
    let Some(item) = selected.map(|index| &evidence[index]) else {
        return Ok(InteractionObservation::none(committed_at));
    };
    let Some(ForegroundClaim::Interaction(claim)) = item.claim() else {
        return Err(corrupt_decision(
            "selected interaction evidence has wrong axis",
        ));
    };
    InteractionObservation::new(
        claim.state(),
        claim.modality(),
        claim.semantic().cloned(),
        selected_basis(item, committed_at)?,
    )
}

fn selected_constraint_observation(
    evidence: &[ForegroundEvidence],
    selected: Option<usize>,
    committed_at: TimestampMillis,
) -> DomainResult<RuntimeConstraintObservation> {
    let Some(item) = selected.map(|index| &evidence[index]) else {
        return Ok(RuntimeConstraintObservation::clear(committed_at));
    };
    let Some(ForegroundClaim::Constraint(claim)) = item.claim() else {
        return Err(corrupt_decision(
            "selected constraint evidence has wrong axis",
        ));
    };
    RuntimeConstraintObservation::new(
        claim.state(),
        claim.semantic().cloned(),
        selected_basis(item, committed_at)?,
    )
}

fn selected_basis(
    evidence: &ForegroundEvidence,
    committed_at: TimestampMillis,
) -> DomainResult<ObservationBasis> {
    ObservationBasis::new(
        evidence.confidence(),
        Some(evidence.source().clone()),
        evidence.disposition(),
        committed_at,
        Some(evidence.observed_at()),
    )
}

fn invalid_claim(rule: &'static str) -> DomainError {
    DomainError::InvalidValue {
        field: "foreground_claim",
        reason: InvalidValueReason::InvalidCombination { rule },
    }
}

fn corrupt_decision(rule: &'static str) -> DomainError {
    DomainError::CorruptState {
        scope: "foreground_decision",
        rule,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(value: &str) -> EvidenceSource {
        EvidenceSource::new(value).expect("valid source")
    }

    fn confidence(value: u16) -> Confidence {
        Confidence::from_basis_points(value).expect("valid confidence")
    }

    fn policy() -> ForegroundDecisionPolicy {
        ForegroundDecisionPolicy::new(16, Duration::from_secs(5), confidence(5_000))
            .expect("valid policy")
    }

    #[test]
    fn axes_are_selected_independently() {
        let decision = policy()
            .decide(
                vec![
                    ForegroundEvidence::new(
                        source("process"),
                        EvidencePriority::Process,
                        EvidenceDisposition::Affirmative,
                        ForegroundClaim::Execution(ExecutionState::Active),
                        confidence(8_000),
                        TimestampMillis::new(9),
                    )
                    .expect("valid execution evidence"),
                    ForegroundEvidence::new(
                        source("profile:codex"),
                        EvidencePriority::Terminal,
                        EvidenceDisposition::Inference,
                        ForegroundClaim::Interaction(
                            InteractionClaim::new(
                                InteractionState::Available,
                                Some(InteractionModality::Text),
                                Some(
                                    ObservationSemanticId::new("codex.composer")
                                        .expect("valid semantic"),
                                ),
                            )
                            .expect("valid interaction"),
                        ),
                        confidence(9_000),
                        TimestampMillis::new(9),
                    )
                    .expect("valid interaction evidence"),
                ],
                TimestampMillis::new(10),
            )
            .expect("decision succeeds");

        assert_eq!(
            decision.observation().execution().state(),
            ExecutionState::Active
        );
        assert_eq!(
            decision.observation().interaction().state(),
            InteractionState::Available
        );
        assert!(!decision.observation().interaction().is_actionable());
    }

    #[test]
    fn inferred_requested_interaction_is_rejected_at_admission() {
        let claim = ForegroundClaim::Interaction(
            InteractionClaim::new(
                InteractionState::Requested,
                Some(InteractionModality::Options),
                Some(ObservationSemanticId::new("codex.workspace_trust").expect("valid semantic")),
            )
            .expect("valid mechanics"),
        );
        assert!(
            ForegroundEvidence::new(
                source("profile:codex"),
                EvidencePriority::Terminal,
                EvidenceDisposition::Inference,
                claim,
                confidence(9_000),
                TimestampMillis::new(9),
            )
            .is_err()
        );
    }

    #[test]
    fn stale_requested_evidence_falls_back_to_absent_interaction() {
        let decision = policy()
            .decide(
                vec![
                    ForegroundEvidence::new(
                        source("profile:codex"),
                        EvidencePriority::Terminal,
                        EvidenceDisposition::Affirmative,
                        ForegroundClaim::Interaction(
                            InteractionClaim::new(
                                InteractionState::Requested,
                                Some(InteractionModality::Options),
                                Some(
                                    ObservationSemanticId::new("codex.workspace_trust")
                                        .expect("valid semantic"),
                                ),
                            )
                            .expect("valid interaction"),
                        ),
                        confidence(9_000),
                        TimestampMillis::new(1),
                    )
                    .expect("valid evidence"),
                ],
                TimestampMillis::new(10_000),
            )
            .expect("decision succeeds");

        assert_eq!(
            decision.observation().interaction().state(),
            InteractionState::None
        );
        assert_eq!(
            decision.evidence()[0].assessment(),
            EvidenceAssessment::Ignored(IgnoredEvidenceReason::Stale)
        );
    }
}
