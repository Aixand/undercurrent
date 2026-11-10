// SPDX-License-Identifier: Apache-2.0

//! Unversioned current aggregate state with independent foreground axes.
//!
//! Lifecycle revision is restored separately from aggregate revision so a rename
//! cannot masquerade as a lifecycle change. Foreground execution, interaction,
//! and constraint observations retain their bases and assessed evidence; decoding
//! checks axis/claim agreement and then rehydrates the complete domain session.

use serde::{Deserialize, Serialize};
use undercurrent_domain::{
    AssessedForegroundEvidence, Confidence, EvidenceAssessment, EvidenceDisposition,
    EvidencePriority, EvidenceSource, ExecutionObservation, ExecutionState, ForegroundAxis,
    ForegroundClaim, ForegroundDecision, ForegroundEvidence, ForegroundObservation,
    IgnoredEvidenceReason, InteractionClaim, InteractionModality, InteractionObservation,
    InteractionState, ObservationBasis, ObservationSemanticId, Revision, RuntimeConstraintClaim,
    RuntimeConstraintObservation, RuntimeConstraintState, Session, TimestampMillis,
};

use super::aggregate::{SessionCodecError, SessionStateRecord as AggregateSessionStateRecord};

/// Encodes the current complete aggregate format.
///
/// Non-foreground aggregate fields and authoritative foreground state are
/// encoded together by the sole release-0.1.0-beta envelope. This function performs no
/// IO and does not panic.
pub(super) fn encode_state(session: &Session) -> Result<SessionStateRecord, SessionCodecError> {
    let state = session.state();
    Ok(SessionStateRecord {
        lifecycle_revision: state.lifecycle_revision.value(),
        foreground: (&state.foreground).into(),
        foreground_decision: (&state.foreground_decision).into(),
        aggregate: state.try_into()?,
    })
}

/// Decodes the current aggregate format through every domain constructor.
///
/// This consumes an already-deserialized state record. The 0.1.0-beta envelope decoder
/// checks the record byte limit before JSON parsing. This function performs no
/// IO and does not panic.
pub(super) fn decode_state(record: SessionStateRecord) -> Result<Session, SessionCodecError> {
    let foreground = record.foreground.try_into()?;
    let foreground_decision = record.foreground_decision.try_into()?;
    let mut state = record
        .aggregate
        .into_state(foreground, foreground_decision)?;
    state.lifecycle_revision = Revision::new(record.lifecycle_revision);
    Session::rehydrate(state).map_err(Into::into)
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SessionStateRecord {
    lifecycle_revision: u64,
    foreground: ForegroundObservationRecord,
    foreground_decision: ForegroundDecisionRecord,
    aggregate: AggregateSessionStateRecord,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ForegroundObservationRecord {
    execution: ExecutionObservationRecord,
    interaction: InteractionObservationRecord,
    constraint: ConstraintObservationRecord,
}

impl From<&ForegroundObservation> for ForegroundObservationRecord {
    fn from(value: &ForegroundObservation) -> Self {
        Self {
            execution: ExecutionObservationRecord {
                state: value.execution().state().into(),
                basis: value.execution().basis().into(),
            },
            interaction: InteractionObservationRecord {
                state: value.interaction().state().into(),
                modality: value.interaction().modality().map(Into::into),
                semantic: value
                    .interaction()
                    .semantic()
                    .map(|semantic| semantic.as_str().to_owned()),
                basis: value.interaction().basis().into(),
            },
            constraint: ConstraintObservationRecord {
                state: value.constraint().state().into(),
                semantic: value
                    .constraint()
                    .semantic()
                    .map(|semantic| semantic.as_str().to_owned()),
                basis: value.constraint().basis().into(),
            },
        }
    }
}

impl TryFrom<ForegroundObservationRecord> for ForegroundObservation {
    type Error = SessionCodecError;

    fn try_from(value: ForegroundObservationRecord) -> Result<Self, Self::Error> {
        Ok(Self::new(
            ExecutionObservation::new(
                value.execution.state.into(),
                value.execution.basis.try_into()?,
            )?,
            InteractionObservation::new(
                value.interaction.state.into(),
                value.interaction.modality.map(Into::into),
                value
                    .interaction
                    .semantic
                    .map(ObservationSemanticId::new)
                    .transpose()?,
                value.interaction.basis.try_into()?,
            )?,
            RuntimeConstraintObservation::new(
                value.constraint.state.into(),
                value
                    .constraint
                    .semantic
                    .map(ObservationSemanticId::new)
                    .transpose()?,
                value.constraint.basis.try_into()?,
            )?,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionObservationRecord {
    state: ExecutionStateRecord,
    basis: ObservationBasisRecord,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InteractionObservationRecord {
    state: InteractionStateRecord,
    modality: Option<InteractionModalityRecord>,
    semantic: Option<String>,
    basis: ObservationBasisRecord,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConstraintObservationRecord {
    state: RuntimeConstraintStateRecord,
    semantic: Option<String>,
    basis: ObservationBasisRecord,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObservationBasisRecord {
    confidence_basis_points: u16,
    source: Option<String>,
    disposition: EvidenceDispositionRecord,
    committed_at: i64,
    evidence_observed_at: Option<i64>,
}

impl From<&ObservationBasis> for ObservationBasisRecord {
    fn from(value: &ObservationBasis) -> Self {
        Self {
            confidence_basis_points: value.confidence().basis_points(),
            source: value.source().map(|source| source.as_str().to_owned()),
            disposition: value.disposition().into(),
            committed_at: value.committed_at().value(),
            evidence_observed_at: value.evidence_observed_at().map(TimestampMillis::value),
        }
    }
}

impl TryFrom<ObservationBasisRecord> for ObservationBasis {
    type Error = SessionCodecError;

    fn try_from(value: ObservationBasisRecord) -> Result<Self, Self::Error> {
        Ok(Self::new(
            Confidence::from_basis_points(value.confidence_basis_points)?,
            value.source.map(EvidenceSource::new).transpose()?,
            value.disposition.into(),
            TimestampMillis::new(value.committed_at),
            value.evidence_observed_at.map(TimestampMillis::new),
        )?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ForegroundDecisionRecord {
    observation: ForegroundObservationRecord,
    evidence: Vec<AssessedForegroundEvidenceRecord>,
}

impl From<&ForegroundDecision> for ForegroundDecisionRecord {
    fn from(value: &ForegroundDecision) -> Self {
        Self {
            observation: value.observation().into(),
            evidence: value.evidence().iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<ForegroundDecisionRecord> for ForegroundDecision {
    type Error = SessionCodecError;

    fn try_from(value: ForegroundDecisionRecord) -> Result<Self, Self::Error> {
        let evidence = value
            .evidence
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, SessionCodecError>>()?;
        Ok(Self::rehydrate(value.observation.try_into()?, evidence)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AssessedForegroundEvidenceRecord {
    source: String,
    priority: EvidencePriorityRecord,
    disposition: EvidenceDispositionRecord,
    axis: ForegroundAxisRecord,
    claim: Option<ForegroundClaimRecord>,
    confidence_basis_points: u16,
    observed_at: i64,
    assessment: EvidenceAssessmentRecord,
}

impl From<&AssessedForegroundEvidence> for AssessedForegroundEvidenceRecord {
    fn from(value: &AssessedForegroundEvidence) -> Self {
        let evidence = value.evidence();
        Self {
            source: evidence.source().as_str().to_owned(),
            priority: evidence.priority().into(),
            disposition: evidence.disposition().into(),
            axis: evidence.axis().into(),
            claim: evidence.claim().map(Into::into),
            confidence_basis_points: evidence.confidence().basis_points(),
            observed_at: evidence.observed_at().value(),
            assessment: value.assessment().into(),
        }
    }
}

impl TryFrom<AssessedForegroundEvidenceRecord> for (ForegroundEvidence, EvidenceAssessment) {
    type Error = SessionCodecError;

    fn try_from(value: AssessedForegroundEvidenceRecord) -> Result<Self, Self::Error> {
        let source = EvidenceSource::new(value.source)?;
        let priority = value.priority.into();
        let axis = value.axis.into();
        let observed_at = TimestampMillis::new(value.observed_at);
        let evidence = match value.claim {
            Some(claim) => {
                let claim: ForegroundClaim = claim.try_into()?;
                if claim.axis() != axis {
                    return Err(SessionCodecError::InvalidRecord(
                        "foreground evidence axis and claim disagree",
                    ));
                }
                ForegroundEvidence::new(
                    source,
                    priority,
                    value.disposition.into(),
                    claim,
                    Confidence::from_basis_points(value.confidence_basis_points)?,
                    observed_at,
                )?
            }
            None => {
                if EvidenceDisposition::from(value.disposition) != EvidenceDisposition::Unavailable
                    || value.confidence_basis_points != 0
                {
                    return Err(SessionCodecError::InvalidRecord(
                        "claim-free foreground evidence must be unavailable",
                    ));
                }
                ForegroundEvidence::unavailable(source, priority, axis, observed_at)
            }
        };
        Ok((evidence, value.assessment.into()))
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "axis", rename_all = "snake_case")]
enum ForegroundClaimRecord {
    Execution {
        state: ExecutionStateRecord,
    },
    Interaction {
        state: InteractionStateRecord,
        modality: Option<InteractionModalityRecord>,
        semantic: Option<String>,
    },
    Constraint {
        state: RuntimeConstraintStateRecord,
        semantic: Option<String>,
    },
}

impl From<&ForegroundClaim> for ForegroundClaimRecord {
    fn from(value: &ForegroundClaim) -> Self {
        match value {
            ForegroundClaim::Execution(state) => Self::Execution {
                state: (*state).into(),
            },
            ForegroundClaim::Interaction(claim) => Self::Interaction {
                state: claim.state().into(),
                modality: claim.modality().map(Into::into),
                semantic: claim
                    .semantic()
                    .map(|semantic| semantic.as_str().to_owned()),
            },
            ForegroundClaim::Constraint(claim) => Self::Constraint {
                state: claim.state().into(),
                semantic: claim
                    .semantic()
                    .map(|semantic| semantic.as_str().to_owned()),
            },
        }
    }
}

impl TryFrom<ForegroundClaimRecord> for ForegroundClaim {
    type Error = SessionCodecError;

    fn try_from(value: ForegroundClaimRecord) -> Result<Self, Self::Error> {
        Ok(match value {
            ForegroundClaimRecord::Execution { state } => Self::Execution(state.into()),
            ForegroundClaimRecord::Interaction {
                state,
                modality,
                semantic,
            } => Self::Interaction(InteractionClaim::new(
                state.into(),
                modality.map(Into::into),
                semantic.map(ObservationSemanticId::new).transpose()?,
            )?),
            ForegroundClaimRecord::Constraint { state, semantic } => {
                Self::Constraint(RuntimeConstraintClaim::new(
                    state.into(),
                    semantic.map(ObservationSemanticId::new).transpose()?,
                )?)
            }
        })
    }
}

macro_rules! record_enum {
    ($record:ident, $domain:ty, [$($variant:ident),+ $(,)?]) => {
        #[derive(Debug, Clone, Copy, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        enum $record { $($variant),+ }

        impl From<$domain> for $record {
            fn from(value: $domain) -> Self {
                match value { $(<$domain>::$variant => Self::$variant),+ }
            }
        }

        impl From<$record> for $domain {
            fn from(value: $record) -> Self {
                match value { $($record::$variant => Self::$variant),+ }
            }
        }
    };
}

record_enum!(
    ExecutionStateRecord,
    ExecutionState,
    [Active, Quiescent, Unknown]
);
record_enum!(
    InteractionStateRecord,
    InteractionState,
    [None, Available, Requested]
);
record_enum!(
    InteractionModalityRecord,
    InteractionModality,
    [Text, Options, Secret, Navigation]
);
record_enum!(
    RuntimeConstraintStateRecord,
    RuntimeConstraintState,
    [Clear, Present]
);
record_enum!(
    ForegroundAxisRecord,
    ForegroundAxis,
    [Execution, Interaction, Constraint]
);
record_enum!(
    EvidencePriorityRecord,
    EvidencePriority,
    [
        Cooperative,
        ForegroundProcess,
        Terminal,
        TerminalHistory,
        Process,
        Inference
    ]
);
record_enum!(
    EvidenceDispositionRecord,
    EvidenceDisposition,
    [Affirmative, Inference, Unavailable]
);
record_enum!(
    IgnoredEvidenceReasonRecord,
    IgnoredEvidenceReason,
    [Unavailable, Stale, Future, BelowConfidence, LowerPrecedence]
);

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum EvidenceAssessmentRecord {
    Selected,
    Ignored { reason: IgnoredEvidenceReasonRecord },
}

impl From<EvidenceAssessment> for EvidenceAssessmentRecord {
    fn from(value: EvidenceAssessment) -> Self {
        match value {
            EvidenceAssessment::Selected => Self::Selected,
            EvidenceAssessment::Ignored(reason) => Self::Ignored {
                reason: reason.into(),
            },
        }
    }
}

impl From<EvidenceAssessmentRecord> for EvidenceAssessment {
    fn from(value: EvidenceAssessmentRecord) -> Self {
        match value {
            EvidenceAssessmentRecord::Selected => Self::Selected,
            EvidenceAssessmentRecord::Ignored { reason } => Self::Ignored(reason.into()),
        }
    }
}
