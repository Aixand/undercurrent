// SPDX-License-Identifier: Apache-2.0

//! Pure transport presentation for application and domain results.

mod detail;
mod feed;
mod profile;

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use undercurrent_application::{
    QueryWatchBatch, SessionDelta, SessionDeltaKind, SessionPage, SessionSummaryView,
    SessionWatchSnapshot,
};
use undercurrent_domain::{
    EffectiveProfileOrigin, EffectiveProfileSnapshot, EvidenceDisposition, ExecutionState,
    ForegroundAxis, ForegroundClaim, ForegroundObservation, HealthStatus, HealthSummary,
    InteractionModality, InteractionState, Labels, LifecycleState, NoRestartReason,
    ObservationBasis, ProfileBindingMode, ProfileSelectionOrigin, RestartStatus,
    RuntimeConstraintState, SessionSummary, UnclassifiedProfileReason, WorkStatus, WorkSummary,
};
use undercurrent_protocol::{
    EffectiveProfileContextDto, EffectiveProfileOriginDto, EvidenceDispositionDto,
    ExecutionObservationDto, ExecutionStateDto, ForegroundAxisDto, ForegroundClaimDto,
    ForegroundObservationDto, HealthStatusDto, HealthSummaryDto, InteractionModalityDto,
    InteractionObservationDto, InteractionStateDto, LifecycleStateDto, NoRestartReasonDto,
    ObservationBasisDto, ProfileBindingModeDto, ProfileSelectionOriginDto, RestartStatusDto,
    RuntimeConstraintObservationDto, RuntimeConstraintStateDto, RuntimeRoleDto,
    ServerRoleOriginDto, SessionDeltaBatchDto, SessionDeltaDto, SessionDeltaKindDto,
    SessionPageDto, SessionSummaryDto, UnclassifiedProfileReasonDto, WorkStatusDto, WorkSummaryDto,
};

/// Failure to represent a bounded application result in the public protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductPresentationError {
    field: &'static str,
}

impl ProductPresentationError {
    /// Returns the stable field that exceeded the public representation.
    pub const fn field(self) -> &'static str {
        self.field
    }
}

impl Display for ProductPresentationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "application result field {} exceeds product representation",
            self.field
        )
    }
}

impl Error for ProductPresentationError {}

/// Formats domain and application results after use cases have completed.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProductPresentation;

pub(crate) enum TerminalOutputStreamPresentation {
    Gap,
    Data {
        payload: Option<Arc<[u8]>>,
        has_more: bool,
    },
}

impl ProductPresentation {
    /// Formats a compact aggregate summary without exposing complete labels.
    pub fn session_summary(
        summary: &SessionSummary,
    ) -> Result<SessionSummaryDto, ProductPresentationError> {
        summary_dto(SummaryFields {
            id: summary.id().as_str(),
            name: summary.name().as_str(),
            lifecycle: summary.lifecycle(),
            foreground: summary.foreground(),
            health: summary.health(),
            work: summary.work(),
            profile: summary.profile().map(|profile| profile.as_str()),
            base_profile: summary.base_profile().map(|profile| profile.as_str()),
            profile_binding: summary.profile_binding(),
            profile_origin: summary.profile_origin(),
            effective_profile: summary.effective_profile(),
            labels: None,
            started_at_millis: summary.started_at().map(|value| value.value()),
            updated_at_millis: summary.updated_at().value(),
            restart_count: summary.restart_count(),
            restart_status: summary.restart_status(),
            revision: summary.revision().value(),
            lifecycle_revision: summary.lifecycle_revision().value(),
        })
    }

    /// Formats one prepared query page and its separately retained opaque cursor.
    pub fn session_page(
        page: &SessionPage,
        next_cursor: Option<String>,
    ) -> Result<SessionPageDto, ProductPresentationError> {
        let items = page
            .items
            .iter()
            .map(Self::session_summary_view)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SessionPageDto {
            items,
            next_cursor,
            snapshot_revision: page.snapshot_revision.value(),
            total: page
                .total
                .map(|value| {
                    u64::try_from(value).map_err(|_| ProductPresentationError { field: "total" })
                })
                .transpose()?,
        })
    }

    /// Formats the initial bounded page owned by one query watch.
    pub fn session_watch_snapshot(
        snapshot: &SessionWatchSnapshot,
    ) -> Result<SessionPageDto, ProductPresentationError> {
        let items = snapshot
            .items
            .iter()
            .map(Self::session_summary_view)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SessionPageDto {
            items,
            next_cursor: None,
            snapshot_revision: snapshot.revision.value(),
            total: optional_usize(snapshot.total, "total")?,
        })
    }

    /// Formats one authoritative coalesced watched-page transition.
    pub fn session_delta_batch(
        batch: &QueryWatchBatch,
    ) -> Result<SessionDeltaBatchDto, ProductPresentationError> {
        Ok(SessionDeltaBatchDto {
            from_revision: batch.from_revision.value(),
            to_revision: batch.to_revision.value(),
            deltas: batch
                .deltas
                .iter()
                .map(|delta| session_delta(delta, batch.to_revision.value()))
                .collect::<Result<Vec<_>, _>>()?,
            order: batch
                .order
                .iter()
                .map(|session_id| session_id.as_str().to_owned())
                .collect(),
            total: optional_usize(batch.total, "total")?,
        })
    }

    /// Formats one projection-shaped prepared summary.
    pub fn session_summary_view(
        summary: &SessionSummaryView,
    ) -> Result<SessionSummaryDto, ProductPresentationError> {
        summary_dto(SummaryFields {
            id: summary.id().as_str(),
            name: summary.name().as_str(),
            lifecycle: summary.lifecycle(),
            foreground: summary.foreground(),
            health: summary.health(),
            work: summary.work(),
            profile: summary.profile().map(|profile| profile.as_str()),
            base_profile: summary.base_profile().map(|profile| profile.as_str()),
            profile_binding: summary.profile_binding(),
            profile_origin: summary.profile_origin(),
            effective_profile: summary.effective_profile(),
            labels: summary.labels(),
            started_at_millis: summary.started_at().map(|value| value.value()),
            updated_at_millis: summary.updated_at().value(),
            restart_count: summary.restart_count(),
            restart_status: summary.restart_status(),
            revision: summary.revision().value(),
            lifecycle_revision: summary.lifecycle_revision().value(),
        })
    }
}

fn session_delta(
    delta: &SessionDelta,
    catalog_revision: u64,
) -> Result<SessionDeltaDto, ProductPresentationError> {
    Ok(SessionDeltaDto {
        kind: match delta.kind() {
            SessionDeltaKind::Added => SessionDeltaKindDto::Added,
            SessionDeltaKind::Updated => SessionDeltaKindDto::Updated,
            SessionDeltaKind::Removed => SessionDeltaKindDto::Removed,
        },
        session_id: delta.session_id().as_str().to_owned(),
        session_revision: delta.session_revision().value(),
        catalog_revision,
        summary: delta
            .summary()
            .map(ProductPresentation::session_summary_view)
            .transpose()?,
    })
}

fn optional_usize(
    value: Option<usize>,
    field: &'static str,
) -> Result<Option<u64>, ProductPresentationError> {
    value
        .map(|value| u64::try_from(value).map_err(|_| ProductPresentationError { field }))
        .transpose()
}

struct SummaryFields<'a> {
    id: &'a str,
    name: &'a str,
    lifecycle: LifecycleState,
    foreground: &'a ForegroundObservation,
    health: &'a HealthSummary,
    work: &'a WorkSummary,
    profile: Option<&'a str>,
    base_profile: Option<&'a str>,
    profile_binding: ProfileBindingMode,
    profile_origin: Option<ProfileSelectionOrigin>,
    effective_profile: Option<&'a EffectiveProfileSnapshot>,
    labels: Option<&'a Labels>,
    started_at_millis: Option<i64>,
    updated_at_millis: i64,
    restart_count: u32,
    restart_status: RestartStatus,
    revision: u64,
    lifecycle_revision: u64,
}

fn summary_dto(fields: SummaryFields<'_>) -> Result<SessionSummaryDto, ProductPresentationError> {
    Ok(SessionSummaryDto {
        id: fields.id.to_owned(),
        name: fields.name.to_owned(),
        lifecycle: lifecycle(fields.lifecycle),
        foreground: foreground_observation(fields.foreground),
        health: health_summary(fields.health),
        work: work_summary(fields.work)?,
        profile: fields.profile.map(str::to_owned),
        profile_context: effective_profile_context(&fields),
        labels: fields.labels.map(labels),
        started_at_millis: fields.started_at_millis,
        updated_at_millis: fields.updated_at_millis,
        restart_count: fields.restart_count,
        restart_status: restart_status(fields.restart_status),
        revision: fields.revision,
        lifecycle_revision: fields.lifecycle_revision,
    })
}

fn effective_profile_context(fields: &SummaryFields<'_>) -> EffectiveProfileContextDto {
    let (effective_origin, epoch, unclassified_reason, observed_at_millis) =
        match fields.effective_profile {
            Some(effective) => (
                effective_profile_origin(effective.origin()),
                effective.epoch().value(),
                effective
                    .selection()
                    .unclassified_reason()
                    .map(unclassified_profile_reason),
                Some(effective.observed_at().value()),
            ),
            None if fields.profile_binding == ProfileBindingMode::ForegroundAdaptive => (
                EffectiveProfileOriginDto::ForegroundProcess,
                0,
                Some(UnclassifiedProfileReasonDto::AwaitingObservation),
                None,
            ),
            None => (
                EffectiveProfileOriginDto::BaseProfile,
                0,
                fields
                    .profile
                    .is_none()
                    .then_some(UnclassifiedProfileReasonDto::NoMatch),
                Some(fields.updated_at_millis),
            ),
        };
    let matching_evidence = fields
        .effective_profile
        .map(EffectiveProfileSnapshot::matching_evidence)
        .unwrap_or_default();
    let has_primary = matching_evidence
        .iter()
        .any(|item| !item.matching_fallback());
    EffectiveProfileContextDto {
        matching_evidence: matching_evidence
            .iter()
            .map(|item| undercurrent_protocol::ProfileMatchEvidenceDto {
                profile: item.profile().as_str().to_owned(),
                matching_fallback: item.matching_fallback(),
                suppressed: has_primary && item.matching_fallback(),
                process_incarnations: item.process_incarnations().to_vec(),
            })
            .collect(),
        base_profile: fields.base_profile.map(str::to_owned),
        binding: match fields.profile_binding {
            ProfileBindingMode::Fixed => ProfileBindingModeDto::Fixed,
            ProfileBindingMode::ForegroundAdaptive => ProfileBindingModeDto::ForegroundAdaptive,
        },
        base_origin: fields.profile_origin.map(|origin| match origin {
            ProfileSelectionOrigin::Explicit => ProfileSelectionOriginDto::Explicit,
            ProfileSelectionOrigin::LaunchCommand => ProfileSelectionOriginDto::LaunchCommand,
        }),
        effective_origin,
        epoch,
        unclassified_reason,
        observed_at_millis,
        current_directory: fields
            .effective_profile
            .and_then(EffectiveProfileSnapshot::current_directory)
            .map(|path| path.as_str().to_owned()),
        runtime_role: fields
            .effective_profile
            .and_then(EffectiveProfileSnapshot::server_role)
            .map(|_| RuntimeRoleDto::Server),
        server_role_origin: fields
            .effective_profile
            .and_then(EffectiveProfileSnapshot::server_role)
            .map(|origin| match origin {
                undercurrent_domain::ServerRoleOrigin::Profile => ServerRoleOriginDto::Profile,
                undercurrent_domain::ServerRoleOrigin::TcpListener => {
                    ServerRoleOriginDto::TcpListener
                }
                undercurrent_domain::ServerRoleOrigin::ProfileAndTcpListener => {
                    ServerRoleOriginDto::ProfileAndTcpListener
                }
                undercurrent_domain::ServerRoleOrigin::ManagedProcessTcpListener => {
                    ServerRoleOriginDto::ManagedProcessTcpListener
                }
                undercurrent_domain::ServerRoleOrigin::ProfileAndManagedProcessTcpListener => {
                    ServerRoleOriginDto::ProfileAndManagedProcessTcpListener
                }
                undercurrent_domain::ServerRoleOrigin::ManagedTreeTcpListener => {
                    ServerRoleOriginDto::ManagedTreeTcpListener
                }
                undercurrent_domain::ServerRoleOrigin::ProfileAndManagedTreeTcpListener => {
                    ServerRoleOriginDto::ProfileAndManagedTreeTcpListener
                }
            }),
    }
}

const fn effective_profile_origin(value: EffectiveProfileOrigin) -> EffectiveProfileOriginDto {
    match value {
        EffectiveProfileOrigin::BaseProfile => EffectiveProfileOriginDto::BaseProfile,
        EffectiveProfileOrigin::ForegroundProcess => EffectiveProfileOriginDto::ForegroundProcess,
        EffectiveProfileOrigin::CooperativeShell => EffectiveProfileOriginDto::CooperativeShell,
    }
}

const fn unclassified_profile_reason(
    value: UnclassifiedProfileReason,
) -> UnclassifiedProfileReasonDto {
    match value {
        UnclassifiedProfileReason::AwaitingObservation => {
            UnclassifiedProfileReasonDto::AwaitingObservation
        }
        UnclassifiedProfileReason::NoMatch => UnclassifiedProfileReasonDto::NoMatch,
        UnclassifiedProfileReason::Ambiguous => UnclassifiedProfileReasonDto::Ambiguous,
        UnclassifiedProfileReason::IncompleteEvidence => {
            UnclassifiedProfileReasonDto::IncompleteEvidence
        }
        UnclassifiedProfileReason::Unavailable => UnclassifiedProfileReasonDto::Unavailable,
    }
}

const fn restart_status(value: RestartStatus) -> RestartStatusDto {
    match value {
        RestartStatus::Idle => RestartStatusDto::Idle,
        RestartStatus::Scheduled {
            attempt,
            not_before,
        } => RestartStatusDto::Scheduled {
            attempt,
            not_before_millis: not_before.value(),
        },
        RestartStatus::NotScheduled { reason } => RestartStatusDto::NotScheduled {
            reason: no_restart_reason(reason),
        },
        RestartStatus::Exhausted { attempts } => RestartStatusDto::Exhausted { attempts },
    }
}

const fn no_restart_reason(value: NoRestartReason) -> NoRestartReasonDto {
    match value {
        NoRestartReason::PolicyDisabled => NoRestartReasonDto::PolicyDisabled,
        NoRestartReason::SuccessfulExit => NoRestartReasonDto::SuccessfulExit,
        NoRestartReason::ExplicitStop => NoRestartReasonDto::ExplicitStop,
    }
}

const fn lifecycle(value: LifecycleState) -> LifecycleStateDto {
    match value {
        LifecycleState::Starting => LifecycleStateDto::Starting,
        LifecycleState::Running => LifecycleStateDto::Running,
        LifecycleState::Stopping => LifecycleStateDto::Stopping,
        LifecycleState::Stopped => LifecycleStateDto::Stopped,
        LifecycleState::Exited(code) => LifecycleStateDto::Exited { code: Some(code) },
        LifecycleState::Failed => LifecycleStateDto::Failed,
    }
}

fn foreground_observation(value: &ForegroundObservation) -> ForegroundObservationDto {
    ForegroundObservationDto {
        execution: ExecutionObservationDto {
            state: execution_state(value.execution().state()),
            basis: observation_basis(value.execution().basis()),
        },
        interaction: InteractionObservationDto {
            state: interaction_state(value.interaction().state()),
            modality: value.interaction().modality().map(interaction_modality),
            semantic: value
                .interaction()
                .semantic()
                .map(|semantic| semantic.as_str().to_owned()),
            basis: observation_basis(value.interaction().basis()),
        },
        constraint: RuntimeConstraintObservationDto {
            state: constraint_state(value.constraint().state()),
            semantic: value
                .constraint()
                .semantic()
                .map(|semantic| semantic.as_str().to_owned()),
            basis: observation_basis(value.constraint().basis()),
        },
    }
}

fn observation_basis(value: &ObservationBasis) -> ObservationBasisDto {
    ObservationBasisDto {
        confidence_basis_points: value.confidence().basis_points(),
        source: value.source().map(|source| source.as_str().to_owned()),
        disposition: evidence_disposition(value.disposition()),
        committed_at_millis: value.committed_at().value(),
        evidence_observed_at_millis: value.evidence_observed_at().map(|time| time.value()),
    }
}

pub(super) const fn evidence_disposition(value: EvidenceDisposition) -> EvidenceDispositionDto {
    match value {
        EvidenceDisposition::Affirmative => EvidenceDispositionDto::Affirmative,
        EvidenceDisposition::Inference => EvidenceDispositionDto::Inference,
        EvidenceDisposition::Unavailable => EvidenceDispositionDto::Unavailable,
    }
}

pub(super) const fn execution_state(value: ExecutionState) -> ExecutionStateDto {
    match value {
        ExecutionState::Active => ExecutionStateDto::Active,
        ExecutionState::Quiescent => ExecutionStateDto::Quiescent,
        ExecutionState::Unknown => ExecutionStateDto::Unknown,
    }
}

pub(super) const fn interaction_state(value: InteractionState) -> InteractionStateDto {
    match value {
        InteractionState::None => InteractionStateDto::None,
        InteractionState::Available => InteractionStateDto::Available,
        InteractionState::Requested => InteractionStateDto::Requested,
    }
}

pub(super) const fn interaction_modality(value: InteractionModality) -> InteractionModalityDto {
    match value {
        InteractionModality::Text => InteractionModalityDto::Text,
        InteractionModality::Options => InteractionModalityDto::Options,
        InteractionModality::Secret => InteractionModalityDto::Secret,
        InteractionModality::Navigation => InteractionModalityDto::Navigation,
    }
}

pub(super) const fn constraint_state(value: RuntimeConstraintState) -> RuntimeConstraintStateDto {
    match value {
        RuntimeConstraintState::Clear => RuntimeConstraintStateDto::Clear,
        RuntimeConstraintState::Present => RuntimeConstraintStateDto::Present,
    }
}

pub(super) fn foreground_claim(value: &ForegroundClaim) -> ForegroundClaimDto {
    match value {
        ForegroundClaim::Execution(state) => ForegroundClaimDto::Execution {
            state: execution_state(*state),
        },
        ForegroundClaim::Interaction(interaction) => ForegroundClaimDto::Interaction {
            state: interaction_state(interaction.state()),
            modality: interaction.modality().map(interaction_modality),
            semantic: interaction
                .semantic()
                .map(|semantic| semantic.as_str().to_owned()),
        },
        ForegroundClaim::Constraint(constraint) => ForegroundClaimDto::Constraint {
            state: constraint_state(constraint.state()),
            semantic: constraint
                .semantic()
                .map(|semantic| semantic.as_str().to_owned()),
        },
    }
}

pub(super) const fn foreground_axis(value: ForegroundAxis) -> ForegroundAxisDto {
    match value {
        ForegroundAxis::Execution => ForegroundAxisDto::Execution,
        ForegroundAxis::Interaction => ForegroundAxisDto::Interaction,
        ForegroundAxis::Constraint => ForegroundAxisDto::Constraint,
    }
}

fn health_summary(value: &HealthSummary) -> HealthSummaryDto {
    HealthSummaryDto {
        status: match value.status() {
            HealthStatus::Healthy => HealthStatusDto::Healthy,
            HealthStatus::Unhealthy => HealthStatusDto::Unhealthy,
            HealthStatus::Unknown => HealthStatusDto::Unknown,
            HealthStatus::NoCheck => HealthStatusDto::NoCheck,
        },
        observed_at_millis: value.observed_at().map(|time| time.value()),
        configured_checks: u32::from(value.configured_checks()),
    }
}

fn work_summary(value: &WorkSummary) -> Result<WorkSummaryDto, ProductPresentationError> {
    Ok(WorkSummaryDto {
        revision: value.revision.value(),
        total_items: u32::try_from(value.item_count).map_err(|_| ProductPresentationError {
            field: "work_total_items",
        })?,
        active_items: u32::try_from(value.active_item_count).map_err(|_| {
            ProductPresentationError {
                field: "work_active_items",
            }
        })?,
        total_targets: u32::try_from(value.target_count).map_err(|_| ProductPresentationError {
            field: "work_total_targets",
        })?,
        active_targets: u32::try_from(value.active_target_count).map_err(|_| {
            ProductPresentationError {
                field: "work_active_targets",
            }
        })?,
        aggregate_status: value.aggregate_status.map(work_status),
    })
}

pub(super) const fn work_status(value: WorkStatus) -> WorkStatusDto {
    match value {
        WorkStatus::Running => WorkStatusDto::Running,
        WorkStatus::Waiting => WorkStatusDto::Waiting,
        WorkStatus::Blocked => WorkStatusDto::Blocked,
        WorkStatus::Completed => WorkStatusDto::Completed,
        WorkStatus::Failed => WorkStatusDto::Failed,
        WorkStatus::Cancelled => WorkStatusDto::Cancelled,
        WorkStatus::DeliveryFailed => WorkStatusDto::DeliveryFailed,
        WorkStatus::Expired => WorkStatusDto::Expired,
    }
}

pub(super) fn labels(value: &Labels) -> BTreeMap<String, String> {
    value
        .iter()
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
}
