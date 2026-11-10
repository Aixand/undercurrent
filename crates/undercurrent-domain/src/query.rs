// SPDX-License-Identifier: Apache-2.0

//! Typed session query specifications independent of storage and transport.

use std::collections::BTreeSet;

use crate::{
    DomainError, DomainResult, ExecutionState, HealthStatus, InteractionState, InvalidValueReason,
    Labels, LifecycleKind, ProfileName, SessionId, SessionSummary,
};

const MAX_NAME_FILTER_BYTES: usize = 128;

/// Validated session-name substring filter.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NameContains(String);

impl NameContains {
    /// Constructs a non-empty, bounded name substring.
    ///
    /// # Errors
    ///
    /// Returns a validation error for empty, oversized, or control-character
    /// input. The function performs no IO and does not panic.
    pub fn new(value: impl Into<String>) -> DomainResult<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(DomainError::InvalidValue {
                field: "name_contains",
                reason: InvalidValueReason::Empty,
            });
        }
        if value.len() > MAX_NAME_FILTER_BYTES {
            return Err(DomainError::InvalidValue {
                field: "name_contains",
                reason: InvalidValueReason::TooLong {
                    max: MAX_NAME_FILTER_BYTES,
                },
            });
        }
        if let Some((index, character)) = value
            .char_indices()
            .find(|(_, character)| character.is_control())
        {
            return Err(DomainError::InvalidValue {
                field: "name_contains",
                reason: InvalidValueReason::InvalidCharacter { index, character },
            });
        }
        Ok(Self(value))
    }

    /// Returns the validated substring.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Typed list projection controlling summary payload size.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum SessionProjection {
    /// Minimal summary without the complete label set.
    #[default]
    Compact,
    /// Compact summary plus all labels.
    WithLabels,
    /// Compact summary plus bounded operational counters.
    Operational,
}

/// Stable field used to order session query results.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum SessionSortField {
    /// Human-facing session name.
    #[default]
    Name,
    /// Authoritative lifecycle kind.
    Lifecycle,
    /// Best-effort foreground execution state.
    Execution,
    /// Best-effort foreground interaction state.
    Interaction,
    /// Aggregate health status.
    Health,
    /// Most recent domain update time.
    UpdatedAt,
    /// Number of automatic restart attempts.
    RestartCount,
}

/// Direction of a stable server-side sort.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum SortDirection {
    /// Lowest values first.
    #[default]
    Ascending,
    /// Highest values first.
    Descending,
}

/// One server-side sort instruction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct SessionSort {
    /// Field compared before the stable session-ID tie-breaker.
    pub field: SessionSortField,
    /// Requested ordering direction.
    pub direction: SortDirection,
}

/// Validated bounded page size.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageSize(usize);

impl PageSize {
    /// Validates a requested size against the server-configured hard maximum.
    ///
    /// # Errors
    ///
    /// Returns a typed validation error for zero or a capacity error above the
    /// maximum. The function performs no IO and does not panic.
    pub fn new(requested: usize, configured_max: usize) -> DomainResult<Self> {
        if configured_max == 0 {
            return Err(DomainError::InvalidValue {
                field: "configured_max_page_size",
                reason: crate::InvalidValueReason::OutOfRange { min: 1, max: 1 },
            });
        }
        if requested == 0 {
            return Err(DomainError::InvalidValue {
                field: "page_size",
                reason: crate::InvalidValueReason::OutOfRange {
                    min: 1,
                    max: configured_max as u64,
                },
            });
        }
        if requested > configured_max {
            return Err(DomainError::CapacityExceeded {
                resource: "query_page_size",
                limit: configured_max,
            });
        }
        Ok(Self(requested))
    }

    /// Returns the validated number of rows requested.
    pub const fn get(self) -> usize {
        self.0
    }
}

/// Typed filters evaluated by a prepared session read model.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionFilter {
    /// Restrict results to these stable IDs. Empty means unrestricted.
    pub ids: BTreeSet<SessionId>,
    /// Restrict results to one exact unique human-facing name.
    pub name: Option<crate::SessionName>,
    /// Case-sensitive substring required in the display name.
    pub name_contains: Option<NameContains>,
    /// Accepted lifecycle kinds. Empty means unrestricted.
    pub lifecycles: BTreeSet<LifecycleKind>,
    /// Accepted execution states. Empty means unrestricted.
    pub executions: BTreeSet<ExecutionState>,
    /// Accepted interaction states. Empty means unrestricted.
    pub interactions: BTreeSet<InteractionState>,
    /// Accepted health states. Empty means unrestricted.
    pub health: BTreeSet<HealthStatus>,
    /// Required detection profile.
    pub profile: Option<ProfileName>,
    /// Labels that must all be present.
    pub labels: Labels,
    /// Whether at least one WorkContext item must be active.
    pub has_active_work: Option<bool>,
}

impl SessionFilter {
    /// Returns whether a prepared summary belongs in this query.
    ///
    /// This pure predicate performs no IO and does not panic. Adapters may use
    /// indexes to avoid evaluating it against unrelated summaries.
    pub fn matches(&self, summary: &SessionSummary) -> bool {
        (self.ids.is_empty() || self.ids.contains(summary.id()))
            && self.name.as_ref().is_none_or(|name| summary.name() == name)
            && self
                .name_contains
                .as_ref()
                .is_none_or(|needle| summary.name().as_str().contains(needle.as_str()))
            && (self.lifecycles.is_empty() || self.lifecycles.contains(&summary.lifecycle().kind()))
            && (self.executions.is_empty()
                || self
                    .executions
                    .contains(&summary.foreground().execution().state()))
            && (self.interactions.is_empty()
                || self
                    .interactions
                    .contains(&summary.foreground().interaction().state()))
            && (self.health.is_empty() || self.health.contains(&summary.health().status()))
            && self
                .profile
                .as_ref()
                .is_none_or(|profile| summary.profile() == Some(profile))
            && summary.labels().contains_all(&self.labels)
            && self
                .has_active_work
                .is_none_or(|required| (summary.work().active_item_count > 0) == required)
    }
}

/// Complete typed specification for the first page of a session query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionQuery {
    /// Server-side membership filters.
    pub filter: SessionFilter,
    /// Stable ordering before the session-ID tie-breaker.
    pub sort: SessionSort,
    /// Requested summary payload shape.
    pub projection: SessionProjection,
    /// Bounded rows requested in this page.
    pub page_size: PageSize,
    /// Whether an exact total is explicitly requested.
    pub include_total: bool,
}
