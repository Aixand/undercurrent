// SPDX-License-Identifier: Apache-2.0

//! Indexed prepared read model for bounded session list and watch queries.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::RwLock;

use undercurrent_domain::{
    CatalogRevision, EffectiveProfileSnapshot, ExecutionState, ForegroundObservation, HealthStatus,
    HealthSummary, InteractionState, Labels, LifecycleKind, LifecycleState, ProfileBindingMode,
    ProfileName, ProfileSelectionOrigin, RestartStatus, Revision, SessionFilter, SessionId,
    SessionName, SessionProjection, SessionQuery, SessionSort, SessionSortField, SessionSummary,
    SortDirection, TimestampMillis, WorkSummary,
};

use crate::{
    CatalogError, CatalogResult, EffectiveProfileDirectory, PortError, PortErrorKind,
    PortOperation, SessionProjectionWriter,
};

/// Result of applying one prepared-summary catalog change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogChange {
    revision: CatalogRevision,
}

impl CatalogChange {
    /// Returns catalog revision after the change.
    ///
    /// This function performs no IO and does not panic.
    pub const fn revision(self) -> CatalogRevision {
        self.revision
    }
}

/// Internal cursor position before transport-specific signing and encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionPageCursor {
    catalog_revision: CatalogRevision,
    query: SessionQuery,
    after_session_id: SessionId,
}

impl SessionPageCursor {
    /// Reconstructs adapter-retained cursor state from validated application
    /// values.
    ///
    /// Outer adapters should expose only an opaque handle to this value. The
    /// prepared catalog revalidates the query and catalog revision when the
    /// cursor is used. This function performs no IO, cannot fail, and does not
    /// panic.
    pub const fn rehydrate(
        catalog_revision: CatalogRevision,
        query: SessionQuery,
        after_session_id: SessionId,
    ) -> Self {
        Self {
            catalog_revision,
            query,
            after_session_id,
        }
    }

    /// Returns catalog revision bound to this cursor.
    pub const fn catalog_revision(&self) -> CatalogRevision {
        self.catalog_revision
    }

    /// Returns query specification bound to this cursor.
    pub const fn query(&self) -> &SessionQuery {
        &self.query
    }

    /// Returns stable ID of the last item in the previous page.
    pub const fn after_session_id(&self) -> &SessionId {
        &self.after_session_id
    }
}

/// One bounded page from a stable prepared-catalog revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionPage {
    /// Owned projected summaries in requested order.
    pub items: Vec<SessionSummaryView>,
    /// Cursor for the next page when more matching items remain.
    pub next_cursor: Option<SessionPageCursor>,
    /// Catalog revision used to evaluate the page.
    pub snapshot_revision: CatalogRevision,
    /// Exact matching count only when explicitly requested.
    pub total: Option<usize>,
    /// Bounded diagnostic cost of extracting this page from sort indexes.
    pub query_cost: QueryCost,
}

/// Payload-safe session summary shaped by [`SessionProjection`].
///
/// The prepared catalog retains complete internal labels for index evaluation,
/// but this view includes them only for `with_labels`. Complete command,
/// environment, metadata, health evidence, WorkContext items, screen state,
/// and output are never present.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSummaryView {
    projection: SessionProjection,
    id: SessionId,
    name: SessionName,
    lifecycle: LifecycleState,
    foreground: ForegroundObservation,
    health: HealthSummary,
    work: WorkSummary,
    profile: Option<ProfileName>,
    base_profile: Option<ProfileName>,
    profile_origin: Option<ProfileSelectionOrigin>,
    profile_binding: ProfileBindingMode,
    effective_profile: Option<EffectiveProfileSnapshot>,
    labels: Option<Labels>,
    started_at: Option<TimestampMillis>,
    updated_at: TimestampMillis,
    restart_count: u32,
    restart_status: RestartStatus,
    revision: Revision,
    lifecycle_revision: Revision,
}

impl SessionSummaryView {
    /// Builds a requested view from one internal prepared summary.
    ///
    /// This function clones bounded prepared values only, performs no IO, and
    /// does not panic.
    pub fn from_summary(summary: &SessionSummary, projection: SessionProjection) -> Self {
        Self {
            projection,
            id: summary.id().clone(),
            name: summary.name().clone(),
            lifecycle: summary.lifecycle(),
            foreground: summary.foreground().clone(),
            health: summary.health().clone(),
            work: *summary.work(),
            profile: summary.profile().cloned(),
            base_profile: summary.base_profile().cloned(),
            profile_origin: summary.profile_origin(),
            profile_binding: summary.profile_binding(),
            effective_profile: summary.effective_profile().cloned(),
            labels: (projection == SessionProjection::WithLabels).then(|| summary.labels().clone()),
            started_at: summary.started_at(),
            updated_at: summary.updated_at(),
            restart_count: summary.restart_count(),
            restart_status: summary.restart_status(),
            revision: summary.revision(),
            lifecycle_revision: summary.lifecycle_revision(),
        }
    }

    /// Returns projection used to shape this view.
    pub const fn projection(&self) -> SessionProjection {
        self.projection
    }

    /// Returns stable session identity.
    pub const fn id(&self) -> &SessionId {
        &self.id
    }

    /// Returns human-facing display name.
    pub const fn name(&self) -> &SessionName {
        &self.name
    }

    /// Returns authoritative lifecycle.
    pub const fn lifecycle(&self) -> LifecycleState {
        self.lifecycle
    }

    /// Returns compact best-effort foreground.
    pub const fn foreground(&self) -> &ForegroundObservation {
        &self.foreground
    }

    /// Returns compact health state.
    pub const fn health(&self) -> &HealthSummary {
        &self.health
    }

    /// Returns compact WorkContext aggregate.
    pub const fn work(&self) -> &WorkSummary {
        &self.work
    }

    /// Returns configured detection profile when present.
    pub const fn profile(&self) -> Option<&ProfileName> {
        self.profile.as_ref()
    }

    /// Returns the durable launch-selected base profile.
    pub const fn base_profile(&self) -> Option<&ProfileName> {
        self.base_profile.as_ref()
    }

    /// Returns the durable base-profile origin.
    pub const fn profile_origin(&self) -> Option<ProfileSelectionOrigin> {
        self.profile_origin
    }

    /// Returns whether effective selection is fixed or foreground-adaptive.
    pub const fn profile_binding(&self) -> ProfileBindingMode {
        self.profile_binding
    }

    /// Returns the exact adaptive runtime snapshot when one has been observed.
    pub const fn effective_profile(&self) -> Option<&EffectiveProfileSnapshot> {
        self.effective_profile.as_ref()
    }

    /// Returns labels only when `with_labels` was explicitly requested.
    pub const fn labels(&self) -> Option<&Labels> {
        self.labels.as_ref()
    }

    /// Returns current process start time when running.
    pub const fn started_at(&self) -> Option<TimestampMillis> {
        self.started_at
    }

    /// Returns most recent durable session update time.
    pub const fn updated_at(&self) -> TimestampMillis {
        self.updated_at
    }

    /// Returns consecutive automatic restart count.
    pub const fn restart_count(&self) -> u32 {
        self.restart_count
    }

    /// Returns the durable disposition of the latest restart decision.
    pub const fn restart_status(&self) -> RestartStatus {
        self.restart_status
    }

    /// Returns durable session revision represented by this view.
    pub const fn revision(&self) -> Revision {
        self.revision
    }

    /// Returns the revision of the latest authoritative lifecycle transition.
    pub const fn lifecycle_revision(&self) -> Revision {
        self.lifecycle_revision
    }
}

/// Operational query cost safe to record in metrics and diagnostics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryCost {
    /// Ordered index entries examined while filling the page.
    pub index_entries_examined: usize,
}

/// Thread-safe prepared catalog serving queries without session-supervisor IO.
#[derive(Debug, Default)]
pub struct PreparedSessionCatalog {
    state: RwLock<CatalogState>,
}

/// In-memory prepared views used by bounded list and watch queries.
#[derive(Debug, Default)]
pub struct PreparedReadModels {
    catalog: PreparedSessionCatalog,
}

impl PreparedReadModels {
    /// Creates empty prepared read models.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the bounded session-summary catalog.
    pub const fn catalog(&self) -> &PreparedSessionCatalog {
        &self.catalog
    }
}

impl SessionProjectionWriter for PreparedReadModels {
    fn upsert_session(&self, session: &undercurrent_domain::Session) -> Result<(), PortError> {
        self.catalog.upsert_session(session)
    }

    fn remove_summary(&self, session_id: &SessionId) -> Result<(), PortError> {
        self.catalog.remove_summary(session_id)
    }
}

impl PreparedSessionCatalog {
    /// Creates an empty catalog at revision zero.
    ///
    /// This function performs no IO and does not panic.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts or replaces one compact summary.
    ///
    /// Identical summaries are idempotent and do not advance catalog revision.
    /// Secondary indexes are updated atomically under the write lock. This
    /// function performs no external IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns revision exhaustion or a poisoned-lock error.
    pub fn upsert(&self, summary: SessionSummary) -> CatalogResult<Option<CatalogChange>> {
        let mut state = self.state.write().map_err(|_| CatalogError::LockPoisoned)?;
        if state.summaries.get(summary.id()) == Some(&summary) {
            return Ok(None);
        }
        let revision = state.revision.next()?;
        if let Some(previous) = state.summaries.remove(summary.id()) {
            state.indexes.remove(&previous);
        }
        state.indexes.insert(&summary);
        state.summaries.insert(summary.id().clone(), summary);
        state.revision = revision;
        Ok(Some(CatalogChange { revision }))
    }

    /// Removes a summary and all index entries by stable session ID.
    ///
    /// Missing IDs are idempotent no-ops. This function performs no external
    /// IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns revision exhaustion or a poisoned-lock error.
    pub fn remove(&self, session_id: &SessionId) -> CatalogResult<Option<CatalogChange>> {
        let mut state = self.state.write().map_err(|_| CatalogError::LockPoisoned)?;
        if !state.summaries.contains_key(session_id) {
            return Ok(None);
        }
        let revision = state.revision.next()?;
        if let Some(previous) = state.summaries.remove(session_id) {
            state.indexes.remove(&previous);
        }
        state.revision = revision;
        Ok(Some(CatalogChange { revision }))
    }

    /// Returns one compact summary by stable ID.
    ///
    /// This function clones prepared data only; it performs no process, PTY,
    /// detection, health-check, persistence, or network IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a poisoned-lock error.
    pub fn get(&self, session_id: &SessionId) -> CatalogResult<Option<SessionSummary>> {
        let state = self.state.read().map_err(|_| CatalogError::LockPoisoned)?;
        Ok(state.summaries.get(session_id).cloned())
    }

    /// Evaluates a bounded, server-side session query.
    ///
    /// Membership uses maintained indexes where possible. Name substring
    /// matching is applied only to the indexed candidate set. Details, terminal
    /// state, WorkContext items, processes, and detection are never consulted.
    /// This function performs no external IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed expired/mismatched cursor or poisoned-lock error.
    pub fn query(
        &self,
        query: &SessionQuery,
        cursor: Option<&SessionPageCursor>,
    ) -> CatalogResult<SessionPage> {
        let state = self.state.read().map_err(|_| CatalogError::LockPoisoned)?;
        validate_cursor(cursor, query, state.revision)?;

        let candidate_ids = state.indexes.candidates(&query.filter, &state.summaries);
        let scan = state
            .indexes
            .scan(&state.summaries, query, cursor, candidate_ids.as_ref())?;
        let total = query.include_total.then(|| {
            candidate_ids.as_ref().map_or_else(
                || {
                    state
                        .summaries
                        .values()
                        .filter(|summary| query.filter.matches(summary))
                        .count()
                },
                |ids| {
                    ids.iter()
                        .filter_map(|session_id| state.summaries.get(session_id))
                        .filter(|summary| query.filter.matches(summary))
                        .count()
                },
            )
        });
        let next_cursor = if scan.has_more {
            let after_session_id = scan
                .items
                .last()
                .ok_or(CatalogError::InvariantViolation {
                    rule: "non-terminal page must contain an item",
                })?
                .id()
                .clone();
            Some(SessionPageCursor {
                catalog_revision: state.revision,
                query: query.clone(),
                after_session_id,
            })
        } else {
            None
        };

        let items = scan
            .items
            .iter()
            .map(|summary| SessionSummaryView::from_summary(summary, query.projection))
            .collect();
        Ok(SessionPage {
            items,
            next_cursor,
            snapshot_revision: state.revision,
            total,
            query_cost: scan.cost,
        })
    }

    /// Returns current catalog revision and summary count.
    ///
    /// This function performs no external IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a poisoned-lock error.
    pub fn status(&self) -> CatalogResult<(CatalogRevision, usize)> {
        let state = self.state.read().map_err(|_| CatalogError::LockPoisoned)?;
        Ok((state.revision, state.summaries.len()))
    }
}

impl SessionProjectionWriter for PreparedSessionCatalog {
    fn upsert_session(&self, session: &undercurrent_domain::Session) -> Result<(), PortError> {
        self.upsert(session.summary()).map(|_| ()).map_err(|error| {
            PortError::new(
                PortErrorKind::Other,
                PortOperation::UpdateProjection,
                None,
                error,
            )
        })
    }

    fn remove_summary(&self, session_id: &SessionId) -> Result<(), PortError> {
        self.remove(session_id).map(|_| ()).map_err(|error| {
            PortError::new(
                PortErrorKind::Other,
                PortOperation::UpdateProjection,
                Some(session_id.clone()),
                error,
            )
        })
    }
}

impl EffectiveProfileDirectory for PreparedSessionCatalog {
    fn get(
        &self,
        session_id: &SessionId,
        process: &undercurrent_domain::ProcessInstanceId,
    ) -> Result<Option<undercurrent_domain::EffectiveProfileSnapshot>, PortError> {
        self.get(session_id)
            .map(|summary| summary.and_then(|summary| summary.effective_profile_for(process)))
            .map_err(|error| {
                PortError::new(
                    PortErrorKind::Other,
                    PortOperation::ReadEffectiveProfile,
                    Some(session_id.clone()),
                    error,
                )
            })
    }

    fn transition(
        &self,
        session_id: &SessionId,
        transition: undercurrent_domain::EffectiveProfileTransition,
    ) -> Result<Option<undercurrent_domain::EffectiveProfileSnapshot>, PortError> {
        let Some(mut summary) = self.get(session_id).map_err(|error| {
            PortError::new(
                PortErrorKind::Other,
                PortOperation::ReadEffectiveProfile,
                Some(session_id.clone()),
                error,
            )
        })?
        else {
            return Ok(None);
        };
        let changed = summary
            .apply_effective_profile(transition)
            .map_err(|error| {
                PortError::new(
                    PortErrorKind::Conflict,
                    PortOperation::UpdateEffectiveProfile,
                    Some(session_id.clone()),
                    error,
                )
            })?;
        let snapshot = summary.effective_profile().cloned();
        if changed {
            self.upsert(summary).map_err(|error| {
                PortError::new(
                    PortErrorKind::Other,
                    PortOperation::UpdateEffectiveProfile,
                    Some(session_id.clone()),
                    error,
                )
            })?;
        }
        Ok(snapshot)
    }
}

#[derive(Debug, Default)]
struct CatalogState {
    revision: CatalogRevision,
    summaries: BTreeMap<SessionId, SessionSummary>,
    indexes: SummaryIndexes,
}

#[derive(Debug, Default)]
struct SummaryIndexes {
    lifecycle: BTreeMap<LifecycleKind, BTreeSet<SessionId>>,
    execution: BTreeMap<ExecutionState, BTreeSet<SessionId>>,
    interaction: BTreeMap<InteractionState, BTreeSet<SessionId>>,
    health: BTreeMap<HealthStatus, BTreeSet<SessionId>>,
    profile: BTreeMap<ProfileName, BTreeSet<SessionId>>,
    labels: BTreeMap<(String, String), BTreeSet<SessionId>>,
    active_work: BTreeMap<bool, BTreeSet<SessionId>>,
    exact_name: BTreeMap<SessionName, SessionId>,
    by_name: BTreeSet<(SessionName, SessionId)>,
    by_lifecycle: BTreeSet<(LifecycleKind, SessionId)>,
    by_execution: BTreeSet<(ExecutionState, SessionId)>,
    by_interaction: BTreeSet<(InteractionState, SessionId)>,
    by_health: BTreeSet<(HealthStatus, SessionId)>,
    by_updated_at: BTreeSet<(TimestampMillis, SessionId)>,
    by_restart_count: BTreeSet<(u32, SessionId)>,
}

impl SummaryIndexes {
    fn insert(&mut self, summary: &SessionSummary) {
        insert_index(
            &mut self.lifecycle,
            summary.lifecycle().kind(),
            summary.id(),
        );
        insert_index(
            &mut self.execution,
            summary.foreground().execution().state(),
            summary.id(),
        );
        insert_index(
            &mut self.interaction,
            summary.foreground().interaction().state(),
            summary.id(),
        );
        insert_index(&mut self.health, summary.health().status(), summary.id());
        if let Some(profile) = summary.profile() {
            insert_index(&mut self.profile, profile.clone(), summary.id());
        }
        for (key, value) in summary.labels().iter() {
            insert_index(
                &mut self.labels,
                (key.to_owned(), value.to_owned()),
                summary.id(),
            );
        }
        insert_index(
            &mut self.active_work,
            summary.work().active_item_count > 0,
            summary.id(),
        );
        self.exact_name
            .insert(summary.name().clone(), summary.id().clone());
        self.by_name
            .insert((summary.name().clone(), summary.id().clone()));
        self.by_lifecycle
            .insert((summary.lifecycle().kind(), summary.id().clone()));
        self.by_execution.insert((
            summary.foreground().execution().state(),
            summary.id().clone(),
        ));
        self.by_interaction.insert((
            summary.foreground().interaction().state(),
            summary.id().clone(),
        ));
        self.by_health
            .insert((summary.health().status(), summary.id().clone()));
        self.by_updated_at
            .insert((summary.updated_at(), summary.id().clone()));
        self.by_restart_count
            .insert((summary.restart_count(), summary.id().clone()));
    }

    fn remove(&mut self, summary: &SessionSummary) {
        remove_index(
            &mut self.lifecycle,
            &summary.lifecycle().kind(),
            summary.id(),
        );
        remove_index(
            &mut self.execution,
            &summary.foreground().execution().state(),
            summary.id(),
        );
        remove_index(
            &mut self.interaction,
            &summary.foreground().interaction().state(),
            summary.id(),
        );
        remove_index(&mut self.health, &summary.health().status(), summary.id());
        if let Some(profile) = summary.profile() {
            remove_index(&mut self.profile, profile, summary.id());
        }
        for (key, value) in summary.labels().iter() {
            remove_index(
                &mut self.labels,
                &(key.to_owned(), value.to_owned()),
                summary.id(),
            );
        }
        remove_index(
            &mut self.active_work,
            &(summary.work().active_item_count > 0),
            summary.id(),
        );
        self.exact_name.remove(summary.name());
        self.by_name
            .remove(&(summary.name().clone(), summary.id().clone()));
        self.by_lifecycle
            .remove(&(summary.lifecycle().kind(), summary.id().clone()));
        self.by_execution.remove(&(
            summary.foreground().execution().state(),
            summary.id().clone(),
        ));
        self.by_interaction.remove(&(
            summary.foreground().interaction().state(),
            summary.id().clone(),
        ));
        self.by_health
            .remove(&(summary.health().status(), summary.id().clone()));
        self.by_updated_at
            .remove(&(summary.updated_at(), summary.id().clone()));
        self.by_restart_count
            .remove(&(summary.restart_count(), summary.id().clone()));
    }

    fn candidates(
        &self,
        filter: &SessionFilter,
        summaries: &BTreeMap<SessionId, SessionSummary>,
    ) -> Option<BTreeSet<SessionId>> {
        let mut candidates = None;
        if !filter.ids.is_empty() {
            restrict(
                &mut candidates,
                filter
                    .ids
                    .iter()
                    .filter(|id| summaries.contains_key(*id))
                    .cloned()
                    .collect(),
            );
        }
        if let Some(name) = &filter.name {
            restrict(
                &mut candidates,
                self.exact_name.get(name).cloned().into_iter().collect(),
            );
        }
        restrict_union(&mut candidates, &self.lifecycle, &filter.lifecycles);
        restrict_union(&mut candidates, &self.execution, &filter.executions);
        restrict_union(&mut candidates, &self.interaction, &filter.interactions);
        restrict_union(&mut candidates, &self.health, &filter.health);
        if let Some(profile) = &filter.profile {
            restrict(
                &mut candidates,
                self.profile.get(profile).cloned().unwrap_or_default(),
            );
        }
        for label in filter.labels.iter() {
            restrict(
                &mut candidates,
                self.labels
                    .get(&(label.0.to_owned(), label.1.to_owned()))
                    .cloned()
                    .unwrap_or_default(),
            );
        }
        if let Some(has_active_work) = filter.has_active_work {
            restrict(
                &mut candidates,
                self.active_work
                    .get(&has_active_work)
                    .cloned()
                    .unwrap_or_default(),
            );
        }
        candidates
    }

    fn scan(
        &self,
        summaries: &BTreeMap<SessionId, SessionSummary>,
        query: &SessionQuery,
        cursor: Option<&SessionPageCursor>,
        candidates: Option<&BTreeSet<SessionId>>,
    ) -> CatalogResult<PageScan> {
        let cursor_summary = cursor
            .map(|cursor| {
                summaries
                    .get(cursor.after_session_id())
                    .ok_or(CatalogError::CursorExpired {
                        cursor_revision: cursor.catalog_revision,
                        current_revision: cursor.catalog_revision,
                    })
            })
            .transpose()?;
        if let Some(candidates) = candidates {
            return scan_candidates(candidates, summaries, query, cursor_summary);
        }
        match query.sort.field {
            SessionSortField::Name => scan_sort_index(
                &self.by_name,
                cursor_summary.map(|summary| (summary.name().clone(), summary.id().clone())),
                summaries,
                query,
                candidates,
            ),
            SessionSortField::Lifecycle => scan_sort_index(
                &self.by_lifecycle,
                cursor_summary.map(|summary| (summary.lifecycle().kind(), summary.id().clone())),
                summaries,
                query,
                candidates,
            ),
            SessionSortField::Execution => scan_sort_index(
                &self.by_execution,
                cursor_summary.map(|summary| {
                    (
                        summary.foreground().execution().state(),
                        summary.id().clone(),
                    )
                }),
                summaries,
                query,
                candidates,
            ),
            SessionSortField::Interaction => scan_sort_index(
                &self.by_interaction,
                cursor_summary.map(|summary| {
                    (
                        summary.foreground().interaction().state(),
                        summary.id().clone(),
                    )
                }),
                summaries,
                query,
                candidates,
            ),
            SessionSortField::Health => scan_sort_index(
                &self.by_health,
                cursor_summary.map(|summary| (summary.health().status(), summary.id().clone())),
                summaries,
                query,
                candidates,
            ),
            SessionSortField::UpdatedAt => scan_sort_index(
                &self.by_updated_at,
                cursor_summary.map(|summary| (summary.updated_at(), summary.id().clone())),
                summaries,
                query,
                candidates,
            ),
            SessionSortField::RestartCount => scan_sort_index(
                &self.by_restart_count,
                cursor_summary.map(|summary| (summary.restart_count(), summary.id().clone())),
                summaries,
                query,
                candidates,
            ),
        }
    }
}

struct PageScan {
    items: Vec<SessionSummary>,
    has_more: bool,
    cost: QueryCost,
}

fn scan_candidates(
    candidates: &BTreeSet<SessionId>,
    summaries: &BTreeMap<SessionId, SessionSummary>,
    query: &SessionQuery,
    cursor_summary: Option<&SessionSummary>,
) -> CatalogResult<PageScan> {
    let mut matching = Vec::with_capacity(candidates.len().min(query.page_size.get() + 1));
    for session_id in candidates {
        let Some(summary) = summaries.get(session_id) else {
            return Err(CatalogError::InvariantViolation {
                rule: "filter index references a missing summary",
            });
        };
        if query.filter.matches(summary) {
            matching.push(summary);
        }
    }
    matching.sort_by(|left, right| compare_summaries(left, right, query.sort));
    if let Some(cursor) = cursor_summary {
        matching.retain(|summary| compare_summaries(summary, cursor, query.sort).is_gt());
    }
    let has_more = matching.len() > query.page_size.get();
    matching.truncate(query.page_size.get());
    Ok(PageScan {
        items: matching.into_iter().cloned().collect(),
        has_more,
        cost: QueryCost {
            index_entries_examined: candidates.len(),
        },
    })
}

fn compare_summaries(
    left: &SessionSummary,
    right: &SessionSummary,
    sort: SessionSort,
) -> std::cmp::Ordering {
    let ordering = match sort.field {
        SessionSortField::Name => left.name().cmp(right.name()),
        SessionSortField::Lifecycle => left.lifecycle().kind().cmp(&right.lifecycle().kind()),
        SessionSortField::Execution => left
            .foreground()
            .execution()
            .state()
            .cmp(&right.foreground().execution().state()),
        SessionSortField::Interaction => left
            .foreground()
            .interaction()
            .state()
            .cmp(&right.foreground().interaction().state()),
        SessionSortField::Health => left.health().status().cmp(&right.health().status()),
        SessionSortField::UpdatedAt => left.updated_at().cmp(&right.updated_at()),
        SessionSortField::RestartCount => left.restart_count().cmp(&right.restart_count()),
    }
    .then_with(|| left.id().cmp(right.id()));
    match sort.direction {
        SortDirection::Ascending => ordering,
        SortDirection::Descending => ordering.reverse(),
    }
}

fn scan_sort_index<K: Ord>(
    index: &BTreeSet<(K, SessionId)>,
    cursor_key: Option<(K, SessionId)>,
    summaries: &BTreeMap<SessionId, SessionSummary>,
    query: &SessionQuery,
    candidates: Option<&BTreeSet<SessionId>>,
) -> CatalogResult<PageScan> {
    match (query.sort.direction, cursor_key) {
        (SortDirection::Ascending, None) => scan_ordered_ids(
            index.iter().map(|(_, session_id)| session_id),
            summaries,
            query,
            candidates,
        ),
        (SortDirection::Ascending, Some(cursor_key)) => scan_ordered_ids(
            index
                .range((Excluded(cursor_key), Unbounded))
                .map(|(_, session_id)| session_id),
            summaries,
            query,
            candidates,
        ),
        (SortDirection::Descending, None) => scan_ordered_ids(
            index.iter().rev().map(|(_, session_id)| session_id),
            summaries,
            query,
            candidates,
        ),
        (SortDirection::Descending, Some(cursor_key)) => scan_ordered_ids(
            index
                .range((Unbounded, Excluded(cursor_key)))
                .rev()
                .map(|(_, session_id)| session_id),
            summaries,
            query,
            candidates,
        ),
    }
}

fn scan_ordered_ids<'a>(
    ordered_ids: impl Iterator<Item = &'a SessionId>,
    summaries: &BTreeMap<SessionId, SessionSummary>,
    query: &SessionQuery,
    candidates: Option<&BTreeSet<SessionId>>,
) -> CatalogResult<PageScan> {
    let mut items = Vec::with_capacity(query.page_size.get());
    let mut has_more = false;
    let mut index_entries_examined = 0usize;

    for session_id in ordered_ids {
        index_entries_examined = index_entries_examined.saturating_add(1);
        if candidates.is_some_and(|candidates| !candidates.contains(session_id)) {
            continue;
        }
        let Some(summary) = summaries.get(session_id) else {
            return Err(CatalogError::InvariantViolation {
                rule: "sort index references a missing summary",
            });
        };
        if !query.filter.matches(summary) {
            continue;
        }
        if items.len() == query.page_size.get() {
            has_more = true;
            break;
        }
        items.push(summary.clone());
    }
    Ok(PageScan {
        items,
        has_more,
        cost: QueryCost {
            index_entries_examined,
        },
    })
}

fn validate_cursor(
    cursor: Option<&SessionPageCursor>,
    query: &SessionQuery,
    current_revision: CatalogRevision,
) -> CatalogResult<()> {
    let Some(cursor) = cursor else {
        return Ok(());
    };
    if cursor.catalog_revision != current_revision {
        return Err(CatalogError::CursorExpired {
            cursor_revision: cursor.catalog_revision,
            current_revision,
        });
    }
    if cursor.query != *query {
        return Err(CatalogError::CursorQueryMismatch);
    }
    Ok(())
}

fn restrict(current: &mut Option<BTreeSet<SessionId>>, restriction: BTreeSet<SessionId>) {
    *current = Some(match current.take() {
        None => restriction,
        Some(previous) => previous.intersection(&restriction).cloned().collect(),
    });
}

fn restrict_union<K: Ord>(
    current: &mut Option<BTreeSet<SessionId>>,
    index: &BTreeMap<K, BTreeSet<SessionId>>,
    selected: &BTreeSet<K>,
) {
    if selected.is_empty() {
        return;
    }
    let union = selected
        .iter()
        .filter_map(|key| index.get(key))
        .flat_map(BTreeSet::iter)
        .cloned()
        .collect();
    restrict(current, union);
}

fn insert_index<K: Ord + Clone>(
    index: &mut BTreeMap<K, BTreeSet<SessionId>>,
    key: K,
    session_id: &SessionId,
) {
    index.entry(key).or_default().insert(session_id.clone());
}

fn remove_index<K: Ord>(
    index: &mut BTreeMap<K, BTreeSet<SessionId>>,
    key: &K,
    session_id: &SessionId,
) {
    let should_remove = index.get_mut(key).is_some_and(|ids| {
        ids.remove(session_id);
        ids.is_empty()
    });
    if should_remove {
        index.remove(key);
    }
}
