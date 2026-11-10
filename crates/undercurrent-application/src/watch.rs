// SPDX-License-Identifier: Apache-2.0

//! Bounded, coalescing query-window subscriptions for frontend session lists.

use std::collections::BTreeMap;

use undercurrent_domain::{CatalogRevision, Revision, SessionId, SessionQuery};

use crate::{CatalogError, CatalogResult, PreparedSessionCatalog, QueryCost, SessionSummaryView};

/// Stable delta kind for membership in one watched query window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionDeltaKind {
    /// A summary entered the watched page.
    Added,
    /// A summary already in the page changed content.
    Updated,
    /// A summary left the watched page, even if it remains elsewhere in the
    /// complete filtered result.
    Removed,
}

/// One entity delta within a bounded query-window refresh.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionDelta {
    kind: SessionDeltaKind,
    session_id: SessionId,
    session_revision: Revision,
    summary: Option<SessionSummaryView>,
}

impl SessionDelta {
    /// Returns the stable delta kind.
    pub const fn kind(&self) -> SessionDeltaKind {
        self.kind
    }

    /// Returns affected stable session ID.
    pub const fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    /// Returns the affected entity revision, including the last known revision
    /// for a removed item.
    pub const fn session_revision(&self) -> Revision {
        self.session_revision
    }

    /// Returns current summary for added and updated items.
    ///
    /// Removed items intentionally carry no stale summary payload.
    pub const fn summary(&self) -> Option<&SessionSummaryView> {
        self.summary.as_ref()
    }
}

/// Coalesced refresh from one catalog revision to another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryWatchBatch {
    /// Previous revision represented by the client-side window.
    pub from_revision: CatalogRevision,
    /// Current authoritative catalog revision.
    pub to_revision: CatalogRevision,
    /// Added, updated, and removed entity payloads.
    pub deltas: Vec<SessionDelta>,
    /// Authoritative order of stable IDs in the current bounded page.
    pub order: Vec<SessionId>,
    /// Exact total when the watched query requested it.
    pub total: Option<usize>,
    /// Safe operational cost of extracting the replacement page.
    pub query_cost: QueryCost,
}

/// Per-client state for one bounded watched query window.
///
/// The state stores at most one configured page. It is owned by a connection
/// subscription, not the global catalog, so dropping a logical stream releases
/// all of its frontend-specific state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionQueryWatch {
    query: SessionQuery,
    snapshot_revision: CatalogRevision,
    items: Vec<SessionSummaryView>,
    total: Option<usize>,
    query_cost: QueryCost,
}

impl SessionQueryWatch {
    /// Returns watched query specification.
    pub const fn query(&self) -> &SessionQuery {
        &self.query
    }

    /// Returns catalog revision represented by the current window.
    pub const fn snapshot_revision(&self) -> CatalogRevision {
        self.snapshot_revision
    }

    /// Returns current bounded page in authoritative order.
    pub fn items(&self) -> &[SessionSummaryView] {
        &self.items
    }

    /// Returns exact total only when explicitly requested by the query.
    pub const fn total(&self) -> Option<usize> {
        self.total
    }

    /// Returns the cost of the most recent prepared-catalog extraction.
    pub const fn query_cost(&self) -> QueryCost {
        self.query_cost
    }
}

impl PreparedSessionCatalog {
    /// Opens a first-page watch from one internally consistent catalog snapshot.
    ///
    /// The caller sends `items`, `snapshot_revision`, and optional `total` as
    /// the initial snapshot. This function reads prepared summaries only and
    /// performs no process, PTY, detection, health-check, or network IO.
    ///
    /// # Errors
    ///
    /// Returns the same typed failures as [`PreparedSessionCatalog::query`].
    pub fn open_watch(&self, query: SessionQuery) -> CatalogResult<SessionQueryWatch> {
        let page = self.query(&query, None)?;
        Ok(SessionQueryWatch {
            query,
            snapshot_revision: page.snapshot_revision,
            items: page.items,
            total: page.total,
            query_cost: page.query_cost,
        })
    }

    /// Advances a watch to the latest catalog revision using a bounded diff.
    ///
    /// Multiple intervening catalog changes are coalesced into the difference
    /// between the previous page and the latest page. Changes outside the page
    /// return `None` after advancing the watch revision, unless an explicitly
    /// requested total changed. This function performs no external IO and does
    /// not panic.
    ///
    /// # Errors
    ///
    /// Returns the same typed failures as [`PreparedSessionCatalog::query`].
    pub fn refresh_watch(
        &self,
        watch: &mut SessionQueryWatch,
    ) -> CatalogResult<Option<QueryWatchBatch>> {
        let page = self.query(&watch.query, None)?;
        if page.snapshot_revision == watch.snapshot_revision {
            return Ok(None);
        }

        let previous_revision = watch.snapshot_revision;
        let previous: BTreeMap<_, _> = watch
            .items
            .iter()
            .map(|summary| (summary.id().clone(), summary))
            .collect();
        let current: BTreeMap<_, _> = page
            .items
            .iter()
            .map(|summary| (summary.id().clone(), summary))
            .collect();
        let mut deltas = Vec::new();

        for session_id in previous.keys() {
            if !current.contains_key(session_id) {
                let session_revision = previous
                    .get(session_id)
                    .map(|summary| summary.revision())
                    .ok_or(CatalogError::InvariantViolation {
                    rule: "removed watch member must have a previous summary",
                })?;
                deltas.push(SessionDelta {
                    kind: SessionDeltaKind::Removed,
                    session_id: session_id.clone(),
                    session_revision,
                    summary: None,
                });
            }
        }
        for summary in &page.items {
            match previous.get(summary.id()) {
                None => deltas.push(SessionDelta {
                    kind: SessionDeltaKind::Added,
                    session_id: summary.id().clone(),
                    session_revision: summary.revision(),
                    summary: Some(summary.clone()),
                }),
                Some(previous) if *previous != summary => deltas.push(SessionDelta {
                    kind: SessionDeltaKind::Updated,
                    session_id: summary.id().clone(),
                    session_revision: summary.revision(),
                    summary: Some(summary.clone()),
                }),
                Some(_) => {}
            }
        }

        let order: Vec<_> = page
            .items
            .iter()
            .map(|summary| summary.id().clone())
            .collect();
        let total_changed = watch.total != page.total;
        let order_changed = watch
            .items
            .iter()
            .map(SessionSummaryView::id)
            .ne(page.items.iter().map(SessionSummaryView::id));

        watch.snapshot_revision = page.snapshot_revision;
        watch.items = page.items;
        watch.total = page.total;
        watch.query_cost = page.query_cost;

        if deltas.is_empty() && !total_changed && !order_changed {
            return Ok(None);
        }
        Ok(Some(QueryWatchBatch {
            from_revision: previous_revision,
            to_revision: watch.snapshot_revision,
            deltas,
            order,
            total: watch.total,
            query_cost: page.query_cost,
        }))
    }
}
