// SPDX-License-Identifier: Apache-2.0

//! Declared work relationships that complement, but never replace, foreground observation.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    DomainError, DomainResult, Labels, Metadata, Revision, SessionId, TimestampMillis, WorkItemId,
    WorkOwner,
};

const MAX_TITLE_BYTES: usize = 256;
const MAX_DESCRIPTION_BYTES: usize = 2_048;
const MAX_PROVENANCE_BYTES: usize = 512;
const MAX_TARGETS_PER_ITEM: usize = 64;

/// Bounded declaration provenance retained for diagnostics and audit views.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkProvenance(String);

impl WorkProvenance {
    /// Constructs non-empty provenance without control characters.
    ///
    /// The value identifies how or where a declaration was observed. It may be
    /// a product-defined identifier or URI-like reference, but it is not
    /// interpreted by the domain. This function performs no IO and does not
    /// panic.
    ///
    /// # Errors
    ///
    /// Returns a validation error for empty, oversized, or control-character
    /// input.
    pub fn new(value: impl Into<String>) -> DomainResult<Self> {
        let value = value.into();
        validate_text("work_provenance", &value, MAX_PROVENANCE_BYTES, false)?;
        Ok(Self(value))
    }

    /// Returns the uninterpreted provenance value.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn from_owner(owner: &WorkOwner) -> Self {
        Self(owner.as_str().to_owned())
    }
}

/// Stable compound identity of a WorkContext declaration.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// Owner-scoped identity for a declared work item or run.
///
/// `item_id` is opaque outside its owner. The same item/run identifier declared
/// by two owners denotes two distinct items and must never be joined without
/// matching both fields.
pub struct WorkItemKey {
    owner: WorkOwner,
    id: WorkItemId,
}

impl WorkItemKey {
    /// Constructs a key from an owner and owner-scoped item ID.
    ///
    /// Both values have already been validated. The function performs no IO,
    /// cannot fail, and does not panic.
    pub const fn new(owner: WorkOwner, id: WorkItemId) -> Self {
        Self { owner, id }
    }

    /// Returns the declaring owner.
    pub const fn owner(&self) -> &WorkOwner {
        &self.owner
    }

    /// Returns the owner-scoped item identifier.
    pub const fn id(&self) -> &WorkItemId {
        &self.id
    }
}

/// Typed relationship between a session and declared work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WorkRelationKind {
    /// The session cannot make complete progress until the work resolves.
    DependsOn,
    /// The session delegated the work to another actor or session.
    Delegated,
    /// The session spawned the related work.
    Spawned,
    /// The work is relevant but has no stronger declared relationship.
    Related,
}

/// Current declared status of a WorkContext item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// Exact work outcome and retry visibility.
///
/// `Completed`, `Failed`, `Cancelled`, `DeliveryFailed`, and `Expired` are
/// intentionally distinct outcomes. In particular, `DeliveryFailed` remains
/// active so an owner can retry delivery; it must not be reported as completed.
pub enum WorkStatus {
    /// Related work is actively running.
    Running,
    /// Related work is waiting without a declared blocker.
    Waiting,
    /// Related work is blocked.
    Blocked,
    /// Related work completed successfully.
    Completed,
    /// Related work failed.
    Failed,
    /// Related work was cancelled.
    Cancelled,
    /// Work could not be delivered to its intended executor.
    DeliveryFailed,
    /// The declaration expired without an explicit terminal update.
    Expired,
}

impl WorkStatus {
    /// Returns whether the declaration still represents active work.
    pub const fn is_active(self) -> bool {
        matches!(
            self,
            Self::Running | Self::Waiting | Self::Blocked | Self::DeliveryFailed
        )
    }
}

/// A typed target referenced by a WorkContext item.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WorkTarget {
    /// A directly identified Undercurrent session.
    Session(SessionId),
    /// Sessions matching all supplied labels.
    Labels(Labels),
    /// A target owned by an external product.
    External {
        /// Stable target-system identifier.
        system: WorkOwner,
        /// Stable target identifier within that system.
        id: WorkItemId,
    },
}

/// One externally declared item attached to a session's WorkContext.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkItem {
    key: WorkItemKey,
    relation: WorkRelationKind,
    title: String,
    description: Option<String>,
    status: WorkStatus,
    targets: BTreeSet<WorkTarget>,
    provenance: WorkProvenance,
    metadata: Metadata,
    created_at: TimestampMillis,
    updated_at: TimestampMillis,
    expires_at: Option<TimestampMillis>,
}

impl WorkItem {
    /// Constructs a validated WorkContext item.
    ///
    /// Targets are deduplicated and metadata is already bounded. Expiry cannot
    /// precede creation. The function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a validation or capacity error for invalid text, targets, or
    /// timestamps.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        key: WorkItemKey,
        relation: WorkRelationKind,
        title: impl Into<String>,
        description: Option<String>,
        status: WorkStatus,
        targets: impl IntoIterator<Item = WorkTarget>,
        metadata: Metadata,
        created_at: TimestampMillis,
        updated_at: TimestampMillis,
        expires_at: Option<TimestampMillis>,
    ) -> DomainResult<Self> {
        let provenance = WorkProvenance::from_owner(key.owner());
        Self::new_with_provenance(
            key,
            relation,
            title,
            description,
            status,
            targets,
            provenance,
            metadata,
            created_at,
            updated_at,
            expires_at,
        )
    }

    /// Constructs a validated WorkContext item with explicit provenance.
    ///
    /// This is the external-declaration constructor. Callers restoring older
    /// records may use [`Self::new`], which derives provenance from the owner.
    /// The function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a validation or capacity error for invalid text, targets, or
    /// timestamps.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_provenance(
        key: WorkItemKey,
        relation: WorkRelationKind,
        title: impl Into<String>,
        description: Option<String>,
        status: WorkStatus,
        targets: impl IntoIterator<Item = WorkTarget>,
        provenance: WorkProvenance,
        metadata: Metadata,
        created_at: TimestampMillis,
        updated_at: TimestampMillis,
        expires_at: Option<TimestampMillis>,
    ) -> DomainResult<Self> {
        let title = title.into();
        validate_text("work_title", &title, MAX_TITLE_BYTES, false)?;
        if let Some(description) = &description {
            validate_text("work_description", description, MAX_DESCRIPTION_BYTES, true)?;
        }
        if updated_at < created_at || expires_at.is_some_and(|expiry| expiry < created_at) {
            return Err(DomainError::InvalidValue {
                field: "work_timestamps",
                reason: crate::InvalidValueReason::InvalidCombination {
                    rule: "work update and expiry times cannot precede creation",
                },
            });
        }
        let targets: BTreeSet<_> = targets.into_iter().collect();
        if targets.len() > MAX_TARGETS_PER_ITEM {
            return Err(DomainError::CapacityExceeded {
                resource: "work_targets",
                limit: MAX_TARGETS_PER_ITEM,
            });
        }
        for target in &targets {
            if let WorkTarget::Labels(labels) = target
                && labels.is_empty()
            {
                return Err(DomainError::InvalidValue {
                    field: "work_target_labels",
                    reason: crate::InvalidValueReason::Empty,
                });
            }
        }
        Ok(Self {
            key,
            relation,
            title,
            description,
            status,
            targets,
            provenance,
            metadata,
            created_at,
            updated_at,
            expires_at,
        })
    }

    /// Returns the stable compound key.
    pub const fn key(&self) -> &WorkItemKey {
        &self.key
    }

    /// Returns the typed relationship.
    pub const fn relation(&self) -> WorkRelationKind {
        self.relation
    }

    /// Returns the display title.
    pub fn title(&self) -> &str {
        &self.title
    }

    /// Returns the optional safe description.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Returns the current declared status.
    pub const fn status(&self) -> WorkStatus {
        self.status
    }

    /// Iterates over targets in deterministic order.
    pub fn targets(&self) -> impl Iterator<Item = &WorkTarget> {
        self.targets.iter()
    }

    /// Returns the declaration's bounded, uninterpreted provenance.
    pub const fn provenance(&self) -> &WorkProvenance {
        &self.provenance
    }

    /// Returns metadata to an explicitly authorized detail caller.
    pub const fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Returns the creation timestamp.
    pub const fn created_at(&self) -> TimestampMillis {
        self.created_at
    }

    /// Returns the last update timestamp.
    pub const fn updated_at(&self) -> TimestampMillis {
        self.updated_at
    }

    /// Returns the optional expiry timestamp.
    pub const fn expires_at(&self) -> Option<TimestampMillis> {
        self.expires_at
    }

    /// Updates status and timestamp, returning whether content changed.
    ///
    /// Stale timestamps are rejected so callers cannot move a declaration
    /// backward in time. The function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a validation error when `updated_at` predates the current update.
    pub fn update_status(
        &mut self,
        status: WorkStatus,
        updated_at: TimestampMillis,
    ) -> DomainResult<bool> {
        if updated_at < self.updated_at {
            return Err(DomainError::InvalidValue {
                field: "work_updated_at",
                reason: crate::InvalidValueReason::InvalidCombination {
                    rule: "work updates must not move backward in time",
                },
            });
        }
        if self.status == status && self.updated_at == updated_at {
            return Ok(false);
        }
        self.status = status;
        self.updated_at = updated_at;
        Ok(true)
    }

    fn expire(&mut self, now: TimestampMillis) -> bool {
        if self.should_expire(now) {
            self.status = WorkStatus::Expired;
            self.updated_at = now;
            return true;
        }
        false
    }

    fn should_expire(&self, now: TimestampMillis) -> bool {
        self.status.is_active()
            && now >= self.updated_at
            && self.expires_at.is_some_and(|expiry| expiry <= now)
    }
}

/// Compact aggregate carried by session summary projections.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
/// Compact informational counts across the context's owners and item/run IDs.
///
/// A summary is not orchestration authority. A consumer must inspect detailed
/// items and match its own `(owner, item_id)` before deciding the state or
/// outcome of work it owns.
pub struct WorkSummary {
    /// Total declared items, including terminal and expired items.
    pub item_count: usize,
    /// Total explicit targets across all items.
    pub target_count: usize,
    /// Number of items that still represent active work.
    pub active_item_count: usize,
    /// Active target count, treating a targetless active item as one visible unit.
    pub active_target_count: usize,
    /// Highest-priority aggregate status, or `None` when no items exist.
    pub aggregate_status: Option<WorkStatus>,
    /// Revision of the WorkContext used to build this summary.
    pub revision: Revision,
}

/// A session's bounded, revisioned declared work relationships.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkContext {
    items: BTreeMap<WorkItemKey, WorkItem>,
    revision: Revision,
    max_items: usize,
}

impl WorkContext {
    /// Creates an empty WorkContext with an explicit hard item limit.
    ///
    /// # Errors
    ///
    /// Returns a validation error when `max_items` is zero. The function
    /// performs no IO and does not panic.
    pub fn new(max_items: usize) -> DomainResult<Self> {
        if max_items == 0 {
            return Err(DomainError::InvalidValue {
                field: "max_work_items",
                reason: crate::InvalidValueReason::OutOfRange {
                    min: 1,
                    max: usize::MAX as u64,
                },
            });
        }
        Ok(Self {
            items: BTreeMap::new(),
            revision: Revision::INITIAL,
            max_items,
        })
    }

    /// Rehydrates exact persisted WorkContext content and revision.
    ///
    /// Items are canonicalized by compound key. The persisted revision may be
    /// greater than the current item count because replacement, removal, and
    /// expiry also advance it, but it cannot be lower than the number of
    /// distinct items still present. This function performs no IO and does not
    /// panic.
    ///
    /// # Errors
    ///
    /// Returns corrupt-state failure for a zero limit, duplicate keys, excess
    /// items, or an impossible revision.
    pub fn rehydrate(
        max_items: usize,
        revision: Revision,
        items: Vec<WorkItem>,
    ) -> DomainResult<Self> {
        if max_items == 0 {
            return Err(corrupt_work_context(
                "persisted WorkContext item limit must be non-zero",
            ));
        }
        if items.len() > max_items {
            return Err(corrupt_work_context(
                "persisted WorkContext exceeds its item limit",
            ));
        }
        let mut by_key = BTreeMap::new();
        for item in items {
            if by_key.insert(item.key().clone(), item).is_some() {
                return Err(corrupt_work_context(
                    "persisted WorkContext item keys must be unique",
                ));
            }
        }
        let minimum_revision = u64::try_from(by_key.len()).unwrap_or(u64::MAX);
        if revision.value() < minimum_revision {
            return Err(corrupt_work_context(
                "WorkContext revision cannot precede retained item creation",
            ));
        }
        Ok(Self {
            items: by_key,
            revision,
            max_items,
        })
    }

    /// Returns the current WorkContext revision.
    pub const fn revision(&self) -> Revision {
        self.revision
    }

    /// Returns the configured item limit.
    pub const fn max_items(&self) -> usize {
        self.max_items
    }

    /// Returns the number of stored items.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns whether the context has no declarations.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns an item by compound key.
    pub fn get(&self, key: &WorkItemKey) -> Option<&WorkItem> {
        self.items.get(key)
    }

    /// Iterates over items in deterministic compound-key order.
    pub fn items(&self) -> impl Iterator<Item = &WorkItem> {
        self.items.values()
    }

    /// Inserts or replaces a declaration idempotently.
    ///
    /// Identical content is a no-op and does not advance the revision. A new
    /// item cannot exceed the configured limit. This function performs no IO
    /// and does not panic.
    ///
    /// # Errors
    ///
    /// Returns validation failure for an update older than the retained item's
    /// update time, a capacity error, or revision exhaustion. Rejection leaves
    /// the retained declaration and context revision unchanged.
    pub fn upsert(&mut self, item: WorkItem) -> DomainResult<bool> {
        if self.items.get(item.key()) == Some(&item) {
            return Ok(false);
        }
        if self
            .items
            .get(item.key())
            .is_some_and(|current| item.updated_at() < current.updated_at())
        {
            return Err(DomainError::InvalidValue {
                field: "work_updated_at",
                reason: crate::InvalidValueReason::InvalidCombination {
                    rule: "work updates must not move backward in time",
                },
            });
        }
        if !self.items.contains_key(item.key()) && self.items.len() == self.max_items {
            return Err(DomainError::CapacityExceeded {
                resource: "work_items",
                limit: self.max_items,
            });
        }
        let next_revision = self.revision.next("work_context")?;
        self.items.insert(item.key().clone(), item);
        self.revision = next_revision;
        Ok(true)
    }

    /// Removes a declaration by key and returns it.
    ///
    /// # Errors
    ///
    /// Returns `WorkItemNotFound` when absent or revision exhaustion when the
    /// change cannot be represented. This function performs no IO and does not
    /// panic.
    pub fn remove(&mut self, key: &WorkItemKey) -> DomainResult<WorkItem> {
        if !self.items.contains_key(key) {
            return Err(DomainError::WorkItemNotFound);
        }
        let next_revision = self.revision.next("work_context")?;
        let item = self
            .items
            .remove(key)
            .ok_or(DomainError::WorkItemNotFound)?;
        self.revision = next_revision;
        Ok(item)
    }

    /// Marks active declarations whose expiry has passed as `expired`.
    ///
    /// The returned keys identify changed declarations for indexed event
    /// publication. A batch advances the context revision once. This function
    /// performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns revision exhaustion when at least one item expires and the
    /// revision cannot advance.
    pub fn expire(&mut self, now: TimestampMillis) -> DomainResult<Vec<WorkItemKey>> {
        let expired: Vec<_> = self
            .items
            .values()
            .filter(|item| item.should_expire(now))
            .map(|item| item.key().clone())
            .collect();
        if expired.is_empty() {
            return Ok(expired);
        }
        // Reserve the batch's single revision before changing any item. An
        // exhausted revision must leave all declarations and timestamps intact.
        // See [expiry revisions](../docs/internals/work-and-queries.md#active-status-expiry-and-revision-behavior).
        let revision = self.revision.next("work_context")?;
        for key in &expired {
            if let Some(item) = self.items.get_mut(key) {
                item.expire(now);
            }
        }
        self.revision = revision;
        Ok(expired)
    }

    /// Returns whether calling [`Self::expire`] at `now` would change at least
    /// one declaration.
    ///
    /// This function performs no IO and does not panic.
    pub fn has_expired_items(&self, now: TimestampMillis) -> bool {
        self.items.values().any(|item| item.should_expire(now))
    }

    /// Returns direct session targets used to maintain a reverse relationship
    /// index in the application read model.
    pub fn direct_session_targets(&self) -> BTreeSet<SessionId> {
        self.items
            .values()
            .flat_map(WorkItem::targets)
            .filter_map(|target| match target {
                WorkTarget::Session(session_id) => Some(session_id.clone()),
                WorkTarget::Labels(_) | WorkTarget::External { .. } => None,
            })
            .collect()
    }

    /// Builds the compact summary included in a session read model.
    ///
    /// This function performs no IO and does not panic.
    /// Returns compact informational counts, not an owner-scoped work result.
    ///
    /// The aggregate status preserves priority for visibility but cannot replace
    /// inspection of the exact `(owner, item_id)` or collapse terminal outcomes.
    pub fn summary(&self) -> WorkSummary {
        let mut summary = WorkSummary {
            revision: self.revision,
            ..WorkSummary::default()
        };
        let mut selected_priority = None;
        for item in self.items.values() {
            summary.item_count += 1;
            summary.target_count += item.targets.len();
            if item.status.is_active() {
                summary.active_item_count += 1;
                summary.active_target_count += item.targets.len().max(1);
            }
            let priority = status_priority(item.status);
            if selected_priority.is_none_or(|selected| priority < selected) {
                selected_priority = Some(priority);
                summary.aggregate_status = Some(item.status);
            }
        }
        summary
    }
}

fn corrupt_work_context(rule: &'static str) -> DomainError {
    DomainError::CorruptState {
        scope: "work_context",
        rule,
    }
}

fn validate_text(
    field: &'static str,
    value: &str,
    max: usize,
    allow_empty: bool,
) -> DomainResult<()> {
    if !allow_empty && value.is_empty() {
        return Err(DomainError::InvalidValue {
            field,
            reason: crate::InvalidValueReason::Empty,
        });
    }
    if value.len() > max {
        return Err(DomainError::InvalidValue {
            field,
            reason: crate::InvalidValueReason::TooLong { max },
        });
    }
    if let Some((index, character)) = value
        .char_indices()
        .find(|(_, character)| character.is_control())
    {
        return Err(DomainError::InvalidValue {
            field,
            reason: crate::InvalidValueReason::InvalidCharacter { index, character },
        });
    }
    Ok(())
}

const fn status_priority(status: WorkStatus) -> u8 {
    match status {
        WorkStatus::DeliveryFailed => 0,
        WorkStatus::Blocked => 1,
        WorkStatus::Waiting => 2,
        WorkStatus::Running => 3,
        WorkStatus::Failed => 4,
        WorkStatus::Cancelled => 5,
        WorkStatus::Expired => 6,
        WorkStatus::Completed => 7,
    }
}
