// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use undercurrent_application::*;
use undercurrent_domain::*;

fn limits() -> ExecutionLimits {
    ExecutionLimits {
        max_arguments: 8,
        max_command_bytes: 1_024,
        max_environment_entries: 8,
        max_environment_bytes: 2_048,
        max_path_bytes: 512,
        max_health_rules: 8,
        max_health_rule_bytes: 512,
    }
}

fn configuration(index: usize) -> SessionConfiguration {
    let limits = limits();
    SessionConfiguration::new(
        LaunchSpec::new(
            CommandSpec::new("sh", vec!["-lc".to_owned(), "sleep 30".to_owned()], limits)
                .expect("test command is valid"),
            EnvironmentVariables::default(),
            None,
            None,
            TerminalSize::new(80, 24, 500, 200).expect("terminal size is valid"),
        ),
        ExecutionPolicy::new(
            RestartConfig::never(),
            StopPolicy::new(StopSignal::Terminate, Duration::from_secs(5))
                .expect("stop policy is valid"),
            true,
            false,
        ),
        HealthRules::new(Vec::new(), limits).expect("health rules are valid"),
        Some(
            ProfileName::new(if index.is_multiple_of(2) {
                "agent"
            } else {
                "server"
            })
            .expect("profile is valid"),
        ),
        Labels::new([
            (
                "env".to_owned(),
                if index.is_multiple_of(3) {
                    "prod".to_owned()
                } else {
                    "dev".to_owned()
                },
            ),
            ("group".to_owned(), format!("{}", index % 10)),
        ])
        .expect("labels are valid"),
        Metadata::default(),
    )
}

fn session(index: usize) -> Session {
    let created_at = TimestampMillis::new(1);
    let mut session = Session::create(
        SessionId::new(format!("session-{index:04}")).expect("session ID is valid"),
        SessionName::new(format!("worker-{index:04}")).expect("session name is valid"),
        configuration(index),
        8,
        created_at,
    )
    .expect("session creation succeeds")
    .0;
    if index.is_multiple_of(2) {
        session
            .record_process_ownership(
                ProcessIdentity::new(
                    ProcessInstanceId::new(format!("process-{index:04}"))
                        .expect("process ID is valid"),
                    TimestampMillis::new(2),
                ),
                TimestampMillis::new(2),
            )
            .expect("process ownership succeeds");
        session
            .transition_lifecycle(LifecycleState::Running, TimestampMillis::new(2))
            .expect("lifecycle transition succeeds");
    }
    if index.is_multiple_of(4) {
        session
            .update_foreground(foreground(ExecutionState::Active, 3))
            .expect("foreground update succeeds");
    }
    if index.is_multiple_of(5) {
        session
            .upsert_work_item(
                WorkItem::new(
                    WorkItemKey::new(
                        WorkOwner::new("workflow").expect("owner is valid"),
                        WorkItemId::new(format!("work-{index}")).expect("work ID is valid"),
                    ),
                    WorkRelationKind::Delegated,
                    "delegated work",
                    None,
                    WorkStatus::Running,
                    Vec::new(),
                    Metadata::default(),
                    TimestampMillis::new(4),
                    TimestampMillis::new(4),
                    None,
                )
                .expect("work item is valid"),
                TimestampMillis::new(4),
            )
            .expect("work update succeeds");
    }
    session
}

fn query(page_size: usize) -> SessionQuery {
    SessionQuery {
        filter: SessionFilter::default(),
        sort: SessionSort::default(),
        projection: SessionProjection::Compact,
        page_size: PageSize::new(page_size, 100).expect("page size is valid"),
        include_total: false,
    }
}

#[test]
fn identical_upsert_is_idempotent_and_revisioned_once() {
    let catalog = PreparedSessionCatalog::new();
    let summary = session(1).summary();
    let first = catalog
        .upsert(summary.clone())
        .expect("upsert succeeds")
        .expect("catalog changed");
    assert_eq!(first.revision().value(), 1);
    assert!(catalog.upsert(summary).expect("upsert succeeds").is_none());
    assert_eq!(
        catalog.status().expect("status succeeds"),
        (CatalogRevision::new(1), 1)
    );
}

#[test]
fn projection_excludes_labels_unless_explicitly_requested() {
    let catalog = PreparedSessionCatalog::new();
    catalog
        .upsert(session(1).summary())
        .expect("upsert succeeds");
    let compact = catalog
        .query(&query(10), None)
        .expect("compact query succeeds");
    assert_eq!(compact.items[0].projection(), SessionProjection::Compact);
    assert!(compact.items[0].labels().is_none());

    let mut with_labels = query(10);
    with_labels.projection = SessionProjection::WithLabels;
    let projected = catalog
        .query(&with_labels, None)
        .expect("label query succeeds");
    assert_eq!(
        projected.items[0]
            .labels()
            .and_then(|labels| labels.get("env")),
        Some("dev")
    );

    let mut operational = query(10);
    operational.projection = SessionProjection::Operational;
    let projected = catalog
        .query(&operational, None)
        .expect("operational query succeeds");
    assert!(projected.items[0].labels().is_none());
}

#[test]
fn three_hundred_summaries_page_over_one_prepared_catalog() {
    let catalog = PreparedSessionCatalog::new();
    for index in 0..300 {
        catalog
            .upsert(session(index).summary())
            .expect("upsert succeeds");
    }
    let mut query = query(25);
    query.include_total = true;
    let mut cursor = None;
    let mut seen = BTreeSet::new();
    let mut page_count = 0;

    loop {
        let page = catalog
            .query(&query, cursor.as_ref())
            .expect("page query succeeds");
        assert!(page.items.len() <= 25);
        assert!(page.query_cost.index_entries_examined <= 26);
        assert_eq!(page.total, Some(300));
        for summary in page.items {
            assert!(seen.insert(summary.id().clone()));
        }
        page_count += 1;
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }

    assert_eq!(page_count, 12);
    assert_eq!(seen.len(), 300);
}

#[test]
fn secondary_indexes_intersect_lifecycle_profile_labels_and_active_work() {
    let catalog = PreparedSessionCatalog::new();
    for index in 0..300 {
        catalog
            .upsert(session(index).summary())
            .expect("upsert succeeds");
    }
    let mut query = query(100);
    query.filter.lifecycles.insert(LifecycleKind::Running);
    query.filter.profile = Some(ProfileName::new("agent").expect("profile is valid"));
    query.filter.labels =
        Labels::new([("env".to_owned(), "prod".to_owned())]).expect("label is valid");
    query.filter.has_active_work = Some(true);
    query.include_total = true;
    query.projection = SessionProjection::WithLabels;

    let page = catalog.query(&query, None).expect("query succeeds");
    assert_eq!(page.total, Some(10));
    assert_eq!(page.items.len(), 10);
    assert!(page.items.iter().all(|summary| {
        summary.lifecycle() == LifecycleState::Running
            && summary
                .profile()
                .is_some_and(|profile| profile.as_str() == "agent")
            && summary.labels().and_then(|labels| labels.get("env")) == Some("prod")
            && summary.work().active_item_count == 1
    }));
}

#[test]
fn cursor_expires_after_catalog_change() {
    let catalog = PreparedSessionCatalog::new();
    for index in 0..10 {
        catalog
            .upsert(session(index).summary())
            .expect("upsert succeeds");
    }
    let query = query(5);
    let first = catalog.query(&query, None).expect("first page succeeds");
    let cursor = first.next_cursor.expect("more items remain");
    catalog
        .upsert(session(10).summary())
        .expect("catalog update succeeds");

    let error = catalog
        .query(&query, Some(&cursor))
        .expect_err("stale cursor must fail");
    assert_eq!(error.category(), ApplicationErrorCategory::CursorExpired);
}

#[test]
fn cursor_cannot_be_reused_with_another_query() {
    let catalog = PreparedSessionCatalog::new();
    for index in 0..10 {
        catalog
            .upsert(session(index).summary())
            .expect("upsert succeeds");
    }
    let first_query = query(5);
    let cursor = catalog
        .query(&first_query, None)
        .expect("first page succeeds")
        .next_cursor
        .expect("more items remain");
    let mut different_query = query(5);
    different_query.projection = SessionProjection::WithLabels;

    let error = catalog
        .query(&different_query, Some(&cursor))
        .expect_err("mismatched query must fail");
    assert_eq!(error.category(), ApplicationErrorCategory::CursorExpired);
}

#[test]
fn remove_cleans_secondary_indexes() {
    let catalog = PreparedSessionCatalog::new();
    let removed_id = SessionId::new("session-0000").expect("ID is valid");
    catalog
        .upsert(session(0).summary())
        .expect("upsert succeeds");
    catalog.remove(&removed_id).expect("remove succeeds");
    assert!(catalog.get(&removed_id).expect("get succeeds").is_none());

    let mut query = query(10);
    query.filter.has_active_work = Some(true);
    assert!(
        catalog
            .query(&query, None)
            .expect("query succeeds")
            .items
            .is_empty()
    );
}

#[test]
fn exact_total_is_omitted_unless_requested() {
    let catalog = PreparedSessionCatalog::new();
    catalog
        .upsert(session(1).summary())
        .expect("upsert succeeds");
    assert_eq!(
        catalog
            .query(&query(10), None)
            .expect("query succeeds")
            .total,
        None
    );
}

#[test]
fn descending_cursor_pages_seek_from_the_previous_sort_key() {
    let catalog = PreparedSessionCatalog::new();
    for index in 0..10 {
        catalog
            .upsert(session(index).summary())
            .expect("upsert succeeds");
    }
    let mut query = query(3);
    query.sort.direction = SortDirection::Descending;
    let first = catalog.query(&query, None).expect("first page succeeds");
    assert_eq!(
        first
            .items
            .iter()
            .map(|summary| summary.name().as_str())
            .collect::<Vec<_>>(),
        vec!["worker-0009", "worker-0008", "worker-0007"]
    );
    assert_eq!(first.query_cost.index_entries_examined, 4);
    let second = catalog
        .query(&query, first.next_cursor.as_ref())
        .expect("second page succeeds");
    assert_eq!(
        second
            .items
            .iter()
            .map(|summary| summary.name().as_str())
            .collect::<Vec<_>>(),
        vec!["worker-0006", "worker-0005", "worker-0004"]
    );
    assert_eq!(second.query_cost.index_entries_examined, 4);
}

#[test]
fn replacing_a_summary_removes_its_previous_sort_keys() {
    let catalog = PreparedSessionCatalog::new();
    let mut changed = session(1);
    catalog
        .upsert(changed.summary())
        .expect("initial upsert succeeds");
    changed
        .rename(
            SessionName::new("renamed-worker").expect("name is valid"),
            TimestampMillis::new(5),
        )
        .expect("rename succeeds");
    catalog
        .upsert(changed.summary())
        .expect("replacement succeeds");

    let page = catalog.query(&query(10), None).expect("query succeeds");
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].name().as_str(), "renamed-worker");
}

#[test]
fn concurrent_catalog_writers_preserve_all_summaries_and_revisions() {
    let catalog = Arc::new(PreparedSessionCatalog::new());
    let handles: Vec<_> = (0..4)
        .map(|worker| {
            let catalog = Arc::clone(&catalog);
            thread::spawn(move || {
                for offset in 0..75 {
                    let index = worker * 75 + offset;
                    catalog
                        .upsert(session(index).summary())
                        .expect("concurrent upsert succeeds");
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("catalog writer must not panic");
    }

    assert_eq!(
        catalog.status().expect("status succeeds"),
        (CatalogRevision::new(300), 300)
    );
}

#[test]
fn watch_suppresses_changes_outside_its_bounded_page() {
    let catalog = PreparedSessionCatalog::new();
    for index in 0..20 {
        catalog
            .upsert(session(index).summary())
            .expect("upsert succeeds");
    }
    let mut watch = catalog.open_watch(query(5)).expect("watch opens");
    let prior_revision = watch.snapshot_revision();
    catalog
        .upsert(session(100).summary())
        .expect("outside-page update succeeds");

    assert!(
        catalog
            .refresh_watch(&mut watch)
            .expect("watch refresh succeeds")
            .is_none()
    );
    assert!(watch.snapshot_revision() > prior_revision);
    assert_eq!(watch.items().len(), 5);
}

#[test]
fn watch_reports_page_membership_deltas_and_authoritative_order() {
    let catalog = PreparedSessionCatalog::new();
    for index in 0..20 {
        catalog
            .upsert(session(index).summary())
            .expect("upsert succeeds");
    }
    let mut watch = catalog.open_watch(query(5)).expect("watch opens");
    let mut entering = session(19);
    entering
        .rename(
            SessionName::new("aaa-first").expect("name is valid"),
            TimestampMillis::new(6),
        )
        .expect("rename succeeds");
    catalog
        .upsert(entering.summary())
        .expect("replacement succeeds");

    let batch = catalog
        .refresh_watch(&mut watch)
        .expect("watch refresh succeeds")
        .expect("page changed");
    assert_eq!(batch.deltas.len(), 2);
    assert!(batch.deltas.iter().any(|delta| {
        delta.kind() == SessionDeltaKind::Added && delta.session_id().as_str() == "session-0019"
    }));
    assert!(batch.deltas.iter().any(|delta| {
        delta.kind() == SessionDeltaKind::Removed
            && delta.session_id().as_str() == "session-0004"
            && delta.session_revision() == session(4).revision()
            && delta.summary().is_none()
    }));
    assert_eq!(batch.order[0].as_str(), "session-0019");
    assert_eq!(batch.order.len(), 5);
}

#[test]
fn watch_coalesces_multiple_updates_to_the_latest_summary() {
    let catalog = PreparedSessionCatalog::new();
    let mut changed = session(1);
    catalog
        .upsert(changed.summary())
        .expect("initial upsert succeeds");
    let mut watch = catalog.open_watch(query(5)).expect("watch opens");
    changed
        .update_foreground(foreground(ExecutionState::Active, 6))
        .expect("foreground update succeeds");
    catalog
        .upsert(changed.summary())
        .expect("busy update succeeds");
    changed
        .update_foreground(foreground(ExecutionState::Quiescent, 7))
        .expect("foreground update succeeds");
    catalog
        .upsert(changed.summary())
        .expect("idle update succeeds");

    let batch = catalog
        .refresh_watch(&mut watch)
        .expect("watch refresh succeeds")
        .expect("summary changed");
    assert_eq!(batch.deltas.len(), 1);
    let delta = &batch.deltas[0];
    assert_eq!(delta.kind(), SessionDeltaKind::Updated);
    assert_eq!(
        delta
            .summary()
            .expect("updated summary exists")
            .foreground()
            .execution()
            .state(),
        ExecutionState::Quiescent
    );
}

fn foreground(state: ExecutionState, committed_at: i64) -> ForegroundObservation {
    let committed_at = TimestampMillis::new(committed_at);
    ForegroundObservation::new(
        ExecutionObservation::new(
            state,
            ObservationBasis::new(
                Confidence::from_basis_points(8_000).expect("confidence is valid"),
                Some(EvidenceSource::new("test").expect("source is valid")),
                EvidenceDisposition::Inference,
                committed_at,
                Some(committed_at),
            )
            .expect("basis is valid"),
        )
        .expect("execution is valid"),
        InteractionObservation::none(committed_at),
        RuntimeConstraintObservation::clear(committed_at),
    )
}

#[test]
fn watched_exact_total_changes_without_sending_unrelated_summaries() {
    let catalog = PreparedSessionCatalog::new();
    for index in 0..10 {
        catalog
            .upsert(session(index).summary())
            .expect("upsert succeeds");
    }
    let mut watched_query = query(5);
    watched_query.include_total = true;
    let mut watch = catalog.open_watch(watched_query).expect("watch opens");
    catalog
        .upsert(session(100).summary())
        .expect("outside-page update succeeds");

    let batch = catalog
        .refresh_watch(&mut watch)
        .expect("watch refresh succeeds")
        .expect("total changed");
    assert!(batch.deltas.is_empty());
    assert_eq!(batch.total, Some(11));
}
