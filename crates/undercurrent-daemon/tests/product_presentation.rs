// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use undercurrent_application::PreparedSessionCatalog;
use undercurrent_daemon::ProductPresentation;
use undercurrent_domain::{
    CommandSpec, EnvironmentVariables, ExecutionLimits, ExecutionPolicy, HealthRules, Labels,
    LaunchSpec, LifecycleState, Metadata, PageSize, ProcessOutcome, RestartConfig, RestartPolicy,
    Session, SessionConfiguration, SessionFilter, SessionId, SessionName, SessionProjection,
    SessionQuery, SessionSort, StopPolicy, StopSignal, TerminalSize, TimestampMillis, WorkItem,
    WorkItemId, WorkItemKey, WorkOwner, WorkRelationKind, WorkStatus, WorkTarget,
};
use undercurrent_protocol::{NoRestartReasonDto, RestartStatusDto};

fn session() -> Session {
    session_with_restart(RestartConfig::never())
}

fn session_with_restart(restart: RestartConfig) -> Session {
    let limits = ExecutionLimits {
        max_arguments: 4,
        max_command_bytes: 256,
        max_environment_entries: 4,
        max_environment_bytes: 256,
        max_path_bytes: 128,
        max_health_rules: 4,
        max_health_rule_bytes: 128,
    };
    Session::create(
        SessionId::new("session-1").expect("session ID"),
        SessionName::new("build").expect("session name"),
        SessionConfiguration::new(
            LaunchSpec::new(
                CommandSpec::new("sh", Vec::new(), limits).expect("command"),
                EnvironmentVariables::default(),
                None,
                None,
                TerminalSize::new(80, 24, 400, 200).expect("terminal"),
            ),
            ExecutionPolicy::new(
                restart,
                StopPolicy::new(StopSignal::Terminate, std::time::Duration::from_secs(5))
                    .expect("stop policy"),
                true,
                false,
            ),
            HealthRules::default(),
            None,
            Labels::new([("team".to_owned(), "platform".to_owned())]).expect("labels"),
            Metadata::new([("private".to_owned(), "do-not-return".to_owned())]).expect("metadata"),
        ),
        16,
        TimestampMillis::new(1),
    )
    .expect("session")
    .0
}

#[test]
fn mutation_summary_is_compact_and_does_not_expose_labels_or_configuration() {
    let mut session = session();
    session
        .upsert_work_item(
            WorkItem::new(
                WorkItemKey::new(
                    WorkOwner::new("workflow").expect("work owner"),
                    WorkItemId::new("task-1").expect("work ID"),
                ),
                WorkRelationKind::DependsOn,
                "linked task",
                None,
                WorkStatus::Waiting,
                [WorkTarget::Session(
                    SessionId::new("session-2").expect("target session ID"),
                )],
                Metadata::default(),
                TimestampMillis::new(2),
                TimestampMillis::new(2),
                None,
            )
            .expect("work item"),
            TimestampMillis::new(2),
        )
        .expect("work update")
        .expect("work change");
    let dto = ProductPresentation::session_summary(&session.summary()).expect("summary DTO");

    assert_eq!(dto.id, "session-1");
    assert_eq!(dto.name, "build");
    assert!(dto.labels.is_none());
    assert_eq!(dto.work.total_items, 1);
    assert_eq!(dto.work.active_items, 1);
    assert_eq!(dto.work.total_targets, 1);
    assert_eq!(dto.work.active_targets, 1);
    assert_eq!(dto.lifecycle_revision, 1);
    assert_eq!(dto.revision, 2);
    assert_eq!(dto.health.configured_checks, 0);
    assert_eq!(dto.restart_status, RestartStatusDto::Idle);
    assert!(!format!("{dto:?}").contains("do-not-return"));
}

#[test]
fn mutation_summary_preserves_every_durable_restart_disposition() {
    let mut disabled = session();
    disabled
        .transition_lifecycle(LifecycleState::Stopped, TimestampMillis::new(2))
        .expect("terminal transition");
    disabled
        .observe_process_outcome(
            ProcessOutcome::Exited(1),
            Duration::ZERO,
            TimestampMillis::new(2),
        )
        .expect("restart decision");
    assert_eq!(
        ProductPresentation::session_summary(&disabled.summary())
            .expect("summary")
            .restart_status,
        RestartStatusDto::NotScheduled {
            reason: NoRestartReasonDto::PolicyDisabled,
        }
    );

    let restart = RestartConfig::new(
        RestartPolicy::Always,
        1,
        Duration::from_millis(250),
        10_000,
        Duration::from_millis(250),
        Duration::ZERO,
    )
    .expect("restart config");
    let mut scheduled = session_with_restart(restart);
    scheduled
        .transition_lifecycle(LifecycleState::Stopped, TimestampMillis::new(2))
        .expect("terminal transition");
    scheduled
        .observe_process_outcome(
            ProcessOutcome::Exited(1),
            Duration::ZERO,
            TimestampMillis::new(2),
        )
        .expect("restart decision");
    assert_eq!(
        ProductPresentation::session_summary(&scheduled.summary())
            .expect("summary")
            .restart_status,
        RestartStatusDto::Scheduled {
            attempt: 1,
            not_before_millis: 252,
        }
    );

    scheduled
        .transition_lifecycle(LifecycleState::Starting, TimestampMillis::new(3))
        .expect("restart transition");
    scheduled
        .transition_lifecycle(LifecycleState::Stopped, TimestampMillis::new(4))
        .expect("terminal transition");
    scheduled
        .observe_process_outcome(
            ProcessOutcome::Exited(1),
            Duration::ZERO,
            TimestampMillis::new(4),
        )
        .expect("restart exhaustion");
    assert_eq!(
        ProductPresentation::session_summary(&scheduled.summary())
            .expect("summary")
            .restart_status,
        RestartStatusDto::Exhausted { attempts: 1 }
    );
}

#[test]
fn prepared_page_preserves_only_explicit_label_projection_and_opaque_cursor() {
    let catalog = PreparedSessionCatalog::new();
    let session = session();
    catalog.upsert(session.summary()).expect("catalog upsert");
    let query = SessionQuery {
        filter: SessionFilter::default(),
        sort: SessionSort::default(),
        projection: SessionProjection::WithLabels,
        page_size: PageSize::new(1, 100).expect("page size"),
        include_total: true,
    };
    let page = catalog.query(&query, None).expect("query");
    let dto =
        ProductPresentation::session_page(&page, Some("opaque".to_owned())).expect("page DTO");

    assert_eq!(dto.total, Some(1));
    assert_eq!(dto.next_cursor.as_deref(), Some("opaque"));
    assert_eq!(
        dto.items[0]
            .labels
            .as_ref()
            .and_then(|labels| labels.get("team"))
            .map(String::as_str),
        Some("platform")
    );
}

#[test]
fn removal_observation_preserves_directory_and_restart_intent_from_its_snapshot() {
    use undercurrent_application::{
        OutboxSequence, SessionEventQueryCost, SessionEventReadBatch, SessionOutboxEvent,
        SessionOutboxRecord,
    };
    use undercurrent_domain::{DomainEventKind, PathReference};
    let mut removed = session();
    removed
        .transition_lifecycle(LifecycleState::Failed, TimestampMillis::new(2))
        .unwrap();
    removed
        .observe_process_outcome(
            ProcessOutcome::SupervisionFailed,
            Duration::ZERO,
            TimestampMillis::new(2),
        )
        .unwrap();
    let mut state = removed.state();
    state.restoration_directory =
        Some(PathReference::new("/projects/last directory", 4096).unwrap());
    let removed = Session::rehydrate(state).unwrap();
    let batch = SessionEventReadBatch::new(
        vec![SessionOutboxRecord {
            sequence: OutboxSequence::FIRST,
            event: SessionOutboxEvent {
                kind: DomainEventKind::SessionRemoved,
                session_id: removed.id().clone(),
                revision: removed.revision(),
                occurred_at: TimestampMillis::new(3),
            },
            snapshot: Some(removed),
        }],
        Some(OutboxSequence::FIRST),
        None,
        false,
        SessionEventQueryCost {
            index_entries_examined: 1,
        },
    );
    let page = ProductPresentation::session_events(&batch);
    let observation = page.items[0].observation.as_ref().unwrap();
    assert_eq!(
        observation.lifecycle,
        undercurrent_protocol::LifecycleStateDto::Failed
    );
    assert_eq!(
        observation.restoration_directory.as_deref(),
        Some("/projects/last directory")
    );
    assert_eq!(
        observation.restart_status,
        Some(RestartStatusDto::NotScheduled {
            reason: NoRestartReasonDto::PolicyDisabled
        })
    );
    let mut legacy = serde_json::to_value(observation).unwrap();
    legacy
        .as_object_mut()
        .unwrap()
        .remove("restoration_directory");
    legacy.as_object_mut().unwrap().remove("restart_status");
    let legacy: undercurrent_protocol::SessionEventObservationDto =
        serde_json::from_value(legacy).unwrap();
    assert!(legacy.restoration_directory.is_none());
    assert!(legacy.restart_status.is_none());
}
