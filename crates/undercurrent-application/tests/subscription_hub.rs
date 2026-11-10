// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use undercurrent_application::{
    EffectiveProfileDirectory, PreparedReadModels, SessionDeltaKind, SessionProjectionWriter,
    SessionSubscriptionError, SessionSubscriptionHub, SubscriptionWake,
};
use undercurrent_domain::{
    CommandSpec, EffectiveProfileOrigin, EffectiveProfileSelection, EffectiveProfileTransition,
    EnvironmentVariables, ExecutionLimits, ExecutionPolicy, HealthRules, Labels, LaunchSpec,
    LifecycleState, Metadata, PageSize, ProcessIdentity, ProcessInstanceId, ProfileBindingMode,
    ProfileName, RestartConfig, Revision, Session, SessionConfiguration, SessionFilter, SessionId,
    SessionName, SessionProjection, SessionQuery, SessionSort, SortDirection, StopPolicy,
    StopSignal, TerminalSize, TimestampMillis, UnclassifiedProfileReason,
};

#[test]
fn finite_list_and_watch_share_the_same_filter_sort_projection_and_total_model() {
    let hub = SessionSubscriptionHub::new(PreparedReadModels::new(), 2).expect("subscription hub");
    let sessions = (0..6).map(session).collect::<Vec<_>>();
    for session in &sessions {
        hub.upsert_session(session).expect("session projection");
    }
    let mut query = unfiltered_query(2);
    query.filter.ids = BTreeSet::from([
        sessions[1].id().clone(),
        sessions[3].id().clone(),
        sessions[5].id().clone(),
        SessionId::new("removed-session").unwrap(),
    ]);
    query.sort.direction = SortDirection::Descending;
    query.projection = SessionProjection::WithLabels;
    query.include_total = true;

    let listed = hub
        .catalog()
        .query(&query, None)
        .expect("finite list succeeds");
    let watched = hub
        .subscribe(query.clone())
        .expect("watch accepts the same query")
        .snapshot()
        .expect("initial watch snapshot");

    assert_eq!(watched.items, listed.items);
    assert_eq!(watched.revision, listed.snapshot_revision);
    assert_eq!(watched.total, listed.total);
    assert_eq!(watched.query_cost, listed.query_cost);
    assert_eq!(watched.total, Some(3));
    assert!(
        watched
            .items
            .iter()
            .all(|summary| summary.projection() == SessionProjection::WithLabels)
    );
}

#[test]
fn hub_marks_only_interested_watches_and_coalesces_to_latest_revision() {
    let hub = SessionSubscriptionHub::new(PreparedReadModels::new(), 4).expect("subscription hub");
    let mut sessions = (0..300).map(session).collect::<Vec<_>>();
    for session in &sessions {
        hub.upsert_session(session).expect("session projection");
    }
    let first = hub
        .subscribe(query_for(&sessions[0], 1))
        .expect("first subscription");
    let last = hub
        .subscribe(query_for(&sessions[299], 1))
        .expect("last subscription");
    let initial = last.snapshot().expect("initial snapshot");

    sessions[299]
        .rename(
            SessionName::new("renamed-once").expect("session name"),
            TimestampMillis::new(2),
        )
        .expect("first rename");
    hub.upsert_session(&sessions[299])
        .expect("first projection");
    sessions[299]
        .rename(
            SessionName::new("renamed-twice").expect("session name"),
            TimestampMillis::new(3),
        )
        .expect("second rename");
    hub.upsert_session(&sessions[299])
        .expect("second projection");

    let pressure = hub.pressure();
    assert_eq!(pressure.subscriptions, 2);
    assert_eq!(pressure.dirty_subscriptions, 1);
    assert!(first.try_recv().expect("unrelated receive").is_none());
    let batch = last
        .try_recv()
        .expect("changed receive")
        .expect("coalesced batch");
    assert_eq!(batch.from_revision, initial.revision);
    assert!(batch.to_revision > batch.from_revision);
    assert_eq!(batch.deltas.len(), 1);
    let delta = &batch.deltas[0];
    assert_eq!(delta.kind(), SessionDeltaKind::Updated);
    assert_eq!(delta.session_revision(), Revision::new(3));
    assert_eq!(
        delta.summary().expect("updated summary").name().as_str(),
        "renamed-twice"
    );
    assert_eq!(hub.pressure().dirty_subscriptions, 0);
}

#[test]
fn page_membership_removal_retains_the_last_known_session_revision() {
    let hub = SessionSubscriptionHub::new(PreparedReadModels::new(), 2).expect("subscription hub");
    let first = session(0);
    let mut entering = session(1);
    hub.upsert_session(&first).expect("first projection");
    hub.upsert_session(&entering).expect("second projection");
    let subscription = hub
        .subscribe(unfiltered_query(1))
        .expect("query subscription");

    entering
        .rename(
            SessionName::new("aaa-first").expect("session name"),
            TimestampMillis::new(2),
        )
        .expect("rename");
    hub.upsert_session(&entering).expect("changed projection");
    let batch = subscription
        .recv_timeout(Duration::from_secs(1))
        .expect("membership batch");

    assert!(batch.deltas.iter().any(|delta| {
        delta.kind() == SessionDeltaKind::Removed
            && delta.session_id() == first.id()
            && delta.session_revision() == first.revision()
    }));
    assert!(batch.deltas.iter().any(|delta| {
        delta.kind() == SessionDeltaKind::Added
            && delta.session_id() == entering.id()
            && delta.session_revision() == entering.revision()
    }));
}

#[test]
fn blocking_watch_wakes_on_projection_change_without_polling() {
    let hub = Arc::new(
        SessionSubscriptionHub::new(PreparedReadModels::new(), 1).expect("subscription hub"),
    );
    let mut session = session(0);
    hub.upsert_session(&session).expect("initial projection");
    let subscription = hub
        .subscribe(query_for(&session, 1))
        .expect("query subscription");
    let waiting = thread::spawn(move || subscription.recv_timeout(Duration::from_secs(1)));

    session
        .rename(
            SessionName::new("wake-watch").expect("session name"),
            TimestampMillis::new(2),
        )
        .expect("rename");
    hub.upsert_session(&session).expect("changed projection");

    let batch = waiting.join().expect("watch thread").expect("watch wakes");
    assert_eq!(batch.deltas.len(), 1);
    assert_eq!(batch.deltas[0].session_revision(), session.revision());
}

#[test]
fn outer_wake_is_payload_free_and_coalesced_until_receive() {
    let hub = SessionSubscriptionHub::new(PreparedReadModels::new(), 1).expect("subscription hub");
    let mut session = session(0);
    hub.upsert_session(&session).expect("initial projection");
    let wake = Arc::new(CountWake::default());
    let subscription = hub
        .subscribe_with_wake(query_for(&session, 1), wake.clone())
        .expect("query subscription");

    for (name, time) in [("wake-once", 2), ("still-one", 3)] {
        session
            .rename(
                SessionName::new(name).expect("session name"),
                TimestampMillis::new(time),
            )
            .expect("rename");
        hub.upsert_session(&session).expect("changed projection");
    }

    assert_eq!(wake.calls.load(Ordering::Acquire), 1);
    assert!(subscription.try_recv().expect("receive succeeds").is_some());
    session
        .rename(
            SessionName::new("wake-again").expect("session name"),
            TimestampMillis::new(4),
        )
        .expect("rename");
    hub.upsert_session(&session).expect("changed projection");
    assert_eq!(wake.calls.load(Ordering::Acquire), 2);
}

#[test]
fn subscription_capacity_timeout_and_drop_cleanup_are_explicit() {
    let hub = SessionSubscriptionHub::new(PreparedReadModels::new(), 1).expect("subscription hub");
    let first = hub
        .subscribe(unfiltered_query(1))
        .expect("first subscription");
    assert!(matches!(
        hub.subscribe(unfiltered_query(1)),
        Err(SessionSubscriptionError::CapacityExceeded { limit: 1 })
    ));
    assert!(matches!(
        first.recv_timeout(Duration::from_millis(1)),
        Err(SessionSubscriptionError::Timeout)
    ));
    drop(first);
    assert_eq!(hub.pressure().subscriptions, 0);

    let replacement = hub
        .subscribe(unfiltered_query(1))
        .expect("replacement subscription");
    replacement.close();
    assert!(matches!(
        replacement.try_recv(),
        Err(SessionSubscriptionError::Closed)
    ));
}

#[test]
fn adaptive_profile_transition_reindexes_and_survives_only_matching_process_upserts() {
    let hub = SessionSubscriptionHub::new(PreparedReadModels::new(), 2).expect("subscription hub");
    let mut session = adaptive_running_session();
    let process = session.process().expect("owned process").id().clone();
    let durable_revision = session.revision();
    hub.upsert_session(&session).expect("initial projection");

    let initial = hub
        .get(session.id(), &process)
        .expect("profile lookup")
        .expect("exact process profile state");
    assert_eq!(initial.epoch().value(), 0);
    assert_eq!(
        initial.selection().unclassified_reason(),
        Some(UnclassifiedProfileReason::AwaitingObservation)
    );

    let mut codex_query = unfiltered_query(1);
    codex_query.filter.profile = Some(ProfileName::new("codex").expect("profile"));
    let watch = hub.subscribe(codex_query).expect("profile watch");
    assert!(watch.snapshot().expect("initial snapshot").items.is_empty());
    let selected = hub
        .transition(
            session.id(),
            EffectiveProfileTransition::new(
                process.clone(),
                EffectiveProfileSelection::Profile(ProfileName::new("codex").expect("profile")),
                EffectiveProfileOrigin::ForegroundProcess,
                TimestampMillis::new(3),
            ),
        )
        .expect("profile transition")
        .expect("profile snapshot");
    assert_eq!(selected.epoch().value(), 1);
    assert_eq!(session.revision(), durable_revision);
    let update = watch
        .try_recv()
        .expect("watch receive")
        .expect("profile change wakes watch");
    assert_eq!(
        update.deltas[0]
            .summary()
            .expect("added summary")
            .profile()
            .map(ProfileName::as_str),
        Some("codex")
    );

    session
        .rename(
            SessionName::new("adaptive-renamed").expect("name"),
            TimestampMillis::new(4),
        )
        .expect("rename");
    hub.upsert_session(&session)
        .expect("matching process upsert");
    assert_eq!(
        hub.get(session.id(), &process)
            .expect("profile lookup")
            .expect("profile state")
            .selection()
            .profile()
            .map(ProfileName::as_str),
        Some("codex")
    );

    let replacement = ProcessInstanceId::new("process-replacement").expect("process ID");
    let mut state = session.state();
    state.process = Some(ProcessIdentity::new(
        replacement.clone(),
        TimestampMillis::new(2),
    ));
    session = Session::rehydrate(state).expect("replacement state");
    hub.upsert_session(&session)
        .expect("replacement projection");
    assert!(
        hub.get(session.id(), &process)
            .expect("stale lookup")
            .is_none()
    );
    assert_eq!(
        hub.get(session.id(), &replacement)
            .expect("replacement lookup")
            .expect("replacement state")
            .selection()
            .unclassified_reason(),
        Some(UnclassifiedProfileReason::AwaitingObservation)
    );
}

#[derive(Default)]
struct CountWake {
    calls: AtomicUsize,
}

impl SubscriptionWake for CountWake {
    fn wake(&self) {
        self.calls.fetch_add(1, Ordering::AcqRel);
    }
}

fn session(index: usize) -> Session {
    Session::create(
        SessionId::new(format!("session-{index:03}")).expect("session ID"),
        SessionName::new(format!("worker-{index:03}")).expect("session name"),
        configuration(),
        8,
        TimestampMillis::new(1),
    )
    .expect("session")
    .0
}

fn configuration() -> SessionConfiguration {
    let limits = execution_limits();
    SessionConfiguration::new(
        LaunchSpec::new(
            CommandSpec::new("sh", vec!["-lc".to_owned(), "sleep 30".to_owned()], limits)
                .expect("command"),
            EnvironmentVariables::default(),
            None,
            None,
            TerminalSize::new(80, 24, 500, 200).expect("terminal size"),
        ),
        ExecutionPolicy::new(
            RestartConfig::never(),
            StopPolicy::new(StopSignal::Terminate, Duration::from_secs(5)).expect("stop policy"),
            true,
            false,
        ),
        HealthRules::new(Vec::new(), limits).expect("health rules"),
        None,
        Labels::default(),
        Metadata::default(),
    )
}

fn adaptive_running_session() -> Session {
    let mut configuration = configuration();
    configuration.select_profile_if_absent(ProfileName::new("zsh").expect("profile"));
    configuration.set_profile_binding(ProfileBindingMode::ForegroundAdaptive);
    let mut session = Session::create(
        SessionId::new("adaptive-session").expect("session ID"),
        SessionName::new("adaptive").expect("session name"),
        configuration,
        8,
        TimestampMillis::new(1),
    )
    .expect("session")
    .0;
    session
        .record_process_ownership(
            ProcessIdentity::new(
                ProcessInstanceId::new("adaptive-process").expect("process ID"),
                TimestampMillis::new(2),
            ),
            TimestampMillis::new(2),
        )
        .expect("process ownership");
    session
        .transition_lifecycle(LifecycleState::Running, TimestampMillis::new(2))
        .expect("running lifecycle");
    session
}

fn execution_limits() -> ExecutionLimits {
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

fn query_for(session: &Session, page_size: usize) -> SessionQuery {
    let mut query = unfiltered_query(page_size);
    query.filter.ids = BTreeSet::from([session.id().clone()]);
    query
}

fn unfiltered_query(page_size: usize) -> SessionQuery {
    SessionQuery {
        filter: SessionFilter::default(),
        sort: SessionSort::default(),
        projection: SessionProjection::Compact,
        page_size: PageSize::new(page_size, 100).expect("page size"),
        include_total: false,
    }
}
