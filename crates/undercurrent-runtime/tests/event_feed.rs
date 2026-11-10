// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};
use std::time::Duration;

use undercurrent_application::*;
use undercurrent_domain::*;
use undercurrent_runtime::*;

#[test]
fn selective_reads_use_session_index_without_examining_unrelated_records() {
    let journal = BoundedSessionEventJournal::new(300, 8).expect("journal limits are valid");
    for value in 1..=300 {
        let session = if value == 173 {
            "target".to_owned()
        } else {
            format!("session-{value:03}")
        };
        publish(
            &journal,
            record(value, &session, DomainEventKind::ForegroundChanged),
        );
    }
    let filter = filter(&["target"], &[DomainEventKind::ForegroundChanged]);
    let request = request(filter, SessionEventStart::Earliest, 10);

    let batch = block_on(journal.read(&request)).expect("selective read succeeds");

    assert_eq!(batch.records().len(), 1);
    assert_eq!(batch.records()[0].sequence, sequence(173));
    assert_eq!(batch.query_cost().index_entries_examined, 1);
    assert_eq!(batch.checkpoint(), Some(sequence(300)));
}

#[test]
fn earliest_resume_tail_and_live_have_explicit_checkpoint_semantics() {
    let journal = BoundedSessionEventJournal::new(8, 8).expect("journal limits are valid");
    for value in 1..=5 {
        publish(
            &journal,
            record(value, "one", DomainEventKind::LifecycleChanged),
        );
    }

    let earliest =
        block_on(journal.read(&request(filter(&[], &[]), SessionEventStart::Earliest, 2)))
            .expect("earliest read succeeds");
    assert_eq!(sequences(&earliest), vec![1, 2]);
    assert!(earliest.has_more());
    assert_eq!(earliest.checkpoint(), Some(sequence(2)));

    let resumed = block_on(journal.read(&request(
        filter(&[], &[]),
        SessionEventStart::Resume { after: sequence(2) },
        8,
    )))
    .expect("resume succeeds");
    assert_eq!(sequences(&resumed), vec![3, 4, 5]);
    assert!(!resumed.has_more());
    assert_eq!(resumed.checkpoint(), Some(sequence(5)));

    let tail = block_on(journal.read(&request(filter(&[], &[]), SessionEventStart::Tail, 2)))
        .expect("tail succeeds");
    assert_eq!(sequences(&tail), vec![4, 5]);
    assert_eq!(tail.checkpoint(), Some(sequence(5)));

    let live = block_on(journal.read(&request(filter(&[], &[]), SessionEventStart::Live, 8)))
        .expect("live checkpoint succeeds");
    assert!(live.records().is_empty());
    assert_eq!(live.checkpoint(), Some(sequence(5)));

    let future = block_on(journal.read(&request(
        filter(&[], &[]),
        SessionEventStart::Resume { after: sequence(6) },
        8,
    )))
    .expect_err("future checkpoint is rejected");
    assert_eq!(future.kind(), PortErrorKind::InvalidData);

    let empty = BoundedSessionEventJournal::new(8, 8).expect("journal limits are valid");
    let empty_resume = block_on(empty.read(&request(
        filter(&[], &[]),
        SessionEventStart::Resume {
            after: sequence(40),
        },
        8,
    )))
    .expect("empty journal preserves an opaque resume checkpoint");
    assert!(empty_resume.records().is_empty());
    assert_eq!(empty_resume.checkpoint(), Some(sequence(40)));
}

#[test]
fn event_subscription_port_coalesces_payload_free_wakes() {
    let journal = BoundedSessionEventJournal::new(8, 8).expect("journal limits are valid");
    let wake = Arc::new(CountWake::default());
    let request = request(filter(&["one"], &[]), SessionEventStart::Live, 8);
    let opened = SessionEventSubscriber::subscribe(&journal, &request, wake.clone())
        .expect("subscription port opens");
    let (initial, subscription) = opened.into_parts();
    assert!(initial.records().is_empty());

    publish(
        &journal,
        record(1, "one", DomainEventKind::LifecycleChanged),
    );
    publish(
        &journal,
        record(2, "one", DomainEventKind::ForegroundChanged),
    );
    assert_eq!(wake.calls.load(Ordering::Acquire), 1);

    let batch = subscription
        .try_receive()
        .expect("port read succeeds")
        .expect("events are pending");
    assert_eq!(sequences(&batch), vec![1, 2]);
    assert!(
        subscription
            .try_receive()
            .expect("clean port read succeeds")
            .is_none()
    );
    subscription.close();
    assert_eq!(journal.pressure().subscriptions, 0);
}

#[test]
fn subscription_drains_all_replay_pages_without_new_publications() {
    let journal = BoundedSessionEventJournal::new(8, 8).expect("journal limits are valid");
    for value in 1..=5 {
        publish(
            &journal,
            record(value, "one", DomainEventKind::LifecycleChanged),
        );
    }
    let open = journal
        .subscribe(&request(
            filter(&["one"], &[]),
            SessionEventStart::Earliest,
            2,
        ))
        .expect("subscription opens");
    let (initial, subscription) = open.into_parts();
    assert_eq!(sequences(&initial), vec![1, 2]);
    assert!(initial.has_more());

    let second = subscription
        .receive_timeout(Duration::from_millis(50))
        .expect("second retained page is immediately available");
    assert_eq!(sequences(&second), vec![3, 4]);
    assert!(second.has_more());

    let third = subscription
        .receive_timeout(Duration::from_millis(50))
        .expect("final retained page is immediately available");
    assert_eq!(sequences(&third), vec![5]);
    assert!(!third.has_more());
    assert!(matches!(
        subscription.receive_timeout(Duration::from_millis(1)),
        Err(SessionEventSubscriptionError::Timeout)
    ));
}

#[test]
fn resume_reports_exact_global_retention_gap() {
    let journal = BoundedSessionEventJournal::new(3, 8).expect("journal limits are valid");
    for value in 1..=6 {
        publish(
            &journal,
            record(value, "one", DomainEventKind::LifecycleChanged),
        );
    }

    let batch = block_on(journal.read(&request(
        filter(&["one"], &[]),
        SessionEventStart::Resume { after: sequence(1) },
        8,
    )))
    .expect("resume succeeds with gap");

    assert_eq!(sequences(&batch), vec![4, 5, 6]);
    let gap = batch.gap().expect("gap is explicit");
    assert_eq!(gap.missing_from(), sequence(2));
    assert_eq!(gap.missing_through(), sequence(3));
    assert_eq!(gap.first_available(), sequence(4));

    let contiguous = block_on(journal.read(&request(
        filter(&["one"], &[]),
        SessionEventStart::Resume { after: sequence(3) },
        8,
    )))
    .expect("direct predecessor resumes without gap");
    assert!(contiguous.gap().is_none());
}

#[test]
fn subscription_wakes_only_for_matching_interest() {
    let journal = BoundedSessionEventJournal::new(8, 8).expect("journal limits are valid");
    let open = journal
        .subscribe(&request(
            filter(&["one"], &[DomainEventKind::HealthChanged]),
            SessionEventStart::Live,
            8,
        ))
        .expect("subscription opens");
    let (initial, subscription) = open.into_parts();
    assert!(initial.records().is_empty());

    publish(&journal, record(1, "two", DomainEventKind::HealthChanged));
    assert!(matches!(
        subscription.receive_timeout(Duration::from_millis(1)),
        Err(SessionEventSubscriptionError::Timeout)
    ));

    publish(
        &journal,
        record(2, "one", DomainEventKind::ForegroundChanged),
    );
    assert!(matches!(
        subscription.receive_timeout(Duration::from_millis(1)),
        Err(SessionEventSubscriptionError::Timeout)
    ));

    publish(&journal, record(3, "one", DomainEventKind::HealthChanged));
    let batch = subscription
        .receive_timeout(Duration::from_millis(50))
        .expect("matching event wakes receiver");
    assert_eq!(sequences(&batch), vec![3]);
    assert_eq!(journal.pressure().dirty_subscriptions, 0);
}

#[test]
fn slow_subscription_retains_constant_state_and_recovers_with_gap() {
    let journal = BoundedSessionEventJournal::new(2, 8).expect("journal limits are valid");
    publish(
        &journal,
        record(1, "one", DomainEventKind::LifecycleChanged),
    );
    let open = journal
        .subscribe(&request(filter(&["one"], &[]), SessionEventStart::Live, 8))
        .expect("subscription opens");
    let (_, subscription) = open.into_parts();
    for value in 2..=4 {
        publish(
            &journal,
            record(value, "one", DomainEventKind::LifecycleChanged),
        );
    }

    let pressure = journal.pressure();
    assert_eq!(pressure.retained_records, 2);
    assert_eq!(pressure.dirty_subscriptions, 1);
    let batch = subscription
        .receive_timeout(Duration::from_millis(50))
        .expect("slow receiver reads retained head");
    assert_eq!(sequences(&batch), vec![3, 4]);
    let gap = batch.gap().expect("evicted event is explicit");
    assert_eq!(gap.missing_from(), sequence(2));
    assert_eq!(gap.missing_through(), sequence(2));
}

#[test]
fn three_hundred_subscriptions_are_selective_bounded_and_drop_cleanly() {
    let journal = BoundedSessionEventJournal::new(8, 300).expect("journal limits are valid");
    let mut subscriptions = Vec::new();
    for index in 0..300 {
        let name = format!("session-{index:03}");
        let open = journal
            .subscribe(&request(filter(&[&name], &[]), SessionEventStart::Live, 2))
            .expect("subscription opens within capacity");
        subscriptions.push(open.into_parts().1);
    }
    assert_eq!(journal.pressure().subscriptions, 300);
    let overflow = journal
        .subscribe(&request(
            filter(&["overflow"], &[]),
            SessionEventStart::Live,
            2,
        ))
        .err()
        .expect("subscription capacity is explicit");
    assert!(matches!(
        overflow,
        SessionEventSubscriptionError::CapacityExceeded { limit: 300 }
    ));

    publish(
        &journal,
        record(1, "session-173", DomainEventKind::ForegroundChanged),
    );
    let pressure = journal.pressure();
    assert_eq!(pressure.dirty_subscriptions, 1);
    let batch = subscriptions[173]
        .receive_timeout(Duration::from_millis(50))
        .expect("interested subscription wakes");
    assert_eq!(sequences(&batch), vec![1]);

    subscriptions.truncate(150);
    assert_eq!(journal.pressure().subscriptions, 150);
}

fn publish(journal: &BoundedSessionEventJournal, record: SessionOutboxRecord) {
    assert_eq!(
        block_on(journal.publish(&record)).expect("publication succeeds"),
        SessionEventPublishOutcome::Published
    );
}

fn request(
    filter: SessionEventFilter,
    start: SessionEventStart,
    limit: usize,
) -> SessionEventReadRequest {
    SessionEventReadRequest::new(filter, start, limit, 64).expect("request is valid")
}

fn filter(sessions: &[&str], kinds: &[DomainEventKind]) -> SessionEventFilter {
    SessionEventFilter::new(
        sessions.iter().map(|name| session_id(name)).collect(),
        kinds.iter().copied().collect::<BTreeSet<_>>(),
        300,
        DomainEventKind::ALL.len(),
    )
    .expect("filter is valid")
}

fn record(value: u64, session: &str, kind: DomainEventKind) -> SessionOutboxRecord {
    SessionOutboxRecord {
        snapshot: None,
        sequence: sequence(value),
        event: SessionOutboxEvent {
            kind,
            session_id: session_id(session),
            revision: Revision::new(value),
            occurred_at: TimestampMillis::new(i64::try_from(value).expect("test value fits")),
        },
    }
}

fn sequences(batch: &SessionEventReadBatch) -> Vec<u64> {
    batch
        .records()
        .iter()
        .map(|record| record.sequence.value())
        .collect()
}

fn session_id(value: &str) -> SessionId {
    SessionId::new(value).expect("session ID is valid")
}

fn sequence(value: u64) -> OutboxSequence {
    OutboxSequence::new(value).expect("sequence is nonzero")
}

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let parker = ThreadWake(thread::current());
    let waker = Waker::from(std::sync::Arc::new(parker));
    let mut context = Context::from_waker(&waker);
    loop {
        match Pin::as_mut(&mut future).poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}

struct ThreadWake(Thread);

impl Wake for ThreadWake {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.unpark();
    }
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
