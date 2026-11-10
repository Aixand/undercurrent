// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

use undercurrent_application::{
    OutboxSequence, PortErrorKind, SessionEventPublishOutcome, SessionEventPublisher,
    SessionOutboxEvent, SessionOutboxRecord,
};
use undercurrent_domain::{DomainEventKind, Revision, SessionId, TimestampMillis};
use undercurrent_runtime::BoundedSessionEventJournal;

#[test]
fn journal_is_strict_idempotent_bounded_and_payload_redacted() {
    let journal = BoundedSessionEventJournal::new(2, 4).expect("event journal");
    let first = record(1, DomainEventKind::SessionCreated);
    assert_eq!(
        block_on(journal.publish(&first)).expect("first publication"),
        SessionEventPublishOutcome::Published
    );
    assert_eq!(
        block_on(journal.publish(&first)).expect("exact retry"),
        SessionEventPublishOutcome::AlreadyPublished
    );

    let conflicting = record(1, DomainEventKind::LifecycleChanged);
    let conflict = block_on(journal.publish(&conflicting)).expect_err("sequence conflict");
    assert_eq!(conflict.kind(), PortErrorKind::Conflict);
    assert!(!format!("{conflict:?}").contains("different content"));

    let gap = block_on(journal.publish(&record(3, DomainEventKind::LifecycleChanged)))
        .expect_err("sequence gap");
    assert_eq!(gap.kind(), PortErrorKind::InvalidData);
    assert_eq!(
        block_on(journal.publish(&record(2, DomainEventKind::LifecycleChanged)))
            .expect("second publication"),
        SessionEventPublishOutcome::Published
    );
    assert_eq!(
        block_on(journal.publish(&record(3, DomainEventKind::ForegroundChanged)))
            .expect("third publication"),
        SessionEventPublishOutcome::Published
    );

    let pressure = journal.pressure();
    assert_eq!(pressure.retained_records, 2);
    assert_eq!(pressure.record_limit, 2);
    assert_eq!(pressure.oldest_retained, Some(sequence(2)));
    assert_eq!(pressure.latest_published, Some(sequence(3)));
    assert_eq!(pressure.evicted_records, 1);
    assert_eq!(
        journal
            .retained_records()
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![sequence(2), sequence(3)]
    );

    let evicted_retry = block_on(journal.publish(&first)).expect_err("evicted retry is explicit");
    assert_eq!(evicted_retry.kind(), PortErrorKind::ReplayUnavailable);
}

#[test]
fn journal_retention_remains_bounded_across_three_hundred_publications() {
    let journal = BoundedSessionEventJournal::new(37, 4).expect("event journal");
    for value in 1..=300 {
        assert_eq!(
            block_on(journal.publish(&record(value, DomainEventKind::ForegroundEvidenceChanged,)))
                .expect("contiguous publication"),
            SessionEventPublishOutcome::Published
        );
    }

    let pressure = journal.pressure();
    assert_eq!(pressure.retained_records, 37);
    assert_eq!(pressure.evicted_records, 263);
    assert_eq!(pressure.oldest_retained, Some(sequence(264)));
    assert_eq!(pressure.latest_published, Some(sequence(300)));
}

#[test]
fn a_new_journal_can_begin_at_the_oldest_pending_repository_sequence() {
    let journal = BoundedSessionEventJournal::new(4, 4).expect("event journal");
    let restored = record(900, DomainEventKind::SessionRenamed);

    assert_eq!(
        block_on(journal.publish(&restored)).expect("restored sequence publishes"),
        SessionEventPublishOutcome::Published
    );
    assert_eq!(journal.pressure().oldest_retained, Some(sequence(900)));
}

fn record(value: u64, kind: DomainEventKind) -> SessionOutboxRecord {
    SessionOutboxRecord {
        snapshot: None,
        sequence: sequence(value),
        event: SessionOutboxEvent {
            kind,
            session_id: SessionId::new("session-1").expect("session ID is valid"),
            revision: Revision::new(value),
            occurred_at: TimestampMillis::new(i64::try_from(value).unwrap_or(i64::MAX)),
        },
    }
}

fn sequence(value: u64) -> OutboxSequence {
    OutboxSequence::new(value).expect("outbox sequence is valid")
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

    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        self.0.unpark();
    }
}
