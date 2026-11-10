// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;

use undercurrent_application::{
    GetRuntimeAttemptStatusError, GetRuntimeAttemptStatusQuery, GetRuntimeAttemptStatusService,
    PortError, PortFuture, RuntimeAttemptSessionStatus, RuntimeAttemptStatusActor,
    RuntimeAttemptStatusAuthorization, RuntimeAttemptStatusAuthorizer, RuntimeAttemptStatusBinding,
    RuntimeAttemptStatusLookup, RuntimeAttemptStatusRepository, RuntimeAttemptStatusResult,
    RuntimeAttemptStatusSnapshot,
};
use undercurrent_domain::{
    DispatchCellIdentity, FencingToken, LifecycleState, OperationAttemptId, PayloadDigest,
    Revision, SessionId, TimestampMillis,
};

#[derive(Clone, Copy)]
struct FixedAuthorizer(RuntimeAttemptStatusAuthorization);

impl RuntimeAttemptStatusAuthorizer for FixedAuthorizer {
    fn authorize_status<'a>(
        &'a self,
        _actor: &'a RuntimeAttemptStatusActor,
        _attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<RuntimeAttemptStatusAuthorization, PortError>> {
        let decision = self.0;
        Box::pin(async move { Ok(decision) })
    }
}

struct RecordingRepository {
    calls: Arc<AtomicUsize>,
    digests: Arc<Mutex<Vec<PayloadDigest>>>,
    lookup: RuntimeAttemptStatusLookup,
}

impl RuntimeAttemptStatusRepository for RecordingRepository {
    fn get_runtime_attempt_status<'a>(
        &'a self,
        _attempt_id: &'a OperationAttemptId,
        request_digest: PayloadDigest,
    ) -> PortFuture<'a, Result<RuntimeAttemptStatusLookup, PortError>> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        self.digests
            .lock()
            .expect("digest recording lock")
            .push(request_digest);
        let lookup = self.lookup.clone();
        Box::pin(async move { Ok(lookup) })
    }
}

#[test]
fn authorization_denial_precedes_and_suppresses_existence_read() {
    let calls = Arc::new(AtomicUsize::new(0));
    let repository = RecordingRepository {
        calls: calls.clone(),
        digests: Arc::new(Mutex::new(Vec::new())),
        lookup: RuntimeAttemptStatusLookup::NotFound,
    };
    let authorizer = FixedAuthorizer(RuntimeAttemptStatusAuthorization::Denied);
    let error = block_on(
        GetRuntimeAttemptStatusService::new(&authorizer, &repository).execute(query(b"request")),
    )
    .expect_err("denied query fails without existence lookup");

    assert!(matches!(error, GetRuntimeAttemptStatusError::Denied));
    assert_eq!(calls.load(Ordering::Acquire), 0);
}

#[test]
fn exact_digest_reaches_one_read_and_missing_is_never_known_absent() {
    let calls = Arc::new(AtomicUsize::new(0));
    let digests = Arc::new(Mutex::new(Vec::new()));
    let repository = RecordingRepository {
        calls: calls.clone(),
        digests: digests.clone(),
        lookup: RuntimeAttemptStatusLookup::NotFound,
    };
    let authorizer = FixedAuthorizer(RuntimeAttemptStatusAuthorization::Authorized);
    let expected = PayloadDigest::of_bytes(b"request");
    let error = block_on(
        GetRuntimeAttemptStatusService::new(&authorizer, &repository).execute(query(b"request")),
    )
    .expect_err("missing attempt is typed failure");

    assert!(matches!(error, GetRuntimeAttemptStatusError::NotFound));
    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert_eq!(*digests.lock().expect("digest lock"), vec![expected]);
}

#[test]
fn service_preserves_every_strict_store_state_without_side_effect_ports() {
    let cases = [
        RuntimeAttemptStatusSnapshot::Unknown {
            observed_at: TimestampMillis::new(10),
        },
        RuntimeAttemptStatusSnapshot::KnownAbsent {
            observed_at: TimestampMillis::new(20),
        },
        RuntimeAttemptStatusSnapshot::KnownApplied {
            result_digest: PayloadDigest::of_bytes(b"start result"),
            observed_at: TimestampMillis::new(30),
            session: RuntimeAttemptSessionStatus::new(
                SessionId::new("session-1").expect("session ID"),
                LifecycleState::Running,
                Revision::new(5),
                Revision::new(4),
                TimestampMillis::new(31),
            ),
        },
        RuntimeAttemptStatusSnapshot::Compensated {
            observed_at: TimestampMillis::new(40),
        },
    ];
    let authorizer = FixedAuthorizer(RuntimeAttemptStatusAuthorization::Authorized);

    for snapshot in cases {
        let calls = Arc::new(AtomicUsize::new(0));
        let expected = result(snapshot);
        let repository = RecordingRepository {
            calls: calls.clone(),
            digests: Arc::new(Mutex::new(Vec::new())),
            lookup: RuntimeAttemptStatusLookup::Found(Box::new(expected.clone())),
        };
        let actual = block_on(
            GetRuntimeAttemptStatusService::new(&authorizer, &repository)
                .execute(query(b"request")),
        )
        .expect("authorized status result");
        assert_eq!(actual, expected);
        assert_eq!(calls.load(Ordering::Acquire), 1);
    }
}

fn query(digest: &[u8]) -> GetRuntimeAttemptStatusQuery {
    GetRuntimeAttemptStatusQuery::new(
        OperationAttemptId::new("attempt-1").expect("attempt ID"),
        PayloadDigest::of_bytes(digest),
        RuntimeAttemptStatusActor::new(
            "principal-1".to_owned(),
            Some("tenant-1".to_owned()),
            "authority-1".to_owned(),
        )
        .expect("actor"),
    )
}

fn result(snapshot: RuntimeAttemptStatusSnapshot) -> RuntimeAttemptStatusResult {
    RuntimeAttemptStatusResult::new(
        OperationAttemptId::new("attempt-1").expect("attempt ID"),
        FencingToken::new(7).expect("fence"),
        RuntimeAttemptStatusBinding::new(
            DispatchCellIdentity::new("cell:1").expect("cell ID"),
            PayloadDigest::of_bytes(b"request"),
            PayloadDigest::of_bytes(b"profile"),
            PayloadDigest::of_bytes(b"environment"),
            PayloadDigest::of_bytes(b"input"),
        ),
        snapshot,
    )
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct ThreadWake(thread::Thread);
    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match Future::poll(future.as_mut(), &mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}
