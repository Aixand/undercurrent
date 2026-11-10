// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::{Duration, Instant};

use undercurrent_transport_core::{
    BoundedRequestIdempotencyStore, RequestIdempotencyAbandonment, RequestIdempotencyCompletion,
    RequestIdempotencyError, RequestIdempotencyLimits, RequestIdempotencyReservation,
    RequestIdempotencyScope,
};
use undercurrent_wire::{ClientId, IdempotencyKey};

#[test]
fn exact_request_identity_replays_one_completed_response_without_payload_diagnostics() {
    let store = store(2, 64, 16, 16);
    let scope = scope(1, "start-1");
    let request = bytes("private-start-request");
    assert!(matches!(
        store.reserve(scope.clone(), request.clone()),
        Err(RequestIdempotencyError::InvalidRequestSize { .. })
    ));
    let request = bytes("start");
    assert_eq!(
        store
            .reserve(scope.clone(), request.clone())
            .expect("reservation"),
        RequestIdempotencyReservation::Reserved
    );
    assert_eq!(
        store
            .reserve(scope.clone(), request.clone())
            .expect("pending retry"),
        RequestIdempotencyReservation::Pending
    );
    assert_eq!(
        store
            .reserve(scope.clone(), bytes("stop"))
            .expect("payload conflict"),
        RequestIdempotencyReservation::PayloadConflict
    );
    assert_eq!(store.pressure().retained_bytes, 21);

    let response = bytes("secret-result");
    let retained_until = Instant::now() + Duration::from_secs(1);
    assert_eq!(
        store
            .complete(&scope, response.clone(), retained_until)
            .expect("completion"),
        RequestIdempotencyCompletion::Completed
    );
    assert_eq!(store.pressure().retained_bytes, 18);
    assert_eq!(
        store
            .complete(&scope, response.clone(), retained_until)
            .expect("repeat completion"),
        RequestIdempotencyCompletion::AlreadyCompleted
    );
    assert_eq!(
        store
            .complete(&scope, bytes("other-result"), retained_until)
            .expect("conflicting completion"),
        RequestIdempotencyCompletion::ResponseConflict
    );
    let replay = store
        .reserve(scope, request)
        .expect("completed retry is replayable");
    assert!(matches!(
        &replay,
        RequestIdempotencyReservation::Completed { response: replayed }
            if replayed == &response
    ));
    assert!(!format!("{replay:?}").contains("secret-result"));
}

#[test]
fn uref_034_product_error_disposition_replays_byte_exactly() {
    let store = store(1, 512, 64, 256);
    let scope = scope(34, "uref-034");
    let request = bytes("shutdown");
    let response: Arc<[u8]> = Arc::from(
        &br#"{"category":"failed_precondition","disposition":"unknown_inconclusive"}"#[..],
    );
    store
        .reserve(scope.clone(), request.clone())
        .expect("reservation");
    store
        .complete(
            &scope,
            response.clone(),
            Instant::now() + Duration::from_secs(1),
        )
        .expect("completion");

    assert!(matches!(
        store.reserve(scope, request).expect("replay"),
        RequestIdempotencyReservation::Completed { response: replayed }
            if replayed == response
    ));
}

#[test]
fn pending_ambiguity_never_expires_silently_and_requires_explicit_abandonment() {
    let store = store(2, 64, 16, 16);
    let pending = scope(1, "pending-1");
    store
        .reserve(pending.clone(), bytes("input"))
        .expect("pending reservation");

    assert_eq!(
        store.expire_before(Instant::now() + Duration::from_secs(60)),
        Default::default()
    );
    assert_eq!(store.pressure().pending_records, 1);
    assert_eq!(
        store.abandon(&pending),
        RequestIdempotencyAbandonment::Abandoned
    );
    assert_eq!(store.pressure().pending_records, 0);
    assert_eq!(
        store.abandon(&pending),
        RequestIdempotencyAbandonment::Missing
    );
}

#[test]
fn completed_records_expire_by_index_and_release_exact_bytes() {
    let store = store(2, 64, 16, 16);
    let scope = scope(1, "completed-1");
    store
        .reserve(scope.clone(), bytes("request"))
        .expect("reservation");
    let now = Instant::now();
    let deadline = now + Duration::from_millis(10);
    store
        .complete(&scope, bytes("result"), deadline)
        .expect("completion");

    assert_eq!(store.expire_before(now).records, 0);
    let release = store.expire_before(deadline);
    assert_eq!(release.records, 1);
    assert_eq!(release.bytes, 13);
    assert_eq!(store.pressure().completed_records, 0);
}

#[test]
fn logical_clients_are_isolated_and_retirement_removes_only_one_owner() {
    let store = store(2, 64, 16, 16);
    let first = scope(1, "same-key");
    let second = scope(2, "same-key");
    store
        .reserve(first.clone(), bytes("one"))
        .expect("first client");
    store
        .reserve(second.clone(), bytes("two"))
        .expect("second client");
    assert!(matches!(
        store.reserve(scope(3, "third"), bytes("three")),
        Err(RequestIdempotencyError::RecordCapacityExceeded { limit: 2 })
    ));

    let release = store.remove_client(first.client_id());
    assert_eq!(release.records, 1);
    assert_eq!(store.pressure().pending_records, 1);
    assert_eq!(
        store.reserve(second, bytes("two")).expect("second remains"),
        RequestIdempotencyReservation::Pending
    );
}

#[test]
fn pending_reservation_accounts_for_maximum_response_before_mutation_runs() {
    let store = store(4, 32, 16, 16);
    store
        .reserve(scope(1, "full"), Arc::from(vec![b'x'; 16]))
        .expect("full reservation");
    assert!(matches!(
        store.reserve(scope(2, "blocked"), bytes("x")),
        Err(RequestIdempotencyError::ByteCapacityExceeded {
            retained: 32,
            requested: 17,
            limit: 32,
        })
    ));
}

fn store(
    maximum_records: usize,
    maximum_total_bytes: u64,
    maximum_request_bytes: u32,
    maximum_response_bytes: u32,
) -> BoundedRequestIdempotencyStore {
    BoundedRequestIdempotencyStore::new(
        RequestIdempotencyLimits::new(
            maximum_records,
            maximum_total_bytes,
            maximum_request_bytes,
            maximum_response_bytes,
        )
        .expect("idempotency limits"),
    )
}

fn scope(client: usize, key: &str) -> RequestIdempotencyScope {
    RequestIdempotencyScope::new(
        ClientId::new(format!("client-{client}")).expect("client ID"),
        IdempotencyKey::new(key).expect("idempotency key"),
    )
}

fn bytes(value: &str) -> Arc<[u8]> {
    Arc::from(value.as_bytes())
}
