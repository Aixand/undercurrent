// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use undercurrent_application::{OutputOffset, TerminalScreenSnapshot};
use undercurrent_daemon::{
    BoundedScreenCheckpointStore, ScreenCheckpointLimits, ScreenCheckpointStoreError,
};
use undercurrent_domain::SessionId;
use undercurrent_wire::ClientId;

#[test]
fn limits_reject_zero_fields() {
    for (limits, field) in [
        (ScreenCheckpointLimits::new(0, 1, 1), "clients"),
        (
            ScreenCheckpointLimits::new(1, 0, 1),
            "checkpoints_per_client",
        ),
        (ScreenCheckpointLimits::new(1, 1, 0), "bytes_per_client"),
    ] {
        assert_eq!(
            limits,
            Err(ScreenCheckpointStoreError::InvalidLimits { field })
        );
    }
}

#[test]
fn checkpoints_are_client_and_session_scoped_and_payload_redacted() {
    let store = store(2, 2, 32);
    let owner = client("owner");
    let peer = client("peer");
    let target = session("session-one");
    let token = store
        .register(owner.clone(), target.clone(), snapshot(1, b"private"))
        .unwrap();

    assert_eq!(token.len(), 17);
    assert!(token.starts_with('s'));
    let checkpoint = store.resolve(&owner, &token, &target).unwrap();
    assert_eq!(checkpoint.session_id(), &target);
    assert_eq!(checkpoint.snapshot().contents(), b"private");
    assert!(!format!("{checkpoint:?}").contains("private"));
    assert_eq!(
        store.resolve(&peer, &token, &target),
        Err(ScreenCheckpointStoreError::NotFound)
    );
    assert_eq!(
        store.resolve(&owner, &token, &session("session-two")),
        Err(ScreenCheckpointStoreError::SessionMismatch)
    );
    assert_eq!(
        store.resolve(&owner, "not-a-checkpoint", &target),
        Err(ScreenCheckpointStoreError::InvalidToken)
    );
}

#[test]
fn oldest_checkpoints_are_evicted_to_satisfy_count_and_byte_limits() {
    let store = store(1, 3, 10);
    let owner = client("owner");
    let session = session("session-one");
    let first = store
        .register(owner.clone(), session.clone(), snapshot(1, b"123456"))
        .unwrap();
    let second = store
        .register(owner.clone(), session.clone(), snapshot(2, b"7890"))
        .unwrap();
    let third = store
        .register(owner.clone(), session.clone(), snapshot(3, b"abcde"))
        .unwrap();

    assert_eq!(
        store.resolve(&owner, &first, &session),
        Err(ScreenCheckpointStoreError::NotFound)
    );
    assert!(store.resolve(&owner, &second, &session).is_ok());
    assert!(store.resolve(&owner, &third, &session).is_ok());
    let pressure = store.pressure();
    assert_eq!(pressure.clients, 1);
    assert_eq!(pressure.checkpoints, 2);
    assert_eq!(pressure.bytes, 9);
    assert_eq!(pressure.evictions, 1);

    let fourth = store
        .register(owner.clone(), session.clone(), snapshot(4, b"z"))
        .unwrap();
    let fifth = store
        .register(owner.clone(), session.clone(), snapshot(5, b"q"))
        .unwrap();
    assert_eq!(
        store.resolve(&owner, &second, &session),
        Err(ScreenCheckpointStoreError::NotFound)
    );
    assert!(store.resolve(&owner, &third, &session).is_ok());
    assert!(store.resolve(&owner, &fourth, &session).is_ok());
    assert!(store.resolve(&owner, &fifth, &session).is_ok());
    assert_eq!(store.pressure().checkpoints, 3);
    assert_eq!(store.pressure().evictions, 2);
}

#[test]
fn client_and_single_screen_capacity_are_hard_and_teardown_releases_bytes() {
    let store = store(1, 2, 4);
    let owner = client("owner");
    let session = session("session-one");
    store
        .register(owner.clone(), session.clone(), snapshot(1, b"1234"))
        .unwrap();
    assert_eq!(
        store.register(client("overflow"), session.clone(), snapshot(2, b"1")),
        Err(ScreenCheckpointStoreError::ClientCapacity { limit: 1 })
    );
    assert_eq!(
        store.register(owner.clone(), session, snapshot(2, b"12345")),
        Err(ScreenCheckpointStoreError::ScreenCapacity {
            observed: 5,
            limit: 4,
        })
    );

    store.close_client(&owner);
    let pressure = store.pressure();
    assert_eq!(pressure.clients, 0);
    assert_eq!(pressure.checkpoints, 0);
    assert_eq!(pressure.bytes, 0);
}

fn store(
    clients: usize,
    checkpoints_per_client: usize,
    bytes_per_client: usize,
) -> BoundedScreenCheckpointStore {
    BoundedScreenCheckpointStore::new(
        ScreenCheckpointLimits::new(clients, checkpoints_per_client, bytes_per_client).unwrap(),
    )
}

fn snapshot(offset: u64, contents: &[u8]) -> TerminalScreenSnapshot {
    TerminalScreenSnapshot::new(
        OutputOffset::new(offset),
        80,
        24,
        0,
        0,
        true,
        false,
        Arc::from(contents),
    )
    .unwrap()
}

fn client(value: &str) -> ClientId {
    ClientId::new(value).unwrap()
}

fn session(value: &str) -> SessionId {
    SessionId::new(value).unwrap()
}
