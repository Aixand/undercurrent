// SPDX-License-Identifier: Apache-2.0

use undercurrent_application::SessionPageCursor;
use undercurrent_daemon::{
    BoundedProfileCursorStore, BoundedSessionCursorStore, CursorStoreError, CursorStoreLimits,
    ProfilePageCursor,
};
use undercurrent_domain::{
    CatalogRevision, PageSize, SessionFilter, SessionId, SessionProjection, SessionQuery,
    SessionSort,
};
use undercurrent_wire::ClientId;

fn client(value: &str) -> ClientId {
    ClientId::new(value).expect("client ID")
}

fn cursor(index: u64) -> SessionPageCursor {
    SessionPageCursor::rehydrate(
        CatalogRevision::new(index),
        SessionQuery {
            filter: SessionFilter::default(),
            sort: SessionSort::default(),
            projection: SessionProjection::Compact,
            page_size: PageSize::new(25, 100).expect("page size"),
            include_total: false,
        },
        SessionId::new(format!("session-{index}")).expect("session ID"),
    )
}

#[test]
fn cursor_limits_reject_zero_values() {
    assert!(matches!(
        CursorStoreLimits::new(0, 1),
        Err(CursorStoreError::InvalidLimits { field: "clients" })
    ));
    assert!(matches!(
        CursorStoreLimits::new(1, 0),
        Err(CursorStoreError::InvalidLimits {
            field: "cursors_per_client"
        })
    ));
}

#[test]
fn opaque_tokens_are_client_scoped_and_oldest_first_bounded() {
    let store = BoundedSessionCursorStore::new(CursorStoreLimits::new(2, 2).expect("limits"));
    let owner = client("owner");
    let peer = client("peer");
    let first = store
        .register(owner.clone(), cursor(1))
        .expect("first cursor");
    let second = store
        .register(owner.clone(), cursor(2))
        .expect("second cursor");

    assert_eq!(first.len(), 17);
    assert_eq!(
        store.resolve(&owner, &first).expect("owned cursor"),
        cursor(1)
    );
    assert_eq!(
        store.resolve(&peer, &first),
        Err(CursorStoreError::NotFound)
    );
    assert_eq!(
        store.resolve(&owner, "not-a-cursor"),
        Err(CursorStoreError::InvalidToken)
    );

    let third = store
        .register(owner.clone(), cursor(3))
        .expect("third cursor");
    assert_eq!(
        store.resolve(&owner, &first),
        Err(CursorStoreError::NotFound)
    );
    assert_eq!(
        store.resolve(&owner, &second).expect("second retained"),
        cursor(2)
    );
    assert_eq!(
        store.resolve(&owner, &third).expect("third retained"),
        cursor(3)
    );
    assert_eq!(store.pressure().evictions, 1);
    assert_eq!(store.pressure().cursors, 2);

    store.close_client(&owner);
    assert_eq!(store.pressure().clients, 0);
    assert_eq!(store.pressure().cursors, 0);
}

#[test]
fn three_hundred_clients_remain_bounded_and_the_next_is_rejected() {
    let store = BoundedSessionCursorStore::new(CursorStoreLimits::new(300, 1).expect("limits"));
    for index in 0..300 {
        store
            .register(client(&format!("client-{index}")), cursor(index))
            .expect("bounded client cursor");
    }

    let pressure = store.pressure();
    assert_eq!(pressure.clients, 300);
    assert_eq!(pressure.cursors, 300);
    assert_eq!(pressure.cursor_limit, 300);
    assert!(matches!(
        store.register(client("client-overflow"), cursor(301)),
        Err(CursorStoreError::ClientCapacity { limit: 300 })
    ));
}

#[test]
fn profile_cursors_share_ownership_eviction_and_teardown_semantics() {
    let store = BoundedProfileCursorStore::new(CursorStoreLimits::new(2, 1).expect("limits"));
    let owner = client("owner");
    let peer = client("peer");
    let first = store
        .register(owner.clone(), ProfilePageCursor::new("alpha"))
        .expect("profile cursor");
    assert_eq!(
        store.resolve(&owner, &first).expect("owner resolves"),
        ProfilePageCursor::new("alpha")
    );
    assert_eq!(
        store.resolve(&peer, &first),
        Err(CursorStoreError::NotFound)
    );

    let second = store
        .register(owner.clone(), ProfilePageCursor::new("beta"))
        .expect("replacement cursor");
    assert_eq!(
        store.resolve(&owner, &first),
        Err(CursorStoreError::NotFound)
    );
    assert_eq!(
        store
            .resolve(&owner, &second)
            .expect("replacement resolves"),
        ProfilePageCursor::new("beta")
    );
    assert_eq!(store.pressure().evictions, 1);

    store.close_client(&owner);
    assert_eq!(store.pressure().cursors, 0);
}
