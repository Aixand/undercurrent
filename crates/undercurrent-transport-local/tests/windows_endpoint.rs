// SPDX-License-Identifier: Apache-2.0
#![cfg(windows)]
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use undercurrent_transport_local::{
    AsyncLocalListener, LocalEndpoint, LocalListener, connect_local, verify_local_peer,
};

#[tokio::test]
async fn owner_authenticates_both_ends_and_cannot_replace_live_listener() {
    let directory = tempfile::tempdir().unwrap();
    let endpoint = LocalEndpoint::new(directory.path().join("run/control.sock")).unwrap();
    let owner = LocalListener::bind(&endpoint).unwrap();
    assert!(LocalListener::bind(&endpoint).is_err());
    let listener = AsyncLocalListener::from_std(owner.take_listener().unwrap()).unwrap();
    assert!(owner.take_listener().is_err());
    for _ in 0..3 {
        let mut client = connect_local(&endpoint).await.unwrap();
        let (mut server, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
            .await
            .unwrap()
            .unwrap();
        verify_local_peer(&client).unwrap();
        verify_local_peer(&server).unwrap();
        client.write_all(b"\0binary\xff").await.unwrap();
        let mut received = [0; 8];
        tokio::time::timeout(Duration::from_secs(3), server.read_exact(&mut received))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&received, b"\0binary\xff");
        assert!(LocalListener::bind(&endpoint).is_err());
    }
    drop(listener);
    drop(owner);
    // Kernel pipe names disappear when their last handle closes: no stale file removal.
    // Tokio closes handles after outstanding overlapped completions are reaped.
    let replacement = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match LocalListener::bind(&endpoint) {
                Ok(listener) => break listener,
                Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => panic!("endpoint recovery failed: {error}"),
            }
        }
    })
    .await
    .unwrap();
    drop(replacement);
}

#[tokio::test]
async fn cancelled_accept_keeps_listener_usable() {
    let directory = tempfile::tempdir().unwrap();
    let endpoint = LocalEndpoint::new(directory.path().join("control.sock")).unwrap();
    let owner = LocalListener::bind(&endpoint).unwrap();
    let listener = AsyncLocalListener::from_std(owner.take_listener().unwrap()).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
    let _client = connect_local(&endpoint).await.unwrap();
    let (server, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .unwrap()
        .unwrap();
    verify_local_peer(&server).unwrap();
}

#[tokio::test]
async fn missing_endpoint_is_not_a_network_fallback() {
    let directory = tempfile::tempdir().unwrap();
    let endpoint = LocalEndpoint::new(directory.path().join("missing.sock")).unwrap();
    assert_eq!(
        connect_local(&endpoint).await.err().unwrap().kind(),
        std::io::ErrorKind::NotFound
    );
}
