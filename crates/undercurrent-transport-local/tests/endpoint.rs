// SPDX-License-Identifier: Apache-2.0
#![cfg(any(target_os = "linux", target_os = "macos"))]
use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    time::Duration,
};
use undercurrent_transport_local::{LocalEndpoint, LocalListener, connect_local};

#[tokio::test]
async fn owner_connects_and_live_endpoint_cannot_be_replaced() {
    let home = tempfile::tempdir_in(fs::canonicalize(std::env::temp_dir()).unwrap()).unwrap();
    let endpoint = LocalEndpoint::new(home.path().join("run/control.sock")).unwrap();
    let owner = LocalListener::bind(&endpoint).unwrap();
    assert_eq!(fs::metadata(endpoint.path()).unwrap().mode() & 0o777, 0o600);
    assert_eq!(
        fs::metadata(endpoint.path().parent().unwrap())
            .unwrap()
            .mode()
            & 0o777,
        0o700
    );
    let listener = tokio::net::UnixListener::from_std(owner.take_listener().unwrap()).unwrap();
    let client = connect_local(&endpoint).await.unwrap();
    let (server, _) = listener.accept().await.unwrap();
    undercurrent_transport_local::verify_local_peer(&server).unwrap();
    assert!(LocalListener::bind(&endpoint).is_err());
    drop((client, server, listener, owner));
    // Owned stale socket is recovered only after exclusive endpoint ownership.
    let replacement = LocalListener::bind(&endpoint).unwrap();
    drop(replacement);
}

#[test]
fn permissions_symlinks_and_existing_files_fail_closed() {
    let home = tempfile::tempdir_in(fs::canonicalize(std::env::temp_dir()).unwrap()).unwrap();
    let directory = home.path().join("run");
    fs::create_dir(&directory).unwrap();
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o755)).unwrap();
    let endpoint = LocalEndpoint::new(directory.join("control.sock")).unwrap();
    assert!(LocalListener::bind(&endpoint).is_err());
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(endpoint.path(), b"foreign file").unwrap();
    assert!(LocalListener::bind(&endpoint).is_err());
    assert_eq!(fs::read(endpoint.path()).unwrap(), b"foreign file");
    fs::remove_file(endpoint.path()).unwrap();
    std::os::unix::fs::symlink(home.path().join("foreign"), endpoint.path()).unwrap();
    assert!(LocalListener::bind(&endpoint).is_err());
    assert!(
        fs::symlink_metadata(endpoint.path())
            .unwrap()
            .file_type()
            .is_symlink()
    );
}

#[tokio::test]
async fn changed_socket_mode_and_parent_symlink_are_rejected_by_client() {
    let home = tempfile::tempdir_in(fs::canonicalize(std::env::temp_dir()).unwrap()).unwrap();
    let endpoint = LocalEndpoint::new(home.path().join("run/control.sock")).unwrap();
    let _owner = LocalListener::bind(&endpoint).unwrap();
    fs::set_permissions(endpoint.path(), fs::Permissions::from_mode(0o666)).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), connect_local(&endpoint))
            .await
            .unwrap()
            .is_err()
    );
    let alias = home.path().join("alias");
    std::os::unix::fs::symlink(home.path().join("run"), &alias).unwrap();
    let aliased = LocalEndpoint::new(alias.join("control.sock")).unwrap();
    assert!(connect_local(&aliased).await.is_err());
}

#[test]
fn endpoint_rejects_relative_traversal_and_overlong_addresses() {
    for path in [
        "relative.sock".to_owned(),
        "/tmp/../x.sock".to_owned(),
        format!("/tmp/{}", "x".repeat(108)),
    ] {
        assert!(LocalEndpoint::new(path).is_err());
    }
}
