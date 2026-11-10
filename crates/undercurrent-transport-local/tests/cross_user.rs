// SPDX-License-Identifier: Apache-2.0
#![cfg(target_os = "linux")]
//! Run ignored checks inside `unshare --map-root-user --map-auto` (no host root needed).
use std::{
    fs,
    os::unix::{fs::PermissionsExt, process::CommandExt},
    process::Command,
};
use undercurrent_transport_local::{
    LocalEndpoint, LocalListener, connect_local, verify_local_peer,
};

#[test]
#[ignore = "subprocess helper for isolated cross-user qualification"]
fn child_identity_probe() {
    let mode = std::env::var("UC_IDENTITY_TEST_MODE").expect("parent-provided mode");
    assert_eq!(nix::unistd::geteuid().as_raw(), 1);
    let path = std::env::var_os("UC_IDENTITY_TEST_ENDPOINT").unwrap();
    if mode == "fake_server" {
        use std::io::{Read, Write};
        let _listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        println!("fake-server-ready");
        std::io::stdout().flush().unwrap();
        let _ = std::io::stdin().read(&mut [0_u8]);
        return;
    }
    let connected = std::os::unix::net::UnixStream::connect(path);
    if mode == "denied" {
        assert_eq!(
            connected.unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied
        );
    } else {
        assert!(connected.is_ok());
    }
}

#[tokio::test]
#[ignore = "requires isolated namespace with mapped UIDs 0 and 1"]
async fn another_user_is_denied_and_kernel_credentials_cannot_be_spoofed() {
    assert_eq!(
        nix::unistd::geteuid().as_raw(),
        0,
        "run in mapped user namespace"
    );
    let home = tempfile::tempdir().unwrap();
    fs::set_permissions(home.path(), fs::Permissions::from_mode(0o755)).unwrap();
    let endpoint = LocalEndpoint::new(home.path().join("run/control.sock")).unwrap();
    let owner = LocalListener::bind(&endpoint).unwrap();
    let listener = tokio::net::UnixListener::from_std(owner.take_listener().unwrap()).unwrap();
    let child = |mode| {
        Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", "child_identity_probe"])
            .env("UC_IDENTITY_TEST_MODE", mode)
            .env("UC_IDENTITY_TEST_ENDPOINT", endpoint.path())
            .uid(1)
            .gid(1)
            .output()
            .unwrap()
    };
    let denied = child("denied");
    assert!(denied.status.success(), "{:?}", denied);
    // Deliberately bypass filesystem protection to test the independent kernel check.
    fs::set_permissions(
        endpoint.path().parent().unwrap(),
        fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    fs::set_permissions(endpoint.path(), fs::Permissions::from_mode(0o666)).unwrap();
    let connected = child("peer");
    assert!(connected.status.success(), "{:?}", connected);
    let (peer, _) = listener.accept().await.unwrap();
    assert_eq!(peer.peer_cred().unwrap().uid(), 1);
    assert_eq!(
        verify_local_peer(&peer).unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    // A wrong-owner endpoint is also rejected on the client side, before negotiation.
    fs::set_permissions(
        endpoint.path().parent().unwrap(),
        fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    fs::set_permissions(endpoint.path(), fs::Permissions::from_mode(0o600)).unwrap();
    nix::unistd::chown(endpoint.path(), Some(nix::unistd::Uid::from_raw(1)), None).unwrap();
    assert_eq!(
        connect_local(&endpoint).await.unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied
    );
}

#[tokio::test]
#[ignore = "requires isolated namespace with mapped UIDs 0 and 1"]
async fn client_rejects_a_foreign_server_even_with_trusted_socket_metadata() {
    use std::io::{BufRead, BufReader};
    use std::process::Stdio;
    assert_eq!(nix::unistd::geteuid().as_raw(), 0);
    let home = tempfile::tempdir().unwrap();
    fs::set_permissions(home.path(), fs::Permissions::from_mode(0o755)).unwrap();
    let parent = home.path().join("fake");
    fs::create_dir(&parent).unwrap();
    fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
    nix::unistd::chown(&parent, Some(nix::unistd::Uid::from_raw(1)), None).unwrap();
    let endpoint = LocalEndpoint::new(parent.join("control.sock")).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "child_identity_probe",
            "--nocapture",
        ])
        .env("UC_IDENTITY_TEST_MODE", "fake_server")
        .env("UC_IDENTITY_TEST_ENDPOINT", endpoint.path())
        .uid(1)
        .gid(1)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut reader = BufReader::new(child.stdout.take().unwrap());
    loop {
        let mut line = String::new();
        assert!(
            reader.read_line(&mut line).unwrap() > 0,
            "fake server startup failed"
        );
        if line.contains("fake-server-ready") {
            break;
        }
    }
    // Simulate trusted metadata around a listener still owned by a foreign process.
    for path in [&parent, endpoint.path()] {
        nix::unistd::chown(path, Some(nix::unistd::Uid::from_raw(0)), None).unwrap();
    }
    let result = connect_local(&endpoint).await;
    drop(child.stdin.take());
    assert!(child.wait().unwrap().success());
    assert_eq!(
        result.unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied
    );
}
