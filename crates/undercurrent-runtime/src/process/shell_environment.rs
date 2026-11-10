// SPDX-License-Identifier: Apache-2.0

//! Private, bounded root-shell environment capture. Values never enter PTY output.

use std::collections::BTreeSet;
#[cfg(target_os = "linux")]
use std::fs;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::{ffi::OsStringExt, net::UnixStream};
use std::path::PathBuf;
use std::time::Duration;
#[cfg(windows)]
use undercurrent_platform_windows::BlockingPipeStream as UnixStream;

use super::{HostShared, ProcessHostBootstrap, failure, parse_session, read_frame};
use crate::SystemClock;
use crate::process_host_protocol::{
    HostCodec, HostCommand, HostDecodedFrame, HostFailure, HostFailureContext, HostFailureKind,
    HostRequest, HostResolvedSecretEnvironment, HostResponse, HostResponseValue,
};
use undercurrent_application::Clock;

pub(super) struct ShellEnvironmentSnapshot {
    pub(super) values: HostResolvedSecretEnvironment,
    pub(super) observed_at_millis: i64,
    #[cfg(windows)]
    pub(super) working_directory: Option<String>,
}

/// Internal shell-hook helper. Sends inherited exports directly to the session host.
/// Errors are deliberately payload-free; callers must not print captured values.
#[doc(hidden)]
pub fn capture_shell_environment() -> std::io::Result<()> {
    capture_shell_environment_with(std::env::vars_os, std::env::current_dir)
}

// Keep acquisition separate from the authenticated exchange so native fixtures
// can inject otherwise unconstructible input failures without bypassing IPC.
fn capture_shell_environment_with<I>(
    environment: impl FnOnce() -> I,
    current_directory: impl FnOnce() -> std::io::Result<PathBuf>,
) -> std::io::Result<()>
where
    I: IntoIterator<Item = (std::ffi::OsString, std::ffi::OsString)>,
{
    let unavailable = || std::io::Error::other("shell environment capture unavailable");
    let socket = std::env::var_os("UC_SHELL_HOST").ok_or_else(unavailable)?;
    let shell_pid = std::env::var("UC_CAPTURE_SHELL_PID")
        .ok()
        .and_then(|pid| pid.parse::<u32>().ok())
        .ok_or_else(unavailable)?;
    let session_id = std::env::var("UC_SESSION").map_err(|_| unavailable())?;
    let max_bytes = std::env::var("UC_SHELL_ENV_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v <= 16 * 1024 * 1024)
        .ok_or_else(unavailable)?;
    let max_entries = std::env::var("UC_SHELL_ENV_ENTRIES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v <= 65536)
        .ok_or_else(unavailable)?;
    let values = collect_capture(
        environment().into_iter().map(|(name, value)| {
            let name = name.into_string().map_err(|_| ())?;
            if excluded_capture_name(&name) {
                return Ok((name, Vec::new()));
            }
            #[cfg(unix)]
            let value = value.into_vec();
            #[cfg(windows)]
            let value = value.into_string().map_err(|_| ())?.into_bytes();
            Ok((name, value))
        }),
        max_entries,
        max_bytes,
    );
    #[cfg(windows)]
    let mut values = values;
    #[cfg(windows)]
    let working_directory = match current_directory()
        .ok()
        .and_then(|path| path.into_os_string().into_string().ok())
    {
        Some(path) => Some(path),
        None => {
            invalidate_capture(&mut values);
            None
        }
    };
    #[cfg(not(windows))]
    let working_directory = None;
    #[cfg(not(windows))]
    let _ = current_directory;
    // Conversion failure still sends an authenticated invalidation marker. No
    // partial capture is published with a fresh timestamp.
    let socket = PathBuf::from(socket);
    let mut token = String::new();
    crate::process::host_files::read(&ProcessHostBootstrap::authentication_path(&socket))
        .map_err(|_| unavailable())?
        .take(1025)
        .read_to_string(&mut token)
        .map_err(|_| unavailable())?;
    if token.len() > 1024 {
        return Err(unavailable());
    }
    let codec = HostCodec::new(max_bytes.saturating_mul(6).saturating_add(65536))
        .map_err(|_| unavailable())?;
    let request = HostRequest {
        request_id: 0,
        command: HostCommand::CaptureShellEnvironment {
            token,
            session_id,
            shell_pid,
            environment: HostResolvedSecretEnvironment::from_values(values),
            working_directory,
        },
    };
    let mut stream = UnixStream::connect(socket).map_err(|_| unavailable())?;
    stream.set_read_timeout(Some(Duration::from_millis(500)))?;
    stream.set_write_timeout(Some(Duration::from_millis(500)))?;
    let mut encoded = codec.encode_request(&request).map_err(|_| unavailable())?;
    let written = stream.write_all(&encoded);
    encoded.fill(0);
    written.map_err(|_| unavailable())?;
    match read_frame(&mut stream, codec).map_err(|_| unavailable())? {
        HostDecodedFrame::Response(HostResponse {
            result: Ok(HostResponseValue::Completed),
            ..
        }) => Ok(()),
        HostDecodedFrame::Response(HostResponse {
            result: Ok(HostResponseValue::ShellEnvironmentInvalidated),
            ..
        }) => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "shell environment capture failed; previous snapshot invalidated",
        )),
        _ => Err(unavailable()),
    }
}

impl HostShared {
    pub(super) fn capture_environment(
        &self,
        session: String,
        shell_pid: u32,
        values: HostResolvedSecretEnvironment,
        working_directory: Option<String>,
    ) -> Result<HostResponseValue, HostFailure> {
        let session = parse_session(session, HostFailureContext::Execute)?;
        let key = self
            .metadata
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .find(|((id, _), metadata)| {
                id == &session
                    && metadata.root_pid == shell_pid
                    && process_identity(shell_pid)
                        .is_some_and(|(_, ticks)| Some(ticks) == metadata.root_start_ticks)
            })
            .map(|(key, _)| key.clone())
            .ok_or_else(|| failure(HostFailureKind::Unavailable, HostFailureContext::Execute))?;
        let mut snapshots = self
            .shell_environments
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        // Exit handling removes snapshots under this same lock; reject late captures.
        if self
            .exits
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains_key(&key)
            || self
                .runtime
                .hosted_process_identity(&key.0, &key.1)
                .is_err()
        {
            return Err(failure(
                HostFailureKind::Unavailable,
                HostFailureContext::Execute,
            ));
        }
        #[cfg(not(windows))]
        let _ = working_directory;
        store_capture(
            &mut snapshots,
            key,
            ShellEnvironmentSnapshot {
                values,
                observed_at_millis: SystemClock.now().value(),
                #[cfg(windows)]
                working_directory,
            },
            self.limits.environment_entries,
            self.limits.environment_bytes,
            self.limits.max_processes,
        )
    }
}

fn store_capture(
    snapshots: &mut std::collections::BTreeMap<
        (
            undercurrent_domain::SessionId,
            undercurrent_domain::ProcessInstanceId,
        ),
        ShellEnvironmentSnapshot,
    >,
    key: (
        undercurrent_domain::SessionId,
        undercurrent_domain::ProcessInstanceId,
    ),
    snapshot: ShellEnvironmentSnapshot,
    maximum_entries: usize,
    maximum_bytes: usize,
    maximum_snapshots: usize,
) -> Result<HostResponseValue, HostFailure> {
    let mut bytes = 0usize;
    #[cfg(windows)]
    if snapshot.working_directory.as_ref().is_some_and(|path| {
        path.len() > 4096 || path.contains('\0') || !std::path::Path::new(path).is_absolute()
    }) {
        snapshots.remove(&key);
        return Ok(HostResponseValue::ShellEnvironmentInvalidated);
    }
    let mut names = BTreeSet::new();
    for (name, value) in snapshot.values.values() {
        if name.is_empty()
            || name.contains(['=', '\0'])
            || value.contains(&0)
            || !names.insert(name)
        {
            snapshots.remove(&key);
            return Ok(HostResponseValue::ShellEnvironmentInvalidated);
        }
        bytes = bytes.saturating_add(name.len()).saturating_add(value.len());
    }
    if snapshot.values.values().len() > maximum_entries || bytes > maximum_bytes {
        snapshots.remove(&key);
        return Ok(HostResponseValue::ShellEnvironmentInvalidated);
    }
    if !snapshots.contains_key(&key) && snapshots.len() >= maximum_snapshots {
        return Err(failure(
            HostFailureKind::CapacityExceeded,
            HostFailureContext::Execute,
        ));
    }
    snapshots.insert(key, snapshot);
    Ok(HostResponseValue::Completed)
}

fn invalidate_capture(values: &mut Vec<(String, Vec<u8>)>) {
    for (_, value) in values.iter_mut() {
        value.fill(0);
    }
    *values = vec![(String::new(), Vec::new())];
}

fn excluded_capture_name(name: &str) -> bool {
    if name.starts_with("UC_SHELL_") || name == "UC_CAPTURE_SHELL_PID" || name == "_" {
        return true;
    }
    // cmd.exe inherits per-drive working-directory bookkeeping such as =C:.
    // It is not an exported variable and cannot enter the product environment.
    // Ignore only that exact Windows form; other invalid names still invalidate
    // the snapshot through the normal host validation.
    #[cfg(windows)]
    if matches!(name.as_bytes(), [b'=', drive, b':'] if drive.is_ascii_alphabetic()) {
        return true;
    }
    false
}

fn collect_capture(
    entries: impl IntoIterator<Item = Result<(String, Vec<u8>), ()>>,
    maximum_entries: usize,
    maximum_bytes: usize,
) -> Vec<(String, Vec<u8>)> {
    let mut values = Vec::new();
    let mut bytes = 0usize;
    for entry in entries {
        let Ok((name, value)) = entry else {
            invalidate_capture(&mut values);
            break;
        };
        if excluded_capture_name(&name) {
            continue;
        }
        let total = bytes
            .checked_add(name.len())
            .and_then(|n| n.checked_add(value.len()));
        if values.len() >= maximum_entries || total.is_none_or(|n| n > maximum_bytes) {
            invalidate_capture(&mut values);
            break;
        }
        bytes = total.unwrap_or(maximum_bytes);
        values.push((name, value));
    }
    values
}

// Authenticate the actual helper process as a descendant of the managed root.
// This also permits Bash PS0 command-substitution subprocesses.
#[cfg(target_os = "linux")]
pub(super) fn valid_capture_peer(stream: &UnixStream, shell_pid: u32) -> bool {
    use nix::sys::socket::{getsockopt, sockopt::PeerCredentials};
    let Ok(peer) = getsockopt(stream, PeerCredentials) else {
        return false;
    };
    if peer.uid() != nix::unistd::geteuid().as_raw() {
        return false;
    }
    let Ok(mut pid) = u32::try_from(peer.pid()) else {
        return false;
    };
    for _ in 0..8 {
        let Some((parent, _)) = process_identity(pid) else {
            return false;
        };
        if parent == shell_pid {
            return true;
        }
        if parent == 0 || parent == pid {
            return false;
        }
        pid = parent;
    }
    false
}

#[cfg(target_os = "macos")]
pub(super) fn valid_capture_peer(stream: &UnixStream, shell_pid: u32) -> bool {
    use nix::sys::socket::{
        getsockopt,
        sockopt::{LocalPeerCred, LocalPeerPid},
    };
    use undercurrent_platform_macos::identity;
    let Ok(peer) = getsockopt(stream, LocalPeerCred) else {
        return false;
    };
    if peer.uid() != nix::unistd::geteuid().as_raw() {
        return false;
    }
    let Ok(raw_pid) = getsockopt(stream, LocalPeerPid) else {
        return false;
    };
    let Ok(mut pid) = u32::try_from(raw_pid) else {
        return false;
    };
    let Ok(root) = identity(shell_pid) else {
        return false;
    };
    let mut chain = Vec::new();
    for _ in 0..8 {
        let Ok(info) = identity(pid) else {
            return false;
        };
        if info.uid != peer.uid() || chain.contains(&info) {
            return false;
        }
        chain.push(info);
        if info.parent == shell_pid {
            return identity(shell_pid).ok() == Some(root)
                && chain
                    .iter()
                    .all(|info| identity(info.pid).ok() == Some(*info));
        }
        if info.parent <= 1 || info.parent == pid {
            return false;
        }
        pid = info.parent;
    }
    false
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub(super) fn valid_capture_peer(_: &UnixStream, _: u32) -> bool {
    false
}

#[cfg(windows)]
pub(super) fn valid_capture_peer(stream: &UnixStream, shell_pid: u32) -> bool {
    use undercurrent_platform_windows::ProcessHandle;
    let verified = || -> std::io::Result<bool> {
        let root = ProcessHandle::open(shell_pid)?;
        let peer = ProcessHandle::open(stream.peer_process_id()?)?;
        let mut chain = vec![peer];
        for _ in 0..8 {
            let current = chain.last().expect("peer starts the chain");
            if !current.is_alive()? || !current.is_current_user()? {
                return Ok(false);
            }
            let Some(parent) = current.parent()? else {
                return Ok(false);
            };
            if parent.identity() == root.identity() {
                return Ok(root.is_alive()?
                    && root.is_current_user()?
                    && chain.iter().all(|p| p.is_alive().unwrap_or(false)));
            }
            if chain
                .iter()
                .any(|p| p.identity().pid == parent.identity().pid)
            {
                return Ok(false);
            }
            chain.push(parent);
        }
        Ok(false)
    };
    verified().unwrap_or(false)
}

#[cfg(windows)]
fn process_identity(pid: u32) -> Option<(u32, u64)> {
    let process = undercurrent_platform_windows::ProcessHandle::open(pid).ok()?;
    let parent = process.parent().ok()??;
    Some((parent.identity().pid, process.identity().created))
}

#[cfg(target_os = "linux")]
fn process_identity(pid: u32) -> Option<(u32, u64)> {
    let file = fs::File::open(format!("/proc/{pid}/stat")).ok()?;
    let mut bytes = Vec::new();
    file.take(4096).read_to_end(&mut bytes).ok()?;
    let stat = std::str::from_utf8(&bytes).ok()?;
    let fields: Vec<_> = stat.rsplit_once(')')?.1.split_whitespace().collect();
    Some((fields.get(1)?.parse().ok()?, fields.get(19)?.parse().ok()?))
}

#[cfg(target_os = "macos")]
fn process_identity(pid: u32) -> Option<(u32, u64)> {
    undercurrent_platform_macos::identity(pid)
        .ok()
        .map(|info| (info.parent, info.start_micros))
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn process_identity(_: u32) -> Option<(u32, u64)> {
    None
}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::*;
    #[test]
    fn shell_capture_checks_kernel_peer_ancestry() {
        use undercurrent_platform_windows::{BlockingPipeListener, ProcessHandle};
        let root = tempfile::tempdir().unwrap();
        let socket = root.path().join("capture");
        let listener = BlockingPipeListener::bind(&socket).unwrap();
        let _client = UnixStream::connect(&socket).unwrap();
        let (server, _) = listener.accept().unwrap();
        let peer = ProcessHandle::open(std::process::id()).unwrap();
        let parent = peer.parent().unwrap().unwrap();
        assert!(valid_capture_peer(&server, parent.identity().pid));
        assert!(!valid_capture_peer(&server, peer.identity().pid));
        assert!(!valid_capture_peer(&server, 0));
        assert_eq!(
            process_identity(peer.identity().pid),
            Some((parent.identity().pid, peer.identity().created))
        );
    }
}

#[cfg(test)]
mod capture_tests {
    use super::*;
    use undercurrent_domain::{ProcessInstanceId, SessionId};

    fn snapshot(values: Vec<(String, Vec<u8>)>, at: i64) -> ShellEnvironmentSnapshot {
        ShellEnvironmentSnapshot {
            values: HostResolvedSecretEnvironment::from_values(values),
            observed_at_millis: at,
            #[cfg(windows)]
            working_directory: Some(r"C:\work".to_owned()),
        }
    }

    #[cfg(windows)]
    #[test]
    fn drive_directory_metadata_does_not_consume_export_limits_or_allow_invalid_names() {
        let exported = vec![("A".to_owned(), b"v".to_vec())];
        let values = collect_capture(
            [
                Ok(("=C:".into(), vec![b'x'; 200])),
                Ok(exported[0].clone()),
                Ok(("=z:".into(), vec![b'y'; 200])),
            ],
            1,
            2,
        );
        assert_eq!(values, exported);
        let key = (
            SessionId::new("session").unwrap(),
            ProcessInstanceId::new("process").unwrap(),
        );
        for name in ["=", "=1:", "=C:extra", "=é:", "BAD=NAME"] {
            let mut retained = std::collections::BTreeMap::new();
            store_capture(
                &mut retained,
                key.clone(),
                snapshot(exported.clone(), 10),
                2,
                100,
                4,
            )
            .unwrap();
            let invalid = collect_capture([Ok((name.into(), b"invalid".to_vec()))], 2, 100);
            assert!(matches!(
                store_capture(&mut retained, key.clone(), snapshot(invalid, 20), 2, 100, 4)
                    .unwrap(),
                HostResponseValue::ShellEnvironmentInvalidated
            ));
            assert!(!retained.contains_key(&key));
        }
    }

    #[test]
    fn conversion_and_capacity_failures_clear_only_the_authenticated_process_snapshot() {
        let key = (
            SessionId::new("session").unwrap(),
            ProcessInstanceId::new("process").unwrap(),
        );
        let peer = (
            SessionId::new("session").unwrap(),
            ProcessInstanceId::new("replacement").unwrap(),
        );
        let good = || vec![("MESSAGE".to_owned(), b"private".to_vec())];
        for entries in [
            vec![Ok(("MESSAGE".into(), b"partial".to_vec())), Err(())],
            vec![Ok(("MESSAGE".into(), vec![0; 101]))],
            vec![
                Ok(("A".into(), vec![])),
                Ok(("B".into(), vec![])),
                Ok(("C".into(), vec![])),
            ],
        ] {
            let mut retained = std::collections::BTreeMap::new();
            store_capture(&mut retained, key.clone(), snapshot(good(), 10), 2, 100, 4).unwrap();
            store_capture(&mut retained, peer.clone(), snapshot(good(), 11), 2, 100, 4).unwrap();
            let failed = collect_capture(entries, 2, 100);
            assert!(matches!(
                store_capture(&mut retained, key.clone(), snapshot(failed, 20), 2, 100, 4).unwrap(),
                HostResponseValue::ShellEnvironmentInvalidated
            ));
            assert!(!retained.contains_key(&key));
            assert_eq!(retained[&peer].observed_at_millis, 11);
            assert_eq!(retained[&peer].values.values(), good());
        }
        // Directory conversion failure uses the identical invalidation marker.
        let mut values = good();
        invalidate_capture(&mut values);
        assert_eq!(values, vec![(String::new(), Vec::new())]);
    }
}

#[cfg(all(test, windows))]
#[path = "windows_capture_tests.rs"]
mod native_windows_tests;
