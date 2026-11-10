// SPDX-License-Identifier: Apache-2.0
//! Exact-object Windows reset. Retained handles replace pathname quarantine.
use super::*;
use undercurrent_platform_windows::{BlockingPipeStream, PinnedPath, PrivateRemovalFile};

pub(super) struct QuarantinedArtifact {
    pub(super) artifact: ResetArtifact,
    file: Option<PrivateRemovalFile>,
}

pub(super) fn ensure_session_host_inactive(path: &Path) -> Result<(), ResetError> {
    match BlockingPipeStream::connect(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(ResetError::busy(
            ResetStage::VerifyProcessHost,
            std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "session host remains active",
            ),
        )),
        Err(error) if error.raw_os_error() == Some(231) => {
            Err(ResetError::busy(ResetStage::VerifyProcessHost, error))
        }
        Err(error) => Err(ResetError::filesystem(ResetStage::VerifyProcessHost, error)),
    }
}

pub(super) fn quarantine_artifact(
    artifact: ResetArtifact,
) -> Result<Option<QuarantinedArtifact>, ResetError> {
    // Missing directories are absent artifacts, not an invitation to create them.
    let parent = artifact.path.parent().ok_or_else(|| {
        ResetError::unsafe_artifact(ResetStage::InspectArtifact, "artifact has no parent")
    })?;
    let parent_pin = match PinnedPath::owned_directory(parent) {
        Ok(parent) => parent,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(ResetError::filesystem(ResetStage::InspectArtifact, e)),
    };
    if let Some(anchor) = &artifact.anchored
        && (parent_pin
            .identity()
            .map_err(|e| ResetError::filesystem(ResetStage::InspectArtifact, e))?
            != anchor
                .parent
                .identity()
                .map_err(|e| ResetError::filesystem(ResetStage::InspectArtifact, e))?
            || artifact.path.file_name() != Some(anchor.name.as_os_str()))
    {
        return Err(ResetError::unsafe_artifact(
            ResetStage::InspectArtifact,
            "artifact parent identity changed",
        ));
    }
    let file = match PrivateRemovalFile::open(&artifact.path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(ResetError::filesystem(ResetStage::InspectArtifact, e)),
    };
    if artifact.policy == ResetArtifactPolicy::CredentialTemporary
        && file
            .file()
            .metadata()
            .map_err(|e| ResetError::filesystem(ResetStage::InspectArtifact, e))?
            .len()
            > LOOPBACK_TOKEN_BYTES
    {
        return Err(ResetError::unsafe_artifact(
            ResetStage::InspectArtifact,
            "stale credential temporary exceeds bound",
        ));
    }
    Ok(Some(QuarantinedArtifact {
        artifact,
        file: Some(file),
    }))
}

pub(super) fn restore_all(artifacts: &mut Vec<QuarantinedArtifact>) -> Result<(), ResetError> {
    // Preparation never renames or deletes. Releasing handles restores access.
    artifacts.clear();
    Ok(())
}

pub(super) fn remove_quarantined(
    artifact: &mut QuarantinedArtifact,
    _: fn(&File) -> std::io::Result<()>,
) -> Result<(), ResetError> {
    artifact
        .file
        .take()
        .expect("prepared file removed once")
        .remove()
        .map_err(|e| ResetError::filesystem(ResetStage::RemoveArtifact, e))
}

pub(super) fn sync_directory(_: &File) -> std::io::Result<()> {
    // Kept for the shared adapter's Unix durability-injection interface. Windows
    // removal uses exact kernel handles; it has no unprivileged directory fsync.
    // A power loss can leave stale artifacts, which startup rejects until reset.
    Ok(())
}

fn discover(
    path: &Path,
    group: ResetArtifactGroup,
    temporaries: bool,
) -> Result<Vec<ResetArtifact>, ResetError> {
    let directory = match PinnedPath::owned_directory(path) {
        Ok(held) => Arc::new(held),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(ResetError::filesystem(ResetStage::DiscoverArtifacts, e)),
    };
    let mut artifacts = Vec::new();
    for entry in
        fs::read_dir(path).map_err(|e| ResetError::filesystem(ResetStage::DiscoverArtifacts, e))?
    {
        let entry = entry.map_err(|e| ResetError::filesystem(ResetStage::DiscoverArtifacts, e))?;
        let name = entry.file_name();
        if temporaries && !is_loopback_temporary_name(&name) {
            continue;
        }
        artifacts.push(ResetArtifact {
            path: entry.path(),
            group,
            policy: if temporaries {
                ResetArtifactPolicy::CredentialTemporary
            } else {
                ResetArtifactPolicy::General
            },
            anchored: Some(AnchoredResetArtifact {
                parent: Arc::clone(&directory),
                name,
            }),
        });
    }
    Ok(artifacts)
}

pub(super) fn discover_stale_loopback_temporaries(
    path: &Path,
) -> Result<Vec<ResetArtifact>, ResetError> {
    discover(path, ResetArtifactGroup::Connection, true)
}
pub(super) fn discover_private_state_files(
    path: &Path,
    group: ResetArtifactGroup,
) -> Result<Vec<ResetArtifact>, ResetError> {
    discover(path, group, false)
}
fn is_loopback_temporary_name(name: &OsStr) -> bool {
    let Some(name) = name.to_str() else {
        return false;
    };
    name.as_bytes()
        .strip_prefix(LOOPBACK_TEMPORARY_PREFIX)
        .is_some_and(|suffix| {
            suffix.len() == LOOPBACK_TEMPORARY_SUFFIX_BYTES
                && suffix
                    .iter()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn port(root: &Path) -> NativeResetStatePort {
        NativeResetStatePort::from_paths(
            &root.join("run/daemon"),
            root,
            &root.join("state/data.sqlite3"),
            &root.join("logs/daemon.log"),
            2,
        )
        .unwrap()
    }
    #[test]
    fn reset_removes_exact_private_state_and_preserves_unrelated_files() {
        use std::io::Write;
        let root = tempfile::tempdir().unwrap();
        let port = port(root.path());
        drop(
            undercurrent_platform_windows::prepare_private_directory(&root.path().join("state"))
                .unwrap(),
        );
        undercurrent_platform_windows::create_private_file(&root.path().join("state/data.sqlite3"))
            .unwrap()
            .into_file()
            .write_all(b"database")
            .unwrap();
        fs::write(root.path().join("settings.toml"), b"preserved").unwrap();
        let journal = root
            .path()
            .join("state/session-host/session-host.attempt-receipts");
        drop(undercurrent_platform_windows::prepare_private_directory(&journal).unwrap());
        undercurrent_platform_windows::create_private_file(&journal.join("receipts.json"))
            .unwrap()
            .into_file()
            .write_all(b"receipt")
            .unwrap();
        let outcome = port.clear().unwrap();
        assert_eq!(outcome.removed.persistence_files, 2);
        assert!(!root.path().join("state/data.sqlite3").exists());
        assert!(!journal.join("receipts.json").exists());
        assert_eq!(
            fs::read(root.path().join("settings.toml")).unwrap(),
            b"preserved"
        );
        assert_eq!(port.clear().unwrap().removed.total(), 0);
    }
    #[test]
    fn unsafe_hardlink_prevents_any_reset_removal() {
        use std::io::Write;
        let root = tempfile::tempdir().unwrap();
        let port = port(root.path());
        drop(
            undercurrent_platform_windows::prepare_private_directory(&root.path().join("state"))
                .unwrap(),
        );
        let db = root.path().join("state/data.sqlite3");
        undercurrent_platform_windows::create_private_file(&db)
            .unwrap()
            .into_file()
            .write_all(b"database")
            .unwrap();
        let external = root.path().join("external");
        undercurrent_platform_windows::create_private_file(&external)
            .unwrap()
            .into_file()
            .write_all(b"external")
            .unwrap();
        fs::hard_link(&external, root.path().join("state/data.sqlite3-wal")).unwrap();
        assert!(port.clear().is_err());
        assert_eq!(fs::read(db).unwrap(), b"database");
        assert_eq!(fs::read(external).unwrap(), b"external");
    }
    #[test]
    fn live_daemon_or_host_blocks_reset() {
        let root = tempfile::tempdir().unwrap();
        let port = port(root.path());
        let guard = acquire_available_daemon_instance_lock(&port.instance_lock).unwrap();
        assert_eq!(
            port.clear().unwrap_err().stage(),
            ResetStage::AcquireInstanceLock
        );
        drop(guard);
        let _listener =
            undercurrent_platform_windows::BlockingPipeListener::bind(&port.session_host).unwrap();
        assert_eq!(
            port.clear().unwrap_err().stage(),
            ResetStage::VerifyProcessHost
        );
    }
}
