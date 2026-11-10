// SPDX-License-Identifier: Apache-2.0
use super::{LocalDaemonInstanceState, instance_lock_name};
use std::{
    fs::TryLockError,
    io,
    path::{Path, PathBuf},
};
use undercurrent_platform_windows::{
    PinnedPath, open_private_lock_file, prepare_private_directory,
};

pub(crate) struct AvailableDaemonInstanceGuard {
    file: PinnedPath,
}
impl AvailableDaemonInstanceGuard {
    pub(crate) fn revalidate(&self) -> io::Result<()> {
        let named = open_private_lock_file(self.file.path(), false)?;
        if named.identity()? != self.file.identity()? {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "daemon lock identity changed",
            ));
        }
        Ok(())
    }
}
impl Drop for AvailableDaemonInstanceGuard {
    fn drop(&mut self) {
        let _ = self.file.file().unlock();
    }
}

fn scope(scope: &Path, create: bool) -> io::Result<Option<(PinnedPath, PathBuf)>> {
    if !scope.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "instance scope must be absolute",
        ));
    }
    let parent = scope.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "instance scope has no parent")
    })?;
    if !create && !parent.try_exists()? {
        return Ok(None);
    }
    let held = prepare_private_directory(parent)?;
    Ok(Some((held, parent.join(instance_lock_name(scope)?))))
}

pub(crate) fn local_daemon_instance_state(path: &Path) -> io::Result<LocalDaemonInstanceState> {
    let Some((_parent, path)) = scope(path, false)? else {
        return Ok(LocalDaemonInstanceState::Available);
    };
    let file = match open_private_lock_file(&path, false) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Ok(LocalDaemonInstanceState::Available);
        }
        Err(e) => return Err(e),
    };
    match file.file().try_lock() {
        Ok(()) => Ok(LocalDaemonInstanceState::Available),
        Err(TryLockError::WouldBlock) => Ok(LocalDaemonInstanceState::Owned),
        Err(TryLockError::Error(e)) => Err(e),
    }
}

pub(crate) fn acquire_available_daemon_instance_lock(
    path: &Path,
) -> io::Result<AvailableDaemonInstanceGuard> {
    let (_parent, path) = scope(path, true)?
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "instance parent missing"))?;
    let file = open_private_lock_file(&path, true)?;
    match file.file().try_lock() {
        Ok(()) => Ok(AvailableDaemonInstanceGuard { file }),
        Err(TryLockError::WouldBlock) => Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "daemon instance remains owned",
        )),
        Err(TryLockError::Error(e)) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_instance_prevents_replacement_until_released() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("run");
        let scope = parent.join("daemon");
        let guard = acquire_available_daemon_instance_lock(&scope).unwrap();
        let lock = parent.join(instance_lock_name(&scope).unwrap());
        assert!(std::fs::rename(&lock, parent.join("other.lock")).is_err());
        assert!(std::fs::rename(&parent, root.path().join("moved")).is_err());
        assert!(matches!(
            local_daemon_instance_state(&scope).unwrap(),
            LocalDaemonInstanceState::Owned
        ));
        guard.revalidate().unwrap();
        drop(guard);
        assert!(matches!(
            local_daemon_instance_state(&scope).unwrap(),
            LocalDaemonInstanceState::Available
        ));
        std::fs::rename(&lock, parent.join("other.lock")).unwrap();
    }
}
