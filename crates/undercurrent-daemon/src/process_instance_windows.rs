// SPDX-License-Identifier: Apache-2.0
//! Private, handle-pinned Windows daemon instance ownership.
use std::fs::TryLockError;
use std::io;
use std::path::{Path, PathBuf};
use undercurrent_platform_windows::{PinnedPath, open_private_lock_file};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProcessInstanceState {
    Available,
    Owned,
}

#[derive(Debug)]
pub(crate) struct PreparedProcessInstanceScope {
    _parent: PinnedPath,
    lock_path: PathBuf,
}

#[derive(Debug)]
pub(crate) struct ProcessInstanceLock {
    file: PinnedPath,
}

impl Drop for ProcessInstanceLock {
    fn drop(&mut self) {
        let _ = self.file.file().unlock();
    }
}

/// Creates a private local runtime directory, rejecting reparse points and
/// retaining its ancestors throughout ownership verification and ACL setup.
pub fn prepare_private_directory(path: &Path) -> io::Result<()> {
    undercurrent_platform_windows::prepare_private_directory(path).map(drop)
}

pub fn prepare_instance_scope_parent(scope: &Path) -> io::Result<()> {
    prepare_process_instance_scope(scope).map(drop)
}

pub(crate) fn prepare_process_instance_scope(
    scope: &Path,
) -> io::Result<PreparedProcessInstanceScope> {
    open_scope(scope, true)?
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "instance parent missing"))
}

fn open_scope(scope: &Path, create: bool) -> io::Result<Option<PreparedProcessInstanceScope>> {
    if !scope.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "instance scope must be absolute",
        ));
    }
    let parent = scope.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "instance scope has no parent")
    })?;
    let name = scope
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "instance scope has no name"))?;
    if !create && !parent.try_exists()? {
        return Ok(None);
    }
    let held = undercurrent_platform_windows::prepare_private_directory(parent)?;
    let mut lock_name = std::ffi::OsString::from(".");
    lock_name.push(name);
    lock_name.push(".instance.lock");
    Ok(Some(PreparedProcessInstanceScope {
        _parent: held,
        lock_path: parent.join(lock_name),
    }))
}

pub(crate) fn acquire_prepared_process_instance_lock(
    scope: PreparedProcessInstanceScope,
) -> io::Result<ProcessInstanceLock> {
    let file = open_private_lock_file(&scope.lock_path, true)?;
    match file.file().try_lock() {
        Ok(()) => Ok(ProcessInstanceLock { file }),
        Err(TryLockError::WouldBlock) => Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            "another process owns the daemon instance",
        )),
        Err(TryLockError::Error(error)) => Err(error),
    }
}

pub(crate) fn process_instance_state(scope: &Path) -> io::Result<ProcessInstanceState> {
    let Some(scope) = open_scope(scope, false)? else {
        return Ok(ProcessInstanceState::Available);
    };
    let file = match open_private_lock_file(&scope.lock_path, false) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(ProcessInstanceState::Available);
        }
        Err(error) => return Err(error),
    };
    match file.file().try_lock() {
        Ok(()) => Ok(ProcessInstanceState::Available),
        Err(TryLockError::WouldBlock) => Ok(ProcessInstanceState::Owned),
        Err(TryLockError::Error(error)) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ownership_is_exclusive_and_pins_scope_until_release() {
        let root = tempfile::tempdir().unwrap();
        let scope = root.path().join("run/daemon");
        assert_eq!(
            process_instance_state(&scope).unwrap(),
            ProcessInstanceState::Available
        );
        let first =
            acquire_prepared_process_instance_lock(prepare_process_instance_scope(&scope).unwrap())
                .unwrap();
        assert_eq!(
            process_instance_state(&scope).unwrap(),
            ProcessInstanceState::Owned
        );
        assert_eq!(
            acquire_prepared_process_instance_lock(prepare_process_instance_scope(&scope).unwrap())
                .unwrap_err()
                .kind(),
            io::ErrorKind::AddrInUse
        );
        assert!(std::fs::rename(scope.parent().unwrap(), root.path().join("replaced")).is_err());
        drop(first);
        assert_eq!(
            process_instance_state(&scope).unwrap(),
            ProcessInstanceState::Available
        );
        acquire_prepared_process_instance_lock(prepare_process_instance_scope(&scope).unwrap())
            .unwrap();
    }
}
