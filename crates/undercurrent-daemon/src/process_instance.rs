// SPDX-License-Identifier: Apache-2.0

//! Owner-only daemon instance-lock and runtime-directory mechanics.

use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use nix::fcntl::{OFlag, openat};
use nix::sys::stat::Mode;

const OWNER_ONLY_FILE_MODE: u32 = 0o600;
const INSTANCE_LOCK_SUFFIX: &str = ".instance.lock";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProcessInstanceState {
    Available,
    Owned,
}

#[derive(Debug)]
pub(crate) struct ProcessInstanceLock {
    file: File,
    _parent: File,
}

#[derive(Debug)]
pub(crate) struct PreparedProcessInstanceScope {
    parent: File,
    parent_path: PathBuf,
    lock_name: OsString,
}

impl ProcessInstanceLock {
    #[cfg(test)]
    pub(crate) fn metadata(&self) -> Result<fs::Metadata, std::io::Error> {
        self.file.metadata()
    }
}

impl Drop for ProcessInstanceLock {
    fn drop(&mut self) {
        // Explicitly unlock the shared open-file description before close. A
        // concurrent fork may temporarily inherit the descriptor until exec,
        // so close-on-exec alone cannot guarantee immediate ownership release.
        let _ = self.file.unlock();
    }
}

/// Creates or validates one owner-only absolute runtime directory.
///
/// # Errors
///
/// Returns invalid-path, filesystem, type, or permission failure.
pub fn prepare_private_directory(path: &Path) -> Result<(), std::io::Error> {
    if !path.is_absolute() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "private runtime directory must be absolute",
        ));
    }
    if !path.exists() {
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true);
        builder.mode(0o700);
        builder.create(path)?;
    }
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "private runtime path is not a directory",
        ));
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "private runtime directory is not owner-only",
        ));
    }
    Ok(())
}

/// Creates or validates the owner-only parent of a daemon instance scope.
///
/// The scope is only a lock namespace. It is never opened as a semantic Unix
/// socket or exposed to a client.
///
/// # Errors
///
/// Returns invalid-path, filesystem, type, or permission failure.
pub fn prepare_instance_scope_parent(scope: &Path) -> Result<(), std::io::Error> {
    prepare_process_instance_scope(scope).map(drop)
}

pub(crate) fn prepare_process_instance_scope(
    scope: &Path,
) -> Result<PreparedProcessInstanceScope, std::io::Error> {
    open_process_instance_scope(scope, true)?.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "daemon instance-lock parent is unavailable",
        )
    })
}

fn open_process_instance_scope(
    scope: &Path,
    create: bool,
) -> Result<Option<PreparedProcessInstanceScope>, std::io::Error> {
    if !scope.is_absolute() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "daemon instance scope must be absolute",
        ));
    }
    let parent = scope.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "daemon instance scope has no parent",
        )
    })?;
    if create
        && fs::symlink_metadata(parent)
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    {
        prepare_private_directory(parent)?;
    }
    let directory = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(parent)
    {
        Ok(directory) => directory,
        Err(source) if !create && source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(source),
    };
    validate_instance_parent(&directory)?;
    Ok(Some(PreparedProcessInstanceScope {
        parent: directory,
        parent_path: parent.to_path_buf(),
        lock_name: instance_lock_name(scope)?,
    }))
}

#[cfg(test)]
pub(crate) fn acquire_process_instance_lock(
    scope: &Path,
) -> Result<ProcessInstanceLock, std::io::Error> {
    acquire_prepared_process_instance_lock(prepare_process_instance_scope(scope)?)
}

pub(crate) fn acquire_prepared_process_instance_lock(
    prepared: PreparedProcessInstanceScope,
) -> Result<ProcessInstanceLock, std::io::Error> {
    verify_prepared_parent(&prepared)?;
    let lock =
        open_instance_file(&prepared.parent, &prepared.lock_name, true)?.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "daemon instance lock is unavailable",
            )
        })?;
    validate_instance_file(&lock)?;
    match lock.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => Err(std::io::Error::new(
            std::io::ErrorKind::AddrInUse,
            "another process owns the daemon instance",
        ))?,
        Err(TryLockError::Error(source)) => return Err(source),
    }
    // Lock ownership is attached to the opened file, so it is insufficient if
    // the named parent or lock file was replaced during acquisition. Recheck
    // both identities before allowing this owner to open durable state.
    // See [instance ownership](../docs/internals/lifecycle/startup-and-shutdown.md#identity-and-coupled-persistence).
    verify_prepared_parent(&prepared)?;
    let named =
        open_instance_file(&prepared.parent, &prepared.lock_name, false)?.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "daemon instance lock changed during acquisition",
            )
        })?;
    validate_instance_file(&named)?;
    let held_metadata = lock.metadata()?;
    let named_metadata = named.metadata()?;
    if held_metadata.dev() != named_metadata.dev() || held_metadata.ino() != named_metadata.ino() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "daemon instance lock changed during acquisition",
        ));
    }
    Ok(ProcessInstanceLock {
        file: lock,
        _parent: prepared.parent,
    })
}

pub(crate) fn process_instance_state(scope: &Path) -> Result<ProcessInstanceState, std::io::Error> {
    let Some(prepared) = open_process_instance_scope(scope, false)? else {
        return Ok(ProcessInstanceState::Available);
    };
    verify_prepared_parent(&prepared)?;
    let Some(lock) = open_instance_file(&prepared.parent, &prepared.lock_name, false)? else {
        return Ok(ProcessInstanceState::Available);
    };
    validate_instance_file(&lock)?;
    match lock.try_lock() {
        Ok(()) => Ok(ProcessInstanceState::Available),
        Err(TryLockError::WouldBlock) => Ok(ProcessInstanceState::Owned),
        Err(TryLockError::Error(source)) => Err(source),
    }
}

fn validate_instance_parent(parent: &File) -> Result<(), std::io::Error> {
    let metadata = parent.metadata()?;
    if !metadata.is_dir()
        || metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "daemon instance-lock parent is not owner-private",
        ));
    }
    Ok(())
}

fn verify_prepared_parent(prepared: &PreparedProcessInstanceScope) -> Result<(), std::io::Error> {
    let current = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&prepared.parent_path)?;
    validate_instance_parent(&current)?;
    let held = prepared.parent.metadata()?;
    let current = current.metadata()?;
    if held.dev() != current.dev() || held.ino() != current.ino() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "daemon instance-lock parent changed during acquisition",
        ));
    }
    Ok(())
}

fn open_instance_file(
    parent: &File,
    name: &OsStr,
    create: bool,
) -> Result<Option<File>, std::io::Error> {
    let flags = OFlag::O_RDWR | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC | OFlag::O_NONBLOCK;
    match openat(parent, name, flags, Mode::empty()) {
        Ok(file) => Ok(Some(File::from(file))),
        Err(nix::errno::Errno::ENOENT) if create => match openat(
            parent,
            name,
            flags | OFlag::O_CREAT | OFlag::O_EXCL,
            Mode::from_bits_truncate(OWNER_ONLY_FILE_MODE as _),
        ) {
            Ok(file) => {
                let file = File::from(file);
                file.set_permissions(fs::Permissions::from_mode(OWNER_ONLY_FILE_MODE))?;
                Ok(Some(file))
            }
            Err(nix::errno::Errno::EEXIST) => openat(parent, name, flags, Mode::empty())
                .map(File::from)
                .map(Some)
                .map_err(std::io::Error::from),
            Err(source) => Err(std::io::Error::from(source)),
        },
        Err(nix::errno::Errno::ENOENT) => Ok(None),
        Err(source) => Err(std::io::Error::from(source)),
    }
}

fn validate_instance_file(file: &File) -> Result<(), std::io::Error> {
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.nlink() != 1
        || metadata.permissions().mode() & 0o7777 != OWNER_ONLY_FILE_MODE
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "daemon instance lock is not one owner-only regular file",
        ));
    }
    Ok(())
}

fn instance_lock_name(scope: &Path) -> Result<OsString, std::io::Error> {
    let file_name = scope.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "daemon instance scope has no file name",
        )
    })?;
    let mut lock_name = OsString::from(".");
    lock_name.push(file_name);
    lock_name.push(INSTANCE_LOCK_SUFFIX);
    Ok(lock_name)
}

#[cfg(test)]
fn instance_lock_path(scope: &Path) -> Result<PathBuf, std::io::Error> {
    let parent = scope.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "daemon instance scope has no parent",
        )
    })?;
    Ok(parent.join(instance_lock_name(scope)?))
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::{PermissionsExt, symlink};

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn private_directory_rejects_relative_public_and_symbolic_paths() {
        let temporary = tempdir().expect("temporary directory");
        let public = temporary.path().join("public");
        fs::create_dir(&public).expect("public fixture");
        fs::set_permissions(&public, fs::Permissions::from_mode(0o755)).expect("permissions");
        assert_eq!(
            prepare_private_directory(&public)
                .expect_err("public directory")
                .kind(),
            std::io::ErrorKind::PermissionDenied
        );
        let private = temporary.path().join("private");
        fs::create_dir(&private).expect("private fixture");
        fs::set_permissions(&private, fs::Permissions::from_mode(0o700)).expect("permissions");
        let symbolic = temporary.path().join("symbolic");
        symlink(&private, &symbolic).expect("symlink fixture");
        assert!(prepare_private_directory(&symbolic).is_err());
        assert!(prepare_private_directory(Path::new("relative")).is_err());
    }

    #[test]
    fn instance_lock_is_owner_only_exclusive_and_os_released() {
        let temporary = tempdir().expect("temporary directory");
        let scope = temporary.path().join("private/daemon.instance");
        prepare_instance_scope_parent(&scope).expect("private parent");
        let owner = acquire_process_instance_lock(&scope).expect("owner");
        assert_eq!(
            process_instance_state(&scope).expect("owned state"),
            ProcessInstanceState::Owned
        );
        assert_eq!(
            acquire_process_instance_lock(&scope)
                .expect_err("second owner")
                .kind(),
            std::io::ErrorKind::AddrInUse
        );
        assert_eq!(
            owner.metadata().expect("metadata").permissions().mode() & 0o077,
            0
        );
        drop(owner);
        assert_eq!(
            process_instance_state(&scope).expect("available state"),
            ProcessInstanceState::Available
        );
    }

    #[test]
    fn instance_lock_rejects_symbolic_and_hard_linked_files() {
        let temporary = tempdir().expect("temporary directory");
        let scope = temporary.path().join("private/daemon.instance");
        prepare_instance_scope_parent(&scope).expect("private parent");
        let lock_path = instance_lock_path(&scope).expect("lock path");
        let target = temporary.path().join("target");
        fs::write(&target, b"private").expect("target");
        symlink(&target, &lock_path).expect("symlink");
        assert!(acquire_process_instance_lock(&scope).is_err());
        fs::remove_file(&lock_path).expect("remove symlink");
        fs::hard_link(&target, &lock_path).expect("hard link");
        assert!(acquire_process_instance_lock(&scope).is_err());
    }

    #[test]
    fn prepared_instance_scope_rejects_a_replaced_parent_namespace() {
        let temporary = tempdir().expect("temporary directory");
        let parent = temporary.path().join("private");
        let scope = parent.join("daemon.instance");
        let prepared = prepare_process_instance_scope(&scope).expect("prepared scope");
        let displaced = temporary.path().join("displaced");
        fs::rename(&parent, &displaced).expect("displace prepared parent");
        fs::create_dir(&parent).expect("replacement parent");
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700))
            .expect("replacement permissions");

        let error = acquire_prepared_process_instance_lock(prepared)
            .expect_err("replaced parent must fail closed");

        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(!instance_lock_path(&scope).expect("lock path").exists());
        assert!(
            !displaced
                .join(instance_lock_name(&scope).expect("lock name"))
                .exists()
        );
    }
}
