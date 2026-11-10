// SPDX-License-Identifier: Apache-2.0
//! Private host files: native permissions, exclusive staging and journal publication.
#[cfg(unix)]
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::{fs::File, io, path::Path};

pub(super) fn prepare_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        fs::create_dir_all(path)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
    }
    #[cfg(windows)]
    {
        undercurrent_platform_windows::prepare_private_directory(path).map(drop)
    }
}
pub(super) fn create_new(path: &Path) -> io::Result<File> {
    #[cfg(unix)]
    {
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
    }
    #[cfg(windows)]
    {
        undercurrent_platform_windows::create_private_file(path).map(|p| p.into_file())
    }
}
pub(super) fn write_truncated(path: &Path) -> io::Result<File> {
    #[cfg(unix)]
    {
        fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
    }
    #[cfg(windows)]
    {
        let file = undercurrent_platform_windows::open_private_file(path, true, true)?.into_file();
        file.set_len(0)?;
        Ok(file)
    }
}
pub(super) fn read(path: &Path) -> io::Result<File> {
    #[cfg(unix)]
    {
        File::open(path)
    }
    #[cfg(windows)]
    {
        undercurrent_platform_windows::open_private_file(path, false, false).map(|p| p.into_file())
    }
}
pub(super) fn restrict(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
    }
    #[cfg(windows)]
    {
        undercurrent_platform_windows::open_private_file(path, false, false).map(drop)
    }
}
pub(super) fn replace(from: &Path, to: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        fs::rename(from, to)
    }
    #[cfg(windows)]
    {
        undercurrent_platform_windows::replace_private_file(from, to)
    }
}
pub(super) fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()
    }
    #[cfg(windows)]
    {
        // Windows journal publication requests write-through in replace(). There
        // is no unprivileged POSIX directory-fsync equivalent. Retired journal
        // deletion may leave stale files after power loss; recovery revalidates
        // every receipt and never treats a stale file as a live process.
        undercurrent_platform_windows::PinnedPath::directory(path).map(drop)
    }
}
