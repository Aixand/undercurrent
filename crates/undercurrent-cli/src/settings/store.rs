// SPDX-License-Identifier: Apache-2.0

//! Bounded, locked, atomic user settings independent of any selected instance.

use std::fs::{self, File};
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use super::{Settings, invalid, io_error, validate};
use crate::CliError;

const MAX_BYTES: u64 = 128 * 1024;

pub(super) struct Store {
    user_home: PathBuf,
    directory: PathBuf,
}

impl Store {
    pub(super) fn from_environment() -> Result<Self, CliError> {
        let user_home = crate::native_home_directory().map_err(CliError::from)?;
        Ok(Self {
            directory: user_home.join(".aixand").join("undercurrent-cli"),
            user_home,
        })
    }

    pub(super) fn user_home(&self) -> &Path {
        &self.user_home
    }
    pub(super) fn path(&self) -> PathBuf {
        self.directory.join("settings.toml")
    }

    pub(super) fn read(&self) -> Result<Settings, CliError> {
        let file = match open_file(&self.path(), false) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Settings::default());
            }
            Err(error) => return Err(io_error("could not read CLI settings", error)),
        };
        check_file(&file)?;
        let mut bytes = Vec::new();
        file.take(MAX_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| io_error("could not read CLI settings", error))?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(invalid("CLI settings exceed their size limit"));
        }
        let text =
            std::str::from_utf8(&bytes).map_err(|_| invalid("CLI settings are not valid UTF-8"))?;
        let settings = toml::from_str(text).map_err(|_| {
            invalid("CLI settings are malformed; preserve and repair settings.toml")
        })?;
        validate(&settings)?;
        Ok(settings)
    }

    pub(super) fn update(
        &self,
        change: impl FnOnce(&mut Settings) -> Result<(), CliError>,
    ) -> Result<(), CliError> {
        #[cfg(unix)]
        {
            fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(&self.directory)
                .map_err(|error| io_error("could not create CLI settings directory", error))?;
            let metadata = fs::symlink_metadata(&self.directory)
                .map_err(|error| io_error("could not inspect CLI settings directory", error))?;
            if !metadata.is_dir()
                || metadata.uid() != nix::unistd::geteuid().as_raw()
                || metadata.mode() & 0o077 != 0
            {
                return Err(invalid(
                    "CLI settings directory must be private and not a symlink",
                ));
            }
        }
        #[cfg(windows)]
        let _pin = undercurrent_platform_windows::prepare_private_directory(&self.directory)
            .map_err(|error| io_error("could not secure CLI settings directory", error))?;
        let lock = open_file(&self.directory.join("settings.lock"), true)
            .map_err(|error| io_error("could not open CLI settings lock", error))?;
        check_file(&lock)?;
        lock.try_lock()
            .map_err(|_| invalid("CLI settings are busy; retry after the other update finishes"))?;
        let mut settings = self.read()?;
        change(&mut settings)?;
        validate(&settings)?;
        let bytes = toml::to_string_pretty(&settings)
            .map_err(|_| invalid("could not encode CLI settings"))?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(invalid("CLI settings exceed their size limit"));
        }
        let temporary = self
            .directory
            .join(format!(".{}.tmp", uuid::Uuid::now_v7()));
        let result = (|| -> Result<(), std::io::Error> {
            #[cfg(unix)]
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&temporary)?;
            #[cfg(windows)]
            let mut file =
                undercurrent_platform_windows::create_private_file(&temporary)?.into_file();
            file.write_all(bytes.as_bytes())?;
            file.sync_all()?;
            drop(file);
            #[cfg(unix)]
            {
                fs::rename(&temporary, self.path())?;
                File::open(&self.directory)?.sync_all()?;
            }
            #[cfg(windows)]
            undercurrent_platform_windows::replace_private_file(&temporary, &self.path())?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result.map_err(|error| io_error("could not publish CLI settings", error))
    }
}

fn open_file(path: &Path, lock: bool) -> std::io::Result<File> {
    #[cfg(unix)]
    {
        fs::OpenOptions::new()
            .read(true)
            .write(lock)
            .create(lock)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
    }
    #[cfg(windows)]
    {
        if lock {
            undercurrent_platform_windows::open_private_lock_file(path, true)
        } else {
            undercurrent_platform_windows::open_private_file(path, false, false)
        }
        .map(|path| path.into_file())
    }
}

fn check_file(file: &File) -> Result<(), CliError> {
    let metadata = file
        .metadata()
        .map_err(|error| io_error("could not inspect CLI settings", error))?;
    if !metadata.is_file() {
        return Err(invalid("CLI settings must be a regular file"));
    }
    #[cfg(unix)]
    if metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.mode() & 0o077 != 0
        || metadata.nlink() != 1
    {
        return Err(invalid(
            "CLI settings must be owned by this user with private permissions and no hard links",
        ));
    }
    Ok(())
}
