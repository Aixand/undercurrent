// SPDX-License-Identifier: Apache-2.0

//! Product-owned immutable storage for finite command output that exceeds inline bounds.

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::fs;
#[cfg(not(windows))]
use std::fs::OpenOptions;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use undercurrent_application::{
    ImmutableExecutionOutputRef, ImmutableExecutionOutputStore, PortError, PortErrorKind,
    PortFuture, PortOperation,
};
use undercurrent_domain::PayloadDigest;

/// Bounded content-addressed execution-output adapter under the product home.
pub(crate) struct FilesystemExecutionOutputStore {
    root: Arc<PathBuf>,
    #[cfg(windows)]
    _root_pin: undercurrent_platform_windows::PinnedPath,
    maximum_item_bytes: u64,
    next_temporary: AtomicU64,
}

impl FilesystemExecutionOutputStore {
    pub(crate) fn new(root: PathBuf, maximum_item_bytes: u64) -> Result<Self, std::io::Error> {
        if maximum_item_bytes == 0 || !root.is_absolute() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "execution output store configuration is invalid",
            ));
        }
        #[cfg(windows)]
        let root_pin = undercurrent_platform_windows::prepare_private_directory(&root)?;
        #[cfg(not(windows))]
        fs::create_dir_all(&root)?;
        #[cfg(unix)]
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        let metadata = fs::symlink_metadata(&root)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "execution output store root is unsafe",
            ));
        }
        Ok(Self {
            root: Arc::new(root),
            #[cfg(windows)]
            _root_pin: root_pin,
            maximum_item_bytes,
            next_temporary: AtomicU64::new(0),
        })
    }

    fn store_sync(
        &self,
        bytes: &[u8],
        digest: PayloadDigest,
    ) -> Result<ImmutableExecutionOutputRef, PortError> {
        let size_bytes = u64::try_from(bytes.len()).map_err(|_| {
            self.error(
                PortErrorKind::CapacityExceeded,
                ExecutionOutputStoreFailure::Capacity,
            )
        })?;
        if size_bytes > self.maximum_item_bytes {
            return Err(self.error(
                PortErrorKind::CapacityExceeded,
                ExecutionOutputStoreFailure::Capacity,
            ));
        }
        if PayloadDigest::of_bytes(bytes) != digest {
            return Err(self.error(
                PortErrorKind::InvalidData,
                ExecutionOutputStoreFailure::Digest,
            ));
        }
        let digest_hex = digest.to_hex();
        let destination = self.root.join(&digest_hex);
        if destination.exists() {
            self.verify_existing(&destination, digest, size_bytes)?;
            return immutable_reference(digest_hex, digest, size_bytes, self);
        }

        let temporary_ordinal = self.next_temporary.fetch_add(1, Ordering::AcqRel);
        let temporary = self.root.join(format!(
            ".{digest_hex}.{}.{}.tmp",
            std::process::id(),
            temporary_ordinal
        ));
        let write_result = (|| -> Result<(), std::io::Error> {
            #[cfg(not(windows))]
            let mut file = {
                let mut options = OpenOptions::new();
                options.create_new(true).write(true);
                #[cfg(unix)]
                options.mode(0o600);
                options.open(&temporary)?
            };
            #[cfg(windows)]
            let mut file =
                undercurrent_platform_windows::create_private_file(&temporary)?.into_file();
            file.write_all(bytes)?;
            file.sync_all()?;
            // Publish without overwriting an immutable object created by a
            // concurrent writer. AlreadyExists is only provisional success:
            // verify_existing below must still check the winning object's bytes.
            // See [finite output storage](../docs/internals/output-and-streams.md#finite-execution-output-has-different-storage).
            match fs::hard_link(&temporary, &destination) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
                Err(error) => Err(error),
            }
        })();
        let _ = fs::remove_file(&temporary);
        write_result
            .map_err(|_| self.error(PortErrorKind::Unavailable, ExecutionOutputStoreFailure::Io))?;
        self.verify_existing(&destination, digest, size_bytes)?;
        immutable_reference(digest_hex, digest, size_bytes, self)
    }

    fn verify_existing(
        &self,
        path: &Path,
        digest: PayloadDigest,
        size_bytes: u64,
    ) -> Result<(), PortError> {
        #[cfg(windows)]
        let pinned = undercurrent_platform_windows::open_private_file(path, false, false)
            .map_err(|_| self.error(PortErrorKind::Unavailable, ExecutionOutputStoreFailure::Io))?;
        #[cfg(windows)]
        let metadata = pinned.file().metadata();
        #[cfg(not(windows))]
        let metadata = fs::symlink_metadata(path);
        let metadata = metadata
            .map_err(|_| self.error(PortErrorKind::Unavailable, ExecutionOutputStoreFailure::Io))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() != size_bytes
        {
            return Err(self.error(
                PortErrorKind::InvalidData,
                ExecutionOutputStoreFailure::Corrupt,
            ));
        }
        #[cfg(windows)]
        let existing = {
            use std::io::Read;
            let mut bytes = Vec::new();
            pinned
                .file()
                .take(size_bytes.saturating_add(1))
                .read_to_end(&mut bytes)
                .map(|_| bytes)
        };
        #[cfg(not(windows))]
        let existing = fs::read(path);
        let existing = existing
            .map_err(|_| self.error(PortErrorKind::Unavailable, ExecutionOutputStoreFailure::Io))?;
        if PayloadDigest::of_bytes(&existing) != digest {
            return Err(self.error(
                PortErrorKind::InvalidData,
                ExecutionOutputStoreFailure::Corrupt,
            ));
        }
        Ok(())
    }

    fn error(&self, kind: PortErrorKind, source: ExecutionOutputStoreFailure) -> PortError {
        PortError::new(kind, PortOperation::StoreExecutionOutput, None, source)
    }
}

impl Debug for FilesystemExecutionOutputStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FilesystemExecutionOutputStore")
            .field("root", &"[REDACTED]")
            .field("maximum_item_bytes", &self.maximum_item_bytes)
            .finish()
    }
}

impl ImmutableExecutionOutputStore for FilesystemExecutionOutputStore {
    fn store<'a>(
        &'a self,
        bytes: &'a [u8],
        digest: PayloadDigest,
    ) -> PortFuture<'a, Result<ImmutableExecutionOutputRef, PortError>> {
        Box::pin(async move { self.store_sync(bytes, digest) })
    }
}

fn immutable_reference(
    digest_hex: String,
    digest: PayloadDigest,
    size_bytes: u64,
    store: &FilesystemExecutionOutputStore,
) -> Result<ImmutableExecutionOutputRef, PortError> {
    ImmutableExecutionOutputRef::new(
        format!("undercurrent-content:sha256:{digest_hex}"),
        digest,
        size_bytes,
    )
    .map_err(|_| {
        store.error(
            PortErrorKind::InvalidData,
            ExecutionOutputStoreFailure::Reference,
        )
    })
}

#[derive(Debug)]
enum ExecutionOutputStoreFailure {
    Capacity,
    Digest,
    Corrupt,
    Io,
    Reference,
}

impl Display for ExecutionOutputStoreFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Capacity => "execution output exceeds its configured bound",
            Self::Digest => "execution output digest did not match",
            Self::Corrupt => "stored execution output failed integrity validation",
            Self::Io => "execution output storage is unavailable",
            Self::Reference => "execution output reference is invalid",
        })
    }
}

impl Error for ExecutionOutputStoreFailure {}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::*;
    use undercurrent_platform_windows::{PinnedPath, PrivateRemovalFile};

    #[test]
    fn stored_output_is_exact_user_owned_and_reset_can_remove_it() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("output");
        let store = FilesystemExecutionOutputStore::new(root.clone(), 1024).unwrap();
        drop(PinnedPath::owned_directory(&root).unwrap());
        let bytes = b"bounded command output";
        let digest = PayloadDigest::of_bytes(bytes);
        store.store_sync(bytes, digest).unwrap();
        store.store_sync(bytes, digest).unwrap();
        let path = root.join(digest.to_hex());
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
        assert!(fs::rename(&root, temporary.path().join("moved")).is_err());
        drop(store);
        PrivateRemovalFile::open(&path).unwrap().remove().unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn capacity_digest_and_link_failures_do_not_publish_output() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("output");
        let store = FilesystemExecutionOutputStore::new(root.clone(), 4).unwrap();
        assert!(
            store
                .store_sync(b"large", PayloadDigest::of_bytes(b"large"))
                .is_err()
        );
        assert!(
            store
                .store_sync(b"data", PayloadDigest::of_bytes(b"other"))
                .is_err()
        );
        assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
        let digest = PayloadDigest::of_bytes(b"data");
        store.store_sync(b"data", digest).unwrap();
        let path = root.join(digest.to_hex());
        fs::hard_link(&path, temporary.path().join("outside-link")).unwrap();
        assert!(store.store_sync(b"data", digest).is_err());
        assert_eq!(fs::read(&path).unwrap(), b"data");
    }
}
