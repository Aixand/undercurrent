// SPDX-License-Identifier: Apache-2.0
use std::{
    fmt, io,
    path::{Path, PathBuf},
    sync::Arc,
};
/// An absolute platform-local endpoint, never a network URL.
///
/// Construction does not canonicalize, open, or authenticate the address.
/// Native adapters perform their own filesystem or named-pipe checks later.
/// `Debug` redacts the address; [`Self::path`] returns it explicitly.
#[derive(Clone, PartialEq, Eq)]
pub struct LocalEndpoint(Arc<PathBuf>);
impl LocalEndpoint {
    /// Validates an endpoint without opening files or connections.
    ///
    /// Requires an absolute path with a file name and no parent traversal.
    /// On Unix, also rejects NUL and addresses at or above the native path
    /// capacity (104 bytes on macOS, 108 on Linux). This does not resolve
    /// symlink ancestors, check permissions, or establish that a peer exists.
    pub fn new(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        if !path.is_absolute()
            || path.file_name().is_none()
            || path
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "local endpoint must be an absolute normalized path",
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            let path_capacity = if cfg!(target_os = "macos") { 104 } else { 108 };
            if path.as_os_str().as_bytes().len() >= path_capacity
                || path.as_os_str().as_bytes().contains(&0)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "local endpoint must be shorter than {path_capacity} bytes and contain no NUL bytes; choose a shorter socket directory"
                    ),
                ));
            }
        }
        Ok(Self(Arc::new(path)))
    }
    /// Returns the configured local address.
    pub fn path(&self) -> &Path {
        &self.0
    }
}
impl fmt::Debug for LocalEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LocalEndpoint([REDACTED])")
    }
}
