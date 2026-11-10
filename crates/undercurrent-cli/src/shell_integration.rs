// SPDX-License-Identifier: Apache-2.0

//! Idempotent, bounded, atomic native shell-hook installation.

use std::error::Error;
use std::ffi::OsString;
use std::fmt::{Debug, Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

use crate::ShellArgument;

const HOOK_BEGIN: &[u8] = b"# >>> uc detection hooks >>>";
const HOOK_END: &[u8] = b"# <<< uc detection hooks <<<";
const MAX_STARTUP_FILE_BYTES: u64 = 1_048_576;
const MAX_TEMPORARY_COLLISIONS: usize = 128;
static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Stable shell-integration installation outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ShellIntegrationDisposition {
    /// A new hook block was appended.
    Installed,
    /// An older marked hook block was replaced in place.
    Updated,
    /// The exact current hook block was already present.
    AlreadyInstalled,
    /// The hook document was printed without filesystem mutation.
    Printed,
}

impl ShellIntegrationDisposition {
    /// Returns the stable human-facing outcome name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Installed => "installed",
            Self::Updated => "updated",
            Self::AlreadyInstalled => "already installed",
            Self::Printed => "printed",
        }
    }
}

/// Payload-safe result of one shell-integration operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ShellIntegrationReport {
    /// Explicit shell selected by the caller.
    pub shell: ShellArgument,
    /// Whether the current hook was installed, updated, unchanged, or printed.
    pub disposition: ShellIntegrationDisposition,
}

/// Stable filesystem stages used by shell-integration diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShellIntegrationOperation {
    /// Resolve the caller's home directory from the process environment.
    ResolveHome,
    /// Inspect a startup-file path component or destination.
    InspectPath,
    /// Read the bounded existing startup file.
    ReadStartupFile,
    /// Create a missing startup-file parent directory.
    CreateParentDirectory,
    /// Create a unique same-directory temporary file.
    CreateTemporaryFile,
    /// Write the complete replacement to the temporary file.
    WriteTemporaryFile,
    /// Synchronize the complete temporary file.
    SyncTemporaryFile,
    /// Atomically publish the temporary file.
    PublishStartupFile,
    /// Synchronize the destination directory after publication.
    SyncParentDirectory,
}

impl ShellIntegrationOperation {
    /// Returns the stable payload-free operation name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ResolveHome => "resolve_home",
            Self::InspectPath => "inspect_path",
            Self::ReadStartupFile => "read_startup_file",
            Self::CreateParentDirectory => "create_parent_directory",
            Self::CreateTemporaryFile => "create_temporary_file",
            Self::WriteTemporaryFile => "write_temporary_file",
            Self::SyncTemporaryFile => "sync_temporary_file",
            Self::PublishStartupFile => "publish_startup_file",
            Self::SyncParentDirectory => "sync_parent_directory",
        }
    }
}

/// Stable shell-integration failure categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShellIntegrationErrorKind {
    /// The home-directory environment contract is missing or invalid.
    InvalidConfiguration,
    /// A selected path contains a symbolic link or unsafe component.
    UnsafePath,
    /// The startup file is not regular or has inconsistent hook markers.
    InvalidStartupFile,
    /// The startup file exceeds the bounded installer input size.
    CapacityExceeded,
    /// A concrete filesystem operation failed.
    Filesystem,
}

/// Payload-safe typed failure from shell-hook installation.
pub struct ShellIntegrationError {
    operation: ShellIntegrationOperation,
    kind: ShellIntegrationErrorKind,
    source: Option<io::Error>,
}

impl ShellIntegrationError {
    /// Returns the failed stable filesystem stage.
    pub const fn operation(&self) -> ShellIntegrationOperation {
        self.operation
    }

    /// Returns the stable caller-actionable failure category.
    pub const fn kind(&self) -> ShellIntegrationErrorKind {
        self.kind
    }

    fn new(operation: ShellIntegrationOperation, kind: ShellIntegrationErrorKind) -> Self {
        Self {
            operation,
            kind,
            source: None,
        }
    }

    fn io(operation: ShellIntegrationOperation, source: io::Error) -> Self {
        Self {
            operation,
            kind: ShellIntegrationErrorKind::Filesystem,
            source: Some(source),
        }
    }
}

impl Debug for ShellIntegrationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShellIntegrationError")
            .field("operation", &self.operation)
            .field("kind", &self.kind)
            .field("source_kind", &self.source.as_ref().map(io::Error::kind))
            .finish()
    }
}

impl Display for ShellIntegrationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "shell integration {} failed ({})",
            self.operation.as_str(),
            match self.kind {
                ShellIntegrationErrorKind::InvalidConfiguration => "invalid configuration",
                ShellIntegrationErrorKind::UnsafePath => "unsafe path",
                ShellIntegrationErrorKind::InvalidStartupFile => "invalid startup file",
                ShellIntegrationErrorKind::CapacityExceeded => "capacity exceeded",
                ShellIntegrationErrorKind::Filesystem => "filesystem failure",
            }
        )
    }
}

impl Error for ShellIntegrationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

/// Returns the exact embedded hook document for one supported shell.
///
/// This function performs no IO and does not panic.
pub const fn shell_hook_document(shell: ShellArgument) -> &'static [u8] {
    match shell {
        ShellArgument::Bash => include_bytes!("shell_hooks/bash.sh"),
        ShellArgument::Zsh => include_bytes!("shell_hooks/zsh.sh"),
        ShellArgument::Fish => include_bytes!("shell_hooks/fish.fish"),
        ShellArgument::PowerShell => include_bytes!("shell_hooks/powershell.ps1"),
    }
}

/// Renders shell hooks with an explicitly selected prompt default.
pub fn shell_hook_document_with_prompt(
    shell: ShellArgument,
    prompt: crate::PromptArgument,
) -> Vec<u8> {
    String::from_utf8_lossy(shell_hook_document(shell))
        .replace("UC_PROMPT:-off", &format!("UC_PROMPT:-{}", prompt.as_str()))
        .replace(
            "set -l __uc_prompt_mode off",
            &format!("set -l __uc_prompt_mode {}", prompt.as_str()),
        )
        .replace(
            "$ucPromptMode = 'off'",
            &format!("$ucPromptMode = '{}'", prompt.as_str()),
        )
        .into_bytes()
}

/// Resolves the native home-directory contract without exposing its value.
///
/// Reads `USERPROFILE` on Windows and `HOME` elsewhere without filesystem IO.
///
/// # Errors
///
/// Returns an invalid-configuration failure when the user home is absent, empty, or
/// not absolute.
pub fn native_home_directory() -> Result<PathBuf, ShellIntegrationError> {
    let home = undercurrent_config::PathEnvironment::from_process_env()
        .home_dir
        .filter(|value| !value.as_os_str().is_empty())
        .ok_or_else(|| {
            ShellIntegrationError::new(
                ShellIntegrationOperation::ResolveHome,
                ShellIntegrationErrorKind::InvalidConfiguration,
            )
        })?;
    if !home.is_absolute() {
        return Err(ShellIntegrationError::new(
            ShellIntegrationOperation::ResolveHome,
            ShellIntegrationErrorKind::InvalidConfiguration,
        ));
    }
    Ok(home)
}

/// Installs or updates one shell hook under an explicit home directory.
///
/// Existing contents and permissions are preserved through a bounded,
/// same-directory atomic replacement. Missing parents and a new startup file
/// are owner-only on Unix. Existing symbolic links in the home-to-destination
/// path are rejected. This function performs filesystem IO and does not panic.
///
/// # Errors
///
/// Returns typed unsafe-path, invalid-file, capacity, or filesystem failures.
pub fn install_shell_integration(
    shell: ShellArgument,
    home: &Path,
) -> Result<ShellIntegrationReport, ShellIntegrationError> {
    install_shell_integration_with_prompt(shell, home, crate::PromptArgument::Off)
}

/// Installs shell hooks with an explicitly selected prompt default.
///
/// # Errors
/// Returns the same filesystem and validation failures as `install_shell_integration`.
pub fn install_shell_integration_with_prompt(
    shell: ShellArgument,
    home: &Path,
    prompt: crate::PromptArgument,
) -> Result<ShellIntegrationReport, ShellIntegrationError> {
    validate_home(home)?;
    let destination = startup_file(shell, home)?;
    install_at(shell, home, &destination, prompt)
}

/// Installs into a caller-selected absolute startup file. Existing ancestors
/// and the destination undergo the same symlink and file checks as default paths.
pub(crate) fn install_shell_integration_at_path(
    shell: ShellArgument,
    destination: &Path,
    prompt: crate::PromptArgument,
) -> Result<ShellIntegrationReport, ShellIntegrationError> {
    if !destination.is_absolute() || destination.file_name().is_none() {
        return Err(ShellIntegrationError::new(
            ShellIntegrationOperation::InspectPath,
            ShellIntegrationErrorKind::UnsafePath,
        ));
    }
    let anchor = destination
        .parent()
        .and_then(|parent| parent.ancestors().find(|path| path.is_dir()))
        .ok_or_else(|| {
            ShellIntegrationError::new(
                ShellIntegrationOperation::InspectPath,
                ShellIntegrationErrorKind::UnsafePath,
            )
        })?;
    validate_home(anchor)?;
    install_at(shell, anchor, destination, prompt)
}

fn install_at(
    shell: ShellArgument,
    home: &Path,
    destination: &Path,
    prompt: crate::PromptArgument,
) -> Result<ShellIntegrationReport, ShellIntegrationError> {
    ensure_safe_path(home, destination)?;
    let parent = destination.parent().ok_or_else(|| {
        ShellIntegrationError::new(
            ShellIntegrationOperation::InspectPath,
            ShellIntegrationErrorKind::UnsafePath,
        )
    })?;
    create_private_parents(home, parent)?;
    #[cfg(windows)]
    let _parent_pin =
        undercurrent_platform_windows::PinnedPath::directory(parent).map_err(|source| {
            ShellIntegrationError::io(ShellIntegrationOperation::InspectPath, source)
        })?;
    let existing = read_existing(destination)?;
    let (replacement, disposition) = plan_replacement(
        &existing.bytes,
        &shell_hook_document_with_prompt(shell, prompt),
    )?;
    if disposition == ShellIntegrationDisposition::AlreadyInstalled {
        return Ok(ShellIntegrationReport { shell, disposition });
    }
    ensure_safe_path(home, destination)?;
    publish_atomic(destination, &replacement, existing.permissions.as_ref())?;
    Ok(ShellIntegrationReport { shell, disposition })
}

fn validate_home(home: &Path) -> Result<(), ShellIntegrationError> {
    if !home.is_absolute() {
        return Err(ShellIntegrationError::new(
            ShellIntegrationOperation::ResolveHome,
            ShellIntegrationErrorKind::InvalidConfiguration,
        ));
    }
    let metadata = fs::symlink_metadata(home).map_err(|source| {
        ShellIntegrationError::io(ShellIntegrationOperation::InspectPath, source)
    })?;
    if metadata.file_type().is_symlink() {
        return Err(ShellIntegrationError::new(
            ShellIntegrationOperation::InspectPath,
            ShellIntegrationErrorKind::UnsafePath,
        ));
    }
    if !metadata.is_dir() {
        return Err(ShellIntegrationError::new(
            ShellIntegrationOperation::ResolveHome,
            ShellIntegrationErrorKind::InvalidConfiguration,
        ));
    }
    Ok(())
}

fn startup_file(shell: ShellArgument, home: &Path) -> Result<PathBuf, ShellIntegrationError> {
    if !home.is_absolute() {
        return Err(ShellIntegrationError::new(
            ShellIntegrationOperation::ResolveHome,
            ShellIntegrationErrorKind::InvalidConfiguration,
        ));
    }
    Ok(match shell {
        ShellArgument::Bash => home.join(".bashrc"),
        ShellArgument::Zsh => home.join(".zshrc"),
        ShellArgument::Fish => home.join(".config/fish/config.fish"),
        ShellArgument::PowerShell => {
            #[cfg(windows)]
            {
                home.join("Documents/PowerShell/Microsoft.PowerShell_profile.ps1")
            }
            #[cfg(not(windows))]
            {
                home.join(".config/powershell/Microsoft.PowerShell_profile.ps1")
            }
        }
    })
}

struct ExistingStartupFile {
    bytes: Vec<u8>,
    permissions: Option<fs::Permissions>,
}

fn read_existing(path: &Path) -> Result<ExistingStartupFile, ShellIntegrationError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => Some(metadata),
        Err(source) if source.kind() == io::ErrorKind::NotFound => None,
        Err(source) => {
            return Err(ShellIntegrationError::io(
                ShellIntegrationOperation::InspectPath,
                source,
            ));
        }
    };
    let Some(metadata) = metadata else {
        return Ok(ExistingStartupFile {
            bytes: Vec::new(),
            permissions: None,
        });
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(ShellIntegrationError::new(
            ShellIntegrationOperation::InspectPath,
            ShellIntegrationErrorKind::InvalidStartupFile,
        ));
    }
    if metadata.len() > MAX_STARTUP_FILE_BYTES {
        return Err(ShellIntegrationError::new(
            ShellIntegrationOperation::ReadStartupFile,
            ShellIntegrationErrorKind::CapacityExceeded,
        ));
    }
    #[cfg(windows)]
    let opened =
        undercurrent_platform_windows::PinnedPath::executable(path).map(|pin| pin.into_file());
    #[cfg(not(windows))]
    let opened = File::open(path);
    let file = opened.map_err(|source| {
        ShellIntegrationError::io(ShellIntegrationOperation::ReadStartupFile, source)
    })?;
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(0));
    file.take(MAX_STARTUP_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| {
            ShellIntegrationError::io(ShellIntegrationOperation::ReadStartupFile, source)
        })?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_STARTUP_FILE_BYTES {
        return Err(ShellIntegrationError::new(
            ShellIntegrationOperation::ReadStartupFile,
            ShellIntegrationErrorKind::CapacityExceeded,
        ));
    }
    Ok(ExistingStartupFile {
        bytes,
        permissions: Some(metadata.permissions()),
    })
}

fn plan_replacement(
    existing: &[u8],
    document: &[u8],
) -> Result<(Vec<u8>, ShellIntegrationDisposition), ShellIntegrationError> {
    let markers = find_markers(existing)?;
    if let Some((start, end)) = markers {
        if &existing[start..end] == document {
            return Ok((
                existing.to_vec(),
                ShellIntegrationDisposition::AlreadyInstalled,
            ));
        }
        let mut replacement = Vec::with_capacity(existing.len() - (end - start) + document.len());
        replacement.extend_from_slice(&existing[..start]);
        replacement.extend_from_slice(document);
        replacement.extend_from_slice(&existing[end..]);
        return Ok((replacement, ShellIntegrationDisposition::Updated));
    }
    let mut replacement = Vec::with_capacity(existing.len() + document.len() + 2);
    replacement.extend_from_slice(existing);
    if !replacement.is_empty() && !replacement.ends_with(b"\n") {
        replacement.push(b'\n');
    }
    if !replacement.is_empty() {
        replacement.push(b'\n');
    }
    replacement.extend_from_slice(document);
    Ok((replacement, ShellIntegrationDisposition::Installed))
}

fn find_markers(bytes: &[u8]) -> Result<Option<(usize, usize)>, ShellIntegrationError> {
    let mut begin = None;
    let mut end = None;
    let mut offset = 0;
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        let content = line
            .strip_suffix(b"\n")
            .unwrap_or(line)
            .strip_suffix(b"\r")
            .unwrap_or_else(|| line.strip_suffix(b"\n").unwrap_or(line));
        if content == HOOK_BEGIN && (begin.replace(offset).is_some() || end.is_some()) {
            return Err(invalid_markers());
        }
        if content == HOOK_END && (begin.is_none() || end.replace(offset + line.len()).is_some()) {
            return Err(invalid_markers());
        }
        offset += line.len();
    }
    match (begin, end) {
        (None, None) => Ok(None),
        (Some(begin), Some(end)) => Ok(Some((begin, end))),
        _ => Err(invalid_markers()),
    }
}

fn invalid_markers() -> ShellIntegrationError {
    ShellIntegrationError::new(
        ShellIntegrationOperation::ReadStartupFile,
        ShellIntegrationErrorKind::InvalidStartupFile,
    )
}

fn ensure_safe_path(home: &Path, destination: &Path) -> Result<(), ShellIntegrationError> {
    if !home.is_absolute() || !destination.starts_with(home) {
        return Err(ShellIntegrationError::new(
            ShellIntegrationOperation::InspectPath,
            ShellIntegrationErrorKind::UnsafePath,
        ));
    }
    let mut current = PathBuf::new();
    for component in destination.components() {
        match component {
            Component::Prefix(_) => {
                current.push(component.as_os_str());
                // A drive prefix alone (C:) is not the absolute root. Inspect
                // it after the following RootDir component has been appended.
                continue;
            }
            Component::RootDir | Component::Normal(_) => {
                current.push(component.as_os_str());
            }
            Component::CurDir | Component::ParentDir => {
                return Err(ShellIntegrationError::new(
                    ShellIntegrationOperation::InspectPath,
                    ShellIntegrationErrorKind::UnsafePath,
                ));
            }
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(ShellIntegrationError::new(
                    ShellIntegrationOperation::InspectPath,
                    ShellIntegrationErrorKind::UnsafePath,
                ));
            }
            Ok(_) => {}
            Err(source) if source.kind() == io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(ShellIntegrationError::io(
                    ShellIntegrationOperation::InspectPath,
                    source,
                ));
            }
        }
    }
    Ok(())
}

fn create_private_parents(home: &Path, parent: &Path) -> Result<(), ShellIntegrationError> {
    let relative = parent.strip_prefix(home).map_err(|_| {
        ShellIntegrationError::new(
            ShellIntegrationOperation::InspectPath,
            ShellIntegrationErrorKind::UnsafePath,
        )
    })?;
    let mut current = home.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        match fs::create_dir(&current) {
            Ok(()) => set_private_directory_permissions(&current)?,
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
                let metadata = fs::symlink_metadata(&current).map_err(|source| {
                    ShellIntegrationError::io(ShellIntegrationOperation::InspectPath, source)
                })?;
                if !metadata.is_dir() || metadata.file_type().is_symlink() {
                    return Err(ShellIntegrationError::new(
                        ShellIntegrationOperation::InspectPath,
                        ShellIntegrationErrorKind::UnsafePath,
                    ));
                }
            }
            Err(source) => {
                return Err(ShellIntegrationError::io(
                    ShellIntegrationOperation::CreateParentDirectory,
                    source,
                ));
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> Result<(), ShellIntegrationError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|source| {
        ShellIntegrationError::io(ShellIntegrationOperation::CreateParentDirectory, source)
    })
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_path: &Path) -> Result<(), ShellIntegrationError> {
    Ok(())
}

fn publish_atomic(
    destination: &Path,
    contents: &[u8],
    permissions: Option<&fs::Permissions>,
) -> Result<(), ShellIntegrationError> {
    let parent = destination.parent().ok_or_else(|| {
        ShellIntegrationError::new(
            ShellIntegrationOperation::InspectPath,
            ShellIntegrationErrorKind::UnsafePath,
        )
    })?;
    let (temporary_path, mut temporary) = create_temporary(parent)?;
    let mut pending = PendingTemporary::new(temporary_path.clone());
    temporary.write_all(contents).map_err(|source| {
        ShellIntegrationError::io(ShellIntegrationOperation::WriteTemporaryFile, source)
    })?;
    if let Some(permissions) = permissions {
        temporary
            .set_permissions(permissions.clone())
            .map_err(|source| {
                ShellIntegrationError::io(ShellIntegrationOperation::WriteTemporaryFile, source)
            })?;
    }
    temporary.sync_all().map_err(|source| {
        ShellIntegrationError::io(ShellIntegrationOperation::SyncTemporaryFile, source)
    })?;
    drop(temporary);
    fs::rename(&temporary_path, destination).map_err(|source| {
        ShellIntegrationError::io(ShellIntegrationOperation::PublishStartupFile, source)
    })?;
    pending.retain();
    sync_parent(parent)?;
    Ok(())
}

fn create_temporary(parent: &Path) -> Result<(PathBuf, File), ShellIntegrationError> {
    for _ in 0..MAX_TEMPORARY_COLLISIONS {
        let sequence = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let name = format!(
            ".uc-shell-integration-{}-{sequence}.tmp",
            std::process::id()
        );
        let path = parent.join(name);
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(source) => {
                return Err(ShellIntegrationError::io(
                    ShellIntegrationOperation::CreateTemporaryFile,
                    source,
                ));
            }
        }
    }
    Err(ShellIntegrationError::io(
        ShellIntegrationOperation::CreateTemporaryFile,
        io::Error::new(
            io::ErrorKind::AlreadyExists,
            "temporary filename collision limit exhausted",
        ),
    ))
}

#[cfg(unix)]
fn sync_parent(parent: &Path) -> Result<(), ShellIntegrationError> {
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| {
            ShellIntegrationError::io(ShellIntegrationOperation::SyncParentDirectory, source)
        })
}

#[cfg(not(unix))]
fn sync_parent(_parent: &Path) -> Result<(), ShellIntegrationError> {
    Ok(())
}

struct PendingTemporary {
    path: OsString,
    retained: bool,
}

impl PendingTemporary {
    fn new(path: PathBuf) -> Self {
        Self {
            path: path.into_os_string(),
            retained: false,
        }
    }

    fn retain(&mut self) {
        self.retained = true;
    }
}

impl Drop for PendingTemporary {
    fn drop(&mut self) {
        if !self.retained {
            let _ = fs::remove_file(Path::new(&self.path));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn documents_share_the_bounded_cooperative_contract() {
        let osc_number = b"9091";
        let session_environment = b"UC_SESSION";
        for shell in ShellArgument::ALL {
            let document = shell_hook_document(shell);
            assert!(document.starts_with(HOOK_BEGIN));
            assert!(document.ends_with(b"# <<< uc detection hooks <<<\n"));
            assert!(contains(document, session_environment));
            assert!(contains(document, osc_number));
            assert!(contains(document, b"9091;uc=ready"));
            assert!(contains(document, b"9091;uc=running"));
            assert!(!contains(document, b"cmd="));
        }
    }

    #[test]
    fn prompt_configuration_can_be_enabled_changed_and_disabled() {
        for shell in ShellArgument::ALL {
            let home = temporary_home().unwrap();
            for (prompt, expected) in [
                (
                    crate::PromptArgument::Session,
                    ShellIntegrationDisposition::Installed,
                ),
                (
                    crate::PromptArgument::Session,
                    ShellIntegrationDisposition::AlreadyInstalled,
                ),
                (
                    crate::PromptArgument::Simple,
                    ShellIntegrationDisposition::Updated,
                ),
                (
                    crate::PromptArgument::Off,
                    ShellIntegrationDisposition::Updated,
                ),
            ] {
                let report =
                    install_shell_integration_with_prompt(shell, home.path(), prompt).unwrap();
                assert_eq!(report.disposition, expected);
                let file = startup_file(shell, home.path()).unwrap();
                assert_eq!(
                    fs::read(file).unwrap(),
                    shell_hook_document_with_prompt(shell, prompt)
                );
            }
        }
    }

    #[test]
    fn planning_is_idempotent_and_replaces_one_legacy_block_in_place() {
        let document = shell_hook_document(ShellArgument::Bash);
        let (installed, disposition) = plan_replacement(b"alias x=y\n", document).expect("plan");
        assert_eq!(disposition, ShellIntegrationDisposition::Installed);
        let (same, disposition) = plan_replacement(&installed, document).expect("idempotent plan");
        assert_eq!(disposition, ShellIntegrationDisposition::AlreadyInstalled);
        assert_eq!(same, installed);

        let legacy =
            b"before\n# >>> uc detection hooks >>>\nold\n# <<< uc detection hooks <<<\nafter\n";
        let (updated, disposition) = plan_replacement(legacy, document).expect("update plan");
        assert_eq!(disposition, ShellIntegrationDisposition::Updated);
        assert!(updated.starts_with(b"before\n# >>> uc detection hooks >>>"));
        assert!(updated.ends_with(b"after\n"));
        assert!(!contains(&updated, b"\nold\n"));
    }

    #[test]
    fn malformed_markers_are_rejected_without_rewriting() {
        let error = plan_replacement(
            b"# >>> uc detection hooks >>>\nmissing end\n",
            shell_hook_document(ShellArgument::Bash),
        )
        .expect_err("unclosed block must fail");
        assert_eq!(error.kind(), ShellIntegrationErrorKind::InvalidStartupFile);
    }

    #[test]
    fn native_install_is_idempotent_and_preserves_unrelated_bytes() {
        let home = temporary_home().expect("temporary home");
        let startup = home.path().join(".bashrc");
        let private_prefix = b"alias private='value'\n\xff\n";
        fs::write(&startup, private_prefix).expect("startup file");

        let installed = install_shell_integration(ShellArgument::Bash, home.path())
            .expect("initial installation");
        let first = fs::read(&startup).expect("installed contents");
        let unchanged = install_shell_integration(ShellArgument::Bash, home.path())
            .expect("idempotent installation");
        let second = fs::read(&startup).expect("unchanged contents");

        assert_eq!(
            installed.disposition,
            ShellIntegrationDisposition::Installed
        );
        assert_eq!(
            unchanged.disposition,
            ShellIntegrationDisposition::AlreadyInstalled
        );
        assert!(first.starts_with(private_prefix));
        assert_eq!(first, second);
        assert!(
            fs::read_dir(home.path())
                .expect("home entries")
                .all(|entry| !entry
                    .expect("entry")
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".uc-shell-integration-"))
        );
    }

    #[test]
    fn native_install_replaces_legacy_block_and_creates_fish_parents() {
        let home = temporary_home().expect("temporary home");
        let startup = home.path().join(".zshrc");
        fs::write(
            &startup,
            b"before\n# >>> uc detection hooks >>>\nlegacy\n# <<< uc detection hooks <<<\nafter\n",
        )
        .expect("legacy startup");

        let updated =
            install_shell_integration(ShellArgument::Zsh, home.path()).expect("legacy update");
        let bytes = fs::read(&startup).expect("updated contents");
        assert_eq!(updated.disposition, ShellIntegrationDisposition::Updated);
        assert!(bytes.starts_with(b"before\n"));
        assert!(bytes.ends_with(b"after\n"));
        assert!(!contains(&bytes, b"legacy"));

        let fish =
            install_shell_integration(ShellArgument::Fish, home.path()).expect("fish installation");
        assert_eq!(fish.disposition, ShellIntegrationDisposition::Installed);
        assert_eq!(
            fs::read(home.path().join(".config/fish/config.fish")).expect("fish contents"),
            shell_hook_document(ShellArgument::Fish)
        );
    }

    #[cfg(unix)]
    #[test]
    fn native_install_preserves_existing_mode_and_rejects_symlinks() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let home = temporary_home().expect("temporary home");
        let startup = home.path().join(".bashrc");
        fs::write(&startup, b"existing\n").expect("startup file");
        fs::set_permissions(&startup, fs::Permissions::from_mode(0o640))
            .expect("startup permissions");
        install_shell_integration(ShellArgument::Bash, home.path()).expect("installation");
        assert_eq!(
            fs::metadata(&startup)
                .expect("startup metadata")
                .permissions()
                .mode()
                & 0o777,
            0o640
        );

        let linked_home = temporary_home().expect("linked home");
        let outside = tempfile::NamedTempFile::new().expect("outside file");
        symlink(outside.path(), linked_home.path().join(".bashrc")).expect("startup symlink");
        let error = install_shell_integration(ShellArgument::Bash, linked_home.path())
            .expect_err("symlink must be rejected");
        assert_eq!(error.kind(), ShellIntegrationErrorKind::UnsafePath);
        assert!(!format!("{error:?}").contains(&outside.path().display().to_string()));
    }

    #[test]
    fn oversized_startup_file_is_rejected_before_allocation_or_rewrite() {
        let home = temporary_home().expect("temporary home");
        let startup = home.path().join(".bashrc");
        let file = File::create(&startup).expect("startup file");
        file.set_len(MAX_STARTUP_FILE_BYTES + 1)
            .expect("oversized startup");

        let error = install_shell_integration(ShellArgument::Bash, home.path())
            .expect_err("oversized file must fail");

        assert_eq!(error.kind(), ShellIntegrationErrorKind::CapacityExceeded);
        assert_eq!(
            fs::metadata(startup).expect("startup metadata").len(),
            MAX_STARTUP_FILE_BYTES + 1
        );
    }

    fn temporary_home() -> std::io::Result<tempfile::TempDir> {
        // macOS temp_dir can begin with /var, a symlink to /private/var.
        // Use a real parent so these fixtures exercise the intended file checks.
        tempfile::tempdir_in(std::env::temp_dir().canonicalize()?)
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }
}
