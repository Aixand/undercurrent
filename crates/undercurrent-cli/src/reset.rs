// SPDX-License-Identifier: Apache-2.0

//! Confirmed local-state reset orchestration and native filesystem adapter.

use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fmt::{Debug, Display, Formatter};
use std::fs::{self, File};
#[cfg(unix)]
use std::io::Read;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(unix)]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(unix)]
use nix::dir::Dir;
#[cfg(unix)]
use nix::fcntl::{OFlag, open, openat};
#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[cfg(unix)]
use nix::fcntl::{RenameFlags, renameat2};
#[cfg(unix)]
use nix::sys::stat::Mode;
#[cfg(unix)]
use nix::unistd::{UnlinkatFlags, unlinkat};
use serde::Serialize;

#[cfg(windows)]
mod windows;
use crate::daemon_lifecycle::acquire_available_daemon_instance_lock;
use crate::{CliError, DaemonLifecycleService, DaemonStopState, ResolvedCliConfig};
#[cfg(windows)]
use windows::*;

const LOOPBACK_TEMPORARY_PREFIX: &[u8] = b".websocket.token.create-";
const LOOPBACK_TEMPORARY_SUFFIX_BYTES: usize = 32;
const LOOPBACK_TOKEN_BYTES: u64 = 64;
const SESSION_HOST_SOCKET_FILE: &str = "session-host.sock";
const SESSION_HOST_AUTH_EXTENSION: &str = "auth";
const SESSION_HOST_EXECUTION_EXTENSION: &str = "execution-jobs";
const SESSION_HOST_RECEIPT_EXTENSION: &str = "attempt-receipts";
const EXECUTION_OUTPUT_DIRECTORY: &str = "execution-output";
#[cfg(unix)]
const QUARANTINE_ATTEMPTS: usize = 16;
#[cfg(unix)]
const REMOVAL_QUARANTINE_PREFIX: &str = ".undercurrent-reset-remove";
#[cfg(unix)]
const REMOVAL_QUARANTINE_ENTROPY_BYTES: usize = 16;
#[cfg(unix)]
static QUARANTINE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Stable reset stage suitable for path-redacted diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetStage {
    /// Acquire and retain exclusive daemon instance ownership.
    AcquireInstanceLock,
    /// Discover reserved stale credential temporaries.
    DiscoverArtifacts,
    /// Verify that no private process host can mutate coupled state.
    VerifyProcessHost,
    /// Atomically move one current entry into a private quarantine name.
    QuarantineArtifact,
    /// Validate an exact product-owned artifact before removal.
    InspectArtifact,
    /// Restore a quarantined entry after validation failure.
    RestoreArtifact,
    /// Remove one validated product-owned artifact.
    RemoveArtifact,
    /// Construct a non-overlapping artifact plan from resolved paths.
    BuildPlan,
}

impl ResetStage {
    /// Returns the stable stage name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AcquireInstanceLock => "acquire_instance_lock",
            Self::DiscoverArtifacts => "discover_artifacts",
            Self::VerifyProcessHost => "verify_process_host",
            Self::QuarantineArtifact => "quarantine_artifact",
            Self::InspectArtifact => "inspect_artifact",
            Self::RestoreArtifact => "restore_artifact",
            Self::RemoveArtifact => "remove_artifact",
            Self::BuildPlan => "build_plan",
        }
    }
}

/// Stable reset failure category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetErrorKind {
    /// A writer retained lifecycle ownership while reset was requested.
    Busy,
    /// A configured artifact had an unsafe type or collided with another artifact.
    UnsafeArtifact,
    /// Native filesystem IO failed.
    Filesystem,
}

/// Observable mutation progress when reset fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetMutationState {
    /// No artifact was unlinked and quarantined entries were restorable.
    Reversible,
    /// At least one prior artifact was durably removed before a later failure.
    Partial,
    /// Unlink succeeded but the containing directory could not be synchronized.
    DurabilityUnknown,
}

/// Path-redacted reset failure retaining its concrete source internally.
pub struct ResetError {
    kind: ResetErrorKind,
    stage: ResetStage,
    source: std::io::Error,
    mutation: ResetMutationState,
    removed: ResetArtifactCounts,
    durability_unknown: ResetArtifactCounts,
    rollback_complete: bool,
}

impl ResetError {
    fn filesystem(stage: ResetStage, source: std::io::Error) -> Self {
        Self {
            kind: ResetErrorKind::Filesystem,
            stage,
            source,
            mutation: ResetMutationState::Reversible,
            removed: ResetArtifactCounts::default(),
            durability_unknown: ResetArtifactCounts::default(),
            rollback_complete: true,
        }
    }

    fn unsafe_artifact(stage: ResetStage, message: &'static str) -> Self {
        Self {
            kind: ResetErrorKind::UnsafeArtifact,
            stage,
            source: std::io::Error::new(std::io::ErrorKind::InvalidData, message),
            mutation: ResetMutationState::Reversible,
            removed: ResetArtifactCounts::default(),
            durability_unknown: ResetArtifactCounts::default(),
            rollback_complete: true,
        }
    }

    fn busy(stage: ResetStage, source: std::io::Error) -> Self {
        Self {
            kind: ResetErrorKind::Busy,
            stage,
            source,
            mutation: ResetMutationState::Reversible,
            removed: ResetArtifactCounts::default(),
            durability_unknown: ResetArtifactCounts::default(),
            rollback_complete: true,
        }
    }

    #[cfg(unix)]
    fn after_unlink_sync_failure(group: ResetArtifactGroup, source: std::io::Error) -> Self {
        let mut removed = ResetArtifactCounts::default();
        removed.increment(group);
        Self {
            kind: ResetErrorKind::Filesystem,
            stage: ResetStage::RemoveArtifact,
            source,
            mutation: ResetMutationState::DurabilityUnknown,
            removed,
            durability_unknown: removed,
            rollback_complete: true,
        }
    }

    fn with_prior_removed(mut self, prior: ResetArtifactCounts) -> Self {
        self.removed.merge(prior);
        if self.mutation == ResetMutationState::Reversible && self.removed.total() > 0 {
            self.mutation = ResetMutationState::Partial;
        }
        self
    }

    fn mark_rollback_failed(&mut self) {
        self.rollback_complete = false;
    }

    /// Returns the stable failure category.
    pub const fn kind(&self) -> ResetErrorKind {
        self.kind
    }

    /// Returns the stable failed stage.
    pub const fn stage(&self) -> ResetStage {
        self.stage
    }

    /// Returns whether failure was reversible, partial, or durability-unknown.
    pub const fn mutation(&self) -> ResetMutationState {
        self.mutation
    }

    /// Returns artifacts observed unlinked before failure.
    pub const fn removed(&self) -> ResetArtifactCounts {
        self.removed
    }

    /// Returns unlinks whose directory durability could not be confirmed.
    pub const fn durability_unknown(&self) -> ResetArtifactCounts {
        self.durability_unknown
    }

    /// Returns whether every still-named quarantine was restored.
    pub const fn rollback_complete(&self) -> bool {
        self.rollback_complete
    }
}

impl Debug for ResetError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResetError")
            .field("kind", &self.kind)
            .field("stage", &self.stage)
            .field("mutation", &self.mutation)
            .field("removed", &self.removed)
            .field("durability_unknown", &self.durability_unknown)
            .field("rollback_complete", &self.rollback_complete)
            .finish_non_exhaustive()
    }
}

impl Display for ResetError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "local state reset failed at {}",
            self.stage.as_str()
        )
    }
}

impl Error for ResetError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

/// Aggregate exact-artifact counts that never expose configured paths.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ResetArtifactCounts {
    /// SQLite files and coupled process-host receipt, job, and output journals.
    pub persistence_files: usize,
    /// Connection credential files.
    pub connection_files: usize,
    /// Active and rotated structured log files.
    pub log_files: usize,
}

impl ResetArtifactCounts {
    /// Returns the total artifact count across all stable groups.
    pub const fn total(self) -> usize {
        self.persistence_files
            .saturating_add(self.connection_files)
            .saturating_add(self.log_files)
    }

    fn increment(&mut self, group: ResetArtifactGroup) {
        let count = match group {
            ResetArtifactGroup::Persistence => &mut self.persistence_files,
            ResetArtifactGroup::Connection => &mut self.connection_files,
            ResetArtifactGroup::Log => &mut self.log_files,
        };
        *count = count.saturating_add(1);
    }

    fn merge(&mut self, other: Self) {
        self.persistence_files = self
            .persistence_files
            .saturating_add(other.persistence_files);
        self.connection_files = self.connection_files.saturating_add(other.connection_files);
        self.log_files = self.log_files.saturating_add(other.log_files);
    }
}

/// Complete result of exact product-state removal.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ResetStateOutcome {
    /// Artifacts that existed and were removed.
    pub removed: ResetArtifactCounts,
    /// Planned artifacts that were already absent.
    pub already_absent: ResetArtifactCounts,
}

/// Complete successful reset result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ResetOutcome {
    /// Daemon lifecycle transition completed before state removal.
    pub daemon: DaemonStopState,
    /// Exact path-redacted artifact outcome.
    pub state: ResetStateOutcome,
}

/// Process-lifecycle boundary required by reset orchestration.
pub trait ResetLifecyclePort {
    /// Stops the local daemon using the forceful reset policy.
    ///
    /// # Errors
    ///
    /// Returns a typed daemon lifecycle failure.
    fn stop_daemon(&self) -> Result<DaemonStopState, CliError>;
}

/// Native lifecycle adapter that reuses the bounded daemon service.
pub struct NativeResetLifecyclePort<'a> {
    daemon: DaemonLifecycleService<'a>,
    daemon_drain_timeout: String,
}

impl<'a> NativeResetLifecyclePort<'a> {
    /// Binds an existing lifecycle service to reset-specific ordering.
    pub fn new(daemon: DaemonLifecycleService<'a>, daemon_drain_timeout: String) -> Self {
        Self {
            daemon,
            daemon_drain_timeout,
        }
    }
}

impl ResetLifecyclePort for NativeResetLifecyclePort<'_> {
    fn stop_daemon(&self) -> Result<DaemonStopState, CliError> {
        self.daemon
            .stop_for_reset(&self.daemon_drain_timeout)
            .map(|outcome| outcome.state)
    }
}

/// Product-state boundary required by reset orchestration.
pub trait ResetStatePort {
    /// Removes exact validated artifacts after daemon shutdown.
    ///
    /// # Errors
    ///
    /// Returns a typed unsafe-artifact or filesystem failure.
    fn clear(&self) -> Result<ResetStateOutcome, ResetError>;
}

/// Reset use case enforcing daemon shutdown before durable-state removal.
pub struct ResetService<'a> {
    lifecycle: &'a dyn ResetLifecyclePort,
    state: &'a dyn ResetStatePort,
}

impl<'a> ResetService<'a> {
    /// Binds reset to explicit lifecycle and state ports without performing IO.
    pub const fn new(lifecycle: &'a dyn ResetLifecyclePort, state: &'a dyn ResetStatePort) -> Self {
        Self { lifecycle, state }
    }

    /// Confirms local writer shutdown before removing exact owned state.
    ///
    /// # Errors
    ///
    /// Returns the lifecycle or reset-state boundary failure.
    pub fn execute(&self) -> Result<ResetOutcome, CliError> {
        let daemon = self.lifecycle.stop_daemon()?;
        let state = self.state.clear().map_err(CliError::Reset)?;
        Ok(ResetOutcome { daemon, state })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResetArtifactGroup {
    Persistence,
    Connection,
    Log,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResetArtifactPolicy {
    General,
    CredentialTemporary,
}

#[derive(Debug, Clone)]
struct AnchoredResetArtifact {
    #[cfg(unix)]
    parent: Arc<File>,
    #[cfg(windows)]
    parent: Arc<undercurrent_platform_windows::PinnedPath>,
    name: OsString,
}

#[derive(Debug, Clone)]
struct ResetArtifact {
    path: PathBuf,
    group: ResetArtifactGroup,
    policy: ResetArtifactPolicy,
    anchored: Option<AnchoredResetArtifact>,
}

/// Native exact-file reset adapter bound to one CLI-owned configuration projection.
pub struct NativeResetStatePort {
    instance_lock: PathBuf,
    product_home: PathBuf,
    session_host: PathBuf,
    host_state_directories: Vec<PathBuf>,
    artifacts: Vec<ResetArtifact>,
    sync_parent: fn(&File) -> Result<(), std::io::Error>,
}

impl Debug for NativeResetStatePort {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeResetStatePort")
            .field("artifact_count", &self.artifacts.len())
            .finish()
    }
}

impl NativeResetStatePort {
    /// Builds a path-private, non-overlapping reset plan without filesystem IO.
    ///
    /// # Errors
    ///
    /// Returns a redacted unsafe-plan failure when derived paths overlap.
    pub fn new(config: &ResolvedCliConfig) -> Result<Self, ResetError> {
        Self::from_paths(
            config.instance_lock(),
            config.product_home(),
            config.database(),
            config.log(),
            config.retained_log_files(),
        )
    }

    fn from_paths(
        instance_lock: &Path,
        product_home: &Path,
        database: &Path,
        daemon_log: &Path,
        daemon_log_files: usize,
    ) -> Result<Self, ResetError> {
        let mut artifacts = vec![
            artifact(database.to_path_buf(), ResetArtifactGroup::Persistence),
            artifact(
                with_suffix(database, "-wal"),
                ResetArtifactGroup::Persistence,
            ),
            artifact(
                with_suffix(database, "-shm"),
                ResetArtifactGroup::Persistence,
            ),
            artifact(
                with_suffix(database, "-journal"),
                ResetArtifactGroup::Persistence,
            ),
        ];
        for directory in ["group", "tui"] {
            for name in ["workspaces.json", "workspaces.0.1.0-beta.json"] {
                artifacts.push(artifact(
                    product_home.join("state").join(directory).join(name),
                    ResetArtifactGroup::Persistence,
                ));
            }
        }
        let session_host = product_home.join("run").join(SESSION_HOST_SOCKET_FILE);
        artifacts.push(artifact(
            session_host.with_extension(SESSION_HOST_AUTH_EXTENSION),
            ResetArtifactGroup::Connection,
        ));
        let host_state_directories = vec![
            product_home
                .join("state/session-host/session-host")
                .with_extension(SESSION_HOST_RECEIPT_EXTENSION),
            product_home
                .join("state/session-host/session-host")
                .with_extension(SESSION_HOST_EXECUTION_EXTENSION),
            product_home.join("state").join(EXECUTION_OUTPUT_DIRECTORY),
        ];
        add_log_artifacts(&mut artifacts, daemon_log, daemon_log_files);
        let mut paths = artifacts
            .iter()
            .map(|candidate| candidate.path.as_path())
            .collect::<Vec<_>>();
        paths.sort_unstable();
        if paths.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ResetError::unsafe_artifact(
                ResetStage::BuildPlan,
                "derived reset artifact paths overlap",
            ));
        }
        Ok(Self {
            instance_lock: instance_lock.to_path_buf(),
            product_home: product_home.to_path_buf(),
            session_host,
            host_state_directories,
            artifacts,
            sync_parent: sync_directory,
        })
    }
}

impl ResetStatePort for NativeResetStatePort {
    fn clear(&self) -> Result<ResetStateOutcome, ResetError> {
        let instance_guard =
            acquire_available_daemon_instance_lock(&self.instance_lock).map_err(|source| {
                if source.kind() == std::io::ErrorKind::WouldBlock {
                    ResetError::busy(ResetStage::AcquireInstanceLock, source)
                } else {
                    ResetError::filesystem(ResetStage::AcquireInstanceLock, source)
                }
            })?;
        ensure_session_host_inactive(&self.session_host)?;
        let _group_locks =
            crate::group::reset_locks(&self.product_home.join("state")).map_err(|e| {
                ResetError::busy(
                    ResetStage::AcquireInstanceLock,
                    std::io::Error::other(e.to_string()),
                )
            })?;
        let mut artifacts = self.artifacts.clone();
        artifacts.extend(discover_stale_loopback_temporaries(
            &self.product_home.join("run"),
        )?);
        for directory in &self.host_state_directories {
            artifacts.extend(discover_private_state_files(
                directory,
                ResetArtifactGroup::Persistence,
            )?);
        }
        artifacts.sort_by(|left, right| left.path.cmp(&right.path));
        if artifacts
            .windows(2)
            .any(|pair| pair[0].path == pair[1].path)
        {
            return Err(ResetError::unsafe_artifact(
                ResetStage::BuildPlan,
                "discovered reset artifact paths overlap",
            ));
        }
        instance_guard
            .revalidate()
            .map_err(|source| ResetError::filesystem(ResetStage::AcquireInstanceLock, source))?;
        let mut outcome = ResetStateOutcome::default();
        let mut quarantined = Vec::new();
        // Prepare the whole plan before the first unlink so a validation failure
        // can restore prepared entries. Removal is still per-file: once it starts,
        // later failures cannot roll back files already deleted.
        // See [reset recovery](../../../docs/guides/daemon/cli-lifecycle.md#recover-from-reset-failure).
        for artifact in artifacts {
            let group = artifact.group;
            match quarantine_artifact(artifact) {
                Ok(Some(artifact)) => quarantined.push(artifact),
                Ok(None) => outcome.already_absent.increment(group),
                Err(error) => {
                    let mut error = error.with_prior_removed(outcome.removed);
                    if restore_all(&mut quarantined).is_err() {
                        error.mark_rollback_failed();
                    }
                    return Err(error);
                }
            }
        }
        while let Some(mut artifact) = quarantined.pop() {
            let group = artifact.artifact.group;
            if let Err(error) = remove_quarantined(&mut artifact, self.sync_parent) {
                let post_unlink = error.mutation() == ResetMutationState::DurabilityUnknown;
                if !post_unlink {
                    quarantined.push(artifact);
                }
                let mut error = error.with_prior_removed(outcome.removed);
                if restore_all(&mut quarantined).is_err() {
                    error.mark_rollback_failed();
                }
                return Err(error);
            }
            outcome.removed.increment(group);
        }
        Ok(outcome)
    }
}

#[cfg(unix)]
fn ensure_session_host_inactive(socket: &Path) -> Result<(), ResetError> {
    let metadata = match fs::symlink_metadata(socket) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(ResetError::filesystem(
                ResetStage::VerifyProcessHost,
                source,
            ));
        }
    };
    if !metadata.file_type().is_socket()
        || metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.nlink() != 1
    {
        return Err(ResetError::unsafe_artifact(
            ResetStage::VerifyProcessHost,
            "session-host endpoint is not one product-owned socket",
        ));
    }
    match UnixStream::connect(socket) {
        Ok(_) => Err(ResetError::busy(
            ResetStage::VerifyProcessHost,
            std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "session host remains active",
            ),
        )),
        Err(source)
            if matches!(
                source.kind(),
                std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
            ) =>
        {
            Ok(())
        }
        Err(source) => Err(ResetError::filesystem(
            ResetStage::VerifyProcessHost,
            source,
        )),
    }
}

fn artifact(path: PathBuf, group: ResetArtifactGroup) -> ResetArtifact {
    ResetArtifact {
        path,
        group,
        policy: ResetArtifactPolicy::General,
        anchored: None,
    }
}

fn add_log_artifacts(artifacts: &mut Vec<ResetArtifact>, path: &Path, retained_files: usize) {
    artifacts.push(artifact(path.to_path_buf(), ResetArtifactGroup::Log));
    for generation in 1..retained_files {
        artifacts.push(artifact(
            with_suffix(path, &format!(".{generation}")),
            ResetArtifactGroup::Log,
        ));
    }
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value: OsString = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

#[cfg(unix)]
struct QuarantinedArtifact {
    parent: File,
    original_name: OsString,
    quarantine_name: OsString,
    file: File,
    artifact: ResetArtifact,
}

#[cfg(unix)]
fn quarantine_artifact(artifact: ResetArtifact) -> Result<Option<QuarantinedArtifact>, ResetError> {
    let (parent, original_name) = if let Some(anchored) = &artifact.anchored {
        let parent = anchored
            .parent
            .try_clone()
            .map_err(|source| ResetError::filesystem(ResetStage::QuarantineArtifact, source))?;
        validate_private_directory(&parent, ResetStage::QuarantineArtifact)?;
        (parent, anchored.name.clone())
    } else {
        let parent_path = artifact.path.parent().ok_or_else(|| {
            ResetError::unsafe_artifact(
                ResetStage::QuarantineArtifact,
                "reset artifact has no parent",
            )
        })?;
        let original_name = artifact.path.file_name().ok_or_else(|| {
            ResetError::unsafe_artifact(
                ResetStage::QuarantineArtifact,
                "reset artifact has no name",
            )
        })?;
        let Some(parent) = open_private_directory(parent_path, ResetStage::QuarantineArtifact)?
        else {
            return Ok(None);
        };
        (parent, original_name.to_os_string())
    };
    let quarantine_name = (0..QUARANTINE_ATTEMPTS)
        .find_map(|_| {
            let sequence = QUARANTINE_SEQUENCE.fetch_add(1, Ordering::AcqRel);
            let name = OsString::from(format!(
                ".undercurrent-reset-{}-{sequence:016x}",
                std::process::id()
            ));
            match rename_no_replace(&parent, &original_name, &name) {
                Ok(()) => Some(Ok(name)),
                Err(source) if source.raw_os_error() == Some(libc::ENOENT) => Some(Err(None)),
                Err(source) if source.raw_os_error() == Some(libc::EEXIST) => None,
                Err(source) => Some(Err(Some(source))),
            }
        })
        .ok_or_else(|| {
            ResetError::unsafe_artifact(
                ResetStage::QuarantineArtifact,
                "reset quarantine namespace remained contended",
            )
        })?;
    let quarantine_name = match quarantine_name {
        Ok(name) => name,
        Err(None) => return Ok(None),
        Err(Some(source)) => {
            return Err(ResetError::filesystem(
                ResetStage::QuarantineArtifact,
                source,
            ));
        }
    };
    let file = match openat(
        &parent,
        quarantine_name.as_os_str(),
        OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC | OFlag::O_NONBLOCK,
        Mode::empty(),
    ) {
        Ok(file) => File::from(file),
        Err(source) => {
            restore_one(&parent, &quarantine_name, &original_name)?;
            return Err(ResetError::filesystem(
                ResetStage::InspectArtifact,
                std::io::Error::from(source),
            ));
        }
    };
    if let Err(error) = validate_reset_metadata(
        &file
            .metadata()
            .map_err(|source| ResetError::filesystem(ResetStage::InspectArtifact, source))?,
        nix::unistd::geteuid().as_raw(),
        artifact.policy,
    ) {
        restore_one(&parent, &quarantine_name, &original_name)?;
        return Err(error);
    }
    Ok(Some(QuarantinedArtifact {
        parent,
        original_name,
        quarantine_name,
        file,
        artifact,
    }))
}

#[cfg(unix)]
fn revalidate_quarantined_at(
    artifact: &QuarantinedArtifact,
    name: &OsStr,
) -> Result<(), ResetError> {
    let held = artifact
        .file
        .metadata()
        .map_err(|source| ResetError::filesystem(ResetStage::InspectArtifact, source))?;
    validate_reset_metadata(
        &held,
        nix::unistd::geteuid().as_raw(),
        artifact.artifact.policy,
    )?;
    let current = File::from(
        openat(
            &artifact.parent,
            name,
            OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC | OFlag::O_NONBLOCK,
            Mode::empty(),
        )
        .map_err(|source| {
            ResetError::filesystem(ResetStage::InspectArtifact, std::io::Error::from(source))
        })?,
    );
    let current = current
        .metadata()
        .map_err(|source| ResetError::filesystem(ResetStage::InspectArtifact, source))?;
    validate_reset_metadata(
        &current,
        nix::unistd::geteuid().as_raw(),
        artifact.artifact.policy,
    )?;
    if held.dev() != current.dev() || held.ino() != current.ino() {
        return Err(ResetError::unsafe_artifact(
            ResetStage::InspectArtifact,
            "reset quarantine entry changed before removal",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn remove_quarantined(
    artifact: &mut QuarantinedArtifact,
    sync_parent: fn(&File) -> Result<(), std::io::Error>,
) -> Result<(), ResetError> {
    remove_quarantined_with_barrier(artifact, sync_parent, |_, _| Ok(()))
}

#[cfg(unix)]
fn remove_quarantined_with_barrier(
    artifact: &mut QuarantinedArtifact,
    sync_parent: fn(&File) -> Result<(), std::io::Error>,
    after_private_detach: impl FnOnce(&File, &OsStr) -> Result<(), std::io::Error>,
) -> Result<(), ResetError> {
    let previous_name = artifact.quarantine_name.clone();
    // Detach from the predictable preparation name before revalidating and
    // unlinking. A new entry at that old name must survive; a swapped object
    // moved to the removal name must fail the held-inode comparison below.
    let removal_name = (0..QUARANTINE_ATTEMPTS)
        .find_map(|_| {
            let name = match removal_quarantine_name() {
                Ok(name) => name,
                Err(source) => return Some(Err(source)),
            };
            match rename_no_replace(&artifact.parent, &previous_name, &name) {
                Ok(()) => Some(Ok(name)),
                Err(source) if source.raw_os_error() == Some(libc::EEXIST) => None,
                Err(source) => Some(Err(source)),
            }
        })
        .ok_or_else(|| {
            ResetError::unsafe_artifact(
                ResetStage::RemoveArtifact,
                "reset removal quarantine namespace remained contended",
            )
        })?
        .map_err(|source| ResetError::filesystem(ResetStage::RemoveArtifact, source))?;
    if let Err(source) = after_private_detach(&artifact.parent, &previous_name) {
        restore_private_removal(artifact, &removal_name, &previous_name);
        return Err(ResetError::filesystem(ResetStage::InspectArtifact, source));
    }
    if let Err(error) = revalidate_quarantined_at(artifact, &removal_name) {
        restore_private_removal(artifact, &removal_name, &previous_name);
        return Err(error);
    }
    if let Err(source) = unlinkat(
        &artifact.parent,
        removal_name.as_os_str(),
        UnlinkatFlags::NoRemoveDir,
    ) {
        restore_private_removal(artifact, &removal_name, &previous_name);
        return Err(ResetError::filesystem(
            ResetStage::RemoveArtifact,
            std::io::Error::from(source),
        ));
    }
    sync_parent(&artifact.parent)
        .map_err(|source| ResetError::after_unlink_sync_failure(artifact.artifact.group, source))
}

#[cfg(unix)]
fn restore_private_removal(
    artifact: &mut QuarantinedArtifact,
    removal_name: &OsStr,
    previous_name: &OsStr,
) {
    if rename_no_replace(&artifact.parent, removal_name, previous_name).is_err() {
        artifact.quarantine_name = removal_name.to_os_string();
    }
}

#[cfg(unix)]
fn removal_quarantine_name() -> Result<OsString, std::io::Error> {
    let mut entropy = [0_u8; REMOVAL_QUARANTINE_ENTROPY_BYTES];
    File::open("/dev/urandom")?.read_exact(&mut entropy)?;
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut name = String::with_capacity(
        REMOVAL_QUARANTINE_PREFIX.len() + 1 + REMOVAL_QUARANTINE_ENTROPY_BYTES * 2,
    );
    name.push_str(REMOVAL_QUARANTINE_PREFIX);
    name.push('-');
    for byte in entropy {
        name.push(char::from(HEX[usize::from(byte >> 4)]));
        name.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(OsString::from(name))
}

#[cfg(unix)]
fn validate_reset_metadata(
    metadata: &fs::Metadata,
    expected_owner: u32,
    policy: ResetArtifactPolicy,
) -> Result<(), ResetError> {
    if !metadata.file_type().is_file() || metadata.uid() != expected_owner || metadata.nlink() != 1
    {
        return Err(ResetError::unsafe_artifact(
            ResetStage::InspectArtifact,
            "reset artifact must be one product-owned regular file",
        ));
    }
    if policy == ResetArtifactPolicy::CredentialTemporary
        && (metadata.permissions().mode() & 0o7777 != 0o600
            || metadata.len() > LOOPBACK_TOKEN_BYTES)
    {
        return Err(ResetError::unsafe_artifact(
            ResetStage::InspectArtifact,
            "stale credential temporary has unsafe metadata",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn restore_all(artifacts: &mut Vec<QuarantinedArtifact>) -> Result<(), ResetError> {
    let mut first_error = None;
    while let Some(artifact) = artifacts.pop() {
        if let Err(error) = restore_one(
            &artifact.parent,
            &artifact.quarantine_name,
            &artifact.original_name,
        ) && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    Ok(())
}

#[cfg(unix)]
fn restore_one(parent: &File, quarantine: &OsStr, original: &OsStr) -> Result<(), ResetError> {
    rename_no_replace(parent, quarantine, original)
        .map_err(|source| ResetError::filesystem(ResetStage::RestoreArtifact, source))
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn rename_no_replace(
    parent: &File,
    source: &OsStr,
    destination: &OsStr,
) -> Result<(), std::io::Error> {
    renameat2(
        parent,
        source,
        parent,
        destination,
        RenameFlags::RENAME_NOREPLACE,
    )
    .map_err(std::io::Error::from)
}

#[cfg(target_os = "macos")]
fn rename_no_replace(
    parent: &File,
    source: &OsStr,
    destination: &OsStr,
) -> Result<(), std::io::Error> {
    rustix::fs::renameat_with(
        parent,
        source,
        parent,
        destination,
        rustix::fs::RenameFlags::NOREPLACE,
    )
    .map_err(std::io::Error::from)
}

#[cfg(all(
    unix,
    not(any(target_os = "macos", all(target_os = "linux", target_env = "gnu")))
))]
fn rename_no_replace(
    _parent: &File,
    _source: &OsStr,
    _destination: &OsStr,
) -> Result<(), std::io::Error> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "atomic reset quarantine is unsupported on this target",
    ))
}

#[cfg(unix)]
fn open_private_directory(path: &Path, stage: ResetStage) -> Result<Option<File>, ResetError> {
    let directory = match open(
        path,
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    ) {
        Ok(directory) => File::from(directory),
        Err(nix::errno::Errno::ENOENT) => return Ok(None),
        Err(source) => {
            return Err(ResetError::filesystem(stage, std::io::Error::from(source)));
        }
    };
    validate_private_directory(&directory, stage)?;
    Ok(Some(directory))
}

#[cfg(unix)]
fn validate_private_directory(directory: &File, stage: ResetStage) -> Result<(), ResetError> {
    let metadata = directory
        .metadata()
        .map_err(|source| ResetError::filesystem(stage, source))?;
    if !metadata.is_dir()
        || metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(ResetError::unsafe_artifact(
            stage,
            "reset artifact parent is not owner-private",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn sync_directory(directory: &File) -> Result<(), std::io::Error> {
    directory.sync_all()
}

#[cfg(unix)]
fn discover_stale_loopback_temporaries(
    product_home: &Path,
) -> Result<Vec<ResetArtifact>, ResetError> {
    let Some(directory) = open_private_directory(product_home, ResetStage::DiscoverArtifacts)?
    else {
        return Ok(Vec::new());
    };
    let clone = directory
        .try_clone()
        .map_err(|source| ResetError::filesystem(ResetStage::DiscoverArtifacts, source))?;
    let mut entries = Dir::from_fd(clone.into()).map_err(|source| {
        ResetError::filesystem(ResetStage::DiscoverArtifacts, std::io::Error::from(source))
    })?;
    let mut artifacts = Vec::new();
    let anchored_parent = Arc::new(directory);
    for entry in entries.iter() {
        let entry = entry.map_err(|source| {
            ResetError::filesystem(ResetStage::DiscoverArtifacts, std::io::Error::from(source))
        })?;
        let name = OsStr::from_bytes(entry.file_name().to_bytes());
        if is_loopback_temporary_name(name) {
            artifacts.push(ResetArtifact {
                path: product_home.join(name),
                group: ResetArtifactGroup::Connection,
                policy: ResetArtifactPolicy::CredentialTemporary,
                anchored: Some(AnchoredResetArtifact {
                    parent: Arc::clone(&anchored_parent),
                    name: name.to_os_string(),
                }),
            });
        }
    }
    artifacts.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(artifacts)
}

#[cfg(unix)]
fn discover_private_state_files(
    directory_path: &Path,
    group: ResetArtifactGroup,
) -> Result<Vec<ResetArtifact>, ResetError> {
    let Some(directory) = open_private_directory(directory_path, ResetStage::DiscoverArtifacts)?
    else {
        return Ok(Vec::new());
    };
    let clone = directory
        .try_clone()
        .map_err(|source| ResetError::filesystem(ResetStage::DiscoverArtifacts, source))?;
    let mut entries = Dir::from_fd(clone.into()).map_err(|source| {
        ResetError::filesystem(ResetStage::DiscoverArtifacts, std::io::Error::from(source))
    })?;
    let anchored_parent = Arc::new(directory);
    let mut artifacts = Vec::new();
    for entry in entries.iter() {
        let entry = entry.map_err(|source| {
            ResetError::filesystem(ResetStage::DiscoverArtifacts, std::io::Error::from(source))
        })?;
        let name = OsStr::from_bytes(entry.file_name().to_bytes());
        if name.as_bytes() == b"." || name.as_bytes() == b".." {
            continue;
        }
        artifacts.push(ResetArtifact {
            path: directory_path.join(name),
            group,
            policy: ResetArtifactPolicy::General,
            anchored: Some(AnchoredResetArtifact {
                parent: Arc::clone(&anchored_parent),
                name: name.to_os_string(),
            }),
        });
    }
    artifacts.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(artifacts)
}

#[cfg(unix)]
fn is_loopback_temporary_name(name: &OsStr) -> bool {
    let bytes = name.as_bytes();
    let Some(suffix) = bytes.strip_prefix(LOOPBACK_TEMPORARY_PREFIX) else {
        return false;
    };
    suffix.len() == LOOPBACK_TEMPORARY_SUFFIX_BYTES
        && suffix
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
}

#[cfg(all(test, unix))]
mod tests {
    #[cfg(unix)]
    use std::os::unix::net::UnixListener;
    #[cfg(unix)]
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    struct Lifecycle(AtomicBool);

    impl ResetLifecyclePort for Lifecycle {
        fn stop_daemon(&self) -> Result<DaemonStopState, CliError> {
            self.0.store(true, Ordering::Release);
            Ok(DaemonStopState::Stopped)
        }
    }

    struct State<'a>(&'a AtomicBool);

    impl ResetStatePort for State<'_> {
        fn clear(&self) -> Result<ResetStateOutcome, ResetError> {
            assert!(self.0.load(Ordering::Acquire));
            Ok(ResetStateOutcome::default())
        }
    }

    #[test]
    fn daemon_stops_before_state_removal() {
        let stopped = AtomicBool::new(false);
        let lifecycle = Lifecycle(stopped);
        let state = State(&lifecycle.0);
        let outcome = ResetService::new(&lifecycle, &state)
            .execute()
            .expect("reset");
        assert_eq!(outcome.daemon, DaemonStopState::Stopped);
    }

    #[test]
    fn native_reset_removes_only_exact_regular_files() {
        let root = std::env::temp_dir().join(format!(
            "undercurrent-cli-reset-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::create_dir_all(&root).expect("root");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).expect("private root");
        let database = root.join("state.sqlite3");
        let log = root.join("daemon.log");
        let token = root.join("websocket.token");
        let unrelated = root.join("keep");
        for path in [&database, &log, &token, &unrelated] {
            fs::write(path, b"x").expect("fixture");
        }
        let reset =
            NativeResetStatePort::from_paths(&root.join("daemon.lock"), &root, &database, &log, 1)
                .expect("plan");
        let outcome = reset.clear().expect("clear");
        assert_eq!(outcome.removed.persistence_files, 1);
        assert_eq!(outcome.removed.log_files, 1);
        assert_eq!(outcome.removed.connection_files, 0);
        assert!(token.exists(), "operator credential is preserved");
        assert!(unrelated.exists());
        fs::remove_file(unrelated).expect("unrelated cleanup");
        fs::remove_dir_all(root).expect("root cleanup");
    }

    #[test]
    fn native_reset_refuses_a_live_session_host_before_removing_state() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        fs::set_permissions(temporary.path(), fs::Permissions::from_mode(0o700))
            .expect("private directory");
        let database = temporary.path().join("database");
        fs::write(&database, b"state").expect("database");
        fs::create_dir_all(temporary.path().join("run")).unwrap();
        fs::set_permissions(
            temporary.path().join("run"),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        let socket = temporary.path().join("run").join(SESSION_HOST_SOCKET_FILE);
        let listener = UnixListener::bind(&socket).expect("live session host");
        listener
            .set_nonblocking(true)
            .expect("bounded fixture accept");
        let receipt_directory = temporary
            .path()
            .join("state/session-host/session-host")
            .with_extension(SESSION_HOST_RECEIPT_EXTENSION);
        fs::create_dir_all(&receipt_directory).expect("receipt directory");
        fs::set_permissions(&receipt_directory, fs::Permissions::from_mode(0o700))
            .expect("private receipt directory");
        let receipt = receipt_directory.join("receipts.json");
        fs::write(&receipt, b"receipt").expect("receipt");
        let reset = NativeResetStatePort::from_paths(
            &temporary.path().join("daemon.lock"),
            temporary.path(),
            &database,
            &temporary.path().join("daemon.log"),
            1,
        )
        .expect("reset plan");

        let error = reset.clear().expect_err("live host must block reset");

        assert_eq!(error.kind(), ResetErrorKind::Busy);
        assert_eq!(error.stage(), ResetStage::VerifyProcessHost);
        assert_eq!(error.mutation(), ResetMutationState::Reversible);
        assert!(database.exists());
        assert!(receipt.exists());
        let (mut probe, _) = listener.accept().expect("one private liveness probe");
        probe.set_nonblocking(true).expect("bounded fixture read");
        let mut byte = [0_u8; 1];
        assert_eq!(probe.read(&mut byte).expect("closed probe"), 0);
        assert!(
            matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
        );
    }

    #[test]
    fn native_reset_removes_database_and_coupled_host_journals_together() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        fs::set_permissions(temporary.path(), fs::Permissions::from_mode(0o700))
            .expect("private directory");
        let database = temporary.path().join("database");
        fs::write(&database, b"state").expect("database");
        fs::create_dir_all(temporary.path().join("run")).unwrap();
        fs::set_permissions(
            temporary.path().join("run"),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        let socket = temporary.path().join("run").join(SESSION_HOST_SOCKET_FILE);
        let listener = UnixListener::bind(&socket).expect("stale session-host socket");
        drop(listener);
        let authentication = socket.with_extension(SESSION_HOST_AUTH_EXTENSION);
        fs::write(&authentication, b"secret").expect("authentication");
        fs::set_permissions(&authentication, fs::Permissions::from_mode(0o600))
            .expect("private authentication");
        let receipt_directory = temporary
            .path()
            .join("state/session-host/session-host")
            .with_extension(SESSION_HOST_RECEIPT_EXTENSION);
        let execution_directory = temporary
            .path()
            .join("state/session-host/session-host")
            .with_extension(SESSION_HOST_EXECUTION_EXTENSION);
        let output_directory = temporary
            .path()
            .join("state")
            .join(EXECUTION_OUTPUT_DIRECTORY);
        for directory in [&receipt_directory, &execution_directory, &output_directory] {
            fs::create_dir_all(directory).expect("host state directory");
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700))
                .expect("private host state directory");
        }
        let receipt = receipt_directory.join("receipts.json");
        let job = execution_directory.join("job-test.json");
        let output = output_directory.join("digest");
        fs::write(&receipt, b"receipt").expect("receipt");
        fs::write(&job, b"job").expect("job");
        fs::write(&output, b"output").expect("output");
        let unrelated = temporary.path().join("keep");
        fs::write(&unrelated, b"keep").expect("unrelated");
        let reset = NativeResetStatePort::from_paths(
            &temporary.path().join("daemon.lock"),
            temporary.path(),
            &database,
            &temporary.path().join("daemon.log"),
            1,
        )
        .expect("reset plan");

        let outcome = reset.clear().expect("consistent reset");

        assert_eq!(outcome.removed.persistence_files, 4);
        assert_eq!(outcome.removed.connection_files, 1);
        assert!(!database.exists());
        assert!(!authentication.exists());
        assert!(!receipt.exists());
        assert!(!job.exists());
        assert!(!output.exists());
        assert!(unrelated.exists());
    }

    #[test]
    fn quarantine_preserves_a_replacement_created_at_the_original_name() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        fs::set_permissions(temporary.path(), fs::Permissions::from_mode(0o700))
            .expect("private directory");
        let path = temporary.path().join("state");
        fs::write(&path, b"captured").expect("captured file");
        let mut quarantined =
            quarantine_artifact(artifact(path.clone(), ResetArtifactGroup::Persistence))
                .expect("quarantine")
                .expect("present artifact");
        fs::write(&path, b"replacement").expect("replacement");
        remove_quarantined(&mut quarantined, sync_directory).expect("delete captured inode");
        assert_eq!(
            fs::read(path).expect("preserved replacement"),
            b"replacement"
        );
    }

    #[test]
    fn removal_quarantine_preserves_an_entry_swapped_after_validation() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        fs::set_permissions(temporary.path(), fs::Permissions::from_mode(0o700))
            .expect("private directory");
        let path = temporary.path().join("state");
        fs::write(&path, b"captured").expect("captured file");
        let mut quarantined = quarantine_artifact(artifact(path, ResetArtifactGroup::Persistence))
            .expect("quarantine")
            .expect("present artifact");
        let preserved = temporary.path().join("preserved-captured");
        fs::rename(
            temporary.path().join(&quarantined.quarantine_name),
            &preserved,
        )
        .expect("move captured inode");
        fs::write(
            temporary.path().join(&quarantined.quarantine_name),
            b"replacement",
        )
        .expect("swapped entry");

        let error = remove_quarantined(&mut quarantined, sync_directory)
            .expect_err("swapped quarantine entry must fail closed");

        assert_eq!(error.kind(), ResetErrorKind::UnsafeArtifact);
        assert_eq!(fs::read(preserved).expect("captured inode"), b"captured");
        assert_eq!(
            fs::read(temporary.path().join(&quarantined.quarantine_name))
                .expect("preserved replacement"),
            b"replacement"
        );
    }

    #[test]
    fn reset_metadata_rejects_a_foreign_owner() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let path = temporary.path().join("artifact");
        fs::write(&path, b"owned").expect("artifact");
        let metadata = fs::metadata(path).expect("metadata");
        let actual = nix::unistd::geteuid().as_raw();
        let foreign = if actual == u32::MAX {
            actual - 1
        } else {
            actual + 1
        };
        assert!(validate_reset_metadata(&metadata, foreign, ResetArtifactPolicy::General).is_err());
    }

    #[test]
    fn reset_removes_and_accounts_for_a_stale_credential_temporary() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        fs::set_permissions(temporary.path(), fs::Permissions::from_mode(0o700))
            .expect("private directory");
        fs::create_dir(temporary.path().join("run")).unwrap();
        fs::set_permissions(
            temporary.path().join("run"),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        let stale = temporary
            .path()
            .join("run")
            .join(".websocket.token.create-0123456789abcdef0123456789abcdef");
        fs::write(&stale, b"partial-secret").expect("stale credential");
        fs::set_permissions(&stale, fs::Permissions::from_mode(0o600)).expect("credential mode");
        let reset = NativeResetStatePort::from_paths(
            &temporary.path().join("daemon.lock"),
            temporary.path(),
            &temporary.path().join("database"),
            &temporary.path().join("daemon.log"),
            1,
        )
        .expect("reset plan");
        let outcome = reset.clear().expect("reset stale credential");
        assert_eq!(outcome.removed.connection_files, 1);
        assert!(!stale.exists());
    }

    #[test]
    fn discovered_stale_temporary_keeps_its_original_parent_descriptor() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let product_home = temporary.path().join("product");
        fs::create_dir(&product_home).expect("product home");
        fs::set_permissions(&product_home, fs::Permissions::from_mode(0o700))
            .expect("private product home");
        let name = ".websocket.token.create-0123456789abcdef0123456789abcdef";
        let stale = product_home.join(name);
        fs::write(&stale, b"captured").expect("captured stale temporary");
        fs::set_permissions(&stale, fs::Permissions::from_mode(0o600)).expect("captured mode");
        let mut discovered =
            discover_stale_loopback_temporaries(&product_home).expect("discover stale temporary");
        let anchored_home = temporary.path().join("anchored-product");
        fs::rename(&product_home, &anchored_home).expect("move discovered parent");
        fs::create_dir(&product_home).expect("replacement product home");
        fs::set_permissions(&product_home, fs::Permissions::from_mode(0o700))
            .expect("replacement permissions");
        fs::write(product_home.join(name), b"replacement").expect("replacement stale name");

        let mut quarantined = quarantine_artifact(discovered.pop().expect("discovered artifact"))
            .expect("anchored quarantine")
            .expect("anchored stale entry");
        remove_quarantined(&mut quarantined, sync_directory).expect("anchored removal");

        assert!(!anchored_home.join(name).exists());
        assert_eq!(
            fs::read(product_home.join(name)).expect("preserved replacement"),
            b"replacement"
        );
    }

    #[test]
    fn private_removal_never_unlinks_post_detach_replacements() {
        #[cfg(unix)]
        use nix::unistd::mkfifo;

        for kind in ["regular", "symlink", "hardlink", "fifo"] {
            let temporary = tempfile::tempdir().expect("temporary directory");
            fs::set_permissions(temporary.path(), fs::Permissions::from_mode(0o700))
                .expect("private directory");
            let path = temporary.path().join("state");
            fs::write(&path, b"captured").expect("captured file");
            let mut quarantined =
                quarantine_artifact(artifact(path, ResetArtifactGroup::Persistence))
                    .expect("quarantine")
                    .expect("captured artifact");
            let target = temporary.path().join("replacement-target");
            fs::write(&target, b"replacement").expect("replacement target");
            let replacement_name = quarantined.quarantine_name.clone();
            remove_quarantined_with_barrier(
                &mut quarantined,
                sync_directory,
                |_, detached_name| {
                    assert_eq!(detached_name, replacement_name.as_os_str());
                    let replacement = temporary.path().join(detached_name);
                    match kind {
                        "regular" => fs::write(&replacement, b"replacement")?,
                        "symlink" => std::os::unix::fs::symlink(&target, &replacement)?,
                        "hardlink" => fs::hard_link(&target, &replacement)?,
                        "fifo" => mkfifo(&replacement, Mode::from_bits_truncate(0o600))
                            .map_err(std::io::Error::from)?,
                        _ => unreachable!(),
                    }
                    Ok(())
                },
            )
            .expect("remove privately owned captured inode");
            assert!(fs::symlink_metadata(temporary.path().join(replacement_name)).is_ok());
        }
    }

    #[test]
    fn sync_failure_reports_durability_unknown_counts_and_original_source() {
        fn fail_sync(_: &File) -> Result<(), std::io::Error> {
            Err(std::io::Error::other("injected directory sync failure"))
        }

        let temporary = tempfile::tempdir().expect("temporary directory");
        fs::set_permissions(temporary.path(), fs::Permissions::from_mode(0o700))
            .expect("private directory");
        let database = temporary.path().join("database");
        fs::write(&database, b"state").expect("database");
        let mut reset = NativeResetStatePort::from_paths(
            &temporary.path().join("daemon.lock"),
            temporary.path(),
            &database,
            &temporary.path().join("daemon.log"),
            1,
        )
        .expect("reset plan");
        reset.sync_parent = fail_sync;

        let error = reset.clear().expect_err("sync failure must be reported");

        assert_eq!(error.mutation(), ResetMutationState::DurabilityUnknown);
        assert_eq!(error.removed().persistence_files, 1);
        assert_eq!(error.durability_unknown().persistence_files, 1);
        assert!(error.rollback_complete());
        assert_eq!(
            error.source().expect("original source").to_string(),
            "injected directory sync failure"
        );
        assert!(!database.exists());
    }
}
