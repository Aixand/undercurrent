// SPDX-License-Identifier: Apache-2.0

//! Bounded local daemon process-lifecycle use cases and native adapters.

mod process_launch;
#[cfg(windows)]
mod windows_instance;
#[cfg(windows)]
pub(crate) use windows_instance::{
    AvailableDaemonInstanceGuard, acquire_available_daemon_instance_lock,
    local_daemon_instance_state,
};

#[cfg(unix)]
use std::ffi::OsStr;
use std::ffi::OsString;
#[cfg(unix)]
use std::fs::{self, DirBuilder, File, OpenOptions, TryLockError};
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use crate::cli_support::CommonExitCode;
#[cfg(unix)]
use nix::fcntl::{OFlag, openat};
#[cfg(unix)]
use nix::sys::stat::Mode;
use serde::Serialize;
use undercurrent_protocol::{DaemonStatusDto, ShutdownAcceptedDto};
use undercurrent_sdk::ProductClient;

use self::process_launch::isolate_background_process;
use crate::{
    CliDaemonLifecycleError, CliError, CliExecutionLimits, CliPresenter, CliProcessOperation,
    CliProductRequestPort, DaemonAction, FiniteCliExecutor, NativeProductClientFactory,
    ResolvedCliConfig, daemon_restart_text, daemon_start_text, daemon_status_text,
    daemon_stop_text,
};

const INSTANCE_PROBE_INTERVAL: Duration = Duration::from_millis(25);
const DAEMON_READINESS_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(unix)]
const INSTANCE_LOCK_FILE_MODE: u32 = 0o600;
#[cfg(unix)]
const INSTANCE_DIRECTORY_MODE: u32 = 0o700;
const INSTANCE_LOCK_SUFFIX: &str = ".instance.lock";

trait LifecycleClock: Send + Sync {
    fn now(&self) -> Instant;
    fn sleep(&self, duration: Duration);
}

struct SystemLifecycleClock;

impl LifecycleClock for SystemLifecycleClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep(&self, duration: Duration) {
        thread::sleep(duration);
    }
}

static SYSTEM_LIFECYCLE_CLOCK: SystemLifecycleClock = SystemLifecycleClock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocalDaemonInstanceState {
    Available,
    Owned,
}

#[cfg(unix)]
pub(crate) struct AvailableDaemonInstanceGuard {
    file: File,
    parent: File,
    parent_path: OsString,
    lock_name: OsString,
}

#[cfg(unix)]
impl AvailableDaemonInstanceGuard {
    pub(crate) fn revalidate(&self) -> Result<(), std::io::Error> {
        revalidate_instance_namespace(
            Path::new(&self.parent_path),
            &self.parent,
            &self.lock_name,
            &self.file,
        )
    }
}

#[cfg(unix)]
impl Drop for AvailableDaemonInstanceGuard {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

/// Connected daemon capability required by process-lifecycle orchestration.
pub trait DaemonLifecycleClient: Send + Sync {
    /// Returns the finite typed request boundary for status and shutdown use cases.
    fn request_port(&self) -> &dyn CliProductRequestPort;

    /// Closes the shared physical client before waiting for daemon drain.
    fn close(&self);
}

impl DaemonLifecycleClient for ProductClient {
    fn request_port(&self) -> &dyn CliProductRequestPort {
        self
    }

    fn close(&self) {
        ProductClient::close(self);
    }
}

/// Outer daemon process and connection boundary consumed by lifecycle use cases.
pub trait DaemonLifecyclePort: Send + Sync {
    /// Connects to an existing UCMP-0.1.0-beta daemon without process creation.
    ///
    /// `None` means no daemon owns a reachable local listener. Protocol,
    /// permission, and runtime failures must remain errors.
    ///
    /// # Errors
    ///
    /// Returns typed connection failure other than an absent listener.
    fn connect_existing(&self) -> Result<Option<Box<dyn DaemonLifecycleClient>>, CliError>;

    /// Connects to an existing daemon under one externally owned lifecycle budget.
    ///
    /// Native adapters override this boundary with a single physical attempt.
    /// Test and alternate adapters retain the compatibility behavior unless
    /// they need to model budget consumption explicitly.
    fn connect_existing_with_timeout(
        &self,
        _timeout: Duration,
    ) -> Result<Option<Box<dyn DaemonLifecycleClient>>, CliError> {
        self.connect_existing()
    }

    /// Starts one configured process and connects after bounded readiness.
    ///
    /// # Errors
    ///
    /// Returns typed process or connection failure.
    fn launch_and_connect(
        &self,
        timeout: Duration,
    ) -> Result<Box<dyn DaemonLifecycleClient>, CliError>;

    /// Waits for release of the process-lifetime instance lock.
    ///
    /// Returns `false` when the supplied bound elapses. This method must not
    /// infer shutdown only from socket disappearance.
    ///
    /// # Errors
    ///
    /// Returns typed instance-probe failure.
    fn wait_for_release(&self, timeout: Duration) -> Result<bool, CliError>;

    /// Observes either authoritative release or an authenticated reachable
    /// replacement while preserving the supplied total timeout bound.
    fn wait_for_replacement(
        &self,
        timeout: Duration,
    ) -> Result<DaemonReplacementObservation, CliError> {
        self.wait_for_release(timeout).map(|released| {
            if released {
                DaemonReplacementObservation::Released
            } else {
                DaemonReplacementObservation::TimedOut
            }
        })
    }
}

/// Bounded observation made after one accepted restart shutdown.
pub enum DaemonReplacementObservation {
    /// The prior process released its authoritative local lock or the remote
    /// endpoint explicitly refused/reset a new connection.
    Released,
    /// An authenticated endpoint is reachable and must be checked for epoch
    /// rotation before any other replacement action.
    Reachable(Box<dyn DaemonLifecycleClient>),
    /// No release or authenticated replacement was observed within the bound.
    TimedOut,
}

/// Native lifecycle adapter bound to one resolved configuration and client factory.
pub struct NativeDaemonLifecyclePort<'a> {
    factory: &'a NativeProductClientFactory,
    config: &'a ResolvedCliConfig,
}

impl<'a> NativeDaemonLifecyclePort<'a> {
    /// Binds process and socket IO to one side-effect-free configuration resolution.
    ///
    /// This function performs no IO and does not panic.
    pub const fn new(
        factory: &'a NativeProductClientFactory,
        config: &'a ResolvedCliConfig,
    ) -> Self {
        Self { factory, config }
    }
}

impl DaemonLifecyclePort for NativeDaemonLifecyclePort<'_> {
    fn connect_existing(&self) -> Result<Option<Box<dyn DaemonLifecycleClient>>, CliError> {
        self.connect_existing_with_timeout(self.config.startup_timeout())
    }

    fn connect_existing_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<Box<dyn DaemonLifecycleClient>>, CliError> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(Instant::now);

        let state = local_daemon_instance_state(self.config.instance_lock())
            .map_err(instance_probe_error)?;
        let remaining = native_lifecycle_remaining(deadline)?;
        match state {
            LocalDaemonInstanceState::Available => Ok(None),
            LocalDaemonInstanceState::Owned => {
                let result = self
                    .factory
                    .connect_existing_resolved_with_timeout(self.config, remaining)?
                    .map(|client| Box::new(client) as Box<dyn DaemonLifecycleClient>)
                    .map(Some)
                    .ok_or_else(owned_daemon_unreachable);
                reject_late_native_connection(deadline, result?)
            }
        }
    }

    fn launch_and_connect(
        &self,
        timeout: Duration,
    ) -> Result<Box<dyn DaemonLifecycleClient>, CliError> {
        launch_and_connect(self.factory, self.config, timeout)
            .map(|client| Box::new(client) as Box<dyn DaemonLifecycleClient>)
    }

    fn wait_for_release(&self, timeout: Duration) -> Result<bool, CliError> {
        wait_for_local_instance_release(timeout, || {
            local_daemon_instance_state(self.config.instance_lock()).map_err(instance_probe_error)
        })
    }
    fn wait_for_replacement(
        &self,
        timeout: Duration,
    ) -> Result<DaemonReplacementObservation, CliError> {
        self.wait_for_release(timeout).map(|released| {
            if released {
                DaemonReplacementObservation::Released
            } else {
                DaemonReplacementObservation::TimedOut
            }
        })
    }
}

fn wait_for_local_instance_release(
    timeout: Duration,
    state: impl FnMut() -> Result<LocalDaemonInstanceState, CliError>,
) -> Result<bool, CliError> {
    wait_for_local_instance_release_with_clock(&SYSTEM_LIFECYCLE_CLOCK, timeout, state)
}

fn wait_for_local_instance_release_with_clock(
    clock: &dyn LifecycleClock,
    timeout: Duration,
    mut state: impl FnMut() -> Result<LocalDaemonInstanceState, CliError>,
) -> Result<bool, CliError> {
    let deadline = clock
        .now()
        .checked_add(timeout)
        .unwrap_or_else(|| clock.now());
    loop {
        let remaining = deadline.saturating_duration_since(clock.now());
        if remaining.is_zero() {
            return Ok(false);
        }
        let state = state()?;
        let remaining = deadline.saturating_duration_since(clock.now());
        if remaining.is_zero() {
            return Ok(false);
        }
        if state == LocalDaemonInstanceState::Available {
            return Ok(true);
        }
        clock.sleep(INSTANCE_PROBE_INTERVAL.min(remaining));
    }
}

#[cfg(unix)]
pub(crate) fn local_daemon_instance_state(
    scope: &Path,
) -> Result<LocalDaemonInstanceState, std::io::Error> {
    let parent = match validate_instance_parent(scope, false)? {
        Some(parent) => parent,
        None => return Ok(LocalDaemonInstanceState::Available),
    };
    let lock_name = instance_lock_name(scope)?;
    let file = match open_instance_file(&parent, &lock_name, false)? {
        Some(file) => file,
        None => return Ok(LocalDaemonInstanceState::Available),
    };
    validate_instance_file(&file)?;
    let state = match file.try_lock() {
        Ok(()) => {
            file.unlock()?;
            LocalDaemonInstanceState::Available
        }
        Err(TryLockError::WouldBlock) => LocalDaemonInstanceState::Owned,
        Err(TryLockError::Error(source)) => return Err(source),
    };
    revalidate_instance_namespace(
        scope
            .parent()
            .expect("validated absolute scope has a parent"),
        &parent,
        &lock_name,
        &file,
    )?;
    Ok(state)
}

#[cfg(unix)]
pub(crate) fn acquire_available_daemon_instance_lock(
    scope: &Path,
) -> Result<AvailableDaemonInstanceGuard, std::io::Error> {
    let parent = validate_instance_parent(scope, true)?.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "daemon instance-lock parent is unavailable",
        )
    })?;
    let lock_name = instance_lock_name(scope)?;
    let file = open_instance_file(&parent, &lock_name, true)?.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "daemon instance lock is unavailable",
        )
    })?;
    validate_instance_file(&file)?;
    match file.try_lock() {
        Ok(()) => {
            let parent_path = scope.parent().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "daemon instance scope has no parent",
                )
            })?;
            revalidate_instance_namespace(parent_path, &parent, &lock_name, &file)?;
            Ok(AvailableDaemonInstanceGuard {
                file,
                parent,
                parent_path: parent_path.as_os_str().to_os_string(),
                lock_name,
            })
        }
        Err(TryLockError::WouldBlock) => Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "daemon instance remains owned",
        )),
        Err(TryLockError::Error(source)) => Err(source),
    }
}

#[cfg(unix)]
// A held file lock protects its inode, not the pathname used by a later daemon.
// Check both the directory and named file so reset cannot rely on ownership of
// an unlinked or replaced lock. See
// [reset ownership](../../../docs/guides/daemon/cli-lifecycle.md#reset-known-runtime-artifacts).
fn revalidate_instance_namespace(
    parent_path: &Path,
    parent: &File,
    lock_name: &OsStr,
    file: &File,
) -> Result<(), std::io::Error> {
    let current_parent = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(parent_path)?;
    let held_parent = parent.metadata()?;
    let current_parent_metadata = current_parent.metadata()?;
    if held_parent.dev() != current_parent_metadata.dev()
        || held_parent.ino() != current_parent_metadata.ino()
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "daemon instance-lock parent changed while ownership was held",
        ));
    }
    validate_instance_file(file)?;
    let named = open_instance_file(parent, lock_name, false)?.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "daemon instance lock changed while ownership was held",
        )
    })?;
    validate_instance_file(&named)?;
    let held = file.metadata()?;
    let current = named.metadata()?;
    if held.dev() != current.dev() || held.ino() != current.ino() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "daemon instance lock changed while ownership was held",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn validate_instance_parent(scope: &Path, create: bool) -> Result<Option<File>, std::io::Error> {
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
        let mut builder = DirBuilder::new();
        builder.recursive(true).mode(INSTANCE_DIRECTORY_MODE);
        builder.create(parent)?;
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
    let metadata = directory.metadata()?;
    if !metadata.is_dir()
        || metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "daemon instance-lock parent is not owner-private",
        ));
    }
    Ok(Some(directory))
}

fn instance_lock_name(scope: &Path) -> Result<OsString, std::io::Error> {
    let file_name = scope.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "daemon instance scope has no file name",
        )
    })?;
    let mut name = OsString::from(".");
    name.push(file_name);
    name.push(INSTANCE_LOCK_SUFFIX);
    Ok(name)
}

#[cfg(unix)]
fn open_instance_file(
    parent: &File,
    name: &OsStr,
    create: bool,
) -> Result<Option<File>, std::io::Error> {
    let flags = OFlag::O_RDWR | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC | OFlag::O_NONBLOCK;
    match openat(parent, name, flags, Mode::empty()) {
        Ok(file) => Ok(Some(File::from(file))),
        Err(nix::errno::Errno::ENOENT) if create => {
            match openat(
                parent,
                name,
                flags | OFlag::O_CREAT | OFlag::O_EXCL,
                Mode::from_bits_truncate(INSTANCE_LOCK_FILE_MODE as _),
            ) {
                Ok(file) => {
                    let file = File::from(file);
                    file.set_permissions(fs::Permissions::from_mode(INSTANCE_LOCK_FILE_MODE))?;
                    Ok(Some(file))
                }
                Err(nix::errno::Errno::EEXIST) => openat(parent, name, flags, Mode::empty())
                    .map(File::from)
                    .map(Some)
                    .map_err(std::io::Error::from),
                Err(source) => Err(std::io::Error::from(source)),
            }
        }
        Err(nix::errno::Errno::ENOENT) => Ok(None),
        Err(source) => Err(std::io::Error::from(source)),
    }
}

#[cfg(unix)]
fn validate_instance_file(file: &File) -> Result<(), std::io::Error> {
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.nlink() != 1
        || metadata.permissions().mode() & 0o7777 != INSTANCE_LOCK_FILE_MODE
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "daemon instance lock is not one owner-only regular file",
        ));
    }
    Ok(())
}

fn instance_probe_error(source: std::io::Error) -> CliError {
    CliError::Process {
        operation: CliProcessOperation::ProbeDaemonInstance,
        source,
    }
}

fn native_lifecycle_remaining(deadline: Instant) -> Result<Duration, CliError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(lifecycle_deadline_elapsed())
    } else {
        Ok(remaining)
    }
}

fn reject_late_native_connection(
    deadline: Instant,
    client: Option<Box<dyn DaemonLifecycleClient>>,
) -> Result<Option<Box<dyn DaemonLifecycleClient>>, CliError> {
    if deadline.saturating_duration_since(Instant::now()).is_zero() {
        if let Some(client) = client {
            client.close();
        }
        Err(lifecycle_deadline_elapsed())
    } else {
        Ok(client)
    }
}

fn lifecycle_deadline_elapsed() -> CliError {
    instance_probe_error(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "daemon lifecycle deadline elapsed",
    ))
}

fn owned_daemon_unreachable() -> CliError {
    instance_probe_error(std::io::Error::new(
        std::io::ErrorKind::ConnectionRefused,
        "daemon owns the instance lock but is not authentically reachable",
    ))
}

// Reset retains this guard through state removal. All CLI launch paths use
// the same scope so a disconnected watcher cannot recreate state mid-reset.
pub(crate) fn acquire_startup_guard(
    config: &ResolvedCliConfig,
    deadline: Instant,
) -> Result<AvailableDaemonInstanceGuard, CliError> {
    let startup_scope = config.instance_lock().with_extension("startup");
    loop {
        let remaining = native_lifecycle_remaining(deadline)?;
        match acquire_available_daemon_instance_lock(&startup_scope) {
            Ok(guard) => return Ok(guard),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(INSTANCE_PROBE_INTERVAL.min(remaining));
            }
            Err(error) => return Err(instance_probe_error(error)),
        }
    }
}

/// Connects for an ordinary command, starting only an absent local daemon.
/// A separate startup lock lets concurrent CLI invocations share readiness.
pub(crate) fn connect_or_start(
    factory: &NativeProductClientFactory,
    config: &ResolvedCliConfig,
) -> Result<ProductClient, CliError> {
    let deadline = Instant::now() + config.startup_timeout();
    let _guard = acquire_startup_guard(config, deadline)?;
    let state =
        local_daemon_instance_state(config.instance_lock()).map_err(instance_probe_error)?;
    let remaining = native_lifecycle_remaining(deadline)?;
    // Probe even an unowned instance: endpoint permission and protocol errors
    // must not be mistaken for an absent daemon.
    if let Some(client) = factory.connect_existing_resolved_with_timeout(config, remaining)? {
        if let Err(error) = native_lifecycle_remaining(deadline) {
            client.close();
            return Err(error);
        }
        return Ok(client);
    }
    match state {
        LocalDaemonInstanceState::Available => {
            launch_and_connect(factory, config, native_lifecycle_remaining(deadline)?)
        }
        LocalDaemonInstanceState::Owned => Err(owned_daemon_unreachable()),
    }
}

fn launch_and_connect(
    factory: &NativeProductClientFactory,
    config: &ResolvedCliConfig,
    timeout: Duration,
) -> Result<ProductClient, CliError> {
    let mut daemon = launch_daemon(factory.explicit_config_path(), config.product_home())?;
    let deadline = SYSTEM_LIFECYCLE_CLOCK
        .now()
        .checked_add(timeout)
        .unwrap_or_else(|| SYSTEM_LIFECYCLE_CLOCK.now());
    wait_for_launched_daemon(
        &SYSTEM_LIFECYCLE_CLOCK,
        deadline,
        |remaining| factory.connect_existing_resolved_with_timeout(config, remaining),
        || launched_daemon_is_running(&mut daemon),
        |client| client.close(),
    )
}

fn wait_for_launched_daemon<T>(
    clock: &dyn LifecycleClock,
    deadline: Instant,
    mut connect: impl FnMut(Duration) -> Result<Option<T>, CliError>,
    mut process_is_running: impl FnMut() -> Result<bool, CliError>,
    mut reject_client: impl FnMut(T),
) -> Result<T, CliError> {
    loop {
        let remaining = deadline.saturating_duration_since(clock.now());
        if remaining.is_zero() {
            return Err(daemon_readiness_error(
                std::io::ErrorKind::TimedOut,
                "spawned daemon did not become ready within the bound",
            ));
        }
        if !process_is_running()? {
            return Err(daemon_readiness_error(
                std::io::ErrorKind::BrokenPipe,
                "spawned daemon exited before readiness",
            ));
        }
        let remaining = deadline.saturating_duration_since(clock.now());
        if remaining.is_zero() {
            return Err(daemon_readiness_error(
                std::io::ErrorKind::TimedOut,
                "spawned daemon did not become ready within the bound",
            ));
        }
        if let Some(client) = connect(remaining)? {
            if deadline.saturating_duration_since(clock.now()).is_zero() {
                reject_client(client);
                return Err(daemon_readiness_error(
                    std::io::ErrorKind::TimedOut,
                    "spawned daemon did not become ready within the bound",
                ));
            }
            if process_is_running()? {
                if deadline.saturating_duration_since(clock.now()).is_zero() {
                    reject_client(client);
                    return Err(daemon_readiness_error(
                        std::io::ErrorKind::TimedOut,
                        "spawned daemon did not become ready within the bound",
                    ));
                }
                return Ok(client);
            }
            reject_client(client);
            return Err(daemon_readiness_error(
                std::io::ErrorKind::BrokenPipe,
                "spawned daemon exited during readiness",
            ));
        }
        let remaining = deadline.saturating_duration_since(clock.now());
        if remaining.is_zero() {
            return Err(daemon_readiness_error(
                std::io::ErrorKind::TimedOut,
                "spawned daemon did not become ready within the bound",
            ));
        }
        clock.sleep(INSTANCE_PROBE_INTERVAL.min(remaining));
    }
}

fn launched_daemon_is_running(daemon: &mut Child) -> Result<bool, CliError> {
    daemon
        .try_wait()
        .map(|status| status.is_none())
        .map_err(|source| CliError::Process {
            operation: CliProcessOperation::SpawnDaemon,
            source,
        })
}

fn daemon_readiness_error(kind: std::io::ErrorKind, message: &'static str) -> CliError {
    CliError::Process {
        operation: CliProcessOperation::SpawnDaemon,
        source: std::io::Error::new(kind, message),
    }
}

fn launch_daemon(config_path: Option<&Path>, home: &Path) -> Result<Child, CliError> {
    #[cfg(windows)]
    undercurrent_platform_windows::prevent_standard_handle_inheritance().map_err(|source| {
        CliError::Process {
            operation: CliProcessOperation::SpawnDaemon,
            source,
        }
    })?;
    let current = std::env::current_exe().map_err(|source| CliError::Process {
        operation: CliProcessOperation::ResolveCurrentExecutable,
        source,
    })?;
    let mut command = daemon_command(config_path, &current);
    // A saved CLI profile selects this home without changing the caller's environment.
    command.env("UNDERCURRENT_HOME", home);
    isolate_background_process(&mut command);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|source| CliError::Process {
            operation: CliProcessOperation::SpawnDaemon,
            source,
        })
}

fn daemon_command(config_path: Option<&Path>, current_executable: &Path) -> Command {
    let sibling =
        current_executable.with_file_name(format!("uc-daemon{}", std::env::consts::EXE_SUFFIX));
    let program = if sibling.is_file() {
        sibling.into_os_string()
    } else {
        OsString::from(format!("uc-daemon{}", std::env::consts::EXE_SUFFIX))
    };
    let mut command = Command::new(program);
    if let Some(recorder) = undercurrent_diagnostics::process() {
        command.env(
            undercurrent_diagnostics::INCIDENT_ENV,
            recorder.incident_id(),
        );
    }
    if let Some(config) = config_path {
        command.arg("--config").arg(config);
    }
    command
}

/// Whether `daemon start` created a process or observed an existing one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonStartState {
    /// The configured daemon was already reachable.
    AlreadyRunning,
    /// This invocation requested process creation and observed readiness.
    Started,
}

/// Complete successful `daemon start` result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DaemonStartOutcome {
    /// Idempotent lifecycle result.
    pub state: DaemonStartState,
    /// Bounded state read from the reachable daemon.
    pub status: DaemonStatusDto,
}

/// Whether `daemon stop` changed process state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonStopState {
    /// No configured daemon was running.
    AlreadyStopped,
    /// Graceful shutdown completed and the instance lock was released.
    Stopped,
}

/// Complete successful `daemon stop` result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DaemonStopOutcome {
    /// Idempotent lifecycle result.
    pub state: DaemonStopState,
    /// Accepted drain metadata when a process was stopped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shutdown: Option<ShutdownAcceptedDto>,
}

/// Complete successful `daemon restart` result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DaemonRestartOutcome {
    /// Prior process epoch, absent when restart behaved as initial start.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_server_epoch: Option<String>,
    /// Bounded state read from the replacement daemon.
    pub status: DaemonStatusDto,
}

/// Process-level result of one successfully presented daemon lifecycle command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DaemonLifecycleCommandOutcome {
    /// Stable shared process exit category.
    pub exit_code: CommonExitCode,
}

/// Testable lifecycle coordinator over an injected process/connection port.
pub struct DaemonLifecycleService<'a> {
    startup_timeout: Duration,
    port: &'a dyn DaemonLifecyclePort,
    request_timeout: Duration,
    shutdown_wait_timeout: Duration,
    limits: CliExecutionLimits,
    clock: &'a dyn LifecycleClock,
}

impl<'a> DaemonLifecycleService<'a> {
    /// Selects the validated daemon readiness deadline.
    pub fn with_startup_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout = timeout;
        self
    }

    /// Binds lifecycle use cases to explicit request and shutdown bounds.
    ///
    /// Both durations are already validated by shared CLI and daemon
    /// configuration. This function performs no IO and does not panic.
    pub const fn new(
        port: &'a dyn DaemonLifecyclePort,
        request_timeout: Duration,
        shutdown_wait_timeout: Duration,
        limits: CliExecutionLimits,
    ) -> Self {
        Self {
            startup_timeout: DAEMON_READINESS_TIMEOUT,
            port,
            request_timeout,
            shutdown_wait_timeout,
            limits,
            clock: &SYSTEM_LIFECYCLE_CLOCK,
        }
    }

    #[cfg(test)]
    const fn with_clock(
        port: &'a dyn DaemonLifecyclePort,
        request_timeout: Duration,
        shutdown_wait_timeout: Duration,
        limits: CliExecutionLimits,
        clock: &'a dyn LifecycleClock,
    ) -> Self {
        Self {
            startup_timeout: DAEMON_READINESS_TIMEOUT,
            port,
            request_timeout,
            shutdown_wait_timeout,
            limits,
            clock,
        }
    }

    /// Ensures the configured daemon is running and returns its bounded status.
    ///
    /// Existing compatible processes are preserved. Process creation occurs
    /// only when the port proves the local listener absent. The connected
    /// client is closed after readiness is verified.
    ///
    /// # Errors
    ///
    /// Returns typed probe, launch, connection, status, or correlation failure.
    pub fn start(&self) -> Result<DaemonStartOutcome, CliError> {
        let deadline = self.lifecycle_deadline(self.startup_timeout);
        let (client, state) = match self.connect_existing_until(deadline)? {
            Some(client) => (client, DaemonStartState::AlreadyRunning),
            None => {
                let remaining = self.remaining(deadline)?;
                let client = self.port.launch_and_connect(remaining)?;
                (
                    self.accept_client_until(client, deadline)?,
                    DaemonStartState::Started,
                )
            }
        };
        let status = self.status_and_close_until(client, deadline)?;
        Ok(DaemonStartOutcome { state, status })
    }

    /// Reads bounded status without starting an absent daemon.
    ///
    /// # Errors
    ///
    /// Returns not-running, connection, status, or correlation failure.
    pub fn status(&self) -> Result<DaemonStatusDto, CliError> {
        let deadline = self.lifecycle_deadline(self.request_timeout);
        let client = self
            .connect_existing_until(deadline)?
            .ok_or(CliDaemonLifecycleError::NotRunning)?;
        self.status_and_close_until(client, deadline)
    }

    /// Idempotently stops the configured daemon and waits for lock release.
    ///
    /// Closing the command's client immediately after shutdown acceptance lets
    /// the daemon drain without retaining the requester as an active physical
    /// connection. This method performs bounded product, connection, and
    /// instance-probe IO and does not panic unless an injected port panics.
    ///
    /// # Errors
    ///
    /// Returns connection, shutdown, probe, or bounded-wait failure.
    pub fn stop(&self, drain_timeout: &str) -> Result<DaemonStopOutcome, CliError> {
        let deadline = self.lifecycle_deadline(self.shutdown_wait_timeout);
        let Some(client) = self.connect_existing_until(deadline)? else {
            return Ok(DaemonStopOutcome {
                state: DaemonStopState::AlreadyStopped,
                shutdown: None,
            });
        };
        let shutdown = self.shutdown_and_close_until(client, drain_timeout, deadline)?;
        self.require_release_until(deadline)?;
        Ok(DaemonStopOutcome {
            state: DaemonStopState::Stopped,
            shutdown: Some(shutdown),
        })
    }

    pub(crate) fn stop_for_reset(&self, timeout: &str) -> Result<DaemonStopOutcome, CliError> {
        self.stop_immediate(timeout, undercurrent_protocol::DaemonShutdownModeDto::Reset)
    }

    fn stop_immediate(
        &self,
        timeout: &str,
        mode: undercurrent_protocol::DaemonShutdownModeDto,
    ) -> Result<DaemonStopOutcome, CliError> {
        let deadline = self.lifecycle_deadline(self.request_timeout);
        let client = match self.connect_existing_until(deadline) {
            Ok(client) => client,
            Err(CliError::Process {
                operation: CliProcessOperation::ProbeDaemonInstance,
                source,
            }) if source.kind() == std::io::ErrorKind::ConnectionRefused => {
                // A prior reset can close its listener before releasing ownership.
                // Waiting is read-only; an owner that stays alive still blocks reset.
                self.require_release_until(deadline)?;
                return Ok(DaemonStopOutcome {
                    state: DaemonStopState::Stopped,
                    shutdown: None,
                });
            }
            Err(error) => return Err(error),
        };
        let Some(client) = client else {
            return Ok(DaemonStopOutcome {
                state: DaemonStopState::AlreadyStopped,
                shutdown: None,
            });
        };
        let result = FiniteCliExecutor::new(
            client.request_port(),
            self.remaining(deadline)?,
            self.limits,
        )
        .shutdown_daemon_with_mode(timeout, mode);
        client.close();
        let shutdown = match result {
            Ok(shutdown) => Some(shutdown),
            // Physical response loss does not prove that host retirement failed.
            // Lock release and the reset adapter's host probe still gate deletion.
            Err(CliError::Product(ref error))
                if matches!(
                    error.category(),
                    undercurrent_sdk::ProductClientErrorCategory::Unavailable
                        | undercurrent_sdk::ProductClientErrorCategory::Timeout
                ) =>
            {
                None
            }
            Err(error) => return Err(error),
        };
        self.require_release_until(deadline)?;
        Ok(DaemonStopOutcome {
            state: DaemonStopState::Stopped,
            shutdown,
        })
    }

    /// Gracefully replaces the configured daemon and verifies epoch rotation.
    ///
    /// When no daemon is running, restart behaves as a start. Otherwise the
    /// prior status is read, graceful shutdown completes, the authoritative
    /// instance lock is released, and only then is a replacement launched.
    /// This method performs bounded product, process, socket, and lock-probe IO.
    ///
    /// # Errors
    ///
    /// Returns typed status, shutdown, wait, launch, connection, or unchanged-
    /// epoch failure.
    pub fn restart(&self, drain_timeout: &str) -> Result<DaemonRestartOutcome, CliError> {
        self.restart_with_policy(drain_timeout, true)
    }

    fn restart_with_policy(
        &self,
        drain_timeout: &str,
        graceful: bool,
    ) -> Result<DaemonRestartOutcome, CliError> {
        let deadline = self.lifecycle_deadline(self.shutdown_wait_timeout);
        let previous_server_epoch = match self.connect_existing_until(deadline)? {
            Some(client) => {
                let status = match self.status_without_close_until(client.as_ref(), deadline) {
                    Ok(status) => status,
                    Err(error) => {
                        client.close();
                        return Err(error);
                    }
                };
                if graceful {
                    self.shutdown_and_close_until(client, drain_timeout, deadline)?;
                } else {
                    client.close();
                    self.stop_immediate(
                        drain_timeout,
                        undercurrent_protocol::DaemonShutdownModeDto::Immediate,
                    )?;
                }
                Some(status.server_epoch)
            }
            None => None,
        };
        let Some(previous) = previous_server_epoch.as_deref() else {
            let remaining = self.remaining(deadline)?;
            let client = self.port.launch_and_connect(remaining)?;
            let client = self.accept_client_until(client, deadline)?;
            let status = self.status_and_close_until(client, deadline)?;
            return Ok(DaemonRestartOutcome {
                previous_server_epoch,
                status,
            });
        };
        loop {
            let remaining = deadline.saturating_duration_since(self.clock.now());
            if remaining.is_zero() {
                return Err(CliDaemonLifecycleError::ReplacementEpochUnchanged.into());
            }
            let client = match self.port.wait_for_replacement(remaining)? {
                DaemonReplacementObservation::Released => {
                    let remaining = deadline.saturating_duration_since(self.clock.now());
                    if remaining.is_zero() {
                        return Err(CliDaemonLifecycleError::ShutdownTimedOut.into());
                    }
                    self.port.launch_and_connect(remaining)?
                }
                DaemonReplacementObservation::Reachable(client) => client,
                DaemonReplacementObservation::TimedOut => {
                    return Err(CliDaemonLifecycleError::ShutdownTimedOut.into());
                }
            };
            let remaining = deadline.saturating_duration_since(self.clock.now());
            if remaining.is_zero() {
                client.close();
                return Err(CliDaemonLifecycleError::ShutdownTimedOut.into());
            }
            let status = self.status_without_close_until(client.as_ref(), deadline);
            client.close();
            let status = status?;
            if deadline
                .saturating_duration_since(self.clock.now())
                .is_zero()
            {
                return Err(CliDaemonLifecycleError::ShutdownTimedOut.into());
            }
            if status.server_epoch != previous {
                return Ok(DaemonRestartOutcome {
                    previous_server_epoch,
                    status,
                });
            }
            let remaining = deadline.saturating_duration_since(self.clock.now());
            if remaining.is_zero() {
                return Err(CliDaemonLifecycleError::ReplacementEpochUnchanged.into());
            }
            self.clock.sleep(INSTANCE_PROBE_INTERVAL.min(remaining));
        }
    }

    fn lifecycle_deadline(&self, timeout: Duration) -> Instant {
        self.clock
            .now()
            .checked_add(timeout)
            .unwrap_or_else(|| self.clock.now())
    }

    fn remaining(&self, deadline: Instant) -> Result<Duration, CliError> {
        let remaining = deadline.saturating_duration_since(self.clock.now());
        if remaining.is_zero() {
            Err(lifecycle_deadline_elapsed())
        } else {
            Ok(remaining)
        }
    }

    fn connect_existing_until(
        &self,
        deadline: Instant,
    ) -> Result<Option<Box<dyn DaemonLifecycleClient>>, CliError> {
        let remaining = self.remaining(deadline)?;
        let client = self.port.connect_existing_with_timeout(remaining)?;
        if self.remaining(deadline).is_err() {
            if let Some(client) = client {
                client.close();
            }
            return Err(lifecycle_deadline_elapsed());
        }
        Ok(client)
    }

    fn accept_client_until(
        &self,
        client: Box<dyn DaemonLifecycleClient>,
        deadline: Instant,
    ) -> Result<Box<dyn DaemonLifecycleClient>, CliError> {
        if self.remaining(deadline).is_err() {
            client.close();
            Err(lifecycle_deadline_elapsed())
        } else {
            Ok(client)
        }
    }

    fn status_and_close_until(
        &self,
        client: Box<dyn DaemonLifecycleClient>,
        deadline: Instant,
    ) -> Result<DaemonStatusDto, CliError> {
        let result = self.status_without_close_until(client.as_ref(), deadline);
        client.close();
        result
    }

    fn status_without_close_until(
        &self,
        client: &dyn DaemonLifecycleClient,
        deadline: Instant,
    ) -> Result<DaemonStatusDto, CliError> {
        let timeout = self.request_timeout.min(self.remaining(deadline)?);
        let status =
            FiniteCliExecutor::new(client.request_port(), timeout, self.limits).daemon_status()?;
        self.remaining(deadline)?;
        Ok(status)
    }

    fn shutdown_and_close_until(
        &self,
        client: Box<dyn DaemonLifecycleClient>,
        drain_timeout: &str,
        deadline: Instant,
    ) -> Result<ShutdownAcceptedDto, CliError> {
        let remaining = match self.remaining(deadline) {
            Ok(remaining) => remaining,
            Err(error) => {
                client.close();
                return Err(error);
            }
        };
        let timeout = self.request_timeout.min(remaining);
        let result = FiniteCliExecutor::new(client.request_port(), timeout, self.limits)
            .shutdown_daemon(drain_timeout);
        client.close();
        let shutdown = result?;
        self.remaining(deadline)?;
        Ok(shutdown)
    }

    fn require_release_until(&self, deadline: Instant) -> Result<(), CliError> {
        let remaining = self.remaining(deadline)?;
        if self.port.wait_for_release(remaining)? {
            self.remaining(deadline)?;
            Ok(())
        } else {
            Err(CliDaemonLifecycleError::ShutdownTimedOut.into())
        }
    }
}

/// Presentation adapter for all native `uc daemon` subcommands.
pub struct DaemonLifecycleDispatcher<'a> {
    service: DaemonLifecycleService<'a>,
    presenter: CliPresenter,
}

impl<'a> DaemonLifecycleDispatcher<'a> {
    /// Binds lifecycle use cases to one finite presentation context.
    ///
    /// This function performs no IO and does not panic.
    pub const fn new(service: DaemonLifecycleService<'a>, presenter: CliPresenter) -> Self {
        Self { service, presenter }
    }

    /// Executes and presents one complete daemon lifecycle command.
    ///
    /// # Errors
    ///
    /// Returns typed lifecycle or finite-presentation failure.
    pub fn execute<W>(
        &self,
        action: &DaemonAction,
        writer: &mut W,
    ) -> Result<DaemonLifecycleCommandOutcome, CliError>
    where
        W: Write,
    {
        match action {
            DaemonAction::Start => {
                let outcome = self.service.start()?;
                self.presenter
                    .write_finite(writer, &outcome, |_| daemon_start_text(&outcome))?;
            }
            DaemonAction::Stop {
                drain_timeout,
                graceful,
            } => {
                let outcome = if *graceful {
                    self.service.stop(drain_timeout)?
                } else {
                    self.service.stop_immediate(
                        drain_timeout,
                        undercurrent_protocol::DaemonShutdownModeDto::Immediate,
                    )?
                };
                self.presenter
                    .write_finite(writer, &outcome, |_| daemon_stop_text(&outcome))?;
            }
            DaemonAction::Restart {
                drain_timeout,
                graceful,
            } => {
                let outcome = self.service.restart_with_policy(drain_timeout, *graceful)?;
                self.presenter
                    .write_finite(writer, &outcome, |_| daemon_restart_text(&outcome))?;
            }
            DaemonAction::Status => {
                let status = self.service.status()?;
                self.presenter
                    .write_finite(writer, &status, |_| daemon_status_text(&status))?;
            }
        }
        Ok(DaemonLifecycleCommandOutcome {
            exit_code: CommonExitCode::Success,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use tempfile::tempdir;
    use undercurrent_protocol::{ProductRequest, ProductSuccess};
    use undercurrent_sdk::ProductClientError;

    use super::*;

    #[test]
    fn sibling_daemon_binary_wins_over_path_lookup() {
        let temporary = tempdir().expect("temporary directory");
        let cli = temporary.path().join("uc");
        let daemon = temporary
            .path()
            .join(format!("uc-daemon{}", std::env::consts::EXE_SUFFIX));
        std::fs::write(&daemon, b"fixture").expect("daemon fixture");

        let command = daemon_command(Some(&PathBuf::from("/private/config.toml")), &cli);

        assert_eq!(command.get_program(), daemon.as_os_str());
        assert_eq!(
            command.get_args().collect::<Vec<_>>(),
            ["--config", "/private/config.toml"].map(std::ffi::OsStr::new)
        );
    }

    #[test]
    fn missing_sibling_uses_path_without_a_retired_socket_argument() {
        let temporary = tempdir().expect("temporary directory");

        let command = daemon_command(None, &temporary.path().join("uc"));

        assert_eq!(
            command.get_program(),
            std::ffi::OsStr::new(&format!("uc-daemon{}", std::env::consts::EXE_SUFFIX))
        );
        assert!(command.get_args().next().is_none());
    }

    #[test]
    fn persistent_token_does_not_bypass_connection_and_process_readiness() {
        let mut connection_probes = 0_u8;
        let mut process_probes = 0_u8;
        let clock = TestLifecycleClock::new();
        let connected = wait_for_launched_daemon(
            &clock,
            clock.now() + Duration::from_millis(100),
            |remaining| {
                assert!(!remaining.is_zero());
                connection_probes = connection_probes.saturating_add(1);
                Ok((connection_probes == 2).then_some(7_u8))
            },
            || {
                process_probes = process_probes.saturating_add(1);
                Ok(true)
            },
            |_| {},
        )
        .expect("authenticated connection readiness");

        assert_eq!(connected, 7);
        assert_eq!(connection_probes, 2);
        assert!(process_probes >= 3);
    }

    #[test]
    fn listener_absence_does_not_release_an_owned_instance_lock() {
        let temporary = tempdir().expect("temporary directory");
        #[cfg(unix)]
        std::fs::set_permissions(temporary.path(), std::fs::Permissions::from_mode(0o700))
            .expect("private parent");
        #[cfg(windows)]
        let parent = temporary.path().join("private");
        #[cfg(windows)]
        drop(
            undercurrent_platform_windows::prepare_private_directory(&parent)
                .expect("private parent"),
        );
        #[cfg(not(windows))]
        let parent = temporary.path().to_path_buf();
        let scope = parent.join("daemon.lock");
        let guard = acquire_available_daemon_instance_lock(&scope).expect("instance owner");
        assert_eq!(
            local_daemon_instance_state(&scope).expect("owned state"),
            LocalDaemonInstanceState::Owned
        );
        assert!(matches!(
            owned_daemon_unreachable(),
            CliError::Process {
                operation: CliProcessOperation::ProbeDaemonInstance,
                ..
            }
        ));
        drop(guard);
        assert_eq!(
            local_daemon_instance_state(&scope).expect("released state"),
            LocalDaemonInstanceState::Available
        );
    }

    #[test]
    fn release_wait_observes_lock_state_not_a_listener_hint() {
        let clock = TestLifecycleClock::new();
        let mut probes = 0_u8;
        let released =
            wait_for_local_instance_release_with_clock(&clock, Duration::from_millis(100), || {
                probes = probes.saturating_add(1);
                Ok(if probes < 3 {
                    LocalDaemonInstanceState::Owned
                } else {
                    LocalDaemonInstanceState::Available
                })
            })
            .expect("lock release");
        assert!(released);
        assert_eq!(probes, 3);
    }

    #[test]
    fn exited_spawned_process_cannot_be_misreported_as_ready() {
        let clock = TestLifecycleClock::new();
        let mut connection_called = false;
        let error = wait_for_launched_daemon::<u8>(
            &clock,
            clock.now() + Duration::from_millis(100),
            |_| {
                connection_called = true;
                Ok(Some(7))
            },
            || Ok(false),
            |_| {},
        )
        .expect_err("exited child must fail readiness");

        assert!(!connection_called);
        assert!(matches!(error, CliError::Process { .. }));
    }

    #[test]
    fn stalled_local_transport_phases_cannot_multiply_the_readiness_deadline() {
        for phase in ["tcp", "websocket", "uchs"] {
            let clock = TestLifecycleClock::new();
            let deadline = clock.now() + Duration::from_millis(100);
            let mut process_probes = 0_u8;
            let mut connect_probes = 0_u8;
            let error = wait_for_launched_daemon::<u8>(
                &clock,
                deadline,
                |remaining| {
                    assert!(!remaining.is_zero(), "{phase} received zero budget");
                    connect_probes = connect_probes.saturating_add(1);
                    clock.advance(remaining);
                    Ok(None)
                },
                || {
                    process_probes = process_probes.saturating_add(1);
                    Ok(true)
                },
                |_| panic!("an absent stalled connection cannot be rejected"),
            )
            .expect_err("stalled local transport must hit the outer deadline");

            assert!(matches!(error, CliError::Process { .. }));
            assert_eq!(connect_probes, 1, "{phase} retried inside the deadline");
            assert_eq!(process_probes, 1, "{phase} probed after expiry");
        }
    }

    #[test]
    fn local_connection_completing_at_expiry_is_closed_and_rejected() {
        let clock = TestLifecycleClock::new();
        let deadline = clock.now() + Duration::from_millis(100);
        let mut rejected = 0_u8;
        let error = wait_for_launched_daemon(
            &clock,
            deadline,
            |remaining| {
                clock.advance(remaining);
                Ok(Some(7_u8))
            },
            || Ok(true),
            |_| rejected = rejected.saturating_add(1),
        )
        .expect_err("late readiness must fail");

        assert!(matches!(error, CliError::Process { .. }));
        assert_eq!(rejected, 1);
    }

    #[test]
    fn zero_local_deadline_performs_no_process_state_or_connection_probe() {
        let clock = TestLifecycleClock::new();
        let mut process_probes = 0_u8;
        let mut connect_probes = 0_u8;
        let error = wait_for_launched_daemon::<u8>(
            &clock,
            clock.now(),
            |_| {
                connect_probes = connect_probes.saturating_add(1);
                Ok(None)
            },
            || {
                process_probes = process_probes.saturating_add(1);
                Ok(true)
            },
            |_| {},
        )
        .expect_err("zero readiness deadline must fail");
        let mut state_probes = 0_u8;
        let released = wait_for_local_instance_release_with_clock(&clock, Duration::ZERO, || {
            state_probes = state_probes.saturating_add(1);
            Ok(LocalDaemonInstanceState::Available)
        })
        .expect("zero release deadline");

        assert!(matches!(error, CliError::Process { .. }));
        assert!(!released);
        assert_eq!(process_probes, 0);
        assert_eq!(connect_probes, 0);
        assert_eq!(state_probes, 0);
    }

    #[test]
    fn local_connection_ready_before_deadline_is_accepted() {
        let clock = TestLifecycleClock::new();
        let deadline = clock.now() + Duration::from_millis(100);
        let mut process_probes = 0_u8;
        let mut rejected = 0_u8;
        let client = wait_for_launched_daemon(
            &clock,
            deadline,
            |remaining| {
                assert_eq!(remaining, Duration::from_millis(100));
                clock.advance(Duration::from_millis(10));
                Ok(Some(7_u8))
            },
            || {
                process_probes = process_probes.saturating_add(1);
                Ok(true)
            },
            |_| rejected = rejected.saturating_add(1),
        )
        .expect("ready local connection");

        assert_eq!(client, 7);
        assert_eq!(process_probes, 2);
        assert_eq!(rejected, 0);
    }

    #[test]
    fn start_is_idempotent_and_launches_only_when_absent() {
        let running = FakePort::new([Some(connection([status_success("old")]))], [], []);
        let running_service = service(&running);

        let outcome = running_service.start().expect("existing start");

        assert_eq!(outcome.state, DaemonStartState::AlreadyRunning);
        assert_eq!(outcome.status.server_epoch, "old");
        assert_eq!(running.launches.load(Ordering::Acquire), 0);
        assert_eq!(running.closes.load(Ordering::Acquire), 1);

        let absent = FakePort::new([None], [connection([status_success("new")])], []);

        let outcome = service(&absent).start().expect("absent start");

        assert_eq!(outcome.state, DaemonStartState::Started);
        assert_eq!(outcome.status.server_epoch, "new");
        assert_eq!(absent.launches.load(Ordering::Acquire), 1);
        assert_eq!(absent.closes.load(Ordering::Acquire), 1);
    }

    #[test]
    fn stop_is_idempotent_and_waits_after_closing_the_requester() {
        let absent = FakePort::new([None], [], []);
        assert_eq!(
            service(&absent).stop("5s").expect("absent stop").state,
            DaemonStopState::AlreadyStopped
        );
        assert!(absent.waits.lock().expect("wait lock").is_empty());

        let running = FakePort::new([Some(connection([shutdown_success(42)]))], [], [true]);

        let outcome = service(&running).stop("5s").expect("running stop");

        assert_eq!(outcome.state, DaemonStopState::Stopped);
        assert_eq!(
            outcome.shutdown.expect("shutdown").drain_deadline_millis,
            42
        );
        assert_eq!(running.closes.load(Ordering::Acquire), 1);
        assert_eq!(running.wait_calls.load(Ordering::Acquire), 1);
    }

    #[test]
    fn restart_waits_for_release_and_requires_a_new_epoch() {
        let port = FakePort::new(
            [Some(connection([
                status_success("old"),
                shutdown_success(42),
            ]))],
            [connection([status_success("new")])],
            [true],
        );

        let outcome = service(&port).restart("5s").expect("restart");

        assert_eq!(outcome.previous_server_epoch.as_deref(), Some("old"));
        assert_eq!(outcome.status.server_epoch, "new");
        assert_eq!(port.closes.load(Ordering::Acquire), 2);
        assert_eq!(port.wait_calls.load(Ordering::Acquire), 1);
        assert_eq!(port.launches.load(Ordering::Acquire), 1);

        let unchanged = FakePort::new(
            [Some(connection([
                status_success("same"),
                shutdown_success(42),
            ]))],
            [
                connection([status_success("same")]),
                connection([status_success("eventual-new")]),
            ],
            [true, true],
        );
        let outcome = service(&unchanged)
            .restart("5s")
            .expect("eventual epoch change");
        assert_eq!(outcome.status.server_epoch, "eventual-new");
        assert_eq!(unchanged.wait_calls.load(Ordering::Acquire), 2);
        assert_eq!(unchanged.launches.load(Ordering::Acquire), 2);
    }

    #[test]
    fn remote_restart_accepts_direct_authenticated_epoch_change_without_launch() {
        let clock = Arc::new(TestLifecycleClock::new());
        let port = DirectSupervisedReplacementPort {
            existing: Mutex::new(Some(connection([
                status_success("old"),
                shutdown_success(42),
            ]))),
            replacement: Mutex::new(Some(connection([status_success("new")]))),
            launches: AtomicUsize::new(0),
            probes: AtomicUsize::new(0),
        };

        let outcome = service_with_clock(
            &port,
            Duration::from_secs(1),
            Duration::from_secs(2),
            clock.as_ref(),
        )
        .restart("5s")
        .expect("direct remote replacement");

        assert_eq!(outcome.previous_server_epoch.as_deref(), Some("old"));
        assert_eq!(outcome.status.server_epoch, "new");
        assert_eq!(port.launches.load(Ordering::Acquire), 0);
        assert_eq!(port.probes.load(Ordering::Acquire), 1);
        assert_eq!(clock.sleeps.load(Ordering::Acquire), 0);
    }

    #[test]
    fn replacement_status_consuming_the_remaining_deadline_fails_closed() {
        let clock = Arc::new(TestLifecycleClock::new());
        let observed_timeouts = Arc::new(Mutex::new(Vec::new()));
        let port = DirectSupervisedReplacementPort {
            existing: Mutex::new(Some(connection([
                status_success("old"),
                shutdown_success(42),
            ]))),
            replacement: Mutex::new(Some(Box::new(DeadlineConsumingStatusClient {
                clock: clock.clone(),
                observed_timeouts: observed_timeouts.clone(),
            }))),
            launches: AtomicUsize::new(0),
            probes: AtomicUsize::new(0),
        };

        let error = service_with_clock(
            &port,
            Duration::from_secs(5),
            Duration::from_secs(2),
            clock.as_ref(),
        )
        .restart("5s")
        .expect_err("status completing at the deadline must not report success");

        assert!(matches!(
            error,
            CliError::Process {
                operation: CliProcessOperation::ProbeDaemonInstance,
                ref source,
            } if source.kind() == std::io::ErrorKind::TimedOut
        ));
        assert_eq!(
            observed_timeouts.lock().expect("timeout lock").as_slice(),
            &[Duration::from_secs(2)]
        );
        assert_eq!(port.probes.load(Ordering::Acquire), 1);
        assert_eq!(port.launches.load(Ordering::Acquire), 0);
    }

    #[test]
    fn initial_lifecycle_connect_consuming_deadline_is_closed_before_any_next_phase() {
        let clock = Arc::new(TestLifecycleClock::new());
        let closes = Arc::new(AtomicUsize::new(0));
        let port = DeadlineConsumingInitialConnectPort {
            clock: clock.clone(),
            client: Mutex::new(Some(counted_client(
                connection([status_success("late")]),
                closes.clone(),
            ))),
            observed_timeouts: Mutex::new(Vec::new()),
            launches: AtomicUsize::new(0),
        };

        let error = service_with_clock(
            &port,
            Duration::from_secs(1),
            Duration::from_secs(2),
            clock.as_ref(),
        )
        .start()
        .expect_err("late initial connection must fail");

        assert!(matches!(
            error,
            CliError::Process {
                operation: CliProcessOperation::ProbeDaemonInstance,
                ref source,
            } if source.kind() == std::io::ErrorKind::TimedOut
        ));
        assert_eq!(
            port.observed_timeouts
                .lock()
                .expect("connect timeout lock")
                .as_slice(),
            &[DAEMON_READINESS_TIMEOUT]
        );
        assert_eq!(closes.load(Ordering::Acquire), 1);
        assert_eq!(port.launches.load(Ordering::Acquire), 0);
    }

    #[test]
    fn restart_status_shutdown_and_replacement_share_one_outer_budget() {
        let clock = Arc::new(TestLifecycleClock::new());
        let observed_timeouts = Arc::new(Mutex::new(Vec::new()));
        let client: Box<dyn DaemonLifecycleClient> = Box::new(BudgetSharingClient {
            clock: clock.clone(),
            responses: Mutex::new(
                [status_success("old"), shutdown_success(42)]
                    .into_iter()
                    .collect(),
            ),
            advances: Mutex::new(
                [Duration::from_millis(750), Duration::from_millis(750)]
                    .into_iter()
                    .collect(),
            ),
            observed_timeouts: observed_timeouts.clone(),
        });
        let port = FakePort::new([Some(client)], [], [false]);

        let error = service_with_clock(
            &port,
            Duration::from_secs(1),
            Duration::from_secs(2),
            clock.as_ref(),
        )
        .restart("5s")
        .expect_err("remaining replacement budget is intentionally exhausted");

        assert!(matches!(
            error,
            CliError::DaemonLifecycle(CliDaemonLifecycleError::ShutdownTimedOut)
        ));
        assert_eq!(
            observed_timeouts
                .lock()
                .expect("request timeout lock")
                .as_slice(),
            &[Duration::from_secs(1), Duration::from_secs(1)]
        );
        assert_eq!(
            port.wait_timeouts
                .lock()
                .expect("wait timeout lock")
                .as_slice(),
            &[Duration::from_millis(500)]
        );
    }

    #[test]
    fn expired_replacement_deadline_performs_no_probe() {
        let clock = Arc::new(TestLifecycleClock::new());
        let port = DirectSupervisedReplacementPort {
            existing: Mutex::new(Some(connection([
                status_success("old"),
                shutdown_success(42),
            ]))),
            replacement: Mutex::new(None),
            launches: AtomicUsize::new(0),
            probes: AtomicUsize::new(0),
        };

        let error = service_with_clock(
            &port,
            Duration::from_secs(1),
            Duration::ZERO,
            clock.as_ref(),
        )
        .restart("5s")
        .expect_err("zero replacement budget must fail before probing");
        assert!(matches!(
            error,
            CliError::Process {
                operation: CliProcessOperation::ProbeDaemonInstance,
                ref source,
            } if source.kind() == std::io::ErrorKind::TimedOut
        ));
        assert_eq!(port.probes.load(Ordering::Acquire), 0);
        assert_eq!(port.launches.load(Ordering::Acquire), 0);
    }

    #[test]
    fn restart_and_stop_surface_a_bounded_release_timeout() {
        let port = FakePort::new([Some(connection([shutdown_success(42)]))], [], [false]);

        assert!(matches!(
            service(&port).stop("5s"),
            Err(CliError::DaemonLifecycle(
                CliDaemonLifecycleError::ShutdownTimedOut
            ))
        ));
    }

    #[test]
    fn reset_waits_for_a_retiring_owner_and_still_rejects_a_live_owner() {
        struct RetiringPort(bool);
        impl DaemonLifecyclePort for RetiringPort {
            fn connect_existing(&self) -> Result<Option<Box<dyn DaemonLifecycleClient>>, CliError> {
                Err(owned_daemon_unreachable())
            }

            fn launch_and_connect(
                &self,
                _: Duration,
            ) -> Result<Box<dyn DaemonLifecycleClient>, CliError> {
                panic!("reset must never launch")
            }

            fn wait_for_release(&self, timeout: Duration) -> Result<bool, CliError> {
                assert!(timeout <= Duration::from_secs(1));
                Ok(self.0)
            }
        }
        assert_eq!(
            service(&RetiringPort(true))
                .stop_for_reset("1s")
                .unwrap()
                .state,
            DaemonStopState::Stopped
        );
        assert!(matches!(
            service(&RetiringPort(false)).stop_for_reset("1s"),
            Err(CliError::DaemonLifecycle(
                CliDaemonLifecycleError::ShutdownTimedOut
            ))
        ));
    }

    fn service(port: &dyn DaemonLifecyclePort) -> DaemonLifecycleService<'_> {
        DaemonLifecycleService::new(
            port,
            Duration::from_secs(1),
            Duration::from_secs(2),
            test_execution_limits(),
        )
    }

    fn service_with_clock<'a>(
        port: &'a dyn DaemonLifecyclePort,
        request_timeout: Duration,
        shutdown_wait_timeout: Duration,
        clock: &'a dyn LifecycleClock,
    ) -> DaemonLifecycleService<'a> {
        DaemonLifecycleService::with_clock(
            port,
            request_timeout,
            shutdown_wait_timeout,
            test_execution_limits(),
            clock,
        )
    }

    fn test_execution_limits() -> CliExecutionLimits {
        CliExecutionLimits::new(crate::CliExecutionLimitValues {
            catalog_page_size: 10,
            bulk_selection_sessions: 1_000,
            bulk_parallel_requests: 2,
            terminal_columns: 500,
            terminal_rows: 500,
            terminal_input_items: 128,
            terminal_input_bytes: 65_536,
            terminal_output_page_bytes: 1_048_576,
        })
        .expect("limits")
    }

    fn status_success(epoch: &str) -> ProductSuccess {
        ProductSuccess::GetDaemonStatus(DaemonStatusDto {
            server_epoch: epoch.to_owned(),
            started_at_millis: 1,
            accepting_connections: true,
            physical_connections: 1,
            logical_streams: 0,
            managed_sessions: 0,
        })
    }

    fn shutdown_success(deadline: i64) -> ProductSuccess {
        ProductSuccess::ShutdownDaemon(ShutdownAcceptedDto {
            drain_deadline_millis: deadline,
        })
    }

    fn connection(
        responses: impl IntoIterator<Item = ProductSuccess>,
    ) -> Box<dyn DaemonLifecycleClient> {
        Box::new(FakeClient {
            responses: Mutex::new(responses.into_iter().collect()),
        })
    }

    struct FakeClient {
        responses: Mutex<VecDeque<ProductSuccess>>,
    }

    impl CliProductRequestPort for FakeClient {
        fn request(
            &self,
            _request: ProductRequest,
            _timeout: Duration,
        ) -> Result<ProductSuccess, ProductClientError> {
            Ok(self
                .responses
                .lock()
                .expect("response lock")
                .pop_front()
                .expect("configured response"))
        }
    }

    impl DaemonLifecycleClient for FakeClient {
        fn request_port(&self) -> &dyn CliProductRequestPort {
            self
        }

        fn close(&self) {}
    }

    struct DeadlineConsumingStatusClient {
        clock: Arc<TestLifecycleClock>,
        observed_timeouts: Arc<Mutex<Vec<Duration>>>,
    }

    impl CliProductRequestPort for DeadlineConsumingStatusClient {
        fn request(
            &self,
            _request: ProductRequest,
            timeout: Duration,
        ) -> Result<ProductSuccess, ProductClientError> {
            self.observed_timeouts
                .lock()
                .expect("timeout lock")
                .push(timeout);
            self.clock.advance(timeout);
            Ok(status_success("new"))
        }
    }

    impl DaemonLifecycleClient for DeadlineConsumingStatusClient {
        fn request_port(&self) -> &dyn CliProductRequestPort {
            self
        }

        fn close(&self) {}
    }

    struct BudgetSharingClient {
        clock: Arc<TestLifecycleClock>,
        responses: Mutex<VecDeque<ProductSuccess>>,
        advances: Mutex<VecDeque<Duration>>,
        observed_timeouts: Arc<Mutex<Vec<Duration>>>,
    }

    impl CliProductRequestPort for BudgetSharingClient {
        fn request(
            &self,
            _request: ProductRequest,
            timeout: Duration,
        ) -> Result<ProductSuccess, ProductClientError> {
            self.observed_timeouts
                .lock()
                .expect("request timeout lock")
                .push(timeout);
            let advance = self
                .advances
                .lock()
                .expect("advance lock")
                .pop_front()
                .expect("configured clock advance");
            self.clock.advance(advance);
            Ok(self
                .responses
                .lock()
                .expect("response lock")
                .pop_front()
                .expect("configured response"))
        }
    }

    impl DaemonLifecycleClient for BudgetSharingClient {
        fn request_port(&self) -> &dyn CliProductRequestPort {
            self
        }

        fn close(&self) {}
    }

    struct TestLifecycleClock {
        origin: Instant,
        elapsed: Mutex<Duration>,
        sleeps: AtomicUsize,
    }

    impl TestLifecycleClock {
        fn new() -> Self {
            Self {
                origin: Instant::now(),
                elapsed: Mutex::new(Duration::ZERO),
                sleeps: AtomicUsize::new(0),
            }
        }

        fn advance(&self, duration: Duration) {
            let mut elapsed = self.elapsed.lock().expect("clock lock");
            *elapsed = elapsed.saturating_add(duration);
        }
    }

    impl LifecycleClock for TestLifecycleClock {
        fn now(&self) -> Instant {
            self.origin + *self.elapsed.lock().expect("clock lock")
        }

        fn sleep(&self, duration: Duration) {
            self.sleeps.fetch_add(1, Ordering::AcqRel);
            self.advance(duration);
        }
    }

    struct FakePort {
        existing: Mutex<VecDeque<Option<Box<dyn DaemonLifecycleClient>>>>,
        launched: Mutex<VecDeque<Box<dyn DaemonLifecycleClient>>>,
        waits: Mutex<VecDeque<bool>>,
        wait_timeouts: Mutex<Vec<Duration>>,
        launches: AtomicUsize,
        wait_calls: AtomicUsize,
        closes: Arc<AtomicUsize>,
    }

    struct DirectSupervisedReplacementPort {
        existing: Mutex<Option<Box<dyn DaemonLifecycleClient>>>,
        replacement: Mutex<Option<Box<dyn DaemonLifecycleClient>>>,
        launches: AtomicUsize,
        probes: AtomicUsize,
    }

    struct DeadlineConsumingInitialConnectPort {
        clock: Arc<TestLifecycleClock>,
        client: Mutex<Option<Box<dyn DaemonLifecycleClient>>>,
        observed_timeouts: Mutex<Vec<Duration>>,
        launches: AtomicUsize,
    }

    impl DaemonLifecyclePort for DeadlineConsumingInitialConnectPort {
        fn connect_existing(&self) -> Result<Option<Box<dyn DaemonLifecycleClient>>, CliError> {
            panic!("lifecycle service must use the bounded initial-connect boundary")
        }

        fn connect_existing_with_timeout(
            &self,
            timeout: Duration,
        ) -> Result<Option<Box<dyn DaemonLifecycleClient>>, CliError> {
            self.observed_timeouts
                .lock()
                .expect("connect timeout lock")
                .push(timeout);
            self.clock.advance(timeout);
            Ok(self.client.lock().expect("client lock").take())
        }

        fn launch_and_connect(
            &self,
            _timeout: Duration,
        ) -> Result<Box<dyn DaemonLifecycleClient>, CliError> {
            self.launches.fetch_add(1, Ordering::AcqRel);
            panic!("late initial connection must not launch a process")
        }

        fn wait_for_release(&self, _timeout: Duration) -> Result<bool, CliError> {
            panic!("late initial connection must not enter release polling")
        }
    }

    impl DaemonLifecyclePort for DirectSupervisedReplacementPort {
        fn connect_existing(&self) -> Result<Option<Box<dyn DaemonLifecycleClient>>, CliError> {
            Ok(self.existing.lock().expect("existing lock").take())
        }

        fn launch_and_connect(
            &self,
            _timeout: Duration,
        ) -> Result<Box<dyn DaemonLifecycleClient>, CliError> {
            self.launches.fetch_add(1, Ordering::AcqRel);
            panic!("externally supervised replacement must not launch")
        }

        fn wait_for_release(&self, _timeout: Duration) -> Result<bool, CliError> {
            panic!("direct authenticated epoch replacement must not require release")
        }

        fn wait_for_replacement(
            &self,
            timeout: Duration,
        ) -> Result<DaemonReplacementObservation, CliError> {
            assert!(!timeout.is_zero());
            self.probes.fetch_add(1, Ordering::AcqRel);
            Ok(DaemonReplacementObservation::Reachable(
                self.replacement
                    .lock()
                    .expect("replacement lock")
                    .take()
                    .expect("configured replacement"),
            ))
        }
    }

    impl FakePort {
        fn new(
            existing: impl IntoIterator<Item = Option<Box<dyn DaemonLifecycleClient>>>,
            launched: impl IntoIterator<Item = Box<dyn DaemonLifecycleClient>>,
            waits: impl IntoIterator<Item = bool>,
        ) -> Self {
            let closes = Arc::new(AtomicUsize::new(0));
            let existing = existing
                .into_iter()
                .map(|client| client.map(|client| counted_client(client, closes.clone())))
                .collect();
            let launched = launched
                .into_iter()
                .map(|client| counted_client(client, closes.clone()))
                .collect();
            Self {
                existing: Mutex::new(existing),
                launched: Mutex::new(launched),
                waits: Mutex::new(waits.into_iter().collect()),
                wait_timeouts: Mutex::new(Vec::new()),
                launches: AtomicUsize::new(0),
                wait_calls: AtomicUsize::new(0),
                closes,
            }
        }
    }

    fn counted_client(
        inner: Box<dyn DaemonLifecycleClient>,
        closes: Arc<AtomicUsize>,
    ) -> Box<dyn DaemonLifecycleClient> {
        Box::new(CountingClient { inner, closes })
    }

    struct CountingClient {
        inner: Box<dyn DaemonLifecycleClient>,
        closes: Arc<AtomicUsize>,
    }

    impl DaemonLifecycleClient for CountingClient {
        fn request_port(&self) -> &dyn CliProductRequestPort {
            self.inner.request_port()
        }

        fn close(&self) {
            self.closes.fetch_add(1, Ordering::AcqRel);
            self.inner.close();
        }
    }

    impl DaemonLifecyclePort for FakePort {
        fn connect_existing(&self) -> Result<Option<Box<dyn DaemonLifecycleClient>>, CliError> {
            Ok(self
                .existing
                .lock()
                .expect("existing lock")
                .pop_front()
                .expect("configured probe"))
        }

        fn launch_and_connect(
            &self,
            _timeout: Duration,
        ) -> Result<Box<dyn DaemonLifecycleClient>, CliError> {
            self.launches.fetch_add(1, Ordering::AcqRel);
            Ok(self
                .launched
                .lock()
                .expect("launch lock")
                .pop_front()
                .expect("configured launch"))
        }

        fn wait_for_release(&self, timeout: Duration) -> Result<bool, CliError> {
            self.wait_calls.fetch_add(1, Ordering::AcqRel);
            self.wait_timeouts
                .lock()
                .expect("wait timeout lock")
                .push(timeout);
            Ok(self
                .waits
                .lock()
                .expect("wait lock")
                .pop_front()
                .expect("configured wait"))
        }
    }

    #[test]
    #[cfg(unix)]
    fn available_guard_rejects_parent_and_named_lock_replacement_before_reset() {
        let temporary = tempdir().expect("temporary directory");
        #[cfg(unix)]
        std::fs::set_permissions(temporary.path(), std::fs::Permissions::from_mode(0o700))
            .expect("private parent");
        let scope = temporary.path().join("daemon.lock");
        let guard = acquire_available_daemon_instance_lock(&scope).expect("instance guard");
        let displaced = temporary.path().with_extension("displaced");
        std::fs::rename(temporary.path(), &displaced).expect("displace parent");
        std::fs::create_dir(temporary.path()).expect("replacement parent");
        #[cfg(unix)]
        std::fs::set_permissions(temporary.path(), std::fs::Permissions::from_mode(0o700))
            .expect("replacement permissions");
        assert!(guard.revalidate().is_err());
        drop(guard);
        std::fs::remove_dir_all(temporary.path()).expect("replacement cleanup");
        std::fs::rename(&displaced, temporary.path()).expect("restore parent");

        let guard = acquire_available_daemon_instance_lock(&scope).expect("second guard");
        let lock_path = temporary
            .path()
            .join(instance_lock_name(&scope).expect("lock name"));
        std::fs::remove_file(&lock_path).expect("unlink held lock name");
        std::fs::write(&lock_path, b"").expect("replacement lock");
        std::fs::set_permissions(&lock_path, std::fs::Permissions::from_mode(0o600))
            .expect("replacement mode");
        assert!(guard.revalidate().is_err());
    }
}
