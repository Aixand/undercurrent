// SPDX-License-Identifier: Apache-2.0

//! Typed product configuration and canonical daemon runtime paths.

pub use undercurrent_config::{
    DaemonLoggingConfig, DaemonPathConfig, DaemonProfileConfig, DaemonWorkerConfig,
    LocalTransportConfig, PersistenceConfig, ProcessRuntimeConfig, TerminalRuntimeConfig,
    UndercurrentProductConfig,
};

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::structured_logging::{StructuredLogSettings, StructuredLogSettingsError};
use crate::{
    ConfigDiscovery, ConfigError, ConfigSource, PathsError, ResolvedRuntimePath,
    RuntimePathCandidates, RuntimePathSource, TelemetryConfig, UndercurrentPaths, load_config,
};
use serde::Serialize;
use undercurrent_application::{
    ObservationDispatchLimits, OutboxDispatchLimits, StartupReconciliationLimits,
    TerminalOutputLimits,
};
use undercurrent_runtime::{
    NativeHealthCheckExecutorLimits, NativeProcessRuntimeConfig, NativeProcessRuntimeLimits,
    SessionMutationCoordinatorLimits, TerminalScreenLimits,
};
use undercurrent_store_sqlite::{SqliteRepositoryConfig, SqliteRepositoryLimits};
use undercurrent_transport_local::LocalEndpoint;

use crate::{DaemonClientSettings, DaemonServiceSettings, DaemonTransportSettings};

/// Stable product key used by common configuration and path resolution.
pub const UNDERCURRENT_PRODUCT_SLUG: &str = "undercurrent";
/// Default durable database file inside the product home.
pub const UNDERCURRENT_DATABASE_FILE: &str = "undercurrent.db";
/// Default daemon instance-lock file inside the product home.
pub const UNDERCURRENT_INSTANCE_LOCK_FILE: &str = "daemon.lock";
/// Private native session-host socket inside the product home.
pub const UNDERCURRENT_SESSION_HOST_SOCKET_FILE: &str = "session-host.sock";
/// Owner-only local agent bridge socket inside the product home.
pub const UNDERCURRENT_AGENT_BRIDGE_SOCKET_FILE: &str = "agent-bridge.sock";
/// Structured daemon log file under the selected product log directory.
pub const UNDERCURRENT_DAEMON_LOG_FILE: &str = "daemon.jsonl";
/// Smallest file bound that can retain one complete payload-safe log event.
pub const MIN_DAEMON_LOG_FILE_BYTES: u64 = 4_096;

/// Central registry of Undercurrent-owned environment variables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DaemonEnvironmentVariable {
    /// Absolute root for all product configuration and runtime storage.
    Home,
}

impl DaemonEnvironmentVariable {
    /// Complete registry of Undercurrent-owned environment variables.
    pub const ALL: [Self; 1] = [Self::Home];

    /// Returns the stable process environment name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Home => "UNDERCURRENT_HOME",
        }
    }
}

/// Explicit low-level embedding overrides. Shipped binaries do not expose these.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct DaemonConfigOverrides {
    database_path: Option<PathBuf>,
    instance_lock_path: Option<PathBuf>,
}

impl DaemonConfigOverrides {
    /// Constructs empty overrides.
    ///
    /// This function performs no IO and does not panic.
    pub const fn new() -> Self {
        Self {
            database_path: None,
            instance_lock_path: None,
        }
    }

    /// Sets the highest-precedence durable database path.
    pub fn with_database_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.database_path = Some(path.into());
        self
    }

    /// Sets the highest-precedence daemon instance-lock path.
    pub fn with_instance_lock_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.instance_lock_path = Some(path.into());
        self
    }
}

impl Debug for DaemonConfigOverrides {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonConfigOverrides")
            .field("database_path_set", &self.database_path.is_some())
            .field("instance_lock_path_set", &self.instance_lock_path.is_some())
            .finish()
    }
}

/// Validated structured logging settings used by the outer daemon adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DaemonLoggingSettings {
    structured: StructuredLogSettings,
}

impl DaemonLoggingSettings {
    /// Validates all logging resource bounds without performing IO.
    ///
    /// # Errors
    ///
    /// Returns a stable rule when the file bound cannot hold a safe event or a
    /// retention or queue bound is zero.
    pub fn new(config: DaemonLoggingConfig) -> Result<Self, DaemonConfigError> {
        if config.max_file_bytes < MIN_DAEMON_LOG_FILE_BYTES {
            return Err(DaemonConfigError::InvalidRelationship {
                rule: "logging max file bytes is below the minimum safe event size",
            });
        }
        let structured = StructuredLogSettings::new(
            config.max_file_bytes,
            config.retained_files,
            config.queue_lines,
        )
        .map_err(|source| DaemonConfigError::InvalidRelationship {
            rule: match source {
                StructuredLogSettingsError::MaxFileBytes => {
                    "logging max file bytes must be non-zero"
                }
                StructuredLogSettingsError::RetainedFiles => {
                    "logging retained file count must be non-zero"
                }
                StructuredLogSettingsError::QueueLines => {
                    "logging queue line count must be non-zero"
                }
            },
        })?;
        Ok(Self { structured })
    }

    /// Returns the inclusive byte bound for one log file.
    pub const fn max_file_bytes(self) -> u64 {
        self.structured.max_file_bytes()
    }

    /// Returns the inclusive total active-plus-rotated file count.
    pub const fn retained_files(self) -> usize {
        self.structured.retained_files()
    }

    /// Returns the bounded non-blocking writer queue in complete lines.
    pub const fn queue_lines(self) -> usize {
        self.structured.queue_lines()
    }

    /// Converts product-validated bounds into the shared outer logging adapter.
    pub const fn structured(self) -> StructuredLogSettings {
        self.structured
    }
}

/// Safe config-source category suitable for diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonConfigSourceKind {
    /// No config file was present.
    Defaults,
    /// Caller selected a config file explicitly.
    Explicit,
    /// Legacy environment-selected source category retained for compatibility.
    /// The current loader rejects `UNDERCURRENT_CONFIG` and never produces this category.
    Environment,
    /// The product-owned default configuration was loaded.
    UndercurrentHome,
}

impl DaemonConfigSourceKind {
    fn from_common(source: &ConfigSource) -> Self {
        match source {
            ConfigSource::Defaults => Self::Defaults,
            ConfigSource::Explicit(_) => Self::Explicit,
            ConfigSource::Environment(_) => Self::Environment,
            ConfigSource::UndercurrentHome(_) => Self::UndercurrentHome,
        }
    }
}

/// Validated database, instance-lock, and private host locations.
#[derive(Clone, PartialEq, Eq)]
pub struct DaemonRuntimePaths {
    product_home: PathBuf,
    database: ResolvedRuntimePath,
    instance_lock: ResolvedRuntimePath,
    session_host: PathBuf,
    log: ResolvedRuntimePath,
}

impl DaemonRuntimePaths {
    /// Returns the canonical product home used for defaults.
    pub fn product_home(&self) -> &Path {
        &self.product_home
    }

    /// Returns the selected absolute durable database path.
    pub fn database(&self) -> &Path {
        &self.database.path
    }

    /// Returns how the database path was selected.
    pub const fn database_source(&self) -> RuntimePathSource {
        self.database.source
    }

    /// Returns the selected absolute daemon instance-lock path.
    pub fn instance_lock(&self) -> &Path {
        &self.instance_lock.path
    }

    /// Returns the private daemon-independent native session-host socket.
    pub fn session_host(&self) -> &Path {
        &self.session_host
    }

    /// Returns how the instance-lock path was selected.
    pub const fn instance_lock_source(&self) -> RuntimePathSource {
        self.instance_lock.source
    }

    /// Returns the selected absolute structured daemon log path.
    pub fn log(&self) -> &Path {
        &self.log.path
    }

    /// Returns how the structured daemon log path was selected.
    pub const fn log_source(&self) -> RuntimePathSource {
        self.log.source
    }
}

impl Debug for DaemonRuntimePaths {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonRuntimePaths")
            .field("product_home", &"<redacted>")
            .field("database", &"<redacted>")
            .field("database_source", &self.database.source)
            .field("instance_lock", &"<redacted>")
            .field("instance_lock_source", &self.instance_lock.source)
            .field("session_host", &"<redacted>")
            .field("log", &"<redacted>")
            .field("log_source", &self.log.source)
            .finish()
    }
}

/// Fully validated, side-effect-free daemon runtime configuration.
#[derive(Clone)]
pub struct ResolvedDaemonConfig {
    profile_directory: PathBuf,
    source: DaemonConfigSourceKind,
    paths: DaemonRuntimePaths,
    telemetry: TelemetryConfig,
    logging: DaemonLoggingSettings,
    local_endpoint: LocalEndpoint,
    sqlite: SqliteRepositoryConfig,
    process: NativeProcessRuntimeLimits,
    terminal_output: TerminalOutputLimits,
    terminal_screen: TerminalScreenLimits,
    health_checks: NativeHealthCheckExecutorLimits,
    force_confirmation_timeout: Duration,
    mutation: SessionMutationCoordinatorLimits,
    outbox: OutboxDispatchLimits,
    observation: ObservationDispatchLimits,
    reconciliation: StartupReconciliationLimits,
    restart_batch_size: usize,
    worker_intervals: DaemonWorkerIntervals,
    transport: DaemonTransportSettings,
    client: DaemonClientSettings,
    services: DaemonServiceSettings,
    redacted_product: UndercurrentProductConfig,
}

/// Effective runtime path selections without any filesystem values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RedactedDaemonPathSources {
    /// Precedence category that selected the durable database path.
    pub database: RuntimePathSource,
    /// Precedence category that selected the daemon instance-lock path.
    pub instance_lock: RuntimePathSource,
    /// Precedence category that selected the structured daemon log path.
    pub log: RuntimePathSource,
}

/// Complete effective Undercurrent configuration safe for presentation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RedactedDaemonConfigReport {
    /// Safe category describing where common configuration was loaded.
    pub source: DaemonConfigSourceKind,
    /// Runtime path precedence without product-home, database, or lock values.
    pub path_sources: RedactedDaemonPathSources,
    /// Effective common telemetry settings.
    pub telemetry: TelemetryConfig,
    /// Effective product settings with both path fields removed.
    pub undercurrent: UndercurrentProductConfig,
}

impl ResolvedDaemonConfig {
    /// Resolved YAML directory; includes private filesystem information.
    pub fn profile_directory(&self) -> &std::path::Path {
        &self.profile_directory
    }

    /// Returns the safe common config source category.
    pub const fn source(&self) -> DaemonConfigSourceKind {
        self.source
    }

    /// Returns selected runtime paths. Callers must not log their values.
    pub const fn paths(&self) -> &DaemonRuntimePaths {
        &self.paths
    }

    /// Returns common safe telemetry preferences.
    pub const fn telemetry(&self) -> &TelemetryConfig {
        &self.telemetry
    }

    /// Returns validated bounded structured logging settings.
    pub const fn logging(&self) -> DaemonLoggingSettings {
        self.logging
    }

    /// Returns the verified-format platform-local client address.
    pub fn local_endpoint(&self) -> &LocalEndpoint {
        &self.local_endpoint
    }

    /// Returns the validated durable repository configuration.
    pub const fn sqlite(&self) -> &SqliteRepositoryConfig {
        &self.sqlite
    }

    /// Builds the native runtime configuration for one newly generated daemon epoch.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns [`DaemonConfigError::InvalidComponent`] if `epoch` violates the
    /// native process identity contract.
    pub fn native_process(
        &self,
        epoch: impl Into<String>,
    ) -> Result<NativeProcessRuntimeConfig, DaemonConfigError> {
        NativeProcessRuntimeConfig::new(
            epoch,
            self.process,
            self.terminal_output,
            self.force_confirmation_timeout,
        )
        .map(|config| config.with_scrollback_lines(self.terminal_screen.scrollback_lines))
        .map_err(|source| DaemonConfigError::component("process", source))
    }

    /// Returns bounded retained terminal output settings.
    pub const fn terminal_output(&self) -> TerminalOutputLimits {
        self.terminal_output
    }

    /// Returns bounded live terminal screen settings.
    pub const fn terminal_screen(&self) -> TerminalScreenLimits {
        self.terminal_screen
    }

    /// Returns fixed-pool and per-rule health execution bounds.
    pub const fn health_checks(&self) -> NativeHealthCheckExecutorLimits {
        self.health_checks
    }

    /// Returns per-session mutation coordination bounds.
    pub const fn mutation(&self) -> SessionMutationCoordinatorLimits {
        self.mutation
    }

    /// Returns transactional outbox dispatch bounds.
    pub const fn outbox(&self) -> OutboxDispatchLimits {
        self.outbox
    }

    /// Returns scheduled observation dispatch bounds.
    pub const fn observation(&self) -> ObservationDispatchLimits {
        self.observation
    }

    /// Returns startup durable/live truth reconciliation bounds.
    pub const fn reconciliation(&self) -> StartupReconciliationLimits {
        self.reconciliation
    }

    /// Returns the maximum due restart jobs claimed by one worker cycle.
    pub const fn restart_batch_size(&self) -> usize {
        self.restart_batch_size
    }

    /// Returns the stack reserved for each daemon-owned blocking worker.
    pub const fn worker_thread_stack_bytes(&self) -> usize {
        self.process.worker_thread_stack_bytes
    }

    /// Returns idle polling intervals for the three scheduled workers.
    pub const fn worker_intervals(&self) -> DaemonWorkerIntervals {
        self.worker_intervals
    }

    /// Returns the validated physical and logical connection settings.
    pub const fn transport(&self) -> &DaemonTransportSettings {
        &self.transport
    }

    /// Returns validated native SDK and local transport settings.
    pub const fn client(&self) -> &DaemonClientSettings {
        &self.client
    }

    /// Returns validated domain boundary and application resource settings.
    pub const fn services(&self) -> &DaemonServiceSettings {
        &self.services
    }

    /// Returns the complete effective configuration with all path values removed.
    ///
    /// The report retains only path precedence categories, so callers may
    /// serialize it without exposing product-home, database, or lock values.
    /// This function performs no IO and does not panic.
    pub fn redacted_report(&self) -> RedactedDaemonConfigReport {
        RedactedDaemonConfigReport {
            source: self.source,
            path_sources: RedactedDaemonPathSources {
                database: self.paths.database_source(),
                instance_lock: self.paths.instance_lock_source(),
                log: self.paths.log_source(),
            },
            telemetry: self.telemetry.clone(),
            undercurrent: self.redacted_product.clone(),
        }
    }
}

impl Debug for ResolvedDaemonConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResolvedDaemonConfig")
            .field("source", &self.source)
            .field("paths", &self.paths)
            .field("telemetry", &self.telemetry)
            .field("logging", &self.logging)
            .field("process", &self.process)
            .field("terminal_output", &self.terminal_output)
            .field("terminal_screen", &self.terminal_screen)
            .field("health_checks", &self.health_checks)
            .field(
                "force_confirmation_timeout",
                &self.force_confirmation_timeout,
            )
            .field("mutation", &self.mutation)
            .field("outbox", &self.outbox)
            .field("observation", &self.observation)
            .field("reconciliation", &self.reconciliation)
            .field("restart_batch_size", &self.restart_batch_size)
            .field("worker_intervals", &self.worker_intervals)
            .field("transport", &self.transport)
            .field("client", &self.client)
            .field("services", &self.services)
            .finish()
    }
}

/// Validated idle polling intervals for daemon background workers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DaemonWorkerIntervals {
    /// Transactional outbox worker idle delay.
    pub outbox: Duration,
    /// Scheduled restart worker idle delay.
    pub restart: Duration,
    /// Scheduled observation worker idle delay.
    pub observation: Duration,
    /// Process-exit source and settlement error backoff.
    pub process_exit_error: Duration,
}

/// Failure loading, resolving, or validating daemon configuration.
#[derive(Debug)]
pub enum DaemonConfigError {
    /// Common config discovery or product-section decoding failed.
    Common(ConfigError),
    /// Common product-home resolution failed.
    Paths(PathsError),
    /// A selected runtime path was empty or not absolute.
    InvalidPath {
        /// Stable path setting name.
        field: &'static str,
        /// Stable non-sensitive validation rule.
        rule: &'static str,
    },
    /// One concrete component rejected its corresponding settings.
    InvalidComponent {
        /// Stable product config section.
        component: &'static str,
        /// Original typed component validation failure.
        source: Box<dyn Error + Send + Sync>,
    },
    /// Individually valid settings violated a product-level relationship.
    InvalidRelationship {
        /// Stable non-sensitive relationship rule.
        rule: &'static str,
    },
}

impl DaemonConfigError {
    pub(crate) fn component(
        component: &'static str,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self::InvalidComponent {
            component,
            source: Box::new(source),
        }
    }
}

impl Display for DaemonConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Common(error) => write!(formatter, "failed to load daemon config: {error}"),
            Self::Paths(error) => write!(formatter, "failed to resolve daemon paths: {error}"),
            Self::InvalidPath { field, rule } => {
                write!(formatter, "daemon path {field} {rule}")
            }
            Self::InvalidComponent { component, source } => {
                write!(
                    formatter,
                    "invalid daemon {component} configuration: {source}"
                )
            }
            Self::InvalidRelationship { rule } => formatter.write_str(rule),
        }
    }
}

impl Error for DaemonConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Common(error) => Some(error),
            Self::Paths(error) => Some(error),
            Self::InvalidComponent { source, .. } => Some(source.as_ref()),
            Self::InvalidPath { .. } | Self::InvalidRelationship { .. } => None,
        }
    }
}

impl From<ConfigError> for DaemonConfigError {
    fn from(error: ConfigError) -> Self {
        Self::Common(error)
    }
}

impl From<PathsError> for DaemonConfigError {
    fn from(error: PathsError) -> Self {
        Self::Paths(error)
    }
}

/// Loads the standalone document, decodes runtime settings, resolves path
/// precedence, and validates every runtime setting represented by this module.
///
/// The operation reads only the selected config file and process-independent
/// discovery inputs. It does not create directories, open the database, bind a
/// listeners, spawn processes, or start workers. Runtime paths must be absolute so
/// daemon identity never depends on its current working directory.
///
/// # Errors
///
/// Returns [`DaemonConfigError`] for config IO/decoding, product-home
/// resolution, ambiguous paths, or an invalid concrete component limit.
///
/// # Panics
///
/// Does not panic.
pub fn load_daemon_config(
    discovery: &ConfigDiscovery,
    overrides: DaemonConfigOverrides,
) -> Result<ResolvedDaemonConfig, DaemonConfigError> {
    for name in [
        "UNDERCURRENT_ENDPOINT",
        "UNDERCURRENT_TOKEN_FILE",
        "UNDERCURRENT_TLS_CA_FILE",
        "UNDERCURRENT_TLS_CLIENT_CERT_FILE",
        "UNDERCURRENT_TLS_CLIENT_KEY_FILE",
    ] {
        if std::env::var_os(name).is_some() {
            return Err(DaemonConfigError::InvalidRelationship {
                rule: "retired network environment override; use UNDERCURRENT_HOME",
            });
        }
    }
    let loaded = load_config(discovery)?;
    let product = loaded.runtime_config::<UndercurrentProductConfig>()?;
    let redacted_product = product.clone();
    let paths = UndercurrentPaths::resolve(&discovery.path_env)?;
    let profile_directory = paths.profiles_dir();
    let product_home = paths.home().to_path_buf();
    let log = ResolvedRuntimePath {
        path: paths.logs_dir().join(UNDERCURRENT_DAEMON_LOG_FILE),
        source: RuntimePathSource::Default,
    };
    let session_host = paths.run_dir().join(UNDERCURRENT_SESSION_HOST_SOCKET_FILE);
    // Explicit low-level overrides remain available for embedding; shipped binaries expose none.
    let database = RuntimePathCandidates::new(paths.state_dir().join(UNDERCURRENT_DATABASE_FILE))
        .with_explicit(overrides.database_path)
        .resolve();
    let instance_lock =
        RuntimePathCandidates::new(paths.run_dir().join(UNDERCURRENT_INSTANCE_LOCK_FILE))
            .with_explicit(overrides.instance_lock_path)
            .resolve();
    validate_absolute_path("database", &database.path)?;
    validate_absolute_path("instance_lock", &instance_lock.path)?;
    validate_absolute_path("session_host", &session_host)?;
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    validate_session_host_socket_path(&session_host)?;
    validate_absolute_path("log", &log.path)?;
    let runtime_paths = [
        database.path.as_path(),
        instance_lock.path.as_path(),
        session_host.as_path(),
        log.path.as_path(),
    ];
    if runtime_paths
        .iter()
        .enumerate()
        .any(|(index, path)| runtime_paths[index + 1..].contains(path))
    {
        return Err(DaemonConfigError::InvalidRelationship {
            rule: "daemon database, instance lock, private host socket, and structured log paths must be distinct",
        });
    }

    let logging = DaemonLoggingSettings::new(product.logging)?;

    let persistence = product.persistence;
    let sqlite_limits = SqliteRepositoryLimits::new(
        persistence.max_sessions,
        persistence.max_outbox_events,
        persistence.max_record_bytes,
        persistence.max_read_batch,
        persistence.command_queue_capacity,
    )
    .map_err(|source| DaemonConfigError::component("persistence", source))?;
    let sqlite = SqliteRepositoryConfig::new(
        database.path.clone(),
        sqlite_limits,
        Duration::from_millis(persistence.busy_timeout_millis),
    )
    .map_err(|source| DaemonConfigError::component("persistence", source))?;

    let terminal = product.terminal;
    let terminal_output = TerminalOutputLimits {
        per_session_bytes: terminal.per_session_bytes,
        global_bytes: terminal.global_bytes,
        chunk_bytes: terminal.chunk_bytes,
        append_bytes: terminal.append_bytes,
        replay_bytes: terminal.replay_bytes,
        sessions: terminal.sessions,
        subscriptions: terminal.subscriptions,
    }
    .validate()
    .map_err(|source| DaemonConfigError::component("terminal", source))?;
    let process = product.process;
    let process_limits = NativeProcessRuntimeLimits {
        max_processes: process.max_processes,
        spawn_workers: process.spawn_workers,
        spawn_queue_capacity: process.spawn_queue_capacity,
        input_queue_sequences: process.input_queue_sequences,
        input_queue_bytes_per_process: process.input_queue_bytes_per_process,
        output_read_bytes: process.output_read_bytes,
        screen_snapshot_bytes: process.screen_snapshot_bytes,
        environment_file_bytes: process.environment_file_bytes,
        environment_entries: process.environment_entries,
        environment_bytes: process.environment_bytes,
        worker_thread_stack_bytes: process.worker_thread_stack_bytes,
    }
    .validate()
    .map_err(|source| DaemonConfigError::component("process", source))?;
    NativeProcessRuntimeConfig::new(
        "validation",
        process_limits,
        terminal_output,
        Duration::from_millis(process.force_confirmation_timeout_millis),
    )
    .map_err(|source| DaemonConfigError::component("process", source))?;
    let terminal_screen = TerminalScreenLimits {
        sessions: process_limits.max_processes,
        snapshot_bytes: process_limits.screen_snapshot_bytes,
        scrollback_lines: terminal.scrollback.memory_lines,
    }
    .validate()
    .map_err(|source| DaemonConfigError::component("process", source))?;
    if process_limits.max_processes > sqlite_limits.max_sessions() {
        return Err(DaemonConfigError::InvalidRelationship {
            rule: "process capacity must not exceed durable session capacity",
        });
    }

    let workers = product.workers;
    let mutation = SessionMutationCoordinatorLimits::new(
        workers.mutation_active_sessions,
        workers.mutation_waiters_per_session,
        workers.mutation_diagnostic_page_size,
    )
    .map_err(|source| DaemonConfigError::component("workers.mutation", source))?;
    let outbox = OutboxDispatchLimits::new(
        workers.outbox_batch_size,
        Duration::from_millis(workers.foreground_activation_delay_millis),
        Duration::from_millis(workers.health_activation_delay_millis),
    )
    .map_err(|source| DaemonConfigError::component("workers.outbox", source))?;
    let restart_batch_size =
        nonzero_count("workers.restart_batch_size", workers.restart_batch_size)?;
    let observation = ObservationDispatchLimits::new(
        workers.observation_batch_size,
        workers.observation_retained_issues,
        Duration::from_millis(workers.foreground_interval_millis),
        Duration::from_millis(workers.health_interval_millis),
        Duration::from_millis(workers.observation_retry_interval_millis),
    )
    .map_err(|source| DaemonConfigError::component("workers.observation", source))?;
    let reconciliation = StartupReconciliationLimits::new(
        workers.reconciliation_batch_size,
        workers.reconciliation_retained_issues,
    )
    .map_err(|source| DaemonConfigError::component("workers.reconciliation", source))?;
    if outbox.batch_size() > sqlite_limits.max_read_batch() {
        return Err(DaemonConfigError::InvalidRelationship {
            rule: "outbox batch size must not exceed the persistence read batch limit",
        });
    }
    if reconciliation.batch_size() > sqlite_limits.max_read_batch() {
        return Err(DaemonConfigError::InvalidRelationship {
            rule: "reconciliation batch size must not exceed the persistence read batch limit",
        });
    }
    let worker_intervals = DaemonWorkerIntervals {
        outbox: nonzero_duration(
            "workers.outbox_poll_interval_millis",
            workers.outbox_poll_interval_millis,
        )?,
        restart: nonzero_duration(
            "workers.restart_poll_interval_millis",
            workers.restart_poll_interval_millis,
        )?,
        observation: nonzero_duration(
            "workers.observation_poll_interval_millis",
            workers.observation_poll_interval_millis,
        )?,
        process_exit_error: nonzero_duration(
            "workers.process_exit_error_backoff_millis",
            workers.process_exit_error_backoff_millis,
        )?,
    };
    let transport = DaemonTransportSettings::validate(
        product.transport,
        product.requests,
        terminal_output,
        process_limits,
    )?;
    let local_path = paths.run_dir().join("control.sock");
    let local_endpoint = LocalEndpoint::new(local_path)
        .map_err(|e| DaemonConfigError::component("local.endpoint", e))?;
    let client = DaemonClientSettings::validate(product.client, &transport)?;
    let service_config = product.services;
    let health_screen_cells = usize::from(service_config.max_terminal_columns)
        .checked_mul(usize::from(service_config.max_terminal_rows))
        .ok_or(DaemonConfigError::InvalidRelationship {
            rule: "maximum terminal geometry exceeds health screen capacity",
        })?;
    let health_pattern_bytes = service_config.max_health_rule_bytes;
    let services = DaemonServiceSettings::validate(
        service_config,
        terminal_output,
        process_limits,
        outbox,
        &transport,
    )?;
    if restart_batch_size > services.resources().restart_jobs() {
        return Err(DaemonConfigError::InvalidRelationship {
            rule: "restart batch size must not exceed restart queue capacity",
        });
    }
    let health_checks = NativeHealthCheckExecutorLimits {
        workers: workers.health_check_workers,
        queue_capacity: workers.health_check_queue_capacity,
        check_timeout: Duration::from_millis(workers.health_check_timeout_millis),
        screen_text_bytes: workers.health_check_screen_text_bytes,
        screen_cells: health_screen_cells,
        pattern_bytes: health_pattern_bytes,
        detail_bytes: workers.health_check_detail_bytes,
        worker_thread_stack_bytes: process_limits.worker_thread_stack_bytes,
    }
    .validate()
    .map_err(|source| DaemonConfigError::component("workers.health_checks", source))?;
    if health_checks.workers > process_limits.max_processes {
        return Err(DaemonConfigError::InvalidRelationship {
            rule: "health worker count must not exceed process capacity",
        });
    }
    if health_checks.screen_text_bytes > services.regex_pattern().match_text_bytes {
        return Err(DaemonConfigError::InvalidRelationship {
            rule: "regex match input must contain one maximum health screen sample",
        });
    }
    Ok(ResolvedDaemonConfig {
        profile_directory,
        source: DaemonConfigSourceKind::from_common(&loaded.source),
        paths: DaemonRuntimePaths {
            product_home,
            database,
            instance_lock,
            session_host,
            log,
        },
        telemetry: loaded.config.telemetry,
        logging,
        local_endpoint,
        sqlite,
        process: process_limits,
        terminal_output,
        terminal_screen,
        health_checks,
        force_confirmation_timeout: Duration::from_millis(
            process.force_confirmation_timeout_millis,
        ),
        mutation,
        outbox,
        observation,
        reconciliation,
        restart_batch_size,
        worker_intervals,
        transport,
        client,
        services,
        redacted_product,
    })
}

fn validate_absolute_path(field: &'static str, path: &Path) -> Result<(), DaemonConfigError> {
    if path.as_os_str().is_empty() {
        return Err(DaemonConfigError::InvalidPath {
            field,
            rule: "must not be empty",
        });
    }
    if !path.is_absolute() {
        return Err(DaemonConfigError::InvalidPath {
            field,
            rule: "must be absolute",
        });
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn validate_session_host_socket_path(path: &Path) -> Result<(), DaemonConfigError> {
    use std::os::unix::ffi::OsStrExt;

    let sun_path_capacity = std::mem::size_of::<libc::sockaddr_un>()
        - std::mem::offset_of!(libc::sockaddr_un, sun_path);
    if path.as_os_str().as_bytes().len() >= sun_path_capacity {
        return Err(DaemonConfigError::InvalidPath {
            field: "session_host",
            rule: "exceeds the platform AF_UNIX pathname capacity including its terminating NUL; choose a shorter absolute UNDERCURRENT_HOME (this selects a different state directory)",
        });
    }
    Ok(())
}

fn nonzero_duration(field: &'static str, millis: u64) -> Result<Duration, DaemonConfigError> {
    let duration = Duration::from_millis(millis);
    if duration.is_zero() {
        return Err(DaemonConfigError::InvalidComponent {
            component: field,
            source: Box::new(ZeroDuration),
        });
    }
    Ok(duration)
}

fn nonzero_count(field: &'static str, value: usize) -> Result<usize, DaemonConfigError> {
    if value == 0 {
        return Err(DaemonConfigError::InvalidComponent {
            component: field,
            source: Box::new(ZeroCount),
        });
    }
    Ok(value)
}

#[derive(Debug)]
struct ZeroDuration;

impl Display for ZeroDuration {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("duration must be non-zero")
    }
}

impl Error for ZeroDuration {}

#[derive(Debug)]
struct ZeroCount;

impl Display for ZeroCount {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("count must be non-zero")
    }
}

impl Error for ZeroCount {}

#[cfg(test)]
mod tests {
    use crate::{DaemonRequestConfig, DaemonTransportConfig};
    use std::ffi::OsString;
    use std::fs;

    use crate::PathEnvironment;
    use tempfile::TempDir;
    use undercurrent_wire::ClientId;

    use super::*;

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn home_with_session_host_path_bytes(base: &Path, target_bytes: usize) -> PathBuf {
        use std::os::unix::ffi::OsStrExt;

        const SESSION_HOST_SUFFIX: &str = "/.aixand/undercurrent/run/session-host.sock";
        let fixed_bytes = base.as_os_str().as_bytes().len() + 1 + SESSION_HOST_SUFFIX.len();
        let component_bytes = target_bytes
            .checked_sub(fixed_bytes)
            .expect("temporary base must leave room for the boundary fixture");
        base.join("h".repeat(component_bytes))
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn session_host_socket_accepts_the_exact_pathname_boundary_without_io() {
        use std::os::unix::ffi::OsStrExt;

        let temp = tempfile::Builder::new()
            .prefix("uc-")
            .tempdir_in(if cfg!(target_os = "macos") {
                "/private/tmp"
            } else {
                "/tmp"
            })
            .expect("temporary base");
        let sun_path_capacity = std::mem::size_of::<libc::sockaddr_un>()
            - std::mem::offset_of!(libc::sockaddr_un, sun_path);
        let home = home_with_session_host_path_bytes(temp.path(), sun_path_capacity - 1);
        let session_host = home.join(".aixand/undercurrent/run/session-host.sock");
        assert_eq!(
            session_host.as_os_str().as_bytes().len() + 1,
            sun_path_capacity
        );
        assert_eq!(fs::read_dir(temp.path()).expect("empty base").count(), 0);

        let discovery = ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(&home));
        let config = load_daemon_config(&discovery, DaemonConfigOverrides::new())
            .expect("maximum platform pathname must remain valid");

        assert_eq!(config.paths().session_host(), session_host);
        assert_eq!(
            fs::read_dir(temp.path()).expect("untouched base").count(),
            0
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn session_host_socket_rejects_an_overlong_path_without_io_or_disclosure() {
        use std::os::unix::ffi::OsStrExt;

        let temp = tempfile::Builder::new()
            .prefix("uc-")
            .tempdir_in(if cfg!(target_os = "macos") {
                "/private/tmp"
            } else {
                "/tmp"
            })
            .expect("temporary base");
        let sun_path_capacity = std::mem::size_of::<libc::sockaddr_un>()
            - std::mem::offset_of!(libc::sockaddr_un, sun_path);
        let home = home_with_session_host_path_bytes(temp.path(), sun_path_capacity);
        let session_host = home.join(".aixand/undercurrent/run/session-host.sock");
        assert_eq!(session_host.as_os_str().as_bytes().len(), sun_path_capacity);
        assert_eq!(fs::read_dir(temp.path()).expect("empty base").count(), 0);

        let discovery = ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(&home));
        let error = load_daemon_config(&discovery, DaemonConfigOverrides::new())
            .expect_err("pathname without NUL capacity must fail before runtime setup");
        let rendered = error.to_string();

        assert!(matches!(
            error,
            DaemonConfigError::InvalidPath {
                field: "session_host",
                rule: "exceeds the platform AF_UNIX pathname capacity including its terminating NUL; choose a shorter absolute UNDERCURRENT_HOME (this selects a different state directory)"
            }
        ));
        assert_eq!(
            rendered,
            "daemon path session_host exceeds the platform AF_UNIX pathname capacity including its terminating NUL; choose a shorter absolute UNDERCURRENT_HOME (this selects a different state directory)"
        );
        assert!(!rendered.contains(&home.display().to_string()));
        assert_eq!(
            fs::read_dir(temp.path()).expect("untouched base").count(),
            0
        );
    }

    #[test]
    fn yaml_profile_paths_follow_explicit_home_and_default_precedence() {
        let home = TempDir::new().unwrap();
        let discovery = ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(home.path()));
        let config = load_daemon_config(&discovery, DaemonConfigOverrides::new()).unwrap();
        assert_eq!(
            config.profile_directory(),
            home.path().join(".aixand/undercurrent/profiles")
        );
        let product_home = home.path().join("product");
        let discovery = ConfigDiscovery::from_path_env(
            PathEnvironment::with_home_dir(home.path()).with_undercurrent_home(&product_home),
        );
        let config = load_daemon_config(&discovery, DaemonConfigOverrides::new()).unwrap();
        assert_eq!(config.profile_directory(), product_home.join("profiles"));
        let explicit = home.path().join("editable");
        let config_file = home.path().join("explicit.toml");
        fs::write(
            &config_file,
            format!("[profiles]\ndirectory = '{}'\n", explicit.display()),
        )
        .unwrap();
        let discovery = discovery.with_explicit_path(&config_file);
        assert!(load_daemon_config(&discovery, DaemonConfigOverrides::new()).is_err());
        assert!(
            !explicit.exists(),
            "configuration resolution performs no writes"
        );
        fs::write(&config_file, "[profiles]\ndirectory = 'relative'\n").unwrap();
        assert!(load_daemon_config(&discovery, DaemonConfigOverrides::new()).is_err());
    }

    #[test]
    fn defaults_use_canonical_product_home_without_runtime_io() {
        let home = TempDir::new().expect("temporary home");
        let discovery = ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(home.path()));

        let config =
            load_daemon_config(&discovery, DaemonConfigOverrides::new()).expect("default config");

        assert_eq!(config.source(), DaemonConfigSourceKind::Defaults);
        assert_eq!(
            config.paths().product_home(),
            home.path().join(".aixand/undercurrent")
        );
        assert_eq!(
            config.paths().database(),
            home.path()
                .join(".aixand/undercurrent/state/undercurrent.db")
        );
        assert_eq!(
            config.paths().instance_lock(),
            home.path().join(".aixand/undercurrent/run/daemon.lock")
        );
        assert_eq!(
            config.paths().session_host(),
            home.path()
                .join(".aixand/undercurrent/run/session-host.sock")
        );
        assert_eq!(
            config.paths().log(),
            home.path().join(".aixand/undercurrent/logs/daemon.jsonl")
        );
        assert_eq!(config.paths().database_source(), RuntimePathSource::Default);
        assert_eq!(
            config.paths().instance_lock_source(),
            RuntimePathSource::Default
        );
        assert_eq!(config.paths().log_source(), RuntimePathSource::Default);
        assert!(!config.paths().product_home().exists());
        config
            .native_process("daemon-epoch")
            .expect("validated native process config");
        assert_eq!(config.health_checks().workers, 8);
        assert_eq!(config.health_checks().queue_capacity, 512);
        assert_eq!(config.health_checks().screen_cells, 500 * 500);
        assert_eq!(config.health_checks().screen_text_bytes, 131_072);
        assert_eq!(config.health_checks().pattern_bytes, 8_192);
        let hello = config
            .client()
            .hello(ClientId::new("config-test").expect("client ID"))
            .expect("client hello");
        assert_eq!(
            hello.limits().max_logical_streams(),
            config.transport().policy().limits().max_logical_streams()
        );
        assert_eq!(
            config
                .client()
                .product()
                .client_limits()
                .stream_mailbox_bytes(),
            262_144
        );
        assert_eq!(config.client().bulk_parallel_requests(), 16);
        assert_eq!(config.services().max_query_page_size(), 256);
        assert!(config.local_endpoint().path().ends_with("run/control.sock"));
    }

    #[test]
    fn redacted_report_retains_settings_and_never_retains_path_values() {
        let home = TempDir::new().expect("temporary home");
        let discovery = ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(home.path()));
        let config = load_daemon_config(
            &discovery,
            DaemonConfigOverrides::new()
                .with_database_path(home.path().join("private/database.sqlite"))
                .with_instance_lock_path(home.path().join("private/daemon.lock")),
        )
        .expect("resolved config");

        let report = config.redacted_report();

        assert_eq!(report.path_sources.database, RuntimePathSource::Explicit);
        assert_eq!(
            report.path_sources.instance_lock,
            RuntimePathSource::Explicit
        );
        assert_eq!(report.path_sources.log, RuntimePathSource::Default);
        assert_eq!(
            report.undercurrent.persistence.max_sessions,
            config.sqlite().limits().max_sessions()
        );
        assert_eq!(
            serde_json::to_value(&report.undercurrent.paths).unwrap(),
            serde_json::json!({})
        );
        let rendered = format!("{report:?}");
        assert!(!rendered.contains("/private/database.sqlite"));
        assert!(!rendered.contains("/private/daemon.lock"));
        assert!(!rendered.contains(&home.path().display().to_string()));
    }

    #[test]
    fn explicit_embedding_paths_remain_available() {
        let temp = TempDir::new().expect("temporary directory");
        let config_path = temp.path().join("config.toml");
        let explicit_database = temp.path().join("explicit.db");
        let explicit_instance_lock = temp.path().join("explicit.lock");
        fs::write(&config_path, "schema_version = \"0.1.0-beta\"\n").expect("write config");
        let discovery = ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(temp.path()))
            .with_explicit_path(config_path);
        let overrides = DaemonConfigOverrides::new()
            .with_database_path(&explicit_database)
            .with_instance_lock_path(&explicit_instance_lock);

        let config = load_daemon_config(&discovery, overrides).expect("resolved config");

        assert_eq!(config.paths().database(), explicit_database);
        assert_eq!(config.paths().instance_lock(), explicit_instance_lock);
        assert_eq!(
            config.paths().database_source(),
            RuntimePathSource::Explicit
        );
        assert_eq!(
            config.paths().instance_lock_source(),
            RuntimePathSource::Explicit
        );
    }

    #[test]
    fn configured_state_and_log_directories_are_retired() {
        let temp = TempDir::new().unwrap();
        let config_path = temp.path().join("config.toml");
        fs::write(
            &config_path,
            "[paths]\nstate_dir = 'state'\nlogs_dir = 'logs'\n",
        )
        .unwrap();
        let discovery = ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(temp.path()))
            .with_explicit_path(config_path);
        assert!(matches!(
            load_daemon_config(&discovery, DaemonConfigOverrides::new()),
            Err(DaemonConfigError::Common(ConfigError::RetiredPath))
        ));
    }

    #[test]
    fn logging_resource_bounds_are_validated_without_io() {
        let cases = [
            (
                DaemonLoggingConfig {
                    max_file_bytes: MIN_DAEMON_LOG_FILE_BYTES - 1,
                    ..DaemonLoggingConfig::default()
                },
                "logging max file bytes is below the minimum safe event size",
            ),
            (
                DaemonLoggingConfig {
                    retained_files: 0,
                    ..DaemonLoggingConfig::default()
                },
                "logging retained file count must be non-zero",
            ),
            (
                DaemonLoggingConfig {
                    queue_lines: 0,
                    ..DaemonLoggingConfig::default()
                },
                "logging queue line count must be non-zero",
            ),
        ];

        for (config, expected) in cases {
            let error = DaemonLoggingSettings::new(config).expect_err("invalid logging bounds");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn approved_first_release_retention_defaults_are_stable() {
        let terminal = TerminalRuntimeConfig::default();
        assert_eq!(terminal.per_session_bytes, 2 * 1_024 * 1_024);
        assert_eq!(terminal.global_bytes, 256 * 1_024 * 1_024);

        let transport = DaemonTransportConfig::default();
        assert_eq!(transport.resume_retention_millis, 30_000);

        let requests = DaemonRequestConfig::default();
        assert_eq!(requests.idempotency_retention_millis, 5 * 60 * 1_000);
        assert_eq!(requests.idempotency_records, 1_024);
        assert_eq!(requests.idempotency_total_bytes, 256 * 1_024 * 1_024);
    }

    #[test]
    fn product_values_are_decoded_and_validated() {
        let temp = TempDir::new().expect("temporary directory");
        let config_path = temp.path().join("config.toml");
        fs::write(
            &config_path,
            "schema_version = \"0.1.0-beta\"\n[persistence]\nmax_read_batch = 17\n[workers]\noutbox_batch_size = 17\nrestart_batch_size = 13\nreconciliation_batch_size = 17\noutbox_poll_interval_millis = 41\nprocess_exit_error_backoff_millis = 47\nhealth_check_workers = 3\nhealth_check_queue_capacity = 17\nhealth_check_timeout_millis = 333\nhealth_check_screen_text_bytes = 65_536\nhealth_check_detail_bytes = 64\n[client]\nbulk_parallel_requests = 7\n[services]\nmax_query_page_size = 23\n[cli]\nlist_page_size = 23\n",
        )
        .expect("write config");
        let discovery = ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(temp.path()))
            .with_explicit_path(config_path);

        let config =
            load_daemon_config(&discovery, DaemonConfigOverrides::new()).expect("resolved config");

        assert_eq!(config.sqlite().limits().max_read_batch(), 17);
        assert_eq!(config.restart_batch_size(), 13);
        assert_eq!(config.worker_thread_stack_bytes(), 524_288);
        assert_eq!(config.worker_intervals().outbox, Duration::from_millis(41));
        assert_eq!(
            config.worker_intervals().process_exit_error,
            Duration::from_millis(47)
        );
        assert_eq!(config.health_checks().workers, 3);
        assert_eq!(config.health_checks().queue_capacity, 17);
        assert_eq!(
            config.health_checks().check_timeout,
            Duration::from_millis(333)
        );
        assert_eq!(config.health_checks().detail_bytes, 64);
        assert_eq!(config.health_checks().screen_text_bytes, 65_536);
        assert_eq!(config.client().bulk_parallel_requests(), 7);
        assert_eq!(config.services().max_query_page_size(), 23);
    }

    #[test]
    fn unknown_product_fields_are_rejected() {
        let temp = TempDir::new().expect("temporary directory");
        let config_path = temp.path().join("config.toml");
        fs::write(
            &config_path,
            "schema_version = \"0.1.0-beta\"\nunbounded_queue = true\n",
        )
        .expect("write config");
        let discovery = ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(temp.path()))
            .with_explicit_path(config_path);

        let error = load_daemon_config(&discovery, DaemonConfigOverrides::new())
            .expect_err("unknown field must fail");

        assert!(matches!(
            error,
            DaemonConfigError::Common(ConfigError::UnknownSetting)
        ));
    }

    #[test]
    fn retired_activity_configuration_names_are_rejected() {
        for retired in [
            "[workers]\nactivity_activation_delay_millis = 1\n",
            "[workers]\nactivity_interval_millis = 1\n",
            "[services]\nactivity_evidence_items = 1\n",
            "[services]\nactivity_evidence_age_millis = 1\n",
            "[services]\nactivity_minimum_confidence_basis_points = 1\n",
            "[services]\nactivity_motion_confidence_basis_points = 1\n",
            "[services]\nactivity_server_idle_confidence_basis_points = 1\n",
        ] {
            let temp = TempDir::new().expect("temporary directory");
            let config_path = temp.path().join("config.toml");
            fs::write(
                &config_path,
                format!("schema_version = \"0.1.0-beta\"\n{retired}"),
            )
            .expect("write config");
            let discovery =
                ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(temp.path()))
                    .with_explicit_path(config_path);

            let error = load_daemon_config(&discovery, DaemonConfigOverrides::new())
                .expect_err("retired activity field must fail");

            assert!(matches!(
                error,
                DaemonConfigError::Common(ConfigError::Invalid {
                    rule: "unknown field",
                    ..
                })
            ));
        }
    }

    #[test]
    fn client_mailbox_must_fit_the_negotiated_stream_queue() {
        let temp = TempDir::new().expect("temporary directory");
        let config_path = temp.path().join("config.toml");
        let oversized_mailbox_bytes = DaemonTransportConfig::default()
            .stream_queue_bytes
            .checked_add(1)
            .expect("default stream queue must leave room for an oversized mailbox fixture");
        fs::write(
            &config_path,
            format!(
                "schema_version = \"0.1.0-beta\"\n[client]\nstream_mailbox_bytes = {oversized_mailbox_bytes}\n"
            ),
        )
        .expect("write config");
        let discovery = ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(temp.path()))
            .with_explicit_path(config_path);

        let error = load_daemon_config(&discovery, DaemonConfigOverrides::new())
            .expect_err("oversized client stream mailbox must fail");

        assert_eq!(
            error.to_string(),
            "failed to load daemon config: invalid configuration client.stream_mailbox_bytes: must fit stream queue"
        );
    }

    #[test]
    fn client_bulk_parallelism_must_fit_negotiated_and_local_queues() {
        let temp = TempDir::new().expect("temporary directory");
        let config_path = temp.path().join("config.toml");
        fs::write(
            &config_path,
            "schema_version = \"0.1.0-beta\"\n[transport]\nmax_pending_requests = 4\n[client]\nbulk_parallel_requests = 5\n",
        )
        .expect("write config");
        let discovery = ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(temp.path()))
            .with_explicit_path(config_path);

        let error = load_daemon_config(&discovery, DaemonConfigOverrides::new())
            .expect_err("oversized bulk concurrency must fail");

        assert_eq!(
            error.to_string(),
            "failed to load daemon config: invalid configuration client.bulk_parallel_requests: must fit writer queue and pending request limits"
        );
    }

    #[test]
    fn relative_daemon_paths_are_rejected_without_exposing_value() {
        let temp = TempDir::new().expect("temporary directory");
        let discovery = ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(temp.path()));

        let error = load_daemon_config(
            &discovery,
            DaemonConfigOverrides::new().with_instance_lock_path("private/daemon.lock"),
        )
        .expect_err("relative instance-lock path must fail");

        assert_eq!(
            error.to_string(),
            "daemon path instance_lock must be absolute"
        );
        assert!(!error.to_string().contains("private/daemon.sock"));
    }

    #[test]
    fn runtime_paths_must_be_distinct() {
        let temp = TempDir::new().expect("temporary directory");
        let discovery = ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(temp.path()));
        let shared = temp.path().join("daemon.node");

        let error = load_daemon_config(
            &discovery,
            DaemonConfigOverrides::new()
                .with_database_path(&shared)
                .with_instance_lock_path(shared),
        )
        .expect_err("shared runtime node must fail");

        assert_eq!(
            error.to_string(),
            "daemon database, instance lock, private host socket, and structured log paths must be distinct"
        );

        let session_host = temp
            .path()
            .join(".aixand/undercurrent/run/session-host.sock");
        let error = load_daemon_config(
            &discovery,
            DaemonConfigOverrides::new().with_instance_lock_path(session_host),
        )
        .expect_err("public and session-host sockets must differ");
        assert_eq!(
            error.to_string(),
            "daemon database, instance lock, private host socket, and structured log paths must be distinct"
        );
    }

    #[test]
    fn worker_batches_must_fit_the_repository_read_bound() {
        let temp = TempDir::new().expect("temporary directory");
        let config_path = temp.path().join("config.toml");
        fs::write(
            &config_path,
            "schema_version = \"0.1.0-beta\"\n[persistence]\nmax_read_batch = 8\n[workers]\noutbox_batch_size = 9\nreconciliation_batch_size = 8\n",
        )
        .expect("write config");
        let discovery = ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(temp.path()))
            .with_explicit_path(config_path);

        let error = load_daemon_config(&discovery, DaemonConfigOverrides::new())
            .expect_err("oversized worker batch must fail");

        assert_eq!(
            error.to_string(),
            "failed to load daemon config: invalid configuration workers.outbox_batch_size: must fit persistence read batch"
        );
    }

    #[test]
    fn restart_batch_must_be_nonzero_and_fit_the_queue() {
        let temp = TempDir::new().expect("temporary directory");
        let config_path = temp.path().join("config.toml");
        fs::write(
            &config_path,
            "schema_version = \"0.1.0-beta\"\n[workers]\nrestart_batch_size = 0\n",
        )
        .expect("write config");
        let discovery = ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(temp.path()))
            .with_explicit_path(&config_path);

        let error = load_daemon_config(&discovery, DaemonConfigOverrides::new())
            .expect_err("zero restart batch must fail");
        assert!(matches!(
            error,
            DaemonConfigError::Common(ConfigError::Invalid { ref field, .. }) if field == "workers.restart_batch_size"
        ));
        assert!(Error::source(&error).is_some());

        fs::write(
            &config_path,
            "schema_version = \"0.1.0-beta\"\n[workers]\nrestart_batch_size = 9\n[services]\nrestart_jobs = 8\n",
        )
        .expect("rewrite config");
        let error = load_daemon_config(&discovery, DaemonConfigOverrides::new())
            .expect_err("oversized restart batch must fail");
        assert_eq!(
            error.to_string(),
            "failed to load daemon config: invalid configuration workers.restart_batch_size: must fit restart queue"
        );
    }

    #[test]
    fn invalid_component_limits_preserve_their_source() {
        let temp = TempDir::new().expect("temporary directory");
        let config_path = temp.path().join("config.toml");
        fs::write(
            &config_path,
            "schema_version = \"0.1.0-beta\"\n[process]\nspawn_workers = 0\n",
        )
        .expect("write config");
        let discovery = ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(temp.path()))
            .with_explicit_path(config_path);

        let error = load_daemon_config(&discovery, DaemonConfigOverrides::new())
            .expect_err("zero worker count must fail");

        assert!(matches!(
            error,
            DaemonConfigError::Common(ConfigError::Invalid { ref field, .. }) if field == "process.spawn_workers"
        ));
        assert!(Error::source(&error).is_some());
    }

    #[test]
    fn health_worker_count_cannot_exceed_process_capacity() {
        let temp = TempDir::new().expect("temporary directory");
        let config_path = temp.path().join("config.toml");
        fs::write(
            &config_path,
            "schema_version = \"0.1.0-beta\"\n[process]\nmax_processes = 4\nspawn_workers = 4\n[workers]\nhealth_check_workers = 5\n[services]\nobservation_jobs = 8\n",
        )
        .expect("write config");
        let discovery = ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(temp.path()))
            .with_explicit_path(config_path);

        let error = load_daemon_config(&discovery, DaemonConfigOverrides::new())
            .expect_err("excess health workers must fail");

        assert_eq!(
            error.to_string(),
            "failed to load daemon config: invalid configuration workers.health_check_workers: must fit process capacity"
        );
    }

    #[test]
    fn regex_input_capacity_must_cover_health_screen_samples() {
        let temp = TempDir::new().expect("temporary directory");
        let config_path = temp.path().join("config.toml");
        fs::write(
            &config_path,
            "schema_version = \"0.1.0-beta\"\n[workers]\nhealth_check_screen_text_bytes = 131_073\n",
        )
        .expect("write config");
        let discovery = ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(temp.path()))
            .with_explicit_path(config_path);

        let error = load_daemon_config(&discovery, DaemonConfigOverrides::new())
            .expect_err("undersized health regex input must fail");

        assert_eq!(
            error.to_string(),
            "failed to load daemon config: invalid configuration services.regex_match_text_bytes: must contain a health screen sample"
        );
    }

    #[test]
    fn debug_output_redacts_runtime_paths() {
        let home = TempDir::new().expect("temporary home");
        let discovery = ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(home.path()));
        let secret_component = "private-user-component";
        let database = home.path().join(secret_component).join("state.db");
        let config = load_daemon_config(
            &discovery,
            DaemonConfigOverrides::new().with_database_path(database),
        )
        .expect("resolved config");

        let rendered = format!("{config:?}");

        assert!(!rendered.contains(secret_component));
        assert!(rendered.contains("<redacted>"));
    }

    #[test]
    fn environment_variable_registry_is_complete_and_stable() {
        assert_eq!(
            DaemonEnvironmentVariable::ALL.map(DaemonEnvironmentVariable::as_str),
            ["UNDERCURRENT_HOME"]
        );
        let overrides = DaemonConfigOverrides {
            database_path: Some(PathBuf::from(OsString::from("database"))),
            instance_lock_path: Some(PathBuf::from(OsString::from("instance-lock"))),
        };
        let rendered = format!("{overrides:?}");
        assert!(!rendered.contains("database\"") && !rendered.contains("instance-lock\""));
    }
}
