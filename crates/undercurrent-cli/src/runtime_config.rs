// SPDX-License-Identifier: Apache-2.0

//! CLI-owned standalone connection, path, and orchestration configuration.

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Serialize;

use crate::bootstrap_config::{ConfigDiscovery, ConfigError, ConfigSource, load_config};
use crate::bootstrap_paths::{PathsError, UndercurrentPaths};

/// Safe source category for one resolved CLI configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CliConfigSourceKind {
    /// No configuration file was found.
    Defaults,
    /// An explicit configuration path was selected.
    Explicit,
    /// `UNDERCURRENT_CONFIG` selected the file.
    Environment,
    /// The product-owned default configuration file was selected.
    UndercurrentHome,
}

/// Local IPC placement used by the standalone CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CliConnectionPlacement {
    /// OS-user-authenticated local IPC.
    Local,
}

/// Validated local endpoint retained at the SDK boundary.
#[derive(Clone, PartialEq, Eq)]
pub struct CliConnectionTarget {
    endpoint: PathBuf,
}
impl Debug for CliConnectionTarget {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("CliConnectionTarget([REDACTED])")
    }
}
impl CliConnectionTarget {
    /// Returns the absolute platform-local endpoint.
    pub fn endpoint(&self) -> &Path {
        &self.endpoint
    }
    /// Returns the single supported placement.
    pub const fn placement(&self) -> CliConnectionPlacement {
        CliConnectionPlacement::Local
    }
}

/// Path-redacted effective configuration shown by `uc config show`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RedactedCliConfigReport {
    /// Complete redacted on-disk snapshot; not running daemon state.
    pub effective: serde_json::Value,
    /// Configuration source category.
    pub source: CliConfigSourceKind,
    /// Network placement without credentials or host path values.
    pub placement: CliConnectionPlacement,
    /// Maximum sessions selected by one bulk command.
    pub max_sessions: usize,
    /// Maximum concurrent bulk requests.
    pub bulk_parallel_requests: usize,
    /// Maximum request payload bytes read from standard input.
    pub request_bytes: usize,
}

/// CLI-owned resolved configuration; no daemon implementation type crosses this boundary.
#[derive(Clone, PartialEq, Eq)]
pub struct ResolvedCliConfig {
    document: undercurrent_config::UndercurrentProductConfig,
    default_profile: Option<String>,
    report: serde_json::Value,
    source: CliConfigSourceKind,
    product_home: PathBuf,
    database: PathBuf,
    instance_lock: PathBuf,
    log: PathBuf,
    retained_log_files: usize,
    connection: CliConnectionTarget,
    maximum_drain_timeout: Duration,
    worker_thread_stack_bytes: usize,
    request_bytes: usize,
    max_sessions: usize,
    bulk_parallel_requests: usize,
    max_query_page_size: u32,
    max_terminal_columns: u16,
    max_terminal_rows: u16,
    max_input_items: usize,
    max_input_bytes: usize,
    terminal_replay_bytes: u32,
}

impl Debug for ResolvedCliConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResolvedCliConfig")
            .field("source", &self.source)
            .field("connection", &self.connection)
            .field("paths", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

impl ResolvedCliConfig {
    /// Detection profile applied by `uc start` when no explicit profile is given.
    pub fn default_profile(&self) -> Option<&str> {
        self.default_profile.as_deref()
    }

    /// Returns the shared validated product settings.
    pub fn document(&self) -> &undercurrent_config::UndercurrentProductConfig {
        &self.document
    }
    /// Returns the startup deadline used by the native lifecycle adapter.
    pub fn startup_timeout(&self) -> Duration {
        Duration::from_millis(self.document.cli.daemon_startup_timeout_millis)
    }

    /// Returns the safe configuration source category.
    pub const fn source(&self) -> CliConfigSourceKind {
        self.source
    }
    /// Returns the product-owned runtime directory.
    pub fn product_home(&self) -> &Path {
        &self.product_home
    }
    /// Returns the product-owned SQLite path.
    pub fn database(&self) -> &Path {
        &self.database
    }
    /// Returns the daemon instance-lock scope shared with the daemon process.
    pub fn instance_lock(&self) -> &Path {
        &self.instance_lock
    }
    /// Returns the product-owned structured log path.
    pub fn log(&self) -> &Path {
        &self.log
    }
    /// Returns retained structured log generations.
    pub const fn retained_log_files(&self) -> usize {
        self.retained_log_files
    }
    /// Returns the home-derived authenticated local IPC endpoint selection.
    pub const fn connection(&self) -> &CliConnectionTarget {
        &self.connection
    }
    /// Returns maximum daemon drain accepted by CLI lifecycle orchestration.
    pub const fn maximum_drain_timeout(&self) -> Duration {
        self.maximum_drain_timeout
    }
    /// Returns worker thread stack bytes for stream signal adapters.
    pub const fn worker_thread_stack_bytes(&self) -> usize {
        self.worker_thread_stack_bytes
    }
    /// Returns maximum request bytes read by the CLI.
    pub const fn request_bytes(&self) -> usize {
        self.request_bytes
    }
    /// Returns maximum selected sessions.
    pub const fn max_sessions(&self) -> usize {
        self.max_sessions
    }
    /// Returns maximum concurrent bulk requests.
    pub const fn bulk_parallel_requests(&self) -> usize {
        self.bulk_parallel_requests
    }
    /// Returns maximum query page size.
    pub const fn max_query_page_size(&self) -> u32 {
        self.max_query_page_size
    }
    /// Returns maximum terminal columns.
    pub const fn max_terminal_columns(&self) -> u16 {
        self.max_terminal_columns
    }
    /// Returns maximum terminal rows.
    pub const fn max_terminal_rows(&self) -> u16 {
        self.max_terminal_rows
    }
    /// Returns maximum terminal input items.
    pub const fn max_input_items(&self) -> usize {
        self.max_input_items
    }
    /// Returns maximum terminal input bytes.
    pub const fn max_input_bytes(&self) -> usize {
        self.max_input_bytes
    }
    /// Returns maximum terminal replay page bytes.
    pub const fn terminal_replay_bytes(&self) -> u32 {
        self.terminal_replay_bytes
    }

    /// Returns the complete path-redacted effective report.
    pub fn redacted_report(&self) -> RedactedCliConfigReport {
        RedactedCliConfigReport {
            effective: self.report.clone(),
            source: self.source,
            placement: self.connection.placement(),
            max_sessions: self.max_sessions,
            bulk_parallel_requests: self.bulk_parallel_requests,
            request_bytes: self.request_bytes,
        }
    }
}

/// Failure to resolve the CLI-owned standalone configuration.
pub enum CliConfigurationError {
    /// Common-document discovery, decoding, or validation failed.
    Bootstrap(ConfigError),
    /// Product path resolution failed.
    Paths(PathsError),
    /// One required runtime path was not absolute.
    RelativePath {
        /// Stable path field name; the configured value remains redacted.
        field: &'static str,
    },
    /// One transport environment variable was present but empty or invalid.
    InvalidEnvironment {
        /// Stable environment variable name; its value remains redacted.
        name: &'static str,
    },
    /// A compiled SDK profile invariant failed.
    InvalidStaticProfile {
        /// Stable SDK profile component that violated its compiled invariant.
        component: &'static str,
    },
    /// Local endpoint is relative, traversing, or too long.
    InvalidLocalEndpoint,
}

impl Debug for CliConfigurationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bootstrap(_) => formatter.write_str("Bootstrap([REDACTED])"),
            Self::Paths(_) => formatter.write_str("Paths([REDACTED])"),
            Self::RelativePath { field } => formatter
                .debug_struct("RelativePath")
                .field("field", field)
                .finish(),
            Self::InvalidEnvironment { name } => formatter
                .debug_struct("InvalidEnvironment")
                .field("name", name)
                .finish(),
            Self::InvalidStaticProfile { component } => formatter
                .debug_struct("InvalidStaticProfile")
                .field("component", component)
                .finish(),
            Self::InvalidLocalEndpoint => formatter.write_str("InvalidLocalEndpoint"),
        }
    }
}

impl Display for CliConfigurationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bootstrap(source) => Display::fmt(source, formatter),
            Self::Paths(_) => formatter.write_str("standalone runtime paths could not be resolved"),
            Self::RelativePath { field } => {
                write!(formatter, "configuration path {field} must be absolute")
            }
            Self::InvalidEnvironment { name } => {
                write!(
                    formatter,
                    "environment variable {name} is invalid or retired; use UNDERCURRENT_HOME for local IPC"
                )
            }
            Self::InvalidStaticProfile { component } => {
                write!(formatter, "compiled client profile {component} is invalid")
            }
            Self::InvalidLocalEndpoint => write!(
                formatter,
                "local endpoint must be an absolute, non-traversing path without NUL bytes, shorter than {} bytes; choose a shorter absolute UNDERCURRENT_HOME (this selects a different state directory)",
                if cfg!(target_os = "macos") { 104 } else { 108 }
            ),
        }
    }
}

impl Error for CliConfigurationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Bootstrap(source) => Some(source),
            Self::Paths(source) => Some(source),
            Self::RelativePath { .. }
            | Self::InvalidEnvironment { .. }
            | Self::InvalidStaticProfile { .. }
            | Self::InvalidLocalEndpoint => None,
        }
    }
}

impl From<ConfigError> for CliConfigurationError {
    fn from(source: ConfigError) -> Self {
        Self::Bootstrap(source)
    }
}

impl From<PathsError> for CliConfigurationError {
    fn from(source: PathsError) -> Self {
        Self::Paths(source)
    }
}

/// Resolves CLI-owned configuration from one discovery policy.
pub fn load_cli_config(
    discovery: &ConfigDiscovery,
) -> Result<ResolvedCliConfig, CliConfigurationError> {
    let loaded = load_config(discovery)?;
    let report = loaded.redacted_report();
    let product = loaded.settings;
    let paths = UndercurrentPaths::resolve(&discovery.path_env)?;
    let product_home = paths.home().to_path_buf();
    let database = paths.state_dir().join("undercurrent.db");
    let instance_lock = paths.run_dir().join("daemon.lock");
    let log = paths.logs_dir().join("daemon.jsonl");
    require_absolute("database", &database)?;
    require_absolute("instance_lock", &instance_lock)?;
    require_absolute("log", &log)?;
    for name in [
        "UNDERCURRENT_ENDPOINT",
        "UNDERCURRENT_TOKEN_FILE",
        "UNDERCURRENT_TLS_CA_FILE",
        "UNDERCURRENT_TLS_CLIENT_CERT_FILE",
        "UNDERCURRENT_TLS_CLIENT_KEY_FILE",
    ] {
        if std::env::var_os(name).is_some() {
            return Err(CliConfigurationError::InvalidEnvironment { name });
        }
    }
    let endpoint = paths.run_dir().join("control.sock");
    undercurrent_sdk::LocalClientConfig::validate_endpoint(endpoint.clone())
        .map_err(|_| CliConfigurationError::InvalidLocalEndpoint)?;
    Ok(ResolvedCliConfig {
        document: product.clone(),
        default_profile: loaded.config.default_profile,
        report,
        source: source_kind(&loaded.source),
        product_home,
        database,
        instance_lock,
        log,
        retained_log_files: product.logging.retained_files,
        connection: CliConnectionTarget { endpoint },
        maximum_drain_timeout: Duration::from_millis(
            product.requests.maximum_shutdown_drain_millis,
        ),
        worker_thread_stack_bytes: product.process.worker_thread_stack_bytes,
        request_bytes: product.requests.request_bytes as usize,
        max_sessions: product.persistence.max_sessions,
        bulk_parallel_requests: product.client.bulk_parallel_requests,
        max_query_page_size: product.services.max_query_page_size as u32,
        max_terminal_columns: product.services.max_terminal_columns,
        max_terminal_rows: product.services.max_terminal_rows,
        max_input_items: product.services.max_input_items,
        max_input_bytes: product.services.max_input_bytes,
        terminal_replay_bytes: product.terminal.replay_bytes as u32,
    })
}

fn source_kind(source: &ConfigSource) -> CliConfigSourceKind {
    match source {
        ConfigSource::Defaults => CliConfigSourceKind::Defaults,
        ConfigSource::Explicit(_) => CliConfigSourceKind::Explicit,
        ConfigSource::Environment(_) => CliConfigSourceKind::Environment,
        ConfigSource::UndercurrentHome(_) => CliConfigSourceKind::UndercurrentHome,
    }
}

fn require_absolute(field: &'static str, path: &Path) -> Result<(), CliConfigurationError> {
    path.is_absolute()
        .then_some(())
        .ok_or(CliConfigurationError::RelativePath { field })
}
