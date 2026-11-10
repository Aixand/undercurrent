// SPDX-License-Identifier: Apache-2.0

//! Standalone Undercurrent configuration discovery and bounded document loading.
//!
//! Home discovery selects one main file, never layers formats or falls back from
//! invalid content. The loader then expands defaults, applies shared constraints before returning an owned
//! snapshot. It performs no writes, component construction, or live reload.

#![deny(missing_docs)]

use crate::UndercurrentProductConfig;
use crate::paths::{PathEnvironment, PathsError, UndercurrentPaths};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

/// Retired standalone configuration-file environment variable.
///
/// Capture is retained in discovery inputs, but [`load_config`] rejects its
/// presence, even if an explicit file was supplied. Select a complete home via
/// [`crate::UNDERCURRENT_HOME_ENV`] for shipped binaries.
pub const UNDERCURRENT_CONFIG_ENV: &str = "UNDERCURRENT_CONFIG";
/// Current standalone configuration release, serialized as `0.1.0-beta`.
pub const CURRENT_CONFIG_SCHEMA_VERSION: undercurrent_foundation::ReleaseVersion =
    undercurrent_foundation::ReleaseVersion::Current;

/// Bootstrap settings shared by CLI and daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapConfig {
    /// Configuration release identity; omitted values use the current release.
    #[serde(default = "default_schema_version")]
    pub schema_version: undercurrent_foundation::ReleaseVersion,
    /// Optional default detection profile preference.
    ///
    /// Loading does not resolve the name or verify that a profile exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_profile: Option<String>,
    /// Reserved path table; storage derives from the selected home.
    #[serde(default)]
    pub paths: ConfigPaths,
    /// Product telemetry preferences.
    #[serde(default)]
    pub telemetry: TelemetryConfig,
}
impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            schema_version: CURRENT_CONFIG_SCHEMA_VERSION,
            default_profile: None,
            paths: ConfigPaths::default(),
            telemetry: TelemetryConfig::default(),
        }
    }
}
/// Reserved path table; storage locations derive exclusively from the home.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigPaths {}
/// Source that selected a resolved runtime path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimePathSource {
    /// Caller supplied an explicit command or API override.
    Explicit,
    /// The common or product config supplied the path.
    Config,
    /// A project-local runtime directory supplied the path.
    Project,
    /// The product's common home supplied the fallback.
    Default,
}

/// One runtime path and the precedence source that selected it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedRuntimePath {
    /// Selected filesystem path.
    pub path: PathBuf,
    /// Source that won precedence resolution.
    pub source: RuntimePathSource,
}

/// Candidate paths ordered by explicit, config, project, then default.
///
/// This lexical helper performs no IO or path validation. Its candidate kinds
/// do not enable document path overrides or project configuration discovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePathCandidates {
    explicit: Option<PathBuf>,
    configured: Option<PathBuf>,
    project: Option<PathBuf>,
    default: PathBuf,
}

impl RuntimePathCandidates {
    /// Creates path candidates with a required default fallback.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn new(default: impl Into<PathBuf>) -> Self {
        Self {
            explicit: None,
            configured: None,
            project: None,
            default: default.into(),
        }
    }

    /// Adds an explicit command or API override.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn with_explicit(mut self, path: Option<impl Into<PathBuf>>) -> Self {
        self.explicit = path.map(Into::into);
        self
    }

    /// Adds a configured path.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn with_config(mut self, path: Option<impl Into<PathBuf>>) -> Self {
        self.configured = path.map(Into::into);
        self
    }

    /// Adds a project-local candidate.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn with_project(mut self, path: Option<impl Into<PathBuf>>) -> Self {
        self.project = path.map(Into::into);
        self
    }

    /// Resolves candidates using explicit, config, project, then default order.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn resolve(self) -> ResolvedRuntimePath {
        if let Some(path) = self.explicit {
            return ResolvedRuntimePath {
                path,
                source: RuntimePathSource::Explicit,
            };
        }
        if let Some(path) = self.configured {
            return ResolvedRuntimePath {
                path,
                source: RuntimePathSource::Config,
            };
        }
        if let Some(path) = self.project {
            return ResolvedRuntimePath {
                path,
                source: RuntimePathSource::Project,
            };
        }
        ResolvedRuntimePath {
            path: self.default,
            source: RuntimePathSource::Default,
        }
    }
}

/// Common telemetry preferences.
///
/// Both an omitted section and omitted fields use [`Self::default`]: diagnostics
/// enabled and debug disabled. Explicit field values override those defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TelemetryConfig {
    /// Enables diagnostic capture where supported by the product.
    pub diagnostics_enabled: bool,
    /// Enables debug-level observability where supported by the product.
    pub debug_enabled: bool,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            diagnostics_enabled: true,
            debug_enabled: false,
        }
    }
}

/// Source of the selected configuration document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfigSource {
    /// No default configuration exists.
    Defaults,
    /// Internal caller-selected file path.
    Explicit(PathBuf),
    /// Retained source shape for an environment-selected file.
    ///
    /// The current loader rejects `UNDERCURRENT_CONFIG` and never returns this
    /// variant through discovery.
    Environment(PathBuf),
    /// The product-owned default configuration file.
    UndercurrentHome(PathBuf),
}
impl ConfigSource {
    /// Returns the selected file, if any.
    pub fn path(&self) -> Option<&Path> {
        match self {
            Self::Defaults => None,
            Self::Explicit(path) | Self::Environment(path) | Self::UndercurrentHome(path) => {
                Some(path)
            }
        }
    }
}
/// Bootstrap settings and validated product settings from one load.
///
/// Public fields may be mutated after loading; validation applies to the
/// snapshot returned by [`load_config`]. `Debug` hides settings but includes
/// the source path. Use [`Self::redacted_report`] for a path-free report.
#[derive(Clone, PartialEq)]
pub struct LoadedConfig {
    /// Parsed bootstrap preferences.
    pub config: BootstrapConfig,
    /// Discovery source.
    pub source: ConfigSource,
    /// Validated complete runtime document.
    pub settings: UndercurrentProductConfig,
}
impl std::fmt::Debug for LoadedConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LoadedConfig")
            .field("schema_version", &self.config.schema_version)
            .field("source", &self.source)
            .field("settings", &"[REDACTED]")
            .finish()
    }
}
impl LoadedConfig {
    /// Reports default-expanded settings, application timing, and prefix provenance.
    ///
    /// This describes on-disk effective configuration, not a running daemon.
    /// It omits discovery paths and masks `default_profile`, but includes normal
    /// product preferences verbatim; new sensitive fields need explicit review.
    pub fn redacted_report(&self) -> serde_json::Value {
        let mut report = serde_json::to_value(&self.settings).expect("serializable config");
        let object = report.as_object_mut().expect("config object");
        object.insert("schema_version".into(), self.config.schema_version.into());
        object.insert(
            "telemetry".into(),
            serde_json::to_value(&self.config.telemetry).expect("telemetry"),
        );
        if self.config.default_profile.is_some() {
            object.insert("default_profile".into(), "[REDACTED]".into());
        }
        serde_json::json!({"settings": report, "scope": "on_disk_effective_configuration",
            "applies": "next CLI invocation or daemon restart", "cli_connection_profile": "compiled_standard",
            "prefix_source": "main_config_or_default"})
    }

    /// Projects product settings through JSON without exposing decode details.
    ///
    /// Bootstrap fields are excluded. This performs no IO and does not revalidate
    /// potentially modified settings. Incompatible target types return
    /// [`ConfigError::Invalid`] with field `configuration` and a fixed rule.
    pub fn runtime_config<T: DeserializeOwned>(&self) -> Result<T, ConfigError> {
        serde_json::from_value(serde_json::to_value(&self.settings).expect("serializable config"))
            .map_err(|_| ConfigError::Invalid {
                field: "configuration".into(),
                rule: "could not decode runtime projection",
            })
    }
}
/// Supplied inputs for configuration discovery.
///
/// Constructing these explicitly avoids environment capture, but [`load_config`]
/// still checks the actual process for retired environment variables.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConfigDiscovery {
    /// Optional internal caller-selected configuration file.
    pub explicit_path: Option<PathBuf>,
    /// Retained `UNDERCURRENT_CONFIG` input; any `Some` value is rejected.
    pub undercurrent_config_env: Option<PathBuf>,
    /// Home inputs and XDG roots used only for legacy-layout detection.
    pub path_env: PathEnvironment,
}
impl ConfigDiscovery {
    /// Captures the environment and an optional explicit file.
    pub fn from_process_env(explicit_path: Option<PathBuf>) -> Self {
        Self {
            explicit_path,
            undercurrent_config_env: std::env::var_os(UNDERCURRENT_CONFIG_ENV).map(PathBuf::from),
            path_env: PathEnvironment::from_process_env(),
        }
    }
    /// Creates discovery inputs without reading the process environment.
    pub fn from_path_env(path_env: PathEnvironment) -> Self {
        Self {
            path_env,
            ..Self::default()
        }
    }
    /// Selects an internal explicit file after the retired-environment guard.
    ///
    /// This bypasses home discovery and legacy-layout scanning. Only the selected
    /// main file is loaded; relative paths use the working directory.
    pub fn with_explicit_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.explicit_path = Some(path.into());
        self
    }
    /// Supplies a retired environment-selected file input, rejected when loaded.
    pub fn with_undercurrent_config_env(mut self, path: impl Into<PathBuf>) -> Self {
        self.undercurrent_config_env = Some(path.into());
        self
    }
}
/// Failure to discover, read, decode, validate, or project configuration.
///
/// `Display` avoids source values and file paths. Derived `Debug` and typed
/// payloads can contain paths or underlying errors; they are not redacted reports.
#[derive(Debug)]
pub enum ConfigError {
    /// Multiple active main files would make the selected configuration ambiguous.
    Ambiguous {
        /// Conflicting file names, without their parent directories.
        files: Vec<String>,
    },
    /// A field or document violates the configuration contract.
    Invalid {
        /// Safe schema field or format name.
        field: String,
        /// Explanation that never contains source values.
        rule: &'static str,
    },
    /// Product path resolution failed.
    Paths(PathsError),
    /// Reading the configuration failed.
    Io {
        /// Selected file.
        path: PathBuf,
        /// Underlying failure.
        source: std::io::Error,
    },
    /// Retained TOML parse-error shape; the current loader emits fixed `Invalid`
    /// errors instead of exposing parser details through this variant.
    Parse {
        /// Selected file.
        path: PathBuf,
        /// Underlying decoding failure.
        source: toml::de::Error,
    },
    /// Retained runtime parse-error shape; current JSON projections emit `Invalid`.
    RuntimeParse {
        /// Underlying decoding failure.
        source: toml::de::Error,
    },
    /// The root contains an unsupported setting.
    UnknownSetting,
    /// An independent storage override is retired.
    RetiredPath,
    /// Unsupported storage layout is rejected without importing or moving it.
    LegacyLayout,
}
impl Display for ConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ambiguous { files } => write!(formatter, "multiple configuration files: {}; keep one active main file", files.join(", ")),
            Self::Invalid { field, rule } => write!(formatter, "invalid configuration {field}: {rule}"),
            Self::LegacyLayout => formatter.write_str("legacy Undercurrent storage detected or unreadable; preserve state and diagnose offline using docs/guides/configuration/diagnose.md#resolve-a-legacy-home-rejection"),
            Self::RetiredPath => formatter.write_str("independent storage paths are retired; use UNDERCURRENT_HOME; see docs/guides/configuration/README.md"),
            Self::Paths(source) => source.fmt(formatter),
            Self::Io { .. } => formatter.write_str("configuration file could not be read"),
            Self::Parse { .. } | Self::RuntimeParse { .. } => {
                formatter.write_str("configuration settings could not be decoded")
            }
            Self::UnknownSetting => {
                formatter.write_str("configuration contains an unknown root setting; use supported schema_version = \"0.1.0-beta\" settings and home-derived local IPC; see docs/guides/configuration/diagnose.md#diagnose-a-rejection")
            }
        }
    }
}
impl Error for ConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Paths(source) => Some(source),
            Self::Io { source, .. } => Some(source),
            Self::Parse { source, .. } | Self::RuntimeParse { source } => Some(source),
            Self::Ambiguous { .. }
            | Self::Invalid { .. }
            | Self::LegacyLayout
            | Self::RetiredPath
            | Self::UnknownSetting => None,
        }
    }
}
impl From<PathsError> for ConfigError {
    fn from(source: PathsError) -> Self {
        Self::Paths(source)
    }
}

/// Loads and validates one configuration snapshot without writing files.
///
/// After rejecting retired environment inputs, an explicit file bypasses home
/// discovery. Otherwise the loader resolves the home, checks legacy markers,
/// and requires at most one of `config.yaml`, `config.yml`, and `config.toml`.
/// No main file means defaults; invalid selected content never falls back.
/// Only the main document supplies shortcut preferences; omitted values use defaults.
///
/// Reads are synchronous and byte-bounded, without a cancellation token,
/// deadline, filesystem lock, or atomic filesystem snapshot.
/// No runtime components are created and no live reload is scheduled.
///
/// # Errors
///
/// Returns discovery/path errors, ambiguous or unreadable files, retired inputs,
/// unsupported schema generations, or invalid document/relationship constraints.
/// Component adapters may impose additional path and component checks afterward.
pub fn load_config(discovery: &ConfigDiscovery) -> Result<LoadedConfig, ConfigError> {
    for name in [
        "UNDERCURRENT_CONFIG",
        "UNDERCURRENT_DATABASE_PATH",
        "UNDERCURRENT_INSTANCE_LOCK_PATH",
        "UNDERCURRENT_LOCAL_ENDPOINT",
    ] {
        if std::env::var_os(name).is_some() {
            return Err(ConfigError::RetiredPath);
        }
    }
    if discovery.undercurrent_config_env.is_some() {
        return Err(ConfigError::RetiredPath);
    }
    if let Some(path) = &discovery.explicit_path {
        return read_config(path, ConfigSource::Explicit(path.clone()));
    }
    let paths = UndercurrentPaths::resolve(&discovery.path_env)?;
    paths
        .check_legacy_layout(&discovery.path_env)
        .map_err(|_| ConfigError::LegacyLayout)?;
    let mut candidates = Vec::new();
    for name in ["config.yaml", "config.yml", "config.toml"] {
        let path = paths.home().join(name);
        // Count dangling links and directories too: a broken configured entry must
        // fail the load rather than silently select defaults or another format.
        // See [discovery policy](../docs/internals/loading.md#discovery-is-selection-not-merging).
        match fs::symlink_metadata(&path) {
            Ok(_) => candidates.push(path),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => return Err(ConfigError::Io { path, source }),
        }
    }
    match candidates.as_slice() {
        [] => finish_config(serde_json::json!({}), ConfigSource::Defaults, paths.home()),
        [path] => read_config(path, ConfigSource::UndercurrentHome(path.clone())),
        _ => Err(ConfigError::Ambiguous {
            files: candidates
                .iter()
                .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
                .collect(),
        }),
    }
}

/// Maximum main-document bytes read from disk.
pub const MAX_CONFIG_BYTES: u64 = 1_048_576;

fn read_bounded(path: &Path, maximum: u64) -> Result<String, ConfigError> {
    if !fs::metadata(path)
        .map_err(|source| ConfigError::Io {
            path: path.into(),
            source,
        })?
        .is_file()
    {
        return Err(invalid("document", "must be a regular file"));
    }
    let file = fs::File::open(path).map_err(|source| ConfigError::Io {
        path: path.into(),
        source,
    })?;
    let mut body = String::new();
    // Read one byte beyond the limit to detect overflow instead of accepting a
    // truncated document. Bounding the read also covers growth after metadata.
    file.take(maximum + 1)
        .read_to_string(&mut body)
        .map_err(|source| ConfigError::Io {
            path: path.into(),
            source,
        })?;
    if body.len() as u64 > maximum {
        return Err(invalid("document", "exceeds file size limit"));
    }
    Ok(body)
}

fn read_config(path: &Path, source: ConfigSource) -> Result<LoadedConfig, ConfigError> {
    let body = read_bounded(path, MAX_CONFIG_BYTES)?;
    let value = match path.extension().and_then(|x| x.to_str()) {
        Some("toml") => {
            let table: toml::Table =
                toml::from_str(&body).map_err(|_| invalid("TOML", "could not decode document"))?;
            serde_json::to_value(table).map_err(|_| invalid("TOML", "unsupported value"))?
        }
        Some("yaml" | "yml") => decode_yaml(&body)?,
        _ => return Err(invalid("format", "expected .yaml, .yml or .toml")),
    };
    finish_config(value, source, path.parent().unwrap_or(Path::new(".")))
}

pub(crate) fn invalid(field: impl Into<String>, rule: &'static str) -> ConfigError {
    ConfigError::Invalid {
        field: field.into(),
        rule,
    }
}

// Check YAML syntax features before reducing the document to JSON values, which
// cannot retain anchor or tag provenance. The decoder separately rejects duplicate
// keys and bounds decoded nodes and scalar bytes; both passes enforce the
// [format contract](../../../docs/reference/configuration-fields.md#formats-and-parser-budgets).
fn decode_yaml(body: &str) -> Result<serde_json::Value, ConfigError> {
    use granit_parser::{Event, Parser};
    let mut parser = Parser::new_from_str(body);
    let mut depth = 0usize;
    let mut events = 0usize;
    let mut documents = 0usize;
    while let Some(event) = parser.next_event() {
        let (event, _) = event.map_err(|_| invalid("YAML", "invalid syntax"))?;
        events += 1;
        if events > 32_768 {
            return Err(invalid("YAML", "too many events"));
        }
        match &event {
            Event::Scalar(_, _, anchor, tag)
            | Event::SequenceStart(_, anchor, tag)
            | Event::MappingStart(_, anchor, tag)
                if *anchor != 0 || tag.is_some() =>
            {
                return Err(invalid("YAML", "tags and anchors are not supported"));
            }
            Event::Alias(_) => return Err(invalid("YAML", "aliases are not supported")),
            _ => {}
        }
        match event {
            Event::DocumentStart(_) => {
                documents += 1;
                if documents > 1 {
                    return Err(invalid("YAML", "expected one document"));
                }
            }
            Event::SequenceStart(..) | Event::MappingStart(..) => {
                depth += 1;
                if depth > 32 {
                    return Err(invalid("YAML", "nesting limit exceeded"));
                }
            }
            Event::SequenceEnd | Event::MappingEnd => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    let options = serde_saphyr::options! {
        duplicate_keys: serde_saphyr::DuplicateKeyPolicy::Error,
        strict_booleans: true, with_snippet: false,
        budget: serde_saphyr::budget! { max_depth: 32, max_events: 32_768, max_nodes: 16_384,
            max_total_scalar_bytes: MAX_CONFIG_BYTES as usize, max_aliases: 0, max_anchors: 0,
            max_merge_keys: 0, max_documents: 1 },
    };
    serde_saphyr::from_str_with_options(body, options)
        .map_err(|_| invalid("YAML", "invalid or duplicate mapping values"))
}

fn finish_config(
    value: serde_json::Value,
    source: ConfigSource,
    _directory: &Path,
) -> Result<LoadedConfig, ConfigError> {
    let mut settings = value
        .as_object()
        .cloned()
        .ok_or_else(|| invalid("document", "expected a mapping"))?;
    for (table, keys) in [
        (
            "paths",
            &[
                "state_dir",
                "logs_dir",
                "cache_dir",
                "database",
                "instance_lock",
            ][..],
        ),
        ("profiles", &["directory"][..]),
        ("local", &["endpoint"][..]),
    ] {
        if settings
            .get(table)
            .and_then(|v| v.as_object())
            .is_some_and(|values| keys.iter().any(|key| values.contains_key(*key)))
        {
            return Err(ConfigError::RetiredPath);
        }
    }
    let mut bootstrap = serde_json::Map::new();
    for key in ["schema_version", "default_profile", "telemetry"] {
        if let Some(value) = settings.remove(key) {
            bootstrap.insert(key.into(), value);
        }
    }
    let config: BootstrapConfig = serde_json::from_value(bootstrap.into())
        .map_err(|_| invalid("bootstrap", "invalid fields or values"))?;
    let defaults =
        serde_json::to_value(UndercurrentProductConfig::default()).expect("serializable config");
    if settings.keys().any(|key| defaults.get(key).is_none()) {
        return Err(ConfigError::UnknownSetting);
    }
    // Check field names and types before typed deserialization, without echoing values.
    crate::validate::validate_shape(&serde_json::Value::Object(settings.clone()), &defaults, "")?;
    let product: UndercurrentProductConfig = serde_json::from_value(settings.into())
        .map_err(|_| invalid("document", "invalid field type or range"))?;
    crate::validate::validate(&product)?;
    Ok(LoadedConfig {
        config,
        source,
        settings: product,
    })
}

fn default_schema_version() -> undercurrent_foundation::ReleaseVersion {
    CURRENT_CONFIG_SCHEMA_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;
    fn write(root: &Path, name: &str, body: &str) -> PathBuf {
        let file = root.join(name);
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(&file, body).unwrap();
        file
    }
    #[test]
    fn independent_storage_settings_are_rejected() {
        let temp = tempfile::tempdir().unwrap();
        for body in [
            "[paths]\nstate_dir = '/state'",
            "[paths]\ndatabase = '/db'",
            "[paths]\ninstance_lock = '/lock'",
            "[paths]\nlogs_dir = '/logs'",
            "[paths]\ncache_dir = '/cache'",
            "[local]\nendpoint = '/socket'",
            "[profiles]\ndirectory = '/profiles'",
        ] {
            let path = write(temp.path(), "config.toml", body);
            assert!(matches!(
                load_config(&ConfigDiscovery::default().with_explicit_path(path)),
                Err(ConfigError::RetiredPath)
            ));
        }
    }
    #[test]
    fn discovery_uses_home_and_rejects_retired_environment() {
        let temp = tempfile::tempdir().unwrap();
        let default = write(temp.path(), "config.toml", "default_profile = 'default'");
        let env = write(temp.path(), "env.toml", "default_profile = 'environment'");
        let explicit = write(temp.path(), "explicit.toml", "default_profile = 'explicit'");
        let discovery = ConfigDiscovery::from_path_env(
            PathEnvironment::default().with_undercurrent_home(temp.path()),
        );
        assert_eq!(
            load_config(&discovery).unwrap().source,
            ConfigSource::UndercurrentHome(default)
        );
        assert!(matches!(
            load_config(&discovery.clone().with_undercurrent_config_env(env)),
            Err(ConfigError::RetiredPath)
        ));
        let loaded = load_config(&discovery.with_explicit_path(explicit.clone())).unwrap();
        assert_eq!(loaded.source, ConfigSource::Explicit(explicit));
        assert_eq!(loaded.config.default_profile.as_deref(), Some("explicit"));
    }
    #[test]
    fn other_product_configuration_is_not_discovered_or_accepted() {
        let temp = tempfile::tempdir().unwrap();
        let unrelated = write(
            temp.path(),
            ".aixand/other-product/config.toml",
            "[products.undercurrent.websocket]\nport = 9111",
        );
        let discovery = ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(temp.path()));
        assert_eq!(
            load_config(&discovery).unwrap().source,
            ConfigSource::Defaults
        );
        assert!(matches!(
            load_config(&discovery.with_explicit_path(unrelated)),
            Err(ConfigError::UnknownSetting)
        ));
    }
    #[test]
    fn invalid_or_missing_explicit_documents_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        for (name, body) in [
            ("future", "schema_version = 3"),
            ("unknown", "telemetery = true"),
            ("invalid", "not = = toml"),
        ] {
            let path = write(temp.path(), name, body);
            assert!(load_config(&ConfigDiscovery::default().with_explicit_path(path)).is_err());
        }
        assert!(matches!(
            load_config(
                &ConfigDiscovery::default()
                    .with_undercurrent_config_env(temp.path().join("absent"))
            ),
            Err(ConfigError::RetiredPath)
        ));
    }
    #[test]
    fn runtime_path_precedence_remains_explicit_config_project_default() {
        let resolved = RuntimePathCandidates::new("default")
            .with_project(Some("project"))
            .with_config(Some("config"))
            .with_explicit(Some("explicit"))
            .resolve();
        assert_eq!(resolved.source, RuntimePathSource::Explicit);
        assert_eq!(resolved.path, Path::new("explicit"));
        assert_eq!(
            RuntimePathCandidates::new("default")
                .with_config(Some("config"))
                .resolve()
                .source,
            RuntimePathSource::Config
        );
        assert_eq!(
            RuntimePathCandidates::new("default")
                .with_project(Some("project"))
                .resolve()
                .source,
            RuntimePathSource::Project
        );
        assert_eq!(
            RuntimePathCandidates::new("default").resolve().source,
            RuntimePathSource::Default
        );
    }
}
