// SPDX-License-Identifier: Apache-2.0

//! Product-owned configuration and state locations for Undercurrent.
//!
//! Resolution is lexical and creates no directories. Filesystem probing is
//! confined to [`UndercurrentPaths::check_legacy_layout`]; access control,
//! database validation, and endpoint construction belong to receiving adapters.

#![deny(missing_docs)]

use serde::{Deserialize, Serialize};
use std::env;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};

/// Environment override for the complete local Undercurrent home.
pub const UNDERCURRENT_HOME_ENV: &str = "UNDERCURRENT_HOME";

#[cfg(windows)]
const USER_HOME_ENV: &str = "USERPROFILE";
#[cfg(not(windows))]
const USER_HOME_ENV: &str = "HOME";

/// Joins a relative candidate to a base, preserving absolute candidates.
///
/// Performs no IO, canonicalization, or containment check: `..` is preserved.
pub fn resolve_path_from_base(base: &Path, candidate: &Path) -> PathBuf {
    if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        base.join(candidate)
    }
}

/// Failure to resolve an absolute product-owned location.
#[derive(Debug)]
pub enum PathsError {
    /// A default location needs a user home but none was supplied.
    HomeNotFound,
    /// An environment override is empty or relative.
    InvalidEnvironment {
        /// Name of the invalid environment variable; its value is never disclosed.
        name: &'static str,
    },
}

impl Display for PathsError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HomeNotFound => formatter.write_str("could not determine a user home directory"),
            Self::InvalidEnvironment { name } => write!(
                formatter,
                "environment variable {name} must contain an absolute path"
            ),
        }
    }
}
impl Error for PathsError {}

/// Process-independent inputs for product path resolution.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PathEnvironment {
    /// Optional product home override, used for both configuration and state.
    pub undercurrent_home: Option<PathBuf>,
    /// Optional XDG configuration root, used only for legacy detection.
    pub xdg_config_home: Option<PathBuf>,
    /// Optional XDG state root, used only for legacy detection.
    pub xdg_state_home: Option<PathBuf>,
    /// Optional user home directory.
    pub home_dir: Option<PathBuf>,
}
impl PathEnvironment {
    /// Captures product paths and the native user home (`USERPROFILE` on Windows,
    /// `HOME` elsewhere) from the process environment.
    pub fn from_process_env() -> Self {
        Self {
            undercurrent_home: env::var_os(UNDERCURRENT_HOME_ENV).map(PathBuf::from),
            xdg_config_home: env::var_os("XDG_CONFIG_HOME")
                .filter(|value| !value.is_empty())
                .map(PathBuf::from),
            xdg_state_home: env::var_os("XDG_STATE_HOME")
                .filter(|value| !value.is_empty())
                .map(PathBuf::from),
            home_dir: env::var_os(USER_HOME_ENV).map(PathBuf::from),
        }
    }
    /// Creates deterministic discovery inputs without reading the process environment.
    pub fn with_home_dir(home: impl Into<PathBuf>) -> Self {
        Self {
            home_dir: Some(home.into()),
            ..Self::default()
        }
    }
    /// Selects a complete product home without requiring a user home.
    pub fn with_undercurrent_home(mut self, home: impl Into<PathBuf>) -> Self {
        self.undercurrent_home = Some(home.into());
        self
    }
}

/// Resolved product locations. Resolution never creates directories.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UndercurrentPaths {
    config_file: PathBuf,
    state_dir: PathBuf,
    home: PathBuf,
}
impl UndercurrentPaths {
    /// Resolves the complete home, ignoring XDG storage defaults.
    ///
    /// An absolute product-home override wins; otherwise an absolute native
    /// user home is required and `.aixand/undercurrent` is appended. This does not
    /// canonicalize paths or inspect existence, permissions, or legacy state.
    ///
    /// # Errors
    ///
    /// Returns [`PathsError::HomeNotFound`] without a required user home, or
    /// [`PathsError::InvalidEnvironment`] for an empty or relative selected input.
    pub fn resolve(environment: &PathEnvironment) -> Result<Self, PathsError> {
        let home = if let Some(home) = &environment.undercurrent_home {
            validate_absolute(home, UNDERCURRENT_HOME_ENV)?;
            home.clone()
        } else {
            let home = environment
                .home_dir
                .as_deref()
                .ok_or(PathsError::HomeNotFound)?;
            validate_absolute(home, USER_HOME_ENV)?;
            home.join(".aixand").join("undercurrent")
        };
        Ok(Self {
            config_file: home.join("config.yaml"),
            state_dir: home.join("state"),
            home,
        })
    }
    /// Returns the root of all product-owned storage.
    pub fn home(&self) -> &Path {
        &self.home
    }
    /// Returns transient runtime storage.
    pub fn run_dir(&self) -> PathBuf {
        self.home.join("run")
    }
    /// Returns structured log storage.
    pub fn logs_dir(&self) -> PathBuf {
        self.home.join("logs")
    }
    /// Returns the reserved cache location without creating it.
    pub fn cache_dir(&self) -> PathBuf {
        self.home.join("cache")
    }
    /// Returns editable profile storage.
    pub fn profiles_dir(&self) -> PathBuf {
        self.home.join("profiles")
    }

    /// Checks known legacy markers without moving, deleting, or creating files.
    ///
    /// A path at `home/state/undercurrent.db` bypasses further checks; its contents
    /// and file type are not validated. Otherwise the selected home is scanned.
    /// Without an explicit product-home override, old user-home and absolute XDG
    /// roots are scanned too. This is a finite marker scan, not a migration tool.
    ///
    /// # Errors
    ///
    /// Returns an IO error for a detected marker or a failed existence probe.
    /// [`crate::load_config`] maps either case to [`crate::ConfigError::LegacyLayout`].
    pub fn check_legacy_layout(&self, environment: &PathEnvironment) -> std::io::Result<()> {
        // The current database path marks an initialized layout, so leftover legacy
        // roots do not block it. Existence alone does not establish database validity.
        // See [legacy-layout policy](../docs/internals/paths.md#legacy-layout-detection).
        if self.state_dir.join("undercurrent.db").try_exists()? {
            return Ok(());
        }
        let mut roots = vec![self.home.clone()];
        if environment.undercurrent_home.is_none() {
            if let Some(home) = &environment.home_dir {
                roots.push(home.join(".local/state/undercurrent"));
                roots.push(home.join(".config/undercurrent"));
            }
            for root in [&environment.xdg_state_home, &environment.xdg_config_home]
                .into_iter()
                .flatten()
            {
                if root.is_absolute() {
                    roots.push(root.join("undercurrent"));
                }
            }
        }
        for root in roots {
            for name in [
                "undercurrent.db",
                "undercurrent.db-wal",
                "undercurrent.db-shm",
                "undercurrent.db-journal",
                "session-host.sock",
                "session-host.auth",
                "session-host.execution-jobs",
                "session-host.attempt-receipts",
                "execution-output",
                "tui",
                "group",
            ] {
                if root.join(name).try_exists()? {
                    return Err(std::io::Error::other(
                        "legacy Undercurrent layout detected; preserve state and diagnose offline using docs/guides/configuration/diagnose.md#resolve-a-legacy-home-rejection before starting",
                    ));
                }
            }
            if root != self.home
                && ((root.join("config.yaml").try_exists()?
                    || root.join("config.toml").try_exists()?
                    || root.join("config.yml").try_exists()?)
                    || root.join("shortcuts.toml").try_exists()?)
            {
                return Err(std::io::Error::other(
                    "legacy Undercurrent configuration detected; preserve state and diagnose offline using docs/guides/configuration/diagnose.md#resolve-a-legacy-home-rejection before starting",
                ));
            }
        }
        Ok(())
    }
    /// Returns the preferred `config.yaml` path, without discovering a file.
    ///
    /// [`crate::load_config`] also considers `config.yml` and `config.toml` and
    /// rejects multiple active names rather than preferring this path.
    pub fn config_file(&self) -> PathBuf {
        self.config_file.clone()
    }
    /// Returns the default product state directory.
    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }
}
fn validate_absolute(path: &Path, name: &'static str) -> Result<(), PathsError> {
    if path.is_absolute() {
        Ok(())
    } else {
        Err(PathsError::InvalidEnvironment { name })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn default_configuration_and_state_are_product_owned() {
        let home = std::env::temp_dir().join("alice");
        let paths = UndercurrentPaths::resolve(&PathEnvironment::with_home_dir(&home)).unwrap();
        assert_eq!(
            paths.config_file(),
            home.join(".aixand/undercurrent/config.yaml")
        );
        assert_eq!(paths.state_dir(), home.join(".aixand/undercurrent/state"));
    }
    #[test]
    fn xdg_roots_do_not_replace_home() {
        let env = PathEnvironment {
            xdg_config_home: Some("/config".into()),
            xdg_state_home: Some("/state".into()),
            ..PathEnvironment::default()
        };
        assert!(matches!(
            UndercurrentPaths::resolve(&env),
            Err(PathsError::HomeNotFound)
        ));
        let home = std::env::temp_dir().join("alice");
        let paths = UndercurrentPaths::resolve(&PathEnvironment {
            home_dir: Some(home.clone()),
            ..env
        })
        .unwrap();
        assert_eq!(paths.home(), home.join(".aixand/undercurrent"));
        assert_eq!(paths.run_dir(), paths.home().join("run"));
        assert_eq!(paths.profiles_dir(), paths.home().join("profiles"));
    }
    #[test]
    fn explicit_product_home_wins_over_xdg_and_needs_no_home() {
        let home = std::env::temp_dir().join("private/uc");
        let env = PathEnvironment {
            xdg_config_home: Some("relative-unused".into()),
            ..PathEnvironment::default().with_undercurrent_home(&home)
        };
        let paths = UndercurrentPaths::resolve(&env).unwrap();
        assert_eq!(paths.config_file(), home.join("config.yaml"));
        assert_eq!(paths.state_dir(), home.join("state"));
    }
    #[test]
    fn invalid_overrides_fail_without_disclosing_their_value() {
        for home in ["", "private-relative-component"] {
            let error = UndercurrentPaths::resolve(
                &PathEnvironment::default().with_undercurrent_home(home),
            )
            .unwrap_err();
            assert!(matches!(
                error,
                PathsError::InvalidEnvironment {
                    name: UNDERCURRENT_HOME_ENV
                }
            ));
            assert!(!error.to_string().contains("private-relative-component"));
        }
        assert!(matches!(
            UndercurrentPaths::resolve(&PathEnvironment::default()),
            Err(PathsError::HomeNotFound)
        ));
    }
    #[test]
    fn legacy_detection_distinguishes_default_custom_and_initialized_homes() {
        let temp = tempfile::tempdir().unwrap();
        let env = PathEnvironment::with_home_dir(temp.path());
        let paths = UndercurrentPaths::resolve(&env).unwrap();
        paths.check_legacy_layout(&env).unwrap();
        let old = temp.path().join(".local/state/undercurrent");
        std::fs::create_dir_all(&old).unwrap();
        std::fs::write(old.join("undercurrent.db"), b"legacy").unwrap();
        assert!(paths.check_legacy_layout(&env).is_err());
        assert!(!paths.home().exists());
        let custom_env = env
            .clone()
            .with_undercurrent_home(temp.path().join("custom"));
        let custom = UndercurrentPaths::resolve(&custom_env).unwrap();
        custom.check_legacy_layout(&custom_env).unwrap();
        std::fs::create_dir_all(custom.home()).unwrap();
        std::fs::write(custom.home().join("undercurrent.db"), b"legacy").unwrap();
        assert!(custom.check_legacy_layout(&custom_env).is_err());
        std::fs::create_dir_all(custom.state_dir()).unwrap();
        std::fs::write(custom.state_dir().join("undercurrent.db"), b"current").unwrap();
        custom.check_legacy_layout(&custom_env).unwrap();
        std::fs::create_dir_all(paths.state_dir()).unwrap();
        std::fs::write(paths.state_dir().join("undercurrent.db"), b"current").unwrap();
        paths.check_legacy_layout(&env).unwrap();
    }

    #[test]
    fn relative_path_resolution_preserves_absolute_overrides() {
        assert_eq!(
            resolve_path_from_base(Path::new("/state"), Path::new("logs")),
            Path::new("/state/logs")
        );
        assert_eq!(
            resolve_path_from_base(Path::new("/state"), Path::new("/logs")),
            Path::new("/logs")
        );
    }
}
