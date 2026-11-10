// SPDX-License-Identifier: Apache-2.0

//! Bounded environment-file loading for native process launches.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};

use undercurrent_domain::LaunchSpec;

use super::NativeProcessRuntimeLimits;

/// Payload-safe environment resolution failure.
#[derive(Debug)]
pub(super) enum EnvironmentLoadError {
    /// A daemon-relative filesystem reference was supplied.
    RelativePath {
        /// Stable field containing the invalid reference.
        field: &'static str,
    },
    /// An environment file could not be opened.
    Open {
        /// Concrete filesystem failure.
        source: std::io::Error,
    },
    /// An environment file could not be read.
    Read {
        /// Concrete filesystem failure.
        source: std::io::Error,
    },
    /// An environment file exceeded its byte budget.
    FileCapacity {
        /// Configured environment-file byte limit.
        limit: usize,
    },
    /// Dotenv syntax or substitution was invalid.
    Parse {
        /// Concrete parser failure.
        source: dotenvy::Error,
    },
    /// Merged environment entry capacity was exceeded.
    EntryCapacity {
        /// Configured merged-entry limit.
        limit: usize,
    },
    /// Merged environment byte capacity was exceeded.
    ByteCapacity {
        /// Configured merged-byte limit.
        limit: usize,
    },
}

impl Display for EnvironmentLoadError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RelativePath { field } => {
                write!(formatter, "native process {field} must be absolute")
            }
            Self::Open { .. } => formatter.write_str("could not open process environment file"),
            Self::Read { .. } => formatter.write_str("could not read process environment file"),
            Self::FileCapacity { limit } => write!(
                formatter,
                "process environment file exceeds the {limit}-byte limit"
            ),
            Self::Parse { .. } => {
                formatter.write_str("process environment file contains invalid dotenv data")
            }
            Self::EntryCapacity { limit } => write!(
                formatter,
                "merged process environment exceeds the {limit}-entry limit"
            ),
            Self::ByteCapacity { limit } => write!(
                formatter,
                "merged process environment exceeds the {limit}-byte limit"
            ),
        }
    }
}

impl Error for EnvironmentLoadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Open { source } | Self::Read { source } => Some(source),
            Self::Parse { source } => Some(source),
            Self::RelativePath { .. }
            | Self::FileCapacity { .. }
            | Self::EntryCapacity { .. }
            | Self::ByteCapacity { .. } => None,
        }
    }
}

/// Resolves an absolute working-directory reference without touching it.
pub(super) fn working_directory(
    launch: &LaunchSpec,
) -> Result<Option<PathBuf>, EnvironmentLoadError> {
    launch
        .working_directory()
        .map(|reference| absolute_path("working_directory", reference.as_str()))
        .transpose()
}

/// Loads and merges a bounded dotenv file with explicit variables.
///
/// File entries are applied first and explicit launch entries override them.
/// Ambient process variables remain controlled by `portable-pty`; dotenv
/// substitutions therefore match the inherited environment used by the child.
pub(super) fn load_environment(
    launch: &LaunchSpec,
    limits: NativeProcessRuntimeLimits,
) -> Result<BTreeMap<String, String>, EnvironmentLoadError> {
    let mut values = BTreeMap::new();
    if let Some(reference) = launch.environment_file() {
        let path = absolute_path("environment_file", reference.as_str())?;
        let bytes = read_bounded(&path, limits.environment_file_bytes)?;
        for parsed in dotenvy::from_read_iter(Cursor::new(bytes)) {
            let (key, value) = parsed.map_err(|source| EnvironmentLoadError::Parse { source })?;
            insert_value(&mut values, key, value);
            validate_capacity(&values, limits)?;
        }
    }
    for (key, value) in launch.environment().iter() {
        insert_value(&mut values, key.to_owned(), value.to_owned());
        validate_capacity(&values, limits)?;
    }
    Ok(values)
}

fn insert_value(values: &mut BTreeMap<String, String>, key: String, value: String) {
    // Windows environment names are case insensitive. Normalize before applying
    // the explicit overlay so a dotenv `Path` cannot override an explicit `PATH`.
    #[cfg(windows)]
    let key = key.to_ascii_uppercase();
    values.insert(key, value);
}

fn absolute_path(field: &'static str, value: &str) -> Result<PathBuf, EnvironmentLoadError> {
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(EnvironmentLoadError::RelativePath { field });
    }
    Ok(path)
}

fn read_bounded(path: &Path, limit: usize) -> Result<Vec<u8>, EnvironmentLoadError> {
    let file = File::open(path).map_err(|source| EnvironmentLoadError::Open { source })?;
    let read_limit = u64::try_from(limit).unwrap_or(u64::MAX).saturating_add(1);
    let mut bytes = Vec::with_capacity(limit.min(64 * 1024));
    file.take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|source| EnvironmentLoadError::Read { source })?;
    if bytes.len() > limit {
        return Err(EnvironmentLoadError::FileCapacity { limit });
    }
    Ok(bytes)
}

fn validate_capacity(
    values: &BTreeMap<String, String>,
    limits: NativeProcessRuntimeLimits,
) -> Result<(), EnvironmentLoadError> {
    if values.len() > limits.environment_entries {
        return Err(EnvironmentLoadError::EntryCapacity {
            limit: limits.environment_entries,
        });
    }
    let total_bytes = values.iter().try_fold(0usize, |total, (key, value)| {
        total.checked_add(key.len())?.checked_add(value.len())
    });
    if total_bytes.is_none_or(|total| total > limits.environment_bytes) {
        return Err(EnvironmentLoadError::ByteCapacity {
            limit: limits.environment_bytes,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;
    use undercurrent_domain::{
        CommandSpec, EnvironmentVariables, ExecutionLimits, LaunchSpec, PathReference, TerminalSize,
    };

    use super::*;

    fn execution_limits() -> ExecutionLimits {
        ExecutionLimits {
            max_arguments: 8,
            max_command_bytes: 512,
            max_environment_entries: 8,
            max_environment_bytes: 512,
            max_path_bytes: 4_096,
            max_health_rules: 1,
            max_health_rule_bytes: 64,
        }
    }

    fn runtime_limits() -> NativeProcessRuntimeLimits {
        NativeProcessRuntimeLimits {
            max_processes: 4,
            spawn_workers: 1,
            spawn_queue_capacity: 4,
            input_queue_sequences: 4,
            input_queue_bytes_per_process: 128,
            output_read_bytes: 128,
            screen_snapshot_bytes: 64 * 1024,
            environment_file_bytes: 128,
            environment_entries: 4,
            environment_bytes: 64,
            worker_thread_stack_bytes: 64 * 1024,
        }
    }

    fn launch(environment_file: Option<PathReference>) -> LaunchSpec {
        LaunchSpec::new(
            CommandSpec::new("sh", Vec::new(), execution_limits()).expect("valid command"),
            EnvironmentVariables::new(
                [("OVERRIDE".to_owned(), "explicit".to_owned())],
                execution_limits(),
            )
            .expect("valid environment"),
            environment_file,
            None,
            TerminalSize::new(80, 24, 200, 100).expect("valid terminal size"),
        )
    }

    #[test]
    fn explicit_environment_overrides_file_values() {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("session.env");
        fs::write(&path, "FILE_ONLY=value\nOVERRIDE=file\n").expect("write environment");
        let reference = PathReference::new(path.to_string_lossy(), 4_096).expect("valid path");

        let values = load_environment(&launch(Some(reference)), runtime_limits())
            .expect("environment resolves");

        assert_eq!(values.get("FILE_ONLY").map(String::as_str), Some("value"));
        assert_eq!(values.get("OVERRIDE").map(String::as_str), Some("explicit"));
    }

    #[test]
    fn relative_environment_reference_is_rejected_without_exposing_it() {
        let reference = PathReference::new("private/session.env", 4_096).expect("valid path");
        let error = load_environment(&launch(Some(reference)), runtime_limits())
            .expect_err("relative path must fail");

        assert!(matches!(
            error,
            EnvironmentLoadError::RelativePath {
                field: "environment_file"
            }
        ));
        assert!(!error.to_string().contains("private"));
    }

    #[test]
    fn oversized_environment_file_is_rejected_before_parsing() {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("session.env");
        fs::write(&path, vec![b'X'; 129]).expect("write environment");
        let reference = PathReference::new(path.to_string_lossy(), 4_096).expect("valid path");

        let error = load_environment(&launch(Some(reference)), runtime_limits())
            .expect_err("oversized file must fail");

        assert!(matches!(
            error,
            EnvironmentLoadError::FileCapacity { limit: 128 }
        ));
    }
}
