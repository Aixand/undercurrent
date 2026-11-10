// SPDX-License-Identifier: Apache-2.0

//! Shared finite/stream output, verbosity, timeout, and config selection.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::time::Duration;

use crate::cli_support::{CliContext, CommandTimeout, OutputFormat, TimeoutParseError, Verbosity};

/// Default finite request and stream-activation deadline.
pub const DEFAULT_CLI_TIMEOUT: Duration = Duration::from_secs(30);

/// Raw global presentation and runtime selections supplied by an argument adapter.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GlobalCliOptions {
    /// Render finite values as JSON.
    pub json: bool,
    /// Render stream values as JSON Lines.
    pub jsonl: bool,
    /// Suppress non-essential text.
    pub quiet: bool,
    /// Add safe operational context.
    pub verbose: bool,
    /// Add maximum payload-redacted diagnostic context.
    pub diagnostic: bool,
    /// Explicit common duration text.
    pub timeout: Option<String>,
    /// Explicit common config file.
    pub config_path: Option<PathBuf>,
    /// One-invocation named local connection.
    pub connection: Option<String>,
}

/// Validated context shared by every command handler in one invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliInvocationContext {
    pub(crate) configuration: Option<std::sync::Arc<crate::ResolvedCliConfig>>,
    pub(crate) selection: Option<crate::settings::Selection>,
    pub(crate) selection_failed: bool,
    timeout_explicit: bool,
    common: CliContext,
    config_path: Option<PathBuf>,
    connection: Option<String>,
}

impl CliInvocationContext {
    pub(crate) fn with_configuration(
        &self,
        config: crate::ResolvedCliConfig,
    ) -> Result<Self, CliContextError> {
        let mut context = self.clone();
        if !context.timeout_explicit {
            context.common.timeout = CommandTimeout::new(
                Duration::from_millis(config.document().cli.command_timeout_millis),
                crate::cli_support::MAX_COMMON_TIMEOUT,
            )
            .map_err(CliContextError::Timeout)?;
        }
        context.configuration = Some(std::sync::Arc::new(config));
        Ok(context)
    }

    /// Validates mutually exclusive formats, verbosity, and the common timeout.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed conflict or timeout parse failure.
    pub fn from_options(options: GlobalCliOptions) -> Result<Self, CliContextError> {
        if options.json && options.jsonl {
            return Err(CliContextError::ConflictingOutputFormats);
        }
        let selected_verbosity = usize::from(options.quiet)
            + usize::from(options.verbose)
            + usize::from(options.diagnostic);
        if selected_verbosity > 1 {
            return Err(CliContextError::ConflictingVerbosity);
        }
        let output = if options.json {
            OutputFormat::Json
        } else if options.jsonl {
            OutputFormat::Jsonl
        } else {
            OutputFormat::Text
        };
        let verbosity = if options.quiet {
            Verbosity::Quiet
        } else if options.diagnostic {
            Verbosity::Diagnostic
        } else if options.verbose {
            Verbosity::Verbose
        } else {
            Verbosity::Normal
        };
        let timeout_explicit = options.timeout.is_some();
        let timeout = match options.timeout {
            Some(value) => value.parse().map_err(CliContextError::Timeout)?,
            None => CommandTimeout::new(DEFAULT_CLI_TIMEOUT, DEFAULT_CLI_TIMEOUT)
                .map_err(CliContextError::Timeout)?,
        };
        Ok(Self {
            configuration: None,
            selection: None,
            selection_failed: false,
            timeout_explicit,
            common: CliContext::new(output, verbosity, timeout),
            config_path: options.config_path,
            connection: options.connection,
        })
    }

    /// Returns the common presentation and timeout context.
    pub const fn common(&self) -> CliContext {
        self.common
    }

    /// Returns the explicit config path without logging or displaying it.
    pub fn config_path(&self) -> Option<&std::path::Path> {
        self.config_path.as_deref()
    }

    pub(crate) fn connection_name(&self) -> Option<&str> {
        self.connection.as_deref()
    }
}

/// Invalid global CLI presentation or timeout selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliContextError {
    /// JSON and JSON Lines were selected together.
    ConflictingOutputFormats,
    /// More than one verbosity mode was selected.
    ConflictingVerbosity,
    /// Timeout text was invalid or out of bounds.
    Timeout(TimeoutParseError),
}

impl Display for CliContextError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConflictingOutputFormats => {
                formatter.write_str("--json and --jsonl cannot be combined")
            }
            Self::ConflictingVerbosity => {
                formatter.write_str("--quiet, --verbose, and --diagnostic are mutually exclusive")
            }
            Self::Timeout(source) => write!(formatter, "invalid command timeout: {source}"),
        }
    }
}

impl Error for CliContextError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Timeout(source) => Some(source),
            Self::ConflictingOutputFormats | Self::ConflictingVerbosity => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_text_normal_and_thirty_seconds() {
        let context = CliInvocationContext::from_options(GlobalCliOptions::default())
            .expect("default context");

        assert_eq!(context.common().output, OutputFormat::Text);
        assert_eq!(context.common().verbosity, Verbosity::Normal);
        assert_eq!(context.common().timeout.get(), DEFAULT_CLI_TIMEOUT);
    }

    #[test]
    fn machine_formats_and_verbosity_conflicts_are_rejected() {
        assert_eq!(
            CliInvocationContext::from_options(GlobalCliOptions {
                json: true,
                jsonl: true,
                ..GlobalCliOptions::default()
            }),
            Err(CliContextError::ConflictingOutputFormats)
        );
        assert_eq!(
            CliInvocationContext::from_options(GlobalCliOptions {
                quiet: true,
                diagnostic: true,
                ..GlobalCliOptions::default()
            }),
            Err(CliContextError::ConflictingVerbosity)
        );
    }

    #[test]
    fn explicit_timeout_and_config_path_are_retained_without_resolution() {
        let context = CliInvocationContext::from_options(GlobalCliOptions {
            timeout: Some("250ms".to_owned()),
            config_path: Some(PathBuf::from("config.toml")),
            ..GlobalCliOptions::default()
        })
        .expect("explicit context");

        assert_eq!(context.common().timeout.get(), Duration::from_millis(250));
        assert_eq!(
            context.config_path(),
            Some(std::path::Path::new("config.toml"))
        );
    }
}

#[cfg(test)]
mod configuration_tests {
    use super::*;
    #[test]
    fn configuration_timeout_preserves_explicit_default_and_is_read_once() {
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join("config.yaml");
        std::fs::write(&path, "cli:\n  command_timeout_millis: 45000\n").unwrap();
        let discovery = undercurrent_config::ConfigDiscovery::from_path_env(
            undercurrent_config::PathEnvironment::default().with_undercurrent_home(home.path()),
        );
        let config = crate::load_cli_config(&discovery).unwrap();
        std::fs::remove_file(&path).unwrap();
        for explicit in [false, true] {
            let context = CliInvocationContext::from_options(GlobalCliOptions {
                timeout: explicit.then(|| "30s".into()),
                ..Default::default()
            })
            .unwrap()
            .with_configuration(config.clone())
            .unwrap();
            assert_eq!(
                context.common().timeout.get(),
                Duration::from_secs(if explicit { 30 } else { 45 })
            );
            let cached = crate::NativeProductClientFactory::from_context(&context)
                .resolve()
                .unwrap();
            assert_eq!(cached.document().cli.command_timeout_millis, 45000);
        }
    }
}
