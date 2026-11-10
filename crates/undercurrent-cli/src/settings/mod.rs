// SPDX-License-Identifier: Apache-2.0

//! CLI-owned saved local connections. Daemons and SDK clients never read this store.

mod store;

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::Write;
use std::path::{Component, Path, PathBuf};

use clap::{Args, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};

use crate::{CliError, CliInvocationContext, CliPresenter, CommonExitCode, NativeCliCommand};
use store::Store;

/// Saved connection management arguments. These operations never load workspace configuration.
#[derive(Clone, Args, PartialEq, Eq)]
pub struct SettingsArguments {
    /// Settings operation.
    #[command(subcommand)]
    pub action: SettingsAction,
}

/// Offline CLI settings operations.
#[derive(Clone, Subcommand, PartialEq, Eq)]
pub enum SettingsAction {
    /// Show saved and effective connection selection.
    Show,
    /// Change the saved connection selection.
    Set {
        /// Setting to change.
        #[command(subcommand)]
        setting: SettingsSet,
    },
    /// Manage named local connections, independently of detection profiles.
    Profiles {
        /// Profile operation.
        #[command(subcommand)]
        action: ConnectionProfileAction,
    },
}

/// Supported saved connection settings.
#[derive(Clone, Subcommand, PartialEq, Eq)]
pub enum SettingsSet {
    /// Select the connection used by subsequent commands.
    Profile {
        /// Existing connection name, or the permanent built-in default.
        name: String,
    },
}

/// Named connection operations; removal never deletes instance data.
#[derive(Clone, Subcommand, PartialEq, Eq)]
pub enum ConnectionProfileAction {
    /// List saved connections, including the permanent default.
    List,
    /// Show one connection definition.
    Show {
        /// Existing connection name.
        name: String,
    },
    /// Add a connection without changing the saved selection.
    Add {
        /// Unique connection name; default is reserved.
        name: String,
        /// Absolute Undercurrent instance home.
        #[arg(long)]
        home: PathBuf,
        /// Whether ordinary commands may start an absent daemon.
        #[arg(long, value_enum, default_value = "auto")]
        auto_start: AutoStartPolicy,
    },
    /// Register an application's connection idempotently; other owners are preserved.
    Register {
        /// Connection name; default is reserved.
        name: String,
        /// Absolute Undercurrent instance home.
        #[arg(long)]
        home: PathBuf,
        /// Stable registration owner, such as example-app.
        #[arg(long)]
        owner: String,
        /// Whether ordinary commands may start an absent daemon.
        #[arg(long, value_enum, default_value = "never")]
        auto_start: AutoStartPolicy,
        /// Select only when creating this registration; retries preserve later choices.
        #[arg(long)]
        select_if_new: bool,
    },
    /// Forget a connection; an active connection is switched to default.
    Remove {
        /// Connection to forget; default cannot be removed.
        name: String,
    },
}

/// Automatic daemon startup policy for ordinary commands.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum AutoStartPolicy {
    /// Preserve normal Undercurrent automatic startup.
    #[default]
    Auto,
    /// Only connect; the owning application starts its runtime.
    Never,
}

/// Payload-safe CLI settings failure.
#[derive(Debug)]
pub struct CliSettingsError {
    message: &'static str,
    source: Option<std::io::Error>,
    code: CommonExitCode,
}

impl CliSettingsError {
    /// Returns the process result category.
    pub const fn exit_code(&self) -> CommonExitCode {
        self.code
    }
}

impl Display for CliSettingsError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message)
    }
}

impl Error for CliSettingsError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_ref().map(|source| source as &dyn Error)
    }
}

fn invalid(message: &'static str) -> CliError {
    CliError::Settings(CliSettingsError {
        message,
        source: None,
        code: CommonExitCode::InvalidInput,
    })
}

fn io_error(message: &'static str, source: std::io::Error) -> CliError {
    CliError::Settings(CliSettingsError {
        message,
        source: Some(source),
        code: CommonExitCode::Runtime,
    })
}

pub(crate) fn unavailable() -> CliError {
    CliError::Settings(CliSettingsError {
        message: "selected connection is not running; open its owning application or select another connection with `uc settings set profile default`",
        source: None,
        code: CommonExitCode::Failure,
    })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Profile {
    home: PathBuf,
    #[serde(default)]
    auto_start: AutoStartPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    owner: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    schema_version: undercurrent_protocol::ReleaseVersion,
    active_profile: String,
    #[serde(default)]
    profiles: BTreeMap<String, Profile>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            schema_version: undercurrent_protocol::ReleaseVersion::Current,
            active_profile: "default".into(),
            profiles: BTreeMap::new(),
        }
    }
}

/// One already resolved CLI connection; kept immutable for the invocation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct Selection {
    pub(crate) profile: Option<String>,
    pub(crate) home: PathBuf,
    pub(crate) source: &'static str,
    pub(crate) auto_start: AutoStartPolicy,
}

impl Selection {
    pub(crate) fn apply_paths(&self, environment: &mut crate::PathEnvironment) {
        if self.profile.as_deref() == Some("default") {
            // Preserve normal default-home legacy discovery. Treating default
            // as an explicit override would skip evidence in old XDG roots.
            environment.undercurrent_home = None;
            environment.home_dir = self
                .home
                .parent()
                .and_then(Path::parent)
                .map(Path::to_path_buf);
        } else {
            environment.undercurrent_home = Some(self.home.clone());
        }
    }
}

fn name_valid(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
}

fn validate_home(home: &Path) -> Result<(), CliError> {
    if !home.is_absolute()
        || home.as_os_str().len() > 4096
        || home.components().any(|c| matches!(c, Component::ParentDir))
        || home
            .to_str()
            .is_none_or(|value| value.contains(['\0', '\r', '\n']))
    {
        return Err(invalid(
            "connection home must be an absolute path without traversal or control characters",
        ));
    }
    // Validate the same endpoint bound as normal CLI configuration, without creating it.
    undercurrent_sdk::LocalClientConfig::validate_endpoint(home.join("run").join("control.sock"))
        .map_err(|_| invalid("connection home exceeds the platform's local endpoint limits"))?;
    Ok(())
}

fn validate(settings: &Settings) -> Result<(), CliError> {
    if settings.schema_version != undercurrent_protocol::ReleaseVersion::Current
        || settings.profiles.len() > 128
        || settings.profiles.contains_key("default")
        || !name_valid(&settings.active_profile)
        || settings.active_profile != "default"
            && !settings.profiles.contains_key(&settings.active_profile)
    {
        return Err(invalid(
            "invalid CLI settings; preserve the settings file and repair its profile definitions",
        ));
    }
    for (name, profile) in &settings.profiles {
        if !name_valid(name)
            || profile
                .owner
                .as_ref()
                .is_some_and(|owner| !name_valid(owner))
        {
            return Err(invalid("invalid connection profile name or owner"));
        }
        validate_home(&profile.home)?;
    }
    Ok(())
}

fn profile(settings: &Settings, name: &str, user_home: &Path) -> Result<Profile, CliError> {
    if name == "default" {
        return Ok(Profile {
            home: user_home.join(".aixand").join("undercurrent"),
            auto_start: AutoStartPolicy::Auto,
            owner: None,
        });
    }
    settings
        .profiles
        .get(name)
        .cloned()
        .ok_or_else(|| invalid("connection profile not found; run `uc settings profiles list`"))
}

fn resolve(explicit: Option<&str>) -> Result<Selection, CliError> {
    if explicit.is_none()
        && let Some(home) = std::env::var_os("UNDERCURRENT_HOME")
    {
        let home = PathBuf::from(home);
        validate_home(&home)?;
        return Ok(Selection {
            profile: None,
            home,
            source: "environment",
            auto_start: AutoStartPolicy::Auto,
        });
    }
    let store = Store::from_environment()?;
    // An explicit default remains usable even when saved settings are malformed.
    let settings = if explicit == Some("default") {
        Settings::default()
    } else {
        store.read()?
    };
    let name = explicit.unwrap_or(&settings.active_profile);
    let selected = profile(&settings, name, store.user_home())?;
    Ok(Selection {
        profile: Some(name.to_owned()),
        home: selected.home,
        source: if explicit.is_some() {
            "flag"
        } else if name == "default" {
            "default"
        } else {
            "saved_profile"
        },
        auto_start: selected.auto_start,
    })
}

pub(crate) fn prepare_context(
    command: &NativeCliCommand,
    mut context: CliInvocationContext,
) -> Result<CliInvocationContext, CliError> {
    if context.selection_failed {
        return Ok(context);
    }
    if context.selection.is_none() && matches!(command, NativeCliCommand::Settings(_)) {
        // Global settings stay editable without loading a workspace. When the
        // saved selection is readable, diagnostics still belong to that home.
        context.selection = resolve(context.connection_name()).ok();
        return Ok(context);
    }
    let offline_context =
        matches!(command, NativeCliCommand::Context(arguments) if !arguments.name);
    let legacy_report = matches!(
        command,
        NativeCliCommand::Group(crate::group::GroupArguments {
            action: Some(crate::group::GroupAction::Report { .. })
        })
    );
    if context.selection.is_none()
        && !offline_context
        && !legacy_report
        && !matches!(
            command,
            NativeCliCommand::Settings(_)
                | NativeCliCommand::Completion(_)
                | NativeCliCommand::Events
                | NativeCliCommand::Init(_)
        )
    {
        match resolve(context.connection_name()) {
            Ok(selection) => context.selection = Some(selection),
            Err(_) if matches!(command, NativeCliCommand::Debug(arguments) if matches!(arguments.action, Some(crate::DebugAction::Collect(_)))) =>
            {
                // Collection must survive invalid global settings too, without
                // silently inspecting or connecting to a different instance.
                context.selection_failed = true;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(context)
}

pub(crate) fn execute<W: Write>(
    arguments: &SettingsArguments,
    context: &CliInvocationContext,
    presenter: CliPresenter,
    output: &mut W,
) -> Result<(), CliError> {
    let store = Store::from_environment()?;
    let mut created = false;
    match &arguments.action {
        SettingsAction::Set {
            setting: SettingsSet::Profile { name },
        } => {
            store.update(|settings| {
                profile(settings, name, store.user_home())?;
                settings.active_profile = name.clone();
                Ok(())
            })?;
        }
        SettingsAction::Profiles {
            action:
                ConnectionProfileAction::Add {
                    name,
                    home,
                    auto_start,
                },
        } => {
            store.update(|settings| {
                if name == "default" || !name_valid(name) {
                    return Err(invalid(
                        "default is reserved; choose a valid connection profile name",
                    ));
                }
                if settings.profiles.contains_key(name) {
                    return Err(invalid("connection profile already exists"));
                }
                validate_home(home)?;
                settings.profiles.insert(
                    name.clone(),
                    Profile {
                        home: home.clone(),
                        auto_start: *auto_start,
                        owner: None,
                    },
                );
                created = true;
                Ok(())
            })?;
        }
        SettingsAction::Profiles {
            action:
                ConnectionProfileAction::Register {
                    name,
                    home,
                    owner,
                    auto_start,
                    select_if_new,
                },
        } => {
            store.update(|settings| {
                if name == "default" || !name_valid(name) || !name_valid(owner) { return Err(invalid("default is reserved; choose a valid connection name and owner")); }
                validate_home(home)?;
                if let Some(existing) = settings.profiles.get(name) {
                    if existing.owner.as_deref() != Some(owner) { return Err(invalid("connection profile belongs to another owner; register under a different name")); }
                } else { created = true; }
                settings.profiles.insert(name.clone(), Profile { home: home.clone(), auto_start: *auto_start, owner: Some(owner.clone()) });
                if created && *select_if_new { settings.active_profile = name.clone(); }
                Ok(())
            })?;
        }
        SettingsAction::Profiles {
            action: ConnectionProfileAction::Remove { name },
        } => {
            store.update(|settings| {
                if name == "default" {
                    return Err(invalid("the built-in default connection cannot be removed"));
                }
                if settings.profiles.remove(name).is_none() {
                    return Err(invalid("connection profile not found"));
                }
                if settings.active_profile == *name {
                    settings.active_profile = "default".into();
                }
                Ok(())
            })?;
        }
        _ => {}
    }
    let settings = store.read()?;
    let value = match &arguments.action {
        SettingsAction::Profiles {
            action: ConnectionProfileAction::Show { name },
        } => {
            serde_json::json!({ "name": name, "active": name == &settings.active_profile, "connection": profile(&settings, name, store.user_home())? })
        }
        SettingsAction::Profiles {
            action: ConnectionProfileAction::List,
        } => {
            let mut profiles = settings.profiles.clone();
            profiles.insert(
                "default".into(),
                profile(&settings, "default", store.user_home())?,
            );
            serde_json::json!({ "active_profile": settings.active_profile, "profiles": profiles })
        }
        _ => serde_json::json!({ "active_profile": settings.active_profile,
            "effective": resolve(context.connection_name()).map(|selection| serde_json::to_value(selection).unwrap_or_default())
                .unwrap_or_else(|error| serde_json::json!({ "error": error.to_string() })),
            "created": created, "settings_file": store.path() }),
    };
    presenter.write_finite(output, &value, |_| {
        serde_json::to_string_pretty(&value).unwrap_or_else(|_| "CLI settings updated".into())
    })?;
    Ok(())
}
