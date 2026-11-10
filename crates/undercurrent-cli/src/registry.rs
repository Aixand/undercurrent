// SPDX-License-Identifier: Apache-2.0

//! Central CLI command, diagnostic event, and log-field registries.

use serde::{Deserialize, Serialize};

/// Stable retained CLI command names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CliCommand {
    /// Create and activate one managed session.
    Start,
    /// Stop one or more sessions.
    Stop,
    /// Restart one session.
    Restart,
    /// List a bounded prepared page of sessions.
    Ls,
    /// Inspect explicitly selected session detail.
    Inspect,
    /// Interact with one session terminal.
    Attach,
    /// Browse and manage sessions in the persistent terminal workspace.
    Tui,
    /// Manage and display local terminal workspaces.
    Group,
    /// Deliver typed terminal input.
    Send,
    /// Execute a one-shot command in one session context.
    Exec,
    /// Wait for a selected observed condition.
    Wait,
    /// Watch selected session state changes.
    Watch,
    /// Remove confirmed terminal sessions.
    Clean,
    /// Display the interactive bounded session dashboard.
    Top,
    /// Read retained or live output for selected sessions.
    Logs,
    /// Read one current terminal screen.
    Peek,
    /// Explain one current foreground decision.
    Explain,
    /// Identify the current inherited managed-session context.
    Context,
    /// Install or inspect shell integration.
    Init,
    /// Inspect and validate effective configuration.
    Config,
    /// Manage saved local connection profiles without a daemon.
    Settings,
    /// Inspect and test detection profiles.
    Profile,
    /// Control the local daemon process.
    Daemon,
    /// Destructively reset confirmed local state.
    Reset,
    /// Read bounded operational diagnostics.
    Debug,
    /// Generate shell completion definitions.
    Completion,
    /// Enumerate stable diagnostic event names.
    Events,
}

impl CliCommand {
    /// Whether ordinary execution may start an absent local daemon.
    pub(crate) const fn automatically_starts_daemon(self) -> bool {
        match self {
            Self::Start
            | Self::Stop
            | Self::Restart
            | Self::Ls
            | Self::Inspect
            | Self::Attach
            | Self::Tui
            | Self::Group
            | Self::Send
            | Self::Exec
            | Self::Wait
            | Self::Watch
            | Self::Clean
            | Self::Top
            | Self::Logs
            | Self::Peek
            | Self::Explain
            | Self::Profile => true,
            Self::Context
            | Self::Init
            | Self::Config
            | Self::Settings
            | Self::Daemon
            | Self::Reset
            | Self::Debug
            | Self::Completion
            | Self::Events => false,
        }
    }

    /// Complete retained command registry. The removed `stack` surface is not present.
    pub const ALL: [Self; 27] = [
        Self::Start,
        Self::Stop,
        Self::Restart,
        Self::Ls,
        Self::Inspect,
        Self::Attach,
        Self::Tui,
        Self::Group,
        Self::Send,
        Self::Exec,
        Self::Wait,
        Self::Watch,
        Self::Clean,
        Self::Top,
        Self::Logs,
        Self::Peek,
        Self::Explain,
        Self::Context,
        Self::Init,
        Self::Config,
        Self::Settings,
        Self::Profile,
        Self::Daemon,
        Self::Reset,
        Self::Debug,
        Self::Completion,
        Self::Events,
    ];

    /// Returns the stable command-line spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Stop => "stop",
            Self::Restart => "restart",
            Self::Ls => "ls",
            Self::Inspect => "inspect",
            Self::Attach => "attach",
            Self::Tui => "tui",
            Self::Group => "group",
            Self::Send => "send",
            Self::Exec => "exec",
            Self::Wait => "wait",
            Self::Watch => "watch",
            Self::Clean => "clean",
            Self::Top => "top",
            Self::Logs => "logs",
            Self::Peek => "peek",
            Self::Explain => "explain",
            Self::Context => "context",
            Self::Init => "init",
            Self::Config => "config",
            Self::Settings => "settings",
            Self::Profile => "profile",
            Self::Daemon => "daemon",
            Self::Reset => "reset",
            Self::Debug => "debug",
            Self::Completion => "completion",
            Self::Events => "events",
        }
    }
}

/// Stable payload-free CLI diagnostic events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CliDiagnosticEvent {
    /// One parsed command began execution.
    CommandStarted,
    /// One parsed command completed with a stable result category.
    CommandCompleted,
    /// Validated configuration was resolved.
    ConfigurationResolved,
    /// The CLI requested local daemon startup.
    DaemonStartRequested,
    /// A daemon process was spawned.
    DaemonSpawned,
    /// One multiplexed product connection was established.
    ConnectionEstablished,
    /// A finite product request completed.
    RequestCompleted,
    /// A logical stream was activated.
    StreamOpened,
    /// A stream reported an explicit replay gap.
    StreamGap,
    /// Command execution failed at a typed boundary.
    CommandFailed,
}

impl CliDiagnosticEvent {
    /// Complete CLI diagnostic event registry.
    pub const ALL: [Self; 10] = [
        Self::CommandStarted,
        Self::CommandCompleted,
        Self::ConfigurationResolved,
        Self::DaemonStartRequested,
        Self::DaemonSpawned,
        Self::ConnectionEstablished,
        Self::RequestCompleted,
        Self::StreamOpened,
        Self::StreamGap,
        Self::CommandFailed,
    ];

    /// Returns the stable namespaced event name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CommandStarted => "cli.command.started",
            Self::CommandCompleted => "cli.command.completed",
            Self::ConfigurationResolved => "cli.configuration.resolved",
            Self::DaemonStartRequested => "cli.daemon.start_requested",
            Self::DaemonSpawned => "cli.daemon.spawned",
            Self::ConnectionEstablished => "cli.connection.established",
            Self::RequestCompleted => "cli.request.completed",
            Self::StreamOpened => "cli.stream.opened",
            Self::StreamGap => "cli.stream.gap",
            Self::CommandFailed => "cli.command.failed",
        }
    }
}

/// Stable safe structured-log fields emitted by the CLI adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CliLogField {
    /// Stable diagnostic event name.
    Event,
    /// CLI command name.
    Command,
    /// Product operation name.
    Operation,
    /// Safe result category.
    ResultCategory,
    /// Completed operation duration.
    DurationMicros,
    /// Stable connection client identity.
    ClientId,
    /// Logical stream identity.
    StreamId,
    /// Stable session identity.
    SessionId,
    /// Bounded attempt number.
    Attempt,
}

impl CliLogField {
    /// Complete CLI log-field registry.
    pub const ALL: [Self; 9] = [
        Self::Event,
        Self::Command,
        Self::Operation,
        Self::ResultCategory,
        Self::DurationMicros,
        Self::ClientId,
        Self::StreamId,
        Self::SessionId,
        Self::Attempt,
    ];

    /// Returns the stable field key.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Event => "event",
            Self::Command => "command",
            Self::Operation => "operation",
            Self::ResultCategory => "result_category",
            Self::DurationMicros => "duration_micros",
            Self::ClientId => "client_id",
            Self::StreamId => "stream_id",
            Self::SessionId => "session_id",
            Self::Attempt => "attempt",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn retained_command_registry_is_unique_and_excludes_stack() {
        let names = CliCommand::ALL
            .into_iter()
            .map(CliCommand::as_str)
            .collect::<BTreeSet<_>>();

        assert_eq!(names.len(), CliCommand::ALL.len());
        assert!(!names.contains("stack"));
    }

    #[test]
    fn diagnostic_and_field_registries_are_unique() {
        assert_eq!(
            CliDiagnosticEvent::ALL
                .into_iter()
                .map(CliDiagnosticEvent::as_str)
                .collect::<BTreeSet<_>>()
                .len(),
            CliDiagnosticEvent::ALL.len()
        );
        assert_eq!(
            CliLogField::ALL
                .into_iter()
                .map(CliLogField::as_str)
                .collect::<BTreeSet<_>>()
                .len(),
            CliLogField::ALL.len()
        );
    }
}
