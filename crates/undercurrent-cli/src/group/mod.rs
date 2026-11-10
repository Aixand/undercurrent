// SPDX-License-Identifier: Apache-2.0

//! CLI-owned groups; runtime operations cross the public SDK only.

pub(crate) mod api;
pub(crate) mod renderer;
mod restoration;
mod service;
mod store;
pub(crate) use store::reset_locks;
mod workspace;

use crate::{
    CliContext, CliDiagnosticSink, CliError, CliExecutionLimits, CliPresenter,
    CliProductStreamPort, CommonExitCode, ResolvedCliConfig,
};
use clap::{Args, Subcommand};
use std::io::Write;
use std::path::PathBuf;

/// Arguments for local group management and the terminal interface.
#[derive(Clone, Args, PartialEq, Eq)]
#[command(
    after_help = "Run uc group without a subcommand to open the group picker.\nPicker: Up/Down or k/j selects, h/l pages, Enter opens, q/Esc cancels.\n\nExamples:\n  uc group\n  uc group new project\n  uc group add project dev\n  uc group attach project\n\nRun uc group <command> --help for options."
)]
pub struct GroupArguments {
    /// Omit to open the group picker.
    #[command(subcommand)]
    pub action: Option<GroupAction>,
}

impl GroupArguments {
    pub(crate) fn interactive(&self) -> bool {
        matches!(
            self.action,
            None | Some(GroupAction::New { .. } | GroupAction::Attach { .. })
        )
    }
}

/// Group command surface. Membership changes never alter process lifecycle.
#[derive(Clone, Subcommand, PartialEq, Eq)]
pub enum GroupAction {
    /// Create an empty group without starting a shell or opening a terminal.
    Create {
        /// Unique local group name.
        group: String,
        /// Default directory for new shells.
        #[arg(long)]
        cwd: Option<PathBuf>,
        /// Default interactive shell executable.
        #[arg(long)]
        shell: Option<String>,
        /// Remember terminals subsequently created in this group.
        #[arg(long)]
        persist: bool,
    },
    /// Inspect saved group membership without opening a terminal.
    Inspect {
        /// Existing group name.
        group: String,
    },
    /// Create a group, start its first shell, and open it.
    New {
        /// Unique local group name.
        group: String,
        /// Default directory for new shells.
        #[arg(long)]
        cwd: Option<PathBuf>,
        /// Default interactive shell executable.
        #[arg(long)]
        shell: Option<String>,
        /// Remember interactive terminals and restore lost shells on attach (default).
        #[arg(long, default_value_t = true, conflicts_with = "no_persist")]
        persist: bool,
        /// Disable directory tracking and automatic shell restoration for this group.
        #[arg(long)]
        no_persist: bool,
    },
    /// Reopen a saved group.
    Attach {
        /// Existing group name.
        group: String,
    },
    /// Forget a group, optionally stopping its member processes first.
    Delete {
        /// Existing group name.
        group: String,
        /// Stop member sessions immediately before deleting the group.
        #[arg(long)]
        stop: bool,
        /// Stop immediately (the default with --stop).
        #[arg(long, requires = "stop", conflicts_with = "graceful")]
        force: bool,
        /// Allow each member's configured graceful stop period.
        #[arg(long, requires = "stop")]
        graceful: bool,
    },
    /// Explicitly restore an unavailable terminal whose prior outcome is known.
    Restore {
        /// Existing persistent group.
        group: String,
        /// Saved terminal name or session ID.
        session: String,
    },
    #[command(hide = true)]
    Report {
        #[arg(long)]
        cwd: Option<PathBuf>,
        #[arg(long)]
        closed: bool,
    },
    /// List groups for the configured daemon endpoint.
    Ls,
    /// Add an existing session to a group.
    Add {
        /// Existing group name.
        group: String,
        /// Exact session name.
        session: String,
    },
    /// Remove membership or a pending creation record; never stop a process.
    Remove {
        /// Existing group name.
        group: String,
        /// Session name, stable ID, or pending creation name.
        session: String,
    },
}

pub(super) fn error(message: impl Into<String>) -> CliError {
    CliError::Workspace {
        message: message.into(),
        source: None,
    }
}
pub(super) fn io_error(message: &'static str, source: std::io::Error) -> CliError {
    CliError::Workspace {
        message: message.into(),
        source: Some(source),
    }
}

pub(crate) fn execute<P: CliProductStreamPort, W: Write>(
    arguments: &GroupArguments,
    client: &P,
    configuration: &ResolvedCliConfig,
    limits: CliExecutionLimits,
    context: CliContext,
    output: &mut W,
    diagnostics: &dyn CliDiagnosticSink,
) -> Result<CommonExitCode, CliError>
where
    P::Subscription: 'static,
{
    let service = service::Service::new(client, configuration, limits, context.timeout.get())?;
    match &arguments.action {
        Some(GroupAction::Create {
            group,
            cwd,
            shell,
            persist,
        }) => {
            let saved = service.create_group(group, cwd.as_deref(), shell.as_deref(), *persist)?;
            CliPresenter::new(context).write_finite(
                output,
                &serde_json::json!({"created":true, "name":saved.name}),
                |_| "Group created.".to_owned(),
            )?;
        }
        Some(GroupAction::Inspect { group }) => {
            let saved = api::GroupRecord::from(service.store.group(group)?);
            CliPresenter::new(context).write_finite(output, &saved, |_| {
                let mut text = format!("{}\t{} sessions", saved.name, saved.members.len());
                for member in &saved.members {
                    text.push_str(&format!("\n{}\t{}", member.name, member.session_id));
                }
                text
            })?;
        }
        Some(GroupAction::Ls) => {
            let groups = service.store.groups()?;
            let rows: Vec<_> = groups
                .iter()
                .map(|g| {
                    serde_json::json!({
                        "name": g.name,
                        "members": g.members.len(),
                        "persistent": g.persistent,
                        "deletion_pending": g.deleting,
                        "pending_creation": g.pending.is_some(),
                        "pending_session": g.pending.as_ref().map(|p| &p.request.name),
                    })
                })
                .collect();
            CliPresenter::new(context).write_finite(output, &rows, |_| {
                if groups.is_empty() {
                    "No groups. Use uc group new GROUP.".to_owned()
                } else {
                    groups
                        .iter()
                        .map(|g| {
                            format!(
                                "{}\t{} sessions{}{}{}",
                                g.name,
                                g.members.len(),
                                if g.persistent { " (persistent)" } else { "" },
                                if g.deleting {
                                    " (deletion pending)"
                                } else {
                                    ""
                                },
                                g.pending.as_ref().map_or(String::new(), |p| {
                                    format!(" (creation pending: {})", p.request.name)
                                })
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                }
            })?;
        }
        Some(GroupAction::Delete {
            group,
            stop,
            graceful,
            ..
        }) => {
            service.delete(group, *stop, !*graceful)?;
            CliPresenter::new(context).write_finite(
                output,
                &serde_json::json!({"deleted":true}),
                |_| "Group deleted.".to_owned(),
            )?;
        }
        Some(GroupAction::Restore { group, session }) => {
            let saved = service.store.group(group)?;
            let _lock = service.store.group_lock(&saved.id)?;
            let notices = service.restore(
                group,
                Some(session),
                undercurrent_protocol::TerminalSizeDto {
                    columns: 80,
                    rows: 24,
                },
            )?;
            if !notices.is_empty() {
                return Err(error(format!(
                    "terminal could not be restored: {}",
                    notices.join("; ")
                )));
            }
            CliPresenter::new(context).write_finite(
                output,
                &serde_json::json!({"restored":true}),
                |_| "Terminal restored.".to_owned(),
            )?;
        }
        Some(GroupAction::Add { group, session }) => {
            service.add(group, session)?;
            CliPresenter::new(context).write_finite(
                output,
                &serde_json::json!({"added":true}),
                |_| "Session added to group.".to_owned(),
            )?;
        }
        Some(GroupAction::Remove { group, session }) => {
            service.remove(group, session)?;
            CliPresenter::new(context).write_finite(
                output,
                &serde_json::json!({"removed":true}),
                |_| "Group membership removed; process lifecycle unchanged.".to_owned(),
            )?;
        }
        _ => {
            return workspace::run(
                &service,
                arguments,
                configuration.worker_thread_stack_bytes(),
                output,
                diagnostics,
            );
        }
    }
    Ok(CommonExitCode::Success)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn new_groups_default_to_persistence_with_an_explicit_opt_out() {
        for (flags, expected) in [
            (vec![], true),
            (vec!["--persist"], true),
            (vec!["--no-persist"], false),
        ] {
            let mut args = vec!["uc", "group", "new", "work"];
            args.extend(flags);
            let parsed = crate::NativeCliArguments::try_parse_from(args).unwrap();
            let crate::NativeCliCommand::Group(GroupArguments {
                action:
                    Some(GroupAction::New {
                        persist,
                        no_persist,
                        ..
                    }),
            }) = parsed.command
            else {
                panic!("expected group new");
            };
            assert_eq!(persist && !no_persist, expected);
        }
        assert!(
            crate::NativeCliArguments::try_parse_from([
                "uc",
                "group",
                "new",
                "work",
                "--persist",
                "--no-persist",
            ])
            .is_err()
        );
    }
}
