// SPDX-License-Identifier: Apache-2.0

//! Bounded background reads and explicit mutations over the existing SDK connection.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    time::Duration,
};

use crate::deadline::CliCommandDeadline;
use crate::{
    CliCommand, CliError, CliExecutionLimits, CliMutationContextSource, CliProductStreamPort,
    CliStreamCancellation, GroupClient, GroupRecord, ResolvedCliConfig, StartArguments,
    StartPreparationContext, expect_list, prepare_start_request,
};
use undercurrent_protocol::*;

use super::{
    error,
    forms::LifecycleAction,
    model::{Preview, Tab, Target},
};

pub(super) enum Action {
    Start {
        arguments: Box<StartArguments>,
        group: Option<String>,
        attach: bool,
    },
    Group {
        targets: Vec<Target>,
        destination: Option<String>,
        create: bool,
    },
    Labels {
        targets: Vec<Target>,
        set: BTreeMap<String, String>,
        remove: Vec<String>,
    },
    Lifecycle {
        targets: Vec<Target>,
        action: LifecycleAction,
    },
}

pub(super) enum Job {
    Refresh,
    Preview { id: String, tab: Tab },
    Action(Action),
}

pub(super) struct Catalog {
    pub sessions: Vec<SessionSummaryDto>,
    pub groups: Vec<GroupRecord>,
}

pub(super) enum Reply {
    Catalog(Result<Catalog, CliError>),
    Preview(Preview),
    Completed {
        message: String,
        attach: Option<String>,
    },
}

pub(super) struct Worker<'a, P> {
    pub client: &'a P,
    pub config: &'a ResolvedCliConfig,
    pub limits: CliExecutionLimits,
    pub timeout: Duration,
    pub cwd: PathBuf,
    pub cancellation: CliStreamCancellation,
}

impl<P: CliProductStreamPort> Worker<'_, P> {
    pub fn execute(&self, job: Job) -> Reply {
        match job {
            Job::Refresh => Reply::Catalog(self.catalog()),
            Job::Preview { id, tab } => {
                let lines = self
                    .preview(&id, tab)
                    .unwrap_or_else(|e| vec![format!("Preview unavailable: {e}")]);
                Reply::Preview(Preview { id, tab, lines })
            }
            Job::Action(action) => match self.action(action) {
                Ok((message, attach)) => Reply::Completed { message, attach },
                Err(e) => Reply::Completed {
                    message: e.to_string(),
                    attach: None,
                },
            },
        }
    }

    fn deadline(&self) -> Result<CliCommandDeadline, CliError> {
        CliCommandDeadline::start(CliCommand::Tui, self.timeout)
    }

    fn remaining(&self, deadline: CliCommandDeadline) -> Result<Duration, CliError> {
        if self.cancellation.is_cancelled() {
            return Err(error(
                "Workspace interrupted; remaining actions were not sent.",
            ));
        }
        deadline.remaining()
    }

    pub(super) fn catalog(&self) -> Result<Catalog, CliError> {
        let deadline = self.deadline()?;
        // Closing attach streams can briefly interrupt their shared connection. Retry only
        // this read-only snapshot, from page one, within the same total deadline. Never
        // reuse an old connection's cursor or replay an action after an uncertain result.
        for attempt in 0..3 {
            match self.catalog_once(deadline) {
                Err(CliError::Product(ref e))
                    if attempt < 2
                        && matches!(
                            e.category(),
                            undercurrent_sdk::ProductClientErrorCategory::Unavailable
                                | undercurrent_sdk::ProductClientErrorCategory::Overloaded
                        ) =>
                {
                    let delay =
                        Duration::from_millis(100 << attempt).min(self.remaining(deadline)?);
                    std::thread::sleep(delay);
                }
                result => return result,
            }
        }
        unreachable!("last attempt always returns")
    }

    fn catalog_once(&self, deadline: CliCommandDeadline) -> Result<Catalog, CliError> {
        let groups =
            GroupClient::new(self.client, self.config, self.remaining(deadline)?)?.list()?;
        let mut sessions = vec![];
        let mut ids = BTreeSet::new();
        let mut cursors = BTreeSet::new();
        let mut cursor = None;
        loop {
            let page = expect_list(self.client.request(
                ProductRequest::ListSessions(ListSessionsRequest {
                    filter: Default::default(),
                    sort: Default::default(),
                    projection: SessionProjectionDto::WithLabels,
                    page_size: self.limits.catalog_page_size().min(100),
                    cursor,
                    include_total: false,
                }),
                self.remaining(deadline)?,
            )?)?;
            for summary in page.items {
                if !ids.insert(summary.id.clone()) {
                    return Err(error(
                        "Session catalog repeated an identity; refresh to retry.",
                    ));
                }
                sessions.push(summary);
                if sessions.len() > self.limits.bulk_selection_sessions() {
                    return Err(error(
                        "Session catalog exceeds the configured session limit.",
                    ));
                }
            }
            cursor = page.next_cursor;
            let Some(next) = &cursor else {
                break;
            };
            if !cursors.insert(next.clone())
                || cursors.len() > self.limits.bulk_selection_sessions()
            {
                return Err(error(
                    "Session catalog cursor did not advance; refresh to retry.",
                ));
            }
        }
        Ok(Catalog { sessions, groups })
    }

    fn preview(&self, id: &str, tab: Tab) -> Result<Vec<String>, CliError> {
        let deadline = self.deadline()?;
        if tab == Tab::Screen {
            let result = self.client.request(
                ProductRequest::ReadTerminalScreen(ReadTerminalScreenRequest {
                    session_id: id.into(),
                }),
                self.remaining(deadline)?,
            )?;
            let ProductSuccess::ReadTerminalScreen(screen) = result else {
                return Err(error("Unexpected screen response."));
            };
            if screen.rows == 0
                || screen.columns == 0
                || screen.rows > self.limits.terminal_rows()
                || screen.columns > self.limits.terminal_columns()
            {
                return Err(error("Screen exceeds the configured terminal dimensions."));
            }
            let mut parser = vt100::Parser::new(screen.rows, screen.columns, 0);
            parser.process(&screen.contents);
            return Ok(parser
                .screen()
                .contents()
                .lines()
                .map(str::to_owned)
                .collect());
        }
        // Discover the retained end, then read only its bounded tail. No resize/input effects.
        let read = |offset, size| -> Result<TerminalOutputPageDto, CliError> {
            match self.client.request(
                ProductRequest::ReadTerminalOutput(ReadTerminalOutputRequest {
                    session_id: id.into(),
                    from_offset: offset,
                    max_bytes: size,
                }),
                self.remaining(deadline)?,
            )? {
                ProductSuccess::ReadTerminalOutput(page) => Ok(page),
                _ => Err(error("Unexpected output response.")),
            }
        };
        let (available, end) = match read(0, 1)? {
            TerminalOutputPageDto::Data { next_offset, .. } => (0, next_offset),
            TerminalOutputPageDto::Gap {
                available_from,
                next_offset,
                ..
            } => (available_from, next_offset),
        };
        let limit = self.limits.terminal_output_page_bytes().min(16_384);
        let page = read(end.saturating_sub(u64::from(limit)).max(available), limit)?;
        let bytes = match page {
            TerminalOutputPageDto::Data { bytes, .. } => bytes,
            TerminalOutputPageDto::Gap { .. } => {
                return Ok(vec!["Output rotated during preview; refreshing...".into()]);
            }
        };
        let mut parser = vt100::Parser::new(100, 160, 0);
        parser.process(&bytes);
        let mut lines = vec!["Recent output (up to 16 KiB; earlier history may be omitted)".into()];
        lines.extend(parser.screen().contents().lines().map(str::to_owned));
        Ok(lines)
    }

    fn action(&self, action: Action) -> Result<(String, Option<String>), CliError> {
        let deadline = self.deadline()?;
        let mutations = CliMutationContextSource::default();
        if let Action::Start {
            arguments,
            group,
            attach,
        } = action
        {
            let context = StartPreparationContext::new(
                &self.cwd,
                Some(crate::preparation::native_default_shell()),
            )?;
            let mut request = prepare_start_request(
                &arguments,
                &context,
                mutations.next(CliCommand::Tui, ProductOperation::StartSession),
            )?;
            let name = request.name.clone();
            let result = if let Some(group) = &group {
                GroupClient::new(self.client, self.config, self.remaining(deadline)?)?
                    .start_prepared(group, request)
            } else {
                request
                    .labels
                    .insert("uc.tui.creation".into(), uuid::Uuid::now_v7().to_string());
                match self.client.request(
                    ProductRequest::StartSession(request),
                    self.remaining(deadline)?,
                ) {
                    Ok(ProductSuccess::StartSession(result)) => Ok(result.session),
                    Ok(_) => Err(error("Unexpected start response.")),
                    Err(e) => Err(e.into()),
                }
            };
            return match result {
                Ok(session) => Ok((
                    format!("Started {}.", session.name),
                    attach.then_some(session.id),
                )),
                Err(e) => Err(error(format!(
                    "Start {name}: {e}. Check the session list before starting again; no retry was sent.{}",
                    if group.is_some() {
                        " Uncertain group creation remains saved for reconciliation."
                    } else {
                        ""
                    }
                ))),
            };
        }
        let (targets, operation) = match &action {
            Action::Group { targets, .. } => (targets, "Group"),
            Action::Labels { targets, .. } => (targets, "Labels"),
            Action::Lifecycle { targets, action } => (targets, action.name()),
            Action::Start { .. } => unreachable!(),
        };
        if targets.len() > self.limits.bulk_selection_sessions() {
            return Err(error("Too many selected sessions."));
        }
        if let Action::Group {
            create: true,
            destination: Some(name),
            ..
        } = &action
        {
            GroupClient::new(self.client, self.config, self.remaining(deadline)?)?.create(
                name,
                &self.cwd,
                &crate::preparation::native_default_shell(),
                false,
            )?;
        }
        let mut succeeded = 0;
        let mut failures = vec![];
        for target in targets {
            let result = (|| {
                let timeout = self.remaining(deadline)?;
                let group = GroupClient::new(self.client, self.config, timeout)?;
                match &action {
                    Action::Group { destination, .. } => {
                        group.assign(&target.id, target.group.as_deref(), destination.as_deref())
                    }
                    Action::Labels { set, remove, .. } => {
                        let result = self.client.request(
                            ProductRequest::UpdateSessionLabels(UpdateSessionLabelsRequest {
                                mutation: mutations
                                    .next(CliCommand::Tui, ProductOperation::UpdateSessionLabels),
                                session_id: target.id.clone(),
                                set: set.clone(),
                                remove: remove.clone(),
                            }),
                            timeout,
                        )?;
                        if !matches!(result, ProductSuccess::UpdateSessionLabels(_)) {
                            return Err(error("Unexpected label response."));
                        }
                        group
                            .remember_label_patch(&target.id, set, remove)
                            .map_err(|e| {
                                error(format!(
                                    "Labels changed, but saved shell recipe was not updated: {e}"
                                ))
                            })
                    }
                    Action::Lifecycle { action, .. } => {
                        let request = match action {
                            LifecycleAction::Stop | LifecycleAction::ForceStop => {
                                ProductRequest::StopSession(StopSessionRequest {
                                    mutation: mutations
                                        .next(CliCommand::Tui, ProductOperation::StopSession),
                                    session_id: target.id.clone(),
                                    expected_revision: Some(target.revision),
                                    force: *action == LifecycleAction::ForceStop,
                                    graceful_only: false,
                                })
                            }
                            LifecycleAction::Restart => {
                                ProductRequest::RestartSession(RestartSessionRequest {
                                    mutation: mutations
                                        .next(CliCommand::Tui, ProductOperation::RestartSession),
                                    session_id: target.id.clone(),
                                    force: false,
                                })
                            }
                        };
                        let response = self.client.request(request, timeout)?;
                        match response {
                            ProductSuccess::StopSession(_) | ProductSuccess::RestartSession(_) => {
                                Ok(())
                            }
                            _ => Err(error("Unexpected lifecycle response.")),
                        }
                    }
                    _ => unreachable!(),
                }
            })();
            match result {
                Ok(()) => succeeded += 1,
                Err(e) => failures.push(format!("{}: {e}", target.name)),
            }
        }
        let message = if failures.is_empty() {
            if targets.is_empty() {
                "Created empty group.".into()
            } else {
                format!("{operation}: {succeeded}/{} succeeded.", targets.len())
            }
        } else {
            format!(
                "{operation}: {succeeded}/{} succeeded; {}. Failed or uncertain actions were not retried.",
                targets.len(),
                failures.join("; ")
            )
        };
        Ok((message, None))
    }
}
