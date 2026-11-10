// SPDX-License-Identifier: Apache-2.0

//! Persistent interactive session catalog. Runtime behavior stays behind the SDK.

mod forms;
mod model;
#[cfg(test)]
mod tests;
mod view;
mod worker;

use std::{
    io::Write,
    path::PathBuf,
    sync::mpsc,
    time::{Duration, Instant},
};

use clap::Parser;
use undercurrent_protocol::{KeyDto, TerminalInitialModeDto, TerminalInputDto, TerminalSizeDto};

use crate::{
    CliContext, CliDiagnosticSink, CliError, CliExecutionLimits, CliOutputError,
    CliProductStreamPort, CliStreamCancellation, CommonExitCode, InteractiveAttachExecutor,
    InteractiveTerminalEvent, InteractiveTerminalPort, NativeCliArguments, NativeCliCommand,
    NativeInteractiveTerminal, ResolvedCliConfig,
};

use forms::{Dialog, Field, Kind, LifecycleAction};
use model::{App, Filter, Tab};
use worker::{Action, Job, Reply, Worker};

const REFRESH: Duration = Duration::from_secs(1);
const HOME_MODES: &[u8] = b"\x1b[0m\x1b[?25l\x1b[?7h\x1b[r\x1b[?1006l\x1b[?1015l\x1b[?1003l\x1b[?1002l\x1b[?1000l\x1b[?2004h\x1b[2J\x1b[H";
const LEAVE: &[u8] = b"\x1b[0m\x1b[?25h\x1b[?7h\x1b[r\x1b[?1006l\x1b[?1015l\x1b[?1003l\x1b[?1002l\x1b[?1000l\x1b[?2004l\x1b[?1049l";

fn error(message: impl Into<String>) -> CliError {
    CliError::Workspace {
        message: message.into(),
        source: None,
    }
}

/// One owner for raw mode and the outer alternate screen, including attachment.
struct Screen<'a, W: Write> {
    terminal: NativeInteractiveTerminal,
    output: &'a mut W,
}

impl<'a, W: Write> Screen<'a, W> {
    fn enter(output: &'a mut W) -> Result<Self, CliError> {
        let mut screen = Self {
            terminal: NativeInteractiveTerminal::new(),
            output,
        };
        screen.terminal.enter_raw_mode()?;
        screen
            .output
            .write_all(b"\x1b[?1049h")
            .map_err(CliOutputError::from)?;
        screen.home()?;
        Ok(screen)
    }

    fn home(&mut self) -> Result<(), CliError> {
        self.output
            .write_all(HOME_MODES)
            .and_then(|()| self.output.flush())
            .map_err(CliOutputError::from)?;
        Ok(())
    }
}

impl<W: Write> Drop for Screen<'_, W> {
    fn drop(&mut self) {
        let _ = self
            .output
            .write_all(LEAVE)
            .and_then(|()| self.output.flush());
        let _ = self.terminal.restore();
    }
}

pub(crate) fn run<P: CliProductStreamPort, W: Write>(
    client: &P,
    config: &ResolvedCliConfig,
    context: CliContext,
    connection: &str,
    output: &mut W,
    diagnostics: &dyn CliDiagnosticSink,
) -> Result<CommonExitCode, CliError>
where
    P::Subscription: 'static,
{
    let limits = CliExecutionLimits::from_daemon(config)?;
    let cwd = std::env::current_dir().map_err(|source| CliError::Workspace {
        message: "Could not resolve the workspace directory.".into(),
        source: Some(source),
    })?;
    let cancellation = CliStreamCancellation::default();
    let signal = crate::session_picker::install_signals(
        cancellation.clone(),
        config.worker_thread_stack_bytes(),
    )?;
    let result = std::thread::scope(|scope| {
        let (jobs, requests) = mpsc::sync_channel(1);
        let (results, replies) = mpsc::channel();
        let worker = Worker {
            client,
            config,
            limits,
            timeout: context.timeout.get(),
            cwd: cwd.clone(),
            cancellation: cancellation.clone(),
        };
        std::thread::Builder::new()
            .name("uc-workspace".into())
            .stack_size(config.worker_thread_stack_bytes())
            .spawn_scoped(scope, move || {
                while let Ok(job) = requests.recv() {
                    if worker.cancellation.is_cancelled() {
                        break;
                    }
                    if results.send(worker.execute(job)).is_err() {
                        break;
                    }
                }
            })
            .map_err(|source| CliError::Workspace {
                message: "Could not start workspace worker.".into(),
                source: Some(source),
            })?;
        let mut screen = Screen::enter(output)?;
        let result = run_loop(
            client,
            config,
            context,
            connection,
            limits,
            &cwd,
            &mut screen,
            jobs,
            replies,
            &cancellation,
            diagnostics,
        );
        cancellation.cancel_local();
        result
        // Screen and job sender drop before scope joins: the caller's terminal is restored
        // even if an already-sent request is still finishing under its deadline.
    });
    let finished = signal.finish();
    let code = result?;
    finished?;
    Ok(code)
}

#[allow(clippy::too_many_arguments)]
fn run_loop<P: CliProductStreamPort, W: Write>(
    client: &P,
    config: &ResolvedCliConfig,
    context: CliContext,
    connection: &str,
    limits: CliExecutionLimits,
    cwd: &std::path::Path,
    screen: &mut Screen<'_, W>,
    jobs: mpsc::SyncSender<Job>,
    replies: mpsc::Receiver<Reply>,
    cancellation: &CliStreamCancellation,
    diagnostics: &dyn CliDiagnosticSink,
) -> Result<CommonExitCode, CliError>
where
    P::Subscription: 'static,
{
    let mut app = App::default();
    let mut size = viewport(screen.terminal.size()?, limits);
    let mut previous = vec![];
    let mut last_refresh = Instant::now()
        .checked_sub(REFRESH)
        .unwrap_or_else(Instant::now);
    let mut last_preview = Instant::now();
    let mut dirty = true;
    loop {
        if cancellation.is_cancelled() {
            return Ok(CommonExitCode::Interrupted);
        }
        while let Ok(reply) = replies.try_recv() {
            app.pending = app.pending.saturating_sub(1);
            match reply {
                Reply::Catalog(Ok(catalog)) => {
                    last_refresh = Instant::now();
                    app.apply_catalog(catalog.sessions, catalog.groups);
                }
                Reply::Catalog(Err(e)) => {
                    last_refresh = Instant::now();
                    app.fresh = false;
                    app.notice =
                        format!("Refresh failed: {e}. Retrying; actions disabled until refreshed.");
                }
                Reply::Preview(preview) => {
                    last_preview = Instant::now();
                    app.preview = Some(preview);
                }
                Reply::Completed { message, attach } => {
                    app.operations = app.operations.saturating_sub(1);
                    app.notice = message;
                    last_refresh = Instant::now()
                        .checked_sub(REFRESH)
                        .unwrap_or_else(Instant::now);
                    if let Some(id) = attach {
                        app.selected = Some(id.clone());
                        attach_session(
                            client,
                            config,
                            context,
                            limits,
                            screen,
                            &id,
                            cancellation,
                            diagnostics,
                            &mut app,
                        )?;
                        size = viewport(screen.terminal.size()?, limits);
                        previous.clear();
                    }
                }
            }
            dirty = true;
        }
        if app.pending == 0 {
            if last_refresh.elapsed() >= REFRESH {
                submit(&jobs, Job::Refresh, &mut app)?;
                last_refresh = Instant::now();
                dirty = true;
            } else if app.tab != Tab::Details
                && app.selected.is_some()
                && (last_preview.elapsed() >= REFRESH
                    || app
                        .preview
                        .as_ref()
                        .is_none_or(|p| Some(&p.id) != app.selected.as_ref() || p.tab != app.tab))
            {
                submit(
                    &jobs,
                    Job::Preview {
                        id: app.selected.clone().expect("checked"),
                        tab: app.tab,
                    },
                    &mut app,
                )?;
                last_preview = Instant::now();
                dirty = true;
            }
        }
        if dirty {
            view::draw(
                screen.output,
                view::lines(&app, size, connection),
                &mut previous,
            )?;
            dirty = false;
        }
        let Some(event) = screen.terminal.poll_event(Duration::from_millis(33))? else {
            continue;
        };
        dirty = true;
        let input = match event {
            InteractiveTerminalEvent::Resize(new_size) => {
                size = viewport(new_size, limits);
                screen.home()?;
                previous.clear();
                continue;
            }
            InteractiveTerminalEvent::Input(input) => input.item,
        };
        if matches!(
            input,
            TerminalInputDto::Key {
                key: KeyDto::Control { character: 'c' }
            }
        ) {
            cancellation.cancel_local();
            return Ok(CommonExitCode::Interrupted);
        }
        if let Some(mut dialog) = app.dialog.take() {
            if matches!(
                input,
                TerminalInputDto::Key {
                    key: KeyDto::Escape
                }
            ) {
                continue;
            }
            if dialog.navigate(&input) {
                match finish_dialog(&mut app, &dialog, cwd, size) {
                    Ok(Some(action)) => {
                        if !app.fresh {
                            dialog.error =
                                "Wait for the session list to refresh before submitting.".into();
                            app.dialog = Some(dialog);
                            continue;
                        }
                        if let Err(e) = submit(&jobs, Job::Action(action), &mut app) {
                            dialog.error = e.to_string();
                            app.dialog = Some(dialog);
                        } else {
                            app.notice = "Applying selection...".into();
                        }
                    }
                    Ok(None) => {}
                    Err(message) => {
                        dialog.error = message;
                        app.dialog = Some(dialog);
                    }
                }
            } else {
                app.dialog = Some(dialog);
            }
            continue;
        }
        let text = match &input {
            TerminalInputDto::Text { text } => text.as_str(),
            _ => "",
        };
        let key = match &input {
            TerminalInputDto::Key { key } => Some(key),
            _ => None,
        };
        if text == "q" || key == Some(&KeyDto::Escape) {
            return Ok(CommonExitCode::Success);
        }
        if text == "?" {
            let mut dialog = Dialog::form(Kind::Help, "Workspace help", vec![], vec![]);
            dialog.choices = vec!["Close".into()];
            app.dialog = Some(dialog);
        } else if text == "e" {
            let mut dialog = Dialog::form(
                Kind::Message,
                "Last operation (Up/Down scroll, Enter close)",
                vec![],
                vec![],
            );
            let mut line = String::new();
            dialog.choices.clear();
            for word in app.notice.split_whitespace() {
                if line.len() + word.len() > 58 {
                    dialog.choices.push(std::mem::take(&mut line));
                }
                line.push_str(word);
                line.push(' ');
            }
            dialog.choices.push(line);
            app.dialog = Some(dialog);
        } else if text == "/" {
            app.dialog = Some(Dialog::form(
                Kind::Search,
                "Search names, profiles, IDs or key=value",
                vec![Field::new("Search", &app.search)],
                vec![],
            ));
        } else if key == Some(&KeyDto::Tab) || key == Some(&KeyDto::BackTab) {
            app.sidebar = !app.sidebar;
        } else if text == "j"
            || text == "k"
            || matches!(
                key,
                Some(KeyDto::Up | KeyDto::Down | KeyDto::PageUp | KeyDto::PageDown)
            )
        {
            let step = if matches!(key, Some(KeyDto::PageUp | KeyDto::PageDown)) {
                view::list_height(size) as isize
            } else {
                1
            };
            let step = if text == "k" || matches!(key, Some(KeyDto::Up | KeyDto::PageUp)) {
                -step
            } else {
                step
            };
            if app.sidebar {
                app.move_filter(step);
            } else {
                app.move_selection(step);
            }
        } else if text == " " || key == Some(&KeyDto::Space) {
            if let Some(id) = &app.selected
                && !app.marked.remove(id)
            {
                app.marked.insert(id.clone());
            }
        } else if text == "u" {
            app.marked.clear();
        } else if text == "r" {
            last_refresh = Instant::now()
                .checked_sub(REFRESH)
                .unwrap_or_else(Instant::now);
        } else if ["1", "2", "3"].contains(&text) {
            app.tab = match text {
                "2" => Tab::Screen,
                "3" => Tab::Logs,
                _ => Tab::Details,
            };
            app.preview_scroll = 0;
        } else if text == "[" || text == "]" {
            app.preview_scroll = if text == "[" {
                app.preview_scroll.saturating_sub(1)
            } else {
                (app.preview_scroll + 1).min(
                    app.preview
                        .as_ref()
                        .filter(|p| p.tab == app.tab && Some(&p.id) == app.selected.as_ref())
                        .map_or(5, |p| p.lines.len().saturating_sub(1)),
                )
            };
        } else if key == Some(&KeyDto::Enter) && app.sidebar {
            app.sidebar = false;
        } else if text == "n"
            || text == "g"
            || text == "l"
            || text == "a"
            || key == Some(&KeyDto::Enter)
        {
            if !app.fresh {
                app.notice = "Wait for a current session list before taking actions.".into();
                continue;
            }
            match text {
                "n" => {
                    app.dialog = Some(Dialog::new_session(
                        &cwd.to_string_lossy(),
                        if let Filter::Group(name) = &app.filter {
                            name
                        } else {
                            ""
                        },
                    ))
                }
                "g" => {
                    let mut dialog = Dialog::form(
                        Kind::Groups,
                        "Group selected sessions",
                        vec![],
                        app.targets(),
                    );
                    dialog.choices = vec!["Create new group...".into()];
                    dialog
                        .choices
                        .extend(app.groups.iter().map(|g| g.name.clone()));
                    dialog.choices.push("Remove from current groups".into());
                    app.dialog = Some(dialog);
                }
                "l" if !app.targets().is_empty() => {
                    app.dialog = Some(Dialog::form(
                        Kind::Labels,
                        "Edit labels on selected sessions",
                        vec![
                            Field::new("Set: key=value", ""),
                            Field::new("Remove: keys", ""),
                        ],
                        app.targets(),
                    ))
                }
                "a" if !app.targets().is_empty() => {
                    let mut dialog =
                        Dialog::form(Kind::Actions, "Session actions", vec![], app.targets());
                    dialog.choices = vec![
                        "Stop gracefully".into(),
                        "Force stop".into(),
                        "Restart".into(),
                    ];
                    app.dialog = Some(dialog);
                }
                "" => {
                    if let Some(id) = app.selected.clone() {
                        app.notice = "Attaching...".into();
                        view::draw(
                            screen.output,
                            view::lines(&app, size, connection),
                            &mut previous,
                        )?;
                        attach_session(
                            client,
                            config,
                            context,
                            limits,
                            screen,
                            &id,
                            cancellation,
                            diagnostics,
                            &mut app,
                        )?;
                        size = viewport(screen.terminal.size()?, limits);
                        previous.clear();
                        last_refresh = Instant::now()
                            .checked_sub(REFRESH)
                            .unwrap_or_else(Instant::now);
                    }
                }
                _ => {}
            }
        }
    }
}

fn viewport(size: TerminalSizeDto, limits: CliExecutionLimits) -> TerminalSizeDto {
    TerminalSizeDto {
        columns: size.columns.min(limits.terminal_columns()),
        rows: size.rows.min(limits.terminal_rows()),
    }
}

fn submit(jobs: &mpsc::SyncSender<Job>, job: Job, app: &mut App) -> Result<(), CliError> {
    let operation = matches!(job, Job::Action(_));
    jobs.try_send(job)
        .map_err(|_| error("Workspace is busy; wait for the current action before submitting."))?;
    app.pending += 1;
    app.operations += usize::from(operation);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn attach_session<P: CliProductStreamPort, W: Write>(
    client: &P,
    config: &ResolvedCliConfig,
    context: CliContext,
    limits: CliExecutionLimits,
    screen: &mut Screen<'_, W>,
    id: &str,
    cancellation: &CliStreamCancellation,
    diagnostics: &dyn CliDiagnosticSink,
    app: &mut App,
) -> Result<(), CliError>
where
    P::Subscription: 'static,
{
    let result = InteractiveAttachExecutor::new(client, context.timeout.get(), limits)
        .with_scrollback(config.document().terminal.scrollback)
        .open_session(id, TerminalInitialModeDto::Screen, screen.terminal.size()?)
        .and_then(|opened| {
            opened.consume_in_workspace(
                &mut screen.terminal,
                screen.output,
                cancellation,
                diagnostics,
                crate::shortcuts::matcher(&config.document().shortcuts)?,
            )
        });
    screen.home()?;
    app.notice = match result {
        Ok(_) => "Back in workspace. Detaching leaves the session running; final exits remain in the list.".into(),
        Err(e) => format!("Attachment ended: {e}. Select another session or inspect its logs."),
    };
    Ok(())
}

fn finish_dialog(
    app: &mut App,
    dialog: &Dialog,
    _cwd: &std::path::Path,
    size: TerminalSizeDto,
) -> Result<Option<Action>, String> {
    match &dialog.kind {
        Kind::Help | Kind::Message => Ok(None),
        Kind::Search => {
            app.search.clone_from(&dialog.fields[0].text);
            app.normalize_selection();
            Ok(None)
        }
        Kind::Groups if dialog.active == 0 => {
            app.dialog = Some(Dialog::form(
                Kind::GroupName,
                "Create group from selection",
                vec![Field::new("Group name", "")],
                dialog.targets.clone(),
            ));
            Ok(None)
        }
        Kind::Groups => Ok(Some(Action::Group {
            targets: dialog.targets.clone(),
            destination: (dialog.active + 1 < dialog.choices.len())
                .then(|| dialog.choices[dialog.active].clone()),
            create: false,
        })),
        Kind::GroupName => {
            let name = dialog.fields[0].text.trim();
            if name.is_empty() {
                return Err("Enter a group name.".into());
            }
            Ok(Some(Action::Group {
                targets: dialog.targets.clone(),
                destination: Some(name.into()),
                create: true,
            }))
        }
        Kind::Labels => {
            let set = forms::attributes(&dialog.fields[0].text)?;
            let remove = forms::words(&dialog.fields[1].text)?;
            validate_label_keys(set.keys().chain(remove.iter()))?;
            if set.is_empty() && remove.is_empty() {
                return Err("Enter labels to set or keys to remove.".into());
            }
            if remove.iter().any(|key| set.contains_key(key)) {
                return Err("A label cannot be set and removed together.".into());
            }
            Ok(Some(Action::Labels {
                targets: dialog.targets.clone(),
                set,
                remove,
            }))
        }
        Kind::Actions => {
            let action = match dialog.active {
                0 => LifecycleAction::Stop,
                1 => LifecycleAction::ForceStop,
                _ => LifecycleAction::Restart,
            };
            let mut confirm = Dialog::form(
                Kind::Confirm(action),
                format!("{} {} session(s)?", action.name(), dialog.targets.len()),
                vec![],
                dialog.targets.clone(),
            );
            confirm.choices = vec!["Cancel".into(), format!("Confirm: {}", action.name())];
            app.dialog = Some(confirm);
            Ok(None)
        }
        Kind::Confirm(action) => Ok((dialog.active == 1).then(|| Action::Lifecycle {
            targets: dialog.targets.clone(),
            action: *action,
        })),
        Kind::New => prepare_new(dialog, size).map(Some),
    }
}

fn validate_label_keys<'a>(keys: impl Iterator<Item = &'a String>) -> Result<(), String> {
    if keys.into_iter().any(|key| key.starts_with("uc.")) {
        Err("uc.* labels are reserved for session and group recovery.".into())
    } else {
        Ok(())
    }
}

fn prepare_new(dialog: &Dialog, size: TerminalSizeDto) -> Result<Action, String> {
    let values = dialog
        .fields
        .iter()
        .map(|f| f.text.as_str())
        .collect::<Vec<_>>();
    if values[0].trim().is_empty() {
        return Err("Enter a session name.".into());
    }
    if values[2].trim().is_empty() {
        return Err("Enter an executable or shell.".into());
    }
    let parsed =
        NativeCliArguments::try_parse_from(["uc", "start", "workspace-session", "--interactive"])
            .map_err(|_| "Could not prepare session defaults.")?;
    let NativeCliCommand::Start(mut arguments) = parsed.command else {
        unreachable!()
    };
    arguments.name = values[0].trim().into();
    arguments.interactive = values[1] == "shell";
    if arguments.interactive {
        if !values[3].trim().is_empty() {
            return Err("Use Command mode to pass arguments.".into());
        }
        arguments.shell = Some(values[2].into());
    } else {
        arguments.command = vec![values[2].into()];
        arguments.command.extend(forms::words(values[3])?);
    }
    if !values[4].trim().is_empty() {
        arguments.cwd = Some(PathBuf::from(values[4]));
    }
    let labels = forms::attributes(values[6])?;
    validate_label_keys(labels.keys())?;
    arguments.label = labels
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    arguments.environment = forms::attributes(values[7])?
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    if !values[8].trim().is_empty() {
        arguments.profile = Some(values[8].trim().into());
    }
    arguments.restart = match values[9] {
        "on-failure" => crate::RestartModeArgument::OnFailure,
        "always" => crate::RestartModeArgument::Always,
        _ => crate::RestartModeArgument::Never,
    };
    arguments.columns = size.columns.max(1);
    arguments.rows = size.rows.saturating_sub(1).max(1);
    Ok(Action::Start {
        arguments,
        group: (!values[5].trim().is_empty()).then(|| values[5].trim().into()),
        attach: dialog.active == dialog.fields.len() + 1,
    })
}
