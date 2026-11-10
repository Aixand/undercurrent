// SPDX-License-Identifier: Apache-2.0

//! Confined, cell-width-aware workspace frames. Child escape sequences are never emitted.

use crossterm::{
    cursor::MoveTo,
    queue,
    style::{Attribute, Print, SetAttribute},
    terminal::{Clear, ClearType},
};
use std::io::Write;
use undercurrent_protocol::TerminalSizeDto;
use unicode_width::UnicodeWidthStr;

use super::{
    forms::{Dialog, Kind},
    model::{App, Tab},
};
use crate::group::renderer::fit;
use crate::{
    CliError, CliOutputError, constraint_text, execution_text, health_text, interaction_text,
    lifecycle_text,
};

fn pad(text: &str, width: usize) -> String {
    let text = fit(text, width);
    format!("{text}{}", " ".repeat(width.saturating_sub(text.width())))
}

fn border(width: usize) -> String {
    format!("+{}+", "-".repeat(width.saturating_sub(2)))
}

fn row(text: &str, width: usize) -> String {
    format!("|{}|", pad(text, width.saturating_sub(2)))
}

pub(super) fn list_height(size: TerminalSizeDto) -> usize {
    let height = usize::from(size.rows);
    height
        .saturating_sub(if height >= 22 { 16 } else { 10 })
        .max(1)
}

pub(super) fn lines(app: &App, size: TerminalSizeDto, connection: &str) -> Vec<String> {
    let width = usize::from(size.columns.saturating_sub(1));
    let height = usize::from(size.rows);
    if width < 32 || height < 12 {
        return [
            "UC Workspace",
            "Terminal too small (minimum 33x12).",
            "Resize to continue; q quits.",
        ]
        .into_iter()
        .map(|line| fit(line, width))
        .take(height)
        .collect();
    }
    let sidebar = if width >= 76 { 20 } else { 0 };
    let table_width = width.saturating_sub(sidebar + 3);
    let rows = list_height(size);
    let mut lines = vec![
        row(
            &format!(
                " UC Workspace | {connection} | {}{}",
                app.filter.name(),
                if app.fresh {
                    ""
                } else {
                    " | STALE / reconnecting"
                }
            ),
            width,
        ),
        row(
            &format!(
                " / Search: {}{}",
                app.search,
                if app.operations > 0 {
                    "   [working...]"
                } else {
                    ""
                }
            ),
            width,
        ),
        border(width),
    ];
    let filters = app.filters();
    let filter_index = filters.iter().position(|f| f == &app.filter).unwrap_or(0);
    let sidebar_start = filter_index.saturating_sub(rows.saturating_sub(2));
    let visible = app.visible();
    let index = visible
        .iter()
        .position(|s| Some(&s.id) == app.selected.as_ref())
        .unwrap_or(0);
    let start = index / rows * rows;
    let header = if table_width >= 70 {
        "    NAME               LIFECYCLE    EXECUTION   INTERACTION    PROFILE"
    } else if table_width >= 58 {
        "    NAME               LIFECYCLE    INTERACTION    PROFILE"
    } else {
        "    NAME               LIFECYCLE"
    };
    for offset in 0..=rows {
        let left = if offset == 0 {
            if app.sidebar {
                "> VIEWS / GROUPS".into()
            } else {
                "  VIEWS / GROUPS".into()
            }
        } else if let Some(filter) = filters.get(sidebar_start + offset - 1) {
            let count = app
                .sessions
                .iter()
                .filter(|s| app.matches(s, filter))
                .count();
            format!(
                "{} {} ({count})",
                if filter == &app.filter { ">" } else { " " },
                filter.name()
            )
        } else {
            String::new()
        };
        let right = if offset == 0 {
            header.into()
        } else if let Some(session) = visible.get(start + offset - 1) {
            let prefix = format!(
                "{}[{}]",
                if Some(&session.id) == app.selected.as_ref() {
                    ">"
                } else {
                    " "
                },
                if app.marked.contains(&session.id) {
                    "x"
                } else {
                    " "
                }
            );
            let base = format!(
                "{prefix} {} {}",
                pad(&session.name, 18),
                pad(&lifecycle_text(session.lifecycle), 11)
            );
            if table_width >= 70 {
                format!(
                    "{base} {} {} {}",
                    pad(execution_text(session.foreground.execution.state), 11),
                    pad(&interaction_text(session), 14),
                    session.profile.as_deref().unwrap_or("-")
                )
            } else if table_width >= 58 {
                format!(
                    "{base} {} {}",
                    pad(&interaction_text(session), 14),
                    session.profile.as_deref().unwrap_or("-")
                )
            } else {
                base
            }
        } else if visible.is_empty() && offset == 1 {
            if app.fresh {
                "No matching sessions. Press n to start one.".into()
            } else {
                "Waiting for sessions...".into()
            }
        } else {
            String::new()
        };
        lines.push(if sidebar > 0 {
            format!("|{}|{}|", pad(&left, sidebar), pad(&right, table_width))
        } else {
            row(&right, width)
        });
    }
    lines.push(row(
        &format!(
            " {} visible / {} sessions | {} selected | PgUp/PgDn page | Tab {}",
            visible.len(),
            app.sessions.len(),
            app.marked.len(),
            if app.sidebar { "sessions" } else { "groups" }
        ),
        width,
    ));
    lines.push(border(width));
    let details_height = height.saturating_sub(lines.len() + 4);
    if details_height > 0 {
        let tabs = match app.tab {
            Tab::Details => "[1 Details]  2 Screen  3 Logs",
            Tab::Screen => "1 Details  [2 Screen]  3 Logs",
            Tab::Logs => "1 Details   2 Screen [3 Logs]",
        };
        lines.push(row(
            &format!(
                " {} | {tabs}",
                app.current()
                    .map_or("No session selected", |s| s.name.as_str())
            ),
            width,
        ));
        let details = details(app);
        for line in details
            .iter()
            .skip(app.preview_scroll)
            .take(details_height.saturating_sub(1))
        {
            lines.push(row(&format!(" {line}"), width));
        }
        while lines.len() < height.saturating_sub(4) {
            lines.push(row("", width));
        }
    }
    lines.push(border(width));
    lines.push(row(&format!(" {}", app.notice), width));
    lines.push(row(if width >= 95 { " Enter attach  Space select  n new  g group  l labels  a actions  / search  ? help  q quit" }
        else { " Enter attach  n new  g group  l labels  ? help  q quit" }, width));
    lines.push(border(width));
    lines.truncate(height);
    if let Some(dialog) = &app.dialog {
        overlay(&mut lines, dialog, width, height);
    }
    lines
}

fn details(app: &App) -> Vec<String> {
    let Some(session) = app.current() else {
        return vec![];
    };
    if app.tab != Tab::Details {
        return app
            .preview
            .as_ref()
            .filter(|p| p.id == session.id && p.tab == app.tab)
            .map_or_else(
                || vec!["Loading preview... (does not resize the session)".into()],
                |p| p.lines.clone(),
            );
    }
    let labels = session
        .labels
        .as_ref()
        .map(|labels| {
            labels
                .iter()
                .filter(|(k, _)| !k.starts_with("uc."))
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("  ")
        })
        .unwrap_or_default();
    vec![
        format!(
            "Group: {}    Directory: {}",
            app.owner(&session.id).unwrap_or("ungrouped"),
            session
                .profile_context
                .current_directory
                .as_deref()
                .unwrap_or("unknown")
        ),
        format!("Labels: {}", if labels.is_empty() { "-" } else { &labels }),
        format!(
            "Lifecycle: {}    Execution: {}    Interaction: {}",
            lifecycle_text(session.lifecycle),
            execution_text(session.foreground.execution.state),
            interaction_text(session)
        ),
        format!(
            "Health: {}    Constraint: {}    Linked: {}/{}    Role: {}",
            health_text(session.health.status),
            constraint_text(session),
            session.work.active_targets,
            session.work.total_targets,
            if session.profile_context.runtime_role.is_some() {
                "server"
            } else {
                "-"
            }
        ),
        format!(
            "Profile: {}    Restarts: {}    {}",
            session.profile.as_deref().unwrap_or("unknown"),
            session.restart_count,
            match session.restart_status {
                undercurrent_protocol::RestartStatusDto::Scheduled { attempt, .. } =>
                    format!("Restart scheduled (attempt {attempt})"),
                undercurrent_protocol::RestartStatusDto::Exhausted { .. } =>
                    "Restart attempts exhausted".into(),
                _ => "No restart scheduled".into(),
            }
        ),
        format!("ID: {}", session.id),
    ]
}

fn overlay(lines: &mut [String], dialog: &Dialog, width: usize, height: usize) {
    let box_width = width.saturating_sub(6).clamp(24, 88).min(width);
    let left = (width - box_width) / 2;
    let mut content = vec![
        border(box_width),
        row(&format!(" {}", dialog.title), box_width),
        border(box_width),
    ];
    let body = if matches!(dialog.kind, Kind::Help) {
        vec![
            "j/k or arrows: select     Tab: groups / sessions".into(),
            "Enter: attach            configured prefix then d: workspace".into(),
            "Space: mark session      u: clear all marks".into(),
            "n: new session           g: group selection".into(),
            "l: edit labels           a: stop / restart actions".into(),
            "/: search names, profiles, IDs, key=value labels".into(),
            "1/2/3: details / screen / logs    [/]: scroll details".into(),
            "PgUp/PgDn: page list     r: refresh     e: full result     q: quit".into(),
            "Forms: Tab/Shift-Tab, arrows, Ctrl-U clear, Esc cancel".into(),
            "Arguments: quote values with spaces; no shell expansion.".into(),
            "Refreshes every second. Preview never resizes a session.".into(),
            "Quitting and detaching leave all processes running.".into(),
            "[ Close ]".into(),
        ]
    } else if matches!(dialog.kind, Kind::Message) {
        dialog.choices.clone()
    } else {
        let mut body = dialog
            .fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                format!(
                    "{} {} [{}]",
                    if index == dialog.active { ">" } else { " " },
                    pad(field.label, 23.min(box_width.saturating_sub(12))),
                    field.display(
                        index == dialog.active,
                        box_width.saturating_sub(23.min(box_width.saturating_sub(12)) + 9)
                    )
                )
            })
            .collect::<Vec<_>>();
        body.extend(dialog.choices.iter().enumerate().map(|(index, choice)| {
            format!(
                "{} [ {choice} ]",
                if dialog.fields.len() + index == dialog.active {
                    ">"
                } else {
                    " "
                }
            )
        }));
        body
    };
    let available = height.saturating_sub(7).max(1);
    let start = dialog
        .active
        .saturating_sub(available.saturating_sub(1))
        .min(body.len().saturating_sub(available));
    for text in body.iter().skip(start).take(available) {
        content.push(row(&format!(" {text}"), box_width));
    }
    if !dialog.targets.is_empty() {
        content.push(row(
            &format!(
                " {} selected: {}",
                dialog.targets.len(),
                dialog
                    .targets
                    .iter()
                    .map(|t| t.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            box_width,
        ));
    }
    if !dialog.error.is_empty() {
        content.push(row(&format!(" {}", dialog.error), box_width));
    }
    content.push(row(
        " Esc cancel | Tab/Up/Down move | Enter choose",
        box_width,
    ));
    content.push(border(box_width));
    content.truncate(height);
    let top = (height - content.len()) / 2;
    for (index, text) in content.into_iter().enumerate() {
        if let Some(line) = lines.get_mut(top + index) {
            // Clear the entire underlying row so wide glyphs cannot straddle the modal.
            *line = format!("{}{}", " ".repeat(left), text);
        }
    }
}

pub(super) fn draw(
    output: &mut impl Write,
    lines: Vec<String>,
    previous: &mut Vec<String>,
) -> Result<(), CliError> {
    let mut frame = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if previous.get(index) != Some(line) {
            queue!(
                frame,
                MoveTo(0, index as u16),
                SetAttribute(Attribute::Reset),
                Clear(ClearType::CurrentLine),
                Print(line)
            )
            .map_err(CliOutputError::from)?;
        }
    }
    if !frame.is_empty() {
        output
            .write_all(&frame)
            .and_then(|()| output.flush())
            .map_err(CliOutputError::from)?;
    }
    *previous = lines;
    Ok(())
}
