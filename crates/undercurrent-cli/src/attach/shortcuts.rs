// SPDX-License-Identifier: Apache-2.0

//! Local shortcut help and paged session selection.

use super::*;
use crate::detach::ShortcutAction;
use crate::session_picker::{PickerKey, SessionPicker};
use undercurrent_protocol::KeyDto;

impl OpenedInteractiveAttach<'_> {
    pub(super) fn show_shortcuts<W: Write>(
        &mut self,
        action: ShortcutAction,
        prefix: &str,
        terminal: &mut dyn InteractiveTerminalPort,
        writer: &mut W,
        cancellation: &CliStreamCancellation,
        occupied: &[String],
    ) -> Result<Option<Self>, CliError> {
        let splitting = matches!(
            action,
            ShortcutAction::SplitColumns | ShortcutAction::SplitRows
        );
        let mut picker = if action == ShortcutAction::Sessions || splitting {
            Some(SessionPicker::new(self.client, self.timeout, self.limits)?)
        } else {
            None
        };
        loop {
            let height = usize::from(self.terminal_size.rows);
            let mut lines = Vec::new();
            if let Some(picker) = &picker {
                let title = if splitting {
                    if self.read_only {
                        "Split · Enter: open · q/Esc: close"
                    } else {
                        "Split · Enter: open · C: new shell · q/Esc: close"
                    }
                } else {
                    "Sessions · Enter: open · q/Esc: close"
                };
                let empty = if splitting && !self.read_only {
                    "No sessions. C: new shell · q/Esc: close"
                } else {
                    "No sessions. q/Esc: close"
                };
                lines = picker.lines(self.terminal_size, title, empty, |session| {
                    let suffix = if occupied.contains(&session.id) {
                        " (visible)"
                    } else if session.id == self.session_id {
                        " (current)"
                    } else {
                        ""
                    };
                    format!("{}{suffix}", session.name)
                });
            } else {
                lines.extend([
                    format!("Undercurrent — {prefix}"),
                    format!("{prefix}, d: detach"),
                    format!("{prefix}, s: sessions"),
                    format!("{prefix}, [: copy mode  ]: paste buffer"),
                    format!("{prefix}, %: split columns  \" : split rows"),
                    format!("{prefix}, o: next pane  z: zoom  x: close pane"),
                    format!("{prefix}, ?: help"),
                    format!("{prefix} twice: send literal prefix"),
                    "Copy: arrows/hjkl move, v/Space select, V line, y copy".to_owned(),
                    "Shift+PageUp/PageDown: UC history".to_owned(),
                    "Escape/q: close this view · Escape: cancel prefix".to_owned(),
                ]);
            }
            // Bound rows and display columns before parsing: narrow panes must not
            // wrap their instructions and scroll the selected session out of view.
            let display = lines
                .iter()
                .take(height)
                .map(|line| fit(line, usize::from(self.terminal_size.columns)))
                .collect::<Vec<_>>()
                .join("\r\n");
            let mut overlay = Renderer::with_scrollback(
                self.terminal_size,
                undercurrent_config::ScrollbackConfig { memory_lines: 0 },
            );
            overlay.reset(display.as_bytes(), false);
            if self.outer_size.rows == 1 {
                self.dirty = true;
                self.draw(writer)?;
            } else {
                if self.clear_frame {
                    self.renderer.invalidate();
                    self.clear_frame = false;
                }
                overlay.draw_region(
                    writer,
                    &self.bar(),
                    self.outer_size.rows - 1,
                    self.origin,
                    true,
                )?;
            }
            loop {
                if cancellation.is_cancelled() {
                    return Ok(None);
                }
                let Some(event) = terminal.poll_event(IDLE_POLL_INTERVAL)? else {
                    continue;
                };
                match event {
                    InteractiveTerminalEvent::Resize(size) => {
                        self.overlay_resize = Some(size);
                        // Return to the workspace to recompute every pane before redrawing.
                        self.renderer.invalidate();
                        self.dirty = true;
                        return Ok(None);
                    }
                    InteractiveTerminalEvent::Input(input) => {
                        if let Some(picker) = &mut picker
                            && picker.navigate(&input.item)
                        {
                            break;
                        }
                        if PickerKey::from_input(&input.item) == Some(PickerKey::Cancel) {
                            self.resynchronize_screen(writer)?;
                            return Ok(None);
                        }
                        match input.item {
                            TerminalInputDto::Key { key: KeyDto::Enter } => {
                                if let Some(picker) = &mut picker
                                    && let Some(session) = picker.selected()
                                {
                                    if occupied.contains(&session.id) {
                                        picker.message =
                                            "That session is already visible in a pane.\r\n"
                                                .to_owned();
                                        break;
                                    }
                                    if session.id == self.session_id {
                                        self.resynchronize_screen(writer)?;
                                        return Ok(None);
                                    }
                                    match (self.reopen)(&session.id, self.outer_size) {
                                        Ok(mut replacement) => {
                                            replacement.read_only = self.read_only;
                                            replacement.detach_hint.clone_from(&self.detach_hint);
                                            if splitting {
                                                return Ok(Some(replacement));
                                            }
                                            replacement.origin = self.origin;
                                            replacement.tiled = self.tiled;
                                            replacement.focused = self.focused;
                                            self.cancel_streams();
                                            *self = replacement;
                                            self.draw(writer)?;
                                            return Ok(None);
                                        }
                                        Err(_) => {
                                            picker.message = "Could not attach; select another session or press Esc.\r\n".to_owned();
                                        }
                                    }
                                    break;
                                }
                            }
                            TerminalInputDto::Text { text }
                                if splitting && text == "c" && !self.read_only =>
                            {
                                match self.create_shell() {
                                    Ok(mut pane) => {
                                        pane.detach_hint.clone_from(&self.detach_hint);
                                        return Ok(Some(pane));
                                    }
                                    Err(error) => {
                                        picker.as_mut().expect("split picker").message = format!(
                                            "Could not create/open shell: {}\r\n",
                                            fit(
                                                &error.to_string(),
                                                usize::from(self.terminal_size.columns)
                                            )
                                        )
                                    }
                                }
                                break;
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }

    fn create_shell(&self) -> Result<Self, CliError> {
        use clap::Parser;
        let name = format!("pane-{}", uuid::Uuid::now_v7());
        let arguments = crate::NativeCliArguments::try_parse_from([
            "uc",
            "start",
            &name,
            "--interactive",
            "--env",
            "TERM=xterm-256color",
        ])
        .map_err(|_| attach_invariant())?;
        let crate::NativeCliCommand::Start(mut arguments) = arguments.command else {
            return Err(attach_invariant());
        };
        arguments.columns = self.terminal_size.columns;
        arguments.rows = self.terminal_size.rows;
        let context = crate::StartPreparationContext::new(
            std::env::current_dir().map_err(CliOutputError::from)?,
            Some(crate::preparation::native_default_shell()),
        )?;
        let request = crate::prepare_start_request(
            &arguments,
            &context,
            self.mutations
                .next(CliCommand::Start, ProductOperation::StartSession),
        )?;
        match self
            .client
            .request(ProductRequest::StartSession(request), self.timeout)?
        {
            ProductSuccess::StartSession(result) => {
                (self.reopen)(&result.session.id, self.outer_size)
            }
            other => Err(operation_mismatch(ProductOperation::StartSession, other)),
        }
    }
}
