// SPDX-License-Identifier: Apache-2.0

//! Local pane ownership. Removing a view never stops its managed session.

use super::layout::{Axis, Layout, Rect};
use super::*;
use crate::detach::ShortcutAction;
use undercurrent_protocol::{MouseButtonDto, MouseEventKindDto};

const MAX_PANES: usize = 16;

pub(super) struct Workspace<'a> {
    panes: Vec<OpenedInteractiveAttach<'a>>,
    active: usize,
    layout: Layout,
    size: TerminalSizeDto,
    zoom: bool,
    clipboard: Option<String>,
    rectangles: Vec<(usize, Rect)>,
    focus_click: bool,
}

impl<'a> Workspace<'a> {
    pub fn new(pane: OpenedInteractiveAttach<'a>) -> Self {
        let size = pane.outer_size;
        Self {
            panes: vec![pane],
            active: 0,
            layout: Layout::Pane(0),
            size,
            zoom: false,
            clipboard: None,
            rectangles: vec![(0, Rect { x: 0, y: 0, size })],
            focus_click: false,
        }
    }

    fn full_rect(&self) -> Rect {
        Rect {
            x: 0,
            y: 0,
            size: self.size,
        }
    }

    fn arrange(&mut self, writer: &mut impl Write) -> Result<(), CliError> {
        let mut rectangles = Vec::new();
        if self.zoom || !self.layout.rectangles(self.full_rect(), &mut rectangles) {
            rectangles = vec![(self.active, self.full_rect())];
        }
        let tiled = self.panes.len() > 1;
        for (index, pane) in self.panes.iter_mut().enumerate() {
            pane.visible = false;
            pane.focused = index == self.active;
            pane.tiled = tiled;
            pane.renderer.invalidate();
            pane.dirty = true;
        }
        for (index, rect) in &rectangles {
            let pane = &mut self.panes[*index];
            pane.visible = true;
            pane.origin = (rect.x, rect.y);
            if pane.outer_size != rect.size {
                pane.resize(rect.size)?;
            }
        }
        self.rectangles = rectangles;
        write_bytes(writer, b"\x1b[0m\x1b[2J")?;
        self.draw(writer)
    }

    fn draw(&mut self, writer: &mut impl Write) -> Result<(), CliError> {
        let changed = self.panes.iter().any(|p| p.visible && p.dirty);
        for (index, pane) in self.panes.iter_mut().enumerate() {
            if index != self.active {
                pane.draw(writer)?;
            }
        }
        // The last renderer owns the outer cursor, including after background output.
        self.panes[self.active].dirty |= changed;
        self.panes[self.active].draw(writer)
    }

    fn close(&mut self, index: usize, writer: &mut impl Write) -> Result<(), CliError> {
        self.panes.remove(index).cancel_streams();
        self.layout = self
            .layout
            .clone()
            .remove(index)
            .expect("another pane remains");
        if self.active > index {
            self.active -= 1;
        }
        self.active = self.active.min(self.panes.len() - 1);
        self.arrange(writer)
    }

    fn focus(&mut self, index: usize, writer: &mut impl Write) -> Result<(), CliError> {
        if self.active != index {
            self.active = index;
            self.arrange(writer)?;
        }
        Ok(())
    }

    pub fn run(
        &mut self,
        terminal: &mut dyn InteractiveTerminalPort,
        writer: &mut impl Write,
        cancellation: &CliStreamCancellation,
        diagnostics: &dyn CliDiagnosticSink,
        detach: &mut DetachMatcher,
    ) -> Result<CommonExitCode, CliError> {
        self.draw(writer)?;
        loop {
            if cancellation.is_cancelled() {
                return Ok(CommonExitCode::Interrupted);
            }
            let mut index = 0;
            while index < self.panes.len() {
                let pane = &mut self.panes[index];
                for _ in 0..MAXIMUM_STREAM_ITEMS_PER_TURN {
                    let Some(item) = receive_available(pane.terminal.as_ref(), cancellation)?
                    else {
                        break;
                    };
                    pane.consume_terminal_item(item, writer, diagnostics)?;
                    // Screen checkpoints can draw immediately; still restore the
                    // focused pane's cursor after a background pane draws.
                    pane.dirty = true;
                }
                if let Some(item) = receive_available(pane.lifecycle.as_ref(), cancellation)?
                    && pane.consume_lifecycle_item(item, diagnostics)?
                {
                    pane.drain_terminal_quiescence(writer, cancellation, diagnostics)?;
                    pane.drain_final_output(writer, diagnostics)?;
                    pane.draw(writer)?;
                    if self.panes.len() == 1 {
                        return Ok(CommonExitCode::Success);
                    }
                    self.close(index, writer)?;
                } else {
                    index += 1;
                }
            }
            self.draw(writer)?;
            let Some(mut event) = terminal.poll_event(IDLE_POLL_INTERVAL)? else {
                continue;
            };
            let mut input = InputBatch::new(self.panes[self.active].limits);
            loop {
                match &mut event {
                    InteractiveTerminalEvent::Resize(size) => {
                        input.examined += 1;
                        input.flush(&self.panes[self.active])?;
                        attach_viewport(*size, self.panes[self.active].limits)?;
                        self.size = *size;
                        self.arrange(writer)?;
                    }
                    InteractiveTerminalEvent::Input(local) => {
                        let mut routed = true;
                        if let TerminalInputDto::Mouse { event: mouse } = &mut local.item {
                            if self.focus_click
                                && mouse.button == MouseButtonDto::Left
                                && mouse.event != MouseEventKindDto::Press
                            {
                                self.focus_click = mouse.event != MouseEventKindDto::Release;
                                routed = false;
                            } else if let Some((index, rect)) = self
                                .rectangles
                                .iter()
                                .find(|(_, rect)| rect.contains(mouse.column, mouse.row))
                                .copied()
                            {
                                if index != self.active
                                    && mouse.button == MouseButtonDto::Left
                                    && mouse.event == MouseEventKindDto::Press
                                {
                                    input.flush(&self.panes[self.active])?;
                                    self.focus(index, writer)?;
                                    // The focus click belongs to UC, not the child application.
                                    routed = false;
                                    self.focus_click = true;
                                }
                                routed &= index == self.active;
                                mouse.column -= rect.x;
                                mouse.row -= rect.y;
                            } else {
                                routed = false;
                            }
                        }
                        if !routed {
                            input.examined += 1;
                        }
                        if routed {
                            let pane = &mut self.panes[self.active];
                            if pane.consume_local_event(
                                event,
                                pane.read_only,
                                detach,
                                &mut input,
                            )? {
                                input.flush(pane)?;
                                return Ok(CommonExitCode::Success);
                            }
                            if let Some(text) = pane.renderer.take_copy() {
                                crate::group::renderer::clipboard(writer, &text)?;
                                self.clipboard = Some(text);
                            }
                        }
                    }
                }
                if detach.has_action()
                    || input.examined >= self.panes[self.active].limits.terminal_input_items()
                {
                    break;
                }
                let Some(next) = terminal.poll_event(INPUT_COALESCE_INTERVAL)? else {
                    break;
                };
                event = next;
            }
            input.flush(&self.panes[self.active])?;
            if let Some(action) = detach.take_action() {
                if action == ShortcutAction::ClosePane && self.panes.len() == 1 {
                    return Ok(CommonExitCode::Success);
                }
                self.action(
                    action,
                    &detach.prefix_label(),
                    terminal,
                    writer,
                    cancellation,
                )?;
            }
        }
    }

    fn action(
        &mut self,
        action: ShortcutAction,
        prefix: &str,
        terminal: &mut dyn InteractiveTerminalPort,
        writer: &mut impl Write,
        cancellation: &CliStreamCancellation,
    ) -> Result<(), CliError> {
        self.panes[self.active].notice.clear();
        self.panes[self.active].dirty = true;
        match action {
            ShortcutAction::Copy => {
                self.panes[self.active].renderer.enter_copy();
                self.panes[self.active].dirty = true;
            }
            ShortcutAction::Paste => {
                let pane = &mut self.panes[self.active];
                if !pane.read_only
                    && let Some(text) = &self.clipboard
                {
                    let item = TerminalInputDto::Paste { text: text.clone() };
                    if !pane.renderer.history_input(&item)
                        && let Some(item) = pane.renderer.input(item)
                    {
                        let mut batch = InputBatch::new(pane.limits);
                        batch.push(pane, item)?;
                        batch.flush(pane)?;
                        pane.dirty = true;
                    }
                }
            }
            ShortcutAction::Focus => self.focus((self.active + 1) % self.panes.len(), writer)?,
            ShortcutAction::Zoom => {
                self.zoom = !self.zoom;
                self.arrange(writer)?;
            }
            ShortcutAction::ClosePane => self.close(self.active, writer)?,
            ShortcutAction::SplitColumns | ShortcutAction::SplitRows => {
                let axis = if action == ShortcutAction::SplitColumns {
                    Axis::Columns
                } else {
                    Axis::Rows
                };
                let mut layout = self.layout.clone();
                layout.split(self.active, self.panes.len(), axis);
                let mut rectangles = Vec::new();
                if self.panes.len() >= MAX_PANES
                    || !layout.rectangles(self.full_rect(), &mut rectangles)
                {
                    self.panes[self.active].notice = if self.panes.len() >= MAX_PANES {
                        "UC · pane limit reached (16)".into()
                    } else {
                        "UC · enlarge the terminal before splitting".into()
                    };
                    return Ok(());
                }
                let occupied = self
                    .panes
                    .iter()
                    .map(|p| p.session_id.clone())
                    .collect::<Vec<_>>();
                if let Some(pane) = self.panes[self.active].show_shortcuts(
                    action,
                    prefix,
                    terminal,
                    writer,
                    cancellation,
                    &occupied,
                )? {
                    self.active = self.panes.len();
                    self.panes.push(pane);
                    self.layout = layout;
                    self.zoom = false;
                }
                if let Some(size) = self.panes[self.active].overlay_resize.take() {
                    self.size = size;
                }
                self.arrange(writer)?;
            }
            ShortcutAction::Help | ShortcutAction::Sessions => {
                let occupied = self
                    .panes
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != self.active)
                    .map(|(_, p)| p.session_id.clone())
                    .collect::<Vec<_>>();
                self.panes[self.active].show_shortcuts(
                    action,
                    prefix,
                    terminal,
                    writer,
                    cancellation,
                    &occupied,
                )?;
                // Overlays can receive an outer-terminal resize.
                if let Some(size) = self.panes[self.active].overlay_resize.take() {
                    self.size = size;
                }
                self.arrange(writer)?;
            }
            ShortcutAction::Create | ShortcutAction::Next | ShortcutAction::Previous => {}
        }
        Ok(())
    }
}

impl Drop for Workspace<'_> {
    fn drop(&mut self) {
        for pane in &self.panes {
            pane.cancel_streams();
        }
    }
}
