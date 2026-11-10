// SPDX-License-Identifier: Apache-2.0

//! Shared paged session selection for attach overlays and standalone pickers.

use crate::group::renderer::fit;
use crate::{
    CliError, CliExecutionLimits, CliOutputError, CliProductRequestPort, CliProductSubscription,
    CliStreamCancellation, CommonExitCode, InteractiveTerminalEvent, InteractiveTerminalPort,
    NativeStreamSignalGuard, expect_list,
};
use crossterm::{
    cursor::MoveTo,
    queue,
    style::Print,
    terminal::{Clear, ClearType},
};
use std::{io::Write, sync::Arc, time::Duration};
use undercurrent_protocol::{
    KeyDto, ListSessionsRequest, ProductRequest, SessionPageDto, SessionSummaryDto,
    TerminalInputDto, TerminalSizeDto,
};
use undercurrent_sdk::{ProductClientError, ProductSubscriptionItem};

const IDLE_POLL_INTERVAL: Duration = Duration::from_millis(10);

const ENTER: &[u8] = b"\x1b[?1049h\x1b[?25l\x1b[0m\x1b[2J\x1b[H";
const LEAVE: &[u8] = b"\x1b[0m\x1b[?25h\x1b[?1049l";

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum PickerKey {
    Up,
    Down,
    PreviousPage,
    NextPage,
    Refresh,
    Cancel,
}

impl PickerKey {
    pub(crate) fn from_input(input: &TerminalInputDto) -> Option<Self> {
        match input {
            TerminalInputDto::Key { key: KeyDto::Up } => Some(Self::Up),
            TerminalInputDto::Key { key: KeyDto::Down } => Some(Self::Down),
            TerminalInputDto::Key {
                key: KeyDto::Escape,
            } => Some(Self::Cancel),
            TerminalInputDto::Text { text } => match text.as_str() {
                "k" => Some(Self::Up),
                "j" => Some(Self::Down),
                "h" => Some(Self::PreviousPage),
                "l" | "n" => Some(Self::NextPage),
                "r" => Some(Self::Refresh),
                "q" => Some(Self::Cancel),
                _ => None,
            },
            _ => None,
        }
    }

    /// Navigation in a local list; page boundaries keep the current selection.
    pub(crate) fn navigate(self, selected: &mut usize, count: usize, rows: usize) -> bool {
        let rows = rows.max(1);
        match self {
            Self::Up => *selected = selected.saturating_sub(1),
            Self::Down => *selected = (*selected + 1).min(count.saturating_sub(1)),
            Self::PreviousPage => {
                if *selected >= rows {
                    *selected = (*selected / rows - 1) * rows;
                }
            }
            Self::NextPage => {
                let next = (*selected / rows + 1) * rows;
                if next < count {
                    *selected = next;
                }
            }
            _ => return false,
        }
        true
    }
}

pub(crate) struct SessionPicker<'a> {
    client: &'a dyn CliProductRequestPort,
    timeout: Duration,
    limits: CliExecutionLimits,
    page: SessionPageDto,
    cursor: Option<String>,
    previous_cursors: Vec<Option<String>>,
    selected: usize,
    pub(crate) message: String,
}

impl<'a> SessionPicker<'a> {
    pub(crate) fn new(
        client: &'a dyn CliProductRequestPort,
        timeout: Duration,
        limits: CliExecutionLimits,
    ) -> Result<Self, CliError> {
        let page = Self::load(client, timeout, limits, None)?;
        Ok(Self {
            client,
            timeout,
            limits,
            page,
            cursor: None,
            previous_cursors: Vec::new(),
            selected: 0,
            message: String::new(),
        })
    }

    fn load(
        client: &dyn CliProductRequestPort,
        timeout: Duration,
        limits: CliExecutionLimits,
        cursor: Option<String>,
    ) -> Result<SessionPageDto, CliError> {
        expect_list(client.request(
            ProductRequest::ListSessions(ListSessionsRequest {
                filter: Default::default(),
                sort: Default::default(),
                projection: Default::default(),
                page_size: limits.catalog_page_size().min(100),
                cursor,
                include_total: false,
            }),
            timeout,
        )?)
    }

    pub(crate) fn selected(&self) -> Option<&SessionSummaryDto> {
        self.page.items.get(self.selected)
    }

    /// Consumes navigation only; Enter and cancellation belong to the caller.
    pub(crate) fn navigate(&mut self, input: &TerminalInputDto) -> bool {
        match PickerKey::from_input(input) {
            Some(PickerKey::Up) => {
                self.selected = self.selected.saturating_sub(1);
            }
            Some(PickerKey::Down) => {
                self.selected = (self.selected + 1).min(self.page.items.len().saturating_sub(1));
            }
            Some(action @ (PickerKey::PreviousPage | PickerKey::NextPage | PickerKey::Refresh)) => {
                let next = match action {
                    PickerKey::Refresh => None,
                    PickerKey::PreviousPage => {
                        let Some(previous) = self.previous_cursors.last() else {
                            return true;
                        };
                        previous.clone()
                    }
                    PickerKey::NextPage => {
                        let Some(next) = self.page.next_cursor.clone() else {
                            return true;
                        };
                        if self.cursor.as_ref() == Some(&next)
                            || self.previous_cursors.contains(&Some(next.clone()))
                            || self.previous_cursors.len() >= self.limits.bulk_selection_sessions()
                        {
                            self.message = "Session list changed; press R to refresh.".to_owned();
                            return true;
                        }
                        Some(next)
                    }
                    _ => unreachable!("page navigation"),
                };
                match Self::load(self.client, self.timeout, self.limits, next.clone()) {
                    Ok(page) => {
                        match action {
                            PickerKey::PreviousPage => {
                                self.previous_cursors.pop();
                            }
                            PickerKey::NextPage => self.previous_cursors.push(self.cursor.clone()),
                            PickerKey::Refresh => self.previous_cursors.clear(),
                            _ => unreachable!("page navigation"),
                        }
                        self.page = page;
                        self.cursor = next;
                        self.selected = 0;
                        self.message.clear();
                    }
                    Err(_) => {
                        self.message = "Could not load sessions; press R to retry.".to_owned()
                    }
                }
            }
            _ => return false,
        }
        true
    }

    pub(crate) fn lines(
        &self,
        size: TerminalSizeDto,
        title: &str,
        empty: &str,
        label: impl Fn(&SessionSummaryDto) -> String,
    ) -> Vec<String> {
        let height = usize::from(size.rows);
        let mut lines = Vec::new();
        if height >= 3 {
            lines.push(title.to_owned());
        }
        if height >= 5 {
            lines.push("Up/k Down/j: select · h/l: page · n: next · r: refresh".to_owned());
        }
        let rows = height
            .saturating_sub(lines.len() + usize::from(!self.message.is_empty()))
            .max(1);
        let start = self.selected / rows * rows;
        for (index, session) in self.page.items.iter().enumerate().skip(start).take(rows) {
            let marker = if index == self.selected { ">" } else { " " };
            lines.push(format!("{marker} {}", label(session)));
        }
        if self.page.items.is_empty() {
            lines.push(empty.to_owned());
        }
        if !self.message.is_empty() {
            lines.push(self.message.clone());
        }
        lines
    }
}

pub(crate) enum Outcome<T> {
    Selected(T),
    Exit(CommonExitCode),
}

pub(crate) struct PickerView {
    pub title: &'static str,
    pub selection_error: &'static str,
    pub label: fn(&SessionSummaryDto) -> String,
}

// No remote subscription exists until a session is selected. Signals still need
// to wake the local picker and restore its terminal, including for an empty list.
struct Wake;
impl CliProductSubscription for Wake {
    fn recv(&self) -> Result<ProductSubscriptionItem, ProductClientError> {
        unreachable!("signal-only picker wake")
    }
    fn recv_timeout(&self, _: Duration) -> Result<ProductSubscriptionItem, ProductClientError> {
        unreachable!("signal-only picker wake")
    }
    fn cancel(&self) {}
    fn close(self) {}
}

pub(crate) fn install_signals(
    cancellation: CliStreamCancellation,
    worker_stack_bytes: usize,
) -> Result<NativeStreamSignalGuard, CliError> {
    Ok(NativeStreamSignalGuard::install(
        Arc::new(Wake),
        cancellation,
        worker_stack_bytes,
    )?)
}

// The callback can reject a stale selection and keep the picker open. Callers
// that open resources must release them if terminal restoration later fails.
pub(crate) fn choose<T, W: Write>(
    mut picker: SessionPicker<'_>,
    terminal: &mut dyn InteractiveTerminalPort,
    writer: &mut W,
    cancellation: &CliStreamCancellation,
    view: PickerView,
    mut select: impl FnMut(&SessionSummaryDto, TerminalSizeDto) -> Result<T, CliError>,
) -> Result<Outcome<T>, CliError> {
    let mut size = terminal.size()?;
    terminal.enter_raw_mode()?;
    let result: Result<Outcome<T>, CliError> = (|| {
        write_bytes(writer, ENTER)?;
        loop {
            if cancellation.is_cancelled() {
                return Ok(Outcome::Exit(CommonExitCode::Interrupted));
            }
            let lines = picker.lines(
                size,
                view.title,
                "No sessions. R: refresh · q/Esc: cancel",
                view.label,
            );
            let mut frame = Vec::new();
            queue!(frame, MoveTo(0, 0), Clear(ClearType::All)).map_err(CliOutputError::from)?;
            for (row, line) in lines.iter().take(usize::from(size.rows)).enumerate() {
                // Leave the final column free to prevent wrapping on small terminals.
                queue!(
                    frame,
                    MoveTo(0, row as u16),
                    Print(fit(line, usize::from(size.columns.saturating_sub(1))))
                )
                .map_err(CliOutputError::from)?;
            }
            write_bytes(writer, &frame)?;
            loop {
                if cancellation.is_cancelled() {
                    return Ok(Outcome::Exit(CommonExitCode::Interrupted));
                }
                let Some(event) = terminal.poll_event(IDLE_POLL_INTERVAL)? else {
                    continue;
                };
                match event {
                    InteractiveTerminalEvent::Resize(new_size) => {
                        size = new_size;
                        break;
                    }
                    InteractiveTerminalEvent::Input(input) => {
                        if picker.navigate(&input.item) {
                            break;
                        }
                        if PickerKey::from_input(&input.item) == Some(PickerKey::Cancel) {
                            return Ok(Outcome::Exit(CommonExitCode::Success));
                        }
                        match input.item {
                            TerminalInputDto::Key {
                                key: KeyDto::Control { character: 'c' },
                            } => return Ok(Outcome::Exit(CommonExitCode::Interrupted)),
                            TerminalInputDto::Key { key: KeyDto::Enter } => {
                                if let Some(session) = picker.selected() {
                                    // Resize notifications can arrive after the Enter key.
                                    size = terminal.size()?;
                                    match select(session, size) {
                                        Ok(selected) => return Ok(Outcome::Selected(selected)),
                                        Err(_) => picker.message = view.selection_error.to_owned(),
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
    })();
    let leave = write_bytes(writer, LEAVE);
    let restore = terminal.restore().map_err(CliError::from);
    let outcome = result?;
    leave?;
    restore?;
    Ok(outcome)
}

fn write_bytes(writer: &mut dyn Write, bytes: &[u8]) -> Result<(), CliError> {
    writer
        .write_all(bytes)
        .and_then(|()| writer.flush())
        .map_err(CliOutputError::from)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_paging_preserves_boundary_selection_and_handles_resizing() {
        let mut selected = 4;
        PickerKey::PreviousPage.navigate(&mut selected, 25, 10);
        assert_eq!(selected, 4);
        PickerKey::NextPage.navigate(&mut selected, 25, 10);
        assert_eq!(selected, 10);
        PickerKey::NextPage.navigate(&mut selected, 25, 10);
        assert_eq!(selected, 20);
        selected = 24;
        PickerKey::NextPage.navigate(&mut selected, 25, 10);
        assert_eq!(selected, 24);
        PickerKey::PreviousPage.navigate(&mut selected, 25, 10);
        assert_eq!(selected, 10);
        PickerKey::PreviousPage.navigate(&mut selected, 25, 4);
        assert_eq!(selected, 4);
        PickerKey::NextPage.navigate(&mut selected, 25, 4);
        assert_eq!(selected, 8);
        for key in [
            PickerKey::Up,
            PickerKey::Down,
            PickerKey::PreviousPage,
            PickerKey::NextPage,
        ] {
            let mut empty = 0;
            assert!(key.navigate(&mut empty, 0, 0));
            assert_eq!(empty, 0);
        }
    }
}
