// SPDX-License-Identifier: Apache-2.0

//! In-place presentation of the bounded session watch used by `uc top`.

use std::{io::Write, time::Duration};

use crossterm::{
    cursor::{Hide, MoveTo, Show},
    execute, queue,
    style::Print,
    terminal::{Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen},
};
use undercurrent_protocol::{
    KeyDto, ProductStreamItem, SessionDeltaKindDto, SessionPageDto, TerminalInputDto,
    TerminalSizeDto,
};
use undercurrent_sdk::{ProductClientErrorCategory, ProductSubscriptionItem};

use crate::{
    CliContext, CliError, CliOutputError, CliProductSubscription, CliStreamCancellation,
    CommonExitCode, DetachKey, InteractiveTerminalEvent, InteractiveTerminalPort,
    NativeInteractiveTerminal, NativeStreamSignalGuard, OpenedConnectedStream, OutputFormat,
    Verbosity, session_page_text,
};

pub(crate) fn interactive(stdin: bool, stdout: bool, context: CliContext) -> bool {
    stdin && stdout && context.output == OutputFormat::Text && context.verbosity != Verbosity::Quiet
}

struct Screen<'a, W: Write>(&'a mut W);
impl<W: Write> Screen<'_, W> {
    fn enter(output: &mut W) -> Result<Screen<'_, W>, CliError> {
        let screen = Screen(output);
        execute!(screen.0, EnterAlternateScreen, Hide, Clear(ClearType::All))
            .map_err(CliOutputError::from)?;
        Ok(screen)
    }
}
impl<W: Write> Drop for Screen<'_, W> {
    fn drop(&mut self) {
        let _ = execute!(self.0, Show, LeaveAlternateScreen);
    }
}

pub(crate) fn run<W: Write>(
    opened: OpenedConnectedStream,
    verbosity: Verbosity,
    stack: usize,
    output: &mut W,
) -> Result<CommonExitCode, CliError> {
    let subscription = opened.subscription();
    let cancellation = CliStreamCancellation::default();
    // Always cancel the subscription, including terminal setup and output failures.
    let result = (|| {
        let signal =
            NativeStreamSignalGuard::install(subscription.clone(), cancellation.clone(), stack)?;
        let mut terminal = NativeInteractiveTerminal::new();
        terminal.enter_raw_mode()?;
        let screen = Screen::enter(output)?;
        let result = run_loop(
            subscription.as_ref(),
            &cancellation,
            &mut terminal,
            verbosity,
            screen.0,
        );
        drop(screen);
        let restored = terminal.restore();
        let finished = signal.finish();
        let exit = result?;
        restored?;
        finished?;
        Ok(exit)
    })();
    subscription.cancel();
    result
}

#[derive(Default)]
struct Dashboard {
    page: Option<SessionPageDto>,
    gap: bool,
    scroll: usize,
}
impl Dashboard {
    fn apply(&mut self, item: ProductSubscriptionItem) {
        match item {
            ProductSubscriptionItem::Json {
                item: ProductStreamItem::SessionSnapshot(page),
                ..
            } => {
                self.page = Some(page);
                self.gap = false;
            }
            ProductSubscriptionItem::Json {
                item: ProductStreamItem::SessionDeltaBatch(batch),
                ..
            } => {
                if let Some(page) = &mut self.page {
                    for delta in batch.deltas {
                        page.items.retain(|session| session.id != delta.session_id);
                        if delta.kind != SessionDeltaKindDto::Removed
                            && let Some(summary) = delta.summary
                        {
                            page.items.push(summary);
                        }
                    }
                    // Membership and ordering belong to the server's bounded query window.
                    let mut items = page
                        .items
                        .drain(..)
                        .map(|s| (s.id.clone(), s))
                        .collect::<std::collections::BTreeMap<_, _>>();
                    page.items = batch
                        .order
                        .iter()
                        .filter_map(|id| items.remove(id))
                        .collect();
                    page.snapshot_revision = batch.to_revision;
                    page.total = batch.total;
                }
            }
            ProductSubscriptionItem::Gap(_) => {
                self.page = None;
                self.gap = true;
            }
            _ => {}
        }
    }

    fn lines(&mut self, size: TerminalSizeDto, verbosity: Verbosity) -> Vec<String> {
        let height = usize::from(size.rows);
        let mut lines = vec![match &self.page {
            Some(page) => format!(
                "uc top | {} loaded / {} total",
                page.items.len(),
                page.total.map_or_else(|| "?".into(), |n| n.to_string())
            ),
            None => "uc top".into(),
        }];
        let table = self.page.as_ref().map_or_else(
            || {
                if self.gap {
                    "Updates interrupted; restart uc top to resync".into()
                } else {
                    "Waiting for session snapshot...".into()
                }
            },
            |page| session_page_text(page, verbosity),
        );
        let mut rows = table.lines();
        lines.push(rows.next().unwrap_or_default().to_owned());
        let rows = rows.collect::<Vec<_>>();
        let available = height.saturating_sub(3);
        self.scroll = self.scroll.min(rows.len().saturating_sub(available));
        lines.extend(
            rows.iter()
                .skip(self.scroll)
                .take(available)
                .map(|s| (*s).to_owned()),
        );
        lines.resize(height.saturating_sub(1), String::new());
        lines.push("q quit | Ctrl-C interrupt | Up/Down scroll".into());
        lines.truncate(height);
        lines
            .into_iter()
            .map(|line| fit(&line, usize::from(size.columns.saturating_sub(1))))
            .collect()
    }
}

// Reserve the last column so a full-width line never wraps or scrolls the screen.
fn fit(text: &str, width: usize) -> String {
    let mut used = 0;
    text.chars()
        .take_while(|ch| {
            used += unicode_width::UnicodeWidthChar::width(*ch).unwrap_or(0);
            used <= width
        })
        .collect()
}

fn draw(
    output: &mut impl Write,
    lines: &[String],
    previous: &mut Vec<String>,
) -> Result<(), CliError> {
    let mut frame = Vec::new();
    for (row, line) in lines.iter().enumerate() {
        if previous.get(row) != Some(line) {
            queue!(
                frame,
                MoveTo(0, row as u16),
                Clear(ClearType::CurrentLine),
                Print(line)
            )
            .map_err(CliOutputError::from)?;
        }
    }
    if !frame.is_empty() {
        output.write_all(&frame).map_err(CliOutputError::from)?;
        output.flush().map_err(CliOutputError::from)?;
    }
    *previous = lines.to_vec();
    Ok(())
}

fn run_loop(
    subscription: &dyn CliProductSubscription,
    cancellation: &CliStreamCancellation,
    terminal: &mut impl InteractiveTerminalPort,
    verbosity: Verbosity,
    output: &mut impl Write,
) -> Result<CommonExitCode, CliError> {
    let mut size = terminal.size()?;
    let mut dashboard = Dashboard::default();
    let mut previous = Vec::new();
    loop {
        if cancellation.is_cancelled() {
            return Ok(CommonExitCode::Interrupted);
        }
        draw(output, &dashboard.lines(size, verbosity), &mut previous)?;
        if let Some(event) = terminal.poll_event(Duration::ZERO)? {
            match event {
                InteractiveTerminalEvent::Resize(new_size) => {
                    size = new_size;
                    execute!(output, Clear(ClearType::All)).map_err(CliOutputError::from)?;
                    previous.clear();
                }
                InteractiveTerminalEvent::Input(input) => {
                    match input.detach_key {
                        Some(DetachKey::Literal('q')) => return Ok(CommonExitCode::Success),
                        Some(DetachKey::Control('c')) => return Ok(CommonExitCode::Interrupted),
                        _ => {}
                    }
                    match input.item {
                        TerminalInputDto::Key { key: KeyDto::Up } => {
                            dashboard.scroll = dashboard.scroll.saturating_sub(1)
                        }
                        TerminalInputDto::Key { key: KeyDto::Down } => {
                            dashboard.scroll = dashboard.scroll.saturating_add(1)
                        }
                        _ => {}
                    }
                }
            }
        }
        match subscription.recv_timeout(Duration::from_millis(33)) {
            Ok(item) => dashboard.apply(item),
            Err(error) if error.category() == ProductClientErrorCategory::Timeout => {}
            Err(_) if cancellation.is_cancelled() => return Ok(CommonExitCode::Interrupted),
            Err(error) => return Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CliTerminalError, CommandTimeout, InteractiveInput};
    use std::{collections::VecDeque, sync::Mutex};
    use undercurrent_protocol::*;
    use undercurrent_sdk::{
        ProductClientError, ProductStreamGap, ProductStreamGapReason, ProductStreamRecovery,
    };

    fn summary(id: &str, name: &str) -> SessionSummaryDto {
        SessionSummaryDto {
            id: id.into(),
            name: name.into(),
            lifecycle: LifecycleStateDto::Running,
            foreground: crate::test_foreground(
                ExecutionStateDto::Quiescent,
                InteractionStateDto::None,
            ),
            health: HealthSummaryDto {
                status: HealthStatusDto::NoCheck,
                observed_at_millis: None,
                configured_checks: 0,
            },
            work: WorkSummaryDto {
                revision: 0,
                total_items: 0,
                active_items: 0,
                total_targets: 0,
                active_targets: 0,
                aggregate_status: None,
            },
            profile: None,
            profile_context: Default::default(),
            labels: None,
            started_at_millis: Some(1),
            updated_at_millis: 1,
            restart_count: 0,
            restart_status: RestartStatusDto::Idle,
            revision: 1,
            lifecycle_revision: 1,
        }
    }
    fn snapshot(items: Vec<SessionSummaryDto>) -> ProductSubscriptionItem {
        ProductSubscriptionItem::Json {
            sequence: 1,
            item: ProductStreamItem::SessionSnapshot(SessionPageDto {
                total: Some(items.len() as u64),
                items,
                next_cursor: None,
                snapshot_revision: 1,
            }),
        }
    }
    fn batch() -> ProductSubscriptionItem {
        ProductSubscriptionItem::Json {
            sequence: 2,
            item: ProductStreamItem::SessionDeltaBatch(SessionDeltaBatchDto {
                from_revision: 1,
                to_revision: 2,
                total: Some(2),
                order: vec!["c".into(), "a".into()],
                deltas: vec![
                    SessionDeltaDto {
                        kind: SessionDeltaKindDto::Updated,
                        session_id: "a".into(),
                        session_revision: 2,
                        catalog_revision: 2,
                        summary: Some(summary("a", "short")),
                    },
                    SessionDeltaDto {
                        kind: SessionDeltaKindDto::Removed,
                        session_id: "b".into(),
                        session_revision: 2,
                        catalog_revision: 2,
                        summary: None,
                    },
                    SessionDeltaDto {
                        kind: SessionDeltaKindDto::Added,
                        session_id: "c".into(),
                        session_revision: 1,
                        catalog_revision: 2,
                        summary: Some(summary("c", "new")),
                    },
                ],
            }),
        }
    }
    const SIZE: TerminalSizeDto = TerminalSizeDto {
        columns: 120,
        rows: 12,
    };

    #[test]
    fn updates_replace_rows_remove_sessions_and_follow_server_order() {
        let mut dashboard = Dashboard::default();
        let mut previous = Vec::new();
        let mut bytes = Vec::new();
        dashboard.apply(snapshot(vec![
            summary("a", "long-original-name"),
            summary("b", "removed"),
        ]));
        draw(
            &mut bytes,
            &dashboard.lines(SIZE, Verbosity::Normal),
            &mut previous,
        )
        .unwrap();
        dashboard.apply(batch());
        draw(
            &mut bytes,
            &dashboard.lines(SIZE, Verbosity::Normal),
            &mut previous,
        )
        .unwrap();
        let mut parser = vt100::Parser::new(SIZE.rows, SIZE.columns, 0);
        parser.process(&bytes);
        let screen = parser.screen().contents();
        assert!(screen.contains("short"));
        assert!(!screen.contains("long-original-name"));
        assert!(!screen.contains("removed"));
        assert!(screen.find("new").unwrap() < screen.find("short").unwrap());
        assert!(!bytes.contains(&b'\n'), "redraw must not append lines");
        bytes.clear();
        draw(
            &mut bytes,
            &dashboard.lines(SIZE, Verbosity::Normal),
            &mut previous,
        )
        .unwrap();
        assert!(bytes.is_empty(), "unchanged screen should not be repainted");
    }

    #[test]
    fn gap_hides_stale_rows_until_replacement_snapshot() {
        let mut dashboard = Dashboard::default();
        dashboard.apply(snapshot(vec![summary("a", "stale")]));
        dashboard.apply(ProductSubscriptionItem::Gap(ProductStreamGap {
            stream_id: "top".into(),
            first_missing: 2,
            last_missing: 3,
            reason: ProductStreamGapReason::ReplayEvicted,
            recovery: ProductStreamRecovery::RestartQuery,
        }));
        dashboard.apply(batch());
        let lines = dashboard.lines(SIZE, Verbosity::Normal).join("\n");
        assert!(lines.contains("interrupted"));
        assert!(!lines.contains("stale"));
        dashboard.apply(snapshot(vec![]));
        let lines = dashboard.lines(SIZE, Verbosity::Normal).join("\n");
        assert!(lines.contains("no sessions"));
        assert!(!lines.contains("interrupted"));
    }

    #[test]
    fn viewport_clips_unicode_and_scrolls_without_control_sequences() {
        let mut dashboard = Dashboard::default();
        dashboard.apply(snapshot(
            (0..20)
                .map(|n| summary(&n.to_string(), &format!("worker{n:02}\x1b[2J")))
                .collect(),
        ));
        dashboard.scroll = 19;
        let size = TerminalSizeDto {
            columns: 40,
            rows: 6,
        };
        let lines = dashboard.lines(size, Verbosity::Normal);
        assert_eq!(lines.len(), 6);
        assert!(lines.join("\n").contains("worker19"));
        assert!(!lines.join("\n").contains('\x1b'));
        assert!(
            lines
                .iter()
                .all(|s| unicode_width::UnicodeWidthStr::width(s.as_str()) < 40)
        );
        assert_eq!(fit("a界b", 3), "a界");
        for rows in 0..3 {
            assert_eq!(
                dashboard
                    .lines(TerminalSizeDto { columns: 1, rows }, Verbosity::Normal)
                    .len(),
                usize::from(rows)
            );
        }
    }

    #[test]
    fn dashboard_requires_interactive_nonquiet_text() {
        let timeout = CommandTimeout::new(Duration::from_secs(1), Duration::from_secs(1)).unwrap();
        for format in [OutputFormat::Text, OutputFormat::Jsonl] {
            for verbosity in [Verbosity::Normal, Verbosity::Quiet] {
                for (stdin, stdout) in [(true, true), (false, true), (true, false), (false, false)]
                {
                    assert_eq!(
                        interactive(stdin, stdout, CliContext::new(format, verbosity, timeout)),
                        stdin
                            && stdout
                            && format == OutputFormat::Text
                            && verbosity != Verbosity::Quiet
                    );
                }
            }
        }
    }

    struct Subscription(Mutex<VecDeque<ProductSubscriptionItem>>);
    impl CliProductSubscription for Subscription {
        fn recv(&self) -> Result<ProductSubscriptionItem, ProductClientError> {
            self.0
                .lock()
                .unwrap()
                .pop_front()
                .ok_or(ProductClientError::Subscription(
                    undercurrent_sdk::ResilientSubscriptionError::Timeout,
                ))
        }
        fn recv_timeout(&self, _: Duration) -> Result<ProductSubscriptionItem, ProductClientError> {
            self.recv()
        }
        fn cancel(&self) {}
        fn close(self) {}
    }
    struct Terminal(VecDeque<Option<InteractiveTerminalEvent>>);
    impl InteractiveTerminalPort for Terminal {
        fn size(&mut self) -> Result<TerminalSizeDto, CliTerminalError> {
            Ok(SIZE)
        }
        fn enter_raw_mode(&mut self) -> Result<(), CliTerminalError> {
            Ok(())
        }
        fn restore(&mut self) -> Result<(), CliTerminalError> {
            Ok(())
        }
        fn poll_event(
            &mut self,
            _: Duration,
        ) -> Result<Option<InteractiveTerminalEvent>, CliTerminalError> {
            Ok(self.0.pop_front().expect("loop must exit"))
        }
    }
    fn key(key: DetachKey) -> Option<InteractiveTerminalEvent> {
        Some(InteractiveTerminalEvent::Input(InteractiveInput {
            item: TerminalInputDto::Raw { bytes: vec![] },
            detach_key: Some(key),
        }))
    }

    #[test]
    fn event_loop_handles_live_changes_resize_and_quit() {
        let subscription = Subscription(Mutex::new(VecDeque::from([
            snapshot(vec![summary("a", "old"), summary("b", "gone")]),
            batch(),
        ])));
        let mut terminal = Terminal(VecDeque::from([
            None,
            Some(InteractiveTerminalEvent::Resize(TerminalSizeDto {
                columns: 80,
                rows: 8,
            })),
            key(DetachKey::Literal('q')),
        ]));
        let mut output = Vec::new();
        let exit = run_loop(
            &subscription,
            &CliStreamCancellation::default(),
            &mut terminal,
            Verbosity::Normal,
            &mut output,
        )
        .unwrap();
        assert_eq!(exit, CommonExitCode::Success);
        let mut parser = vt100::Parser::new(8, 80, 0);
        parser.process(&output);
        let screen = parser.screen().contents();
        assert!(screen.contains("short"));
        assert!(!screen.contains("gone"));
        assert!(screen.contains("q quit"));
    }

    #[test]
    fn ctrl_c_and_external_cancellation_exit_as_interrupted() {
        let subscription = Subscription(Mutex::new(VecDeque::new()));
        for cancelled in [false, true] {
            let cancellation = CliStreamCancellation::default();
            if cancelled {
                cancellation.cancel(&subscription);
            }
            let mut terminal = Terminal(VecDeque::from([key(DetachKey::Control('c'))]));
            assert_eq!(
                run_loop(
                    &subscription,
                    &cancellation,
                    &mut terminal,
                    Verbosity::Normal,
                    &mut Vec::new()
                )
                .unwrap(),
                CommonExitCode::Interrupted
            );
        }
    }

    #[test]
    fn alternate_screen_and_cursor_are_restored_on_error() {
        let mut bytes = Vec::new();
        fn fail(output: &mut Vec<u8>) -> Result<(), ()> {
            let _screen = Screen::enter(output).unwrap();
            Err(())
        }
        let result = fail(&mut bytes);
        assert!(result.is_err());
        let output = String::from_utf8(bytes).unwrap();
        assert!(output.starts_with("\x1b[?1049h"));
        assert!(output.ends_with("\x1b[?25h\x1b[?1049l"));
    }
}
