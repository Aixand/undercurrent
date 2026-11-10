// SPDX-License-Identifier: Apache-2.0

//! Group event loop. Only the selected session owns an output subscription.

use super::{
    GroupAction, GroupArguments, error, io_error,
    renderer::{Renderer, fit},
    service::{Service, mutation},
    store::{Group, Pending},
};
use crate::detach::ShortcutAction;
use crate::session_picker::PickerKey;
use crate::{
    CliDiagnosticSink, CliError, CliOutputError, CliProductStreamPort, CliProductSubscription,
    CliStreamCancellation, CommonExitCode, DetachMatcher, InteractiveTerminalEvent,
    InteractiveTerminalPort, NativeInteractiveTerminal, NativeStreamSignalGuard, expect_list,
    prepare_terminal_stream,
};
use crossterm::{
    event::{DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen},
};
use std::collections::BTreeMap;
use std::io::Write;
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};
use undercurrent_protocol::*;
use undercurrent_sdk::{ProductClientError, ProductClientErrorCategory, ProductSubscriptionItem};

const POLL: Duration = Duration::from_millis(8);
const FRAME: Duration = Duration::from_millis(33);

type CreationResult = (String, Result<ProductSuccess, ProductClientError>);

struct Subscription(Arc<dyn CliProductSubscription>);
impl Drop for Subscription {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
struct Watch {
    ids: Vec<String>,
    stream: Subscription,
}
struct Active {
    id: String,
    stream: Subscription,
    offset: u64,
    renderer: Renderer,
}

// The signal worker cancels the local loop even when no sessions exist.
struct Wake;
impl CliProductSubscription for Wake {
    fn recv(&self) -> Result<ProductSubscriptionItem, ProductClientError> {
        unreachable!("signal-only wake")
    }
    fn recv_timeout(
        &self,
        _timeout: Duration,
    ) -> Result<ProductSubscriptionItem, ProductClientError> {
        unreachable!("signal-only wake")
    }
    fn cancel(&self) {}
    fn close(self) {}
}

struct TerminalGuard;
impl TerminalGuard {
    fn enter(output: &mut impl Write) -> Result<Self, CliError> {
        crossterm::terminal::enable_raw_mode()
            .map_err(|e| io_error("could not enter group terminal", e))?;
        let guard = Self;
        execute!(
            output,
            EnterAlternateScreen,
            EnableBracketedPaste,
            EnableMouseCapture
        )
        .map_err(CliOutputError::from)?;
        output
            .write_all(b"\x1b[?7l\x1b[0m\x1b[2J\x1b[H")
            .map_err(CliOutputError::from)?;
        Ok(guard)
    }
}
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let mut output = std::io::stdout();
        let _ = output.write_all(b"\x1b[0m\x1b[?25h\x1b[?7h\x1b[r");
        let _ = execute!(
            output,
            DisableMouseCapture,
            DisableBracketedPaste,
            LeaveAlternateScreen
        );
        let _ = crossterm::terminal::disable_raw_mode();
    }
}

pub(super) fn run<P: CliProductStreamPort, W: Write>(
    service: &Service<'_, P>,
    args: &GroupArguments,
    stack: usize,
    output: &mut W,
    _diagnostics: &dyn CliDiagnosticSink,
) -> Result<CommonExitCode, CliError>
where
    P::Subscription: 'static,
{
    let mut keys = crate::shortcuts::matcher(&service.shortcuts)?.for_workspace();
    let mut terminal = NativeInteractiveTerminal::new();
    let size = viewport(terminal.size()?, service);
    let cancellation = CliStreamCancellation::default();
    let signal = NativeStreamSignalGuard::install(Arc::new(Wake), cancellation.clone(), stack)?;
    let _guard = TerminalGuard::enter(output)?;
    let group = match &args.action {
        Some(GroupAction::New {
            group,
            cwd,
            shell,
            persist,
            no_persist,
        }) => Some((
            service.create_group(
                group,
                cwd.as_deref(),
                shell.as_deref(),
                *persist && !*no_persist,
            )?,
            true,
        )),
        Some(GroupAction::Attach { group }) => Some((service.store.group(group)?, false)),
        None => pick_group(
            service,
            &mut terminal,
            output,
            &cancellation,
            &mut keys,
            size,
        )?,
        _ => return Err(error("invalid group route")),
    };
    let Some((group, create)) = group else {
        return Ok(CommonExitCode::Success);
    };
    let _lock = service.store.group_lock(&group.id)?;
    service.reconcile(&group.name)?;
    let restore_notices = service.restore(&group.name, None, size)?;
    let group = service.store.group(&group.name)?;
    std::thread::scope(|scope| {
        let (jobs_tx, jobs_rx) = mpsc::sync_channel::<Pending>(1);
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("uc-group-create".into())
            .stack_size(stack)
            .spawn_scoped(scope, move || {
                while let Ok(pending) = jobs_rx.recv() {
                    let result = service.client.request(
                        ProductRequest::StartSession(pending.request),
                        service.timeout,
                    );
                    if result_tx.send((pending.token, result)).is_err() {
                        break;
                    }
                }
            })
            .map_err(|e| io_error("could not start group creation worker", e))?;
        let mut workspace = Workspace {
            service,
            group,
            active: None,
            watches: vec![],
            summaries: BTreeMap::new(),
            size,
            keys,
            notice: restore_notices.join("; "),
            jobs: jobs_tx,
            results: result_rx,
            creating: false,
            mode: Mode::Terminal,
            clipboard: None,
            dirty: true,
        };
        workspace.refresh_watches()?;
        if let Some(id) = workspace.group.selected_session() {
            workspace
                .switch(&id)
                .unwrap_or_else(|e| workspace.notice = e.to_string())
        }
        if !restore_notices.is_empty() {
            workspace.notice = restore_notices.join("; ");
        }
        if create {
            workspace
                .create()
                .unwrap_or_else(|e| workspace.notice = e.to_string())
        }
        workspace.event_loop(&mut terminal, output, &cancellation)
    })
    .and_then(|code| {
        signal.finish()?;
        Ok(code)
    })
}

fn viewport<P>(size: TerminalSizeDto, service: &Service<'_, P>) -> TerminalSizeDto {
    TerminalSizeDto {
        columns: size.columns.max(1).min(service.limits.terminal_columns()),
        rows: size
            .rows
            .saturating_sub(1)
            .max(1)
            .min(service.limits.terminal_rows()),
    }
}

enum Mode {
    Terminal,
    Help,
    Picker(usize),
}
struct Workspace<'a, 'p, P> {
    service: &'a Service<'p, P>,
    group: Group,
    active: Option<Active>,
    watches: Vec<Watch>,
    summaries: BTreeMap<String, SessionSummaryDto>,
    size: TerminalSizeDto,
    keys: DetachMatcher,
    notice: String,
    jobs: mpsc::SyncSender<Pending>,
    results: mpsc::Receiver<CreationResult>,
    creating: bool,
    mode: Mode,
    clipboard: Option<String>,
    dirty: bool,
}

impl<P: CliProductStreamPort> Workspace<'_, '_, P>
where
    P::Subscription: 'static,
{
    fn create(&mut self) -> Result<(), CliError> {
        let pending = self.service.prepare_create(&self.group.name, self.size)?;
        self.group = self.service.store.group(&self.group.name)?;
        self.jobs
            .try_send(pending)
            .map_err(|_| error("creation worker busy; creation intent retained for recovery"))?;
        self.creating = true;
        self.notice = "Creating shell…".to_owned();
        self.dirty = true;
        Ok(())
    }

    fn completed_create(
        &mut self,
        token: String,
        result: Result<ProductSuccess, ProductClientError>,
    ) -> Result<(), CliError> {
        self.creating = false;
        match result {
            Ok(ProductSuccess::StartSession(result)) => {
                self.service
                    .finish_create(&self.group.name, &token, &result.session)?;
                self.group = self.service.store.group(&self.group.name)?;
                self.refresh_watches()?;
                self.switch(&result.session.id)?;
                self.notice.clear();
            }
            Ok(_) => {
                return Err(error(
                    "unexpected creation response; pending intent retained",
                ));
            }
            Err(e) => {
                // Only explicit owner evidence establishes that no start happened.
                if matches!(&e, ProductClientError::Server(error) if error.effect_certainty == Some(EffectCertaintyDto::KnownAbsent))
                {
                    self.service.reject_create(&self.group.name, &token)?;
                }
                self.group = self.service.store.group(&self.group.name)?;
                self.notice = if self.group.pending.is_some() {
                    "Creation outcome unknown; reopen to reconcile. No retry was sent.".into()
                } else {
                    "Shell creation rejected; check shell and directory settings.".into()
                };
            }
        }
        self.dirty = true;
        Ok(())
    }

    fn switch(&mut self, id: &str) -> Result<(), CliError> {
        if self.group.deleting {
            return Err(error("Deletion pending; resume uc group delete --stop"));
        }
        if self.active.as_ref().is_some_and(|a| a.id == id) {
            self.mode = Mode::Terminal;
            if let Some(a) = &mut self.active {
                a.renderer.invalidate();
            }
            return Ok(());
        }
        let summary = expect_list(self.service.client.request(
            ProductRequest::ListSessions(crate::exact_id_lookup(id)),
            self.service.timeout,
        )?)?
        .items
        .into_iter()
        .next()
        .ok_or_else(|| error("session unavailable; current selection retained"))?;
        if matches!(
            summary.lifecycle,
            LifecycleStateDto::Running | LifecycleStateDto::Starting | LifecycleStateDto::Stopping
        ) {
            self.resize_session(id)?;
        }
        let opened = self.service.client.open_product_subscription(
            prepare_terminal_stream(id, TerminalInitialModeDto::Screen),
            self.service.timeout,
        )?;
        let next = Active {
            id: id.to_owned(),
            stream: Subscription(Arc::new(opened.subscription)),
            offset: 0,
            renderer: Renderer::with_scrollback(self.size, self.service.scrollback),
        };
        self.service.select(&self.group.name, id)?;
        self.group.selected = self
            .group
            .members
            .iter()
            .find(|m| m.id == id)
            .map(|m| m.key().to_owned());
        self.summaries.insert(id.to_owned(), summary);
        self.active = Some(next);
        self.mode = Mode::Terminal;
        self.dirty = true;
        self.notice.clear();
        Ok(())
    }

    fn resize_session(&self, id: &str) -> Result<(), CliError> {
        let result = self.service.client.request(
            ProductRequest::ResizeTerminal(ResizeTerminalRequest {
                mutation: mutation(ProductOperation::ResizeTerminal),
                session_id: id.to_owned(),
                size: self.size,
            }),
            self.service.timeout,
        )?;
        if matches!(result, ProductSuccess::ResizeTerminal(_)) {
            Ok(())
        } else {
            Err(error("unexpected resize response"))
        }
    }

    fn refresh_watches(&mut self) -> Result<(), CliError> {
        let mut watches = vec![];
        for chunk in self
            .group
            .members
            .chunks(self.service.limits.catalog_page_size().min(100) as usize)
        {
            let ids: Vec<_> = chunk.iter().map(|m| m.id.clone()).collect();
            let query = ListSessionsRequest {
                filter: SessionFilterDto {
                    ids: ids.clone(),
                    ..Default::default()
                },
                sort: Default::default(),
                projection: Default::default(),
                page_size: ids.len() as u32,
                cursor: None,
                include_total: false,
            };
            let opened = self.service.client.open_product_subscription(
                ProductStreamSpec::SessionWatch(SessionWatchSpec { query }),
                self.service.timeout,
            )?;
            watches.push(Watch {
                ids,
                stream: Subscription(Arc::new(opened.subscription)),
            });
        }
        self.watches = watches;
        self.summaries
            .retain(|id, _| self.group.members.iter().any(|m| &m.id == id));
        Ok(())
    }

    fn send(&self, items: Vec<TerminalInputDto>) -> Result<(), CliError> {
        if items.is_empty() {
            return Ok(());
        }
        let Some(active) = &self.active else {
            return Ok(());
        };
        let result = self.service.client.request(
            ProductRequest::SendTerminalInput(SendTerminalInputRequest {
                mutation: mutation(ProductOperation::SendTerminalInput),
                session_id: active.id.clone(),
                items,
                expected_revision: None,
                delivery_id: None,
                observation_policy: InputObservationPolicyDto::Preserve,
            }),
            self.service.timeout,
        )?;
        if matches!(result, ProductSuccess::SendTerminalInput(_)) {
            Ok(())
        } else {
            Err(error("unexpected terminal input response"))
        }
    }

    fn pump(&mut self) -> Result<(), CliError> {
        for watch in &self.watches {
            for _ in 0..8 {
                let Some(item) = receive(&watch.stream)? else {
                    break;
                };
                match item {
                    ProductSubscriptionItem::Json {
                        item: ProductStreamItem::SessionSnapshot(page),
                        ..
                    } => {
                        self.summaries.retain(|id, _| !watch.ids.contains(id));
                        for s in page.items {
                            self.summaries.insert(s.id.clone(), s);
                        }
                    }
                    ProductSubscriptionItem::Json {
                        item: ProductStreamItem::SessionDeltaBatch(batch),
                        ..
                    } => {
                        for delta in batch.deltas {
                            match delta.summary {
                                Some(s) => {
                                    self.summaries.insert(s.id.clone(), s);
                                }
                                None => {
                                    self.summaries.remove(&delta.session_id);
                                }
                            }
                        }
                    }
                    ProductSubscriptionItem::Gap(_) => {
                        return Err(error("session-state gap; reconnecting group watch"));
                    }
                    _ => {}
                }
                self.dirty = true;
            }
        }
        let mut replies = vec![];
        if let Some(active) = &mut self.active {
            let output_turn = Instant::now();
            for _ in 0..32 {
                if output_turn.elapsed() >= Duration::from_millis(8) {
                    break;
                }
                let Some(item) = receive(&active.stream)? else {
                    break;
                };
                match item {
                    ProductSubscriptionItem::Terminal {
                        item: TerminalStreamItem::Screen(screen),
                        ..
                    } => {
                        active
                            .renderer
                            .reset(screen.contents(), screen.alternate_screen());
                        active.offset = screen.output_offset();
                    }
                    ProductSubscriptionItem::Terminal {
                        item: TerminalStreamItem::Output(chunk),
                        ..
                    } => {
                        let end = chunk
                            .end_offset()
                            .ok_or_else(|| error("terminal output offset overflow"))?;
                        if end <= active.offset {
                            continue;
                        }
                        if chunk.start_offset() > active.offset {
                            resync(self.service, active)?;
                            self.notice="Output gap — restored current viewport; older history may be unavailable.".into();
                        } else {
                            let skip = (active.offset - chunk.start_offset()) as usize;
                            let response = active.renderer.process(&chunk.bytes()[skip..]);
                            if !response.is_empty() {
                                replies.push(TerminalInputDto::Raw { bytes: response });
                            }
                            active.offset = end;
                        }
                    }
                    ProductSubscriptionItem::Gap(_) => {
                        resync(self.service, active)?;
                        self.notice="Output gap — restored current viewport; older history may be unavailable.".into();
                    }
                    _ => {}
                }
                self.dirty = true;
            }
        }
        for chunk in replies.chunks(self.service.limits.terminal_input_items()) {
            self.send(chunk.to_vec())?;
        }
        Ok(())
    }

    fn navigate(&mut self, forward: bool) -> Result<(), CliError> {
        let len = self.group.members.len();
        if len == 0 {
            return Ok(());
        }
        let index = self
            .group
            .members
            .iter()
            .position(|m| Some(m.key()) == self.group.selected.as_deref())
            .unwrap_or(0);
        let next = if forward {
            (index + 1) % len
        } else {
            (index + len - 1) % len
        };
        let id = self.group.members[next].id.clone();
        self.switch(&id)
    }

    fn event_loop(
        &mut self,
        terminal: &mut NativeInteractiveTerminal,
        output: &mut impl Write,
        cancellation: &CliStreamCancellation,
    ) -> Result<CommonExitCode, CliError> {
        let mut last_frame = Instant::now() - FRAME;
        let mut last_metadata = Instant::now();
        let mut metadata_stamp = self.service.store.stamp()?;
        let mut last_reconnect = Instant::now();
        let mut reconnect = false;
        let mut epoch = self.service.client.server_epoch();
        let mut last_epoch_check = Instant::now();
        let initial_size = terminal.size()?;
        let mut too_small = initial_size.rows < 3 || initial_size.columns < 10;
        loop {
            if cancellation.is_cancelled() {
                return Ok(CommonExitCode::Interrupted);
            }
            if let Ok((token, result)) = self.results.try_recv()
                && let Err(e) = self.completed_create(token, result)
            {
                self.notice = e.to_string();
                self.dirty = true
            }
            if self.group.persistent && last_epoch_check.elapsed() >= Duration::from_millis(500) {
                last_epoch_check = Instant::now();
                if self.service.client.server_epoch().as_ref() != epoch.as_ref() {
                    reconnect = true;
                    last_reconnect = Instant::now() - Duration::from_secs(1);
                }
            }
            if !reconnect {
                if let Err(e) = self.pump() {
                    self.notice = format!("Connection/output unavailable: {e}");
                    reconnect = true;
                    self.dirty = true;
                    last_reconnect = Instant::now();
                }
            } else if last_reconnect.elapsed() >= Duration::from_secs(1) {
                last_reconnect = Instant::now();
                let mut recovery_notice = String::new();
                if self.group.persistent {
                    match self.service.client.server_epoch() {
                        Some(current) if epoch.as_ref() != Some(&current) => {
                            let result = self.service.reconcile(&self.group.name).and_then(|()| {
                                self.service.restore(&self.group.name, None, self.size)
                            });
                            match result {
                                Ok(notices) => {
                                    self.group = self.service.store.group(&self.group.name)?;
                                    if self.active.as_ref().is_some_and(|a| {
                                        !self.group.members.iter().any(|m| m.id == a.id)
                                    }) {
                                        self.active = None;
                                    }
                                    recovery_notice = notices.join("; ");
                                    epoch = Some(current);
                                }
                                Err(e) => {
                                    self.notice = e.to_string();
                                    self.dirty = true;
                                    continue;
                                }
                            }
                        }
                        Some(_) => {}
                        None => {
                            // The SDK may need an operation to establish its next connection.
                            // Do not wait on a cached agreement before driving reconnection.
                            if let Err(e) = self.refresh_watches() {
                                self.notice = e.to_string();
                                self.dirty = true;
                            }
                            continue;
                        }
                    }
                }
                if self.refresh_watches().is_ok() {
                    if let Some(active) = &mut self.active {
                        if let Ok(opened) = self.service.client.open_product_subscription(
                            prepare_terminal_stream(&active.id, TerminalInitialModeDto::Screen),
                            self.service.timeout,
                        ) {
                            active.stream = Subscription(Arc::new(opened.subscription));
                            reconnect = false;
                            self.notice =
                                "Reconnected; viewport restored, history may be partial.".into();
                        }
                    } else {
                        reconnect = false;
                        if let Some(id) = self.group.selected_session()
                            && let Err(e) = self.switch(&id)
                        {
                            self.notice = e.to_string();
                        }
                    }
                    if !recovery_notice.is_empty() {
                        self.notice = recovery_notice;
                    }
                    self.dirty = true;
                }
            }
            if last_metadata.elapsed() >= Duration::from_millis(500) {
                last_metadata = Instant::now();
                let next_stamp = self.service.store.stamp()?;
                if next_stamp != metadata_stamp {
                    metadata_stamp = next_stamp;
                    match self.service.store.group(&self.group.name) {
                        Ok(group) => {
                            let changed = group.members.iter().map(|m| &m.id).ne(self
                                .group
                                .members
                                .iter()
                                .map(|m| &m.id));
                            self.group = group;
                            self.dirty = true;
                            if changed {
                                if self.active.as_ref().is_some_and(|a| {
                                    !self.group.members.iter().any(|m| m.id == a.id)
                                }) {
                                    self.active = None;
                                }
                                if let Err(e) = self.refresh_watches() {
                                    self.notice = e.to_string();
                                }
                                if self.active.is_none()
                                    && let Some(id) = self.group.selected_session()
                                    && let Err(e) = self.switch(&id)
                                {
                                    self.notice = e.to_string();
                                }
                                self.dirty = true;
                            }
                        }
                        Err(e) => {
                            self.notice = e.to_string();
                            self.dirty = true;
                        }
                    }
                }
            }
            if self.dirty && last_frame.elapsed() >= FRAME {
                self.draw(output, too_small)?;
                self.dirty = false;
                last_frame = Instant::now();
            }
            let Some(event) = terminal.poll_event(POLL)? else {
                continue;
            };
            match event {
                InteractiveTerminalEvent::Resize(size) => {
                    too_small = size.rows < 3 || size.columns < 10;
                    if !too_small {
                        self.size = viewport(size, self.service);
                        if let Some(active) = &mut self.active {
                            active.renderer.resize(self.size);
                        }
                        if let Some(id) = self.active.as_ref().map(|a| a.id.clone())
                            && let Err(e) = self.resize_session(&id)
                        {
                            self.notice = e.to_string();
                        }
                    }
                    self.dirty = true;
                }
                InteractiveTerminalEvent::Input(input) => {
                    if !matches!(self.mode, Mode::Terminal) {
                        self.overlay_input(input.item);
                        self.dirty = true;
                        continue;
                    }
                    let matched = self.keys.feed(input);
                    self.dirty = true;
                    if matched.detached {
                        return Ok(CommonExitCode::Success);
                    }
                    if !too_small {
                        let items = matched
                            .passthrough
                            .into_iter()
                            .filter_map(|i| {
                                self.active.as_mut().and_then(|a| {
                                    if a.renderer.history_input(&i) {
                                        None
                                    } else {
                                        a.renderer.input(i)
                                    }
                                })
                            })
                            .collect();
                        if let Err(e) = self.send(items) {
                            self.notice = e.to_string();
                        }
                    }
                    if let Some(text) = self.active.as_mut().and_then(|a| a.renderer.take_copy()) {
                        super::renderer::clipboard(output, &text)?;
                        self.clipboard = Some(text);
                    }
                    if let Some(action) = self.keys.take_action() {
                        let result = match action {
                            ShortcutAction::Copy => {
                                if let Some(active) = &mut self.active {
                                    active.renderer.enter_copy();
                                }
                                Ok(())
                            }
                            ShortcutAction::Paste => {
                                let item = self.clipboard.as_ref().and_then(|text| {
                                    self.active.as_mut().and_then(|active| {
                                        let item = TerminalInputDto::Paste { text: text.clone() };
                                        if active.renderer.history_input(&item) {
                                            None
                                        } else {
                                            active.renderer.input(item)
                                        }
                                    })
                                });
                                self.send(item.into_iter().collect())
                            }
                            ShortcutAction::SplitColumns
                            | ShortcutAction::SplitRows
                            | ShortcutAction::Focus
                            | ShortcutAction::Zoom
                            | ShortcutAction::ClosePane => Ok(()),
                            ShortcutAction::Create => self.create(),
                            ShortcutAction::Next => self.navigate(true),
                            ShortcutAction::Previous => self.navigate(false),
                            ShortcutAction::Sessions => {
                                self.mode = Mode::Picker(0);
                                Ok(())
                            }
                            ShortcutAction::Help => {
                                self.mode = Mode::Help;
                                Ok(())
                            }
                        };
                        if let Err(e) = result {
                            self.notice = e.to_string();
                        }
                        if let Some(a) = &mut self.active {
                            a.renderer.invalidate();
                        }
                    }
                }
            }
        }
    }

    fn overlay_input(&mut self, input: TerminalInputDto) {
        let key = PickerKey::from_input(&input);
        if key == Some(PickerKey::Cancel) {
            self.mode = Mode::Terminal;
            if let Some(a) = &mut self.active {
                a.renderer.invalidate();
            }
            return;
        }
        if let Mode::Picker(index) = &mut self.mode
            && let Some(key) = key
            && key.navigate(
                index,
                self.group.members.len(),
                usize::from(self.size.rows.saturating_sub(3)),
            )
        {
            return;
        }
        if let TerminalInputDto::Key { key: KeyDto::Enter } = input
            && let Mode::Picker(index) = self.mode
            && let Some(member) = self.group.members.get(index)
        {
            let id = member.id.clone();
            if let Err(e) = self.switch(&id) {
                self.notice = e.to_string();
            }
        }
    }

    fn bar(&self) -> String {
        let head = format!("uc | {} | ", self.group.name);
        if self.keys.prefix_pending() {
            return format!(
                "{head}C new · N next · P previous · S sessions · D detach · ? help · Esc cancel"
            );
        }
        if !self.notice.is_empty() {
            return format!("{head}{}", self.notice);
        }
        if let Some(p) = &self.group.pending {
            return format!("{head}Pending {} — reopen to reconcile", p.request.name);
        }
        let selected = self
            .group
            .members
            .iter()
            .position(|m| Some(m.key()) == self.group.selected.as_deref())
            .unwrap_or(0);
        let mut entries = Vec::new();
        for (index, m) in self.group.members.iter().enumerate() {
            let name = m.name.as_str();
            let name = name
                .strip_prefix(&format!("{}-", self.group.name))
                .unwrap_or(name);
            let state = self
                .summaries
                .get(&m.id)
                .map_or("?", |s| state_label(&s.lifecycle));
            let state = if m.restore.as_ref().is_some_and(|r| !r.open) {
                "closed"
            } else if state == "running"
                && m.restore.is_some()
                && self
                    .summaries
                    .get(&m.id)
                    .is_none_or(|s| s.profile_context.current_directory.is_none())
            {
                " (directory unobserved)"
            } else {
                state
            };
            let label = format!(
                "{}:{}{}",
                index + 1,
                name,
                if state == "running" { "" } else { state }
            );
            entries.push(if index == selected {
                format!("[{label}]")
            } else {
                label
            });
        }
        let mut start = 0;
        while start < selected
            && unicode_width::UnicodeWidthStr::width(
                format!("{head}{}", entries[start..=selected].join(" ")).as_str(),
            ) > self.size.columns.saturating_sub(5) as usize
        {
            start += 1
        }
        format!(
            "{head}{}{} | {} → ? help",
            if start > 0 { "… " } else { "" },
            entries[start..].join(" "),
            self.keys.prefix_label()
        )
    }

    fn draw(&mut self, output: &mut impl Write, too_small: bool) -> Result<(), CliError> {
        if too_small {
            output
                .write_all(b"\x1b[0m\x1b[2J\x1b[HResize terminal to continue")
                .and_then(|()| output.flush())
                .map_err(CliOutputError::from)?;
            return Ok(());
        }
        let bar = self.bar();
        match &self.mode {
            Mode::Terminal => {
                if let Some(active) = &mut self.active {
                    active.renderer.draw(output, &bar)
                } else {
                    let mut blank = Renderer::with_scrollback(
                        self.size,
                        undercurrent_config::ScrollbackConfig { memory_lines: 0 },
                    );
                    blank.process(
                        b"No active session. Use the prefix, then C to create or S to select.",
                    );
                    blank.draw(output, &bar)
                }
            }
            Mode::Help => {
                let mut help = Renderer::with_scrollback(
                    self.size,
                    undercurrent_config::ScrollbackConfig { memory_lines: 0 },
                );
                let text = format!(
                    "Undercurrent group shortcuts\r\n\r\n{} then:\r\nC  create shell\r\nN / P  next / previous\r\nS  session picker\r\n[  copy / visual mode   ]  paste copied text\r\nD  detach group (processes keep running)\r\n?  help\r\nPrefix twice  send literal prefix\r\nShift+PageUp/PageDown  UC history\r\nShift+End / Escape in history  live output\r\nEscape  close help / cancel prefix",
                    self.keys.prefix_label()
                );
                help.process(text.as_bytes());
                help.draw(output, &bar)
            }
            Mode::Picker(selected) => {
                let mut picker = Renderer::with_scrollback(
                    self.size,
                    undercurrent_config::ScrollbackConfig { memory_lines: 0 },
                );
                let mut text = format!(
                    "{}\r\n\r\n",
                    fit(
                        "Group sessions — Up/k Down/j · h/l: page · Enter: open · q/Esc: close",
                        usize::from(self.size.columns),
                    )
                );
                let rows = self.size.rows.saturating_sub(3).max(1) as usize;
                let start = selected / rows * rows;
                for (index, m) in self.group.members.iter().enumerate().skip(start).take(rows) {
                    let state = self.summaries.get(&m.id).map_or(
                        if m.restore.as_ref().is_some_and(|r| !r.open) {
                            "closed"
                        } else {
                            "unavailable"
                        },
                        |s| state_label(&s.lifecycle),
                    );
                    text.push_str(&format!(
                        "{} {} {}\r\n",
                        if index == *selected { ">" } else { " " },
                        fit(&m.name, self.size.columns.saturating_sub(20) as usize),
                        state
                    ));
                }
                picker.process(text.as_bytes());
                picker.draw(output, &bar)
            }
        }
    }
}

fn state_label(state: &LifecycleStateDto) -> &'static str {
    match state {
        LifecycleStateDto::Running => "running",
        LifecycleStateDto::Starting => "(starting)",
        LifecycleStateDto::Stopping => "(stopping)",
        LifecycleStateDto::Stopped => "(stopped)",
        LifecycleStateDto::Exited { .. } => "(exited)",
        LifecycleStateDto::Failed => "(failed)",
    }
}
fn receive(stream: &Subscription) -> Result<Option<ProductSubscriptionItem>, CliError> {
    match stream.0.recv_timeout(Duration::from_millis(1)) {
        Ok(item) => Ok(Some(item)),
        Err(e) if e.category() == ProductClientErrorCategory::Timeout => Ok(None),
        Err(e) => Err(e.into()),
    }
}
fn resync<P: CliProductStreamPort>(
    service: &Service<'_, P>,
    active: &mut Active,
) -> Result<(), CliError> {
    let result = service.client.request(
        ProductRequest::ReadTerminalScreen(ReadTerminalScreenRequest {
            session_id: active.id.clone(),
        }),
        service.timeout,
    )?;
    if let ProductSuccess::ReadTerminalScreen(screen) = result {
        active
            .renderer
            .reset(&screen.contents, screen.alternate_screen);
        active.offset = screen.output_offset;
        Ok(())
    } else {
        Err(error("unexpected screen recovery response"))
    }
}

fn pick_group<P: CliProductStreamPort>(
    service: &Service<'_, P>,
    terminal: &mut NativeInteractiveTerminal,
    output: &mut impl Write,
    cancel: &CliStreamCancellation,
    keys: &mut DetachMatcher,
    mut size: TerminalSizeDto,
) -> Result<Option<(Group, bool)>, CliError> {
    let mut selected = 0usize;
    let mut name: Option<String> = None;
    let mut persistent = true;
    let mut notice = String::new();
    loop {
        let groups = service.store.groups()?;
        selected = selected.min(groups.len().saturating_sub(1));
        let mut view = Renderer::with_scrollback(
            size,
            undercurrent_config::ScrollbackConfig { memory_lines: 0 },
        );
        let mut text = format!(
            "Undercurrent groups\r\n{}\r\n\r\n",
            fit(
                "Up/k Down/j · h/l: page · Enter: open · c/t: create · q/Esc: exit",
                usize::from(size.columns),
            )
        );
        let rows = size.rows.saturating_sub(5).max(1) as usize;
        let start = selected / rows * rows;
        for (index, g) in groups.iter().enumerate().skip(start).take(rows) {
            text.push_str(&format!(
                "{} {} ({} sessions)\r\n",
                if index == selected { ">" } else { " " },
                g.name,
                g.members.len()
            ));
        }
        if groups.is_empty() {
            text.push_str("No groups. Press C to create.\r\n")
        }
        if let Some(name) = &name {
            text.push_str(&format!(
                "\r\nNew {} group: {name}",
                if persistent { "saved" } else { "temporary" }
            ));
        }
        text.push_str(&notice);
        view.process(text.as_bytes());
        view.draw(output, "uc | groups")?;
        loop {
            if cancel.is_cancelled() {
                return Ok(None);
            }
            let Some(event) = terminal.poll_event(POLL)? else {
                continue;
            };
            if let InteractiveTerminalEvent::Resize(new_size) = event {
                size = viewport(new_size, service);
                break;
            }
            if let InteractiveTerminalEvent::Input(input) = event {
                if let Some(name) = &mut name {
                    match input.item {
                        TerminalInputDto::Key {
                            key: KeyDto::Escape,
                        } => return Ok(None),
                        TerminalInputDto::Key { key: KeyDto::Enter } => {
                            match service.create_group(name, None, None, persistent) {
                                Ok(group) => return Ok(Some((group, true))),
                                Err(e) => {
                                    notice = e.to_string();
                                }
                            }
                        }
                        TerminalInputDto::Key {
                            key: KeyDto::Backspace,
                        } => {
                            name.pop();
                        }
                        TerminalInputDto::Text { text }
                            if name.len() + text.len() <= 48
                                && text
                                    .bytes()
                                    .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c)) =>
                        {
                            name.push_str(&text)
                        }
                        _ => {}
                    }
                } else {
                    let matched = keys.feed(input);
                    if matched.detached {
                        return Ok(None);
                    }
                    if keys.take_action() == Some(ShortcutAction::Create) {
                        persistent = true;
                        name = Some(String::new());
                    }
                    for item in matched.passthrough {
                        let key = PickerKey::from_input(&item);
                        if key == Some(PickerKey::Cancel) {
                            return Ok(None);
                        }
                        if key.is_some_and(|key| key.navigate(&mut selected, groups.len(), rows)) {
                            continue;
                        }
                        match item {
                            TerminalInputDto::Key { key: KeyDto::Enter } => {
                                if let Some(group) = groups.get(selected) {
                                    return Ok(Some((group.clone(), false)));
                                }
                            }
                            TerminalInputDto::Text { text } if text == "c" || text == "t" => {
                                persistent = text == "c";
                                name = Some(String::new())
                            }
                            _ => {}
                        }
                    }
                }
                break;
            }
        }
    }
}
