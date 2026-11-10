// SPDX-License-Identifier: Apache-2.0

//! Restart-aware interactive attachment over multiplexed product streams.
//!
//! Each pane owns terminal-output and lifecycle subscriptions for one stable
//! session ID. Byte-offset discontinuities recover a current screen, not lost
//! history. The local workspace owns layout and input focus; dropping a view
//! cancels its streams without stopping the managed session. Read-only mode
//! suppresses input and query replies but still applies terminal geometry.

use std::cell::Cell;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use crate::cli_support::{CliOutputError, CommonExitCode};
use crate::group::renderer::{Renderer, fit};
use undercurrent_protocol::{
    InputObservationPolicyDto, LifecycleStateDto, ProductErrorCategory, ProductOperation,
    ProductRequest, ProductStreamItem, ProductSuccess, ReadTerminalOutputRequest,
    ReadTerminalScreenRequest, ResizeTerminalRequest, RestartStatusDto, SendTerminalInputRequest,
    SessionDeltaDto, SessionDeltaKindDto, SessionSummaryDto, TerminalInitialModeDto,
    TerminalInputDto, TerminalOutputPageDto, TerminalScreenSnapshotDto, TerminalSizeDto,
    TerminalStreamItem,
};
use undercurrent_sdk::{ProductClientError, ProductClientErrorCategory, ProductSubscriptionItem};
use unicode_width::UnicodeWidthStr;

use crate::deadline::CliCommandDeadline;
use crate::{
    CliCommand, CliDiagnosticEvent, CliDiagnosticRecord, CliDiagnosticSink, CliError,
    CliExecutionLimits, CliInputError, CliMutationContextSource, CliProductStreamPort,
    CliProductSubscription, CliStreamCancellation, DetachMatcher, InteractiveTerminalEvent,
    InteractiveTerminalPort, OpenedCliSubscription, exact_name_lookup, expect_list,
    operation_mismatch, prepare_terminal_stream, prepare_wait_stream,
};

mod layout;
pub(crate) mod picker;
mod shortcuts;
mod workspace;

type AttachOpener<'a> =
    Box<dyn Fn(&str, TerminalSizeDto) -> Result<OpenedInteractiveAttach<'a>, CliError> + 'a>;

const IDLE_POLL_INTERVAL: Duration = Duration::from_millis(10);
const INPUT_COALESCE_INTERVAL: Duration = Duration::from_millis(2);
const STREAM_READINESS_INTERVAL: Duration = Duration::from_millis(1);
const MAXIMUM_STREAM_ITEMS_PER_TURN: usize = 32;
const MAXIMUM_FINAL_STREAM_ITEMS: usize = 256;
// Keep UC's frame separate from the caller's screen, as in group attachments.
const ATTACH_ENTER: &[u8] = b"\x1b[?1049h\x1b[?2004h\x1b[?1000h\x1b[?1002h\x1b[?1003h\x1b[?1015h\x1b[?1006h\x1b[0m\x1b[2J\x1b[H";
const ATTACH_LEAVE: &[u8] = b"\x1b[0m\x1b[?25h\x1b[?7h\x1b[r\x1b[?1006l\x1b[?1015l\x1b[?1003l\x1b[?1002l\x1b[?1000l\x1b[?2004l\x1b[?1049l";

/// One activated interactive attachment using two logical streams on one client.
pub struct OpenedInteractiveAttach<'a> {
    client: &'a dyn crate::CliProductRequestPort,
    reopen: AttachOpener<'a>,
    terminal: Arc<dyn CliProductSubscription>,
    lifecycle: Arc<dyn CliProductSubscription>,
    session_id: String,
    output_offset: u64,
    timeout: Duration,
    limits: CliExecutionLimits,
    mutations: Arc<CliMutationContextSource>,
    terminal_size: TerminalSizeDto,
    outer_size: TerminalSizeDto,
    renderer: Renderer,
    session_name: String,
    read_only: bool,
    input_finished: Cell<bool>,
    detach_hint: String,
    dirty: bool,
    clear_frame: bool,
    origin: (u16, u16),
    focused: bool,
    tiled: bool,
    visible: bool,
    overlay_resize: Option<TerminalSizeDto>,
    notice: String,
}

impl OpenedInteractiveAttach<'_> {
    /// Runs under the workspace's existing raw-mode and alternate-screen guard.
    /// Dropping the pane still cancels both streams; the caller restores its UI.
    pub(crate) fn consume_in_workspace<W: Write>(
        mut self,
        terminal: &mut dyn InteractiveTerminalPort,
        writer: &mut W,
        cancellation: &CliStreamCancellation,
        diagnostics: &dyn CliDiagnosticSink,
        mut detach: DetachMatcher,
    ) -> Result<CommonExitCode, CliError> {
        self.detach_hint = detach.detach_label().replace(": detach", ": workspace");
        let mut workspace = workspace::Workspace::new(self);
        // The outer workspace owns 1049 and raw mode. Only child input modes change here.
        write_bytes(writer, b"\x1b[?2004h\x1b[?1000h\x1b[?1002h\x1b[?1003h\x1b[?1015h\x1b[?1006h\x1b[0m\x1b[2J\x1b[H")?;
        workspace.run(terminal, writer, cancellation, diagnostics, &mut detach)
    }

    /// Returns the output subscription used to wake attach on process signals.
    ///
    /// Cloning the handle opens no connection or stream. This function performs
    /// no IO and does not panic.
    pub fn terminal_subscription(&self) -> Arc<dyn CliProductSubscription> {
        Arc::clone(&self.terminal)
    }

    /// Enters raw mode and consumes terminal, lifecycle, and local input events.
    ///
    /// The terminal is restored and both logical streams are cancelled on every
    /// return path. A terminal process state ends attachment only when automatic
    /// restart is not scheduled. Before returning, retained output is drained
    /// from the last rendered byte offset. This method performs terminal,
    /// product-port, and writer IO and does not panic unless an adapter panics.
    ///
    /// # Errors
    ///
    /// Returns typed terminal, product, input-bound, or writer failure.
    pub fn consume<W>(
        mut self,
        terminal: &mut dyn InteractiveTerminalPort,
        writer: &mut W,
        cancellation: &CliStreamCancellation,
        diagnostics: &dyn CliDiagnosticSink,
        read_only: bool,
        mut detach: DetachMatcher,
    ) -> Result<CommonExitCode, CliError>
    where
        W: Write,
    {
        if let Err(error) = terminal.enter_raw_mode() {
            self.cancel_streams();
            return Err(error.into());
        }
        self.read_only = read_only;
        self.detach_hint = detach.detach_label();
        let mut workspace = workspace::Workspace::new(self);
        let result = write_bytes(writer, ATTACH_ENTER)
            .and_then(|()| workspace.run(terminal, writer, cancellation, diagnostics, &mut detach));
        drop(workspace);
        let leave = write_bytes(writer, ATTACH_LEAVE);
        let restore = terminal.restore().map_err(CliError::from);
        match result {
            Err(error) => Err(error),
            Ok(exit_code) => {
                leave?;
                restore?;
                Ok(exit_code)
            }
        }
    }

    fn consume_local_event(
        &mut self,
        event: InteractiveTerminalEvent,
        read_only: bool,
        detach: &mut DetachMatcher,
        input: &mut InputBatch,
    ) -> Result<bool, CliError> {
        input.examined = input.examined.saturating_add(1);
        match event {
            InteractiveTerminalEvent::Input(event) => {
                let matched = detach.feed(event);
                for item in matched.passthrough {
                    self.dirty = true;
                    if !self.renderer.history_input(&item)
                        && !read_only
                        && let Some(item) = self.renderer.input(item)
                    {
                        input.push(self, item)?;
                    }
                }
                Ok(matched.detached)
            }
            InteractiveTerminalEvent::Resize(size) => {
                input.flush(self)?;
                self.resize(size)?;
                Ok(false)
            }
        }
    }

    fn consume_terminal_item<W>(
        &mut self,
        item: ProductSubscriptionItem,
        writer: &mut W,
        diagnostics: &dyn CliDiagnosticSink,
    ) -> Result<(), CliError>
    where
        W: Write,
    {
        match item {
            ProductSubscriptionItem::Terminal { item, .. } => match item {
                TerminalStreamItem::Screen(screen) => self.write_screen(&screen, writer),
                TerminalStreamItem::Output(output) => {
                    let start = output.start_offset();
                    let Some(end) = output.end_offset() else {
                        return Err(CliError::PaginationInvariant {
                            command: CliCommand::Attach,
                        });
                    };
                    if end <= self.output_offset {
                        return Ok(());
                    }
                    if start > self.output_offset {
                        diagnostics.emit(CliDiagnosticRecord::command(
                            CliDiagnosticEvent::StreamGap,
                            CliCommand::Attach,
                        ));
                        return self.resynchronize_screen(writer);
                    }
                    let skipped = usize::try_from(self.output_offset.saturating_sub(start))
                        .map_err(|_| attach_invariant())?;
                    let bytes = output.bytes().get(skipped..).ok_or_else(attach_invariant)?;
                    self.process_output(bytes)?;
                    self.output_offset = end;
                    Ok(())
                }
            },
            ProductSubscriptionItem::Gap(_) => {
                diagnostics.emit(CliDiagnosticRecord::command(
                    CliDiagnosticEvent::StreamGap,
                    CliCommand::Attach,
                ));
                self.resynchronize_screen(writer)
            }
            ProductSubscriptionItem::Json { .. } => Err(CliError::PaginationInvariant {
                command: CliCommand::Attach,
            }),
        }
    }

    fn consume_lifecycle_item(
        &mut self,
        item: ProductSubscriptionItem,
        diagnostics: &dyn CliDiagnosticSink,
    ) -> Result<bool, CliError> {
        let observation = match item {
            ProductSubscriptionItem::Json { item, .. } => {
                lifecycle_observation(item, &self.session_id)
            }
            ProductSubscriptionItem::Gap(_) => {
                diagnostics.emit(CliDiagnosticRecord::command(
                    CliDiagnosticEvent::StreamGap,
                    CliCommand::Attach,
                ));
                self.read_summary()?
                    .map_or(LifecycleObservation::Removed, |summary| {
                        LifecycleObservation::Summary(Box::new(summary))
                    })
            }
            ProductSubscriptionItem::Terminal { .. } => LifecycleObservation::None,
        };
        let finished = match observation {
            LifecycleObservation::Removed => true,
            LifecycleObservation::Summary(summary) => {
                self.dirty |= self.session_name != summary.name;
                self.session_name.clone_from(&summary.name);
                session_is_final(&summary)
            }
            LifecycleObservation::None => false,
        };
        if finished {
            self.input_finished.set(true);
        }
        Ok(finished)
    }

    fn send_input(&self, items: Vec<TerminalInputDto>) -> Result<(), CliError> {
        if self.input_finished.get() {
            return Ok(());
        }
        let result = self.client.request(
            ProductRequest::SendTerminalInput(SendTerminalInputRequest {
                expected_revision: None,
                mutation: self
                    .mutations
                    .next(CliCommand::Attach, ProductOperation::SendTerminalInput),
                session_id: self.session_id.clone(),
                delivery_id: None,
                items,
                observation_policy: InputObservationPolicyDto::Preserve,
            }),
            self.timeout,
        );
        let success = match result {
            Ok(success) => success,
            Err(error) => {
                // Output and exit travel on separate streams. A confirmed final
                // session no longer needs query replies; never retry the input.
                if matches!(&error, ProductClientError::Server(error)
                    if matches!(error.category, ProductErrorCategory::NotFound | ProductErrorCategory::FailedPrecondition))
                    && self.read_summary().is_ok_and(|summary| {
                        summary.is_none_or(|summary| session_is_final(&summary))
                    })
                {
                    self.input_finished.set(true);
                    return Ok(());
                }
                return Err(error.into());
            }
        };
        match success {
            ProductSuccess::SendTerminalInput(_) => Ok(()),
            other => Err(operation_mismatch(
                ProductOperation::SendTerminalInput,
                other,
            )),
        }
    }

    fn resize(&mut self, size: TerminalSizeDto) -> Result<(), CliError> {
        let outer_size = size;
        let size = attach_viewport(size, self.limits)?;
        let success = self.client.request(
            ProductRequest::ResizeTerminal(ResizeTerminalRequest {
                mutation: self
                    .mutations
                    .next(CliCommand::Attach, ProductOperation::ResizeTerminal),
                session_id: self.session_id.clone(),
                size,
            }),
            self.timeout,
        )?;
        match success {
            ProductSuccess::ResizeTerminal(_) => {
                self.terminal_size = size;
                self.outer_size = outer_size;
                self.renderer.resize(size);
                self.dirty = true;
                self.clear_frame = true;
                Ok(())
            }
            other => Err(operation_mismatch(ProductOperation::ResizeTerminal, other)),
        }
    }

    fn read_screen(&self) -> Result<TerminalScreenSnapshotDto, CliError> {
        let success = self.client.request(
            ProductRequest::ReadTerminalScreen(ReadTerminalScreenRequest {
                session_id: self.session_id.clone(),
            }),
            self.timeout,
        )?;
        match success {
            ProductSuccess::ReadTerminalScreen(screen) => Ok(screen),
            other => Err(operation_mismatch(
                ProductOperation::ReadTerminalScreen,
                other,
            )),
        }
    }

    fn read_summary(&self) -> Result<Option<SessionSummaryDto>, CliError> {
        let success = self.client.request(
            ProductRequest::ListSessions(crate::exact_id_lookup(&self.session_id)),
            self.timeout,
        )?;
        Ok(expect_list(success)?.items.into_iter().next())
    }

    fn resynchronize_screen<W>(&mut self, writer: &mut W) -> Result<(), CliError>
    where
        W: Write,
    {
        let screen = self.read_screen()?;
        self.renderer
            .reset(&screen.contents, screen.alternate_screen);
        self.dirty = true;
        self.output_offset = screen.output_offset;
        self.draw(writer)
    }

    fn write_screen<W>(
        &mut self,
        screen: &undercurrent_protocol::TerminalScreenSnapshot,
        writer: &mut W,
    ) -> Result<(), CliError>
    where
        W: Write,
    {
        self.renderer
            .reset(screen.contents(), screen.alternate_screen());
        self.dirty = true;
        self.output_offset = screen.output_offset();
        self.draw(writer)
    }

    fn bar(&self) -> String {
        if !self.notice.is_empty() {
            return fit(&self.notice, usize::from(self.terminal_size.columns));
        }
        let mode = if self.read_only {
            "read-only"
        } else {
            "attached"
        };
        let width = usize::from(self.terminal_size.columns.saturating_sub(1));
        let fixed = format!(" UC ·  · {mode}");
        let hint_width = self.detach_hint.width() + 3;
        let show_hint = width >= fixed.width() + hint_width + 8;
        let name_width =
            width.saturating_sub(fixed.width() + if show_hint { hint_width } else { 0 });
        let name = fit(&self.session_name, name_width);
        let mut bar = format!(" UC · {name} · {mode}");
        if show_hint {
            bar.push_str(&" ".repeat(width.saturating_sub(bar.width() + self.detach_hint.width())));
            bar.push_str(&self.detach_hint);
        }
        if self.tiled {
            bar = format!("{} {bar}", if self.focused { ">" } else { " " });
        }
        fit(&bar, width)
    }

    fn draw(&mut self, writer: &mut impl Write) -> Result<(), CliError> {
        if !self.dirty || !self.visible {
            return Ok(());
        }
        if self.clear_frame {
            self.renderer.invalidate();
            self.clear_frame = false;
        }
        let bar = self.bar();
        if self.outer_size.rows == 1 {
            // There is no room for content until the terminal grows again.
            write_bytes(
                writer,
                format!("\x1b[?25l\x1b[?7l\x1b[1;1H\x1b[0;7m{bar}\x1b[K\x1b[0m").as_bytes(),
            )?;
        } else {
            self.renderer.draw_region(
                writer,
                &bar,
                self.outer_size.rows - 1,
                self.origin,
                self.focused,
            )?;
        }
        self.dirty = false;
        Ok(())
    }

    fn process_output(&mut self, bytes: &[u8]) -> Result<(), CliError> {
        let replies = self.renderer.process(bytes);
        self.dirty = true;
        if !self.read_only && !replies.is_empty() {
            self.send_input(vec![TerminalInputDto::Raw { bytes: replies }])?;
        }
        Ok(())
    }

    fn drain_final_output<W>(
        &mut self,
        writer: &mut W,
        diagnostics: &dyn CliDiagnosticSink,
    ) -> Result<(), CliError>
    where
        W: Write,
    {
        loop {
            let result = self.client.request(
                ProductRequest::ReadTerminalOutput(ReadTerminalOutputRequest {
                    session_id: self.session_id.clone(),
                    from_offset: self.output_offset,
                    max_bytes: self.limits.terminal_output_page_bytes(),
                }),
                self.timeout,
            );
            let success = match result {
                Ok(success) => success,
                Err(error) if product_not_found(&error) => return Ok(()),
                Err(error) => return Err(error.into()),
            };
            let page = match success {
                ProductSuccess::ReadTerminalOutput(page) => page,
                other => {
                    return Err(operation_mismatch(
                        ProductOperation::ReadTerminalOutput,
                        other,
                    ));
                }
            };
            match page {
                TerminalOutputPageDto::Data {
                    from_offset,
                    to_offset,
                    next_offset,
                    has_more,
                    bytes,
                    ..
                } => {
                    let encoded_to = u64::try_from(bytes.len())
                        .ok()
                        .and_then(|length| from_offset.checked_add(length));
                    if encoded_to != Some(to_offset)
                        || next_offset < to_offset
                        || (has_more && next_offset <= self.output_offset)
                    {
                        return Err(attach_invariant());
                    }
                    if to_offset > self.output_offset {
                        if from_offset > self.output_offset {
                            diagnostics.emit(CliDiagnosticRecord::command(
                                CliDiagnosticEvent::StreamGap,
                                CliCommand::Attach,
                            ));
                            return self.resynchronize_screen(writer);
                        }
                        let skipped =
                            usize::try_from(self.output_offset.saturating_sub(from_offset))
                                .map_err(|_| attach_invariant())?;
                        let remaining = bytes.get(skipped..).ok_or_else(attach_invariant)?;
                        self.process_output(remaining)?;
                    }
                    self.output_offset = next_offset;
                    if !has_more {
                        return Ok(());
                    }
                }
                TerminalOutputPageDto::Gap { .. } => {
                    diagnostics.emit(CliDiagnosticRecord::command(
                        CliDiagnosticEvent::StreamGap,
                        CliCommand::Attach,
                    ));
                    return self.resynchronize_screen(writer);
                }
            }
        }
    }

    fn drain_terminal_quiescence<W>(
        &mut self,
        writer: &mut W,
        cancellation: &CliStreamCancellation,
        diagnostics: &dyn CliDiagnosticSink,
    ) -> Result<(), CliError>
    where
        W: Write,
    {
        for _ in 0..MAXIMUM_FINAL_STREAM_ITEMS {
            let Some(item) =
                receive_with_timeout(self.terminal.as_ref(), cancellation, IDLE_POLL_INTERVAL)?
            else {
                return Ok(());
            };
            self.consume_terminal_item(item, writer, diagnostics)?;
        }
        Ok(())
    }

    fn cancel_streams(&self) {
        self.terminal.cancel();
        self.lifecycle.cancel();
    }
}

/// Opens interactive attachment streams through one already-connected client.
pub struct InteractiveAttachExecutor<'a, P>
where
    P: CliProductStreamPort,
{
    scrollback: undercurrent_config::ScrollbackConfig,
    client: &'a P,
    timeout: Duration,
    limits: CliExecutionLimits,
    mutations: Arc<CliMutationContextSource>,
}

impl<'a, P> InteractiveAttachExecutor<'a, P>
where
    P: CliProductStreamPort,
    P::Subscription: 'static,
{
    /// Binds attachment activation to one multiplexed product client.
    ///
    /// This function performs no IO and does not panic.
    pub fn new(client: &'a P, timeout: Duration, limits: CliExecutionLimits) -> Self {
        Self {
            scrollback: undercurrent_config::ScrollbackConfig::default(),
            client,
            timeout,
            limits,
            mutations: Arc::new(CliMutationContextSource::default()),
        }
    }

    /// Sets validated local history preferences for this attachment and its reopen path.
    pub fn with_scrollback(mut self, scrollback: undercurrent_config::ScrollbackConfig) -> Self {
        self.scrollback = scrollback;
        self
    }

    /// Resolves one exact name, sends initial geometry, and opens two streams.
    ///
    /// Name lookup, resize, terminal output, and lifecycle activation all use
    /// the same client. The activation deadline covers every operation. If the
    /// lifecycle stream fails to open, the already-open terminal stream is
    /// cancelled. This method performs product-port IO and does not panic unless
    /// the injected adapter panics.
    ///
    /// # Errors
    ///
    /// Returns lookup, input, product, or activation-deadline failure.
    pub fn open_name(
        &self,
        name: &str,
        initial: TerminalInitialModeDto,
        size: TerminalSizeDto,
    ) -> Result<OpenedInteractiveAttach<'a>, CliError> {
        let deadline = CliCommandDeadline::start(CliCommand::Attach, self.timeout)?;
        let success = self.client.request(
            ProductRequest::ListSessions(exact_name_lookup(name)),
            deadline.remaining()?,
        )?;
        let session_id = expect_list(success)?
            .items
            .into_iter()
            .next()
            .map(|summary| summary.id)
            .ok_or_else(|| CliError::SessionNotFound {
                name: name.to_owned(),
            })?;
        self.open_at_deadline(session_id, initial, size, deadline)
    }

    /// Sends initial geometry and opens two streams for an already resolved ID.
    ///
    /// This supports `start --attach` without a mutable-name round trip. All
    /// operations share the same product client and one activation deadline.
    ///
    /// # Errors
    ///
    /// Returns input, product, or activation-deadline failure.
    pub fn open_session(
        &self,
        session_id: impl Into<String>,
        initial: TerminalInitialModeDto,
        size: TerminalSizeDto,
    ) -> Result<OpenedInteractiveAttach<'a>, CliError> {
        let deadline = CliCommandDeadline::start(CliCommand::Attach, self.timeout)?;
        self.open_at_deadline(session_id.into(), initial, size, deadline)
    }

    fn open_at_deadline(
        &self,
        session_id: String,
        initial: TerminalInitialModeDto,
        size: TerminalSizeDto,
        deadline: CliCommandDeadline,
    ) -> Result<OpenedInteractiveAttach<'a>, CliError> {
        let mutations = Arc::clone(&self.mutations);
        let outer_size = size;
        let size = attach_viewport(size, self.limits)?;
        let resized = self.client.request(
            ProductRequest::ResizeTerminal(ResizeTerminalRequest {
                mutation: mutations.next(CliCommand::Attach, ProductOperation::ResizeTerminal),
                session_id: session_id.clone(),
                size,
            }),
            deadline.remaining()?,
        )?;
        let session_name = match resized {
            ProductSuccess::ResizeTerminal(result) => result.session.name,
            other => return Err(operation_mismatch(ProductOperation::ResizeTerminal, other)),
        };

        let output_offset = match &initial {
            TerminalInitialModeDto::None
            | TerminalInitialModeDto::Screen
            | TerminalInitialModeDto::Tail { .. } => 0,
            TerminalInitialModeDto::Resume { after_offset } => *after_offset,
        };
        let OpenedCliSubscription {
            subscription: terminal,
            ..
        } = self.client.open_product_subscription(
            prepare_terminal_stream(session_id.clone(), initial),
            deadline.remaining()?,
        )?;
        let terminal: Arc<dyn CliProductSubscription> = Arc::new(terminal);
        let lifecycle = match self.client.open_product_subscription(
            prepare_wait_stream(session_id.clone()),
            deadline.remaining()?,
        ) {
            Ok(opened) => Arc::new(opened.subscription) as Arc<dyn CliProductSubscription>,
            Err(error) => {
                terminal.cancel();
                return Err(error.into());
            }
        };
        let client = self.client;
        let timeout = self.timeout;
        let limits = self.limits;
        let scrollback = self.scrollback;
        let reopen_mutations = Arc::clone(&mutations);
        Ok(OpenedInteractiveAttach {
            client: self.client,
            reopen: Box::new(move |id, size| {
                Self {
                    scrollback,
                    client,
                    timeout,
                    limits,
                    mutations: Arc::clone(&reopen_mutations),
                }
                .open_session(id, TerminalInitialModeDto::Screen, size)
            }),
            terminal,
            lifecycle,
            session_id,
            output_offset,
            timeout: self.timeout,
            limits: self.limits,
            mutations,
            terminal_size: size,
            outer_size,
            renderer: Renderer::with_scrollback(size, self.scrollback),
            session_name,
            read_only: false,
            input_finished: Cell::new(false),
            detach_hint: String::new(),
            dirty: true,
            clear_frame: false,
            origin: (0, 0),
            focused: true,
            tiled: false,
            visible: true,
            overlay_resize: None,
            notice: String::new(),
        })
    }
}

struct InputBatch {
    items: Vec<TerminalInputDto>,
    bytes: usize,
    examined: usize,
    limits: CliExecutionLimits,
}

impl InputBatch {
    const fn new(limits: CliExecutionLimits) -> Self {
        Self {
            items: Vec::new(),
            bytes: 0,
            examined: 0,
            limits,
        }
    }

    fn push(
        &mut self,
        attach: &OpenedInteractiveAttach<'_>,
        item: TerminalInputDto,
    ) -> Result<(), CliError> {
        let item_bytes = input_bytes(&item).ok_or(CliInputError::TerminalInputTooLarge {
            maximum: self.limits.terminal_input_bytes(),
        })?;
        if item_bytes > self.limits.terminal_input_bytes() {
            return Err(CliInputError::TerminalInputTooLarge {
                maximum: self.limits.terminal_input_bytes(),
            }
            .into());
        }
        let exceeds_items = self.items.len() == self.limits.terminal_input_items();
        let exceeds_bytes = self
            .bytes
            .checked_add(item_bytes)
            .is_none_or(|bytes| bytes > self.limits.terminal_input_bytes());
        if exceeds_items || exceeds_bytes {
            self.flush(attach)?;
        }
        self.bytes = self.bytes.saturating_add(item_bytes);
        self.items.push(item);
        Ok(())
    }

    fn flush(&mut self, attach: &OpenedInteractiveAttach<'_>) -> Result<(), CliError> {
        if self.items.is_empty() {
            return Ok(());
        }
        self.bytes = 0;
        attach.send_input(std::mem::take(&mut self.items))
    }
}

fn input_bytes(input: &TerminalInputDto) -> Option<usize> {
    match input {
        TerminalInputDto::Text { text } => Some(text.len()),
        TerminalInputDto::Raw { bytes } => Some(bytes.len()),
        TerminalInputDto::Paste { text } | TerminalInputDto::AutoText { text } => {
            text.len().checked_add(12)
        }
        TerminalInputDto::Key { .. } => Some(16),
        TerminalInputDto::Mouse { .. } => Some(18),
    }
}

fn attach_viewport(
    size: TerminalSizeDto,
    limits: CliExecutionLimits,
) -> Result<TerminalSizeDto, CliError> {
    if size.columns == 0 || size.rows == 0 {
        return Err(CliInputError::InvalidTerminalSize.into());
    }
    Ok(TerminalSizeDto {
        columns: size.columns.min(limits.terminal_columns()),
        rows: size
            .rows
            .saturating_sub(1)
            .max(1)
            .min(limits.terminal_rows()),
    })
}

fn receive_available(
    subscription: &dyn CliProductSubscription,
    cancellation: &CliStreamCancellation,
) -> Result<Option<ProductSubscriptionItem>, CliError> {
    receive_with_timeout(subscription, cancellation, STREAM_READINESS_INTERVAL)
}

fn receive_with_timeout(
    subscription: &dyn CliProductSubscription,
    cancellation: &CliStreamCancellation,
    timeout: Duration,
) -> Result<Option<ProductSubscriptionItem>, CliError> {
    match subscription.recv_timeout(timeout) {
        Ok(item) => Ok(Some(item)),
        Err(error) if error.category() == ProductClientErrorCategory::Timeout => Ok(None),
        Err(error)
            if cancellation.is_cancelled()
                && error.category() == ProductClientErrorCategory::Cancelled =>
        {
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

enum LifecycleObservation {
    Summary(Box<SessionSummaryDto>),
    Removed,
    None,
}

fn lifecycle_observation(item: ProductStreamItem, session_id: &str) -> LifecycleObservation {
    match item {
        ProductStreamItem::SessionSnapshot(page) => page
            .items
            .into_iter()
            .find(|summary| summary.id == session_id)
            .map_or(LifecycleObservation::Removed, |summary| {
                LifecycleObservation::Summary(Box::new(summary))
            }),
        ProductStreamItem::SessionDeltaBatch(batch) => batch
            .deltas
            .into_iter()
            .rev()
            .find(|delta| delta.session_id == session_id)
            .map_or(LifecycleObservation::None, lifecycle_delta),
        ProductStreamItem::SessionEvent(_) | ProductStreamItem::AgentEvent(_) => {
            LifecycleObservation::None
        }
    }
}

fn lifecycle_delta(delta: SessionDeltaDto) -> LifecycleObservation {
    if delta.kind == SessionDeltaKindDto::Removed {
        LifecycleObservation::Removed
    } else {
        delta.summary.map_or(LifecycleObservation::None, |summary| {
            LifecycleObservation::Summary(Box::new(summary))
        })
    }
}

const fn session_is_final(summary: &SessionSummaryDto) -> bool {
    let terminal = matches!(
        summary.lifecycle,
        LifecycleStateDto::Stopped | LifecycleStateDto::Exited { .. } | LifecycleStateDto::Failed
    );
    terminal && !matches!(summary.restart_status, RestartStatusDto::Scheduled { .. })
}

fn write_bytes(writer: &mut dyn Write, bytes: &[u8]) -> Result<(), CliError> {
    writer
        .write_all(bytes)
        .and_then(|()| writer.flush())
        .map_err(CliOutputError::from)?;
    Ok(())
}

fn product_not_found(error: &ProductClientError) -> bool {
    matches!(
        error,
        ProductClientError::Server(error) if error.category == ProductErrorCategory::NotFound
    )
}

const fn attach_invariant() -> CliError {
    CliError::PaginationInvariant {
        command: CliCommand::Attach,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use undercurrent_protocol::{
        HealthStatusDto, HealthSummaryDto, ProductStreamOpened, ProductStreamSpec,
        SessionMutationResultDto, SessionPageDto, TerminalOutputChunk, TerminalScreenSnapshot,
        WorkSummaryDto,
    };
    use undercurrent_sdk::ResilientSubscriptionError;

    use crate::{CliProductRequestPort, DetachKey, DisabledCliDiagnosticSink, InteractiveInput};

    use super::*;

    #[derive(Clone)]
    struct FakeSubscription {
        items: Arc<Mutex<VecDeque<Result<ProductSubscriptionItem, ProductClientError>>>>,
        cancelled: Arc<AtomicBool>,
    }

    impl FakeSubscription {
        fn new(items: impl IntoIterator<Item = ProductSubscriptionItem>) -> Self {
            Self {
                items: Arc::new(Mutex::new(items.into_iter().map(Ok).collect())),
                cancelled: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    impl CliProductSubscription for FakeSubscription {
        fn recv(&self) -> Result<ProductSubscriptionItem, ProductClientError> {
            self.recv_timeout(Duration::MAX)
        }

        fn recv_timeout(
            &self,
            timeout: Duration,
        ) -> Result<ProductSubscriptionItem, ProductClientError> {
            assert!(
                !timeout.is_zero(),
                "queued stream reads need a real deadline"
            );
            self.items
                .lock()
                .expect("subscription items")
                .pop_front()
                .unwrap_or_else(|| {
                    Err(ProductClientError::Subscription(
                        ResilientSubscriptionError::Timeout,
                    ))
                })
        }

        fn cancel(&self) {
            self.cancelled.store(true, Ordering::Release);
        }

        fn close(self) {
            self.cancel();
        }
    }

    struct FakeClient {
        requests: Mutex<Vec<ProductRequest>>,
        responses: Mutex<VecDeque<Result<ProductSuccess, ProductClientError>>>,
        specifications: Mutex<Vec<ProductStreamSpec>>,
        subscriptions: Mutex<VecDeque<FakeSubscription>>,
    }

    impl FakeClient {
        fn new(
            responses: impl IntoIterator<Item = ProductSuccess>,
            subscriptions: impl IntoIterator<Item = FakeSubscription>,
        ) -> Self {
            Self {
                requests: Mutex::new(Vec::new()),
                responses: Mutex::new(responses.into_iter().map(Ok).collect()),
                specifications: Mutex::new(Vec::new()),
                subscriptions: Mutex::new(subscriptions.into_iter().collect()),
            }
        }
    }

    impl CliProductRequestPort for FakeClient {
        fn request(
            &self,
            request: ProductRequest,
            _timeout: Duration,
        ) -> Result<ProductSuccess, ProductClientError> {
            self.requests.lock().expect("requests").push(request);
            self.responses
                .lock()
                .expect("responses")
                .pop_front()
                .expect("recorded response")
        }
    }

    impl CliProductStreamPort for FakeClient {
        type Subscription = FakeSubscription;

        fn open_product_subscription(
            &self,
            specification: ProductStreamSpec,
            _timeout: Duration,
        ) -> Result<OpenedCliSubscription<Self::Subscription>, ProductClientError> {
            let kind = specification.kind();
            self.specifications
                .lock()
                .expect("specifications")
                .push(specification);
            Ok(OpenedCliSubscription {
                metadata: ProductStreamOpened {
                    kind,
                    initial_sequence: 1,
                    resumable: true,
                    available_from: Some(1),
                },
                subscription: self
                    .subscriptions
                    .lock()
                    .expect("subscriptions")
                    .pop_front()
                    .expect("recorded subscription"),
            })
        }
    }

    #[test]
    fn final_exit_suppresses_terminal_query_replies() {
        let running = summary(LifecycleStateDto::Running, RestartStatusDto::Idle);
        let client = FakeClient::new(
            [resized(running)],
            [FakeSubscription::new([]), FakeSubscription::new([])],
        );
        let executor = InteractiveAttachExecutor::new(&client, Duration::from_secs(1), limits());
        let mut opened = executor
            .open_session(
                "session-1",
                TerminalInitialModeDto::Screen,
                TerminalSizeDto {
                    columns: 80,
                    rows: 24,
                },
            )
            .unwrap();
        let final_summary = summary(
            LifecycleStateDto::Stopped,
            RestartStatusDto::NotScheduled {
                reason: undercurrent_protocol::NoRestartReasonDto::ExplicitStop,
            },
        );
        assert!(
            opened
                .consume_lifecycle_item(
                    lifecycle_snapshot(1, final_summary),
                    &DisabledCliDiagnosticSink
                )
                .unwrap()
        );
        opened.process_output(b"\x1b[?6n").unwrap();
        assert!(
            !client
                .requests
                .lock()
                .unwrap()
                .iter()
                .any(|request| matches!(request, ProductRequest::SendTerminalInput(_)))
        );
    }

    #[test]
    fn rejected_input_requires_confirmed_final_state_and_is_never_retried() {
        for final_state in [false, true] {
            let running = summary(LifecycleStateDto::Running, RestartStatusDto::Idle);
            let client = FakeClient::new(
                [resized(running.clone())],
                [FakeSubscription::new([]), FakeSubscription::new([])],
            );
            let executor =
                InteractiveAttachExecutor::new(&client, Duration::from_secs(1), limits());
            let opened = executor
                .open_session(
                    "session-1",
                    TerminalInitialModeDto::Screen,
                    TerminalSizeDto {
                        columns: 80,
                        rows: 24,
                    },
                )
                .unwrap();
            let observed = if final_state {
                summary(
                    LifecycleStateDto::Stopped,
                    RestartStatusDto::NotScheduled {
                        reason: undercurrent_protocol::NoRestartReasonDto::ExplicitStop,
                    },
                )
            } else {
                running
            };
            client.responses.lock().unwrap().extend([
                Err(ProductClientError::Server(
                    undercurrent_protocol::ProductError {
                        effect_certainty: None,
                        operation: Some(ProductOperation::SendTerminalInput),
                        category: ProductErrorCategory::NotFound,
                        disposition: None,
                        message: "process exited".to_owned(),
                        session_id: Some("session-1".to_owned()),
                        retryable: false,
                        retry_after_millis: None,
                        limit: None,
                        resynchronization: None,
                    },
                )),
                Ok(ProductSuccess::ListSessions(page([observed]))),
            ]);
            assert_eq!(
                opened
                    .send_input(vec![TerminalInputDto::Raw {
                        bytes: b"x".to_vec()
                    }])
                    .is_ok(),
                final_state
            );
            assert_eq!(
                client
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|request| matches!(request, ProductRequest::SendTerminalInput(_)))
                    .count(),
                1
            );
            assert_eq!(opened.input_finished.get(), final_state);
        }
    }

    #[derive(Default)]
    struct FakeTerminal {
        events: VecDeque<InteractiveTerminalEvent>,
        entered: bool,
        restored: bool,
        size: Option<TerminalSizeDto>,
    }

    impl InteractiveTerminalPort for FakeTerminal {
        fn size(&mut self) -> Result<TerminalSizeDto, crate::CliTerminalError> {
            Ok(self.size.unwrap_or(TerminalSizeDto {
                columns: 80,
                rows: 24,
            }))
        }

        fn enter_raw_mode(&mut self) -> Result<(), crate::CliTerminalError> {
            self.entered = true;
            Ok(())
        }

        fn restore(&mut self) -> Result<(), crate::CliTerminalError> {
            self.restored = true;
            Ok(())
        }

        fn poll_event(
            &mut self,
            _timeout: Duration,
        ) -> Result<Option<InteractiveTerminalEvent>, crate::CliTerminalError> {
            let event = self.events.pop_front();
            if let Some(InteractiveTerminalEvent::Resize(size)) = &event {
                self.size = Some(*size);
            }
            Ok(event)
        }
    }

    #[derive(Default)]
    struct RecordingDiagnostics(Mutex<Vec<CliDiagnosticEvent>>);

    #[test]
    fn initial_picker_selects_by_id_preserves_offset_and_read_only_attachment() {
        use undercurrent_protocol::KeyDto;
        let first = summary(LifecycleStateDto::Running, RestartStatusDto::Idle);
        let mut second = first.clone();
        second.id = "session-2".into();
        second.name = "second".into();
        let streams = [FakeSubscription::new([]), FakeSubscription::new([])];
        let client = FakeClient::new(
            [
                ProductSuccess::ListSessions(page([first, second.clone()])),
                resized(second),
            ],
            streams.clone(),
        );
        let executor = InteractiveAttachExecutor::new(&client, Duration::from_secs(1), limits());
        let mut terminal = FakeTerminal {
            events: VecDeque::from([
                named_event(KeyDto::Up), // Clamp at the first row.
                input_event('k'),
                input_event('j'),
                input_event('k'),
                named_event(KeyDto::Down),
                named_event(KeyDto::Down), // Clamp at the last row.
                InteractiveTerminalEvent::Resize(TerminalSizeDto {
                    columns: 60,
                    rows: 12,
                }),
                named_event(KeyDto::Enter),
                input_event('x'), // Read-only attachment must not forward this.
                control_event('b'),
                input_event('d'),
            ]),
            ..Default::default()
        };
        let mut output = Vec::new();
        let outcome = picker::choose(
            &executor,
            TerminalInitialModeDto::Resume { after_offset: 42 },
            &mut terminal,
            &mut output,
            &CliStreamCancellation::default(),
        )
        .unwrap();
        let picker::Outcome::Attached(opened) = outcome else {
            panic!("selected session")
        };
        assert!(terminal.entered && terminal.restored);
        assert!(String::from_utf8_lossy(&output).contains("> second"));
        assert_eq!(opened.session_id, "session-2");
        assert_eq!(
            opened.outer_size,
            TerminalSizeDto {
                columns: 60,
                rows: 12
            }
        );
        let result = opened
            .consume(
                &mut terminal,
                &mut output,
                &CliStreamCancellation::default(),
                &DisabledCliDiagnosticSink,
                true,
                DetachMatcher::parse("ctrl-b,d").unwrap(),
            )
            .unwrap();
        assert_eq!(result, CommonExitCode::Success);
        assert!(String::from_utf8_lossy(&output).contains("read-only"));
        assert!(streams.iter().all(|s| s.cancelled.load(Ordering::Acquire)));
        let requests = client.requests.lock().unwrap();
        assert_eq!(requests.len(), 2); // Selection keys and read-only input stay local.
        assert!(matches!(&requests[1], ProductRequest::ResizeTerminal(r)
            if r.session_id == "session-2" && r.size == TerminalSizeDto { columns: 60, rows: 11 }));
        assert!(matches!(&client.specifications.lock().unwrap()[0],
            ProductStreamSpec::TerminalOutput(s)
                if s.session_id == "session-2" && s.initial == TerminalInitialModeDto::Resume { after_offset: 42 }));
    }

    #[test]
    fn initial_picker_empty_list_cancels_and_interrupts_without_opening_streams() {
        use undercurrent_protocol::KeyDto;
        for (event, expected, signal) in [
            (named_event(KeyDto::Escape), CommonExitCode::Success, false),
            (input_event('q'), CommonExitCode::Success, false),
            (control_event('c'), CommonExitCode::Interrupted, false),
            (
                named_event(KeyDto::Enter),
                CommonExitCode::Interrupted,
                true,
            ),
        ] {
            let client = FakeClient::new([ProductSuccess::ListSessions(page([]))], []);
            let executor =
                InteractiveAttachExecutor::new(&client, Duration::from_secs(1), limits());
            let mut terminal = FakeTerminal {
                events: VecDeque::from([
                    named_event(KeyDto::Down),
                    named_event(KeyDto::Enter),
                    event,
                ]),
                ..Default::default()
            };
            let cancellation = CliStreamCancellation::default();
            if signal {
                cancellation.cancel(&FakeSubscription::new([]));
            }
            let mut output = Vec::new();
            let outcome = picker::choose(
                &executor,
                TerminalInitialModeDto::Screen,
                &mut terminal,
                &mut output,
                &cancellation,
            )
            .unwrap();
            assert!(matches!(outcome, picker::Outcome::Exit(code) if code == expected));
            assert!(terminal.entered && terminal.restored);
            assert!(output.ends_with(b"\x1b[?1049l"));
            if !signal {
                assert!(String::from_utf8_lossy(&output).contains("No sessions."));
            }
            assert_eq!(client.requests.lock().unwrap().len(), 1);
            assert!(client.specifications.lock().unwrap().is_empty());
        }
    }

    #[test]
    fn initial_picker_vim_paging_keeps_history_on_failure_and_refresh_clears_it() {
        let running = summary(LifecycleStateDto::Running, RestartStatusDto::Idle);
        let mut first = page([running.clone()]);
        first.next_cursor = Some("page-b".into());
        let mut second = page([running.clone()]);
        second.next_cursor = Some("page-c".into());
        let third = page([running]);
        let client = FakeClient::new(
            [
                first.clone(),
                second.clone(),
                third,
                second.clone(),
                first.clone(),
                second.clone(),
                first,
                second,
            ]
            .map(ProductSuccess::ListSessions),
            [],
        );
        client.responses.lock().unwrap().insert(
            3,
            Err(ProductClientError::Subscription(
                ResilientSubscriptionError::Timeout,
            )),
        );
        let executor = InteractiveAttachExecutor::new(&client, Duration::from_secs(1), limits());
        let mut terminal = FakeTerminal {
            events: "hllhhhlrhnq".chars().map(input_event).collect(),
            ..Default::default()
        };
        let mut output = Vec::new();
        let outcome = picker::choose(
            &executor,
            TerminalInitialModeDto::Screen,
            &mut terminal,
            &mut output,
            &CliStreamCancellation::default(),
        )
        .unwrap();
        assert!(matches!(
            outcome,
            picker::Outcome::Exit(CommonExitCode::Success)
        ));
        assert!(terminal.restored);
        assert!(String::from_utf8_lossy(&output).contains("Could not load sessions"));
        assert!(client.specifications.lock().unwrap().is_empty());
        let requests = client.requests.lock().unwrap();
        let cursors: Vec<_> = requests
            .iter()
            .map(|request| {
                let ProductRequest::ListSessions(query) = request else {
                    panic!("navigation must only list sessions");
                };
                query.cursor.as_deref()
            })
            .collect();
        assert_eq!(
            cursors,
            [
                None,
                Some("page-b"),
                Some("page-c"),
                Some("page-b"),
                Some("page-b"),
                None,
                Some("page-b"),
                None,
                Some("page-b")
            ]
        );
    }

    #[test]
    fn initial_picker_retries_paging_and_disappearing_sessions_then_refreshes() {
        use undercurrent_protocol::KeyDto;
        let running = summary(LifecycleStateDto::Running, RestartStatusDto::Idle);
        let mut first = page([running.clone()]);
        first.next_cursor = Some("next-page".into());
        let client = FakeClient::new(
            [
                ProductSuccess::ListSessions(first),
                ProductSuccess::ListSessions(page([running.clone()])),
                ProductSuccess::ListSessions(page([])),
                ProductSuccess::ListSessions(page([running.clone()])),
                resized(running),
            ],
            [FakeSubscription::new([]), FakeSubscription::new([])],
        );
        {
            let mut responses = client.responses.lock().unwrap();
            responses.insert(
                1,
                Err(ProductClientError::Subscription(
                    ResilientSubscriptionError::Timeout,
                )),
            );
            responses.insert(
                3,
                Err(ProductClientError::Subscription(
                    ResilientSubscriptionError::ClientClosed,
                )),
            );
        }
        let executor = InteractiveAttachExecutor::new(&client, Duration::from_secs(1), limits());
        let mut terminal = FakeTerminal {
            events: VecDeque::from([
                input_event('n'),
                input_event('n'),
                named_event(KeyDto::Enter),
                input_event('r'),
                named_event(KeyDto::Enter),
                input_event('r'),
                named_event(KeyDto::Enter),
            ]),
            ..Default::default()
        };
        let mut output = Vec::new();
        let outcome = picker::choose(
            &executor,
            TerminalInitialModeDto::Screen,
            &mut terminal,
            &mut output,
            &CliStreamCancellation::default(),
        )
        .unwrap();
        let picker::Outcome::Attached(opened) = outcome else {
            panic!("retry attaches")
        };
        opened.cancel_streams();
        let output = String::from_utf8_lossy(&output);
        assert!(output.contains("Could not load sessions"));
        assert!(output.contains("Could not attach"));
        assert!(output.contains("No sessions."));
        assert!(terminal.restored);
        let requests = client.requests.lock().unwrap();
        let queries: Vec<_> = requests
            .iter()
            .filter_map(|r| match r {
                ProductRequest::ListSessions(q) => Some(q),
                _ => None,
            })
            .collect();
        assert_eq!(
            queries
                .iter()
                .map(|q| q.cursor.as_deref())
                .collect::<Vec<_>>(),
            [None, Some("next-page"), Some("next-page"), None, None]
        );
        assert!(
            queries
                .iter()
                .all(|q| q.page_size == 100 && !q.include_total)
        );
    }

    #[test]
    fn initial_picker_writer_failure_restores_terminal_and_cancels_opened_streams() {
        struct FailingWriter {
            fail_on_leave: bool,
        }
        impl Write for FailingWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if !self.fail_on_leave || bytes.ends_with(b"\x1b[?1049l") {
                    Err(std::io::Error::other("picker output failed"))
                } else {
                    Ok(bytes.len())
                }
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        for fail_on_leave in [false, true] {
            let running = summary(LifecycleStateDto::Running, RestartStatusDto::Idle);
            let streams = [FakeSubscription::new([]), FakeSubscription::new([])];
            let client = FakeClient::new(
                [
                    ProductSuccess::ListSessions(page([running.clone()])),
                    resized(running),
                ],
                streams.clone(),
            );
            let executor =
                InteractiveAttachExecutor::new(&client, Duration::from_secs(1), limits());
            let mut terminal = FakeTerminal {
                events: VecDeque::from([named_event(undercurrent_protocol::KeyDto::Enter)]),
                ..Default::default()
            };
            let outcome = picker::choose(
                &executor,
                TerminalInitialModeDto::Screen,
                &mut terminal,
                &mut FailingWriter { fail_on_leave },
                &CliStreamCancellation::default(),
            );
            assert!(outcome.is_err());
            assert!(terminal.entered && terminal.restored);
            if fail_on_leave {
                assert!(streams.iter().all(|s| s.cancelled.load(Ordering::Acquire)));
            } else {
                assert!(client.specifications.lock().unwrap().is_empty());
            }
        }
    }

    impl CliDiagnosticSink for RecordingDiagnostics {
        fn emit(&self, record: CliDiagnosticRecord) {
            self.0.lock().expect("diagnostics").push(record.event);
        }
    }

    #[test]
    fn split_creates_one_shell_and_cancels_all_streams_even_when_arrangement_fails() {
        for fail in [false, true] {
            let running = summary(LifecycleStateDto::Running, RestartStatusDto::Idle);
            let mut other = running.clone();
            other.id = "session-2".into();
            let subscriptions: Vec<_> = (0..4).map(|_| FakeSubscription::new([])).collect();
            let cancelled: Vec<_> = subscriptions.iter().map(|s| s.cancelled.clone()).collect();
            let client = FakeClient::new(
                [
                    resized(running.clone()),
                    ProductSuccess::ListSessions(page([])),
                    ProductSuccess::StartSession(SessionMutationResultDto {
                        session: other.clone(),
                        changed: true,
                        executable_resolution: None,
                    }),
                    resized(other.clone()),
                    resized(running),
                    resized(other),
                ],
                subscriptions,
            );
            if fail {
                *client.responses.lock().unwrap().back_mut().unwrap() = Err(
                    ProductClientError::Subscription(ResilientSubscriptionError::ClientClosed),
                );
            }
            let opened = InteractiveAttachExecutor::new(&client, Duration::from_secs(1), limits())
                .open_session(
                    "session-1",
                    TerminalInitialModeDto::Screen,
                    TerminalSizeDto {
                        columns: 80,
                        rows: 24,
                    },
                )
                .unwrap();
            let mut terminal = FakeTerminal {
                events: VecDeque::from([
                    prefix_event(),
                    input_event('%'),
                    input_event('c'),
                    prefix_event(),
                    input_event('d'),
                ]),
                ..Default::default()
            };
            let result = opened.consume(
                &mut terminal,
                &mut vec![],
                &CliStreamCancellation::default(),
                &DisabledCliDiagnosticSink,
                false,
                crate::shortcuts::matcher(&undercurrent_config::ShortcutPreferences::default())
                    .unwrap(),
            );
            assert_eq!(result.is_err(), fail);
            let requests = client.requests.lock().unwrap();
            let starts: Vec<_> = requests
                .iter()
                .filter_map(|r| match r {
                    ProductRequest::StartSession(r) => Some(r),
                    _ => None,
                })
                .collect();
            assert_eq!(starts.len(), 1);
            assert!(starts[0].name.starts_with("pane-"));
            assert!(
                !requests
                    .iter()
                    .any(|r| matches!(r, ProductRequest::StopSession(_)))
            );
            assert!(cancelled.iter().all(|s| s.load(Ordering::Acquire)));
            assert!(terminal.restored);
        }
    }

    #[test]
    fn mouse_coordinates_are_local_to_the_pane_and_focus_flushes_prior_input() {
        use undercurrent_protocol::{MouseButtonDto, MouseEventDto, MouseEventKindDto};
        let mouse = |column| {
            InteractiveTerminalEvent::Input(InteractiveInput {
                item: TerminalInputDto::Mouse {
                    event: MouseEventDto {
                        column,
                        row: 2,
                        button: MouseButtonDto::Left,
                        event: MouseEventKindDto::Press,
                    },
                },
                detach_key: None,
            })
        };
        let running = summary(LifecycleStateDto::Running, RestartStatusDto::Idle);
        let mut other = running.clone();
        other.id = "session-2".into();
        let client = FakeClient::new(
            [
                resized(running.clone()),
                ProductSuccess::ListSessions(page([other.clone()])),
                resized(other.clone()),
                resized(running),
                resized(other),
                input_delivered(),
                input_delivered(),
            ],
            [
                FakeSubscription::new([]),
                FakeSubscription::new([]),
                FakeSubscription::new([terminal_screen(1, 0, b"\x1b[?1000h\x1b[?1006h")]),
                FakeSubscription::new([]),
            ],
        );
        let opened = InteractiveAttachExecutor::new(&client, Duration::from_secs(1), limits())
            .open_session(
                "session-1",
                TerminalInitialModeDto::Screen,
                TerminalSizeDto {
                    columns: 80,
                    rows: 24,
                },
            )
            .unwrap();
        let mut terminal = FakeTerminal {
            events: VecDeque::from([
                prefix_event(),
                input_event('%'),
                named_event(undercurrent_protocol::KeyDto::Enter),
                mouse(45),
                mouse(2),
                input_event('a'),
                prefix_event(),
                input_event('d'),
            ]),
            ..Default::default()
        };
        opened
            .consume(
                &mut terminal,
                &mut vec![],
                &CliStreamCancellation::default(),
                &DisabledCliDiagnosticSink,
                false,
                crate::shortcuts::matcher(&undercurrent_config::ShortcutPreferences::default())
                    .unwrap(),
            )
            .unwrap();
        let requests = client.requests.lock().unwrap();
        let inputs: Vec<_> = requests
            .iter()
            .filter_map(|r| match r {
                ProductRequest::SendTerminalInput(r) => Some(r),
                _ => None,
            })
            .collect();
        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[0].session_id, "session-2");
        assert!(
            inputs[0].items
                == vec![TerminalInputDto::Raw {
                    bytes: b"\x1b[<0;5;2M".to_vec()
                }]
        );
        assert_eq!(inputs[1].session_id, "session-1");
        assert!(inputs[1].items == vec![TerminalInputDto::Text { text: "a".into() }]);
    }

    #[test]
    fn tiled_attach_routes_input_and_copy_buffer_to_the_focused_session() {
        let running = summary(LifecycleStateDto::Running, RestartStatusDto::Idle);
        let mut other = running.clone();
        other.id = "session-2".into();
        other.name = "other".into();
        let subscriptions = [
            FakeSubscription::new([terminal_screen(1, 0, b"hello\x1b[H")]),
            FakeSubscription::new([]),
            FakeSubscription::new([terminal_screen(1, 0, b"second\x1b[H\x1b[?2004h")]),
            FakeSubscription::new([]),
        ];
        let cancelled: Vec<_> = subscriptions.iter().map(|s| s.cancelled.clone()).collect();
        let client = FakeClient::new(
            [
                resized(running.clone()),
                ProductSuccess::ListSessions(page([other.clone()])),
                resized(other.clone()),
                resized(running),
                resized(other),
                input_delivered(),
                input_delivered(),
            ],
            subscriptions,
        );
        let opened = InteractiveAttachExecutor::new(&client, Duration::from_secs(1), limits())
            .open_session(
                "session-1",
                TerminalInitialModeDto::Screen,
                TerminalSizeDto {
                    columns: 80,
                    rows: 24,
                },
            )
            .unwrap();
        let mut terminal = FakeTerminal {
            events: VecDeque::from([
                prefix_event(),
                input_event('['),
                input_event('v'),
                input_event('l'),
                input_event('l'),
                input_event('l'),
                input_event('l'),
                input_event('y'),
                named_event(undercurrent_protocol::KeyDto::Escape),
                prefix_event(),
                input_event('%'),
                named_event(undercurrent_protocol::KeyDto::Enter),
                prefix_event(),
                input_event(']'),
                prefix_event(),
                input_event('o'),
                input_event('a'),
                prefix_event(),
                input_event('d'),
            ]),
            ..Default::default()
        };
        let mut output = vec![];
        opened
            .consume(
                &mut terminal,
                &mut output,
                &CliStreamCancellation::default(),
                &DisabledCliDiagnosticSink,
                false,
                crate::shortcuts::matcher(&undercurrent_config::ShortcutPreferences::default())
                    .unwrap(),
            )
            .unwrap();
        let screen = attached_screen(&output, 24, 80);
        let first = screen.rows(0, 80).next().unwrap();
        assert!(first.starts_with("hello"));
        assert_eq!(first[40..].trim_end(), "second");
        assert!(
            screen
                .rows(0, 80)
                .last()
                .unwrap()
                .contains(">  UC · worker")
        );
        assert!(String::from_utf8_lossy(&output).contains("\x1b]52;c;aGVsbG8=\x07"));
        let requests = client.requests.lock().unwrap();
        let inputs: Vec<_> = requests
            .iter()
            .filter_map(|r| match r {
                ProductRequest::SendTerminalInput(r) => Some(r),
                _ => None,
            })
            .collect();
        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[0].session_id, "session-2");
        assert!(
            inputs[0].items
                == vec![TerminalInputDto::Raw {
                    bytes: b"\x1b[200~hello\x1b[201~".to_vec()
                }]
        );
        assert_eq!(inputs[1].session_id, "session-1");
        assert!(inputs[1].items == vec![TerminalInputDto::Text { text: "a".into() }]);
        let sizes: Vec<_> = requests
            .iter()
            .filter_map(|r| match r {
                ProductRequest::ResizeTerminal(r) => Some(r.size),
                _ => None,
            })
            .collect();
        assert_eq!(
            &sizes[2..],
            &[
                TerminalSizeDto {
                    columns: 39,
                    rows: 23
                },
                TerminalSizeDto {
                    columns: 40,
                    rows: 23
                }
            ]
        );
        assert!(cancelled.iter().all(|s| s.load(Ordering::Acquire)));
        assert!(terminal.restored);
    }

    #[test]
    fn read_only_tiles_resize_zoom_and_close_without_sending_or_stopping() {
        let running = summary(LifecycleStateDto::Running, RestartStatusDto::Idle);
        let mut other = running.clone();
        other.id = "session-2".into();
        other.name = "other".into();
        let subscriptions: Vec<_> = (0..4).map(|_| FakeSubscription::new([])).collect();
        let cancelled: Vec<_> = subscriptions.iter().map(|s| s.cancelled.clone()).collect();
        let client = FakeClient::new(
            [
                resized(running.clone()),
                ProductSuccess::ListSessions(page([other.clone()])),
                resized(other.clone()),
                resized(running.clone()),
                resized(other.clone()),
                resized(other.clone()), // zoom
                resized(other.clone()), // tiny terminal
                resized(other.clone()), // growing while zoomed
                resized(running.clone()),
                resized(other.clone()), // unzoom
                resized(running),       // closing the second pane
            ],
            subscriptions,
        );
        let opened = InteractiveAttachExecutor::new(&client, Duration::from_secs(1), limits())
            .open_session(
                "session-1",
                TerminalInitialModeDto::Screen,
                TerminalSizeDto {
                    columns: 80,
                    rows: 24,
                },
            )
            .unwrap();
        let mut terminal = FakeTerminal {
            events: VecDeque::from([
                prefix_event(),
                input_event('"'),
                input_event('c'), // read-only cannot create
                named_event(undercurrent_protocol::KeyDto::Enter),
                input_event('a'),
                prefix_event(),
                input_event(']'),
                prefix_event(),
                input_event('z'),
                InteractiveTerminalEvent::Resize(TerminalSizeDto {
                    columns: 2,
                    rows: 1,
                }),
                InteractiveTerminalEvent::Resize(TerminalSizeDto {
                    columns: 100,
                    rows: 30,
                }),
                prefix_event(),
                input_event('z'),
                prefix_event(),
                input_event('x'),
                prefix_event(),
                input_event('d'),
            ]),
            ..Default::default()
        };
        opened
            .consume(
                &mut terminal,
                &mut vec![],
                &CliStreamCancellation::default(),
                &DisabledCliDiagnosticSink,
                true,
                crate::shortcuts::matcher(&undercurrent_config::ShortcutPreferences::default())
                    .unwrap(),
            )
            .unwrap();
        let requests = client.requests.lock().unwrap();
        assert!(requests.iter().all(|r| matches!(
            r,
            ProductRequest::ResizeTerminal(_) | ProductRequest::ListSessions(_)
        )));
        let ProductRequest::ResizeTerminal(last) = requests.last().unwrap() else {
            panic!("resize")
        };
        assert_eq!(last.session_id, "session-1");
        assert_eq!(
            last.size,
            TerminalSizeDto {
                columns: 100,
                rows: 29
            }
        );
        assert!(cancelled.iter().all(|s| s.load(Ordering::Acquire)));
        assert!(terminal.restored);
    }

    #[test]
    fn read_only_history_navigation_redraws_without_sending_child_input() {
        let client = FakeClient::new(
            [resized(summary(
                LifecycleStateDto::Running,
                RestartStatusDto::Idle,
            ))],
            [FakeSubscription::new([]), FakeSubscription::new([])],
        );
        let mut opened = InteractiveAttachExecutor::new(&client, Duration::from_secs(1), limits())
            .open_session(
                "session-1",
                TerminalInitialModeDto::Screen,
                TerminalSizeDto {
                    columns: 80,
                    rows: 4,
                },
            )
            .unwrap();
        for index in 0..10 {
            opened
                .process_output(format!("line {index}\r\n").as_bytes())
                .unwrap();
        }
        let mut output = Vec::new();
        opened.draw(&mut output).unwrap();
        let mut detach =
            crate::shortcuts::matcher(&undercurrent_config::ShortcutPreferences::default())
                .unwrap();
        let mut input = InputBatch::new(limits());
        let wheel = InteractiveTerminalEvent::Input(InteractiveInput {
            item: TerminalInputDto::Mouse {
                event: undercurrent_protocol::MouseEventDto {
                    row: 1,
                    column: 1,
                    button: undercurrent_protocol::MouseButtonDto::ScrollUp,
                    event: undercurrent_protocol::MouseEventKindDto::Press,
                },
            },
            detach_key: None,
        });
        assert!(
            !opened
                .consume_local_event(wheel, true, &mut detach, &mut input)
                .unwrap()
        );
        assert!(opened.dirty);
        input.flush(&opened).unwrap();
        opened.draw(&mut output).unwrap();
        let mut outer = vt100::Parser::new(4, 80, 0);
        outer.process(&output);
        assert_eq!(
            outer.screen().rows(0, 80).next().unwrap().trim_end(),
            "line 5"
        );
        assert!(outer.screen().contents().contains("history"));
        assert_eq!(client.requests.lock().unwrap().len(), 1);
    }

    #[test]
    fn exact_attach_uses_two_streams_and_waits_across_scheduled_restart() {
        let running = summary(LifecycleStateDto::Running, RestartStatusDto::Idle);
        let scheduled = summary(
            LifecycleStateDto::Exited { code: Some(1) },
            RestartStatusDto::Scheduled {
                attempt: 1,
                not_before_millis: 20,
            },
        );
        let final_summary = summary(
            LifecycleStateDto::Exited { code: Some(0) },
            RestartStatusDto::NotScheduled {
                reason: undercurrent_protocol::NoRestartReasonDto::SuccessfulExit,
            },
        );
        let terminal = FakeSubscription::new([terminal_screen(1, 5, b"seed")]);
        let lifecycle = FakeSubscription::new([
            lifecycle_snapshot(1, scheduled),
            lifecycle_snapshot(2, final_summary),
        ]);
        let client = FakeClient::new(
            [
                ProductSuccess::ListSessions(page([running.clone()])),
                resized(running),
                output_page(5, b"tail", false),
            ],
            [terminal.clone(), lifecycle.clone()],
        );
        let executor = InteractiveAttachExecutor::new(&client, Duration::from_secs(1), limits());
        let opened = executor
            .open_name(
                "worker",
                TerminalInitialModeDto::Screen,
                TerminalSizeDto {
                    columns: 80,
                    rows: 24,
                },
            )
            .expect("attachment opens");
        let mut native = FakeTerminal::default();
        let mut output = Vec::new();

        let outcome = opened
            .consume(
                &mut native,
                &mut output,
                &CliStreamCancellation::default(),
                &DisabledCliDiagnosticSink,
                false,
                DetachMatcher::parse("ctrl-b,d").expect("detach keys"),
            )
            .expect("attachment completes");

        assert_eq!(outcome, CommonExitCode::Success);
        let screen = attached_screen(&output, 24, 80);
        assert_eq!(screen.rows(0, 80).next().unwrap().trim_end(), "seedtail");
        assert!(
            screen
                .rows(0, 80)
                .last()
                .unwrap()
                .contains("UC · worker · attached")
        );
        assert!(native.entered && native.restored);
        assert!(terminal.cancelled.load(Ordering::Acquire));
        assert!(lifecycle.cancelled.load(Ordering::Acquire));
        let specifications = client.specifications.lock().expect("specifications");
        assert_eq!(specifications.len(), 2);
        assert!(matches!(
            &specifications[0],
            ProductStreamSpec::TerminalOutput(specification)
                if specification.session_id == "session-1"
                    && specification.initial == TerminalInitialModeDto::Screen
        ));
        assert!(matches!(
            &specifications[1],
            ProductStreamSpec::SessionWatch(specification)
                if specification.query.filter.ids == ["session-1"]
                    && specification.query.page_size == 1
        ));
        let requests = client.requests.lock().expect("requests");
        assert!(matches!(requests[0], ProductRequest::ListSessions(_)));
        assert!(matches!(requests[1], ProductRequest::ResizeTerminal(_)));
        assert!(matches!(requests[2], ProductRequest::ReadTerminalOutput(_)));
    }

    #[test]
    fn local_input_resize_and_detach_preserve_order_and_bounds() {
        let running = summary(LifecycleStateDto::Running, RestartStatusDto::Idle);
        let terminal_stream = FakeSubscription::new([]);
        let lifecycle_stream = FakeSubscription::new([]);
        let client = FakeClient::new(
            [
                resized(running.clone()),
                input_delivered(),
                resized(running),
            ],
            [terminal_stream, lifecycle_stream],
        );
        let executor = InteractiveAttachExecutor::new(&client, Duration::from_secs(1), limits());
        let opened = executor
            .open_session(
                "session-1",
                TerminalInitialModeDto::Screen,
                TerminalSizeDto {
                    columns: 80,
                    rows: 24,
                },
            )
            .expect("attachment opens");
        let mut native = FakeTerminal {
            events: VecDeque::from([
                input_event('x'),
                InteractiveTerminalEvent::Resize(TerminalSizeDto {
                    columns: 500,
                    rows: 500,
                }),
                InteractiveTerminalEvent::Input(InteractiveInput {
                    item: TerminalInputDto::Mouse {
                        event: undercurrent_protocol::MouseEventDto {
                            column: 161,
                            row: 81,
                            button: undercurrent_protocol::MouseButtonDto::Left,
                            event: undercurrent_protocol::MouseEventKindDto::Press,
                        },
                    },
                    detach_key: None,
                }),
                control_event('b'),
                input_event('d'),
            ]),
            ..FakeTerminal::default()
        };

        let outcome = opened
            .consume(
                &mut native,
                &mut Vec::new(),
                &CliStreamCancellation::default(),
                &DisabledCliDiagnosticSink,
                false,
                DetachMatcher::parse("ctrl-b,d").expect("detach keys"),
            )
            .expect("detaches");

        assert_eq!(outcome, CommonExitCode::Success);
        assert!(native.entered && native.restored);
        let requests = client.requests.lock().expect("requests");
        assert_eq!(requests.len(), 3);
        let ProductRequest::SendTerminalInput(input) = &requests[1] else {
            panic!("expected input request")
        };
        assert_eq!(input.items.len(), 1);
        assert!(matches!(
            &input.items[0],
            TerminalInputDto::Text { text } if text == "x"
        ));
        assert_eq!(
            input.observation_policy,
            InputObservationPolicyDto::Preserve
        );
        let ProductRequest::ResizeTerminal(resize) = &requests[2] else {
            panic!("expected resize request")
        };
        assert_eq!(resize.size.columns, 160);
        assert_eq!(resize.size.rows, 80);
    }

    #[test]
    fn output_discontinuity_resynchronizes_from_an_authoritative_screen() {
        let running = summary(LifecycleStateDto::Running, RestartStatusDto::Idle);
        let final_summary = summary(
            LifecycleStateDto::Stopped,
            RestartStatusDto::NotScheduled {
                reason: undercurrent_protocol::NoRestartReasonDto::ExplicitStop,
            },
        );
        let terminal = FakeSubscription::new([ProductSubscriptionItem::Terminal {
            sequence: 1,
            item: TerminalStreamItem::Output(TerminalOutputChunk::new(
                10,
                Arc::<[u8]>::from(&b"stale"[..]),
            )),
        }]);
        let lifecycle = FakeSubscription::new([lifecycle_snapshot(1, final_summary)]);
        let client = FakeClient::new(
            [
                resized(running),
                ProductSuccess::ReadTerminalScreen(screen_dto(15, b"synchronized")),
                output_page(15, b"", false),
            ],
            [terminal, lifecycle],
        );
        let opened = InteractiveAttachExecutor::new(&client, Duration::from_secs(1), limits())
            .open_session(
                "session-1",
                TerminalInitialModeDto::Screen,
                TerminalSizeDto {
                    columns: 80,
                    rows: 24,
                },
            )
            .expect("attachment opens");
        let diagnostics = RecordingDiagnostics::default();
        let mut output = Vec::new();

        opened
            .consume(
                &mut FakeTerminal::default(),
                &mut output,
                &CliStreamCancellation::default(),
                &diagnostics,
                false,
                DetachMatcher::parse("ctrl-b,d").expect("detach keys"),
            )
            .expect("attachment completes");

        assert_eq!(
            attached_screen(&output, 24, 80)
                .rows(0, 80)
                .next()
                .unwrap()
                .trim_end(),
            "synchronized"
        );
        assert_eq!(
            diagnostics.0.lock().expect("diagnostics").as_slice(),
            [CliDiagnosticEvent::StreamGap]
        );
    }

    fn prefix_event() -> InteractiveTerminalEvent {
        InteractiveTerminalEvent::Input(InteractiveInput {
            item: TerminalInputDto::Raw {
                bytes: b"\x1bu".to_vec(),
            },
            detach_key: Some(crate::DetachKey::Alt('u')),
        })
    }

    fn named_event(key: undercurrent_protocol::KeyDto) -> InteractiveTerminalEvent {
        InteractiveTerminalEvent::Input(InteractiveInput {
            item: TerminalInputDto::Key { key },
            detach_key: None,
        })
    }

    #[test]
    fn shortcut_help_restores_screen_without_forwarding_keys() {
        let running = summary(LifecycleStateDto::Running, RestartStatusDto::Idle);
        let client = FakeClient::new(
            [
                resized(running),
                ProductSuccess::ReadTerminalScreen(screen_dto(15, b"restored-screen")),
            ],
            [FakeSubscription::new([]), FakeSubscription::new([])],
        );
        let opened = InteractiveAttachExecutor::new(&client, Duration::from_secs(1), limits())
            .open_session(
                "session-1",
                TerminalInitialModeDto::Screen,
                TerminalSizeDto {
                    columns: 80,
                    rows: 24,
                },
            )
            .unwrap();
        let mut terminal = FakeTerminal {
            events: VecDeque::from([
                prefix_event(),
                input_event('?'),
                named_event(undercurrent_protocol::KeyDto::Escape),
                prefix_event(),
                input_event('d'),
            ]),
            ..Default::default()
        };
        let mut output = Vec::new();
        let result = opened
            .consume(
                &mut terminal,
                &mut output,
                &CliStreamCancellation::default(),
                &DisabledCliDiagnosticSink,
                true,
                crate::shortcuts::matcher(&undercurrent_config::ShortcutPreferences::default())
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(result, CommonExitCode::Success);
        let screen = attached_screen(&output, 24, 80);
        let rendered = String::from_utf8(output).unwrap();
        assert!(rendered.contains("Alt+U, d: detach"));
        assert_eq!(
            screen.rows(0, 80).next().unwrap().trim_end(),
            "restored-screen"
        );
        assert!(screen.rows(0, 80).last().unwrap().contains("read-only"));
        assert!(terminal.restored);
        assert_eq!(client.requests.lock().unwrap().len(), 2);
    }

    #[test]
    fn picker_switches_by_id_and_cancels_both_attachments_without_stopping_sessions() {
        let running = summary(LifecycleStateDto::Running, RestartStatusDto::Idle);
        let mut other = running.clone();
        other.id = "session-2".to_owned();
        other.name = "other".to_owned();
        let subscriptions: Vec<_> = (0..4).map(|_| FakeSubscription::new([])).collect();
        let cancelled: Vec<_> = subscriptions.iter().map(|s| s.cancelled.clone()).collect();
        let client = FakeClient::new(
            [
                resized(running),
                ProductSuccess::ListSessions(page([other.clone()])),
                resized(other),
                input_delivered(),
            ],
            subscriptions,
        );
        let opened = InteractiveAttachExecutor::new(&client, Duration::from_secs(1), limits())
            .open_session(
                "session-1",
                TerminalInitialModeDto::Screen,
                TerminalSizeDto {
                    columns: 80,
                    rows: 24,
                },
            )
            .unwrap();
        let mut terminal = FakeTerminal {
            events: VecDeque::from([
                prefix_event(),
                input_event('s'),
                named_event(undercurrent_protocol::KeyDto::Enter),
                input_event('x'),
                prefix_event(),
                input_event('d'),
            ]),
            ..Default::default()
        };
        let mut output = Vec::new();
        let result = opened
            .consume(
                &mut terminal,
                &mut output,
                &CliStreamCancellation::default(),
                &DisabledCliDiagnosticSink,
                false,
                crate::shortcuts::matcher(&undercurrent_config::ShortcutPreferences::default())
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(result, CommonExitCode::Success);
        assert!(cancelled.iter().all(|c| c.load(Ordering::Acquire)));
        let requests = client.requests.lock().unwrap();
        assert_eq!(requests.len(), 4);
        let screen = attached_screen(&output, 24, 80);
        let bar = screen.rows(0, 80).last().unwrap();
        assert!(bar.contains("UC · other · attached"));
        assert!(bar.contains("Alt+U, d: detach"));
        let ProductRequest::ResizeTerminal(first_resize) = &requests[0] else {
            panic!("initial resize")
        };
        let ProductRequest::ResizeTerminal(second_resize) = &requests[2] else {
            panic!("switch resize")
        };
        assert_eq!(first_resize.size.rows, 23);
        assert_eq!(second_resize.size.rows, 23);
        assert_ne!(
            first_resize.mutation.idempotency_key,
            second_resize.mutation.idempotency_key
        );
        let ProductRequest::SendTerminalInput(input) = &requests[3] else {
            panic!("expected input")
        };
        assert_eq!(input.session_id, "session-2");
        assert_eq!(input.items.len(), 1);
        assert!(matches!(&input.items[0], TerminalInputDto::Text { text } if text == "x"));
        assert!(terminal.restored);
    }

    #[test]
    fn picker_pages_and_failed_switch_preserve_original_attachment() {
        let running = summary(LifecycleStateDto::Running, RestartStatusDto::Idle);
        let mut other = running.clone();
        other.id = "session-2".to_owned();
        let mut first = page([running.clone()]);
        first.next_cursor = Some("next-page".to_owned());
        let client = FakeClient::new(
            [
                resized(running),
                ProductSuccess::ListSessions(first),
                ProductSuccess::ListSessions(page([other])),
                ProductSuccess::ReadTerminalScreen(screen_dto(12, b"original-screen")),
            ],
            [FakeSubscription::new([]), FakeSubscription::new([])],
        );
        client.responses.lock().unwrap().insert(
            3,
            Err(ProductClientError::Subscription(
                ResilientSubscriptionError::Timeout,
            )),
        );
        let opened = InteractiveAttachExecutor::new(&client, Duration::from_secs(1), limits())
            .open_session(
                "session-1",
                TerminalInitialModeDto::Screen,
                TerminalSizeDto {
                    columns: 80,
                    rows: 24,
                },
            )
            .unwrap();
        let mut terminal = FakeTerminal {
            events: VecDeque::from([
                prefix_event(),
                input_event('s'),
                input_event('n'),
                named_event(undercurrent_protocol::KeyDto::Enter),
                named_event(undercurrent_protocol::KeyDto::Escape),
                prefix_event(),
                input_event('d'),
            ]),
            ..Default::default()
        };
        let mut output = Vec::new();
        opened
            .consume(
                &mut terminal,
                &mut output,
                &CliStreamCancellation::default(),
                &DisabledCliDiagnosticSink,
                false,
                crate::shortcuts::matcher(&undercurrent_config::ShortcutPreferences::default())
                    .unwrap(),
            )
            .unwrap();
        let screen = attached_screen(&output, 24, 80);
        let rendered = String::from_utf8(output).unwrap();
        assert!(rendered.contains("Could not attach"));
        assert_eq!(
            screen.rows(0, 80).next().unwrap().trim_end(),
            "original-screen"
        );
        let requests = client.requests.lock().unwrap();
        let ProductRequest::ListSessions(next) = &requests[2] else {
            panic!("next page")
        };
        assert_eq!(next.cursor.as_deref(), Some("next-page"));
        let ProductRequest::ReadTerminalScreen(restored) = &requests[4] else {
            panic!("restore")
        };
        assert_eq!(restored.session_id, "session-1");
    }

    #[test]
    fn status_survives_fullscreen_scrolling_resizing_and_session_rename() {
        let running = summary(LifecycleStateDto::Running, RestartStatusDto::Idle);
        let client = FakeClient::new(
            (0..4).map(|_| resized(running.clone())),
            [FakeSubscription::new([]), FakeSubscription::new([])],
        );
        let mut opened = InteractiveAttachExecutor::new(&client, Duration::from_secs(1), limits())
            .open_session(
                "session-1",
                TerminalInitialModeDto::Screen,
                TerminalSizeDto {
                    columns: 80,
                    rows: 24,
                },
            )
            .unwrap();
        opened.detach_hint = DetachMatcher::with_prefix("ctrl-a").unwrap().detach_label();
        let mut output = Vec::new();
        let mut outer = vt100::Parser::new(24, 80, 0);
        for bytes in [
            b"\x1b[?1049h\x1b[2J\x1b[999;1Hbottom".as_slice(),
            b"\r\nscroll\r\nscroll\x1b[2J\x1b[Happlication",
            b"\x1b[?1049l\x1b[2J\x1b[Hshell",
        ] {
            opened.process_output(bytes).unwrap();
            output.clear();
            opened.draw(&mut output).unwrap();
            outer.process(&output);
            let bar = outer.screen().rows(0, 80).last().unwrap();
            assert!(bar.contains("UC · worker · attached"));
            assert!(bar.contains("Ctrl+A, d: detach"));
        }
        let mut renamed = running;
        renamed.name = "renamed".into();
        opened
            .consume_lifecycle_item(lifecycle_snapshot(2, renamed), &DisabledCliDiagnosticSink)
            .unwrap();
        for rows in [6, 1, 24] {
            opened
                .resize(TerminalSizeDto { columns: 80, rows })
                .unwrap();
            outer.set_size(rows, 80);
            output.clear();
            opened.draw(&mut output).unwrap();
            outer.process(&output);
            assert!(
                outer
                    .screen()
                    .rows(0, 80)
                    .last()
                    .unwrap()
                    .contains("UC · renamed · attached")
            );
        }
        assert_eq!(
            outer.screen().rows(0, 80).next().unwrap().trim_end(),
            "shell"
        );
        let requests = client.requests.lock().unwrap();
        let sizes: Vec<_> = requests
            .iter()
            .filter_map(|r| match r {
                ProductRequest::ResizeTerminal(r) => Some(r.size.rows),
                _ => None,
            })
            .collect();
        assert_eq!(sizes, [23, 5, 1, 23]);
    }

    #[test]
    fn silent_read_only_attach_shows_identity_and_restores_callers_screen() {
        let running = summary(LifecycleStateDto::Running, RestartStatusDto::Idle);
        let client = FakeClient::new(
            [resized(running)],
            [FakeSubscription::new([]), FakeSubscription::new([])],
        );
        let opened = InteractiveAttachExecutor::new(&client, Duration::from_secs(1), limits())
            .open_session(
                "session-1",
                TerminalInitialModeDto::Screen,
                TerminalSizeDto {
                    columns: 80,
                    rows: 24,
                },
            )
            .unwrap();
        let mut terminal = FakeTerminal {
            events: VecDeque::from([input_event('x'), control_event('b'), input_event('d')]),
            ..Default::default()
        };
        let mut output = Vec::new();
        opened
            .consume(
                &mut terminal,
                &mut output,
                &CliStreamCancellation::default(),
                &DisabledCliDiagnosticSink,
                true,
                DetachMatcher::parse("ctrl-b,d").unwrap(),
            )
            .unwrap();
        let screen = attached_screen(&output, 24, 80);
        let bar = screen.rows(0, 80).last().unwrap();
        assert!(bar.contains("UC · worker · read-only"));
        assert!(bar.contains("Ctrl+B, d: detach"));
        assert_eq!(
            client.requests.lock().unwrap().len(),
            1,
            "read-only input is not forwarded"
        );
        let mut outer = vt100::Parser::new(24, 80, 0);
        outer.process(b"original shell");
        outer.process(&output);
        assert!(!outer.screen().alternate_screen());
        assert_eq!(outer.screen().contents(), "original shell");
        assert!(terminal.restored);
    }

    #[test]
    fn stream_failure_restores_terminal_and_cancels_both_streams() {
        let running = summary(LifecycleStateDto::Running, RestartStatusDto::Idle);
        let stream = FakeSubscription::new([]);
        stream
            .items
            .lock()
            .unwrap()
            .push_back(Err(ProductClientError::Subscription(
                ResilientSubscriptionError::ClientClosed,
            )));
        let lifecycle = FakeSubscription::new([]);
        let client = FakeClient::new([resized(running)], [stream.clone(), lifecycle.clone()]);
        let opened = InteractiveAttachExecutor::new(&client, Duration::from_secs(1), limits())
            .open_session(
                "session-1",
                TerminalInitialModeDto::Screen,
                TerminalSizeDto {
                    columns: 80,
                    rows: 24,
                },
            )
            .unwrap();
        let mut terminal = FakeTerminal::default();
        let mut output = Vec::new();
        assert!(
            opened
                .consume(
                    &mut terminal,
                    &mut output,
                    &CliStreamCancellation::default(),
                    &DisabledCliDiagnosticSink,
                    false,
                    crate::shortcuts::matcher(&undercurrent_config::ShortcutPreferences::default())
                        .unwrap()
                )
                .is_err()
        );
        assert!(output.ends_with(ATTACH_LEAVE));
        assert!(terminal.restored);
        assert!(stream.cancelled.load(Ordering::Acquire));
        assert!(lifecycle.cancelled.load(Ordering::Acquire));
    }

    fn attached_screen(output: &[u8], rows: u16, columns: u16) -> vt100::Screen {
        let mut outer = vt100::Parser::new(rows, columns, 0);
        let frame = output
            .strip_suffix(ATTACH_LEAVE)
            .expect("attachment restores terminal modes");
        outer.process(frame);
        outer.screen().clone()
    }

    fn limits() -> CliExecutionLimits {
        CliExecutionLimits::new(crate::CliExecutionLimitValues {
            catalog_page_size: 100,
            bulk_selection_sessions: 1_000,
            bulk_parallel_requests: 4,
            terminal_columns: 160,
            terminal_rows: 80,
            terminal_input_items: 16,
            terminal_input_bytes: 1_024,
            terminal_output_page_bytes: 128,
        })
        .expect("valid limits")
    }

    fn summary(
        lifecycle: LifecycleStateDto,
        restart_status: RestartStatusDto,
    ) -> SessionSummaryDto {
        SessionSummaryDto {
            id: "session-1".to_owned(),
            name: "worker".to_owned(),
            lifecycle,
            foreground: crate::test_foreground(
                undercurrent_protocol::ExecutionStateDto::Quiescent,
                undercurrent_protocol::InteractionStateDto::None,
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
            labels: Some(BTreeMap::new()),
            started_at_millis: Some(1),
            updated_at_millis: 1,
            restart_count: 0,
            restart_status,
            revision: 1,
            lifecycle_revision: 1,
        }
    }

    fn page<const N: usize>(items: [SessionSummaryDto; N]) -> SessionPageDto {
        SessionPageDto {
            items: items.into(),
            next_cursor: None,
            snapshot_revision: 1,
            total: None,
        }
    }

    fn resized(session: SessionSummaryDto) -> ProductSuccess {
        ProductSuccess::ResizeTerminal(SessionMutationResultDto {
            session,
            changed: true,
            executable_resolution: None,
        })
    }

    fn input_delivered() -> ProductSuccess {
        ProductSuccess::SendTerminalInput(undercurrent_protocol::SendTerminalInputResultDto {
            session_id: "session-1".to_owned(),
            delivered_at_millis: 1,
            delivery_id: None,
            duplicate: false,
            foreground_revision: None,
        })
    }

    fn output_page(from_offset: u64, bytes: &[u8], has_more: bool) -> ProductSuccess {
        let to_offset = from_offset + u64::try_from(bytes.len()).expect("test length");
        ProductSuccess::ReadTerminalOutput(TerminalOutputPageDto::Data {
            from_offset,
            to_offset,
            next_offset: to_offset,
            has_more,
            bytes: bytes.to_vec(),
            payload_digest_sha256: undercurrent_protocol::execution_output_payload_digest_sha256(
                bytes,
            ),
            truncation: None,
        })
    }

    fn terminal_screen(
        sequence: u64,
        output_offset: u64,
        contents: &[u8],
    ) -> ProductSubscriptionItem {
        ProductSubscriptionItem::Terminal {
            sequence,
            item: TerminalStreamItem::Screen(
                TerminalScreenSnapshot::new(
                    output_offset,
                    80,
                    24,
                    0,
                    0,
                    true,
                    false,
                    Arc::<[u8]>::from(contents),
                )
                .expect("valid screen"),
            ),
        }
    }

    fn screen_dto(output_offset: u64, contents: &[u8]) -> TerminalScreenSnapshotDto {
        TerminalScreenSnapshotDto {
            checkpoint: "checkpoint".to_owned(),
            output_offset,
            columns: 80,
            rows: 24,
            cursor_row: 0,
            cursor_column: 0,
            cursor_visible: true,
            alternate_screen: false,
            contents: contents.to_vec(),
        }
    }

    fn lifecycle_snapshot(sequence: u64, summary: SessionSummaryDto) -> ProductSubscriptionItem {
        ProductSubscriptionItem::Json {
            sequence,
            item: ProductStreamItem::SessionSnapshot(page([summary])),
        }
    }

    fn input_event(character: char) -> InteractiveTerminalEvent {
        InteractiveTerminalEvent::Input(InteractiveInput {
            item: TerminalInputDto::Text {
                text: character.to_string(),
            },
            detach_key: Some(DetachKey::Literal(character)),
        })
    }

    fn control_event(character: char) -> InteractiveTerminalEvent {
        InteractiveTerminalEvent::Input(InteractiveInput {
            item: TerminalInputDto::Key {
                key: undercurrent_protocol::KeyDto::Control { character },
            },
            detach_key: Some(DetachKey::Control(character)),
        })
    }
}
