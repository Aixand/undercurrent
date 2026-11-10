// SPDX-License-Identifier: Apache-2.0

//! Blocking, cancellation-aware CLI product-stream consumption.

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::cli_support::CommonExitCode;
use undercurrent_protocol::ProductStreamOpened;
use undercurrent_sdk::ProductClientErrorCategory;

use crate::{
    AttachArguments, CliError, CliPresenter, CliProductStreamPort, CliProductSubscription,
    StreamCliExecutor, TopArguments, WaitArguments, WatchArguments, session_summary_text,
};

/// One daemon-backed command with a persistent stream response shape.
pub enum ConnectedStreamCommand<'a> {
    /// Open one bounded dashboard query stream.
    Top(&'a TopArguments),
    /// Watch one bounded query window or exact named selection.
    Watch(&'a WatchArguments),
    /// Observe one selected terminal, seeded by screen or retained offset.
    Attach(&'a AttachArguments),
    /// Continue selected terminal output strictly after one finite page.
    LogsFollow(&'a str, u64),
    /// Continue output for an already resolved stable session ID.
    LogsFollowSession(&'a str, u64),
}

/// One activated logical stream sharing its client's physical connection.
pub struct OpenedConnectedStream {
    /// Server-declared replay and activation metadata.
    pub metadata: ProductStreamOpened,
    subscription: Arc<dyn CliProductSubscription>,
}

impl OpenedConnectedStream {
    /// Returns a shared handle suitable for a scoped signal adapter.
    ///
    /// Cloning this handle does not open a connection or logical stream. This
    /// function performs no IO and does not panic.
    pub fn subscription(&self) -> Arc<dyn CliProductSubscription> {
        Arc::clone(&self.subscription)
    }

    /// Consumes the activated logical stream without idle polling.
    ///
    /// # Errors
    ///
    /// Returns typed stream or presentation failure. Local cancellation maps
    /// to an interrupted outcome. This method performs blocking stream and
    /// writer IO and does not panic unless an injected adapter panics.
    pub fn consume<W>(
        self,
        cancellation: &CliStreamCancellation,
        presenter: CliPresenter,
        writer: &mut W,
    ) -> Result<CommonExitCode, CliError>
    where
        W: Write,
    {
        consume_product_stream(self.subscription.as_ref(), cancellation, presenter, writer)
    }
}

/// Selective stream-command application adapter over one product client.
pub struct ConnectedStreamDispatcher<'a, P>
where
    P: CliProductStreamPort,
{
    executor: StreamCliExecutor<'a, P>,
    presenter: CliPresenter,
}

impl<'a, P> ConnectedStreamDispatcher<'a, P>
where
    P: CliProductStreamPort,
    P::Subscription: 'static,
{
    /// Binds every stream activation and finite target lookup to one client.
    ///
    /// This function performs no IO and does not panic.
    pub const fn new(client: &'a P, timeout: std::time::Duration, presenter: CliPresenter) -> Self {
        Self {
            executor: StreamCliExecutor::new(client, timeout),
            presenter,
        }
    }

    /// Opens one selective logical stream on the injected physical client.
    ///
    /// # Errors
    ///
    /// Returns typed local validation, exact-name lookup, product, or stream
    /// activation failure. This method performs product-port IO and does not
    /// panic unless an injected port panics.
    pub fn open(
        &self,
        command: ConnectedStreamCommand<'_>,
    ) -> Result<OpenedConnectedStream, CliError> {
        let opened = match command {
            ConnectedStreamCommand::Top(arguments) => self.executor.top(arguments)?,
            ConnectedStreamCommand::Watch(arguments) => self.executor.watch(arguments)?,
            ConnectedStreamCommand::Attach(arguments) => self.executor.attach(arguments)?,
            ConnectedStreamCommand::LogsFollow(name, after_offset) => {
                self.executor.logs_follow(name, after_offset)?
            }
            ConnectedStreamCommand::LogsFollowSession(session_id, after_offset) => self
                .executor
                .logs_follow_session(session_id, after_offset)?,
        };
        Ok(OpenedConnectedStream {
            metadata: opened.metadata,
            subscription: Arc::new(opened.subscription),
        })
    }

    /// Waits for one selected condition and presents its matching summary.
    ///
    /// The wait uses one selective subscription internally but has a finite
    /// result contract, so JSON rather than JSON Lines is accepted. This method
    /// performs product-port and writer IO and does not panic unless an injected
    /// adapter panics.
    ///
    /// # Errors
    ///
    /// Returns typed condition, lookup, timeout, stream, resynchronization,
    /// serialization, or writer failure.
    pub fn wait<W>(
        &self,
        arguments: &WaitArguments,
        writer: &mut W,
    ) -> Result<CommonExitCode, CliError>
    where
        W: Write,
    {
        let result = self.executor.wait(arguments)?;
        self.presenter.write_finite(writer, &result, |verbosity| {
            session_summary_text(&result, verbosity)
        })?;
        Ok(CommonExitCode::Success)
    }
}

/// Shared local cancellation state for one logical CLI stream.
#[derive(Debug, Clone, Default)]
pub struct CliStreamCancellation {
    cancelled: Arc<AtomicBool>,
}

impl CliStreamCancellation {
    /// Cancels a local event loop without a subscription to wake.
    pub(crate) fn cancel_local(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Returns whether the caller explicitly cancelled this stream.
    ///
    /// This function performs no IO and does not panic.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Marks local cancellation and wakes one blocked logical subscription.
    ///
    /// The operation is idempotent and does not stop managed runtime processes.
    /// Peer isolation depends on the [`CliProductSubscription::cancel`] contract.
    /// It performs bounded client writer IO and does not panic unless an injected
    /// subscription panics.
    pub fn cancel<S>(&self, subscription: &S)
    where
        S: CliProductSubscription + ?Sized,
    {
        self.cancel_local();
        subscription.cancel();
    }
}

/// Consumes one logical product stream with no idle polling.
///
/// Each decoded item is written before the next blocking receive, preserving
/// per-stream order and applying transport credit only at the SDK boundary.
/// The caller can share the subscription and [`CliStreamCancellation`] with a
/// signal adapter; cancellation wakes this function immediately.
///
/// # Errors
///
/// Returns typed SDK, output-shape, serialization, or writer failure. A local
/// cancellation is returned as [`CommonExitCode::Interrupted`] rather than an
/// error so the process boundary can exit without printing a false failure.
/// This function performs blocking stream and writer IO and does not panic
/// unless an injected subscription or writer panics.
pub fn consume_product_stream<S, W>(
    subscription: &S,
    cancellation: &CliStreamCancellation,
    presenter: CliPresenter,
    writer: &mut W,
) -> Result<CommonExitCode, CliError>
where
    S: CliProductSubscription + ?Sized,
    W: Write,
{
    loop {
        match subscription.recv() {
            Ok(item) => {
                if let Err(error) = presenter.write_product_stream_item(writer, &item) {
                    subscription.cancel();
                    return Err(error);
                }
            }
            Err(error)
                if cancellation.is_cancelled()
                    && error.category() == ProductClientErrorCategory::Cancelled =>
            {
                return Ok(CommonExitCode::Interrupted);
            }
            Err(error) => return Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use crate::cli_support::{CliContext, CommandTimeout, OutputFormat, Verbosity};
    use undercurrent_protocol::{
        ProductRequest, ProductStreamItem, ProductStreamKind, ProductStreamOpened,
        ProductStreamSpec, ProductSuccess, SessionPageDto, TerminalOutputChunk, TerminalStreamItem,
    };
    use undercurrent_sdk::{
        ProductClientError, ProductSubscriptionItem, ResilientSubscriptionError,
    };

    use crate::{CliProductRequestPort, OpenedCliSubscription, SessionProjectionArgument};

    use super::*;

    #[derive(Clone)]
    struct FakeSubscription {
        items: Arc<Mutex<VecDeque<Result<ProductSubscriptionItem, ProductClientError>>>>,
        cancelled: Arc<AtomicBool>,
    }

    impl FakeSubscription {
        fn new(
            items: impl IntoIterator<Item = Result<ProductSubscriptionItem, ProductClientError>>,
        ) -> Self {
            Self {
                items: Arc::new(Mutex::new(items.into_iter().collect())),
                cancelled: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    impl CliProductSubscription for FakeSubscription {
        fn recv(&self) -> Result<ProductSubscriptionItem, ProductClientError> {
            self.items
                .lock()
                .expect("items lock")
                .pop_front()
                .unwrap_or_else(|| {
                    Err(ProductClientError::Subscription(
                        ResilientSubscriptionError::Unsubscribed,
                    ))
                })
        }

        fn recv_timeout(
            &self,
            _timeout: Duration,
        ) -> Result<ProductSubscriptionItem, ProductClientError> {
            self.recv()
        }

        fn cancel(&self) {
            self.cancelled.store(true, Ordering::Release);
        }

        fn close(self) {
            self.cancel();
        }
    }

    #[test]
    fn ordered_jsonl_items_are_written_before_terminal_stream_failure() {
        let subscription = FakeSubscription::new([
            Ok(snapshot(1)),
            Ok(terminal(2, vec![0xff, b'x'])),
            Err(ProductClientError::Subscription(
                ResilientSubscriptionError::ClientClosed,
            )),
        ]);
        let mut output = Vec::new();

        let error = consume_product_stream(
            &subscription,
            &CliStreamCancellation::default(),
            presenter(OutputFormat::Jsonl),
            &mut output,
        )
        .expect_err("remote close");

        assert_eq!(
            undercurrent_sdk::ProductClientErrorCategory::Unavailable,
            match error {
                CliError::Product(error) => error.category(),
                other => panic!("unexpected error: {other}"),
            }
        );
        let records = String::from_utf8(output)
            .expect("JSONL UTF-8")
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("JSON record"))
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["record"], "session_snapshot");
        assert_eq!(records[1]["record"], "terminal_output");
        assert_eq!(records[1]["sequence"], 2);
    }

    #[test]
    fn explicit_cancellation_wakes_receive_and_maps_to_interrupted() {
        let subscription = FakeSubscription::new([Err(ProductClientError::Subscription(
            ResilientSubscriptionError::Unsubscribed,
        ))]);
        let cancellation = CliStreamCancellation::default();
        cancellation.cancel(&subscription);

        let result = consume_product_stream(
            &subscription,
            &cancellation,
            presenter(OutputFormat::Text),
            &mut Vec::new(),
        )
        .expect("local cancellation");

        assert_eq!(result, CommonExitCode::Interrupted);
        assert!(subscription.cancelled.load(Ordering::Acquire));
    }

    #[test]
    fn writer_failure_cancels_only_the_consumed_subscription() {
        let subscription = FakeSubscription::new([Ok(terminal(1, vec![b'x']))]);
        let mut writer = FailingWriter;

        let error = consume_product_stream(
            &subscription,
            &CliStreamCancellation::default(),
            presenter(OutputFormat::Text),
            &mut writer,
        )
        .expect_err("writer failure");

        assert!(matches!(error, CliError::Output(_)));
        assert!(subscription.cancelled.load(Ordering::Acquire));
    }

    #[test]
    fn connected_dispatch_opens_and_cancels_one_top_stream_on_the_same_port() {
        let client = FakeStreamClient::new(FakeSubscription::new([Err(
            ProductClientError::Subscription(ResilientSubscriptionError::Unsubscribed),
        )]));
        let dispatcher = ConnectedStreamDispatcher::new(
            &client,
            Duration::from_secs(1),
            presenter(OutputFormat::Jsonl),
        );
        let arguments = TopArguments {
            page_size: Some(25),
            projection: SessionProjectionArgument::Compact,
        };

        let opened = dispatcher
            .open(ConnectedStreamCommand::Top(&arguments))
            .expect("top stream");

        assert_eq!(opened.metadata.kind, ProductStreamKind::SessionWatch);
        let handle = opened.subscription();
        let cancellation = CliStreamCancellation::default();
        cancellation.cancel(handle.as_ref());
        let outcome = opened
            .consume(
                &cancellation,
                presenter(OutputFormat::Jsonl),
                &mut Vec::new(),
            )
            .expect("interrupted top stream");
        assert_eq!(outcome, CommonExitCode::Interrupted);
        assert_eq!(
            client.specifications.lock().expect("specifications").len(),
            1
        );
        let specification = client
            .specifications
            .lock()
            .expect("specifications")
            .first()
            .cloned()
            .expect("top specification");
        let ProductStreamSpec::SessionWatch(watch) = specification else {
            panic!("expected session watch")
        };
        assert_eq!(watch.query.page_size, 25);
    }

    struct FakeStreamClient {
        subscription: FakeSubscription,
        specifications: Mutex<Vec<ProductStreamSpec>>,
    }

    impl FakeStreamClient {
        fn new(subscription: FakeSubscription) -> Self {
            Self {
                subscription,
                specifications: Mutex::new(Vec::new()),
            }
        }
    }

    impl CliProductRequestPort for FakeStreamClient {
        fn request(
            &self,
            _request: ProductRequest,
            _timeout: Duration,
        ) -> Result<ProductSuccess, ProductClientError> {
            panic!("top stream must not issue a finite request")
        }
    }

    impl CliProductStreamPort for FakeStreamClient {
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
                subscription: self.subscription.clone(),
            })
        }
    }

    struct FailingWriter;

    impl Write for FailingWriter {
        fn write(&mut self, _buffer: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "closed output",
            ))
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn snapshot(sequence: u64) -> ProductSubscriptionItem {
        ProductSubscriptionItem::Json {
            sequence,
            item: ProductStreamItem::SessionSnapshot(SessionPageDto {
                items: Vec::new(),
                next_cursor: None,
                snapshot_revision: 1,
                total: None,
            }),
        }
    }

    fn terminal(sequence: u64, bytes: Vec<u8>) -> ProductSubscriptionItem {
        ProductSubscriptionItem::Terminal {
            sequence,
            item: TerminalStreamItem::Output(TerminalOutputChunk::new(0, Arc::<[u8]>::from(bytes))),
        }
    }

    fn presenter(output: OutputFormat) -> CliPresenter {
        CliPresenter::new(CliContext::new(
            output,
            Verbosity::Normal,
            CommandTimeout::new(Duration::from_secs(1), Duration::from_secs(1)).expect("timeout"),
        ))
    }
}
