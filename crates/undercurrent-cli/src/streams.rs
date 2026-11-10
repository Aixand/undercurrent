// SPDX-License-Identifier: Apache-2.0

//! Selective CLI stream use cases over one shared multiplexed product client.

use std::collections::BTreeSet;
use std::time::Duration;

use undercurrent_protocol::{
    ExecutionStateDto, HealthStatusDto, InteractionStateDto, LifecycleKindDto, LifecycleStateDto,
    ProductRequest, ProductStreamItem, ProductStreamOpened, ProductStreamSpec, SessionDeltaDto,
    SessionDeltaKindDto, SessionPageDto, SessionSummaryDto, TerminalInitialModeDto,
};
use undercurrent_sdk::{
    OpenedProductSubscription, ProductClient, ProductClientError, ProductClientErrorCategory,
    ProductSubscription, ProductSubscriptionItem,
};

use crate::deadline::CliCommandDeadline;
use crate::{
    AttachArguments, CliCommand, CliError, CliInputError, CliProductRequestPort, TopArguments,
    WaitArguments, WatchArguments, exact_id_lookup, exact_name_lookup, execution, expect_list,
    health, interaction, lifecycle, prepare_terminal_stream, prepare_top_stream,
    prepare_wait_stream, prepare_watch_stream,
};

/// Persistent logical subscription consumed by CLI stream use cases.
pub trait CliProductSubscription: Send + Sync {
    /// Blocks across reconnection until one decoded item or terminal failure.
    ///
    /// # Errors
    ///
    /// Returns a typed SDK stream, recovery, cancellation, or protocol failure.
    fn recv(&self) -> Result<ProductSubscriptionItem, ProductClientError>;

    /// Waits for and decodes one item up to the caller-owned deadline.
    ///
    /// # Errors
    ///
    /// Returns a typed SDK stream, recovery, timeout, or protocol failure.
    fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<ProductSubscriptionItem, ProductClientError>;

    /// Cancels this logical interest and wakes its blocked receiver.
    ///
    /// Managed runtime processes are not stopped. With the production SDK
    /// adapter, peers normally remain active; failure to queue `CloseStream`
    /// closes the shared physical connection and peers follow normal recovery.
    /// See [close pressure](../../../docs/sdk/rust/streams-and-recovery.md#close-pressure-can-affect-peers).
    fn cancel(&self);

    /// Closes this logical interest with the same peer-pressure caveat as
    /// [`Self::cancel`]. Managed runtime processes are not stopped.
    fn close(self);
}

impl CliProductSubscription for ProductSubscription {
    fn recv(&self) -> Result<ProductSubscriptionItem, ProductClientError> {
        ProductSubscription::recv(self)
    }

    fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<ProductSubscriptionItem, ProductClientError> {
        ProductSubscription::recv_timeout(self, timeout)
    }

    fn cancel(&self) {
        ProductSubscription::cancel(self);
    }

    fn close(self) {
        ProductSubscription::close(self);
    }
}

/// Successful typed stream activation with an implementation-specific handle.
pub struct OpenedCliSubscription<S> {
    /// Server-declared replay and activation metadata.
    pub metadata: ProductStreamOpened,
    /// Logical subscription sharing its product client's physical connection.
    pub subscription: S,
}

/// Stream-opening boundary implemented by one already-connected product client.
pub trait CliProductStreamPort: CliProductRequestPort {
    /// Concrete persistent logical subscription handle.
    type Subscription: CliProductSubscription;

    /// Opens one typed logical stream on the existing physical connection.
    ///
    /// # Errors
    ///
    /// Returns the typed SDK boundary failure. Implementations must not log the
    /// stream specification or terminal payloads.
    fn open_product_subscription(
        &self,
        specification: ProductStreamSpec,
        timeout: Duration,
    ) -> Result<OpenedCliSubscription<Self::Subscription>, ProductClientError>;
}

impl CliProductStreamPort for ProductClient {
    type Subscription = ProductSubscription;

    fn open_product_subscription(
        &self,
        specification: ProductStreamSpec,
        timeout: Duration,
    ) -> Result<OpenedCliSubscription<Self::Subscription>, ProductClientError> {
        let OpenedProductSubscription {
            metadata,
            subscription,
        } = ProductClient::open_subscription(self, specification, timeout)?;
        Ok(OpenedCliSubscription {
            metadata,
            subscription,
        })
    }
}

/// Validated wait predicate using OR within fields and AND across fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitCondition {
    executions: Vec<ExecutionStateDto>,
    interactions: Vec<InteractionStateDto>,
    lifecycles: Vec<LifecycleKindDto>,
    health: Vec<HealthStatusDto>,
}

impl WaitCondition {
    /// Translates parsed targets into a non-empty predicate.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed input failure when every target collection is empty.
    pub fn from_arguments(arguments: &WaitArguments) -> Result<Self, CliInputError> {
        if arguments.execution.is_empty()
            && arguments.interaction.is_empty()
            && arguments.lifecycle.is_empty()
            && arguments.health.is_empty()
        {
            return Err(CliInputError::MissingWaitCondition);
        }
        Ok(Self {
            executions: arguments.execution.iter().copied().map(execution).collect(),
            interactions: arguments
                .interaction
                .iter()
                .copied()
                .map(interaction)
                .collect(),
            lifecycles: arguments.lifecycle.iter().copied().map(lifecycle).collect(),
            health: arguments.health.iter().copied().map(health).collect(),
        })
    }

    /// Returns whether one compact summary satisfies every selected dimension.
    ///
    /// This function performs no IO and does not panic.
    pub fn matches(&self, summary: &SessionSummaryDto) -> bool {
        (self.executions.is_empty()
            || self
                .executions
                .contains(&summary.foreground.execution.state))
            && (self.interactions.is_empty()
                || self
                    .interactions
                    .contains(&summary.foreground.interaction.state))
            && (self.lifecycles.is_empty()
                || self
                    .lifecycles
                    .contains(&lifecycle_kind(&summary.lifecycle)))
            && (self.health.is_empty() || self.health.contains(&summary.health.status))
    }
}

/// Selective CLI stream use cases sharing one physical product connection.
pub struct StreamCliExecutor<'a, P: CliProductStreamPort> {
    client: &'a P,
    timeout: Duration,
}

impl<'a, P: CliProductStreamPort> StreamCliExecutor<'a, P> {
    /// Binds all stream activation and finite target resolution to one client.
    ///
    /// This function performs no IO and does not panic.
    pub const fn new(client: &'a P, timeout: Duration) -> Self {
        Self { client, timeout }
    }

    /// Opens one bounded dashboard query stream.
    ///
    /// # Errors
    ///
    /// Returns local page-size, SDK, server, or activation-deadline failure.
    pub fn top(
        &self,
        arguments: &TopArguments,
    ) -> Result<OpenedCliSubscription<P::Subscription>, CliError> {
        let deadline = self.deadline(CliCommand::Top)?;
        let specification = prepare_top_stream(arguments)?;
        self.open(specification, deadline)
    }

    /// Opens one bounded query stream for exact named sessions or one first page.
    ///
    /// Every exact name is resolved before activation through the same client.
    /// Duplicate names collapse to one stable session identity. The accepted
    /// health and WorkContext flags currently do not change the query or
    /// presentation; summaries already carry those fields. No wildcard event
    /// stream is opened.
    ///
    /// # Errors
    ///
    /// Returns input, exact-name lookup, SDK, server, or activation-deadline failure.
    pub fn watch(
        &self,
        arguments: &WatchArguments,
    ) -> Result<OpenedCliSubscription<P::Subscription>, CliError> {
        let deadline = self.deadline(CliCommand::Watch)?;
        let mut session_ids = BTreeSet::new();
        for name in arguments.names.iter().collect::<BTreeSet<_>>() {
            session_ids.insert(self.resolve_name(name, deadline)?);
        }
        let specification = prepare_watch_stream(arguments, session_ids.into_iter().collect())?;
        self.open(specification, deadline)
    }

    /// Opens one selective terminal stream seeded by a screen or retained offset.
    ///
    /// # Errors
    ///
    /// Returns missing selection, exact-name lookup, SDK, server, or activation-deadline failure.
    pub fn attach(
        &self,
        arguments: &AttachArguments,
    ) -> Result<OpenedCliSubscription<P::Subscription>, CliError> {
        let initial = arguments
            .from_offset
            .map_or(TerminalInitialModeDto::Screen, |after_offset| {
                TerminalInitialModeDto::Resume { after_offset }
            });
        // The native interactive route resolves picker selections before opening streams.
        let name = arguments
            .name
            .as_deref()
            .ok_or(CliInputError::MissingSessionSelection)?;
        self.terminal(name, initial, CliCommand::Attach)
    }

    /// Opens one selective terminal stream strictly after a retained offset.
    ///
    /// This is used after a finite `logs` page so replay and live output do not
    /// overlap. The stream remains binary and is never represented as base64.
    ///
    /// # Errors
    ///
    /// Returns exact-name lookup, SDK, server, or activation-deadline failure.
    pub fn logs_follow(
        &self,
        name: &str,
        after_offset: u64,
    ) -> Result<OpenedCliSubscription<P::Subscription>, CliError> {
        self.terminal(
            name,
            TerminalInitialModeDto::Resume { after_offset },
            CliCommand::Logs,
        )
    }

    /// Opens live output for one already resolved stable session ID.
    ///
    /// This is used after a finite retained-page read so a concurrent rename
    /// cannot redirect the continuation to another session. The ID must have
    /// come from the same product connection. This method performs one logical
    /// stream activation and does not panic unless the injected port panics.
    ///
    /// # Errors
    ///
    /// Returns typed activation-deadline or product stream failure.
    pub fn logs_follow_session(
        &self,
        session_id: impl Into<String>,
        after_offset: u64,
    ) -> Result<OpenedCliSubscription<P::Subscription>, CliError> {
        let deadline = self.deadline(CliCommand::Logs)?;
        self.open(
            prepare_terminal_stream(
                session_id.into(),
                TerminalInitialModeDto::Resume { after_offset },
            ),
            deadline,
        )
    }

    /// Waits until one exact session satisfies a non-empty predicate.
    ///
    /// The initial snapshot, every delta, reconnect snapshot, and explicit gap
    /// resynchronization all use the same bounded deadline and physical client.
    ///
    /// # Errors
    ///
    /// Returns input, lookup, SDK, protocol, removal, or total wait-timeout failure.
    pub fn wait(&self, arguments: &WaitArguments) -> Result<SessionSummaryDto, CliError> {
        let condition = WaitCondition::from_arguments(arguments)?;
        let deadline = self.deadline(CliCommand::Wait)?;
        let session_id = self.resolve_name(&arguments.name, deadline)?;
        let opened = self.open(prepare_wait_stream(session_id.clone()), deadline)?;
        let subscription = opened.subscription;

        loop {
            let remaining = match deadline.remaining() {
                Ok(remaining) => remaining,
                Err(_) => {
                    subscription.close();
                    return Err(CliError::WaitTimedOut {
                        name: arguments.name.clone(),
                    });
                }
            };
            let item = match subscription.recv_timeout(remaining) {
                Ok(item) => item,
                Err(error) if error.category() == ProductClientErrorCategory::Timeout => {
                    subscription.close();
                    return Err(CliError::WaitTimedOut {
                        name: arguments.name.clone(),
                    });
                }
                Err(error) => {
                    subscription.close();
                    return Err(error.into());
                }
            };

            let observation = match item {
                ProductSubscriptionItem::Json { item, .. } => wait_observation(item, &session_id),
                ProductSubscriptionItem::Gap(_) => {
                    match self.read_exact_id(&session_id, deadline) {
                        Ok(Some(summary)) => WaitObservation::Summary(Box::new(summary)),
                        Ok(None) => WaitObservation::Removed,
                        Err(error) => {
                            subscription.close();
                            return Err(error);
                        }
                    }
                }
                ProductSubscriptionItem::Terminal { .. } => WaitObservation::None,
            };

            match observation {
                WaitObservation::Summary(summary) if condition.matches(&summary) => {
                    subscription.close();
                    return Ok(*summary);
                }
                WaitObservation::Removed => {
                    subscription.close();
                    return Err(CliError::SessionNotFound {
                        name: arguments.name.clone(),
                    });
                }
                WaitObservation::Summary(_) | WaitObservation::None => {}
            }
        }
    }

    fn terminal(
        &self,
        name: &str,
        initial: TerminalInitialModeDto,
        command: CliCommand,
    ) -> Result<OpenedCliSubscription<P::Subscription>, CliError> {
        let deadline = self.deadline(command)?;
        let session_id = self.resolve_name(name, deadline)?;
        self.open(prepare_terminal_stream(session_id, initial), deadline)
    }

    fn resolve_name(&self, name: &str, deadline: CliCommandDeadline) -> Result<String, CliError> {
        let page = self.list(exact_name_lookup(name), deadline)?;
        page.items
            .into_iter()
            .next()
            .map(|summary| summary.id)
            .ok_or_else(|| CliError::SessionNotFound {
                name: name.to_owned(),
            })
    }

    fn read_exact_id(
        &self,
        session_id: &str,
        deadline: CliCommandDeadline,
    ) -> Result<Option<SessionSummaryDto>, CliError> {
        Ok(self
            .list(exact_id_lookup(session_id), deadline)?
            .items
            .into_iter()
            .next())
    }

    fn list(
        &self,
        request: undercurrent_protocol::ListSessionsRequest,
        deadline: CliCommandDeadline,
    ) -> Result<SessionPageDto, CliError> {
        let success = self
            .client
            .request(ProductRequest::ListSessions(request), deadline.remaining()?)?;
        expect_list(success)
    }

    fn open(
        &self,
        specification: ProductStreamSpec,
        deadline: CliCommandDeadline,
    ) -> Result<OpenedCliSubscription<P::Subscription>, CliError> {
        self.client
            .open_product_subscription(specification, deadline.remaining()?)
            .map_err(Into::into)
    }

    fn deadline(&self, command: CliCommand) -> Result<CliCommandDeadline, CliError> {
        CliCommandDeadline::start(command, self.timeout)
    }
}

enum WaitObservation {
    Summary(Box<SessionSummaryDto>),
    Removed,
    None,
}

fn wait_observation(item: ProductStreamItem, session_id: &str) -> WaitObservation {
    match item {
        ProductStreamItem::SessionSnapshot(page) => page
            .items
            .into_iter()
            .find(|summary| summary.id == session_id)
            .map_or(WaitObservation::Removed, |summary| {
                WaitObservation::Summary(Box::new(summary))
            }),
        ProductStreamItem::SessionDeltaBatch(batch) => batch
            .deltas
            .into_iter()
            .rev()
            .find(|delta| delta.session_id == session_id)
            .map_or(WaitObservation::None, wait_delta),
        ProductStreamItem::SessionEvent(_) | ProductStreamItem::AgentEvent(_) => {
            WaitObservation::None
        }
    }
}

fn wait_delta(delta: SessionDeltaDto) -> WaitObservation {
    match (delta.kind, delta.summary) {
        (SessionDeltaKindDto::Removed, _) => WaitObservation::Removed,
        (_, Some(summary)) => WaitObservation::Summary(Box::new(summary)),
        (_, None) => WaitObservation::None,
    }
}

const fn lifecycle_kind(state: &LifecycleStateDto) -> LifecycleKindDto {
    match state {
        LifecycleStateDto::Starting => LifecycleKindDto::Starting,
        LifecycleStateDto::Running => LifecycleKindDto::Running,
        LifecycleStateDto::Stopping => LifecycleKindDto::Stopping,
        LifecycleStateDto::Stopped => LifecycleKindDto::Stopped,
        LifecycleStateDto::Exited { .. } => LifecycleKindDto::Exited,
        LifecycleStateDto::Failed => LifecycleKindDto::Failed,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use clap::Parser;
    use undercurrent_protocol::{
        HealthSummaryDto, ProductStreamKind, ProductSuccess, WorkSummaryDto,
    };

    use super::*;
    use crate::{NativeCliArguments, NativeCliCommand};

    #[derive(Clone, Default)]
    struct FakeSubscription {
        items: Arc<Mutex<VecDeque<Result<ProductSubscriptionItem, ProductClientError>>>>,
        closed: Arc<Mutex<bool>>,
    }

    impl CliProductSubscription for FakeSubscription {
        fn recv(&self) -> Result<ProductSubscriptionItem, ProductClientError> {
            self.recv_timeout(Duration::MAX)
        }

        fn recv_timeout(
            &self,
            _timeout: Duration,
        ) -> Result<ProductSubscriptionItem, ProductClientError> {
            self.items
                .lock()
                .expect("subscription items")
                .pop_front()
                .expect("recorded stream item")
        }

        fn cancel(&self) {
            *self.closed.lock().expect("closed") = true;
        }

        fn close(self) {
            *self.closed.lock().expect("closed") = true;
        }
    }

    #[derive(Default)]
    struct RecordingClient {
        requests: Mutex<Vec<ProductRequest>>,
        responses: Mutex<VecDeque<ProductSuccess>>,
        specifications: Mutex<Vec<ProductStreamSpec>>,
        subscription: FakeSubscription,
    }

    impl RecordingClient {
        fn with_responses(responses: impl IntoIterator<Item = ProductSuccess>) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().collect()),
                ..Self::default()
            }
        }
    }

    impl CliProductRequestPort for RecordingClient {
        fn request(
            &self,
            request: ProductRequest,
            _timeout: Duration,
        ) -> Result<ProductSuccess, ProductClientError> {
            self.requests.lock().expect("requests").push(request);
            Ok(self
                .responses
                .lock()
                .expect("responses")
                .pop_front()
                .expect("recorded response"))
        }
    }

    impl CliProductStreamPort for RecordingClient {
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

    #[test]
    fn attach_resolves_once_and_opens_one_screen_seed_on_the_same_client() {
        let client =
            RecordingClient::with_responses([ProductSuccess::ListSessions(page([summary(
                "session-1",
                "worker",
                ExecutionStateDto::Quiescent,
                InteractionStateDto::None,
            )]))]);
        let executor = StreamCliExecutor::new(&client, Duration::from_secs(1));
        let arguments = AttachArguments {
            name: Some("worker".to_owned()),
            from_offset: None,
            read_only: false,
            detach_keys: "ctrl-b,d".to_owned(),
        };

        let opened = executor.attach(&arguments).expect("attach opens");

        assert_eq!(opened.metadata.kind, ProductStreamKind::TerminalOutput);
        assert_eq!(client.requests.lock().expect("requests").len(), 1);
        assert!(matches!(
            &client.specifications.lock().expect("specifications")[0],
            ProductStreamSpec::TerminalOutput(specification)
                if specification.session_id == "session-1"
                    && specification.initial == TerminalInitialModeDto::Screen
        ));
    }

    #[test]
    fn named_watch_deduplicates_names_and_uses_one_bounded_id_query_stream() {
        let client = RecordingClient::with_responses([
            ProductSuccess::ListSessions(page([summary(
                "session-a",
                "worker-a",
                ExecutionStateDto::Quiescent,
                InteractionStateDto::None,
            )])),
            ProductSuccess::ListSessions(page([summary(
                "session-b",
                "worker-b",
                ExecutionStateDto::Active,
                InteractionStateDto::None,
            )])),
        ]);
        let executor = StreamCliExecutor::new(&client, Duration::from_secs(1));
        let parsed = NativeCliArguments::try_parse_from([
            "uc",
            "watch",
            "worker-a",
            "worker-a",
            "worker-b",
            "--page-size",
            "2",
        ])
        .expect("valid watch");
        let NativeCliCommand::Watch(arguments) = parsed.command else {
            panic!("expected watch")
        };

        executor.watch(&arguments).expect("watch opens");

        assert_eq!(client.requests.lock().expect("requests").len(), 2);
        let ProductStreamSpec::SessionWatch(specification) =
            &client.specifications.lock().expect("specifications")[0]
        else {
            panic!("expected session watch")
        };
        assert_eq!(specification.query.filter.ids, ["session-a", "session-b"]);
        assert_eq!(specification.query.page_size, 2);
    }

    #[test]
    fn logs_follow_resumes_after_the_finite_page_without_replaying_it() {
        let client =
            RecordingClient::with_responses([ProductSuccess::ListSessions(page([summary(
                "session-1",
                "worker",
                ExecutionStateDto::Quiescent,
                InteractionStateDto::None,
            )]))]);
        let executor = StreamCliExecutor::new(&client, Duration::from_secs(1));

        executor
            .logs_follow("worker", 4_096)
            .expect("follow stream opens");

        assert!(matches!(
            &client.specifications.lock().expect("specifications")[0],
            ProductStreamSpec::TerminalOutput(specification)
                if specification.session_id == "session-1"
                    && specification.initial
                        == TerminalInitialModeDto::Resume { after_offset: 4_096 }
        ));
    }

    #[test]
    fn wait_returns_from_initial_snapshot_and_closes_only_its_logical_stream() {
        let client =
            RecordingClient::with_responses([ProductSuccess::ListSessions(page([summary(
                "session-1",
                "worker",
                ExecutionStateDto::Quiescent,
                InteractionStateDto::None,
            )]))]);
        client
            .subscription
            .items
            .lock()
            .expect("items")
            .push_back(Ok(ProductSubscriptionItem::Json {
                sequence: 1,
                item: ProductStreamItem::SessionSnapshot(page([summary(
                    "session-1",
                    "worker",
                    ExecutionStateDto::Active,
                    InteractionStateDto::None,
                )])),
            }));
        let executor = StreamCliExecutor::new(&client, Duration::from_secs(1));
        let arguments = WaitArguments {
            name: "worker".to_owned(),
            execution: vec![crate::ExecutionArgument::Active],
            interaction: Vec::new(),
            lifecycle: vec![crate::LifecycleArgument::Running],
            health: Vec::new(),
        };

        let reached = executor.wait(&arguments).expect("condition reached");

        assert_eq!(
            reached.foreground.execution.state,
            ExecutionStateDto::Active
        );
        assert!(*client.subscription.closed.lock().expect("closed"));
        assert_eq!(
            client.specifications.lock().expect("specifications").len(),
            1
        );
    }

    #[test]
    fn wait_predicate_is_or_within_dimensions_and_and_across_them() {
        let condition = WaitCondition::from_arguments(&WaitArguments {
            name: "worker".to_owned(),
            execution: vec![
                crate::ExecutionArgument::Active,
                crate::ExecutionArgument::Quiescent,
            ],
            interaction: vec![crate::InteractionArgument::Requested],
            lifecycle: vec![crate::LifecycleArgument::Running],
            health: vec![crate::HealthArgument::Healthy],
        })
        .expect("condition");
        let mut current = summary(
            "session-1",
            "worker",
            ExecutionStateDto::Quiescent,
            InteractionStateDto::Requested,
        );
        current.health.status = HealthStatusDto::Healthy;
        assert!(condition.matches(&current));
        current.lifecycle = LifecycleStateDto::Stopped;
        assert!(!condition.matches(&current));
    }

    #[test]
    fn empty_wait_predicate_fails_before_any_client_operation() {
        let client = RecordingClient::default();
        let executor = StreamCliExecutor::new(&client, Duration::from_secs(1));
        let error = executor
            .wait(&WaitArguments {
                name: "worker".to_owned(),
                execution: Vec::new(),
                interaction: Vec::new(),
                lifecycle: Vec::new(),
                health: Vec::new(),
            })
            .expect_err("empty predicate must fail");

        assert!(matches!(
            error,
            CliError::Input(CliInputError::MissingWaitCondition)
        ));
        assert!(client.requests.lock().expect("requests").is_empty());
        assert!(
            client
                .specifications
                .lock()
                .expect("specifications")
                .is_empty()
        );
    }

    fn page<const N: usize>(items: [SessionSummaryDto; N]) -> SessionPageDto {
        SessionPageDto {
            items: items.into(),
            next_cursor: None,
            total: None,
            snapshot_revision: 1,
        }
    }

    fn summary(
        id: &str,
        name: &str,
        execution: ExecutionStateDto,
        interaction: InteractionStateDto,
    ) -> SessionSummaryDto {
        SessionSummaryDto {
            id: id.to_owned(),
            name: name.to_owned(),
            lifecycle: LifecycleStateDto::Running,
            foreground: crate::test_foreground(execution, interaction),
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
            restart_status: undercurrent_protocol::RestartStatusDto::Idle,
            revision: 1,
            lifecycle_revision: 1,
        }
    }
}
