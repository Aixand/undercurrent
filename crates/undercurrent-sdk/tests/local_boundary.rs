// SPDX-License-Identifier: Apache-2.0
#![cfg(any(target_os = "linux", target_os = "macos", windows))]
use std::{collections::BTreeSet, error::Error, path::PathBuf, sync::Arc, time::Duration};
use undercurrent_multiplex::{
    DeliverySemantics, ReplayLimits, SchedulerMessageLimits, StreamKind, StreamRecoveryAction,
    TrafficClass,
};
use undercurrent_protocol::AuthenticatedRequestContext;
use undercurrent_sdk::{
    Client, ClientConnectionHello, ClientConnectionLimits, ClientConnector, ClientProtocolVersion,
    ClientRecoveryCancellation, ClientRuntimeLimits, LocalClientConfig, LocalClientConnector,
    LocalClientLimits,
};
use undercurrent_transport_local::{
    AuthenticatedActionDispatcher, LocalEndpoint, LocalServer, LocalServerLimits, ServerAction,
    ServerConnectionSession, ServerDiagnosticLimits, ServerDispatchError,
    ServerStreamConfiguration,
};
use undercurrent_wire::{
    CURRENT_PROTOCOL_VERSION, ConnectionLimits, ConnectionPolicy, HeartbeatPolicy, NamespacedName,
    ServerEpoch, connection_capabilities,
};
struct EchoDispatcher;
impl AuthenticatedActionDispatcher for EchoDispatcher {
    fn dispatch_authenticated(
        &self,
        authentication: Arc<AuthenticatedRequestContext>,
        connection: Arc<ServerConnectionSession>,
        action: ServerAction,
    ) -> Result<(), ServerDispatchError> {
        assert_eq!(
            authentication.principal().as_str(),
            "local-undercurrent-user"
        );
        if let Some(tenant) = authentication.tenant() {
            assert_eq!(tenant.as_str(), "tenant-a");
            assert_eq!(authentication.authority().as_str(), "undercurrent.remote");
        }
        match action {
            ServerAction::Request {
                request_id,
                payload,
                ..
            } => connection
                .complete_request(&request_id, payload)
                .expect("echo finite response"),
            ServerAction::OpenStream {
                request_id,
                stream_id,
                payload,
                ..
            }
            | ServerAction::ResumeStream {
                request_id,
                stream_id,
                payload,
                ..
            } => {
                connection
                    .activate_stream(
                        &request_id,
                        &stream_id,
                        ServerStreamConfiguration {
                            kind: StreamKind::TerminalOutput,
                            class: TrafficClass::Bulk,
                            delivery: DeliverySemantics::Droppable,
                            recovery: StreamRecoveryAction::RestartFromAvailableOffset,
                            retain_for_resume: true,
                        },
                        payload,
                    )
                    .expect("activate logical stream");
            }
            ServerAction::CancelRequest { .. }
            | ServerAction::CloseStream { .. }
            | ServerAction::CloseConnection => {}
        }
        Ok(())
    }
}

#[derive(Default)]
struct SessionLifetimeDispatcher(
    Mutex<Option<std::sync::Weak<ServerConnectionSession>>>,
    AtomicUsize,
);

impl AuthenticatedActionDispatcher for SessionLifetimeDispatcher {
    fn dispatch_authenticated(
        &self,
        authentication: Arc<AuthenticatedRequestContext>,
        connection: Arc<ServerConnectionSession>,
        action: ServerAction,
    ) -> Result<(), ServerDispatchError> {
        *self.0.lock().expect("observed session") = Some(Arc::downgrade(&connection));
        if matches!(action, ServerAction::CloseStream { .. }) {
            self.1.fetch_add(1, Ordering::Release);
        }
        EchoDispatcher.dispatch_authenticated(authentication, connection, action)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_writer_recycle_releases_session_and_registration_before_client_reuse() {
    let directory = physical_tempdir().expect("temporary endpoint");
    let endpoint = LocalEndpoint::new(directory.path().join("run/control.sock")).expect("endpoint");
    let (hello, policy) = connection_contract().expect("connection contract");
    let limits = server_limits(&policy).expect("server limits");
    let dispatcher = Arc::new(SessionLifetimeDispatcher::default());
    let server = Arc::new(
        LocalServer::bind(
            endpoint,
            ServerEpoch::new("writer-cleanup-epoch").expect("epoch"),
            policy,
            limits,
            dispatcher.clone(),
        )
        .expect("bind server"),
    );
    let running = server.clone();
    let worker = tokio::spawn(async move { running.run().await });
    assert!(server.wait_until_running(Duration::from_secs(2)).await);

    // Reuse the same logical client ID after each recycle. Registry counters
    // alone cannot establish writer retirement, so also observe the session's
    // last strong owner disappearing after the writer join.
    for _ in 0..2 {
        let path = server.endpoint().path().to_path_buf();
        let hello = hello.clone();
        let client = tokio::task::spawn_blocking(move || connect_client(path, hello))
            .await
            .expect("connector task")
            .expect("connect logical client");
        assert_eq!(
            client
                .request(Arc::from(&b"idle-next"[..]), Duration::from_secs(2))
                .expect("round trip"),
            Arc::<[u8]>::from(&b"idle-next"[..]),
        );
        let session = dispatcher
            .0
            .lock()
            .expect("observed session")
            .clone()
            .expect("session observed");
        server.recycle_connections();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let pressure = server.pressure();
                if pressure.connections.clients == 0
                    && pressure.connections.connections == 0
                    && pressure.connection_workers == 0
                    && session.strong_count() == 0
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("writer joins and connection ownership is released");
        tokio::task::spawn_blocking(move || drop(client))
            .await
            .expect("drop SDK client");
    }

    server.shutdown();
    let report = tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .expect("server shutdown completes")
        .expect("server task")
        .expect("server report");
    assert_eq!(report.accepted, 2);
    assert_eq!(report.rejected, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborting_local_server_releases_idle_writer_session() {
    let directory = physical_tempdir().expect("temporary endpoint");
    let endpoint = LocalEndpoint::new(directory.path().join("run/control.sock")).expect("endpoint");
    let (hello, policy) = connection_contract().expect("connection contract");
    let limits = server_limits(&policy).expect("server limits");
    let dispatcher = Arc::new(SessionLifetimeDispatcher::default());
    let server = Arc::new(
        LocalServer::bind(
            endpoint,
            ServerEpoch::new("abort-cleanup-epoch").expect("epoch"),
            policy,
            limits,
            dispatcher.clone(),
        )
        .expect("bind server"),
    );
    let running = server.clone();
    let worker = tokio::spawn(async move { running.run().await });
    assert!(server.wait_until_running(Duration::from_secs(2)).await);
    let path = server.endpoint().path().to_path_buf();
    let client = tokio::task::spawn_blocking(move || connect_client(path, hello))
        .await
        .expect("connector task")
        .expect("connect client");
    client
        .request(Arc::from(&b"idle-next"[..]), Duration::from_secs(2))
        .expect("round trip");
    let session = dispatcher
        .0
        .lock()
        .expect("observed session")
        .clone()
        .expect("session observed");
    worker.abort();
    assert!(worker.await.expect_err("run aborted").is_cancelled());
    let retired = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let pressure = server.pressure();
            if session.strong_count() == 0
                && pressure.connections.connections == 0
                && pressure.connection_workers == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    if !retired {
        // Rescue the original implementation so the failing regression does not
        // leave a native writer alive in the test process.
        if let Some(session) = session.upgrade() {
            session.close();
        }
    }
    tokio::task::spawn_blocking(move || drop(client))
        .await
        .expect("drop client");
    assert!(
        retired,
        "aborting run must release the idle writer and its session"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticated_sdk_preserves_binary_and_three_hundred_streams_on_one_socket() {
    let (_directory, server, hello) = server().expect("bind local server");
    let endpoint = server.endpoint().path().to_path_buf();
    let running = Arc::clone(&server);
    let worker = tokio::spawn(async move { running.run().await });
    assert!(server.wait_until_running(Duration::from_secs(2)).await);

    let client = tokio::task::spawn_blocking(move || connect_client(endpoint, hello))
        .await
        .expect("client connector task")
        .expect("authenticated SDK connection");
    let exact = Arc::<[u8]>::from(&b"\0pty\xffbytes\n"[..]);
    let response = client
        .request(exact.clone(), Duration::from_secs(2))
        .expect("binary request response");
    assert_eq!(response, exact);

    let mut subscriptions = Vec::with_capacity(300);
    for index in 0..300_u16 {
        let payload = Arc::<[u8]>::from(index.to_be_bytes());
        let opened = client
            .open_subscription(payload.clone(), Duration::from_secs(2))
            .expect("logical stream open");
        assert_eq!(opened.response, payload);
        subscriptions.push(opened.subscription);
    }
    assert_eq!(subscriptions.len(), 300);

    drop(subscriptions);
    drop(client);
    server.shutdown();
    let report = worker
        .await
        .expect("server task")
        .expect("server shutdown report");
    assert_eq!(report.accepted, 1);
    assert_eq!(report.rejected, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owner_backed_server_write_preserves_large_binary_over_real_local_socket() {
    let (_directory, server, hello) = server().expect("bind local server");
    let endpoint = server.endpoint().path().to_path_buf();
    let running = Arc::clone(&server);
    let worker = tokio::spawn(async move { running.run().await });
    assert!(server.wait_until_running(Duration::from_secs(2)).await);

    let client = tokio::task::spawn_blocking(move || connect_client(endpoint, hello))
        .await
        .expect("client connector task")
        .expect("authenticated SDK connection");
    let exact = Arc::<[u8]>::from(
        (0..24 * 1_024)
            .map(|index| u8::try_from(index % 256).expect("bounded byte"))
            .collect::<Vec<_>>(),
    );
    let response = client
        .request(exact.clone(), Duration::from_secs(2))
        .expect("large binary request response");
    assert_eq!(response, exact);

    drop(client);
    server.shutdown();
    let report = worker
        .await
        .expect("server task")
        .expect("server shutdown report");
    assert_eq!(report.accepted, 1);
    assert_eq!(report.rejected, 0);
    assert_eq!(report.failed, 0);
}

fn server() -> Result<(tempfile::TempDir, Arc<LocalServer>, ClientConnectionHello), Box<dyn Error>>
{
    let directory = physical_tempdir()?;
    let endpoint = LocalEndpoint::new(directory.path().join("run/control.sock"))?;
    let (hello, policy) = connection_contract()?;
    let limits = server_limits(&policy)?;
    let server = LocalServer::bind(
        endpoint,
        ServerEpoch::new("local-test-epoch")?,
        policy,
        limits,
        Arc::new(EchoDispatcher),
    )?;
    Ok((directory, Arc::new(server), hello))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_local_inbound_queue_backpressures_without_resetting_peer_requests() {
    use undercurrent_sdk::{
        ClientTransport, ClientTransportError, ClientTransportInbound, ClientTransportOutbound,
    };
    struct SlowReader(Arc<dyn ClientTransport>);
    impl ClientTransport for SlowReader {
        fn send(&self, value: ClientTransportOutbound) -> Result<(), ClientTransportError> {
            self.0.send(value)
        }
        fn receive(&self) -> Result<ClientTransportInbound, ClientTransportError> {
            std::thread::sleep(Duration::from_millis(10));
            self.0.receive()
        }
        fn close(&self) {
            self.0.close();
        }
    }
    let (_directory, server, hello) = server().unwrap();
    let running = server.clone();
    let worker = tokio::spawn(async move { running.run().await });
    assert!(server.wait_until_running(Duration::from_secs(2)).await);
    let endpoint = server.endpoint().path().to_path_buf();
    let client = tokio::task::spawn_blocking(move || {
        let config = LocalClientConfig::new(
            endpoint,
            hello,
            LocalClientLimits::new(
                16_384,
                Duration::from_secs(2),
                Duration::from_secs(2),
                65_536,
                64,
                1,
            )
            .unwrap(),
        )
        .unwrap();
        let negotiated = LocalClientConnector::new(config)
            .connect(&ClientRecoveryCancellation::default())
            .unwrap();
        let (agreement, transport) = negotiated.into_parts();
        Client::connect(
            agreement,
            Arc::new(SlowReader(transport)),
            ClientRuntimeLimits::new(32_768, 64, 8, 65_536).unwrap(),
        )
        .unwrap()
    })
    .await
    .unwrap();
    let requests = (0..24)
        .map(|index| {
            let client = client.clone();
            tokio::task::spawn_blocking(move || {
                let payload: Arc<[u8]> = Arc::from(format!("burst-{index}").into_bytes());
                assert_eq!(
                    client
                        .request(payload.clone(), Duration::from_secs(3))
                        .expect("burst response survives inbound backpressure"),
                    payload
                );
            })
        })
        .collect::<Vec<_>>();
    for request in requests {
        request.await.unwrap();
    }
    assert!(!client.is_closed());
    client.close();
    server.shutdown();
    worker.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_overload_closes_exactly_one_remote_interest_and_keeps_the_local_connection() {
    let directory = physical_tempdir().unwrap();
    let endpoint = LocalEndpoint::new(directory.path().join("run/control.sock")).unwrap();
    let (hello, policy) = connection_contract().unwrap();
    let dispatcher = Arc::new(SessionLifetimeDispatcher::default());
    let server = Arc::new(
        LocalServer::bind(
            endpoint,
            ServerEpoch::new("overload-epoch").unwrap(),
            policy.clone(),
            server_limits(&policy).unwrap(),
            dispatcher.clone(),
        )
        .unwrap(),
    );
    let running = server.clone();
    let worker = tokio::spawn(async move { running.run().await });
    assert!(server.wait_until_running(Duration::from_secs(2)).await);
    let path = server.endpoint().path().to_path_buf();
    let client = tokio::task::spawn_blocking(move || connect_client(path, hello))
        .await
        .unwrap()
        .unwrap();
    let opened = client
        .open_subscription(Arc::from(&b"terminal"[..]), Duration::from_secs(2))
        .unwrap();
    let stream =
        undercurrent_wire::StreamId::new(opened.subscription.stream_id().as_str()).unwrap();
    let connection = dispatcher
        .0
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .upgrade()
        .unwrap();
    for sequence in 1..=32 {
        if connection
            .publish_stream(
                &stream,
                undercurrent_wire::StreamSequence::new(sequence),
                Arc::from(&b"x"[..]),
            )
            .is_err()
        {
            break;
        }
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        // Telemetry can be dropped under contention. Observe remote teardown,
        // which follows local mailbox closure, without consuming queued data.
        while dispatcher.1.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("remote stream teardown is observed before consuming");
    let error = opened
        .subscription
        .recv_timeout(Duration::from_secs(2))
        .expect_err("slow stream must terminate");
    assert_eq!(
        error.overload().unwrap().resource(),
        "client_stream_mailbox"
    );
    drop(opened);
    // A second CloseStream or late WindowUpdate would be a server-side unknown
    // route error. Multiple real round trips prove teardown remains isolated.
    for _ in 0..3 {
        assert_eq!(
            client
                .request(Arc::from(&b"peer"[..]), Duration::from_secs(2))
                .unwrap(),
            Arc::<[u8]>::from(&b"peer"[..])
        );
    }
    assert!(!client.is_closed());
    assert_eq!(dispatcher.1.load(Ordering::Acquire), 1);
    client.close();
    drop(connection);
    server.shutdown();
    worker.await.unwrap().unwrap();
}
fn server_limits(policy: &ConnectionPolicy) -> Result<LocalServerLimits, Box<dyn Error>> {
    Ok(LocalServerLimits::new(
        16_384,
        Duration::from_secs(2),
        Duration::from_secs(2),
        65_536,
        8,
        32,
        4_096,
        SchedulerMessageLimits::new(4_096, 16, [512, 512, 512, 512])?,
        ReplayLimits::new(400, 1_048_576, 134_217_728, 65_536)?,
        ServerDiagnosticLimits::new(512, 512)?,
        policy,
    )?)
}

fn connect_client(
    endpoint: PathBuf,
    hello: ClientConnectionHello,
) -> Result<Client, Box<dyn Error + Send + Sync>> {
    let connector = connector(endpoint, hello)?;
    let negotiated = connector.connect(&ClientRecoveryCancellation::default())?;
    let (agreement, transport) = negotiated.into_parts();
    Ok(Client::connect(
        agreement,
        transport,
        ClientRuntimeLimits::new(32_768, 512, 8, 65_536)?,
    )?)
}

fn connector(
    endpoint: PathBuf,
    hello: ClientConnectionHello,
) -> Result<Arc<LocalClientConnector>, Box<dyn Error + Send + Sync>> {
    let config = LocalClientConfig::new(
        endpoint,
        hello,
        LocalClientLimits::new(
            16_384,
            Duration::from_secs(2),
            Duration::from_secs(2),
            65_536,
            512,
            512,
        )?,
    )?;
    Ok(Arc::new(LocalClientConnector::new(config)))
}

fn connection_contract() -> Result<(ClientConnectionHello, ConnectionPolicy), Box<dyn Error>> {
    connection_contract_with_pending(64)
}

fn connection_contract_with_pending(
    max_pending_requests: u32,
) -> Result<(ClientConnectionHello, ConnectionPolicy), Box<dyn Error>> {
    let capability_names = [
        connection_capabilities::MULTIPLEX.to_owned(),
        connection_capabilities::FLOW_CONTROL.to_owned(),
        connection_capabilities::STREAM_RESUME.to_owned(),
        connection_capabilities::BINARY_PAYLOADS.to_owned(),
        connection_capabilities::TRACE_CONTEXT.to_owned(),
    ];
    let capabilities = BTreeSet::from([
        capability(connection_capabilities::MULTIPLEX)?,
        capability(connection_capabilities::FLOW_CONTROL)?,
        capability(connection_capabilities::STREAM_RESUME)?,
        capability(connection_capabilities::BINARY_PAYLOADS)?,
        capability(connection_capabilities::TRACE_CONTEXT)?,
    ]);
    let limits =
        ConnectionLimits::new(32_768, 400, max_pending_requests, 4_194_304, 65_536, 32_768)?;
    let hello = ClientConnectionHello::new(
        "websocket-test-client",
        ClientProtocolVersion::CURRENT,
        capability_names,
        std::iter::empty(),
        ClientConnectionLimits::new(32_768, 400, max_pending_requests, 4_194_304, 65_536, 32_768)?,
    )?;
    let policy = ConnectionPolicy::new(
        CURRENT_PROTOCOL_VERSION,
        capabilities,
        limits,
        HeartbeatPolicy::new(60_000, 120_000)?,
        Some(60_000),
    )?;
    Ok((hello, policy))
}

fn capability(value: &str) -> Result<NamespacedName, Box<dyn Error>> {
    Ok(NamespacedName::new(value)?)
}

use std::sync::{
    Condvar, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use undercurrent_sdk::{
    ClientRecoveryJitter, ClientRecoveryPolicy, ClientStreamGapReason, ClientStreamItem,
    ClientStreamRecovery, LocalTransportError, LocalTransportStage, ResilientClient,
};
use undercurrent_wire::{ClientMessage, RequestId};
struct CountingDispatcher(Arc<AtomicUsize>);

#[derive(Clone)]
struct GatedDispatcher {
    started: Arc<AtomicBool>,
    released: Arc<(Mutex<bool>, Condvar)>,
    completed: Arc<AtomicUsize>,
}

impl GatedDispatcher {
    fn new() -> Self {
        Self {
            started: Arc::new(AtomicBool::new(false)),
            released: Arc::new((Mutex::new(false), Condvar::new())),
            completed: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn release(&self) {
        *self
            .released
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        self.released.1.notify_all();
    }
}

struct SaturatedSessionDispatcher(AtomicBool);

impl AuthenticatedActionDispatcher for CountingDispatcher {
    fn dispatch_authenticated(
        &self,
        _authentication: Arc<AuthenticatedRequestContext>,
        _connection: Arc<ServerConnectionSession>,
        _action: ServerAction,
    ) -> Result<(), ServerDispatchError> {
        self.0.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

impl AuthenticatedActionDispatcher for GatedDispatcher {
    fn dispatch_authenticated(
        &self,
        _authentication: Arc<AuthenticatedRequestContext>,
        connection: Arc<ServerConnectionSession>,
        action: ServerAction,
    ) -> Result<(), ServerDispatchError> {
        if let ServerAction::Request {
            request_id,
            payload,
            ..
        } = action
        {
            self.started.store(true, Ordering::Release);
            let released = self.released.clone();
            let completed = self.completed.clone();
            std::thread::spawn(move || {
                let guard = released
                    .0
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let (guard, _) = released
                    .1
                    .wait_timeout_while(guard, Duration::from_secs(5), |released| !*released)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if *guard && connection.complete_request(&request_id, payload).is_ok() {
                    completed.fetch_add(1, Ordering::AcqRel);
                }
            });
        }
        Ok(())
    }
}

impl AuthenticatedActionDispatcher for SaturatedSessionDispatcher {
    fn dispatch_authenticated(
        &self,
        _authentication: Arc<AuthenticatedRequestContext>,
        connection: Arc<ServerConnectionSession>,
        action: ServerAction,
    ) -> Result<(), ServerDispatchError> {
        if let ServerAction::Request {
            request_id,
            trace_context,
            payload,
        } = action
        {
            connection
                .complete_request(&request_id, payload.clone())
                .expect("fixture completes the caller-visible request");
            if !self.0.swap(true, Ordering::AcqRel) {
                connection
                    .accept(ClientMessage::Request {
                        request_id: RequestId::new("server-fixture-held-request")
                            .expect("fixture request id"),
                        trace_context,
                        payload,
                    })
                    .expect("fixture reserves one server-only pending slot");
            }
        }
        Ok(())
    }
}

struct ZeroJitter;

impl ClientRecoveryJitter for ZeroJitter {
    fn sample_basis_points(&self, _failed_attempt: u32, _maximum: u16) -> u16 {
        0
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_admission_capacity_reaches_sdk_as_typed_retryable_overload() {
    let (hello, policy) =
        connection_contract_with_pending(1).expect("capacity connection contract");
    let limits = server_limits(&policy).expect("capacity server limits");
    let directory = physical_tempdir().unwrap();
    let server = Arc::new(
        LocalServer::bind(
            LocalEndpoint::new(directory.path().join("run/control.sock")).unwrap(),
            ServerEpoch::new("capacity-loopback-epoch").expect("server epoch"),
            policy,
            limits,
            Arc::new(SaturatedSessionDispatcher(AtomicBool::new(false))),
        )
        .expect("bind capacity Local server"),
    );
    let endpoint = server.endpoint().path().to_path_buf();
    let running = server.clone();
    let worker = tokio::spawn(async move { running.run().await });
    assert!(server.wait_until_running(Duration::from_secs(2)).await);

    let client = tokio::task::spawn_blocking(move || connect_client(endpoint, hello))
        .await
        .expect("client connector task")
        .expect("authenticated SDK connection");
    assert_eq!(
        client
            .request(Arc::from(&b"prime"[..]), Duration::from_secs(2))
            .expect("first request primes server-only capacity")
            .as_ref(),
        b"prime"
    );

    let error = match client.request(
        Arc::from(&b"server-over-capacity"[..]),
        Duration::from_secs(2),
    ) {
        Err(error) => error,
        Ok(_) => panic!("second request must hit server-session admission capacity"),
    };
    assert_eq!(
        error.category(),
        undercurrent_sdk::ClientErrorCategory::Overloaded
    );
    let overload = error
        .overload()
        .expect("caller-visible SDK error retains server overload context");
    assert_eq!(overload.resource(), "server_connection_capacity");
    assert_eq!(overload.requested(), Some(1));
    assert_eq!(
        overload.retry(),
        undercurrent_sdk::ClientRetryGuidance::WithBackoff
    );

    drop(client);
    server.shutdown();
    let report = worker
        .await
        .expect("server task")
        .expect("server shutdown report");
    assert_eq!(report.accepted, 1);
    assert_eq!(report.rejected, 0);
    assert_eq!(report.failed, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_cancelled_local_setup_does_not_dial_authenticate_spawn_or_register() {
    let (hello, policy) = connection_contract().expect("connection contract");
    let limits = server_limits(&policy).expect("server limits");
    let dispatch_calls = Arc::new(AtomicUsize::new(0));
    let directory = physical_tempdir().unwrap();
    let server = Arc::new(
        LocalServer::bind(
            LocalEndpoint::new(directory.path().join("run/control.sock")).unwrap(),
            ServerEpoch::new("pre-cancelled-epoch").expect("server epoch"),
            policy,
            limits,
            Arc::new(CountingDispatcher(dispatch_calls.clone())),
        )
        .expect("bind pre-cancellation server"),
    );
    let endpoint = server.endpoint().path().to_path_buf();
    let running = server.clone();
    let worker = tokio::spawn(async move { running.run().await });
    assert!(server.wait_until_running(Duration::from_secs(2)).await);

    let before = server.pressure();
    let before_registry = server
        .connection_registry()
        .diagnostic_page(None, 8)
        .expect("initial connection registry");
    let connector = connector(endpoint, hello).expect("Local connector");
    let cancellation = ClientRecoveryCancellation::default();
    cancellation.cancel();
    let error = match connector.connect(&cancellation) {
        Ok(_) => panic!("pre-cancelled connector must not start setup"),
        Err(error) => error,
    };
    let transport = error
        .source()
        .and_then(|source| source.downcast_ref::<LocalTransportError>())
        .expect("typed Local cancellation source");
    assert_eq!(transport.stage(), LocalTransportStage::Cancelled);

    let after = server.pressure();
    let after_registry = server
        .connection_registry()
        .diagnostic_page(None, 8)
        .expect("unchanged connection registry");
    assert_eq!(after, before);
    assert_eq!(after.connection_workers, 0);
    assert_eq!(after.connections.clients, 0);
    assert_eq!(after.connections.connections, 0);
    assert_eq!(after_registry.registry, before_registry.registry);
    assert!(after_registry.items.is_empty());
    assert_eq!(dispatch_calls.load(Ordering::Acquire), 0);

    server.shutdown();
    let report = worker
        .await
        .expect("pre-cancellation server task")
        .expect("pre-cancellation server shutdown");
    assert_eq!(report.accepted, 0);
    assert_eq!(report.rejected, 0);
    assert_eq!(report.failed, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_accepting_refuses_new_admission_while_established_request_drains() {
    let (hello, policy) = connection_contract().expect("connection contract");
    let limits = server_limits(&policy).expect("server limits");
    let dispatcher = Arc::new(GatedDispatcher::new());
    let directory = physical_tempdir().unwrap();
    let server = Arc::new(
        LocalServer::bind(
            LocalEndpoint::new(directory.path().join("run/control.sock")).unwrap(),
            ServerEpoch::new("draining-admission-epoch").expect("server epoch"),
            policy,
            limits,
            dispatcher.clone(),
        )
        .expect("bind draining-admission server"),
    );
    let endpoint = server.endpoint().path().to_path_buf();
    let running = server.clone();
    let worker = tokio::spawn(async move { running.run().await });
    assert!(server.wait_until_running(Duration::from_secs(2)).await);

    let established_endpoint = endpoint.clone();
    let client = Arc::new(
        tokio::task::spawn_blocking(move || connect_client(established_endpoint, hello))
            .await
            .expect("established client connector task")
            .expect("established client"),
    );
    let request_client = client.clone();
    let request = tokio::task::spawn_blocking(move || {
        request_client.request(Arc::from(&b"drain-established"[..]), Duration::from_secs(2))
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !dispatcher.started.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("established request reaches gated dispatcher");
    assert_eq!(dispatcher.completed.load(Ordering::Acquire), 0);

    server.stop_accepting();
    assert!(!server.pressure().accepting);
    tokio::time::sleep(Duration::from_millis(20)).await;
    let before_rejected_dial = server.pressure();
    let before_registry = server
        .connection_registry()
        .diagnostic_page(None, 8)
        .expect("established connection registry");
    assert_eq!(before_rejected_dial.connection_workers, 1);
    assert_eq!(before_rejected_dial.connections.clients, 1);
    assert_eq!(before_registry.items.len(), 1);

    let rejected_config = LocalClientConfig::new(
        endpoint,
        client_hello("stopped-admission-client").expect("rejected client hello"),
        LocalClientLimits::new(
            16_384,
            Duration::from_millis(100),
            Duration::from_millis(100),
            65_536,
            32,
            32,
        )
        .expect("rejected client limits"),
    )
    .expect("rejected client configuration");
    let rejected = tokio::task::spawn_blocking(move || {
        LocalClientConnector::new(rejected_config).connect(&ClientRecoveryCancellation::default())
    })
    .await
    .expect("rejected client connector task");
    let rejected = match rejected {
        Ok(_) => panic!("stopped listener must not admit a new Local"),
        Err(error) => error,
    };
    let rejected_transport = rejected
        .source()
        .and_then(|source| source.downcast_ref::<LocalTransportError>())
        .expect("typed stopped-admission error");
    assert_eq!(rejected_transport.stage(), LocalTransportStage::Connect);

    let after_rejected_dial = server.pressure();
    let after_registry = server
        .connection_registry()
        .diagnostic_page(None, 8)
        .expect("unchanged established registry");
    assert_eq!(after_rejected_dial, before_rejected_dial);
    assert_eq!(after_registry.registry, before_registry.registry);
    assert_eq!(after_registry.items.len(), 1);
    assert_eq!(dispatcher.completed.load(Ordering::Acquire), 0);

    dispatcher.release();
    let response = request
        .await
        .expect("established request task")
        .expect("established request drains successfully");
    assert_eq!(response.as_ref(), b"drain-established");
    assert_eq!(dispatcher.completed.load(Ordering::Acquire), 1);
    client.close();
    let report = tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .expect("draining server completion deadline")
        .expect("draining server task")
        .expect("draining server report");
    assert_eq!(report.accepted, 1);
    assert_eq!(report.completed, 1);
    assert_eq!(report.rejected, 0);
    assert_eq!(report.failed, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resilient_sdk_reconnects_and_resumes_after_physical_local_recycle() {
    let (_directory, server, hello) = server().expect("bind Local server");
    let endpoint = server.endpoint().path().to_path_buf();
    let running = Arc::clone(&server);
    let worker = tokio::spawn(async move { running.run().await });
    assert!(server.wait_until_running(Duration::from_secs(2)).await);
    let connector = connector(endpoint, hello).expect("Local connector");
    let client = tokio::task::spawn_blocking(move || {
        ResilientClient::connect(
            connector,
            ClientRuntimeLimits::new(32_768, 512, 8, 65_536).expect("client limits"),
            ClientRecoveryPolicy::new(
                Duration::from_millis(5),
                Duration::from_millis(50),
                10_000,
                1,
                20,
                400,
            )
            .expect("recovery policy"),
            Arc::new(ZeroJitter),
            Duration::from_secs(2),
        )
    })
    .await
    .expect("resilient client task")
    .expect("resilient client");
    let opened = client
        .open_subscription(Arc::from(&b"resume-me"[..]), Duration::from_secs(2))
        .expect("persistent subscription");
    assert_eq!(client.pressure().generation, 1);

    server.recycle_connections();
    let recovery = client.clone();
    let generation = tokio::task::spawn_blocking(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            let pressure = recovery.pressure();
            if pressure.generation >= 2 {
                break pressure.generation;
            }
            assert!(std::time::Instant::now() < deadline, "reconnect deadline");
            std::thread::sleep(Duration::from_millis(10));
        }
    })
    .await
    .expect("recovery observer");
    assert_eq!(generation, 2);
    assert_eq!(client.pressure().subscriptions, 1);

    drop(opened);
    drop(client);
    server.shutdown();
    let report = worker
        .await
        .expect("server task")
        .expect("server shutdown report");
    assert_eq!(report.accepted, 2);
    assert_eq!(report.rejected, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn daemon_epoch_restart_emits_explicit_server_epoch_gap_and_recovery_action() {
    let directory = physical_tempdir().unwrap();
    let address = directory.path().join("run/control.sock");
    let (first, hello) =
        endpoint_server(address.clone(), "local-epoch-one").expect("first daemon listener");
    let endpoint = address.clone();
    let running = Arc::clone(&first);
    let first_worker = tokio::spawn(async move { running.run().await });
    assert!(first.wait_until_running(Duration::from_secs(2)).await);
    let connector = connector(endpoint, hello).expect("Local connector");
    let client = tokio::task::spawn_blocking(move || {
        ResilientClient::connect(
            connector,
            ClientRuntimeLimits::new(32_768, 512, 8, 65_536).expect("client limits"),
            ClientRecoveryPolicy::new(
                Duration::from_millis(5),
                Duration::from_millis(50),
                10_000,
                1,
                200,
                400,
            )
            .expect("recovery policy"),
            Arc::new(ZeroJitter),
            Duration::from_secs(2),
        )
    })
    .await
    .expect("resilient client task")
    .expect("resilient client");
    let opened = client
        .open_subscription(Arc::from(&b"epoch-stream"[..]), Duration::from_secs(2))
        .expect("persistent subscription");

    first.shutdown();
    first_worker
        .await
        .expect("first server task")
        .expect("first shutdown");
    drop(first);
    let (second, _) =
        endpoint_server(address, "local-epoch-two").expect("replacement daemon listener");
    let running = Arc::clone(&second);
    let second_worker = tokio::spawn(async move { running.run().await });
    assert!(second.wait_until_running(Duration::from_secs(2)).await);
    assert!(client.wait_connected(Duration::from_secs(3)));
    let item = opened
        .subscription
        .recv_timeout(Duration::from_secs(3))
        .expect("typed epoch gap");
    let ClientStreamItem::Gap(gap) = item else {
        panic!("daemon epoch replacement must emit a gap")
    };
    assert_eq!(gap.reason, ClientStreamGapReason::ServerEpochChanged);
    assert_eq!(
        gap.recovery,
        ClientStreamRecovery::RestartFromAvailableOffset
    );

    drop(opened);
    drop(client);
    second.shutdown();
    second_worker
        .await
        .expect("second server task")
        .expect("second shutdown");
}

fn endpoint_server(
    address: PathBuf,
    epoch: &str,
) -> Result<(Arc<LocalServer>, ClientConnectionHello), Box<dyn Error>> {
    let (hello, policy) = connection_contract()?;
    let limits = server_limits(&policy)?;
    Ok((
        Arc::new(LocalServer::bind(
            LocalEndpoint::new(address)?,
            ServerEpoch::new(epoch)?,
            policy,
            limits,
            Arc::new(EchoDispatcher),
        )?),
        hello,
    ))
}
fn client_hello(id: &str) -> Result<ClientConnectionHello, Box<dyn Error>> {
    let (hello, _) = connection_contract()?;
    Ok(ClientConnectionHello::new(
        id.to_owned(),
        ClientProtocolVersion::CURRENT,
        hello.required_capabilities().into_iter().map(str::to_owned),
        hello.optional_capabilities().into_iter().map(str::to_owned),
        hello.limits(),
    )?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_flight_handshake_cancellation_and_deadline_unblock_connector() {
    for cancel in [true, false] {
        let directory = physical_tempdir().unwrap();
        let endpoint = LocalEndpoint::new(directory.path().join("run/control.sock")).unwrap();
        let owner = undercurrent_transport_local::LocalListener::bind(&endpoint).unwrap();
        let listener = undercurrent_transport_local::AsyncLocalListener::from_std(
            owner.take_listener().unwrap(),
        )
        .unwrap();
        let (hello, _) = connection_contract().unwrap();
        let config = LocalClientConfig::new(
            endpoint.path(),
            hello,
            LocalClientLimits::new(
                16_384,
                Duration::from_millis(200),
                Duration::from_millis(200),
                65_536,
                8,
                8,
            )
            .unwrap(),
        )
        .unwrap();
        let cancellation = ClientRecoveryCancellation::default();
        let worker_cancel = cancellation.clone();
        let started = std::time::Instant::now();
        let worker = tokio::task::spawn_blocking(move || {
            LocalClientConnector::new(config).connect(&worker_cancel)
        });
        let (_silent_peer, _) = listener.accept().await.unwrap();
        if cancel {
            cancellation.cancel();
        }
        let error = match tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap()
        {
            Ok(_) => panic!("silent peer cannot negotiate"),
            Err(error) => error,
        };
        let source = error
            .source()
            .unwrap()
            .downcast_ref::<LocalTransportError>()
            .unwrap();
        assert_eq!(
            source.stage(),
            if cancel {
                LocalTransportStage::Cancelled
            } else {
                LocalTransportStage::Negotiate
            }
        );
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_shutdown_interrupts_a_silent_handshake() {
    let (_directory, server, _) = server().unwrap();
    let running = server.clone();
    let worker = tokio::spawn(async move { running.run().await });
    assert!(server.wait_until_running(Duration::from_secs(1)).await);
    let _silent_client = undercurrent_transport_local::connect_local(server.endpoint())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.pressure().connection_workers == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    server.shutdown();
    tokio::time::timeout(Duration::from_millis(500), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

use undercurrent_protocol::product_capabilities;
use undercurrent_sdk::{ClientConnectionAgreement, StandaloneClientProfile};
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_sdk_profiles_negotiate_stream_queue_limits_component_wise() {
    let policy = queue_profile_policy().expect("queue profile policy");
    let limits = queue_profile_server_limits(&policy).expect("queue profile server limits");
    let directory = physical_tempdir().unwrap();
    let server = Arc::new(
        LocalServer::bind(
            LocalEndpoint::new(directory.path().join("run/control.sock")).unwrap(),
            ServerEpoch::new("queue-profile-epoch").expect("server epoch"),
            policy,
            limits,
            Arc::new(EchoDispatcher),
        )
        .expect("bind queue profile Local server"),
    );
    let endpoint = server.endpoint().path().to_path_buf();
    let running = server.clone();
    let worker = tokio::spawn(async move { running.run().await });
    assert!(server.wait_until_running(Duration::from_secs(2)).await);

    let standard_endpoint = endpoint.clone();
    let standard = tokio::task::spawn_blocking(move || {
        connect_profile_agreement(
            standard_endpoint,
            StandaloneClientProfile::standard("standard-queue-client")?,
        )
    })
    .await
    .expect("standard connector task")
    .expect("standard SDK agreement");
    assert_eq!(standard.limits().connection_queue_bytes(), 16_777_216);
    assert_eq!(standard.limits().stream_queue_bytes(), 8_388_608);
    assert_eq!(standard.limits().initial_stream_window_bytes(), 262_144);

    let recovery = tokio::task::spawn_blocking(move || {
        connect_profile_agreement(
            endpoint,
            StandaloneClientProfile::retained_recovery_qualification("recovery-queue-client")?,
        )
    })
    .await
    .expect("recovery connector task")
    .expect("retained-recovery SDK agreement");
    assert_eq!(recovery.limits().connection_queue_bytes(), 16_777_216);
    assert_eq!(recovery.limits().stream_queue_bytes(), 8_388_608);
    assert_eq!(recovery.limits().initial_stream_window_bytes(), 262_144);

    server.shutdown();
    let report = worker
        .await
        .expect("server task")
        .expect("server shutdown report");
    assert_eq!(report.accepted, 2);
    assert_eq!(report.rejected, 0);
    assert_eq!(report.failed, 0);
}

fn queue_profile_policy() -> Result<ConnectionPolicy, Box<dyn Error>> {
    let capabilities = BTreeSet::from([
        capability(connection_capabilities::MULTIPLEX)?,
        capability(connection_capabilities::FLOW_CONTROL)?,
        capability(connection_capabilities::STREAM_RESUME)?,
        capability(connection_capabilities::BINARY_PAYLOADS)?,
        capability(connection_capabilities::TRACE_CONTEXT)?,
        capability(product_capabilities::ERROR_DISPOSITION)?,
    ]);
    Ok(ConnectionPolicy::new(
        CURRENT_PROTOCOL_VERSION,
        capabilities,
        ConnectionLimits::new(262_144, 1_024, 256, 16_777_216, 8_388_608, 262_144)?,
        HeartbeatPolicy::new(60_000, 120_000)?,
        Some(60_000),
    )?)
}

fn queue_profile_server_limits(
    policy: &ConnectionPolicy,
) -> Result<LocalServerLimits, Box<dyn Error>> {
    Ok(LocalServerLimits::new(
        65_536,
        Duration::from_secs(2),
        Duration::from_secs(2),
        262_144,
        8,
        32,
        16_384,
        SchedulerMessageLimits::new(16_384, 1_024, [2_048, 4_096, 4_096, 6_144])?,
        ReplayLimits::new(1_024, 1_048_576, 67_108_864, 262_144)?,
        ServerDiagnosticLimits::new(512, 512)?,
        policy,
    )?)
}

fn connect_profile_agreement(
    endpoint: PathBuf,
    profile: StandaloneClientProfile,
) -> Result<ClientConnectionAgreement, Box<dyn Error + Send + Sync>> {
    let config = LocalClientConfig::new(endpoint, profile.hello().clone(), profile.local())?;
    let connector = LocalClientConnector::new(config);
    let negotiated = connector.connect(&ClientRecoveryCancellation::default())?;
    let (agreement, transport) = negotiated.into_parts();
    let client = Client::connect(
        agreement.clone(),
        transport,
        profile.product().client_limits(),
    )?;
    let payload: Arc<[u8]> = Arc::from(&b"small request to older daemon"[..]);
    assert_eq!(
        client.request(payload.clone(), Duration::from_secs(2))?,
        payload
    );
    assert!(matches!(
        client.request(Arc::from(vec![0; 262_144]), Duration::from_secs(2)),
        Err(undercurrent_sdk::ClientError::InvalidPayloadSize { .. })
    ));
    assert_eq!(
        client.request(payload.clone(), Duration::from_secs(2))?,
        payload
    );
    client.close();
    Ok(agreement)
}

#[derive(Default)]
struct LargeResponseDispatcher(Mutex<Vec<Arc<undercurrent_transport_local::ServerResponseWrite>>>);
impl AuthenticatedActionDispatcher for LargeResponseDispatcher {
    fn dispatch_authenticated(
        &self,
        _: Arc<AuthenticatedRequestContext>,
        session: Arc<ServerConnectionSession>,
        action: ServerAction,
    ) -> Result<(), ServerDispatchError> {
        if let ServerAction::Request { request_id, .. } = action {
            let completion = Arc::new(undercurrent_transport_local::ServerResponseWrite::default());
            session
                .complete_request_observed(
                    &request_id,
                    Arc::from(vec![42_u8; 24_000]),
                    completion.clone(),
                )
                .unwrap();
            self.0.lock().unwrap().push(completion);
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_reader_does_not_block_other_clients_or_shutdown() {
    use futures_util::{SinkExt, StreamExt};
    use undercurrent_transport_local::LocalFrameStream;
    use undercurrent_wire::{
        ClientId, ConnectionHello, HandshakeCodec, SpanId, TraceContext, TraceId, WireCodec,
    };
    let directory = physical_tempdir().unwrap();
    let endpoint = LocalEndpoint::new(directory.path().join("run/control.sock")).unwrap();
    let (hello, policy) = connection_contract().unwrap();
    let offer = ConnectionHello::new(
        ClientId::new("slow-reader").unwrap(),
        CURRENT_PROTOCOL_VERSION,
        policy.capabilities().clone(),
        BTreeSet::new(),
        policy.limits(),
    )
    .unwrap();
    let limits = server_limits(&policy).unwrap();
    let dispatcher = Arc::new(LargeResponseDispatcher::default());
    let server = Arc::new(
        LocalServer::bind(
            endpoint.clone(),
            ServerEpoch::new("slow-epoch").unwrap(),
            policy,
            limits,
            dispatcher.clone(),
        )
        .unwrap(),
    );
    let running = server.clone();
    let worker = tokio::spawn(async move { running.run().await });
    assert!(server.wait_until_running(Duration::from_secs(1)).await);
    let stream = undercurrent_transport_local::connect_local(&endpoint)
        .await
        .unwrap();
    let mut slow = LocalFrameStream::new(stream, 65_536).unwrap();
    let handshake = HandshakeCodec::new(16_384).unwrap();
    slow.send(handshake.encode_hello(&offer).unwrap().into())
        .await
        .unwrap();
    let accepted = slow.next().await.unwrap().unwrap();
    ClientConnectionAgreement::decode_accepted(&accepted, 16_384).unwrap();
    let codec = WireCodec::new(65_536).unwrap();
    // Small requests fit in the client send buffer; their much larger responses
    // fill the server send buffer while this peer deliberately never reads again.
    // Keep all 32 responses below the 1 MiB control queue cap: this exercises
    // blocked physical writes, rather than connection-fatal queue saturation.
    for i in 0..32 {
        let request = ClientMessage::Request {
            request_id: RequestId::new(format!("slow-{i}")).unwrap(),
            trace_context: TraceContext::new(
                TraceId::new("slow-trace").unwrap(),
                SpanId::new("slow-span").unwrap(),
            ),
            payload: Arc::from(&b"small"[..]),
        };
        tokio::time::timeout(
            Duration::from_secs(1),
            slow.send(codec.encode_client(&request).unwrap().into()),
        )
        .await
        .unwrap()
        .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while !dispatcher
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|write| !write.is_finished())
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("slow reader must leave a physical response write pending");
    let address = endpoint.path().to_path_buf();
    let healthy = tokio::task::spawn_blocking(move || connect_client(address, hello))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        healthy
            .request(Arc::from(&b"control"[..]), Duration::from_secs(1))
            .unwrap()
            .len(),
        24_000
    );
    healthy.close();
    server.shutdown();
    tokio::time::timeout(Duration::from_millis(500), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(slow);
    assert!(
        dispatcher
            .0
            .lock()
            .unwrap()
            .iter()
            .all(|write| write.is_finished())
    );
}

#[test]
fn listener_rejects_a_policy_that_exceeds_its_adapter_frame_bound_before_binding() {
    let directory = physical_tempdir().unwrap();
    let endpoint = LocalEndpoint::new(directory.path().join("run/control.sock")).unwrap();
    let (_, small_policy) = connection_contract().unwrap();
    let limits = server_limits(&small_policy).unwrap();
    let larger_policy = queue_profile_policy().unwrap();
    let result = LocalServer::bind(
        endpoint.clone(),
        ServerEpoch::new("invalid-policy").unwrap(),
        larger_policy,
        limits,
        Arc::new(EchoDispatcher),
    );
    assert!(
        result.is_err(),
        "reusing limits cannot silently enlarge the native I/O bound"
    );
    assert!(
        !endpoint.path().exists(),
        "invalid composition must not publish an endpoint"
    );
}

// macOS's default temp directory can exceed AF_UNIX limits once the product's
// run paths are appended. Keep IPC fixtures short and free of symlink ancestors.
fn physical_tempdir() -> std::io::Result<tempfile::TempDir> {
    #[cfg(target_os = "macos")]
    let base = std::path::PathBuf::from("/tmp");
    #[cfg(not(target_os = "macos"))]
    let base = std::env::temp_dir();
    tempfile::tempdir_in(std::fs::canonicalize(base)?)
}
