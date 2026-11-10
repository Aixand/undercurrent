// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::{Mutex, Weak, mpsc as native_channel};

use undercurrent_multiplex::MultiplexMetricObservation;
use undercurrent_wire::{
    CURRENT_PROTOCOL_VERSION, ClientId, ConnectionHello, ConnectionLimits, HeartbeatPolicy,
    NamespacedName, SpanId, TraceContext, TraceId, WireCodec, connection_capabilities,
};

use super::*;
use crate::ServerDispatchError;

const DEADLINE: Duration = Duration::from_secs(3);

#[derive(Default)]
struct Dispatcher {
    session: Mutex<Option<Weak<ServerConnectionSession>>>,
    closes: AtomicUsize,
    gate: Mutex<Option<(native_channel::Sender<()>, native_channel::Receiver<()>)>>,
    panic_on_close: AtomicBool,
}

impl AuthenticatedActionDispatcher for Dispatcher {
    fn dispatch_authenticated(
        &self,
        _authentication: Arc<AuthenticatedRequestContext>,
        session: Arc<ServerConnectionSession>,
        action: ServerAction,
    ) -> Result<(), ServerDispatchError> {
        *self.session.lock().unwrap() = Some(Arc::downgrade(&session));
        match action {
            ServerAction::Request {
                request_id,
                payload,
                ..
            } => {
                session.complete_request(&request_id, payload).unwrap();
            }
            ServerAction::CloseConnection => {
                self.closes.fetch_add(1, Ordering::AcqRel);
                let gate = self.gate.lock().unwrap().take();
                if let Some((entered, release)) = gate {
                    entered.send(()).unwrap();
                    release
                        .recv_timeout(DEADLINE)
                        .expect("release close callback");
                }
                assert!(
                    !self.panic_on_close.swap(false, Ordering::AcqRel),
                    "injected close panic"
                );
            }
            _ => {}
        }
        Ok(())
    }
}

impl Dispatcher {
    fn gate(&self) -> (native_channel::Receiver<()>, native_channel::Sender<()>) {
        let (entered, observed) = native_channel::channel();
        let (release, released) = native_channel::channel();
        *self.gate.lock().unwrap() = Some((entered, released));
        (observed, release)
    }
}

async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(DEADLINE, future)
        .await
        .expect("bounded cleanup")
}

async fn until(mut condition: impl FnMut() -> bool) {
    within(async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await;
}

fn fixture(dispatcher: Arc<Dispatcher>) -> (tempfile::TempDir, Arc<LocalServer>, ConnectionHello) {
    fixture_with_telemetry(dispatcher, Arc::new(NoopMultiplexTelemetry))
}

fn fixture_with_telemetry(
    dispatcher: Arc<Dispatcher>,
    telemetry: Arc<dyn MultiplexTelemetry>,
) -> (tempfile::TempDir, Arc<LocalServer>, ConnectionHello) {
    let directory = physical_tempdir().unwrap();
    let endpoint = LocalEndpoint::new(directory.path().join("run/control.sock")).unwrap();
    let capabilities = [
        connection_capabilities::MULTIPLEX,
        connection_capabilities::FLOW_CONTROL,
        connection_capabilities::STREAM_RESUME,
        connection_capabilities::TRACE_CONTEXT,
    ]
    .into_iter()
    .map(|s| NamespacedName::new(s).unwrap())
    .collect::<BTreeSet<_>>();
    let limits = ConnectionLimits::new(1_024, 4, 8, 32_768, 8_192, 4_096).unwrap();
    let policy = ConnectionPolicy::new(
        CURRENT_PROTOCOL_VERSION,
        capabilities.clone(),
        limits,
        HeartbeatPolicy::new(60_000, 120_000).unwrap(),
        Some(60_000),
    )
    .unwrap();
    let hello = ConnectionHello::new(
        ClientId::new("cleanup-client").unwrap(),
        CURRENT_PROTOCOL_VERSION,
        capabilities,
        BTreeSet::new(),
        limits,
    )
    .unwrap();
    let bounds = LocalServerLimits::new(
        4_096,
        DEADLINE,
        DEADLINE,
        4_096,
        4,
        8,
        1,
        SchedulerMessageLimits::new(32, 8, [8, 8, 8, 8]).unwrap(),
        ReplayLimits::new(8, 1_024, 8_192, 1_024).unwrap(),
        ServerDiagnosticLimits::new(8, 8).unwrap(),
        &policy,
    )
    .unwrap();
    let server = Arc::new(
        LocalServer::bind_with_telemetry(
            endpoint,
            ServerEpoch::new("cleanup-epoch").unwrap(),
            policy,
            bounds,
            dispatcher,
            telemetry,
        )
        .unwrap(),
    );
    (directory, server, hello)
}

async fn peer(
    server: &LocalServer,
    hello: &ConnectionHello,
) -> (LocalFrameStream<LocalStream>, HandshakeResponse) {
    let stream = crate::connect_local(server.endpoint()).await.unwrap();
    let mut peer = LocalFrameStream::new(stream, 4_096).unwrap();
    let codec = HandshakeCodec::new(4_096).unwrap();
    peer.send(codec.encode_hello(hello).unwrap().into())
        .await
        .unwrap();
    let bytes = within(peer.next()).await.unwrap().unwrap();
    let HandshakeDecodeStatus::Complete(frame) = codec.decode_response(&bytes).unwrap() else {
        panic!("complete handshake");
    };
    (peer, frame.message)
}

fn request(id: &str) -> ClientMessage {
    ClientMessage::Request {
        request_id: RequestId::new(id).unwrap(),
        trace_context: TraceContext::new(
            TraceId::new("cleanup-trace").unwrap(),
            SpanId::new("cleanup-span").unwrap(),
        ),
        payload: Arc::from(&b"echo"[..]),
    }
}

async fn active_peer(
    server: &LocalServer,
    hello: &ConnectionHello,
    dispatcher: &Dispatcher,
) -> (LocalFrameStream<LocalStream>, Weak<ServerConnectionSession>) {
    let (mut peer, response) = peer(server, hello).await;
    assert!(matches!(response, HandshakeResponse::Accepted(_)));
    let codec = WireCodec::new(1_024).unwrap();
    peer.send(codec.encode_client(&request("first")).unwrap().into())
        .await
        .unwrap();
    within(peer.next()).await.unwrap().unwrap();
    let session = dispatcher.session.lock().unwrap().clone().unwrap();
    (peer, session)
}

async fn retired(server: &LocalServer, session: &Weak<ServerConnectionSession>) {
    until(|| {
        session.strong_count() == 0
            && server.pressure().connection_workers == 0
            && server.pressure().connections.connections == 0
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accept_failure_drains_writer_and_preserves_listener_error() {
    let dispatcher = Arc::new(Dispatcher::default());
    let (_directory, server, hello) = fixture(dispatcher.clone());
    let running = server.clone();
    let task = tokio::spawn(async move { running.run().await });
    assert!(server.wait_until_running(DEADLINE).await);
    let (_peer, session) = active_peer(&server, &hello, &dispatcher).await;
    server.accept_failure.notify_one();
    let error = within(task)
        .await
        .unwrap()
        .expect_err("accept failure returned");
    assert_eq!(error.stage(), LocalServerStage::Listener);
    assert_eq!(error.category(), LocalServerErrorCategory::Transport);
    assert_eq!(error.io_kind(), Some(std::io::ErrorKind::Other));
    assert_eq!(session.strong_count(), 0);
    assert_eq!(server.pressure().connections.connections, 0);
    assert_eq!(server.pressure().connection_workers, 0);
    assert_eq!(dispatcher.closes.load(Ordering::Acquire), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_keeps_registration_until_product_cleanup_finishes() {
    let dispatcher = Arc::new(Dispatcher::default());
    let (_directory, server, hello) = fixture(dispatcher.clone());
    let running = server.clone();
    let task = tokio::spawn(async move { running.run().await });
    assert!(server.wait_until_running(DEADLINE).await);
    let (_peer, session) = active_peer(&server, &hello, &dispatcher).await;
    let (entered, release) = dispatcher.gate();
    task.abort();
    assert!(within(task).await.unwrap_err().is_cancelled());
    within(tokio::task::spawn_blocking(move || {
        entered.recv_timeout(DEADLINE).unwrap()
    }))
    .await
    .unwrap();
    let live = session.upgrade().unwrap();
    assert!(live.is_closed());
    assert!(
        server
            .connection_registry()
            .register_session(&live)
            .is_err(),
        "old ID stays reserved during cleanup"
    );
    assert_eq!(server.pressure().connection_workers, 1);
    release.send(()).unwrap();
    drop(live);
    retired(&server, &session).await;
    assert_eq!(dispatcher.closes.load(Ordering::Acquire), 1);
    let agreement = negotiate_connection(
        &hello,
        &server.shared.policy,
        ConnectionId::new("replacement").unwrap(),
        server.shared.epoch.clone(),
    )
    .unwrap();
    server
        .connection_registry()
        .register(hello.client_id().clone(), &agreement)
        .expect("ID released after cleanup")
        .release();
}

fn owned_connection(
    server: &LocalServer,
    hello: &ConnectionHello,
    supervisor: &CleanupSupervisor,
    semaphore: &Arc<Semaphore>,
) -> (ConnectionCleanup, Arc<ServerConnectionSession>) {
    let agreement = negotiate_connection(
        hello,
        &server.shared.policy,
        ConnectionId::new("owned-connection").unwrap(),
        server.shared.epoch.clone(),
    )
    .unwrap();
    let scheduler = scheduler_limits(&agreement, server.shared.limits.scheduler_messages).unwrap();
    let session = Arc::new(
        ServerConnectionSession::with_telemetry(
            hello.client_id().clone(),
            agreement,
            scheduler,
            server.shared.replay.clone(),
            server.shared.limits.diagnostics,
            server.shared.telemetry.clone(),
        )
        .unwrap(),
    );
    let registration = server
        .connection_registry()
        .register_session(&session)
        .unwrap();
    let authentication = Arc::new(AuthenticatedRequestContext::new(
        AuthenticatedPrincipalRef::new("local-undercurrent-user").unwrap(),
        None,
        AuthorityRefDto::new("undercurrent.local").unwrap(),
    ));
    let mut cleanup = supervisor.own(
        server.shared.clone(),
        semaphore.clone().try_acquire_owned().unwrap(),
    );
    cleanup.attach(session.clone(), authentication, registration);
    cleanup.start_writer(1).unwrap();
    (cleanup, session)
}

#[tokio::test]
async fn dropping_owner_releases_writer_blocked_on_full_channel() {
    let dispatcher = Arc::new(Dispatcher::default());
    let (_directory, server, hello) = fixture(dispatcher.clone());
    let supervisor = CleanupSupervisor::start().unwrap();
    let semaphore = Arc::new(Semaphore::new(1));
    let (mut cleanup, session) = owned_connection(&server, &hello, &supervisor, &semaphore);
    for id in ["one", "two"] {
        session.accept(request(id)).unwrap();
        session
            .complete_request(&RequestId::new(id).unwrap(), Arc::from(&b"data"[..]))
            .unwrap();
    }
    // With no receiver polling, selecting both frames fills the one-slot channel
    // and leaves the second send blocked (or about to block).
    until(|| cleanup.receiver().len() == 1 && session.pressure().scheduler.queued_messages == 0)
        .await;
    let weak = Arc::downgrade(&session);
    drop(session);
    drop(cleanup);
    within(supervisor.finish()).await.unwrap();
    assert_eq!(weak.strong_count(), 0);
    assert_eq!(semaphore.available_permits(), 1);
    assert_eq!(server.pressure().connection_workers, 0);
    assert_eq!(dispatcher.closes.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn cancellation_during_cleanup_keeps_permit_and_finishes_join() {
    let dispatcher = Arc::new(Dispatcher::default());
    let (_directory, server, hello) = fixture(dispatcher.clone());
    let supervisor = CleanupSupervisor::start().unwrap();
    let semaphore = Arc::new(Semaphore::new(1));
    let (cleanup, session) = owned_connection(&server, &hello, &supervisor, &semaphore);
    let weak = Arc::downgrade(&session);
    drop(session);
    let (entered, release) = dispatcher.gate();
    let task = tokio::spawn(cleanup.finish());
    within(tokio::task::spawn_blocking(move || {
        entered.recv_timeout(DEADLINE).unwrap()
    }))
    .await
    .unwrap();
    task.abort();
    assert!(within(task).await.unwrap_err().is_cancelled());
    assert_eq!(semaphore.available_permits(), 0);
    assert_eq!(server.pressure().connection_workers, 1);
    release.send(()).unwrap();
    within(supervisor.finish()).await.unwrap();
    assert_eq!(weak.strong_count(), 0);
    assert_eq!(semaphore.available_permits(), 1);
    assert_eq!(dispatcher.closes.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn cleanup_continues_after_close_callback_panics() {
    let dispatcher = Arc::new(Dispatcher::default());
    let (_directory, server, hello) = fixture(dispatcher.clone());
    let supervisor = CleanupSupervisor::start().unwrap();
    let semaphore = Arc::new(Semaphore::new(1));
    for panics in [true, false] {
        let (cleanup, session) = owned_connection(&server, &hello, &supervisor, &semaphore);
        let weak = Arc::downgrade(&session);
        drop(session);
        dispatcher.panic_on_close.store(panics, Ordering::Release);
        let result = within(cleanup.finish()).await;
        assert_eq!(result.is_err(), panics);
        assert_eq!(weak.strong_count(), 0);
        assert_eq!(semaphore.available_permits(), 1);
    }
    within(supervisor.finish()).await.unwrap();
    assert_eq!(dispatcher.closes.load(Ordering::Acquire), 2);
}

#[derive(Default)]
struct PanickingTelemetry(AtomicBool);

impl MultiplexTelemetry for PanickingTelemetry {
    fn record_metric(&self, observation: MultiplexMetricObservation) {
        if observation.name == undercurrent_multiplex::MultiplexMetricName::PendingRequests {
            assert!(
                !self.0.swap(false, Ordering::AcqRel),
                "injected telemetry panic"
            );
        }
    }
}

#[tokio::test]
async fn close_telemetry_panic_does_not_strand_writer_join() {
    let dispatcher = Arc::new(Dispatcher::default());
    let telemetry = Arc::new(PanickingTelemetry::default());
    let (_directory, server, hello) = fixture_with_telemetry(dispatcher.clone(), telemetry.clone());
    let supervisor = CleanupSupervisor::start().unwrap();
    let semaphore = Arc::new(Semaphore::new(1));
    let (cleanup, session) = owned_connection(&server, &hello, &supervisor, &semaphore);
    let weak = Arc::downgrade(&session);
    drop(session);
    telemetry.0.store(true, Ordering::Release);
    assert!(within(cleanup.finish()).await.is_err());
    within(supervisor.finish()).await.unwrap();
    assert_eq!(weak.strong_count(), 0);
    assert_eq!(semaphore.available_permits(), 1);
    assert_eq!(dispatcher.closes.load(Ordering::Acquire), 1);
}

#[test]
fn cleanup_survives_destruction_of_the_tokio_runtime() {
    let dispatcher = Arc::new(Dispatcher::default());
    let (_directory, server, hello) = fixture(dispatcher.clone());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (peer, weak, task) = runtime.block_on(async {
        let running = server.clone();
        let task = tokio::spawn(async move { running.run().await });
        assert!(server.wait_until_running(DEADLINE).await);
        let (peer, weak) = active_peer(&server, &hello, &dispatcher).await;
        (peer, weak, task)
    });
    let (entered, release) = dispatcher.gate();
    drop(runtime);
    // There is no runtime left to poll another async cleanup task. The native
    // owner must close and retire the session on its own.
    entered
        .recv_timeout(DEADLINE)
        .expect("cleanup runs without Tokio");
    assert_eq!(server.pressure().connection_workers, 1);
    release.send(()).unwrap();
    let deadline = Instant::now() + DEADLINE;
    while weak.strong_count() != 0 || server.pressure().connection_workers != 0 {
        assert!(
            Instant::now() < deadline,
            "runtime drop retires native writer"
        );
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(server.pressure().connections.connections, 0);
    assert_eq!(dispatcher.closes.load(Ordering::Acquire), 1);
    drop((task, peer));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_close_is_dispatched_once_and_allows_same_client_to_reconnect() {
    let dispatcher = Arc::new(Dispatcher::default());
    let (_directory, server, hello) = fixture(dispatcher.clone());
    let running = server.clone();
    let task = tokio::spawn(async move { running.run().await });
    assert!(server.wait_until_running(DEADLINE).await);
    for count in 1..=2 {
        let (mut peer, session) = active_peer(&server, &hello, &dispatcher).await;
        peer.send(
            WireCodec::new(1_024)
                .unwrap()
                .encode_client(&ClientMessage::Close)
                .unwrap()
                .into(),
        )
        .await
        .unwrap();
        retired(&server, &session).await;
        assert_eq!(dispatcher.closes.load(Ordering::Acquire), count);
    }
    server.shutdown();
    let report = within(task).await.unwrap().unwrap();
    assert_eq!(report.completed, 2);
    assert_eq!(report.failed, 0);
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
