// SPDX-License-Identifier: Apache-2.0

//! Daemon-hosted authenticated Local listener and connection workers.
//!
//! Each async connection worker owns native I/O. A per-connection OS thread
//! bridges transport core's blocking writer wait to a bounded Tokio channel.
//! Successful physical writes are confirmed for in-memory replay; they do not
//! acknowledge peer processing or commit product state.

use crate::{LocalEndpoint, LocalFrameStream, LocalListener, verify_local_peer};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

#[cfg(windows)]
use crate::AsyncLocalListener;
use crate::LocalStream;
use crate::{
    ActiveConnectionPressure, ActiveConnectionRegistry, AuthenticatedActionDispatcher,
    PendingServerWrite, ServerAction, ServerAdmissionRejection, ServerAdmissionResponseKind,
    ServerConnectionError, ServerConnectionErrorCategory, ServerConnectionSession,
    ServerDiagnosticLimits,
};
use futures_util::{SinkExt, StreamExt};
#[cfg(unix)]
use tokio::net::UnixListener as AsyncLocalListener;
use tokio::sync::{Notify, Semaphore, broadcast, mpsc, watch};
use tokio::task::JoinSet;
use undercurrent_multiplex::{
    ClientMessage, MultiplexOverload, MultiplexTelemetry, NoopMultiplexTelemetry, ReplayJournal,
    ReplayLimits, ReplayPressure, SchedulerLimits, SchedulerMessageLimits,
};
use undercurrent_protocol::AuthenticatedRequestContext;
use undercurrent_wire::{
    ConnectionAgreement, ConnectionId, ConnectionNegotiationError, ConnectionPolicy, RequestId,
    ServerEpoch, negotiate_connection,
};
use undercurrent_wire::{
    DecodeStatus, HandshakeCodec, HandshakeDecodeStatus, HandshakeRejection,
    HandshakeRejectionCategory, HandshakeResponse,
};

use crate::{LocalServerError, LocalServerErrorCategory, LocalServerStage};
use undercurrent_protocol::{AuthenticatedPrincipalRef, AuthorityRefDto};

mod cleanup;
use cleanup::{CleanupSupervisor, ConnectionCleanup};
#[cfg(test)]
mod cleanup_tests;

const REPLAY_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(5);

/// Invalid daemon Local resource limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalServerLimitError {
    /// One required numeric limit was zero.
    ZeroLimit {
        /// Stable invalid field.
        field: &'static str,
    },
    /// One required duration was zero.
    ZeroDuration {
        /// Stable invalid field.
        field: &'static str,
    },
    /// Handshake maximum cannot contain its fixed header.
    InvalidHandshakeMaximum,
    /// Local message maximum cannot contain one handshake frame.
    MessageBelowHandshake,
    /// Maximum message bytes cannot contain the offered UCMP frame.
    MessageBelowPolicy,
    /// Negotiated queue cannot reserve one frame for each traffic class.
    ConnectionQueueBelowClassReservations,
    /// Negotiated stream queue cannot retain one maximum frame.
    StreamQueueBelowFrame,
    /// Replay cannot retain one maximum frame.
    ReplayItemBelowFrame,
}

impl Display for LocalServerLimitError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroLimit { field } => write!(formatter, "{field} must be non-zero"),
            Self::ZeroDuration { field } => write!(formatter, "{field} must be non-zero"),
            Self::InvalidHandshakeMaximum => {
                formatter.write_str("handshake frame maximum is invalid")
            }
            Self::MessageBelowPolicy => {
                formatter.write_str("message maximum is below policy frame maximum")
            }
            Self::MessageBelowHandshake => {
                formatter.write_str("message maximum is below handshake maximum")
            }
            Self::ConnectionQueueBelowClassReservations => {
                formatter.write_str("connection queue cannot reserve one frame per traffic class")
            }
            Self::StreamQueueBelowFrame => {
                formatter.write_str("stream queue cannot retain one frame")
            }
            Self::ReplayItemBelowFrame => formatter.write_str("replay cannot retain one frame"),
        }
    }
}

impl Error for LocalServerLimitError {}

/// Explicit listener, handshake, worker, and connection-state bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalServerLimits {
    handshake_frame_bytes: u32,
    handshake_timeout: Duration,
    write_timeout: Duration,
    maximum_message_bytes: usize,
    maximum_connections: usize,
    maximum_retained_issues: usize,
    writer_queue_messages: usize,
    scheduler_messages: SchedulerMessageLimits,
    replay: ReplayLimits,
    diagnostics: ServerDiagnosticLimits,
}

impl LocalServerLimits {
    /// Validates daemon adapter bounds against the maximum policy offer.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        handshake_frame_bytes: u32,
        handshake_timeout: Duration,
        write_timeout: Duration,
        maximum_message_bytes: usize,
        maximum_connections: usize,
        maximum_retained_issues: usize,
        writer_queue_messages: usize,
        scheduler_messages: SchedulerMessageLimits,
        replay: ReplayLimits,
        diagnostics: ServerDiagnosticLimits,
        policy: &ConnectionPolicy,
    ) -> Result<Self, LocalServerLimitError> {
        if HandshakeCodec::new(handshake_frame_bytes).is_err() {
            return Err(LocalServerLimitError::InvalidHandshakeMaximum);
        }
        for (field, duration) in [
            ("handshake_timeout", handshake_timeout),
            ("write_timeout", write_timeout),
        ] {
            if duration.is_zero() {
                return Err(LocalServerLimitError::ZeroDuration { field });
            }
        }
        for (field, value) in [
            ("maximum_message_bytes", maximum_message_bytes),
            ("maximum_connections", maximum_connections),
            ("maximum_retained_issues", maximum_retained_issues),
            ("writer_queue_messages", writer_queue_messages),
        ] {
            if value == 0 {
                return Err(LocalServerLimitError::ZeroLimit { field });
            }
        }
        if maximum_message_bytes < usize::try_from(handshake_frame_bytes).unwrap_or(usize::MAX) {
            return Err(LocalServerLimitError::MessageBelowHandshake);
        }
        if maximum_message_bytes < policy.limits().max_frame_bytes() as usize {
            return Err(LocalServerLimitError::MessageBelowPolicy);
        }
        let frame = u64::from(policy.limits().max_frame_bytes());
        if policy.limits().connection_queue_bytes() < frame.saturating_mul(4) {
            return Err(LocalServerLimitError::ConnectionQueueBelowClassReservations);
        }
        if policy.limits().stream_queue_bytes() < frame {
            return Err(LocalServerLimitError::StreamQueueBelowFrame);
        }
        if replay.max_item_bytes() < frame {
            return Err(LocalServerLimitError::ReplayItemBelowFrame);
        }
        Ok(Self {
            handshake_frame_bytes,
            handshake_timeout,
            write_timeout,
            maximum_message_bytes,
            maximum_connections,
            maximum_retained_issues,
            writer_queue_messages,
            scheduler_messages,
            replay,
            diagnostics,
        })
    }

    /// Returns the UCHS complete-frame maximum.
    pub const fn handshake_frame_bytes(self) -> u32 {
        self.handshake_frame_bytes
    }

    /// Returns the inclusive active physical-connection maximum.
    pub const fn maximum_connections(self) -> usize {
        self.maximum_connections
    }

    /// Returns the first complete UCHS hello read timeout.
    ///
    /// Response writes and rejection close use separate write timeouts; this
    /// is not one deadline for peer verification and the complete exchange.
    pub const fn handshake_timeout(self) -> Duration {
        self.handshake_timeout
    }

    /// Returns the maximum duration of one complete server message write.
    pub const fn write_timeout(self) -> Duration {
        self.write_timeout
    }

    /// Returns the bounded diagnostic page settings.
    pub const fn diagnostic_limits(self) -> ServerDiagnosticLimits {
        self.diagnostics
    }
}

/// Payload-free connection failure retained by one run report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalConnectionIssue {
    /// Monotonic accepted connection ordinal.
    pub connection_ordinal: u64,
    /// Stable failed adapter stage.
    pub stage: LocalServerStage,
    /// Stable failure category.
    pub category: LocalServerErrorCategory,
}

/// Bounded aggregate outcome from one listener run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocalServerRunReport {
    /// Native streams accepted, including immediate capacity rejections.
    pub accepted: u64,
    /// Workers returning `Completed`, including shutdown during hello reception.
    /// This does not count product executions or require completed negotiation.
    pub completed: u64,
    /// Connections rejected before UCMP activation.
    pub rejected: u64,
    /// Recorded connection worker failures, including worker task join failures.
    /// Fatal listener errors return directly instead of returning this report.
    pub failed: u64,
    /// Total issues, including those beyond retention.
    pub issue_count: u64,
    /// Bounded payload-free retained issues.
    pub issues: Vec<LocalConnectionIssue>,
}

impl LocalServerRunReport {
    fn record_failure(&mut self, ordinal: u64, error: LocalServerError, maximum: usize) {
        self.failed = self.failed.saturating_add(1);
        self.issue_count = self.issue_count.saturating_add(1);
        if self.issues.len() < maximum {
            self.issues.push(LocalConnectionIssue {
                connection_ordinal: ordinal,
                stage: error.stage(),
                category: error.category(),
            });
        }
    }
}

/// Current bounded listener and connection-state pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalServerPressure {
    /// Registered logical clients and physical connections.
    pub connections: ActiveConnectionPressure,
    /// Accepted connections still owned by an async worker or its cleanup.
    pub connection_workers: usize,
    /// Shared reconnect replay pressure.
    pub replay: ReplayPressure,
    /// Whether new local IPC sockets are admitted.
    pub accepting: bool,
}

/// OS-authenticated local listener hosted directly by `uc-daemon`.
///
/// Bind performs native I/O; [`Self::run`] uses a caller-owned Tokio runtime
/// exactly once. Stop admission to drain, or request shutdown and await the
/// run future to close workers. There is no aggregate drain deadline here.
/// Unix endpoint ownership remains held until this object is dropped.
pub struct LocalServer {
    endpoint: LocalEndpoint,
    _ownership: LocalListener,
    shared: Arc<ServerShared>,
    started: AtomicBool,
    running: AtomicBool,
    running_changed: Notify,
    admission: watch::Sender<bool>,
    shutdown: watch::Sender<bool>,
    disconnect: broadcast::Sender<()>,
    #[cfg(test)]
    accept_failure: Notify,
}

impl LocalServer {
    /// Binds a kernel-authenticated local listener without starting a runtime.
    pub fn bind(
        endpoint: LocalEndpoint,
        epoch: ServerEpoch,
        policy: ConnectionPolicy,
        limits: LocalServerLimits,
        dispatcher: Arc<dyn AuthenticatedActionDispatcher>,
    ) -> Result<Self, LocalServerError> {
        Self::bind_with_telemetry(
            endpoint,
            epoch,
            policy,
            limits,
            dispatcher,
            Arc::new(NoopMultiplexTelemetry),
        )
    }

    /// Binds the local endpoint with explicit bounded telemetry.
    pub fn bind_with_telemetry(
        endpoint: LocalEndpoint,
        epoch: ServerEpoch,
        policy: ConnectionPolicy,
        limits: LocalServerLimits,
        dispatcher: Arc<dyn AuthenticatedActionDispatcher>,
        telemetry: Arc<dyn MultiplexTelemetry>,
    ) -> Result<Self, LocalServerError> {
        if policy.limits().max_frame_bytes() as usize > limits.maximum_message_bytes {
            return Err(LocalServerError::new(
                LocalServerErrorCategory::Configuration,
                LocalServerStage::Listener,
            )
            .with_source(LocalServerLimitError::MessageBelowPolicy));
        }
        let ownership = LocalListener::bind(&endpoint).map_err(|e| {
            transport_error(LocalServerStage::Listener)
                .with_io_kind(e.kind())
                .with_source(e)
        })?;
        let connections =
            ActiveConnectionRegistry::new(limits.maximum_connections, limits.diagnostics)
                .map_err(|e| server_error(LocalServerStage::ComposeConnection).with_source(e))?;
        let replay = Arc::new(ReplayJournal::new(epoch.clone(), limits.replay));
        let (admission, _) = watch::channel(false);
        let (shutdown, _) = watch::channel(false);
        let (disconnect, _) = broadcast::channel(limits.maximum_connections);
        Ok(Self {
            endpoint,
            _ownership: ownership,
            shared: Arc::new(ServerShared {
                epoch,
                policy,
                limits,
                dispatcher,
                telemetry,
                connections,
                replay,
                next_connection: AtomicU64::new(0),
                active_workers: AtomicUsize::new(0),
                accepting: AtomicBool::new(true),
            }),
            started: AtomicBool::new(false),
            running: AtomicBool::new(false),
            running_changed: Notify::new(),
            admission,
            shutdown,
            disconnect,
            #[cfg(test)]
            accept_failure: Notify::new(),
        })
    }

    /// Returns the bound platform-local endpoint.
    pub fn endpoint(&self) -> &LocalEndpoint {
        &self.endpoint
    }

    /// Waits until the single owned listener has entered its Tokio accept loop.
    ///
    /// Observes the running flag, not application readiness or a completed
    /// client handshake. A concurrent exit may make readiness transient.
    pub async fn wait_until_running(&self, timeout: Duration) -> bool {
        let changed = self.running_changed.notified();
        if self.running.load(Ordering::Acquire) {
            return true;
        }
        tokio::time::timeout(timeout, changed).await.is_ok() && self.running.load(Ordering::Acquire)
    }

    /// Runs the bounded async accept loop exactly once.
    ///
    /// Drains workers after admission stops, with no aggregate deadline.
    /// A second invocation fails even if the first invocation failed. Fatal
    /// accept errors close and drain connections before returning the error.
    /// Aborting this future transfers connection retirement to a native cleanup
    /// supervisor; it does not wait for completion. Use [`Self::shutdown`] and
    /// await this future when completion must be observed.
    pub async fn run(&self) -> Result<LocalServerRunReport, LocalServerError> {
        if self.started.swap(true, Ordering::AcqRel) {
            return Err(LocalServerError::new(
                LocalServerErrorCategory::Internal,
                LocalServerStage::Listener,
            ));
        }
        let listener = self
            ._ownership
            .take_listener()
            .map_err(|e| transport_error(LocalServerStage::Listener).with_source(e))?;
        let listener = AsyncLocalListener::from_std(listener).map_err(|source| {
            LocalServerError::new(
                LocalServerErrorCategory::Transport,
                LocalServerStage::Listener,
            )
            .with_io_kind(source.kind())
            .with_source(source)
        })?;
        let cleanup = CleanupSupervisor::start()?;
        self.running.store(true, Ordering::Release);
        self.running_changed.notify_waiters();
        let _running = RunningListener::new(self);
        let semaphore = Arc::new(Semaphore::new(self.shared.limits.maximum_connections));
        let mut admission = self.admission.subscribe();
        let mut workers = JoinSet::new();
        let mut report = LocalServerRunReport::default();
        let mut replay_maintenance = ReplayMaintenance::new(self.shared.replay.clone());

        if !self.shared.accepting.load(Ordering::Acquire) {
            cleanup.finish().await?;
            return Ok(report);
        }
        let mut listener_error = None;

        loop {
            tokio::select! {
                accepted = async {
                    #[cfg(test)]
                    {
                        tokio::select! {
                            () = self.accept_failure.notified() => Err(std::io::Error::other("injected accept failure")),
                            result = listener.accept() => result,
                        }
                    }
                    #[cfg(not(test))]
                    { listener.accept().await }
                }, if self.shared.accepting.load(Ordering::Acquire) => {
                    let (stream, _peer) = match accepted {
                        Ok(accepted) => accepted,
                        Err(source) => {
                            listener_error = Some(LocalServerError::new(
                                LocalServerErrorCategory::Transport,
                                LocalServerStage::Listener,
                            ).with_io_kind(source.kind()).with_source(source));
                            self.shutdown();
                            break;
                        }
                    };
                    report.accepted = report.accepted.saturating_add(1);
                    let Some(ordinal) = self.shared.next_connection.fetch_add(1, Ordering::AcqRel)
                        .checked_add(1) else {
                            listener_error = Some(server_error(LocalServerStage::ComposeConnection));
                            self.shutdown();
                            break;
                        };
                    let Ok(permit) = semaphore.clone().try_acquire_owned() else {
                        report.rejected = report.rejected.saturating_add(1);
                        drop(stream);
                        continue;
                    };
                    let shared = self.shared.clone();
                    let shutdown = self.shutdown.subscribe();
                    let disconnect = self.disconnect.subscribe();
                    let connection_cleanup = cleanup.own(shared.clone(), permit);
                    workers.spawn(async move {
                        (ordinal, serve_connection(
                            stream,
                            ordinal,
                            shared,
                            shutdown,
                            disconnect,
                            connection_cleanup,
                        ).await)
                    });
                }
                completion = workers.join_next(), if !workers.is_empty() => {
                    if let Some(completion) = completion {
                        record_completion(completion, &mut report, self.shared.limits.maximum_retained_issues);
                    }
                }
                () = replay_maintenance.tick() => {}
                changed = admission.changed() => {
                    let _ = changed;
                    break;
                }
            }
        }
        drop(listener);
        while !workers.is_empty() {
            tokio::select! {
                completion = workers.join_next() => {
                    if let Some(completion) = completion {
                        record_completion(
                            completion,
                            &mut report,
                            self.shared.limits.maximum_retained_issues,
                        );
                    }
                }
                () = replay_maintenance.tick() => {}
            }
        }
        // Workers may have panicked or been cancelled while owning a cleanup
        // guard. Drain its independent supervisor before claiming completion.
        let cleaned = cleanup.finish().await;
        if let Some(error) = listener_error {
            return Err(error);
        }
        cleaned?;
        Ok(report)
    }

    /// Stops accepting new sockets while allowing active connections to drain.
    ///
    /// Does not close existing workers or release the Unix endpoint lock.
    /// A native accept already in flight may race this signal.
    pub fn stop_accepting(&self) {
        self.shared.accepting.store(false, Ordering::Release);
        let _ = self.admission.send(true);
    }

    /// Stops admission and requests closure of active native connections.
    ///
    /// Signals workers; it does not synchronously wait for their cleanup,
    /// roll back product effects, or cancel a managed runtime attempt.
    pub fn shutdown(&self) {
        self.stop_accepting();
        let _ = self.shutdown.send(true);
    }

    /// Closes current physical sockets without stopping listener admission.
    ///
    /// Logical clients can reconnect and resume against the retained epoch and
    /// replay journal. This also supports bounded connection-recovery drills.
    pub fn recycle_connections(&self) {
        let _ = self.disconnect.send(());
    }

    /// Returns current payload-free listener and connection pressure.
    ///
    /// The fields are separate concurrent samples, not an atomic snapshot.
    /// Admission being enabled does not establish that `run` is live.
    pub fn pressure(&self) -> LocalServerPressure {
        LocalServerPressure {
            connections: self.shared.connections.pressure(),
            connection_workers: self.shared.active_workers.load(Ordering::Acquire),
            replay: self.shared.replay.pressure(),
            accepting: self.shared.accepting.load(Ordering::Acquire),
        }
    }

    /// Returns the bounded active-connection diagnostic registry.
    pub fn connection_registry(&self) -> &ActiveConnectionRegistry {
        &self.shared.connections
    }
}

impl Drop for LocalServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct RunningListener<'a> {
    server: &'a LocalServer,
}

impl<'a> RunningListener<'a> {
    const fn new(server: &'a LocalServer) -> Self {
        Self { server }
    }
}

impl Drop for RunningListener<'_> {
    fn drop(&mut self) {
        self.server.running.store(false, Ordering::Release);
        self.server.running_changed.notify_waiters();
    }
}

struct ServerShared {
    epoch: ServerEpoch,
    policy: ConnectionPolicy,
    limits: LocalServerLimits,
    dispatcher: Arc<dyn AuthenticatedActionDispatcher>,
    telemetry: Arc<dyn MultiplexTelemetry>,
    connections: ActiveConnectionRegistry,
    replay: Arc<ReplayJournal>,
    next_connection: AtomicU64,
    active_workers: AtomicUsize,
    accepting: AtomicBool,
}

struct ReplayMaintenance {
    replay: Arc<ReplayJournal>,
    interval: tokio::time::Interval,
}

impl ReplayMaintenance {
    fn new(replay: Arc<ReplayJournal>) -> Self {
        Self::with_interval(replay, REPLAY_MAINTENANCE_INTERVAL)
    }

    fn with_interval(replay: Arc<ReplayJournal>, cadence: Duration) -> Self {
        let first = tokio::time::Instant::now() + cadence;
        let mut interval = tokio::time::interval_at(first, cadence);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Self { replay, interval }
    }

    async fn tick(&mut self) {
        self.interval.tick().await;
        self.replay.expire_before(Instant::now());
    }
}

#[cfg(test)]
mod replay_maintenance_tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use undercurrent_multiplex::{
        ReplayJournal, ReplayLimits, ReplayStreamKey, StreamRecoveryAction,
    };
    use undercurrent_wire::{ClientId, ServerEpoch, StreamId};

    use super::ReplayMaintenance;

    #[tokio::test]
    async fn idle_timer_expires_every_due_journal_without_pressure_or_traffic() {
        let replay = Arc::new(ReplayJournal::new(
            ServerEpoch::new("maintenance-epoch").expect("server epoch"),
            ReplayLimits::new(3, 8, 24, 4).expect("replay limits"),
        ));
        let keys = (0..3)
            .map(|index| {
                ReplayStreamKey::new(
                    ClientId::new(format!("maintenance-client-{index}")).expect("client ID"),
                    StreamId::new(format!("maintenance-stream-{index}")).expect("stream ID"),
                )
            })
            .collect::<Vec<_>>();
        for key in &keys {
            replay
                .register_stream(key.clone(), StreamRecoveryAction::RestartQuery)
                .expect("journal registers");
        }
        let common_deadline = Instant::now();
        for key in &keys {
            assert!(replay.deactivate_stream(key, common_deadline));
        }

        let mut maintenance =
            ReplayMaintenance::with_interval(replay.clone(), Duration::from_millis(1));
        tokio::time::timeout(Duration::from_secs(1), maintenance.tick())
            .await
            .expect("maintenance timer fires");

        assert_eq!(replay.expire_before(Instant::now()).streams, 0);
        let pressure = replay.pressure();
        assert_eq!(pressure.streams, 0);
        assert_eq!(pressure.expiring_streams, 0);
    }
}

struct ActiveWorker(Arc<ServerShared>);

impl ActiveWorker {
    fn new(shared: Arc<ServerShared>) -> Self {
        shared.active_workers.fetch_add(1, Ordering::AcqRel);
        Self(shared)
    }
}

impl Drop for ActiveWorker {
    fn drop(&mut self) {
        self.0.active_workers.fetch_sub(1, Ordering::AcqRel);
    }
}

enum ConnectionDisposition {
    Completed,
    Rejected,
}

#[allow(clippy::too_many_arguments)]
async fn serve_connection(
    stream: LocalStream,
    ordinal: u64,
    shared: Arc<ServerShared>,
    mut shutdown: watch::Receiver<bool>,
    mut disconnect: broadcast::Receiver<()>,
    mut cleanup: ConnectionCleanup,
) -> Result<ConnectionDisposition, LocalServerError> {
    if verify_local_peer(&stream).is_err() {
        return Ok(ConnectionDisposition::Rejected);
    }
    let authentication = Arc::new(AuthenticatedRequestContext::new(
        AuthenticatedPrincipalRef::new("local-undercurrent-user")
            .map_err(|e| server_error(LocalServerStage::Authenticate).with_source(e))?,
        None,
        AuthorityRefDto::new("undercurrent.local")
            .map_err(|e| server_error(LocalServerStage::Authenticate).with_source(e))?,
    ));
    let mut local = LocalFrameStream::new(stream, shared.limits.handshake_frame_bytes as usize)
        .map_err(|e| transport_error(LocalServerStage::Negotiate).with_source(e))?;

    let handshake = HandshakeCodec::new(shared.limits.handshake_frame_bytes).map_err(|source| {
        LocalServerError::new(
            LocalServerErrorCategory::Configuration,
            LocalServerStage::Negotiate,
        )
        .with_source(source)
    })?;
    let hello_message = tokio::select! {
    _ = shutdown.changed() => return Ok(ConnectionDisposition::Completed),
    _ = disconnect.recv() => return Ok(ConnectionDisposition::Completed),
        result = tokio::time::timeout(shared.limits.handshake_timeout, local.next()) => result.map_err(|source| {
        LocalServerError::new(
            LocalServerErrorCategory::Timeout,
            LocalServerStage::Negotiate,
        )
        .with_source(source)
    })?,
    };
    let hello_bytes = match hello_message {
        Some(Ok(bytes)) => bytes,
        Some(Err(source)) => {
            return Err(transport_error(LocalServerStage::Negotiate).with_source(source));
        }
        None => {
            send_handshake_rejection(
                &mut local,
                shared.limits.write_timeout,
                handshake,
                HandshakeRejectionCategory::InvalidHandshake,
                false,
            )
            .await?;
            return Ok(ConnectionDisposition::Rejected);
        }
    };
    let hello = match handshake.decode_hello(&hello_bytes) {
        Ok(HandshakeDecodeStatus::Complete(frame)) if frame.consumed == hello_bytes.len() => {
            frame.message
        }
        _ => {
            send_handshake_rejection(
                &mut local,
                shared.limits.write_timeout,
                handshake,
                HandshakeRejectionCategory::InvalidHandshake,
                false,
            )
            .await?;
            return Ok(ConnectionDisposition::Rejected);
        }
    };
    let connection_id = ConnectionId::new(format!("local-{ordinal}"))
        .map_err(|source| server_error(LocalServerStage::ComposeConnection).with_source(source))?;
    let agreement =
        match negotiate_connection(&hello, &shared.policy, connection_id, shared.epoch.clone()) {
            Ok(agreement) => agreement,
            Err(error) => {
                send_handshake_rejection(
                    &mut local,
                    shared.limits.write_timeout,
                    handshake,
                    negotiation_rejection(&error),
                    false,
                )
                .await?;
                return Ok(ConnectionDisposition::Rejected);
            }
        };
    let scheduler = scheduler_limits(&agreement, shared.limits.scheduler_messages)?;
    let session = Arc::new(
        ServerConnectionSession::with_telemetry(
            hello.client_id().clone(),
            agreement.clone(),
            scheduler,
            shared.replay.clone(),
            shared.limits.diagnostics,
            shared.telemetry.clone(),
        )
        .map_err(|source| server_error(LocalServerStage::ComposeConnection).with_source(source))?,
    );
    // Reserve the logical client before sending Accepted so a duplicate live
    // client gets a handshake rejection. The lease also releases this reservation
    // if the acceptance write fails; see ../docs/internals/server.md#bind-admission-and-negotiation.
    let registration = match shared.connections.register_session(&session) {
        Ok(registration) => registration,
        Err(error) => {
            send_handshake_rejection(
                &mut local,
                shared.limits.write_timeout,
                handshake,
                server_rejection(&error),
                error.category() == ServerConnectionErrorCategory::Overloaded,
            )
            .await?;
            return Ok(ConnectionDisposition::Rejected);
        }
    };
    cleanup.attach(session.clone(), authentication.clone(), registration);
    let accepted = handshake
        .encode_response(&HandshakeResponse::Accepted(agreement.clone()))
        .map_err(|source| server_error(LocalServerStage::Negotiate).with_source(source))?;
    tokio::time::timeout(shared.limits.write_timeout, local.send(accepted.into()))
        .await
        .map_err(|e| transport_error(LocalServerStage::Negotiate).with_source(e))?
        .map_err(|source| transport_error(LocalServerStage::Negotiate).with_source(source))?;

    local
        .set_maximum(agreement.limits().max_frame_bytes() as usize)
        .map_err(|e| transport_error(LocalServerStage::Negotiate).with_source(e))?;
    cleanup.start_writer(ordinal)?;
    let mut interrupted_write = false;
    let mut close_dispatched = false;
    let mut result = Ok(());
    loop {
        let idle = tokio::time::sleep(Duration::from_millis(
            agreement.heartbeat().timeout_millis(),
        ));
        tokio::pin!(idle);
        tokio::select! {
            changed = shutdown.changed() => {
                let _ = changed;
                break;
            }
            recycled = disconnect.recv() => {
                let _ = recycled;
                break;
            }
            () = &mut idle => {
                result = Err(LocalServerError::new(
                    LocalServerErrorCategory::Timeout,
                    LocalServerStage::Dispatch,
                ));
                break;
            }
            pending = cleanup.receiver().recv() => {
                let Some(pending) = pending else { break; };
                let started = Instant::now();
                let sent = tokio::select! {
                    _ = shutdown.changed() => { interrupted_write = true; break; },
                    _ = disconnect.recv() => { interrupted_write = true; break; },
                    sent = tokio::time::timeout(shared.limits.write_timeout, local.send(pending.shared_bytes())) => sent,
                };
                session.record_connection_write_duration(started.elapsed());
                match sent {
                    Ok(Ok(())) => {
                        if let Err(source) = pending.confirm_written() {
                            result = Err(
                                server_error(LocalServerStage::Write).with_source(source)
                            );
                            break;
                        }
                    }
                    Ok(Err(source)) => {
                        result = Err(
                            transport_error(LocalServerStage::Write).with_source(source)
                        );
                        break;
                    }
                    Err(source) => {
                        result = Err(LocalServerError::new(
                            LocalServerErrorCategory::Timeout,
                            LocalServerStage::Write,
                        ).with_source(source));
                        break;
                    }
                }
            }
            incoming = local.next() => {
                match incoming {
                    Some(Ok(bytes)) => {
                        let message = match session.decode_client(&bytes) {
                            Ok(DecodeStatus::Complete(frame)) if frame.consumed == bytes.len() => frame.message,
                            Ok(_) => {
                                result = Err(LocalServerError::new(
                                    LocalServerErrorCategory::Protocol,
                                    LocalServerStage::Dispatch,
                                ));
                                break;
                            }
                            Err(source) => {
                                result = Err(server_connection_error(source));
                                break;
                            }
                        };
                        let admission = ServerAdmissionCorrelation::from_message(&message);
                        let actions = match session.accept(message) {
                            Ok(actions) => actions,
                            Err(error) => {
                                if let (Some(admission), Some(overload)) =
                                    (admission, error.overload())
                                {
                                    let rejection = admission.rejection(overload);
                                    match shared.dispatcher.reject_authenticated_admission(
                                        authentication.clone(),
                                        session.clone(),
                                        rejection,
                                    ) {
                                        Ok(true) => continue,
                                        Ok(false) => {
                                            result = Err(server_connection_error(error));
                                            break;
                                        }
                                        Err(source) => {
                                            result = Err(LocalServerError::new(
                                                LocalServerErrorCategory::Capacity,
                                                LocalServerStage::Dispatch,
                                            )
                                            .with_source(source));
                                            break;
                                        }
                                    }
                                }
                                result = Err(server_connection_error(error));
                                break;
                            }
                        };
                        for action in actions {
                            let closes = matches!(action, ServerAction::CloseConnection);
                            if closes {
                                cleanup.mark_close_dispatched();
                            }
                            if let Err(source) = shared.dispatcher.dispatch_authenticated(
                                authentication.clone(),
                                session.clone(),
                                action,
                            ) {
                                result = Err(
                                    server_error(LocalServerStage::Dispatch).with_source(source)
                                );
                                break;
                            }
                            if closes {
                                close_dispatched = true;
                                break;
                            }
                        }
                        if close_dispatched || result.is_err() {
                            break;
                        }
                    }
                    None => break,
                    Some(Err(source)) => {
                        result = Err(
                            transport_error(LocalServerStage::Dispatch).with_source(source)
                        );
                        break;
                    }
                }
            }
        }
    }

    if let Err(error) = cleanup.begin_close()
        && result.is_ok()
    {
        result = Err(error);
    }
    if result
        .as_ref()
        .err()
        .is_some_and(|error| error.category() == LocalServerErrorCategory::Capacity)
    {
        let _ = tokio::time::timeout(
            shared.limits.write_timeout,
            local.send(Arc::from(crate::CAPACITY_CLOSE_FRAME.as_slice())),
        )
        .await;
    }
    // Sink close flushes retained output. After an interrupted send, that would
    // resume the very write shutdown/recycle cancelled, without confirming it.
    // See ../docs/reference/framing.md#buffer-and-cancellation-behavior.
    if !interrupted_write && result.is_ok() {
        let _ = tokio::time::timeout(shared.limits.write_timeout, local.close()).await;
    }
    if let Err(error) = cleanup.finish().await
        && result.is_ok()
    {
        result = Err(error);
    }
    result.map(|()| ConnectionDisposition::Completed)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ServerAdmissionCorrelation {
    request_id: RequestId,
    response_kind: ServerAdmissionResponseKind,
}

impl ServerAdmissionCorrelation {
    fn from_message(message: &ClientMessage) -> Option<Self> {
        match message {
            ClientMessage::Request { request_id, .. } => Some(Self {
                request_id: request_id.clone(),
                response_kind: ServerAdmissionResponseKind::FiniteRequest,
            }),
            ClientMessage::OpenStream { request_id, .. }
            | ClientMessage::ResumeStream { request_id, .. } => Some(Self {
                request_id: request_id.clone(),
                response_kind: ServerAdmissionResponseKind::StreamOpen,
            }),
            ClientMessage::CancelRequest { .. }
            | ClientMessage::CloseStream { .. }
            | ClientMessage::WindowUpdate { .. }
            | ClientMessage::Ping
            | ClientMessage::Close => None,
        }
    }

    fn rejection(self, overload: MultiplexOverload) -> ServerAdmissionRejection {
        ServerAdmissionRejection::new(self.request_id, self.response_kind, overload)
    }
}

fn server_connection_error(error: ServerConnectionError) -> LocalServerError {
    let category = match error.category() {
        ServerConnectionErrorCategory::Configuration => LocalServerErrorCategory::Configuration,
        ServerConnectionErrorCategory::Protocol => LocalServerErrorCategory::Protocol,
        ServerConnectionErrorCategory::Overloaded => LocalServerErrorCategory::Capacity,
        ServerConnectionErrorCategory::Unavailable => LocalServerErrorCategory::Transport,
        ServerConnectionErrorCategory::InvariantViolation => LocalServerErrorCategory::Internal,
    };
    LocalServerError::new(category, LocalServerStage::Dispatch).with_source(error)
}

async fn send_handshake_rejection(
    local: &mut LocalFrameStream<LocalStream>,
    timeout: Duration,
    codec: HandshakeCodec,
    category: HandshakeRejectionCategory,
    retryable: bool,
) -> Result<(), LocalServerError> {
    let response = HandshakeResponse::Rejected(HandshakeRejection::new(category, retryable));
    let bytes = codec
        .encode_response(&response)
        .map_err(|source| server_error(LocalServerStage::Negotiate).with_source(source))?;
    tokio::time::timeout(timeout, local.send(bytes.into()))
        .await
        .map_err(|e| transport_error(LocalServerStage::Negotiate).with_source(e))?
        .map_err(|source| transport_error(LocalServerStage::Negotiate).with_source(source))?;
    tokio::time::timeout(timeout, local.close())
        .await
        .map_err(|e| transport_error(LocalServerStage::Negotiate).with_source(e))?
        .map_err(|source| transport_error(LocalServerStage::Negotiate).with_source(source))
}

// Reserve one maximum frame per class so bulk saturation cannot consume the
// space needed by control, lifecycle, or state traffic. These are queue caps,
// not service weights; see ../docs/reference/server-limits.md#scheduler-byte-budgets.
fn scheduler_limits(
    agreement: &ConnectionAgreement,
    messages: SchedulerMessageLimits,
) -> Result<SchedulerLimits, LocalServerError> {
    let limits = agreement.limits();
    let frame = u64::from(limits.max_frame_bytes());
    let reserved = frame.saturating_mul(4);
    if limits.connection_queue_bytes() < reserved || limits.stream_queue_bytes() < frame {
        return Err(server_error(LocalServerStage::ComposeConnection));
    }
    let units = limits.connection_queue_bytes().saturating_sub(reserved) / 8;
    let budgets = [
        frame.saturating_add(units.saturating_mul(2)),
        frame.saturating_add(units.saturating_mul(2)),
        frame.saturating_add(units),
        frame.saturating_add(units.saturating_mul(3)),
    ];
    let burst = frame
        .saturating_mul(4)
        .min(limits.stream_queue_bytes())
        .max(frame);
    SchedulerLimits::new(
        limits,
        budgets,
        limits.connection_queue_bytes(),
        burst,
        messages,
    )
    .map_err(|source| server_error(LocalServerStage::ComposeConnection).with_source(source))
}

fn negotiation_rejection(error: &ConnectionNegotiationError) -> HandshakeRejectionCategory {
    match error {
        ConnectionNegotiationError::IncompatibleVersions { .. } => {
            HandshakeRejectionCategory::IncompatibleVersion
        }
        ConnectionNegotiationError::MissingRequiredCapabilities { .. } => {
            HandshakeRejectionCategory::MissingCapability
        }
    }
}

fn server_rejection(error: &ServerConnectionError) -> HandshakeRejectionCategory {
    match error.category() {
        ServerConnectionErrorCategory::Overloaded => HandshakeRejectionCategory::CapacityExceeded,
        ServerConnectionErrorCategory::Protocol => HandshakeRejectionCategory::Conflict,
        ServerConnectionErrorCategory::Configuration
        | ServerConnectionErrorCategory::Unavailable
        | ServerConnectionErrorCategory::InvariantViolation => {
            HandshakeRejectionCategory::Unavailable
        }
    }
}

fn record_completion(
    completion: Result<
        (u64, Result<ConnectionDisposition, LocalServerError>),
        tokio::task::JoinError,
    >,
    report: &mut LocalServerRunReport,
    maximum: usize,
) {
    match completion {
        Ok((_, Ok(ConnectionDisposition::Completed))) => {
            report.completed = report.completed.saturating_add(1);
        }
        Ok((_, Ok(ConnectionDisposition::Rejected))) => {
            report.rejected = report.rejected.saturating_add(1);
        }
        Ok((ordinal, Err(error))) => report.record_failure(ordinal, error, maximum),
        Err(source) => report.record_failure(
            u64::MAX,
            server_error(LocalServerStage::SpawnWorker).with_source(source),
            maximum,
        ),
    }
}

const fn server_error(stage: LocalServerStage) -> LocalServerError {
    LocalServerError::new(LocalServerErrorCategory::Internal, stage)
}

const fn transport_error(stage: LocalServerStage) -> LocalServerError {
    LocalServerError::new(LocalServerErrorCategory::Transport, stage)
}
