// SPDX-License-Identifier: Apache-2.0

//! Authenticated local adapter for the blocking SDK runtime.
//!
//! Configuration validation performs no IO. Connection starts a dedicated Tokio
//! worker, delegates OS peer verification to transport-local, then exchanges UCHS.
//! Bounded queues bridge synchronous SDK workers and async socket/pipe IO. A
//! terminal inbound error has separate storage so a full queue cannot erase it.
//! Endpoint discovery and daemon startup remain caller responsibilities.

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc as tokio_mpsc, watch};
use undercurrent_transport_local::{LocalEndpoint, LocalFrameStream, connect_local};
use undercurrent_wire::ConnectionAgreement;
use undercurrent_wire::{
    HandshakeCodec, HandshakeDecodeStatus, HandshakeRejectionCategory, HandshakeResponse, WireCodec,
};

use crate::{
    ClientConnectionAgreement, ClientConnectionHello, ClientConnector, ClientOverload,
    ClientRecoveryCancellation, ClientRetryGuidance, ClientTransport, ClientTransportError,
    ClientTransportInbound, ClientTransportOperation, ClientTransportOutbound,
    NegotiatedClientTransport,
};

/// Bounded dial, handshake, message, and queue settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalClientLimits {
    handshake_frame_bytes: u32,
    connect_timeout: Duration,
    handshake_timeout: Duration,
    maximum_message_bytes: usize,
    outbound_queue_messages: usize,
    inbound_queue_messages: usize,
}

impl LocalClientLimits {
    /// Constructs explicit non-zero client transport limits.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        handshake_frame_bytes: u32,
        connect_timeout: Duration,
        handshake_timeout: Duration,
        maximum_message_bytes: usize,
        outbound_queue_messages: usize,
        inbound_queue_messages: usize,
    ) -> Result<Self, LocalClientConfigError> {
        if HandshakeCodec::new(handshake_frame_bytes).is_err() {
            return Err(LocalClientConfigError::InvalidHandshakeMaximum);
        }
        for (field, duration) in [
            ("connect_timeout", connect_timeout),
            ("handshake_timeout", handshake_timeout),
        ] {
            if duration.is_zero() {
                return Err(LocalClientConfigError::ZeroDuration { field });
            }
        }
        for (field, value) in [
            ("maximum_message_bytes", maximum_message_bytes),
            ("outbound_queue_messages", outbound_queue_messages),
            ("inbound_queue_messages", inbound_queue_messages),
        ] {
            if value == 0 {
                return Err(LocalClientConfigError::ZeroLimit { field });
            }
        }
        if maximum_message_bytes < usize::try_from(handshake_frame_bytes).unwrap_or(usize::MAX) {
            return Err(LocalClientConfigError::MessageBelowHandshake);
        }
        Ok(Self {
            handshake_frame_bytes,
            connect_timeout,
            handshake_timeout,
            maximum_message_bytes,
            outbound_queue_messages,
            inbound_queue_messages,
        })
    }

    /// Returns the maximum complete UCHS frame size.
    pub const fn handshake_frame_bytes(self) -> u32 {
        self.handshake_frame_bytes
    }

    /// Returns the complete local connection deadline.
    pub const fn connect_timeout(self) -> Duration {
        self.connect_timeout
    }

    /// Returns the UCHS exchange deadline after peer verification.
    pub const fn handshake_timeout(self) -> Duration {
        self.handshake_timeout
    }

    /// Returns the maximum complete Local data message size.
    pub const fn maximum_message_bytes(self) -> usize {
        self.maximum_message_bytes
    }

    /// Returns the bounded pending client-message count.
    pub const fn outbound_queue_messages(self) -> usize {
        self.outbound_queue_messages
    }

    /// Returns the bounded decoded server-message count.
    pub const fn inbound_queue_messages(self) -> usize {
        self.inbound_queue_messages
    }
}

/// Validated platform-local endpoint, client offer, and transport bounds.
#[derive(Debug, Clone)]
pub struct LocalClientConfig {
    endpoint: LocalEndpoint,
    hello: ClientConnectionHello,
    limits: LocalClientLimits,
}
impl LocalClientConfig {
    /// Validates an absolute platform-local address without performing IO.
    pub fn new(
        endpoint: impl Into<std::path::PathBuf>,
        hello: ClientConnectionHello,
        limits: LocalClientLimits,
    ) -> Result<Self, LocalClientConfigError> {
        let endpoint =
            LocalEndpoint::new(endpoint).map_err(|_| LocalClientConfigError::InvalidEndpoint)?;
        if usize::try_from(hello.limits().max_frame_bytes()).unwrap_or(usize::MAX)
            > limits.maximum_message_bytes
        {
            return Err(LocalClientConfigError::MessageBelowOffer);
        }
        Ok(Self {
            endpoint,
            hello,
            limits,
        })
    }
    /// Validates a platform-local address without connecting or accessing the filesystem.
    pub fn validate_endpoint(
        endpoint: impl Into<std::path::PathBuf>,
    ) -> Result<(), LocalClientConfigError> {
        LocalEndpoint::new(endpoint)
            .map(|_| ())
            .map_err(|_| LocalClientConfigError::InvalidEndpoint)
    }
    /// Returns the local address.
    pub fn endpoint(&self) -> &std::path::Path {
        self.endpoint.path()
    }
    /// Returns the offer sent after OS peer identity verification.
    pub const fn hello(&self) -> &ClientConnectionHello {
        &self.hello
    }
    /// Returns validated connection bounds.
    pub const fn limits(&self) -> LocalClientLimits {
        self.limits
    }
}

/// Invalid SDK Local adapter configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalClientConfigError {
    /// Endpoint is not a valid absolute local address.
    InvalidEndpoint,
    /// One duration was zero.
    ZeroDuration {
        /// Stable invalid field.
        field: &'static str,
    },
    /// One resource bound was zero.
    ZeroLimit {
        /// Stable invalid field.
        field: &'static str,
    },
    /// UCHS maximum cannot contain its fixed header.
    InvalidHandshakeMaximum,
    /// Local message maximum cannot contain one UCHS frame.
    MessageBelowHandshake,
    /// The offered UCMP frame exceeds the adapter message bound.
    MessageBelowOffer,
}

impl Display for LocalClientConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MessageBelowOffer => {
                formatter.write_str("message maximum is below offered UCMP maximum")
            }
            Self::InvalidEndpoint => formatter.write_str("invalid Local endpoint"),
            Self::ZeroDuration { field } => write!(formatter, "{field} must be non-zero"),
            Self::ZeroLimit { field } => write!(formatter, "{field} must be non-zero"),
            Self::InvalidHandshakeMaximum => {
                formatter.write_str("handshake frame maximum is invalid")
            }
            Self::MessageBelowHandshake => {
                formatter.write_str("message maximum is below handshake maximum")
            }
        }
    }
}

impl Error for LocalClientConfigError {}

/// Redaction-safe Local transport stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalTransportStage {
    /// Establish protected local IPC.
    Connect,
    /// Exchange and validate UCHS negotiation messages.
    Negotiate,
    /// OS peer identity verification failed.
    Authenticate,
    /// Encode or send one UCMP message.
    Send,
    /// Receive or decode one UCMP message.
    Receive,
    /// Close the physical Local.
    Close,
    /// Start the transport worker.
    SpawnWorker,
    /// A bounded adapter queue was saturated.
    Capacity,
    /// The attempt was cancelled.
    Cancelled,
}

/// Payload- and credential-redacted adapter failure.
#[derive(Clone)]
pub struct LocalTransportError {
    stage: LocalTransportStage,
    overload: Option<ClientOverload>,
    source: Option<Arc<dyn Error + Send + Sync>>,
}

impl LocalTransportError {
    const fn at(stage: LocalTransportStage) -> Self {
        Self {
            stage,
            overload: None,
            source: None,
        }
    }

    fn with_source(stage: LocalTransportStage, source: impl Error + Send + Sync + 'static) -> Self {
        let overload = overload_from_source(&source);
        Self {
            stage,
            overload,
            source: Some(Arc::new(source)),
        }
    }

    const fn capacity(
        resource: &'static str,
        limit: Option<u64>,
        retry: ClientRetryGuidance,
    ) -> Self {
        Self {
            stage: LocalTransportStage::Capacity,
            overload: Some(ClientOverload::new(resource, limit, limit, Some(1), retry)),
            source: None,
        }
    }

    const fn handshake_rejection(category: HandshakeRejectionCategory, retryable: bool) -> Self {
        let overload = if matches!(category, HandshakeRejectionCategory::CapacityExceeded) {
            Some(ClientOverload::new(
                "physical_connections",
                None,
                None,
                Some(1),
                if retryable {
                    ClientRetryGuidance::WithBackoff
                } else {
                    ClientRetryGuidance::DoNotRetryUnchanged
                },
            ))
        } else {
            None
        };
        Self {
            stage: LocalTransportStage::Negotiate,
            overload,
            source: None,
        }
    }

    /// Returns the stable failed transport stage.
    pub const fn stage(&self) -> LocalTransportStage {
        self.stage
    }

    /// Returns redacted admission or adapter-queue capacity context.
    pub const fn overload(&self) -> Option<ClientOverload> {
        self.overload
    }
}

fn overload_from_source(mut source: &(dyn Error + 'static)) -> Option<ClientOverload> {
    loop {
        if let Some(error) = source.downcast_ref::<LocalTransportError>()
            && let Some(overload) = error.overload()
        {
            return Some(overload);
        }
        if let Some(error) = source.downcast_ref::<ClientTransportError>()
            && let Some(overload) = error.overload()
        {
            return Some(overload);
        }
        source = source.source()?;
    }
}

impl Debug for LocalTransportError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalTransportError")
            .field("stage", &self.stage)
            .field("overload", &self.overload)
            .finish_non_exhaustive()
    }
}

impl PartialEq for LocalTransportError {
    fn eq(&self, other: &Self) -> bool {
        self.stage == other.stage && self.overload == other.overload
    }
}

impl Eq for LocalTransportError {}

impl Display for LocalTransportError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Local transport failed at {:?}", self.stage)
    }
}

impl Error for LocalTransportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

/// Typed connector for clients using the local transport.
#[derive(Debug, Clone)]
pub struct LocalClientConnector {
    config: LocalClientConfig,
}

impl LocalClientConnector {
    /// Constructs a connector without performing IO.
    pub const fn new(config: LocalClientConfig) -> Self {
        Self { config }
    }

    /// Returns the validated redaction-safe configuration.
    pub const fn config(&self) -> &LocalClientConfig {
        &self.config
    }
}

impl ClientConnector for LocalClientConnector {
    fn connect(
        &self,
        cancellation: &ClientRecoveryCancellation,
    ) -> Result<NegotiatedClientTransport, ClientTransportError> {
        if cancellation.is_cancelled() {
            return Err(client_error(
                ClientTransportOperation::Connect,
                LocalTransportStage::Cancelled,
            ));
        }
        let limits = self.config.limits;
        let (outbound_sender, outbound_receiver) =
            tokio_mpsc::channel(limits.outbound_queue_messages);
        let (inbound_sender, inbound_receiver) = mpsc::sync_channel(limits.inbound_queue_messages);
        let inbound_drained = Arc::new(tokio::sync::Notify::new());
        let worker_drained = inbound_drained.clone();
        let terminal_inbound_error = Arc::new(Mutex::new(None));
        let (startup_sender, startup_receiver) = mpsc::sync_channel(1);
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);
        let config = self.config.clone();
        let cancellation = cancellation.clone();
        let worker_terminal_error = terminal_inbound_error.clone();
        thread::Builder::new()
            .name("undercurrent-sdk-local".to_owned())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build();
                let runtime = match runtime {
                    Ok(runtime) => runtime,
                    Err(source) => {
                        let _ = startup_sender.send(Err(LocalTransportError::with_source(
                            LocalTransportStage::SpawnWorker,
                            source,
                        )));
                        return;
                    }
                };
                runtime.block_on(run_local(
                    config,
                    cancellation,
                    LocalWorkerChannels {
                        outbound: outbound_receiver,
                        inbound: inbound_sender,
                        inbound_drained: worker_drained,
                        terminal_inbound_error: worker_terminal_error,
                        shutdown: shutdown_receiver,
                        startup: startup_sender,
                    },
                ));
            })
            .map_err(|source| {
                client_error_from_source(
                    ClientTransportOperation::Connect,
                    LocalTransportError::with_source(LocalTransportStage::SpawnWorker, source),
                )
            })?;

        let startup_deadline = limits
            .connect_timeout
            .saturating_add(limits.handshake_timeout);
        let agreement = startup_receiver
            .recv_timeout(startup_deadline)
            .map_err(|source| {
                let _ = shutdown_sender.send(true);
                client_error_from_source(
                    ClientTransportOperation::Connect,
                    LocalTransportError::with_source(LocalTransportStage::Connect, source),
                )
            })?
            .map_err(|source| {
                client_error_from_source(ClientTransportOperation::Connect, source)
            })?;
        let agreement = ClientConnectionAgreement::try_from_raw(agreement).map_err(|source| {
            let _ = shutdown_sender.send(true);
            client_error_from_source(
                ClientTransportOperation::Connect,
                LocalTransportError::with_source(LocalTransportStage::Negotiate, source),
            )
        })?;
        let transport = Arc::new(LocalClientTransport {
            maximum_frame_bytes: agreement.limits().max_frame_bytes(),
            outbound_queue_messages: limits.outbound_queue_messages,
            outbound: outbound_sender,
            inbound: Mutex::new(inbound_receiver),
            inbound_drained,
            terminal_inbound_error,
            shutdown: shutdown_sender,
            closed: AtomicBool::new(false),
        });
        Ok(NegotiatedClientTransport::new(agreement, transport))
    }
}

struct LocalClientTransport {
    maximum_frame_bytes: u32,
    outbound_queue_messages: usize,
    outbound: tokio_mpsc::Sender<EncodedLocalOutbound>,
    inbound: Mutex<mpsc::Receiver<Result<ClientTransportInbound, LocalTransportError>>>,
    terminal_inbound_error: Arc<Mutex<Option<LocalTransportError>>>,
    inbound_drained: Arc<tokio::sync::Notify>,
    shutdown: watch::Sender<bool>,
    closed: AtomicBool,
}

struct EncodedLocalOutbound {
    bytes: Vec<u8>,
    closes_connection: bool,
}

impl ClientTransport for LocalClientTransport {
    fn send(&self, message: ClientTransportOutbound) -> Result<(), ClientTransportError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(client_error(
                ClientTransportOperation::Send,
                LocalTransportStage::Close,
            ));
        }
        let bytes = message.encode(self.maximum_frame_bytes).map_err(|source| {
            client_error_from_source(
                ClientTransportOperation::Send,
                LocalTransportError::with_source(LocalTransportStage::Send, source),
            )
        })?;
        let message = EncodedLocalOutbound {
            bytes,
            closes_connection: message.closes_connection(),
        };
        self.outbound.try_send(message).map_err(|error| {
            let source = match error {
                tokio_mpsc::error::TrySendError::Full(_) => LocalTransportError::capacity(
                    "local_outbound_queue_messages",
                    Some(usize_as_u64(self.outbound_queue_messages)),
                    ClientRetryGuidance::WithBackoff,
                ),
                tokio_mpsc::error::TrySendError::Closed(_) => {
                    LocalTransportError::at(LocalTransportStage::Close)
                }
            };
            client_error_from_source(ClientTransportOperation::Send, source)
        })
    }

    fn receive(&self) -> Result<ClientTransportInbound, ClientTransportError> {
        let result = receive_inbound(
            &self
                .inbound
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            &self.terminal_inbound_error,
        )
        .map_err(|source| client_error_from_source(ClientTransportOperation::Receive, source));
        self.inbound_drained.notify_one();
        result
    }

    fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            let _ = self.shutdown.send(true);
        }
    }
}

impl Drop for LocalClientTransport {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

struct LocalWorkerChannels {
    outbound: tokio_mpsc::Receiver<EncodedLocalOutbound>,
    inbound: mpsc::SyncSender<Result<ClientTransportInbound, LocalTransportError>>,
    inbound_drained: Arc<tokio::sync::Notify>,
    terminal_inbound_error: Arc<Mutex<Option<LocalTransportError>>>,
    shutdown: watch::Receiver<bool>,
    startup: mpsc::SyncSender<Result<ConnectionAgreement, LocalTransportError>>,
}

async fn run_local(
    config: LocalClientConfig,
    cancellation: ClientRecoveryCancellation,
    channels: LocalWorkerChannels,
) {
    let LocalWorkerChannels {
        mut outbound,
        inbound,
        inbound_drained,
        terminal_inbound_error,
        mut shutdown,
        startup,
    } = channels;
    let mut opening_shutdown = shutdown.clone();
    let opening = open_local(&config, &cancellation, &mut opening_shutdown);
    tokio::pin!(opening);
    let deadline = tokio::time::sleep(
        config
            .limits
            .connect_timeout
            .saturating_add(config.limits.handshake_timeout),
    );
    tokio::pin!(deadline);
    let mut cancellation_poll = tokio::time::interval(Duration::from_millis(20));
    let result = loop {
        tokio::select! {
            result = &mut opening => break result,
            _ = shutdown.changed() => break Err(LocalTransportError::at(LocalTransportStage::Cancelled)),
            () = &mut deadline => break Err(LocalTransportError::at(LocalTransportStage::Negotiate)),
            _ = cancellation_poll.tick() => if cancellation.is_cancelled() {
                break Err(LocalTransportError::at(LocalTransportStage::Cancelled));
            },
        }
    };
    let (mut local, agreement, codec) = match result {
        Ok(opened) => opened,
        Err(error) => {
            let _ = startup.send(Err(error));
            return;
        }
    };
    if startup.send(Ok(agreement)).is_err() {
        let _ = local.close().await;
        return;
    }

    let mut pending_inbound: Option<Result<ClientTransportInbound, LocalTransportError>> = None;
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                let _ = changed;
                break;
            }
            message = outbound.recv() => {
                let Some(message) = message else {
                    break;
                };
                let sent = tokio::select! {
                    _ = shutdown.changed() => break,
                    sent = tokio::time::timeout(config.limits.handshake_timeout, local.send(message.bytes.into())) => {
                        match sent { Ok(result) => result, Err(_) => Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "local write deadline exceeded")) }
                    }
                };
                if let Err(source) = sent {
                    send_inbound_error(
                        &inbound,
                        &terminal_inbound_error,
                        LocalTransportError::with_source(
                            LocalTransportStage::Send,
                            source,
                        ),
                    );
                    break;
                }
                if message.closes_connection {
                    continue;
                }
            }
            _ = inbound_drained.notified(), if pending_inbound.is_some() => {
                let frame = pending_inbound.take().unwrap();
                let closes = frame.as_ref().is_ok_and(ClientTransportInbound::closes_connection);
                match inbound.try_send(frame) {
                    Ok(()) => if closes { break; },
                    Err(mpsc::TrySendError::Full(frame)) => pending_inbound = Some(frame),
                    Err(mpsc::TrySendError::Disconnected(_)) => break,
                }
            }
            received = local.next(), if pending_inbound.is_none() => {
                match received {
                    Some(Ok(bytes)) => {
                        if bytes.as_ref() == undercurrent_transport_local::CAPACITY_CLOSE_FRAME {
                            send_inbound_error(&inbound, &terminal_inbound_error, LocalTransportError::capacity("server_connection_capacity", None, ClientRetryGuidance::WithBackoff));
                            break;
                        }
                        match ClientTransportInbound::decode(&bytes, codec.max_frame_bytes()) {
                            Ok(frame) => {
                                let closes = frame.closes_connection();
                                match inbound.try_send(Ok(frame)) {
                                    Ok(()) => {}
                                    Err(mpsc::TrySendError::Full(frame)) => {
                                        pending_inbound = Some(frame);
                                    }
                                    Err(mpsc::TrySendError::Disconnected(_)) => break,
                                }
                                if closes && pending_inbound.is_none() {
                                    break;
                                }
                            }
                            Err(source) => {
                                send_inbound_error(
                                    &inbound,
                                    &terminal_inbound_error,
                                    LocalTransportError::with_source(
                                        LocalTransportStage::Receive,
                                        source,
                                    ),
                                );
                                break;
                            }
                        }
                    }
                    None => break,
                    Some(Err(source)) => {
                        send_inbound_error(
                            &inbound,
                            &terminal_inbound_error,
                            LocalTransportError::with_source(
                                LocalTransportStage::Receive,
                                source,
                            ),
                        );
                        break;
                    }

                }
            }
        }
    }
}

async fn open_local(
    config: &LocalClientConfig,
    cancellation: &ClientRecoveryCancellation,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<
    (
        LocalFrameStream<undercurrent_transport_local::LocalStream>,
        ConnectionAgreement,
        WireCodec,
    ),
    LocalTransportError,
> {
    let connect = connect_local(&config.endpoint);
    tokio::pin!(connect);
    let timeout = tokio::time::sleep(config.limits.connect_timeout);
    tokio::pin!(timeout);
    let mut cancellation_poll = tokio::time::interval(Duration::from_millis(20));
    let stream = loop {
        tokio::select! {
            result = &mut connect => break result.map_err(|e| LocalTransportError::with_source(LocalTransportStage::Connect, e))?,
            () = &mut timeout => return Err(LocalTransportError::at(LocalTransportStage::Connect)),
            _ = cancellation_poll.tick() => if cancellation.is_cancelled() { return Err(LocalTransportError::at(LocalTransportStage::Cancelled)); },
            _ = shutdown.changed() => return Err(LocalTransportError::at(LocalTransportStage::Cancelled)),
        }
    };
    let mut local = LocalFrameStream::new(stream, config.limits.handshake_frame_bytes as usize)
        .map_err(|e| LocalTransportError::with_source(LocalTransportStage::Negotiate, e))?;
    let handshake = HandshakeCodec::new(config.limits.handshake_frame_bytes).map_err(|source| {
        LocalTransportError::with_source(LocalTransportStage::Negotiate, source)
    })?;
    let hello = handshake
        .encode_hello(config.hello.as_raw())
        .map_err(|source| {
            LocalTransportError::with_source(LocalTransportStage::Negotiate, source)
        })?;
    local.send(hello.into()).await.map_err(|source| {
        LocalTransportError::with_source(LocalTransportStage::Negotiate, source)
    })?;
    let response = tokio::time::timeout(config.limits.handshake_timeout, local.next())
        .await
        .map_err(|source| LocalTransportError::with_source(LocalTransportStage::Negotiate, source))?
        .ok_or_else(|| LocalTransportError::at(LocalTransportStage::Negotiate))?
        .map_err(|source| {
            LocalTransportError::with_source(LocalTransportStage::Negotiate, source)
        })?;
    let agreement = match handshake.decode_response(&response).map_err(|source| {
        LocalTransportError::with_source(LocalTransportStage::Negotiate, source)
    })? {
        HandshakeDecodeStatus::Complete(frame) if frame.consumed == response.len() => {
            match frame.message {
                HandshakeResponse::Accepted(agreement) => agreement,
                HandshakeResponse::Rejected(rejection) => {
                    return Err(LocalTransportError::handshake_rejection(
                        rejection.category(),
                        rejection.retryable(),
                    ));
                }
            }
        }
        HandshakeDecodeStatus::Complete(_) | HandshakeDecodeStatus::NeedMore { .. } => {
            return Err(LocalTransportError::at(LocalTransportStage::Negotiate));
        }
    };
    let codec = WireCodec::new(agreement.limits().max_frame_bytes()).map_err(|source| {
        LocalTransportError::with_source(LocalTransportStage::Negotiate, source)
    })?;
    if codec.max_frame_bytes() as usize > config.limits.maximum_message_bytes {
        return Err(LocalTransportError::at(LocalTransportStage::Negotiate));
    }
    local
        .set_maximum(codec.max_frame_bytes() as usize)
        .map_err(|e| LocalTransportError::with_source(LocalTransportStage::Negotiate, e))?;
    Ok((local, agreement, codec))
}

fn send_inbound_error(
    inbound: &mpsc::SyncSender<Result<ClientTransportInbound, LocalTransportError>>,
    terminal: &Mutex<Option<LocalTransportError>>,
    error: LocalTransportError,
) {
    match inbound.try_send(Err(error)) {
        Ok(()) => {}
        Err(mpsc::TrySendError::Full(Err(error)))
        | Err(mpsc::TrySendError::Disconnected(Err(error))) => {
            store_terminal_inbound_error(terminal, error);
        }
        Err(mpsc::TrySendError::Full(Ok(_))) | Err(mpsc::TrySendError::Disconnected(Ok(_))) => {}
    }
}

fn store_terminal_inbound_error(
    terminal: &Mutex<Option<LocalTransportError>>,
    error: LocalTransportError,
) {
    let mut terminal = terminal
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if terminal.is_none() {
        *terminal = Some(error);
    }
}

fn receive_inbound(
    inbound: &mpsc::Receiver<Result<ClientTransportInbound, LocalTransportError>>,
    terminal: &Mutex<Option<LocalTransportError>>,
) -> Result<ClientTransportInbound, LocalTransportError> {
    // The terminal slot holds a failure that could not enter the bounded queue.
    // Drain accepted frames first; reporting that failure early would discard
    // replies and stream items that preceded it. Worker exit drops the sender.
    // See [adapter pressure](../../../docs/sdk/rust/limits-and-diagnostics.md#profile-variants-and-custom-limits).
    match inbound.recv() {
        Ok(result) => result,
        Err(source) => Err(terminal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .unwrap_or_else(|| {
                LocalTransportError::with_source(LocalTransportStage::Close, source)
            })),
    }
}

fn client_error(
    operation: ClientTransportOperation,
    stage: LocalTransportStage,
) -> ClientTransportError {
    client_error_from_source(operation, LocalTransportError::at(stage))
}

fn client_error_from_source(
    operation: ClientTransportOperation,
    source: LocalTransportError,
) -> ClientTransportError {
    match source.overload() {
        Some(overload) => ClientTransportError::with_overload(operation, source, overload),
        None => ClientTransportError::new(operation, source),
    }
}

fn usize_as_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use undercurrent_multiplex::ClientMessage;

    #[test]
    fn capacity_rejection_and_adapter_queue_expose_safe_retry_guidance() {
        let admission = client_error_from_source(
            ClientTransportOperation::Connect,
            LocalTransportError::handshake_rejection(
                HandshakeRejectionCategory::CapacityExceeded,
                true,
            ),
        )
        .overload()
        .expect("capacity rejection context reaches the connector caller");
        assert_eq!(admission.resource(), "physical_connections");
        assert_eq!(admission.limit(), None);
        assert_eq!(admission.requested(), Some(1));
        assert_eq!(admission.retry(), ClientRetryGuidance::WithBackoff);

        let queue = LocalTransportError::capacity(
            "local_outbound_queue_messages",
            Some(8),
            ClientRetryGuidance::WithBackoff,
        )
        .overload()
        .expect("queue context");
        assert_eq!(queue.limit(), Some(8));
        assert_eq!(queue.current(), Some(8));
        assert_eq!(queue.retry(), ClientRetryGuidance::WithBackoff);
    }

    #[test]
    fn real_wire_codec_error_is_retained_but_redacted_by_transport_wrappers() {
        let (outbound, _outbound_receiver) = tokio_mpsc::channel(1);
        let (_inbound_sender, inbound) = mpsc::sync_channel(1);
        let terminal_inbound_error = Arc::new(Mutex::new(None));
        let (shutdown, _shutdown_receiver) = watch::channel(false);
        let transport = LocalClientTransport {
            maximum_frame_bytes: 1,
            outbound_queue_messages: 1,
            outbound,
            inbound: Mutex::new(inbound),
            inbound_drained: Arc::new(tokio::sync::Notify::new()),
            terminal_inbound_error,
            shutdown,
            closed: AtomicBool::new(false),
        };

        let error = transport
            .send(ClientTransportOutbound::from_raw(ClientMessage::Ping))
            .expect_err("one byte cannot contain a UCMP frame");
        let local = error
            .source()
            .and_then(|source| source.downcast_ref::<LocalTransportError>())
            .expect("typed Local source");
        assert_eq!(local.stage(), LocalTransportStage::Send);
        assert!(
            local
                .source()
                .and_then(|source| source.source())
                .and_then(|source| source.downcast_ref::<undercurrent_wire::WireError>())
                .is_some(),
            "real wire codec source remains available"
        );
        assert!(!error.to_string().contains("frame"));
        assert!(!format!("{error:?}").contains("frame"));
    }

    #[test]
    fn outbound_queue_owns_exact_wire_bytes_and_close_semantics() {
        let (outbound, mut receiver) = tokio_mpsc::channel(2);
        let (_inbound_sender, inbound) = mpsc::sync_channel(1);
        let terminal_inbound_error = Arc::new(Mutex::new(None));
        let (shutdown, _shutdown_receiver) = watch::channel(false);
        let transport = LocalClientTransport {
            maximum_frame_bytes: 1_024,
            outbound_queue_messages: 2,
            outbound,
            inbound: Mutex::new(inbound),
            inbound_drained: Arc::new(tokio::sync::Notify::new()),
            terminal_inbound_error,
            shutdown,
            closed: AtomicBool::new(false),
        };

        let ping = ClientTransportOutbound::from_raw(ClientMessage::Ping);
        let expected_ping = ping.encode(1_024).expect("encode expected ping");
        transport.send(ping).expect("enqueue ping");
        let queued_ping = receiver.try_recv().expect("receive encoded ping");
        assert_eq!(queued_ping.bytes, expected_ping);
        assert!(!queued_ping.closes_connection);

        let close = ClientTransportOutbound::from_raw(ClientMessage::Close);
        let expected_close = close.encode(1_024).expect("encode expected close");
        transport.send(close).expect("enqueue close");
        let queued_close = receiver.try_recv().expect("receive encoded close");
        assert_eq!(queued_close.bytes, expected_close);
        assert!(queued_close.closes_connection);
    }

    #[test]
    fn invalid_outbound_precedes_full_queue_and_closed_precedes_encoding() {
        let (outbound, _receiver) = tokio_mpsc::channel(1);
        outbound
            .try_send(EncodedLocalOutbound {
                bytes: vec![0],
                closes_connection: false,
            })
            .expect("fill outbound queue");
        let (_inbound_sender, inbound) = mpsc::sync_channel(1);
        let terminal_inbound_error = Arc::new(Mutex::new(None));
        let (shutdown, _shutdown_receiver) = watch::channel(false);
        let transport = LocalClientTransport {
            maximum_frame_bytes: 1,
            outbound_queue_messages: 1,
            outbound,
            inbound: Mutex::new(inbound),
            inbound_drained: Arc::new(tokio::sync::Notify::new()),
            terminal_inbound_error,
            shutdown,
            closed: AtomicBool::new(false),
        };

        let invalid = transport
            .send(ClientTransportOutbound::from_raw(ClientMessage::Ping))
            .expect_err("invalid frame precedes full queue");
        let invalid_local = invalid
            .source()
            .and_then(|source| source.downcast_ref::<LocalTransportError>())
            .expect("typed invalid-frame source");
        assert_eq!(invalid_local.stage(), LocalTransportStage::Send);
        assert_eq!(invalid_local.overload(), None);

        transport.closed.store(true, Ordering::Release);
        let closed = transport
            .send(ClientTransportOutbound::from_raw(ClientMessage::Ping))
            .expect_err("closed transport precedes invalid frame");
        let closed_local = closed
            .source()
            .and_then(|source| source.downcast_ref::<LocalTransportError>())
            .expect("typed closed source");
        assert_eq!(closed_local.stage(), LocalTransportStage::Close);
        assert_eq!(closed_local.overload(), None);
    }

    #[test]
    fn full_and_closed_outbound_queues_keep_typed_errors() {
        let (outbound, receiver) = tokio_mpsc::channel(1);
        let (_inbound_sender, inbound) = mpsc::sync_channel(1);
        let terminal_inbound_error = Arc::new(Mutex::new(None));
        let (shutdown, _shutdown_receiver) = watch::channel(false);
        let transport = LocalClientTransport {
            maximum_frame_bytes: 1_024,
            outbound_queue_messages: 1,
            outbound,
            inbound: Mutex::new(inbound),
            inbound_drained: Arc::new(tokio::sync::Notify::new()),
            terminal_inbound_error,
            shutdown,
            closed: AtomicBool::new(false),
        };

        transport
            .send(ClientTransportOutbound::from_raw(ClientMessage::Ping))
            .expect("fill outbound queue");
        let full = transport
            .send(ClientTransportOutbound::from_raw(ClientMessage::Ping))
            .expect_err("bounded outbound queue is full");
        let overload = full.overload().expect("typed outbound overload");
        assert_eq!(overload.resource(), "local_outbound_queue_messages");
        assert_eq!(overload.limit(), Some(1));
        assert_eq!(overload.current(), Some(1));
        assert_eq!(overload.retry(), ClientRetryGuidance::WithBackoff);

        drop(receiver);
        let closed = transport
            .send(ClientTransportOutbound::from_raw(ClientMessage::Close))
            .expect_err("closed outbound queue rejects close");
        let local = closed
            .source()
            .and_then(|source| source.downcast_ref::<LocalTransportError>())
            .expect("typed closed-queue source");
        assert_eq!(local.stage(), LocalTransportStage::Close);
        assert_eq!(local.overload(), None);
    }

    #[test]
    fn full_inbound_queue_retains_terminal_overload_out_of_band() {
        let (inbound, receiver) = mpsc::sync_channel(1);
        let terminal = Mutex::new(None);
        inbound
            .send(Ok(ClientTransportInbound::pong()))
            .expect("fill inbound queue");
        send_inbound_error(
            &inbound,
            &terminal,
            LocalTransportError::capacity(
                "local_inbound_queue_messages",
                Some(1),
                ClientRetryGuidance::WithBackoff,
            ),
        );
        drop(inbound);

        receive_inbound(&receiver, &terminal).expect("queued frame is delivered first");
        let error = receive_inbound(&receiver, &terminal)
            .expect_err("terminal overload follows the drained queue");
        let overload = error.overload().expect("typed terminal overload");
        assert_eq!(overload.resource(), "local_inbound_queue_messages");
        assert_eq!(overload.limit(), Some(1));
        assert_eq!(overload.retry(), ClientRetryGuidance::WithBackoff);
    }
}
