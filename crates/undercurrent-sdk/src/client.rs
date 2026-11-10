// SPDX-License-Identifier: Apache-2.0

//! One blocking physical client with shared reader, writer, and heartbeat workers.
//!
//! Routes are registered before queue admission to handle fast responses. Each
//! stream has a bounded mailbox; mailbox overflow closes only that logical stream
//! without advancing its consumed checkpoint. If its close cannot be queued, the
//! physical connection is closed. Recovery ownership is a separate
//! layer in `resilient`; this layer never reconnects or replays finite requests.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use undercurrent_multiplex::{
    ClientMessage, ConnectionRouter, InboundStream, InboundStreamError, InboundStreamItem,
    MultiplexError, PendingResponse, ServerMessage,
};
use undercurrent_wire::{
    ConnectionAgreement, RequestId, StreamId, StreamSequence, TraceContext, connection_capabilities,
};

use crate::{
    BoundedClientTelemetry, ClientConnectionAgreement, ClientDiagnosticName, ClientError,
    ClientMetricName, ClientProtocolVersion, ClientServerEpoch, ClientStreamId, ClientStreamItem,
    ClientStreamReceiveError, ClientStreamSequence, ClientTelemetry, ClientTraceContext,
    ClientTransport, ClientTransportError, ClientTransportOutbound, TraceContextGenerationError,
    TraceContextGenerator,
};

/// Invalid SDK runtime bounds relative to the negotiated connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientLimitError {
    /// A required bound was zero.
    ZeroLimit {
        /// Stable invalid field.
        field: &'static str,
    },
    /// Product payload bound exceeded the negotiated frame maximum.
    PayloadExceedsFrame,
    /// Client stream mailbox exceeded the negotiated stream queue.
    MailboxExceedsStreamQueue,
}

impl Display for ClientLimitError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroLimit { field } => write!(formatter, "client limit {field} must be non-zero"),
            Self::PayloadExceedsFrame => {
                formatter.write_str("client payload bytes exceed negotiated frame bytes")
            }
            Self::MailboxExceedsStreamQueue => {
                formatter.write_str("client mailbox bytes exceed negotiated stream queue bytes")
            }
        }
    }
}

impl Error for ClientLimitError {}

/// Explicit bounded memory limits for one SDK physical connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientRuntimeLimits {
    max_payload_bytes: u32,
    writer_queue_messages: usize,
    stream_mailbox_messages: usize,
    stream_mailbox_bytes: u64,
}

impl ClientRuntimeLimits {
    /// Constructs non-zero SDK-local limits.
    ///
    /// [`Client::connect`] reduces these ceilings to a smaller negotiated peer.
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed zero-limit failure.
    pub const fn new(
        max_payload_bytes: u32,
        writer_queue_messages: usize,
        stream_mailbox_messages: usize,
        stream_mailbox_bytes: u64,
    ) -> Result<Self, ClientLimitError> {
        if max_payload_bytes == 0 {
            return Err(ClientLimitError::ZeroLimit {
                field: "max_payload_bytes",
            });
        }
        if writer_queue_messages == 0 {
            return Err(ClientLimitError::ZeroLimit {
                field: "writer_queue_messages",
            });
        }
        if stream_mailbox_messages == 0 {
            return Err(ClientLimitError::ZeroLimit {
                field: "stream_mailbox_messages",
            });
        }
        if stream_mailbox_bytes == 0 {
            return Err(ClientLimitError::ZeroLimit {
                field: "stream_mailbox_bytes",
            });
        }
        Ok(Self {
            max_payload_bytes,
            writer_queue_messages,
            stream_mailbox_messages,
            stream_mailbox_bytes,
        })
    }

    /// Returns maximum product payload before transport framing overhead.
    pub const fn max_payload_bytes(self) -> u32 {
        self.max_payload_bytes
    }

    /// Returns bounded semantic messages waiting for the one writer.
    pub const fn writer_queue_messages(self) -> usize {
        self.writer_queue_messages
    }

    /// Returns maximum retained items in one subscription mailbox.
    pub const fn stream_mailbox_messages(self) -> usize {
        self.stream_mailbox_messages
    }

    /// Returns maximum encoded bytes retained in one subscription mailbox.
    pub const fn stream_mailbox_bytes(self) -> u64 {
        self.stream_mailbox_bytes
    }

    fn validate(self, agreement: &ConnectionAgreement) -> Result<(), ClientLimitError> {
        if self.max_payload_bytes > agreement.limits().max_frame_bytes() {
            return Err(ClientLimitError::PayloadExceedsFrame);
        }
        if self.stream_mailbox_bytes > agreement.limits().stream_queue_bytes() {
            return Err(ClientLimitError::MailboxExceedsStreamQueue);
        }
        Ok(())
    }

    fn negotiated(mut self, agreement: &ConnectionAgreement) -> Result<Self, ClientLimitError> {
        if let Some(payload_bytes) = agreement
            .limits()
            .max_frame_bytes()
            .checked_sub(undercurrent_wire::MAX_CLIENT_FRAME_ENVELOPE_BYTES)
            .filter(|bytes| *bytes > 0)
        {
            // A larger local offer must still connect to an older daemon. Reserve
            // the full envelope so overlarge calls fail locally before enqueue.
            self.max_payload_bytes = self.max_payload_bytes.min(payload_bytes);
        }
        self.stream_mailbox_bytes = self
            .stream_mailbox_bytes
            .min(agreement.limits().stream_queue_bytes());
        self.validate(agreement)?;
        Ok(self)
    }
}

/// Cheaply clonable SDK client sharing one reader, writer, and physical transport.
#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

enum WriterCommand {
    Message(ClientMessage),
    Shutdown,
}

impl Client {
    /// Starts one bounded reader, writer, and heartbeat timer around a transport.
    ///
    /// All clones and subscriptions retain the same connection. Construction
    /// spawns three named worker threads, performs no socket setup itself, and
    /// does not panic.
    /// Payload and mailbox ceilings are reduced to fit a smaller negotiated
    /// peer; normal standalone frames reserve the maximum client envelope.
    ///
    /// # Errors
    ///
    /// Returns limits incompatible with the negotiated connection or worker
    /// thread creation failure.
    pub fn connect(
        agreement: ClientConnectionAgreement,
        transport: Arc<dyn ClientTransport>,
        limits: ClientRuntimeLimits,
    ) -> Result<Self, ClientConnectError> {
        let telemetry = Arc::new(BoundedClientTelemetry::default());
        Self::connect_inner(
            agreement.into_raw(),
            transport,
            limits,
            telemetry.clone(),
            Some(telemetry),
        )
    }

    /// Starts one shared physical client with caller-owned telemetry.
    ///
    /// The telemetry implementation must be non-blocking and payload-safe.
    /// Connection behavior and failure semantics match [`Self::connect`].
    ///
    /// # Errors
    ///
    /// Returns limits incompatible with the negotiated connection or worker
    /// thread creation failure.
    pub fn connect_with_telemetry(
        agreement: ClientConnectionAgreement,
        transport: Arc<dyn ClientTransport>,
        limits: ClientRuntimeLimits,
        telemetry: Arc<dyn ClientTelemetry>,
    ) -> Result<Self, ClientConnectError> {
        Self::connect_inner(agreement.into_raw(), transport, limits, telemetry, None)
    }

    fn connect_inner(
        agreement: ConnectionAgreement,
        transport: Arc<dyn ClientTransport>,
        limits: ClientRuntimeLimits,
        telemetry: Arc<dyn ClientTelemetry>,
        default_telemetry: Option<Arc<BoundedClientTelemetry>>,
    ) -> Result<Self, ClientConnectError> {
        let protocol_version = agreement.protocol_version();
        if protocol_version.major != ClientProtocolVersion::CURRENT.major
            || protocol_version.minor != ClientProtocolVersion::CURRENT.minor
        {
            return Err(ClientConnectError::UnsupportedProtocolVersion {
                version: ClientProtocolVersion::new(protocol_version.major, protocol_version.minor),
            });
        }
        for capability in [
            connection_capabilities::MULTIPLEX,
            connection_capabilities::FLOW_CONTROL,
            connection_capabilities::TRACE_CONTEXT,
        ] {
            if !agreement
                .capabilities()
                .iter()
                .any(|enabled| enabled.as_str() == capability)
            {
                return Err(ClientConnectError::MissingRequiredCapability { capability });
            }
        }
        let limits = limits
            .negotiated(&agreement)
            .map_err(ClientConnectError::Limits)?;
        let traces = TraceContextGenerator::secure().map_err(ClientConnectError::TraceContext)?;
        let router = ConnectionRouter::new(agreement.limits());
        let (writer_tx, writer_rx) = mpsc::sync_channel(limits.writer_queue_messages());
        let heartbeat = Arc::new(HeartbeatState::new());
        let inner = Arc::new(ClientInner {
            agreement,
            limits,
            transport: transport.clone(),
            router: router.clone(),
            writer_tx,
            writer_admission: Mutex::new(()),
            deferred_credits: Mutex::new(std::collections::BTreeMap::new()),
            writer_queued_messages: AtomicUsize::new(0),
            next_request: AtomicU64::new(0),
            next_stream: AtomicU64::new(0),
            traces,
            closed: AtomicBool::new(false),
            transport_failure: Mutex::new(None),
            multiplex_failure: Mutex::new(None),
            close_wait: Mutex::new(()),
            close_changed: Condvar::new(),
            #[cfg(test)]
            before_close_wait: Mutex::new(None),
            heartbeat: heartbeat.clone(),
            workers: Mutex::new(Vec::new()),
            telemetry,
            default_telemetry,
            pending_requests: AtomicUsize::new(0),
            logical_subscriptions: AtomicUsize::new(0),
        });

        let writer_inner = Arc::downgrade(&inner);
        let writer_transport = transport.clone();
        let writer = thread::Builder::new()
            .name("undercurrent-sdk-writer".into())
            .spawn(move || {
                while let Ok(command) = writer_rx.recv() {
                    let message = match command {
                        WriterCommand::Shutdown => break,
                        WriterCommand::Message(message) => message,
                    };
                    let Some(inner) = writer_inner.upgrade() else {
                        break;
                    };
                    inner.writer_message_dequeued();
                    if inner.closed.load(Ordering::Acquire) {
                        break;
                    }
                    if let Err(error) = writer_transport
                        .send(ClientTransportOutbound::from_raw(message))
                        .and_then(|()| inner.flush_deferred_credits())
                    {
                        inner.fail_connection_with_transport(error);
                        break;
                    }
                }
            })
            .map_err(ClientConnectError::WriterThread)?;

        let reader_inner = Arc::downgrade(&inner);
        let reader_transport = transport.clone();
        let reader = match thread::Builder::new()
            .name("undercurrent-sdk-reader".into())
            .spawn(move || {
                loop {
                    match reader_transport.receive().map(|message| message.into_raw()) {
                        Ok(ServerMessage::Response {
                            request_id,
                            payload,
                        }) => {
                            if let Some(inner) = reader_inner.upgrade() {
                                inner.heartbeat.observed();
                            }
                            match router.route_response(&request_id, payload) {
                                Ok(()) => {
                                    if let Some(inner) = reader_inner.upgrade() {
                                        inner
                                            .telemetry
                                            .emit_diagnostic(ClientDiagnosticName::ResponseRouted);
                                    }
                                }
                                Err(MultiplexError::UnknownRequest { .. }) => {
                                    if let Some(inner) = reader_inner.upgrade() {
                                        inner.telemetry.emit_diagnostic(
                                            ClientDiagnosticName::LateResponseIgnored,
                                        );
                                    }
                                }
                                Err(_) => {}
                            }
                        }
                        Ok(ServerMessage::StreamItem { stream_id, item }) => {
                            if let Some(inner) = reader_inner.upgrade() {
                                inner.heartbeat.observed();
                            }
                            match router.route_stream_item_with_queued_bytes(&stream_id, item) {
                                Err(error) => {
                                    if matches!(error, MultiplexError::UnknownStream { .. }) {
                                        continue;
                                    }
                                    if let Some(inner) = reader_inner.upgrade() {
                                        if matches!(
                                            error,
                                            MultiplexError::QueueSaturated {
                                                queue: "client_stream_mailbox",
                                                ..
                                            }
                                        ) {
                                            // Stop the offending route rather than resetting every
                                            // terminal and finite request on this connection.
                                            if router.overload_stream(&stream_id).is_ok()
                                                && inner
                                                    .try_enqueue(ClientMessage::CloseStream {
                                                        stream_id,
                                                    })
                                                    .is_err()
                                            {
                                                inner.fail_connection_with_multiplex(error);
                                                break;
                                            }
                                            // Publish the diagnostic only after the mailbox is
                                            // closed and its remote teardown has been admitted.
                                            inner.telemetry.record_metric(
                                                ClientMetricName::OverloadRejections,
                                                1.0,
                                            );
                                            inner.telemetry.emit_diagnostic(
                                                ClientDiagnosticName::SlowConsumerDetected,
                                            );
                                            continue;
                                        }
                                        inner.fail_connection_with_multiplex(error);
                                    } else {
                                        router.close();
                                        reader_transport.close();
                                    }
                                    break;
                                }
                                Ok(bytes) => {
                                    if let Some(inner) = reader_inner.upgrade() {
                                        inner.telemetry.emit_diagnostic(
                                            ClientDiagnosticName::StreamItemRouted,
                                        );
                                        inner.telemetry.record_metric(
                                            ClientMetricName::StreamMailboxBytes,
                                            bytes as f64,
                                        );
                                    }
                                }
                            }
                        }
                        Ok(ServerMessage::Pong) => {
                            if let Some(inner) = reader_inner.upgrade() {
                                inner.heartbeat.observed();
                            }
                        }
                        Ok(ServerMessage::Close) => {
                            if let Some(inner) = reader_inner.upgrade() {
                                inner.fail_connection();
                            } else {
                                router.close();
                                reader_transport.close();
                            }
                            break;
                        }
                        Err(error) => {
                            if let Some(inner) = reader_inner.upgrade() {
                                inner.fail_connection_with_transport(error);
                            } else {
                                router.close();
                                reader_transport.close();
                            }
                            break;
                        }
                    }
                }
            }) {
            Ok(reader) => reader,
            Err(source) => {
                inner.fail_connection();
                let _ = writer.join();
                return Err(ClientConnectError::ReaderThread(source));
            }
        };
        let heartbeat_inner = Arc::downgrade(&inner);
        let heartbeat_policy = inner.agreement.heartbeat();
        let heartbeat_worker = match thread::Builder::new()
            .name("undercurrent-sdk-heartbeat".into())
            .spawn(move || {
                let interval = Duration::from_millis(heartbeat_policy.interval_millis());
                let timeout = Duration::from_millis(heartbeat_policy.timeout_millis());
                while !heartbeat.wait_closed(interval) {
                    let Some(inner) = heartbeat_inner.upgrade() else {
                        break;
                    };
                    if heartbeat.elapsed_since_observation() >= timeout {
                        inner.fail_connection();
                        break;
                    }
                    match inner.try_enqueue(ClientMessage::Ping) {
                        Ok(()) | Err(ClientError::WriterSaturated { .. }) => {}
                        Err(_) => break,
                    }
                }
            }) {
            Ok(worker) => worker,
            Err(source) => {
                inner.fail_connection();
                let _ = writer.join();
                let _ = reader.join();
                return Err(ClientConnectError::HeartbeatThread(source));
            }
        };
        inner
            .workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend([writer, reader, heartbeat_worker]);
        inner
            .telemetry
            .emit_diagnostic(ClientDiagnosticName::ConnectionStarted);
        inner
            .telemetry
            .record_metric(ClientMetricName::PhysicalConnections, 1.0);
        Ok(Self { inner })
    }

    /// Executes one finite correlated request through the shared connection.
    ///
    /// Concurrent calls may receive responses out of order. Timeout sends a
    /// best-effort cancellation without affecting other requests. This method
    /// performs bounded queueing and blocking wait IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns payload, capacity, connection, cancellation, timeout, or
    /// correlation failure.
    pub fn request(&self, payload: Arc<[u8]>, timeout: Duration) -> Result<Arc<[u8]>, ClientError> {
        self.start_request(payload)?.wait(timeout)
    }

    /// Executes one request under caller-supplied distributed trace context.
    ///
    /// The typed context is transmitted without entering payload logs or SDK
    /// diagnostics. This method performs bounded queueing and blocking wait IO.
    ///
    /// # Errors
    ///
    /// Returns payload, capacity, connection, cancellation, timeout, or
    /// correlation failure.
    pub fn request_with_trace_context(
        &self,
        payload: Arc<[u8]>,
        trace_context: ClientTraceContext,
        timeout: Duration,
    ) -> Result<Arc<[u8]>, ClientError> {
        self.start_request_with_trace_context(payload, trace_context)?
            .wait(timeout)
    }

    pub(crate) fn start_request(
        &self,
        payload: Arc<[u8]>,
    ) -> Result<ClientPendingRequest, ClientError> {
        let trace_context = self.next_trace_context()?;
        self.start_request_with_trace_context(payload, ClientTraceContext::from_raw(trace_context))
    }

    pub(crate) fn start_request_with_trace_context(
        &self,
        payload: Arc<[u8]>,
        trace_context: ClientTraceContext,
    ) -> Result<ClientPendingRequest, ClientError> {
        let _duration = RequestDurationMetric::new(self.inner.clone());
        self.validate_payload(&payload)?;
        let request_id = self.next_request_id()?;
        let pending = self.inner.router.register_request(request_id.clone())?;
        let _pending_metric = PendingRequestMetric::new(self.inner.clone());
        if let Err(error) = self.enqueue(ClientMessage::Request {
            request_id: request_id.clone(),
            trace_context: trace_context.into_raw(),
            payload,
        }) {
            self.inner.router.cancel_request(&request_id);
            return Err(error);
        }
        self.inner
            .telemetry
            .emit_diagnostic(ClientDiagnosticName::RequestQueued);
        Ok(ClientPendingRequest {
            client: self.clone(),
            request_id,
            pending: Some(pending),
            state: Mutex::new(ClientPendingRequestState::default()),
            _pending_metric,
            _duration,
        })
    }

    /// Opens one independently owned logical stream through the shared connection.
    ///
    /// The returned handle keeps shared connection ownership but does not
    /// consume this client. The finite open response is returned so the product
    /// protocol layer can decode accepted initial state or a typed rejection.
    /// This method performs bounded queueing and blocking wait IO and does not
    /// panic.
    ///
    /// # Errors
    ///
    /// Returns payload, capacity, connection, cancellation, timeout, or
    /// correlation failure. Failed open removes the provisional stream route.
    pub fn open_subscription(
        &self,
        payload: Arc<[u8]>,
        timeout: Duration,
    ) -> Result<OpenedSubscription, ClientError> {
        let trace_context = self.next_trace_context()?;
        self.open_subscription_with_trace_context(
            payload,
            ClientTraceContext::from_raw(trace_context),
            timeout,
        )
    }

    /// Opens one subscription under caller-supplied distributed trace context.
    ///
    /// # Errors
    ///
    /// Returns payload, capacity, connection, cancellation, timeout, or
    /// correlation failure.
    pub fn open_subscription_with_trace_context(
        &self,
        payload: Arc<[u8]>,
        trace_context: ClientTraceContext,
        timeout: Duration,
    ) -> Result<OpenedSubscription, ClientError> {
        let stream_id = self.next_stream_id()?;
        self.open_subscription_with_id(stream_id, payload, trace_context.into_raw(), timeout)
    }

    /// Opens one subscription using a caller-owned persistent stream identity.
    ///
    /// This is reserved for persistent recovery ownership, which allocates IDs
    /// outside physical connection generations. It performs bounded queueing
    /// and blocking response-wait IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns payload, capacity, connection, timeout, or correlation failure.
    pub(crate) fn open_subscription_with_id(
        &self,
        stream_id: StreamId,
        payload: Arc<[u8]>,
        trace_context: TraceContext,
        timeout: Duration,
    ) -> Result<OpenedSubscription, ClientError> {
        let _duration = RequestDurationMetric::new(self.inner.clone());
        self.validate_payload(&payload)?;
        let stream = self.inner.router.open_stream(
            stream_id.clone(),
            self.inner.limits.stream_mailbox_messages(),
            self.inner.limits.stream_mailbox_bytes(),
        )?;
        let request_id = self.next_request_id()?;
        let pending = match self.inner.router.register_request(request_id.clone()) {
            Ok(pending) => pending,
            Err(error) => {
                let _ = self.inner.router.close_stream(&stream_id);
                return Err(error.into());
            }
        };
        let _pending_metric = PendingRequestMetric::new(self.inner.clone());
        if let Err(error) = self.enqueue(ClientMessage::OpenStream {
            request_id: request_id.clone(),
            stream_id: stream_id.clone(),
            trace_context: trace_context.clone(),
            payload,
        }) {
            self.inner.router.cancel_request(&request_id);
            let _ = self.inner.router.close_stream(&stream_id);
            return Err(error);
        }
        self.inner
            .telemetry
            .emit_diagnostic(ClientDiagnosticName::RequestQueued);
        let response = match pending.recv_timeout(timeout) {
            Ok(response) => response,
            Err(error) => {
                let _ = self.enqueue(ClientMessage::CancelRequest { request_id });
                let _ = self.inner.router.close_stream(&stream_id);
                let _ = self.enqueue(ClientMessage::CloseStream { stream_id });
                return Err(self.inner.pending_error(error));
            }
        };
        self.inner.subscription_opened();
        Ok(OpenedSubscription {
            response,
            subscription: Subscription {
                client: self.clone(),
                stream: Mutex::new(Some(stream)),
                deferred_credit: Arc::new(AtomicU64::new(0)),
                stream_id,
                trace_context,
                closed: AtomicBool::new(false),
            },
        })
    }

    /// Restores one client-scoped logical stream after reconnect.
    ///
    /// The caller supplies the prior stream ID, server epoch, and last fully
    /// consumed sequence. The server responds with replayed items or an
    /// explicit gap on the returned subscription. This method uses the same
    /// physical connection as every other request and stream, performs bounded
    /// queueing and blocking wait IO, and does not panic.
    ///
    /// # Errors
    ///
    /// Returns payload, active-ID, capacity, connection, cancellation, timeout,
    /// or correlation failure. Failed resume removes the provisional route.
    pub fn resume_subscription(
        &self,
        stream_id: ClientStreamId,
        server_epoch: ClientServerEpoch,
        after: ClientStreamSequence,
        payload: Arc<[u8]>,
        timeout: Duration,
    ) -> Result<OpenedSubscription, ClientError> {
        let trace_context = self.next_trace_context()?;
        self.resume_subscription_with_trace_context(
            stream_id,
            server_epoch,
            after,
            payload,
            ClientTraceContext::from_raw(trace_context),
            timeout,
        )
    }

    /// Restores one stream under its retained distributed trace context.
    ///
    /// # Errors
    ///
    /// Returns payload, active-ID, capacity, connection, cancellation, timeout,
    /// or correlation failure.
    pub fn resume_subscription_with_trace_context(
        &self,
        stream_id: ClientStreamId,
        server_epoch: ClientServerEpoch,
        after: ClientStreamSequence,
        payload: Arc<[u8]>,
        trace_context: ClientTraceContext,
        timeout: Duration,
    ) -> Result<OpenedSubscription, ClientError> {
        let stream_id = stream_id.into_raw();
        let server_epoch = server_epoch.into_raw();
        let after = after.into_raw();
        let trace_context = trace_context.into_raw();
        let _duration = RequestDurationMetric::new(self.inner.clone());
        self.validate_payload(&payload)?;
        let stream = self.inner.router.open_stream_after(
            stream_id.clone(),
            self.inner.limits.stream_mailbox_messages(),
            self.inner.limits.stream_mailbox_bytes(),
            after,
        )?;
        let request_id = self.next_request_id()?;
        let pending = match self.inner.router.register_request(request_id.clone()) {
            Ok(pending) => pending,
            Err(error) => {
                let _ = self.inner.router.close_stream(&stream_id);
                return Err(error.into());
            }
        };
        let _pending_metric = PendingRequestMetric::new(self.inner.clone());
        if let Err(error) = self.enqueue(ClientMessage::ResumeStream {
            request_id: request_id.clone(),
            stream_id: stream_id.clone(),
            server_epoch,
            after,
            trace_context: trace_context.clone(),
            payload,
        }) {
            self.inner.router.cancel_request(&request_id);
            let _ = self.inner.router.close_stream(&stream_id);
            return Err(error);
        }
        self.inner
            .telemetry
            .emit_diagnostic(ClientDiagnosticName::RequestQueued);
        let response = match pending.recv_timeout(timeout) {
            Ok(response) => response,
            Err(error) => {
                let _ = self.enqueue(ClientMessage::CancelRequest { request_id });
                let _ = self.inner.router.close_stream(&stream_id);
                let _ = self.enqueue(ClientMessage::CloseStream { stream_id });
                return Err(error.into());
            }
        };
        self.inner.subscription_opened();
        Ok(OpenedSubscription {
            response,
            subscription: Subscription {
                client: self.clone(),
                stream: Mutex::new(Some(stream)),
                deferred_credit: Arc::new(AtomicU64::new(0)),
                stream_id,
                trace_context,
                closed: AtomicBool::new(false),
            },
        })
    }

    /// Sends an application heartbeat through the one bounded writer.
    ///
    /// # Errors
    ///
    /// Returns writer saturation or closed-connection failure.
    pub fn ping(&self) -> Result<(), ClientError> {
        self.enqueue(ClientMessage::Ping)
    }

    /// Closes the shared connection for all clones and logical streams.
    ///
    /// Repeated close is idempotent. This function performs transport close IO
    /// and does not panic.
    pub fn close(&self) {
        let _ = self.enqueue(ClientMessage::Close);
        self.inner.fail_connection();
    }

    /// Returns whether physical connection teardown has begun.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// Returns the immutable negotiated handshake agreement.
    pub fn agreement(&self) -> ClientConnectionAgreement {
        ClientConnectionAgreement::from_raw(self.inner.agreement.clone())
    }

    pub(crate) fn agreement_raw(&self) -> &ConnectionAgreement {
        &self.inner.agreement
    }

    /// Returns default bounded SDK diagnostics, or `None` for a custom sink.
    pub fn telemetry_diagnostics(&self) -> Option<Vec<ClientDiagnosticName>> {
        self.inner
            .default_telemetry
            .as_ref()
            .map(|telemetry| telemetry.diagnostics())
    }

    /// Returns default fixed-cardinality SDK metrics, or `None` for a custom sink.
    pub fn telemetry_metric_series(&self) -> Option<Vec<crate::ClientMetricSeries>> {
        self.inner
            .default_telemetry
            .as_ref()
            .map(|telemetry| telemetry.metric_series())
    }

    /// Returns default telemetry retention pressure, or `None` for a custom sink.
    pub fn telemetry_pressure(&self) -> Option<crate::ClientTelemetryPressure> {
        self.inner
            .default_telemetry
            .as_ref()
            .map(|telemetry| telemetry.pressure())
    }

    /// Blocks until physical connection teardown begins.
    ///
    /// This synchronization hook is used by the persistent recovery supervisor
    /// and performs no polling or transport IO. It does not panic.
    pub(crate) fn wait_closed(&self) {
        let guard = self
            .inner
            .close_wait
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        drop(
            self.inner
                .close_changed
                .wait_while(guard, |_| {
                    let open = !self.is_closed();
                    #[cfg(test)]
                    if open {
                        let hook = self
                            .inner
                            .before_close_wait
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .take();
                        if let Some(hook) = hook {
                            hook();
                        }
                    }
                    open
                })
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
    }

    #[cfg(test)]
    pub(crate) fn before_close_wait(&self, hook: impl FnOnce() + Send + 'static) {
        let _guard = self.inner.close_wait.lock().expect("close wait lock");
        *self
            .inner
            .before_close_wait
            .lock()
            .expect("close wait hook") = Some(Box::new(hook));
        // Recheck the predicate if the supervisor was already waiting when the
        // test installed its gate. The hook belongs to this physical client.
        self.inner.close_changed.notify_all();
    }

    fn validate_payload(&self, payload: &[u8]) -> Result<(), ClientError> {
        if payload.is_empty() || payload.len() > self.inner.limits.max_payload_bytes() as usize {
            return Err(ClientError::InvalidPayloadSize {
                actual: payload.len(),
                maximum: self.inner.limits.max_payload_bytes(),
            });
        }
        Ok(())
    }

    fn next_request_id(&self) -> Result<RequestId, ClientError> {
        let value = next_counter(&self.inner.next_request, "request_id")?;
        RequestId::new(format!("r-{value}"))
            .map_err(|_| MultiplexError::CounterExhausted {
                counter: "request_id_encoding",
            })
            .map_err(Into::into)
    }

    fn next_stream_id(&self) -> Result<StreamId, ClientError> {
        let value = next_counter(&self.inner.next_stream, "stream_id")?;
        StreamId::new(format!("s-{value}"))
            .map_err(|_| MultiplexError::CounterExhausted {
                counter: "stream_id_encoding",
            })
            .map_err(Into::into)
    }

    pub(crate) fn next_trace_context(&self) -> Result<TraceContext, ClientError> {
        self.inner.traces.next().map_err(Into::into)
    }

    fn enqueue(&self, message: ClientMessage) -> Result<(), ClientError> {
        self.inner.try_enqueue(message)
    }
}

/// Successful logical-stream open response and independent handle.
pub struct OpenedSubscription {
    /// Encoded finite response to the open operation.
    pub response: Arc<[u8]>,
    /// Independent logical stream handle.
    pub subscription: Subscription,
}

/// One independent logical stream sharing client connection ownership.
pub struct Subscription {
    client: Client,
    stream: Mutex<Option<InboundStream>>,
    stream_id: StreamId,
    trace_context: TraceContext,
    deferred_credit: Arc<AtomicU64>,
    closed: AtomicBool,
}

impl Subscription {
    /// Returns the independent logical stream identity.
    pub fn stream_id(&self) -> ClientStreamId {
        ClientStreamId::from_raw(self.stream_id.clone())
    }

    pub(crate) const fn stream_id_raw(&self) -> &StreamId {
        &self.stream_id
    }

    /// Returns the distributed context retained across logical-stream recovery.
    pub fn trace_context(&self) -> ClientTraceContext {
        ClientTraceContext::from_raw(self.trace_context.clone())
    }

    pub(crate) const fn trace_context_raw(&self) -> &TraceContext {
        &self.trace_context
    }

    pub(crate) const fn client(&self) -> &Client {
        &self.client
    }

    /// Blocks until the next ordered stream item or terminal stream state.
    ///
    /// Consumed bytes are returned through the one writer. Writer saturation
    /// accumulates one bounded numeric deferred-credit counter instead of
    /// dropping the delivered item or allocating more messages. This method
    /// performs blocking wait IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns timeout, unsubscribe, cancellation, or connection closure.
    pub fn recv(&self) -> Result<ClientStreamItem, ClientStreamReceiveError> {
        self.recv_raw()
            .map(ClientStreamItem::from_raw)
            .map_err(|error| self.client.inner.stream_receive_error(error))
    }

    pub(crate) fn recv_raw(&self) -> Result<InboundStreamItem, InboundStreamError> {
        let stream = self
            .stream
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let item = stream
            .as_ref()
            .ok_or(InboundStreamError::Unsubscribed)?
            .recv()?;
        drop(stream);
        self.record_mailbox_bytes();
        self.return_item_credit(&item);
        Ok(item)
    }

    /// Waits up to `timeout` for the next ordered stream item.
    ///
    /// Credit behavior matches [`Self::recv`]. This method performs blocking
    /// wait IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns timeout, unsubscribe, cancellation, or connection closure.
    pub fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<ClientStreamItem, ClientStreamReceiveError> {
        self.recv_timeout_raw(timeout)
            .map(ClientStreamItem::from_raw)
            .map_err(|error| self.client.inner.stream_receive_error(error))
    }

    pub(crate) fn recv_timeout_raw(
        &self,
        timeout: Duration,
    ) -> Result<InboundStreamItem, InboundStreamError> {
        let stream = self
            .stream
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let item = stream
            .as_ref()
            .ok_or(InboundStreamError::Unsubscribed)?
            .recv_timeout(timeout)?;
        drop(stream);
        self.record_mailbox_bytes();
        self.return_item_credit(&item);
        Ok(item)
    }

    /// Returns the latest caller-consumed sequence for reconnect resume.
    ///
    /// Enqueueing an item does not advance this cursor. Consuming a gap advances
    /// it through the inclusive missing interval, not through recovered data.
    pub fn last_sequence(&self) -> ClientStreamSequence {
        ClientStreamSequence::from_raw(self.last_sequence_raw())
    }

    pub(crate) fn last_sequence_raw(&self) -> StreamSequence {
        self.stream
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map_or(StreamSequence::INITIAL, InboundStream::last_sequence)
    }

    /// Returns consumed bytes whose credit update has not entered the writer.
    pub fn deferred_credit_bytes(&self) -> u64 {
        self.deferred_credit.load(Ordering::Acquire)
    }

    /// Attempts to return all deferred credit through the bounded writer.
    ///
    /// # Errors
    ///
    /// Returns writer saturation or connection closure. Failed credit remains
    /// retained in the bounded numeric counter for a later retry.
    pub fn flush_credit(&self) -> Result<(), ClientError> {
        // A failed admission must restore the counter before the writer scans it.
        let _admission = self
            .client
            .inner
            .writer_admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let deferred = self.deferred_credit.swap(0, Ordering::AcqRel);
        if deferred == 0 {
            self.client
                .inner
                .telemetry
                .record_metric(ClientMetricName::DeferredCreditBytes, 0.0);
            return Ok(());
        }
        if let Err(error) = self
            .client
            .inner
            .try_enqueue_locked(ClientMessage::WindowUpdate {
                stream_id: self.stream_id.clone(),
                additional_bytes: deferred,
            })
        {
            saturating_atomic_add(&self.deferred_credit, deferred);
            if !self.closed.load(Ordering::Acquire) && !self.client.is_closed() {
                self.client
                    .inner
                    .deferred_credits
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(self.stream_id.clone(), self.deferred_credit.clone());
            }
            self.client.inner.telemetry.record_metric(
                ClientMetricName::DeferredCreditBytes,
                self.deferred_credit.load(Ordering::Acquire) as f64,
            );
            self.client
                .inner
                .telemetry
                .emit_diagnostic(ClientDiagnosticName::CreditDeferred);
            return Err(error);
        }
        self.client
            .inner
            .telemetry
            .record_metric(ClientMetricName::DeferredCreditBytes, 0.0);
        Ok(())
    }

    /// Unsubscribes this stream and queues its remote close.
    ///
    /// Repeated close is idempotent. If the close command cannot enter the
    /// bounded writer, the physical connection is closed to prevent an untracked
    /// remote interest. Successful close queueing leaves peer work active.
    pub fn close(self) {
        self.close_inner();
    }

    fn return_item_credit(&self, item: &InboundStreamItem) {
        let bytes = match item {
            InboundStreamItem::Data {
                flow_controlled_bytes,
                ..
            } => *flow_controlled_bytes,
            InboundStreamItem::Gap(_) => 0,
        };
        if bytes == 0 {
            return;
        }
        saturating_atomic_add(&self.deferred_credit, bytes);
        let _ = self.flush_credit();
    }

    fn record_mailbox_bytes(&self) {
        if let Ok(bytes) = self
            .client
            .inner
            .router
            .stream_queued_bytes(&self.stream_id)
        {
            self.client
                .inner
                .telemetry
                .record_metric(ClientMetricName::StreamMailboxBytes, bytes as f64);
        }
    }

    /// Closes an `Arc`-owned subscription and wakes any blocked receiver.
    pub(crate) fn close_shared(&self) {
        self.close_inner();
    }

    fn close_inner(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        // A receiver holds `self.stream` while waiting in the mailbox. Close the
        // route first to wake it; taking that mutex before closing could leave
        // both the receiver and this closer waiting indefinitely.
        let remote_open = self
            .client
            .inner
            .router
            .close_stream(&self.stream_id)
            .is_ok();
        self.client
            .inner
            .deferred_credits
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.stream_id);
        self.client.inner.subscription_closed();
        if remote_open
            && self
                .client
                .enqueue(ClientMessage::CloseStream {
                    stream_id: self.stream_id.clone(),
                })
                .is_err()
        {
            self.client.inner.fail_connection();
        }
        self.stream
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.close_inner();
    }
}

/// SDK construction failure before a usable client is returned.
#[derive(Debug)]
pub enum ClientConnectError {
    /// Negotiated agreement selected a connection protocol version other than 2.0.
    UnsupportedProtocolVersion {
        /// Rejected server-selected protocol version.
        version: ClientProtocolVersion,
    },
    /// Negotiated agreement omitted a capability required by this SDK runtime.
    MissingRequiredCapability {
        /// Stable missing Undercurrent connection capability name.
        capability: &'static str,
    },
    /// SDK-local limits conflict with negotiated connection limits.
    Limits(ClientLimitError),
    /// Operating system could not create the one writer worker.
    WriterThread(std::io::Error),
    /// Operating system could not create the one reader worker.
    ReaderThread(std::io::Error),
    /// Operating system could not create the negotiated heartbeat worker.
    HeartbeatThread(std::io::Error),
    /// Secure client trace-context initialization failed.
    TraceContext(TraceContextGenerationError),
}

impl Display for ClientConnectError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedProtocolVersion { version } => write!(
                formatter,
                "negotiated protocol {}.{} is unsupported; SDK requires connection protocol 2.0",
                version.major, version.minor
            ),
            Self::MissingRequiredCapability { capability } => {
                write!(formatter, "negotiated connection is missing {capability}")
            }
            Self::Limits(error) => Display::fmt(error, formatter),
            Self::WriterThread(_) => formatter.write_str("could not start SDK connection writer"),
            Self::ReaderThread(_) => formatter.write_str("could not start SDK connection reader"),
            Self::HeartbeatThread(_) => {
                formatter.write_str("could not start SDK connection heartbeat")
            }
            Self::TraceContext(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for ClientConnectError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::UnsupportedProtocolVersion { .. } | Self::MissingRequiredCapability { .. } => {
                None
            }
            Self::Limits(error) => Some(error),
            Self::WriterThread(error)
            | Self::ReaderThread(error)
            | Self::HeartbeatThread(error) => Some(error),
            Self::TraceContext(error) => Some(error),
        }
    }
}

struct ClientInner {
    agreement: ConnectionAgreement,
    limits: ClientRuntimeLimits,
    transport: Arc<dyn ClientTransport>,
    router: ConnectionRouter,
    writer_tx: SyncSender<WriterCommand>,
    writer_admission: Mutex<()>,
    deferred_credits: Mutex<std::collections::BTreeMap<StreamId, Arc<AtomicU64>>>,
    writer_queued_messages: AtomicUsize,
    next_request: AtomicU64,
    next_stream: AtomicU64,
    traces: TraceContextGenerator,
    closed: AtomicBool,
    transport_failure: Mutex<Option<Arc<crate::ClientTransportError>>>,
    multiplex_failure: Mutex<Option<MultiplexError>>,
    close_wait: Mutex<()>,
    close_changed: Condvar,
    #[cfg(test)]
    before_close_wait: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    heartbeat: Arc<HeartbeatState>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    telemetry: Arc<dyn ClientTelemetry>,
    default_telemetry: Option<Arc<BoundedClientTelemetry>>,
    pending_requests: AtomicUsize,
    logical_subscriptions: AtomicUsize,
}

impl ClientInner {
    fn flush_deferred_credits(&self) -> Result<(), ClientTransportError> {
        let updates = {
            let _admission = self
                .writer_admission
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let mut credits = self
                .deferred_credits
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let mut updates = Vec::new();
            for (stream_id, credit) in std::mem::take(&mut *credits) {
                if self.router.stream_queued_bytes(&stream_id).is_err() {
                    credit.store(0, Ordering::Release);
                    continue;
                }
                let bytes = credit.swap(0, Ordering::AcqRel);
                if bytes > 0 {
                    updates.push(ClientMessage::WindowUpdate {
                        stream_id: stream_id.clone(),
                        additional_bytes: bytes,
                    });
                }
            }
            updates
        };
        // Only the writer calls this, so updates cannot overtake a queued close.
        for update in updates {
            self.transport
                .send(ClientTransportOutbound::from_raw(update))?;
            self.telemetry
                .record_metric(ClientMetricName::DeferredCreditBytes, 0.0);
        }
        Ok(())
    }

    fn try_enqueue(&self, message: ClientMessage) -> Result<(), ClientError> {
        // Share admission with fail_connection so closure cannot land between
        // the open check and queue insertion. The atomic flag alone cannot
        // protect that interval.
        // See [admission](../docs/internals/client-runtime.md#serialize-admission-and-correlate-replies).
        let _admission = self
            .writer_admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.try_enqueue_locked(message)
    }

    // Caller owns writer_admission.
    fn try_enqueue_locked(&self, message: ClientMessage) -> Result<(), ClientError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(self.connection_closed_error());
        }
        // Serialize credit admission with the writer: after local route removal
        // no new credit may follow its CloseStream on the wire. A receiver can
        // finish parsing an item concurrently with cancellation or overload.
        if let ClientMessage::WindowUpdate { stream_id, .. } = &message
            && self.router.stream_queued_bytes(stream_id).is_err()
        {
            return Ok(());
        }
        let capacity = self.limits.writer_queue_messages();
        if self.writer_queued_messages.load(Ordering::Acquire) >= capacity {
            self.telemetry
                .emit_diagnostic(ClientDiagnosticName::WriterSaturated);
            self.telemetry
                .record_metric(ClientMetricName::OverloadRejections, 1.0);
            return Err(ClientError::WriterSaturated { capacity });
        }
        let current = self
            .writer_queued_messages
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        match self.writer_tx.try_send(WriterCommand::Message(message)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.writer_message_dequeued();
                self.telemetry
                    .emit_diagnostic(ClientDiagnosticName::WriterSaturated);
                self.telemetry
                    .record_metric(ClientMetricName::OverloadRejections, 1.0);
                return Err(ClientError::WriterSaturated { capacity });
            }
            Err(TrySendError::Disconnected(_)) => {
                self.writer_message_dequeued();
                return Err(self.connection_closed_error());
            }
        }
        self.telemetry
            .record_metric(ClientMetricName::WriterQueuedMessages, current as f64);
        Ok(())
    }

    fn fail_connection(&self) {
        {
            let _admission = self
                .writer_admission
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            {
                // Publish closure under the waiter's mutex so it either sees
                // closed or enrolls before notification. Lock admission first;
                // wait_closed never acquires it. Release both before callbacks.
                let _wait = self
                    .close_wait
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if self.closed.swap(true, Ordering::AcqRel) {
                    return;
                }
            }
            // Wake an idle writer without waiting for capacity. If this queue
            // is full, the writer already has work to wake on and will observe
            // `closed`; transport.close below also unblocks a send in progress.
            let _ = self.writer_tx.try_send(WriterCommand::Shutdown);
        }
        // Failure-specific callers must retain their cause before reaching
        // this wake-up: pending waiters consult it as soon as the router closes.
        self.router.close();
        self.transport.close();
        self.heartbeat.close();
        self.pending_requests.store(0, Ordering::Release);
        self.logical_subscriptions.store(0, Ordering::Release);
        self.writer_queued_messages.store(0, Ordering::Release);
        self.close_changed.notify_all();
        self.telemetry
            .emit_diagnostic(ClientDiagnosticName::ConnectionClosed);
        self.telemetry
            .record_metric(ClientMetricName::PhysicalConnections, 0.0);
        self.telemetry
            .record_metric(ClientMetricName::PendingRequests, 0.0);
        self.telemetry
            .record_metric(ClientMetricName::LogicalSubscriptions, 0.0);
        self.telemetry
            .record_metric(ClientMetricName::WriterQueuedMessages, 0.0);
    }

    fn fail_connection_with_transport(&self, error: crate::ClientTransportError) {
        let mut failure = self
            .transport_failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if failure.is_none() {
            *failure = Some(Arc::new(error));
        }
        drop(failure);
        self.fail_connection();
    }

    fn fail_connection_with_multiplex(&self, error: MultiplexError) {
        let mut failure = self
            .multiplex_failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if failure.is_none() {
            *failure = Some(error);
        }
        drop(failure);
        self.fail_connection();
    }

    fn connection_overload(&self) -> Option<crate::ClientOverload> {
        let multiplex = self
            .multiplex_failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(overload) = multiplex.and_then(|error| ClientError::from(error).overload()) {
            return Some(overload);
        }

        self.transport_failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .and_then(|error| error.overload())
    }

    fn stream_receive_error(&self, error: InboundStreamError) -> ClientStreamReceiveError {
        if matches!(error, InboundStreamError::ConnectionClosed)
            && let Some(overload) = self.connection_overload()
        {
            return ClientStreamReceiveError::Overloaded { overload };
        }
        error.into()
    }

    fn connection_closed_error(&self) -> ClientError {
        if let Some(error) = self
            .multiplex_failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
        {
            return error.into();
        }
        self.transport_failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
            .map(ClientError::transport)
            .unwrap_or_else(|| MultiplexError::ConnectionClosed.into())
    }

    fn pending_error(&self, error: undercurrent_multiplex::PendingResponseError) -> ClientError {
        if matches!(
            error,
            undercurrent_multiplex::PendingResponseError::ConnectionClosed
                | undercurrent_multiplex::PendingResponseError::RouteDisconnected
        ) {
            if let Some(failure) = self
                .multiplex_failure
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
            {
                return failure.into();
            }
            return self
                .transport_failure
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
                .map(ClientError::transport)
                .unwrap_or_else(|| error.into());
        }
        error.into()
    }

    fn writer_message_dequeued(&self) {
        let queued = self
            .writer_queued_messages
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(1))
            })
            .unwrap_or(0)
            .saturating_sub(1);
        self.telemetry
            .record_metric(ClientMetricName::WriterQueuedMessages, queued as f64);
    }

    fn pending_request_started(&self) {
        let pending = self
            .pending_requests
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        self.telemetry
            .record_metric(ClientMetricName::PendingRequests, pending as f64);
    }

    fn pending_request_finished(&self) {
        let pending = self
            .pending_requests
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                Some(value.saturating_sub(1))
            })
            .unwrap_or(0)
            .saturating_sub(1);
        self.telemetry
            .record_metric(ClientMetricName::PendingRequests, pending as f64);
    }

    fn subscription_opened(&self) {
        let active = self
            .logical_subscriptions
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        self.telemetry
            .emit_diagnostic(ClientDiagnosticName::SubscriptionOpened);
        self.telemetry
            .record_metric(ClientMetricName::LogicalSubscriptions, active as f64);
    }

    fn subscription_closed(&self) {
        let active = self
            .logical_subscriptions
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                Some(value.saturating_sub(1))
            })
            .unwrap_or(0)
            .saturating_sub(1);
        self.telemetry
            .emit_diagnostic(ClientDiagnosticName::SubscriptionClosed);
        self.telemetry
            .record_metric(ClientMetricName::LogicalSubscriptions, active as f64);
    }
}

#[derive(Default)]
struct ClientPendingRequestState {
    cancellation_sent: bool,
    terminal_received: bool,
}

pub(crate) struct ClientPendingRequest {
    client: Client,
    request_id: RequestId,
    pending: Option<PendingResponse>,
    state: Mutex<ClientPendingRequestState>,
    _pending_metric: PendingRequestMetric,
    _duration: RequestDurationMetric,
}

impl ClientPendingRequest {
    pub(crate) fn cancel(&self) -> Result<(), ClientError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.cancellation_sent || state.terminal_received {
            return Ok(());
        }
        self.client.enqueue(ClientMessage::CancelRequest {
            request_id: self.request_id.clone(),
        })?;
        state.cancellation_sent = true;
        Ok(())
    }

    pub(crate) fn wait(mut self, timeout: Duration) -> Result<Arc<[u8]>, ClientError> {
        let pending = self.pending.take().expect("pending response is owned");
        match pending.recv_timeout(timeout) {
            Ok(response) => {
                self.state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .terminal_received = true;
                Ok(response)
            }
            Err(error) => {
                let _ = self.cancel();
                Err(self.client.inner.pending_error(error))
            }
        }
    }
}

impl Drop for ClientPendingRequest {
    fn drop(&mut self) {
        if self.pending.is_some() {
            let _ = self.cancel();
        }
    }
}

struct PendingRequestMetric {
    inner: Arc<ClientInner>,
}

impl PendingRequestMetric {
    fn new(inner: Arc<ClientInner>) -> Self {
        inner.pending_request_started();
        Self { inner }
    }
}

impl Drop for PendingRequestMetric {
    fn drop(&mut self) {
        self.inner.pending_request_finished();
    }
}

struct RequestDurationMetric {
    inner: Arc<ClientInner>,
    started_at: Instant,
}

impl RequestDurationMetric {
    fn new(inner: Arc<ClientInner>) -> Self {
        Self {
            inner,
            started_at: Instant::now(),
        }
    }
}

impl Drop for RequestDurationMetric {
    fn drop(&mut self) {
        self.inner.telemetry.record_metric(
            ClientMetricName::RequestDurationSeconds,
            self.started_at.elapsed().as_secs_f64(),
        );
    }
}

struct HeartbeatState {
    last_observed: Mutex<Instant>,
    wait: Mutex<()>,
    changed: Condvar,
    closed: AtomicBool,
}

impl HeartbeatState {
    fn new() -> Self {
        Self {
            last_observed: Mutex::new(Instant::now()),
            wait: Mutex::new(()),
            changed: Condvar::new(),
            closed: AtomicBool::new(false),
        }
    }

    fn observed(&self) {
        *self
            .last_observed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Instant::now();
    }

    fn elapsed_since_observation(&self) -> Duration {
        self.last_observed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .elapsed()
    }

    fn wait_closed(&self, interval: Duration) -> bool {
        let guard = self
            .wait
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        drop(
            self.changed
                .wait_timeout_while(guard, interval, |_| !self.closed.load(Ordering::Acquire))
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        self.closed.load(Ordering::Acquire)
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.changed.notify_all();
    }
}

impl Drop for ClientInner {
    fn drop(&mut self) {
        self.fail_connection();
        let workers = std::mem::take(
            self.workers
                .get_mut()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        for worker in workers {
            // A worker can drop the last strong reference after upgrading its
            // weak handle. Teardown on that worker must not join itself.
            if worker.thread().id() != thread::current().id() {
                let _ = worker.join();
            }
        }
    }
}

fn next_counter(counter: &AtomicU64, name: &'static str) -> Result<u64, ClientError> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(1)
        })
        .map(|previous| previous + 1)
        .map_err(|_| MultiplexError::CounterExhausted { counter: name }.into())
}

fn saturating_atomic_add(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        Some(current.saturating_add(value))
    });
}
