// SPDX-License-Identifier: Apache-2.0

//! Bounded product decoding, idempotency, cancellation, and stream activation.

use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use async_channel::{Receiver, Sender, TrySendError};
use undercurrent_domain::SessionId;
use undercurrent_protocol::{
    AuthenticatedRequestContext, OutputCaptureModeDto, ProductCodec, ProductError,
    ProductErrorCategory, ProductErrorDisposition, ProductOperation, ProductRequest,
    ProductResponse, ProductStreamError, ProductStreamKind, ProductStreamOpenResponse,
    ProductStreamOpened, ProductStreamSpec, TerminalOutputCodec, product_capabilities,
};
use undercurrent_transport_core::AuthenticatedActionDispatcher;
use undercurrent_transport_core::{
    BoundedRequestIdempotencyStore, RequestIdempotencyCompletion, RequestIdempotencyError,
    RequestIdempotencyReservation, RequestIdempotencyScope, ServerAction, ServerAdmissionRejection,
    ServerAdmissionResponseKind, ServerConnectionSession, ServerDispatchError,
};
use undercurrent_wire::{
    ClientId, ConnectionId, IdempotencyKey, RequestId, ServerEpoch, StreamId, StreamSequence,
    TraceContext,
};

use crate::{
    PreparedProductStream, ProductCancellation, ProductDispatchEventName, ProductDispatchFailure,
    ProductDispatchResultCategory, ProductDispatcherCreateError, ProductDispatcherLimitError,
    ProductStreamPolicy, ProductStreamPublisher, product_request_span, product_stream_span,
    record_product_operation, record_product_request_outcome, record_product_stream_kind,
    record_product_stream_outcome,
};

/// Validated hard limits for bounded product dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductDispatcherLimits {
    workers: usize,
    queued_actions: usize,
    idempotency_retention: Duration,
}

impl ProductDispatcherLimits {
    /// Constructs non-zero worker, queue, and retention limits.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns the first stable zero-valued field.
    pub const fn new(
        workers: usize,
        queued_actions: usize,
        idempotency_retention: Duration,
    ) -> Result<Self, ProductDispatcherLimitError> {
        if workers == 0 {
            return Err(ProductDispatcherLimitError::ZeroLimit { field: "workers" });
        }
        if queued_actions == 0 {
            return Err(ProductDispatcherLimitError::ZeroLimit {
                field: "queued_actions",
            });
        }
        if idempotency_retention.is_zero() {
            return Err(ProductDispatcherLimitError::ZeroLimit {
                field: "idempotency_retention",
            });
        }
        Ok(Self {
            workers,
            queued_actions,
            idempotency_retention,
        })
    }

    /// Returns bounded worker count.
    pub const fn workers(self) -> usize {
        self.workers
    }

    /// Returns bounded queued action count.
    pub const fn queued_actions(self) -> usize {
        self.queued_actions
    }

    /// Returns completed mutation result retention.
    pub const fn idempotency_retention(self) -> Duration {
        self.idempotency_retention
    }
}

/// Safe context for one finite product execution.
#[derive(Debug, Clone)]
pub struct ProductRequestContext {
    /// Credential-free identity established by the Local upgrade.
    pub authentication: Arc<AuthenticatedRequestContext>,
    /// Negotiated physical connection identity.
    pub connection_id: ConnectionId,
    /// Daemon epoch that accepted the request.
    pub server_epoch: ServerEpoch,
    /// Stable logical client identity.
    pub client_id: ClientId,
    /// Connection-epoch request identity.
    pub request_id: RequestId,
    /// Distributed caller context propagated into application adapters.
    pub trace_context: TraceContext,
    /// Cooperative cancellation observed by use-case adapters.
    pub cancellation: ProductCancellation,
    /// Physical completion of this request's response, independent of its effect.
    pub response_write: Arc<undercurrent_transport_core::ServerResponseWrite>,
}

/// Prior connection state supplied for a stream resume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductStreamResume {
    /// Previously observed server process epoch.
    pub server_epoch: ServerEpoch,
    /// Last stream item fully consumed by the caller.
    pub after: StreamSequence,
}

/// Safe context for one logical-stream preparation.
#[derive(Debug, Clone)]
pub struct ProductStreamContext {
    /// Credential-free identity established by the Local upgrade.
    pub authentication: Arc<AuthenticatedRequestContext>,
    /// Negotiated physical connection identity.
    pub connection_id: ConnectionId,
    /// Daemon epoch that accepted the stream.
    pub server_epoch: ServerEpoch,
    /// Stable logical client identity.
    pub client_id: ClientId,
    /// Correlated stream-open request identity.
    pub request_id: RequestId,
    /// Stable client-selected stream identity.
    pub stream_id: StreamId,
    /// Distributed caller context propagated into stream preparation.
    pub trace_context: TraceContext,
    /// Prior connection resume state when reconnecting.
    pub resume: Option<ProductStreamResume>,
    /// Cooperative cancellation during preparation.
    pub cancellation: ProductCancellation,
}

/// Application-facing product execution port used by the daemon adapter.
pub trait ProductActionExecutor: Send + Sync {
    /// Executes one already decoded finite request.
    ///
    /// Implementations translate DTOs into validated domain/application values,
    /// authorize independently, and return a typed safe response. They should
    /// observe cancellation around external IO and must not log request payloads.
    fn execute(&self, context: &ProductRequestContext, request: ProductRequest) -> ProductResponse;

    /// Prepares selective stream interest before transport activation.
    ///
    /// The returned registration must already own all fallible authorization
    /// and capacity reservations. Dropping it must cancel that interest.
    fn prepare_stream(
        &self,
        context: &ProductStreamContext,
        specification: ProductStreamSpec,
    ) -> Result<Box<dyn PreparedProductStream>, ProductStreamError>;

    /// Releases one active or preparing product subscription idempotently.
    ///
    /// This callback must be bounded, non-blocking, perform no external IO, and
    /// must not panic.
    fn close_stream(&self, client_id: &ClientId, stream_id: &StreamId);

    /// Releases every product resource retained for one physical connection.
    ///
    /// This callback must be bounded, non-blocking, perform no external IO, and
    /// must not panic.
    fn close_connection(&self, client_id: &ClientId);

    /// Stops product execution and releases global adapter resources.
    ///
    /// Dispatcher shutdown invokes this after its bounded work queue drains.
    fn shutdown(&self);
}

/// Payload-free product dispatcher event.
#[derive(Debug, Clone)]
pub struct ProductDispatchEvent {
    /// Stable event name.
    pub name: ProductDispatchEventName,
    /// Physical connection identity when the event belongs to a connection.
    pub connection_id: Option<ConnectionId>,
    /// Server epoch that accepted the connection.
    pub server_epoch: Option<ServerEpoch>,
    /// Logical client owner.
    pub client_id: Option<ClientId>,
    /// Correlated request identity.
    pub request_id: Option<RequestId>,
    /// Logical stream identity when applicable.
    pub stream_id: Option<StreamId>,
    /// Distributed caller context when the event belongs to correlated work.
    pub trace_context: Option<TraceContext>,
    /// Decoded finite operation when available.
    pub operation: Option<ProductOperation>,
    /// Decoded logical stream kind when available.
    pub stream_kind: Option<ProductStreamKind>,
    /// Stable outcome category.
    pub result: ProductDispatchResultCategory,
    /// Time spent after queue admission, excluding socket IO.
    pub duration_micros: Option<u64>,
    /// Encoded finite response bytes when this event settled an RPC.
    pub response_bytes: Option<usize>,
}

/// Non-blocking diagnostic sink for product dispatch decisions.
pub trait ProductDispatchTelemetry: Send + Sync {
    /// Records one payload-free event without retaining high-volume content.
    fn emit(&self, event: ProductDispatchEvent);
}

/// No-op telemetry adapter for compositions that explicitly disable reporting.
#[derive(Debug, Default)]
pub struct NoopProductDispatchTelemetry;

impl ProductDispatchTelemetry for NoopProductDispatchTelemetry {
    fn emit(&self, _event: ProductDispatchEvent) {}
}

/// Current bounded dispatcher pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductDispatcherPressure {
    /// Actions currently waiting for a worker.
    pub queued_actions: usize,
    /// Inclusive queued action bound.
    pub queued_action_limit: usize,
    /// Workers currently executing an action.
    pub active_workers: usize,
    /// Combined ordinary, force, and reserved control worker count.
    pub worker_limit: usize,
    /// Accepted requests with live cancellation state.
    pub cancellable_requests: usize,
    /// Total actions admitted since startup.
    pub admitted_actions: u64,
    /// Total actions rejected at admission since startup.
    pub rejected_actions: u64,
    /// Whether shutdown has begun.
    pub closed: bool,
}

/// Bounded multi-worker implementation of the transport dispatch port.
pub struct BoundedProductDispatcher {
    shared: Arc<DispatcherShared>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

impl BoundedProductDispatcher {
    /// Starts named worker threads around validated product resources.
    ///
    /// Construction verifies that the fixed internal error envelope fits the
    /// response codec before accepting any action. It spawns exactly the
    /// configured worker count and performs no network or application IO.
    ///
    /// # Errors
    ///
    /// Returns invalid limits, unusable response codec limits, or an operating-
    /// system thread creation failure. Partially started workers are stopped and
    /// joined before returning failure.
    pub fn new(
        codec: ProductCodec,
        terminal_codec: TerminalOutputCodec,
        executor: Arc<dyn ProductActionExecutor>,
        idempotency: Arc<BoundedRequestIdempotencyStore>,
        telemetry: Arc<dyn ProductDispatchTelemetry>,
        limits: ProductDispatcherLimits,
    ) -> Result<Self, ProductDispatcherCreateError> {
        let fallback = internal_response(None);
        let fallback_response = codec
            .encode_response(&fallback)
            .map(Arc::from)
            .map_err(ProductDispatcherCreateError::Codec)?;
        let legacy_fallback_response = codec
            .encode_response(&response_for_profile(false, false, fallback))
            .map(Arc::from)
            .map_err(ProductDispatcherCreateError::Codec)?;
        let (sender, receiver) = async_channel::bounded(limits.queued_actions);
        // Graceful effects can occupy every ordinary worker. Keep bounded lanes
        // for force and short control/query requests, including name resolution.
        let (force_sender, force_receiver) = async_channel::bounded(limits.queued_actions);
        let (control_sender, control_receiver) = async_channel::bounded(limits.queued_actions);
        let shared = Arc::new(DispatcherShared {
            codec,
            fallback_response,
            legacy_fallback_response,
            terminal_codec,
            executor,
            idempotency,
            telemetry,
            limits,
            sender,
            force_sender,
            control_sender,
            cancellations: Mutex::new(BTreeMap::new()),
            active_workers: AtomicUsize::new(0),
            admitted_actions: AtomicU64::new(0),
            rejected_actions: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            executor_shutdown: AtomicBool::new(false),
        });
        let mut workers = Vec::with_capacity(limits.workers * 2 + 2);
        for index in 0..limits.workers * 2 + 2 {
            let worker_shared = shared.clone();
            let worker_receiver = if index < limits.workers {
                receiver.clone()
            } else if index < limits.workers * 2 {
                force_receiver.clone()
            } else {
                control_receiver.clone()
            };
            match thread::Builder::new()
                .name(format!("undercurrent-product-{index}"))
                .spawn(move || worker_loop(worker_shared, worker_receiver))
            {
                Ok(worker) => workers.push(worker),
                Err(source) => {
                    shared.closed.store(true, Ordering::Release);
                    shared.sender.close();
                    shared.force_sender.close();
                    shared.control_sender.close();
                    drop(receiver);
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(ProductDispatcherCreateError::Worker { index, source });
                }
            }
        }
        Ok(Self {
            shared,
            workers: Mutex::new(workers),
        })
    }

    /// Returns bounded pressure sampled from independent queue, counter, and
    /// cancellation state. Fields can reflect different instants during activity.
    pub fn pressure(&self) -> ProductDispatcherPressure {
        ProductDispatcherPressure {
            queued_actions: self.shared.sender.len()
                + self.shared.force_sender.len()
                + self.shared.control_sender.len(),
            queued_action_limit: self.shared.limits.queued_actions * 3,
            active_workers: self.shared.active_workers.load(Ordering::Acquire),
            worker_limit: self.shared.limits.workers * 2 + 2,
            cancellable_requests: self
                .shared
                .cancellations
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len(),
            admitted_actions: self.shared.admitted_actions.load(Ordering::Acquire),
            rejected_actions: self.shared.rejected_actions.load(Ordering::Acquire),
            closed: self.shared.closed.load(Ordering::Acquire),
        }
    }

    pub(crate) fn cancel_for_reset(&self, client: &ClientId, request: &RequestId) {
        for (key, cancellation) in self
            .shared
            .cancellations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
        {
            if &key.client_id != client || &key.request_id != request {
                cancellation.cancel();
            }
        }
    }

    /// Stops admission, cancels queued/in-flight work, drains workers, and
    /// shuts down the product executor.
    ///
    /// Repeated calls are idempotent. This function may block until accepted
    /// cooperative work returns, performs no socket IO itself, and does not
    /// panic when a worker previously panicked.
    pub fn shutdown(&self) {
        if !self.shared.closed.swap(true, Ordering::AcqRel) {
            self.shared.sender.close();
            self.shared.force_sender.close();
            self.shared.control_sender.close();
            self.shared.cancel_all();
        }
        let workers = std::mem::take(
            &mut *self
                .workers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        for worker in workers {
            let _ = worker.join();
        }
        if !self.shared.executor_shutdown.swap(true, Ordering::AcqRel) {
            let _ = catch_unwind(AssertUnwindSafe(|| self.shared.executor.shutdown()));
        }
    }

    fn admit(
        &self,
        authentication: Arc<AuthenticatedRequestContext>,
        connection: Arc<ServerConnectionSession>,
        action: ServerAction,
    ) -> Result<(), ProductDispatchFailure> {
        if self.shared.closed.load(Ordering::Acquire) {
            self.shared.reject_admission(&connection, &action);
            return Err(ProductDispatchFailure::Closed);
        }
        match action {
            ServerAction::Request {
                request_id,
                trace_context,
                payload,
            } => self.admit_job(
                &connection,
                &ServerAction::Request {
                    request_id: request_id.clone(),
                    trace_context: trace_context.clone(),
                    payload: payload.clone(),
                },
                ProductJob::Request(ProductJobContext::new(
                    authentication,
                    connection.clone(),
                    request_id,
                    trace_context,
                    payload,
                )),
            ),
            ServerAction::OpenStream {
                request_id,
                stream_id,
                trace_context,
                payload,
            } => self.admit_job(
                &connection,
                &ServerAction::OpenStream {
                    request_id: request_id.clone(),
                    stream_id: stream_id.clone(),
                    trace_context: trace_context.clone(),
                    payload: payload.clone(),
                },
                ProductJob::Stream(ProductStreamJobContext::new(
                    authentication,
                    connection.clone(),
                    request_id,
                    stream_id,
                    trace_context,
                    payload,
                    None,
                )),
            ),
            ServerAction::ResumeStream {
                request_id,
                stream_id,
                server_epoch,
                after,
                trace_context,
                payload,
            } => self.admit_job(
                &connection,
                &ServerAction::ResumeStream {
                    request_id: request_id.clone(),
                    stream_id: stream_id.clone(),
                    server_epoch: server_epoch.clone(),
                    after,
                    trace_context: trace_context.clone(),
                    payload: payload.clone(),
                },
                ProductJob::Stream(ProductStreamJobContext::new(
                    authentication,
                    connection.clone(),
                    request_id,
                    stream_id,
                    trace_context,
                    payload,
                    Some(ProductStreamResume {
                        server_epoch,
                        after,
                    }),
                )),
            ),
            ServerAction::CancelRequest {
                request_id,
                request_kind,
            } => {
                self.shared.cancel(connection.client_id(), &request_id);
                let payload: Arc<[u8]> = match request_kind {
                    undercurrent_transport_core::ServerRequestKind::FiniteRequest => {
                        let response = response_for_connection(
                            connection.as_ref(),
                            ProductResponse::Error(ProductError {
                                effect_certainty: None,
                                operation: None,
                                category: ProductErrorCategory::Cancelled,
                                disposition: Some(ProductErrorDisposition::UnknownInconclusive),
                                message: "request cancellation was accepted".to_owned(),
                                session_id: None,
                                retryable: false,
                                retry_after_millis: None,
                                limit: None,
                                resynchronization: None,
                            }),
                        );
                        self.shared.codec.encode_response(&response)
                    }
                    undercurrent_transport_core::ServerRequestKind::StreamOpen => {
                        let response = stream_response_for_connection(
                            connection.as_ref(),
                            ProductStreamOpenResponse::Error(ProductStreamError {
                                kind: None,
                                category: ProductErrorCategory::Cancelled,
                                disposition: Some(ProductErrorDisposition::UnknownInconclusive),
                                message: "stream cancellation was accepted".to_owned(),
                                session_id: None,
                                retryable: false,
                                retry_after_millis: None,
                                limit: None,
                                resynchronization: None,
                            }),
                        );
                        self.shared.codec.encode_stream_open_response(&response)
                    }
                }
                .map(Arc::from)
                .map_err(|_| {
                    connection.close();
                    ProductDispatchFailure::Closed
                })?;
                connection
                    .reject_unaccepted_request(&request_id, payload.clone())
                    .map_err(|_| {
                        connection.close();
                        ProductDispatchFailure::Closed
                    })?;
                self.shared.emit(ProductDispatchEvent {
                    name: ProductDispatchEventName::RequestCancelled,
                    connection_id: Some(connection.agreement().connection_id().clone()),
                    server_epoch: Some(connection.agreement().server_epoch().clone()),
                    client_id: Some(connection.client_id().clone()),
                    request_id: Some(request_id),
                    stream_id: None,
                    trace_context: None,
                    operation: None,
                    stream_kind: None,
                    result: ProductDispatchResultCategory::Cancelled,
                    duration_micros: None,
                    response_bytes: Some(payload.len()),
                });
                Ok(())
            }
            ServerAction::CloseStream { stream_id } => {
                let client_id = connection.client_id().clone();
                let executor = &self.shared.executor;
                let _ = catch_unwind(AssertUnwindSafe(|| {
                    executor.close_stream(&client_id, &stream_id)
                }));
                Ok(())
            }
            ServerAction::CloseConnection => {
                let client_id = connection.client_id().clone();
                self.shared.cancel_client(&client_id);
                let executor = &self.shared.executor;
                let result =
                    catch_unwind(AssertUnwindSafe(|| executor.close_connection(&client_id)));
                self.shared.emit(ProductDispatchEvent {
                    name: if result.is_ok() {
                        ProductDispatchEventName::ConnectionReleased
                    } else {
                        ProductDispatchEventName::ExecutorPanicked
                    },
                    connection_id: Some(connection.agreement().connection_id().clone()),
                    server_epoch: Some(connection.agreement().server_epoch().clone()),
                    client_id: Some(client_id),
                    request_id: None,
                    stream_id: None,
                    trace_context: None,
                    operation: None,
                    stream_kind: None,
                    result: if result.is_ok() {
                        ProductDispatchResultCategory::Success
                    } else {
                        ProductDispatchResultCategory::Internal
                    },
                    duration_micros: None,
                    response_bytes: None,
                });
                Ok(())
            }
        }
    }

    fn admit_job(
        &self,
        connection: &Arc<ServerConnectionSession>,
        action: &ServerAction,
        job: ProductJob,
    ) -> Result<(), ProductDispatchFailure> {
        let key = job.key();
        self.shared.register(key.clone(), job.cancellation());
        let sender = match &job {
            ProductJob::Request(context) => {
                match self.shared.codec.decode_request(&context.payload) {
                    Ok(ProductRequest::StopSession(request)) if request.force => {
                        &self.shared.force_sender
                    }
                    Ok(ProductRequest::RestartSession(request)) if request.force => {
                        &self.shared.force_sender
                    }
                    Ok(request)
                        if matches!(
                            request.operation(),
                            ProductOperation::ShutdownDaemon
                                | ProductOperation::GetDaemonStatus
                                | ProductOperation::ListSessions
                                | ProductOperation::GetSessionDetail
                        ) =>
                    {
                        &self.shared.control_sender
                    }
                    _ => &self.shared.sender,
                }
            }
            _ => &self.shared.sender,
        };
        match sender.try_send(job) {
            Ok(()) => {
                self.shared.admitted_actions.fetch_add(1, Ordering::AcqRel);
                self.shared.emit(ProductDispatchEvent {
                    name: ProductDispatchEventName::ActionAdmitted,
                    connection_id: Some(connection.agreement().connection_id().clone()),
                    server_epoch: Some(connection.agreement().server_epoch().clone()),
                    client_id: Some(key.client_id),
                    request_id: Some(key.request_id),
                    stream_id: action_stream_id(action),
                    trace_context: action_trace_context(action),
                    operation: None,
                    stream_kind: None,
                    result: ProductDispatchResultCategory::Success,
                    duration_micros: None,
                    response_bytes: None,
                });
                Ok(())
            }
            Err(TrySendError::Full(_)) => {
                self.shared.remove(&key);
                self.shared.reject_admission(connection, action);
                Err(ProductDispatchFailure::Saturated {
                    limit: self.shared.limits.queued_actions,
                })
            }
            Err(TrySendError::Closed(_)) => {
                self.shared.remove(&key);
                self.shared.reject_admission(connection, action);
                Err(ProductDispatchFailure::Closed)
            }
        }
    }
}

impl AuthenticatedActionDispatcher for BoundedProductDispatcher {
    fn dispatch_authenticated(
        &self,
        authentication: Arc<AuthenticatedRequestContext>,
        connection: Arc<ServerConnectionSession>,
        action: ServerAction,
    ) -> Result<(), ServerDispatchError> {
        self.admit(authentication, connection, action)
            .map_err(ServerDispatchError::new)
    }

    fn reject_authenticated_admission(
        &self,
        _authentication: Arc<AuthenticatedRequestContext>,
        connection: Arc<ServerConnectionSession>,
        rejection: ServerAdmissionRejection,
    ) -> Result<bool, ServerDispatchError> {
        let overload = rejection.overload();
        let (retryable, message) = overload_retry(overload.retry());
        let payload = match rejection.response_kind() {
            ServerAdmissionResponseKind::FiniteRequest => {
                let response = response_for_connection(
                    connection.as_ref(),
                    ProductResponse::Error(ProductError {
                        effect_certainty: None,
                        operation: None,
                        category: ProductErrorCategory::CapacityExceeded,
                        disposition: Some(ProductErrorDisposition::CapacityRejected),
                        message: message.to_owned(),
                        session_id: None,
                        retryable,
                        retry_after_millis: None,
                        limit: overload.limit(),
                        resynchronization: None,
                    }),
                );
                self.shared.codec.encode_response(&response)
            }
            ServerAdmissionResponseKind::StreamOpen => {
                let response = stream_response_for_connection(
                    connection.as_ref(),
                    ProductStreamOpenResponse::Error(ProductStreamError {
                        kind: None,
                        category: ProductErrorCategory::CapacityExceeded,
                        disposition: Some(ProductErrorDisposition::CapacityRejected),
                        message: message.to_owned(),
                        session_id: None,
                        retryable,
                        retry_after_millis: None,
                        limit: overload.limit(),
                        resynchronization: None,
                    }),
                );
                self.shared.codec.encode_stream_open_response(&response)
            }
        }
        .map(Arc::from)
        .map_err(ServerDispatchError::new)?;
        connection
            .reject_unaccepted_request(rejection.request_id(), payload)
            .map_err(ServerDispatchError::new)?;
        self.shared.rejected_actions.fetch_add(1, Ordering::AcqRel);
        self.shared.emit(ProductDispatchEvent {
            name: ProductDispatchEventName::AdmissionRejected,
            connection_id: Some(connection.agreement().connection_id().clone()),
            server_epoch: Some(connection.agreement().server_epoch().clone()),
            client_id: Some(connection.client_id().clone()),
            request_id: Some(rejection.request_id().clone()),
            stream_id: None,
            trace_context: None,
            operation: None,
            stream_kind: None,
            result: ProductDispatchResultCategory::Capacity,
            duration_micros: None,
            response_bytes: None,
        });
        Ok(true)
    }
}

fn overload_retry(
    guidance: undercurrent_multiplex::MultiplexRetryGuidance,
) -> (bool, &'static str) {
    match guidance {
        undercurrent_multiplex::MultiplexRetryGuidance::AfterCapacityAvailable => (
            true,
            "connection capacity is exhausted; retry only after capacity becomes available",
        ),
        undercurrent_multiplex::MultiplexRetryGuidance::AfterQueueDrain => (
            true,
            "connection queue is full; retry only after queued work makes progress",
        ),
        undercurrent_multiplex::MultiplexRetryGuidance::AfterWindowUpdate => (
            true,
            "stream credit is exhausted; retry only after a window update",
        ),
        undercurrent_multiplex::MultiplexRetryGuidance::DoNotRetryUnchanged => (
            false,
            "the unchanged request exceeds a fixed connection limit and must not be retried",
        ),
    }
}

impl Drop for BoundedProductDispatcher {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct DispatcherShared {
    codec: ProductCodec,
    fallback_response: Arc<[u8]>,
    legacy_fallback_response: Arc<[u8]>,
    terminal_codec: TerminalOutputCodec,
    executor: Arc<dyn ProductActionExecutor>,
    idempotency: Arc<BoundedRequestIdempotencyStore>,
    telemetry: Arc<dyn ProductDispatchTelemetry>,
    limits: ProductDispatcherLimits,
    sender: Sender<ProductJob>,
    force_sender: Sender<ProductJob>,
    control_sender: Sender<ProductJob>,
    cancellations: Mutex<BTreeMap<ProductJobKey, ProductCancellation>>,
    active_workers: AtomicUsize,
    admitted_actions: AtomicU64,
    rejected_actions: AtomicU64,
    closed: AtomicBool,
    executor_shutdown: AtomicBool,
}

impl DispatcherShared {
    fn register(&self, key: ProductJobKey, cancellation: ProductCancellation) {
        self.cancellations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(key, cancellation);
    }

    fn remove(&self, key: &ProductJobKey) {
        self.cancellations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(key);
    }

    fn cancel(&self, client_id: &ClientId, request_id: &RequestId) {
        if let Some(cancellation) = self
            .cancellations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&ProductJobKey {
                client_id: client_id.clone(),
                request_id: request_id.clone(),
            })
        {
            cancellation.cancel();
        }
    }

    fn cancel_client(&self, client_id: &ClientId) {
        let mut cancellations = self
            .cancellations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (key, cancellation) in cancellations.iter() {
            if &key.client_id == client_id {
                cancellation.cancel();
            }
        }
        cancellations.retain(|key, _| &key.client_id != client_id);
    }

    fn cancel_all(&self) {
        let mut cancellations = self
            .cancellations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for cancellation in cancellations.values() {
            cancellation.cancel();
        }
        cancellations.clear();
    }

    fn reject_admission(&self, connection: &ServerConnectionSession, action: &ServerAction) {
        self.rejected_actions.fetch_add(1, Ordering::AcqRel);
        self.emit(ProductDispatchEvent {
            name: ProductDispatchEventName::AdmissionRejected,
            connection_id: Some(connection.agreement().connection_id().clone()),
            server_epoch: Some(connection.agreement().server_epoch().clone()),
            client_id: Some(connection.client_id().clone()),
            request_id: action_request_id(action),
            stream_id: action_stream_id(action),
            trace_context: action_trace_context(action),
            operation: None,
            stream_kind: None,
            result: ProductDispatchResultCategory::Capacity,
            duration_micros: None,
            response_bytes: None,
        });
    }

    fn emit(&self, event: ProductDispatchEvent) {
        self.telemetry.emit(event);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ProductJobKey {
    client_id: ClientId,
    request_id: RequestId,
}

enum ProductJob {
    Request(ProductJobContext),
    Stream(ProductStreamJobContext),
}

impl ProductJob {
    fn key(&self) -> ProductJobKey {
        match self {
            Self::Request(context) => context.key(),
            Self::Stream(context) => context.key(),
        }
    }

    fn cancellation(&self) -> ProductCancellation {
        match self {
            Self::Request(context) => context.cancellation.clone(),
            Self::Stream(context) => context.cancellation.clone(),
        }
    }

    fn enqueued_at(&self) -> Instant {
        match self {
            Self::Request(context) => context.enqueued_at,
            Self::Stream(context) => context.enqueued_at,
        }
    }

    fn event(
        &self,
        name: ProductDispatchEventName,
        result: ProductDispatchResultCategory,
        duration_micros: u64,
    ) -> ProductDispatchEvent {
        match self {
            Self::Request(context) => ProductDispatchEvent {
                name,
                connection_id: Some(context.connection.agreement().connection_id().clone()),
                server_epoch: Some(context.connection.agreement().server_epoch().clone()),
                client_id: Some(context.connection.client_id().clone()),
                request_id: Some(context.request_id.clone()),
                stream_id: None,
                trace_context: Some(context.trace_context.clone()),
                operation: None,
                stream_kind: None,
                result,
                duration_micros: Some(duration_micros),
                response_bytes: None,
            },
            Self::Stream(context) => ProductDispatchEvent {
                name,
                connection_id: Some(context.connection.agreement().connection_id().clone()),
                server_epoch: Some(context.connection.agreement().server_epoch().clone()),
                client_id: Some(context.connection.client_id().clone()),
                request_id: Some(context.request_id.clone()),
                stream_id: Some(context.stream_id.clone()),
                trace_context: Some(context.trace_context.clone()),
                operation: None,
                stream_kind: None,
                result,
                duration_micros: Some(duration_micros),
                response_bytes: None,
            },
        }
    }
}

struct ProductJobContext {
    authentication: Arc<AuthenticatedRequestContext>,
    connection: Arc<ServerConnectionSession>,
    request_id: RequestId,
    trace_context: TraceContext,
    payload: Arc<[u8]>,
    cancellation: ProductCancellation,
    response_write: Arc<undercurrent_transport_core::ServerResponseWrite>,
    enqueued_at: Instant,
}

impl ProductJobContext {
    fn new(
        authentication: Arc<AuthenticatedRequestContext>,
        connection: Arc<ServerConnectionSession>,
        request_id: RequestId,
        trace_context: TraceContext,
        payload: Arc<[u8]>,
    ) -> Self {
        Self {
            authentication,
            connection,
            request_id,
            trace_context,
            payload,
            cancellation: ProductCancellation::new(),
            response_write: Arc::default(),
            enqueued_at: Instant::now(),
        }
    }

    fn key(&self) -> ProductJobKey {
        ProductJobKey {
            client_id: self.connection.client_id().clone(),
            request_id: self.request_id.clone(),
        }
    }
}

struct ProductStreamJobContext {
    authentication: Arc<AuthenticatedRequestContext>,
    connection: Arc<ServerConnectionSession>,
    request_id: RequestId,
    stream_id: StreamId,
    trace_context: TraceContext,
    payload: Arc<[u8]>,
    resume: Option<ProductStreamResume>,
    cancellation: ProductCancellation,
    enqueued_at: Instant,
}

impl ProductStreamJobContext {
    fn new(
        authentication: Arc<AuthenticatedRequestContext>,
        connection: Arc<ServerConnectionSession>,
        request_id: RequestId,
        stream_id: StreamId,
        trace_context: TraceContext,
        payload: Arc<[u8]>,
        resume: Option<ProductStreamResume>,
    ) -> Self {
        Self {
            authentication,
            connection,
            request_id,
            stream_id,
            trace_context,
            payload,
            resume,
            cancellation: ProductCancellation::new(),
            enqueued_at: Instant::now(),
        }
    }

    fn key(&self) -> ProductJobKey {
        ProductJobKey {
            client_id: self.connection.client_id().clone(),
            request_id: self.request_id.clone(),
        }
    }
}

fn worker_loop(shared: Arc<DispatcherShared>, receiver: Receiver<ProductJob>) {
    while let Ok(job) = receiver.recv_blocking() {
        shared.active_workers.fetch_add(1, Ordering::AcqRel);
        shared.emit(job.event(
            ProductDispatchEventName::ActionStarted,
            ProductDispatchResultCategory::Success,
            duration_micros(job.enqueued_at().elapsed()),
        ));
        let execution_started = Instant::now();
        let key = job.key();
        let cancellation = job.cancellation();
        let mut finished_event = job.event(
            ProductDispatchEventName::ActionFinished,
            ProductDispatchResultCategory::Success,
            0,
        );
        match job {
            ProductJob::Request(context) => execute_request(&shared, context),
            ProductJob::Stream(context) => execute_stream(&shared, context),
        }
        shared.remove(&key);
        shared.active_workers.fetch_sub(1, Ordering::AcqRel);
        finished_event.result = if cancellation.is_cancelled() {
            ProductDispatchResultCategory::Cancelled
        } else {
            ProductDispatchResultCategory::Success
        };
        finished_event.duration_micros = Some(duration_micros(execution_started.elapsed()));
        shared.emit(finished_event);
    }
}

fn execute_request(shared: &DispatcherShared, job: ProductJobContext) {
    let _incident = undercurrent_diagnostics::IncidentScope::enter(
        job.trace_context
            .correlation_id
            .as_ref()
            .map(|id| id.as_str()),
    );
    let span = product_request_span(
        job.connection.agreement().connection_id(),
        job.connection.agreement().server_epoch(),
        job.connection.client_id(),
        &job.request_id,
        &job.trace_context,
    );
    let _entered = span.enter();
    let started_at = Instant::now();
    if job.cancellation.is_cancelled() {
        record_product_request_outcome(
            &span,
            None,
            ProductDispatchResultCategory::Cancelled,
            duration_micros(started_at.elapsed()),
            None,
        );
        return;
    }
    let request = match shared.codec.decode_request(&job.payload) {
        Ok(request) => request,
        Err(_) => {
            settle_response(
                shared,
                &job,
                ProductRequestTrace::new(&span, None),
                invalid_response(
                    None,
                    ProductErrorCategory::InvalidRequest,
                    "request payload is malformed",
                ),
                ProductDispatchEventName::RequestFailed,
                ProductDispatchResultCategory::InvalidRequest,
                started_at,
            );
            return;
        }
    };
    let operation = request.operation();
    let session_id = validated_trace_session_id(request.target_session_id());
    record_product_operation(&span, operation);
    if request.session_revision_precondition().is_some()
        && !job
            .connection
            .agreement()
            .capabilities()
            .iter()
            .any(|capability| {
                capability.as_str() == product_capabilities::SESSION_REVISION_PRECONDITION
            })
    {
        settle_response(
            shared,
            &job,
            ProductRequestTrace::new(&span, session_id.as_deref()),
            invalid_response(
                Some(operation),
                ProductErrorCategory::ProtocolMismatch,
                "session revision preconditions were not negotiated for this connection",
            ),
            ProductDispatchEventName::RequestFailed,
            ProductDispatchResultCategory::Rejected,
            started_at,
        );
        return;
    }
    if matches!(
        &request,
        ProductRequest::StartSession(start)
            if start.output_capture_mode == OutputCaptureModeDto::Opaque
    ) && !opaque_output_capture_negotiated(&job.connection)
    {
        settle_response(
            shared,
            &job,
            ProductRequestTrace::new(&span, session_id.as_deref()),
            invalid_response(
                Some(operation),
                ProductErrorCategory::ProtocolMismatch,
                "opaque output capture was not negotiated for this connection",
            ),
            ProductDispatchEventName::RequestFailed,
            ProductDispatchResultCategory::Rejected,
            started_at,
        );
        return;
    }
    if operation.is_mutating()
        && undercurrent_protocol::validate_product_request_contract(&request).is_err()
    {
        settle_response(
            shared,
            &job,
            ProductRequestTrace::new(&span, session_id.as_deref()),
            invalid_response(
                Some(operation),
                ProductErrorCategory::InvalidRequest,
                "current 0.1.0-beta mutation contract metadata is required",
            ),
            ProductDispatchEventName::RequestFailed,
            ProductDispatchResultCategory::InvalidRequest,
            started_at,
        );
        return;
    }
    shared.emit(ProductDispatchEvent {
        name: ProductDispatchEventName::RequestDecoded,
        connection_id: Some(job.connection.agreement().connection_id().clone()),
        server_epoch: Some(job.connection.agreement().server_epoch().clone()),
        client_id: Some(job.connection.client_id().clone()),
        request_id: Some(job.request_id.clone()),
        stream_id: None,
        trace_context: Some(job.trace_context.clone()),
        operation: Some(operation),
        stream_kind: None,
        result: ProductDispatchResultCategory::Success,
        duration_micros: Some(duration_micros(started_at.elapsed())),
        response_bytes: None,
    });

    let idempotency = match reserve_idempotency(shared, &job, &request) {
        IdempotencyDecision::Execute(scope) => scope,
        IdempotencyDecision::Replay(response) => {
            settle_encoded(
                shared,
                &job,
                ProductRequestTrace::new(&span, session_id.as_deref()),
                response,
                operation,
                ProductDispatchResultCategory::Success,
                started_at,
            );
            return;
        }
        IdempotencyDecision::Reject(response) => {
            settle_response(
                shared,
                &job,
                ProductRequestTrace::new(&span, session_id.as_deref()),
                response,
                ProductDispatchEventName::RequestFailed,
                ProductDispatchResultCategory::Rejected,
                started_at,
            );
            return;
        }
    };

    if job.cancellation.is_cancelled() {
        if let Some(scope) = idempotency.as_ref() {
            shared.idempotency.abandon(scope);
        }
        record_product_request_outcome(
            &span,
            session_id.as_deref(),
            ProductDispatchResultCategory::Cancelled,
            duration_micros(started_at.elapsed()),
            None,
        );
        return;
    }
    let context = ProductRequestContext {
        authentication: job.authentication.clone(),
        connection_id: job.connection.agreement().connection_id().clone(),
        server_epoch: job.connection.agreement().server_epoch().clone(),
        client_id: job.connection.client_id().clone(),
        request_id: job.request_id.clone(),
        trace_context: job.trace_context.clone(),
        cancellation: job.cancellation.clone(),
        response_write: job.response_write.clone(),
    };
    let response = match catch_unwind(AssertUnwindSafe(|| {
        shared.executor.execute(&context, request)
    })) {
        Ok(response) if response.operation() == Some(operation) => response,
        Ok(_) => internal_response(Some(operation)),
        Err(_) => {
            shared.emit(ProductDispatchEvent {
                name: ProductDispatchEventName::ExecutorPanicked,
                connection_id: Some(context.connection_id.clone()),
                server_epoch: Some(context.server_epoch.clone()),
                client_id: Some(context.client_id.clone()),
                request_id: Some(context.request_id.clone()),
                stream_id: None,
                trace_context: Some(context.trace_context.clone()),
                operation: Some(operation),
                stream_kind: None,
                result: ProductDispatchResultCategory::Internal,
                duration_micros: Some(duration_micros(started_at.elapsed())),
                response_bytes: None,
            });
            internal_response(Some(operation))
        }
    };
    let execution_result = response_result_category(&response);
    let session_id = validated_trace_session_id(response.session_id()).or(session_id);
    let encoded = encode_response_with_fallback(shared, response, Some(operation));
    // Retain the canonical result before queueing a response: delivery can fail
    // after effects commit, and a retry may negotiate different optional fields.
    // settle_encoded applies the current peer's projection after this retention.
    // See [response-loss ordering](../docs/internals/requests/request-flow.md#what-each-boundary-owns).
    if let Some(scope) = idempotency {
        let deadline = Instant::now()
            .checked_add(shared.limits.idempotency_retention)
            .unwrap_or_else(Instant::now);
        match shared
            .idempotency
            .complete(&scope, encoded.clone(), deadline)
        {
            Ok(RequestIdempotencyCompletion::Completed)
            | Ok(RequestIdempotencyCompletion::AlreadyCompleted) => {}
            Ok(RequestIdempotencyCompletion::ResponseConflict) | Err(_) => {
                job.connection.close();
                let duration = duration_micros(started_at.elapsed());
                record_product_request_outcome(
                    &span,
                    session_id.as_deref(),
                    ProductDispatchResultCategory::Internal,
                    duration,
                    None,
                );
                shared.emit(ProductDispatchEvent {
                    name: ProductDispatchEventName::RequestFailed,
                    connection_id: Some(context.connection_id),
                    server_epoch: Some(context.server_epoch),
                    client_id: Some(context.client_id),
                    request_id: Some(context.request_id),
                    stream_id: None,
                    trace_context: Some(context.trace_context),
                    operation: Some(operation),
                    stream_kind: None,
                    result: ProductDispatchResultCategory::Internal,
                    duration_micros: Some(duration),
                    response_bytes: None,
                });
                return;
            }
        }
    }
    settle_encoded(
        shared,
        &job,
        ProductRequestTrace::new(&span, session_id.as_deref()),
        encoded,
        operation,
        execution_result,
        started_at,
    );
}

fn execute_stream(shared: &DispatcherShared, job: ProductStreamJobContext) {
    let _incident = undercurrent_diagnostics::IncidentScope::enter(
        job.trace_context
            .correlation_id
            .as_ref()
            .map(|id| id.as_str()),
    );
    let span = product_stream_span(
        job.connection.agreement().connection_id(),
        job.connection.agreement().server_epoch(),
        job.connection.client_id(),
        &job.request_id,
        &job.stream_id,
        &job.trace_context,
    );
    let _entered = span.enter();
    let started_at = Instant::now();
    if job.cancellation.is_cancelled() {
        record_product_stream_outcome(
            &span,
            None,
            ProductDispatchResultCategory::Cancelled,
            duration_micros(started_at.elapsed()),
        );
        return;
    }
    let specification = match shared.codec.decode_stream_spec(&job.payload) {
        Ok(specification) => specification,
        Err(_) => {
            reject_stream(
                shared,
                &job,
                &span,
                None,
                ProductStreamError {
                    kind: None,
                    category: ProductErrorCategory::InvalidRequest,
                    disposition: Some(ProductErrorDisposition::SemanticRejection),
                    message: "stream specification is malformed".into(),
                    session_id: None,
                    retryable: false,
                    retry_after_millis: None,
                    limit: None,
                    resynchronization: None,
                },
                ProductDispatchResultCategory::InvalidRequest,
                started_at,
            );
            return;
        }
    };
    let kind = specification.kind();
    let session_id = validated_trace_session_id(specification.target_session_id());
    record_product_stream_kind(&span, kind);
    let policy = ProductStreamPolicy::for_specification(&specification);
    if job.resume.is_some() && !policy.resumable {
        reject_stream(
            shared,
            &job,
            &span,
            session_id.as_deref(),
            ProductStreamError {
                kind: Some(kind),
                category: ProductErrorCategory::FailedPrecondition,
                disposition: Some(ProductErrorDisposition::SemanticRejection),
                message: "this logical stream kind cannot be resumed".into(),
                session_id: None,
                retryable: false,
                retry_after_millis: None,
                limit: None,
                resynchronization: None,
            },
            ProductDispatchResultCategory::Rejected,
            started_at,
        );
        return;
    }
    let context = ProductStreamContext {
        authentication: job.authentication.clone(),
        connection_id: job.connection.agreement().connection_id().clone(),
        server_epoch: job.connection.agreement().server_epoch().clone(),
        client_id: job.connection.client_id().clone(),
        request_id: job.request_id.clone(),
        stream_id: job.stream_id.clone(),
        trace_context: job.trace_context.clone(),
        resume: job.resume.clone(),
        cancellation: job.cancellation.clone(),
    };
    let prepared = match catch_unwind(AssertUnwindSafe(|| {
        shared.executor.prepare_stream(&context, specification)
    })) {
        Ok(Ok(prepared)) => prepared,
        Ok(Err(mut error)) => {
            error.kind = Some(kind);
            reject_stream(
                shared,
                &job,
                &span,
                session_id.as_deref(),
                error,
                ProductDispatchResultCategory::Rejected,
                started_at,
            );
            return;
        }
        Err(_) => {
            shared.emit(ProductDispatchEvent {
                name: ProductDispatchEventName::ExecutorPanicked,
                connection_id: Some(context.connection_id.clone()),
                server_epoch: Some(context.server_epoch.clone()),
                client_id: Some(context.client_id),
                request_id: Some(context.request_id),
                stream_id: Some(context.stream_id),
                trace_context: Some(context.trace_context.clone()),
                operation: None,
                stream_kind: Some(kind),
                result: ProductDispatchResultCategory::Internal,
                duration_micros: Some(duration_micros(started_at.elapsed())),
                response_bytes: None,
            });
            reject_stream(
                shared,
                &job,
                &span,
                session_id.as_deref(),
                ProductStreamError {
                    kind: Some(kind),
                    category: ProductErrorCategory::Internal,
                    disposition: Some(ProductErrorDisposition::InfrastructureFailure),
                    message: "stream preparation failed internally".into(),
                    session_id: None,
                    retryable: false,
                    retry_after_millis: None,
                    limit: None,
                    resynchronization: None,
                },
                ProductDispatchResultCategory::Internal,
                started_at,
            );
            return;
        }
    };
    if job.cancellation.is_cancelled() {
        record_product_stream_outcome(
            &span,
            session_id.as_deref(),
            ProductDispatchResultCategory::Cancelled,
            duration_micros(started_at.elapsed()),
        );
        return;
    }
    shared.emit(ProductDispatchEvent {
        name: ProductDispatchEventName::StreamPrepared,
        connection_id: Some(context.connection_id.clone()),
        server_epoch: Some(context.server_epoch.clone()),
        client_id: Some(context.client_id.clone()),
        request_id: Some(context.request_id.clone()),
        stream_id: Some(context.stream_id.clone()),
        trace_context: Some(context.trace_context.clone()),
        operation: None,
        stream_kind: Some(kind),
        result: ProductDispatchResultCategory::Success,
        duration_micros: Some(duration_micros(started_at.elapsed())),
        response_bytes: None,
    });
    let baseline = job
        .resume
        .as_ref()
        .map_or(StreamSequence::INITIAL, |resume| resume.after);
    let opened = ProductStreamOpenResponse::Opened(ProductStreamOpened {
        kind,
        initial_sequence: baseline.value(),
        resumable: policy.resumable,
        available_from: prepared.available_from(),
    });
    let response = match shared.codec.encode_stream_open_response(&opened) {
        Ok(response) => Arc::from(response),
        Err(_) => {
            job.connection.close();
            record_product_stream_outcome(
                &span,
                session_id.as_deref(),
                ProductDispatchResultCategory::Encoding,
                duration_micros(started_at.elapsed()),
            );
            return;
        }
    };
    let outcome = match job.connection.activate_stream(
        &job.request_id,
        &job.stream_id,
        policy.configuration,
        response,
    ) {
        Ok(outcome) => outcome,
        Err(_) if job.cancellation.is_cancelled() => {
            record_product_stream_outcome(
                &span,
                session_id.as_deref(),
                ProductDispatchResultCategory::Cancelled,
                duration_micros(started_at.elapsed()),
            );
            return;
        }
        Err(_) if job.connection.is_closed() => {
            record_product_stream_outcome(
                &span,
                session_id.as_deref(),
                ProductDispatchResultCategory::Connection,
                duration_micros(started_at.elapsed()),
            );
            return;
        }
        Err(_) => {
            job.connection.close();
            record_product_stream_outcome(
                &span,
                session_id.as_deref(),
                ProductDispatchResultCategory::Connection,
                duration_micros(started_at.elapsed()),
            );
            return;
        }
    };
    let publisher = ProductStreamPublisher::new(
        job.connection.clone(),
        job.stream_id.clone(),
        outcome.last_sequence,
        shared.codec,
        shared.terminal_codec,
    );
    let activation = catch_unwind(AssertUnwindSafe(|| prepared.activate(publisher)));
    if activation.is_err() {
        job.connection.close();
        let duration = duration_micros(started_at.elapsed());
        record_product_stream_outcome(
            &span,
            session_id.as_deref(),
            ProductDispatchResultCategory::Internal,
            duration,
        );
        shared.emit(ProductDispatchEvent {
            name: ProductDispatchEventName::ExecutorPanicked,
            connection_id: Some(context.connection_id.clone()),
            server_epoch: Some(context.server_epoch.clone()),
            client_id: Some(context.client_id),
            request_id: Some(context.request_id),
            stream_id: Some(context.stream_id),
            trace_context: Some(context.trace_context),
            operation: None,
            stream_kind: Some(kind),
            result: ProductDispatchResultCategory::Internal,
            duration_micros: Some(duration),
            response_bytes: None,
        });
        return;
    }
    let duration = duration_micros(started_at.elapsed());
    record_product_stream_outcome(
        &span,
        session_id.as_deref(),
        ProductDispatchResultCategory::Success,
        duration,
    );
    shared.emit(ProductDispatchEvent {
        name: ProductDispatchEventName::StreamActivated,
        connection_id: Some(context.connection_id),
        server_epoch: Some(context.server_epoch),
        client_id: Some(context.client_id),
        request_id: Some(context.request_id),
        stream_id: Some(context.stream_id),
        trace_context: Some(context.trace_context),
        operation: None,
        stream_kind: Some(kind),
        result: ProductDispatchResultCategory::Success,
        duration_micros: Some(duration),
        response_bytes: None,
    });
}

enum IdempotencyDecision {
    Execute(Option<RequestIdempotencyScope>),
    Replay(Arc<[u8]>),
    Reject(ProductResponse),
}

fn reserve_idempotency(
    shared: &DispatcherShared,
    job: &ProductJobContext,
    request: &ProductRequest,
) -> IdempotencyDecision {
    // Runtime cancellation owns a durable authorization-first replay ledger.
    // Replaying this command from the connection-local cache would bypass a
    // fresh exact control/attempt authorization decision after revocation.
    if matches!(request, ProductRequest::CancelRuntimeAttempt(_)) {
        return IdempotencyDecision::Execute(None);
    }
    shared.idempotency.expire_before(Instant::now());
    let Some(raw_key) = request
        .mutation()
        .and_then(|mutation| mutation.idempotency_key.as_deref())
    else {
        return IdempotencyDecision::Execute(None);
    };
    let operation = request.operation();
    let key = match IdempotencyKey::new(raw_key) {
        Ok(key) => key,
        Err(_) => {
            return IdempotencyDecision::Reject(invalid_response(
                Some(operation),
                ProductErrorCategory::InvalidRequest,
                "idempotency key is invalid",
            ));
        }
    };
    let scope = RequestIdempotencyScope::new(job.connection.client_id().clone(), key);
    match shared
        .idempotency
        .reserve(scope.clone(), job.payload.clone())
    {
        Ok(RequestIdempotencyReservation::Reserved) => IdempotencyDecision::Execute(Some(scope)),
        Ok(RequestIdempotencyReservation::Completed { response }) => {
            IdempotencyDecision::Replay(response)
        }
        Ok(RequestIdempotencyReservation::Pending) => {
            IdempotencyDecision::Reject(ProductResponse::Error(ProductError {
                effect_certainty: None,
                operation: Some(operation),
                category: ProductErrorCategory::FailedPrecondition,
                disposition: Some(ProductErrorDisposition::SemanticRejection),
                message: "the idempotent mutation is already in progress".into(),
                session_id: None,
                retryable: true,
                retry_after_millis: Some(50),
                limit: None,
                resynchronization: None,
            }))
        }
        Ok(RequestIdempotencyReservation::PayloadConflict) => {
            IdempotencyDecision::Reject(invalid_response(
                Some(operation),
                ProductErrorCategory::FailedPrecondition,
                "idempotency key is already bound to another request",
            ))
        }
        Err(error) => IdempotencyDecision::Reject(idempotency_error(operation, error)),
    }
}

fn idempotency_error(
    operation: ProductOperation,
    error: RequestIdempotencyError,
) -> ProductResponse {
    let (category, message, limit) = match error {
        RequestIdempotencyError::InvalidRequestSize { maximum, .. } => (
            ProductErrorCategory::CapacityExceeded,
            "idempotent request exceeds its retained byte limit",
            Some(u64::from(maximum)),
        ),
        RequestIdempotencyError::RecordCapacityExceeded { limit } => (
            ProductErrorCategory::CapacityExceeded,
            "idempotency record capacity is exhausted",
            Some(u64::try_from(limit).unwrap_or(u64::MAX)),
        ),
        RequestIdempotencyError::ByteCapacityExceeded { limit, .. } => (
            ProductErrorCategory::CapacityExceeded,
            "idempotency retained byte capacity is exhausted",
            Some(limit),
        ),
        RequestIdempotencyError::InvalidResponseSize { maximum, .. } => (
            ProductErrorCategory::Internal,
            "idempotency settlement failed internally",
            Some(u64::from(maximum)),
        ),
        RequestIdempotencyError::ReservationNotFound { .. } => (
            ProductErrorCategory::Internal,
            "idempotency settlement failed internally",
            None,
        ),
    };
    ProductResponse::Error(ProductError {
        effect_certainty: None,
        operation: Some(operation),
        category,
        disposition: Some(ProductErrorDisposition::default_for_category(category)),
        message: message.into(),
        session_id: None,
        retryable: category == ProductErrorCategory::CapacityExceeded,
        retry_after_millis: None,
        limit,
        resynchronization: None,
    })
}

#[derive(Clone, Copy)]
struct ProductRequestTrace<'a> {
    span: &'a tracing::Span,
    session_id: Option<&'a str>,
}

impl<'a> ProductRequestTrace<'a> {
    const fn new(span: &'a tracing::Span, session_id: Option<&'a str>) -> Self {
        Self { span, session_id }
    }
}

fn settle_response(
    shared: &DispatcherShared,
    job: &ProductJobContext,
    trace: ProductRequestTrace<'_>,
    response: ProductResponse,
    event: ProductDispatchEventName,
    result: ProductDispatchResultCategory,
    started_at: Instant,
) {
    let operation = response.operation();
    let session_id = validated_trace_session_id(response.session_id())
        .or_else(|| trace.session_id.map(str::to_owned));
    let encoded = response_bytes_for_connection(
        shared,
        job.connection.as_ref(),
        encode_response_with_fallback(shared, response, operation),
    );
    let response_bytes = encoded.len();
    let settlement = job
        .connection
        .complete_request(&job.request_id, encoded)
        .is_ok();
    let result = if settlement {
        result
    } else {
        ProductDispatchResultCategory::Connection
    };
    let duration = duration_micros(started_at.elapsed());
    record_product_request_outcome(
        trace.span,
        session_id.as_deref(),
        result,
        duration,
        Some(response_bytes),
    );
    shared.emit(ProductDispatchEvent {
        name: if settlement {
            event
        } else {
            ProductDispatchEventName::RequestFailed
        },
        connection_id: Some(job.connection.agreement().connection_id().clone()),
        server_epoch: Some(job.connection.agreement().server_epoch().clone()),
        client_id: Some(job.connection.client_id().clone()),
        request_id: Some(job.request_id.clone()),
        stream_id: None,
        trace_context: Some(job.trace_context.clone()),
        operation,
        stream_kind: None,
        result,
        duration_micros: Some(duration),
        response_bytes: Some(response_bytes),
    });
}

fn settle_encoded(
    shared: &DispatcherShared,
    job: &ProductJobContext,
    trace: ProductRequestTrace<'_>,
    response: Arc<[u8]>,
    operation: ProductOperation,
    result: ProductDispatchResultCategory,
    started_at: Instant,
) {
    let response = response_bytes_for_connection(shared, job.connection.as_ref(), response);
    let response_bytes = response.len();
    let settlement = job
        .connection
        .complete_request_observed(&job.request_id, response, job.response_write.clone())
        .is_ok();
    let result = if settlement {
        result
    } else {
        ProductDispatchResultCategory::Connection
    };
    let duration = duration_micros(started_at.elapsed());
    record_product_request_outcome(
        trace.span,
        trace.session_id,
        result,
        duration,
        Some(response_bytes),
    );
    shared.emit(ProductDispatchEvent {
        name: if settlement {
            ProductDispatchEventName::RequestCompleted
        } else {
            ProductDispatchEventName::RequestFailed
        },
        connection_id: Some(job.connection.agreement().connection_id().clone()),
        server_epoch: Some(job.connection.agreement().server_epoch().clone()),
        client_id: Some(job.connection.client_id().clone()),
        request_id: Some(job.request_id.clone()),
        stream_id: None,
        trace_context: Some(job.trace_context.clone()),
        operation: Some(operation),
        stream_kind: None,
        result,
        duration_micros: Some(duration),
        response_bytes: Some(response_bytes),
    });
}

fn reject_stream(
    shared: &DispatcherShared,
    job: &ProductStreamJobContext,
    span: &tracing::Span,
    request_session_id: Option<&str>,
    error: ProductStreamError,
    result: ProductDispatchResultCategory,
    started_at: Instant,
) {
    let kind = error.kind;
    let session_id = validated_trace_session_id(error.session_id.as_deref())
        .or_else(|| request_session_id.map(str::to_owned));
    let response = stream_response_for_connection(
        job.connection.as_ref(),
        ProductStreamOpenResponse::Error(error),
    );
    let encoded = shared
        .codec
        .encode_stream_open_response(&response)
        .map(Arc::from);
    let settled = encoded.is_ok_and(|payload| {
        job.connection
            .reject_request(&job.request_id, payload)
            .is_ok()
    });
    if !settled && !job.cancellation.is_cancelled() {
        job.connection.close();
    }
    let result = if settled {
        result
    } else {
        ProductDispatchResultCategory::Connection
    };
    let duration = duration_micros(started_at.elapsed());
    record_product_stream_outcome(span, session_id.as_deref(), result, duration);
    shared.emit(ProductDispatchEvent {
        name: ProductDispatchEventName::StreamRejected,
        connection_id: Some(job.connection.agreement().connection_id().clone()),
        server_epoch: Some(job.connection.agreement().server_epoch().clone()),
        client_id: Some(job.connection.client_id().clone()),
        request_id: Some(job.request_id.clone()),
        stream_id: Some(job.stream_id.clone()),
        trace_context: Some(job.trace_context.clone()),
        operation: None,
        stream_kind: kind,
        result,
        duration_micros: Some(duration),
        response_bytes: None,
    });
}

fn validated_trace_session_id(value: Option<&str>) -> Option<String> {
    value
        .and_then(|value| SessionId::new(value.to_owned()).ok())
        .map(SessionId::into_string)
}

const fn response_result_category(response: &ProductResponse) -> ProductDispatchResultCategory {
    match response {
        ProductResponse::Success(_) => ProductDispatchResultCategory::Success,
        ProductResponse::Error(error) => match error.category {
            ProductErrorCategory::InvalidRequest | ProductErrorCategory::InvalidConfiguration => {
                ProductDispatchResultCategory::InvalidRequest
            }
            ProductErrorCategory::Cancelled => ProductDispatchResultCategory::Cancelled,
            ProductErrorCategory::CapacityExceeded => ProductDispatchResultCategory::Capacity,
            ProductErrorCategory::Internal => ProductDispatchResultCategory::Internal,
            ProductErrorCategory::NotFound
            | ProductErrorCategory::NameConflict
            | ProductErrorCategory::FailedPrecondition
            | ProductErrorCategory::RevisionPreconditionFailed
            | ProductErrorCategory::Unauthenticated
            | ProductErrorCategory::PermissionDenied
            | ProductErrorCategory::TimedOut
            | ProductErrorCategory::ReplayUnavailable
            | ProductErrorCategory::CursorExpired
            | ProductErrorCategory::ProtocolMismatch
            | ProductErrorCategory::Process
            | ProductErrorCategory::Terminal
            | ProductErrorCategory::Persistence
            | ProductErrorCategory::Transport => ProductDispatchResultCategory::Rejected,
        },
    }
}

fn encode_response_with_fallback(
    shared: &DispatcherShared,
    response: ProductResponse,
    operation: Option<ProductOperation>,
) -> Arc<[u8]> {
    shared
        .codec
        .encode_response(&response)
        .or_else(|_| shared.codec.encode_response(&internal_response(operation)))
        .map(Arc::from)
        .unwrap_or_else(|_| shared.fallback_response.clone())
}

fn response_bytes_for_connection(
    shared: &DispatcherShared,
    connection: &ServerConnectionSession,
    canonical: Arc<[u8]>,
) -> Arc<[u8]> {
    let include_disposition = error_disposition_negotiated(connection);
    let include_effect_certainty = error_effect_certainty_negotiated(connection);
    if include_disposition && include_effect_certainty {
        return canonical;
    }
    shared
        .codec
        .decode_response(&canonical)
        .map(|response| {
            response_for_profile(include_disposition, include_effect_certainty, response)
        })
        .and_then(|response| shared.codec.encode_response(&response))
        .map(Arc::from)
        .unwrap_or_else(|_| shared.legacy_fallback_response.clone())
}

fn response_for_connection(
    connection: &ServerConnectionSession,
    response: ProductResponse,
) -> ProductResponse {
    response_for_profile(
        error_disposition_negotiated(connection),
        error_effect_certainty_negotiated(connection),
        response,
    )
}

fn response_for_profile(
    include_disposition: bool,
    include_effect_certainty: bool,
    mut response: ProductResponse,
) -> ProductResponse {
    if !include_disposition && let ProductResponse::Error(error) = &mut response {
        error.disposition = None;
    }
    if !include_effect_certainty && let ProductResponse::Error(error) = &mut response {
        error.effect_certainty = None;
    }
    response
}

fn error_effect_certainty_negotiated(connection: &ServerConnectionSession) -> bool {
    connection
        .agreement()
        .capabilities()
        .iter()
        .any(|capability| capability.as_str() == product_capabilities::ERROR_EFFECT_CERTAINTY)
}

fn stream_response_for_connection(
    connection: &ServerConnectionSession,
    response: ProductStreamOpenResponse,
) -> ProductStreamOpenResponse {
    stream_response_for_profile(error_disposition_negotiated(connection), response)
}

fn stream_response_for_profile(
    include_disposition: bool,
    mut response: ProductStreamOpenResponse,
) -> ProductStreamOpenResponse {
    if !include_disposition && let ProductStreamOpenResponse::Error(error) = &mut response {
        error.disposition = None;
    }
    response
}

fn error_disposition_negotiated(connection: &ServerConnectionSession) -> bool {
    connection
        .agreement()
        .capabilities()
        .iter()
        .any(|capability| capability.as_str() == product_capabilities::ERROR_DISPOSITION)
}

fn opaque_output_capture_negotiated(connection: &ServerConnectionSession) -> bool {
    connection
        .agreement()
        .capabilities()
        .iter()
        .any(|capability| capability.as_str() == product_capabilities::OPAQUE_OUTPUT_CAPTURE)
}

fn invalid_response(
    operation: Option<ProductOperation>,
    category: ProductErrorCategory,
    message: &'static str,
) -> ProductResponse {
    ProductResponse::Error(ProductError {
        effect_certainty: None,
        operation,
        category,
        disposition: Some(ProductErrorDisposition::default_for_category(category)),
        message: message.into(),
        session_id: None,
        retryable: false,
        retry_after_millis: None,
        limit: None,
        resynchronization: None,
    })
}

fn internal_response(operation: Option<ProductOperation>) -> ProductResponse {
    invalid_response(
        operation,
        ProductErrorCategory::Internal,
        "request failed internally",
    )
}

fn duration_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn action_request_id(action: &ServerAction) -> Option<RequestId> {
    match action {
        ServerAction::Request { request_id, .. }
        | ServerAction::CancelRequest { request_id, .. }
        | ServerAction::OpenStream { request_id, .. }
        | ServerAction::ResumeStream { request_id, .. } => Some(request_id.clone()),
        ServerAction::CloseStream { .. } | ServerAction::CloseConnection => None,
    }
}

fn action_stream_id(action: &ServerAction) -> Option<StreamId> {
    match action {
        ServerAction::OpenStream { stream_id, .. }
        | ServerAction::ResumeStream { stream_id, .. }
        | ServerAction::CloseStream { stream_id } => Some(stream_id.clone()),
        ServerAction::Request { .. }
        | ServerAction::CancelRequest { .. }
        | ServerAction::CloseConnection => None,
    }
}

fn action_trace_context(action: &ServerAction) -> Option<TraceContext> {
    match action {
        ServerAction::Request { trace_context, .. }
        | ServerAction::OpenStream { trace_context, .. }
        | ServerAction::ResumeStream { trace_context, .. } => Some(trace_context.clone()),
        ServerAction::CancelRequest { .. }
        | ServerAction::CloseStream { .. }
        | ServerAction::CloseConnection => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_context_revalidates_session_ids_and_preserves_result_categories() {
        assert_eq!(
            validated_trace_session_id(Some("session-1")).as_deref(),
            Some("session-1")
        );
        assert_eq!(validated_trace_session_id(Some("")), None);

        for (category, expected) in [
            (
                ProductErrorCategory::InvalidRequest,
                ProductDispatchResultCategory::InvalidRequest,
            ),
            (
                ProductErrorCategory::CapacityExceeded,
                ProductDispatchResultCategory::Capacity,
            ),
            (
                ProductErrorCategory::Cancelled,
                ProductDispatchResultCategory::Cancelled,
            ),
            (
                ProductErrorCategory::Internal,
                ProductDispatchResultCategory::Internal,
            ),
            (
                ProductErrorCategory::Persistence,
                ProductDispatchResultCategory::Rejected,
            ),
        ] {
            let response = ProductResponse::Error(ProductError {
                effect_certainty: None,
                operation: Some(ProductOperation::GetDaemonStatus),
                category,
                disposition: Some(ProductErrorDisposition::default_for_category(category)),
                message: "safe failure".to_owned(),
                session_id: None,
                retryable: false,
                retry_after_millis: None,
                limit: None,
                resynchronization: None,
            });
            assert_eq!(response_result_category(&response), expected);
        }
    }
}

#[cfg(test)]
#[path = "dispatch_effect_evidence.rs"]
mod effect_evidence_tests;
