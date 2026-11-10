// SPDX-License-Identifier: Apache-2.0

//! Persistent client and subscription ownership across physical reconnection.
//!
//! A supervisor replaces physical clients and installs generation-checked stream
//! slots. Finite requests retain the old physical client and are never replayed.
//! Resume responses stay encoded at this layer. Higher-level owners can register
//! an activation validator before a recovered stream is installed.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use undercurrent_multiplex::{InboundStreamError, InboundStreamItem};
use undercurrent_wire::{ServerEpoch, StreamId, StreamSequence};

use crate::{
    BoundedClientTelemetry, Client, ClientConnector, ClientDiagnosticName, ClientError,
    ClientErrorCategory, ClientMetricName, ClientPendingRequest, ClientRecoveryCancellation,
    ClientRecoveryError, ClientRecoveryJitter, ClientRecoveryPolicy, ClientRuntimeLimits,
    ClientTelemetry, Subscription, SubscriptionCheckpoint,
    recover_client_connection_with_telemetry,
};

/// Stable reason persistent physical recovery stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistentRecoveryFailure {
    /// Runtime recovery bounds were invalid.
    InvalidLimits,
    /// A generated checkpoint set violated stream identity or capacity rules.
    InvalidCheckpoints,
    /// Replacement agreement did not support logical stream resume.
    IncompatibleAgreement,
    /// Configured physical connection attempts were exhausted.
    AttemptsExhausted,
    /// Connector forbids retrying the unchanged connection.
    ConnectorRejected,
    /// Replacement client workers could not start.
    ClientRuntime,
    /// Bounded logical stream resume workers failed.
    ResumeRuntime,
}

impl PersistentRecoveryFailure {
    /// Complete stable persistent recovery failure registry.
    pub const ALL: [Self; 7] = [
        Self::InvalidLimits,
        Self::InvalidCheckpoints,
        Self::IncompatibleAgreement,
        Self::AttemptsExhausted,
        Self::ConnectorRejected,
        Self::ClientRuntime,
        Self::ResumeRuntime,
    ];

    /// Returns the stable snake-case failure name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidLimits => "invalid_limits",
            Self::InvalidCheckpoints => "invalid_checkpoints",
            Self::IncompatibleAgreement => "incompatible_agreement",
            Self::AttemptsExhausted => "attempts_exhausted",
            Self::ConnectorRejected => "connector_rejected",
            Self::ClientRuntime => "client_runtime",
            Self::ResumeRuntime => "resume_runtime",
        }
    }

    /// Reduces a source-retaining recovery error to stable operational state.
    fn from_error(error: &ClientRecoveryError) -> Option<Self> {
        match error {
            ClientRecoveryError::Cancelled { .. } => None,
            ClientRecoveryError::Limits(_) => Some(Self::InvalidLimits),
            ClientRecoveryError::DuplicateStreamId { .. }
            | ClientRecoveryError::TooManyStreams { .. } => Some(Self::InvalidCheckpoints),
            ClientRecoveryError::MissingResumeCapability => Some(Self::IncompatibleAgreement),
            ClientRecoveryError::AttemptsExhausted { .. } => Some(Self::AttemptsExhausted),
            ClientRecoveryError::ConnectorRejected { .. } => Some(Self::ConnectorRejected),
            ClientRecoveryError::Client(_) => Some(Self::ClientRuntime),
            ClientRecoveryError::ResumeWorker(_)
            | ClientRecoveryError::ResumeWorkerPanicked
            | ClientRecoveryError::IncompleteResumeBatch { .. } => Some(Self::ResumeRuntime),
        }
    }
}

/// Current persistent client connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResilientClientPhase {
    /// One physical connection is available for new work.
    Connected,
    /// The supervisor is establishing and restoring a replacement connection.
    Recovering,
    /// Bounded automatic recovery stopped with a stable reason.
    Failed(PersistentRecoveryFailure),
    /// Explicit client shutdown completed.
    Closed,
}

pub(crate) struct ResilientPendingRequest {
    inner: ClientPendingRequest,
}

impl ResilientPendingRequest {
    pub(crate) fn cancel(&self) -> Result<(), ResilientClientError> {
        self.inner.cancel().map_err(Into::into)
    }

    pub(crate) fn wait(self, timeout: Duration) -> Result<Arc<[u8]>, ResilientClientError> {
        self.inner.wait(timeout).map_err(Into::into)
    }
}

/// Bounded persistent client state safe for operational diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResilientClientPressure {
    /// Current physical recovery phase.
    pub phase: ResilientClientPhase,
    /// Monotonic installed physical connection generation.
    pub generation: u64,
    /// Cumulative physical connect attempts including initial connection.
    pub physical_connection_attempts: u64,
    /// Persistent logical subscription handles.
    pub subscriptions: usize,
}

/// Failure constructing a persistent SDK client.
#[derive(Debug)]
pub enum ResilientClientConnectError {
    /// Initial bounded physical connection recovery failed.
    Recovery(ClientRecoveryError),
    /// Operating system could not start the recovery supervisor.
    SupervisorThread(std::io::Error),
}

impl Display for ResilientClientConnectError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Recovery(error) => Display::fmt(error, formatter),
            Self::SupervisorThread(_) => {
                formatter.write_str("persistent SDK recovery supervisor could not start")
            }
        }
    }
}

impl Error for ResilientClientConnectError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Recovery(error) => Some(error),
            Self::SupervisorThread(error) => Some(error),
        }
    }
}

/// Failure starting finite work through the persistent client.
#[derive(Debug)]
pub enum ResilientClientError {
    /// Physical recovery is in progress; the caller may retry safely.
    Recovering,
    /// Bounded physical recovery stopped.
    RecoveryFailed(PersistentRecoveryFailure),
    /// Persistent client was explicitly closed.
    Closed,
    /// Current physical client rejected or could not complete the operation.
    Client(ClientError),
}

impl Display for ResilientClientError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Recovering => formatter.write_str("persistent SDK client is recovering"),
            Self::RecoveryFailed(reason) => {
                write!(
                    formatter,
                    "persistent SDK recovery stopped: {}",
                    reason.as_str()
                )
            }
            Self::Closed => formatter.write_str("persistent SDK client is closed"),
            Self::Client(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for ResilientClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Client(error) => Some(error),
            Self::Recovering | Self::RecoveryFailed(_) | Self::Closed => None,
        }
    }
}

impl From<ClientError> for ResilientClientError {
    fn from(value: ClientError) -> Self {
        Self::Client(value)
    }
}

/// Terminal or caller-visible state while reading a persistent subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResilientSubscriptionError {
    /// Caller wait deadline elapsed across connection generations.
    Timeout,
    /// Only this subscription exhausted its bounded mailbox.
    Overloaded,
    /// This logical subscription was explicitly closed.
    Unsubscribed,
    /// Persistent client was explicitly closed.
    ClientClosed,
    /// Bounded physical recovery stopped.
    RecoveryFailed(PersistentRecoveryFailure),
    /// This stream alone was rejected while its peers recovered.
    ResumeRejected(ClientErrorCategory),
    /// A higher-level validator rejected the encoded resume activation response.
    ResumeActivationRejected(Arc<[u8]>),
}

impl Display for ResilientSubscriptionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout => formatter.write_str("persistent subscription wait timed out"),
            Self::Overloaded => formatter.write_str("persistent subscription mailbox is overloaded; resume from the application checkpoint"),
            Self::Unsubscribed => formatter.write_str("persistent subscription was closed"),
            Self::ClientClosed => formatter.write_str("persistent SDK client was closed"),
            Self::RecoveryFailed(reason) => {
                write!(
                    formatter,
                    "persistent subscription recovery stopped: {}",
                    reason.as_str()
                )
            }
            Self::ResumeActivationRejected(_) => formatter.write_str("stream resume activation was rejected"),
            Self::ResumeRejected(category) => {
                write!(
                    formatter,
                    "persistent subscription resume failed: {}",
                    category.as_str()
                )
            }
        }
    }
}

impl Error for ResilientSubscriptionError {}

/// Successful persistent stream open response and stable logical handle.
pub struct OpenedResilientSubscription {
    /// Encoded finite response to the initial open operation.
    pub response: Arc<[u8]>,
    /// Logical handle whose identity survives physical reconnection.
    pub subscription: ResilientSubscription,
}

/// Cheaply clonable SDK client with one automatic recovery supervisor.
#[derive(Clone)]
pub struct ResilientClient {
    inner: Arc<ResilientClientInner>,
}

impl ResilientClient {
    /// Connects with bounded backoff and starts automatic stream recovery.
    ///
    /// One supervisor waits on connection-close notification without polling.
    /// It owns replacement connection establishment and resumes all live
    /// logical streams on that one connection. This function performs connector,
    /// transport, thread, and blocking response-wait IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns initial recovery or supervisor thread creation failure.
    pub fn connect(
        connector: Arc<dyn ClientConnector>,
        client_limits: ClientRuntimeLimits,
        recovery_policy: ClientRecoveryPolicy,
        jitter: Arc<dyn ClientRecoveryJitter>,
        resume_timeout: Duration,
    ) -> Result<Self, ResilientClientConnectError> {
        let telemetry = Arc::new(BoundedClientTelemetry::default());
        Self::connect_inner(
            connector,
            client_limits,
            (recovery_policy, recovery_policy),
            jitter,
            resume_timeout,
            telemetry.clone(),
            Some(telemetry),
        )
    }

    /// Uses separate bounded policies for initial connection and later recovery.
    ///
    /// This lets a lifecycle readiness probe return a client that still retries
    /// transient failures after the initial connection succeeds.
    ///
    /// # Errors
    /// Returns initial recovery or supervisor thread creation failure.
    pub fn connect_with_initial_policy(
        connector: Arc<dyn ClientConnector>,
        client_limits: ClientRuntimeLimits,
        initial_policy: ClientRecoveryPolicy,
        recovery_policy: ClientRecoveryPolicy,
        jitter: Arc<dyn ClientRecoveryJitter>,
        resume_timeout: Duration,
    ) -> Result<Self, ResilientClientConnectError> {
        let telemetry = Arc::new(BoundedClientTelemetry::default());
        Self::connect_inner(
            connector,
            client_limits,
            (initial_policy, recovery_policy),
            jitter,
            resume_timeout,
            telemetry.clone(),
            Some(telemetry),
        )
    }

    /// Connects with caller-owned SDK telemetry for external aggregation.
    ///
    /// The telemetry implementation must be non-blocking and payload-safe.
    /// Connection behavior and failure semantics match [`Self::connect`].
    ///
    /// # Errors
    ///
    /// Returns initial recovery or supervisor thread creation failure.
    pub fn connect_with_telemetry(
        connector: Arc<dyn ClientConnector>,
        client_limits: ClientRuntimeLimits,
        recovery_policy: ClientRecoveryPolicy,
        jitter: Arc<dyn ClientRecoveryJitter>,
        resume_timeout: Duration,
        telemetry: Arc<dyn ClientTelemetry>,
    ) -> Result<Self, ResilientClientConnectError> {
        Self::connect_inner(
            connector,
            client_limits,
            (recovery_policy, recovery_policy),
            jitter,
            resume_timeout,
            telemetry,
            None,
        )
    }

    fn connect_inner(
        connector: Arc<dyn ClientConnector>,
        client_limits: ClientRuntimeLimits,
        policies: (ClientRecoveryPolicy, ClientRecoveryPolicy),
        jitter: Arc<dyn ClientRecoveryJitter>,
        resume_timeout: Duration,
        telemetry: Arc<dyn ClientTelemetry>,
        default_telemetry: Option<Arc<BoundedClientTelemetry>>,
    ) -> Result<Self, ResilientClientConnectError> {
        let (initial_policy, recovery_policy) = policies;
        let cancellation = ClientRecoveryCancellation::default();
        let recovered = recover_client_connection_with_telemetry(
            connector.as_ref(),
            client_limits,
            initial_policy,
            jitter.as_ref(),
            &cancellation,
            Vec::new(),
            resume_timeout,
            telemetry.clone(),
        )
        .map_err(ResilientClientConnectError::Recovery)?;
        let inner = Arc::new(ResilientClientInner {
            connector,
            client_limits,
            recovery_policy,
            jitter,
            resume_timeout,
            cancellation,
            telemetry,
            default_telemetry,
            next_stream: AtomicU64::new(0),
            physical_connection_attempts: AtomicU64::new(u64::from(recovered.connection_attempts)),
            state: Mutex::new(ResilientState {
                client: Some(recovered.client),
                phase: ResilientClientPhase::Connected,
                generation: 1,
                subscriptions: BTreeMap::new(),
                last_recovery_error: None,
            }),
            state_changed: Condvar::new(),
            supervisor: Mutex::new(None),
        });
        let worker_inner = Arc::downgrade(&inner);
        let supervisor = thread::Builder::new()
            .name("undercurrent-sdk-recovery".into())
            .spawn(move || recovery_supervisor(worker_inner))
            .map_err(|error| {
                inner.shutdown();
                ResilientClientConnectError::SupervisorThread(error)
            })?;
        *inner
            .supervisor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(supervisor);
        Ok(Self { inner })
    }

    /// Executes one finite request on the current physical connection.
    ///
    /// Ambiguous in-flight failures are not retried automatically. Callers may
    /// retry only according to their product idempotency contract.
    ///
    /// # Errors
    ///
    /// Returns recovery state or current physical client failure.
    pub fn request(
        &self,
        payload: Arc<[u8]>,
        timeout: Duration,
    ) -> Result<Arc<[u8]>, ResilientClientError> {
        self.start_request(payload)?.wait(timeout)
    }

    /// Executes one finite request under caller-supplied trace context.
    ///
    /// # Errors
    ///
    /// Returns recovery state or current physical client failure.
    pub fn request_with_trace_context(
        &self,
        payload: Arc<[u8]>,
        trace_context: crate::ClientTraceContext,
        timeout: Duration,
    ) -> Result<Arc<[u8]>, ResilientClientError> {
        self.start_request_with_trace_context(payload, trace_context)?
            .wait(timeout)
    }

    pub(crate) fn start_request(
        &self,
        payload: Arc<[u8]>,
    ) -> Result<ResilientPendingRequest, ResilientClientError> {
        let (client, _) = self.inner.current_client()?;
        Ok(ResilientPendingRequest {
            inner: client.start_request(payload)?,
        })
    }

    pub(crate) fn correlated_context(
        &self,
        id: undercurrent_wire::CorrelationId,
    ) -> Result<crate::ClientTraceContext, ResilientClientError> {
        let (client, _) = self.inner.current_client()?;
        Ok(crate::ClientTraceContext::from_raw(
            client.next_trace_context()?.with_correlation_id(id),
        ))
    }

    pub(crate) fn start_request_with_trace_context(
        &self,
        payload: Arc<[u8]>,
        trace_context: crate::ClientTraceContext,
    ) -> Result<ResilientPendingRequest, ResilientClientError> {
        let (client, _) = self.inner.current_client()?;
        Ok(ResilientPendingRequest {
            inner: client.start_request_with_trace_context(payload, trace_context)?,
        })
    }

    /// Opens one persistent logical subscription on the current connection.
    ///
    /// The returned stream ID is allocated by this persistent client rather
    /// than a physical generation, preventing ID reuse after reconnect.
    ///
    /// # Errors
    ///
    /// Returns identifier exhaustion, recovery state, or current client failure.
    pub fn open_subscription(
        &self,
        payload: Arc<[u8]>,
        timeout: Duration,
    ) -> Result<OpenedResilientSubscription, ResilientClientError> {
        let (client, _) = self.inner.current_client()?;
        let trace_context = client.next_trace_context()?;
        self.open_subscription_with_trace_context(
            payload,
            crate::ClientTraceContext::from_raw(trace_context),
            timeout,
        )
    }

    /// Opens one persistent subscription under caller-supplied trace context.
    ///
    /// The same context is retained for every physical resume of this logical
    /// stream. This method performs bounded connection and activation IO.
    ///
    /// # Errors
    ///
    /// Returns identifier exhaustion, recovery state, or current client failure.
    pub fn open_subscription_with_trace_context(
        &self,
        payload: Arc<[u8]>,
        trace_context: crate::ClientTraceContext,
        timeout: Duration,
    ) -> Result<OpenedResilientSubscription, ResilientClientError> {
        self.open_subscription_validated(payload, trace_context, timeout, None)
    }

    pub(crate) fn next_trace_context(
        &self,
    ) -> Result<crate::ClientTraceContext, ResilientClientError> {
        let (client, _) = self.inner.current_client()?;
        Ok(crate::ClientTraceContext::from_raw(
            client.next_trace_context()?,
        ))
    }

    pub(crate) fn open_subscription_validated(
        &self,
        payload: Arc<[u8]>,
        trace_context: crate::ClientTraceContext,
        timeout: Duration,
        validator: Option<ResumeValidator>,
    ) -> Result<OpenedResilientSubscription, ResilientClientError> {
        let stream_id = self.inner.next_stream_id()?;
        let (client, generation) = self.inner.current_client()?;
        let opened = client.open_subscription_with_id(
            stream_id.clone(),
            payload.clone(),
            trace_context.into_raw(),
            timeout,
        )?;
        let response = opened.response;
        let subscription = Arc::new(opened.subscription);
        let slot = Arc::new(PersistentSubscription::new(
            stream_id.clone(),
            payload,
            subscription,
            generation,
            validator,
        ));
        let mut state = self.inner.lock_state();
        if state.phase != ResilientClientPhase::Connected || state.generation != generation {
            drop(state);
            slot.close_local(ResilientSubscriptionError::Unsubscribed);
            return Err(ResilientClientError::Recovering);
        }
        state.subscriptions.insert(stream_id, Arc::downgrade(&slot));
        drop(state);
        Ok(OpenedResilientSubscription {
            response,
            subscription: ResilientSubscription {
                owner: self.inner.clone(),
                slot,
            },
        })
    }

    /// Sends one heartbeat on the current physical connection.
    ///
    /// # Errors
    ///
    /// Returns recovery state or writer failure.
    pub fn ping(&self) -> Result<(), ResilientClientError> {
        let (client, _) = self.inner.current_client()?;
        client.ping().map_err(Into::into)
    }

    /// Waits until a connection is installed or recovery becomes terminal.
    ///
    /// Returns `true` only if the observed phase is `Connected`; timeout,
    /// terminal failure, and explicit closure return `false`. Inspect
    /// [`Self::pressure`] to distinguish those cases. This is a snapshot that
    /// can race with later disconnection and performs no transport IO.
    pub fn wait_connected(&self, timeout: Duration) -> bool {
        let guard = self.inner.lock_state();
        if guard.phase != ResilientClientPhase::Recovering {
            return guard.phase == ResilientClientPhase::Connected;
        }
        let (guard, _) = self
            .inner
            .state_changed
            .wait_timeout_while(guard, timeout, |state| {
                state.phase == ResilientClientPhase::Recovering
            })
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.phase == ResilientClientPhase::Connected
    }

    /// Returns current bounded recovery state without transport IO.
    pub fn pressure(&self) -> ResilientClientPressure {
        let state = self.inner.lock_state();
        ResilientClientPressure {
            phase: state.phase,
            generation: state.generation,
            physical_connection_attempts: self
                .inner
                .physical_connection_attempts
                .load(Ordering::Acquire),
            subscriptions: state.subscriptions.len(),
        }
    }

    /// Returns the current negotiated agreement when connected.
    pub fn agreement(&self) -> Option<crate::ClientConnectionAgreement> {
        self.inner.lock_state().client.as_ref().map(|client| {
            crate::ClientConnectionAgreement::from_raw(client.agreement_raw().clone())
        })
    }

    /// Returns the last payload-redacted terminal recovery error with its source.
    pub fn last_recovery_error(&self) -> Option<Arc<ClientRecoveryError>> {
        self.inner.lock_state().last_recovery_error.clone()
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

    /// Stops recovery, closes the physical connection, and wakes all streams.
    pub fn close(&self) {
        self.inner.shutdown();
    }
}

/// One logical subscription whose handle survives physical reconnection.
pub struct ResilientSubscription {
    owner: Arc<ResilientClientInner>,
    slot: Arc<PersistentSubscription>,
}

impl ResilientSubscription {
    /// Returns the stable client-scoped logical stream identity.
    pub fn stream_id(&self) -> crate::ClientStreamId {
        crate::ClientStreamId::from_raw(self.slot.stream_id.clone())
    }

    pub(crate) fn stream_id_raw(&self) -> &StreamId {
        &self.slot.stream_id
    }

    /// Blocks across recovery until one ordered item or terminal state arrives.
    ///
    /// # Errors
    ///
    /// Returns explicit close, recovery, isolated resume, or client state.
    pub fn recv(&self) -> Result<crate::ClientStreamItem, ResilientSubscriptionError> {
        self.recv_raw().map(crate::ClientStreamItem::from_raw)
    }

    /// Waits across physical generations up to one caller deadline.
    ///
    /// # Errors
    ///
    /// Returns timeout, explicit close, recovery, or isolated resume failure.
    pub fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<crate::ClientStreamItem, ResilientSubscriptionError> {
        self.recv_timeout_raw(timeout)
            .map(crate::ClientStreamItem::from_raw)
    }

    /// Returns the last caller-consumed sequence across generations.
    pub fn last_sequence(&self) -> crate::ClientStreamSequence {
        crate::ClientStreamSequence::from_raw(self.last_sequence_raw())
    }

    /// Independently closes this logical stream and removes its recovery state.
    pub fn close(self) {
        self.close_inner();
    }

    /// Independently cancels this logical stream through a shared reference.
    ///
    /// This is used by typed protocol adapters when a stream sends malformed or
    /// kind-inconsistent data. Repeated cancellation is idempotent. If bounded
    /// close-command queueing fails, the physical client closes and peer work
    /// follows its ordinary failure/recovery path.
    pub fn cancel(&self) {
        self.close_inner();
    }

    pub(crate) fn recv_raw(&self) -> Result<InboundStreamItem, ResilientSubscriptionError> {
        self.recv_until(None)
    }

    pub(crate) fn recv_timeout_raw(
        &self,
        timeout: Duration,
    ) -> Result<InboundStreamItem, ResilientSubscriptionError> {
        self.recv_until(Instant::now().checked_add(timeout))
    }

    pub(crate) fn last_sequence_raw(&self) -> StreamSequence {
        self.slot.last_sequence()
    }

    fn recv_until(
        &self,
        deadline: Option<Instant>,
    ) -> Result<InboundStreamItem, ResilientSubscriptionError> {
        loop {
            let (subscription, generation) = self.slot.wait_current(deadline)?;
            let result = match deadline {
                Some(deadline) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(ResilientSubscriptionError::Timeout);
                    }
                    subscription.recv_timeout_raw(remaining)
                }
                None => subscription.recv_raw(),
            };
            match result {
                Ok(item) => return Ok(item),
                Err(InboundStreamError::Timeout) => {
                    return Err(ResilientSubscriptionError::Timeout);
                }
                Err(InboundStreamError::ConnectionClosed | InboundStreamError::Cancelled) => {
                    self.slot.detach(&subscription, generation);
                }
                Err(InboundStreamError::Overloaded) => {
                    self.slot.fail(ResilientSubscriptionError::Overloaded);
                    self.owner.unregister(&self.slot);
                    return Err(ResilientSubscriptionError::Overloaded);
                }
                Err(InboundStreamError::Unsubscribed) => {
                    return Err(ResilientSubscriptionError::Unsubscribed);
                }
            }
        }
    }

    fn close_inner(&self) {
        self.slot
            .close_local(ResilientSubscriptionError::Unsubscribed);
        self.owner.unregister(&self.slot);
    }
}

impl Drop for ResilientSubscription {
    fn drop(&mut self) {
        self.close_inner();
    }
}

struct ResilientClientInner {
    connector: Arc<dyn ClientConnector>,
    client_limits: ClientRuntimeLimits,
    recovery_policy: ClientRecoveryPolicy,
    jitter: Arc<dyn ClientRecoveryJitter>,
    resume_timeout: Duration,
    cancellation: ClientRecoveryCancellation,
    telemetry: Arc<dyn ClientTelemetry>,
    default_telemetry: Option<Arc<BoundedClientTelemetry>>,
    next_stream: AtomicU64,
    physical_connection_attempts: AtomicU64,
    state: Mutex<ResilientState>,
    state_changed: Condvar,
    supervisor: Mutex<Option<JoinHandle<()>>>,
}

impl ResilientClientInner {
    /// Clones the installed client and generation or returns current phase.
    fn current_client(&self) -> Result<(Client, u64), ResilientClientError> {
        let state = self.lock_state();
        match state.phase {
            ResilientClientPhase::Connected => state
                .client
                .clone()
                .map(|client| (client, state.generation))
                .ok_or(ResilientClientError::Recovering),
            ResilientClientPhase::Recovering => Err(ResilientClientError::Recovering),
            ResilientClientPhase::Failed(reason) => {
                Err(ResilientClientError::RecoveryFailed(reason))
            }
            ResilientClientPhase::Closed => Err(ResilientClientError::Closed),
        }
    }

    /// Allocates one client-lifetime stream identity without wrapping.
    fn next_stream_id(&self) -> Result<StreamId, ResilientClientError> {
        let value = self
            .next_stream
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map(|previous| previous + 1)
            .map_err(|_| ResilientClientError::Closed)?;
        StreamId::new(format!("rs-{value}")).map_err(|_| {
            ResilientClientError::RecoveryFailed(PersistentRecoveryFailure::ClientRuntime)
        })
    }

    /// Clones the connection the supervisor should currently watch.
    fn connection_snapshot(&self) -> Option<(Client, u64)> {
        let state = self.lock_state();
        (state.phase == ResilientClientPhase::Connected)
            .then(|| {
                state
                    .client
                    .clone()
                    .map(|client| (client, state.generation))
            })
            .flatten()
    }

    /// Moves live slots into recovery and captures caller-consumed checkpoints.
    fn prepare_recovery(&self, generation: u64) -> Vec<SubscriptionCheckpoint> {
        let slots = {
            let mut state = self.lock_state();
            if state.phase == ResilientClientPhase::Closed || state.generation != generation {
                return Vec::new();
            }
            state.phase = ResilientClientPhase::Recovering;
            state.client = None;
            state
                .subscriptions
                .retain(|_, slot| slot.strong_count() > 0);
            state
                .subscriptions
                .values()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>()
        };
        self.state_changed.notify_all();
        let checkpoints = slots
            .into_iter()
            .filter_map(|slot| slot.checkpoint())
            .collect();
        self.telemetry
            .emit_diagnostic(ClientDiagnosticName::RecoveryStarted);
        checkpoints
    }

    /// Installs one replacement client and each isolated stream result.
    fn install_recovered(&self, recovered: crate::RecoveredClient) {
        self.physical_connection_attempts
            .fetch_add(u64::from(recovered.connection_attempts), Ordering::AcqRel);
        self.telemetry.record_metric(
            ClientMetricName::ReconnectAttempts,
            f64::from(recovered.connection_attempts),
        );
        let slots = {
            let state = self.lock_state();
            state
                .subscriptions
                .iter()
                .filter_map(|(stream_id, slot)| {
                    slot.upgrade().map(|slot| (stream_id.clone(), slot))
                })
                .collect::<BTreeMap<_, _>>()
        };
        let resume_attempts = recovered.subscriptions.len();
        let mut resume_succeeded = 0_usize;
        let mut resume_failed = 0_usize;
        for recovered_subscription in recovered.subscriptions {
            let stream_id = recovered_subscription.checkpoint.stream_id_raw().clone();
            match recovered_subscription.result {
                Ok(opened)
                    if slots.get(&stream_id).is_some_and(|slot| {
                        slot.validator
                            .as_ref()
                            .is_some_and(|validate| !validate(&opened.response))
                    }) =>
                {
                    resume_failed = resume_failed.saturating_add(1);
                    self.telemetry
                        .emit_diagnostic(ClientDiagnosticName::SubscriptionResumeFailed);
                    if let Some(slot) = slots.get(&stream_id) {
                        slot.fail(ResilientSubscriptionError::ResumeActivationRejected(
                            opened.response,
                        ));
                    }
                    opened.subscription.close_shared();
                }
                Ok(opened) => {
                    resume_succeeded = resume_succeeded.saturating_add(1);
                    if let Some(slot) = slots.get(&stream_id) {
                        slot.install(Arc::new(opened.subscription));
                    } else {
                        opened.subscription.close_shared();
                    }
                }
                Err(error) => {
                    resume_failed = resume_failed.saturating_add(1);
                    if error.category() == ClientErrorCategory::Overloaded {
                        self.telemetry
                            .record_metric(ClientMetricName::OverloadRejections, 1.0);
                    }
                    self.telemetry
                        .emit_diagnostic(ClientDiagnosticName::SubscriptionResumeFailed);
                    if let Some(slot) = slots.get(&stream_id) {
                        slot.fail(ResilientSubscriptionError::ResumeRejected(error.category()));
                    }
                }
            }
        }
        self.telemetry
            .record_metric(ClientMetricName::ResumeAttempts, resume_attempts as f64);
        self.telemetry
            .record_metric(ClientMetricName::ResumeSucceeded, resume_succeeded as f64);
        self.telemetry
            .record_metric(ClientMetricName::ResumeFailed, resume_failed as f64);

        let mut state = self.lock_state();
        if state.phase == ResilientClientPhase::Closed {
            drop(state);
            recovered.client.close();
            return;
        }
        state.generation = state.generation.saturating_add(1);
        state.client = Some(recovered.client);
        state.phase = ResilientClientPhase::Connected;
        state.last_recovery_error = None;
        drop(state);
        self.telemetry
            .emit_diagnostic(ClientDiagnosticName::RecoverySucceeded);
        self.state_changed.notify_all();
    }

    /// Makes one terminal recovery failure visible to clients and streams.
    fn fail_recovery(&self, error: ClientRecoveryError) {
        let Some(reason) = PersistentRecoveryFailure::from_error(&error) else {
            return;
        };
        self.physical_connection_attempts
            .fetch_add(u64::from(recovery_attempts(&error)), Ordering::AcqRel);
        self.telemetry.record_metric(
            ClientMetricName::ReconnectAttempts,
            f64::from(recovery_attempts(&error)),
        );
        let slots = {
            let mut state = self.lock_state();
            if state.phase == ResilientClientPhase::Closed {
                return;
            }
            state.phase = ResilientClientPhase::Failed(reason);
            state.client = None;
            state.last_recovery_error = Some(Arc::new(error));
            state
                .subscriptions
                .values()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>()
        };
        // Complete terminal observability before waking callers that may inspect it.
        self.telemetry
            .emit_diagnostic(ClientDiagnosticName::RecoveryFailed);
        for slot in slots {
            slot.fail(ResilientSubscriptionError::RecoveryFailed(reason));
        }
        self.state_changed.notify_all();
    }

    /// Removes exactly the dropped logical handle from future recovery passes.
    fn unregister(&self, slot: &Arc<PersistentSubscription>) {
        let mut state = self.lock_state();
        if state
            .subscriptions
            .get(&slot.stream_id)
            .and_then(Weak::upgrade)
            .is_some_and(|registered| Arc::ptr_eq(&registered, slot))
        {
            state.subscriptions.remove(&slot.stream_id);
        }
    }

    /// Cancels recovery and closes all current physical and logical resources.
    fn shutdown(&self) {
        if self.cancellation.is_cancelled() {
            return;
        }
        self.cancellation.cancel();
        let (client, slots) = {
            let mut state = self.lock_state();
            state.phase = ResilientClientPhase::Closed;
            let client = state.client.take();
            let slots = state
                .subscriptions
                .values()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>();
            state.subscriptions.clear();
            (client, slots)
        };
        if let Some(client) = client {
            client.close();
        }
        for slot in slots {
            slot.close_local(ResilientSubscriptionError::ClientClosed);
        }
        self.state_changed.notify_all();
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, ResilientState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Drop for ResilientClientInner {
    fn drop(&mut self) {
        self.shutdown();
        let worker = self
            .supervisor
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(worker) = worker
            && worker.thread().id() != thread::current().id()
        {
            let _ = worker.join();
        }
    }
}

struct ResilientState {
    client: Option<Client>,
    phase: ResilientClientPhase,
    generation: u64,
    subscriptions: BTreeMap<StreamId, Weak<PersistentSubscription>>,
    last_recovery_error: Option<Arc<ClientRecoveryError>>,
}

type ResumeValidator = Arc<dyn Fn(&[u8]) -> bool + Send + Sync>;

struct PersistentSubscription {
    validator: Option<ResumeValidator>,
    stream_id: StreamId,
    payload: Arc<[u8]>,
    trace_context: undercurrent_wire::TraceContext,
    state: Mutex<PersistentSubscriptionState>,
    changed: Condvar,
}

impl PersistentSubscription {
    /// Captures immutable resume specification and initial physical stream.
    fn new(
        stream_id: StreamId,
        payload: Arc<[u8]>,
        subscription: Arc<Subscription>,
        generation: u64,
        validator: Option<ResumeValidator>,
    ) -> Self {
        Self {
            validator,
            stream_id,
            payload,
            trace_context: subscription.trace_context_raw().clone(),
            state: Mutex::new(PersistentSubscriptionState {
                server_epoch: subscription.client().agreement_raw().server_epoch().clone(),
                after: subscription.last_sequence_raw(),
                current: Some(subscription),
                generation,
                terminal: None,
                closed: false,
            }),
            changed: Condvar::new(),
        }
    }

    /// Detaches the old physical stream at its last consumed sequence.
    fn checkpoint(&self) -> Option<SubscriptionCheckpoint> {
        let mut state = self.lock_state();
        if state.closed || state.terminal.is_some() {
            return None;
        }
        if let Some(subscription) = state.current.take() {
            state.server_epoch = subscription.client().agreement_raw().server_epoch().clone();
            state.after = subscription.last_sequence_raw();
        }
        self.changed.notify_all();
        Some(SubscriptionCheckpoint::new_raw(
            self.stream_id.clone(),
            state.server_epoch.clone(),
            state.after,
            self.trace_context.clone(),
            self.payload.clone(),
        ))
    }

    /// Swaps a successfully resumed physical stream behind the stable handle.
    fn install(&self, subscription: Arc<Subscription>) {
        let mut state = self.lock_state();
        if state.closed || state.terminal.is_some() {
            drop(state);
            subscription.close_shared();
            return;
        }
        state.server_epoch = subscription.client().agreement_raw().server_epoch().clone();
        state.after = subscription.last_sequence_raw();
        state.generation = state.generation.saturating_add(1);
        state.current = Some(subscription);
        drop(state);
        self.changed.notify_all();
    }

    /// Detaches a failed physical generation without disturbing a newer one.
    fn detach(&self, subscription: &Arc<Subscription>, generation: u64) {
        let mut state = self.lock_state();
        // Receive runs outside the slot lock. Its old failure can arrive after
        // install has replaced the stream, so only the captured stream may be
        // detached or supply the checkpoint.
        if state.generation != generation
            || !state
                .current
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, subscription))
        {
            return;
        }
        state.server_epoch = subscription.client().agreement_raw().server_epoch().clone();
        state.after = subscription.last_sequence_raw();
        state.current = None;
        drop(state);
        self.changed.notify_all();
    }

    /// Records one terminal recovery failure and closes current stream state.
    fn fail(&self, error: ResilientSubscriptionError) {
        let current = {
            let mut state = self.lock_state();
            if state.closed || state.terminal.is_some() {
                return;
            }
            state.terminal = Some(error);
            state.current.take()
        };
        if let Some(current) = current {
            current.close_shared();
        }
        self.changed.notify_all();
    }

    /// Applies explicit local or owning-client closure idempotently.
    fn close_local(&self, error: ResilientSubscriptionError) {
        let current = {
            let mut state = self.lock_state();
            if state.closed {
                return;
            }
            state.closed = true;
            state.terminal = Some(error);
            state.current.take()
        };
        if let Some(current) = current {
            current.close_shared();
        }
        self.changed.notify_all();
    }

    /// Waits for a current physical stream without extending caller deadline.
    fn wait_current(
        &self,
        deadline: Option<Instant>,
    ) -> Result<(Arc<Subscription>, u64), ResilientSubscriptionError> {
        let mut state = self.lock_state();
        loop {
            if state.closed {
                return Err(state
                    .terminal
                    .clone()
                    .unwrap_or(ResilientSubscriptionError::Unsubscribed));
            }
            if let Some(error) = state.terminal.clone() {
                return Err(error);
            }
            if let Some(current) = &state.current {
                return Ok((current.clone(), state.generation));
            }
            match deadline {
                Some(deadline) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(ResilientSubscriptionError::Timeout);
                    }
                    let (next, timeout) = self
                        .changed
                        .wait_timeout(state, remaining)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    state = next;
                    if timeout.timed_out() && state.current.is_none() {
                        return Err(ResilientSubscriptionError::Timeout);
                    }
                }
                None => {
                    state = self
                        .changed
                        .wait(state)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
            }
        }
    }

    /// Reads the newest caller-consumed checkpoint across physical generations.
    fn last_sequence(&self) -> StreamSequence {
        let state = self.lock_state();
        state
            .current
            .as_ref()
            .map_or(state.after, |subscription| subscription.last_sequence_raw())
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, PersistentSubscriptionState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

struct PersistentSubscriptionState {
    server_epoch: ServerEpoch,
    after: StreamSequence,
    current: Option<Arc<Subscription>>,
    generation: u64,
    terminal: Option<ResilientSubscriptionError>,
    closed: bool,
}

/// Waits without polling, recovers one connection, and repeats until terminal.
fn recovery_supervisor(inner: Weak<ResilientClientInner>) {
    loop {
        let Some(owner) = inner.upgrade() else {
            return;
        };
        let Some((client, generation)) = owner.connection_snapshot() else {
            return;
        };
        // Release the persistent owner across blocking waits, including the
        // recovery call below. Otherwise this worker could keep the last owner
        // alive and prevent its Drop from closing/cancelling the work we await.
        // See [ownership](../docs/internals/client-runtime.md#runtime-ownership-and-concurrency).
        drop(owner);
        client.wait_closed();
        drop(client);

        let Some(owner) = inner.upgrade() else {
            return;
        };
        if owner.cancellation.is_cancelled() {
            return;
        }
        let checkpoints = owner.prepare_recovery(generation);
        let connector = owner.connector.clone();
        let client_limits = owner.client_limits;
        let recovery_policy = owner.recovery_policy;
        let jitter = owner.jitter.clone();
        let cancellation = owner.cancellation.clone();
        let resume_timeout = owner.resume_timeout;
        let telemetry = owner.telemetry.clone();
        drop(owner);

        let recovered = recover_client_connection_with_telemetry(
            connector.as_ref(),
            client_limits,
            recovery_policy,
            jitter.as_ref(),
            &cancellation,
            checkpoints,
            resume_timeout,
            telemetry,
        );
        let Some(owner) = inner.upgrade() else {
            if let Ok(recovered) = recovered {
                recovered.client.close();
            }
            return;
        };
        match recovered {
            Ok(recovered) => owner.install_recovered(recovered),
            Err(ClientRecoveryError::Cancelled { .. }) => return,
            Err(error) => owner.fail_recovery(error),
        }
    }
}

/// Returns physical attempts embedded in a terminal recovery result.
fn recovery_attempts(error: &ClientRecoveryError) -> u32 {
    match error {
        ClientRecoveryError::Cancelled { attempts }
        | ClientRecoveryError::ConnectorRejected { attempts, .. }
        | ClientRecoveryError::AttemptsExhausted { attempts, .. } => *attempts,
        ClientRecoveryError::Limits(_)
        | ClientRecoveryError::DuplicateStreamId { .. }
        | ClientRecoveryError::TooManyStreams { .. }
        | ClientRecoveryError::MissingResumeCapability
        | ClientRecoveryError::Client(_)
        | ClientRecoveryError::ResumeWorker(_)
        | ClientRecoveryError::ResumeWorkerPanicked
        | ClientRecoveryError::IncompleteResumeBatch { .. } => 0,
    }
}

#[cfg(test)]
mod close_tests;
