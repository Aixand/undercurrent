// SPDX-License-Identifier: Apache-2.0

//! Bounded connection recovery and logical-stream resume orchestration.
//!
//! This layer retries connector failures with interruptible backoff, then resumes
//! an explicit checkpoint set through bounded workers. It returns results in
//! checkpoint order and does not replay finite requests. It does not decode
//! product activation responses or install a persistent recovery supervisor.

use std::collections::{BTreeSet, VecDeque};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use undercurrent_wire::{ServerEpoch, StreamId, StreamSequence, TraceContext};

use crate::{
    BoundedClientTelemetry, Client, ClientConnectError, ClientConnectionAgreement, ClientError,
    ClientRetryGuidance, ClientRuntimeLimits, ClientServerEpoch, ClientStreamId,
    ClientStreamSequence, ClientTelemetry, ClientTraceContext, ClientTransport,
    ClientTransportError, OpenedSubscription, Subscription,
};

const BASIS_POINTS: u128 = 10_000;
const NANOS_PER_SECOND: u128 = 1_000_000_000;
const RESUME_CANCELLATION_POLL: Duration = Duration::from_millis(25);

/// Invalid SDK connection-recovery configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientRecoveryLimitError {
    /// A required bound was zero.
    ZeroLimit {
        /// Stable invalid field name.
        field: &'static str,
    },
    /// Exponential multiplier was less than 1.0x.
    InvalidBackoffMultiplier,
    /// Maximum delay was lower than initial delay.
    InvalidDelayRange,
    /// Additive jitter exceeded the supported 100 percent bound.
    InvalidJitter,
}

impl Display for ClientRecoveryLimitError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroLimit { field } => {
                write!(formatter, "client recovery limit {field} must be non-zero")
            }
            Self::InvalidBackoffMultiplier => {
                formatter.write_str("client recovery backoff must be at least 10000 basis points")
            }
            Self::InvalidDelayRange => formatter
                .write_str("client recovery maximum delay cannot be below the initial delay"),
            Self::InvalidJitter => {
                formatter.write_str("client recovery jitter must be within 1..=10000 basis points")
            }
        }
    }
}

impl Error for ClientRecoveryLimitError {}

/// Explicit bounded exponential-backoff policy for physical reconnection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientRecoveryPolicy {
    initial_delay: Duration,
    maximum_delay: Duration,
    backoff_basis_points: u32,
    jitter_basis_points: u16,
    maximum_attempts: u32,
    maximum_parallel_resumes: usize,
}

impl ClientRecoveryPolicy {
    /// Constructs a bounded policy without selecting product-specific values.
    ///
    /// `backoff_basis_points` uses 10,000 for 1.0x. Jitter is additive and
    /// capped by `maximum_delay`. This function performs no IO and does not
    /// panic.
    ///
    /// # Errors
    ///
    /// Returns a typed error for zero bounds, delay inversion, a multiplier
    /// below 1.0x, or jitter outside greater-than-zero through 100 percent.
    pub fn new(
        initial_delay: Duration,
        maximum_delay: Duration,
        backoff_basis_points: u32,
        jitter_basis_points: u16,
        maximum_attempts: u32,
        maximum_parallel_resumes: usize,
    ) -> Result<Self, ClientRecoveryLimitError> {
        if initial_delay.is_zero() {
            return Err(ClientRecoveryLimitError::ZeroLimit {
                field: "initial_delay",
            });
        }
        if maximum_delay.is_zero() {
            return Err(ClientRecoveryLimitError::ZeroLimit {
                field: "maximum_delay",
            });
        }
        if maximum_attempts == 0 {
            return Err(ClientRecoveryLimitError::ZeroLimit {
                field: "maximum_attempts",
            });
        }
        if maximum_parallel_resumes == 0 {
            return Err(ClientRecoveryLimitError::ZeroLimit {
                field: "maximum_parallel_resumes",
            });
        }
        if backoff_basis_points < BASIS_POINTS as u32 {
            return Err(ClientRecoveryLimitError::InvalidBackoffMultiplier);
        }
        if maximum_delay < initial_delay {
            return Err(ClientRecoveryLimitError::InvalidDelayRange);
        }
        if jitter_basis_points == 0 || u128::from(jitter_basis_points) > BASIS_POINTS {
            return Err(ClientRecoveryLimitError::InvalidJitter);
        }
        Ok(Self {
            initial_delay,
            maximum_delay,
            backoff_basis_points,
            jitter_basis_points,
            maximum_attempts,
            maximum_parallel_resumes,
        })
    }

    /// Returns the inclusive physical connection-attempt limit.
    pub const fn maximum_attempts(self) -> u32 {
        self.maximum_attempts
    }

    /// Returns the delay applied after the first failed attempt before jitter.
    pub const fn initial_delay(self) -> Duration {
        self.initial_delay
    }

    /// Returns the inclusive reconnect-delay ceiling.
    pub const fn maximum_delay(self) -> Duration {
        self.maximum_delay
    }

    /// Returns the exponential multiplier where 10,000 is 1.0x.
    pub const fn backoff_basis_points(self) -> u32 {
        self.backoff_basis_points
    }

    /// Returns the maximum additive jitter in basis points.
    pub const fn jitter_basis_points(self) -> u16 {
        self.jitter_basis_points
    }

    /// Returns the bounded number of concurrent stream resume requests.
    pub const fn maximum_parallel_resumes(self) -> usize {
        self.maximum_parallel_resumes
    }

    /// Calculates the wait after a one-based failed connection attempt.
    ///
    /// The jitter sample is clamped to this policy's configured range. Integer
    /// arithmetic saturates at the maximum delay. This function performs no IO
    /// and does not panic.
    pub fn delay_after_failure(self, failed_attempt: u32, jitter_sample: u16) -> Duration {
        let exponent = failed_attempt.saturating_sub(1);
        let maximum_nanos = self.maximum_delay.as_nanos();
        let initial_nanos = self.initial_delay.as_nanos();
        let maximum_factor = maximum_nanos
            .saturating_mul(BASIS_POINTS)
            .div_ceil(initial_nanos);
        let factor = scaled_power(
            u128::from(self.backoff_basis_points),
            exponent,
            maximum_factor,
        );
        let base_nanos = initial_nanos
            .saturating_mul(factor)
            .div_ceil(BASIS_POINTS)
            .min(maximum_nanos);
        let bounded_sample = jitter_sample.min(self.jitter_basis_points);
        let jitter_nanos = base_nanos
            .saturating_mul(u128::from(bounded_sample))
            .div_ceil(BASIS_POINTS);
        duration_from_nanos(base_nanos.saturating_add(jitter_nanos).min(maximum_nanos))
    }
}

/// Interruptible cancellation shared with connector and backoff waits.
#[derive(Clone, Default)]
pub struct ClientRecoveryCancellation {
    inner: Arc<ClientRecoveryCancellationInner>,
}

impl ClientRecoveryCancellation {
    /// Requests cancellation and wakes any current backoff wait.
    ///
    /// Repeated cancellation is idempotent. This function performs no external
    /// IO and does not panic.
    pub fn cancel(&self) {
        // Use the waiter's mutex so cancellation cannot notify between its
        // predicate check and entry into the condition-variable wait.
        let _guard = self
            .inner
            .wait_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.inner.cancelled.store(true, Ordering::Release);
        self.inner.changed.notify_all();
    }

    /// Returns whether cancellation was requested.
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    fn wait_cancelled(&self, duration: Duration) -> bool {
        if self.is_cancelled() {
            return true;
        }
        let guard = self
            .inner
            .wait_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _ = self
            .inner
            .changed
            .wait_timeout_while(guard, duration, |_| !self.is_cancelled())
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.is_cancelled()
    }
}

#[derive(Default)]
struct ClientRecoveryCancellationInner {
    cancelled: AtomicBool,
    wait_lock: Mutex<()>,
    changed: Condvar,
}

/// Source of bounded backoff jitter, supplied at the SDK adapter boundary.
pub trait ClientRecoveryJitter: Send + Sync {
    /// Returns an additive jitter sample in basis points.
    ///
    /// Implementations should return a value within `0..=maximum`; callers
    /// clamp larger values. This function must not expose private client data.
    fn sample_basis_points(&self, failed_attempt: u32, maximum: u16) -> u16;
}

/// Allocation-free deterministic jitter generator for a caller-provided seed.
///
/// Production adapters should choose distinct, non-secret seeds per logical
/// client so reconnecting clients do not synchronize their retry attempts.
pub struct DeterministicClientRecoveryJitter {
    state: AtomicU64,
}

impl DeterministicClientRecoveryJitter {
    /// Constructs a generator. Zero is remapped to a non-zero xorshift state.
    pub const fn new(seed: u64) -> Self {
        Self {
            state: AtomicU64::new(if seed == 0 {
                0x9e37_79b9_7f4a_7c15
            } else {
                seed
            }),
        }
    }
}

impl ClientRecoveryJitter for DeterministicClientRecoveryJitter {
    fn sample_basis_points(&self, _failed_attempt: u32, maximum: u16) -> u16 {
        if maximum == 0 {
            return 0;
        }
        let previous = self
            .state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |mut value| {
                value ^= value << 13;
                value ^= value >> 7;
                value ^= value << 17;
                Some(value)
            })
            .unwrap_or_else(|value| value);
        let mut sample = previous;
        sample ^= sample << 13;
        sample ^= sample >> 7;
        sample ^= sample << 17;
        u16::try_from(sample % (u64::from(maximum) + 1)).unwrap_or(maximum)
    }
}

/// One newly negotiated physical transport returned by a connector adapter.
pub struct NegotiatedClientTransport {
    agreement: ClientConnectionAgreement,
    transport: Arc<dyn ClientTransport>,
}

impl NegotiatedClientTransport {
    /// Groups a successfully negotiated agreement and its physical transport.
    pub fn new(agreement: ClientConnectionAgreement, transport: Arc<dyn ClientTransport>) -> Self {
        Self {
            agreement,
            transport,
        }
    }

    /// Separates the negotiated agreement from its owned physical transport.
    pub fn into_parts(self) -> (ClientConnectionAgreement, Arc<dyn ClientTransport>) {
        (self.agreement, self.transport)
    }
}

/// Adapter port that establishes and negotiates replacement client transports.
pub trait ClientConnector: Send + Sync {
    /// Attempts one physical connection.
    ///
    /// Implementations must honor cancellation while blocking and must apply
    /// their own bounded dial and handshake timeouts.
    ///
    /// # Errors
    ///
    /// Returns a payload-redacted transport failure retaining its source.
    fn connect(
        &self,
        cancellation: &ClientRecoveryCancellation,
    ) -> Result<NegotiatedClientTransport, ClientTransportError>;
}

/// Resume-safe state for one logical subscription.
pub struct SubscriptionCheckpoint {
    stream_id: StreamId,
    server_epoch: ServerEpoch,
    after: StreamSequence,
    trace_context: TraceContext,
    payload: Arc<[u8]>,
}

impl SubscriptionCheckpoint {
    /// Constructs a checkpoint from a prior stream and its product
    /// subscription specification.
    pub fn new(
        stream_id: ClientStreamId,
        server_epoch: ClientServerEpoch,
        after: ClientStreamSequence,
        trace_context: ClientTraceContext,
        payload: Arc<[u8]>,
    ) -> Self {
        Self {
            stream_id: stream_id.into_raw(),
            server_epoch: server_epoch.into_raw(),
            after: after.into_raw(),
            trace_context: trace_context.into_raw(),
            payload,
        }
    }

    pub(crate) fn new_raw(
        stream_id: StreamId,
        server_epoch: ServerEpoch,
        after: StreamSequence,
        trace_context: TraceContext,
        payload: Arc<[u8]>,
    ) -> Self {
        Self {
            stream_id,
            server_epoch,
            after,
            trace_context,
            payload,
        }
    }

    /// Returns the stable client-scoped stream identity.
    pub fn stream_id(&self) -> ClientStreamId {
        ClientStreamId::from_raw(self.stream_id.clone())
    }

    pub(crate) const fn stream_id_raw(&self) -> &StreamId {
        &self.stream_id
    }

    /// Returns the prior server epoch used to detect replay loss.
    pub fn server_epoch(&self) -> ClientServerEpoch {
        ClientServerEpoch::from_raw(self.server_epoch.clone())
    }

    /// Returns the last sequence consumed by the caller.
    pub const fn after(&self) -> ClientStreamSequence {
        ClientStreamSequence::from_raw(self.after)
    }

    /// Returns distributed context retained from the original logical open.
    pub fn trace_context(&self) -> ClientTraceContext {
        ClientTraceContext::from_raw(self.trace_context.clone())
    }

    /// Returns the encoded product subscription specification.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl Debug for SubscriptionCheckpoint {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SubscriptionCheckpoint")
            .field("stream_id", &self.stream_id)
            .field("server_epoch", &self.server_epoch)
            .field("after", &self.after)
            .field("payload_bytes", &self.payload.len())
            .finish()
    }
}

impl Subscription {
    /// Captures the minimum state needed to resume this logical stream.
    ///
    /// The sequence advances only after [`Subscription::recv`] or
    /// [`Subscription::recv_timeout`] delivers an item to the caller. Product
    /// payload bytes are retained but omitted from debug output.
    pub fn checkpoint(&self, payload: Arc<[u8]>) -> SubscriptionCheckpoint {
        SubscriptionCheckpoint::new_raw(
            self.stream_id_raw().clone(),
            self.client().agreement_raw().server_epoch().clone(),
            self.last_sequence_raw(),
            self.trace_context_raw().clone(),
            payload,
        )
    }
}

/// Per-stream result of one replacement-connection recovery pass.
pub struct RecoveredSubscription {
    /// Original resume-safe checkpoint.
    pub checkpoint: SubscriptionCheckpoint,
    /// Opened replacement stream or its isolated typed failure.
    pub result: Result<OpenedSubscription, ClientError>,
}

/// A replacement client and every independently attempted logical stream.
pub struct RecoveredClient {
    /// One newly established shared physical client connection.
    pub client: Client,
    /// Exact physical connection attempts used by this recovery pass.
    pub connection_attempts: u32,
    /// Results in the same stable order as supplied checkpoints.
    pub subscriptions: Vec<RecoveredSubscription>,
}

/// Failure before a replacement physical connection can be returned.
#[derive(Debug)]
pub enum ClientRecoveryError {
    /// Recovery was cancelled before a replacement became usable.
    Cancelled {
        /// Physical connection attempts completed before cancellation.
        attempts: u32,
    },
    /// Runtime recovery bounds are invalid.
    Limits(ClientRecoveryLimitError),
    /// The checkpoint set reused one active logical stream ID.
    DuplicateStreamId {
        /// Duplicated stream identity.
        stream_id: ClientStreamId,
    },
    /// Checkpoints exceed the replacement connection's negotiated stream limit.
    TooManyStreams {
        /// Requested stream count.
        requested: usize,
        /// Negotiated inclusive maximum.
        maximum: u32,
    },
    /// Replacement agreement cannot resume prior logical streams.
    MissingResumeCapability,
    /// Bounded physical connection attempts were exhausted.
    AttemptsExhausted {
        /// Exact attempts made.
        attempts: u32,
        /// Last payload-redacted connector failure.
        source: ClientTransportError,
    },
    /// The connector explicitly forbids retrying the unchanged connection.
    ConnectorRejected {
        /// Exact physical attempts made before the rejection.
        attempts: u32,
        /// Payload-redacted connector failure with retry guidance.
        source: ClientTransportError,
    },
    /// A negotiated transport could not start the shared SDK workers.
    Client(ClientConnectError),
    /// Operating system could not create a bounded resume worker.
    ResumeWorker(std::io::Error),
    /// A resume worker terminated unexpectedly.
    ResumeWorkerPanicked,
    /// A resume worker failed to return every bounded result.
    IncompleteResumeBatch {
        /// Expected checkpoint result count.
        expected: usize,
        /// Observed result count.
        actual: usize,
    },
}

impl Display for ClientRecoveryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled { attempts } => write!(
                formatter,
                "client connection recovery was cancelled after {attempts} attempts"
            ),
            Self::Limits(error) => Display::fmt(error, formatter),
            Self::DuplicateStreamId { stream_id } => {
                write!(
                    formatter,
                    "stream ID {} has more than one checkpoint",
                    stream_id.as_str()
                )
            }
            Self::TooManyStreams { requested, maximum } => write!(
                formatter,
                "client recovery requested {requested} streams above negotiated maximum {maximum}"
            ),
            Self::MissingResumeCapability => formatter
                .write_str("replacement connection did not negotiate stream resume capability"),
            Self::AttemptsExhausted { attempts, .. } => write!(
                formatter,
                "client connection recovery exhausted {attempts} attempts"
            ),
            Self::ConnectorRejected { attempts, .. } => write!(
                formatter,
                "client connector rejected unchanged recovery after {attempts} attempts"
            ),
            Self::Client(_) => formatter.write_str("replacement SDK connection could not start"),
            Self::ResumeWorker(_) => {
                formatter.write_str("client stream resume worker could not start")
            }
            Self::ResumeWorkerPanicked => {
                formatter.write_str("client stream resume worker terminated unexpectedly")
            }
            Self::IncompleteResumeBatch { expected, actual } => write!(
                formatter,
                "client stream resume returned {actual} of {expected} results"
            ),
        }
    }
}

impl Error for ClientRecoveryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Limits(error) => Some(error),
            Self::AttemptsExhausted { source, .. } | Self::ConnectorRejected { source, .. } => {
                Some(source)
            }
            Self::Client(error) => Some(error),
            Self::ResumeWorker(error) => Some(error),
            Self::Cancelled { .. }
            | Self::DuplicateStreamId { .. }
            | Self::TooManyStreams { .. }
            | Self::MissingResumeCapability
            | Self::ResumeWorkerPanicked
            | Self::IncompleteResumeBatch { .. } => None,
        }
    }
}

impl From<ClientRecoveryLimitError> for ClientRecoveryError {
    fn from(value: ClientRecoveryLimitError) -> Self {
        Self::Limits(value)
    }
}

/// Establishes one replacement physical connection and resumes checkpoints.
///
/// Connector failures use bounded exponential backoff with interruptible
/// jittered waits unless the connector forbids retrying unchanged. After connection, checkpoint resumes execute concurrently up
/// to the configured worker bound, while failures remain isolated per logical
/// stream. Cancellation during resume closes the replacement and joins its resume
/// workers before returning; subscriptions may already have been opened remotely.
/// This function performs connector, transport, thread, and blocking
/// response-wait IO and does not panic.
///
/// # Errors
///
/// Returns cancellation, invalid checkpoint/bound, exhausted or non-retryable connector,
/// incompatible agreement, SDK worker, or resume-worker failures. Individual
/// stream rejection is returned inside [`RecoveredSubscription`].
pub fn recover_client_connection(
    connector: &dyn ClientConnector,
    client_limits: ClientRuntimeLimits,
    recovery_policy: ClientRecoveryPolicy,
    jitter: &dyn ClientRecoveryJitter,
    cancellation: &ClientRecoveryCancellation,
    checkpoints: Vec<SubscriptionCheckpoint>,
    resume_timeout: Duration,
) -> Result<RecoveredClient, ClientRecoveryError> {
    recover_client_connection_with_telemetry(
        connector,
        client_limits,
        recovery_policy,
        jitter,
        cancellation,
        checkpoints,
        resume_timeout,
        Arc::new(BoundedClientTelemetry::default()),
    )
}

/// Establishes and resumes one replacement connection with caller telemetry.
///
/// The supplied telemetry is shared by the physical client and every logical
/// resume. It must be non-blocking and payload-safe. Recovery behavior and
/// failure semantics match [`recover_client_connection`].
///
/// # Errors
///
/// Returns cancellation, invalid checkpoint/bound, exhausted or non-retryable connector,
/// incompatible agreement, SDK worker, or resume-worker failures.
#[allow(clippy::too_many_arguments)]
pub fn recover_client_connection_with_telemetry(
    connector: &dyn ClientConnector,
    client_limits: ClientRuntimeLimits,
    recovery_policy: ClientRecoveryPolicy,
    jitter: &dyn ClientRecoveryJitter,
    cancellation: &ClientRecoveryCancellation,
    checkpoints: Vec<SubscriptionCheckpoint>,
    resume_timeout: Duration,
    telemetry: Arc<dyn ClientTelemetry>,
) -> Result<RecoveredClient, ClientRecoveryError> {
    if resume_timeout.is_zero() {
        return Err(ClientRecoveryLimitError::ZeroLimit {
            field: "resume_timeout",
        }
        .into());
    }
    validate_checkpoint_ids(&checkpoints)?;
    let (client, connection_attempts) = connect_with_recovery(
        connector,
        client_limits,
        recovery_policy,
        jitter,
        cancellation,
        telemetry,
    )?;
    if cancellation.is_cancelled() {
        client.close();
        return Err(ClientRecoveryError::Cancelled {
            attempts: connection_attempts,
        });
    }
    let maximum = client.agreement().limits().max_logical_streams();
    if checkpoints.len() > maximum as usize {
        client.close();
        return Err(ClientRecoveryError::TooManyStreams {
            requested: checkpoints.len(),
            maximum,
        });
    }
    if !checkpoints.is_empty()
        && !client.agreement().capabilities().iter().any(|capability| {
            *capability == undercurrent_wire::connection_capabilities::STREAM_RESUME
        })
    {
        client.close();
        return Err(ClientRecoveryError::MissingResumeCapability);
    }
    let subscriptions = resume_checkpoints(
        &client,
        checkpoints,
        recovery_policy.maximum_parallel_resumes(),
        resume_timeout,
        cancellation,
        connection_attempts,
    )?;
    if cancellation.is_cancelled() {
        client.close();
        return Err(ClientRecoveryError::Cancelled {
            attempts: connection_attempts,
        });
    }
    Ok(RecoveredClient {
        client,
        connection_attempts,
        subscriptions,
    })
}

fn connect_with_recovery(
    connector: &dyn ClientConnector,
    client_limits: ClientRuntimeLimits,
    policy: ClientRecoveryPolicy,
    jitter: &dyn ClientRecoveryJitter,
    cancellation: &ClientRecoveryCancellation,
    telemetry: Arc<dyn ClientTelemetry>,
) -> Result<(Client, u32), ClientRecoveryError> {
    for attempt in 1..=policy.maximum_attempts() {
        if cancellation.is_cancelled() {
            return Err(ClientRecoveryError::Cancelled {
                attempts: attempt.saturating_sub(1),
            });
        }
        match connector.connect(cancellation) {
            Ok(connection) => {
                let (agreement, transport) = connection.into_parts();
                return Client::connect_with_telemetry(
                    agreement,
                    transport,
                    client_limits,
                    telemetry.clone(),
                )
                .map(|client| (client, attempt))
                .map_err(ClientRecoveryError::Client);
            }
            Err(source) => {
                if cancellation.is_cancelled() {
                    return Err(ClientRecoveryError::Cancelled { attempts: attempt });
                }
                if source.overload().is_some_and(|overload| {
                    overload.retry() == ClientRetryGuidance::DoNotRetryUnchanged
                }) {
                    return Err(ClientRecoveryError::ConnectorRejected {
                        attempts: attempt,
                        source,
                    });
                }
                if attempt == policy.maximum_attempts() {
                    return Err(ClientRecoveryError::AttemptsExhausted {
                        attempts: attempt,
                        source,
                    });
                }
                let sample = jitter.sample_basis_points(attempt, policy.jitter_basis_points);
                if cancellation.wait_cancelled(policy.delay_after_failure(attempt, sample)) {
                    return Err(ClientRecoveryError::Cancelled { attempts: attempt });
                }
            }
        }
    }
    Err(ClientRecoveryError::Cancelled {
        attempts: policy.maximum_attempts(),
    })
}

fn validate_checkpoint_ids(
    checkpoints: &[SubscriptionCheckpoint],
) -> Result<(), ClientRecoveryError> {
    let mut ids = BTreeSet::new();
    for checkpoint in checkpoints {
        if !ids.insert(checkpoint.stream_id.clone()) {
            return Err(ClientRecoveryError::DuplicateStreamId {
                stream_id: ClientStreamId::from_raw(checkpoint.stream_id.clone()),
            });
        }
    }
    Ok(())
}

fn resume_checkpoints(
    client: &Client,
    checkpoints: Vec<SubscriptionCheckpoint>,
    maximum_parallel: usize,
    timeout: Duration,
    cancellation: &ClientRecoveryCancellation,
    connection_attempts: u32,
) -> Result<Vec<RecoveredSubscription>, ClientRecoveryError> {
    let expected = checkpoints.len();
    if expected == 0 {
        return Ok(Vec::new());
    }
    let work = Arc::new(Mutex::new(
        checkpoints.into_iter().enumerate().collect::<VecDeque<_>>(),
    ));
    // Error and cancellation paths may join before draining all results. Keep
    // room for every checkpoint so no sender can block that cleanup join.
    // See [resume bounds](../docs/internals/subscription-recovery.md#bounded-attempts-and-manual-recovery).
    let (sender, receiver) = mpsc::sync_channel(expected);
    let worker_count = expected.min(maximum_parallel);
    let mut workers = Vec::with_capacity(worker_count);
    for index in 0..worker_count {
        let work = work.clone();
        let sender = sender.clone();
        let worker_client = client.clone();
        let worker_cancellation = cancellation.clone();
        let worker = thread::Builder::new()
            .name(format!("undercurrent-sdk-resume-{index}"))
            .spawn(move || {
                loop {
                    if worker_cancellation.is_cancelled() {
                        break;
                    }
                    let Some((position, checkpoint)) = work
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .pop_front()
                    else {
                        break;
                    };
                    if worker_cancellation.is_cancelled() {
                        break;
                    }
                    let result = worker_client.resume_subscription_with_trace_context(
                        ClientStreamId::from_raw(checkpoint.stream_id.clone()),
                        ClientServerEpoch::from_raw(checkpoint.server_epoch.clone()),
                        ClientStreamSequence::from_raw(checkpoint.after),
                        checkpoint.payload.clone(),
                        ClientTraceContext::from_raw(checkpoint.trace_context.clone()),
                        timeout,
                    );
                    if sender.send((position, checkpoint, result)).is_err() {
                        break;
                    }
                }
            })
            .map_err(|error| {
                client.close();
                join_workers(workers.drain(..));
                ClientRecoveryError::ResumeWorker(error)
            })?;
        workers.push(worker);
    }
    drop(sender);
    let mut results = Vec::with_capacity(expected);
    while results.len() < expected {
        if cancellation.is_cancelled() {
            // Closing the shared replacement releases active activation waits.
            // Joining first would wait for every per-stream timeout to expire.
            client.close();
            break;
        }
        match receiver.recv_timeout(RESUME_CANCELLATION_POLL) {
            Ok(result) => results.push(result),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    if join_workers(workers.drain(..)) {
        client.close();
        return Err(ClientRecoveryError::ResumeWorkerPanicked);
    }
    if cancellation.is_cancelled() {
        client.close();
        return Err(ClientRecoveryError::Cancelled {
            attempts: connection_attempts,
        });
    }
    if results.len() != expected {
        client.close();
        return Err(ClientRecoveryError::IncompleteResumeBatch {
            expected,
            actual: results.len(),
        });
    }
    results.sort_unstable_by_key(|(position, _, _)| *position);
    Ok(results
        .into_iter()
        .map(|(_, checkpoint, result)| RecoveredSubscription { checkpoint, result })
        .collect())
}

fn join_workers(workers: impl Iterator<Item = JoinHandle<()>>) -> bool {
    let mut panicked = false;
    for worker in workers {
        panicked |= worker.join().is_err();
    }
    panicked
}

fn scaled_power(mut base: u128, mut exponent: u32, cap: u128) -> u128 {
    let mut result = BASIS_POINTS.min(cap);
    while exponent > 0 {
        if exponent & 1 == 1 {
            result = scaled_multiply(result, base, cap);
            if result >= cap {
                return cap;
            }
        }
        exponent >>= 1;
        if exponent > 0 {
            base = scaled_multiply(base, base, cap);
        }
    }
    result
}

fn scaled_multiply(left: u128, right: u128, cap: u128) -> u128 {
    left.saturating_mul(right).div_ceil(BASIS_POINTS).min(cap)
}

fn duration_from_nanos(nanos: u128) -> Duration {
    let seconds = (nanos / NANOS_PER_SECOND).min(u128::from(u64::MAX));
    let subsecond = u32::try_from(nanos % NANOS_PER_SECOND).unwrap_or(999_999_999);
    Duration::new(u64::try_from(seconds).unwrap_or(u64::MAX), subsecond)
}
