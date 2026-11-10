// SPDX-License-Identifier: Apache-2.0

//! One event-driven worker for every daemon-owned logical product stream.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::{self, JoinHandle};

use undercurrent_application::{MetricName, MetricObservation, SubscriptionWake, Telemetry};
use undercurrent_transport_core::ServerQueueDrainWake;
use undercurrent_wire::{ClientId, StreamId};

use super::{PreparedProductStream, ProductStreamPublisher};

/// Hard bound for daemon-owned logical product stream registrations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductStreamPumpLimits {
    streams: usize,
}

impl ProductStreamPumpLimits {
    /// Constructs a non-zero global stream-registration bound.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed limit failure when `streams` is zero.
    pub const fn new(streams: usize) -> Result<Self, ProductStreamPumpLimitError> {
        if streams == 0 {
            return Err(ProductStreamPumpLimitError);
        }
        Ok(Self { streams })
    }

    /// Returns the inclusive daemon-owned registration maximum.
    pub const fn streams(self) -> usize {
        self.streams
    }
}

/// Invalid product stream pump configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductStreamPumpLimitError;

impl Display for ProductStreamPumpLimitError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("product stream pump limit streams must be non-zero")
    }
}

impl Error for ProductStreamPumpLimitError {}

/// Failure constructing the shared product stream worker.
#[derive(Debug)]
pub enum ProductStreamPumpCreateError {
    /// Configured hard limits are invalid.
    Limits(ProductStreamPumpLimitError),
    /// The named shared worker thread could not be started.
    Worker(std::io::Error),
}

impl Display for ProductStreamPumpCreateError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Limits(error) => Display::fmt(error, formatter),
            Self::Worker(_) => formatter.write_str("product stream pump worker could not start"),
        }
    }
}

impl Error for ProductStreamPumpCreateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Limits(error) => Some(error),
            Self::Worker(error) => Some(error),
        }
    }
}

impl From<ProductStreamPumpLimitError> for ProductStreamPumpCreateError {
    fn from(value: ProductStreamPumpLimitError) -> Self {
        Self::Limits(value)
    }
}

/// Payload-free failure reserving one logical stream slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProductStreamReservationError {
    Closed,
    Capacity { limit: usize },
    IdentityInUse,
    IdentityExhausted,
}

impl Display for ProductStreamReservationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => formatter.write_str("product stream pump is closed"),
            Self::Capacity { limit } => {
                write!(
                    formatter,
                    "product stream pump capacity {limit} is exhausted"
                )
            }
            Self::IdentityInUse => formatter.write_str("logical product stream is already active"),
            Self::IdentityExhausted => {
                formatter.write_str("product stream registration identity is exhausted")
            }
        }
    }
}

impl Error for ProductStreamReservationError {}

/// Current payload-free pressure for the shared product stream pump.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductStreamPumpPressure {
    /// Reserved streams still preparing application sources.
    pub preparing_streams: usize,
    /// Activated application sources.
    pub active_streams: usize,
    /// Coalesced source-ready registrations.
    pub ready_streams: usize,
    /// Reliable publishers waiting for scheduler capacity.
    pub backpressured_streams: usize,
    /// Streams removed after a source, presentation, or publication failure.
    pub failed_streams: u64,
    /// Inclusive configured registration maximum.
    pub limit: usize,
    /// Whether shutdown has begun.
    pub closed: bool,
}

/// Result of one bounded source publication attempt.
pub(crate) enum ProductStreamSourcePoll {
    Idle,
    More,
}

/// Stable payload-free source failure stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProductStreamSourceError {
    Backpressured,
    Source,
    Presentation,
    Publication,
}

/// One independently owned selective stream source.
pub(crate) trait ProductStreamSource: Send {
    /// Publishes at most one bounded item and reports immediate retained work.
    fn publish_next(
        &mut self,
        publisher: &ProductStreamPublisher,
    ) -> Result<ProductStreamSourcePoll, ProductStreamSourceError>;
}

/// Shared event-driven product stream worker.
pub struct ProductStreamPump {
    shared: Arc<ProductStreamPumpShared>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl ProductStreamPump {
    /// Starts exactly one named worker for all configured logical streams.
    ///
    /// Construction performs operating-system thread creation but no network,
    /// process, terminal, or storage IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns invalid-limit or worker-thread creation failure.
    pub fn new(limits: ProductStreamPumpLimits) -> Result<Self, ProductStreamPumpCreateError> {
        Self::build(limits, None)
    }

    /// Starts the shared stream worker with a non-blocking telemetry port.
    ///
    /// Logical-stream gauges are emitted only after registration locks are
    /// released and carry no client or stream identity.
    ///
    /// # Errors
    ///
    /// Returns invalid-limit or worker-thread creation failure.
    pub fn with_telemetry(
        limits: ProductStreamPumpLimits,
        telemetry: Arc<dyn Telemetry>,
    ) -> Result<Self, ProductStreamPumpCreateError> {
        Self::build(limits, Some(telemetry))
    }

    fn build(
        limits: ProductStreamPumpLimits,
        telemetry: Option<Arc<dyn Telemetry>>,
    ) -> Result<Self, ProductStreamPumpCreateError> {
        let limits = ProductStreamPumpLimits::new(limits.streams())?;
        let shared = Arc::new(ProductStreamPumpShared {
            limits,
            next_id: AtomicU64::new(0),
            state: Mutex::new(ProductStreamPumpState::default()),
            ready: Condvar::new(),
            telemetry,
        });
        let worker_shared = shared.clone();
        let worker = thread::Builder::new()
            .name("undercurrent-stream-pump".to_owned())
            .spawn(move || run_worker(worker_shared))
            .map_err(ProductStreamPumpCreateError::Worker)?;
        Ok(Self {
            shared,
            worker: Mutex::new(Some(worker)),
        })
    }

    /// Returns a lock-consistent payload-free pressure snapshot.
    pub fn pressure(&self) -> ProductStreamPumpPressure {
        self.shared.pressure()
    }

    /// Releases one logical stream without inspecting or scanning peer sources.
    ///
    /// Unknown streams are an idempotent no-op. This function performs bounded
    /// in-memory cleanup, no external IO, and does not panic.
    pub fn close_stream(&self, client_id: &ClientId, stream_id: &StreamId) {
        self.shared
            .close_key(&ProductStreamKey::new(client_id.clone(), stream_id.clone()));
    }

    /// Releases every stream owned by one logical client.
    ///
    /// Work is proportional only to that client's registered streams. This
    /// function performs no external IO and does not panic.
    pub fn close_connection(&self, client_id: &ClientId) {
        self.shared.close_client(client_id);
    }

    /// Stops the shared worker and releases every retained source.
    ///
    /// Repeated shutdown is idempotent. This function joins the one pump worker,
    /// performs no network, process, terminal, or storage IO, and does not panic.
    pub fn shutdown(&self) {
        self.shared.shutdown();
        let worker = self
            .worker
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(worker) = worker {
            let _ = worker.join();
        }
    }

    pub(crate) fn reserve(
        &self,
        client_id: ClientId,
        stream_id: StreamId,
    ) -> Result<ProductStreamReservation, ProductStreamReservationError> {
        let key = ProductStreamKey::new(client_id, stream_id);
        let id = self.shared.reserve(key)?;
        Ok(ProductStreamReservation {
            shared: Arc::downgrade(&self.shared),
            id,
            retained: true,
        })
    }
}

impl Drop for ProductStreamPump {
    fn drop(&mut self) {
        self.shutdown();
    }
}

pub(crate) struct ProductStreamReservation {
    shared: Weak<ProductStreamPumpShared>,
    id: u64,
    retained: bool,
}

impl ProductStreamReservation {
    pub(crate) fn wake(&self) -> Arc<dyn SubscriptionWake> {
        Arc::new(ProductStreamWake {
            shared: self.shared.clone(),
            id: self.id,
        })
    }

    pub(crate) fn into_prepared(
        self,
        available_from: Option<u64>,
        source: Box<dyn ProductStreamSource>,
    ) -> Box<dyn PreparedProductStream> {
        Box::new(PumpPreparedProductStream {
            reservation: Some(self),
            available_from,
            source: Some(source),
        })
    }

    fn activate(mut self, source: Box<dyn ProductStreamSource>, publisher: ProductStreamPublisher) {
        self.retained = false;
        let Some(shared) = self.shared.upgrade() else {
            publisher.close();
            return;
        };
        let client_id = shared.client_for(self.id);
        let Some(client_id) = client_id else {
            publisher.close();
            return;
        };
        let wake: Arc<dyn ServerQueueDrainWake> = Arc::new(ProductConnectionDrainWake {
            shared: Arc::downgrade(&shared),
            client_id,
        });
        if publisher.set_queue_drain_wake(wake).is_err() {
            shared.close_id(self.id, false);
            publisher.close();
            return;
        }
        shared.activate(self.id, source, publisher);
    }
}

impl Drop for ProductStreamReservation {
    fn drop(&mut self) {
        if self.retained
            && let Some(shared) = self.shared.upgrade()
        {
            shared.close_id(self.id, false);
        }
    }
}

struct PumpPreparedProductStream {
    reservation: Option<ProductStreamReservation>,
    available_from: Option<u64>,
    source: Option<Box<dyn ProductStreamSource>>,
}

impl PreparedProductStream for PumpPreparedProductStream {
    fn available_from(&self) -> Option<u64> {
        self.available_from
    }

    fn activate(mut self: Box<Self>, publisher: ProductStreamPublisher) {
        let Some(reservation) = self.reservation.take() else {
            publisher.close();
            return;
        };
        let Some(source) = self.source.take() else {
            publisher.close();
            return;
        };
        reservation.activate(source, publisher);
    }
}

struct ProductStreamPumpShared {
    limits: ProductStreamPumpLimits,
    next_id: AtomicU64,
    state: Mutex<ProductStreamPumpState>,
    ready: Condvar,
    telemetry: Option<Arc<dyn Telemetry>>,
}

impl ProductStreamPumpShared {
    fn reserve(&self, key: ProductStreamKey) -> Result<u64, ProductStreamReservationError> {
        let mut state = self.lock_state();
        if state.closed {
            return Err(ProductStreamReservationError::Closed);
        }
        if state.by_key.contains_key(&key) {
            return Err(ProductStreamReservationError::IdentityInUse);
        }
        if state.registrations.len() >= self.limits.streams() {
            return Err(ProductStreamReservationError::Capacity {
                limit: self.limits.streams(),
            });
        }
        let id = self
            .next_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map(|previous| previous + 1)
            .map_err(|_| ProductStreamReservationError::IdentityExhausted)?;
        state
            .by_client
            .entry(key.client_id.clone())
            .or_default()
            .insert(id);
        state.by_key.insert(key.clone(), id);
        state.registrations.insert(
            id,
            PumpRegistration {
                key,
                active: None,
                ready: false,
                backpressured: false,
                wake_pending: false,
            },
        );
        let registrations = state.registrations.len();
        drop(state);
        self.record_logical_streams(registrations);
        Ok(id)
    }

    fn activate(
        &self,
        id: u64,
        source: Box<dyn ProductStreamSource>,
        publisher: ProductStreamPublisher,
    ) {
        let active = Arc::new(ActiveProductStream {
            source: Mutex::new(source),
            publisher,
            closed: AtomicBool::new(false),
        });
        let mut state = self.lock_state();
        if state.closed {
            drop(state);
            active.close();
            return;
        }
        let Some(registration) = state.registrations.get_mut(&id) else {
            drop(state);
            active.close();
            return;
        };
        registration.active = Some(active);
        if !registration.ready {
            registration.ready = true;
            state.ready.push_back(id);
            self.ready.notify_one();
        }
    }

    fn client_for(&self, id: u64) -> Option<ClientId> {
        self.lock_state()
            .registrations
            .get(&id)
            .map(|registration| registration.key.client_id.clone())
    }

    fn mark_ready(&self, id: u64) {
        let mut state = self.lock_state();
        let Some(registration) = state.registrations.get_mut(&id) else {
            return;
        };
        if registration.active.is_none() || registration.ready {
            return;
        }
        if registration.backpressured {
            registration.wake_pending = true;
            return;
        }
        registration.ready = true;
        state.ready.push_back(id);
        self.ready.notify_one();
    }

    fn complete_turn(&self, id: u64, source_has_more: bool) {
        let mut state = self.lock_state();
        let Some(registration) = state.registrations.get_mut(&id) else {
            return;
        };
        if registration.backpressured || registration.active.is_none() || registration.ready {
            return;
        }
        if !source_has_more && !registration.wake_pending {
            return;
        }
        registration.wake_pending = false;
        registration.ready = true;
        state.ready.push_back(id);
        self.ready.notify_one();
    }

    fn mark_backpressured(&self, id: u64) {
        let mut state = self.lock_state();
        let Some(registration) = state.registrations.get(&id) else {
            return;
        };
        if registration.backpressured {
            return;
        }
        let client_id = registration.key.client_id.clone();
        // The writer can drain after publication fails but before we park here.
        // Consume that remembered opportunity so a stream with no further source
        // events still retries its pending item instead of sleeping indefinitely.
        // See [pump scheduling](../../docs/internals/output-and-streams.md#why-one-slow-stream-does-not-allocate-a-worker).
        if state.drain_available.remove(&client_id) {
            let Some(registration) = state.registrations.get_mut(&id) else {
                return;
            };
            if !registration.ready {
                registration.ready = true;
                state.ready.push_back(id);
                self.ready.notify_one();
            }
            return;
        }
        if let Some(registration) = state.registrations.get_mut(&id) {
            registration.backpressured = true;
        }
        state
            .backpressured
            .entry(client_id)
            .or_default()
            .push_back(id);
    }

    fn wake_backpressured(&self, client_id: &ClientId) {
        let mut state = self.lock_state();
        loop {
            let Some(id) = state
                .backpressured
                .get_mut(client_id)
                .and_then(VecDeque::pop_front)
            else {
                state.backpressured.remove(client_id);
                state.drain_available.insert(client_id.clone());
                return;
            };
            let Some(registration) = state.registrations.get_mut(&id) else {
                continue;
            };
            if !registration.backpressured || registration.active.is_none() {
                continue;
            }
            registration.backpressured = false;
            if !registration.ready {
                registration.ready = true;
                state.ready.push_back(id);
                self.ready.notify_one();
            }
            if state
                .backpressured
                .get(client_id)
                .is_some_and(VecDeque::is_empty)
            {
                state.backpressured.remove(client_id);
            }
            return;
        }
    }

    fn next_ready(&self) -> Option<(u64, Arc<ActiveProductStream>)> {
        let mut state = self.lock_state();
        loop {
            if state.closed {
                return None;
            }
            while let Some(id) = state.ready.pop_front() {
                let Some(registration) = state.registrations.get_mut(&id) else {
                    continue;
                };
                registration.ready = false;
                if let Some(active) = &registration.active {
                    return Some((id, active.clone()));
                }
            }
            state = self
                .ready
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    fn close_key(&self, key: &ProductStreamKey) {
        let id = self.lock_state().by_key.get(key).copied();
        if let Some(id) = id {
            self.close_id(id, false);
        }
    }

    fn close_client(&self, client_id: &ClientId) {
        let ids = self
            .lock_state()
            .by_client
            .get(client_id)
            .cloned()
            .unwrap_or_default();
        for id in ids {
            self.close_id(id, false);
        }
    }

    fn close_id(&self, id: u64, failed: bool) {
        let (active, registrations) = {
            let mut state = self.lock_state();
            let Some(registration) = state.registrations.remove(&id) else {
                return;
            };
            state.by_key.remove(&registration.key);
            if let Some(ids) = state.by_client.get_mut(&registration.key.client_id) {
                ids.remove(&id);
                if ids.is_empty() {
                    state.by_client.remove(&registration.key.client_id);
                }
            }
            state.ready.retain(|candidate| *candidate != id);
            if let Some(ids) = state.backpressured.get_mut(&registration.key.client_id) {
                ids.retain(|candidate| *candidate != id);
                if ids.is_empty() {
                    state.backpressured.remove(&registration.key.client_id);
                }
            }
            if !state.by_client.contains_key(&registration.key.client_id) {
                state.drain_available.remove(&registration.key.client_id);
            }
            if failed {
                state.failed_streams = state.failed_streams.saturating_add(1);
            }
            (registration.active, state.registrations.len())
        };
        self.record_logical_streams(registrations);
        if let Some(active) = active {
            active.close();
        }
    }

    fn shutdown(&self) {
        let (active, changed) = {
            let mut state = self.lock_state();
            if state.closed {
                (Vec::new(), false)
            } else {
                state.closed = true;
                state.ready.clear();
                state.backpressured.clear();
                state.drain_available.clear();
                state.by_key.clear();
                state.by_client.clear();
                let registrations = std::mem::take(&mut state.registrations);
                self.ready.notify_all();
                (
                    registrations
                        .into_values()
                        .filter_map(|registration| registration.active)
                        .collect(),
                    true,
                )
            }
        };
        if changed {
            self.record_logical_streams(0);
        }
        for active in active {
            active.close();
        }
    }

    fn pressure(&self) -> ProductStreamPumpPressure {
        let state = self.lock_state();
        let active_streams = state
            .registrations
            .values()
            .filter(|registration| registration.active.is_some())
            .count();
        ProductStreamPumpPressure {
            preparing_streams: state.registrations.len().saturating_sub(active_streams),
            active_streams,
            ready_streams: state
                .registrations
                .values()
                .filter(|registration| registration.ready)
                .count(),
            backpressured_streams: state
                .registrations
                .values()
                .filter(|registration| registration.backpressured)
                .count(),
            failed_streams: state.failed_streams,
            limit: self.limits.streams(),
            closed: state.closed,
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, ProductStreamPumpState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn record_logical_streams(&self, value: usize) {
        if let Some(telemetry) = self.telemetry.as_ref() {
            telemetry.record_metric(MetricObservation {
                name: MetricName::LogicalStreamsActive,
                value: value as f64,
                operation: None,
            });
        }
    }
}

#[derive(Default)]
struct ProductStreamPumpState {
    registrations: BTreeMap<u64, PumpRegistration>,
    by_key: BTreeMap<ProductStreamKey, u64>,
    by_client: BTreeMap<ClientId, BTreeSet<u64>>,
    ready: VecDeque<u64>,
    backpressured: BTreeMap<ClientId, VecDeque<u64>>,
    drain_available: BTreeSet<ClientId>,
    failed_streams: u64,
    closed: bool,
}

struct PumpRegistration {
    key: ProductStreamKey,
    active: Option<Arc<ActiveProductStream>>,
    ready: bool,
    backpressured: bool,
    wake_pending: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ProductStreamKey {
    client_id: ClientId,
    stream_id: StreamId,
}

impl ProductStreamKey {
    const fn new(client_id: ClientId, stream_id: StreamId) -> Self {
        Self {
            client_id,
            stream_id,
        }
    }
}

struct ActiveProductStream {
    source: Mutex<Box<dyn ProductStreamSource>>,
    publisher: ProductStreamPublisher,
    closed: AtomicBool,
}

impl ActiveProductStream {
    fn publish_next(&self) -> Result<ProductStreamSourcePoll, ProductStreamSourceError> {
        if self.closed.load(Ordering::Acquire) || self.publisher.is_closed() {
            return Err(ProductStreamSourceError::Publication);
        }
        self.source
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .publish_next(&self.publisher)
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.publisher.close();
    }
}

struct ProductStreamWake {
    shared: Weak<ProductStreamPumpShared>,
    id: u64,
}

impl SubscriptionWake for ProductStreamWake {
    fn wake(&self) {
        if let Some(shared) = self.shared.upgrade() {
            shared.mark_ready(self.id);
        }
    }
}

struct ProductConnectionDrainWake {
    shared: Weak<ProductStreamPumpShared>,
    client_id: ClientId,
}

impl ServerQueueDrainWake for ProductConnectionDrainWake {
    fn wake(&self) {
        if let Some(shared) = self.shared.upgrade() {
            shared.wake_backpressured(&self.client_id);
        }
    }
}

fn run_worker(shared: Arc<ProductStreamPumpShared>) {
    while let Some((id, active)) = shared.next_ready() {
        match active.publish_next() {
            Ok(ProductStreamSourcePoll::More) => shared.complete_turn(id, true),
            Ok(ProductStreamSourcePoll::Idle) => shared.complete_turn(id, false),
            Err(ProductStreamSourceError::Backpressured) => shared.mark_backpressured(id),
            Err(
                ProductStreamSourceError::Source
                | ProductStreamSourceError::Presentation
                | ProductStreamSourceError::Publication,
            ) => shared.close_id(id, true),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservations_are_bounded_unique_and_released_by_owner() {
        assert!(ProductStreamPumpLimits::new(0).is_err());
        let pump = ProductStreamPump::new(ProductStreamPumpLimits::new(1).unwrap()).unwrap();
        let client = ClientId::new("client-1").unwrap();
        let first_stream = StreamId::new("stream-1").unwrap();
        let first = pump
            .reserve(client.clone(), first_stream.clone())
            .expect("first reservation");
        assert!(matches!(
            pump.reserve(client.clone(), first_stream),
            Err(ProductStreamReservationError::IdentityInUse)
        ));
        assert!(matches!(
            pump.reserve(client.clone(), StreamId::new("stream-2").unwrap()),
            Err(ProductStreamReservationError::Capacity { limit: 1 })
        ));
        assert_eq!(pump.pressure().preparing_streams, 1);

        drop(first);
        assert_eq!(pump.pressure().preparing_streams, 0);
        let _replacement = pump
            .reserve(client.clone(), StreamId::new("stream-2").unwrap())
            .expect("released capacity is reusable");
        pump.close_connection(&client);
        assert_eq!(pump.pressure().preparing_streams, 0);

        pump.shutdown();
        assert!(pump.pressure().closed);
        assert!(matches!(
            pump.reserve(client, StreamId::new("stream-3").unwrap()),
            Err(ProductStreamReservationError::Closed)
        ));
    }

    #[test]
    fn registration_gauge_is_emitted_after_state_changes() {
        let telemetry =
            Arc::new(undercurrent_runtime::BoundedTelemetryRecorder::new(1, 8).expect("telemetry"));
        let pump = ProductStreamPump::with_telemetry(
            ProductStreamPumpLimits::new(2).expect("limits"),
            telemetry.clone(),
        )
        .expect("stream pump");
        let reservation = pump
            .reserve(
                ClientId::new("client-metric").expect("client"),
                StreamId::new("stream-metric").expect("stream"),
            )
            .expect("reservation");
        drop(reservation);

        let aggregate = telemetry
            .metric_aggregates()
            .into_iter()
            .find(|aggregate| aggregate.name == MetricName::LogicalStreamsActive)
            .expect("logical stream gauge");
        assert_eq!(aggregate.samples, 2);
        assert_eq!(aggregate.minimum, 0.0);
        assert_eq!(aggregate.maximum, 1.0);
        assert_eq!(aggregate.latest, 0.0);
        pump.shutdown();
    }
}
