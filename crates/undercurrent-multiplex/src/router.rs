// SPDX-License-Identifier: Apache-2.0

//! Client-side correlation and logical-stream routing over one connection.
//!
//! Pending response waits consume their handles; stream waits borrow a mailbox.
//! Mailboxes track routed and consumed sequences separately so teardown cannot
//! acknowledge unread data. The outer adapter must explicitly close the router,
//! send transport cancellation/unsubscribe messages, and decide whether routing
//! failures require a physical reconnect.

use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

use undercurrent_wire::{ConnectionLimits, RequestId, StreamId, StreamSequence};

use crate::{InboundStreamItem, MultiplexError};

/// Terminal reason delivered to pending requests and logical streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteClosedReason {
    /// Request or stream was explicitly cancelled by its owner.
    Cancelled,
    /// Physical connection ended or failed.
    ConnectionClosed,
    /// One logical stream was unsubscribed without closing the connection.
    Unsubscribed,
    /// A single stream exceeded its bounded mailbox.
    Overloaded,
}

/// Failure while waiting for one correlated finite response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingResponseError {
    /// Request was explicitly cancelled.
    Cancelled,
    /// Physical connection closed before the response arrived.
    ConnectionClosed,
    /// Caller-supplied wait timeout elapsed.
    Timeout,
    /// Internal one-shot response route ended unexpectedly.
    RouteDisconnected,
}

impl Display for PendingResponseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Cancelled => "pending request was cancelled",
            Self::ConnectionClosed => "connection closed before response arrived",
            Self::Timeout => "pending response wait timed out",
            Self::RouteDisconnected => "pending response route disconnected",
        })
    }
}

impl Error for PendingResponseError {}

type RoutedResponse = Result<Arc<[u8]>, RouteClosedReason>;

/// One finite response handle correlated by request ID.
///
/// Dropping it removes its pending registration without sending a transport
/// cancellation. Do not reuse the identity while an older handle remains alive:
/// cleanup removes by ID, without a generation token.
pub struct PendingResponse {
    request_id: RequestId,
    receiver: Receiver<RoutedResponse>,
    router: Weak<Mutex<RouterState>>,
}

impl PendingResponse {
    /// Returns the correlated request identity.
    pub const fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    /// Blocks until the matching response, cancellation, or teardown arrives.
    ///
    /// No router lock is retained while waiting. This method performs blocking
    /// synchronization, no transport IO, and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed cancellation, connection, or internal route failure.
    pub fn recv(self) -> Result<Arc<[u8]>, PendingResponseError> {
        map_routed_response(self.receiver.recv())
    }

    /// Waits up to `timeout` for the matching finite response.
    ///
    /// The wait consumes this handle even on timeout; dropping it releases the
    /// pending route. A timeout does not establish whether remote work executed.
    ///
    /// No router lock is retained while waiting. This method performs blocking
    /// synchronization, no transport IO, and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed timeout, cancellation, connection, or route failure.
    pub fn recv_timeout(self, timeout: Duration) -> Result<Arc<[u8]>, PendingResponseError> {
        match self.receiver.recv_timeout(timeout) {
            Ok(response) => map_route_result(response),
            Err(mpsc::RecvTimeoutError::Timeout) => Err(PendingResponseError::Timeout),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Err(PendingResponseError::RouteDisconnected)
            }
        }
    }
}

impl Drop for PendingResponse {
    fn drop(&mut self) {
        let Some(router) = self.router.upgrade() else {
            return;
        };
        router
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pending
            .remove(&self.request_id);
    }
}

/// Error returned while reading one logical stream handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundStreamError {
    /// Stream was cancelled by its handle owner.
    Cancelled,
    /// Stream was independently unsubscribed.
    Unsubscribed,
    /// This logical stream exhausted its mailbox. Peers remain usable.
    Overloaded,
    /// Physical connection closed.
    ConnectionClosed,
    /// Caller-supplied wait timeout elapsed.
    Timeout,
}

impl Display for InboundStreamError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Cancelled => "logical stream was cancelled",
            Self::Unsubscribed => "logical stream was unsubscribed",
            Self::Overloaded => "logical stream mailbox is overloaded",
            Self::ConnectionClosed => "logical stream connection closed",
            Self::Timeout => "logical stream wait timed out",
        })
    }
}

impl Error for InboundStreamError {}

/// Independently owned mailbox with a weak reference to connection-router state.
///
/// Dropping the handle removes its route but sends no transport message. Do not
/// reopen the same identity while an older handle can still remove it on drop.
pub struct InboundStream {
    stream_id: StreamId,
    mailbox: Arc<StreamMailbox>,
    router: Weak<Mutex<RouterState>>,
}

impl InboundStream {
    /// Returns this independent logical stream identity.
    pub const fn stream_id(&self) -> &StreamId {
        &self.stream_id
    }

    /// Blocks until the next item or terminal stream reason.
    ///
    /// No connection-router lock is retained while waiting. This method
    /// performs blocking synchronization, no transport IO, and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed cancellation, unsubscribe, or connection closure.
    pub fn recv(&self) -> Result<InboundStreamItem, InboundStreamError> {
        self.mailbox.recv(None)
    }

    /// Waits up to `timeout` for the next stream item.
    ///
    /// No connection-router lock is retained while waiting. This method
    /// performs blocking synchronization, no transport IO, and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed timeout or terminal stream reason.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<InboundStreamItem, InboundStreamError> {
        self.mailbox.recv(Some(timeout))
    }

    /// Returns the latest sequence delivered to this consumer, including after
    /// connection teardown.
    pub fn last_sequence(&self) -> StreamSequence {
        self.mailbox
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .consumed_sequence
    }

    /// Returns exact encoded bytes currently retained for this consumer.
    pub fn queued_bytes(&self) -> u64 {
        self.mailbox
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .queued_bytes
    }
}

impl Drop for InboundStream {
    fn drop(&mut self) {
        let Some(router) = self.router.upgrade() else {
            return;
        };
        let mailbox = router
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .streams
            .remove(&self.stream_id);
        if let Some(mailbox) = mailbox {
            mailbox.close(RouteClosedReason::Cancelled);
        }
    }
}

/// Current bounded correlation-router pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouterPressure {
    /// Pending finite request count.
    pub pending_requests: usize,
    /// Active logical stream count.
    pub active_streams: usize,
    /// Exact encoded bytes retained across client stream mailboxes.
    pub stream_queued_bytes: u64,
}

/// Resources released by client-side connection teardown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouterRelease {
    /// Pending finite requests failed.
    pub pending_requests: usize,
    /// Logical stream mailboxes terminated.
    pub streams: usize,
    /// Exact encoded stream bytes released.
    pub queued_bytes: u64,
}

/// Cheaply clonable client-side request and stream correlation router.
#[derive(Clone)]
pub struct ConnectionRouter {
    state: Arc<Mutex<RouterState>>,
    max_pending_requests: usize,
    max_streams: usize,
}

impl ConnectionRouter {
    /// Constructs an empty router from negotiated request and stream limits.
    ///
    /// Construction performs no IO and does not panic.
    pub fn new(limits: ConnectionLimits) -> Self {
        Self {
            state: Arc::new(Mutex::new(RouterState::default())),
            max_pending_requests: limits.max_pending_requests() as usize,
            max_streams: limits.max_logical_streams() as usize,
        }
    }

    /// Registers one unique finite request before its frame is written.
    ///
    /// # Errors
    ///
    /// Returns connection-closed, active-ID conflict, or capacity failure.
    pub fn register_request(
        &self,
        request_id: RequestId,
    ) -> Result<PendingResponse, MultiplexError> {
        let mut state = self.lock_state();
        state.ensure_open()?;
        if state.pending.contains_key(&request_id) {
            return Err(MultiplexError::RequestIdInUse { request_id });
        }
        if state.pending.len() >= self.max_pending_requests {
            return Err(MultiplexError::CapacityExceeded {
                resource: "pending_requests",
                limit: self.max_pending_requests,
            });
        }
        let (sender, receiver) = mpsc::sync_channel(1);
        state.pending.insert(request_id.clone(), sender);
        Ok(PendingResponse {
            request_id,
            receiver,
            router: Arc::downgrade(&self.state),
        })
    }

    /// Routes an out-of-order response to its exact pending caller.
    ///
    /// The route is removed before notification and no router lock is retained
    /// while a caller consumes the result. This function performs no transport
    /// IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns connection-closed or unknown-request failure.
    pub fn route_response(
        &self,
        request_id: &RequestId,
        bytes: Arc<[u8]>,
    ) -> Result<(), MultiplexError> {
        let sender = {
            let mut state = self.lock_state();
            state.ensure_open()?;
            state
                .pending
                .remove(request_id)
                .ok_or_else(|| MultiplexError::UnknownRequest {
                    request_id: request_id.clone(),
                })?
        };
        let _ = sender.send(Ok(bytes));
        Ok(())
    }

    /// Cancels one pending finite request without affecting other routes.
    ///
    /// Returns whether an active route was cancelled. This function performs no
    /// transport IO and does not panic.
    pub fn cancel_request(&self, request_id: &RequestId) -> bool {
        let sender = self.lock_state().pending.remove(request_id);
        if let Some(sender) = sender {
            let _ = sender.send(Err(RouteClosedReason::Cancelled));
            true
        } else {
            false
        }
    }

    /// Registers one independently bounded inbound logical stream mailbox.
    ///
    /// # Errors
    ///
    /// Returns connection-closed, active-ID conflict, zero mailbox limit, or
    /// negotiated stream capacity failure.
    pub fn open_stream(
        &self,
        stream_id: StreamId,
        max_messages: usize,
        max_bytes: u64,
    ) -> Result<InboundStream, MultiplexError> {
        self.open_stream_after(stream_id, max_messages, max_bytes, StreamSequence::INITIAL)
    }

    /// Registers a resumed inbound stream after its last consumed sequence.
    ///
    /// Initializing both routed and consumed checkpoints prevents a second
    /// reconnect from moving backward before any new item arrives. This
    /// function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns connection-closed, active-ID conflict, zero mailbox limit, or
    /// negotiated stream capacity failure.
    pub fn open_stream_after(
        &self,
        stream_id: StreamId,
        max_messages: usize,
        max_bytes: u64,
        after: StreamSequence,
    ) -> Result<InboundStream, MultiplexError> {
        let mut state = self.lock_state();
        state.ensure_open()?;
        if max_messages == 0 || max_bytes == 0 {
            return Err(MultiplexError::InvalidLimit {
                field: "stream mailbox limits",
            });
        }
        if state.streams.contains_key(&stream_id) {
            return Err(MultiplexError::StreamIdInUse { stream_id });
        }
        if state.streams.len() >= self.max_streams {
            return Err(MultiplexError::CapacityExceeded {
                resource: "logical_streams",
                limit: self.max_streams,
            });
        }
        let mailbox = Arc::new(StreamMailbox::new(max_messages, max_bytes, after));
        state.streams.insert(stream_id.clone(), mailbox.clone());
        Ok(InboundStream {
            stream_id,
            mailbox,
            router: Arc::downgrade(&self.state),
        })
    }

    /// Routes one ordered item only to its explicitly registered stream.
    ///
    /// # Errors
    ///
    /// Returns connection, stream, sequence, or bounded slow-consumer failure.
    pub fn route_stream_item(
        &self,
        stream_id: &StreamId,
        item: InboundStreamItem,
    ) -> Result<(), MultiplexError> {
        self.route_stream_item_with_queued_bytes(stream_id, item)
            .map(|_| ())
    }

    /// Routes one item and returns exact post-enqueue bytes from the mailbox lock.
    ///
    /// This atomic pressure value prevents telemetry from racing a concurrent
    /// consumer that drains the item immediately after it becomes visible.
    pub fn route_stream_item_with_queued_bytes(
        &self,
        stream_id: &StreamId,
        item: InboundStreamItem,
    ) -> Result<u64, MultiplexError> {
        let mailbox =
            {
                let state = self.lock_state();
                state.ensure_open()?;
                state.streams.get(stream_id).cloned().ok_or_else(|| {
                    MultiplexError::UnknownStream {
                        stream_id: stream_id.clone(),
                    }
                })?
            };
        mailbox.push(stream_id, item)
    }

    /// Unsubscribes one logical stream without changing requests or peers.
    ///
    /// # Errors
    ///
    /// Returns connection-closed or unknown-stream failure.
    pub fn close_stream(&self, stream_id: &StreamId) -> Result<(), MultiplexError> {
        self.close_stream_with_reason(stream_id, RouteClosedReason::Unsubscribed)
    }

    /// Terminates only one overloaded stream, preserving requests and peers.
    pub fn overload_stream(&self, stream_id: &StreamId) -> Result<(), MultiplexError> {
        self.close_stream_with_reason(stream_id, RouteClosedReason::Overloaded)
    }

    fn close_stream_with_reason(
        &self,
        stream_id: &StreamId,
        reason: RouteClosedReason,
    ) -> Result<(), MultiplexError> {
        let mailbox = {
            let mut state = self.lock_state();
            state.ensure_open()?;
            state
                .streams
                .remove(stream_id)
                .ok_or_else(|| MultiplexError::UnknownStream {
                    stream_id: stream_id.clone(),
                })?
        };
        mailbox.close(reason);
        Ok(())
    }

    /// Returns registry counts and a sampled sum of mailbox byte charges.
    ///
    /// The registry remains locked while each mailbox is sampled, but consumers
    /// can drain other mailboxes concurrently. The sum is not a single atomic
    /// snapshot across all mailbox locks.
    pub fn pressure(&self) -> RouterPressure {
        let state = self.lock_state();
        let stream_queued_bytes = state.streams.values().fold(0_u64, |total, mailbox| {
            total.saturating_add(
                mailbox
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .queued_bytes,
            )
        });
        RouterPressure {
            pending_requests: state.pending.len(),
            active_streams: state.streams.len(),
            stream_queued_bytes,
        }
    }

    /// Returns exact encoded bytes retained by one logical stream mailbox.
    ///
    /// Lookup is logarithmic in active stream count and does not scan peer
    /// mailboxes. This function performs no external IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns connection-closed or unknown-stream failure.
    pub fn stream_queued_bytes(&self, stream_id: &StreamId) -> Result<u64, MultiplexError> {
        let mailbox =
            {
                let state = self.lock_state();
                state.ensure_open()?;
                state.streams.get(stream_id).cloned().ok_or_else(|| {
                    MultiplexError::UnknownStream {
                        stream_id: stream_id.clone(),
                    }
                })?
            };
        Ok(mailbox
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .queued_bytes)
    }

    /// Fails pending requests, terminates streams, and releases router entries.
    ///
    /// Adapters must call this during teardown: dropping all router clones does
    /// not close mailboxes still owned by stream handles. Unread items are cleared.
    ///
    /// Stream handles retain their last sequence for reconnect resume. Repeated
    /// close is idempotent. This function performs no transport IO and does not
    /// panic.
    pub fn close(&self) -> RouterRelease {
        let (pending, streams) = {
            let mut state = self.lock_state();
            if state.closed {
                return RouterRelease {
                    pending_requests: 0,
                    streams: 0,
                    queued_bytes: 0,
                };
            }
            state.closed = true;
            (
                std::mem::take(&mut state.pending),
                std::mem::take(&mut state.streams),
            )
        };
        for sender in pending.values() {
            let _ = sender.send(Err(RouteClosedReason::ConnectionClosed));
        }
        let queued_bytes = streams.values().fold(0_u64, |total, mailbox| {
            total.saturating_add(mailbox.close(RouteClosedReason::ConnectionClosed))
        });
        RouterRelease {
            pending_requests: pending.len(),
            streams: streams.len(),
            queued_bytes,
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, RouterState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[derive(Default)]
struct RouterState {
    closed: bool,
    pending: BTreeMap<RequestId, SyncSender<RoutedResponse>>,
    streams: BTreeMap<StreamId, Arc<StreamMailbox>>,
}

impl RouterState {
    fn ensure_open(&self) -> Result<(), MultiplexError> {
        if self.closed {
            Err(MultiplexError::ConnectionClosed)
        } else {
            Ok(())
        }
    }
}

struct StreamMailbox {
    max_messages: usize,
    max_bytes: u64,
    state: Mutex<StreamMailboxState>,
    available: Condvar,
}

impl StreamMailbox {
    fn new(max_messages: usize, max_bytes: u64, after: StreamSequence) -> Self {
        Self {
            max_messages,
            max_bytes,
            state: Mutex::new(StreamMailboxState {
                routed_sequence: after,
                consumed_sequence: after,
                ..StreamMailboxState::default()
            }),
            available: Condvar::new(),
        }
    }

    fn push(&self, stream_id: &StreamId, item: InboundStreamItem) -> Result<u64, MultiplexError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Route lookup releases the router lock with a cloned mailbox. Close can
        // win before this lock is acquired, so registry lookup alone is insufficient.
        // See [close and ownership](../docs/internals/routing.md#waits-close-and-ownership).
        if state.closed.is_some() {
            return Err(MultiplexError::UnknownStream {
                stream_id: stream_id.clone(),
            });
        }
        let sequence = item.sequence();
        if sequence <= state.routed_sequence {
            return Err(MultiplexError::StreamSequenceOutOfOrder {
                stream_id: stream_id.clone(),
                previous: state.routed_sequence.value(),
                received: sequence.value(),
            });
        }
        let bytes = item.encoded_len();
        if state.queue.len() >= self.max_messages
            || state.queued_bytes.saturating_add(bytes) > self.max_bytes
        {
            return Err(MultiplexError::QueueSaturated {
                queue: "client_stream_mailbox",
                byte_limit: self.max_bytes,
                queued_bytes: state.queued_bytes,
                requested_bytes: bytes,
            });
        }
        state.routed_sequence = sequence;
        state.queued_bytes = state.queued_bytes.saturating_add(bytes);
        state.queue.push_back(item);
        let queued_bytes = state.queued_bytes;
        self.available.notify_one();
        Ok(queued_bytes)
    }

    fn recv(&self, timeout: Option<Duration>) -> Result<InboundStreamItem, InboundStreamError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let started_at = Instant::now();
        loop {
            if let Some(item) = state.queue.pop_front() {
                state.queued_bytes = state.queued_bytes.saturating_sub(item.encoded_len());
                state.consumed_sequence = item.sequence();
                return Ok(item);
            }
            if let Some(reason) = state.closed {
                return Err(map_stream_close(reason));
            }
            match timeout {
                Some(timeout) => {
                    let elapsed = started_at.elapsed();
                    if elapsed >= timeout {
                        return Err(InboundStreamError::Timeout);
                    }
                    let (next, result) = self
                        .available
                        .wait_timeout(state, timeout.saturating_sub(elapsed))
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    state = next;
                    if result.timed_out() && state.queue.is_empty() {
                        return Err(InboundStreamError::Timeout);
                    }
                }
                None => {
                    state = self
                        .available
                        .wait(state)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
            }
        }
    }

    fn close(&self, reason: RouteClosedReason) -> u64 {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.closed.is_some() {
            return 0;
        }
        let released = state.queued_bytes;
        // Keep consumed_sequence unchanged: discarded arrivals must remain eligible
        // for replay after disconnect, even though routed_sequence has seen them.
        // See [consumed checkpoints](../docs/internals/routing.md#stream-mailboxes-and-checkpoints).
        state.queue.clear();
        state.queued_bytes = 0;
        state.closed = Some(reason);
        self.available.notify_all();
        released
    }
}

#[derive(Default)]
struct StreamMailboxState {
    queue: VecDeque<InboundStreamItem>,
    queued_bytes: u64,
    routed_sequence: StreamSequence,
    consumed_sequence: StreamSequence,
    closed: Option<RouteClosedReason>,
}

fn map_routed_response(
    response: Result<RoutedResponse, mpsc::RecvError>,
) -> Result<Arc<[u8]>, PendingResponseError> {
    match response {
        Ok(response) => map_route_result(response),
        Err(_) => Err(PendingResponseError::RouteDisconnected),
    }
}

fn map_route_result(response: RoutedResponse) -> Result<Arc<[u8]>, PendingResponseError> {
    match response {
        Ok(bytes) => Ok(bytes),
        Err(
            RouteClosedReason::Cancelled
            | RouteClosedReason::Unsubscribed
            | RouteClosedReason::Overloaded,
        ) => Err(PendingResponseError::Cancelled),
        Err(RouteClosedReason::ConnectionClosed) => Err(PendingResponseError::ConnectionClosed),
    }
}

const fn map_stream_close(reason: RouteClosedReason) -> InboundStreamError {
    match reason {
        RouteClosedReason::Cancelled => InboundStreamError::Cancelled,
        RouteClosedReason::ConnectionClosed => InboundStreamError::ConnectionClosed,
        RouteClosedReason::Unsubscribed => InboundStreamError::Unsubscribed,
        RouteClosedReason::Overloaded => InboundStreamError::Overloaded,
    }
}
