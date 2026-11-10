// SPDX-License-Identifier: Apache-2.0

//! One daemon-side physical connection and its logical work registry.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use undercurrent_multiplex::{
    ClientMessage, DeliverySemantics, EnqueueOutcome, FrameRoute, MultiplexError,
    MultiplexMetricName, MultiplexMetricObservation, MultiplexTelemetry, NoopMultiplexTelemetry,
    OutboundFrame, OutboundScheduler, ReplayDisposition, ReplayJournal, ReplayRecordOutcome,
    ReplayResult, ReplayStreamKey, ScheduledFrame, SchedulerLimits, SchedulerPressure,
    ServerMessage, StreamFrameKind, StreamGap, StreamKind, StreamPressure, StreamRecoveryAction,
    TrafficClass,
};
use undercurrent_wire::{
    ClientId, ConnectionAgreement, PageSize, RequestId, ServerEpoch, StreamId, StreamSequence,
    TraceContext, connection_capabilities,
};
use undercurrent_wire::{DecodeStatus, WireCodec};

use crate::{ServerConnectionError, ServerDiagnosticLimits};

/// Maximum distinct request identities retained for one physical connection epoch.
///
/// Exhausting this ledger requires a replacement connection rather than eviction,
/// so a completed request identity can never become reusable in the same epoch.
pub const REQUEST_ID_EPOCH_LEDGER_LIMIT: usize = 1_048_576;

/// Maximum retained request-ID UTF-8 bytes per physical connection epoch.
///
/// This 16 MiB payload budget complements the entry cap; it is not an RSS cap.
/// Exhaustion requires connection replacement, never eviction or identity reuse.
pub const REQUEST_ID_EPOCH_LEDGER_BYTES: usize = 16 * 1024 * 1024;

/// Internal product response class for a cancelled pending request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerRequestKind {
    /// Finite product request.
    FiniteRequest,
    /// New or resumed logical-stream activation request.
    StreamOpen,
}

/// Product work dispatched after connection-level validation and reservation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerAction {
    /// Execute one finite product request.
    Request {
        /// Connection-epoch request identity.
        request_id: RequestId,
        /// Distributed caller context for execution and diagnostics.
        trace_context: TraceContext,
        /// Opaque product protocol payload.
        payload: Arc<[u8]>,
    },
    /// Stop no-longer-useful request work when safe.
    CancelRequest {
        /// Previously accepted request identity.
        request_id: RequestId,
        /// Product response codec required for the correlated terminal result.
        request_kind: ServerRequestKind,
    },
    /// Validate and prepare one new product subscription.
    OpenStream {
        /// Finite open-operation identity.
        request_id: RequestId,
        /// Reserved logical stream identity.
        stream_id: StreamId,
        /// Distributed caller context for stream preparation diagnostics.
        trace_context: TraceContext,
        /// Opaque product subscription specification.
        payload: Arc<[u8]>,
    },
    /// Validate and prepare one reconnecting product subscription.
    ResumeStream {
        /// Finite resume-operation identity.
        request_id: RequestId,
        /// Reserved prior logical stream identity.
        stream_id: StreamId,
        /// Prior server epoch supplied by the client.
        server_epoch: ServerEpoch,
        /// Last fully consumed prior sequence.
        after: StreamSequence,
        /// Original logical-stream context retained across reconnects.
        trace_context: TraceContext,
        /// Opaque product subscription specification.
        payload: Arc<[u8]>,
    },
    /// Stop one product subscription without affecting its peers.
    CloseStream {
        /// Logical stream being removed.
        stream_id: StreamId,
    },
    /// Stop connection-owned request and subscription work.
    CloseConnection,
}

/// Product-selected delivery contract for one accepted logical stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerStreamConfiguration {
    /// Stable product stream kind.
    pub kind: StreamKind,
    /// Semantic outbound priority class.
    pub class: TrafficClass,
    /// Live saturation behavior.
    pub delivery: DeliverySemantics,
    /// Deterministic action after an unrecoverable gap.
    pub recovery: StreamRecoveryAction,
    /// Whether successfully written frames enter bounded reconnect replay.
    pub retain_for_resume: bool,
}

/// Accepted stream activation and any immediate recovery work queued.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamActivationOutcome {
    /// Stream activated by a resume request rather than a new open.
    pub resumed: bool,
    /// Retained frames scheduled after reconnect.
    pub replayed_items: usize,
    /// Highest accepted logical sequence after replay or gap scheduling.
    pub last_sequence: StreamSequence,
    /// Explicit recovery gap scheduled when replay was incomplete.
    pub gap: Option<StreamGap>,
}

/// Current bounded state for one daemon-side physical connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerConnectionPressure {
    /// Pending finite requests, including stream activation requests.
    pub pending_requests: usize,
    /// Distinct request identities retained for this connection epoch.
    pub request_ids_used: usize,
    /// Hard non-evicting request-identity ledger bound.
    pub request_id_limit: usize,
    /// Reserved and active logical streams.
    pub logical_streams: usize,
    /// Scheduler queue and credit pressure.
    pub scheduler: SchedulerPressure,
    /// Whether teardown has begun.
    pub closed: bool,
}

/// Payload-free lifecycle and pressure state for one logical stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerStreamDiagnosticState {
    /// Product stream activation has not completed.
    Opening {
        /// Finite request that reserved the stream identity.
        request_id: RequestId,
    },
    /// Product stream is active in the outbound scheduler.
    Active {
        /// Product-selected delivery and recovery behavior.
        configuration: ServerStreamConfiguration,
        /// Current stream-local queue, credit, sequence, and gap pressure.
        pressure: StreamPressure,
    },
}

/// One payload-free logical-stream diagnostic row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerStreamDiagnostic {
    /// Connection-local logical stream identity.
    pub stream_id: StreamId,
    /// Inclusive encoded-byte queue bound enforced for this logical stream.
    pub queue_byte_limit: u64,
    /// Inclusive queued-message bound enforced for this logical stream.
    pub queue_message_limit: u64,
    /// Current activation state and bounded pressure.
    pub state: ServerStreamDiagnosticState,
}

/// Work performed to construct one logical-stream diagnostic page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerStreamDiagnosticCost {
    /// Ordered stream entries examined, including one look-ahead entry.
    pub stream_entries_examined: usize,
    /// Active scheduler streams sampled for pressure.
    pub active_streams_sampled: usize,
}

/// Stable stream-ID-ordered page for one physical connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerStreamDiagnosticPage {
    /// Payload-free rows in ascending stream-ID order.
    pub items: Vec<ServerStreamDiagnostic>,
    /// Exclusive stream-ID cursor for the next page when more rows exist.
    pub next_after: Option<StreamId>,
    /// Aggregate connection pressure captured with page selection.
    pub connection: ServerConnectionPressure,
    /// Exact bounded work performed by this read.
    pub cost: ServerStreamDiagnosticCost,
}

/// Connection-local resources released during teardown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerConnectionRelease {
    /// Pending requests cancelled.
    pub pending_requests: usize,
    /// Reserved and active streams removed.
    pub logical_streams: usize,
    /// Exact queued encoded bytes released.
    pub queued_bytes: u64,
    /// Queued frames released.
    pub queued_messages: usize,
}

/// Payload-free notification that one physical writer removed queued work.
///
/// Implementations should only mark bounded application work ready. They must
/// return promptly, perform no transport IO, and must not call back into the
/// server connection synchronously.
pub trait ServerQueueDrainWake: Send + Sync {
    /// Signals that a previously backpressured product publisher may retry.
    fn wake(&self);
}

/// Completion of a finite response's physical write, including failed delivery.
///
/// This is independent of application settlement. A written response may still
/// be lost before the peer consumes it. Teardown can observe completion without
/// retaining a connection or invoking callbacks under transport locks.
#[derive(Debug, Default)]
pub struct ServerResponseWrite {
    state: AtomicU8,
}

impl ServerResponseWrite {
    /// Returns whether the response was written or abandoned.
    pub fn is_finished(&self) -> bool {
        self.state.load(Ordering::Acquire) != 0
    }

    /// Returns whether the complete frame was accepted by the transport.
    pub fn was_written(&self) -> bool {
        self.state.load(Ordering::Acquire) == 1
    }
}

struct ResponseWriteGuard(Arc<ServerResponseWrite>);

impl Drop for ResponseWriteGuard {
    fn drop(&mut self) {
        let _ = self
            .0
            .state
            .compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire);
    }
}

/// One selected physical write whose replay effect awaits IO success.
///
/// Selection already removed this frame from scheduler queue accounting. Dropping
/// the handle neither records replay nor requeues the frame. Preserve selection
/// order through physical writes and confirmations.
pub struct PendingServerWrite {
    bytes: Arc<[u8]>,
    replay: Arc<ReplayJournal>,
    confirmation: Option<ReplayConfirmation>,
    response_write: Option<ResponseWriteGuard>,
}

impl PendingServerWrite {
    /// Returns exact binary frame bytes for the physical transport writer.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Shares encoded bytes without confirming IO or advancing replay retention.
    pub fn shared_bytes(&self) -> Arc<[u8]> {
        self.bytes.clone()
    }

    /// Confirms successful physical write and advances replay when required.
    ///
    /// The caller must invoke this only after the complete frame was accepted
    /// by the transport. This function performs bounded in-memory retention,
    /// no transport IO, and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed replay size, ordering, capacity, or invariant failure.
    pub fn confirm_written(self) -> Result<Option<ReplayRecordOutcome>, ServerConnectionError> {
        if let Some(completion) = &self.response_write {
            completion.0.state.store(1, Ordering::Release);
        }
        let Some(confirmation) = self.confirmation else {
            return Ok(None);
        };
        match self.replay.record_frame(
            &confirmation.key,
            confirmation.sequence,
            confirmation.kind,
            self.bytes,
        ) {
            Ok(outcome) => Ok(Some(outcome)),
            // Unsubscribe can remove the journal while an already selected
            // physical write finishes. Its bytes were delivered, but the closed
            // interest no longer needs retention; peers must stay connected.
            Err(MultiplexError::UnknownStream { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

/// Transport-neutral server state machine for one negotiated connection.
///
/// # Callback contract
///
/// The engine itself performs no transport IO. Telemetry and queue-drain
/// callbacks run synchronously and their blocking or panics are not isolated.
/// A panic can propagate after state or queue mutation; recovering a poisoned
/// mutex does not roll back that mutation. Callbacks must not reenter session
/// methods: telemetry can run while the connection-state lock is held.
/// See [synchronization and lifetimes](../../docs/internals/ownership.md#synchronization-and-lifetimes).
pub struct ServerConnectionSession {
    client_id: ClientId,
    agreement: ConnectionAgreement,
    codec: WireCodec,
    scheduler: OutboundScheduler,
    stream_queue_message_limit: u64,
    telemetry: Arc<dyn MultiplexTelemetry>,
    replay: Arc<ReplayJournal>,
    diagnostic_limits: ServerDiagnosticLimits,
    state: Mutex<ConnectionState>,
    write_wait: Mutex<u64>,
    write_ready: Condvar,
    #[cfg(test)]
    write_wait_hooks: wake_tests::WaitHooks,
    queue_drain_wake: Mutex<Option<Arc<dyn ServerQueueDrainWake>>>,
}

impl ServerConnectionSession {
    /// Composes one validated server connection runtime.
    ///
    /// Construction performs no transport IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns missing capability, mismatched negotiated limits or epoch,
    /// insufficient replay item capacity, or wire-header configuration failure.
    pub fn new(
        client_id: ClientId,
        agreement: ConnectionAgreement,
        scheduler_limits: SchedulerLimits,
        replay: Arc<ReplayJournal>,
        diagnostic_limits: ServerDiagnosticLimits,
    ) -> Result<Self, ServerConnectionError> {
        Self::with_telemetry(
            client_id,
            agreement,
            scheduler_limits,
            replay,
            diagnostic_limits,
            Arc::new(NoopMultiplexTelemetry),
        )
    }

    /// Composes one connection runtime with typed multiplex telemetry.
    ///
    /// The session and its scheduler retain the supplied telemetry port;
    /// callbacks must remain non-blocking. Construction performs no
    /// transport IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns the same typed validation failures as [`Self::new`].
    pub fn with_telemetry(
        client_id: ClientId,
        agreement: ConnectionAgreement,
        scheduler_limits: SchedulerLimits,
        replay: Arc<ReplayJournal>,
        diagnostic_limits: ServerDiagnosticLimits,
        telemetry: Arc<dyn MultiplexTelemetry>,
    ) -> Result<Self, ServerConnectionError> {
        for capability in [
            connection_capabilities::MULTIPLEX,
            connection_capabilities::FLOW_CONTROL,
            connection_capabilities::TRACE_CONTEXT,
        ] {
            if !agreement
                .capabilities()
                .iter()
                .any(|candidate| candidate.as_str() == capability)
            {
                return Err(ServerConnectionError::MissingCapability { capability });
            }
        }
        if scheduler_limits.negotiated() != agreement.limits() {
            return Err(ServerConnectionError::InconsistentConfiguration {
                rule: "scheduler limits do not match the negotiated connection limits",
            });
        }
        if replay.server_epoch() != agreement.server_epoch() {
            return Err(ServerConnectionError::InconsistentConfiguration {
                rule: "replay journal epoch does not match the negotiated server epoch",
            });
        }
        if replay.limits().max_item_bytes() < u64::from(agreement.limits().max_frame_bytes()) {
            return Err(ServerConnectionError::InconsistentConfiguration {
                rule: "replay item bytes cannot contain one negotiated frame",
            });
        }
        let codec = WireCodec::new(agreement.limits().max_frame_bytes())?;
        let stream_queue_message_limit = u64::try_from(
            scheduler_limits.messages().stream_queue_messages(),
        )
        .map_err(|_| ServerConnectionError::InconsistentConfiguration {
            rule: "stream queue message limit exceeds the diagnostic integer representation",
        })?;
        Ok(Self {
            client_id,
            agreement,
            codec,
            scheduler: OutboundScheduler::with_telemetry(scheduler_limits, telemetry.clone()),
            stream_queue_message_limit,
            telemetry,
            replay,
            diagnostic_limits,
            state: Mutex::new(ConnectionState::default()),
            write_wait: Mutex::new(0),
            write_ready: Condvar::new(),
            #[cfg(test)]
            write_wait_hooks: wake_tests::WaitHooks::default(),
            queue_drain_wake: Mutex::new(None),
        })
    }

    /// Installs the payload-free application wake used after writer dequeue
    /// frees bounded scheduler capacity.
    ///
    /// Replacing the callback is idempotent for a connection-owned pump. This
    /// function performs no external IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns connection-closed failure.
    pub fn set_queue_drain_wake(
        &self,
        wake: Arc<dyn ServerQueueDrainWake>,
    ) -> Result<(), ServerConnectionError> {
        let state = self.lock_state();
        state.ensure_open()?;
        *self
            .queue_drain_wake
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(wake);
        Ok(())
    }

    /// Decodes at most one bounded client frame for this connection.
    ///
    /// This function performs bounded allocation, no transport IO, and does
    /// not panic.
    ///
    /// # Errors
    ///
    /// Returns a payload-redacted binary framing failure.
    pub fn decode_client(
        &self,
        bytes: &[u8],
    ) -> Result<DecodeStatus<ClientMessage>, ServerConnectionError> {
        self.codec.decode_client(bytes).map_err(Into::into)
    }

    /// Accepts one decoded client message and returns only product-level work.
    ///
    /// Heartbeats and flow control are handled immediately. Request and stream
    /// identities are reserved before work is returned.
    ///
    /// # Errors
    ///
    /// Returns closed, active-ID, capacity, capability, flow-credit, or encoding
    /// failure.
    ///
    /// # Panics
    ///
    /// Supplied callbacks may block or panic; see the
    /// [`ServerConnectionSession` callback contract](ServerConnectionSession#callback-contract).
    pub fn accept(
        &self,
        message: ClientMessage,
    ) -> Result<Vec<ServerAction>, ServerConnectionError> {
        let mut state = self.lock_state();
        state.ensure_open()?;
        match message {
            ClientMessage::Request {
                request_id,
                trace_context,
                payload,
            } => {
                state.reserve_request(&self.agreement, request_id.clone(), PendingRequest::Rpc)?;
                self.record_pending_requests(state.pending.len());
                Ok(vec![ServerAction::Request {
                    request_id,
                    trace_context,
                    payload,
                }])
            }
            ClientMessage::CancelRequest { request_id } => {
                let Some(request) = state.pending.remove(&request_id) else {
                    return Ok(Vec::new());
                };
                let request_kind = match request {
                    PendingRequest::Rpc => ServerRequestKind::FiniteRequest,
                    PendingRequest::Stream { stream_id, .. } => {
                        state.streams.remove(&stream_id);
                        ServerRequestKind::StreamOpen
                    }
                };
                self.record_pending_requests(state.pending.len());
                Ok(vec![ServerAction::CancelRequest {
                    request_id,
                    request_kind,
                }])
            }
            ClientMessage::OpenStream {
                request_id,
                stream_id,
                trace_context,
                payload,
            } => {
                state.reserve_stream_request(
                    &self.agreement,
                    request_id.clone(),
                    stream_id.clone(),
                    None,
                )?;
                self.record_pending_requests(state.pending.len());
                Ok(vec![ServerAction::OpenStream {
                    request_id,
                    stream_id,
                    trace_context,
                    payload,
                }])
            }
            ClientMessage::ResumeStream {
                request_id,
                stream_id,
                server_epoch,
                after,
                trace_context,
                payload,
            } => {
                self.require_resume_capability()?;
                state.reserve_stream_request(
                    &self.agreement,
                    request_id.clone(),
                    stream_id.clone(),
                    Some(ResumeRequest {
                        server_epoch: server_epoch.clone(),
                        after,
                    }),
                )?;
                self.record_pending_requests(state.pending.len());
                Ok(vec![ServerAction::ResumeStream {
                    request_id,
                    stream_id,
                    server_epoch,
                    after,
                    trace_context,
                    payload,
                }])
            }
            ClientMessage::CloseStream { stream_id } => {
                let reservation = state.streams.remove(&stream_id).ok_or_else(|| {
                    MultiplexError::UnknownStream {
                        stream_id: stream_id.clone(),
                    }
                })?;
                let mut actions = Vec::new();
                match reservation {
                    StreamReservation::Opening { request_id } => {
                        state.pending.remove(&request_id);
                        self.record_pending_requests(state.pending.len());
                        actions.push(ServerAction::CancelRequest {
                            request_id,
                            request_kind: ServerRequestKind::StreamOpen,
                        });
                    }
                    StreamReservation::Active(_) => {
                        self.scheduler.close_stream(&stream_id)?;
                        self.replay.remove_stream(&ReplayStreamKey::new(
                            self.client_id.clone(),
                            stream_id.clone(),
                        ));
                    }
                }
                actions.push(ServerAction::CloseStream { stream_id });
                Ok(actions)
            }
            ClientMessage::WindowUpdate {
                stream_id,
                additional_bytes,
            } => {
                self.scheduler
                    .grant_consumed_credit(&stream_id, additional_bytes)?;
                self.signal_writer();
                Ok(Vec::new())
            }
            ClientMessage::Ping => {
                self.enqueue_server_control(&ServerMessage::Pong)?;
                Ok(Vec::new())
            }
            ClientMessage::Close => {
                drop(state);
                self.close();
                Ok(vec![ServerAction::CloseConnection])
            }
        }
    }

    /// Completes one finite non-stream request with an encoded product response.
    ///
    /// State is removed only after the reliable response enters the reserved
    /// control queue.
    ///
    /// # Errors
    ///
    /// Returns unknown request, request-kind mismatch, frame encoding, or queue
    /// saturation failure.
    ///
    /// # Panics
    ///
    /// Supplied callbacks may block or panic; see the
    /// [`ServerConnectionSession` callback contract](ServerConnectionSession#callback-contract).
    pub fn complete_request(
        &self,
        request_id: &RequestId,
        payload: Arc<[u8]>,
    ) -> Result<(), ServerConnectionError> {
        self.complete_request_inner(request_id, payload, None)
    }

    /// Completes a response and observes its write or abandonment.
    ///
    /// The signal finishes on queue failure, connection closure, an abandoned
    /// selected write, or successful physical IO. It never requires peer ACK.
    ///
    /// # Errors
    /// Returns the same validation and queue errors as [`Self::complete_request`].
    pub fn complete_request_observed(
        &self,
        request_id: &RequestId,
        payload: Arc<[u8]>,
        completion: Arc<ServerResponseWrite>,
    ) -> Result<(), ServerConnectionError> {
        self.complete_request_inner(request_id, payload, Some(ResponseWriteGuard(completion)))
    }

    fn complete_request_inner(
        &self,
        request_id: &RequestId,
        payload: Arc<[u8]>,
        completion: Option<ResponseWriteGuard>,
    ) -> Result<(), ServerConnectionError> {
        let frame = self.response_frame(request_id, payload)?;
        let mut state = self.lock_state();
        state.ensure_open()?;
        match state.pending.get(request_id) {
            Some(PendingRequest::Rpc) => {}
            Some(PendingRequest::Stream { .. }) => {
                return Err(ServerConnectionError::RequestKindMismatch {
                    request_id: request_id.clone(),
                    expected: "finite_request",
                });
            }
            None => {
                return Err(MultiplexError::UnknownRequest {
                    request_id: request_id.clone(),
                }
                .into());
            }
        }
        self.scheduler.enqueue(frame)?;
        if let Some(completion) = completion {
            state.response_writes.insert(request_id.clone(), completion);
        }
        state.pending.remove(request_id);
        self.record_pending_requests(state.pending.len());
        self.signal_writer();
        Ok(())
    }

    /// Rejects any pending request and queues the product's typed error payload.
    ///
    /// A provisional stream reservation is released only after the response
    /// enters the control queue.
    ///
    /// # Errors
    ///
    /// Returns unknown request, frame encoding, or queue saturation failure.
    ///
    /// # Panics
    ///
    /// Supplied callbacks may block or panic; see the
    /// [`ServerConnectionSession` callback contract](ServerConnectionSession#callback-contract).
    pub fn reject_request(
        &self,
        request_id: &RequestId,
        payload: Arc<[u8]>,
    ) -> Result<(), ServerConnectionError> {
        let frame = self.response_frame(request_id, payload)?;
        let mut state = self.lock_state();
        state.ensure_open()?;
        let request = state.pending.get(request_id).cloned().ok_or_else(|| {
            MultiplexError::UnknownRequest {
                request_id: request_id.clone(),
            }
        })?;
        self.scheduler.enqueue(frame)?;
        state.pending.remove(request_id);
        self.record_pending_requests(state.pending.len());
        if let PendingRequest::Stream { stream_id, .. } = request {
            state.streams.remove(&stream_id);
        }
        self.signal_writer();
        Ok(())
    }

    /// Queues a typed response for a request rejected before pending-work admission.
    ///
    /// The request identity must already have entered the connection-epoch ledger.
    /// This path deliberately does not create or remove pending product state.
    ///
    /// # Errors
    ///
    /// Returns closed, active-request, frame encoding, or queue saturation failure.
    ///
    /// # Panics
    ///
    /// Supplied callbacks may block or panic; see the
    /// [`ServerConnectionSession` callback contract](ServerConnectionSession#callback-contract).
    pub fn reject_unaccepted_request(
        &self,
        request_id: &RequestId,
        payload: Arc<[u8]>,
    ) -> Result<(), ServerConnectionError> {
        let frame = self.response_frame(request_id, payload)?;
        let state = self.lock_state();
        state.ensure_open()?;
        if state.pending.contains_key(request_id) {
            return Err(MultiplexError::RequestIdInUse {
                request_id: request_id.clone(),
            }
            .into());
        }
        if !state.request_ids.contains(request_id) {
            return Err(MultiplexError::UnknownRequest {
                request_id: request_id.clone(),
            }
            .into());
        }
        self.scheduler.enqueue(frame)?;
        drop(state);
        self.signal_writer();
        Ok(())
    }

    /// Activates one reserved product stream and queues its finite open response.
    ///
    /// Resume activation schedules any gap followed by retained frames before
    /// releasing the connection lock; the higher-priority response is selected
    /// first by the writer. Any admission failure rolls back scheduler and newly
    /// allocated replay state.
    ///
    /// # Errors
    ///
    /// Returns request/stream mismatch, capability, replay, encoding, ordering,
    /// capacity, or queue saturation failure.
    ///
    /// # Panics
    ///
    /// Supplied callbacks may block or panic; see the
    /// [`ServerConnectionSession` callback contract](ServerConnectionSession#callback-contract).
    pub fn activate_stream(
        &self,
        request_id: &RequestId,
        stream_id: &StreamId,
        configuration: ServerStreamConfiguration,
        response_payload: Arc<[u8]>,
    ) -> Result<StreamActivationOutcome, ServerConnectionError> {
        let response = self.response_frame(request_id, response_payload)?;
        let mut state = self.lock_state();
        state.ensure_open()?;
        let request = state.pending.get(request_id).cloned().ok_or_else(|| {
            MultiplexError::UnknownRequest {
                request_id: request_id.clone(),
            }
        })?;
        let PendingRequest::Stream {
            stream_id: reserved_stream,
            resume,
        } = request
        else {
            return Err(ServerConnectionError::RequestKindMismatch {
                request_id: request_id.clone(),
                expected: "stream_activation",
            });
        };
        if &reserved_stream != stream_id {
            return Err(ServerConnectionError::StreamRequestMismatch {
                request_id: request_id.clone(),
                stream_id: stream_id.clone(),
            });
        }
        if configuration.retain_for_resume {
            self.require_resume_capability()?;
        }
        if resume.is_some() && !configuration.retain_for_resume {
            return Err(ServerConnectionError::InconsistentConfiguration {
                rule: "a resumed stream must retain reconnect replay",
            });
        }
        let after = resume
            .as_ref()
            .map_or(StreamSequence::INITIAL, |resume| resume.after);
        self.scheduler.open_stream_after(
            stream_id.clone(),
            configuration.kind,
            configuration.recovery,
            after,
        )?;

        let key = ReplayStreamKey::new(self.client_id.clone(), stream_id.clone());
        let mut created_replay = false;
        let mut previous_replay_expiration = None;
        let mut replayed_items = 0;
        let mut gap = None;
        let activation = (|| -> Result<(), ServerConnectionError> {
            self.scheduler.enqueue(response)?;
            if let Some(resume) = &resume {
                let replay = self.replay.activate_resume(
                    &resume.server_epoch,
                    key.clone(),
                    resume.after,
                    configuration.recovery,
                    Instant::now(),
                )?;
                created_replay = replay.created;
                previous_replay_expiration = replay.previous_expiration;
                let (replay_gap, items) = match replay.replay {
                    ReplayResult::Items(items) => (None, items),
                    ReplayResult::Gap(gap) => (Some(gap), Vec::new()),
                    ReplayResult::GapWithItems { gap, items } => (Some(gap), items),
                };
                if let Some(replay_gap) = replay_gap {
                    let encoded =
                        Arc::from(self.codec.encode_server(&ServerMessage::StreamItem {
                            stream_id: stream_id.clone(),
                            item: undercurrent_multiplex::InboundStreamItem::Gap(
                                replay_gap.clone(),
                            ),
                        })?);
                    // A new journal needs this gap recorded after write to
                    // establish its sequence. For an existing journal, a gap
                    // can end at its already-recorded latest sequence, so
                    // recording it again would fail the strict sequence check.
                    // See [write confirmation](../../docs/internals/streams-and-recovery.md#terminal-output-and-the-physical-write-boundary).
                    let disposition = if created_replay {
                        ReplayDisposition::RecordAfterWrite
                    } else {
                        ReplayDisposition::AlreadyRetained
                    };
                    let frame = OutboundFrame::gap(
                        TrafficClass::Control,
                        stream_id.clone(),
                        replay_gap.last_missing,
                        encoded,
                        disposition,
                        self.agreement.limits().max_frame_bytes(),
                    )?;
                    self.scheduler.enqueue(frame)?;
                    gap = Some(replay_gap);
                }
                // Admit the whole suffix before exposing the stream as active.
                // Its final sequence seeds the publisher; any queue failure must
                // roll back the gap, suffix, and response together below.
                replayed_items = items.len();
                for item in items {
                    let frame = match item.kind {
                        StreamFrameKind::Data => OutboundFrame::replayed_stream(
                            configuration.class,
                            stream_id.clone(),
                            item.sequence,
                            item.bytes,
                            self.agreement.limits().max_frame_bytes(),
                        ),
                        StreamFrameKind::Gap => OutboundFrame::gap(
                            configuration.class,
                            stream_id.clone(),
                            item.sequence,
                            item.bytes,
                            ReplayDisposition::AlreadyRetained,
                            self.agreement.limits().max_frame_bytes(),
                        ),
                    }?;
                    self.scheduler.enqueue(frame)?;
                }
            } else if configuration.retain_for_resume {
                self.replay
                    .register_stream(key.clone(), configuration.recovery)?;
                created_replay = true;
            }
            Ok(())
        })();
        if let Err(error) = activation {
            let _ = self.scheduler.close_stream(stream_id);
            let _ = self.scheduler.remove_request_frames(request_id);
            // Failed resume must preserve prior history and its original expiry;
            // it must not grant another retention interval. Only a journal created
            // by this activation can be discarded during rollback.
            // See [activation rollback](../../docs/internals/streams-and-recovery.md#activation-ordering-and-rollback).
            if created_replay {
                self.replay.remove_stream(&key);
            } else if let Some(previous_expiration) = previous_replay_expiration {
                self.replay.deactivate_stream(&key, previous_expiration);
            }
            return Err(error);
        }

        state.pending.remove(request_id);
        self.record_pending_requests(state.pending.len());
        state
            .streams
            .insert(stream_id.clone(), StreamReservation::Active(configuration));
        self.signal_writer();
        let last_sequence = self.scheduler.stream_pressure(stream_id)?.last_sequence;
        if replayed_items > 0 {
            self.telemetry.record_metric(MultiplexMetricObservation {
                name: MultiplexMetricName::ReplayedItems,
                value: replayed_items as f64,
                traffic_class: Some(configuration.class),
            });
        }
        if gap.is_some() {
            self.telemetry.record_metric(MultiplexMetricObservation {
                name: MultiplexMetricName::ReportedGaps,
                value: 1.0,
                traffic_class: Some(TrafficClass::Control),
            });
        }
        Ok(StreamActivationOutcome {
            resumed: resume.is_some(),
            replayed_items,
            last_sequence,
            gap,
        })
    }

    /// Queues one ordered product stream item for its active subscriber only.
    ///
    /// The item enters the stream's configured priority and saturation policy.
    /// Replay retention occurs only after the writer confirms physical IO.
    ///
    ///
    /// # Errors
    ///
    /// Returns unknown/inactive stream, frame encoding, sequence, capacity, or
    /// queue saturation failure.
    ///
    /// # Panics
    ///
    /// Supplied callbacks may block or panic; see the
    /// [`ServerConnectionSession` callback contract](ServerConnectionSession#callback-contract).
    pub fn publish_stream(
        &self,
        stream_id: &StreamId,
        sequence: StreamSequence,
        payload: Arc<[u8]>,
    ) -> Result<EnqueueOutcome, ServerConnectionError> {
        let state = self.lock_state();
        state.ensure_open()?;
        let configuration = match state.streams.get(stream_id) {
            Some(StreamReservation::Active(configuration)) => *configuration,
            Some(StreamReservation::Opening { .. }) | None => {
                return Err(MultiplexError::UnknownStream {
                    stream_id: stream_id.clone(),
                }
                .into());
            }
        };
        let encoded = Arc::from(self.codec.encode_server(&ServerMessage::StreamItem {
            stream_id: stream_id.clone(),
            item: undercurrent_multiplex::InboundStreamItem::Data {
                sequence,
                bytes: payload,
                flow_controlled_bytes: 0,
            },
        })?);
        let frame = OutboundFrame::stream(
            configuration.class,
            stream_id.clone(),
            sequence,
            configuration.delivery,
            encoded,
            self.agreement.limits().max_frame_bytes(),
        )?;
        let outcome = self.scheduler.enqueue(frame)?;
        self.signal_writer();
        Ok(outcome)
    }

    /// Reports one ordered product item that its application source no longer
    /// retains.
    ///
    /// The scheduler advances the stream sequence and queues a typed gap using
    /// the stream's configured recovery action. No placeholder payload is allocated.
    ///
    /// # Errors
    ///
    /// Returns closed, unknown/inactive stream, or non-monotonic sequence
    /// failure.
    ///
    /// # Panics
    ///
    /// Supplied callbacks may block or panic; see the
    /// [`ServerConnectionSession` callback contract](ServerConnectionSession#callback-contract).
    pub fn publish_source_gap(
        &self,
        stream_id: &StreamId,
        sequence: StreamSequence,
    ) -> Result<StreamGap, ServerConnectionError> {
        let state = self.lock_state();
        state.ensure_open()?;
        if !matches!(
            state.streams.get(stream_id),
            Some(StreamReservation::Active(_))
        ) {
            return Err(MultiplexError::UnknownStream {
                stream_id: stream_id.clone(),
            }
            .into());
        }
        let gap = self.scheduler.record_source_gap(stream_id, sequence)?;
        self.signal_writer();
        Ok(gap)
    }

    /// Selects the next weighted-fair binary frame for the physical writer.
    ///
    /// A successful selection removes the frame from bounded connection queues.
    /// Replay is unchanged until [`PendingServerWrite::confirm_written`] is
    /// called after successful IO.
    ///
    /// # Errors
    ///
    /// Returns connection, flow-credit, encoding, or stream invariant failure.
    ///
    /// # Panics
    ///
    /// Supplied callbacks may block or panic; see the
    /// [`ServerConnectionSession` callback contract](ServerConnectionSession#callback-contract).
    pub fn next_write(&self) -> Result<Option<PendingServerWrite>, ServerConnectionError> {
        let mut state = self.lock_state();
        state.ensure_open()?;
        let Some(scheduled) = self.scheduler.dequeue()? else {
            return Ok(None);
        };
        let response_write = match &scheduled {
            ScheduledFrame::Data(frame) => match frame.route() {
                FrameRoute::Request(id) => state.response_writes.remove(id),
                _ => None,
            },
            ScheduledFrame::Gap(_) => None,
        };
        let (bytes, stream_id, sequence, disposition, kind) = match scheduled {
            ScheduledFrame::Data(frame) => {
                let stream_id = match frame.route() {
                    FrameRoute::Stream(stream_id) => Some(stream_id.clone()),
                    FrameRoute::Connection | FrameRoute::Request(_) => None,
                };
                (
                    frame.shared_bytes(),
                    stream_id,
                    frame.sequence(),
                    frame.replay_disposition(),
                    frame.stream_frame_kind(),
                )
            }
            ScheduledFrame::Gap(gap) => {
                let bytes = Arc::from(self.codec.encode_server(&ServerMessage::StreamItem {
                    stream_id: gap.stream_id.clone(),
                    item: undercurrent_multiplex::InboundStreamItem::Gap(gap.clone()),
                })?);
                (
                    bytes,
                    Some(gap.stream_id),
                    Some(gap.last_missing),
                    Some(ReplayDisposition::RecordAfterWrite),
                    StreamFrameKind::Gap,
                )
            }
        };
        let confirmation = match (stream_id, sequence, disposition) {
            (Some(stream_id), Some(sequence), Some(ReplayDisposition::RecordAfterWrite))
                if state.streams.get(&stream_id).is_some_and(|reservation| {
                    matches!(
                        reservation,
                        StreamReservation::Active(ServerStreamConfiguration {
                            retain_for_resume: true,
                            ..
                        })
                    )
                }) =>
            {
                Some(ReplayConfirmation {
                    key: ReplayStreamKey::new(self.client_id.clone(), stream_id),
                    sequence,
                    kind,
                })
            }
            _ => None,
        };
        let write = PendingServerWrite {
            bytes,
            replay: self.replay.clone(),
            confirmation,
            response_write,
        };
        drop(state);
        // Dequeue has already freed scheduler capacity, so backpressured producers
        // can resume before physical IO finishes. Invoke their wake outside the
        // session lock; write confirmation remains a separate replay boundary.
        // See [writer handoff](../../docs/internals/streams-and-recovery.md#terminal-output-and-the-physical-write-boundary).
        self.wake_queue_drain();
        Ok(Some(write))
    }

    /// Blocks until one frame is writable or connection teardown begins.
    ///
    /// Producers wake this boundary only after queue admission or newly granted
    /// flow credit, so an idle physical writer performs no periodic polling.
    /// Exactly one writer should call this method. It performs blocking
    /// synchronization.
    ///
    /// # Errors
    ///
    /// Returns scheduler, encoding, flow-credit, or invariant failures. A
    /// closed connection returns `Ok(None)`.
    ///
    /// # Panics
    ///
    /// Supplied callbacks may block or panic; see the
    /// [`ServerConnectionSession` callback contract](ServerConnectionSession#callback-contract).
    pub fn wait_for_write(&self) -> Result<Option<PendingServerWrite>, ServerConnectionError> {
        loop {
            if self.is_closed() {
                return Ok(None);
            }
            // Snapshot before checking the queue so admission, credit, or close
            // during that check prevents sleeping. Producers can hold state
            // while signaling: never acquire state while holding write_wait.
            let observed_generation = *self
                .write_wait
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match self.next_write() {
                Ok(Some(write)) => return Ok(Some(write)),
                Ok(None) => {}
                Err(_) if self.is_closed() => return Ok(None),
                Err(error) => return Err(error),
            }

            #[cfg(test)]
            self.write_wait_hooks.run(wake_tests::WaitPoint::AfterCheck);

            let guard = self
                .write_wait
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            drop(
                self.write_ready
                    .wait_while(guard, |generation| {
                        let waiting = *generation == observed_generation;
                        #[cfg(test)]
                        if waiting {
                            self.write_wait_hooks.run(wake_tests::WaitPoint::BeforePark);
                        }
                        waiting
                    })
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
            );
        }
    }

    /// Returns a lock-consistent bounded connection pressure snapshot.
    pub fn pressure(&self) -> ServerConnectionPressure {
        let state = self.lock_state();
        ServerConnectionPressure {
            pending_requests: state.pending.len(),
            request_ids_used: state.request_ids.len(),
            request_id_limit: REQUEST_ID_EPOCH_LEDGER_LIMIT,
            logical_streams: state.streams.len(),
            scheduler: self.scheduler.pressure(),
            closed: state.closed,
        }
    }

    /// Reads one bounded, stable stream-ID-ordered diagnostic page.
    ///
    /// `after` is an exclusive seek cursor. The read examines at most one more
    /// stream registry entry than requested and samples scheduler pressure only
    /// for active streams on the returned page. It never reads subscription
    /// specifications or stream payloads and performs no transport IO.
    ///
    /// # Errors
    ///
    /// Returns zero/excessive page-size failure or an internal registry and
    /// scheduler divergence.
    pub fn stream_diagnostic_page(
        &self,
        after: Option<&StreamId>,
        requested_page_size: u32,
    ) -> Result<ServerStreamDiagnosticPage, ServerConnectionError> {
        let page_size = PageSize::new(
            requested_page_size,
            self.diagnostic_limits.stream_page_size(),
        )?
        .get() as usize;
        let state = self.lock_state();
        let mut selected: Vec<_> = match after {
            Some(after) => state
                .streams
                .range((Excluded(after.clone()), Unbounded))
                .take(page_size.saturating_add(1))
                .map(|(stream_id, reservation)| (stream_id.clone(), reservation.clone()))
                .collect(),
            None => state
                .streams
                .iter()
                .take(page_size.saturating_add(1))
                .map(|(stream_id, reservation)| (stream_id.clone(), reservation.clone()))
                .collect(),
        };
        let stream_entries_examined = selected.len();
        let has_more = selected.len() > page_size;
        if has_more {
            selected.pop();
        }
        let next_after = if has_more {
            selected.last().map(|(stream_id, _)| stream_id.clone())
        } else {
            None
        };
        let mut active_streams_sampled = 0;
        let items = selected
            .into_iter()
            .map(|(stream_id, reservation)| {
                let diagnostic_state = match reservation {
                    StreamReservation::Opening { request_id } => {
                        ServerStreamDiagnosticState::Opening { request_id }
                    }
                    StreamReservation::Active(configuration) => {
                        let pressure =
                            self.scheduler.stream_pressure(&stream_id).map_err(|_| {
                                ServerConnectionError::StreamRegistryDiverged {
                                    stream_id: stream_id.clone(),
                                }
                            })?;
                        if pressure.kind != configuration.kind {
                            return Err(ServerConnectionError::StreamRegistryDiverged {
                                stream_id,
                            });
                        }
                        active_streams_sampled += 1;
                        ServerStreamDiagnosticState::Active {
                            configuration,
                            pressure,
                        }
                    }
                };
                Ok(ServerStreamDiagnostic {
                    stream_id,
                    queue_byte_limit: self.agreement.limits().stream_queue_bytes(),
                    queue_message_limit: self.stream_queue_message_limit,
                    state: diagnostic_state,
                })
            })
            .collect::<Result<Vec<_>, ServerConnectionError>>()?;
        let connection = ServerConnectionPressure {
            pending_requests: state.pending.len(),
            request_ids_used: state.request_ids.len(),
            request_id_limit: REQUEST_ID_EPOCH_LEDGER_LIMIT,
            logical_streams: state.streams.len(),
            scheduler: self.scheduler.pressure(),
            closed: state.closed,
        };

        Ok(ServerStreamDiagnosticPage {
            items,
            next_after,
            connection,
            cost: ServerStreamDiagnosticCost {
                stream_entries_examined,
                active_streams_sampled,
            },
        })
    }

    /// Closes connection-local state while retaining configured replay journals.
    ///
    /// Repeated close is idempotent.
    ///
    /// # Panics
    ///
    /// Supplied callbacks may block or panic; see the
    /// [`ServerConnectionSession` callback contract](ServerConnectionSession#callback-contract).
    pub fn close(&self) -> ServerConnectionRelease {
        let mut state = self.lock_state();
        if state.closed {
            return ServerConnectionRelease {
                pending_requests: 0,
                logical_streams: 0,
                queued_bytes: 0,
                queued_messages: 0,
            };
        }
        let pending_requests = state.pending.len();
        let logical_streams = state.streams.len();
        let replay_deadline = self.replay_deadline();
        for (stream_id, reservation) in &state.streams {
            if matches!(
                reservation,
                StreamReservation::Active(ServerStreamConfiguration {
                    retain_for_resume: true,
                    ..
                })
            ) {
                self.replay.deactivate_stream(
                    &ReplayStreamKey::new(self.client_id.clone(), stream_id.clone()),
                    replay_deadline,
                );
            }
        }
        state.closed = true;
        // The writer will recheck closure after this state lock is released.
        // Publish its wake before telemetry callbacks: an unwinding callback
        // must not strand a writer that cleanup still needs to join.
        self.signal_writer();
        state.pending.clear();
        state.response_writes.clear();
        state.request_ids.clear();
        state.request_id_bytes = 0;
        state.streams.clear();
        self.record_pending_requests(0);
        let scheduler = self.scheduler.close();
        ServerConnectionRelease {
            pending_requests,
            logical_streams,
            queued_bytes: scheduler.queued_bytes,
            queued_messages: scheduler.queued_messages,
        }
    }

    /// Returns the stable logical client identity used for replay isolation.
    pub const fn client_id(&self) -> &ClientId {
        &self.client_id
    }

    /// Returns the immutable negotiated physical connection agreement.
    pub const fn agreement(&self) -> &ConnectionAgreement {
        &self.agreement
    }

    /// Records one completed physical socket-write duration.
    ///
    /// Local transport adapters call this after a complete write attempt. The
    /// observation carries no connection identity or payload.
    ///
    /// # Panics
    ///
    /// Supplied callbacks may block or panic; see the
    /// [`ServerConnectionSession` callback contract](ServerConnectionSession#callback-contract).
    pub fn record_connection_write_duration(&self, duration: Duration) {
        self.telemetry.record_metric(MultiplexMetricObservation {
            name: MultiplexMetricName::ConnectionWriteDurationSeconds,
            value: duration.as_secs_f64(),
            traffic_class: None,
        });
    }

    fn response_frame(
        &self,
        request_id: &RequestId,
        payload: Arc<[u8]>,
    ) -> Result<OutboundFrame, ServerConnectionError> {
        let encoded = Arc::from(self.codec.encode_server(&ServerMessage::Response {
            request_id: request_id.clone(),
            payload,
        })?);
        OutboundFrame::response(
            request_id.clone(),
            encoded,
            self.agreement.limits().max_frame_bytes(),
        )
        .map_err(Into::into)
    }

    fn enqueue_server_control(&self, message: &ServerMessage) -> Result<(), ServerConnectionError> {
        let bytes = Arc::from(self.codec.encode_server(message)?);
        let frame = OutboundFrame::control(bytes, self.agreement.limits().max_frame_bytes())?;
        self.scheduler.enqueue(frame)?;
        self.signal_writer();
        Ok(())
    }

    /// Returns whether connection teardown has begun.
    ///
    /// This lock-bounded status read performs no IO and does not panic.
    pub fn is_closed(&self) -> bool {
        self.lock_state().closed
    }

    fn signal_writer(&self) {
        // Updating the generation under the waiter's mutex couples notification
        // to wait enrollment. Keep this scope free of session/scheduler access.
        let mut generation = self
            .write_wait
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *generation = generation.wrapping_add(1);
        self.write_ready.notify_one();
    }

    fn record_pending_requests(&self, pending: usize) {
        self.telemetry.record_metric(MultiplexMetricObservation {
            name: MultiplexMetricName::PendingRequests,
            value: pending as f64,
            traffic_class: None,
        });
    }

    fn wake_queue_drain(&self) {
        let wake = self
            .queue_drain_wake
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(wake) = wake {
            wake.wake();
        }
    }

    fn require_resume_capability(&self) -> Result<(), ServerConnectionError> {
        if self
            .agreement
            .capabilities()
            .iter()
            .any(|candidate| candidate.as_str() == connection_capabilities::STREAM_RESUME)
        {
            Ok(())
        } else {
            Err(ServerConnectionError::MissingCapability {
                capability: connection_capabilities::STREAM_RESUME,
            })
        }
    }

    fn replay_deadline(&self) -> Instant {
        let now = Instant::now();
        let retention =
            Duration::from_millis(self.agreement.resume_retention_millis().unwrap_or_default());
        now.checked_add(retention).unwrap_or(now)
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, ConnectionState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[derive(Debug, Clone)]
enum PendingRequest {
    Rpc,
    Stream {
        stream_id: StreamId,
        resume: Option<ResumeRequest>,
    },
}

#[derive(Debug, Clone)]
struct ResumeRequest {
    server_epoch: ServerEpoch,
    after: StreamSequence,
}

#[derive(Debug, Clone)]
enum StreamReservation {
    Opening { request_id: RequestId },
    Active(ServerStreamConfiguration),
}

#[derive(Default)]
struct ConnectionState {
    closed: bool,
    request_ids: BTreeSet<RequestId>,
    request_id_bytes: usize,
    pending: BTreeMap<RequestId, PendingRequest>,
    response_writes: BTreeMap<RequestId, ResponseWriteGuard>,
    streams: BTreeMap<StreamId, StreamReservation>,
}

impl ConnectionState {
    fn ensure_open(&self) -> Result<(), ServerConnectionError> {
        if self.closed {
            Err(MultiplexError::ConnectionClosed.into())
        } else {
            Ok(())
        }
    }

    fn reserve_request(
        &mut self,
        agreement: &ConnectionAgreement,
        request_id: RequestId,
        request: PendingRequest,
    ) -> Result<(), ServerConnectionError> {
        self.reserve_request_id(request_id.clone())?;
        let limit = agreement.limits().max_pending_requests() as usize;
        if self.pending.len() >= limit {
            return Err(MultiplexError::CapacityExceeded {
                resource: "pending_requests",
                limit,
            }
            .into());
        }
        self.pending.insert(request_id, request);
        Ok(())
    }

    fn reserve_stream_request(
        &mut self,
        agreement: &ConnectionAgreement,
        request_id: RequestId,
        stream_id: StreamId,
        resume: Option<ResumeRequest>,
    ) -> Result<(), ServerConnectionError> {
        self.reserve_request_id(request_id.clone())?;
        if self.streams.contains_key(&stream_id) {
            return Err(MultiplexError::StreamIdInUse { stream_id }.into());
        }
        let request_limit = agreement.limits().max_pending_requests() as usize;
        if self.pending.len() >= request_limit {
            return Err(MultiplexError::CapacityExceeded {
                resource: "pending_requests",
                limit: request_limit,
            }
            .into());
        }
        let stream_limit = agreement.limits().max_logical_streams() as usize;
        if self.streams.len() >= stream_limit {
            return Err(MultiplexError::CapacityExceeded {
                resource: "logical_streams",
                limit: stream_limit,
            }
            .into());
        }
        self.pending.insert(
            request_id.clone(),
            PendingRequest::Stream {
                stream_id: stream_id.clone(),
                resume,
            },
        );
        self.streams
            .insert(stream_id, StreamReservation::Opening { request_id });
        Ok(())
    }

    // Call before pending/stream capacity and stream-ID conflict checks. Rejected
    // work still consumes its ID so a delayed response cannot settle later work
    // reusing that ID. Capacity rejection also needs this ledger entry to queue
    // its correlated response through reject_unaccepted_request.
    // See [request ID lifetime](../../docs/internals/request-lifecycle.md#request-ids-survive-settlement).
    fn reserve_request_id(&mut self, request_id: RequestId) -> Result<(), ServerConnectionError> {
        if self.request_ids.contains(&request_id) {
            return Err(MultiplexError::RequestIdInUse { request_id }.into());
        }
        if self.request_ids.len() >= REQUEST_ID_EPOCH_LEDGER_LIMIT {
            return Err(MultiplexError::CapacityExceeded {
                resource: "request_ids_per_connection_epoch",
                limit: REQUEST_ID_EPOCH_LEDGER_LIMIT,
            }
            .into());
        }
        let bytes = request_id.as_str().len();
        if bytes > REQUEST_ID_EPOCH_LEDGER_BYTES.saturating_sub(self.request_id_bytes) {
            return Err(MultiplexError::CapacityExceeded {
                resource: "request_id_bytes_per_connection_epoch",
                limit: REQUEST_ID_EPOCH_LEDGER_BYTES,
            }
            .into());
        }
        self.request_id_bytes += bytes;
        self.request_ids.insert(request_id);
        Ok(())
    }
}

struct ReplayConfirmation {
    key: ReplayStreamKey,
    sequence: StreamSequence,
    kind: StreamFrameKind,
}

#[cfg(test)]
mod wake_tests;

#[cfg(test)]
mod ledger_tests {
    use super::*;

    #[test]
    fn byte_budget_retains_old_ids_and_rejects_without_partial_admission() {
        let mut state = ConnectionState::default();
        let count = REQUEST_ID_EPOCH_LEDGER_BYTES / 256;
        for i in 0..count {
            state
                .reserve_request_id(RequestId::new(format!("{i:0256}")).unwrap())
                .unwrap();
        }
        assert_eq!(state.request_id_bytes, REQUEST_ID_EPOCH_LEDGER_BYTES);
        assert!(state.request_ids.len() < REQUEST_ID_EPOCH_LEDGER_LIMIT);
        let next = RequestId::new("next").unwrap();
        assert!(matches!(
            state.reserve_request_id(next.clone()),
            Err(ServerConnectionError::Multiplex(
                MultiplexError::CapacityExceeded {
                    resource: "request_id_bytes_per_connection_epoch",
                    ..
                }
            ))
        ));
        assert!(!state.request_ids.contains(&next));
        assert!(matches!(
            state.reserve_request_id(RequestId::new(format!("{:0256}", 0)).unwrap()),
            Err(ServerConnectionError::Multiplex(
                MultiplexError::RequestIdInUse { .. }
            ))
        ));
        assert_eq!(state.request_id_bytes, REQUEST_ID_EPOCH_LEDGER_BYTES);
        ConnectionState::default().reserve_request_id(next).unwrap();
    }

    #[test]
    fn entry_budget_is_independent_of_byte_budget() {
        let mut state = ConnectionState::default();
        for i in 0..REQUEST_ID_EPOCH_LEDGER_LIMIT {
            state
                .reserve_request_id(RequestId::new(format!("{i:x}")).unwrap())
                .unwrap();
        }
        assert!(state.request_id_bytes < REQUEST_ID_EPOCH_LEDGER_BYTES);
        assert!(matches!(
            state.reserve_request_id(RequestId::new("next").unwrap()),
            Err(ServerConnectionError::Multiplex(
                MultiplexError::CapacityExceeded {
                    resource: "request_ids_per_connection_epoch",
                    ..
                }
            ))
        ));
    }
}
