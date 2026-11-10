// SPDX-License-Identifier: Apache-2.0

//! Authorized selective source preparation for the shared product stream pump.

use std::collections::VecDeque;
use std::sync::Arc;

use undercurrent_application::{
    AccessAction, AccessDecision, AccessDenialReason, AccessResource, AgentChannelRepository,
    AgentChannelTarget, AgentEventPollOutcome, Clock, OutputGapRecovery, OutputOffset,
    SessionEventFilter, SessionEventReadBatch, SessionEventReadRequest, SessionEventStart,
    SessionEventSubscriptionPort, SessionQuerySubscription, SessionSubscriptionError,
    TerminalOutputError, TerminalOutputStreamReplay, TerminalOutputStreamSubscription,
    TerminalScreenSnapshot as ApplicationTerminalScreenSnapshot, validate_session_event_batch,
};
use undercurrent_protocol::{
    AgentEventDto, AgentEventsSpec, GetAgentEventRequest, ProductErrorCategory,
    ProductErrorDisposition, ProductStreamError, ProductStreamItem, ProductStreamKind,
    ProductStreamSpec, ResynchronizationDto, SessionEventsSpec, SessionWatchSpec,
    TerminalInitialModeDto, TerminalOutputCodec, TerminalOutputSpec,
    TerminalScreenSnapshot as ProtocolTerminalScreenSnapshot,
};

use super::{ApplicationProductExecutor, error, finite};
use crate::agent_event_notifications::AgentEventNotificationSubscription;
use crate::presentation::TerminalOutputStreamPresentation;
use crate::stream::{
    ProductStreamReservationError, ProductStreamSource, ProductStreamSourceError,
    ProductStreamSourcePoll,
};
use crate::{PreparedProductStream, ProductPresentation, ProductStreamContext, blocking::block_on};

impl ApplicationProductExecutor {
    pub(super) fn prepare_product_stream(
        &self,
        context: &ProductStreamContext,
        specification: ProductStreamSpec,
    ) -> Result<Box<dyn PreparedProductStream>, ProductStreamError> {
        match specification {
            ProductStreamSpec::SessionWatch(specification) => {
                self.prepare_session_watch(context, &specification)
            }
            ProductStreamSpec::SessionEvents(specification) => {
                self.prepare_session_events(context, &specification)
            }
            ProductStreamSpec::TerminalOutput(specification) => {
                self.prepare_terminal_output(context, specification)
            }
            ProductStreamSpec::AgentEvents(specification) => {
                self.prepare_agent_events(context, specification)
            }
        }
    }

    fn prepare_agent_events(
        &self,
        context: &ProductStreamContext,
        specification: AgentEventsSpec,
    ) -> Result<Box<dyn PreparedProductStream>, ProductStreamError> {
        let kind = ProductStreamKind::AgentEvents;
        let session_id = Some(specification.session_id.clone());
        let target = finite::get_agent_target(&GetAgentEventRequest {
            tenant_id: specification.tenant_id,
            source_product: specification.source_product,
            actor_context_ref: specification.actor_context_ref,
            authority_ref: specification.authority_ref,
            undercurrent_provider_id: specification.undercurrent_provider_id,
            undercurrent_provider_generation: specification.undercurrent_provider_generation,
            session_id: specification.session_id,
            attempt_id: specification.attempt_id,
            attempt_revision: specification.attempt_revision,
            recovery_epoch: specification.recovery_epoch,
            fence: specification.fence,
            request_digest_sha256: specification.request_digest_sha256,
            after_sequence: specification.after_sequence,
            maximum_wait_millis: 0,
        })
        .map_err(|source| {
            stream_error(
                kind,
                ProductErrorCategory::InvalidRequest,
                &source.to_string(),
                session_id.clone(),
                false,
                None,
                None,
            )
        })?;
        self.authorize_stream(
            kind,
            AccessAction::ReadEvents,
            &AccessResource::Session(target.session_id.clone()),
            session_id.clone(),
        )?;
        let repository = self
            .ports
            .execution
            .as_ref()
            .and_then(|ports| ports.agent_channel.clone())
            .ok_or_else(|| {
                stream_error(
                    kind,
                    ProductErrorCategory::FailedPrecondition,
                    "agent event subscriptions are unavailable",
                    session_id.clone(),
                    false,
                    None,
                    None,
                )
            })?;
        let reservation = self.reserve_stream(context, kind)?;
        // Register before the initial read. An event committed in between the
        // two operations wakes the reserved stream, and the canonical queue
        // read below still observes it.
        let notification = self
            .agent_event_notifications
            .subscribe(target.attempt_id.clone(), reservation.wake());
        let after_sequence = specification.after_sequence;
        let pending = read_agent_event(
            repository.as_ref(),
            &target,
            after_sequence,
            self.ports.clock.as_ref(),
        )
        .map_err(|source| agent_stream_prepare_error(kind, session_id, source))?;
        Ok(reservation.into_prepared(
            Some(after_sequence),
            Box::new(AgentEventProductStreamSource {
                repository,
                clock: self.ports.clock.clone(),
                target,
                after_sequence,
                pending,
                _notification: notification,
            }),
        ))
    }

    fn prepare_session_watch(
        &self,
        context: &ProductStreamContext,
        specification: &SessionWatchSpec,
    ) -> Result<Box<dyn PreparedProductStream>, ProductStreamError> {
        let kind = ProductStreamKind::SessionWatch;
        let query = self
            .boundary
            .session_watch(specification)
            .map_err(|source| boundary_error(kind, None, source))?;
        self.authorize_stream(
            kind,
            AccessAction::WatchSessions,
            &AccessResource::Sessions,
            None,
        )?;
        let reservation = self.reserve_stream(context, kind)?;
        let subscription = self
            .ports
            .projections
            .subscribe_with_wake(query, reservation.wake())
            .map_err(|source| query_subscription_error(kind, source))?;
        let snapshot = subscription
            .snapshot()
            .map_err(|source| query_subscription_error(kind, source))?;
        let available_from = Some(snapshot.revision.value());
        let initial = ProductStreamItem::SessionSnapshot(
            ProductPresentation::session_watch_snapshot(&snapshot).map_err(|_| {
                stream_error(
                    kind,
                    ProductErrorCategory::Internal,
                    "session watch snapshot could not be represented",
                    None,
                    false,
                    None,
                    None,
                )
            })?,
        );
        Ok(reservation.into_prepared(
            available_from,
            Box::new(QueryProductStreamSource {
                subscription,
                pending: Some(initial),
            }),
        ))
    }

    fn prepare_session_events(
        &self,
        context: &ProductStreamContext,
        specification: &SessionEventsSpec,
    ) -> Result<Box<dyn PreparedProductStream>, ProductStreamError> {
        let kind = ProductStreamKind::SessionEvents;
        let request = self
            .boundary
            .session_event_stream(specification)
            .map_err(|source| boundary_error(kind, None, source))?;
        if request.filter().sessions().is_empty() {
            self.authorize_stream(
                kind,
                AccessAction::ReadEvents,
                &AccessResource::Sessions,
                None,
            )?;
        } else {
            for session_id in request.filter().sessions() {
                self.authorize_stream(
                    kind,
                    AccessAction::ReadEvents,
                    &AccessResource::Session(session_id.clone()),
                    Some(session_id.as_str().to_owned()),
                )?;
            }
        }
        let reservation = self.reserve_stream(context, kind)?;
        let opened = self
            .ports
            .reads
            .events
            .subscriber
            .subscribe(&request, reservation.wake())
            .map_err(|source| port_stream_error(kind, None, &source))?;
        let (initial, subscription) = opened.into_parts();
        validate_session_event_batch(&request, &initial).map_err(|_| {
            stream_error(
                kind,
                ProductErrorCategory::Internal,
                "session event source returned an invalid initial batch",
                None,
                false,
                None,
                None,
            )
        })?;
        let available_from = initial
            .gap()
            .map(|gap| gap.first_available().value())
            .or_else(|| initial.checkpoint().map(|checkpoint| checkpoint.value()));
        let source = EventProductStreamSource::new(request, initial, subscription);
        Ok(reservation.into_prepared(available_from, Box::new(source)))
    }

    fn prepare_terminal_output(
        &self,
        context: &ProductStreamContext,
        specification: TerminalOutputSpec,
    ) -> Result<Box<dyn PreparedProductStream>, ProductStreamError> {
        let kind = ProductStreamKind::TerminalOutput;
        let session_id_text = specification.session_id.clone();
        let (session_id, initial_mode) = self
            .boundary
            .terminal_output_stream(specification)
            .map_err(|source| boundary_error(kind, Some(session_id_text.clone()), source))?;
        self.authorize_stream(
            kind,
            AccessAction::SubscribeTerminal,
            &AccessResource::Session(session_id.clone()),
            Some(session_id_text.clone()),
        )?;
        let status = self
            .ports
            .reads
            .output
            .session_status(&session_id)
            .map_err(|source| terminal_subscription_error(kind, &session_id_text, source))?;
        let next_offset = status.map_or(OutputOffset::ZERO, |status| status.next_offset);
        let (from, max_bytes, recovery, screen) = match initial_mode {
            TerminalInitialModeDto::None => (
                next_offset,
                self.ports.reads.terminal_stream_bytes,
                OutputGapRecovery::RestartAtAvailableOffset,
                None,
            ),
            TerminalInitialModeDto::Tail { max_bytes } => {
                let max_bytes = max_bytes as usize;
                if max_bytes == 0 || max_bytes > self.ports.reads.terminal_stream_bytes {
                    return Err(stream_error(
                        kind,
                        ProductErrorCategory::InvalidRequest,
                        "terminal tail bytes exceed the configured stream chunk limit",
                        Some(session_id_text),
                        false,
                        Some(self.ports.reads.terminal_stream_bytes as u64),
                        None,
                    ));
                }
                (
                    OutputOffset::new(next_offset.value().saturating_sub(max_bytes as u64)),
                    max_bytes,
                    OutputGapRecovery::RestartAtAvailableOffset,
                    None,
                )
            }
            TerminalInitialModeDto::Resume { after_offset } => (
                OutputOffset::new(after_offset),
                self.ports.reads.terminal_stream_bytes,
                OutputGapRecovery::RestartAtAvailableOffset,
                None,
            ),
            TerminalInitialModeDto::Screen => {
                self.authorize_stream(
                    kind,
                    AccessAction::ReadScreen,
                    &AccessResource::Session(session_id.clone()),
                    Some(session_id_text.clone()),
                )?;
                let snapshot = block_on(self.ports.reads.screens.snapshot(&session_id))
                    .map_err(|source| {
                        terminal_screen_initial_error(kind, &session_id_text, &source)
                    })?
                    .ok_or_else(|| {
                        stream_error(
                            kind,
                            ProductErrorCategory::NotFound,
                            "terminal screen state was not found",
                            Some(session_id_text.clone()),
                            false,
                            None,
                            None,
                        )
                    })?;
                if snapshot.contents().len() > self.ports.reads.terminal_stream_bytes {
                    return Err(stream_error(
                        kind,
                        ProductErrorCategory::CapacityExceeded,
                        "terminal screen exceeds the configured stream item limit",
                        Some(session_id_text),
                        false,
                        Some(self.ports.reads.terminal_stream_bytes as u64),
                        None,
                    ));
                }
                (
                    snapshot.output_offset(),
                    self.ports.reads.terminal_stream_bytes,
                    OutputGapRecovery::RequestScreenSnapshot,
                    Some(snapshot),
                )
            }
        };
        let reservation = self.reserve_stream(context, kind)?;
        let opened = self
            .ports
            .reads
            .output
            .subscribe_stream(session_id, from, max_bytes, recovery, reservation.wake())
            .map_err(|source| terminal_subscription_error(kind, &session_id_text, source))?;
        let (initial, subscription) = opened.into_parts();
        let available_from = match &initial {
            TerminalOutputStreamReplay::Gap { available_from, .. } => Some(available_from.value()),
            TerminalOutputStreamReplay::Data { from, .. } => Some(from.value()),
        };
        let terminal_codec = TerminalOutputCodec::new(max_bytes).map_err(|_| {
            stream_error(
                kind,
                ProductErrorCategory::Internal,
                "terminal output stream codec could not be constructed",
                Some(session_id_text),
                false,
                None,
                None,
            )
        })?;
        Ok(reservation.into_prepared(
            available_from,
            Box::new(OutputProductStreamSource::new(
                initial,
                subscription,
                screen,
                terminal_codec,
            )?),
        ))
    }

    fn reserve_stream(
        &self,
        context: &ProductStreamContext,
        kind: ProductStreamKind,
    ) -> Result<crate::stream::ProductStreamReservation, ProductStreamError> {
        self.streams
            .reserve(context.client_id.clone(), context.stream_id.clone())
            .map_err(|source| reservation_error(kind, source))
    }

    fn authorize_stream(
        &self,
        kind: ProductStreamKind,
        action: AccessAction,
        resource: &AccessResource,
        session_id: Option<String>,
    ) -> Result<(), ProductStreamError> {
        match block_on(self.ports.reads.authorizer.authorize(action, resource)) {
            Ok(AccessDecision::Permit) => Ok(()),
            Ok(AccessDecision::Deny(AccessDenialReason::Unauthenticated)) => Err(stream_error(
                kind,
                ProductErrorCategory::Unauthenticated,
                "authentication is required for this logical stream",
                session_id,
                false,
                None,
                None,
            )),
            Ok(AccessDecision::Deny(AccessDenialReason::Forbidden)) => Err(stream_error(
                kind,
                ProductErrorCategory::PermissionDenied,
                "permission denied for this logical stream",
                session_id,
                false,
                None,
                None,
            )),
            Err(source) => Err(port_stream_error(kind, session_id, &source)),
        }
    }
}

struct QueryProductStreamSource {
    subscription: SessionQuerySubscription,
    pending: Option<ProductStreamItem>,
}

impl ProductStreamSource for QueryProductStreamSource {
    fn publish_next(
        &mut self,
        publisher: &crate::ProductStreamPublisher,
    ) -> Result<ProductStreamSourcePoll, ProductStreamSourceError> {
        if self.pending.is_none() {
            let Some(batch) = self
                .subscription
                .try_recv()
                .map_err(|_| ProductStreamSourceError::Source)?
            else {
                return Ok(ProductStreamSourcePoll::Idle);
            };
            self.pending = Some(ProductStreamItem::SessionDeltaBatch(
                ProductPresentation::session_delta_batch(&batch)
                    .map_err(|_| ProductStreamSourceError::Presentation)?,
            ));
        }
        let item = self
            .pending
            .as_ref()
            .ok_or(ProductStreamSourceError::Source)?;
        publish(publisher.publish(item))?;
        self.pending = None;
        Ok(ProductStreamSourcePoll::More)
    }
}

struct EventProductStreamSource {
    filter: SessionEventFilter,
    limit: usize,
    checkpoint: Option<undercurrent_application::OutboxSequence>,
    pending_gap: bool,
    pending: VecDeque<ProductStreamItem>,
    poll_again: bool,
    subscription: Box<dyn SessionEventSubscriptionPort>,
}

enum AgentEventReadError {
    Source,
    Retained,
    StaleTarget,
    BindingExpired,
    NotFound,
}

fn read_agent_event(
    repository: &dyn AgentChannelRepository,
    target: &AgentChannelTarget,
    after_sequence: u64,
    clock: &dyn Clock,
) -> Result<Option<AgentEventDto>, AgentEventReadError> {
    match block_on(repository.poll_agent_event(target, after_sequence, clock.now()))
        .map_err(|_| AgentEventReadError::Source)?
    {
        AgentEventPollOutcome::Event(event) => finite::agent_event_dto(*event)
            .map(Some)
            .map_err(|_| AgentEventReadError::Retained),
        AgentEventPollOutcome::None { .. } => Ok(None),
        AgentEventPollOutcome::StaleTarget => Err(AgentEventReadError::StaleTarget),
        AgentEventPollOutcome::BindingExpired => Err(AgentEventReadError::BindingExpired),
        AgentEventPollOutcome::NotFound => Err(AgentEventReadError::NotFound),
    }
}

struct AgentEventProductStreamSource {
    repository: Arc<dyn AgentChannelRepository>,
    clock: Arc<dyn Clock>,
    target: AgentChannelTarget,
    after_sequence: u64,
    pending: Option<AgentEventDto>,
    _notification: AgentEventNotificationSubscription,
}

impl ProductStreamSource for AgentEventProductStreamSource {
    fn publish_next(
        &mut self,
        publisher: &crate::ProductStreamPublisher,
    ) -> Result<ProductStreamSourcePoll, ProductStreamSourceError> {
        if self.pending.is_none() {
            self.pending = read_agent_event(
                self.repository.as_ref(),
                &self.target,
                self.after_sequence,
                self.clock.as_ref(),
            )
            .map_err(|_| ProductStreamSourceError::Source)?;
        }
        let Some(event) = self.pending.as_ref() else {
            return Ok(ProductStreamSourcePoll::Idle);
        };
        publish(publisher.publish(&ProductStreamItem::AgentEvent(Box::new(event.clone()))))?;
        self.after_sequence = event.sequence;
        self.pending = None;
        Ok(ProductStreamSourcePoll::More)
    }
}

impl EventProductStreamSource {
    fn new(
        request: SessionEventReadRequest,
        initial: SessionEventReadBatch,
        subscription: Box<dyn SessionEventSubscriptionPort>,
    ) -> Self {
        let mut source = Self {
            filter: request.filter().clone(),
            limit: request.limit(),
            checkpoint: None,
            pending_gap: false,
            pending: VecDeque::new(),
            poll_again: false,
            subscription,
        };
        source.load(initial);
        source
    }

    fn load(&mut self, batch: SessionEventReadBatch) {
        self.checkpoint = batch.checkpoint();
        self.pending_gap = batch.gap().is_some();
        self.poll_again = batch.has_more();
        self.pending.extend(
            ProductPresentation::session_events(&batch)
                .items
                .into_iter()
                .map(ProductStreamItem::SessionEvent),
        );
    }

    fn read_continuation(&mut self) -> Result<(), ProductStreamSourceError> {
        let Some(batch) = self
            .subscription
            .try_receive()
            .map_err(|_| ProductStreamSourceError::Source)?
        else {
            self.poll_again = false;
            return Ok(());
        };
        let start = self
            .checkpoint
            .map_or(SessionEventStart::Earliest, |after| {
                SessionEventStart::Resume { after }
            });
        let request =
            SessionEventReadRequest::new(self.filter.clone(), start, self.limit, self.limit)
                .map_err(|_| ProductStreamSourceError::Source)?;
        validate_session_event_batch(&request, &batch)
            .map_err(|_| ProductStreamSourceError::Source)?;
        self.load(batch);
        Ok(())
    }
}

impl ProductStreamSource for EventProductStreamSource {
    fn publish_next(
        &mut self,
        publisher: &crate::ProductStreamPublisher,
    ) -> Result<ProductStreamSourcePoll, ProductStreamSourceError> {
        if !self.pending_gap && self.pending.is_empty() {
            self.read_continuation()?;
        }
        if self.pending_gap {
            publish(publisher.publish_source_gap())?;
            self.pending_gap = false;
            return Ok(ProductStreamSourcePoll::More);
        } else if let Some(item) = self.pending.front() {
            publish(publisher.publish(item))?;
            self.pending.pop_front();
            return Ok(ProductStreamSourcePoll::More);
        }
        Ok(if self.poll_again {
            ProductStreamSourcePoll::More
        } else {
            ProductStreamSourcePoll::Idle
        })
    }
}

enum OutputPending {
    Gap,
    Data(Arc<[u8]>),
    Screen(ProtocolTerminalScreenSnapshot),
}

struct OutputProductStreamSource {
    pending: VecDeque<OutputPending>,
    poll_again: bool,
    subscription: TerminalOutputStreamSubscription,
    terminal_codec: TerminalOutputCodec,
}

impl OutputProductStreamSource {
    fn new(
        initial: TerminalOutputStreamReplay,
        subscription: TerminalOutputStreamSubscription,
        screen: Option<ApplicationTerminalScreenSnapshot>,
        terminal_codec: TerminalOutputCodec,
    ) -> Result<Self, ProductStreamError> {
        let mut source = Self {
            pending: VecDeque::new(),
            poll_again: false,
            subscription,
            terminal_codec,
        };
        if let Some(screen) = screen {
            source.pending.push_back(OutputPending::Screen(
                ProductPresentation::terminal_screen_stream(&screen).map_err(|_| {
                    stream_error(
                        ProductStreamKind::TerminalOutput,
                        ProductErrorCategory::Internal,
                        "terminal screen could not be represented",
                        None,
                        false,
                        None,
                        None,
                    )
                })?,
            ));
        }
        source.load(initial).map_err(|_| {
            stream_error(
                ProductStreamKind::TerminalOutput,
                ProductErrorCategory::Internal,
                "terminal output replay could not be represented",
                None,
                false,
                None,
                None,
            )
        })?;
        Ok(source)
    }

    fn load(&mut self, replay: TerminalOutputStreamReplay) -> Result<(), ProductStreamSourceError> {
        let page = ProductPresentation::terminal_output_stream(&replay, self.terminal_codec)
            .map_err(|_| ProductStreamSourceError::Presentation)?;
        match page {
            TerminalOutputStreamPresentation::Gap => {
                self.pending.push_back(OutputPending::Gap);
                self.poll_again = true;
            }
            TerminalOutputStreamPresentation::Data { payload, has_more } => {
                if let Some(payload) = payload {
                    self.pending.push_back(OutputPending::Data(payload));
                }
                self.poll_again = has_more;
            }
        }
        Ok(())
    }
}

impl ProductStreamSource for OutputProductStreamSource {
    fn publish_next(
        &mut self,
        publisher: &crate::ProductStreamPublisher,
    ) -> Result<ProductStreamSourcePoll, ProductStreamSourceError> {
        if self.pending.is_empty() {
            let Some(replay) = self
                .subscription
                .try_receive()
                .map_err(|_| ProductStreamSourceError::Source)?
            else {
                self.poll_again = false;
                return Ok(ProductStreamSourcePoll::Idle);
            };
            self.load(replay)?;
        }
        if let Some(pending) = self.pending.front() {
            match pending {
                OutputPending::Gap => {
                    publish(publisher.publish_source_gap())?;
                }
                OutputPending::Data(payload) => {
                    publish(publisher.publish_encoded_terminal(Arc::clone(payload)))?;
                }
                OutputPending::Screen(snapshot) => {
                    publish(publisher.publish_terminal_screen(snapshot))?;
                }
            }
            self.pending.pop_front();
            return Ok(ProductStreamSourcePoll::More);
        }
        Ok(if self.poll_again {
            ProductStreamSourcePoll::More
        } else {
            ProductStreamSourcePoll::Idle
        })
    }
}

fn publish<T>(
    result: Result<T, crate::ProductStreamPublishError>,
) -> Result<T, ProductStreamSourceError> {
    result.map_err(|error| {
        if error.is_backpressured() {
            ProductStreamSourceError::Backpressured
        } else {
            ProductStreamSourceError::Publication
        }
    })
}

fn agent_stream_prepare_error(
    kind: ProductStreamKind,
    session_id: Option<String>,
    source: AgentEventReadError,
) -> ProductStreamError {
    let (category, message, retryable) = match source {
        AgentEventReadError::Source => (
            ProductErrorCategory::Internal,
            "agent event subscription source failed internally",
            true,
        ),
        AgentEventReadError::Retained => (
            ProductErrorCategory::Internal,
            "retained agent event could not be represented",
            false,
        ),
        AgentEventReadError::StaleTarget => (
            ProductErrorCategory::FailedPrecondition,
            "agent event subscription target is stale",
            false,
        ),
        AgentEventReadError::BindingExpired => (
            ProductErrorCategory::FailedPrecondition,
            "agent event subscription binding has expired",
            false,
        ),
        AgentEventReadError::NotFound => (
            ProductErrorCategory::NotFound,
            "agent event subscription target was not found",
            false,
        ),
    };
    stream_error(kind, category, message, session_id, retryable, None, None)
}

fn boundary_error(
    kind: ProductStreamKind,
    session_id: Option<String>,
    source: crate::ProductBoundaryError,
) -> ProductStreamError {
    let category = source.category();
    let limit = source.limit();
    stream_error(
        kind,
        category,
        &source.to_string(),
        session_id,
        false,
        limit,
        None,
    )
}

fn reservation_error(
    kind: ProductStreamKind,
    source: ProductStreamReservationError,
) -> ProductStreamError {
    match source {
        ProductStreamReservationError::Closed => stream_error(
            kind,
            ProductErrorCategory::Internal,
            "logical stream service is shutting down",
            None,
            true,
            None,
            None,
        ),
        ProductStreamReservationError::Capacity { limit } => stream_error(
            kind,
            ProductErrorCategory::CapacityExceeded,
            "logical stream capacity is exhausted",
            None,
            true,
            Some(limit as u64),
            None,
        ),
        ProductStreamReservationError::IdentityInUse => stream_error(
            kind,
            ProductErrorCategory::FailedPrecondition,
            "logical stream identity is already active",
            None,
            false,
            None,
            None,
        ),
        ProductStreamReservationError::IdentityExhausted => stream_error(
            kind,
            ProductErrorCategory::Internal,
            "logical stream identity allocation failed",
            None,
            false,
            None,
            None,
        ),
    }
}

fn query_subscription_error(
    kind: ProductStreamKind,
    source: SessionSubscriptionError,
) -> ProductStreamError {
    match source {
        SessionSubscriptionError::CapacityExceeded { limit } => stream_error(
            kind,
            ProductErrorCategory::CapacityExceeded,
            "session watch capacity is exhausted",
            None,
            true,
            Some(limit as u64),
            None,
        ),
        SessionSubscriptionError::Catalog(error) => stream_error(
            kind,
            error::product_category_for_error(error.category(), &error),
            &error.to_string(),
            None,
            false,
            None,
            None,
        ),
        SessionSubscriptionError::InvalidLimit
        | SessionSubscriptionError::IdentityExhausted
        | SessionSubscriptionError::Closed
        | SessionSubscriptionError::Timeout => stream_error(
            kind,
            ProductErrorCategory::Internal,
            "session watch preparation failed internally",
            None,
            false,
            None,
            None,
        ),
    }
}

fn terminal_subscription_error(
    kind: ProductStreamKind,
    session_id: &str,
    source: TerminalOutputError,
) -> ProductStreamError {
    let (category, retryable, limit) = match &source {
        TerminalOutputError::SubscriptionCapacity { limit } => (
            ProductErrorCategory::CapacityExceeded,
            true,
            Some(*limit as u64),
        ),
        TerminalOutputError::ReplayCapacity { limit }
        | TerminalOutputError::DiagnosticCapacity { limit } => (
            ProductErrorCategory::InvalidRequest,
            false,
            Some(*limit as u64),
        ),
        TerminalOutputError::FutureOffset { .. } => {
            (ProductErrorCategory::InvalidRequest, false, None)
        }
        TerminalOutputError::InvalidLimits { .. }
        | TerminalOutputError::SessionCapacity { .. }
        | TerminalOutputError::AppendCapacity { .. }
        | TerminalOutputError::OffsetExhausted
        | TerminalOutputError::CheckpointMismatch { .. }
        | TerminalOutputError::CaptureModeMismatch { .. }
        | TerminalOutputError::ChunkSequenceExhausted
        | TerminalOutputError::SubscriptionIdentityExhausted
        | TerminalOutputError::SubscriptionClosed
        | TerminalOutputError::LockPoisoned
        | TerminalOutputError::InvariantViolation { .. } => {
            (ProductErrorCategory::Internal, false, None)
        }
    };
    let message = if category == ProductErrorCategory::Internal {
        "terminal output subscription failed internally".to_owned()
    } else {
        source.to_string()
    };
    stream_error(
        kind,
        category,
        &message,
        Some(session_id.to_owned()),
        retryable,
        limit,
        None,
    )
}

fn terminal_screen_initial_error(
    kind: ProductStreamKind,
    session_id: &str,
    source: &undercurrent_application::PortError,
) -> ProductStreamError {
    if source.kind() == undercurrent_application::PortErrorKind::Unsupported {
        stream_error(
            kind,
            ProductErrorCategory::FailedPrecondition,
            "terminal screen is unavailable for this session's output capture mode",
            Some(session_id.to_owned()),
            false,
            None,
            None,
        )
    } else {
        port_stream_error(kind, Some(session_id.to_owned()), source)
    }
}

fn port_stream_error(
    kind: ProductStreamKind,
    session_id: Option<String>,
    source: &undercurrent_application::PortError,
) -> ProductStreamError {
    let category = error::product_category_for_error(source.category(), source);
    let message = if category == ProductErrorCategory::Internal {
        "logical stream source failed internally".to_owned()
    } else {
        source.to_string()
    };
    stream_error(
        kind,
        category,
        &message,
        session_id,
        matches!(
            source.category(),
            undercurrent_application::ApplicationErrorCategory::RateLimited
                | undercurrent_application::ApplicationErrorCategory::Timeout
                | undercurrent_application::ApplicationErrorCategory::Interrupted
                | undercurrent_application::ApplicationErrorCategory::Unavailable
        ),
        None,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn stream_error(
    kind: ProductStreamKind,
    category: ProductErrorCategory,
    message: &str,
    session_id: Option<String>,
    retryable: bool,
    limit: Option<u64>,
    resynchronization: Option<ResynchronizationDto>,
) -> ProductStreamError {
    ProductStreamError {
        kind: Some(kind),
        category,
        disposition: Some(ProductErrorDisposition::default_for_category(category)),
        message: message.to_owned(),
        session_id,
        retryable,
        retry_after_millis: None,
        limit,
        resynchronization,
    }
}
