// SPDX-License-Identifier: Apache-2.0

//! Bounded selective reads over retained durable session events.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{Display, Formatter};

use undercurrent_domain::{DomainEventKind, SessionId, TimestampMillis};

use crate::{
    AccessAction, AccessDecision, AccessDenialReason, AccessResource, ApplicationErrorCategory,
    ApplicationOperation, AuthorizationPort, Clock, DiagnosticEventName, DiagnosticLevel,
    DiagnosticRecord, MetricName, MetricObservation, OutboxSequence, PortError, SessionEventReader,
    SessionOutboxRecord, Telemetry,
};

/// Selective interest for retained session events.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionEventFilter {
    sessions: BTreeSet<SessionId>,
    kinds: BTreeSet<DomainEventKind>,
}

impl SessionEventFilter {
    /// Constructs a bounded event filter.
    ///
    /// Empty sets mean all sessions or all event kinds respectively. This
    /// function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed invalid request for zero limits or excessive filters.
    pub fn new(
        sessions: BTreeSet<SessionId>,
        kinds: BTreeSet<DomainEventKind>,
        max_sessions: usize,
        max_kinds: usize,
    ) -> Result<Self, InvalidSessionEventRequest> {
        if max_sessions == 0 {
            return Err(InvalidSessionEventRequest {
                field: "max_filter_sessions",
            });
        }
        if max_kinds == 0 {
            return Err(InvalidSessionEventRequest {
                field: "max_filter_kinds",
            });
        }
        if sessions.len() > max_sessions {
            return Err(InvalidSessionEventRequest {
                field: "filter_sessions",
            });
        }
        if kinds.len() > max_kinds {
            return Err(InvalidSessionEventRequest {
                field: "filter_kinds",
            });
        }
        Ok(Self { sessions, kinds })
    }

    /// Returns explicit session interests; empty means all sessions.
    pub const fn sessions(&self) -> &BTreeSet<SessionId> {
        &self.sessions
    }

    /// Returns explicit event-kind interests; empty means all kinds.
    pub const fn kinds(&self) -> &BTreeSet<DomainEventKind> {
        &self.kinds
    }

    /// Returns whether one record belongs to this interest set.
    pub fn matches(&self, record: &SessionOutboxRecord) -> bool {
        (self.sessions.is_empty() || self.sessions.contains(&record.event.session_id))
            && (self.kinds.is_empty() || self.kinds.contains(&record.event.kind))
    }
}

/// Initial position for one retained session-event read or subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEventStart {
    /// Read oldest retained matching records first.
    Earliest,
    /// Establish a checkpoint at current journal head without replay.
    Live,
    /// Read the most recent matching records up to the request limit.
    Tail,
    /// Continue strictly after one previously consumed global checkpoint.
    Resume { after: OutboxSequence },
}

/// Validated bounded retained event-read request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionEventReadRequest {
    filter: SessionEventFilter,
    start: SessionEventStart,
    limit: usize,
}

impl SessionEventReadRequest {
    /// Constructs one bounded event read.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed invalid request for zero or excessive page size.
    pub fn new(
        filter: SessionEventFilter,
        start: SessionEventStart,
        limit: usize,
        max_limit: usize,
    ) -> Result<Self, InvalidSessionEventRequest> {
        if max_limit == 0 {
            return Err(InvalidSessionEventRequest {
                field: "max_event_page_size",
            });
        }
        if limit == 0 || limit > max_limit {
            return Err(InvalidSessionEventRequest {
                field: "event_page_size",
            });
        }
        Ok(Self {
            filter,
            start,
            limit,
        })
    }

    /// Returns selective session and kind interest.
    pub const fn filter(&self) -> &SessionEventFilter {
        &self.filter
    }

    /// Returns the initial read position.
    pub const fn start(&self) -> SessionEventStart {
        self.start
    }

    /// Returns the maximum records in this batch.
    pub const fn limit(&self) -> usize {
        self.limit
    }
}

/// Invalid event-filter or page bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidSessionEventRequest {
    field: &'static str,
}

impl InvalidSessionEventRequest {
    /// Returns the stable invalid field name.
    pub const fn field(self) -> &'static str {
        self.field
    }
}

impl Display for InvalidSessionEventRequest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "invalid session event request field {}",
            self.field
        )
    }
}

impl Error for InvalidSessionEventRequest {}

/// Explicit loss range when a resume checkpoint predates retained history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionEventGap {
    missing_from: OutboxSequence,
    missing_through: OutboxSequence,
    first_available: OutboxSequence,
}

impl SessionEventGap {
    /// Constructs one exact global journal retention gap.
    ///
    /// Callers should use this only when `missing_through` immediately precedes
    /// `first_available`. The application service validates adapter results.
    pub const fn new(
        missing_from: OutboxSequence,
        missing_through: OutboxSequence,
        first_available: OutboxSequence,
    ) -> Self {
        Self {
            missing_from,
            missing_through,
            first_available,
        }
    }

    /// Returns the first unavailable global event position.
    pub const fn missing_from(self) -> OutboxSequence {
        self.missing_from
    }

    /// Returns the final unavailable global event position.
    pub const fn missing_through(self) -> OutboxSequence {
        self.missing_through
    }

    /// Returns the oldest global event position still retained.
    pub const fn first_available(self) -> OutboxSequence {
        self.first_available
    }
}

/// Bounded extraction work performed by a retained event read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionEventQueryCost {
    /// Candidate sequence-index entries examined.
    pub index_entries_examined: usize,
}

/// One coherent bounded retained session-event batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionEventReadBatch {
    records: Vec<SessionOutboxRecord>,
    checkpoint: Option<OutboxSequence>,
    gap: Option<SessionEventGap>,
    has_more: bool,
    query_cost: SessionEventQueryCost,
}

impl SessionEventReadBatch {
    /// Constructs an adapter batch for application contract validation.
    ///
    /// This function performs no IO and does not panic. The read service rejects
    /// inconsistent ordering, filters, checkpoints, gaps, and limits.
    pub fn new(
        records: Vec<SessionOutboxRecord>,
        checkpoint: Option<OutboxSequence>,
        gap: Option<SessionEventGap>,
        has_more: bool,
        query_cost: SessionEventQueryCost,
    ) -> Self {
        Self {
            records,
            checkpoint,
            gap,
            has_more,
            query_cost,
        }
    }

    /// Returns records in strictly increasing global sequence order.
    pub fn records(&self) -> &[SessionOutboxRecord] {
        &self.records
    }

    /// Returns the global position consumed by this batch.
    pub const fn checkpoint(&self) -> Option<OutboxSequence> {
        self.checkpoint
    }

    /// Returns an explicit retention gap, if resume lost global history.
    pub const fn gap(&self) -> Option<SessionEventGap> {
        self.gap
    }

    /// Returns whether another matching page is already retained.
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    /// Returns bounded index extraction cost.
    pub const fn query_cost(&self) -> SessionEventQueryCost {
        self.query_cost
    }
}

/// Invalid event-reader adapter batch contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEventBatchViolation {
    /// Adapter returned more records than requested.
    LimitExceeded,
    /// Records were not strictly increasing.
    SequenceOrder,
    /// A returned record did not match requested interest.
    FilterMismatch,
    /// A live-only request unexpectedly returned replay records.
    LiveReplay,
    /// Resume returned a record at or before the consumed checkpoint.
    ResumeOrder,
    /// Batch records were not covered by its returned checkpoint.
    InvalidCheckpoint,
    /// `has_more` was asserted without a record checkpoint.
    InvalidContinuation,
    /// Gap range did not correspond to this resume request.
    InvalidGap,
}

impl Display for SessionEventBatchViolation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::LimitExceeded => "session event batch exceeded requested limit",
            Self::SequenceOrder => "session event records are not strictly increasing",
            Self::FilterMismatch => "session event batch contains an unrelated record",
            Self::LiveReplay => "live-only session event read returned replay records",
            Self::ResumeOrder => "session event resume returned an already-consumed record",
            Self::InvalidCheckpoint => "session event checkpoint does not cover returned records",
            Self::InvalidContinuation => "session event continuation has no record checkpoint",
            Self::InvalidGap => "session event retention gap is inconsistent",
        })
    }
}

/// Failure from an authorized retained session-event read.
#[derive(Debug)]
pub enum SessionEventReadError {
    /// No valid authentication was established.
    Unauthenticated {
        /// Explicit session resource when authorization was scoped.
        session_id: Option<SessionId>,
    },
    /// Authenticated caller lacks event-read permission.
    PermissionDenied {
        /// Explicit session resource when authorization was scoped.
        session_id: Option<SessionId>,
    },
    /// Reader returned a batch that violates its port contract.
    InvalidBatch(SessionEventBatchViolation),
    /// Authorization or retained journal infrastructure failed.
    Infrastructure(PortError),
}

impl SessionEventReadError {
    /// Returns the stable presentation category.
    pub const fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::Unauthenticated { .. } => ApplicationErrorCategory::Unauthenticated,
            Self::PermissionDenied { .. } => ApplicationErrorCategory::PermissionDenied,
            Self::InvalidBatch(_) => ApplicationErrorCategory::InvariantViolation,
            Self::Infrastructure(error) => error.category(),
        }
    }

    fn port_error(&self) -> Option<&PortError> {
        match self {
            Self::Infrastructure(error) => Some(error),
            Self::Unauthenticated { .. }
            | Self::PermissionDenied { .. }
            | Self::InvalidBatch(_) => None,
        }
    }
}

impl Display for SessionEventReadError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unauthenticated { session_id } => {
                write_event_access_error(formatter, "authentication is required", session_id)
            }
            Self::PermissionDenied { session_id } => {
                write_event_access_error(formatter, "permission denied", session_id)
            }
            Self::InvalidBatch(violation) => Display::fmt(violation, formatter),
            Self::Infrastructure(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for SessionEventReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidBatch(violation) => Some(violation),
            Self::Infrastructure(error) => Some(error),
            Self::Unauthenticated { .. } | Self::PermissionDenied { .. } => None,
        }
    }
}

impl Error for SessionEventBatchViolation {}

/// Authorizes and validates one bounded selective retained session-event read.
pub struct ReadSessionEventsService<'a> {
    authorizer: &'a dyn AuthorizationPort,
    reader: &'a dyn SessionEventReader,
    clock: &'a dyn Clock,
    telemetry: &'a dyn Telemetry,
}

impl<'a> ReadSessionEventsService<'a> {
    /// Constructs the event-read use case without performing IO.
    pub const fn new(
        authorizer: &'a dyn AuthorizationPort,
        reader: &'a dyn SessionEventReader,
        clock: &'a dyn Clock,
        telemetry: &'a dyn Telemetry,
    ) -> Self {
        Self {
            authorizer,
            reader,
            clock,
            telemetry,
        }
    }

    /// Authorizes every requested session scope and reads one bounded batch.
    ///
    /// Authorization completes before journal access. An empty session filter
    /// authorizes the session collection once; explicit session filters are
    /// authorized independently. This method performs no process, PTY,
    /// detection, health, or WorkContext IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed access, infrastructure, or adapter-contract failures.
    pub async fn read(
        &self,
        request: &SessionEventReadRequest,
    ) -> Result<SessionEventReadBatch, SessionEventReadError> {
        let started_at = self.clock.now();
        let result = async {
            self.authorize(request.filter()).await?;
            let batch = self
                .reader
                .read(request)
                .await
                .map_err(SessionEventReadError::Infrastructure)?;
            validate_session_event_batch(request, &batch)
                .map_err(SessionEventReadError::InvalidBatch)?;
            Ok(batch)
        }
        .await;
        self.finish(started_at, result.as_ref());
        result
    }

    async fn authorize(&self, filter: &SessionEventFilter) -> Result<(), SessionEventReadError> {
        if filter.sessions().is_empty() {
            self.authorize_resource(&AccessResource::Sessions, None)
                .await
        } else {
            for session_id in filter.sessions() {
                self.authorize_resource(
                    &AccessResource::Session(session_id.clone()),
                    Some(session_id),
                )
                .await?;
            }
            Ok(())
        }
    }

    async fn authorize_resource(
        &self,
        resource: &AccessResource,
        session_id: Option<&SessionId>,
    ) -> Result<(), SessionEventReadError> {
        let decision = self
            .authorizer
            .authorize(AccessAction::ReadEvents, resource)
            .await
            .map_err(SessionEventReadError::Infrastructure)?;
        match decision {
            AccessDecision::Permit => Ok(()),
            AccessDecision::Deny(AccessDenialReason::Unauthenticated) => {
                Err(SessionEventReadError::Unauthenticated {
                    session_id: session_id.cloned(),
                })
            }
            AccessDecision::Deny(AccessDenialReason::Forbidden) => {
                Err(SessionEventReadError::PermissionDenied {
                    session_id: session_id.cloned(),
                })
            }
        }
    }

    fn finish(
        &self,
        started_at: TimestampMillis,
        result: Result<&SessionEventReadBatch, &SessionEventReadError>,
    ) {
        let finished_at = self.clock.now();
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::OperationDurationSeconds,
            value: finished_at
                .saturating_duration_since(started_at)
                .as_secs_f64(),
            operation: Some(ApplicationOperation::ReadSessionEvents),
        });
        if let Ok(batch) = result {
            self.telemetry.record_metric(MetricObservation {
                name: MetricName::QueryRowsReturned,
                value: batch.records().len() as f64,
                operation: Some(ApplicationOperation::ReadSessionEvents),
            });
            self.telemetry.record_metric(MetricObservation {
                name: MetricName::QueryIndexEntriesExamined,
                value: batch.query_cost().index_entries_examined as f64,
                operation: Some(ApplicationOperation::ReadSessionEvents),
            });
        }
        let (event, level, application_error, port_operation, error_kind) = match result {
            Ok(_) => (
                DiagnosticEventName::SessionEventsRead,
                DiagnosticLevel::Debug,
                None,
                None,
                None,
            ),
            Err(error) => {
                let category = error.category();
                let event = if category.is_access_denial() {
                    DiagnosticEventName::AuthorizationDenied
                } else {
                    DiagnosticEventName::SessionEventReadFailed
                };
                let level = DiagnosticLevel::for_application_error(category);
                (
                    event,
                    level,
                    Some(category),
                    error.port_error().map(PortError::operation),
                    error.port_error().map(PortError::kind),
                )
            }
        };
        self.telemetry.emit_diagnostic(&DiagnosticRecord {
            event,
            level,
            timestamp: finished_at,
            session_id: None,
            application_operation: Some(ApplicationOperation::ReadSessionEvents),
            application_error,
            operation: port_operation,
            error_kind,
        });
    }
}

/// Validates one event-reader batch against its selective bounded request.
///
/// Adapters that combine atomic replay with a live subscription use this same
/// invariant boundary for both initial and continuation reads. This function
/// performs no IO and does not panic.
///
/// # Errors
///
/// Returns the first ordering, filter, checkpoint, gap, or size violation.
pub fn validate_session_event_batch(
    request: &SessionEventReadRequest,
    batch: &SessionEventReadBatch,
) -> Result<(), SessionEventBatchViolation> {
    if batch.records().len() > request.limit() {
        return Err(SessionEventBatchViolation::LimitExceeded);
    }
    if batch
        .records()
        .windows(2)
        .any(|records| records[0].sequence >= records[1].sequence)
    {
        return Err(SessionEventBatchViolation::SequenceOrder);
    }
    if batch
        .records()
        .iter()
        .any(|record| !request.filter().matches(record))
    {
        return Err(SessionEventBatchViolation::FilterMismatch);
    }
    match request.start() {
        SessionEventStart::Live if !batch.records().is_empty() => {
            return Err(SessionEventBatchViolation::LiveReplay);
        }
        SessionEventStart::Resume { after }
            if batch
                .records()
                .iter()
                .any(|record| record.sequence <= after) =>
        {
            return Err(SessionEventBatchViolation::ResumeOrder);
        }
        SessionEventStart::Earliest
        | SessionEventStart::Live
        | SessionEventStart::Tail
        | SessionEventStart::Resume { .. } => {}
    }
    // These checkpoints use the global journal sequence. Only an exhausted
    // filtered read may advance past its last returned event to skip unrelated
    // entries; doing that with a continuation could skip matching events.
    // See ../docs/internals/recovery/events-and-outbox.md#read-and-resume-retained-events.
    if batch.has_more() {
        let Some(last) = batch.records().last() else {
            return Err(SessionEventBatchViolation::InvalidContinuation);
        };
        if batch.checkpoint() != Some(last.sequence) {
            return Err(SessionEventBatchViolation::InvalidCheckpoint);
        }
    } else if let Some(last) = batch.records().last()
        && batch
            .checkpoint()
            .is_none_or(|checkpoint| checkpoint < last.sequence)
    {
        return Err(SessionEventBatchViolation::InvalidCheckpoint);
    }
    if let SessionEventStart::Resume { after } = request.start()
        && batch
            .checkpoint()
            .is_none_or(|checkpoint| checkpoint < after)
    {
        return Err(SessionEventBatchViolation::InvalidCheckpoint);
    }
    match (request.start(), batch.gap()) {
        (SessionEventStart::Resume { after }, Some(gap)) => {
            if after.checked_next() != Some(gap.missing_from())
                || gap.missing_through().checked_next() != Some(gap.first_available())
                || gap.missing_from() > gap.missing_through()
            {
                return Err(SessionEventBatchViolation::InvalidGap);
            }
        }
        (SessionEventStart::Resume { .. }, None)
        | (SessionEventStart::Earliest, None)
        | (SessionEventStart::Live, None)
        | (SessionEventStart::Tail, None) => {}
        (SessionEventStart::Earliest, Some(_))
        | (SessionEventStart::Live, Some(_))
        | (SessionEventStart::Tail, Some(_)) => {
            return Err(SessionEventBatchViolation::InvalidGap);
        }
    }
    Ok(())
}

fn write_event_access_error(
    formatter: &mut Formatter<'_>,
    prefix: &str,
    session_id: &Option<SessionId>,
) -> std::fmt::Result {
    match session_id {
        Some(session_id) => write!(
            formatter,
            "{prefix} to read events for session {session_id}"
        ),
        None => write!(formatter, "{prefix} to read session events"),
    }
}
