// SPDX-License-Identifier: Apache-2.0

//! Ordered transactional outbox publication and session-local activation.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::time::Duration;

use undercurrent_domain::{DomainEventKind, LifecycleState, Revision, Session, TimestampMillis};

use crate::{
    ApplicationErrorCategory, ApplicationOperation, Clock, DiagnosticEventName, DiagnosticLevel,
    DiagnosticRecord, MetricName, MetricObservation, ObservationScheduler,
    OutboxAcknowledgeOutcome, OutboxSequence, PortError, PortErrorKind, PortOperation,
    SessionEventPublishOutcome, SessionEventPublisher, SessionObservationPlan, SessionOutbox,
    SessionOutboxRecord, SessionRepository, Telemetry,
};

/// Validated memory and cadence bounds for one outbox dispatch worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboxDispatchLimits {
    batch_size: usize,
    foreground_delay: Duration,
    health_delay: Duration,
}

impl OutboxDispatchLimits {
    /// Validates one outbox batch and observation-activation cadence.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns the stable name of a zero batch or delay field.
    pub const fn new(
        batch_size: usize,
        foreground_delay: Duration,
        health_delay: Duration,
    ) -> Result<Self, InvalidOutboxDispatchLimits> {
        if batch_size == 0 {
            return Err(InvalidOutboxDispatchLimits {
                field: "batch_size",
            });
        }
        if foreground_delay.is_zero() {
            return Err(InvalidOutboxDispatchLimits {
                field: "foreground_delay",
            });
        }
        if health_delay.is_zero() {
            return Err(InvalidOutboxDispatchLimits {
                field: "health_delay",
            });
        }
        Ok(Self {
            batch_size,
            foreground_delay,
            health_delay,
        })
    }

    /// Returns the maximum records read and retained in one cycle.
    pub const fn batch_size(self) -> usize {
        self.batch_size
    }

    /// Returns the configured foreground activation delay.
    pub const fn foreground_delay(self) -> Duration {
        self.foreground_delay
    }

    /// Returns the configured health activation delay.
    pub const fn health_delay(self) -> Duration {
        self.health_delay
    }
}

/// Validation failure for outbox worker limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidOutboxDispatchLimits {
    field: &'static str,
}

impl InvalidOutboxDispatchLimits {
    /// Returns the stable invalid field name.
    pub const fn field(self) -> &'static str {
        self.field
    }
}

impl Display for InvalidOutboxDispatchLimits {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "outbox dispatch field {} must be non-zero",
            self.field
        )
    }
}

impl Error for InvalidOutboxDispatchLimits {}

/// Invalid ordered batch returned by an outbox adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxBatchViolation {
    /// Adapter returned more records than requested.
    LimitExceeded,
    /// Record positions were not strictly increasing.
    SequenceOrder,
    /// Consecutive pending records contained a sequence gap.
    SequenceGap,
}

impl Display for OutboxBatchViolation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::LimitExceeded => "outbox batch exceeded its requested limit",
            Self::SequenceOrder => "outbox batch positions were not strictly increasing",
            Self::SequenceGap => "outbox batch positions were not contiguous",
        })
    }
}

/// Stable stage at which one ordered outbox record stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxRecordStage {
    /// Publish the durable event to retained and live consumers.
    PublishEvent,
    /// Load current durable session state for schedule derivation.
    LoadSession,
    /// Atomically apply the current session's observation plan.
    ReconcileObservations,
    /// Cancel observation state for a removed session.
    CancelObservations,
    /// Validate that loaded durable state includes the published revision.
    ValidateSessionRevision,
}

impl OutboxRecordStage {
    /// Complete outbox record-stage registry in stable declaration order.
    pub const ALL: [Self; 5] = [
        Self::PublishEvent,
        Self::LoadSession,
        Self::ReconcileObservations,
        Self::CancelObservations,
        Self::ValidateSessionRevision,
    ];

    /// Returns the stable snake-case stage name used by presentation adapters.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PublishEvent => "publish_event",
            Self::LoadSession => "load_session",
            Self::ReconcileObservations => "reconcile_observations",
            Self::CancelObservations => "cancel_observations",
            Self::ValidateSessionRevision => "validate_session_revision",
        }
    }
}

/// Failure while processing one exact outbox record.
#[derive(Debug)]
pub enum OutboxRecordFailure {
    /// Concrete adapter boundary failed.
    Infrastructure(PortError),
    /// Repository returned an aggregate older than its committed outbox event.
    RepositoryRevisionBehind {
        /// Revision represented by the durable event.
        event_revision: Revision,
        /// Revision returned by the current repository read.
        session_revision: Revision,
    },
}

/// Failure while acknowledging an already processed outbox prefix.
#[derive(Debug)]
pub enum OutboxAcknowledgementFailure {
    /// Concrete acknowledgement boundary failed.
    Infrastructure(PortError),
    /// Adapter returned a result inconsistent with the processed prefix.
    InvalidOutcome(OutboxAcknowledgeOutcome),
}

/// Aggregate result of one bounded ordered outbox cycle.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutboxDispatchReport {
    /// Pending records read from the source.
    pub read: usize,
    /// Records published for the first time.
    pub published: usize,
    /// Exact records deduplicated by the publication boundary.
    pub deduplicated: usize,
    /// Records whose publication and required observation action completed.
    pub processed: usize,
    /// Current session observation plans applied.
    pub observation_plans: usize,
    /// Events that did not invalidate their session's observation plan.
    pub observation_plans_skipped: usize,
    /// Removed or already-absent sessions whose jobs were cancelled.
    pub observation_cancellations: usize,
    /// Processed records removed from the transactional outbox.
    pub acknowledged: usize,
    /// Latest durable acknowledgement checkpoint from this cycle.
    pub last_acknowledged: Option<OutboxSequence>,
}

/// Fatal ordered outbox dispatch failure with bounded partial progress.
#[derive(Debug)]
pub enum OutboxDispatchError {
    /// Pending records could not be read.
    Read(PortError),
    /// Source returned an invalid ordered batch contract.
    InvalidBatch(OutboxBatchViolation),
    /// One record failed; any completed prefix was acknowledged when possible.
    Record {
        /// Exact record that did not complete.
        sequence: OutboxSequence,
        /// Stable failed stage.
        stage: OutboxRecordStage,
        /// Primary record failure.
        failure: OutboxRecordFailure,
        /// Secondary prefix-acknowledgement failure, if one also occurred.
        acknowledgement_failure: Option<OutboxAcknowledgementFailure>,
        /// Bounded progress before the failure.
        partial: Box<OutboxDispatchReport>,
    },
    /// A complete processed batch could not be acknowledged consistently.
    Acknowledge {
        /// Requested final processed checkpoint.
        through: OutboxSequence,
        /// Concrete or contract acknowledgement failure.
        failure: OutboxAcknowledgementFailure,
        /// Bounded progress before acknowledgement.
        partial: Box<OutboxDispatchReport>,
    },
}

impl OutboxDispatchError {
    /// Returns the stable presentation category for this infrastructure path.
    pub const fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::InvalidBatch(_) => ApplicationErrorCategory::InvariantViolation,
            Self::Record {
                failure: OutboxRecordFailure::RepositoryRevisionBehind { .. },
                ..
            }
            | Self::Acknowledge {
                failure: OutboxAcknowledgementFailure::InvalidOutcome(_),
                ..
            }
            | Self::Record {
                acknowledgement_failure: Some(OutboxAcknowledgementFailure::InvalidOutcome(_)),
                ..
            } => ApplicationErrorCategory::InvariantViolation,
            Self::Read(error) => error.category(),
            Self::Record {
                failure: OutboxRecordFailure::Infrastructure(error),
                ..
            } => error.category(),
            Self::Acknowledge {
                failure: OutboxAcknowledgementFailure::Infrastructure(error),
                ..
            } => error.category(),
        }
    }

    /// Returns bounded progress retained at a record or acknowledgement error.
    pub const fn partial_report(&self) -> Option<&OutboxDispatchReport> {
        match self {
            Self::Record { partial, .. } | Self::Acknowledge { partial, .. } => Some(partial),
            Self::Read(_) | Self::InvalidBatch(_) => None,
        }
    }
}

impl Display for OutboxDispatchError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read(error) => write!(formatter, "could not read pending outbox: {error}"),
            Self::InvalidBatch(violation) => Display::fmt(violation, formatter),
            Self::Record {
                sequence, stage, ..
            } => write!(
                formatter,
                "outbox record {} failed at {}",
                sequence.value(),
                stage.as_str()
            ),
            Self::Acknowledge { through, .. } => write!(
                formatter,
                "could not acknowledge outbox through {}",
                through.value()
            ),
        }
    }
}

impl Error for OutboxDispatchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Read(error) => Some(error),
            Self::Record {
                failure: OutboxRecordFailure::Infrastructure(error),
                ..
            } => Some(error),
            Self::Acknowledge {
                failure: OutboxAcknowledgementFailure::Infrastructure(error),
                ..
            } => Some(error),
            Self::InvalidBatch(_)
            | Self::Record {
                failure: OutboxRecordFailure::RepositoryRevisionBehind { .. },
                ..
            }
            | Self::Acknowledge {
                failure: OutboxAcknowledgementFailure::InvalidOutcome(_),
                ..
            } => None,
        }
    }
}

/// Publishes and activates one bounded contiguous transactional outbox prefix.
pub struct DispatchOutboxService<'a> {
    clock: &'a dyn Clock,
    outbox: &'a dyn SessionOutbox,
    repository: &'a dyn SessionRepository,
    publisher: &'a dyn SessionEventPublisher,
    observations: &'a dyn ObservationScheduler,
    telemetry: &'a dyn Telemetry,
    limits: OutboxDispatchLimits,
}

impl<'a> DispatchOutboxService<'a> {
    /// Composes one ordered outbox worker from explicit inward-facing ports.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        clock: &'a dyn Clock,
        outbox: &'a dyn SessionOutbox,
        repository: &'a dyn SessionRepository,
        publisher: &'a dyn SessionEventPublisher,
        observations: &'a dyn ObservationScheduler,
        telemetry: &'a dyn Telemetry,
        limits: OutboxDispatchLimits,
    ) -> Self {
        Self {
            clock,
            outbox,
            repository,
            publisher,
            observations,
            telemetry,
            limits,
        }
    }

    /// Processes one bounded contiguous prefix in durable sequence order.
    ///
    /// Publication is idempotent and occurs before observation activation.
    /// Records are acknowledged only after both complete. On one record failure,
    /// the successfully processed prefix is acknowledged and later records are
    /// untouched. No global session scan occurs and no application lock is held
    /// across adapter IO. This method may emit telemetry and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed read, batch-contract, record, or acknowledgement failure
    /// with bounded partial progress where applicable.
    pub async fn execute(&self) -> Result<OutboxDispatchReport, OutboxDispatchError> {
        let started_at = self.clock.now();
        self.emit(
            DiagnosticEventName::OutboxDispatchStarted,
            DiagnosticLevel::Debug,
            started_at,
            DispatchDiagnosticContext::default(),
        );
        let records = self
            .outbox
            .read_pending(self.limits.batch_size())
            .await
            .map_err(|error| {
                self.emit_port_failure(&error);
                OutboxDispatchError::Read(error)
            })?;
        validate_batch(&records, self.limits.batch_size()).map_err(|violation| {
            self.emit_invariant_failure();
            OutboxDispatchError::InvalidBatch(violation)
        })?;
        let mut report = OutboxDispatchReport {
            read: records.len(),
            ..OutboxDispatchReport::default()
        };
        let mut completed_through = None;
        for record in &records {
            self.telemetry.record_metric(MetricObservation {
                name: MetricName::OutboxDispatchLagSeconds,
                value: self
                    .clock
                    .now()
                    .saturating_duration_since(record.event.occurred_at)
                    .as_secs_f64(),
                operation: Some(ApplicationOperation::DispatchOutbox),
            });
            match self.publisher.publish(record).await {
                Ok(SessionEventPublishOutcome::Published) => {
                    report.published = report.published.saturating_add(1)
                }
                Ok(SessionEventPublishOutcome::AlreadyPublished) => {
                    report.deduplicated = report.deduplicated.saturating_add(1)
                }
                Err(error) => {
                    return Err(self
                        .record_failure(
                            record,
                            OutboxRecordStage::PublishEvent,
                            OutboxRecordFailure::Infrastructure(error),
                            completed_through,
                            report,
                        )
                        .await);
                }
            }
            if let Err((stage, failure)) = self.activate(record, &mut report).await {
                return Err(self
                    .record_failure(record, stage, failure, completed_through, report)
                    .await);
            }
            report.processed = report.processed.saturating_add(1);
            completed_through = Some(record.sequence);
            self.emit(
                DiagnosticEventName::OutboxRecordPublished,
                DiagnosticLevel::Debug,
                self.clock.now(),
                DispatchDiagnosticContext {
                    session_id: Some(record.event.session_id.clone()),
                    ..DispatchDiagnosticContext::default()
                },
            );
        }
        if let Some(through) = completed_through
            && let Err(failure) = self.acknowledge(through, &mut report).await
        {
            self.emit_ack_failure(&failure);
            return Err(OutboxDispatchError::Acknowledge {
                through,
                failure,
                partial: Box::new(report),
            });
        }
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::OutboxRecordsPublished,
            value: report.published as f64,
            operation: Some(ApplicationOperation::DispatchOutbox),
        });
        self.emit(
            DiagnosticEventName::OutboxDispatchCompleted,
            DiagnosticLevel::Debug,
            self.clock.now(),
            DispatchDiagnosticContext::default(),
        );
        Ok(report)
    }

    async fn activate(
        &self,
        record: &SessionOutboxRecord,
        report: &mut OutboxDispatchReport,
    ) -> Result<(), (OutboxRecordStage, OutboxRecordFailure)> {
        if record.event.kind == DomainEventKind::SessionRemoved {
            self.observations
                .cancel_session_observations(&record.event.session_id)
                .await
                .map_err(|error| {
                    (
                        OutboxRecordStage::CancelObservations,
                        OutboxRecordFailure::Infrastructure(error),
                    )
                })?;
            report.observation_cancellations = report.observation_cancellations.saturating_add(1);
            return Ok(());
        }
        if !record.event.kind.invalidates_observation_plan() {
            report.observation_plans_skipped = report.observation_plans_skipped.saturating_add(1);
            return Ok(());
        }
        // A delayed event can describe an older lifecycle or configuration.
        // Build scheduling intent from the current aggregate so replaying that
        // event cannot restore an obsolete plan from its historical snapshot.
        // See ../docs/internals/recovery/events-and-outbox.md#commit-publish-activate-acknowledge.
        let session = self
            .repository
            .load(&record.event.session_id)
            .await
            .map_err(|error| {
                (
                    OutboxRecordStage::LoadSession,
                    OutboxRecordFailure::Infrastructure(error),
                )
            })?;
        let Some(session) = session else {
            self.observations
                .cancel_session_observations(&record.event.session_id)
                .await
                .map_err(|error| {
                    (
                        OutboxRecordStage::CancelObservations,
                        OutboxRecordFailure::Infrastructure(error),
                    )
                })?;
            report.observation_cancellations = report.observation_cancellations.saturating_add(1);
            return Ok(());
        };
        if session.revision() < record.event.revision {
            return Err((
                OutboxRecordStage::ValidateSessionRevision,
                OutboxRecordFailure::RepositoryRevisionBehind {
                    event_revision: record.event.revision,
                    session_revision: session.revision(),
                },
            ));
        }
        let plan = self.observation_plan(&session, record.event.occurred_at);
        self.observations
            .reconcile_session_observations(&plan)
            .await
            .map_err(|error| {
                (
                    OutboxRecordStage::ReconcileObservations,
                    OutboxRecordFailure::Infrastructure(error),
                )
            })?;
        report.observation_plans = report.observation_plans.saturating_add(1);
        Ok(())
    }

    fn observation_plan(
        &self,
        session: &Session,
        occurred_at: TimestampMillis,
    ) -> SessionObservationPlan {
        let running = session.lifecycle() == LifecycleState::Running;
        SessionObservationPlan {
            session_id: session.id().clone(),
            expected_revision: session.revision(),
            foreground_not_before: (running
                && session.configuration().execution().detection_enabled())
            .then(|| occurred_at.saturating_add(self.limits.foreground_delay())),
            health_not_before: (running && !session.configuration().health_rules().is_empty())
                .then(|| occurred_at.saturating_add(self.limits.health_delay())),
        }
    }

    async fn record_failure(
        &self,
        record: &SessionOutboxRecord,
        stage: OutboxRecordStage,
        failure: OutboxRecordFailure,
        completed_through: Option<OutboxSequence>,
        mut report: OutboxDispatchReport,
    ) -> OutboxDispatchError {
        self.emit_record_failure(record, &failure);
        let acknowledgement_failure = if let Some(through) = completed_through {
            match self.acknowledge(through, &mut report).await {
                Ok(()) => None,
                Err(error) => {
                    self.emit_ack_failure(&error);
                    Some(error)
                }
            }
        } else {
            None
        };
        OutboxDispatchError::Record {
            sequence: record.sequence,
            stage,
            failure,
            acknowledgement_failure,
            partial: Box::new(report),
        }
    }

    async fn acknowledge(
        &self,
        through: OutboxSequence,
        report: &mut OutboxDispatchReport,
    ) -> Result<(), OutboxAcknowledgementFailure> {
        let outcome = self
            .outbox
            .acknowledge_through(through)
            .await
            .map_err(OutboxAcknowledgementFailure::Infrastructure)?;
        match outcome {
            OutboxAcknowledgeOutcome::Acknowledged {
                records,
                through: acknowledged_through,
            } if records == report.processed && acknowledged_through == through => {}
            OutboxAcknowledgeOutcome::AlreadyAcknowledged {
                through: acknowledged_through,
            } if acknowledged_through >= through => {}
            outcome => return Err(OutboxAcknowledgementFailure::InvalidOutcome(outcome)),
        }
        report.acknowledged = report.processed;
        report.last_acknowledged = Some(through);
        Ok(())
    }

    fn emit_port_failure(&self, error: &PortError) {
        self.emit(
            DiagnosticEventName::OutboxDispatchFailed,
            DiagnosticLevel::for_port_error(error.kind()),
            self.clock.now(),
            DispatchDiagnosticContext {
                session_id: error.session_id().cloned(),
                application_error: Some(error.category()),
                operation: Some(error.operation()),
                error_kind: Some(error.kind()),
            },
        );
    }

    fn emit_record_failure(&self, record: &SessionOutboxRecord, failure: &OutboxRecordFailure) {
        let (level, context) = match failure {
            OutboxRecordFailure::Infrastructure(error) => (
                DiagnosticLevel::for_port_error(error.kind()),
                DispatchDiagnosticContext {
                    session_id: Some(record.event.session_id.clone()),
                    application_error: Some(error.category()),
                    operation: Some(error.operation()),
                    error_kind: Some(error.kind()),
                },
            ),
            OutboxRecordFailure::RepositoryRevisionBehind { .. } => (
                DiagnosticLevel::Error,
                DispatchDiagnosticContext {
                    session_id: Some(record.event.session_id.clone()),
                    application_error: Some(ApplicationErrorCategory::InvariantViolation),
                    ..DispatchDiagnosticContext::default()
                },
            ),
        };
        self.emit(
            DiagnosticEventName::OutboxDispatchFailed,
            level,
            self.clock.now(),
            context,
        );
    }

    fn emit_ack_failure(&self, failure: &OutboxAcknowledgementFailure) {
        let (level, context) = match failure {
            OutboxAcknowledgementFailure::Infrastructure(error) => (
                DiagnosticLevel::for_port_error(error.kind()),
                DispatchDiagnosticContext {
                    session_id: error.session_id().cloned(),
                    application_error: Some(error.category()),
                    operation: Some(error.operation()),
                    error_kind: Some(error.kind()),
                },
            ),
            OutboxAcknowledgementFailure::InvalidOutcome(_) => (
                DiagnosticLevel::Error,
                DispatchDiagnosticContext {
                    application_error: Some(ApplicationErrorCategory::InvariantViolation),
                    ..DispatchDiagnosticContext::default()
                },
            ),
        };
        self.emit(
            DiagnosticEventName::OutboxDispatchFailed,
            level,
            self.clock.now(),
            context,
        );
    }

    fn emit_invariant_failure(&self) {
        self.emit(
            DiagnosticEventName::OutboxDispatchFailed,
            DiagnosticLevel::Error,
            self.clock.now(),
            DispatchDiagnosticContext {
                application_error: Some(ApplicationErrorCategory::InvariantViolation),
                ..DispatchDiagnosticContext::default()
            },
        );
    }

    fn emit(
        &self,
        event: DiagnosticEventName,
        level: DiagnosticLevel,
        timestamp: TimestampMillis,
        context: DispatchDiagnosticContext,
    ) {
        self.telemetry.emit_diagnostic(&DiagnosticRecord {
            event,
            level,
            timestamp,
            session_id: context.session_id,
            application_operation: Some(ApplicationOperation::DispatchOutbox),
            application_error: context.application_error,
            operation: context.operation,
            error_kind: context.error_kind,
        });
    }
}

fn validate_batch(
    records: &[SessionOutboxRecord],
    limit: usize,
) -> Result<(), OutboxBatchViolation> {
    if records.len() > limit {
        return Err(OutboxBatchViolation::LimitExceeded);
    }
    for pair in records.windows(2) {
        if pair[0].sequence >= pair[1].sequence {
            return Err(OutboxBatchViolation::SequenceOrder);
        }
        if pair[0].sequence.checked_next() != Some(pair[1].sequence) {
            return Err(OutboxBatchViolation::SequenceGap);
        }
    }
    Ok(())
}

#[derive(Default)]
struct DispatchDiagnosticContext {
    session_id: Option<undercurrent_domain::SessionId>,
    application_error: Option<ApplicationErrorCategory>,
    operation: Option<PortOperation>,
    error_kind: Option<PortErrorKind>,
}
