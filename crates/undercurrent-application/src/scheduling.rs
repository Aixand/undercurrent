// SPDX-License-Identifier: Apache-2.0

//! Revision-aware per-session observation scheduling contracts.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::time::Duration;

use undercurrent_domain::{Revision, SessionId, TimestampMillis};

use crate::{
    ApplicationErrorCategory, ApplicationOperation, Clock, DiagnosticEventName, DiagnosticLevel,
    DiagnosticRecord, DueObservationSource, MetricName, MetricObservation, ObservationError,
    ObservationOutcome, ObservationScheduler, ObservationSkipReason, ObserveForegroundService,
    ObserveHealthService, PortError, PortErrorKind, PortFuture, PortOperation, Telemetry,
};

/// Independently scheduled observation class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ObservationJobKind {
    /// Collect and decide best-effort foreground foreground.
    Foreground,
    /// Execute configured session health rules.
    Health,
}

impl ObservationJobKind {
    /// Complete observation job registry.
    pub const ALL: [Self; 2] = [Self::Foreground, Self::Health];

    /// Returns the stable snake-case job name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Foreground => "foreground",
            Self::Health => "health",
        }
    }
}

/// One revision-checked session-local observation deadline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservationJob {
    /// Stable owning session.
    pub session_id: SessionId,
    /// Session revision that caused this observation request.
    pub expected_revision: Revision,
    /// Independent observation class.
    pub kind: ObservationJobKind,
    /// Earliest application-clock execution time.
    pub not_before: TimestampMillis,
}

/// Result of idempotently scheduling one observation key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservationScheduleOutcome {
    /// A new session and observation-kind key was inserted.
    Scheduled,
    /// A newer revision or earlier same-revision deadline replaced prior work.
    Replaced,
    /// Exact job was already scheduled.
    Unchanged,
    /// Older session revision cannot replace newer scheduled work.
    StaleIgnored,
    /// A later deadline at the same revision cannot delay earlier work.
    EarlierDeadlineRetained,
}

/// Complete desired observation schedule for one durable session revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionObservationPlan {
    /// Stable owning session.
    pub session_id: SessionId,
    /// Durable revision from which this desired schedule was derived.
    pub expected_revision: Revision,
    /// Next foreground deadline, absent when foreground observation is inactive.
    pub foreground_not_before: Option<TimestampMillis>,
    /// Next health deadline, absent when health observation is inactive.
    pub health_not_before: Option<TimestampMillis>,
}

impl SessionObservationPlan {
    /// Returns the desired deadline for `kind`, if active.
    pub const fn deadline(&self, kind: ObservationJobKind) -> Option<TimestampMillis> {
        match kind {
            ObservationJobKind::Foreground => self.foreground_not_before,
            ObservationJobKind::Health => self.health_not_before,
        }
    }

    /// Returns the desired job for `kind`, if that observation is active.
    ///
    /// This function allocates only the returned stable session identity,
    /// performs no IO, and does not panic.
    pub fn job(&self, kind: ObservationJobKind) -> Option<ObservationJob> {
        let not_before = self.deadline(kind)?;
        Some(ObservationJob {
            session_id: self.session_id.clone(),
            expected_revision: self.expected_revision,
            kind,
            not_before,
        })
    }
}

/// Aggregate result of atomically applying one session observation plan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObservationReconcileOutcome {
    /// Missing desired jobs inserted.
    pub scheduled: usize,
    /// Existing jobs replaced by newer revisions or earlier deadlines.
    pub replaced: usize,
    /// Existing exact or earlier same-revision jobs retained.
    pub retained: usize,
    /// No-longer-desired jobs removed.
    pub removed: usize,
    /// Per-kind changes ignored because queued work had a newer revision.
    pub stale_ignored: usize,
}

/// Validated memory and cadence policy for one observation dispatch worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservationDispatchLimits {
    batch_size: usize,
    max_retained_issues: usize,
    foreground_interval: Duration,
    health_interval: Duration,
    retry_interval: Duration,
}

impl ObservationDispatchLimits {
    /// Validates one worker's batch, diagnostic, and cadence bounds.
    ///
    /// `max_retained_issues` may be zero when only aggregate diagnostics are
    /// required. This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns the stable name of a zero batch or interval field.
    pub const fn new(
        batch_size: usize,
        max_retained_issues: usize,
        foreground_interval: Duration,
        health_interval: Duration,
        retry_interval: Duration,
    ) -> Result<Self, InvalidObservationDispatchLimits> {
        if batch_size == 0 {
            return Err(InvalidObservationDispatchLimits {
                field: "batch_size",
            });
        }
        if foreground_interval.is_zero() {
            return Err(InvalidObservationDispatchLimits {
                field: "foreground_interval",
            });
        }
        if health_interval.is_zero() {
            return Err(InvalidObservationDispatchLimits {
                field: "health_interval",
            });
        }
        if retry_interval.is_zero() {
            return Err(InvalidObservationDispatchLimits {
                field: "retry_interval",
            });
        }
        Ok(Self {
            batch_size,
            max_retained_issues,
            foreground_interval,
            health_interval,
            retry_interval,
        })
    }

    /// Returns the maximum jobs claimed in one dispatch cycle.
    pub const fn batch_size(self) -> usize {
        self.batch_size
    }

    /// Returns the maximum payload-safe issues retained in one report.
    pub const fn max_retained_issues(self) -> usize {
        self.max_retained_issues
    }

    /// Returns the normal cadence for one successful observation kind.
    pub const fn interval(self, kind: ObservationJobKind) -> Duration {
        match kind {
            ObservationJobKind::Foreground => self.foreground_interval,
            ObservationJobKind::Health => self.health_interval,
        }
    }

    /// Returns the bounded delay used after transient or stale work.
    pub const fn retry_interval(self) -> Duration {
        self.retry_interval
    }
}

/// Validation failure for observation worker limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidObservationDispatchLimits {
    field: &'static str,
}

impl InvalidObservationDispatchLimits {
    /// Returns the stable invalid field name.
    pub const fn field(self) -> &'static str {
        self.field
    }
}

impl Display for InvalidObservationDispatchLimits {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "observation dispatch field {} must be non-zero",
            self.field
        )
    }
}

impl Error for InvalidObservationDispatchLimits {}

/// Application boundary for running one revision-checked observation job.
pub trait ScheduledObservationRunner: Send + Sync {
    /// Executes only the addressed observation kind for one session.
    fn run<'a>(
        &'a self,
        job: &'a ObservationJob,
    ) -> PortFuture<'a, Result<ObservationOutcome, ObservationError>>;
}

/// Routes scheduled jobs to the independent foreground and health use cases.
pub struct ObservationServiceRouter<'a> {
    foreground: &'a ObserveForegroundService<'a>,
    health: &'a ObserveHealthService<'a>,
}

impl<'a> ObservationServiceRouter<'a> {
    /// Composes the two observation use cases without performing IO.
    pub const fn new(
        foreground: &'a ObserveForegroundService<'a>,
        health: &'a ObserveHealthService<'a>,
    ) -> Self {
        Self { foreground, health }
    }
}

impl ScheduledObservationRunner for ObservationServiceRouter<'_> {
    fn run<'a>(
        &'a self,
        job: &'a ObservationJob,
    ) -> PortFuture<'a, Result<ObservationOutcome, ObservationError>> {
        Box::pin(async move {
            match job.kind {
                ObservationJobKind::Foreground => {
                    self.foreground
                        .execute_scheduled(&job.session_id, job.expected_revision)
                        .await
                }
                ObservationJobKind::Health => {
                    self.health
                        .execute_scheduled(&job.session_id, job.expected_revision)
                        .await
                }
            }
        })
    }
}

/// Stable dispatch stage retained for one failed job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservationDispatchStage {
    /// Execute foreground collection or health evaluation.
    Execute,
    /// Reinsert a periodic or retry deadline.
    Reschedule,
    /// Remove remaining jobs after the session disappeared.
    CancelSession,
}

/// Payload-safe failure classification retained by a bounded dispatch report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservationDispatchFailure {
    /// Observation use case failed with a stable application category.
    Application(ApplicationErrorCategory),
    /// Queue adapter failed with typed operation and infrastructure kind.
    Infrastructure {
        /// Stable failed boundary operation.
        operation: PortOperation,
        /// Stable infrastructure failure kind.
        kind: PortErrorKind,
    },
}

/// One bounded payload-safe issue from an otherwise isolated dispatch batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservationDispatchIssue {
    /// Stable affected session identity.
    pub session_id: SessionId,
    /// Independent foreground or health job kind.
    pub kind: ObservationJobKind,
    /// Stage that did not complete.
    pub stage: ObservationDispatchStage,
    /// Safe failure category without adapter source text.
    pub failure: ObservationDispatchFailure,
}

/// Aggregate result of one bounded due-observation claim.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObservationDispatchReport {
    /// Jobs removed from the due queue for this cycle.
    pub claimed: usize,
    /// Jobs that committed a new durable observation.
    pub committed: usize,
    /// Jobs whose durable observation was already current.
    pub unchanged: usize,
    /// Jobs disabled by an expected current precondition.
    pub skipped: usize,
    /// Jobs superseded by a newer durable session revision.
    pub stale: usize,
    /// Jobs whose observation use case returned an error.
    pub failed: usize,
    /// Periodic or retry deadlines accepted by the scheduler.
    pub rescheduled: usize,
    /// Jobs intentionally left inactive after a terminal or disabled state.
    pub deactivated: usize,
    /// Reschedules displaced by newer or earlier already-queued work.
    pub superseded: usize,
    /// Total issues encountered, including issues beyond the retention bound.
    pub issue_count: usize,
    /// Payload-safe issues retained up to the configured bound.
    pub issues: Vec<ObservationDispatchIssue>,
}

/// Fatal failure before any due observation can be dispatched.
#[derive(Debug)]
pub enum ObservationDispatchError {
    /// Due-work source could not claim a bounded batch.
    Claim(PortError),
}

impl ObservationDispatchError {
    /// Returns the stable presentation category for the fatal source failure.
    pub const fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::Claim(error) => error.category(),
        }
    }
}

impl Display for ObservationDispatchError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Claim(error) => write!(formatter, "could not claim due observations: {error}"),
        }
    }
}

impl Error for ObservationDispatchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Claim(error) => Some(error),
        }
    }
}

/// Runs one bounded due-observation batch without scanning session state.
pub struct DispatchObservationsService<'a> {
    clock: &'a dyn Clock,
    source: &'a dyn DueObservationSource,
    scheduler: &'a dyn ObservationScheduler,
    runner: &'a dyn ScheduledObservationRunner,
    telemetry: &'a dyn Telemetry,
    limits: ObservationDispatchLimits,
}

impl<'a> DispatchObservationsService<'a> {
    /// Composes one observation worker from explicit inward-facing boundaries.
    pub const fn new(
        clock: &'a dyn Clock,
        source: &'a dyn DueObservationSource,
        scheduler: &'a dyn ObservationScheduler,
        runner: &'a dyn ScheduledObservationRunner,
        telemetry: &'a dyn Telemetry,
        limits: ObservationDispatchLimits,
    ) -> Self {
        Self {
            clock,
            source,
            scheduler,
            runner,
            telemetry,
            limits,
        }
    }

    /// Claims and executes at most one configured batch in stable due order.
    ///
    /// Individual execution and rescheduling failures are isolated and retained
    /// as bounded safe issues. The method never scans unrelated sessions and
    /// holds no application lock across observation adapter IO. It may emit
    /// diagnostics and metrics but does not panic.
    ///
    /// # Errors
    ///
    /// Returns a fatal typed failure only when due work cannot be claimed.
    pub async fn execute(&self) -> Result<ObservationDispatchReport, ObservationDispatchError> {
        let claimed_at = self.clock.now();
        self.emit(
            DiagnosticEventName::ObservationDispatchStarted,
            DiagnosticLevel::Debug,
            claimed_at,
            DispatchDiagnosticContext::default(),
        );
        let jobs = self
            .source
            .claim_due_observations(claimed_at, self.limits.batch_size())
            .map_err(|error| {
                self.emit(
                    DiagnosticEventName::ObservationDispatchFailed,
                    DiagnosticLevel::for_port_error(error.kind()),
                    self.clock.now(),
                    DispatchDiagnosticContext {
                        session_id: error.session_id().cloned(),
                        application_error: Some(error.category()),
                        operation: Some(error.operation()),
                        error_kind: Some(error.kind()),
                    },
                );
                ObservationDispatchError::Claim(error)
            })?;
        let mut report = ObservationDispatchReport {
            claimed: jobs.len(),
            ..ObservationDispatchReport::default()
        };
        for job in jobs {
            self.telemetry.record_metric(MetricObservation {
                name: MetricName::ObservationSchedulingLagSeconds,
                value: claimed_at
                    .saturating_duration_since(job.not_before)
                    .as_secs_f64(),
                operation: Some(ApplicationOperation::DispatchObservations),
            });
            self.dispatch(job, &mut report).await;
        }
        self.emit(
            DiagnosticEventName::ObservationDispatchCompleted,
            if report.issue_count == 0 {
                DiagnosticLevel::Debug
            } else {
                DiagnosticLevel::Warn
            },
            self.clock.now(),
            DispatchDiagnosticContext::default(),
        );
        Ok(report)
    }

    async fn dispatch(&self, job: ObservationJob, report: &mut ObservationDispatchReport) {
        match self.runner.run(&job).await {
            Ok(ObservationOutcome::Committed { revision, .. }) => {
                report.committed = report.committed.saturating_add(1);
                self.reschedule(&job, revision, self.limits.interval(job.kind), report)
                    .await;
            }
            Ok(ObservationOutcome::Unchanged { revision }) => {
                report.unchanged = report.unchanged.saturating_add(1);
                self.reschedule(&job, revision, self.limits.interval(job.kind), report)
                    .await;
            }
            Ok(ObservationOutcome::Stale {
                current_revision, ..
            }) => {
                report.stale = report.stale.saturating_add(1);
                self.reschedule(&job, current_revision, self.limits.retry_interval(), report)
                    .await;
            }
            Ok(ObservationOutcome::Skipped(reason)) => {
                report.skipped = report.skipped.saturating_add(1);
                if reason == ObservationSkipReason::MissingProcessBinding {
                    self.reschedule(
                        &job,
                        job.expected_revision,
                        self.limits.retry_interval(),
                        report,
                    )
                    .await;
                } else {
                    report.deactivated = report.deactivated.saturating_add(1);
                }
            }
            Err(error) => {
                let category = error.category();
                if category == ApplicationErrorCategory::NotFound {
                    self.emit(
                        DiagnosticEventName::ObservationJobDeactivated,
                        DiagnosticLevel::Debug,
                        self.clock.now(),
                        DispatchDiagnosticContext {
                            session_id: Some(job.session_id.clone()),
                            application_error: Some(category),
                            ..DispatchDiagnosticContext::default()
                        },
                    );
                    self.cancel_session(&job, report).await;
                    report.deactivated = report.deactivated.saturating_add(1);
                    return;
                }
                report.failed = report.failed.saturating_add(1);
                self.record_issue(
                    report,
                    ObservationDispatchIssue {
                        session_id: job.session_id.clone(),
                        kind: job.kind,
                        stage: ObservationDispatchStage::Execute,
                        failure: ObservationDispatchFailure::Application(category),
                    },
                );
                self.emit(
                    DiagnosticEventName::ObservationJobFailed,
                    DiagnosticLevel::Warn,
                    self.clock.now(),
                    DispatchDiagnosticContext {
                        session_id: Some(job.session_id.clone()),
                        application_error: Some(category),
                        ..DispatchDiagnosticContext::default()
                    },
                );
                self.reschedule(
                    &job,
                    job.expected_revision,
                    self.limits.retry_interval(),
                    report,
                )
                .await;
            }
        }
    }

    async fn reschedule(
        &self,
        prior: &ObservationJob,
        expected_revision: Revision,
        delay: Duration,
        report: &mut ObservationDispatchReport,
    ) {
        let job = ObservationJob {
            session_id: prior.session_id.clone(),
            expected_revision,
            kind: prior.kind,
            not_before: self.clock.now().saturating_add(delay),
        };
        match self.scheduler.schedule_observation(&job).await {
            Ok(
                ObservationScheduleOutcome::Scheduled
                | ObservationScheduleOutcome::Replaced
                | ObservationScheduleOutcome::Unchanged,
            ) => report.rescheduled = report.rescheduled.saturating_add(1),
            Ok(
                ObservationScheduleOutcome::StaleIgnored
                | ObservationScheduleOutcome::EarlierDeadlineRetained,
            ) => report.superseded = report.superseded.saturating_add(1),
            Err(error) => {
                self.record_infrastructure_issue(
                    report,
                    prior,
                    ObservationDispatchStage::Reschedule,
                    &error,
                );
            }
        }
    }

    async fn cancel_session(&self, job: &ObservationJob, report: &mut ObservationDispatchReport) {
        if let Err(error) = self
            .scheduler
            .cancel_session_observations(&job.session_id)
            .await
        {
            self.record_infrastructure_issue(
                report,
                job,
                ObservationDispatchStage::CancelSession,
                &error,
            );
        }
    }

    fn record_infrastructure_issue(
        &self,
        report: &mut ObservationDispatchReport,
        job: &ObservationJob,
        stage: ObservationDispatchStage,
        error: &PortError,
    ) {
        self.record_issue(
            report,
            ObservationDispatchIssue {
                session_id: job.session_id.clone(),
                kind: job.kind,
                stage,
                failure: ObservationDispatchFailure::Infrastructure {
                    operation: error.operation(),
                    kind: error.kind(),
                },
            },
        );
        self.emit(
            DiagnosticEventName::ObservationRescheduleFailed,
            DiagnosticLevel::for_port_error(error.kind()),
            self.clock.now(),
            DispatchDiagnosticContext {
                session_id: Some(job.session_id.clone()),
                application_error: Some(error.category()),
                operation: Some(error.operation()),
                error_kind: Some(error.kind()),
            },
        );
    }

    fn record_issue(
        &self,
        report: &mut ObservationDispatchReport,
        issue: ObservationDispatchIssue,
    ) {
        report.issue_count = report.issue_count.saturating_add(1);
        if report.issues.len() < self.limits.max_retained_issues() {
            report.issues.push(issue);
        }
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
            application_operation: Some(ApplicationOperation::DispatchObservations),
            application_error: context.application_error,
            operation: context.operation,
            error_kind: context.error_kind,
        });
    }
}

#[derive(Default)]
struct DispatchDiagnosticContext {
    session_id: Option<SessionId>,
    application_error: Option<ApplicationErrorCategory>,
    operation: Option<PortOperation>,
    error_kind: Option<PortErrorKind>,
}
