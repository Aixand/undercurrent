// SPDX-License-Identifier: Apache-2.0

//! Queue, detection, persistence, and configuration diagnostics.

use undercurrent_application::{MetricName, SessionRecoverySource};
use undercurrent_domain::{
    ForegroundClaim, ForegroundDecision, ObservationBasis, RuntimeConstraintState, SessionId,
};
use undercurrent_runtime::{MetricAggregate, SessionMutationDiagnostic};
use undercurrent_store_sqlite::SqliteRepositoryPressure;

use super::DaemonRuntimeDiagnostics;
use super::rows::{boolean, count, number, row, signed, text};
use crate::{
    BoundedProductDispatcher, DaemonDiagnosticField, DaemonDiagnosticRow, DaemonDiagnosticSection,
    DaemonOperationsError, MultiplexMetricSeries, ProductDispatchMetricSeries,
    ResolvedDaemonConfig, blocking::block_on,
};

impl DaemonRuntimeDiagnostics {
    /// Reports bounded queue pressure across daemon background services.
    pub(super) fn supervisor_rows(
        &self,
        dispatcher: Option<&BoundedProductDispatcher>,
        persistence: SqliteRepositoryPressure,
        row_limit: usize,
    ) -> Result<Vec<DaemonDiagnosticRow>, DaemonOperationsError> {
        let processes = self
            .core
            .processes
            .pressure()
            .map_err(|_| DaemonOperationsError::SourceUnavailable)?;
        let dispatch = dispatcher.map(BoundedProductDispatcher::pressure);
        let dispatch_telemetry = self.core.observability.product_dispatch.pressure();
        let multiplex_telemetry = self.core.observability.multiplex.pressure();
        let observations = self.observation.observations.pressure();
        let health = self.observation.health.pressure();
        let mutations = self.core.coordinator.pressure();
        let log_queue_lines = self.core.observability.log_pressure.as_ref().map_or_else(
            || self.core.config.logging().queue_lines(),
            |value| value.queue_lines(),
        );
        let dropped_log_lines = self
            .core
            .observability
            .log_pressure
            .as_ref()
            .map_or(0, |value| value.dropped_lines());
        let mut rows = vec![
            row(
                DaemonDiagnosticSection::SupervisorQueues,
                "product_dispatcher",
                [
                    count(
                        DaemonDiagnosticField::Queued,
                        dispatch.map(|value| value.queued_actions).unwrap_or(0),
                    ),
                    count(
                        DaemonDiagnosticField::Limit,
                        dispatch
                            .map(|value| value.queued_action_limit)
                            .unwrap_or_else(|| {
                                self.core.config.transport().dispatcher().queued_actions()
                            }),
                    ),
                    count(
                        DaemonDiagnosticField::Active,
                        dispatch.map(|value| value.active_workers).unwrap_or(0),
                    ),
                    count(
                        DaemonDiagnosticField::Workers,
                        dispatch
                            .map(|value| value.worker_limit)
                            .unwrap_or_else(|| self.core.config.transport().dispatcher().workers()),
                    ),
                    count(
                        DaemonDiagnosticField::Pending,
                        dispatch
                            .map(|value| value.cancellable_requests)
                            .unwrap_or(0),
                    ),
                    number(
                        DaemonDiagnosticField::Count,
                        dispatch.map(|value| value.admitted_actions).unwrap_or(0),
                    ),
                    number(
                        DaemonDiagnosticField::Failed,
                        dispatch.map(|value| value.rejected_actions).unwrap_or(0),
                    ),
                    boolean(
                        DaemonDiagnosticField::Closed,
                        dispatch.is_none_or(|value| value.closed),
                    ),
                ],
            ),
            row(
                DaemonDiagnosticSection::SupervisorQueues,
                "product_dispatch_telemetry",
                [
                    count(
                        DaemonDiagnosticField::Items,
                        dispatch_telemetry.retained_events,
                    ),
                    count(DaemonDiagnosticField::Limit, dispatch_telemetry.event_limit),
                    number(
                        DaemonDiagnosticField::Dropped,
                        dispatch_telemetry
                            .dropped_events
                            .saturating_add(dispatch_telemetry.dropped_metric_updates),
                    ),
                    number(
                        DaemonDiagnosticField::Count,
                        dispatch_telemetry.observed_events,
                    ),
                    number(
                        DaemonDiagnosticField::Failed,
                        dispatch_telemetry.unsuccessful_events,
                    ),
                    number(
                        DaemonDiagnosticField::Samples,
                        dispatch_telemetry.timed_events,
                    ),
                    number(
                        DaemonDiagnosticField::DurationMicros,
                        dispatch_telemetry.total_duration_micros,
                    ),
                    number(
                        DaemonDiagnosticField::MaximumDurationMicros,
                        dispatch_telemetry.maximum_duration_micros,
                    ),
                    count(
                        DaemonDiagnosticField::MetricSeries,
                        dispatch_telemetry.metric_series,
                    ),
                ],
            ),
            row(
                DaemonDiagnosticSection::SupervisorQueues,
                "structured_logging",
                [
                    count(DaemonDiagnosticField::Limit, log_queue_lines),
                    count(DaemonDiagnosticField::Dropped, dropped_log_lines),
                ],
            ),
            row(
                DaemonDiagnosticSection::SupervisorQueues,
                "multiplex_telemetry",
                [
                    count(
                        DaemonDiagnosticField::MetricSeries,
                        multiplex_telemetry.metric_series,
                    ),
                    number(
                        DaemonDiagnosticField::Dropped,
                        multiplex_telemetry.dropped_metric_updates,
                    ),
                ],
            ),
            row(
                DaemonDiagnosticSection::SupervisorQueues,
                "process_runtime",
                [
                    count(DaemonDiagnosticField::Queued, processes.queued_spawns),
                    count(DaemonDiagnosticField::Limit, processes.spawn_queue_capacity),
                    count(DaemonDiagnosticField::Ready, processes.queued_exits),
                    count(
                        DaemonDiagnosticField::Pending,
                        processes.published_process_exits,
                    ),
                    count(
                        DaemonDiagnosticField::Active,
                        processes.active_process_entries,
                    ),
                ],
            ),
            row(
                DaemonDiagnosticSection::SupervisorQueues,
                "process_preparation",
                [
                    count(DaemonDiagnosticField::Queued, processes.queued_preparations),
                    count(
                        DaemonDiagnosticField::Limit,
                        processes.preparation_queue_capacity,
                    ),
                ],
            ),
            row(
                DaemonDiagnosticSection::SupervisorQueues,
                "restart_queue",
                [
                    count(
                        DaemonDiagnosticField::Queued,
                        self.observation.restarts.len(),
                    ),
                    count(
                        DaemonDiagnosticField::Limit,
                        self.core.config.services().resources().restart_jobs(),
                    ),
                ],
            ),
            row(
                DaemonDiagnosticSection::SupervisorQueues,
                "observation_queue",
                [
                    count(DaemonDiagnosticField::Queued, observations.jobs),
                    count(DaemonDiagnosticField::Sessions, observations.sessions),
                    count(
                        DaemonDiagnosticField::Pending,
                        observations.planned_sessions,
                    ),
                    count(DaemonDiagnosticField::Limit, observations.limit),
                ],
            ),
            row(
                DaemonDiagnosticSection::SupervisorQueues,
                "health_checks",
                [
                    count(DaemonDiagnosticField::Queued, health.queued_requests),
                    count(DaemonDiagnosticField::Limit, health.queue_limit),
                    count(DaemonDiagnosticField::Active, health.active_workers),
                    count(DaemonDiagnosticField::Workers, health.workers),
                    number(DaemonDiagnosticField::Items, health.completed_requests),
                    number(
                        DaemonDiagnosticField::Dropped,
                        health
                            .cancelled_requests
                            .saturating_add(health.capacity_rejections),
                    ),
                ],
            ),
            row(
                DaemonDiagnosticSection::SupervisorQueues,
                "persistence_worker",
                [
                    count(DaemonDiagnosticField::Queued, persistence.queued_requests),
                    count(
                        DaemonDiagnosticField::Limit,
                        persistence.request_queue_capacity,
                    ),
                    count(DaemonDiagnosticField::Pending, persistence.outbox_events),
                ],
            ),
            row(
                DaemonDiagnosticSection::SupervisorQueues,
                "session_mutations",
                [
                    count(DaemonDiagnosticField::Sessions, mutations.active_sessions),
                    count(DaemonDiagnosticField::Active, mutations.held_sessions),
                    count(
                        DaemonDiagnosticField::WaitingCallers,
                        mutations.waiting_callers,
                    ),
                    number(DaemonDiagnosticField::Failed, mutations.capacity_rejections),
                ],
            ),
        ];
        rows.truncate(row_limit);
        let application_metric_aggregates = self
            .core
            .observability
            .telemetry
            .metric_aggregates()
            .into_iter()
            .filter(|aggregate| {
                matches!(
                    aggregate.name,
                    MetricName::SupervisorMailboxDepth
                        | MetricName::SupervisorCommandDurationSeconds
                )
            })
            .collect::<Vec<_>>();
        let reserved_metric_rows = application_metric_aggregates
            .len()
            .min(row_limit.saturating_sub(rows.len()));
        let page_limit = row_limit
            .saturating_sub(rows.len())
            .saturating_sub(reserved_metric_rows)
            .min(self.core.config.mutation().max_diagnostic_page_size());
        if page_limit > 0 {
            let page = self
                .core
                .coordinator
                .diagnostic_page(None, page_limit)
                .map_err(|_| DaemonOperationsError::SourceInvariant)?;
            rows.extend(page.items.into_iter().map(session_mutation_row));
        }
        let mut application_metrics = application_metric_aggregates.into_iter();
        let mut dispatch_metrics = self
            .core
            .observability
            .product_dispatch
            .metric_series()
            .into_iter();
        let mut multiplex_metrics = self
            .core
            .observability
            .multiplex
            .metric_series()
            .into_iter();
        while rows.len() < row_limit {
            let before = rows.len();
            if let Some(aggregate) = application_metrics.next() {
                rows.push(metric_aggregate_row(
                    DaemonDiagnosticSection::SupervisorQueues,
                    aggregate,
                ));
            }
            if rows.len() < row_limit
                && let Some(aggregate) = dispatch_metrics.next()
            {
                rows.push(dispatch_metric_row(aggregate));
            }
            if rows.len() < row_limit
                && let Some(aggregate) = multiplex_metrics.next()
            {
                rows.push(multiplex_metric_row(aggregate));
            }
            if rows.len() == before {
                break;
            }
        }
        Ok(rows)
    }

    /// Reports aggregate pressure and a bounded durable decision-evidence page.
    pub(super) fn detection_rows(
        &self,
        row_limit: usize,
    ) -> Result<Vec<DaemonDiagnosticRow>, DaemonOperationsError> {
        let processes = self
            .core
            .processes
            .pressure()
            .map_err(|_| DaemonOperationsError::SourceUnavailable)?;
        let foreground = self.observation.foreground.pressure();
        let profiles = self.observation.profiles.pressure();
        let patterns = self.observation.patterns.pressure();
        let mut rows = vec![
            row(
                DaemonDiagnosticSection::DetectionEvidence,
                "cooperative_signals",
                [
                    count(
                        DaemonDiagnosticField::Items,
                        processes.cooperative_signals.retained_signals,
                    ),
                    count(
                        DaemonDiagnosticField::Limit,
                        processes.cooperative_signals.retained_signal_limit,
                    ),
                    number(
                        DaemonDiagnosticField::Count,
                        processes.cooperative_signals.accepted_signals,
                    ),
                    number(
                        DaemonDiagnosticField::Failed,
                        processes
                            .cooperative_signals
                            .malformed_signals
                            .saturating_add(processes.cooperative_signals.oversized_signals),
                    ),
                    number(
                        DaemonDiagnosticField::Evictions,
                        processes.cooperative_signals.evictions,
                    ),
                ],
            ),
            row(
                DaemonDiagnosticSection::DetectionEvidence,
                "foreground_evidence",
                [
                    count(DaemonDiagnosticField::Sessions, foreground.tracked_sessions),
                    count(
                        DaemonDiagnosticField::Limit,
                        foreground.tracked_session_limit,
                    ),
                    number(DaemonDiagnosticField::Count, foreground.cycles),
                    number(
                        DaemonDiagnosticField::Items,
                        foreground
                            .cooperative_matches
                            .saturating_add(foreground.prompt_matches)
                            .saturating_add(foreground.motion_matches)
                            .saturating_add(foreground.server_idle_matches),
                    ),
                    number(DaemonDiagnosticField::Evictions, foreground.evictions),
                ],
            ),
            row(
                DaemonDiagnosticSection::DetectionEvidence,
                "profiles",
                [
                    count(DaemonDiagnosticField::Items, profiles.profiles),
                    count(DaemonDiagnosticField::Limit, profiles.profile_limit),
                    number(DaemonDiagnosticField::Revision, profiles.generation.value()),
                ],
            ),
            row(
                DaemonDiagnosticSection::DetectionEvidence,
                "regex_cache",
                [
                    count(DaemonDiagnosticField::Items, patterns.cached_patterns),
                    count(DaemonDiagnosticField::Limit, patterns.cache_limit),
                    number(
                        DaemonDiagnosticField::Count,
                        patterns.cache_hits.saturating_add(patterns.cache_misses),
                    ),
                    number(DaemonDiagnosticField::Evictions, patterns.cache_evictions),
                ],
            ),
        ];
        rows.truncate(row_limit);
        let session_limit = row_limit
            .saturating_sub(rows.len())
            .min(self.core.config.sqlite().limits().max_read_batch());
        if session_limit == 0 {
            return Ok(rows);
        }
        let page = block_on(self.core.repository.read_batch(None, session_limit))
            .map_err(|_| DaemonOperationsError::SourceUnavailable)?;
        for session in page.sessions {
            let remaining = row_limit.saturating_sub(rows.len());
            if remaining == 0 {
                break;
            }
            rows.extend(foreground_decision_rows(
                session.id(),
                session.foreground_decision(),
                remaining,
            ));
        }
        Ok(rows)
    }

    /// Reports retained persistence, event, and telemetry pressure.
    pub(super) fn persistence_rows(
        &self,
        persistence: SqliteRepositoryPressure,
        row_limit: usize,
    ) -> Vec<DaemonDiagnosticRow> {
        let events = self.streams.events.pressure();
        let telemetry = self.core.observability.telemetry.pressure();
        let mut rows = vec![
            row(
                DaemonDiagnosticSection::Persistence,
                "sqlite",
                [
                    count(DaemonDiagnosticField::Sessions, persistence.sessions),
                    count(DaemonDiagnosticField::Pending, persistence.outbox_events),
                    count(DaemonDiagnosticField::Limit, persistence.session_limit),
                    count(DaemonDiagnosticField::ItemLimit, persistence.outbox_limit),
                    count(DaemonDiagnosticField::Queued, persistence.queued_requests),
                ],
            ),
            row(
                DaemonDiagnosticSection::Persistence,
                "event_journal",
                [
                    count(DaemonDiagnosticField::Items, events.retained_records),
                    count(DaemonDiagnosticField::Limit, events.record_limit),
                    number(DaemonDiagnosticField::Evictions, events.evicted_records),
                ],
            ),
            row(
                DaemonDiagnosticSection::Persistence,
                "telemetry_retention",
                [
                    count(DaemonDiagnosticField::Items, telemetry.diagnostics),
                    count(DaemonDiagnosticField::Count, telemetry.metrics),
                    count(DaemonDiagnosticField::Samples, telemetry.aggregate_series),
                    number(
                        DaemonDiagnosticField::Dropped,
                        telemetry
                            .dropped_diagnostics
                            .saturating_add(telemetry.dropped_metrics),
                    ),
                ],
            ),
        ];
        rows.truncate(row_limit);
        for aggregate in self
            .core
            .observability
            .telemetry
            .metric_aggregates()
            .into_iter()
            .take(row_limit.saturating_sub(rows.len()))
        {
            rows.push(metric_aggregate_row(
                DaemonDiagnosticSection::Persistence,
                aggregate,
            ));
        }
        rows
    }
}

fn foreground_decision_rows(
    session_id: &SessionId,
    decision: &ForegroundDecision,
    limit: usize,
) -> Vec<DaemonDiagnosticRow> {
    if limit == 0 {
        return Vec::new();
    }
    let observation = decision.observation();
    let mut rows = Vec::with_capacity(limit.min(decision.evidence().len().saturating_add(3)));
    rows.push(axis_decision_row(
        session_id,
        "execution",
        observation.execution().basis(),
        [text(
            DaemonDiagnosticField::Execution,
            observation.execution().state().to_string(),
        )],
    ));
    rows.push(axis_decision_row(
        session_id,
        "interaction",
        observation.interaction().basis(),
        [
            text(
                DaemonDiagnosticField::Interaction,
                observation.interaction().state().to_string(),
            ),
            text(
                DaemonDiagnosticField::InteractionModality,
                observation
                    .interaction()
                    .modality()
                    .map_or("none".to_owned(), |value| value.to_string()),
            ),
            text(
                DaemonDiagnosticField::Semantic,
                observation
                    .interaction()
                    .semantic()
                    .map_or("none", |value| value.as_str()),
            ),
        ],
    ));
    rows.push(axis_decision_row(
        session_id,
        "constraint",
        observation.constraint().basis(),
        [
            text(
                DaemonDiagnosticField::Constraint,
                constraint_name(observation.constraint().state()),
            ),
            text(
                DaemonDiagnosticField::Semantic,
                observation
                    .constraint()
                    .semantic()
                    .map_or("none", |value| value.as_str()),
            ),
        ],
    ));
    rows.truncate(limit);
    for (index, assessed) in decision
        .evidence()
        .iter()
        .enumerate()
        .take(limit.saturating_sub(rows.len()))
    {
        let evidence = assessed.evidence();
        let identity = format!(
            "foreground_evidence:{}:{index}:{}",
            session_id.as_str(),
            evidence.source().as_str()
        );
        let mut fields = vec![
            text(DaemonDiagnosticField::SessionId, session_id.as_str()),
            text(
                DaemonDiagnosticField::ForegroundAxis,
                evidence.axis().as_str(),
            ),
            text(
                DaemonDiagnosticField::EvidenceSource,
                evidence.source().as_str(),
            ),
            text(
                DaemonDiagnosticField::EvidencePriority,
                evidence.priority().as_str(),
            ),
            text(
                DaemonDiagnosticField::EvidenceDisposition,
                evidence.disposition().as_str(),
            ),
            text(
                DaemonDiagnosticField::EvidenceAssessment,
                assessed.assessment().as_str(),
            ),
            number(
                DaemonDiagnosticField::ConfidenceBasisPoints,
                u64::from(evidence.confidence().basis_points()),
            ),
            signed(
                DaemonDiagnosticField::ObservedAtMillis,
                evidence.observed_at().value(),
            ),
        ];
        match evidence.claim() {
            Some(ForegroundClaim::Execution(state)) => {
                fields.push(text(DaemonDiagnosticField::Execution, state.to_string()));
            }
            Some(ForegroundClaim::Interaction(claim)) => {
                fields.push(text(
                    DaemonDiagnosticField::Interaction,
                    claim.state().to_string(),
                ));
                fields.push(text(
                    DaemonDiagnosticField::InteractionModality,
                    claim
                        .modality()
                        .map_or("none".to_owned(), |value| value.to_string()),
                ));
                fields.push(text(
                    DaemonDiagnosticField::Semantic,
                    claim.semantic().map_or("none", |value| value.as_str()),
                ));
            }
            Some(ForegroundClaim::Constraint(claim)) => {
                fields.push(text(
                    DaemonDiagnosticField::Constraint,
                    constraint_name(claim.state()),
                ));
                fields.push(text(
                    DaemonDiagnosticField::Semantic,
                    claim.semantic().map_or("none", |value| value.as_str()),
                ));
            }
            None => {}
        }
        rows.push(row(
            DaemonDiagnosticSection::DetectionEvidence,
            identity,
            fields,
        ));
    }
    rows
}

fn axis_decision_row<const N: usize>(
    session_id: &SessionId,
    axis: &'static str,
    basis: &ObservationBasis,
    axis_fields: [(DaemonDiagnosticField, crate::DaemonDiagnosticValue); N],
) -> DaemonDiagnosticRow {
    let mut fields = vec![
        text(DaemonDiagnosticField::SessionId, session_id.as_str()),
        text(DaemonDiagnosticField::ForegroundAxis, axis),
        number(
            DaemonDiagnosticField::ConfidenceBasisPoints,
            u64::from(basis.confidence().basis_points()),
        ),
        text(
            DaemonDiagnosticField::EvidenceSource,
            basis.source().map_or("none", |value| value.as_str()),
        ),
        text(
            DaemonDiagnosticField::EvidenceDisposition,
            basis.disposition().as_str(),
        ),
        signed(
            DaemonDiagnosticField::CommittedAtMillis,
            basis.committed_at().value(),
        ),
    ];
    if let Some(observed_at) = basis.evidence_observed_at() {
        fields.push(signed(
            DaemonDiagnosticField::ObservedAtMillis,
            observed_at.value(),
        ));
    }
    fields.extend(axis_fields);
    row(
        DaemonDiagnosticSection::DetectionEvidence,
        format!("foreground_decision:{}:{axis}", session_id.as_str()),
        fields,
    )
}

const fn constraint_name(value: RuntimeConstraintState) -> &'static str {
    match value {
        RuntimeConstraintState::Clear => "clear",
        RuntimeConstraintState::Present => "present",
    }
}

fn session_mutation_row(item: SessionMutationDiagnostic) -> DaemonDiagnosticRow {
    row(
        DaemonDiagnosticSection::SupervisorQueues,
        format!("session_mutation:{}", item.session_id.as_str()),
        [
            text(DaemonDiagnosticField::SessionId, item.session_id.as_str()),
            boolean(DaemonDiagnosticField::Held, item.held),
            count(DaemonDiagnosticField::WaitingCallers, item.waiting_callers),
            boolean(DaemonDiagnosticField::AtLimit, item.at_waiter_limit),
        ],
    )
}

fn dispatch_metric_row(aggregate: ProductDispatchMetricSeries) -> DaemonDiagnosticRow {
    let operation = aggregate
        .operation
        .map_or("all", |operation| operation.as_str());
    let result = aggregate.result.map_or("all", |result| result.as_str());
    row(
        DaemonDiagnosticSection::SupervisorQueues,
        format!(
            "dispatch_metric:{}:{operation}:{result}",
            aggregate.name.as_str()
        ),
        [
            text(DaemonDiagnosticField::MetricName, aggregate.name.as_str()),
            text(DaemonDiagnosticField::Operation, operation),
            text(DaemonDiagnosticField::Result, result),
            number(DaemonDiagnosticField::Samples, aggregate.samples),
            text(
                DaemonDiagnosticField::MetricTotal,
                aggregate.total.to_string(),
            ),
            text(
                DaemonDiagnosticField::MetricMinimum,
                aggregate.minimum.to_string(),
            ),
            text(
                DaemonDiagnosticField::MetricMaximum,
                aggregate.maximum.to_string(),
            ),
            text(
                DaemonDiagnosticField::MetricLatest,
                aggregate.latest.to_string(),
            ),
        ],
    )
}

fn multiplex_metric_row(aggregate: MultiplexMetricSeries) -> DaemonDiagnosticRow {
    let traffic_class = aggregate
        .traffic_class
        .map_or("all", |traffic_class| traffic_class.as_str());
    row(
        DaemonDiagnosticSection::SupervisorQueues,
        format!(
            "multiplex_metric:{}:{traffic_class}",
            aggregate.name.as_str()
        ),
        [
            text(DaemonDiagnosticField::MetricName, aggregate.name.as_str()),
            text(DaemonDiagnosticField::TrafficClass, traffic_class),
            number(DaemonDiagnosticField::Samples, aggregate.samples),
            text(
                DaemonDiagnosticField::MetricTotal,
                aggregate.total.to_string(),
            ),
            text(
                DaemonDiagnosticField::MetricMinimum,
                aggregate.minimum.to_string(),
            ),
            text(
                DaemonDiagnosticField::MetricMaximum,
                aggregate.maximum.to_string(),
            ),
            text(
                DaemonDiagnosticField::MetricLatest,
                aggregate.latest.to_string(),
            ),
        ],
    )
}

fn metric_aggregate_row(
    section: DaemonDiagnosticSection,
    aggregate: MetricAggregate,
) -> DaemonDiagnosticRow {
    let operation = aggregate
        .operation
        .map_or("all", |operation| operation.as_str());
    row(
        section,
        format!("metric:{}:{operation}", aggregate.name.as_str()),
        [
            text(DaemonDiagnosticField::MetricName, aggregate.name.as_str()),
            text(DaemonDiagnosticField::Operation, operation),
            number(DaemonDiagnosticField::Samples, aggregate.samples),
            text(
                DaemonDiagnosticField::MetricTotal,
                aggregate.total.to_string(),
            ),
            text(
                DaemonDiagnosticField::MetricMinimum,
                aggregate.minimum.to_string(),
            ),
            text(
                DaemonDiagnosticField::MetricMaximum,
                aggregate.maximum.to_string(),
            ),
            text(
                DaemonDiagnosticField::MetricLatest,
                aggregate.latest.to_string(),
            ),
        ],
    )
}

/// Reports validated capacity settings without exposing configured paths.
pub(super) fn configuration_rows(config: &ResolvedDaemonConfig) -> Vec<DaemonDiagnosticRow> {
    let transport = config.transport();
    let services = config.services().resources();
    let terminal = config.terminal_output();
    vec![
        row(
            DaemonDiagnosticSection::Configuration,
            "transport_limits",
            [
                count(
                    DaemonDiagnosticField::Connections,
                    transport.local_server().maximum_connections(),
                ),
                number(
                    DaemonDiagnosticField::Streams,
                    u64::from(transport.policy().limits().max_logical_streams()),
                ),
                count(
                    DaemonDiagnosticField::Workers,
                    transport.dispatcher().workers(),
                ),
                count(
                    DaemonDiagnosticField::Queued,
                    transport.dispatcher().queued_actions(),
                ),
            ],
        ),
        row(
            DaemonDiagnosticSection::Configuration,
            "session_limits",
            [
                count(
                    DaemonDiagnosticField::Sessions,
                    config.sqlite().limits().max_sessions(),
                ),
                count(
                    DaemonDiagnosticField::Items,
                    services.session_event_records(),
                ),
                count(DaemonDiagnosticField::Pending, services.restart_jobs()),
            ],
        ),
        row(
            DaemonDiagnosticSection::Configuration,
            "terminal_limits",
            [
                count(DaemonDiagnosticField::Sessions, terminal.sessions),
                count(DaemonDiagnosticField::Bytes, terminal.global_bytes),
                count(DaemonDiagnosticField::ByteLimit, terminal.per_session_bytes),
                count(DaemonDiagnosticField::Streams, terminal.subscriptions),
            ],
        ),
    ]
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use undercurrent_application::{ApplicationOperation, MetricName};
    use undercurrent_domain::{
        Confidence, EvidenceDisposition, EvidencePriority, EvidenceSource, ExecutionState,
        ForegroundAxis, ForegroundClaim, ForegroundDecisionPolicy, ForegroundEvidence, SessionId,
        TimestampMillis,
    };
    use undercurrent_multiplex::{MultiplexMetricName, TrafficClass};
    use undercurrent_protocol::ProductOperation;

    use super::*;

    #[test]
    fn foreground_decision_rows_include_axes_and_ignored_rationale_without_payloads() {
        let selected = ForegroundEvidence::new(
            EvidenceSource::new("terminal_prompt").expect("source"),
            EvidencePriority::Terminal,
            EvidenceDisposition::Affirmative,
            ForegroundClaim::Execution(ExecutionState::Active),
            Confidence::from_basis_points(8_000).expect("confidence"),
            TimestampMillis::new(90),
        )
        .expect("evidence");
        let unavailable = ForegroundEvidence::unavailable(
            EvidenceSource::new("cooperative_shell").expect("source"),
            EvidencePriority::Cooperative,
            ForegroundAxis::Interaction,
            TimestampMillis::new(90),
        );
        let decision = ForegroundDecisionPolicy::new(
            4,
            Duration::from_secs(1),
            Confidence::from_basis_points(1_000).expect("confidence"),
        )
        .expect("policy")
        .decide(vec![selected, unavailable], TimestampMillis::new(100))
        .expect("decision");

        let rows = foreground_decision_rows(
            &SessionId::new("session-safe").expect("session ID"),
            &decision,
            5,
        );

        assert_eq!(rows.len(), 5);
        assert_eq!(
            rows[0].identity,
            "foreground_decision:session-safe:execution"
        );
        assert_eq!(
            rows[3]
                .fields
                .get(&DaemonDiagnosticField::EvidenceAssessment),
            Some(&crate::DaemonDiagnosticValue::Text("selected".to_owned()))
        );
        assert_eq!(
            rows[4]
                .fields
                .get(&DaemonDiagnosticField::EvidenceAssessment),
            Some(&crate::DaemonDiagnosticValue::Text(
                "ignored_unavailable".to_owned()
            ))
        );
        assert!(!format!("{rows:?}").contains("command"));
    }

    #[test]
    fn session_mutation_rows_are_payload_free_and_session_scoped() {
        let row = session_mutation_row(SessionMutationDiagnostic {
            session_id: SessionId::new("session-safe").expect("session ID"),
            held: true,
            waiting_callers: 2,
            at_waiter_limit: true,
        });

        assert_eq!(row.identity, "session_mutation:session-safe");
        assert_eq!(
            row.fields.get(&DaemonDiagnosticField::Held),
            Some(&crate::DaemonDiagnosticValue::Boolean(true))
        );
        assert_eq!(
            row.fields.get(&DaemonDiagnosticField::WaitingCallers),
            Some(&crate::DaemonDiagnosticValue::Unsigned(2))
        );
        assert_eq!(
            row.fields.get(&DaemonDiagnosticField::AtLimit),
            Some(&crate::DaemonDiagnosticValue::Boolean(true))
        );
    }

    #[test]
    fn metric_aggregate_rows_use_typed_dimensions_and_finite_decimal_values() {
        let row = metric_aggregate_row(
            DaemonDiagnosticSection::Persistence,
            MetricAggregate {
                name: MetricName::OperationDurationSeconds,
                operation: Some(ApplicationOperation::ListSessions),
                samples: 3,
                total: 0.75,
                minimum: 0.1,
                maximum: 0.4,
                latest: 0.25,
            },
        );

        assert_eq!(
            row.identity,
            "metric:operation_duration_seconds:list_sessions"
        );
        assert_eq!(
            row.fields.get(&DaemonDiagnosticField::MetricTotal),
            Some(&crate::DaemonDiagnosticValue::Text("0.75".to_owned()))
        );
        assert_eq!(
            row.fields.get(&DaemonDiagnosticField::Samples),
            Some(&crate::DaemonDiagnosticValue::Unsigned(3))
        );
        assert_eq!(row.section, DaemonDiagnosticSection::Persistence);
    }

    #[test]
    fn supervisor_metric_rows_remain_in_the_supervisor_section() {
        let row = metric_aggregate_row(
            DaemonDiagnosticSection::SupervisorQueues,
            MetricAggregate {
                name: MetricName::SupervisorCommandDurationSeconds,
                operation: None,
                samples: 1,
                total: 0.01,
                minimum: 0.01,
                maximum: 0.01,
                latest: 0.01,
            },
        );

        assert_eq!(row.section, DaemonDiagnosticSection::SupervisorQueues);
        assert_eq!(
            row.identity,
            "metric:supervisor_command_duration_seconds:all"
        );
    }

    #[test]
    fn dispatch_metric_rows_preserve_typed_operation_and_result_dimensions() {
        let row = dispatch_metric_row(ProductDispatchMetricSeries {
            name: crate::ProductDispatchMetricName::RpcResults,
            operation: Some(ProductOperation::ListSessions),
            result: Some(crate::ProductDispatchResultCategory::Success),
            samples: 2,
            total: 2.0,
            minimum: 1.0,
            maximum: 1.0,
            latest: 1.0,
        });

        assert_eq!(
            row.identity,
            "dispatch_metric:rpc_results:list_sessions:success"
        );
        assert_eq!(
            row.fields.get(&DaemonDiagnosticField::Result),
            Some(&crate::DaemonDiagnosticValue::Text("success".to_owned()))
        );
    }

    #[test]
    fn multiplex_metric_rows_preserve_typed_priority_dimensions() {
        let row = multiplex_metric_row(MultiplexMetricSeries {
            name: MultiplexMetricName::QueuedBytes,
            traffic_class: Some(TrafficClass::Bulk),
            samples: 3,
            total: 96.0,
            minimum: 0.0,
            maximum: 64.0,
            latest: 32.0,
        });

        assert_eq!(row.identity, "multiplex_metric:queued_bytes:bulk");
        assert_eq!(
            row.fields.get(&DaemonDiagnosticField::TrafficClass),
            Some(&crate::DaemonDiagnosticValue::Text("bulk".to_owned()))
        );
    }
}
