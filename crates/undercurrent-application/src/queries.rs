// SPDX-License-Identifier: Apache-2.0

//! Observable use-case boundary for prepared session queries and watches.

use undercurrent_domain::{CatalogRevision, SessionProjection, SessionQuery};

use crate::{
    ApplicationOperation, CatalogError, CatalogResult, Clock, DiagnosticEventName, DiagnosticLevel,
    DiagnosticRecord, MetricName, MetricObservation, PreparedSessionCatalog, QueryCost,
    QueryWatchBatch, SessionPage, SessionPageCursor, SessionQueryWatch, Telemetry,
};

/// Executes bounded prepared session reads without process or supervisor IO.
pub struct SessionQueryService<'a> {
    clock: &'a dyn Clock,
    catalog: &'a PreparedSessionCatalog,
    telemetry: &'a dyn Telemetry,
}

impl<'a> SessionQueryService<'a> {
    /// Constructs a query service without performing IO.
    pub const fn new(
        clock: &'a dyn Clock,
        catalog: &'a PreparedSessionCatalog,
        telemetry: &'a dyn Telemetry,
    ) -> Self {
        Self {
            clock,
            catalog,
            telemetry,
        }
    }

    /// Returns one bounded prepared summary page.
    ///
    /// This use case reads only the indexed catalog, records bounded query cost,
    /// and never loads session details or crosses process, PTY, detection, or
    /// health ports. It does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed catalog, cursor-expiry, or invariant failures.
    pub fn list(
        &self,
        query: &SessionQuery,
        cursor: Option<&SessionPageCursor>,
    ) -> CatalogResult<SessionPage> {
        let operation = ApplicationOperation::ListSessions;
        let started_at = self.clock.now();
        let result = self.catalog.query(query, cursor);
        let finished_at = self.clock.now();
        self.record_duration(operation, started_at, finished_at);
        match &result {
            Ok(page) => {
                self.record_query_metrics(
                    operation,
                    query,
                    page.snapshot_revision,
                    page.items.len(),
                    page.query_cost,
                );
                self.emit(
                    DiagnosticEventName::SessionQueryCompleted,
                    DiagnosticLevel::Info,
                    operation,
                    finished_at,
                    None,
                );
            }
            Err(error) => self.emit_failure(
                DiagnosticEventName::SessionQueryFailed,
                operation,
                finished_at,
                error,
            ),
        }
        result
    }

    /// Opens one bounded first-page query watch.
    ///
    /// The returned handle owns only one projected page and can be retained by
    /// a logical multiplex stream. This function performs no external IO and
    /// does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed prepared-catalog failures.
    pub fn open_watch(&self, query: SessionQuery) -> CatalogResult<SessionQueryWatch> {
        let operation = ApplicationOperation::OpenSessionWatch;
        let started_at = self.clock.now();
        let result = self.catalog.open_watch(query);
        let finished_at = self.clock.now();
        self.record_duration(operation, started_at, finished_at);
        match &result {
            Ok(watch) => {
                self.record_query_metrics(
                    operation,
                    watch.query(),
                    watch.snapshot_revision(),
                    watch.items().len(),
                    watch.query_cost(),
                );
                self.emit(
                    DiagnosticEventName::QueryWatchOpened,
                    DiagnosticLevel::Info,
                    operation,
                    finished_at,
                    None,
                );
            }
            Err(error) => self.emit_failure(
                DiagnosticEventName::QueryWatchFailed,
                operation,
                finished_at,
                error,
            ),
        }
        result
    }

    /// Refreshes one bounded watch to the latest prepared catalog revision.
    ///
    /// Multiple changes may be coalesced into one page delta. The watch advances
    /// even when no projected membership or content changed. This function
    /// performs no external IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed prepared-catalog failures without modifying the watch on
    /// query failure.
    pub fn refresh_watch(
        &self,
        watch: &mut SessionQueryWatch,
    ) -> CatalogResult<Option<QueryWatchBatch>> {
        let operation = ApplicationOperation::RefreshSessionWatch;
        let started_at = self.clock.now();
        let result = self.catalog.refresh_watch(watch);
        let finished_at = self.clock.now();
        self.record_duration(operation, started_at, finished_at);
        match &result {
            Ok(_) => {
                self.record_query_metrics(
                    operation,
                    watch.query(),
                    watch.snapshot_revision(),
                    watch.items().len(),
                    watch.query_cost(),
                );
                self.emit(
                    DiagnosticEventName::QueryWatchRefreshed,
                    DiagnosticLevel::Debug,
                    operation,
                    finished_at,
                    None,
                );
            }
            Err(error) => self.emit_failure(
                DiagnosticEventName::QueryWatchFailed,
                operation,
                finished_at,
                error,
            ),
        }
        result
    }

    fn record_duration(
        &self,
        operation: ApplicationOperation,
        started_at: undercurrent_domain::TimestampMillis,
        finished_at: undercurrent_domain::TimestampMillis,
    ) {
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::OperationDurationSeconds,
            value: finished_at
                .saturating_duration_since(started_at)
                .as_secs_f64(),
            operation: Some(operation),
        });
    }

    fn record_query_metrics(
        &self,
        operation: ApplicationOperation,
        query: &SessionQuery,
        revision: CatalogRevision,
        rows: usize,
        cost: QueryCost,
    ) {
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::QueryRowsReturned,
            value: rows as f64,
            operation: Some(operation),
        });
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::QueryIndexEntriesExamined,
            value: cost.index_entries_examined as f64,
            operation: Some(operation),
        });
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::QueryCatalogRevision,
            value: revision.value() as f64,
            operation: Some(operation),
        });
        let projection = match query.projection {
            SessionProjection::Compact => MetricName::QueryProjectionCompact,
            SessionProjection::WithLabels => MetricName::QueryProjectionWithLabels,
            SessionProjection::Operational => MetricName::QueryProjectionOperational,
        };
        self.telemetry.record_metric(MetricObservation {
            name: projection,
            value: 1.0,
            operation: Some(operation),
        });
    }

    fn emit_failure(
        &self,
        event: DiagnosticEventName,
        operation: ApplicationOperation,
        timestamp: undercurrent_domain::TimestampMillis,
        error: &CatalogError,
    ) {
        if error.category() == crate::ApplicationErrorCategory::CursorExpired {
            self.telemetry.record_metric(MetricObservation {
                name: MetricName::QueryCursorExpirations,
                value: 1.0,
                operation: Some(operation),
            });
        }
        let level = DiagnosticLevel::for_application_error(error.category());
        self.emit(event, level, operation, timestamp, Some(error.category()));
    }

    fn emit(
        &self,
        event: DiagnosticEventName,
        level: DiagnosticLevel,
        operation: ApplicationOperation,
        timestamp: undercurrent_domain::TimestampMillis,
        application_error: Option<crate::ApplicationErrorCategory>,
    ) {
        self.telemetry.emit_diagnostic(&DiagnosticRecord {
            event,
            level,
            timestamp,
            session_id: None,
            application_operation: Some(operation),
            application_error,
            operation: None,
            error_kind: None,
        });
    }
}
