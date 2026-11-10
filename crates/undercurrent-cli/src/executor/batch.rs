// SPDX-License-Identifier: Apache-2.0

//! Paged selection and bounded parallel session mutations.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use crate::cli_support::CommonExitCode;
use serde::Serialize;
use undercurrent_protocol::{
    ListSessionsRequest, ProductOperation, ProductRequest, ProductSuccess, RemovalConfirmationDto,
    RemoveSessionRequest, RemoveSessionResultDto, SessionFilterDto, SessionProjectionDto,
    SessionSortDto, StopSessionRequest, StopSessionResultDto,
};

use super::{FiniteCliExecutor, operation_mismatch};
use crate::deadline::CliCommandDeadline;
use crate::{
    CleanArguments, CliCommand, CliError, PreparedSessionSelection, StopArguments,
    prepare_clean_selection, prepare_stop_selection,
};

/// Payload-safe failure for one stable session in a bulk command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SessionBatchFailure {
    /// Stable session identity selected before mutation began.
    pub session_id: String,
    /// Stable process result category used by native CLI exit handling.
    pub result_category: CommonExitCode,
    /// Safe actionable error text from the typed CLI boundary.
    pub message: String,
}

/// Deterministic result of one exact or paged bulk session command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SessionBatchResult<T> {
    /// Number of stable session identities selected before mutation began.
    pub selected: usize,
    /// Successful typed results in deterministic selection order.
    pub succeeded: Vec<T>,
    /// Failed identities and payload-safe categories in selection order.
    pub failures: Vec<SessionBatchFailure>,
}

impl<T> SessionBatchResult<T> {
    /// Returns whether every selected mutation completed successfully.
    pub const fn is_complete(&self) -> bool {
        self.failures.is_empty()
    }
}

impl FiniteCliExecutor<'_> {
    /// Stops one exact name or one complete paged selection.
    ///
    /// Query selections are fully materialized as compact stable IDs before
    /// mutations begin, so catalog changes caused by this command cannot
    /// invalidate its own cursor. Mutations then execute with configured bounded
    /// concurrency over the same multiplexed client.
    ///
    /// # Errors
    ///
    /// Returns selection preparation, lookup, pagination, request, or total
    /// command-deadline failures that occur before a bulk mutation report exists.
    pub fn stop_selected(
        &self,
        arguments: &StopArguments,
    ) -> Result<SessionBatchResult<StopSessionResultDto>, CliError> {
        let deadline = CliCommandDeadline::start(CliCommand::Stop, self.timeout)?;
        match prepare_stop_selection(arguments)? {
            PreparedSessionSelection::ExactName(name) => {
                let session_id = self.resolve_name_with_timeout(&name, deadline.remaining()?)?;
                let result =
                    self.stop_id(session_id, !arguments.graceful, deadline.remaining()?)?;
                Ok(SessionBatchResult {
                    selected: 1,
                    succeeded: vec![result],
                    failures: Vec::new(),
                })
            }
            PreparedSessionSelection::Query(filter) => {
                let session_ids = self.collect_session_ids(filter, deadline, CliCommand::Stop)?;
                Ok(
                    self.execute_parallel(
                        session_ids,
                        deadline,
                        |executor, session_id, timeout| {
                            executor.stop_id(session_id, !arguments.graceful, timeout)
                        },
                    ),
                )
            }
        }
    }

    /// Removes one exact name or every selected terminal session after confirmation.
    ///
    /// The caller must collect interactive or explicit-force confirmation before
    /// invoking this use case. `clean` without a name selects only terminal lifecycle
    /// summaries, snapshots every ID, then removes them with bounded concurrency.
    ///
    /// # Errors
    ///
    /// Returns selection preparation, lookup, pagination, request, or total
    /// command-deadline failures that occur before a bulk mutation report exists.
    pub fn clean_selected(
        &self,
        arguments: &CleanArguments,
        confirmation: RemovalConfirmationDto,
    ) -> Result<SessionBatchResult<RemoveSessionResultDto>, CliError> {
        let deadline = CliCommandDeadline::start(CliCommand::Clean, self.timeout)?;
        match prepare_clean_selection(arguments)? {
            PreparedSessionSelection::ExactName(name) => {
                let session_id = self.resolve_name_with_timeout(&name, deadline.remaining()?)?;
                let result = self.remove_id(session_id, confirmation, deadline.remaining()?)?;
                Ok(SessionBatchResult {
                    selected: 1,
                    succeeded: vec![result],
                    failures: Vec::new(),
                })
            }
            PreparedSessionSelection::Query(filter) => {
                let session_ids = self.collect_session_ids(filter, deadline, CliCommand::Clean)?;
                Ok(
                    self.execute_parallel(
                        session_ids,
                        deadline,
                        |executor, session_id, timeout| {
                            executor.remove_id(session_id, confirmation, timeout)
                        },
                    ),
                )
            }
        }
    }

    fn collect_session_ids(
        &self,
        filter: SessionFilterDto,
        deadline: CliCommandDeadline,
        command: CliCommand,
    ) -> Result<Vec<String>, CliError> {
        let mut cursor = None;
        let mut cursors = BTreeSet::new();
        let mut identities = BTreeSet::new();
        let selection_limit = self.limits.bulk_selection_sessions();
        let initial_capacity = usize::try_from(self.limits.catalog_page_size())
            .unwrap_or(usize::MAX)
            .min(selection_limit);
        let mut selected = Vec::with_capacity(initial_capacity);
        loop {
            let page = self.list_request_with_timeout(
                ListSessionsRequest {
                    filter: filter.clone(),
                    sort: SessionSortDto::default(),
                    projection: SessionProjectionDto::Compact,
                    page_size: self.limits.catalog_page_size(),
                    cursor: cursor.clone(),
                    include_total: false,
                },
                deadline.remaining()?,
            )?;
            if page.items.is_empty() && page.next_cursor.is_some() {
                return Err(CliError::PaginationInvariant { command });
            }
            for summary in page.items {
                if !identities.insert(summary.id.clone()) {
                    return Err(CliError::PaginationInvariant { command });
                }
                if selected.len() == selection_limit {
                    return Err(CliError::SelectionCapacity {
                        command,
                        limit: selection_limit,
                    });
                }
                selected.push(summary.id);
            }
            let Some(next) = page.next_cursor else {
                return Ok(selected);
            };
            if cursor.as_ref() == Some(&next) || !cursors.insert(next.clone()) {
                return Err(CliError::PaginationInvariant { command });
            }
            cursor = Some(next);
        }
    }

    fn execute_parallel<T, F>(
        &self,
        session_ids: Vec<String>,
        deadline: CliCommandDeadline,
        operation: F,
    ) -> SessionBatchResult<T>
    where
        T: Send,
        F: Fn(&Self, String, Duration) -> Result<T, CliError> + Sync,
    {
        let selected_count = session_ids.len();
        if selected_count == 0 {
            return SessionBatchResult {
                selected: 0,
                succeeded: Vec::new(),
                failures: Vec::new(),
            };
        }
        let worker_count = selected_count.min(self.limits.bulk_parallel_requests());
        let next = AtomicUsize::new(0);
        let mut outcomes = std::thread::scope(|scope| {
            let (sender, receiver) = mpsc::sync_channel(worker_count);
            for _ in 0..worker_count {
                let sender = sender.clone();
                let session_ids = &session_ids;
                let operation = &operation;
                let next = &next;
                scope.spawn(move || {
                    loop {
                        let index = next.fetch_add(1, Ordering::AcqRel);
                        let Some(session_id) = session_ids.get(index).cloned() else {
                            break;
                        };
                        let result = deadline
                            .remaining()
                            .and_then(|timeout| operation(self, session_id.clone(), timeout));
                        if sender.send((index, session_id, result)).is_err() {
                            break;
                        }
                    }
                });
            }
            drop(sender);
            receiver.into_iter().collect::<Vec<_>>()
        });
        outcomes.sort_by_key(|(index, _, _)| *index);

        let mut succeeded = Vec::new();
        let mut failures = Vec::new();
        for (_, session_id, result) in outcomes {
            match result {
                Ok(result) => succeeded.push(result),
                Err(error) => failures.push(SessionBatchFailure {
                    session_id,
                    result_category: error.exit_code(),
                    message: error.to_string(),
                }),
            }
        }
        SessionBatchResult {
            selected: selected_count,
            succeeded,
            failures,
        }
    }

    pub(crate) fn stop_id(
        &self,
        session_id: String,
        force: bool,
        timeout: Duration,
    ) -> Result<StopSessionResultDto, CliError> {
        let success = self.execute_with_timeout(
            ProductRequest::StopSession(StopSessionRequest {
                graceful_only: false,
                expected_revision: None,
                mutation: self
                    .mutations
                    .next(CliCommand::Stop, ProductOperation::StopSession),
                session_id,
                force,
            }),
            timeout,
        )?;
        match success {
            ProductSuccess::StopSession(result) => Ok(result),
            other => Err(operation_mismatch(ProductOperation::StopSession, other)),
        }
    }

    fn remove_id(
        &self,
        session_id: String,
        confirmation: RemovalConfirmationDto,
        timeout: Duration,
    ) -> Result<RemoveSessionResultDto, CliError> {
        let success = self.execute_with_timeout(
            ProductRequest::RemoveSession(RemoveSessionRequest {
                mutation: self
                    .mutations
                    .next(CliCommand::Clean, ProductOperation::RemoveSession),
                session_id,
                confirmation,
            }),
            timeout,
        )?;
        match success {
            ProductSuccess::RemoveSession(result) => Ok(result),
            other => Err(operation_mismatch(ProductOperation::RemoveSession, other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Barrier, Mutex};

    use undercurrent_protocol::{
        HealthStatusDto, HealthSummaryDto, LifecycleStateDto, ProductError, ProductErrorCategory,
        SessionPageDto, SessionSummaryDto, WorkSummaryDto,
    };
    use undercurrent_sdk::ProductClientError;

    use super::*;
    use crate::{CliExecutionLimitValues, CliExecutionLimits, CliProductRequestPort};

    struct RecordingClient {
        requests: Mutex<Vec<ProductRequest>>,
        responses: Mutex<VecDeque<Result<ProductSuccess, ProductClientError>>>,
    }

    impl RecordingClient {
        fn new(
            responses: impl IntoIterator<Item = Result<ProductSuccess, ProductClientError>>,
        ) -> Self {
            Self {
                requests: Mutex::new(Vec::new()),
                responses: Mutex::new(responses.into_iter().collect()),
            }
        }
    }

    impl CliProductRequestPort for RecordingClient {
        fn request(
            &self,
            request: ProductRequest,
            _timeout: Duration,
        ) -> Result<ProductSuccess, ProductClientError> {
            self.requests.lock().expect("requests").push(request);
            self.responses
                .lock()
                .expect("responses")
                .pop_front()
                .expect("recorded response")
        }
    }

    struct ScaleClient {
        list_requests: Mutex<Vec<ListSessionsRequest>>,
        stopped: Mutex<Vec<(String, Option<String>)>>,
        pages_completed: AtomicUsize,
        mutations_started: AtomicUsize,
        mutation_started_before_snapshot: AtomicBool,
        active_mutations: AtomicUsize,
        peak_mutations: AtomicUsize,
        first_mutation_wave_size: usize,
        first_mutation_wave: Barrier,
    }

    impl ScaleClient {
        fn new(parallel_requests: usize) -> Self {
            Self {
                list_requests: Mutex::new(Vec::new()),
                stopped: Mutex::new(Vec::new()),
                pages_completed: AtomicUsize::new(0),
                mutations_started: AtomicUsize::new(0),
                mutation_started_before_snapshot: AtomicBool::new(false),
                active_mutations: AtomicUsize::new(0),
                peak_mutations: AtomicUsize::new(0),
                first_mutation_wave_size: parallel_requests,
                first_mutation_wave: Barrier::new(parallel_requests),
            }
        }
    }

    impl CliProductRequestPort for ScaleClient {
        fn request(
            &self,
            request: ProductRequest,
            _timeout: Duration,
        ) -> Result<ProductSuccess, ProductClientError> {
            match request {
                ProductRequest::ListSessions(request) => {
                    let (start, end, next_cursor) = match request.cursor.as_deref() {
                        None => (0, 128, Some("cursor-1".to_owned())),
                        Some("cursor-1") => (128, 256, Some("cursor-2".to_owned())),
                        Some("cursor-2") => (256, 300, None),
                        Some(_) => panic!("unexpected cursor"),
                    };
                    let response = SessionPageDto {
                        items: (start..end).map(summary).collect(),
                        next_cursor,
                        snapshot_revision: 77,
                        total: None,
                    };
                    self.list_requests
                        .lock()
                        .expect("list requests")
                        .push(request);
                    self.pages_completed.fetch_add(1, Ordering::AcqRel);
                    Ok(ProductSuccess::ListSessions(response))
                }
                ProductRequest::StopSession(request) => {
                    if self.pages_completed.load(Ordering::Acquire) != 3 {
                        self.mutation_started_before_snapshot
                            .store(true, Ordering::Release);
                    }
                    assert!(request.force);
                    let ordinal = self.mutations_started.fetch_add(1, Ordering::AcqRel);
                    let active = self.active_mutations.fetch_add(1, Ordering::AcqRel) + 1;
                    self.peak_mutations.fetch_max(active, Ordering::AcqRel);
                    let index = request
                        .session_id
                        .strip_prefix("session-")
                        .expect("session prefix")
                        .parse::<usize>()
                        .expect("session index");
                    self.stopped
                        .lock()
                        .expect("stopped")
                        .push((request.session_id, request.mutation.idempotency_key));
                    if ordinal < self.first_mutation_wave_size {
                        self.first_mutation_wave.wait();
                    }
                    self.active_mutations.fetch_sub(1, Ordering::AcqRel);
                    Ok(ProductSuccess::StopSession(StopSessionResultDto {
                        session: summary(index),
                        already_terminal: false,
                        exit_code: None,
                    }))
                }
                _ => panic!("unexpected product request"),
            }
        }
    }

    #[test]
    fn stop_all_snapshots_three_hundred_sessions_before_bounded_parallel_mutation() {
        const PARALLEL_REQUESTS: usize = 8;
        let client = ScaleClient::new(PARALLEL_REQUESTS);
        let executor = FiniteCliExecutor::new(
            &client,
            Duration::from_secs(2),
            CliExecutionLimits::new(CliExecutionLimitValues {
                catalog_page_size: 128,
                bulk_selection_sessions: 300,
                bulk_parallel_requests: PARALLEL_REQUESTS,
                terminal_columns: 500,
                terminal_rows: 500,
                terminal_input_items: 128,
                terminal_input_bytes: 65_536,
                terminal_output_page_bytes: 1_048_576,
            })
            .expect("limits"),
        );

        let result = executor
            .stop_selected(&StopArguments {
                name: None,
                all: true,
                label: None,
                graceful: false,
                force: true,
            })
            .expect("bulk stop");

        assert_eq!(result.selected, 300);
        assert_eq!(result.succeeded.len(), 300);
        assert!(result.failures.is_empty());
        assert!(
            result
                .succeeded
                .iter()
                .enumerate()
                .all(|(index, stopped)| stopped.session.id == format!("session-{index:03}"))
        );
        assert!(
            !client
                .mutation_started_before_snapshot
                .load(Ordering::Acquire)
        );
        assert_eq!(
            client.peak_mutations.load(Ordering::Acquire),
            PARALLEL_REQUESTS
        );

        let list_requests = client.list_requests.lock().expect("list requests");
        assert_eq!(list_requests.len(), 3);
        assert_eq!(list_requests[0].cursor, None);
        assert_eq!(list_requests[1].cursor.as_deref(), Some("cursor-1"));
        assert_eq!(list_requests[2].cursor.as_deref(), Some("cursor-2"));
        assert!(list_requests.iter().all(|request| {
            request.page_size == 128
                && request.projection == SessionProjectionDto::Compact
                && !request.include_total
        }));

        let stopped = client.stopped.lock().expect("stopped");
        assert_eq!(stopped.len(), 300);
        assert_eq!(
            stopped
                .iter()
                .filter_map(|(_, key)| key.as_deref())
                .collect::<BTreeSet<_>>()
                .len(),
            300
        );
    }

    #[test]
    fn bulk_selection_capacity_fails_before_any_mutation() {
        let client = RecordingClient::new([Ok(ProductSuccess::ListSessions(page(
            vec![summary(0), summary(1)],
            None,
        )))]);
        let executor = FiniteCliExecutor::new(&client, Duration::from_secs(2), limits(2, 1, 1));

        let error = executor
            .stop_selected(&StopArguments {
                name: None,
                all: true,
                label: None,
                graceful: false,
                force: true,
            })
            .expect_err("selection must remain bounded");

        assert!(matches!(
            &error,
            CliError::SelectionCapacity {
                command: CliCommand::Stop,
                limit: 1
            }
        ));
        assert_eq!(error.exit_code(), CommonExitCode::Incomplete);
        assert!(
            client
                .requests
                .lock()
                .expect("requests")
                .iter()
                .all(|request| matches!(request, ProductRequest::ListSessions(_)))
        );
    }

    #[test]
    fn bulk_failures_are_reported_without_cancelling_independent_sessions() {
        let responses = [
            Ok(ProductSuccess::ListSessions(page(
                vec![summary(0), summary(1)],
                None,
            ))),
            Err(ProductClientError::Server(ProductError {
                effect_certainty: None,
                operation: Some(ProductOperation::StopSession),
                category: ProductErrorCategory::FailedPrecondition,
                disposition: Some(
                    undercurrent_protocol::ProductErrorDisposition::SemanticRejection,
                ),
                message: "session cannot stop in its current state".to_owned(),
                session_id: Some("session-000".to_owned()),
                retryable: false,
                retry_after_millis: None,
                limit: None,
                resynchronization: None,
            })),
            Ok(ProductSuccess::StopSession(StopSessionResultDto {
                session: summary(1),
                already_terminal: false,
                exit_code: None,
            })),
        ];
        let client = RecordingClient::new(responses);
        let executor = FiniteCliExecutor::new(&client, Duration::from_secs(2), limits(2, 2, 1));

        let result = executor
            .stop_selected(&StopArguments {
                name: None,
                all: true,
                label: None,
                graceful: true,
                force: false,
            })
            .expect("report exists");

        assert_eq!(result.selected, 2);
        assert_eq!(result.succeeded.len(), 1);
        assert_eq!(result.failures.len(), 1);
        assert_eq!(result.failures[0].session_id, "session-000");
        assert_eq!(result.failures[0].result_category, CommonExitCode::Failure);
        assert!(!result.is_complete());
    }

    #[test]
    fn repeated_cursor_fails_before_any_mutation() {
        let client = RecordingClient::new([
            Ok(ProductSuccess::ListSessions(page(
                vec![summary(0)],
                Some("same".to_owned()),
            ))),
            Ok(ProductSuccess::ListSessions(page(
                vec![summary(1)],
                Some("same".to_owned()),
            ))),
        ]);
        let executor = FiniteCliExecutor::new(&client, Duration::from_secs(2), limits(1, 2, 1));

        let error = executor
            .clean_selected(
                &CleanArguments {
                    name: None,
                    all: true,
                    yes: true,
                },
                RemovalConfirmationDto::Forced,
            )
            .expect_err("cursor must fail");

        assert!(matches!(
            error,
            CliError::PaginationInvariant {
                command: CliCommand::Clean
            }
        ));
        assert!(
            client
                .requests
                .lock()
                .expect("requests")
                .iter()
                .all(|request| matches!(request, ProductRequest::ListSessions(_)))
        );
    }

    fn page(items: Vec<SessionSummaryDto>, next_cursor: Option<String>) -> SessionPageDto {
        SessionPageDto {
            items,
            next_cursor,
            snapshot_revision: 1,
            total: None,
        }
    }

    fn limits(page_size: u32, selection_sessions: usize, parallel: usize) -> CliExecutionLimits {
        CliExecutionLimits::new(CliExecutionLimitValues {
            catalog_page_size: page_size,
            bulk_selection_sessions: selection_sessions,
            bulk_parallel_requests: parallel,
            terminal_columns: 500,
            terminal_rows: 500,
            terminal_input_items: 128,
            terminal_input_bytes: 65_536,
            terminal_output_page_bytes: 1_048_576,
        })
        .expect("limits")
    }

    fn summary(index: usize) -> SessionSummaryDto {
        SessionSummaryDto {
            id: format!("session-{index:03}"),
            name: format!("worker-{index:03}"),
            lifecycle: LifecycleStateDto::Running,
            foreground: crate::test_foreground(
                undercurrent_protocol::ExecutionStateDto::Quiescent,
                undercurrent_protocol::InteractionStateDto::None,
            ),
            health: HealthSummaryDto {
                status: HealthStatusDto::NoCheck,
                observed_at_millis: None,
                configured_checks: 0,
            },
            work: WorkSummaryDto {
                revision: 0,
                total_items: 0,
                active_items: 0,
                total_targets: 0,
                active_targets: 0,
                aggregate_status: None,
            },
            profile: None,
            profile_context: Default::default(),
            labels: Some(BTreeMap::new()),
            started_at_millis: Some(1),
            updated_at_millis: 1,
            restart_count: 0,
            restart_status: undercurrent_protocol::RestartStatusDto::Idle,
            revision: 1,
            lifecycle_revision: 1,
        }
    }
}
