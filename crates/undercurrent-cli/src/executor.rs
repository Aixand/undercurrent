// SPDX-License-Identifier: Apache-2.0

//! Finite CLI use-case orchestration over one multiplexed product-client port.

mod batch;
mod operations;

pub use batch::{SessionBatchFailure, SessionBatchResult};

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use undercurrent_protocol::{
    CloseStandardInputRequest, CloseStandardInputResultDto, CommandSpecDto,
    ExecuteInSessionRequest, ExecuteInSessionResultDto, ExecutionContextDto,
    GetExecutionContextRequest, GetSessionDetailRequest, ListSessionsRequest, MutationContext,
    ProductOperation, ProductRequest, ProductSuccess, ReadTerminalScreenDiffRequest,
    ReadTerminalScreenRequest, SendTerminalInputResultDto, SessionDetailDto,
    SessionDetailProjectionDto, SessionMutationResultDto, SessionPageDto, StopSessionResultDto,
    TerminalScreenSnapshotDto, TerminalScreenUpdateDto,
};
use undercurrent_sdk::{ProductClient, ProductClientError};

use crate::{
    CliCommand, CliError, CliExecutionLimits, InspectArguments, ListArguments, SendArguments,
    StartArguments, StartPreparationContext, exact_name_lookup, prepare_list_request,
    prepare_start_request, prepare_terminal_input_request,
};

/// Finite typed product-request boundary consumed by CLI use cases.
pub trait CliProductRequestPort: Send + Sync {
    /// Returns cached negotiated daemon identity without network IO.
    fn server_epoch(&self) -> Option<String> {
        None
    }

    /// Executes one finite request on an already established shared connection.
    ///
    /// # Errors
    ///
    /// Returns the typed SDK boundary failure. Implementations must not log
    /// request payloads.
    fn request(
        &self,
        request: ProductRequest,
        timeout: Duration,
    ) -> Result<ProductSuccess, ProductClientError>;
}

impl CliProductRequestPort for ProductClient {
    fn server_epoch(&self) -> Option<String> {
        self.agreement()
            .map(|agreement| agreement.server_epoch().to_owned())
    }

    fn request(
        &self,
        request: ProductRequest,
        timeout: Duration,
    ) -> Result<ProductSuccess, ProductClientError> {
        ProductClient::request(self, request, timeout)
    }
}

/// Collision-resistant per-client semantic mutation context source.
pub struct CliMutationContextSource {
    process_id: u32,
    sequence: AtomicU64,
    instance: Option<uuid::Uuid>,
}

impl CliMutationContextSource {
    /// Creates a source using one process identity and initial sequence.
    ///
    /// This function performs no IO and does not panic. Production callers use
    /// [`Default`]; explicit construction supports deterministic tests.
    pub const fn new(process_id: u32, initial_sequence: u64) -> Self {
        Self {
            process_id,
            sequence: AtomicU64::new(initial_sequence),
            instance: None,
        }
    }

    /// Allocates one protocol-safe key without inspecting mutation content.
    pub fn next(&self, command: CliCommand, operation: ProductOperation) -> MutationContext {
        let sequence = self
            .sequence
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        let mut mutation = MutationContext::current(operation);
        mutation.idempotency_key = Some(match self.instance {
            Some(instance) => format!(
                "uc-{instance}-{}-{}-{sequence}",
                self.process_id,
                command.as_str()
            ),
            None => format!("uc-{}-{}-{sequence}", self.process_id, command.as_str()),
        });
        mutation
    }
}

impl Default for CliMutationContextSource {
    fn default() -> Self {
        Self {
            instance: Some(uuid::Uuid::now_v7()),
            ..Self::new(std::process::id(), 0)
        }
    }
}

/// Finite CLI use cases sharing one physical product connection.
pub struct FiniteCliExecutor<'a> {
    client: &'a dyn CliProductRequestPort,
    timeout: Duration,
    limits: CliExecutionLimits,
    mutations: CliMutationContextSource,
}

impl<'a> FiniteCliExecutor<'a> {
    /// Binds all finite operations to one injected product connection.
    ///
    /// This function performs no IO and does not panic.
    pub fn new(
        client: &'a dyn CliProductRequestPort,
        timeout: Duration,
        limits: CliExecutionLimits,
    ) -> Self {
        Self {
            client,
            timeout,
            limits,
            mutations: CliMutationContextSource::default(),
        }
    }

    /// Replaces mutation-key allocation for deterministic tests.
    pub fn with_mutation_source(mut self, source: CliMutationContextSource) -> Self {
        self.mutations = source;
        self
    }

    /// Creates one session after complete local request preparation.
    ///
    /// # Errors
    ///
    /// Returns local input, SDK, server, or correlation failure.
    pub fn start(
        &self,
        arguments: &StartArguments,
        context: &StartPreparationContext,
    ) -> Result<SessionMutationResultDto, CliError> {
        let request = prepare_start_request(
            arguments,
            context,
            self.mutations
                .next(CliCommand::Start, ProductOperation::StartSession),
        )?;
        expect_start(self.execute(ProductRequest::StartSession(request))?)
    }

    /// Executes one bounded server-side list query.
    ///
    /// # Errors
    ///
    /// Returns local query input, SDK, server, or correlation failure.
    pub fn list(&self, arguments: &ListArguments) -> Result<SessionPageDto, CliError> {
        let request = prepare_list_request(arguments)?;
        expect_list(self.execute(ProductRequest::ListSessions(request))?)
    }

    /// Resolves one exact name through the prepared unique-name index.
    ///
    /// # Errors
    ///
    /// Returns a typed not-found, SDK, server, or correlation failure.
    pub fn resolve_name(&self, name: &str) -> Result<String, CliError> {
        self.resolve_name_with_timeout(name, self.timeout)
    }

    fn resolve_name_with_timeout(&self, name: &str, timeout: Duration) -> Result<String, CliError> {
        let page = self.list_request_with_timeout(exact_name_lookup(name), timeout)?;
        page.items
            .into_iter()
            .next()
            .map(|summary| summary.id)
            .ok_or_else(|| CliError::SessionNotFound {
                name: name.to_owned(),
            })
    }

    /// Stops one exact named session using the same connection for lookup and mutation.
    ///
    /// # Errors
    ///
    /// Returns lookup, SDK, server, or correlation failure.
    pub fn stop(&self, name: &str, force: bool) -> Result<StopSessionResultDto, CliError> {
        let deadline = crate::deadline::CliCommandDeadline::start(CliCommand::Stop, self.timeout)?;
        let session_id = self.resolve_name_with_timeout(name, deadline.remaining()?)?;
        self.stop_id(session_id, force, deadline.remaining()?)
    }

    /// Restarts one exact named session using the same connection for lookup and mutation.
    ///
    /// # Errors
    ///
    /// Returns lookup, SDK, server, or correlation failure.
    pub fn restart(&self, name: &str, force: bool) -> Result<SessionMutationResultDto, CliError> {
        let session_id = self.resolve_name(name)?;
        let success = self.execute(ProductRequest::RestartSession(
            undercurrent_protocol::RestartSessionRequest {
                force,
                mutation: self
                    .mutations
                    .next(CliCommand::Restart, ProductOperation::RestartSession),
                session_id,
            },
        ))?;
        match success {
            ProductSuccess::RestartSession(result) => Ok(result),
            other => Err(operation_mismatch(ProductOperation::RestartSession, other)),
        }
    }

    /// Reads all authorized detail sections for one exact named session.
    ///
    /// # Errors
    ///
    /// Returns lookup, SDK, server, or correlation failure.
    pub fn inspect(&self, arguments: &InspectArguments) -> Result<SessionDetailDto, CliError> {
        self.session_detail(
            &arguments.name,
            SessionDetailProjectionDto {
                configuration: true,
                work_context: true,
                process: true,
                foreground_evidence: true,
                health_results: true,
            },
        )
    }

    fn session_detail(
        &self,
        name: &str,
        projection: SessionDetailProjectionDto,
    ) -> Result<SessionDetailDto, CliError> {
        let session_id = self.resolve_name(name)?;
        let success = self.execute(ProductRequest::GetSessionDetail(GetSessionDetailRequest {
            session_id,
            projection,
        }))?;
        match success {
            ProductSuccess::GetSessionDetail(result) => Ok(*result),
            other => Err(operation_mismatch(
                ProductOperation::GetSessionDetail,
                other,
            )),
        }
    }

    /// Resolves private environment and cwd for one exact named session.
    ///
    /// The returned DTO contains environment values and must be consumed only
    /// by the native one-shot process adapter. This method does not log or
    /// present the response.
    ///
    /// # Errors
    ///
    /// Returns lookup, SDK, server, authorization, or correlation failure.
    pub fn execution_context(&self, name: &str) -> Result<ExecutionContextDto, CliError> {
        let session_id = self.resolve_name(name)?;
        let success = self.execute(ProductRequest::GetExecutionContext(
            GetExecutionContextRequest { session_id },
        ))?;
        match success {
            ProductSuccess::GetExecutionContext(result) => Ok(result),
            other => Err(operation_mismatch(
                ProductOperation::GetExecutionContext,
                other,
            )),
        }
    }

    /// Executes one command inside the daemon-owned resolved session context.
    ///
    /// The command, output, timeout, and exit status all cross the typed product
    /// protocol; this adapter never constructs a local child process.
    pub fn execute_in_session(
        &self,
        arguments: &crate::ExecArguments,
    ) -> Result<ExecuteInSessionResultDto, CliError> {
        let deadline = crate::deadline::CliCommandDeadline::start(CliCommand::Exec, self.timeout)?;
        let session_id = self.resolve_name_with_timeout(&arguments.name, deadline.remaining()?)?;
        let remaining = deadline.remaining()?;
        let success = self.execute_with_timeout(
            ProductRequest::ExecuteInSession(ExecuteInSessionRequest {
                dir_launch: arguments.dir_launch,
                env_launch: arguments.env_launch,
                mutation: self
                    .mutations
                    .next(CliCommand::Exec, ProductOperation::ExecuteInSession),
                session_id,
                command: CommandSpecDto {
                    program: arguments.command[0].clone(),
                    arguments: arguments.command[1..].to_vec(),
                },
                output_inline_limit_bytes: self.limits.terminal_output_page_bytes(),
                deadline_millis: Some(deadline_millis(remaining)),
            }),
            remaining,
        )?;
        match success {
            ProductSuccess::ExecuteInSession(result) => Ok(result),
            other => Err(operation_mismatch(
                ProductOperation::ExecuteInSession,
                other,
            )),
        }
    }

    /// Delivers one atomic terminal input sequence to one exact named session.
    ///
    /// # Errors
    ///
    /// Returns local input, lookup, SDK, server, or correlation failure.
    pub fn send(
        &self,
        arguments: &SendArguments,
        stdin_text: Option<String>,
    ) -> Result<SendTerminalInputResultDto, CliError> {
        let session_id = self.resolve_name(&arguments.name)?;
        let request = prepare_terminal_input_request(
            arguments,
            session_id,
            stdin_text,
            self.mutations
                .next(CliCommand::Send, ProductOperation::SendTerminalInput),
        )?;
        let success = self.execute(ProductRequest::SendTerminalInput(request))?;
        match success {
            ProductSuccess::SendTerminalInput(result) => Ok(result),
            other => Err(operation_mismatch(
                ProductOperation::SendTerminalInput,
                other,
            )),
        }
    }

    /// Closes one exact session's standard-input stream idempotently.
    ///
    /// # Errors
    ///
    /// Returns lookup, SDK, server, or correlation failure.
    pub fn close_standard_input(
        &self,
        name: &str,
    ) -> Result<CloseStandardInputResultDto, CliError> {
        let session_id = self.resolve_name(name)?;
        let success = self.execute(ProductRequest::CloseStandardInput(
            CloseStandardInputRequest {
                mutation: self
                    .mutations
                    .next(CliCommand::Send, ProductOperation::CloseStandardInput),
                session_id,
            },
        ))?;
        match success {
            ProductSuccess::CloseStandardInput(result) => Ok(result),
            other => Err(operation_mismatch(
                ProductOperation::CloseStandardInput,
                other,
            )),
        }
    }

    /// Reads one current terminal screen for an exact named session.
    ///
    /// # Errors
    ///
    /// Returns lookup, SDK, server, or correlation failure.
    pub fn screen(&self, name: &str) -> Result<TerminalScreenSnapshotDto, CliError> {
        let session_id = self.resolve_name(name)?;
        let success = self.execute(ProductRequest::ReadTerminalScreen(
            ReadTerminalScreenRequest { session_id },
        ))?;
        match success {
            ProductSuccess::ReadTerminalScreen(result) => Ok(result),
            other => Err(operation_mismatch(
                ProductOperation::ReadTerminalScreen,
                other,
            )),
        }
    }

    /// Reads one bounded screen update from a prior client-scoped checkpoint.
    ///
    /// # Errors
    ///
    /// Returns lookup, SDK, server, or correlation failure.
    pub fn screen_diff(
        &self,
        name: &str,
        checkpoint: String,
    ) -> Result<TerminalScreenUpdateDto, CliError> {
        let session_id = self.resolve_name(name)?;
        let success = self.execute(ProductRequest::ReadTerminalScreenDiff(
            ReadTerminalScreenDiffRequest {
                session_id,
                checkpoint,
            },
        ))?;
        match success {
            ProductSuccess::ReadTerminalScreenDiff(result) => Ok(result),
            other => Err(operation_mismatch(
                ProductOperation::ReadTerminalScreenDiff,
                other,
            )),
        }
    }

    /// Reads foreground evidence and WorkContext needed to explain one session.
    ///
    /// # Errors
    ///
    /// Returns lookup, SDK, server, or correlation failure.
    pub fn explain(&self, name: &str) -> Result<SessionDetailDto, CliError> {
        self.session_detail(
            name,
            SessionDetailProjectionDto {
                configuration: false,
                work_context: true,
                process: false,
                foreground_evidence: true,
                health_results: true,
            },
        )
    }

    fn list_request_with_timeout(
        &self,
        request: ListSessionsRequest,
        timeout: Duration,
    ) -> Result<SessionPageDto, CliError> {
        expect_list(self.execute_with_timeout(ProductRequest::ListSessions(request), timeout)?)
    }

    fn execute(&self, request: ProductRequest) -> Result<ProductSuccess, CliError> {
        self.execute_with_timeout(request, self.timeout)
    }

    fn execute_with_timeout(
        &self,
        request: ProductRequest,
        timeout: Duration,
    ) -> Result<ProductSuccess, CliError> {
        self.client.request(request, timeout).map_err(Into::into)
    }
}

fn deadline_millis(remaining: Duration) -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(now.saturating_add(remaining.as_millis())).unwrap_or(i64::MAX)
}

fn expect_start(success: ProductSuccess) -> Result<SessionMutationResultDto, CliError> {
    match success {
        ProductSuccess::StartSession(result) => Ok(result),
        other => Err(operation_mismatch(ProductOperation::StartSession, other)),
    }
}

pub(crate) fn expect_list(success: ProductSuccess) -> Result<SessionPageDto, CliError> {
    match success {
        ProductSuccess::ListSessions(result) => Ok(result),
        other => Err(operation_mismatch(ProductOperation::ListSessions, other)),
    }
}

pub(crate) fn operation_mismatch(expected: ProductOperation, actual: ProductSuccess) -> CliError {
    CliError::Product(ProductClientError::OperationMismatch {
        expected,
        actual: Some(actual.operation()),
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn fresh_sources_do_not_reuse_attempt_keys_for_the_same_pid() {
        let first = super::CliMutationContextSource::default();
        let second = super::CliMutationContextSource::default();
        assert_eq!(first.process_id, second.process_id);
        assert_ne!(
            first
                .next(
                    crate::CliCommand::Exec,
                    undercurrent_protocol::ProductOperation::ExecuteInSession
                )
                .idempotency_key,
            second
                .next(
                    crate::CliCommand::Exec,
                    undercurrent_protocol::ProductOperation::ExecuteInSession
                )
                .idempotency_key,
        );
    }
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::Mutex;

    use clap::Parser;
    use undercurrent_protocol::{
        HealthStatusDto, HealthSummaryDto, LifecycleStateDto, SessionSummaryDto, WorkSummaryDto,
    };

    use super::*;
    use crate::{NativeCliArguments, NativeCliCommand};

    #[derive(Default)]
    struct RecordingClient {
        requests: Mutex<Vec<ProductRequest>>,
        responses: Mutex<VecDeque<ProductSuccess>>,
    }

    impl RecordingClient {
        fn with_responses(responses: impl IntoIterator<Item = ProductSuccess>) -> Self {
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
            Ok(self
                .responses
                .lock()
                .expect("responses")
                .pop_front()
                .expect("recorded response"))
        }
    }

    fn limits() -> CliExecutionLimits {
        CliExecutionLimits::new(crate::CliExecutionLimitValues {
            catalog_page_size: 100,
            bulk_selection_sessions: 1_000,
            bulk_parallel_requests: 4,
            terminal_columns: 500,
            terminal_rows: 500,
            terminal_input_items: 128,
            terminal_input_bytes: 65_536,
            terminal_output_page_bytes: 1_048_576,
        })
        .expect("CLI execution limits")
    }

    #[test]
    fn inspect_always_requests_all_sections_and_explain_keeps_its_projection() {
        for args in [
            vec!["uc", "inspect", "worker"],
            vec!["uc", "inspect", "worker", "--all"],
            vec!["uc", "inspect", "worker", "--process"],
            vec!["uc", "explain", "worker"],
        ] {
            let parsed = NativeCliArguments::try_parse_from(args).expect("valid arguments");
            let client = RecordingClient::with_responses([
                ProductSuccess::ListSessions(SessionPageDto {
                    items: vec![summary("session-1", "worker")],
                    next_cursor: None,
                    snapshot_revision: 7,
                    total: None,
                }),
                ProductSuccess::GetSessionDetail(Box::new(SessionDetailDto {
                    shell_environment: None,
                    restoration_directory: None,
                    summary: summary("session-1", "worker"),
                    configuration: None,
                    work_context: None,
                    process: None,
                    foreground_evidence: None,
                    health_results: None,
                    persisted: true,
                })),
            ]);
            let executor = FiniteCliExecutor::new(&client, Duration::from_secs(1), limits());
            let inspect = match parsed.command {
                NativeCliCommand::Inspect(arguments) => {
                    executor.inspect(&arguments).expect("inspect");
                    true
                }
                NativeCliCommand::Explain(arguments) => {
                    executor.explain(&arguments.name).expect("explain");
                    false
                }
                _ => panic!("expected inspect or explain"),
            };
            let requests = client.requests.lock().expect("requests");
            let ProductRequest::GetSessionDetail(request) = &requests[1] else {
                panic!("expected session detail")
            };
            assert_eq!(request.session_id, "session-1");
            assert_eq!(request.projection.configuration, inspect);
            assert_eq!(request.projection.process, inspect);
            assert!(request.projection.work_context);
            assert!(request.projection.foreground_evidence);
            assert!(request.projection.health_results);
        }
    }

    #[test]
    fn named_stop_uses_one_indexed_lookup_and_the_same_client_for_mutation() {
        let client = RecordingClient::with_responses([
            ProductSuccess::ListSessions(SessionPageDto {
                items: vec![summary("session-1", "worker")],
                next_cursor: None,
                snapshot_revision: 7,
                total: None,
            }),
            ProductSuccess::StopSession(StopSessionResultDto {
                session: summary("session-1", "worker"),
                already_terminal: false,
                exit_code: None,
            }),
        ]);
        let executor = FiniteCliExecutor::new(&client, Duration::from_secs(1), limits())
            .with_mutation_source(CliMutationContextSource::new(42, 0));

        let result = executor.stop("worker", true).expect("stopped");

        assert_eq!(result.session.id, "session-1");
        let requests = client.requests.lock().expect("requests");
        assert_eq!(requests.len(), 2);
        let ProductRequest::ListSessions(lookup) = &requests[0] else {
            panic!("expected lookup")
        };
        assert_eq!(lookup.filter.name.as_deref(), Some("worker"));
        assert_eq!(lookup.page_size, 1);
        let ProductRequest::StopSession(stop) = &requests[1] else {
            panic!("expected stop")
        };
        assert_eq!(stop.session_id, "session-1");
        assert!(stop.force);
        assert_eq!(
            stop.mutation.idempotency_key.as_deref(),
            Some("uc-42-stop-1")
        );
    }

    #[test]
    fn missing_exact_name_returns_typed_not_found_without_a_detail_request() {
        let client =
            RecordingClient::with_responses([ProductSuccess::ListSessions(SessionPageDto {
                items: Vec::new(),
                next_cursor: None,
                snapshot_revision: 9,
                total: None,
            })]);
        let executor = FiniteCliExecutor::new(&client, Duration::from_secs(1), limits());

        let error = executor.resolve_name("missing").expect_err("not found");

        assert!(matches!(error, CliError::SessionNotFound { .. }));
        assert_eq!(client.requests.lock().expect("requests").len(), 1);
    }

    #[test]
    fn execution_context_uses_indexed_lookup_and_private_operation() {
        let client = RecordingClient::with_responses([
            ProductSuccess::ListSessions(SessionPageDto {
                items: vec![summary("session-1", "worker")],
                next_cursor: None,
                snapshot_revision: 9,
                total: None,
            }),
            ProductSuccess::GetExecutionContext(ExecutionContextDto {
                environment: BTreeMap::from([("MODE".to_owned(), "test".to_owned())]),
                working_directory: Some("/workspace".to_owned()),
            }),
        ]);
        let executor = FiniteCliExecutor::new(&client, Duration::from_secs(1), limits());

        let context = executor.execution_context("worker").expect("context");

        assert_eq!(
            context.environment.get("MODE").map(String::as_str),
            Some("test")
        );
        assert_eq!(context.working_directory.as_deref(), Some("/workspace"));
        let requests = client.requests.lock().expect("requests");
        assert_eq!(requests.len(), 2);
        assert!(matches!(requests[0], ProductRequest::ListSessions(_)));
        let ProductRequest::GetExecutionContext(request) = &requests[1] else {
            panic!("expected execution context")
        };
        assert_eq!(request.session_id, "session-1");
    }

    #[test]
    fn start_prepares_before_issuing_exactly_one_mutating_request() {
        let parsed = NativeCliArguments::try_parse_from([
            "uc", "start", "worker", "--", "sh", "-c", "echo ok",
        ])
        .expect("valid start");
        let NativeCliCommand::Start(arguments) = parsed.command else {
            panic!("expected start")
        };
        let client = RecordingClient::with_responses([ProductSuccess::StartSession(
            SessionMutationResultDto {
                session: summary("session-1", "worker"),
                changed: true,
                executable_resolution: None,
            },
        )]);
        let executor = FiniteCliExecutor::new(&client, Duration::from_secs(1), limits())
            .with_mutation_source(CliMutationContextSource::new(7, 9));
        let context = StartPreparationContext::new(std::env::temp_dir(), None).expect("context");

        executor.start(&arguments, &context).expect("started");

        let requests = client.requests.lock().expect("requests");
        assert_eq!(requests.len(), 1);
        let ProductRequest::StartSession(start) = &requests[0] else {
            panic!("expected start")
        };
        assert_eq!(
            start.mutation.idempotency_key.as_deref(),
            Some("uc-7-start-10")
        );
    }

    #[test]
    fn mutation_keys_are_unique_across_independent_semantic_actions() {
        let source = CliMutationContextSource::new(13, 0);

        let first = source.next(CliCommand::Restart, ProductOperation::RestartSession);
        let second = source.next(CliCommand::Restart, ProductOperation::RestartSession);

        assert_ne!(first.idempotency_key, second.idempotency_key);
        assert_eq!(first.idempotency_key.as_deref(), Some("uc-13-restart-1"));
        assert_eq!(second.idempotency_key.as_deref(), Some("uc-13-restart-2"));
    }

    fn summary(id: &str, name: &str) -> SessionSummaryDto {
        SessionSummaryDto {
            id: id.to_owned(),
            name: name.to_owned(),
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
