// SPDX-License-Identifier: Apache-2.0

//! Connected finite-command dispatch over one shared product client.

use std::io::Write;
use std::time::Duration;

use crate::cli_support::CommonExitCode;
use undercurrent_protocol::{
    RemovalConfirmationDto, RepairOperationAttemptResolutionDto, RepairOperationAttemptResultDto,
    TerminalOutputPageDto,
};

use crate::{
    CleanArguments, CliError, CliExecutionLimits, CliPresenter, CliProductRequestPort, DebugAction,
    DebugArguments, FiniteCliExecutor, InspectArguments, ListArguments, PeekArguments,
    ProfileAction, ProfileArguments, SendArguments, SessionBatchResult, StartArguments,
    StartPreparationContext, StopArguments, clean_batch_text, daemon_status_text,
    diagnostic_bundle_text, diagnostic_page_text, profile_detail_text, profile_lint_text,
    profile_match_text, profile_page_text, profile_test_text, session_detail_text,
    session_mutation_text, session_page_text, shutdown_text, standard_input_closed_text,
    stop_batch_text, terminal_input_text,
};

/// One daemon-backed command with a finite response shape.
pub enum ConnectedFiniteCommand<'a> {
    /// Create one session without entering `--attach` streaming.
    Start(&'a StartArguments, &'a StartPreparationContext),
    /// Stop one exact or server-side selected session set.
    Stop(&'a StopArguments),
    /// Restart one exact named session.
    Restart(&'a crate::RestartArguments),
    /// Read one bounded compact session page.
    List(&'a ListArguments),
    /// Inspect one explicitly projected session detail.
    Inspect(&'a InspectArguments),
    /// Send one typed input sequence and optional outer-adapter stdin text.
    Send(&'a SendArguments, Option<String>),
    /// Remove one confirmed exact or server-side selected terminal session set.
    Clean(&'a CleanArguments, RemovalConfirmationDto),
    /// Read one bounded retained terminal-output page without `--follow`.
    TerminalOutput(&'a str, u64, u32),
    /// Read one current terminal screen or checkpoint diff.
    Peek(&'a PeekArguments),
    /// Read foreground evidence and WorkContext explaining one session.
    Explain(&'a str),
    /// Execute one finite detection-profile operation.
    Profile(&'a ProfileArguments),
    /// Read aggregate daemon identity and pressure.
    DaemonStatus,
    /// Request graceful daemon shutdown with the supplied duration text.
    DaemonStop(&'a str),
    /// Read one bounded operational diagnostic page.
    Debug(&'a DebugArguments),
}

/// Process-level result of one successfully dispatched finite command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectedFiniteOutcome {
    /// Stable exit category, including incomplete bulk mutation reports.
    pub exit_code: CommonExitCode,
}

impl ConnectedFiniteOutcome {
    const SUCCESS: Self = Self {
        exit_code: CommonExitCode::Success,
    };
}

/// Finite command application adapter bound to one multiplexed client.
pub struct ConnectedFiniteDispatcher<'a> {
    executor: FiniteCliExecutor<'a>,
    presenter: CliPresenter,
}

impl<'a> ConnectedFiniteDispatcher<'a> {
    /// Binds all finite command handling to one already-connected product port.
    ///
    /// This function performs no IO and does not panic.
    pub fn new(
        client: &'a dyn CliProductRequestPort,
        timeout: Duration,
        limits: CliExecutionLimits,
        presenter: CliPresenter,
    ) -> Self {
        Self {
            executor: FiniteCliExecutor::new(client, timeout, limits),
            presenter,
        }
    }

    /// Executes and presents one complete finite command.
    ///
    /// All exact-name resolution, paging, mutations, and finite reads use the
    /// same injected product port. No command handler can establish another
    /// physical connection. This method performs product-port and writer IO and
    /// does not panic unless an injected port panics.
    ///
    /// # Errors
    ///
    /// Returns typed input, product, pagination, output-shape, serialization,
    /// or writer failure. Partial bulk failures are successful reports with an
    /// [`CommonExitCode::Incomplete`] outcome rather than a discarded error.
    pub fn execute<W>(
        &self,
        command: ConnectedFiniteCommand<'_>,
        writer: &mut W,
    ) -> Result<ConnectedFiniteOutcome, CliError>
    where
        W: Write,
    {
        match command {
            ConnectedFiniteCommand::Start(arguments, context) => {
                let result = self.executor.start(arguments, context)?;
                self.presenter.write_finite(writer, &result, |_| {
                    session_mutation_text(&result, "started")
                })?;
            }
            ConnectedFiniteCommand::Stop(arguments) => {
                let result = self.executor.stop_selected(arguments)?;
                self.presenter
                    .write_finite(writer, &result, |_| stop_batch_text(&result))?;
                return Ok(batch_outcome(&result));
            }
            ConnectedFiniteCommand::Restart(arguments) => {
                let result = self
                    .executor
                    .restart(&arguments.name, !arguments.graceful)?;
                self.presenter.write_finite(writer, &result, |_| {
                    session_mutation_text(&result, "restarted")
                })?;
            }
            ConnectedFiniteCommand::List(arguments) => {
                let result = self.executor.list(arguments)?;
                self.presenter.write_finite(writer, &result, |verbosity| {
                    session_page_text(&result, verbosity)
                })?;
            }
            ConnectedFiniteCommand::Inspect(arguments) => {
                let result = self.executor.inspect(arguments)?;
                self.presenter.write_finite(writer, &result, |verbosity| {
                    session_detail_text(&result, verbosity)
                })?;
            }
            ConnectedFiniteCommand::Send(arguments, stdin_text) => {
                if arguments.eof {
                    let result = self.executor.close_standard_input(&arguments.name)?;
                    self.presenter
                        .write_finite(writer, &result, |_| standard_input_closed_text(&result))?;
                } else {
                    let result = self.executor.send(arguments, stdin_text)?;
                    self.presenter
                        .write_finite(writer, &result, |_| terminal_input_text(&result))?;
                }
            }
            ConnectedFiniteCommand::Clean(arguments, confirmation) => {
                let result = self.executor.clean_selected(arguments, confirmation)?;
                self.presenter
                    .write_finite(writer, &result, |_| clean_batch_text(&result))?;
                return Ok(batch_outcome(&result));
            }
            ConnectedFiniteCommand::TerminalOutput(name, from_offset, max_bytes) => {
                let result = self
                    .executor
                    .terminal_output(name, from_offset, max_bytes)?;
                self.presenter.write_terminal_output_page(writer, &result)?;
            }
            ConnectedFiniteCommand::Peek(arguments) => {
                if let Some(checkpoint) = &arguments.checkpoint {
                    let result = self
                        .executor
                        .screen_diff(&arguments.name, checkpoint.clone())?;
                    self.presenter
                        .write_terminal_screen_update(writer, &result, arguments.raw)?;
                } else {
                    let result = self.executor.screen(&arguments.name)?;
                    self.presenter
                        .write_terminal_screen(writer, &result, arguments.raw)?;
                }
            }
            ConnectedFiniteCommand::Explain(name) => {
                let result = self.executor.explain(name)?;
                self.presenter.write_finite(writer, &result, |verbosity| {
                    session_detail_text(&result, verbosity)
                })?;
            }
            ConnectedFiniteCommand::Profile(arguments) => {
                self.execute_profile(arguments, writer)?;
            }
            ConnectedFiniteCommand::DaemonStatus => {
                let result = self.executor.daemon_status()?;
                self.presenter
                    .write_finite(writer, &result, |_| daemon_status_text(&result))?;
            }
            ConnectedFiniteCommand::DaemonStop(drain_timeout) => {
                let result = self.executor.shutdown_daemon(drain_timeout)?;
                self.presenter
                    .write_finite(writer, &result, |_| shutdown_text(&result))?;
            }
            ConnectedFiniteCommand::Debug(arguments) => match &arguments.action {
                Some(DebugAction::Collect(_)) => {
                    return Err(crate::CliInputError::InvalidArguments.into());
                }
                Some(action @ DebugAction::Bundle { .. }) => {
                    let result = self.executor.diagnostic_bundle(action)?;
                    self.presenter
                        .write_finite(writer, &result, |_| diagnostic_bundle_text(&result))?;
                }
                Some(DebugAction::RepairAttempt { attempt_id }) => {
                    let result = self.executor.repair_operation_attempt(attempt_id.clone())?;
                    self.presenter.write_finite(writer, &result, |_| {
                        repair_operation_attempt_text(&result)
                    })?;
                }
                None => {
                    let result = self.executor.diagnostics(arguments)?;
                    self.presenter
                        .write_finite(writer, &result, |_| diagnostic_page_text(&result))?;
                }
            },
        }
        Ok(ConnectedFiniteOutcome::SUCCESS)
    }

    fn execute_profile<W>(
        &self,
        arguments: &ProfileArguments,
        writer: &mut W,
    ) -> Result<(), CliError>
    where
        W: Write,
    {
        match &arguments.action {
            ProfileAction::List(arguments) => {
                let result = self.executor.profile_list(arguments)?;
                self.presenter
                    .write_finite(writer, &result, |_| profile_page_text(&result))
            }
            ProfileAction::Inspect(arguments) => {
                let result = self.executor.profile(&arguments.name)?;
                self.presenter
                    .write_finite(writer, &result, |_| profile_detail_text(&result))
            }
            ProfileAction::Match(arguments) => {
                let result = self.executor.profile_match(arguments)?;
                self.presenter
                    .write_finite(writer, &result, |_| profile_match_text(&result))
            }
            ProfileAction::Lint(arguments) => {
                let result = self.executor.profile_lint(arguments.name.as_deref())?;
                self.presenter
                    .write_finite(writer, &result, |_| profile_lint_text(&result))
            }
            ProfileAction::Test(arguments) => {
                let result = self.executor.profile_test(arguments)?;
                self.presenter
                    .write_finite(writer, &result, |_| profile_test_text(&result))
            }
        }
    }
}

fn repair_operation_attempt_text(result: &RepairOperationAttemptResultDto) -> String {
    let resolution = match result.resolution() {
        RepairOperationAttemptResolutionDto::KnownAbsent => "known_absent".to_owned(),
        RepairOperationAttemptResolutionDto::KnownApplied {
            result_digest_sha256,
        } => format!("known_applied:{result_digest_sha256}"),
    };
    format!(
        "repaired operation attempt {} ({resolution}); unresolved={}\n",
        result.attempt_id(),
        result.unresolved_count()
    )
}

fn batch_outcome<T>(result: &SessionBatchResult<T>) -> ConnectedFiniteOutcome {
    ConnectedFiniteOutcome {
        exit_code: if result.is_complete() {
            CommonExitCode::Success
        } else {
            CommonExitCode::Incomplete
        },
    }
}

/// Returns the next offset after one finite retained-output page.
///
/// Data continues at the returned page end; gaps continue at the retained start. This
/// function performs no IO and does not panic.
pub const fn terminal_output_next_offset(page: &TerminalOutputPageDto) -> u64 {
    match page {
        TerminalOutputPageDto::Data { to_offset, .. } => *to_offset,
        TerminalOutputPageDto::Gap { available_from, .. } => *available_from,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::Mutex;

    use crate::cli_support::{CliContext, CommandTimeout, OutputFormat, Verbosity};
    use clap::Parser;
    use undercurrent_protocol::{
        CloseStandardInputResultDto, DiagnosticBundleDto, DiagnosticBundleTerminalContentDto,
        DiagnosticPageDto, HealthStatusDto, HealthSummaryDto, ProductOperation, ProductRequest,
        ProductSuccess, SessionPageDto, SessionSummaryDto, TerminalOutputPageDto, WorkSummaryDto,
    };
    use undercurrent_sdk::ProductClientError;

    use crate::{NativeCliArguments, NativeCliCommand};

    use super::*;

    #[test]
    fn output_gap_continues_at_retained_start_instead_of_producer_head() {
        let page = TerminalOutputPageDto::Gap {
            requested_offset: 0,
            available_from: 20,
            next_offset: 100,
            recovery: undercurrent_protocol::ResynchronizationDto::RestartFromAvailableOffset,
        };
        assert_eq!(terminal_output_next_offset(&page), 20);
    }

    #[test]
    fn output_continues_at_delivered_end_instead_of_producer_head() {
        let page = TerminalOutputPageDto::Data {
            from_offset: 0,
            to_offset: 4,
            next_offset: 100,
            has_more: true,
            bytes: vec![0; 4],
            payload_digest_sha256: undercurrent_protocol::execution_output_payload_digest_sha256(
                &[0; 4],
            ),
            truncation: None,
        };
        assert_eq!(terminal_output_next_offset(&page), 4);
    }

    #[test]
    fn list_dispatches_one_typed_request_and_presents_json() {
        let client = FakeClient::new([ProductSuccess::ListSessions(SessionPageDto {
            items: vec![summary()],
            next_cursor: None,
            snapshot_revision: 1,
            total: Some(1),
        })]);
        let parsed =
            NativeCliArguments::try_parse_from(["uc", "ls", "--total"]).expect("list arguments");
        let NativeCliCommand::Ls(arguments) = parsed.command else {
            panic!("expected list")
        };
        let mut output = Vec::new();

        let outcome = dispatcher(&client, OutputFormat::Json)
            .execute(ConnectedFiniteCommand::List(&arguments), &mut output)
            .expect("list dispatch");

        assert_eq!(outcome.exit_code, CommonExitCode::Success);
        assert_eq!(client.operations(), vec![ProductOperation::ListSessions]);
        let value: serde_json::Value = serde_json::from_slice(&output).expect("JSON");
        assert_eq!(value["items"][0]["name"], "worker");
    }

    #[test]
    fn diagnostic_bundle_is_redacted_by_default_and_hex_encodes_explicit_binary_content() {
        let safe_client = FakeClient::new([ProductSuccess::CreateDiagnosticBundle(
            DiagnosticBundleDto {
                diagnostics: DiagnosticPageDto {
                    items: Vec::new(),
                    next_cursor: None,
                    captured_at_millis: 1,
                },
                terminal_content: None,
                terminal_content_redacted: true,
            },
        )]);
        let safe = NativeCliArguments::try_parse_from(["uc", "debug", "bundle"])
            .expect("safe bundle arguments");
        let NativeCliCommand::Debug(safe) = safe.command else {
            panic!("expected debug bundle")
        };
        let mut safe_output = Vec::new();
        dispatcher(&safe_client, OutputFormat::Text)
            .execute(ConnectedFiniteCommand::Debug(&safe), &mut safe_output)
            .expect("safe bundle dispatch");
        assert_eq!(
            String::from_utf8(safe_output).expect("text output"),
            "no diagnostic rows\nterminal content: redacted\n"
        );
        assert_eq!(
            safe_client.operations(),
            vec![ProductOperation::CreateDiagnosticBundle]
        );

        let bytes = vec![0, 1, 255];
        let explicit_client = FakeClient::new([ProductSuccess::CreateDiagnosticBundle(
            DiagnosticBundleDto {
                diagnostics: DiagnosticPageDto {
                    items: Vec::new(),
                    next_cursor: None,
                    captured_at_millis: 2,
                },
                terminal_content: Some(DiagnosticBundleTerminalContentDto {
                    session_id: "session-1".to_owned(),
                    page: TerminalOutputPageDto::Data {
                        from_offset: 0,
                        to_offset: 3,
                        next_offset: 3,
                        has_more: false,
                        payload_digest_sha256:
                            undercurrent_protocol::execution_output_payload_digest_sha256(&bytes),
                        bytes,
                        truncation: None,
                    },
                }),
                terminal_content_redacted: false,
            },
        )]);
        let explicit = NativeCliArguments::try_parse_from([
            "uc",
            "debug",
            "bundle",
            "--include-terminal-content",
            "session-1",
        ])
        .expect("explicit bundle arguments");
        let NativeCliCommand::Debug(explicit) = explicit.command else {
            panic!("expected debug bundle")
        };
        let mut explicit_output = Vec::new();
        dispatcher(&explicit_client, OutputFormat::Text)
            .execute(
                ConnectedFiniteCommand::Debug(&explicit),
                &mut explicit_output,
            )
            .expect("explicit bundle dispatch");
        let explicit_output = String::from_utf8(explicit_output).expect("text output");
        assert!(explicit_output.contains("session=session-1"));
        assert!(explicit_output.contains("bytes_hex=0001ff"));
    }

    #[test]
    fn terminal_output_resolves_once_then_preserves_exact_bytes() {
        let page = TerminalOutputPageDto::Data {
            from_offset: 10,
            to_offset: 12,
            next_offset: 12,
            has_more: false,
            bytes: vec![0xff, b'x'],
            payload_digest_sha256: undercurrent_protocol::execution_output_payload_digest_sha256(
                &[0xff, b'x'],
            ),
            truncation: None,
        };
        let client = FakeClient::new([
            ProductSuccess::ListSessions(SessionPageDto {
                items: vec![summary()],
                next_cursor: None,
                snapshot_revision: 1,
                total: None,
            }),
            ProductSuccess::ReadTerminalOutput(page.clone()),
        ]);
        let mut output = Vec::new();

        dispatcher(&client, OutputFormat::Text)
            .execute(
                ConnectedFiniteCommand::TerminalOutput("worker", 10, 128),
                &mut output,
            )
            .expect("terminal output dispatch");

        assert_eq!(output, [0xff, b'x']);
        assert_eq!(terminal_output_next_offset(&page), 12);
        assert_eq!(
            client.operations(),
            vec![
                ProductOperation::ListSessions,
                ProductOperation::ReadTerminalOutput
            ]
        );
    }

    #[test]
    fn send_eof_resolves_once_then_dispatches_the_typed_close_operation() {
        let client = FakeClient::new([
            ProductSuccess::ListSessions(SessionPageDto {
                items: vec![summary()],
                next_cursor: None,
                snapshot_revision: 1,
                total: None,
            }),
            ProductSuccess::CloseStandardInput(CloseStandardInputResultDto {
                session_id: "session-1".to_owned(),
                closed_at_millis: 42,
                already_closed: false,
            }),
        ]);
        let parsed = NativeCliArguments::try_parse_from(["uc", "send", "worker", "--eof"])
            .expect("typed standard-input close arguments");
        let NativeCliCommand::Send(arguments) = parsed.command else {
            panic!("expected send command")
        };
        let mut output = Vec::new();

        dispatcher(&client, OutputFormat::Text)
            .execute(ConnectedFiniteCommand::Send(&arguments, None), &mut output)
            .expect("standard-input close dispatch");

        assert_eq!(
            client.operations(),
            vec![
                ProductOperation::ListSessions,
                ProductOperation::CloseStandardInput
            ]
        );
        assert_eq!(
            String::from_utf8(output).expect("text output"),
            "standard input closed for session-1\n"
        );
    }

    #[test]
    fn partial_batch_reports_map_to_incomplete_without_discarding_output() {
        let report = SessionBatchResult::<()> {
            selected: 2,
            succeeded: vec![()],
            failures: vec![crate::SessionBatchFailure {
                session_id: "session-2".to_owned(),
                result_category: CommonExitCode::Failure,
                message: "process stop failed".to_owned(),
            }],
        };

        assert_eq!(batch_outcome(&report).exit_code, CommonExitCode::Incomplete);
    }

    struct FakeClient {
        responses: Mutex<VecDeque<ProductSuccess>>,
        operations: Mutex<Vec<ProductOperation>>,
    }

    impl FakeClient {
        fn new(responses: impl IntoIterator<Item = ProductSuccess>) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().collect()),
                operations: Mutex::new(Vec::new()),
            }
        }

        fn operations(&self) -> Vec<ProductOperation> {
            self.operations.lock().expect("operations lock").clone()
        }
    }

    impl CliProductRequestPort for FakeClient {
        fn request(
            &self,
            request: ProductRequest,
            _timeout: Duration,
        ) -> Result<ProductSuccess, ProductClientError> {
            self.operations
                .lock()
                .expect("operations lock")
                .push(request.operation());
            Ok(self
                .responses
                .lock()
                .expect("responses lock")
                .pop_front()
                .expect("fake product response"))
        }
    }

    fn dispatcher<'a>(
        client: &'a FakeClient,
        output: OutputFormat,
    ) -> ConnectedFiniteDispatcher<'a> {
        ConnectedFiniteDispatcher::new(
            client,
            Duration::from_secs(1),
            CliExecutionLimits::new(crate::CliExecutionLimitValues {
                catalog_page_size: 128,
                bulk_selection_sessions: 1_000,
                bulk_parallel_requests: 4,
                terminal_columns: 500,
                terminal_rows: 500,
                terminal_input_items: 128,
                terminal_input_bytes: 65_536,
                terminal_output_page_bytes: 1_048_576,
            })
            .expect("limits"),
            CliPresenter::new(CliContext::new(
                output,
                Verbosity::Normal,
                CommandTimeout::new(Duration::from_secs(1), Duration::from_secs(1))
                    .expect("timeout"),
            )),
        )
    }

    fn summary() -> SessionSummaryDto {
        SessionSummaryDto {
            id: "session-1".to_owned(),
            name: "worker".to_owned(),
            lifecycle: undercurrent_protocol::LifecycleStateDto::Running,
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
