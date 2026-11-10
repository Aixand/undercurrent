// SPDX-License-Identifier: Apache-2.0

//! Interactive target selection followed by the ordinary finite stop operation.

use std::io::Write;

use crate::session_picker::{self, Outcome, PickerView, SessionPicker};
use crate::{
    CliContext, CliError, CliExecutionLimits, CliPresenter, CliProductRequestPort,
    CliStreamCancellation, CommonExitCode, FiniteCliExecutor, InteractiveTerminalPort,
    NativeInteractiveTerminal, SessionBatchResult, stop_batch_text, stop_picker_label,
};

pub(crate) fn run<W: Write>(
    client: &dyn CliProductRequestPort,
    context: CliContext,
    limits: CliExecutionLimits,
    force: bool,
    worker_stack_bytes: usize,
    output: &mut W,
) -> Result<CommonExitCode, CliError> {
    let cancellation = CliStreamCancellation::default();
    let signal = session_picker::install_signals(cancellation.clone(), worker_stack_bytes)?;
    let picker = SessionPicker::new(client, context.timeout.get(), limits)?;
    let result = choose(
        picker,
        force,
        &mut NativeInteractiveTerminal::new(),
        output,
        &cancellation,
    );
    let finished = signal.finish();
    let selection = result?;
    finished?;
    if cancellation.is_cancelled() {
        return Ok(CommonExitCode::Interrupted);
    }
    // Waiting for human input does not consume the stop request's timeout. The
    // picker and signal worker are retired before any process mutation begins.
    complete(
        &FiniteCliExecutor::new(client, context.timeout.get(), limits),
        selection,
        context,
        force,
        output,
    )
}

fn choose<W: Write>(
    picker: SessionPicker<'_>,
    force: bool,
    terminal: &mut dyn InteractiveTerminalPort,
    output: &mut W,
    cancellation: &CliStreamCancellation,
) -> Result<Outcome<String>, CliError> {
    session_picker::choose(
        picker,
        terminal,
        output,
        cancellation,
        PickerView {
            title: if force {
                "uc stop --force · Enter: force stop · q/Esc: cancel"
            } else {
                "uc stop · Enter: stop · q/Esc: cancel"
            },
            selection_error: "Could not select session; press R to refresh.",
            label: stop_picker_label,
        },
        // Keep the catalog's stable ID: a name may be reused while the picker is open.
        |session, _| Ok(session.id.clone()),
    )
}

fn complete<W: Write>(
    executor: &FiniteCliExecutor<'_>,
    selection: Outcome<String>,
    context: CliContext,
    force: bool,
    output: &mut W,
) -> Result<CommonExitCode, CliError> {
    let session_id = match selection {
        Outcome::Selected(id) => id,
        Outcome::Exit(code) => return Ok(code),
    };
    // A failed response can follow a process effect. Surface the existing error
    // without reopening the picker or automatically repeating the mutation.
    let stopped = executor.stop_id(session_id, force, context.timeout.get())?;
    let result = SessionBatchResult {
        selected: 1,
        succeeded: vec![stopped],
        failures: Vec::new(),
    };
    CliPresenter::new(context).write_finite(output, &result, |_| stop_batch_text(&result))?;
    Ok(CommonExitCode::Success)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CliTerminalError, CliTerminalOperation, InteractiveInput, InteractiveTerminalEvent,
        OutputFormat, Verbosity,
    };
    use std::{
        collections::{BTreeMap, VecDeque},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };
    use undercurrent_protocol::{
        HealthStatusDto, HealthSummaryDto, KeyDto, LifecycleStateDto, ProductError,
        ProductErrorCategory, ProductOperation, ProductRequest, ProductSuccess, RestartStatusDto,
        SessionPageDto, SessionSummaryDto, StopSessionResultDto, TerminalInputDto, TerminalSizeDto,
        WorkSummaryDto,
    };
    use undercurrent_sdk::ProductClientError;

    struct Client {
        sessions: Vec<SessionSummaryDto>,
        requests: Mutex<Vec<ProductRequest>>,
        restored: Arc<AtomicBool>,
        failure: Option<ProductErrorCategory>,
    }

    impl Client {
        fn new(sessions: Vec<SessionSummaryDto>) -> Self {
            Self {
                sessions,
                requests: Mutex::new(Vec::new()),
                restored: Arc::new(AtomicBool::new(false)),
                failure: None,
            }
        }
    }

    impl CliProductRequestPort for Client {
        fn request(
            &self,
            request: ProductRequest,
            timeout: Duration,
        ) -> Result<ProductSuccess, ProductClientError> {
            assert_eq!(timeout, context().timeout.get());
            self.requests.lock().unwrap().push(request.clone());
            match request {
                ProductRequest::ListSessions(query) => {
                    // Terminal sessions can still have a pending restart to cancel.
                    assert!(query.filter.lifecycles.is_empty());
                    let mut page = page([]);
                    page.items = self.sessions.clone();
                    Ok(ProductSuccess::ListSessions(page))
                }
                ProductRequest::StopSession(request) => {
                    assert!(
                        self.restored.load(Ordering::Acquire),
                        "restore before mutation"
                    );
                    if let Some(category) = self.failure {
                        return Err(ProductClientError::Server(ProductError {
                            operation: Some(ProductOperation::StopSession),
                            category,
                            disposition: None,
                            effect_certainty: None,
                            message: "stop failed".into(),
                            session_id: Some(request.session_id),
                            retryable: true,
                            retry_after_millis: None,
                            limit: None,
                            resynchronization: None,
                        }));
                    }
                    let mut session = self
                        .sessions
                        .iter()
                        .find(|s| s.id == request.session_id)
                        .unwrap()
                        .clone();
                    session.lifecycle = LifecycleStateDto::Stopped;
                    Ok(ProductSuccess::StopSession(StopSessionResultDto {
                        session,
                        already_terminal: false,
                        exit_code: None,
                    }))
                }
                _ => panic!("picker must only list and stop by ID"),
            }
        }
    }

    struct Terminal {
        events: VecDeque<InteractiveTerminalEvent>,
        restored: Arc<AtomicBool>,
        fail_restore: bool,
    }

    impl InteractiveTerminalPort for Terminal {
        fn size(&mut self) -> Result<TerminalSizeDto, CliTerminalError> {
            Ok(TerminalSizeDto {
                columns: 100,
                rows: 24,
            })
        }
        fn enter_raw_mode(&mut self) -> Result<(), CliTerminalError> {
            Ok(())
        }
        fn restore(&mut self) -> Result<(), CliTerminalError> {
            self.restored.store(true, Ordering::Release);
            if self.fail_restore {
                return Err(CliTerminalError::new(
                    CliTerminalOperation::RestoreRawMode,
                    std::io::Error::other("restore failure"),
                ));
            }
            Ok(())
        }
        fn poll_event(
            &mut self,
            _: Duration,
        ) -> Result<Option<InteractiveTerminalEvent>, CliTerminalError> {
            Ok(Some(self.events.pop_front().expect("bounded picker input")))
        }
    }

    fn key(key: KeyDto) -> InteractiveTerminalEvent {
        InteractiveTerminalEvent::Input(InteractiveInput {
            item: TerminalInputDto::Key { key },
            detach_key: None,
        })
    }

    fn text(text: &str) -> InteractiveTerminalEvent {
        InteractiveTerminalEvent::Input(InteractiveInput {
            item: TerminalInputDto::Text {
                text: text.to_owned(),
            },
            detach_key: None,
        })
    }

    fn context() -> CliContext {
        CliContext::new(OutputFormat::Text, Verbosity::Normal, "1s".parse().unwrap())
    }

    fn exercise(
        client: &Client,
        terminal: &mut Terminal,
        force: bool,
        writer: &mut impl Write,
    ) -> Result<CommonExitCode, CliError> {
        let picker = SessionPicker::new(client, context().timeout.get(), limits())?;
        let choice = choose(
            picker,
            force,
            terminal,
            writer,
            &CliStreamCancellation::default(),
        )?;
        complete(
            &FiniteCliExecutor::new(client, context().timeout.get(), limits()),
            choice,
            context(),
            force,
            writer,
        )
    }

    #[test]
    fn selection_stops_the_exact_id_after_restoration_and_preserves_force() {
        for force in [false, true] {
            let first = summary(LifecycleStateDto::Running, RestartStatusDto::Idle);
            let mut selected = summary(
                LifecycleStateDto::Exited { code: Some(1) },
                RestartStatusDto::Scheduled {
                    attempt: 2,
                    not_before_millis: 100,
                },
            );
            selected.id = "selected-id".into();
            selected.name = "another-worker".into();
            let client = Client::new(vec![first, selected]);
            let mut terminal = Terminal {
                events: [key(KeyDto::Down), key(KeyDto::Enter)].into(),
                restored: client.restored.clone(),
                fail_restore: false,
            };
            let mut output = Vec::new();
            assert_eq!(
                exercise(&client, &mut terminal, force, &mut output).unwrap(),
                CommonExitCode::Success
            );
            let requests = client.requests.lock().unwrap();
            assert_eq!(requests.len(), 2);
            assert!(
                matches!(&requests[1], ProductRequest::StopSession(r) if r.session_id == "selected-id" && r.force == force && !r.graceful_only)
            );
            let output = String::from_utf8(output).unwrap();
            assert!(output.contains("exited(1) · restart scheduled (attempt 2)"));
            assert_eq!(output.contains("Enter: force stop"), force);
            assert!(
                output.find("\x1b[?1049l").unwrap()
                    < output.find("stopped another-worker").unwrap()
            );
        }
    }

    #[test]
    fn vim_navigation_only_stops_after_enter() {
        for confirm in [false, true] {
            let first = summary(LifecycleStateDto::Running, RestartStatusDto::Idle);
            let mut second = first.clone();
            second.id = "second-id".into();
            second.name = "second".into();
            let client = Client::new(vec![first, second]);
            let mut events: VecDeque<_> = ["k", "j", "k", "j", "l", "h"].map(text).into();
            events.push_back(if confirm {
                key(KeyDto::Enter)
            } else {
                text("q")
            });
            let mut terminal = Terminal {
                events,
                restored: client.restored.clone(),
                fail_restore: false,
            };
            assert_eq!(
                exercise(&client, &mut terminal, false, &mut Vec::new()).unwrap(),
                CommonExitCode::Success
            );
            let requests = client.requests.lock().unwrap();
            assert_eq!(requests.len(), if confirm { 2 } else { 1 });
            if confirm {
                assert!(matches!(&requests[1], ProductRequest::StopSession(request)
                    if request.session_id == "second-id"));
            }
            assert!(client.restored.load(Ordering::Acquire));
        }
    }

    #[test]
    fn cancellation_and_empty_selection_never_stop() {
        for populated in [false, true] {
            for (event, exit) in [
                (key(KeyDto::Escape), CommonExitCode::Success),
                (text("q"), CommonExitCode::Success),
                (
                    key(KeyDto::Control { character: 'c' }),
                    CommonExitCode::Interrupted,
                ),
            ] {
                let client = Client::new(if populated {
                    vec![summary(LifecycleStateDto::Running, RestartStatusDto::Idle)]
                } else {
                    vec![]
                });
                let mut events = VecDeque::new();
                if !populated {
                    events.push_back(key(KeyDto::Enter));
                }
                events.push_back(event);
                let mut terminal = Terminal {
                    events,
                    restored: client.restored.clone(),
                    fail_restore: false,
                };
                let mut output = Vec::new();
                assert_eq!(
                    exercise(&client, &mut terminal, false, &mut output).unwrap(),
                    exit
                );
                assert_eq!(client.requests.lock().unwrap().len(), 1);
                assert!(client.restored.load(Ordering::Acquire));
                assert!(output.ends_with(b"\x1b[?1049l"));
            }
        }
    }

    #[test]
    fn stop_errors_preserve_category_and_never_retry() {
        for category in [
            ProductErrorCategory::NotFound,
            ProductErrorCategory::TimedOut,
        ] {
            let mut client = Client::new(vec![summary(
                LifecycleStateDto::Running,
                RestartStatusDto::Idle,
            )]);
            client.failure = Some(category);
            let mut terminal = Terminal {
                events: [key(KeyDto::Enter)].into(),
                restored: client.restored.clone(),
                fail_restore: false,
            };
            let error = exercise(&client, &mut terminal, false, &mut Vec::new()).unwrap_err();
            assert_eq!(error.product_error_category(), Some(category));
            assert_eq!(client.requests.lock().unwrap().len(), 2);
        }
    }

    #[test]
    fn failed_terminal_cleanup_prevents_stop() {
        struct Writer {
            fail_leave: bool,
        }
        impl Write for Writer {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if self.fail_leave && bytes.ends_with(b"\x1b[?1049l") {
                    return Err(std::io::Error::other("leave failure"));
                }
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        for fail_leave in [false, true] {
            let client = Client::new(vec![summary(
                LifecycleStateDto::Running,
                RestartStatusDto::Idle,
            )]);
            let mut terminal = Terminal {
                events: [key(KeyDto::Enter)].into(),
                restored: client.restored.clone(),
                fail_restore: !fail_leave,
            };
            assert!(exercise(&client, &mut terminal, false, &mut Writer { fail_leave }).is_err());
            assert!(client.restored.load(Ordering::Acquire));
            assert_eq!(client.requests.lock().unwrap().len(), 1);
        }
    }
    fn limits() -> CliExecutionLimits {
        CliExecutionLimits::new(crate::CliExecutionLimitValues {
            catalog_page_size: 100,
            bulk_selection_sessions: 1_000,
            bulk_parallel_requests: 4,
            terminal_columns: 160,
            terminal_rows: 80,
            terminal_input_items: 16,
            terminal_input_bytes: 1_024,
            terminal_output_page_bytes: 128,
        })
        .expect("valid limits")
    }

    fn summary(
        lifecycle: LifecycleStateDto,
        restart_status: RestartStatusDto,
    ) -> SessionSummaryDto {
        SessionSummaryDto {
            id: "session-1".to_owned(),
            name: "worker".to_owned(),
            lifecycle,
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
            restart_status,
            revision: 1,
            lifecycle_revision: 1,
        }
    }

    fn page<const N: usize>(items: [SessionSummaryDto; N]) -> SessionPageDto {
        SessionPageDto {
            items: items.into(),
            next_cursor: None,
            snapshot_revision: 1,
            total: None,
        }
    }
}
