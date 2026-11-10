// SPDX-License-Identifier: Apache-2.0
//! Typed product calls and finite observation; no workflow-success inference.
use crate::{
    client::{Connection, failure, sdk_error},
    params::*,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    path::Path,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;
use undercurrent_protocol::*;
use undercurrent_sdk::*;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
type Result<T> = std::result::Result<T, Value>;

fn parse<T: DeserializeOwned>(value: Value) -> Result<T> {
    serde_json::from_value(value)
        .map_err(|_| failure("invalid_argument", "Arguments do not match the tool schema"))
}
fn value(data: impl Serialize) -> Value {
    json!(data)
}
fn mutation(operation: ProductOperation) -> MutationContext {
    let mut result = MutationContext::current(operation);
    result.idempotency_key = Some(uuid::Uuid::now_v7().to_string());
    result
}
fn bounded(size: u32) -> Result<()> {
    if !(1..=32768).contains(&size) {
        return Err(failure(
            "invalid_argument",
            "max_bytes must be between 1 and 32768",
        ));
    }
    Ok(())
}

pub fn execute(
    connection: &Connection,
    name: &str,
    args: Value,
    cancel: &CancellationToken,
) -> Result<Value> {
    // Validate and construct requests before connecting, so malformed input has no effects.
    let request = match name {
        "get_status" => {
            let _: Empty = parse(args)?;
            return match connection.get() {
                Ok(client) => {
                    match client.request(ProductRequest::GetDaemonStatus, REQUEST_TIMEOUT) {
                        Ok(ProductSuccess::GetDaemonStatus(status)) => Ok(json!({"connected":true,
                        "adapter_version":env!("CARGO_PKG_VERSION"), "home":connection.home,
                        "endpoint":connection.endpoint, "daemon":status,
                        "capabilities":client.agreement().map(|a| a.capabilities().into_iter().map(str::to_owned).collect::<Vec<_>>())})),
                        Ok(_) => Err(failure("protocol_violation", "Unexpected status response")),
                        Err(error) => Ok(
                            json!({"connected":false,"adapter_version":env!("CARGO_PKG_VERSION"),
                        "home":connection.home,"endpoint":connection.endpoint,"error":sdk_error(error)}),
                        ),
                    }
                }
                Err(error) => Ok(
                    json!({"connected":false,"adapter_version":env!("CARGO_PKG_VERSION"),
                    "home":connection.home,"endpoint":connection.endpoint,"error":error,
                    "next_action":"Start the matching daemon explicitly for this home; no daemon was started by this call."}),
                ),
            };
        }
        "list_sessions" => {
            let input: List = parse(args)?;
            if !(1..=100).contains(&input.page_size) {
                return Err(failure(
                    "invalid_argument",
                    "page_size must be between 1 and 100",
                ));
            }
            ProductRequest::ListSessions(ListSessionsRequest {
                filter: SessionFilterDto {
                    name: input.name,
                    ids: input.ids,
                    labels: input.labels,
                    ..Default::default()
                },
                page_size: input.page_size,
                cursor: input.cursor,
                sort: Default::default(),
                projection: SessionProjectionDto::WithLabels,
                include_total: true,
            })
        }
        "inspect_session" => {
            let input: Inspect = parse(args)?;
            ProductRequest::GetSessionDetail(GetSessionDetailRequest {
                session_id: input.session_id,
                projection: SessionDetailProjectionDto {
                    process: input.process,
                    foreground_evidence: input.foreground_evidence,
                    health_results: input.health_results,
                    ..Default::default()
                },
            })
        }
        "start_session" => {
            let input: Start = parse(args)?;
            if !Path::new(&input.cwd).is_absolute()
                || input.name.is_empty()
                || input.program.is_empty()
            {
                return Err(failure(
                    "invalid_argument",
                    "Start requires a unique name, program, and absolute cwd",
                ));
            }
            let environment = EnvironmentSpecDto {
                variables: input.environment,
                binding: Some(ExecutionEnvironmentBindingDto {
                    working_directory_root: input.cwd.clone(),
                    binding_digest_sha256: standalone_environment_binding_digest_sha256(&input.cwd),
                }),
                ..Default::default()
            };
            ProductRequest::StartSession(StartSessionRequest {
                launch_mode: LaunchModeDto::Native,
                mutation: mutation(ProductOperation::StartSession),
                dispatch: None,
                name: input.name,
                command: CommandSpecDto {
                    program: input.program,
                    arguments: input.arguments,
                },
                working_directory: Some(input.cwd),
                environment,
                terminal: TerminalSizeDto {
                    columns: 120,
                    rows: 40,
                },
                output_capture_mode: Default::default(),
                execution: Default::default(),
                health: Default::default(),
                profile: input.profile,
                profile_binding: Default::default(),
                labels: input.labels,
                metadata: Default::default(),
            })
        }
        "read_output" => {
            let input: Output = parse(args)?;
            bounded(input.max_bytes)?;
            let client = connection.get()?;
            return match client
                .request(
                    ProductRequest::ReadTerminalOutput(ReadTerminalOutputRequest {
                        session_id: input.session_id,
                        from_offset: input.from_offset,
                        max_bytes: input.max_bytes,
                    }),
                    REQUEST_TIMEOUT,
                )
                .map_err(sdk_error)?
            {
                ProductSuccess::ReadTerminalOutput(page) => {
                    Ok(output_page(page, input.include_bytes))
                }
                _ => Err(failure("protocol_violation", "Unexpected output response")),
            };
        }
        "read_screen" => {
            let input: Screen = parse(args)?;
            bounded(input.max_bytes)?;
            let client = connection.get()?;
            return match client
                .request(
                    ProductRequest::ReadTerminalScreen(ReadTerminalScreenRequest {
                        session_id: input.session_id,
                    }),
                    REQUEST_TIMEOUT,
                )
                .map_err(sdk_error)?
            {
                ProductSuccess::ReadTerminalScreen(screen) => {
                    let end = screen.contents.len().min(input.max_bytes as usize);
                    let bytes = &screen.contents[..end];
                    Ok(
                        json!({"output_offset":screen.output_offset,"columns":screen.columns,"rows":screen.rows,
                        "text":String::from_utf8_lossy(bytes),"encoding":"ansi_utf8", "decoding_loss":std::str::from_utf8(bytes).is_err(),
                        "truncated":end < screen.contents.len(),"total_bytes":screen.contents.len()}),
                    )
                }
                _ => Err(failure("protocol_violation", "Unexpected screen response")),
            };
        }
        "send_input" => {
            let input: Send = parse(args)?;
            if input.items.is_empty()
                || input.items.len() > 64
                || input.delivery_id.is_empty()
                || input.delivery_id.len() > 128
            {
                return Err(failure(
                    "invalid_argument",
                    "Input requires 1..64 items and a delivery_id of 1..128 bytes",
                ));
            }
            let items = input
                .items
                .into_iter()
                .map(|item| match item {
                    Input::Text { text } => TerminalInputDto::Text { text },
                    Input::Paste { text } => TerminalInputDto::Paste { text },
                    Input::Key { key } => TerminalInputDto::Key {
                        key: match key {
                            Key::Enter => KeyDto::Enter,
                            Key::Tab => KeyDto::Tab,
                            Key::Escape => KeyDto::Escape,
                            Key::Backspace => KeyDto::Backspace,
                            Key::Delete => KeyDto::Delete,
                            Key::Up => KeyDto::Up,
                            Key::Down => KeyDto::Down,
                            Key::Left => KeyDto::Left,
                            Key::Right => KeyDto::Right,
                            Key::Home => KeyDto::Home,
                            Key::End => KeyDto::End,
                            Key::PageUp => KeyDto::PageUp,
                            Key::PageDown => KeyDto::PageDown,
                            Key::CtrlC => KeyDto::Control { character: 'c' },
                            Key::CtrlD => KeyDto::Control { character: 'd' },
                        },
                    },
                })
                .collect();
            ProductRequest::SendTerminalInput(SendTerminalInputRequest {
                session_id: input.session_id,
                mutation: mutation(ProductOperation::SendTerminalInput),
                delivery_id: Some(input.delivery_id),
                expected_revision: input.expected_revision,
                items,
                observation_policy: Default::default(),
            })
        }
        "stop_session" => {
            let input: Stop = parse(args)?;
            // The Windows runtime cannot send POSIX graceful signals. Reject
            // before admission rather than leaving an unresolved stop attempt.
            #[cfg(windows)]
            if matches!(input.mode, StopMode::GracefulOnly) {
                return Err(json!({
                    "category": "unsupported_mode",
                    "message": "graceful_only is unsupported on Windows; no stop was submitted. Use application-specific shutdown or explicitly choose graceful_then_force or force.",
                    "effect_certainty": EffectCertaintyDto::KnownAbsent,
                }));
            }
            ProductRequest::StopSession(StopSessionRequest {
                session_id: input.session_id,
                mutation: mutation(ProductOperation::StopSession),
                expected_revision: input.expected_revision,
                graceful_only: matches!(input.mode, StopMode::GracefulOnly),
                force: matches!(input.mode, StopMode::Force),
            })
        }
        "wait_for_state" => {
            let input: Wait = parse(args)?;
            if !(1..=60).contains(&input.timeout_seconds)
                || input.lifecycle.is_empty()
                    && input.execution.is_empty()
                    && input.interaction.is_empty()
                    && input.health.is_empty()
            {
                return Err(failure(
                    "invalid_argument",
                    "Wait needs at least one state axis and a timeout of 1..60 seconds",
                ));
            }
            return wait(&connection.get()?, input, cancel);
        }
        _ => return Err(failure("unknown_tool", "Unknown tool")),
    };
    if cancel.is_cancelled() {
        return Err(failure("cancelled", "Cancelled before submission"));
    }
    let client = connection.get()?;
    if cancel.is_cancelled() {
        return Err(failure("cancelled", "Cancelled before submission"));
    }
    let result = client
        .request(request, REQUEST_TIMEOUT)
        .map_err(sdk_error)?;
    Ok(match result {
        ProductSuccess::ListSessions(page) => value(page),
        ProductSuccess::GetSessionDetail(detail) => value(detail),
        ProductSuccess::StartSession(receipt) => json!({"session":receipt.session}),
        ProductSuccess::SendTerminalInput(receipt) => value(receipt),
        ProductSuccess::StopSession(receipt) => value(receipt),
        _ => return Err(failure("protocol_violation", "Unexpected product response")),
    })
}

pub fn output_page(page: TerminalOutputPageDto, include_bytes: bool) -> Value {
    match page {
        TerminalOutputPageDto::Data {
            from_offset,
            to_offset,
            next_offset,
            has_more,
            bytes,
            payload_digest_sha256,
            truncation,
        } => {
            let mut result = json!({"result":"data","from_offset":from_offset,"to_offset":to_offset,
                "next_offset":next_offset,"has_more":has_more,"text":String::from_utf8_lossy(&bytes),
                "decoding_loss":std::str::from_utf8(&bytes).is_err(),"payload_digest_sha256":payload_digest_sha256,"truncation":truncation});
            if include_bytes {
                result["bytes"] = json!(bytes);
            }
            result
        }
        gap @ TerminalOutputPageDto::Gap { .. } => value(gap),
    }
}

fn matches_axis<T: Serialize>(wanted: &[T], actual: Value) -> bool {
    wanted.is_empty() || wanted.iter().any(|item| value(item) == actual)
}
fn matches(input: &Wait, summary: &SessionSummaryDto) -> bool {
    input
        .min_revision
        .is_none_or(|revision| summary.revision >= revision)
        && matches_axis(&input.lifecycle, value(summary.lifecycle)["state"].clone())
        && matches_axis(&input.execution, value(summary.foreground.execution.state))
        && matches_axis(
            &input.interaction,
            value(summary.foreground.interaction.state),
        )
        && matches_axis(&input.health, value(summary.health.status))
}

// Drop cleanup also covers timeout, cancellation, gaps, and decode errors.
struct Watch(ProductSubscription);
impl Drop for Watch {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

fn wait(client: &ProductClient, input: Wait, cancel: &CancellationToken) -> Result<Value> {
    if cancel.is_cancelled() {
        return Err(failure("cancelled", "Cancelled before observation"));
    }
    let deadline = Instant::now() + Duration::from_secs(input.timeout_seconds);
    let query = ListSessionsRequest {
        filter: SessionFilterDto {
            ids: vec![input.session_id.clone()],
            ..Default::default()
        },
        sort: Default::default(),
        projection: Default::default(),
        page_size: 1,
        cursor: None,
        include_total: false,
    };
    let opened = client
        .open_subscription(
            ProductStreamSpec::SessionWatch(SessionWatchSpec { query }),
            deadline
                .saturating_duration_since(Instant::now())
                .min(REQUEST_TIMEOUT),
        )
        .map_err(sdk_error)?;
    let watch = Watch(opened.subscription);
    let mut last: Option<SessionSummaryDto> = None;
    loop {
        if cancel.is_cancelled() {
            return Err(failure(
                "cancelled",
                "Observation cancelled; process was not stopped",
            ));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(json!({"outcome":"timeout","session":last}));
        }
        match watch
            .0
            .recv_timeout(remaining.min(Duration::from_millis(100)))
        {
            Ok(ProductSubscriptionItem::Json {
                item: ProductStreamItem::SessionSnapshot(page),
                ..
            }) => {
                last = page
                    .items
                    .into_iter()
                    .find(|item| item.id == input.session_id);
                if last.is_none() {
                    return Err(failure(
                        "not_found",
                        "Session was not found in the watch snapshot",
                    ));
                }
            }
            Ok(ProductSubscriptionItem::Json {
                item: ProductStreamItem::SessionDeltaBatch(batch),
                ..
            }) => {
                for delta in batch.deltas {
                    if delta.session_id == input.session_id {
                        if matches!(delta.kind, SessionDeltaKindDto::Removed) {
                            return Err(failure(
                                "session_removed",
                                "Session was removed during observation",
                            ));
                        }
                        if let Some(summary) = delta.summary {
                            last = Some(summary);
                        }
                    }
                }
            }
            Ok(ProductSubscriptionItem::Gap(gap)) => {
                return Err(json!({"category":"observation_gap",
                "message":"Watch continuity was lost; inspect and begin a fresh wait", "reason":gap.reason.as_str(),
                "recovery":gap.recovery.as_str(),"effect_certainty":null}));
            }
            Err(ProductClientError::Subscription(ResilientSubscriptionError::Timeout)) => continue,
            Err(error) => return Err(sdk_error(error)),
            _ => return Err(failure("protocol_violation", "Unexpected watch item")),
        }
        if let Some(summary) = last.as_ref() {
            if matches(&input, summary) {
                return Ok(json!({"outcome":"matched","session":summary}));
            }
            if matches!(
                summary.lifecycle,
                LifecycleStateDto::Stopped
                    | LifecycleStateDto::Exited { .. }
                    | LifecycleStateDto::Failed
            ) {
                return Ok(json!({"outcome":"session_ended","session":summary}));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_preserves_byte_continuity_and_split_utf8() {
        let page = TerminalOutputPageDto::Data {
            from_offset: 41,
            to_offset: 43,
            next_offset: 100,
            has_more: true,
            bytes: vec![0xf0, 0x9f],
            payload_digest_sha256: "test-digest".into(),
            truncation: None,
        };
        let result = output_page(page, true);
        assert_eq!(result["to_offset"], 43);
        assert_eq!(result["next_offset"], 100);
        assert_eq!(result["bytes"], json!([240, 159]));
        assert_eq!(result["decoding_loss"], true);
    }

    #[test]
    fn invalid_mutation_and_wait_do_not_connect_or_create_home() {
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("absent");
        let connection = Connection::new(home.clone(), 1_048_576);
        for (tool, arguments) in [
            (
                "start_session",
                json!({"name":"test","program":"sleep","cwd":"relative"}),
            ),
            (
                "send_input",
                json!({"session_id":"test","delivery_id":"test","items":[]}),
            ),
            ("wait_for_state", json!({"session_id":"test"})),
            (
                "wait_for_state",
                json!({"session_id":"test","lifecycle":["running"],"timeout_seconds":0}),
            ),
        ] {
            let error =
                execute(&connection, tool, arguments, &CancellationToken::new()).unwrap_err();
            assert_eq!(error["category"], "invalid_argument");
        }
        assert!(!home.exists());
    }

    #[test]
    fn cancellation_before_submission_does_not_connect() {
        let directory = tempfile::tempdir().unwrap();
        let connection = Connection::new(directory.path().join("absent"), 1_048_576);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let error = execute(
            &connection,
            "stop_session",
            json!({"session_id":"test","mode":"force"}),
            &cancel,
        )
        .unwrap_err();
        assert_eq!(error["category"], "cancelled");
    }

    #[cfg(windows)]
    #[test]
    fn unsupported_graceful_stop_is_rejected_before_connection() {
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("absent");
        let connection = Connection::new(home.clone(), 1_048_576);
        for arguments in [
            json!({"session_id":"test"}),
            json!({"session_id":"test","mode":"graceful_only"}),
        ] {
            let error = execute(
                &connection,
                "stop_session",
                arguments,
                &CancellationToken::new(),
            )
            .unwrap_err();
            assert_eq!(error["category"], "unsupported_mode");
            assert_eq!(error["effect_certainty"], "known_absent");
        }
        assert!(!home.exists());
    }
}
