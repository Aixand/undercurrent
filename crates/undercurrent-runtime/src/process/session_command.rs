// SPDX-License-Identifier: Apache-2.0
//! Daemon-side execution admission, polling, and application result translation.
//!
//! A failed execute response is conservatively unknown. After acceptance,
//! explicit cancellation sends a best-effort cancel and continues observing the
//! host job; dropping the future itself does not request cancellation. A terminal
//! cancelled, expired, or output-limited job can still have known applied effects.
//! Output externalization failure does not erase that effect certainty.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use undercurrent_application::{
    ExecuteInSessionPortError, ExecuteInSessionResult, ExecutionOutput,
    ImmutableExecutionOutputStore, ObservedOperationAttemptEffect, OperationAttemptEffectObserver,
    PortError, PortErrorKind, PortFuture, PortOperation, SessionCommandExecution,
    SessionCommandExecutor,
};
use undercurrent_domain::{
    EffectCertainty, OperationAttempt, PayloadDigest, ProcessMutationKind, SessionId,
    TimestampMillis,
};

use super::HostedProcessRuntime;
use crate::process_host_protocol::{
    HostCommand, HostExecutionTerminalKind, HostFailureKind, HostResponseValue,
};

const EXECUTION_POLL_MILLIS: u64 = 25;

/// Daemon-side finite command adapter backed by the private session-host job protocol.
#[derive(Clone)]
pub struct HostedSessionCommandExecutor {
    runtime: HostedProcessRuntime,
    output_store: Arc<dyn ImmutableExecutionOutputStore>,
}

impl HostedSessionCommandExecutor {
    pub fn new(
        runtime: HostedProcessRuntime,
        output_store: Arc<dyn ImmutableExecutionOutputStore>,
    ) -> Self {
        Self {
            runtime,
            output_store,
        }
    }

    async fn map_output(
        &self,
        bytes: Vec<u8>,
        source_bytes: u64,
        inline_limit: usize,
    ) -> Result<ExecutionOutput, PortError> {
        map_execution_output(
            self.output_store.as_ref(),
            bytes,
            source_bytes,
            inline_limit,
        )
        .await
    }

    async fn acknowledge_attempt(
        &self,
        attempt: &OperationAttempt,
        operation: PortOperation,
    ) -> Result<(), PortError> {
        if attempt.kind() != ProcessMutationKind::ExecuteInSession {
            return Err(PortError::new(
                PortErrorKind::InvalidData,
                operation,
                attempt.target_session().cloned(),
                ExecutionAdapterFailure("attempt is not a retained execution job"),
            ));
        }
        let Some(session_id) = attempt.target_session() else {
            return Err(PortError::new(
                PortErrorKind::InvalidData,
                operation,
                None,
                ExecutionAdapterFailure("execution attempt has no target session"),
            ));
        };
        let response = match self
            .runtime
            .session_command_request(
                session_id,
                HostCommand::AcknowledgeExecution {
                    job_id: attempt.id().as_str().to_owned(),
                },
            )
            .await
        {
            Ok(response) => response,
            Err(error)
                if matches!(
                    error.kind(),
                    PortErrorKind::Unavailable
                        | PortErrorKind::Timeout
                        | PortErrorKind::Interrupted
                ) =>
            {
                self.runtime
                    .session_command_request(
                        session_id,
                        HostCommand::AcknowledgeExecution {
                            job_id: attempt.id().as_str().to_owned(),
                        },
                    )
                    .await?
            }
            Err(error) => return Err(error),
        };
        if matches!(response, HostResponseValue::Completed) {
            Ok(())
        } else {
            Err(PortError::new(
                PortErrorKind::ProtocolViolation,
                operation,
                Some(session_id.clone()),
                ExecutionAdapterFailure("session host rejected execution acknowledgement"),
            ))
        }
    }
}

async fn map_execution_output(
    output_store: &dyn ImmutableExecutionOutputStore,
    bytes: Vec<u8>,
    source_bytes: u64,
    inline_limit: usize,
) -> Result<ExecutionOutput, PortError> {
    if bytes.len() <= inline_limit {
        return ExecutionOutput::inline(bytes, source_bytes).map_err(|_| {
            PortError::new(
                PortErrorKind::InvalidData,
                PortOperation::ExecuteInSession,
                None,
                ExecutionAdapterFailure("execution output integrity metadata is invalid"),
            )
        });
    }
    let digest = PayloadDigest::of_bytes(&bytes);
    output_store
        .store(&bytes, digest)
        .await
        .map(ExecutionOutput::ContentReference)
}

impl SessionCommandExecutor for HostedSessionCommandExecutor {
    fn execute<'a>(
        &'a self,
        execution: SessionCommandExecution<'a>,
    ) -> PortFuture<'a, Result<ExecuteInSessionResult, ExecuteInSessionPortError>> {
        let SessionCommandExecution {
            mutation,
            session_id,
            command,
            context,
            output_limits,
            deadline,
            cancellation,
        } = execution;
        Box::pin(async move {
            if mutation.target_session() != Some(session_id) {
                return Err(execution_error(
                    PortErrorKind::InvalidData,
                    session_id,
                    EffectCertainty::KnownAbsent,
                    None,
                    "execution mutation targets a different session",
                ));
            }
            if output_limits.total_bytes()
                > self.runtime.maximum_session_command_output_bytes() as u64
            {
                return Err(execution_error(
                    PortErrorKind::CapacityExceeded,
                    session_id,
                    EffectCertainty::KnownAbsent,
                    None,
                    "execution output exceeds the private host transport bound",
                ));
            }
            if cancellation.is_cancelled() {
                return Err(execution_error(
                    PortErrorKind::Cancelled,
                    session_id,
                    EffectCertainty::KnownAbsent,
                    None,
                    "execution was cancelled before host admission",
                ));
            }
            if deadline.is_some_and(|deadline| wall_clock_millis() >= deadline.value()) {
                return Err(execution_error(
                    PortErrorKind::Timeout,
                    session_id,
                    EffectCertainty::KnownAbsent,
                    None,
                    "execution deadline elapsed before host admission",
                ));
            }

            let job_id = mutation.attempt_id().as_str().to_owned();
            let response = self
                .runtime
                .session_command_request(
                    session_id,
                    HostCommand::Execute {
                        live_environment_process: context
                            .live_environment_process()
                            .map(|p| p.as_str().to_owned()),
                        job_id: job_id.clone(),
                        session_id: session_id.as_str().to_owned(),
                        payload_digest: mutation.payload_digest().to_hex(),
                        program: command.program().to_owned(),
                        arguments: command.arguments().to_vec(),
                        environment: context
                            .environment()
                            .iter()
                            .map(|(name, value)| (name.clone(), value.clone()))
                            .collect(),
                        working_directory: context.working_directory().map(str::to_owned),
                        output_bytes: output_limits.total_bytes(),
                        deadline_millis: deadline.map(TimestampMillis::value),
                    },
                )
                .await
                .map_err(|source| {
                    ExecuteInSessionPortError::new(source, EffectCertainty::Unknown, None)
                })?;
            if !matches!(response, HostResponseValue::ExecutionAccepted) {
                return Err(execution_error(
                    PortErrorKind::ProtocolViolation,
                    session_id,
                    EffectCertainty::Unknown,
                    None,
                    "session host did not acknowledge execution",
                ));
            }

            let mut cancellation_sent = false;
            loop {
                if !cancellation_sent
                    && (cancellation.is_cancelled()
                        || deadline.is_some_and(|deadline| wall_clock_millis() >= deadline.value()))
                {
                    cancellation_sent = true;
                    let _ = self
                        .runtime
                        .session_command_request(
                            session_id,
                            HostCommand::CancelExecution {
                                job_id: job_id.clone(),
                            },
                        )
                        .await;
                }
                let response = self
                    .runtime
                    .session_command_request(
                        session_id,
                        HostCommand::PollExecution {
                            job_id: job_id.clone(),
                            wait_millis: EXECUTION_POLL_MILLIS,
                        },
                    )
                    .await
                    .map_err(|source| {
                        ExecuteInSessionPortError::new(source, EffectCertainty::Unknown, None)
                    })?;
                match response {
                    HostResponseValue::ExecutionRunning => continue,
                    HostResponseValue::ExecutionFailed {
                        kind,
                        effect_started,
                    } => {
                        return Err(execution_error(
                            port_kind(kind),
                            session_id,
                            if effect_started {
                                EffectCertainty::Unknown
                            } else {
                                EffectCertainty::KnownAbsent
                            },
                            None,
                            "session-host execution failed",
                        ));
                    }
                    HostResponseValue::ExecutionTerminal {
                        kind,
                        exit_code,
                        stdout,
                        stdout_source_bytes,
                        stderr,
                        stderr_source_bytes,
                    } => {
                        let result_digest = result_digest(
                            session_id,
                            exit_code,
                            &stdout,
                            stdout_source_bytes,
                            &stderr,
                            stderr_source_bytes,
                            kind,
                        );
                        if !matches!(kind, HostExecutionTerminalKind::Completed) {
                            let error_kind = match kind {
                                HostExecutionTerminalKind::Cancelled => PortErrorKind::Cancelled,
                                HostExecutionTerminalKind::DeadlineExceeded => {
                                    PortErrorKind::Timeout
                                }
                                HostExecutionTerminalKind::OutputLimitExceeded => {
                                    PortErrorKind::CapacityExceeded
                                }
                                HostExecutionTerminalKind::Completed => PortErrorKind::Other,
                            };
                            return Err(execution_error(
                                error_kind,
                                session_id,
                                EffectCertainty::KnownApplied,
                                Some(result_digest),
                                "session-host execution terminated before normal completion",
                            ));
                        }
                        let stdout = self
                            .map_output(stdout, stdout_source_bytes, output_limits.inline_bytes())
                            .await
                            .map_err(|source| {
                                ExecuteInSessionPortError::new(
                                    source,
                                    EffectCertainty::KnownApplied,
                                    Some(result_digest),
                                )
                            })?;
                        let stderr = self
                            .map_output(stderr, stderr_source_bytes, output_limits.inline_bytes())
                            .await
                            .map_err(|source| {
                                ExecuteInSessionPortError::new(
                                    source,
                                    EffectCertainty::KnownApplied,
                                    Some(result_digest),
                                )
                            })?;
                        return Ok(ExecuteInSessionResult {
                            session_id: session_id.clone(),
                            exit_code,
                            stdout,
                            stderr,
                            result_digest,
                        });
                    }
                    _ => {
                        return Err(execution_error(
                            PortErrorKind::ProtocolViolation,
                            session_id,
                            EffectCertainty::Unknown,
                            None,
                            "session host returned an unexpected execution response",
                        ));
                    }
                }
            }
        })
    }

    fn acknowledge<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            self.acknowledge_attempt(attempt, PortOperation::ExecuteInSession)
                .await
        })
    }
}

impl OperationAttemptEffectObserver for HostedSessionCommandExecutor {
    fn observe<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<ObservedOperationAttemptEffect, PortError>> {
        Box::pin(async move {
            if attempt.kind() != ProcessMutationKind::ExecuteInSession {
                return Ok(unknown_effect());
            }
            let Some(session_id) = attempt.target_session() else {
                return Ok(unknown_effect());
            };
            let response = match self
                .runtime
                .session_command_request(
                    session_id,
                    HostCommand::PollExecution {
                        job_id: attempt.id().as_str().to_owned(),
                        wait_millis: 0,
                    },
                )
                .await
            {
                Ok(response) => response,
                Err(_) => return Ok(unknown_effect()),
            };
            Ok(match response {
                HostResponseValue::ExecutionRunning => unknown_effect(),
                HostResponseValue::ExecutionFailed {
                    effect_started: false,
                    ..
                } => ObservedOperationAttemptEffect {
                    certainty: EffectCertainty::KnownAbsent,
                    result_digest: None,
                },
                HostResponseValue::ExecutionFailed {
                    effect_started: true,
                    ..
                } => unknown_effect(),
                HostResponseValue::ExecutionTerminal {
                    kind,
                    exit_code,
                    stdout,
                    stdout_source_bytes,
                    stderr,
                    stderr_source_bytes,
                } => ObservedOperationAttemptEffect {
                    certainty: EffectCertainty::KnownApplied,
                    result_digest: Some(result_digest(
                        session_id,
                        exit_code,
                        &stdout,
                        stdout_source_bytes,
                        &stderr,
                        stderr_source_bytes,
                        kind,
                    )),
                },
                _ => return Err(observer_protocol_error(session_id)),
            })
        })
    }

    fn acknowledge_observation<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            self.acknowledge_attempt(attempt, PortOperation::ObserveOperationAttemptEffect)
                .await
        })
    }
}

const fn unknown_effect() -> ObservedOperationAttemptEffect {
    ObservedOperationAttemptEffect {
        certainty: EffectCertainty::Unknown,
        result_digest: None,
    }
}

fn observer_protocol_error(session_id: &SessionId) -> PortError {
    PortError::new(
        PortErrorKind::ProtocolViolation,
        PortOperation::ObserveOperationAttemptEffect,
        Some(session_id.clone()),
        ExecutionAdapterFailure("session host returned an unexpected observation response"),
    )
}

fn result_digest(
    session_id: &SessionId,
    exit_code: Option<i32>,
    stdout: &[u8],
    stdout_source_bytes: u64,
    stderr: &[u8],
    stderr_source_bytes: u64,
    kind: HostExecutionTerminalKind,
) -> PayloadDigest {
    let exit_code = exit_code.unwrap_or(i32::MIN).to_be_bytes();
    let stdout_length = stdout_source_bytes.to_be_bytes();
    let stderr_length = stderr_source_bytes.to_be_bytes();
    let kind = [match kind {
        HostExecutionTerminalKind::Completed => 1,
        HostExecutionTerminalKind::Cancelled => 2,
        HostExecutionTerminalKind::DeadlineExceeded => 3,
        HostExecutionTerminalKind::OutputLimitExceeded => 4,
    }];
    PayloadDigest::of_slices([
        b"undercurrent-execute-result-v0.1.0-beta".as_slice(),
        session_id.as_str().as_bytes(),
        &exit_code,
        &kind,
        &stdout_length,
        stdout,
        &stderr_length,
        stderr,
    ])
}

fn port_kind(kind: HostFailureKind) -> PortErrorKind {
    kind.into()
}

fn execution_error(
    kind: PortErrorKind,
    session_id: &SessionId,
    certainty: EffectCertainty,
    result_digest: Option<PayloadDigest>,
    message: &'static str,
) -> ExecuteInSessionPortError {
    ExecuteInSessionPortError::new(
        PortError::new(
            kind,
            PortOperation::ExecuteInSession,
            Some(session_id.clone()),
            ExecutionAdapterFailure(message),
        ),
        certainty,
        result_digest,
    )
}

fn wall_clock_millis() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

#[derive(Debug)]
struct ExecutionAdapterFailure(&'static str);

impl Display for ExecutionAdapterFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for ExecutionAdapterFailure {}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Mutex;
    use std::task::{Context, Poll, Wake, Waker};

    use undercurrent_application::{ImmutableExecutionOutputRef, PortFuture};

    use super::*;

    #[test]
    fn output_mapping_keeps_binary_inline_and_references_complete_overflow() {
        let store = RecordingOutputStore::default();
        let inline =
            block_on(map_execution_output(&store, vec![0, 0xff, 1], 3, 3)).expect("inline output");
        assert_eq!(inline.inline_bytes(), Some([0, 0xff, 1].as_slice()));
        assert_eq!(inline.integrity().expect("integrity").source_bytes(), 3);

        let referenced_bytes = vec![0xfe, 0, 2, 3];
        let referenced = block_on(map_execution_output(
            &store,
            referenced_bytes.clone(),
            referenced_bytes.len() as u64,
            2,
        ))
        .expect("referenced output");
        let ExecutionOutput::ContentReference(reference) = referenced else {
            panic!("large complete output was not referenced");
        };
        assert_eq!(reference.uri(), "memory://execution-output");
        assert_eq!(reference.size_bytes(), 4);
        assert_eq!(
            store
                .bytes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_slice(),
            referenced_bytes
        );
    }

    #[derive(Default)]
    struct RecordingOutputStore {
        bytes: Mutex<Vec<u8>>,
    }

    impl ImmutableExecutionOutputStore for RecordingOutputStore {
        fn store<'a>(
            &'a self,
            bytes: &'a [u8],
            digest: PayloadDigest,
        ) -> PortFuture<'a, Result<ImmutableExecutionOutputRef, PortError>> {
            Box::pin(async move {
                *self
                    .bytes
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = bytes.to_vec();
                ImmutableExecutionOutputRef::new(
                    "memory://execution-output",
                    digest,
                    bytes.len() as u64,
                )
                .map_err(|_| {
                    PortError::new(
                        PortErrorKind::InvalidData,
                        PortOperation::StoreExecutionOutput,
                        None,
                        ExecutionAdapterFailure("invalid test output reference"),
                    )
                })
            })
        }
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        struct ThreadWake(std::thread::Thread);

        impl Wake for ThreadWake {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }

            fn wake_by_ref(self: &Arc<Self>) {
                self.0.unpark();
            }
        }

        let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(future);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(value) => return value,
                Poll::Pending => std::thread::park(),
            }
        }
    }
}
