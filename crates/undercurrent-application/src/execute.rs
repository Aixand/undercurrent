// SPDX-License-Identifier: Apache-2.0

//! Daemon-supervised one-shot command execution within a session context.

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

use undercurrent_domain::{
    CommandSpec, EffectCertainty, OperationAttempt, OperationAttemptState, OutputChunkIntegrity,
    PayloadDigest, SessionId, TimestampMillis,
};

use crate::{
    ApplicationErrorCategory, AuthorizationPort, Clock, ExecutionContextError,
    ExecutionContextResolver, OperationAttemptAcceptance, OperationAttemptAdmission,
    OperationAttemptReconciliation, OperationAttemptSettlement, OperationAttemptSettlementOutcome,
    OperationAttemptStore, PortError, PortFuture, ProcessMutationContext,
    ResolveExecutionContextService, ResolvedExecutionContext, SessionRepository, Telemetry,
};

/// Validated finite-output admission limits for one command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionOutputLimits {
    inline_bytes: usize,
    total_bytes: u64,
}

impl ExecutionOutputLimits {
    /// Constructs non-zero limits with inline output bounded by total output.
    pub fn new(
        inline_bytes: usize,
        total_bytes: u64,
    ) -> Result<Self, InvalidExecutionOutputLimits> {
        if inline_bytes == 0
            || total_bytes == 0
            || u64::try_from(inline_bytes)
                .ok()
                .is_none_or(|inline| inline > total_bytes)
        {
            return Err(InvalidExecutionOutputLimits);
        }
        Ok(Self {
            inline_bytes,
            total_bytes,
        })
    }

    /// Returns maximum bytes in a finite response.
    pub const fn inline_bytes(self) -> usize {
        self.inline_bytes
    }
    /// Returns maximum admitted output before termination/truncation policy.
    pub const fn total_bytes(self) -> u64 {
        self.total_bytes
    }
}

/// Invalid command-output limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidExecutionOutputLimits;

impl Display for InvalidExecutionOutputLimits {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("execution output limits must be non-zero and internally bounded")
    }
}

impl Error for InvalidExecutionOutputLimits {}

/// Cooperative cancellation observed by a session-command adapter.
///
/// This application-owned abstraction prevents an async-runtime cancellation
/// token from leaking into the inward-facing port.
pub trait OperationCancellation: Send + Sync {
    /// Returns whether cancellation was already requested.
    fn is_cancelled(&self) -> bool;

    /// Resolves when cancellation is requested.
    fn cancelled(&self) -> PortFuture<'_, ()>;
}

/// Immutable complete output reference owned by a configured content adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImmutableExecutionOutputRef {
    uri: String,
    digest: PayloadDigest,
    size_bytes: u64,
}

impl ImmutableExecutionOutputRef {
    /// Constructs a non-empty immutable reference with exact digest and size.
    pub fn new(
        uri: impl Into<String>,
        digest: PayloadDigest,
        size_bytes: u64,
    ) -> Result<Self, InvalidExecutionOutputReference> {
        let uri = uri.into();
        if uri.is_empty() || uri.len() > 2048 || uri.chars().any(char::is_control) {
            return Err(InvalidExecutionOutputReference);
        }
        Ok(Self {
            uri,
            digest,
            size_bytes,
        })
    }

    /// Returns opaque immutable URI.
    pub fn uri(&self) -> &str {
        &self.uri
    }
    /// Returns complete content digest.
    pub const fn digest(&self) -> PayloadDigest {
        self.digest
    }
    /// Returns complete content size.
    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
}

/// Invalid immutable output reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidExecutionOutputReference;

/// Persists complete execution output outside a bounded finite response.
pub trait ImmutableExecutionOutputStore: Send + Sync {
    /// Stores exactly `bytes` under the supplied verified digest.
    fn store<'a>(
        &'a self,
        bytes: &'a [u8],
        digest: PayloadDigest,
    ) -> PortFuture<'a, Result<ImmutableExecutionOutputRef, PortError>>;
}

/// Bounded command output without diagnostic payload exposure.
#[derive(Clone, PartialEq, Eq)]
pub enum ExecutionOutput {
    /// Bounded bytes carried by the finite result.
    Inline {
        bytes: Vec<u8>,
        integrity: OutputChunkIntegrity,
    },
    /// Complete immutable output stored outside the finite response.
    ContentReference(ImmutableExecutionOutputRef),
    /// Bounded resumable logical stream for live or large output.
    Stream {
        stream_id: String,
        available_from: u64,
        next_offset: u64,
    },
}

impl ExecutionOutput {
    /// Constructs verified bounded inline output.
    pub fn inline(bytes: Vec<u8>, source_bytes: u64) -> Result<Self, InvalidExecutionOutput> {
        let integrity = OutputChunkIntegrity::for_payload(&bytes, source_bytes)
            .map_err(|_| InvalidExecutionOutput)?;
        Ok(Self::Inline { bytes, integrity })
    }

    /// Returns inline bytes only to an authorized presentation adapter.
    pub fn inline_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Inline { bytes, .. } => Some(bytes),
            Self::ContentReference(_) | Self::Stream { .. } => None,
        }
    }

    /// Returns inline integrity evidence.
    pub const fn integrity(&self) -> Option<OutputChunkIntegrity> {
        match self {
            Self::Inline { integrity, .. } => Some(*integrity),
            Self::ContentReference(_) | Self::Stream { .. } => None,
        }
    }
}

impl Debug for ExecutionOutput {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inline { bytes, integrity } => formatter
                .debug_struct("InlineExecutionOutput")
                .field("byte_len", &bytes.len())
                .field("integrity", integrity)
                .finish_non_exhaustive(),
            Self::ContentReference(reference) => Debug::fmt(reference, formatter),
            Self::Stream {
                stream_id,
                available_from,
                next_offset,
            } => formatter
                .debug_struct("StreamExecutionOutput")
                .field("stream_id", stream_id)
                .field("available_from", available_from)
                .field("next_offset", next_offset)
                .finish(),
        }
    }
}

/// Invalid execution output or integrity metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidExecutionOutput;

/// Transport-neutral command accepted by the daemon application boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteInSessionCommand {
    /// Select the saved launch directory instead of the observed live directory.
    pub dir_launch: bool,
    /// Use launch environment settings instead of the latest shell exports.
    pub env_launch: bool,
    /// Durable process mutation context.
    pub mutation: OperationAttemptAdmission,
    /// Stable target session.
    pub session_id: SessionId,
    /// Validated executable and argv.
    pub command: CommandSpec,
    /// Bounded output policy.
    pub output_limits: ExecutionOutputLimits,
    /// Absolute product-clock deadline, when the caller supplied one.
    pub deadline: Option<TimestampMillis>,
}

/// Confirmed command result with bounded or referenced output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteInSessionResult {
    /// Stable target session.
    pub session_id: SessionId,
    /// Portable exit code when normal termination was observed.
    pub exit_code: Option<i32>,
    /// Standard output.
    pub stdout: ExecutionOutput,
    /// Standard error.
    pub stderr: ExecutionOutput,
    /// Digest covering the complete semantic result.
    pub result_digest: PayloadDigest,
}

/// Process adapter failure retaining explicit side-effect certainty.
pub struct ExecuteInSessionPortError {
    source: PortError,
    certainty: EffectCertainty,
    result_digest: Option<PayloadDigest>,
}

impl ExecuteInSessionPortError {
    /// Constructs a failure without converting ambiguity into absence.
    pub const fn new(
        source: PortError,
        certainty: EffectCertainty,
        result_digest: Option<PayloadDigest>,
    ) -> Self {
        Self {
            source,
            certainty,
            result_digest,
        }
    }

    /// Returns adapter error.
    pub const fn source_error(&self) -> &PortError {
        &self.source
    }
    /// Returns effect certainty.
    pub const fn certainty(&self) -> EffectCertainty {
        self.certainty
    }
    /// Returns exact partial/terminal result digest when available.
    pub const fn result_digest(&self) -> Option<PayloadDigest> {
        self.result_digest
    }
    /// Consumes the wrapper into its adapter error.
    pub fn into_source(self) -> PortError {
        self.source
    }
}

impl Debug for ExecuteInSessionPortError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecuteInSessionPortError")
            .field("source", &self.source)
            .field("certainty", &self.certainty)
            .field("result_digest", &self.result_digest)
            .finish()
    }
}

/// Borrowed, transport-neutral input to one supervised command execution.
#[derive(Clone, Copy)]
pub struct SessionCommandExecution<'a> {
    /// Durable mutation context carrying attempt identity and fence.
    pub mutation: &'a ProcessMutationContext,
    /// Stable target session.
    pub session_id: &'a SessionId,
    /// Validated executable and arguments.
    pub command: &'a CommandSpec,
    /// Resolved private execution context.
    pub context: &'a ResolvedExecutionContext,
    /// Bounded output policy.
    pub output_limits: ExecutionOutputLimits,
    /// Absolute product-clock deadline, when supplied.
    pub deadline: Option<TimestampMillis>,
    /// Cooperative application-owned cancellation.
    pub cancellation: &'a dyn OperationCancellation,
}

impl<'a> SessionCommandExecution<'a> {
    /// Borrows one accepted command and its resolved private execution context.
    pub const fn new(
        mutation: &'a ProcessMutationContext,
        request: &'a ExecuteInSessionCommand,
        context: &'a ResolvedExecutionContext,
        cancellation: &'a dyn OperationCancellation,
    ) -> Self {
        Self {
            mutation,
            session_id: &request.session_id,
            command: &request.command,
            context,
            output_limits: request.output_limits,
            deadline: request.deadline,
            cancellation,
        }
    }
}

/// Daemon-owned adapter that supervises one bounded command execution.
pub trait SessionCommandExecutor: Send + Sync {
    /// Executes using resolved private context without exposing process handles.
    fn execute<'a>(
        &'a self,
        execution: SessionCommandExecution<'a>,
    ) -> PortFuture<'a, Result<ExecuteInSessionResult, ExecuteInSessionPortError>>;

    /// Releases a retained terminal job only after its product attempt is durable.
    ///
    /// Implementations with retained host results must make this idempotent.
    fn acknowledge<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecuteSettlementMode {
    Settle,
    ReconcileUnknown,
    ReplayKnownApplied(PayloadDigest),
}

/// Failure from daemon-supervised execution.
#[derive(Debug)]
pub enum ExecuteInSessionError {
    /// Caller cancelled before the operation was durably accepted.
    Cancelled,
    /// Caller deadline elapsed before the operation was durably accepted.
    DeadlineExceeded,
    /// Authorization, session lookup, or private context resolution failed.
    Context(ExecutionContextError),
    /// Attempt persistence failed.
    AttemptPersistence(PortError),
    /// Duplicate, stale, or conflicting attempt requires explicit reconciliation.
    AttemptRejected,
    /// Execution adapter failed with preserved certainty.
    Execution(ExecuteInSessionPortError),
    /// Effect returned but its settlement could not be made durable.
    Settlement(PortError),
}

impl ExecuteInSessionError {
    /// Returns the stable category for outward mapping.
    pub const fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::Cancelled => ApplicationErrorCategory::Cancelled,
            Self::DeadlineExceeded => ApplicationErrorCategory::Timeout,
            Self::Context(error) => error.category(),
            Self::AttemptPersistence(error) | Self::Settlement(error) => error.category(),
            Self::AttemptRejected => ApplicationErrorCategory::Conflict,
            Self::Execution(error) => error.source_error().category(),
        }
    }
}

/// Orchestrates context resolution, durable attempt state, and daemon execution.
pub struct ExecuteInSessionService<'a> {
    context: ResolveExecutionContextService<'a>,
    clock: &'a dyn Clock,
    attempts: &'a dyn OperationAttemptStore,
    executor: &'a dyn SessionCommandExecutor,
}

impl<'a> ExecuteInSessionService<'a> {
    /// Constructs the use case without performing IO.
    pub const fn new(
        authorizer: &'a dyn AuthorizationPort,
        repository: &'a dyn SessionRepository,
        resolver: &'a dyn ExecutionContextResolver,
        clock: &'a dyn Clock,
        telemetry: &'a dyn Telemetry,
        attempts: &'a dyn OperationAttemptStore,
        executor: &'a dyn SessionCommandExecutor,
    ) -> Self {
        Self {
            context: ResolveExecutionContextService::new(
                authorizer, repository, resolver, clock, telemetry,
            ),
            clock,
            attempts,
            executor,
        }
    }

    /// Executes only after authorization, context resolution, and durable acceptance.
    pub async fn execute(
        &self,
        command: &ExecuteInSessionCommand,
        catalog: &crate::PreparedSessionCatalog,
        cancellation: &dyn OperationCancellation,
    ) -> Result<ExecuteInSessionResult, ExecuteInSessionError> {
        if cancellation.is_cancelled() {
            return Err(ExecuteInSessionError::Cancelled);
        }
        if command
            .deadline
            .is_some_and(|deadline| self.clock.now() >= deadline)
        {
            return Err(ExecuteInSessionError::DeadlineExceeded);
        }
        let context = self
            .context
            .resolve_for_execution(
                &command.session_id,
                catalog,
                command.dir_launch,
                command.env_launch,
            )
            .await
            .map_err(ExecuteInSessionError::Context)?;
        let (attempt, settlement_mode) = match self
            .attempts
            .accept_next(&command.mutation)
            .await
            .map_err(ExecuteInSessionError::AttemptPersistence)?
        {
            OperationAttemptAcceptance::Accepted(attempt)
            | OperationAttemptAcceptance::Duplicate(attempt) => {
                (attempt, ExecuteSettlementMode::Settle)
            }
            OperationAttemptAcceptance::AlreadySettled(attempt)
                if matches!(attempt.state(), OperationAttemptState::Settled { .. }) =>
            {
                let mode = match attempt.state() {
                    OperationAttemptState::Settled {
                        certainty: EffectCertainty::KnownApplied,
                        result_digest: Some(digest),
                        ..
                    } => ExecuteSettlementMode::ReplayKnownApplied(*digest),
                    OperationAttemptState::Settled {
                        certainty: EffectCertainty::Unknown,
                        ..
                    } => ExecuteSettlementMode::ReconcileUnknown,
                    OperationAttemptState::Accepted
                    | OperationAttemptState::Compensated { .. }
                    | OperationAttemptState::Settled {
                        certainty: EffectCertainty::KnownAbsent,
                        ..
                    }
                    | OperationAttemptState::Settled {
                        certainty: EffectCertainty::KnownApplied,
                        result_digest: None,
                        ..
                    } => return Err(ExecuteInSessionError::AttemptRejected),
                };
                (attempt, mode)
            }
            OperationAttemptAcceptance::AlreadySettled(_)
            | OperationAttemptAcceptance::AlreadyCompensated(_)
            | OperationAttemptAcceptance::StaleFence { .. }
            | OperationAttemptAcceptance::PayloadConflict => {
                return Err(ExecuteInSessionError::AttemptRejected);
            }
        };
        let mutation = ProcessMutationContext::from_attempt(&attempt);

        match self
            .executor
            .execute(SessionCommandExecution::new(
                &mutation,
                command,
                &context,
                cancellation,
            ))
            .await
        {
            Ok(result) => {
                if let ExecuteSettlementMode::ReplayKnownApplied(expected) = settlement_mode {
                    if result.result_digest != expected {
                        return Err(ExecuteInSessionError::AttemptRejected);
                    }
                    self.executor
                        .acknowledge(&attempt)
                        .await
                        .map_err(ExecuteInSessionError::Settlement)?;
                    return Ok(result);
                }
                let settlement = OperationAttemptSettlement {
                    attempt_id: mutation.attempt_id().clone(),
                    fencing_token: mutation.fencing_token(),
                    certainty: EffectCertainty::KnownApplied,
                    result_digest: Some(result.result_digest),
                    settled_at: self.clock.now(),
                };
                let outcome = if settlement_mode == ExecuteSettlementMode::ReconcileUnknown {
                    self.attempts
                        .reconcile(&OperationAttemptReconciliation {
                            attempt_id: settlement.attempt_id,
                            fencing_token: settlement.fencing_token,
                            certainty: settlement.certainty,
                            result_digest: settlement.result_digest,
                            reconciled_at: settlement.settled_at,
                        })
                        .await
                } else {
                    self.attempts.settle(&settlement).await
                };
                acknowledge_settlement(self.executor, outcome)
                    .await
                    .map_err(ExecuteInSessionError::Settlement)?;
                Ok(result)
            }
            Err(error) => {
                let settlement = OperationAttemptSettlement {
                    attempt_id: mutation.attempt_id().clone(),
                    fencing_token: mutation.fencing_token(),
                    certainty: error.certainty(),
                    result_digest: error.result_digest(),
                    settled_at: self.clock.now(),
                };
                if matches!(
                    settlement_mode,
                    ExecuteSettlementMode::ReplayKnownApplied(_)
                ) {
                    return Err(ExecuteInSessionError::Execution(error));
                }
                if !(settlement_mode == ExecuteSettlementMode::ReconcileUnknown
                    && settlement.certainty == EffectCertainty::Unknown)
                {
                    let outcome = if settlement_mode == ExecuteSettlementMode::ReconcileUnknown {
                        self.attempts
                            .reconcile(&OperationAttemptReconciliation {
                                attempt_id: settlement.attempt_id,
                                fencing_token: settlement.fencing_token,
                                certainty: settlement.certainty,
                                result_digest: settlement.result_digest,
                                reconciled_at: settlement.settled_at,
                            })
                            .await
                    } else {
                        self.attempts.settle(&settlement).await
                    };
                    if settlement.certainty == EffectCertainty::Unknown {
                        require_settlement(outcome).map_err(ExecuteInSessionError::Settlement)?;
                    } else {
                        acknowledge_settlement(self.executor, outcome)
                            .await
                            .map_err(ExecuteInSessionError::Settlement)?;
                    }
                }
                Err(ExecuteInSessionError::Execution(error))
            }
        }
    }
}

fn require_settlement(
    outcome: Result<OperationAttemptSettlementOutcome, PortError>,
) -> Result<OperationAttempt, PortError> {
    match outcome? {
        OperationAttemptSettlementOutcome::Settled(attempt)
        | OperationAttemptSettlementOutcome::Duplicate(attempt) => Ok(attempt),
        OperationAttemptSettlementOutcome::StaleFence { .. }
        | OperationAttemptSettlementOutcome::NotFound
        | OperationAttemptSettlementOutcome::PayloadConflict => Err(PortError::new(
            crate::PortErrorKind::Conflict,
            crate::PortOperation::SettleOperationAttempt,
            None,
            SettlementRejected,
        )),
    }
}

async fn acknowledge_settlement(
    executor: &dyn SessionCommandExecutor,
    outcome: Result<OperationAttemptSettlementOutcome, PortError>,
) -> Result<(), PortError> {
    let committed = require_settlement(outcome)?;
    executor.acknowledge(&committed).await
}

#[derive(Debug)]
struct SettlementRejected;

impl Display for SettlementRejected {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("operation attempt settlement was rejected")
    }
}

impl Error for SettlementRejected {}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};
    use std::thread::{self, Thread};

    use undercurrent_domain::{FencingToken, OperationAttemptId, ProcessMutationKind};

    use super::*;

    #[test]
    fn retained_execution_is_acknowledged_only_for_a_committed_settlement() {
        let executor = RecordingExecutor::default();
        let committed = settled_attempt();

        block_on(acknowledge_settlement(
            &executor,
            Ok(OperationAttemptSettlementOutcome::Settled(committed)),
        ))
        .expect("durable settlement is acknowledged");
        assert_eq!(executor.acknowledgements.load(Ordering::SeqCst), 1);

        assert!(
            block_on(acknowledge_settlement(
                &executor,
                Ok(OperationAttemptSettlementOutcome::NotFound),
            ))
            .is_err()
        );
        assert_eq!(executor.acknowledgements.load(Ordering::SeqCst), 1);
    }

    fn settled_attempt() -> OperationAttempt {
        let digest = PayloadDigest::of_bytes(b"terminal-result");
        let mut attempt = OperationAttempt::accepted(
            OperationAttemptId::new("execute-ack-test").unwrap(),
            Some(SessionId::new("session-1").unwrap()),
            ProcessMutationKind::ExecuteInSession,
            FencingToken::new(1).unwrap(),
            PayloadDigest::of_bytes(b"command"),
            TimestampMillis::new(1),
        );
        attempt
            .settle(
                FencingToken::new(1).unwrap(),
                EffectCertainty::KnownApplied,
                Some(digest),
                TimestampMillis::new(2),
            )
            .unwrap();
        attempt
    }

    #[derive(Default)]
    struct RecordingExecutor {
        acknowledgements: AtomicUsize,
    }

    impl SessionCommandExecutor for RecordingExecutor {
        fn execute<'a>(
            &'a self,
            _execution: SessionCommandExecution<'a>,
        ) -> PortFuture<'a, Result<ExecuteInSessionResult, ExecuteInSessionPortError>> {
            Box::pin(async { panic!("execute is not part of this settlement-order test") })
        }

        fn acknowledge<'a>(
            &'a self,
            _attempt: &'a OperationAttempt,
        ) -> PortFuture<'a, Result<(), PortError>> {
            Box::pin(async move {
                self.acknowledgements.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let mut future = Box::pin(future);
        let parker = ThreadWake(thread::current());
        let waker = Waker::from(Arc::new(parker));
        let mut context = Context::from_waker(&waker);
        loop {
            match Pin::as_mut(&mut future).poll(&mut context) {
                Poll::Ready(output) => return output,
                Poll::Pending => thread::park(),
            }
        }
    }

    struct ThreadWake(Thread);

    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }
}
