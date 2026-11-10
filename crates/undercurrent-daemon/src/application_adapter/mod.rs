// SPDX-License-Identifier: Apache-2.0

//! Concrete product executor composed from inward-facing application ports.

mod error;
mod finite;
mod operations;
mod profiles;
mod reads;
mod streams;

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use undercurrent_application::{
    AuthorizationPort, Clock, DetectionProfileCatalog, ExecutionContextResolver,
    InputDeliveryStore, OperationAttemptRepairAuthorizer, OperationAttemptRepairObserver,
    OperationAttemptRepairRepository, OperationAttemptStore, ProcessRuntime, ProfilePatternEngine,
    ProfileServiceLimits, RestartScheduler, RuntimeAttemptStatusAuthorizer,
    RuntimeAttemptStatusRepository, RuntimeCancellationAuthorizer, RuntimeCancellationRepository,
    SessionCommandExecutor, SessionEventReader, SessionEventSubscriber, SessionIdSource,
    SessionMutationCoordinator, SessionProcessDirectory, SessionRepository, SessionSubscriptionHub,
    Telemetry, TerminalOutputStore, TerminalScreenPort,
};
use undercurrent_protocol::{
    ProductCodec, ProductError, ProductErrorCategory, ProductErrorDisposition, ProductRequest,
    ProductResponse, ProductStreamError, ProductStreamSpec,
};

use crate::{
    AgentEventNotificationHub, BoundedProfileCursorStore, BoundedScreenCheckpointStore,
    BoundedSessionCursorStore, DaemonOperationsPort, OperationAttemptReadinessGate,
    PreparedProductStream, ProductActionExecutor, ProductBoundary, ProductRequestContext,
    ProductStreamContext, ProductStreamPump,
};

/// Invalid composition of authorized product read and stream ports.
#[derive(Debug)]
pub enum ProductReadPortsError {
    /// Standalone detection-profile limits are invalid.
    Profiles(undercurrent_application::ProfileServiceError),
    /// One terminal stream chunk cannot fit the output replay boundary.
    TerminalStreamBytes {
        /// Configured terminal stream chunk bytes.
        configured: usize,
        /// Inclusive output-store replay maximum.
        maximum: usize,
    },
}

impl Display for ProductReadPortsError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Profiles(error) => Display::fmt(error, formatter),
            Self::TerminalStreamBytes {
                configured,
                maximum,
            } => write!(
                formatter,
                "terminal stream chunk bytes {configured} must be within 1..={maximum}"
            ),
        }
    }
}

impl Error for ProductReadPortsError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Profiles(error) => Some(error),
            Self::TerminalStreamBytes { .. } => None,
        }
    }
}

/// Finite and live access to one retained session-event source.
pub struct ProductEventPorts {
    pub(crate) reader: Arc<dyn SessionEventReader>,
    pub(crate) subscriber: Arc<dyn SessionEventSubscriber>,
}

impl ProductEventPorts {
    /// Groups finite event reads and atomic live subscriptions for one source.
    pub fn new(
        reader: Arc<dyn SessionEventReader>,
        subscriber: Arc<dyn SessionEventSubscriber>,
    ) -> Self {
        Self { reader, subscriber }
    }
}

/// Authorized bounded read dependencies retained by the product executor.
pub struct ProductReadPorts {
    pub(crate) authorizer: Arc<dyn AuthorizationPort>,
    pub(crate) events: ProductEventPorts,
    pub(crate) output: Arc<TerminalOutputStore>,
    pub(crate) screens: Arc<dyn TerminalScreenPort>,
    pub(crate) terminal_stream_bytes: usize,
    pub(crate) profiles: Arc<dyn DetectionProfileCatalog>,
    pub(crate) patterns: Arc<dyn ProfilePatternEngine>,
    pub(crate) profile_limits: ProfileServiceLimits,
}

/// Coherent terminal read and stream dependencies.
pub struct ProductTerminalPorts {
    output: Arc<TerminalOutputStore>,
    screens: Arc<dyn TerminalScreenPort>,
    stream_bytes: usize,
}

impl ProductTerminalPorts {
    /// Groups terminal state sources behind one validated stream byte limit.
    ///
    /// # Errors
    ///
    /// Returns a typed configuration error when one stream chunk cannot fit
    /// within the retained-output replay boundary. This function performs no IO
    /// and does not panic.
    pub fn new(
        output: Arc<TerminalOutputStore>,
        screens: Arc<dyn TerminalScreenPort>,
        stream_bytes: usize,
    ) -> Result<Self, ProductReadPortsError> {
        let maximum = output.limits().replay_bytes;
        if stream_bytes == 0 || stream_bytes > maximum {
            return Err(ProductReadPortsError::TerminalStreamBytes {
                configured: stream_bytes,
                maximum,
            });
        }
        Ok(Self {
            output,
            screens,
            stream_bytes,
        })
    }
}

impl ProductReadPorts {
    /// Composes detail, output, event, and profile read boundaries without IO.
    ///
    /// # Errors
    ///
    /// Returns a typed configuration error when profile-service limits are
    /// invalid or terminal stream chunks exceed output replay bounds. This
    /// function does not panic.
    pub fn new(
        authorizer: Arc<dyn AuthorizationPort>,
        events: ProductEventPorts,
        terminal: ProductTerminalPorts,
        profiles: Arc<dyn DetectionProfileCatalog>,
        patterns: Arc<dyn ProfilePatternEngine>,
        profile_limits: ProfileServiceLimits,
    ) -> Result<Self, ProductReadPortsError> {
        Ok(Self {
            authorizer,
            events,
            output: terminal.output,
            screens: terminal.screens,
            terminal_stream_bytes: terminal.stream_bytes,
            profiles,
            patterns,
            profile_limits: profile_limits
                .validate()
                .map_err(ProductReadPortsError::Profiles)?,
        })
    }
}

/// Typed client-scoped cursor stores for independent product query families.
pub struct ProductCursorStores {
    pub(crate) sessions: Arc<BoundedSessionCursorStore>,
    pub(crate) profiles: Arc<BoundedProfileCursorStore>,
    pub(crate) screens: Arc<BoundedScreenCheckpointStore>,
}

impl ProductCursorStores {
    /// Groups already bounded cursor stores without allocating cursor entries.
    pub fn new(
        sessions: Arc<BoundedSessionCursorStore>,
        profiles: Arc<BoundedProfileCursorStore>,
        screens: Arc<BoundedScreenCheckpointStore>,
    ) -> Self {
        Self {
            sessions,
            profiles,
            screens,
        }
    }

    fn close_client(&self, client_id: &undercurrent_wire::ClientId) {
        self.sessions.close_client(client_id);
        self.profiles.close_client(client_id);
        self.screens.close_client(client_id);
    }
}

/// Concrete application dependencies retained by the product executor.
pub struct ProductApplicationPorts {
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) ids: Arc<dyn SessionIdSource>,
    pub(crate) coordinator: Arc<dyn SessionMutationCoordinator>,
    pub(crate) repository: Arc<dyn SessionRepository>,
    pub(crate) runtime: Arc<dyn ProcessRuntime>,
    pub(crate) execution_context: Arc<dyn ExecutionContextResolver>,
    pub(crate) processes: Arc<dyn SessionProcessDirectory>,
    pub(crate) restarts: Arc<dyn RestartScheduler>,
    pub(crate) deliveries: Arc<dyn InputDeliveryStore>,
    pub(crate) projections: Arc<SessionSubscriptionHub>,
    pub(crate) telemetry: Arc<dyn Telemetry>,
    pub(crate) reads: ProductReadPorts,
    pub(crate) execution: Option<ProductExecutionPorts>,
}

/// Durable attempt and private runtime ports for daemon-supervised finite commands.
pub struct ProductExecutionPorts {
    pub(crate) attempts: Arc<dyn OperationAttemptStore>,
    pub(crate) executor: Arc<dyn SessionCommandExecutor>,
    pub(crate) repair_repository: Arc<dyn OperationAttemptRepairRepository>,
    pub(crate) repair_observer: Arc<dyn OperationAttemptRepairObserver>,
    pub(crate) repair_authorizer: Arc<dyn OperationAttemptRepairAuthorizer>,
    pub(crate) status_repository: Arc<dyn RuntimeAttemptStatusRepository>,
    pub(crate) status_authorizer: Arc<dyn RuntimeAttemptStatusAuthorizer>,
    pub(crate) cancellation_repository: Option<Arc<dyn RuntimeCancellationRepository>>,
    pub(crate) cancellation_authorizer: Option<Arc<dyn RuntimeCancellationAuthorizer>>,
    pub(crate) agent_channel: Option<Arc<dyn undercurrent_application::AgentChannelRepository>>,
    pub(crate) readiness: Arc<OperationAttemptReadinessGate>,
    pub(crate) readiness_limit: usize,
    pub(crate) total_output_bytes: u64,
}

impl ProductExecutionPorts {
    /// Groups execution-only dependencies behind one explicit daemon composition boundary.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        attempts: Arc<dyn OperationAttemptStore>,
        executor: Arc<dyn SessionCommandExecutor>,
        repair_repository: Arc<dyn OperationAttemptRepairRepository>,
        repair_observer: Arc<dyn OperationAttemptRepairObserver>,
        repair_authorizer: Arc<dyn OperationAttemptRepairAuthorizer>,
        status_repository: Arc<dyn RuntimeAttemptStatusRepository>,
        status_authorizer: Arc<dyn RuntimeAttemptStatusAuthorizer>,
        readiness: Arc<OperationAttemptReadinessGate>,
        readiness_limit: usize,
        total_output_bytes: u64,
    ) -> Result<Self, ProductReadPortsError> {
        if total_output_bytes == 0 || readiness_limit == 0 {
            return Err(ProductReadPortsError::TerminalStreamBytes {
                configured: 0,
                maximum: 0,
            });
        }
        Ok(Self {
            attempts,
            executor,
            repair_repository,
            repair_observer,
            repair_authorizer,
            status_repository,
            status_authorizer,
            cancellation_repository: None,
            cancellation_authorizer: None,
            agent_channel: None,
            readiness,
            readiness_limit,
            total_output_bytes,
        })
    }

    /// Enables exact durable runtime-attempt cancellation and status lookup.
    pub fn with_runtime_cancellation(
        mut self,
        repository: Arc<dyn RuntimeCancellationRepository>,
        authorizer: Arc<dyn RuntimeCancellationAuthorizer>,
    ) -> Self {
        self.cancellation_repository = Some(repository);
        self.cancellation_authorizer = Some(authorizer);
        self
    }

    /// Enables the durable technical queue used by protected agent runtimes.
    pub fn with_agent_channel(
        mut self,
        repository: Arc<dyn undercurrent_application::AgentChannelRepository>,
    ) -> Self {
        self.agent_channel = Some(repository);
        self
    }
}

impl ProductApplicationPorts {
    /// Composes the lifecycle, terminal, query, and WorkContext application
    /// boundaries without performing IO.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        clock: Arc<dyn Clock>,
        ids: Arc<dyn SessionIdSource>,
        coordinator: Arc<dyn SessionMutationCoordinator>,
        repository: Arc<dyn SessionRepository>,
        runtime: Arc<dyn ProcessRuntime>,
        execution_context: Arc<dyn ExecutionContextResolver>,
        processes: Arc<dyn SessionProcessDirectory>,
        restarts: Arc<dyn RestartScheduler>,
        deliveries: Arc<dyn InputDeliveryStore>,
        projections: Arc<SessionSubscriptionHub>,
        telemetry: Arc<dyn Telemetry>,
        reads: ProductReadPorts,
    ) -> Self {
        Self {
            clock,
            ids,
            coordinator,
            repository,
            runtime,
            execution_context,
            processes,
            restarts,
            deliveries,
            projections,
            telemetry,
            reads,
            execution: None,
        }
    }

    /// Enables the durable daemon-supervised command operation.
    pub fn with_execution(mut self, execution: ProductExecutionPorts) -> Self {
        self.execution = Some(execution);
        self
    }
}

/// Product executor that invokes existing application use cases through ports.
pub struct ApplicationProductExecutor {
    profile_gate: Option<crate::profile_files::ProfileGate>,
    pub(crate) boundary: ProductBoundary,
    pub(crate) response_codec: ProductCodec,
    pub(crate) ports: ProductApplicationPorts,
    pub(crate) cursors: ProductCursorStores,
    pub(crate) streams: Arc<ProductStreamPump>,
    pub(crate) operations: Arc<dyn DaemonOperationsPort>,
    pub(crate) agent_event_notifications: Arc<AgentEventNotificationHub>,
    outbox_wake: Arc<dyn OutboxDispatchWake>,
    closed: AtomicBool,
}

/// Commit-edge notification used to wake transactional outbox dispatch.
pub trait OutboxDispatchWake: Send + Sync {
    /// Wakes the outbox worker after a mutation outcome may have committed.
    fn wake_outbox_dispatch(&self);

    /// Wakes durable effect reconciliation after a process effect is inconclusive.
    fn wake_attempt_reconciliation(&self);
}

impl ApplicationProductExecutor {
    /// Constructs an executor without starting processes or performing IO.
    pub fn new(
        boundary: ProductBoundary,
        response_codec: ProductCodec,
        ports: ProductApplicationPorts,
        cursors: ProductCursorStores,
        streams: Arc<ProductStreamPump>,
        operations: Arc<dyn DaemonOperationsPort>,
        outbox_wake: Arc<dyn OutboxDispatchWake>,
    ) -> Self {
        Self {
            profile_gate: None,
            boundary,
            response_codec,
            ports,
            cursors,
            streams,
            operations,
            agent_event_notifications: Arc::new(AgentEventNotificationHub::default()),
            outbox_wake,
            closed: AtomicBool::new(false),
        }
    }

    pub(crate) fn with_profile_gate(mut self, gate: crate::profile_files::ProfileGate) -> Self {
        self.profile_gate = Some(gate);
        self
    }

    pub(crate) fn with_agent_event_notifications(
        mut self,
        notifications: Arc<AgentEventNotificationHub>,
    ) -> Self {
        self.agent_event_notifications = notifications;
        self
    }

    fn attempt_admission_blocker(&self, request: &ProductRequest) -> Option<String> {
        use undercurrent_application::{OperationAttemptScan, OperationAttemptScope};
        let execution = self.ports.execution.as_ref()?;
        if !execution.readiness.scopes_available() {
            return Some("operation-attempt recovery cannot inspect durable state; inspect daemon recovery diagnostics".to_owned());
        }
        let scope = match request.target_session_id() {
            Some(id) => match undercurrent_domain::SessionId::new(id) {
                Ok(id) => OperationAttemptScope::Session(id),
                Err(_) => return None, // The operation's normal validation owns malformed IDs.
            },
            None => OperationAttemptScope::Host,
        };
        let mut scan = OperationAttemptScan::without_exclusions(execution.readiness_limit)
            .ok()?
            .with_scope(scope);
        if matches!(request, ProductRequest::StopSession(_)) {
            scan = scan.without_input_attempts();
        }
        match crate::blocking::block_on(execution.attempts.pending_reconciliation(&scan)) {
            Ok(attempts) => {
                if attempts.len() > scan.limit() || attempts.iter().any(|attempt| !scan.includes(attempt)) {
                    execution.readiness.close();
                    return Some("operation-attempt recovery scan violated its resource boundary".to_owned());
                }
                attempts.first().map(|attempt| {
                    let recovery = if attempt.kind() == undercurrent_domain::ProcessMutationKind::WriteInput {
                        "stop the affected session if active; recovery requires verified exit and input quiescence".to_owned()
                    } else { format!("request evidence-backed repair with `uc debug repair-attempt {}`", attempt.id()) };
                    format!("unresolved {} attempt {} blocks {}; inspect `uc debug reconciliation`; {}",
                        attempt.kind().as_str(), attempt.id(),
                        attempt.target_session().map_or_else(|| "host-wide operations".to_owned(), |id| format!("session {id}")), recovery)
                })
            }
            Err(_) => Some("operation-attempt recovery state could not be read; inspect daemon persistence and recovery diagnostics".to_owned()),
        }
    }

    fn unavailable(&self, request: &ProductRequest) -> ProductResponse {
        ProductResponse::Error(ProductError {
            effect_certainty: None,
            operation: Some(request.operation()),
            category: ProductErrorCategory::Internal,
            disposition: Some(ProductErrorDisposition::InfrastructureFailure),
            message: "product executor is shutting down".to_owned(),
            session_id: None,
            retryable: true,
            retry_after_millis: None,
            limit: None,
            resynchronization: None,
        })
    }
}

impl ProductActionExecutor for ApplicationProductExecutor {
    fn execute(&self, context: &ProductRequestContext, request: ProductRequest) -> ProductResponse {
        let _incident = undercurrent_diagnostics::IncidentScope::enter(
            context
                .trace_context
                .correlation_id
                .as_ref()
                .map(|id| id.as_str()),
        );
        // Shutdown must be able to join workers without holding their activation gate.
        let _profiles = if matches!(&request, ProductRequest::ShutdownDaemon(_)) {
            None
        } else {
            self.profile_gate
                .as_ref()
                .map(|gate| gate.read().unwrap_or_else(|p| p.into_inner()))
        };
        if self.closed.load(Ordering::Acquire) {
            return self.unavailable(&request);
        }
        let operation = request.operation();
        let semantic_mutation = operation.is_mutating();
        if matches!(
            &request,
            undercurrent_protocol::ProductRequest::RepairOperationAttempt(_)
        ) && let Err(source) = undercurrent_protocol::validate_product_request_contract(&request)
        {
            return error::application(
                operation,
                None,
                undercurrent_application::ApplicationErrorCategory::InvalidArgument,
                &source,
            );
        }
        if mutation_requires_attempt_readiness(operation)
            && let Some(message) = self.attempt_admission_blocker(&request)
        {
            self.outbox_wake.wake_attempt_reconciliation();
            return ProductResponse::Error(ProductError {
                effect_certainty: None,
                operation: Some(operation),
                category: ProductErrorCategory::FailedPrecondition,
                disposition: Some(ProductErrorDisposition::UnknownInconclusive),
                message,
                session_id: request.target_session_id().map(str::to_owned),
                retryable: false,
                retry_after_millis: None,
                limit: None,
                resynchronization: None,
            });
        }
        let _mutation_permit = if semantic_mutation
            && request.operation() != undercurrent_protocol::ProductOperation::ShutdownDaemon
        {
            match self.operations.admit_mutation() {
                Ok(permit) => Some(permit),
                Err(_) => {
                    return ProductResponse::Error(ProductError {
                        effect_certainty: None,
                        operation: Some(request.operation()),
                        category: ProductErrorCategory::FailedPrecondition,
                        disposition: Some(ProductErrorDisposition::SemanticRejection),
                        message: "daemon is draining and no longer accepts semantic mutations"
                            .to_owned(),
                        session_id: request.target_session_id().map(str::to_owned),
                        retryable: false,
                        retry_after_millis: None,
                        limit: None,
                        resynchronization: None,
                    });
                }
            }
        } else {
            None
        };
        let may_commit = semantic_mutation;
        let response = self.execute_finite(context, request);
        let authorization_rejected = matches!(
            &response,
            ProductResponse::Error(error)
                if matches!(
                    error.category,
                    ProductErrorCategory::Unauthenticated
                        | ProductErrorCategory::PermissionDenied
                )
        );
        // A mutation can fail after committing events. Wake dispatch on those
        // errors too, so publication does not depend on a successful response.
        // See [commit-to-response ordering](../../docs/internals/requests/request-flow.md#trace-session-creation).
        if may_commit && !authorization_rejected {
            self.outbox_wake.wake_outbox_dispatch();
        }
        if mutation_requires_attempt_readiness(operation)
            && matches!(
                &response,
                ProductResponse::Error(error)
                    if error.disposition == Some(ProductErrorDisposition::UnknownInconclusive)
            )
        {
            self.outbox_wake.wake_attempt_reconciliation();
        }
        response
    }

    fn prepare_stream(
        &self,
        context: &ProductStreamContext,
        specification: ProductStreamSpec,
    ) -> Result<Box<dyn PreparedProductStream>, ProductStreamError> {
        if self.closed.load(Ordering::Acquire) || self.operations.reset_requested() {
            return Err(ProductStreamError {
                kind: Some(specification.kind()),
                category: ProductErrorCategory::Internal,
                disposition: Some(ProductErrorDisposition::InfrastructureFailure),
                message: "product executor is shutting down".to_owned(),
                session_id: None,
                retryable: true,
                retry_after_millis: None,
                limit: None,
                resynchronization: None,
            });
        }
        self.prepare_product_stream(context, specification)
    }

    fn close_stream(
        &self,
        client_id: &undercurrent_wire::ClientId,
        stream_id: &undercurrent_wire::StreamId,
    ) {
        self.streams.close_stream(client_id, stream_id);
    }

    fn close_connection(&self, client_id: &undercurrent_wire::ClientId) {
        self.cursors.close_client(client_id);
        self.operations.close_client(client_id);
        self.streams.close_connection(client_id);
    }

    fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        self.streams.shutdown();
    }
}

const fn mutation_requires_attempt_readiness(
    operation: undercurrent_protocol::ProductOperation,
) -> bool {
    matches!(
        operation,
        undercurrent_protocol::ProductOperation::StartSession
            | undercurrent_protocol::ProductOperation::StopSession
            | undercurrent_protocol::ProductOperation::RestartSession
            | undercurrent_protocol::ProductOperation::RenameSession
            | undercurrent_protocol::ProductOperation::UpdateSessionLabels
            | undercurrent_protocol::ProductOperation::RemoveSession
            | undercurrent_protocol::ProductOperation::ResizeTerminal
            | undercurrent_protocol::ProductOperation::SendTerminalInput
            | undercurrent_protocol::ProductOperation::CloseStandardInput
            | undercurrent_protocol::ProductOperation::ExecuteInSession
            | undercurrent_protocol::ProductOperation::UpsertWorkItem
            | undercurrent_protocol::ProductOperation::RemoveWorkItem
    )
}
