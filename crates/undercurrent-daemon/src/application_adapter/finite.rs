// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Display, Formatter};
use std::time::{Duration, Instant};

use undercurrent_application::{
    AccessAction, AccessResource, AgentChannelTarget, AgentEventPollOutcome,
    AgentEventReplyAdmission, AgentEventReplyOutcome, CancelRuntimeAttemptCommand,
    CancelRuntimeAttemptError, CancelRuntimeAttemptService, CloseStandardInputService,
    DetectionProfileSelector, ExecuteInSessionError, ExecuteInSessionService, ExecutionOutput,
    GetRuntimeAttemptStatusQuery, GetRuntimeAttemptStatusService,
    GetRuntimeCancellationStatusQuery, GetRuntimeCancellationStatusService,
    OperationAttemptRepairActor, ProfileMatchOutcome, RemoveSessionService, RenameSessionService,
    RepairOperationAttemptCommand, RepairOperationAttemptError, RepairOperationAttemptService,
    ResizeTerminalService, RestartSessionService, RuntimeAttemptStatusActor,
    RuntimeAttemptStatusSnapshot, RuntimeCancellationActor, RuntimeCancellationControl,
    RuntimeCancellationMode, RuntimeCancellationPropagationPolicy, RuntimeCancellationResult,
    RuntimeCancellationSessionSnapshot, RuntimeCancellationState, SendTerminalInputService,
    SessionPage, SessionPageCursor, SessionQueryService, StartSessionError, StartSessionService,
    StopSessionResult, StopSessionService, UpdateSessionLabelsService, WorkContextService,
};
use undercurrent_domain::{
    DispatchCellIdentity, EffectCertainty, FencingToken, LifecycleState, OperationAttempt,
    OperationAttemptId, OperationAttemptState, PayloadDigest, ProcessInstanceId, Revision,
    SessionId, TimestampMillis,
};
use undercurrent_protocol::{
    AgentEventDto, AgentToolCallDto, CancelRuntimeAttemptResultDto, CloseStandardInputResultDto,
    EffectCertaintyDto, ExecuteInSessionResultDto, ExecutionOutputDto, GetAgentEventResultDto,
    LifecycleStateDto, OperationAttemptOutcomeDto, OutputTruncationDto, ProductCodecErrorCategory,
    ProductErrorDisposition, ProductOperation, ProductRequest, ProductResponse, ProductSuccess,
    RemoveSessionResultDto, RepairOperationAttemptResolutionDto, RepairOperationAttemptResultDto,
    ReplyAgentEventResultDto, RuntimeAttemptBindingDto, RuntimeAttemptSessionSnapshotDto,
    RuntimeAttemptStatusResultDto, RuntimeAttemptStatusSnapshotDto,
    RuntimeCancellationCommandDigestMaterial, RuntimeCancellationControlBindingDto,
    RuntimeCancellationModeDto, RuntimeCancellationPropagationPolicyDto,
    RuntimeCancellationSessionSnapshotDto, RuntimeCancellationStateDto,
    RuntimeCancellationStatusResultDto, RuntimeCancellationTargetBindingDto,
    SendTerminalInputResultDto, SessionMutationResultDto, SessionPageDto, StopSessionResultDto,
    canonical_agent_event_reply_outcome, canonical_agent_tool_call,
};

use super::ApplicationProductExecutor;
use super::error;
use crate::{
    ProductPresentation, ProductRequestContext, blocking::block_on,
    cursor::SESSION_CURSOR_TOKEN_PLACEHOLDER,
};

impl ApplicationProductExecutor {
    pub(super) fn execute_finite(
        &self,
        context: &ProductRequestContext,
        request: ProductRequest,
    ) -> ProductResponse {
        let operation = request.operation();
        if context.cancellation.is_cancelled() {
            return error::cancelled(operation);
        }
        match request {
            ProductRequest::StartSession(request) => self.start(request),
            ProductRequest::StopSession(request) => self.stop(request),
            ProductRequest::RestartSession(request) => self.restart(request),
            ProductRequest::RenameSession(request) => self.rename(request),
            ProductRequest::UpdateSessionLabels(request) => self.update_labels(request),
            ProductRequest::RemoveSession(request) => self.remove(request),
            ProductRequest::ResizeTerminal(request) => self.resize(request),
            ProductRequest::SendTerminalInput(request) => self.input(request),
            ProductRequest::CloseStandardInput(request) => self.close_input(request),
            ProductRequest::ExecuteInSession(request) => self.execute_in_session(context, request),
            ProductRequest::RepairOperationAttempt(request) => {
                self.repair_operation_attempt(context, request)
            }
            ProductRequest::GetRuntimeAttemptStatus(request) => {
                self.get_runtime_attempt_status(context, request)
            }
            ProductRequest::CancelRuntimeAttempt(request) => {
                self.cancel_runtime_attempt(context, request)
            }
            ProductRequest::GetRuntimeCancellationStatus(request) => {
                self.get_runtime_cancellation_status(context, request)
            }
            ProductRequest::GetAgentEvent(request) => self.get_agent_event(context, request),
            ProductRequest::ReplyAgentEvent(request) => self.reply_agent_event(context, request),
            ProductRequest::ListSessions(request) => self.list(context, request),
            ProductRequest::UpsertWorkItem(request) => self.upsert_work(request),
            ProductRequest::RemoveWorkItem(request) => self.remove_work(request),
            ProductRequest::GetSessionDetail(request) => self.detail(request),
            ProductRequest::GetExecutionContext(request) => self.execution_context(request),
            ProductRequest::ReadTerminalOutput(request) => self.output(request),
            ProductRequest::ReadTerminalScreen(request) => self.screen(context, request),
            ProductRequest::ReadTerminalScreenDiff(request) => self.screen_diff(context, request),
            ProductRequest::ReadSessionEvents(request) => self.events(request),
            ProductRequest::ListProfiles(request) => self.list_profiles(context, request),
            ProductRequest::GetProfile(request) => self.get_profile(request),
            ProductRequest::MatchProfile(request) => self.match_profile(request),
            ProductRequest::LintProfiles(request) => self.lint_profiles(request),
            ProductRequest::TestProfile(request) => self.test_profile(request),
            ProductRequest::GetDiagnostics(request) => self.diagnostics(context, request),
            ProductRequest::CreateDiagnosticBundle(request) => {
                self.diagnostic_bundle(context, request)
            }
            ProductRequest::GetDaemonStatus => self.daemon_status(),
            ProductRequest::ShutdownDaemon(request) => self.shutdown_daemon(context, request),
        }
    }

    fn get_agent_event(
        &self,
        context: &ProductRequestContext,
        request: undercurrent_protocol::GetAgentEventRequest,
    ) -> ProductResponse {
        let operation = ProductOperation::GetAgentEvent;
        let session_id = Some(request.session_id.clone());
        let target = match get_agent_target(&request) {
            Ok(target) => target,
            Err(source) => {
                return error::application(
                    operation,
                    session_id,
                    undercurrent_application::ApplicationErrorCategory::InvalidArgument,
                    &source,
                );
            }
        };
        if request.maximum_wait_millis > undercurrent_protocol::MAX_AGENT_EVENT_WAIT_MILLIS {
            return error::application(
                operation,
                session_id,
                undercurrent_application::ApplicationErrorCategory::InvalidArgument,
                &AgentChannelBoundaryInvariant("agent event wait exceeds the protocol bound"),
            );
        }
        // Binding coordinates establish the target, not the caller's authority
        // to read it. Apply the same per-session policy as AgentEvents streams.
        if let Err(response) = self.authorize(
            operation,
            AccessAction::ReadEvents,
            &AccessResource::Session(target.session_id.clone()),
            session_id.clone(),
        ) {
            return response;
        }
        let Some(repository) = self
            .ports
            .execution
            .as_ref()
            .and_then(|ports| ports.agent_channel.as_deref())
        else {
            return error::invalid_configuration(operation);
        };
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(request.maximum_wait_millis))
            .unwrap_or_else(Instant::now);
        loop {
            if context.cancellation.is_cancelled() {
                return error::cancelled(operation);
            }
            let outcome = match block_on(repository.poll_agent_event(
                &target,
                request.after_sequence,
                self.ports.clock.now(),
            )) {
                Ok(outcome) => outcome,
                Err(source) => {
                    return error::application(operation, session_id, source.category(), &source);
                }
            };
            match outcome {
                AgentEventPollOutcome::Event(event) => match agent_event_dto(*event) {
                    Ok(event) => {
                        return success(ProductSuccess::GetAgentEvent(
                            GetAgentEventResultDto::Event {
                                event: Box::new(event),
                            },
                        ));
                    }
                    Err(source) => {
                        return error::application(
                            operation,
                            session_id,
                            undercurrent_application::ApplicationErrorCategory::Internal,
                            &source,
                        );
                    }
                },
                AgentEventPollOutcome::None { next_sequence } => {
                    if request.maximum_wait_millis == 0 || Instant::now() >= deadline {
                        return success(ProductSuccess::GetAgentEvent(
                            GetAgentEventResultDto::None { next_sequence },
                        ));
                    }
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    std::thread::sleep(remaining.min(Duration::from_millis(25)));
                }
                AgentEventPollOutcome::StaleTarget => {
                    return agent_channel_state_error(
                        operation,
                        session_id,
                        undercurrent_application::ApplicationErrorCategory::FailedPrecondition,
                        "agent event target is stale",
                    );
                }
                AgentEventPollOutcome::BindingExpired => {
                    return agent_channel_state_error(
                        operation,
                        session_id,
                        undercurrent_application::ApplicationErrorCategory::FailedPrecondition,
                        "agent event binding has expired",
                    );
                }
                AgentEventPollOutcome::NotFound => {
                    return agent_channel_state_error(
                        operation,
                        session_id,
                        undercurrent_application::ApplicationErrorCategory::NotFound,
                        "agent event target was not found",
                    );
                }
            }
        }
    }

    fn reply_agent_event(
        &self,
        _context: &ProductRequestContext,
        request: undercurrent_protocol::ReplyAgentEventRequest,
    ) -> ProductResponse {
        let operation = ProductOperation::ReplyAgentEvent;
        let session_id = Some(request.session_id.clone());
        let target = match reply_agent_target(&request) {
            Ok(target) => target,
            Err(source) => {
                return error::application(
                    operation,
                    session_id,
                    undercurrent_application::ApplicationErrorCategory::InvalidArgument,
                    &source,
                );
            }
        };
        if let Err(response) = self.authorize(
            operation,
            AccessAction::ReplyAgentEvent,
            &AccessResource::Session(target.session_id.clone()),
            session_id.clone(),
        ) {
            return response;
        }
        if request.event_id.is_empty()
            || request.event_id.len() > 256
            || request.event_id.chars().any(char::is_control)
            || request.sequence == 0
        {
            return error::application(
                operation,
                session_id,
                undercurrent_application::ApplicationErrorCategory::InvalidArgument,
                &AgentChannelBoundaryInvariant("agent event reply identity is invalid"),
            );
        }
        let call_digest = match PayloadDigest::from_hex(&request.call_digest_sha256) {
            Ok(digest) => digest,
            Err(source) => {
                return error::application(
                    operation,
                    session_id,
                    undercurrent_application::ApplicationErrorCategory::InvalidArgument,
                    &source,
                );
            }
        };
        let outcome_canonical_json = match canonical_agent_event_reply_outcome(&request.outcome) {
            Ok(outcome) => outcome,
            Err(source) => {
                return error::application(
                    operation,
                    session_id,
                    undercurrent_application::ApplicationErrorCategory::InvalidArgument,
                    &source,
                );
            }
        };
        let reply_digest = PayloadDigest::of_bytes(&outcome_canonical_json);
        let Some(repository) = self
            .ports
            .execution
            .as_ref()
            .and_then(|ports| ports.agent_channel.as_deref())
        else {
            return error::invalid_configuration(operation);
        };
        let admission = AgentEventReplyAdmission {
            target,
            event_id: request.event_id.clone(),
            sequence: request.sequence,
            call_digest,
            reply_digest,
            outcome_canonical_json,
            replied_at: self.ports.clock.now(),
        };
        match block_on(repository.reply_agent_event(&admission)) {
            Ok(AgentEventReplyOutcome::Accepted(_)) => {
                self.agent_event_notifications
                    .notify_reply(&admission.target.attempt_id, &admission.event_id);
                success(ProductSuccess::ReplyAgentEvent(ReplyAgentEventResultDto {
                    session_id: request.session_id,
                    event_id: request.event_id,
                    sequence: request.sequence,
                    reply_digest_sha256: reply_digest.to_hex(),
                    duplicate: false,
                }))
            }
            Ok(AgentEventReplyOutcome::Duplicate(_)) => {
                self.agent_event_notifications
                    .notify_reply(&admission.target.attempt_id, &admission.event_id);
                success(ProductSuccess::ReplyAgentEvent(ReplyAgentEventResultDto {
                    session_id: request.session_id,
                    event_id: request.event_id,
                    sequence: request.sequence,
                    reply_digest_sha256: reply_digest.to_hex(),
                    duplicate: true,
                }))
            }
            Ok(AgentEventReplyOutcome::Conflict) => agent_channel_state_error(
                operation,
                session_id,
                undercurrent_application::ApplicationErrorCategory::Conflict,
                "agent event already has a different reply",
            ),
            Ok(AgentEventReplyOutcome::StaleTarget) => agent_channel_state_error(
                operation,
                session_id,
                undercurrent_application::ApplicationErrorCategory::FailedPrecondition,
                "agent event reply target is stale",
            ),
            Ok(AgentEventReplyOutcome::BindingExpired) => agent_channel_state_error(
                operation,
                session_id,
                undercurrent_application::ApplicationErrorCategory::FailedPrecondition,
                "agent event binding has expired",
            ),
            Ok(AgentEventReplyOutcome::NotFound) => agent_channel_state_error(
                operation,
                session_id,
                undercurrent_application::ApplicationErrorCategory::NotFound,
                "agent event was not found",
            ),
            Err(source) => error::application(operation, session_id, source.category(), &source),
        }
    }

    fn cancel_runtime_attempt(
        &self,
        context: &ProductRequestContext,
        request: undercurrent_protocol::CancelRuntimeAttemptRequest,
    ) -> ProductResponse {
        let operation = ProductOperation::CancelRuntimeAttempt;
        if let Err(source) = request.validate() {
            return error::application(
                operation,
                None,
                undercurrent_application::ApplicationErrorCategory::InvalidArgument,
                &source,
            );
        }
        let session_id = Some(request.session_id().to_owned());
        let authentication = context.authentication.as_ref();
        let command = (|| -> Result<CancelRuntimeAttemptCommand, Box<dyn std::error::Error>> {
            let actor = RuntimeCancellationActor::new(
                authentication.principal().as_str().to_owned(),
                authentication
                    .tenant()
                    .map(|tenant| tenant.as_str().to_owned()),
                authentication.authority().as_str().to_owned(),
                request.source_product().to_owned(),
                request.tenant_id().to_owned(),
                request.actor_context_ref().to_owned(),
                request.authority_ref().to_owned(),
            )?;
            let target = undercurrent_application::RuntimeCancellationTarget::new(
                OperationAttemptId::new(request.target_attempt_id().to_owned())?,
                PayloadDigest::from_hex(request.target_request_digest_sha256())?,
                FencingToken::new(request.target_fencing_generation())?,
                DispatchCellIdentity::new(request.caller_cell_identity().to_owned())?,
                SessionId::new(request.session_id().to_owned())?,
                Revision::new(request.expected_session_revision()),
                Revision::new(request.expected_lifecycle_revision()),
                request.expected_provider_id().to_owned(),
                request.expected_provider_generation(),
            )?;
            let idempotency_key = request
                .mutation()
                .idempotency_key
                .clone()
                .ok_or(RuntimeCancellationBoundaryInvariant)?;
            let control = RuntimeCancellationControl::new(
                OperationAttemptId::new(request.control_id().to_owned())?,
                PayloadDigest::from_hex(request.command_digest_sha256())?,
                PayloadDigest::from_hex(&request.product_request_fingerprint_sha256()?)?,
                idempotency_key,
                match request.mode() {
                    RuntimeCancellationModeDto::Graceful => RuntimeCancellationMode::Graceful,
                    RuntimeCancellationModeDto::Force => RuntimeCancellationMode::Force,
                },
                request.reason().to_owned(),
                match request.propagation_policy() {
                    RuntimeCancellationPropagationPolicyDto::TargetOnly => {
                        RuntimeCancellationPropagationPolicy::TargetOnly
                    }
                },
                TimestampMillis::new(request.absolute_deadline_millis()),
                request.source_product().to_owned(),
                request.tenant_id().to_owned(),
                request.actor_context_ref().to_owned(),
                request.authority_ref().to_owned(),
            )?;
            Ok(CancelRuntimeAttemptCommand::new(target, control, actor)?)
        })();
        let command = match command {
            Ok(command) => command,
            Err(source) => {
                return error::application(
                    operation,
                    session_id,
                    undercurrent_application::ApplicationErrorCategory::InvalidArgument,
                    source.as_ref(),
                );
            }
        };
        let Some(execution) = self.ports.execution.as_ref() else {
            return error::invalid_configuration(operation);
        };
        let (Some(repository), Some(authorizer)) = (
            execution.cancellation_repository.as_deref(),
            execution.cancellation_authorizer.as_deref(),
        ) else {
            return error::invalid_configuration(operation);
        };
        let service = CancelRuntimeAttemptService::new(
            authorizer,
            repository,
            self.ports.repository.as_ref(),
            self.ports.coordinator.as_ref(),
            self.ports.runtime.as_ref(),
            self.ports.processes.as_ref(),
            self.ports.restarts.as_ref(),
            self.ports.projections.as_ref(),
            self.ports.telemetry.as_ref(),
            self.ports.clock.as_ref(),
        );
        let result = match block_on(service.execute(command)) {
            Ok(result) => result,
            Err(source) => {
                return runtime_cancellation_error_response(operation, session_id, &source);
            }
        };
        match runtime_cancellation_to_dto(result).and_then(|(target, control, state)| {
            CancelRuntimeAttemptResultDto::new(target, control, state)
        }) {
            Ok(result) => success(ProductSuccess::CancelRuntimeAttempt(result)),
            Err(source) => error::application(
                operation,
                session_id,
                undercurrent_application::ApplicationErrorCategory::Internal,
                &source,
            ),
        }
    }

    fn get_runtime_cancellation_status(
        &self,
        context: &ProductRequestContext,
        request: undercurrent_protocol::GetRuntimeCancellationStatusRequest,
    ) -> ProductResponse {
        let operation = ProductOperation::GetRuntimeCancellationStatus;
        if let Err(source) = request.validate() {
            return error::application(
                operation,
                None,
                undercurrent_application::ApplicationErrorCategory::InvalidArgument,
                &source,
            );
        }
        let session_id = Some(request.session_id().to_owned());
        let authentication = context.authentication.as_ref();
        let query =
            (|| -> Result<GetRuntimeCancellationStatusQuery, Box<dyn std::error::Error>> {
                let actor = RuntimeCancellationActor::new(
                    authentication.principal().as_str().to_owned(),
                    authentication
                        .tenant()
                        .map(|tenant| tenant.as_str().to_owned()),
                    authentication.authority().as_str().to_owned(),
                    request.source_product().to_owned(),
                    request.tenant_id().to_owned(),
                    request.actor_context_ref().to_owned(),
                    request.authority_ref().to_owned(),
                )?;
                Ok(GetRuntimeCancellationStatusQuery::new(
                    OperationAttemptId::new(request.control_id().to_owned())?,
                    PayloadDigest::from_hex(request.command_digest_sha256())?,
                    OperationAttemptId::new(request.target_attempt_id().to_owned())?,
                    PayloadDigest::from_hex(request.target_request_digest_sha256())?,
                    SessionId::new(request.session_id().to_owned())?,
                    request.expected_provider_id().to_owned(),
                    request.expected_provider_generation(),
                    actor,
                )?)
            })();
        let query = match query {
            Ok(query) => query,
            Err(source) => {
                return error::application(
                    operation,
                    session_id,
                    undercurrent_application::ApplicationErrorCategory::InvalidArgument,
                    source.as_ref(),
                );
            }
        };
        let Some(execution) = self.ports.execution.as_ref() else {
            return error::invalid_configuration(operation);
        };
        let (Some(repository), Some(authorizer)) = (
            execution.cancellation_repository.as_deref(),
            execution.cancellation_authorizer.as_deref(),
        ) else {
            return error::invalid_configuration(operation);
        };
        let service = GetRuntimeCancellationStatusService::new(authorizer, repository);
        let result = match block_on(service.execute(query)) {
            Ok(result) => result,
            Err(source) => {
                return error::application(operation, session_id, source.category(), &source);
            }
        };
        match runtime_cancellation_to_dto(result).and_then(|(target, control, state)| {
            RuntimeCancellationStatusResultDto::new(target, control, state)
        }) {
            Ok(result) => success(ProductSuccess::RuntimeCancellationStatus(result)),
            Err(source) => error::application(
                operation,
                session_id,
                undercurrent_application::ApplicationErrorCategory::Internal,
                &source,
            ),
        }
    }

    fn get_runtime_attempt_status(
        &self,
        context: &ProductRequestContext,
        request: undercurrent_protocol::GetRuntimeAttemptStatusRequest,
    ) -> ProductResponse {
        let operation = ProductOperation::GetRuntimeAttemptStatus;
        if let Err(source) = request.validate() {
            return error::application(
                operation,
                None,
                undercurrent_application::ApplicationErrorCategory::InvalidArgument,
                &source,
            );
        }
        let attempt_id = match OperationAttemptId::new(request.attempt_id().to_owned()) {
            Ok(attempt_id) => attempt_id,
            Err(source) => {
                return error::application(
                    operation,
                    None,
                    undercurrent_application::ApplicationErrorCategory::InvalidArgument,
                    &source,
                );
            }
        };
        let request_digest = match PayloadDigest::from_hex(request.request_digest_sha256()) {
            Ok(digest) => digest,
            Err(source) => {
                return error::application(
                    operation,
                    None,
                    undercurrent_application::ApplicationErrorCategory::InvalidArgument,
                    &source,
                );
            }
        };
        let authentication = context.authentication.as_ref();
        let actor = match RuntimeAttemptStatusActor::new(
            authentication.principal().as_str().to_owned(),
            authentication
                .tenant()
                .map(|tenant| tenant.as_str().to_owned()),
            authentication.authority().as_str().to_owned(),
        ) {
            Ok(actor) => actor,
            Err(source) => {
                return error::application(
                    operation,
                    None,
                    undercurrent_application::ApplicationErrorCategory::InvalidArgument,
                    &source,
                );
            }
        };
        let Some(execution) = self.ports.execution.as_ref() else {
            return error::invalid_configuration(operation);
        };
        let service = GetRuntimeAttemptStatusService::new(
            execution.status_authorizer.as_ref(),
            execution.status_repository.as_ref(),
        );
        let result = match block_on(service.execute(GetRuntimeAttemptStatusQuery::new(
            attempt_id,
            request_digest,
            actor,
        ))) {
            Ok(result) => result,
            Err(source) => {
                return error::application(operation, None, source.category(), &source);
            }
        };
        match runtime_attempt_status_to_dto(result) {
            Ok(result) => success(ProductSuccess::RuntimeAttemptStatus(result)),
            Err(source) => error::application(
                operation,
                None,
                undercurrent_application::ApplicationErrorCategory::Internal,
                &source,
            ),
        }
    }

    fn repair_operation_attempt(
        &self,
        context: &ProductRequestContext,
        request: undercurrent_protocol::RepairOperationAttemptRequest,
    ) -> ProductResponse {
        let operation = ProductOperation::RepairOperationAttempt;
        if let Err(source) = request.validate() {
            return error::application(
                operation,
                None,
                undercurrent_application::ApplicationErrorCategory::InvalidArgument,
                &source,
            );
        }
        let attempt_id = match OperationAttemptId::new(request.attempt_id().to_owned()) {
            Ok(attempt_id) => attempt_id,
            Err(source) => {
                return error::application(
                    operation,
                    None,
                    undercurrent_application::ApplicationErrorCategory::InvalidArgument,
                    &source,
                );
            }
        };
        let authentication = context.authentication.as_ref();
        let actor = match OperationAttemptRepairActor::new(
            authentication.principal().as_str().to_owned(),
            authentication
                .tenant()
                .map(|tenant| tenant.as_str().to_owned()),
            authentication.authority().as_str().to_owned(),
        ) {
            Ok(actor) => actor,
            Err(source) => {
                return error::application(
                    operation,
                    None,
                    undercurrent_application::ApplicationErrorCategory::InvalidArgument,
                    &source,
                );
            }
        };
        let Some(execution) = self.ports.execution.as_ref() else {
            return error::invalid_configuration(operation);
        };
        let service = match RepairOperationAttemptService::new(
            execution.repair_authorizer.as_ref(),
            execution.repair_repository.as_ref(),
            execution.repair_observer.as_ref(),
            self.ports.clock.as_ref(),
            execution.readiness_limit,
        ) {
            Ok(service) => service,
            Err(source) => return error::invalid_configuration_with_source(operation, &source),
        };
        let command = match request.request_digest_sha256() {
            Some(request_digest) => match PayloadDigest::from_hex(request_digest) {
                Ok(request_digest) => {
                    RepairOperationAttemptCommand::attached(attempt_id, request_digest, actor)
                }
                Err(source) => {
                    return error::application(
                        operation,
                        None,
                        undercurrent_application::ApplicationErrorCategory::InvalidArgument,
                        &source,
                    );
                }
            },
            None => RepairOperationAttemptCommand::new(attempt_id, actor),
        };
        let result = match block_on(service.execute(command)) {
            Ok(result) => result,
            Err(source) => {
                return error::application(
                    operation,
                    None,
                    repair_error_category(&source),
                    &source,
                );
            }
        };
        let unresolved = result.readiness().unresolved();
        execution
            .readiness
            .apply_unresolved_count(unresolved.total());
        let resolution = match result.certainty() {
            EffectCertainty::KnownAbsent => RepairOperationAttemptResolutionDto::KnownAbsent,
            EffectCertainty::KnownApplied => {
                let Some(digest) = result.result_digest() else {
                    return error::application(
                        operation,
                        None,
                        undercurrent_application::ApplicationErrorCategory::Internal,
                        &RepairResultInvariant,
                    );
                };
                match RepairOperationAttemptResolutionDto::known_applied(digest.to_hex()) {
                    Ok(resolution) => resolution,
                    Err(_) => {
                        return error::application(
                            operation,
                            None,
                            undercurrent_application::ApplicationErrorCategory::Internal,
                            &RepairResultInvariant,
                        );
                    }
                }
            }
            EffectCertainty::Unknown => {
                return error::application(
                    operation,
                    None,
                    undercurrent_application::ApplicationErrorCategory::Internal,
                    &RepairResultInvariant,
                );
            }
        };
        let unresolved_count = match u64::try_from(unresolved.total()) {
            Ok(count) => count,
            Err(_) => {
                return error::application(
                    operation,
                    None,
                    undercurrent_application::ApplicationErrorCategory::Internal,
                    &RepairResultInvariant,
                );
            }
        };
        let dto = match RepairOperationAttemptResultDto::new(
            result.attempt_id().as_str().to_owned(),
            resolution,
            unresolved_count,
            unresolved
                .attempt_ids()
                .iter()
                .map(|attempt_id| attempt_id.as_str().to_owned())
                .collect(),
        ) {
            Ok(dto) => dto,
            Err(_) => {
                return error::application(
                    operation,
                    None,
                    undercurrent_application::ApplicationErrorCategory::Internal,
                    &RepairResultInvariant,
                );
            }
        };
        success(ProductSuccess::RepairOperationAttempt(dto))
    }

    fn start(&self, request: undercurrent_protocol::StartSessionRequest) -> ProductResponse {
        let operation = ProductOperation::StartSession;
        let automatic_profile = match self.boundary.automatic_profile_match_target(&request) {
            Ok(target) => target,
            Err(source) => return error::boundary(operation, None, source),
        };
        let mut command = match self.boundary.start_session(request, self.ports.clock.now()) {
            Ok(command) => command,
            Err(source) => return error::boundary(operation, None, source),
        };
        if let Err(response) = self.authorize(
            operation,
            AccessAction::StartSession,
            &AccessResource::Sessions,
            None,
        ) {
            return response;
        }
        if let Some(target) = automatic_profile {
            let selector = match DetectionProfileSelector::new(
                self.ports.reads.profiles.as_ref(),
                self.ports.reads.patterns.as_ref(),
                self.ports.reads.profile_limits.max_profiles,
            ) {
                Ok(selector) => selector,
                Err(_) => return error::invalid_configuration(operation),
            };
            match selector.select(&target) {
                Ok(ProfileMatchOutcome::Matched { profile, .. }) => {
                    command.configuration.select_profile_if_absent(profile);
                }
                Ok(ProfileMatchOutcome::NoMatch | ProfileMatchOutcome::Ambiguous { .. }) => {}
                Err(source) => {
                    return error::application(operation, None, source.category(), &source);
                }
            }
        }
        // Creation and profile activation share the daemon's catalog gate. A
        // queued request must not acquire a name removed by the preceding reload.
        if let Some(profile) = command.configuration.profile() {
            match self.ports.reads.profiles.get(profile) {
                Ok(Some(_)) => {}
                Ok(None) => return error::profile_unavailable(operation, None, profile.as_str()),
                Err(source) => {
                    return error::application(operation, None, source.category(), &source);
                }
            }
        }
        let service = StartSessionService::new(
            self.ports.clock.as_ref(),
            self.ports.ids.as_ref(),
            self.ports.coordinator.as_ref(),
            self.ports.repository.as_ref(),
            self.ports.runtime.as_ref(),
            self.ports.processes.as_ref(),
            self.ports.projections.as_ref(),
            self.ports.telemetry.as_ref(),
        );
        let service = if let Some(execution) = self.ports.execution.as_ref() {
            service.with_operation_attempt_store(execution.attempts.as_ref())
        } else {
            service
        };
        let result = crate::blocking::block_on_until(service.execute(command), || {
            self.operations.reset_requested()
        });
        let Some(result) = result else {
            return error::reset_cancelled(operation);
        };
        match result {
            Ok(started) => start_success(operation, &started.session, |result| {
                ProductSuccess::StartSession(result)
            }),
            Err(source @ StartSessionError::NameConflict { .. }) => {
                error::name_conflict(operation, source.to_string())
            }
            Err(StartSessionError::InitialCommit(source)) => error::initial_session_commit(&source),
            Err(source) => error::application(operation, None, source.category(), &source),
        }
    }

    fn execute_in_session(
        &self,
        context: &ProductRequestContext,
        request: undercurrent_protocol::ExecuteInSessionRequest,
    ) -> ProductResponse {
        let operation = ProductOperation::ExecuteInSession;
        let session_id = Some(request.session_id.clone());
        let Some(execution) = self.ports.execution.as_ref() else {
            return error::invalid_configuration(operation);
        };
        let command = match self.boundary.execute_in_session(
            request,
            self.ports.clock.now(),
            execution.total_output_bytes,
        ) {
            Ok(command) => command,
            Err(source) => return error::boundary(operation, session_id, source),
        };
        let service = ExecuteInSessionService::new(
            self.ports.reads.authorizer.as_ref(),
            self.ports.repository.as_ref(),
            self.ports.execution_context.as_ref(),
            self.ports.clock.as_ref(),
            self.ports.telemetry.as_ref(),
            execution.attempts.as_ref(),
            execution.executor.as_ref(),
        );
        let result = match block_on(service.execute(
            &command,
            self.ports.projections.catalog(),
            &context.cancellation,
        )) {
            Ok(result) => result,
            Err(ExecuteInSessionError::Cancelled) => return error::cancelled(operation),
            Err(ExecuteInSessionError::DeadlineExceeded) => {
                return error::application(
                    operation,
                    session_id,
                    undercurrent_application::ApplicationErrorCategory::Timeout,
                    &ExecuteDeadlineElapsed,
                );
            }
            Err(ExecuteInSessionError::Context(source)) => {
                return error::application(operation, session_id, source.category(), &source);
            }
            Err(
                ExecuteInSessionError::AttemptPersistence(source)
                | ExecuteInSessionError::Settlement(source),
            ) => {
                return error::application(operation, session_id, source.category(), &source);
            }
            Err(ExecuteInSessionError::AttemptRejected) => {
                return error::application(
                    operation,
                    session_id,
                    undercurrent_application::ApplicationErrorCategory::Conflict,
                    &ExecuteAttemptRejected,
                );
            }
            Err(ExecuteInSessionError::Execution(source)) => {
                return error::application(
                    operation,
                    session_id,
                    source.source_error().category(),
                    source.source_error(),
                );
            }
        };
        let attempt = match block_on(execution.attempts.load(command.mutation.attempt_id())) {
            Ok(Some(attempt)) => attempt,
            Ok(None) => {
                return error::application(
                    operation,
                    session_id,
                    undercurrent_application::ApplicationErrorCategory::Internal,
                    &MissingSettledAttempt,
                );
            }
            Err(source) => {
                return error::application(operation, session_id, source.category(), &source);
            }
        };
        success(ProductSuccess::ExecuteInSession(
            ExecuteInSessionResultDto {
                session_id: result.session_id.into_string(),
                attempt: attempt_outcome(&attempt),
                exit_code: result.exit_code,
                stdout: execution_output(result.stdout),
                stderr: execution_output(result.stderr),
            },
        ))
    }

    fn stop(&self, request: undercurrent_protocol::StopSessionRequest) -> ProductResponse {
        let operation = ProductOperation::StopSession;
        let session_id = Some(request.session_id.clone());
        let command = match self.boundary.stop_session(request) {
            Ok(command) => command,
            Err(source) => return error::boundary(operation, session_id, source),
        };
        if let Err(response) = self.authorize(
            operation,
            AccessAction::StopSession,
            &AccessResource::Session(command.session_id.clone()),
            session_id.clone(),
        ) {
            return response;
        }
        let service = StopSessionService::new(
            self.ports.clock.as_ref(),
            self.ports.coordinator.as_ref(),
            self.ports.repository.as_ref(),
            self.ports.runtime.as_ref(),
            self.ports.processes.as_ref(),
            self.ports.restarts.as_ref(),
            self.ports.projections.as_ref(),
            self.ports.telemetry.as_ref(),
        );
        let result = crate::blocking::block_on_until(service.execute(command), || {
            self.operations.abandon_stops_requested()
        });
        let Some(result) = result else {
            return error::reset_cancelled(operation);
        };
        match result {
            Ok(StopSessionResult::AlreadyTerminal { session }) => {
                stop_success(operation, &session, true, None)
            }
            Ok(StopSessionResult::Terminated {
                session,
                termination,
            }) => stop_success(operation, &session, false, termination.exit_code()),
            Err(undercurrent_application::StopSessionError::Mutation(
                undercurrent_application::SessionMutationError::RevisionPreconditionFailed {
                    ..
                },
            )) => error::revision_precondition_failed(operation, session_id),
            Err(source) => error::application(operation, session_id, source.category(), &source),
        }
    }

    fn restart(&self, request: undercurrent_protocol::RestartSessionRequest) -> ProductResponse {
        let operation = ProductOperation::RestartSession;
        let session_id = Some(request.session_id.clone());
        let command = match self.boundary.restart_session(request) {
            Ok(command) => command,
            Err(source) => return error::boundary(operation, session_id, source),
        };
        if let Err(response) = self.authorize(
            operation,
            AccessAction::RestartSession,
            &AccessResource::Session(command.session_id.clone()),
            session_id.clone(),
        ) {
            return response;
        }
        // A stopped session can retain a custom name removed since its last run.
        // Reject before stopping/spawning anything; activation cannot race this check.
        match block_on(self.ports.repository.load(&command.session_id)) {
            Ok(Some(session)) => {
                if let Some(profile) = session.profile() {
                    match self.ports.reads.profiles.get(profile) {
                        Ok(Some(_)) => {}
                        Ok(None) => {
                            return error::profile_unavailable(
                                operation,
                                session_id,
                                profile.as_str(),
                            );
                        }
                        Err(source) => {
                            return error::application(
                                operation,
                                session_id,
                                source.category(),
                                &source,
                            );
                        }
                    }
                }
            }
            Ok(None) => {} // The use case supplies the usual not-found response.
            Err(source) => {
                return error::application(operation, session_id, source.category(), &source);
            }
        }
        let service = RestartSessionService::new(
            self.ports.clock.as_ref(),
            self.ports.coordinator.as_ref(),
            self.ports.repository.as_ref(),
            self.ports.runtime.as_ref(),
            self.ports.processes.as_ref(),
            self.ports.restarts.as_ref(),
            self.ports.projections.as_ref(),
            self.ports.telemetry.as_ref(),
        );
        let result = crate::blocking::block_on_until(
            service.execute_with_cancellation(command, &|| self.operations.reset_requested()),
            || self.operations.abandon_stops_requested(),
        );
        let Some(result) = result else {
            return error::reset_cancelled(operation);
        };
        match result {
            Ok(restarted) => mutation_success(operation, &restarted.session, true, |result| {
                ProductSuccess::RestartSession(result)
            }),
            Err(source) => error::application(operation, session_id, source.category(), &source),
        }
    }

    fn rename(&self, request: undercurrent_protocol::RenameSessionRequest) -> ProductResponse {
        let operation = ProductOperation::RenameSession;
        let session_id = Some(request.session_id.clone());
        let command = match self.boundary.rename_session(request) {
            Ok(command) => command,
            Err(source) => return error::boundary(operation, session_id, source),
        };
        if let Err(response) = self.authorize(
            operation,
            AccessAction::RenameSession,
            &AccessResource::Session(command.session_id.clone()),
            session_id.clone(),
        ) {
            return response;
        }
        let service = RenameSessionService::new(
            self.ports.clock.as_ref(),
            self.ports.coordinator.as_ref(),
            self.ports.repository.as_ref(),
            self.ports.projections.as_ref(),
            self.ports.telemetry.as_ref(),
        );
        let result = crate::blocking::block_on_until(service.execute(command), || {
            self.operations.reset_requested()
        });
        let Some(result) = result else {
            return error::reset_cancelled(operation);
        };
        match result {
            Ok(renamed) => {
                mutation_success(operation, &renamed.session, renamed.changed, |result| {
                    ProductSuccess::RenameSession(result)
                })
            }
            Err(source) => {
                if matches!(
                    source,
                    undercurrent_application::SessionMutationError::NameConflict { .. }
                ) {
                    error::name_conflict(operation, source.to_string())
                } else {
                    error::application(operation, session_id, source.category(), &source)
                }
            }
        }
    }

    fn update_labels(
        &self,
        request: undercurrent_protocol::UpdateSessionLabelsRequest,
    ) -> ProductResponse {
        let operation = ProductOperation::UpdateSessionLabels;
        let session_id = Some(request.session_id.clone());
        let command = match self.boundary.update_session_labels(request) {
            Ok(command) => command,
            Err(source) => return error::boundary(operation, session_id, source),
        };
        if let Err(response) = self.authorize(
            operation,
            AccessAction::ManageConfiguration,
            &AccessResource::Session(command.session_id.clone()),
            session_id.clone(),
        ) {
            return response;
        }
        let service = UpdateSessionLabelsService::new(
            self.ports.clock.as_ref(),
            self.ports.coordinator.as_ref(),
            self.ports.repository.as_ref(),
            self.ports.projections.as_ref(),
            self.ports.telemetry.as_ref(),
        );
        let Some(result) = crate::blocking::block_on_until(service.execute(command), || {
            self.operations.reset_requested()
        }) else {
            return error::reset_cancelled(operation);
        };
        match result {
            Ok(updated) => mutation_success(
                operation,
                &updated.session,
                updated.changed,
                ProductSuccess::UpdateSessionLabels,
            ),
            Err(source) => error::application(operation, session_id, source.category(), &source),
        }
    }

    fn remove(&self, request: undercurrent_protocol::RemoveSessionRequest) -> ProductResponse {
        let operation = ProductOperation::RemoveSession;
        let session_id = Some(request.session_id.clone());
        let command = match self.boundary.remove_session(request) {
            Ok(command) => command,
            Err(source) => return error::boundary(operation, session_id, source),
        };
        if let Err(response) = self.authorize(
            operation,
            AccessAction::RemoveSession,
            &AccessResource::Session(command.session_id.clone()),
            session_id.clone(),
        ) {
            return response;
        }
        let service = RemoveSessionService::new(
            self.ports.clock.as_ref(),
            self.ports.coordinator.as_ref(),
            self.ports.repository.as_ref(),
            self.ports.projections.as_ref(),
            self.ports.telemetry.as_ref(),
        );
        let result = crate::blocking::block_on_until(service.execute(command), || {
            self.operations.reset_requested()
        });
        let Some(result) = result else {
            return error::reset_cancelled(operation);
        };
        match result {
            Ok(removed) => success(ProductSuccess::RemoveSession(RemoveSessionResultDto {
                session_id: removed.session_id.into_string(),
                removal_revision: removed.removal_revision.value(),
            })),
            Err(source) => error::application(operation, session_id, source.category(), &source),
        }
    }

    fn resize(&self, request: undercurrent_protocol::ResizeTerminalRequest) -> ProductResponse {
        let operation = ProductOperation::ResizeTerminal;
        let session_id = Some(request.session_id.clone());
        let command = match self.boundary.resize_terminal(request) {
            Ok(command) => command,
            Err(source) => return error::boundary(operation, session_id, source),
        };
        if let Err(response) = self.authorize(
            operation,
            AccessAction::ResizeTerminal,
            &AccessResource::Session(command.session_id.clone()),
            session_id.clone(),
        ) {
            return response;
        }
        let service = ResizeTerminalService::new(
            self.ports.clock.as_ref(),
            self.ports.coordinator.as_ref(),
            self.ports.repository.as_ref(),
            self.ports.runtime.as_ref(),
            self.ports.processes.as_ref(),
            self.ports.projections.as_ref(),
            self.ports.telemetry.as_ref(),
        );
        let result = crate::blocking::block_on_until(service.execute(command), || {
            self.operations.reset_requested()
        });
        let Some(result) = result else {
            return error::reset_cancelled(operation);
        };
        match result {
            Ok(resized) => {
                mutation_success(operation, &resized.session, resized.changed, |result| {
                    ProductSuccess::ResizeTerminal(result)
                })
            }
            Err(source) => error::application(operation, session_id, source.category(), &source),
        }
    }

    fn input(&self, request: undercurrent_protocol::SendTerminalInputRequest) -> ProductResponse {
        let operation = ProductOperation::SendTerminalInput;
        let session_id = Some(request.session_id.clone());
        let delivery_id = request.delivery_id.clone();
        let command = match self.boundary.send_terminal_input(request) {
            Ok(command) => command,
            Err(source) => return error::boundary(operation, session_id, source),
        };
        if let Err(response) = self.authorize(
            operation,
            AccessAction::SendTerminalInput,
            &AccessResource::Session(command.session_id.clone()),
            session_id.clone(),
        ) {
            return response;
        }
        let service = SendTerminalInputService::new(
            self.ports.clock.as_ref(),
            &undercurrent_runtime::RandomOperationAttemptIdSource,
            self.ports.coordinator.as_ref(),
            self.ports.repository.as_ref(),
            self.ports.runtime.as_ref(),
            self.ports.processes.as_ref(),
            self.ports.reads.profiles.as_ref(),
            self.ports.projections.as_ref(),
            self.ports.deliveries.as_ref(),
            self.ports.projections.as_ref(),
            self.ports.telemetry.as_ref(),
        );
        let result = crate::blocking::block_on_until(service.execute(command), || {
            self.operations.reset_requested()
        });
        let Some(result) = result else {
            return error::reset_cancelled(operation);
        };
        match result {
            Ok(delivered) => success(ProductSuccess::SendTerminalInput(
                SendTerminalInputResultDto {
                    session_id: delivered.session_id.into_string(),
                    delivered_at_millis: delivered.delivered_at.value(),
                    delivery_id,
                    duplicate: delivered.deduplicated,
                    foreground_revision: delivered.foreground_revision.map(|value| value.value()),
                },
            )),
            Err(undercurrent_application::SendTerminalInputError::Mutation(
                undercurrent_application::SessionMutationError::RevisionPreconditionFailed {
                    ..
                },
            )) => error::revision_precondition_failed(operation, session_id),
            Err(source) => error::application(operation, session_id, source.category(), &source),
        }
    }

    fn close_input(
        &self,
        request: undercurrent_protocol::CloseStandardInputRequest,
    ) -> ProductResponse {
        let operation = ProductOperation::CloseStandardInput;
        let session_id = Some(request.session_id.clone());
        let command = match self.boundary.close_standard_input(request) {
            Ok(command) => command,
            Err(source) => return error::boundary(operation, session_id, source),
        };
        if let Err(response) = self.authorize(
            operation,
            AccessAction::CloseStandardInput,
            &AccessResource::Session(command.session_id.clone()),
            session_id.clone(),
        ) {
            return response;
        }
        let service = CloseStandardInputService::new(
            self.ports.clock.as_ref(),
            self.ports.coordinator.as_ref(),
            self.ports.repository.as_ref(),
            self.ports.runtime.as_ref(),
            self.ports.processes.as_ref(),
            self.ports.projections.as_ref(),
            self.ports.telemetry.as_ref(),
        );
        let result = crate::blocking::block_on_until(service.execute(command), || {
            self.operations.reset_requested()
        });
        let Some(result) = result else {
            return error::reset_cancelled(operation);
        };
        match result {
            Ok(closed) => success(ProductSuccess::CloseStandardInput(
                CloseStandardInputResultDto {
                    session_id: closed.session_id.into_string(),
                    closed_at_millis: closed.closed_at.value(),
                    already_closed: closed.already_closed,
                },
            )),
            Err(source) => error::application(operation, session_id, source.category(), &source),
        }
    }

    fn list(
        &self,
        context: &ProductRequestContext,
        request: undercurrent_protocol::ListSessionsRequest,
    ) -> ProductResponse {
        let operation = ProductOperation::ListSessions;
        let query = match self.boundary.session_query(&request) {
            Ok(query) => query,
            Err(source) => return error::boundary(operation, None, source),
        };
        if let Err(response) = self.authorize(
            operation,
            AccessAction::ListSessions,
            &AccessResource::Sessions,
            None,
        ) {
            return response;
        }
        let cursor = match request.cursor.as_deref() {
            Some(token) => match self.cursors.sessions.resolve(&context.client_id, token) {
                Ok(cursor) => Some(cursor),
                Err(source) => return error::cursor(operation, source),
            },
            None => None,
        };
        let service = SessionQueryService::new(
            self.ports.clock.as_ref(),
            self.ports.projections.catalog(),
            self.ports.telemetry.as_ref(),
        );
        let page = match service.list(&query, cursor.as_ref()) {
            Ok(page) => page,
            Err(source) => {
                return error::application(operation, None, source.category(), &source);
            }
        };
        let (mut presented, continuation) = match self.bounded_session_page(&query, &page) {
            Ok(page) => page,
            Err(response) => return response,
        };
        let next_cursor = match continuation {
            Some(cursor) => match self
                .cursors
                .sessions
                .register(context.client_id.clone(), cursor)
            {
                Ok(token) => Some(token),
                Err(source) => return error::cursor(operation, source),
            },
            None => None,
        };
        presented.next_cursor = next_cursor;
        success(ProductSuccess::ListSessions(presented))
    }

    // Reserve the fixed token width while sizing, before registering a cursor.
    // If the byte limit shortens the page, continue after the last returned row;
    // the original continuation would skip the rows removed here.
    // See [bounded listing](../../docs/internals/requests/request-flow.md#where-other-finite-operations-go).
    fn bounded_session_page(
        &self,
        query: &undercurrent_domain::SessionQuery,
        page: &SessionPage,
    ) -> Result<(SessionPageDto, Option<SessionPageCursor>), ProductResponse> {
        let operation = ProductOperation::ListSessions;
        let original_continuation = page.next_cursor.clone();
        let placeholder = original_continuation
            .as_ref()
            .map(|_| SESSION_CURSOR_TOKEN_PLACEHOLDER.to_owned());
        let mut presented = ProductPresentation::session_page(page, placeholder)
            .map_err(|source| error::presentation(operation, source))?;
        if self.session_page_fits(&presented)? {
            return Ok((presented, original_continuation));
        }

        let mut lower = 0usize;
        let mut upper = presented.items.len();
        while lower < upper {
            let candidate_size = lower + (upper - lower).div_ceil(2);
            let mut candidate = presented.clone();
            candidate.items.truncate(candidate_size);
            candidate.next_cursor = Some(SESSION_CURSOR_TOKEN_PLACEHOLDER.to_owned());
            if self.session_page_fits(&candidate)? {
                lower = candidate_size;
            } else {
                upper = candidate_size.saturating_sub(1);
            }
        }
        if lower == 0 {
            return Err(error::response_capacity(
                operation,
                self.response_codec.limits().response_bytes(),
            ));
        }

        presented.items.truncate(lower);
        presented.next_cursor = Some(SESSION_CURSOR_TOKEN_PLACEHOLDER.to_owned());
        let after_session_id = page.items[lower - 1].id().clone();
        let continuation =
            SessionPageCursor::rehydrate(page.snapshot_revision, query.clone(), after_session_id);
        Ok((presented, Some(continuation)))
    }

    fn session_page_fits(&self, page: &SessionPageDto) -> Result<bool, ProductResponse> {
        let response = success(ProductSuccess::ListSessions(page.clone()));
        match self.response_codec.encode_response(&response) {
            Ok(_) => Ok(true),
            Err(source) if source.category() == ProductCodecErrorCategory::Capacity => Ok(false),
            Err(_) => Err(error::result_encoding(ProductOperation::ListSessions)),
        }
    }

    fn upsert_work(
        &self,
        request: undercurrent_protocol::UpsertWorkItemRequest,
    ) -> ProductResponse {
        let operation = ProductOperation::UpsertWorkItem;
        let session_id = Some(request.session_id.clone());
        let command = match self.boundary.upsert_work_item(request) {
            Ok(command) => command,
            Err(source) => return error::boundary(operation, session_id, source),
        };
        if let Err(response) = self.authorize(
            operation,
            AccessAction::WriteWorkContext,
            &AccessResource::Session(command.session_id.clone()),
            session_id.clone(),
        ) {
            return response;
        }
        let service = WorkContextService::new(
            self.ports.clock.as_ref(),
            self.ports.coordinator.as_ref(),
            self.ports.repository.as_ref(),
            self.ports.projections.as_ref(),
            self.ports.telemetry.as_ref(),
        );
        match block_on(service.upsert(command)) {
            Ok(updated) => {
                mutation_success(operation, &updated.session, updated.changed, |result| {
                    ProductSuccess::UpsertWorkItem(result)
                })
            }
            Err(source) => error::application(operation, session_id, source.category(), &source),
        }
    }

    fn remove_work(
        &self,
        request: undercurrent_protocol::RemoveWorkItemRequest,
    ) -> ProductResponse {
        let operation = ProductOperation::RemoveWorkItem;
        let session_id = Some(request.session_id.clone());
        let command = match self.boundary.remove_work_item(request) {
            Ok(command) => command,
            Err(source) => return error::boundary(operation, session_id, source),
        };
        if let Err(response) = self.authorize(
            operation,
            AccessAction::WriteWorkContext,
            &AccessResource::Session(command.session_id.clone()),
            session_id.clone(),
        ) {
            return response;
        }
        let service = WorkContextService::new(
            self.ports.clock.as_ref(),
            self.ports.coordinator.as_ref(),
            self.ports.repository.as_ref(),
            self.ports.projections.as_ref(),
            self.ports.telemetry.as_ref(),
        );
        match block_on(service.remove(command)) {
            Ok(updated) => {
                mutation_success(operation, &updated.session, updated.changed, |result| {
                    ProductSuccess::RemoveWorkItem(result)
                })
            }
            Err(source) => error::application(operation, session_id, source.category(), &source),
        }
    }
}

fn repair_error_category(
    error: &RepairOperationAttemptError,
) -> undercurrent_application::ApplicationErrorCategory {
    use undercurrent_application::ApplicationErrorCategory as Category;
    match error {
        RepairOperationAttemptError::Denied => Category::PermissionDenied,
        RepairOperationAttemptError::NotFound => Category::NotFound,
        RepairOperationAttemptError::NotUnknown
        | RepairOperationAttemptError::StaleEvidence
        | RepairOperationAttemptError::StaleFence { .. }
        | RepairOperationAttemptError::PayloadConflict
        | RepairOperationAttemptError::EvidenceConflict => Category::FailedPrecondition,
        RepairOperationAttemptError::Authorization(source)
        | RepairOperationAttemptError::Load(source)
        | RepairOperationAttemptError::Observation(source)
        | RepairOperationAttemptError::Commit(source)
        | RepairOperationAttemptError::AcknowledgementAfterCommit(source)
        | RepairOperationAttemptError::ReadinessAfterCommit(source)
        | RepairOperationAttemptError::Readiness(source) => source.category(),
    }
}

#[derive(Debug)]
struct RepairResultInvariant;

impl std::fmt::Display for RepairResultInvariant {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("attempt repair result violated its application invariant")
    }
}

impl std::error::Error for RepairResultInvariant {}

fn mutation_success(
    operation: ProductOperation,
    session: &undercurrent_domain::Session,
    changed: bool,
    wrap: impl FnOnce(SessionMutationResultDto) -> ProductSuccess,
) -> ProductResponse {
    match ProductPresentation::session_summary(&session.summary()) {
        Ok(session) => success(wrap(SessionMutationResultDto {
            session,
            changed,
            executable_resolution: None,
        })),
        Err(source) => error::presentation(operation, source),
    }
}

fn start_success(
    operation: ProductOperation,
    session: &undercurrent_domain::Session,
    wrap: impl FnOnce(SessionMutationResultDto) -> ProductSuccess,
) -> ProductResponse {
    let launch = session.configuration().launch();
    let executable_resolution = launch.command().executable_resolution().map(|resolution| {
        undercurrent_protocol::ExecutableResolutionDto {
            launch_mode: match launch.command().launch_mode() {
                undercurrent_domain::LaunchMode::Native => {
                    undercurrent_protocol::LaunchModeDto::Native
                }
                undercurrent_domain::LaunchMode::Sealed => {
                    undercurrent_protocol::LaunchModeDto::Sealed
                }
            },
            requested_reference: resolution.requested_reference().to_owned(),
            resolved_program: launch.command().program().to_owned(),
            content_digest_sha256: resolution.content_digest().to_hex(),
            receipt_digest_sha256: resolution.receipt_digest().to_hex(),
        }
    });
    match ProductPresentation::session_summary(&session.summary()) {
        Ok(session) => success(wrap(SessionMutationResultDto {
            session,
            changed: true,
            executable_resolution,
        })),
        Err(source) => error::presentation(operation, source),
    }
}

fn stop_success(
    operation: ProductOperation,
    session: &undercurrent_domain::Session,
    already_terminal: bool,
    exit_code: Option<i32>,
) -> ProductResponse {
    match ProductPresentation::session_summary(&session.summary()) {
        Ok(session) => success(ProductSuccess::StopSession(StopSessionResultDto {
            session,
            already_terminal,
            exit_code,
        })),
        Err(source) => error::presentation(operation, source),
    }
}

fn attempt_outcome(attempt: &OperationAttempt) -> OperationAttemptOutcomeDto {
    let (effect_certainty, result_digest) = match attempt.state() {
        OperationAttemptState::Accepted => (EffectCertainty::Unknown, None),
        OperationAttemptState::Settled {
            certainty,
            result_digest,
            ..
        } => (*certainty, *result_digest),
        // Compensated is a distinct terminal audit, not a claim that spawn never applied.
        // This legacy certainty projection therefore remains Unknown while the explicit
        // reconciliation flag below truthfully reports that no recovery debt remains.
        OperationAttemptState::Compensated { .. } => (EffectCertainty::Unknown, None),
    };
    OperationAttemptOutcomeDto {
        attempt_id: attempt.id().as_str().to_owned(),
        fencing_token: attempt.fencing_token().value(),
        effect_certainty: match effect_certainty {
            EffectCertainty::KnownAbsent => EffectCertaintyDto::KnownAbsent,
            EffectCertainty::KnownApplied => EffectCertaintyDto::KnownApplied,
            EffectCertainty::Unknown => EffectCertaintyDto::Unknown,
        },
        result_digest_sha256: result_digest.map(|digest| digest.to_hex()),
        reconciliation_required: attempt.state().requires_reconciliation(),
    }
}

fn runtime_cancellation_to_dto(
    result: RuntimeCancellationResult,
) -> Result<
    (
        RuntimeCancellationTargetBindingDto,
        RuntimeCancellationControlBindingDto,
        RuntimeCancellationStateDto,
    ),
    undercurrent_protocol::RuntimeCancellationDtoError,
> {
    let target = RuntimeCancellationTargetBindingDto::new(
        result.target().attempt_id().as_str().to_owned(),
        result.target().request_digest().to_hex(),
        result.target().fencing_token().value(),
        result.target().caller_cell_identity().as_str().to_owned(),
        result.target().session_id().as_str().to_owned(),
        result.target().expected_session_revision().value(),
        result.target().expected_lifecycle_revision().value(),
        result.target().expected_provider_id().to_owned(),
        result.target().expected_provider_generation(),
    )?;
    let material = RuntimeCancellationCommandDigestMaterial::new(
        target.clone(),
        result.control().control_id().as_str().to_owned(),
        result.control().idempotency_key().to_owned(),
        match result.control().mode() {
            RuntimeCancellationMode::Graceful => RuntimeCancellationModeDto::Graceful,
            RuntimeCancellationMode::Force => RuntimeCancellationModeDto::Force,
        },
        result.control().reason().to_owned(),
        match result.control().propagation_policy() {
            RuntimeCancellationPropagationPolicy::TargetOnly => {
                RuntimeCancellationPropagationPolicyDto::TargetOnly
            }
        },
        result.control().deadline().value(),
        result.control().source_product().to_owned(),
        result.control().tenant_id().to_owned(),
        result.control().actor_context_ref().to_owned(),
        result.control().authority_ref().to_owned(),
    )?;
    let control = RuntimeCancellationControlBindingDto::new(
        &material,
        result.control().command_digest().to_hex(),
    )?;
    let state = match result.state() {
        RuntimeCancellationState::AcceptedStopping(session) => {
            RuntimeCancellationStateDto::AcceptedStopping {
                session: runtime_cancellation_session_to_dto(session)?,
            }
        }
        RuntimeCancellationState::AppliedTerminated(session) => {
            RuntimeCancellationStateDto::AppliedTerminated {
                session: runtime_cancellation_session_to_dto(session)?,
            }
        }
        RuntimeCancellationState::AlreadyTerminal(session) => {
            RuntimeCancellationStateDto::AlreadyTerminal {
                session: runtime_cancellation_session_to_dto(session)?,
            }
        }
        RuntimeCancellationState::Unknown(session) => RuntimeCancellationStateDto::Unknown {
            session: runtime_cancellation_session_to_dto(session)?,
        },
    };
    Ok((target, control, state))
}

fn runtime_cancellation_session_to_dto(
    session: &RuntimeCancellationSessionSnapshot,
) -> Result<RuntimeCancellationSessionSnapshotDto, undercurrent_protocol::RuntimeCancellationDtoError>
{
    RuntimeCancellationSessionSnapshotDto::new(
        session.session_id().as_str().to_owned(),
        runtime_attempt_lifecycle(session.lifecycle()),
        session.revision().value(),
        session.lifecycle_revision().value(),
        session.observed_at().value(),
    )
}

fn runtime_attempt_status_to_dto(
    result: undercurrent_application::RuntimeAttemptStatusResult,
) -> Result<RuntimeAttemptStatusResultDto, undercurrent_protocol::RuntimeAttemptStatusDtoError> {
    let binding = RuntimeAttemptBindingDto::new(
        result.binding().caller_cell_identity().as_str().to_owned(),
        result.binding().request_digest().to_hex(),
        result.binding().launch_profile_digest().to_hex(),
        result.binding().environment_binding_digest().to_hex(),
        result.binding().input_digest().to_hex(),
    )?;
    let snapshot = match result.snapshot() {
        RuntimeAttemptStatusSnapshot::Unknown { observed_at } => {
            RuntimeAttemptStatusSnapshotDto::Unknown {
                observed_at_millis: observed_at.value(),
            }
        }
        RuntimeAttemptStatusSnapshot::KnownAbsent { observed_at } => {
            RuntimeAttemptStatusSnapshotDto::KnownAbsent {
                observed_at_millis: observed_at.value(),
            }
        }
        RuntimeAttemptStatusSnapshot::KnownApplied {
            result_digest,
            observed_at,
            session,
        } => RuntimeAttemptStatusSnapshotDto::KnownApplied {
            result_digest_sha256: result_digest.to_hex(),
            observed_at_millis: observed_at.value(),
            session: RuntimeAttemptSessionSnapshotDto::new(
                session.session_id().as_str().to_owned(),
                runtime_attempt_lifecycle(session.lifecycle()),
                session.revision().value(),
                session.lifecycle_revision().value(),
                session.observed_at().value(),
            )?,
        },
        RuntimeAttemptStatusSnapshot::Compensated { observed_at } => {
            RuntimeAttemptStatusSnapshotDto::Compensated {
                observed_at_millis: observed_at.value(),
            }
        }
    };
    RuntimeAttemptStatusResultDto::new(
        result.attempt_id().as_str().to_owned(),
        result.fencing_token().value(),
        binding,
        snapshot,
    )
}

const fn runtime_attempt_lifecycle(lifecycle: LifecycleState) -> LifecycleStateDto {
    match lifecycle {
        LifecycleState::Starting => LifecycleStateDto::Starting,
        LifecycleState::Running => LifecycleStateDto::Running,
        LifecycleState::Stopping => LifecycleStateDto::Stopping,
        LifecycleState::Stopped => LifecycleStateDto::Stopped,
        LifecycleState::Exited(code) => LifecycleStateDto::Exited { code: Some(code) },
        LifecycleState::Failed => LifecycleStateDto::Failed,
    }
}

fn execution_output(output: ExecutionOutput) -> ExecutionOutputDto {
    match output {
        ExecutionOutput::Inline { bytes, integrity } => ExecutionOutputDto::Inline {
            bytes,
            payload_digest_sha256: integrity.digest().to_hex(),
            truncation: integrity.truncated().then_some(OutputTruncationDto {
                source_bytes: integrity.source_bytes(),
                delivered_bytes: integrity.delivered_bytes(),
            }),
        },
        ExecutionOutput::ContentReference(reference) => ExecutionOutputDto::ContentReference {
            uri: reference.uri().to_owned(),
            payload_digest_sha256: reference.digest().to_hex(),
            size_bytes: reference.size_bytes(),
        },
        ExecutionOutput::Stream {
            stream_id,
            available_from,
            next_offset,
        } => ExecutionOutputDto::Stream {
            stream_id,
            available_from,
            next_offset,
        },
    }
}

#[derive(Debug)]
struct MissingSettledAttempt;

impl std::fmt::Display for MissingSettledAttempt {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("settled operation attempt is unavailable")
    }
}

impl std::error::Error for MissingSettledAttempt {}

#[derive(Debug)]
struct ExecuteDeadlineElapsed;

impl std::fmt::Display for ExecuteDeadlineElapsed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("execute deadline elapsed")
    }
}

impl std::error::Error for ExecuteDeadlineElapsed {}

#[derive(Debug)]
struct ExecuteAttemptRejected;

impl std::fmt::Display for ExecuteAttemptRejected {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("execute attempt requires reconciliation")
    }
}

impl std::error::Error for ExecuteAttemptRejected {}

pub(super) fn get_agent_target(
    request: &undercurrent_protocol::GetAgentEventRequest,
) -> Result<AgentChannelTarget, AgentChannelBoundaryInvariant> {
    validate_agent_context(
        &request.tenant_id,
        &request.source_product,
        &request.actor_context_ref,
        &request.authority_ref,
        &request.undercurrent_provider_id,
        request.undercurrent_provider_generation,
        request.attempt_revision,
        request.recovery_epoch,
    )?;
    Ok(AgentChannelTarget {
        tenant_id: request.tenant_id.clone(),
        source_product: request.source_product.clone(),
        actor_context_ref: request.actor_context_ref.clone(),
        authority_ref: request.authority_ref.clone(),
        undercurrent_provider_id: request.undercurrent_provider_id.clone(),
        undercurrent_provider_generation: request.undercurrent_provider_generation,
        session_id: SessionId::new(request.session_id.clone())
            .map_err(|_| AgentChannelBoundaryInvariant("agent session identity is invalid"))?,
        process_instance_id: None,
        attempt_id: OperationAttemptId::new(request.attempt_id.clone())
            .map_err(|_| AgentChannelBoundaryInvariant("agent attempt identity is invalid"))?,
        attempt_revision: request.attempt_revision,
        recovery_epoch: request.recovery_epoch,
        fence: FencingToken::new(request.fence)
            .map_err(|_| AgentChannelBoundaryInvariant("agent fence is invalid"))?,
        request_digest: PayloadDigest::from_hex(&request.request_digest_sha256)
            .map_err(|_| AgentChannelBoundaryInvariant("agent request digest is invalid"))?,
    })
}

pub(super) fn agent_event_dto(
    event: undercurrent_application::AgentEventRecord,
) -> Result<AgentEventDto, AgentChannelBoundaryInvariant> {
    let call = match serde_json::from_slice::<AgentToolCallDto>(&event.call_canonical_json) {
        Ok(call)
            if canonical_agent_tool_call(&call)
                .is_ok_and(|canonical| canonical == event.call_canonical_json) =>
        {
            call
        }
        _ => {
            return Err(AgentChannelBoundaryInvariant(
                "retained agent event is not canonical protocol data",
            ));
        }
    };
    let Some(process_instance_id) = event.target.process_instance_id else {
        return Err(AgentChannelBoundaryInvariant(
            "retained agent event lacks its process identity",
        ));
    };
    Ok(AgentEventDto {
        event_id: event.event_id,
        sequence: event.sequence,
        session_id: event.target.session_id.as_str().to_owned(),
        process_instance_id: process_instance_id.as_str().to_owned(),
        attempt_id: event.target.attempt_id.as_str().to_owned(),
        attempt_revision: event.target.attempt_revision,
        recovery_epoch: event.target.recovery_epoch,
        fence: event.target.fence.value(),
        request_digest_sha256: event.target.request_digest.to_hex(),
        call_digest_sha256: event.call_digest.to_hex(),
        call,
        received_at_millis: event.received_at.value(),
    })
}

fn reply_agent_target(
    request: &undercurrent_protocol::ReplyAgentEventRequest,
) -> Result<AgentChannelTarget, AgentChannelBoundaryInvariant> {
    validate_agent_context(
        &request.tenant_id,
        &request.source_product,
        &request.actor_context_ref,
        &request.authority_ref,
        &request.undercurrent_provider_id,
        request.undercurrent_provider_generation,
        request.attempt_revision,
        request.recovery_epoch,
    )?;
    Ok(AgentChannelTarget {
        tenant_id: request.tenant_id.clone(),
        source_product: request.source_product.clone(),
        actor_context_ref: request.actor_context_ref.clone(),
        authority_ref: request.authority_ref.clone(),
        undercurrent_provider_id: request.undercurrent_provider_id.clone(),
        undercurrent_provider_generation: request.undercurrent_provider_generation,
        session_id: SessionId::new(request.session_id.clone())
            .map_err(|_| AgentChannelBoundaryInvariant("agent session identity is invalid"))?,
        process_instance_id: Some(
            ProcessInstanceId::new(request.process_instance_id.clone())
                .map_err(|_| AgentChannelBoundaryInvariant("agent process identity is invalid"))?,
        ),
        attempt_id: OperationAttemptId::new(request.attempt_id.clone())
            .map_err(|_| AgentChannelBoundaryInvariant("agent attempt identity is invalid"))?,
        attempt_revision: request.attempt_revision,
        recovery_epoch: request.recovery_epoch,
        fence: FencingToken::new(request.fence)
            .map_err(|_| AgentChannelBoundaryInvariant("agent fence is invalid"))?,
        request_digest: PayloadDigest::from_hex(&request.request_digest_sha256)
            .map_err(|_| AgentChannelBoundaryInvariant("agent request digest is invalid"))?,
    })
}

#[allow(clippy::too_many_arguments)]
fn validate_agent_context(
    tenant_id: &str,
    source_product: &str,
    actor_context_ref: &str,
    authority_ref: &str,
    provider_id: &str,
    provider_generation: u64,
    attempt_revision: u64,
    recovery_epoch: u64,
) -> Result<(), AgentChannelBoundaryInvariant> {
    if [
        tenant_id,
        source_product,
        actor_context_ref,
        authority_ref,
        provider_id,
    ]
    .into_iter()
    .any(|value| value.is_empty() || value.len() > 512 || value.chars().any(char::is_control))
        || provider_generation == 0
        || attempt_revision == 0
        || recovery_epoch == 0
    {
        return Err(AgentChannelBoundaryInvariant(
            "agent channel identity is invalid",
        ));
    }
    Ok(())
}

fn agent_channel_state_error(
    operation: ProductOperation,
    session_id: Option<String>,
    category: undercurrent_application::ApplicationErrorCategory,
    message: &'static str,
) -> ProductResponse {
    error::application(
        operation,
        session_id,
        category,
        &AgentChannelBoundaryInvariant(message),
    )
}

#[derive(Debug)]
pub(super) struct AgentChannelBoundaryInvariant(&'static str);

impl Display for AgentChannelBoundaryInvariant {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for AgentChannelBoundaryInvariant {}

#[derive(Debug)]
struct RuntimeCancellationBoundaryInvariant;

impl std::fmt::Display for RuntimeCancellationBoundaryInvariant {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("validated runtime-cancellation request lost mutation context")
    }
}

impl std::error::Error for RuntimeCancellationBoundaryInvariant {}

fn runtime_cancellation_error_response(
    operation: ProductOperation,
    session_id: Option<String>,
    source: &CancelRuntimeAttemptError,
) -> ProductResponse {
    if matches!(
        source,
        CancelRuntimeAttemptError::EffectUnknown(_)
            | CancelRuntimeAttemptError::EffectStartConflict
            | CancelRuntimeAttemptError::Settlement(_)
            | CancelRuntimeAttemptError::SettlementConflict
    ) {
        error::application_with_disposition(
            operation,
            session_id,
            source.category(),
            ProductErrorDisposition::UnknownInconclusive,
            source,
        )
    } else {
        error::application(operation, session_id, source.category(), source)
    }
}

fn success(value: ProductSuccess) -> ProductResponse {
    ProductResponse::Success(Box::new(value))
}

#[cfg(test)]
mod tests {
    use std::io;

    use undercurrent_application::{PortError, PortErrorKind, PortOperation};

    use super::*;

    #[test]
    fn post_effect_settlement_failure_is_unknown_and_status_reconcilable() {
        let source = CancelRuntimeAttemptError::Settlement(Box::new(PortError::new(
            PortErrorKind::Unavailable,
            PortOperation::SettleRuntimeCancellation,
            None,
            io::Error::other("private settlement failure"),
        )));

        let ProductResponse::Error(error) = runtime_cancellation_error_response(
            ProductOperation::CancelRuntimeAttempt,
            Some("session-1".to_owned()),
            &source,
        ) else {
            panic!("settlement failure must remain an error")
        };
        assert_eq!(
            error.disposition,
            Some(ProductErrorDisposition::UnknownInconclusive)
        );
        assert!(error.retryable);
    }
}
