// SPDX-License-Identifier: Apache-2.0

use undercurrent_application::{
    AccessDecision, AccessResource, GetSessionDetailService, OutputGapRecovery,
    ReadSessionEventsService, ReadTerminalOutputService, ReadTerminalScreenService,
    ResolveExecutionContextService,
};
use undercurrent_protocol::{
    GetExecutionContextRequest, GetSessionDetailRequest, ProductOperation, ProductResponse,
    ProductSuccess, ReadSessionEventsRequest, ReadTerminalOutputRequest,
    ReadTerminalScreenDiffRequest, ReadTerminalScreenRequest,
};

use super::{ApplicationProductExecutor, error};
use crate::{ProductPresentation, ProductRequestContext, blocking::block_on};

impl ApplicationProductExecutor {
    /// Applies the configured authorization policy before finite operation IO.
    pub(super) fn authorize(
        &self,
        operation: ProductOperation,
        action: undercurrent_application::AccessAction,
        resource: &AccessResource,
        session_id: Option<String>,
    ) -> Result<(), ProductResponse> {
        match block_on(self.ports.reads.authorizer.authorize(action, resource)) {
            Ok(AccessDecision::Permit) => Ok(()),
            Ok(AccessDecision::Deny(reason)) => {
                Err(error::authorization_denied(operation, session_id, reason))
            }
            Err(source) => Err(error::authorization_failure(operation, session_id, &source)),
        }
    }

    pub(super) fn detail(&self, request: GetSessionDetailRequest) -> ProductResponse {
        let operation = ProductOperation::GetSessionDetail;
        let session_id = Some(request.session_id.clone());
        let (session, projection) = match self.boundary.session_detail(&request) {
            Ok(value) => value,
            Err(source) => return error::boundary(operation, session_id, source),
        };
        let service = GetSessionDetailService::new(
            self.ports.reads.authorizer.as_ref(),
            self.ports.repository.as_ref(),
            self.ports.processes.as_ref(),
            self.ports.projections.catalog(),
            self.ports.clock.as_ref(),
            self.ports.telemetry.as_ref(),
        );
        match block_on(service.get(&session, projection)) {
            Ok(detail) => match ProductPresentation::session_detail(&detail) {
                Ok(mut projected) => {
                    if let Some(process) = detail.summary().process() {
                        match block_on(
                            self.ports
                                .execution_context
                                .shell_environment_observed_at(&session, process),
                        ) {
                            Ok(Some(observed_at)) => {
                                projected.shell_environment =
                                    Some(undercurrent_protocol::ShellEnvironmentStatusDto {
                                        process_id: process.as_str().to_owned(),
                                        observed_at_millis: observed_at.value(),
                                    })
                            }
                            Ok(None) => {}
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
                    success(ProductSuccess::GetSessionDetail(Box::new(projected)))
                }
                Err(source) => error::presentation(operation, source),
            },
            Err(source) => error::application(
                operation,
                source
                    .session_id()
                    .map(|id| id.as_str().to_owned())
                    .or(session_id),
                source.category(),
                &source,
            ),
        }
    }

    pub(super) fn execution_context(&self, request: GetExecutionContextRequest) -> ProductResponse {
        let operation = ProductOperation::GetExecutionContext;
        let session_id = Some(request.session_id.clone());
        let session = match self.boundary.execution_context(&request) {
            Ok(session) => session,
            Err(source) => return error::boundary(operation, session_id, source),
        };
        let service = ResolveExecutionContextService::new(
            self.ports.reads.authorizer.as_ref(),
            self.ports.repository.as_ref(),
            self.ports.execution_context.as_ref(),
            self.ports.clock.as_ref(),
            self.ports.telemetry.as_ref(),
        );
        match block_on(service.resolve(&session)) {
            Ok(context) => success(ProductSuccess::GetExecutionContext(
                ProductPresentation::execution_context(&context),
            )),
            Err(source) => error::application(
                operation,
                source
                    .session_id()
                    .map(|id| id.as_str().to_owned())
                    .or(session_id),
                source.category(),
                &source,
            ),
        }
    }

    pub(super) fn output(&self, request: ReadTerminalOutputRequest) -> ProductResponse {
        let operation = ProductOperation::ReadTerminalOutput;
        let session_id = Some(request.session_id.clone());
        let (session, from, max_bytes) = match self.boundary.terminal_output_read(&request) {
            Ok(value) => value,
            Err(source) => return error::boundary(operation, session_id, source),
        };
        let service = ReadTerminalOutputService::new(
            self.ports.reads.authorizer.as_ref(),
            self.ports.reads.output.as_ref(),
            self.ports.clock.as_ref(),
            self.ports.telemetry.as_ref(),
        );
        match block_on(service.read(
            &session,
            from,
            max_bytes,
            OutputGapRecovery::RestartAtAvailableOffset,
        )) {
            Ok(replay) => match ProductPresentation::terminal_output(&replay) {
                Ok(page) => success(ProductSuccess::ReadTerminalOutput(page)),
                Err(source) => error::presentation(operation, source),
            },
            Err(source) => error::application(
                operation,
                source
                    .session_id()
                    .map(|id| id.as_str().to_owned())
                    .or(session_id),
                source.category(),
                &source,
            ),
        }
    }

    pub(super) fn screen(
        &self,
        context: &ProductRequestContext,
        request: ReadTerminalScreenRequest,
    ) -> ProductResponse {
        let operation = ProductOperation::ReadTerminalScreen;
        let session_id = Some(request.session_id.clone());
        let session = match self.boundary.terminal_screen_read(&request) {
            Ok(session) => session,
            Err(source) => return error::boundary(operation, session_id, source),
        };
        let service = ReadTerminalScreenService::new(
            self.ports.reads.authorizer.as_ref(),
            self.ports.reads.screens.as_ref(),
            self.ports.clock.as_ref(),
            self.ports.telemetry.as_ref(),
        );
        match block_on(service.read(&session)) {
            Ok(snapshot) => {
                let checkpoint = match self.cursors.screens.register(
                    context.client_id.clone(),
                    session,
                    snapshot.clone(),
                ) {
                    Ok(checkpoint) => checkpoint,
                    Err(source) => return error::screen_checkpoint(operation, source),
                };
                success(ProductSuccess::ReadTerminalScreen(
                    ProductPresentation::terminal_screen(&snapshot, checkpoint),
                ))
            }
            Err(source) => error::application(
                operation,
                source
                    .session_id()
                    .map(|id| id.as_str().to_owned())
                    .or(session_id),
                source.category(),
                &source,
            ),
        }
    }

    pub(super) fn screen_diff(
        &self,
        context: &ProductRequestContext,
        request: ReadTerminalScreenDiffRequest,
    ) -> ProductResponse {
        let operation = ProductOperation::ReadTerminalScreenDiff;
        let session_id = Some(request.session_id.clone());
        let session = match self.boundary.terminal_screen_diff(&request) {
            Ok(session) => session,
            Err(source) => return error::boundary(operation, session_id, source),
        };
        if let Err(response) = self.authorize(
            operation,
            undercurrent_application::AccessAction::ReadScreen,
            &AccessResource::Session(session.clone()),
            session_id.clone(),
        ) {
            return response;
        }
        let checkpoint =
            match self
                .cursors
                .screens
                .resolve(&context.client_id, &request.checkpoint, &session)
            {
                Ok(checkpoint) => checkpoint,
                Err(source) => return error::screen_checkpoint(operation, source),
            };
        let service = ReadTerminalScreenService::new(
            self.ports.reads.authorizer.as_ref(),
            self.ports.reads.screens.as_ref(),
            self.ports.clock.as_ref(),
            self.ports.telemetry.as_ref(),
        );
        match block_on(service.diff(&session, checkpoint.snapshot())) {
            Ok(update) => {
                let next_checkpoint = match self.cursors.screens.register(
                    context.client_id.clone(),
                    session,
                    update.current().clone(),
                ) {
                    Ok(checkpoint) => checkpoint,
                    Err(source) => return error::screen_checkpoint(operation, source),
                };
                success(ProductSuccess::ReadTerminalScreenDiff(
                    ProductPresentation::terminal_screen_update(&update, next_checkpoint),
                ))
            }
            Err(source) => error::application(
                operation,
                source
                    .session_id()
                    .map(|id| id.as_str().to_owned())
                    .or(session_id),
                source.category(),
                &source,
            ),
        }
    }

    pub(super) fn events(&self, request: ReadSessionEventsRequest) -> ProductResponse {
        let operation = ProductOperation::ReadSessionEvents;
        let include_observation = request.include_observation;
        let request = match self.boundary.session_event_read(&request) {
            Ok(request) => request,
            Err(source) => return error::boundary(operation, None, source),
        };
        let service = ReadSessionEventsService::new(
            self.ports.reads.authorizer.as_ref(),
            self.ports.reads.events.reader.as_ref(),
            self.ports.clock.as_ref(),
            self.ports.telemetry.as_ref(),
        );
        match block_on(service.read(&request)) {
            Ok(batch) => success(ProductSuccess::ReadSessionEvents({
                let mut page = ProductPresentation::session_events(&batch);
                if !include_observation {
                    for event in &mut page.items {
                        event.observation = None;
                    }
                }
                page
            })),
            Err(source) => error::application(operation, None, source.category(), &source),
        }
    }
}

fn success(value: ProductSuccess) -> ProductResponse {
    ProductResponse::Success(Box::new(value))
}
