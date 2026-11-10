// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::time::Duration;

use undercurrent_application::{
    AccessAction, AccessResource, ApplicationErrorCategory, AuthorizeDiagnosticBundleService,
    DiagnosticBundleRequest, OutputGapRecovery, ReadTerminalOutputService,
    ShutdownProcessHostError,
};
use undercurrent_protocol::{
    CreateDiagnosticBundleRequest, DaemonStatusDto, DiagnosticBundleDto,
    DiagnosticBundleTerminalContentDto, DiagnosticPageDto, DiagnosticRowDto, DiagnosticSectionDto,
    DiagnosticValueDto, GetDiagnosticsRequest, ProductErrorDisposition, ProductOperation,
    ProductResponse, ProductSuccess, ReadTerminalOutputRequest, ShutdownAcceptedDto,
    ShutdownDaemonRequest,
};

use super::{ApplicationProductExecutor, error};
use crate::{
    DaemonDiagnosticQuery, DaemonDiagnosticRow, DaemonDiagnosticSection, DaemonDiagnosticValue,
    DaemonProductShutdownError, DaemonStatusSnapshot, ProductPresentation, ProductRequestContext,
    blocking::block_on,
};

impl ApplicationProductExecutor {
    fn stop_sessions_for_shutdown(
        &self,
        timeout: Duration,
    ) -> Result<(), DaemonProductShutdownError> {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use undercurrent_application::{StopMode, StopSessionCommand, StopSessionService};
        let deadline = std::time::Instant::now() + timeout;
        let sessions = self
            .ports
            .processes
            .session_ids()
            .map_err(|_| DaemonProductShutdownError::SessionTermination)?;
        let next = AtomicUsize::new(0);
        let failed = AtomicBool::new(false);
        // Keep completion work bounded even when the daemon owns many sessions.
        std::thread::scope(|scope| {
            for _ in 0..sessions.len().min(8) {
                scope.spawn(|| {
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
                    while let Some(session_id) = sessions.get(next.fetch_add(1, Ordering::Relaxed))
                    {
                        // Existing recovery debt remains protected. The host-wide
                        // shutdown can still retire its processes without inventing
                        // a new per-session outcome for the unresolved operation.
                        let request = undercurrent_protocol::ProductRequest::StopSession(
                            undercurrent_protocol::StopSessionRequest {
                                mutation: Default::default(),
                                session_id: session_id.to_string(),
                                expected_revision: None,
                                force: true,
                                graceful_only: false,
                            },
                        );
                        if self.attempt_admission_blocker(&request).is_some() {
                            continue;
                        }
                        let result = crate::blocking::block_on_until(
                            service.execute(StopSessionCommand {
                                session_id: session_id.clone(),
                                expected_revision: None,
                                mode: StopMode::Force,
                            }),
                            || std::time::Instant::now() >= deadline,
                        );
                        if !matches!(result, Some(Ok(_))) {
                            failed.store(true, Ordering::Release);
                        }
                    }
                });
            }
        });
        if failed.load(Ordering::Acquire) {
            Err(DaemonProductShutdownError::SessionTermination)
        } else {
            Ok(())
        }
    }

    pub(super) fn diagnostics(
        &self,
        context: &ProductRequestContext,
        request: GetDiagnosticsRequest,
    ) -> ProductResponse {
        let operation = ProductOperation::GetDiagnostics;
        if let Err(response) = self.authorize(
            operation,
            AccessAction::ReadDiagnostics,
            &AccessResource::Diagnostics,
            None,
        ) {
            return response;
        }
        let query = DaemonDiagnosticQuery {
            sections: request.sections.into_iter().map(section_from_dto).collect(),
            page_size: usize::try_from(request.page_size).unwrap_or(usize::MAX),
            cursor: request.cursor,
        };
        match self.operations.diagnostics(&context.client_id, query) {
            Ok(page) => success(ProductSuccess::GetDiagnostics(DiagnosticPageDto {
                items: page.items.into_iter().map(row_to_dto).collect(),
                next_cursor: page.next_cursor,
                captured_at_millis: page.captured_at_millis,
            })),
            Err(source) => error::daemon_operations(operation, source),
        }
    }

    pub(super) fn diagnostic_bundle(
        &self,
        context: &ProductRequestContext,
        request: CreateDiagnosticBundleRequest,
    ) -> ProductResponse {
        let operation = ProductOperation::CreateDiagnosticBundle;
        let target_session = request
            .terminal_content
            .as_ref()
            .map(|content| content.session_id.clone());
        if let Err(source) = request.validate() {
            return error::application(
                operation,
                target_session,
                ApplicationErrorCategory::InvalidArgument,
                &source,
            );
        }

        let terminal = match request.terminal_content {
            Some(content) => {
                let boundary_request = ReadTerminalOutputRequest {
                    session_id: content.session_id.clone(),
                    from_offset: content.from_offset,
                    max_bytes: content.max_bytes,
                };
                match self.boundary.terminal_output_read(&boundary_request) {
                    Ok((session, from, max_bytes)) => {
                        Some((content.session_id, session, from, max_bytes))
                    }
                    Err(source) => {
                        return error::boundary(operation, target_session, source);
                    }
                }
            }
            None => None,
        };

        let authorization_request = match terminal.as_ref() {
            Some((_, session, _, _)) => {
                DiagnosticBundleRequest::safe().explicitly_include_terminal_content(session.clone())
            }
            None => DiagnosticBundleRequest::safe(),
        };
        let authorization = match block_on(
            AuthorizeDiagnosticBundleService::new(self.ports.reads.authorizer.as_ref())
                .authorize(authorization_request),
        ) {
            Ok(authorization) => authorization,
            Err(source) => {
                return error::application(operation, target_session, source.category(), &source);
            }
        };

        let terminal_content = match terminal {
            Some((session_id, session, from, max_bytes)) => {
                let service = ReadTerminalOutputService::new(
                    self.ports.reads.authorizer.as_ref(),
                    self.ports.reads.output.as_ref(),
                    self.ports.clock.as_ref(),
                    self.ports.telemetry.as_ref(),
                );
                let replay = match block_on(service.read(
                    &session,
                    from,
                    max_bytes,
                    OutputGapRecovery::RestartAtAvailableOffset,
                )) {
                    Ok(replay) => replay,
                    Err(source) => {
                        return error::application(
                            operation,
                            Some(session_id),
                            source.category(),
                            &source,
                        );
                    }
                };
                let page = match ProductPresentation::terminal_output(&replay) {
                    Ok(page) => page,
                    Err(source) => return error::presentation(operation, source),
                };
                Some(DiagnosticBundleTerminalContentDto { session_id, page })
            }
            None => None,
        };

        let query = DaemonDiagnosticQuery {
            sections: request.sections.into_iter().map(section_from_dto).collect(),
            page_size: usize::try_from(request.page_size).unwrap_or(usize::MAX),
            cursor: request.cursor,
        };
        match self.operations.diagnostics(&context.client_id, query) {
            Ok(page) => success(ProductSuccess::CreateDiagnosticBundle(
                DiagnosticBundleDto {
                    diagnostics: DiagnosticPageDto {
                        items: page.items.into_iter().map(row_to_dto).collect(),
                        next_cursor: page.next_cursor,
                        captured_at_millis: page.captured_at_millis,
                    },
                    terminal_content,
                    terminal_content_redacted: !authorization.includes_terminal_content(),
                },
            )),
            Err(source) => error::daemon_operations(operation, source),
        }
    }

    pub(super) fn daemon_status(&self) -> ProductResponse {
        let operation = ProductOperation::GetDaemonStatus;
        if let Err(response) = self.authorize(
            operation,
            AccessAction::ReadDiagnostics,
            &AccessResource::Daemon,
            None,
        ) {
            return response;
        }
        match self.operations.status() {
            Ok(status) => match status_to_dto(status) {
                Ok(status) => success(ProductSuccess::GetDaemonStatus(status)),
                Err(source) => error::daemon_operations(operation, source),
            },
            Err(source) => error::daemon_operations(operation, source),
        }
    }

    pub(super) fn shutdown_daemon(
        &self,
        context: &crate::ProductRequestContext,
        request: ShutdownDaemonRequest,
    ) -> ProductResponse {
        let operation = ProductOperation::ShutdownDaemon;
        if let Err(response) = self.authorize(
            operation,
            AccessAction::ControlDaemon,
            &AccessResource::Daemon,
            None,
        ) {
            return response;
        }
        let timeout = Duration::from_millis(request.drain_timeout_millis);
        let result = match request.mode {
            undercurrent_protocol::DaemonShutdownModeDto::Graceful => {
                self.operations.begin_product_shutdown(timeout)
            }
            undercurrent_protocol::DaemonShutdownModeDto::Reset => {
                self.operations.begin_reset_shutdown(timeout, context)
            }
            undercurrent_protocol::DaemonShutdownModeDto::Immediate => self
                .operations
                .begin_immediate_shutdown(timeout, context, &mut |remaining| {
                    self.stop_sessions_for_shutdown(remaining)
                }),
        };
        match result {
            Ok(drain_deadline_millis) => {
                success(ProductSuccess::ShutdownDaemon(ShutdownAcceptedDto {
                    drain_deadline_millis,
                }))
            }
            Err(DaemonProductShutdownError::Operations(source)) => {
                error::daemon_operations(operation, source)
            }
            Err(DaemonProductShutdownError::NotConfigured) => {
                error::invalid_configuration(operation)
            }
            Err(DaemonProductShutdownError::Host(source)) => error::application_with_disposition(
                operation,
                None,
                shutdown_error_category(&source),
                shutdown_error_disposition(&source),
                &source,
            ),
            Err(source @ DaemonProductShutdownError::AlreadyInProgress) => error::application(
                operation,
                None,
                ApplicationErrorCategory::FailedPrecondition,
                &source,
            ),
            Err(source @ DaemonProductShutdownError::MutationDrainTimedOut) => {
                error::application(operation, None, ApplicationErrorCategory::Timeout, &source)
            }
            Err(
                source @ (DaemonProductShutdownError::StateInvariant
                | DaemonProductShutdownError::SessionTermination),
            ) => error::application(operation, None, ApplicationErrorCategory::Internal, &source),
        }
    }
}

fn shutdown_error_category(error: &ShutdownProcessHostError) -> ApplicationErrorCategory {
    match error {
        ShutdownProcessHostError::Admission(source)
        | ShutdownProcessHostError::SettlementAfterEffect(source)
        | ShutdownProcessHostError::AcknowledgementAfterCommit(source) => source.category(),
        ShutdownProcessHostError::KnownAbsentSettlement { settlement, .. } => settlement.category(),
        ShutdownProcessHostError::PayloadConflict
        | ShutdownProcessHostError::StaleFence { .. }
        | ShutdownProcessHostError::KnownAbsent { .. }
        | ShutdownProcessHostError::RejectedKnownAbsent { .. }
        | ShutdownProcessHostError::KnownAbsentSettlementRejected { .. } => {
            ApplicationErrorCategory::Conflict
        }
        ShutdownProcessHostError::UnresolvedEffect { .. }
        | ShutdownProcessHostError::EffectUnknown(_)
        | ShutdownProcessHostError::UnknownSettlement { .. }
        | ShutdownProcessHostError::UnknownSettlementRejected { .. } => {
            ApplicationErrorCategory::FailedPrecondition
        }
        ShutdownProcessHostError::StoreContractViolation
        | ShutdownProcessHostError::SettlementRejected => ApplicationErrorCategory::Internal,
    }
}

fn shutdown_error_disposition(error: &ShutdownProcessHostError) -> ProductErrorDisposition {
    match error {
        ShutdownProcessHostError::UnresolvedEffect { .. }
        | ShutdownProcessHostError::EffectUnknown(_)
        | ShutdownProcessHostError::UnknownSettlement { .. }
        | ShutdownProcessHostError::UnknownSettlementRejected { .. } => {
            ProductErrorDisposition::UnknownInconclusive
        }
        other => ProductErrorDisposition::default_for_category(error::product_category(
            shutdown_error_category(other),
        )),
    }
}

#[cfg(test)]
mod error_taxonomy_tests {
    use std::io;

    use undercurrent_application::{PortError, PortErrorKind, PortOperation};
    use undercurrent_domain::OperationAttemptId;

    use super::*;

    fn port_error() -> Box<PortError> {
        Box::new(PortError::new(
            PortErrorKind::Unavailable,
            PortOperation::ShutdownProcessHost,
            None,
            io::Error::other("private host detail"),
        ))
    }

    #[test]
    fn uref_034_shutdown_unknown_and_inconclusive_variants_keep_exact_disposition() {
        let errors = [
            ShutdownProcessHostError::UnresolvedEffect {
                attempt_id: OperationAttemptId::new("shutdown:unknown").expect("attempt ID"),
            },
            ShutdownProcessHostError::EffectUnknown(port_error()),
            ShutdownProcessHostError::UnknownSettlement {
                effect: port_error(),
                settlement: port_error(),
            },
            ShutdownProcessHostError::UnknownSettlementRejected {
                effect: port_error(),
            },
        ];

        for error in errors {
            assert_eq!(
                shutdown_error_disposition(&error),
                ProductErrorDisposition::UnknownInconclusive
            );
        }
    }
}

fn section_from_dto(section: DiagnosticSectionDto) -> DaemonDiagnosticSection {
    match section {
        DiagnosticSectionDto::Connections => DaemonDiagnosticSection::Connections,
        DiagnosticSectionDto::Streams => DaemonDiagnosticSection::Streams,
        DiagnosticSectionDto::SlowConsumers => DaemonDiagnosticSection::SlowConsumers,
        DiagnosticSectionDto::OutputProducers => DaemonDiagnosticSection::OutputProducers,
        DiagnosticSectionDto::SupervisorQueues => DaemonDiagnosticSection::SupervisorQueues,
        DiagnosticSectionDto::DetectionEvidence => DaemonDiagnosticSection::DetectionEvidence,
        DiagnosticSectionDto::Persistence => DaemonDiagnosticSection::Persistence,
        DiagnosticSectionDto::Reconciliation => DaemonDiagnosticSection::Reconciliation,
        DiagnosticSectionDto::Configuration => DaemonDiagnosticSection::Configuration,
    }
}

fn section_to_dto(section: DaemonDiagnosticSection) -> DiagnosticSectionDto {
    match section {
        DaemonDiagnosticSection::Connections => DiagnosticSectionDto::Connections,
        DaemonDiagnosticSection::Streams => DiagnosticSectionDto::Streams,
        DaemonDiagnosticSection::SlowConsumers => DiagnosticSectionDto::SlowConsumers,
        DaemonDiagnosticSection::OutputProducers => DiagnosticSectionDto::OutputProducers,
        DaemonDiagnosticSection::SupervisorQueues => DiagnosticSectionDto::SupervisorQueues,
        DaemonDiagnosticSection::DetectionEvidence => DiagnosticSectionDto::DetectionEvidence,
        DaemonDiagnosticSection::Persistence => DiagnosticSectionDto::Persistence,
        DaemonDiagnosticSection::Reconciliation => DiagnosticSectionDto::Reconciliation,
        DaemonDiagnosticSection::Configuration => DiagnosticSectionDto::Configuration,
    }
}

fn row_to_dto(row: DaemonDiagnosticRow) -> DiagnosticRowDto {
    DiagnosticRowDto {
        section: section_to_dto(row.section),
        identity: row.identity,
        fields: row
            .fields
            .into_iter()
            .map(|(field, value)| (field.as_str().to_owned(), value_to_dto(value)))
            .collect::<BTreeMap<_, _>>(),
    }
}

fn value_to_dto(value: DaemonDiagnosticValue) -> DiagnosticValueDto {
    match value {
        DaemonDiagnosticValue::Boolean(value) => DiagnosticValueDto::Boolean(value),
        DaemonDiagnosticValue::Unsigned(value) => DiagnosticValueDto::Unsigned(value),
        DaemonDiagnosticValue::Signed(value) => DiagnosticValueDto::Signed(value),
        DaemonDiagnosticValue::Text(value) => DiagnosticValueDto::Text(value),
    }
}

fn status_to_dto(
    status: DaemonStatusSnapshot,
) -> Result<DaemonStatusDto, crate::DaemonOperationsError> {
    Ok(DaemonStatusDto {
        server_epoch: status.server_epoch.as_str().to_owned(),
        started_at_millis: status.started_at_millis,
        accepting_connections: status.accepting_connections,
        physical_connections: u32::try_from(status.physical_connections)
            .map_err(|_| crate::DaemonOperationsError::SourceInvariant)?,
        logical_streams: u32::try_from(status.logical_streams)
            .map_err(|_| crate::DaemonOperationsError::SourceInvariant)?,
        managed_sessions: u32::try_from(status.managed_sessions)
            .map_err(|_| crate::DaemonOperationsError::SourceInvariant)?,
    })
}

fn success(value: ProductSuccess) -> ProductResponse {
    ProductResponse::Success(Box::new(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_limits_remain_unsigned_in_the_public_diagnostic_row() {
        let row = DaemonDiagnosticRow {
            section: DaemonDiagnosticSection::Streams,
            identity: "stream:client-1:stream-1".to_owned(),
            fields: BTreeMap::from([
                (
                    crate::DaemonDiagnosticField::QueueByteLimit,
                    DaemonDiagnosticValue::Unsigned(4_096),
                ),
                (
                    crate::DaemonDiagnosticField::QueueMessageLimit,
                    DaemonDiagnosticValue::Unsigned(32),
                ),
            ]),
        };

        let dto = row_to_dto(row);

        assert_eq!(
            dto.fields.get("queue_byte_limit"),
            Some(&DiagnosticValueDto::Unsigned(4_096))
        );
        assert_eq!(
            dto.fields.get("queue_message_limit"),
            Some(&DiagnosticValueDto::Unsigned(32))
        );
    }
}
