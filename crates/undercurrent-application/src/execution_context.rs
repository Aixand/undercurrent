// SPDX-License-Identifier: Apache-2.0

//! Authorized resolution of private one-shot process execution context.

use std::error::Error;
use std::fmt::{Display, Formatter};

use undercurrent_domain::{SessionId, TimestampMillis};

use crate::{
    AccessAction, AccessDecision, AccessDenialReason, AccessResource, ApplicationErrorCategory,
    ApplicationOperation, AuthorizationPort, Clock, DiagnosticEventName, DiagnosticLevel,
    DiagnosticRecord, ExecutionContextResolver, MetricName, MetricObservation, PortError,
    ResolvedExecutionContext, SessionRepository, Telemetry,
};

/// Failure from an authorized private execution-context read.
#[derive(Debug)]
pub enum ExecutionContextError {
    /// No valid authentication was established for the request.
    Unauthenticated { session_id: SessionId },
    /// The authenticated principal lacks one-shot execution permission.
    PermissionDenied { session_id: SessionId },
    /// No durable session exists under the supplied stable ID.
    NotFound { session_id: SessionId },
    /// No live directory observation belongs to the current process.
    LiveDirectoryUnavailable { session_id: SessionId },
    /// No exported environment has been captured for the current shell.
    LiveEnvironmentUnavailable { session_id: SessionId },
    /// Authorization, persistence, or context resolution failed.
    Infrastructure(PortError),
}

impl ExecutionContextError {
    /// Returns the stable category used by protocol adapters.
    pub const fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::Unauthenticated { .. } => ApplicationErrorCategory::Unauthenticated,
            Self::PermissionDenied { .. } => ApplicationErrorCategory::PermissionDenied,
            Self::NotFound { .. } => ApplicationErrorCategory::NotFound,
            Self::LiveDirectoryUnavailable { .. } | Self::LiveEnvironmentUnavailable { .. } => {
                ApplicationErrorCategory::Unavailable
            }
            Self::Infrastructure(error) => error.category(),
        }
    }

    /// Returns the requested safe session identity when available.
    pub fn session_id(&self) -> Option<&SessionId> {
        match self {
            Self::Unauthenticated { session_id }
            | Self::PermissionDenied { session_id }
            | Self::NotFound { session_id }
            | Self::LiveDirectoryUnavailable { session_id }
            | Self::LiveEnvironmentUnavailable { session_id } => Some(session_id),
            Self::Infrastructure(error) => error.session_id(),
        }
    }
}

impl Display for ExecutionContextError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unauthenticated { session_id } => write!(
                formatter,
                "authentication is required to execute in session {session_id}"
            ),
            Self::PermissionDenied { session_id } => write!(
                formatter,
                "permission denied to execute in session {session_id}"
            ),
            Self::NotFound { session_id } => {
                write!(formatter, "session {session_id} was not found")
            }
            Self::LiveDirectoryUnavailable { .. } => formatter.write_str(
                "live directory unavailable; use --dir-launch to execute in the launch directory",
            ),
            Self::LiveEnvironmentUnavailable { .. } => formatter.write_str(
                "live shell environment unavailable; update shell integration with uc init, or use --env-launch",
            ),
            Self::Infrastructure(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for ExecutionContextError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Infrastructure(error) => Some(error),
            Self::Unauthenticated { .. }
            | Self::PermissionDenied { .. }
            | Self::NotFound { .. }
            | Self::LiveDirectoryUnavailable { .. }
            | Self::LiveEnvironmentUnavailable { .. } => None,
        }
    }
}

/// Authorizes and resolves one session's private launch environment and cwd.
pub struct ResolveExecutionContextService<'a> {
    authorizer: &'a dyn AuthorizationPort,
    repository: &'a dyn SessionRepository,
    resolver: &'a dyn ExecutionContextResolver,
    clock: &'a dyn Clock,
    telemetry: &'a dyn Telemetry,
}

impl<'a> ResolveExecutionContextService<'a> {
    /// Constructs the use case without performing IO.
    pub const fn new(
        authorizer: &'a dyn AuthorizationPort,
        repository: &'a dyn SessionRepository,
        resolver: &'a dyn ExecutionContextResolver,
        clock: &'a dyn Clock,
        telemetry: &'a dyn Telemetry,
    ) -> Self {
        Self {
            authorizer,
            repository,
            resolver,
            clock,
            telemetry,
        }
    }

    /// Returns private process context only after independent authorization.
    ///
    /// Authorization completes before persistence and environment-file IO. The
    /// result contains environment values and is only suitable for a one-shot
    /// execution adapter. Diagnostics retain identities and categories but no
    /// paths, variable names, or values. This method does not panic unless an
    /// injected port panics.
    ///
    /// # Errors
    ///
    /// Returns typed authentication, permission, absence, persistence, or
    /// context-resolution failures.
    pub async fn resolve(
        &self,
        session_id: &SessionId,
    ) -> Result<ResolvedExecutionContext, ExecutionContextError> {
        self.resolve_directory(session_id, None, true).await
    }

    /// Resolves the selected directory and launch values or a private live-environment reference.
    /// Live directory observations must belong to the current process incarnation.
    pub async fn resolve_for_execution(
        &self,
        session_id: &SessionId,
        catalog: &crate::PreparedSessionCatalog,
        dir_launch: bool,
        env_launch: bool,
    ) -> Result<ResolvedExecutionContext, ExecutionContextError> {
        self.resolve_directory(session_id, (!dir_launch).then_some(catalog), env_launch)
            .await
    }

    async fn resolve_directory(
        &self,
        session_id: &SessionId,
        live_catalog: Option<&crate::PreparedSessionCatalog>,
        env_launch: bool,
    ) -> Result<ResolvedExecutionContext, ExecutionContextError> {
        let started_at = self.clock.now();
        let resource = AccessResource::Session(session_id.clone());
        let decision = self
            .authorizer
            .authorize(AccessAction::ExecuteInSessionContext, &resource)
            .await
            .map_err(|error| self.port_failure(started_at, session_id, error))?;
        if let AccessDecision::Deny(reason) = decision {
            let error = match reason {
                AccessDenialReason::Unauthenticated => ExecutionContextError::Unauthenticated {
                    session_id: session_id.clone(),
                },
                AccessDenialReason::Forbidden => ExecutionContextError::PermissionDenied {
                    session_id: session_id.clone(),
                },
            };
            self.finish(
                started_at,
                DiagnosticEventName::AuthorizationDenied,
                DiagnosticLevel::Debug,
                Some(error.category()),
                session_id,
            );
            return Err(error);
        }

        let session = self
            .repository
            .load(session_id)
            .await
            .map_err(|error| self.port_failure(started_at, session_id, error))?
            .ok_or_else(|| {
                let error = ExecutionContextError::NotFound {
                    session_id: session_id.clone(),
                };
                self.finish(
                    started_at,
                    DiagnosticEventName::ExecutionContextFailed,
                    DiagnosticLevel::Debug,
                    Some(error.category()),
                    session_id,
                );
                error
            })?;
        let launch = session.configuration().launch();
        let mut context = if env_launch {
            self.resolver.resolve(launch).await
        } else {
            self.resolver.resolve_directory(launch).await
        }
        .map_err(|error| self.port_failure(started_at, session_id, error))?;
        if let Some(catalog) = live_catalog {
            let prepared = catalog.get(session_id).map_err(|source| {
                self.port_failure(
                    started_at,
                    session_id,
                    PortError::new(
                        crate::PortErrorKind::Other,
                        crate::PortOperation::ReadEffectiveProfile,
                        Some(session_id.clone()),
                        source,
                    ),
                )
            })?;
            let mut summary = session.summary();
            if let Some(prepared) = prepared {
                summary.retain_effective_profile_from(&prepared);
            }
            let directory = summary
                .effective_profile()
                .and_then(|profile| profile.current_directory())
                .ok_or_else(|| {
                    let error = ExecutionContextError::LiveDirectoryUnavailable {
                        session_id: session_id.clone(),
                    };
                    self.finish(
                        started_at,
                        DiagnosticEventName::ExecutionContextFailed,
                        DiagnosticLevel::Debug,
                        Some(error.category()),
                        session_id,
                    );
                    error
                })?;
            context = ResolvedExecutionContext::new(
                context.environment().clone(),
                Some(directory.as_str().to_owned()),
            );
        }
        if !env_launch {
            let process = session.process().map(|process| process.id());
            let observed = match process {
                Some(process) => self
                    .resolver
                    .shell_environment_observed_at(session_id, process)
                    .await
                    .map_err(|error| self.port_failure(started_at, session_id, error))?,
                None => None,
            };
            if observed.is_none() {
                let error = ExecutionContextError::LiveEnvironmentUnavailable {
                    session_id: session_id.clone(),
                };
                self.finish(
                    started_at,
                    DiagnosticEventName::ExecutionContextFailed,
                    DiagnosticLevel::Debug,
                    Some(error.category()),
                    session_id,
                );
                return Err(error);
            }
            context =
                context.with_live_environment(process.expect("observed process exists").clone());
        }
        self.finish(
            started_at,
            DiagnosticEventName::ExecutionContextResolved,
            DiagnosticLevel::Info,
            None,
            session_id,
        );
        Ok(context)
    }

    fn port_failure(
        &self,
        started_at: TimestampMillis,
        session_id: &SessionId,
        error: PortError,
    ) -> ExecutionContextError {
        let category = error.category();
        let operation = error.operation();
        let kind = error.kind();
        self.finish_with_port(started_at, Some(category), session_id, operation, kind);
        ExecutionContextError::Infrastructure(error)
    }

    fn finish(
        &self,
        started_at: TimestampMillis,
        event: DiagnosticEventName,
        level: DiagnosticLevel,
        application_error: Option<ApplicationErrorCategory>,
        session_id: &SessionId,
    ) {
        let finished_at = self.clock.now();
        self.metric(started_at, finished_at);
        self.telemetry.emit_diagnostic(&DiagnosticRecord {
            event,
            level,
            timestamp: finished_at,
            session_id: Some(session_id.clone()),
            application_operation: Some(ApplicationOperation::ResolveExecutionContext),
            application_error,
            operation: None,
            error_kind: None,
        });
    }

    fn finish_with_port(
        &self,
        started_at: TimestampMillis,
        application_error: Option<ApplicationErrorCategory>,
        session_id: &SessionId,
        operation: crate::PortOperation,
        error_kind: crate::PortErrorKind,
    ) {
        let finished_at = self.clock.now();
        self.metric(started_at, finished_at);
        self.telemetry.emit_diagnostic(&DiagnosticRecord {
            event: DiagnosticEventName::ExecutionContextFailed,
            level: DiagnosticLevel::for_application_error(
                application_error.unwrap_or(ApplicationErrorCategory::Internal),
            ),
            timestamp: finished_at,
            session_id: Some(session_id.clone()),
            application_operation: Some(ApplicationOperation::ResolveExecutionContext),
            application_error,
            operation: Some(operation),
            error_kind: Some(error_kind),
        });
    }

    fn metric(&self, started_at: TimestampMillis, finished_at: TimestampMillis) {
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::OperationDurationSeconds,
            value: finished_at
                .saturating_duration_since(started_at)
                .as_secs_f64(),
            operation: Some(ApplicationOperation::ResolveExecutionContext),
        });
    }
}
