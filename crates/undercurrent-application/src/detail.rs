// SPDX-License-Identifier: Apache-2.0

//! Authorized, projection-safe detailed session reads.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

use undercurrent_domain::{
    ExecutableResolution, ExecutionEnvironmentBinding, ExecutionPolicy, ForegroundDecision,
    HealthObservation, HealthRules, Labels, Metadata, OutputCaptureMode, PathReference,
    ProfileBindingMode, ProfileName, ProfileSelectionOrigin, Revision, Session, SessionId,
    SessionSummary, TerminalSize, TimestampMillis, WorkItem,
};

use crate::{
    AccessAction, AccessDecision, AccessDenialReason, AccessResource, ApplicationErrorCategory,
    ApplicationOperation, AuthorizationPort, Clock, DiagnosticEventName, DiagnosticLevel,
    DiagnosticRecord, MetricName, MetricObservation, PortError, PortErrorKind, PortOperation,
    PreparedSessionCatalog, ProcessInstance, SessionProcessDirectory, SessionRepository, Telemetry,
};

/// Typed optional sections of one detailed session response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionDetailProjection {
    configuration: bool,
    work_context: bool,
    process: bool,
    foreground_evidence: bool,
    health_results: bool,
}

impl SessionDetailProjection {
    /// Returns only the complete compact session summary.
    pub const OVERVIEW: Self = Self {
        configuration: false,
        work_context: false,
        process: false,
        foreground_evidence: false,
        health_results: false,
    };

    /// Returns every currently supported detail section.
    pub const COMPLETE: Self = Self {
        configuration: true,
        work_context: true,
        process: true,
        foreground_evidence: true,
        health_results: true,
    };

    /// Constructs an explicit typed detail projection.
    ///
    /// This function performs no IO, cannot fail, and does not panic.
    pub const fn new(
        configuration: bool,
        work_context: bool,
        process: bool,
        foreground_evidence: bool,
        health_results: bool,
    ) -> Self {
        Self {
            configuration,
            work_context,
            process,
            foreground_evidence,
            health_results,
        }
    }

    /// Returns whether launch and execution configuration is requested.
    pub const fn includes_configuration(self) -> bool {
        self.configuration
    }

    /// Returns whether complete WorkContext declarations are requested.
    pub const fn includes_work_context(self) -> bool {
        self.work_context
    }

    /// Returns whether the current runtime process binding is requested.
    pub const fn includes_process(self) -> bool {
        self.process
    }

    /// Returns whether complete foreground evidence rationale is requested.
    pub const fn includes_foreground_evidence(self) -> bool {
        self.foreground_evidence
    }

    /// Returns whether complete per-rule health results are requested.
    pub const fn includes_health_results(self) -> bool {
        self.health_results
    }
}

/// Authorized command detail with payload-redacted diagnostics.
#[derive(Clone, PartialEq, Eq)]
pub struct CommandDetail {
    launch_mode: undercurrent_domain::LaunchMode,
    program: String,
    arguments: Vec<String>,
    executable_resolution: Option<ExecutableResolution>,
}

impl CommandDetail {
    /// Returns the executable policy saved with this session.
    pub const fn launch_mode(&self) -> undercurrent_domain::LaunchMode {
        self.launch_mode
    }

    /// Returns the configured executable to an authorized detail presenter.
    pub fn program(&self) -> &str {
        &self.program
    }

    /// Returns configured arguments to an authorized detail presenter.
    ///
    /// Arguments may contain private data and must not be logged or traced.
    pub fn arguments(&self) -> &[String] {
        &self.arguments
    }

    /// Returns the durable pre-acceptance executable resolution receipt.
    pub const fn executable_resolution(&self) -> Option<&ExecutableResolution> {
        self.executable_resolution.as_ref()
    }
}

impl Debug for CommandDetail {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CommandDetail")
            .field("program_bytes", &self.program.len())
            .field("argument_count", &self.arguments.len())
            .field(
                "executable_is_pinned",
                &self.executable_resolution.is_some(),
            )
            .finish_non_exhaustive()
    }
}

/// Environment information safe for a detailed response.
///
/// Explicit values are deliberately absent. The process adapter remains the
/// only boundary that can read those values from the domain launch contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvironmentDetail {
    variable_names: Vec<String>,
    environment_file: Option<PathReference>,
    binding: Option<ExecutionEnvironmentBinding>,
}

impl EnvironmentDetail {
    /// Returns explicit variable names in deterministic order.
    pub fn variable_names(&self) -> &[String] {
        &self.variable_names
    }

    /// Returns the optional opaque environment-file reference.
    pub const fn environment_file(&self) -> Option<&PathReference> {
        self.environment_file.as_ref()
    }

    /// Returns the supplied environment boundary used for working-directory resolution.
    pub const fn binding(&self) -> Option<&ExecutionEnvironmentBinding> {
        self.binding.as_ref()
    }
}

/// Authorized durable configuration projection without environment values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionConfigurationDetail {
    command: CommandDetail,
    environment: EnvironmentDetail,
    working_directory: Option<PathReference>,
    terminal_size: TerminalSize,
    output_capture_mode: OutputCaptureMode,
    execution: ExecutionPolicy,
    health_rules: HealthRules,
    profile: Option<ProfileName>,
    profile_origin: Option<ProfileSelectionOrigin>,
    profile_binding: ProfileBindingMode,
    labels: Labels,
    metadata: Metadata,
}

impl SessionConfigurationDetail {
    /// Returns authorized command detail.
    pub const fn command(&self) -> &CommandDetail {
        &self.command
    }

    /// Returns environment names and references without explicit values.
    pub const fn environment(&self) -> &EnvironmentDetail {
        &self.environment
    }

    /// Returns the optional opaque working-directory reference.
    pub const fn working_directory(&self) -> Option<&PathReference> {
        self.working_directory.as_ref()
    }

    /// Returns current durable terminal dimensions.
    pub const fn terminal_size(&self) -> TerminalSize {
        self.terminal_size
    }

    /// Returns how process output is interpreted and retained.
    pub const fn output_capture_mode(&self) -> OutputCaptureMode {
        self.output_capture_mode
    }

    /// Returns restart, stop, detection, and removal policy.
    pub const fn execution(&self) -> &ExecutionPolicy {
        &self.execution
    }

    /// Returns configured health rules.
    pub const fn health_rules(&self) -> &HealthRules {
        &self.health_rules
    }

    /// Returns the selected foreground-detection profile.
    pub const fn profile(&self) -> Option<&ProfileName> {
        self.profile.as_ref()
    }

    /// Returns the durable base-profile selection provenance.
    pub const fn profile_origin(&self) -> Option<ProfileSelectionOrigin> {
        self.profile_origin
    }

    /// Returns the effective-profile binding policy.
    pub const fn profile_binding(&self) -> ProfileBindingMode {
        self.profile_binding
    }

    /// Returns complete session labels.
    pub const fn labels(&self) -> &Labels {
        &self.labels
    }

    /// Returns private metadata to the already-authorized presenter.
    ///
    /// Values must not be logged, traced, or added to compact projections.
    pub const fn metadata(&self) -> &Metadata {
        &self.metadata
    }
}

/// Complete declared-work section loaded only by explicit projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkContextDetail {
    revision: Revision,
    items: Vec<WorkItem>,
}

impl WorkContextDetail {
    /// Returns the WorkContext revision represented by this detail.
    pub const fn revision(&self) -> Revision {
        self.revision
    }

    /// Returns declarations in deterministic compound-key order.
    pub fn items(&self) -> &[WorkItem] {
        &self.items
    }
}

/// Persistence status attached to a successfully loaded durable detail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionPersistenceStatus {
    /// The detail was loaded from a durable record.
    Persisted,
}

/// One authorized detailed session response with explicitly requested sections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionDetail {
    restoration_directory: Option<PathReference>,
    summary: SessionSummary,
    configuration: Option<SessionConfigurationDetail>,
    work_context: Option<WorkContextDetail>,
    process: Option<ProcessInstance>,
    foreground_decision: Option<ForegroundDecision>,
    health_observation: Option<HealthObservation>,
    persistence: SessionPersistenceStatus,
}

impl SessionDetail {
    /// Last durable root-directory observation for a group-created terminal.
    pub const fn restoration_directory(&self) -> Option<&PathReference> {
        self.restoration_directory.as_ref()
    }

    /// Returns the complete compact session projection.
    pub const fn summary(&self) -> &SessionSummary {
        &self.summary
    }

    /// Returns configuration only when explicitly projected.
    pub const fn configuration(&self) -> Option<&SessionConfigurationDetail> {
        self.configuration.as_ref()
    }

    /// Returns WorkContext declarations only when explicitly projected.
    pub const fn work_context(&self) -> Option<&WorkContextDetail> {
        self.work_context.as_ref()
    }

    /// Returns the current process binding when projected and present.
    pub const fn process(&self) -> Option<&ProcessInstance> {
        self.process.as_ref()
    }

    /// Returns complete foreground rationale only when separately authorized and projected.
    pub const fn foreground_decision(&self) -> Option<&ForegroundDecision> {
        self.foreground_decision.as_ref()
    }

    /// Returns detailed health results only when separately authorized and projected.
    pub const fn health_observation(&self) -> Option<&HealthObservation> {
        self.health_observation.as_ref()
    }

    /// Returns durable persistence status.
    pub const fn persistence(&self) -> SessionPersistenceStatus {
        self.persistence
    }
}

/// Failure from an authorized detailed session read.
#[derive(Debug)]
pub enum SessionDetailError {
    /// No valid authentication was established for the request.
    Unauthenticated {
        /// Stable requested session identity.
        session_id: SessionId,
    },
    /// The authenticated principal lacks detailed-read permission.
    PermissionDenied {
        /// Stable requested session identity.
        session_id: SessionId,
    },
    /// No durable session exists under the supplied stable ID.
    NotFound {
        /// Stable requested session identity.
        session_id: SessionId,
    },
    /// Authorization, persistence, or process-directory infrastructure failed.
    Infrastructure(PortError),
}

impl SessionDetailError {
    /// Returns the stable category used by protocol and presentation adapters.
    pub const fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::Unauthenticated { .. } => ApplicationErrorCategory::Unauthenticated,
            Self::PermissionDenied { .. } => ApplicationErrorCategory::PermissionDenied,
            Self::NotFound { .. } => ApplicationErrorCategory::NotFound,
            Self::Infrastructure(error) => error.category(),
        }
    }

    /// Returns the requested safe session identity when available.
    pub fn session_id(&self) -> Option<&SessionId> {
        match self {
            Self::Unauthenticated { session_id }
            | Self::PermissionDenied { session_id }
            | Self::NotFound { session_id } => Some(session_id),
            Self::Infrastructure(error) => error.session_id(),
        }
    }
}

impl Display for SessionDetailError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unauthenticated { session_id } => {
                write!(
                    formatter,
                    "authentication is required to inspect session {session_id}"
                )
            }
            Self::PermissionDenied { session_id } => {
                write!(
                    formatter,
                    "permission denied to inspect session {session_id}"
                )
            }
            Self::NotFound { session_id } => {
                write!(formatter, "session {session_id} was not found")
            }
            Self::Infrastructure(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for SessionDetailError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Infrastructure(error) => Some(error),
            Self::Unauthenticated { .. }
            | Self::PermissionDenied { .. }
            | Self::NotFound { .. } => None,
        }
    }
}

/// Authorizes and loads detailed session data without widening list queries.
pub struct GetSessionDetailService<'a> {
    authorizer: &'a dyn AuthorizationPort,
    repository: &'a dyn SessionRepository,
    processes: &'a dyn SessionProcessDirectory,
    catalog: &'a PreparedSessionCatalog,
    clock: &'a dyn Clock,
    telemetry: &'a dyn Telemetry,
}

impl<'a> GetSessionDetailService<'a> {
    /// Constructs the detailed-read use case without performing IO.
    pub const fn new(
        authorizer: &'a dyn AuthorizationPort,
        repository: &'a dyn SessionRepository,
        processes: &'a dyn SessionProcessDirectory,
        catalog: &'a PreparedSessionCatalog,
        clock: &'a dyn Clock,
        telemetry: &'a dyn Telemetry,
    ) -> Self {
        Self {
            authorizer,
            repository,
            processes,
            catalog,
            clock,
            telemetry,
        }
    }

    /// Authorizes and loads durable detail enriched with matching live profile context.
    ///
    /// Authorization completes before persistence or process-directory access.
    /// The result never contains explicit environment values. This method may
    /// cross authorization and persistence ports and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed authentication, permission, absence, or infrastructure
    /// failures.
    pub async fn get(
        &self,
        session_id: &SessionId,
        projection: SessionDetailProjection,
    ) -> Result<SessionDetail, SessionDetailError> {
        let started_at = self.clock.now();
        let resource = AccessResource::Session(session_id.clone());
        let mut actions = vec![AccessAction::ReadSessionDetail];
        if projection.includes_work_context() {
            actions.push(AccessAction::ReadWorkContext);
        }
        if projection.includes_foreground_evidence() {
            actions.push(AccessAction::ExplainForeground);
        }
        if projection.includes_health_results() {
            actions.push(AccessAction::ReadHealth);
        }
        for action in actions {
            self.authorize(started_at, session_id, action, &resource)
                .await?;
        }

        let loaded = match self.repository.load(session_id).await {
            Ok(loaded) => loaded,
            Err(error) => {
                self.finish_port_failure(started_at, &error, session_id);
                return Err(SessionDetailError::Infrastructure(error));
            }
        };
        let session = loaded.ok_or_else(|| SessionDetailError::NotFound {
            session_id: session_id.clone(),
        });
        let session = match session {
            Ok(session) => session,
            Err(error) => {
                self.finish(
                    started_at,
                    DiagnosticEventName::SessionDetailFailed,
                    DiagnosticLevel::Debug,
                    Some(error.category()),
                    session_id,
                );
                return Err(error);
            }
        };

        let process = if projection.includes_process() {
            match self.processes.lookup(session_id) {
                Ok(process) => process.filter(|binding| {
                    session
                        .process()
                        .is_some_and(|durable| binding.matches(durable))
                }),
                Err(error) => {
                    self.finish_port_failure(started_at, &error, session_id);
                    return Err(SessionDetailError::Infrastructure(error));
                }
            }
        } else {
            None
        };
        let mut detail = project_detail(&session, projection, process);
        let prepared = self.catalog.get(session_id).map_err(|source| {
            let error = PortError::new(
                PortErrorKind::Other,
                PortOperation::ReadEffectiveProfile,
                Some(session_id.clone()),
                source,
            );
            self.finish_port_failure(started_at, &error, session_id);
            SessionDetailError::Infrastructure(error)
        })?;
        // Keep durable fields authoritative and reject context from a replaced process
        // or a different profile configuration.
        if let Some(prepared) = prepared {
            detail.summary.retain_effective_profile_from(&prepared);
        }
        self.finish(
            started_at,
            DiagnosticEventName::SessionDetailLoaded,
            DiagnosticLevel::Info,
            None,
            session_id,
        );
        Ok(detail)
    }

    async fn authorize(
        &self,
        started_at: TimestampMillis,
        session_id: &SessionId,
        action: AccessAction,
        resource: &AccessResource,
    ) -> Result<(), SessionDetailError> {
        let decision = match self.authorizer.authorize(action, resource).await {
            Ok(decision) => decision,
            Err(error) => {
                self.finish_port_failure(started_at, &error, session_id);
                return Err(SessionDetailError::Infrastructure(error));
            }
        };
        let AccessDecision::Deny(reason) = decision else {
            return Ok(());
        };
        let error = match reason {
            AccessDenialReason::Unauthenticated => SessionDetailError::Unauthenticated {
                session_id: session_id.clone(),
            },
            AccessDenialReason::Forbidden => SessionDetailError::PermissionDenied {
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
        Err(error)
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
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::OperationDurationSeconds,
            value: finished_at
                .saturating_duration_since(started_at)
                .as_secs_f64(),
            operation: Some(ApplicationOperation::GetSessionDetail),
        });
        self.telemetry.emit_diagnostic(&DiagnosticRecord {
            event,
            level,
            timestamp: finished_at,
            session_id: Some(session_id.clone()),
            application_operation: Some(ApplicationOperation::GetSessionDetail),
            application_error,
            operation: None,
            error_kind: None,
        });
    }

    fn finish_port_failure(
        &self,
        started_at: TimestampMillis,
        error: &PortError,
        session_id: &SessionId,
    ) {
        let finished_at = self.clock.now();
        self.telemetry.record_metric(MetricObservation {
            name: MetricName::OperationDurationSeconds,
            value: finished_at
                .saturating_duration_since(started_at)
                .as_secs_f64(),
            operation: Some(ApplicationOperation::GetSessionDetail),
        });
        self.telemetry.emit_diagnostic(&DiagnosticRecord {
            event: DiagnosticEventName::SessionDetailFailed,
            level: DiagnosticLevel::for_application_error(error.category()),
            timestamp: finished_at,
            session_id: Some(session_id.clone()),
            application_operation: Some(ApplicationOperation::GetSessionDetail),
            application_error: Some(error.category()),
            operation: Some(error.operation()),
            error_kind: Some(error.kind()),
        });
    }
}

fn project_detail(
    session: &Session,
    projection: SessionDetailProjection,
    process: Option<ProcessInstance>,
) -> SessionDetail {
    let configuration = projection
        .includes_configuration()
        .then(|| project_configuration(session));
    let work_context = projection
        .includes_work_context()
        .then(|| WorkContextDetail {
            revision: session.work_context().revision(),
            items: session.work_context().items().cloned().collect(),
        });
    SessionDetail {
        restoration_directory: session.restoration_directory().cloned(),
        summary: session.summary(),
        configuration,
        work_context,
        process,
        foreground_decision: projection
            .includes_foreground_evidence()
            .then(|| session.foreground_decision().clone()),
        health_observation: projection
            .includes_health_results()
            .then(|| session.health_observation().clone()),
        persistence: SessionPersistenceStatus::Persisted,
    }
}

fn project_configuration(session: &Session) -> SessionConfigurationDetail {
    let configuration = session.configuration();
    let launch = configuration.launch();
    SessionConfigurationDetail {
        command: CommandDetail {
            launch_mode: launch.command().launch_mode(),
            program: launch.command().program().to_owned(),
            arguments: launch.command().arguments().to_vec(),
            executable_resolution: launch.command().executable_resolution().cloned(),
        },
        environment: EnvironmentDetail {
            variable_names: launch
                .environment()
                .names()
                .chain(launch.secret_environment().names())
                .map(str::to_owned)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            environment_file: launch.environment_file().cloned(),
            binding: launch.environment_binding().cloned(),
        },
        working_directory: launch.working_directory().cloned(),
        terminal_size: launch.terminal_size(),
        output_capture_mode: launch.output_capture_mode(),
        execution: configuration.execution().clone(),
        health_rules: configuration.health_rules().clone(),
        profile: configuration.profile().cloned(),
        profile_origin: configuration.profile_origin(),
        profile_binding: configuration.profile_binding(),
        labels: configuration.labels().clone(),
        metadata: configuration.metadata().clone(),
    }
}
