// SPDX-License-Identifier: Apache-2.0

//! Validated translation between the product protocol and application inputs.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::time::Duration;

use undercurrent_application::{
    CloseStandardInputCommand, ExecuteInSessionCommand, ExecutionOutputLimits,
    InvalidExecutionOutputLimits, InvalidSessionEventRequest, OperationAttemptAdmission,
    OutboxSequence, OutputOffset, RemovalConfirmation, RemoveSessionCommand, RemoveWorkItemCommand,
    RenameSessionCommand, ResizeTerminalCommand, RestartSessionCommand, SendTerminalInputCommand,
    SessionDetailProjection, SessionEventFilter, SessionEventReadRequest, SessionEventStart,
    StartSessionCommand, StopMode, StopSessionCommand, UpdateSessionLabelsCommand,
    UpsertWorkItemCommand,
};
use undercurrent_domain::{
    AgentRuntimeBinding, CommandSpec, ControlKey, DeliveryId, DispatchCellIdentity, DomainError,
    EnvironmentName, EnvironmentVariables, ErrorCategory, ExecutionEnvironmentBinding,
    ExecutionLimits, ExecutionPolicy, ExecutionState, FunctionKey, HealthRule, HealthRules,
    HealthStatus, InputItem, InputObservationPolicy, InputSequence, InteractionState,
    InvalidValueReason, Key, KeyModifiers, LabelPatch, Labels, LaunchSpec, LifecycleKind, Metadata,
    ModifiableKey, MouseButton, MouseEvent, MouseEventKind, NameContains, OperationAttemptId,
    OutputCaptureMode, PageSize, PathReference, PayloadDigest, ProcessMutationKind,
    ProfileBindingMode, ProfileMatchTarget, ProfileName, ProfileSampleView, RestartConfig,
    RestartPolicy, RuntimeCallbackRef, RuntimeDispatchBinding, RuntimeDispatchLimits,
    RuntimeDispatchPolicy, RuntimeOutputContract, RuntimeRetentionPolicyRef,
    RuntimeTelemetryPolicyRef, SecretEnvironmentVariables, SecretReference, SessionConfiguration,
    SessionFilter, SessionId, SessionName, SessionProjection, SessionQuery, SessionSort,
    SessionSortField, SortDirection, StopPolicy, StopSignal, TerminalSize, TimestampMillis,
    WorkItem, WorkItemId, WorkItemKey, WorkOwner, WorkProvenance, WorkRelationKind, WorkStatus,
    WorkTarget,
};
use undercurrent_protocol::{
    CanonicalPayloadError, CloseStandardInputRequest, ExecuteInSessionRequest, ExecutionStateDto,
    GetExecutionContextRequest, GetProfileRequest, GetSessionDetailRequest, HealthRuleDto,
    HealthStatusDto, InputObservationPolicyDto, InteractionStateDto, KeyDto, LifecycleKindDto,
    LintProfilesRequest, ListProfilesRequest, ListSessionsRequest, MatchProfileRequest,
    ModifiableKeyDto, MouseButtonDto, MouseEventDto, MouseEventKindDto, OutputCaptureModeDto,
    ProductError, ProductErrorCategory, ProductOperation, ProfileBindingModeDto,
    ReadSessionEventsRequest, ReadTerminalOutputRequest, ReadTerminalScreenDiffRequest,
    ReadTerminalScreenRequest, RemovalConfirmationDto, RemoveSessionRequest, RemoveWorkItemRequest,
    RenameSessionRequest, ResizeTerminalRequest, RestartPolicyDto, RestartSessionRequest,
    SendTerminalInputRequest, SessionEventsSpec, SessionProjectionDto, SessionSortFieldDto,
    SessionWatchSpec, SortDirectionDto, StartSessionRequest, StopSessionRequest, StopSignalDto,
    TerminalInitialModeDto, TerminalInputDto, TerminalOutputSpec, TestProfileRequest,
    UpdateSessionLabelsRequest, UpsertWorkItemRequest, WorkRelationshipDto, WorkStatusDto,
    WorkTargetDto, canonical_product_request_digest,
};

use crate::event_kind::to_domain;

/// Hard bounds used while admitting product requests into application values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductBoundaryLimits {
    /// Process launch and health declaration limits.
    pub execution: ExecutionLimits,
    /// Maximum terminal columns accepted from a client.
    pub max_terminal_columns: u16,
    /// Maximum terminal rows accepted from a client.
    pub max_terminal_rows: u16,
    /// Maximum typed items in one atomic input request.
    pub max_input_items: usize,
    /// Maximum estimated encoded bytes in one atomic input request.
    pub max_input_bytes: usize,
    /// Maximum summaries in one prepared query page.
    pub max_query_page_size: usize,
    /// Maximum WorkContext declarations retained by one new session.
    pub max_work_items: usize,
    /// Limits for durable event filters and pages.
    pub events: ProductEventBoundaryLimits,
    /// Limits for standalone detection-profile requests.
    pub profiles: ProductProfileBoundaryLimits,
}

impl ProductBoundaryLimits {
    /// Validates every boundary limit before the daemon accepts requests.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed domain validation failure when a limit is zero or the
    /// WorkContext limit cannot be represented by the product protocol.
    pub fn validate(self) -> Result<Self, ProductBoundaryError> {
        self.execution.validate()?;
        if self.max_terminal_columns == 0
            || self.max_terminal_rows == 0
            || self.max_input_items == 0
            || self.max_input_bytes == 0
            || self.max_query_page_size == 0
            || self.max_work_items == 0
        {
            return Err(ProductBoundaryError::InvalidLimits);
        }
        if self.max_work_items > u32::MAX as usize {
            return Err(ProductBoundaryError::InvalidLimits);
        }
        self.events.validate()?;
        self.profiles.validate()?;
        Ok(self)
    }
}

/// Hard bounds for one durable session-event request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductEventBoundaryLimits {
    /// Maximum distinct session identities in one event filter.
    pub max_filter_sessions: usize,
    /// Maximum distinct registered domain event kinds in one filter.
    pub max_filter_kinds: usize,
    /// Maximum durable event records returned by one finite read.
    pub max_page_size: usize,
}

impl ProductEventBoundaryLimits {
    fn validate(self) -> Result<Self, ProductBoundaryError> {
        if self.max_filter_sessions == 0 || self.max_filter_kinds == 0 || self.max_page_size == 0 {
            return Err(ProductBoundaryError::InvalidLimits);
        }
        Ok(self)
    }
}

/// Hard bounds for standalone detection-profile requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductProfileBoundaryLimits {
    /// Maximum compact profiles returned by one page.
    pub max_page_size: usize,
    /// Maximum UTF-8 bytes in one profile match environment name.
    pub max_environment_name_bytes: usize,
    /// Maximum UTF-8 bytes in each standalone screen sample view.
    pub max_sample_bytes: usize,
}

impl ProductProfileBoundaryLimits {
    fn validate(self) -> Result<Self, ProductBoundaryError> {
        if self.max_page_size == 0
            || self.max_environment_name_bytes == 0
            || self.max_sample_bytes == 0
        {
            return Err(ProductBoundaryError::InvalidLimits);
        }
        Ok(self)
    }
}

/// Payload-safe failure produced while translating a product request.
#[derive(Debug)]
pub enum ProductBoundaryError {
    /// A domain constructor rejected request content.
    Domain(DomainError),
    /// A retained event filter or page violated application bounds.
    EventRequest(InvalidSessionEventRequest),
    /// Canonical protocol serialization failed before durable admission.
    CanonicalRequest(CanonicalPayloadError),
    /// Execute request omitted both supported stable retry identities.
    MissingAttemptIdentity,
    /// An externally dispatched start omitted its executable content pin.
    MissingExecutableDigest,
    /// An externally dispatched start omitted its complete compiled runtime policy.
    MissingRuntimePolicy,
    /// An externally dispatched start omitted its resolved environment binding.
    MissingEnvironmentBinding,
    /// The resolved environment binding does not match the dispatch pin.
    EnvironmentBindingMismatch,
    /// An attached agent binding is incomplete, stale, or conflicts with the runtime attempt.
    InvalidAgentBinding,
    /// Execute output bounds are zero or exceed the daemon-owned total.
    ExecutionOutputLimits(InvalidExecutionOutputLimits),
    /// Daemon boundary limits are internally invalid.
    InvalidLimits,
}

impl ProductBoundaryError {
    /// Returns the stable public error category.
    pub const fn category(&self) -> ProductErrorCategory {
        match self {
            Self::Domain(error) => match error.category() {
                ErrorCategory::InvalidArgument => ProductErrorCategory::InvalidRequest,
                ErrorCategory::NotFound => ProductErrorCategory::NotFound,
                ErrorCategory::Conflict => ProductErrorCategory::FailedPrecondition,
                ErrorCategory::FailedPrecondition => ProductErrorCategory::FailedPrecondition,
                ErrorCategory::CapacityExceeded => ProductErrorCategory::CapacityExceeded,
                ErrorCategory::InvariantViolation => ProductErrorCategory::Internal,
            },
            Self::EventRequest(_)
            | Self::CanonicalRequest(_)
            | Self::MissingAttemptIdentity
            | Self::MissingExecutableDigest
            | Self::MissingRuntimePolicy
            | Self::MissingEnvironmentBinding
            | Self::EnvironmentBindingMismatch
            | Self::InvalidAgentBinding
            | Self::ExecutionOutputLimits(_) => ProductErrorCategory::InvalidRequest,
            Self::InvalidLimits => ProductErrorCategory::InvalidConfiguration,
        }
    }

    /// Returns the applicable hard limit when a collection exceeded capacity.
    pub fn limit(&self) -> Option<u64> {
        match self {
            Self::Domain(DomainError::CapacityExceeded { limit, .. }) => {
                Some(u64::try_from(*limit).unwrap_or(u64::MAX))
            }
            Self::Domain(_)
            | Self::EventRequest(_)
            | Self::CanonicalRequest(_)
            | Self::MissingAttemptIdentity
            | Self::MissingExecutableDigest
            | Self::MissingRuntimePolicy
            | Self::MissingEnvironmentBinding
            | Self::EnvironmentBindingMismatch
            | Self::InvalidAgentBinding
            | Self::ExecutionOutputLimits(_)
            | Self::InvalidLimits => None,
        }
    }

    /// Converts the boundary failure to the public finite-response envelope.
    pub fn into_product_error(
        self,
        operation: ProductOperation,
        session_id: Option<String>,
    ) -> ProductError {
        ProductError {
            effect_certainty: None,
            operation: Some(operation),
            category: self.category(),
            disposition: Some(
                undercurrent_protocol::ProductErrorDisposition::default_for_category(
                    self.category(),
                ),
            ),
            message: self.to_string(),
            session_id,
            retryable: false,
            retry_after_millis: None,
            limit: self.limit(),
            resynchronization: None,
        }
    }
}

impl Display for ProductBoundaryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Domain(error) => Display::fmt(error, formatter),
            Self::EventRequest(error) => Display::fmt(error, formatter),
            Self::CanonicalRequest(error) => Display::fmt(error, formatter),
            Self::MissingAttemptIdentity => {
                formatter.write_str("execute request requires an attempt ID or idempotency key")
            }
            Self::MissingExecutableDigest => {
                formatter.write_str("dispatched start requires an executable content digest")
            }
            Self::MissingRuntimePolicy => {
                formatter.write_str("dispatched start requires a complete runtime policy")
            }
            Self::MissingEnvironmentBinding => {
                formatter.write_str("dispatched start requires a resolved environment binding")
            }
            Self::EnvironmentBindingMismatch => formatter.write_str(
                "resolved environment binding does not match the dispatch binding digest",
            ),
            Self::InvalidAgentBinding => formatter
                .write_str("agent runtime binding is invalid or conflicts with the attempt"),
            Self::ExecutionOutputLimits(error) => Display::fmt(error, formatter),
            Self::InvalidLimits => formatter.write_str("daemon request limits are invalid"),
        }
    }
}

impl Error for ProductBoundaryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Domain(error) => Some(error),
            Self::EventRequest(error) => Some(error),
            Self::CanonicalRequest(error) => Some(error),
            Self::ExecutionOutputLimits(error) => Some(error),
            Self::MissingAttemptIdentity
            | Self::MissingExecutableDigest
            | Self::MissingRuntimePolicy
            | Self::MissingEnvironmentBinding
            | Self::EnvironmentBindingMismatch
            | Self::InvalidAgentBinding
            | Self::InvalidLimits => None,
        }
    }
}

impl From<DomainError> for ProductBoundaryError {
    fn from(value: DomainError) -> Self {
        Self::Domain(value)
    }
}

impl From<InvalidSessionEventRequest> for ProductBoundaryError {
    fn from(value: InvalidSessionEventRequest) -> Self {
        Self::EventRequest(value)
    }
}

impl From<CanonicalPayloadError> for ProductBoundaryError {
    fn from(value: CanonicalPayloadError) -> Self {
        Self::CanonicalRequest(value)
    }
}

impl From<InvalidExecutionOutputLimits> for ProductBoundaryError {
    fn from(value: InvalidExecutionOutputLimits) -> Self {
        Self::ExecutionOutputLimits(value)
    }
}

/// Stateless translator configured with daemon-owned request bounds.
#[derive(Debug, Clone, Copy)]
pub struct ProductBoundary {
    limits: ProductBoundaryLimits,
}

impl ProductBoundary {
    /// Constructs a translator after validating all daemon-owned limits.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns [`ProductBoundaryError::InvalidLimits`] or a domain limit error
    /// when the supplied configuration is unusable.
    pub fn new(limits: ProductBoundaryLimits) -> Result<Self, ProductBoundaryError> {
        Ok(Self {
            limits: limits.validate()?,
        })
    }

    /// Returns the configured maximum summaries admitted in one query page.
    ///
    /// This function performs no IO and does not panic.
    pub const fn max_query_page_size(self) -> usize {
        self.limits.max_query_page_size
    }

    /// Returns the configured maximum terminal columns admitted from a client.
    pub const fn max_terminal_columns(self) -> u16 {
        self.limits.max_terminal_columns
    }

    /// Returns the configured maximum terminal rows admitted from a client.
    pub const fn max_terminal_rows(self) -> u16 {
        self.limits.max_terminal_rows
    }

    /// Returns the configured maximum typed items in one terminal input request.
    pub const fn max_input_items(self) -> usize {
        self.limits.max_input_items
    }

    /// Returns the configured maximum estimated bytes in one input request.
    pub const fn max_input_bytes(self) -> usize {
        self.limits.max_input_bytes
    }

    /// Translates a complete start request into a validated application command.
    ///
    /// Environment and metadata values are moved directly into redacting domain
    /// types and must not be logged by the caller. This function performs no IO
    /// and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed validation or capacity failures for any invalid field.
    pub fn start_session(
        &self,
        request: StartSessionRequest,
        accepted_at: TimestampMillis,
    ) -> Result<StartSessionCommand, ProductBoundaryError> {
        if let Some(dispatch) = request.dispatch.as_ref() {
            let environment_binding = request
                .environment
                .binding
                .as_ref()
                .ok_or(ProductBoundaryError::MissingEnvironmentBinding)?;
            if environment_binding.binding_digest_sha256
                != dispatch.environment_binding_digest_sha256
            {
                return Err(ProductBoundaryError::EnvironmentBindingMismatch);
            }
        }
        let expected_executable_digest = request
            .dispatch
            .as_ref()
            .map(|binding| {
                binding
                    .executable_digest_sha256
                    .as_deref()
                    .ok_or(ProductBoundaryError::MissingExecutableDigest)
                    .and_then(|digest| PayloadDigest::from_hex(digest).map_err(Into::into))
            })
            .transpose()?;
        let runtime_policy = request
            .dispatch
            .as_ref()
            .map(|binding| {
                let policy = binding
                    .runtime_policy
                    .as_deref()
                    .ok_or(ProductBoundaryError::MissingRuntimePolicy)?;
                let limits = RuntimeDispatchLimits::new(
                    policy.limits.wall_time_millis,
                    policy.limits.stdout_bytes,
                    policy.limits.stderr_bytes,
                    policy.limits.memory_bytes,
                    policy.limits.cpu_millis,
                )?;
                let output_contract = RuntimeOutputContract::new(
                    policy.output_contract.schema.clone(),
                    PayloadDigest::from_hex(&policy.output_contract.schema_digest_sha256)?,
                    policy.output_contract.classification,
                )?;
                let telemetry_policy_ref =
                    RuntimeTelemetryPolicyRef::new(policy.telemetry_policy_ref.clone())?;
                let retention_policy_ref =
                    RuntimeRetentionPolicyRef::new(policy.retention_policy_ref.clone())?;
                let callback_refs = policy
                    .callback_refs
                    .iter()
                    .cloned()
                    .map(RuntimeCallbackRef::new)
                    .collect::<Result<Vec<_>, _>>()?;
                RuntimeDispatchPolicy::new(
                    limits,
                    output_contract,
                    telemetry_policy_ref,
                    retention_policy_ref,
                    callback_refs,
                    TimestampMillis::new(policy.launch_profile_expires_at_millis),
                    TimestampMillis::new(policy.environment_binding_expires_at_millis),
                )
                .map_err(ProductBoundaryError::from)
            })
            .transpose()?;
        let dispatch = match (
            request.dispatch.as_ref(),
            request.mutation.attempt.as_ref(),
            request.mutation.idempotency_key.as_ref(),
        ) {
            (None, None, _) => None,
            (Some(binding), Some(attempt), Some(idempotency_key)) => {
                OperationAttemptId::new(idempotency_key.clone())?;
                let canonical = canonical_product_request_digest(
                    &undercurrent_protocol::ProductRequest::StartSession(request.clone()),
                )?
                .to_string();
                let canonical = canonical
                    .strip_prefix("sha256:")
                    .ok_or(ProductBoundaryError::InvalidLimits)?;
                if attempt.payload_digest_sha256 != canonical {
                    return Err(ProductBoundaryError::InvalidLimits);
                }
                let payload_digest = PayloadDigest::from_hex(canonical)?;
                let mut dispatch_binding = RuntimeDispatchBinding::new(
                    DispatchCellIdentity::new(binding.caller_cell_identity.clone())?,
                    PayloadDigest::from_hex(&binding.request_digest_sha256)?,
                    PayloadDigest::from_hex(&binding.launch_profile_digest_sha256)?,
                    PayloadDigest::from_hex(&binding.environment_binding_digest_sha256)?,
                    PayloadDigest::from_hex(&binding.input_digest_sha256)?,
                    runtime_policy.ok_or(ProductBoundaryError::MissingRuntimePolicy)?,
                );
                if let Some(agent) = binding.agent.as_deref() {
                    let schema_digest =
                        undercurrent_protocol::canonical_agent_result_schema_digest(agent)?
                            .to_string();
                    let schema_digest = schema_digest
                        .strip_prefix("sha256:")
                        .ok_or(ProductBoundaryError::InvalidAgentBinding)?;
                    let runtime_output = dispatch_binding
                        .runtime_policy()
                        .ok_or(ProductBoundaryError::MissingRuntimePolicy)?
                        .output_contract();
                    let expected_classification = match runtime_output.classification() {
                        undercurrent_domain::Classification::Public => "public",
                        undercurrent_domain::Classification::Internal => "internal",
                        undercurrent_domain::Classification::Confidential => "confidential",
                        undercurrent_domain::Classification::Restricted => "restricted",
                        undercurrent_domain::Classification::Secret => "secret",
                    };
                    if agent.attempt_id != attempt.attempt_id
                        || agent.fence != attempt.fencing_token
                        || agent.request_digest_sha256 != binding.request_digest_sha256
                        || agent.input_manifest_digest_sha256 != binding.input_digest_sha256
                        || agent.expected_result_schema != runtime_output.schema()
                        || agent.expected_result_schema_digest_sha256 != schema_digest
                        || agent.expected_result_schema_digest_sha256
                            != runtime_output.schema_digest().to_hex()
                        || agent.output_classification != expected_classification
                        || agent.undercurrent_provider_generation == 0
                        || agent.expires_at_millis <= accepted_at.value()
                    {
                        return Err(ProductBoundaryError::InvalidAgentBinding);
                    }
                    let canonical_json =
                        undercurrent_protocol::canonical_agent_runtime_binding(agent)?;
                    let expected_result_schema_canonical_json =
                        undercurrent_protocol::canonical_agent_result_schema(agent)?;
                    dispatch_binding = dispatch_binding.with_agent(AgentRuntimeBinding::new(
                        agent.tenant_id.clone(),
                        agent.source_product.clone(),
                        agent.actor_context_ref.clone(),
                        agent.authority_ref.clone(),
                        agent.undercurrent_provider_id.clone(),
                        agent.undercurrent_provider_generation,
                        OperationAttemptId::new(agent.attempt_id.clone())?,
                        agent.attempt_revision,
                        agent.recovery_epoch,
                        undercurrent_domain::FencingToken::new(agent.fence)?,
                        PayloadDigest::from_hex(&agent.request_digest_sha256)?,
                        expected_result_schema_canonical_json,
                        canonical_json,
                        TimestampMillis::new(agent.expires_at_millis),
                    )?);
                }
                Some(OperationAttemptAdmission::dispatched(
                    OperationAttemptId::new(attempt.attempt_id.clone())?,
                    None,
                    ProcessMutationKind::Spawn,
                    undercurrent_domain::FencingToken::new(attempt.fencing_token)?,
                    payload_digest,
                    dispatch_binding,
                    accepted_at,
                ))
            }
            _ => return Err(ProductBoundaryError::MissingAttemptIdentity),
        };
        let launch_mode = match request.launch_mode {
            undercurrent_protocol::LaunchModeDto::Native => undercurrent_domain::LaunchMode::Native,
            undercurrent_protocol::LaunchModeDto::Sealed => undercurrent_domain::LaunchMode::Sealed,
        };
        if expected_executable_digest.is_some()
            && launch_mode == undercurrent_domain::LaunchMode::Native
        {
            return Err(ProductBoundaryError::Domain(DomainError::InvalidValue {
                field: "launch_mode",
                reason: undercurrent_domain::InvalidValueReason::InvalidCombination {
                    rule: "pinned dispatches require sealed execution",
                },
            }));
        }
        let command = self
            .command(request.command)?
            .with_launch_mode(launch_mode)
            .with_expected_executable_digest(expected_executable_digest);
        let environment_binding = request
            .environment
            .binding
            .map(|binding| {
                Ok::<_, ProductBoundaryError>(ExecutionEnvironmentBinding::new(
                    PayloadDigest::from_hex(&binding.binding_digest_sha256)?,
                    PathReference::new(
                        binding.working_directory_root,
                        self.limits.execution.max_path_bytes,
                    )?,
                ))
            })
            .transpose()?;
        let environment =
            EnvironmentVariables::new(request.environment.variables, self.limits.execution)?;
        let secret_environment = SecretEnvironmentVariables::new(
            request
                .environment
                .secret_variables
                .into_iter()
                .map(|(name, reference)| {
                    Ok::<_, ProductBoundaryError>((
                        name,
                        SecretReference::new(String::from(reference))?,
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?,
            &environment,
            self.limits.execution,
        )?;
        let environment_file = request
            .environment
            .file
            .map(|path| PathReference::new(path, self.limits.execution.max_path_bytes))
            .transpose()?;
        let working_directory = request
            .working_directory
            .map(|path| PathReference::new(path, self.limits.execution.max_path_bytes))
            .transpose()?;
        let terminal = self.terminal_size(request.terminal)?;
        let restart = restart_config(request.execution.restart)?;
        let stop = StopPolicy::new(
            stop_signal(request.execution.stop.signal),
            Duration::from_millis(request.execution.stop.timeout_millis),
        )?;
        let execution = ExecutionPolicy::new(
            restart,
            stop,
            request.execution.detection_enabled,
            request.execution.auto_remove,
        );
        let health_rules = request
            .health
            .rules
            .into_iter()
            .map(|rule| self.health_rule(rule))
            .collect::<Result<Vec<_>, _>>()?;
        let mut configuration = SessionConfiguration::new(
            LaunchSpec::new(
                command,
                environment,
                environment_file,
                working_directory,
                terminal,
            )
            .with_secret_environment(secret_environment, self.limits.execution)?
            .with_environment_binding(environment_binding)
            .with_output_capture_mode(match request.output_capture_mode {
                OutputCaptureModeDto::Terminal => OutputCaptureMode::Terminal,
                OutputCaptureModeDto::Opaque => OutputCaptureMode::Opaque,
            }),
            execution,
            HealthRules::new(health_rules, self.limits.execution)?,
            request.profile.map(ProfileName::new).transpose()?,
            Labels::new(request.labels)?,
            Metadata::new(request.metadata)?,
        );
        configuration.set_profile_binding(match request.profile_binding {
            ProfileBindingModeDto::Fixed => ProfileBindingMode::Fixed,
            ProfileBindingModeDto::ForegroundAdaptive => ProfileBindingMode::ForegroundAdaptive,
        });
        Ok(StartSessionCommand {
            name: SessionName::new(request.name)?,
            configuration,
            max_work_items: self.limits.max_work_items,
            dispatch,
        })
    }

    /// Maps one command execution without trusting client fence or digest metadata.
    pub fn execute_in_session(
        &self,
        request: ExecuteInSessionRequest,
        accepted_at: TimestampMillis,
        total_output_bytes: u64,
    ) -> Result<ExecuteInSessionCommand, ProductBoundaryError> {
        let digest = canonical_product_request_digest(
            &undercurrent_protocol::ProductRequest::ExecuteInSession(request.clone()),
        )?
        .to_string();
        let payload_digest = PayloadDigest::from_hex(
            digest
                .strip_prefix("sha256:")
                .ok_or(ProductBoundaryError::InvalidLimits)?,
        )?;
        let attempt_id = request
            .mutation
            .attempt
            .as_ref()
            .map(|attempt| attempt.attempt_id.clone())
            .or_else(|| request.mutation.idempotency_key.clone())
            .ok_or(ProductBoundaryError::MissingAttemptIdentity)
            .and_then(|value| OperationAttemptId::new(value).map_err(Into::into))?;
        let session_id = SessionId::new(request.session_id)?;
        let command = self.command(request.command)?;
        let inline_bytes = usize::try_from(request.output_inline_limit_bytes).map_err(|_| {
            ProductBoundaryError::ExecutionOutputLimits(InvalidExecutionOutputLimits)
        })?;
        let output_limits = ExecutionOutputLimits::new(inline_bytes, total_output_bytes)?;
        Ok(ExecuteInSessionCommand {
            dir_launch: request.dir_launch,
            env_launch: request.env_launch,
            mutation: OperationAttemptAdmission::new(
                attempt_id,
                Some(session_id.clone()),
                ProcessMutationKind::ExecuteInSession,
                payload_digest,
                accepted_at,
            ),
            session_id,
            command,
            output_limits,
            deadline: request.deadline_millis.map(TimestampMillis::new),
        })
    }

    /// Derives bounded command facts when session start needs profile selection.
    ///
    /// Explicit profiles and disabled detection return `None` without further
    /// work. Environment values are never copied into the result; only names
    /// already validated by the start boundary participate. This function
    /// performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed validation or capacity failure when automatic matching
    /// facts violate the same execution bounds as session creation.
    pub fn automatic_profile_match_target(
        &self,
        request: &StartSessionRequest,
    ) -> Result<Option<ProfileMatchTarget>, ProductBoundaryError> {
        if request.profile.is_some() || !request.execution.detection_enabled {
            return Ok(None);
        }
        let environment_names = request
            .environment
            .variables
            .keys()
            .chain(request.environment.secret_variables.keys())
            .cloned()
            .map(|name| EnvironmentName::new(name, self.limits.execution.max_environment_bytes))
            .collect::<Result<BTreeSet<_>, _>>()?;
        let target = ProfileMatchTarget::new(
            request.command.program.clone(),
            request.command.arguments.clone(),
            environment_names,
            self.limits.execution.max_command_bytes,
            self.limits.execution.max_arguments,
            self.limits.execution.max_environment_entries,
        )?;
        #[cfg(windows)]
        let target =
            target.with_match_convention(undercurrent_domain::ProfileMatchConvention::Windows);
        Ok(Some(target))
    }

    /// Translates one stop request.
    pub fn stop_session(
        &self,
        request: StopSessionRequest,
    ) -> Result<StopSessionCommand, ProductBoundaryError> {
        Ok(StopSessionCommand {
            expected_revision: request
                .expected_revision
                .map(undercurrent_domain::Revision::new),
            session_id: SessionId::new(request.session_id)?,
            mode: if request.force {
                StopMode::Force
            } else if request.graceful_only {
                StopMode::GracefulOnly
            } else {
                StopMode::Graceful
            },
        })
    }

    /// Translates one explicit restart request.
    pub fn restart_session(
        &self,
        request: RestartSessionRequest,
    ) -> Result<RestartSessionCommand, ProductBoundaryError> {
        Ok(RestartSessionCommand {
            stop_mode: if request.force {
                StopMode::Force
            } else {
                StopMode::Graceful
            },
            session_id: SessionId::new(request.session_id)?,
        })
    }

    /// Translates one rename request.
    pub fn rename_session(
        &self,
        request: RenameSessionRequest,
    ) -> Result<RenameSessionCommand, ProductBoundaryError> {
        Ok(RenameSessionCommand {
            session_id: SessionId::new(request.session_id)?,
            name: SessionName::new(request.name)?,
        })
    }

    /// Translates a bounded atomic label patch.
    pub fn update_session_labels(
        &self,
        request: UpdateSessionLabelsRequest,
    ) -> Result<UpdateSessionLabelsCommand, ProductBoundaryError> {
        if request.remove.len() > undercurrent_domain::MAX_LABELS {
            return Err(DomainError::CapacityExceeded {
                resource: "label_removals",
                limit: undercurrent_domain::MAX_LABELS,
            }
            .into());
        }
        Ok(UpdateSessionLabelsCommand {
            session_id: SessionId::new(request.session_id)?,
            patch: LabelPatch::new(Labels::new(request.set)?, request.remove)?,
        })
    }

    /// Translates one presentation-confirmed removal request.
    pub fn remove_session(
        &self,
        request: RemoveSessionRequest,
    ) -> Result<RemoveSessionCommand, ProductBoundaryError> {
        Ok(RemoveSessionCommand {
            session_id: SessionId::new(request.session_id)?,
            confirmation: match request.confirmation {
                RemovalConfirmationDto::Confirmed => RemovalConfirmation::Confirmed,
                RemovalConfirmationDto::Forced => RemovalConfirmation::Forced,
            },
        })
    }

    /// Translates one bounded terminal resize request.
    pub fn resize_terminal(
        &self,
        request: ResizeTerminalRequest,
    ) -> Result<ResizeTerminalCommand, ProductBoundaryError> {
        Ok(ResizeTerminalCommand {
            session_id: SessionId::new(request.session_id)?,
            size: self.terminal_size(request.size)?,
        })
    }

    /// Translates one atomic terminal input request using configured maxima for
    /// admission. The application service revalidates terminal-relative items
    /// against current dimensions under the session mutation lease.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed validation or capacity failures for identifiers, keys,
    /// mouse coordinates, or total input size.
    pub fn send_terminal_input(
        &self,
        request: SendTerminalInputRequest,
    ) -> Result<SendTerminalInputCommand, ProductBoundaryError> {
        let admitted_terminal = TerminalSize::new(
            self.limits.max_terminal_columns,
            self.limits.max_terminal_rows,
            self.limits.max_terminal_columns,
            self.limits.max_terminal_rows,
        )?;
        let items = request
            .items
            .into_iter()
            .map(|item| input_item(item, admitted_terminal))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SendTerminalInputCommand {
            expected_revision: request
                .expected_revision
                .map(undercurrent_domain::Revision::new),
            session_id: SessionId::new(request.session_id)?,
            input: InputSequence::new(
                items,
                self.limits.max_input_items,
                self.limits.max_input_bytes,
            )?,
            delivery_id: request.delivery_id.map(DeliveryId::new).transpose()?,
            observation_policy: match request.observation_policy {
                InputObservationPolicyDto::ForceActive => InputObservationPolicy::ForceActive,
                InputObservationPolicyDto::Preserve => InputObservationPolicy::Preserve,
            },
        })
    }

    /// Translates typed standard-input closure without treating EOF as terminal bytes.
    pub fn close_standard_input(
        &self,
        request: CloseStandardInputRequest,
    ) -> Result<CloseStandardInputCommand, ProductBoundaryError> {
        Ok(CloseStandardInputCommand {
            session_id: SessionId::new(request.session_id)?,
        })
    }

    /// Translates one first-page prepared session query.
    ///
    /// Cursor validation remains the responsibility of the cursor codec because
    /// application cursors are opaque to clients. This function performs no IO
    /// and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed validation or capacity failures for filter values or page
    /// size.
    pub fn session_query(
        &self,
        request: &ListSessionsRequest,
    ) -> Result<SessionQuery, ProductBoundaryError> {
        let ids = request
            .filter
            .ids
            .iter()
            .cloned()
            .map(SessionId::new)
            .collect::<Result<BTreeSet<_>, _>>()?;
        let lifecycles = request
            .filter
            .lifecycles
            .iter()
            .copied()
            .map(lifecycle_kind)
            .collect();
        let executions = request
            .filter
            .executions
            .iter()
            .copied()
            .map(execution_state)
            .collect();
        let interactions = request
            .filter
            .interactions
            .iter()
            .copied()
            .map(interaction_state)
            .collect();
        let health = request
            .filter
            .health
            .iter()
            .copied()
            .map(health_status)
            .collect();
        Ok(SessionQuery {
            filter: SessionFilter {
                ids,
                name: request
                    .filter
                    .name
                    .as_ref()
                    .cloned()
                    .map(SessionName::new)
                    .transpose()?,
                name_contains: request
                    .filter
                    .name_contains
                    .as_ref()
                    .cloned()
                    .map(NameContains::new)
                    .transpose()?,
                lifecycles,
                executions,
                interactions,
                health,
                profile: request
                    .filter
                    .profile
                    .as_ref()
                    .cloned()
                    .map(ProfileName::new)
                    .transpose()?,
                labels: Labels::new(request.filter.labels.clone())?,
                has_active_work: request.filter.has_active_work,
            },
            sort: SessionSort {
                field: session_sort_field(request.sort.field),
                direction: sort_direction(request.sort.direction),
            },
            projection: session_projection(request.projection),
            page_size: PageSize::new(request.page_size as usize, self.limits.max_query_page_size)?,
            include_total: request.include_total,
        })
    }

    /// Translates one first-page watched query and rejects finite cursor state.
    ///
    /// A watch owns its current bounded page and therefore cannot begin from an
    /// opaque finite pagination cursor. This function performs no IO and does
    /// not panic.
    ///
    /// # Errors
    ///
    /// Returns typed cursor, filter, projection, sort, or page validation
    /// failure.
    pub fn session_watch(
        &self,
        specification: &SessionWatchSpec,
    ) -> Result<SessionQuery, ProductBoundaryError> {
        if specification.query.cursor.is_some() {
            return Err(DomainError::InvalidValue {
                field: "cursor",
                reason: InvalidValueReason::InvalidCombination {
                    rule: "a session watch must begin from its first bounded page",
                },
            }
            .into());
        }
        self.session_query(&specification.query)
    }

    /// Translates the target and projection for one session detail request.
    pub fn session_detail(
        &self,
        request: &GetSessionDetailRequest,
    ) -> Result<(SessionId, SessionDetailProjection), ProductBoundaryError> {
        Ok((
            SessionId::new(request.session_id.clone())?,
            SessionDetailProjection::new(
                request.projection.configuration,
                request.projection.work_context,
                request.projection.process,
                request.projection.foreground_evidence,
                request.projection.health_results,
            ),
        ))
    }

    /// Translates one private execution-context target.
    pub fn execution_context(
        &self,
        request: &GetExecutionContextRequest,
    ) -> Result<SessionId, ProductBoundaryError> {
        SessionId::new(request.session_id.clone()).map_err(Into::into)
    }

    /// Translates one retained terminal-output page target and bounds.
    pub fn terminal_output_read(
        &self,
        request: &ReadTerminalOutputRequest,
    ) -> Result<(SessionId, OutputOffset, usize), ProductBoundaryError> {
        Ok((
            SessionId::new(request.session_id.clone())?,
            OutputOffset::new(request.from_offset),
            request.max_bytes as usize,
        ))
    }

    /// Translates one current terminal-screen target.
    pub fn terminal_screen_read(
        &self,
        request: &ReadTerminalScreenRequest,
    ) -> Result<SessionId, ProductBoundaryError> {
        SessionId::new(request.session_id.clone()).map_err(Into::into)
    }

    /// Translates one terminal-screen diff target.
    pub fn terminal_screen_diff(
        &self,
        request: &ReadTerminalScreenDiffRequest,
    ) -> Result<SessionId, ProductBoundaryError> {
        SessionId::new(request.session_id.clone()).map_err(Into::into)
    }

    /// Translates one selected terminal-output stream target.
    pub fn terminal_output_stream(
        &self,
        specification: TerminalOutputSpec,
    ) -> Result<(SessionId, TerminalInitialModeDto), ProductBoundaryError> {
        Ok((
            SessionId::new(specification.session_id)?,
            specification.initial,
        ))
    }

    /// Translates one bounded durable session-event read.
    pub fn session_event_read(
        &self,
        request: &ReadSessionEventsRequest,
    ) -> Result<SessionEventReadRequest, ProductBoundaryError> {
        let sessions = request
            .session_ids
            .iter()
            .cloned()
            .map(SessionId::new)
            .collect::<Result<BTreeSet<_>, _>>()?;
        let kinds = request
            .kinds
            .iter()
            .copied()
            .map(to_domain)
            .collect::<BTreeSet<_>>();
        let filter = SessionEventFilter::new(
            sessions,
            kinds,
            self.limits.events.max_filter_sessions,
            self.limits.events.max_filter_kinds,
        )?;
        let start = match request.after_sequence {
            Some(after) => SessionEventStart::Resume {
                after: OutboxSequence::new(after).ok_or(DomainError::InvalidValue {
                    field: "after_sequence",
                    reason: InvalidValueReason::OutOfRange {
                        min: 1,
                        max: u64::MAX,
                    },
                })?,
            },
            None if request.tail => SessionEventStart::Tail,
            None => SessionEventStart::Earliest,
        };
        Ok(SessionEventReadRequest::new(
            filter,
            start,
            request.limit as usize,
            self.limits.events.max_page_size,
        )?)
    }

    /// Translates a durable event stream into an atomic live-or-resume read.
    ///
    /// New subscriptions start at the current journal head. Explicit durable
    /// checkpoints replay strictly after the supplied sequence. Each source
    /// extraction is bounded by the configured event page maximum.
    pub fn session_event_stream(
        &self,
        specification: &SessionEventsSpec,
    ) -> Result<SessionEventReadRequest, ProductBoundaryError> {
        let request = ReadSessionEventsRequest {
            tail: false,
            include_observation: false,
            session_ids: specification.session_ids.clone(),
            kinds: specification.kinds.clone(),
            after_sequence: specification.after_sequence,
            limit: u32::try_from(self.limits.events.max_page_size)
                .map_err(|_| ProductBoundaryError::InvalidLimits)?,
        };
        let mut translated = self.session_event_read(&request)?;
        if specification.after_sequence.is_none() {
            translated = SessionEventReadRequest::new(
                translated.filter().clone(),
                SessionEventStart::Live,
                translated.limit(),
                self.limits.events.max_page_size,
            )?;
        }
        Ok(translated)
    }

    /// Validates one bounded profile-list page size.
    pub fn profile_page_size(
        &self,
        request: &ListProfilesRequest,
    ) -> Result<PageSize, ProductBoundaryError> {
        Ok(PageSize::new(
            request.page_size as usize,
            self.limits.profiles.max_page_size,
        )?)
    }

    /// Validates one requested detection-profile name.
    pub fn profile_name(
        &self,
        request: &GetProfileRequest,
    ) -> Result<ProfileName, ProductBoundaryError> {
        Ok(ProfileName::new(request.name.clone())?)
    }

    /// Translates explicit bounded command facts for profile matching.
    pub fn profile_match_target(
        &self,
        request: MatchProfileRequest,
    ) -> Result<ProfileMatchTarget, ProductBoundaryError> {
        let environment_names = request
            .environment_names
            .into_iter()
            .map(|name| EnvironmentName::new(name, self.limits.profiles.max_environment_name_bytes))
            .collect::<Result<BTreeSet<_>, _>>()?;
        let target = ProfileMatchTarget::new(
            request.executable,
            request.arguments,
            environment_names,
            self.limits.execution.max_command_bytes,
            self.limits.execution.max_arguments,
            self.limits.execution.max_environment_entries,
        )?;
        #[cfg(windows)]
        let target =
            target.with_match_convention(undercurrent_domain::ProfileMatchConvention::Windows);
        Ok(target)
    }

    /// Validates an optional profile lint selection.
    pub fn profile_lint_name(
        &self,
        request: &LintProfilesRequest,
    ) -> Result<Option<ProfileName>, ProductBoundaryError> {
        Ok(request
            .name
            .as_ref()
            .cloned()
            .map(ProfileName::new)
            .transpose()?)
    }

    /// Translates one bounded standalone profile sample.
    pub fn profile_test(
        &self,
        request: TestProfileRequest,
    ) -> Result<(ProfileName, ProfileSampleView), ProductBoundaryError> {
        Ok((
            ProfileName::new(request.name)?,
            ProfileSampleView::new(
                None,
                request.screen_text,
                request.last_line,
                request.cursor_at_end,
                self.limits.profiles.max_sample_bytes,
            )?,
        ))
    }

    /// Translates one complete WorkContext upsert request.
    pub fn upsert_work_item(
        &self,
        request: UpsertWorkItemRequest,
    ) -> Result<UpsertWorkItemCommand, ProductBoundaryError> {
        let item = request.item;
        let targets = item
            .targets
            .into_iter()
            .map(work_target)
            .collect::<Result<Vec<_>, _>>()?;
        let work_item = WorkItem::new_with_provenance(
            WorkItemKey::new(WorkOwner::new(item.owner)?, WorkItemId::new(item.item_id)?),
            work_relationship(item.relationship),
            item.title,
            item.description,
            work_status(item.status),
            targets,
            WorkProvenance::new(item.provenance)?,
            Metadata::new(item.metadata)?,
            TimestampMillis::new(item.created_at_millis),
            TimestampMillis::new(item.updated_at_millis),
            item.expires_at_millis.map(TimestampMillis::new),
        )?;
        Ok(UpsertWorkItemCommand {
            session_id: SessionId::new(request.session_id)?,
            item: work_item,
        })
    }

    /// Translates one owner-scoped WorkContext removal request.
    pub fn remove_work_item(
        &self,
        request: RemoveWorkItemRequest,
    ) -> Result<RemoveWorkItemCommand, ProductBoundaryError> {
        Ok(RemoveWorkItemCommand {
            session_id: SessionId::new(request.session_id)?,
            item_key: WorkItemKey::new(
                WorkOwner::new(request.owner)?,
                WorkItemId::new(request.item_id)?,
            ),
        })
    }

    fn terminal_size(
        &self,
        size: undercurrent_protocol::TerminalSizeDto,
    ) -> Result<TerminalSize, ProductBoundaryError> {
        Ok(TerminalSize::new(
            size.columns,
            size.rows,
            self.limits.max_terminal_columns,
            self.limits.max_terminal_rows,
        )?)
    }

    fn command(
        &self,
        command: undercurrent_protocol::CommandSpecDto,
    ) -> Result<CommandSpec, ProductBoundaryError> {
        Ok(CommandSpec::new(
            command.program,
            command.arguments,
            self.limits.execution,
        )?)
    }

    fn health_rule(&self, rule: HealthRuleDto) -> Result<HealthRule, ProductBoundaryError> {
        let max_bytes = self.limits.execution.max_health_rule_bytes;
        Ok(match rule {
            HealthRuleDto::TcpPort { port } => HealthRule::tcp_port(port)?,
            HealthRuleDto::HttpEndpoint { endpoint } => {
                HealthRule::http_endpoint(endpoint, max_bytes)?
            }
            HealthRuleDto::OutputContains { pattern } => {
                HealthRule::output_contains(pattern, max_bytes)?
            }
            HealthRuleDto::OutputRejects { pattern } => {
                HealthRule::output_rejects(pattern, max_bytes)?
            }
            HealthRuleDto::RedCellThreshold { minimum_cells } => {
                HealthRule::red_cell_threshold(minimum_cells)?
            }
            HealthRuleDto::Command { command } => HealthRule::command(self.command(command)?),
        })
    }
}

fn restart_config(policy: RestartPolicyDto) -> Result<RestartConfig, DomainError> {
    let (policy, max_attempts, initial_delay, backoff, max_delay, min_uptime) = match policy {
        RestartPolicyDto::Never => return Ok(RestartConfig::never()),
        RestartPolicyDto::OnFailure {
            max_attempts,
            initial_delay_millis,
            max_delay_millis,
            backoff_basis_points,
            min_uptime_millis,
        } => (
            RestartPolicy::OnFailure,
            max_attempts,
            initial_delay_millis,
            backoff_basis_points,
            max_delay_millis,
            min_uptime_millis,
        ),
        RestartPolicyDto::Always {
            max_attempts,
            initial_delay_millis,
            max_delay_millis,
            backoff_basis_points,
            min_uptime_millis,
        } => (
            RestartPolicy::Always,
            max_attempts,
            initial_delay_millis,
            backoff_basis_points,
            max_delay_millis,
            min_uptime_millis,
        ),
    };
    RestartConfig::new(
        policy,
        max_attempts,
        Duration::from_millis(initial_delay),
        backoff,
        Duration::from_millis(max_delay),
        Duration::from_millis(min_uptime),
    )
}

const fn stop_signal(signal: StopSignalDto) -> StopSignal {
    match signal {
        StopSignalDto::Terminate => StopSignal::Terminate,
        StopSignalDto::Interrupt => StopSignal::Interrupt,
        StopSignalDto::Hangup => StopSignal::Hangup,
        StopSignalDto::Quit => StopSignal::Quit,
        StopSignalDto::User1 => StopSignal::User1,
        StopSignalDto::User2 => StopSignal::User2,
    }
}

fn input_item(item: TerminalInputDto, terminal: TerminalSize) -> Result<InputItem, DomainError> {
    Ok(match item {
        TerminalInputDto::AutoText { text } => InputItem::AutoText(text),
        TerminalInputDto::Text { text } => InputItem::Text(text),
        TerminalInputDto::Paste { text } => InputItem::Paste(text),
        TerminalInputDto::Key { key } => InputItem::Key(key_value(key)?),
        TerminalInputDto::Raw { bytes } => InputItem::Bytes(bytes),
        TerminalInputDto::Mouse { event } => InputItem::Mouse(mouse_event(event, terminal)?),
    })
}

fn key_value(key: KeyDto) -> Result<Key, DomainError> {
    Ok(match key {
        KeyDto::Enter => Key::Enter,
        KeyDto::Tab => Key::Tab,
        KeyDto::BackTab => Key::BackTab,
        KeyDto::Escape => Key::Escape,
        KeyDto::Backspace => Key::Backspace,
        KeyDto::Delete => Key::Delete,
        KeyDto::Space => Key::Space,
        KeyDto::Up => Key::Up,
        KeyDto::Down => Key::Down,
        KeyDto::Left => Key::Left,
        KeyDto::Right => Key::Right,
        KeyDto::Home => Key::Home,
        KeyDto::End => Key::End,
        KeyDto::PageUp => Key::PageUp,
        KeyDto::PageDown => Key::PageDown,
        KeyDto::Function { number } => Key::Function(FunctionKey::new(number)?),
        KeyDto::Control { character } => Key::Control(ControlKey::new(character)?),
        KeyDto::Modified { base, modifiers } => Key::Modified {
            base: modifiable_key(base)?,
            modifiers: KeyModifiers {
                shift: modifiers.shift,
                control: modifiers.control,
                alt: modifiers.alt,
            },
        },
    })
}

fn modifiable_key(key: ModifiableKeyDto) -> Result<ModifiableKey, DomainError> {
    Ok(match key {
        ModifiableKeyDto::Up => ModifiableKey::Up,
        ModifiableKeyDto::Down => ModifiableKey::Down,
        ModifiableKeyDto::Left => ModifiableKey::Left,
        ModifiableKeyDto::Right => ModifiableKey::Right,
        ModifiableKeyDto::Home => ModifiableKey::Home,
        ModifiableKeyDto::End => ModifiableKey::End,
        ModifiableKeyDto::PageUp => ModifiableKey::PageUp,
        ModifiableKeyDto::PageDown => ModifiableKey::PageDown,
        ModifiableKeyDto::Function { number } => ModifiableKey::Function(FunctionKey::new(number)?),
    })
}

fn mouse_event(event: MouseEventDto, terminal: TerminalSize) -> Result<MouseEvent, DomainError> {
    MouseEvent::new(
        event.column,
        event.row,
        match event.button {
            MouseButtonDto::Left => MouseButton::Left,
            MouseButtonDto::Middle => MouseButton::Middle,
            MouseButtonDto::Right => MouseButton::Right,
            MouseButtonDto::ScrollUp => MouseButton::ScrollUp,
            MouseButtonDto::ScrollDown => MouseButton::ScrollDown,
        },
        match event.event {
            MouseEventKindDto::Press => MouseEventKind::Press,
            MouseEventKindDto::Release => MouseEventKind::Release,
            MouseEventKindDto::Move => MouseEventKind::Move,
        },
        terminal,
    )
}

fn work_target(target: WorkTargetDto) -> Result<WorkTarget, DomainError> {
    Ok(match target {
        WorkTargetDto::SessionId { session_id } => WorkTarget::Session(SessionId::new(session_id)?),
        WorkTargetDto::Labels { labels } => WorkTarget::Labels(Labels::new(labels)?),
        WorkTargetDto::External {
            namespace,
            selector,
        } => WorkTarget::External {
            system: WorkOwner::new(namespace)?,
            id: WorkItemId::new(selector)?,
        },
    })
}

const fn work_relationship(relationship: WorkRelationshipDto) -> WorkRelationKind {
    match relationship {
        WorkRelationshipDto::DependsOn => WorkRelationKind::DependsOn,
        WorkRelationshipDto::Delegated => WorkRelationKind::Delegated,
        WorkRelationshipDto::Spawned => WorkRelationKind::Spawned,
        WorkRelationshipDto::Related => WorkRelationKind::Related,
    }
}

const fn work_status(status: WorkStatusDto) -> WorkStatus {
    match status {
        WorkStatusDto::Running => WorkStatus::Running,
        WorkStatusDto::Waiting => WorkStatus::Waiting,
        WorkStatusDto::Blocked => WorkStatus::Blocked,
        WorkStatusDto::Completed => WorkStatus::Completed,
        WorkStatusDto::Failed => WorkStatus::Failed,
        WorkStatusDto::Cancelled => WorkStatus::Cancelled,
        WorkStatusDto::DeliveryFailed => WorkStatus::DeliveryFailed,
        WorkStatusDto::Expired => WorkStatus::Expired,
    }
}

const fn lifecycle_kind(value: LifecycleKindDto) -> LifecycleKind {
    match value {
        LifecycleKindDto::Starting => LifecycleKind::Starting,
        LifecycleKindDto::Running => LifecycleKind::Running,
        LifecycleKindDto::Stopping => LifecycleKind::Stopping,
        LifecycleKindDto::Stopped => LifecycleKind::Stopped,
        LifecycleKindDto::Exited => LifecycleKind::Exited,
        LifecycleKindDto::Failed => LifecycleKind::Failed,
    }
}

const fn execution_state(value: ExecutionStateDto) -> ExecutionState {
    match value {
        ExecutionStateDto::Active => ExecutionState::Active,
        ExecutionStateDto::Quiescent => ExecutionState::Quiescent,
        ExecutionStateDto::Unknown => ExecutionState::Unknown,
    }
}

const fn interaction_state(value: InteractionStateDto) -> InteractionState {
    match value {
        InteractionStateDto::None => InteractionState::None,
        InteractionStateDto::Available => InteractionState::Available,
        InteractionStateDto::Requested => InteractionState::Requested,
    }
}

const fn health_status(value: HealthStatusDto) -> HealthStatus {
    match value {
        HealthStatusDto::Healthy => HealthStatus::Healthy,
        HealthStatusDto::Unhealthy => HealthStatus::Unhealthy,
        HealthStatusDto::Unknown => HealthStatus::Unknown,
        HealthStatusDto::NoCheck => HealthStatus::NoCheck,
    }
}

const fn session_projection(value: SessionProjectionDto) -> SessionProjection {
    match value {
        SessionProjectionDto::Compact => SessionProjection::Compact,
        SessionProjectionDto::WithLabels => SessionProjection::WithLabels,
        SessionProjectionDto::Operational => SessionProjection::Operational,
    }
}

const fn session_sort_field(value: SessionSortFieldDto) -> SessionSortField {
    match value {
        SessionSortFieldDto::Name => SessionSortField::Name,
        SessionSortFieldDto::Lifecycle => SessionSortField::Lifecycle,
        SessionSortFieldDto::Execution => SessionSortField::Execution,
        SessionSortFieldDto::Interaction => SessionSortField::Interaction,
        SessionSortFieldDto::Health => SessionSortField::Health,
        SessionSortFieldDto::UpdatedAt => SessionSortField::UpdatedAt,
        SessionSortFieldDto::RestartCount => SessionSortField::RestartCount,
    }
}

const fn sort_direction(value: SortDirectionDto) -> SortDirection {
    match value {
        SortDirectionDto::Ascending => SortDirection::Ascending,
        SortDirectionDto::Descending => SortDirection::Descending,
    }
}
