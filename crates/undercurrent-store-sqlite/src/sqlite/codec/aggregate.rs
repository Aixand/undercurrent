// SPDX-License-Identifier: Apache-2.0

//! Unversioned internal record components for complete session aggregates.
//!
//! Conversion revalidates domain grammar, uniqueness, duration precision and
//! platform-size conversions beneath the envelope byte bound. Constructor
//! limits are derived from retained values rather than current daemon policy.
//! Secret environment fields contain opaque references; ordinary environment,
//! health detail and WorkContext may still contain private data.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use undercurrent_domain::{
    CommandSpec, DomainError, EnvironmentVariables, ExecutionEnvironmentBinding, ExecutionLimits,
    ExecutionPolicy, ForegroundDecision, ForegroundObservation, HealthCheckResult,
    HealthObservation, HealthRule, HealthRuleKind, HealthRules, HealthStatus, HealthSummary,
    Labels, LaunchSpec, LifecycleState, Metadata, NoRestartReason, OutputCaptureMode,
    PathReference, PayloadDigest, ProcessIdentity, ProcessInstanceId, ProfileName, RestartConfig,
    RestartPolicy, RestartStatus, RestartTracker, Revision, SecretEnvironmentVariables,
    SecretReference, SessionConfiguration, SessionId, SessionName, SessionState, StopPolicy,
    StopSignal, TerminalSize, TimestampMillis, WorkContext, WorkItem, WorkItemId, WorkItemKey,
    WorkOwner, WorkProvenance, WorkRelationKind, WorkStatus, WorkTarget,
};

/// Failure to encode or validate one aggregate record.
#[derive(Debug)]
pub(crate) enum SessionCodecError {
    Json(serde_json::Error),
    Domain(DomainError),
    UnsupportedVersion(u32),
    RecordTooLarge { actual: usize, limit: usize },
    InvalidRecord(&'static str),
}

impl Display for SessionCodecError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json(_) => formatter.write_str("session record JSON is invalid"),
            Self::Domain(_) => formatter.write_str("session record violates domain invariants"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "session record version {version} is unsupported")
            }
            Self::RecordTooLarge { actual, limit } => {
                write!(
                    formatter,
                    "session record has {actual} bytes above limit {limit}"
                )
            }
            Self::InvalidRecord(rule) => write!(formatter, "session record is invalid: {rule}"),
        }
    }
}

impl Error for SessionCodecError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Json(error) => Some(error),
            Self::Domain(error) => Some(error),
            Self::UnsupportedVersion(_) | Self::RecordTooLarge { .. } | Self::InvalidRecord(_) => {
                None
            }
        }
    }
}

impl From<serde_json::Error> for SessionCodecError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl From<DomainError> for SessionCodecError {
    fn from(error: DomainError) -> Self {
        Self::Domain(error)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SessionStateRecord {
    id: String,
    name: String,
    lifecycle: LifecycleStateRecord,
    health: HealthSummaryRecord,
    health_observation: HealthObservationRecord,
    work_context: WorkContextRecord,
    configuration: SessionConfigurationRecord,

    restoration_directory: Option<String>,
    process: Option<ProcessIdentityRecord>,
    updated_at: i64,
    restart_tracker: u32,
    restart_status: RestartStatusRecord,
    revision: u64,
}

impl TryFrom<SessionState> for SessionStateRecord {
    type Error = SessionCodecError;

    fn try_from(state: SessionState) -> Result<Self, Self::Error> {
        Ok(Self {
            id: state.id.into_string(),
            name: state.name.into_string(),
            lifecycle: state.lifecycle.into(),
            health: (&state.health).into(),
            health_observation: (&state.health_observation).into(),
            work_context: (&state.work_context).into(),
            configuration: (&state.configuration).try_into()?,
            restoration_directory: state.restoration_directory.map(|p| p.as_str().to_owned()),
            process: state.process.as_ref().map(Into::into),
            updated_at: state.updated_at.value(),
            restart_tracker: state.restart_tracker.consecutive_attempts(),
            restart_status: state.restart_status.into(),
            revision: state.revision.value(),
        })
    }
}

impl SessionStateRecord {
    pub(super) fn into_state(
        self,
        foreground: ForegroundObservation,
        foreground_decision: ForegroundDecision,
    ) -> Result<SessionState, SessionCodecError> {
        let state = self;
        Ok(SessionState {
            id: SessionId::new(state.id)?,
            name: SessionName::new(state.name)?,
            lifecycle: state.lifecycle.into(),
            foreground,
            foreground_decision,
            health: state.health.try_into()?,
            health_observation: state.health_observation.try_into()?,
            work_context: state.work_context.try_into()?,
            configuration: state.configuration.try_into()?,
            restoration_directory: state
                .restoration_directory
                .map(|p| PathReference::new(p, 4096))
                .transpose()?,
            process: state.process.map(TryInto::try_into).transpose()?,
            updated_at: TimestampMillis::new(state.updated_at),
            restart_tracker: RestartTracker::new(state.restart_tracker),
            restart_status: state.restart_status.into(),
            revision: Revision::new(state.revision),
            lifecycle_revision: Revision::new(state.revision),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LifecycleStateRecord {
    Starting,
    Running,
    Stopping,
    Stopped,
    Exited { code: i32 },
    Failed,
}

impl From<LifecycleState> for LifecycleStateRecord {
    fn from(value: LifecycleState) -> Self {
        match value {
            LifecycleState::Starting => Self::Starting,
            LifecycleState::Running => Self::Running,
            LifecycleState::Stopping => Self::Stopping,
            LifecycleState::Stopped => Self::Stopped,
            LifecycleState::Exited(code) => Self::Exited { code },
            LifecycleState::Failed => Self::Failed,
        }
    }
}

impl From<LifecycleStateRecord> for LifecycleState {
    fn from(value: LifecycleStateRecord) -> Self {
        match value {
            LifecycleStateRecord::Starting => Self::Starting,
            LifecycleStateRecord::Running => Self::Running,
            LifecycleStateRecord::Stopping => Self::Stopping,
            LifecycleStateRecord::Stopped => Self::Stopped,
            LifecycleStateRecord::Exited { code } => Self::Exited(code),
            LifecycleStateRecord::Failed => Self::Failed,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HealthSummaryRecord {
    status: HealthStatusRecord,
    configured_checks: u16,
    observed_at: Option<i64>,
}

impl From<&HealthSummary> for HealthSummaryRecord {
    fn from(value: &HealthSummary) -> Self {
        Self {
            status: value.status().into(),
            configured_checks: value.configured_checks(),
            observed_at: value.observed_at().map(TimestampMillis::value),
        }
    }
}

impl TryFrom<HealthSummaryRecord> for HealthSummary {
    type Error = SessionCodecError;

    fn try_from(value: HealthSummaryRecord) -> Result<Self, Self::Error> {
        Ok(Self::new(
            value.status.into(),
            value.configured_checks,
            value.observed_at.map(TimestampMillis::new),
        )?)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum HealthStatusRecord {
    Healthy,
    Unhealthy,
    Unknown,
    NoCheck,
}

impl From<HealthStatus> for HealthStatusRecord {
    fn from(value: HealthStatus) -> Self {
        match value {
            HealthStatus::Healthy => Self::Healthy,
            HealthStatus::Unhealthy => Self::Unhealthy,
            HealthStatus::Unknown => Self::Unknown,
            HealthStatus::NoCheck => Self::NoCheck,
        }
    }
}

impl From<HealthStatusRecord> for HealthStatus {
    fn from(value: HealthStatusRecord) -> Self {
        match value {
            HealthStatusRecord::Healthy => Self::Healthy,
            HealthStatusRecord::Unhealthy => Self::Unhealthy,
            HealthStatusRecord::Unknown => Self::Unknown,
            HealthStatusRecord::NoCheck => Self::NoCheck,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HealthObservationRecord {
    summary: HealthSummaryRecord,
    results: Vec<HealthCheckResultRecord>,
}

impl From<&HealthObservation> for HealthObservationRecord {
    fn from(value: &HealthObservation) -> Self {
        Self {
            summary: value.summary().into(),
            results: value.results().iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<HealthObservationRecord> for HealthObservation {
    type Error = SessionCodecError;

    fn try_from(value: HealthObservationRecord) -> Result<Self, Self::Error> {
        let results = value
            .results
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self::rehydrate(value.summary.try_into()?, results)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HealthCheckResultRecord {
    rule_index: u16,
    passed: bool,
    latency: DurationRecord,
    detail: Option<String>,
}

impl From<&HealthCheckResult> for HealthCheckResultRecord {
    fn from(value: &HealthCheckResult) -> Self {
        Self {
            rule_index: value.rule_index(),
            passed: value.passed(),
            latency: value.latency().into(),
            detail: value.detail().map(str::to_owned),
        }
    }
}

impl TryFrom<HealthCheckResultRecord> for HealthCheckResult {
    type Error = SessionCodecError;

    fn try_from(value: HealthCheckResultRecord) -> Result<Self, Self::Error> {
        let max_detail_bytes = value
            .detail
            .as_ref()
            .map_or(1, |detail| detail.len().max(1));
        Ok(Self::new(
            value.rule_index,
            value.passed,
            value.latency.try_into()?,
            value.detail,
            max_detail_bytes,
        )?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkContextRecord {
    max_items: u64,
    revision: u64,
    items: Vec<WorkItemRecord>,
}

impl From<&WorkContext> for WorkContextRecord {
    fn from(value: &WorkContext) -> Self {
        Self {
            max_items: u64::try_from(value.max_items()).unwrap_or(u64::MAX),
            revision: value.revision().value(),
            items: value.items().map(Into::into).collect(),
        }
    }
}

impl TryFrom<WorkContextRecord> for WorkContext {
    type Error = SessionCodecError;

    fn try_from(value: WorkContextRecord) -> Result<Self, Self::Error> {
        let max_items = usize::try_from(value.max_items).map_err(|_| {
            SessionCodecError::InvalidRecord("WorkContext item limit exceeds this platform")
        })?;
        let items = value
            .items
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self::rehydrate(
            max_items,
            Revision::new(value.revision),
            items,
        )?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkItemRecord {
    owner: String,
    id: String,
    relation: WorkRelationKindRecord,
    title: String,
    description: Option<String>,
    status: WorkStatusRecord,
    targets: Vec<WorkTargetRecord>,
    provenance: String,
    metadata: Vec<(String, String)>,
    created_at: i64,
    updated_at: i64,
    expires_at: Option<i64>,
}

impl From<&WorkItem> for WorkItemRecord {
    fn from(value: &WorkItem) -> Self {
        Self {
            owner: value.key().owner().as_str().to_owned(),
            id: value.key().id().as_str().to_owned(),
            relation: value.relation().into(),
            title: value.title().to_owned(),
            description: value.description().map(str::to_owned),
            status: value.status().into(),
            targets: value.targets().map(Into::into).collect(),
            provenance: value.provenance().as_str().to_owned(),
            metadata: value
                .metadata()
                .iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect(),
            created_at: value.created_at().value(),
            updated_at: value.updated_at().value(),
            expires_at: value.expires_at().map(TimestampMillis::value),
        }
    }
}

impl TryFrom<WorkItemRecord> for WorkItem {
    type Error = SessionCodecError;

    fn try_from(value: WorkItemRecord) -> Result<Self, Self::Error> {
        let targets = value
            .targets
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<WorkTarget>, _>>()?;
        if targets.iter().collect::<BTreeSet<_>>().len() != targets.len() {
            return Err(SessionCodecError::InvalidRecord(
                "work item targets must be unique",
            ));
        }
        let owner = WorkOwner::new(value.owner)?;
        let provenance = WorkProvenance::new(value.provenance)?;
        Ok(Self::new_with_provenance(
            WorkItemKey::new(owner, WorkItemId::new(value.id)?),
            value.relation.into(),
            value.title,
            value.description,
            value.status.into(),
            targets,
            provenance,
            decode_metadata(value.metadata)?,
            TimestampMillis::new(value.created_at),
            TimestampMillis::new(value.updated_at),
            value.expires_at.map(TimestampMillis::new),
        )?)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WorkRelationKindRecord {
    DependsOn,
    Delegated,
    Spawned,
    Related,
}

impl From<WorkRelationKind> for WorkRelationKindRecord {
    fn from(value: WorkRelationKind) -> Self {
        match value {
            WorkRelationKind::DependsOn => Self::DependsOn,
            WorkRelationKind::Delegated => Self::Delegated,
            WorkRelationKind::Spawned => Self::Spawned,
            WorkRelationKind::Related => Self::Related,
        }
    }
}

impl From<WorkRelationKindRecord> for WorkRelationKind {
    fn from(value: WorkRelationKindRecord) -> Self {
        match value {
            WorkRelationKindRecord::DependsOn => Self::DependsOn,
            WorkRelationKindRecord::Delegated => Self::Delegated,
            WorkRelationKindRecord::Spawned => Self::Spawned,
            WorkRelationKindRecord::Related => Self::Related,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WorkStatusRecord {
    Running,
    Waiting,
    Blocked,
    Completed,
    Failed,
    Cancelled,
    DeliveryFailed,
    Expired,
}

impl From<WorkStatus> for WorkStatusRecord {
    fn from(value: WorkStatus) -> Self {
        match value {
            WorkStatus::Running => Self::Running,
            WorkStatus::Waiting => Self::Waiting,
            WorkStatus::Blocked => Self::Blocked,
            WorkStatus::Completed => Self::Completed,
            WorkStatus::Failed => Self::Failed,
            WorkStatus::Cancelled => Self::Cancelled,
            WorkStatus::DeliveryFailed => Self::DeliveryFailed,
            WorkStatus::Expired => Self::Expired,
        }
    }
}

impl From<WorkStatusRecord> for WorkStatus {
    fn from(value: WorkStatusRecord) -> Self {
        match value {
            WorkStatusRecord::Running => Self::Running,
            WorkStatusRecord::Waiting => Self::Waiting,
            WorkStatusRecord::Blocked => Self::Blocked,
            WorkStatusRecord::Completed => Self::Completed,
            WorkStatusRecord::Failed => Self::Failed,
            WorkStatusRecord::Cancelled => Self::Cancelled,
            WorkStatusRecord::DeliveryFailed => Self::DeliveryFailed,
            WorkStatusRecord::Expired => Self::Expired,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WorkTargetRecord {
    Session { session_id: String },
    Labels { labels: Vec<(String, String)> },
    External { system: String, id: String },
}

impl From<&WorkTarget> for WorkTargetRecord {
    fn from(value: &WorkTarget) -> Self {
        match value {
            WorkTarget::Session(session_id) => Self::Session {
                session_id: session_id.as_str().to_owned(),
            },
            WorkTarget::Labels(labels) => Self::Labels {
                labels: labels
                    .iter()
                    .map(|(key, value)| (key.to_owned(), value.to_owned()))
                    .collect(),
            },
            WorkTarget::External { system, id } => Self::External {
                system: system.as_str().to_owned(),
                id: id.as_str().to_owned(),
            },
        }
    }
}

impl TryFrom<WorkTargetRecord> for WorkTarget {
    type Error = SessionCodecError;

    fn try_from(value: WorkTargetRecord) -> Result<Self, Self::Error> {
        Ok(match value {
            WorkTargetRecord::Session { session_id } => Self::Session(SessionId::new(session_id)?),
            WorkTargetRecord::Labels { labels } => Self::Labels(decode_labels(labels)?),
            WorkTargetRecord::External { system, id } => Self::External {
                system: WorkOwner::new(system)?,
                id: WorkItemId::new(id)?,
            },
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionConfigurationRecord {
    launch: LaunchSpecRecord,
    execution: ExecutionPolicyRecord,
    health_rules: Vec<HealthRuleRecord>,
    profile: Option<String>,
    labels: Vec<(String, String)>,
    metadata: Vec<(String, String)>,
}

impl TryFrom<&SessionConfiguration> for SessionConfigurationRecord {
    type Error = SessionCodecError;

    fn try_from(value: &SessionConfiguration) -> Result<Self, Self::Error> {
        Ok(Self {
            launch: value.launch().into(),
            execution: value.execution().into(),
            health_rules: value
                .health_rules()
                .iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
            profile: value.profile().map(|profile| profile.as_str().to_owned()),
            labels: value
                .labels()
                .iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect(),
            metadata: value
                .metadata()
                .iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect(),
        })
    }
}

impl TryFrom<SessionConfigurationRecord> for SessionConfiguration {
    type Error = SessionCodecError;

    fn try_from(value: SessionConfigurationRecord) -> Result<Self, Self::Error> {
        let rule_limit = value.health_rules.len().max(1);
        let rules = value
            .health_rules
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;
        if rules.iter().collect::<BTreeSet<_>>().len() != rules.len() {
            return Err(SessionCodecError::InvalidRecord(
                "health rules must be unique",
            ));
        }
        Ok(Self::new(
            value.launch.try_into()?,
            value.execution.try_into()?,
            HealthRules::new(rules, limits_with_health_rule_count(rule_limit))?,
            value.profile.map(ProfileName::new).transpose()?,
            decode_labels(value.labels)?,
            decode_metadata(value.metadata)?,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LaunchSpecRecord {
    command: CommandSpecRecord,
    environment: Vec<(String, String)>,

    secret_environment: Vec<(String, String)>,
    environment_file: Option<String>,
    working_directory: Option<String>,

    working_directory_identity_sha256: Option<String>,

    environment_binding: Option<ExecutionEnvironmentBindingRecord>,
    terminal_columns: u16,
    terminal_rows: u16,

    output_capture_mode: OutputCaptureModeRecord,
}

impl From<&LaunchSpec> for LaunchSpecRecord {
    fn from(value: &LaunchSpec) -> Self {
        Self {
            command: value.command().into(),
            environment: value
                .environment()
                .iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect(),
            secret_environment: value
                .secret_environment()
                .iter()
                .map(|(key, reference)| (key.to_owned(), reference.as_str().to_owned()))
                .collect(),
            environment_file: value
                .environment_file()
                .map(|path| path.as_str().to_owned()),
            working_directory: value
                .working_directory()
                .map(|path| path.as_str().to_owned()),
            working_directory_identity_sha256: value
                .working_directory_identity()
                .map(PayloadDigest::to_hex),
            environment_binding: value.environment_binding().map(|binding| {
                ExecutionEnvironmentBindingRecord {
                    digest_sha256: binding.digest().to_hex(),
                    working_directory_root: binding.working_directory_root().as_str().to_owned(),
                    root_identity_sha256: binding.root_identity().map(PayloadDigest::to_hex),
                }
            }),
            terminal_columns: value.terminal_size().columns(),
            terminal_rows: value.terminal_size().rows(),
            output_capture_mode: value.output_capture_mode().into(),
        }
    }
}

impl TryFrom<LaunchSpecRecord> for LaunchSpec {
    type Error = SessionCodecError;

    fn try_from(value: LaunchSpecRecord) -> Result<Self, Self::Error> {
        let environment = decode_environment(value.environment)?;
        let secret_environment = decode_secret_environment(value.secret_environment, &environment)?;
        let combined_environment_limits = limits_with_environment(
            environment
                .len()
                .checked_add(secret_environment.len())
                .ok_or(SessionCodecError::InvalidRecord(
                    "environment entry count overflows this platform",
                ))?
                .max(1),
            environment
                .total_bytes()
                .checked_add(secret_environment.total_reference_bytes())
                .ok_or(SessionCodecError::InvalidRecord(
                    "environment metadata byte count overflows this platform",
                ))?
                .max(1),
        );
        let environment_file = value
            .environment_file
            .map(decode_path_reference)
            .transpose()?;
        let working_directory = value
            .working_directory
            .map(decode_path_reference)
            .transpose()?;
        let working_directory_identity = value
            .working_directory_identity_sha256
            .map(|identity| PayloadDigest::from_hex(&identity))
            .transpose()?;
        let environment_binding = value
            .environment_binding
            .map(|binding| {
                Ok::<_, SessionCodecError>(
                    ExecutionEnvironmentBinding::new(
                        PayloadDigest::from_hex(&binding.digest_sha256)?,
                        decode_path_reference(binding.working_directory_root)?,
                    )
                    .with_root_identity(
                        binding
                            .root_identity_sha256
                            .map(|identity| PayloadDigest::from_hex(&identity))
                            .transpose()?,
                    ),
                )
            })
            .transpose()?;
        let terminal_size = TerminalSize::new(
            value.terminal_columns,
            value.terminal_rows,
            value.terminal_columns,
            value.terminal_rows,
        )?;
        Ok(Self::new(
            value.command.try_into()?,
            environment,
            environment_file,
            working_directory,
            terminal_size,
        )
        .with_secret_environment(secret_environment, combined_environment_limits)?
        .with_environment_binding(environment_binding)
        .with_working_directory_identity(working_directory_identity)
        .with_output_capture_mode(value.output_capture_mode.into()))
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionEnvironmentBindingRecord {
    digest_sha256: String,
    working_directory_root: String,

    root_identity_sha256: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OutputCaptureModeRecord {
    #[default]
    Terminal,
    Opaque,
}

impl From<OutputCaptureMode> for OutputCaptureModeRecord {
    fn from(value: OutputCaptureMode) -> Self {
        match value {
            OutputCaptureMode::Terminal => Self::Terminal,
            OutputCaptureMode::Opaque => Self::Opaque,
        }
    }
}

impl From<OutputCaptureModeRecord> for OutputCaptureMode {
    fn from(value: OutputCaptureModeRecord) -> Self {
        match value {
            OutputCaptureModeRecord::Terminal => Self::Terminal,
            OutputCaptureModeRecord::Opaque => Self::Opaque,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandSpecRecord {
    program: String,

    launch_mode: StoredLaunchMode,
    arguments: Vec<String>,

    expected_executable_digest_sha256: Option<String>,

    executable_resolution: Option<ExecutableResolutionRecord>,
}

impl From<&CommandSpec> for CommandSpecRecord {
    fn from(value: &CommandSpec) -> Self {
        Self {
            launch_mode: value.launch_mode().into(),
            program: value.program().to_owned(),
            arguments: value.arguments().to_vec(),
            expected_executable_digest_sha256: value
                .expected_executable_digest()
                .map(PayloadDigest::to_hex),
            executable_resolution: value.executable_resolution().map(|resolution| {
                ExecutableResolutionRecord {
                    requested_reference: resolution.requested_reference().to_owned(),
                    content_digest_sha256: resolution.content_digest().to_hex(),
                    object_identity_sha256: resolution.object_identity().map(PayloadDigest::to_hex),
                    receipt_digest_sha256: resolution.receipt_digest().to_hex(),
                }
            }),
        }
    }
}

impl TryFrom<CommandSpecRecord> for CommandSpec {
    type Error = SessionCodecError;

    fn try_from(value: CommandSpecRecord) -> Result<Self, Self::Error> {
        let CommandSpecRecord {
            launch_mode,
            program,
            arguments,
            expected_executable_digest_sha256,
            executable_resolution,
        } = value;
        let argument_count = arguments.len();
        let requested_program = executable_resolution
            .as_ref()
            .map_or(program.as_str(), |resolution| {
                resolution.requested_reference.as_str()
            });
        let bytes = arguments.iter().try_fold(
            program.len().max(requested_program.len()),
            |total, argument| {
                total
                    .checked_add(argument.len())
                    .ok_or(SessionCodecError::InvalidRecord(
                        "command byte count overflows this platform",
                    ))
            },
        )?;
        let limits = limits_with_command(argument_count.max(1), bytes.max(1));
        let expected_executable_digest = expected_executable_digest_sha256
            .map(|digest| PayloadDigest::from_hex(&digest))
            .transpose()?;
        let command = Self::new(requested_program, arguments, limits)?
            .with_launch_mode(launch_mode.into())
            .with_expected_executable_digest(expected_executable_digest);
        match executable_resolution {
            None => Ok(command),
            Some(resolution) => {
                let content_digest = PayloadDigest::from_hex(&resolution.content_digest_sha256)?;
                let receipt_digest = PayloadDigest::from_hex(&resolution.receipt_digest_sha256)?;
                match resolution.object_identity_sha256 {
                    Some(identity) => Ok(command.pin_executable_with_identity(
                        program,
                        content_digest,
                        PayloadDigest::from_hex(&identity)?,
                        receipt_digest,
                        limits,
                    )?),
                    None => Ok(command.pin_executable(
                        program,
                        content_digest,
                        receipt_digest,
                        limits,
                    )?),
                }
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutableResolutionRecord {
    requested_reference: String,
    content_digest_sha256: String,

    object_identity_sha256: Option<String>,
    receipt_digest_sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionPolicyRecord {
    restart: RestartConfigRecord,
    stop_signal: StopSignalRecord,
    stop_force_after: DurationRecord,

    stop_no_escalation: bool,
    detection_enabled: bool,
    auto_remove: bool,
}

impl From<&ExecutionPolicy> for ExecutionPolicyRecord {
    fn from(value: &ExecutionPolicy) -> Self {
        Self {
            restart: value.restart().into(),
            stop_signal: value.stop().signal().into(),
            stop_force_after: value.stop().force_after().into(),
            stop_no_escalation: value.stop().no_escalation(),
            detection_enabled: value.detection_enabled(),
            auto_remove: value.auto_remove(),
        }
    }
}

impl TryFrom<ExecutionPolicyRecord> for ExecutionPolicy {
    type Error = SessionCodecError;

    fn try_from(value: ExecutionPolicyRecord) -> Result<Self, Self::Error> {
        Ok(Self::new(
            value.restart.try_into()?,
            StopPolicy::new(value.stop_signal.into(), value.stop_force_after.try_into()?)?
                .with_no_escalation(value.stop_no_escalation),
            value.detection_enabled,
            value.auto_remove,
        ))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StopSignalRecord {
    Interrupt,
    Terminate,
    Hangup,
    Quit,
    User1,
    User2,
}

impl From<StopSignal> for StopSignalRecord {
    fn from(value: StopSignal) -> Self {
        match value {
            StopSignal::Interrupt => Self::Interrupt,
            StopSignal::Terminate => Self::Terminate,
            StopSignal::Hangup => Self::Hangup,
            StopSignal::Quit => Self::Quit,
            StopSignal::User1 => Self::User1,
            StopSignal::User2 => Self::User2,
        }
    }
}

impl From<StopSignalRecord> for StopSignal {
    fn from(value: StopSignalRecord) -> Self {
        match value {
            StopSignalRecord::Interrupt => Self::Interrupt,
            StopSignalRecord::Terminate => Self::Terminate,
            StopSignalRecord::Hangup => Self::Hangup,
            StopSignalRecord::Quit => Self::Quit,
            StopSignalRecord::User1 => Self::User1,
            StopSignalRecord::User2 => Self::User2,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestartConfigRecord {
    policy: RestartPolicyRecord,
    max_restarts: u32,
    initial_delay: DurationRecord,
    backoff_basis_points: u32,
    max_delay: DurationRecord,
    min_uptime: DurationRecord,
}

impl From<&RestartConfig> for RestartConfigRecord {
    fn from(value: &RestartConfig) -> Self {
        Self {
            policy: value.policy().into(),
            max_restarts: value.max_restarts(),
            initial_delay: value.initial_delay().into(),
            backoff_basis_points: value.backoff_basis_points(),
            max_delay: value.max_delay().into(),
            min_uptime: value.min_uptime().into(),
        }
    }
}

impl TryFrom<RestartConfigRecord> for RestartConfig {
    type Error = SessionCodecError;

    fn try_from(value: RestartConfigRecord) -> Result<Self, Self::Error> {
        Ok(Self::new(
            value.policy.into(),
            value.max_restarts,
            value.initial_delay.try_into()?,
            value.backoff_basis_points,
            value.max_delay.try_into()?,
            value.min_uptime.try_into()?,
        )?)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RestartPolicyRecord {
    Never,
    OnFailure,
    Always,
}

impl From<RestartPolicy> for RestartPolicyRecord {
    fn from(value: RestartPolicy) -> Self {
        match value {
            RestartPolicy::Never => Self::Never,
            RestartPolicy::OnFailure => Self::OnFailure,
            RestartPolicy::Always => Self::Always,
        }
    }
}

impl From<RestartPolicyRecord> for RestartPolicy {
    fn from(value: RestartPolicyRecord) -> Self {
        match value {
            RestartPolicyRecord::Never => Self::Never,
            RestartPolicyRecord::OnFailure => Self::OnFailure,
            RestartPolicyRecord::Always => Self::Always,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum HealthRuleRecord {
    TcpPort { port: u16 },
    HttpEndpoint { value: String },
    OutputContains { value: String },
    OutputRejects { value: String },
    RedCellThreshold { minimum_cells: u32 },
    Command { command: CommandSpecRecord },
}

impl TryFrom<&HealthRule> for HealthRuleRecord {
    type Error = SessionCodecError;

    fn try_from(value: &HealthRule) -> Result<Self, Self::Error> {
        Ok(match value.kind() {
            HealthRuleKind::TcpPort => Self::TcpPort {
                port: value.port().ok_or(SessionCodecError::InvalidRecord(
                    "TCP health rule is missing its port",
                ))?,
            },
            HealthRuleKind::HttpEndpoint => Self::HttpEndpoint {
                value: value
                    .text_value()
                    .ok_or(SessionCodecError::InvalidRecord(
                        "HTTP health rule is missing its endpoint",
                    ))?
                    .to_owned(),
            },
            HealthRuleKind::OutputContains => Self::OutputContains {
                value: value
                    .text_value()
                    .ok_or(SessionCodecError::InvalidRecord(
                        "output health rule is missing its pattern",
                    ))?
                    .to_owned(),
            },
            HealthRuleKind::OutputRejects => Self::OutputRejects {
                value: value
                    .text_value()
                    .ok_or(SessionCodecError::InvalidRecord(
                        "rejection health rule is missing its pattern",
                    ))?
                    .to_owned(),
            },
            HealthRuleKind::RedCellThreshold => Self::RedCellThreshold {
                minimum_cells: value
                    .minimum_cells()
                    .ok_or(SessionCodecError::InvalidRecord(
                        "red-cell health rule is missing its threshold",
                    ))?,
            },
            HealthRuleKind::Command => Self::Command {
                command: value
                    .command_spec()
                    .ok_or(SessionCodecError::InvalidRecord(
                        "command health rule is missing its command",
                    ))?
                    .into(),
            },
        })
    }
}

impl TryFrom<HealthRuleRecord> for HealthRule {
    type Error = SessionCodecError;

    fn try_from(value: HealthRuleRecord) -> Result<Self, Self::Error> {
        Ok(match value {
            HealthRuleRecord::TcpPort { port } => Self::tcp_port(port)?,
            HealthRuleRecord::HttpEndpoint { value } => {
                let max_bytes = value.len().max(1);
                Self::http_endpoint(value, max_bytes)?
            }
            HealthRuleRecord::OutputContains { value } => {
                let max_bytes = value.len().max(1);
                Self::output_contains(value, max_bytes)?
            }
            HealthRuleRecord::OutputRejects { value } => {
                let max_bytes = value.len().max(1);
                Self::output_rejects(value, max_bytes)?
            }
            HealthRuleRecord::RedCellThreshold { minimum_cells } => {
                Self::red_cell_threshold(minimum_cells)?
            }
            HealthRuleRecord::Command { command } => Self::command(command.try_into()?),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessIdentityRecord {
    id: String,
    started_at: i64,
}

impl From<&ProcessIdentity> for ProcessIdentityRecord {
    fn from(value: &ProcessIdentity) -> Self {
        Self {
            id: value.id().as_str().to_owned(),
            started_at: value.started_at().value(),
        }
    }
}

impl TryFrom<ProcessIdentityRecord> for ProcessIdentity {
    type Error = SessionCodecError;

    fn try_from(value: ProcessIdentityRecord) -> Result<Self, Self::Error> {
        Ok(Self::new(
            ProcessInstanceId::new(value.id)?,
            TimestampMillis::new(value.started_at),
        ))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RestartStatusRecord {
    Idle,
    Scheduled { attempt: u32, not_before: i64 },
    NotScheduled { reason: NoRestartReasonRecord },
    Exhausted { attempts: u32 },
}

impl From<RestartStatus> for RestartStatusRecord {
    fn from(value: RestartStatus) -> Self {
        match value {
            RestartStatus::Idle => Self::Idle,
            RestartStatus::Scheduled {
                attempt,
                not_before,
            } => Self::Scheduled {
                attempt,
                not_before: not_before.value(),
            },
            RestartStatus::NotScheduled { reason } => Self::NotScheduled {
                reason: reason.into(),
            },
            RestartStatus::Exhausted { attempts } => Self::Exhausted { attempts },
        }
    }
}

impl From<RestartStatusRecord> for RestartStatus {
    fn from(value: RestartStatusRecord) -> Self {
        match value {
            RestartStatusRecord::Idle => Self::Idle,
            RestartStatusRecord::Scheduled {
                attempt,
                not_before,
            } => Self::Scheduled {
                attempt,
                not_before: TimestampMillis::new(not_before),
            },
            RestartStatusRecord::NotScheduled { reason } => Self::NotScheduled {
                reason: reason.into(),
            },
            RestartStatusRecord::Exhausted { attempts } => Self::Exhausted { attempts },
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum NoRestartReasonRecord {
    PolicyDisabled,
    SuccessfulExit,
    ExplicitStop,
}

impl From<NoRestartReason> for NoRestartReasonRecord {
    fn from(value: NoRestartReason) -> Self {
        match value {
            NoRestartReason::PolicyDisabled => Self::PolicyDisabled,
            NoRestartReason::SuccessfulExit => Self::SuccessfulExit,
            NoRestartReason::ExplicitStop => Self::ExplicitStop,
        }
    }
}

impl From<NoRestartReasonRecord> for NoRestartReason {
    fn from(value: NoRestartReasonRecord) -> Self {
        match value {
            NoRestartReasonRecord::PolicyDisabled => Self::PolicyDisabled,
            NoRestartReasonRecord::SuccessfulExit => Self::SuccessfulExit,
            NoRestartReasonRecord::ExplicitStop => Self::ExplicitStop,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurationRecord {
    seconds: u64,
    nanoseconds: u32,
}

impl From<Duration> for DurationRecord {
    fn from(value: Duration) -> Self {
        Self {
            seconds: value.as_secs(),
            nanoseconds: value.subsec_nanos(),
        }
    }
}

impl TryFrom<DurationRecord> for Duration {
    type Error = SessionCodecError;

    fn try_from(value: DurationRecord) -> Result<Self, Self::Error> {
        if value.nanoseconds >= 1_000_000_000 {
            return Err(SessionCodecError::InvalidRecord(
                "duration nanoseconds must be below one second",
            ));
        }
        Ok(Self::new(value.seconds, value.nanoseconds))
    }
}

fn decode_environment(
    entries: Vec<(String, String)>,
) -> Result<EnvironmentVariables, SessionCodecError> {
    ensure_unique_entry_keys(&entries, "environment variable names must be unique")?;
    let entry_count = entries.len();
    let total_bytes = entries.iter().try_fold(0usize, |total, (key, value)| {
        total
            .checked_add(key.len())
            .and_then(|with_key| with_key.checked_add(value.len()))
            .ok_or(SessionCodecError::InvalidRecord(
                "environment byte count overflows this platform",
            ))
    })?;
    Ok(EnvironmentVariables::new(
        entries,
        limits_with_environment(entry_count.max(1), total_bytes.max(1)),
    )?)
}

fn decode_secret_environment(
    entries: Vec<(String, String)>,
    ordinary: &EnvironmentVariables,
) -> Result<SecretEnvironmentVariables, SessionCodecError> {
    ensure_unique_entry_keys(&entries, "secret environment variable names must be unique")?;
    let reference_bytes = entries.iter().try_fold(0usize, |total, (key, reference)| {
        total
            .checked_add(key.len())
            .and_then(|with_key| with_key.checked_add(reference.len()))
            .ok_or(SessionCodecError::InvalidRecord(
                "secret environment metadata byte count overflows this platform",
            ))
    })?;
    let entry_limit = ordinary
        .len()
        .checked_add(entries.len())
        .ok_or(SessionCodecError::InvalidRecord(
            "environment entry count overflows this platform",
        ))?
        .max(1);
    let byte_limit = ordinary
        .total_bytes()
        .checked_add(reference_bytes)
        .ok_or(SessionCodecError::InvalidRecord(
            "environment metadata byte count overflows this platform",
        ))?
        .max(1);
    let entries = entries
        .into_iter()
        .map(|(name, reference)| Ok((name, SecretReference::new(reference)?)))
        .collect::<Result<Vec<_>, SessionCodecError>>()?;
    Ok(SecretEnvironmentVariables::new(
        entries,
        ordinary,
        limits_with_environment(entry_limit, byte_limit),
    )?)
}

fn decode_labels(entries: Vec<(String, String)>) -> Result<Labels, SessionCodecError> {
    ensure_unique_entry_keys(&entries, "label keys must be unique")?;
    Ok(Labels::new(entries)?)
}

fn decode_metadata(entries: Vec<(String, String)>) -> Result<Metadata, SessionCodecError> {
    ensure_unique_entry_keys(&entries, "metadata keys must be unique")?;
    Ok(Metadata::new(entries)?)
}

// Domain map constructors keep the last value for repeated keys. Reject them
// before construction so reading a durable record cannot silently discard data.
// See [retained validation](../../../docs/reference/storage-format.md#retained-fields-and-validation).
fn ensure_unique_entry_keys(
    entries: &[(String, String)],
    rule: &'static str,
) -> Result<(), SessionCodecError> {
    if entries
        .iter()
        .map(|(key, _)| key)
        .collect::<BTreeSet<_>>()
        .len()
        != entries.len()
    {
        return Err(SessionCodecError::InvalidRecord(rule));
    }
    Ok(())
}

fn decode_path_reference(value: String) -> Result<PathReference, SessionCodecError> {
    let max_path_bytes = value.len().max(1);
    Ok(PathReference::new(value, max_path_bytes)?)
}

// These are nonzero placeholders required by ExecutionLimits::validate, not
// daemon admission policy. Each caller replaces the dimensions it consumes with
// retained counts/lengths; the envelope byte bound limits the input as a whole.
const fn baseline_limits() -> ExecutionLimits {
    ExecutionLimits {
        max_arguments: 1,
        max_command_bytes: 1,
        max_environment_entries: 1,
        max_environment_bytes: 1,
        max_path_bytes: 1,
        max_health_rules: 1,
        max_health_rule_bytes: 1,
    }
}

fn limits_with_command(max_arguments: usize, max_command_bytes: usize) -> ExecutionLimits {
    ExecutionLimits {
        max_arguments,
        max_command_bytes,
        ..baseline_limits()
    }
}

fn limits_with_environment(
    max_environment_entries: usize,
    max_environment_bytes: usize,
) -> ExecutionLimits {
    ExecutionLimits {
        max_environment_entries,
        max_environment_bytes,
        ..baseline_limits()
    }
}

fn limits_with_health_rule_count(max_health_rules: usize) -> ExecutionLimits {
    ExecutionLimits {
        max_health_rules,
        ..baseline_limits()
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StoredLaunchMode {
    Native,
    #[default]
    Sealed,
}
impl From<undercurrent_domain::LaunchMode> for StoredLaunchMode {
    fn from(mode: undercurrent_domain::LaunchMode) -> Self {
        match mode {
            undercurrent_domain::LaunchMode::Native => Self::Native,
            undercurrent_domain::LaunchMode::Sealed => Self::Sealed,
        }
    }
}
impl From<StoredLaunchMode> for undercurrent_domain::LaunchMode {
    fn from(mode: StoredLaunchMode) -> Self {
        match mode {
            StoredLaunchMode::Native => Self::Native,
            StoredLaunchMode::Sealed => Self::Sealed,
        }
    }
}
