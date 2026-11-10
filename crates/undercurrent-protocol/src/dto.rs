// SPDX-License-Identifier: Apache-2.0

//! Typed, transport-facing Undercurrent product messages.

use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};

use serde::{Deserialize, Serialize};
use undercurrent_foundation::Classification;

use crate::ProductErrorDisposition;

use crate::ExecuteInSessionResultDto;
use crate::{
    AgentEventDto, CancelRuntimeAttemptRequest, CancelRuntimeAttemptResultDto,
    GetAgentEventRequest, GetAgentEventResultDto, GetRuntimeAttemptStatusRequest,
    GetRuntimeCancellationStatusRequest, RepairOperationAttemptRequest,
    RepairOperationAttemptResultDto, ReplyAgentEventRequest, ReplyAgentEventResultDto,
    RuntimeAttemptSessionSnapshotDto, RuntimeAttemptStatusResultDto,
    RuntimeCancellationStatusResultDto,
};
use serde_json::Value;

use crate::{ProductErrorCategory, ProductOperation, ProductStreamKind, SessionEventKindDto};

/// One finite request decoded after connection-level negotiation.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", content = "input", rename_all = "snake_case")]
pub enum ProductRequest {
    /// Start one durable managed session.
    StartSession(StartSessionRequest),
    /// Stop one managed session.
    StopSession(StopSessionRequest),
    /// Restart one managed session.
    RestartSession(RestartSessionRequest),
    /// Rename one session.
    RenameSession(RenameSessionRequest),
    /// Patch one session's query-visible labels without process effects.
    UpdateSessionLabels(UpdateSessionLabelsRequest),
    /// Remove one terminal session.
    RemoveSession(RemoveSessionRequest),
    /// Resize one running terminal.
    ResizeTerminal(ResizeTerminalRequest),
    /// Deliver ordered terminal input.
    SendTerminalInput(SendTerminalInputRequest),
    /// Close the managed process's standard-input stream.
    CloseStandardInput(CloseStandardInputRequest),
    /// Read one prepared session page.
    ListSessions(ListSessionsRequest),
    /// Read explicitly projected session detail.
    GetSessionDetail(GetSessionDetailRequest),
    /// Resolve private launch context for authorized one-shot execution.
    GetExecutionContext(GetExecutionContextRequest),
    /// Execute a bounded command under daemon supervision in a session context.
    ExecuteInSession(ExecuteInSessionRequest),
    /// Repair one Unknown operation attempt using server-retained evidence.
    RepairOperationAttempt(RepairOperationAttemptRequest),
    /// Read one exact caller-bound runtime attempt without changing product state.
    GetRuntimeAttemptStatus(GetRuntimeAttemptStatusRequest),
    /// Durably admit and apply control to one exact attached runtime attempt.
    CancelRuntimeAttempt(CancelRuntimeAttemptRequest),
    /// Read one exact durable runtime-cancellation control without redrive.
    GetRuntimeCancellationStatus(GetRuntimeCancellationStatusRequest),
    /// Read one exact unsettled event for an attached agent attempt.
    GetAgentEvent(GetAgentEventRequest),
    /// Durably reply to one exact attached-runtime agent event.
    ReplyAgentEvent(ReplyAgentEventRequest),
    /// Read retained terminal bytes.
    ReadTerminalOutput(ReadTerminalOutputRequest),
    /// Read the current terminal screen state.
    ReadTerminalScreen(ReadTerminalScreenRequest),
    /// Read a bounded update from an issued terminal screen checkpoint.
    ReadTerminalScreenDiff(ReadTerminalScreenDiffRequest),
    /// Read durable session events.
    ReadSessionEvents(ReadSessionEventsRequest),
    /// Insert or replace one WorkContext declaration.
    UpsertWorkItem(UpsertWorkItemRequest),
    /// Remove one WorkContext declaration.
    RemoveWorkItem(RemoveWorkItemRequest),
    /// List compact detection-profile summaries.
    ListProfiles(ListProfilesRequest),
    /// Get one complete detection profile.
    GetProfile(GetProfileRequest),
    /// Match process characteristics to profiles.
    MatchProfile(MatchProfileRequest),
    /// Validate configured detection profiles.
    LintProfiles(LintProfilesRequest),
    /// Evaluate one profile against a bounded sample.
    TestProfile(TestProfileRequest),
    /// Read bounded operational diagnostics.
    GetDiagnostics(GetDiagnosticsRequest),
    /// Create one bounded privacy-safe diagnostic bundle.
    CreateDiagnosticBundle(CreateDiagnosticBundleRequest),
    /// Read daemon identity and pressure.
    GetDaemonStatus,
    /// Begin graceful daemon shutdown.
    ShutdownDaemon(ShutdownDaemonRequest),
}

impl ProductRequest {
    /// Returns the explicit aggregate-revision precondition on the two
    /// negotiated session mutations. This is separate from an attempt fence.
    pub fn session_revision_precondition(&self) -> Option<u64> {
        match self {
            Self::StopSession(request) => request.expected_revision,
            Self::SendTerminalInput(request) => request.expected_revision,
            _ => None,
        }
    }

    /// Returns the stable operation without inspecting private payload fields.
    pub const fn operation(&self) -> ProductOperation {
        match self {
            Self::StartSession(_) => ProductOperation::StartSession,
            Self::StopSession(_) => ProductOperation::StopSession,
            Self::RestartSession(_) => ProductOperation::RestartSession,
            Self::RenameSession(_) => ProductOperation::RenameSession,
            Self::UpdateSessionLabels(_) => ProductOperation::UpdateSessionLabels,
            Self::RemoveSession(_) => ProductOperation::RemoveSession,
            Self::ResizeTerminal(_) => ProductOperation::ResizeTerminal,
            Self::SendTerminalInput(_) => ProductOperation::SendTerminalInput,
            Self::CloseStandardInput(_) => ProductOperation::CloseStandardInput,
            Self::ListSessions(_) => ProductOperation::ListSessions,
            Self::GetSessionDetail(_) => ProductOperation::GetSessionDetail,
            Self::GetExecutionContext(_) => ProductOperation::GetExecutionContext,
            Self::ExecuteInSession(_) => ProductOperation::ExecuteInSession,
            Self::RepairOperationAttempt(_) => ProductOperation::RepairOperationAttempt,
            Self::GetRuntimeAttemptStatus(_) => ProductOperation::GetRuntimeAttemptStatus,
            Self::CancelRuntimeAttempt(_) => ProductOperation::CancelRuntimeAttempt,
            Self::GetRuntimeCancellationStatus(_) => ProductOperation::GetRuntimeCancellationStatus,
            Self::GetAgentEvent(_) => ProductOperation::GetAgentEvent,
            Self::ReplyAgentEvent(_) => ProductOperation::ReplyAgentEvent,
            Self::ReadTerminalOutput(_) => ProductOperation::ReadTerminalOutput,
            Self::ReadTerminalScreen(_) => ProductOperation::ReadTerminalScreen,
            Self::ReadTerminalScreenDiff(_) => ProductOperation::ReadTerminalScreenDiff,
            Self::ReadSessionEvents(_) => ProductOperation::ReadSessionEvents,
            Self::UpsertWorkItem(_) => ProductOperation::UpsertWorkItem,
            Self::RemoveWorkItem(_) => ProductOperation::RemoveWorkItem,
            Self::ListProfiles(_) => ProductOperation::ListProfiles,
            Self::GetProfile(_) => ProductOperation::GetProfile,
            Self::MatchProfile(_) => ProductOperation::MatchProfile,
            Self::LintProfiles(_) => ProductOperation::LintProfiles,
            Self::TestProfile(_) => ProductOperation::TestProfile,
            Self::GetDiagnostics(_) => ProductOperation::GetDiagnostics,
            Self::CreateDiagnosticBundle(_) => ProductOperation::CreateDiagnosticBundle,
            Self::GetDaemonStatus => ProductOperation::GetDaemonStatus,
            Self::ShutdownDaemon(_) => ProductOperation::ShutdownDaemon,
        }
    }

    /// Returns mutation context for operations that may use semantic retry.
    pub const fn mutation(&self) -> Option<&MutationContext> {
        match self {
            Self::StartSession(request) => Some(&request.mutation),
            Self::StopSession(request) => Some(&request.mutation),
            Self::RestartSession(request) => Some(&request.mutation),
            Self::RenameSession(request) => Some(&request.mutation),
            Self::UpdateSessionLabels(request) => Some(&request.mutation),
            Self::RemoveSession(request) => Some(&request.mutation),
            Self::ResizeTerminal(request) => Some(&request.mutation),
            Self::SendTerminalInput(request) => Some(&request.mutation),
            Self::CloseStandardInput(request) => Some(&request.mutation),
            Self::ExecuteInSession(request) => Some(&request.mutation),
            Self::CancelRuntimeAttempt(request) => Some(request.mutation()),
            Self::ReplyAgentEvent(request) => Some(&request.mutation),
            Self::UpsertWorkItem(request) => Some(&request.mutation),
            Self::RemoveWorkItem(request) => Some(&request.mutation),
            Self::ShutdownDaemon(request) => Some(&request.mutation),
            Self::ListSessions(_)
            | Self::GetSessionDetail(_)
            | Self::GetExecutionContext(_)
            | Self::ReadTerminalOutput(_)
            | Self::ReadTerminalScreen(_)
            | Self::ReadTerminalScreenDiff(_)
            | Self::ReadSessionEvents(_)
            | Self::ListProfiles(_)
            | Self::GetProfile(_)
            | Self::MatchProfile(_)
            | Self::LintProfiles(_)
            | Self::TestProfile(_)
            | Self::GetDiagnostics(_)
            | Self::CreateDiagnosticBundle(_)
            | Self::GetDaemonStatus
            | Self::RepairOperationAttempt(_)
            | Self::GetRuntimeAttemptStatus(_) => None,
            Self::GetRuntimeCancellationStatus(_) | Self::GetAgentEvent(_) => None,
        }
    }

    /// Returns the single session identifier explicitly targeted by this
    /// request, when one exists.
    ///
    /// The returned string is transport input and remains unvalidated. Callers
    /// must validate it before using it as diagnostic or authorization context.
    /// Multi-session queries intentionally return `None`.
    pub fn target_session_id(&self) -> Option<&str> {
        match self {
            Self::StopSession(request) => Some(&request.session_id),
            Self::RestartSession(request) => Some(&request.session_id),
            Self::RenameSession(request) => Some(&request.session_id),
            Self::UpdateSessionLabels(request) => Some(&request.session_id),
            Self::RemoveSession(request) => Some(&request.session_id),
            Self::ResizeTerminal(request) => Some(&request.session_id),
            Self::SendTerminalInput(request) => Some(&request.session_id),
            Self::CloseStandardInput(request) => Some(&request.session_id),
            Self::GetSessionDetail(request) => Some(&request.session_id),
            Self::GetExecutionContext(request) => Some(&request.session_id),
            Self::ExecuteInSession(request) => Some(&request.session_id),
            Self::CancelRuntimeAttempt(request) => Some(request.session_id()),
            Self::GetRuntimeCancellationStatus(request) => Some(request.session_id()),
            Self::GetAgentEvent(request) => Some(&request.session_id),
            Self::ReplyAgentEvent(request) => Some(&request.session_id),
            Self::ReadTerminalOutput(request) => Some(&request.session_id),
            Self::ReadTerminalScreen(request) => Some(&request.session_id),
            Self::ReadTerminalScreenDiff(request) => Some(&request.session_id),
            Self::CreateDiagnosticBundle(request) => request
                .terminal_content
                .as_ref()
                .map(|content| content.session_id.as_str()),
            Self::UpsertWorkItem(request) => Some(&request.session_id),
            Self::RemoveWorkItem(request) => Some(&request.session_id),
            Self::StartSession(_)
            | Self::ListSessions(_)
            | Self::ReadSessionEvents(_)
            | Self::ListProfiles(_)
            | Self::GetProfile(_)
            | Self::MatchProfile(_)
            | Self::LintProfiles(_)
            | Self::TestProfile(_)
            | Self::GetDiagnostics(_)
            | Self::GetDaemonStatus
            | Self::ShutdownDaemon(_)
            | Self::RepairOperationAttempt(_)
            | Self::GetRuntimeAttemptStatus(_) => None,
        }
    }
}

impl Debug for ProductRequest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProductRequest")
            .field("operation", &self.operation())
            .finish_non_exhaustive()
    }
}

/// Idempotency and trace-safe context for a mutating operation.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationContext {
    /// Stable client-generated retry identity when ambiguous retry is required.
    pub idempotency_key: Option<String>,
    /// Durable product attempt metadata for process-affecting mutations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<crate::OperationAttemptDto>,
    /// Explicit capability, schema, and contract versions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract: Option<crate::ProductContractDto>,
    /// Additive fields retained for compatible re-encoding.
    #[serde(default, flatten)]
    pub compatible_unknown: BTreeMap<String, Value>,
}

impl MutationContext {
    /// Creates current 0.1.0-beta request metadata for one mutating operation.
    pub fn current(operation: ProductOperation) -> Self {
        Self {
            contract: Some(crate::ProductContractDto::current(
                operation,
                crate::ContractPayloadKind::Request,
            )),
            ..Self::default()
        }
    }
}

/// Executable launch policy. Omitted launch mode selects the current sealed default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LaunchModeDto {
    /// Run from the original installation path.
    Native,
    /// Run verified immutable executable bytes.
    #[default]
    Sealed,
}

impl LaunchModeDto {
    /// Whether this is the default sealed policy, omitted from canonical requests.
    pub fn is_sealed(&self) -> bool {
        matches!(self, Self::Sealed)
    }
}

/// Complete external request to create and activate a session.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartSessionRequest {
    /// Persisted executable policy; pinned dispatches require sealed.
    #[serde(default, skip_serializing_if = "LaunchModeDto::is_sealed")]
    pub launch_mode: LaunchModeDto,
    /// Retry context for the semantic start operation.
    #[serde(default)]
    pub mutation: MutationContext,
    /// Immutable opaque caller identity and independently pinned dispatch inputs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch: Option<Box<RuntimeDispatchBindingDto>>,
    /// Unique human-facing session name.
    pub name: String,
    /// Executable and argument vector.
    pub command: CommandSpecDto,
    /// Optional working-directory reference.
    pub working_directory: Option<String>,
    /// Explicit environment values and optional environment-file reference.
    #[serde(default)]
    pub environment: EnvironmentSpecDto,
    /// Initial terminal dimensions.
    pub terminal: TerminalSizeDto,
    /// Whether output is interpreted as terminal data or retained as opaque bytes.
    #[serde(default, skip_serializing_if = "OutputCaptureModeDto::is_terminal")]
    pub output_capture_mode: OutputCaptureModeDto,
    /// Process lifecycle policy.
    #[serde(default)]
    pub execution: ExecutionPolicyDto,
    /// Configured health checks.
    #[serde(default)]
    pub health: HealthRulesDto,
    /// Explicit or automatically selected detection profile.
    pub profile: Option<String>,
    /// Whether the selected base profile may follow foreground process ownership.
    #[serde(default)]
    pub profile_binding: ProfileBindingModeDto,
    /// Query-visible labels.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Authorized private metadata, never returned in compact list responses.
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

/// Opaque caller-owned identity and content pins for an externally dispatched start.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeDispatchBindingDto {
    /// Caller-owned identity whose monotonic fencing sequence guards the dispatch.
    pub caller_cell_identity: String,
    /// Exact lowercase SHA-256 digest of the caller's immutable dispatch request.
    pub request_digest_sha256: String,
    /// Exact lowercase SHA-256 digest of the pinned launch profile.
    pub launch_profile_digest_sha256: String,
    /// Exact executable content digest supplied by the pinned launch profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable_digest_sha256: Option<String>,
    /// Exact lowercase SHA-256 digest of the opaque environment binding.
    pub environment_binding_digest_sha256: String,
    /// Exact lowercase SHA-256 digest of the pinned input set.
    pub input_digest_sha256: String,
    /// Complete profile-compiled runtime policy.
    ///
    /// The daemon rejects attached dispatches without this value, and retained
    /// dispatch bindings require complete policy provenance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_policy: Option<Box<RuntimeDispatchPolicyDto>>,
    /// Exact caller-owned semantic binding for an agent-task runtime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<Box<crate::AgentRuntimeBindingDto>>,
}

/// Exact positive runtime and output ceilings compiled into a launch profile.
///
/// These requested ceilings are retained for audit. This protocol does not
/// assert that the selected process adapter enforces any operating-system limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeResourceLimitsDto {
    /// Compiled/requested wall-clock ceiling retained for audit; enforcement is not proven.
    pub wall_time_millis: u64,
    /// Compiled/requested stdout-byte ceiling retained for audit; enforcement is not proven.
    pub stdout_bytes: u64,
    /// Compiled/requested stderr-byte ceiling retained for audit; enforcement is not proven.
    pub stderr_bytes: u64,
    /// Optional compiled/requested memory ceiling retained for audit; enforcement is not proven.
    #[serde(deserialize_with = "deserialize_required_option")]
    pub memory_bytes: Option<u64>,
    /// Optional compiled/requested CPU ceiling retained for audit; enforcement is not proven.
    #[serde(deserialize_with = "deserialize_required_option")]
    pub cpu_millis: Option<u32>,
}

// Using deserialize_with without serde(default) keeps memory/CPU members
// required even though their values may be null. A plain Option would also
// accept omission, losing the [explicit policy shape](../../../docs/reference/protocol/operations.md#attached-start-policy).
fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

/// Pinned schema and privacy contract for canonical runtime output.
#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeOutputContractDto {
    /// Exact output schema identifier.
    pub schema: String,
    /// Lowercase, unprefixed SHA-256 of the exact output schema document.
    pub schema_digest_sha256: String,
    /// Foundation privacy classification required for canonical output.
    pub classification: Classification,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeOutputContractWire {
    schema: String,
    schema_digest_sha256: String,
    classification: Classification,
}

impl<'de> Deserialize<'de> for RuntimeOutputContractDto {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = RuntimeOutputContractWire::deserialize(deserializer)?;
        if wire.classification == Classification::Secret {
            return Err(serde::de::Error::custom(
                "Secret runtime output requires immutable content-reference transport",
            ));
        }
        Ok(Self {
            schema: wire.schema,
            schema_digest_sha256: wire.schema_digest_sha256,
            classification: wire.classification,
        })
    }
}

impl Debug for RuntimeOutputContractDto {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeOutputContractDto")
            .field("schema", &self.schema)
            .field("classification", &self.classification)
            .finish_non_exhaustive()
    }
}

/// Consequential launch policy that must reach the external daemon unchanged.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeDispatchPolicyDto {
    /// Positive resource and output bounds.
    pub limits: RuntimeResourceLimitsDto,
    /// Exact schema, digest, and classification expected from the attempt.
    pub output_contract: RuntimeOutputContractDto,
    /// Immutable telemetry policy reference validated at daemon admission.
    pub telemetry_policy_ref: String,
    /// Immutable output-retention policy reference validated at daemon admission.
    pub retention_policy_ref: String,
    /// Strictly sorted, unique product-owned runtime-callback 0.1.0-beta references.
    pub callback_refs: Vec<String>,
    /// Unix epoch milliseconds after which the launch profile is stale.
    pub launch_profile_expires_at_millis: i64,
    /// Unix epoch milliseconds after which the environment binding is stale.
    pub environment_binding_expires_at_millis: i64,
}

impl Debug for RuntimeDispatchPolicyDto {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeDispatchPolicyDto")
            .field("limits", &self.limits)
            .field("output_contract", &self.output_contract)
            .field("callback_count", &self.callback_refs.len())
            .field(
                "launch_profile_expires_at_millis",
                &self.launch_profile_expires_at_millis,
            )
            .field(
                "environment_binding_expires_at_millis",
                &self.environment_binding_expires_at_millis,
            )
            .finish_non_exhaustive()
    }
}

/// Resolved subset of an environment binding needed to constrain one process
/// working directory.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionEnvironmentBindingDto {
    /// Exact lowercase SHA-256 digest of the complete caller-supplied binding.
    pub binding_digest_sha256: String,
    /// Root inside which the configured working directory must resolve.
    pub working_directory_root: String,
}

/// Derives a stable binding digest for a standalone local-directory launch.
///
/// Ecosystem callers retain the owning Environment product's binding digest.
/// This helper serves standalone local-directory launches, including the CLI
/// and Python bridge. It hashes the exact directory bytes without validating
/// the path; see the [encoding reference](../../../docs/reference/protocol/encoding.md).
pub fn standalone_environment_binding_digest_sha256(working_directory_root: &str) -> String {
    let digest = undercurrent_foundation::ContentDigest::sha256(
        [
            b"undercurrent.standalone-environment-binding.v0.1.0-beta\0".as_slice(),
            working_directory_root.as_bytes(),
        ]
        .concat()
        .as_slice(),
    )
    .to_string();
    digest
        .strip_prefix("sha256:")
        .expect("Foundation SHA-256 digests always carry the algorithm prefix")
        .to_owned()
}

/// Output interpretation selected when a managed session is launched.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputCaptureModeDto {
    /// Interpret output as terminal data and maintain terminal screen state.
    #[default]
    Terminal,
    /// Preserve output as opaque bytes without terminal interpretation.
    Opaque,
}

impl OutputCaptureModeDto {
    /// Returns whether this is the compatible terminal-output default.
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Terminal)
    }
}

/// Runtime binding policy for a session's effective detection profile.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileBindingModeDto {
    /// Keep the launch-selected base profile effective for the process lifetime.
    #[default]
    Fixed,
    /// Permit exact foreground process-group evidence to replace the base profile.
    ForegroundAdaptive,
}

/// Executable and ordered argument vector.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandSpecDto {
    /// Executable path or resolvable program name.
    pub program: String,
    /// Ordered process arguments without a synthetic `argv[0]`.
    #[serde(default)]
    pub arguments: Vec<String>,
}

impl Debug for CommandSpecDto {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CommandSpecDto")
            .field("program_bytes", &self.program.len())
            .field("argument_count", &self.arguments.len())
            .finish_non_exhaustive()
    }
}

/// Environment persistence schema for the complete 0.1.0-beta contract.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentSchemaVersionDto {
    /// Every raw value is explicitly classified as non-secret.
    #[default]
    #[serde(rename = "0.1.0-beta")]
    Current,
}

/// A raw entry is credential-classified or conflicts with a secret reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CredentialClassifiedEnvironment;

impl std::fmt::Display for CredentialClassifiedEnvironment {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("credential-classified environment values require secret references")
    }
}

impl std::error::Error for CredentialClassifiedEnvironment {}

/// Environment inputs whose values must never enter diagnostics.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentSpecDto {
    /// Explicit environment persistence schema generation.
    pub schema_version: EnvironmentSchemaVersionDto,
    /// Explicit process environment values.
    #[serde(default, serialize_with = "serialize_non_secret_environment")]
    pub variables: BTreeMap<String, String>,
    /// Durable handles resolved ephemerally by an authorized adapter.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub secret_variables: BTreeMap<String, crate::SecretReferenceDto>,
    /// Optional environment-file reference resolved by the runtime adapter.
    pub file: Option<String>,
    /// Supplied environment boundary constraining working-directory resolution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding: Option<ExecutionEnvironmentBindingDto>,
}

impl EnvironmentSpecDto {
    /// Validates that raw values are safe for durable state, events, and artifacts.
    pub fn validate_for_persistence(&self) -> Result<(), CredentialClassifiedEnvironment> {
        if self
            .variables
            .keys()
            .any(|name| self.secret_variables.contains_key(name))
            || raw_environment_contains_sensitive_material(&self.variables)
        {
            return Err(CredentialClassifiedEnvironment);
        }
        Ok(())
    }
}

// Serialization also feeds canonical request hashing, so it must reject detected
// raw sensitive material even when persistence validation was not called first.
// Map overlap still needs the [separate persistence check](../../../docs/reference/protocol/encoding.md#environment-persistence-checks).
fn serialize_non_secret_environment<S>(
    values: &BTreeMap<String, String>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    if raw_environment_contains_sensitive_material(values) {
        return Err(serde::ser::Error::custom(
            "credential-classified environment values cannot be serialized",
        ));
    }
    serde::Serialize::serialize(values, serializer)
}

fn raw_environment_contains_sensitive_material(values: &BTreeMap<String, String>) -> bool {
    values.iter().any(|(name, value)| {
        undercurrent_foundation::is_sensitive_field_name(name)
            || undercurrent_foundation::credential_like_environment_text(value)
    })
}

/// Process execution and bounded recovery policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPolicyDto {
    /// Automatic restart behavior.
    #[serde(default)]
    pub restart: RestartPolicyDto,
    /// Graceful termination behavior.
    #[serde(default)]
    pub stop: StopPolicyDto,
    /// Whether foreground observation is scheduled.
    #[serde(default = "default_true")]
    pub detection_enabled: bool,
    /// Whether a session is removed automatically after becoming terminal.
    #[serde(default)]
    pub auto_remove: bool,
}

impl Default for ExecutionPolicyDto {
    fn default() -> Self {
        Self {
            restart: RestartPolicyDto::default(),
            stop: StopPolicyDto::default(),
            detection_enabled: true,
            auto_remove: false,
        }
    }
}

/// Automatic process restart behavior.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum RestartPolicyDto {
    /// Never restart automatically.
    #[default]
    Never,
    /// Restart after non-successful process exit.
    OnFailure {
        /// Maximum consecutive attempts.
        max_attempts: u32,
        /// Initial delay before the first attempt.
        initial_delay_millis: u64,
        /// Maximum exponential-backoff delay.
        max_delay_millis: u64,
        /// Exponential multiplier in basis points, where 10,000 is 1.0x.
        backoff_basis_points: u32,
        /// Successful uptime required to reset consecutive attempts.
        min_uptime_millis: u64,
    },
    /// Restart after every process exit.
    Always {
        /// Maximum consecutive attempts.
        max_attempts: u32,
        /// Initial delay before the first attempt.
        initial_delay_millis: u64,
        /// Maximum exponential-backoff delay.
        max_delay_millis: u64,
        /// Exponential multiplier in basis points, where 10,000 is 1.0x.
        backoff_basis_points: u32,
        /// Successful uptime required to reset consecutive attempts.
        min_uptime_millis: u64,
    },
}

/// Graceful stop signal and escalation deadline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopPolicyDto {
    /// Portable signal vocabulary interpreted by the runtime adapter.
    pub signal: StopSignalDto,
    /// Time before forced termination is permitted.
    pub timeout_millis: u64,
}

impl Default for StopPolicyDto {
    fn default() -> Self {
        Self {
            signal: StopSignalDto::Terminate,
            timeout_millis: 5_000,
        }
    }
}

/// Portable graceful stop signals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopSignalDto {
    /// Conventional termination request.
    Terminate,
    /// Interrupt comparable to an interactive Ctrl-C.
    Interrupt,
    /// Hangup request.
    Hangup,
    /// Quit request.
    Quit,
    /// First user-defined signal.
    User1,
    /// Second user-defined signal.
    User2,
}

/// Configured bounded health rules.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthRulesDto {
    /// Individual checks evaluated by the observation adapter.
    #[serde(default)]
    pub rules: Vec<HealthRuleDto>,
}

/// One health-check declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HealthRuleDto {
    /// Connect to a local TCP port.
    TcpPort { port: u16 },
    /// Perform an HTTP request to the supplied endpoint.
    HttpEndpoint { endpoint: String },
    /// Require a pattern in recent output.
    OutputContains { pattern: String },
    /// Reject a pattern in recent output.
    OutputRejects { pattern: String },
    /// Fail above a red-cell threshold.
    RedCellThreshold { minimum_cells: u32 },
    /// Execute a bounded external check command.
    Command { command: CommandSpecDto },
}

/// Target and retry context for a one-session mutation.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionMutationRequest {
    /// Retry context.
    #[serde(default)]
    pub mutation: MutationContext,
    /// Stable target session identity.
    pub session_id: String,
}

/// Restart policy; omitted force preserves graceful wire behavior.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestartSessionRequest {
    #[serde(default)]
    pub mutation: MutationContext,
    pub session_id: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub force: bool,
}

/// One daemon-supervised command using the target session's resolved context.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecuteInSessionRequest {
    /// Durable retry, attempt, and contract metadata.
    #[serde(default)]
    pub mutation: MutationContext,
    /// Stable target session identity.
    pub session_id: String,
    /// Use the saved launch directory instead of the observed live directory.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub dir_launch: bool,
    /// Use launch environment settings instead of the latest shell exports.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub env_launch: bool,
    /// Executable and ordered argument vector.
    pub command: CommandSpecDto,
    /// Maximum bytes returned inline across stdout and stderr.
    pub output_inline_limit_bytes: u32,
    /// Absolute daemon-clock deadline in Unix epoch milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_millis: Option<i64>,
}

/// Stop request with explicit force selection.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopSessionRequest {
    /// Refuse automatic forced escalation after the graceful deadline.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub graceful_only: bool,
    /// Retry context.
    #[serde(default)]
    pub mutation: MutationContext,
    /// Stable target session identity.
    pub session_id: String,
    /// If present, the daemon checks this aggregate revision while holding the
    /// session mutation lease, before changing state or signaling the process.
    /// Omission preserves the existing unconditional stop contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<u64>,
    /// Skip graceful termination and force process termination.
    #[serde(default)]
    pub force: bool,
}

/// Rename request preserving stable identity.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenameSessionRequest {
    /// Retry context.
    #[serde(default)]
    pub mutation: MutationContext,
    /// Stable target session identity.
    pub session_id: String,
    /// New unique human-facing name.
    pub name: String,
}

/// Atomic partial update to one session's query-visible labels.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateSessionLabelsRequest {
    /// Retry context.
    #[serde(default)]
    pub mutation: MutationContext,
    /// Stable target session identity.
    pub session_id: String,
    /// Keys to insert or replace; unmentioned labels are preserved.
    #[serde(default)]
    pub set: BTreeMap<String, String>,
    /// Keys to remove. A key cannot also appear in `set`.
    #[serde(default)]
    pub remove: Vec<String>,
}

/// Destructive removal request with explicit presentation confirmation.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoveSessionRequest {
    /// Retry context.
    #[serde(default)]
    pub mutation: MutationContext,
    /// Stable target session identity.
    pub session_id: String,
    /// Evidence collected by the presentation boundary before deletion.
    pub confirmation: RemovalConfirmationDto,
}

/// Presentation-boundary evidence authorizing destructive removal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemovalConfirmationDto {
    /// An interactive caller accepted a confirmation prompt.
    Confirmed,
    /// A non-interactive caller supplied an explicit force option.
    Forced,
}

/// Terminal dimensions supplied to domain validation by the daemon boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalSizeDto {
    /// Terminal columns.
    pub columns: u16,
    /// Terminal rows.
    pub rows: u16,
}

/// Live and durable terminal resize request.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResizeTerminalRequest {
    /// Retry context.
    #[serde(default)]
    pub mutation: MutationContext,
    /// Stable target session identity.
    pub session_id: String,
    /// Requested dimensions.
    pub size: TerminalSizeDto,
}

/// Ordered terminal input request.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SendTerminalInputRequest {
    /// Retry context; delivery ID remains the semantic deduplication identity.
    #[serde(default)]
    pub mutation: MutationContext,
    /// Stable target session identity.
    pub session_id: String,
    /// If present, the daemon checks this aggregate revision while holding the
    /// session mutation lease, before reserving delivery or writing input.
    /// Omission preserves the existing unconditional input contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<u64>,
    /// Optional stable semantic delivery identity.
    pub delivery_id: Option<String>,
    /// Complete sequence written atomically relative to peer sequences.
    pub items: Vec<TerminalInputDto>,
    /// Effect of confirmed input on best-effort foreground observation.
    #[serde(default)]
    pub observation_policy: InputObservationPolicyDto,
}

/// Idempotent request to close one managed process's standard-input stream.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloseStandardInputRequest {
    /// Retry and contract context.
    #[serde(default)]
    pub mutation: MutationContext,
    /// Stable target session identity.
    pub session_id: String,
}

/// Best-effort foreground behavior after confirmed terminal input.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputObservationPolicyDto {
    /// Mark programmatic delivery as active work.
    #[default]
    ForceActive,
    /// Preserve current foreground observation for direct human interaction.
    Preserve,
}

/// One typed terminal input item.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TerminalInputDto {
    /// UTF-8 prompt text using the effective profile default.
    AutoText { text: String },
    /// UTF-8 text typed without an implicit newline.
    Text { text: String },
    /// Explicit bracketed paste input.
    Paste { text: String },
    /// Named terminal key.
    Key { key: KeyDto },
    /// Explicit escaped byte sequence represented as byte values.
    Raw { bytes: Vec<u8> },
    /// SGR-compatible mouse event.
    Mouse { event: MouseEventDto },
}

/// Named terminal keys with optional modifiers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "key", rename_all = "snake_case")]
pub enum KeyDto {
    Enter,
    Tab,
    BackTab,
    Escape,
    Backspace,
    Delete,
    Space,
    Up,
    Down,
    Left,
    Right,
    Home,
    End,
    PageUp,
    PageDown,
    Function {
        number: u8,
    },
    Control {
        character: char,
    },
    Modified {
        base: ModifiableKeyDto,
        #[serde(default)]
        modifiers: KeyModifiersDto,
    },
}

/// Base key accepted by the modified-key encoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "key", rename_all = "snake_case")]
pub enum ModifiableKeyDto {
    Up,
    Down,
    Left,
    Right,
    Home,
    End,
    PageUp,
    PageDown,
    Function { number: u8 },
}

/// Keyboard modifier state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyModifiersDto {
    #[serde(default)]
    pub shift: bool,
    #[serde(default)]
    pub control: bool,
    #[serde(default)]
    pub alt: bool,
}

/// One one-based terminal mouse event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MouseEventDto {
    pub column: u16,
    pub row: u16,
    pub button: MouseButtonDto,
    pub event: MouseEventKindDto,
}

/// Mouse button vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MouseButtonDto {
    Left,
    Middle,
    Right,
    ScrollUp,
    ScrollDown,
}

/// Mouse event vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MouseEventKindDto {
    Press,
    Release,
    Move,
}

/// Prepared list query and opaque cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListSessionsRequest {
    #[serde(default)]
    pub filter: SessionFilterDto,
    #[serde(default)]
    pub sort: SessionSortDto,
    #[serde(default)]
    pub projection: SessionProjectionDto,
    pub page_size: u32,
    pub cursor: Option<String>,
    #[serde(default)]
    pub include_total: bool,
}

/// Server-side session selection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionFilterDto {
    #[serde(default)]
    pub ids: Vec<String>,
    /// Exact case-sensitive session name resolved through the prepared unique-name index.
    pub name: Option<String>,
    pub name_contains: Option<String>,
    #[serde(default)]
    pub lifecycles: Vec<LifecycleKindDto>,
    #[serde(default)]
    pub executions: Vec<ExecutionStateDto>,
    #[serde(default)]
    pub interactions: Vec<InteractionStateDto>,
    #[serde(default)]
    pub health: Vec<HealthStatusDto>,
    pub profile: Option<String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    pub has_active_work: Option<bool>,
}

/// Stable summary sort.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionSortDto {
    #[serde(default)]
    pub field: SessionSortFieldDto,
    #[serde(default)]
    pub direction: SortDirectionDto,
}

/// Summary projection controlling payload size.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionProjectionDto {
    #[default]
    Compact,
    WithLabels,
    Operational,
}

/// Stable sort fields.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionSortFieldDto {
    #[default]
    Name,
    Lifecycle,
    Execution,
    Interaction,
    Health,
    UpdatedAt,
    RestartCount,
}

/// Sort direction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortDirectionDto {
    #[default]
    Ascending,
    Descending,
}

/// Explicit sections for an authorized detail query.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionDetailProjectionDto {
    #[serde(default)]
    pub configuration: bool,
    #[serde(default)]
    pub work_context: bool,
    #[serde(default)]
    pub process: bool,
    #[serde(default)]
    pub foreground_evidence: bool,
    #[serde(default)]
    pub health_results: bool,
}

/// Authorized session detail query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetSessionDetailRequest {
    pub session_id: String,
    #[serde(default)]
    pub projection: SessionDetailProjectionDto,
}

/// Private target for authorized one-shot execution context resolution.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetExecutionContextRequest {
    /// Stable managed session identity.
    pub session_id: String,
}

/// Private environment and cwd returned only to an authorized exec adapter.
///
/// This DTO must never be logged, traced, or reused as a list/detail
/// projection because it contains environment values.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionContextDto {
    /// Fully merged explicit and environment-file variables.
    pub environment: BTreeMap<String, String>,
    /// Resolved absolute working directory when one was configured.
    pub working_directory: Option<String>,
}

/// One bounded retained-output read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadTerminalOutputRequest {
    pub session_id: String,
    pub from_offset: u64,
    pub max_bytes: u32,
}

/// One current terminal-screen read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadTerminalScreenRequest {
    pub session_id: String,
}

/// One bounded update from a client-scoped terminal screen checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadTerminalScreenDiffRequest {
    pub session_id: String,
    /// Opaque checkpoint returned by a prior screen snapshot or update.
    pub checkpoint: String,
}

/// One bounded durable-event read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadSessionEventsRequest {
    /// Read the most recent matching observations as an initial baseline.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub tail: bool,
    /// Opt in to versioned event-time observations.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub include_observation: bool,
    #[serde(default)]
    pub session_ids: Vec<String>,
    #[serde(default)]
    pub kinds: Vec<SessionEventKindDto>,
    pub after_sequence: Option<u64>,
    pub limit: u32,
}

/// Insert or replace one externally declared work item.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpsertWorkItemRequest {
    #[serde(default)]
    pub mutation: MutationContext,
    pub session_id: String,
    pub item: WorkItemDto,
}

/// Remove one owner-scoped work declaration.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoveWorkItemRequest {
    #[serde(default)]
    pub mutation: MutationContext,
    pub session_id: String,
    pub owner: String,
    pub item_id: String,
}

/// Complete external WorkContext declaration.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkItemDto {
    pub owner: String,
    pub item_id: String,
    pub relationship: WorkRelationshipDto,
    pub title: String,
    pub description: Option<String>,
    pub status: WorkStatusDto,
    #[serde(default)]
    pub targets: Vec<WorkTargetDto>,
    pub provenance: String,
    pub created_at_millis: i64,
    pub updated_at_millis: i64,
    pub expires_at_millis: Option<i64>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

/// Relationship declared by an external owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkRelationshipDto {
    DependsOn,
    Delegated,
    Spawned,
    Related,
}

/// External work status kept independent from foreground observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkStatusDto {
    Running,
    Waiting,
    Blocked,
    Completed,
    Failed,
    Cancelled,
    DeliveryFailed,
    Expired,
}

/// Typed target that may be resolved by an observation adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkTargetDto {
    SessionId { session_id: String },
    Labels { labels: BTreeMap<String, String> },
    External { namespace: String, selector: String },
}

/// Bounded profile list request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListProfilesRequest {
    pub page_size: u32,
    pub cursor: Option<String>,
}

/// Detection profile name request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetProfileRequest {
    pub name: String,
}

/// Process characteristics used for deterministic profile selection.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatchProfileRequest {
    pub executable: String,
    #[serde(default)]
    pub arguments: Vec<String>,
    #[serde(default)]
    pub environment_names: Vec<String>,
}

/// Profile lint scope.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LintProfilesRequest {
    pub name: Option<String>,
}

/// Bounded screen sample used to explain one profile decision.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestProfileRequest {
    pub name: String,
    pub screen_text: String,
    pub last_line: String,
    pub cursor_at_end: bool,
}

/// Bounded operational diagnostic selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetDiagnosticsRequest {
    #[serde(default)]
    pub sections: Vec<DiagnosticSectionDto>,
    pub page_size: u32,
    pub cursor: Option<String>,
}

/// Maximum terminal bytes carried by one diagnostic-bundle response.
pub const MAX_DIAGNOSTIC_BUNDLE_TERMINAL_CONTENT_BYTES: u32 = 16_384;

/// One bounded privacy-safe diagnostic-bundle request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateDiagnosticBundleRequest {
    /// Nonempty unique safe diagnostic sections captured into one page.
    pub sections: Vec<DiagnosticSectionDto>,
    /// Maximum safe diagnostic rows returned in the page.
    pub page_size: u32,
    /// Opaque continuation cursor returned by a prior bundle page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Explicit sensitive-content request; absent is the safe default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_content: Option<DiagnosticBundleTerminalContentRequestDto>,
}

impl CreateDiagnosticBundleRequest {
    /// Validates current-0.1.0-beta bounds before authorization or source access.
    pub fn validate(&self) -> Result<(), InvalidDiagnosticBundleRequest> {
        if self.sections.is_empty() || self.page_size == 0 {
            return Err(InvalidDiagnosticBundleRequest::InvalidDiagnosticsSelection);
        }
        let unique = self
            .sections
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        if unique.len() != self.sections.len() || unique.len() > DiagnosticSectionDto::ALL.len() {
            return Err(InvalidDiagnosticBundleRequest::InvalidDiagnosticsSelection);
        }
        if self.terminal_content.as_ref().is_some_and(|content| {
            content.max_bytes == 0
                || content.max_bytes > MAX_DIAGNOSTIC_BUNDLE_TERMINAL_CONTENT_BYTES
        }) {
            return Err(InvalidDiagnosticBundleRequest::InvalidTerminalContentLimit);
        }
        Ok(())
    }
}

/// Explicit session-scoped terminal content selected for a diagnostic bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticBundleTerminalContentRequestDto {
    pub session_id: String,
    pub from_offset: u64,
    pub max_bytes: u32,
}

/// Invalid diagnostic-bundle selection rejected before authorization or IO.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidDiagnosticBundleRequest {
    /// Sections or page size are empty, duplicated, or outside the registry.
    InvalidDiagnosticsSelection,
    /// Terminal content is zero or exceeds the public bundle byte bound.
    InvalidTerminalContentLimit,
}

impl std::fmt::Display for InvalidDiagnosticBundleRequest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidDiagnosticsSelection => {
                formatter.write_str("invalid diagnostic bundle selection")
            }
            Self::InvalidTerminalContentLimit => formatter
                .write_str("diagnostic bundle terminal content exceeds its bounded request limit"),
        }
    }
}

impl std::error::Error for InvalidDiagnosticBundleRequest {}

/// Public safe diagnostic sections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticSectionDto {
    Connections,
    Streams,
    SlowConsumers,
    OutputProducers,
    SupervisorQueues,
    DetectionEvidence,
    Persistence,
    Reconciliation,
    Configuration,
}

impl DiagnosticSectionDto {
    /// Complete safe diagnostic-section registry.
    pub const ALL: [Self; 9] = [
        Self::Connections,
        Self::Streams,
        Self::SlowConsumers,
        Self::OutputProducers,
        Self::SupervisorQueues,
        Self::DetectionEvidence,
        Self::Persistence,
        Self::Reconciliation,
        Self::Configuration,
    ];
}

/// Shutdown policy for existing work and connected clients.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonShutdownModeDto {
    /// Allow accepted work and connections to drain within the requested window.
    #[default]
    Graceful,
    /// Cancel other work and close connections after the shutdown response write.
    Reset,
    /// Cancel work and close clients promptly, preserving persisted user state.
    Immediate,
}

impl DaemonShutdownModeDto {
    fn is_graceful(&self) -> bool {
        *self == Self::Graceful
    }
}

/// Explicit daemon shutdown request.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShutdownDaemonRequest {
    #[serde(default)]
    pub mutation: MutationContext,
    pub drain_timeout_millis: u64,
    /// Omitted by graceful callers for compatibility with existing daemons.
    #[serde(default, skip_serializing_if = "DaemonShutdownModeDto::is_graceful")]
    pub mode: DaemonShutdownModeDto,
}

/// One logical-stream specification decoded before activation.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "stream", content = "input", rename_all = "snake_case")]
pub enum ProductStreamSpec {
    /// Initial prepared page followed by deltas.
    SessionWatch(SessionWatchSpec),
    /// Filtered durable session events.
    SessionEvents(SessionEventsSpec),
    /// Explicitly selected terminal output.
    TerminalOutput(TerminalOutputSpec),
    /// Durable agent tool calls for one exact fenced runtime attempt.
    AgentEvents(AgentEventsSpec),
}

impl ProductStreamSpec {
    /// Returns the stable stream kind without inspecting payload data.
    pub const fn kind(&self) -> ProductStreamKind {
        match self {
            Self::SessionWatch(_) => ProductStreamKind::SessionWatch,
            Self::SessionEvents(_) => ProductStreamKind::SessionEvents,
            Self::TerminalOutput(_) => ProductStreamKind::TerminalOutput,
            Self::AgentEvents(_) => ProductStreamKind::AgentEvents,
        }
    }

    /// Returns the single transport-supplied session target when the stream is
    /// scoped to exactly one session.
    ///
    /// The returned string remains unvalidated and must be validated before it
    /// is used as diagnostic or authorization context.
    pub fn target_session_id(&self) -> Option<&str> {
        match self {
            Self::TerminalOutput(specification) => Some(&specification.session_id),
            Self::AgentEvents(specification) => Some(&specification.session_id),
            Self::SessionWatch(_) | Self::SessionEvents(_) => None,
        }
    }
}

impl Debug for ProductStreamSpec {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProductStreamSpec")
            .field("kind", &self.kind())
            .finish_non_exhaustive()
    }
}

/// Query watch specification sharing list filter and projection semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionWatchSpec {
    pub query: ListSessionsRequest,
}

/// Filtered event subscription specification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionEventsSpec {
    #[serde(default)]
    pub session_ids: Vec<String>,
    #[serde(default)]
    pub kinds: Vec<SessionEventKindDto>,
    /// Last durable global event sequence already consumed. An absent value
    /// subscribes from the journal head without replaying retained history.
    #[serde(default)]
    pub after_sequence: Option<u64>,
}

/// Exact durable agent-event subscription target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentEventsSpec {
    pub tenant_id: String,
    pub source_product: String,
    pub actor_context_ref: String,
    pub authority_ref: String,
    pub undercurrent_provider_id: String,
    pub undercurrent_provider_generation: u64,
    pub session_id: String,
    pub attempt_id: String,
    pub attempt_revision: u64,
    pub recovery_epoch: u64,
    pub fence: u64,
    pub request_digest_sha256: String,
    /// Last durable agent-event sequence already committed by the subscriber.
    pub after_sequence: u64,
}

/// Initial terminal-output behavior for one selected session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalOutputSpec {
    pub session_id: String,
    pub initial: TerminalInitialModeDto,
}

/// Initial output sent before live terminal bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum TerminalInitialModeDto {
    None,
    Screen,
    Tail { max_bytes: u32 },
    Resume { after_offset: u64 },
}

/// Finite response carrying either one typed success or a typed safe error.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", content = "value", rename_all = "snake_case")]
pub enum ProductResponse {
    Success(Box<ProductSuccess>),
    Error(ProductError),
}

impl ProductResponse {
    /// Returns the operation when decoding identified it.
    pub const fn operation(&self) -> Option<ProductOperation> {
        match self {
            Self::Success(success) => Some(success.operation()),
            Self::Error(error) => error.operation,
        }
    }

    /// Returns authoritative or presentation-safe single-session context when
    /// the result carries one.
    pub fn session_id(&self) -> Option<&str> {
        match self {
            Self::Success(success) => success.session_id(),
            Self::Error(error) => error.session_id.as_deref(),
        }
    }
}

impl Debug for ProductResponse {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let category = match self {
            Self::Success(_) => None,
            Self::Error(error) => Some(error.category),
        };
        formatter
            .debug_struct("ProductResponse")
            .field("operation", &self.operation())
            .field("error_category", &category)
            .finish_non_exhaustive()
    }
}

/// Typed successful result registry.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", content = "result", rename_all = "snake_case")]
pub enum ProductSuccess {
    StartSession(SessionMutationResultDto),
    StopSession(StopSessionResultDto),
    RestartSession(SessionMutationResultDto),
    RenameSession(SessionMutationResultDto),
    /// Updated labels, stable session identity, and changed flag.
    UpdateSessionLabels(SessionMutationResultDto),
    RemoveSession(RemoveSessionResultDto),
    ResizeTerminal(SessionMutationResultDto),
    SendTerminalInput(SendTerminalInputResultDto),
    CloseStandardInput(CloseStandardInputResultDto),
    ListSessions(SessionPageDto),
    GetSessionDetail(Box<SessionDetailDto>),
    GetExecutionContext(ExecutionContextDto),
    ExecuteInSession(ExecuteInSessionResultDto),
    /// Evidence-backed attempt repair and post-commit readiness.
    RepairOperationAttempt(RepairOperationAttemptResultDto),
    /// Read-only exact runtime-attempt status snapshot.
    RuntimeAttemptStatus(RuntimeAttemptStatusResultDto),
    /// Durable exact runtime-attempt cancellation control result.
    CancelRuntimeAttempt(CancelRuntimeAttemptResultDto),
    /// Read-only exact runtime-cancellation status result.
    RuntimeCancellationStatus(RuntimeCancellationStatusResultDto),
    /// Read-only exact agent-event poll result.
    GetAgentEvent(GetAgentEventResultDto),
    /// Durable exact agent-event reply acknowledgement.
    ReplyAgentEvent(ReplyAgentEventResultDto),
    ReadTerminalOutput(TerminalOutputPageDto),
    ReadTerminalScreen(TerminalScreenSnapshotDto),
    ReadTerminalScreenDiff(TerminalScreenUpdateDto),
    ReadSessionEvents(SessionEventPageDto),
    UpsertWorkItem(SessionMutationResultDto),
    RemoveWorkItem(SessionMutationResultDto),
    ListProfiles(ProfilePageDto),
    GetProfile(ProfileDetailDto),
    MatchProfile(ProfileMatchDto),
    LintProfiles(ProfileLintDto),
    TestProfile(ProfileTestDto),
    GetDiagnostics(DiagnosticPageDto),
    CreateDiagnosticBundle(DiagnosticBundleDto),
    GetDaemonStatus(DaemonStatusDto),
    ShutdownDaemon(ShutdownAcceptedDto),
}

impl ProductSuccess {
    /// Returns the stable successful operation.
    pub const fn operation(&self) -> ProductOperation {
        match self {
            Self::StartSession(_) => ProductOperation::StartSession,
            Self::StopSession(_) => ProductOperation::StopSession,
            Self::RestartSession(_) => ProductOperation::RestartSession,
            Self::RenameSession(_) => ProductOperation::RenameSession,
            Self::UpdateSessionLabels(_) => ProductOperation::UpdateSessionLabels,
            Self::RemoveSession(_) => ProductOperation::RemoveSession,
            Self::ResizeTerminal(_) => ProductOperation::ResizeTerminal,
            Self::SendTerminalInput(_) => ProductOperation::SendTerminalInput,
            Self::CloseStandardInput(_) => ProductOperation::CloseStandardInput,
            Self::ListSessions(_) => ProductOperation::ListSessions,
            Self::GetSessionDetail(_) => ProductOperation::GetSessionDetail,
            Self::GetExecutionContext(_) => ProductOperation::GetExecutionContext,
            Self::ExecuteInSession(_) => ProductOperation::ExecuteInSession,
            Self::ReadTerminalOutput(_) => ProductOperation::ReadTerminalOutput,
            Self::ReadTerminalScreen(_) => ProductOperation::ReadTerminalScreen,
            Self::ReadTerminalScreenDiff(_) => ProductOperation::ReadTerminalScreenDiff,
            Self::ReadSessionEvents(_) => ProductOperation::ReadSessionEvents,
            Self::UpsertWorkItem(_) => ProductOperation::UpsertWorkItem,
            Self::RemoveWorkItem(_) => ProductOperation::RemoveWorkItem,
            Self::ListProfiles(_) => ProductOperation::ListProfiles,
            Self::GetProfile(_) => ProductOperation::GetProfile,
            Self::MatchProfile(_) => ProductOperation::MatchProfile,
            Self::LintProfiles(_) => ProductOperation::LintProfiles,
            Self::TestProfile(_) => ProductOperation::TestProfile,
            Self::GetDiagnostics(_) => ProductOperation::GetDiagnostics,
            Self::CreateDiagnosticBundle(_) => ProductOperation::CreateDiagnosticBundle,
            Self::GetDaemonStatus(_) => ProductOperation::GetDaemonStatus,
            Self::ShutdownDaemon(_) => ProductOperation::ShutdownDaemon,
            Self::RepairOperationAttempt(_) => ProductOperation::RepairOperationAttempt,
            Self::RuntimeAttemptStatus(_) => ProductOperation::GetRuntimeAttemptStatus,
            Self::CancelRuntimeAttempt(_) => ProductOperation::CancelRuntimeAttempt,
            Self::RuntimeCancellationStatus(_) => ProductOperation::GetRuntimeCancellationStatus,
            Self::GetAgentEvent(_) => ProductOperation::GetAgentEvent,
            Self::ReplyAgentEvent(_) => ProductOperation::ReplyAgentEvent,
        }
    }

    /// Returns the single session represented by this result when available.
    pub fn session_id(&self) -> Option<&str> {
        match self {
            Self::StartSession(result)
            | Self::RestartSession(result)
            | Self::RenameSession(result)
            | Self::UpdateSessionLabels(result)
            | Self::ResizeTerminal(result)
            | Self::UpsertWorkItem(result)
            | Self::RemoveWorkItem(result) => Some(&result.session.id),
            Self::StopSession(result) => Some(&result.session.id),
            Self::RemoveSession(result) => Some(&result.session_id),
            Self::SendTerminalInput(result) => Some(&result.session_id),
            Self::CloseStandardInput(result) => Some(&result.session_id),
            Self::GetSessionDetail(result) => Some(&result.summary.id),
            Self::ExecuteInSession(result) => Some(&result.session_id),
            Self::RuntimeAttemptStatus(result) => result
                .snapshot()
                .session()
                .map(RuntimeAttemptSessionSnapshotDto::session_id),
            Self::CancelRuntimeAttempt(result) => Some(result.state().session().session_id()),
            Self::RuntimeCancellationStatus(result) => Some(result.state().session().session_id()),
            Self::GetAgentEvent(result) => match result {
                GetAgentEventResultDto::Event { event } => Some(&event.session_id),
                GetAgentEventResultDto::None { .. } => None,
            },
            Self::ReplyAgentEvent(result) => Some(&result.session_id),
            Self::ListSessions(_)
            | Self::GetExecutionContext(_)
            | Self::ReadTerminalOutput(_)
            | Self::ReadTerminalScreen(_)
            | Self::ReadTerminalScreenDiff(_)
            | Self::ReadSessionEvents(_)
            | Self::ListProfiles(_)
            | Self::GetProfile(_)
            | Self::MatchProfile(_)
            | Self::LintProfiles(_)
            | Self::TestProfile(_)
            | Self::GetDiagnostics(_)
            | Self::CreateDiagnosticBundle(_)
            | Self::GetDaemonStatus(_)
            | Self::ShutdownDaemon(_)
            | Self::RepairOperationAttempt(_) => None,
        }
    }
}

/// Safe error response produced only at the presentation boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProductError {
    /// Identified operation, absent only when request decoding failed first.
    pub operation: Option<ProductOperation>,
    pub category: ProductErrorCategory,
    /// Exact high-level disposition when its optional connection capability was negotiated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disposition: Option<ProductErrorDisposition>,
    /// Owner evidence about the complete operation, emitted only when the
    /// error-effect-certainty capability was negotiated. Absence does not prove
    /// no effect; categories and lower-level port failures are not substitutes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect_certainty: Option<crate::EffectCertaintyDto>,
    pub message: String,
    pub session_id: Option<String>,
    pub retryable: bool,
    pub retry_after_millis: Option<u64>,
    pub limit: Option<u64>,
    pub resynchronization: Option<ResynchronizationDto>,
}

/// Required recovery after stale replay or cursor state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResynchronizationDto {
    RequestSnapshot,
    RestartQuery,
    RestartFromAvailableOffset,
}

/// Common durable mutation result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionMutationResultDto {
    pub session: SessionSummaryDto,
    pub changed: bool,
    /// Durable proof of executable resolution before initial session acceptance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable_resolution: Option<ExecutableResolutionDto>,
}

/// Public pre-acceptance executable resolution receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutableResolutionDto {
    /// Whether this receipt pins future executable bytes or records native acceptance.
    #[serde(default, skip_serializing_if = "LaunchModeDto::is_sealed")]
    pub launch_mode: LaunchModeDto,
    /// Caller-supplied path, program name, or executable URI.
    pub requested_reference: String,
    /// Exact absolute path selected before durable acceptance.
    pub resolved_program: String,
    /// SHA-256 digest of the selected executable bytes.
    pub content_digest_sha256: String,
    /// Digest binding the reference, selected path, content, and environment binding.
    pub receipt_digest_sha256: String,
}

/// Explicit stop disposition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopSessionResultDto {
    pub session: SessionSummaryDto,
    pub already_terminal: bool,
    pub exit_code: Option<i32>,
}

/// Explicit removal outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoveSessionResultDto {
    pub session_id: String,
    pub removal_revision: u64,
}

/// Terminal input delivery outcome without echoing private input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SendTerminalInputResultDto {
    pub session_id: String,
    pub delivered_at_millis: i64,
    pub delivery_id: Option<String>,
    pub duplicate: bool,
    pub foreground_revision: Option<u64>,
}

/// Standard-input closure outcome without terminal or command payloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloseStandardInputResultDto {
    pub session_id: String,
    pub closed_at_millis: i64,
    pub already_closed: bool,
}

/// One compact prepared session page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionPageDto {
    pub items: Vec<SessionSummaryDto>,
    pub next_cursor: Option<String>,
    pub snapshot_revision: u64,
    pub total: Option<u64>,
}

/// Compact session summary that excludes configuration and output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionSummaryDto {
    pub id: String,
    pub name: String,
    pub lifecycle: LifecycleStateDto,
    pub foreground: ForegroundObservationDto,
    pub health: HealthSummaryDto,
    pub work: WorkSummaryDto,
    pub profile: Option<String>,
    pub profile_context: EffectiveProfileContextDto,
    pub labels: Option<BTreeMap<String, String>>,
    pub started_at_millis: Option<i64>,
    pub updated_at_millis: i64,
    pub restart_count: u32,
    pub restart_status: RestartStatusDto,
    pub revision: u64,
    pub lifecycle_revision: u64,
}

/// Base binding and process-local evidence behind the effective profile field.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectiveProfileContextDto {
    /// All confirmed matches before automatic fallback filtering.
    #[serde(default)]
    pub matching_evidence: Vec<ProfileMatchEvidenceDto>,
    pub base_profile: Option<String>,
    #[serde(default)]
    pub binding: ProfileBindingModeDto,
    pub base_origin: Option<ProfileSelectionOriginDto>,
    pub effective_origin: EffectiveProfileOriginDto,
    pub epoch: u64,
    pub unclassified_reason: Option<UnclassifiedProfileReasonDto>,
    pub observed_at_millis: Option<i64>,
    #[serde(default)]
    pub current_directory: Option<String>,
    #[serde(default)]
    pub runtime_role: Option<RuntimeRoleDto>,
    #[serde(default)]
    pub server_role_origin: Option<ServerRoleOriginDto>,
}

/// Payload-free explanation of one foreground profile candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileMatchEvidenceDto {
    pub profile: String,
    pub matching_fallback: bool,
    pub suppressed: bool,
    pub process_incarnations: Vec<String>,
}

/// Conservative runtime role derived independently from lifecycle and waiting state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeRoleDto {
    Server,
}

/// Evidence family supporting the conservative server runtime role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServerRoleOriginDto {
    Profile,
    TcpListener,
    ProfileAndTcpListener,
    /// Exact managed root ownership, without asserting foreground membership.
    ManagedProcessTcpListener,
    /// Server profile plus exact managed root listener ownership.
    ProfileAndManagedProcessTcpListener,
    /// A member of the managed process tree; background children are included.
    ManagedTreeTcpListener,
    ProfileAndManagedTreeTcpListener,
}

/// Evidence family responsible for the current effective profile state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectiveProfileOriginDto {
    /// Durable base configuration is authoritative.
    #[default]
    BaseProfile,
    /// Exact foreground process-group evidence is authoritative.
    ForegroundProcess,
    /// A payload-free shell-ready signal restored the base profile.
    CooperativeShell,
}

/// Why no effective profile could be selected safely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnclassifiedProfileReasonDto {
    AwaitingObservation,
    NoMatch,
    Ambiguous,
    IncompleteEvidence,
    Unavailable,
}

/// Durable disposition of the latest automatic-restart decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RestartStatusDto {
    /// No automatic-restart decision is pending for the active process.
    Idle,
    /// An automatic restart is due at or after the supplied timestamp.
    Scheduled {
        attempt: u32,
        not_before_millis: i64,
    },
    /// The last terminal process outcome is final for the supplied reason.
    NotScheduled { reason: NoRestartReasonDto },
    /// Restart policy permitted another attempt, but its bound was exhausted.
    Exhausted { attempts: u32 },
}

/// Stable reason an automatic restart will not occur.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NoRestartReasonDto {
    /// Automatic restart is disabled.
    PolicyDisabled,
    /// An `on_failure` policy observed a successful exit.
    SuccessfulExit,
    /// An operator explicitly stopped the session before its pending restart.
    ExplicitStop,
}

/// Authoritative lifecycle including exit detail when known.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum LifecycleStateDto {
    Starting,
    Running,
    Stopping,
    Stopped,
    Exited { code: Option<i32> },
    Failed,
}

/// Lifecycle filter vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleKindDto {
    Starting,
    Running,
    Stopping,
    Stopped,
    Exited,
    Failed,
}

/// Best-effort foreground execution state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStateDto {
    Active,
    Quiescent,
    Unknown,
}

/// Mechanical interaction availability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionStateDto {
    None,
    Available,
    Requested,
}

/// Mechanical input form for an observed interaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionModalityDto {
    Text,
    Options,
    Secret,
    Navigation,
}

/// Whether a profile-owned runtime constraint is currently present.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeConstraintStateDto {
    Clear,
    Present,
}

/// Affirmative, inferred, or unavailable evidence disposition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceDispositionDto {
    Affirmative,
    Inference,
    Unavailable,
}

/// Confidence, provenance, and freshness shared by one observation axis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationBasisDto {
    pub confidence_basis_points: u16,
    pub source: Option<String>,
    pub disposition: EvidenceDispositionDto,
    pub committed_at_millis: i64,
    pub evidence_observed_at_millis: Option<i64>,
}

/// Compact committed execution observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionObservationDto {
    pub state: ExecutionStateDto,
    pub basis: ObservationBasisDto,
}

/// Compact committed interaction observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InteractionObservationDto {
    pub state: InteractionStateDto,
    pub modality: Option<InteractionModalityDto>,
    pub semantic: Option<String>,
    pub basis: ObservationBasisDto,
}

/// Compact committed runtime-constraint observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConstraintObservationDto {
    pub state: RuntimeConstraintStateDto,
    pub semantic: Option<String>,
    pub basis: ObservationBasisDto,
}

/// Compact independent foreground observations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForegroundObservationDto {
    pub execution: ExecutionObservationDto,
    pub interaction: InteractionObservationDto,
    pub constraint: RuntimeConstraintObservationDto,
}

/// Health aggregate vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatusDto {
    Healthy,
    Unhealthy,
    Unknown,
    NoCheck,
}

/// Compact health state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthSummaryDto {
    pub status: HealthStatusDto,
    pub observed_at_millis: Option<i64>,
    pub configured_checks: u32,
}

/// Compact WorkContext aggregate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkSummaryDto {
    pub revision: u64,
    pub total_items: u32,
    pub active_items: u32,
    pub total_targets: u32,
    pub active_targets: u32,
    pub aggregate_status: Option<WorkStatusDto>,
}

/// Authorized detail result whose private sections remain projection-gated.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionDetailDto {
    /// Latest available exported-environment snapshot; values are never included.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shell_environment: Option<ShellEnvironmentStatusDto>,
    /// Last observed root working directory retained for group restoration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restoration_directory: Option<String>,
    pub summary: SessionSummaryDto,
    pub configuration: Option<SessionConfigurationDetailDto>,
    pub work_context: Option<WorkContextDetailDto>,
    pub process: Option<ProcessDetailDto>,
    pub foreground_evidence: Option<Vec<ForegroundEvidenceDto>>,
    pub health_results: Option<Vec<HealthResultDto>>,
    pub persisted: bool,
}

/// Payload-free metadata for an in-memory root-shell environment capture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShellEnvironmentStatusDto {
    pub process_id: String,
    pub observed_at_millis: i64,
}

/// Authorized durable configuration without environment values.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionConfigurationDetailDto {
    pub command: CommandSpecDto,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable_resolution: Option<ExecutableResolutionDto>,
    pub environment_variable_names: Vec<String>,
    pub environment_file: Option<String>,
    pub working_directory: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment_binding: Option<ExecutionEnvironmentBindingDto>,
    pub terminal: TerminalSizeDto,
    #[serde(default, skip_serializing_if = "OutputCaptureModeDto::is_terminal")]
    pub output_capture_mode: OutputCaptureModeDto,
    pub execution: ExecutionPolicyDto,
    pub health: HealthRulesDto,
    pub profile: Option<String>,
    pub profile_origin: Option<ProfileSelectionOriginDto>,
    pub profile_binding: ProfileBindingModeDto,
    pub labels: BTreeMap<String, String>,
    pub metadata: BTreeMap<String, String>,
}

/// Provenance of a durable base detection profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileSelectionOriginDto {
    Explicit,
    LaunchCommand,
}

/// Complete projected WorkContext.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkContextDetailDto {
    pub revision: u64,
    pub items: Vec<WorkItemDto>,
}

/// Current concrete process binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessDetailDto {
    pub instance_id: String,
    pub started_at_millis: i64,
}

/// Independent foreground axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForegroundAxisDto {
    Execution,
    Interaction,
    Constraint,
}

/// Profile-owned foreground claim without evidence provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "axis", rename_all = "snake_case")]
pub enum ForegroundClaimDto {
    Execution {
        state: ExecutionStateDto,
    },
    Interaction {
        state: InteractionStateDto,
        modality: Option<InteractionModalityDto>,
        semantic: Option<String>,
    },
    Constraint {
        state: RuntimeConstraintStateDto,
        semantic: Option<String>,
    },
}

/// One safe foreground-evidence assessment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForegroundEvidenceDto {
    pub source: String,
    pub priority: String,
    pub disposition: EvidenceDispositionDto,
    pub axis: ForegroundAxisDto,
    pub claim: Option<ForegroundClaimDto>,
    pub confidence_basis_points: u16,
    pub observed_at_millis: i64,
    pub assessment: String,
}

/// One health-check result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthResultDto {
    pub rule_index: u16,
    pub passed: bool,
    pub latency_millis: u64,
    pub safe_detail: Option<String>,
}

/// One bounded retained-output result.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum TerminalOutputPageDto {
    /// One bounded byte interval; source end can extend beyond this page.
    Data {
        /// Inclusive start of the returned interval.
        from_offset: u64,
        /// Exclusive delivered end; use this as the next finite read offset.
        to_offset: u64,
        /// Exclusive source end at extraction time, not necessarily delivered.
        next_offset: u64,
        /// Whether retained bytes remain between `to_offset` and `next_offset`.
        has_more: bool,
        bytes: Vec<u8>,
        /// Mandatory SHA-256 digest of exactly `bytes`.
        payload_digest_sha256: String,
        /// Original/delivered size evidence when bounded output is truncated.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        truncation: Option<crate::OutputTruncationDto>,
    },
    Gap {
        requested_offset: u64,
        available_from: u64,
        next_offset: u64,
        recovery: ResynchronizationDto,
    },
}

/// Complete current terminal state and its exact raw-output checkpoint.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalScreenSnapshotDto {
    /// Opaque client-scoped baseline for a subsequent diff request.
    pub checkpoint: String,
    pub output_offset: u64,
    pub columns: u16,
    pub rows: u16,
    pub cursor_row: u16,
    pub cursor_column: u16,
    pub cursor_visible: bool,
    pub alternate_screen: bool,
    /// Complete ANSI screen rendering.
    pub contents: Vec<u8>,
}

impl Debug for TerminalScreenSnapshotDto {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TerminalScreenSnapshotDto")
            .field("checkpoint", &self.checkpoint)
            .field("output_offset", &self.output_offset)
            .field("columns", &self.columns)
            .field("rows", &self.rows)
            .field("cursor_row", &self.cursor_row)
            .field("cursor_column", &self.cursor_column)
            .field("cursor_visible", &self.cursor_visible)
            .field("alternate_screen", &self.alternate_screen)
            .field("content_bytes", &self.contents.len())
            .finish_non_exhaustive()
    }
}

/// Bounded update from one issued screen checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", content = "screen", rename_all = "snake_case")]
pub enum TerminalScreenUpdateDto {
    /// Minimal ANSI patch and the checkpoint it reaches.
    Diff(TerminalScreenDiffDto),
    /// Authoritative replacement after incompatible screen state.
    Snapshot(TerminalScreenSnapshotDto),
}

/// Minimal ANSI patch between compatible terminal screen checkpoints.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalScreenDiffDto {
    /// Opaque client-scoped baseline for a subsequent diff request.
    pub checkpoint: String,
    pub from_output_offset: u64,
    pub to_output_offset: u64,
    pub columns: u16,
    pub rows: u16,
    pub cursor_row: u16,
    pub cursor_column: u16,
    pub cursor_visible: bool,
    pub alternate_screen: bool,
    /// Private ANSI patch transforming the prior screen into this state.
    pub contents: Vec<u8>,
}

impl Debug for TerminalScreenDiffDto {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TerminalScreenDiffDto")
            .field("checkpoint", &self.checkpoint)
            .field("from_output_offset", &self.from_output_offset)
            .field("to_output_offset", &self.to_output_offset)
            .field("columns", &self.columns)
            .field("rows", &self.rows)
            .field("cursor_row", &self.cursor_row)
            .field("cursor_column", &self.cursor_column)
            .field("cursor_visible", &self.cursor_visible)
            .field("alternate_screen", &self.alternate_screen)
            .field("content_bytes", &self.contents.len())
            .finish_non_exhaustive()
    }
}

/// Bounded durable event page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionEventPageDto {
    pub items: Vec<SessionEventDto>,
    pub checkpoint: Option<u64>,
    pub has_more: bool,
    pub gap: Option<EventGapDto>,
}

/// Safe revisioned durable session event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionEventDto {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation: Option<SessionEventObservationDto>,
    pub sequence: u64,
    pub session_id: String,
    pub session_revision: u64,
    pub kind: SessionEventKindDto,
    pub occurred_at_millis: i64,
}

/// Event-time state, never reconstructed from a later session revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionEventObservationDto {
    /// Event-time restart disposition, including explicit stop intent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_status: Option<RestartStatusDto>,
    /// Last observed group-terminal directory retained even after cleanup.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restoration_directory: Option<String>,
    pub version: crate::ReleaseVersion,
    pub lifecycle: LifecycleStateDto,
    pub lifecycle_revision: u64,
    pub foreground: ForegroundObservationDto,
    pub health: HealthSummaryDto,
}

/// Explicit missing event interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventGapDto {
    pub missing_from: u64,
    pub missing_through: u64,
    pub first_available: u64,
}

/// Bounded profile page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfilePageDto {
    pub items: Vec<ProfileSummaryDto>,
    pub next_cursor: Option<String>,
}

/// Compact profile summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileSummaryDto {
    pub name: String,
    pub source: String,
    pub description: Option<String>,
    pub disabled: bool,
}

/// Default encoding for profile-selected text input.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileTextModeDto {
    #[default]
    Raw,
    Paste,
}

/// Inspectable profile-owned terminal input delivery behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProfileInputDeliveryDto {
    /// Deliver the complete typed sequence without an intentional pause.
    Immediate {
        #[serde(default)]
        text_mode: ProfileTextModeDto,
    },
    /// Flush the body, pause, then deliver one trailing Enter key.
    PacedTrailingSubmit {
        delay_millis: u64,
        #[serde(default)]
        text_mode: ProfileTextModeDto,
    },
}

/// Inspectable profile contract without compiled adapter state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileDetailDto {
    #[serde(default)]
    pub matching_fallback: bool,
    pub summary: ProfileSummaryDto,
    pub schema_version: crate::ReleaseVersion,
    pub archetype: String,
    pub input_delivery: ProfileInputDeliveryDto,
    pub binary_patterns: Vec<String>,
    pub argument_patterns: Vec<String>,
    pub required_environment: Vec<String>,
    pub foreground_rule_count: u32,
    pub background_rule_count: u32,
}

/// Deterministic profile match result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileMatchDto {
    pub selected: Option<String>,
    pub candidates: Vec<String>,
}

/// Profile validation diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileLintDto {
    pub valid: bool,
    pub diagnostics: Vec<ProfileDiagnosticDto>,
}

/// Safe profile diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileDiagnosticDto {
    pub profile: Option<String>,
    pub severity: String,
    pub field: Option<String>,
    pub message: String,
}

/// Explained profile sample evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileTestDto {
    pub profile: String,
    pub evaluations: Vec<ProfileTestEvaluationDto>,
    pub selected_foreground: Vec<ProfileTestSelectionDto>,
}

/// One profile rule evaluation without retaining terminal sample text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileTestEvaluationDto {
    pub rule: u32,
    pub kind: ProfileTestRuleKindDto,
    pub sample: Option<u32>,
    pub disposition: ProfileTestDispositionDto,
    pub matched_patterns: Vec<u32>,
    pub excluded_by: Option<u32>,
    pub matched_name_patterns: Vec<u32>,
}

/// Profile rule category in a standalone test report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProfileTestRuleKindDto {
    Foreground { axis: ForegroundAxisDto },
    Background { work_kind: String },
}

/// Typed result of evaluating one profile rule against one sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileTestDispositionDto {
    Matched,
    NotMatched,
    Excluded,
    TraitsRejected,
    SampleUnavailable,
}

/// Winning foreground classification from a standalone profile test.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileTestSelectionDto {
    pub rule: u32,
    pub sample: u32,
    pub claim: ForegroundClaimDto,
    pub confidence_basis_points: u16,
}

/// One bounded payload-free diagnostic page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticPageDto {
    pub items: Vec<DiagnosticRowDto>,
    pub next_cursor: Option<String>,
    pub captured_at_millis: i64,
}

/// One bounded privacy-safe diagnostic bundle.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticBundleDto {
    /// Safe diagnostic page and optional continuation cursor.
    pub diagnostics: DiagnosticPageDto,
    /// Explicitly authorized terminal content, absent by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_content: Option<DiagnosticBundleTerminalContentDto>,
    /// True when terminal content was intentionally omitted by safe policy.
    pub terminal_content_redacted: bool,
}

impl Debug for DiagnosticBundleDto {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DiagnosticBundleDto")
            .field("diagnostics", &self.diagnostics)
            .field(
                "terminal_content_included",
                &self.terminal_content.is_some(),
            )
            .field("terminal_content_redacted", &self.terminal_content_redacted)
            .finish()
    }
}

/// Session identity and bounded terminal page included by explicit authorization.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticBundleTerminalContentDto {
    pub session_id: String,
    pub page: TerminalOutputPageDto,
}

/// Stable safe diagnostic key/value row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticRowDto {
    pub section: DiagnosticSectionDto,
    pub identity: String,
    pub fields: BTreeMap<String, DiagnosticValueDto>,
}

/// Bounded, non-secret diagnostic scalar.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DiagnosticValueDto {
    Boolean(bool),
    Unsigned(u64),
    Signed(i64),
    Text(String),
}

/// Current daemon identity and bounded aggregate pressure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonStatusDto {
    pub server_epoch: String,
    pub started_at_millis: i64,
    pub accepting_connections: bool,
    pub physical_connections: u32,
    pub logical_streams: u32,
    pub managed_sessions: u32,
}

/// Acknowledgement that graceful shutdown has begun.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShutdownAcceptedDto {
    pub drain_deadline_millis: i64,
}

/// Successful logical-stream activation response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProductStreamOpened {
    pub kind: ProductStreamKind,
    pub initial_sequence: u64,
    pub resumable: bool,
    pub available_from: Option<u64>,
}

/// Logical-stream activation response with stream-specific error context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", content = "value", rename_all = "snake_case")]
pub enum ProductStreamOpenResponse {
    /// The logical stream was activated and may begin emitting items.
    Opened(ProductStreamOpened),
    /// Product validation or authorization rejected activation.
    Error(ProductStreamError),
}

/// Safe stream-open error that does not pretend to be a finite operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProductStreamError {
    /// Requested stream kind, absent only when specification decoding failed.
    pub kind: Option<ProductStreamKind>,
    /// Stable presentation category.
    pub category: ProductErrorCategory,
    /// Exact high-level disposition when its optional connection capability was negotiated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disposition: Option<ProductErrorDisposition>,
    /// Safe actionable explanation without request payload content.
    pub message: String,
    /// Stable target session identity when the subscription selected one.
    pub session_id: Option<String>,
    /// Whether reopening may succeed without changing the specification.
    pub retryable: bool,
    /// Optional server-provided retry delay.
    pub retry_after_millis: Option<u64>,
    /// Optional relevant configured hard limit.
    pub limit: Option<u64>,
    /// Required resynchronization when prior resume state is unavailable.
    pub resynchronization: Option<ResynchronizationDto>,
}

/// Non-terminal JSON product stream items.
// Preserve the public by-value SessionEvent constructor used by stream adapters.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "item", content = "value", rename_all = "snake_case")]
pub enum ProductStreamItem {
    SessionSnapshot(SessionPageDto),
    /// One authoritative coalesced watched-page transition.
    SessionDeltaBatch(SessionDeltaBatchDto),
    SessionEvent(SessionEventDto),
    /// One durable unsettled agent tool call.
    AgentEvent(Box<AgentEventDto>),
}

/// One complete coalesced transition for a bounded watched query page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionDeltaBatchDto {
    pub from_revision: u64,
    pub to_revision: u64,
    pub deltas: Vec<SessionDeltaDto>,
    /// Authoritative session-ID order after applying this transition.
    pub order: Vec<String>,
    /// Exact filtered total when requested by the watch.
    pub total: Option<u64>,
}

/// Query membership or projected-content delta.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionDeltaDto {
    pub kind: SessionDeltaKindDto,
    pub session_id: String,
    pub session_revision: u64,
    pub catalog_revision: u64,
    pub summary: Option<SessionSummaryDto>,
}

/// Query delta vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionDeltaKindDto {
    Added,
    Updated,
    Removed,
}

const fn default_true() -> bool {
    true
}
