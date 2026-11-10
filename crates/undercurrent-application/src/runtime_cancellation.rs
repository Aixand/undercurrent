// SPDX-License-Identifier: Apache-2.0

//! Authorization-first durable control of one exact attached runtime attempt.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;

use undercurrent_domain::{
    DispatchCellIdentity, FencingToken, LifecycleState, OperationAttemptId, PayloadDigest,
    Revision, Session, SessionId, TimestampMillis,
};

use crate::{
    ApplicationErrorCategory, Clock, PortError, PortFuture, ProcessRuntime, RestartScheduler,
    SessionMutationCoordinator, SessionMutationLease, SessionProcessDirectory,
    SessionProjectionWriter, SessionRepository, StopMode, StopSessionCommand, StopSessionError,
    StopSessionResult, StopSessionService, Telemetry,
};

const MAX_CONTEXT_REFERENCE_BYTES: usize = 512;
const MAX_PRODUCT_REFERENCE_BYTES: usize = 128;
const MAX_TENANT_REFERENCE_BYTES: usize = 128;
const MAX_PROVIDER_REFERENCE_BYTES: usize = 128;
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 256;
const MAX_REASON_BYTES: usize = 512;

/// Exact process-control behavior for an attached cancellation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeCancellationMode {
    /// Ask the target process to stop cooperatively.
    ///
    /// Persists no-escalation intent before process IO. Timeout does not grant
    /// force authority; a separate Force command must be authorized.
    /// See [ADR 0003](../../../docs/architecture/decisions/0003-runtime-cancellation-graceful-authority.md).
    Graceful,
    /// Apply an independently authorized force-stop command.
    Force,
}

/// Durable startup-recovery instruction derived from retained cancellation state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeCancellationRecovery {
    /// Resume a control atomically marked in flight immediately before process IO.
    Resume(RuntimeCancellationMode),
    /// Preserve an Unknown effect without automatically redriving process control.
    PreserveUnknown,
}

/// Runtime cancellation never expands beyond the exact target attempt/session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeCancellationPropagationPolicy {
    /// Control only the exact predecessor attempt and its exact session.
    TargetOnly,
}

/// Authenticated and asserted context supplied to an attempt/control-scoped decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeCancellationActor {
    principal: String,
    authenticated_tenant: Option<String>,
    transport_authority: String,
    source_product: String,
    tenant_id: String,
    actor_context_ref: String,
    authority_ref: String,
}

impl RuntimeCancellationActor {
    /// Constructs bounded credential-free references from an authenticated request.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        principal: String,
        authenticated_tenant: Option<String>,
        transport_authority: String,
        source_product: String,
        tenant_id: String,
        actor_context_ref: String,
        authority_ref: String,
    ) -> Result<Self, RuntimeCancellationValidationError> {
        validate_reference("principal", &principal, MAX_CONTEXT_REFERENCE_BYTES)?;
        if let Some(tenant) = authenticated_tenant.as_deref() {
            validate_reference("authenticated_tenant", tenant, MAX_TENANT_REFERENCE_BYTES)?;
        }
        validate_reference(
            "transport_authority",
            &transport_authority,
            MAX_CONTEXT_REFERENCE_BYTES,
        )?;
        validate_product_coordinate(
            "source_product",
            &source_product,
            MAX_PRODUCT_REFERENCE_BYTES,
        )?;
        validate_reference("tenant_id", &tenant_id, MAX_TENANT_REFERENCE_BYTES)?;
        validate_bounded_plain(
            "actor_context_ref",
            &actor_context_ref,
            MAX_CONTEXT_REFERENCE_BYTES,
        )?;
        validate_bounded_plain("authority_ref", &authority_ref, MAX_CONTEXT_REFERENCE_BYTES)?;
        Ok(Self {
            principal,
            authenticated_tenant,
            transport_authority,
            source_product,
            tenant_id,
            actor_context_ref,
            authority_ref,
        })
    }

    /// Returns the authenticated transport principal.
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// Returns the optional tenant established by transport authentication.
    pub fn authenticated_tenant(&self) -> Option<&str> {
        self.authenticated_tenant.as_deref()
    }

    /// Returns the transport authority reference.
    pub fn transport_authority(&self) -> &str {
        &self.transport_authority
    }

    /// Returns the asserted source product carried by the provider request.
    pub fn source_product(&self) -> &str {
        &self.source_product
    }

    /// Returns the exact asserted tenant scope.
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Returns the asserted actor-context reference.
    pub fn actor_context_ref(&self) -> &str {
        &self.actor_context_ref
    }

    /// Returns the current control/status authority reference.
    pub fn authority_ref(&self) -> &str {
        &self.authority_ref
    }
}

/// Exact immutable predecessor binding validated at atomic admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeCancellationTarget {
    attempt_id: OperationAttemptId,
    request_digest: PayloadDigest,
    fencing_token: FencingToken,
    caller_cell_identity: DispatchCellIdentity,
    session_id: SessionId,
    expected_session_revision: Revision,
    expected_lifecycle_revision: Revision,
    expected_provider_id: String,
    expected_provider_generation: u64,
}

impl RuntimeCancellationTarget {
    /// Constructs the complete target after DTO/value validation.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        attempt_id: OperationAttemptId,
        request_digest: PayloadDigest,
        fencing_token: FencingToken,
        caller_cell_identity: DispatchCellIdentity,
        session_id: SessionId,
        expected_session_revision: Revision,
        expected_lifecycle_revision: Revision,
        expected_provider_id: String,
        expected_provider_generation: u64,
    ) -> Result<Self, RuntimeCancellationValidationError> {
        if expected_session_revision == Revision::INITIAL
            || expected_lifecycle_revision == Revision::INITIAL
            || expected_lifecycle_revision.value() > expected_session_revision.value()
        {
            return Err(RuntimeCancellationValidationError {
                field: "expected_lifecycle_revision",
            });
        }
        validate_reference(
            "expected_provider_id",
            &expected_provider_id,
            MAX_PROVIDER_REFERENCE_BYTES,
        )?;
        if expected_provider_generation == 0 {
            return Err(RuntimeCancellationValidationError {
                field: "expected_provider_generation",
            });
        }
        Ok(Self {
            attempt_id,
            request_digest,
            fencing_token,
            caller_cell_identity,
            session_id,
            expected_session_revision,
            expected_lifecycle_revision,
            expected_provider_id,
            expected_provider_generation,
        })
    }

    /// Returns the exact predecessor attempt identity.
    pub const fn attempt_id(&self) -> &OperationAttemptId {
        &self.attempt_id
    }

    /// Returns the original caller runtime-start request digest.
    pub const fn request_digest(&self) -> PayloadDigest {
        self.request_digest
    }

    /// Returns the exact predecessor dispatch fence.
    pub const fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }

    /// Returns the caller cell whose fence owns the predecessor.
    pub const fn caller_cell_identity(&self) -> &DispatchCellIdentity {
        &self.caller_cell_identity
    }

    /// Returns the exact target session.
    pub const fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    /// Returns the exact expected aggregate revision.
    pub const fn expected_session_revision(&self) -> Revision {
        self.expected_session_revision
    }

    /// Returns the exact expected lifecycle revision.
    pub const fn expected_lifecycle_revision(&self) -> Revision {
        self.expected_lifecycle_revision
    }

    /// Returns the exact expected provider identity.
    pub fn expected_provider_id(&self) -> &str {
        &self.expected_provider_id
    }

    /// Returns the exact expected provider generation.
    pub const fn expected_provider_generation(&self) -> u64 {
        self.expected_provider_generation
    }
}

/// Complete retained identity and caller/envelope binding for one control command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeCancellationControl {
    control_id: OperationAttemptId,
    command_digest: PayloadDigest,
    product_request_fingerprint: PayloadDigest,
    idempotency_key: String,
    mode: RuntimeCancellationMode,
    reason: String,
    propagation_policy: RuntimeCancellationPropagationPolicy,
    deadline: TimestampMillis,
    source_product: String,
    tenant_id: String,
    actor_context_ref: String,
    authority_ref: String,
}

impl RuntimeCancellationControl {
    /// Constructs a complete retained control binding.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        control_id: OperationAttemptId,
        command_digest: PayloadDigest,
        product_request_fingerprint: PayloadDigest,
        idempotency_key: String,
        mode: RuntimeCancellationMode,
        reason: String,
        propagation_policy: RuntimeCancellationPropagationPolicy,
        deadline: TimestampMillis,
        source_product: String,
        tenant_id: String,
        actor_context_ref: String,
        authority_ref: String,
    ) -> Result<Self, RuntimeCancellationValidationError> {
        validate_idempotency_key(
            "idempotency_key",
            &idempotency_key,
            MAX_IDEMPOTENCY_KEY_BYTES,
        )?;
        validate_bounded_plain("reason", &reason, MAX_REASON_BYTES)?;
        if deadline.value() <= 0 {
            return Err(RuntimeCancellationValidationError { field: "deadline" });
        }
        validate_product_coordinate(
            "source_product",
            &source_product,
            MAX_PRODUCT_REFERENCE_BYTES,
        )?;
        validate_reference("tenant_id", &tenant_id, MAX_TENANT_REFERENCE_BYTES)?;
        validate_bounded_plain(
            "actor_context_ref",
            &actor_context_ref,
            MAX_CONTEXT_REFERENCE_BYTES,
        )?;
        validate_bounded_plain("authority_ref", &authority_ref, MAX_CONTEXT_REFERENCE_BYTES)?;
        Ok(Self {
            control_id,
            command_digest,
            product_request_fingerprint,
            idempotency_key,
            mode,
            reason,
            propagation_policy,
            deadline,
            source_product,
            tenant_id,
            actor_context_ref,
            authority_ref,
        })
    }

    /// Returns the distinct cancellation-control identity.
    pub const fn control_id(&self) -> &OperationAttemptId {
        &self.control_id
    }

    /// Returns the caller's semantic command digest.
    pub const fn command_digest(&self) -> PayloadDigest {
        self.command_digest
    }

    /// Returns the complete internal product-request fingerprint.
    pub const fn product_request_fingerprint(&self) -> PayloadDigest {
        self.product_request_fingerprint
    }

    /// Returns the exact enclosing idempotency key.
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    /// Returns the independently authorized process-control mode.
    pub const fn mode(&self) -> RuntimeCancellationMode {
        self.mode
    }

    /// Returns the bounded retained reason.
    pub fn reason(&self) -> &str {
        &self.reason
    }

    /// Returns the exact target-only propagation policy.
    pub const fn propagation_policy(&self) -> RuntimeCancellationPropagationPolicy {
        self.propagation_policy
    }

    /// Returns the original absolute command-admission deadline.
    pub const fn deadline(&self) -> TimestampMillis {
        self.deadline
    }

    /// Returns the original authenticated source product.
    pub fn source_product(&self) -> &str {
        &self.source_product
    }

    /// Returns the original exact tenant scope.
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Returns the original actor-context reference.
    pub fn actor_context_ref(&self) -> &str {
        &self.actor_context_ref
    }

    /// Returns the original control-authority reference.
    pub fn authority_ref(&self) -> &str {
        &self.authority_ref
    }
}

/// Validated exact cancellation use-case input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CancelRuntimeAttemptCommand {
    target: RuntimeCancellationTarget,
    control: RuntimeCancellationControl,
    actor: RuntimeCancellationActor,
}

impl CancelRuntimeAttemptCommand {
    /// Constructs a command and rejects control/target identity aliasing or context drift.
    pub fn new(
        target: RuntimeCancellationTarget,
        control: RuntimeCancellationControl,
        actor: RuntimeCancellationActor,
    ) -> Result<Self, RuntimeCancellationValidationError> {
        if target.attempt_id == control.control_id {
            return Err(RuntimeCancellationValidationError {
                field: "control_id",
            });
        }
        if control.source_product != actor.source_product
            || control.tenant_id != actor.tenant_id
            || control.actor_context_ref != actor.actor_context_ref
            || control.authority_ref != actor.authority_ref
        {
            return Err(RuntimeCancellationValidationError {
                field: "actor_context",
            });
        }
        Ok(Self {
            target,
            control,
            actor,
        })
    }

    /// Returns the exact predecessor/session/provider target.
    pub const fn target(&self) -> &RuntimeCancellationTarget {
        &self.target
    }

    /// Returns the complete retained control binding.
    pub const fn control(&self) -> &RuntimeCancellationControl {
        &self.control
    }

    /// Returns credential-free authenticated authorization context.
    pub const fn actor(&self) -> &RuntimeCancellationActor {
        &self.actor
    }
}

/// Exact durable session snapshot retained on a cancellation record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeCancellationSessionSnapshot {
    session_id: SessionId,
    lifecycle: LifecycleState,
    revision: Revision,
    lifecycle_revision: Revision,
    observed_at: TimestampMillis,
}

impl RuntimeCancellationSessionSnapshot {
    /// Constructs a snapshot from validated durable values.
    pub fn new(
        session_id: SessionId,
        lifecycle: LifecycleState,
        revision: Revision,
        lifecycle_revision: Revision,
        observed_at: TimestampMillis,
    ) -> Result<Self, RuntimeCancellationValidationError> {
        let snapshot = Self {
            session_id,
            lifecycle,
            revision,
            lifecycle_revision,
            observed_at,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    /// Projects one authoritative aggregate without its private payload.
    pub fn from_session(session: &Session, observed_at: TimestampMillis) -> Self {
        Self {
            session_id: session.id().clone(),
            lifecycle: session.lifecycle(),
            revision: session.revision(),
            lifecycle_revision: session.lifecycle_revision(),
            observed_at,
        }
    }

    /// Returns the exact session identity.
    pub const fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    /// Returns the authoritative durable lifecycle.
    pub const fn lifecycle(&self) -> LifecycleState {
        self.lifecycle
    }

    /// Returns the exact aggregate revision.
    pub const fn revision(&self) -> Revision {
        self.revision
    }

    /// Returns the exact lifecycle revision.
    pub const fn lifecycle_revision(&self) -> Revision {
        self.lifecycle_revision
    }

    /// Returns the durable observation time.
    pub const fn observed_at(&self) -> TimestampMillis {
        self.observed_at
    }

    /// Rejects incoherent durable revision snapshots during adapter decoding.
    pub fn validate(&self) -> Result<(), RuntimeCancellationValidationError> {
        if self.revision == Revision::INITIAL
            || self.lifecycle_revision == Revision::INITIAL
            || self.lifecycle_revision > self.revision
        {
            return Err(RuntimeCancellationValidationError {
                field: "session_snapshot_revision",
            });
        }
        Ok(())
    }
}

/// Provider-owned durable state for one exact cancellation command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeCancellationState {
    /// Control and stopping lifecycle are durable; process outcome is pending.
    AcceptedStopping(RuntimeCancellationSessionSnapshot),
    /// Process termination and terminal session state are durable.
    AppliedTerminated(RuntimeCancellationSessionSnapshot),
    /// The exact target session was terminal at admission.
    AlreadyTerminal(RuntimeCancellationSessionSnapshot),
    /// Control is durable but retained evidence cannot prove its process effect.
    Unknown(RuntimeCancellationSessionSnapshot),
}

impl RuntimeCancellationState {
    /// Returns the exact durable session snapshot carried by every state.
    pub const fn session(&self) -> &RuntimeCancellationSessionSnapshot {
        match self {
            Self::AcceptedStopping(session)
            | Self::AppliedTerminated(session)
            | Self::AlreadyTerminal(session)
            | Self::Unknown(session) => session,
        }
    }

    /// Enforces the lifecycle discriminator carried by the durable state.
    pub fn validate(&self) -> Result<(), RuntimeCancellationValidationError> {
        self.session().validate()?;
        let coherent = match self {
            Self::AcceptedStopping(session) => session.lifecycle() == LifecycleState::Stopping,
            Self::AppliedTerminated(session) | Self::AlreadyTerminal(session) => {
                session.lifecycle().is_terminal()
            }
            Self::Unknown(_) => true,
        };
        if !coherent {
            return Err(RuntimeCancellationValidationError {
                field: "runtime_cancellation_state",
            });
        }
        Ok(())
    }
}

/// Complete retained target/control/state result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeCancellationResult {
    target: RuntimeCancellationTarget,
    control: RuntimeCancellationControl,
    state: RuntimeCancellationState,
}

impl RuntimeCancellationResult {
    /// Constructs a complete result from coherent durable values.
    pub fn new(
        target: RuntimeCancellationTarget,
        control: RuntimeCancellationControl,
        state: RuntimeCancellationState,
    ) -> Result<Self, RuntimeCancellationValidationError> {
        let result = Self {
            target,
            control,
            state,
        };
        result.validate()?;
        Ok(result)
    }

    /// Revalidates all retained target, state, and revision bindings.
    pub fn validate(&self) -> Result<(), RuntimeCancellationValidationError> {
        self.state.validate()?;
        let snapshot = self.state.session();
        if self.target.attempt_id == self.control.control_id
            || self.target.session_id != *snapshot.session_id()
            || snapshot.revision() < self.target.expected_session_revision
            || snapshot.lifecycle_revision() < self.target.expected_lifecycle_revision
        {
            return Err(RuntimeCancellationValidationError {
                field: "runtime_cancellation_result_binding",
            });
        }
        Ok(())
    }

    /// Returns the exact predecessor/session/provider target.
    pub const fn target(&self) -> &RuntimeCancellationTarget {
        &self.target
    }

    /// Returns the complete retained control binding.
    pub const fn control(&self) -> &RuntimeCancellationControl {
        &self.control
    }

    /// Returns the discriminated durable control state.
    pub const fn state(&self) -> &RuntimeCancellationState {
        &self.state
    }
}

/// Atomic durable admission classification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeCancellationAdmission {
    /// A new control and initial state were committed before process IO.
    Admitted(Box<RuntimeCancellationResult>),
    /// The exact complete command already exists. An `AcceptedStopping` replay
    /// may continue through the effect-start marker; all other retained states
    /// return without repeating process IO.
    Replay(Box<RuntimeCancellationResult>),
    /// Control identity or scoped idempotency was reused for different semantics.
    IdentityConflict,
    /// No retained replay exists and the admission deadline has elapsed.
    DeadlineExpired,
    /// The exact predecessor/session/fence/revision binding is known absent.
    TargetMismatch,
}

/// Durable process-effect-boundary marker classification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeCancellationEffectStart {
    /// The exact accepted control was atomically marked before process IO.
    Started,
    /// The exact control already crossed or settled beyond the effect boundary.
    Replay(Box<RuntimeCancellationResult>),
    /// The admitted control disappeared unexpectedly.
    NotFound,
    /// The control identity or command digest conflicts with durable state.
    Conflict,
}

/// Durable post-effect settlement classification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeCancellationSettlement {
    /// A new durable state was committed.
    Settled(Box<RuntimeCancellationResult>),
    /// The exact state was already durable.
    Duplicate(Box<RuntimeCancellationResult>),
    /// The admitted control disappeared unexpectedly.
    NotFound,
    /// The proposed transition or control identity conflicts with durable state.
    Conflict,
}

/// Exact read-only cancellation-status lookup input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetRuntimeCancellationStatusQuery {
    control_id: OperationAttemptId,
    command_digest: PayloadDigest,
    target_attempt_id: OperationAttemptId,
    target_request_digest: PayloadDigest,
    session_id: SessionId,
    expected_provider_id: String,
    expected_provider_generation: u64,
    actor: RuntimeCancellationActor,
}

impl GetRuntimeCancellationStatusQuery {
    /// Constructs an exact lookup after boundary validation.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        control_id: OperationAttemptId,
        command_digest: PayloadDigest,
        target_attempt_id: OperationAttemptId,
        target_request_digest: PayloadDigest,
        session_id: SessionId,
        expected_provider_id: String,
        expected_provider_generation: u64,
        actor: RuntimeCancellationActor,
    ) -> Result<Self, RuntimeCancellationValidationError> {
        if control_id == target_attempt_id || expected_provider_generation == 0 {
            return Err(RuntimeCancellationValidationError {
                field: "status_target",
            });
        }
        validate_reference(
            "expected_provider_id",
            &expected_provider_id,
            MAX_PROVIDER_REFERENCE_BYTES,
        )?;
        Ok(Self {
            control_id,
            command_digest,
            target_attempt_id,
            target_request_digest,
            session_id,
            expected_provider_id,
            expected_provider_generation,
            actor,
        })
    }

    pub const fn control_id(&self) -> &OperationAttemptId {
        &self.control_id
    }
    pub const fn command_digest(&self) -> PayloadDigest {
        self.command_digest
    }
    pub const fn target_attempt_id(&self) -> &OperationAttemptId {
        &self.target_attempt_id
    }
    pub const fn target_request_digest(&self) -> PayloadDigest {
        self.target_request_digest
    }
    pub const fn session_id(&self) -> &SessionId {
        &self.session_id
    }
    pub fn expected_provider_id(&self) -> &str {
        &self.expected_provider_id
    }
    pub const fn expected_provider_generation(&self) -> u64 {
        self.expected_provider_generation
    }
    pub const fn actor(&self) -> &RuntimeCancellationActor {
        &self.actor
    }
}

/// Authorization decision made before cancellation existence or store access.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeCancellationAuthorization {
    /// The exact attempt/control scope may be used.
    Authorized,
    /// The exact attempt/control scope is denied without existence disclosure.
    Denied,
}

/// Authorizes exact cancellation commands and status reads before state access.
pub trait RuntimeCancellationAuthorizer: Send + Sync {
    /// Authorizes a mutating exact attempt/control scope.
    fn authorize_cancel<'a>(
        &'a self,
        actor: &'a RuntimeCancellationActor,
        control_id: &'a OperationAttemptId,
        target_attempt_id: &'a OperationAttemptId,
        mode: RuntimeCancellationMode,
    ) -> PortFuture<'a, Result<RuntimeCancellationAuthorization, PortError>>;

    /// Authorizes a read-only exact attempt/control scope.
    fn authorize_status<'a>(
        &'a self,
        actor: &'a RuntimeCancellationActor,
        control_id: &'a OperationAttemptId,
        target_attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<RuntimeCancellationAuthorization, PortError>>;
}

/// Product-owned durable cancellation-control boundary.
pub trait RuntimeCancellationRepository: Send + Sync {
    /// Atomically revalidates predecessor/session state and commits a new
    /// control plus initial receipt before process IO.
    fn admit_runtime_cancellation<'a>(
        &'a self,
        command: &'a CancelRuntimeAttemptCommand,
        observed_at: TimestampMillis,
    ) -> PortFuture<'a, Result<RuntimeCancellationAdmission, PortError>>;

    /// Atomically records that the exact admitted command may begin process IO.
    /// Once this marker is durable, unresolved replay and restart project
    /// Unknown and must never redrive the process effect.
    fn mark_runtime_cancellation_effect_started<'a>(
        &'a self,
        control_id: &'a OperationAttemptId,
        command_digest: PayloadDigest,
        observed_at: TimestampMillis,
    ) -> PortFuture<'a, Result<RuntimeCancellationEffectStart, PortError>>;

    /// Commits a post-effect durable state without holding a process transaction.
    fn settle_runtime_cancellation<'a>(
        &'a self,
        control_id: &'a OperationAttemptId,
        command_digest: PayloadDigest,
        state: RuntimeCancellationState,
    ) -> PortFuture<'a, Result<RuntimeCancellationSettlement, PortError>>;

    /// Reads only retained control state and performs no process IO or writes.
    fn get_runtime_cancellation_status<'a>(
        &'a self,
        query: &'a GetRuntimeCancellationStatusQuery,
    ) -> PortFuture<'a, Result<RuntimeCancellationStatusLookup, PortError>>;
}

/// Read-only repository lookup classification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeCancellationStatusLookup {
    /// Exact retained state was found.
    Found(Box<RuntimeCancellationResult>),
    /// No control exists under the exact authorized identity.
    NotFound,
    /// A retained control exists but does not match all supplied pins.
    InconsistentBinding,
}

/// Orchestrates authorization, atomic admission, and one non-redriven stop effect.
pub struct CancelRuntimeAttemptService<'a> {
    authorizer: &'a dyn RuntimeCancellationAuthorizer,
    cancellation_repository: &'a dyn RuntimeCancellationRepository,
    session_repository: &'a dyn SessionRepository,
    coordinator: &'a dyn SessionMutationCoordinator,
    stop: StopSessionService<'a>,
    clock: &'a dyn Clock,
}

impl<'a> CancelRuntimeAttemptService<'a> {
    /// Constructs the service from explicit hexagonal ports without IO.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        authorizer: &'a dyn RuntimeCancellationAuthorizer,
        cancellation_repository: &'a dyn RuntimeCancellationRepository,
        session_repository: &'a dyn SessionRepository,
        coordinator: &'a dyn SessionMutationCoordinator,
        runtime: &'a dyn ProcessRuntime,
        processes: &'a dyn SessionProcessDirectory,
        restarts: &'a dyn RestartScheduler,
        projection: &'a dyn SessionProjectionWriter,
        telemetry: &'a dyn Telemetry,
        clock: &'a dyn Clock,
    ) -> Self {
        Self {
            authorizer,
            cancellation_repository,
            session_repository,
            coordinator,
            stop: StopSessionService::new(
                clock,
                coordinator,
                session_repository,
                runtime,
                processes,
                restarts,
                projection,
                telemetry,
            ),
            clock,
        }
    }

    /// Applies one exact cancellation at most once across retries and restarts.
    pub async fn execute(
        &self,
        command: CancelRuntimeAttemptCommand,
    ) -> Result<RuntimeCancellationResult, CancelRuntimeAttemptError> {
        let authorization = authorize_runtime_cancellation(self.authorizer, &command)
            .await
            .map_err(|source| CancelRuntimeAttemptError::Authorization(Box::new(source)))?;
        if authorization == RuntimeCancellationAuthorization::Denied {
            return Err(CancelRuntimeAttemptError::Denied);
        }

        let session_id = command.target().session_id().clone();
        let (lease, admission) = acquire_and_admit_runtime_cancellation(
            self.cancellation_repository,
            self.coordinator,
            self.clock,
            &command,
        )
        .await?;
        let admitted = match admission {
            RuntimeCancellationAdmission::Replay(result)
                if !matches!(
                    result.state(),
                    RuntimeCancellationState::AcceptedStopping(_)
                ) =>
            {
                drop(lease);
                return Ok(*result);
            }
            RuntimeCancellationAdmission::Replay(result) => *result,
            RuntimeCancellationAdmission::IdentityConflict => {
                drop(lease);
                return Err(CancelRuntimeAttemptError::IdentityConflict);
            }
            RuntimeCancellationAdmission::DeadlineExpired => {
                drop(lease);
                return Err(CancelRuntimeAttemptError::DeadlineExpired);
            }
            RuntimeCancellationAdmission::TargetMismatch => {
                drop(lease);
                return Err(CancelRuntimeAttemptError::TargetMismatch);
            }
            RuntimeCancellationAdmission::Admitted(result) => *result,
        };
        if !admitted_requires_process_io(&admitted) {
            drop(lease);
            return Ok(admitted);
        }

        let session = self
            .session_repository
            .load(&session_id)
            .await
            .map_err(|source| CancelRuntimeAttemptError::SessionRead(Box::new(source)))?
            .ok_or(CancelRuntimeAttemptError::TargetMismatch)?;
        let admitted_session = admitted.state().session();
        if session.id() != admitted_session.session_id()
            || session.lifecycle() != LifecycleState::Stopping
            || session.revision() < admitted_session.revision()
            || session.lifecycle_revision() < admitted_session.lifecycle_revision()
        {
            drop(lease);
            return Err(CancelRuntimeAttemptError::TargetMismatch);
        }

        let mode = match command.control().mode() {
            RuntimeCancellationMode::Graceful => StopMode::GracefulOnly,
            RuntimeCancellationMode::Force => StopMode::Force,
        };
        let marked_effect = execute_after_runtime_cancellation_effect_marker(
            self.cancellation_repository,
            self.clock,
            &command,
            self.stop.execute_with_owned_session(
                StopSessionCommand {
                    expected_revision: None,
                    session_id: session_id.clone(),
                    mode,
                },
                session,
            ),
        )
        .await?;
        let stop_result = match marked_effect {
            RuntimeCancellationMarkedEffect::Replay(result) => {
                drop(lease);
                return Ok(*result);
            }
            RuntimeCancellationMarkedEffect::Executed(result) => result,
        };

        let (state, stop_error) = match stop_result {
            Ok(StopSessionResult::AlreadyTerminal { session }) => (
                RuntimeCancellationState::AlreadyTerminal(
                    RuntimeCancellationSessionSnapshot::from_session(&session, self.clock.now()),
                ),
                None,
            ),
            Ok(StopSessionResult::Terminated { session, .. }) => (
                RuntimeCancellationState::AppliedTerminated(
                    RuntimeCancellationSessionSnapshot::from_session(&session, self.clock.now()),
                ),
                None,
            ),
            Err(StopSessionError::DirectoryCleanup { session, .. })
            | Err(StopSessionError::DirectoryChanged { session, .. }) => (
                RuntimeCancellationState::AppliedTerminated(
                    RuntimeCancellationSessionSnapshot::from_session(&session, self.clock.now()),
                ),
                None,
            ),
            Err(error) => {
                let snapshot = match self.session_repository.load(&session_id).await {
                    Ok(Some(session)) => {
                        RuntimeCancellationSessionSnapshot::from_session(&session, self.clock.now())
                    }
                    _ => admitted.state().session().clone(),
                };
                (RuntimeCancellationState::Unknown(snapshot), Some(error))
            }
        };
        let terminal = matches!(
            state,
            RuntimeCancellationState::AppliedTerminated(_)
                | RuntimeCancellationState::AlreadyTerminal(_)
        );
        drop(lease);
        if terminal {
            self.stop.cancel_queued_restarts(&session_id).await;
        }
        let settlement = self
            .cancellation_repository
            .settle_runtime_cancellation(
                command.control().control_id(),
                command.control().command_digest(),
                state,
            )
            .await
            .map_err(|source| CancelRuntimeAttemptError::Settlement(Box::new(source)))?;
        let result = match settlement {
            RuntimeCancellationSettlement::Settled(result)
            | RuntimeCancellationSettlement::Duplicate(result) => *result,
            RuntimeCancellationSettlement::NotFound | RuntimeCancellationSettlement::Conflict => {
                return Err(CancelRuntimeAttemptError::SettlementConflict);
            }
        };
        if let Some(source) = stop_error {
            if matches!(
                result.state(),
                RuntimeCancellationState::AppliedTerminated(_)
            ) {
                return Ok(result);
            }
            return Err(CancelRuntimeAttemptError::EffectUnknown(Box::new(source)));
        }
        Ok(result)
    }
}

fn admitted_requires_process_io(result: &RuntimeCancellationResult) -> bool {
    !matches!(result.state(), RuntimeCancellationState::AlreadyTerminal(_))
}

enum RuntimeCancellationMarkedEffect<T> {
    Replay(Box<RuntimeCancellationResult>),
    Executed(T),
}

async fn execute_after_runtime_cancellation_effect_marker<T>(
    repository: &dyn RuntimeCancellationRepository,
    clock: &dyn Clock,
    command: &CancelRuntimeAttemptCommand,
    effect: impl Future<Output = T>,
) -> Result<RuntimeCancellationMarkedEffect<T>, CancelRuntimeAttemptError> {
    let marker = repository
        .mark_runtime_cancellation_effect_started(
            command.control().control_id(),
            command.control().command_digest(),
            clock.now(),
        )
        .await
        .map_err(|source| CancelRuntimeAttemptError::EffectStart(Box::new(source)))?;
    match marker {
        RuntimeCancellationEffectStart::Started => {
            Ok(RuntimeCancellationMarkedEffect::Executed(effect.await))
        }
        RuntimeCancellationEffectStart::Replay(result) => {
            Ok(RuntimeCancellationMarkedEffect::Replay(result))
        }
        RuntimeCancellationEffectStart::NotFound | RuntimeCancellationEffectStart::Conflict => {
            Err(CancelRuntimeAttemptError::EffectStartConflict)
        }
    }
}

async fn authorize_runtime_cancellation(
    authorizer: &dyn RuntimeCancellationAuthorizer,
    command: &CancelRuntimeAttemptCommand,
) -> Result<RuntimeCancellationAuthorization, PortError> {
    authorizer
        .authorize_cancel(
            command.actor(),
            command.control().control_id(),
            command.target().attempt_id(),
            command.control().mode(),
        )
        .await
}

async fn acquire_and_admit_runtime_cancellation(
    repository: &dyn RuntimeCancellationRepository,
    coordinator: &dyn SessionMutationCoordinator,
    clock: &dyn Clock,
    command: &CancelRuntimeAttemptCommand,
) -> Result<(Box<dyn SessionMutationLease>, RuntimeCancellationAdmission), CancelRuntimeAttemptError>
{
    let lease = coordinator
        .acquire(command.target().session_id())
        .await
        .map_err(|source| CancelRuntimeAttemptError::Acquire(Box::new(source)))?;
    let admission_time = clock.now();
    let admission = repository
        .admit_runtime_cancellation(command, admission_time)
        .await
        .map_err(|source| CancelRuntimeAttemptError::Admission(Box::new(source)))?;
    if matches!(admission, RuntimeCancellationAdmission::DeadlineExpired) {
        drop(lease);
        return Err(CancelRuntimeAttemptError::DeadlineExpired);
    }
    Ok((lease, admission))
}

/// Authorization-first read-only cancellation-status service.
pub struct GetRuntimeCancellationStatusService<'a> {
    authorizer: &'a dyn RuntimeCancellationAuthorizer,
    repository: &'a dyn RuntimeCancellationRepository,
}

impl<'a> GetRuntimeCancellationStatusService<'a> {
    /// Constructs the stateless status service.
    pub const fn new(
        authorizer: &'a dyn RuntimeCancellationAuthorizer,
        repository: &'a dyn RuntimeCancellationRepository,
    ) -> Self {
        Self {
            authorizer,
            repository,
        }
    }

    /// Returns retained state without process IO, writes, or command redrive.
    pub async fn execute(
        &self,
        query: GetRuntimeCancellationStatusQuery,
    ) -> Result<RuntimeCancellationResult, GetRuntimeCancellationStatusError> {
        let authorization = self
            .authorizer
            .authorize_status(query.actor(), query.control_id(), query.target_attempt_id())
            .await
            .map_err(|source| GetRuntimeCancellationStatusError::Authorization(Box::new(source)))?;
        if authorization == RuntimeCancellationAuthorization::Denied {
            return Err(GetRuntimeCancellationStatusError::Denied);
        }
        match self
            .repository
            .get_runtime_cancellation_status(&query)
            .await
            .map_err(|source| GetRuntimeCancellationStatusError::Read(Box::new(source)))?
        {
            RuntimeCancellationStatusLookup::Found(result) => Ok(*result),
            RuntimeCancellationStatusLookup::NotFound => {
                Err(GetRuntimeCancellationStatusError::NotFound)
            }
            RuntimeCancellationStatusLookup::InconsistentBinding => {
                Err(GetRuntimeCancellationStatusError::InconsistentBinding)
            }
        }
    }
}

/// Stable application-value validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeCancellationValidationError {
    field: &'static str,
}

impl Display for RuntimeCancellationValidationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid runtime-cancellation {}", self.field)
    }
}

impl Error for RuntimeCancellationValidationError {}

/// Typed cancellation-command failures.
#[derive(Debug)]
pub enum CancelRuntimeAttemptError {
    Authorization(Box<PortError>),
    Denied,
    DeadlineExpired,
    Acquire(Box<PortError>),
    Admission(Box<PortError>),
    IdentityConflict,
    TargetMismatch,
    SessionRead(Box<PortError>),
    EffectStart(Box<PortError>),
    EffectStartConflict,
    EffectUnknown(Box<StopSessionError>),
    Settlement(Box<PortError>),
    SettlementConflict,
}

impl CancelRuntimeAttemptError {
    /// Returns the stable presentation category.
    pub fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::Authorization(source)
            | Self::Acquire(source)
            | Self::Admission(source)
            | Self::SessionRead(source)
            | Self::EffectStart(source)
            | Self::Settlement(source) => source.category(),
            Self::Denied => ApplicationErrorCategory::PermissionDenied,
            Self::DeadlineExpired
            | Self::IdentityConflict
            | Self::TargetMismatch
            | Self::EffectStartConflict
            | Self::SettlementConflict => ApplicationErrorCategory::FailedPrecondition,
            Self::EffectUnknown(_) => ApplicationErrorCategory::Interrupted,
        }
    }
}

impl Display for CancelRuntimeAttemptError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Authorization(_) => "runtime cancellation authorization failed",
            Self::Denied => "runtime cancellation denied",
            Self::DeadlineExpired => "runtime cancellation deadline expired before admission",
            Self::Acquire(_) => "runtime cancellation ordering failed",
            Self::Admission(_) => "runtime cancellation durable admission failed",
            Self::IdentityConflict => "runtime cancellation identity conflicts with retained state",
            Self::TargetMismatch => "runtime cancellation target binding is stale or mismatched",
            Self::SessionRead(_) => "runtime cancellation session read failed",
            Self::EffectStart(_) => "runtime cancellation effect marker failed",
            Self::EffectStartConflict => {
                "runtime cancellation effect marker conflicts with durable state"
            }
            Self::EffectUnknown(_) => "runtime cancellation process effect remains unknown",
            Self::Settlement(_) => "runtime cancellation durable settlement failed",
            Self::SettlementConflict => {
                "runtime cancellation settlement conflicts with retained state"
            }
        })
    }
}

impl Error for CancelRuntimeAttemptError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Authorization(source)
            | Self::Acquire(source)
            | Self::Admission(source)
            | Self::SessionRead(source)
            | Self::EffectStart(source)
            | Self::Settlement(source) => Some(source.as_ref()),
            Self::EffectUnknown(source) => Some(source.as_ref()),
            Self::Denied
            | Self::DeadlineExpired
            | Self::IdentityConflict
            | Self::TargetMismatch
            | Self::EffectStartConflict
            | Self::SettlementConflict => None,
        }
    }
}

/// Typed read-only status failures.
#[derive(Debug)]
pub enum GetRuntimeCancellationStatusError {
    Authorization(Box<PortError>),
    Denied,
    Read(Box<PortError>),
    NotFound,
    InconsistentBinding,
}

impl GetRuntimeCancellationStatusError {
    /// Returns the stable presentation category without existence disclosure on denial.
    pub fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::Authorization(source) | Self::Read(source) => source.category(),
            Self::Denied => ApplicationErrorCategory::PermissionDenied,
            Self::NotFound => ApplicationErrorCategory::NotFound,
            Self::InconsistentBinding => ApplicationErrorCategory::FailedPrecondition,
        }
    }
}

impl Display for GetRuntimeCancellationStatusError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Authorization(_) => "runtime-cancellation status authorization failed",
            Self::Denied => "runtime-cancellation status read denied",
            Self::Read(_) => "runtime-cancellation status read failed",
            Self::NotFound => "runtime-cancellation control was not found",
            Self::InconsistentBinding => {
                "runtime-cancellation status pins conflict with retained control"
            }
        })
    }
}

impl Error for GetRuntimeCancellationStatusError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Authorization(source) | Self::Read(source) => Some(source.as_ref()),
            Self::Denied | Self::NotFound | Self::InconsistentBinding => None,
        }
    }
}

fn validate_reference(
    field: &'static str,
    value: &str,
    maximum: usize,
) -> Result<(), RuntimeCancellationValidationError> {
    if value.is_empty() || value.len() > maximum || value.chars().any(char::is_control) {
        return Err(RuntimeCancellationValidationError { field });
    }
    Ok(())
}

fn validate_bounded_plain(
    field: &'static str,
    value: &str,
    maximum: usize,
) -> Result<(), RuntimeCancellationValidationError> {
    if value.trim().is_empty() || value.len() > maximum || value.chars().any(char::is_control) {
        return Err(RuntimeCancellationValidationError { field });
    }
    Ok(())
}

fn validate_idempotency_key(
    field: &'static str,
    value: &str,
    maximum: usize,
) -> Result<(), RuntimeCancellationValidationError> {
    if value.is_empty()
        || value.len() > maximum
        || !value.is_ascii()
        || value.chars().any(char::is_control)
    {
        return Err(RuntimeCancellationValidationError { field });
    }
    Ok(())
}

fn validate_product_coordinate(
    field: &'static str,
    value: &str,
    maximum: usize,
) -> Result<(), RuntimeCancellationValidationError> {
    if value.len() > maximum {
        return Err(RuntimeCancellationValidationError { field });
    }
    if value.split('/').enumerate().any(|(index, segment)| {
        index >= 2
            || !(1..=64).contains(&segment.len())
            || !segment
                .bytes()
                .next()
                .is_some_and(|byte| byte.is_ascii_lowercase())
            || !segment
                .bytes()
                .last()
                .is_some_and(|byte| byte.is_ascii_alphanumeric())
            || !segment
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    }) {
        return Err(RuntimeCancellationValidationError { field });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::io;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Wake, Waker};
    use std::thread;

    use super::*;

    #[test]
    fn source_products_are_bounded_without_a_company_scope_registry() {
        for value in ["workflow", "tools/workflow", "custom-scope/client-1"] {
            assert!(
                validate_product_coordinate("source_product", value, MAX_PRODUCT_REFERENCE_BYTES)
                    .is_ok(),
                "{value}"
            );
        }
        for value in [
            "",
            "/workflow",
            "tools/",
            "tools/workflow/extra",
            "Tools/workflow",
            "tools/work_flow",
            "1client",
            "client-",
            "é",
            "tools/client\n",
        ] {
            assert!(
                validate_product_coordinate("source_product", value, MAX_PRODUCT_REFERENCE_BYTES)
                    .is_err(),
                "{value:?}"
            );
        }
        let long = "a".repeat(65);
        let value = long.as_str();
        assert!(
            validate_product_coordinate("source_product", value, MAX_PRODUCT_REFERENCE_BYTES)
                .is_err()
        );
    }

    #[test]
    fn target_requires_nonzero_session_and_lifecycle_revisions() {
        for (revision, lifecycle_revision) in [(0, 1), (1, 0)] {
            assert!(
                RuntimeCancellationTarget::new(
                    OperationAttemptId::new("runtime-start-1").expect("attempt ID"),
                    PayloadDigest::of_bytes(b"start request"),
                    FencingToken::new(1).expect("fence"),
                    DispatchCellIdentity::new("cell-1").expect("cell"),
                    SessionId::new("session-1").expect("session ID"),
                    Revision::new(revision),
                    Revision::new(lifecycle_revision),
                    "undercurrent-provider-1".to_owned(),
                    1,
                )
                .is_err()
            );
        }
    }

    #[test]
    fn retained_snapshot_requires_nonzero_revisions() {
        for (revision, lifecycle_revision) in [(0, 0), (1, 0), (0, 1)] {
            assert!(
                RuntimeCancellationSessionSnapshot::new(
                    SessionId::new("session-1").expect("session ID"),
                    LifecycleState::Stopping,
                    Revision::new(revision),
                    Revision::new(lifecycle_revision),
                    TimestampMillis::new(1),
                )
                .is_err()
            );
        }
    }

    #[test]
    fn retained_state_enforces_its_lifecycle_discriminator() {
        let snapshot = RuntimeCancellationSessionSnapshot::new(
            SessionId::new("session-1").expect("session ID"),
            LifecycleState::Running,
            Revision::new(2),
            Revision::new(2),
            TimestampMillis::new(1),
        )
        .expect("coherent snapshot");

        assert!(
            RuntimeCancellationState::AcceptedStopping(snapshot)
                .validate()
                .is_err()
        );
    }

    #[test]
    fn retained_result_rejects_session_or_lifecycle_revision_regression() {
        let command = command(RuntimeCancellationMode::Graceful);
        let session_revision_regression = RuntimeCancellationTarget::new(
            command.target().attempt_id().clone(),
            command.target().request_digest(),
            command.target().fencing_token(),
            command.target().caller_cell_identity().clone(),
            command.target().session_id().clone(),
            Revision::new(2),
            Revision::new(1),
            command.target().expected_provider_id().to_owned(),
            command.target().expected_provider_generation(),
        )
        .expect("target with independently pinned revisions");
        let session_revision_regression = RuntimeCancellationResult::new(
            session_revision_regression,
            command.control().clone(),
            RuntimeCancellationState::AlreadyTerminal(
                RuntimeCancellationSessionSnapshot::new(
                    command.target().session_id().clone(),
                    LifecycleState::Stopped,
                    Revision::new(1),
                    Revision::new(1),
                    TimestampMillis::new(20),
                )
                .expect("internally coherent regressed snapshot"),
            ),
        );
        assert!(matches!(
            session_revision_regression,
            Err(RuntimeCancellationValidationError {
                field: "runtime_cancellation_result_binding"
            })
        ));

        let lifecycle_revision_regression = RuntimeCancellationResult::new(
            command.target().clone(),
            command.control().clone(),
            RuntimeCancellationState::AlreadyTerminal(
                RuntimeCancellationSessionSnapshot::new(
                    command.target().session_id().clone(),
                    LifecycleState::Stopped,
                    command.target().expected_session_revision(),
                    Revision::new(command.target().expected_lifecycle_revision().value() - 1),
                    TimestampMillis::new(20),
                )
                .expect("internally coherent lifecycle-regressed snapshot"),
            ),
        );
        assert!(matches!(
            lifecycle_revision_regression,
            Err(RuntimeCancellationValidationError {
                field: "runtime_cancellation_result_binding"
            })
        ));
    }

    #[test]
    fn cancellation_control_enforces_public_text_and_product_grammars() {
        let command = command(RuntimeCancellationMode::Graceful);
        let control = |idempotency_key: String,
                       reason: String,
                       source_product: String,
                       actor_context_ref: String,
                       authority_ref: String| {
            RuntimeCancellationControl::new(
                command.control().control_id().clone(),
                command.control().command_digest(),
                command.control().product_request_fingerprint(),
                idempotency_key,
                command.control().mode(),
                reason,
                command.control().propagation_policy(),
                command.control().deadline(),
                source_product,
                command.control().tenant_id().to_owned(),
                actor_context_ref,
                authority_ref,
            )
        };

        assert!(
            control(
                "idempotency-1".to_owned(),
                "operator stop".to_owned(),
                "example/workflow".to_owned(),
                "actor://current".to_owned(),
                "authority://cancel/1".to_owned(),
            )
            .is_ok()
        );
        for invalid in [
            control(
                "idém".to_owned(),
                "operator stop".to_owned(),
                "example/workflow".to_owned(),
                "actor://current".to_owned(),
                "authority://cancel/1".to_owned(),
            ),
            control(
                "idempotency-1".to_owned(),
                "   ".to_owned(),
                "example/workflow".to_owned(),
                "actor://current".to_owned(),
                "authority://cancel/1".to_owned(),
            ),
            control(
                "idempotency-1".to_owned(),
                "operator stop".to_owned(),
                "INVALID/workflow".to_owned(),
                "actor://current".to_owned(),
                "authority://cancel/1".to_owned(),
            ),
            control(
                "idempotency-1".to_owned(),
                "operator stop".to_owned(),
                "example/workflow".to_owned(),
                "é".repeat(MAX_CONTEXT_REFERENCE_BYTES / 2 + 1),
                "authority://cancel/1".to_owned(),
            ),
            control(
                "idempotency-1".to_owned(),
                "operator stop".to_owned(),
                "example/workflow".to_owned(),
                "actor://current".to_owned(),
                "   ".to_owned(),
            ),
        ] {
            assert!(invalid.is_err());
        }
    }

    #[test]
    fn force_mode_reaches_the_exact_authorization_decision() {
        let authorizer = RecordingAuthorizer::default();
        let command = command(RuntimeCancellationMode::Force);

        assert_eq!(
            block_on(authorize_runtime_cancellation(&authorizer, &command)).expect("authorization"),
            RuntimeCancellationAuthorization::Authorized
        );
        assert_eq!(
            authorizer
                .observed
                .lock()
                .expect("recording lock")
                .as_slice(),
            &[RuntimeCancellationMode::Force]
        );
    }

    #[test]
    fn atomic_admission_checks_deadline_after_waiting_for_the_session_lease() {
        let repository = RecordingRepository::default();
        let error = match block_on(acquire_and_admit_runtime_cancellation(
            &repository,
            &ImmediateCoordinator,
            &FixedClock(TimestampMillis::new(50)),
            &command_with_deadline(RuntimeCancellationMode::Graceful, 50),
        )) {
            Err(error) => error,
            Ok(_) => panic!("the deadline must expire while the command is queued"),
        };

        assert!(matches!(error, CancelRuntimeAttemptError::DeadlineExpired));
        assert_eq!(repository.admissions.load(Ordering::Acquire), 1);
    }

    #[test]
    fn authorized_exact_replay_remains_readable_after_its_admission_deadline() {
        let command = command_with_deadline(RuntimeCancellationMode::Graceful, 50);
        let (_lease, admission) = block_on(acquire_and_admit_runtime_cancellation(
            &ReplayRepository,
            &ImmediateCoordinator,
            &FixedClock(TimestampMillis::new(50)),
            &command,
        ))
        .expect("retained replay is not a new admission");

        assert!(matches!(admission, RuntimeCancellationAdmission::Replay(_)));
    }

    #[test]
    fn already_terminal_admission_never_requires_process_io() {
        let command = command(RuntimeCancellationMode::Graceful);
        let result = RuntimeCancellationResult::new(
            command.target().clone(),
            command.control().clone(),
            RuntimeCancellationState::AlreadyTerminal(
                RuntimeCancellationSessionSnapshot::new(
                    command.target().session_id().clone(),
                    LifecycleState::Stopped,
                    Revision::new(3),
                    Revision::new(3),
                    TimestampMillis::new(20),
                )
                .expect("terminal snapshot"),
            ),
        )
        .expect("terminal admission");

        assert!(!admitted_requires_process_io(&result));
    }

    #[test]
    fn durable_effect_marker_precedes_polling_the_runtime_effect() {
        let repository = RecordingRepository::default();
        let command = command(RuntimeCancellationMode::Force);
        let events = &repository.effect_events;

        let result = block_on(execute_after_runtime_cancellation_effect_marker(
            &repository,
            &FixedClock(TimestampMillis::new(25)),
            &command,
            async {
                events.lock().expect("effect-events lock").push("runtime");
                7_u8
            },
        ))
        .expect("durable marker permits the effect");

        assert!(matches!(
            result,
            RuntimeCancellationMarkedEffect::Executed(7)
        ));
        assert_eq!(
            repository
                .effect_events
                .lock()
                .expect("effect-events lock")
                .as_slice(),
            &["marker", "runtime"]
        );
    }

    #[test]
    fn failing_effect_marker_never_polls_the_runtime_effect() {
        let repository = RecordingRepository {
            fail_effect_start: true,
            ..RecordingRepository::default()
        };
        let command = command(RuntimeCancellationMode::Force);
        let events = &repository.effect_events;

        let result = block_on(execute_after_runtime_cancellation_effect_marker(
            &repository,
            &FixedClock(TimestampMillis::new(25)),
            &command,
            async {
                events.lock().expect("effect-events lock").push("runtime");
            },
        ));

        assert!(matches!(
            result,
            Err(CancelRuntimeAttemptError::EffectStart(_))
        ));
        assert_eq!(
            repository
                .effect_events
                .lock()
                .expect("effect-events lock")
                .as_slice(),
            &["marker"]
        );
    }

    #[test]
    fn status_denial_precedes_and_suppresses_retained_state_access() {
        let repository = RecordingRepository::default();
        let service = GetRuntimeCancellationStatusService::new(&DeniedAuthorizer, &repository);
        let command = command(RuntimeCancellationMode::Graceful);
        let query = GetRuntimeCancellationStatusQuery::new(
            command.control().control_id().clone(),
            command.control().command_digest(),
            command.target().attempt_id().clone(),
            command.target().request_digest(),
            command.target().session_id().clone(),
            command.target().expected_provider_id().to_owned(),
            command.target().expected_provider_generation(),
            command.actor().clone(),
        )
        .expect("status query");

        assert!(matches!(
            block_on(service.execute(query)),
            Err(GetRuntimeCancellationStatusError::Denied)
        ));
        assert_eq!(repository.status_reads.load(Ordering::Acquire), 0);
    }

    fn command(mode: RuntimeCancellationMode) -> CancelRuntimeAttemptCommand {
        command_with_deadline(mode, 10_000)
    }

    fn command_with_deadline(
        mode: RuntimeCancellationMode,
        deadline_millis: i64,
    ) -> CancelRuntimeAttemptCommand {
        let actor = RuntimeCancellationActor::new(
            "principal-1".to_owned(),
            Some("tenant-1".to_owned()),
            "transport-authority-1".to_owned(),
            "example/workflow".to_owned(),
            "tenant-1".to_owned(),
            "actor-context-1".to_owned(),
            "authority-1".to_owned(),
        )
        .expect("actor");
        let target = RuntimeCancellationTarget::new(
            OperationAttemptId::new("runtime-start-1").expect("attempt ID"),
            PayloadDigest::of_bytes(b"start request"),
            FencingToken::new(1).expect("fence"),
            DispatchCellIdentity::new("cell-1").expect("cell"),
            SessionId::new("session-1").expect("session ID"),
            Revision::new(2),
            Revision::new(2),
            "undercurrent-provider-1".to_owned(),
            1,
        )
        .expect("target");
        let control = RuntimeCancellationControl::new(
            OperationAttemptId::new("runtime-cancel-1").expect("control ID"),
            PayloadDigest::of_bytes(b"cancel command"),
            PayloadDigest::of_bytes(b"product request"),
            "idempotency-1".to_owned(),
            mode,
            "operator requested stop".to_owned(),
            RuntimeCancellationPropagationPolicy::TargetOnly,
            TimestampMillis::new(deadline_millis),
            actor.source_product().to_owned(),
            actor.tenant_id().to_owned(),
            actor.actor_context_ref().to_owned(),
            actor.authority_ref().to_owned(),
        )
        .expect("control");
        CancelRuntimeAttemptCommand::new(target, control, actor).expect("command")
    }

    #[derive(Default)]
    struct RecordingAuthorizer {
        observed: Mutex<Vec<RuntimeCancellationMode>>,
    }

    struct ImmediateCoordinator;
    struct TestLease;
    impl SessionMutationLease for TestLease {}

    impl SessionMutationCoordinator for ImmediateCoordinator {
        fn acquire<'a>(
            &'a self,
            _session_id: &'a SessionId,
        ) -> PortFuture<'a, Result<Box<dyn SessionMutationLease>, PortError>> {
            Box::pin(async { Ok(Box::new(TestLease) as Box<dyn SessionMutationLease>) })
        }
    }

    #[derive(Default)]
    struct RecordingRepository {
        admissions: AtomicUsize,
        status_reads: AtomicUsize,
        effect_events: Mutex<Vec<&'static str>>,
        fail_effect_start: bool,
    }

    impl RuntimeCancellationRepository for RecordingRepository {
        fn admit_runtime_cancellation<'a>(
            &'a self,
            _command: &'a CancelRuntimeAttemptCommand,
            _observed_at: TimestampMillis,
        ) -> PortFuture<'a, Result<RuntimeCancellationAdmission, PortError>> {
            self.admissions.fetch_add(1, Ordering::AcqRel);
            let expired = _command.control().deadline() <= _observed_at;
            Box::pin(async move {
                Ok(if expired {
                    RuntimeCancellationAdmission::DeadlineExpired
                } else {
                    RuntimeCancellationAdmission::TargetMismatch
                })
            })
        }

        fn settle_runtime_cancellation<'a>(
            &'a self,
            _control_id: &'a OperationAttemptId,
            _command_digest: PayloadDigest,
            _state: RuntimeCancellationState,
        ) -> PortFuture<'a, Result<RuntimeCancellationSettlement, PortError>> {
            Box::pin(async { panic!("settlement is unreachable in this test") })
        }

        fn mark_runtime_cancellation_effect_started<'a>(
            &'a self,
            _control_id: &'a OperationAttemptId,
            _command_digest: PayloadDigest,
            _observed_at: TimestampMillis,
        ) -> PortFuture<'a, Result<RuntimeCancellationEffectStart, PortError>> {
            self.effect_events
                .lock()
                .expect("effect-events lock")
                .push("marker");
            if self.fail_effect_start {
                Box::pin(async {
                    Err(PortError::new(
                        crate::PortErrorKind::Unavailable,
                        crate::PortOperation::MarkRuntimeCancellationEffectStarted,
                        None,
                        io::Error::other("private marker failure"),
                    ))
                })
            } else {
                Box::pin(async { Ok(RuntimeCancellationEffectStart::Started) })
            }
        }

        fn get_runtime_cancellation_status<'a>(
            &'a self,
            _query: &'a GetRuntimeCancellationStatusQuery,
        ) -> PortFuture<'a, Result<RuntimeCancellationStatusLookup, PortError>> {
            self.status_reads.fetch_add(1, Ordering::AcqRel);
            Box::pin(async { Ok(RuntimeCancellationStatusLookup::NotFound) })
        }
    }

    struct ReplayRepository;

    impl RuntimeCancellationRepository for ReplayRepository {
        fn admit_runtime_cancellation<'a>(
            &'a self,
            command: &'a CancelRuntimeAttemptCommand,
            _observed_at: TimestampMillis,
        ) -> PortFuture<'a, Result<RuntimeCancellationAdmission, PortError>> {
            let result = RuntimeCancellationResult::new(
                command.target().clone(),
                command.control().clone(),
                RuntimeCancellationState::AlreadyTerminal(
                    RuntimeCancellationSessionSnapshot::new(
                        command.target().session_id().clone(),
                        LifecycleState::Stopped,
                        Revision::new(3),
                        Revision::new(3),
                        TimestampMillis::new(49),
                    )
                    .expect("terminal replay snapshot"),
                ),
            )
            .expect("terminal replay result");
            Box::pin(async move { Ok(RuntimeCancellationAdmission::Replay(Box::new(result))) })
        }

        fn settle_runtime_cancellation<'a>(
            &'a self,
            _control_id: &'a OperationAttemptId,
            _command_digest: PayloadDigest,
            _state: RuntimeCancellationState,
        ) -> PortFuture<'a, Result<RuntimeCancellationSettlement, PortError>> {
            Box::pin(async { panic!("settlement is unreachable for an exact replay") })
        }

        fn mark_runtime_cancellation_effect_started<'a>(
            &'a self,
            _control_id: &'a OperationAttemptId,
            _command_digest: PayloadDigest,
            _observed_at: TimestampMillis,
        ) -> PortFuture<'a, Result<RuntimeCancellationEffectStart, PortError>> {
            Box::pin(async { panic!("effect marker is unreachable for an exact replay") })
        }

        fn get_runtime_cancellation_status<'a>(
            &'a self,
            _query: &'a GetRuntimeCancellationStatusQuery,
        ) -> PortFuture<'a, Result<RuntimeCancellationStatusLookup, PortError>> {
            Box::pin(async { panic!("status lookup is unreachable for an exact replay") })
        }
    }

    struct FixedClock(TimestampMillis);

    impl Clock for FixedClock {
        fn now(&self) -> TimestampMillis {
            self.0
        }
    }

    struct DeniedAuthorizer;

    impl RuntimeCancellationAuthorizer for DeniedAuthorizer {
        fn authorize_cancel<'a>(
            &'a self,
            _actor: &'a RuntimeCancellationActor,
            _control_id: &'a OperationAttemptId,
            _target_attempt_id: &'a OperationAttemptId,
            _mode: RuntimeCancellationMode,
        ) -> PortFuture<'a, Result<RuntimeCancellationAuthorization, PortError>> {
            Box::pin(async { Ok(RuntimeCancellationAuthorization::Denied) })
        }

        fn authorize_status<'a>(
            &'a self,
            _actor: &'a RuntimeCancellationActor,
            _control_id: &'a OperationAttemptId,
            _target_attempt_id: &'a OperationAttemptId,
        ) -> PortFuture<'a, Result<RuntimeCancellationAuthorization, PortError>> {
            Box::pin(async { Ok(RuntimeCancellationAuthorization::Denied) })
        }
    }

    impl RuntimeCancellationAuthorizer for RecordingAuthorizer {
        fn authorize_cancel<'a>(
            &'a self,
            _actor: &'a RuntimeCancellationActor,
            _control_id: &'a OperationAttemptId,
            _target_attempt_id: &'a OperationAttemptId,
            mode: RuntimeCancellationMode,
        ) -> PortFuture<'a, Result<RuntimeCancellationAuthorization, PortError>> {
            self.observed.lock().expect("recording lock").push(mode);
            Box::pin(async { Ok(RuntimeCancellationAuthorization::Authorized) })
        }

        fn authorize_status<'a>(
            &'a self,
            _actor: &'a RuntimeCancellationActor,
            _control_id: &'a OperationAttemptId,
            _target_attempt_id: &'a OperationAttemptId,
        ) -> PortFuture<'a, Result<RuntimeCancellationAuthorization, PortError>> {
            Box::pin(async { Ok(RuntimeCancellationAuthorization::Authorized) })
        }
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        struct ThreadWake(thread::Thread);
        impl Wake for ThreadWake {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }

        let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(future);
        loop {
            match Future::poll(future.as_mut(), &mut context) {
                Poll::Ready(output) => return output,
                Poll::Pending => thread::park(),
            }
        }
    }
}
