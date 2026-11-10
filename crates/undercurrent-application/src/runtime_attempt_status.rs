// SPDX-License-Identifier: Apache-2.0

//! Authorization-first, read-only runtime-attempt status lookup.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use undercurrent_domain::{
    DispatchCellIdentity, EffectCertainty, FencingToken, LifecycleState, OperationAttemptId,
    PayloadDigest, Revision, SessionId, TimestampMillis,
};

use crate::{ApplicationErrorCategory, PortError, PortFuture};

const MAX_ACTOR_REFERENCE_BYTES: usize = 128;

/// Credential-free caller context for one attempt-scoped status read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeAttemptStatusActor {
    principal: String,
    tenant: Option<String>,
    authority: String,
}

impl RuntimeAttemptStatusActor {
    /// Constructs bounded references from an already authenticated request context.
    pub fn new(
        principal: String,
        tenant: Option<String>,
        authority: String,
    ) -> Result<Self, RuntimeAttemptStatusValidationError> {
        validate_reference("principal", &principal)?;
        if let Some(tenant) = tenant.as_deref() {
            validate_reference("tenant", tenant)?;
        }
        validate_reference("authority", &authority)?;
        Ok(Self {
            principal,
            tenant,
            authority,
        })
    }

    /// Returns the authenticated principal reference, never a credential.
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// Returns the optional authenticated tenant reference.
    pub fn tenant(&self) -> Option<&str> {
        self.tenant.as_deref()
    }

    /// Returns the authority reference used by the authorization decision.
    pub fn authority(&self) -> &str {
        &self.authority
    }
}

/// Trusted application query constructed only after protocol validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetRuntimeAttemptStatusQuery {
    attempt_id: OperationAttemptId,
    request_digest: PayloadDigest,
    actor: RuntimeAttemptStatusActor,
}

impl GetRuntimeAttemptStatusQuery {
    /// Constructs an exact attached lookup; digestless queries are unrepresentable.
    pub const fn new(
        attempt_id: OperationAttemptId,
        request_digest: PayloadDigest,
        actor: RuntimeAttemptStatusActor,
    ) -> Self {
        Self {
            attempt_id,
            request_digest,
            actor,
        }
    }

    /// Returns the exact durable attempt identity.
    pub const fn attempt_id(&self) -> &OperationAttemptId {
        &self.attempt_id
    }

    /// Returns the exact caller runtime-start request digest.
    pub const fn request_digest(&self) -> PayloadDigest {
        self.request_digest
    }

    /// Returns credential-free authorization context.
    pub const fn actor(&self) -> &RuntimeAttemptStatusActor {
        &self.actor
    }
}

/// Attempt-scoped read authorization completed before repository access.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeAttemptStatusAuthorization {
    Authorized,
    Denied,
}

/// Safe immutable dispatch identity required by a caller to verify the result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeAttemptStatusBinding {
    caller_cell_identity: DispatchCellIdentity,
    request_digest: PayloadDigest,
    launch_profile_digest: PayloadDigest,
    environment_binding_digest: PayloadDigest,
    input_digest: PayloadDigest,
}

impl RuntimeAttemptStatusBinding {
    /// Constructs a payload-free projection of a current durable dispatch binding.
    pub const fn new(
        caller_cell_identity: DispatchCellIdentity,
        request_digest: PayloadDigest,
        launch_profile_digest: PayloadDigest,
        environment_binding_digest: PayloadDigest,
        input_digest: PayloadDigest,
    ) -> Self {
        Self {
            caller_cell_identity,
            request_digest,
            launch_profile_digest,
            environment_binding_digest,
            input_digest,
        }
    }

    pub const fn caller_cell_identity(&self) -> &DispatchCellIdentity {
        &self.caller_cell_identity
    }
    pub const fn request_digest(&self) -> PayloadDigest {
        self.request_digest
    }
    pub const fn launch_profile_digest(&self) -> PayloadDigest {
        self.launch_profile_digest
    }
    pub const fn environment_binding_digest(&self) -> PayloadDigest {
        self.environment_binding_digest
    }
    pub const fn input_digest(&self) -> PayloadDigest {
        self.input_digest
    }
}

/// Minimal durable session state associated with one known-applied start.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeAttemptSessionStatus {
    session_id: SessionId,
    lifecycle: LifecycleState,
    revision: Revision,
    lifecycle_revision: Revision,
    observed_at: TimestampMillis,
}

impl RuntimeAttemptSessionStatus {
    /// Constructs a coherent domain-validated session projection.
    pub const fn new(
        session_id: SessionId,
        lifecycle: LifecycleState,
        revision: Revision,
        lifecycle_revision: Revision,
        observed_at: TimestampMillis,
    ) -> Self {
        Self {
            session_id,
            lifecycle,
            revision,
            lifecycle_revision,
            observed_at,
        }
    }

    pub const fn session_id(&self) -> &SessionId {
        &self.session_id
    }
    pub const fn lifecycle(&self) -> LifecycleState {
        self.lifecycle
    }
    pub const fn revision(&self) -> Revision {
        self.revision
    }
    pub const fn lifecycle_revision(&self) -> Revision {
        self.lifecycle_revision
    }
    pub const fn observed_at(&self) -> TimestampMillis {
        self.observed_at
    }
}

/// Strict retained effect state for a runtime-start attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeAttemptStatusSnapshot {
    Unknown {
        observed_at: TimestampMillis,
    },
    KnownAbsent {
        observed_at: TimestampMillis,
    },
    KnownApplied {
        result_digest: PayloadDigest,
        observed_at: TimestampMillis,
        session: RuntimeAttemptSessionStatus,
    },
    Compensated {
        observed_at: TimestampMillis,
    },
}

impl RuntimeAttemptStatusSnapshot {
    /// Returns attempt-state observation time without inspecting session state.
    pub const fn observed_at(&self) -> TimestampMillis {
        match self {
            Self::Unknown { observed_at }
            | Self::KnownAbsent { observed_at }
            | Self::KnownApplied { observed_at, .. }
            | Self::Compensated { observed_at } => *observed_at,
        }
    }

    /// Returns exact effect certainty when compensation is not the durable state.
    pub const fn certainty(&self) -> Option<EffectCertainty> {
        match self {
            Self::Unknown { .. } => Some(EffectCertainty::Unknown),
            Self::KnownAbsent { .. } => Some(EffectCertainty::KnownAbsent),
            Self::KnownApplied { .. } => Some(EffectCertainty::KnownApplied),
            Self::Compensated { .. } => None,
        }
    }
}

/// Complete safe application result returned from one coherent store snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeAttemptStatusResult {
    attempt_id: OperationAttemptId,
    fencing_token: FencingToken,
    binding: RuntimeAttemptStatusBinding,
    snapshot: RuntimeAttemptStatusSnapshot,
}

impl RuntimeAttemptStatusResult {
    /// Constructs a result from already validated durable values.
    pub const fn new(
        attempt_id: OperationAttemptId,
        fencing_token: FencingToken,
        binding: RuntimeAttemptStatusBinding,
        snapshot: RuntimeAttemptStatusSnapshot,
    ) -> Self {
        Self {
            attempt_id,
            fencing_token,
            binding,
            snapshot,
        }
    }

    pub const fn attempt_id(&self) -> &OperationAttemptId {
        &self.attempt_id
    }
    pub const fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }
    pub const fn binding(&self) -> &RuntimeAttemptStatusBinding {
        &self.binding
    }
    pub const fn snapshot(&self) -> &RuntimeAttemptStatusSnapshot {
        &self.snapshot
    }
}

/// Store-owned classification after exact provenance preflight.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeAttemptStatusLookup {
    Found(Box<RuntimeAttemptStatusResult>),
    NotFound,
    UnverifiableProvenance,
    InconsistentState,
}

/// Authorizes an exact attempt read without revealing whether the attempt exists.
pub trait RuntimeAttemptStatusAuthorizer: Send + Sync {
    fn authorize_status<'a>(
        &'a self,
        actor: &'a RuntimeAttemptStatusActor,
        attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<RuntimeAttemptStatusAuthorization, PortError>>;
}

/// Product-owned coherent attempt-and-session read boundary.
pub trait RuntimeAttemptStatusRepository: Send + Sync {
    /// Reads one exact snapshot. Implementations must validate provenance before
    /// loading the target session and must perform no writes.
    fn get_runtime_attempt_status<'a>(
        &'a self,
        attempt_id: &'a OperationAttemptId,
        request_digest: PayloadDigest,
    ) -> PortFuture<'a, Result<RuntimeAttemptStatusLookup, PortError>>;
}

/// Authorization-first orchestration for a single bounded status lookup.
pub struct GetRuntimeAttemptStatusService<'a> {
    authorizer: &'a dyn RuntimeAttemptStatusAuthorizer,
    repository: &'a dyn RuntimeAttemptStatusRepository,
}

impl<'a> GetRuntimeAttemptStatusService<'a> {
    /// Constructs the stateless read service.
    pub const fn new(
        authorizer: &'a dyn RuntimeAttemptStatusAuthorizer,
        repository: &'a dyn RuntimeAttemptStatusRepository,
    ) -> Self {
        Self {
            authorizer,
            repository,
        }
    }

    /// Returns one coherent snapshot without observing or repairing runtime evidence.
    pub async fn execute(
        &self,
        query: GetRuntimeAttemptStatusQuery,
    ) -> Result<RuntimeAttemptStatusResult, GetRuntimeAttemptStatusError> {
        let authorization = self
            .authorizer
            .authorize_status(query.actor(), query.attempt_id())
            .await
            .map_err(|source| GetRuntimeAttemptStatusError::Authorization(Box::new(source)))?;
        if authorization == RuntimeAttemptStatusAuthorization::Denied {
            return Err(GetRuntimeAttemptStatusError::Denied);
        }

        match self
            .repository
            .get_runtime_attempt_status(query.attempt_id(), query.request_digest())
            .await
            .map_err(|source| GetRuntimeAttemptStatusError::Read(Box::new(source)))?
        {
            RuntimeAttemptStatusLookup::Found(result) => Ok(*result),
            RuntimeAttemptStatusLookup::NotFound => Err(GetRuntimeAttemptStatusError::NotFound),
            RuntimeAttemptStatusLookup::UnverifiableProvenance => {
                Err(GetRuntimeAttemptStatusError::UnverifiableProvenance)
            }
            RuntimeAttemptStatusLookup::InconsistentState => {
                Err(GetRuntimeAttemptStatusError::InconsistentState)
            }
        }
    }
}

/// Stable actor-reference validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeAttemptStatusValidationError {
    field: &'static str,
}

impl Display for RuntimeAttemptStatusValidationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid runtime-attempt status {} reference",
            self.field
        )
    }
}

impl Error for RuntimeAttemptStatusValidationError {}

/// Typed read failures with payload-free presentation.
#[derive(Debug)]
pub enum GetRuntimeAttemptStatusError {
    Authorization(Box<PortError>),
    Denied,
    Read(Box<PortError>),
    NotFound,
    UnverifiableProvenance,
    InconsistentState,
}

impl GetRuntimeAttemptStatusError {
    /// Returns the stable presentation category without leaking existence on denial.
    pub fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::Authorization(source) | Self::Read(source) => source.category(),
            Self::Denied => ApplicationErrorCategory::PermissionDenied,
            Self::NotFound => ApplicationErrorCategory::NotFound,
            Self::UnverifiableProvenance => ApplicationErrorCategory::FailedPrecondition,
            Self::InconsistentState => ApplicationErrorCategory::Internal,
        }
    }
}

impl Display for GetRuntimeAttemptStatusError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Authorization(_) => "runtime-attempt status authorization failed",
            Self::Denied => "runtime-attempt status read denied",
            Self::Read(_) => "runtime-attempt status read failed",
            Self::NotFound => "runtime attempt was not found",
            Self::UnverifiableProvenance => {
                "runtime attempt lacks the exact current attached provenance"
            }
            Self::InconsistentState => "runtime attempt and session state are inconsistent",
        })
    }
}

impl Error for GetRuntimeAttemptStatusError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Authorization(source) | Self::Read(source) => Some(source.as_ref()),
            Self::Denied
            | Self::NotFound
            | Self::UnverifiableProvenance
            | Self::InconsistentState => None,
        }
    }
}

fn validate_reference(
    field: &'static str,
    value: &str,
) -> Result<(), RuntimeAttemptStatusValidationError> {
    if value.is_empty()
        || value.len() > MAX_ACTOR_REFERENCE_BYTES
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':' | b'/')
        })
    {
        return Err(RuntimeAttemptStatusValidationError { field });
    }
    Ok(())
}
