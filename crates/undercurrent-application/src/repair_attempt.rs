// SPDX-License-Identifier: Apache-2.0

//! Authorized, evidence-backed terminalization of unresolved operation attempts.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use undercurrent_domain::{
    EffectCertainty, FencingToken, OperationAttempt, OperationAttemptId, OperationAttemptState,
    PayloadDigest, TimestampMillis,
};

use crate::{Clock, PortError, PortFuture};

const MAX_ACTOR_REFERENCE_BYTES: usize = 128;
const MAX_EVIDENCE_REFERENCE_BYTES: usize = 256;

/// Credential-free actor references persisted with an attempt-repair audit record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationAttemptRepairActor {
    principal: String,
    tenant: Option<String>,
    authority: String,
}

impl OperationAttemptRepairActor {
    /// Constructs bounded references from an already authenticated request context.
    pub fn new(
        principal: String,
        tenant: Option<String>,
        authority: String,
    ) -> Result<Self, OperationAttemptRepairValidationError> {
        validate_reference("principal", &principal, MAX_ACTOR_REFERENCE_BYTES)?;
        if let Some(tenant) = tenant.as_deref() {
            validate_reference("tenant", tenant, MAX_ACTOR_REFERENCE_BYTES)?;
        }
        validate_reference("authority", &authority, MAX_ACTOR_REFERENCE_BYTES)?;
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

    /// Returns the optional tenant reference.
    pub fn tenant(&self) -> Option<&str> {
        self.tenant.as_deref()
    }

    /// Returns the authority reference used for the authorization decision.
    pub fn authority(&self) -> &str {
        &self.authority
    }
}

/// Immutable reference to retained runtime or operator-reviewed effect evidence.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationAttemptRepairEvidenceRef(String);

impl OperationAttemptRepairEvidenceRef {
    /// Constructs a bounded payload-free evidence reference.
    pub fn new(value: String) -> Result<Self, OperationAttemptRepairValidationError> {
        validate_reference("evidence", &value, MAX_EVIDENCE_REFERENCE_BYTES)?;
        Ok(Self(value))
    }

    /// Returns the safe reference value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Adapter-observed terminal evidence tied to one exact attempt fence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedOperationAttemptRepair {
    attempt_id: OperationAttemptId,
    fencing_token: FencingToken,
    certainty: EffectCertainty,
    result_digest: Option<PayloadDigest>,
    evidence_reference: OperationAttemptRepairEvidenceRef,
}

impl ObservedOperationAttemptRepair {
    /// Validates that the observation is terminal and internally consistent.
    pub fn new(
        attempt_id: OperationAttemptId,
        fencing_token: FencingToken,
        certainty: EffectCertainty,
        result_digest: Option<PayloadDigest>,
        evidence_reference: OperationAttemptRepairEvidenceRef,
    ) -> Result<Self, OperationAttemptRepairValidationError> {
        match (&certainty, &result_digest) {
            (EffectCertainty::KnownAbsent, None) | (EffectCertainty::KnownApplied, Some(_)) => {}
            (EffectCertainty::Unknown, _) => {
                return Err(OperationAttemptRepairValidationError::UnknownIsNotTerminal);
            }
            (EffectCertainty::KnownAbsent, Some(_)) => {
                return Err(OperationAttemptRepairValidationError::KnownAbsentHasDigest);
            }
            (EffectCertainty::KnownApplied, None) => {
                return Err(OperationAttemptRepairValidationError::KnownAppliedMissingDigest);
            }
        }
        Ok(Self {
            attempt_id,
            fencing_token,
            certainty,
            result_digest,
            evidence_reference,
        })
    }

    /// Returns the exact observed attempt identifier.
    pub fn attempt_id(&self) -> &OperationAttemptId {
        &self.attempt_id
    }

    /// Returns the fence attached to the retained evidence.
    pub const fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }

    /// Returns the terminal effect certainty.
    pub const fn certainty(&self) -> EffectCertainty {
        self.certainty
    }

    /// Returns the stable result digest required for KnownApplied.
    pub fn result_digest(&self) -> Option<&PayloadDigest> {
        self.result_digest.as_ref()
    }

    /// Returns the immutable evidence reference.
    pub const fn evidence_reference(&self) -> &OperationAttemptRepairEvidenceRef {
        &self.evidence_reference
    }
}

/// Trusted application command constructed after transport authentication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairOperationAttemptCommand {
    attempt_id: OperationAttemptId,
    request_digest: Option<PayloadDigest>,
    actor: OperationAttemptRepairActor,
}

impl RepairOperationAttemptCommand {
    /// Constructs a standalone/operator repair without an attached dispatch digest.
    pub const fn new(attempt_id: OperationAttemptId, actor: OperationAttemptRepairActor) -> Self {
        Self {
            attempt_id,
            request_digest: None,
            actor,
        }
    }

    /// Constructs an attached repair bound to the caller's canonical request digest.
    pub const fn attached(
        attempt_id: OperationAttemptId,
        request_digest: PayloadDigest,
        actor: OperationAttemptRepairActor,
    ) -> Self {
        Self {
            attempt_id,
            request_digest: Some(request_digest),
            actor,
        }
    }

    /// Returns the attempt to inspect and repair.
    pub const fn attempt_id(&self) -> &OperationAttemptId {
        &self.attempt_id
    }

    /// Returns the attached caller request digest, if this is a dispatched repair.
    pub const fn request_digest(&self) -> Option<PayloadDigest> {
        self.request_digest
    }

    /// Returns credential-free audit and authorization context.
    pub const fn actor(&self) -> &OperationAttemptRepairActor {
        &self.actor
    }
}

/// Authorization decision made before attempt or runtime evidence access.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationAttemptRepairAuthorization {
    /// The actor may repair this attempt.
    Authorized,
    /// The actor is denied without exposing whether the attempt exists.
    Denied,
}

/// Atomic durable repair record including its stable audit context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationAttemptRepairCommit {
    attempt_id: OperationAttemptId,
    fencing_token: FencingToken,
    certainty: EffectCertainty,
    result_digest: Option<PayloadDigest>,
    evidence_reference: OperationAttemptRepairEvidenceRef,
    actor: OperationAttemptRepairActor,
    repaired_at: TimestampMillis,
}

impl OperationAttemptRepairCommit {
    fn from_observation(
        observation: &ObservedOperationAttemptRepair,
        actor: OperationAttemptRepairActor,
        repaired_at: TimestampMillis,
    ) -> Self {
        Self {
            attempt_id: observation.attempt_id.clone(),
            fencing_token: observation.fencing_token,
            certainty: observation.certainty,
            result_digest: observation.result_digest,
            evidence_reference: observation.evidence_reference.clone(),
            actor,
            repaired_at,
        }
    }

    /// Returns the repaired attempt identifier.
    pub const fn attempt_id(&self) -> &OperationAttemptId {
        &self.attempt_id
    }

    /// Returns the evidence fence that must match durable state.
    pub const fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }

    /// Returns the terminal effect certainty.
    pub const fn certainty(&self) -> EffectCertainty {
        self.certainty
    }

    /// Returns the stable result digest required for KnownApplied.
    pub fn result_digest(&self) -> Option<&PayloadDigest> {
        self.result_digest.as_ref()
    }

    /// Returns the immutable evidence reference committed with the audit record.
    pub const fn evidence_reference(&self) -> &OperationAttemptRepairEvidenceRef {
        &self.evidence_reference
    }

    /// Returns credential-free actor context.
    pub const fn actor(&self) -> &OperationAttemptRepairActor {
        &self.actor
    }

    /// Returns the application clock value used for the durable transition.
    pub const fn repaired_at(&self) -> TimestampMillis {
        self.repaired_at
    }
}

/// Result of the atomic Unknown-only repair transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationAttemptRepairOutcome {
    /// Unknown was terminalized and the audit context was committed atomically.
    Repaired,
    /// The exact evidence-backed repair had already committed.
    Duplicate,
    /// No attempt exists for the supplied identifier.
    NotFound,
    /// The attempt is not Unknown and was not repaired by this exact commit.
    NotUnknown,
    /// The evidence refers to an obsolete fence.
    StaleFence { current: FencingToken },
    /// The attempt was repaired with different terminal effect data.
    PayloadConflict,
    /// The attempt was repaired with a different evidence or actor audit record.
    EvidenceConflict,
}

/// Exact unresolved readiness projection returned by the durable store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnresolvedOperationAttempts {
    total: usize,
    attempt_ids: Vec<OperationAttemptId>,
}

impl UnresolvedOperationAttempts {
    /// Constructs a bounded page with an exact total.
    pub fn new(
        total: usize,
        attempt_ids: Vec<OperationAttemptId>,
    ) -> Result<Self, OperationAttemptRepairValidationError> {
        if total < attempt_ids.len() {
            return Err(OperationAttemptRepairValidationError::InvalidUnresolvedTotal);
        }
        for pair in attempt_ids.windows(2) {
            if pair[0] == pair[1] {
                return Err(OperationAttemptRepairValidationError::DuplicateUnresolvedAttempt);
            }
            if pair[0] > pair[1] {
                return Err(OperationAttemptRepairValidationError::UnorderedUnresolvedAttempt);
            }
        }
        Ok(Self { total, attempt_ids })
    }

    /// Returns the exact unresolved count, including IDs outside this page.
    pub const fn total(&self) -> usize {
        self.total
    }

    /// Returns the bounded stable-ID page.
    pub fn attempt_ids(&self) -> &[OperationAttemptId] {
        &self.attempt_ids
    }

    /// Reports whether the ID page is truncated.
    pub const fn truncated(&self) -> bool {
        self.total > self.attempt_ids.len()
    }
}

/// Fail-closed readiness state derived from unresolved attempts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationAttemptReadinessStatus {
    /// No unresolved attempt remains.
    Ready,
    /// Mutating readiness is blocked until every unresolved attempt is repaired.
    BlockedUnresolvedAttempts,
}

/// Readiness snapshot returned after repair and to daemon startup checks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationAttemptReadiness {
    status: OperationAttemptReadinessStatus,
    unresolved: UnresolvedOperationAttempts,
}

impl OperationAttemptReadiness {
    fn from_unresolved(unresolved: UnresolvedOperationAttempts) -> Self {
        let status = if unresolved.total() == 0 {
            OperationAttemptReadinessStatus::Ready
        } else {
            OperationAttemptReadinessStatus::BlockedUnresolvedAttempts
        };
        Self { status, unresolved }
    }

    /// Returns the explicit readiness status.
    pub const fn status(&self) -> OperationAttemptReadinessStatus {
        self.status
    }

    /// Returns true only when the exact unresolved total is zero.
    pub const fn complete(&self) -> bool {
        matches!(self.status, OperationAttemptReadinessStatus::Ready)
    }

    /// Returns the exact total and bounded identifier page.
    pub const fn unresolved(&self) -> &UnresolvedOperationAttempts {
        &self.unresolved
    }
}

/// Whether the durable repair was newly committed or replayed idempotently.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationAttemptRepairDisposition {
    /// This call committed the terminal transition.
    Repaired,
    /// This call resumed the exact already-committed transition.
    Duplicate,
}

/// Successful terminal repair and post-commit readiness result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairOperationAttemptResult {
    attempt_id: OperationAttemptId,
    certainty: EffectCertainty,
    result_digest: Option<PayloadDigest>,
    evidence_reference: OperationAttemptRepairEvidenceRef,
    disposition: OperationAttemptRepairDisposition,
    readiness: OperationAttemptReadiness,
}

impl RepairOperationAttemptResult {
    /// Returns the repaired attempt identifier.
    pub const fn attempt_id(&self) -> &OperationAttemptId {
        &self.attempt_id
    }

    /// Returns KnownAbsent or KnownApplied.
    pub const fn certainty(&self) -> EffectCertainty {
        self.certainty
    }

    /// Returns the stable result digest for KnownApplied.
    pub fn result_digest(&self) -> Option<&PayloadDigest> {
        self.result_digest.as_ref()
    }

    /// Returns the immutable evidence reference used for the repair.
    pub const fn evidence_reference(&self) -> &OperationAttemptRepairEvidenceRef {
        &self.evidence_reference
    }

    /// Returns whether this call committed or replayed the repair.
    pub const fn disposition(&self) -> OperationAttemptRepairDisposition {
        self.disposition
    }

    /// Returns fail-closed readiness after the durable transition and ACK.
    pub const fn readiness(&self) -> &OperationAttemptReadiness {
        &self.readiness
    }
}

/// Authorizes repair before the application reveals attempt existence or evidence.
pub trait OperationAttemptRepairAuthorizer: Send + Sync {
    /// Evaluates the authenticated actor and attempt identifier.
    fn authorize_repair<'a>(
        &'a self,
        actor: &'a OperationAttemptRepairActor,
        attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<OperationAttemptRepairAuthorization, PortError>>;
}

/// Retained runtime evidence source whose receipt is ACKed only after commit.
pub trait OperationAttemptRepairObserver: Send + Sync {
    /// Returns evidence for one exact durable attempt without consuming it.
    fn observe_repair<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<ObservedOperationAttemptRepair, PortError>>;

    /// Idempotently releases retained evidence after the repair commit.
    fn acknowledge_repair<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
        evidence_reference: &'a OperationAttemptRepairEvidenceRef,
    ) -> PortFuture<'a, Result<(), PortError>>;
}

/// Product-owned atomic repair, audit, and unresolved-readiness store.
pub trait OperationAttemptRepairRepository: Send + Sync {
    /// Loads the current durable attempt after authorization.
    fn load_repair_candidate<'a>(
        &'a self,
        attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<Option<OperationAttempt>, PortError>>;

    /// Atomically terminalizes only Unknown at the exact fence and records audit context.
    fn repair_unknown<'a>(
        &'a self,
        commit: &'a OperationAttemptRepairCommit,
    ) -> PortFuture<'a, Result<OperationAttemptRepairOutcome, PortError>>;

    /// Returns an exact unresolved total and bounded ordered identifier page.
    fn unresolved_attempts<'a>(
        &'a self,
        scan: &'a crate::OperationAttemptScan,
    ) -> PortFuture<'a, Result<UnresolvedOperationAttempts, PortError>>;
}

/// Authorized orchestration for one evidence-backed Unknown repair.
pub struct RepairOperationAttemptService<'a> {
    authorizer: &'a dyn OperationAttemptRepairAuthorizer,
    repository: &'a dyn OperationAttemptRepairRepository,
    observer: &'a dyn OperationAttemptRepairObserver,
    clock: &'a dyn Clock,
    readiness_limit: usize,
}

impl<'a> RepairOperationAttemptService<'a> {
    /// Constructs the service with a bounded readiness page.
    pub fn new(
        authorizer: &'a dyn OperationAttemptRepairAuthorizer,
        repository: &'a dyn OperationAttemptRepairRepository,
        observer: &'a dyn OperationAttemptRepairObserver,
        clock: &'a dyn Clock,
        readiness_limit: usize,
    ) -> Result<Self, OperationAttemptRepairValidationError> {
        if readiness_limit == 0 {
            return Err(OperationAttemptRepairValidationError::ZeroReadinessLimit);
        }
        Ok(Self {
            authorizer,
            repository,
            observer,
            clock,
            readiness_limit,
        })
    }

    /// Repairs one Unknown attempt without trusting peer-supplied effect claims.
    pub async fn execute(
        &self,
        command: RepairOperationAttemptCommand,
    ) -> Result<RepairOperationAttemptResult, RepairOperationAttemptError> {
        let authorization = self
            .authorizer
            .authorize_repair(command.actor(), command.attempt_id())
            .await
            .map_err(|source| RepairOperationAttemptError::Authorization(Box::new(source)))?;
        if authorization == OperationAttemptRepairAuthorization::Denied {
            return Err(RepairOperationAttemptError::Denied);
        }

        let attempt = self
            .repository
            .load_repair_candidate(command.attempt_id())
            .await
            .map_err(|source| RepairOperationAttemptError::Load(Box::new(source)))?
            .ok_or(RepairOperationAttemptError::NotFound)?;
        match (attempt.dispatch_binding(), command.request_digest()) {
            (None, None) => {}
            (Some(binding), Some(request_digest))
                if binding.request_digest() == Some(request_digest) => {}
            _ => return Err(RepairOperationAttemptError::PayloadConflict),
        }
        // Keep terminal candidates eligible for exact repair replay after a
        // lost acknowledgement. The repository must compare retained audit
        // evidence before accepting a duplicate; a terminal state alone is not proof.
        // See ../docs/internals/execution/runtime-controls.md#repair-an-unknown-attempt-from-evidence.
        if matches!(attempt.state(), OperationAttemptState::Accepted) {
            return Err(RepairOperationAttemptError::NotUnknown);
        }

        let observation = self
            .observer
            .observe_repair(&attempt)
            .await
            .map_err(|source| RepairOperationAttemptError::Observation(Box::new(source)))?;
        if observation.attempt_id() != attempt.id()
            || observation.fencing_token() != attempt.fencing_token()
        {
            return Err(RepairOperationAttemptError::StaleEvidence);
        }

        let commit = OperationAttemptRepairCommit::from_observation(
            &observation,
            command.actor,
            self.clock.now(),
        );
        let disposition = match self
            .repository
            .repair_unknown(&commit)
            .await
            .map_err(|source| RepairOperationAttemptError::Commit(Box::new(source)))?
        {
            OperationAttemptRepairOutcome::Repaired => OperationAttemptRepairDisposition::Repaired,
            OperationAttemptRepairOutcome::Duplicate => {
                OperationAttemptRepairDisposition::Duplicate
            }
            OperationAttemptRepairOutcome::NotFound => {
                return Err(RepairOperationAttemptError::NotFound);
            }
            OperationAttemptRepairOutcome::NotUnknown => {
                return Err(RepairOperationAttemptError::NotUnknown);
            }
            OperationAttemptRepairOutcome::StaleFence { current } => {
                return Err(RepairOperationAttemptError::StaleFence { current });
            }
            OperationAttemptRepairOutcome::PayloadConflict => {
                return Err(RepairOperationAttemptError::PayloadConflict);
            }
            OperationAttemptRepairOutcome::EvidenceConflict => {
                return Err(RepairOperationAttemptError::EvidenceConflict);
            }
        };

        self.observer
            .acknowledge_repair(&attempt, observation.evidence_reference())
            .await
            .map_err(|source| {
                RepairOperationAttemptError::AcknowledgementAfterCommit(Box::new(source))
            })?;
        let scan = crate::OperationAttemptScan::without_exclusions(self.readiness_limit).map_err(
            |source| {
                RepairOperationAttemptError::ReadinessAfterCommit(Box::new(PortError::new(
                    crate::PortErrorKind::InvalidData,
                    crate::PortOperation::InspectOperationAttemptReadiness,
                    None,
                    source,
                )))
            },
        )?;
        let unresolved = self
            .repository
            .unresolved_attempts(&scan)
            .await
            .map_err(|source| {
                RepairOperationAttemptError::ReadinessAfterCommit(Box::new(source))
            })?;

        Ok(RepairOperationAttemptResult {
            attempt_id: observation.attempt_id,
            certainty: observation.certainty,
            result_digest: observation.result_digest,
            evidence_reference: observation.evidence_reference,
            disposition,
            readiness: OperationAttemptReadiness::from_unresolved(unresolved),
        })
    }
}

/// Readiness projection used by startup and daemon health without repairing effects.
pub struct OperationAttemptReadinessService<'a> {
    repository: &'a dyn OperationAttemptRepairRepository,
    limit: usize,
}

impl<'a> OperationAttemptReadinessService<'a> {
    /// Constructs the bounded readiness query.
    pub fn new(
        repository: &'a dyn OperationAttemptRepairRepository,
        limit: usize,
    ) -> Result<Self, OperationAttemptRepairValidationError> {
        if limit == 0 {
            return Err(OperationAttemptRepairValidationError::ZeroReadinessLimit);
        }
        Ok(Self { repository, limit })
    }

    /// Returns fail-closed readiness with exact count and bounded IDs.
    pub async fn inspect(&self) -> Result<OperationAttemptReadiness, RepairOperationAttemptError> {
        let scan =
            crate::OperationAttemptScan::without_exclusions(self.limit).map_err(|source| {
                RepairOperationAttemptError::Readiness(Box::new(PortError::new(
                    crate::PortErrorKind::InvalidData,
                    crate::PortOperation::InspectOperationAttemptReadiness,
                    None,
                    source,
                )))
            })?;
        self.repository
            .unresolved_attempts(&scan)
            .await
            .map(OperationAttemptReadiness::from_unresolved)
            .map_err(|source| RepairOperationAttemptError::Readiness(Box::new(source)))
    }
}

/// Stable validation failures for repair values and bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationAttemptRepairValidationError {
    /// A reference is empty, too large, or contains unsafe characters.
    InvalidReference { field: &'static str },
    /// Unknown cannot be terminalized without new evidence.
    UnknownIsNotTerminal,
    /// KnownAbsent cannot carry a result digest.
    KnownAbsentHasDigest,
    /// KnownApplied requires a stable result digest.
    KnownAppliedMissingDigest,
    /// The exact total cannot be smaller than the returned ID page.
    InvalidUnresolvedTotal,
    /// An unresolved page cannot contain duplicate attempt identifiers.
    DuplicateUnresolvedAttempt,
    /// An unresolved identifier page must use stable ascending order.
    UnorderedUnresolvedAttempt,
    /// Readiness queries must always be bounded and nonzero.
    ZeroReadinessLimit,
}

impl Display for OperationAttemptRepairValidationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidReference { field } => write!(formatter, "invalid {field} reference"),
            Self::UnknownIsNotTerminal => formatter.write_str("Unknown is not a terminal repair"),
            Self::KnownAbsentHasDigest => {
                formatter.write_str("KnownAbsent repair cannot carry a result digest")
            }
            Self::KnownAppliedMissingDigest => {
                formatter.write_str("KnownApplied repair requires a result digest")
            }
            Self::InvalidUnresolvedTotal => {
                formatter.write_str("unresolved total is smaller than its identifier page")
            }
            Self::DuplicateUnresolvedAttempt => {
                formatter.write_str("unresolved identifier page contains a duplicate")
            }
            Self::UnorderedUnresolvedAttempt => {
                formatter.write_str("unresolved identifier page is not stably ordered")
            }
            Self::ZeroReadinessLimit => formatter.write_str("readiness limit must be nonzero"),
        }
    }
}

impl Error for OperationAttemptRepairValidationError {}

/// Typed service failures with source text redacted from presentation.
#[derive(Debug)]
pub enum RepairOperationAttemptError {
    /// Authorization infrastructure failed before state access.
    Authorization(Box<PortError>),
    /// The authenticated actor is not permitted to repair attempts.
    Denied,
    /// Durable candidate loading failed.
    Load(Box<PortError>),
    /// The attempt does not exist.
    NotFound,
    /// The attempt is not a repairable Unknown or exact repair retry.
    NotUnknown,
    /// Runtime evidence observation failed.
    Observation(Box<PortError>),
    /// Evidence identifies another attempt or an obsolete fence.
    StaleEvidence,
    /// The atomic repair transaction failed.
    Commit(Box<PortError>),
    /// Durable state has advanced beyond the evidence fence.
    StaleFence { current: FencingToken },
    /// Dispatch identity or terminal effect data conflicts with durable state.
    PayloadConflict,
    /// Evidence or actor audit context conflicts with an already committed repair.
    EvidenceConflict,
    /// Repair committed, but retained evidence acknowledgement must be retried.
    AcknowledgementAfterCommit(Box<PortError>),
    /// Repair committed and ACKed, but post-commit readiness could not be read.
    ReadinessAfterCommit(Box<PortError>),
    /// Standalone readiness inspection failed closed.
    Readiness(Box<PortError>),
}

impl Display for RepairOperationAttemptError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::Authorization(_) => "attempt repair authorization failed",
            Self::Denied => "attempt repair denied",
            Self::Load(_) => "attempt repair candidate load failed",
            Self::NotFound => "attempt repair candidate was not found",
            Self::NotUnknown => "attempt is not repairable Unknown state",
            Self::Observation(_) => "attempt repair evidence observation failed",
            Self::StaleEvidence => "attempt repair evidence is stale",
            Self::Commit(_) => "attempt repair commit failed",
            Self::StaleFence { .. } => "attempt repair fence is stale",
            Self::PayloadConflict => {
                "attempt repair request digest or effect conflicts with durable state"
            }
            Self::EvidenceConflict => "attempt repair evidence conflicts with durable audit state",
            Self::AcknowledgementAfterCommit(_) => {
                "attempt repair committed but evidence acknowledgement is pending"
            }
            Self::ReadinessAfterCommit(_) => {
                "attempt repair committed but readiness inspection failed"
            }
            Self::Readiness(_) => "attempt repair readiness inspection failed",
        };
        formatter.write_str(message)
    }
}

impl Error for RepairOperationAttemptError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Authorization(source)
            | Self::Load(source)
            | Self::Observation(source)
            | Self::Commit(source)
            | Self::AcknowledgementAfterCommit(source)
            | Self::ReadinessAfterCommit(source)
            | Self::Readiness(source) => Some(source.as_ref()),
            Self::Denied
            | Self::NotFound
            | Self::NotUnknown
            | Self::StaleEvidence
            | Self::StaleFence { .. }
            | Self::PayloadConflict
            | Self::EvidenceConflict => None,
        }
    }
}

fn validate_reference(
    field: &'static str,
    value: &str,
    maximum_bytes: usize,
) -> Result<(), OperationAttemptRepairValidationError> {
    if value.is_empty()
        || value.len() > maximum_bytes
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':' | b'/')
        })
    {
        return Err(OperationAttemptRepairValidationError::InvalidReference { field });
    }
    Ok(())
}
