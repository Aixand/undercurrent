// SPDX-License-Identifier: Apache-2.0

//! Durable acceptance, settlement, and reconciliation boundary for operations.

use undercurrent_domain::{
    EffectCertainty, FencingToken, OperationAttempt, OperationAttemptId, OperationAttemptState,
    PayloadDigest, ProcessIdentity, ProcessMutationKind, Revision, RuntimeDispatchBinding,
    SessionId, TimestampMillis,
};

use crate::{PortError, PortFuture};

/// Allocates fresh identities for independent internal mutations.
///
/// Implementations must avoid reuse across calls, threads, and process restarts.
/// Caller-supplied retry or delivery identities are handled separately.
pub trait OperationAttemptIdSource: Send + Sync {
    /// Allocates an identity before any effect or delivery reservation.
    ///
    /// # Errors
    /// Returns a port failure when a fresh identity cannot be allocated.
    fn next_attempt_id(&self) -> Result<OperationAttemptId, PortError>;
}

/// Transport-neutral context threaded through a process-affecting mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessMutationContext {
    attempt_id: OperationAttemptId,
    target_session: Option<SessionId>,
    kind: ProcessMutationKind,
    fencing_token: FencingToken,
    payload_digest: PayloadDigest,
    dispatch_binding: Option<RuntimeDispatchBinding>,
    accepted_at: TimestampMillis,
}

impl ProcessMutationContext {
    /// Constructs context after boundary validation and before durable acceptance.
    pub const fn new(
        attempt_id: OperationAttemptId,
        target_session: Option<SessionId>,
        kind: ProcessMutationKind,
        fencing_token: FencingToken,
        payload_digest: PayloadDigest,
        accepted_at: TimestampMillis,
    ) -> Self {
        Self {
            attempt_id,
            target_session,
            kind,
            fencing_token,
            payload_digest,
            dispatch_binding: None,
            accepted_at,
        }
    }

    /// Returns attempt identity.
    pub const fn attempt_id(&self) -> &OperationAttemptId {
        &self.attempt_id
    }
    /// Returns optional target session.
    pub const fn target_session(&self) -> Option<&SessionId> {
        self.target_session.as_ref()
    }
    /// Returns process mutation kind.
    pub const fn kind(&self) -> ProcessMutationKind {
        self.kind
    }
    /// Returns fencing token.
    pub const fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }
    /// Returns exact semantic payload digest.
    pub const fn payload_digest(&self) -> PayloadDigest {
        self.payload_digest
    }
    /// Returns the opaque external dispatch binding, if present.
    pub const fn dispatch_binding(&self) -> Option<&RuntimeDispatchBinding> {
        self.dispatch_binding.as_ref()
    }
    /// Returns durable acceptance time.
    pub const fn accepted_at(&self) -> TimestampMillis {
        self.accepted_at
    }

    /// Builds the record an adapter must commit before invoking the mutation.
    pub fn accepted_attempt(&self) -> OperationAttempt {
        OperationAttempt::accepted_with_dispatch(
            self.attempt_id.clone(),
            self.target_session.clone(),
            self.kind,
            self.fencing_token,
            self.payload_digest,
            self.dispatch_binding.clone(),
            self.accepted_at,
        )
    }

    /// Reconstructs runtime context from a server-admitted durable attempt.
    pub fn from_attempt(attempt: &OperationAttempt) -> Self {
        Self {
            attempt_id: attempt.id().clone(),
            target_session: attempt.target_session().cloned(),
            kind: attempt.kind(),
            fencing_token: attempt.fencing_token(),
            payload_digest: attempt.payload_digest(),
            dispatch_binding: attempt.dispatch_binding().cloned(),
            accepted_at: attempt.accepted_at(),
        }
    }
}

/// Fence-free admission request evaluated atomically by the product store.
///
/// Callers may select a stable attempt identity for retries, but the server
/// computes the semantic payload digest and the store allocates the next
/// per-target fencing token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationAttemptAdmission {
    attempt_id: OperationAttemptId,
    target_session: Option<SessionId>,
    kind: ProcessMutationKind,
    payload_digest: PayloadDigest,
    supplied_fencing_token: Option<FencingToken>,
    dispatch_binding: Option<RuntimeDispatchBinding>,
    accepted_at: TimestampMillis,
}

impl OperationAttemptAdmission {
    /// Constructs a fence-free server admission request.
    pub const fn new(
        attempt_id: OperationAttemptId,
        target_session: Option<SessionId>,
        kind: ProcessMutationKind,
        payload_digest: PayloadDigest,
        accepted_at: TimestampMillis,
    ) -> Self {
        Self {
            attempt_id,
            target_session,
            kind,
            payload_digest,
            supplied_fencing_token: None,
            dispatch_binding: None,
            accepted_at,
        }
    }

    /// Constructs an externally fenced admission with a complete immutable binding.
    pub const fn dispatched(
        attempt_id: OperationAttemptId,
        target_session: Option<SessionId>,
        kind: ProcessMutationKind,
        fencing_token: FencingToken,
        payload_digest: PayloadDigest,
        dispatch_binding: RuntimeDispatchBinding,
        accepted_at: TimestampMillis,
    ) -> Self {
        Self {
            attempt_id,
            target_session,
            kind,
            payload_digest,
            supplied_fencing_token: Some(fencing_token),
            dispatch_binding: Some(dispatch_binding),
            accepted_at,
        }
    }

    /// Returns the stable retry identity.
    pub const fn attempt_id(&self) -> &OperationAttemptId {
        &self.attempt_id
    }
    /// Returns the target whose fence is allocated atomically.
    pub const fn target_session(&self) -> Option<&SessionId> {
        self.target_session.as_ref()
    }
    /// Returns the process-affecting operation classification.
    pub const fn kind(&self) -> ProcessMutationKind {
        self.kind
    }
    /// Returns the server-computed semantic payload digest.
    pub const fn payload_digest(&self) -> PayloadDigest {
        self.payload_digest
    }
    /// Returns the product-clock acceptance timestamp.
    pub const fn accepted_at(&self) -> TimestampMillis {
        self.accepted_at
    }

    /// Returns the caller-supplied fence only for a complete external dispatch.
    pub const fn supplied_fencing_token(&self) -> Option<FencingToken> {
        self.supplied_fencing_token
    }

    /// Returns the complete opaque external dispatch binding, if present.
    pub const fn dispatch_binding(&self) -> Option<&RuntimeDispatchBinding> {
        self.dispatch_binding.as_ref()
    }

    /// Retargets admission to the product-allocated session without changing dispatch identity.
    pub fn for_target_session(&self, target_session: SessionId) -> Self {
        let mut admission = self.clone();
        admission.target_session = Some(target_session);
        admission
    }

    /// Builds the exact caller-fenced record, or `None` for server-fenced admission.
    pub fn supplied_attempt(&self) -> Option<OperationAttempt> {
        Some(OperationAttempt::accepted_with_dispatch(
            self.attempt_id.clone(),
            self.target_session.clone(),
            self.kind,
            self.supplied_fencing_token?,
            self.payload_digest,
            self.dispatch_binding.clone(),
            self.accepted_at,
        ))
    }

    /// Compares retry semantics while deliberately excluding the allocated fence.
    pub fn has_same_semantics(&self, attempt: &OperationAttempt) -> bool {
        self.attempt_id == *attempt.id()
            && self.target_session.as_ref() == attempt.target_session()
            && self.kind == attempt.kind()
            && self.payload_digest == attempt.payload_digest()
            && self
                .supplied_fencing_token
                .is_none_or(|fence| fence == attempt.fencing_token())
            && self.dispatch_binding.as_ref() == attempt.dispatch_binding()
    }

    /// Compares a boundary dispatch with a durable record whose target was allocated later.
    pub fn has_same_dispatched_semantics(&self, attempt: &OperationAttempt) -> bool {
        self.attempt_id == *attempt.id()
            && self.kind == attempt.kind()
            && self.payload_digest == attempt.payload_digest()
            && self.supplied_fencing_token == Some(attempt.fencing_token())
            && self.dispatch_binding.as_ref() == attempt.dispatch_binding()
    }
}

/// One exact durable attempt that a bounded scan must omit from its result and total.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationAttemptScanExclusion {
    attempt_id: OperationAttemptId,
    fencing_token: FencingToken,
}

impl OperationAttemptScanExclusion {
    /// Constructs an exact attempt-and-fence exclusion.
    pub const fn new(attempt_id: OperationAttemptId, fencing_token: FencingToken) -> Self {
        Self {
            attempt_id,
            fencing_token,
        }
    }

    /// Returns the stable attempt identity.
    pub const fn attempt_id(&self) -> &OperationAttemptId {
        &self.attempt_id
    }

    /// Returns the exact fencing token.
    pub const fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }
}

/// Resource boundary for recovery admission. Session checks include host-wide debt.
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub enum OperationAttemptScope {
    /// Every attempt, used for reconciliation.
    #[default]
    All,
    /// Only attempts that affect the whole host, used before creating a new session.
    Host,
    /// This session and attempts that affect the whole host.
    Session(SessionId),
}

impl OperationAttemptScope {
    /// Whether an attempt can affect this admission scope.
    pub fn includes(&self, attempt: &OperationAttempt) -> bool {
        self.includes_target(attempt.target_session())
    }

    /// Whether a target belongs to this boundary, including host-wide attempts.
    pub fn includes_target(&self, target: Option<&SessionId>) -> bool {
        match self {
            Self::All => true,
            Self::Host => target.is_none(),
            Self::Session(session) => target.is_none_or(|target| target == session),
        }
    }
}

/// Stable keyset position for a bounded recovery sweep.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationAttemptScanCursor {
    /// Original admission timestamp.
    pub accepted_at: TimestampMillis,
    /// Tie-breaker for equal timestamps.
    pub attempt_id: OperationAttemptId,
}

/// Bounded operation-attempt scan with exact exclusions that do not consume the result limit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationAttemptScan {
    limit: usize,
    exclusions: Vec<OperationAttemptScanExclusion>,
    scope: OperationAttemptScope,
    skip_input: bool,
    after: Option<OperationAttemptScanCursor>,
}

impl OperationAttemptScan {
    /// Constructs a bounded scan and rejects an unrepresentable exclusion set.
    pub fn new(
        limit: usize,
        exclusions: Vec<OperationAttemptScanExclusion>,
    ) -> Result<Self, InvalidOperationAttemptScan> {
        if limit == 0 {
            return Err(InvalidOperationAttemptScan::ZeroLimit);
        }
        if exclusions.len() > limit {
            return Err(InvalidOperationAttemptScan::TooManyExclusions {
                limit,
                exclusions: exclusions.len(),
            });
        }
        for (index, exclusion) in exclusions.iter().enumerate() {
            if exclusions[..index].contains(exclusion) {
                return Err(InvalidOperationAttemptScan::DuplicateExclusion);
            }
        }
        Ok(Self {
            limit,
            exclusions,
            scope: OperationAttemptScope::All,
            skip_input: false,
            after: None,
        })
    }

    /// Constructs a bounded scan without exclusions.
    pub fn without_exclusions(limit: usize) -> Result<Self, InvalidOperationAttemptScan> {
        Self::new(limit, Vec::new())
    }

    /// Restricts rows before pagination; host-wide attempts remain in session scans.
    pub fn with_scope(mut self, scope: OperationAttemptScope) -> Self {
        self.scope = scope;
        self
    }

    /// Allows termination to proceed despite input uncertainty; never replays input.
    pub fn without_input_attempts(mut self) -> Self {
        self.skip_input = true;
        self
    }

    /// Whether input attempts are excluded before pagination.
    pub const fn skips_input(&self) -> bool {
        self.skip_input
    }

    /// Returns the requested recovery boundary.
    pub const fn scope(&self) -> &OperationAttemptScope {
        &self.scope
    }

    /// Continues a recovery sweep past previously inspected unknown attempts.
    pub fn after(mut self, cursor: Option<OperationAttemptScanCursor>) -> Self {
        self.after = cursor;
        self
    }

    /// Returns the exclusive recovery sweep position.
    pub const fn cursor(&self) -> Option<&OperationAttemptScanCursor> {
        self.after.as_ref()
    }

    /// Checks scope, input policy, and cursor independently of live exclusions.
    pub fn includes(&self, attempt: &OperationAttempt) -> bool {
        self.scope.includes(attempt)
            && (!self.skip_input || attempt.kind() != ProcessMutationKind::WriteInput)
            && self.after.as_ref().is_none_or(|after| {
                (attempt.accepted_at(), attempt.id()) > (after.accepted_at, &after.attempt_id)
            })
    }

    /// Returns the maximum number of non-excluded results.
    pub const fn limit(&self) -> usize {
        self.limit
    }

    /// Returns exact attempt-and-fence exclusions.
    pub fn exclusions(&self) -> &[OperationAttemptScanExclusion] {
        &self.exclusions
    }
}

/// Invalid operation-attempt scan bounds or exclusions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidOperationAttemptScan {
    /// A scan must request at least one result.
    ZeroLimit,
    /// Exact exclusions cannot exceed the scan's bounded representation.
    TooManyExclusions { limit: usize, exclusions: usize },
    /// The same exact attempt-and-fence pair appeared more than once.
    DuplicateExclusion,
}

impl std::fmt::Display for InvalidOperationAttemptScan {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroLimit => formatter.write_str("operation-attempt scan limit must be non-zero"),
            Self::TooManyExclusions { limit, exclusions } => write!(
                formatter,
                "operation-attempt scan exclusions ({exclusions}) exceed limit ({limit})"
            ),
            Self::DuplicateExclusion => {
                formatter.write_str("operation-attempt scan exclusions contain an exact duplicate")
            }
        }
    }
}

impl std::error::Error for InvalidOperationAttemptScan {}

/// Constructs a server-owned admission for an internal call path.
///
/// A supplied retry identity remains stable. Otherwise identity is derived from
/// the semantic payload digest and product-clock admission time. Equal inputs
/// at an equal timestamp produce the same identity; this helper adds no nonce.
/// The payload digest itself covers only semantic operation input.
pub fn process_mutation_admission(
    requested_attempt_id: Option<OperationAttemptId>,
    target_session: SessionId,
    kind: ProcessMutationKind,
    semantic_payload: &[u8],
    accepted_at: TimestampMillis,
) -> OperationAttemptAdmission {
    let target = target_session.to_string();
    let payload_digest = PayloadDigest::of_slices([
        kind.as_str().as_bytes(),
        target.as_bytes(),
        semantic_payload,
    ]);
    let timestamp = format!("{accepted_at:?}");
    let identity_digest =
        PayloadDigest::of_slices([&payload_digest.as_bytes()[..], timestamp.as_bytes()]);
    let attempt_id =
        requested_attempt_id.unwrap_or_else(|| OperationAttemptId::derived(kind, identity_digest));
    OperationAttemptAdmission::new(
        attempt_id,
        Some(target_session),
        kind,
        payload_digest,
        accepted_at,
    )
}

/// Atomic durable acceptance decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationAttemptAcceptance {
    /// New attempt was durably accepted.
    Accepted(OperationAttempt),
    /// Exact accepted attempt already exists.
    Duplicate(OperationAttempt),
    /// Exact attempt already has a durable settlement.
    AlreadySettled(OperationAttempt),
    /// Exact attempt was already terminalized after superseding compensation.
    AlreadyCompensated(OperationAttempt),
    /// Fence is older than durable target state.
    StaleFence { current: FencingToken },
    /// Attempt identity was reused for different semantics.
    PayloadConflict,
}

/// Requested durable settlement after a side-effect boundary returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationAttemptSettlement {
    /// Attempt identity.
    pub attempt_id: OperationAttemptId,
    /// Fence used to invoke the effect.
    pub fencing_token: FencingToken,
    /// Knowledge about the effect.
    pub certainty: EffectCertainty,
    /// Exact result digest when known applied.
    pub result_digest: Option<PayloadDigest>,
    /// Settlement observation time.
    pub settled_at: TimestampMillis,
}

/// Requested resolution of a previously unknown effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationAttemptReconciliation {
    /// Attempt identity.
    pub attempt_id: OperationAttemptId,
    /// Original fence.
    pub fencing_token: FencingToken,
    /// Newly established known certainty.
    pub certainty: EffectCertainty,
    /// Exact result digest when known applied.
    pub result_digest: Option<PayloadDigest>,
    /// Reconciliation observation time.
    pub reconciled_at: TimestampMillis,
}

/// Atomic durable settlement or reconciliation decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationAttemptSettlementOutcome {
    /// New settlement committed.
    Settled(OperationAttempt),
    /// Exact settlement was already durable.
    Duplicate(OperationAttempt),
    /// Fence is older than durable target state.
    StaleFence { current: FencingToken },
    /// Attempt is absent.
    NotFound,
    /// Settlement conflicts with accepted semantics or prior result.
    PayloadConflict,
}

/// Audited terminalization request after exact cleanup of a superseded dispatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationAttemptCompensation {
    attempt_id: OperationAttemptId,
    fencing_token: FencingToken,
    superseding_fencing_token: FencingToken,
    cleanup_digest: PayloadDigest,
    compensated_at: TimestampMillis,
}

impl OperationAttemptCompensation {
    /// Constructs an exact compensation commit request.
    pub const fn new(
        attempt_id: OperationAttemptId,
        fencing_token: FencingToken,
        superseding_fencing_token: FencingToken,
        cleanup_digest: PayloadDigest,
        compensated_at: TimestampMillis,
    ) -> Self {
        Self {
            attempt_id,
            fencing_token,
            superseding_fencing_token,
            cleanup_digest,
            compensated_at,
        }
    }

    pub const fn attempt_id(&self) -> &OperationAttemptId {
        &self.attempt_id
    }

    pub const fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }

    pub const fn superseding_fencing_token(&self) -> FencingToken {
        self.superseding_fencing_token
    }

    pub const fn cleanup_digest(&self) -> PayloadDigest {
        self.cleanup_digest
    }

    pub const fn compensated_at(&self) -> TimestampMillis {
        self.compensated_at
    }
}

/// Atomic durable superseded-compensation decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationAttemptCompensationOutcome {
    /// New terminal compensation audit committed.
    Compensated(OperationAttempt),
    /// The exact compensation audit was already durable.
    Duplicate(OperationAttempt),
    /// A still-newer same-cell fence superseded the supplied proof.
    StaleFence { current: FencingToken },
    /// Attempt is absent.
    NotFound,
    /// Attempt is terminal in a different disposition.
    NotAccepted,
    /// Attempt, fence, or cleanup evidence conflicts with durable state.
    PayloadConflict,
}

/// Durable exact-target intent committed before destructive dispatch cleanup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationAttemptCompensationPreparation {
    attempt_id: OperationAttemptId,
    fencing_token: FencingToken,
    superseding_fencing_token: FencingToken,
    process: ProcessIdentity,
    session_revision: Revision,
    cleanup_digest: PayloadDigest,
    prepared_at: TimestampMillis,
}

impl OperationAttemptCompensationPreparation {
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        attempt_id: OperationAttemptId,
        fencing_token: FencingToken,
        superseding_fencing_token: FencingToken,
        process: ProcessIdentity,
        session_revision: Revision,
        cleanup_digest: PayloadDigest,
        prepared_at: TimestampMillis,
    ) -> Self {
        Self {
            attempt_id,
            fencing_token,
            superseding_fencing_token,
            process,
            session_revision,
            cleanup_digest,
            prepared_at,
        }
    }

    pub const fn attempt_id(&self) -> &OperationAttemptId {
        &self.attempt_id
    }

    pub const fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }

    pub const fn superseding_fencing_token(&self) -> FencingToken {
        self.superseding_fencing_token
    }

    pub const fn process(&self) -> &ProcessIdentity {
        &self.process
    }

    pub const fn session_revision(&self) -> Revision {
        self.session_revision
    }

    pub const fn cleanup_digest(&self) -> PayloadDigest {
        self.cleanup_digest
    }

    pub const fn prepared_at(&self) -> TimestampMillis {
        self.prepared_at
    }
}

/// Atomic durable compensation-intent decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationAttemptCompensationPreparationOutcome {
    Prepared(OperationAttempt),
    Duplicate(OperationAttempt),
    StaleFence { current: FencingToken },
    NotFound,
    NotAccepted,
    PayloadConflict,
}

#[derive(Debug)]
struct CompensationUnsupported;

impl std::fmt::Display for CompensationUnsupported {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("operation-attempt compensation is unavailable")
    }
}

impl std::error::Error for CompensationUnsupported {}

/// Host proof that a session has exited and this input attempt has no executing worker.
/// This proves quiescence only; it does not prove whether input was delivered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputAttemptRetirementEvidence {
    /// Confirmed session exit witness; legacy input receipts do not bind an incarnation.
    pub process_id: undercurrent_domain::ProcessInstanceId,
    pub exited_at: TimestampMillis,
}

/// Audited withdrawal of an exited session's input attempt from admission blocking.
/// The stored attempt remains Unknown and cannot be replayed after retirement.
#[derive(Clone, Debug)]
pub struct InputAttemptRetirement {
    pub attempt_id: OperationAttemptId,
    pub fencing_token: FencingToken,
    pub session_revision: Revision,
    pub evidence: InputAttemptRetirementEvidence,
    pub retired_at: TimestampMillis,
}

/// Product-owned persistence boundary for accepted and settled attempts.
pub trait OperationAttemptStore: Send + Sync {
    /// Retires only Unknown standalone input for an unchanged terminal session.
    /// Atomically retain the session snapshot and host proof without changing certainty.
    /// Returns false for stale/inapplicable evidence or an unsupported adapter.
    /// The caller must hold the session mutation lease from its terminal-state,
    /// process-directory, and host observations through this commit. A matching
    /// revision alone does not exclude an in-flight replacement process.
    fn retire_input<'a>(
        &'a self,
        _retirement: &'a InputAttemptRetirement,
    ) -> PortFuture<'a, Result<bool, PortError>> {
        Box::pin(async { Ok(false) })
    }

    /// Lists committed retirements whose host evidence still needs acknowledgement.
    fn pending_input_retirement_acks(
        &self,
        _limit: usize,
    ) -> PortFuture<'_, Result<Vec<OperationAttempt>, PortError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    /// Marks the exact retirement acknowledged after the host releases its receipt.
    fn acknowledge_input_retirement<'a>(
        &'a self,
        _attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async { Ok(()) })
    }

    /// Atomically allocates the next per-target fence and accepts the attempt.
    fn accept_next<'a>(
        &'a self,
        admission: &'a OperationAttemptAdmission,
    ) -> PortFuture<'a, Result<OperationAttemptAcceptance, PortError>>;

    /// Atomically accepts or classifies an attempt before external IO.
    fn accept<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<OperationAttemptAcceptance, PortError>>;

    /// Atomically settles an accepted attempt.
    fn settle<'a>(
        &'a self,
        settlement: &'a OperationAttemptSettlement,
    ) -> PortFuture<'a, Result<OperationAttemptSettlementOutcome, PortError>>;

    /// Atomically prepares an exact cleanup target under the current higher cell fence.
    fn prepare_compensation<'a>(
        &'a self,
        _preparation: &'a OperationAttemptCompensationPreparation,
    ) -> PortFuture<'a, Result<OperationAttemptCompensationPreparationOutcome, PortError>> {
        Box::pin(async {
            Err(PortError::new(
                crate::PortErrorKind::Other,
                crate::PortOperation::PrepareOperationAttemptCompensation,
                None,
                CompensationUnsupported,
            ))
        })
    }

    /// Atomically terminalizes an exact accepted dispatch only under the current higher cell fence.
    fn compensate<'a>(
        &'a self,
        _compensation: &'a OperationAttemptCompensation,
    ) -> PortFuture<'a, Result<OperationAttemptCompensationOutcome, PortError>> {
        Box::pin(async {
            Err(PortError::new(
                crate::PortErrorKind::Other,
                crate::PortOperation::CompensateOperationAttempt,
                None,
                CompensationUnsupported,
            ))
        })
    }

    /// Resolves a previously unknown effect.
    fn reconcile<'a>(
        &'a self,
        reconciliation: &'a OperationAttemptReconciliation,
    ) -> PortFuture<'a, Result<OperationAttemptSettlementOutcome, PortError>>;

    /// Loads one durable attempt.
    fn load<'a>(
        &'a self,
        attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<Option<OperationAttempt>, PortError>>;

    /// Reads pending attempts in ascending `(accepted_at, attempt_id)` order.
    /// Apply scope, cursor, input policy, and live exclusions before the result limit.
    /// Audited input retirements are not pending, although their certainty remains Unknown.
    fn pending_reconciliation<'a>(
        &'a self,
        scan: &'a OperationAttemptScan,
    ) -> PortFuture<'a, Result<Vec<OperationAttempt>, PortError>>;
}

/// Adapter observation used to reconcile an unknown external effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservedOperationAttemptEffect {
    /// Newly established effect certainty.
    pub certainty: EffectCertainty,
    /// Exact terminal result digest when known applied.
    pub result_digest: Option<PayloadDigest>,
}

/// Trusted retained proof that an accepted dispatched incarnation is exactly absent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedOperationAttemptCompensation {
    attempt_id: OperationAttemptId,
    fencing_token: FencingToken,
    superseding_fencing_token: FencingToken,
    cleanup_digest: PayloadDigest,
}

impl ObservedOperationAttemptCompensation {
    /// Constructs proof produced by a runtime/session/binding observer, never by the peer.
    pub const fn new(
        attempt_id: OperationAttemptId,
        fencing_token: FencingToken,
        superseding_fencing_token: FencingToken,
        cleanup_digest: PayloadDigest,
    ) -> Self {
        Self {
            attempt_id,
            fencing_token,
            superseding_fencing_token,
            cleanup_digest,
        }
    }

    pub const fn attempt_id(&self) -> &OperationAttemptId {
        &self.attempt_id
    }

    pub const fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }

    pub const fn superseding_fencing_token(&self) -> FencingToken {
        self.superseding_fencing_token
    }

    pub const fn cleanup_digest(&self) -> PayloadDigest {
        self.cleanup_digest
    }
}

/// Observes retained runtime/job/process truth without exposing adapter types.
pub trait OperationAttemptEffectObserver: Send + Sync {
    /// Supplies quiescence proof only for an exact retained Unknown input receipt.
    /// Missing receipts, live/replacement processes, and executing workers cannot qualify.
    fn observe_input_retirement<'a>(
        &'a self,
        _attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<Option<InputAttemptRetirementEvidence>, PortError>> {
        Box::pin(async { Ok(None) })
    }

    /// Cancels an uncommitted shutdown preparation without retiring any host.
    ///
    /// Shutdown's destructive step is its post-settlement acknowledgement. An
    /// abandoned Accepted/Unknown attempt has not authorized that step. Return
    /// true only after durably preventing this exact preparation from replaying.
    /// Live attempts must be excluded by the attempt store before recovery scans.
    fn cancel_uncommitted_shutdown<'a>(
        &'a self,
        _attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<bool, PortError>> {
        Box::pin(async { Ok(false) })
    }

    /// Releases a cancellation receipt after KnownAbsent is durably committed.
    /// This must never send the acknowledgement that retires a process host.
    fn acknowledge_shutdown_cancellation<'a>(
        &'a self,
        _attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async { Ok(()) })
    }

    /// Inspects one exact accepted attempt after response loss or daemon restart.
    fn observe<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<ObservedOperationAttemptEffect, PortError>>;

    /// Releases retained adapter evidence only after product reconciliation commits.
    ///
    /// Implementations must make acknowledgement idempotent.
    fn acknowledge_observation<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>>;

    /// Returns exact durable absence evidence for an Accepted superseded dispatch.
    ///
    /// `None` is deliberately inconclusive and leaves the attempt unresolved.
    fn observe_compensation<'a>(
        &'a self,
        _attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<Option<ObservedOperationAttemptCompensation>, PortError>> {
        Box::pin(async { Ok(None) })
    }

    /// Releases retained cleanup evidence only after the compensation audit commits.
    fn acknowledge_compensation<'a>(
        &'a self,
        _attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async { Ok(()) })
    }
}

/// Bounded startup/retry reconciliation limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationAttemptReconciliationLimits {
    batch_size: usize,
    max_batches: usize,
}

impl OperationAttemptReconciliationLimits {
    /// Constructs non-zero bounded scan limits.
    pub fn new(
        batch_size: usize,
        max_batches: usize,
    ) -> Result<Self, InvalidAttemptReconciliationLimits> {
        if batch_size == 0 || max_batches == 0 {
            return Err(InvalidAttemptReconciliationLimits);
        }
        Ok(Self {
            batch_size,
            max_batches,
        })
    }

    /// Returns the maximum store page size.
    pub const fn batch_size(self) -> usize {
        self.batch_size
    }
    /// Returns the maximum pages inspected in one readiness cycle.
    pub const fn max_batches(self) -> usize {
        self.max_batches
    }
}

/// Invalid zero reconciliation bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidAttemptReconciliationLimits;

impl std::fmt::Display for InvalidAttemptReconciliationLimits {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("operation-attempt reconciliation limits must be non-zero")
    }
}

impl std::error::Error for InvalidAttemptReconciliationLimits {}

/// Payload-safe result of one bounded reconciliation cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationAttemptReconciliationReport {
    /// Attempts inspected.
    pub inspected: usize,
    /// Unknown effects resolved to known truth.
    pub resolved: usize,
    /// Stable attempts that remain unknown.
    pub unresolved: Vec<OperationAttemptId>,
    /// Whether no pending work remains within the bounded scan.
    pub complete: bool,
}

/// Failure from bounded durable attempt reconciliation.
#[derive(Debug)]
pub enum ReconcileOperationAttemptsError {
    /// Store or observation adapter failed.
    Port(PortError),
    /// Adapter returned more rows than requested or a non-pending attempt.
    InvalidBatch,
    /// Store rejected a newly established reconciliation result.
    ReconciliationRejected,
}

impl std::fmt::Display for ReconcileOperationAttemptsError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Port(_) => "operation-attempt reconciliation adapter failed",
            Self::InvalidBatch => "operation-attempt reconciliation batch violated its contract",
            Self::ReconciliationRejected => "operation-attempt reconciliation was rejected",
        })
    }
}

impl std::error::Error for ReconcileOperationAttemptsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Port(error) => Some(error),
            Self::InvalidBatch | Self::ReconciliationRejected => None,
        }
    }
}

/// Reconciles retained unknown effects for startup and bounded background recovery.
pub struct ReconcileOperationAttemptsService<'a> {
    attempts: &'a dyn OperationAttemptStore,
    observer: &'a dyn OperationAttemptEffectObserver,
    clock: &'a dyn crate::Clock,
    limits: OperationAttemptReconciliationLimits,
}

impl<'a> ReconcileOperationAttemptsService<'a> {
    /// Constructs the bounded application use case.
    pub const fn new(
        attempts: &'a dyn OperationAttemptStore,
        observer: &'a dyn OperationAttemptEffectObserver,
        clock: &'a dyn crate::Clock,
        limits: OperationAttemptReconciliationLimits,
    ) -> Self {
        Self {
            attempts,
            observer,
            clock,
            limits,
        }
    }

    /// Resolves known retained effects and reports unknown truth without guessing.
    pub async fn execute(
        &self,
    ) -> Result<OperationAttemptReconciliationReport, ReconcileOperationAttemptsError> {
        self.execute_inner(&mut None, false).await
    }

    /// Advances a bounded background sweep even when an earlier page stays Unknown.
    /// The cursor resets at the end so unresolved attempts are revisited on the next sweep.
    pub async fn execute_from(
        &self,
        cursor: &mut Option<OperationAttemptScanCursor>,
    ) -> Result<OperationAttemptReconciliationReport, ReconcileOperationAttemptsError> {
        self.execute_inner(cursor, true).await
    }

    async fn execute_inner(
        &self,
        cursor: &mut Option<OperationAttemptScanCursor>,
        advance: bool,
    ) -> Result<OperationAttemptReconciliationReport, ReconcileOperationAttemptsError> {
        let started_at_beginning = cursor.is_none();
        let mut scan = OperationAttemptScan::without_exclusions(self.limits.batch_size)
            .map_err(|_| ReconcileOperationAttemptsError::InvalidBatch)?
            .after(cursor.clone());
        let mut report = OperationAttemptReconciliationReport {
            inspected: 0,
            resolved: 0,
            unresolved: Vec::new(),
            complete: false,
        };
        for _ in 0..self.limits.max_batches {
            let pending = self
                .attempts
                .pending_reconciliation(&scan)
                .await
                .map_err(ReconcileOperationAttemptsError::Port)?;
            if pending.len() > self.limits.batch_size
                || pending.windows(2).any(|pair| {
                    (pair[0].accepted_at(), pair[0].id()) >= (pair[1].accepted_at(), pair[1].id())
                })
                || pending.iter().any(|attempt| {
                    !scan.includes(attempt)
                        || !matches!(
                            attempt.state(),
                            OperationAttemptState::Accepted
                                | OperationAttemptState::Settled {
                                    certainty: EffectCertainty::Unknown,
                                    ..
                                }
                        )
                })
            {
                return Err(ReconcileOperationAttemptsError::InvalidBatch);
            }
            if pending.is_empty() {
                report.complete = started_at_beginning && report.unresolved.is_empty();
                *cursor = None;
                return Ok(report);
            }
            if advance {
                let last = pending.last().expect("nonempty batch");
                *cursor = Some(OperationAttemptScanCursor {
                    accepted_at: last.accepted_at(),
                    attempt_id: last.id().clone(),
                });
                scan = scan.after(cursor.clone());
            }
            let mut progress = false;
            for attempt in pending {
                report.inspected = report.inspected.saturating_add(1);
                if attempt.kind() == ProcessMutationKind::Shutdown {
                    if self.recover_shutdown(&attempt).await? {
                        report.resolved = report.resolved.saturating_add(1);
                        progress = true;
                    } else if !report.unresolved.contains(attempt.id()) {
                        report.unresolved.push(attempt.id().clone());
                    }
                    continue;
                }
                if matches!(attempt.state(), OperationAttemptState::Accepted) {
                    let Some(observed) = self
                        .observer
                        .observe_compensation(&attempt)
                        .await
                        .map_err(ReconcileOperationAttemptsError::Port)?
                    else {
                        if !report.unresolved.contains(attempt.id()) {
                            report.unresolved.push(attempt.id().clone());
                        }
                        continue;
                    };
                    if observed.attempt_id() != attempt.id()
                        || observed.fencing_token() != attempt.fencing_token()
                    {
                        return Err(ReconcileOperationAttemptsError::ReconciliationRejected);
                    }
                    let outcome = self
                        .attempts
                        .compensate(&OperationAttemptCompensation::new(
                            attempt.id().clone(),
                            attempt.fencing_token(),
                            observed.superseding_fencing_token(),
                            observed.cleanup_digest(),
                            self.clock.now(),
                        ))
                        .await
                        .map_err(ReconcileOperationAttemptsError::Port)?;
                    match outcome {
                        OperationAttemptCompensationOutcome::Compensated(committed)
                        | OperationAttemptCompensationOutcome::Duplicate(committed) => {
                            self.observer
                                .acknowledge_compensation(&committed)
                                .await
                                .map_err(ReconcileOperationAttemptsError::Port)?;
                            report.resolved = report.resolved.saturating_add(1);
                            progress = true;
                        }
                        OperationAttemptCompensationOutcome::StaleFence { .. }
                        | OperationAttemptCompensationOutcome::NotFound
                        | OperationAttemptCompensationOutcome::NotAccepted
                        | OperationAttemptCompensationOutcome::PayloadConflict => {
                            return Err(ReconcileOperationAttemptsError::ReconciliationRejected);
                        }
                    }
                    continue;
                }
                let observed = self
                    .observer
                    .observe(&attempt)
                    .await
                    .map_err(ReconcileOperationAttemptsError::Port)?;
                if observed.certainty == EffectCertainty::Unknown {
                    if !report.unresolved.contains(attempt.id()) {
                        report.unresolved.push(attempt.id().clone());
                    }
                    continue;
                }
                let outcome = self
                    .attempts
                    .reconcile(&OperationAttemptReconciliation {
                        attempt_id: attempt.id().clone(),
                        fencing_token: attempt.fencing_token(),
                        certainty: observed.certainty,
                        result_digest: observed.result_digest,
                        reconciled_at: self.clock.now(),
                    })
                    .await
                    .map_err(ReconcileOperationAttemptsError::Port)?;
                match outcome {
                    OperationAttemptSettlementOutcome::Settled(committed)
                    | OperationAttemptSettlementOutcome::Duplicate(committed) => {
                        self.observer
                            .acknowledge_observation(&committed)
                            .await
                            .map_err(ReconcileOperationAttemptsError::Port)?;
                        report.resolved = report.resolved.saturating_add(1);
                        progress = true;
                    }
                    OperationAttemptSettlementOutcome::StaleFence { .. }
                    | OperationAttemptSettlementOutcome::NotFound
                    | OperationAttemptSettlementOutcome::PayloadConflict => {
                        return Err(ReconcileOperationAttemptsError::ReconciliationRejected);
                    }
                }
            }
            if !progress && !advance {
                return Ok(report);
            }
        }
        Ok(report)
    }

    async fn recover_shutdown(
        &self,
        attempt: &OperationAttempt,
    ) -> Result<bool, ReconcileOperationAttemptsError> {
        if !self
            .observer
            .cancel_uncommitted_shutdown(attempt)
            .await
            .map_err(ReconcileOperationAttemptsError::Port)?
        {
            return Ok(false);
        }
        let outcome = if matches!(attempt.state(), OperationAttemptState::Accepted) {
            self.attempts
                .settle(&OperationAttemptSettlement {
                    attempt_id: attempt.id().clone(),
                    fencing_token: attempt.fencing_token(),
                    certainty: EffectCertainty::KnownAbsent,
                    result_digest: None,
                    settled_at: self.clock.now(),
                })
                .await
        } else {
            self.attempts
                .reconcile(&OperationAttemptReconciliation {
                    attempt_id: attempt.id().clone(),
                    fencing_token: attempt.fencing_token(),
                    certainty: EffectCertainty::KnownAbsent,
                    result_digest: None,
                    reconciled_at: self.clock.now(),
                })
                .await
        }
        .map_err(ReconcileOperationAttemptsError::Port)?;
        match outcome {
            OperationAttemptSettlementOutcome::Settled(committed)
            | OperationAttemptSettlementOutcome::Duplicate(committed) => {
                self.observer
                    .acknowledge_shutdown_cancellation(&committed)
                    .await
                    .map_err(ReconcileOperationAttemptsError::Port)?;
                Ok(true)
            }
            _ => Err(ReconcileOperationAttemptsError::ReconciliationRejected),
        }
    }
}
