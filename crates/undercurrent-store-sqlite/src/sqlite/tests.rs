// SPDX-License-Identifier: Apache-2.0

//! Compensation tests for durable intent, returned-error rollback, reopen,
//! terminal audit, and readiness. These use temporary databases and simulated
//! evidence; they do not perform or prove a real process cleanup effect.

use std::future::Future;
use std::path::Path;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use rusqlite::Connection;
use undercurrent_application::{
    OperationAttemptAcceptance, OperationAttemptCompensation, OperationAttemptCompensationOutcome,
    OperationAttemptCompensationPreparation, OperationAttemptCompensationPreparationOutcome,
    OperationAttemptReadinessService, OperationAttemptScan, OperationAttemptSettlement,
    OperationAttemptSettlementOutcome, OperationAttemptStore,
};
use undercurrent_domain::{
    DispatchCellIdentity, EffectCertainty, FencingToken, OperationAttempt, OperationAttemptId,
    OperationAttemptState, PayloadDigest, ProcessIdentity, ProcessInstanceId, ProcessMutationKind,
    Revision, RuntimeCallbackRef, RuntimeDispatchBinding, RuntimeDispatchLimits,
    RuntimeDispatchPolicy, RuntimeOutputContract, RuntimeTelemetryPolicyRef, SessionId,
    TimestampMillis,
};
use undercurrent_foundation::Classification;

use super::{
    SqliteCrashInjector, SqliteCrashPoint, SqliteRepositoryConfig, SqliteRepositoryLimits,
    SqliteSessionRepository,
};

#[test]
fn compensated_dispatch_is_atomic_terminal_audited_and_restart_safe() {
    for point in [
        SqliteCrashPoint::AttemptCompensateAfterWrite,
        SqliteCrashPoint::AttemptCompensateBeforeCommit,
    ] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let database_path = directory
            .path()
            .join(format!("compensate-{point:?}.sqlite3"));
        let repository = open_with_crash(&database_path, point);
        let low = dispatched_attempt("dispatch-low", 7, b"low input");
        let high = dispatched_attempt("dispatch-high", 8, b"high input");
        accept(&repository, &low);
        accept(&repository, &high);
        let high_result = PayloadDigest::of_bytes(b"high canonical process");
        assert!(matches!(
            block_on(OperationAttemptStore::settle(
                &repository,
                &OperationAttemptSettlement {
                    attempt_id: high.id().clone(),
                    fencing_token: high.fencing_token(),
                    certainty: EffectCertainty::KnownApplied,
                    result_digest: Some(high_result),
                    settled_at: TimestampMillis::new(30),
                },
            ))
            .expect("settle high dispatch"),
            OperationAttemptSettlementOutcome::Settled(_)
        ));

        let cleanup_digest = PayloadDigest::of_bytes(b"exact low process cleanup evidence");
        let cleanup_process = ProcessIdentity::new(
            ProcessInstanceId::new("low-process-instance").expect("process identity"),
            TimestampMillis::new(25),
        );
        let preparation = OperationAttemptCompensationPreparation::new(
            low.id().clone(),
            low.fencing_token(),
            high.fencing_token(),
            cleanup_process.clone(),
            Revision::new(12),
            cleanup_digest,
            TimestampMillis::new(35),
        );
        assert!(matches!(
            block_on(OperationAttemptStore::prepare_compensation(
                &repository,
                &preparation,
            ))
            .expect("prepare exact compensation"),
            OperationAttemptCompensationPreparationOutcome::Prepared(attempt)
                if attempt.id() == low.id()
        ));
        let latest = dispatched_attempt("dispatch-latest", 9, b"latest input");
        accept(&repository, &latest);
        let latest_result = PayloadDigest::of_bytes(b"latest canonical process");
        assert!(matches!(
            block_on(OperationAttemptStore::settle(
                &repository,
                &OperationAttemptSettlement {
                    attempt_id: latest.id().clone(),
                    fencing_token: latest.fencing_token(),
                    certainty: EffectCertainty::KnownApplied,
                    result_digest: Some(latest_result),
                    settled_at: TimestampMillis::new(38),
                },
            ))
            .expect("settle latest dispatch"),
            OperationAttemptSettlementOutcome::Settled(_)
        ));
        let compensation = OperationAttemptCompensation::new(
            low.id().clone(),
            low.fencing_token(),
            high.fencing_token(),
            cleanup_digest,
            TimestampMillis::new(40),
        );
        assert!(
            block_on(OperationAttemptStore::compensate(
                &repository,
                &compensation,
            ))
            .is_err(),
            "{point:?} must roll back the terminal transition",
        );
        drop(repository);

        let repository = open(&database_path);
        let recovered_low = load(&repository, low.id());
        assert!(matches!(
            recovered_low.state(),
            OperationAttemptState::Accepted
        ));
        let recovered_intent = recovered_low
            .compensation_intent()
            .expect("durable compensation intent");
        assert_eq!(
            recovered_intent.superseding_fencing_token(),
            high.fencing_token()
        );
        assert_eq!(recovered_intent.process(), &cleanup_process);
        assert_eq!(recovered_intent.session_revision(), Revision::new(12));
        assert_eq!(recovered_intent.cleanup_digest(), cleanup_digest);
        assert_eq!(recovered_intent.prepared_at(), TimestampMillis::new(35));
        let recovered_high = load(&repository, high.id());
        assert!(matches!(
            recovered_high.state(),
            OperationAttemptState::Settled {
                certainty: EffectCertainty::KnownApplied,
                result_digest: Some(digest),
                ..
            } if *digest == high_result
        ));
        let recovered_latest = load(&repository, latest.id());
        assert!(matches!(
            recovered_latest.state(),
            OperationAttemptState::Settled {
                certainty: EffectCertainty::KnownApplied,
                result_digest: Some(digest),
                ..
            } if *digest == latest_result
        ));
        let readiness =
            OperationAttemptReadinessService::new(&repository, 16).expect("readiness service");
        assert!(
            !block_on(readiness.inspect())
                .expect("readiness after rollback")
                .complete()
        );
        let pending = block_on(OperationAttemptStore::pending_reconciliation(
            &repository,
            &OperationAttemptScan::without_exclusions(16).expect("attempt scan"),
        ))
        .expect("pending attempts after rollback");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id(), low.id());

        let wrong_higher = OperationAttemptCompensation::new(
            low.id().clone(),
            low.fencing_token(),
            FencingToken::new(9).expect("fencing token"),
            cleanup_digest,
            TimestampMillis::new(40),
        );
        assert_eq!(
            block_on(OperationAttemptStore::compensate(
                &repository,
                &wrong_higher,
            ))
            .expect("reject superseding fence that mismatches the durable intent"),
            OperationAttemptCompensationOutcome::PayloadConflict,
        );
        assert!(matches!(
            block_on(OperationAttemptStore::compensate(
                &repository,
                &compensation,
            ))
            .expect("commit exact compensation"),
            OperationAttemptCompensationOutcome::Compensated(attempt)
                if attempt.id() == low.id()
        ));
        assert!(matches!(
            block_on(OperationAttemptStore::compensate(
                &repository,
                &compensation,
            ))
            .expect("deduplicate exact compensation"),
            OperationAttemptCompensationOutcome::Duplicate(attempt)
                if attempt.id() == low.id()
        ));
        let conflicting_cleanup = OperationAttemptCompensation::new(
            low.id().clone(),
            low.fencing_token(),
            high.fencing_token(),
            PayloadDigest::of_bytes(b"different cleanup evidence"),
            TimestampMillis::new(40),
        );
        assert_eq!(
            block_on(OperationAttemptStore::compensate(
                &repository,
                &conflicting_cleanup,
            ))
            .expect("reject conflicting compensation audit"),
            OperationAttemptCompensationOutcome::PayloadConflict,
        );
        assert!(
            block_on(readiness.inspect())
                .expect("readiness after compensation")
                .complete()
        );
        assert!(
            block_on(OperationAttemptStore::pending_reconciliation(
                &repository,
                &OperationAttemptScan::without_exclusions(16).expect("attempt scan"),
            ))
            .expect("pending attempts after compensation")
            .is_empty()
        );
        drop(repository);

        let repository = open(&database_path);
        let recovered_low = load(&repository, low.id());
        assert!(matches!(
            recovered_low.state(),
            OperationAttemptState::Compensated {
                superseding_fencing_token,
                cleanup_digest: recovered_cleanup,
                compensated_at,
            } if *superseding_fencing_token == high.fencing_token()
                && *recovered_cleanup == cleanup_digest
                && *compensated_at == TimestampMillis::new(40)
        ));
        let recovered_high = load(&repository, high.id());
        assert!(matches!(
            recovered_high.state(),
            OperationAttemptState::Settled {
                certainty: EffectCertainty::KnownApplied,
                result_digest: Some(digest),
                ..
            } if *digest == high_result
        ));
        let recovered_latest = load(&repository, latest.id());
        assert!(matches!(
            recovered_latest.state(),
            OperationAttemptState::Settled {
                certainty: EffectCertainty::KnownApplied,
                result_digest: Some(digest),
                ..
            } if *digest == latest_result
        ));
        let readiness = OperationAttemptReadinessService::new(&repository, 16)
            .expect("reopened readiness service");
        assert!(
            block_on(readiness.inspect())
                .expect("reopened readiness")
                .complete()
        );
        drop(repository);

        let connection = Connection::open(&database_path).expect("open audit database");
        let audit: (i64, Vec<u8>, String, String, i64, Vec<u8>, i64, i64) = connection
            .query_row(
                "SELECT state, superseding_fencing_token, cleanup_digest,
                        compensation_process_id, compensation_process_started_at,
                        compensation_session_revision, compensation_prepared_at, settled_at
                 FROM operation_attempts WHERE attempt_id = ?1",
                [low.id().as_str()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                    ))
                },
            )
            .expect("compensation audit row");
        assert_eq!(audit.0, 4);
        assert_eq!(audit.1, high.fencing_token().value().to_be_bytes());
        assert_eq!(audit.2, cleanup_digest.to_hex());
        assert_eq!(audit.3, cleanup_process.id().as_str());
        assert_eq!(audit.4, cleanup_process.started_at().value());
        assert_eq!(audit.5, Revision::new(12).value().to_be_bytes());
        assert_eq!(audit.6, 35);
        assert_eq!(audit.7, 40);
    }
}

#[test]
fn compensation_preparation_crash_boundaries_roll_back_the_complete_intent() {
    for point in [
        SqliteCrashPoint::AttemptCompensationPrepareAfterWrite,
        SqliteCrashPoint::AttemptCompensationPrepareBeforeCommit,
    ] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let database_path = directory
            .path()
            .join(format!("prepare-compensation-{point:?}.sqlite3"));
        let repository = open_with_crash(&database_path, point);
        let low = dispatched_attempt("prepare-low", 7, b"low input");
        let high = dispatched_attempt("prepare-high", 8, b"high input");
        accept(&repository, &low);
        accept(&repository, &high);
        let preparation = OperationAttemptCompensationPreparation::new(
            low.id().clone(),
            low.fencing_token(),
            high.fencing_token(),
            ProcessIdentity::new(
                ProcessInstanceId::new("prepared-process").expect("process identity"),
                TimestampMillis::new(25),
            ),
            Revision::new(12),
            PayloadDigest::of_bytes(b"cleanup proof"),
            TimestampMillis::new(35),
        );
        assert!(
            block_on(OperationAttemptStore::prepare_compensation(
                &repository,
                &preparation,
            ))
            .is_err()
        );
        drop(repository);

        let repository = open(&database_path);
        let recovered = load(&repository, low.id());
        assert!(matches!(recovered.state(), OperationAttemptState::Accepted));
        assert!(recovered.compensation_intent().is_none());
        assert!(matches!(
            block_on(OperationAttemptStore::prepare_compensation(
                &repository,
                &preparation,
            ))
            .expect("prepare after rollback"),
            OperationAttemptCompensationPreparationOutcome::Prepared(attempt)
                if attempt.id() == low.id()
        ));
    }
}

fn accept(repository: &SqliteSessionRepository, attempt: &OperationAttempt) {
    assert!(matches!(
        block_on(OperationAttemptStore::accept(repository, attempt)).expect("accept attempt"),
        OperationAttemptAcceptance::Accepted(accepted) if accepted.id() == attempt.id()
    ));
}

fn load(repository: &SqliteSessionRepository, attempt_id: &OperationAttemptId) -> OperationAttempt {
    block_on(OperationAttemptStore::load(repository, attempt_id))
        .expect("load attempt")
        .expect("attempt exists")
}

fn dispatched_attempt(id: &str, fence: u64, input: &[u8]) -> OperationAttempt {
    OperationAttempt::accepted_with_dispatch(
        OperationAttemptId::new(id).expect("attempt ID"),
        Some(SessionId::new("dispatch-session").expect("session ID")),
        ProcessMutationKind::Spawn,
        FencingToken::new(fence).expect("fencing token"),
        PayloadDigest::of_bytes(b"canonical dispatched start"),
        Some(RuntimeDispatchBinding::new(
            DispatchCellIdentity::new("cell:build").expect("cell identity"),
            PayloadDigest::of_bytes(b"caller dispatch request"),
            PayloadDigest::of_bytes(b"launch profile"),
            PayloadDigest::of_bytes(b"environment binding"),
            PayloadDigest::of_bytes(input),
            runtime_policy(),
        )),
        TimestampMillis::new(10),
    )
}

fn runtime_policy() -> RuntimeDispatchPolicy {
    RuntimeDispatchPolicy::new(
        RuntimeDispatchLimits::new(30_000, 1_024, 2_048, None, None).expect("limits"),
        RuntimeOutputContract::new(
            "sqlite-test/0.1.0-beta",
            PayloadDigest::of_bytes(b"sqlite test output schema"),
            Classification::Internal,
        )
        .expect("output contract"),
        RuntimeTelemetryPolicyRef::new("trust://telemetry/policies/sqlite-test")
            .expect("telemetry ref"),
        undercurrent_domain::RuntimeRetentionPolicyRef::new(
            "trust://retention/policies/sqlite-test",
        )
        .expect("retention ref"),
        vec![
            RuntimeCallbackRef::new("runtime-callback://example/workflow/v0.1.0-beta/sqlite-test")
                .expect("callback ref"),
        ],
        TimestampMillis::new(10_000),
        TimestampMillis::new(20_000),
    )
    .expect("runtime dispatch policy")
}

fn open(path: &Path) -> SqliteSessionRepository {
    SqliteSessionRepository::open(config(path)).expect("open repository")
}

fn open_with_crash(path: &Path, point: SqliteCrashPoint) -> SqliteSessionRepository {
    SqliteSessionRepository::open_with_crash_injector(config(path), Arc::new(CrashAt(point)))
        .expect("open crash-injected repository")
}

fn config(path: &Path) -> SqliteRepositoryConfig {
    SqliteRepositoryConfig::new(
        path,
        SqliteRepositoryLimits::new(100, 100, 1_048_576, 100, 32).expect("limits"),
        Duration::from_secs(1),
    )
    .expect("configuration")
}

#[derive(Debug)]
struct CrashAt(SqliteCrashPoint);

impl SqliteCrashInjector for CrashAt {
    fn should_crash(&self, point: SqliteCrashPoint) -> bool {
        self.0 == point
    }
}

struct ThreadWake(thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => thread::park(),
        }
    }
}
