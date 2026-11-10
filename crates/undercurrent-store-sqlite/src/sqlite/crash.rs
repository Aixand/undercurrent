// SPDX-License-Identifier: Apache-2.0

//! Deterministic returned-error hooks for transaction and restore recovery tests.
//!
//! A triggered transaction hook returns an error and relies on transaction drop
//! for rollback. The restore hook runs its error rollback path. Neither kills
//! the process or models power loss that bypasses destructors and error handlers.

use std::fmt;

/// Stable write boundaries at which tests may inject a returned store failure.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum SqliteCrashPoint {
    InsertAfterOutbox,
    InsertAfterAggregate,
    InsertAfterCounters,
    InsertBeforeCommit,
    CommitAfterOutbox,
    CommitAfterAggregate,
    CommitAfterCounters,
    CommitBeforeCommit,
    DeleteAfterOutbox,
    DeleteAfterAggregate,
    DeleteAfterCounters,
    DeleteBeforeCommit,
    AcknowledgeAfterDelete,
    AcknowledgeAfterCounters,
    AcknowledgeBeforeCommit,
    SchemaCreationAfterSchema,
    SchemaCreationBeforeCommit,
    AttemptAcceptAfterWrite,
    AttemptAcceptBeforeCommit,
    AttemptSettleAfterWrite,
    AttemptSettleBeforeCommit,
    AttemptCompensationPrepareAfterWrite,
    AttemptCompensationPrepareBeforeCommit,
    AttemptCompensateAfterWrite,
    AttemptCompensateBeforeCommit,
    AttemptReconcileAfterWrite,
    AttemptReconcileBeforeCommit,
    AttemptRepairAfterWrite,
    AttemptRepairBeforeCommit,
    RestoreAfterInstallBeforeVerify,
}

impl fmt::Display for SqliteCrashPoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

/// Failure injector used by recovery tests. Production opens use the no-op implementation.
///
/// Implementations run synchronously inside database or maintenance work and
/// should return promptly without panicking. Returning `true` causes an error
/// at that hook; it does not terminate the thread or process.
pub trait SqliteCrashInjector: Send + Sync {
    fn should_crash(&self, point: SqliteCrashPoint) -> bool;
}

/// Production injector that never requests a fault.
#[derive(Debug, Default)]
pub struct NoopSqliteCrashInjector;

impl SqliteCrashInjector for NoopSqliteCrashInjector {
    fn should_crash(&self, _point: SqliteCrashPoint) -> bool {
        false
    }
}
