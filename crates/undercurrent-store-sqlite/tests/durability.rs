// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use rusqlite::Connection;
use undercurrent_application::{
    Clock, ObservedOperationAttemptRepair, OperationAttemptAcceptance, OperationAttemptAdmission,
    OperationAttemptReadinessService, OperationAttemptReconciliation, OperationAttemptRepairActor,
    OperationAttemptRepairAuthorization, OperationAttemptRepairAuthorizer,
    OperationAttemptRepairDisposition, OperationAttemptRepairEvidenceRef,
    OperationAttemptRepairObserver, OperationAttemptRepairRepository, OperationAttemptScan,
    OperationAttemptScanExclusion, OperationAttemptSettlement, OperationAttemptSettlementOutcome,
    OperationAttemptStore, PortError, PortErrorKind, PortFuture, RepairOperationAttemptCommand,
    RepairOperationAttemptError, RepairOperationAttemptService,
};
use undercurrent_domain::{
    DispatchCellIdentity, EffectCertainty, FencingToken, OperationAttempt, OperationAttemptId,
    OperationAttemptState, PayloadDigest, ProcessMutationKind, RuntimeCallbackRef,
    RuntimeDispatchBinding, RuntimeDispatchLimits, RuntimeDispatchPolicy, RuntimeOutputContract,
    RuntimeTelemetryPolicyRef, SessionId, TimestampMillis,
};
use undercurrent_foundation::Classification;
use undercurrent_store_sqlite::{
    SqliteBackupEvidence, SqliteCrashInjector, SqliteCrashPoint, SqliteMaintenanceError,
    SqliteRepositoryConfig, SqliteRepositoryLimits, SqliteRepositoryOpenError,
    SqliteRestoreOptions, SqliteSessionRepository, backup_sqlite_database, restore_sqlite_database,
    restore_sqlite_database_with_options, sqlite_schema_evidence, verify_sqlite_database,
};

#[test]
fn completed_stop_can_settle_behind_force_but_not_a_replacement_or_unknown_effect() {
    for replacement in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let repository = open(&directory.path().join("overlapping-stops.sqlite3"));
        let make = |id: &str, kind, fence| {
            OperationAttempt::accepted(
                OperationAttemptId::new(id).unwrap(),
                Some(SessionId::new("session").unwrap()),
                kind,
                FencingToken::new(fence).unwrap(),
                PayloadDigest::of_bytes(id.as_bytes()),
                TimestampMillis::new(10),
            )
        };
        let stop = make("graceful", ProcessMutationKind::Stop, 1);
        block_on(repository.accept(&stop)).unwrap();
        if replacement {
            block_on(repository.accept(&make("replacement", ProcessMutationKind::Spawn, 2)))
                .unwrap();
        }
        block_on(repository.accept(&make("force", ProcessMutationKind::ForceStop, 3))).unwrap();
        let mut settlement = OperationAttemptSettlement {
            attempt_id: stop.id().clone(),
            fencing_token: stop.fencing_token(),
            certainty: EffectCertainty::Unknown,
            result_digest: None,
            settled_at: TimestampMillis::new(20),
        };
        assert!(matches!(
            block_on(repository.settle(&settlement)).unwrap(),
            OperationAttemptSettlementOutcome::StaleFence { .. }
        ));
        settlement.certainty = EffectCertainty::KnownApplied;
        settlement.result_digest = Some(PayloadDigest::of_bytes(
            b"exact retained termination receipt",
        ));
        let result = block_on(repository.settle(&settlement)).unwrap();
        if replacement {
            assert!(matches!(
                result,
                OperationAttemptSettlementOutcome::StaleFence { .. }
            ));
        } else {
            assert!(matches!(
                result,
                OperationAttemptSettlementOutcome::Settled(_)
            ));
        }
    }
}

#[test]
fn cancelled_shutdown_can_settle_behind_newer_fence_without_authorizing_old_effects() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("shutdown-recovery.sqlite3");
    let repository = open(&path);
    let shutdown = |id: &str, fence: u64| {
        OperationAttempt::accepted(
            OperationAttemptId::new(id).unwrap(),
            None,
            ProcessMutationKind::Shutdown,
            FencingToken::new(fence).unwrap(),
            PayloadDigest::of_bytes(id.as_bytes()),
            TimestampMillis::new(10),
        )
    };
    let accepted = shutdown("abandoned-accepted", 1);
    let unknown = shutdown("abandoned-unknown", 2);
    let newer = shutdown("newer-shutdown", 3);
    for attempt in [&accepted, &unknown, &newer] {
        assert!(matches!(
            block_on(repository.accept(attempt)).unwrap(),
            OperationAttemptAcceptance::Accepted(_)
        ));
        if attempt.id() == unknown.id() {
            settle_unknown(&repository, attempt);
        }
    }
    let mut settlement = OperationAttemptSettlement {
        attempt_id: accepted.id().clone(),
        fencing_token: accepted.fencing_token(),
        certainty: EffectCertainty::KnownApplied,
        result_digest: Some(PayloadDigest::of_bytes(b"old effect")),
        settled_at: TimestampMillis::new(30),
    };
    assert!(matches!(
        block_on(repository.settle(&settlement)).unwrap(),
        OperationAttemptSettlementOutcome::StaleFence { .. }
    ));
    settlement.certainty = EffectCertainty::KnownAbsent;
    settlement.result_digest = None;
    settlement.fencing_token = newer.fencing_token();
    assert!(matches!(
        block_on(repository.settle(&settlement)).unwrap(),
        OperationAttemptSettlementOutcome::PayloadConflict
    ));
    settlement.fencing_token = accepted.fencing_token();
    assert!(matches!(
        block_on(repository.settle(&settlement)).unwrap(),
        OperationAttemptSettlementOutcome::Settled(_)
    ));
    assert!(matches!(
        block_on(repository.reconcile(&OperationAttemptReconciliation {
            attempt_id: unknown.id().clone(),
            fencing_token: unknown.fencing_token(),
            certainty: EffectCertainty::KnownAbsent,
            result_digest: None,
            reconciled_at: TimestampMillis::new(30),
        }))
        .unwrap(),
        OperationAttemptSettlementOutcome::Settled(_)
    ));
    drop(repository);
    let repository = open(&path);
    for attempt in [&accepted, &unknown] {
        assert!(matches!(
            block_on(repository.load(attempt.id()))
                .unwrap()
                .unwrap()
                .state(),
            OperationAttemptState::Settled {
                certainty: EffectCertainty::KnownAbsent,
                ..
            }
        ));
    }
    assert!(matches!(
        block_on(repository.load(newer.id()))
            .unwrap()
            .unwrap()
            .state(),
        OperationAttemptState::Accepted
    ));
}

#[test]
fn accept_next_allocates_and_recovers_monotonic_target_fences() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory.path().join("next-fence.sqlite3");
    let repository = open(&database_path);
    let target = SessionId::new("session-next").expect("session ID");
    let first = OperationAttemptAdmission::new(
        OperationAttemptId::new("next-1").expect("attempt ID"),
        Some(target.clone()),
        ProcessMutationKind::Spawn,
        PayloadDigest::of_bytes(b"first"),
        TimestampMillis::new(10),
    );
    let accepted = block_on(OperationAttemptStore::accept_next(&repository, &first))
        .expect("accept first admission");
    let first_attempt = match accepted {
        OperationAttemptAcceptance::Accepted(value) => value,
        other => panic!("unexpected first admission outcome: {other:?}"),
    };
    assert_eq!(first_attempt.fencing_token().value(), 1);
    assert!(matches!(
        block_on(OperationAttemptStore::accept_next(&repository, &first))
            .expect("deduplicate first admission"),
        OperationAttemptAcceptance::Duplicate(value)
            if value.fencing_token() == first_attempt.fencing_token()
    ));

    let second = OperationAttemptAdmission::new(
        OperationAttemptId::new("next-2").expect("attempt ID"),
        Some(target.clone()),
        ProcessMutationKind::Spawn,
        PayloadDigest::of_bytes(b"second"),
        TimestampMillis::new(20),
    );
    let second_attempt = match block_on(OperationAttemptStore::accept_next(&repository, &second))
        .expect("accept second admission")
    {
        OperationAttemptAcceptance::Accepted(value) => value,
        other => panic!("unexpected second admission outcome: {other:?}"),
    };
    assert_eq!(second_attempt.fencing_token().value(), 2);
    drop(repository);

    let repository = open(&database_path);
    let third = OperationAttemptAdmission::new(
        OperationAttemptId::new("next-3").expect("attempt ID"),
        Some(target),
        ProcessMutationKind::Spawn,
        PayloadDigest::of_bytes(b"third"),
        TimestampMillis::new(30),
    );
    assert!(matches!(
        block_on(OperationAttemptStore::accept_next(&repository, &third))
            .expect("accept after restart"),
        OperationAttemptAcceptance::Accepted(value) if value.fencing_token().value() == 3
    ));
}

#[cfg(windows)]
#[test]
fn windows_database_sidecars_and_backups_are_owned_by_the_account() {
    use undercurrent_platform_windows::{
        PrivateRemovalFile, open_private_lock_file, prepare_private_directory,
    };
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("private-state");
    let _root = prepare_private_directory(&root).unwrap();
    let database = root.join("state.sqlite3");
    let backup = root.join("backup.sqlite3");
    let restored = root.join("restored.sqlite3");
    let repository = open(&database);
    accept_scan_attempts(&repository, 2);
    // Use the sharing-compatible owner validator while SQLite holds its files.
    // This also runs on elevated CI where generic creation defaults to a group.
    for suffix in ["", "-wal", "-shm"] {
        let path = root.join(format!("state.sqlite3{suffix}"));
        assert!(path.is_file(), "SQLite did not create {suffix}");
        drop(open_private_lock_file(&path, false).expect("exact-account owner"));
    }
    backup_sqlite_database(&database, &backup).unwrap();
    drop(repository);
    restore_sqlite_database(&backup, &restored).unwrap();
    assert_eq!(
        verify_sqlite_database(&database).unwrap(),
        verify_sqlite_database(&restored).unwrap()
    );
    for path in [&database, &backup, &restored] {
        PrivateRemovalFile::open(path)
            .expect("reset-compatible state")
            .remove()
            .unwrap();
        assert!(!path.exists());
    }
}

#[test]
fn attempts_are_fenced_idempotent_and_recover_after_restart() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory.path().join("attempts.sqlite3");
    let repository = open(&database_path);
    let accepted_attempt = attempt("attempt-1", Some("session-1"), 2, b"spawn");

    let accepted = block_on(OperationAttemptStore::accept(
        &repository,
        &accepted_attempt,
    ))
    .expect("accept attempt");
    assert!(
        matches!(accepted, OperationAttemptAcceptance::Accepted(ref value) if value == &accepted_attempt)
    );

    let duplicate = block_on(OperationAttemptStore::accept(
        &repository,
        &accepted_attempt,
    ))
    .expect("deduplicate attempt");
    assert!(
        matches!(duplicate, OperationAttemptAcceptance::Duplicate(ref value) if value == &accepted_attempt)
    );

    let conflicting = OperationAttempt::accepted(
        accepted_attempt.id().clone(),
        accepted_attempt.target_session().cloned(),
        accepted_attempt.kind(),
        accepted_attempt.fencing_token(),
        PayloadDigest::of_bytes(b"different payload"),
        accepted_attempt.accepted_at(),
    );
    assert_eq!(
        block_on(OperationAttemptStore::accept(&repository, &conflicting))
            .expect("classify reused identity"),
        OperationAttemptAcceptance::PayloadConflict
    );

    let stale = attempt("attempt-stale", Some("session-1"), 1, b"stale");
    assert_eq!(
        block_on(OperationAttemptStore::accept(&repository, &stale)).expect("classify stale fence"),
        OperationAttemptAcceptance::StaleFence {
            current: FencingToken::new(2).expect("fence"),
        }
    );

    let settlement = OperationAttemptSettlement {
        attempt_id: accepted_attempt.id().clone(),
        fencing_token: accepted_attempt.fencing_token(),
        certainty: EffectCertainty::Unknown,
        result_digest: None,
        settled_at: TimestampMillis::new(20),
    };
    let settled =
        block_on(OperationAttemptStore::settle(&repository, &settlement)).expect("settle attempt");
    assert!(matches!(
        settled,
        OperationAttemptSettlementOutcome::Settled(_)
    ));
    let duplicate = block_on(OperationAttemptStore::settle(&repository, &settlement))
        .expect("deduplicate settlement");
    assert!(matches!(
        duplicate,
        OperationAttemptSettlementOutcome::Duplicate(_)
    ));

    let scan = OperationAttemptScan::without_exclusions(10).expect("attempt scan");
    let pending = block_on(OperationAttemptStore::pending_reconciliation(
        &repository,
        &scan,
    ))
    .expect("read pending attempts");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id(), accepted_attempt.id());
    drop(repository);

    let repository = open(&database_path);
    let recovered = block_on(OperationAttemptStore::load(
        &repository,
        accepted_attempt.id(),
    ))
    .expect("load recovered attempt")
    .expect("attempt exists");
    assert!(matches!(
        recovered.state(),
        OperationAttemptState::Settled {
            certainty: EffectCertainty::Unknown,
            ..
        }
    ));

    let result_digest = PayloadDigest::of_bytes(b"spawn-result");
    let reconciliation = OperationAttemptReconciliation {
        attempt_id: accepted_attempt.id().clone(),
        fencing_token: accepted_attempt.fencing_token(),
        certainty: EffectCertainty::KnownApplied,
        result_digest: Some(result_digest),
        reconciled_at: TimestampMillis::new(30),
    };
    let reconciled = block_on(OperationAttemptStore::reconcile(
        &repository,
        &reconciliation,
    ))
    .expect("reconcile attempt");
    assert!(matches!(
        reconciled,
        OperationAttemptSettlementOutcome::Settled(_)
    ));
    assert!(
        block_on(OperationAttemptStore::pending_reconciliation(
            &repository,
            &scan
        ))
        .expect("read reconciled attempts")
        .is_empty()
    );
}

#[test]
fn dispatched_attempt_binding_and_result_survive_restart_and_fence_by_caller_cell() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory.path().join("dispatch-attempt.sqlite3");
    let repository = open(&database_path);
    let dispatched = dispatched_attempt("dispatch-5", "session-a", 5, b"input-a");
    assert!(matches!(
        block_on(OperationAttemptStore::accept(&repository, &dispatched)).expect("accept dispatch"),
        OperationAttemptAcceptance::Accepted(_)
    ));
    let result_digest = PayloadDigest::of_bytes(b"process-instance-result");
    assert!(matches!(
        block_on(OperationAttemptStore::settle(
            &repository,
            &OperationAttemptSettlement {
                attempt_id: dispatched.id().clone(),
                fencing_token: dispatched.fencing_token(),
                certainty: EffectCertainty::KnownApplied,
                result_digest: Some(result_digest),
                settled_at: TimestampMillis::new(20),
            },
        ))
        .expect("settle dispatch"),
        OperationAttemptSettlementOutcome::Settled(_)
    ));
    drop(repository);

    let repository = open(&database_path);
    let recovered = block_on(OperationAttemptStore::load(&repository, dispatched.id()))
        .expect("load dispatch")
        .expect("durable dispatch");
    assert_eq!(recovered.dispatch_binding(), dispatched.dispatch_binding());
    assert_eq!(recovered.target_session(), dispatched.target_session());
    assert!(matches!(
        recovered.state(),
        OperationAttemptState::Settled {
            certainty: EffectCertainty::KnownApplied,
            result_digest: Some(digest),
            ..
        } if *digest == result_digest
    ));
    assert!(matches!(
        block_on(OperationAttemptStore::accept(&repository, &dispatched))
            .expect("replay exact dispatch"),
        OperationAttemptAcceptance::AlreadySettled(value) if value == recovered
    ));

    for changed_input in [b"input-b".as_slice(), b"input-c".as_slice()] {
        let changed = dispatched_attempt("dispatch-5", "session-a", 5, changed_input);
        assert_eq!(
            block_on(OperationAttemptStore::accept(&repository, &changed))
                .expect("classify changed binding"),
            OperationAttemptAcceptance::PayloadConflict
        );
    }

    let superseding = dispatched_attempt("dispatch-6", "session-b", 6, b"input-b");
    assert!(matches!(
        block_on(OperationAttemptStore::accept(&repository, &superseding))
            .expect("accept superseding dispatch"),
        OperationAttemptAcceptance::Accepted(_)
    ));
    let stale = dispatched_attempt("dispatch-stale", "session-c", 5, b"input-c");
    assert_eq!(
        block_on(OperationAttemptStore::accept(&repository, &stale))
            .expect("reject stale dispatch"),
        OperationAttemptAcceptance::StaleFence {
            current: FencingToken::new(6).expect("current fence")
        }
    );
}

#[test]
fn persisted_policy_requires_explicit_optional_limit_members_after_restart() {
    let directory = tempfile::tempdir().expect("temporary directory");

    for (index, field) in ["memory_bytes", "cpu_millis"].into_iter().enumerate() {
        let database_path = directory.path().join(format!("missing-{field}.sqlite3"));
        let repository = open(&database_path);
        let stored = dispatched_attempt(
            &format!("dispatch-missing-limit-{index}"),
            "session-a",
            1,
            b"input-a",
        );
        assert!(matches!(
            block_on(OperationAttemptStore::accept(&repository, &stored)).expect("accept dispatch"),
            OperationAttemptAcceptance::Accepted(_)
        ));
        drop(repository);

        let connection = Connection::open(&database_path).expect("open policy store");
        let json_path = format!("$.limits.{field}");
        connection
            .execute(
                "UPDATE operation_attempts
                 SET runtime_policy_json = json_remove(runtime_policy_json, ?1)
                 WHERE attempt_id = ?2",
                (&json_path, stored.id().as_str()),
            )
            .expect("remove required optional limit member");
        drop(connection);

        let repository = open(&database_path);
        let error = block_on(OperationAttemptStore::load(&repository, stored.id()))
            .expect_err("omitted durable limit member must be corruption");
        assert_eq!(error.kind(), PortErrorKind::InvalidData);
    }
}

#[test]
fn current_policy_provenance_rejects_paired_downgrade_and_new_policyless_dispatches() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory.path().join("policy-provenance.sqlite3");
    let repository = open(&database_path);
    let current = dispatched_attempt("current-policy", "session-a", 1, b"input-a");
    assert!(matches!(
        block_on(OperationAttemptStore::accept(&repository, &current))
            .expect("accept current dispatch"),
        OperationAttemptAcceptance::Accepted(_)
    ));

    drop(repository);

    let connection = Connection::open(&database_path).expect("open policy store");
    assert!(
        connection
            .execute(
                "UPDATE operation_attempts SET runtime_policy_json = NULL WHERE attempt_id = ?1",
                [current.id().as_str()],
            )
            .is_err(),
        "the row-level provenance check must reject policy loss"
    );
    assert!(
        connection
            .execute(
                "INSERT INTO legacy_policyless_operation_attempts (attempt_id) VALUES (?1)",
                [current.id().as_str()],
            )
            .is_err(),
        "current rows cannot acquire migration-owned legacy provenance"
    );
    connection
        .execute(
            "UPDATE operation_attempts
             SET runtime_policy_json = NULL, runtime_policy_state = 0
             WHERE attempt_id = ?1",
            [current.id().as_str()],
        )
        .expect("install constraint-valid paired downgrade fixture");
    drop(connection);

    assert!(
        SqliteSessionRepository::open(config(&database_path)).is_err(),
        "startup must not reinterpret a paired current-policy downgrade as migrated legacy provenance"
    );
}

#[test]
fn current_request_digest_provenance_rejects_tampering_and_new_digestless_dispatches() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory.path().join("request-digest-provenance.sqlite3");
    let repository = open(&database_path);
    let current = dispatched_attempt("current-request-digest", "session-a", 1, b"input-a");
    assert!(matches!(
        block_on(OperationAttemptStore::accept(&repository, &current))
            .expect("accept current dispatch"),
        OperationAttemptAcceptance::Accepted(_)
    ));

    drop(repository);

    let connection = Connection::open(&database_path).expect("open request-digest store");
    assert!(
        connection
            .execute(
                "UPDATE operation_attempts SET request_digest = ?1 WHERE attempt_id = ?2",
                ("A".repeat(64), current.id().as_str()),
            )
            .is_err(),
        "the column check must reject noncanonical uppercase digest text"
    );
    assert!(
        connection
            .execute(
                "INSERT INTO legacy_request_digestless_operation_attempts (attempt_id) VALUES (?1)",
                [current.id().as_str()],
            )
            .is_err(),
        "current rows cannot acquire migration-owned request-digestless provenance"
    );
    connection
        .execute(
            "UPDATE operation_attempts SET request_digest = NULL WHERE attempt_id = ?1",
            [current.id().as_str()],
        )
        .expect("install constraint-valid request-digest downgrade fixture");
    drop(connection);

    assert!(
        SqliteSessionRepository::open(config(&database_path)).is_err(),
        "startup must reject request-digest loss without migration provenance"
    );
    assert!(verify_sqlite_database(&database_path).is_err());
}

#[test]
fn modified_policyless_v1_schema_is_rejected_without_mutation() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory
        .path()
        .join("modified-policyless-0.1.0-beta.sqlite3");
    drop(open(&database_path));
    install_unsupported_release(&database_path);
    let connection = Connection::open(&database_path).expect("open policyless baseline");
    connection
        .execute_batch("CREATE TABLE unexpected_legacy_extension (value TEXT) STRICT;")
        .expect("alter otherwise exact 0.1.0-beta schema");
    drop(connection);
    let before = std::fs::read(&database_path).expect("read modified baseline");

    assert!(SqliteSessionRepository::open(config(&database_path)).is_err());
    assert_eq!(
        std::fs::read(&database_path).expect("read rejected baseline"),
        before,
        "rejected 0.1.0-beta schema must remain byte-for-byte unchanged"
    );
}

#[test]
fn policyless_v1_schema_fingerprint_preflight_is_resource_bounded_and_read_only() {
    for fixture in ["oversized", "too-many"] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let database_path = directory.path().join(format!("{fixture}-schema.sqlite3"));
        drop(open(&database_path));
        install_unsupported_release(&database_path);
        let connection = Connection::open(&database_path).expect("open policyless baseline");
        match fixture {
            "oversized" => {
                let oversized_literal = "x".repeat(262_145);
                connection
                    .execute(
                        &format!(
                            "CREATE VIEW oversized_schema_object AS SELECT '{oversized_literal}' AS value"
                        ),
                        [],
                    )
                    .expect("install oversized schema object");
            }
            "too-many" => {
                for index in 0..130 {
                    connection
                        .execute(
                            &format!(
                                "CREATE VIEW excessive_schema_object_{index} AS SELECT {index} AS value"
                            ),
                            [],
                        )
                        .expect("install excessive schema object");
                }
            }
            _ => unreachable!("fixture list is closed"),
        }
        drop(connection);
        let before = std::fs::read(&database_path).expect("read malformed baseline");

        let error = match SqliteSessionRepository::open(config(&database_path)) {
            Ok(_) => panic!("bounded schema preflight must reject malformed 0.1.0-beta"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            SqliteRepositoryOpenError::Validate(ref validation)
                if validation.kind() == PortErrorKind::InvalidData
        ));
        assert_eq!(
            std::fs::read(&database_path).expect("read rejected baseline"),
            before,
            "bounded preflight rejection must not mutate the database"
        );
    }
}

#[test]
fn current_snapshot_schema_is_size_checked_before_definition_hydration() {
    use std::error::Error;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("oversized-current-snapshot.sqlite3");
    drop(open(&path));
    let connection = Connection::open(&path).unwrap();
    let definition: String = connection
        .query_row(
            "SELECT sql FROM sqlite_master WHERE name='session_event_snapshots'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    // Keep the table parseable and its current migration ledger intact, while
    // making normalization observably different from the released definition.
    connection
        .execute_batch("PRAGMA writable_schema=ON")
        .unwrap();
    connection
        .execute(
            "UPDATE sqlite_master SET sql=?1 WHERE name='session_event_snapshots'",
            [format!("{definition} /*{}*/", "x".repeat(262_145))],
        )
        .unwrap();
    drop(connection);
    let before = std::fs::read(&path).unwrap();
    let error = match SqliteSessionRepository::open(config(&path)) {
        Ok(_) => panic!("oversized current definition must fail admission"),
        Err(error) => error,
    };
    let mut diagnostics = error.to_string();
    let mut source = error.source();
    while let Some(error) = source {
        diagnostics.push_str(&error.to_string());
        source = error.source();
    }
    assert!(
        diagnostics.contains("schema evidence exceeds the bounded release layout"),
        "size admission must precede snapshot normalization: {diagnostics}"
    );
    assert!(!diagnostics.contains("snapshot layout does not match"));
    assert_eq!(std::fs::read(&path).unwrap(), before);
}

#[test]
fn migration_ledger_preflight_does_not_hydrate_unbounded_rows_or_text() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory.path().join("oversized-migration-ledger.sqlite3");
    drop(open(&database_path));
    install_unsupported_release(&database_path);
    let connection = Connection::open(&database_path).expect("open policyless baseline");
    let oversized_text = "x".repeat(300_000);
    for version in [3, 4] {
        connection
            .execute(
                "INSERT INTO schema_identity
                 (version, release_version, name, checksum, applied_at_unix_seconds)
                 VALUES (?1, '0.1.0-beta', ?2, ?3, 1)",
                (version, &oversized_text, &oversized_text),
            )
            .expect("install oversized unexpected migration evidence");
    }
    drop(connection);
    let before = std::fs::read(&database_path).expect("read malformed migration ledger");

    let error = match SqliteSessionRepository::open(config(&database_path)) {
        Ok(_) => panic!("bounded migration-evidence preflight must reject malformed 0.1.0-beta"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        SqliteRepositoryOpenError::Validate(ref validation)
            if validation.kind() == PortErrorKind::InvalidData
    ));
    assert!(verify_sqlite_database(&database_path).is_err());
    assert_eq!(
        std::fs::read(&database_path).expect("read rejected migration ledger"),
        before,
        "migration-evidence rejection must not mutate the database"
    );
}

#[test]
fn former_whitespace_token_collision_is_rejected_with_current_ledger_evidence() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory.path().join("whitespace-token-collision.sqlite3");
    drop(open(&database_path));
    install_unsupported_release(&database_path);
    let connection = Connection::open(&database_path).expect("open policyless baseline");
    connection
        .pragma_update(None, "writable_schema", true)
        .expect("enable adversarial schema fixture write");
    let changed = connection
        .execute(
            "UPDATE sqlite_master
             SET sql = replace(sql, 'accepted_at INTEGER NOT NULL',
                                    'accepted_at INTEGERNOT NULL')
             WHERE type = 'table' AND name = 'operation_attempts'",
            [],
        )
        .expect("install former whitespace-normalization collision");
    assert_eq!(changed, 1);
    connection
        .pragma_update(None, "writable_schema", false)
        .expect("disable adversarial schema fixture write");
    drop(connection);
    let before = std::fs::read(&database_path).expect("read altered baseline");

    assert!(SqliteSessionRepository::open(config(&database_path)).is_err());
    assert!(verify_sqlite_database(&database_path).is_err());
    assert_eq!(
        std::fs::read(&database_path).expect("read rejected altered baseline"),
        before,
        "schema collision rejection must remain read-only"
    );
}

#[test]
fn incomplete_prerelease_v1_attempt_layout_is_rejected_without_mutation() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory.path().join("old-0.1.0-beta.sqlite3");
    drop(open(&database_path));
    let connection = Connection::open(&database_path).expect("open database");
    connection
        .execute(
            "UPDATE schema_identity SET checksum = ?1 WHERE version = 1",
            ["sha256:794e7850ed562d5fa95b323a4614492b04825d544f0ccc2a3e73d9e7c5059714"],
        )
        .expect("install incomplete-0.1.0-beta evidence");
    drop(connection);
    let before = std::fs::read(&database_path).expect("read before rejected open");

    assert!(SqliteSessionRepository::open(config(&database_path)).is_err());
    assert_eq!(
        std::fs::read(&database_path).expect("read after rejected open"),
        before,
        "compatibility rejection must be read-only"
    );
}

#[test]
fn attempt_scan_excludes_one_live_attempt_and_proves_snapshot_exhaustion() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let repository = open_with_read_limit(&directory.path().join("one-live.sqlite3"), 256);
    let attempts = accept_scan_attempts(&repository, 1);
    let scan = attempt_scan(256, &attempts);

    assert!(
        block_on(OperationAttemptStore::pending_reconciliation(
            &repository,
            &scan
        ))
        .expect("scan pending attempts")
        .is_empty()
    );
    let unresolved = block_on(OperationAttemptRepairRepository::unresolved_attempts(
        &repository,
        &scan,
    ))
    .expect("scan unresolved attempts");
    assert_eq!(unresolved.total(), 0);
    assert!(unresolved.attempt_ids().is_empty());
}

#[test]
fn attempt_scan_fills_hard_page_after_one_exact_live_exclusion() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let repository = open_with_read_limit(&directory.path().join("one-live-page.sqlite3"), 256);
    let attempts = accept_scan_attempts(&repository, 257);
    let scan = attempt_scan(256, &attempts[..1]);

    let pending = block_on(OperationAttemptStore::pending_reconciliation(
        &repository,
        &scan,
    ))
    .expect("scan pending attempts");
    assert_eq!(pending.len(), 256);
    assert_eq!(
        pending.first().map(OperationAttempt::id),
        Some(attempts[1].id())
    );
    assert_eq!(
        pending.last().map(OperationAttempt::id),
        Some(attempts[256].id())
    );
    let unresolved = block_on(OperationAttemptRepairRepository::unresolved_attempts(
        &repository,
        &scan,
    ))
    .expect("scan unresolved attempts");
    assert_eq!(unresolved.total(), 256);
    assert_eq!(unresolved.attempt_ids().len(), 256);
}

#[test]
fn attempt_scan_does_not_hide_unresolved_row_behind_full_live_prefix() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let repository = open_with_read_limit(&directory.path().join("live-prefix.sqlite3"), 256);
    let attempts = accept_scan_attempts(&repository, 257);
    let scan = attempt_scan(256, &attempts[..256]);

    let pending = block_on(OperationAttemptStore::pending_reconciliation(
        &repository,
        &scan,
    ))
    .expect("scan pending attempts");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id(), attempts[256].id());
    let unresolved = block_on(OperationAttemptRepairRepository::unresolved_attempts(
        &repository,
        &scan,
    ))
    .expect("scan unresolved attempts");
    assert_eq!(unresolved.total(), 1);
    assert_eq!(unresolved.attempt_ids(), &[attempts[256].id().clone()]);
}

#[test]
fn attempt_scan_rejects_exclusion_overflow_as_capacity_without_querying() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let repository = open_with_read_limit(&directory.path().join("overflow.sqlite3"), 256);
    let attempts = (0..257).map(scan_attempt).collect::<Vec<_>>();
    let scan = attempt_scan(257, &attempts);

    let pending = block_on(OperationAttemptStore::pending_reconciliation(
        &repository,
        &scan,
    ))
    .expect_err("pending scan above the hard bound");
    assert_eq!(pending.kind(), PortErrorKind::CapacityExceeded);
    let unresolved = block_on(OperationAttemptRepairRepository::unresolved_attempts(
        &repository,
        &scan,
    ))
    .expect_err("readiness scan above the hard bound");
    assert_eq!(unresolved.kind(), PortErrorKind::CapacityExceeded);
}

#[test]
fn attempt_scan_preserves_exact_hard_result_bound_and_stable_order() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let repository = open_with_read_limit(&directory.path().join("hard-bound.sqlite3"), 256);
    let attempts = accept_scan_attempts(&repository, 257);
    let scan = OperationAttemptScan::without_exclusions(256).expect("attempt scan");

    let pending = block_on(OperationAttemptStore::pending_reconciliation(
        &repository,
        &scan,
    ))
    .expect("bounded pending scan");
    assert_eq!(pending.len(), 256);
    assert_eq!(
        pending.first().map(OperationAttempt::id),
        Some(attempts[0].id())
    );
    assert_eq!(
        pending.last().map(OperationAttempt::id),
        Some(attempts[255].id())
    );
    let unresolved = block_on(OperationAttemptRepairRepository::unresolved_attempts(
        &repository,
        &scan,
    ))
    .expect("bounded readiness scan");
    assert_eq!(unresolved.total(), 257);
    assert_eq!(unresolved.attempt_ids().len(), 256);
    assert!(unresolved.truncated());
}

#[test]
fn wal_backup_is_verified_and_restore_rolls_back_on_interruption() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let source_path = directory.path().join("source.sqlite3");
    let backup_path = directory.path().join("backup.sqlite3");
    let target_path = directory.path().join("target.sqlite3");
    let source_attempt = attempt("source-attempt", Some("source-session"), 1, b"source");
    let target_attempt = attempt("target-attempt", Some("target-session"), 1, b"target");

    let source_repository = open(&source_path);
    block_on(OperationAttemptStore::accept(
        &source_repository,
        &source_attempt,
    ))
    .expect("accept source attempt");
    let SqliteBackupEvidence { verification } =
        backup_sqlite_database(&source_path, &backup_path).expect("create WAL-safe backup");
    assert_eq!(verification.schema_version, 1);
    assert_eq!(
        sqlite_schema_evidence(&backup_path)
            .expect("migration evidence")
            .len(),
        1
    );
    drop(source_repository);

    let target_repository = open(&target_path);
    block_on(OperationAttemptStore::accept(
        &target_repository,
        &target_attempt,
    ))
    .expect("accept target attempt");
    drop(target_repository);

    let interrupted = restore_sqlite_database_with_options(
        &backup_path,
        &target_path,
        SqliteRestoreOptions::default().with_crash_injector(Arc::new(CrashAt(
            SqliteCrashPoint::RestoreAfterInstallBeforeVerify,
        ))),
    );
    assert!(matches!(
        interrupted,
        Err(SqliteMaintenanceError::InjectedCrash(
            SqliteCrashPoint::RestoreAfterInstallBeforeVerify
        ))
    ));

    let target_repository = open(&target_path);
    assert!(
        block_on(OperationAttemptStore::load(
            &target_repository,
            target_attempt.id()
        ))
        .expect("load rollback target")
        .is_some()
    );
    assert!(
        block_on(OperationAttemptStore::load(
            &target_repository,
            source_attempt.id()
        ))
        .expect("exclude interrupted restore")
        .is_none()
    );
    drop(target_repository);

    let restored = restore_sqlite_database(&backup_path, &target_path).expect("restore backup");
    assert!(restored.replaced_existing_database);
    assert_eq!(restored.source, restored.installed);
    let target_repository = open(&target_path);
    assert!(
        block_on(OperationAttemptStore::load(
            &target_repository,
            source_attempt.id()
        ))
        .expect("load restored attempt")
        .is_some()
    );
    assert!(
        block_on(OperationAttemptStore::load(
            &target_repository,
            target_attempt.id()
        ))
        .expect("old state replaced")
        .is_none()
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&backup_path)
                .expect("backup metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(&target_path)
                .expect("restored metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

#[test]
fn verification_rejects_corruption_and_future_schema() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let source_path = directory.path().join("source.sqlite3");
    let corrupt_path = directory.path().join("corrupt.sqlite3");
    let future_path = directory.path().join("future.sqlite3");
    let repository = open(&source_path);
    backup_sqlite_database(&source_path, &corrupt_path).expect("backup for corruption");
    backup_sqlite_database(&source_path, &future_path).expect("backup for compatibility");
    drop(repository);

    std::fs::write(&corrupt_path, b"not a sqlite database").expect("corrupt backup");
    assert!(verify_sqlite_database(&corrupt_path).is_err());

    let connection = Connection::open(&future_path).expect("open future database");
    connection
        .pragma_update(None, "user_version", 99_i64)
        .expect("set future version");
    drop(connection);
    assert!(matches!(
        verify_sqlite_database(&future_path),
        Err(SqliteMaintenanceError::UnsupportedSchema {
            found: 99,
            maximum: 1
        })
    ));
}

#[test]
fn injected_attempt_and_initial_schema_failures_leave_restart_safe_state() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory.path().join("attempt-crash.sqlite3");
    let value = attempt("crash-attempt", Some("session-1"), 1, b"spawn");
    let repository = open_with_crash(&database_path, SqliteCrashPoint::AttemptAcceptAfterWrite);
    assert!(block_on(OperationAttemptStore::accept(&repository, &value)).is_err());
    drop(repository);
    let repository = open(&database_path);
    assert!(
        block_on(OperationAttemptStore::load(&repository, value.id()))
            .expect("load after injected failure")
            .is_none()
    );
    drop(repository);

    let initial_path = directory.path().join("initial.sqlite3");
    let initialization = SqliteSessionRepository::open_with_crash_injector(
        config(&initial_path),
        Arc::new(CrashAt(SqliteCrashPoint::SchemaCreationAfterSchema)),
    );
    assert!(initialization.is_err());
    let connection = Connection::open(&initial_path).expect("reopen initial database");
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .expect("rolled-back version");
    let evidence_tables: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'schema_identity'",
            [],
            |row| row.get(0),
        )
        .expect("initial ledger table count");
    assert_eq!(version, 0);
    assert_eq!(evidence_tables, 0);
    drop(connection);
    let repository = open(&initial_path);
    drop(repository);
    assert_eq!(
        sqlite_schema_evidence(&initial_path)
            .expect("completed initial schema")
            .len(),
        1
    );

    let prerelease_path = directory.path().join("prerelease.sqlite3");
    create_prerelease_database(&prerelease_path);
    let before = std::fs::read(&prerelease_path).expect("read prerelease database");
    assert!(SqliteSessionRepository::open(config(&prerelease_path)).is_err());
    assert_eq!(
        std::fs::read(&prerelease_path).expect("reread prerelease database"),
        before,
        "unsupported prerelease database was mutated",
    );
}

#[test]
fn evidence_backed_unknown_repair_is_atomic_audited_idempotent_and_readiness_clearing() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory.path().join("repair.sqlite3");
    let repository = open(&database_path);
    let value = attempt("repair-attempt", Some("repair-session"), 1, b"repair");
    settle_unknown(&repository, &value);

    let authorizer = AllowRepair;
    let observer = RepairEvidence::known_applied(b"repair-result", "receipt:repair:0.1.0-beta");
    let clock = FixedClock(TimestampMillis::new(30));
    let readiness = OperationAttemptReadinessService::new(&repository, 10).expect("readiness");
    assert!(
        !block_on(readiness.inspect())
            .expect("blocked readiness")
            .complete()
    );

    let service =
        RepairOperationAttemptService::new(&authorizer, &repository, &observer, &clock, 10)
            .expect("repair service");
    let command = RepairOperationAttemptCommand::new(value.id().clone(), repair_actor("operator"));
    let repaired = block_on(service.execute(command.clone())).expect("repair unknown");
    assert_eq!(
        repaired.disposition(),
        OperationAttemptRepairDisposition::Repaired
    );
    assert!(repaired.readiness().complete());
    assert_eq!(observer.acknowledged.load(Ordering::Acquire), 1);

    let duplicate = block_on(service.execute(command)).expect("duplicate repair");
    assert_eq!(
        duplicate.disposition(),
        OperationAttemptRepairDisposition::Duplicate
    );
    assert_eq!(observer.acknowledged.load(Ordering::Acquire), 2);

    let conflict_observer =
        RepairEvidence::known_applied(b"other-result", "receipt:repair:0.1.0-beta");
    let conflict_service = RepairOperationAttemptService::new(
        &authorizer,
        &repository,
        &conflict_observer,
        &clock,
        10,
    )
    .expect("conflict service");
    assert!(matches!(
        block_on(conflict_service.execute(RepairOperationAttemptCommand::new(
            value.id().clone(),
            repair_actor("operator"),
        ))),
        Err(RepairOperationAttemptError::PayloadConflict)
    ));
    drop(repository);

    let connection = Connection::open(&database_path).expect("open audit database");
    let audit: (String, String, String, i64) = connection
        .query_row(
            "SELECT evidence_reference, actor_principal, actor_authority, committed_at
             FROM operation_attempt_repairs WHERE attempt_id = ?1",
            [value.id().as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("repair audit row");
    assert_eq!(
        audit,
        (
            "receipt:repair:0.1.0-beta".to_owned(),
            "operator".to_owned(),
            "repair-authority".to_owned(),
            30,
        )
    );
}

#[test]
fn repair_crash_boundaries_roll_back_terminal_state_and_audit_together() {
    for point in [
        SqliteCrashPoint::AttemptRepairAfterWrite,
        SqliteCrashPoint::AttemptRepairBeforeCommit,
    ] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let database_path = directory.path().join(format!("repair-{point:?}.sqlite3"));
        let repository = open_with_crash(&database_path, point);
        let value = attempt("repair-crash", Some("repair-crash-session"), 1, b"repair");
        settle_unknown(&repository, &value);
        let observer = RepairEvidence::known_applied(b"result", "receipt:crash:0.1.0-beta");
        let clock = FixedClock(TimestampMillis::new(30));
        let service =
            RepairOperationAttemptService::new(&AllowRepair, &repository, &observer, &clock, 10)
                .expect("repair service");
        assert!(matches!(
            block_on(service.execute(RepairOperationAttemptCommand::new(
                value.id().clone(),
                repair_actor("operator"),
            ))),
            Err(RepairOperationAttemptError::Commit(_))
        ));
        drop(repository);

        let repository = open(&database_path);
        let recovered = block_on(OperationAttemptStore::load(&repository, value.id()))
            .expect("load recovered attempt")
            .expect("attempt exists");
        assert!(matches!(
            recovered.state(),
            OperationAttemptState::Settled {
                certainty: EffectCertainty::Unknown,
                ..
            }
        ));
        let readiness = OperationAttemptReadinessService::new(&repository, 10).expect("readiness");
        assert!(
            !block_on(readiness.inspect())
                .expect("readiness result")
                .complete()
        );
    }
}

fn open(path: &Path) -> SqliteSessionRepository {
    SqliteSessionRepository::open(config(path)).expect("open repository")
}

fn open_with_read_limit(path: &Path, max_read_batch: usize) -> SqliteSessionRepository {
    let config = SqliteRepositoryConfig::new(
        path,
        SqliteRepositoryLimits::new(100, 100, 1_048_576, max_read_batch, 32).expect("limits"),
        Duration::from_secs(1),
    )
    .expect("configuration");
    SqliteSessionRepository::open(config).expect("open repository")
}

fn accept_scan_attempts(
    repository: &SqliteSessionRepository,
    count: usize,
) -> Vec<OperationAttempt> {
    (0..count)
        .map(|index| {
            let attempt = scan_attempt(index);
            block_on(OperationAttemptStore::accept(repository, &attempt))
                .expect("accept scan attempt");
            attempt
        })
        .collect()
}

fn scan_attempt(index: usize) -> OperationAttempt {
    let id = format!("scan-attempt-{index:04}");
    OperationAttempt::accepted(
        OperationAttemptId::new(id.clone()).expect("attempt ID"),
        Some(SessionId::new(format!("scan-session-{index:04}")).expect("session ID")),
        ProcessMutationKind::Spawn,
        FencingToken::new(1).expect("fencing token"),
        PayloadDigest::of_bytes(id.as_bytes()),
        TimestampMillis::new(i64::try_from(index).expect("timestamp range") + 10),
    )
}

fn attempt_scan(limit: usize, attempts: &[OperationAttempt]) -> OperationAttemptScan {
    OperationAttemptScan::new(
        limit,
        attempts
            .iter()
            .map(|attempt| {
                OperationAttemptScanExclusion::new(attempt.id().clone(), attempt.fencing_token())
            })
            .collect(),
    )
    .expect("attempt scan")
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

fn attempt(id: &str, target_session: Option<&str>, fence: u64, payload: &[u8]) -> OperationAttempt {
    OperationAttempt::accepted(
        OperationAttemptId::new(id).expect("attempt ID"),
        target_session.map(|id| SessionId::new(id).expect("session ID")),
        ProcessMutationKind::Spawn,
        FencingToken::new(fence).expect("fencing token"),
        PayloadDigest::of_bytes(payload),
        TimestampMillis::new(10),
    )
}

fn dispatched_attempt(
    id: &str,
    target_session: &str,
    fence: u64,
    input: &[u8],
) -> OperationAttempt {
    OperationAttempt::accepted_with_dispatch(
        OperationAttemptId::new(id).expect("attempt ID"),
        Some(SessionId::new(target_session).expect("session ID")),
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
        RuntimeDispatchLimits::new(30_000, 1_024, 2_048, Some(4_096), Some(500)).expect("limits"),
        RuntimeOutputContract::new(
            "project-test-result/0.1.0-beta",
            PayloadDigest::of_bytes(b"output schema"),
            Classification::Confidential,
        )
        .expect("output contract"),
        RuntimeTelemetryPolicyRef::new("trust://telemetry/policies/internal")
            .expect("telemetry policy ref"),
        undercurrent_domain::RuntimeRetentionPolicyRef::new("trust://retention/policies/internal")
            .expect("retention policy ref"),
        vec![
            RuntimeCallbackRef::new("runtime-callback://example/workflow/v0.1.0-beta/progress")
                .expect("callback"),
            RuntimeCallbackRef::new("runtime-callback://example/workflow/v0.1.0-beta/terminal")
                .expect("callback"),
        ],
        TimestampMillis::new(10_000),
        TimestampMillis::new(20_000),
    )
    .expect("runtime dispatch policy")
}

fn settle_unknown(repository: &SqliteSessionRepository, attempt: &OperationAttempt) {
    block_on(OperationAttemptStore::accept(repository, attempt)).expect("accept attempt");
    block_on(OperationAttemptStore::settle(
        repository,
        &OperationAttemptSettlement {
            attempt_id: attempt.id().clone(),
            fencing_token: attempt.fencing_token(),
            certainty: EffectCertainty::Unknown,
            result_digest: None,
            settled_at: TimestampMillis::new(20),
        },
    ))
    .expect("settle unknown");
}

fn repair_actor(principal: &str) -> OperationAttemptRepairActor {
    OperationAttemptRepairActor::new(
        principal.to_owned(),
        Some("repair-tenant".to_owned()),
        "repair-authority".to_owned(),
    )
    .expect("repair actor")
}

struct AllowRepair;

impl OperationAttemptRepairAuthorizer for AllowRepair {
    fn authorize_repair<'a>(
        &'a self,
        _actor: &'a OperationAttemptRepairActor,
        _attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<OperationAttemptRepairAuthorization, PortError>> {
        Box::pin(async { Ok(OperationAttemptRepairAuthorization::Authorized) })
    }
}

struct RepairEvidence {
    digest: PayloadDigest,
    reference: OperationAttemptRepairEvidenceRef,
    observed: AtomicUsize,
    acknowledged: AtomicUsize,
}

impl RepairEvidence {
    fn known_applied(bytes: &[u8], reference: &str) -> Self {
        Self {
            digest: PayloadDigest::of_bytes(bytes),
            reference: OperationAttemptRepairEvidenceRef::new(reference.to_owned())
                .expect("evidence reference"),
            observed: AtomicUsize::new(0),
            acknowledged: AtomicUsize::new(0),
        }
    }
}

impl OperationAttemptRepairObserver for RepairEvidence {
    fn observe_repair<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<ObservedOperationAttemptRepair, PortError>> {
        Box::pin(async move {
            self.observed.fetch_add(1, Ordering::AcqRel);
            Ok(ObservedOperationAttemptRepair::new(
                attempt.id().clone(),
                attempt.fencing_token(),
                EffectCertainty::KnownApplied,
                Some(self.digest),
                self.reference.clone(),
            )
            .expect("repair observation"))
        })
    }

    fn acknowledge_repair<'a>(
        &'a self,
        _attempt: &'a OperationAttempt,
        evidence_reference: &'a OperationAttemptRepairEvidenceRef,
    ) -> PortFuture<'a, Result<(), PortError>> {
        Box::pin(async move {
            assert_eq!(evidence_reference, &self.reference);
            self.acknowledged.fetch_add(1, Ordering::AcqRel);
            Ok(())
        })
    }
}

struct FixedClock(TimestampMillis);

impl Clock for FixedClock {
    fn now(&self) -> TimestampMillis {
        self.0
    }
}

#[derive(Debug)]
struct CrashAt(SqliteCrashPoint);

impl SqliteCrashInjector for CrashAt {
    fn should_crash(&self, point: SqliteCrashPoint) -> bool {
        self.0 == point
    }
}

fn create_prerelease_database(path: &Path) {
    let connection = Connection::open(path).expect("create prerelease database");
    connection
        .execute_batch(
            "CREATE TABLE prerelease_sentinel (value TEXT NOT NULL) STRICT;
             INSERT INTO prerelease_sentinel (value) VALUES ('preserve-me');
             PRAGMA user_version = 4;",
        )
        .expect("create prerelease schema");
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct ThreadWake(std::thread::Thread);

    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::park(),
        }
    }
}

fn install_unsupported_release(path: &Path) {
    let connection = Connection::open(path).expect("open fixture");
    install_unsupported_release_identity(&connection);
}

fn install_unsupported_release_identity(connection: &Connection) {
    connection
        .execute("UPDATE schema_identity SET release_version = '99.0.0'", [])
        .expect("unsupported release fixture");
}
