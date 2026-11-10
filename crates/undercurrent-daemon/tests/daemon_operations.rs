// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use tempfile::TempDir;
use undercurrent_application::{
    AuthorizeDiagnosticBundleService, Clock, DiagnosticBundleRequest, OperationAttemptStore,
    PortError, PortErrorKind, PortFuture, PortOperation, ProcessHostShutdown,
    ProcessMutationContext, ShutdownProcessHostError,
};
use undercurrent_daemon::{
    AuthenticatedTransportAuthorization, BoundedDaemonOperations, DaemonDiagnosticField,
    DaemonDiagnosticQuery, DaemonDiagnosticRow, DaemonDiagnosticSection, DaemonDiagnosticSource,
    DaemonDiagnosticValue, DaemonOperationalPressure, DaemonOperationsError,
    DaemonOperationsLimits, DaemonOperationsPort, DaemonProductShutdownError, DaemonShutdownPhase,
    DaemonShutdownSignal, DaemonStatusSource,
};
use undercurrent_domain::{
    EffectCertainty, OperationAttempt, OperationAttemptId, OperationAttemptState, SessionId,
    TimestampMillis,
};
use undercurrent_store_sqlite::{
    SqliteRepositoryConfig, SqliteRepositoryLimits, SqliteSessionRepository,
};
use undercurrent_wire::{ClientId, ServerEpoch};

#[test]
fn diagnostic_continuations_are_consistent_client_scoped_snapshots() {
    let source = Arc::new(MutableDiagnosticSource::new(vec![
        row(DaemonDiagnosticSection::Streams, "stream-c", 3),
        row(DaemonDiagnosticSection::Configuration, "config-a", 9),
        row(DaemonDiagnosticSection::Streams, "stream-a", 1),
        row(DaemonDiagnosticSection::Streams, "stream-b", 2),
    ]));
    let clock = Arc::new(FixedClock::new(1_000));
    let operations = operations(
        clock,
        source.clone(),
        DaemonOperationsLimits::new(2, 4, 4, 16, Duration::from_secs(30)).expect("limits"),
    );
    let owner = client("owner");
    let selection = BTreeSet::from([DaemonDiagnosticSection::Streams]);

    let first = operations
        .diagnostics(
            &owner,
            DaemonDiagnosticQuery {
                sections: selection.clone(),
                page_size: 2,
                cursor: None,
            },
        )
        .expect("first page");
    assert_eq!(identities(&first.items), ["stream-a", "stream-b"]);
    assert_eq!(first.captured_at_millis, 1_000);
    let cursor = first.next_cursor.expect("continuation");
    assert_eq!(source.calls.load(Ordering::Acquire), 1);

    source.replace(vec![row(
        DaemonDiagnosticSection::Streams,
        "replacement",
        10,
    )]);
    let second = operations
        .diagnostics(
            &owner,
            DaemonDiagnosticQuery {
                sections: selection.clone(),
                page_size: 2,
                cursor: Some(cursor.clone()),
            },
        )
        .expect("continuation page");
    assert_eq!(identities(&second.items), ["stream-c"]);
    assert_eq!(second.captured_at_millis, first.captured_at_millis);
    assert_eq!(second.next_cursor, None);
    assert_eq!(source.calls.load(Ordering::Acquire), 1);

    let foreign = operations.diagnostics(
        &client("foreign"),
        DaemonDiagnosticQuery {
            sections: selection.clone(),
            page_size: 2,
            cursor: Some(cursor.clone()),
        },
    );
    assert_eq!(foreign.unwrap_err(), DaemonOperationsError::CursorExpired);

    operations.close_client(&owner);
    let closed = operations.diagnostics(
        &owner,
        DaemonDiagnosticQuery {
            sections: selection,
            page_size: 2,
            cursor: Some(cursor),
        },
    );
    assert_eq!(closed.unwrap_err(), DaemonOperationsError::CursorExpired);
    assert_eq!(operations.cursor_pressure().clients, 0);
}

#[test]
fn diagnostic_cursor_retention_is_oldest_first_and_hard_bounded() {
    let operations = operations(
        Arc::new(FixedClock::new(10)),
        Arc::new(MutableDiagnosticSource::new(vec![
            row(DaemonDiagnosticSection::Streams, "a", 1),
            row(DaemonDiagnosticSection::Streams, "b", 2),
        ])),
        DaemonOperationsLimits::new(1, 1, 1, 2, Duration::from_secs(30)).expect("limits"),
    );
    let owner = client("owner");
    let oldest_cursor = first_cursor(&operations, &owner);
    let second_cursor = first_cursor(&operations, &owner);

    assert_ne!(oldest_cursor, second_cursor);
    assert_eq!(operations.cursor_pressure().cursors, 1);
    assert_eq!(operations.cursor_pressure().evictions, 1);
    let expired = operations.diagnostics(
        &owner,
        DaemonDiagnosticQuery {
            sections: BTreeSet::new(),
            page_size: 1,
            cursor: Some(oldest_cursor),
        },
    );
    assert_eq!(expired.unwrap_err(), DaemonOperationsError::CursorExpired);

    let capacity = operations.diagnostics(
        &client("second-client"),
        DaemonDiagnosticQuery {
            sections: BTreeSet::new(),
            page_size: 1,
            cursor: None,
        },
    );
    assert_eq!(
        capacity.unwrap_err(),
        DaemonOperationsError::ClientCapacity { limit: 1 }
    );
}

#[test]
fn diagnostic_sources_cannot_escape_selection_or_snapshot_bounds() {
    let wrong_section = operations(
        Arc::new(FixedClock::new(10)),
        Arc::new(UnfilteredDiagnosticSource),
        DaemonOperationsLimits::new(1, 1, 1, 2, Duration::from_secs(30)).expect("limits"),
    );
    let error = wrong_section.diagnostics(
        &client("owner"),
        DaemonDiagnosticQuery {
            sections: BTreeSet::from([DaemonDiagnosticSection::Streams]),
            page_size: 1,
            cursor: None,
        },
    );
    assert_eq!(error.unwrap_err(), DaemonOperationsError::SourceInvariant);

    let duplicate = operations(
        Arc::new(FixedClock::new(10)),
        Arc::new(MutableDiagnosticSource::new(vec![
            row(DaemonDiagnosticSection::Streams, "duplicate", 1),
            row(DaemonDiagnosticSection::Streams, "duplicate", 2),
        ])),
        DaemonOperationsLimits::new(1, 1, 1, 2, Duration::from_secs(30)).expect("limits"),
    );
    let error = duplicate.diagnostics(
        &client("owner"),
        DaemonDiagnosticQuery {
            sections: BTreeSet::new(),
            page_size: 1,
            cursor: None,
        },
    );
    assert_eq!(error.unwrap_err(), DaemonOperationsError::SourceInvariant);

    let oversized = operations(
        Arc::new(FixedClock::new(10)),
        Arc::new(MutableDiagnosticSource::new(vec![
            row(DaemonDiagnosticSection::Streams, "a", 1),
            row(DaemonDiagnosticSection::Streams, "b", 2),
            row(DaemonDiagnosticSection::Streams, "c", 3),
        ])),
        DaemonOperationsLimits::new(1, 1, 1, 2, Duration::from_secs(30)).expect("limits"),
    );
    let error = oversized.diagnostics(
        &client("owner"),
        DaemonDiagnosticQuery {
            sections: BTreeSet::new(),
            page_size: 1,
            cursor: None,
        },
    );
    assert_eq!(
        error.unwrap_err(),
        DaemonOperationsError::SnapshotCapacity { limit: 2 }
    );
}

#[test]
fn diagnostic_bundle_redacts_terminal_content_unless_explicitly_authorized() {
    let operations = operations(
        Arc::new(FixedClock::new(42)),
        Arc::new(MutableDiagnosticSource::new(vec![row(
            DaemonDiagnosticSection::Streams,
            "safe-stream",
            1,
        )])),
        DaemonOperationsLimits::new(2, 2, 2, 4, Duration::from_secs(30)).expect("limits"),
    );
    let authorizer = AuthenticatedTransportAuthorization::new();
    let service = AuthorizeDiagnosticBundleService::new(&authorizer);
    let private = Arc::<[u8]>::from(&b"private-terminal-content\0\xff"[..]);

    let safe = block_on(service.authorize(DiagnosticBundleRequest::default()))
        .expect("safe bundle authorization");
    let safe_bundle = operations
        .create_diagnostic_bundle(
            safe,
            BTreeSet::from([DaemonDiagnosticSection::Streams]),
            Some(private.clone()),
        )
        .expect("safe diagnostic bundle");
    assert_eq!(safe_bundle.items().len(), 1);
    assert_eq!(safe_bundle.captured_at_millis(), 42);
    assert!(safe_bundle.terminal_content_redacted());
    assert_eq!(safe_bundle.terminal_content(), None);
    assert!(!format!("{safe_bundle:?}").contains("private-terminal-content"));

    let explicit = block_on(service.authorize(
        DiagnosticBundleRequest::safe().explicitly_include_terminal_content(
            SessionId::new("diagnostic-session").expect("diagnostic session id"),
        ),
    ))
    .expect("explicit content authorization");
    let explicit_bundle = operations
        .create_diagnostic_bundle(
            explicit,
            BTreeSet::from([DaemonDiagnosticSection::Streams]),
            Some(private.clone()),
        )
        .expect("explicit diagnostic bundle");
    assert!(!explicit_bundle.terminal_content_redacted());
    assert_eq!(explicit_bundle.terminal_content(), Some(private.as_ref()));
    assert!(!format!("{explicit_bundle:?}").contains("private-terminal-content"));
}

#[test]
fn shutdown_is_idempotent_and_immediately_closes_mutation_admission() {
    let clock = Arc::new(FixedClock::new(1_000));
    let operations = operations(
        clock.clone(),
        Arc::new(MutableDiagnosticSource::new(Vec::new())),
        DaemonOperationsLimits::new(1, 1, 1, 2, Duration::from_secs(30)).expect("limits"),
    );
    assert!(
        operations
            .status()
            .expect("ready status")
            .accepting_connections
    );

    let deadline = operations
        .begin_shutdown(Duration::from_secs(5))
        .expect("shutdown");
    assert_eq!(deadline, 6_000);
    assert_eq!(
        operations
            .shutdown_signal()
            .wait_timeout(Duration::from_millis(1)),
        Some(deadline)
    );
    assert!(
        operations
            .status()
            .expect("draining status")
            .accepting_connections
    );
    assert!(matches!(
        operations.admit_mutation(),
        Err(DaemonOperationsError::Draining)
    ));

    clock.set(2_000);
    assert_eq!(
        operations
            .begin_shutdown(Duration::from_secs(1))
            .expect("repeated shutdown"),
        deadline
    );
    assert_eq!(
        operations.begin_shutdown(Duration::ZERO).unwrap_err(),
        DaemonOperationsError::InvalidDrainTimeout {
            requested: Duration::ZERO,
            maximum: Duration::from_secs(30),
        }
    );
}

#[test]
fn product_shutdown_drains_live_mutation_then_settles_and_acks_before_finalizing() {
    let directory = TempDir::new().expect("temporary repository directory");
    let attempts = sqlite_attempt_store(&directory);
    let host = Arc::new(RecordingShutdownHost::new(
        attempts.clone(),
        ShutdownEffect::KnownApplied,
    ));
    let clock = Arc::new(FixedClock::new(1_000));
    let operations = durable_operations(clock, attempts.clone(), host.clone());
    let live_mutation = operations.admit_mutation().expect("live mutation permit");
    let shutdown = operations.shutdown_signal().clone();
    let shutdown_operations = operations.clone();
    let worker =
        thread::spawn(move || shutdown_operations.begin_product_shutdown(Duration::from_secs(5)));

    assert_eq!(
        shutdown.wait_phase_timeout(Duration::from_secs(1)),
        DaemonShutdownPhase::Draining
    );
    assert!(matches!(
        operations.admit_mutation(),
        Err(DaemonOperationsError::Draining)
    ));
    assert_eq!(host.effect_calls.load(Ordering::Acquire), 0);
    assert_eq!(host.acknowledgement_calls.load(Ordering::Acquire), 0);

    drop(live_mutation);
    let deadline = worker
        .join()
        .expect("product shutdown worker")
        .expect("durable product shutdown");

    assert_eq!(deadline, 6_000);
    assert_eq!(
        shutdown.wait_for_finalization(Duration::from_secs(1)),
        DaemonShutdownPhase::Finalizing(deadline)
    );
    assert_eq!(host.trace(), ["effect", "ack"]);
    assert_eq!(host.effect_calls.load(Ordering::Acquire), 1);
    assert_eq!(host.acknowledgement_calls.load(Ordering::Acquire), 1);
    let attempt_id = host.attempt_id();
    let durable = block_on(attempts.load(&attempt_id))
        .expect("load shutdown attempt")
        .expect("durable shutdown attempt");
    assert!(matches!(
        durable.state(),
        OperationAttemptState::Settled {
            certainty: EffectCertainty::KnownApplied,
            result_digest: Some(_),
            ..
        }
    ));
}

#[test]
fn unknown_product_shutdown_remains_draining_and_exact_retry_has_no_host_io() {
    let directory = TempDir::new().expect("temporary repository directory");
    let attempts = sqlite_attempt_store(&directory);
    let host = Arc::new(RecordingShutdownHost::new(
        attempts.clone(),
        ShutdownEffect::Unknown,
    ));
    let clock = Arc::new(FixedClock::new(1_000));
    let operations = durable_operations(clock, attempts.clone(), host.clone());

    let first = operations
        .begin_product_shutdown(Duration::from_secs(5))
        .expect_err("lost host response remains Unknown");
    assert!(matches!(
        first,
        DaemonProductShutdownError::Host(ShutdownProcessHostError::EffectUnknown(_))
    ));
    assert_eq!(
        operations
            .shutdown_signal()
            .wait_phase_timeout(Duration::from_millis(1)),
        DaemonShutdownPhase::Draining
    );
    assert!(matches!(
        operations.admit_mutation(),
        Err(DaemonOperationsError::Draining)
    ));

    let retry = operations
        .begin_product_shutdown(Duration::from_secs(5))
        .expect_err("exact Unknown retry fails closed");
    assert!(matches!(
        retry,
        DaemonProductShutdownError::Host(ShutdownProcessHostError::UnresolvedEffect { .. })
    ));
    assert_eq!(
        operations
            .shutdown_signal()
            .wait_phase_timeout(Duration::from_millis(1)),
        DaemonShutdownPhase::Draining
    );
    assert_eq!(host.trace(), ["effect"]);
    assert_eq!(host.effect_calls.load(Ordering::Acquire), 1);
    assert_eq!(host.acknowledgement_calls.load(Ordering::Acquire), 0);
    let attempt_id = host.attempt_id();
    let durable = block_on(attempts.load(&attempt_id))
        .expect("load shutdown attempt")
        .expect("durable shutdown attempt");
    assert!(matches!(
        durable.state(),
        OperationAttemptState::Settled {
            certainty: EffectCertainty::Unknown,
            result_digest: None,
            ..
        }
    ));
}

#[test]
fn known_absent_product_shutdown_reopens_admission_and_terminalizes_attempt() {
    let directory = TempDir::new().expect("temporary repository directory");
    let attempts = sqlite_attempt_store(&directory);
    let host = Arc::new(RecordingShutdownHost::new(
        attempts.clone(),
        ShutdownEffect::KnownAbsent,
    ));
    let clock = Arc::new(FixedClock::new(1_000));
    let operations = durable_operations(clock, attempts.clone(), host.clone());

    let first = operations
        .begin_product_shutdown(Duration::from_secs(5))
        .expect_err("host rejects shutdown before an effect");
    assert!(matches!(
        first,
        DaemonProductShutdownError::Host(ShutdownProcessHostError::RejectedKnownAbsent { .. })
    ));
    assert_eq!(
        operations
            .shutdown_signal()
            .wait_phase_timeout(Duration::from_millis(1)),
        DaemonShutdownPhase::Running
    );
    drop(operations.admit_mutation().expect("admission reopened"));

    let retry = operations
        .begin_product_shutdown(Duration::from_secs(5))
        .expect_err("exact known-absent attempt is terminal");
    assert!(matches!(
        retry,
        DaemonProductShutdownError::Host(ShutdownProcessHostError::KnownAbsent { .. })
    ));
    assert_eq!(
        operations
            .shutdown_signal()
            .wait_phase_timeout(Duration::from_millis(1)),
        DaemonShutdownPhase::Running
    );
    assert_eq!(host.trace(), ["effect"]);
    assert_eq!(host.effect_calls.load(Ordering::Acquire), 1);
    assert_eq!(host.acknowledgement_calls.load(Ordering::Acquire), 0);
    let attempt_id = host.attempt_id();
    let durable = block_on(attempts.load(&attempt_id))
        .expect("load shutdown attempt")
        .expect("durable shutdown attempt");
    assert!(matches!(
        durable.state(),
        OperationAttemptState::Settled {
            certainty: EffectCertainty::KnownAbsent,
            result_digest: None,
            ..
        }
    ));
}

fn operations(
    clock: Arc<FixedClock>,
    diagnostics: Arc<dyn DaemonDiagnosticSource>,
    limits: DaemonOperationsLimits,
) -> BoundedDaemonOperations {
    BoundedDaemonOperations::new(
        ServerEpoch::new("test-epoch").expect("epoch"),
        5,
        clock,
        Arc::new(FixedStatus),
        vec![diagnostics],
        Arc::new(DaemonShutdownSignal::default()),
        limits,
    )
}

fn durable_operations(
    clock: Arc<FixedClock>,
    attempts: Arc<SqliteSessionRepository>,
    host: Arc<RecordingShutdownHost>,
) -> Arc<BoundedDaemonOperations> {
    Arc::new(
        operations(
            clock,
            Arc::new(MutableDiagnosticSource::new(Vec::new())),
            DaemonOperationsLimits::new(1, 1, 1, 2, Duration::from_secs(30)).expect("limits"),
        )
        .with_process_host_shutdown(attempts, host),
    )
}

fn sqlite_attempt_store(directory: &TempDir) -> Arc<SqliteSessionRepository> {
    let limits =
        SqliteRepositoryLimits::new(8, 32, 512 * 1_024, 32, 32).expect("SQLite repository limits");
    let config = SqliteRepositoryConfig::new(
        directory.path().join("state").join("undercurrent.sqlite"),
        limits,
        Duration::from_secs(1),
    )
    .expect("SQLite repository configuration");
    Arc::new(SqliteSessionRepository::open(config).expect("open SQLite attempt store"))
}

fn first_cursor(operations: &BoundedDaemonOperations, owner: &ClientId) -> String {
    operations
        .diagnostics(
            owner,
            DaemonDiagnosticQuery {
                sections: BTreeSet::new(),
                page_size: 1,
                cursor: None,
            },
        )
        .expect("first page")
        .next_cursor
        .expect("cursor")
}

fn row(section: DaemonDiagnosticSection, identity: &str, count: u64) -> DaemonDiagnosticRow {
    DaemonDiagnosticRow {
        section,
        identity: identity.to_owned(),
        fields: BTreeMap::from([(
            DaemonDiagnosticField::Count,
            DaemonDiagnosticValue::Unsigned(count),
        )]),
    }
}

fn identities<const N: usize>(items: &[DaemonDiagnosticRow]) -> [&str; N] {
    std::array::from_fn(|index| items[index].identity.as_str())
}

fn client(value: &str) -> ClientId {
    ClientId::new(value).expect("client ID")
}

struct FixedClock(AtomicI64);

impl FixedClock {
    fn new(value: i64) -> Self {
        Self(AtomicI64::new(value))
    }

    fn set(&self, value: i64) {
        self.0.store(value, Ordering::Release);
    }
}

impl Clock for FixedClock {
    fn now(&self) -> TimestampMillis {
        TimestampMillis::new(self.0.load(Ordering::Acquire))
    }
}

struct FixedStatus;

impl DaemonStatusSource for FixedStatus {
    fn pressure(&self) -> Result<DaemonOperationalPressure, DaemonOperationsError> {
        Ok(DaemonOperationalPressure {
            accepting_connections: true,
            physical_connections: 3,
            logical_streams: 7,
            managed_sessions: 300,
        })
    }
}

struct MutableDiagnosticSource {
    rows: Mutex<Vec<DaemonDiagnosticRow>>,
    calls: AtomicUsize,
}

impl MutableDiagnosticSource {
    fn new(rows: Vec<DaemonDiagnosticRow>) -> Self {
        Self {
            rows: Mutex::new(rows),
            calls: AtomicUsize::new(0),
        }
    }

    fn replace(&self, rows: Vec<DaemonDiagnosticRow>) {
        *self
            .rows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = rows;
    }
}

impl DaemonDiagnosticSource for MutableDiagnosticSource {
    fn snapshot(
        &self,
        sections: &BTreeSet<DaemonDiagnosticSection>,
    ) -> Result<Vec<DaemonDiagnosticRow>, DaemonOperationsError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Ok(self
            .rows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|row| sections.contains(&row.section))
            .cloned()
            .collect())
    }
}

struct UnfilteredDiagnosticSource;

impl DaemonDiagnosticSource for UnfilteredDiagnosticSource {
    fn snapshot(
        &self,
        _sections: &BTreeSet<DaemonDiagnosticSection>,
    ) -> Result<Vec<DaemonDiagnosticRow>, DaemonOperationsError> {
        Ok(vec![row(
            DaemonDiagnosticSection::Configuration,
            "wrong-section",
            1,
        )])
    }
}

#[derive(Clone, Copy)]
enum ShutdownEffect {
    KnownApplied,
    KnownAbsent,
    Unknown,
}

struct RecordingShutdownHost {
    attempts: Arc<SqliteSessionRepository>,
    effect: ShutdownEffect,
    effect_calls: AtomicUsize,
    acknowledgement_calls: AtomicUsize,
    attempt_id: Mutex<Option<OperationAttemptId>>,
    trace: Mutex<Vec<&'static str>>,
}

impl RecordingShutdownHost {
    fn new(attempts: Arc<SqliteSessionRepository>, effect: ShutdownEffect) -> Self {
        Self {
            attempts,
            effect,
            effect_calls: AtomicUsize::new(0),
            acknowledgement_calls: AtomicUsize::new(0),
            attempt_id: Mutex::new(None),
            trace: Mutex::new(Vec::new()),
        }
    }

    fn attempt_id(&self) -> OperationAttemptId {
        self.attempt_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
            .expect("recorded shutdown attempt")
    }

    fn trace(&self) -> Vec<&'static str> {
        self.trace
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl ProcessHostShutdown for RecordingShutdownHost {
    fn shutdown_attempted<'a>(
        &'a self,
        context: &'a ProcessMutationContext,
    ) -> PortFuture<'a, Result<(), PortError>> {
        self.effect_calls.fetch_add(1, Ordering::AcqRel);
        *self
            .attempt_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(context.attempt_id().clone());
        self.trace
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push("effect");
        let effect = self.effect;
        let attempt_id = context.attempt_id().clone();
        Box::pin(async move {
            match effect {
                ShutdownEffect::KnownApplied => Ok(()),
                ShutdownEffect::KnownAbsent => Err(PortError::new(
                    PortErrorKind::Conflict,
                    PortOperation::ShutdownProcessHost,
                    None,
                    TestPortFailure,
                )
                .with_attempt_effect(attempt_id, EffectCertainty::KnownAbsent)),
                ShutdownEffect::Unknown => Err(PortError::new(
                    PortErrorKind::Unavailable,
                    PortOperation::ShutdownProcessHost,
                    None,
                    TestPortFailure,
                )
                .with_attempt_effect(attempt_id, EffectCertainty::Unknown)),
            }
        })
    }

    fn acknowledge_shutdown<'a>(
        &'a self,
        attempt: &'a OperationAttempt,
    ) -> PortFuture<'a, Result<(), PortError>> {
        self.acknowledgement_calls.fetch_add(1, Ordering::AcqRel);
        self.trace
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push("ack");
        Box::pin(async move {
            let durable = self
                .attempts
                .load(attempt.id())
                .await?
                .expect("settlement must precede host acknowledgement");
            assert_eq!(&durable, attempt);
            assert!(matches!(
                durable.state(),
                OperationAttemptState::Settled {
                    certainty: EffectCertainty::KnownApplied,
                    result_digest: Some(_),
                    ..
                }
            ));
            Ok(())
        })
    }
}

#[derive(Debug)]
struct TestPortFailure;

impl Display for TestPortFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("test process-host failure")
    }
}

impl Error for TestPortFailure {}

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::yield_now(),
        }
    }
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn reset_context() -> undercurrent_daemon::ProductRequestContext {
    use undercurrent_protocol::{
        AuthenticatedPrincipalRef, AuthenticatedRequestContext, AuthorityRefDto,
    };
    use undercurrent_wire::{ConnectionId, RequestId, SpanId, TraceContext, TraceId};
    undercurrent_daemon::ProductRequestContext {
        authentication: Arc::new(AuthenticatedRequestContext::new(
            AuthenticatedPrincipalRef::new("reset-test").unwrap(),
            None,
            AuthorityRefDto::new("local").unwrap(),
        )),
        connection_id: ConnectionId::new("reset-connection").unwrap(),
        server_epoch: ServerEpoch::new("reset-epoch").unwrap(),
        client_id: ClientId::new("reset-client").unwrap(),
        request_id: RequestId::new("reset-request").unwrap(),
        trace_context: TraceContext::new(
            TraceId::new("reset-trace").unwrap(),
            SpanId::new("reset-span").unwrap(),
        ),
        cancellation: undercurrent_daemon::ProductCancellation::new(),
        response_write: Arc::default(),
    }
}

#[test]
fn reset_settles_and_acknowledges_host_before_bounded_response_delivery() {
    let directory = TempDir::new().unwrap();
    let attempts = sqlite_attempt_store(&directory);
    let host = Arc::new(RecordingShutdownHost::new(
        attempts.clone(),
        ShutdownEffect::KnownApplied,
    ));
    let clock = Arc::new(FixedClock::new(1_000));
    let operations = durable_operations(clock, attempts, host.clone());
    let context = reset_context();
    let deadline = operations
        .begin_reset_shutdown(Duration::from_secs(30), &context)
        .unwrap();
    assert_eq!(host.trace(), ["effect", "ack"]);
    assert_eq!(
        deadline, 1_250,
        "reset reserves only a bounded response-write window"
    );
    assert!(operations.reset_requested());
    assert!(
        !context.response_write.is_finished(),
        "settlement is not delivery"
    );
    assert_eq!(
        operations
            .shutdown_signal()
            .wait_phase_timeout(Duration::ZERO),
        DaemonShutdownPhase::Finalizing(deadline)
    );
}

#[test]
fn reset_mutation_timeout_keeps_admission_closed_and_does_not_retire_host() {
    let directory = TempDir::new().unwrap();
    let attempts = sqlite_attempt_store(&directory);
    let host = Arc::new(RecordingShutdownHost::new(
        attempts.clone(),
        ShutdownEffect::KnownApplied,
    ));
    let clock = Arc::new(FixedClock::new(1_000));
    let operations = durable_operations(clock, attempts, host.clone());
    let mutation = operations.admit_mutation().unwrap();
    let error = operations
        .begin_reset_shutdown(Duration::from_millis(1), &reset_context())
        .unwrap_err();
    assert!(matches!(
        error,
        DaemonProductShutdownError::MutationDrainTimedOut
    ));
    assert!(matches!(
        operations.admit_mutation(),
        Err(DaemonOperationsError::Draining)
    ));
    assert!(host.trace().is_empty());
    drop(mutation);
    operations
        .begin_reset_shutdown(Duration::from_secs(5), &reset_context())
        .expect("retry may extend the observation budget after cancellation settles");
    assert_eq!(host.trace(), ["effect", "ack"]);
}
