// SPDX-License-Identifier: Apache-2.0

//! Linux real-host crash and response-loss coverage for private mutation receipts.

use std::fs;
use std::future::Future;
use std::io::{Read, Write};
use std::net::Shutdown;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use undercurrent_application::shutdown_process_host::ProcessHostShutdown;
use undercurrent_application::{
    Clock, DurableProcessRuntime, EphemeralSecret, ImmutableExecutionOutputRef,
    ImmutableExecutionOutputStore, ObservedOperationAttemptEffect, OperationAttemptAdmission,
    OperationAttemptEffectObserver, OperationAttemptRepairActor,
    OperationAttemptRepairAuthorization, OperationAttemptRepairAuthorizer,
    OperationAttemptRepairObserver, OperationAttemptScan, OperationAttemptStore, PortError,
    PortFuture, ProcessExitSource, ProcessInstance, ProcessMutationContext, ProcessRuntime,
    ProcessTruthSource, RepairOperationAttemptCommand, RepairOperationAttemptService,
    SecretResolver, Telemetry, process_instance_result_digest,
};
use undercurrent_domain::{
    CommandSpec, EffectCertainty, EnvironmentVariables, ExecutionLimits, FencingToken, InputItem,
    InputSequence, Key, LaunchSpec, OperationAttempt, OperationAttemptId, OperationAttemptState,
    PayloadDigest, ProcessInstanceId, ProcessMutationKind, SecretEnvironmentVariables,
    SecretReference, SessionId, StopPolicy, StopSignal, TerminalSize, TimestampMillis,
};
use undercurrent_store_sqlite::{
    SqliteRepositoryConfig, SqliteRepositoryLimits, SqliteSessionRepository,
};

use super::{
    HostedOperationAttemptEffectObserver, HostedProcessRuntime, HostedProcessRuntimeConfig,
    HostedSessionCommandExecutor, NativeProcessRuntimeConfig, NativeProcessRuntimeLimits,
    ProcessHostBootstrap, run_process_host,
};
use crate::process_host_protocol::{HostCodec, HostCommand, HostRequest, HostResponseValue};
use crate::{BoundedTelemetryRecorder, SystemClock};

const HOST_MARKER_ENV: &str = "UNDERCURRENT_RUNTIME_TEST_RECEIPT_HOST";
const MODE_ENV: &str = "UNDERCURRENT_RUNTIME_TEST_RECEIPT_MODE";
const PROCESS_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);

#[test]
fn receipt_host_subprocess() {
    if std::env::var(HOST_MARKER_ENV).as_deref() != Ok("1") {
        return;
    }
    let bootstrap = ProcessHostBootstrap::read_from(std::io::stdin().lock()).expect("bootstrap");
    run_process_host(bootstrap).expect("test process host");
}

#[test]
fn real_host_accepts_configured_execution_output_bound_above_screen_snapshot() {
    let directory = tempfile::tempdir().expect("temporary host directory");
    let harness = HostHarness::start(directory.path(), "none", "execution-capacity");
    let session = SessionId::new("execution-capacity-session").expect("session ID");
    assert_eq!(
        harness.runtime.maximum_session_command_output_bytes(),
        2 * 1_024 * 1_024,
    );
    let response = block_on(harness.runtime.session_command_request(
        &session,
        HostCommand::Execute {
            live_environment_process: None,
            job_id: "execution-capacity-attempt".to_owned(),
            session_id: session.as_str().to_owned(),
            payload_digest: "00".repeat(32),
            program: "/bin/true".to_owned(),
            arguments: Vec::new(),
            environment: Vec::new(),
            working_directory: None,
            output_bytes: 2 * 1_024 * 1_024,
            deadline_millis: None,
        },
    ))
    .expect("host accepts configured output bound");
    assert!(matches!(response, HostResponseValue::ExecutionAccepted));
}

#[test]
fn private_uchs_rejects_unauthorized_peers_and_keeps_every_artifact_owner_only() {
    let directory = tempfile::tempdir().expect("temporary host directory");
    let host_report = directory.path().join("host-private-uchs.pid");
    let wrapper = write_host_wrapper(directory.path(), "none", &host_report);
    let config = hosted_config(directory.path(), wrapper.clone());
    let socket = config.socket().to_path_buf();
    let authentication = ProcessHostBootstrap::authentication_path(&socket);
    let execution_journal = ProcessHostBootstrap::execution_journal_path(&socket);
    let attempt_journal = ProcessHostBootstrap::attempt_receipt_journal_path(&socket);
    let token = "11".repeat(32);
    let bootstrap = ProcessHostBootstrap::new(socket.clone(), &token, config.native())
        .expect("private host bootstrap");
    let mut host = Command::new(&wrapper)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("launch private host directly");
    let mut stdin = host.stdin.take().expect("private host stdin");
    bootstrap
        .write_to(&mut stdin)
        .expect("write private host bootstrap");
    drop(stdin);

    wait_until(PROCESS_CONVERGENCE_TIMEOUT, || {
        socket.exists()
            && authentication.exists()
            && execution_journal.exists()
            && attempt_journal.exists()
    });
    let initial_host = wait_for_identity(&host_report);
    assert_path_mode(directory.path(), 0o700);
    assert_path_mode(&socket, 0o600);
    assert_path_mode(&authentication, 0o600);
    assert_path_mode(&execution_journal, 0o700);
    assert_path_mode(&attempt_journal, 0o700);

    assert_uchs_peer_rejected(
        &socket,
        vec![HostRequest {
            request_id: 41,
            command: HostCommand::Shutdown,
        }],
    );
    assert!(
        process_identity_matches(initial_host),
        "semantic-first peer activated the private host"
    );
    assert_uchs_peer_rejected(
        &socket,
        vec![
            HostRequest {
                request_id: 42,
                command: HostCommand::Authenticate {
                    token: "22".repeat(32),
                },
            },
            HostRequest {
                request_id: 43,
                command: HostCommand::Shutdown,
            },
        ],
    );
    assert!(
        process_identity_matches(initial_host),
        "wrong-token peer activated its pipelined semantic command"
    );

    let telemetry: Arc<dyn Telemetry> =
        Arc::new(BoundedTelemetryRecorder::new(128, 128).expect("telemetry"));
    let runtime = HostedProcessRuntime::start(config, Arc::new(SystemClock), telemetry)
        .expect("valid client authenticates after rejected peers");
    let session = SessionId::new("private-uchs-permissions").expect("session ID");
    let response = block_on(runtime.session_command_request(
        &session,
        HostCommand::Execute {
            live_environment_process: None,
            job_id: "permission-job".to_owned(),
            session_id: session.as_str().to_owned(),
            payload_digest: "00".repeat(32),
            program: "/bin/sleep".to_owned(),
            arguments: vec!["30".to_owned()],
            environment: Vec::new(),
            working_directory: None,
            output_bytes: 4_096,
            deadline_millis: None,
        },
    ))
    .expect("execution journal entry accepted");
    assert!(matches!(response, HostResponseValue::ExecutionAccepted));
    let spawn = MutationFixture::prepare(&runtime, directory.path(), GenericKind::Spawn);
    let spawned = apply_mutation(&runtime, &spawn)
        .expect("receipt mutation applies")
        .expect("spawn result");
    assert_private_journal(&execution_journal);
    assert_private_journal(&attempt_journal);

    cleanup_fixture(&runtime, &spawn, Some(&spawned));
    block_on(ProcessRuntime::acknowledge_attempt(
        &runtime,
        &spawn.attempt,
    ))
    .expect("acknowledge inspected receipt");
    let shutdown = MutationFixture::prepare(&runtime, directory.path(), GenericKind::Shutdown);
    apply_mutation(&runtime, &shutdown).expect("record shutdown receipt");
    block_on(ProcessHostShutdown::acknowledge_shutdown(
        &runtime,
        &shutdown.attempt,
    ))
    .expect("retire inspected host");
    host.wait().expect("reap private host");
    assert_identity_gone(initial_host);
    assert_host_artifacts_absent(directory.path());
}

#[test]
fn admitted_start_uses_real_host_and_sqlite_instead_of_unsupported_default() {
    let directory = tempfile::tempdir().expect("temporary host directory");
    let harness = HostHarness::start(directory.path(), "none", "admitted-start");
    let fixture = MutationFixture::prepare(&harness.runtime, directory.path(), GenericKind::Spawn);
    let (runtime, repository) = durable_runtime(&harness, directory.path());

    let spawned = apply_admitted(&runtime, &fixture)
        .expect("admitted real-host spawn")
        .expect("spawn result");
    assert_stored_known_applied(
        repository.as_ref(),
        &fixture,
        process_instance_result_digest(&spawned),
    );
    assert_eq!(
        retry_observe(&harness.runtime, &fixture.attempt).certainty,
        EffectCertainty::Unknown,
        "receipt must be released only after SQLite settlement commits",
    );
    cleanup_fixture(&harness.runtime, &fixture, Some(&spawned));
}

#[test]
fn receipt_save_capacity_after_input_never_claims_absence() {
    let directory = tempfile::tempdir().unwrap();
    let harness = HostHarness::start(
        directory.path(),
        "receipt_commit_capacity",
        "input-receipt-save",
    );
    let fixture =
        MutationFixture::prepare(&harness.runtime, directory.path(), GenericKind::WriteInput);
    let (runtime, repository) = durable_runtime(&harness, directory.path());
    let error = apply_admitted(&runtime, &fixture).expect_err("injected receipt save failure");
    assert_eq!(
        error.kind(),
        undercurrent_application::PortErrorKind::CapacityExceeded
    );
    assert_eq!(error.effect_certainty(), Some(EffectCertainty::Unknown));
    let stored = block_on(repository.load(fixture.attempt.id()))
        .unwrap()
        .unwrap();
    assert!(matches!(
        stored.state(),
        OperationAttemptState::Settled {
            certainty: EffectCertainty::Unknown,
            ..
        }
    ));
    cleanup_fixture(&harness.runtime, &fixture, None);
}

fn naturally_exited_fixture(
    harness: &HostHarness,
    directory: &Path,
    kind: GenericKind,
) -> MutationFixture {
    let fixture = MutationFixture::prepare(&harness.runtime, directory, kind);
    let process = fixture.process.as_ref().expect("process");
    let session = fixture.session.as_ref().expect("session");
    block_on(harness.runtime.arm_exit(session, &process.id)).expect("arm exact exit");
    let identity = wait_for_identity(&fixture.process_report);
    assert!(process_identity_matches(identity));
    nix::sys::signal::killpg(Pid::from_raw(identity.pid as i32), Signal::SIGTERM)
        .expect("natural group exit");
    let exit = block_on(harness.runtime.next_exit()).expect("retire exact exited process");
    assert_eq!(exit.process_id, process.id);
    assert!(
        matches!(exit.outcome, undercurrent_domain::ProcessOutcome::Exited(_)),
        "expected confirmed exit: {exit:?}"
    );
    fixture
}

fn exited_input_fixture(harness: &HostHarness, directory: &Path) -> MutationFixture {
    let mut fixture = naturally_exited_fixture(harness, directory, GenericKind::Stop);
    fixture.kind = GenericKind::WriteInput;
    fixture.attempt = OperationAttempt::accepted(
        OperationAttemptId::new("input-after-exit").unwrap(),
        fixture.session.clone(),
        ProcessMutationKind::WriteInput,
        FencingToken::new(1).unwrap(),
        PayloadDigest::of_bytes(b"input-after-exit"),
        TimestampMillis::new(10),
    );
    fixture.context = ProcessMutationContext::from_attempt(&fixture.attempt);
    fixture.admission = OperationAttemptAdmission::new(
        fixture.attempt.id().clone(),
        fixture.session.clone(),
        ProcessMutationKind::WriteInput,
        fixture.attempt.payload_digest(),
        TimestampMillis::new(10),
    );
    fixture
}

#[test]
fn input_after_confirmed_exit_settles_absent_without_recovery_debt() {
    let directory = tempfile::tempdir().unwrap();
    let harness = HostHarness::start(directory.path(), "none", "input-after-exit");
    let fixture = exited_input_fixture(&harness, directory.path());
    let (runtime, repository) = durable_runtime(&harness, directory.path());
    let error = apply_admitted(&runtime, &fixture).expect_err("retired process rejects input");
    assert_eq!(
        error.kind(),
        undercurrent_application::PortErrorKind::NotFound
    );
    assert_eq!(error.effect_certainty(), Some(EffectCertainty::KnownAbsent));
    let stored = block_on(repository.load(fixture.attempt.id()))
        .unwrap()
        .unwrap();
    assert!(matches!(
        stored.state(),
        OperationAttemptState::Settled {
            certainty: EffectCertainty::KnownAbsent,
            ..
        }
    ));
    assert!(
        block_on(
            repository
                .pending_reconciliation(&OperationAttemptScan::without_exclusions(8).unwrap())
        )
        .unwrap()
        .is_empty()
    );
}

#[test]
fn rejected_input_receipt_survives_host_restart_and_replays_absence() {
    let directory = tempfile::tempdir().unwrap();
    let harness = HostHarness::start(directory.path(), "none", "input-absence-replay");
    let fixture = exited_input_fixture(&harness, directory.path());
    let first = apply_mutation(&harness.runtime, &fixture).unwrap_err();
    assert_eq!(first.effect_certainty(), Some(EffectCertainty::KnownAbsent));
    kill_exact(harness.initial_host, Signal::SIGKILL);
    harness.wait_for_restart();
    assert_eq!(
        retry_observe(&harness.runtime, &fixture.attempt).certainty,
        EffectCertainty::KnownAbsent
    );
    let replay = apply_mutation(&harness.runtime, &fixture).unwrap_err();
    assert_eq!(replay.kind(), first.kind());
    assert_eq!(
        replay.effect_certainty(),
        Some(EffectCertainty::KnownAbsent)
    );
}

#[test]
fn unknown_input_retirement_requires_quiescence_and_rejects_live_replacement() {
    let directory = tempfile::tempdir().unwrap();
    let harness = HostHarness::start(
        directory.path(),
        "input_receipt_unknown",
        "input-retirement",
    );
    let fixture = exited_input_fixture(&harness, directory.path());
    assert_eq!(
        apply_mutation(&harness.runtime, &fixture)
            .unwrap_err()
            .effect_certainty(),
        Some(EffectCertainty::Unknown)
    );
    assert!(
        block_on(harness.runtime.observe_input_retirement(&fixture.attempt))
            .unwrap()
            .is_some()
    );
    assert_eq!(
        retry_observe(&harness.runtime, &fixture.attempt).certainty,
        EffectCertainty::Unknown
    );
    let session = fixture.session.as_ref().unwrap();
    let replacement = block_on(harness.runtime.spawn(session, &fixture.launch)).unwrap();
    assert!(
        block_on(harness.runtime.observe_input_retirement(&fixture.attempt))
            .unwrap()
            .is_none()
    );
    block_on(harness.runtime.force_stop(session, &replacement.id)).unwrap();
}

#[test]
fn real_host_stop_after_exact_retirement_retains_terminal_receipt() {
    for kind in [GenericKind::Stop, GenericKind::ForceStop] {
        let directory = tempfile::tempdir().unwrap();
        let harness = HostHarness::start(directory.path(), "none", kind.name());
        let fixture = naturally_exited_fixture(&harness, directory.path(), kind);
        apply_mutation(&harness.runtime, &fixture)
            .expect("exact retired process is already terminated");
        assert_eq!(
            retry_observe(&harness.runtime, &fixture.attempt).certainty,
            EffectCertainty::KnownApplied
        );
        let wrong = ProcessInstanceId::new("different-incarnation").unwrap();
        let error = block_on(harness.runtime.force_stop_attempted(
            &fixture.context,
            fixture.session.as_ref().unwrap(),
            &wrong,
        ))
        .expect_err("cannot replay for another process");
        assert!(matches!(
            error.effect_certainty(),
            Some(EffectCertainty::KnownAbsent)
        ));
    }
}

#[test]
fn real_host_unknown_stop_repairs_from_durable_exact_exit_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    let harness = HostHarness::start(directory.path(), "stop_receipt_unknown", "stop-repair");
    let fixture = naturally_exited_fixture(&harness, directory.path(), GenericKind::ForceStop);
    let (runtime, repository) = durable_runtime(&harness, directory.path());
    let error =
        apply_admitted(&runtime, &fixture).expect_err("retain injected unknown stop receipt");
    assert_eq!(error.effect_certainty(), Some(EffectCertainty::Unknown));
    kill_exact(harness.initial_host, Signal::SIGKILL);
    harness.wait_for_restart();
    assert_eq!(
        retry_known_observe(&harness.runtime, &fixture.attempt).certainty,
        EffectCertainty::KnownApplied
    );
    let observer = HostedOperationAttemptEffectObserver::new(
        harness.runtime.clone(),
        HostedSessionCommandExecutor::new(harness.runtime.clone(), Arc::new(UnusedContentStore)),
    );
    let repair = block_on(observer.observe_repair(&fixture.attempt))
        .expect("durable exact exit repairs bound receipt");
    assert_eq!(repair.certainty(), EffectCertainty::KnownApplied);
    let actor =
        OperationAttemptRepairActor::new("test".to_owned(), None, "test-authority".to_owned())
            .unwrap();
    let service = RepairOperationAttemptService::new(
        &AllowRepair,
        repository.as_ref(),
        &observer,
        &SystemClock,
        8,
    )
    .unwrap();
    block_on(service.execute(RepairOperationAttemptCommand::new(
        fixture.attempt.id().clone(),
        actor,
    )))
    .expect("authorized repair commits SQLite before host receipt acknowledgement");
    assert!(
        block_on(OperationAttemptStore::pending_reconciliation(
            repository.as_ref(),
            &OperationAttemptScan::without_exclusions(8).unwrap()
        ))
        .unwrap()
        .is_empty()
    );
    assert_stored_known_applied(
        repository.as_ref(),
        &fixture,
        *repair.result_digest().unwrap(),
    );
}

struct AllowRepair;
impl OperationAttemptRepairAuthorizer for AllowRepair {
    fn authorize_repair<'a>(
        &'a self,
        _actor: &'a OperationAttemptRepairActor,
        _attempt: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<OperationAttemptRepairAuthorization, PortError>> {
        Box::pin(async { Ok(OperationAttemptRepairAuthorization::Authorized) })
    }
}

#[test]
fn real_host_unknown_stop_without_exact_exit_remains_unrepairable() {
    let directory = tempfile::tempdir().unwrap();
    let harness = HostHarness::start(directory.path(), "stop_receipt_unknown", "stop-no-proof");
    let fixture =
        MutationFixture::prepare(&harness.runtime, directory.path(), GenericKind::ForceStop);
    apply_mutation(&harness.runtime, &fixture)
        .expect_err("injected unknown receipt while still running");
    let observer = HostedOperationAttemptEffectObserver::new(
        harness.runtime.clone(),
        HostedSessionCommandExecutor::new(harness.runtime.clone(), Arc::new(UnusedContentStore)),
    );
    assert!(block_on(observer.observe_repair(&fixture.attempt)).is_err());
    assert_eq!(
        retry_observe(&harness.runtime, &fixture.attempt).certainty,
        EffectCertainty::Unknown
    );
    cleanup_fixture(&harness.runtime, &fixture, None);
}

#[test]
fn real_host_unknown_stop_rejects_another_incarnation_exit() {
    let directory = tempfile::tempdir().unwrap();
    let harness = HostHarness::start(directory.path(), "stop_receipt_unknown", "stop-wrong-proof");
    let mut fixture = naturally_exited_fixture(&harness, directory.path(), GenericKind::ForceStop);
    fixture.process.as_mut().unwrap().id =
        ProcessInstanceId::new("another-process-incarnation").unwrap();
    apply_mutation(&harness.runtime, &fixture)
        .expect_err("retain unknown receipt for wrong process");
    let observer = HostedOperationAttemptEffectObserver::new(
        harness.runtime.clone(),
        HostedSessionCommandExecutor::new(harness.runtime.clone(), Arc::new(UnusedContentStore)),
    );
    assert!(block_on(observer.observe_repair(&fixture.attempt)).is_err());
    assert_eq!(
        retry_observe(&harness.runtime, &fixture.attempt).certainty,
        EffectCertainty::Unknown
    );
}

#[test]
fn real_host_stop_receipt_replay_preserves_live_replacement() {
    let directory = tempfile::tempdir().unwrap();
    let harness = HostHarness::start(directory.path(), "none", "stop-replacement");
    let fixture = naturally_exited_fixture(&harness, directory.path(), GenericKind::ForceStop);
    apply_mutation(&harness.runtime, &fixture).expect("stop original incarnation");
    let session = fixture.session.as_ref().unwrap();
    let replacement = block_on(harness.runtime.spawn(
        session,
        &prepared_launch(&harness.runtime, &launch("printf replacement; sleep 300")),
    ))
    .unwrap();
    apply_mutation(&harness.runtime, &fixture).expect("replay original incarnation stop");
    let identity =
        undercurrent_domain::ProcessIdentity::new(replacement.id.clone(), replacement.started_at);
    let state = block_on(harness.runtime.observe(session, &identity))
        .expect("replacement output remains observable");
    assert!(matches!(
        state,
        undercurrent_domain::ProcessTruth::Running { .. }
    ));
    block_on(harness.runtime.force_stop(session, &replacement.id))
        .expect("clean up only replacement");
}

#[test]
fn admitted_process_mutations_recover_real_host_response_loss_and_ack_after_commit() {
    for kind in GenericKind::ADMITTED {
        let directory = tempfile::tempdir().expect("temporary host directory");
        let harness = HostHarness::start(directory.path(), "response_loss", kind.name());
        let fixture = MutationFixture::prepare(&harness.runtime, directory.path(), kind);
        let (runtime, repository) = durable_runtime(&harness, directory.path());

        let first = apply_admitted(&runtime, &fixture)
            .expect_err("first durable response is deliberately lost");
        assert_eq!(first.effect_certainty(), Some(EffectCertainty::Unknown));
        let scan = OperationAttemptScan::without_exclusions(8).expect("attempt scan");
        let pending = block_on(OperationAttemptStore::pending_reconciliation(
            repository.as_ref(),
            &scan,
        ))
        .expect("read pending attempts");
        assert_eq!(pending.len(), 1, "missing Unknown settlement for {kind:?}");

        let observed = retry_known_observe(&harness.runtime, &fixture.attempt);
        let result_digest = observed
            .result_digest
            .expect("known applied receipt has a result digest");
        let replayed = retry_admitted(&runtime, &fixture);
        assert!(
            block_on(OperationAttemptStore::pending_reconciliation(
                repository.as_ref(),
                &scan,
            ))
            .expect("read reconciled attempts")
            .is_empty(),
            "replayed {kind:?} attempt remained unresolved",
        );
        assert_stored_known_applied(repository.as_ref(), &fixture, result_digest);
        assert_eq!(
            retry_observe(&harness.runtime, &fixture.attempt).certainty,
            EffectCertainty::Unknown,
            "replayed {kind:?} receipt survived its post-commit ACK",
        );
        cleanup_fixture(&harness.runtime, &fixture, replayed.as_ref());
    }
}

#[test]
fn secret_spawn_response_loss_replays_receipt_without_persisting_material() {
    const SECRET_CANARY: &str = "hosted-response-loss-secret-3fb2";
    let directory = tempfile::tempdir().expect("temporary host directory");
    let resolutions = Arc::new(AtomicUsize::new(0));
    let resolver = Arc::new(CanarySecretResolver {
        resolutions: Arc::clone(&resolutions),
        bytes: SECRET_CANARY.as_bytes().to_vec(),
    });
    let harness = HostHarness::start_with_secret_resolver(
        directory.path(),
        "response_loss",
        "secret-spawn",
        resolver,
    );
    let mut fixture =
        MutationFixture::prepare(&harness.runtime, directory.path(), GenericKind::Spawn);
    let secret_report = directory.path().join("authorized-child-secret.txt");
    let script = format!(
        "printf '%s' \"$UNDERCURRENT_TEST_CREDENTIAL\" > {}; printf '%s %s\\n' \"$$\" \"$(awk '{{print $22}}' /proc/$$/stat)\" > {}; sync; sleep 300",
        shell_quote(&secret_report),
        shell_quote(&fixture.process_report),
    );
    fixture.launch = prepared_launch(
        &harness.runtime,
        &launch_with_secret(&script, "passport-handle-response-loss"),
    );
    let (runtime, repository) = durable_runtime(&harness, directory.path());

    let first = apply_admitted(&runtime, &fixture)
        .expect_err("first durable secret-spawn response is deliberately lost");
    assert_eq!(first.effect_certainty(), Some(EffectCertainty::Unknown));
    assert_eq!(resolutions.load(Ordering::Acquire), 1);
    assert!(!format!("{first:?}").contains(SECRET_CANARY));
    wait_until(PROCESS_CONVERGENCE_TIMEOUT, || {
        fs::read(&secret_report).is_ok_and(|bytes| bytes == SECRET_CANARY.as_bytes())
    });
    let original_identity = wait_for_identity(&fixture.process_report);
    assert_eq!(
        retry_known_observe(&harness.runtime, &fixture.attempt).certainty,
        EffectCertainty::KnownApplied
    );
    let replayed = retry_admitted(&runtime, &fixture).expect("spawn receipt replays");
    assert_eq!(
        resolutions.load(Ordering::Acquire),
        1,
        "receipt replay must not reacquire a one-use credential handle",
    );
    assert_eq!(
        wait_for_identity(&fixture.process_report),
        original_identity
    );

    assert_tree_excludes_bytes(
        &ProcessHostBootstrap::attempt_receipt_journal_path(&harness.socket),
        SECRET_CANARY.as_bytes(),
    );
    for suffix in ["", "-wal", "-shm"] {
        let path = directory.path().join(format!("attempts.sqlite3{suffix}"));
        if path.is_file() {
            assert_file_excludes_bytes(&path, SECRET_CANARY.as_bytes());
        }
    }
    drop(repository);
    let _ = block_on(harness.runtime.force_stop(
        fixture.session.as_ref().expect("spawn session"),
        &replayed.id,
    ));
}

#[test]
fn real_host_crash_before_and_after_effect_keeps_every_generic_kind_unknown() {
    for kind in GenericKind::ALL {
        for mode in ["crash_before_effect", "crash_after_effect"] {
            let directory = tempfile::tempdir().expect("temporary host directory");
            let harness = HostHarness::start(directory.path(), mode, kind.name());
            let fixture = MutationFixture::prepare(&harness.runtime, directory.path(), kind);
            let identities = fixture.process_identities();

            let error = apply_mutation(&harness.runtime, &fixture)
                .expect_err("host crash must lose the response");
            assert_eq!(error.effect_certainty(), Some(EffectCertainty::Unknown));
            harness.wait_for_restart();
            for identity in identities {
                assert_identity_gone(identity);
            }
            if let Some(identity) = read_identity(&fixture.process_report) {
                assert_identity_gone(identity);
            }

            let observed = retry_observe(&harness.runtime, &fixture.attempt);
            assert_eq!(observed.certainty, EffectCertainty::Unknown);
            let commands = HostedSessionCommandExecutor::new(
                harness.runtime.clone(),
                Arc::new(UnusedContentStore),
            );
            let observer =
                HostedOperationAttemptEffectObserver::new(harness.runtime.clone(), commands);
            assert!(block_on(observer.observe_repair(&fixture.attempt)).is_err());
        }
    }
}

#[test]
fn real_host_response_loss_replays_every_generic_kind_and_ack_is_post_commit_idempotent() {
    for kind in GenericKind::ALL {
        let directory = tempfile::tempdir().expect("temporary host directory");
        let harness = HostHarness::start(directory.path(), "response_loss", kind.name());
        let fixture = MutationFixture::prepare(&harness.runtime, directory.path(), kind);

        let first = apply_mutation(&harness.runtime, &fixture)
            .expect_err("first durable response is deliberately lost");
        assert_eq!(first.effect_certainty(), Some(EffectCertainty::Unknown));
        assert_eq!(
            read_identity(&harness.host_report),
            Some(harness.initial_host),
            "response suppression unexpectedly restarted the {kind:?} host",
        );
        let observed = retry_known_observe(&harness.runtime, &fixture.attempt);
        assert_eq!(
            observed.certainty,
            EffectCertainty::KnownApplied,
            "missing applied receipt for {kind:?}",
        );

        let replayed = retry_apply(&harness.runtime, &fixture);
        cleanup_fixture(&harness.runtime, &fixture, replayed.as_ref());

        let commands = HostedSessionCommandExecutor::new(
            harness.runtime.clone(),
            Arc::new(UnusedContentStore),
        );
        let observer = HostedOperationAttemptEffectObserver::new(harness.runtime.clone(), commands);
        let repair = block_on(observer.observe_repair(&fixture.attempt))
            .expect("known receipt produces typed repair evidence");
        assert!(
            repair
                .evidence_reference()
                .as_str()
                .starts_with("undercurrent-receipt:v0.1.0-beta:")
        );
        block_on(observer.acknowledge_observation(&fixture.attempt))
            .expect("idempotent ACK retry recovers response loss");
        if kind == GenericKind::Shutdown {
            assert_identity_gone(harness.initial_host);
            assert_host_artifacts_absent(directory.path());
            assert_eq!(
                read_identity(&harness.host_report),
                Some(harness.initial_host),
                "shutdown ACK response loss launched a replacement host",
            );
            block_on(ProcessHostShutdown::acknowledge_shutdown(
                &harness.runtime,
                &fixture.attempt,
            ))
            .expect("shutdown ACK retry is idempotent after exact host retirement");
        } else {
            assert_eq!(
                retry_observe(&harness.runtime, &fixture.attempt).certainty,
                EffectCertainty::Unknown
            );
        }
    }
}

#[test]
fn shutdown_cancellation_keeps_replacement_host_and_processes_alive() {
    let directory = tempfile::tempdir().expect("temporary host directory");
    let harness = HostHarness::start(directory.path(), "none", "cancel-shutdown");
    let fixture =
        MutationFixture::prepare(&harness.runtime, directory.path(), GenericKind::Shutdown);
    apply_mutation(&harness.runtime, &fixture).expect("prepare uncommitted shutdown");
    assert!(
        block_on(
            harness
                .runtime
                .cancel_uncommitted_shutdown(&fixture.attempt)
        )
        .expect("cancel shutdown")
    );
    let rejected = apply_mutation(&harness.runtime, &fixture)
        .expect_err("cancelled preparation cannot replay");
    assert_eq!(
        rejected.effect_certainty(),
        Some(EffectCertainty::KnownAbsent)
    );

    // Lose the cancellation response and the original host before SQLite
    // settlement. The replacement must retain the negative receipt.
    kill_exact(harness.initial_host, Signal::SIGKILL);
    harness.wait_for_restart();
    assert_eq!(
        retry_observe(&harness.runtime, &fixture.attempt).certainty,
        EffectCertainty::KnownAbsent
    );
    assert!(
        block_on(
            harness
                .runtime
                .cancel_uncommitted_shutdown(&fixture.attempt)
        )
        .expect("retry cancellation after restart")
    );

    let session = SessionId::new("replacement-survivor").unwrap();
    let report = directory.path().join("survivor.pid");
    let spec = prepared_launch(
        &harness.runtime,
        &launch(&format!(
            "printf '%s %s\\n' \"$$\" \"$(awk '{{print $22}}' /proc/$$/stat)\" > {}; sleep 300",
            shell_quote(&report),
        )),
    );
    let process = block_on(harness.runtime.spawn(&session, &spec)).expect("spawn on replacement");
    let survivor = wait_for_identity(&report);
    let replacement = read_identity(&harness.host_report).expect("replacement identity");
    let mut committed = fixture.attempt.clone();
    committed
        .settle(
            committed.fencing_token(),
            EffectCertainty::KnownAbsent,
            None,
            TimestampMillis::new(20),
        )
        .unwrap();
    for _ in 0..2 {
        block_on(harness.runtime.acknowledge_process_attempt(&committed))
            .expect("release cancellation without retiring host");
    }
    assert!(process_identity_matches(replacement));
    assert!(process_identity_matches(survivor));

    // Explicit repair uses its pre-commit Unknown snapshot for ACK. It must
    // follow the cancellation path too, even when a prepared receipt exists.
    let mut manual = OperationAttempt::accepted(
        OperationAttemptId::new("manual-shutdown-repair").unwrap(),
        None,
        ProcessMutationKind::Shutdown,
        FencingToken::new(2).unwrap(),
        PayloadDigest::of_bytes(b"manual shutdown"),
        TimestampMillis::new(30),
    );
    block_on(
        harness
            .runtime
            .shutdown_attempted(&ProcessMutationContext::from_attempt(&manual)),
    )
    .expect("prepare manual repair shutdown");
    manual
        .settle(
            manual.fencing_token(),
            EffectCertainty::Unknown,
            None,
            TimestampMillis::new(40),
        )
        .unwrap();
    let observer = HostedOperationAttemptEffectObserver::new(
        harness.runtime.clone(),
        HostedSessionCommandExecutor::new(harness.runtime.clone(), Arc::new(UnusedContentStore)),
    );
    let observation =
        block_on(observer.observe_repair(&manual)).expect("repair observes durable cancellation");
    assert_eq!(observation.certainty(), EffectCertainty::KnownAbsent);
    let mut repaired = manual.clone();
    repaired
        .reconcile(EffectCertainty::KnownAbsent, None, TimestampMillis::new(50))
        .unwrap();
    block_on(observer.acknowledge_repair(&manual, observation.evidence_reference()))
        .expect("post-commit repair ACK");
    block_on(observer.acknowledge_repair(&repaired, observation.evidence_reference()))
        .expect("repair ACK retry");
    assert!(process_identity_matches(replacement));
    assert!(process_identity_matches(survivor));
    block_on(harness.runtime.force_stop(&session, &process.id)).expect("clean up survivor");
}

#[test]
fn shutdown_receipt_waits_for_post_settlement_ack_then_retires_exact_host_tree() {
    let directory = tempfile::tempdir().expect("temporary host directory");
    let harness = HostHarness::start(directory.path(), "none", "shutdown-order");
    let parent_report = directory.path().join("shutdown-parent.pid");
    let child_report = directory.path().join("shutdown-child.pid");
    let session = SessionId::new("shutdown-tree").expect("session ID");
    let script = format!(
        "sleep 300 & child=$!; printf '%s %s\\n' \"$$\" \"$(awk '{{print $22}}' /proc/$$/stat)\" > {}; printf '%s %s\\n' \"$child\" \"$(awk '{{print $22}}' /proc/$child/stat)\" > {}; sync; wait \"$child\"",
        shell_quote(&parent_report),
        shell_quote(&child_report),
    );
    let launch = prepared_launch(&harness.runtime, &launch(&script));
    let process =
        block_on(harness.runtime.spawn(&session, &launch)).expect("spawn hosted process tree");
    let parent = wait_for_identity(&parent_report);
    let child = wait_for_identity(&child_report);
    let fixture =
        MutationFixture::prepare(&harness.runtime, directory.path(), GenericKind::Shutdown);

    apply_mutation(&harness.runtime, &fixture).expect("persist shutdown receipt");
    let observed = retry_known_observe(&harness.runtime, &fixture.attempt);
    assert_eq!(observed.certainty, EffectCertainty::KnownApplied);
    assert!(process_identity_matches(harness.initial_host));
    assert!(process_identity_matches(parent));
    assert!(process_identity_matches(child));
    assert_eq!(
        retry_apply(&harness.runtime, &fixture),
        None,
        "shutdown receipt replay changed the effect result",
    );
    assert_eq!(
        read_identity(&harness.host_report),
        Some(harness.initial_host),
        "shutdown replay launched a duplicate host",
    );

    block_on(ProcessHostShutdown::acknowledge_shutdown(
        &harness.runtime,
        &fixture.attempt,
    ))
    .expect("post-settlement shutdown ACK");
    assert_identity_gone(harness.initial_host);
    assert_identity_gone(parent);
    assert_identity_gone(child);
    assert_host_artifacts_absent(directory.path());
    assert_eq!(
        read_identity(&harness.host_report),
        Some(harness.initial_host),
        "terminal shutdown ACK launched a replacement host",
    );
    block_on(ProcessHostShutdown::acknowledge_shutdown(
        &harness.runtime,
        &fixture.attempt,
    ))
    .expect("post-retirement shutdown ACK retry");
    let _ = process;
}

#[test]
fn failed_native_wait_prevents_retirement_even_after_the_process_entry_is_gone() {
    let directory = tempfile::tempdir().unwrap();
    let config = hosted_config(directory.path(), directory.path().join("unused-wrapper"));
    let runtime = super::NativeProcessRuntime::start(
        config.native().clone(),
        Arc::new(SystemClock),
        Arc::new(BoundedTelemetryRecorder::new(128, 128).unwrap()),
    )
    .unwrap();
    let core = &runtime.owner.core;
    core.complete_exit(
        &SessionId::new("retired-session").unwrap(),
        &ProcessInstanceId::new("retired-process").unwrap(),
        undercurrent_domain::ProcessOutcome::SupervisionFailed,
        false,
    );
    assert!(
        core.force_all_and_confirm(Duration::from_millis(10))
            .is_err()
    );
}

#[cfg(target_os = "linux")]
#[test]
fn uref_032_real_pid_start_identity_guards_hosted_process_boundary() {
    let directory = tempfile::tempdir().expect("temporary host directory");
    let harness = HostHarness::start(directory.path(), "none", "uref-032-process-identity");
    let process_report = directory.path().join("uref-032-process.pid");
    let session = SessionId::new("uref-032-process-boundary").expect("session ID");
    let script = format!(
        "printf '%s %s\\n' \"$$\" \"$(awk '{{print $22}}' /proc/$$/stat)\" > {}; sync; sleep 300",
        shell_quote(&process_report),
    );

    let launch = prepared_launch(&harness.runtime, &launch(&script));
    let process = block_on(harness.runtime.spawn(&session, &launch))
        .expect("spawn through the private hosted-process boundary");
    let live = wait_for_identity(&process_report);
    assert_eq!(linux_process_start_ticks(live.pid), Some(live.start_ticks));
    assert!(process_identity_matches(live));

    let stale = ProcessIdentity {
        pid: live.pid,
        start_ticks: live.start_ticks.saturating_add(1),
    };
    assert_ne!(stale.start_ticks, live.start_ticks);
    assert!(
        !process_identity_matches(stale),
        "the same PID with a stale start identity must not match the live process"
    );

    block_on(harness.runtime.force_stop(&session, &process.id))
        .expect("stop the exact hosted process incarnation");
    assert_identity_gone(live);
    assert!(
        process_identity_matches(harness.initial_host),
        "stopping the workload must not cross the private host-process boundary"
    );
}

#[test]
fn spawn_receipt_downgrades_after_real_host_sigkill_and_exact_process_tree_cleanup() {
    let directory = tempfile::tempdir().expect("temporary host directory");
    let harness = HostHarness::start(directory.path(), "none", "pid-reuse");
    let fixture = MutationFixture::prepare(&harness.runtime, directory.path(), GenericKind::Spawn);
    let spawned = apply_mutation(&harness.runtime, &fixture)
        .expect("spawn applies")
        .expect("spawn result");
    let workload = wait_for_identity(&fixture.process_report);
    let host = wait_for_identity(&harness.host_report);
    kill_exact(host, Signal::SIGKILL);
    harness.wait_for_restart();
    assert_identity_gone(workload);

    let observed = retry_observe(&harness.runtime, &fixture.attempt);
    assert_eq!(observed.certainty, EffectCertainty::Unknown);
    let retry = apply_mutation(&harness.runtime, &fixture)
        .expect_err("a restarted host must not replay a stale spawn incarnation");
    assert_eq!(retry.effect_certainty(), Some(EffectCertainty::Unknown));
    let _ = spawned;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GenericKind {
    Spawn,
    ArmExit,
    Stop,
    ForceStop,
    WriteInput,
    Resize,
    Shutdown,
}

impl GenericKind {
    const ALL: [Self; 7] = [
        Self::Spawn,
        Self::ArmExit,
        Self::Stop,
        Self::ForceStop,
        Self::WriteInput,
        Self::Resize,
        Self::Shutdown,
    ];

    const ADMITTED: [Self; 6] = [
        Self::Spawn,
        Self::ArmExit,
        Self::Stop,
        Self::ForceStop,
        Self::WriteInput,
        Self::Resize,
    ];

    const fn mutation_kind(self) -> ProcessMutationKind {
        match self {
            Self::Spawn => ProcessMutationKind::Spawn,
            Self::ArmExit => ProcessMutationKind::ArmExit,
            Self::Stop => ProcessMutationKind::Stop,
            Self::ForceStop => ProcessMutationKind::ForceStop,
            Self::WriteInput => ProcessMutationKind::WriteInput,
            Self::Resize => ProcessMutationKind::Resize,
            Self::Shutdown => ProcessMutationKind::Shutdown,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Spawn => "spawn",
            Self::ArmExit => "arm",
            Self::Stop => "stop",
            Self::ForceStop => "force",
            Self::WriteInput => "input",
            Self::Resize => "resize",
            Self::Shutdown => "shutdown",
        }
    }
}

struct HostHarness {
    runtime: HostedProcessRuntime,
    socket: PathBuf,
    host_report: PathBuf,
    initial_host: ProcessIdentity,
}

impl HostHarness {
    fn start(directory: &Path, mode: &str, label: &str) -> Self {
        Self::start_with_secret_resolver(
            directory,
            mode,
            label,
            Arc::new(undercurrent_application::UnavailableSecretResolver),
        )
    }

    fn start_with_secret_resolver(
        directory: &Path,
        mode: &str,
        label: &str,
        secret_resolver: Arc<dyn SecretResolver>,
    ) -> Self {
        let host_report = directory.join(format!("host-{label}.pid"));
        let wrapper = write_host_wrapper(directory, mode, &host_report);
        let config = hosted_config(directory, wrapper);
        let socket = config.socket().to_path_buf();
        let telemetry: Arc<dyn Telemetry> =
            Arc::new(BoundedTelemetryRecorder::new(128, 128).expect("telemetry"));
        let runtime = HostedProcessRuntime::start_with_secret_resolver(
            config,
            Arc::new(SystemClock),
            telemetry,
            secret_resolver,
        )
        .expect("hosted runtime");
        let initial_host = wait_for_identity(&host_report);
        Self {
            runtime,
            socket,
            host_report,
            initial_host,
        }
    }

    fn wait_for_restart(&self) {
        wait_until(PROCESS_CONVERGENCE_TIMEOUT, || {
            read_identity(&self.host_report).is_some_and(|current| current != self.initial_host)
        });
    }
}

struct MutationFixture {
    kind: GenericKind,
    session: Option<SessionId>,
    process: Option<ProcessInstance>,
    launch: LaunchSpec,
    input: InputSequence,
    size: TerminalSize,
    attempt: OperationAttempt,
    admission: OperationAttemptAdmission,
    context: ProcessMutationContext,
    process_report: PathBuf,
}

impl MutationFixture {
    fn prepare(runtime: &HostedProcessRuntime, directory: &Path, kind: GenericKind) -> Self {
        let name = kind.name();
        let session = (kind != GenericKind::Shutdown)
            .then(|| SessionId::new(format!("receipt-{name}")).expect("session ID"));
        let process_report = directory.join(format!("process-{name}.pid"));
        let marker = directory.join(format!("effect-{name}.txt"));
        let script = match kind {
            GenericKind::WriteInput => format!(
                "printf '%s %s\\n' \"$$\" \"$(awk '{{print $22}}' /proc/$$/stat)\" > {}; sync; stty -echo; IFS= read -r line; printf '%s\\n' \"$line\" > {}; sync; sleep 300",
                shell_quote(&process_report),
                shell_quote(&marker),
            ),
            GenericKind::Stop | GenericKind::ForceStop => format!(
                "trap 'exit 0' TERM; printf '%s %s\\n' \"$$\" \"$(awk '{{print $22}}' /proc/$$/stat)\" > {}; sync; while :; do sleep 1; done",
                shell_quote(&process_report),
            ),
            _ => format!(
                "printf '%s %s\\n' \"$$\" \"$(awk '{{print $22}}' /proc/$$/stat)\" > {}; sync; sleep 300",
                shell_quote(&process_report),
            ),
        };
        let launch = prepared_launch(runtime, &launch(&script));
        let process = match kind {
            GenericKind::Spawn | GenericKind::Shutdown => None,
            _ => {
                let session = session.as_ref().expect("process session");
                let process = block_on(runtime.spawn(session, &launch)).expect("setup process");
                let _ = wait_for_identity(&process_report);
                Some(process)
            }
        };
        let attempt = OperationAttempt::accepted(
            OperationAttemptId::new(format!("receipt-attempt-{name}")).expect("attempt ID"),
            session.clone(),
            kind.mutation_kind(),
            FencingToken::new(1).expect("fence"),
            PayloadDigest::of_bytes(name.as_bytes()),
            TimestampMillis::new(10),
        );
        let admission = OperationAttemptAdmission::new(
            OperationAttemptId::new(format!("receipt-attempt-{name}")).expect("attempt ID"),
            session.clone(),
            kind.mutation_kind(),
            PayloadDigest::of_bytes(name.as_bytes()),
            TimestampMillis::new(10),
        );
        let context = ProcessMutationContext::from_attempt(&attempt);
        Self {
            kind,
            session,
            process,
            launch,
            input: InputSequence::new(
                vec![
                    InputItem::Text("binary-safe-input".to_owned()),
                    InputItem::Key(Key::Enter),
                ],
                4,
                128,
            )
            .expect("input"),
            size: TerminalSize::new(101, 37, 200, 100).expect("terminal size"),
            attempt,
            admission,
            context,
            process_report,
        }
    }

    fn process_identities(&self) -> Vec<ProcessIdentity> {
        read_identity(&self.process_report).into_iter().collect()
    }
}

fn apply_mutation(
    runtime: &HostedProcessRuntime,
    fixture: &MutationFixture,
) -> Result<Option<ProcessInstance>, PortError> {
    let session = fixture.session.as_ref();
    let process = fixture.process.as_ref();
    match fixture.kind {
        GenericKind::Spawn => block_on(runtime.spawn_attempted(
            &fixture.context,
            session.expect("spawn session"),
            &fixture.launch,
        ))
        .map(Some),
        GenericKind::ArmExit => block_on(runtime.arm_exit_attempted(
            &fixture.context,
            session.expect("arm session"),
            &process.expect("arm process").id,
        ))
        .map(|()| None),
        GenericKind::Stop => block_on(runtime.stop_attempted(
            &fixture.context,
            session.expect("stop session"),
            &process.expect("stop process").id,
            StopPolicy::new(StopSignal::Terminate, Duration::from_secs(2)).expect("stop policy"),
        ))
        .map(|_| None),
        GenericKind::ForceStop => block_on(runtime.force_stop_attempted(
            &fixture.context,
            session.expect("force session"),
            &process.expect("force process").id,
        ))
        .map(|_| None),
        GenericKind::WriteInput => block_on(runtime.write_input_attempted(
            &fixture.context,
            session.expect("input session"),
            &process.expect("input process").id,
            &fixture.input,
            undercurrent_domain::InputDeliveryPolicy::immediate(),
        ))
        .map(|()| None),
        GenericKind::Resize => block_on(runtime.resize_attempted(
            &fixture.context,
            session.expect("resize session"),
            &process.expect("resize process").id,
            fixture.size,
        ))
        .map(|()| None),
        GenericKind::Shutdown => {
            block_on(runtime.shutdown_attempted(&fixture.context)).map(|()| None)
        }
    }
}

fn retry_apply(
    runtime: &HostedProcessRuntime,
    fixture: &MutationFixture,
) -> Option<ProcessInstance> {
    let deadline = Instant::now() + PROCESS_CONVERGENCE_TIMEOUT;
    loop {
        match apply_mutation(runtime, fixture) {
            Ok(result) => return result,
            Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(25)),
            Err(error) => panic!("receipt replay did not recover: {error}"),
        }
    }
}

fn durable_runtime(
    harness: &HostHarness,
    directory: &Path,
) -> (DurableProcessRuntime, Arc<SqliteSessionRepository>) {
    let repository = Arc::new(
        SqliteSessionRepository::open(
            SqliteRepositoryConfig::new(
                directory.join("attempts.sqlite3"),
                SqliteRepositoryLimits::new(32, 32, 1_048_576, 32, 16).expect("SQLite limits"),
                Duration::from_secs(1),
            )
            .expect("SQLite configuration"),
        )
        .expect("SQLite repository"),
    );
    let inner: Arc<dyn ProcessRuntime> = Arc::new(harness.runtime.clone());
    let attempts: Arc<dyn OperationAttemptStore> = repository.clone();
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    (
        DurableProcessRuntime::new(inner, attempts, clock),
        repository,
    )
}

fn apply_admitted(
    runtime: &DurableProcessRuntime,
    fixture: &MutationFixture,
) -> Result<Option<ProcessInstance>, PortError> {
    let session = fixture.session.as_ref();
    let process = fixture.process.as_ref();
    match fixture.kind {
        GenericKind::Spawn => block_on(runtime.spawn_admitted(
            &fixture.admission,
            session.expect("spawn session"),
            &fixture.launch,
        ))
        .map(Some),
        GenericKind::ArmExit => block_on(runtime.arm_exit_admitted(
            &fixture.admission,
            session.expect("arm session"),
            &process.expect("arm process").id,
        ))
        .map(|()| None),
        GenericKind::Stop => block_on(runtime.stop_admitted(
            &fixture.admission,
            session.expect("stop session"),
            &process.expect("stop process").id,
            StopPolicy::new(StopSignal::Terminate, Duration::from_secs(2)).expect("stop policy"),
        ))
        .map(|_| None),
        GenericKind::ForceStop => block_on(runtime.force_stop_admitted(
            &fixture.admission,
            session.expect("force session"),
            &process.expect("force process").id,
        ))
        .map(|_| None),
        GenericKind::WriteInput => block_on(runtime.write_input_admitted(
            &fixture.admission,
            session.expect("input session"),
            &process.expect("input process").id,
            &fixture.input,
            undercurrent_domain::InputDeliveryPolicy::immediate(),
        ))
        .map(|()| None),
        GenericKind::Resize => block_on(runtime.resize_admitted(
            &fixture.admission,
            session.expect("resize session"),
            &process.expect("resize process").id,
            fixture.size,
        ))
        .map(|()| None),
        GenericKind::Shutdown => panic!("shutdown is not a ProcessRuntime admitted operation"),
    }
}

fn retry_admitted(
    runtime: &DurableProcessRuntime,
    fixture: &MutationFixture,
) -> Option<ProcessInstance> {
    let deadline = Instant::now() + PROCESS_CONVERGENCE_TIMEOUT;
    loop {
        match apply_admitted(runtime, fixture) {
            Ok(result) => return result,
            Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(25)),
            Err(error) => panic!("durable receipt replay did not recover: {error}"),
        }
    }
}

fn assert_stored_known_applied(
    repository: &SqliteSessionRepository,
    fixture: &MutationFixture,
    expected_digest: PayloadDigest,
) {
    let stored = block_on(OperationAttemptStore::load(
        repository,
        fixture.attempt.id(),
    ))
    .expect("load durable attempt")
    .expect("durable attempt exists");
    assert_eq!(stored.fencing_token(), fixture.attempt.fencing_token());
    match stored.state() {
        OperationAttemptState::Settled {
            certainty,
            result_digest,
            ..
        } => {
            assert_eq!(*certainty, EffectCertainty::KnownApplied);
            assert_eq!(*result_digest, Some(expected_digest));
        }
        OperationAttemptState::Accepted => panic!("attempt was not durably settled"),
        OperationAttemptState::Compensated { .. } => {
            panic!("known-applied runtime attempt was durably compensated")
        }
    }
}

fn retry_observe(
    runtime: &HostedProcessRuntime,
    attempt: &OperationAttempt,
) -> ObservedOperationAttemptEffect {
    let deadline = Instant::now() + PROCESS_CONVERGENCE_TIMEOUT;
    loop {
        match block_on(runtime.observe_process_attempt(attempt)) {
            Ok(observation) => return observation,
            Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(25)),
            Err(error) => panic!("receipt observation did not recover: {error}"),
        }
    }
}

fn retry_known_observe(
    runtime: &HostedProcessRuntime,
    attempt: &OperationAttempt,
) -> ObservedOperationAttemptEffect {
    let deadline = Instant::now() + PROCESS_CONVERGENCE_TIMEOUT;
    loop {
        let observation = retry_observe(runtime, attempt);
        if observation.certainty != EffectCertainty::Unknown {
            return observation;
        }
        assert!(
            Instant::now() < deadline,
            "terminal receipt did not become observable"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn cleanup_fixture(
    runtime: &HostedProcessRuntime,
    fixture: &MutationFixture,
    replayed: Option<&ProcessInstance>,
) {
    if matches!(
        fixture.kind,
        GenericKind::ArmExit | GenericKind::WriteInput | GenericKind::Resize
    ) && let (Some(session), Some(process)) = (&fixture.session, &fixture.process)
    {
        let _ = block_on(runtime.force_stop(session, &process.id));
    }
    if fixture.kind == GenericKind::Spawn
        && let (Some(session), Some(process)) = (&fixture.session, replayed)
    {
        let _ = block_on(runtime.force_stop(session, &process.id));
    }
}

fn hosted_config(directory: &Path, wrapper: PathBuf) -> HostedProcessRuntimeConfig {
    let native = NativeProcessRuntimeConfig::new(
        "receipt-test-epoch",
        NativeProcessRuntimeLimits {
            max_processes: 4,
            spawn_workers: 4,
            spawn_queue_capacity: 8,
            input_queue_sequences: 8,
            input_queue_bytes_per_process: 4_096,
            output_read_bytes: 1_024,
            screen_snapshot_bytes: 128 * 1_024,
            environment_file_bytes: 4_096,
            environment_entries: 16,
            environment_bytes: 8_192,
            worker_thread_stack_bytes: 128 * 1_024,
        },
        undercurrent_application::TerminalOutputLimits {
            per_session_bytes: 2 * 1_024 * 1_024,
            global_bytes: 16 * 1_024 * 1_024,
            chunk_bytes: 1_024,
            append_bytes: 1_024,
            replay_bytes: 1_024 * 1_024,
            sessions: 8,
            subscriptions: 16,
        },
        PROCESS_CONVERGENCE_TIMEOUT,
    )
    .expect("native config");
    HostedProcessRuntimeConfig::new(
        native,
        directory.join("session-host.sock"),
        wrapper,
        Duration::from_secs(5),
        Duration::from_millis(10),
        Duration::from_millis(100),
        Duration::from_secs(2),
    )
    .expect("hosted config")
}

fn write_host_wrapper(directory: &Path, mode: &str, report: &Path) -> PathBuf {
    let wrapper = directory.join("session-host-test.sh");
    let executable = std::env::current_exe().expect("current test executable");
    let script = format!(
        "#!/bin/sh\nexport {HOST_MARKER_ENV}=1\nexport {MODE_ENV}={}\nprintf '%s %s\\n' \"$$\" \"$(awk '{{print $22}}' /proc/$$/stat)\" > {}\nexec {} --exact process::host_receipt_tests::receipt_host_subprocess --nocapture\n",
        shell_quote_value(mode),
        shell_quote(report),
        shell_quote(&executable),
    );
    fs::write(&wrapper, script).expect("write host wrapper");
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o700)).expect("wrapper mode");
    wrapper
}

fn launch(script: &str) -> LaunchSpec {
    LaunchSpec::new(
        CommandSpec::new(
            "/bin/sh",
            vec!["-c".to_owned(), script.to_owned()],
            execution_limits(),
        )
        .expect("command"),
        EnvironmentVariables::new(Vec::new(), execution_limits()).expect("environment"),
        None,
        None,
        TerminalSize::new(80, 24, 200, 100).expect("terminal size"),
    )
}

fn launch_with_secret(script: &str, reference: &str) -> LaunchSpec {
    let limits = execution_limits();
    let ordinary = EnvironmentVariables::new(Vec::new(), limits).expect("environment");
    let secrets = SecretEnvironmentVariables::new(
        [(
            "UNDERCURRENT_TEST_CREDENTIAL".to_owned(),
            SecretReference::new(reference).expect("opaque secret reference"),
        )],
        &ordinary,
        limits,
    )
    .expect("secret environment");
    LaunchSpec::new(
        CommandSpec::new("/bin/sh", vec!["-c".to_owned(), script.to_owned()], limits)
            .expect("command"),
        ordinary,
        None,
        None,
        TerminalSize::new(80, 24, 200, 100).expect("terminal size"),
    )
    .with_secret_environment(secrets, limits)
    .expect("secret launch")
}

struct CanarySecretResolver {
    resolutions: Arc<AtomicUsize>,
    bytes: Vec<u8>,
}

impl SecretResolver for CanarySecretResolver {
    fn resolve<'a>(
        &'a self,
        _reference: &'a SecretReference,
    ) -> PortFuture<'a, Result<EphemeralSecret, PortError>> {
        self.resolutions.fetch_add(1, Ordering::AcqRel);
        let bytes = self.bytes.clone();
        Box::pin(async move { Ok(EphemeralSecret::new(bytes)) })
    }
}

fn assert_tree_excludes_bytes(root: &Path, canary: &[u8]) {
    for entry in fs::read_dir(root)
        .expect("private journal directory")
        .collect::<Result<Vec<_>, _>>()
        .expect("private journal entries")
    {
        let path = entry.path();
        if path.is_dir() {
            assert_tree_excludes_bytes(&path, canary);
        } else if path.is_file() {
            assert_file_excludes_bytes(&path, canary);
        }
    }
}

fn assert_file_excludes_bytes(path: &Path, canary: &[u8]) {
    let bytes = fs::read(path).expect("private durable artifact");
    assert!(
        !bytes.windows(canary.len()).any(|window| window == canary),
        "raw secret canary entered a durable artifact",
    );
}

fn prepared_launch(runtime: &HostedProcessRuntime, launch: &LaunchSpec) -> LaunchSpec {
    block_on(runtime.prepare_launch(launch)).expect("launch preparation")
}

fn execution_limits() -> ExecutionLimits {
    ExecutionLimits {
        max_arguments: 8,
        max_command_bytes: 8_192,
        max_environment_entries: 16,
        max_environment_bytes: 8_192,
        max_path_bytes: 4_096,
        max_health_rules: 1,
        max_health_rule_bytes: 64,
    }
}

fn shell_quote(path: &Path) -> String {
    shell_quote_value(&path.to_string_lossy())
}

fn shell_quote_value(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProcessIdentity {
    pid: u32,
    start_ticks: u64,
}

fn wait_for_identity(path: &Path) -> ProcessIdentity {
    let deadline = Instant::now() + PROCESS_CONVERGENCE_TIMEOUT;
    loop {
        if let Some(identity) = read_identity(path) {
            return identity;
        }
        assert!(
            Instant::now() < deadline,
            "process identity was not reported"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn read_identity(path: &Path) -> Option<ProcessIdentity> {
    let value = fs::read_to_string(path).ok()?;
    let mut fields = value.split_whitespace();
    Some(ProcessIdentity {
        pid: fields.next()?.parse().ok()?,
        start_ticks: fields.next()?.parse().ok()?,
    })
}

fn kill_exact(identity: ProcessIdentity, signal: Signal) {
    assert!(process_identity_matches(identity));
    kill(Pid::from_raw(identity.pid as i32), signal).expect("signal exact process");
}

fn assert_identity_gone(identity: ProcessIdentity) {
    wait_until(PROCESS_CONVERGENCE_TIMEOUT, || {
        !process_identity_matches(identity)
    });
}

fn assert_host_artifacts_absent(directory: &Path) {
    let socket = directory.join("session-host.sock");
    let authentication = ProcessHostBootstrap::authentication_path(&socket);
    let execution_journal = ProcessHostBootstrap::execution_journal_path(&socket);
    let attempt_journal = ProcessHostBootstrap::attempt_receipt_journal_path(&socket);
    wait_until(PROCESS_CONVERGENCE_TIMEOUT, || {
        !socket.exists()
            && !authentication.exists()
            && !execution_journal.exists()
            && !attempt_journal.exists()
    });
}

fn assert_uchs_peer_rejected(socket: &Path, requests: Vec<HostRequest>) {
    let codec = HostCodec::new(64 * 1_024).expect("test host codec");
    let mut peer = UnixStream::connect(socket).expect("connect unauthorized peer");
    peer.set_read_timeout(Some(Duration::from_secs(2)))
        .and_then(|()| peer.set_write_timeout(Some(Duration::from_secs(2))))
        .expect("peer timeouts");
    for request in requests {
        peer.write_all(
            &codec
                .encode_request(&request)
                .expect("encode unauthorized request"),
        )
        .expect("write unauthorized request");
    }
    peer.shutdown(Shutdown::Write)
        .expect("finish unauthorized request stream");
    let mut response = Vec::new();
    match peer.read_to_end(&mut response) {
        Ok(_) => assert!(response.is_empty(), "unauthorized peer received a response"),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            ) =>
        {
            assert!(
                response.is_empty(),
                "unauthorized peer received partial data"
            );
        }
        Err(error) => panic!("unauthorized peer was not promptly rejected: {error}"),
    }
}

fn assert_path_mode(path: &Path, expected: u32) {
    let metadata = fs::symlink_metadata(path).expect("private artifact metadata");
    assert!(!metadata.file_type().is_symlink());
    assert_eq!(
        metadata.permissions().mode() & 0o777,
        expected,
        "unexpected permission bits on {}",
        path.display()
    );
}

fn assert_private_journal(directory: &Path) {
    assert_path_mode(directory, 0o700);
    let entries = fs::read_dir(directory)
        .expect("private journal directory")
        .collect::<Result<Vec<_>, _>>()
        .expect("private journal entries");
    assert!(!entries.is_empty(), "private journal was not materialized");
    for entry in entries {
        assert!(
            entry.file_type().expect("journal entry type").is_file(),
            "journal entry is not a regular file"
        );
        assert_path_mode(&entry.path(), 0o600);
    }
}

fn process_identity_matches(identity: ProcessIdentity) -> bool {
    linux_process_start_ticks(identity.pid).is_some_and(|ticks| ticks == identity.start_ticks)
}

fn linux_process_start_ticks(pid: u32) -> Option<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let end = stat.rfind(')')?;
    stat.get(end + 2..)?
        .split_whitespace()
        .nth(19)?
        .parse()
        .ok()
}

fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !condition() {
        assert!(Instant::now() < deadline, "condition did not become true");
        thread::sleep(Duration::from_millis(10));
    }
}

struct UnusedContentStore;

impl ImmutableExecutionOutputStore for UnusedContentStore {
    fn store<'a>(
        &'a self,
        _bytes: &'a [u8],
        _digest: PayloadDigest,
    ) -> PortFuture<'a, Result<ImmutableExecutionOutputRef, PortError>> {
        Box::pin(async { panic!("generic receipt tests never externalize command output") })
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park_timeout(Duration::from_millis(10)),
        }
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
