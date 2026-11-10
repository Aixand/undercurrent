// SPDX-License-Identifier: Apache-2.0

#![cfg(unix)]

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use tempfile::tempdir;
#[cfg(target_os = "linux")]
use undercurrent_application::ForegroundProcessObserver;
use undercurrent_application::{
    EphemeralSecret, MetricName, OutputGapRecovery, OutputOffset, OutputReplay, PortError,
    PortErrorKind, PortFuture, ProcessExitSource, ProcessInstance, ProcessRuntime,
    ProcessTruthSource, SecretResolver, StandardInputClosure, SubscriptionWake,
    TerminalOutputLimits, TerminalScreenReader, TerminationKind,
};
use undercurrent_domain::{
    CommandSpec, EnvironmentVariables, ExecutionEnvironmentBinding, ExecutionLimits,
    InputDeliveryPolicy, InputItem, InputSequence, Key, LaunchMode, LaunchSpec, OutputCaptureMode,
    PathReference, PayloadDigest, ProcessIdentity, ProcessInstanceId, ProcessOutcome,
    SecretEnvironmentVariables, SecretReference, SessionId, StopPolicy, StopSignal, TerminalSize,
    TimestampMillis,
};
use undercurrent_runtime::{
    BoundedTelemetryRecorder, CooperativeForegroundState, NativeProcessRuntime,
    NativeProcessRuntimeConfig, NativeProcessRuntimeConfigError, NativeProcessRuntimeLimits,
    SystemClock,
};

// macOS rejects sealed launches; retain the existing sealed coverage on Linux.
const FIXTURE_LAUNCH_MODE: LaunchMode = if cfg!(target_os = "macos") {
    LaunchMode::Native
} else {
    LaunchMode::Sealed
};

#[test]
fn runtime_configuration_rejects_invalid_or_inconsistent_hard_bounds() {
    let mut limits = native_limits(2);
    limits.spawn_workers = 0;
    assert!(matches!(
        NativeProcessRuntimeConfig::new("epoch", limits, output_limits(2), Duration::from_secs(1),),
        Err(NativeProcessRuntimeConfigError::ZeroLimit {
            field: "spawn_workers"
        })
    ));
    assert!(matches!(
        NativeProcessRuntimeConfig::new(
            "",
            native_limits(2),
            output_limits(2),
            Duration::from_secs(1),
        ),
        Err(NativeProcessRuntimeConfigError::EmptyEpoch)
    ));
    assert!(matches!(
        NativeProcessRuntimeConfig::new(
            "x".repeat(256),
            native_limits(2),
            output_limits(2),
            Duration::from_secs(1),
        ),
        Err(NativeProcessRuntimeConfigError::InvalidEpoch(_))
    ));
}

#[test]
fn pty_spawn_passes_shell_metacharacters_as_literal_argv() {
    let directory = tempdir().expect("temporary directory");
    let side_effect = directory.path().join("pty-shell-side-effect");
    let redirected = directory.path().join("pty-shell-redirection");
    let literal = format!(
        "literal;$HOME|$(touch {})&*?[x]>{}",
        side_effect.display(),
        redirected.display()
    );
    let command = CommandSpec::new(
        "/usr/bin/printf",
        vec!["%s".to_owned(), literal.clone()],
        execution_limits(),
    )
    .expect("literal argv command")
    .with_launch_mode(FIXTURE_LAUNCH_MODE);
    let launch = LaunchSpec::new(
        command,
        EnvironmentVariables::new(Vec::new(), execution_limits()).expect("empty environment"),
        None,
        None,
        terminal_size(),
    );
    let runtime = runtime(1);
    let session = session_id("literal-argv");
    let process = spawn(&runtime, &session, &launch).expect("PTY process spawns");
    block_on(runtime.arm_exit(&session, &process.id)).expect("exit arms");
    assert_eq!(next_exit(&runtime).outcome, ProcessOutcome::Exited(0));
    assert_eq!(replay_bytes(&runtime, &session), literal.as_bytes());
    assert!(!side_effect.exists());
    assert!(!redirected.exists());
}

#[test]
fn secret_reference_resolves_only_at_spawn_and_enters_child_ephemerally() {
    const SECRET_CANARY: &str = "runtime-secret-canary-a17c";
    let limits = execution_limits();
    let ordinary = EnvironmentVariables::new(Vec::new(), limits).expect("empty environment");
    let secret_environment = SecretEnvironmentVariables::new(
        [(
            "UNDERCURRENT_TEST_CREDENTIAL".to_owned(),
            SecretReference::new("credential-handle-17").expect("opaque reference"),
        )],
        &ordinary,
        limits,
    )
    .expect("secret environment");
    let launch = LaunchSpec::new(
        CommandSpec::new("/usr/bin/env", Vec::new(), limits)
            .expect("environment command")
            .with_launch_mode(FIXTURE_LAUNCH_MODE),
        ordinary,
        None,
        None,
        terminal_size(),
    )
    .with_secret_environment(secret_environment, limits)
    .expect("secret launch");
    let resolutions = Arc::new(AtomicUsize::new(0));
    let resolver = Arc::new(CanarySecretResolver {
        resolutions: Arc::clone(&resolutions),
        bytes: SECRET_CANARY.as_bytes().to_vec(),
    });
    let config = NativeProcessRuntimeConfig::new(
        "secret-epoch",
        native_limits(1),
        output_limits(1),
        Duration::from_secs(2),
    )
    .expect("runtime config");
    let telemetry = Arc::new(BoundedTelemetryRecorder::new(64, 64).expect("telemetry"));
    let credential_runtime = NativeProcessRuntime::start_with_secret_resolver(
        config.clone(),
        Arc::new(SystemClock),
        telemetry.clone(),
        resolver.clone(),
    )
    .expect("runtime starts");
    let session = session_id("secret-environment");

    let prepared = block_on(credential_runtime.prepare_launch(&launch)).expect("launch prepares");
    assert_eq!(resolutions.load(Ordering::Acquire), 0);
    assert_eq!(
        prepared
            .secret_environment()
            .iter()
            .map(|(name, reference)| (name, reference.as_str()))
            .collect::<Vec<_>>(),
        [("UNDERCURRENT_TEST_CREDENTIAL", "credential-handle-17")],
    );
    assert!(!format!("{prepared:?}").contains(SECRET_CANARY));
    let process =
        block_on(credential_runtime.spawn(&session, &prepared)).expect("secret launch spawns");
    assert_eq!(resolutions.load(Ordering::Acquire), 1);
    block_on(credential_runtime.arm_exit(&session, &process.id)).expect("exit arms");
    assert_eq!(
        next_exit(&credential_runtime).outcome,
        ProcessOutcome::Exited(0)
    );
    let output = replay_bytes(&credential_runtime, &session);
    assert!(
        output
            .windows(SECRET_CANARY.len())
            .any(|window| window == SECRET_CANARY.as_bytes())
    );

    drop(credential_runtime);
    let restarted_runtime = NativeProcessRuntime::start_with_secret_resolver(
        config,
        Arc::new(SystemClock),
        telemetry,
        resolver,
    )
    .expect("runtime restarts");
    let restarted = block_on(restarted_runtime.spawn(&session, &prepared))
        .expect("restart resolves a fresh ephemeral overlay");
    assert_eq!(resolutions.load(Ordering::Acquire), 2);
    block_on(restarted_runtime.arm_exit(&session, &restarted.id)).expect("restart exit arms");
    assert_eq!(
        next_exit(&restarted_runtime).outcome,
        ProcessOutcome::Exited(0)
    );

    let default_runtime = runtime(1);
    let default_session = session_id("secret-unavailable");
    let prepared = block_on(default_runtime.prepare_launch(&launch)).expect("launch prepares");
    let error = block_on(default_runtime.spawn(&default_session, &prepared))
        .expect_err("missing credential adapter fails closed");
    assert_eq!(error.kind(), PortErrorKind::Unsupported);
    assert_eq!(
        default_runtime
            .pressure()
            .expect("pressure")
            .occupied_process_slots,
        0
    );
    assert!(!format!("{error:?}").contains(SECRET_CANARY));
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

#[test]
fn cancelled_successful_spawn_is_compensated_before_acceptance() {
    let runtime = runtime(1);
    let session = session_id("cancelled-spawn");
    let launch = launch("exec /bin/cat");
    let prepared = block_on(runtime.prepare_launch(&launch)).expect("launch prepares");
    let mut spawn = Box::pin(runtime.spawn(&session, &prepared));
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);

    assert!(matches!(spawn.as_mut().poll(&mut context), Poll::Pending));
    wait_until(Duration::from_secs(5), || {
        runtime
            .pressure()
            .is_ok_and(|pressure| pressure.active_process_entries == 1)
    });
    drop(spawn);
    wait_until(Duration::from_secs(5), || {
        runtime.pressure().is_ok_and(|pressure| {
            pressure.occupied_process_slots == 0 && pressure.active_process_entries == 0
        })
    });
    let pressure = runtime.pressure().expect("cancelled-spawn pressure");
    assert_eq!(pressure.published_process_exits, 0);
    assert_eq!(pressure.queued_exits, 0);
}

#[test]
fn three_hundred_live_processes_remain_bounded_and_cleanup_without_pollers() {
    #[cfg(target_os = "macos")]
    if std::env::var("UNDERCURRENT_RUNTIME_LOAD_FIXTURE").as_deref() != Ok("1") {
        // Each live PTY needs multiple descriptors. Raise the usual macOS soft
        // limit only in this fixture's subprocess, preserving the 300-process
        // assertions without changing limits for other tests or the caller.
        let status = std::process::Command::new("/bin/sh")
            .args([
                "-c",
                "ulimit -n 2048 && exec \"$1\" --exact three_hundred_live_processes_remain_bounded_and_cleanup_without_pollers --nocapture --test-threads=1",
                "uc-runtime-load",
            ])
            .arg(std::env::current_exe().expect("current test executable"))
            .env("UNDERCURRENT_RUNTIME_LOAD_FIXTURE", "1")
            .status()
            .expect("load fixture subprocess starts");
        assert!(status.success(), "load fixture subprocess failed: {status}");
        return;
    }

    const SESSION_COUNT: usize = 300;

    let runtime = runtime(SESSION_COUNT);
    let mut processes = Vec::with_capacity(SESSION_COUNT);
    for index in 0..SESSION_COUNT {
        let session = session_id(&format!("load-{index}"));
        let process =
            spawn(&runtime, &session, &launch("exec /bin/cat")).expect("load process spawns");
        block_on(runtime.arm_exit(&session, &process.id)).expect("load process exit arms");
        processes.push((session, process));
    }

    let pressure = runtime.pressure().expect("load pressure");
    assert_eq!(pressure.occupied_process_slots, SESSION_COUNT);
    assert_eq!(pressure.active_process_entries, SESSION_COUNT);
    assert_eq!(pressure.published_process_exits, 0);
    assert_eq!(pressure.input_workers, 0);
    assert_eq!(pressure.queued_input_bytes, 0);
    assert_eq!(pressure.queued_exits, 0);

    for (session, process) in processes {
        block_on(runtime.force_stop(&session, &process.id)).expect("load process stops");
    }
    let pressure = runtime.pressure().expect("clean pressure");
    assert_eq!(pressure.occupied_process_slots, 0);
    assert_eq!(pressure.active_process_entries, 0);
    assert_eq!(pressure.published_process_exits, 0);
    assert_eq!(pressure.queued_exits, 0);
}

#[test]
fn fast_exit_is_retained_until_armed_and_consumed_before_releasing_capacity() {
    let runtime = runtime(1);
    let first_session = session_id("fast-exit");
    let first = spawn(&runtime, &first_session, &launch("exit 7")).expect("fast process spawns");

    wait_until(Duration::from_secs(3), || {
        runtime
            .pressure()
            .is_ok_and(|pressure| pressure.exited_unarmed_processes == 1)
    });
    let pressure = runtime.pressure().expect("runtime pressure");
    assert_eq!(pressure.queued_exits, 0);
    assert_eq!(pressure.occupied_process_slots, 1);

    let blocked_session = session_id("blocked-by-exit");
    let error = spawn(&runtime, &blocked_session, &launch("exit 0"))
        .expect_err("unconsumed exit retains capacity");
    assert_eq!(error.kind(), PortErrorKind::CapacityExceeded);

    block_on(runtime.arm_exit(&first_session, &first.id)).expect("exit arms");
    block_on(runtime.arm_exit(&first_session, &first.id)).expect("repeated arm is idempotent");
    let exit = next_exit(&runtime);
    assert_eq!(exit.session_id, first_session);
    assert_eq!(exit.process_id, first.id);
    assert_eq!(exit.outcome, ProcessOutcome::Exited(7));
    assert_eq!(
        runtime
            .pressure()
            .expect("settled pressure")
            .occupied_process_slots,
        0
    );

    let replacement =
        spawn(&runtime, &blocked_session, &launch("exit 0")).expect("capacity is reusable");
    block_on(runtime.arm_exit(&blocked_session, &replacement.id)).expect("replacement arms");
    assert_eq!(next_exit(&runtime).outcome, ProcessOutcome::Exited(0));
}

#[test]
fn process_truth_uses_exact_runtime_incarnations_and_retains_known_exit() {
    let runtime = runtime(1);
    let session = session_id("process-truth");
    let process = spawn(&runtime, &session, &launch("sleep 1; exit 7")).expect("process spawns");
    let identity = ProcessIdentity::new(process.id.clone(), process.started_at);
    assert!(matches!(
        block_on(runtime.observe(&session, &identity)).expect("running truth"),
        undercurrent_domain::ProcessTruth::Running { .. }
    ));

    let stale = ProcessIdentity::new(
        ProcessInstanceId::new("prior-epoch-1-1").expect("stale identity"),
        TimestampMillis::new(1),
    );
    assert!(matches!(
        block_on(runtime.observe(&session, &stale)).expect("stale truth"),
        undercurrent_domain::ProcessTruth::Missing { .. }
    ));

    wait_until(Duration::from_secs(3), || {
        runtime
            .pressure()
            .is_ok_and(|pressure| pressure.exited_unarmed_processes == 1)
    });
    assert!(matches!(
        block_on(runtime.observe(&session, &identity)).expect("exit truth"),
        undercurrent_domain::ProcessTruth::Exited { exit_code: 7, .. }
    ));
    block_on(runtime.arm_exit(&session, &process.id)).expect("exit arms");
    assert_eq!(next_exit(&runtime).outcome, ProcessOutcome::Exited(7));
}

#[test]
fn shutdown_closes_spawn_admission_and_wakes_exit_waiters() {
    let runtime = runtime(1);
    let waiter_runtime = runtime.clone();
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let waiter = thread::spawn(move || sender.send(block_on(waiter_runtime.next_exit())));

    runtime.begin_shutdown();
    runtime.begin_shutdown();
    let error = receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("exit waiter wakes")
        .expect_err("closed source is unavailable");
    assert_eq!(error.kind(), PortErrorKind::Unavailable);
    waiter
        .join()
        .expect("waiter does not panic")
        .expect("result sends");
    assert!(runtime.pressure().expect("pressure").closed);

    let error = spawn(&runtime, &session_id("after-close"), &launch("exit 0"))
        .expect_err("closed runtime rejects new spawn");
    assert_eq!(error.kind(), PortErrorKind::Unavailable);
}

#[test]
fn typed_input_is_written_atomically_and_output_is_replayable() {
    let (runtime, telemetry) = runtime_with_telemetry(2);
    let session = session_id("input-output");
    let process = spawn(
        &runtime,
        &session,
        &launch("stty -echo; IFS= read -r line; printf 'reply:%s\\n' \"$line\""),
    )
    .expect("interactive process spawns");
    block_on(runtime.arm_exit(&session, &process.id)).expect("exit arms");
    let input = InputSequence::new(
        vec![
            InputItem::Text("hello".to_owned()),
            InputItem::Key(Key::Enter),
        ],
        4,
        64,
    )
    .expect("valid input");

    block_on(runtime.write_input(
        &session,
        &process.id,
        &input,
        undercurrent_domain::InputDeliveryPolicy::immediate(),
    ))
    .expect("input writes");
    wait_for_output(&runtime, &session, b"reply:hello");
    assert_eq!(next_exit(&runtime).outcome, ProcessOutcome::Exited(0));

    let output = replay_bytes(&runtime, &session);
    assert!(contains_bytes(&output, b"reply:hello"));
    let screens = runtime.screen_store();
    let screen = block_on(screens.snapshot(&session))
        .expect("screen read succeeds")
        .expect("screen exists");
    assert!(contains_bytes(screen.contents(), b"reply:hello"));
    assert_eq!(screen.output_offset().value(), output.len() as u64);
    assert_eq!(runtime.pressure().expect("pressure").input_workers, 0);
    let metrics = telemetry.metric_aggregates();
    assert!(metrics.iter().any(|metric| {
        metric.name == MetricName::TerminalOutputBytesAppended && metric.total > 0.0
    }));
    assert!(metrics.iter().any(|metric| {
        metric.name == MetricName::TerminalOutputRetainedBytes
            && metric.latest == output.len() as f64
    }));
}

#[test]
fn slow_terminal_consumer_does_not_block_supervised_process_completion() {
    let runtime = runtime(1);
    let session = session_id("slow-terminal-consumer");
    let output = runtime.output_store();
    let wake = Arc::new(CountingWake::default());
    let opened = output
        .subscribe(
            session.clone(),
            OutputOffset::ZERO,
            1_024,
            OutputGapRecovery::RestartAtAvailableOffset,
            wake.clone(),
        )
        .expect("slow consumer subscription opens");
    let (_, subscription) = opened.into_parts();

    let process = spawn(
        &runtime,
        &session,
        &launch("i=0; while [ \"$i\" -lt 8192 ]; do printf '0123456789abcdef'; i=$((i + 1)); done"),
    )
    .expect("high-output process spawns");
    block_on(runtime.arm_exit(&session, &process.id)).expect("process exit arms");
    assert_eq!(next_exit(&runtime).outcome, ProcessOutcome::Exited(0));

    let limits = output_limits(1);
    let status = output
        .session_status(&session)
        .expect("output status reads")
        .expect("output status exists");
    assert!(status.next_offset.value() > limits.per_session_bytes as u64);
    assert!(status.available_from.value() > 0);
    assert!(status.retained_bytes <= limits.per_session_bytes);
    let pressure = output.status().expect("output pressure reads");
    assert_eq!(pressure.subscriptions, 1);
    assert_eq!(pressure.dirty_subscriptions, 1);
    assert_eq!(wake.calls.load(Ordering::Acquire), 1);

    assert!(matches!(
        subscription
            .try_receive()
            .expect("slow consumer can recover after process completion"),
        Some(OutputReplay::Gap {
            requested: OutputOffset::ZERO,
            available_from,
            next_offset,
            recovery: OutputGapRecovery::RestartAtAvailableOffset,
        }) if available_from.value() > 0 && next_offset == status.next_offset
    ));
}

#[test]
fn paced_submit_blocks_other_writers_until_its_trailing_enter_is_flushed() {
    let runtime = runtime(1);
    let session = session_id("paced-input");
    let process = spawn(
        &runtime,
        &session,
        &launch(
            "stty -echo; IFS= read -r first; IFS= read -r second; printf 'reply:%s|%s\\n' \"$first\" \"$second\"",
        ),
    )
    .expect("interactive process spawns");
    block_on(runtime.arm_exit(&session, &process.id)).expect("exit arms");
    let first = InputSequence::new(
        vec![
            InputItem::Text("first".to_owned()),
            InputItem::Key(Key::Enter),
        ],
        2,
        64,
    )
    .expect("first input");
    let second = InputSequence::new(
        vec![
            InputItem::Text("second".to_owned()),
            InputItem::Key(Key::Enter),
        ],
        2,
        64,
    )
    .expect("second input");
    let writer_runtime = runtime.clone();
    let writer_session = session.clone();
    let writer_process = process.id.clone();
    let first_writer = thread::spawn(move || {
        block_on(
            writer_runtime.write_input(
                &writer_session,
                &writer_process,
                &first,
                InputDeliveryPolicy::paced_trailing_submit(
                    Duration::from_millis(150),
                    Duration::from_secs(2),
                )
                .expect("paced policy"),
            ),
        )
    });
    wait_until(Duration::from_secs(1), || {
        runtime
            .pressure()
            .is_ok_and(|pressure| pressure.queued_input_bytes > 0)
    });

    let second_started = Instant::now();
    block_on(runtime.write_input(
        &session,
        &process.id,
        &second,
        InputDeliveryPolicy::immediate(),
    ))
    .expect("second input writes after paced delivery");
    assert!(second_started.elapsed() >= Duration::from_millis(100));
    first_writer
        .join()
        .expect("first writer does not panic")
        .expect("first input writes");

    wait_for_output(&runtime, &session, b"reply:first|second");
    assert_eq!(next_exit(&runtime).outcome, ProcessOutcome::Exited(0));
    assert_eq!(runtime.pressure().expect("pressure").queued_input_bytes, 0);
}

#[test]
fn typed_standard_input_close_drains_prior_writes_and_is_idempotent() {
    let runtime = runtime(1);
    let session = session_id("typed-stdin-close");
    let process = spawn(
        &runtime,
        &session,
        &launch(
            "stty -echo; IFS= read -r line; if IFS= read -r unexpected; then exit 9; fi; printf 'closed:%s\\n' \"$line\"",
        ),
    )
    .expect("interactive process spawns");
    block_on(runtime.arm_exit(&session, &process.id)).expect("exit arms");
    let input = InputSequence::new(
        vec![
            InputItem::Text("prior-write".to_owned()),
            InputItem::Key(Key::Enter),
        ],
        2,
        64,
    )
    .expect("prior input");
    let writer_runtime = runtime.clone();
    let writer_session = session.clone();
    let writer_process = process.id.clone();
    let writer = thread::spawn(move || {
        block_on(
            writer_runtime.write_input(
                &writer_session,
                &writer_process,
                &input,
                InputDeliveryPolicy::paced_trailing_submit(
                    Duration::from_millis(150),
                    Duration::from_secs(2),
                )
                .expect("paced policy"),
            ),
        )
    });
    wait_until(Duration::from_secs(1), || {
        runtime
            .pressure()
            .is_ok_and(|pressure| pressure.queued_input_bytes > 0)
    });

    assert_eq!(
        block_on(runtime.close_input(&session, &process.id)).expect("standard input closes"),
        StandardInputClosure::Closed
    );
    writer
        .join()
        .expect("prior writer does not panic")
        .expect("prior write completes before close");
    wait_until(Duration::from_secs(5), || {
        runtime
            .pressure()
            .is_ok_and(|pressure| pressure.published_process_exits == 1)
    });
    assert_eq!(
        block_on(runtime.close_input(&session_id("another-session"), &process.id))
            .expect_err("pending exit cannot authorize another session")
            .kind(),
        PortErrorKind::NotFound
    );
    assert_eq!(
        block_on(runtime.close_input(
            &session,
            &ProcessInstanceId::new("another-process").expect("process identity"),
        ))
        .expect_err("pending exit cannot authorize another process")
        .kind(),
        PortErrorKind::NotFound
    );
    assert_eq!(
        block_on(runtime.close_input(&session, &process.id)).expect("close retry succeeds"),
        StandardInputClosure::AlreadyClosed
    );
    assert_eq!(next_exit(&runtime).outcome, ProcessOutcome::Exited(0));
    assert_eq!(
        block_on(runtime.close_input(&session, &process.id))
            .expect_err("consuming the exit retires its input identity")
            .kind(),
        PortErrorKind::NotFound
    );
    assert!(contains_bytes(
        &replay_bytes(&runtime, &session),
        b"closed:prior-write"
    ));
}

#[test]
fn resize_targets_the_exact_live_pty() {
    let runtime = runtime(1);
    let session = session_id("resize");
    let process = spawn(
        &runtime,
        &session,
        &launch("stty size; IFS= read -r line; stty size"),
    )
    .expect("resize process spawns");
    block_on(runtime.arm_exit(&session, &process.id)).expect("exit arms");
    wait_for_output(&runtime, &session, b"24 80");

    let resized = TerminalSize::new(101, 37, 200, 100).expect("valid terminal size");
    block_on(runtime.resize(&session, &process.id, resized)).expect("PTY resizes");
    let screens = runtime.screen_store();
    let screen = block_on(screens.snapshot(&session))
        .expect("screen read succeeds")
        .expect("screen exists");
    assert_eq!((screen.columns(), screen.rows()), (101, 37));
    let input = InputSequence::new(vec![InputItem::Key(Key::Enter)], 1, 16).expect("valid input");
    block_on(runtime.write_input(
        &session,
        &process.id,
        &input,
        undercurrent_domain::InputDeliveryPolicy::immediate(),
    ))
    .expect("input writes");

    wait_for_output(&runtime, &session, b"37 101");
    assert_eq!(next_exit(&runtime).outcome, ProcessOutcome::Exited(0));
}

#[test]
fn environment_file_precedes_explicit_values_and_absolute_cwd_is_applied() {
    let directory = tempdir().expect("temporary directory");
    let environment_path = directory.path().join("session.env");
    std::fs::write(&environment_path, "FROM_FILE=file\nOVERRIDE=file\n")
        .expect("write environment file");
    let environment = EnvironmentVariables::new(
        [("OVERRIDE".to_owned(), "explicit".to_owned())],
        execution_limits(),
    )
    .expect("valid environment");
    let launch = LaunchSpec::new(
        command("printf '%s|%s|%s\\n' \"$FROM_FILE\" \"$OVERRIDE\" \"$PWD\""),
        environment,
        Some(
            PathReference::new(environment_path.to_string_lossy(), 4_096)
                .expect("valid environment path"),
        ),
        Some(
            PathReference::new(directory.path().to_string_lossy(), 4_096)
                .expect("valid working directory"),
        ),
        terminal_size(),
    )
    .with_environment_binding(Some(ExecutionEnvironmentBinding::new(
        PayloadDigest::of_bytes(b"test environment binding"),
        PathReference::new(directory.path().to_string_lossy(), 4_096)
            .expect("valid environment root"),
    )));
    let runtime = runtime(1);
    let session = session_id("environment");

    let process = spawn(&runtime, &session, &launch).expect("process spawns");
    block_on(runtime.arm_exit(&session, &process.id)).expect("exit arms");
    wait_for_output(&runtime, &session, b"file|explicit|");
    assert_eq!(next_exit(&runtime).outcome, ProcessOutcome::Exited(0));
    let output = String::from_utf8_lossy(&replay_bytes(&runtime, &session)).into_owned();
    assert!(output.contains(&directory.path().to_string_lossy().into_owned()));
}

#[test]
#[cfg(target_os = "linux")]
fn live_shell_readiness_does_not_survive_a_silent_child_taking_over() {
    for job_control in ["", "set -m;"] {
        let runtime = runtime(1);
        let session = session_id("shell-ownership");
        let process = spawn(&runtime, &session, &launch(&format!(
            "{job_control} printf '\\033]9091;uc=ready\\007'; printf 'waiting\\n'; read input; sleep 30; read input"
        ))).expect("shell spawns");
        wait_for_output(&runtime, &session, b"waiting");
        wait_until(Duration::from_secs(3), || {
            block_on(runtime.observe_foreground(&session, &process.id)).is_ok_and(|observation| {
                observation.cooperative() == Some(CooperativeForegroundState::Ready)
            })
        });
        let input = InputSequence::new(vec![InputItem::Key(Key::Enter)], 4, 64).unwrap();
        block_on(runtime.write_input(
            &session,
            &process.id,
            &input,
            InputDeliveryPolicy::immediate(),
        ))
        .expect("start silent child");
        // Deliberately omit the running hook: ownership must independently
        // invalidate the old ready signal, including without job control.
        wait_until(Duration::from_secs(3), || {
            block_on(runtime.observe_foreground(&session, &process.id)).is_ok_and(|observation| {
                observation.cooperative().is_none()
                    && observation
                        .members()
                        .iter()
                        .any(|member| member.target().basename() == "sleep")
            })
        });
        block_on(runtime.force_stop(&session, &process.id)).expect("shell stops");
        assert!(block_on(runtime.observe_foreground(&session, &process.id)).is_err());
    }
}

#[test]
fn managed_session_identity_and_cooperative_signals_stay_out_of_terminal_output() {
    let runtime = runtime(1);
    let session = session_id("cooperative-shell");
    let process = spawn(
        &runtime,
        &session,
        &launch(
            "printf 'session:%s\\n' \"$UC_SESSION\"; printf '\\033]9091;uc=ready\\007'; printf 'visible\\n'; sleep 2",
        ),
    )
    .expect("cooperative process spawns");
    block_on(runtime.arm_exit(&session, &process.id)).expect("exit arms");

    wait_for_output(&runtime, &session, b"visible");
    wait_until(Duration::from_secs(3), || {
        runtime
            .cooperative_signal_store()
            .latest_exact(&session, &process.id)
            .is_some()
    });

    let output = replay_bytes(&runtime, &session);
    assert!(contains_bytes(&output, b"session:cooperative-shell"));
    assert!(contains_bytes(&output, b"visible"));
    assert!(!contains_bytes(&output, b"9091"));
    assert_eq!(
        runtime
            .cooperative_signal_store()
            .latest_exact(&session, &process.id)
            .map(|signal| signal.foreground()),
        Some(CooperativeForegroundState::Ready)
    );

    block_on(runtime.force_stop(&session, &process.id)).expect("process stops");
    assert!(
        runtime
            .cooperative_signal_store()
            .latest_exact(&session, &process.id)
            .is_none()
    );
}

#[test]
fn opaque_capture_preserves_cooperative_osc_bytes_without_screen_projection() {
    let runtime = runtime(1);
    let session = session_id("opaque-output");
    let launch = launch("printf 'prefix'; printf '\\033]9091;uc=ready\\007'; printf 'suffix'")
        .with_output_capture_mode(OutputCaptureMode::Opaque);
    let process = spawn(&runtime, &session, &launch).expect("opaque process spawns");
    block_on(runtime.arm_exit(&session, &process.id)).expect("exit arms");
    assert_eq!(next_exit(&runtime).outcome, ProcessOutcome::Exited(0));

    assert_eq!(
        replay_bytes(&runtime, &session),
        b"prefix\x1b]9091;uc=ready\x07suffix"
    );
    let screens = runtime.screen_store();
    assert_eq!(
        screens.capture_mode(&session),
        Some(OutputCaptureMode::Opaque)
    );
    assert_eq!(
        block_on(screens.snapshot(&session)).unwrap_err().kind(),
        PortErrorKind::Unsupported
    );
    assert!(
        runtime
            .cooperative_signal_store()
            .latest_exact(&session, &process.id)
            .is_none()
    );
}

#[test]
fn opaque_capture_resize_updates_the_pty_without_allocating_a_screen() {
    let runtime = runtime(1);
    let session = session_id("opaque-resize");
    let launch = launch("stty size; IFS= read -r line; stty size")
        .with_output_capture_mode(OutputCaptureMode::Opaque);
    let process = spawn(&runtime, &session, &launch).expect("opaque process spawns");
    block_on(runtime.arm_exit(&session, &process.id)).expect("exit arms");
    wait_for_output(&runtime, &session, b"24 80");

    let resized = TerminalSize::new(101, 37, 200, 100).expect("valid terminal size");
    block_on(runtime.resize(&session, &process.id, resized)).expect("opaque PTY resizes");
    let screens = runtime.screen_store();
    assert_eq!(
        screens.capture_mode(&session),
        Some(OutputCaptureMode::Opaque)
    );
    assert_eq!(
        block_on(screens.snapshot(&session)).unwrap_err().kind(),
        PortErrorKind::Unsupported
    );
    let input = InputSequence::new(vec![InputItem::Key(Key::Enter)], 1, 16).expect("valid input");
    block_on(runtime.write_input(
        &session,
        &process.id,
        &input,
        InputDeliveryPolicy::immediate(),
    ))
    .expect("input writes");

    wait_for_output(&runtime, &session, b"37 101");
    assert_eq!(next_exit(&runtime).outcome, ProcessOutcome::Exited(0));
}

#[test]
fn graceful_stop_and_timeout_escalation_are_distinguished() {
    let runtime = runtime(2);
    let graceful_session = session_id("graceful-stop");
    let graceful = spawn(
        &runtime,
        &graceful_session,
        &launch("trap 'exit 0' TERM; printf 'ready\\n'; while :; do sleep 1; done"),
    )
    .expect("graceful process spawns");
    block_on(runtime.arm_exit(&graceful_session, &graceful.id)).expect("exit arms");
    wait_for_output(&runtime, &graceful_session, b"ready");
    let graceful_policy =
        StopPolicy::new(StopSignal::Terminate, Duration::from_secs(2)).expect("valid policy");

    let termination = block_on(runtime.stop(&graceful_session, &graceful.id, graceful_policy))
        .expect("graceful stop confirms");
    assert_eq!(termination.kind(), TerminationKind::Graceful);
    assert_eq!(termination.exit_code(), Some(0));
    let pressure = runtime.pressure().expect("graceful pressure");
    assert_eq!(pressure.occupied_process_slots, 0);
    assert_eq!(pressure.queued_exits, 0);

    let forced_session = session_id("forced-stop");
    let forced = spawn(
        &runtime,
        &forced_session,
        &launch("trap '' TERM; printf 'ready\\n'; while :; do sleep 1; done"),
    )
    .expect("signal-ignoring process spawns");
    block_on(runtime.arm_exit(&forced_session, &forced.id)).expect("exit arms");
    wait_for_output(&runtime, &forced_session, b"ready");
    let short_policy =
        StopPolicy::new(StopSignal::Terminate, Duration::from_millis(100)).expect("valid policy");

    let termination = block_on(runtime.stop(&forced_session, &forced.id, short_policy))
        .expect("escalated stop confirms");
    assert_eq!(termination.kind(), TerminationKind::ForcedAfterTimeout);
    assert_eq!(termination.exit_code(), None);
    let pressure = runtime.pressure().expect("forced pressure");
    assert_eq!(pressure.occupied_process_slots, 0);
    assert_eq!(pressure.queued_exits, 0);

    let replacement = spawn(&runtime, &forced_session, &launch("exit 9"))
        .expect("deliberately stopped session can restart immediately");
    block_on(runtime.arm_exit(&forced_session, &replacement.id)).expect("replacement arms");
    assert_eq!(next_exit(&runtime).outcome, ProcessOutcome::Exited(9));
}

#[test]
fn graceful_only_timeout_leaves_process_alive_until_explicit_force() {
    let runtime = runtime(1);
    let id = session_id("graceful-only");
    let process = spawn(
        &runtime,
        &id,
        &launch("trap '' TERM; printf 'ready\\n'; while :; do sleep 1; done"),
    )
    .unwrap();
    block_on(runtime.arm_exit(&id, &process.id)).unwrap();
    wait_for_output(&runtime, &id, b"ready");
    let policy = StopPolicy::new(StopSignal::Terminate, Duration::from_millis(100))
        .unwrap()
        .with_no_escalation(true);
    let result = block_on(runtime.stop(&id, &process.id, policy));
    assert_eq!(result.unwrap().kind(), TerminationKind::GracefulTimedOut);
    assert_eq!(runtime.pressure().unwrap().occupied_process_slots, 1);
    let result = block_on(runtime.force_stop(&id, &process.id)).unwrap();
    assert_eq!(result.kind(), TerminationKind::Forced);
}

#[test]
fn force_interrupts_an_active_graceful_wait() {
    let runtime = runtime(1);
    let id = session_id("interrupt-grace");
    let process = spawn(
        &runtime,
        &id,
        &launch("trap 'printf signalled' TERM; printf ready; while :; do sleep 1; done"),
    )
    .unwrap();
    block_on(runtime.arm_exit(&id, &process.id)).unwrap();
    wait_for_output(&runtime, &id, b"ready");
    thread::scope(|scope| {
        let graceful = scope.spawn(|| {
            block_on(runtime.stop(
                &id,
                &process.id,
                StopPolicy::new(StopSignal::Terminate, Duration::from_secs(10)).unwrap(),
            ))
        });
        wait_for_output(&runtime, &id, b"signalled");
        let started = Instant::now();
        assert_eq!(
            block_on(runtime.force_stop(&id, &process.id))
                .unwrap()
                .kind(),
            TerminationKind::Forced
        );
        assert!(started.elapsed() < Duration::from_secs(2));
        graceful.join().unwrap().unwrap();
    });
    assert_eq!(runtime.pressure().unwrap().occupied_process_slots, 0);
}

#[test]
fn graceful_stop_escalates_when_only_descendants_retain_the_terminal() {
    let runtime = runtime(1);
    let id = session_id("descendant-drain");
    let process = spawn(
        &runtime,
        &id,
        &launch("trap 'exit 0' TERM; (trap '' TERM HUP; printf ready; exec sleep 30) & wait"),
    )
    .unwrap();
    block_on(runtime.arm_exit(&id, &process.id)).unwrap();
    wait_for_output(&runtime, &id, b"ready");
    let started = Instant::now();
    let termination = block_on(runtime.stop(
        &id,
        &process.id,
        StopPolicy::new(StopSignal::Terminate, Duration::from_millis(100)).unwrap(),
    ))
    .unwrap();
    assert_eq!(termination.kind(), TerminationKind::ForcedAfterTimeout);
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(runtime.pressure().unwrap().occupied_process_slots, 0);
}

fn runtime(max_processes: usize) -> NativeProcessRuntime {
    runtime_with_telemetry(max_processes).0
}

fn spawn(
    runtime: &NativeProcessRuntime,
    session: &SessionId,
    launch: &LaunchSpec,
) -> Result<ProcessInstance, PortError> {
    let prepared = block_on(runtime.prepare_launch(launch))?;
    block_on(runtime.spawn(session, &prepared))
}

fn runtime_with_telemetry(
    max_processes: usize,
) -> (NativeProcessRuntime, Arc<BoundedTelemetryRecorder>) {
    let config = NativeProcessRuntimeConfig::new(
        "test-epoch",
        native_limits(max_processes),
        output_limits(max_processes),
        Duration::from_secs(2),
    )
    .expect("valid runtime configuration");
    let telemetry = Arc::new(BoundedTelemetryRecorder::new(64, 64).expect("telemetry"));
    let runtime = NativeProcessRuntime::start_with_system_clock(config, telemetry.clone())
        .expect("runtime starts");
    (runtime, telemetry)
}

fn native_limits(max_processes: usize) -> NativeProcessRuntimeLimits {
    NativeProcessRuntimeLimits {
        max_processes,
        spawn_workers: max_processes.min(2),
        spawn_queue_capacity: max_processes.max(2),
        input_queue_sequences: 8,
        input_queue_bytes_per_process: 4_096,
        output_read_bytes: 1_024,
        screen_snapshot_bytes: 256 * 1_024,
        environment_file_bytes: 4_096,
        environment_entries: 32,
        environment_bytes: 8_192,
        worker_thread_stack_bytes: 128 * 1024,
    }
}

fn output_limits(max_processes: usize) -> TerminalOutputLimits {
    TerminalOutputLimits {
        per_session_bytes: 64 * 1024,
        global_bytes: max_processes.max(4) * 64 * 1024,
        chunk_bytes: 1_024,
        append_bytes: 1_024,
        replay_bytes: 64 * 1024,
        sessions: max_processes.max(8),
        subscriptions: max_processes.max(8) * 4,
    }
}

fn launch(script: &str) -> LaunchSpec {
    LaunchSpec::new(
        command(script),
        EnvironmentVariables::new(Vec::new(), execution_limits()).expect("empty environment"),
        None,
        None,
        terminal_size(),
    )
}

fn command(script: &str) -> CommandSpec {
    CommandSpec::new(
        "/bin/sh",
        vec!["-c".to_owned(), script.to_owned()],
        execution_limits(),
    )
    .expect("valid command")
    .with_launch_mode(FIXTURE_LAUNCH_MODE)
}

fn execution_limits() -> ExecutionLimits {
    ExecutionLimits {
        max_arguments: 8,
        max_command_bytes: 4_096,
        max_environment_entries: 32,
        max_environment_bytes: 8_192,
        max_path_bytes: 4_096,
        max_health_rules: 1,
        max_health_rule_bytes: 64,
    }
}

fn terminal_size() -> TerminalSize {
    TerminalSize::new(80, 24, 200, 100).expect("valid terminal size")
}

fn session_id(value: &str) -> SessionId {
    SessionId::new(value).expect("valid session ID")
}

fn next_exit(runtime: &NativeProcessRuntime) -> undercurrent_application::ProcessExitObservation {
    let runtime = runtime.clone();
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let worker = thread::spawn(move || sender.send(block_on(runtime.next_exit())));
    let result = receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("process exit arrives before timeout")
        .expect("exit source remains available");
    worker
        .join()
        .expect("exit waiter does not panic")
        .expect("reply sends");
    result
}

fn wait_for_output(runtime: &NativeProcessRuntime, session_id: &SessionId, needle: &[u8]) {
    wait_until(Duration::from_secs(5), || {
        contains_bytes(&replay_bytes(runtime, session_id), needle)
    });
}

fn replay_bytes(runtime: &NativeProcessRuntime, session_id: &SessionId) -> Vec<u8> {
    match runtime
        .output_store()
        .replay(
            session_id,
            OutputOffset::ZERO,
            64 * 1024,
            OutputGapRecovery::RestartAtAvailableOffset,
        )
        .expect("output replay")
    {
        OutputReplay::Data { slices, .. } => slices
            .iter()
            .flat_map(|slice| slice.bytes().iter().copied())
            .collect(),
        OutputReplay::Gap { .. } => panic!("test output must remain retained"),
    }
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !predicate() {
        assert!(
            Instant::now() < deadline,
            "condition did not become true before timeout"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => thread::park(),
        }
    }
}

struct ThreadWake(thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

#[derive(Default)]
struct CountingWake {
    calls: AtomicUsize,
}

impl SubscriptionWake for CountingWake {
    fn wake(&self) {
        self.calls.fetch_add(1, Ordering::AcqRel);
    }
}
