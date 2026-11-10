// SPDX-License-Identifier: Apache-2.0

#![cfg(unix)]

use std::fs;
use std::future::Future;
use std::io::{Read, Write};
use std::process::{Command as SystemCommand, Stdio};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use tempfile::tempdir;
use undercurrent_application::{
    MetricName, OutputGapRecovery, OutputOffset, OutputReplay, ProcessExitSource, ProcessRuntime,
    ProcessTruthSource, StandardInputClosure, TerminalOutputLimits, TerminalScreenReader,
};
use undercurrent_domain::{
    CommandSpec, EnvironmentVariables, ExecutionLimits, InputDeliveryPolicy, InputItem,
    InputSequence, Key, LaunchMode, LaunchSpec, OutputCaptureMode, ProcessIdentity, ProcessOutcome,
    ProcessTruth, SessionId, TerminalSize,
};
use undercurrent_runtime::{
    BoundedTelemetryRecorder, HostedProcessRuntime, HostedProcessRuntimeConfig,
    NativeProcessRuntimeConfig, NativeProcessRuntimeLimits, SystemClock,
};

const HOST_EXECUTABLE: &str = env!("CARGO_BIN_EXE_uc-session-host");

// macOS rejects sealed launches; retain the existing sealed coverage on Linux.
const FIXTURE_LAUNCH_MODE: LaunchMode = if cfg!(target_os = "macos") {
    LaunchMode::Native
} else {
    LaunchMode::Sealed
};

#[test]
fn stopped_session_relaunch_preserves_fresh_output_and_screen() {
    assert_stopped_session_relaunch_output(false);
}

#[test]
fn attempted_session_relaunch_preserves_fresh_output_and_screen() {
    assert_stopped_session_relaunch_output(true);
}

fn assert_stopped_session_relaunch_output(attempted: bool) {
    let temp = tempdir().expect("temporary directory");
    let socket = temp.path().join("session-host.sock");
    let trigger = temp.path().join("finish");
    let session = SessionId::new("restart-output").expect("session ID");
    let runtime = hosted_runtime(&socket, "daemon-restart-output");
    let prepared = block_on(runtime.prepare_launch(&wait_for_trigger_launch(&trigger)))
        .expect("launch prepares once");
    let policy = undercurrent_domain::StopPolicy::new(
        undercurrent_domain::StopSignal::Terminate,
        Duration::from_secs(2),
    )
    .expect("stop policy");
    let mut previous_offset = OutputOffset::ZERO;

    for run in 1..=5 {
        let process = if attempted {
            let context = undercurrent_application::ProcessMutationContext::new(
                undercurrent_domain::OperationAttemptId::new(format!("restart-{run}"))
                    .expect("attempt ID"),
                Some(session.clone()),
                undercurrent_domain::ProcessMutationKind::Spawn,
                undercurrent_domain::FencingToken::new(run as u64).expect("fence"),
                undercurrent_domain::PayloadDigest::of_bytes(b"restart-output"),
                undercurrent_domain::TimestampMillis::new(run as i64),
            );
            block_on(runtime.spawn_attempted(&context, &session, &prepared))
        } else {
            block_on(runtime.spawn(&session, &prepared))
        }
        .expect("saved launch starts");
        block_on(runtime.arm_exit(&session, &process.id)).expect("exit arms");
        wait_until(Duration::from_secs(5), || {
            runtime
                .output_store()
                .session_status(&session)
                .unwrap()
                .is_some_and(|status| status.next_offset > previous_offset)
        });
        let screen = block_on(runtime.screen_store().snapshot(&session))
            .expect("screen reads")
            .expect("screen exists");
        let bytes = replay_bytes(&runtime, &session);
        block_on(runtime.stop(&session, &process.id, policy)).expect("process stops");
        assert!(
            contains_bytes(screen.contents(), b"before-restart"),
            "replacement process output must populate its new screen"
        );
        assert_eq!(
            bytes
                .windows(b"before-restart".len())
                .filter(|window| *window == b"before-restart")
                .count(),
            run,
            "restart must retain earlier logs and append the replacement output exactly once"
        );
        previous_offset = runtime
            .output_store()
            .session_status(&session)
            .unwrap()
            .unwrap()
            .next_offset;
    }

    runtime.begin_shutdown();
    drop(runtime);
    wait_until(Duration::from_secs(5), || !socket.exists());
}

#[test]
fn execution_supervisor_relays_output_through_the_real_host_binary() {
    let mut supervisor = SystemCommand::new(HOST_EXECUTABLE)
        .arg(undercurrent_runtime::EXECUTION_SUPERVISOR_ARGUMENT)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("execution supervisor starts");
    let bootstrap = serde_json::to_vec(&serde_json::json!({
        "expected_parent_pid": std::process::id(),
        "program": "/bin/sh",
        "arguments": ["-c", "printf 'supervisor-output'"],
        "environment": [],
        "working_directory": null
    }))
    .expect("supervisor bootstrap encodes");
    let mut stdin = supervisor.stdin.take().expect("supervisor stdin is piped");
    stdin
        .write_all(&(bootstrap.len() as u64).to_be_bytes())
        .and_then(|()| stdin.write_all(&bootstrap))
        .and_then(|()| stdin.flush())
        .expect("supervisor bootstrap writes");
    drop(stdin);

    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        match supervisor
            .try_wait()
            .expect("supervisor status is observable")
        {
            Some(status) => break status,
            None if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
            None => {
                supervisor.kill().expect("timed-out supervisor terminates");
                supervisor.wait().expect("timed-out supervisor reaps");
                panic!(
                    "real execution supervisor did not relay output and exit within five seconds"
                );
            }
        }
    };
    let mut stdout = Vec::new();
    supervisor
        .stdout
        .take()
        .expect("supervisor stdout is piped")
        .read_to_end(&mut stdout)
        .expect("supervisor stdout reads");
    let mut stderr = String::new();
    supervisor
        .stderr
        .take()
        .expect("supervisor stderr is piped")
        .read_to_string(&mut stderr)
        .expect("supervisor stderr reads");

    assert!(status.success(), "supervisor failed: {stderr}");
    assert!(stdout.starts_with(b"UCHXSUP0.1.0-beta\x01"));
    assert!(stdout.len() >= 30);
    let mut framed = &stdout[30..];
    let mut output = Vec::new();
    loop {
        assert!(framed.len() >= 4, "missing output completion marker");
        let length = u32::from_be_bytes(framed[..4].try_into().unwrap()) as usize;
        framed = &framed[4..];
        if length == 0 {
            assert!(framed.is_empty());
            break;
        }
        assert!(length <= 8192 && framed.len() >= length);
        output.extend_from_slice(&framed[..length]);
        framed = &framed[length..];
    }
    assert_eq!(output, b"supervisor-output");
}

#[test]
fn running_process_and_screen_reattach_then_publish_exact_exit() {
    let temp = tempdir().expect("temporary directory");
    let socket = temp.path().join("session-host.sock");
    let trigger = temp.path().join("finish");
    let session = SessionId::new("reattach-running").expect("session ID");
    let launch = wait_for_trigger_launch(&trigger);

    let first = hosted_runtime(&socket, "daemon-one");
    let process = spawn(&first, &session, &launch).expect("process starts");
    block_on(first.arm_exit(&session, &process.id)).expect("exit arms");
    wait_for_output(&first, &session, b"before-restart");
    let durable = ProcessIdentity::new(process.id.clone(), process.started_at);
    first.begin_shutdown();
    drop(first);

    let second = hosted_runtime(&socket, "daemon-two");
    assert!(matches!(
        block_on(second.observe(&session, &durable)).expect("process truth"),
        ProcessTruth::Running { .. }
    ));
    block_on(second.arm_exit(&session, &process.id)).expect("reattached exit arms");
    let screen = block_on(second.screen_store().snapshot(&session))
        .expect("screen read")
        .expect("screen exists");
    assert!(contains_bytes(screen.contents(), b"before-restart"));

    fs::write(&trigger, b"finish").expect("release process");
    let exit = next_exit(&second);
    assert_eq!(exit.session_id, session);
    assert_eq!(exit.process_id, process.id);
    assert_eq!(exit.outcome, ProcessOutcome::Exited(7));
    wait_for_output(&second, &exit.session_id, b"after-restart");
    second.begin_shutdown();
    drop(second);
    wait_until(Duration::from_secs(5), || !socket.exists());
}

#[test]
fn exit_while_daemon_is_unavailable_reconciles_code_and_buffered_screen() {
    let temp = tempdir().expect("temporary directory");
    let socket = temp.path().join("session-host.sock");
    let trigger = temp.path().join("finish");
    let session = SessionId::new("exit-while-unavailable").expect("session ID");

    let first = hosted_runtime(&socket, "daemon-one");
    let process =
        spawn(&first, &session, &wait_for_trigger_launch(&trigger)).expect("process starts");
    block_on(first.arm_exit(&session, &process.id)).expect("exit arms");
    wait_for_output(&first, &session, b"before-restart");
    let pid = fs::read_to_string(trigger.with_extension("pid"))
        .expect("fixture PID reads")
        .parse::<u32>()
        .expect("fixture PID parses");
    let durable = ProcessIdentity::new(process.id.clone(), process.started_at);
    first.begin_shutdown();
    drop(first);
    fs::write(&trigger, b"finish").expect("release process without daemon");
    // Observe actual process death before restarting the daemon; a fixed sleep
    // does not guarantee that the fixture ran on a busy hosted runner.
    wait_until(Duration::from_secs(5), || !process_is_alive(pid));

    let second = hosted_runtime(&socket, "daemon-two");
    wait_until(Duration::from_secs(5), || {
        matches!(
            block_on(second.observe(&session, &durable)).expect("process truth"),
            ProcessTruth::Exited { exit_code: 7, .. }
        )
    });
    let screen = block_on(second.screen_store().snapshot(&session))
        .expect("screen read")
        .expect("screen exists");
    assert!(contains_bytes(screen.contents(), b"before-restart"));
    assert!(contains_bytes(screen.contents(), b"after-restart"));
    second.begin_shutdown();
    drop(second);
    wait_until(Duration::from_secs(5), || !socket.exists());
}

#[test]
fn fast_exit_without_output_is_retained_and_published_through_the_real_host() {
    let temp = tempdir().expect("temporary directory");
    let socket = temp.path().join("session-host.sock");
    let session = SessionId::new("silent-fast-exit").expect("session ID");
    let runtime = hosted_runtime(&socket, "daemon-silent-fast-exit");

    let process = spawn(&runtime, &session, &silent_exit_launch()).expect("process starts");
    block_on(runtime.arm_exit(&session, &process.id)).expect("exit arms");

    let exit = next_exit(&runtime);
    assert_eq!(exit.session_id, session);
    assert_eq!(exit.process_id, process.id);
    assert_eq!(exit.outcome, ProcessOutcome::Exited(7));
    runtime.begin_shutdown();
    drop(runtime);
    wait_until(Duration::from_secs(5), || !socket.exists());
}

#[test]
fn opaque_output_reattaches_with_exact_bytes_and_without_a_screen_projection() {
    let temp = tempdir().expect("temporary directory");
    let socket = temp.path().join("session-host.sock");
    let trigger = temp.path().join("finish");
    let session = SessionId::new("reattach-opaque").expect("session ID");
    let launch =
        wait_for_trigger_launch(&trigger).with_output_capture_mode(OutputCaptureMode::Opaque);

    let first = hosted_runtime(&socket, "opaque-daemon-one");
    let process = spawn(&first, &session, &launch).expect("opaque process starts");
    wait_for_output(&first, &session, b"before-restart");
    let screen = block_on(first.screen_store().snapshot(&session)).unwrap_err();
    assert_eq!(
        screen.kind(),
        undercurrent_application::PortErrorKind::Unsupported
    );
    let durable = ProcessIdentity::new(process.id.clone(), process.started_at);
    first.begin_shutdown();
    drop(first);

    let second = hosted_runtime(&socket, "opaque-daemon-two");
    assert!(matches!(
        block_on(second.observe(&session, &durable)).expect("opaque process truth"),
        ProcessTruth::Running { .. }
    ));
    fs::write(&trigger, b"finish").expect("release opaque process");
    wait_for_output(&second, &session, b"after-restart");
    let bytes = replay_bytes(&second, &session);
    assert_eq!(
        bytes, b"before-restart\r\nafter-restart\r\n",
        "opaque replay neither loses nor duplicates bytes across daemon reattach"
    );
    let status = second
        .output_store()
        .session_status(&session)
        .expect("output status")
        .expect("opaque output session");
    assert_eq!(status.output_capture_mode, OutputCaptureMode::Opaque);
    assert_eq!(status.next_offset.value(), bytes.len() as u64);
    let screen = block_on(second.screen_store().snapshot(&session)).unwrap_err();
    assert_eq!(
        screen.kind(),
        undercurrent_application::PortErrorKind::Unsupported
    );
    wait_until(Duration::from_secs(5), || {
        matches!(
            block_on(second.observe(&session, &durable)),
            Ok(ProcessTruth::Exited { exit_code: 7, .. })
        )
    });
    second.begin_shutdown();
    drop(second);
    wait_until(Duration::from_secs(5), || !socket.exists());
}

#[test]
fn cancelled_spawn_future_force_cleans_remote_process_before_adoption() {
    let temp = tempdir().expect("temporary directory");
    let socket = temp.path().join("session-host.sock");
    let pid_file = temp.path().join("spawn.pid");
    let session = SessionId::new("cancelled-hosted-spawn").expect("session ID");
    let runtime = hosted_runtime(&socket, "daemon-cancel");
    let launch = record_pid_launch(&pid_file);
    let prepared = block_on(runtime.prepare_launch(&launch)).expect("launch prepares");
    let mut spawn = Box::pin(runtime.spawn(&session, &prepared));
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);

    assert!(matches!(spawn.as_mut().poll(&mut context), Poll::Pending));
    wait_until(Duration::from_secs(5), || pid_file.exists());
    let pid = fs::read_to_string(&pid_file)
        .expect("PID file reads")
        .parse::<u32>()
        .expect("PID parses");
    assert!(process_is_alive(pid));

    drop(spawn);
    wait_until(Duration::from_secs(5), || !process_is_alive(pid));
    wait_until(Duration::from_secs(5), || {
        runtime
            .pressure()
            .is_ok_and(|pressure| pressure.occupied_process_slots == 0)
    });
    runtime.begin_shutdown();
    drop(runtime);
    wait_until(Duration::from_secs(5), || !socket.exists());
}

#[test]
fn paced_terminal_input_round_trips_through_the_host_channel() {
    let temp = tempdir().expect("temporary directory");
    let socket = temp.path().join("session-host.sock");
    let session = SessionId::new("hosted-input").expect("session ID");
    let (runtime, telemetry) = hosted_runtime_with_telemetry(&socket, "daemon-input");
    let process =
        spawn(&runtime, &session, &interactive_launch()).expect("interactive process starts");
    block_on(runtime.arm_exit(&session, &process.id)).expect("exit arms");
    let input = InputSequence::new(
        vec![
            InputItem::Text("hosted-hello".to_owned()),
            InputItem::Key(Key::Enter),
        ],
        4,
        64,
    )
    .expect("input sequence");

    block_on(
        runtime.write_input(
            &session,
            &process.id,
            &input,
            InputDeliveryPolicy::paced_trailing_submit(
                Duration::from_millis(50),
                Duration::from_secs(2),
            )
            .expect("paced policy"),
        ),
    )
    .expect("input writes");
    wait_for_output(&runtime, &session, b"reply:hosted-hello");
    let metrics = telemetry.metric_aggregates();
    assert!(metrics.iter().any(|metric| {
        metric.name == MetricName::TerminalOutputBytesAppended && metric.total > 0.0
    }));
    assert!(metrics.iter().any(|metric| {
        metric.name == MetricName::TerminalOutputRetainedBytes && metric.latest > 0.0
    }));
    assert_eq!(next_exit(&runtime).outcome, ProcessOutcome::Exited(0));
    runtime.begin_shutdown();
    drop(runtime);
    wait_until(Duration::from_secs(5), || !socket.exists());
}

#[test]
fn typed_standard_input_close_round_trips_through_the_private_host_channel() {
    let temp = tempdir().expect("temporary directory");
    let socket = temp.path().join("session-host.sock");
    let session = SessionId::new("hosted-stdin-close").expect("session ID");
    let runtime = hosted_runtime(&socket, "daemon-stdin-close");
    let process = spawn(&runtime, &session, &eof_launch()).expect("interactive process starts");
    block_on(runtime.arm_exit(&session, &process.id)).expect("exit arms");
    let input = InputSequence::new(
        vec![
            InputItem::Text("before-eof".to_owned()),
            InputItem::Key(Key::Enter),
        ],
        4,
        64,
    )
    .expect("input sequence");

    block_on(runtime.write_input(
        &session,
        &process.id,
        &input,
        InputDeliveryPolicy::immediate(),
    ))
    .expect("input writes");
    assert_eq!(
        block_on(runtime.close_input(&session, &process.id)).expect("standard input closes"),
        StandardInputClosure::Closed
    );
    assert_eq!(
        block_on(runtime.close_input(&session, &process.id)).expect("close retry succeeds"),
        StandardInputClosure::AlreadyClosed
    );
    assert_eq!(next_exit(&runtime).outcome, ProcessOutcome::Exited(0));
    wait_for_output(&runtime, &session, b"closed:before-eof");
    runtime.begin_shutdown();
    drop(runtime);
    wait_until(Duration::from_secs(5), || !socket.exists());
}

#[test]
fn terminal_input_targets_only_the_exact_process_with_three_hosted_ptys() {
    let temp = tempdir().expect("temporary directory");
    let socket = temp.path().join("session-host.sock");
    let runtime = hosted_runtime(&socket, "daemon-input-routing");
    let sessions = [
        SessionId::new("hosted-input-idle-one").expect("first session ID"),
        SessionId::new("hosted-input-idle-two").expect("second session ID"),
        SessionId::new("hosted-input-target").expect("target session ID"),
    ];
    let mut processes = Vec::with_capacity(sessions.len());
    for session in &sessions {
        let process =
            spawn(&runtime, session, &interactive_launch()).expect("interactive process starts");
        block_on(runtime.arm_exit(session, &process.id)).expect("exit arms");
        processes.push(process);
    }
    let input = InputSequence::new(
        vec![
            InputItem::Text("burst".to_owned()),
            InputItem::Key(Key::Enter),
        ],
        4,
        64,
    )
    .expect("input sequence");
    assert_eq!(
        undercurrent_runtime::encode_terminal_input(&input).expect("input encodes"),
        b"burst\r"
    );

    block_on(runtime.write_input(
        &sessions[2],
        &processes[2].id,
        &input,
        InputDeliveryPolicy::immediate(),
    ))
    .expect("target input writes");
    wait_for_output(&runtime, &sessions[2], b"reply:burst");
    let exit = next_exit(&runtime);
    assert_eq!(exit.session_id, sessions[2]);
    assert_eq!(
        runtime
            .pressure()
            .expect("runtime pressure")
            .active_process_entries,
        2
    );

    block_on(runtime.force_stop(&sessions[0], &processes[0].id))
        .expect("first non-target process remains independently stoppable");
    block_on(runtime.force_stop(&sessions[1], &processes[1].id))
        .expect("second non-target process remains independently stoppable");
    runtime.begin_shutdown();
    drop(runtime);
    wait_until(Duration::from_secs(5), || !socket.exists());
}

fn hosted_runtime(socket: &std::path::Path, epoch: &str) -> HostedProcessRuntime {
    hosted_runtime_with_telemetry(socket, epoch).0
}

fn hosted_runtime_with_telemetry(
    socket: &std::path::Path,
    epoch: &str,
) -> (HostedProcessRuntime, Arc<BoundedTelemetryRecorder>) {
    let native = NativeProcessRuntimeConfig::new(
        epoch,
        NativeProcessRuntimeLimits {
            max_processes: 4,
            spawn_workers: 2,
            spawn_queue_capacity: 4,
            input_queue_sequences: 8,
            input_queue_bytes_per_process: 4_096,
            output_read_bytes: 1_024,
            screen_snapshot_bytes: 256 * 1_024,
            environment_file_bytes: 4_096,
            environment_entries: 32,
            environment_bytes: 8_192,
            worker_thread_stack_bytes: 128 * 1_024,
        },
        TerminalOutputLimits {
            per_session_bytes: 64 * 1_024,
            global_bytes: 256 * 1_024,
            chunk_bytes: 1_024,
            append_bytes: 1_024,
            replay_bytes: 64 * 1_024,
            sessions: 8,
            subscriptions: 32,
        },
        Duration::from_secs(2),
    )
    .expect("native configuration");
    let hosted = HostedProcessRuntimeConfig::new(
        native,
        socket,
        HOST_EXECUTABLE,
        Duration::from_secs(5),
        Duration::from_millis(10),
        Duration::from_millis(250),
        Duration::from_secs(2),
    )
    .expect("hosted configuration");
    let telemetry = Arc::new(BoundedTelemetryRecorder::new(128, 128).expect("telemetry"));
    let runtime = HostedProcessRuntime::start(hosted, Arc::new(SystemClock), telemetry.clone())
        .expect("hosted runtime starts");
    (runtime, telemetry)
}

fn wait_for_trigger_launch(trigger: &std::path::Path) -> LaunchSpec {
    let limits = ExecutionLimits {
        max_arguments: 8,
        max_command_bytes: 4_096,
        max_environment_entries: 8,
        max_environment_bytes: 4_096,
        max_path_bytes: 4_096,
        max_health_rules: 1,
        max_health_rule_bytes: 64,
    };
    LaunchSpec::new(
        CommandSpec::new(
            "/bin/sh",
            vec![
                "-c".to_owned(),
                "printf '%s' \"$$\" > \"$1.pid\"; printf 'before-restart\\n'; while [ ! -f \"$1\" ]; do sleep 0.02; done; printf 'after-restart\\n'; exit 7".to_owned(),
                "undercurrent-test".to_owned(),
                trigger.to_string_lossy().into_owned(),
            ],
            limits,
        )
        .expect("command")
        .with_launch_mode(FIXTURE_LAUNCH_MODE),
        EnvironmentVariables::new(Vec::new(), limits).expect("environment"),
        None,
        None,
        TerminalSize::new(80, 24, 200, 100).expect("terminal size"),
    )
}

fn silent_exit_launch() -> LaunchSpec {
    let limits = ExecutionLimits {
        max_arguments: 4,
        max_command_bytes: 4_096,
        max_environment_entries: 1,
        max_environment_bytes: 1,
        max_path_bytes: 1,
        max_health_rules: 1,
        max_health_rule_bytes: 1,
    };
    LaunchSpec::new(
        CommandSpec::new(
            "/bin/sh",
            vec!["-c".to_owned(), "exit 7".to_owned()],
            limits,
        )
        .expect("command")
        .with_launch_mode(FIXTURE_LAUNCH_MODE),
        EnvironmentVariables::new(Vec::new(), limits).expect("environment"),
        None,
        None,
        TerminalSize::new(80, 24, 200, 100).expect("terminal size"),
    )
}

fn record_pid_launch(pid_file: &std::path::Path) -> LaunchSpec {
    let limits = ExecutionLimits {
        max_arguments: 8,
        max_command_bytes: 4_096,
        max_environment_entries: 8,
        max_environment_bytes: 4_096,
        max_path_bytes: 4_096,
        max_health_rules: 1,
        max_health_rule_bytes: 64,
    };
    LaunchSpec::new(
        CommandSpec::new(
            "/bin/sh",
            vec![
                "-c".to_owned(),
                "printf '%s' \"$$\" > \"$1\"; exec /bin/cat".to_owned(),
                "undercurrent-test".to_owned(),
                pid_file.to_string_lossy().into_owned(),
            ],
            limits,
        )
        .expect("command")
        .with_launch_mode(FIXTURE_LAUNCH_MODE),
        EnvironmentVariables::new(Vec::new(), limits).expect("environment"),
        None,
        None,
        TerminalSize::new(80, 24, 200, 100).expect("terminal size"),
    )
}

fn interactive_launch() -> LaunchSpec {
    let limits = ExecutionLimits {
        max_arguments: 4,
        max_command_bytes: 4_096,
        max_environment_entries: 1,
        max_environment_bytes: 1,
        max_path_bytes: 1,
        max_health_rules: 1,
        max_health_rule_bytes: 1,
    };
    LaunchSpec::new(
        CommandSpec::new(
            "/bin/sh",
            vec![
                "-c".to_owned(),
                "stty -echo; IFS= read -r line; printf 'reply:%s\\n' \"$line\"".to_owned(),
            ],
            limits,
        )
        .expect("command")
        .with_launch_mode(FIXTURE_LAUNCH_MODE),
        EnvironmentVariables::new(Vec::new(), limits).expect("environment"),
        None,
        None,
        TerminalSize::new(80, 24, 200, 100).expect("terminal size"),
    )
}

fn eof_launch() -> LaunchSpec {
    let limits = ExecutionLimits {
        max_arguments: 4,
        max_command_bytes: 4_096,
        max_environment_entries: 1,
        max_environment_bytes: 1,
        max_path_bytes: 1,
        max_health_rules: 1,
        max_health_rule_bytes: 1,
    };
    LaunchSpec::new(
        CommandSpec::new(
            "/bin/sh",
            vec![
                "-c".to_owned(),
                "stty -echo; IFS= read -r line; if IFS= read -r unexpected; then exit 9; fi; printf 'closed:%s\\n' \"$line\"".to_owned(),
            ],
            limits,
        )
        .expect("command")
        .with_launch_mode(FIXTURE_LAUNCH_MODE),
        EnvironmentVariables::new(Vec::new(), limits).expect("environment"),
        None,
        None,
        TerminalSize::new(80, 24, 200, 100).expect("terminal size"),
    )
}

fn process_is_alive(pid: u32) -> bool {
    SystemCommand::new("/bin/kill")
        .args(["-0", &pid.to_string()])
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn next_exit(runtime: &HostedProcessRuntime) -> undercurrent_application::ProcessExitObservation {
    let runtime = runtime.clone();
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let worker = thread::spawn(move || sender.send(block_on(runtime.next_exit())));
    let result = receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("exit arrives")
        .expect("exit source remains available");
    worker.join().expect("exit worker").expect("reply sends");
    result
}

fn wait_for_output(runtime: &HostedProcessRuntime, session: &SessionId, needle: &[u8]) {
    wait_until(Duration::from_secs(5), || {
        contains_bytes(&replay_bytes(runtime, session), needle)
    });
}

fn replay_bytes(runtime: &HostedProcessRuntime, session: &SessionId) -> Vec<u8> {
    let output = runtime.output_store();
    let replay = output
        .replay(
            session,
            OutputOffset::ZERO,
            64 * 1_024,
            OutputGapRecovery::RestartAtAvailableOffset,
        )
        .expect("output replay");
    let replay = match replay {
        OutputReplay::Gap { available_from, .. } => output
            .replay(
                session,
                available_from,
                64 * 1_024,
                OutputGapRecovery::RestartAtAvailableOffset,
            )
            .expect("output replay from retained offset"),
        replay => replay,
    };
    match replay {
        OutputReplay::Data { slices, .. } => slices
            .iter()
            .flat_map(|slice| slice.bytes().iter().copied())
            .collect(),
        OutputReplay::Gap { .. } => Vec::new(),
    }
}

fn spawn(
    runtime: &HostedProcessRuntime,
    session: &SessionId,
    launch: &LaunchSpec,
) -> Result<undercurrent_application::ProcessInstance, undercurrent_application::PortError> {
    let prepared = block_on(runtime.prepare_launch(launch))?;
    block_on(runtime.spawn(session, &prepared))
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !predicate() {
        assert!(Instant::now() < deadline, "condition timed out");
        thread::sleep(Duration::from_millis(10));
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
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}
