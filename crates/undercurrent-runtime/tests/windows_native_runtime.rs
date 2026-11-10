// SPDX-License-Identifier: Apache-2.0
#![cfg(windows)]
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};
use undercurrent_application::{
    ForegroundProcessObserver, OutputGapRecovery, OutputOffset, OutputReplay, PortErrorKind,
    ProcessExitSource, ProcessGroupCompleteness, ProcessRuntime, TcpListenerObservation,
    TerminalOutputLimits,
};
use undercurrent_domain::{
    CommandSpec, EnvironmentVariables, ExecutionEnvironmentBinding, ExecutionLimits,
    InputDeliveryPolicy, InputItem, InputSequence, Key, LaunchMode, LaunchSpec, PathReference,
    PayloadDigest, ProcessOutcome, SessionId, TerminalSize,
};
use undercurrent_runtime::{
    BoundedTelemetryRecorder, NativeProcessRuntime, NativeProcessRuntimeConfig,
    NativeProcessRuntimeLimits,
};

fn runtime() -> NativeProcessRuntime {
    let limits = NativeProcessRuntimeLimits {
        max_processes: 2,
        spawn_workers: 1,
        spawn_queue_capacity: 2,
        input_queue_sequences: 8,
        input_queue_bytes_per_process: 4096,
        output_read_bytes: 1024,
        screen_snapshot_bytes: 256 * 1024,
        environment_file_bytes: 4096,
        environment_entries: 32,
        environment_bytes: 8192,
        worker_thread_stack_bytes: 256 * 1024,
    };
    let output = TerminalOutputLimits {
        per_session_bytes: 65536,
        global_bytes: 262144,
        chunk_bytes: 1024,
        append_bytes: 1024,
        replay_bytes: 65536,
        sessions: 8,
        subscriptions: 32,
    };
    NativeProcessRuntime::start_with_system_clock(
        NativeProcessRuntimeConfig::new("windows-test", limits, output, Duration::from_secs(5))
            .unwrap(),
        Arc::new(BoundedTelemetryRecorder::new(64, 64).unwrap()),
    )
    .unwrap()
}
fn limits() -> ExecutionLimits {
    ExecutionLimits {
        max_arguments: 16,
        max_command_bytes: 8192,
        max_environment_entries: 32,
        max_environment_bytes: 8192,
        max_path_bytes: 4096,
        max_health_rules: 1,
        max_health_rule_bytes: 64,
    }
}
fn launch(root: &Path, args: &[&str]) -> LaunchSpec {
    LaunchSpec::new(
        CommandSpec::new(
            "cmd.exe",
            args.iter().map(|s| (*s).to_owned()).collect(),
            limits(),
        )
        .unwrap()
        .with_launch_mode(LaunchMode::Native),
        EnvironmentVariables::new(Vec::new(), limits()).unwrap(),
        None,
        None,
        TerminalSize::new(80, 24, 200, 100).unwrap(),
    )
    .with_environment_binding(Some(ExecutionEnvironmentBinding::new(
        PayloadDigest::of_bytes(b"windows-test-root"),
        PathReference::new(root.to_string_lossy(), 4096).unwrap(),
    )))
}
struct ThreadWake(std::thread::Thread);
impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}
fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => {
                assert!(Instant::now() < deadline, "operation deadline exceeded");
                std::thread::park_timeout(Duration::from_millis(10));
            }
        }
    }
}
fn wait_output(runtime: &NativeProcessRuntime, session: &SessionId, needle: &[u8]) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let output = runtime
            .output_store()
            .replay(
                session,
                OutputOffset::ZERO,
                65536,
                OutputGapRecovery::RestartAtAvailableOffset,
            )
            .unwrap();
        if let OutputReplay::Data { slices, .. } = output {
            let bytes: Vec<_> = slices
                .iter()
                .flat_map(|s| s.bytes().iter().copied())
                .collect();
            if bytes.windows(needle.len()).any(|w| w == needle) {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "terminal output deadline exceeded"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn native_cmd_launch_captures_output_and_exit() {
    let root = tempfile::tempdir().unwrap();
    let runtime = runtime();
    let launch = launch(root.path(), &["/D", "/Q", "/C", "echo WINDOWS_NATIVE_OK"]);
    let prepared = block_on(runtime.prepare_launch(&launch)).unwrap();
    assert!(Path::new(prepared.command().program()).is_absolute());
    let session = SessionId::new("windows-cmd").unwrap();
    let process = block_on(runtime.spawn(&session, &prepared)).unwrap();
    block_on(runtime.arm_exit(&session, &process.id)).unwrap();
    wait_output(&runtime, &session, b"WINDOWS_NATIVE_OK");
    assert_eq!(
        block_on(runtime.next_exit()).unwrap().outcome,
        ProcessOutcome::Exited(0)
    );
}

#[test]
fn batch_argument_fixture() {
    let Some(report) = std::env::var_os("UC_BATCH_REPORT") else {
        return;
    };
    let arguments: Vec<String> = std::env::args()
        .skip_while(|arg| arg != "--")
        .skip(1)
        .collect();
    std::fs::write(report, serde_json::to_vec(&arguments).unwrap()).unwrap();
}

#[test]
fn native_batch_shims_match_standard_library_argument_encoding() {
    let root = tempfile::tempdir().unwrap();
    let scripts = root.path().join("scripts with spaces");
    std::fs::create_dir(&scripts).unwrap();
    let executable = std::env::current_exe().unwrap();
    let args = [
        "plain",
        "two words",
        "",
        "a&echo INJECTION",
        "(group)|more",
        "%UC_BATCH_EXPAND%",
        "!UC_BATCH_EXPAND!",
        "a\"b",
        "x\" & echo INJECTION > injected.txt & rem \"",
        "trailing\\",
        "日本語",
    ];
    for extension in ["cmd", "BAT"] {
        let script = scripts.join(format!("shim.{extension}"));
        std::fs::write(
            &script,
            format!(
                "@echo off\r\n\"{}\" --exact batch_argument_fixture --nocapture -- %*\r\n",
                executable.display()
            ),
        )
        .unwrap();
        let baseline = root.path().join("baseline.json");
        let status = std::process::Command::new(&script)
            .args(args)
            .current_dir(root.path())
            .env("UC_BATCH_REPORT", &baseline)
            .env("UC_BATCH_EXPAND", "UNWANTED_EXPANSION")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        assert!(status.success());
        assert!(!root.path().join("injected.txt").exists());
        let expected = std::fs::read(&baseline).unwrap();
        let report = root.path().join(format!("conpty-{extension}.json"));
        let runtime = runtime();
        let spec = launch(root.path(), &args)
            .with_command(
                CommandSpec::new(
                    script.to_string_lossy(),
                    args.iter().map(|s| s.to_string()).collect(),
                    limits(),
                )
                .unwrap()
                .with_launch_mode(LaunchMode::Native),
            )
            .with_resolved_environment(
                EnvironmentVariables::new(
                    vec![
                        (
                            "UC_BATCH_REPORT".into(),
                            report.to_string_lossy().into_owned(),
                        ),
                        ("UC_BATCH_EXPAND".into(), "UNWANTED_EXPANSION".into()),
                        ("PATH".into(), scripts.to_string_lossy().into_owned()),
                        // An untrusted COMSPEC must not select the batch interpreter.
                        (
                            "COMSPEC".into(),
                            root.path()
                                .join("missing.exe")
                                .to_string_lossy()
                                .into_owned(),
                        ),
                    ],
                    limits(),
                )
                .unwrap(),
                limits(),
            )
            .unwrap();
        let prepared = block_on(runtime.prepare_launch(&spec)).unwrap();
        assert_eq!(prepared.command().program(), script.to_str().unwrap());
        let by_name = spec.clone().with_command(
            CommandSpec::new("shim", Vec::new(), limits())
                .unwrap()
                .with_launch_mode(LaunchMode::Native),
        );
        assert_eq!(
            block_on(runtime.prepare_launch(&by_name))
                .unwrap()
                .command()
                .program(),
            scripts.join("shim.cmd").to_str().unwrap()
        );
        let session = SessionId::new(format!("batch-{extension}")).unwrap();
        let process = block_on(runtime.spawn(&session, &prepared)).unwrap();
        block_on(runtime.arm_exit(&session, &process.id)).unwrap();
        assert_eq!(
            block_on(runtime.next_exit()).unwrap().outcome,
            ProcessOutcome::Exited(0)
        );
        assert_eq!(std::fs::read(&report).unwrap(), expected);
        assert!(!root.path().join("injected.txt").exists());
        let percent_script = scripts.join("shim %UC_BATCH_EXPAND%.cmd");
        std::fs::copy(&script, &percent_script).unwrap();
        let percent_spec = spec.clone().with_command(
            CommandSpec::new(
                std::fs::canonicalize(&percent_script)
                    .unwrap()
                    .to_string_lossy(),
                args.iter().map(|s| s.to_string()).collect(),
                limits(),
            )
            .unwrap()
            .with_launch_mode(LaunchMode::Native),
        );
        let prepared = block_on(runtime.prepare_launch(&percent_spec)).unwrap();
        std::fs::remove_file(&report).unwrap();
        let process = block_on(runtime.spawn(&session, &prepared)).unwrap();
        block_on(runtime.arm_exit(&session, &process.id)).unwrap();
        assert_eq!(
            block_on(runtime.next_exit()).unwrap().outcome,
            ProcessOutcome::Exited(0)
        );
        assert_eq!(std::fs::read(&report).unwrap(), expected);
        for invalid in ["line\nbreak", "line\rbreak", &"x".repeat(8192)] {
            let spec = spec.clone().with_command(
                CommandSpec::new(
                    script.to_string_lossy(),
                    vec![invalid.into()],
                    ExecutionLimits {
                        max_command_bytes: 16384,
                        ..limits()
                    },
                )
                .unwrap()
                .with_launch_mode(LaunchMode::Native),
            );
            assert!(block_on(runtime.prepare_launch(&spec)).is_err());
        }
    }
}

#[test]
fn batch_verbatim_conversion_rejects_a_different_ordinary_file() {
    let root = tempfile::tempdir().unwrap();
    let ordinary = root.path().join("alias");
    let verbatim = std::path::PathBuf::from(format!(r"\\?\{}\alias.", root.path().display()));
    std::fs::create_dir(&ordinary).unwrap();
    std::fs::create_dir(&verbatim).unwrap();
    std::fs::write(ordinary.join("run.cmd"), b"@echo WRONG_FILE\r\n").unwrap();
    std::fs::write(verbatim.join("run.cmd"), b"@echo ACCEPTED_FILE\r\n").unwrap();
    let spec = launch(root.path(), &[]).with_command(
        CommandSpec::new(
            verbatim.join("run.cmd").to_string_lossy(),
            Vec::new(),
            limits(),
        )
        .unwrap()
        .with_launch_mode(LaunchMode::Native),
    );
    assert!(block_on(runtime().prepare_launch(&spec)).is_err());
    // tempdir's ordinary cleanup cannot address verbatim-only trailing dots.
    std::fs::remove_file(verbatim.join("run.cmd")).unwrap();
    std::fs::remove_dir(verbatim).unwrap();
}

#[test]
fn native_cmd_accepts_input_resize_and_force_stop() {
    let root = tempfile::tempdir().unwrap();
    let runtime = runtime();
    let prepared =
        block_on(runtime.prepare_launch(&launch(root.path(), &["/D", "/Q", "/K"]))).unwrap();
    let session = SessionId::new("windows-interactive").unwrap();
    let process = block_on(runtime.spawn(&session, &prepared)).unwrap();
    block_on(runtime.arm_exit(&session, &process.id)).unwrap();
    let observation = block_on(runtime.observe_foreground(&session, &process.id)).unwrap();
    assert_eq!(observation.process(), &process.id);
    assert!(observation.members().is_empty());
    assert_eq!(
        observation.completeness(),
        ProcessGroupCompleteness::Unavailable
    );
    assert_eq!(
        observation.tcp_listener(),
        TcpListenerObservation::Unavailable
    );
    assert!(observation.current_directory().is_none());
    block_on(runtime.resize(
        &session,
        &process.id,
        TerminalSize::new(100, 30, 200, 100).unwrap(),
    ))
    .unwrap();
    let input = InputSequence::new(
        vec![
            InputItem::Text("echo WINDOWS_INPUT_OK".into()),
            InputItem::Key(Key::Enter),
        ],
        2,
        256,
    )
    .unwrap();
    block_on(runtime.write_input(
        &session,
        &process.id,
        &input,
        InputDeliveryPolicy::immediate(),
    ))
    .unwrap();
    wait_output(&runtime, &session, b"WINDOWS_INPUT_OK");
    block_on(runtime.force_stop(&session, &process.id)).unwrap();
    assert_eq!(runtime.pressure().unwrap().occupied_process_slots, 0);
    assert_eq!(
        block_on(runtime.observe_foreground(&session, &process.id))
            .unwrap_err()
            .kind(),
        PortErrorKind::NotFound
    );
}

#[test]
fn sealed_requests_and_replaced_working_directories_fail_before_spawn() {
    let root = tempfile::tempdir().unwrap();
    let runtime = runtime();
    let launch = launch(root.path(), &["/D", "/C", "exit 0"]);
    let sealed = launch.clone().with_command(
        launch
            .command()
            .clone()
            .with_launch_mode(LaunchMode::Sealed),
    );
    assert_eq!(
        block_on(runtime.prepare_launch(&sealed))
            .unwrap_err()
            .kind(),
        PortErrorKind::Unsupported
    );
    let cwd = root.path().join("cwd");
    std::fs::create_dir(&cwd).unwrap();
    let binding = launch.environment_binding().cloned();
    let launch = launch.with_resolved_paths(
        Some(PathReference::new(cwd.to_string_lossy(), 4096).unwrap()),
        binding,
    );
    let prepared = block_on(runtime.prepare_launch(&launch)).unwrap();
    std::fs::rename(&cwd, root.path().join("old")).unwrap();
    std::fs::create_dir(&cwd).unwrap();
    let session = SessionId::new("windows-stale-cwd").unwrap();
    assert!(block_on(runtime.spawn(&session, &prepared)).is_err());
}

#[test]
fn batch_child_listener_is_observed_without_foreground_membership() {
    let root = tempfile::tempdir().unwrap();
    let runtime = runtime();
    let script = root.path().join("listener.cmd");
    // A Store-packaged child may be brokered outside the parent's job. This
    // regression qualifies ordinary, kernel-confirmed job members.
    let powershell = undercurrent_platform_windows::command_interpreter()
        .unwrap()
        .parent()
        .unwrap()
        .join("WindowsPowerShell\\v1.0\\powershell.exe");
    std::fs::write(&script, format!("@echo off\r\n\"{}\" -NoProfile -Command \"$l=[Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,0); $l.Start(); Write-Output TCP_CHILD_READY; [Console]::ReadLine(); $l.Stop(); Write-Output TCP_CHILD_CLOSED; [Console]::ReadLine()\"\r\n", powershell.to_string_lossy().replace('%', "%%"))).unwrap();
    let spec = launch(root.path(), &[]).with_command(
        CommandSpec::new(script.to_string_lossy(), Vec::new(), limits())
            .unwrap()
            .with_launch_mode(LaunchMode::Native),
    );
    let prepared = block_on(runtime.prepare_launch(&spec)).unwrap();
    let session = SessionId::new("windows-child-listener").unwrap();
    let process = block_on(runtime.spawn(&session, &prepared)).unwrap();
    wait_output(&runtime, &session, b"TCP_CHILD_READY");
    let observed = block_on(runtime.observe_foreground(&session, &process.id)).unwrap();
    assert_eq!(
        observed.managed_tree_tcp_listener(),
        TcpListenerObservation::Present,
        "{observed:?}"
    );
    assert_eq!(
        observed.managed_process_tcp_listener(),
        TcpListenerObservation::Absent
    );
    assert_eq!(
        observed.completeness(),
        ProcessGroupCompleteness::Unavailable
    );
    block_on(runtime.force_stop(&session, &process.id)).unwrap();
}

#[test]
fn bounded_stop_escalates_but_graceful_only_leaves_the_process_running() {
    use undercurrent_domain::{StopPolicy, StopSignal};
    let root = tempfile::tempdir().unwrap();
    let runtime = runtime();
    let prepared =
        block_on(runtime.prepare_launch(&launch(root.path(), &["/D", "/Q", "/K"]))).unwrap();
    let session = SessionId::new("windows-bounded-stop").unwrap();
    let process = block_on(runtime.spawn(&session, &prepared)).unwrap();
    block_on(runtime.arm_exit(&session, &process.id)).unwrap();
    let policy = StopPolicy::new(StopSignal::Terminate, Duration::from_millis(50)).unwrap();
    assert!(
        block_on(runtime.stop(&session, &process.id, policy.with_no_escalation(true))).is_err()
    );
    assert_eq!(runtime.pressure().unwrap().occupied_process_slots, 1);
    let started = Instant::now();
    let outcome = block_on(runtime.stop(&session, &process.id, policy)).unwrap();
    assert!(started.elapsed() >= Duration::from_millis(50));
    assert_eq!(
        outcome.kind(),
        undercurrent_application::TerminationKind::ForcedAfterTimeout
    );
    assert_eq!(runtime.pressure().unwrap().occupied_process_slots, 0);
}

#[test]
fn explicit_windows_environment_overrides_dotenv_without_case_duplicates() {
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("environment");
    std::fs::write(&file, "My_Variable=from-file\n").unwrap();
    let base = launch(root.path(), &["/D", "/C", "exit 0"]);
    let launch = LaunchSpec::new(
        base.command().clone(),
        EnvironmentVariables::new(
            [("MY_VARIABLE".to_owned(), "explicit".to_owned())],
            limits(),
        )
        .unwrap(),
        Some(PathReference::new(file.to_string_lossy(), 4096).unwrap()),
        None,
        TerminalSize::new(80, 24, 200, 100).unwrap(),
    )
    .with_environment_binding(base.environment_binding().cloned());
    let runtime = runtime();
    let prepared = block_on(runtime.prepare_launch(&launch)).unwrap();
    let values: Vec<_> = prepared
        .environment()
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("MY_VARIABLE"))
        .collect();
    assert_eq!(values, vec![("MY_VARIABLE", "explicit")]);
}

#[test]
fn native_restart_uses_accepted_path_and_allows_installed_updates() {
    let root = tempfile::tempdir().unwrap();
    let installed = root.path().join("installed.exe");
    let system =
        std::path::PathBuf::from(std::env::var_os("SystemRoot").unwrap()).join("System32/cmd.exe");
    std::fs::copy(&system, &installed).unwrap();
    let base = launch(root.path(), &["/D", "/C", "exit 0"]);
    let command = CommandSpec::new(
        installed.to_string_lossy(),
        vec!["/D".into(), "/C".into(), "exit 0".into()],
        limits(),
    )
    .unwrap()
    .with_launch_mode(LaunchMode::Native);
    let runtime = runtime();
    let prepared = block_on(runtime.prepare_launch(&base.with_command(command))).unwrap();
    std::fs::rename(&installed, root.path().join("previous.exe")).unwrap();
    std::fs::copy(&system, &installed).unwrap();
    let session = SessionId::new("windows-updated-native").unwrap();
    let process = block_on(runtime.spawn(&session, &prepared)).unwrap();
    block_on(runtime.arm_exit(&session, &process.id)).unwrap();
    assert_eq!(
        block_on(runtime.next_exit()).unwrap().outcome,
        ProcessOutcome::Exited(0)
    );
}
