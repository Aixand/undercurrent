// SPDX-License-Identifier: Apache-2.0
use super::*;

#[test]
fn supervisor_subprocess() {
    if std::env::var("UNDERCURRENT_EXECUTION_SUPERVISOR_TEST").as_deref() != Ok("1") {
        return;
    }
    match run_execution_supervisor(io::stdin().lock()) {
        Ok(outcome) => outcome.terminate(),
        Err(_) => std::process::exit(1),
    }
}

#[test]
fn host_parent_subprocess() {
    let Some(report) = std::env::var_os("UC_WINDOWS_HOST_CRASH_REPORT") else {
        return;
    };
    let report = PathBuf::from(report);
    let registry = registry(&report.with_extension("jobs"));
    let mut request = spec("host-crash", "");
    request.program = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
        .join("System32/WindowsPowerShell/v1.0/powershell.exe")
        .to_string_lossy()
        .into_owned();
    request.arguments = vec!["-NoProfile".into(), "-NonInteractive".into(), "-Command".into(),
        r#"$child = Start-Process -FilePath ($env:SystemRoot + '/System32/ping.exe') -ArgumentList '-n','60','127.0.0.1' -WindowStyle Hidden -PassThru; [IO.File]::WriteAllText($env:UC_WINDOWS_HOST_CRASH_REPORT, "$PID $($child.Id)"); Start-Sleep -Seconds 60"#.into()];
    registry.start(request).unwrap();
    thread::sleep(Duration::from_secs(60));
    registry.cancel_all_and_wait(Duration::from_secs(5));
}

#[test]
fn host_process_death_kills_workload_and_descendant_and_recovers_unknown() {
    let root = tempfile::tempdir().unwrap();
    let report = root.path().join("processes");
    // A plain process is intentional: the test runner must not own a job that
    // could accidentally make the host's own crash supervision appear to work.
    let mut host = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "process::execute_host::windows_tests::host_parent_subprocess",
            "--nocapture",
        ])
        .env("UC_WINDOWS_HOST_CRASH_REPORT", &report)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    let identities = loop {
        if let Ok(text) = fs::read_to_string(&report) {
            let pids: Vec<u32> = text
                .split_whitespace()
                .filter_map(|s| s.parse().ok())
                .collect();
            if pids.len() == 2 {
                break pids
                    .into_iter()
                    .map(|pid| undercurrent_platform_windows::ProcessHandle::open(pid).unwrap())
                    .collect::<Vec<_>>();
            }
        }
        if Instant::now() >= deadline {
            let _ = host.kill();
            let _ = host.wait();
            panic!("workload did not publish identities");
        }
        thread::sleep(Duration::from_millis(10));
    };
    assert!(identities.iter().all(|p| p.is_alive().unwrap()));
    host.kill().unwrap();
    host.wait().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while identities.iter().any(|p| p.is_alive().unwrap()) {
        assert!(Instant::now() < deadline, "descendant survived host crash");
        thread::sleep(Duration::from_millis(10));
    }
    let recovered = registry(&report.with_extension("jobs"));
    assert!(matches!(
        terminal(&recovered, "host-crash"),
        HostResponseValue::ExecutionFailed {
            effect_started: true,
            ..
        }
    ));
}

fn registry(path: &Path) -> HostExecutionRegistry {
    HostExecutionRegistry::new_with_launcher(
        ExecutionRegistryConfig {
            limit: 4,
            command_bytes_limit: 4096,
            environment_entries_limit: 256,
            environment_bytes_limit: 32768,
            capture_bytes_limit: 4096,
            worker_stack_bytes: 512 * 1024,
            journal_directory: path.to_owned(),
        },
        ExecutionLauncher::Supervisor {
            executable: std::env::current_exe().unwrap(),
            arguments: vec![
                "--exact".into(),
                "process::execute_host::windows_tests::supervisor_subprocess".into(),
                "--nocapture".into(),
            ],
            test_helper: true,
        },
    )
    .unwrap()
}

fn spec(id: &str, script: &str) -> HostExecutionSpec {
    HostExecutionSpec {
        live_environment: None,
        job_id: id.into(),
        session_id: "windows-session".into(),
        payload_digest: "ab".repeat(32),
        program: std::env::var("COMSPEC").unwrap(),
        arguments: vec!["/D".into(), "/S".into(), "/C".into(), script.into()],
        // Windows shells expose hidden drive entries such as `=C:` through
        // std::env. These are not valid explicit process-environment keys.
        environment: std::env::vars()
            .filter(|(name, _)| !name.starts_with('='))
            .collect(),
        working_directory: None,
        output_bytes: 4096,
        deadline_millis: None,
    }
}

fn terminal(registry: &HostExecutionRegistry, id: &str) -> HostResponseValue {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let response = registry.poll(id, Duration::from_millis(25)).unwrap();
        if !matches!(response, HostResponseValue::ExecutionRunning) {
            return response;
        }
        assert!(Instant::now() < deadline, "execution did not settle");
    }
}

#[test]
fn native_execution_runs_batch_shims_with_spaces() {
    let root = tempfile::tempdir().unwrap();
    let script = root.path().join("one shot.bat");
    fs::write(&script, b"@echo off\r\necho BATCH_EXEC_%~1\r\n").unwrap();
    let registry = registry(&root.path().join("journal"));
    let mut request = spec("batch", "");
    request.program = script.to_string_lossy().into_owned();
    request.arguments = vec!["with spaces".into()];
    request.environment.clear();
    request.working_directory = Some(root.path().to_string_lossy().into_owned());
    registry.start(request).unwrap();
    let response = terminal(&registry, "batch");
    assert!(
        matches!(response, HostResponseValue::ExecutionTerminal { kind: HostExecutionTerminalKind::Completed, exit_code: Some(0), ref stdout, .. } if stdout == b"BATCH_EXEC_with spaces\r\n"),
        "{response:?}"
    );
}

#[test]
fn native_execution_preserves_output_status_and_replays_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("journal");
    let first = registry(&path);
    first
        .start(spec("replay", "echo persisted & exit /b 17"))
        .unwrap();
    let response = terminal(&first, "replay");
    assert!(
        matches!(response, HostResponseValue::ExecutionTerminal {
        kind: HostExecutionTerminalKind::Completed, exit_code: Some(17), ref stdout, ..
    } if stdout == b"persisted \r\n"),
        "{response:?}"
    );
    drop(first);
    let second = registry(&path);
    assert!(matches!(
        terminal(&second, "replay"),
        HostResponseValue::ExecutionTerminal {
            exit_code: Some(17),
            ..
        }
    ));
    second.acknowledge("replay").unwrap();
    assert!(!path.join("job-replay.json").exists());
}

#[test]
fn native_execution_cancellation_deadline_and_output_bound_settle() {
    let root = tempfile::tempdir().unwrap();
    let registry = registry(&root.path().join("journal"));
    registry
        .start(spec("cancel", "ping -n 30 127.0.0.1 >nul"))
        .unwrap();
    // Wait for readiness rather than cancelling before the command can start.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let started = registry
            .jobs
            .lock()
            .unwrap()
            .get("cancel")
            .unwrap()
            .workload_pid
            .load(Ordering::Acquire)
            != 0;
        if started {
            break;
        }
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(5));
    }
    registry.cancel("cancel").unwrap();
    assert!(matches!(
        terminal(&registry, "cancel"),
        HostResponseValue::ExecutionTerminal {
            kind: HostExecutionTerminalKind::Cancelled,
            ..
        }
    ));
    let mut request = spec("deadline", "ping -n 30 127.0.0.1 >nul");
    request.deadline_millis = Some(wall_clock_millis() + 1000);
    registry.start(request).unwrap();
    assert!(matches!(
        terminal(&registry, "deadline"),
        HostResponseValue::ExecutionTerminal {
            kind: HostExecutionTerminalKind::DeadlineExceeded,
            ..
        }
    ));
    let mut request = spec(
        "overflow",
        "for /L %i in (1,1,10000) do @echo 0123456789abcdef",
    );
    request.output_bytes = 64;
    registry.start(request).unwrap();
    assert!(
        matches!(terminal(&registry, "overflow"), HostResponseValue::ExecutionTerminal { kind: HostExecutionTerminalKind::OutputLimitExceeded, stdout, .. } if stdout.len() <= 64)
    );
}
