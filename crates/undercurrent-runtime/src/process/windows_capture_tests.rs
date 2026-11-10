// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::process::host_files;
use crate::process::windows_test_support::{config, launch, wait_until};
use crate::process_host_protocol::{HostExecutionTerminalKind, HostLaunchSpec};
use std::ffi::OsString;
use std::fs;
use std::os::windows::ffi::OsStringExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Instant;

const ROOT_TEST: &str =
    "process::host::shell_environment::native_windows_tests::capture_root_subprocess";
const HELPER_TEST: &str =
    "process::host::shell_environment::native_windows_tests::capture_helper_subprocess";

struct HostClient {
    child: Child,
    stream: UnixStream,
    codec: HostCodec,
    sequence: u64,
    socket: PathBuf,
    roots: Vec<undercurrent_platform_windows::ProcessHandle>,
}
impl HostClient {
    fn start(root: &Path) -> Self {
        let executable = std::env::var_os("UC_WINDOWS_TEST_HOST").expect(
            "build CLI/daemon --bins, then set UC_WINDOWS_TEST_HOST to uc-session-host.exe",
        );
        let socket = root.join("capture-host");
        let bootstrap = ProcessHostBootstrap::new(&socket, "11".repeat(32), &config(4)).unwrap();
        let mut child = Command::new(executable)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        bootstrap.write_to(child.stdin.take().unwrap()).unwrap();
        let mut connected = None;
        wait_until(|| {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("native host exited during setup: {status}");
            }
            connected = UnixStream::connect(&socket).ok();
            connected.is_some()
        });
        let stream = connected.unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut client = Self {
            child,
            stream,
            codec: HostCodec::new(2 * 1024 * 1024).unwrap(),
            sequence: 0,
            socket,
            roots: Vec::new(),
        };
        assert!(matches!(
            client
                .request(HostCommand::Authenticate {
                    token: "11".repeat(32)
                })
                .unwrap(),
            HostResponseValue::Authenticated { .. }
        ));
        client
    }
    fn request(&mut self, command: HostCommand) -> Result<HostResponseValue, HostFailure> {
        self.sequence += 1;
        let request_id = self.sequence;
        let encoded = self
            .codec
            .encode_request(&HostRequest {
                request_id,
                command,
            })
            .unwrap();
        self.stream.write_all(&encoded).unwrap();
        loop {
            if let HostDecodedFrame::Response(response) =
                read_frame(&mut self.stream, self.codec).unwrap()
                && response.request_id == request_id
            {
                return response.result;
            }
        }
    }
    fn spawn_root(&mut self, root: &Path, name: &str, value: &str) -> Shell {
        let directory = root.join(name);
        fs::create_dir(&directory).unwrap();
        let spec = launch(
            &directory,
            &std::env::current_exe().unwrap().to_string_lossy(),
            vec!["--exact".into(), ROOT_TEST.into(), "--nocapture".into()],
            vec![
                (
                    "UC_CAPTURE_TEST_ROOT".into(),
                    directory.to_string_lossy().into_owned(),
                ),
                ("UC_CAPTURE_TEST_VALUE".into(), value.into()),
            ],
        );
        // Host Spawn expects a prepared native launch, just like its normal caller.
        let preparer = crate::NativeProcessRuntime::start_with_system_clock(
            config(1),
            std::sync::Arc::new(crate::BoundedTelemetryRecorder::new(32, 32).unwrap()),
        )
        .unwrap();
        use undercurrent_application::ProcessRuntime;
        let spec =
            crate::process::windows_test_support::block_on(preparer.prepare_launch(&spec)).unwrap();
        let spawn_token = format!("{:064x}", self.sequence + 1);
        let response = self
            .request(HostCommand::Spawn {
                spawn_token: spawn_token.clone(),
                session_id: name.into(),
                launch: Box::new(HostLaunchSpec::from(&spec)),
                secret_environment: HostResolvedSecretEnvironment::from_values(Vec::new()),
            })
            .unwrap();
        let HostResponseValue::Spawned {
            process_id,
            root_pid,
            ..
        } = response
        else {
            panic!("root did not spawn")
        };
        self.roots
            .push(undercurrent_platform_windows::ProcessHandle::open(root_pid).unwrap());
        self.request(HostCommand::AdoptSpawn { spawn_token })
            .unwrap();
        Shell {
            session: name.into(),
            process: process_id,
            pid: root_pid,
            directory,
            sequence: 0,
            value: value.into(),
        }
    }
    fn timestamp(&mut self, shell: &Shell) -> Option<i64> {
        match self
            .request(HostCommand::ShellEnvironmentStatus {
                session_id: shell.session.clone(),
                process_id: shell.process.clone(),
            })
            .unwrap()
        {
            HostResponseValue::ShellEnvironmentStatus { observed_at_millis } => observed_at_millis,
            _ => panic!("unexpected snapshot response"),
        }
    }
    fn directory(&mut self, shell: &Shell) -> Option<String> {
        match self
            .request(HostCommand::ObserveForeground {
                session_id: shell.session.clone(),
                process_id: shell.process.clone(),
            })
            .unwrap()
        {
            HostResponseValue::ForegroundContext { context } => context.current_directory,
            _ => panic!("unexpected directory response"),
        }
    }
    fn check_execution(&mut self, shell: &Shell, captured: bool, live: bool) {
        let directory = if live {
            self.directory(shell)
        } else {
            Some(shell.directory.to_string_lossy().into_owned())
        };
        if live {
            assert_eq!(directory.is_some(), captured);
        }
        let job_id = format!("capture-job-{}", self.sequence + 1);
        let result = self.request(HostCommand::Execute {
            live_environment_process: live.then(|| shell.process.clone()),
            job_id: job_id.clone(),
            session_id: shell.session.clone(),
            payload_digest: "ab".repeat(32),
            program: undercurrent_platform_windows::command_interpreter()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
            arguments: vec![
                "/D".into(),
                "/C".into(),
                "echo %UC_NATIVE_CAPTURE_VALUE% & cd".into(),
            ],
            environment: vec![("UC_NATIVE_CAPTURE_VALUE".into(), "launch-value".into())],
            working_directory: directory,
            output_bytes: 4096,
            deadline_millis: None,
        });
        if live && !captured {
            assert!(
                matches!(
                    result,
                    Err(HostFailure {
                        kind: HostFailureKind::Unavailable,
                        ..
                    })
                ),
                "stale live capture remained usable"
            );
            return;
        }
        assert!(matches!(
            result.unwrap(),
            HostResponseValue::ExecutionAccepted
        ));
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match self
                .request(HostCommand::PollExecution {
                    job_id: job_id.clone(),
                    wait_millis: 25,
                })
                .unwrap()
            {
                HostResponseValue::ExecutionRunning => {
                    assert!(Instant::now() < deadline, "execution timed out")
                }
                HostResponseValue::ExecutionTerminal {
                    kind: HostExecutionTerminalKind::Completed,
                    exit_code: Some(0),
                    stdout,
                    ..
                } => {
                    let output = String::from_utf8(stdout).unwrap();
                    assert!(output.contains(if live { &shell.value } else { "launch-value" }));
                    assert!(output.contains(shell.directory.to_str().unwrap()));
                    break;
                }
                result => panic!("unexpected execution result: {result:?}"),
            }
        }
        self.request(HostCommand::AcknowledgeExecution { job_id })
            .unwrap();
    }
}
impl Drop for HostClient {
    fn drop(&mut self) {
        // Closing the real host also closes its jobs, even if an assertion failed.
        let _ = self.child.kill();
        let _ = self.child.wait();
        let deadline = Instant::now() + Duration::from_secs(5);
        while self
            .roots
            .iter()
            .any(|root| root.is_alive().unwrap_or(true))
        {
            if Instant::now() >= deadline {
                if !thread::panicking() {
                    panic!("capture fixture root survived host cleanup");
                }
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

struct Shell {
    session: String,
    process: String,
    pid: u32,
    directory: PathBuf,
    sequence: u64,
    value: String,
}
impl Shell {
    fn capture(&mut self, mode: &str) -> serde_json::Value {
        self.sequence += 1;
        let pending = self.directory.join("request-next");
        fs::write(&pending, format!("{} {mode}", self.sequence)).unwrap();
        // The root polls a single file; complete publication avoids partial commands.
        fs::rename(pending, self.directory.join("request")).unwrap();
        let completed = self.directory.join(format!("done-{}", self.sequence));
        let mut result = None;
        wait_until(|| {
            result = fs::read(&completed)
                .ok()
                .and_then(|bytes| serde_json::from_slice(&bytes).ok());
            result.is_some()
        });
        result.unwrap()
    }
}

#[test]
#[ignore = "requires matching native uc-session-host.exe via UC_WINDOWS_TEST_HOST; see COMMENT_REVIEW_WINDOWS_MACOS.md"]
fn authenticated_native_capture_failures_preserve_snapshot_isolation() {
    let directory = tempfile::tempdir().unwrap();
    // Let the host create its private directory with the exact account owner.
    // An elevated tempfile root can instead belong to Administrators.
    let root = directory.path().join("capture");
    let mut host = HostClient::start(&root);
    let mut first = host.spawn_root(&root, "first-shell", "private-original");
    let mut other = host.spawn_root(&root, "other-shell", "private-other");
    let captured = other.capture("good");
    assert_eq!(captured["ok"], true, "{captured}");
    let other_at = host.timestamp(&other).expect("other snapshot captured");
    let authentication = ProcessHostBootstrap::authentication_path(&host.socket);
    let token = fs::read(&authentication).unwrap();
    let unavailable = first.directory.join("unavailable-host");
    host_files::create_new(&ProcessHostBootstrap::authentication_path(&unavailable))
        .unwrap()
        .write_all(&token)
        .unwrap();
    for mode in [
        "non-unicode",
        "directory-unicode",
        "directory-error",
        "invalid-name",
        "entry-overflow",
        "byte-overflow",
        "bad-auth",
        "transport",
        "bad-peer",
    ] {
        assert_eq!(first.capture("good")["ok"], true);
        let old = host.timestamp(&first).expect("valid snapshot captured");
        host.check_execution(&first, true, true);
        if mode == "bad-auth" {
            host_files::write_truncated(&authentication)
                .unwrap()
                .write_all("22".repeat(32).as_bytes())
                .unwrap();
        }
        let requested = if mode == "bad-peer" {
            format!("bad-peer:{}", other.pid)
        } else {
            mode.to_owned()
        };
        let result = first.capture(&requested);
        if mode == "bad-auth" {
            host_files::write_truncated(&authentication)
                .unwrap()
                .write_all(&token)
                .unwrap();
        }
        assert_eq!(result["ok"], false, "{mode}: {result}");
        let delivered = !matches!(mode, "bad-auth" | "transport" | "bad-peer");
        if delivered {
            assert_eq!(result["kind"], "InvalidData", "{mode}: {result}");
            assert_eq!(
                result["message"],
                "shell environment capture failed; previous snapshot invalidated"
            );
            assert_eq!(host.timestamp(&first), None, "{mode}");
        } else {
            assert_eq!(result["message"], "shell environment capture unavailable");
            assert_eq!(
                host.timestamp(&first),
                Some(old),
                "{mode}: timestamp changed without delivery"
            );
        }
        assert_eq!(
            host.timestamp(&other),
            Some(other_at),
            "{mode}: unrelated snapshot changed"
        );
        host.check_execution(&first, !delivered, true);
        host.check_execution(&first, !delivered, false);
        host.check_execution(&other, true, true);
        println!(
            "{mode}: authenticated delivery={delivered}, exact snapshot/other timestamp/live and launch bindings verified"
        );
    }
}

#[test]
fn capture_root_subprocess() {
    let Some(root) = std::env::var_os("UC_CAPTURE_TEST_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let mut sequence = 0;
    let deadline = Instant::now() + Duration::from_secs(120);
    while Instant::now() < deadline {
        let command = fs::read_to_string(root.join("request")).unwrap_or_default();
        let Some((number, mode)) = command.split_once(' ') else {
            thread::sleep(Duration::from_millis(5));
            continue;
        };
        let Ok(number) = number.parse::<u64>() else {
            continue;
        };
        if number <= sequence {
            thread::sleep(Duration::from_millis(5));
            continue;
        }
        sequence = number;
        let result_path = root.join("helper-result");
        let _ = fs::remove_file(&result_path);
        let mut helper = Command::new(std::env::current_exe().unwrap());
        helper
            .args(["--exact", HELPER_TEST, "--nocapture"])
            .env("UC_CAPTURE_HELPER_MODE", mode)
            .env("UC_CAPTURE_RESULT", &result_path)
            .env("UC_CAPTURE_SHELL_PID", std::process::id().to_string())
            .env(
                "UC_NATIVE_CAPTURE_VALUE",
                if mode == "good" {
                    std::env::var("UC_CAPTURE_TEST_VALUE").unwrap()
                } else {
                    "private-rejected".into()
                },
            )
            .current_dir(&root)
            .stdin(Stdio::null());
        match mode {
            "non-unicode" => {
                helper.env("UC_NATIVE_BAD", OsString::from_wide(&[0xd800]));
            }
            "directory-unicode" => {
                let directory = root.join(OsString::from_wide(&[b'd' as u16, 0xd800]));
                fs::create_dir_all(&directory).unwrap();
                helper.current_dir(directory);
            }
            "entry-overflow" => {
                helper.env("UC_SHELL_ENV_ENTRIES", "1");
            }
            "byte-overflow" => {
                helper.env("UC_SHELL_ENV_BYTES", "1");
            }
            "transport" => {
                helper.env("UC_SHELL_HOST", root.join("unavailable-host"));
            }
            _ => {
                if let Some(pid) = mode.strip_prefix("bad-peer:") {
                    helper.env("UC_CAPTURE_SHELL_PID", pid);
                }
            }
        }
        let output = helper.output().unwrap();
        assert!(output.status.success(), "capture helper failed to run");
        for bytes in [&output.stdout, &output.stderr] {
            assert!(
                !String::from_utf8_lossy(bytes).contains("private-"),
                "helper leaked a captured value"
            );
        }
        let result = fs::read(&result_path).unwrap();
        fs::write(root.join(format!("done-{sequence}")), result).unwrap();
    }
}

#[test]
fn capture_helper_subprocess() {
    let Ok(mode) = std::env::var("UC_CAPTURE_HELPER_MODE") else {
        return;
    };
    let result = match mode.as_str() {
        "good" => capture_shell_environment_with(
            || std::env::vars_os().chain([(OsString::from("=C:"), OsString::from(r"C:\fixture"))]),
            std::env::current_dir,
        ),
        "directory-error" => capture_shell_environment_with(std::env::vars_os, || {
            Err(std::io::Error::other(
                "injected directory acquisition failure",
            ))
        }),
        "invalid-name" => capture_shell_environment_with(
            || {
                std::env::vars_os().chain([(
                    OsString::from("BAD=NAME"),
                    OsString::from("private-rejected"),
                )])
            },
            std::env::current_dir,
        ),
        _ => capture_shell_environment(),
    };
    let result = match result {
        Ok(()) => serde_json::json!({"ok": true}),
        Err(error) => {
            serde_json::json!({"ok": false, "kind": format!("{:?}", error.kind()), "message": error.to_string()})
        }
    };
    fs::write(
        std::env::var_os("UC_CAPTURE_RESULT").unwrap(),
        serde_json::to_vec(&result).unwrap(),
    )
    .unwrap();
}
