// SPDX-License-Identifier: Apache-2.0
use super::super::windows_test_support::{block_on, config, launch, wait_until};
use super::*;
use portable_pty::{ChildKiller, ExitStatus};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use undercurrent_application::ProcessRuntime;
use undercurrent_platform_windows::ProcessHandle;

struct Injection {
    report: PathBuf,
    fail_wait: bool,
    processes: Arc<Mutex<Vec<ProcessHandle>>>,
    events: Arc<Mutex<Vec<&'static str>>>,
}
// Match a single session and consume its injection once, so parallel unrelated
// tests and the subsequent capacity-reuse launch use the normal reaper.
static INJECTIONS: LazyLock<Mutex<BTreeMap<SessionId, Injection>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

pub(in crate::process) fn needs_output(session: &SessionId) -> bool {
    INJECTIONS.lock().unwrap().contains_key(session)
}

pub(super) fn inject(
    session: &SessionId,
    child: Box<dyn Child + Send + Sync>,
) -> (Box<dyn Child + Send + Sync>, bool) {
    let Some(injection) = INJECTIONS.lock().unwrap().remove(session) else {
        return (child, false);
    };
    wait_until(|| {
        std::fs::read_to_string(&injection.report).is_ok_and(|s| s.parse::<u32>().is_ok())
    });
    let descendant = std::fs::read_to_string(&injection.report)
        .unwrap()
        .parse()
        .unwrap();
    let root = ProcessHandle::open(child.process_id().unwrap()).unwrap();
    let descendant = ProcessHandle::open(descendant).unwrap();
    assert!(root.is_alive().unwrap() && descendant.is_alive().unwrap());
    *injection.processes.lock().unwrap() = vec![root, descendant];
    (
        Box::new(ObservedChild {
            inner: child,
            fail_wait: injection.fail_wait,
            events: injection.events,
        }),
        true,
    )
}

#[derive(Debug)]
struct ObservedChild {
    inner: Box<dyn Child + Send + Sync>,
    fail_wait: bool,
    events: Arc<Mutex<Vec<&'static str>>>,
}
impl Drop for ObservedChild {
    fn drop(&mut self) {
        self.events.lock().unwrap().push("drop");
    }
}
impl ChildKiller for ObservedChild {
    fn kill(&mut self) -> std::io::Result<()> {
        self.inner.kill()
    }
    fn clone_killer(&self) -> Box<dyn ChildKiller + Send + Sync> {
        self.inner.clone_killer()
    }
}
impl Child for ObservedChild {
    fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        self.inner.try_wait()
    }
    fn wait(&mut self) -> std::io::Result<ExitStatus> {
        let status = self.inner.wait()?;
        self.events.lock().unwrap().push("native-wait");
        if self.fail_wait {
            Err(std::io::Error::other("injected wait confirmation failure"))
        } else {
            Ok(status)
        }
    }
    fn process_id(&self) -> Option<u32> {
        self.inner.process_id()
    }
    fn as_raw_handle(&self) -> Option<std::os::windows::io::RawHandle> {
        self.inner.as_raw_handle()
    }
}

fn powershell() -> PathBuf {
    undercurrent_platform_windows::windows_directory()
        .unwrap()
        .join("System32/WindowsPowerShell/v1.0/powershell.exe")
}
fn handle_count() -> u32 {
    let result = Command::new(powershell())
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            &format!("(Get-Process -Id {}).HandleCount", std::process::id()),
        ])
        .output()
        .unwrap();
    assert!(result.status.success());
    String::from_utf8(result.stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

#[test]
fn native_waiter_failure_retires_job_releases_handles_and_recovers_capacity() {
    // Isolate handle accounting from the test runner's other concurrently active
    // fixtures. The subprocess still executes the real runtime and ConPTY path.
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "process::reaper::windows_tests::native_waiter_failure_probe",
            "--nocapture",
        ])
        .env("UC_WAITER_FAILURE_PROBE", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("waiter fixture timed out");
        }
        thread::sleep(Duration::from_millis(20));
    }
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    print!("{}", String::from_utf8_lossy(&output.stdout));
}

#[test]
fn native_waiter_failure_probe() {
    if std::env::var("UC_WAITER_FAILURE_PROBE").as_deref() != Ok("1") {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let runtime = super::super::NativeProcessRuntime::start_with_system_clock(
        config(1),
        Arc::new(crate::BoundedTelemetryRecorder::new(64, 64).unwrap()),
    )
    .unwrap();
    let mut settled_counts = Vec::new();
    for iteration in 0..6 {
        let report = directory.path().join(format!("descendant-{iteration}"));
        let session = SessionId::new(format!("waiter-failure-{iteration}")).unwrap();
        let processes = Arc::new(Mutex::new(Vec::new()));
        let events = Arc::new(Mutex::new(Vec::new()));
        let fail_wait = iteration == 5;
        INJECTIONS.lock().unwrap().insert(
            session.clone(),
            Injection {
                report: report.clone(),
                fail_wait,
                processes: processes.clone(),
                events: events.clone(),
            },
        );
        let spec = launch(
            directory.path(),
            &std::env::current_exe().unwrap().to_string_lossy(),
            vec![
                "--exact".into(),
                "process::reaper::windows_tests::native_waiter_workload".into(),
                "--nocapture".into(),
            ],
            vec![(
                "UC_WAITER_REPORT".into(),
                report.to_string_lossy().into_owned(),
            )],
        );
        let prepared = block_on(runtime.prepare_launch(&spec)).unwrap();
        let error = block_on(runtime.spawn(&session, &prepared)).unwrap_err();
        assert_eq!(error.kind(), PortErrorKind::Unavailable);
        wait_until(|| {
            processes
                .lock()
                .unwrap()
                .iter()
                .all(|p| !p.is_alive().unwrap())
        });
        assert_eq!(*events.lock().unwrap(), ["native-wait", "drop"]);
        processes.lock().unwrap().clear();
        let expected_slots = usize::from(fail_wait);
        assert_eq!(
            runtime.pressure().unwrap().occupied_process_slots,
            expected_slots,
            "a failed wait must not release the occupied slot"
        );
        let replacement = launch(
            directory.path(),
            "cmd.exe",
            vec!["/D".into(), "/Q".into(), "/K".into()],
            Vec::new(),
        );
        let prepared = block_on(runtime.prepare_launch(&replacement)).unwrap();
        let next = SessionId::new(format!("replacement-{iteration}")).unwrap();
        if fail_wait {
            assert_eq!(
                block_on(runtime.spawn(&next, &prepared))
                    .unwrap_err()
                    .kind(),
                PortErrorKind::CapacityExceeded
            );
        } else {
            let process = block_on(runtime.spawn(&next, &prepared)).unwrap();
            block_on(runtime.force_stop(&next, &process.id)).unwrap();
            wait_until(|| runtime.pressure().unwrap().occupied_process_slots == 0);
            settled_counts.push(handle_count());
        }
    }
    println!("settled native handle counts: {settled_counts:?}");
    // Warm-up may initialize OS facilities. Four later repetitions must not
    // retain another process/job/ConPTY handle on every failed registration.
    assert!(
        settled_counts[4] <= settled_counts[1] + 2,
        "native handle growth: {settled_counts:?}"
    );
}

#[test]
fn native_waiter_workload() {
    let Some(report) = std::env::var_os("UC_WAITER_REPORT") else {
        return;
    };
    // Readiness comes from a file rather than an interactive shell prompt. The
    // descendant inherits the real managed job before waiter creation fails.
    let mut descendant = Command::new(
        undercurrent_platform_windows::windows_directory()
            .unwrap()
            .join("System32/ping.exe"),
    )
    .args(["-n", "60", "127.0.0.1"])
    .stdin(Stdio::null())
    .stdout(Stdio::null())
    .stderr(Stdio::null())
    .spawn()
    .unwrap();
    std::fs::write(report, descendant.id().to_string()).unwrap();
    let _ = descendant.wait();
}
