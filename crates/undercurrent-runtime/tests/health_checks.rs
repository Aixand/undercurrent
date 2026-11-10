// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::error::Error;
use std::future::Future;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;
#[cfg(unix)]
use std::time::Instant;

use undercurrent_application::{
    HealthCheckExecutor, OutputOffset, PortErrorKind, ProfilePatternEngine, TerminalOutputLimits,
    TerminalOutputStore,
};
#[cfg(unix)]
use undercurrent_domain::CommandSpec;
use undercurrent_domain::{
    ExecutionLimits, HealthRule, HealthRules, ProcessInstanceId, SessionId, TerminalSize,
};
use undercurrent_runtime::{
    NativeHealthCheckDetail, NativeHealthCheckExecutor, NativeHealthCheckExecutorLimits,
    RegexPatternEngine, RegexPatternEngineLimits, TerminalScreenLimits, VtTerminalScreenStore,
};

#[test]
fn native_health_details_are_complete_unique_and_bounded() {
    let names = NativeHealthCheckDetail::ALL
        .into_iter()
        .map(NativeHealthCheckDetail::as_str)
        .collect::<BTreeSet<_>>();

    assert_eq!(names.len(), NativeHealthCheckDetail::ALL.len());
    assert!(names.iter().all(|name| name.len() <= 32));
    assert!(names.contains("tcp_connected"));
    assert!(names.contains("red_cell_threshold_met"));
    assert!(names.contains("command_timeout"));
}

#[test]
fn tcp_checks_distinguish_a_listener_from_expected_unavailability() {
    let fixture = Fixture::new();
    let executor = fixture.executor(Duration::from_millis(250), 2, 4);
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let listening_port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || listener.accept().unwrap());
    let unavailable_port = (1..1_024)
        .find(|port| {
            TcpStream::connect_timeout(
                &SocketAddr::from((Ipv4Addr::LOCALHOST, *port)),
                Duration::from_millis(10),
            )
            .is_err()
        })
        .expect("localhost must expose at least one closed privileged port");
    let rules = rules([
        HealthRule::tcp_port(listening_port).unwrap(),
        HealthRule::tcp_port(unavailable_port).unwrap(),
    ]);

    let results = run(&executor, "tcp-health", &rules).unwrap();

    server.join().unwrap();
    assert_eq!(results.len(), 2);
    assert!(results[0].passed());
    assert_eq!(results[0].detail(), Some("tcp_connected"));
    assert!(!results[1].passed());
    assert!(matches!(
        results[1].detail(),
        Some("tcp_unavailable" | "tcp_timeout")
    ));
}

#[test]
fn http_checks_require_exact_status_200_without_reading_a_body() {
    let fixture = Fixture::new();
    let executor = fixture.executor(Duration::from_millis(500), 2, 4);
    let (ok_endpoint, ok_server) = http_server(200);
    let (unhealthy_endpoint, unhealthy_server) = http_server(204);
    let rules = rules([
        HealthRule::http_endpoint(ok_endpoint, 1_024).unwrap(),
        HealthRule::http_endpoint(unhealthy_endpoint, 1_024).unwrap(),
    ]);

    let results = run(&executor, "http-health", &rules).unwrap();

    ok_server.join().unwrap();
    unhealthy_server.join().unwrap();
    assert!(results[0].passed());
    assert_eq!(results[0].detail(), Some("http_ok"));
    assert!(!results[1].passed());
    assert_eq!(results[1].detail(), Some("http_status_not_ok"));
}

#[test]
fn screen_patterns_and_red_cells_share_one_private_checkpoint() {
    let fixture = Fixture::new();
    let session = session("screen-health");
    fixture.begin(&session);
    fixture.output(
        &session,
        b"private-ready-marker \x1b[31mERR\x1b[0m ordinary",
    );
    let executor = fixture.executor(Duration::from_millis(250), 1, 4);
    let rules = rules([
        HealthRule::output_contains("private-ready-marker", 1_024).unwrap(),
        HealthRule::output_rejects("fatal-marker", 1_024).unwrap(),
        HealthRule::red_cell_threshold(3).unwrap(),
    ]);

    let results = run_for(&executor, &session, &rules).unwrap();

    assert_eq!(results.len(), 3);
    assert!(results[0].passed());
    assert_eq!(results[0].detail(), Some("pattern_present"));
    assert!(results[1].passed());
    assert_eq!(results[1].detail(), Some("pattern_absent"));
    assert!(!results[2].passed());
    assert_eq!(results[2].detail(), Some("red_cell_threshold_met"));
    assert!(!format!("{results:?}").contains("private-ready-marker"));
    let pressure = executor.pressure();
    assert_eq!(pressure.passed_checks, 2);
    assert_eq!(pressure.failed_checks, 1);
}

#[test]
fn malformed_patterns_and_missing_screens_are_typed_and_redacted() {
    let fixture = Fixture::new();
    let session = session("invalid-pattern-health");
    fixture.begin(&session);
    fixture.output(&session, b"private-screen-value");
    let executor = fixture.executor(Duration::from_millis(250), 1, 4);
    let invalid = rules([HealthRule::output_contains("private-(value", 1_024).unwrap()]);

    let invalid_error = run_for(&executor, &session, &invalid).unwrap_err();

    assert_eq!(invalid_error.kind(), PortErrorKind::InvalidData);
    assert!(!format!("{invalid_error:?}").contains("private-(value"));
    assert!(!format!("{invalid_error:?}").contains("private-screen-value"));
    assert!(
        !invalid_error
            .source()
            .unwrap()
            .to_string()
            .contains("private")
    );

    let missing = rules([HealthRule::output_contains("ready", 1_024).unwrap()]);
    let missing_error = run(&executor, "missing-screen-health", &missing).unwrap_err();
    assert_eq!(missing_error.kind(), PortErrorKind::NotFound);
}

#[cfg(unix)]
#[test]
fn direct_commands_report_success_nonzero_spawn_failure_and_timeout() {
    let fixture = Fixture::new();
    let executor = fixture.executor(Duration::from_millis(100), 1, 4);
    let rules = rules([
        HealthRule::command(command("sh", &["-c", "exit 0"])),
        HealthRule::command(command("sh", &["-c", "exit 7"])),
        HealthRule::command(command(
            "missing-private-health-command-should-not-exist",
            &[],
        )),
        HealthRule::command(command("sh", &["-c", "sleep 2"])),
    ]);

    let results = run(&executor, "command-health", &rules).unwrap();

    assert_eq!(results.len(), 4);
    assert!(results[0].passed());
    assert_eq!(results[0].detail(), Some("command_succeeded"));
    assert!(!results[1].passed());
    assert_eq!(results[1].detail(), Some("command_exit_nonzero"));
    assert!(!results[2].passed());
    assert_eq!(results[2].detail(), Some("command_spawn_failed"));
    assert!(!results[3].passed());
    assert_eq!(results[3].detail(), Some("command_timeout"));
    assert!(!format!("{results:?}").contains("missing-private"));
}

#[cfg(unix)]
#[test]
fn fixed_pool_rejects_excess_work_and_cancels_abandoned_requests() {
    let fixture = Fixture::new();
    let executor = fixture.executor(Duration::from_secs(5), 1, 1);
    let process = process();
    let first_session = session("health-queue-first");
    let second_session = session("health-queue-second");
    let third_session = session("health-queue-third");
    let quick_session = session("health-queue-recovered");
    let slow = rules([HealthRule::command(command("sh", &["-c", "sleep 3"]))]);
    let quick = rules([HealthRule::command(command("sh", &["-c", "exit 0"]))]);
    let first = executor.run(&first_session, &process, &slow);
    wait_until(Duration::from_secs(1), || {
        executor.pressure().active_workers == 1
    });
    let second = executor.run(&second_session, &process, &slow);
    wait_until(Duration::from_secs(1), || {
        executor.pressure().queued_requests == 1
    });

    let rejection = block_on(executor.run(&third_session, &process, &slow)).unwrap_err();

    assert_eq!(rejection.kind(), PortErrorKind::CapacityExceeded);
    assert_eq!(executor.pressure().capacity_rejections, 1);
    drop(first);
    drop(second);
    wait_until(Duration::from_secs(1), || {
        let pressure = executor.pressure();
        pressure.active_workers == 0
            && pressure.queued_requests == 0
            && pressure.cancelled_requests >= 2
    });
    let recovered = block_on(executor.run(&quick_session, &process, &quick)).unwrap();
    assert!(recovered[0].passed());
    assert_eq!(executor.pressure().workers, 1);
}

#[test]
fn executor_limits_reject_zero_and_undersized_resource_bounds() {
    let mut limits = health_limits(Duration::from_millis(250), 1, 1);
    limits.workers = 0;
    assert!(limits.validate().is_err());

    let mut limits = health_limits(Duration::from_millis(250), 1, 1);
    limits.detail_bytes = 31;
    assert!(limits.validate().is_err());

    let mut limits = health_limits(Duration::ZERO, 1, 1);
    assert!(limits.validate().is_err());
    limits.check_timeout = Duration::from_millis(1);
    limits.worker_thread_stack_bytes = 63 * 1_024;
    assert!(limits.validate().is_err());
}

struct Fixture {
    screens: Arc<VtTerminalScreenStore>,
    output: TerminalOutputStore,
    patterns: Arc<RegexPatternEngine>,
}

impl Fixture {
    fn new() -> Self {
        Self {
            screens: Arc::new(
                VtTerminalScreenStore::new(TerminalScreenLimits {
                    scrollback_lines: 0,
                    sessions: 32,
                    snapshot_bytes: 64 * 1_024,
                })
                .unwrap(),
            ),
            output: TerminalOutputStore::new(TerminalOutputLimits {
                per_session_bytes: 64 * 1_024,
                global_bytes: 512 * 1_024,
                chunk_bytes: 1_024,
                append_bytes: 8 * 1_024,
                replay_bytes: 64 * 1_024,
                sessions: 32,
                subscriptions: 32,
            })
            .unwrap(),
            patterns: Arc::new(
                RegexPatternEngine::new(RegexPatternEngineLimits {
                    cached_patterns: 64,
                    compiled_size_bytes: 256 * 1_024,
                    determinize_size_bytes: 512 * 1_024,
                    match_text_bytes: 64 * 1_024,
                })
                .unwrap(),
            ),
        }
    }

    fn executor(
        &self,
        timeout: Duration,
        workers: usize,
        queue_capacity: usize,
    ) -> NativeHealthCheckExecutor {
        let patterns: Arc<dyn ProfilePatternEngine> = self.patterns.clone();
        NativeHealthCheckExecutor::new(
            Arc::clone(&self.screens),
            patterns,
            health_limits(timeout, workers, queue_capacity),
        )
        .unwrap()
    }

    fn begin(&self, session: &SessionId) {
        self.screens
            .begin_session(
                session.clone(),
                TerminalSize::new(80, 24, 400, 200).unwrap(),
                OutputOffset::ZERO,
            )
            .unwrap();
    }

    fn output(&self, session: &SessionId, bytes: &[u8]) {
        self.screens
            .observe_output(&self.output, session, bytes)
            .unwrap();
    }
}

fn health_limits(
    check_timeout: Duration,
    workers: usize,
    queue_capacity: usize,
) -> NativeHealthCheckExecutorLimits {
    NativeHealthCheckExecutorLimits {
        workers,
        queue_capacity,
        check_timeout,
        screen_text_bytes: 64 * 1_024,
        screen_cells: 80 * 24,
        pattern_bytes: 1_024,
        detail_bytes: 64,
        worker_thread_stack_bytes: 256 * 1_024,
    }
}

fn execution_limits() -> ExecutionLimits {
    ExecutionLimits {
        max_arguments: 16,
        max_command_bytes: 4_096,
        max_environment_entries: 16,
        max_environment_bytes: 4_096,
        max_path_bytes: 1_024,
        max_health_rules: 16,
        max_health_rule_bytes: 1_024,
    }
}

fn rules<const N: usize>(rules: [HealthRule; N]) -> HealthRules {
    HealthRules::new(rules, execution_limits()).unwrap()
}

#[cfg(unix)]
fn command(program: &str, arguments: &[&str]) -> CommandSpec {
    CommandSpec::new(
        program,
        arguments.iter().map(|value| (*value).to_owned()).collect(),
        execution_limits(),
    )
    .unwrap()
}

fn run(
    executor: &NativeHealthCheckExecutor,
    session_id: &str,
    rules: &HealthRules,
) -> Result<Vec<undercurrent_domain::HealthCheckResult>, undercurrent_application::PortError> {
    run_for(executor, &session(session_id), rules)
}

fn run_for(
    executor: &NativeHealthCheckExecutor,
    session: &SessionId,
    rules: &HealthRules,
) -> Result<Vec<undercurrent_domain::HealthCheckResult>, undercurrent_application::PortError> {
    block_on(executor.run(session, &process(), rules))
}

fn session(value: &str) -> SessionId {
    SessionId::new(value).unwrap()
}

fn process() -> ProcessInstanceId {
    ProcessInstanceId::new("health-process").unwrap()
}

fn http_server(status: u16) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let worker = thread::spawn(move || {
        let (mut connection, _) = listener.accept().unwrap();
        let mut request = [0_u8; 2_048];
        let _ = connection.read(&mut request);
        write!(
            connection,
            "HTTP/1.1 {status} Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        )
        .unwrap();
    });
    (format!("http://{address}/health"), worker)
}

#[cfg(unix)]
fn wait_until(timeout: Duration, condition: impl Fn() -> bool) {
    let started = Instant::now();
    while !condition() {
        assert!(started.elapsed() < timeout, "condition timed out");
        thread::sleep(Duration::from_millis(5));
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
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
