// SPDX-License-Identifier: Apache-2.0
use super::{NativeProcessRuntimeConfig, NativeProcessRuntimeLimits};
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};
use undercurrent_application::TerminalOutputLimits;
use undercurrent_domain::{
    CommandSpec, EnvironmentVariables, ExecutionEnvironmentBinding, ExecutionLimits, LaunchMode,
    LaunchSpec, PathReference, PayloadDigest, TerminalSize,
};

pub(super) fn config(max_processes: usize) -> NativeProcessRuntimeConfig {
    NativeProcessRuntimeConfig::new(
        "windows-failure-test",
        NativeProcessRuntimeLimits {
            max_processes,
            spawn_workers: 1,
            spawn_queue_capacity: 2,
            input_queue_sequences: 8,
            input_queue_bytes_per_process: 4096,
            output_read_bytes: 1024,
            screen_snapshot_bytes: 256 * 1024,
            environment_file_bytes: 65536,
            environment_entries: 256,
            environment_bytes: 65536,
            worker_thread_stack_bytes: 512 * 1024,
        },
        TerminalOutputLimits {
            per_session_bytes: 65536,
            global_bytes: 262144,
            chunk_bytes: 1024,
            append_bytes: 1024,
            replay_bytes: 65536,
            sessions: 8,
            subscriptions: 32,
        },
        Duration::from_secs(5),
    )
    .unwrap()
}

pub(super) fn launch(
    root: &Path,
    program: &str,
    arguments: Vec<String>,
    environment: Vec<(String, String)>,
) -> LaunchSpec {
    let limits = ExecutionLimits {
        max_arguments: 32,
        max_command_bytes: 16384,
        max_environment_entries: 256,
        max_environment_bytes: 65536,
        max_path_bytes: 4096,
        max_health_rules: 1,
        max_health_rule_bytes: 64,
    };
    LaunchSpec::new(
        CommandSpec::new(program, arguments, limits)
            .unwrap()
            .with_launch_mode(LaunchMode::Native),
        EnvironmentVariables::new(environment, limits).unwrap(),
        None,
        None,
        TerminalSize::new(80, 24, 200, 100).unwrap(),
    )
    .with_environment_binding(Some(ExecutionEnvironmentBinding::new(
        PayloadDigest::of_bytes(b"windows-failure-fixture"),
        PathReference::new(root.to_string_lossy(), 4096).unwrap(),
    )))
}

struct ThreadWake(std::thread::Thread);
impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}
pub(super) fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => {
                assert!(Instant::now() < deadline, "fixture operation timed out");
                std::thread::park_timeout(Duration::from_millis(10));
            }
        }
    }
}
pub(super) fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(Instant::now() < deadline, "fixture condition timed out");
        std::thread::sleep(Duration::from_millis(10));
    }
}
