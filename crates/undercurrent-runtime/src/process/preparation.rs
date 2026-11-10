// SPDX-License-Identifier: Apache-2.0

//! Bounded blocking workers for launch filesystem preparation.

use std::sync::Arc;
use std::thread::{self, JoinHandle};

use undercurrent_application::{PortError, PortErrorKind, PortOperation};
use undercurrent_domain::{LaunchSpec, SessionId};

use super::NativeProcessRuntimeLimits;
use super::preflight::{self, LaunchPreparationError};
use crate::RuntimeAdapterFailure;

type PreparationResult = Result<LaunchSpec, LaunchPreparationError>;
type PreparationWorker =
    dyn Fn(&LaunchSpec, NativeProcessRuntimeLimits) -> PreparationResult + Send + Sync;

/// Fixed worker pool separating filesystem reads and executable hashing from
/// the asynchronous application executor.
pub(super) struct PreparationPool {
    sender: async_channel::Sender<PreparationRequest>,
    receiver: async_channel::Receiver<PreparationRequest>,
    workers: Vec<JoinHandle<()>>,
    queue_capacity: usize,
}

impl PreparationPool {
    /// Starts one fixed-size worker set using the runtime's existing worker and
    /// queue bounds.
    pub(super) fn start(limits: NativeProcessRuntimeLimits) -> Result<Self, std::io::Error> {
        Self::start_with_worker(limits, Arc::new(preflight::prepare_launch))
    }

    fn start_with_worker(
        limits: NativeProcessRuntimeLimits,
        prepare: Arc<PreparationWorker>,
    ) -> Result<Self, std::io::Error> {
        let (sender, receiver) = async_channel::bounded(limits.spawn_queue_capacity);
        let mut workers = Vec::with_capacity(limits.spawn_workers);
        for worker_number in 0..limits.spawn_workers {
            let worker_receiver = receiver.clone();
            let worker = Arc::clone(&prepare);
            match thread::Builder::new()
                .name(format!("undercurrent-preparation-{worker_number}"))
                .stack_size(limits.worker_thread_stack_bytes)
                .spawn(move || preparation_loop(worker_receiver, worker))
            {
                Ok(handle) => workers.push(handle),
                Err(error) => {
                    sender.close();
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(error);
                }
            }
        }
        Ok(Self {
            sender,
            receiver,
            workers,
            queue_capacity: limits.spawn_queue_capacity,
        })
    }

    /// Attempts bounded admission without blocking an asynchronous executor.
    pub(super) fn submit(
        &self,
        launch: LaunchSpec,
        limits: NativeProcessRuntimeLimits,
    ) -> Result<async_channel::Receiver<Result<LaunchSpec, PortError>>, PortError> {
        let (reply, receiver) = async_channel::bounded(1);
        self.sender
            .try_send(PreparationRequest {
                launch,
                limits,
                reply,
            })
            .map_err(|error| {
                let kind = if error.is_full() {
                    PortErrorKind::CapacityExceeded
                } else {
                    PortErrorKind::Unavailable
                };
                PortError::new(kind, PortOperation::PrepareLaunch, None, error)
            })?;
        Ok(receiver)
    }

    pub(super) fn queued(&self) -> usize {
        self.sender.len()
    }

    pub(super) const fn queue_capacity(&self) -> usize {
        self.queue_capacity
    }

    /// Closes admission immediately while allowing already-admitted bounded
    /// work to settle.
    pub(super) fn close(&self) {
        self.sender.close();
    }

    pub(super) fn cancel_queued(&self) {
        self.close();
        while let Ok(request) = self.receiver.try_recv() {
            let _ = request.reply.try_send(Err(PortError::new(
                PortErrorKind::Cancelled,
                PortOperation::PrepareLaunch,
                None::<SessionId>,
                RuntimeAdapterFailure("native launch preparation is shutting down"),
            )));
        }
    }

    pub(super) fn is_quiescent(&self) -> bool {
        self.workers.iter().all(JoinHandle::is_finished)
    }

    /// Stops admission, cancels work that has not started, and joins the fixed
    /// workers. In-flight filesystem work completes before its worker exits.
    pub(super) fn shutdown(&mut self) {
        self.cancel_queued();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

impl Drop for PreparationPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct PreparationRequest {
    launch: LaunchSpec,
    limits: NativeProcessRuntimeLimits,
    reply: async_channel::Sender<Result<LaunchSpec, PortError>>,
}

fn preparation_loop(
    receiver: async_channel::Receiver<PreparationRequest>,
    prepare: Arc<PreparationWorker>,
) {
    while let Ok(request) = receiver.recv_blocking() {
        if request.reply.is_closed() {
            continue;
        }
        let result = prepare(&request.launch, request.limits).map_err(|source| {
            PortError::new(
                source.port_kind(),
                PortOperation::PrepareLaunch,
                None,
                source,
            )
        });
        let _ = request.reply.send_blocking(result);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, mpsc};

    use undercurrent_application::PortErrorKind;
    use undercurrent_domain::{
        CommandSpec, EnvironmentVariables, ExecutionLimits, LaunchSpec, TerminalSize,
    };

    use super::*;

    #[test]
    fn fixed_pool_rejects_pressure_beyond_its_bounded_queue() {
        let (started_tx, started_rx) = mpsc::sync_channel(2);
        let (release_tx, release_rx) = mpsc::sync_channel(2);
        let release_rx = Arc::new(Mutex::new(release_rx));
        let worker = {
            let release_rx = Arc::clone(&release_rx);
            Arc::new(
                move |launch: &LaunchSpec, _limits: NativeProcessRuntimeLimits| {
                    started_tx.send(()).expect("signal worker start");
                    release_rx
                        .lock()
                        .expect("release lock")
                        .recv()
                        .expect("release worker");
                    Ok(launch.clone())
                },
            ) as Arc<PreparationWorker>
        };
        let limits = runtime_limits();
        let mut pool = PreparationPool::start_with_worker(limits, worker).expect("pool starts");

        let first = pool.submit(launch(), limits).expect("first admitted");
        started_rx.recv().expect("first worker active");
        let second = pool.submit(launch(), limits).expect("second queued");
        assert_eq!(pool.queued(), 1);
        assert_eq!(pool.queue_capacity(), 1);
        let rejected = pool
            .submit(launch(), limits)
            .expect_err("third exceeds queue capacity");
        assert_eq!(rejected.kind(), PortErrorKind::CapacityExceeded);

        release_tx.send(()).expect("release first");
        first
            .recv_blocking()
            .expect("first reply")
            .expect("first prepares");
        started_rx.recv().expect("second worker active");
        release_tx.send(()).expect("release second");
        second
            .recv_blocking()
            .expect("second reply")
            .expect("second prepares");
        pool.shutdown();
    }

    fn launch() -> LaunchSpec {
        let limits = domain_limits();
        LaunchSpec::new(
            CommandSpec::new("true", Vec::new(), limits).expect("command"),
            EnvironmentVariables::new(Vec::new(), limits).expect("environment"),
            None,
            None,
            TerminalSize::new(80, 24, 200, 100).expect("terminal"),
        )
    }

    const fn runtime_limits() -> NativeProcessRuntimeLimits {
        NativeProcessRuntimeLimits {
            max_processes: 1,
            spawn_workers: 1,
            spawn_queue_capacity: 1,
            input_queue_sequences: 1,
            input_queue_bytes_per_process: 1,
            output_read_bytes: 1,
            screen_snapshot_bytes: 1,
            environment_file_bytes: 1,
            environment_entries: 1,
            environment_bytes: 1,
            worker_thread_stack_bytes: 64 * 1024,
        }
    }

    const fn domain_limits() -> ExecutionLimits {
        ExecutionLimits {
            max_arguments: 1,
            max_command_bytes: 16,
            max_environment_entries: 1,
            max_environment_bytes: 16,
            max_path_bytes: 16,
            max_health_rules: 1,
            max_health_rule_bytes: 16,
        }
    }
}
