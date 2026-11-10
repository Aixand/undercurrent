// SPDX-License-Identifier: Apache-2.0

//! Bounded native PTY spawn workers and output readers.

use std::ffi::OsString;
use std::io::Read;
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use portable_pty::{Child, CommandBuilder, PtySize, native_pty_system};
use undercurrent_application::{
    PortError, PortErrorKind, PortOperation, ProcessInstance, ResolvedSecretEnvironment,
};
use undercurrent_domain::{LaunchSpec, OutputCaptureMode, SessionId};

use super::environment::load_environment;
use super::managed::ManagedProcess;
use super::reaper::ReaperSender;
use super::{NativeProcessRuntimeLimits, RuntimeCore};
use crate::{
    CooperativeEnvironmentVariable, CooperativeOscFilter, FilteredTerminalOutput,
    RuntimeAdapterFailure, RuntimeBoxedFailure,
};

/// Fixed worker pool for filesystem and process creation side effects.
pub(super) struct SpawnPool {
    sender: async_channel::Sender<SpawnRequest>,
    receiver: async_channel::Receiver<SpawnRequest>,
    workers: Vec<JoinHandle<()>>,
    core: Arc<RuntimeCore>,
}

impl SpawnPool {
    /// Starts the configured number of bounded blocking spawn workers.
    pub(super) fn start(
        core: Arc<RuntimeCore>,
        reaper: ReaperSender,
        limits: NativeProcessRuntimeLimits,
    ) -> Result<Self, std::io::Error> {
        let (sender, receiver) = async_channel::bounded(limits.spawn_queue_capacity);
        let mut workers = Vec::with_capacity(limits.spawn_workers);
        for worker_number in 0..limits.spawn_workers {
            let worker_receiver = receiver.clone();
            let worker_core = Arc::clone(&core);
            let worker_reaper = reaper.clone();
            match thread::Builder::new()
                .name(format!("undercurrent-spawn-{worker_number}"))
                .stack_size(limits.worker_thread_stack_bytes)
                .spawn(move || spawn_loop(worker_core, worker_reaper, worker_receiver))
            {
                Ok(worker) => workers.push(worker),
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
            core,
        })
    }

    /// Admits one already-reserved spawn without blocking the caller executor.
    pub(super) fn submit(
        &self,
        session_id: SessionId,
        launch: LaunchSpec,
        secret_environment: ResolvedSecretEnvironment,
    ) -> Result<async_channel::Receiver<SpawnReply>, PortError> {
        let error_session = session_id.clone();
        let (reply, receiver) = async_channel::bounded(1);
        self.sender
            .try_send(SpawnRequest {
                session_id,
                launch,
                secret_environment,
                reply,
            })
            .map_err(|error| {
                let kind = if error.is_full() {
                    PortErrorKind::CapacityExceeded
                } else {
                    PortErrorKind::Unavailable
                };
                PortError::new(
                    kind,
                    PortOperation::SpawnProcess,
                    Some(error_session),
                    error,
                )
            })?;
        Ok(receiver)
    }

    pub(super) fn queued(&self) -> usize {
        self.sender.len()
    }

    pub(super) fn close(&self) {
        self.sender.close();
        while let Ok(request) = self.receiver.try_recv() {
            self.core.release_reservation(&request.session_id);
            let _ = request.reply.try_send(SpawnReply::Failed(PortError::new(
                PortErrorKind::Cancelled,
                PortOperation::SpawnProcess,
                Some(request.session_id),
                RuntimeAdapterFailure("native process runtime is shutting down"),
            )));
        }
    }

    pub(super) fn is_quiescent(&self) -> bool {
        self.workers.iter().all(JoinHandle::is_finished)
    }

    /// Stops admission, cancels queued requests, and joins active workers.
    pub(super) fn shutdown(&mut self) {
        self.close();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

impl Drop for SpawnPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct SpawnRequest {
    session_id: SessionId,
    launch: LaunchSpec,
    secret_environment: ResolvedSecretEnvironment,
    reply: async_channel::Sender<SpawnReply>,
}

pub(super) enum SpawnReply {
    Spawned {
        process: ProcessInstance,
        acceptance: SpawnAcceptanceGuard,
    },
    Failed(PortError),
}

// Sending a successful reply does not prove the caller received it. Keep cleanup
// ownership inside the reply until the caller accepts it, including when a queued
// reply is dropped after spawn completed.
// See [native process lifecycle](../../docs/internals/processes/native-processes.md#follow-a-successful-spawn-and-its-cancellation-points).
pub(super) struct SpawnAcceptanceGuard {
    core: Arc<RuntimeCore>,
    session_id: SessionId,
    process_id: undercurrent_domain::ProcessInstanceId,
    accepted: bool,
}

impl SpawnAcceptanceGuard {
    pub(super) fn accept(mut self) {
        self.accepted = true;
    }
}

impl Drop for SpawnAcceptanceGuard {
    fn drop(&mut self) {
        if !self.accepted {
            self.core
                .abandon_process(&self.session_id, &self.process_id);
        }
    }
}

fn spawn_loop(
    core: Arc<RuntimeCore>,
    reaper: ReaperSender,
    receiver: async_channel::Receiver<SpawnRequest>,
) {
    while let Ok(request) = receiver.recv_blocking() {
        if request.reply.is_closed() {
            core.release_reservation(&request.session_id);
            continue;
        }
        let reply = match spawn_one(
            &core,
            &reaper,
            &request.session_id,
            &request.launch,
            &request.secret_environment,
        ) {
            Ok(process) => SpawnReply::Spawned {
                acceptance: SpawnAcceptanceGuard {
                    core: Arc::clone(&core),
                    session_id: request.session_id.clone(),
                    process_id: process.id.clone(),
                    accepted: false,
                },
                process,
            },
            Err(error) => SpawnReply::Failed(error),
        };
        let _ = request.reply.send_blocking(reply);
    }
}

fn spawn_one(
    core: &Arc<RuntimeCore>,
    reaper: &ReaperSender,
    session_id: &SessionId,
    launch: &LaunchSpec,
    secret_environment: &ResolvedSecretEnvironment,
) -> Result<ProcessInstance, PortError> {
    match prepare_and_spawn(core, reaper, session_id, launch, secret_environment) {
        Ok(instance) => Ok(instance),
        Err(error) => {
            core.release_reservation(session_id);
            Err(error)
        }
    }
}

fn prepare_and_spawn(
    core: &Arc<RuntimeCore>,
    reaper: &ReaperSender,
    session_id: &SessionId,
    launch: &LaunchSpec,
    secret_environment: &ResolvedSecretEnvironment,
) -> Result<ProcessInstance, PortError> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    let verified = super::preflight::verify_prepared_launch_cached(
        launch,
        &core.sealed_executables,
        &core.pinned_directories,
    );
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    let verified = super::preflight::verify_prepared_launch(launch);
    let launch_handles = verified.map_err(|source| {
        PortError::new(
            source.port_kind(),
            PortOperation::SpawnProcess,
            Some(session_id.clone()),
            source,
        )
    })?;
    let limits = core.config.limits();
    let environment = load_environment(launch, limits).map_err(|error| {
        PortError::new(
            PortErrorKind::InvalidData,
            PortOperation::SpawnProcess,
            Some(session_id.clone()),
            error,
        )
    })?;
    let size = launch.terminal_size();
    let capture_mode = launch.output_capture_mode();
    let pair = native_pty_system()
        .openpty(PtySize {
            rows: size.rows(),
            cols: size.columns(),
            pixel_width: 0,
            pixel_height: 0,
        })
        .map_err(|error| {
            PortError::new(
                PortErrorKind::Unavailable,
                PortOperation::SpawnProcess,
                Some(session_id.clone()),
                RuntimeBoxedFailure::new(error.into_boxed_dyn_error()),
            )
        })?;
    let reader = pair.master.try_clone_reader().map_err(|error| {
        PortError::new(
            PortErrorKind::Unavailable,
            PortOperation::SpawnProcess,
            Some(session_id.clone()),
            RuntimeBoxedFailure::new(error.into_boxed_dyn_error()),
        )
    })?;
    let writer = pair.master.take_writer().map_err(|error| {
        PortError::new(
            PortErrorKind::Unavailable,
            PortOperation::SpawnProcess,
            Some(session_id.clone()),
            RuntimeBoxedFailure::new(error.into_boxed_dyn_error()),
        )
    })?;

    let mut command = CommandBuilder::new(launch_handles.program_path());
    command.args(launch.command().arguments());
    #[cfg(windows)]
    if let Some(script) = launch_handles.windows_batch_script() {
        command.windows_batch_script(script).map_err(|_| {
            PortError::new(
                PortErrorKind::InvalidData,
                PortOperation::SpawnProcess,
                Some(session_id.clone()),
                super::RuntimeAdapterFailure("batch command is invalid"),
            )
        })?;
    }
    for (key, value) in environment {
        command.env(key, value);
    }
    for (key, secret) in secret_environment.iter() {
        let value = secret.expose(secret_os_value).map_err(|source| {
            PortError::new(
                PortErrorKind::InvalidData,
                PortOperation::SpawnProcess,
                Some(session_id.clone()),
                source,
            )
        })?;
        command.env(key, value);
    }
    command.env(
        CooperativeEnvironmentVariable::Session.as_str(),
        session_id.as_str(),
    );
    if let Some(socket) = &core.config.shell_capture_socket {
        command.env("UC_SHELL_HOST", socket.as_os_str());
        command.env("UC_SHELL_ENV_BYTES", limits.environment_bytes.to_string());
        command.env(
            "UC_SHELL_ENV_ENTRIES",
            limits.environment_entries.to_string(),
        );
        if let Ok(helper) = std::env::current_exe() {
            command.env("UC_SHELL_HELPER", helper.as_os_str());
        }
    }
    if let Some(directory) = launch_handles.working_directory_path() {
        command.cwd(directory);
    }
    #[cfg(target_os = "macos")]
    if let Some(directory) = launch_handles.working_directory_handle() {
        command.cwd_handle(directory).map_err(|error| {
            PortError::new(
                PortErrorKind::Unavailable,
                PortOperation::SpawnProcess,
                Some(session_id.clone()),
                error,
            )
        })?;
    }
    let child = pair.slave.spawn_command(command).map_err(|error| {
        PortError::new(
            PortErrorKind::Unavailable,
            PortOperation::SpawnProcess,
            Some(session_id.clone()),
            RuntimeBoxedFailure::new(error.into_boxed_dyn_error()),
        )
    })?;
    drop(pair.slave);
    let mut child = UnregisteredChild::new(child);
    let pid = child
        .child()
        .and_then(|child| child.process_id())
        .ok_or_else(|| {
            PortError::new(
                PortErrorKind::InvalidData,
                PortOperation::SpawnProcess,
                Some(session_id.clone()),
                RuntimeAdapterFailure("native PTY child did not expose a process identifier"),
            )
        })?;
    child.set_pid(pid);
    let root_start_ticks = super::platform::process_start_ticks(pid).map_err(|source| {
        PortError::new(
            PortErrorKind::InvalidData,
            PortOperation::SpawnProcess,
            Some(session_id.clone()),
            source,
        )
    })?;
    let instance = ProcessInstance {
        id: core.next_instance_id(pid)?,
        started_at: core.clock.now(),
    };
    let killer = child
        .child()
        .map(|child| child.clone_killer())
        .ok_or_else(|| {
            PortError::new(
                PortErrorKind::InvalidData,
                PortOperation::SpawnProcess,
                Some(session_id.clone()),
                RuntimeAdapterFailure("native PTY child ownership disappeared before transfer"),
            )
        })?;
    let process = Arc::new(ManagedProcess::new(
        session_id.clone(),
        instance.clone(),
        pid,
        root_start_ticks,
        pair.master,
        writer,
        killer,
        launch_handles,
        limits.input_queue_sequences,
        limits.input_queue_bytes_per_process,
        limits.worker_thread_stack_bytes,
    ));
    let output_offset = core
        .output
        .session_status(session_id)
        .map_err(|error| {
            PortError::new(
                PortErrorKind::Other,
                PortOperation::SpawnProcess,
                Some(session_id.clone()),
                error,
            )
        })?
        .map_or(undercurrent_application::OutputOffset::ZERO, |status| {
            status.next_offset
        });
    let screen_update = core
        .screens
        .prepare_session_with_capture_mode(session_id.clone(), size, output_offset, capture_mode)
        .map_err(|error| {
            PortError::new(
                PortErrorKind::CapacityExceeded,
                PortOperation::SpawnProcess,
                Some(session_id.clone()),
                error,
            )
        })?;
    core.install_process(session_id, Arc::clone(&process))?;

    #[cfg(all(test, windows))]
    let reader = if super::reaper::windows_tests::needs_output(session_id) {
        // Let the failure fixture answer ConPTY startup queries and start its
        // descendant before rejecting waiter creation. Normal launches retain
        // the production ordering below; the injected launch never succeeds.
        spawn_output_reader(
            core,
            session_id.clone(),
            instance.id.clone(),
            capture_mode,
            reader,
        )?;
        Box::new(std::io::empty()) as Box<dyn Read + Send>
    } else {
        reader
    };

    let Some(reaper_child) = child.take() else {
        core.abandon_reaped_process(session_id, &instance.id);
        core.complete_output(session_id, &instance.id);
        core.complete_exit(
            session_id,
            &instance.id,
            undercurrent_domain::ProcessOutcome::SupervisionFailed,
            false,
        );
        return Err(PortError::new(
            PortErrorKind::InvalidData,
            PortOperation::SpawnProcess,
            Some(session_id.clone()),
            RuntimeAdapterFailure("native PTY child ownership disappeared before reaping"),
        ));
    };
    if let Err(error) = reaper.register(session_id.clone(), instance.id.clone(), reaper_child) {
        let settled = error.child_settled();
        if settled {
            core.abandon_reaped_process(session_id, &instance.id);
        } else {
            core.abandon_process(session_id, &instance.id);
        }
        #[cfg(windows)]
        process.finish_terminal_output();
        core.complete_output(session_id, &instance.id);
        // A failed wait does not prove settlement. Keep the abandoned slot
        // occupied until a real waiter reports exit or the runtime is dropped.
        if settled {
            core.complete_exit(
                session_id,
                &instance.id,
                undercurrent_domain::ProcessOutcome::SupervisionFailed,
                true,
            );
        }
        return Err(error.into_port_error());
    }
    if let Err(error) = spawn_output_reader(
        core,
        session_id.clone(),
        instance.id.clone(),
        capture_mode,
        reader,
    ) {
        core.complete_output(session_id, &instance.id);
        core.abandon_process(session_id, &instance.id);
        return Err(error);
    }
    screen_update.commit();
    Ok(instance)
}

#[cfg(unix)]
fn secret_os_value(bytes: &[u8]) -> Result<OsString, RuntimeAdapterFailure> {
    Ok(OsString::from_vec(bytes.to_vec()))
}

#[cfg(not(unix))]
fn secret_os_value(bytes: &[u8]) -> Result<OsString, RuntimeAdapterFailure> {
    String::from_utf8(bytes.to_vec())
        .map(OsString::from)
        .map_err(|_| {
            RuntimeAdapterFailure("resolved secret is not valid platform environment text")
        })
}

struct UnregisteredChild {
    child: Option<Box<dyn Child + Send + Sync>>,
    pid: Option<u32>,
}

impl UnregisteredChild {
    fn new(child: Box<dyn Child + Send + Sync>) -> Self {
        Self {
            child: Some(child),
            pid: None,
        }
    }

    fn child(&self) -> Option<&(dyn Child + Send + Sync)> {
        self.child.as_deref()
    }

    fn take(&mut self) -> Option<Box<dyn Child + Send + Sync>> {
        self.child.take()
    }

    fn set_pid(&mut self, pid: u32) {
        self.pid = Some(pid);
    }
}

impl Drop for UnregisteredChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            if let Some(pid) = self.pid {
                let killer = Mutex::new(child.clone_killer());
                let _ = super::platform::send_force(pid, &killer);
            } else {
                let _ = child.kill();
            }
            let _ = child.wait();
        }
    }
}

fn spawn_output_reader(
    core: &Arc<RuntimeCore>,
    session_id: SessionId,
    process_id: undercurrent_domain::ProcessInstanceId,
    capture_mode: OutputCaptureMode,
    reader: Box<dyn Read + Send>,
) -> Result<(), PortError> {
    let error_session = session_id.clone();
    let weak = Arc::downgrade(core);
    let read_bytes = core.config.limits().output_read_bytes;
    thread::Builder::new()
        .name("undercurrent-pty-output".to_owned())
        .stack_size(core.config.limits().worker_thread_stack_bytes)
        .spawn(move || match capture_mode {
            OutputCaptureMode::Terminal => {
                read_terminal_output(weak, session_id, process_id, reader, read_bytes)
            }
            OutputCaptureMode::Opaque => {
                read_opaque_output(weak, session_id, process_id, reader, read_bytes)
            }
        })
        .map(|_thread| ())
        .map_err(|error| {
            PortError::new(
                PortErrorKind::Unavailable,
                PortOperation::ReadTerminalOutput,
                Some(error_session),
                error,
            )
        })
}

fn read_terminal_output(
    weak: std::sync::Weak<RuntimeCore>,
    session_id: SessionId,
    process_id: undercurrent_domain::ProcessInstanceId,
    mut reader: Box<dyn Read + Send>,
    read_bytes: usize,
) {
    let mut buffer = vec![0_u8; read_bytes];
    let mut filter = CooperativeOscFilter::new();
    let mut flush_filter = true;
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => {
                let Some(core) = weak.upgrade() else {
                    break;
                };
                let filtered = filter.filter(&buffer[..count]);
                if let Err(error) =
                    observe_filtered_output(&core, &session_id, &process_id, filtered)
                {
                    core.terminal_output_failed(
                        &session_id,
                        &process_id,
                        PortOperation::RetainTerminalOutput,
                        error,
                    );
                    break;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            #[cfg(unix)]
            Err(error) if error.raw_os_error() == Some(nix::errno::Errno::EIO as i32) => break,
            Err(error) => {
                flush_filter = false;
                if let Some(core) = weak.upgrade() {
                    core.terminal_output_failed(
                        &session_id,
                        &process_id,
                        PortOperation::ReadTerminalOutput,
                        error,
                    );
                }
                break;
            }
        }
    }
    if let Some(core) = weak.upgrade() {
        if flush_filter {
            let filtered = filter.finish();
            if let Err(error) = observe_filtered_output(&core, &session_id, &process_id, filtered) {
                core.terminal_output_failed(
                    &session_id,
                    &process_id,
                    PortOperation::RetainTerminalOutput,
                    error,
                );
            }
        }
        core.complete_output(&session_id, &process_id);
    }
}

fn read_opaque_output(
    weak: std::sync::Weak<RuntimeCore>,
    session_id: SessionId,
    process_id: undercurrent_domain::ProcessInstanceId,
    mut reader: Box<dyn Read + Send>,
    read_bytes: usize,
) {
    let mut buffer = vec![0_u8; read_bytes];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => {
                let Some(core) = weak.upgrade() else {
                    break;
                };
                if let Err(error) =
                    observe_opaque_output(&core, &session_id, &process_id, &buffer[..count])
                {
                    core.terminal_output_failed(
                        &session_id,
                        &process_id,
                        PortOperation::RetainTerminalOutput,
                        error,
                    );
                    break;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            #[cfg(unix)]
            Err(error) if error.raw_os_error() == Some(nix::errno::Errno::EIO as i32) => break,
            Err(error) => {
                if let Some(core) = weak.upgrade() {
                    core.terminal_output_failed(
                        &session_id,
                        &process_id,
                        PortOperation::ReadTerminalOutput,
                        error,
                    );
                }
                break;
            }
        }
    }
    if let Some(core) = weak.upgrade() {
        core.complete_output(&session_id, &process_id);
    }
}

fn observe_filtered_output(
    core: &Arc<RuntimeCore>,
    session_id: &SessionId,
    process_id: &undercurrent_domain::ProcessInstanceId,
    filtered: FilteredTerminalOutput,
) -> Result<(), PortError> {
    core.cooperative_signals
        .record_filter_outcomes(filtered.malformed, filtered.oversized);
    for signal in filtered.signals {
        let observed_at = core.clock.now();
        core.cooperative_signals.record(
            session_id.clone(),
            process_id.clone(),
            signal,
            observed_at,
        );
        core.output_observer
            .observe_cooperative(session_id, process_id, signal, observed_at);
    }
    if filtered.visible.is_empty() {
        return Ok(());
    }
    let visible = filtered.visible;
    let appended = core
        .screens
        .observe_output(core.output.as_ref(), session_id, &visible)?;
    let replies = core.screens.take_replies(session_id);
    if !replies.is_empty()
        && let Ok(process) = core.resolve_process(session_id, process_id, PortOperation::WriteInput)
    {
        process.queue_terminal_reply(replies);
    }
    core.record_output_metrics(session_id, visible.len(), appended.global_bytes_retained);
    core.output_observer
        .observe_owned(session_id, process_id, appended, visible);
    Ok(())
}

fn observe_opaque_output(
    core: &Arc<RuntimeCore>,
    session_id: &SessionId,
    process_id: &undercurrent_domain::ProcessInstanceId,
    bytes: &[u8],
) -> Result<(), PortError> {
    let appended = core
        .screens
        .observe_output(core.output.as_ref(), session_id, bytes)?;
    core.record_output_metrics(session_id, bytes.len(), appended.global_bytes_retained);
    core.output_observer
        .observe(session_id, process_id, appended, bytes);
    Ok(())
}
