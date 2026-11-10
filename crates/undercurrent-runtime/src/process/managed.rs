// SPDX-License-Identifier: Apache-2.0

//! One exact native PTY process and its bounded mutable adapter state.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread;
use std::time::{Duration, Instant};

use portable_pty::{ChildKiller, MasterPty, PtySize};
use undercurrent_application::{
    PortError, PortErrorKind, PortOperation, ProcessExitObservation, ProcessInstance,
    ProcessTermination, StandardInputClosure, TerminationKind,
};
use undercurrent_domain::{
    InputDeliveryPolicy, InputSequence, ProcessOutcome, SessionId, StopPolicy, TerminalSize,
    TimestampMillis,
};

use super::platform;
use super::preflight::PreparedLaunchHandles;
use crate::{
    EncodedTerminalInput, RuntimeAdapterFailure, RuntimeBoxedFailure,
    encode_terminal_input_delivery,
};

/// Requested termination behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TerminationMode {
    /// Signal gracefully, then force after the policy deadline.
    Graceful(StopPolicy),
    /// Force immediately.
    Forced,
}

/// Payload-safe snapshot used by runtime pressure diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ManagedProcessStatus {
    pub(super) exit_armed: bool,
    pub(super) exited: bool,
    pub(super) input_worker_active: bool,
    pub(super) queued_input_bytes: usize,
}

/// Result of recording a child exit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ExitHandling {
    /// Exit remains retained until activation arms publication.
    Retained,
    /// Exit was published to the bounded runtime queue.
    Published,
    /// The caller abandoned spawn ownership and the exit should be retired.
    Abandoned,
}

/// Result of abandoning a spawn whose response cannot be delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Abandonment {
    /// Exit has not arrived; the reaper will retire it when observed.
    AwaitingExit,
    /// Exit was already retained and can be retired immediately.
    AlreadyExited,
}

pub(super) struct ManagedProcess {
    session_id: SessionId,
    instance: ProcessInstance,
    pid: u32,
    root_start_ticks: Option<u64>,
    master: Mutex<Box<dyn MasterPty + Send>>,
    killer: Mutex<Box<dyn ChildKiller + Send + Sync>>,
    _launch_handles: PreparedLaunchHandles,
    input: Mutex<InputState>,
    input_changed: Condvar,
    queued_input_bytes: AtomicUsize,
    input_queue_sequences: usize,
    input_queue_bytes: usize,
    worker_stack_bytes: usize,
    exit: Mutex<ExitState>,
    exit_changed: Condvar,
    termination_gate: Mutex<()>,
}

enum InputState {
    Dormant(Option<Box<dyn Write + Send>>),
    Running(async_channel::Sender<InputWriteRequest>),
    Closing,
    CloseFailed(InputCloseFailure),
    Closed,
}

struct InputWriteRequest {
    delivery: EncodedTerminalInput,
    reply: async_channel::Sender<Result<(), InputWriteFailure>>,
}

#[derive(Debug, Clone, Copy)]
struct InputCloseFailure {
    kind: std::io::ErrorKind,
    raw_os_error: Option<i32>,
}

impl From<&std::io::Error> for InputCloseFailure {
    fn from(value: &std::io::Error) -> Self {
        Self {
            kind: value.kind(),
            raw_os_error: value.raw_os_error(),
        }
    }
}

impl Display for InputCloseFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "could not deliver terminal EOF ({:?}, os={:?})",
            self.kind, self.raw_os_error
        )
    }
}

impl Error for InputCloseFailure {}

#[derive(Debug)]
struct InputWriteFailure(std::io::Error, bool);

impl Display for InputWriteFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("could not write an atomic terminal input sequence")
    }
}

impl Error for InputWriteFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExitPublication {
    Unarmed,
    Armed,
    Published,
    Abandoned,
}

struct ExitState {
    observation: Option<ProcessExitObservation>,
    output_complete: bool,
    publication: ExitPublication,
}

impl ManagedProcess {
    /// Constructs one installed process around exclusively owned PTY handles.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        session_id: SessionId,
        instance: ProcessInstance,
        pid: u32,
        root_start_ticks: Option<u64>,
        master: Box<dyn MasterPty + Send>,
        writer: Box<dyn Write + Send>,
        killer: Box<dyn ChildKiller + Send + Sync>,
        launch_handles: PreparedLaunchHandles,
        input_queue_sequences: usize,
        input_queue_bytes: usize,
        worker_stack_bytes: usize,
    ) -> Self {
        Self {
            session_id,
            instance,
            pid,
            root_start_ticks,
            master: Mutex::new(master),
            killer: Mutex::new(killer),
            _launch_handles: launch_handles,
            input: Mutex::new(InputState::Dormant(Some(writer))),
            input_changed: Condvar::new(),
            queued_input_bytes: AtomicUsize::new(0),
            input_queue_sequences,
            input_queue_bytes,
            worker_stack_bytes,
            exit: Mutex::new(ExitState {
                observation: None,
                output_complete: false,
                // Activation must commit and bind the exact process before its
                // exit can reach the application. Retain fast exits until that
                // caller arms publication and the output reader has finished.
                // See [native process lifecycle](../../docs/internals/processes/native-processes.md#publish-an-exit-only-after-ownership-and-output-are-ready).
                publication: ExitPublication::Unarmed,
            }),
            exit_changed: Condvar::new(),
            termination_gate: Mutex::new(()),
        }
    }

    pub(super) const fn instance(&self) -> &ProcessInstance {
        &self.instance
    }

    #[cfg(windows)]
    pub(super) fn finish_terminal_output(&self) {
        self.master
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .finish_output();
    }

    /// Returns the adapter-owned native root PID for private observation.
    pub(super) const fn root_pid(&self) -> u32 {
        self.pid
    }

    pub(super) const fn root_start_ticks(&self) -> Option<u64> {
        self.root_start_ticks
    }

    #[cfg(windows)]
    pub(super) fn job_observer(
        &self,
    ) -> std::io::Result<Option<undercurrent_platform_windows::JobProcessObserver>> {
        self.killer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone_job_observer()
            .map(|job| {
                job.map(undercurrent_platform_windows::JobProcessObserver::from_owned_handle)
            })
    }

    /// Returns the kernel foreground process group of the managed PTY.
    ///
    /// The live master remains owned and locked by this process. This performs
    /// one kernel query, exposes no raw descriptor, and does not panic.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pub(super) fn foreground_process_group(&self) -> Option<u32> {
        #[cfg(unix)]
        {
            self.master
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .process_group_leader()
                .and_then(|pid| u32::try_from(pid).ok())
        }
        // ConPTY does not expose the POSIX foreground process-group contract.
        #[cfg(not(unix))]
        {
            None
        }
    }

    pub(super) fn status(&self) -> ManagedProcessStatus {
        let exit = self
            .exit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let input = self
            .input
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        ManagedProcessStatus {
            exit_armed: matches!(
                exit.publication,
                ExitPublication::Armed | ExitPublication::Published
            ),
            exited: exit.observation.is_some(),
            input_worker_active: matches!(*input, InputState::Running(_) | InputState::Closing),
            queued_input_bytes: self.queued_input_bytes.load(Ordering::Acquire),
        }
    }

    pub(super) fn exit_armed(&self) -> bool {
        let state = self
            .exit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        matches!(
            state.publication,
            ExitPublication::Armed | ExitPublication::Published
        )
    }

    pub(super) fn has_exited(&self) -> bool {
        self.exit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .observation
            .is_some()
    }

    /// Returns exact in-memory incarnation truth without querying by PID.
    pub(super) fn observed_state(
        &self,
        observed_at: TimestampMillis,
    ) -> undercurrent_domain::ProcessTruth {
        let state = self
            .exit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match state.observation.as_ref() {
            Some(ProcessExitObservation {
                outcome: ProcessOutcome::Exited(exit_code),
                observed_at,
                ..
            }) => undercurrent_domain::ProcessTruth::Exited {
                exit_code: *exit_code,
                observed_at: *observed_at,
            },
            Some(ProcessExitObservation {
                outcome: ProcessOutcome::SupervisionFailed,
                observed_at,
                ..
            }) => undercurrent_domain::ProcessTruth::Missing {
                observed_at: *observed_at,
            },
            None => undercurrent_domain::ProcessTruth::Running { observed_at },
        }
    }

    /// Arms exact-once exit publication and publishes any retained fast exit.
    pub(super) fn arm_exit(
        &self,
        exits: &async_channel::Sender<ProcessExitObservation>,
    ) -> Result<ExitHandling, PortError> {
        let mut state = self
            .exit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match state.publication {
            ExitPublication::Published => return Ok(ExitHandling::Published),
            ExitPublication::Armed => return Ok(ExitHandling::Retained),
            ExitPublication::Abandoned => {
                return Err(self.error(
                    PortErrorKind::Conflict,
                    PortOperation::ArmProcessExit,
                    RuntimeAdapterFailure("abandoned native process exit cannot be armed"),
                ));
            }
            ExitPublication::Unarmed => {}
        }
        if state.output_complete
            && let Some(observation) = state.observation.clone()
        {
            exits.try_send(observation).map_err(|error| {
                self.error(
                    PortErrorKind::CapacityExceeded,
                    PortOperation::ArmProcessExit,
                    error,
                )
            })?;
            state.publication = ExitPublication::Published;
            Ok(ExitHandling::Published)
        } else {
            state.publication = ExitPublication::Armed;
            Ok(ExitHandling::Retained)
        }
    }

    /// Retains, publishes, or retires one exact child outcome.
    pub(super) fn observe_exit(
        &self,
        outcome: ProcessOutcome,
        observed_at: undercurrent_domain::TimestampMillis,
        exits: &async_channel::Sender<ProcessExitObservation>,
    ) -> Result<ExitHandling, PortError> {
        let mut state = self
            .exit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.observation.is_some() {
            return Ok(match state.publication {
                ExitPublication::Abandoned if state.output_complete => ExitHandling::Abandoned,
                ExitPublication::Abandoned => ExitHandling::Retained,
                ExitPublication::Published => ExitHandling::Published,
                ExitPublication::Unarmed | ExitPublication::Armed => ExitHandling::Retained,
            });
        }
        let observation = ProcessExitObservation {
            session_id: self.session_id.clone(),
            process_id: self.instance.id.clone(),
            outcome,
            observed_at,
        };
        state.observation = Some(observation.clone());
        self.exit_changed.notify_all();
        match state.publication {
            ExitPublication::Unarmed => Ok(ExitHandling::Retained),
            ExitPublication::Armed if state.output_complete => {
                exits.try_send(observation).map_err(|error| {
                    self.error(
                        PortErrorKind::CapacityExceeded,
                        PortOperation::ReadProcessExit,
                        error,
                    )
                })?;
                state.publication = ExitPublication::Published;
                Ok(ExitHandling::Published)
            }
            ExitPublication::Armed => Ok(ExitHandling::Retained),
            ExitPublication::Published => Ok(ExitHandling::Published),
            ExitPublication::Abandoned if state.output_complete => Ok(ExitHandling::Abandoned),
            ExitPublication::Abandoned => Ok(ExitHandling::Retained),
        }
    }

    /// Completes output drainage and releases a now-settleable exit.
    pub(super) fn observe_output_complete(
        &self,
        exits: &async_channel::Sender<ProcessExitObservation>,
    ) -> Result<ExitHandling, PortError> {
        let mut state = self
            .exit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.output_complete = true;
        let Some(observation) = state.observation.clone() else {
            return Ok(ExitHandling::Retained);
        };
        self.exit_changed.notify_all();
        match state.publication {
            ExitPublication::Armed => {
                exits.try_send(observation).map_err(|error| {
                    self.error(
                        PortErrorKind::CapacityExceeded,
                        PortOperation::ReadProcessExit,
                        error,
                    )
                })?;
                state.publication = ExitPublication::Published;
                Ok(ExitHandling::Published)
            }
            ExitPublication::Abandoned => Ok(ExitHandling::Abandoned),
            ExitPublication::Unarmed => Ok(ExitHandling::Retained),
            ExitPublication::Published => Ok(ExitHandling::Published),
        }
    }

    /// Marks a response-cancelled spawn so it can never emit an orphan exit.
    pub(super) fn abandon(&self) -> Abandonment {
        let mut state = self
            .exit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.publication = ExitPublication::Abandoned;
        if state.observation.is_some() && state.output_complete {
            Abandonment::AlreadyExited
        } else {
            Abandonment::AwaitingExit
        }
    }

    /// Encodes, admits, and serializes one complete input sequence.
    pub(super) async fn write_input(
        self: &Arc<Self>,
        input: &InputSequence,
        policy: InputDeliveryPolicy,
    ) -> Result<(), PortError> {
        if self.has_exited() {
            return Err(self.error(
                PortErrorKind::Conflict,
                PortOperation::WriteInput,
                RuntimeAdapterFailure("native process already exited"),
            ));
        }
        let delivery = encode_terminal_input_delivery(input, policy).map_err(|error| {
            self.error(PortErrorKind::InvalidData, PortOperation::WriteInput, error)
        })?;
        self.write_encoded_input(delivery).await
    }

    /// Admits one delivery encoded by the trusted daemon-side input adapter.
    pub(super) async fn write_encoded_input(
        self: &Arc<Self>,
        delivery: EncodedTerminalInput,
    ) -> Result<(), PortError> {
        self.write_encoded_input_attempt(delivery, None).await
    }

    pub(super) async fn write_encoded_input_attempt(
        self: &Arc<Self>,
        delivery: EncodedTerminalInput,
        attempt: Option<&undercurrent_domain::OperationAttemptId>,
    ) -> Result<(), PortError> {
        if self.has_exited() {
            return Err(absent_input_error(
                self.error(
                    PortErrorKind::Conflict,
                    PortOperation::WriteInput,
                    RuntimeAdapterFailure("native process already exited"),
                ),
                attempt,
            ));
        }
        if delivery.bytes().is_empty() {
            return Err(absent_input_error(
                self.error(
                    PortErrorKind::InvalidData,
                    PortOperation::WriteInput,
                    RuntimeAdapterFailure("encoded terminal input is empty"),
                ),
                attempt,
            ));
        }
        let reservation = self
            .reserve_input_bytes(delivery.bytes().len())
            .map_err(|error| absent_input_error(error, attempt))?;
        let sender = self
            .input_sender()
            .map_err(|error| absent_input_error(error, attempt))?;
        let (reply, receiver) = async_channel::bounded(1);
        sender
            .send(InputWriteRequest { delivery, reply })
            .await
            .map_err(|error| {
                absent_input_error(
                    self.error(PortErrorKind::Unavailable, PortOperation::WriteInput, error),
                    attempt,
                )
            })?;
        // Before queue admission, dropping this future releases its byte budget.
        // After admission the worker owns both the write and that budget; losing
        // the reply must not free capacity while the bytes are still in flight.
        // See [native process lifecycle](../../docs/internals/processes/native-processes.md#serialize-input-while-keeping-ambiguous-writes-visible).
        reservation.transfer_to_worker();
        receiver
            .recv()
            .await
            .map_err(|error| {
                self.error(PortErrorKind::Unavailable, PortOperation::WriteInput, error)
            })?
            .map_err(|error| {
                let started = error.1;
                let error = self.error(PortErrorKind::Other, PortOperation::WriteInput, error);
                if started {
                    error
                } else {
                    absent_input_error(error, attempt)
                }
            })
    }

    /// Enqueues a bounded automatic terminal reply without blocking the output
    /// reader. Under input pressure, drop the reply instead of deadlocking a
    /// child that is writing output without reading its input.
    pub(super) fn queue_terminal_reply(self: &Arc<Self>, bytes: Vec<u8>) {
        if bytes.is_empty() || bytes.len() > 1024 || self.has_exited() {
            return;
        }
        let Ok(reservation) = self.reserve_input_bytes(bytes.len()) else {
            return;
        };
        let Ok(sender) = self.input_sender() else {
            return;
        };
        let Ok(delivery) = EncodedTerminalInput::from_parts(bytes, None) else {
            return;
        };
        let (reply, _receiver) = async_channel::bounded(1);
        if sender
            .try_send(InputWriteRequest { delivery, reply })
            .is_ok()
        {
            reservation.transfer_to_worker();
        }
    }

    /// Closes PTY input after every write accepted before this call.
    pub(super) fn close_input(self: &Arc<Self>) -> Result<StandardInputClosure, PortError> {
        let mut initiated = false;
        let mut state = self
            .input
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        loop {
            match &mut *state {
                InputState::Dormant(writer) => {
                    let Some(mut writer) = writer.take() else {
                        *state = InputState::CloseFailed(InputCloseFailure {
                            kind: std::io::ErrorKind::BrokenPipe,
                            raw_os_error: None,
                        });
                        continue;
                    };
                    let result = deliver_terminal_eof(writer.as_mut());
                    *state = match result.as_ref() {
                        Ok(()) => InputState::Closed,
                        Err(error) => InputState::CloseFailed(InputCloseFailure::from(error)),
                    };
                    drop(state);
                    drop(writer);
                    self.input_changed.notify_all();
                    return result
                        .map(|()| StandardInputClosure::Closed)
                        .map_err(|error| {
                            self.error(
                                PortErrorKind::Other,
                                PortOperation::CloseInput,
                                InputCloseFailure::from(&error),
                            )
                        });
                }
                InputState::Running(sender) => {
                    sender.close();
                    *state = InputState::Closing;
                    initiated = true;
                }
                InputState::Closing => {}
                InputState::CloseFailed(error) => {
                    return Err(self.error(
                        PortErrorKind::Other,
                        PortOperation::CloseInput,
                        *error,
                    ));
                }
                InputState::Closed => {
                    return Ok(if initiated {
                        StandardInputClosure::Closed
                    } else {
                        StandardInputClosure::AlreadyClosed
                    });
                }
            }
            state = self
                .input_changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    /// Applies validated dimensions to this process's exact PTY master.
    pub(super) fn resize(&self, size: TerminalSize) -> Result<(), PortError> {
        if self.has_exited() {
            return Err(self.error(
                PortErrorKind::Conflict,
                PortOperation::ResizeTerminal,
                RuntimeAdapterFailure("native process already exited"),
            ));
        }
        self.master
            .lock()
            .map_err(|_| {
                self.error(
                    PortErrorKind::Other,
                    PortOperation::ResizeTerminal,
                    RuntimeAdapterFailure("native PTY master lock is poisoned"),
                )
            })?
            .resize(PtySize {
                rows: size.rows(),
                cols: size.columns(),
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(|error| {
                self.error(
                    PortErrorKind::Other,
                    PortOperation::ResizeTerminal,
                    RuntimeBoxedFailure::new(error.into_boxed_dyn_error()),
                )
            })
    }

    /// Confirms graceful or forced termination without blocking an async caller.
    pub(super) async fn terminate(
        self: &Arc<Self>,
        mode: TerminationMode,
        force_confirmation_timeout: Duration,
        worker_stack_bytes: usize,
    ) -> Result<ProcessTermination, PortError> {
        if !matches!(mode, TerminationMode::Graceful(policy) if policy.no_escalation()) {
            self.suppress_exit_publication();
        }
        let (reply, receiver) = async_channel::bounded(1);
        let process = Arc::clone(self);
        thread::Builder::new()
            .name("undercurrent-process-stop".to_owned())
            .stack_size(worker_stack_bytes)
            .spawn(move || {
                let result = process.terminate_blocking(mode, force_confirmation_timeout);
                let _ = reply.send_blocking(result);
            })
            .map_err(|error| {
                self.error(
                    PortErrorKind::Unavailable,
                    termination_operation(mode),
                    error,
                )
            })?;
        receiver.recv().await.map_err(|error| {
            self.error(
                PortErrorKind::Unavailable,
                termination_operation(mode),
                error,
            )
        })?
    }

    /// Sends immediate force for spawn cancellation or runtime shutdown.
    pub(super) fn force_now(&self) -> Result<(), PortError> {
        let result = if self.exit_observation().is_some() {
            if self.wait_for_termination_complete(Duration::ZERO, PortOperation::ForceProcess)? {
                return Ok(());
            }
            platform::send_force_descendants(self.pid, &self.killer)
        } else {
            platform::send_force(self.pid, &self.killer)
        };
        result.map_err(|error| self.error(PortErrorKind::Other, PortOperation::ForceProcess, error))
    }

    pub(super) fn confirm_shutdown(&self, deadline: Instant) -> Result<(), PortError> {
        if self
            .wait_for_exit(deadline.saturating_duration_since(Instant::now()))
            .is_none()
        {
            return Err(self.error(
                PortErrorKind::Timeout,
                PortOperation::ForceProcess,
                RuntimeAdapterFailure("shutdown did not confirm native process exit"),
            ));
        }
        self.confirm_termination_complete(
            PortOperation::ForceProcess,
            deadline.saturating_duration_since(Instant::now()),
        )
    }

    fn terminate_blocking(
        &self,
        mode: TerminationMode,
        force_confirmation_timeout: Duration,
    ) -> Result<ProcessTermination, PortError> {
        let operation = termination_operation(mode);
        if matches!(mode, TerminationMode::Forced) {
            // Another controller can hold the gate throughout its grace window.
            // Signal this retained incarnation before joining its completion.
            if let Err(error) = self.force_now()
                && self.exit_observation().is_none()
            {
                return Err(error);
            }
        }
        let _gate = self.termination_gate.lock().map_err(|_| {
            self.error(
                PortErrorKind::Other,
                operation,
                RuntimeAdapterFailure("native process termination lock is poisoned"),
            )
        })?;
        if let Some(observation) = self.exit_observation() {
            let mut escalated = false;
            if let TerminationMode::Graceful(policy) = mode
                && !self.wait_for_termination_complete(policy.force_after(), operation)?
            {
                if policy.no_escalation() {
                    return Ok(ProcessTermination::new(
                        TerminationKind::GracefulTimedOut,
                        None,
                        observation.observed_at,
                    ));
                }
                self.force_now()?;
                escalated = true;
            }
            self.confirm_termination_complete(operation, force_confirmation_timeout)?;
            return Ok(termination_from_observation(mode, escalated, observation));
        }

        match mode {
            TerminationMode::Forced => {
                let observation = self
                    .wait_for_exit(force_confirmation_timeout)
                    .ok_or_else(|| {
                        self.error(
                            PortErrorKind::Timeout,
                            operation,
                            RuntimeAdapterFailure(
                                "forced native process termination was not confirmed before deadline",
                            ),
                        )
                    })?;
                self.confirm_termination_complete(operation, force_confirmation_timeout)?;
                Ok(termination_from_observation(mode, false, observation))
            }
            TerminationMode::Graceful(policy) => {
                if let Err(error) = platform::send_graceful(self.pid, policy.signal(), &self.killer)
                {
                    if let Some(observation) = self.exit_observation() {
                        self.confirm_termination_complete(operation, force_confirmation_timeout)?;
                        return Ok(termination_from_observation(mode, false, observation));
                    }
                    // Windows has no POSIX process-group signal equivalent.
                    // A bounded stop still honors its grace period before Job
                    // Object termination. Never silently force a graceful-only
                    // request, and never report forced termination as graceful.
                    #[cfg(windows)]
                    let allow_escalation =
                        matches!(error, platform::ProcessSignalError::GracefulUnsupported)
                            && !policy.no_escalation();
                    #[cfg(not(windows))]
                    let allow_escalation = false;
                    if !allow_escalation {
                        return Err(self.error(PortErrorKind::Other, operation, error));
                    }
                }
                let deadline = Instant::now() + policy.force_after();
                if let Some(observation) = self.wait_for_exit(policy.force_after())
                    && self.wait_for_termination_complete(
                        deadline.saturating_duration_since(Instant::now()),
                        operation,
                    )?
                {
                    return Ok(termination_from_observation(mode, false, observation));
                }
                if policy.no_escalation() {
                    let millis = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis()
                        .min(i64::MAX as u128) as i64;
                    return Ok(ProcessTermination::new(
                        TerminationKind::GracefulTimedOut,
                        None,
                        TimestampMillis::new(millis),
                    ));
                }
                // A root can exit while descendants keep the PTY open. They share
                // the same grace deadline and must be killed before settling stop.
                self.force_now()?;
                let observation = self
                    .wait_for_exit(force_confirmation_timeout)
                    .ok_or_else(|| {
                        self.error(
                            PortErrorKind::Timeout,
                            operation,
                            RuntimeAdapterFailure(
                                "escalated native process termination was not confirmed before deadline",
                            ),
                        )
                    })?;
                self.confirm_termination_complete(operation, force_confirmation_timeout)?;
                Ok(termination_from_observation(mode, true, observation))
            }
        }
    }

    fn confirm_termination_complete(
        &self,
        operation: PortOperation,
        timeout: Duration,
    ) -> Result<(), PortError> {
        if self.wait_for_termination_complete(timeout, operation)? {
            return Ok(());
        }
        Err(self.error(
            PortErrorKind::Timeout,
            operation,
            RuntimeAdapterFailure(
                "native process exited but terminal output or descendants remained before deadline",
            ),
        ))
    }

    fn suppress_exit_publication(&self) {
        let mut state = self
            .exit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.publication != ExitPublication::Published {
            state.publication = ExitPublication::Abandoned;
        }
    }

    fn exit_observation(&self) -> Option<ProcessExitObservation> {
        self.exit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .observation
            .clone()
    }

    fn wait_for_exit(&self, timeout: Duration) -> Option<ProcessExitObservation> {
        let deadline = Instant::now().checked_add(timeout)?;
        let mut state = self
            .exit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        loop {
            if let Some(observation) = state.observation.clone() {
                return Some(observation);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return None;
            }
            let (next, timed_out) = self
                .exit_changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next;
            if timed_out.timed_out() && state.observation.is_none() {
                return None;
            }
        }
    }

    fn wait_for_termination_complete(
        &self,
        timeout: Duration,
        operation: PortOperation,
    ) -> Result<bool, PortError> {
        #[cfg(not(target_os = "macos"))]
        {
            let _ = operation;
            Ok(self.wait_for_output_complete(timeout))
        }
        #[cfg(target_os = "macos")]
        {
            let deadline = Instant::now() + timeout;
            if !self.wait_for_output_complete(timeout) {
                return Ok(false);
            }
            // EOF alone cannot confirm Darwin descendant termination. Keep the
            // group in the same grace window, including after its root exits.
            while platform::process_group_exists(self.pid)
                .map_err(|error| self.error(PortErrorKind::Other, operation, error))?
            {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Ok(false);
                }
                thread::sleep(remaining.min(Duration::from_millis(10)));
            }
            Ok(true)
        }
    }

    fn wait_for_output_complete(&self, timeout: Duration) -> bool {
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            return false;
        };
        let mut state = self
            .exit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        loop {
            if state.output_complete {
                return true;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let (next, timed_out) = self
                .exit_changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next;
            if timed_out.timed_out() && !state.output_complete {
                return false;
            }
        }
    }

    fn input_sender(
        self: &Arc<Self>,
    ) -> Result<async_channel::Sender<InputWriteRequest>, PortError> {
        let mut state = self
            .input
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match &*state {
            InputState::Running(sender) => return Ok(sender.clone()),
            InputState::Closed => {
                return Err(self.error(
                    PortErrorKind::Unavailable,
                    PortOperation::WriteInput,
                    RuntimeAdapterFailure("native PTY input is closed"),
                ));
            }
            InputState::Closing => {
                return Err(self.error(
                    PortErrorKind::Conflict,
                    PortOperation::WriteInput,
                    RuntimeAdapterFailure("native PTY input is closing"),
                ));
            }
            InputState::CloseFailed(_) => {
                return Err(self.error(
                    PortErrorKind::Unavailable,
                    PortOperation::WriteInput,
                    RuntimeAdapterFailure("native PTY EOF delivery failed"),
                ));
            }
            InputState::Dormant(_) => {}
        }
        let InputState::Dormant(writer) = &mut *state else {
            return Err(self.error(
                PortErrorKind::InvalidData,
                PortOperation::WriteInput,
                RuntimeAdapterFailure("native PTY input state changed during worker startup"),
            ));
        };
        let Some(mut writer) = writer.take() else {
            *state = InputState::Closed;
            return Err(self.error(
                PortErrorKind::InvalidData,
                PortOperation::WriteInput,
                RuntimeAdapterFailure("native PTY writer disappeared before worker startup"),
            ));
        };
        let (sender, receiver) =
            async_channel::bounded::<InputWriteRequest>(self.input_queue_sequences);
        let weak = Arc::downgrade(self);
        let queued = Arc::new(QueuedBytesRelease {
            process: Arc::downgrade(self),
        });
        let worker_release = Arc::clone(&queued);
        let spawn = thread::Builder::new()
            .name("undercurrent-pty-input".to_owned())
            .stack_size(self.worker_stack_bytes)
            .spawn(move || {
                while let Ok(request) = receiver.recv_blocking() {
                    let byte_count = request.delivery.bytes().len();
                    let result = write_input_delivery(writer.as_mut(), &request.delivery);
                    worker_release.release(byte_count);
                    let failed = result.is_err();
                    let _ = request.reply.send_blocking(result);
                    if failed {
                        receiver.close();
                        while let Ok(queued) = receiver.try_recv() {
                            worker_release.release(queued.delivery.bytes().len());
                            let _ = queued.reply.send_blocking(Err(InputWriteFailure(
                                std::io::Error::new(
                                    std::io::ErrorKind::BrokenPipe,
                                    "native PTY input writer closed",
                                ),
                                false,
                            )));
                        }
                        break;
                    }
                }
                if let Some(process) = weak.upgrade() {
                    let mut input = process
                        .input
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    *input = if matches!(*input, InputState::Closing) {
                        match deliver_terminal_eof(writer.as_mut()) {
                            Ok(()) => InputState::Closed,
                            Err(error) => InputState::CloseFailed(InputCloseFailure::from(&error)),
                        }
                    } else {
                        InputState::Closed
                    };
                    drop(writer);
                    process.input_changed.notify_all();
                }
            });
        match spawn {
            Ok(_handle) => {
                *state = InputState::Running(sender.clone());
                Ok(sender)
            }
            Err(error) => {
                *state = InputState::Closed;
                Err(self.error(PortErrorKind::Unavailable, PortOperation::WriteInput, error))
            }
        }
    }

    fn reserve_input_bytes(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<InputByteReservation, PortError> {
        let mut current = self.queued_input_bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                return Err(self.input_capacity_error());
            };
            if next > self.input_queue_bytes {
                return Err(self.input_capacity_error());
            }
            match self.queued_input_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(InputByteReservation {
                        process: Arc::downgrade(self),
                        bytes,
                        transferred: false,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn input_capacity_error(&self) -> PortError {
        self.error(
            PortErrorKind::CapacityExceeded,
            PortOperation::WriteInput,
            RuntimeAdapterFailure("native process input byte capacity is exhausted"),
        )
    }

    fn error(
        &self,
        kind: PortErrorKind,
        operation: PortOperation,
        source: impl Error + Send + Sync + 'static,
    ) -> PortError {
        PortError::new(kind, operation, Some(self.session_id.clone()), source)
    }
}

/// Writes and flushes one serialized delivery, retaining worker ownership
/// across its optional internal pause.
fn write_input_delivery(
    writer: &mut dyn Write,
    delivery: &EncodedTerminalInput,
) -> Result<(), InputWriteFailure> {
    let result = (|| {
        if let Some(pause) = delivery.pause() {
            let (prefix, suffix) = delivery.bytes().split_at(pause.offset());
            writer.write_all(prefix)?;
            writer.flush()?;
            thread::sleep(pause.delay());
            writer.write_all(suffix)?;
            writer.flush()
        } else {
            writer.write_all(delivery.bytes())?;
            writer.flush()
        }
    })();
    result.map_err(|error| InputWriteFailure(error, true))
}

/// PTY standard input has no portable half-close. The typed close operation
/// maps at this adapter boundary to the conventional terminal EOF control byte,
/// flushes it after all queued writes, and then permanently closes admission.
fn deliver_terminal_eof(writer: &mut dyn Write) -> Result<(), std::io::Error> {
    writer.write_all(&[0x04])?;
    writer.flush()
}

struct InputByteReservation {
    process: Weak<ManagedProcess>,
    bytes: usize,
    transferred: bool,
}

impl InputByteReservation {
    fn transfer_to_worker(mut self) {
        self.transferred = true;
    }
}

impl Drop for InputByteReservation {
    fn drop(&mut self) {
        if !self.transferred
            && let Some(process) = self.process.upgrade()
        {
            process
                .queued_input_bytes
                .fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }
}

struct QueuedBytesRelease {
    process: Weak<ManagedProcess>,
}

impl QueuedBytesRelease {
    fn release(&self, bytes: usize) {
        if let Some(process) = self.process.upgrade() {
            process
                .queued_input_bytes
                .fetch_sub(bytes, Ordering::AcqRel);
        }
    }
}

fn termination_operation(mode: TerminationMode) -> PortOperation {
    match mode {
        TerminationMode::Graceful(_) => PortOperation::StopProcess,
        TerminationMode::Forced => PortOperation::ForceProcess,
    }
}

fn termination_from_observation(
    mode: TerminationMode,
    escalated: bool,
    observation: ProcessExitObservation,
) -> ProcessTermination {
    let kind = match (mode, escalated) {
        (TerminationMode::Forced, _) => TerminationKind::Forced,
        (TerminationMode::Graceful(_), true) => TerminationKind::ForcedAfterTimeout,
        (TerminationMode::Graceful(_), false) => TerminationKind::Graceful,
    };
    let exit_code = match observation.outcome {
        ProcessOutcome::Exited(code) => Some(code),
        ProcessOutcome::SupervisionFailed => None,
    };
    ProcessTermination::new(kind, exit_code, observation.observed_at)
}

#[cfg(test)]
mod tests {
    use super::deliver_terminal_eof;

    #[test]
    fn typed_close_maps_to_one_exact_terminal_eof_at_the_pty_boundary() {
        let mut bytes = Vec::new();

        deliver_terminal_eof(&mut bytes).expect("terminal EOF delivery");

        assert_eq!(bytes, [0x04]);
    }
}

// Only the caller that has not queued a write (or the worker discarding an
// unstarted request) can supply this proof. A failed write/flush may be partial.
fn absent_input_error(
    error: PortError,
    attempt: Option<&undercurrent_domain::OperationAttemptId>,
) -> PortError {
    match attempt {
        Some(id) => error.with_attempt_effect(
            id.clone(),
            undercurrent_domain::EffectCertainty::KnownAbsent,
        ),
        None => error,
    }
}

#[cfg(test)]
mod input_failure_tests {
    use super::*;

    struct FailingWriter {
        bytes: Vec<u8>,
        fail_flush: bool,
    }
    impl Write for FailingWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if !self.fail_flush && !self.bytes.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "injected after prefix",
                ));
            }
            let count = if self.fail_flush { bytes.len() } else { 1 };
            self.bytes.extend_from_slice(&bytes[..count]);
            Ok(count)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "injected flush failure",
            ))
        }
    }

    #[test]
    fn partial_write_and_flush_failure_never_prove_absence() {
        let delivery = EncodedTerminalInput::from_parts(b"abc".to_vec(), None).unwrap();
        for fail_flush in [false, true] {
            let mut writer = FailingWriter {
                bytes: vec![],
                fail_flush,
            };
            let error = write_input_delivery(&mut writer, &delivery).unwrap_err();
            assert_eq!(
                writer.bytes,
                if fail_flush {
                    b"abc".to_vec()
                } else {
                    b"a".to_vec()
                }
            );
            assert!(
                error.1,
                "an effect began before the failure, so absence is unproven"
            );
        }
    }
}
