// SPDX-License-Identifier: Apache-2.0

//! Runtime-wide event-driven child reaping.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::{Mutex, Weak};
use std::thread::{self, JoinHandle};

use portable_pty::Child;
use undercurrent_application::{PortError, PortErrorKind, PortOperation};
use undercurrent_domain::{ProcessInstanceId, ProcessOutcome, SessionId};

use super::RuntimeCore;
use crate::RuntimeAdapterFailure;

/// Failure while starting the shared child reaper.
#[derive(Debug)]
pub enum ReaperStartError {
    /// Unix signal subscription could not be established.
    #[cfg(unix)]
    Signals(std::io::Error),
    /// One required runtime-wide worker thread could not be started.
    Thread(std::io::Error),
}

impl Display for ReaperStartError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(unix)]
            Self::Signals(_) => formatter.write_str("could not subscribe to child exit signals"),
            Self::Thread(_) => formatter.write_str("could not start child reaper thread"),
        }
    }
}

impl Error for ReaperStartError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            #[cfg(unix)]
            Self::Signals(error) => Some(error),
            Self::Thread(error) => Some(error),
        }
    }
}

/// Cloneable registration edge held by bounded spawn workers.
#[derive(Clone)]
pub(super) struct ReaperSender {
    commands: async_channel::Sender<ReaperCommand>,
}

pub(super) struct ReaperRegistrationError {
    error: PortError,
    child_settled: bool,
}

impl ReaperRegistrationError {
    pub(super) fn child_settled(&self) -> bool {
        self.child_settled
    }

    pub(super) fn into_port_error(self) -> PortError {
        self.error
    }
}

impl ReaperSender {
    /// Transfers sole child-wait ownership to the runtime-wide reaper.
    pub(super) fn register(
        &self,
        session_id: SessionId,
        process_id: ProcessInstanceId,
        child: Box<dyn Child + Send + Sync>,
    ) -> Result<(), ReaperRegistrationError> {
        let error_session = session_id.clone();
        let (reply, receiver) = async_channel::bounded(1);
        let command = ReaperCommand::Register {
            session_id,
            process_id,
            child,
            reply,
        };
        if let Err(error) = self.commands.send_blocking(command) {
            let ReaperCommand::Register { child, .. } = error.into_inner() else {
                return Err(ReaperRegistrationError {
                    error: PortError::new(
                        PortErrorKind::InvalidData,
                        PortOperation::SpawnProcess,
                        Some(error_session),
                        RuntimeAdapterFailure("reaper registration command changed kind"),
                    ),
                    child_settled: false,
                });
            };
            let child_settled = settle_child(child);
            return Err(ReaperRegistrationError {
                error: PortError::new(
                    PortErrorKind::Unavailable,
                    PortOperation::SpawnProcess,
                    Some(error_session),
                    RuntimeAdapterFailure("native process reaper registration channel is closed"),
                ),
                child_settled,
            });
        }
        match receiver.recv_blocking() {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(error),
            Err(error) => Err(ReaperRegistrationError {
                error: PortError::new(
                    PortErrorKind::Unavailable,
                    PortOperation::SpawnProcess,
                    Some(error_session),
                    error,
                ),
                child_settled: false,
            }),
        }
    }
}

/// Owns the signal subscription and shared reaper worker lifetime.
pub(super) struct ReaperHandle {
    sender: ReaperSender,
    #[cfg(unix)]
    signal_handle: signal_hook::iterator::Handle,
    signal_thread: Option<JoinHandle<()>>,
    reaper_thread: Option<JoinHandle<()>>,
}

impl ReaperHandle {
    /// Starts bounded runtime-wide signal forwarding and child ownership.
    pub(super) fn start(
        core: Weak<RuntimeCore>,
        capacity: usize,
        worker_stack_bytes: usize,
    ) -> Result<Self, ReaperStartError> {
        let (commands, receiver) = async_channel::bounded(capacity);
        let reaper_thread = thread::Builder::new()
            .name("undercurrent-process-reaper".to_owned())
            .stack_size(worker_stack_bytes)
            .spawn(move || reaper_loop(core, receiver, worker_stack_bytes))
            .map_err(ReaperStartError::Thread)?;

        #[cfg(unix)]
        {
            let mut signals =
                match signal_hook::iterator::Signals::new([signal_hook::consts::signal::SIGCHLD]) {
                    Ok(signals) => signals,
                    Err(error) => {
                        commands.close();
                        let _ = reaper_thread.join();
                        return Err(ReaperStartError::Signals(error));
                    }
                };
            let signal_handle = signals.handle();
            let signal_commands = commands.clone();
            let signal_thread = match thread::Builder::new()
                .name("undercurrent-sigchld".to_owned())
                .stack_size(worker_stack_bytes)
                .spawn(move || {
                    for _signal in signals.forever() {
                        if signal_commands.send_blocking(ReaperCommand::Wake).is_err() {
                            break;
                        }
                    }
                }) {
                Ok(thread) => thread,
                Err(error) => {
                    signal_handle.close();
                    commands.close();
                    let _ = reaper_thread.join();
                    return Err(ReaperStartError::Thread(error));
                }
            };
            Ok(Self {
                sender: ReaperSender { commands },
                signal_handle,
                signal_thread: Some(signal_thread),
                reaper_thread: Some(reaper_thread),
            })
        }

        #[cfg(not(unix))]
        Ok(Self {
            sender: ReaperSender { commands },
            signal_thread: None,
            reaper_thread: Some(reaper_thread),
        })
    }

    pub(super) fn sender(&self) -> ReaperSender {
        self.sender.clone()
    }
}

impl Drop for ReaperHandle {
    fn drop(&mut self) {
        #[cfg(unix)]
        self.signal_handle.close();
        let _ = self.sender.commands.send_blocking(ReaperCommand::Shutdown);
        self.sender.commands.close();
        if let Some(thread) = self.signal_thread.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.reaper_thread.take() {
            let _ = thread.join();
        }
    }
}

enum ReaperCommand {
    Register {
        session_id: SessionId,
        process_id: ProcessInstanceId,
        child: Box<dyn Child + Send + Sync>,
        reply: async_channel::Sender<Result<(), ReaperRegistrationError>>,
    },
    #[cfg(unix)]
    Wake,
    Shutdown,
}

struct RegisteredChild {
    #[cfg(unix)]
    session_id: SessionId,
    child: Box<dyn Child + Send + Sync>,
}

fn reaper_loop(
    core: Weak<RuntimeCore>,
    commands: async_channel::Receiver<ReaperCommand>,
    worker_stack_bytes: usize,
) {
    let mut children = BTreeMap::<ProcessInstanceId, RegisteredChild>::new();
    while let Ok(command) = commands.recv_blocking() {
        match command {
            ReaperCommand::Register {
                session_id,
                process_id,
                child,
                reply,
            } => {
                let result = register_child(
                    &core,
                    &mut children,
                    session_id,
                    process_id,
                    child,
                    worker_stack_bytes,
                );
                let _ = reply.send_blocking(result);
            }
            #[cfg(unix)]
            ReaperCommand::Wake => inspect_children(&core, &mut children),
            ReaperCommand::Shutdown => break,
        }
    }
    for (_, child) in children {
        settle_child(child.child);
    }
}

#[cfg(unix)]
fn register_child(
    core: &Weak<RuntimeCore>,
    children: &mut BTreeMap<ProcessInstanceId, RegisteredChild>,
    session_id: SessionId,
    process_id: ProcessInstanceId,
    child: Box<dyn Child + Send + Sync>,
    _worker_stack_bytes: usize,
) -> Result<(), ReaperRegistrationError> {
    if children.contains_key(&process_id) {
        let child_settled = settle_child(child);
        return Err(ReaperRegistrationError {
            error: PortError::new(
                PortErrorKind::Conflict,
                PortOperation::SpawnProcess,
                Some(session_id),
                RuntimeAdapterFailure("native process is already registered with the reaper"),
            ),
            child_settled,
        });
    }
    children.insert(process_id.clone(), RegisteredChild { session_id, child });
    // SIGCHLD may already have been handled before this registration arrived.
    // Inspect now so a fast exit does not wait for an unrelated child to wake us.
    // See [native process lifecycle](../../docs/internals/processes/native-processes.md#child-waiting-and-output-completion).
    inspect_one(core, children, &process_id);
    Ok(())
}

#[cfg(not(unix))]
fn register_child(
    core: &Weak<RuntimeCore>,
    _children: &mut BTreeMap<ProcessInstanceId, RegisteredChild>,
    session_id: SessionId,
    process_id: ProcessInstanceId,
    child: Box<dyn Child + Send + Sync>,
    worker_stack_bytes: usize,
) -> Result<(), ReaperRegistrationError> {
    #[cfg(all(test, windows))]
    let (child, fail_start) = windows_tests::inject(&session_id, child);
    register_waiter(core, session_id, process_id, child, |waiter| {
        #[cfg(all(test, windows))]
        if fail_start {
            return Err(std::io::Error::other("injected waiter creation failure"));
        }
        thread::Builder::new()
            .name("undercurrent-process-wait".to_owned())
            .stack_size(worker_stack_bytes)
            .spawn(waiter)
            .map(|_thread| ())
    })
}

#[cfg(any(not(unix), test))]
fn register_waiter(
    core: &Weak<RuntimeCore>,
    session_id: SessionId,
    process_id: ProcessInstanceId,
    child: Box<dyn Child + Send + Sync>,
    launch: impl FnOnce(Box<dyn FnOnce() + Send>) -> std::io::Result<()>,
) -> Result<(), ReaperRegistrationError> {
    let error_session = session_id.clone();
    let core = core.clone();
    // A failed spawn drops its closure. Keep a separate handle to the child
    // until the worker actually takes wait ownership, so failure can retire it.
    let pending = std::sync::Arc::new(Mutex::new(Some(child)));
    let worker_pending = pending.clone();
    launch(Box::new(move || {
        let Some(mut child) = worker_pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        else {
            return;
        };
        let result = child.wait();
        let wait_confirmed = result.is_ok();
        let outcome = result.map_or(ProcessOutcome::SupervisionFailed, exit_outcome);
        if let Some(core) = core.upgrade() {
            core.complete_exit(&session_id, &process_id, outcome, wait_confirmed);
        }
    }))
    .map_err(|error| {
        let child_settled = pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .is_some_and(settle_child);
        ReaperRegistrationError {
            error: PortError::new(
                PortErrorKind::Unavailable,
                PortOperation::SpawnProcess,
                Some(error_session),
                error,
            ),
            child_settled,
        }
    })
}

#[cfg(unix)]
fn inspect_children(
    core: &Weak<RuntimeCore>,
    children: &mut BTreeMap<ProcessInstanceId, RegisteredChild>,
) {
    let process_ids: Vec<_> = children.keys().cloned().collect();
    for process_id in process_ids {
        inspect_one(core, children, &process_id);
    }
}

#[cfg(unix)]
fn inspect_one(
    core: &Weak<RuntimeCore>,
    children: &mut BTreeMap<ProcessInstanceId, RegisteredChild>,
    process_id: &ProcessInstanceId,
) {
    let outcome = match children.get_mut(process_id) {
        Some(child) => match child.child.try_wait() {
            Ok(Some(status)) => Some((exit_outcome(status), true)),
            Ok(None) => None,
            Err(_) => Some((ProcessOutcome::SupervisionFailed, false)),
        },
        None => None,
    };
    let Some((outcome, wait_confirmed)) = outcome else {
        return;
    };
    let Some(child) = children.remove(process_id) else {
        return;
    };
    if let Some(core) = core.upgrade() {
        core.complete_exit(&child.session_id, process_id, outcome, wait_confirmed);
    }
}

fn exit_outcome(status: portable_pty::ExitStatus) -> ProcessOutcome {
    if status.signal().is_some() {
        return ProcessOutcome::SupervisionFailed;
    }
    i32::try_from(status.exit_code())
        .map_or(ProcessOutcome::SupervisionFailed, ProcessOutcome::Exited)
}

fn settle_child(mut child: Box<dyn Child + Send + Sync>) -> bool {
    if let Some(pid) = child.process_id() {
        let killer = Mutex::new(child.clone_killer());
        let _ = super::platform::send_force(pid, &killer);
    } else {
        let _ = child.kill();
    }
    child.wait().is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use portable_pty::{ChildKiller, ExitStatus};
    use std::sync::Arc;

    #[derive(Debug, Clone)]
    struct ProbeChild {
        events: Arc<Mutex<Vec<&'static str>>>,
        fail_wait: bool,
    }
    impl Drop for ProbeChild {
        fn drop(&mut self) {
            self.events.lock().unwrap().push("drop");
        }
    }
    impl ChildKiller for ProbeChild {
        fn kill(&mut self) -> std::io::Result<()> {
            self.events.lock().unwrap().push("kill");
            Ok(())
        }
        fn clone_killer(&self) -> Box<dyn ChildKiller + Send + Sync> {
            Box::new(self.clone())
        }
    }
    impl Child for ProbeChild {
        fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
            Ok(None)
        }
        fn wait(&mut self) -> std::io::Result<ExitStatus> {
            self.events.lock().unwrap().push("wait");
            if self.fail_wait {
                Err(std::io::Error::other("injected wait failure"))
            } else {
                Ok(ExitStatus::with_exit_code(0))
            }
        }
        fn process_id(&self) -> Option<u32> {
            None
        }
        #[cfg(windows)]
        fn as_raw_handle(&self) -> Option<std::os::windows::io::RawHandle> {
            None
        }
    }
    #[test]
    fn waiter_start_failure_keeps_child_for_cleanup() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let result = register_waiter(
            &Weak::new(),
            SessionId::new("waiter-test").unwrap(),
            ProcessInstanceId::new("waiter-process").unwrap(),
            Box::new(ProbeChild {
                events: events.clone(),
                fail_wait: false,
            }),
            |_waiter| Err(std::io::Error::other("injected thread creation failure")),
        );
        assert!(result.err().unwrap().child_settled());
        assert_eq!(*events.lock().unwrap(), ["kill", "wait", "drop"]);
    }
    #[test]
    fn failed_wait_is_not_reported_as_settled() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let result = register_waiter(
            &Weak::new(),
            SessionId::new("waiter-test").unwrap(),
            ProcessInstanceId::new("waiter-process").unwrap(),
            Box::new(ProbeChild {
                events: events.clone(),
                fail_wait: true,
            }),
            |_waiter| Err(std::io::Error::other("injected thread creation failure")),
        );
        assert!(!result.err().unwrap().child_settled());
        assert_eq!(*events.lock().unwrap(), ["kill", "wait", "drop"]);
    }
    #[test]
    fn started_waiter_takes_sole_wait_ownership() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let result = register_waiter(
            &Weak::new(),
            SessionId::new("waiter-test").unwrap(),
            ProcessInstanceId::new("waiter-process").unwrap(),
            Box::new(ProbeChild {
                events: events.clone(),
                fail_wait: false,
            }),
            |waiter| {
                waiter();
                Ok(())
            },
        );
        assert!(result.is_ok());
        assert_eq!(*events.lock().unwrap(), ["wait", "drop"]);
    }
}

#[cfg(all(test, windows))]
#[path = "windows_reaper_tests.rs"]
pub(super) mod windows_tests;
