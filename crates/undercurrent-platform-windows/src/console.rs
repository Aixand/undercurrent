// SPDX-License-Identifier: Apache-2.0
//! Cancellable native console notifications for blocking application workers.
use std::io;
use tokio::signal::windows::{CtrlBreak, CtrlC};
use tokio::sync::watch;

/// Blocking Ctrl-C/Ctrl-Break iterator; both notifications yield `()`.
///
/// A cloned close handle wakes an idle iterator. Registration errors are returned
/// by construction; reactor wait errors during iteration become `None`.
pub struct ConsoleSignals {
    interrupt: CtrlC,
    terminate: CtrlBreak,
    closed: watch::Receiver<bool>,
    handle: ConsoleSignalHandle,
}

#[derive(Clone)]
/// Shared terminal close flag for a console notification iterator.
pub struct ConsoleSignalHandle(watch::Sender<bool>);
impl ConsoleSignalHandle {
    /// Wakes the iterator and makes subsequent calls end. This neither delivers
    /// a console signal nor cancels a runtime attempt.
    pub fn close(&self) {
        self.0.send_replace(true);
    }
}

impl ConsoleSignals {
    pub fn new() -> io::Result<Self> {
        let runtime = crate::blocking_pipe::reactor()?;
        let (interrupt, terminate) = {
            let _entered = runtime.enter();
            (
                tokio::signal::windows::ctrl_c()?,
                tokio::signal::windows::ctrl_break()?,
            )
        };
        let (handle, closed) = watch::channel(false);
        Ok(Self {
            interrupt,
            terminate,
            closed,
            handle: ConsoleSignalHandle(handle),
        })
    }
    pub fn handle(&self) -> ConsoleSignalHandle {
        self.handle.clone()
    }
    /// Returns this iterator by mutable reference; the close handle can still
    /// terminate iteration.
    pub fn forever(&mut self) -> &mut Self {
        self
    }
}

impl Iterator for ConsoleSignals {
    type Item = ();
    fn next(&mut self) -> Option<()> {
        if *self.closed.borrow() {
            return None;
        }
        crate::blocking_pipe::wait(async {
            tokio::select! {
                biased;
                _ = self.closed.changed() => None,
                value = self.interrupt.recv() => value,
                value = self.terminate.recv() => value,
            }
        })
        .ok()
        .flatten()
    }
}

/// Prevent a detached child from retaining the caller's redirected standard
/// streams in addition to its explicitly configured null streams. This does
/// not close or replace the handles; explicit stdio inheritance still works
/// because std::process duplicates its selected handles for each child.
#[allow(unsafe_code)]
pub fn prevent_standard_handle_inheritance() -> std::io::Result<()> {
    use windows_sys::Win32::Foundation::{
        HANDLE_FLAG_INHERIT, INVALID_HANDLE_VALUE, SetHandleInformation,
    };
    use windows_sys::Win32::System::Console::{
        GetStdHandle, STD_ERROR_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
    };
    for selector in [STD_INPUT_HANDLE, STD_OUTPUT_HANDLE, STD_ERROR_HANDLE] {
        // SAFETY: selectors are the documented standard handle identifiers.
        let handle = unsafe { GetStdHandle(selector) };
        if handle.is_null() || handle == INVALID_HANDLE_VALUE {
            continue;
        }
        // SAFETY: the handle remains owned by the process; only its inheritance
        // flag changes. No ownership transfer or close occurs.
        if unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn closing_registration_wakes_idle_worker_and_is_terminal() {
        let mut signals = ConsoleSignals::new().unwrap();
        let handle = signals.handle();
        let worker = std::thread::Builder::new()
            .stack_size(64 * 1024)
            .spawn(move || {
                assert!(signals.next().is_none());
                assert!(signals.next().is_none());
            })
            .unwrap();
        handle.close();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !worker.is_finished() {
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        worker.join().unwrap();
    }
}
