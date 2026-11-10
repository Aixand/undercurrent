// SPDX-License-Identifier: Apache-2.0

//! Operating-system termination primitives for native supervised processes.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;
use std::sync::Mutex;

use portable_pty::ChildKiller;
use undercurrent_domain::StopSignal;

#[cfg(target_os = "linux")]
pub(super) fn process_start_ticks(pid: u32) -> io::Result<Option<u64>> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let fields = stat
        .rsplit_once(')')
        .map(|(_, fields)| fields)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid process stat"))?;
    fields
        .split_whitespace()
        .nth(19)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing process start time"))?
        .parse::<u64>()
        .map(Some)
        .map_err(|source| io::Error::new(io::ErrorKind::InvalidData, source))
}

/// Opaque process incarnation: Darwin uses microseconds since Unix epoch;
/// Linux retains its existing start-ticks representation.
#[cfg(target_os = "macos")]
pub(super) fn process_start_ticks(pid: u32) -> io::Result<Option<u64>> {
    undercurrent_platform_macos::identity(pid).map(|identity| Some(identity.start_micros))
}

#[cfg(windows)]
pub(super) fn process_start_ticks(pid: u32) -> io::Result<Option<u64>> {
    undercurrent_platform_windows::ProcessHandle::open(pid)
        .map(|process| Some(process.identity().created))
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub(super) const fn process_start_ticks(_pid: u32) -> io::Result<Option<u64>> {
    Ok(None)
}

/// Payload-safe process signalling failure.
#[derive(Debug)]
pub(super) enum ProcessSignalError {
    /// Unix process-group and root-process signalling both failed.
    #[cfg(unix)]
    Unix {
        /// Concrete operating-system error.
        source: nix::errno::Errno,
    },
    /// Portable forced termination failed.
    Portable {
        /// Concrete portable child-handle error.
        source: std::io::Error,
    },
    /// A thread panicked while owning the portable killer.
    KillerLockPoisoned,
    /// The platform cannot express the requested graceful signal.
    #[cfg(not(unix))]
    GracefulUnsupported,
}

impl Display for ProcessSignalError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(unix)]
            Self::Unix { .. } => formatter.write_str("could not signal native process group"),
            Self::Portable { .. } => formatter.write_str("could not terminate native process"),
            Self::KillerLockPoisoned => {
                formatter.write_str("native process killer lock is poisoned")
            }
            #[cfg(not(unix))]
            Self::GracefulUnsupported => {
                formatter.write_str("graceful process signals are unsupported on this platform")
            }
        }
    }
}

impl Error for ProcessSignalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            #[cfg(unix)]
            Self::Unix { source } => Some(source),
            Self::Portable { source } => Some(source),
            Self::KillerLockPoisoned => None,
            #[cfg(not(unix))]
            Self::GracefulUnsupported => None,
        }
    }
}

/// Darwin can revoke the controlling terminal when its session leader exits,
/// producing PTY EOF while members of the supervised group are still alive.
#[cfg(target_os = "macos")]
pub(super) fn process_group_exists(pid: u32) -> Result<bool, ProcessSignalError> {
    let pid = i32::try_from(pid)
        .ok()
        .filter(|pid| *pid > 0)
        .ok_or(ProcessSignalError::Unix {
            source: nix::errno::Errno::EINVAL,
        })?;
    match nix::sys::signal::kill(nix::unistd::Pid::from_raw(-pid), None) {
        // Darwin can return EPERM while an exiting group's last members are
        // zombies. Denied signalling also proves presence, never absence.
        Ok(()) | Err(nix::errno::Errno::EPERM) => Ok(true),
        Err(nix::errno::Errno::ESRCH) => Ok(false),
        Err(source) => Err(ProcessSignalError::Unix { source }),
    }
}

/// Attempts the configured graceful signal on the process group, falling back
/// to the root PID if the group is absent. Exit confirmation belongs to the caller.
#[cfg(unix)]
pub(super) fn send_graceful(
    pid: u32,
    signal: StopSignal,
    _killer: &Mutex<Box<dyn ChildKiller + Send + Sync>>,
) -> Result<(), ProcessSignalError> {
    send_unix(pid, unix_signal(signal))
}

/// Reports unsupported graceful signalling on platforms without POSIX signals.
#[cfg(not(unix))]
pub(super) fn send_graceful(
    _pid: u32,
    _signal: StopSignal,
    _killer: &Mutex<Box<dyn ChildKiller + Send + Sync>>,
) -> Result<(), ProcessSignalError> {
    Err(ProcessSignalError::GracefulUnsupported)
}

/// Forcibly terminates the complete process group where available.
#[cfg(unix)]
pub(super) fn send_force(
    pid: u32,
    killer: &Mutex<Box<dyn ChildKiller + Send + Sync>>,
) -> Result<(), ProcessSignalError> {
    match send_unix(pid, nix::sys::signal::Signal::SIGKILL) {
        Ok(()) => Ok(()),
        Err(ProcessSignalError::Unix {
            source: nix::errno::Errno::ESRCH,
        }) => kill_portable(killer),
        Err(error) => Err(error),
    }
}

/// Delegates force termination to the portable killer. On Windows this targets
/// the retained job when available, otherwise the root. The caller confirms exit.
#[cfg(not(unix))]
pub(super) fn send_force(
    _pid: u32,
    killer: &Mutex<Box<dyn ChildKiller + Send + Sync>>,
) -> Result<(), ProcessSignalError> {
    kill_portable(killer)
}

// Once the root has been reaped its numeric PID can be reused. Descendant
// cleanup must target only the retained group/job, never that root PID.
pub(super) fn send_force_descendants(
    pid: u32,
    killer: &Mutex<Box<dyn ChildKiller + Send + Sync>>,
) -> Result<(), ProcessSignalError> {
    #[cfg(unix)]
    {
        let _ = killer;
        let pid = i32::try_from(pid).map_err(|_| ProcessSignalError::Unix {
            source: nix::errno::Errno::EINVAL,
        })?;
        match nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(-pid),
            nix::sys::signal::Signal::SIGKILL,
        ) {
            Ok(()) | Err(nix::errno::Errno::ESRCH) => Ok(()),
            Err(source) => Err(ProcessSignalError::Unix { source }),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        kill_portable(killer)
    }
}

fn kill_portable(
    killer: &Mutex<Box<dyn ChildKiller + Send + Sync>>,
) -> Result<(), ProcessSignalError> {
    killer
        .lock()
        .map_err(|_| ProcessSignalError::KillerLockPoisoned)?
        .kill()
        .map_err(|source| ProcessSignalError::Portable { source })
}

#[cfg(unix)]
fn send_unix(pid: u32, signal: nix::sys::signal::Signal) -> Result<(), ProcessSignalError> {
    let pid = i32::try_from(pid).map_err(|_| ProcessSignalError::Unix {
        source: nix::errno::Errno::EINVAL,
    })?;
    let process = nix::unistd::Pid::from_raw(pid);
    let group = nix::unistd::Pid::from_raw(-pid);
    match nix::sys::signal::kill(group, signal) {
        Ok(()) => Ok(()),
        Err(nix::errno::Errno::ESRCH) => nix::sys::signal::kill(process, signal)
            .map_err(|source| ProcessSignalError::Unix { source }),
        Err(source) => Err(ProcessSignalError::Unix { source }),
    }
}

#[cfg(unix)]
const fn unix_signal(signal: StopSignal) -> nix::sys::signal::Signal {
    match signal {
        StopSignal::Interrupt => nix::sys::signal::Signal::SIGINT,
        StopSignal::Terminate => nix::sys::signal::Signal::SIGTERM,
        StopSignal::Hangup => nix::sys::signal::Signal::SIGHUP,
        StopSignal::Quit => nix::sys::signal::Signal::SIGQUIT,
        StopSignal::User1 => nix::sys::signal::Signal::SIGUSR1,
        StopSignal::User2 => nix::sys::signal::Signal::SIGUSR2,
    }
}
