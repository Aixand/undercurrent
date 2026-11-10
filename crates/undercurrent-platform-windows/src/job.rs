// SPDX-License-Identifier: Apache-2.0
//! Crash-safe process trees for piped one-shot execution supervisors.
use std::io;
use std::ops::{Deref, DerefMut};
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::os::windows::process::CommandExt;
use std::process::{Child, Command};
use std::sync::Arc;
use windows_sys::Win32::Foundation::{INVALID_HANDLE_VALUE, WAIT_TIMEOUT};
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
    SetInformationJobObject, TerminateJobObject,
};
use windows_sys::Win32::System::Threading::{
    CREATE_NO_WINDOW, CREATE_SUSPENDED, GetProcessIdOfThread, OpenThread, ResumeThread,
    THREAD_QUERY_LIMITED_INFORMATION, THREAD_SUSPEND_RESUME, WaitForSingleObject,
};

/// Cloneable control of the exact retained job, without reopening a PID.
#[derive(Clone)]
pub struct JobControl(Arc<OwnedHandle>);
impl JobControl {
    #[allow(unsafe_code)]
    fn new() -> io::Result<Self> {
        // SAFETY: no borrowed buffers are passed. The new unnamed handle is checked.
        let raw = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if raw.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful creation transfers an owned, non-inheritable job handle.
        let job = Self(Arc::new(unsafe { OwnedHandle::from_raw_handle(raw) }));
        let mut info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // SAFETY: job is retained and info has the advertised layout and size.
        if unsafe {
            SetInformationJobObject(
                job.0.as_raw_handle(),
                JobObjectExtendedLimitInformation,
                (&info as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                std::mem::size_of_val(&info) as u32,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(job)
    }
    /// Requests whole-job termination with exit code 1. Does not wait for exit
    /// or output EOF; OS errors propagate to the caller.
    #[allow(unsafe_code)]
    pub fn terminate(&self) -> io::Result<()> {
        // SAFETY: self retains a live job object. This never opens a process by PID.
        if unsafe { TerminateJobObject(self.0.as_raw_handle(), 1) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

/// Child assigned to a kill-on-close job before its primary thread is resumed.
///
/// Drop attempts whole-job termination even when control clones remain, ignores
/// termination errors, and does not wait. Standard child access supports explicit
/// waiting and stdio handling; root exit alone need not retire descendants.
pub struct JobChild {
    child: Child,
    job: JobControl,
}
impl JobChild {
    /// Spawns suspended without a console window, assigns the job, then verifies
    /// and resumes the unique primary thread. Replaces the command's creation
    /// flags with this launch policy. Setup failure attempts child/job cleanup;
    /// success does not represent a durable execution receipt.
    #[allow(unsafe_code)]
    pub fn spawn(command: &mut Command) -> io::Result<Self> {
        let job = JobControl::new()?;
        command.creation_flags(CREATE_SUSPENDED | CREATE_NO_WINDOW);
        let mut child = command.spawn()?;
        // SAFETY: both handles are owned and live; the primary thread has not run.
        let assigned =
            unsafe { AssignProcessToJobObject(job.0.as_raw_handle(), child.as_raw_handle()) };
        if assigned == 0 {
            let error = io::Error::last_os_error();
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
        let result = Self { child, job };
        result.resume_primary_thread()?;
        Ok(result)
    }
    pub fn control(&self) -> JobControl {
        self.job.clone()
    }
    /// Requests termination of the whole job, not just the root process.
    pub fn kill(&mut self) -> io::Result<()> {
        self.job.terminate()
    }

    #[allow(unsafe_code)]
    fn resume_primary_thread(&self) -> io::Result<()> {
        // std::process::Child owns the process but closes the primary-thread
        // handle. Before any user code runs, require exactly one suspended thread.
        // SAFETY: snapshot creation takes scalar flags only; returned handle is checked.
        let raw = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
        if raw == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the snapshot is uniquely owned and closed automatically.
        let snapshot = unsafe { OwnedHandle::from_raw_handle(raw) };
        let mut entry = THREADENTRY32 {
            dwSize: std::mem::size_of::<THREADENTRY32>() as u32,
            ..Default::default()
        };
        // SAFETY: snapshot is valid and entry declares its actual writable size.
        let mut available = unsafe { Thread32First(snapshot.as_raw_handle(), &mut entry) } != 0;
        let mut primary = None;
        let mut count = 0usize;
        while available {
            count += 1;
            if count > 1_000_000 {
                return Err(io::Error::other("thread snapshot exceeds bound"));
            }
            if entry.th32OwnerProcessID == self.child.id()
                && primary.replace(entry.th32ThreadID).is_some()
            {
                return Err(io::Error::other("suspended process has multiple threads"));
            }
            // SAFETY: snapshot and entry remain valid for the enumeration.
            available = unsafe { Thread32Next(snapshot.as_raw_handle(), &mut entry) } != 0;
        }
        let end = io::Error::last_os_error();
        if end.raw_os_error() != Some(18) {
            // ERROR_NO_MORE_FILES
            return Err(end);
        }
        let primary =
            primary.ok_or_else(|| io::Error::other("suspended primary thread disappeared"))?;
        // SAFETY: OpenThread takes an integer ID and the returned handle is checked.
        let raw = unsafe {
            OpenThread(
                THREAD_SUSPEND_RESUME | THREAD_QUERY_LIMITED_INFORMATION,
                0,
                primary,
            )
        };
        if raw.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: this is a uniquely owned thread handle.
        let thread = unsafe { OwnedHandle::from_raw_handle(raw) };
        // SAFETY: both handles remain live; the bounded query verifies that the
        // retained root is alive and this thread still belongs to that root.
        let valid = unsafe {
            GetProcessIdOfThread(thread.as_raw_handle()) == self.child.id()
                && WaitForSingleObject(self.child.as_raw_handle(), 0) == WAIT_TIMEOUT
        };
        if !valid {
            return Err(io::Error::other("suspended process identity changed"));
        }
        // SAFETY: thread is the retained, validated primary thread of the assigned child.
        if unsafe { ResumeThread(thread.as_raw_handle()) } != 1 {
            return Err(io::Error::other(
                "primary thread was not suspended exactly once",
            ));
        }
        Ok(())
    }
}
impl Deref for JobChild {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.child
    }
}
impl DerefMut for JobChild {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.child
    }
}
impl Drop for JobChild {
    fn drop(&mut self) {
        let _ = self.job.terminate();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Stdio;
    use std::time::{Duration, Instant};

    #[test]
    fn piped_command_runs_and_drop_terminates_retained_process() {
        let mut command = Command::new("cmd.exe");
        command
            .args(["/D", "/Q", "/K"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let child = JobChild::spawn(&mut command).unwrap();
        let process = crate::ProcessHandle::open(child.id()).unwrap();
        assert!(process.is_alive().unwrap());
        drop(child);
        let deadline = Instant::now() + Duration::from_secs(3);
        while process.is_alive().unwrap() {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn command_exit_status_is_preserved() {
        let mut command = Command::new("cmd.exe");
        command
            .args(["/D", "/C", "exit 17"])
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut child = JobChild::spawn(&mut command).unwrap();
        assert_eq!(child.wait().unwrap().code(), Some(17));
    }

    #[test]
    fn observer_retains_exact_job_members_and_detects_tree_changes() {
        use std::io::Write;
        let mut command = Command::new(crate::command_interpreter().unwrap());
        command
            .args(["/D", "/Q", "/K"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut child = JobChild::spawn(&mut command).unwrap();
        let observer =
            crate::JobProcessObserver::from_owned_handle(child.job.0.try_clone().unwrap());
        let root = crate::ProcessHandle::open(child.id()).unwrap();
        assert!(observer.contains(&root).unwrap());
        assert!(
            !observer
                .contains(&crate::ProcessHandle::open(std::process::id()).unwrap())
                .unwrap()
        );
        let initial = observer.snapshot().unwrap();
        assert!(initial.complete());
        assert_eq!(initial.processes().len(), 1);
        assert!(observer.is_current(&initial).unwrap());
        child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(b"start \"\" /b cmd.exe /d /q /k\r\n")
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let expanded = loop {
            let snapshot = observer.snapshot().unwrap();
            if snapshot.complete() && snapshot.processes().len() >= 2 {
                break snapshot;
            }
            assert!(Instant::now() < deadline, "descendant did not join the job");
            std::thread::sleep(Duration::from_millis(10));
        };
        assert!(!observer.is_current(&initial).unwrap());
        assert!(observer.is_current(&expanded).unwrap());
        drop(child);
        let deadline = Instant::now() + Duration::from_secs(5);
        while observer.is_current(&expanded).unwrap() {
            assert!(
                Instant::now() < deadline,
                "terminated membership remained live"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
