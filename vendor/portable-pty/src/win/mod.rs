use crate::{Child, ChildKiller, ExitStatus};
use anyhow::Context as _;
use std::io::{Error as IoError, Result as IoResult};
use std::os::windows::io::{AsRawHandle, RawHandle};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use winapi::shared::minwindef::DWORD;
use winapi::um::minwinbase::STILL_ACTIVE;
use winapi::um::processthreadsapi::*;
use winapi::um::synchapi::WaitForSingleObject;
use winapi::um::winbase::INFINITE;

pub mod conpty;
mod job;
mod procthreadattr;
mod psuedocon;

use filedescriptor::OwnedHandle;

#[derive(Debug)]
pub struct WinChild {
    proc: Mutex<OwnedHandle>,
    job: Option<Arc<job::ProcessJob>>,
}

impl WinChild {
    fn is_complete(&mut self) -> IoResult<Option<ExitStatus>> {
        let mut status: DWORD = 0;
        let proc = self.proc.lock().unwrap().try_clone().unwrap();
        let res = unsafe { GetExitCodeProcess(proc.as_raw_handle() as _, &mut status) };
        if res != 0 {
            if status == STILL_ACTIVE {
                Ok(None)
            } else {
                Ok(Some(ExitStatus::with_exit_code(status)))
            }
        } else {
            Err(IoError::last_os_error())
        }
    }

    fn do_kill(&mut self) -> IoResult<()> {
        if let Some(job) = &self.job {
            return job.terminate();
        }
        let proc = self.proc.lock().unwrap().try_clone().unwrap();
        let res = unsafe { TerminateProcess(proc.as_raw_handle() as _, 1) };
        let err = IoError::last_os_error();
        if res == 0 {
            Err(err)
        } else {
            Ok(())
        }
    }
}

impl ChildKiller for WinChild {
    fn clone_job_observer(&self) -> IoResult<Option<std::os::windows::io::OwnedHandle>> {
        self.job
            .as_ref()
            .map(|job| job.clone_observer())
            .transpose()
    }
    fn kill(&mut self) -> IoResult<()> {
        self.do_kill()
    }

    fn clone_killer(&self) -> Box<dyn ChildKiller + Send + Sync> {
        let proc = self.proc.lock().unwrap().try_clone().unwrap();
        Box::new(WinChildKiller {
            proc,
            job: self.job.clone(),
        })
    }
}

#[derive(Debug)]
pub struct WinChildKiller {
    proc: OwnedHandle,
    job: Option<Arc<job::ProcessJob>>,
}

impl ChildKiller for WinChildKiller {
    fn clone_job_observer(&self) -> IoResult<Option<std::os::windows::io::OwnedHandle>> {
        self.job
            .as_ref()
            .map(|job| job.clone_observer())
            .transpose()
    }
    fn kill(&mut self) -> IoResult<()> {
        if let Some(job) = &self.job {
            return job.terminate();
        }
        let res = unsafe { TerminateProcess(self.proc.as_raw_handle() as _, 1) };
        let err = IoError::last_os_error();
        if res == 0 {
            Err(err)
        } else {
            Ok(())
        }
    }

    fn clone_killer(&self) -> Box<dyn ChildKiller + Send + Sync> {
        let proc = self.proc.try_clone().unwrap();
        Box::new(WinChildKiller {
            proc,
            job: self.job.clone(),
        })
    }
}

impl Child for WinChild {
    fn try_wait(&mut self) -> IoResult<Option<ExitStatus>> {
        self.is_complete()
    }

    fn wait(&mut self) -> IoResult<ExitStatus> {
        if let Ok(Some(status)) = self.try_wait() {
            return Ok(status);
        }
        let proc = self.proc.lock().unwrap().try_clone().unwrap();
        unsafe {
            WaitForSingleObject(proc.as_raw_handle() as _, INFINITE);
        }
        let mut status: DWORD = 0;
        let res = unsafe { GetExitCodeProcess(proc.as_raw_handle() as _, &mut status) };
        if res != 0 {
            Ok(ExitStatus::with_exit_code(status))
        } else {
            Err(IoError::last_os_error())
        }
    }

    fn process_id(&self) -> Option<u32> {
        let res = unsafe { GetProcessId(self.proc.lock().unwrap().as_raw_handle() as _) };
        if res == 0 {
            None
        } else {
            Some(res)
        }
    }

    fn as_raw_handle(&self) -> Option<std::os::windows::io::RawHandle> {
        let proc = self.proc.lock().unwrap();
        Some(proc.as_raw_handle())
    }
}

impl std::future::Future for WinChild {
    type Output = anyhow::Result<ExitStatus>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<anyhow::Result<ExitStatus>> {
        match self.is_complete() {
            Ok(Some(status)) => Poll::Ready(Ok(status)),
            Err(err) => Poll::Ready(Err(err).context("Failed to retrieve process exit status")),
            Ok(None) => {
                struct PassRawHandleToWaiterThread(pub RawHandle);
                unsafe impl Send for PassRawHandleToWaiterThread {}

                let proc = self.proc.lock().unwrap().try_clone()?;
                let handle = PassRawHandleToWaiterThread(proc.as_raw_handle());

                let waker = cx.waker().clone();
                std::thread::spawn(move || {
                    unsafe {
                        WaitForSingleObject(handle.0 as _, INFINITE);
                    }
                    waker.wake();
                });
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Command, Stdio};

    #[test]
    fn termination_reports_success_for_child_and_cloned_killer() {
        for cloned in [false, true] {
            let mut process = Command::new("cmd.exe")
                .args(["/D", "/Q", "/K"])
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            let mut child = WinChild {
                proc: Mutex::new(OwnedHandle::dup(&process).unwrap()),
                job: None,
            };
            let result = if cloned {
                child.clone_killer().kill()
            } else {
                child.kill()
            };
            // Always clean up, including when the assertion fails.
            let _ = process.kill();
            process.wait().unwrap();
            assert!(result.is_ok(), "termination failed: {:?}", result);
        }
    }

    #[test]
    fn invalid_process_handles_propagate_query_and_termination_errors() {
        // A file handle is valid and owned, but is not a process handle.
        let file = std::fs::File::open(std::env::current_exe().unwrap()).unwrap();
        let mut child = WinChild {
            proc: Mutex::new(OwnedHandle::dup(&file).unwrap()),
            job: None,
        };
        assert!(child.try_wait().is_err());
        assert!(child.kill().is_err());
        assert!(child.clone_killer().kill().is_err());
    }

    #[test]
    fn process_tree_fixture() {
        let Ok(stage) = std::env::var("UC_PTY_JOB_FIXTURE") else {
            return;
        };
        if stage == "root" {
            let child = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "win::tests::process_tree_fixture", "--nocapture"])
                .env("UC_PTY_JOB_FIXTURE", "leaf")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            std::fs::write(
                std::env::var_os("UC_PTY_JOB_MARKER").unwrap(),
                child.id().to_string(),
            )
            .unwrap();
        }
        loop {
            std::thread::park();
        }
    }

    #[test]
    fn conpty_killer_terminates_root_and_descendant() {
        use crate::{CommandBuilder, PtySize};
        use std::io::Write;
        use std::time::{Duration, Instant};
        use winapi::um::winnt::SYNCHRONIZE;
        let marker = std::env::temp_dir().join(format!("uc-pty-job-{}.pid", std::process::id()));
        assert!(
            !marker.exists(),
            "stale fixture marker must be investigated"
        );
        let pair = crate::native_pty_system()
            .openpty(PtySize::default())
            .unwrap();
        let mut reader = pair.master.try_clone_reader().unwrap();
        let drain = std::thread::spawn(move || std::io::copy(&mut reader, &mut std::io::sink()));
        let mut writer = pair.master.take_writer().unwrap();
        let mut command = CommandBuilder::new(std::env::current_exe().unwrap());
        command.args(["--exact", "win::tests::process_tree_fixture", "--nocapture"]);
        command.env("UC_PTY_JOB_FIXTURE", "root");
        command.env("UC_PTY_JOB_MARKER", marker.as_os_str());
        let mut child = pair.slave.spawn_command(command).unwrap();
        let observer = child.clone_killer().clone_job_observer().unwrap().unwrap();
        // SAFETY: the accounting structure consists of integer fields; zero is
        // a valid initialization before the kernel fills the output buffer.
        let mut accounting: winapi::um::winnt::JOBOBJECT_BASIC_ACCOUNTING_INFORMATION =
            unsafe { std::mem::zeroed() };
        // SAFETY: observer is retained and accounting is writable for its exact size.
        assert_ne!(
            unsafe {
                winapi::um::jobapi2::QueryInformationJobObject(
                    observer.as_raw_handle() as _,
                    winapi::um::winnt::JobObjectBasicAccountingInformation,
                    &mut accounting as *mut _ as _,
                    std::mem::size_of_val(&accounting) as u32,
                    std::ptr::null_mut(),
                )
            },
            0
        );
        assert!(accounting.ActiveProcesses >= 1);
        let mut flags = 0;
        // SAFETY: the retained handle is valid and flags is a writable DWORD.
        assert_ne!(
            unsafe {
                winapi::um::handleapi::GetHandleInformation(
                    observer.as_raw_handle() as _,
                    &mut flags,
                )
            },
            0
        );
        assert_eq!(flags & 1, 0, "observer must not be inheritable");
        // SAFETY: observer is a retained job handle; this deliberately tests
        // that the query-only duplicate cannot terminate the fixture's tree.
        let denied =
            unsafe { winapi::um::jobapi2::TerminateJobObject(observer.as_raw_handle() as _, 1) };
        assert_eq!(denied, 0);
        assert_eq!(IoError::last_os_error().raw_os_error(), Some(5));
        drop(observer);
        // Answer ConPTY's initial cursor-position query without a terminal frontend.
        writer.write_all(b"\x1b[1;1R").unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let descendant = loop {
            if let Ok(text) = std::fs::read_to_string(&marker) {
                if let Ok(pid) = text.parse::<u32>() {
                    break Some(pid);
                }
            }
            if Instant::now() >= deadline {
                break None;
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        // Retain the descendant handle before termination to avoid PID reuse races.
        let descendant = descendant.map(|pid| {
            // SAFETY: OpenProcess takes an integer PID; the handle is checked.
            let raw = unsafe { OpenProcess(SYNCHRONIZE, 0, pid) };
            assert!(!raw.is_null());
            // SAFETY: newly returned process handle is uniquely owned.
            unsafe {
                <OwnedHandle as std::os::windows::io::FromRawHandle>::from_raw_handle(raw as _)
            }
        });
        let result = child.clone_killer().kill();
        let deadline = Instant::now() + Duration::from_secs(5);
        let exited = loop {
            if child.try_wait().unwrap().is_some() {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        let descendant_exited = descendant.as_ref().map(|process| {
            // SAFETY: the retained handle has SYNCHRONIZE rights; the wait is bounded.
            unsafe { WaitForSingleObject(process.as_raw_handle() as _, 5000) == 0 }
        });
        drop((child, writer, pair));
        drain.join().unwrap().unwrap();
        if marker.exists() {
            std::fs::remove_file(&marker).unwrap();
        }
        assert!(result.is_ok());
        assert!(exited, "root process survived tree termination");
        assert_eq!(
            descendant_exited,
            Some(true),
            "descendant must start and be terminated"
        );
    }
}
pub(crate) mod batch;
