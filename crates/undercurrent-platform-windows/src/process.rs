// SPDX-License-Identifier: Apache-2.0
//! Retained process identities, bounded ancestry queries, and OS-selected paths.
//!
//! Handles identify kernel objects across PID reuse but do not keep processes
//! alive. Callers recheck liveness around collections of related observations.
use std::io;
use std::os::windows::ffi::OsStringExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use windows_sys::Win32::Foundation::{FILETIME, WAIT_OBJECT_0, WAIT_TIMEOUT};
use windows_sys::Win32::System::Threading::{
    GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE,
    WaitForSingleObject,
};

/// Select the OS command interpreter without consulting PATH or COMSPEC.
#[allow(unsafe_code)]
pub fn command_interpreter() -> io::Result<std::path::PathBuf> {
    let mut buffer = vec![0u16; 32768];
    // SAFETY: buffer is writable for the declared number of UTF-16 units.
    let length = unsafe {
        windows_sys::Win32::System::SystemInformation::GetSystemDirectoryW(
            buffer.as_mut_ptr(),
            buffer.len() as u32,
        )
    };
    if length == 0 {
        return Err(io::Error::last_os_error());
    }
    if length as usize >= buffer.len() {
        return Err(io::Error::other("system directory exceeds bound"));
    }
    buffer.truncate(length as usize);
    Ok(std::path::PathBuf::from(std::ffi::OsString::from_wide(&buffer)).join("cmd.exe"))
}

/// Windows loader and command-interpreter prerequisite for isolated environments.
#[allow(unsafe_code)]
pub fn windows_directory() -> io::Result<std::path::PathBuf> {
    let mut buffer = vec![0u16; 32768];
    // SAFETY: buffer is writable for the declared number of UTF-16 units.
    let length = unsafe {
        windows_sys::Win32::System::SystemInformation::GetWindowsDirectoryW(
            buffer.as_mut_ptr(),
            buffer.len() as u32,
        )
    };
    if length == 0 {
        return Err(io::Error::last_os_error());
    }
    if length as usize >= buffer.len() {
        return Err(io::Error::other("Windows directory exceeds bound"));
    }
    buffer.truncate(length as usize);
    Ok(std::ffi::OsString::from_wide(&buffer).into())
}

/// Creation time is an opaque 100-nanosecond Windows timestamp, not a PID alone.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub created: u64,
}

/// Pins a process object across PID reuse. No termination rights are requested.
pub struct ProcessHandle {
    handle: OwnedHandle,
    identity: ProcessIdentity,
}

impl ProcessHandle {
    #[allow(unsafe_code)]
    pub(crate) fn belongs_to_job(&self, job: &OwnedHandle) -> io::Result<bool> {
        let mut belongs = 0;
        // SAFETY: both objects are retained; process grants limited query rights
        // and the job query handle is validated by the kernel. Output is writable.
        if unsafe {
            windows_sys::Win32::System::JobObjects::IsProcessInJob(
                self.handle.as_raw_handle(),
                job.as_raw_handle(),
                &mut belongs,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(belongs != 0)
    }
    /// Reads the executable of this retained process object, not a reopened PID.
    #[allow(unsafe_code)]
    pub fn executable_path(&self) -> io::Result<std::path::PathBuf> {
        let mut buffer = vec![0u16; 32768];
        let mut length = buffer.len() as u32;
        // SAFETY: the retained handle grants limited query access and buffer has
        // length UTF-16 slots. The synchronous API does not retain the pointer.
        if unsafe {
            windows_sys::Win32::System::Threading::QueryFullProcessImageNameW(
                self.handle.as_raw_handle(),
                0,
                buffer.as_mut_ptr(),
                &mut length,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        buffer.truncate(length as usize);
        Ok(std::ffi::OsString::from_wide(&buffer).into())
    }

    /// Compares the recorded PID's process-token user with this process's account.
    ///
    /// The token query opens by PID rather than using the retained handle. This
    /// method alone does not prove liveness or atomically validate an identity;
    /// callers collecting security evidence must also recheck the retained object.
    pub fn is_current_user(&self) -> io::Result<bool> {
        Ok(crate::security::process_user_sid(self.identity.pid)? == crate::current_user_sid()?)
    }

    /// Windows retains a numeric creator PID even after that process exits.
    /// Reject a reused PID by comparing creation times before returning a handle.
    #[allow(unsafe_code)]
    pub fn parent(&self) -> io::Result<Option<Self>> {
        use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
        use windows_sys::Win32::System::Diagnostics::ToolHelp::{
            CreateToolhelp32Snapshot, PROCESSENTRY32W, Process32FirstW, Process32NextW,
            TH32CS_SNAPPROCESS,
        };
        if !self.is_alive()? {
            return Err(io::Error::new(io::ErrorKind::NotFound, "process exited"));
        }
        // SAFETY: scalar snapshot flags only; the returned handle is checked.
        let raw = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
        if raw == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: snapshot creation returned a uniquely owned live handle.
        let snapshot = unsafe { OwnedHandle::from_raw_handle(raw) };
        let mut entry = PROCESSENTRY32W {
            dwSize: std::mem::size_of::<PROCESSENTRY32W>() as u32,
            ..Default::default()
        };
        // SAFETY: snapshot is live; entry is writable and declares its actual size.
        let mut found = unsafe { Process32FirstW(snapshot.as_raw_handle(), &mut entry) } != 0;
        let mut count = 0;
        while found {
            count += 1;
            if count > 65536 {
                return Err(io::Error::other("process snapshot exceeds bound"));
            }
            if entry.th32ProcessID == self.identity.pid {
                if entry.th32ParentProcessID == 0 {
                    return Ok(None);
                }
                let parent = Self::open(entry.th32ParentProcessID)?;
                if parent.identity.created > self.identity.created
                    || parent.identity.pid == self.identity.pid
                    || !parent.is_alive()?
                    || !self.is_alive()?
                {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "parent process identity changed",
                    ));
                }
                return Ok(Some(parent));
            }
            // SAFETY: snapshot and initialized output entry remain live.
            found = unsafe { Process32NextW(snapshot.as_raw_handle(), &mut entry) } != 0;
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(18) {
            return Err(error);
        }
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "process absent from snapshot",
        ))
    }

    /// Opens a process for limited queries and synchronization and records its
    /// creation time. PID zero is invalid; OS errors propagate. Opening does not
    /// itself assert that the process is still alive.
    #[allow(unsafe_code)]
    pub fn open(pid: u32) -> io::Result<Self> {
        if pid == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid process ID",
            ));
        }
        // SAFETY: OpenProcess accepts an integer PID; the returned handle is checked
        // and transferred exactly once into OwnedHandle for automatic closure.
        let raw = unsafe {
            OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
                0,
                pid,
            )
        };
        if raw.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: raw is a newly owned, non-null process handle.
        let handle = unsafe { OwnedHandle::from_raw_handle(raw) };
        let mut created = FILETIME::default();
        let mut exited = FILETIME::default();
        let mut kernel = FILETIME::default();
        let mut user = FILETIME::default();
        // SAFETY: the handle remains live and each output points to a distinct,
        // correctly aligned FILETIME. Windows retains none of these pointers.
        if unsafe {
            GetProcessTimes(
                handle.as_raw_handle(),
                &mut created,
                &mut exited,
                &mut kernel,
                &mut user,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            handle,
            identity: ProcessIdentity {
                pid,
                created: (u64::from(created.dwHighDateTime) << 32)
                    | u64::from(created.dwLowDateTime),
            },
        })
    }

    /// Returns the identity captured at open, without querying current liveness.
    pub const fn identity(&self) -> ProcessIdentity {
        self.identity
    }

    /// Checks the retained object, never a newly opened process with the same PID.
    #[allow(unsafe_code)]
    pub fn is_alive(&self) -> io::Result<bool> {
        // SAFETY: self retains the process handle with SYNCHRONIZE access;
        // a zero timeout performs a nonblocking query.
        match unsafe { WaitForSingleObject(self.handle.as_raw_handle(), 0) } {
            WAIT_OBJECT_0 => Ok(false),
            WAIT_TIMEOUT => Ok(true),
            _ => Err(io::Error::last_os_error()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Command, Stdio};

    #[test]
    fn identity_is_stable_and_retained_handle_observes_exit() {
        let mut child = Command::new("cmd.exe")
            .args(["/D", "/Q", "/K"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let observed = ProcessHandle::open(child.id());
        let again = ProcessHandle::open(child.id());
        let alive = observed.as_ref().is_ok_and(|p| p.is_alive().unwrap());
        let parent = observed.as_ref().unwrap().parent();
        let executable = observed.as_ref().unwrap().executable_path();
        let owner = observed.as_ref().unwrap().is_current_user();
        child.kill().unwrap();
        child.wait().unwrap();
        let observed = observed.unwrap();
        assert!(alive);
        assert_eq!(observed.identity(), again.unwrap().identity());
        assert_ne!(observed.identity().created, 0);
        assert!(!observed.is_alive().unwrap());
        assert_eq!(parent.unwrap().unwrap().identity().pid, std::process::id());
        assert!(
            executable
                .unwrap()
                .file_name()
                .unwrap()
                .eq_ignore_ascii_case("cmd.exe")
        );
        assert!(owner.unwrap());
        assert!(observed.parent().is_err());
    }

    #[test]
    fn invalid_pid_fails_closed() {
        assert!(ProcessHandle::open(0).is_err());
        assert!(ProcessHandle::open(u32::MAX).is_err());
    }
}
