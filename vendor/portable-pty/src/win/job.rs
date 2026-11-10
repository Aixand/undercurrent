//! Retained job ownership for one PTY process tree.
use filedescriptor::OwnedHandle;
use std::io;
use std::os::windows::io::{AsRawHandle, FromRawHandle};
use winapi::um::jobapi2::{
    AssignProcessToJobObject, CreateJobObjectW, SetInformationJobObject, TerminateJobObject,
};
use winapi::um::winnt::{
    JobObjectExtendedLimitInformation, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
};

#[derive(Debug)]
pub(super) struct ProcessJob(OwnedHandle);

impl ProcessJob {
    pub(super) fn clone_observer(&self) -> io::Result<std::os::windows::io::OwnedHandle> {
        use winapi::um::handleapi::DuplicateHandle;
        use winapi::um::processthreadsapi::GetCurrentProcess;
        use winapi::um::winnt::JOB_OBJECT_QUERY;
        let mut observer = std::ptr::null_mut();
        // SAFETY: the source job is retained, the process pseudo handles are
        // valid for this call, and observer is writable. No handle is inherited.
        if unsafe {
            DuplicateHandle(
                GetCurrentProcess(),
                self.0.as_raw_handle() as _,
                GetCurrentProcess(),
                &mut observer,
                JOB_OBJECT_QUERY,
                0,
                0,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful duplication transferred one new owned query handle.
        Ok(unsafe { std::os::windows::io::OwnedHandle::from_raw_handle(observer as _) })
    }

    pub(super) fn new() -> io::Result<Self> {
        // SAFETY: no security attributes or name are supplied; the returned
        // unnamed job handle is non-inheritable and checked before ownership.
        let raw = unsafe { CreateJobObjectW(std::ptr::null_mut(), std::ptr::null()) };
        if raw.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful creation transferred a new job handle.
        let job = Self(unsafe { OwnedHandle::from_raw_handle(raw as _) });
        // SAFETY: this C struct consists of integer and pointer-sized fields for
        // which zero represents the documented default limits.
        let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // SAFETY: job is live and limits has the exact type and size requested.
        if unsafe {
            SetInformationJobObject(
                job.0.as_raw_handle() as _,
                JobObjectExtendedLimitInformation,
                &mut limits as *mut _ as _,
                std::mem::size_of_val(&limits) as u32,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(job)
    }

    /// The launcher assigns the root while its primary thread is still suspended.
    pub(super) fn assign(&self, process: &OwnedHandle) -> io::Result<()> {
        // SAFETY: both retained handles are live for the complete call.
        if unsafe {
            AssignProcessToJobObject(self.0.as_raw_handle() as _, process.as_raw_handle() as _)
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub(super) fn terminate(&self) -> io::Result<()> {
        // SAFETY: self owns a valid job handle; the call retains no pointers.
        if unsafe { TerminateJobObject(self.0.as_raw_handle() as _, 1) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}
