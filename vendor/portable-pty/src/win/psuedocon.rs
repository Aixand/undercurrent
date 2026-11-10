use super::WinChild;
use crate::cmdbuilder::CommandBuilder;
use crate::win::procthreadattr::ProcThreadAttributeList;
use anyhow::{bail, ensure, Error};
use filedescriptor::{FileDescriptor, OwnedHandle};
use lazy_static::lazy_static;
use shared_library::shared_library;
use std::ffi::OsString;
use std::io::Error as IoError;
use std::os::windows::ffi::OsStringExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle};
use std::sync::{Arc, Mutex};
use std::{mem, ptr};
use winapi::shared::minwindef::DWORD;
use winapi::shared::winerror::{HRESULT, S_OK};
use winapi::um::handleapi::*;
use winapi::um::processthreadsapi::*;
use winapi::um::winbase::{
    CREATE_SUSPENDED, CREATE_UNICODE_ENVIRONMENT, EXTENDED_STARTUPINFO_PRESENT,
    STARTF_USESTDHANDLES, STARTUPINFOEXW,
};
use winapi::um::wincon::COORD;
use winapi::um::winnt::HANDLE;

pub type HPCON = HANDLE;

pub const PSUEDOCONSOLE_INHERIT_CURSOR: DWORD = 0x1;
pub const PSEUDOCONSOLE_RESIZE_QUIRK: DWORD = 0x2;
pub const PSEUDOCONSOLE_WIN32_INPUT_MODE: DWORD = 0x4;
#[allow(dead_code)]
pub const PSEUDOCONSOLE_PASSTHROUGH_MODE: DWORD = 0x8;

shared_library!(ConPtyFuncs,
    pub fn ConptyCreatePseudoConsole(
        size: COORD,
        hInput: HANDLE,
        hOutput: HANDLE,
        flags: DWORD,
        hpc: *mut HPCON
    ) -> HRESULT,
    pub fn ConptyResizePseudoConsole(hpc: HPCON, size: COORD) -> HRESULT,
    pub fn ConptyClosePseudoConsole(hpc: HPCON),
);

fn load_conpty() -> Result<ConPtyFuncs, String> {
    // Server 2022's system ClosePseudoConsole retains a conhost process handle
    // per console. Use Microsoft's serviced runtime for installed and Cargo
    // builds alike. Resolve it only beside this executable, never through PATH
    // or the working directory, and report missing/incompatible assets.
    let executable = std::env::current_exe().map_err(|error| error.to_string())?;
    let library = executable
        .parent()
        .ok_or("executable has no parent")?
        .join("uc-conpty/conpty.dll");
    let console = library.with_file_name("OpenConsole.exe");
    if !console.is_file() {
        // The DLL itself falls back to the system conhost when this is absent.
        // Reject that incomplete installation instead of restoring the OS leak.
        return Err(format!(
            "required ConPTY host is missing: {}",
            console.display()
        ));
    }
    ConPtyFuncs::open(&library).map_err(|error| format!("load {}: {error:?}", library.display()))
}

lazy_static! {
    static ref CONPTY: Result<ConPtyFuncs, String> = load_conpty();
}

pub struct PsuedoCon {
    con: HPCON,
}

unsafe impl Send for PsuedoCon {}
unsafe impl Sync for PsuedoCon {}

impl Drop for PsuedoCon {
    fn drop(&mut self) {
        self.close();
    }
}

impl PsuedoCon {
    pub fn close(&mut self) {
        let con = mem::replace(&mut self.con, ptr::null_mut());
        if !con.is_null() {
            // SAFETY: this object owns the console handle and clears it exactly
            // once before closure. ConPtyMasterPty serializes control operations.
            unsafe { (CONPTY.as_ref().unwrap().ConptyClosePseudoConsole)(con) };
        }
    }
    pub fn new(size: COORD, input: FileDescriptor, output: FileDescriptor) -> Result<Self, Error> {
        let api = CONPTY
            .as_ref()
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        let mut con: HPCON = INVALID_HANDLE_VALUE;
        let result = unsafe {
            (api.ConptyCreatePseudoConsole)(
                size,
                input.as_raw_handle() as _,
                output.as_raw_handle() as _,
                PSUEDOCONSOLE_INHERIT_CURSOR
                    | PSEUDOCONSOLE_RESIZE_QUIRK
                    | PSEUDOCONSOLE_WIN32_INPUT_MODE,
                &mut con,
            )
        };
        ensure!(
            result == S_OK,
            "failed to create psuedo console: HRESULT {}",
            result
        );
        Ok(Self { con })
    }

    pub fn resize(&self, size: COORD) -> Result<(), Error> {
        ensure!(!self.con.is_null(), "pseudo console is closed");
        let result =
            unsafe { (CONPTY.as_ref().unwrap().ConptyResizePseudoConsole)(self.con, size) };
        ensure!(
            result == S_OK,
            "failed to resize console to {}x{}: HRESULT: {}",
            size.X,
            size.Y,
            result
        );
        Ok(())
    }

    pub fn spawn_command(&self, cmd: CommandBuilder) -> anyhow::Result<WinChild> {
        ensure!(!self.con.is_null(), "pseudo console is closed");
        let mut si: STARTUPINFOEXW = unsafe { mem::zeroed() };
        si.StartupInfo.cb = mem::size_of::<STARTUPINFOEXW>() as u32;
        // Explicitly set the stdio handles as invalid handles otherwise
        // we can end up with a weird state where the spawned process can
        // inherit the explicitly redirected output handles from its parent.
        // For example, when daemonizing wezterm-mux-server, the stdio handles
        // are redirected to a log file and the spawned process would end up
        // writing its output there instead of to the pty we just created.
        si.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
        si.StartupInfo.hStdInput = INVALID_HANDLE_VALUE;
        si.StartupInfo.hStdOutput = INVALID_HANDLE_VALUE;
        si.StartupInfo.hStdError = INVALID_HANDLE_VALUE;

        let mut attrs = ProcThreadAttributeList::with_capacity(1)?;
        attrs.set_pty(self.con)?;
        si.lpAttributeList = attrs.as_mut_ptr();

        let mut pi: PROCESS_INFORMATION = unsafe { mem::zeroed() };

        let (mut exe, mut cmdline) = cmd.cmdline()?;
        let cmd_os = OsString::from_wide(&cmdline);

        let cwd = cmd.current_directory();
        let job = Arc::new(super::job::ProcessJob::new()?);

        let res = unsafe {
            CreateProcessW(
                exe.as_mut_slice().as_mut_ptr(),
                cmdline.as_mut_slice().as_mut_ptr(),
                ptr::null_mut(),
                ptr::null_mut(),
                0,
                EXTENDED_STARTUPINFO_PRESENT | CREATE_UNICODE_ENVIRONMENT | CREATE_SUSPENDED,
                cmd.environment_block().as_mut_slice().as_mut_ptr() as *mut _,
                cwd.as_ref()
                    .map(|c| c.as_slice().as_ptr())
                    .unwrap_or(ptr::null()),
                &mut si.StartupInfo,
                &mut pi,
            )
        };
        if res == 0 {
            let err = IoError::last_os_error();
            let msg = format!(
                "CreateProcessW `{:?}` in cwd `{:?}` failed: {}",
                cmd_os,
                cwd.as_ref().map(|c| OsString::from_wide(c)),
                err
            );
            log::error!("{}", msg);
            bail!("{}", msg);
        }

        // Make sure we close out the thread handle so we don't leak it;
        // we do this simply by making it owned
        let main_thread = unsafe { OwnedHandle::from_raw_handle(pi.hThread as _) };
        let proc = unsafe { OwnedHandle::from_raw_handle(pi.hProcess as _) };

        if let Err(error) = job.assign(&proc) {
            // SAFETY: creation succeeded but the primary thread remains suspended.
            // Terminate and reap the unassigned root before releasing its handle.
            unsafe {
                TerminateProcess(pi.hProcess, 1);
                winapi::um::synchapi::WaitForSingleObject(
                    pi.hProcess,
                    winapi::um::winbase::INFINITE,
                );
            }
            return Err(error.into());
        }
        // SAFETY: main_thread retains the newly created suspended primary thread.
        if unsafe { ResumeThread(main_thread.as_raw_handle() as _) } == u32::MAX {
            let error = IoError::last_os_error();
            // Dropping the final job handle terminates the assigned process.
            return Err(error.into());
        }

        Ok(WinChild {
            proc: Mutex::new(proc),
            job: Some(job),
        })
    }
}
