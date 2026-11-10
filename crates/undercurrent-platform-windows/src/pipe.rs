// SPDX-License-Identifier: Apache-2.0
//! Owner-restricted, local-only named pipes with kernel-supplied peer identities.
use crate::security::{current_user_sid, pipe_client_user_sid, process_user_sid};
use sha2::{Digest, Sha256};
use std::io;
use std::os::windows::io::{AsRawHandle, FromRawHandle, IntoRawHandle, OwnedHandle};
use std::path::{Component, Path};
use tokio::net::windows::named_pipe::{NamedPipeClient, NamedPipeServer};
use windows_sys::Win32::Foundation::{ERROR_ACCESS_DENIED, INVALID_HANDLE_VALUE, LocalFree};
use windows_sys::Win32::Security::Authorization::{
    ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
};
use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
use windows_sys::Win32::Storage::FileSystem::{
    FILE_FLAG_FIRST_PIPE_INSTANCE, FILE_FLAG_OVERLAPPED, PIPE_ACCESS_DUPLEX,
};
use windows_sys::Win32::System::Pipes::{
    CreateNamedPipeW, GetNamedPipeClientProcessId, GetNamedPipeServerProcessId, PIPE_READMODE_BYTE,
    PIPE_REJECT_REMOTE_CLIENTS, PIPE_TYPE_BYTE, PIPE_UNLIMITED_INSTANCES,
};

/// Maps an absolute configured endpoint to an account-scoped local kernel name.
///
/// Hashes the supplied UTF-16 spelling without canonicalization or case folding.
/// Rejects missing leaf names, parent traversal, and NULs. The path need not
/// exist and no filesystem entry is created.
pub fn pipe_name(path: &Path) -> io::Result<String> {
    use std::os::windows::ffi::OsStrExt;
    if !path.is_absolute()
        || path.file_name().is_none()
        || path.components().any(|c| matches!(c, Component::ParentDir))
        || path.as_os_str().encode_wide().any(|c| c == 0)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid local endpoint path",
        ));
    }
    let mut digest = Sha256::new();
    digest.update(b"undercurrent.local-pipe.v0.1.0-beta\0");
    digest.update(current_user_sid()?.as_bytes());
    digest.update([0]);
    for code in path.as_os_str().encode_wide() {
        digest.update(code.to_le_bytes());
    }
    Ok(format!(r"\\.\pipe\undercurrent-{:x}", digest.finalize()))
}

/// Created outside Tokio; transferred exactly once into the listener's reactor.
pub struct PipeListenerHandle(OwnedHandle);

impl PipeListenerHandle {
    /// First-instance exclusivity prevents silently joining another server's pipe.
    #[allow(unsafe_code)]
    pub fn create(name: &str, first: bool) -> io::Result<Self> {
        if !name.starts_with(r"\\.\pipe\undercurrent-") || name.contains('\0') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid private pipe name",
            ));
        }
        let sid = current_user_sid()?;
        // Protected DACL: only the current account may access or create instances.
        let descriptor: Vec<u16> = format!("O:{sid}D:P(A;;GA;;;{sid})")
            .encode_utf16()
            .chain(Some(0))
            .collect();
        let mut security = std::ptr::null_mut();
        // SAFETY: descriptor is NUL terminated and security is writable output.
        if unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                descriptor.as_ptr(),
                SDDL_REVISION_1,
                &mut security,
                std::ptr::null_mut(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let attributes = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: security,
            bInheritHandle: 0,
        };
        let name: Vec<u16> = name.encode_utf16().chain(Some(0)).collect();
        // SAFETY: name and security attributes remain live through creation;
        // Windows copies their contents and the handle is not inheritable.
        let raw = unsafe {
            CreateNamedPipeW(
                name.as_ptr(),
                PIPE_ACCESS_DUPLEX
                    | FILE_FLAG_OVERLAPPED
                    | if first {
                        FILE_FLAG_FIRST_PIPE_INSTANCE
                    } else {
                        0
                    },
                PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_REJECT_REMOTE_CLIENTS,
                PIPE_UNLIMITED_INSTANCES,
                65536,
                65536,
                0,
                &attributes,
            )
        };
        let error = io::Error::last_os_error();
        // SAFETY: security was allocated by the conversion API above.
        unsafe {
            LocalFree(security);
        }
        if raw == INVALID_HANDLE_VALUE {
            return Err(error);
        }
        // SAFETY: successful creation returns a newly owned handle.
        Ok(Self(unsafe { OwnedHandle::from_raw_handle(raw) }))
    }

    /// Transfers ownership into the currently entered Tokio I/O runtime.
    /// The raw handle is consumed exactly once, including registration failure.
    #[allow(unsafe_code)]
    pub fn into_async(self) -> io::Result<NamedPipeServer> {
        // SAFETY: this is a uniquely owned overlapped named-pipe server handle.
        // Tokio takes ownership, including closing it if reactor registration fails.
        unsafe { NamedPipeServer::from_raw_handle(self.0.into_raw_handle()) }
    }
}

/// Verifies the connected client's kernel-authenticated account, not its
/// executable, session, or application credentials. Process-inspection denial
/// uses the pipe identification token; that fallback rejects anonymous clients.
pub fn verify_pipe_client(pipe: &NamedPipeServer) -> io::Result<()> {
    client_process_id(pipe).map(drop)
}

#[allow(unsafe_code)]
pub(crate) fn client_process_id(pipe: &NamedPipeServer) -> io::Result<u32> {
    let mut pid = 0;
    // SAFETY: pipe retains a live server handle and pid is writable output.
    if unsafe { GetNamedPipeClientProcessId(pipe.as_raw_handle(), &mut pid) } == 0 {
        return Err(io::Error::last_os_error());
    }
    match process_user_sid(pid) {
        Ok(sid) => verify_account(&sid)?,
        Err(error) if error.raw_os_error() == Some(ERROR_ACCESS_DENIED as i32) => {
            // A regular server cannot inspect some elevated/SSH processes.
            // Windows still authenticates their account on the connected pipe.
            verify_account(&pipe_client_user_sid(pipe)?)?;
        }
        Err(error) => return Err(error),
    }
    Ok(pid)
}

/// Verifies the kernel-reported server's process-token account. This does not
/// replace a higher-level protocol handshake or application authentication.
pub fn verify_pipe_server(pipe: &NamedPipeClient) -> io::Result<()> {
    server_process_id(pipe).map(drop)
}

#[allow(unsafe_code)]
pub(crate) fn server_process_id(pipe: &NamedPipeClient) -> io::Result<u32> {
    let mut pid = 0;
    // SAFETY: pipe retains a live client handle and pid is writable output.
    if unsafe { GetNamedPipeServerProcessId(pipe.as_raw_handle(), &mut pid) } == 0 {
        return Err(io::Error::last_os_error());
    }
    verify_user(pid)?;
    Ok(pid)
}

fn verify_user(pid: u32) -> io::Result<()> {
    verify_account(&process_user_sid(pid)?)
}

fn verify_account(sid: &str) -> io::Result<()> {
    if sid != current_user_sid()? {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "local peer account mismatch",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use windows_sys::Win32::Foundation::{GetHandleInformation, HANDLE_FLAG_INHERIT};
    use windows_sys::Win32::Security::Authorization::{GetSecurityInfo, SE_KERNEL_OBJECT};
    use windows_sys::Win32::Security::{
        DACL_SECURITY_INFORMATION, GetSecurityDescriptorControl, SE_DACL_PROTECTED,
    };

    #[test]
    #[allow(unsafe_code)]
    fn kernel_pipe_has_protected_single_account_acl_and_noninheritable_handle() {
        let name = format!(r"\\.\pipe\undercurrent-acl-test-{}", std::process::id());
        let pipe = PipeListenerHandle::create(&name, true).unwrap();
        let mut flags = 0;
        assert_ne!(
            // SAFETY: pipe retains the handle and flags is writable output.
            unsafe { GetHandleInformation(pipe.0.as_raw_handle(), &mut flags) },
            0
        );
        assert_eq!(flags & HANDLE_FLAG_INHERIT, 0);
        let mut acl = std::ptr::null_mut();
        let mut descriptor = std::ptr::null_mut();
        // SAFETY: the pipe handle is valid; output pointers receive one allocated
        // descriptor and its contained DACL, released below using LocalFree.
        let result = unsafe {
            GetSecurityInfo(
                pipe.0.as_raw_handle(),
                SE_KERNEL_OBJECT,
                DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut acl,
                std::ptr::null_mut(),
                &mut descriptor,
            )
        };
        assert_eq!(result, 0);
        let mut control = 0;
        let mut revision = 0;
        // SAFETY: GetSecurityInfo returned a valid descriptor with a non-null DACL.
        let (count, queried) = unsafe {
            (
                (*acl).AceCount,
                GetSecurityDescriptorControl(descriptor, &mut control, &mut revision),
            )
        };
        // SAFETY: descriptor was allocated by GetSecurityInfo.
        unsafe {
            LocalFree(descriptor);
        }
        assert_ne!(queried, 0);
        assert_eq!(count, 1);
        assert_ne!(control & SE_DACL_PROTECTED, 0);
    }
}
