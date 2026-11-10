// SPDX-License-Identifier: Apache-2.0
//! Kernel account identity for pipe peers and explicit private ACLs.
//!
//! Process identity ignores thread impersonation. The pipe-client fallback
//! briefly queries the pipe's identification token and restores the preceding
//! thread context. Handles and SID allocations remain local to each query.
use std::io;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use tokio::net::windows::named_pipe::NamedPipeServer;
use windows_sys::Win32::Foundation::{ERROR_NO_TOKEN, HANDLE, LocalFree};
use windows_sys::Win32::Security::Authorization::ConvertSidToStringSidW;
use windows_sys::Win32::Security::{
    GetTokenInformation, SecurityIdentification, TOKEN_IMPERSONATE, TOKEN_QUERY, TOKEN_USER,
    TokenImpersonationLevel, TokenUser,
};
use windows_sys::Win32::System::Pipes::ImpersonateNamedPipeClient;
use windows_sys::Win32::System::Threading::{
    GetCurrentProcess, GetCurrentThread, OpenProcess, OpenProcessToken, OpenThreadToken,
    PROCESS_QUERY_LIMITED_INFORMATION, SetThreadToken,
};

/// Returns the current process token's user SID as a string.
///
/// This is not the token's default file owner or a thread impersonation identity.
/// OS query failures propagate; invalid converted text is `InvalidData`.
pub fn current_user_sid() -> io::Result<String> {
    // SAFETY: GetCurrentProcess returns a borrowed pseudo-handle valid in this process.
    #[allow(unsafe_code)]
    process_sid(unsafe { GetCurrentProcess() })
}

#[allow(unsafe_code)]
pub(crate) fn process_user_sid(pid: u32) -> io::Result<String> {
    // SAFETY: no pointers are passed; the handle is checked before being owned.
    let raw = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if raw.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: raw is a newly owned, valid handle, transferred once.
    let process = unsafe { OwnedHandle::from_raw_handle(raw) };
    process_sid(process.as_raw_handle())
}

#[allow(unsafe_code)]
fn process_sid(process: HANDLE) -> io::Result<String> {
    let mut raw = std::ptr::null_mut();
    // SAFETY: caller supplies a live process handle; raw is writable output.
    if unsafe { OpenProcessToken(process, TOKEN_QUERY, &mut raw) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful OpenProcessToken transfers a new token handle.
    let token = unsafe { OwnedHandle::from_raw_handle(raw) };
    token_user_sid(&token)
}

/// Queries the kernel-authenticated pipe client without opening its process.
///
/// An elevated or SSH client can deny process inspection to a regular server
/// even when both use the same account. Identification through the connected
/// pipe requires no process access or administrator privilege. The synchronous
/// scope only queries identity, restores any preceding thread token before
/// returning, and never executes protocol or filesystem work as the client.
#[allow(unsafe_code)]
pub(crate) fn pipe_client_user_sid(pipe: &NamedPipeServer) -> io::Result<String> {
    let mut raw = std::ptr::null_mut();
    // SAFETY: the current-thread pseudo-handle and writable output are valid.
    let previous = if unsafe {
        OpenThreadToken(
            GetCurrentThread(),
            TOKEN_QUERY | TOKEN_IMPERSONATE,
            1,
            &mut raw,
        )
    } != 0
    {
        // SAFETY: successful token open transfers a newly owned handle.
        Some(unsafe { OwnedHandle::from_raw_handle(raw) })
    } else {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(ERROR_NO_TOKEN as i32) {
            return Err(error);
        }
        None
    };
    // This guard cannot leave this synchronous function or cross an await.
    let _restore = RestoreThreadToken(previous);
    // SAFETY: the borrowed server keeps its connected pipe handle live. The
    // client controls the maximum impersonation level; anonymous is rejected.
    if unsafe { ImpersonateNamedPipeClient(pipe.as_raw_handle()) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: query only the calling thread's identification token, performing
    // the access check as the server rather than as the identified client.
    if unsafe { OpenThreadToken(GetCurrentThread(), TOKEN_QUERY, 1, &mut raw) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful token open transfers a newly owned handle.
    let token = unsafe { OwnedHandle::from_raw_handle(raw) };
    let mut level = 0i32;
    let mut size = 0;
    // SAFETY: token is live; level is a correctly sized/aligned writable
    // SECURITY_IMPERSONATION_LEVEL value and size receives the output length.
    if unsafe {
        GetTokenInformation(
            token.as_raw_handle(),
            TokenImpersonationLevel,
            (&mut level as *mut i32).cast(),
            std::mem::size_of_val(&level) as u32,
            &mut size,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    if level < SecurityIdentification {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "local client identity is anonymous",
        ));
    }
    token_user_sid(&token)
}

struct RestoreThreadToken(Option<OwnedHandle>);

impl Drop for RestoreThreadToken {
    #[allow(unsafe_code)]
    fn drop(&mut self) {
        let previous = self
            .0
            .as_ref()
            .map_or(std::ptr::null_mut(), AsRawHandle::as_raw_handle);
        // SAFETY: a retained TOKEN_IMPERSONATE handle (or null to remove the
        // temporary token) is applied synchronously to this same thread.
        if unsafe { SetThreadToken(std::ptr::null(), previous) } == 0 {
            // Never continue a worker in an unintended security context.
            std::process::abort();
        }
    }
}

#[allow(unsafe_code)]
fn token_user_sid(token: &OwnedHandle) -> io::Result<String> {
    // TOKEN_USER plus a maximum-size SID fits in this aligned, bounded buffer.
    let mut buffer = [0usize; 128];
    let mut size = 0;
    // SAFETY: buffer is pointer-aligned and writable for the advertised size.
    if unsafe {
        GetTokenInformation(
            token.as_raw_handle(),
            TokenUser,
            buffer.as_mut_ptr().cast(),
            std::mem::size_of_val(&buffer) as u32,
            &mut size,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful TokenUser query initializes the TOKEN_USER header.
    let user = unsafe { &*buffer.as_ptr().cast::<TOKEN_USER>() };
    let mut sid = std::ptr::null_mut();
    // SAFETY: user.Sid points into buffer, which remains live during the conversion.
    if unsafe { ConvertSidToStringSidW(user.User.Sid, &mut sid) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let mut length = 0;
    // SAFETY: successful conversion allocates a NUL-terminated UTF-16 SID string.
    while unsafe { *sid.add(length) } != 0 {
        length += 1;
    }
    // SAFETY: the preceding scan measured the allocated string before its terminator.
    let result = String::from_utf16(unsafe { std::slice::from_raw_parts(sid, length) })
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid account SID"));
    // SAFETY: the conversion API requires its allocation to be freed using LocalFree.
    unsafe {
        LocalFree(sid.cast());
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PipeListenerHandle, pipe_name, with_current_user_file_owner};
    use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient};
    use windows_sys::Win32::Security::{TOKEN_STATISTICS, TokenStatistics};
    use windows_sys::Win32::Storage::FileSystem::SECURITY_ANONYMOUS;

    #[allow(unsafe_code)]
    fn thread_token_id() -> Option<(i32, u32)> {
        let mut raw = std::ptr::null_mut();
        // SAFETY: the current-thread pseudo-handle and output are valid.
        if unsafe { OpenThreadToken(GetCurrentThread(), TOKEN_QUERY, 1, &mut raw) } == 0 {
            assert_eq!(
                io::Error::last_os_error().raw_os_error(),
                Some(ERROR_NO_TOKEN as i32)
            );
            return None;
        }
        // SAFETY: a successful open transfers this newly owned token handle.
        let token = unsafe { OwnedHandle::from_raw_handle(raw) };
        let mut statistics = std::mem::MaybeUninit::<TOKEN_STATISTICS>::uninit();
        let mut size = 0;
        // SAFETY: a live token and correctly sized/aligned writable outputs.
        assert_ne!(
            // SAFETY: a live token and correctly sized/aligned writable outputs.
            unsafe {
                GetTokenInformation(
                    token.as_raw_handle(),
                    TokenStatistics,
                    statistics.as_mut_ptr().cast(),
                    std::mem::size_of::<TOKEN_STATISTICS>() as u32,
                    &mut size,
                )
            },
            0
        );
        // SAFETY: successful query initialized the entire statistics structure.
        let id = unsafe { statistics.assume_init() }.TokenId;
        Some((id.HighPart, id.LowPart))
    }

    async fn connected_pair(anonymous: bool) -> (NamedPipeServer, NamedPipeClient) {
        let directory = tempfile::tempdir().unwrap();
        let name = pipe_name(&directory.path().join("peer-token")).unwrap();
        let server = PipeListenerHandle::create(&name, true)
            .unwrap()
            .into_async()
            .unwrap();
        let mut options = ClientOptions::new();
        if anonymous {
            options.security_qos_flags(SECURITY_ANONYMOUS);
        }
        let client = options.open(&name).unwrap();
        server.connect().await.unwrap();
        (server, client)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pipe_identification_restores_empty_and_existing_thread_tokens() {
        let (server, _client) = connected_pair(false).await;
        let account = current_user_sid().unwrap();
        let before = thread_token_id();
        assert_eq!(pipe_client_user_sid(&server).unwrap(), account);
        assert_eq!(thread_token_id(), before);
        with_current_user_file_owner(|| {
            let scoped = thread_token_id();
            assert!(scoped.is_some());
            assert_ne!(scoped, before);
            assert_eq!(pipe_client_user_sid(&server).unwrap(), account);
            assert_eq!(thread_token_id(), scoped);
        })
        .unwrap();
        assert_eq!(thread_token_id(), before);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn anonymous_identification_fails_without_leaking_a_thread_token() {
        let (server, _client) = connected_pair(true).await;
        let before = thread_token_id();
        assert!(pipe_client_user_sid(&server).is_err());
        assert_eq!(thread_token_id(), before);
        with_current_user_file_owner(|| {
            let scoped = thread_token_id();
            assert!(pipe_client_user_sid(&server).is_err());
            assert_eq!(thread_token_id(), scoped);
        })
        .unwrap();
        assert_eq!(thread_token_id(), before);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unconnected_identification_fails_and_restores_the_thread_context() {
        let directory = tempfile::tempdir().unwrap();
        let name = pipe_name(&directory.path().join("not-connected")).unwrap();
        let server = PipeListenerHandle::create(&name, true)
            .unwrap()
            .into_async()
            .unwrap();
        let before = thread_token_id();
        assert!(pipe_client_user_sid(&server).is_err());
        assert_eq!(thread_token_id(), before);
    }
}
