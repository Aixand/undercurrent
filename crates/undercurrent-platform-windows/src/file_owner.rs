// SPDX-License-Identifier: Apache-2.0
//! Thread-local default ownership for libraries which create their own files.
use std::io;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use windows_sys::Win32::Foundation::ERROR_NO_TOKEN;
use windows_sys::Win32::Security::{
    DuplicateTokenEx, GetTokenInformation, SecurityImpersonation, SetTokenInformation,
    TOKEN_ADJUST_DEFAULT, TOKEN_DUPLICATE, TOKEN_IMPERSONATE, TOKEN_OWNER, TOKEN_QUERY, TOKEN_USER,
    TokenImpersonation, TokenOwner, TokenUser,
};
use windows_sys::Win32::System::Threading::{
    GetCurrentProcess, GetCurrentThread, OpenProcessToken, OpenThreadToken, SetThreadToken,
};

/// Runs synchronous file creation with the effective account as default owner.
///
/// Elevated Windows tokens can default to Administrators as owner. Libraries
/// such as SQLite do not expose security attributes for their journal files.
/// Duplicate the effective token and change only its default owner, preserving
/// its identity, restrictions, groups and privileges. The process token and
/// other threads are untouched. Nested calls restore their preceding token.
/// The callback must perform its work synchronously on the calling thread.
/// Returning a future or spawning a worker does not extend this ownership scope.
/// Unlike `current_user_sid`, this helper respects an existing thread token.
/// It changes no file DACL and supplies no transaction or content-flush guarantee.
///
/// Setup errors skip the callback. A callback's own result remains nested inside
/// this function's `io::Result`; panic unwinding restores the preceding token.
///
/// Failure to restore the original security context aborts the process.
#[allow(unsafe_code)]
pub fn with_current_user_file_owner<T>(operation: impl FnOnce() -> T) -> io::Result<T> {
    let mut raw = std::ptr::null_mut();
    // SAFETY: the pseudo-handle is valid and raw is writable handle output.
    let previous = if unsafe {
        OpenThreadToken(
            GetCurrentThread(),
            TOKEN_QUERY | TOKEN_DUPLICATE | TOKEN_IMPERSONATE,
            1,
            &mut raw,
        )
    } != 0
    {
        // SAFETY: successful token open returned a newly owned handle.
        Some(unsafe { OwnedHandle::from_raw_handle(raw) })
    } else {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(ERROR_NO_TOKEN as i32) {
            return Err(error);
        }
        None
    };
    let process;
    let source = if let Some(previous) = &previous {
        previous
    } else {
        // SAFETY: the process pseudo-handle is valid; raw is writable output.
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY | TOKEN_DUPLICATE, &mut raw) }
            == 0
        {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful token open returned a newly owned handle.
        process = unsafe { OwnedHandle::from_raw_handle(raw) };
        &process
    };
    // SAFETY: source is live, null attributes make the new handle non-inheritable,
    // and raw is writable output. The original token is never modified.
    if unsafe {
        DuplicateTokenEx(
            source.as_raw_handle(),
            TOKEN_QUERY | TOKEN_ADJUST_DEFAULT | TOKEN_IMPERSONATE,
            std::ptr::null(),
            SecurityImpersonation,
            TokenImpersonation,
            &mut raw,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful duplication returned a newly owned handle.
    let token = unsafe { OwnedHandle::from_raw_handle(raw) };
    // Pointer-aligned storage accommodates TOKEN_USER and a maximum-size SID.
    let mut buffer = [0usize; 128];
    let mut size = 0;
    // SAFETY: token is live and the aligned output buffer has the given size.
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
    // SAFETY: successful TokenUser query initialized the header and its SID.
    let user = unsafe { &*buffer.as_ptr().cast::<TOKEN_USER>() };
    let owner = TOKEN_OWNER {
        Owner: user.User.Sid,
    };
    // SAFETY: owner and its SID storage remain live for this synchronous call.
    if unsafe {
        SetTokenInformation(
            token.as_raw_handle(),
            TokenOwner,
            (&owner as *const TOKEN_OWNER).cast(),
            std::mem::size_of::<TOKEN_OWNER>() as u32,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: null thread selects this thread; token is a live impersonation token.
    if unsafe { SetThreadToken(std::ptr::null(), token.as_raw_handle()) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // This private guard cannot escape the callback or be dropped out of order.
    let _restore = RestoreToken(previous);
    Ok(operation())
}

struct RestoreToken(Option<OwnedHandle>);

impl Drop for RestoreToken {
    #[allow(unsafe_code)]
    fn drop(&mut self) {
        let previous = self
            .0
            .as_ref()
            .map_or(std::ptr::null_mut(), AsRawHandle::as_raw_handle);
        // SAFETY: a retained token handle (or null to remove impersonation) is
        // applied to this thread. The guard never leaves its synchronous call.
        if unsafe { SetThreadToken(std::ptr::null(), previous) } == 0 {
            // Continuing with an unintended security context is unsafe, including
            // while unwinding a callback panic.
            std::process::abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PrivateRemovalFile, prepare_private_directory};

    #[test]
    fn library_created_files_have_exact_user_ownership() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("private");
        let _root = prepare_private_directory(&root).unwrap();
        let path = root.join("library-file");
        with_current_user_file_owner(|| std::fs::write(&path, b"state"))
            .unwrap()
            .unwrap();
        PrivateRemovalFile::open(&path).unwrap().remove().unwrap();
    }

    #[test]
    #[allow(unsafe_code)]
    fn nested_callbacks_and_unwinding_restore_the_thread_token() {
        fn token_id() -> Option<windows_sys::Win32::Foundation::LUID> {
            use windows_sys::Win32::Security::{TOKEN_STATISTICS, TokenStatistics};
            let mut raw = std::ptr::null_mut();
            // SAFETY: valid pseudo-handle and writable output.
            if unsafe { OpenThreadToken(GetCurrentThread(), TOKEN_QUERY, 1, &mut raw) } == 0 {
                assert_eq!(
                    io::Error::last_os_error().raw_os_error(),
                    Some(ERROR_NO_TOKEN as i32)
                );
                return None;
            }
            // SAFETY: newly owned successful open result.
            let token = unsafe { OwnedHandle::from_raw_handle(raw) };
            let mut statistics = std::mem::MaybeUninit::<TOKEN_STATISTICS>::uninit();
            let mut size = 0;
            // SAFETY: live token and properly sized/aligned writable storage.
            assert_ne!(
                // SAFETY: live token and properly sized/aligned writable storage.
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
            // SAFETY: the successful call initialized the entire structure.
            Some(unsafe { statistics.assume_init() }.TokenId)
        }
        fn identity() -> Option<(i32, u32)> {
            token_id().map(|id| (id.HighPart, id.LowPart))
        }
        let before = identity();
        with_current_user_file_owner(|| {
            let outer = identity();
            assert!(outer.is_some());
            assert_ne!(outer, before);
            let child = std::thread::spawn(identity).join().unwrap();
            assert!(child.is_none());
            assert!(
                std::panic::catch_unwind(|| {
                    with_current_user_file_owner(|| {
                        assert_ne!(identity(), outer);
                        panic!("exercise restoration");
                    })
                    .unwrap();
                })
                .is_err()
            );
            assert_eq!(identity(), outer);
        })
        .unwrap();
        assert_eq!(identity(), before);
    }
}
