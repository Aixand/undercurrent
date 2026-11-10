// SPDX-License-Identifier: Apache-2.0
//! Bounded, read-only Darwin process evidence for the daemon-private runtime.
//!
//! This macOS-only crate is an internal FFI boundary, not a supported consumer
//! SDK. Calls are synchronous, retain no native pointers, and return owned Rust
//! values. Bounds limit allocation and iteration, not wall-clock latency; there
//! is no cancellation or shutdown protocol and no process mutation.
//!
//! On macOS, `identity` and `group_members` supply facts for caller-owned membership
//! and UID checks. `arguments` returns sensitive, unparsed bytes in an
//! `ArgumentBuffer`; the runtime owns parsing and caching. `working_directory`
//! and `executable_path` preserve native path bytes. `tcp_listener` adds
//! bounded descriptor/socket inspection and before/after process checks.
//!
//! Separate reads are not an atomic process snapshot. Errors and truncation must
//! remain unavailable or incomplete evidence, rather than certifying absence,
//! readiness, or the outcome of an earlier runtime operation.
#![cfg(target_os = "macos")]

use std::io;
use std::mem::{MaybeUninit, size_of};
use std::os::unix::ffi::OsStringExt;
use std::path::PathBuf;

/// Maximum returned group members; enumeration reserves one extra sentinel slot.
pub const MAX_GROUP_MEMBERS: usize = 32;
/// Fixed byte capacity for one `KERN_PROCARGS2` read; oversized reads are not retried.
pub const MAX_ARGUMENT_BUFFER: usize = 128 * 1024;

/// One kernel process snapshot, including identity and caller-checkable ownership.
///
/// Equality compares all fields. PID and creation time help detect PID reuse;
/// equal snapshots do not exclude an intervening exec or other transient change.
/// The adapter reports group and UID without enforcing an expected owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessIdentity {
    /// Queried process ID, checked against the kernel response.
    pub pid: u32,
    /// Kernel-reported parent process ID.
    pub parent: u32,
    /// Kernel-reported process-group ID.
    pub group: u32,
    /// Kernel-reported UID; callers compare it with the required owner.
    pub uid: u32,
    /// Nonzero creation time in microseconds, converted from Darwin's start time.
    pub start_micros: u64,
    /// Raw `pbi_name` bytes, compared by the runtime's command-fact cache.
    ///
    /// An image-name change invalidates that cache entry, but this bounded name
    /// is not a unique executable identity or a guarantee of detecting every exec.
    pub image_name: [u8; 32],
}

fn pid(value: u32) -> io::Result<i32> {
    i32::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid process ID"))
}
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "incomplete Darwin process information",
    )
}

/// Reads one exact-size `PROC_PIDTBSDINFO` snapshot.
///
/// Rejects invalid IDs, short or inconsistent replies, and invalid/overflowing
/// start times. Native failures retain the OS error. This performs no UID/group
/// authorization and does not keep the process alive after return.
#[allow(unsafe_code)]
pub fn identity(process: u32) -> io::Result<ProcessIdentity> {
    let process = pid(process)?;
    let mut info = MaybeUninit::<libc::proc_bsdinfo>::uninit();
    // SAFETY: Darwin receives an aligned, writable proc_bsdinfo allocation of
    // exactly the advertised size. The pointer is not retained by the call.
    let read = unsafe {
        libc::proc_pidinfo(
            process,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            size_of::<libc::proc_bsdinfo>() as i32,
        )
    };
    if read <= 0 {
        return Err(io::Error::last_os_error());
    }
    if read as usize != size_of::<libc::proc_bsdinfo>() {
        return Err(invalid());
    }
    // SAFETY: a successful exact-size PROC_PIDTBSDINFO call initializes the
    // entire C structure; its integer and byte-array fields have no invalid bits.
    let info = unsafe { info.assume_init() };
    if info.pbi_pid != process as u32 || info.pbi_start_tvusec >= 1_000_000 {
        return Err(invalid());
    }
    let start_micros = info
        .pbi_start_tvsec
        .checked_mul(1_000_000)
        .and_then(|value| value.checked_add(info.pbi_start_tvusec))
        .filter(|value| *value > 0)
        .ok_or_else(invalid)?;
    Ok(ProcessIdentity {
        pid: info.pbi_pid,
        parent: info.pbi_ppid,
        group: info.pbi_pgid,
        uid: info.pbi_uid,
        start_micros,
        image_name: info.pbi_name.map(|byte| byte as u8),
    })
}

/// Returns sorted group members and whether enumeration fit within the bound.
///
/// At most [`MAX_GROUP_MEMBERS`] PIDs are returned. A filled sentinel slot sets
/// the Boolean to `false`; the retained prefix is sorted, not a globally smallest
/// selection. Nonpositive IDs, malformed sizes, and duplicates in the retained
/// prefix fail. A zero-byte native result is an error, not an empty group.
/// Callers must independently recheck membership and each member's ownership.
#[allow(unsafe_code)]
pub fn group_members(group: u32) -> io::Result<(Vec<u32>, bool)> {
    pid(group)?;
    // One sentinel slot makes filling the buffer a conservative incomplete read.
    let mut buffer = [0_i32; MAX_GROUP_MEMBERS + 1];
    // SAFETY: the fixed, initialized PID array covers the exact byte count;
    // PROC_PGRP_ONLY (2 in libproc.h) writes only process IDs and retains nothing.
    let bytes = unsafe {
        libc::proc_listpids(
            2,
            group,
            buffer.as_mut_ptr().cast(),
            size_of_val(&buffer) as i32,
        )
    };
    if bytes <= 0 {
        return Err(io::Error::last_os_error());
    }
    let bytes = bytes as usize;
    if bytes > size_of_val(&buffer) || !bytes.is_multiple_of(size_of::<i32>()) {
        return Err(invalid());
    }
    let count = bytes / size_of::<i32>();
    if buffer[..count].iter().any(|value| *value <= 0) {
        return Err(invalid());
    }
    let mut members = buffer[..count.min(MAX_GROUP_MEMBERS)]
        .iter()
        .map(|value| *value as u32)
        .collect::<Vec<_>>();
    // The runtime compares successive lists by equality to detect membership
    // changes, so enumeration order alone must not invalidate a sample. See
    // [ownership rechecks](../docs/internals/runtime-integration.md#collection-checks).
    members.sort_unstable();
    if members.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(invalid());
    }
    Ok((members, count <= MAX_GROUP_MEMBERS))
}

/// Owned, sensitive `KERN_PROCARGS2` bytes; deliberately has no `Debug` output.
///
/// Drop fills the vector's current length with zero, including error paths after
/// acquisition. Successful reads truncate the vector before returning it. This
/// ordinary fill is not optimizer-resistant erasure and cannot clear caller
/// copies or promise erasure of unused capacity. Do not log the borrowed bytes.
pub struct ArgumentBuffer(Vec<u8>);
impl ArgumentBuffer {
    /// Borrows the unparsed native bytes, which can include environment values.
    ///
    /// The borrow cannot outlive this buffer. Parsing and data minimization are
    /// the caller's responsibility; acquisition validates only storage bounds.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}
impl Drop for ArgumentBuffer {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

/// Reads argument/environment bytes with one fixed-capacity `sysctl` call.
///
/// The returned length must fit [`MAX_ARGUMENT_BUFFER`] and contain at least an
/// `i32` argument count. Syntax and encoding are not validated here. There is no
/// resize retry or process-identity recheck; callers bracket dependent facts with
/// their own identity checks. Native failures retain the OS error.
#[allow(unsafe_code)]
pub fn arguments(process: u32) -> io::Result<ArgumentBuffer> {
    let mut mib = [libc::CTL_KERN, libc::KERN_PROCARGS2, pid(process)?];
    // Install the drop owner before native access so both error returns below
    // run the same clearing step as success; wrapping only a successful result
    // would bypass it on failure. See the limits of that clearing in
    // [sensitive storage](../docs/reference/query-contracts.md#arguments-and-sensitive-storage).
    let mut buffer = ArgumentBuffer(vec![0; MAX_ARGUMENT_BUFFER]);
    let mut length = buffer.0.len();
    // SAFETY: both slices are initialized, exclusive writable allocations;
    // length initially equals the output capacity, newp is null for a read-only
    // query, and neither pointer is retained. Oversized results fail, not retry.
    let result = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            mib.len() as u32,
            buffer.0.as_mut_ptr().cast(),
            &mut length,
            std::ptr::null_mut(),
            0,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    if length < size_of::<i32>() || length > buffer.0.len() {
        return Err(invalid());
    }
    buffer.0.truncate(length);
    Ok(buffer)
}

/// Reads an absolute working-directory path while preserving non-UTF-8 bytes.
///
/// Requires an exact-size `PROC_PIDVNODEPATHINFO` reply and a NUL within the path
/// field. The path is neither canonicalized nor opened, and identity is not
/// rechecked. Query errors do not imply that the process or directory is absent.
#[allow(unsafe_code)]
pub fn working_directory(process: u32) -> io::Result<PathBuf> {
    let process = pid(process)?;
    let mut info = MaybeUninit::<libc::proc_vnodepathinfo>::uninit();
    // SAFETY: aligned output has the exact size required by this flavor; no
    // pointers are retained. The structure is read only after full initialization.
    let read = unsafe {
        libc::proc_pidinfo(
            process,
            libc::PROC_PIDVNODEPATHINFO,
            0,
            info.as_mut_ptr().cast(),
            size_of::<libc::proc_vnodepathinfo>() as i32,
        )
    };
    if read <= 0 {
        return Err(io::Error::last_os_error());
    }
    if read as usize != size_of::<libc::proc_vnodepathinfo>() {
        return Err(invalid());
    }
    // SAFETY: the full C structure was initialized; its fields accept all bit patterns.
    let info = unsafe { info.assume_init() };
    let path = info.pvi_cdir.vip_path.as_flattened();
    let end = path
        .iter()
        .position(|value| *value == 0)
        .ok_or_else(invalid)?;
    let path = PathBuf::from(std::ffi::OsString::from_vec(
        path[..end].iter().map(|value| *value as u8).collect(),
    ));
    if !path.is_absolute() {
        return Err(invalid());
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reads_current_identity_arguments_and_directory() {
        assert_eq!(
            executable_path(std::process::id()).unwrap(),
            std::env::current_exe().unwrap()
        );
        let own = identity(std::process::id()).unwrap();
        assert_eq!(own.pid, std::process::id());
        assert!(own.start_micros > 0);
        assert!(group_members(own.group).unwrap().0.contains(&own.pid));
        assert!(!arguments(own.pid).unwrap().as_bytes().is_empty());
        assert_eq!(
            working_directory(own.pid).unwrap(),
            std::env::current_dir().unwrap()
        );
    }
    #[test]
    fn invalid_and_absent_processes_fail() {
        for process in [0, u32::MAX] {
            assert!(identity(process).is_err());
            assert!(arguments(process).is_err());
            assert!(working_directory(process).is_err());
            assert!(group_members(process).is_err());
        }
        assert!(identity(i32::MAX as u32).is_err());
    }
}

/// Absolute kernel-reported executable path (not `argv[0]`).
///
/// Uses a fixed 4096-byte buffer and requires the first NUL exactly at the
/// positive returned count, below capacity. Preserves non-UTF-8 path bytes;
/// performs no canonicalization, executable pinning, or identity recheck.
#[allow(unsafe_code)]
pub fn executable_path(process: u32) -> io::Result<PathBuf> {
    let process = pid(process)?;
    let mut bytes = [0u8; 4096];
    // SAFETY: proc_pidpath receives a writable buffer of the exact advertised
    // size, retains no pointer, and returns a bounded NUL-terminated path.
    let count =
        unsafe { libc::proc_pidpath(process, bytes.as_mut_ptr().cast(), bytes.len() as u32) };
    if count <= 0 {
        return Err(io::Error::last_os_error());
    }
    if count as usize >= bytes.len() {
        return Err(invalid());
    }
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(invalid)?;
    if end == 0 || end != count as usize {
        return Err(invalid());
    }
    let path = PathBuf::from(std::ffi::OsString::from_vec(bytes[..end].to_vec()));
    if !path.is_absolute() {
        return Err(invalid());
    }
    Ok(path)
}

/// Evidence from one bounded socket sample, not an application health result.
///
/// Unavailable inspection is represented by the error from [`tcp_listener`],
/// rather than a variant that could be confused with observed absence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TcpListener {
    /// No qualifying listener in the successfully checked descriptor snapshot.
    Absent,
    /// At least one checked IPv4/IPv6 TCP stream socket was in the listen state.
    Present,
    /// The descriptor cap prevented complete inspection; absence is not known.
    Truncated,
}

/// Bounded socket inspection, with process and descriptor rechecks.
///
/// The C helper grows descriptor limits from 256 to 4096 with one sentinel slot,
/// retries only overflow, and frees its arrays before returning. Socket reads
/// and a second descriptor list must agree; the full process identity is also
/// compared before and after the helper. These checks do not make sampling atomic.
///
/// Identity-query errors propagate; changed identity is `InvalidData`. Native
/// helper failures collapse to `Other` without preserving errno. A final identity
/// failure takes precedence over any helper result. No port or address is exposed
/// and no connection is attempted.
#[allow(unsafe_code)]
pub fn tcp_listener(process: u32) -> io::Result<TcpListener> {
    let before = identity(process)?;
    let pid = pid(process)?;
    unsafe extern "C" {
        fn uc_macos_tcp_listener(pid: libc::c_int) -> libc::c_int;
    }
    // SAFETY: the SDK-compiled adapter takes only a validated positive integer;
    // its bounded heap buffers do not escape and no pointers cross this boundary.
    let result = unsafe { uc_macos_tcp_listener(pid) };
    if identity(process)? != before {
        return Err(invalid());
    }
    match result {
        0 => Ok(TcpListener::Absent),
        1 => Ok(TcpListener::Present),
        2 => Ok(TcpListener::Truncated),
        _ => Err(io::Error::other("Darwin socket observation unavailable")),
    }
}

#[cfg(test)]
mod listener_tests {
    use super::*;
    #[test]
    fn bounded_listener_failure_contracts() {
        use std::os::unix::fs::DirBuilderExt;
        let directory =
            std::env::temp_dir().join(format!("uc-listener-contract-{}", std::process::id()));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&directory)
            .unwrap();
        let output = directory.join("contract");
        let status = std::process::Command::new("cc")
            .args(["-Wall", "-Wextra", "-Werror"])
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/listeners_contract.c"
            ))
            .arg("-o")
            .arg(&output)
            .status()
            .unwrap();
        assert!(status.success());
        let result = std::process::Command::new(&output).status();
        std::fs::remove_dir_all(directory).unwrap();
        assert!(result.unwrap().success());
    }

    #[test]
    fn observes_listener_above_old_descriptor_limit() {
        const CHILD: &str = "UC_HIGH_FD_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            // Isolate descriptors and raise only the child's soft limit.
            let status = std::process::Command::new("/bin/sh")
                .args(["-c", "ulimit -n 2048 && exec \"$1\" --exact listener_tests::observes_listener_above_old_descriptor_limit --nocapture", "uc-test"])
                .arg(std::env::current_exe().unwrap())
                .env(CHILD, "1")
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }
        // Keep these handles alive through both observations: they force growth
        // past the helper's initial descriptor capacity even after listener removal.
        // See [high-descriptor regression](../docs/development/contributing.md#native-queries).
        let files: Vec<_> = (0..700)
            .map(|_| std::fs::File::open("/dev/null").unwrap())
            .collect();
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        assert_eq!(
            tcp_listener(std::process::id()).unwrap(),
            TcpListener::Present
        );
        drop(listener);
        assert_eq!(
            tcp_listener(std::process::id()).unwrap(),
            TcpListener::Absent
        );
        drop(files);
    }

    #[test]
    fn observes_real_listener_and_its_removal() {
        const CHILD: &str = "UC_LISTENER_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            // Other tests spawn processes and change this process's descriptor
            // table. Inspect an isolated child so those changes cannot race
            // the adapter's required before/after descriptor recheck.
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "listener_tests::observes_real_listener_and_its_removal",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        assert_eq!(
            tcp_listener(std::process::id()).unwrap(),
            TcpListener::Present
        );
        drop(listener);
        assert_eq!(
            tcp_listener(std::process::id()).unwrap(),
            TcpListener::Absent
        );
        assert!(tcp_listener(0).is_err());
    }
}
