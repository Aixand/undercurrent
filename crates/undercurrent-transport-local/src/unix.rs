// SPDX-License-Identifier: Apache-2.0
//! Unix filesystem endpoints and kernel-authenticated peers.
//!
//! Retained directory handles anchor traversal. Bind holds a validated lock
//! through listener draining and recovers only a verified refused socket.
//! Connect validates metadata before dialing and checks the peer UID afterward.
//! Neither path follows symlink ancestors or repairs an unsafe existing entry.
use crate::LocalEndpoint;
use std::{
    fs::{self, File, OpenOptions},
    io,
    os::{
        fd::AsRawFd,
        unix::{
            fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
            net::UnixListener,
        },
    },
    path::{Component, Path, PathBuf},
};
/// Native stream kept behind the local adapter boundary.
pub type LocalStream = tokio::net::UnixStream;
pub use tokio::net::UnixListener as AsyncLocalListener;

fn denied() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "unsafe local endpoint ownership or permissions",
    )
}
fn anchored(parent: &File, name: &std::ffi::OsStr) -> io::Result<PathBuf> {
    #[cfg(target_os = "linux")]
    {
        Ok(PathBuf::from(format!("/proc/self/fd/{}", parent.as_raw_fd())).join(name))
    }
    #[cfg(target_os = "macos")]
    {
        let mut path = PathBuf::new();
        nix::fcntl::fcntl(parent, nix::fcntl::FcntlArg::F_GETPATH(&mut path))?;
        let actual = fs::symlink_metadata(&path)?;
        let expected = parent.metadata()?;
        if actual.dev() != expected.dev() || actual.ino() != expected.ino() {
            return Err(denied());
        }
        Ok(path.join(name))
    }
}
fn open_directory(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
}
fn private_parent(endpoint: &LocalEndpoint, create: bool) -> io::Result<File> {
    let parent = endpoint.path().parent().ok_or_else(denied)?;
    let uid = nix::unistd::geteuid().as_raw();
    let mut directory = open_directory(Path::new("/"))?;
    let components: Vec<_> = parent
        .components()
        .filter_map(|c| {
            if let Component::Normal(n) = c {
                Some(n)
            } else {
                None
            }
        })
        .collect();
    for (index, name) in components.iter().enumerate() {
        let path = anchored(&directory, name)?;
        if create && index + 1 == components.len() && !path.try_exists()? {
            use std::os::unix::fs::DirBuilderExt;
            match fs::DirBuilder::new().mode(0o700).create(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(e),
            }
        }
        directory = nix::fcntl::openat(
            &directory,
            *name,
            nix::fcntl::OFlag::O_RDONLY
                | nix::fcntl::OFlag::O_DIRECTORY
                | nix::fcntl::OFlag::O_NOFOLLOW
                | nix::fcntl::OFlag::O_CLOEXEC,
            nix::sys::stat::Mode::empty(),
        )?
        .into();
        let meta = directory.metadata()?;
        if meta.uid() != uid && meta.uid() != 0 {
            return Err(denied());
        }
        let mode = meta.mode();
        // Root-owned sticky ancestors permit temporary roots such as /tmp;
        // the immediate endpoint parent must still pass the stricter 0700 check.
        // See ../docs/internals/endpoints.md#linux-and-macos-checks.
        if mode & 0o022 != 0 && !(meta.uid() == 0 && mode & 0o1000 != 0) {
            return Err(denied());
        }
    }
    let meta = directory.metadata()?;
    if meta.uid() != uid || meta.mode() & 0o777 != 0o700 {
        return Err(denied());
    }
    Ok(directory)
}
fn check_socket(path: &Path) -> io::Result<fs::Metadata> {
    let meta = fs::symlink_metadata(path)?;
    if !meta.file_type().is_socket()
        || meta.uid() != nix::unistd::geteuid().as_raw()
        || meta.mode() & 0o777 != 0o600
        || meta.nlink() != 1
    {
        return Err(denied());
    }
    Ok(meta)
}
/// Verifies local credentials supplied by the kernel, never by a client message.
pub fn verify_local_peer(stream: &tokio::net::UnixStream) -> io::Result<()> {
    if stream.peer_cred()?.uid() != nix::unistd::geteuid().as_raw() {
        return Err(denied());
    }
    Ok(())
}
/// Connects to a protected endpoint and verifies the daemon's OS identity.
///
/// Does not create directories or impose an overall connection deadline.
/// The caller must supply its own timeout and retain any failure as distinct
/// from a typed product rejection.
pub async fn connect_local(endpoint: &LocalEndpoint) -> io::Result<tokio::net::UnixStream> {
    let parent = private_parent(endpoint, false)?;
    let address = anchored(&parent, endpoint.path().file_name().ok_or_else(denied)?)?;
    check_socket(&address)?;
    let stream = tokio::net::UnixStream::connect(&address).await?;
    verify_local_peer(&stream)?;
    Ok(stream)
}
/// Owns an endpoint lock and retained parent handle for the entire listener lifetime.
/// Socket entries are left for the next verified owner to recover after shutdown.
pub struct LocalListener {
    listener: std::sync::Mutex<Option<UnixListener>>,
    _parent: File,
    _lock: File,
}
impl LocalListener {
    /// Creates a protected listener or recovers an owned stale endpoint.
    pub fn bind(endpoint: &LocalEndpoint) -> io::Result<Self> {
        let parent = private_parent(endpoint, true)?;
        let name = endpoint.path().file_name().ok_or_else(denied)?;
        let address = anchored(&parent, name)?;
        let mut lock_name = name.to_os_string();
        lock_name.push(".lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
            .open(anchored(&parent, &lock_name)?)?;
        let meta = lock.metadata()?;
        if !meta.is_file()
            || meta.uid() != nix::unistd::geteuid().as_raw()
            || meta.mode() & 0o777 != 0o600
            || meta.nlink() != 1
        {
            return Err(denied());
        }
        lock.try_lock().map_err(|_| {
            io::Error::new(
                io::ErrorKind::AddrInUse,
                "local endpoint is owned by a live listener",
            )
        })?;
        // The lock protects the opened inode. A replaced path could let another
        // binder lock a different inode, so verify the entry after acquisition.
        let locked_entry = fs::symlink_metadata(anchored(&parent, &lock_name)?)?;
        if locked_entry.dev() != meta.dev() || locked_entry.ino() != meta.ino() {
            return Err(denied());
        }
        match fs::symlink_metadata(&address) {
            Ok(_) => {
                let original = check_socket(&address)?;
                use nix::sys::socket::{
                    AddressFamily, SockFlag, SockType, UnixAddr, connect, socket,
                };
                let probe = socket(
                    AddressFamily::Unix,
                    SockType::Stream,
                    SockFlag::empty(),
                    None,
                )?;
                nix::fcntl::fcntl(
                    &probe,
                    nix::fcntl::FcntlArg::F_SETFL(nix::fcntl::OFlag::O_NONBLOCK),
                )?;
                nix::fcntl::fcntl(
                    &probe,
                    nix::fcntl::FcntlArg::F_SETFD(nix::fcntl::FdFlag::FD_CLOEXEC),
                )?;
                let probe_address = UnixAddr::new(&address)?;
                // Only refusal permits stale recovery. A pending nonblocking
                // connect is not evidence that the endpoint can be removed.
                // See ../docs/internals/endpoints.md#unix-stale-recovery.
                match connect(probe.as_raw_fd(), &probe_address) {
                    Err(nix::errno::Errno::ECONNREFUSED) => {}
                    Ok(()) | Err(nix::errno::Errno::EAGAIN | nix::errno::Errno::EINPROGRESS) => {
                        return Err(io::Error::new(
                            io::ErrorKind::AddrInUse,
                            "local endpoint is active",
                        ));
                    }
                    Err(e) => return Err(e.into()),
                }
                // Capture the entry before deleting. Never unlink a concurrent replacement.
                let mut random = [0u8; 16];
                getrandom::fill(&mut random).map_err(io::Error::other)?;
                let suffix: String = random.iter().map(|b| format!("{b:02x}")).collect();
                let quarantine =
                    anchored(&parent, std::ffi::OsStr::new(&format!(".stale-{suffix}")))?;
                rustix::fs::renameat_with(
                    &parent,
                    name,
                    &parent,
                    quarantine.file_name().ok_or_else(denied)?,
                    rustix::fs::RenameFlags::NOREPLACE,
                )?;
                let captured = fs::symlink_metadata(&quarantine)?;
                if captured.dev() != original.dev() || captured.ino() != original.ino() {
                    // Preserve a replacement. Restore only if its original name is still vacant.
                    let _ = rustix::fs::renameat_with(
                        &parent,
                        quarantine.file_name().ok_or_else(denied)?,
                        &parent,
                        name,
                        rustix::fs::RenameFlags::NOREPLACE,
                    );
                    return Err(denied());
                }
                fs::remove_file(&quarantine)?;
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let listener = UnixListener::bind(&address)?;
        fs::set_permissions(&address, fs::Permissions::from_mode(0o600))?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            listener: std::sync::Mutex::new(Some(listener)),
            _parent: parent,
            _lock: lock,
        })
    }
    /// Takes the listening descriptor once; endpoint ownership remains with this guard.
    pub fn take_listener(&self) -> io::Result<UnixListener> {
        self.listener
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(|| io::Error::other("listener already taken"))
    }
}
