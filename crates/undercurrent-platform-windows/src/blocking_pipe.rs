// SPDX-License-Identifier: Apache-2.0
//! Blocking private-host IPC on the same owner-restricted pipe primitive.
//!
//! A single bounded reactor services every pipe. Callers remain ordinary blocking
//! host workers; no per-read or per-write OS thread is allocated.
use crate::{PipeListenerHandle, pipe_name, verify_pipe_client, verify_pipe_server};
use std::future::Future;
use std::io::{self, Read, Write};
use std::net::Shutdown;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;
use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient, NamedPipeServer};
use tokio::sync::watch;

pub(crate) fn reactor() -> io::Result<&'static tokio::runtime::Runtime> {
    static REACTOR: OnceLock<io::Result<tokio::runtime::Runtime>> = OnceLock::new();
    REACTOR
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("uc-private-ipc")
                .enable_all()
                .build()
        })
        .as_ref()
        .map_err(|e| io::Error::new(e.kind(), "private IPC reactor unavailable"))
}

struct ThreadWake(std::thread::Thread);
impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

// Poll on the blocking caller rather than Runtime::block_on: synchronous callers
// can also originate in another Tokio context, just as they can use UnixStream.
pub(crate) fn wait<F: Future>(future: F) -> io::Result<F::Output> {
    let _entered = reactor()?.enter();
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return Ok(value),
            Poll::Pending => std::thread::park(),
        }
    }
}

enum Pipe {
    Server(NamedPipeServer),
    Client(NamedPipeClient),
}
impl Pipe {
    async fn readable(&self) -> io::Result<()> {
        match self {
            Self::Server(p) => p.readable().await,
            Self::Client(p) => p.readable().await,
        }
    }
    async fn writable(&self) -> io::Result<()> {
        match self {
            Self::Server(p) => p.writable().await,
            Self::Client(p) => p.writable().await,
        }
    }
    fn read(&self, bytes: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Server(p) => p.try_read(bytes),
            Self::Client(p) => p.try_read(bytes),
        }
    }
    fn write(&self, bytes: &[u8]) -> io::Result<usize> {
        match self {
            Self::Server(p) => p.try_write(bytes),
            Self::Client(p) => p.try_write(bytes),
        }
    }
}

struct State {
    pipe: Option<Arc<Pipe>>,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
}
struct Shared {
    state: Mutex<State>,
    closed: watch::Sender<bool>,
}

/// Blocking byte stream whose clones share the connection, timeouts, and shutdown.
///
/// Read and write calls can proceed independently, but callers must serialize
/// complete protocol frames. Each call snapshots its configured timeout; this
/// is not a total request deadline. A timeout does not close the connection.
#[derive(Clone)]
pub struct BlockingPipeStream(Arc<Shared>);
impl BlockingPipeStream {
    fn new(pipe: Pipe) -> Self {
        Self(Arc::new(Shared {
            state: Mutex::new(State {
                pipe: Some(Arc::new(pipe)),
                read_timeout: None,
                write_timeout: None,
            }),
            closed: watch::channel(false).0,
        }))
    }

    /// Opens once and verifies the server's account. Busy-pipe errors propagate;
    /// no connection retry, application handshake, or deadline is supplied here.
    pub fn connect(path: impl AsRef<Path>) -> io::Result<Self> {
        let name = pipe_name(path.as_ref())?;
        let _entered = reactor()?.enter();
        let pipe = ClientOptions::new().open(&name)?;
        verify_pipe_server(&pipe)?;
        Ok(Self::new(Pipe::Client(pipe)))
    }
    pub fn try_clone(&self) -> io::Result<Self> {
        Ok(self.clone())
    }

    /// Queries the kernel endpoint and verifies the peer still belongs to this account.
    pub fn peer_process_id(&self) -> io::Result<u32> {
        let state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        match state.pipe.as_deref().ok_or_else(closed)? {
            Pipe::Server(pipe) => crate::pipe::client_process_id(pipe),
            Pipe::Client(pipe) => crate::pipe::server_process_id(pipe),
        }
    }

    /// Sets the timeout for future reads on all clones. `None` waits indefinitely;
    /// a zero duration is invalid. Already-started reads keep their prior value.
    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        validate_timeout(timeout)?;
        self.0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .read_timeout = timeout;
        Ok(())
    }
    /// Sets the timeout for future writes on all clones, rejecting zero durations.
    /// `None` waits indefinitely; already-started writes keep their prior value.
    pub fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        validate_timeout(timeout)?;
        self.0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .write_timeout = timeout;
        Ok(())
    }
    /// Closes shared state and wakes clone I/O waits. Only `Shutdown::Both` is
    /// supported; repeated full shutdown succeeds. Reads observe EOF and writes
    /// observe `BrokenPipe`, subject to races with already-ready I/O. Delivered
    /// bytes and application effects are not rolled back.
    pub fn shutdown(&self, direction: Shutdown) -> io::Result<()> {
        if direction != Shutdown::Both {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "private pipes require full shutdown",
            ));
        }
        // In-flight I/O retains its own Arc<Pipe>, so removing this reference
        // alone cannot wake parked clones. Publish closure after removing the
        // shared handle so subsequent calls also observe the closed state.
        // See [shutdown lifetimes](../docs/internals/private-ipc.md#stop-streams-and-listeners).
        self.0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pipe
            .take();
        self.0.closed.send_replace(true);
        Ok(())
    }
}

fn validate_timeout(timeout: Option<Duration>) -> io::Result<()> {
    if timeout.is_some_and(|t| t.is_zero()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "zero pipe timeout",
        ));
    }
    Ok(())
}
fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "private pipe closed")
}

async fn deadline<F: Future<Output = io::Result<usize>>>(
    timeout: Option<Duration>,
    future: F,
) -> io::Result<usize> {
    match timeout {
        Some(timeout) => tokio::time::timeout(timeout, future).await.map_err(|_| {
            io::Error::new(io::ErrorKind::TimedOut, "private pipe deadline exceeded")
        })?,
        None => future.await,
    }
}

impl Read for BlockingPipeStream {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let mut closed = self.0.closed.subscribe();
        let (pipe, timeout) = {
            let state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
            let Some(pipe) = state.pipe.clone() else {
                return Ok(0);
            };
            (pipe, state.read_timeout)
        };
        wait(async {
            deadline(timeout, async {
                loop {
                    if *closed.borrow() {
                        return Ok(0);
                    }
                    tokio::select! {
                        _ = closed.changed() => return Ok(0),
                        ready = pipe.readable() => ready?,
                    }
                    match pipe.read(bytes) {
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                        Err(e) if e.kind() == io::ErrorKind::BrokenPipe => return Ok(0),
                        result => return result,
                    }
                }
            })
            .await
        })?
    }
}
impl Write for BlockingPipeStream {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let mut cancelled = self.0.closed.subscribe();
        let (pipe, timeout) = {
            let state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
            (state.pipe.clone().ok_or_else(closed)?, state.write_timeout)
        };
        wait(async {
            deadline(timeout, async {
                loop {
                    if *cancelled.borrow() {
                        return Err(closed());
                    }
                    tokio::select! {
                        _ = cancelled.changed() => return Err(closed()),
                        ready = pipe.writable() => ready?,
                    }
                    match pipe.write(bytes) {
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                        result => return result,
                    }
                }
            })
            .await
        })?
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Serialized private-pipe accepts with an optional single-poll mode.
///
/// Blocking accepts have no internal cancellation or timeout. A caller needing
/// bounded shutdown should own a nonblocking accept loop and its stop condition.
pub struct BlockingPipeListener {
    name: String,
    next: Mutex<NamedPipeServer>,
    nonblocking: AtomicBool,
}
impl BlockingPipeListener {
    pub fn bind(path: impl AsRef<Path>) -> io::Result<Self> {
        let name = pipe_name(path.as_ref())?;
        let first = PipeListenerHandle::create(&name, true)?;
        let _entered = reactor()?.enter();
        Ok(Self {
            name,
            next: Mutex::new(first.into_async()?),
            nonblocking: AtomicBool::new(false),
        })
    }
    /// Changes subsequent accepts; does not interrupt a blocking accept already
    /// in progress. Nonblocking mode does not bypass the listener mutex.
    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.nonblocking.store(nonblocking, Ordering::Release);
        Ok(())
    }
    /// Installs a successor pipe instance before returning the verified client.
    /// Peer account or successor-creation failures can occur after connection.
    /// Nonblocking mode returns `WouldBlock` when connection polling is pending.
    pub fn accept(&self) -> io::Result<(BlockingPipeStream, ())> {
        let mut next = self.next.lock().unwrap_or_else(|e| e.into_inner());
        if self.nonblocking.load(Ordering::Acquire) {
            let _entered = reactor()?.enter();
            let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
            let mut context = Context::from_waker(&waker);
            match std::pin::pin!(next.connect()).as_mut().poll(&mut context) {
                Poll::Ready(result) => result?,
                Poll::Pending => return Err(io::ErrorKind::WouldBlock.into()),
            }
        } else {
            wait(next.connect())??;
        }
        let _entered = reactor()?.enter();
        // Keep an instance under the listener's ownership before handing off or
        // rejecting this peer; otherwise dropping the last connected instance
        // could release the pipe name between accepts.
        // See [accept ordering](../docs/internals/private-ipc.md#accept-ordering-and-protocol-ownership).
        let replacement = PipeListenerHandle::create(&self.name, false)?.into_async()?;
        let connected = std::mem::replace(&mut *next, replacement);
        verify_pipe_client(&connected)?;
        Ok((BlockingPipeStream::new(Pipe::Server(connected)), ()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn nonblocking_accept_and_kernel_peer_identity() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("bridge");
        let listener = BlockingPipeListener::bind(&path).unwrap();
        listener.set_nonblocking(true).unwrap();
        assert!(matches!(listener.accept(), Err(e) if e.kind() == io::ErrorKind::WouldBlock));
        let client = BlockingPipeStream::connect(&path).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let server = loop {
            match listener.accept() {
                Ok((server, _)) => break server,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    assert!(std::time::Instant::now() < deadline);
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) => panic!("accept: {e}"),
            }
        };
        assert_eq!(server.peer_process_id().unwrap(), std::process::id());
        assert_eq!(client.peer_process_id().unwrap(), std::process::id());
        assert!(matches!(listener.accept(), Err(e) if e.kind() == io::ErrorKind::WouldBlock));
    }
    #[test]
    fn private_pipe_timeout_recovery_and_clone_shutdown() {
        let path =
            std::env::temp_dir().join(format!("uc-private-pipe-test-{}.sock", std::process::id()));
        let listener = BlockingPipeListener::bind(&path).unwrap();
        let mut client = BlockingPipeStream::connect(&path).unwrap();
        let (mut server, _) = listener.accept().unwrap();
        server
            .set_read_timeout(Some(Duration::from_millis(20)))
            .unwrap();
        let mut bytes = [0; 4];
        assert_eq!(
            server.read(&mut bytes).unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        client.write_all(b"test").unwrap();
        server
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        server.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"test");
        let shutdown = server.try_clone().unwrap();
        let worker = std::thread::spawn(move || server.read(&mut [0]));
        shutdown.shutdown(Shutdown::Both).unwrap();
        assert_eq!(worker.join().unwrap().unwrap(), 0);
        client
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        assert_eq!(client.read(&mut [0]).unwrap(), 0);
    }
}
