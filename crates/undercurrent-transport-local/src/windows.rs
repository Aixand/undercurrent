// SPDX-License-Identifier: Apache-2.0
//! Windows named-pipe implementation of the authenticated local byte stream.
//!
//! The platform crate derives an account-scoped name from the configured path
//! and owns ACL and peer-token checks. No socket or lock file is created.
//! Pipe handles retain the kernel name; each accept creates the successor
//! instance before returning the connected one.
use crate::LocalEndpoint;
use std::{
    io,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient, NamedPipeServer};
use undercurrent_platform_windows::{PipeListenerHandle, verify_pipe_client, verify_pipe_server};

/// Both pipe ends implement the same bounded framing contract.
pub enum LocalStream {
    /// Accepted server-side pipe instance.
    Server(NamedPipeServer),
    /// Connected client-side pipe instance.
    Client(NamedPipeClient),
}

impl AsyncRead for LocalStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Server(pipe) => Pin::new(pipe).poll_read(cx, buf),
            Self::Client(pipe) => Pin::new(pipe).poll_read(cx, buf),
        }
    }
}
impl AsyncWrite for LocalStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Server(pipe) => Pin::new(pipe).poll_write(cx, buf),
            Self::Client(pipe) => Pin::new(pipe).poll_write(cx, buf),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Server(pipe) => Pin::new(pipe).poll_flush(cx),
            Self::Client(pipe) => Pin::new(pipe).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Server(pipe) => Pin::new(pipe).poll_shutdown(cx),
            Self::Client(pipe) => Pin::new(pipe).poll_shutdown(cx),
        }
    }
}

// Keep the public endpoint path contract. The pipe lives in the local kernel
// namespace, scoped by the owner's SID and the configured path's UTF-16 spelling.
// Client and server must use the same spelling; aliases are not canonicalized.
// See ../docs/internals/endpoints.md#windows-named-pipes.
fn pipe_name(endpoint: &LocalEndpoint) -> io::Result<String> {
    undercurrent_platform_windows::pipe_name(endpoint.path())
}

/// Checks the kernel-reported peer process token against the current account.
///
/// Identity lookup failure is an error; no message-supplied identity is trusted.
pub fn verify_local_peer(stream: &LocalStream) -> io::Result<()> {
    match stream {
        LocalStream::Server(pipe) => verify_pipe_client(pipe),
        LocalStream::Client(pipe) => verify_pipe_server(pipe),
    }
}

/// Opens the account-scoped pipe and verifies the server's account.
///
/// Retries only `ERROR_PIPE_BUSY`, sleeping 10 ms per attempt. The caller owns
/// the overall deadline; other errors return without a network fallback.
pub async fn connect_local(endpoint: &LocalEndpoint) -> io::Result<LocalStream> {
    let name = pipe_name(endpoint)?;
    loop {
        match ClientOptions::new().open(&name) {
            Ok(pipe) => {
                verify_pipe_server(&pipe)?;
                return Ok(LocalStream::Client(pipe));
            }
            // ERROR_PIPE_BUSY is transient while the server replaces an accepted
            // instance. The caller owns the overall connection deadline.
            Err(error) if error.raw_os_error() == Some(231) => {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// First pipe instance awaiting transfer into a Tokio I/O reactor.
pub struct RawLocalListener {
    name: String,
    first: PipeListenerHandle,
}

/// Owns the first native pipe handle until it is taken once.
///
/// After transfer, the async listener and connected instances own the name's
/// lifetime. Unlike the Unix guard, this value holds no separate file lock.
pub struct LocalListener {
    listener: Mutex<Option<RawLocalListener>>,
}
impl LocalListener {
    /// Creates the exclusive first instance without requiring a Tokio runtime.
    pub fn bind(endpoint: &LocalEndpoint) -> io::Result<Self> {
        let name = pipe_name(endpoint)?;
        let first = PipeListenerHandle::create(&name, true)?;
        Ok(Self {
            listener: Mutex::new(Some(RawLocalListener { name, first })),
        })
    }
    /// Transfers the first instance once; a second call returns an I/O error.
    pub fn take_listener(&self) -> io::Result<RawLocalListener> {
        self.listener
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(|| io::Error::other("listener already taken"))
    }
}

/// Serializes accepts while retaining a successor pipe instance.
pub struct AsyncLocalListener {
    name: String,
    next: tokio::sync::Mutex<NamedPipeServer>,
}
impl AsyncLocalListener {
    /// Registers the first overlapped handle with the current Tokio I/O reactor.
    pub fn from_std(listener: RawLocalListener) -> io::Result<Self> {
        Ok(Self {
            name: listener.name,
            next: tokio::sync::Mutex::new(listener.first.into_async()?),
        })
    }
    /// Waits for a connection and creates its successor before returning it.
    ///
    /// The accepted peer still needs [`verify_local_peer`]. Cancelling the wait
    /// retains the pending instance in this listener. Successor creation errors
    /// are returned rather than releasing ownership to fabricate an accept.
    pub async fn accept(&self) -> io::Result<(LocalStream, ())> {
        let mut next = self.next.lock().await;
        next.connect().await?;
        // Create the successor before releasing the connected instance so that
        // another process cannot seize the pipe name between accepts.
        let replacement = PipeListenerHandle::create(&self.name, false)?.into_async()?;
        Ok((
            LocalStream::Server(std::mem::replace(&mut *next, replacement)),
            (),
        ))
    }
}
