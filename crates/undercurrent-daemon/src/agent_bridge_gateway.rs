// SPDX-License-Identifier: Apache-2.0

//! Owner-only local gateway for per-attempt agent MCP façades.

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
#[cfg(unix)]
use std::fs;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
#[cfg(windows)]
use undercurrent_platform_windows::{
    BlockingPipeListener as UnixListener, BlockingPipeStream as UnixStream,
};

use undercurrent_application::{
    AgentChannelRepository, AgentChannelTargetResolution, AgentEventAdmission,
    AgentEventAdmissionOutcome, Clock, ResolvedAgentChannelBinding,
};
use undercurrent_domain::PayloadDigest;
use undercurrent_protocol::{
    AGENT_BRIDGE_PROTOCOL_VERSION, AgentBridgeCallResultDto, AgentBridgeDescribeResultDto,
    AgentBridgeErrorDto, AgentBridgeRequestDto, AgentBridgeResponseDto, AgentEventReplyOutcomeDto,
    AgentToolCallDto, MAX_AGENT_EVENT_FRAME_BYTES, MAX_AGENT_INLINE_RESULT_BYTES,
    MAX_AGENT_MESSAGE_BYTES, canonical_agent_event_reply_outcome, canonical_agent_schema_document,
    canonical_agent_tool_call,
};
use undercurrent_runtime::HostedProcessRuntime;

use crate::{AgentEventNotificationHub, blocking::block_on};

const WORKERS: usize = 4;
const QUEUE_CAPACITY: usize = 32;
const IO_TIMEOUT: Duration = Duration::from_secs(5);
const REPLY_LIVENESS_INTERVAL: Duration = Duration::from_millis(250);

/// Process-owned listener and fixed worker pool for the protected local channel.
pub struct AgentBridgeGateway {
    #[cfg(unix)]
    socket: PathBuf,
    shutdown: Arc<AtomicBool>,
    queue: async_channel::Sender<UnixStream>,
    acceptor: Mutex<Option<JoinHandle<()>>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

impl AgentBridgeGateway {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start(
        socket: impl Into<PathBuf>,
        expected_bridge_executable: impl Into<PathBuf>,
        runtime: Arc<HostedProcessRuntime>,
        repository: Arc<dyn AgentChannelRepository>,
        notifications: Arc<AgentEventNotificationHub>,
        clock: Arc<dyn Clock>,
        worker_stack_bytes: usize,
    ) -> Result<Self, AgentBridgeGatewayStartError> {
        let socket = socket.into();
        let expected_bridge_executable = expected_bridge_executable.into();
        if !socket.is_absolute()
            || !expected_bridge_executable.is_absolute()
            || worker_stack_bytes < 64 * 1024
        {
            return Err(AgentBridgeGatewayStartError::InvalidConfiguration);
        }
        prepare_socket(&socket)?;
        let listener = UnixListener::bind(&socket).map_err(AgentBridgeGatewayStartError::Io)?;
        #[cfg(unix)]
        fs::set_permissions(&socket, fs::Permissions::from_mode(0o600))
            .map_err(AgentBridgeGatewayStartError::Io)?;
        listener
            .set_nonblocking(true)
            .map_err(AgentBridgeGatewayStartError::Io)?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let (queue, receiver) = async_channel::bounded(QUEUE_CAPACITY);
        let mut startup = GatewayStartupGuard {
            #[cfg(unix)]
            socket: socket.clone(),
            shutdown: shutdown.clone(),
            queue: queue.clone(),
            acceptor: None,
            workers: Vec::with_capacity(WORKERS),
            armed: true,
        };
        for index in 0..WORKERS {
            let receiver = receiver.clone();
            let runtime = runtime.clone();
            let repository = repository.clone();
            let notifications = notifications.clone();
            let clock = clock.clone();
            let shutdown = shutdown.clone();
            let expected = expected_bridge_executable.clone();
            startup.workers.push(
                thread::Builder::new()
                    .name(format!("undercurrent-agent-bridge-{index}"))
                    .stack_size(worker_stack_bytes)
                    .spawn(move || {
                        while let Ok(stream) = receiver.recv_blocking() {
                            handle_connection(
                                stream,
                                &runtime,
                                repository.as_ref(),
                                &notifications,
                                clock.as_ref(),
                                &expected,
                                &shutdown,
                            );
                        }
                    })
                    .map_err(AgentBridgeGatewayStartError::Worker)?,
            );
        }
        let accept_shutdown = shutdown.clone();
        let accept_queue = queue.clone();
        startup.acceptor = Some(
            thread::Builder::new()
                .name("undercurrent-agent-bridge-accept".to_owned())
                .stack_size(worker_stack_bytes)
                .spawn(move || {
                    while !accept_shutdown.load(Ordering::Acquire) {
                        match listener.accept() {
                            Ok((stream, _)) => {
                                let _ = accept_queue.try_send(stream);
                            }
                            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                thread::sleep(Duration::from_millis(10));
                            }
                            Err(_) => break,
                        }
                    }
                    accept_queue.close();
                })
                .map_err(AgentBridgeGatewayStartError::Worker)?,
        );
        startup.armed = false;
        Ok(Self {
            #[cfg(unix)]
            socket,
            shutdown,
            queue,
            acceptor: Mutex::new(startup.acceptor.take()),
            workers: Mutex::new(std::mem::take(&mut startup.workers)),
        })
    }
}

struct GatewayStartupGuard {
    #[cfg(unix)]
    socket: PathBuf,
    shutdown: Arc<AtomicBool>,
    queue: async_channel::Sender<UnixStream>,
    acceptor: Option<JoinHandle<()>>,
    workers: Vec<JoinHandle<()>>,
    armed: bool,
}

impl Drop for GatewayStartupGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.shutdown.store(true, Ordering::Release);
        self.queue.close();
        if let Some(acceptor) = self.acceptor.take() {
            let _ = acceptor.join();
        }
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
        #[cfg(unix)]
        if fs::symlink_metadata(&self.socket).is_ok_and(|metadata| metadata.file_type().is_socket())
        {
            let _ = fs::remove_file(&self.socket);
        }
    }
}

impl Debug for AgentBridgeGateway {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentBridgeGateway")
            .field("socket", &"<redacted>")
            .field("closed", &self.shutdown.load(Ordering::Acquire))
            .finish()
    }
}

impl Drop for AgentBridgeGateway {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        self.queue.close();
        if let Some(acceptor) = self
            .acceptor
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            let _ = acceptor.join();
        }
        for worker in self
            .workers
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .drain(..)
        {
            let _ = worker.join();
        }
        #[cfg(unix)]
        if fs::symlink_metadata(&self.socket).is_ok_and(|metadata| metadata.file_type().is_socket())
        {
            let _ = fs::remove_file(&self.socket);
        }
    }
}

#[cfg(unix)]
fn prepare_socket(path: &Path) -> Result<(), AgentBridgeGatewayStartError> {
    let parent = path
        .parent()
        .ok_or(AgentBridgeGatewayStartError::InvalidConfiguration)?;
    let metadata = fs::metadata(parent).map_err(AgentBridgeGatewayStartError::Io)?;
    if !metadata.is_dir()
        || metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(AgentBridgeGatewayStartError::UntrustedDirectory);
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {
            if UnixStream::connect(path).is_ok() {
                return Err(AgentBridgeGatewayStartError::AlreadyRunning);
            }
            fs::remove_file(path).map_err(AgentBridgeGatewayStartError::Io)
        }
        Ok(_) => Err(AgentBridgeGatewayStartError::UnsafeExistingPath),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(AgentBridgeGatewayStartError::Io(error)),
    }
}

#[cfg(windows)]
fn prepare_socket(path: &Path) -> Result<(), AgentBridgeGatewayStartError> {
    let parent = path
        .parent()
        .ok_or(AgentBridgeGatewayStartError::InvalidConfiguration)?;
    undercurrent_platform_windows::prepare_private_directory(parent)
        .map(drop)
        .map_err(AgentBridgeGatewayStartError::Io)
}

fn handle_connection(
    mut stream: UnixStream,
    runtime: &HostedProcessRuntime,
    repository: &dyn AgentChannelRepository,
    notifications: &Arc<AgentEventNotificationHub>,
    clock: &dyn Clock,
    expected_bridge_executable: &Path,
    shutdown: &AtomicBool,
) {
    let _ = stream.set_read_timeout(Some(IO_TIMEOUT));
    let _ = stream.set_write_timeout(Some(IO_TIMEOUT));
    let response = (|| {
        #[cfg(target_os = "linux")]
        let credentials =
            nix::sys::socket::getsockopt(&stream, nix::sys::socket::sockopt::PeerCredentials)
                .map_err(|_| GatewayError::Unauthenticated)?;
        #[cfg(target_os = "macos")]
        let credentials =
            nix::sys::socket::getsockopt(&stream, nix::sys::socket::sockopt::LocalPeerCred)
                .map_err(|_| GatewayError::Unauthenticated)?;
        #[cfg(unix)]
        if credentials.uid() != nix::unistd::geteuid().as_raw() {
            return Err(GatewayError::Unauthenticated);
        }
        let request = read_request(&mut stream)?;
        #[cfg(target_os = "linux")]
        let peer_pid =
            u32::try_from(credentials.pid()).map_err(|_| GatewayError::Unauthenticated)?;
        #[cfg(target_os = "macos")]
        let peer_pid = u32::try_from(
            nix::sys::socket::getsockopt(&stream, nix::sys::socket::sockopt::LocalPeerPid)
                .map_err(|_| GatewayError::Unauthenticated)?,
        )
        .map_err(|_| GatewayError::Unauthenticated)?;
        #[cfg(windows)]
        let peer_pid = stream
            .peer_process_id()
            .map_err(|_| GatewayError::Unauthenticated)?;
        let peer = runtime
            .resolve_agent_peer(peer_pid, expected_bridge_executable)
            .map_err(|_| GatewayError::Unauthenticated)?
            .ok_or(GatewayError::Unauthenticated)?;
        let resolved = match block_on(repository.resolve_agent_channel_target(
            peer.session_id(),
            peer.process_id(),
            clock.now(),
        ))
        .map_err(|_| GatewayError::Unavailable)?
        {
            AgentChannelTargetResolution::Current(binding) => *binding,
            AgentChannelTargetResolution::BindingExpired => return Err(GatewayError::Expired),
            AgentChannelTargetResolution::NotFound => return Err(GatewayError::Stale),
            AgentChannelTargetResolution::Ambiguous => return Err(GatewayError::Ambiguous),
        };
        if peer
            .attempt_id()
            .is_some_and(|attempt_id| attempt_id != &resolved.target.attempt_id)
        {
            return Err(GatewayError::Stale);
        }
        dispatch_request(
            request,
            resolved,
            peer_pid,
            runtime,
            repository,
            notifications,
            clock,
            expected_bridge_executable,
            shutdown,
        )
    })();
    let response = response.unwrap_or_else(|error| AgentBridgeResponseDto::Error(error.dto()));
    let _ = write_response(&mut stream, &response);
}

#[allow(clippy::too_many_arguments)]
fn dispatch_request(
    request: AgentBridgeRequestDto,
    binding: ResolvedAgentChannelBinding,
    peer_pid: u32,
    runtime: &HostedProcessRuntime,
    repository: &dyn AgentChannelRepository,
    notifications: &Arc<AgentEventNotificationHub>,
    clock: &dyn Clock,
    expected_bridge_executable: &Path,
    shutdown: &AtomicBool,
) -> Result<AgentBridgeResponseDto, GatewayError> {
    match request {
        AgentBridgeRequestDto::Describe(request) => {
            if request.protocol_version != AGENT_BRIDGE_PROTOCOL_VERSION {
                return Err(GatewayError::Protocol);
            }
            let schema: serde_json::Value =
                serde_json::from_slice(&binding.expected_result_schema_canonical_json)
                    .map_err(|_| GatewayError::Internal)?;
            if !matches!(
                canonical_agent_schema_document(&schema),
                Ok(canonical) if canonical == binding.expected_result_schema_canonical_json
            ) {
                return Err(GatewayError::Internal);
            }
            Ok(AgentBridgeResponseDto::Describe(
                AgentBridgeDescribeResultDto {
                    protocol_version: AGENT_BRIDGE_PROTOCOL_VERSION,
                    expected_result_schema_document: schema,
                },
            ))
        }
        AgentBridgeRequestDto::Call(request) => {
            if request.protocol_version != AGENT_BRIDGE_PROTOCOL_VERSION
                || request.request_id.is_empty()
                || request.request_id.len() > 128
                || request.request_id.chars().any(char::is_control)
            {
                return Err(GatewayError::Protocol);
            }
            validate_tool_call(&request.call)?;
            let call =
                canonical_agent_tool_call(&request.call).map_err(|_| GatewayError::Protocol)?;
            let admission = AgentEventAdmission {
                target: binding.target.clone(),
                request_id: request.request_id.clone(),
                call_digest: PayloadDigest::of_bytes(&call),
                call_canonical_json: call,
                received_at: clock.now(),
            };
            let event_id = match block_on(repository.enqueue_agent_event(&admission))
                .map_err(|_| GatewayError::Unavailable)?
            {
                AgentEventAdmissionOutcome::Queued(event)
                | AgentEventAdmissionOutcome::DuplicatePending(event) => event.event_id,
                AgentEventAdmissionOutcome::DuplicateReplied(reply) => {
                    return retained_reply(request.request_id, reply);
                }
                AgentEventAdmissionOutcome::RequestConflict => return Err(GatewayError::Conflict),
                AgentEventAdmissionOutcome::CapacityExceeded => return Err(GatewayError::Capacity),
                AgentEventAdmissionOutcome::StaleTarget => return Err(GatewayError::Stale),
                AgentEventAdmissionOutcome::BindingExpired => return Err(GatewayError::Expired),
                AgentEventAdmissionOutcome::NotFound => return Err(GatewayError::Stale),
            };
            // Register before waking event readers, then read the durable reply
            // once even without a notification. A reply committed before this
            // registration has no wake to replay; the notification generation
            // keeps later wakes from being missed between the read and wait.
            // See [commit/wake ordering](../docs/internals/requests/agent-channel.md#commit-before-waking-readers).
            let reply_notification =
                notifications.subscribe_reply(binding.target.attempt_id.clone(), event_id.clone());
            let mut reply_generation = reply_notification.generation();
            notifications.notify(&binding.target.attempt_id);
            let mut read_reply = true;
            loop {
                if shutdown.load(Ordering::Acquire) {
                    return Err(GatewayError::Unavailable);
                }
                let now = clock.now();
                if now >= binding.expires_at {
                    return Err(GatewayError::Expired);
                }
                if runtime
                    .resolve_agent_peer(peer_pid, expected_bridge_executable)
                    .map_err(|_| GatewayError::Stale)?
                    .is_none()
                {
                    return Err(GatewayError::Stale);
                }
                if read_reply {
                    if let Some(reply) =
                        block_on(repository.get_agent_event_reply(&binding.target, &event_id, now))
                            .map_err(|_| GatewayError::Unavailable)?
                    {
                        return retained_reply(request.request_id, reply);
                    }
                    if reply_generation != 0 {
                        return Err(GatewayError::Internal);
                    }
                    read_reply = false;
                }
                let remaining_millis =
                    u64::try_from(binding.expires_at.value().saturating_sub(now.value()))
                        .unwrap_or(u64::MAX);
                let wait = REPLY_LIVENESS_INTERVAL.min(Duration::from_millis(remaining_millis));
                let next_generation = reply_notification.wait_for_change(reply_generation, wait);
                if next_generation != reply_generation {
                    reply_generation = next_generation;
                    read_reply = true;
                }
            }
        }
    }
}

fn retained_reply(
    request_id: String,
    reply: undercurrent_application::AgentEventReplyRecord,
) -> Result<AgentBridgeResponseDto, GatewayError> {
    if PayloadDigest::of_bytes(&reply.outcome_canonical_json) != reply.reply_digest {
        return Err(GatewayError::Internal);
    }
    let outcome: AgentEventReplyOutcomeDto = serde_json::from_slice(&reply.outcome_canonical_json)
        .map_err(|_| GatewayError::Internal)?;
    if !matches!(
        canonical_agent_event_reply_outcome(&outcome),
        Ok(canonical) if canonical == reply.outcome_canonical_json
    ) {
        return Err(GatewayError::Internal);
    }
    Ok(AgentBridgeResponseDto::Call(AgentBridgeCallResultDto {
        protocol_version: AGENT_BRIDGE_PROTOCOL_VERSION,
        request_id,
        outcome,
    }))
}

fn validate_tool_call(call: &AgentToolCallDto) -> Result<(), GatewayError> {
    match call {
        AgentToolCallDto::Begin => Ok(()),
        AgentToolCallDto::Progress { message } => validate_message(message),
        AgentToolCallDto::Submit { result } => {
            let bytes = serde_json::to_vec(result).map_err(|_| GatewayError::Protocol)?;
            if bytes.len() > MAX_AGENT_INLINE_RESULT_BYTES {
                Err(GatewayError::Capacity)
            } else {
                Ok(())
            }
        }
        AgentToolCallDto::NeedInput {
            question,
            answer_schema,
            choices,
        } => {
            validate_message(question)?;
            if choices.len() > 128 {
                return Err(GatewayError::Capacity);
            }
            if answer_schema.as_ref().is_some_and(|schema| {
                serde_json::to_vec(schema).map_or(true, |bytes| bytes.len() > 64 * 1024)
            }) || serde_json::to_vec(choices)
                .map_or(true, |bytes| bytes.len() > MAX_AGENT_INLINE_RESULT_BYTES)
            {
                return Err(GatewayError::Capacity);
            }
            Ok(())
        }
        AgentToolCallDto::TurnBoundary { turn_ordinal } => {
            if *turn_ordinal == 0 {
                Err(GatewayError::Protocol)
            } else {
                Ok(())
            }
        }
    }
}

fn validate_message(message: &str) -> Result<(), GatewayError> {
    if message.is_empty() || message.len() > MAX_AGENT_MESSAGE_BYTES || message.contains('\0') {
        Err(GatewayError::Protocol)
    } else {
        Ok(())
    }
}

fn read_request(stream: &mut UnixStream) -> Result<AgentBridgeRequestDto, GatewayError> {
    let mut length = [0_u8; 4];
    stream
        .read_exact(&mut length)
        .map_err(|_| GatewayError::Protocol)?;
    let length = usize::try_from(u32::from_be_bytes(length)).map_err(|_| GatewayError::Capacity)?;
    if length == 0 || length > MAX_AGENT_EVENT_FRAME_BYTES {
        return Err(GatewayError::Capacity);
    }
    let mut payload = vec![0_u8; length];
    stream
        .read_exact(&mut payload)
        .map_err(|_| GatewayError::Protocol)?;
    serde_json::from_slice(&payload).map_err(|_| GatewayError::Protocol)
}

fn write_response(
    stream: &mut UnixStream,
    response: &AgentBridgeResponseDto,
) -> Result<(), GatewayError> {
    let payload = serde_json::to_vec(response).map_err(|_| GatewayError::Internal)?;
    if payload.is_empty() || payload.len() > MAX_AGENT_EVENT_FRAME_BYTES {
        return Err(GatewayError::Capacity);
    }
    let length = u32::try_from(payload.len()).map_err(|_| GatewayError::Capacity)?;
    stream
        .write_all(&length.to_be_bytes())
        .and_then(|_| stream.write_all(&payload))
        .and_then(|_| stream.flush())
        .map_err(|_| GatewayError::Unavailable)
}

#[derive(Clone, Copy)]
enum GatewayError {
    Unauthenticated,
    Protocol,
    Capacity,
    Conflict,
    Stale,
    Expired,
    Ambiguous,
    Unavailable,
    Internal,
}

impl GatewayError {
    fn dto(self) -> AgentBridgeErrorDto {
        let (code, safe_message, retryable) = match self {
            Self::Unauthenticated => (
                "unauthenticated",
                "agent bridge authentication failed",
                false,
            ),
            Self::Protocol => ("invalid_request", "agent bridge request is invalid", false),
            Self::Capacity => (
                "capacity_exceeded",
                "agent bridge capacity is exhausted",
                true,
            ),
            Self::Conflict => (
                "request_conflict",
                "agent bridge request identity conflicts",
                false,
            ),
            Self::Stale => ("stale_attempt", "agent attempt is no longer current", false),
            Self::Expired => (
                "binding_expired",
                "agent attempt binding has expired",
                false,
            ),
            Self::Ambiguous => (
                "ambiguous_binding",
                "agent runtime binding is ambiguous",
                false,
            ),
            Self::Unavailable => (
                "unavailable",
                "agent bridge is temporarily unavailable",
                true,
            ),
            Self::Internal => ("internal", "agent bridge failed internally", false),
        };
        AgentBridgeErrorDto {
            code: code.to_owned(),
            safe_message: safe_message.to_owned(),
            retryable,
        }
    }
}

#[derive(Debug)]
pub enum AgentBridgeGatewayStartError {
    InvalidConfiguration,
    UntrustedDirectory,
    UnsafeExistingPath,
    AlreadyRunning,
    Io(std::io::Error),
    Worker(std::io::Error),
}

impl Display for AgentBridgeGatewayStartError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidConfiguration => {
                formatter.write_str("agent bridge gateway configuration is invalid")
            }
            Self::UntrustedDirectory => {
                formatter.write_str("agent bridge gateway directory is not owner-only")
            }
            Self::UnsafeExistingPath => {
                formatter.write_str("agent bridge gateway path is not a socket")
            }
            Self::AlreadyRunning => formatter.write_str("agent bridge gateway is already active"),
            Self::Io(_) => formatter.write_str("agent bridge gateway socket could not start"),
            Self::Worker(_) => formatter.write_str("agent bridge gateway worker could not start"),
        }
    }
}

impl Error for AgentBridgeGatewayStartError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(source) | Self::Worker(source) => Some(source),
            _ => None,
        }
    }
}
