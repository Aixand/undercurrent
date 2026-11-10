// SPDX-License-Identifier: Apache-2.0

//! Bounded blocking health-check execution outside application workers.

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use undercurrent_application::{
    HealthCheckExecutor, PortError, PortErrorKind, PortFuture, PortOperation, ProfilePatternEngine,
};
use undercurrent_domain::{
    HealthCheckResult, HealthRule, HealthRuleKind, HealthRules, PatternExpression,
    ProcessInstanceId, SessionId,
};

use crate::{RuntimeLimitError, TerminalHealthSample, VtTerminalScreenStore};

const MINIMUM_WORKER_STACK_BYTES: usize = 64 * 1024;
const MINIMUM_DETAIL_BYTES: usize = 32;
const COMMAND_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Stable payload-free details retained for individual health-check outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NativeHealthCheckDetail {
    /// Local TCP connection succeeded.
    TcpConnected,
    /// Local TCP listener was unavailable.
    TcpUnavailable,
    /// Local TCP connection reached its deadline.
    TcpTimeout,
    /// HTTP endpoint returned exactly status 200.
    HttpOk,
    /// HTTP endpoint returned another status.
    HttpStatusNotOk,
    /// HTTP request failed before receiving a response.
    HttpRequestFailed,
    /// HTTP request reached its deadline.
    HttpTimeout,
    /// Configured output pattern was present.
    PatternPresent,
    /// Configured output pattern was absent.
    PatternAbsent,
    /// Red foreground cells remained below the configured threshold.
    RedCellThresholdClear,
    /// Red foreground cells met or exceeded the configured threshold.
    RedCellThresholdMet,
    /// Direct check command exited successfully.
    CommandSucceeded,
    /// Direct check command exited unsuccessfully.
    CommandExitNonzero,
    /// Direct check command could not be started or observed.
    CommandSpawnFailed,
    /// Direct check command reached its deadline and was terminated.
    CommandTimeout,
}

impl NativeHealthCheckDetail {
    /// Complete detail registry in rule execution order.
    pub const ALL: [Self; 15] = [
        Self::TcpConnected,
        Self::TcpUnavailable,
        Self::TcpTimeout,
        Self::HttpOk,
        Self::HttpStatusNotOk,
        Self::HttpRequestFailed,
        Self::HttpTimeout,
        Self::PatternPresent,
        Self::PatternAbsent,
        Self::RedCellThresholdClear,
        Self::RedCellThresholdMet,
        Self::CommandSucceeded,
        Self::CommandExitNonzero,
        Self::CommandSpawnFailed,
        Self::CommandTimeout,
    ];

    /// Returns the stable safe diagnostic value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TcpConnected => "tcp_connected",
            Self::TcpUnavailable => "tcp_unavailable",
            Self::TcpTimeout => "tcp_timeout",
            Self::HttpOk => "http_ok",
            Self::HttpStatusNotOk => "http_status_not_ok",
            Self::HttpRequestFailed => "http_request_failed",
            Self::HttpTimeout => "http_timeout",
            Self::PatternPresent => "pattern_present",
            Self::PatternAbsent => "pattern_absent",
            Self::RedCellThresholdClear => "red_cell_threshold_clear",
            Self::RedCellThresholdMet => "red_cell_threshold_met",
            Self::CommandSucceeded => "command_succeeded",
            Self::CommandExitNonzero => "command_exit_nonzero",
            Self::CommandSpawnFailed => "command_spawn_failed",
            Self::CommandTimeout => "command_timeout",
        }
    }
}

/// Hard concurrency, deadline, extraction, and result limits for health checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeHealthCheckExecutorLimits {
    /// Fixed blocking worker count shared by all sessions.
    pub workers: usize,
    /// Requests admitted while every worker is occupied.
    pub queue_capacity: usize,
    /// Per-rule TCP, HTTP, and command deadline.
    pub check_timeout: Duration,
    /// Maximum private visible-screen bytes examined per request.
    pub screen_text_bytes: usize,
    /// Maximum visible cells examined per request.
    pub screen_cells: usize,
    /// Maximum configured pattern bytes compiled per screen rule.
    pub pattern_bytes: usize,
    /// Maximum safe detail bytes retained per rule result.
    pub detail_bytes: usize,
    /// Stack reserved for each fixed blocking worker.
    pub worker_thread_stack_bytes: usize,
}

impl NativeHealthCheckExecutorLimits {
    /// Validates every explicit resource bound and their minimum safe sizes.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed limit failure for the first zero or undersized bound.
    pub fn validate(self) -> Result<Self, NativeHealthCheckExecutorBuildError> {
        for (field, value) in [
            ("health_workers", self.workers),
            ("health_queue_capacity", self.queue_capacity),
            ("health_screen_text_bytes", self.screen_text_bytes),
            ("health_screen_cells", self.screen_cells),
            ("health_pattern_bytes", self.pattern_bytes),
            ("health_detail_bytes", self.detail_bytes),
            (
                "health_worker_thread_stack_bytes",
                self.worker_thread_stack_bytes,
            ),
        ] {
            if value == 0 {
                return Err(NativeHealthCheckExecutorBuildError::Limit(
                    RuntimeLimitError::zero(field),
                ));
            }
        }
        if self.check_timeout.is_zero() {
            return Err(NativeHealthCheckExecutorBuildError::Limit(
                RuntimeLimitError::zero("health_check_timeout"),
            ));
        }
        if self.detail_bytes < MINIMUM_DETAIL_BYTES {
            return Err(NativeHealthCheckExecutorBuildError::InvalidRelationship {
                rule: "health detail capacity must be at least 32 bytes",
            });
        }
        if self.worker_thread_stack_bytes < MINIMUM_WORKER_STACK_BYTES {
            return Err(NativeHealthCheckExecutorBuildError::InvalidRelationship {
                rule: "health worker thread stack must be at least 64 KiB",
            });
        }
        Ok(self)
    }
}

/// Construction failure for the fixed native health worker pool.
#[derive(Debug)]
pub enum NativeHealthCheckExecutorBuildError {
    /// One configured hard bound is zero.
    Limit(RuntimeLimitError),
    /// Two valid individual limits form an unsafe configuration.
    InvalidRelationship {
        /// Stable non-sensitive violated rule.
        rule: &'static str,
    },
    /// A fixed worker thread could not be started.
    Worker(std::io::Error),
}

impl Display for NativeHealthCheckExecutorBuildError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Limit(error) => Display::fmt(error, formatter),
            Self::InvalidRelationship { rule } => formatter.write_str(rule),
            Self::Worker(_) => formatter.write_str("health check worker could not be started"),
        }
    }
}

impl Error for NativeHealthCheckExecutorBuildError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Limit(error) => Some(error),
            Self::Worker(error) => Some(error),
            Self::InvalidRelationship { .. } => None,
        }
    }
}

/// Payload-free occupancy and cumulative health-execution outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeHealthCheckExecutorPressure {
    /// Admitted requests not yet claimed by a worker.
    pub queued_requests: usize,
    /// Inclusive waiting-request maximum.
    pub queue_limit: usize,
    /// Fixed configured worker count.
    pub workers: usize,
    /// Workers currently evaluating a request.
    pub active_workers: usize,
    /// Requests completed with either results or a typed executor error.
    pub completed_requests: u64,
    /// Requests stopped because their caller or executor shut down.
    pub cancelled_requests: u64,
    /// Requests rejected without waiting because the queue was full.
    pub capacity_rejections: u64,
    /// Individual rules producing affirmative health results.
    pub passed_checks: u64,
    /// Individual rules producing ordinary negative health results.
    pub failed_checks: u64,
}

/// Cloneable health adapter backed by a fixed, bounded blocking worker pool.
///
/// Requests are admitted when `run` is called, before its future is polled.
/// Dropping that future requests cancellation; blocking network calls finish
/// at their own timeout before observing it. The process argument does not
/// constrain probes or session-keyed screen reads. Application callers must
/// revalidate sampled session revision and process identity before committing
/// results, as the application health observation service does.
#[derive(Clone)]
pub struct NativeHealthCheckExecutor {
    inner: Arc<HealthExecutorInner>,
}

impl NativeHealthCheckExecutor {
    /// Starts the fixed worker pool over shared screen and pattern adapters.
    ///
    /// HTTP clients disable environment proxies, enforce one global deadline,
    /// and never read response bodies. This function starts exactly `workers`
    /// blocking threads and performs no health IO itself.
    ///
    /// # Errors
    ///
    /// Returns a typed invalid-limit or worker-start failure. Partially started
    /// pools are closed and joined before returning an error.
    pub fn new(
        screens: Arc<VtTerminalScreenStore>,
        patterns: Arc<dyn ProfilePatternEngine>,
        limits: NativeHealthCheckExecutorLimits,
    ) -> Result<Self, NativeHealthCheckExecutorBuildError> {
        let limits = limits.validate()?;
        let config = ureq::Agent::config_builder()
            .timeout_global(Some(limits.check_timeout))
            .proxy(None)
            .max_redirects(3)
            .http_status_as_error(false)
            .build();
        let shared = Arc::new(HealthWorkerShared {
            screens,
            patterns,
            agent: ureq::Agent::new_with_config(config),
            limits,
            counters: HealthExecutorCounters::default(),
            shutdown: AtomicBool::new(false),
        });
        let (sender, receiver) = async_channel::bounded(limits.queue_capacity);
        let mut workers = Vec::with_capacity(limits.workers);
        for worker_number in 0..limits.workers {
            let worker_receiver = receiver.clone();
            let worker_shared = Arc::clone(&shared);
            match thread::Builder::new()
                .name(format!("undercurrent-health-{worker_number}"))
                .stack_size(limits.worker_thread_stack_bytes)
                .spawn(move || health_worker_loop(worker_shared, worker_receiver))
            {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    shared.shutdown.store(true, Ordering::Release);
                    sender.close();
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(NativeHealthCheckExecutorBuildError::Worker(error));
                }
            }
        }
        Ok(Self {
            inner: Arc::new(HealthExecutorInner {
                sender,
                receiver,
                workers: Mutex::new(workers),
                shared,
            }),
        })
    }

    /// Returns current queue occupancy and payload-free cumulative counters.
    ///
    /// This function performs no external IO and does not panic.
    pub fn pressure(&self) -> NativeHealthCheckExecutorPressure {
        let counters = &self.inner.shared.counters;
        NativeHealthCheckExecutorPressure {
            queued_requests: self.inner.sender.len(),
            queue_limit: self.inner.shared.limits.queue_capacity,
            workers: self.inner.shared.limits.workers,
            active_workers: counters.active_workers.load(Ordering::Acquire),
            completed_requests: counters.completed_requests.load(Ordering::Acquire),
            cancelled_requests: counters.cancelled_requests.load(Ordering::Acquire),
            capacity_rejections: counters.capacity_rejections.load(Ordering::Acquire),
            passed_checks: counters.passed_checks.load(Ordering::Acquire),
            failed_checks: counters.failed_checks.load(Ordering::Acquire),
        }
    }

    fn submit(
        &self,
        session_id: SessionId,
        rules: HealthRules,
    ) -> Result<HealthResponse, PortError> {
        let error_session = session_id.clone();
        let cancellation = Arc::new(AtomicBool::new(false));
        let (reply, receiver) = async_channel::bounded(1);
        match self.inner.sender.try_send(HealthRequest {
            session_id,
            rules,
            cancellation: Arc::clone(&cancellation),
            reply,
        }) {
            Ok(()) => Ok(HealthResponse {
                receiver,
                cancellation,
                armed: true,
            }),
            Err(async_channel::TrySendError::Full(_)) => {
                increment_counter(&self.inner.shared.counters.capacity_rejections);
                Err(health_port_error(
                    PortErrorKind::CapacityExceeded,
                    Some(error_session),
                    "health check request queue is full",
                ))
            }
            Err(async_channel::TrySendError::Closed(_)) => Err(health_port_error(
                PortErrorKind::Unavailable,
                Some(error_session),
                "health check executor is unavailable",
            )),
        }
    }
}

impl Debug for NativeHealthCheckExecutor {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeHealthCheckExecutor")
            .field("limits", &self.inner.shared.limits)
            .field("pressure", &self.pressure())
            .finish_non_exhaustive()
    }
}

impl HealthCheckExecutor for NativeHealthCheckExecutor {
    fn run<'a>(
        &'a self,
        session_id: &'a SessionId,
        _process: &'a ProcessInstanceId,
        rules: &'a HealthRules,
    ) -> PortFuture<'a, Result<Vec<HealthCheckResult>, PortError>> {
        let response = self.submit(session_id.clone(), rules.clone());
        Box::pin(async move {
            let mut response = response?;
            let result = response.receiver.recv().await.map_err(|_| {
                health_port_error(
                    PortErrorKind::Interrupted,
                    Some(session_id.clone()),
                    "health check worker ended before replying",
                )
            })?;
            response.disarm();
            result
        })
    }
}

struct HealthExecutorInner {
    sender: async_channel::Sender<HealthRequest>,
    receiver: async_channel::Receiver<HealthRequest>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    shared: Arc<HealthWorkerShared>,
}

impl Drop for HealthExecutorInner {
    fn drop(&mut self) {
        self.shared.shutdown.store(true, Ordering::Release);
        self.sender.close();
        while let Ok(request) = self.receiver.try_recv() {
            request.cancellation.store(true, Ordering::Release);
            increment_counter(&self.shared.counters.cancelled_requests);
            let _ = request
                .reply
                .try_send(Err(cancelled_error(&request.session_id)));
        }
        let workers = self
            .workers
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for worker in workers.drain(..) {
            let _ = worker.join();
        }
    }
}

struct HealthWorkerShared {
    screens: Arc<VtTerminalScreenStore>,
    patterns: Arc<dyn ProfilePatternEngine>,
    agent: ureq::Agent,
    limits: NativeHealthCheckExecutorLimits,
    counters: HealthExecutorCounters,
    shutdown: AtomicBool,
}

#[derive(Default)]
struct HealthExecutorCounters {
    active_workers: AtomicUsize,
    completed_requests: AtomicU64,
    cancelled_requests: AtomicU64,
    capacity_rejections: AtomicU64,
    passed_checks: AtomicU64,
    failed_checks: AtomicU64,
}

struct HealthRequest {
    session_id: SessionId,
    rules: HealthRules,
    cancellation: Arc<AtomicBool>,
    reply: async_channel::Sender<Result<Vec<HealthCheckResult>, PortError>>,
}

struct HealthResponse {
    receiver: async_channel::Receiver<Result<Vec<HealthCheckResult>, PortError>>,
    cancellation: Arc<AtomicBool>,
    armed: bool,
}

impl HealthResponse {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for HealthResponse {
    fn drop(&mut self) {
        if self.armed {
            self.cancellation.store(true, Ordering::Release);
        }
    }
}

fn health_worker_loop(
    shared: Arc<HealthWorkerShared>,
    receiver: async_channel::Receiver<HealthRequest>,
) {
    while let Ok(request) = receiver.recv_blocking() {
        let _active_worker = ActiveWorkerGuard::new(&shared.counters.active_workers);
        let result = execute_request(&shared, &request);
        if result
            .as_ref()
            .is_err_and(|error| error.kind() == PortErrorKind::Cancelled)
        {
            increment_counter(&shared.counters.cancelled_requests);
        } else {
            increment_counter(&shared.counters.completed_requests);
        }
        let _ = request.reply.try_send(result);
    }
}

struct ActiveWorkerGuard<'a>(&'a AtomicUsize);

impl<'a> ActiveWorkerGuard<'a> {
    fn new(active: &'a AtomicUsize) -> Self {
        active.fetch_add(1, Ordering::AcqRel);
        Self(active)
    }
}

impl Drop for ActiveWorkerGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn execute_request(
    shared: &HealthWorkerShared,
    request: &HealthRequest,
) -> Result<Vec<HealthCheckResult>, PortError> {
    ensure_running(shared, request)?;
    let needs_screen = request.rules.iter().any(|rule| {
        matches!(
            rule.kind(),
            HealthRuleKind::OutputContains
                | HealthRuleKind::OutputRejects
                | HealthRuleKind::RedCellThreshold
        )
    });
    let screen = needs_screen
        .then(|| {
            shared.screens.health_sample(
                &request.session_id,
                shared.limits.screen_text_bytes,
                shared.limits.screen_cells,
            )
        })
        .transpose()?;
    let mut results = Vec::with_capacity(request.rules.len());
    for (index, rule) in request.rules.iter().enumerate() {
        ensure_running(shared, request)?;
        let started = Instant::now();
        let outcome = execute_rule(shared, request, rule, screen.as_ref())?;
        ensure_running(shared, request)?;
        if outcome.passed {
            increment_counter(&shared.counters.passed_checks);
        } else {
            increment_counter(&shared.counters.failed_checks);
        }
        let result = HealthCheckResult::new(
            u16::try_from(index).map_err(|error| {
                health_port_error_with_source(
                    PortErrorKind::InvalidData,
                    Some(request.session_id.clone()),
                    error,
                )
            })?,
            outcome.passed,
            started.elapsed(),
            Some(outcome.detail.as_str().to_owned()),
            shared.limits.detail_bytes,
        )
        .map_err(|error| {
            health_port_error_with_source(
                PortErrorKind::InvalidData,
                Some(request.session_id.clone()),
                error,
            )
        })?;
        results.push(result);
    }
    Ok(results)
}

fn execute_rule(
    shared: &HealthWorkerShared,
    request: &HealthRequest,
    rule: &HealthRule,
    screen: Option<&TerminalHealthSample>,
) -> Result<HealthOutcome, PortError> {
    match rule.kind() {
        HealthRuleKind::TcpPort => execute_tcp(shared, request, rule),
        HealthRuleKind::HttpEndpoint => execute_http(shared, request, rule),
        HealthRuleKind::OutputContains => execute_pattern(shared, request, rule, screen, true),
        HealthRuleKind::OutputRejects => execute_pattern(shared, request, rule, screen, false),
        HealthRuleKind::RedCellThreshold => execute_red_cells(request, rule, screen),
        HealthRuleKind::Command => execute_command(shared, request, rule),
    }
}

fn execute_tcp(
    shared: &HealthWorkerShared,
    request: &HealthRequest,
    rule: &HealthRule,
) -> Result<HealthOutcome, PortError> {
    let port = rule
        .port()
        .ok_or_else(|| invalid_rule(&request.session_id))?;
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    Ok(
        match TcpStream::connect_timeout(&address, shared.limits.check_timeout) {
            Ok(_) => HealthOutcome::passed(NativeHealthCheckDetail::TcpConnected),
            Err(error) if error.kind() == std::io::ErrorKind::TimedOut => {
                HealthOutcome::failed(NativeHealthCheckDetail::TcpTimeout)
            }
            Err(_) => HealthOutcome::failed(NativeHealthCheckDetail::TcpUnavailable),
        },
    )
}

fn execute_http(
    shared: &HealthWorkerShared,
    request: &HealthRequest,
    rule: &HealthRule,
) -> Result<HealthOutcome, PortError> {
    let endpoint = rule
        .text_value()
        .ok_or_else(|| invalid_rule(&request.session_id))?;
    match shared.agent.get(endpoint).call() {
        Ok(response) if response.status().as_u16() == 200 => {
            Ok(HealthOutcome::passed(NativeHealthCheckDetail::HttpOk))
        }
        Ok(_) | Err(ureq::Error::StatusCode(_)) => Ok(HealthOutcome::failed(
            NativeHealthCheckDetail::HttpStatusNotOk,
        )),
        Err(ureq::Error::Timeout(_)) => {
            Ok(HealthOutcome::failed(NativeHealthCheckDetail::HttpTimeout))
        }
        Err(ureq::Error::BadUri(_) | ureq::Error::Http(_) | ureq::Error::RequireHttpsOnly(_)) => {
            Err(health_port_error(
                PortErrorKind::InvalidData,
                Some(request.session_id.clone()),
                "configured health endpoint is invalid",
            ))
        }
        Err(_) => Ok(HealthOutcome::failed(
            NativeHealthCheckDetail::HttpRequestFailed,
        )),
    }
}

fn execute_pattern(
    shared: &HealthWorkerShared,
    request: &HealthRequest,
    rule: &HealthRule,
    screen: Option<&TerminalHealthSample>,
    require_presence: bool,
) -> Result<HealthOutcome, PortError> {
    let value = rule
        .text_value()
        .ok_or_else(|| invalid_rule(&request.session_id))?;
    let expression = PatternExpression::new(value.to_owned(), shared.limits.pattern_bytes)
        .map_err(|error| {
            health_port_error_with_source(
                PortErrorKind::InvalidData,
                Some(request.session_id.clone()),
                error,
            )
        })?;
    let present = shared
        .patterns
        .is_match(
            &expression,
            screen
                .ok_or_else(|| missing_screen(&request.session_id))?
                .text(),
        )
        .map_err(|error| {
            health_port_error_with_source(
                PortErrorKind::InvalidData,
                Some(request.session_id.clone()),
                error,
            )
        })?;
    let passed = if require_presence { present } else { !present };
    Ok(HealthOutcome {
        passed,
        detail: if present {
            NativeHealthCheckDetail::PatternPresent
        } else {
            NativeHealthCheckDetail::PatternAbsent
        },
    })
}

fn execute_red_cells(
    request: &HealthRequest,
    rule: &HealthRule,
    screen: Option<&TerminalHealthSample>,
) -> Result<HealthOutcome, PortError> {
    let minimum = rule
        .minimum_cells()
        .ok_or_else(|| invalid_rule(&request.session_id))?;
    let clear = screen
        .ok_or_else(|| missing_screen(&request.session_id))?
        .red_cells()
        < minimum;
    Ok(if clear {
        HealthOutcome::passed(NativeHealthCheckDetail::RedCellThresholdClear)
    } else {
        HealthOutcome::failed(NativeHealthCheckDetail::RedCellThresholdMet)
    })
}

fn execute_command(
    shared: &HealthWorkerShared,
    request: &HealthRequest,
    rule: &HealthRule,
) -> Result<HealthOutcome, PortError> {
    let specification = rule
        .command_spec()
        .ok_or_else(|| invalid_rule(&request.session_id))?;
    let mut command = Command::new(specification.program());
    command
        .args(specification.arguments())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    configure_command_group(&mut command);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(_) => {
            return Ok(HealthOutcome::failed(
                NativeHealthCheckDetail::CommandSpawnFailed,
            ));
        }
    };
    let started = Instant::now();
    loop {
        if request.cancellation.load(Ordering::Acquire) || shared.shutdown.load(Ordering::Acquire) {
            terminate_child(&mut child);
            return Err(cancelled_error(&request.session_id));
        }
        match child.try_wait() {
            Ok(Some(status)) if status.success() => {
                return Ok(HealthOutcome::passed(
                    NativeHealthCheckDetail::CommandSucceeded,
                ));
            }
            Ok(Some(_)) => {
                return Ok(HealthOutcome::failed(
                    NativeHealthCheckDetail::CommandExitNonzero,
                ));
            }
            Ok(None) if started.elapsed() >= shared.limits.check_timeout => {
                terminate_child(&mut child);
                return Ok(HealthOutcome::failed(
                    NativeHealthCheckDetail::CommandTimeout,
                ));
            }
            Ok(None) => {
                let remaining = shared
                    .limits
                    .check_timeout
                    .saturating_sub(started.elapsed());
                thread::sleep(COMMAND_POLL_INTERVAL.min(remaining));
            }
            Err(_) => {
                terminate_child(&mut child);
                return Ok(HealthOutcome::failed(
                    NativeHealthCheckDetail::CommandSpawnFailed,
                ));
            }
        }
    }
}

#[cfg(unix)]
fn configure_command_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    command.process_group(0);
}

#[cfg(not(unix))]
fn configure_command_group(_command: &mut Command) {}

#[cfg(unix)]
fn terminate_child(child: &mut Child) {
    use nix::sys::signal::{Signal, killpg};
    use nix::unistd::Pid;

    let process_group = i32::try_from(child.id()).unwrap_or(i32::MAX);
    if killpg(Pid::from_raw(process_group), Signal::SIGKILL).is_err() {
        let _ = child.kill();
    }
    let _ = child.wait();
}

#[cfg(not(unix))]
fn terminate_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn ensure_running(shared: &HealthWorkerShared, request: &HealthRequest) -> Result<(), PortError> {
    if request.cancellation.load(Ordering::Acquire) || shared.shutdown.load(Ordering::Acquire) {
        return Err(cancelled_error(&request.session_id));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct HealthOutcome {
    passed: bool,
    detail: NativeHealthCheckDetail,
}

impl HealthOutcome {
    const fn passed(detail: NativeHealthCheckDetail) -> Self {
        Self {
            passed: true,
            detail,
        }
    }

    const fn failed(detail: NativeHealthCheckDetail) -> Self {
        Self {
            passed: false,
            detail,
        }
    }
}

fn invalid_rule(session_id: &SessionId) -> PortError {
    health_port_error(
        PortErrorKind::InvalidData,
        Some(session_id.clone()),
        "health rule kind and specification disagree",
    )
}

fn missing_screen(session_id: &SessionId) -> PortError {
    health_port_error(
        PortErrorKind::NotFound,
        Some(session_id.clone()),
        "terminal screen is unavailable for health evaluation",
    )
}

fn cancelled_error(session_id: &SessionId) -> PortError {
    health_port_error(
        PortErrorKind::Cancelled,
        Some(session_id.clone()),
        "health check request was cancelled",
    )
}

fn health_port_error(
    kind: PortErrorKind,
    session_id: Option<SessionId>,
    message: &'static str,
) -> PortError {
    health_port_error_with_source(kind, session_id, HealthExecutorFailure(message))
}

fn health_port_error_with_source(
    kind: PortErrorKind,
    session_id: Option<SessionId>,
    source: impl Error + Send + Sync + 'static,
) -> PortError {
    PortError::new(kind, PortOperation::RunHealthChecks, session_id, source)
}

#[derive(Debug)]
struct HealthExecutorFailure(&'static str);

impl Display for HealthExecutorFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for HealthExecutorFailure {}

fn increment_counter(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(1))
    });
}
