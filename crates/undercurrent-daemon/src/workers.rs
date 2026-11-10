// SPDX-License-Identifier: Apache-2.0

//! Fixed daemon background-worker ownership and payload-free diagnostics.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use tracing::{Level, Span};
use undercurrent_application::{ApplicationErrorCategory, TraceSpanName};

use crate::{
    DaemonDiagnosticField, DaemonDiagnosticRow, DaemonDiagnosticSection, DaemonDiagnosticSource,
    DaemonDiagnosticValue, DaemonOperationsError,
};

pub(crate) const MINIMUM_WORKER_STACK_BYTES: usize = 64 * 1_024;

/// Stable registry of daemon-owned background task kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DaemonWorkerKind {
    /// Reconcile durable process-effect attempts against retained runtime evidence.
    AttemptReconciliation,
    /// Publish transactional outbox records and activate derived work.
    Outbox,
    /// Execute due revision-checked automatic restarts.
    Restart,
    /// Execute due foreground and health observations.
    Observation,
    /// Settle event-driven supervised process exits.
    ProcessExit,
    /// Validate and activate stable YAML profile snapshots.
    ProfileReload,
}

impl DaemonWorkerKind {
    /// Complete worker-kind registry in startup and diagnostic order.
    pub const ALL: [Self; 6] = [
        Self::AttemptReconciliation,
        Self::Outbox,
        Self::Restart,
        Self::Observation,
        Self::ProcessExit,
        Self::ProfileReload,
    ];

    /// Returns the stable thread and diagnostic name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProfileReload => "profile_reload",
            Self::AttemptReconciliation => "attempt_reconciliation",
            Self::Outbox => "outbox",
            Self::Restart => "restart",
            Self::Observation => "observation",
            Self::ProcessExit => "process_exit",
        }
    }

    /// Returns the central application span represented by one worker cycle.
    pub const fn trace_span_name(self) -> TraceSpanName {
        match self {
            Self::ProfileReload => TraceSpanName::ReloadProfiles,
            Self::AttemptReconciliation => TraceSpanName::ReconcileOperationAttempts,
            Self::Outbox => TraceSpanName::DispatchOutbox,
            Self::Restart => TraceSpanName::RunScheduledRestart,
            Self::Observation => TraceSpanName::DispatchObservations,
            Self::ProcessExit => TraceSpanName::ObserveProcessExit,
        }
    }
}

/// Result of one bounded background task cycle.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DaemonWorkerCycle {
    /// Number of bounded records or jobs handled during the cycle.
    pub processed: usize,
    /// Number of isolated item failures handled without aborting the cycle.
    pub failed: usize,
    /// Most recent safe isolated failure category, when retained.
    pub last_failure: Option<ApplicationErrorCategory>,
    /// Whether the task reached its batch bound and should run again immediately.
    pub saturated: bool,
}

impl DaemonWorkerCycle {
    /// Constructs an idle cycle that waits before checking again.
    pub const fn idle() -> Self {
        Self {
            processed: 0,
            failed: 0,
            last_failure: None,
            saturated: false,
        }
    }

    /// Constructs a completed cycle.
    ///
    /// Zero processed items always clear `saturated`, preventing accidental busy
    /// loops. This function performs no IO and does not panic.
    pub const fn processed(processed: usize, saturated: bool) -> Self {
        Self {
            processed,
            failed: 0,
            last_failure: None,
            saturated: processed > 0 && saturated,
        }
    }

    /// Constructs a cycle that isolated one or more item failures.
    ///
    /// Saturation is retained only for a non-empty handled batch. A failure
    /// category is discarded when no failure is reported and may otherwise be
    /// absent when the bounded report retained only aggregate counts. This
    /// function performs no IO and does not panic.
    pub const fn with_failures(
        processed: usize,
        failed: usize,
        last_failure: Option<ApplicationErrorCategory>,
        saturated: bool,
    ) -> Self {
        Self {
            processed,
            failed,
            last_failure: if failed == 0 { None } else { last_failure },
            saturated: processed > 0 && saturated,
        }
    }
}

/// Typed background task failure preserving its source behind safe diagnostics.
pub struct DaemonBackgroundTaskError {
    category: ApplicationErrorCategory,
    source: Box<dyn Error + Send + Sync>,
}

impl DaemonBackgroundTaskError {
    /// Wraps one application or adapter error with a stable safe category.
    pub fn new(
        category: ApplicationErrorCategory,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            category,
            source: Box::new(source),
        }
    }

    /// Returns the safe failure category retained by worker diagnostics.
    pub const fn category(&self) -> ApplicationErrorCategory {
        self.category
    }
}

impl Display for DaemonBackgroundTaskError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "daemon background task failed with {}",
            self.category.as_str()
        )
    }
}

impl Debug for DaemonBackgroundTaskError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonBackgroundTaskError")
            .field("category", &self.category)
            .finish_non_exhaustive()
    }
}

impl Error for DaemonBackgroundTaskError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// One independently executable daemon background boundary.
pub trait DaemonBackgroundTask: Send + Sync {
    /// Executes at most one bounded cycle.
    ///
    /// Implementations must not scan unaddressed session state. A blocking task
    /// such as process-exit settlement must unblock after [`Self::interrupt`].
    fn execute(&self) -> Result<DaemonWorkerCycle, DaemonBackgroundTaskError>;

    /// Interrupts a currently blocked execution during shutdown.
    ///
    /// Periodic non-blocking tasks may use the default no-op implementation.
    fn interrupt(&self) {}
}

/// Validated task registration for one fixed daemon worker.
pub struct DaemonWorkerSpec {
    kind: DaemonWorkerKind,
    idle_interval: Duration,
    maximum_empty_idle_interval: Duration,
    task: Arc<dyn DaemonBackgroundTask>,
}

impl DaemonWorkerSpec {
    /// Registers one task and its idle/error backoff interval.
    ///
    /// # Errors
    ///
    /// Returns a typed configuration failure for a zero interval. This function
    /// performs no IO and does not panic.
    pub fn new(
        kind: DaemonWorkerKind,
        idle_interval: Duration,
        task: Arc<dyn DaemonBackgroundTask>,
    ) -> Result<Self, DaemonBackgroundWorkersCreateError> {
        if idle_interval.is_zero() {
            return Err(DaemonBackgroundWorkersCreateError::InvalidLimit {
                field: "idle_interval",
            });
        }
        Ok(Self {
            kind,
            idle_interval,
            maximum_empty_idle_interval: idle_interval,
            task,
        })
    }

    /// Enables bounded exponential delay after consecutive successful empty cycles.
    ///
    /// Work, failure, or a targeted wake resets the next delay to the base idle
    /// interval. The maximum must not be shorter than that base interval.
    ///
    /// # Errors
    ///
    /// Returns a typed configuration failure when `maximum` is shorter than
    /// the registered base idle interval. This function performs no IO and
    /// does not panic.
    pub fn with_adaptive_empty_backoff(
        mut self,
        maximum: Duration,
    ) -> Result<Self, DaemonBackgroundWorkersCreateError> {
        if maximum < self.idle_interval {
            return Err(DaemonBackgroundWorkersCreateError::InvalidLimit {
                field: "maximum_empty_idle_interval",
            });
        }
        self.maximum_empty_idle_interval = maximum;
        Ok(self)
    }

    /// Returns the stable registered worker kind.
    pub const fn kind(&self) -> DaemonWorkerKind {
        self.kind
    }
}

impl Debug for DaemonWorkerSpec {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonWorkerSpec")
            .field("kind", &self.kind)
            .field("idle_interval", &self.idle_interval)
            .field(
                "maximum_empty_idle_interval",
                &self.maximum_empty_idle_interval,
            )
            .finish_non_exhaustive()
    }
}

/// Failure validating or starting the fixed background-worker set.
#[derive(Debug)]
pub enum DaemonBackgroundWorkersCreateError {
    /// One hard worker bound is zero or undersized.
    InvalidLimit {
        /// Stable invalid bound name.
        field: &'static str,
    },
    /// The same stable worker kind was registered twice.
    DuplicateKind(DaemonWorkerKind),
    /// One named worker thread could not be started.
    WorkerStart {
        /// Stable affected worker kind.
        kind: DaemonWorkerKind,
        /// Operating-system thread creation failure.
        source: std::io::Error,
    },
}

impl Display for DaemonBackgroundWorkersCreateError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLimit { field } => write!(formatter, "worker limit {field} is invalid"),
            Self::DuplicateKind(kind) => {
                write!(
                    formatter,
                    "worker kind {} is registered twice",
                    kind.as_str()
                )
            }
            Self::WorkerStart { kind, .. } => {
                write!(formatter, "worker {} could not be started", kind.as_str())
            }
        }
    }
}

impl Error for DaemonBackgroundWorkersCreateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::WorkerStart { source, .. } => Some(source),
            Self::InvalidLimit { .. } | Self::DuplicateKind(_) => None,
        }
    }
}

/// Payload-free state for one fixed background worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DaemonWorkerPressure {
    /// Stable worker kind.
    pub kind: DaemonWorkerKind,
    /// Whether a task cycle is currently executing.
    pub active: bool,
    /// Whether the worker thread has ended.
    pub stopped: bool,
    /// Completed cycle count, including failed cycles.
    pub cycles: u64,
    /// Sum of bounded items reported by successful cycles.
    pub processed: u64,
    /// Typed task failures contained at the worker boundary.
    pub failures: u64,
    /// Panics contained before stopping the affected worker.
    pub panics: u64,
    /// Most recent safe failure category.
    pub last_failure: Option<ApplicationErrorCategory>,
}

/// Fixed, named daemon background workers with coordinated shutdown.
pub struct DaemonBackgroundWorkers {
    shared: Arc<BackgroundWorkersShared>,
    specifications: Vec<Arc<DaemonWorkerSpec>>,
    workers: Mutex<Vec<(DaemonWorkerKind, JoinHandle<()>)>>,
}

impl DaemonBackgroundWorkers {
    /// Starts exactly one named thread for every supplied unique task kind.
    ///
    /// Worker loops continue immediately only after a non-empty saturated
    /// cycle; idle, partial, and failed cycles wait on a shutdown-aware
    /// condition variable. This function starts threads but performs no task IO
    /// on the caller.
    ///
    /// # Errors
    ///
    /// Returns a typed empty-registration, stack-size, duplicate-kind, or
    /// thread-start failure. Partially started workers are interrupted and
    /// joined before the error is returned.
    pub fn start(
        specifications: Vec<DaemonWorkerSpec>,
        worker_thread_stack_bytes: usize,
    ) -> Result<Self, DaemonBackgroundWorkersCreateError> {
        if specifications.is_empty() {
            return Err(DaemonBackgroundWorkersCreateError::InvalidLimit {
                field: "worker_count",
            });
        }
        if worker_thread_stack_bytes < MINIMUM_WORKER_STACK_BYTES {
            return Err(DaemonBackgroundWorkersCreateError::InvalidLimit {
                field: "worker_thread_stack_bytes",
            });
        }
        let mut kinds = BTreeSet::new();
        for specification in &specifications {
            if !kinds.insert(specification.kind) {
                return Err(DaemonBackgroundWorkersCreateError::DuplicateKind(
                    specification.kind,
                ));
            }
        }
        let states = specifications
            .iter()
            .map(|specification| (specification.kind, WorkerState::default()))
            .collect();
        let wake_generations = kinds.iter().map(|kind| (*kind, 0_u64)).collect();
        let shared = Arc::new(BackgroundWorkersShared {
            stopped: AtomicBool::new(false),
            interrupts_sent: AtomicBool::new(false),
            wake_generations: Mutex::new(wake_generations),
            changed: Condvar::new(),
            states: Mutex::new(states),
        });
        let specifications = specifications.into_iter().map(Arc::new).collect::<Vec<_>>();
        let mut workers = Vec::with_capacity(specifications.len());
        for specification in &specifications {
            let worker_shared = Arc::clone(&shared);
            let worker_specification = Arc::clone(specification);
            match thread::Builder::new()
                .name(format!(
                    "undercurrent-background-{}",
                    specification.kind.as_str()
                ))
                .stack_size(worker_thread_stack_bytes)
                .spawn(move || worker_loop(worker_shared, worker_specification))
            {
                Ok(worker) => workers.push((specification.kind, worker)),
                Err(source) => {
                    stop_and_interrupt(&shared, &specifications);
                    join_workers(&shared, workers);
                    return Err(DaemonBackgroundWorkersCreateError::WorkerStart {
                        kind: specification.kind,
                        source,
                    });
                }
            }
        }
        Ok(Self {
            shared,
            specifications,
            workers: Mutex::new(workers),
        })
    }

    /// Returns one stable payload-free state row per registered worker.
    pub fn pressure(&self) -> Vec<DaemonWorkerPressure> {
        self.shared
            .states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|(kind, state)| state.pressure(*kind))
            .collect()
    }

    pub(crate) fn request_reset(&self) {
        self.shared.stopped.store(true, Ordering::Release);
        self.shared.changed.notify_all();
        // Interrupting the exit source disconnects the host client. Keep that
        // channel alive until durable host shutdown and its ACK have completed.
        for spec in &self.specifications {
            if spec.kind != DaemonWorkerKind::ProcessExit {
                let _ = catch_unwind(AssertUnwindSafe(|| spec.task.interrupt()));
            }
        }
    }

    pub(crate) fn is_quiescent(&self) -> bool {
        self.workers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|(kind, _)| *kind != DaemonWorkerKind::ProcessExit)
            .all(|(_, worker)| worker.is_finished())
    }

    /// Stops cycles, interrupts blocking tasks, and joins every worker.
    ///
    /// Repeated calls are idempotent. Implementations of blocking task
    /// [`DaemonBackgroundTask::interrupt`] are responsible for waking their
    /// external wait. Panics in interrupt callbacks and worker threads are
    /// contained and reflected in pressure. This function may block while
    /// admitted task IO observes interruption and does not panic.
    pub fn shutdown(&self) {
        stop_and_interrupt(&self.shared, &self.specifications);
        let workers = std::mem::take(
            &mut *self
                .workers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        join_workers(&self.shared, workers);
    }

    /// Returns whether coordinated worker shutdown has begun.
    pub fn is_shutdown(&self) -> bool {
        self.shared.stopped.load(Ordering::Acquire)
    }

    /// Wakes one registered worker without disturbing other idle workers.
    ///
    /// The generation is advanced while holding the same mutex used to enter
    /// the worker wait, so a wake racing the end of a task cycle cannot be lost.
    pub fn wake(&self, kind: DaemonWorkerKind) {
        self.shared.wake(kind);
    }
}

impl Debug for DaemonBackgroundWorkers {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonBackgroundWorkers")
            .field("shutdown", &self.is_shutdown())
            .field("pressure", &self.pressure())
            .finish_non_exhaustive()
    }
}

impl Drop for DaemonBackgroundWorkers {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl DaemonDiagnosticSource for DaemonBackgroundWorkers {
    fn snapshot(
        &self,
        sections: &BTreeSet<DaemonDiagnosticSection>,
    ) -> Result<Vec<DaemonDiagnosticRow>, DaemonOperationsError> {
        if !sections.contains(&DaemonDiagnosticSection::SupervisorQueues) {
            return Ok(Vec::new());
        }
        Ok(self
            .pressure()
            .into_iter()
            .map(|pressure| {
                let mut fields = BTreeMap::new();
                fields.insert(
                    DaemonDiagnosticField::Active,
                    DaemonDiagnosticValue::Boolean(pressure.active),
                );
                fields.insert(
                    DaemonDiagnosticField::Closed,
                    DaemonDiagnosticValue::Boolean(pressure.stopped),
                );
                fields.insert(
                    DaemonDiagnosticField::Count,
                    DaemonDiagnosticValue::Unsigned(pressure.cycles),
                );
                fields.insert(
                    DaemonDiagnosticField::Items,
                    DaemonDiagnosticValue::Unsigned(pressure.processed),
                );
                fields.insert(
                    DaemonDiagnosticField::Failed,
                    DaemonDiagnosticValue::Unsigned(
                        pressure.failures.saturating_add(pressure.panics),
                    ),
                );
                DaemonDiagnosticRow {
                    section: DaemonDiagnosticSection::SupervisorQueues,
                    identity: pressure.kind.as_str().to_owned(),
                    fields,
                }
            })
            .collect())
    }
}

#[derive(Default)]
struct WorkerState {
    active: bool,
    stopped: bool,
    cycles: u64,
    processed: u64,
    failures: u64,
    panics: u64,
    last_failure: Option<ApplicationErrorCategory>,
}

impl WorkerState {
    fn pressure(&self, kind: DaemonWorkerKind) -> DaemonWorkerPressure {
        DaemonWorkerPressure {
            kind,
            active: self.active,
            stopped: self.stopped,
            cycles: self.cycles,
            processed: self.processed,
            failures: self.failures,
            panics: self.panics,
            last_failure: self.last_failure,
        }
    }
}

struct BackgroundWorkersShared {
    stopped: AtomicBool,
    interrupts_sent: AtomicBool,
    wake_generations: Mutex<BTreeMap<DaemonWorkerKind, u64>>,
    changed: Condvar,
    states: Mutex<BTreeMap<DaemonWorkerKind, WorkerState>>,
}

impl BackgroundWorkersShared {
    fn update(&self, kind: DaemonWorkerKind, apply: impl FnOnce(&mut WorkerState)) {
        let mut states = self
            .states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(state) = states.get_mut(&kind) {
            apply(state);
        }
    }

    fn wake_generation(&self, kind: DaemonWorkerKind) -> u64 {
        self.wake_generations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&kind)
            .copied()
            .unwrap_or(0)
    }

    fn wake(&self, kind: DaemonWorkerKind) {
        let mut generations = self
            .wake_generations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(generation) = generations.get_mut(&kind) {
            *generation = generation.saturating_add(1);
            self.changed.notify_all();
        }
    }

    fn wait(&self, kind: DaemonWorkerKind, observed_generation: u64, interval: Duration) {
        let generations = self
            .wake_generations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _guard = self
            .changed
            .wait_timeout_while(generations, interval, |generations| {
                !self.stopped.load(Ordering::Acquire)
                    && generations.get(&kind).copied().unwrap_or(0) == observed_generation
            })
            .unwrap_or_else(|poisoned| poisoned.into_inner());
    }
}

fn worker_loop(shared: Arc<BackgroundWorkersShared>, specification: Arc<DaemonWorkerSpec>) {
    let mut observed_wake_generation = shared.wake_generation(specification.kind);
    let mut empty_idle_interval = specification.idle_interval;
    while !shared.stopped.load(Ordering::Acquire) {
        let wake_generation = shared.wake_generation(specification.kind);
        if wake_generation != observed_wake_generation {
            observed_wake_generation = wake_generation;
            empty_idle_interval = specification.idle_interval;
        }
        shared.update(specification.kind, |state| state.active = true);
        let result = {
            let span = worker_execution_span(specification.kind);
            let _entered = span.enter();
            catch_unwind(AssertUnwindSafe(|| specification.task.execute()))
        };
        let mut successful_empty_cycle = false;
        let saturated = match result {
            Ok(Ok(cycle)) => {
                successful_empty_cycle = cycle.processed == 0 && cycle.failed == 0;
                if cycle.processed > 0
                    && matches!(
                        specification.kind,
                        DaemonWorkerKind::Restart
                            | DaemonWorkerKind::Observation
                            | DaemonWorkerKind::ProcessExit
                    )
                {
                    shared.wake(DaemonWorkerKind::Outbox);
                }
                shared.update(specification.kind, |state| {
                    state.active = false;
                    state.cycles = state.cycles.saturating_add(1);
                    state.processed = state
                        .processed
                        .saturating_add(u64::try_from(cycle.processed).unwrap_or(u64::MAX));
                    state.failures = state
                        .failures
                        .saturating_add(u64::try_from(cycle.failed).unwrap_or(u64::MAX));
                    if cycle.last_failure.is_some() {
                        state.last_failure = cycle.last_failure;
                    }
                });
                cycle.saturated
            }
            Ok(Err(error)) => {
                shared.update(specification.kind, |state| {
                    state.active = false;
                    state.cycles = state.cycles.saturating_add(1);
                    state.failures = state.failures.saturating_add(1);
                    state.last_failure = Some(error.category());
                });
                false
            }
            Err(_) => {
                shared.update(specification.kind, |state| {
                    state.active = false;
                    state.cycles = state.cycles.saturating_add(1);
                    state.panics = state.panics.saturating_add(1);
                    state.last_failure = Some(ApplicationErrorCategory::Internal);
                    state.stopped = true;
                });
                return;
            }
        };
        if !successful_empty_cycle {
            empty_idle_interval = specification.idle_interval;
        }
        if !saturated {
            shared.wait(
                specification.kind,
                observed_wake_generation,
                empty_idle_interval,
            );
            if successful_empty_cycle {
                empty_idle_interval = empty_idle_interval
                    .saturating_mul(2)
                    .min(specification.maximum_empty_idle_interval);
            }
        }
    }
    shared.update(specification.kind, |state| {
        state.active = false;
        state.stopped = true;
    });
}

/// Builds the fixed callsite matching a worker's central application span.
fn worker_execution_span(kind: DaemonWorkerKind) -> Span {
    match kind {
        DaemonWorkerKind::ProfileReload => {
            tracing::span!(target: "undercurrent.background", Level::INFO, TraceSpanName::ReloadProfiles.as_str(), worker_kind = kind.as_str())
        }
        DaemonWorkerKind::AttemptReconciliation => tracing::span!(
            target: "undercurrent.background",
            Level::INFO,
            TraceSpanName::ReconcileOperationAttempts.as_str(),
            worker_kind = kind.as_str(),
        ),
        DaemonWorkerKind::Outbox => tracing::span!(
            target: "undercurrent.background",
            Level::INFO,
            TraceSpanName::DispatchOutbox.as_str(),
            worker_kind = kind.as_str(),
        ),
        DaemonWorkerKind::Restart => tracing::span!(
            target: "undercurrent.background",
            Level::INFO,
            TraceSpanName::RunScheduledRestart.as_str(),
            worker_kind = kind.as_str(),
        ),
        DaemonWorkerKind::Observation => tracing::span!(
            target: "undercurrent.background",
            Level::INFO,
            TraceSpanName::DispatchObservations.as_str(),
            worker_kind = kind.as_str(),
        ),
        DaemonWorkerKind::ProcessExit => tracing::span!(
            target: "undercurrent.background",
            Level::INFO,
            TraceSpanName::ObserveProcessExit.as_str(),
            worker_kind = kind.as_str(),
        ),
    }
}

fn stop_and_interrupt(shared: &BackgroundWorkersShared, specifications: &[Arc<DaemonWorkerSpec>]) {
    shared.stopped.store(true, Ordering::Release);
    if shared.interrupts_sent.swap(true, Ordering::AcqRel) {
        return;
    }
    shared.changed.notify_all();
    for specification in specifications {
        if catch_unwind(AssertUnwindSafe(|| specification.task.interrupt())).is_err() {
            shared.update(specification.kind, |state| {
                state.panics = state.panics.saturating_add(1);
                state.last_failure = Some(ApplicationErrorCategory::Internal);
            });
        }
    }
}

fn join_workers(
    shared: &BackgroundWorkersShared,
    workers: Vec<(DaemonWorkerKind, JoinHandle<()>)>,
) {
    for (kind, worker) in workers {
        if worker.join().is_err() {
            shared.update(kind, |state| {
                state.panics = state.panics.saturating_add(1);
                state.last_failure = Some(ApplicationErrorCategory::Internal);
            });
        }
        shared.update(kind, |state| {
            state.active = false;
            state.stopped = true;
        });
    }
}
