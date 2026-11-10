// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeSet, VecDeque};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use undercurrent_application::{ApplicationErrorCategory, TraceSpanName};
use undercurrent_daemon::{
    DaemonBackgroundTask, DaemonBackgroundTaskError, DaemonBackgroundWorkers,
    DaemonBackgroundWorkersCreateError, DaemonDiagnosticField, DaemonDiagnosticSection,
    DaemonDiagnosticSource, DaemonDiagnosticValue, DaemonWorkerCycle, DaemonWorkerKind,
    DaemonWorkerSpec,
};

const WORKER_STACK_BYTES: usize = 256 * 1_024;
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[test]
fn worker_kind_registry_is_complete_and_stable() {
    assert_eq!(
        DaemonWorkerKind::ALL.map(DaemonWorkerKind::as_str),
        [
            "attempt_reconciliation",
            "outbox",
            "restart",
            "observation",
            "process_exit",
            "profile_reload",
        ]
    );
    assert_eq!(
        DaemonWorkerKind::ALL
            .into_iter()
            .collect::<BTreeSet<_>>()
            .len(),
        DaemonWorkerKind::ALL.len()
    );
    assert_eq!(
        DaemonWorkerKind::ALL.map(DaemonWorkerKind::trace_span_name),
        [
            TraceSpanName::ReconcileOperationAttempts,
            TraceSpanName::DispatchOutbox,
            TraceSpanName::RunScheduledRestart,
            TraceSpanName::DispatchObservations,
            TraceSpanName::ObserveProcessExit,
            TraceSpanName::ReloadProfiles,
        ]
    );
    let mut emitted_spans = DaemonWorkerKind::ALL
        .map(DaemonWorkerKind::trace_span_name)
        .into_iter()
        .collect::<BTreeSet<_>>();
    emitted_spans.insert(TraceSpanName::ReconcileStartup);
    assert_eq!(
        emitted_spans,
        TraceSpanName::ALL.into_iter().collect::<BTreeSet<_>>()
    );
}

#[test]
fn worker_configuration_rejects_unbounded_or_ambiguous_ownership() {
    assert!(matches!(
        DaemonBackgroundWorkers::start(Vec::new(), WORKER_STACK_BYTES),
        Err(DaemonBackgroundWorkersCreateError::InvalidLimit {
            field: "worker_count"
        })
    ));
    assert!(matches!(
        DaemonBackgroundWorkers::start(
            vec![specification(DaemonWorkerKind::Outbox, Arc::new(IdleTask))],
            32 * 1_024,
        ),
        Err(DaemonBackgroundWorkersCreateError::InvalidLimit {
            field: "worker_thread_stack_bytes"
        })
    ));
    assert!(matches!(
        DaemonWorkerSpec::new(DaemonWorkerKind::Outbox, Duration::ZERO, Arc::new(IdleTask)),
        Err(DaemonBackgroundWorkersCreateError::InvalidLimit {
            field: "idle_interval"
        })
    ));
    assert!(matches!(
        DaemonWorkerSpec::new(
            DaemonWorkerKind::Outbox,
            Duration::from_millis(20),
            Arc::new(IdleTask),
        )
        .expect("base interval")
        .with_adaptive_empty_backoff(Duration::from_millis(10)),
        Err(DaemonBackgroundWorkersCreateError::InvalidLimit {
            field: "maximum_empty_idle_interval"
        })
    ));
    assert!(matches!(
        DaemonBackgroundWorkers::start(
            vec![
                specification(DaemonWorkerKind::Outbox, Arc::new(IdleTask)),
                specification(DaemonWorkerKind::Outbox, Arc::new(IdleTask)),
            ],
            WORKER_STACK_BYTES,
        ),
        Err(DaemonBackgroundWorkersCreateError::DuplicateKind(
            DaemonWorkerKind::Outbox
        ))
    ));
}

#[test]
fn saturated_nonempty_cycles_continue_without_polling_delay() {
    let task = Arc::new(ScriptedTask::new([
        DaemonWorkerCycle::processed(2, true),
        DaemonWorkerCycle::processed(2, true),
        DaemonWorkerCycle::processed(1, false),
    ]));
    let workers = DaemonBackgroundWorkers::start(
        vec![specification(DaemonWorkerKind::Outbox, task.clone())],
        WORKER_STACK_BYTES,
    )
    .expect("workers");

    wait_until(|| task.calls.load(Ordering::Acquire) == 3);
    thread::sleep(Duration::from_millis(30));
    assert_eq!(task.calls.load(Ordering::Acquire), 3);

    workers.shutdown();
    assert_eq!(
        workers.pressure(),
        [undercurrent_daemon::DaemonWorkerPressure {
            kind: DaemonWorkerKind::Outbox,
            active: false,
            stopped: true,
            cycles: 3,
            processed: 5,
            failures: 0,
            panics: 0,
            last_failure: None,
        }]
    );
}

#[test]
fn targeted_wake_is_not_lost_between_task_execution_and_idle_wait() {
    let outbox = Arc::new(WakeRaceTask::default());
    let restart = Arc::new(ScriptedTask::new([DaemonWorkerCycle::idle()]));
    let interval = Duration::from_secs(10);
    let workers = DaemonBackgroundWorkers::start(
        vec![
            DaemonWorkerSpec::new(DaemonWorkerKind::Outbox, interval, outbox.clone())
                .expect("outbox specification"),
            DaemonWorkerSpec::new(DaemonWorkerKind::Restart, interval, restart.clone())
                .expect("restart specification"),
        ],
        WORKER_STACK_BYTES,
    )
    .expect("workers");
    wait_until(|| outbox.entered.load(Ordering::Acquire));
    wait_until(|| restart.calls.load(Ordering::Acquire) == 1);

    workers.wake(DaemonWorkerKind::Outbox);
    outbox.release();
    wait_until(|| outbox.calls.load(Ordering::Acquire) >= 2);
    thread::sleep(Duration::from_millis(30));
    assert_eq!(outbox.calls.load(Ordering::Acquire), 2);
    assert_eq!(restart.calls.load(Ordering::Acquire), 1);

    workers.shutdown();
}

#[test]
fn consecutive_empty_cycles_back_off_to_cap_and_targeted_wake_resets_the_base_delay() {
    let task = Arc::new(TimedIdleTask::default());
    // Separate the base and cap enough to distinguish reset behavior even when
    // a hosted runner delays a runnable worker by hundreds of milliseconds.
    let base = Duration::from_millis(200);
    let cap = Duration::from_millis(1_600);
    let specification = DaemonWorkerSpec::new(DaemonWorkerKind::Outbox, base, task.clone())
        .expect("base specification")
        .with_adaptive_empty_backoff(cap)
        .expect("adaptive specification");
    let workers =
        DaemonBackgroundWorkers::start(vec![specification], WORKER_STACK_BYTES).expect("workers");

    wait_until(|| task.call_count() >= 5);
    let calls = task.call_times();
    assert!(calls[1].duration_since(calls[0]) >= Duration::from_millis(150));
    assert!(calls[2].duration_since(calls[1]) >= Duration::from_millis(350));
    assert!(calls[3].duration_since(calls[2]) >= Duration::from_millis(750));
    assert!(calls[4].duration_since(calls[3]) >= Duration::from_millis(1_500));

    let before_wake = task.call_count();
    let wake_started = Instant::now();
    workers.wake(DaemonWorkerKind::Outbox);
    wait_until(|| task.call_count() > before_wake);
    assert!(wake_started.elapsed() < Duration::from_millis(1_200));
    let after_wake = task.call_count();
    wait_until(|| task.call_count() > after_wake);
    let reset_calls = task.call_times();
    let last = reset_calls.len();
    let reset_delay = reset_calls[last - 1].duration_since(reset_calls[last - 2]);
    assert!(reset_delay >= Duration::from_millis(150));
    assert!(reset_delay < Duration::from_millis(1_200));

    workers.shutdown();
}

#[test]
fn nonempty_state_workers_wake_outbox_dispatch_without_waiting_for_its_poll() {
    let outbox = Arc::new(ScriptedTask::new([DaemonWorkerCycle::idle()]));
    let restart = Arc::new(GatedProcessedTask::default());
    let interval = Duration::from_secs(10);
    let workers = DaemonBackgroundWorkers::start(
        vec![
            DaemonWorkerSpec::new(DaemonWorkerKind::Outbox, interval, outbox.clone())
                .expect("outbox specification"),
            DaemonWorkerSpec::new(DaemonWorkerKind::Restart, interval, restart.clone())
                .expect("restart specification"),
        ],
        WORKER_STACK_BYTES,
    )
    .expect("workers");
    wait_until(|| outbox.calls.load(Ordering::Acquire) == 1);
    wait_until(|| restart.entered.load(Ordering::Acquire));

    restart.release();
    wait_until(|| outbox.calls.load(Ordering::Acquire) >= 2);
    assert_eq!(outbox.calls.load(Ordering::Acquire), 2);

    workers.shutdown();
}

#[test]
fn task_failures_are_redacted_and_counted_without_stopping_the_worker() {
    let task = Arc::new(FailingTask {
        calls: AtomicUsize::new(0),
    });
    let direct_error = task.execute().expect_err("failure");
    assert_eq!(
        direct_error.category(),
        ApplicationErrorCategory::Unavailable
    );
    assert!(!format!("{direct_error}").contains("private-value"));
    assert!(!format!("{direct_error:?}").contains("private-value"));
    assert_eq!(
        direct_error.source().expect("preserved source").to_string(),
        "private-value"
    );

    let workers = DaemonBackgroundWorkers::start(
        vec![specification(DaemonWorkerKind::Restart, task.clone())],
        WORKER_STACK_BYTES,
    )
    .expect("workers");
    wait_until(|| task.calls.load(Ordering::Acquire) >= 2);

    let pressure = workers.pressure();
    assert!(pressure[0].failures >= 1);
    assert!(!pressure[0].stopped);
    assert_eq!(
        pressure[0].last_failure,
        Some(ApplicationErrorCategory::Unavailable)
    );
    workers.shutdown();
}

#[test]
fn a_task_panic_is_contained_and_stops_only_its_worker() {
    let task = Arc::new(PanicTask {
        called: AtomicBool::new(false),
    });
    let workers = DaemonBackgroundWorkers::start(
        vec![
            specification(DaemonWorkerKind::Observation, task.clone()),
            specification(DaemonWorkerKind::Outbox, Arc::new(IdleTask)),
        ],
        WORKER_STACK_BYTES,
    )
    .expect("workers");

    wait_until(|| {
        workers
            .pressure()
            .iter()
            .any(|pressure| pressure.kind == DaemonWorkerKind::Observation && pressure.stopped)
    });
    let pressure = workers.pressure();
    let observation = pressure
        .iter()
        .find(|pressure| pressure.kind == DaemonWorkerKind::Observation)
        .expect("observation pressure");
    let outbox = pressure
        .iter()
        .find(|pressure| pressure.kind == DaemonWorkerKind::Outbox)
        .expect("outbox pressure");
    assert!(task.called.load(Ordering::Acquire));
    assert_eq!(observation.panics, 1);
    assert_eq!(
        observation.last_failure,
        Some(ApplicationErrorCategory::Internal)
    );
    assert!(!outbox.stopped);
    workers.shutdown();
}

#[test]
fn shutdown_interrupts_blocking_tasks_once_and_joins_them() {
    let task = Arc::new(BlockingTask::default());
    let workers = DaemonBackgroundWorkers::start(
        vec![specification(DaemonWorkerKind::ProcessExit, task.clone())],
        WORKER_STACK_BYTES,
    )
    .expect("workers");
    wait_until(|| task.entered.load(Ordering::Acquire));

    let started = Instant::now();
    workers.shutdown();
    assert!(started.elapsed() < Duration::from_secs(1));
    workers.shutdown();
    assert_eq!(task.interrupts.load(Ordering::Acquire), 1);
    assert!(workers.is_shutdown());
    assert!(workers.pressure()[0].stopped);
}

#[test]
fn diagnostics_are_selected_and_payload_free() {
    let task = Arc::new(ScriptedTask::new([DaemonWorkerCycle::with_failures(
        3,
        2,
        Some(ApplicationErrorCategory::Unavailable),
        false,
    )]));
    let workers = DaemonBackgroundWorkers::start(
        vec![specification(DaemonWorkerKind::Outbox, task.clone())],
        WORKER_STACK_BYTES,
    )
    .expect("workers");
    wait_until(|| task.calls.load(Ordering::Acquire) == 1);

    assert!(
        workers
            .snapshot(&BTreeSet::from([DaemonDiagnosticSection::Streams]))
            .expect("unselected snapshot")
            .is_empty()
    );
    let rows = workers
        .snapshot(&BTreeSet::from([DaemonDiagnosticSection::SupervisorQueues]))
        .expect("selected snapshot");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].identity, "outbox");
    assert_eq!(
        rows[0].fields.get(&DaemonDiagnosticField::Count),
        Some(&DaemonDiagnosticValue::Unsigned(1))
    );
    assert_eq!(
        rows[0].fields.get(&DaemonDiagnosticField::Items),
        Some(&DaemonDiagnosticValue::Unsigned(3))
    );
    assert_eq!(
        rows[0].fields.get(&DaemonDiagnosticField::Failed),
        Some(&DaemonDiagnosticValue::Unsigned(2))
    );
    assert_eq!(
        workers.pressure()[0].last_failure,
        Some(ApplicationErrorCategory::Unavailable)
    );
    assert_eq!(
        rows[0].fields.keys().copied().collect::<BTreeSet<_>>(),
        BTreeSet::from([
            DaemonDiagnosticField::Active,
            DaemonDiagnosticField::Closed,
            DaemonDiagnosticField::Count,
            DaemonDiagnosticField::Failed,
            DaemonDiagnosticField::Items,
        ])
    );
    workers.shutdown();
}

fn specification(kind: DaemonWorkerKind, task: Arc<dyn DaemonBackgroundTask>) -> DaemonWorkerSpec {
    DaemonWorkerSpec::new(kind, Duration::from_secs(1), task).expect("specification")
}

fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + TEST_TIMEOUT;
    while !condition() {
        assert!(Instant::now() < deadline, "condition timed out");
        thread::sleep(Duration::from_millis(2));
    }
}

struct IdleTask;

impl DaemonBackgroundTask for IdleTask {
    fn execute(&self) -> Result<DaemonWorkerCycle, DaemonBackgroundTaskError> {
        Ok(DaemonWorkerCycle::idle())
    }
}

struct ScriptedTask {
    cycles: Mutex<VecDeque<DaemonWorkerCycle>>,
    calls: AtomicUsize,
}

#[derive(Default)]
struct TimedIdleTask {
    calls: Mutex<Vec<Instant>>,
}

impl TimedIdleTask {
    fn call_count(&self) -> usize {
        self.calls.lock().expect("timed calls").len()
    }

    fn call_times(&self) -> Vec<Instant> {
        self.calls.lock().expect("timed calls").clone()
    }
}

impl DaemonBackgroundTask for TimedIdleTask {
    fn execute(&self) -> Result<DaemonWorkerCycle, DaemonBackgroundTaskError> {
        self.calls.lock().expect("timed calls").push(Instant::now());
        Ok(DaemonWorkerCycle::idle())
    }
}

#[derive(Default)]
struct GatedProcessedTask {
    entered: AtomicBool,
    released: Mutex<bool>,
    changed: Condvar,
}

impl GatedProcessedTask {
    fn release(&self) {
        *self.released.lock().expect("release lock") = true;
        self.changed.notify_all();
    }
}

impl DaemonBackgroundTask for GatedProcessedTask {
    fn execute(&self) -> Result<DaemonWorkerCycle, DaemonBackgroundTaskError> {
        self.entered.store(true, Ordering::Release);
        let released = self.released.lock().expect("release lock");
        let _guard = self
            .changed
            .wait_while(released, |released| !*released)
            .expect("release wait");
        Ok(DaemonWorkerCycle::processed(1, false))
    }
}

#[derive(Default)]
struct WakeRaceTask {
    calls: AtomicUsize,
    entered: AtomicBool,
    released: Mutex<bool>,
    changed: Condvar,
}

impl WakeRaceTask {
    fn release(&self) {
        *self.released.lock().expect("release lock") = true;
        self.changed.notify_all();
    }
}

impl DaemonBackgroundTask for WakeRaceTask {
    fn execute(&self) -> Result<DaemonWorkerCycle, DaemonBackgroundTaskError> {
        let call = self.calls.fetch_add(1, Ordering::AcqRel);
        if call == 0 {
            self.entered.store(true, Ordering::Release);
            let released = self.released.lock().expect("release lock");
            let _guard = self
                .changed
                .wait_while(released, |released| !*released)
                .expect("release wait");
        }
        Ok(DaemonWorkerCycle::idle())
    }
}

impl ScriptedTask {
    fn new<const N: usize>(cycles: [DaemonWorkerCycle; N]) -> Self {
        Self {
            cycles: Mutex::new(cycles.into()),
            calls: AtomicUsize::new(0),
        }
    }
}

impl DaemonBackgroundTask for ScriptedTask {
    fn execute(&self) -> Result<DaemonWorkerCycle, DaemonBackgroundTaskError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Ok(self
            .cycles
            .lock()
            .expect("cycles")
            .pop_front()
            .unwrap_or_else(DaemonWorkerCycle::idle))
    }
}

struct FailingTask {
    calls: AtomicUsize,
}

impl DaemonBackgroundTask for FailingTask {
    fn execute(&self) -> Result<DaemonWorkerCycle, DaemonBackgroundTaskError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Err(DaemonBackgroundTaskError::new(
            ApplicationErrorCategory::Unavailable,
            PrivateError,
        ))
    }
}

#[derive(Debug)]
struct PrivateError;

impl Display for PrivateError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("private-value")
    }
}

impl Error for PrivateError {}

struct PanicTask {
    called: AtomicBool,
}

impl DaemonBackgroundTask for PanicTask {
    fn execute(&self) -> Result<DaemonWorkerCycle, DaemonBackgroundTaskError> {
        self.called.store(true, Ordering::Release);
        panic!("contained test panic")
    }
}

#[derive(Default)]
struct BlockingTask {
    entered: AtomicBool,
    interrupted: Mutex<bool>,
    changed: Condvar,
    interrupts: AtomicUsize,
}

impl DaemonBackgroundTask for BlockingTask {
    fn execute(&self) -> Result<DaemonWorkerCycle, DaemonBackgroundTaskError> {
        self.entered.store(true, Ordering::Release);
        let interrupted = self.interrupted.lock().expect("interrupted");
        let _guard = self
            .changed
            .wait_while(interrupted, |interrupted| !*interrupted)
            .expect("interrupted wait");
        Ok(DaemonWorkerCycle::idle())
    }

    fn interrupt(&self) {
        self.interrupts.fetch_add(1, Ordering::AcqRel);
        *self.interrupted.lock().expect("interrupted") = true;
        self.changed.notify_all();
    }
}
