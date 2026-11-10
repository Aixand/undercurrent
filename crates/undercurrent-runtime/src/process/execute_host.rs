// SPDX-License-Identifier: Apache-2.0
//! Host-owned one-shot execution, process supervision, and retained binary results.
//!
//! Jobs commit a running record before starting a supervisor and persist terminal
//! evidence before exposing it. Running records recovered after host loss become
//! failures with unknown effects, not permission to execute again. Terminal jobs
//! consume capacity until acknowledged after application settlement. Live shell
//! exports are omitted from the journal; ordinary request context is persisted.

use super::host_files;
use std::collections::{BTreeMap, btree_map::Entry};
use std::fs;
use std::io::{self, Read, Write};
#[cfg(all(test, unix))]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::process::Child;
use std::process::{Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
#[cfg(windows)]
use undercurrent_platform_windows::{JobChild as Child, JobControl};

#[cfg(unix)]
use nix::sys::signal::{Signal, kill};
#[cfg(unix)]
use nix::unistd::Pid;
use serde::{Deserialize, Serialize};

#[cfg(target_os = "linux")]
#[cfg(unix)]
use nix::sys::prctl;
#[cfg(unix)]
use nix::unistd::getppid;

use crate::process_host_protocol::{
    HostExecutionTerminalKind, HostFailure, HostFailureContext, HostFailureKind, HostResponseValue,
};

const MAXIMUM_JOB_ID_BYTES: usize = 128;
const MAXIMUM_POLL_WAIT: Duration = Duration::from_millis(100);
const MAXIMUM_SUPERVISOR_BOOTSTRAP_BYTES: usize = 16 * 1024 * 1024;
const MAXIMUM_SUPERVISOR_PREFIX_BYTES: usize = 4 * 1024;
const EXECUTION_JOURNAL_VERSION: undercurrent_foundation::ReleaseVersion =
    undercurrent_foundation::ReleaseVersion::Current;
const EXECUTION_JOURNAL_ALLOWANCE: usize = 64 * 1024;
const SUPERVISOR_STATUS_MAGIC: &[u8] = concat!("UCHXSUP", env!("CARGO_PKG_VERSION")).as_bytes();
const SUPERVISOR_SPAWN_FAILED: u8 = 0;
const SUPERVISOR_READY: u8 = 1;
const SUPERVISOR_STARTED_FAILED: u8 = 2;
#[cfg(any(target_os = "linux", target_os = "macos"))]
const OUTPUT_DRAIN_GRACE: Duration = Duration::from_secs(1);
const SUPERVISOR_OUTPUT_CHUNK: usize = 8192;
const SUPERVISOR_IDENTITY_BYTES: usize = 4 + 8;

/// Private argv marker used only by the daemon-independent session-host binary.
#[doc(hidden)]
pub const EXECUTION_SUPERVISOR_ARGUMENT: &str = "--undercurrent-execution-supervisor";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct HostExecutionSpec {
    // Live values exist only in memory and the private supervisor pipe.
    #[serde(skip)]
    pub live_environment: Option<crate::process_host_protocol::HostResolvedSecretEnvironment>,
    pub job_id: String,
    pub session_id: String,
    pub payload_digest: String,
    pub program: String,
    pub arguments: Vec<String>,
    pub environment: Vec<(String, String)>,
    pub working_directory: Option<String>,
    pub output_bytes: u64,
    pub deadline_millis: Option<i64>,
}

pub(super) struct HostExecutionRegistry {
    limit: usize,
    command_bytes_limit: usize,
    environment_entries_limit: usize,
    environment_bytes_limit: usize,
    capture_bytes_limit: usize,
    worker_stack_bytes: usize,
    launcher: ExecutionLauncher,
    journal: Arc<ExecutionJournal>,
    jobs: Mutex<BTreeMap<String, Arc<ExecutionJob>>>,
}

#[derive(Clone)]
enum ExecutionLauncher {
    Supervisor {
        executable: PathBuf,
        arguments: Vec<String>,
        test_helper: bool,
    },
    Unavailable,
}

struct ExecutionRegistryConfig {
    limit: usize,
    command_bytes_limit: usize,
    environment_entries_limit: usize,
    environment_bytes_limit: usize,
    capture_bytes_limit: usize,
    worker_stack_bytes: usize,
    journal_directory: PathBuf,
}

impl HostExecutionRegistry {
    pub(super) fn new(
        limit: usize,
        command_bytes_limit: usize,
        environment_entries_limit: usize,
        environment_bytes_limit: usize,
        capture_bytes_limit: usize,
        worker_stack_bytes: usize,
        journal_directory: PathBuf,
    ) -> io::Result<Self> {
        #[cfg(any(target_os = "linux", target_os = "macos", windows))]
        let launcher = std::env::current_exe()
            .map(|executable| ExecutionLauncher::Supervisor {
                executable,
                arguments: vec![EXECUTION_SUPERVISOR_ARGUMENT.to_owned()],
                test_helper: false,
            })
            .unwrap_or(ExecutionLauncher::Unavailable);
        #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
        let launcher = ExecutionLauncher::Unavailable;
        Self::new_with_launcher(
            ExecutionRegistryConfig {
                limit,
                command_bytes_limit,
                environment_entries_limit,
                environment_bytes_limit,
                capture_bytes_limit,
                worker_stack_bytes,
                journal_directory,
            },
            launcher,
        )
    }

    fn new_with_launcher(
        config: ExecutionRegistryConfig,
        launcher: ExecutionLauncher,
    ) -> io::Result<Self> {
        let ExecutionRegistryConfig {
            limit,
            command_bytes_limit,
            environment_entries_limit,
            environment_bytes_limit,
            capture_bytes_limit,
            worker_stack_bytes,
            journal_directory,
        } = config;
        let journal_record_limit = command_bytes_limit
            .checked_add(environment_bytes_limit)
            .and_then(|bytes| bytes.checked_add(capture_bytes_limit.saturating_mul(6)))
            .and_then(|bytes| bytes.checked_add(EXECUTION_JOURNAL_ALLOWANCE))
            .ok_or_else(|| io::Error::other("execution journal byte limit overflow"))?;
        let journal = Arc::new(ExecutionJournal::open(
            journal_directory,
            journal_record_limit,
        )?);
        let mut jobs = BTreeMap::new();
        for (spec, state) in journal.recover(limit.max(1), capture_bytes_limit.max(1))? {
            let job = Arc::new(ExecutionJob {
                spec: spec.clone(),
                cancelled: AtomicBool::new(false),
                supervisor_pid: AtomicU64::new(0),
                #[cfg(windows)]
                supervisor_job: Mutex::new(None),
                workload_pid: AtomicU64::new(0),
                workload_start_ticks: AtomicU64::new(0),
                journal: Arc::clone(&journal),
                state: Mutex::new(state),
                changed: Condvar::new(),
            });
            if jobs.insert(spec.job_id.clone(), job).is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "execution journal contains duplicate job identity",
                ));
            }
        }
        let registry = Self {
            limit: limit.max(1),
            command_bytes_limit: command_bytes_limit.max(1),
            environment_entries_limit: environment_entries_limit.max(1),
            environment_bytes_limit: environment_bytes_limit.max(1),
            capture_bytes_limit: capture_bytes_limit.max(1),
            worker_stack_bytes,
            launcher,
            journal,
            jobs: Mutex::new(jobs),
        };
        if registry
            .jobs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .any(|job| registry.validate(&job.spec).is_err())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "execution journal contains an invalid job specification",
            ));
        }
        Ok(registry)
    }

    pub(super) fn replay(
        &self,
        job_id: &str,
        session_id: &str,
        payload_digest: &str,
    ) -> Option<Result<HostResponseValue, HostFailure>> {
        let jobs = self.jobs.lock().unwrap_or_else(|p| p.into_inner());
        jobs.get(job_id).map(|job| {
            if job.spec.session_id == session_id && job.spec.payload_digest == payload_digest {
                Ok(HostResponseValue::ExecutionAccepted)
            } else {
                Err(failure(
                    HostFailureKind::Conflict,
                    HostFailureContext::Execute,
                ))
            }
        })
    }

    pub(super) fn start(&self, spec: HostExecutionSpec) -> Result<HostResponseValue, HostFailure> {
        self.validate(&spec)?;
        let job = Arc::new(ExecutionJob {
            spec: spec.clone(),
            cancelled: AtomicBool::new(false),
            supervisor_pid: AtomicU64::new(0),
            #[cfg(windows)]
            supervisor_job: Mutex::new(None),
            workload_pid: AtomicU64::new(0),
            workload_start_ticks: AtomicU64::new(0),
            journal: Arc::clone(&self.journal),
            state: Mutex::new(ExecutionState::Running),
            changed: Condvar::new(),
        });
        {
            let mut jobs = self
                .jobs
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let at_capacity = jobs.len() >= self.limit;
            match jobs.entry(spec.job_id.clone()) {
                Entry::Occupied(existing) => {
                    return if existing.get().spec == spec {
                        Ok(HostResponseValue::ExecutionAccepted)
                    } else {
                        Err(failure(
                            HostFailureKind::Conflict,
                            HostFailureContext::Execute,
                        ))
                    };
                }
                Entry::Vacant(_) if at_capacity => {
                    return Err(failure(
                        HostFailureKind::CapacityExceeded,
                        HostFailureContext::Execute,
                    ));
                }
                Entry::Vacant(entry) => {
                    self.journal
                        .persist(&spec, &ExecutionState::Running)
                        .map_err(|_| {
                            failure(HostFailureKind::Unavailable, HostFailureContext::Execute)
                        })?;
                    entry.insert(Arc::clone(&job));
                }
            }
        }

        let worker = Arc::clone(&job);
        let launcher = self.launcher.clone();
        if thread::Builder::new()
            .name("undercurrent-host-execution".to_owned())
            .stack_size(self.worker_stack_bytes)
            .spawn(move || run_execution(worker, launcher))
            .is_err()
        {
            let removed = self
                .jobs
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&spec.job_id);
            if removed.is_some() {
                let _ = self.journal.remove(&spec.job_id);
            }
            return Err(failure(
                HostFailureKind::Unavailable,
                HostFailureContext::Execute,
            ));
        }
        Ok(HostResponseValue::ExecutionAccepted)
    }

    pub(super) fn poll(
        &self,
        job_id: &str,
        wait: Duration,
    ) -> Result<HostResponseValue, HostFailure> {
        validate_job_id(job_id, HostFailureContext::PollExecution)?;
        let job = self
            .jobs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(job_id)
            .cloned()
            .ok_or_else(|| failure(HostFailureKind::NotFound, HostFailureContext::PollExecution))?;
        let state = job
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let state = if matches!(*state, ExecutionState::Running) && !wait.is_zero() {
            job.changed
                .wait_timeout(state, wait.min(MAXIMUM_POLL_WAIT))
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0
        } else {
            state
        };
        Ok(match &*state {
            ExecutionState::Running => HostResponseValue::ExecutionRunning,
            ExecutionState::Terminal(result) => HostResponseValue::ExecutionTerminal {
                kind: result.kind,
                exit_code: result.exit_code,
                stdout: result.stdout.clone(),
                stdout_source_bytes: result.stdout_source_bytes,
                stderr: result.stderr.clone(),
                stderr_source_bytes: result.stderr_source_bytes,
            },
            ExecutionState::Failed {
                kind,
                effect_started,
            } => HostResponseValue::ExecutionFailed {
                kind: *kind,
                effect_started: *effect_started,
            },
        })
    }

    pub(super) fn cancel(&self, job_id: &str) -> Result<HostResponseValue, HostFailure> {
        validate_job_id(job_id, HostFailureContext::CancelExecution)?;
        let job = self
            .jobs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(job_id)
            .cloned()
            .ok_or_else(|| {
                failure(
                    HostFailureKind::NotFound,
                    HostFailureContext::CancelExecution,
                )
            })?;
        job.cancelled.store(true, Ordering::Release);
        signal_supervisor(&job);
        Ok(HostResponseValue::Completed)
    }

    pub(super) fn acknowledge(&self, job_id: &str) -> Result<HostResponseValue, HostFailure> {
        validate_job_id(job_id, HostFailureContext::AcknowledgeExecution)?;
        let mut jobs = self
            .jobs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(job) = jobs.get(job_id) else {
            return Ok(HostResponseValue::Completed);
        };
        if matches!(
            *job.state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            ExecutionState::Running
        ) {
            return Err(failure(
                HostFailureKind::Conflict,
                HostFailureContext::AcknowledgeExecution,
            ));
        }
        self.journal.remove(job_id).map_err(|_| {
            failure(
                HostFailureKind::Unavailable,
                HostFailureContext::AcknowledgeExecution,
            )
        })?;
        jobs.remove(job_id);
        Ok(HostResponseValue::Completed)
    }

    pub(super) fn cancel_all(&self) {
        for job in self
            .jobs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
        {
            job.cancelled.store(true, Ordering::Release);
            signal_supervisor(job);
        }
    }

    pub(super) fn cancel_all_and_wait(&self, timeout: Duration) {
        self.cancel_all();
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if !self.has_running_jobs() {
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }
        for job in self
            .jobs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
        {
            force_exact_job_cleanup(job);
        }
        let fallback_deadline = Instant::now() + Duration::from_millis(250);
        while Instant::now() < fallback_deadline && self.has_running_jobs() {
            thread::sleep(Duration::from_millis(5));
        }
    }

    pub(super) fn has_running_jobs(&self) -> bool {
        self.jobs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .any(|job| {
                matches!(
                    *job.state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()),
                    ExecutionState::Running
                )
            })
    }

    fn validate(&self, spec: &HostExecutionSpec) -> Result<(), HostFailure> {
        validate_job_id(&spec.job_id, HostFailureContext::Execute)?;
        if spec.session_id.is_empty()
            || spec.program.is_empty()
            || spec.output_bytes == 0
            || spec.program.contains('\0')
            || spec.arguments.iter().any(|value| value.contains('\0'))
            || spec.payload_digest.len() != 64
            || !spec
                .payload_digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(failure(
                HostFailureKind::InvalidData,
                HostFailureContext::Execute,
            ));
        }
        let command_bytes = spec
            .arguments
            .iter()
            .try_fold(spec.program.len(), |total, value| {
                total.checked_add(value.len())
            });
        if command_bytes.is_none_or(|bytes| bytes > self.command_bytes_limit)
            || spec.environment.len() > self.environment_entries_limit
            || spec.output_bytes > self.capture_bytes_limit as u64
        {
            return Err(failure(
                HostFailureKind::CapacityExceeded,
                HostFailureContext::Execute,
            ));
        }
        if let Some(environment) = &spec.live_environment {
            let bytes = environment
                .values()
                .iter()
                .fold(0usize, |total, (name, value)| {
                    total.saturating_add(name.len()).saturating_add(value.len())
                });
            if environment.values().len() > self.environment_entries_limit
                || bytes > self.environment_bytes_limit
            {
                return Err(failure(
                    HostFailureKind::CapacityExceeded,
                    HostFailureContext::Execute,
                ));
            }
        }
        let mut names = BTreeMap::new();
        let mut environment_bytes = 0usize;
        for (name, value) in &spec.environment {
            if name.is_empty()
                || name.contains(['=', '\0'])
                || value.contains('\0')
                || names.insert(name, ()).is_some()
            {
                return Err(failure(
                    HostFailureKind::InvalidData,
                    HostFailureContext::Execute,
                ));
            }
            environment_bytes = environment_bytes
                .checked_add(name.len())
                .and_then(|bytes| bytes.checked_add(value.len()))
                .ok_or_else(|| {
                    failure(
                        HostFailureKind::CapacityExceeded,
                        HostFailureContext::Execute,
                    )
                })?;
        }
        if environment_bytes > self.environment_bytes_limit
            || spec
                .working_directory
                .as_deref()
                .is_some_and(|path| path.contains('\0') || !Path::new(path).is_absolute())
        {
            return Err(failure(
                HostFailureKind::InvalidData,
                HostFailureContext::Execute,
            ));
        }
        Ok(())
    }
}

struct ExecutionJournal {
    directory: PathBuf,
    record_bytes_limit: usize,
    next_temporary: AtomicU64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableExecutionRecord {
    version: undercurrent_foundation::ReleaseVersion,
    spec: HostExecutionSpec,
    state: DurableExecutionState,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum DurableExecutionState {
    Running,
    Terminal {
        kind: HostExecutionTerminalKind,
        exit_code: Option<i32>,
        stdout: Vec<u8>,
        stdout_source_bytes: u64,
        stderr: Vec<u8>,
        stderr_source_bytes: u64,
    },
    Failed {
        kind: HostFailureKind,
        effect_started: bool,
    },
}

impl ExecutionJournal {
    fn open(directory: PathBuf, record_bytes_limit: usize) -> io::Result<Self> {
        host_files::prepare_directory(&directory)?;
        let metadata = fs::symlink_metadata(&directory)?;
        if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "execution journal path is not a private directory",
            ));
        }

        Ok(Self {
            directory,
            record_bytes_limit: record_bytes_limit.max(EXECUTION_JOURNAL_ALLOWANCE),
            next_temporary: AtomicU64::new(0),
        })
    }

    fn recover(
        &self,
        job_limit: usize,
        capture_bytes_limit: usize,
    ) -> io::Result<Vec<(HostExecutionSpec, ExecutionState)>> {
        let mut recovered = Vec::new();
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let Some(file_name) = file_name.to_str() else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "execution journal contains a non-text file name",
                ));
            };
            if file_name.starts_with(".tmp-") {
                let _ = fs::remove_file(entry.path());
                continue;
            }
            if !file_name.starts_with("job-") || !file_name.ends_with(".json") {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "execution journal contains an unknown record",
                ));
            }
            if recovered.len() >= job_limit {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "execution journal exceeds configured job capacity",
                ));
            }
            let metadata = fs::symlink_metadata(entry.path())?;
            if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "execution journal record is not a regular file",
                ));
            }
            if metadata.len() > self.record_bytes_limit as u64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "execution journal record exceeds its byte limit",
                ));
            }
            host_files::restrict(&entry.path())?;
            let mut encoded = Vec::with_capacity(metadata.len() as usize);
            host_files::read(&entry.path())?
                .take(self.record_bytes_limit as u64 + 1)
                .read_to_end(&mut encoded)?;
            if encoded.len() > self.record_bytes_limit {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "execution journal record exceeds its byte limit",
                ));
            }
            let record: DurableExecutionRecord = serde_json::from_slice(&encoded)
                .map_err(|source| io::Error::new(io::ErrorKind::InvalidData, source))?;
            if record.version != EXECUTION_JOURNAL_VERSION
                || validate_job_id(&record.spec.job_id, HostFailureContext::Execute).is_err()
                || file_name != self.file_name(&record.spec.job_id)
                || record.spec.output_bytes > capture_bytes_limit as u64
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "execution journal record is incompatible",
                ));
            }
            let (state, recovered_running) = durable_state(record.state, capture_bytes_limit)?;
            if recovered_running {
                self.persist(&record.spec, &state)?;
            }
            recovered.push((record.spec, state));
        }
        Ok(recovered)
    }

    fn persist(&self, spec: &HostExecutionSpec, state: &ExecutionState) -> io::Result<()> {
        let record = DurableExecutionRecord {
            version: EXECUTION_JOURNAL_VERSION,
            spec: spec.clone(),
            state: DurableExecutionState::from(state),
        };
        let encoded = serde_json::to_vec(&record)
            .map_err(|source| io::Error::new(io::ErrorKind::InvalidData, source))?;
        if encoded.len() > self.record_bytes_limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "execution journal record exceeds its byte limit",
            ));
        }
        let sequence = self.next_temporary.fetch_add(1, Ordering::Relaxed);
        let temporary = self
            .directory
            .join(format!(".tmp-{}-{sequence}", std::process::id()));
        let mut file = host_files::create_new(&temporary)?;
        let write_result = file
            .write_all(&encoded)
            .and_then(|()| file.sync_all())
            .and_then(|()| {
                drop(file);
                host_files::replace(&temporary, &self.record_path(&spec.job_id))
            })
            .and_then(|()| self.sync_directory());
        if write_result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        write_result
    }

    fn remove(&self, job_id: &str) -> io::Result<()> {
        match fs::remove_file(self.record_path(job_id)) {
            Ok(()) => self.sync_directory(),
            Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(source),
        }
    }

    fn record_path(&self, job_id: &str) -> PathBuf {
        self.directory.join(self.file_name(job_id))
    }

    fn file_name(&self, job_id: &str) -> String {
        format!("job-{job_id}.json")
    }

    fn sync_directory(&self) -> io::Result<()> {
        host_files::sync_directory(&self.directory)
    }
}

impl From<&ExecutionState> for DurableExecutionState {
    fn from(state: &ExecutionState) -> Self {
        match state {
            ExecutionState::Running => Self::Running,
            ExecutionState::Terminal(result) => Self::Terminal {
                kind: result.kind,
                exit_code: result.exit_code,
                stdout: result.stdout.clone(),
                stdout_source_bytes: result.stdout_source_bytes,
                stderr: result.stderr.clone(),
                stderr_source_bytes: result.stderr_source_bytes,
            },
            ExecutionState::Failed {
                kind,
                effect_started,
            } => Self::Failed {
                kind: *kind,
                effect_started: *effect_started,
            },
        }
    }
}

fn durable_state(
    state: DurableExecutionState,
    capture_bytes_limit: usize,
) -> io::Result<(ExecutionState, bool)> {
    Ok(match state {
        DurableExecutionState::Running => (
            ExecutionState::Failed {
                kind: HostFailureKind::Unavailable,
                effect_started: true,
            },
            true,
        ),
        DurableExecutionState::Terminal {
            kind,
            exit_code,
            stdout,
            stdout_source_bytes,
            stderr,
            stderr_source_bytes,
        } => {
            if stdout.len().saturating_add(stderr.len()) > capture_bytes_limit
                || stdout_source_bytes < stdout.len() as u64
                || stderr_source_bytes < stderr.len() as u64
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "execution journal terminal output is inconsistent",
                ));
            }
            (
                ExecutionState::Terminal(ExecutionResult {
                    kind,
                    exit_code,
                    stdout,
                    stdout_source_bytes,
                    stderr,
                    stderr_source_bytes,
                }),
                false,
            )
        }
        DurableExecutionState::Failed {
            kind,
            effect_started,
        } => (
            ExecutionState::Failed {
                kind,
                effect_started,
            },
            false,
        ),
    })
}

struct ExecutionJob {
    spec: HostExecutionSpec,
    cancelled: AtomicBool,
    supervisor_pid: AtomicU64,
    #[cfg(windows)]
    supervisor_job: Mutex<Option<JobControl>>,
    workload_pid: AtomicU64,
    workload_start_ticks: AtomicU64,
    journal: Arc<ExecutionJournal>,
    state: Mutex<ExecutionState>,
    changed: Condvar,
}

enum ExecutionState {
    Running,
    Terminal(ExecutionResult),
    Failed {
        kind: HostFailureKind,
        effect_started: bool,
    },
}

struct ExecutionResult {
    kind: HostExecutionTerminalKind,
    exit_code: Option<i32>,
    stdout: Vec<u8>,
    stdout_source_bytes: u64,
    stderr: Vec<u8>,
    stderr_source_bytes: u64,
}

struct CapturedOutput {
    bytes: Vec<u8>,
    source_bytes: u64,
    failed: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionSupervisorBootstrap {
    #[serde(default)]
    live_environment: Option<crate::process_host_protocol::HostResolvedSecretEnvironment>,
    expected_parent_pid: u32,
    program: String,
    arguments: Vec<String>,
    environment: Vec<(String, String)>,
    working_directory: Option<String>,
}

/// Terminal status returned by the private execution supervisor entry point.
#[doc(hidden)]
pub struct ExecutionSupervisorOutcome {
    code: Option<i32>,
    #[cfg_attr(windows, allow(dead_code))]
    signal: Option<i32>,
}

impl ExecutionSupervisorOutcome {
    /// Terminates the supervisor with the exact workload exit status.
    #[doc(hidden)]
    pub fn terminate(self) -> ! {
        #[cfg(unix)]
        if let Some(signal) = self.signal {
            let _ = signal_hook::low_level::emulate_default_handler(signal);
        }
        std::process::exit(self.code.unwrap_or(1))
    }
}

/// Runs the parent-death supervisor used by private UCHS jobs.
///
/// The caller must be the private `uc-session-host` self-exec mode. The
/// bootstrap is bounded and comes from its inherited stdin pipe. No command is
/// started on Unix until Linux `PDEATHSIG` or Darwin `NOTE_EXIT` is installed
/// and the expected parent PID has been checked again. On Windows the host
/// assigns the supervisor to a kill-on-close job before resuming its primary
/// thread. Readiness identifies a started workload, not its completed result.
#[doc(hidden)]
pub fn run_execution_supervisor(mut reader: impl Read) -> io::Result<ExecutionSupervisorOutcome> {
    #[cfg(windows)]
    {
        run_windows_execution_supervisor(&mut reader)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    {
        let _ = &mut reader;
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "execution supervision requires Linux or macOS",
        ));
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let bootstrap = read_supervisor_bootstrap(&mut reader)?;
        let parent_dead = Arc::new(AtomicBool::new(false));
        let signal_id =
            signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&parent_dead))?;
        #[cfg(target_os = "linux")]
        prctl::set_pdeathsig(Signal::SIGTERM).map_err(errno_io)?;
        #[cfg(target_os = "macos")]
        let parent_watch = ParentExitWatch::new(bootstrap.expected_parent_pid)?;
        if u32::try_from(getppid().as_raw()).ok() != Some(bootstrap.expected_parent_pid)
            || parent_dead.load(Ordering::Acquire)
        {
            signal_hook::low_level::unregister(signal_id);
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "session host exited before supervisor readiness",
            ));
        }

        let mut command = Command::new(&bootstrap.program);
        command
            .args(&bootstrap.arguments)
            .env_clear()
            .envs(
                bootstrap
                    .environment
                    .iter()
                    .map(|(name, value)| (name, value)),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0);
        if let Some(environment) = &bootstrap.live_environment {
            use std::os::unix::ffi::OsStringExt;
            command.env_clear();
            command.envs(
                environment
                    .values()
                    .iter()
                    .map(|(key, value)| (key, std::ffi::OsString::from_vec(value.clone()))),
            );
        }
        if let Some(directory) = &bootstrap.working_directory {
            // PWD must agree with the selected directory even with --dir-launch.
            command.env("PWD", directory);
            command.current_dir(directory);
        }
        #[cfg(target_os = "macos")]
        if parent_watch.exited() {
            parent_dead.store(true, Ordering::Release);
        }
        if parent_dead.load(Ordering::Acquire) {
            write_supervisor_status(SUPERVISOR_SPAWN_FAILED, None)?;
            signal_hook::low_level::unregister(signal_id);
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "execution was cancelled before workload spawn",
            ));
        }
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(source) => {
                write_supervisor_status(SUPERVISOR_SPAWN_FAILED, None)?;
                signal_hook::low_level::unregister(signal_id);
                return Err(source);
            }
        };
        let child_pid = child.id();
        let start_ticks = match execution_process_start(child_pid) {
            Ok(value) => value,
            Err(source) => {
                terminate_process_group(&mut child);
                let _ = child.wait();
                write_supervisor_status(SUPERVISOR_STARTED_FAILED, None)?;
                signal_hook::low_level::unregister(signal_id);
                return Err(source);
            }
        };
        let Some(stdout) = child.stdout.take() else {
            terminate_process_group(&mut child);
            let _ = child.wait();
            write_supervisor_status(SUPERVISOR_STARTED_FAILED, None)?;
            signal_hook::low_level::unregister(signal_id);
            return Err(io::Error::other("workload stdout was not piped"));
        };
        let Some(stderr) = child.stderr.take() else {
            terminate_process_group(&mut child);
            let _ = child.wait();
            write_supervisor_status(SUPERVISOR_STARTED_FAILED, None)?;
            signal_hook::low_level::unregister(signal_id);
            return Err(io::Error::other("workload stderr was not piped"));
        };
        write_supervisor_status(SUPERVISOR_READY, Some((child_pid, start_ticks)))?;

        let stop_relays = Arc::new(AtomicBool::new(false));
        let stdout_worker = relay_unix_output(stdout, FramedSupervisorOutput, stop_relays.clone());
        let stderr_worker = relay_unix_output(stderr, io::stderr(), stop_relays.clone());
        let (stdout_worker, stderr_worker) = match (stdout_worker, stderr_worker) {
            (Ok(stdout_worker), Ok(stderr_worker)) => (stdout_worker, stderr_worker),
            _ => {
                terminate_process_group(&mut child);
                let _ = child.wait();
                signal_hook::low_level::unregister(signal_id);
                return Err(io::Error::other("workload output relay could not start"));
            }
        };

        let mut drain_started = None;
        let status = loop {
            #[cfg(target_os = "macos")]
            if parent_watch.exited() {
                parent_dead.store(true, Ordering::Release);
            }
            if parent_dead.load(Ordering::Acquire) {
                terminate_process_group(&mut child);
                stop_relays.store(true, Ordering::Release);
                break child.wait()?;
            }
            // Pin the exited root identity until the owned process group is
            // retired and buffered output drains. Root exit ends the job; an
            // escaped writer must not retain its output pipes indefinitely.
            use rustix::process::{WaitId, WaitIdOptions, waitid};
            let pid = rustix::process::Pid::from_raw(child_pid as i32)
                .ok_or_else(|| io::Error::other("invalid workload PID"))?;
            let exited = match waitid(
                WaitId::Pid(pid),
                WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
            ) {
                Ok(status) => status,
                Err(rustix::io::Errno::INTR) => continue,
                Err(error) => {
                    terminate_process_group(&mut child);
                    let _ = child.wait();
                    signal_hook::low_level::unregister(signal_id);
                    return Err(error.into());
                }
            };
            if exited.is_some() {
                if drain_started.is_none() {
                    terminate_process_group(&mut child);
                    drain_started = Some(Instant::now());
                }
                if drain_started.is_some_and(|started| started.elapsed() >= OUTPUT_DRAIN_GRACE) {
                    stop_relays.store(true, Ordering::Release);
                }
                if stdout_worker.is_finished() && stderr_worker.is_finished() {
                    break child.wait()?;
                }
            }
            thread::sleep(Duration::from_millis(5));
        };
        let stdout_result = stdout_worker
            .join()
            .map_err(|_| io::Error::other("workload stdout relay panicked"))?;
        let stderr_result = stderr_worker
            .join()
            .map_err(|_| io::Error::other("workload stderr relay panicked"))?;
        stdout_result?;
        stderr_result?;
        finish_supervisor_output()?;
        signal_hook::low_level::unregister(signal_id);
        if parent_dead.load(Ordering::Acquire) {
            return Ok(ExecutionSupervisorOutcome {
                code: Some(1),
                signal: None,
            });
        }
        Ok(ExecutionSupervisorOutcome {
            code: status.code(),
            signal: status.signal(),
        })
    }
}

#[cfg(windows)]
fn run_windows_execution_supervisor(
    reader: &mut impl Read,
) -> io::Result<ExecutionSupervisorOutcome> {
    // The host assigns this supervisor to a kill-on-close job before resuming
    // its primary thread. Workloads inherit that job, including during setup.
    let bootstrap = read_supervisor_bootstrap(reader)?;
    let mut command = Command::new(&bootstrap.program);
    command
        .args(&bootstrap.arguments)
        .env_clear()
        .envs(bootstrap.environment)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(environment) = &bootstrap.live_environment {
        command.env_clear();
        for (key, value) in environment.values() {
            let value = std::str::from_utf8(value)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            command.env(key, value);
        }
    }
    if let Some(directory) = &bootstrap.working_directory {
        command.env("PWD", directory).current_dir(directory);
    }
    // cmd batch execution and Windows loader components require SystemRoot even
    // with an otherwise empty selected environment. Never replace an explicit value.
    if !command.get_envs().any(|(name, value)| {
        value.is_some()
            && name
                .to_str()
                .is_some_and(|name| name.eq_ignore_ascii_case("SystemRoot"))
    }) {
        command.env(
            "SystemRoot",
            undercurrent_platform_windows::windows_directory()?,
        );
    }
    let mut child = match Child::spawn(&mut command) {
        Ok(child) => child,
        Err(error) => {
            write_supervisor_status(SUPERVISOR_SPAWN_FAILED, None)?;
            return Err(error);
        }
    };
    let start = match execution_process_start(child.id()) {
        Ok(start) => start,
        Err(error) => {
            write_supervisor_status(SUPERVISOR_STARTED_FAILED, None)?;
            return Err(error);
        }
    };
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("workload stdout was not piped"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| io::Error::other("workload stderr was not piped"))?;
    write_supervisor_status(SUPERVISOR_READY, Some((child.id(), start)))?;
    let stdout_worker = relay_output(stdout, FramedSupervisorOutput)?;
    let stderr_worker = relay_output(stderr, io::stderr())?;
    let status = child.wait()?;
    // Descendants can retain output handles after the root exits. Retire the
    // complete workload before waiting for the output relays to observe EOF.
    child.kill()?;
    stdout_worker
        .join()
        .map_err(|_| io::Error::other("stdout relay panicked"))??;
    stderr_worker
        .join()
        .map_err(|_| io::Error::other("stderr relay panicked"))??;
    finish_supervisor_output()?;
    Ok(ExecutionSupervisorOutcome {
        code: status.code(),
        signal: None,
    })
}

fn spawn_supervisor(command: &mut Command) -> io::Result<Child> {
    #[cfg(unix)]
    {
        command.spawn()
    }
    #[cfg(windows)]
    {
        Child::spawn(command)
    }
}

#[cfg(windows)]
fn signal_supervisor(job: &ExecutionJob) {
    if let Some(control) = job
        .supervisor_job
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .as_ref()
    {
        let _ = control.terminate();
    }
}

#[cfg(windows)]
fn force_exact_job_cleanup(job: &ExecutionJob) {
    signal_supervisor(job);
}

#[cfg(windows)]
fn execution_process_start(pid: u32) -> io::Result<u64> {
    undercurrent_platform_windows::ProcessHandle::open(pid).map(|p| p.identity().created)
}

fn run_execution(job: Arc<ExecutionJob>, launcher: ExecutionLauncher) {
    if job.cancelled.load(Ordering::Acquire) {
        finish_failed(&job, HostFailureKind::Cancelled, false);
        return;
    }
    if job
        .spec
        .deadline_millis
        .is_some_and(|deadline| wall_clock_millis() >= deadline)
    {
        finish_failed(&job, HostFailureKind::TimedOut, false);
        return;
    }

    match launcher {
        ExecutionLauncher::Supervisor {
            executable,
            arguments,
            test_helper,
        } => run_supervised_execution(job, &executable, &arguments, test_helper),
        ExecutionLauncher::Unavailable => {
            finish_failed(&job, HostFailureKind::Unavailable, false);
        }
    }
}

fn run_supervised_execution(
    job: Arc<ExecutionJob>,
    executable: &Path,
    supervisor_arguments: &[String],
    test_helper: bool,
) {
    let bootstrap = ExecutionSupervisorBootstrap {
        live_environment: job.spec.live_environment.clone(),
        expected_parent_pid: std::process::id(),
        program: job.spec.program.clone(),
        arguments: job.spec.arguments.clone(),
        environment: job.spec.environment.clone(),
        working_directory: job.spec.working_directory.clone(),
    };
    let mut encoded = match serde_json::to_vec(&bootstrap) {
        Ok(encoded) if encoded.len() <= MAXIMUM_SUPERVISOR_BOOTSTRAP_BYTES => encoded,
        Ok(mut encoded) => {
            encoded.fill(0);
            finish_failed(&job, HostFailureKind::CapacityExceeded, false);
            return;
        }
        Err(_) => {
            finish_failed(&job, HostFailureKind::CapacityExceeded, false);
            return;
        }
    };
    let mut command = Command::new(executable);
    command
        .args(supervisor_arguments)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if test_helper {
        command.env("UNDERCURRENT_EXECUTION_SUPERVISOR_TEST", "1");
    }
    let mut supervisor = match spawn_supervisor(&mut command) {
        Ok(child) => child,
        Err(_) => {
            finish_failed(&job, HostFailureKind::Unavailable, false);
            return;
        }
    };
    #[cfg(windows)]
    {
        *job.supervisor_job.lock().unwrap_or_else(|p| p.into_inner()) = Some(supervisor.control());
    }
    job.supervisor_pid
        .store(u64::from(supervisor.id()), Ordering::Release);
    if job.cancelled.load(Ordering::Acquire) {
        signal_supervisor(&job);
    }
    let bootstrap_result = supervisor
        .stdin
        .take()
        .ok_or_else(|| io::Error::other("supervisor stdin was not piped"))
        .and_then(|mut stdin| write_supervisor_bootstrap(&mut stdin, &encoded));
    encoded.fill(0);
    if bootstrap_result.is_err() {
        terminate_supervisor(&job, &mut supervisor);
        finish_failed(&job, HostFailureKind::Unavailable, false);
        return;
    }
    let Some(mut stdout) = supervisor.stdout.take() else {
        terminate_supervisor(&job, &mut supervisor);
        finish_failed(&job, HostFailureKind::Unavailable, false);
        return;
    };
    let Some(stderr) = supervisor.stderr.take() else {
        terminate_supervisor(&job, &mut supervisor);
        finish_failed(&job, HostFailureKind::Unavailable, false);
        return;
    };
    let (status_marker, identity) = match read_supervisor_status(&mut stdout) {
        Ok(status) => status,
        Err(_) => {
            let status = supervisor.wait().ok();
            job.supervisor_pid.store(0, Ordering::Release);
            finish_pre_ready(&job, status);
            return;
        }
    };
    match status_marker {
        SUPERVISOR_SPAWN_FAILED => {
            let _ = supervisor.wait();
            job.supervisor_pid.store(0, Ordering::Release);
            finish_failed(&job, HostFailureKind::Unavailable, false);
            return;
        }
        SUPERVISOR_STARTED_FAILED => {
            let _ = supervisor.wait();
            job.supervisor_pid.store(0, Ordering::Release);
            finish_failed(&job, HostFailureKind::Unavailable, true);
            return;
        }
        SUPERVISOR_READY => {}
        _ => {
            terminate_supervisor(&job, &mut supervisor);
            finish_failed(&job, HostFailureKind::Unavailable, false);
            return;
        }
    }
    let Some((workload_pid, start_ticks)) = identity else {
        terminate_supervisor(&job, &mut supervisor);
        finish_failed(&job, HostFailureKind::Unavailable, true);
        return;
    };
    if workload_pid == 0 || start_ticks == 0 {
        terminate_supervisor(&job, &mut supervisor);
        finish_failed(&job, HostFailureKind::Unavailable, true);
        return;
    }
    job.workload_pid
        .store(u64::from(workload_pid), Ordering::Release);
    job.workload_start_ticks
        .store(start_ticks, Ordering::Release);

    let remaining = Arc::new(AtomicU64::new(job.spec.output_bytes));
    let output_exceeded = Arc::new(AtomicBool::new(false));
    let output_complete = Arc::new(AtomicBool::new(false));
    let stdout_worker = spawn_reader(
        SupervisorOutputReader {
            reader: stdout,
            remaining: 0,
            complete: output_complete.clone(),
        },
        Arc::clone(&remaining),
        Arc::clone(&output_exceeded),
    );
    let stderr_worker = spawn_reader(stderr, Arc::clone(&remaining), Arc::clone(&output_exceeded));
    let (stdout_worker, stderr_worker) = match (stdout_worker, stderr_worker) {
        (Ok(stdout_worker), Ok(stderr_worker)) => (stdout_worker, stderr_worker),
        _ => {
            terminate_supervisor(&job, &mut supervisor);
            finish_failed(&job, HostFailureKind::Unavailable, true);
            return;
        }
    };

    let mut terminal_kind = HostExecutionTerminalKind::Completed;
    let status = loop {
        if job.cancelled.load(Ordering::Acquire) {
            terminal_kind = HostExecutionTerminalKind::Cancelled;
            terminate_supervisor(&job, &mut supervisor);
            break supervisor.wait().ok();
        }
        if job
            .spec
            .deadline_millis
            .is_some_and(|deadline| wall_clock_millis() >= deadline)
        {
            terminal_kind = HostExecutionTerminalKind::DeadlineExceeded;
            terminate_supervisor(&job, &mut supervisor);
            break supervisor.wait().ok();
        }
        if output_exceeded.load(Ordering::Acquire) {
            terminal_kind = HostExecutionTerminalKind::OutputLimitExceeded;
            terminate_supervisor(&job, &mut supervisor);
            break supervisor.wait().ok();
        }
        match supervisor.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) => thread::sleep(Duration::from_millis(5)),
            Err(_) => {
                terminate_supervisor(&job, &mut supervisor);
                break None;
            }
        }
    };
    job.supervisor_pid.store(0, Ordering::Release);
    job.workload_pid.store(0, Ordering::Release);
    job.workload_start_ticks.store(0, Ordering::Release);
    let stdout = join_capture(stdout_worker);
    let stderr = join_capture(stderr_worker);
    if status.is_none()
        || stdout.failed
        || stderr.failed
        || (terminal_kind == HostExecutionTerminalKind::Completed
            && !output_complete.load(Ordering::Acquire))
    {
        finish_failed(&job, HostFailureKind::Unavailable, true);
        return;
    }
    finish_terminal(
        &job,
        ExecutionResult {
            kind: terminal_kind,
            exit_code: if terminal_kind == HostExecutionTerminalKind::Completed {
                status.and_then(exit_code)
            } else {
                None
            },
            stdout: stdout.bytes,
            stdout_source_bytes: stdout.source_bytes,
            stderr: stderr.bytes,
            stderr_source_bytes: stderr.source_bytes,
        },
    );
}

fn finish_pre_ready(job: &ExecutionJob, _status: Option<ExitStatus>) {
    if job.cancelled.load(Ordering::Acquire) {
        finish_failed(job, HostFailureKind::Cancelled, true);
    } else if job
        .spec
        .deadline_millis
        .is_some_and(|deadline| wall_clock_millis() >= deadline)
    {
        finish_failed(job, HostFailureKind::TimedOut, true);
    } else {
        finish_failed(job, HostFailureKind::Unavailable, true);
    }
}

fn write_supervisor_bootstrap(mut writer: impl Write, encoded: &[u8]) -> io::Result<()> {
    let length = u64::try_from(encoded.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "bootstrap length overflow"))?;
    writer.write_all(&length.to_be_bytes())?;
    writer.write_all(encoded)?;
    writer.flush()
}

fn read_supervisor_bootstrap(mut reader: impl Read) -> io::Result<ExecutionSupervisorBootstrap> {
    let mut length = [0u8; 8];
    reader.read_exact(&mut length)?;
    let length = usize::try_from(u64::from_be_bytes(length)).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "supervisor bootstrap is too large",
        )
    })?;
    if length == 0 || length > MAXIMUM_SUPERVISOR_BOOTSTRAP_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "supervisor bootstrap is outside its byte limit",
        ));
    }
    let mut encoded = vec![0u8; length];
    if let Err(error) = reader.read_exact(&mut encoded) {
        encoded.fill(0);
        return Err(error);
    }
    let result = serde_json::from_slice(&encoded)
        .map_err(|_| io::Error::other("invalid supervisor bootstrap"));
    encoded.fill(0);
    result
}

fn read_supervisor_status(mut reader: impl Read) -> io::Result<(u8, Option<(u32, u64)>)> {
    let mut matched = 0usize;
    for _ in 0..MAXIMUM_SUPERVISOR_PREFIX_BYTES {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte)?;
        if byte[0] == SUPERVISOR_STATUS_MAGIC[matched] {
            matched += 1;
            if matched == SUPERVISOR_STATUS_MAGIC.len() {
                break;
            }
        } else {
            matched = usize::from(byte[0] == SUPERVISOR_STATUS_MAGIC[0]);
        }
    }
    if matched != SUPERVISOR_STATUS_MAGIC.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "supervisor readiness prefix was not found",
        ));
    }
    let mut status = [0u8; 1];
    reader.read_exact(&mut status)?;
    if status[0] != SUPERVISOR_READY {
        return Ok((status[0], None));
    }
    let mut identity = [0u8; SUPERVISOR_IDENTITY_BYTES];
    reader.read_exact(&mut identity)?;
    let pid = u32::from_be_bytes(identity[..4].try_into().unwrap_or([0; 4]));
    let start_ticks = u64::from_be_bytes(identity[4..].try_into().unwrap_or([0; 8]));
    Ok((status[0], Some((pid, start_ticks))))
}

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
fn write_supervisor_status(status: u8, identity: Option<(u32, u64)>) -> io::Result<()> {
    let mut stdout = io::stdout().lock();
    stdout.write_all(SUPERVISOR_STATUS_MAGIC)?;
    stdout.write_all(&[status])?;
    if let Some((pid, start_ticks)) = identity {
        stdout.write_all(&pid.to_be_bytes())?;
        stdout.write_all(&start_ticks.to_be_bytes())?;
    }
    stdout.flush()
}

#[cfg(windows)]
fn relay_output(
    mut reader: impl Read + Send + 'static,
    mut writer: impl Write + Send + 'static,
) -> io::Result<thread::JoinHandle<io::Result<()>>> {
    thread::Builder::new()
        .name("undercurrent-execution-supervisor-output".to_owned())
        .spawn(move || io::copy(&mut reader, &mut writer).map(|_| ()))
}

// Child output cannot forge the completion marker: relays frame each data chunk,
// and only the supervisor writes a zero-length frame after both drains succeed.
struct FramedSupervisorOutput;
impl Write for FramedSupervisorOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut stdout = io::stdout().lock();
        for chunk in bytes.chunks(SUPERVISOR_OUTPUT_CHUNK) {
            stdout.write_all(&(chunk.len() as u32).to_be_bytes())?;
            stdout.write_all(chunk)?;
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        io::stdout().flush()
    }
}
fn finish_supervisor_output() -> io::Result<()> {
    let mut stdout = io::stdout().lock();
    stdout.write_all(&0u32.to_be_bytes())?;
    stdout.flush()
}
struct SupervisorOutputReader<R> {
    reader: R,
    remaining: usize,
    complete: Arc<AtomicBool>,
}
impl<R: Read> Read for SupervisorOutputReader<R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if bytes.is_empty() || self.complete.load(Ordering::Acquire) {
            return Ok(0);
        }
        if self.remaining == 0 {
            let mut header = [0; 4];
            match self.reader.read_exact(&mut header) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(0),
                Err(error) => return Err(error),
            }
            self.remaining = u32::from_be_bytes(header) as usize;
            if self.remaining == 0 {
                self.complete.store(true, Ordering::Release);
                return Ok(0);
            }
            if self.remaining > SUPERVISOR_OUTPUT_CHUNK {
                return Err(io::Error::other("invalid supervisor output frame"));
            }
        }
        let length = bytes.len().min(self.remaining);
        let read = self.reader.read(&mut bytes[..length])?;
        self.remaining -= read;
        Ok(read)
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn relay_unix_output(
    mut reader: impl Read + std::os::fd::AsFd + Send + 'static,
    mut writer: impl Write + Send + 'static,
    stopped: Arc<AtomicBool>,
) -> io::Result<thread::JoinHandle<io::Result<()>>> {
    use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
    fcntl_setfl(&reader, fcntl_getfl(&reader)? | OFlags::NONBLOCK)?;
    thread::Builder::new()
        .name("undercurrent-execution-supervisor-output".into())
        .spawn(move || {
            let mut buffer = [0; SUPERVISOR_OUTPUT_CHUNK];
            loop {
                if stopped.load(Ordering::Acquire) {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "workload output drain incomplete",
                    ));
                }
                match reader.read(&mut buffer) {
                    Ok(0) => return Ok(()),
                    Ok(read) => writer.write_all(&buffer[..read])?,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5))
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(error) => return Err(error),
                }
            }
        })
}

fn spawn_reader(
    reader: impl Read + Send + 'static,
    remaining: Arc<AtomicU64>,
    exceeded: Arc<AtomicBool>,
) -> std::io::Result<thread::JoinHandle<CapturedOutput>> {
    thread::Builder::new()
        .name("undercurrent-host-execution-output".to_owned())
        .spawn(move || capture_output(reader, &remaining, &exceeded))
}

fn capture_output(
    mut reader: impl Read,
    remaining: &AtomicU64,
    exceeded: &AtomicBool,
) -> CapturedOutput {
    let mut bytes = Vec::new();
    let mut source_bytes = 0u64;
    let mut buffer = [0u8; 8192];
    loop {
        let read = match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => read,
            Err(_) => {
                return CapturedOutput {
                    bytes,
                    source_bytes,
                    failed: true,
                };
            }
        };
        source_bytes = source_bytes.saturating_add(read as u64);
        let admitted = reserve_bytes(remaining, read as u64) as usize;
        bytes.extend_from_slice(&buffer[..admitted]);
        if admitted < read {
            exceeded.store(true, Ordering::Release);
        }
    }
    CapturedOutput {
        bytes,
        source_bytes,
        failed: false,
    }
}

fn reserve_bytes(remaining: &AtomicU64, requested: u64) -> u64 {
    let mut current = remaining.load(Ordering::Acquire);
    loop {
        let admitted = current.min(requested);
        match remaining.compare_exchange_weak(
            current,
            current - admitted,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return admitted,
            Err(next) => current = next,
        }
    }
}

fn join_capture(worker: thread::JoinHandle<CapturedOutput>) -> CapturedOutput {
    worker.join().unwrap_or(CapturedOutput {
        bytes: Vec::new(),
        source_bytes: 0,
        failed: true,
    })
}

#[cfg(unix)]
fn terminate_process_group(child: &mut Child) {
    if let Ok(pid) = i32::try_from(child.id()) {
        let _ = kill(Pid::from_raw(-pid), Signal::SIGKILL);
    }
    let _ = child.kill();
}

#[cfg(unix)]
fn signal_supervisor(job: &ExecutionJob) {
    let raw_pid = job.supervisor_pid.load(Ordering::Acquire);
    if let Ok(pid) = i32::try_from(raw_pid)
        && pid > 0
    {
        let _ = kill(Pid::from_raw(pid), Signal::SIGTERM);
    }
}

fn terminate_supervisor(job: &ExecutionJob, supervisor: &mut Child) {
    signal_supervisor(job);
    if job.supervisor_pid.load(Ordering::Acquire) == 0 {
        let _ = supervisor.kill();
    }
}

#[cfg(unix)]
fn force_exact_job_cleanup(job: &ExecutionJob) {
    let raw_pid = job.workload_pid.load(Ordering::Acquire);
    let start_ticks = job.workload_start_ticks.load(Ordering::Acquire);
    if let Ok(pid) = u32::try_from(raw_pid)
        && pid > 0
        && start_ticks > 0
        && execution_process_identity_matches(pid, start_ticks)
        && let Ok(pid) = i32::try_from(pid)
    {
        let _ = kill(Pid::from_raw(-pid), Signal::SIGKILL);
    }
    let supervisor = job.supervisor_pid.load(Ordering::Acquire);
    if let Ok(pid) = i32::try_from(supervisor)
        && pid > 0
    {
        let _ = kill(Pid::from_raw(pid), Signal::SIGKILL);
    }
}

#[cfg(target_os = "linux")]
fn execution_process_start(pid: u32) -> io::Result<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let fields = stat
        .rsplit_once(')')
        .map(|(_, fields)| fields)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid process stat"))?;
    fields
        .split_whitespace()
        .nth(19)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing process start time"))?
        .parse()
        .map_err(|source| io::Error::new(io::ErrorKind::InvalidData, source))
}

#[cfg(target_os = "macos")]
fn execution_process_start(pid: u32) -> io::Result<u64> {
    undercurrent_platform_macos::identity(pid).map(|info| info.start_micros)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn execution_process_start(_pid: u32) -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "process incarnation verification requires Linux or macOS",
    ))
}

#[cfg(unix)]
fn execution_process_identity_matches(pid: u32, expected_start_ticks: u64) -> bool {
    execution_process_start(pid).ok() == Some(expected_start_ticks)
}

#[cfg(target_os = "linux")]
fn errno_io(source: nix::errno::Errno) -> io::Error {
    io::Error::from_raw_os_error(source as i32)
}

fn exit_code(status: ExitStatus) -> Option<i32> {
    status.code()
}

fn finish_terminal(job: &ExecutionJob, result: ExecutionResult) {
    let state = ExecutionState::Terminal(result);
    let state = if job.journal.persist(&job.spec, &state).is_ok() {
        state
    } else {
        ExecutionState::Failed {
            kind: HostFailureKind::Unavailable,
            effect_started: true,
        }
    };
    *job.state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = state;
    job.changed.notify_all();
}

fn finish_failed(job: &ExecutionJob, kind: HostFailureKind, effect_started: bool) {
    let state = ExecutionState::Failed {
        kind,
        effect_started,
    };
    let state = if job.journal.persist(&job.spec, &state).is_ok() {
        state
    } else {
        ExecutionState::Failed {
            kind: HostFailureKind::Unavailable,
            effect_started: true,
        }
    };
    *job.state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = state;
    job.changed.notify_all();
}

fn validate_job_id(job_id: &str, context: HostFailureContext) -> Result<(), HostFailure> {
    if job_id.is_empty()
        || job_id.len() > MAXIMUM_JOB_ID_BYTES
        || !job_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        Err(failure(HostFailureKind::InvalidData, context))
    } else {
        Ok(())
    }
}

fn wall_clock_millis() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

fn failure(kind: HostFailureKind, context: HostFailureContext) -> HostFailure {
    HostFailure { kind, context }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn live_environment_preserves_non_utf8_values() {
        let registry = registry(1, 4096);
        let mut request = spec(
            "live-bytes",
            vec!["-c", "printf '%s' \"$CHECK\""],
            4096,
            None,
        );
        request.live_environment = Some(
            crate::process_host_protocol::HostResolvedSecretEnvironment::from_values(vec![(
                "CHECK".to_owned(),
                vec![0xff, b'\n'],
            )]),
        );
        registry.start(request).unwrap();
        assert!(
            matches!(terminal(&registry, "live-bytes"), HostResponseValue::ExecutionTerminal {
            exit_code: Some(0), stdout, ..
        } if stdout == vec![0xff, b'\n'])
        );
    }

    #[test]
    fn live_exports_reach_child_but_not_journal_or_debug_and_retry_does_not_respawn() {
        use crate::process_host_protocol::HostResolvedSecretEnvironment;
        let directory = tempfile::tempdir().unwrap();
        let journal = directory.path().join("jobs");
        let registry = registry_at(journal.clone(), 2, 4096);
        let marker = "live-private-marker-8a0b";
        let mut request = spec(
            "live-env",
            vec!["-c", "test -n \"$CHECK\" && test -z \"${REMOVED+x}\""],
            4096,
            None,
        );
        request.environment = vec![("REMOVED".into(), "launch".into())];
        request.live_environment = Some(HostResolvedSecretEnvironment::from_values(vec![(
            "CHECK".into(),
            marker.as_bytes().to_vec(),
        )]));
        assert!(!format!("{request:?}").contains(marker));
        registry.start(request.clone()).unwrap();
        assert!(matches!(
            terminal(&registry, "live-env"),
            HostResponseValue::ExecutionTerminal {
                exit_code: Some(0),
                ..
            }
        ));
        for entry in fs::read_dir(&journal).unwrap() {
            let bytes = fs::read(entry.unwrap().path()).unwrap();
            assert!(!bytes.windows(marker.len()).any(|w| w == marker.as_bytes()));
        }
        assert!(
            registry
                .replay("live-env", "session-1", &request.payload_digest)
                .unwrap()
                .is_ok()
        );
        assert!(
            registry
                .replay("live-env", "session-1", "different")
                .unwrap()
                .is_err()
        );
        drop(registry);
        let recovered = registry_at(journal, 2, 4096);
        assert!(
            recovered
                .replay("live-env", "session-1", &request.payload_digest)
                .unwrap()
                .is_ok()
        );
        assert!(matches!(
            terminal(&recovered, "live-env"),
            HostResponseValue::ExecutionTerminal {
                exit_code: Some(0),
                ..
            }
        ));
    }

    #[test]
    fn execution_supervisor_subprocess() {
        if std::env::var("UNDERCURRENT_EXECUTION_SUPERVISOR_TEST").as_deref() != Ok("1") {
            return;
        }
        let stdin = io::stdin();
        match run_execution_supervisor(stdin.lock()) {
            Ok(outcome) => outcome.terminate(),
            Err(_) => std::process::exit(1),
        }
    }

    #[test]
    fn execution_host_parent_subprocess() {
        if std::env::var("UNDERCURRENT_EXECUTION_HOST_PARENT_TEST").as_deref() != Ok("1") {
            return;
        }
        let report_path =
            std::env::var("UNDERCURRENT_EXECUTION_HOST_REPORT").expect("host report path");
        let journal_path =
            std::env::var("UNDERCURRENT_EXECUTION_HOST_JOURNAL").expect("host journal path");
        let registry = registry_at(PathBuf::from(journal_path), 1, 4096);
        let mut request = host_death_spec(&report_path);
        if std::env::var("UNDERCURRENT_EXECUTION_ROOT_EXIT").as_deref() == Ok("1") {
            request.arguments[1] = request.arguments[1].replace("; wait", "; exit 7");
        }
        registry.start(request).expect("start host-death workload");
        loop {
            if !matches!(
                registry
                    .poll("host-death", Duration::from_millis(25))
                    .expect("poll host-death workload"),
                HostResponseValue::ExecutionRunning
            ) {
                std::process::exit(2);
            }
        }
    }

    #[test]
    fn terminal_result_is_binary_exact_and_retained_until_acknowledged() {
        let registry = registry(2, 4096);
        registry
            .start(spec(
                "binary",
                vec!["-c", "printf '\\001\\002'; printf '\\376\\377' >&2; exit 7"],
                4096,
                None,
            ))
            .expect("start binary job");
        let first = terminal(&registry, "binary");
        let second = terminal(&registry, "binary");
        for response in [first, second] {
            assert!(matches!(
                response,
                HostResponseValue::ExecutionTerminal {
                    kind: HostExecutionTerminalKind::Completed,
                    exit_code: Some(7),
                    stdout,
                    stderr,
                    stdout_source_bytes: 2,
                    stderr_source_bytes: 2,
                } if stdout == [1, 2] && stderr == [0xfe, 0xff]
            ));
        }
        registry.acknowledge("binary").expect("acknowledge job");
        assert!(registry.poll("binary", Duration::ZERO).is_err());
    }

    #[test]
    fn one_shot_execution_passes_shell_metacharacters_as_literal_argv() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let side_effect = directory.path().join("shell-side-effect");
        let redirected = directory.path().join("shell-redirection");
        let literal = format!(
            "literal;$HOME|$(touch {})&*?[x]>{}",
            side_effect.display(),
            redirected.display()
        );
        let registry = registry(1, 4_096);
        let mut command = spec("literal-argv", Vec::new(), 4_096, None);
        command.program = "/usr/bin/printf".to_owned();
        command.arguments = vec!["%s".to_owned(), literal.clone()];
        registry.start(command).expect("start literal argv job");

        assert!(matches!(
            terminal(&registry, "literal-argv"),
            HostResponseValue::ExecutionTerminal {
                kind: HostExecutionTerminalKind::Completed,
                exit_code: Some(0),
                stdout,
                stderr,
                ..
            } if stdout == literal.as_bytes() && stderr.is_empty()
        ));
        assert!(!side_effect.exists());
        assert!(!redirected.exists());
    }

    #[test]
    fn cancellation_and_deadline_kill_the_complete_process_group() {
        let directory = tempfile::tempdir().expect("temporary process report directory");
        let report = directory.path().join("cancel-processes");
        let registry = registry(2, 4096);
        let mut cancellation = host_death_spec(report.to_str().expect("process report path"));
        cancellation.job_id = "cancel".to_owned();
        registry.start(cancellation).expect("start cancellable job");
        // Wait for a real descendant and retain its exact incarnation before
        // cancellation. Terminal stdout is not a child-startup handshake.
        let (_, child_pid) = wait_for_process_report(&report);
        let child_start = execution_process_start(child_pid).expect("live descendant identity");
        registry.cancel("cancel").expect("cancel job");
        assert!(matches!(
            terminal(&registry, "cancel"),
            HostResponseValue::ExecutionTerminal {
                kind: HostExecutionTerminalKind::Cancelled,
                ..
            }
        ));
        wait_until_identity_gone(child_pid, child_start);

        registry
            .start(spec(
                "deadline",
                vec!["-c", "sleep 30"],
                4096,
                Some(wall_clock_millis().saturating_add(40)),
            ))
            .expect("start deadline job");
        assert!(matches!(
            terminal(&registry, "deadline"),
            HostResponseValue::ExecutionTerminal {
                kind: HostExecutionTerminalKind::DeadlineExceeded,
                ..
            }
        ));
    }

    fn root_has_exited(pid: u32) -> bool {
        let status = Command::new("/bin/ps")
            .args(["-o", "stat=", "-p", &pid.to_string()])
            .output()
            .unwrap();
        let state = String::from_utf8(status.stdout).unwrap();
        state.trim().is_empty() || state.trim().starts_with('Z')
    }

    #[test]
    fn execution_escaped_writer_subprocess() {
        if std::env::var("UNDERCURRENT_ESCAPED_WRITER").as_deref() != Ok("1") {
            return;
        }
        nix::unistd::setsid().unwrap();
        let report = std::env::var_os("UNDERCURRENT_ESCAPE_REPORT").unwrap();
        let root = std::env::var("UNDERCURRENT_ESCAPE_ROOT").unwrap();
        fs::write(report, format!("{root} {}\n", std::process::id())).unwrap();
        thread::sleep(Duration::from_secs(30));
        std::process::exit(0);
    }

    #[test]
    fn escaped_pipe_has_bounded_drain_and_keeps_cancellation_and_deadline_active() {
        for mode in ["drain", "cancel", "deadline"] {
            let directory = tempfile::tempdir().unwrap();
            let report = directory.path().join("processes");
            let registry = registry(1, 4096);
            let script = r#"UNDERCURRENT_ESCAPED_WRITER=1 UNDERCURRENT_ESCAPE_REPORT="$1" UNDERCURRENT_ESCAPE_ROOT="$$" "$2" --exact process::execute_host::tests::execution_escaped_writer_subprocess --nocapture & while [ ! -s "$1" ]; do sleep 0.005; done; exit 7"#;
            let helper = std::env::current_exe().unwrap();
            registry
                .start(spec(
                    "escaped",
                    vec![
                        "-c",
                        script,
                        "sh",
                        report.to_str().unwrap(),
                        helper.to_str().unwrap(),
                    ],
                    4096,
                    (mode == "deadline").then(|| wall_clock_millis().saturating_add(500)),
                ))
                .unwrap();
            let (root, escaped) = wait_for_process_report(&report);
            let escaped_start = execution_process_start(escaped).unwrap();
            for _ in 0..100 {
                if root_has_exited(root) {
                    break;
                }
                thread::sleep(Duration::from_millis(5));
            }
            assert!(root_has_exited(root));
            if mode == "cancel" {
                registry.cancel("escaped").unwrap();
            }
            let started = Instant::now();
            let result = terminal(&registry, "escaped");
            // Escaped sessions are outside workload ownership. Explicitly clean
            // this test fixture only after verifying its retained incarnation.
            if execution_process_identity_matches(escaped, escaped_start) {
                let _ = kill(Pid::from_raw(-(escaped as i32)), Signal::SIGKILL);
            }
            assert!(started.elapsed() < Duration::from_secs(3));
            match mode {
                "drain" => assert!(matches!(
                    result,
                    HostResponseValue::ExecutionFailed {
                        effect_started: true,
                        ..
                    }
                )),
                "cancel" => assert!(matches!(
                    result,
                    HostResponseValue::ExecutionTerminal {
                        kind: HostExecutionTerminalKind::Cancelled,
                        ..
                    }
                )),
                _ => assert!(matches!(
                    result,
                    HostResponseValue::ExecutionTerminal {
                        kind: HostExecutionTerminalKind::DeadlineExceeded,
                        ..
                    }
                )),
            }
        }
    }

    #[test]
    fn ordinary_root_exit_retires_owned_descendants_and_preserves_buffered_output() {
        let registry = registry(1, 4096);
        registry
            .start(spec(
                "root-exit",
                vec!["-c", "sleep 30 & printf before; exit 7"],
                4096,
                None,
            ))
            .unwrap();
        let started = Instant::now();
        assert!(
            matches!(terminal(&registry, "root-exit"), HostResponseValue::ExecutionTerminal {
            kind: HostExecutionTerminalKind::Completed, exit_code: Some(7), stdout, .. } if stdout == b"before")
        );
        assert!(started.elapsed() < Duration::from_secs(3));
    }

    #[test]
    fn output_overflow_terminates_instead_of_allocating_past_the_bound() {
        let registry = registry(1, 64);
        registry
            .start(spec(
                "overflow",
                vec!["-c", "while :; do printf 0123456789abcdef; done"],
                64,
                None,
            ))
            .expect("start overflow job");
        assert!(matches!(
            terminal(&registry, "overflow"),
            HostResponseValue::ExecutionTerminal {
                kind: HostExecutionTerminalKind::OutputLimitExceeded,
                stdout,
                stdout_source_bytes,
                ..
            } if stdout.len() <= 64 && stdout_source_bytes > stdout.len() as u64
        ));
    }

    #[test]
    fn retained_jobs_enforce_capacity_and_semantic_idempotency() {
        let registry = registry(2, 4096);
        let first = spec("first", vec!["-c", "exit 0"], 4096, None);
        registry.start(first.clone()).expect("first job");
        registry.start(first).expect("duplicate first job");
        registry
            .start(spec("second", vec!["-c", "exit 0"], 4096, None))
            .expect("second job");
        let _ = terminal(&registry, "first");
        let _ = terminal(&registry, "second");
        assert!(
            registry
                .start(spec("third", vec!["-c", "exit 0"], 4096, None))
                .is_err()
        );
        registry.acknowledge("first").expect("retire first");
        registry
            .start(spec("third", vec!["-c", "exit 0"], 4096, None))
            .expect("capacity released");
    }

    #[test]
    fn terminal_job_replays_after_registry_restart_until_durable_acknowledgement() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let journal = directory.path().join("jobs");
        let value = spec(
            "restart-terminal",
            vec!["-c", "printf persisted; exit 9"],
            4096,
            None,
        );
        let first = registry_at(journal.clone(), 1, 4096);
        first.start(value.clone()).expect("start terminal job");
        let _ = terminal(&first, "restart-terminal");
        drop(first);

        let second = registry_at(journal.clone(), 1, 4096);
        assert!(matches!(
            second.start(value).expect("classify retained terminal"),
            HostResponseValue::ExecutionAccepted
        ));
        assert!(matches!(
            second
                .poll("restart-terminal", Duration::ZERO)
                .expect("replay retained terminal"),
            HostResponseValue::ExecutionTerminal {
                kind: HostExecutionTerminalKind::Completed,
                exit_code: Some(9),
                stdout,
                ..
            } if stdout == b"persisted"
        ));
        let directory_mode = fs::metadata(&journal)
            .expect("journal metadata")
            .permissions()
            .mode()
            & 0o777;
        let record_mode = fs::metadata(journal.join("job-restart-terminal.json"))
            .expect("record metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(directory_mode, 0o700);
        assert_eq!(record_mode, 0o600);
        second
            .acknowledge("restart-terminal")
            .expect("acknowledge durable settlement");
        assert!(!journal.join("job-restart-terminal.json").exists());
    }

    #[test]
    fn future_or_corrupt_execution_journal_fails_closed() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let journal = directory.path().join("jobs");
        let registry = registry_at(journal.clone(), 1, 4096);
        registry
            .start(spec("incompatible", vec!["-c", "exit 0"], 4096, None))
            .expect("start journal fixture");
        let _ = terminal(&registry, "incompatible");
        drop(registry);
        let record = journal.join("job-incompatible.json");
        let encoded = fs::read_to_string(&record).expect("read journal record");
        fs::write(
            &record,
            encoded.replacen("\"version\":\"0.1.0-beta\"", "\"version\":\"99.0.0\"", 1),
        )
        .expect("write future record");
        assert!(registry_result_at(journal.clone(), 1, 4096).is_err());
        fs::write(&record, b"{").expect("write corrupt record");
        assert!(registry_result_at(journal, 1, 4096).is_err());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn process_identity_rejects_pid_reuse_before_group_signalling() {
        let pid = std::process::id();
        let start_ticks = execution_process_start(pid).expect("current process start ticks");
        assert!(execution_process_identity_matches(pid, start_ticks));
        assert!(!execution_process_identity_matches(
            pid,
            start_ticks.saturating_add(1)
        ));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn host_sigkill_cleans_exact_workload_process_group_and_descendant() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let report = directory.path().join("processes");
        let journal = report.with_extension("jobs");
        let mut host = spawn_host_parent(&report);
        let (workload_pid, descendant_pid) = wait_for_process_report(&report);
        let workload_start = execution_process_start(workload_pid).expect("workload start ticks");
        let descendant_start =
            execution_process_start(descendant_pid).expect("descendant start ticks");

        kill(
            Pid::from_raw(i32::try_from(host.id()).expect("host PID")),
            Signal::SIGKILL,
        )
        .expect("kill host parent");
        let _ = host.wait();
        wait_until_identity_gone(workload_pid, workload_start);
        wait_until_identity_gone(descendant_pid, descendant_start);

        let recovered = registry_at(journal, 1, 4096);
        assert!(matches!(
            recovered
                .start(host_death_spec(report.to_str().expect("report path text")))
                .expect("classify recovered job"),
            HostResponseValue::ExecutionAccepted
        ));
        assert!(matches!(
            recovered
                .poll("host-death", Duration::ZERO)
                .expect("observe recovered job"),
            HostResponseValue::ExecutionFailed {
                effect_started: true,
                ..
            }
        ));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn host_death_retires_descendant_after_root_exit() {
        let directory = tempfile::tempdir().unwrap();
        let report = directory.path().join("processes");
        let mut host = spawn_host_parent_with_root_exit(&report, true);
        let (root, descendant) = wait_for_process_report(&report);
        let descendant_start = execution_process_start(descendant).ok();
        for _ in 0..400 {
            if root_has_exited(root) {
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
        let root_exited = root_has_exited(root);
        host.kill().unwrap();
        host.wait().unwrap();
        for _ in 0..400 {
            if !descendant_start
                .is_some_and(|start| execution_process_identity_matches(descendant, start))
            {
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
        let survived = descendant_start
            .is_some_and(|start| execution_process_identity_matches(descendant, start));
        if survived {
            let _ = kill(Pid::from_raw(-(root as i32)), Signal::SIGKILL);
        }
        assert!(root_exited);
        assert!(!survived, "descendant survived host death after root exit");
    }

    #[test]
    fn finite_output_before_root_exit_drains_without_changing_root_status() {
        let registry = registry(1, 4096);
        registry
            .start(spec(
                "root-status",
                vec!["-c", "(printf tail) & wait; exit 7"],
                4096,
                None,
            ))
            .unwrap();
        assert!(matches!(terminal(&registry, "root-status"),
            HostResponseValue::ExecutionTerminal { exit_code: Some(7), stdout, .. }
            if stdout == b"tail"));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn parent_death_setup_race_never_leaves_a_reported_workload() {
        let directory = tempfile::tempdir().expect("temporary directory");
        for iteration in 0..8 {
            let report = directory.path().join(format!("race-{iteration}"));
            let mut host = spawn_host_parent(&report);
            kill(
                Pid::from_raw(i32::try_from(host.id()).expect("host PID")),
                Signal::SIGKILL,
            )
            .expect("kill racing host parent");
            let _ = host.wait();
            thread::sleep(Duration::from_millis(30));
            if report.exists() {
                let (workload_pid, descendant_pid) = read_process_report(&report);
                if let Ok(start_ticks) = execution_process_start(workload_pid) {
                    wait_until_identity_gone(workload_pid, start_ticks);
                }
                if let Ok(start_ticks) = execution_process_start(descendant_pid) {
                    wait_until_identity_gone(descendant_pid, start_ticks);
                }
            }
        }
    }

    fn registry(limit: usize, capture: usize) -> HostExecutionRegistry {
        let directory = tempfile::tempdir()
            .expect("temporary journal directory")
            .keep();
        registry_at(directory.join("jobs"), limit, capture)
    }

    fn registry_at(journal: PathBuf, limit: usize, capture: usize) -> HostExecutionRegistry {
        registry_result_at(journal, limit, capture).expect("execution registry")
    }

    fn registry_result_at(
        journal: PathBuf,
        limit: usize,
        capture: usize,
    ) -> io::Result<HostExecutionRegistry> {
        HostExecutionRegistry::new_with_launcher(
            ExecutionRegistryConfig {
                limit,
                command_bytes_limit: 4096,
                environment_entries_limit: 16,
                environment_bytes_limit: 4096,
                capture_bytes_limit: capture,
                worker_stack_bytes: 256 * 1024,
                journal_directory: journal,
            },
            ExecutionLauncher::Supervisor {
                executable: std::env::current_exe().expect("current test executable"),
                arguments: vec![
                    "--exact".to_owned(),
                    "process::execute_host::tests::execution_supervisor_subprocess".to_owned(),
                    "--nocapture".to_owned(),
                    "--test-threads=1".to_owned(),
                ],
                test_helper: true,
            },
        )
    }

    fn spawn_host_parent(report: &Path) -> Child {
        spawn_host_parent_with_root_exit(report, false)
    }

    fn spawn_host_parent_with_root_exit(report: &Path, exit_root: bool) -> Child {
        Command::new(std::env::current_exe().expect("current test executable"))
            .args([
                "--exact",
                "process::execute_host::tests::execution_host_parent_subprocess",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("UNDERCURRENT_EXECUTION_HOST_PARENT_TEST", "1")
            .env(
                "UNDERCURRENT_EXECUTION_ROOT_EXIT",
                if exit_root { "1" } else { "0" },
            )
            .env("UNDERCURRENT_EXECUTION_HOST_REPORT", report)
            .env(
                "UNDERCURRENT_EXECUTION_HOST_JOURNAL",
                report.with_extension("jobs"),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn host parent fixture")
    }

    fn host_death_spec(report_path: &str) -> HostExecutionSpec {
        HostExecutionSpec {
            live_environment: None,
            job_id: "host-death".to_owned(),
            session_id: "session-1".to_owned(),
            payload_digest: "ab".repeat(32),
            program: "/bin/sh".to_owned(),
            arguments: vec![
                "-c".to_owned(),
                "sleep 30 & descendant=$!; printf '%s %s\\n' \"$$\" \"$descendant\" > \"$1\"; wait"
                    .to_owned(),
                "sh".to_owned(),
                report_path.to_owned(),
            ],
            environment: Vec::new(),
            working_directory: Some("/tmp".to_owned()),
            output_bytes: 4096,
            deadline_millis: None,
        }
    }

    fn wait_for_process_report(path: &Path) -> (u32, u32) {
        for _ in 0..400 {
            if let Some(report) = try_read_process_report(path) {
                return report;
            }
            thread::sleep(Duration::from_millis(5));
        }
        panic!("workload process report did not appear")
    }

    fn read_process_report(path: &Path) -> (u32, u32) {
        try_read_process_report(path).expect("complete process report")
    }

    fn try_read_process_report(path: &Path) -> Option<(u32, u32)> {
        let report = std::fs::read_to_string(path).ok()?;
        let mut values = report.split_whitespace();
        let workload = values.next()?.parse().ok()?;
        let descendant = values.next()?.parse().ok()?;
        (values.next().is_none()).then_some((workload, descendant))
    }

    fn wait_until_identity_gone(pid: u32, start_ticks: u64) {
        for _ in 0..400 {
            if !execution_process_identity_matches(pid, start_ticks) {
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }
        panic!("exact process incarnation {pid} survived host death")
    }

    fn spec(
        id: &str,
        arguments: Vec<&str>,
        output_bytes: u64,
        deadline_millis: Option<i64>,
    ) -> HostExecutionSpec {
        HostExecutionSpec {
            live_environment: None,
            job_id: id.to_owned(),
            session_id: "session-1".to_owned(),
            payload_digest: "ab".repeat(32),
            program: "/bin/sh".to_owned(),
            arguments: arguments.into_iter().map(str::to_owned).collect(),
            environment: Vec::new(),
            working_directory: Some("/tmp".to_owned()),
            output_bytes,
            deadline_millis,
        }
    }

    fn terminal(registry: &HostExecutionRegistry, id: &str) -> HostResponseValue {
        for _ in 0..200 {
            let response = registry
                .poll(id, Duration::from_millis(25))
                .expect("poll job");
            if !matches!(response, HostResponseValue::ExecutionRunning) {
                return response;
            }
        }
        panic!("job did not terminate within test deadline")
    }
}

// Kernel registration precedes the parent PID recheck and workload spawn. A
// registration/query error fails closed; we never replace this with PID polling.
#[cfg(target_os = "macos")]
struct ParentExitWatch(nix::sys::event::Kqueue);

#[cfg(target_os = "macos")]
impl ParentExitWatch {
    fn new(parent: u32) -> io::Result<Self> {
        #[cfg(unix)]
        use nix::sys::event::{EvFlags, EventFilter, FilterFlag, KEvent, Kqueue};
        let queue = Kqueue::new()?;
        nix::fcntl::fcntl(
            &queue,
            nix::fcntl::FcntlArg::F_SETFD(nix::fcntl::FdFlag::FD_CLOEXEC),
        )?;
        let change = KEvent::new(
            parent as usize,
            EventFilter::EVFILT_PROC,
            EvFlags::EV_ADD | EvFlags::EV_ENABLE,
            FilterFlag::NOTE_EXIT,
            0,
            0,
        );
        queue.kevent(&[change], &mut [], None)?;
        Ok(Self(queue))
    }
    fn exited(&self) -> bool {
        #[cfg(unix)]
        use nix::sys::event::{EvFlags, EventFilter, FilterFlag, KEvent};
        let mut events = [KEvent::new(
            0,
            EventFilter::EVFILT_PROC,
            EvFlags::empty(),
            FilterFlag::empty(),
            0,
            0,
        )];
        !matches!(
            self.0.kevent(
                &[],
                &mut events,
                Some(nix::libc::timespec {
                    tv_sec: 0,
                    tv_nsec: 0
                })
            ),
            Ok(0)
        )
    }
}

#[cfg(all(test, windows))]
#[path = "windows_execution_tests.rs"]
mod windows_tests;
