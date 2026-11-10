// SPDX-License-Identifier: Apache-2.0

//! Durable private receipts for idempotent hosted process mutations.
//!
//! Admission persists `Running` before the host performs an effect. Applied
//! responses remain replayable until application settlement permits their ACK;
//! capacity never evicts an unresolved receipt. A missing record is unknown
//! effect evidence, including after acknowledgement.
//!
//! Host recovery downgrades running and applied-spawn records to unknown.
//! Shutdown cancellation uses a separate durable tombstone and acknowledgement
//! path, because normal shutdown acknowledgement retires the host itself.

use super::host_files;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::process_host_protocol::{
    HostAttemptContext, HostAttemptKind, HostEffectCertainty, HostFailure, HostResponseValue,
};

const JOURNAL_VERSION: undercurrent_foundation::ReleaseVersion =
    undercurrent_foundation::ReleaseVersion::Current;
const JOURNAL_FILE: &str = "receipts.json";
const TEMPORARY_FILE: &str = "receipts.tmp";
const BYTES_PER_RECEIPT: usize = 16 * 1024;

pub(super) struct HostAttemptReceiptRegistry {
    capacity: usize,
    journal: ReceiptJournal,
    records: Mutex<BTreeMap<String, ReceiptRecord>>,
    crash_hook: Arc<dyn ReceiptCrashHook>,
}

impl HostAttemptReceiptRegistry {
    pub(super) fn new(capacity: usize, path: PathBuf) -> Result<Self, ReceiptError> {
        Self::new_inner(capacity, path, Arc::new(NoReceiptCrash))
    }

    pub(super) fn new_for_host(capacity: usize, path: PathBuf) -> Result<Self, ReceiptError> {
        #[cfg(test)]
        if let Some(point) = process_test_crash_point()? {
            return Self::new_inner(capacity, path, Arc::new(AbortProcessAt(point)));
        }
        Self::new(capacity, path)
    }

    #[cfg(test)]
    fn new_with_hook(
        capacity: usize,
        path: PathBuf,
        crash_hook: Arc<dyn ReceiptCrashHook>,
    ) -> Result<Self, ReceiptError> {
        Self::new_inner(capacity, path, crash_hook)
    }

    fn new_inner(
        capacity: usize,
        path: PathBuf,
        crash_hook: Arc<dyn ReceiptCrashHook>,
    ) -> Result<Self, ReceiptError> {
        if capacity == 0 {
            return Err(ReceiptError::Invalid);
        }
        let journal = ReceiptJournal::new(path, capacity)?;
        let mut records = journal.load()?;
        let recovered_running = records.values_mut().fold(false, |changed, record| {
            if matches!(record.state, ReceiptState::Running)
                || (record.attempt.kind == HostAttemptKind::Spawn
                    && matches!(record.state, ReceiptState::Applied { .. }))
            {
                record.state = ReceiptState::Unknown;
                true
            } else {
                changed
            }
        });
        if records.len() > capacity {
            return Err(ReceiptError::Capacity);
        }
        if recovered_running {
            journal.commit(&records)?;
        }
        Ok(Self {
            capacity,
            journal,
            records: Mutex::new(records),
            crash_hook,
        })
    }

    #[cfg(test)]
    pub(super) fn begin(&self, attempt: &HostAttemptContext) -> Result<ReceiptBegin, ReceiptError> {
        self.begin_bound(attempt, None)
    }

    pub(super) fn begin_bound(
        &self,
        attempt: &HostAttemptContext,
        process_id: Option<&str>,
    ) -> Result<ReceiptBegin, ReceiptError> {
        if !valid_attempt(attempt) {
            return Err(ReceiptError::Invalid);
        }
        if process_id.is_some_and(|id| {
            !matches!(
                attempt.kind,
                HostAttemptKind::Stop | HostAttemptKind::ForceStop
            ) || undercurrent_domain::ProcessInstanceId::new(id.to_owned()).is_err()
        }) {
            return Err(ReceiptError::Invalid);
        }
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(record) = records.get(&attempt.attempt_id) {
            if record.attempt != *attempt {
                return Ok(ReceiptBegin::Conflict);
            }
            if record
                .process_id
                .as_deref()
                .is_some_and(|id| Some(id) != process_id)
            {
                return Ok(ReceiptBegin::Conflict);
            }
            return Ok(match &record.state {
                ReceiptState::Absent { failure } => ReceiptBegin::Applied {
                    response: HostResponseValue::AttemptFailed {
                        certainty: HostEffectCertainty::KnownAbsent,
                        failure: failure.clone(),
                    },
                },
                ReceiptState::CancelledShutdown => ReceiptBegin::KnownAbsent,
                ReceiptState::Running | ReceiptState::Unknown => ReceiptBegin::Unknown,
                ReceiptState::Applied { response, .. } => ReceiptBegin::Applied {
                    response: response.clone(),
                },
            });
        }
        if records.values().any(|record| {
            same_fencing_scope(&record.attempt, attempt)
                && record.attempt.attempt_id != attempt.attempt_id
                && record.attempt.fencing_token >= attempt.fencing_token
        }) {
            return Ok(ReceiptBegin::Conflict);
        }
        if records.len() >= self.capacity {
            return Err(ReceiptError::Capacity);
        }
        let prior = records.insert(
            attempt.attempt_id.clone(),
            ReceiptRecord {
                attempt: attempt.clone(),
                process_id: process_id.map(str::to_owned),
                state: ReceiptState::Running,
                failure: None,
            },
        );
        debug_assert!(prior.is_none());
        if let Err(error) = self.journal.commit(&records) {
            records.remove(&attempt.attempt_id);
            return Err(error);
        }
        self.inject(ReceiptCrashPoint::RunningCommitted)?;
        Ok(ReceiptBegin::Started)
    }

    pub(super) fn complete_applied(
        &self,
        attempt: &HostAttemptContext,
        result_digest: String,
        response: HostResponseValue,
    ) -> Result<(), ReceiptError> {
        if !valid_digest(&result_digest) || !valid_response(attempt.kind, &response) {
            return Err(ReceiptError::Invalid);
        }
        self.inject(ReceiptCrashPoint::EffectCompleted)?;
        #[cfg(test)]
        if std::env::var("UNDERCURRENT_RUNTIME_TEST_RECEIPT_MODE").as_deref()
            == Ok("receipt_commit_capacity")
        {
            return Err(ReceiptError::Capacity);
        }
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(record) = records.get_mut(&attempt.attempt_id) else {
            return Err(ReceiptError::Conflict);
        };
        if record.attempt != *attempt
            || !matches!(record.state, ReceiptState::Running | ReceiptState::Unknown)
        {
            return Err(ReceiptError::Conflict);
        }
        let prior = record.state.clone();
        record.state = ReceiptState::Applied {
            result_digest,
            response,
        };
        if let Err(error) = self.journal.commit(&records) {
            if let Some(record) = records.get_mut(&attempt.attempt_id) {
                record.state = prior;
            }
            return Err(error);
        }
        drop(records);
        self.inject(ReceiptCrashPoint::AppliedCommitted)
    }

    pub(super) fn complete_absent(
        &self,
        attempt: &HostAttemptContext,
        failure: HostFailure,
    ) -> Result<(), ReceiptError> {
        let mut records = self.records.lock().unwrap_or_else(|p| p.into_inner());
        let record = records
            .get_mut(&attempt.attempt_id)
            .ok_or(ReceiptError::Conflict)?;
        if record.attempt != *attempt
            || attempt.kind != HostAttemptKind::WriteInput
            || !matches!(record.state, ReceiptState::Running)
        {
            return Err(ReceiptError::Conflict);
        }
        let prior = record.state.clone();
        record.state = ReceiptState::Absent { failure };
        if let Err(error) = self.journal.commit(&records) {
            records.get_mut(&attempt.attempt_id).unwrap().state = prior;
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn mark_unknown(&self, attempt: &HostAttemptContext) -> Result<(), ReceiptError> {
        self.mark_unknown_with_failure(attempt, None)
    }

    pub(super) fn mark_unknown_with_failure(
        &self,
        attempt: &HostAttemptContext,
        failure: Option<HostFailure>,
    ) -> Result<(), ReceiptError> {
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(record) = records.get_mut(&attempt.attempt_id) else {
            return Err(ReceiptError::Conflict);
        };
        if record.attempt != *attempt {
            return Err(ReceiptError::Conflict);
        }
        if matches!(
            record.state,
            ReceiptState::Applied { .. }
                | ReceiptState::Absent { .. }
                | ReceiptState::CancelledShutdown
        ) {
            return Ok(());
        }
        let prior = record.state.clone();
        record.state = ReceiptState::Unknown;
        let prior_failure = std::mem::replace(&mut record.failure, failure);
        if let Err(error) = self.journal.commit(&records) {
            if let Some(record) = records.get_mut(&attempt.attempt_id) {
                record.state = prior;
                record.failure = prior_failure;
            }
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn input_is_quiescent(&self, attempt: &HostAttemptContext) -> bool {
        let records = self.records.lock().unwrap_or_else(|p| p.into_inner());
        attempt.kind == HostAttemptKind::WriteInput
            && records.get(&attempt.attempt_id).is_some_and(|record| {
                record.attempt == *attempt && matches!(record.state, ReceiptState::Unknown)
            })
            && !records.values().any(|record| {
                record.attempt.session_id == attempt.session_id
                    && matches!(record.state, ReceiptState::Running)
            })
    }

    pub(super) fn observe(
        &self,
        attempt: &HostAttemptContext,
    ) -> Result<ReceiptObservation, ReceiptError> {
        let records = self
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(record) = records.get(&attempt.attempt_id) else {
            return Ok(ReceiptObservation {
                certainty: HostEffectCertainty::Unknown,
                result_digest: None,
            });
        };
        if record.attempt != *attempt {
            return Err(ReceiptError::Conflict);
        }
        Ok(match &record.state {
            ReceiptState::Absent { .. } | ReceiptState::CancelledShutdown => ReceiptObservation {
                certainty: HostEffectCertainty::KnownAbsent,
                result_digest: None,
            },
            ReceiptState::Applied { result_digest, .. } => ReceiptObservation {
                certainty: HostEffectCertainty::KnownApplied,
                result_digest: Some(result_digest.clone()),
            },
            ReceiptState::Running | ReceiptState::Unknown => ReceiptObservation {
                certainty: HostEffectCertainty::Unknown,
                result_digest: None,
            },
        })
    }

    pub(super) fn stop_process(
        &self,
        attempt: &HostAttemptContext,
    ) -> Result<Option<String>, ReceiptError> {
        let records = self.records.lock().unwrap_or_else(|p| p.into_inner());
        let Some(record) = records.get(&attempt.attempt_id) else {
            return Ok(None);
        };
        if record.attempt != *attempt {
            return Err(ReceiptError::Conflict);
        }
        Ok(record.process_id.clone())
    }

    pub(super) fn probe(&self, attempt: &HostAttemptContext) -> Result<ReceiptProbe, ReceiptError> {
        if !valid_attempt(attempt) {
            return Err(ReceiptError::Invalid);
        }
        let records = self
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(record) = records.get(&attempt.attempt_id) else {
            return Ok(ReceiptProbe::Absent);
        };
        if record.attempt != *attempt {
            return Err(ReceiptError::Conflict);
        }
        Ok(match &record.state {
            ReceiptState::Absent { .. } | ReceiptState::CancelledShutdown => ReceiptProbe::Unknown,
            ReceiptState::Running | ReceiptState::Unknown => ReceiptProbe::Unknown,
            ReceiptState::Applied { response, .. } => ReceiptProbe::Applied {
                response: response.clone(),
            },
        })
    }

    pub(super) fn acknowledge(&self, attempt: &HostAttemptContext) -> Result<(), ReceiptError> {
        self.acknowledge_inner(attempt, false)
    }

    /// Cancels only the preparation phase; shutdown effects happen on ACK.
    /// The daemon calls this only for an abandoned, durably uncommitted shutdown.
    /// Keep a tombstone until settlement so a lost reply or host restart cannot
    /// cause the exact preparation to execute again.
    pub(super) fn cancel_shutdown(&self, attempt: &HostAttemptContext) -> Result<(), ReceiptError> {
        if !valid_attempt(attempt) || attempt.kind != HostAttemptKind::Shutdown {
            return Err(ReceiptError::Invalid);
        }
        let mut records = self.records.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(record) = records.get(&attempt.attempt_id) {
            if record.attempt != *attempt {
                return Err(ReceiptError::Conflict);
            }
            if matches!(record.state, ReceiptState::CancelledShutdown) {
                return Ok(());
            }
        } else if records.len() >= self.capacity {
            return Err(ReceiptError::Capacity);
        }
        let prior = records.insert(
            attempt.attempt_id.clone(),
            ReceiptRecord {
                attempt: attempt.clone(),
                process_id: None,
                state: ReceiptState::CancelledShutdown,
                failure: None,
            },
        );
        if let Err(error) = self.journal.commit(&records) {
            if let Some(prior) = prior {
                records.insert(attempt.attempt_id.clone(), prior);
            } else {
                records.remove(&attempt.attempt_id);
            }
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn acknowledge_shutdown_cancellation(
        &self,
        attempt: &HostAttemptContext,
    ) -> Result<(), ReceiptError> {
        if !valid_attempt(attempt) || attempt.kind != HostAttemptKind::Shutdown {
            return Err(ReceiptError::Invalid);
        }
        self.acknowledge_inner(attempt, true)
    }

    fn acknowledge_inner(
        &self,
        attempt: &HostAttemptContext,
        cancelled: bool,
    ) -> Result<(), ReceiptError> {
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(record) = records.get(&attempt.attempt_id) else {
            return Ok(());
        };
        if record.attempt != *attempt {
            return Err(ReceiptError::Conflict);
        }
        if matches!(record.state, ReceiptState::CancelledShutdown) != cancelled {
            return Err(ReceiptError::Conflict);
        }
        let removed = records.remove(&attempt.attempt_id);
        if let Err(error) = self.journal.commit(&records) {
            if let Some(record) = removed {
                records.insert(attempt.attempt_id.clone(), record);
            }
            return Err(error);
        }
        self.inject(ReceiptCrashPoint::AcknowledgementCommitted)
    }

    fn inject(&self, point: ReceiptCrashPoint) -> Result<(), ReceiptError> {
        self.crash_hook.at(point)
    }
}

#[derive(Debug)]
pub(super) enum ReceiptBegin {
    Started,
    KnownAbsent,
    Applied { response: HostResponseValue },
    Unknown,
    Conflict,
}

pub(super) struct ReceiptObservation {
    pub(super) certainty: HostEffectCertainty,
    pub(super) result_digest: Option<String>,
}

pub(super) enum ReceiptProbe {
    Absent,
    Unknown,
    Applied { response: HostResponseValue },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReceiptCrashPoint {
    RunningCommitted,
    EffectCompleted,
    AppliedCommitted,
    AcknowledgementCommitted,
}

trait ReceiptCrashHook: Send + Sync {
    fn at(&self, point: ReceiptCrashPoint) -> Result<(), ReceiptError>;
}

#[cfg(test)]
const PROCESS_TEST_MODE_ENV: &str = "UNDERCURRENT_RUNTIME_TEST_RECEIPT_MODE";

#[cfg(test)]
fn process_test_crash_point() -> Result<Option<ReceiptCrashPoint>, ReceiptError> {
    match std::env::var(PROCESS_TEST_MODE_ENV).ok().as_deref() {
        None
        | Some(
            "none"
            | "response_loss"
            | "stop_receipt_unknown"
            | "input_receipt_unknown"
            | "receipt_commit_capacity",
        ) => Ok(None),
        Some("crash_before_effect") => Ok(Some(ReceiptCrashPoint::RunningCommitted)),
        Some("crash_after_effect") => Ok(Some(ReceiptCrashPoint::EffectCompleted)),
        Some(_) => Err(ReceiptError::Invalid),
    }
}

#[cfg(test)]
struct AbortProcessAt(ReceiptCrashPoint);

#[cfg(test)]
impl ReceiptCrashHook for AbortProcessAt {
    fn at(&self, point: ReceiptCrashPoint) -> Result<(), ReceiptError> {
        if point == self.0 {
            std::process::exit(86);
        }
        Ok(())
    }
}

struct NoReceiptCrash;

impl ReceiptCrashHook for NoReceiptCrash {
    fn at(&self, _point: ReceiptCrashPoint) -> Result<(), ReceiptError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptRecord {
    attempt: HostAttemptContext,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    process_id: Option<String>,
    state: ReceiptState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    failure: Option<HostFailure>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum ReceiptState {
    Running,
    CancelledShutdown,
    Absent {
        failure: HostFailure,
    },
    Applied {
        result_digest: String,
        response: HostResponseValue,
    },
    Unknown,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptLedger {
    version: undercurrent_foundation::ReleaseVersion,
    records: Vec<ReceiptRecord>,
}

struct ReceiptJournal {
    directory: PathBuf,
    file: PathBuf,
    maximum_bytes: usize,
}

impl ReceiptJournal {
    fn new(directory: PathBuf, capacity: usize) -> Result<Self, ReceiptError> {
        if let Ok(metadata) = fs::symlink_metadata(&directory)
            && (metadata.file_type().is_symlink() || !metadata.is_dir())
        {
            return Err(ReceiptError::Invalid);
        }
        host_files::prepare_directory(&directory).map_err(ReceiptError::Io)?;
        let maximum_bytes = capacity
            .checked_mul(BYTES_PER_RECEIPT)
            .and_then(|bytes| bytes.checked_add(1024))
            .ok_or(ReceiptError::Capacity)?;
        Ok(Self {
            file: directory.join(JOURNAL_FILE),
            directory,
            maximum_bytes,
        })
    }

    fn load(&self) -> Result<BTreeMap<String, ReceiptRecord>, ReceiptError> {
        let _ = fs::remove_file(self.directory.join(TEMPORARY_FILE));
        if !self.file.exists() {
            return Ok(BTreeMap::new());
        }
        let metadata = fs::symlink_metadata(&self.file).map_err(ReceiptError::Io)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(ReceiptError::Invalid);
        }
        #[cfg(unix)]
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(ReceiptError::Invalid);
        }
        let mut bytes = Vec::new();
        host_files::read(&self.file)
            .and_then(|file| {
                file.take(self.maximum_bytes as u64 + 1)
                    .read_to_end(&mut bytes)
            })
            .map_err(ReceiptError::Io)?;
        if bytes.len() > self.maximum_bytes {
            return Err(ReceiptError::Capacity);
        }
        let ledger: ReceiptLedger = serde_json::from_slice(&bytes).map_err(ReceiptError::Codec)?;
        if ledger.version != JOURNAL_VERSION {
            return Err(ReceiptError::Compatibility);
        }
        let mut records = BTreeMap::new();
        for record in ledger.records {
            let key = record.attempt.attempt_id.clone();
            if !valid_record(&record) || records.insert(key, record).is_some() {
                return Err(ReceiptError::Invalid);
            }
        }
        Ok(records)
    }

    fn commit(&self, records: &BTreeMap<String, ReceiptRecord>) -> Result<(), ReceiptError> {
        let ledger = ReceiptLedger {
            version: JOURNAL_VERSION,
            records: records.values().cloned().collect(),
        };
        let bytes = serde_json::to_vec(&ledger).map_err(ReceiptError::Codec)?;
        if bytes.len() > self.maximum_bytes {
            return Err(ReceiptError::Capacity);
        }
        let temporary = self.directory.join(TEMPORARY_FILE);
        let _ = fs::remove_file(&temporary);
        let mut file = host_files::create_new(&temporary).map_err(ReceiptError::Io)?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(ReceiptError::Io)?;
        drop(file);
        host_files::replace(&temporary, &self.file).map_err(ReceiptError::Io)?;
        host_files::restrict(&self.file).map_err(ReceiptError::Io)?;
        sync_directory(&self.directory).map_err(ReceiptError::Io)
    }
}

fn sync_directory(path: &Path) -> std::io::Result<()> {
    host_files::sync_directory(path)
}

fn valid_record(record: &ReceiptRecord) -> bool {
    valid_attempt(&record.attempt)
        && record.process_id.as_ref().is_none_or(|id| {
            matches!(
                record.attempt.kind,
                HostAttemptKind::Stop | HostAttemptKind::ForceStop
            ) && undercurrent_domain::ProcessInstanceId::new(id.clone()).is_ok()
        })
        && match &record.state {
            ReceiptState::CancelledShutdown => record.attempt.kind == HostAttemptKind::Shutdown,
            ReceiptState::Absent { .. } => record.attempt.kind == HostAttemptKind::WriteInput,
            ReceiptState::Running | ReceiptState::Unknown => true,
            ReceiptState::Applied {
                result_digest,
                response,
            } => valid_digest(result_digest) && valid_response(record.attempt.kind, response),
        }
}

fn valid_attempt(attempt: &HostAttemptContext) -> bool {
    let target_valid = match attempt.kind {
        HostAttemptKind::Spawn
        | HostAttemptKind::ArmExit
        | HostAttemptKind::Stop
        | HostAttemptKind::ForceStop
        | HostAttemptKind::WriteInput
        | HostAttemptKind::Resize => attempt
            .session_id
            .as_ref()
            .is_some_and(|session| !session.is_empty()),
        HostAttemptKind::Shutdown => attempt.session_id.is_none(),
        HostAttemptKind::ExecuteInSession => false,
    };
    !attempt.attempt_id.is_empty()
        && attempt.fencing_token != 0
        && valid_digest(&attempt.payload_digest)
        && attempt.fencing_scope.as_ref().is_none_or(|scope| {
            !scope.is_empty()
                && scope.len() <= 256
                && scope.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':')
                })
        })
        && target_valid
}

fn same_fencing_scope(left: &HostAttemptContext, right: &HostAttemptContext) -> bool {
    match (&left.fencing_scope, &right.fencing_scope) {
        (Some(left), Some(right)) => left == right,
        // A legacy receipt did not retain its exact counter domain. Compare it
        // session-wide so an upgrade never weakens an already retained fence.
        _ => left.session_id == right.session_id,
    }
}

fn valid_digest(digest: &str) -> bool {
    digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_response(kind: HostAttemptKind, response: &HostResponseValue) -> bool {
    matches!(
        (kind, response),
        (HostAttemptKind::Spawn, HostResponseValue::Spawned { .. })
            | (
                HostAttemptKind::Stop | HostAttemptKind::ForceStop,
                HostResponseValue::Terminated { .. }
            )
            | (
                HostAttemptKind::ArmExit
                    | HostAttemptKind::WriteInput
                    | HostAttemptKind::Resize
                    | HostAttemptKind::Shutdown,
                HostResponseValue::Completed
            )
    )
}

#[derive(Debug)]
pub(super) enum ReceiptError {
    Capacity,
    Compatibility,
    Conflict,
    Invalid,
    Io(std::io::Error),
    Codec(serde_json::Error),
}

impl Display for ReceiptError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Capacity => "host attempt receipt capacity was exceeded",
            Self::Compatibility => "host attempt receipt version is unsupported",
            Self::Conflict => "host attempt receipt conflicts with retained evidence",
            Self::Invalid => "host attempt receipt journal is invalid",
            Self::Io(_) => "host attempt receipt journal IO failed",
            Self::Codec(_) => "host attempt receipt journal encoding failed",
        })
    }
}

impl Error for ReceiptError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(source) => Some(source),
            Self::Codec(source) => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::process_host_protocol::HostTerminationKind;
    struct FailAt(ReceiptCrashPoint);

    impl ReceiptCrashHook for FailAt {
        fn at(&self, point: ReceiptCrashPoint) -> Result<(), ReceiptError> {
            if point == self.0 {
                Err(ReceiptError::Io(std::io::Error::other(
                    "injected receipt crash",
                )))
            } else {
                Ok(())
            }
        }
    }

    fn attempt(kind: HostAttemptKind) -> HostAttemptContext {
        HostAttemptContext {
            attempt_id: format!("attempt-{kind:?}"),
            session_id: (kind != HostAttemptKind::Shutdown).then(|| "session-1".to_owned()),
            fencing_scope: Some(if kind == HostAttemptKind::Shutdown {
                "host".to_owned()
            } else {
                "session:session-1".to_owned()
            }),
            kind,
            fencing_token: 7,
            payload_digest: "ab".repeat(32),
            accepted_at_millis: 42,
        }
    }

    fn response(kind: HostAttemptKind) -> HostResponseValue {
        match kind {
            HostAttemptKind::Spawn => HostResponseValue::Spawned {
                process_id: "host-1-42-1".to_owned(),
                started_at_millis: 43,
                root_pid: 42,
                root_start_ticks: Some(7),
            },
            HostAttemptKind::Stop | HostAttemptKind::ForceStop => HostResponseValue::Terminated {
                kind: HostTerminationKind::Forced,
                exit_code: Some(9),
                observed_at_millis: 44,
            },
            HostAttemptKind::ArmExit
            | HostAttemptKind::WriteInput
            | HostAttemptKind::Resize
            | HostAttemptKind::Shutdown => HostResponseValue::Completed,
            HostAttemptKind::ExecuteInSession => unreachable!("separate execution journal"),
        }
    }

    #[test]
    fn shutdown_cancellation_is_durable_exact_and_prevents_preparation_replay() {
        for prepared in [false, true] {
            let directory = tempfile::tempdir().expect("journal directory");
            let path = directory.path().join("receipts");
            let registry = HostAttemptReceiptRegistry::new(8, path.clone()).expect("registry");
            let attempt = attempt(HostAttemptKind::Shutdown);
            if prepared {
                registry.begin(&attempt).expect("begin preparation");
                registry
                    .complete_applied(&attempt, "cd".repeat(32), HostResponseValue::Completed)
                    .expect("prepare shutdown without retiring host");
            }
            registry
                .cancel_shutdown(&attempt)
                .expect("cancel preparation");
            registry
                .cancel_shutdown(&attempt)
                .expect("lost cancellation response retry");
            assert!(matches!(
                registry.acknowledge(&attempt),
                Err(ReceiptError::Conflict)
            ));
            assert!(matches!(
                registry.begin(&attempt),
                Ok(ReceiptBegin::KnownAbsent)
            ));
            assert!(
                registry
                    .complete_applied(&attempt, "cd".repeat(32), HostResponseValue::Completed)
                    .is_err()
            );
            registry
                .mark_unknown(&attempt)
                .expect("late inconclusive response");
            drop(registry);

            let registry = HostAttemptReceiptRegistry::new(8, path).expect("reopen journal");
            assert_eq!(
                registry
                    .observe(&attempt)
                    .expect("durable cancellation")
                    .certainty,
                HostEffectCertainty::KnownAbsent
            );
            let mut conflicting = attempt.clone();
            conflicting.fencing_token += 1;
            assert!(matches!(
                registry.cancel_shutdown(&conflicting),
                Err(ReceiptError::Conflict)
            ));
            registry
                .acknowledge_shutdown_cancellation(&attempt)
                .expect("release after settlement");
            registry
                .acknowledge_shutdown_cancellation(&attempt)
                .expect("idempotent release");
        }
        let directory = tempfile::tempdir().expect("journal directory");
        let registry = HostAttemptReceiptRegistry::new(8, directory.path().join("receipts"))
            .expect("registry");
        assert!(matches!(
            registry.cancel_shutdown(&attempt(HostAttemptKind::WriteInput)),
            Err(ReceiptError::Invalid)
        ));
    }

    #[test]
    fn every_process_kind_is_unknown_after_crash_before_or_after_effect() {
        for kind in [
            HostAttemptKind::Spawn,
            HostAttemptKind::ArmExit,
            HostAttemptKind::Stop,
            HostAttemptKind::ForceStop,
            HostAttemptKind::WriteInput,
            HostAttemptKind::Resize,
            HostAttemptKind::Shutdown,
        ] {
            for point in [
                ReceiptCrashPoint::RunningCommitted,
                ReceiptCrashPoint::EffectCompleted,
            ] {
                let directory = tempfile::tempdir().expect("temporary receipt directory");
                let path = directory.path().join("journal");
                let registry = HostAttemptReceiptRegistry::new_with_hook(
                    16,
                    path.clone(),
                    Arc::new(FailAt(point)),
                )
                .expect("registry");
                let attempt = attempt(kind);
                if point == ReceiptCrashPoint::RunningCommitted {
                    assert!(registry.begin(&attempt).is_err());
                } else {
                    assert!(matches!(
                        registry.begin(&attempt),
                        Ok(ReceiptBegin::Started)
                    ));
                    assert!(
                        registry
                            .complete_applied(&attempt, "cd".repeat(32), response(kind),)
                            .is_err()
                    );
                }
                drop(registry);
                let recovered = HostAttemptReceiptRegistry::new(16, path).expect("recover");
                assert_eq!(
                    recovered.observe(&attempt).expect("observe").certainty,
                    HostEffectCertainty::Unknown
                );
                assert!(matches!(
                    recovered.begin(&attempt),
                    Ok(ReceiptBegin::Unknown)
                ));
            }
        }
    }

    #[test]
    fn every_process_kind_replays_after_receipt_commit_until_acknowledged() {
        for kind in [
            HostAttemptKind::Spawn,
            HostAttemptKind::ArmExit,
            HostAttemptKind::Stop,
            HostAttemptKind::ForceStop,
            HostAttemptKind::WriteInput,
            HostAttemptKind::Resize,
            HostAttemptKind::Shutdown,
        ] {
            let directory = tempfile::tempdir().expect("temporary receipt directory");
            let path = directory.path().join("journal");
            let registry = HostAttemptReceiptRegistry::new_with_hook(
                16,
                path.clone(),
                Arc::new(FailAt(ReceiptCrashPoint::AppliedCommitted)),
            )
            .expect("registry");
            let attempt = attempt(kind);
            assert!(matches!(
                registry.begin(&attempt),
                Ok(ReceiptBegin::Started)
            ));
            assert!(
                registry
                    .complete_applied(&attempt, "cd".repeat(32), response(kind),)
                    .is_err()
            );
            assert!(matches!(
                registry.begin(&attempt),
                Ok(ReceiptBegin::Applied { .. })
            ));
            drop(registry);
            let recovered = HostAttemptReceiptRegistry::new(16, path).expect("recover");
            let observation = recovered.observe(&attempt).expect("observe");
            if kind == HostAttemptKind::Spawn {
                assert_eq!(observation.certainty, HostEffectCertainty::Unknown);
                assert!(matches!(
                    recovered.begin(&attempt),
                    Ok(ReceiptBegin::Unknown)
                ));
            } else {
                assert_eq!(observation.certainty, HostEffectCertainty::KnownApplied);
                assert_eq!(
                    observation.result_digest.as_deref(),
                    Some("cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd")
                );
                assert!(matches!(
                    recovered.begin(&attempt),
                    Ok(ReceiptBegin::Applied { .. })
                ));
            }
            if kind != HostAttemptKind::Spawn {
                recovered.acknowledge(&attempt).expect("acknowledge");
                assert_eq!(
                    recovered
                        .observe(&attempt)
                        .expect("observe after ack")
                        .certainty,
                    HostEffectCertainty::Unknown
                );
            }
        }
    }

    #[test]
    fn journal_is_owner_only_and_rejects_future_versions() {
        let directory = tempfile::tempdir().expect("temporary receipt directory");
        let path = directory.path().join("journal");
        let registry = HostAttemptReceiptRegistry::new(2, path.clone()).expect("registry");
        let attempt = attempt(HostAttemptKind::ArmExit);
        assert!(matches!(
            registry.begin(&attempt),
            Ok(ReceiptBegin::Started)
        ));
        #[cfg(unix)]
        let mode = fs::metadata(path.join(JOURNAL_FILE))
            .expect("journal metadata")
            .permissions()
            .mode()
            & 0o777;
        #[cfg(unix)]
        assert_eq!(mode, 0o600);
        drop(registry);
        fs::write(path.join(JOURNAL_FILE), br#"{"version":2,"records":[]}"#)
            .expect("write future journal");
        assert!(matches!(
            HostAttemptReceiptRegistry::new(2, path),
            Err(ReceiptError::Codec(_))
        ));
    }

    #[test]
    fn acknowledgement_response_loss_does_not_restore_released_evidence() {
        let directory = tempfile::tempdir().expect("temporary receipt directory");
        let path = directory.path().join("journal");
        let registry = HostAttemptReceiptRegistry::new_with_hook(
            2,
            path.clone(),
            Arc::new(FailAt(ReceiptCrashPoint::AcknowledgementCommitted)),
        )
        .expect("registry");
        let attempt = attempt(HostAttemptKind::Resize);
        assert!(matches!(
            registry.begin(&attempt),
            Ok(ReceiptBegin::Started)
        ));
        registry
            .complete_applied(&attempt, "ef".repeat(32), HostResponseValue::Completed)
            .expect("complete receipt");
        assert!(registry.acknowledge(&attempt).is_err());
        drop(registry);
        let recovered = HostAttemptReceiptRegistry::new(2, path).expect("recover");
        assert_eq!(
            recovered.observe(&attempt).expect("observe").certainty,
            HostEffectCertainty::Unknown
        );
        recovered
            .acknowledge(&attempt)
            .expect("idempotent acknowledgement");
    }

    #[test]
    fn retained_target_fence_rejects_stale_or_colliding_attempts() {
        let directory = tempfile::tempdir().expect("temporary receipt directory");
        let registry =
            HostAttemptReceiptRegistry::new(4, directory.path().join("journal")).expect("registry");
        let mut current = attempt(HostAttemptKind::Resize);
        current.fencing_token = 9;
        assert!(matches!(
            registry.begin(&current),
            Ok(ReceiptBegin::Started)
        ));
        registry
            .complete_applied(&current, "ab".repeat(32), HostResponseValue::Completed)
            .expect("complete current fence");

        for fence in [8, 9] {
            let mut stale = attempt(HostAttemptKind::Resize);
            stale.attempt_id = format!("stale-{fence}");
            stale.fencing_token = fence;
            assert!(matches!(registry.begin(&stale), Ok(ReceiptBegin::Conflict)));
        }
        let mut next = attempt(HostAttemptKind::Resize);
        next.attempt_id = "next-fence".to_owned();
        next.fencing_token = 10;
        assert!(matches!(registry.begin(&next), Ok(ReceiptBegin::Started)));
    }

    #[test]
    fn stop_binding_survives_restart_and_legacy_receipt_never_guesses_a_process() {
        let directory = tempfile::tempdir().unwrap();
        let journal = directory.path().join("journal");
        let target = attempt(HostAttemptKind::ForceStop);
        let registry = HostAttemptReceiptRegistry::new(8, journal.clone()).unwrap();
        registry
            .begin_bound(&target, Some("exact-process"))
            .unwrap();
        registry.mark_unknown(&target).unwrap();
        drop(registry);
        let registry = HostAttemptReceiptRegistry::new(8, journal).unwrap();
        assert_eq!(
            registry.stop_process(&target).unwrap().as_deref(),
            Some("exact-process")
        );
        assert!(matches!(
            registry.begin_bound(&target, Some("replacement")).unwrap(),
            ReceiptBegin::Conflict
        ));

        let legacy = tempfile::tempdir().unwrap();
        let journal = legacy.path().join("journal");
        let registry = HostAttemptReceiptRegistry::new(8, journal.clone()).unwrap();
        registry.begin(&target).unwrap();
        registry.mark_unknown(&target).unwrap();
        drop(registry);
        let registry = HostAttemptReceiptRegistry::new(8, journal).unwrap();
        assert_eq!(registry.stop_process(&target).unwrap(), None);
        assert!(matches!(
            registry
                .begin_bound(&target, Some("unproven-process"))
                .unwrap(),
            ReceiptBegin::Unknown
        ));
        assert_eq!(registry.stop_process(&target).unwrap(), None);
    }

    #[test]
    fn independent_fencing_scopes_do_not_compare_counters() {
        let directory = tempfile::tempdir().expect("temporary receipt directory");
        let registry =
            HostAttemptReceiptRegistry::new(4, directory.path().join("journal")).expect("registry");
        let mut dispatched = attempt(HostAttemptKind::Spawn);
        dispatched.attempt_id = "dispatch-spawn".to_owned();
        dispatched.fencing_scope = Some("dispatch:cell-build".to_owned());
        dispatched.fencing_token = 9;
        assert!(matches!(
            registry.begin(&dispatched),
            Ok(ReceiptBegin::Started)
        ));
        registry
            .complete_applied(
                &dispatched,
                "ab".repeat(32),
                response(HostAttemptKind::Spawn),
            )
            .expect("complete dispatched spawn");

        let mut arm = attempt(HostAttemptKind::ArmExit);
        arm.attempt_id = "session-arm".to_owned();
        arm.fencing_scope = Some("session:session-1".to_owned());
        arm.fencing_token = 1;
        assert!(matches!(registry.begin(&arm), Ok(ReceiptBegin::Started)));
    }
}
