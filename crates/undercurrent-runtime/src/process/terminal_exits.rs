// SPDX-License-Identifier: Apache-2.0

//! Bounded durable proof that one exact native process incarnation exited.

use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use undercurrent_domain::{ProcessInstanceId, SessionId};

use super::attempt_receipts::ReceiptError;
use super::host_files;
use crate::process_host_protocol::HostExitEvent;

type ExitMap = BTreeMap<(String, String), HostExitEvent>;

pub(super) struct TerminalExitRegistry {
    directory: PathBuf,
    capacity: usize,
    entries: Mutex<ExitMap>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ledger {
    version: undercurrent_foundation::ReleaseVersion,
    exits: Vec<HostExitEvent>,
}

impl TerminalExitRegistry {
    pub(super) fn new(directory: PathBuf, capacity: usize) -> Result<Self, ReceiptError> {
        host_files::prepare_directory(&directory).map_err(ReceiptError::Io)?;
        let path = directory.join("terminal-exits.json");
        let mut entries = BTreeMap::new();
        if path.exists() {
            let metadata = fs::symlink_metadata(&path).map_err(ReceiptError::Io)?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(ReceiptError::Invalid);
            }
            #[cfg(unix)]
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(ReceiptError::Invalid);
            }
            let maximum = capacity.saturating_mul(1024).saturating_add(1024);
            let mut bytes = Vec::new();
            host_files::read(&path)
                .and_then(|file| file.take(maximum as u64 + 1).read_to_end(&mut bytes))
                .map_err(ReceiptError::Io)?;
            if bytes.len() > maximum {
                return Err(ReceiptError::Capacity);
            }
            let ledger: Ledger = serde_json::from_slice(&bytes).map_err(ReceiptError::Codec)?;
            if ledger.version != undercurrent_foundation::ReleaseVersion::Current {
                return Err(ReceiptError::Compatibility);
            }
            for event in ledger.exits {
                if !valid(&event)
                    || entries
                        .insert((event.session_id.clone(), event.process_id.clone()), event)
                        .is_some()
                {
                    return Err(ReceiptError::Invalid);
                }
            }
            if entries.len() > capacity {
                return Err(ReceiptError::Capacity);
            }
        }
        Ok(Self {
            directory,
            capacity,
            entries: Mutex::new(entries),
        })
    }

    pub(super) fn record(&self, event: &HostExitEvent) -> Result<(), ReceiptError> {
        if !valid(event) {
            return Err(ReceiptError::Invalid);
        }
        let mut entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        let key = (event.session_id.clone(), event.process_id.clone());
        if let Some(previous) = entries.get(&key) {
            return if previous.observed_at_millis == event.observed_at_millis
                && previous.exit_code == event.exit_code
            {
                Ok(())
            } else {
                Err(ReceiptError::Conflict)
            };
        }
        let mut next = entries.clone();
        next.insert(key, event.clone());
        if next.len() > self.capacity {
            let oldest = next
                .iter()
                .min_by_key(|(_, value)| value.observed_at_millis)
                .map(|(key, _)| key.clone())
                .ok_or(ReceiptError::Capacity)?;
            next.remove(&oldest);
        }
        let bytes = serde_json::to_vec(&Ledger {
            version: undercurrent_foundation::ReleaseVersion::Current,
            exits: next.values().cloned().collect(),
        })
        .map_err(ReceiptError::Codec)?;
        let temporary = self.directory.join("terminal-exits.tmp");
        let _ = fs::remove_file(&temporary);
        let mut file = host_files::create_new(&temporary).map_err(ReceiptError::Io)?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(ReceiptError::Io)?;
        drop(file);
        host_files::replace(&temporary, &self.directory.join("terminal-exits.json"))
            .map_err(ReceiptError::Io)?;
        host_files::sync_directory(&self.directory).map_err(ReceiptError::Io)?;
        *entries = next;
        Ok(())
    }

    pub(super) fn latest_for_session(&self, session: &str) -> Option<HostExitEvent> {
        self.entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .filter(|event| event.session_id == session && !event.supervision_failed)
            .max_by_key(|event| event.observed_at_millis)
            .cloned()
    }

    pub(super) fn find(&self, session: &str, process: &str) -> Option<HostExitEvent> {
        self.entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&(session.to_owned(), process.to_owned()))
            .cloned()
    }
}

fn valid(event: &HostExitEvent) -> bool {
    SessionId::new(event.session_id.clone()).is_ok()
        && ProcessInstanceId::new(event.process_id.clone()).is_ok()
        && !event.supervision_failed
        && event.exit_code.is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_exit_evidence_is_bounded_exact_and_rejects_uncertain_outcomes() {
        let directory = tempfile::tempdir().unwrap();
        let journal = directory.path().join("exits");
        let registry = TerminalExitRegistry::new(journal.clone(), 1).unwrap();
        let first = HostExitEvent {
            session_id: "session".to_owned(),
            process_id: "first".to_owned(),
            output_offset: 0,
            exit_code: Some(0),
            supervision_failed: false,
            observed_at_millis: 10,
        };
        registry.record(&first).unwrap();
        assert!(registry.find("session", "replacement").is_none());
        let mut uncertain = first.clone();
        uncertain.supervision_failed = true;
        uncertain.exit_code = None;
        assert!(registry.record(&uncertain).is_err());
        let mut second = first.clone();
        second.process_id = "second".to_owned();
        second.observed_at_millis = 20;
        registry.record(&second).unwrap();
        drop(registry);
        let registry = TerminalExitRegistry::new(journal, 1).unwrap();
        assert!(registry.find("session", "first").is_none());
        assert_eq!(
            registry.find("session", "second").unwrap().exit_code,
            Some(0)
        );
    }
}
