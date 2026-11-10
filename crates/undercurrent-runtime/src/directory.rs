// SPDX-License-Identifier: Apache-2.0

//! In-process supervised process bindings.

use std::collections::BTreeMap;
use std::sync::RwLock;

use undercurrent_application::{
    PortError, PortErrorKind, PortOperation, ProcessInstance, ProcessUnbindOutcome,
    SessionProcessDirectory,
};
use undercurrent_domain::{ProcessInstanceId, SessionId};

use crate::RuntimeAdapterFailure;

/// Thread-safe exact process incarnation directory.
#[derive(Default)]
pub struct InMemoryProcessDirectory {
    processes: RwLock<BTreeMap<SessionId, ProcessInstance>>,
}

impl InMemoryProcessDirectory {
    /// Returns active process binding count without process IO.
    pub fn len(&self) -> usize {
        self.processes
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    /// Returns whether no process binding is active.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl SessionProcessDirectory for InMemoryProcessDirectory {
    fn session_ids(&self) -> Result<Vec<SessionId>, PortError> {
        Ok(self
            .processes
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .keys()
            .cloned()
            .collect())
    }

    fn bind(&self, session_id: &SessionId, process: ProcessInstance) -> Result<(), PortError> {
        let mut processes = self
            .processes
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if processes.contains_key(session_id) {
            return Err(PortError::new(
                PortErrorKind::Conflict,
                PortOperation::BindProcess,
                Some(session_id.clone()),
                RuntimeAdapterFailure("session already has a process binding"),
            ));
        }
        processes.insert(session_id.clone(), process);
        Ok(())
    }

    fn lookup(&self, session_id: &SessionId) -> Result<Option<ProcessInstance>, PortError> {
        Ok(self
            .processes
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(session_id)
            .cloned())
    }

    fn unbind_if(
        &self,
        session_id: &SessionId,
        process: &ProcessInstanceId,
    ) -> Result<ProcessUnbindOutcome, PortError> {
        let mut processes = self
            .processes
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(current) = processes.get(session_id) else {
            return Ok(ProcessUnbindOutcome::Missing);
        };
        if &current.id != process {
            return Ok(ProcessUnbindOutcome::ProcessMismatch);
        }
        Ok(processes
            .remove(session_id)
            .map_or(ProcessUnbindOutcome::Missing, ProcessUnbindOutcome::Unbound))
    }
}
