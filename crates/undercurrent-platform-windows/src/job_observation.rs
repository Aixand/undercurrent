// SPDX-License-Identifier: Apache-2.0
//! Bounded, retained Job Object membership. No parent-PID inference is used.
use crate::ProcessHandle;
use std::io;
use std::os::windows::io::{AsRawHandle, OwnedHandle};

const MAX_PROCESSES: usize = 128;

/// A retained job handle used only for process membership queries.
pub struct JobProcessObserver(OwnedHandle);

/// Process handles whose membership was checked against the retained job.
/// Completeness describes the collection, not a promise of future membership.
pub struct JobProcessSnapshot {
    processes: Vec<ProcessHandle>,
    complete: bool,
}

impl JobProcessSnapshot {
    pub fn processes(&self) -> &[ProcessHandle] {
        &self.processes
    }
    /// Whether capacity, member access, liveness, and before/after membership
    /// checks all succeeded. Recheck with `JobProcessObserver::is_current` after
    /// collecting further facts.
    pub const fn complete(&self) -> bool {
        self.complete
    }
}

impl JobProcessObserver {
    /// Observes host TCP listeners owned by this exact root's retained job.
    /// Each address family has one bounded collection for the whole tree, with
    /// up to three query attempts to accommodate table growth.
    /// Membership churn forbids a negative result, and process exit invalidates
    /// facts rather than transferring them to a replacement PID.
    pub fn tcp_listener(&self, root: &ProcessHandle) -> io::Result<crate::TcpListenerObservation> {
        use crate::TcpListenerObservation::{Absent, Truncated};
        if !self.contains(root)? {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "managed root is outside job",
            ));
        }
        let snapshot = self.snapshot()?;
        let processes: Vec<_> = snapshot.processes().iter().collect();
        let observed = crate::listeners::processes_tcp_listener(&processes)?;
        if !self.contains(root)? {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "managed root exited",
            ));
        }
        for process in &processes {
            if !self.contains(process)? {
                return Err(io::Error::new(io::ErrorKind::NotFound, "job member exited"));
            }
        }
        // New or inaccessible members could own a listener outside this sample.
        // Only absence needs complete, current membership; a positive match can
        // survive churn once the retained members pass the checks above.
        // See [job-wide negatives](../docs/internals/processes.md#combine-address-families-then-check-job-membership).
        if observed == Absent && !self.is_current(&snapshot)? {
            return Ok(Truncated);
        }
        Ok(observed)
    }
    /// Takes ownership of a handle granting JOB_OBJECT_QUERY. Invalid handles
    /// fail kernel queries; no raw handle is exposed to the runtime.
    pub fn from_owned_handle(handle: OwnedHandle) -> Self {
        Self(handle)
    }

    pub fn contains(&self, process: &ProcessHandle) -> io::Result<bool> {
        Ok(process.is_alive()? && process.belongs_to_job(&self.0)? && process.is_alive()?)
    }

    /// Samples at most 128 members and retains each verified process object.
    /// An inaccessible/exited member or changing membership makes the snapshot
    /// incomplete. PID reuse cannot turn an unrelated process into a member.
    pub fn snapshot(&self) -> io::Result<JobProcessSnapshot> {
        let (pids, mut complete) = self.process_ids()?;
        let mut processes = Vec::with_capacity(pids.len());
        for pid in &pids {
            match ProcessHandle::open(*pid) {
                Ok(process) if self.contains(&process).unwrap_or(false) => processes.push(process),
                _ => complete = false,
            }
        }
        let (after, after_complete) = self.process_ids()?;
        complete &= after_complete && pids == after;
        for process in &processes {
            complete &= self.contains(process).unwrap_or(false);
        }
        Ok(JobProcessSnapshot {
            processes,
            complete,
        })
    }

    /// Rechecks retained identities and the current membership set. Call after
    /// collecting process facts; an earlier snapshot is not a lifetime promise.
    pub fn is_current(&self, snapshot: &JobProcessSnapshot) -> io::Result<bool> {
        if !snapshot.complete {
            return Ok(false);
        }
        let (pids, complete) = self.process_ids()?;
        let retained: Vec<_> = snapshot
            .processes
            .iter()
            .map(|p| p.identity().pid)
            .collect();
        if !complete || pids != retained {
            return Ok(false);
        }
        for process in &snapshot.processes {
            if !self.contains(process)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    #[allow(unsafe_code)]
    fn process_ids(&self) -> io::Result<(Vec<u32>, bool)> {
        use std::mem::{offset_of, size_of};
        use windows_sys::Win32::Foundation::ERROR_MORE_DATA;
        use windows_sys::Win32::System::JobObjects::{
            JOBOBJECT_BASIC_PROCESS_ID_LIST, JobObjectBasicProcessIdList, QueryInformationJobObject,
        };
        const OFFSET: usize = offset_of!(JOBOBJECT_BASIC_PROCESS_ID_LIST, ProcessIdList);
        let bytes = OFFSET + MAX_PROCESSES * size_of::<usize>();
        let mut storage = vec![0usize; bytes.div_ceil(size_of::<usize>())];
        // SAFETY: storage is ULONG_PTR-aligned and writable for the exact byte
        // bound; the retained handle is kernel-validated and no pointer escapes.
        if unsafe {
            QueryInformationJobObject(
                self.0.as_raw_handle(),
                JobObjectBasicProcessIdList,
                storage.as_mut_ptr().cast(),
                bytes as u32,
                std::ptr::null_mut(),
            )
        } == 0
        {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(ERROR_MORE_DATA as i32) {
                return Ok((Vec::new(), false));
            }
            return Err(error);
        }
        // SAFETY: successful query initialized this aligned header; storage is
        // large enough for the complete C struct including its first array slot.
        let info = unsafe { &*storage.as_ptr().cast::<JOBOBJECT_BASIC_PROCESS_ID_LIST>() };
        let count = info.NumberOfProcessIdsInList as usize;
        if count > MAX_PROCESSES || info.NumberOfAssignedProcesses < info.NumberOfProcessIdsInList {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid job process count",
            ));
        }
        let start = OFFSET / size_of::<usize>();
        let mut pids = storage[start..start + count]
            .iter()
            .map(|pid| {
                u32::try_from(*pid)
                    .ok()
                    .filter(|pid| *pid != 0)
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "invalid job process ID")
                    })
            })
            .collect::<io::Result<Vec<_>>>()?;
        pids.sort_unstable();
        if pids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "duplicate job process ID",
            ));
        }
        Ok((pids, count == info.NumberOfAssignedProcesses as usize))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_non_job_handle_cannot_produce_membership_evidence() {
        let observer = JobProcessObserver::from_owned_handle(tempfile::tempfile().unwrap().into());
        assert!(observer.snapshot().is_err());
        assert!(
            observer
                .contains(&ProcessHandle::open(std::process::id()).unwrap())
                .is_err()
        );
    }
}
