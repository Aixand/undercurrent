// SPDX-License-Identifier: Apache-2.0
//! Bounded host TCP ownership queries. This does not infer foreground ownership.
use std::io;
use std::mem::{offset_of, size_of};

use windows_sys::Win32::Foundation::{ERROR_INSUFFICIENT_BUFFER, NO_ERROR};
use windows_sys::Win32::NetworkManagement::IpHelper::{
    GetExtendedTcpTable, MIB_TCP_STATE_LISTEN, MIB_TCP6ROW_OWNER_MODULE,
    MIB_TCP6TABLE_OWNER_MODULE, MIB_TCPROW_OWNER_MODULE, MIB_TCPTABLE_OWNER_MODULE,
    TCP_TABLE_OWNER_MODULE_LISTENER,
};
use windows_sys::Win32::Networking::WinSock::{AF_INET, AF_INET6};

use crate::{ProcessHandle, ProcessIdentity};

const MAX_TABLE_BYTES: usize = 1024 * 1024;
const MAX_ROWS: usize = 8192;
const MAX_ATTEMPTS: usize = 3;

/// Bounded host TCP ownership evidence for retained processes or job members.
/// This is neither foreground ownership nor application readiness. Query errors
/// are reported separately through `io::Result`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TcpListenerObservation {
    /// At least one inspected listener matches a retained process identity.
    Present,
    /// Both address families were completely inspected without a match.
    Absent,
    /// A complete negative could not be established: collection was bounded,
    /// a matching row lacked a creation timestamp, or job membership was
    /// incomplete or changed. See the [evidence rules](../docs/internals/processes.md).
    Truncated,
}

#[derive(Clone, Copy)]
struct Layout {
    header_words: usize,
    row_words: usize,
    owner_word: usize,
    state_word: usize,
    created_word: usize,
}

const IPV4: Layout = Layout {
    header_words: offset_of!(MIB_TCPTABLE_OWNER_MODULE, table) / 4,
    row_words: size_of::<MIB_TCPROW_OWNER_MODULE>() / 4,
    owner_word: offset_of!(MIB_TCPROW_OWNER_MODULE, dwOwningPid) / 4,
    state_word: offset_of!(MIB_TCPROW_OWNER_MODULE, dwState) / 4,
    created_word: offset_of!(MIB_TCPROW_OWNER_MODULE, liCreateTimestamp) / 4,
};
const IPV6: Layout = Layout {
    header_words: offset_of!(MIB_TCP6TABLE_OWNER_MODULE, table) / 4,
    row_words: size_of::<MIB_TCP6ROW_OWNER_MODULE>() / 4,
    owner_word: offset_of!(MIB_TCP6ROW_OWNER_MODULE, dwOwningPid) / 4,
    state_word: offset_of!(MIB_TCP6ROW_OWNER_MODULE, dwState) / 4,
    created_word: offset_of!(MIB_TCP6ROW_OWNER_MODULE, liCreateTimestamp) / 4,
};

impl ProcessHandle {
    /// Samples IPv4 and IPv6 listeners while retaining this process object.
    /// Exit during collection is an error, never evidence about a reused PID.
    pub fn tcp_listener(&self) -> io::Result<TcpListenerObservation> {
        processes_tcp_listener(&[self])
    }
}

pub(crate) fn processes_tcp_listener(
    processes: &[&ProcessHandle],
) -> io::Result<TcpListenerObservation> {
    if processes.len() > 128 {
        return Ok(TcpListenerObservation::Truncated);
    }
    if processes.is_empty() {
        return Ok(TcpListenerObservation::Absent);
    }
    for process in processes {
        if !process.is_alive()? {
            return Err(io::Error::new(io::ErrorKind::NotFound, "process exited"));
        }
    }
    let mut pids: Vec<_> = processes.iter().map(|process| process.identity()).collect();
    pids.sort_unstable_by_key(|identity| identity.pid);
    pids.dedup();
    let v4 = family(&pids, u32::from(AF_INET), IPV4);
    let v6 = family(&pids, u32::from(AF_INET6), IPV6);
    for process in processes {
        if !process.is_alive()? {
            return Err(io::Error::new(io::ErrorKind::NotFound, "process exited"));
        }
    }
    combine(v4, v6)
}

// One verified listener proves presence even if the other family query failed.
// Absence needs two complete negatives, so errors and truncation remain visible
// whenever neither family supplies a positive match.
// See [family evidence](../docs/internals/processes.md#combine-address-families-then-check-job-membership).
fn combine(
    v4: io::Result<TcpListenerObservation>,
    v6: io::Result<TcpListenerObservation>,
) -> io::Result<TcpListenerObservation> {
    use TcpListenerObservation::{Absent, Present, Truncated};
    match (v4, v6) {
        (Ok(Present), _) | (_, Ok(Present)) => Ok(Present),
        (Err(error), _) | (_, Err(error)) => Err(error),
        (Ok(Truncated), _) | (_, Ok(Truncated)) => Ok(Truncated),
        (Ok(Absent), Ok(Absent)) => Ok(Absent),
    }
}

#[allow(unsafe_code)]
fn family(
    pids: &[ProcessIdentity],
    family: u32,
    layout: Layout,
) -> io::Result<TcpListenerObservation> {
    collect(pids, layout, |buffer, bytes| {
        // SAFETY: u64 storage satisfies the owner-module table/row alignment;
        // bytes advertises exactly its writable capacity. API retains no pointer.
        unsafe {
            GetExtendedTcpTable(
                buffer.as_mut_ptr().cast(),
                bytes,
                0,
                family,
                TCP_TABLE_OWNER_MODULE_LISTENER,
                0,
            )
        }
    })
}

fn collect(
    pids: &[ProcessIdentity],
    layout: Layout,
    mut query: impl FnMut(&mut [u64], &mut u32) -> u32,
) -> io::Result<TcpListenerObservation> {
    let mut capacity = 4096usize;
    for _ in 0..MAX_ATTEMPTS {
        let mut buffer = vec![0u64; capacity.div_ceil(8)];
        let mut bytes = (buffer.len() * 8) as u32;
        match query(&mut buffer, &mut bytes) {
            NO_ERROR => {
                if bytes as usize > buffer.len() * 8 || bytes < 4 || !bytes.is_multiple_of(4) {
                    return Err(invalid_table());
                }
                let words: Vec<u32> = buffer
                    .iter()
                    .flat_map(|word| [*word as u32, (*word >> 32) as u32])
                    .take(bytes as usize / 4)
                    .collect();
                return inspect(&words, pids, layout);
            }
            ERROR_INSUFFICIENT_BUFFER => {
                if bytes as usize > MAX_TABLE_BYTES {
                    return Ok(TcpListenerObservation::Truncated);
                }
                capacity = (bytes as usize).max(capacity.saturating_mul(2));
                if capacity > MAX_TABLE_BYTES {
                    return Ok(TcpListenerObservation::Truncated);
                }
            }
            error => return Err(io::Error::from_raw_os_error(error as i32)),
        }
    }
    Ok(TcpListenerObservation::Truncated)
}

fn invalid_table() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid TCP ownership table")
}

fn inspect(
    words: &[u32],
    pids: &[ProcessIdentity],
    layout: Layout,
) -> io::Result<TcpListenerObservation> {
    let count = *words.first().ok_or_else(invalid_table)? as usize;
    let end = count
        .checked_mul(layout.row_words)
        .and_then(|size| size.checked_add(layout.header_words))
        .filter(|end| *end <= words.len())
        .ok_or_else(invalid_table)?;
    let mut incomplete = count > MAX_ROWS;
    for row in words[layout.header_words..end]
        .chunks_exact(layout.row_words)
        .take(MAX_ROWS)
    {
        if row[layout.state_word] == MIB_TCP_STATE_LISTEN as u32
            && let Ok(index) =
                pids.binary_search_by_key(&row[layout.owner_word], |identity| identity.pid)
        {
            // A PID match can refer to a listener left by an earlier process.
            // Its bind timestamp must be at least this retained process's
            // creation time before the row can establish ownership.
            // See [row identity](../docs/internals/processes.md#match-rows-to-retained-process-identities).
            let created = u64::from(row[layout.created_word])
                | (u64::from(row[layout.created_word + 1]) << 32);
            if created == 0 {
                // Missing identity evidence prevents absence, but another row
                // within the scan bound can still prove an owned listener.
                incomplete = true;
                continue;
            }
            if created >= pids[index].created {
                return Ok(TcpListenerObservation::Present);
            }
        }
    }
    Ok(if incomplete {
        TcpListenerObservation::Truncated
    } else {
        TcpListenerObservation::Absent
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    const OWNER: ProcessIdentity = ProcessIdentity {
        pid: 42,
        created: 100,
    };
    const OTHER: ProcessIdentity = ProcessIdentity {
        pid: 43,
        created: 100,
    };

    #[test]
    fn both_address_families_match_only_listening_owned_rows() {
        for layout in [IPV4, IPV6] {
            let mut table = vec![0; layout.header_words + layout.row_words];
            table[0] = 1;
            table[layout.header_words + layout.owner_word] = 42;
            table[layout.header_words + layout.created_word] = 101;
            table[layout.header_words + layout.state_word] = MIB_TCP_STATE_LISTEN as u32;
            assert_eq!(
                inspect(&table, &[OWNER], layout).unwrap(),
                TcpListenerObservation::Present
            );
            assert_eq!(
                inspect(&table, &[OTHER], layout).unwrap(),
                TcpListenerObservation::Absent
            );
            assert_eq!(
                inspect(&table, &[OWNER, OTHER], layout).unwrap(),
                TcpListenerObservation::Present
            );
            let reused_pid = ProcessIdentity {
                pid: OWNER.pid,
                created: 102,
            };
            assert_eq!(
                inspect(&table, &[reused_pid], layout).unwrap(),
                TcpListenerObservation::Absent
            );
            table[layout.header_words + layout.created_word] = 0;
            assert_eq!(
                inspect(&table, &[OWNER], layout).unwrap(),
                TcpListenerObservation::Truncated
            );
            table[layout.header_words + layout.created_word] = 101;
            table[layout.header_words + layout.state_word] = 5; // ESTABLISHED
            assert_eq!(
                inspect(&table, &[OWNER], layout).unwrap(),
                TcpListenerObservation::Absent
            );
            table[0] = u32::MAX;
            assert!(inspect(&table, &[OWNER], layout).is_err());
        }
    }

    #[test]
    fn incomplete_rows_do_not_hide_later_verified_listeners() {
        for layout in [IPV4, IPV6] {
            for (created, expected) in [
                ([0, 101], TcpListenerObservation::Present),
                ([101, 0], TcpListenerObservation::Present),
                ([0, 99], TcpListenerObservation::Truncated),
                ([99, 0], TcpListenerObservation::Truncated),
                ([0, 0], TcpListenerObservation::Truncated),
            ] {
                let mut table = vec![0; layout.header_words + 2 * layout.row_words];
                table[0] = 2;
                for (index, created) in created.into_iter().enumerate() {
                    let base = layout.header_words + index * layout.row_words;
                    table[base + layout.owner_word] = OWNER.pid;
                    table[base + layout.state_word] = MIB_TCP_STATE_LISTEN as u32;
                    table[base + layout.created_word] = created;
                }
                assert_eq!(inspect(&table, &[OWNER], layout).unwrap(), expected);
            }
            let mut table = vec![0; layout.header_words + (MAX_ROWS + 1) * layout.row_words];
            table[0] = (MAX_ROWS + 1) as u32;
            for index in [0, MAX_ROWS] {
                let base = layout.header_words + index * layout.row_words;
                table[base + layout.owner_word] = OWNER.pid;
                table[base + layout.state_word] = MIB_TCP_STATE_LISTEN as u32;
                table[base + layout.created_word] = if index == 0 { 0 } else { 101 };
            }
            assert_eq!(
                inspect(&table, &[OWNER], layout).unwrap(),
                TcpListenerObservation::Truncated,
                "positive evidence beyond the scan limit must not be inspected"
            );
        }
    }

    #[test]
    fn growth_errors_and_row_limits_never_claim_absence() {
        let mut calls = 0;
        let result = collect(&[OWNER], IPV4, |_, bytes| {
            calls += 1;
            *bytes += 4;
            ERROR_INSUFFICIENT_BUFFER
        })
        .unwrap();
        assert_eq!(calls, MAX_ATTEMPTS);
        assert_eq!(result, TcpListenerObservation::Truncated);
        assert_eq!(
            collect(&[OWNER], IPV4, |_, bytes| {
                *bytes = (MAX_TABLE_BYTES + 4) as u32;
                ERROR_INSUFFICIENT_BUFFER
            })
            .unwrap(),
            TcpListenerObservation::Truncated
        );
        assert!(collect(&[OWNER], IPV4, |_, _| 5).is_err());
        assert!(
            collect(&[OWNER], IPV4, |_, bytes| {
                *bytes = u32::MAX;
                NO_ERROR
            })
            .is_err()
        );
        let mut table = vec![0; IPV4.header_words + (MAX_ROWS + 1) * IPV4.row_words];
        table[0] = (MAX_ROWS + 1) as u32;
        assert_eq!(
            inspect(&table, &[OWNER], IPV4).unwrap(),
            TcpListenerObservation::Truncated
        );
        table[IPV4.header_words + IPV4.owner_word] = 42;
        table[IPV4.header_words + IPV4.created_word] = 101;
        table[IPV4.header_words + IPV4.state_word] = MIB_TCP_STATE_LISTEN as u32;
        assert_eq!(
            inspect(&table, &[OWNER], IPV4).unwrap(),
            TcpListenerObservation::Present
        );
    }

    #[test]
    fn incomplete_families_preserve_positive_evidence_but_not_negative() {
        use TcpListenerObservation::{Absent, Present, Truncated};
        assert_eq!(combine(Ok(Present), Err(invalid_table())).unwrap(), Present);
        assert!(combine(Ok(Absent), Err(invalid_table())).is_err());
        assert_eq!(combine(Ok(Absent), Ok(Truncated)).unwrap(), Truncated);
        assert_eq!(combine(Ok(Absent), Ok(Absent)).unwrap(), Absent);
    }

    #[test]
    fn observes_real_ipv4_and_ipv6_listener_ownership() {
        let process = ProcessHandle::open(std::process::id()).unwrap();
        for (address, address_family, layout) in
            [("127.0.0.1:0", AF_INET, IPV4), ("[::1]:0", AF_INET6, IPV6)]
        {
            let listener = std::net::TcpListener::bind(address).unwrap();
            assert_eq!(
                family(&[process.identity()], u32::from(address_family), layout).unwrap(),
                TcpListenerObservation::Present
            );
            assert_eq!(
                process.tcp_listener().unwrap(),
                TcpListenerObservation::Present
            );
            drop(listener);
        }
    }

    #[test]
    fn unrelated_listeners_and_exited_processes_are_not_attributed() {
        use std::process::{Child, Command, Stdio};
        struct ChildGuard(Child);
        impl Drop for ChildGuard {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let _unrelated = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut child = ChildGuard(
            Command::new(crate::command_interpreter().unwrap())
                .args(["/D", "/Q", "/K"])
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let retained = ProcessHandle::open(child.0.id()).unwrap();
        assert_eq!(
            retained.tcp_listener().unwrap(),
            TcpListenerObservation::Absent
        );
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        assert_eq!(
            retained.tcp_listener().unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
    }
}
