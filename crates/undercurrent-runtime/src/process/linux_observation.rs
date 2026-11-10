// SPDX-License-Identifier: Apache-2.0

//! Shared bounded Linux `/proc` snapshot for foreground process-group observation.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{self, Read};
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use undercurrent_application::{
    ForegroundProcessGroupObservation, ForegroundProgramMember, PortError, PortErrorKind,
    PortOperation, ProcessGroupCompleteness, TcpListenerObservation,
};
use undercurrent_domain::{
    EnvironmentName, PathReference, ProcessInstanceId, ProfileMatchTarget, TimestampMillis,
};

use super::ManagedProcess;
use crate::RuntimeBoxedFailure;

const SNAPSHOT_FRESHNESS: Duration = Duration::from_millis(100);
const MAX_SNAPSHOT_PROCESSES: usize = 131_072;
const MAX_GROUP_MEMBERS: usize = 32;
const MAX_COMMAND_BYTES: usize = 16 * 1024;
const MAX_ARGUMENTS: usize = 64;
const MAX_ENVIRONMENT_BYTES: usize = 64 * 1024;
const MAX_ENVIRONMENT_NAMES: usize = 64;
const MAX_ENVIRONMENT_NAME_BYTES: usize = 256;
const MAX_CURRENT_DIRECTORY_BYTES: usize = 4 * 1024;
const MAX_FILE_DESCRIPTORS_PER_MEMBER: usize = 512;
const MAX_TCP_TABLE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug)]
pub(super) struct LinuxProcessObservationCache {
    state: Mutex<CacheState>,
}

#[derive(Debug)]
struct CacheState {
    refreshed_at: Option<Instant>,
    snapshot: Arc<LinuxProcessSnapshot>,
}

#[derive(Debug, Default)]
struct LinuxProcessSnapshot {
    groups: BTreeMap<u32, Vec<ProcessStat>>,
    listening_inodes: BTreeSet<u64>,
    complete: bool,
    listeners_available: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProcessStat {
    pid: u32,
    process_group: u32,
    start_ticks: u64,
}

impl LinuxProcessObservationCache {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(CacheState {
                refreshed_at: None,
                snapshot: Arc::new(LinuxProcessSnapshot::default()),
            }),
        }
    }

    pub(super) fn observe(
        &self,
        process: &ManagedProcess,
        process_id: &ProcessInstanceId,
        observed_at: TimestampMillis,
        cooperative: Option<undercurrent_domain::CooperativeForegroundState>,
    ) -> Result<ForegroundProcessGroupObservation, PortError> {
        let Some(process_group) = process.foreground_process_group() else {
            return Ok(ForegroundProcessGroupObservation::new(
                process_id.clone(),
                observed_at,
                Vec::new(),
                ProcessGroupCompleteness::Unavailable,
                current_directory(process.root_pid()),
                TcpListenerObservation::Unavailable,
                None,
            ));
        };
        let snapshot = self.snapshot().map_err(|source| {
            PortError::new(
                PortErrorKind::Unavailable,
                PortOperation::ObserveForegroundProcessGroup,
                None,
                RuntimeBoxedFailure::new(Box::new(source)),
            )
        })?;
        let Some(group) = snapshot.groups.get(&process_group) else {
            return Ok(ForegroundProcessGroupObservation::new(
                process_id.clone(),
                observed_at,
                Vec::new(),
                ProcessGroupCompleteness::Unavailable,
                current_directory(process.root_pid()),
                TcpListenerObservation::Unavailable,
                None,
            ));
        };
        let mut members = Vec::new();
        let mut complete = snapshot.complete;
        for stat in group.iter().take(MAX_GROUP_MEMBERS) {
            match program_member(*stat) {
                Ok(Some(member)) => members.push(member),
                Ok(None) | Err(_) => complete = false,
            }
        }
        if group.len() > MAX_GROUP_MEMBERS {
            complete = false;
        }
        let tcp_listener = listener_observation(group, &snapshot);
        // A retained ready event is valid only while the exact root shell is
        // still the sole foreground member. A child taking over the terminal
        // must not inherit its parent's readiness, even if no hook was emitted.
        let cooperative = cooperative.filter(|state| {
            *state != undercurrent_domain::CooperativeForegroundState::Ready
                || (complete
                    && group.len() == 1
                    && group[0].pid == process.root_pid()
                    && Some(group[0].start_ticks) == process.root_start_ticks())
        });
        Ok(ForegroundProcessGroupObservation::new(
            process_id.clone(),
            observed_at,
            members,
            if complete {
                ProcessGroupCompleteness::Complete
            } else {
                ProcessGroupCompleteness::Truncated
            },
            current_directory(process.root_pid()),
            tcp_listener,
            cooperative,
        ))
    }

    fn snapshot(&self) -> io::Result<Arc<LinuxProcessSnapshot>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state
            .refreshed_at
            .is_some_and(|refreshed| refreshed.elapsed() <= SNAPSHOT_FRESHNESS)
        {
            return Ok(Arc::clone(&state.snapshot));
        }
        let snapshot = Arc::new(refresh_snapshot()?);
        state.refreshed_at = Some(Instant::now());
        state.snapshot = Arc::clone(&snapshot);
        Ok(snapshot)
    }
}

fn refresh_snapshot() -> io::Result<LinuxProcessSnapshot> {
    let mut groups = BTreeMap::<u32, Vec<ProcessStat>>::new();
    let mut complete = true;
    let mut seen = 0_usize;
    for entry in fs::read_dir("/proc")? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => {
                complete = false;
                continue;
            }
        };
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        if seen >= MAX_SNAPSHOT_PROCESSES {
            complete = false;
            break;
        }
        seen += 1;
        let Ok(Some(bytes)) = read_bounded(entry.path().join("stat"), 16 * 1024) else {
            continue;
        };
        if let Some(stat) = parse_process_stat(pid, &bytes) {
            groups.entry(stat.process_group).or_default().push(stat);
        }
    }
    for members in groups.values_mut() {
        members.sort_by_key(|member| (member.pid, member.start_ticks));
    }
    let (listening_inodes, listeners_available) = read_listening_inodes();
    Ok(LinuxProcessSnapshot {
        groups,
        listening_inodes,
        complete,
        listeners_available,
    })
}

fn parse_process_stat(expected_pid: u32, bytes: &[u8]) -> Option<ProcessStat> {
    let text = std::str::from_utf8(bytes).ok()?;
    let open = text.find('(')?;
    let close = text.rfind(')')?;
    let pid = text[..open].trim().parse::<u32>().ok()?;
    if pid != expected_pid || close <= open {
        return None;
    }
    let fields = text[close + 1..].split_whitespace().collect::<Vec<_>>();
    Some(ProcessStat {
        pid,
        process_group: fields.get(2)?.parse().ok()?,
        start_ticks: fields.get(19)?.parse().ok()?,
    })
}

fn program_member(stat: ProcessStat) -> io::Result<Option<ForegroundProgramMember>> {
    let proc = PathBuf::from("/proc").join(stat.pid.to_string());
    let Some(current_stat) = read_bounded(proc.join("stat"), 16 * 1024)? else {
        return Ok(None);
    };
    if parse_process_stat(stat.pid, &current_stat) != Some(stat) {
        return Ok(None);
    }
    let Some(cmdline) = read_bounded(proc.join("cmdline"), MAX_COMMAND_BYTES)? else {
        return Ok(None);
    };
    let mut fields = cmdline
        .split(|byte| *byte == 0)
        .filter(|field| !field.is_empty());
    let argv0 = fields
        .next()
        .and_then(|value| std::str::from_utf8(value).ok());
    let executable = fs::read_link(proc.join("exe"))
        .ok()
        .filter(|path| path.as_os_str().as_bytes().len() <= MAX_COMMAND_BYTES)
        .and_then(|path| path.to_str().map(str::to_owned));
    let command_name = read_bounded(proc.join("comm"), 4 * 1024)
        .ok()
        .flatten()
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .map(|text| text.trim_end().to_owned());
    let Some(program) = preferred_program(argv0, executable, command_name) else {
        return Ok(None);
    };
    let arguments = fields
        .take(MAX_ARGUMENTS + 1)
        .map(|value| std::str::from_utf8(value).map(str::to_owned))
        .collect::<Result<Vec<_>, _>>()
        .ok();
    let Some(arguments) = arguments.filter(|arguments| arguments.len() <= MAX_ARGUMENTS) else {
        return Ok(None);
    };
    let Some(environment) = read_bounded(proc.join("environ"), MAX_ENVIRONMENT_BYTES)? else {
        return Ok(None);
    };
    let (environment_names, environment_names_complete) = environment
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
        .take(MAX_ENVIRONMENT_NAMES + 1)
        .map(|entry| {
            let separator = entry.iter().position(|byte| *byte == b'=')?;
            std::str::from_utf8(&entry[..separator])
                .ok()
                .filter(|name| !name.is_empty())
                .and_then(|name| EnvironmentName::new(name, MAX_ENVIRONMENT_NAME_BYTES).ok())
        })
        .collect::<Option<Vec<_>>>()
        .map(|names| {
            let complete = names.len() <= MAX_ENVIRONMENT_NAMES;
            (
                names
                    .into_iter()
                    .take(MAX_ENVIRONMENT_NAMES)
                    .collect::<BTreeSet<_>>(),
                complete,
            )
        })
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid environment name"))?;
    let target = ProfileMatchTarget::new(
        program,
        arguments,
        environment_names,
        MAX_COMMAND_BYTES,
        MAX_ARGUMENTS,
        MAX_ENVIRONMENT_NAMES,
    )
    .ok();
    Ok(target.map(|target| {
        let member =
            ForegroundProgramMember::new(format!("{}:{}", stat.pid, stat.start_ticks), target);
        if environment_names_complete {
            member
        } else {
            member.with_truncated_environment_names()
        }
    }))
}

fn current_directory(pid: u32) -> Option<PathReference> {
    let path = fs::read_link(format!("/proc/{pid}/cwd")).ok()?;
    let value = path.to_str()?;
    PathReference::new(value, MAX_CURRENT_DIRECTORY_BYTES).ok()
}

/// Selects the invocation identity before the resolved image path because
/// version-managed native launchers commonly expose only a version as the image basename.
fn preferred_program(
    argv0: Option<&str>,
    executable: Option<String>,
    command_name: Option<String>,
) -> Option<String> {
    argv0
        .map(str::to_owned)
        .or(executable)
        .or(command_name)
        .filter(|program| !program.is_empty())
}

fn read_listening_inodes() -> (BTreeSet<u64>, bool) {
    let mut inodes = BTreeSet::new();
    let mut available = true;
    for path in ["/proc/net/tcp", "/proc/net/tcp6"] {
        match read_bounded(path, MAX_TCP_TABLE_BYTES) {
            Ok(Some(bytes)) => parse_listening_inodes(&bytes, &mut inodes),
            Ok(None) | Err(_) => available = false,
        }
    }
    (inodes, available)
}

fn parse_listening_inodes(bytes: &[u8], inodes: &mut BTreeSet<u64>) {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return;
    };
    for line in text.lines().skip(1) {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        if fields.get(3) == Some(&"0A")
            && let Some(inode) = fields.get(9).and_then(|value| value.parse().ok())
        {
            inodes.insert(inode);
        }
    }
}

fn listener_observation(
    group: &[ProcessStat],
    snapshot: &LinuxProcessSnapshot,
) -> TcpListenerObservation {
    if !snapshot.listeners_available {
        return TcpListenerObservation::Unavailable;
    }
    let mut truncated = false;
    for member in group.iter().take(MAX_GROUP_MEMBERS) {
        let entries = match fs::read_dir(format!("/proc/{}/fd", member.pid)) {
            Ok(entries) => entries,
            Err(_) => return TcpListenerObservation::Unavailable,
        };
        for (index, entry) in entries.enumerate() {
            if index >= MAX_FILE_DESCRIPTORS_PER_MEMBER {
                truncated = true;
                break;
            }
            let Ok(entry) = entry else {
                return TcpListenerObservation::Unavailable;
            };
            let Ok(target) = fs::read_link(entry.path()) else {
                continue;
            };
            let Some(inode) = socket_inode(target.as_os_str().as_bytes()) else {
                continue;
            };
            if snapshot.listening_inodes.contains(&inode) {
                return TcpListenerObservation::Present;
            }
        }
    }
    if truncated || group.len() > MAX_GROUP_MEMBERS {
        TcpListenerObservation::Truncated
    } else {
        TcpListenerObservation::Absent
    }
}

fn socket_inode(value: &[u8]) -> Option<u64> {
    let value = value.strip_prefix(b"socket:[")?.strip_suffix(b"]")?;
    std::str::from_utf8(value).ok()?.parse().ok()
}

fn read_bounded(path: impl Into<PathBuf>, limit: usize) -> io::Result<Option<Vec<u8>>> {
    let mut bytes = Vec::new();
    File::open(path.into())?
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)?;
    Ok((bytes.len() <= limit).then_some(bytes))
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use super::*;

    #[test]
    fn stat_parser_handles_spaces_and_closing_parentheses_in_comm() {
        let line = b"42 (name with ) paren) S 1 42 42 0 -1 0 0 0 0 0 0 0 0 0 0 0 0 0 987 0 0";
        let parsed = parse_process_stat(42, line).expect("stat");
        assert_eq!(parsed.process_group, 42);
        assert_eq!(parsed.start_ticks, 987);
    }

    #[test]
    fn listener_parser_and_socket_target_are_exact() {
        let table = b"sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n0: 0100007F:1234 00000000:0000 0A 0:0 0:0 00:0 0 0 12345\n";
        let mut inodes = BTreeSet::new();
        parse_listening_inodes(table, &mut inodes);
        assert!(inodes.contains(&12345));
        assert_eq!(socket_inode(b"socket:[12345]"), Some(12345));
        assert_eq!(socket_inode(b"pipe:[12345]"), None);
    }

    #[test]
    fn invocation_name_precedes_a_versioned_native_executable_path() {
        assert_eq!(
            preferred_program(
                Some("claude"),
                Some("/home/user/.local/share/claude/versions/2.1.229".to_owned()),
                Some("2.1.229".to_owned()),
            )
            .as_deref(),
            Some("claude")
        );
        assert_eq!(
            preferred_program(None, Some("/usr/bin/python3".to_owned()), None).as_deref(),
            Some("/usr/bin/python3")
        );
    }

    #[test]
    fn oversized_environment_keeps_program_evidence_and_marks_names_truncated() {
        let mut child = Command::new("sleep")
            .arg("30")
            .env_clear()
            .envs((0..66).map(|index| (format!("UC_TEST_{index:02}"), "1")))
            .spawn()
            .expect("spawn bounded child");
        let observed = (|| {
            let pid = child.id();
            let stat = read_bounded(format!("/proc/{pid}/stat"), 16 * 1024)?
                .and_then(|bytes| parse_process_stat(pid, &bytes))
                .ok_or_else(|| io::Error::other("child stat unavailable"))?;
            program_member(stat)?.ok_or_else(|| io::Error::other("child member unavailable"))
        })();
        let _ = child.kill();
        let _ = child.wait();

        let member = observed.expect("program evidence remains available");
        assert_eq!(member.target().basename(), "sleep");
        assert_eq!(
            member.target().environment_names().len(),
            MAX_ENVIRONMENT_NAMES
        );
        assert!(!member.environment_names_complete());
    }
}
