// SPDX-License-Identifier: Apache-2.0
//! Bounded Darwin foreground observation. Kernel membership is never cached.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::ManagedProcess;
use undercurrent_application::{
    ForegroundProcessGroupObservation, ForegroundProgramMember, PortError,
    ProcessGroupCompleteness, TcpListenerObservation,
};
use undercurrent_domain::{
    CooperativeForegroundState, EnvironmentName, PathReference, ProcessInstanceId,
    ProfileMatchTarget, TimestampMillis,
};
use undercurrent_platform_macos::{self as os, ProcessIdentity};

const MAX_COMMAND_BYTES: usize = 16 * 1024;
const MAX_ARGUMENTS: usize = 64;
const MAX_ENVIRONMENT_BYTES: usize = 64 * 1024;
const MAX_ENVIRONMENT_NAMES: usize = 64;
const MAX_PATH_BYTES: usize = 4096;
const MAX_CACHED_MEMBERS: usize = 128;
const CACHE_FRESHNESS: Duration = Duration::from_millis(100);

trait ProcessSource {
    fn identity(&self, pid: u32) -> io::Result<ProcessIdentity>;
    fn group(&self, pgid: u32) -> io::Result<(Vec<u32>, bool)>;
    fn member(&self, identity: ProcessIdentity) -> io::Result<ForegroundProgramMember>;
    fn cwd(&self, pid: u32) -> Option<PathReference>;
    fn listener(&self, _pid: u32) -> TcpListenerObservation {
        TcpListenerObservation::Unavailable
    }
}
struct Darwin;
impl ProcessSource for Darwin {
    fn listener(&self, pid: u32) -> TcpListenerObservation {
        match os::tcp_listener(pid) {
            Ok(os::TcpListener::Present) => TcpListenerObservation::Present,
            Ok(os::TcpListener::Absent) => TcpListenerObservation::Absent,
            Ok(os::TcpListener::Truncated) => TcpListenerObservation::Truncated,
            Err(_) => TcpListenerObservation::Unavailable,
        }
    }

    fn identity(&self, pid: u32) -> io::Result<ProcessIdentity> {
        os::identity(pid)
    }
    fn group(&self, pgid: u32) -> io::Result<(Vec<u32>, bool)> {
        os::group_members(pgid)
    }
    fn member(&self, identity: ProcessIdentity) -> io::Result<ForegroundProgramMember> {
        program_member(identity)
    }
    fn cwd(&self, pid: u32) -> Option<PathReference> {
        os::working_directory(pid).ok().and_then(|path| {
            path.to_str()
                .and_then(|text| PathReference::new(text, MAX_PATH_BYTES).ok())
        })
    }
}

struct CachedMember {
    identity: ProcessIdentity,
    sampled: Instant,
    member: ForegroundProgramMember,
}

pub(super) struct MacosProcessObservationCache {
    // Serialize collection and cap retained command facts, without retaining raw
    // argument buffers or environment values. Errors never enter the cache.
    members: Mutex<BTreeMap<u32, CachedMember>>,
}

impl MacosProcessObservationCache {
    pub(super) fn new() -> Self {
        Self {
            members: Mutex::new(BTreeMap::new()),
        }
    }

    pub(super) fn observe(
        &self,
        process: &ManagedProcess,
        process_id: &ProcessInstanceId,
        observed_at: TimestampMillis,
        cooperative: Option<CooperativeForegroundState>,
    ) -> Result<ForegroundProcessGroupObservation, PortError> {
        Ok(self.collect(
            &Darwin,
            process.root_pid(),
            process.root_start_ticks(),
            process_id,
            observed_at,
            cooperative,
            || process.foreground_process_group(),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn collect(
        &self,
        source: &impl ProcessSource,
        root: u32,
        root_start: Option<u64>,
        process_id: &ProcessInstanceId,
        observed_at: TimestampMillis,
        cooperative: Option<CooperativeForegroundState>,
        foreground: impl Fn() -> Option<u32>,
    ) -> ForegroundProcessGroupObservation {
        let unavailable = || {
            ForegroundProcessGroupObservation::new(
                process_id.clone(),
                observed_at,
                Vec::new(),
                ProcessGroupCompleteness::Unavailable,
                None,
                TcpListenerObservation::Unavailable,
                None,
            )
        };
        let Ok(root_before) = source.identity(root) else {
            return unavailable();
        };
        if Some(root_before.start_micros) != root_start {
            return unavailable();
        }
        let Some(group) = foreground().filter(|group| *group > 0) else {
            return unavailable();
        };
        let Ok((pids, mut complete)) = source.group(group) else {
            return unavailable();
        };
        if pids.is_empty() {
            return unavailable();
        }
        let mut cache = self
            .members
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        cache.retain(|_, cached| cached.sampled.elapsed() <= CACHE_FRESHNESS);
        let mut members = Vec::new();
        let mut identities = Vec::new();
        for pid in &pids {
            let Ok(before) = source.identity(*pid) else {
                complete = false;
                continue;
            };
            if before.group != group || before.uid != root_before.uid {
                complete = false;
                continue;
            }
            // A persistent Ready event always requires fresh command facts as
            // well as fresh membership, including after exec in the root PID.
            let cached = cache.get(pid).filter(|cached| {
                cached.identity == before && cooperative != Some(CooperativeForegroundState::Ready)
            });
            let member = match cached {
                Some(cached) => Ok(cached.member.clone()),
                None => source.member(before),
            };
            let Ok(member) = member else {
                complete = false;
                continue;
            };
            if source.identity(*pid).ok() != Some(before) {
                complete = false;
                continue;
            }
            if cache.len() >= MAX_CACHED_MEMBERS
                && !cache.contains_key(pid)
                && let Some(oldest) = cache
                    .iter()
                    .min_by_key(|(_, value)| value.sampled)
                    .map(|(pid, _)| *pid)
            {
                cache.remove(&oldest);
            }
            // Preserve original sample age on a cache hit; reads cannot refresh it.
            if cached_is_stale(cache.get(pid), before, cooperative) {
                cache.insert(
                    *pid,
                    CachedMember {
                        identity: before,
                        sampled: Instant::now(),
                        member: member.clone(),
                    },
                );
            }
            identities.push(before);
            members.push(member);
        }
        let cwd = source.cwd(root);
        let mut listener = TcpListenerObservation::Absent;
        for pid in &pids {
            match source.listener(*pid) {
                TcpListenerObservation::Unavailable => {
                    listener = TcpListenerObservation::Unavailable;
                    break;
                }
                TcpListenerObservation::Truncated => {
                    listener = TcpListenerObservation::Truncated;
                    break;
                }
                TcpListenerObservation::Present => listener = TcpListenerObservation::Present,
                TcpListenerObservation::Absent => {}
            }
        }

        // Re-enumerate: a pipeline can grow while its first member is sampled.
        let Ok((after, after_complete)) = source.group(group) else {
            return unavailable();
        };
        if foreground() != Some(group)
            || after != pids
            || source.identity(root).ok() != Some(root_before)
        {
            return unavailable();
        }
        complete &= after_complete;
        for identity in &identities {
            if source.identity(identity.pid).ok() != Some(*identity) {
                complete = false;
            }
        }
        let cooperative = cooperative.filter(|state| {
            *state != CooperativeForegroundState::Ready
                || ready_root(complete, &identities, &members, root_before)
        });
        ForegroundProcessGroupObservation::new(
            process_id.clone(),
            observed_at,
            members,
            if complete {
                ProcessGroupCompleteness::Complete
            } else {
                ProcessGroupCompleteness::Truncated
            },
            cwd,
            if complete {
                listener
            } else {
                TcpListenerObservation::Unavailable
            },
            cooperative,
        )
    }
}

fn cached_is_stale(
    cached: Option<&CachedMember>,
    identity: ProcessIdentity,
    cooperative: Option<CooperativeForegroundState>,
) -> bool {
    cached.is_none_or(|cached| cached.identity != identity)
        || cooperative == Some(CooperativeForegroundState::Ready)
}

fn ready_root(
    complete: bool,
    identities: &[ProcessIdentity],
    members: &[ForegroundProgramMember],
    root: ProcessIdentity,
) -> bool {
    complete
        && identities == [root]
        && members.len() == 1
        && matches!(
            members[0].target().basename().trim_start_matches('-'),
            "zsh" | "bash" | "fish" | "sh"
        )
}

fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "incomplete Darwin command evidence",
    )
}

fn program_member(identity: ProcessIdentity) -> io::Result<ForegroundProgramMember> {
    let buffer = os::arguments(identity.pid)?;
    let (target, environment_complete) = parse_arguments(buffer.as_bytes())?;
    let member = ForegroundProgramMember::new(
        format!("{}:{}", identity.pid, identity.start_micros),
        target,
    );
    Ok(if environment_complete {
        member
    } else {
        member.with_truncated_environment_names()
    })
}

fn cstring<'a>(bytes: &mut &'a [u8]) -> io::Result<&'a [u8]> {
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(invalid)?;
    let value = &bytes[..end];
    *bytes = &bytes[end + 1..];
    Ok(value)
}

/// Parse argc, executable path/padding, exactly argc arguments, then environment.
/// Only names leave this function. Empty positional arguments are preserved.
fn parse_arguments(bytes: &[u8]) -> io::Result<(ProfileMatchTarget, bool)> {
    let argc = i32::from_ne_bytes(
        bytes
            .get(..4)
            .ok_or_else(invalid)?
            .try_into()
            .map_err(|_| invalid())?,
    );
    if !(1..=MAX_ARGUMENTS as i32 + 1).contains(&argc) {
        return Err(invalid());
    }
    let mut bytes = &bytes[4..];
    if cstring(&mut bytes)?.len() > MAX_PATH_BYTES {
        return Err(invalid());
    }
    while bytes.first() == Some(&0) {
        bytes = &bytes[1..];
    }
    let mut args = Vec::new();
    let mut command_bytes = 0_usize;
    for _ in 0..argc {
        let arg = cstring(&mut bytes)?;
        command_bytes = command_bytes.checked_add(arg.len()).ok_or_else(invalid)?;
        if command_bytes > MAX_COMMAND_BYTES {
            return Err(invalid());
        }
        args.push(std::str::from_utf8(arg).map_err(|_| invalid())?.to_owned());
    }
    let program = args.remove(0);
    if program.is_empty() {
        return Err(invalid());
    }
    let mut names = BTreeSet::new();
    let mut complete = true;
    let mut env_bytes = 0_usize;
    let mut entries = 0_usize;
    while !bytes.is_empty() && bytes[0] != 0 {
        let Ok(entry) = cstring(&mut bytes) else {
            complete = false;
            break;
        };
        env_bytes = env_bytes.saturating_add(entry.len() + 1);
        entries += 1;
        if env_bytes > MAX_ENVIRONMENT_BYTES || entries > MAX_ENVIRONMENT_NAMES {
            complete = false;
            break;
        }
        let name = entry
            .iter()
            .position(|byte| *byte == b'=')
            .and_then(|end| std::str::from_utf8(&entry[..end]).ok())
            .and_then(|name| EnvironmentName::new(name, 256).ok());
        match name {
            Some(name) => {
                names.insert(name);
            }
            None => {
                complete = false;
            }
        }
    }
    // Nonzero data after an environment terminator is not a complete projection.
    if bytes.iter().any(|byte| *byte != 0) {
        complete = false;
    }
    let target = ProfileMatchTarget::new(
        program,
        args,
        names,
        MAX_COMMAND_BYTES,
        MAX_ARGUMENTS,
        MAX_ENVIRONMENT_NAMES,
    )
    .map_err(|_| invalid())?;
    Ok((target, complete))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn raw(args: &[&[u8]], env: &[&[u8]]) -> Vec<u8> {
        let mut bytes = (args.len() as i32).to_ne_bytes().to_vec();
        bytes.extend_from_slice(b"/a/versioned-image\0\0\0");
        for value in args.iter().chain(env.iter()) {
            bytes.extend_from_slice(value);
            bytes.push(0);
        }
        bytes.push(0);
        bytes
    }

    #[test]
    fn arguments_keep_invocation_empty_arguments_and_only_environment_names() {
        let bytes = raw(
            &[b"codex", b"", b"two words", b""],
            &[b"TOKEN=secret=value", b"PATH=/bin"],
        );
        let (target, complete) = parse_arguments(&bytes).unwrap();
        assert_eq!(target.basename(), "codex");
        assert_eq!(target.arguments(), &["", "two words", ""]);
        assert_eq!(
            target
                .environment_names()
                .iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>(),
            vec!["PATH", "TOKEN"]
        );
        assert!(complete);
    }

    #[test]
    fn malformed_and_oversized_commands_cannot_be_complete_evidence() {
        for bytes in [
            vec![],
            vec![0; 4],
            (-1_i32).to_ne_bytes().to_vec(),
            raw(&vec![b"x".as_slice(); MAX_ARGUMENTS + 2], &[]),
            raw(&[b"codex", &vec![b'x'; MAX_COMMAND_BYTES]], &[]),
        ] {
            assert!(parse_arguments(&bytes).is_err());
        }
        let mut bytes = raw(&[b"codex"], &[]);
        bytes.truncate(bytes.len() - 2);
        assert!(parse_arguments(&bytes).is_err());
    }

    #[test]
    fn environment_overflow_and_invalid_names_keep_command_but_mark_incomplete() {
        let strings = (0..65)
            .map(|index| format!("VAR_{index}=private"))
            .collect::<Vec<_>>();
        let refs = strings
            .iter()
            .map(|value| value.as_bytes())
            .collect::<Vec<_>>();
        let (target, complete) = parse_arguments(&raw(&[b"codex"], &refs)).unwrap();
        assert!(!complete);
        assert_eq!(target.basename(), "codex");
        assert_eq!(target.environment_names().len(), 64);
        assert!(!parse_arguments(&raw(&[b"codex"], &[b"=bad"])).unwrap().1);
        let mut incomplete = raw(&[b"codex"], &[b"TOKEN=private"]);
        incomplete.truncate(incomplete.len() - 2);
        assert!(!parse_arguments(&incomplete).unwrap().1);
    }

    fn identity(pid: u32) -> ProcessIdentity {
        ProcessIdentity {
            pid,
            parent: 1,
            group: 42,
            uid: 501,
            start_micros: 100 + u64::from(pid),
            image_name: [0; 32],
        }
    }
    fn member(name: &str) -> ForegroundProgramMember {
        ForegroundProgramMember::new(
            "test",
            parse_arguments(&raw(&[name.as_bytes()], &[])).unwrap().0,
        )
    }
    #[derive(Default)]
    struct Fake {
        child: bool,
        truncated: bool,
        refused: bool,
        reused: bool,
        changed_group: bool,
        exec: bool,
        calls: Cell<usize>,
        group_calls: Cell<usize>,
        member_calls: Cell<usize>,
    }
    impl ProcessSource for Fake {
        fn identity(&self, pid: u32) -> io::Result<ProcessIdentity> {
            let mut info = identity(pid);
            let call = self.calls.get();
            self.calls.set(call + 1);
            if self.reused && call > 1 {
                info.start_micros += 1;
            }
            Ok(info)
        }
        fn group(&self, _: u32) -> io::Result<(Vec<u32>, bool)> {
            let call = self.group_calls.get();
            self.group_calls.set(call + 1);
            Ok((
                if self.child || (self.changed_group && call > 0) {
                    vec![42, 43]
                } else {
                    vec![42]
                },
                !self.truncated,
            ))
        }
        fn member(&self, info: ProcessIdentity) -> io::Result<ForegroundProgramMember> {
            self.member_calls.set(self.member_calls.get() + 1);
            if self.refused {
                return Err(io::Error::from(io::ErrorKind::PermissionDenied));
            }
            Ok(member(if info.pid == 42 && !self.exec {
                "zsh"
            } else {
                "codex"
            }))
        }
        fn cwd(&self, _: u32) -> Option<PathReference> {
            None
        }
        fn listener(&self, _: u32) -> TcpListenerObservation {
            TcpListenerObservation::Present
        }
    }
    fn observe(
        cache: &MacosProcessObservationCache,
        fake: &Fake,
        state: Option<CooperativeForegroundState>,
    ) -> ForegroundProcessGroupObservation {
        cache.collect(
            fake,
            42,
            Some(142),
            &ProcessInstanceId::new("p-test").unwrap(),
            TimestampMillis::new(1),
            state,
            || Some(42),
        )
    }

    #[test]
    fn readiness_requires_live_complete_same_root_shell() {
        let cache = MacosProcessObservationCache::new();
        let ready = Some(CooperativeForegroundState::Ready);
        assert_eq!(
            observe(&cache, &Fake::default(), ready).cooperative(),
            ready
        );
        for fake in [
            Fake {
                child: true,
                ..Fake::default()
            },
            Fake {
                truncated: true,
                ..Fake::default()
            },
            Fake {
                refused: true,
                ..Fake::default()
            },
            Fake {
                reused: true,
                ..Fake::default()
            },
            Fake {
                changed_group: true,
                ..Fake::default()
            },
            Fake {
                exec: true,
                ..Fake::default()
            },
        ] {
            assert_eq!(observe(&cache, &fake, ready).cooperative(), None);
        }
    }

    #[test]
    fn listener_evidence_requires_complete_stable_process_ownership() {
        let cache = MacosProcessObservationCache::new();
        assert_eq!(
            observe(&cache, &Fake::default(), None).tcp_listener(),
            TcpListenerObservation::Present
        );
        for fake in [
            Fake {
                refused: true,
                ..Fake::default()
            },
            Fake {
                truncated: true,
                ..Fake::default()
            },
            Fake {
                reused: true,
                ..Fake::default()
            },
            Fake {
                changed_group: true,
                ..Fake::default()
            },
        ] {
            assert_eq!(
                // Each case must actually query its failing source, not reuse
                // valid command facts cached by a different fake source.
                observe(&MacosProcessObservationCache::new(), &fake, None).tcp_listener(),
                TcpListenerObservation::Unavailable
            );
        }
    }

    #[test]
    fn cached_command_never_refreshes_age_or_certifies_ready() {
        let cache = MacosProcessObservationCache::new();
        let fake = Fake::default();
        observe(&cache, &fake, None);
        let first = cache.members.lock().unwrap()[&42].sampled;
        observe(&cache, &fake, None);
        assert_eq!(fake.member_calls.get(), 1);
        assert_eq!(cache.members.lock().unwrap()[&42].sampled, first);
        observe(&cache, &fake, Some(CooperativeForegroundState::Ready));
        assert_eq!(fake.member_calls.get(), 2);
    }

    #[test]
    fn group_switch_during_collection_is_unavailable() {
        let cache = MacosProcessObservationCache::new();
        let reads = Cell::new(0);
        let result = cache.collect(
            &Fake::default(),
            42,
            Some(142),
            &ProcessInstanceId::new("p-test").unwrap(),
            TimestampMillis::new(1),
            Some(CooperativeForegroundState::Ready),
            || {
                let call = reads.get();
                reads.set(call + 1);
                Some(if call == 0 { 42 } else { 43 })
            },
        );
        assert_eq!(result.completeness(), ProcessGroupCompleteness::Unavailable);
        assert_eq!(result.cooperative(), None);
    }
}
