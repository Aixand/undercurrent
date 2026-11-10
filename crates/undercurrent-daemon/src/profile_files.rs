// SPDX-License-Identifier: Apache-2.0

//! Bounded profile snapshots, non-overwriting installation, and atomic live activation.
use crate::{
    DaemonBackgroundTask, DaemonBackgroundTaskError, DaemonWorkerCycle,
    blocking::block_on,
    profile_builtins::{EMBEDDED, built_in_detection_profiles},
    profile_yaml::{MAX_PROFILE_BYTES, ProfileResult, decode},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
};
use undercurrent_application::{
    ApplicationErrorCategory, Clock, EffectiveProfileDirectory, ForegroundObservationWake,
    ProfilePatternEngine, SessionProcessDirectory, SessionRecoverySource,
};
use undercurrent_domain::{DetectionProfile, DetectionProfileLimits};
use undercurrent_runtime::InMemoryDetectionProfileCatalog;

const MAX_FILES: usize = 128;
const MAX_DIRECTORY_ENTRIES: usize = 1024;
const MAX_TOTAL_BYTES: usize = 8 * 1024 * 1024;
type Snapshot = BTreeMap<PathBuf, String>;
pub(crate) type ProfileGate = Arc<RwLock<()>>;

#[derive(Debug)]
struct ReloadFailure {
    stage: &'static str,
    source: Box<dyn std::error::Error + Send + Sync>,
}

impl std::fmt::Display for ReloadFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(f)
    }
}
impl std::error::Error for ReloadFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}
fn reload_failure(
    stage: &'static str,
    source: Box<dyn std::error::Error + Send + Sync>,
) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(ReloadFailure { stage, source })
}

pub(crate) fn seed(directory: &Path) -> ProfileResult<()> {
    fs::create_dir_all(directory)?;
    for (name, text) in EMBEDDED {
        let path = directory.join(name);
        if fs::symlink_metadata(&path).is_ok() {
            continue;
        }
        let mut temporary = tempfile::NamedTempFile::new_in(directory)?;
        temporary.write_all(text.as_bytes())?;
        temporary.as_file().sync_all()?;
        match temporary.persist_noclobber(&path) {
            Ok(_) => {}
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.error.into()),
        }
    }
    Ok(())
}

fn snapshot(directory: &Path) -> ProfileResult<Snapshot> {
    let mut files = BTreeMap::new();
    let mut total = 0usize;
    for (index, entry) in fs::read_dir(directory)?.enumerate() {
        if index >= MAX_DIRECTORY_ENTRIES {
            return Err("profile directory exceeds 1024 entries".into());
        }
        let entry = entry?;
        let path = entry.path();
        if !matches!(
            path.extension().and_then(|s| s.to_str()),
            Some("yaml" | "yml")
        ) {
            continue;
        }
        if entry.file_type()?.is_dir() {
            continue;
        }
        if files.len() >= MAX_FILES {
            return Err("profile directory exceeds 128 YAML files".into());
        }
        let result = (|| -> ProfileResult<String> {
            if !entry.file_type()?.is_file() {
                return Err("profile must be a regular file, not a symlink".into());
            }
            let mut options = OpenOptions::new();
            options.read(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
            }
            let file = options.open(&path)?;
            if !file.metadata()?.is_file() {
                return Err("profile must be a regular file".into());
            }
            let mut source = String::new();
            file.take((MAX_PROFILE_BYTES + 1) as u64)
                .read_to_string(&mut source)?;
            if source.len() > MAX_PROFILE_BYTES {
                return Err("profile exceeds 256 KiB".into());
            }
            Ok(source)
        })();
        let source = result.map_err(|e| format!("{}: {e}", entry.file_name().to_string_lossy()))?;
        total += source.len();
        if total > MAX_TOTAL_BYTES {
            return Err("profile directory exceeds 8 MiB".into());
        }
        files.insert(entry.file_name().into(), source);
    }
    Ok(files)
}

fn definitions(
    snapshot: &Snapshot,
    limits: DetectionProfileLimits,
    patterns: &dyn ProfilePatternEngine,
) -> ProfileResult<Vec<DetectionProfile>> {
    snapshot
        .iter()
        .map(|(path, source)| {
            let profile = decode(source, limits)
                .map_err(|e| reload_failure("decode", format!("{}: {e}", path.display()).into()))?;
            lint(&profile, patterns).map_err(|e| {
                reload_failure(
                    "pattern_validation",
                    format!("{}: {e}", path.display()).into(),
                )
            })?;
            Ok(profile)
        })
        .collect()
}

fn lint(profile: &DetectionProfile, patterns: &dyn ProfilePatternEngine) -> ProfileResult<()> {
    let report = undercurrent_application::lint_detection_profile(profile, patterns);
    if let Some(issue) = report
        .issues()
        .iter()
        .find(|issue| issue.severity() == undercurrent_application::ProfileIssueSeverity::Error)
    {
        return Err(format!(
            "profile {}: {:?} at {:?}",
            profile.name(),
            issue.code(),
            issue.location()
        )
        .into());
    }
    Ok(())
}

fn embedded(
    limits: DetectionProfileLimits,
    patterns: &dyn ProfilePatternEngine,
) -> ProfileResult<Vec<DetectionProfile>> {
    let profiles = built_in_detection_profiles(limits)?;
    for profile in &profiles {
        lint(profile, patterns)?;
    }
    Ok(profiles)
}

struct ReloadState {
    active: Snapshot,
    pending: Option<Snapshot>,
    last_error: Option<String>,
    last_rejection_at_millis: Option<u64>,
}

/// Context used under the activation gate to protect live references and refresh observations.
pub(crate) struct ProfileSessions {
    pub recovery: Arc<dyn SessionRecoverySource>,
    pub processes: Arc<dyn SessionProcessDirectory>,
    pub effective: Arc<dyn EffectiveProfileDirectory>,
    pub wake: Arc<dyn ForegroundObservationWake>,
    pub clock: Arc<dyn Clock>,
    pub max_sessions: usize,
    pub batch_size: usize,
}

pub(crate) struct ProfileReload {
    directory: PathBuf,
    limits: DetectionProfileLimits,
    catalog: Arc<InMemoryDetectionProfileCatalog>,
    patterns: Arc<dyn ProfilePatternEngine>,
    pub gate: ProfileGate,
    state: Mutex<ReloadState>,
    sessions: ProfileSessions,
}

impl ProfileReload {
    pub fn start(
        directory: PathBuf,
        limits: DetectionProfileLimits,
        catalog: Arc<InMemoryDetectionProfileCatalog>,
        patterns: Arc<dyn ProfilePatternEngine>,
        sessions: ProfileSessions,
    ) -> ProfileResult<Self> {
        seed(&directory)?;
        let active = snapshot(&directory)?;
        if snapshot(&directory)? != active {
            return Err("profiles changed during startup; retry startup".into());
        }
        catalog.replace(
            embedded(limits, patterns.as_ref())?,
            definitions(&active, limits, patterns.as_ref())?,
            patterns.as_ref(),
        )?;
        tracing::info!(target: "undercurrent.profiles", generation = catalog.pressure().generation.value(), "YAML profile catalog loaded");
        let reload = Self {
            directory,
            limits,
            catalog,
            patterns,
            sessions,
            gate: Arc::new(RwLock::new(())),
            state: Mutex::new(ReloadState {
                active,
                pending: None,
                last_error: None,
                last_rejection_at_millis: None,
            }),
        };
        reload.write_status("active", "startup", false, None);
        Ok(reload)
    }

    fn write_status(&self, status: &str, stage: &str, previous: bool, last_rejection: Option<u64>) {
        // Offline collection must distinguish the running catalog from a
        // rejected edit. Publishing this sidecar never changes reload success.
        // Only the product-owned profiles directory has an adjacent log owner.
        if self
            .directory
            .file_name()
            .is_none_or(|name| name != "profiles")
        {
            return;
        }
        let Some(home) = self.directory.parent() else {
            return;
        };
        let record = serde_json::json!({
            "observed_at_millis": undercurrent_diagnostics::now_millis(),
            "active_generation": self.catalog.pressure().generation.value(),
            "previous_definition_active": previous,
            "stage": stage, "status": status,
            "last_rejection_at_millis": last_rejection,
        });
        if let Ok(bytes) = serde_json::to_vec(&record) {
            let _ = undercurrent_diagnostics::write_private_replace(
                &home.join("logs/profile-reload.json"),
                &bytes,
            );
        }
    }

    fn tick(&self) -> ProfileResult<bool> {
        let candidate = match snapshot(&self.directory) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.state.lock().unwrap_or_else(|p| p.into_inner()).pending = None;
                return Err(reload_failure("snapshot", error));
            }
        };
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if candidate == state.active {
            state.pending = None;
            if state.last_error.is_some() {
                self.write_status("active", "restored", false, state.last_rejection_at_millis);
            }
            state.last_error = None;
            return Ok(false);
        }
        if state.pending.as_ref() != Some(&candidate) {
            state.pending = Some(candidate);
            self.write_status(
                "pending",
                "stabilizing",
                true,
                state.last_rejection_at_millis,
            );
            return Ok(false);
        }
        let builtins = embedded(self.limits, self.patterns.as_ref())?;
        let user = definitions(&candidate, self.limits, self.patterns.as_ref())?;
        // Lint outside the activation gate, preserving availability while parsing/compiling.
        let validation =
            InMemoryDetectionProfileCatalog::new(self.catalog.pressure().profile_limit)?;
        validation
            .replace(builtins.clone(), user.clone(), self.patterns.as_ref())
            .map_err(|error| reload_failure("catalog_validation", Box::new(error)))?;
        let names: BTreeSet<_> = builtins
            .iter()
            .chain(user.iter())
            .map(|p| p.name().clone())
            .collect();
        // Never wait behind a long-running request or prevent prompt shutdown. Retry next cycle.
        let _activation = match self.gate.try_write() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(false),
            Err(std::sync::TryLockError::Poisoned(error)) => {
                return Err(format!("profile activation gate poisoned: {error}").into());
            }
        };
        if snapshot(&self.directory)? != candidate {
            state.pending = None;
            return Ok(false);
        }
        let mut cursor = None;
        let mut live = Vec::new();
        let mut count = 0usize;
        loop {
            let batch = block_on(
                self.sessions
                    .recovery
                    .read_batch(cursor.as_ref(), self.sessions.batch_size),
            )?;
            for session in batch.sessions {
                count += 1;
                if count > self.sessions.max_sessions {
                    return Err("session scan exceeds configured capacity".into());
                }
                // Exit settlement stores terminal state and restart intent atomically.
                // Keep the fixed profile available through the backoff interval too.
                if !session.lifecycle().is_active()
                    && !matches!(
                        session.restart_status(),
                        undercurrent_domain::RestartStatus::Scheduled { .. }
                    )
                {
                    continue;
                }
                let fixed = (session.configuration().profile_binding()
                    == undercurrent_domain::ProfileBindingMode::Fixed)
                    .then(|| session.profile())
                    .flatten();
                let effective = self
                    .sessions
                    .processes
                    .lookup(session.id())?
                    .map(|process| self.sessions.effective.get(session.id(), &process.id))
                    .transpose()?
                    .flatten();
                let selected = effective.as_ref().and_then(|p| p.selection().profile());
                for name in fixed.into_iter().chain(selected) {
                    if !names.contains(name) {
                        return Err(format!(
                            "cannot remove profile {name}: referenced by live session {}",
                            session.id()
                        )
                        .into());
                    }
                }
                live.push(session.id().clone());
            }
            match batch.next_after {
                None => break,
                Some(next) if cursor.as_ref().is_none_or(|previous| &next > previous) => {
                    cursor = Some(next)
                }
                Some(_) => return Err("profile session scan cursor did not advance".into()),
            }
        }
        // Keep observation readers excluded through wake scheduling and replacement,
        // so successful activation lets them sample the new catalog. If a wake or
        // replacement fails, earlier wakes remain queued against the retained catalog.
        // See [activation boundaries](../docs/internals/configuration/profile-reload.md#snapshot-and-activation-protocol).
        for id in live {
            self.sessions
                .wake
                .wake_foreground(&id, self.sessions.clock.now())?;
        }
        let activation = self
            .catalog
            .replace(builtins, user, self.patterns.as_ref())?;
        state.active = candidate;
        state.pending = None;
        state.last_error = None;
        let last_rejection = state.last_rejection_at_millis;
        drop(_activation);
        drop(state);
        self.write_status("active", "activation", false, last_rejection);
        tracing::info!(target: "undercurrent.profiles", generation = activation.generation().value(), profiles = activation.profiles(), "YAML profiles reloaded");
        Ok(true)
    }
}

impl DaemonBackgroundTask for ProfileReload {
    fn execute(&self) -> Result<DaemonWorkerCycle, DaemonBackgroundTaskError> {
        match self.tick() {
            Ok(changed) => Ok(DaemonWorkerCycle::processed(usize::from(changed), false)),
            Err(error) => {
                let message = error.to_string();
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                if state.last_error.as_ref() != Some(&message) {
                    let stage = error
                        .downcast_ref::<ReloadFailure>()
                        .map_or("activation", |error| error.stage);
                    state.last_rejection_at_millis = Some(undercurrent_diagnostics::now_millis());
                    self.write_status("rejected", stage, true, state.last_rejection_at_millis);
                    if let Some(recorder) = undercurrent_diagnostics::process() {
                        let mut record =
                            recorder.record(stage, "profile_reload_rejected", Some(error.as_ref()));
                        record.incident_id = undercurrent_diagnostics::new_incident();
                        let _ = recorder.save(&record);
                    }
                    tracing::warn!(target: "undercurrent.profiles", error = %message, "profile reload rejected; retaining active catalog");
                    state.last_error = Some(message);
                }
                Err(DaemonBackgroundTaskError::new(
                    ApplicationErrorCategory::InvalidArgument,
                    std::io::Error::other(error),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile_builtins::tests::limits;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    use undercurrent_application::*;
    use undercurrent_domain::*;
    use undercurrent_runtime::{
        InMemoryProcessDirectory, RegexPatternEngine, RegexPatternEngineLimits,
    };

    #[test]
    fn rejected_disk_edit_is_visible_without_replacing_the_active_catalog() {
        let home = tempfile::tempdir().unwrap();
        let directory = home.path().join("profiles");
        let (reload, _) = start(&directory);
        let original_generation = generation(&reload);
        let path = directory.join("codex.yaml");
        let original = fs::read_to_string(&path).unwrap();
        fs::write(&path, "schema_version: 99\nSECRET: [\n").unwrap();
        reload.execute().unwrap();
        assert!(reload.execute().is_err());
        assert_eq!(generation(&reload), original_generation);
        let evidence_path = home.path().join("logs/profile-reload.json");
        let bytes = fs::read(&evidence_path).unwrap();
        assert!(!String::from_utf8_lossy(&bytes).contains("SECRET"));
        let rejected: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(rejected["status"], "rejected");
        assert_eq!(rejected["stage"], "decode");
        assert_eq!(rejected["previous_definition_active"], true);
        fs::write(path, original).unwrap();
        reload.execute().unwrap();
        let restored: serde_json::Value =
            serde_json::from_slice(&fs::read(evidence_path).unwrap()).unwrap();
        assert_eq!(restored["status"], "active");
        assert_eq!(restored["previous_definition_active"], false);
        assert_eq!(
            restored["last_rejection_at_millis"],
            rejected["last_rejection_at_millis"]
        );
    }

    #[derive(Default)]
    struct Sessions {
        entries: Mutex<Vec<Session>>,
        wakes: AtomicUsize,
        effective: Mutex<Option<EffectiveProfileSnapshot>>,
    }
    impl SessionRecoverySource for Sessions {
        fn read_batch<'a>(
            &'a self,
            after: Option<&'a SessionId>,
            limit: usize,
        ) -> PortFuture<'a, Result<RecoverySessionBatch, PortError>> {
            Box::pin(async move {
                let entries = self.entries.lock().unwrap();
                let mut sessions: Vec<_> = entries
                    .iter()
                    .filter(|s| after.is_none_or(|a| s.id() > a))
                    .take(limit + 1)
                    .cloned()
                    .collect();
                let next_after = if sessions.len() > limit {
                    sessions.pop();
                    sessions.last().map(|s| s.id().clone())
                } else {
                    None
                };
                Ok(RecoverySessionBatch {
                    sessions,
                    next_after,
                })
            })
        }
    }
    impl EffectiveProfileDirectory for Sessions {
        fn get(
            &self,
            _: &SessionId,
            _: &ProcessInstanceId,
        ) -> Result<Option<EffectiveProfileSnapshot>, PortError> {
            Ok(self.effective.lock().unwrap().clone())
        }
        fn transition(
            &self,
            _: &SessionId,
            _: EffectiveProfileTransition,
        ) -> Result<Option<EffectiveProfileSnapshot>, PortError> {
            Ok(None)
        }
    }
    impl Clock for Sessions {
        fn now(&self) -> TimestampMillis {
            TimestampMillis::new(10)
        }
    }
    impl ForegroundObservationWake for Sessions {
        fn wake_foreground(
            &self,
            _: &SessionId,
            _: TimestampMillis,
        ) -> Result<Option<ObservationScheduleOutcome>, PortError> {
            self.wakes.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }
    }
    fn start(path: &Path) -> (ProfileReload, Arc<Sessions>) {
        let (reload, sessions) = try_start(path);
        (reload.unwrap(), sessions)
    }
    fn try_start(path: &Path) -> (ProfileResult<ProfileReload>, Arc<Sessions>) {
        let sessions = Arc::new(Sessions::default());
        let reload = ProfileReload::start(
            path.into(),
            limits(),
            Arc::new(InMemoryDetectionProfileCatalog::new(32).unwrap()),
            Arc::new(
                RegexPatternEngine::new(RegexPatternEngineLimits {
                    cached_patterns: 256,
                    compiled_size_bytes: 1_048_576,
                    determinize_size_bytes: 2_097_152,
                    match_text_bytes: 131_072,
                })
                .unwrap(),
            ),
            ProfileSessions {
                recovery: sessions.clone(),
                effective: sessions.clone(),
                processes: Arc::new(InMemoryProcessDirectory::default()),
                wake: sessions.clone(),
                clock: sessions.clone(),
                max_sessions: 32,
                batch_size: 8,
            },
        );
        (reload, sessions)
    }
    fn codex() -> &'static str {
        EMBEDDED.iter().find(|(n, _)| *n == "codex.yaml").unwrap().1
    }
    fn generation(reload: &ProfileReload) -> u64 {
        reload.catalog.pressure().generation.value()
    }
    fn settle(reload: &ProfileReload) -> ProfileResult<bool> {
        assert!(!reload.tick()?);
        reload.tick()
    }

    #[test]
    fn invalid_startup_names_the_file_and_preserves_it_for_repair() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("codex.yaml");
        fs::write(&path, "schema_version: 999\nname: codex\narchetype: animated_agent\ninput_delivery: {type: immediate}\n").unwrap();
        let (result, _) = try_start(dir.path());
        let error = result.err().unwrap().to_string();
        assert!(
            error.contains("codex.yaml") && error.contains("0.1.0-beta"),
            "{error}"
        );
        assert!(fs::read_to_string(&path).unwrap().contains("999"));
        fs::write(&path, codex()).unwrap();
        assert!(try_start(dir.path()).0.is_ok());
    }

    #[test]
    fn seeding_is_concurrent_safe_and_never_overwrites() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("codex.yaml"), "customized").unwrap();
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| seed(dir.path()).unwrap());
            }
        });
        assert_eq!(
            fs::read_to_string(dir.path().join("codex.yaml")).unwrap(),
            "customized"
        );
        assert_eq!(snapshot(dir.path()).unwrap().len(), EMBEDDED.len());
        assert_eq!(
            fs::read_dir(dir.path()).unwrap().count(),
            EMBEDDED.len(),
            "temporary files cleaned up"
        );
    }
    #[test]
    fn reload_is_atomic_recovers_and_preserves_unchanged_generations() {
        let dir = tempfile::tempdir().unwrap();
        let (reload, _) = start(dir.path());
        assert!(!reload.tick().unwrap());
        let path = dir.path().join("codex.yaml");
        let changed = codex().replace("delay_millis: 120", "delay_millis: 240");
        fs::write(&path, &changed).unwrap();
        assert!(settle(&reload).unwrap());
        assert_eq!(generation(&reload), 2);
        let entry = reload
            .catalog
            .get(&ProfileName::new("codex").unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(
            entry.profile().input_delivery().trailing_submit_delay(),
            Some(Duration::from_millis(240))
        );
        assert!(!reload.tick().unwrap());
        fs::write(
            &path,
            changed.replace("schema_version: 0.1.0-beta", "schema_version: 9"),
        )
        .unwrap();
        assert!(settle(&reload).is_err());
        assert_eq!(generation(&reload), 2);
        fs::write(
            &path,
            changed.replace("delay_millis: 240", "delay_millis: 360"),
        )
        .unwrap();
        assert!(settle(&reload).unwrap());
        assert_eq!(generation(&reload), 3);
        // Invalid regex is rejected by catalog linting, after YAML/domain validation.
        fs::write(
            &path,
            codex().replace("(?m)needs your approval", "[invalid"),
        )
        .unwrap();
        assert!(settle(&reload).is_err());
        assert_eq!(generation(&reload), 3);
        fs::write(&path, &changed).unwrap();
        fs::write(dir.path().join("duplicate.yml"), &changed).unwrap();
        assert!(settle(&reload).is_err());
        assert_eq!(generation(&reload), 3);
        fs::remove_file(dir.path().join("duplicate.yml")).unwrap();
        assert!(settle(&reload).unwrap());
        fs::remove_file(&path).unwrap();
        assert!(settle(&reload).unwrap());
        assert_eq!(
            reload
                .catalog
                .get(&ProfileName::new("codex").unwrap())
                .unwrap()
                .unwrap()
                .source(),
            ProfileSourceKind::BuiltIn
        );
    }
    #[test]
    fn partial_writes_and_directory_loss_preserve_active_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("profiles");
        let (reload, _) = start(&path);
        fs::write(path.join("codex.yaml"), "partial:").unwrap();
        assert!(!reload.tick().unwrap());
        fs::write(path.join("codex.yaml"), codex()).unwrap();
        assert!(!reload.tick().unwrap());
        let displaced = dir.path().join("displaced");
        fs::rename(&path, &displaced).unwrap();
        assert!(reload.tick().is_err());
        assert_eq!(generation(&reload), 1);
        fs::rename(&displaced, &path).unwrap();
        assert!(!reload.tick().unwrap());
        let tmp = path.join("save.tmp");
        fs::write(
            &tmp,
            codex().replace("delay_millis: 120", "delay_millis: 121"),
        )
        .unwrap();
        fs::rename(&tmp, path.join("codex.yaml")).unwrap();
        assert!(settle(&reload).unwrap());
    }
    #[test]
    fn activation_waits_for_operations_and_protects_live_references() {
        let dir = tempfile::tempdir().unwrap();
        let (reload, sessions) = start(dir.path());
        let custom = dir.path().join("custom.yaml");
        fs::write(&custom, codex().replace("name: codex", "name: custom")).unwrap();
        let operation = reload.gate.read().unwrap();
        assert!(!settle(&reload).unwrap());
        assert_eq!(generation(&reload), 1);
        drop(operation);
        assert!(reload.tick().unwrap());
        sessions.entries.lock().unwrap().push(session("custom"));
        fs::remove_file(&custom).unwrap();
        let error = settle(&reload).unwrap_err().to_string();
        assert!(error.contains("referenced by live session"), "{error}");
        assert_eq!(generation(&reload), 2);
        sessions.entries.lock().unwrap().clear();
        assert!(
            reload.tick().unwrap(),
            "unchanged rejected snapshots retry after references disappear"
        );
        sessions.entries.lock().unwrap().push(session("codex"));
        fs::write(
            dir.path().join("codex.yaml"),
            codex().replace("delay_millis: 120", "delay_millis: 121"),
        )
        .unwrap();
        assert!(settle(&reload).unwrap());
        assert_eq!(sessions.wakes.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn automatic_restart_backoff_keeps_its_fixed_profile() {
        let dir = tempfile::tempdir().unwrap();
        let (reload, sessions) = start(dir.path());
        let path = dir.path().join("custom.yaml");
        fs::write(&path, codex().replace("name: codex", "name: custom")).unwrap();
        assert!(settle(&reload).unwrap());
        let mut state = session("custom").state();
        state.lifecycle = LifecycleState::Exited(1);
        state.restart_tracker = RestartTracker::new(1);
        state.restart_status = RestartStatus::Scheduled {
            attempt: 1,
            not_before: TimestampMillis::new(100),
        };
        sessions
            .entries
            .lock()
            .unwrap()
            .push(Session::rehydrate(state).unwrap());
        fs::remove_file(&path).unwrap();
        assert!(
            settle(&reload)
                .unwrap_err()
                .to_string()
                .contains("referenced by live session")
        );
    }

    #[test]
    fn adaptive_effective_profile_is_protected_independently_of_base_profile() {
        let dir = tempfile::tempdir().unwrap();
        let (reload, sessions) = start(dir.path());
        let path = dir.path().join("custom.yaml");
        fs::write(&path, codex().replace("name: codex", "name: custom")).unwrap();
        assert!(settle(&reload).unwrap());
        let mut state = session("codex").state();
        state
            .configuration
            .set_profile_binding(ProfileBindingMode::ForegroundAdaptive);
        let session = Session::rehydrate(state).unwrap();
        let process = ProcessInstanceId::new("adaptive-process").unwrap();
        reload
            .sessions
            .processes
            .bind(
                session.id(),
                ProcessInstance {
                    id: process.clone(),
                    started_at: TimestampMillis::new(1),
                },
            )
            .unwrap();
        *sessions.effective.lock().unwrap() = Some(EffectiveProfileSnapshot::new(
            process,
            EffectiveProfileEpoch::INITIAL,
            EffectiveProfileSelection::Profile(ProfileName::new("custom").unwrap()),
            EffectiveProfileOrigin::ForegroundProcess,
            TimestampMillis::new(1),
        ));
        sessions.entries.lock().unwrap().push(session);
        fs::remove_file(&path).unwrap();
        assert!(
            settle(&reload)
                .unwrap_err()
                .to_string()
                .contains("referenced by live session")
        );
        *sessions.effective.lock().unwrap() = None;
        assert!(reload.tick().unwrap());
    }

    #[test]
    fn snapshot_rejects_symlinks_and_oversized_files() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("large.yaml"),
            vec![b' '; MAX_PROFILE_BYTES + 1],
        )
        .unwrap();
        assert!(snapshot(dir.path()).is_err());
        fs::remove_file(dir.path().join("large.yaml")).unwrap();
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("missing", dir.path().join("linked.yaml")).unwrap();
            assert!(snapshot(dir.path()).is_err());
        }
    }
    fn session(profile: &str) -> Session {
        let limits = ExecutionLimits {
            max_arguments: 32,
            max_command_bytes: 8192,
            max_environment_entries: 32,
            max_environment_bytes: 16384,
            max_path_bytes: 1024,
            max_health_rules: 16,
            max_health_rule_bytes: 2048,
        };
        let configuration = SessionConfiguration::new(
            LaunchSpec::new(
                CommandSpec::new("sh", vec![], limits).unwrap(),
                EnvironmentVariables::new(vec![], limits).unwrap(),
                None,
                None,
                TerminalSize::new(80, 24, 500, 200).unwrap(),
            ),
            ExecutionPolicy::new(
                RestartConfig::never(),
                StopPolicy::new(StopSignal::Terminate, Duration::from_secs(1)).unwrap(),
                true,
                false,
            ),
            HealthRules::new(vec![], limits).unwrap(),
            Some(ProfileName::new(profile).unwrap()),
            Labels::default(),
            Metadata::default(),
        );
        Session::create(
            SessionId::new("live").unwrap(),
            SessionName::new("live").unwrap(),
            configuration,
            8,
            TimestampMillis::new(1),
        )
        .unwrap()
        .0
    }
}
