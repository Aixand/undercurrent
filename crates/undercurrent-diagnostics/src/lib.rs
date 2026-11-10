// SPDX-License-Identifier: Apache-2.0

//! Private, bounded failure evidence shared by executable adapters.
//!
//! Records contain machine categories, never arbitrary error messages or panic
//! payloads. Collection and process lifecycle policy belong to the adapters.

use std::error::Error;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

pub const MAX_RECORD_BYTES: usize = 16 * 1024;
pub const RETAINED_RECORDS: usize = 128;
pub const MAX_BACKTRACE_BYTES: usize = 64 * 1024;
pub const RETAINED_BACKTRACES: usize = 8;
pub const INCIDENT_ENV: &str = "_UNDERCURRENT_INCIDENT_ID";
pub const REVISION: Option<&str> = option_env!("UC_BUILD_REVISION");

static PROCESS: OnceLock<Recorder> = OnceLock::new();
thread_local! { static REQUEST_INCIDENT: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) }; }

pub struct IncidentScope(Option<String>, std::marker::PhantomData<std::rc::Rc<()>>);

impl IncidentScope {
    /// Scoped to synchronous adapter execution; never hold this guard across an
    /// await that can migrate to another thread.
    pub fn enter(id: Option<&str>) -> Self {
        Self(
            REQUEST_INCIDENT
                .with(|slot| slot.replace(id.filter(|s| valid_incident(s)).map(str::to_owned))),
            std::marker::PhantomData,
        )
    }
}

impl Drop for IncidentScope {
    fn drop(&mut self) {
        REQUEST_INCIDENT.with(|slot| {
            slot.replace(self.0.take());
        });
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Component {
    Cli,
    Daemon,
    SessionHost,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IoCause {
    pub kind: String,
    pub os_code: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailureRecord {
    pub schema: String,
    pub incident_id: String,
    pub timestamp_millis: u64,
    pub component: Component,
    pub process_id: u32,
    pub version: String,
    pub build_revision: Option<String>,
    pub os: String,
    pub arch: String,
    pub stage: String,
    pub category: String,
    pub command: Option<String>,
    #[serde(default)]
    pub operation: Option<String>,
    #[serde(default)]
    pub duration_micros: Option<u64>,
    #[serde(default)]
    pub client_events: Vec<String>,
    #[serde(default)]
    pub dropped_client_events: Option<u64>,
    pub exit_code: Option<u8>,
    pub effect_certainty: Option<String>,
    pub io_causes: Vec<IoCause>,
    pub backtrace_available: bool,
}

impl FailureRecord {
    /// Checks the shareable vocabulary even when reading a local record written
    /// by an older or modified executable. Unknown fields are rejected by Serde.
    pub fn is_safe(&self) -> bool {
        self.schema == "undercurrent.failure/v0.1.0-beta"
            && valid_incident(&self.incident_id)
            && [&self.stage, &self.category, &self.os, &self.arch]
                .into_iter()
                .all(|s| safe_word(s))
            && self.command.as_deref().is_none_or(safe_word)
            && self.operation.as_deref().is_none_or(safe_word)
            && self.client_events.len() <= 128
            && self.client_events.iter().all(|s| {
                s.len() <= 128
                    && s.bytes()
                        .all(|c| c.is_ascii_alphanumeric() || b"_.".contains(&c))
            })
            && self.effect_certainty.as_deref().is_none_or(safe_word)
            && self.version.len() <= 32
            && self
                .version
                .bytes()
                .all(|c| c.is_ascii_digit() || c.is_ascii_lowercase() || c == b'.' || c == b'-')
            && self.version == env!("CARGO_PKG_VERSION")
            && self
                .build_revision
                .as_deref()
                .is_none_or(|s| s.len() <= 64 && s.bytes().all(|c| c.is_ascii_hexdigit()))
            && self.io_causes.len() <= 8
            && self.io_causes.iter().all(|c| safe_word(&c.kind))
    }
}

fn safe_word(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_')
}

pub fn valid_incident(value: &str) -> bool {
    value.len() == 35
        && value.starts_with("UC-")
        && value[3..].bytes().all(|c| c.is_ascii_hexdigit())
}

pub fn new_incident() -> String {
    let mut bytes = [0; 16];
    if getrandom::fill(&mut bytes).is_err() {
        // Diagnostic identity must not turn an entropy failure into a panic.
        // This fallback is only a support reference, never an authority token.
        static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let sequence = SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        static SEED: OnceLock<u64> = OnceLock::new();
        let seed = *SEED.get_or_init(|| {
            let time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            time as u64 ^ u64::from(std::process::id()).rotate_left(32)
        });
        let value = (u128::from(seed) << 64) | u128::from(sequence);
        bytes = value.to_be_bytes();
    }
    format!("UC-{:032x}", u128::from_be_bytes(bytes))
}

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

/// Extracts only typed OS evidence; Display and Debug of nested errors may
/// contain credentials, paths, YAML excerpts, or terminal content.
pub fn io_causes(mut error: &(dyn Error + 'static)) -> Vec<IoCause> {
    let mut causes = Vec::new();
    for _ in 0..16 {
        if let Some(io) = error.downcast_ref::<io::Error>() {
            if causes.len() < 8 {
                causes.push(IoCause {
                    kind: format!("{:?}", io.kind()),
                    os_code: io.raw_os_error(),
                });
            }
            if let Some(inner) = io.get_ref() {
                error = inner;
                continue;
            }
        }
        match error.source() {
            Some(source) => error = source,
            None => break,
        }
    }
    causes
}

#[derive(Debug, Clone)]
pub struct Recorder {
    directory: Option<PathBuf>,
    component: Component,
    version: &'static str,
    incident: String,
}

impl Recorder {
    /// Construction performs no filesystem writes, including for help/version.
    pub fn new(
        home: Option<&Path>,
        component: Component,
        version: &'static str,
        incident: Option<String>,
    ) -> Self {
        Self {
            directory: home.map(|p| p.join("logs/incidents")),
            component,
            version,
            incident: incident
                .filter(|s| valid_incident(s))
                .unwrap_or_else(new_incident),
        }
    }

    pub fn incident_id(&self) -> &str {
        &self.incident
    }

    pub fn record(
        &self,
        stage: &str,
        category: &str,
        error: Option<&(dyn Error + 'static)>,
    ) -> FailureRecord {
        FailureRecord {
            schema: "undercurrent.failure/v0.1.0-beta".into(),
            incident_id: REQUEST_INCIDENT
                .with(|slot| slot.borrow().clone())
                .unwrap_or_else(|| self.incident.clone()),
            timestamp_millis: now_millis(),
            component: self.component,
            process_id: std::process::id(),
            version: self.version.into(),
            build_revision: REVISION.map(str::to_owned),
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
            stage: stage.into(),
            category: category.into(),
            command: None,
            operation: None,
            duration_micros: None,
            client_events: Vec::new(),
            dropped_client_events: None,
            exit_code: None,
            effect_certainty: None,
            io_causes: error.map(io_causes).unwrap_or_default(),
            backtrace_available: false,
        }
    }

    /// Writes one independent record synchronously, outside the lossy log queue.
    /// Recording failure must never replace the original operation's result.
    /// Atomic publication prevents collectors from reading a half-written JSON
    /// document; concurrent writers may briefly exceed the retention count.
    pub fn save(&self, record: &FailureRecord) -> io::Result<()> {
        if !record.is_safe() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let directory = self.directory.as_ref().ok_or(io::ErrorKind::NotFound)?;
        prepare_private_directory(directory)?;
        let bytes = serde_json::to_vec(record)?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let name = format!("{:020}-{}.json", record.timestamp_millis, new_incident());
        write_private_new(&directory.join(name), &bytes)?;
        prune(directory, "json", RETAINED_RECORDS)?;
        Ok(())
    }

    /// Retains the last started binary's build identity independently of failures.
    /// Call only for actual execution, not for help/version probes.
    pub fn save_build(&self) -> io::Result<()> {
        let directory = self
            .directory
            .as_ref()
            .and_then(|p| p.parent())
            .ok_or(io::ErrorKind::NotFound)?;
        let component = match self.component {
            Component::Cli => "cli",
            Component::Daemon => "daemon",
            Component::SessionHost => "session-host",
        };
        let value = serde_json::json!({ "component": self.component, "version": self.version,
            "build_revision": REVISION, "os": std::env::consts::OS, "arch": std::env::consts::ARCH,
            "process_id": std::process::id(), "recorded_at_millis": now_millis() });
        write_private_replace(
            &directory.join(format!("build-{component}.json")),
            &serde_json::to_vec(&value)?,
        )
    }

    fn panic(&self) -> String {
        let mut record = self.record("panic", "panic", None);
        // Backtraces can contain build paths. Store them separately and require
        // an explicit collection flag; the panic payload is never formatted.
        if let Some(directory) = &self.directory {
            let result = (|| -> io::Result<()> {
                prepare_private_directory(directory)?;
                let mut trace = std::backtrace::Backtrace::force_capture().to_string();
                let mut end = trace.len().min(MAX_BACKTRACE_BYTES);
                while !trace.is_char_boundary(end) {
                    end -= 1;
                }
                trace.truncate(end);
                let name = format!("{:020}-{}.backtrace", now_millis(), record.incident_id);
                write_private_new(&directory.join(name), trace.as_bytes())?;
                prune(directory, "backtrace", RETAINED_BACKTRACES)
            })();
            record.backtrace_available = result.is_ok();
        }
        let _ = self.save(&record);
        record.incident_id
    }
}

/// Installs executable-owned panic capture once. Libraries do not install it
/// implicitly. The hook avoids the normal logger and never formats the payload.
pub fn install_process(recorder: Recorder) {
    if PROCESS.set(recorder).is_ok() {
        std::panic::set_hook(Box::new(|_| {
            if let Some(recorder) = PROCESS.get() {
                let incident = recorder.panic();
                let _ = writeln!(io::stderr(), "internal panic; error reference: {incident}");
            }
        }));
    }
}

pub fn process() -> Option<&'static Recorder> {
    PROCESS.get()
}

pub fn prepare_private_directory(path: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        undercurrent_platform_windows::prepare_private_directory(path).map(drop)
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
        if !path.is_absolute() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)?;
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.is_dir() || metadata.permissions().mode() & 0o077 != 0 {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
        // A process can read another user's public home. Failure evidence must
        // only be written into a directory it owns.
        if rustix::process::geteuid().as_raw() != metadata.uid() {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
        Ok(())
    }
}

pub fn write_private_replace(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path.parent().ok_or(io::ErrorKind::InvalidInput)?;
    prepare_private_directory(parent)?;
    let temporary = stage_private(parent, bytes)?;
    #[cfg(windows)]
    {
        undercurrent_platform_windows::replace_private_file(&temporary, path)
    }
    #[cfg(not(windows))]
    {
        temporary.persist(path).map(|_| ()).map_err(|e| e.error)
    }
}

pub fn write_private_new(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path.parent().ok_or(io::ErrorKind::InvalidInput)?;
    stage_private(parent, bytes)?
        .persist_noclobber(path)
        .map_err(|e| e.error)
}

fn stage_private(parent: &Path, bytes: &[u8]) -> io::Result<tempfile::TempPath> {
    #[cfg(not(windows))]
    {
        // NamedTempFile creates mode 0600 before any evidence is written.
        let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
        temporary.write_all(bytes)?;
        temporary.as_file().sync_data()?;
        Ok(temporary.into_temp_path())
    }
    #[cfg(windows)]
    {
        // The initial temporary file is empty. Establish the current-user ACL
        // before writing, including when the archive's parent is shared.
        // Elevated tokens can otherwise make Administrators the owner, which
        // the private-file verifier correctly rejects. Set ownership at creation.
        let temporary = undercurrent_platform_windows::with_current_user_file_owner(|| {
            tempfile::NamedTempFile::new_in(parent)
        })??
        .into_temp_path();
        let mut file =
            undercurrent_platform_windows::open_private_file(&temporary, true, false)?.into_file();
        file.write_all(bytes)?;
        file.sync_data()?;
        drop(file);
        Ok(temporary)
    }
}

pub fn read_bounded(path: &Path, maximum: usize) -> io::Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.len() > maximum as u64 {
        return Err(io::ErrorKind::InvalidData.into());
    }
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::OpenOptionsExt;
        fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?
    };
    #[cfg(windows)]
    let file = undercurrent_platform_windows::open_private_file(path, false, false)?.into_file();
    if !file.metadata()?.is_file() {
        return Err(io::ErrorKind::InvalidData.into());
    }
    let mut bytes = Vec::new();
    file.take(maximum as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        return Err(io::ErrorKind::InvalidData.into());
    }
    Ok(bytes)
}

fn prune(directory: &Path, extension: &str, maximum: usize) -> io::Result<()> {
    let mut entries = fs::read_dir(directory)?
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|s| s == extension))
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());
    let remove = entries.len().saturating_sub(maximum);
    for entry in entries.into_iter().take(remove) {
        let _ = fs::remove_file(entry.path());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn panic_capture_survives_without_a_log_worker_and_omits_the_payload() {
        if let Ok(home) = std::env::var("UC_TEST_PANIC_HOME") {
            install_process(Recorder::new(
                Some(Path::new(&home)),
                Component::Daemon,
                "0.1.0-beta",
                None,
            ));
            panic!("PRIVATE_PANIC_PAYLOAD");
        }
        let home = tempfile::tempdir().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::panic_capture_survives_without_a_log_worker_and_omits_the_payload",
                "--nocapture",
            ])
            .env("UC_TEST_PANIC_HOME", home.path())
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("PRIVATE_PANIC_PAYLOAD"));
        let directory = home.path().join("logs/incidents");
        let path = fs::read_dir(directory)
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.extension().is_some_and(|s| s == "json"))
            .unwrap();
        let record: FailureRecord = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        assert_eq!(record.category, "panic");
        assert!(record.backtrace_available);
    }

    #[test]
    fn records_retain_typed_causes_without_source_payloads() {
        let home = tempfile::tempdir().unwrap();
        let recorder = Recorder::new(Some(home.path()), Component::Cli, "0.1.0-beta", None);
        let error = io::Error::other("SECRET /private/path");
        let record = recorder.record("configuration", "invalid_argument", Some(&error));
        recorder.save(&record).unwrap();
        let directory = home.path().join("logs/incidents");
        let path = fs::read_dir(&directory)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let bytes = read_bounded(&path, MAX_RECORD_BYTES).unwrap();
        assert!(!String::from_utf8_lossy(&bytes).contains("SECRET"));
        assert_eq!(record.io_causes[0].kind, "Other");
        assert!(
            serde_json::from_slice::<FailureRecord>(&bytes)
                .unwrap()
                .is_safe()
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn retention_is_bounded_and_existing_files_are_not_replaced() {
        let home = tempfile::tempdir().unwrap();
        let recorder = Recorder::new(Some(home.path()), Component::Daemon, "0.1.0-beta", None);
        for _ in 0..RETAINED_RECORDS + 3 {
            recorder
                .save(&recorder.record("startup", "configuration", None))
                .unwrap();
        }
        assert_eq!(
            fs::read_dir(home.path().join("logs/incidents"))
                .unwrap()
                .count(),
            RETAINED_RECORDS
        );
        let path = home.path().join("existing");
        write_private_new(&path, b"original").unwrap();
        assert!(write_private_new(&path, b"replacement").is_err());
        assert_eq!(fs::read(path).unwrap(), b"original");
    }

    #[test]
    fn malformed_categories_and_oversized_files_are_rejected() {
        let home = tempfile::tempdir().unwrap();
        let recorder = Recorder::new(Some(home.path()), Component::Cli, "0.1.0-beta", None);
        assert!(
            recorder
                .save(&recorder.record("/private/path", "error", None))
                .is_err()
        );
        let path = home.path().join("large");
        fs::write(&path, [0; 20]).unwrap();
        assert!(read_bounded(&path, 10).is_err());
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&path, home.path().join("link")).unwrap();
            assert!(read_bounded(&home.path().join("link"), 30).is_err());
        }
    }
}
