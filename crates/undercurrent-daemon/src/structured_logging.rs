// SPDX-License-Identifier: Apache-2.0

//! Bounded process-local structured logging adapters.

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
#[cfg(not(windows))]
use std::fs::OpenOptions;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use tracing_appender::non_blocking::{ErrorCounter, NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::filter::LevelFilter;

/// Validated hard bounds for one process JSONL log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StructuredLogSettings {
    max_file_bytes: u64,
    retained_files: usize,
    queue_lines: usize,
}

impl StructuredLogSettings {
    /// Creates settings from positive hard bounds.
    ///
    /// This constructor performs no IO and does not panic. Callers retain
    /// ownership of product policy such as a minimum useful record size.
    ///
    /// # Errors
    ///
    /// Returns the exact zero-valued setting that would make retention or the
    /// nonblocking writer ambiguous.
    pub const fn new(
        max_file_bytes: u64,
        retained_files: usize,
        queue_lines: usize,
    ) -> Result<Self, StructuredLogSettingsError> {
        if max_file_bytes == 0 {
            return Err(StructuredLogSettingsError::MaxFileBytes);
        }
        if retained_files == 0 {
            return Err(StructuredLogSettingsError::RetainedFiles);
        }
        if queue_lines == 0 {
            return Err(StructuredLogSettingsError::QueueLines);
        }
        Ok(Self {
            max_file_bytes,
            retained_files,
            queue_lines,
        })
    }

    /// Returns the inclusive byte bound for one active or retained file.
    pub const fn max_file_bytes(self) -> u64 {
        self.max_file_bytes
    }

    /// Returns the inclusive active-plus-rotated file count.
    pub const fn retained_files(self) -> usize {
        self.retained_files
    }

    /// Returns the nonblocking queue capacity in complete lines.
    pub const fn queue_lines(self) -> usize {
        self.queue_lines
    }
}

/// Invalid generic structured-log resource setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StructuredLogSettingsError {
    /// One file could retain no bytes.
    MaxFileBytes,
    /// Rotation would retain no active file.
    RetainedFiles,
    /// The producer queue could retain no complete line.
    QueueLines,
}

impl Display for StructuredLogSettingsError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::MaxFileBytes => "structured log file byte bound must be non-zero",
            Self::RetainedFiles => "structured log retained file count must be non-zero",
            Self::QueueLines => "structured log queue line count must be non-zero",
        })
    }
}

impl Error for StructuredLogSettingsError {}

/// Structured-log initialization stage safe for public diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StructuredLoggingStage {
    /// Create the configured parent directory.
    CreateParent,
    /// Open the active append-only file.
    OpenFile,
    /// Install the process-global tracing subscriber.
    InstallSubscriber,
}

impl StructuredLoggingStage {
    /// Returns the stable stage name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CreateParent => "create_parent",
            Self::OpenFile => "open_file",
            Self::InstallSubscriber => "install_subscriber",
        }
    }
}

/// Payload-redacted structured-log initialization failure.
pub struct StructuredLoggingError {
    stage: StructuredLoggingStage,
    source: Box<dyn Error + Send + Sync>,
}

impl StructuredLoggingError {
    fn new(stage: StructuredLoggingStage, source: impl Error + Send + Sync + 'static) -> Self {
        Self {
            stage,
            source: Box::new(source),
        }
    }

    /// Returns the stable failed stage without exposing a filesystem path.
    pub const fn stage(&self) -> StructuredLoggingStage {
        self.stage
    }
}

impl Debug for StructuredLoggingError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StructuredLoggingError")
            .field("stage", &self.stage)
            .finish_non_exhaustive()
    }
}

impl Display for StructuredLoggingError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "structured logging failed at {}",
            self.stage.as_str()
        )
    }
}

impl Error for StructuredLoggingError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// Process-lifetime owner of the nonblocking writer guard.
///
/// Keep this alive until components stop emitting telemetry. Drop requests
/// shutdown and attempts a bounded flush through tracing-appender; it neither
/// guarantees that every queued line is written nor joins the writer thread.
pub struct StructuredLoggingGuard {
    _worker: WorkerGuard,
    errors: ErrorCounter,
    queue_lines: usize,
}

impl StructuredLoggingGuard {
    /// Returns lines dropped or rejected by the bounded writer since startup.
    pub fn dropped_lines(&self) -> usize {
        self.errors.dropped_lines()
    }

    /// Returns a cloneable read-only pressure view.
    pub fn pressure(&self) -> StructuredLogPressure {
        StructuredLogPressure {
            errors: self.errors.clone(),
            queue_lines: self.queue_lines,
        }
    }
}

impl Debug for StructuredLoggingGuard {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StructuredLoggingGuard")
            .field("dropped_lines", &self.dropped_lines())
            .finish_non_exhaustive()
    }
}

/// Read-only structured-log queue pressure.
#[derive(Clone)]
pub struct StructuredLogPressure {
    errors: ErrorCounter,
    queue_lines: usize,
}

impl StructuredLogPressure {
    /// Returns the configured complete-line queue bound.
    pub const fn queue_lines(&self) -> usize {
        self.queue_lines
    }

    /// Returns lines dropped or rejected without blocking a producer.
    pub fn dropped_lines(&self) -> usize {
        self.errors.dropped_lines()
    }
}

impl Debug for StructuredLogPressure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StructuredLogPressure")
            .field("queue_lines", &self.queue_lines)
            .field("dropped_lines", &self.dropped_lines())
            .finish()
    }
}

/// Installs one bounded nonblocking JSONL subscriber for the current process.
///
/// The configured path is never logged or included in public errors. The file
/// is owner-readable/writable on Unix, rotation keeps a fixed total file count,
/// and a saturated queue drops complete lines instead of delaying producers.
/// This operation performs directory, file, global-subscriber, and thread IO.
///
/// # Errors
///
/// Returns a typed redacted directory, file, or subscriber-installation error.
pub fn install_jsonl_logging(
    path: &Path,
    settings: StructuredLogSettings,
    maximum_level: LevelFilter,
    writer_thread_name: &'static str,
) -> Result<StructuredLoggingGuard, StructuredLoggingError> {
    let writer = RotatingLogWriter::open(path, settings)?;
    let (non_blocking, worker) = NonBlockingBuilder::default()
        .buffered_lines_limit(settings.queue_lines())
        .lossy(true)
        .thread_name(writer_thread_name)
        .finish(writer);
    let errors = non_blocking.error_counter();
    tracing_subscriber::fmt()
        .json()
        .with_ansi(false)
        .with_current_span(true)
        .with_span_list(false)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_target(true)
        .with_max_level(maximum_level)
        .with_writer(non_blocking)
        .try_init()
        .map_err(|source| StructuredLoggingError {
            stage: StructuredLoggingStage::InstallSubscriber,
            source,
        })?;
    Ok(StructuredLoggingGuard {
        _worker: worker,
        errors,
        queue_lines: settings.queue_lines(),
    })
}

struct RotatingLogWriter {
    path: PathBuf,
    file: Option<File>,
    current_bytes: u64,
    settings: StructuredLogSettings,
}

impl RotatingLogWriter {
    fn open(path: &Path, settings: StructuredLogSettings) -> Result<Self, StructuredLoggingError> {
        let parent = path.parent().ok_or_else(|| {
            StructuredLoggingError::new(
                StructuredLoggingStage::CreateParent,
                io::Error::new(io::ErrorKind::InvalidInput, "log path has no parent"),
            )
        })?;
        crate::process_instance::prepare_private_directory(parent).map_err(|source| {
            StructuredLoggingError::new(StructuredLoggingStage::CreateParent, source)
        })?;
        let file = open_private_append_file(path).map_err(|source| {
            StructuredLoggingError::new(StructuredLoggingStage::OpenFile, source)
        })?;
        let current_bytes = file.metadata().map(|value| value.len()).unwrap_or_default();
        Ok(Self {
            path: path.to_path_buf(),
            file: Some(file),
            current_bytes,
            settings,
        })
    }

    fn generation(&self, number: usize) -> PathBuf {
        let mut value = self.path.as_os_str().to_os_string();
        value.push(format!(".{number}"));
        PathBuf::from(value)
    }

    fn rotate(&mut self) -> io::Result<()> {
        if let Some(mut file) = self.file.take() {
            file.flush()?;
        }
        let generations = self.settings.retained_files().saturating_sub(1);
        if generations == 0 {
            remove_if_present(&self.path)?;
        } else {
            remove_if_present(&self.generation(generations))?;
            for number in (1..generations).rev() {
                rename_if_present(&self.generation(number), &self.generation(number + 1))?;
            }
            rename_if_present(&self.path, &self.generation(1))?;
        }
        self.file = Some(open_private_append_file(&self.path)?);
        self.current_bytes = 0;
        Ok(())
    }
}

impl Write for RotatingLogWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let buffer_bytes = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
        if buffer_bytes > self.settings.max_file_bytes() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "structured log record exceeds the configured file bound",
            ));
        }
        if self.current_bytes > 0
            && self.current_bytes.saturating_add(buffer_bytes) > self.settings.max_file_bytes()
        {
            self.rotate()?;
        }
        let written = self
            .file
            .as_mut()
            .ok_or_else(|| io::Error::other("structured log file is unavailable"))?
            .write(buffer)?;
        self.current_bytes = self
            .current_bytes
            .saturating_add(u64::try_from(written).unwrap_or(u64::MAX));
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file
            .as_mut()
            .ok_or_else(|| io::Error::other("structured log file is unavailable"))?
            .flush()
    }
}

#[cfg(windows)]
fn open_private_append_file(path: &Path) -> io::Result<File> {
    use std::io::Seek;
    let mut file = undercurrent_platform_windows::open_private_file(path, true, true)?.into_file();
    file.seek(std::io::SeekFrom::End(0))?;
    Ok(file)
}

#[cfg(not(windows))]
fn open_private_append_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    Ok(file)
}

fn remove_if_present(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn rename_if_present(source: &Path, destination: &Path) -> io::Result<()> {
    match fs::rename(source, destination) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;

    #[test]
    fn settings_reject_every_zero_resource_bound() {
        assert_eq!(
            StructuredLogSettings::new(0, 1, 1),
            Err(StructuredLogSettingsError::MaxFileBytes)
        );
        assert_eq!(
            StructuredLogSettings::new(1, 0, 1),
            Err(StructuredLogSettingsError::RetainedFiles)
        );
        assert_eq!(
            StructuredLogSettings::new(1, 1, 0),
            Err(StructuredLogSettingsError::QueueLines)
        );
    }

    #[test]
    fn rotating_writer_enforces_total_file_count_and_order() {
        let temp = private_temp();
        let path = temp.path().join("process.jsonl");
        let mut writer = test_writer(&path, 16, 3);

        writer.write_all(b"first-record\n").expect("first");
        writer.write_all(b"second-record\n").expect("second");
        writer.write_all(b"third-record\n").expect("third");
        writer.flush().expect("flush");

        assert_eq!(fs::read_to_string(&path).unwrap(), "third-record\n");
        assert_eq!(
            fs::read_to_string(path.with_file_name("process.jsonl.1")).unwrap(),
            "second-record\n"
        );
        assert_eq!(
            fs::read_to_string(path.with_file_name("process.jsonl.2")).unwrap(),
            "first-record\n"
        );
        assert!(!path.with_file_name("process.jsonl.3").exists());
    }

    #[test]
    fn one_retained_file_replaces_the_active_file_on_rotation() {
        let temp = private_temp();
        let path = temp.path().join("process.jsonl");
        let mut writer = test_writer(&path, 8, 1);

        writer.write_all(b"12345678").expect("full file");
        writer.write_all(b"fresh").expect("rotation");
        writer.flush().expect("flush");

        assert_eq!(fs::read(&path).unwrap(), b"fresh");
        assert!(!path.with_file_name("process.jsonl.1").exists());
    }

    #[test]
    fn existing_log_file_permissions_are_repaired_on_open() {
        let temp = private_temp();
        let path = temp.path().join("process.jsonl");
        #[cfg(not(windows))]
        fs::write(&path, b"existing\n").expect("existing log");
        #[cfg(windows)]
        undercurrent_platform_windows::create_private_file(&path)
            .expect("owned log")
            .into_file()
            .write_all(b"existing\n")
            .expect("existing log");
        #[cfg(unix)]
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("broad permissions");

        let writer = RotatingLogWriter::open(&path, settings(64, 2)).expect("writer");

        #[cfg(unix)]
        assert_eq!(
            fs::metadata(&path).expect("metadata").permissions().mode() & 0o777,
            0o600
        );
        drop(writer);
    }

    #[test]
    fn log_parent_is_created_private_and_permissive_parents_are_rejected() {
        let temp = private_temp();
        let private_parent = temp.path().join("private");
        let private_path = private_parent.join("process.jsonl");

        let writer =
            RotatingLogWriter::open(&private_path, settings(64, 2)).expect("private log parent");
        #[cfg(unix)]
        assert_eq!(
            fs::metadata(&private_parent)
                .expect("private parent metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        drop(writer);

        #[cfg(unix)]
        {
            let permissive_parent = temp.path().join("permissive");
            fs::create_dir(&permissive_parent).expect("permissive parent");
            #[cfg(unix)]
            fs::set_permissions(&permissive_parent, fs::Permissions::from_mode(0o755))
                .expect("permissive permissions");
            let permissive_path = permissive_parent.join("process.jsonl");
            let error = RotatingLogWriter::open(&permissive_path, settings(64, 2))
                .err()
                .expect("permissive parent is rejected");

            assert_eq!(error.stage(), StructuredLoggingStage::CreateParent);
            assert!(!permissive_path.exists());
        }
    }

    #[test]
    fn public_logging_errors_redact_path_and_retain_source() {
        let temp = private_temp();
        let secret = "private-log-location";
        let parent_file = temp.path().join(secret);
        fs::write(&parent_file, b"not a directory").unwrap();
        let error = RotatingLogWriter::open(&parent_file.join("process.jsonl"), settings(64, 1))
            .err()
            .expect("invalid parent");

        assert_eq!(error.stage(), StructuredLoggingStage::CreateParent);
        assert!(!error.to_string().contains(secret));
        assert!(!format!("{error:?}").contains(secret));
        assert!(error.source().is_some());
    }

    #[test]
    fn log_pressure_clones_share_the_nonblocking_error_counter() {
        let (writer, worker) = NonBlockingBuilder::default().finish(io::sink());
        let pressure = StructuredLogPressure {
            errors: writer.error_counter(),
            queue_lines: 17,
        };

        assert_eq!(pressure.queue_lines(), 17);
        assert_eq!(pressure.clone().dropped_lines(), 0);
        drop(worker);
    }

    fn settings(max_file_bytes: u64, retained_files: usize) -> StructuredLogSettings {
        StructuredLogSettings::new(max_file_bytes, retained_files, 8).unwrap()
    }

    struct PrivateTemp {
        _root: TempDir,
        path: PathBuf,
    }

    impl PrivateTemp {
        fn path(&self) -> &Path {
            &self.path
        }
    }

    fn private_temp() -> PrivateTemp {
        let temp = TempDir::new().expect("temp directory");
        #[cfg(unix)]
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700))
            .expect("private temp directory");
        #[cfg(windows)]
        let path = {
            let path = temp.path().join("private");
            drop(
                undercurrent_platform_windows::prepare_private_directory(&path)
                    .expect("owned directory"),
            );
            path
        };
        #[cfg(not(windows))]
        let path = temp.path().to_path_buf();
        PrivateTemp { _root: temp, path }
    }

    fn test_writer(path: &Path, max_file_bytes: u64, retained_files: usize) -> RotatingLogWriter {
        RotatingLogWriter::open(path, settings(max_file_bytes, retained_files)).unwrap()
    }
}
