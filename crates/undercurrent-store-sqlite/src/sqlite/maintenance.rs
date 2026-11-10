// SPDX-License-Identifier: Apache-2.0

//! Synchronous verification, SQLite snapshot backup, and closed-target restore.
//!
//! Each operation opens independent connections, outside the repository worker
//! queue and configured busy timeout. Callers must exclude other target owners
//! during restore. Verification requires the current released layout and checks
//! selected repository invariants; it is not exhaustive decoding of all payloads.
//! Returned-error rollback is distinct from recovery after abrupt process death.

use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use getrandom::fill as fill_random;
use rusqlite::backup::Backup;
use rusqlite::{Connection, OpenFlags};

use super::database::{
    DATABASE_SCHEMA_VERSION, SCHEMA_CHECKSUM, SCHEMA_NAME, validate_agent_channel_consistency,
    validate_attempt_repair_consistency, validate_input_retirement_consistency,
    validate_release_schema, validate_request_digest_consistency,
    validate_runtime_cancellation_consistency, validate_runtime_policy_consistency,
};
use super::{NoopSqliteCrashInjector, SqliteCrashInjector, SqliteCrashPoint};

/// Stage associated with a maintenance filesystem or SQLite failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SqliteMaintenanceStage {
    Prepare,
    OpenSource,
    OpenDestination,
    Snapshot,
    Verify,
    Synchronize,
    Install,
    Rollback,
}

/// Maintenance failure with stage/source details where available.
///
/// An error does not universally prove that the destination is unchanged.
/// Backup can leave an incomplete file; restore can fail during rollback or
/// during cleanup after a new target has already been installed.
#[derive(Debug)]
#[non_exhaustive]
pub enum SqliteMaintenanceError {
    InvalidPath(&'static str),
    DestinationExists,
    UnsupportedSchema {
        found: i64,
        maximum: i64,
    },
    IntegrityCheckFailed(String),
    RepositoryInvariant(&'static str),
    SchemaValidation(String),
    InjectedCrash(SqliteCrashPoint),
    Io {
        stage: SqliteMaintenanceStage,
        source: io::Error,
    },
    Sqlite {
        stage: SqliteMaintenanceStage,
        source: rusqlite::Error,
    },
}

impl fmt::Display for SqliteMaintenanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPath(reason) => write!(formatter, "invalid database path: {reason}"),
            Self::DestinationExists => write!(formatter, "backup destination already exists"),
            Self::UnsupportedSchema { found, maximum } => write!(
                formatter,
                "database schema version {found} does not equal supported version {maximum}"
            ),
            Self::IntegrityCheckFailed(detail) => {
                write!(formatter, "SQLite integrity verification failed: {detail}")
            }
            Self::RepositoryInvariant(reason) => {
                write!(
                    formatter,
                    "repository invariant verification failed: {reason}"
                )
            }
            Self::SchemaValidation(reason) => {
                write!(formatter, "database schema validation failed: {reason}")
            }
            Self::InjectedCrash(point) => write!(formatter, "injected crash at {point}"),
            Self::Io { stage, source } => {
                write!(formatter, "filesystem failure during {stage:?}: {source}")
            }
            Self::Sqlite { stage, source } => {
                write!(formatter, "SQLite failure during {stage:?}: {source}")
            }
        }
    }
}

impl Error for SqliteMaintenanceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Sqlite { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Current-layout integrity and selected repository metadata evidence.
///
/// Counts/checkpoints do not establish process truth, application readiness,
/// or successful decoding of every session, attempt, outbox, and snapshot row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqliteVerificationEvidence {
    /// Numeric SQLite storage format marker; release identity is validated separately.
    pub schema_version: i64,
    /// Session table row count, checked against singleton metadata.
    pub session_count: u64,
    /// Pending outbox row count, checked against singleton metadata.
    pub outbox_count: u64,
    /// Retained acknowledgement checkpoint, if present.
    pub last_acknowledged_sequence: Option<u64>,
    /// Next allocation position from singleton metadata.
    pub next_outbox_sequence: u64,
}

/// The exact current release schema identity retained by the database.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqliteSchemaEvidence {
    /// Numeric storage identity marker.
    pub version: i64,
    /// Exact supported Undercurrent release.
    pub release_version: undercurrent_foundation::ReleaseVersion,
    /// Stable executable registry name.
    pub name: String,
    /// Registry checksum text checked against the executable.
    pub checksum: String,
    /// SQLite wall-clock Unix seconds when this entry was recorded.
    pub applied_at_unix_seconds: i64,
}

/// Verification of a synchronized SQLite snapshot destination.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqliteBackupEvidence {
    /// Evidence collected from the completed destination.
    pub verification: SqliteVerificationEvidence,
}

/// Source and installed-target evidence after successful restore and cleanup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqliteRestoreEvidence {
    /// Source backup evidence collected before copying.
    pub source: SqliteVerificationEvidence,
    /// Target evidence collected after installation.
    pub installed: SqliteVerificationEvidence,
    /// Whether a target database existed before installation.
    pub replaced_existing_database: bool,
}

/// Restore fault-injection options; defaults use no injected failures.
#[derive(Clone)]
pub struct SqliteRestoreOptions {
    crash_injector: Arc<dyn SqliteCrashInjector>,
}

impl Default for SqliteRestoreOptions {
    fn default() -> Self {
        Self {
            crash_injector: Arc::new(NoopSqliteCrashInjector),
        }
    }
}

impl SqliteRestoreOptions {
    /// Uses a deterministic returned-error injector for recovery tests.
    pub fn with_crash_injector(mut self, injector: Arc<dyn SqliteCrashInjector>) -> Self {
        self.crash_injector = injector;
        self
    }
}

/// Creates a consistent SQLite snapshot, including committed WAL contents.
///
/// The destination must not exist. Both its parent and the resulting file are restricted to the
/// current owner on Unix platforms.
/// The source may be open; this uses SQLite's backup API, not a main-file copy.
/// Paths are checked for lexical equality only, so callers must exclude aliases.
/// This is synchronous and has no application deadline or cancellation hook.
///
/// # Errors
///
/// Returns path, filesystem, SQLite, or current-layout verification failures.
/// Final verification failure attempts to remove the destination; earlier
/// failures may leave an incomplete file. Runtime journals and output artifacts
/// outside SQLite are not included in the snapshot.
pub fn backup_sqlite_database(
    source_path: impl AsRef<Path>,
    destination_path: impl AsRef<Path>,
) -> Result<SqliteBackupEvidence, SqliteMaintenanceError> {
    with_file_owner(|| backup_with_file_owner(source_path.as_ref(), destination_path.as_ref()))
}

fn backup_with_file_owner(
    source_path: &Path,
    destination_path: &Path,
) -> Result<SqliteBackupEvidence, SqliteMaintenanceError> {
    require_distinct_paths(source_path, destination_path)?;
    if destination_path.exists() {
        return Err(SqliteMaintenanceError::DestinationExists);
    }
    prepare_parent(destination_path)?;

    let source = open_read_only(source_path, SqliteMaintenanceStage::OpenSource)?;
    let mut destination = open_new_database(destination_path)?;
    {
        let backup = Backup::new(&source, &mut destination).map_err(|source| {
            SqliteMaintenanceError::Sqlite {
                stage: SqliteMaintenanceStage::Snapshot,
                source,
            }
        })?;
        backup
            .run_to_completion(128, Duration::from_millis(1), None)
            .map_err(|source| SqliteMaintenanceError::Sqlite {
                stage: SqliteMaintenanceStage::Snapshot,
                source,
            })?;
    }
    destination
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .map_err(|source| SqliteMaintenanceError::Sqlite {
            stage: SqliteMaintenanceStage::Synchronize,
            source,
        })?;
    drop(destination);
    enforce_owner_only_file(destination_path)?;
    sync_file_and_parent(destination_path)?;

    match verify_sqlite_database(destination_path) {
        Ok(verification) => Ok(SqliteBackupEvidence { verification }),
        Err(error) => {
            let _ = fs::remove_file(destination_path);
            Err(error)
        }
    }
}

/// Verifies current layout, SQLite integrity, and selected repository invariants
/// through a read-only connection.
///
/// This neither migrates a historical layout nor decodes every session/outbox
/// payload. Reads have no enclosing snapshot transaction; verify a closed file
/// or completed backup for stable cross-table evidence. It performs synchronous
/// I/O independently of repository queue limits and busy timeout.
///
/// # Errors
///
/// Returns typed integrity, schema, provenance, counter, SQLite, or ownership
/// failures. Success is not an assertion of runtime or application readiness.
pub fn verify_sqlite_database(
    path: impl AsRef<Path>,
) -> Result<SqliteVerificationEvidence, SqliteMaintenanceError> {
    with_file_owner(|| verify_with_file_owner(path.as_ref()))
}

fn verify_with_file_owner(
    path: &Path,
) -> Result<SqliteVerificationEvidence, SqliteMaintenanceError> {
    let connection = open_read_only(path, SqliteMaintenanceStage::Verify)?;
    verify_connection(&connection)
}

/// Reads and verifies the current database schema identity.
///
/// Runs the same verification as [`verify_sqlite_database`] before matching
/// the release version/name/checksum to the executable. Application times are
/// returned as evidence, not compared to fixed expected timestamps.
///
/// # Errors
///
/// Returns verification, read, or identity-mismatch failures. Other releases are rejected.
pub fn sqlite_schema_evidence(
    path: impl AsRef<Path>,
) -> Result<Vec<SqliteSchemaEvidence>, SqliteMaintenanceError> {
    with_file_owner(|| schema_evidence_with_file_owner(path.as_ref()))
}

fn schema_evidence_with_file_owner(
    path: &Path,
) -> Result<Vec<SqliteSchemaEvidence>, SqliteMaintenanceError> {
    let connection = open_read_only(path, SqliteMaintenanceStage::Verify)?;
    let verification = verify_connection(&connection)?;
    if verification.schema_version != DATABASE_SCHEMA_VERSION {
        return Err(SqliteMaintenanceError::UnsupportedSchema {
            found: verification.schema_version,
            maximum: DATABASE_SCHEMA_VERSION,
        });
    }
    let mut statement = connection
        .prepare(
            "SELECT version, name, checksum, applied_at_unix_seconds
             FROM schema_identity ORDER BY version",
        )
        .map_err(|source| SqliteMaintenanceError::Sqlite {
            stage: SqliteMaintenanceStage::Verify,
            source,
        })?;
    let rows = statement
        .query_map([], |row| {
            Ok(SqliteSchemaEvidence {
                version: row.get(0)?,
                release_version: undercurrent_foundation::ReleaseVersion::Current,
                name: row.get(1)?,
                checksum: row.get(2)?,
                applied_at_unix_seconds: row.get(3)?,
            })
        })
        .map_err(|source| SqliteMaintenanceError::Sqlite {
            stage: SqliteMaintenanceStage::Verify,
            source,
        })?;
    let evidence =
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|source| SqliteMaintenanceError::Sqlite {
                stage: SqliteMaintenanceStage::Verify,
                source,
            })?;
    let expected = [(1_i64, SCHEMA_NAME, SCHEMA_CHECKSUM)];
    if evidence.len() != expected.len()
        || evidence.iter().zip(expected).any(|(actual, expected)| {
            (
                actual.version,
                actual.name.as_str(),
                actual.checksum.as_str(),
            ) != expected
        })
    {
        return Err(SqliteMaintenanceError::RepositoryInvariant(
            "schema identity does not match the executable registry",
        ));
    }
    Ok(evidence)
}

/// Stages and installs a verified backup over an externally closed repository.
///
/// The caller must exclude all target connections and path aliases. Source
/// verification requires the current layout before the candidate is installed.
/// This performs synchronous file/SQLite I/O and renames the existing database
/// and its WAL/SHM sidecars to rollback paths before installation.
///
/// # Errors
///
/// Returned install/verification failures attempt to restore the prior target.
/// Rollback can itself fail; cleanup/synchronization errors after installation
/// can leave the new target installed. Abrupt process death bypasses this error
/// handler, so this is not a crash-atomic multi-file replacement guarantee.
pub fn restore_sqlite_database(
    backup_path: impl AsRef<Path>,
    target_path: impl AsRef<Path>,
) -> Result<SqliteRestoreEvidence, SqliteMaintenanceError> {
    restore_sqlite_database_with_options(backup_path, target_path, SqliteRestoreOptions::default())
}

/// Runs [`restore_sqlite_database`] with an explicit test fault injector.
///
/// The same closed-target precondition and partial-failure semantics apply.
/// Injection returns an error rather than terminating the process.
///
/// # Errors
///
/// Returns the ordinary restore failures or an injected failure at a configured
/// boundary. The post-install hook exercises the returned-error rollback path.
pub fn restore_sqlite_database_with_options(
    backup_path: impl AsRef<Path>,
    target_path: impl AsRef<Path>,
    options: SqliteRestoreOptions,
) -> Result<SqliteRestoreEvidence, SqliteMaintenanceError> {
    with_file_owner(|| restore_with_file_owner(backup_path.as_ref(), target_path.as_ref(), options))
}

fn restore_with_file_owner(
    backup_path: &Path,
    target_path: &Path,
    options: SqliteRestoreOptions,
) -> Result<SqliteRestoreEvidence, SqliteMaintenanceError> {
    require_distinct_paths(backup_path, target_path)?;
    let source = verify_sqlite_database(backup_path)?;
    prepare_parent(target_path)?;

    let nonce = random_suffix()?;
    let candidate = sibling_path(target_path, &format!("restore-{nonce}"))?;
    let rollback = sibling_path(target_path, &format!("rollback-{nonce}"))?;
    let replaced_existing_database = target_path.exists();

    let create_candidate = (|| {
        let backup = open_read_only(backup_path, SqliteMaintenanceStage::OpenSource)?;
        let mut destination = open_new_database(&candidate)?;
        {
            let snapshot = Backup::new(&backup, &mut destination).map_err(|source| {
                SqliteMaintenanceError::Sqlite {
                    stage: SqliteMaintenanceStage::Snapshot,
                    source,
                }
            })?;
            snapshot
                .run_to_completion(128, Duration::from_millis(1), None)
                .map_err(|source| SqliteMaintenanceError::Sqlite {
                    stage: SqliteMaintenanceStage::Snapshot,
                    source,
                })?;
        }
        validate_release_schema(&destination)
            .map_err(|error| SqliteMaintenanceError::SchemaValidation(error.to_string()))?;
        drop(destination);
        enforce_owner_only_file(&candidate)?;
        verify_sqlite_database(&candidate)?;
        sync_file_and_parent(&candidate)
    })();
    if let Err(error) = create_candidate {
        let _ = fs::remove_file(&candidate);
        return Err(error);
    }

    // Keep the old database and its WAL/SHM together for returned-error rollback;
    // its committed WAL contents belong to the old target, not the candidate.
    // The caller must already have excluded every target connection.
    // See [restore staging](../../docs/development/maintenance.md#restore-staging-and-rollback).
    if replaced_existing_database {
        fs::rename(target_path, &rollback).map_err(|source| SqliteMaintenanceError::Io {
            stage: SqliteMaintenanceStage::Install,
            source,
        })?;
        if let Err(error) = move_sidecars(target_path, &rollback, SqliteMaintenanceStage::Install) {
            let _ = move_sidecars(&rollback, target_path, SqliteMaintenanceStage::Rollback);
            let _ = fs::rename(&rollback, target_path);
            return Err(error);
        }
    }

    let install_result = (|| {
        fs::rename(&candidate, target_path).map_err(|source| SqliteMaintenanceError::Io {
            stage: SqliteMaintenanceStage::Install,
            source,
        })?;
        sync_parent(target_path)?;
        if options
            .crash_injector
            .should_crash(SqliteCrashPoint::RestoreAfterInstallBeforeVerify)
        {
            return Err(SqliteMaintenanceError::InjectedCrash(
                SqliteCrashPoint::RestoreAfterInstallBeforeVerify,
            ));
        }
        verify_sqlite_database(target_path)
    })();

    match install_result {
        Ok(installed) => {
            if replaced_existing_database {
                remove_sidecars(&rollback, SqliteMaintenanceStage::Synchronize)?;
                fs::remove_file(&rollback).map_err(|source| SqliteMaintenanceError::Io {
                    stage: SqliteMaintenanceStage::Synchronize,
                    source,
                })?;
                sync_parent(target_path)?;
            }
            enforce_owner_only_file(target_path)?;
            Ok(SqliteRestoreEvidence {
                source,
                installed,
                replaced_existing_database,
            })
        }
        Err(error) => {
            let _ = fs::remove_file(target_path);
            let _ = remove_sidecars(target_path, SqliteMaintenanceStage::Rollback);
            if replaced_existing_database {
                fs::rename(&rollback, target_path).map_err(|source| {
                    SqliteMaintenanceError::Io {
                        stage: SqliteMaintenanceStage::Rollback,
                        source,
                    }
                })?;
                move_sidecars(&rollback, target_path, SqliteMaintenanceStage::Rollback)?;
                sync_parent(target_path)?;
            }
            Err(error)
        }
    }
}

fn with_file_owner<T>(
    operation: impl FnOnce() -> Result<T, SqliteMaintenanceError>,
) -> Result<T, SqliteMaintenanceError> {
    super::with_file_owner(operation).map_err(|source| SqliteMaintenanceError::Io {
        stage: SqliteMaintenanceStage::Prepare,
        source,
    })?
}

fn verify_connection(
    connection: &Connection,
) -> Result<SqliteVerificationEvidence, SqliteMaintenanceError> {
    let integrity: String = connection
        .query_row("PRAGMA integrity_check(1)", [], |row| row.get(0))
        .map_err(|source| SqliteMaintenanceError::Sqlite {
            stage: SqliteMaintenanceStage::Verify,
            source,
        })?;
    if integrity != "ok" {
        return Err(SqliteMaintenanceError::IntegrityCheckFailed(integrity));
    }

    let schema_version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|source| SqliteMaintenanceError::Sqlite {
            stage: SqliteMaintenanceStage::Verify,
            source,
        })?;
    if schema_version != DATABASE_SCHEMA_VERSION {
        return Err(SqliteMaintenanceError::UnsupportedSchema {
            found: schema_version,
            maximum: DATABASE_SCHEMA_VERSION,
        });
    }
    validate_release_schema(connection).map_err(|_| {
        SqliteMaintenanceError::RepositoryInvariant(
            "schema schema identity does not match the current release layout",
        )
    })?;
    validate_runtime_policy_consistency(connection).map_err(|_| {
        SqliteMaintenanceError::RepositoryInvariant(
            "runtime dispatch policy provenance does not match retained policy state",
        )
    })?;
    validate_request_digest_consistency(connection).map_err(|_| {
        SqliteMaintenanceError::RepositoryInvariant(
            "caller request-digest provenance does not match retained dispatch state",
        )
    })?;
    validate_attempt_repair_consistency(connection).map_err(|_| {
        SqliteMaintenanceError::RepositoryInvariant(
            "attempt repair audit does not match durable attempt state",
        )
    })?;
    validate_input_retirement_consistency(connection, usize::MAX).map_err(|_| {
        SqliteMaintenanceError::RepositoryInvariant("input retirement audit is inconsistent")
    })?;
    validate_runtime_cancellation_consistency(connection).map_err(|_| {
        SqliteMaintenanceError::RepositoryInvariant(
            "runtime cancellation controls do not match retained attempt/session state",
        )
    })?;
    validate_agent_channel_consistency(connection).map_err(|_| {
        SqliteMaintenanceError::RepositoryInvariant(
            "agent channel records do not match retained attempt bindings",
        )
    })?;

    let session_count = count_rows(connection, "sessions")?;
    let outbox_count = count_rows(connection, "outbox")?;
    let (stored_sessions, stored_outbox, next_sequence, last_acknowledged): (
        i64,
        i64,
        Vec<u8>,
        Option<Vec<u8>>,
    ) = connection
        .query_row(
            "SELECT session_count, outbox_count, next_outbox_sequence, last_acknowledged
             FROM repository_state WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .map_err(|source| SqliteMaintenanceError::Sqlite {
            stage: SqliteMaintenanceStage::Verify,
            source,
        })?;
    if stored_sessions < 0 || stored_outbox < 0 {
        return Err(SqliteMaintenanceError::RepositoryInvariant(
            "negative persisted counter",
        ));
    }
    if stored_sessions as u64 != session_count || stored_outbox as u64 != outbox_count {
        return Err(SqliteMaintenanceError::RepositoryInvariant(
            "persisted counters do not match table counts",
        ));
    }
    let next_outbox_sequence = decode_sequence(&next_sequence)?;
    let last_acknowledged_sequence = last_acknowledged
        .as_deref()
        .map(decode_sequence)
        .transpose()?;
    if next_outbox_sequence == 0
        || last_acknowledged_sequence.is_some_and(|value| value >= next_outbox_sequence)
    {
        return Err(SqliteMaintenanceError::RepositoryInvariant(
            "invalid outbox checkpoint ordering",
        ));
    }

    Ok(SqliteVerificationEvidence {
        schema_version,
        session_count,
        outbox_count,
        last_acknowledged_sequence,
        next_outbox_sequence,
    })
}

fn count_rows(connection: &Connection, table: &'static str) -> Result<u64, SqliteMaintenanceError> {
    let sql = match table {
        "sessions" => "SELECT COUNT(*) FROM sessions",
        "outbox" => "SELECT COUNT(*) FROM outbox",
        _ => return Err(SqliteMaintenanceError::RepositoryInvariant("unknown table")),
    };
    let count: i64 = connection
        .query_row(sql, [], |row| row.get(0))
        .map_err(|source| SqliteMaintenanceError::Sqlite {
            stage: SqliteMaintenanceStage::Verify,
            source,
        })?;
    u64::try_from(count)
        .map_err(|_| SqliteMaintenanceError::RepositoryInvariant("negative table row count"))
}

fn decode_sequence(bytes: &[u8]) -> Result<u64, SqliteMaintenanceError> {
    let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
        SqliteMaintenanceError::RepositoryInvariant("invalid outbox sequence encoding")
    })?;
    Ok(u64::from_be_bytes(bytes))
}

fn open_read_only(
    path: &Path,
    stage: SqliteMaintenanceStage,
) -> Result<Connection, SqliteMaintenanceError> {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|source| SqliteMaintenanceError::Sqlite { stage, source })
}

fn open_new_database(path: &Path) -> Result<Connection, SqliteMaintenanceError> {
    if path.exists() {
        return Err(SqliteMaintenanceError::DestinationExists);
    }
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| SqliteMaintenanceError::Io {
            stage: SqliteMaintenanceStage::OpenDestination,
            source,
        })?;
    restrict_file(&file)?;
    drop(file);
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|source| SqliteMaintenanceError::Sqlite {
        stage: SqliteMaintenanceStage::OpenDestination,
        source,
    })
}

fn require_distinct_paths(left: &Path, right: &Path) -> Result<(), SqliteMaintenanceError> {
    if left.as_os_str().is_empty() || right.as_os_str().is_empty() {
        return Err(SqliteMaintenanceError::InvalidPath(
            "path must not be empty",
        ));
    }
    if left == right {
        return Err(SqliteMaintenanceError::InvalidPath(
            "source and destination must differ",
        ));
    }
    Ok(())
}

fn prepare_parent(path: &Path) -> Result<(), SqliteMaintenanceError> {
    let parent = path.parent().ok_or(SqliteMaintenanceError::InvalidPath(
        "database must have a parent directory",
    ))?;
    fs::create_dir_all(parent).map_err(|source| SqliteMaintenanceError::Io {
        stage: SqliteMaintenanceStage::Prepare,
        source,
    })?;
    enforce_owner_only_directory(parent)
}

fn random_suffix() -> Result<String, SqliteMaintenanceError> {
    let mut bytes = [0_u8; 12];
    fill_random(&mut bytes).map_err(|source| SqliteMaintenanceError::Io {
        stage: SqliteMaintenanceStage::Prepare,
        source: io::Error::other(source.to_string()),
    })?;
    let mut suffix = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use fmt::Write as _;
        let _ = write!(&mut suffix, "{byte:02x}");
    }
    Ok(suffix)
}

fn sibling_path(path: &Path, suffix: &str) -> Result<PathBuf, SqliteMaintenanceError> {
    let parent = path.parent().ok_or(SqliteMaintenanceError::InvalidPath(
        "database must have a parent directory",
    ))?;
    let name = path.file_name().ok_or(SqliteMaintenanceError::InvalidPath(
        "database must have a file name",
    ))?;
    let mut candidate = name.to_os_string();
    candidate.push(format!(".{suffix}"));
    Ok(parent.join(candidate))
}

fn sync_file_and_parent(path: &Path) -> Result<(), SqliteMaintenanceError> {
    #[cfg(windows)]
    // FlushFileBuffers requires a writable handle. Opening read-only succeeds
    // on Windows but sync_all then fails with access denied.
    let file = OpenOptions::new().write(true).open(path);
    #[cfg(not(windows))]
    let file = File::open(path);
    file.and_then(|file| file.sync_all())
        .map_err(|source| SqliteMaintenanceError::Io {
            stage: SqliteMaintenanceStage::Synchronize,
            source,
        })?;
    sync_parent(path)
}

fn sync_parent(path: &Path) -> Result<(), SqliteMaintenanceError> {
    let parent = path.parent().ok_or(SqliteMaintenanceError::InvalidPath(
        "database must have a parent directory",
    ))?;
    sync_directory(parent)
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), SqliteMaintenanceError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| SqliteMaintenanceError::Io {
            stage: SqliteMaintenanceStage::Synchronize,
            source,
        })
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<(), SqliteMaintenanceError> {
    Ok(())
}

fn move_sidecars(
    from: &Path,
    to: &Path,
    stage: SqliteMaintenanceStage,
) -> Result<(), SqliteMaintenanceError> {
    for suffix in ["-wal", "-shm"] {
        let source_path = sidecar_path(from, suffix);
        if source_path.exists() {
            fs::rename(source_path, sidecar_path(to, suffix))
                .map_err(|source| SqliteMaintenanceError::Io { stage, source })?;
        }
    }
    Ok(())
}

fn remove_sidecars(
    path: &Path,
    stage: SqliteMaintenanceStage,
) -> Result<(), SqliteMaintenanceError> {
    for suffix in ["-wal", "-shm"] {
        let sidecar = sidecar_path(path, suffix);
        if let Err(source) = fs::remove_file(sidecar)
            && source.kind() != io::ErrorKind::NotFound
        {
            return Err(SqliteMaintenanceError::Io { stage, source });
        }
    }
    Ok(())
}

fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

#[cfg(unix)]
fn restrict_file(file: &File) -> Result<(), SqliteMaintenanceError> {
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|source| SqliteMaintenanceError::Io {
            stage: SqliteMaintenanceStage::Prepare,
            source,
        })
}

#[cfg(not(unix))]
fn restrict_file(_file: &File) -> Result<(), SqliteMaintenanceError> {
    Ok(())
}

#[cfg(unix)]
fn enforce_owner_only_file(path: &Path) -> Result<(), SqliteMaintenanceError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|source| {
        SqliteMaintenanceError::Io {
            stage: SqliteMaintenanceStage::Prepare,
            source,
        }
    })
}

#[cfg(not(unix))]
fn enforce_owner_only_file(_path: &Path) -> Result<(), SqliteMaintenanceError> {
    Ok(())
}

#[cfg(unix)]
fn enforce_owner_only_directory(path: &Path) -> Result<(), SqliteMaintenanceError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|source| {
        SqliteMaintenanceError::Io {
            stage: SqliteMaintenanceStage::Prepare,
            source,
        }
    })
}

#[cfg(not(unix))]
fn enforce_owner_only_directory(_path: &Path) -> Result<(), SqliteMaintenanceError> {
    Ok(())
}
