// SPDX-License-Identifier: Apache-2.0
#![cfg(unix)] // Uses executable POSIX shell fixtures; Windows uses the native smoke suite.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;
use std::sync::{Mutex, MutexGuard};

use tempfile::TempDir;

// A concurrent spawn can inherit another fixture's writable binary-copy
// descriptor until exec, causing ETXTBSY even after fs::copy returns. Keep
// each copy-and-run lifetime exclusive; this does not test concurrent startup.
static EXECUTABLE_FIXTURE: Mutex<()> = Mutex::new(());

struct Fixture {
    directory: TempDir,
    _exclusive: MutexGuard<'static, ()>,
}

impl Fixture {
    fn new() -> Self {
        let exclusive = EXECUTABLE_FIXTURE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let directory = physical_tempdir().expect("temporary directory");
        fs::copy(env!("CARGO_BIN_EXE_uc"), directory.path().join("uc"))
            .expect("isolated CLI binary");
        let daemon = directory.path().join("uc-daemon");
        fs::write(
            &daemon,
            "#!/bin/sh\nprintf started > \"$STARTUP_MARKER\"\nexit 1\n",
        )
        .expect("fake daemon");
        fs::set_permissions(daemon, fs::Permissions::from_mode(0o700)).expect("executable daemon");
        Self {
            directory,
            _exclusive: exclusive,
        }
    }

    fn command(&self) -> Command {
        let mut command = Command::new(self.directory.path().join("uc"));
        command
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", self.directory.path())
            .env("UNDERCURRENT_HOME", self.directory.path().join("state"))
            .env("STARTUP_MARKER", self.directory.path().join("started"));
        command
    }

    fn started(&self) -> bool {
        self.directory.path().join("started").exists()
    }
}

#[test]
fn ordinary_commands_attempt_startup_and_report_launch_failure() {
    for args in [
        vec!["ls"],
        vec!["profile", "list"],
        vec!["group", "ls"],
        vec!["inspect", "missing"],
        vec!["stop", "missing"],
    ] {
        let fixture = Fixture::new();
        let result = fixture.command().args(&args).output().expect("run CLI");
        assert!(
            fixture.started(),
            "{args:?}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(
            !result.status.success(),
            "failed launch must remain an error"
        );
    }
}

#[test]
fn diagnostics_and_local_commands_do_not_start_daemon() {
    for args in [
        vec!["daemon", "status"],
        vec!["daemon", "stop"],
        vec!["debug"],
        vec!["config", "show"],
        vec!["context"],
        vec!["group", "report", "--cwd", "/tmp"],
        vec!["events"],
        vec!["--help"],
        vec!["--version"],
        vec!["reset"],
    ] {
        let fixture = Fixture::new();
        fixture.command().args(&args).output().expect("run CLI");
        assert!(!fixture.started(), "{args:?} must not start a daemon");
    }
}

#[test]
fn invalid_configuration_does_not_start_daemon() {
    let fixture = Fixture::new();
    let config = fixture.directory.path().join("invalid.toml");
    fs::write(&config, "not valid TOML").expect("invalid configuration");
    let result = fixture
        .command()
        .env("UNDERCURRENT_CONFIG", config)
        .arg("ls")
        .output()
        .expect("run CLI");
    assert!(!result.status.success());
    assert!(!fixture.started());
}

#[test]
fn unreachable_owned_daemon_is_not_replaced() {
    let fixture = Fixture::new();
    let run = fixture.directory.path().join("state/run");
    fs::create_dir_all(&run).expect("run directory");
    fs::set_permissions(&run, fs::Permissions::from_mode(0o700)).expect("private directory");
    let path = run.join(".daemon.lock.instance.lock");
    let owner = fs::File::create(&path).expect("instance lock");
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("private lock");
    owner.lock().expect("own daemon instance");
    let result = fixture.command().arg("ls").output().expect("run CLI");
    assert!(!result.status.success());
    assert!(
        !fixture.started(),
        "an unreachable owner must not be replaced"
    );
}

// macOS's default temp directory can exceed AF_UNIX limits once the product's
// run paths are appended. Keep IPC fixtures short and free of symlink ancestors.
fn physical_tempdir() -> std::io::Result<tempfile::TempDir> {
    #[cfg(target_os = "macos")]
    let base = std::path::PathBuf::from("/tmp");
    #[cfg(not(target_os = "macos"))]
    let base = std::env::temp_dir();
    tempfile::tempdir_in(std::fs::canonicalize(base)?)
}
