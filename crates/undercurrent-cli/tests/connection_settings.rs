// SPDX-License-Identifier: Apache-2.0

//! Process-level settings contracts; no test changes the real user's settings.

use std::path::PathBuf;
use std::process::{Command, Output};

use serde_json::Value;

struct Fixture {
    home: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        Self {
            #[cfg(unix)]
            home: tempfile::Builder::new()
                .prefix("ucs-")
                .tempdir_in(std::fs::canonicalize("/tmp").unwrap())
                .unwrap(),
            #[cfg(windows)]
            home: tempfile::tempdir().unwrap(),
        }
    }
    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_uc"));
        command
            .env("HOME", self.home.path())
            .env("USERPROFILE", self.home.path())
            .env_remove("UNDERCURRENT_HOME")
            .env_remove("UNDERCURRENT_CONFIG")
            .env_remove("XDG_CONFIG_HOME")
            .env_remove("XDG_STATE_HOME")
            .args(["--json"])
            .args(args);
        command
    }
    fn run(&self, args: &[&str]) -> Output {
        self.command(args).output().unwrap()
    }
    fn ok(&self, args: &[&str]) -> Value {
        let result = self.run(args);
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        serde_json::from_slice(&result.stdout).unwrap()
    }
    fn instance(&self, name: &str) -> PathBuf {
        self.home.path().join(name)
    }
    fn add(&self, name: &str) {
        self.ok(&[
            "settings",
            "profiles",
            "add",
            name,
            "--home",
            self.instance(name).to_str().unwrap(),
            "--auto-start",
            "never",
        ]);
    }
}

#[test]
fn default_is_permanent_offline_and_settings_live_outside_instance_data() {
    let f = Fixture::new();
    let profiles = f.ok(&["settings", "profiles", "list"]);
    assert_eq!(profiles["active_profile"], "default");
    assert_eq!(
        profiles["profiles"]["default"]["home"],
        f.home
            .path()
            .join(".aixand")
            .join("undercurrent")
            .to_str()
            .unwrap()
    );
    assert!(!f.home.path().join(".aixand").exists());
    f.add("workspace");
    f.ok(&["settings", "set", "profile", "workspace"]);
    let shown = f.ok(&["settings", "show"]);
    assert_eq!(
        shown["effective"]["home"],
        f.instance("workspace").to_str().unwrap()
    );
    assert_eq!(shown["effective"]["auto_start"], "never");
    assert_eq!(shown["effective"]["source"], "saved_profile");
    assert_eq!(
        shown["settings_file"],
        f.home
            .path()
            .join(".aixand")
            .join("undercurrent-cli")
            .join("settings.toml")
            .to_str()
            .unwrap()
    );
    assert!(
        !f.run(&["settings", "profiles", "remove", "default"])
            .status
            .success()
    );
    assert!(
        !f.run(&[
            "settings",
            "profiles",
            "add",
            "default",
            "--home",
            f.home.path().to_str().unwrap()
        ])
        .status
        .success()
    );
    f.ok(&["settings", "profiles", "remove", "workspace"]);
    assert_eq!(f.ok(&["settings", "show"])["active_profile"], "default");
    assert!(!f.instance("workspace").join("state").exists());
}

#[test]
fn explicit_flag_environment_and_saved_profile_have_stable_precedence() {
    let f = Fixture::new();
    f.add("workspace");
    f.ok(&["settings", "set", "profile", "workspace"]);
    let override_home = f.instance("override");
    let env = f
        .command(&["settings", "show"])
        .env("UNDERCURRENT_HOME", &override_home)
        .output()
        .unwrap();
    let value: Value = serde_json::from_slice(&env.stdout).unwrap();
    assert_eq!(value["effective"]["source"], "environment");
    assert_eq!(value["effective"]["home"], override_home.to_str().unwrap());
    let flag = f
        .command(&["--connection", "default", "settings", "show"])
        .env("UNDERCURRENT_HOME", &override_home)
        .output()
        .unwrap();
    let value: Value = serde_json::from_slice(&flag.stdout).unwrap();
    assert_eq!(value["active_profile"], "workspace");
    assert_eq!(value["effective"]["profile"], "default");
    assert_eq!(value["effective"]["source"], "flag");
    assert_eq!(f.ok(&["settings", "show"])["active_profile"], "workspace");
}

#[test]
fn a_broken_or_unavailable_workspace_does_not_prevent_switching() {
    let f = Fixture::new();
    f.add("workspace");
    std::fs::create_dir_all(f.instance("workspace")).unwrap();
    std::fs::write(f.instance("workspace").join("config.yaml"), "invalid: [").unwrap();
    f.ok(&["settings", "set", "profile", "workspace"]);
    assert!(!f.run(&["config", "show"]).status.success());
    f.ok(&["settings", "set", "profile", "default"]);
    assert_eq!(f.ok(&["settings", "show"])["active_profile"], "default");
    let switched = f
        .command(&["settings", "set", "profile", "default"])
        .env("UNDERCURRENT_HOME", "relative-invalid-home")
        .output()
        .unwrap();
    assert!(switched.status.success());
}

#[test]
fn connect_only_profiles_never_bootstrap_an_empty_instance_or_fall_back() {
    let f = Fixture::new();
    f.add("workspace");
    f.ok(&["settings", "set", "profile", "workspace"]);
    let result = f.run(&["ls"]);
    assert!(!result.status.success());
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("selected connection is not running"),
        "{stderr}"
    );
    assert!(!f.instance("workspace").join("state").exists());
    assert!(!f.home.path().join(".aixand/undercurrent/state").exists());
    assert_eq!(f.ok(&["settings", "show"])["active_profile"], "workspace");
}

#[test]
fn application_registration_is_atomic_owned_and_preserves_user_choices_on_retry() {
    let f = Fixture::new();
    let home = f.instance("workspace");
    let register = [
        "settings",
        "profiles",
        "register",
        "example-app",
        "--home",
        home.to_str().unwrap(),
        "--owner",
        "example-app",
        "--select-if-new",
    ];
    let first = f.ok(&register);
    assert_eq!(first["created"], true);
    assert_eq!(first["active_profile"], "example-app");
    f.ok(&["settings", "set", "profile", "default"]);
    let retry = f.ok(&register);
    assert_eq!(retry["created"], false);
    assert_eq!(retry["active_profile"], "default");
    let conflict = f.run(&[
        "settings",
        "profiles",
        "register",
        "example-app",
        "--home",
        home.to_str().unwrap(),
        "--owner",
        "another-app",
        "--select-if-new",
    ]);
    assert!(!conflict.status.success());
    assert_eq!(
        f.ok(&["settings", "profiles", "show", "example-app"])["connection"]["owner"],
        "example-app"
    );
    f.add("manual");
    assert!(
        !f.run(&[
            "settings",
            "profiles",
            "register",
            "manual",
            "--home",
            home.to_str().unwrap(),
            "--owner",
            "example-app"
        ])
        .status
        .success()
    );
}

#[test]
fn selected_profile_controls_configuration_and_unknown_selections_fail() {
    let f = Fixture::new();
    f.add("workspace");
    std::fs::create_dir_all(f.instance("workspace")).unwrap();
    std::fs::write(
        f.instance("workspace").join("config.yaml"),
        "cli:\n  list_page_size: 17\n",
    )
    .unwrap();
    f.ok(&["settings", "set", "profile", "workspace"]);
    let result = f.run(&["config", "show"]);
    assert!(result.status.success());
    assert!(String::from_utf8_lossy(&result.stdout).contains("\"list_page_size\": 17"));
    assert!(!f.run(&["--connection", "missing", "ls"]).status.success());
    assert!(
        !f.run(&["settings", "set", "profile", "missing"])
            .status
            .success()
    );
    assert!(
        !f.run(&[
            "settings", "profiles", "add", "relative", "--home", "relative"
        ])
        .status
        .success()
    );
    assert_eq!(f.ok(&["settings", "show"])["active_profile"], "workspace");
}

#[cfg(unix)]
#[test]
fn settings_reject_symlinks_and_unsafe_file_permissions() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let f = Fixture::new();
    f.add("workspace");
    let path = f
        .home
        .path()
        .join(".aixand")
        .join("undercurrent-cli")
        .join("settings.toml");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(!f.run(&["settings", "show"]).status.success());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let preserved = path.with_extension("preserved");
    std::fs::rename(&path, &preserved).unwrap();
    symlink(&preserved, &path).unwrap();
    assert!(!f.run(&["settings", "show"]).status.success());
    assert_eq!(
        f.ok(&["--connection", "default", "config", "show"])["source"],
        "defaults"
    );
}

#[test]
fn default_preserves_legacy_layout_checks_while_named_connections_remain_isolated() {
    let f = Fixture::new();
    let legacy = f
        .home
        .path()
        .join(".local")
        .join("state")
        .join("undercurrent");
    std::fs::create_dir_all(&legacy).unwrap();
    std::fs::write(legacy.join("undercurrent.db"), b"retained legacy data").unwrap();
    assert!(!f.run(&["config", "validate"]).status.success());
    f.add("workspace");
    f.ok(&["settings", "set", "profile", "workspace"]);
    f.ok(&["config", "validate"]);
    let explicit = f
        .command(&["--connection", "default", "config", "validate"])
        .env("UNDERCURRENT_HOME", f.instance("workspace"))
        .output()
        .unwrap();
    assert!(!explicit.status.success());
    assert!(String::from_utf8_lossy(&explicit.stderr).contains("legacy"));
    f.ok(&["settings", "set", "profile", "default"]);
    assert!(!f.run(&["config", "validate"]).status.success());
    assert!(
        !f.home
            .path()
            .join(".aixand")
            .join("undercurrent")
            .join("state")
            .exists()
    );
}

#[test]
fn support_collection_survives_invalid_global_settings_without_falling_back() {
    let f = Fixture::new();
    f.add("workspace");
    let path = f
        .home
        .path()
        .join(".aixand")
        .join("undercurrent-cli")
        .join("settings.toml");
    std::fs::write(&path, "malformed = [").unwrap();
    let output = f.home.path().join("support.zip");
    let report = f.ok(&[
        "debug",
        "collect",
        "--offline",
        "--output",
        output.to_str().unwrap(),
    ]);
    assert_eq!(report["partial"], true);
    let mut archive = zip::ZipArchive::new(std::fs::File::open(output).unwrap()).unwrap();
    let manifest: Value =
        serde_json::from_reader(archive.by_name("manifest.json").unwrap()).unwrap();
    assert!(
        manifest["sources"]
            .as_array()
            .unwrap()
            .iter()
            .any(|source| source["reason"] == "invalid_cli_selection")
    );
    assert!(
        !f.home
            .path()
            .join(".aixand")
            .join("undercurrent")
            .join("state")
            .exists()
    );
    assert_eq!(std::fs::read_to_string(path).unwrap(), "malformed = [");
}
