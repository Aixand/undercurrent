// SPDX-License-Identifier: Apache-2.0

use std::io::Read;
use std::path::Path;
use std::process::Command;

fn uc(home: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_uc"));
    command.env("UNDERCURRENT_HOME", home);
    command
}

fn contents(path: &Path) -> (serde_json::Value, String) {
    let mut zip = zip::ZipArchive::new(std::fs::File::open(path).unwrap()).unwrap();
    let manifest: serde_json::Value =
        serde_json::from_reader(zip.by_name("manifest.json").unwrap()).unwrap();
    let mut all = String::new();
    for index in 0..zip.len() {
        zip.by_index(index)
            .unwrap()
            .read_to_string(&mut all)
            .unwrap();
    }
    (manifest, all)
}

#[test]
fn invalid_configuration_still_yields_a_private_redacted_support_archive() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir(&home).unwrap();
    std::fs::write(
        home.join("config.yaml"),
        "telemetry: [SECRET_SOURCE_PAYLOAD\n",
    )
    .unwrap();
    let failure = uc(&home)
        .args(["--json", "config", "validate"])
        .output()
        .unwrap();
    assert!(!failure.status.success());
    let error: serde_json::Value = serde_json::from_slice(&failure.stderr).unwrap();
    let id = error["incident_id"].as_str().unwrap();
    assert!(undercurrent_diagnostics::valid_incident(id));
    let output = root.path().join("report.zip");
    let collected = uc(&home)
        .args([
            "--json",
            "debug",
            "collect",
            "--offline",
            "--incident",
            id,
            "--output",
        ])
        .arg(&output)
        .output()
        .unwrap();
    assert!(
        collected.status.success(),
        "{}",
        String::from_utf8_lossy(&collected.stderr)
    );
    let (manifest, all) = contents(&output);
    assert_eq!(manifest["partial"], true);
    assert_eq!(manifest["incident_id"], id);
    assert!(!all.contains("SECRET_SOURCE_PAYLOAD"));
    assert!(!all.contains(home.to_str().unwrap()));
    assert!(all.contains("invalid_configuration"));
    assert!(!home.join("state").exists());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&output).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    let before = std::fs::read(&output).unwrap();
    let repeated = uc(&home)
        .args(["debug", "collect", "--offline", "--output"])
        .arg(&output)
        .output()
        .unwrap();
    assert!(!repeated.status.success());
    assert_eq!(std::fs::read(output).unwrap(), before);
}

#[test]
fn collection_does_not_start_an_absent_daemon_and_redacts_raw_logs() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(home.join("logs")).unwrap();
    std::fs::write(
        home.join("logs/daemon.jsonl"),
        "{\"fields\":{\"error\":\"SECRET\",\"operation\":\"start_session\"}}\ninvalid SECRET\n",
    )
    .unwrap();
    let output = root.path().join("report.zip");
    let collected = uc(&home)
        .args(["--timeout", "2s", "debug", "collect", "--output"])
        .arg(&output)
        .output()
        .unwrap();
    assert!(
        collected.status.success(),
        "{}",
        String::from_utf8_lossy(&collected.stderr)
    );
    let (manifest, all) = contents(&output);
    assert_eq!(manifest["terminal_content_included"], false);
    assert!(all.contains("start_session"));
    assert!(!all.contains("SECRET"));
    assert!(!home.join("state/undercurrent.db").exists());
    assert!(all.contains("not_running") || all.contains("unavailable"));
}

#[test]
fn private_backtraces_require_opt_in_and_missing_live_details_are_reported() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let directory = home.join("logs/incidents");
    undercurrent_diagnostics::prepare_private_directory(&directory).unwrap();
    let incident = undercurrent_diagnostics::new_incident();
    undercurrent_diagnostics::write_private_new(
        &directory.join(format!("00000000000000000001-{incident}.backtrace")),
        b"PRIVATE_BUILD_PATH_CANARY",
    )
    .unwrap();
    for include in [false, true] {
        let output = root.path().join(format!("backtrace-{include}.zip"));
        let mut command = uc(&home);
        command.args([
            "debug",
            "collect",
            "--offline",
            "--incident",
            &incident,
            "--session",
            "unavailable-session",
            "--profile",
            "private-profile-name",
        ]);
        if include {
            command.arg("--include-backtraces");
        }
        let collected = command.arg("--output").arg(&output).output().unwrap();
        assert!(
            collected.status.success(),
            "{}",
            String::from_utf8_lossy(&collected.stderr)
        );
        let (manifest, all) = contents(&output);
        assert_eq!(all.contains("PRIVATE_BUILD_PATH_CANARY"), include);
        assert!(!all.contains("private-profile-name"));
        for source in ["session", "profile"] {
            assert!(
                manifest["sources"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|entry| entry["source"] == source && entry["status"] == "unavailable")
            );
        }
        assert!(
            manifest["sources"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry["source"] == "backtraces"
                    && entry["status"] == if include { "collected" } else { "redacted" })
        );
    }

    let invalid = root.path().join("invalid-shape.zip");
    let result = uc(&home)
        .args(["--jsonl", "debug", "collect", "--offline", "--output"])
        .arg(&invalid)
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert!(!invalid.exists());
}
