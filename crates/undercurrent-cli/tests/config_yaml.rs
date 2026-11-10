// SPDX-License-Identifier: Apache-2.0
use std::process::Command;

#[test]
fn yaml_show_and_validation_work_offline_and_reject_daemon_invalid_settings() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("config.yaml");
    std::fs::write(
        &path,
        "cli:\n  list_page_size: 17\nterminal:\n  scrollback:\n    memory_lines: 0\n",
    )
    .unwrap();
    let run = |action: &str| {
        Command::new(env!("CARGO_BIN_EXE_uc"))
            .env("UNDERCURRENT_HOME", home.path())
            .args(["--json", "config", action])
            .output()
            .unwrap()
    };
    let validated = run("validate");
    assert!(
        validated.status.success(),
        "{}",
        String::from_utf8_lossy(&validated.stderr)
    );
    let shown = run("show");
    assert!(shown.status.success());
    let text = String::from_utf8(shown.stdout).unwrap();
    assert!(text.contains("on_disk_effective_configuration"));
    assert!(text.contains("memory_lines"));
    assert!(!home.path().join("run").exists());
    std::fs::write(path, "process:\n  spawn_workers: 0\n").unwrap();
    let invalid = run("validate");
    assert!(!invalid.status.success());
    assert!(!home.path().join("run").exists());
}
