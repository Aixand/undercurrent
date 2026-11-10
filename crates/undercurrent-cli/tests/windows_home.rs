// SPDX-License-Identifier: Apache-2.0
#![cfg(windows)]

#[test]
fn configuration_and_shell_installation_use_userprofile_without_home() {
    let home = tempfile::tempdir().unwrap();
    for arguments in [vec!["config", "validate"], vec!["init", "bash"]] {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_uc"))
            .args(arguments)
            .env_remove("HOME")
            .env_remove("UNDERCURRENT_HOME")
            .env_remove("UNDERCURRENT_CONFIG")
            .env("USERPROFILE", home.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert!(home.path().join(".bashrc").is_file());
    assert!(!home.path().join(".aixand/undercurrent/run").exists());
}
