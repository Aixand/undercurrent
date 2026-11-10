// SPDX-License-Identifier: Apache-2.0

use std::process::{Command, Stdio};

#[test]
fn daemon_and_helper_failures_survive_discarded_stderr() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir(&home).unwrap();
    std::fs::write(home.join("config.yaml"), "telemetry: [PRIVATE_CONFIG\n").unwrap();
    let incident = undercurrent_diagnostics::new_incident();
    for program in [
        env!("CARGO_BIN_EXE_uc-daemon"),
        env!("CARGO_BIN_EXE_uc-session-host"),
    ] {
        let status = Command::new(program)
            .env("UNDERCURRENT_HOME", &home)
            .env(undercurrent_diagnostics::INCIDENT_ENV, &incident)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(!status.success());
    }
    let records = std::fs::read_dir(home.join("logs/incidents"))
        .unwrap()
        .map(|entry| {
            let bytes = std::fs::read(entry.unwrap().path()).unwrap();
            assert!(!String::from_utf8_lossy(&bytes).contains("PRIVATE_CONFIG"));
            let record: undercurrent_diagnostics::FailureRecord =
                serde_json::from_slice(&bytes).unwrap();
            assert_eq!(record.incident_id, incident);
            record
        })
        .collect::<Vec<_>>();
    assert!(
        records
            .iter()
            .any(|r| r.component == undercurrent_diagnostics::Component::Daemon)
    );
    assert!(
        records
            .iter()
            .any(|r| r.component == undercurrent_diagnostics::Component::SessionHost)
    );
}
