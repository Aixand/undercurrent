// SPDX-License-Identifier: Apache-2.0
use undercurrent_config::{
    ConfigDiscovery, ConfigError, ConfigSource, PathEnvironment, load_config,
};

fn discovery(home: &std::path::Path) -> ConfigDiscovery {
    ConfigDiscovery::from_path_env(PathEnvironment::default().with_undercurrent_home(home))
}

#[test]
fn default_response_budget_covers_json_encoded_terminal_pages_and_screens() {
    let home = tempfile::tempdir().unwrap();
    let defaults = load_config(&discovery(home.path())).unwrap().settings;
    assert!(defaults.requests.terminal_stream_bytes >= defaults.process.screen_snapshot_bytes);
    for raw_bytes in [
        defaults.process.screen_snapshot_bytes,
        defaults.terminal.replay_bytes,
    ] {
        // Binary terminal data uses JSON integer arrays in finite responses.
        // Include headroom for operation, cursor, digest and session metadata.
        let payload = serde_json::to_vec(&vec![255u8; raw_bytes]).unwrap();
        assert!(payload.len() + 4096 <= defaults.requests.response_bytes as usize);
    }
}

#[test]
fn input_allowance_is_configurable_and_must_fit_request_and_process_budgets() {
    let home = tempfile::tempdir().unwrap();
    let d = discovery(home.path());
    let defaults = load_config(&d).unwrap().settings;
    assert_eq!(defaults.services.max_input_bytes, 5 * 1024 * 1024);
    assert!(
        defaults.requests.request_bytes as usize > 6 * defaults.services.max_input_bytes + 65536
    );
    let path = home.path().join("config.toml");
    std::fs::write(&path, "[services]\nmax_input_bytes = 7340032\n").unwrap();
    assert_eq!(
        load_config(&d).unwrap().settings.services.max_input_bytes,
        7 * 1024 * 1024
    );
    for text in [
        "[services]\nmax_input_bytes = 0\n",
        "[services]\nmax_input_bytes = 8388609\n",
        "[requests]\nrequest_bytes = 5242880\n",
        "[process]\ninput_queue_bytes_per_process = 5242879\n",
    ] {
        std::fs::write(&path, text).unwrap();
        assert!(load_config(&d).is_err(), "{text}");
    }
    std::fs::write(&path, "[services]\nmax_input_bytes = 65536\n[requests]\nrequest_bytes = 131072\n[process]\ninput_queue_bytes_per_process = 1048576\n").unwrap();
    assert_eq!(
        load_config(&d).unwrap().settings.services.max_input_bytes,
        65536
    );
}

#[test]
fn yaml_and_toml_resolve_identical_defaults_and_overrides() {
    let home = tempfile::tempdir().unwrap();
    let d = discovery(home.path());
    let yaml = home.path().join("config.yaml");
    std::fs::write(&yaml, "schema_version: 0.1.0-beta\ncli:\n  command_timeout_millis: 45000\nterminal:\n  scrollback:\n    memory_lines: 0\nshortcuts:\n  prefix: ctrl-a\n").unwrap();
    let first = load_config(&d).unwrap();
    assert_eq!(first.settings.cli.command_timeout_millis, 45000);
    assert_eq!(first.settings.terminal.replay_bytes, 1048576);
    assert_eq!(first.settings.terminal.scrollback.memory_lines, 0);
    std::fs::remove_file(yaml).unwrap();
    std::fs::write(home.path().join("config.toml"), "schema_version = \"0.1.0-beta\"\n[cli]\ncommand_timeout_millis = 45000\n[terminal.scrollback]\nmemory_lines = 0\n[shortcuts]\nprefix = 'ctrl-a'\n").unwrap();
    assert_eq!(first.settings, load_config(&d).unwrap().settings);
}

#[test]
fn telemetry_section_presence_does_not_change_omitted_field_defaults() {
    let cases = [
        ("{}\n", "", true, false),
        ("telemetry: {}\n", "[telemetry]\n", true, false),
        (
            "telemetry:\n  debug_enabled: true\n",
            "[telemetry]\ndebug_enabled = true\n",
            true,
            true,
        ),
        (
            "telemetry:\n  diagnostics_enabled: false\n",
            "[telemetry]\ndiagnostics_enabled = false\n",
            false,
            false,
        ),
        (
            "telemetry:\n  diagnostics_enabled: false\n  debug_enabled: true\n",
            "[telemetry]\ndiagnostics_enabled = false\ndebug_enabled = true\n",
            false,
            true,
        ),
    ];
    for (yaml, toml, diagnostics, debug) in cases {
        for (extension, contents) in [("yaml", yaml), ("toml", toml)] {
            let home = tempfile::tempdir().unwrap();
            std::fs::write(home.path().join(format!("config.{extension}")), contents).unwrap();
            let loaded = load_config(&discovery(home.path())).unwrap();
            assert_eq!(
                loaded.config.telemetry.diagnostics_enabled, diagnostics,
                "{extension}: {contents}"
            );
            assert_eq!(
                loaded.config.telemetry.debug_enabled, debug,
                "{extension}: {contents}"
            );
        }
    }
}

#[test]
fn discovery_rejects_ambiguity_and_never_falls_back_from_invalid_yaml() {
    let home = tempfile::tempdir().unwrap();
    let d = discovery(home.path());
    let loaded = load_config(&d).unwrap();
    assert_eq!(loaded.source, ConfigSource::Defaults);
    assert_eq!(loaded.settings.shortcuts.prefix, "alt-u");
    std::fs::write(home.path().join("config.yaml"), "{}").unwrap();
    assert_eq!(load_config(&d).unwrap().settings.shortcuts.prefix, "alt-u");
    for name in ["config.yml", "config.toml"] {
        std::fs::write(home.path().join(name), "").unwrap();
        assert!(matches!(
            load_config(&d),
            Err(ConfigError::Ambiguous { .. })
        ));
        std::fs::remove_file(home.path().join(name)).unwrap();
    }
    std::fs::write(home.path().join("config.yaml"), "invalid: [").unwrap();
    assert!(load_config(&d).is_err());
    std::fs::rename(
        home.path().join("config.yaml"),
        home.path().join("config.yaml.bak"),
    )
    .unwrap();
    assert!(load_config(&d).is_ok());
    assert!(!home.path().join("state").exists());
}

#[test]
fn separate_shortcut_files_are_not_loaded() {
    let home = tempfile::tempdir().unwrap();
    let d = discovery(home.path());
    std::fs::write(home.path().join("shortcuts.toml"), "prefix = 'ctrl-a'").unwrap();
    assert_eq!(load_config(&d).unwrap().settings.shortcuts.prefix, "alt-u");
    std::fs::write(
        home.path().join("config.yaml"),
        "shortcuts:\n  prefix: ctrl-b",
    )
    .unwrap();
    assert_eq!(load_config(&d).unwrap().settings.shortcuts.prefix, "ctrl-b");
}

#[test]
fn strict_yaml_rejects_ambiguous_documents_and_unknown_nested_fields() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("config.yaml");
    for source in [
        "cli:\n  command_timeout_millis: 1\n  command_timeout_millis: 2",
        "cli: {}\ncli: {}",
        "---\n{}\n---\n{}",
        "cli: &a {}",
        "cli: *a",
        "cli: !custom {}",
        "process:\n  spawn_workres: 8",
        "terminal:\n  scrollback:\n    memory_lines: unlimited",
        "cli:\n  command_timeout_millis: false",
        "shortcuts: []",
        "schema_version: 99",
        "local:\n  endpoint: /tmp/retired.sock",
        "cli:\n  command_timeout_millis: -1",
        "terminal:\n  scrollback:\n    memory_lines: 18446744073709551615",
    ] {
        std::fs::write(&path, source).unwrap();
        assert!(
            load_config(&discovery(home.path())).is_err(),
            "accepted {source}"
        );
    }
}

#[test]
fn config_errors_and_reports_do_not_echo_private_values() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("config.yaml");
    std::fs::write(&path, "process:\n  spawn_workers: PRIVATE_SENTINEL").unwrap();
    let error = load_config(&discovery(home.path())).unwrap_err();
    assert!(error.to_string().contains("process.spawn_workers"));
    assert!(!format!("{error:?} {error}").contains("PRIVATE_SENTINEL"));
    std::fs::write(&path, "default_profile: PRIVATE_SENTINEL").unwrap();
    let report = load_config(&discovery(home.path()))
        .unwrap()
        .redacted_report()
        .to_string();
    assert!(!report.contains("PRIVATE_SENTINEL"));
    assert!(!report.contains(home.path().to_str().unwrap()));
}

#[test]
fn file_size_and_history_allocation_are_bounded() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("config.yaml");
    std::fs::write(
        &path,
        " ".repeat(undercurrent_config::MAX_CONFIG_BYTES as usize + 1),
    )
    .unwrap();
    assert!(load_config(&discovery(home.path())).is_err());
    std::fs::write(&path, "terminal:\n  scrollback:\n    memory_lines: 10001").unwrap();
    assert!(
        load_config(&discovery(home.path()))
            .unwrap_err()
            .to_string()
            .contains("5000000 cells")
    );
    std::fs::write(&path, "terminal:\n  scrollback:\n    memory_lines: 10000").unwrap();
    assert!(load_config(&discovery(home.path())).is_ok());
}
