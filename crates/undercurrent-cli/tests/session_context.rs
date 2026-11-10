// SPDX-License-Identifier: Apache-2.0

use std::process::Command;

fn uc() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_uc"));
    command.env_remove("UC_SESSION");
    // Local checks must not read configuration or connect to a daemon.
    command.env("UNDERCURRENT_CONFIG", "/nonexistent/uc-context-test.toml");
    command
}

#[test]
fn context_outside_and_quiet_have_useful_exit_codes_without_daemon() {
    let outside = uc().args(["context", "--json"]).output().unwrap();
    assert_eq!(outside.status.code(), Some(1));
    let value: serde_json::Value = serde_json::from_slice(&outside.stdout).unwrap();
    assert_eq!(value["inside"], false);
    assert!(value["session_id"].is_null());
    assert!(outside.stderr.is_empty());
    for (session, expected) in [("", 1), ("managed-id", 0)] {
        let result = uc()
            .env("UC_SESSION", session)
            .args(["context", "--quiet"])
            .output()
            .unwrap();
        assert_eq!(result.status.code(), Some(expected));
        assert!(result.stdout.is_empty());
        assert!(result.stderr.is_empty());
    }
}

#[test]
fn unavailable_name_does_not_hide_inherited_session() {
    let result = uc()
        .env("UC_SESSION", "managed-id")
        .args(["context", "--json"])
        .output()
        .unwrap();
    assert!(result.status.success());
    let value: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(value["inside"], true);
    assert_eq!(value["session_id"], "managed-id");
    assert!(value["name_lookup_error"].is_string());
}

#[cfg(unix)]
#[test]
fn bash_and_zsh_prompts_are_opt_in_idempotent_and_safe() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir().unwrap();
    let fake_uc = temp.path().join("uc");
    std::fs::write(
        &fake_uc,
        "#!/bin/sh\nprintf '%s\\n' 'backend$(touch BAD)%F{red}'\nprintf x >> \"$CALLS\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_uc, std::fs::Permissions::from_mode(0o700)).unwrap();
    for shell in ["bash", "zsh"] {
        if Command::new(shell).arg("--version").output().is_err() {
            eprintln!("{shell} unavailable; skipping its runtime checks");
            continue;
        }
        for (mode, inside, expected) in [
            ("off", true, "original> "),
            ("simple", false, "original> "),
            ("simple", true, "[uc] original> "),
            (
                "session",
                true,
                "[uc:backend__touch_BAD__F_red_] original> ",
            ),
        ] {
            let hook = uc()
                .args(["init", shell, "--prompt", mode, "--print"])
                .output()
                .unwrap();
            assert!(hook.status.success());
            let hook_file = temp.path().join("hook");
            let calls = temp.path().join("calls");
            std::fs::write(&calls, "").unwrap();
            std::fs::write(&hook_file, hook.stdout).unwrap();
            let mut command = Command::new(shell);
            command.arg(if shell == "bash" { "--norc" } else { "-f" });
            let result = command.args(["-c", "PS1='original> '; . \"$HOOK\" >/dev/null; . \"$HOOK\" >/dev/null; if command -v __uc_update_prompt >/dev/null; then __uc_update_prompt; __uc_update_prompt; fi; printf '%s' \"$PS1\""])
                .current_dir(temp.path()).env("HOOK", &hook_file).env("CALLS", &calls)
                .env("PATH", format!("{}:/usr/bin:/bin", temp.path().display()))
                .env_remove("UC_PROMPT").env_remove("__UC_SHELL_HOOKS_ACTIVE")
                .env("UC_SESSION", if inside { "managed-id" } else { "" })
                .output().unwrap();
            assert!(result.status.success(), "{shell}: {:?}", result.stderr);
            assert_eq!(
                String::from_utf8(result.stdout).unwrap(),
                expected,
                "{shell} {mode}"
            );
            assert!(!temp.path().join("BAD").exists());
            assert_eq!(
                std::fs::read_to_string(&calls).unwrap(),
                if mode == "session" && inside { "x" } else { "" }
            );
        }
    }
}

#[test]
fn fish_prompt_preserves_original_status_and_is_idempotent() {
    let fish = std::env::var_os("UC_TEST_FISH").unwrap_or_else(|| "fish".into());
    // Resolve before removing PATH below to exercise the prompt fallback.
    let fish = std::path::Path::new(&fish)
        .canonicalize()
        .ok()
        .or_else(|| {
            std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
                .find_map(|directory| directory.join(&fish).canonicalize().ok())
        })
        .unwrap_or_else(|| fish.into());
    if Command::new(&fish).arg("--version").output().is_err() {
        eprintln!("fish unavailable; skipping its runtime checks");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    for (mode, inside, override_mode, expected) in [
        ("off", true, "off", "original:7"),
        ("simple", false, "simple", "original:7"),
        ("simple", true, "simple", "[uc] original:7"),
        ("session", true, "session", "[uc] original:7"),
        ("simple", true, "off", "original:7"),
    ] {
        let hook = uc()
            .args(["init", "fish", "--prompt", mode, "--print"])
            .output()
            .unwrap();
        assert!(hook.status.success());
        let hook_file = temp.path().join("hook.fish");
        std::fs::write(&hook_file, hook.stdout).unwrap();
        let result = Command::new(&fish).args(["--no-config", "-c", "function fish_prompt; printf 'original:%s' $status; end; source $HOOK >/dev/null; source $HOOK >/dev/null; function previous; return 7; end; previous; fish_prompt"])
            .env("HOOK", &hook_file).env("UC_SESSION", if inside { "managed-id" } else { "" })
            .env("UC_PROMPT", override_mode).env_remove("__UC_SHELL_HOOKS_ACTIVE")
            // Name lookup intentionally unavailable: exercise the [uc] fallback.
            .env("PATH", "/nonexistent").output().unwrap();
        assert!(result.status.success(), "{:?}", result.stderr);
        assert_eq!(
            String::from_utf8(result.stdout).unwrap(),
            expected,
            "fish {mode}"
        );
    }
}

#[test]
#[cfg(unix)]
fn shell_hooks_never_report_group_directories_even_with_legacy_registration() {
    use std::os::unix::fs::PermissionsExt;
    let fixture = tempfile::tempdir().unwrap();
    let reporter = fixture.path().join("reporter");
    let hook = fixture.path().join("hook");
    let reports = fixture.path().join("reports");
    let changed = fixture.path().join("directory with spaces");
    std::fs::create_dir(&changed).unwrap();
    std::fs::write(
        &reporter,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$REPORTS\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&reporter, std::fs::Permissions::from_mode(0o700)).unwrap();
    for shell in ["bash", "zsh"] {
        if Command::new(shell).arg("--version").output().is_err() {
            continue;
        }
        std::fs::write(
            &hook,
            uc().args(["init", shell, "--print"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap();
        for opted_in in [false, true] {
            std::fs::write(&reports, "").unwrap();
            let script = if opted_in {
                "export UC_GROUP_ROOT_PID=$$; . \"$HOOK\" >/dev/null; cd \"$CHANGED\"; __uc_precmd"
            } else {
                "unset UC_GROUP_ROOT_PID; . \"$HOOK\" >/dev/null; cd \"$CHANGED\"; __uc_precmd"
            };
            let result = Command::new(shell)
                .args(["-ic", script])
                .env("HOME", fixture.path())
                .env("UC_SESSION", "managed")
                .env("UC_GROUP_REPORTER", &reporter)
                .env("REPORTS", &reports)
                .env("HOOK", &hook)
                .env("CHANGED", &changed)
                .env("TEST_SHELL", shell)
                .env("UC_PROMPT", "off")
                .output()
                .unwrap();
            assert!(
                result.status.success(),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
            let records = std::fs::read_to_string(&reports).unwrap();
            assert!(records.is_empty(), "{shell}: {records}");
        }
    }
}

#[test]
fn legacy_group_reports_are_inert_without_configuration_or_registry_creation() {
    let root = tempfile::tempdir().unwrap();
    let registry = root.path().join("missing-registry");
    for args in [
        vec!["group", "report", "--cwd", "/new"],
        vec!["group", "report", "--closed"],
    ] {
        let result = uc()
            .args(args)
            .env("UC_GROUP_REGISTRY", &registry)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(result.stdout.is_empty());
        assert!(!registry.exists());
    }
}

#[test]
#[cfg(unix)]
fn capture_helper_failure_preserves_prompt_command_status() {
    let fixture = tempfile::tempdir().unwrap();
    for shell in ["bash", "zsh"] {
        if Command::new(shell).arg("--version").output().is_err() {
            continue;
        }
        let hook = fixture.path().join(shell);
        let printed = uc().args(["init", shell, "--print"]).output().unwrap();
        assert!(printed.status.success());
        std::fs::write(&hook, printed.stdout).unwrap();
        let output = Command::new(shell)
            .arg(if shell == "bash" { "--norc" } else { "-f" })
            .args([
                "-c",
                ". \"$HOOK\" >/dev/null; (exit 7); __uc_precmd >/dev/null; printf '%s' \"$?\"",
            ])
            .env("HOOK", &hook)
            .env("UC_SESSION", "test-session")
            .env("UC_SHELL_HELPER", "/bin/false")
            .env("UC_PROMPT", "off")
            .output()
            .unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, b"7", "{shell}");
    }
}
