// SPDX-License-Identifier: Apache-2.0
#![cfg(windows)]

use std::process::Command;

fn shells() -> Vec<std::path::PathBuf> {
    let mut shells = vec![
        std::path::PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32/WindowsPowerShell/v1.0/powershell.exe"),
    ];
    if Command::new("pwsh.exe")
        .args(["-NoProfile", "-Command", "exit 0"])
        .status()
        .is_ok_and(|s| s.success())
    {
        shells.push("pwsh.exe".into());
    }
    shells
}

#[test]
fn powershell_hooks_preserve_prompt_readline_and_exit_code_and_are_idempotent() {
    let root = tempfile::tempdir().unwrap();
    let hook = root.path().join("uc-hook.ps1");
    let script = root.path().join("check.ps1");
    let printed = Command::new(env!("CARGO_BIN_EXE_uc"))
        .args(["init", "powershell", "--prompt", "simple", "--print"])
        .output()
        .unwrap();
    assert!(printed.status.success());
    std::fs::write(&hook, printed.stdout).unwrap();
    std::fs::write(&script, r#"
$ErrorActionPreference = 'Stop'
$global:PromptCalls = 0
function global:prompt { $global:PromptCalls++; 'original:' + $global:LASTEXITCODE + '> ' }
function global:PSConsoleHostReadLine { 'Write-Output PRIVATE_COMMAND' }
$global:LASTEXITCODE = 17
. $env:UC_TEST_HOOK
. $env:UC_TEST_HOOK
$global:LASTEXITCODE = 17
$text = prompt
if ($text -ne '[uc] original:17> ' -or $global:PromptCalls -ne 1 -or $global:LASTEXITCODE -ne 17) { throw 'prompt changed' }
$line = PSConsoleHostReadLine
if ($line -ne 'Write-Output PRIVATE_COMMAND' -or $global:LASTEXITCODE -ne 17) { throw 'readline changed' }
if ($env:UC_CAPTURE_SHELL_PID -ne 'previous') { throw 'helper environment leaked' }
Write-Output 'HOOK_OK'
Remove-Variable LASTEXITCODE -Scope Global
Set-StrictMode -Version Latest
$global:__uc_original_prompt = { 'strict> ' }
if ((prompt) -ne '[uc] strict> ') { throw 'strict prompt changed' }
Write-Output 'STRICT_OK'
"#).unwrap();
    for shell in shells() {
        let output = Command::new(&shell)
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
            ])
            .arg(&script)
            .env("UC_TEST_HOOK", &hook)
            .env("UC_SESSION", "managed")
            .env("UC_SHELL_HELPER", root.path().join("missing-helper.exe"))
            .env("UC_CAPTURE_SHELL_PID", "previous")
            .env_remove("UC_PROMPT")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}: {}",
            shell.display(),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("\x1b]9091;uc=ready\x07"));
        assert!(stdout.contains("\x1b]9091;uc=running\x07"));
        assert!(stdout.contains("HOOK_OK"));
        assert!(stdout.contains("STRICT_OK"));
        assert!(!stdout.contains("PRIVATE_COMMAND"));
        assert!(output.stderr.is_empty());
        let outside = Command::new(&shell)
            .args(["-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", ". $env:UC_TEST_HOOK; if (Get-Variable __UC_SHELL_HOOKS_ACTIVE -Scope Global -ErrorAction Ignore) { exit 1 }"])
            .env("UC_TEST_HOOK", &hook).env_remove("UC_SESSION").output().unwrap();
        assert!(outside.status.success());
        assert!(outside.stdout.is_empty());
        assert!(outside.stderr.is_empty());
    }
}

#[test]
fn explicit_powershell_profile_preserves_contents_and_is_idempotent() {
    let root = tempfile::tempdir().unwrap();
    let profile = root
        .path()
        .join("redirected documents/PowerShell/profile.ps1");
    std::fs::create_dir_all(profile.parent().unwrap()).unwrap();
    std::fs::write(&profile, b"# existing profile\r\n").unwrap();
    for _ in 0..2 {
        let output = Command::new(env!("CARGO_BIN_EXE_uc"))
            .args(["init", "powershell", "--profile"])
            .arg(&profile)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let contents = std::fs::read_to_string(profile).unwrap();
    assert!(contents.starts_with("# existing profile\r\n"));
    assert_eq!(contents.matches("# >>> uc detection hooks >>>").count(), 1);
    let completion = Command::new(env!("CARGO_BIN_EXE_uc"))
        .args(["completion", "powershell"])
        .output()
        .unwrap();
    assert!(completion.status.success());
    assert!(String::from_utf8_lossy(&completion.stdout).contains("Register-ArgumentCompleter"));
}
