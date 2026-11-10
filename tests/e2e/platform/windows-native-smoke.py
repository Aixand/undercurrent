#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Exercise native Windows CLI, daemon recovery, ConPTY and PowerShell context."""

import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
from _support.processes import exit_code, handle_signals, isolated_environment, run

LISTENER = ('$listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0); '
            '$listener.Start(); Write-Output "LISTENER_STARTED"; [Console]::ReadLine() | Out-Null; '
            '$listener.Stop(); Write-Output "LISTENER_CLOSED"; [Console]::ReadLine() | Out-Null')


def eventually(probe, predicate, message, timeout=15, retry_errors=False):
    deadline = time.monotonic() + timeout
    last = None
    while True:
        try:
            last = probe()
            if predicate(last):
                return last
        except subprocess.CalledProcessError as error:
            if not retry_errors:
                raise
            last = error.stderr
        if time.monotonic() >= deadline:
            raise AssertionError(f"{message}: {last}")
        time.sleep(.05)


def exercise(binaries, powershell, evidence):
    work = evidence / "working directory with spaces"
    work.mkdir(parents=True)
    environment = isolated_environment(evidence / "home")
    uc = binaries / "uc.exe"
    daemon = None
    comspec = os.environ["COMSPEC"]
    system_powershell = Path(os.environ["SystemRoot"]) / "System32/WindowsPowerShell/v1.0/powershell.exe"

    def command(*args, check=True):
        return run([uc, "--timeout", "20s", *args], env=environment, cwd=ROOT, timeout=30, check=check).stdout.decode(errors="replace").strip()

    def inspect(name):
        return json.loads(command("--json", "inspect", name))

    def save(name, value):
        (evidence / name).write_text(json.dumps(value, indent=2) + "\n")

    def start(name, *args):
        return command("start", name, "--cwd", str(work), "--stop-timeout", "100ms", *args)

    def wait_role(name, role):
        return eventually(lambda: inspect(name),
            lambda value: value["summary"]["profile_context"].get("server_role_origin") == role,
            f"{name} did not reach listener role {role}")

    def live_context(name, expected, directory, excluded=None):
        return eventually(lambda: command("exec", name, "--", comspec, "/d", "/c", "echo %UC_WINDOWS_LIVE% & cd"),
            lambda value: expected in value and str(directory) in value and (excluded is None or str(excluded) not in value),
            f"{name} live PowerShell context did not arrive", retry_errors=True)

    try:
        with (evidence / "daemon.stdout").open("wb") as output, (evidence / "daemon.stderr").open("wb") as errors:
            daemon = subprocess.Popen([str(binaries / "uc-daemon.exe")], env=environment, cwd=ROOT,
                                      stdin=subprocess.DEVNULL, stdout=output, stderr=errors,
                                      creationflags=subprocess.CREATE_NO_WINDOW)
        def ready():
            assert daemon.poll() is None, "daemon exited before readiness"
            return run([uc, "--quiet", "--timeout", "1s", "daemon", "status"],
                       env=environment, timeout=3, check=False).returncode
        eventually(ready, lambda status: status == 0, "daemon readiness timed out", timeout=20)
        epoch = json.loads(command("--json", "daemon", "status"))["server_epoch"]
        start("native-shell", "--interactive")
        command("send", "native-shell", "echo WINDOWS_NATIVE_INPUT_OK")
        eventually(lambda: command("logs", "native-shell"), lambda output: "WINDOWS_NATIVE_INPUT_OK" in output,
                   "ConPTY output timed out", timeout=10)
        before = inspect("native-shell")
        save("before-crash.json", before)
        assert before["summary"]["lifecycle"]["state"] == "running"
        assert Path(before["configuration"]["command"]["program"]).name.lower() in {"pwsh.exe", "powershell.exe"}
        assert "WINDOWS_EXEC_OK" in command("exec", "native-shell", "--dir-launch", "--env-launch",
                                           "--", comspec, "/d", "/c", "echo WINDOWS_EXEC_OK")
        daemon.kill()
        daemon.wait(timeout=10)
        command("daemon", "start")
        after = inspect("native-shell")
        save("after-crash.json", after)
        assert json.loads(command("--json", "daemon", "status"))["server_epoch"] != epoch
        assert after["summary"]["lifecycle"]["state"] == "running", "session did not survive daemon crash"
        assert before["process"] == after["process"], "process identity changed across daemon crash"
        command("send", "native-shell", "echo WINDOWS_RECOVERED_OK")
        command("stop", "native-shell")
        stopped = inspect("native-shell")
        save("after-stop.json", stopped)
        assert stopped["summary"]["lifecycle"]["state"] in {"stopped", "exited"} and stopped.get("process") is None
        command("restart", "native-shell")
        command("stop", "native-shell")

        batch = work / "native batch.cmd"
        batch.write_bytes(b"@echo off\r\necho BATCH_READY\r\nset /p UC_BATCH_INPUT=\r\necho BATCH_INPUT_%UC_BATCH_INPUT%\r\npause >nul\r\n")
        start("batch-shell", "--", str(batch))
        command("send", "batch-shell", "RECEIVED")
        eventually(lambda: command("logs", "batch-shell"), lambda output: "BATCH_INPUT_RECEIVED" in output,
                   "batch ConPTY input/output timed out", timeout=10)
        one_shot = work / "one shot.bat"
        one_shot.write_bytes(b"@echo off\r\necho BATCH_EXEC_%~1\r\n")
        assert "BATCH_EXEC_with spaces" in command("exec", "batch-shell", "--dir-launch", "--env-launch", "--", str(one_shot), "with spaces")
        command("stop", "batch-shell")

        shim = work / "CoDeX.CMD"
        shim.write_bytes(b"@echo off\r\npause >nul\r\n")
        start("profiled-shim", "--profile-mode", "fixed", "--", str(shim))
        assert inspect("profiled-shim")["summary"]["profile"] == "codex"
        assert json.loads(command("--json", "profile", "match", "--executable", "NODE.EXE"))["selected"] == "node"
        command("stop", "profiled-shim")

        start("tcp-owner", "--", str(powershell), "-NoProfile", "-Command", LISTENER)
        details = wait_role("tcp-owner", "managed_process_tcp_listener")
        assert details["summary"]["profile_context"]["runtime_role"] == "server"
        save("managed-tcp-listener.json", details)
        command("send", "tcp-owner", "close")
        wait_role("tcp-owner", None)
        command("stop", "tcp-owner")

        child_script = work / "child listener.ps1"
        child_script.write_text(LISTENER)
        child_shim = work / "child listener.cmd"
        # System PowerShell retains the child's Job Object membership.
        escaped_shell, escaped_script = (str(path).replace("%", "%%") for path in (system_powershell, child_script))
        child_shim.write_bytes(f'@echo off\r\n"{escaped_shell}" -NoProfile -File "{escaped_script}"\r\n'.encode())
        with socket.socket() as outside:
            outside.bind(("127.0.0.1", 0))
            outside.listen()
            start("tcp-tree", "--", str(child_shim))
            details = wait_role("tcp-tree", "managed_tree_tcp_listener")
            assert details["summary"]["profile_context"]["runtime_role"] == "server"
            save("managed-tree-tcp-listener.json", details)
            command("send", "tcp-tree", "close")
            wait_role("tcp-tree", None)
            command("stop", "tcp-tree")

        hook = evidence / "powershell-hook.ps1"
        hook.write_text(command("init", "powershell", "--print"), encoding="utf-8-sig")
        live = work / "live directory"
        live.mkdir()
        for index, shell in enumerate((system_powershell, powershell), 1):
            name = f"powershell-{index}"
            start(name, "--profile-mode", "fixed", "--", str(shell), "-NoProfile", "-NoLogo", "-NoExit",
                  "-ExecutionPolicy", "Bypass", "-File", str(hook))
            for value, directory, excluded in (("captured-value", live, None), ("replacement-value", work, live)):
                quoted = str(directory).replace("'", "''")
                script = f"$env:UC_WINDOWS_LIVE = '{value}'; Set-Location -LiteralPath '{quoted}'; Write-Output LIVE_CAPTURE_READY"
                command("send", name, script)
                captured = live_context(name, value, directory, excluded)
                if value == "captured-value":
                    (evidence / f"{name}-live-context.txt").write_text(captured)
                    assert inspect(name)["summary"]["profile"] == "powershell"
            command("restart", name)
            live_context(name, "%UC_WINDOWS_LIVE%", work)
            command("stop", name)
        command("daemon", "stop")
        command("reset", "--yes")
        save("result.json", {"platform": "windows", "result": "passed", "evidence": str(evidence), "binaries": str(binaries),
            "checks": ["native_default_shell", "cwd_with_spaces", "conpty_input_output", "one_shot_execution",
                       "daemon_crash_recovery", "bounded_stop", "session_restart", "batch_shims", "windows_profile_matching",
                       "managed_tcp_listener", "managed_tree_tcp_listener", "powershell_live_context", "reset"]})
    finally:
        try:
            command("daemon", "stop", check=False)
        finally:
            if daemon is not None and daemon.poll() is None:
                daemon.kill()
                daemon.wait(timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--bin-dir", type=Path)
    parser.add_argument("--powershell", type=Path, default=shutil.which("pwsh"))
    args = parser.parse_args()
    if sys.platform != "win32":
        parser.error("this smoke test requires native Windows")
    if args.bin_dir and not args.skip_build:
        parser.error("--bin-dir requires --skip-build")
    if args.powershell is None or not Path(args.powershell).is_file():
        parser.error("PowerShell 7 executable is required")
    binaries = (args.bin_dir or ROOT / "target/debug").resolve()
    evidence = ROOT / "target" / f"windows-native-smoke-{uuid.uuid4().hex}"
    try:
        with handle_signals():
            if not args.skip_build:
                run(["cargo", "build", "--locked", "--target-dir", ROOT / "target", "-p", "undercurrent-cli", "-p",
                     "undercurrent-daemon", "--bins"], cwd=ROOT, timeout=None, stdout=None, stderr=None)
            for name in ("uc.exe", "uc-daemon.exe", "uc-session-host.exe"):
                if not (binaries / name).is_file():
                    parser.error(f"build the executable first: {binaries / name}")
            # which() may preserve PATHEXT's uppercase .EXE spelling. Resolve
            # the actual name before exercising protected Store installations.
            exercise(binaries, Path(args.powershell).resolve(strict=True), evidence)
    except subprocess.CalledProcessError as error:
        print(f"Failed command: {subprocess.list2cmdline(error.cmd)}", file=sys.stderr)
        print(f"Evidence: {evidence}", file=sys.stderr)
        print((error.stderr or error.stdout or b"").decode(errors="replace"), file=sys.stderr)
        return exit_code(error.returncode)
    print(f"Windows native smoke passed. Evidence: {evidence}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
