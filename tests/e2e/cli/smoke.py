#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Exercise CLI commands, binary I/O and shutdown against an isolated Linux daemon."""

import argparse
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "_support"))
from _support.processes import (assert_gone, exit_code, handle_signals, identities,
                                isolated_environment, run, single_identity, terminate_identity)


def row(output, name, state):
    assert re.search(rf"(?m)^{re.escape(name)}\s+{re.escape(state)}(?:\s|$)", output), output


def attach_probe(binary_dir, environment, temporary):
    # Reuse the same bounded terminal fixture as the interactive feature tests.
    from terminal import Terminal
    terminal = Terminal(environment, temporary, "binary", binary_dir=binary_dir)
    transcript = bytearray()
    deadline = time.monotonic() + 5
    try:
        terminal.expect(b"attached", timeout=max(0, deadline - time.monotonic()))
        transcript.extend(terminal.output)
        terminal.send(b"x")
        terminal.expect(b"attach-ready", timeout=max(0, deadline - time.monotonic()))
        transcript.extend(terminal.output)
        terminal.send(b"\x02d")
        terminal.exited(0, timeout=max(0, deadline - time.monotonic()))
    finally:
        transcript.extend(terminal.output)
        (temporary / "attach.typescript").write_bytes(transcript)
        terminal.close()


def exercise(binary_dir, temporary):
    uc = binary_dir / "uc"
    home = temporary / "state/undercurrent"
    for folder in (temporary / "home", temporary / "state", temporary / "run", home):
        folder.mkdir(mode=0o700, parents=True, exist_ok=True)
    environment = isolated_environment(home) | {
        "HOME": str(temporary / "home"), "XDG_STATE_HOME": str(temporary / "state"),
        "XDG_RUNTIME_DIR": str(temporary / "run"), "TERM": "xterm-256color",
    }
    path = home / "config.toml"
    path.write_text('schema_version = "0.1.0-beta"\n[local]\n[shortcuts]\nprefix = "ctrl-b"\n')
    path.chmod(0o600)
    stage = "bootstrap"
    daemon_started = False

    def mark(name):
        nonlocal stage
        stage = name
        print(f"[cli-smoke] stage={stage}", flush=True)

    def command(*args, check=True, timeout=20):
        return run([uc, *args], env=environment, cwd=ROOT, timeout=timeout, check=check)

    def text(*args):
        return command(*args).stdout.decode().strip()

    try:
        mark("deadline-wrapper-self-test")
        assert text("config", "validate").startswith("configuration is valid")
        probe = run([sys.executable, "-", "deadline-python-argv"], timeout=2,
                    input=b"import sys\nassert sys.argv == ['-', 'deadline-python-argv']\nprint(sys.argv[1])\n")
        assert probe.stdout.strip() == b"deadline-python-argv"

        mark("preflight")
        assert '"placement": "local"' in text("--json", "config", "show")
        assert "events" in json.loads(text("--json", "events"))
        assert "_uc()" in text("completion", "bash")
        assert "uc" in text("init", "bash", "--print")

        mark("local-auto-bootstrap")
        assert not (home / "run/control.sock").exists(), "preflight bootstrapped a daemon"
        daemon_started = True
        command("ls", "--page-size", "1")
        assert (home / "run/control.sock").is_socket(), "connected command did not bootstrap daemon"
        for removed in (("--socket", str(temporary / "removed.sock"), "ls"),
                        ("--no-auto-daemon", "ls"), ("gateway", "status")):
            assert command(*removed, check=False).returncode != 0, f"removed command accepted: {removed}"
        assert not list(temporary.rglob("connection.token")), "retired transport token created"

        mark("daemon-start")
        start = text("daemon", "start")
        epoch = re.search(r"(?m)^epoch:\s+(\S+)", start)
        assert epoch, start
        status = text("daemon", "status")
        status_epoch = re.search(r"(?m)^epoch:\s+(\S+)", status)
        assert status_epoch and status_epoch[1] == epoch[1], status

        mark("profile-reads")
        row(text("profile", "list"), "codex", "enabled")
        assert "profile: codex" in text("profile", "inspect", "codex")
        assert "selected: codex" in text("profile", "match", "--executable", "codex")
        assert "valid: true" in text("profile", "lint")
        assert "profile: codex" in text("profile", "test", "codex", "--screen-text", "> ")

        mark("session-start")
        command("start", "worker", "--", "sh", "-c", "printf ready; cat")
        command("start", "binary", "--", "sh", "-c",
                r'stty raw -echo; dd bs=1 count=1 2>/dev/null; printf "\000\377attach-ready"; sleep 120')

        mark("finite-operations")
        for args in (("ls", "--page-size", "10"), ("inspect", "worker", "--all"),
                     ("explain", "worker"), ("wait", "worker", "--lifecycle", "running")):
            row(text(*args), "worker", "running")
        assert "ready" in text("peek", "worker")
        assert text("exec", "worker", "--dir-launch", "--env-launch", "--", "sh", "-c", "printf exec-ready") == "exec-ready"
        assert command("exec", "worker", "--dir-launch", "--env-launch", "--", "sh", "-c",
                       r'printf "\001\377exec-ready"').stdout == b"\x01\xffexec-ready"
        command("send", "worker", "smoke-input")
        assert "smoke-input" in text("logs", "worker", "--max-bytes", "1024")

        mark("logs-follow-retained-backlog")
        expected = b"A" * 20000 + b"BACKLOG_END\r\n"
        command("start", "log-backlog", "--", sys.executable, "-u", "-c",
                "import sys,time; sys.stdout.write('A'*20000+'BACKLOG_END\\n'); sys.stdout.flush(); time.sleep(120)")
        deadline = time.monotonic() + 5
        while True:
            page = json.loads(command("logs", "log-backlog", "--max-bytes", "4096", "--json").stdout)
            if page["next_offset"] >= len(expected):
                break
            assert time.monotonic() < deadline, "initial backlog did not arrive"
            time.sleep(.05)
        try:
            command("logs", "log-backlog", "--max-bytes", "4096", "--follow", "--jsonl", timeout=2)
        except subprocess.TimeoutExpired as error:
            frames = [json.loads(line) for line in error.stdout.splitlines()]
        else:
            raise AssertionError("log following must remain active")
        retained = bytearray()
        for frame in frames:
            assert frame["record"] == "terminal_output", frame
            assert frame["start_offset"] == len(retained), frame
            retained.extend(frame["bytes"])
            assert frame["end_offset"] == len(retained), frame
        assert retained == expected, (len(retained), len(expected))
        command("stop", "log-backlog", "--force")
        command("clean", "log-backlog", "--yes")

        mark("exec-live-directory")
        launch, live = temporary / "launch", temporary / "live"
        launch.mkdir()
        live.mkdir()
        command("start", "exec-directory", "-i", "--shell", "/bin/bash", "--cwd", str(launch))
        command("send", "exec-directory", f"cd {shlex.quote(str(live))} && sleep 120")
        deadline = time.monotonic() + 10
        while f"working directory: {live}" not in text("inspect", "exec-directory"):
            assert time.monotonic() < deadline, "live working directory did not update"
            time.sleep(.1)
        assert text("exec", "exec-directory", "--env-launch", "--", "pwd") == str(live)
        assert text("exec", "exec-directory", "--dir-launch", "--env-launch", "--", "pwd") == str(launch)
        command("stop", "exec-directory", "--force")
        command("clean", "exec-directory", "--yes")

        mark("pty-binary-round-trip")
        attach_probe(binary_dir, environment, temporary)
        assert b"\x00\xffattach-ready" in command("logs", "binary", "--max-bytes", "1024").stdout

        mark("bounded-streams")
        for args in (("watch", "worker"), ("top", "--page-size", "10")):
            try:
                command(*args, timeout=2)
            except subprocess.TimeoutExpired:
                pass  # These streams run until the caller cancels them.

        mark("diagnostics")
        for section in ("connections", "streams", "slow-consumers", "output-producers", "supervisors",
                        "detection", "persistence", "reconciliation", "configuration"):
            expected = {"supervisors": "supervisor_queues", "detection": "detection_evidence"}.get(section, section.replace("-", "_"))
            assert f'"section": "{expected}"' in text("--json", "debug", section)

        mark("diagnostic-bundle")
        bundle = json.loads(text("--json", "debug", "bundle"))
        assert bundle["terminal_content_redacted"] is True and bundle.get("terminal_content") is None
        explicit = text("--json", "debug", "bundle", "--include-terminal-content", "worker", "--max-bytes", "1024")
        assert json.loads(explicit)["terminal_content_redacted"] is False
        assert '"session_id": "session-' in explicit and '"bytes": [' in explicit
        assert "ready" in text("logs", "worker", "--max-bytes", "1024")

        mark("session-lifecycle")
        assert text("restart", "worker").startswith("restarted worker")
        for session in ("worker", "binary"):
            command("stop", session, "--force")
            assert "selected 1, removed 1, failed 0" in text("clean", session, "--yes")

        mark("daemon-stop")
        host = single_identity(binary_dir / "uc-session-host", socket=home / "run/session-host.sock")
        command("daemon", "stop", "--graceful", "--drain-timeout", "5s")
        daemon_started = False
        mark("cleanup-proof")
        assert_gone(host, "session-host")
        for name in ("run/session-host.sock", "run/session-host.auth", "state/session-host/session-host.execution-jobs",
                     "state/session-host/session-host.attempt-receipts"):
            path = home / name
            assert not path.exists() and not path.is_symlink(), f"private artifact remains: {path}"

        mark("stopped-state-and-reset")
        assert command("daemon", "status", check=False).returncode != 0
        assert "reset" in text("reset", "--yes")
    except BaseException:
        print(f"[cli-smoke] failed during {stage}", file=sys.stderr)
        raise
    finally:
        if daemon_started:
            try:
                command("daemon", "stop", "--graceful", "--drain-timeout", "2s", check=False, timeout=5)
            except (OSError, subprocess.TimeoutExpired):
                pass
        for identity in identities(home=home):
            terminate_identity(identity)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", type=Path, default=Path(os.environ.get("UC_E2E_BIN_DIR",
        Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target")) / "release")))
    args = parser.parse_args()
    if sys.platform != "linux":
        parser.error("this smoke test requires Linux process and socket inspection")
    binaries = args.bin_dir.resolve()
    for name in ("uc", "uc-daemon", "uc-session-host"):
        if not os.access(binaries / name, os.X_OK):
            parser.error(f"build the executable first: {binaries / name}")
    try:
        with handle_signals(), tempfile.TemporaryDirectory(prefix="uc-smoke-", dir="/tmp") as directory:
            exercise(binaries, Path(directory))
    except subprocess.CalledProcessError as error:
        print((error.stderr or error.stdout or b"").decode(errors="replace"), file=sys.stderr)
        return exit_code(error.returncode)
    except subprocess.TimeoutExpired as error:
        print(f"CLI smoke command timed out: {error.cmd}", file=sys.stderr)
        return 124
    print("CLI process smoke test passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
