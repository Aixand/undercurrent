#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Verify live session recovery after killing only an isolated daemon (Linux)."""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time


class CheckFailure(RuntimeError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise CheckFailure(message)


def digest(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def process_stat(pid: int) -> tuple[int, int, str]:
    text = Path(f"/proc/{pid}/stat").read_text()
    fields = text[text.rfind(")") + 2:].split()
    return int(fields[1]), int(fields[19]), fields[0]


class OwnedProcesses:
    """Pin only this script's descendants; pidfds prevent PID-reuse signals."""

    def __init__(self) -> None:
        self.pins: dict[tuple[int, int], int] = {}

    def capture(self) -> None:
        rows = {}
        for path in Path("/proc").iterdir():
            if path.name.isdigit():
                try:
                    rows[int(path.name)] = process_stat(int(path.name))
                except (OSError, ValueError, IndexError):
                    continue
        descendants = {os.getpid()}
        while True:
            discovered = {pid for pid, row in rows.items() if row[0] in descendants}
            extra = discovered - descendants
            if not extra:
                break
            descendants.update(extra)
        for pid in descendants - {os.getpid()}:
            _, ticks, state = rows[pid]
            key = (pid, ticks)
            if key in self.pins or state == "Z":
                continue
            try:
                fd = os.pidfd_open(pid)
            except OSError:
                continue
            try:
                current = process_stat(pid)
            except (OSError, ValueError, IndexError):
                os.close(fd)
                continue
            if current[1] != ticks:
                os.close(fd)
                continue
            self.pins[key] = fd

    @staticmethod
    def alive(key: tuple[int, int]) -> bool:
        try:
            _, ticks, state = process_stat(key[0])
            return ticks == key[1] and state != "Z"
        except (OSError, ValueError, IndexError):
            return False

    def send(self, key: tuple[int, int], sig: signal.Signals) -> None:
        require(key in self.pins, "refusing to signal an unowned process")
        try:
            signal.pidfd_send_signal(self.pins[key], sig)
        except ProcessLookupError:
            pass

    def live(self) -> list[tuple[int, int]]:
        return [key for key in self.pins if self.alive(key)]

    def reap_and_close(self) -> None:
        for (pid, _), fd in self.pins.items():
            try:
                os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                pass
            os.close(fd)


def wait_for(label: str, probe, seconds: float = 10):
    deadline = time.monotonic() + seconds
    last_error = None
    while time.monotonic() < deadline:
        try:
            value = probe()
            if value:
                return value
        except (CheckFailure, OSError, ValueError) as error:
            last_error = error
        time.sleep(0.05)
    raise CheckFailure(f"timed out waiting for {label}: {last_error or 'condition unmet'}")


class RecoveryCheck:
    def __init__(self, artifacts: Path) -> None:
        self.artifacts = artifacts.resolve(strict=True)
        self.root = Path(tempfile.mkdtemp(prefix="ucr-", dir="/tmp"))
        self.home, self.state = self.root / "h", self.root / "s"
        for path in (self.home, self.state, self.root / "t"):
            path.mkdir(mode=0o700)
        self.config = self.state / "config.toml"
        self.config.write_text(
            'schema_version = "0.1.0-beta"\n[local]\n'
        )
        self.environment = {
            "HOME": str(self.home), "PATH": f"{self.artifacts}:/usr/bin:/bin",
            "TMPDIR": str(self.root / "t"), "LANG": "C.UTF-8", "TERM": "xterm-256color",
            "UNDERCURRENT_HOME": str(self.state),
        }
        self.owned = OwnedProcesses()
        self.daemons: list[subprocess.Popen] = []
        self.log = (self.root / "daemon.log").open("wb")
        self.name = "recovery"
        self.report: dict = {"schema_version": "0.1.0-beta", "result": "failed"}

    def cli(self, *arguments: str, timeout: int = 3) -> dict:
        completed = subprocess.run(
            [str(self.artifacts / "uc"), "--json", "--timeout", f"{timeout}s", *arguments],
            cwd=self.root, env=self.environment, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout + 2,
        )
        require(completed.returncode == 0,
                f"uc {' '.join(arguments[:2])} exited {completed.returncode}: "
                + completed.stderr.decode(errors="replace")[:500])
        return json.loads(completed.stdout)

    def start_daemon(self) -> tuple[subprocess.Popen, dict]:
        daemon = subprocess.Popen(
            [str(self.artifacts / "uc-daemon")],
            cwd=self.root, env=self.environment, stdin=subprocess.DEVNULL,
            stdout=self.log, stderr=self.log, start_new_session=True,
        )
        self.daemons.append(daemon)
        self.owned.capture()

        def ready():
            require(daemon.poll() is None, "owned daemon exited before readiness")
            status = self.cli("daemon", "status", timeout=1)
            return status if status.get("accepting_connections") else None

        status = wait_for("daemon readiness", ready)
        self.owned.capture()
        return daemon, status

    def detail(self) -> dict:
        return self.cli("inspect", self.name, "--process")

    def running(self) -> dict | None:
        detail = self.detail()
        return detail if (detail["summary"]["lifecycle"]["state"] == "running"
                          and detail.get("process")) else None

    def echo(self, marker: str, number: int) -> None:
        self.cli("send", self.name, marker)

        offset = 0
        retained = bytearray()

        def output():
            nonlocal offset
            page = self.cli("logs", self.name, "--from-offset", str(offset), "--max-bytes", "4096")
            if page.get("result") == "gap":
                require(page["recovery"] == "restart_from_available_offset",
                        "unexpected retained-output recovery instruction")
                require(page["available_from"] > offset, "output gap did not advance")
                self.report.setdefault("output_retention_gaps", []).append({
                    "requested_offset": offset, "available_from": page["available_from"],
                })
                offset = page["available_from"]
                retained.clear()
                return False
            require(page.get("result") == "data", "unexpected output page")
            raw = bytes(page["bytes"])
            require(hashlib.sha256(raw).hexdigest() == page["payload_digest_sha256"],
                    "terminal output digest mismatch")
            offset = page["next_offset"]
            retained.extend(raw)
            del retained[:-8192]
            # cat -n adds a line number; terminal input echo alone cannot satisfy this.
            return f"{number}\t{marker}".encode() in retained

        wait_for(f"cat output for line {number}", output)

    def run(self) -> None:
        versions = []
        artifacts = {}
        for name in ("uc", "uc-daemon", "uc-session-host"):
            path = self.artifacts / name
            require(path.is_file() and os.access(path, os.X_OK), f"missing executable {name}")
            output = subprocess.check_output(
                [str(path), "--version"], env=self.environment, cwd=self.root, timeout=5,
            ).decode().strip()
            versions.append(output.split()[-1])
            artifacts[name] = digest(path)
        require(len(set(versions)) == 1, "artifact versions do not match")
        self.report.update({"version": versions[0], "artifacts_sha256": artifacts})
        first, before_status = self.start_daemon()
        self.cli("start", self.name, "--cwd", str(self.root), "--no-detect",
                 "--stop-timeout", "1s", "--", "/usr/bin/cat", "-n")
        before = wait_for("running cat", self.running)
        self.echo("UC_RECOVERY_BEFORE", 1)
        self.owned.capture()
        first_key = next(key for key in self.owned.pins if key[0] == first.pid)
        host_keys = []
        cat_keys = []
        for key in self.owned.live():
            executable = Path(f"/proc/{key[0]}/exe")
            if executable.resolve() == (self.artifacts / "uc-session-host").resolve():
                host_keys.append(key)
            elif key != first_key and digest(executable) == digest(Path("/usr/bin/cat")):
                cat_keys.append(key)
        require(len(host_keys) == len(cat_keys) == 1, "expected one owned host and one cat")
        token = (self.state / "run/session-host.auth").read_bytes()
        require(not (self.state / "websocket.token").exists(), "retired token was created")
        self.owned.send(first_key, signal.SIGKILL)
        require(first.wait(timeout=5) == -signal.SIGKILL, "daemon did not exit via SIGKILL")
        require(all(self.owned.alive(key) for key in host_keys + cat_keys),
                "live host or cat exited with the daemon")
        second, after_status = self.start_daemon()
        after = wait_for("recovered live cat", self.running)
        require(after["summary"]["id"] == before["summary"]["id"], "session identity changed")
        require(after["process"] == before["process"], "recovery replaced the hosted process")
        require(after["summary"]["restart_count"] == before["summary"]["restart_count"],
                "recovery restarted the workload")
        require(all(self.owned.alive(key) for key in host_keys + cat_keys),
                "recovery lost the original native process identities")
        require((self.state / "run/session-host.auth").read_bytes() == token,
                "daemon replacement changed the private host credential")
        require(after_status["server_epoch"] != before_status["server_epoch"],
                "daemon replacement reused its server epoch")
        self.echo("UC_RECOVERY_AFTER", 2)
        self.cli("stop", self.name)
        wait_for("stopped session", lambda: self.detail()["summary"]["lifecycle"]["state"] == "stopped")
        self.cli("daemon", "stop", "--graceful", "--drain-timeout", "2s", timeout=5)
        require(second.wait(timeout=5) == 0, "graceful daemon shutdown failed")
        wait_for("owned processes to exit", lambda: not self.owned.live(), seconds=5)
        self.report.update({
            "result": "passed", "recovery": "same_live_session_and_process",
            "session_id": before["summary"]["id"],
            "process_instance_id": before["process"]["instance_id"],
            "native_process_identity": {"pid": cat_keys[0][0], "start_ticks": cat_keys[0][1]},
            "session_host_identity": {"pid": host_keys[0][0], "start_ticks": host_keys[0][1]},
            "server_epoch_changed": True, "private_host_credential_unchanged": True,
            "cat_numbered_lines_verified": [1, 2], "graceful_shutdown": True,
        })

    def cleanup(self) -> None:
        self.owned.capture()
        forced = bool(self.owned.live())
        for sig in (signal.SIGTERM, signal.SIGKILL):
            for key in self.owned.live():
                self.owned.send(key, sig)
            deadline = time.monotonic() + 2
            while self.owned.live() and time.monotonic() < deadline:
                time.sleep(0.05)
            self.owned.capture()
        for daemon in self.daemons:
            if daemon.poll() is not None:
                daemon.wait()
        live = self.owned.live()
        self.owned.reap_and_close()
        self.log.close()
        self.report["cleanup"] = {"owned_processes_quiescent": not live, "fallback_signals_used": forced}
        if live:
            raise CheckFailure(f"owned processes remain; private artifacts retained at {self.root}")
        shutil.rmtree(self.root)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    arguments = parser.parse_args()
    require(sys.platform == "linux", "this qualification supports Linux only")
    require(hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal"),
            "Python pidfd support is required for exact process cleanup")
    # Adopt only this invocation's orphaned host, so it can be reaped after shutdown.
    libc = ctypes.CDLL(None, use_errno=True)
    require(libc.prctl(36, 1, 0, 0, 0) == 0, "could not become a child subreaper")
    check = RecoveryCheck(arguments.artifact_dir)

    def interrupted(signum, _frame):
        raise CheckFailure(f"interrupted by signal {signum}")

    for sig in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, interrupted)
    try:
        check.run()
    except (Exception, KeyboardInterrupt) as error:
        check.report.update({"result": "failed", "error": str(error)})
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            signal.signal(sig, signal.SIG_IGN)
        try:
            check.cleanup()
        except Exception as error:
            check.report.update({"result": "failed", "cleanup_error": str(error)})
    encoded = json.dumps(check.report, indent=2) + "\n"
    if arguments.report:
        arguments.report.parent.mkdir(parents=True, exist_ok=True)
        arguments.report.write_text(encoded)
    print(encoded, end="")
    return 0 if check.report["result"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
