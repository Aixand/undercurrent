#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Exercise forced reset, response loss, and launch exclusion in isolated Linux homes."""

import argparse
import json
import os
from pathlib import Path
import selectors
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time


def wait_until(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition did not settle"
        time.sleep(.01)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", type=Path, default=Path(__file__).resolve().parents[3] / "target/debug")
    args = parser.parse_args()
    if sys.platform != "linux":
        raise SystemExit("This process-identity test requires Linux /proc")
    uc = str(args.artifact_dir.resolve() / "uc")
    results = {}
    with tempfile.TemporaryDirectory(prefix="uc-reset-e2e-") as temporary:
        root = Path(temporary)
        home = root / "home"
        env = {k: v for k, v in os.environ.items() if not k.startswith(("UC_", "UNDERCURRENT_"))}
        env["UNDERCURRENT_HOME"] = str(home)
        clients = []

        def cli(*arguments, check=True):
            result = subprocess.run([uc, *arguments], env=env, capture_output=True, text=True, timeout=12)
            if check:
                assert result.returncode == 0, (arguments, result.stderr)
            return result

        def launch(*arguments):
            child = subprocess.Popen([uc, *arguments], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            clients.append(child)
            return child

        def watch():
            child = launch("watch", "--jsonl")
            with selectors.DefaultSelector() as selector:
                selector.register(child.stdout, selectors.EVENT_READ)
                assert selector.select(5), "watch did not activate"
            assert json.loads(child.stdout.readline())["record"] == "session_snapshot"
            return child

        def reset(label):
            start = time.monotonic()
            cli("reset", "--yes")
            elapsed = time.monotonic() - start
            assert elapsed < 4, (label, elapsed)
            results[label] = round(elapsed, 3)
            assert not (home / "state/undercurrent.db").exists()

        def finish(child):
            if child.poll() is None:
                child.terminate()
            child.communicate(timeout=5)

        def owned_processes():
            expected = ("UNDERCURRENT_HOME=" + str(home)).encode()
            for entry in Path("/proc").iterdir():
                if not entry.name.isdigit():
                    continue
                try:
                    # Pin identity before reading metadata; cleanup never targets
                    # a recycled PID or another Undercurrent home.
                    handle = os.pidfd_open(int(entry.name))
                except (ProcessLookupError, PermissionError):
                    continue
                try:
                    if expected in (entry / "environ").read_bytes().split(b"\0"):
                        yield handle
                except (FileNotFoundError, ProcessLookupError, PermissionError):
                    pass
                finally:
                    os.close(handle)

        try:
            cli("daemon", "start")
            observer = watch()
            reset("idle_watcher_seconds")
            assert cli("daemon", "status", check=False).returncode != 0
            finish(observer)

            marker = root / "process.json"
            term = root / "term"
            program = """import json,os,signal,subprocess,sys,time
signal.signal(signal.SIGTERM, lambda *_: open(sys.argv[2], 'w').close())
child = subprocess.Popen([sys.executable, '-c', 'import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(300)'])
with open(sys.argv[1], 'w') as out: json.dump([os.getpid(),child.pid],out)
time.sleep(300)
"""
            cli("start", "worker", "--no-detect", "--stop-timeout", "20s", "--", sys.executable, "-u", "-c", program, str(marker), str(term))
            wait_until(marker.exists)
            identities = []
            for pid in json.loads(marker.read_text()):
                fields = Path(f"/proc/{pid}/stat").read_text().split()
                identities.append((pid, fields[21]))
            observer = watch()
            restart = launch("restart", "worker", "--graceful")
            wait_until(term.exists)
            assert restart.poll() is None, "restart must still be waiting for graceful stop"
            reset("inflight_restart_and_process_tree_seconds")
            finish(restart)
            finish(observer)
            def gone(identity):
                pid, start = identity
                try:
                    fields = Path(f"/proc/{pid}/stat").read_text().split()
                    return fields[21] != start or fields[2] == "Z"
                except FileNotFoundError:
                    return True
            wait_until(lambda: all(gone(identity) for identity in identities))

            # Hold the attempt store to make the reset/start overlap deterministic.
            cli("daemon", "start")
            database = home / "state/undercurrent.db"
            with sqlite3.connect(database) as db:
                db.execute("BEGIN IMMEDIATE")
                resetting = launch("reset", "--yes")
                time.sleep(.15)
                starting = launch("daemon", "start")
                time.sleep(.15)
                assert resetting.poll() is None
                assert starting.poll() is None, "explicit start bypassed reset's startup guard"
                db.rollback()
            output, error = resetting.communicate(timeout=8)
            assert resetting.returncode == 0, (output, error)
            output, error = starting.communicate(timeout=8)
            assert starting.returncode == 0, (output, error)
            assert json.loads(cli("ls", "--json").stdout)["items"] == []
            results["concurrent_start"] = "passed"
            reset("after_concurrent_start_seconds")

            cli("daemon", "start")
            with sqlite3.connect(database) as db:
                db.execute("BEGIN IMMEDIATE")
                disconnected = launch("reset", "--yes")
                time.sleep(.2)
                assert disconnected.poll() is None
                disconnected.kill()
                disconnected.communicate(timeout=5)
                db.rollback()
            wait_until(lambda: cli("daemon", "status", check=False).returncode != 0)
            # The dead caller cannot delete files; an offline reset can finish.
            assert database.exists()
            reset("after_requester_disconnect_seconds")
            results["requester_disconnect"] = "passed"

            cli("daemon", "start")
            with sqlite3.connect(database) as db:
                db.execute("BEGIN IMMEDIATE")
                result = cli("--timeout", "100ms", "reset", "--yes", check=False)
                assert result.returncode != 0, "a timed-out reset must not claim success"
                assert database.exists(), "timeout must preserve the database"
                db.rollback()
            wait_until(lambda: cli("daemon", "status", check=False).returncode != 0)
            reset("after_timeout_seconds")
            results["timeout_preserves_state"] = "passed"

            cli("daemon", "start")
            observer = watch()
            start = time.monotonic()
            cli("daemon", "stop", "--graceful", "--drain-timeout", "1s")
            elapsed = time.monotonic() - start
            assert .9 <= elapsed < 4, elapsed
            results["graceful_stop_seconds"] = round(elapsed, 3)
            finish(observer)
            reset("offline_seconds")
        finally:
            for child in clients:
                if child.poll() is None:
                    child.kill()
                child.communicate(timeout=5)
            try:
                cli("daemon", "stop", "--graceful", "--drain-timeout", "1s", check=False)
            finally:
                for handle in owned_processes():
                    try:
                        signal.pidfd_send_signal(handle, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
