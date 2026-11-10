#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Exercise attach and stop pickers through a POSIX PTY with isolated daemon state."""

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "_support"))
from terminal import Terminal, binary_directory

BIN = binary_directory()



def main():
    with tempfile.TemporaryDirectory(prefix="uc-picker-") as directory:
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(("UC_", "UNDERCURRENT_")) and k not in ("ENV", "BASH_ENV")}
        env.update(UNDERCURRENT_HOME=str(Path(directory) / "uc"),
                   SHELL="/bin/sh", TERM="xterm-256color")
        terminals = []

        def cli(*args, check=True):
            result = subprocess.run([str(BIN / "uc"), *args], env=env, cwd=directory,
                                    capture_output=True, text=True, timeout=25)
            if check:
                assert result.returncode == 0, (args, result.stderr)
            return result

        def attach(*args):
            terminal = Terminal(env, directory, *args)
            terminals.append(terminal)
            return terminal

        def stop(*args):
            terminal = Terminal(env, directory, *args, command="stop")
            terminals.append(terminal)
            return terminal

        def inspect(name):
            return json.loads(cli("--json", "inspect", name).stdout)["summary"]

        try:
            rejected = cli("attach", check=False)
            assert rejected.returncode != 0
            assert "\x1b[?1049h" not in rejected.stdout

            for args in [("stop",), ("stop", "--force"), ("stop", "--graceful"),
                         ("--json", "stop"), ("--jsonl", "stop")]:
                rejected = cli(*args, check=False)
                assert rejected.returncode == 2, rejected.stderr
                assert "specify a session name" in rejected.stderr
                assert "\x1b[?1049h" not in rejected.stdout

            for picker in [attach, stop]:
                for action in ["escape", "q", "ctrl-c", "signal"]:
                    terminal = picker()
                    terminal.expect(b"No sessions.")
                    if action == "signal":
                        os.kill(terminal.process.pid, signal.SIGTERM)
                    else:
                        terminal.send({"escape": b"\x1b", "q": b"q", "ctrl-c": b"\x03"}[action])
                    terminal.exited(0 if action in ("escape", "q") else 130)

            terminal = attach("--read-only", "--detach-keys", "ctrl-b,d")
            terminal.expect(b"No sessions.")
            program = (
                "import sys\n"
                "print('READY:' + sys.argv[1], flush=True)\n"
                "for line in sys.stdin: print('ACK:' + line.strip(), flush=True)\n"
            )
            for name in ["picker-a", "picker-b"]:
                cli("start", name, "--", sys.executable, "-u", "-c", program, name)
            terminal.send(b"r")
            terminal.expect(b"picker-b")
            terminal.resize(18, 90)
            terminal.send(b"jkj\r")
            terminal.expect(b"READY:picker-b")
            terminal.expect(b"read-only")
            geometry = json.loads(cli("--json", "inspect", "picker-b").stdout)["configuration"]["terminal"]
            assert geometry == {"columns": 90, "rows": 17}, geometry
            terminal.send(b"SHOULD_NOT_REACH_SESSION\r\x02d")
            terminal.exited(0)
            assert "SHOULD_NOT_REACH_SESSION" not in cli("logs", "picker-b").stdout

            terminal = attach("--detach-keys", "ctrl-b,d")
            terminal.expect(b"picker-a")
            terminal.send(b"\r")
            terminal.expect(b"READY:picker-a")
            terminal.send(b"hjklq\r")
            terminal.expect(b"ACK:hjklq")
            terminal.send(b"\x02d")
            terminal.exited(0)
            sessions = json.loads(cli("--json", "ls").stdout)["items"]
            assert len(sessions) == 2
            assert all(s["lifecycle"]["state"] == "running" for s in sessions)

            for action in ["escape", "q", "ctrl-c", "signal"]:
                terminal = stop()
                terminal.expect(b"picker-a")
                if action == "signal":
                    os.kill(terminal.process.pid, signal.SIGTERM)
                else:
                    terminal.send({"escape": b"\x1b", "q": b"q", "ctrl-c": b"\x03"}[action])
                terminal.exited(0 if action in ("escape", "q") else 130)
                assert inspect("picker-a")["lifecycle"]["state"] == "running"
                assert inspect("picker-b")["lifecycle"]["state"] == "running"

            terminal = stop("--force")
            terminal.expect(b"Enter: force stop")
            # Observe navigation before resizing: the first frame can arrive
            # before the native event reader has registered its resize signal.
            terminal.send(b"\x1b[B")
            terminal.expect(b"> picker-b")
            terminal.output.clear()
            terminal.resize(8, 60)
            terminal.expect(b"> picker-b")
            terminal.send(b"\r")
            terminal.exited(0)
            assert inspect("picker-b")["lifecycle"]["state"] == "stopped"
            assert inspect("picker-a")["lifecycle"]["state"] == "running"
            assert terminal.output.index(b"\x1b[?1049l") < terminal.output.index(b"stopped picker-b")

            terminal = stop()
            terminal.expect(b"Enter: force stop")
            terminal.send(b"\r")
            terminal.exited(0)
            assert inspect("picker-a")["lifecycle"]["state"] == "stopped"

            cli("restart", "picker-a")
            terminal = stop("--graceful")
            terminal.expect(b"Enter: stop")
            terminal.send(b"\r")
            terminal.exited(0)
            assert inspect("picker-a")["lifecycle"]["state"] in ("stopped", "exited")

            # A removed target's reused name must never redirect the selected stop.
            terminal = stop()
            terminal.expect(b"picker-a")
            cli("clean", "picker-a", "--yes")
            cli("start", "picker-a", "--", sys.executable, "-u", "-c", program, "picker-a")
            terminal.send(b"\r")
            terminal.exited(1)
            assert inspect("picker-a")["lifecycle"]["state"] == "running"

            terminal = stop()
            terminal.expect(b"picker-b")
            cli("start", "picker-c", "--restart", "always", "--restart-delay", "10m",
                "--max-restart-delay", "10m", "--", sys.executable, "-c", "raise SystemExit(1)")
            deadline = time.monotonic() + 15
            while inspect("picker-c")["restart_status"]["state"] != "scheduled":
                assert time.monotonic() < deadline, "restart was not scheduled"
                time.sleep(.05)
            terminal.send(b"r")
            terminal.expect(b"restart scheduled")
            terminal.send(b"jj\r")
            terminal.exited(0)
            assert inspect("picker-c")["restart_status"] == {"state": "not_scheduled", "reason": "explicit_stop"}
            print("PASS: attach/stop selection, cancellation, signals, refresh, resize, immediate/default and graceful stop, stable IDs, pending restarts and terminal restoration")
        finally:
            for terminal in terminals:
                terminal.close()
            cli("stop", "--all", "--force", check=False)
            cli("daemon", "stop", check=False)


if __name__ == "__main__":
    main()
