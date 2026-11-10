#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Workspace lifecycle and mutation flows through a real, isolated POSIX PTY."""

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
    with tempfile.TemporaryDirectory(prefix="uc-tui-") as directory:
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(("UC_", "UNDERCURRENT_")) and k not in ("ENV", "BASH_ENV")}
        env.update(UNDERCURRENT_HOME=str(Path(directory) / "uc"),
                   SHELL="/bin/sh", TERM="xterm-256color")
        terminals = []

        def cli(*args, check=True):
            result = subprocess.run([str(BIN / "uc"), *args], env=env, cwd=directory,
                                    capture_output=True, text=True, timeout=30)
            if check:
                assert result.returncode == 0, (args, result.stderr)
            return result

        def tui(command="tui"):
            terminal = Terminal(env, directory, command=command)
            terminals.append(terminal)
            terminal.expect(b"UC Workspace")
            return terminal

        def inspect(name):
            return json.loads(cli("--json", "inspect", name).stdout)

        def wait_state(name, state):
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if inspect(name)["summary"]["lifecycle"]["state"] == state:
                    return
                time.sleep(.1)
            raise AssertionError((name, state, inspect(name)))

        try:
            for command in ("tui", "workspace"):
                for args in [(command,), ("--json", command), ("--jsonl", command)]:
                    rejected = cli(*args, check=False)
                    assert rejected.returncode == 2, (args, rejected.stderr)
                    assert "\x1b[?1049h" not in rejected.stdout
            assert not (Path(directory) / "uc/run/control.sock").exists()

            terminal = tui()
            terminal.expect(b"No matching sessions")
            terminal.send(b"\x03")
            terminal.exited(130)

            program = (
                "import sys\n"
                "print('READY:' + sys.argv[1], flush=True)\n"
                "for line in sys.stdin: print('ACK:' + line.strip(), flush=True)\n"
            )
            for name in ("alpha", "beta"):
                cli("start", name, "--columns", "91", "--rows", "31",
                    "--", sys.executable, "-u", "-c", program, name)

            terminal = tui("workspace")
            terminal.expect(b"beta")
            original_geometry = inspect("alpha")["configuration"]["terminal"]
            terminal.send(b"2")
            terminal.expect(b"READY:alpha")
            assert inspect("alpha")["configuration"]["terminal"] == original_geometry
            terminal.send(b"3")
            terminal.expect(b"Recent output")
            assert inspect("alpha")["configuration"]["terminal"] == original_geometry

            terminal.send(b"1/alpha\r")
            terminal.expect(b"Search: alpha")
            for _ in range(2):
                terminal.send(b"\r")
                terminal.expect(b": workspace")
                terminal.send(b"hjklq\r")
                terminal.expect(b"ACK:hjklq")
                terminal.send(b"\x1bud")
                terminal.expect(b"UC Workspace")
                terminal.expect(b"Search: alpha")
                assert inspect("alpha")["summary"]["lifecycle"]["state"] == "running"

            terminal.send(b"/\x15\r")
            terminal.expect(b"beta")
            terminal.send(b" j g")
            terminal.expect(b"Group selected sessions")
            terminal.send(b"\r")
            terminal.expect(b"Create group from selection")
            terminal.send(b"demo\r")
            terminal.expect(b"Group: 2/2 succeeded")
            group = json.loads(cli("group", "inspect", "demo", "--json").stdout)
            assert {m["name"] for m in group["members"]} == {"alpha", "beta"}, group

            terminal.send(b"lteam=core task=ui\t\t\r")
            terminal.expect(b"Labels: 2/2 succeeded")
            sessions = json.loads(cli("ls", "--projection", "with-labels", "--json").stdout)["items"]
            assert all(s["labels"]["team"] == "core" and s["labels"]["task"] == "ui" for s in sessions)

            # New shell, chosen group and labels, then attach directly.
            terminal.send(b"un")
            terminal.expect(b"New session")
            terminal.send(b"from-tui\t\t\t\t\tdemo\trole=test\t\t\t\t\t\r")
            terminal.expect(b": workspace")
            terminal.send(b"printf 'TUI_SHELL_READY\\n'\r")
            terminal.expect(b"TUI_SHELL_READY")
            terminal.send(b"\x1bud")
            terminal.expect(b"UC Workspace")
            assert inspect("from-tui")["summary"]["lifecycle"]["state"] == "running"
            group = json.loads(cli("group", "inspect", "demo", "--json").stdout)
            assert {m["name"] for m in group["members"]} == {"alpha", "beta", "from-tui"}, group

            terminal.resize(10, 30)
            terminal.expect(b"Terminal too small")
            terminal.output.clear()
            terminal.resize(28, 120)
            terminal.expect(b"UC Workspace")
            terminal.send(b"q")
            terminal.exited(0)
            assert inspect("alpha")["summary"]["lifecycle"]["state"] == "running"
            assert inspect("from-tui")["summary"]["lifecycle"]["state"] == "running"

            terminal = tui()
            terminal.expect(b"alpha")
            terminal.send(b"/alpha\r\r")
            terminal.expect(b": workspace")
            cli("stop", "alpha")
            terminal.expect(b"UC Workspace")
            terminal.send(b"q")
            terminal.exited(0)

            terminal = tui()
            terminal.expect(b"beta")
            terminal.send(b"/beta\ra\r")
            terminal.expect(b"Stop gracefully 1 session(s)?")
            terminal.send(b"\r")  # Default is cancel.
            assert inspect("beta")["summary"]["lifecycle"]["state"] == "running"
            terminal.send(b"a\r\t\r")
            terminal.expect(b"Stop gracefully: 1/1 succeeded")
            wait_state("beta", "stopped")
            terminal.send(b"n")
            terminal.expect(b"New session")
            # Command mode passes the quoted argument vector without a shell.
            command_arguments = '-u -c "import time; print(\'COMMAND_READY\', flush=True); time.sleep(60)"'
            terminal.send(("command-tui\t \t" + sys.executable + "\t" + command_arguments
                           + "\t\t\t\t\t\t\t\r").encode())
            terminal.expect(b"Started command-tui.")
            deadline = time.monotonic() + 5
            while "COMMAND_READY" not in cli("logs", "command-tui").stdout:
                assert time.monotonic() < deadline, "command output was not captured"
                time.sleep(.1)
            assert inspect("command-tui")["summary"]["lifecycle"]["state"] == "running"
            os.kill(terminal.process.pid, signal.SIGTERM)
            terminal.exited(130)
        finally:
            for terminal in terminals:
                terminal.close()
            cli("stop", "--all", check=False)
            cli("daemon", "stop", check=False)
    print("workspace PTY flows passed")


if __name__ == "__main__":
    main()
