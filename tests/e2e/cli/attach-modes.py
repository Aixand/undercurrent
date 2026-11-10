#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Exercise attach copy/paste and tiling through a real POSIX PTY and isolated daemon."""

import fcntl
import json
import os
from pathlib import Path
import pty
import select
import signal
import struct
import subprocess
import sys
import tempfile
import termios
import time


BIN = Path(os.environ.get("UC_E2E_BIN_DIR", Path(__file__).resolve().parents[3] / "target/debug"))


def main():
    with tempfile.TemporaryDirectory(prefix="uc-attach-modes-") as directory:
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(("UC_", "UNDERCURRENT_")) and k not in ("ENV", "BASH_ENV")}
        env.update(UNDERCURRENT_HOME=str(Path(directory) / "uc"),
                   SHELL="/bin/sh", TERM="xterm-256color")
        process = None
        master = None
        output = bytearray()

        def cli(*args, check=True):
            result = subprocess.run([str(BIN / "uc"), *args], env=env, cwd=directory,
                                    capture_output=True, text=True, timeout=25)
            if check:
                assert result.returncode == 0, (args, result.stderr)
            return result.stdout

        def pump():
            if select.select([master], [], [], .05)[0]:
                try:
                    output.extend(os.read(master, 65536))
                except OSError:
                    pass
            assert len(output) < 4 * 1024 * 1024, "unbounded attach output"

        def until(predicate):
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                pump()
                if predicate():
                    return
                assert process.poll() is None, (process.returncode, bytes(output[-2000:]))
            raise AssertionError(bytes(output[-3000:]))

        def expect(marker):
            until(lambda: marker in output)

        def send(data):
            output.clear()
            os.write(master, data)

        def geometry(name):
            return json.loads(cli("--json", "inspect", name))["configuration"]["terminal"]

        try:
            program = (
                "import sys\n"
                "print('\\x1b[2J\\x1b[HCOPYME\\r\\nREADY', flush=True)\n"
                "for line in sys.stdin: print('ACK:' + line.strip(), flush=True)\n"
            )
            for name in ["modes-a", "modes-b"]:
                cli("start", name, "--", sys.executable, "-u", "-c", program)
            master, slave = pty.openpty()
            fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 100, 0, 0))
            try:
                process = subprocess.Popen([str(BIN / "uc"), "attach", "modes-a"],
                                           env=env, cwd=directory, stdin=slave,
                                           stdout=slave, stderr=slave, start_new_session=True)
            finally:
                os.close(slave)
            expect(b"READY")
            send(b"\x1bu[")
            expect(b"COPY")
            send(b"g0vllllly")
            expect(b"\x1b]52;c;Q09QWU1F\x07")
            send(b"\x1b")
            expect(b"Alt+U")
            send(b"\x1bu%")
            expect(b"modes-b")
            send(b"q")
            expect(b"READY")
            send(b"\x1bu%")
            expect(b"modes-b")
            # The picker defaults to name-ascending order.
            send(b"jkj\r")
            until(lambda: geometry("modes-b") == {"columns": 50, "rows": 23})
            send(b"\x1bu]\r")
            expect(b"ACK:COPYME")
            send(b"\x1buoFIRST\r")
            expect(b"ACK:FIRST")
            send(b"\x1buz")
            until(lambda: geometry("modes-a") == {"columns": 100, "rows": 23})
            send(b"\x1buz")
            until(lambda: geometry("modes-a") == {"columns": 49, "rows": 23})
            send(b'\x1bu"')
            expect(b"Split")
            send(b"c")
            until(lambda: len(json.loads(cli("--json", "ls"))["items"]) == 3)
            until(lambda: b"pane-" in output)
            send(b"\x1bux")
            until(lambda: geometry("modes-b") == {"columns": 50, "rows": 23})
            fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack("HHHH", 30, 120, 0, 0))
            os.kill(process.pid, signal.SIGWINCH)
            until(lambda: geometry("modes-b") == {"columns": 60, "rows": 29})
            send(b"\x1bud")
            until(lambda: process.poll() is not None)
            assert process.returncode == 0
            sessions = json.loads(cli("--json", "ls"))["items"]
            assert len(sessions) == 3
            assert all(s["lifecycle"]["state"] == "running" for s in sessions)
            print("PASS: visual copy, OSC 52, cross-pane paste, focus, both splits, new shell, zoom, resize, close and detach")
        finally:
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            if master is not None:
                os.close(master)
            cli("stop", "--all", "--force", check=False)
            cli("daemon", "stop", check=False)


if __name__ == "__main__":
    main()
