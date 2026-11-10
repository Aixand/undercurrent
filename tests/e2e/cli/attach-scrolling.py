#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Verify late-attach input modes, retained history, and frozen mouse copying."""

import base64
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "_support"))
from terminal import Terminal, binary_directory

BIN = binary_directory()
WHEEL_UP = b"\x1b[<64;5;5M"


def main():
    with tempfile.TemporaryDirectory(prefix="uc-scroll-") as directory:
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(("UC_", "UNDERCURRENT_"))
               and k not in ("ENV", "BASH_ENV", "TMUX")}
        env.update(UNDERCURRENT_HOME=str(Path(directory) / "home"),
                   TERM="xterm-256color", SHELL="/bin/sh")
        terminals = []

        def cli(*args, check=True):
            result = subprocess.run([str(BIN / "uc"), *args], env=env, cwd=directory,
                                    capture_output=True, text=True, timeout=20)
            if check:
                assert result.returncode == 0, (args, result.stderr)
            return result.stdout

        def wait_until(predicate):
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                # Keep consuming terminal output while waiting for child input.
                # Darwin's smaller PTY buffer can otherwise block rendering.
                for active in terminals:
                    if active.process.poll() is None:
                        active.pump()
                if predicate():
                    return
                time.sleep(.02)
            raise AssertionError("fixture did not reach the expected state")

        def settle(terminal):
            deadline = time.monotonic() + .25
            while time.monotonic() < deadline:
                terminal.pump()

        def attach(*args):
            terminal = Terminal(env, directory, *args)
            terminals.append(terminal)
            return terminal

        try:
            normal = (
                "import os,tty\n"
                "tty.setraw(0)\n"
                "for row in range(80): os.write(1, f'ROW{row:03d}\\r\\n'.encode())\n"
                "os.write(1,b'READY')\n"
                "while os.read(0,4096): pass\n"
            )
            cli("start", "normal", "--columns", "100", "--rows", "23", "--", sys.executable, "-u", "-c", normal)
            wait_until(lambda: "READY" in cli("peek", "normal"))
            terminal = attach()
            terminal.expect(b"normal")
            terminal.send(b"\r")
            terminal.expect(b"READY")
            terminal.send(WHEEL_UP * 80)
            terminal.expect(b"ROW000")
            # Dragging ordinary output enters copy mode without a prefix.
            for row, text in [(1, b"ROW000"), (2, b"ROW001")]:
                terminal.send(f"\x1b[<0;1;{row}M\x1b[<32;6;{row}M\x1b[<0;6;{row}m".encode())
                terminal.expect(b"\x1b]52;c;" + base64.b64encode(text) + b"\x07")
                terminal.expect(b"copied")
            terminal.send(b"\x1b[<65;5;5M")
            terminal.expect(b"ROW003")
            terminal.send(b"\x1b")
            terminal.expect(b"READY")
            terminal.send(b"\x1bud")
            terminal.exited(0)
            terminal = attach("normal")
            terminal.expect(b"READY")
            terminal.send(WHEEL_UP * 80)
            terminal.expect(b"ROW000")
            terminal.send(b"\x1bud")
            terminal.exited(0)

            received = Path(directory) / "input"
            received.touch()
            application = (
                "import os,sys,tty\n"
                "tty.setraw(0)\n"
                "os.write(1,b'\\x1b[?1049h\\x1b[?1003h\\x1b[?1006h\\x1b[?2004h\\x1b[?1hAPPREADY')\n"
                "with open(sys.argv[1],'ab',buffering=0) as output:\n"
                " while True: output.write(os.read(0,4096))\n"
            )
            cli("start", "application", "--columns", "100", "--rows", "23", "--", sys.executable, "-u", "-c", application, str(received))
            wait_until(lambda: "APPREADY" in cli("peek", "application"))
            terminal = attach()
            terminal.expect(b"application")
            terminal.send(b"\r")
            terminal.expect(b"APPREADY")
            paste = b"\x1b[200~test paste\x1b[201~"
            terminal.send(WHEEL_UP + paste + b"\x1b[A")
            expected = WHEEL_UP + paste + b"\x1bOA"
            wait_until(lambda: received.read_bytes() == expected)
            terminal.send(b"\x1bu[")
            terminal.expect(b"COPY")
            terminal.send(WHEEL_UP * 30)
            terminal.expect(b"COPY")
            settle(terminal)
            assert received.read_bytes() == expected
            terminal.send(b"\x1b")
            terminal.expect(b"Alt+U")
            terminal.send(WHEEL_UP)
            wait_until(lambda: received.read_bytes() == expected + WHEEL_UP)
            terminal.send(b"\x1bud")
            terminal.exited(0)
            assert len(json.loads(cli("--json", "ls"))["items"]) == 2
            print("PASS: picker and direct attach, retained shell history, repeated mouse copies, "
                  "copy scrolling, live return, child mouse/paste/cursor modes and terminal restoration")
        finally:
            for terminal in terminals:
                terminal.close()
            cli("stop", "--all", "--force", check=False)
            cli("daemon", "stop", check=False)


if __name__ == "__main__":
    main()
