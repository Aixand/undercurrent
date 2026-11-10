# SPDX-License-Identifier: Apache-2.0
"""Bounded POSIX terminal fixture shared by interactive CLI regressions."""

import fcntl
import os
from pathlib import Path
import pty
import select
import signal
import struct
import subprocess
import termios
import time


def binary_directory():
    return Path(os.environ.get("UC_E2E_BIN_DIR", Path(__file__).resolve().parents[3] / "target/debug"))


class Terminal:
    def __init__(self, env, directory, *args, command="attach", binary_dir=None):
        self.master, self.slave = pty.openpty()
        self.original = termios.tcgetattr(self.slave)
        self.output = bytearray()
        self.resize(24, 100)
        self.process = subprocess.Popen(
            [str((binary_dir or binary_directory()) / "uc"), command, *args], env=env, cwd=directory,
            stdin=self.slave, stdout=self.slave, stderr=self.slave, start_new_session=True,
        )

    def resize(self, rows, columns):
        fcntl.ioctl(self.slave, termios.TIOCSWINSZ, struct.pack("HHHH", rows, columns, 0, 0))
        if hasattr(self, "process"):
            os.kill(self.process.pid, signal.SIGWINCH)

    def pump(self):
        if select.select([self.master], [], [], .05)[0]:
            self.output.extend(os.read(self.master, 65536))
        assert len(self.output) < 4 * 1024 * 1024, "unbounded picker output"

    def expect(self, marker, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.pump()
            if marker in self.output:
                return
            assert self.process.poll() is None, (self.process.returncode, bytes(self.output[-2000:]))
        raise AssertionError((marker, bytes(self.output[-3000:])))

    def send(self, data):
        self.output.clear()
        os.write(self.master, data)

    def exited(self, code, timeout=10):
        deadline = time.monotonic() + timeout
        while self.process.poll() is None and time.monotonic() < deadline:
            self.pump()
        assert self.process.poll() == code, (self.process.poll(), bytes(self.output[-2000:]))
        self.pump()
        assert b"\x1b[?1049l" in self.output, "caller screen was not restored"
        assert termios.tcgetattr(self.slave) == self.original, "terminal mode was not restored"

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        os.close(self.master)
        os.close(self.slave)
