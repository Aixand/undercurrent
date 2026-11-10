# /// script
# requires-python = ">=3.13"
# dependencies = ["pywinpty==3.0.5"]
# ///
# SPDX-License-Identifier: Apache-2.0
"""Exercise the real Windows attach UI through an outer ConPTY terminal."""
import argparse
import json
import os
from pathlib import Path
import queue
import subprocess
import threading
import time
import uuid

from winpty import PtyProcess
from winpty.enums import Backend

ROOT = Path(__file__).resolve().parents[3]
MAX_OUTPUT = 1024 * 1024


class Terminal:
    def __init__(self, command, env):
        self.process = PtyProcess.spawn(command, env=env, backend=Backend.ConPTY)
        self.output = ""
        self.messages = queue.Queue(maxsize=256)
        self.failure = None
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        size = 0
        try:
            while True:
                data = self.process.read()
                size += len(data)
                if size > MAX_OUTPUT:
                    raise AssertionError("attach output exceeded its bound")
                self.messages.put(data, timeout=5)
        except EOFError:
            pass
        except Exception as error:
            self.failure = error

    def expect(self, marker):
        deadline = time.monotonic() + 15
        while marker not in self.output:
            if self.failure:
                raise self.failure
            try:
                self.output += self.messages.get(timeout=.1)
            except queue.Empty:
                pass
            if time.monotonic() > deadline:
                raise AssertionError(f"missing {marker!r}: {self.output[-4000:]!r}")

    def wait_exit(self):
        deadline = time.monotonic() + 10
        while self.process.isalive():
            if time.monotonic() > deadline:
                raise AssertionError("attach did not exit before deadline")
            time.sleep(.05)
        assert self.process.exitstatus == 0, f"attach exited with {self.process.exitstatus}"

    def detach(self):
        self.process.write("\x02d")
        self.wait_exit()

    def close(self):
        self.process.close(force=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", type=Path, default=ROOT / "target/debug")
    args = parser.parse_args()
    assert os.name == "nt", "native Windows is required"
    uc = args.bin_dir.resolve() / "uc.exe"
    assert uc.is_file(), f"build the native CLI first: {uc}"
    evidence = ROOT / "target" / ("windows-attach-smoke-" + uuid.uuid4().hex)
    evidence.mkdir(parents=True)
    env = {k: v for k, v in os.environ.items() if not k.startswith(("UC_", "UNDERCURRENT_"))}
    env["UNDERCURRENT_HOME"] = str(evidence / "home")
    terminals = []

    def cli(*arguments):
        result = subprocess.run([str(uc), *arguments], env=env, capture_output=True,
                                text=True, timeout=20)
        assert result.returncode == 0, result.stderr
        return result.stdout

    def inspect():
        return json.loads(cli("--json", "inspect", "attach-smoke"))

    def attach(*options):
        terminal = Terminal([str(uc), "attach", "attach-smoke", "--detach-keys", "ctrl-b,d", *options], env)
        terminals.append(terminal)
        terminal.expect("ATTACH_READY")
        return terminal

    try:
        powershell = Path(os.environ["SystemRoot"]) / "System32/WindowsPowerShell/v1.0/powershell.exe"
        cli("start", "attach-smoke", "--stop-timeout", "100ms", "--", str(powershell),
            "-NoProfile", "-Command", 'Write-Output ATTACH_READY; while($true) { '
            '$s=[Console]::ReadLine(); if($null -eq $s -or $s -eq "quit"){break}; '
            'Write-Output ("ACK:"+$s) }')
        terminal = attach()
        terminal.process.write("native-input\r")
        terminal.expect("ACK:native-input")
        terminal.process.setwinsize(35, 110)
        deadline = time.monotonic() + 10
        while inspect()["configuration"]["terminal"] != {"columns": 110, "rows": 34}:
            assert time.monotonic() < deadline, "outer resize did not reach the managed terminal"
            time.sleep(.05)
        terminal.detach()
        assert inspect()["summary"]["lifecycle"]["state"] == "running"

        readonly = attach("--read-only")
        readonly.process.write("blocked-input\r")
        time.sleep(.25)
        cli("send", "attach-smoke", "readonly-check")
        readonly.expect("ACK:readonly-check")
        assert "ACK:blocked-input" not in readonly.output, "read-only attach forwarded input"
        readonly.detach()
        assert inspect()["summary"]["lifecycle"]["state"] == "running"

        final = attach()
        final.expect("ACK:readonly-check")
        final.process.write("quit\r")
        final.wait_exit()
        assert inspect()["summary"]["lifecycle"] == {"state": "exited", "code": 0}
        report = {"platform": "windows", "status": "passed", "binaries": str(uc.parent),
                  "checks": ["conpty-attach", "interactive-input", "resize", "detach-keeps-session",
                             "read-only", "reattach", "process-exit-ends-attach"]}
    finally:
        try:
            for index, terminal in enumerate(terminals):
                terminal.close()
                (evidence / f"terminal-{index}.txt").write_text(terminal.output, encoding="utf-8")
        finally:
            cli("daemon", "stop")
    (evidence / "result.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Windows attach smoke passed. Evidence: {evidence}", flush=True)


if __name__ == "__main__":
    main()
