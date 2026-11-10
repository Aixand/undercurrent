#!/usr/bin/env python3
"""Verify configured finite-input boundaries over the real CLI and Python bridge.

Run after building uc, uc-daemon, uc-session-host and uc-python-bridge. Each
configuration uses a temporary home and a raw PTY reader; no user daemon is used.
"""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time


def wait_for(predicate, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.01)
    raise TimeoutError("input-limit receiver did not reach the expected state")


def receiver(root):
    import tty
    tty.setraw(0)
    (root / "ready").touch()
    sequence = 0
    while True:
        sequence += 1
        control = root / f"control-{sequence}"
        wait_for(control.exists, 120)
        expected = int(control.read_text())
        digest = hashlib.sha256()
        count = 0
        (root / f"armed-{sequence}").touch()
        while count < expected:
            data = os.read(0, min(65536, expected - count))
            if not data:
                raise EOFError("terminal closed before complete input")
            count += len(data)
            digest.update(data)
        temporary = root / "receipt.tmp"
        temporary.write_text(json.dumps({"bytes": count, "sha256": digest.hexdigest()}))
        temporary.replace(root / f"receipt-{sequence}")


class Installation:
    def __init__(self, artifacts, config, rows):
        self.artifacts, self.config, self.rows = artifacts, config, rows
        self.temp = tempfile.TemporaryDirectory(prefix="uc-input-limits-")
        self.root = Path(self.temp.name)
        self.home = self.root / "home"
        self.home.mkdir(mode=0o700)
        (self.home / "config.toml").write_text(config)
        self.env = {k: v for k, v in os.environ.items()
                    if not k.startswith(("UNDERCURRENT_", "UC_"))}
        self.env.update(UNDERCURRENT_HOME=str(self.home), TERM="xterm-256color")
        self.sequence = 0

    def cli(self, *args, input=None, check=True):
        result = subprocess.run([str(self.artifacts / "uc"), "--timeout", "30s", *args],
                                env=self.env, cwd=self.root, input=input,
                                capture_output=True, timeout=40)
        if check and result.returncode:
            raise AssertionError(result.stderr.decode())
        return result

    def __enter__(self):
        try:
            self.cli("config", "validate")
            self.cli("daemon", "start")
            self.cli("start", "reader", "--no-detect", "--", sys.executable,
                     str(Path(__file__).resolve()), "--receiver", str(self.root))
            wait_for((self.root / "ready").exists)
            self.session = json.loads(self.cli("--json", "ls").stdout)["items"][0]["id"]
            return self
        except BaseException:
            self.__exit__()
            raise

    def __exit__(self, *_):
        try:
            self.cli("daemon", "stop", check=False)
        finally:
            # A failed handshake can leave our daemon running even though the
            # CLI cannot stop it. Restrict fallback cleanup to this exact home.
            if sys.platform == "linux":
                marker = f"UNDERCURRENT_HOME={self.home}".encode()
                for entry in Path("/proc").iterdir():
                    if not entry.name.isdecimal():
                        continue
                    try:
                        if marker not in (entry / "environ").read_bytes().split(b"\0"):
                            continue
                        descriptor = os.pidfd_open(int(entry.name))
                        try:
                            if marker in (entry / "environ").read_bytes().split(b"\0"):
                                signal.pidfd_send_signal(descriptor, signal.SIGKILL)
                        finally:
                            os.close(descriptor)
                    except (ProcessLookupError, FileNotFoundError, PermissionError):
                        pass
            self.temp.cleanup()

    def arm(self, expected):
        self.sequence += 1
        (self.root / f"control-{self.sequence}").write_text(str(len(expected)))
        wait_for((self.root / f"armed-{self.sequence}").exists)

    def verify(self, expected, label):
        receipt = self.root / f"receipt-{self.sequence}"
        wait_for(receipt.exists)
        assert json.loads(receipt.read_text()) == {
            "bytes": len(expected), "sha256": hashlib.sha256(expected).hexdigest()
        }, label
        self.rows.append({"case": label, "verified_bytes": len(expected), "status": "passed"})

    def send_cli(self, text, *, paste=False, submit=False, accepted=True, label):
        flags = ["--paste" if paste else "--raw"]
        if not submit:
            flags.append("--no-newline")
        expected = text.encode()
        if paste:
            expected = b"\x1b[200~" + expected + b"\x1b[201~"
        if submit:
            expected += b"\r"
        if accepted:
            self.arm(expected)
        result = self.cli("send", "reader", *flags, input=text.encode(), check=False)
        assert (result.returncode == 0) == accepted, (label, result.stderr.decode())
        if accepted:
            self.verify(expected, label)
        else:
            self.rows.append({"case": label, "status": "rejected"})
            # A rejected operation must leave no prefix in the PTY.
            self.send_cli("sentinel", label=label + "-zero-effects")

    async def send_python(self, text, *, request_bytes=None, accepted=True, label):
        from undercurrent import Client
        from undercurrent.errors import UndercurrentError
        options = {} if request_bytes is None else {"request_bytes": request_bytes}
        async with Client(home=self.home, bridge=self.artifacts / "uc-python-bridge", **options) as client:
            if accepted:
                self.arm(text.encode())
            try:
                await client.send_input(self.session, text, paste=False, submit=False)
            except (UndercurrentError, ValueError):
                if accepted:
                    raise
            else:
                assert accepted, label
            if accepted:
                self.verify(text.encode(), label)
            else:
                self.rows.append({"case": label, "status": "rejected"})
                self.send_cli("sentinel", label=label + "-zero-effects")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", type=Path, default=Path("target/debug"))
    parser.add_argument("--report", type=Path, default=Path("target/input-limits-e2e.json"))
    parser.add_argument("--receiver", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.receiver:
        receiver(args.receiver)
        return
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "python/src"))
    artifacts = args.artifact_dir.resolve()
    rows = []
    mib = 1024 * 1024
    with Installation(artifacts, "schema_version = \"0.1.0-beta\"\n", rows) as rig:
        for delta in (-1, 0, 1):
            rig.send_cli("x" * (5 * mib + delta), accepted=delta <= 0,
                         label=f"default-raw-{delta:+d}")
        rig.send_cli("x" * (5 * mib - 28), paste=True, submit=True, label="paste-enter-at-limit")
        rig.send_cli("x" * (5 * mib - 27), paste=True, submit=True, accepted=False,
                     label="paste-enter-over-limit")
        for text, label in [("\0" * (5 * mib), "escaped"), ("界" * (5 * mib // 3), "unicode")]:
            rig.send_cli(text, label="cli-" + label)
            asyncio.run(rig.send_python(text, label="python-" + label))
        asyncio.run(rig.send_python("x" * (5 * mib + 1), accepted=False, label="python-over-limit"))
    with Installation(artifacts, "[services]\nmax_input_bytes = 1024\n[requests]\nrequest_bytes = 8192\n", rows) as rig:
        rig.send_cli("x" * 1024, label="lower-config-at-limit")
        rig.send_cli("x" * 1025, accepted=False, label="lower-config-over-limit")
        asyncio.run(rig.send_python("\0" * 1024, request_bytes=8192, label="python-lower-request-option"))
        asyncio.run(rig.send_python("x" * 1024, request_bytes=512, accepted=False, label="python-request-codec-rejection"))
    config = """[services]
max_input_bytes = 10485760
[process]
input_queue_bytes_per_process = 16777216
[requests]
request_bytes = 67107026
[transport]
max_frame_bytes = 67108864
connection_queue_bytes = 268435456
stream_queue_bytes = 67108864
replay_item_bytes = 67108864
replay_bytes_per_stream = 67108864
replay_bytes_per_connection = 134217728
"""
    with Installation(artifacts, config, rows) as rig:
        text = "\0" * (6 * mib)
        rig.send_cli(text, label="raised-config-cli-36mib-json")
        asyncio.run(rig.send_python(text, request_bytes=64 * mib - 1838,
                                   label="raised-config-python-36mib-json"))
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps({"status": "passed", "cases": rows}, indent=2) + "\n")
    print(f"Passed {len(rows)} checks; report: {args.report}")


if __name__ == "__main__":
    main()
