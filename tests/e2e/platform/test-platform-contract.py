#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Shared native CLI behavior and explicit known-limit contracts; no skips.

Usage: python3 tests/e2e/platform/test-platform-contract.py target/release
All daemon state and shell configuration are confined to a temporary home.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time


def main():
    if sys.platform not in ("linux", "darwin"):
        raise SystemExit("platform contracts require Linux or macOS")
    project = Path(__file__).resolve().parents[3]
    binaries = Path(sys.argv[1] if len(sys.argv) > 1 else project / "target/release").resolve()
    linux = sys.platform == "linux"
    temporary_parent = "/private/tmp" if not linux else str(Path(tempfile.gettempdir()).resolve())
    with tempfile.TemporaryDirectory(prefix="uc-contract-", dir=temporary_parent) as temporary:
        root = Path(temporary)
        home = root / "home"
        home.mkdir()
        runtime = root / "run"
        runtime.mkdir(mode=0o700)
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(("UNDERCURRENT_", "UC_", "XDG_", "ZDOTDIR"))}
        env.update(HOME=str(home), UNDERCURRENT_HOME=str(root / "product"),
                   XDG_RUNTIME_DIR=str(runtime), ZDOTDIR=str(home),
                   PATH=str(binaries) + os.pathsep + env["PATH"])

        def uc(*args, check=True):
            result = subprocess.run([str(binaries / "uc"), "--timeout", "10s", *args],
                                    env=env, cwd=root, capture_output=True, text=True, timeout=25)
            if check and result.returncode:
                raise AssertionError(f"uc {args}: {result.returncode}: {result.stdout}{result.stderr}")
            return result

        def wait_output(name, marker):
            deadline = time.monotonic() + 12
            while time.monotonic() < deadline:
                if marker in uc("peek", name).stdout:
                    return
                time.sleep(0.1)
            raise AssertionError(f"{name}: output marker missing: {marker}: {uc('inspect', name).stdout}: {uc('peek', name).stdout}")

        def rejected(result, diagnostic):
            assert result.returncode != 0, "unsupported operation unexpectedly succeeded; update capability contract"
            assert diagnostic in result.stdout + result.stderr, (result.stdout, result.stderr)

        try:
            assert "daemon started" in uc("daemon", "start").stdout
            assert "accepting connections: true" in uc("daemon", "status").stdout
            # A running stdin consumer proves PTY input/output without shell hooks.
            uc("start", "core", "--cwd", str(root), "--", "/bin/sh", "-c",
               'printf CORE_READY; while read line; do printf "REPLY:%s\\n" "$line"; done')
            wait_output("core", "CORE_READY")
            uc("send", "core", "hello-platform")
            wait_output("core", "REPLY:hello-platform")
            assert "core" in uc("ls").stdout
            print("PASS supported: daemon, native launch, list, PTY input/output", flush=True)

            # A real raw-mode PTY client must receive replies without an attached UI.
            query_client = r"""
import os, select, time, tty
tty.setraw(0)
os.write(1, b'\x1b[4;7H\x1b[')
os.write(1, b'0c\x1b[5n\x1b[6n')
expected = b'\x1b[?1;2c\x1b[0n\x1b[4;7R'
received = b''
deadline = time.monotonic() + 5
while len(received) < len(expected):
    remaining = deadline - time.monotonic()
    assert remaining > 0 and select.select([0], [], [], remaining)[0], 'terminal query timed out'
    received += os.read(0, 1024)
assert received == expected, repr(received)
os.write(1, b'\r\nTERMINAL_QUERIES_OK\r\n')
"""
            uc("start", "terminal-queries", "--", "/usr/bin/python3", "-c", query_client)
            wait_output("terminal-queries", "TERMINAL_QUERIES_OK")
            print("PASS supported: terminal device, status and cursor query replies", flush=True)

            # Both flags bypass missing live context, testing the supervisor itself.
            result = uc("exec", "core", "--dir-launch", "--env-launch", "--",
                        "/bin/sh", "-c", "printf EXEC_OK; exit 7", check=False)
            assert result.returncode == 7 and result.stdout == "EXEC_OK", result
            print("PASS supported: supervised exec output and exit status", flush=True)

            result = uc("start", "sealed", "--launch-mode", "sealed", "--",
                        "/bin/echo", "SEALED_OK", check=False)
            if linux:
                assert result.returncode == 0, result.stderr
                wait_output("sealed", "SEALED_OK")
                print("PASS supported: sealed native executable", flush=True)
            else:
                rejected(result, "prepare_launch failed with unsupported")
                print("KNOWN LIMIT verified: sealed launch rejected", flush=True)

            uc("start", "listener", "--", sys.executable, "-c",
               "import socket,time; s=socket.socket(); s.bind(('127.0.0.1',0)); "
               "s.listen(); print('TCP_READY',flush=True); time.sleep(120)")
            wait_output("listener", "TCP_READY")
            deadline = time.monotonic() + 12
            while True:
                context = json.loads(uc("--json", "inspect", "listener").stdout)["summary"]["profile_context"]
                if context["server_role_origin"] == "tcp_listener":
                    break
                assert time.monotonic() < deadline, context
                time.sleep(0.1)
            uc("stop", "listener")
            print("PASS supported: real TCP listener ownership without a server profile", flush=True)

            restart_failure = None
            uc("restart", "core")
            uc("send", "core", "after-restart")
            try:
                wait_output("core", "REPLY:after-restart")
            except AssertionError as error:
                # Finish independent capability checks, but keep this a hard failure.
                restart_failure = error
            assert "stopped core" in uc("stop", "core").stdout
            assert "daemon stopped" in uc("daemon", "stop").stdout
            assert "daemon started" in uc("daemon", "start").stdout
            assert "core" in uc("ls").stdout
            uc("daemon", "stop")
            print("PASS supported: stop and daemon restart with persisted state", flush=True)

            # Use real populated state: an empty reset may succeed without quarantine.
            state = root / "product/state/undercurrent.db"
            assert state.is_file(), "reset fixture must have real persisted state"
            result = uc("reset", "--yes", check=False)
            assert result.returncode == 0, result.stderr
            assert not state.exists(), "reset retained the state database"
            print("PASS supported: populated state reset", flush=True)
            if restart_failure is not None:
                raise restart_failure
            print("PASS supported: fresh PTY output after session restart", flush=True)
        finally:
            uc("daemon", "stop", check=False)
    print("Platform contracts passed; KNOWN LIMIT checks do not imply feature support.")


if __name__ == "__main__":
    main()
