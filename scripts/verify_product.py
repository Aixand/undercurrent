# SPDX-License-Identifier: Apache-2.0
"""Exercise installed binaries and SDK against an explicitly disposable daemon home."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import sysconfig
import tempfile
import time
import uuid

import undercurrent

# Loaded explicitly because installed verification runs Python in isolated mode.
import importlib.util
_mcp_spec = importlib.util.spec_from_file_location(
    "undercurrent_mcp_probe", Path(__file__).with_name("mcp_probe.py")
)
_mcp_probe = importlib.util.module_from_spec(_mcp_spec)
_mcp_spec.loader.exec_module(_mcp_probe)

MARKER = "undercurrent-installer-verification-only\n"


def exercise(payload, home, action):
    marker = home / ".installer-verification"
    if (
        home.exists()
        and any(home.iterdir())
        and (not marker.is_file() or marker.read_text() != MARKER)
    ):
        raise RuntimeError(
            "Verification accepts only a new home or its own previous disposable state"
        )
    # Let the native daemon create its own private home. Python's mkdir mode
    # does not set Windows ownership/DACLs (notably for elevated SSH tokens).
    home.parent.mkdir(parents=True, exist_ok=True)
    suffix = ".exe" if os.name == "nt" else ""
    uc = payload / "bin" / ("uc" + suffix)
    daemon_binary = payload / "bin" / ("uc-daemon" + suffix)
    bridge = payload / "bin" / ("uc-python-bridge" + suffix)
    site = Path(sysconfig.get_path("purelib")).resolve()
    assert Path(undercurrent.__file__).resolve().is_relative_to(site)
    environment = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("UNDERCURRENT_", "UC_"))
    }
    environment["UNDERCURRENT_HOME"] = str(home)

    def cli(*args, check=True):
        return subprocess.run(
            [str(uc), *args],
            env=environment,
            cwd=home.parent,
            check=check,
            capture_output=True,
            text=True,
            timeout=20,
        )

    for name in ("uc", "uc-daemon", "uc-session-host", "uc-mcp"):
        result = subprocess.run(
            [payload / "bin" / (name + suffix), "--version"],
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        )
        assert result.stdout.startswith(name + " ")
    log_path = home.parent / (home.name + ".verification-daemon.log")
    with log_path.open("ab") as log:
        daemon = subprocess.Popen(
            [str(daemon_binary)], env=environment, cwd=home.parent, stdout=log, stderr=log
        )
        try:
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if daemon.poll() is not None:
                    raise RuntimeError(
                        f"Disposable daemon exited: {log_path.read_text(errors='replace')[-4000:]}"
                    )
                status = cli("--json", "daemon", "status", check=False)
                if (
                    status.returncode == 0
                    and json.loads(status.stdout)["accepting_connections"]
                ):
                    break
                time.sleep(0.05)
            else:
                raise RuntimeError("Installed daemon did not become ready")

            marker.write_text(MARKER)
            marker.chmod(0o600)

            async def sdk_probe():
                async with undercurrent.Client(
                    home=home, bridge=bridge, request_timeout=10
                ) as client:
                    if action == "full":
                        session = await client.start(
                            [
                                sys.executable,
                                "-I",
                                "-B",
                                "-u",
                                "-c",
                                "import sys,time; print('uc-installer-ready',flush=True); sys.stdin.readline(); print('uc-installer-input-ok',flush=True); time.sleep(120)",
                            ],
                            name="installer-" + uuid.uuid4().hex[:10],
                            cwd=home,
                            env={"PYTHONDONTWRITEBYTECODE": "1"},
                            detection=False,
                            labels={"purpose": "installer-verification"},
                        )
                        try:
                            deadline = time.monotonic() + 15
                            while (
                                b"uc-installer-ready"
                                not in (await client.read_output(session)).data
                            ):
                                if time.monotonic() > deadline:
                                    raise RuntimeError(
                                        "Managed session did not produce PTY output"
                                    )
                                await asyncio.sleep(0.05)
                            await client.send_input(session, "installer-input")
                            deadline = time.monotonic() + 15
                            while (
                                b"uc-installer-input-ok"
                                not in (await client.read_output(session)).data
                            ):
                                if time.monotonic() > deadline:
                                    raise RuntimeError(
                                        "Managed session did not receive input"
                                    )
                                await asyncio.sleep(0.05)
                        finally:
                            await client.stop(session, force=True)
                    return sorted(row.id for row in await client.list())

            sessions = asyncio.run(sdk_probe())
            if action == "full":
                mcp_result = _mcp_probe.exercise(payload / "bin" / ("uc-mcp" + suffix), home)
                sessions = sorted(set(sessions) | set(mcp_result["sessions"]))
            else:
                with _mcp_probe.Client(payload / "bin" / ("uc-mcp" + suffix), home, read_only=True) as mcp:
                    assert mcp.call("get_status")["connected"]
        finally:
            if daemon.poll() is None:
                stopped = cli("daemon", "stop", check=False)
                if stopped.returncode:
                    raise RuntimeError(
                        "Could not stop the disposable daemon: " + stopped.stderr
                    )
            try:
                daemon.wait(timeout=15)
            except subprocess.TimeoutExpired:
                daemon.terminate()
                daemon.wait(timeout=5)
                raise RuntimeError("Disposable daemon did not exit") from None
    return {
        "verified": True,
        "installed_sdk": True,
        "native_daemon": True,
        "sessions": sessions,
        "pty_io": action == "full",
        "mcp": True,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--payload", type=Path, required=True)
    parser.add_argument("--state-home", type=Path)
    parser.add_argument("--action", choices=("full", "inspect"), default="full")
    args = parser.parse_args()
    # Keep Unix socket paths short, including on macOS's /var/folders temp root.
    with tempfile.TemporaryDirectory(
        prefix="ucv-", dir=None if os.name == "nt" else "/tmp"
    ) as temporary:
        print(
            json.dumps(
                exercise(
                    args.payload.resolve(),
                    args.state_home or Path(temporary).resolve() / "home",
                    args.action,
                )
            )
        )
