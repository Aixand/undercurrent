#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Exercise connection switching against real native daemons in isolated homes."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import zipfile


def qualify(binary: Path) -> dict:
    with tempfile.TemporaryDirectory(
        prefix="ucp-", dir=None if os.name == "nt" else "/tmp"
    ) as temporary:
        root = Path(temporary).resolve()
        user = root / "user 世界"
        user.mkdir()
        workspace = root / "workspace"
        other = root / "other"
        env = os.environ.copy()
        for key in (
            "UNDERCURRENT_HOME",
            "UNDERCURRENT_CONFIG",
            "UNDERCURRENT_ENDPOINT",
            "XDG_CONFIG_HOME",
            "XDG_STATE_HOME",
            "UC_SESSION",
        ):
            env.pop(key, None)
        env.update(HOME=str(user), USERPROFILE=str(user))
        calls = []
        started = set()

        def run(*args, success=True, environment=None):
            result = subprocess.run(
                [str(binary), "--json", *args],
                env=env | (environment or {}),
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=35,
            )
            calls.append({"command": list(args[:4]), "exit": result.returncode})
            if success and result.returncode:
                raise AssertionError(f"{args!r}: {result.stderr[-2500:]}")
            if not success:
                assert result.returncode != 0, args
                return json.loads(result.stderr)
            return json.loads(result.stdout)

        try:
            initial = run("settings", "profiles", "list")
            assert initial["active_profile"] == "default"
            assert (
                Path(initial["profiles"]["default"]["home"])
                == user / ".aixand" / "undercurrent"
            )
            run(
                "settings",
                "profiles",
                "add",
                "workspace",
                "--home",
                str(workspace),
                "--auto-start",
                "never",
            )
            run("settings", "profiles", "add", "other", "--home", str(other))
            run("settings", "set", "profile", "workspace")
            failed = run("ls", success=False)
            assert not (workspace / "state").exists()
            archive = root / "offline.zip"
            run(
                "debug",
                "collect",
                "--offline",
                "--incident",
                failed["incident_id"],
                "--output",
                str(archive),
            )
            with zipfile.ZipFile(archive) as report:
                records = json.loads(report.read("incidents.json"))
                assert any(
                    record["incident_id"] == failed["incident_id"] for record in records
                )

            run("daemon", "start")
            started.add("workspace")
            run(
                "start",
                "workspace-terminal-proof",
                "--profile",
                "server",
                "--label",
                "app=example-app",
                "--",
                sys.executable,
                "-c",
                "import time; time.sleep(180)",
            )
            sessions = run("ls")["items"]
            target = next(
                item for item in sessions if item["name"] == "workspace-terminal-proof"
            )
            target_id = target["id"]
            assert not run("--connection", "default", "ls")["items"]
            started.add("default")
            assert run("settings", "show")["active_profile"] == "workspace"
            # The child daemon must inherit the selected connection's home,
            # even when the caller has a different explicit environment home.
            assert not run(
                "--connection",
                "other",
                "ls",
                environment={"UNDERCURRENT_HOME": str(workspace)},
            )["items"]
            started.add("other")
            assert (other / "state" / "undercurrent.db").exists()
            run("settings", "set", "profile", "default")
            assert not run("ls")["items"]
            again = run("--connection", "workspace", "ls")["items"]
            assert any(item["id"] == target_id for item in again)
            run("settings", "set", "profile", "workspace")
            run("daemon", "stop", "--graceful", "--drain-timeout", "5s")
            started.remove("workspace")
            run("ls", success=False)
            run("settings", "set", "profile", "default")
        finally:
            for profile in sorted(started):
                run(
                    "--connection",
                    profile,
                    "daemon",
                    "stop",
                    "--graceful",
                    "--drain-timeout",
                    "5s",
                )
        return {
            "passed": True,
            "platform": sys.platform,
            "invocations": len(calls),
            "checks": [
                "permanent default",
                "saved selection",
                "connect-only startup policy",
                "selected incident home",
                "real session visibility",
                "isolated default sessions",
                "one-off connection selection",
                "daemon child home propagation",
                "switching preserves session identity",
                "no fallback after shutdown",
                "owned daemon cleanup",
            ],
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uc", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    result = qualify(args.uc.resolve())
    text = json.dumps(result, indent=2) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(text, encoding="utf-8")
    print(text, end="")


if __name__ == "__main__":
    main()
