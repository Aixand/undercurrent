#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Exercise support collection against isolated, already-built Linux/POSIX binaries."""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time
import zipfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", type=Path, default=Path("target/debug"))
    args = parser.parse_args()
    binaries = args.artifact_dir.resolve()
    root = Path(tempfile.mkdtemp(prefix="uc-support-e2e-"))
    home = root / "home"
    home.mkdir(mode=0o700)
    (home / "config.yaml").write_text(json.dumps({
        "schema_version": "0.1.0-beta",
        "requests": {"diagnostic_page_size": 2},
    }))
    environment = dict(os.environ, UNDERCURRENT_HOME=str(home))
    environment.pop("_UNDERCURRENT_INCIDENT_ID", None)

    def uc(*arguments):
        return subprocess.run([str(binaries / "uc"), *arguments], env=environment,
                              capture_output=True, timeout=30)

    def success(*arguments):
        result = uc(*arguments)
        if result.returncode:
            raise AssertionError(f"command failed: {arguments[:3]}, {result.stderr.decode(errors='replace')}")
        return json.loads(result.stdout)

    daemon = subprocess.Popen([str(binaries / "uc-daemon")], env=environment,
                              stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL)
    stopped = False
    try:
        deadline = time.monotonic() + 15
        while uc("--json", "--timeout", "1s", "daemon", "status").returncode:
            assert daemon.poll() is None, "isolated daemon failed during startup"
            assert time.monotonic() < deadline, "daemon readiness deadline"
            time.sleep(0.05)

        started = success("--json", "start", "support-work", "--", "/bin/sh", "-c",
                          "printf SUPPORT_OUTPUT_CANARY; sleep 60")
        session_id = started["session"]["id"]
        (home / "profiles/codex.yaml").write_text("schema_version: 0.1.0-beta\nprivate: [SUPPORT_PROFILE_CANARY\n")
        deadline = time.monotonic() + 10
        while True:
            status = json.loads((home / "logs/profile-reload.json").read_text())
            if status["status"] == "rejected":
                break
            assert time.monotonic() < deadline, "profile reload rejection was not recorded"
            time.sleep(0.05)

        failed = uc("--json", "profile", "inspect", "missing-support-profile")
        assert failed.returncode != 0
        reference = json.loads(failed.stderr)["incident_id"]
        archive = root / "support.zip"
        success("--json", "--timeout", "20s", "debug", "collect", "--incident", reference,
                "--session", session_id, "--profile", "codex", "--output", str(archive))
        with zipfile.ZipFile(archive) as bundle:
            manifest = json.loads(bundle.read("manifest.json"))
            records = json.loads(bundle.read("incidents.json"))
            assert {r["component"] for r in records} >= {"cli", "daemon"}
            assert all(r["incident_id"] == reference for r in records)
            assert manifest["terminal_content_included"] is False
            assert json.loads(bundle.read("profile-reload.json"))["previous_definition_active"] is True
            pages = [json.loads(bundle.read(name)) for name in bundle.namelist() if name.startswith("diagnostics/")]
            assert any(len(section) > 1 for section in pages), (
                f"configured two-row pages were not continued: {manifest['sources']}")
            payload = b"".join(bundle.read(name) for name in bundle.namelist())
            assert b"SUPPORT_OUTPUT_CANARY" not in payload
            assert b"SUPPORT_PROFILE_CANARY" not in payload
            assert str(home).encode() not in payload
            assert json.loads(bundle.read("session.json"))["session_id"] == session_id

        with_output = root / "with-output.zip"
        result = success("--json", "debug", "collect", "--include-terminal-content", session_id,
                         "--output", str(with_output))
        assert result["terminal_content_included"] is True
        with zipfile.ZipFile(with_output) as bundle:
            content = json.loads(bundle.read("terminal-content.json"))
            assert content["page"]["result"] == "data"
            assert b"SUPPORT_OUTPUT_CANARY" in bytes(content["page"]["bytes"])

        failed_exec = uc("exec", "support-work", "--dir-launch", "--env-launch",
                         "--", "/bin/sh", "-c", "exit 7")
        assert failed_exec.returncode == 7, failed_exec.stderr.decode(errors="replace")
        exec_reference = re.search(rb"UC-[0-9a-f]{32}", failed_exec.stderr).group().decode()
        records = [json.loads(path.read_text()) for path in (home / "logs/incidents").glob("*.json")]
        assert any(record["incident_id"] == exec_reference and record["stage"] == "command_exit"
                   and record["exit_code"] == 7 for record in records)
        print("Support E2E passed: correlated failures, pagination, rejected reload, privacy, explicit terminal content, exit status")
    finally:
        result = uc("--json", "--timeout", "10s", "daemon", "stop")
        if result.returncode == 0:
            daemon.wait(timeout=10)
            stopped = True
        elif daemon.poll() is not None:
            stopped = not (home / "state/undercurrent.db").exists()
        if stopped:
            shutil.rmtree(root)
        else:
            raise RuntimeError(f"Cleanup could not establish daemon shutdown; evidence retained at {root}")


if __name__ == "__main__":
    main()
