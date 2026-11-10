#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Verify local IPC configuration, rejected inputs, and home-derived endpoints."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--artifact-dir", type=Path, required=True)
parser.add_argument("--report", type=Path, required=True)
args = parser.parse_args()
artifacts = args.artifact_dir.resolve(strict=True)
results = []

def run(binary, env, *arguments, success=True):
    result = subprocess.run([str(binary), *arguments], env=env, capture_output=True, text=True, timeout=15)
    assert (result.returncode == 0) == success, (binary.name, arguments, result.returncode, result.stderr)
    return result.stdout + result.stderr

with tempfile.TemporaryDirectory(prefix="ucm-", dir="/tmp") as temporary:
    root = Path(temporary)
    home = root / "state"
    home.mkdir(mode=0o700)
    config = home / "config.toml"
    env = {"PATH": "/usr/bin:/bin", "HOME": str(root), "TERM": "xterm", "UNDERCURRENT_HOME": str(home)}
    config.write_text("schema_version = \"0.1.0-beta\"\n")
    for name in ("UNDERCURRENT_ENDPOINT", "UNDERCURRENT_TOKEN_FILE", "UNDERCURRENT_TLS_CA_FILE", "UNDERCURRENT_TLS_CLIENT_CERT_FILE", "UNDERCURRENT_TLS_CLIENT_KEY_FILE"):
        for value in ("", "retired-private-value"):
            for binary, command in ((artifacts / "uc", ("config", "validate")), (artifacts / "uc-daemon", ())):
                output = run(binary, env | {name: value}, *command, success=False)
                assert "retired-private-value" not in output
    for document in ("schema_version = \"99.0.0\"\n", "schema_version = \"0.1.0-beta\"\n[websocket]\nport = 9100\n", "schema_version = \"0.1.0-beta\"\n[local]\nport = 9100\n"):
        config.write_text(document)
        run(artifacts / "uc", env, "config", "validate", success=False)
        run(artifacts / "uc-daemon", env, success=False)
    # Failed configuration loads may emit redacted diagnostics. They must still
    # reject the settings before creating sockets, locks, or runtime state.
    assert {p.name for p in home.iterdir()} <= {"config.toml", "logs"}, "rejected settings must not start services or create runtime state"
    for diagnostic in (home / "logs").rglob("*"):
        if diagnostic.is_file():
            assert diagnostic.suffix == ".json", "unexpected diagnostic artifact"
            assert "retired-private-value" not in diagnostic.read_text(), "rejected values must not leak into diagnostics"
    config.write_text("schema_version = \"0.1.0-beta\"\n")
    for endpoint in ("relative.sock", "/tmp/../traversal.sock", "/tmp/" + "x" * 110):
        run(artifacts / "uc", env | {"UNDERCURRENT_LOCAL_ENDPOINT": endpoint}, "config", "validate", success=False)
        run(artifacts / "uc-daemon", env | {"UNDERCURRENT_LOCAL_ENDPOINT": endpoint}, success=False)
    results.append("legacy configuration/environment and unsafe endpoint rejection")
    config.write_text('schema_version = \"0.1.0-beta\"\n[local]\nendpoint = "/tmp/retired.sock"\n')
    run(artifacts / "uc", env, "config", "validate", success=False)
    config.write_text("schema_version = \"0.1.0-beta\"\n")
    try:
        run(artifacts / "uc", env, "daemon", "start")
        assert (home / "run/control.sock").is_socket()
        run(artifacts / "uc", env, "ls")
    finally:
        run(artifacts / "uc", env, "daemon", "stop")
    results.append("CLI/daemon agree on home-derived endpoint; independent endpoints rejected")
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps({"status": "passed", "checks": results, "artifacts_sha256": {name: hashlib.sha256((artifacts/name).read_bytes()).hexdigest() for name in ("uc", "uc-daemon", "uc-session-host")}}, indent=2) + "\n")
    print("local IPC configuration checks passed")
