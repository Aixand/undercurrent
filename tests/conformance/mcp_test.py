#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Qualify the stdio MCP adapter using only a disposable local daemon."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from mcp_probe import Client, exercise


def run(binary_dir):
    suffix = ".exe" if os.name == "nt" else ""
    mcp = binary_dir / ("uc-mcp" + suffix)
    with tempfile.TemporaryDirectory(prefix="ucm-", dir=None if os.name == "nt" else "/tmp") as temporary:
        # macOS /tmp is a symlink; private IPC requires physical parent paths.
        home = Path(temporary).resolve() / "home"
        for protocol in ("2024-11-05", "2025-11-25", "2026-07-28"):
            with Client(mcp, home, protocol=protocol) as client:
                assert len(client.request("tools/list", {})["result"]["tools"]) == 9
                assert client.call("get_status")["connected"] is False
                invalid = client.call_raw("start_session", name="invalid", program="invalid", cwd="relative")
                assert "error" in invalid or invalid["result"]["isError"], invalid
        assert not home.exists(), "Discovery/status/invalid arguments created the runtime home"
        env = {key: value for key, value in os.environ.items() if not key.startswith(("UNDERCURRENT_", "UC_"))}
        env["UNDERCURRENT_HOME"] = str(home)
        uc = binary_dir / ("uc" + suffix)
        with (Path(temporary) / "daemon.log").open("w+") as log:
            daemon = subprocess.Popen([str(binary_dir / ("uc-daemon" + suffix))], env=env, stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 15
                while True:
                    status = subprocess.run([str(uc), "daemon", "status", "--json"], env=env, capture_output=True, timeout=5)
                    if status.returncode == 0:
                        break
                    if daemon.poll() is not None or time.monotonic() > deadline:
                        log.seek(0)
                        raise RuntimeError(log.read()[-4000:])
                    time.sleep(0.05)
                result = exercise(mcp, home)
            finally:
                if daemon.poll() is None:
                    subprocess.run([str(uc), "daemon", "stop"], env=env, capture_output=True, timeout=20, check=True)
                daemon.wait(timeout=15)
        result["protocols"] = ["2024-11-05", "2025-11-25", "2026-07-28"]
        return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary-dir", type=Path, default=ROOT / "target/debug")
    print(json.dumps(run(parser.parse_args().binary_dir.resolve()), indent=2))
