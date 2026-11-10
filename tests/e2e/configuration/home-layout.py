#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Verify home layout, isolation, persistence, and rejected inputs with isolated binaries."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile


def main():
    binaries = Path(sys.argv[1] if len(sys.argv) > 1 else "target/debug").resolve()
    shipped_profiles = Path(__file__).resolve().parents[3] / "crates/undercurrent-daemon/profiles"
    expected_profiles = {path.name for path in shipped_profiles.glob("*.yaml")}
    assert expected_profiles, "shipped profile definitions are missing"
    with tempfile.TemporaryDirectory(prefix="uch-", dir="/tmp") as temporary:
        root = Path(temporary)
        user = root / "user"
        user.mkdir()
        env = {"PATH": "/usr/bin:/bin", "HOME": str(user), "TERM": "xterm",
               "XDG_CONFIG_HOME": str(root / "unused-config"),
               "XDG_STATE_HOME": str(root / "unused-state")}

        def command(*args, environment=None, ok=True, binary="uc"):
            result = subprocess.run([str(binaries / binary), *args],
                                    env=environment or env, capture_output=True,
                                    text=True, timeout=20)
            assert (result.returncode == 0) == ok, (args, result.stdout, result.stderr)
            return result.stdout + result.stderr

        for name in ("UNDERCURRENT_CONFIG", "UNDERCURRENT_DATABASE_PATH",
                     "UNDERCURRENT_INSTANCE_LOCK_PATH", "UNDERCURRENT_LOCAL_ENDPOINT"):
            for value in ("", "/private/retired"):
                for binary, args in (("uc", ("config", "validate")), ("uc-daemon", ())):
                    output = command(*args, environment=env | {name: value}, ok=False, binary=binary)
                    assert "UNDERCURRENT_HOME" in output and "/private/retired" not in output
        command("--config", "/private/retired", "config", "validate", ok=False)
        command("--database", "/private/retired", binary="uc-daemon", ok=False)
        for value in ("", "relative"):
            command("config", "validate", environment=env | {"UNDERCURRENT_HOME": value}, ok=False)
        default = user / ".aixand/undercurrent"
        # Rejected inputs may record failure evidence, but must not initialize
        # daemon state, sockets, configuration, or seeded profiles.
        assert {path.name for path in (user / ".aixand").iterdir()} == {"undercurrent"}
        assert {path.name for path in default.iterdir()} == {"logs"}
        assert list((default / "logs/incidents").glob("*.json"))

        custom = root / "custom"
        custom.mkdir(mode=0o700)
        (custom / "config.yaml").write_text(
            "schema_version: 0.1.0-beta\ncli:\n  list_page_size: 17\n"
            "terminal:\n  scrollback:\n    memory_lines: 17\n")
        for home, selected in ((default, env), (custom, env | {"UNDERCURRENT_HOME": str(custom)})):
            command("config", "validate", environment=selected)
            try:
                command("daemon", "start", environment=selected)
                command("start", "home-record", "--", "/bin/cat", environment=selected)
                command("stop", "home-record", environment=selected)
                command("group", "ls", environment=selected)
                assert (home / "state/undercurrent.db").is_file()
                assert (home / "run/control.sock").is_socket()
                assert (home / "run/session-host.sock").is_socket()
                assert (home / "run/session-host.auth").is_file()
                assert (home / "state/session-host/session-host.execution-jobs").is_dir()
                assert (home / "state/session-host/session-host.attempt-receipts").is_dir()
                assert (home / "state/group").is_dir()
                assert (home / "logs/daemon.jsonl").is_file()
                assert {path.name for path in (home / "profiles").glob("*.yaml")} == expected_profiles
                assert not (home / "undercurrent.db").exists()
                command("daemon", "restart", environment=selected)
                assert "home-record" in command("ls", environment=selected)
            finally:
                command("daemon", "stop", environment=selected)
        assert not (root / "unused-config").exists()
        assert not (root / "unused-state").exists()

        # Reject old state before creating an empty replacement installation.
        legacy = root / "legacy"
        legacy.mkdir(mode=0o700)
        (legacy / "undercurrent.db").write_bytes(b"legacy")
        output = command("daemon", "start", environment=env | {"UNDERCURRENT_HOME": str(legacy)}, ok=False)
        assert "legacy" in output.lower()
        assert not (legacy / "state").exists()
        # A custom initialized home remains usable alongside another old installation.
        (user / ".local/state/undercurrent").mkdir(parents=True)
        (user / ".local/state/undercurrent/undercurrent.db").write_bytes(b"legacy")
        command("config", "validate", environment=env | {"UNDERCURRENT_HOME": str(custom)})
        print(json.dumps({"status": "passed", "checks": ["default/custom home", "retired inputs",
              "CLI/daemon agreement", "restart", "group view", "legacy-layout rejection", "home isolation"]}))


if __name__ == "__main__":
    main()
