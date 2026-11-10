#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Exercise native/sealed launches with isolated real CLI, daemon and session host.

Build the CLI and daemon binaries first, then run:
    python3 tests/e2e/configuration/test-launch-modes.py target/debug
The selected Python must support venv; cc is required for library/resource checks.
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time


def main():
    binaries = Path(sys.argv[1] if len(sys.argv) > 1 else "target/debug").resolve()
    with tempfile.TemporaryDirectory(prefix="uc-launch-modes-") as temporary:
        root = Path(temporary)
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(("UNDERCURRENT_", "UC_"))}
        env["UNDERCURRENT_HOME"] = str(root / "home")
        env["XDG_RUNTIME_DIR"] = str(root / "run")
        Path(env["XDG_RUNTIME_DIR"]).mkdir(mode=0o700)
        daemon = None

        def uc(*args, check=True, json_output=False):
            flags = ["--json"] if json_output else []
            result = subprocess.run(
                [str(binaries / "uc"), "--timeout", "10s", *flags, *args],
                env=env, cwd=root, capture_output=True, text=True, timeout=20,
            )
            if check and result.returncode:
                raise AssertionError(f"uc {args}: {result.stdout}{result.stderr}")
            return json.loads(result.stdout) if json_output else result

        def wait(predicate):
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if predicate():
                    return
                time.sleep(0.1)
            raise AssertionError("condition did not settle")

        def start_daemon():
            nonlocal daemon
            daemon = subprocess.Popen(
                [str(binaries / "uc-daemon")], env=env,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            wait(lambda: uc("daemon", "status", check=False).returncode == 0)

        def completed(name, marker=None):
            wait(lambda: uc("inspect", name, json_output=True)["summary"]["lifecycle"]["state"]
                 in ("exited", "failed", "stopped"))
            state = uc("inspect", name, json_output=True)["summary"]["lifecycle"]
            assert state == {"state": "exited", "code": 0}, state
            if marker:
                wait(lambda: marker in uc("logs", name).stdout)

        def assert_mode(name, expected):
            detail = uc("inspect", name, json_output=True)
            receipt = detail["configuration"]["executable_resolution"]
            assert receipt.get("launch_mode", "sealed") == expected, receipt

        try:
            subprocess.run([sys.executable, "-m", "venv", "--without-pip", str(root / "venv")],
                           check=True, capture_output=True, timeout=30)
            python = root / "venv" / "bin" / "python"
            probe = root / "probe.py"
            probe.write_text(
                "import sys, encodings\n"
                f"assert sys.prefix == {str(root / 'venv')!r}, sys.prefix\n"
                f"assert sys.executable == {str(python)!r}, sys.executable\n"
                "print('VENV_OK', flush=True)\n"
            )
            start_daemon()
            uc("start", "python", "--", str(python), str(probe))
            completed("python", "VENV_OK")
            assert_mode("python", "native")

            entry = root / "entry"
            entry.write_text(f"#!{python}\n" + probe.read_text())
            entry.chmod(0o700)
            uc("start", "entry", "--", str(entry))
            completed("entry", "VENV_OK")
            assert uc("start", "sealed-script", "--launch-mode", "sealed", "--", str(entry),
                      check=False).returncode != 0

            # Native installs retain executable-relative libraries and resources.
            (root / "lib.c").write_text("int answer(void) { return 42; }\n")
            (root / "main.c").write_text('''#include <stdio.h>
#include <unistd.h>
#include <string.h>
extern int answer(void);
int main(void) {
    char path[4096];
    ssize_t n = readlink("/proc/self/exe", path, sizeof(path) - 1);
    if (n < 0) return 1;
    path[n] = 0;
    char *slash = strrchr(path, '/');
    if (!slash) return 2;
    strcpy(slash + 1, "asset.txt");
    FILE *f = fopen(path, "r");
    if (!f) return 3;
    int value = 0;
    int matched = fscanf(f, "%d", &value);
    fclose(f);
    if (matched != 1 || value != answer()) return 4;
    puts("RESOURCE_OK");
    return 0;
}
''')
            (root / "asset.txt").write_text("42\n")
            subprocess.run(["cc", "-shared", "-fPIC", "lib.c", "-o", "libanswer.so"],
                           cwd=root, check=True, capture_output=True, timeout=30)
            subprocess.run(["cc", "main.c", "-L.", "-lanswer", "-Wl,-rpath,$ORIGIN", "-o", "app"],
                           cwd=root, check=True, capture_output=True, timeout=30)
            uc("start", "resources", "--", str(root / "app"))
            completed("resources", "RESOURCE_OK")

            # Automatic restart must retain native mode and accept installed updates.
            automatic = root / "automatic"
            automatic.write_text("#!/bin/sh\necho AUTO_FIRST\nexit 1\n")
            automatic.chmod(0o700)
            uc("start", "automatic", "--restart", "on-failure", "--restart-delay", "2s",
               "--", str(automatic))
            wait(lambda: "AUTO_FIRST" in uc("logs", "automatic").stdout)
            replacement = root / "replacement"
            auto_result = root / "auto-result"
            replacement.write_text(f"#!/bin/sh\nprintf AUTO_UPDATED > '{auto_result}'\n")
            replacement.chmod(0o700)
            replacement.replace(automatic)
            wait(auto_result.exists)
            assert auto_result.read_text() == "AUTO_UPDATED"
            completed("automatic")
            assert_mode("automatic", "native")

            sealed = root / "sealed-echo"
            shutil.copy2("/bin/echo", sealed)
            uc("start", "sealed", "--launch-mode", "sealed", "--", str(sealed), "SEALED_OK")
            completed("sealed", "SEALED_OK")
            assert_mode("sealed", "sealed")

            # Reload durable state, then verify both policies on manual restart.
            uc("daemon", "stop")
            daemon.wait(timeout=15)
            start_daemon()
            assert_mode("python", "native")
            assert_mode("sealed", "sealed")
            uc("restart", "python")
            completed("python", "VENV_OK")
            entry_result = root / "entry-result"
            entry.write_text(f"#!{python}\nfrom pathlib import Path\nPath({str(entry_result)!r}).write_text('UPDATED_ENTRY')\n")
            uc("restart", "entry")
            wait(entry_result.exists)
            completed("entry")
            assert entry_result.read_text() == "UPDATED_ENTRY"
            replacement = root / "replacement-echo"
            shutil.copy2("/bin/false", replacement)
            replacement.replace(sealed)
            assert uc("restart", "sealed", check=False).returncode != 0
            assert_mode("sealed", "sealed")
            print("PASS: native venv, shebang, libraries/assets, automatic/manual restart, recovery; sealed verification")
        finally:
            if daemon is not None:
                uc("daemon", "stop", check=False)
                if daemon.poll() is None:
                    daemon.terminate()
                try:
                    daemon.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    daemon.kill()
                    daemon.wait(timeout=5)


if __name__ == "__main__":
    main()
