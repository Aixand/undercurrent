#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Run cross-user IPC checks in private Linux user and mount namespaces."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from _support.processes import exit_code, handle_signals, run


def main():
    if sys.platform != "linux":
        raise SystemExit("cross-user IPC checks require Linux user namespaces")
    environment = os.environ | {"RUSTUP_TOOLCHAIN": tomllib.loads((ROOT / "rust-toolchain.toml").read_text())["toolchain"]["channel"]}
    try:
        with handle_signals():
            build = run(["cargo", "test", "--locked", "-p", "aixand-undercurrent-transport-local", "--test", "cross_user",
                         "--no-run", "--message-format=json"], cwd=ROOT, env=environment, timeout=None, stderr=None)
            for line in build.stdout.splitlines():
                item = json.loads(line)
                if item.get("reason") == "compiler-artifact" and item.get("executable"):
                    for test in ("another_user_is_denied_and_kernel_credentials_cannot_be_spoofed",
                                 "client_rejects_a_foreign_server_even_with_trusted_socket_metadata"):
                        # Mount and exec must occur inside the new namespace.
                        run(["unshare", "--map-root-user", "--map-auto", "--mount", "/bin/sh", "-c",
                             'mount -t tmpfs -o mode=1777 tmpfs /tmp && exec "$@"', "cross-user",
                             item["executable"], "--ignored", "--exact", test],
                            env=environment, timeout=30, stdout=None, stderr=None)
                    return 0
            raise SystemExit("cross-user test binary was not built")
    except subprocess.CalledProcessError as error:
        return exit_code(error.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
