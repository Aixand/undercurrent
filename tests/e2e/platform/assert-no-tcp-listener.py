#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Fail if the specified live Linux process owns any TCP listening socket."""
import os
from pathlib import Path
import sys

pid = int(sys.argv[1])
root = Path(f"/proc/{pid}")
inodes = set()
for fd in (root / "fd").iterdir():
    try:
        target = os.readlink(fd)
    except FileNotFoundError:
        continue
    if target.startswith("socket:["):
        inodes.add(target[8:-1])
for protocol in ("tcp", "tcp6"):
    for line in (root / "net" / protocol).read_text().splitlines()[1:]:
        fields = line.split()
        if fields[3] == "0A" and fields[9] in inodes:
            raise SystemExit(f"process {pid} owns a {protocol} listener")
assert root.exists(), "process exited during listener check"
print("daemon TCP listeners: 0")
