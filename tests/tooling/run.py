#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Run script self-tests without building or starting the product daemon."""

import argparse
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
GROUPS = ("checks", "e2e", "runners")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group", choices=(*GROUPS, "all"), default="all")
    args = parser.parse_args()
    groups = GROUPS if args.group == "all" else (args.group,)
    env = os.environ | {"PYTHONDONTWRITEBYTECODE": "1"}
    for group in groups:
        folder = Path(__file__).parent / group
        if group != "checks":
            commands = [[sys.executable, "-m", "unittest", "discover", "-s", str(folder), "-p", "test_*.py", "-v"]]
        else:
            commands = [[sys.executable, str(path)] for path in sorted(folder.glob("*_test.py"))]
        for command in commands:
            print(f"[tooling:{group}] {' '.join(command)}", flush=True)
            result = subprocess.run(command, cwd=ROOT, env=env, check=False)
            if result.returncode:
                return result.returncode if result.returncode > 0 else 128 - result.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
