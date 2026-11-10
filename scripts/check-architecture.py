#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Run dependency, public-surface and external-consumer architecture checks."""

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

from _support.processes import exit_code, handle_signals, run

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    for mode in ("all", "positive", "consumer"):
        modes.add_argument("--" + mode, action="store_const", const=mode, dest="mode")
    modes.add_argument("--negative", type=Path, metavar="FIXTURE")
    args = parser.parse_args()
    mode = "negative" if args.negative else args.mode or "all"
    for tool in ("cargo", "nm"):
        if not shutil.which(tool):
            parser.error(f"architecture gate requires {tool} on PATH")
    command = [sys.executable, ROOT / "tests/architecture/architecture_gate.py", "--root", ROOT, "--mode", mode]
    if args.negative:
        command += ["--fixture", (ROOT / args.negative).resolve()]
    try:
        with handle_signals():
            if mode in {"all", "consumer"}:
                cache = Path(os.environ.get("UNDERCURRENT_CACHE_ROOT", Path.home() / ".cache/undercurrent")) / "undercurrent-architecture"
                cache.mkdir(parents=True, exist_ok=True)
                with tempfile.TemporaryDirectory(prefix="consumer.", dir=cache) as temporary:
                    run(command + ["--target-dir", temporary], timeout=None, stdout=None, stderr=None)
            else:
                run(command, timeout=None, stdout=None, stderr=None)
            if mode == "all":
                run([sys.executable, ROOT / "tests/architecture/test_private_reset_boundary.py"],
                    timeout=None, stdout=None, stderr=None)
    except subprocess.CalledProcessError as error:
        return exit_code(error.returncode)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
