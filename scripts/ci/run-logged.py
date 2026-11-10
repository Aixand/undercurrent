#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""GitHub custom shell: retain diagnostics without hiding command failures."""

import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _support.processes import exit_code, handle_signals, terminate_group


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: run-logged.py GENERATED_STEP_SCRIPT")
    script = sys.argv[1]
    folder = Path(os.environ.get("RUNNER_TEMP") or tempfile.gettempdir()) / "uc-ci-logs"
    folder.mkdir(parents=True, exist_ok=True)
    name = re.sub(r"[^a-zA-Z0-9_.-]", "_", os.environ.get("UC_CI_LOG_NAME") or Path(script).name)
    start = time.monotonic()
    status = 1
    with handle_signals(), (folder / f"{name}.log").open("wb") as log:
        process = subprocess.Popen(["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", script],
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            while chunk := process.stdout.read1(65536):
                log.write(chunk)
                log.flush()
                sys.stdout.buffer.write(chunk)
                sys.stdout.buffer.flush()
            status = exit_code(process.wait())
        except BaseException:
            terminate_group(process)
            raise
        finally:
            process.stdout.close()
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary).open("a") as output:
            output.write(f"\n- `{name}`: exit {status}, {int(time.monotonic() - start)}s.\n")
    if status:
        print(f"::error::Step {name} failed with exit {status}; see native diagnostics artifact.")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
