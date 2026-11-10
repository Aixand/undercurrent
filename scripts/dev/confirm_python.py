#!/usr/bin/env python3
"""Confirm the public PyPI wheel equals this run's verified distribution."""

import argparse
import json
from pathlib import Path
import subprocess
import tempfile
import time

from core import Failure, digest, require
from packages import validate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    args = parser.parse_args()
    artifact, manifest = validate(args.artifact)
    require(manifest["profile"].startswith("python-"), "Expected a Python release")
    with tempfile.TemporaryDirectory(prefix="pypi-confirm-") as temporary:
        destination = Path(temporary)
        command = [
            "uv",
            "tool",
            "run",
            "--isolated",
            "--python",
            manifest["runtime"]["minor"],
            "--from",
            "pip==25.3",
            "pip",
            "--isolated",
            "download",
            "--no-cache-dir",
            "--index-url",
            "https://pypi.org/simple",
            "--no-deps",
            "--only-binary=:all:",
            "--dest",
            str(destination),
            manifest["name"] + "==" + manifest["version"],
        ]
        for attempt in range(5):
            result = subprocess.run(command, capture_output=True, text=True)
            if result.returncode == 0:
                break
            if attempt == 4:
                raise Failure(
                    "Published version could not be fetched from PyPI: "
                    + result.stderr[-2000:]
                )
            time.sleep(3)
        wheels = list(destination.glob("*.whl"))
        require(
            len(wheels) == 1
            and digest(wheels[0]) == digest(artifact / manifest["primary"]),
            "Published PyPI wheel differs from the verified artifact",
        )
    print(
        json.dumps(
            {
                "registry_confirmed": True,
                "name": manifest["name"],
                "version": manifest["version"],
            }
        )
    )


if __name__ == "__main__":
    try:
        main()
    except (Failure, OSError, ValueError) as error:
        import sys

        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
