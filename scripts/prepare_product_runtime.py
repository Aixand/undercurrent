# SPDX-License-Identifier: Apache-2.0
"""Fetch the pinned helper/SDK interpreter bundled with Undercurrent."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts/dev"))
from catalog import Workspace, host
from core import digest
from runtime_support import download

RELEASE = "20260825"
TRIPLES = {
    "linux-x86_64-gnu": "x86_64-unknown-linux-gnu",
    "macos-x86_64": "x86_64-apple-darwin",
    "macos-arm64": "aarch64-apple-darwin",
    "windows-x86_64-msvc": "x86_64-pc-windows-msvc",
}


def descriptor(target):
    record = Workspace(ROOT).runtime("python", target)
    record.pop("path")
    record["url"] = (
        "https://github.com/astral-sh/python-build-standalone/releases/download/"
        f"{RELEASE}/cpython-3.12.14%2B{RELEASE}-{TRIPLES[target]}-install_only_stripped.tar.gz"
    )
    return record


def prepare(target):
    record = descriptor(target)
    path = ROOT / Workspace(ROOT).runtime("python", target)["path"]
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if not path.is_file() or path.is_symlink() or digest(path) != record["sha256"]:
            raise RuntimeError(
                f"Changed runtime cache; remove this file explicitly: {path}"
            )
        return path
    with tempfile.TemporaryDirectory(prefix=".runtime-", dir=path.parent) as temporary:
        candidate = Path(temporary) / "archive"
        download(record["url"], candidate, record["sha256"])
        candidate.replace(path)
    return path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=TRIPLES, default=host())
    parser.add_argument("--bootstrap-output", type=Path)
    args = parser.parse_args()
    print(prepare(args.target))
    if args.bootstrap_output:
        from bootstrap import generate

        generate(descriptor(args.target), args.bootstrap_output)
