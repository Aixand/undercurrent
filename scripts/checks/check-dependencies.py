#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Reject missing or escaped local dependencies."""

import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]
MANIFESTS = (
    ROOT / "Cargo.toml",
    ROOT / "tests/architecture/consumer/Cargo.toml",
)

for manifest in MANIFESTS:
    metadata = json.loads(subprocess.check_output([
        "cargo", "metadata", "--manifest-path", str(manifest),
        "--locked", "--offline", "--all-features", "--format-version", "1",
    ], cwd=ROOT, text=True))
    if not Path(metadata["workspace_root"]).resolve().is_relative_to(ROOT):
        raise SystemExit(f"workspace escapes project: {manifest}")
    for package in metadata["packages"]:
        if package.get("source") is None:
            path = Path(package["manifest_path"]).resolve()
            if not path.is_relative_to(ROOT):
                raise SystemExit(f"local dependency escapes project: {path}")
    print(f"dependency closure passed: {manifest.relative_to(ROOT)}")
