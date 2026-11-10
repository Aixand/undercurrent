# SPDX-License-Identifier: Apache-2.0
"""Assemble the Python SDK, reviewed notices, and native companion resources."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts/dev"))
sys.path.insert(0, str(ROOT / "scripts/release"))
from catalog import Workspace, host
from core import digest, relative, source_file
import release_bundle_qualification as bundles
import third_party_licenses
from prepare_product_runtime import prepare

BINARIES = ("uc", "uc-daemon", "uc-session-host", "uc-python-bridge", "uc-mcp")


def run(*args, capture=False, env=None):
    return subprocess.run(
        [str(v) for v in args],
        cwd=ROOT,
        check=True,
        text=True,
        env=env,
        stdout=subprocess.PIPE if capture else sys.stderr,
    ).stdout


def sdk(output):
    # The isolated backend manages bytecode in its disposable build tree.
    # The repository-wide no-bytecode setting causes setuptools warnings here.
    build_env = os.environ.copy()
    build_env.pop("PYTHONDONTWRITEBYTECODE", None)
    run(
        "uv",
        "build",
        "--wheel",
        "--no-sources",
        "--python",
        "3.12.14",
        "--build-constraints",
        ROOT / "scripts/product-build-requirements.txt",
        "--out-dir",
        output / "wheelhouse",
        ROOT / "python",
        env=build_env,
    )


def resources(output, target, native, sdk_output):
    workspace = Workspace(ROOT)
    version = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["package"][
        "version"
    ]
    contract = json.loads((ROOT / "undercurrent-package.json").read_text())
    if (
        workspace.products["undercurrent"]["version"] != version
        or contract["version"] != version
    ):
        raise RuntimeError(
            "Product assembly, Cargo workspace, and package contract versions differ"
        )
    inventory = third_party_licenses.check(ROOT)
    run(sys.executable, "-B", ROOT / "scripts/checks/validate-package-manifest.py")
    bundles.configure_platform(target)
    bundles.copy_conpty_runtime(output)
    copies = {
        "LICENSE": "share/undercurrent/LICENSE",
        "undercurrent-package.json": "share/undercurrent/undercurrent-package.json",
        "schemas/undercurrent-package.schema.json": "share/undercurrent/undercurrent-package.schema.json",
        "schemas/config.schema.json": "share/undercurrent/config.schema.json",
        "crates/undercurrent-foundation/NOTICE": bundles.FOUNDATION_NOTICE_FILE,
        "vendor/vt100/LICENSE": bundles.VT100_LICENSE_FILE,
        "vendor/vt100/PROVENANCE.json": bundles.VT100_PROVENANCE_FILE,
        "vendor/vte/LICENSE-APACHE": bundles.VTE_APACHE_LICENSE_FILE,
        "vendor/vte/LICENSE-MIT": bundles.VTE_MIT_LICENSE_FILE,
        "vendor/vte/PROVENANCE.json": bundles.VTE_PROVENANCE_FILE,
        "vendor/vte/UNSAFE-AUDIT.json": bundles.VTE_UNSAFE_AUDIT_FILE,
        "vendor/portable-pty/LICENSE.md": bundles.PTY_LICENSE_FILE,
        "vendor/portable-pty/PROVENANCE.json": bundles.PTY_PROVENANCE_FILE,
        "docs/getting-started/product-installer.md": "share/undercurrent/INSTALL.md",
        "scripts/verify_product.py": "share/undercurrent/verify.py",
        "scripts/mcp_probe.py": "share/undercurrent/mcp_probe.py",
        "docs/agent/mcp.md": "share/undercurrent/MCP.md",
    }
    copies.update(
        {
            f"licenses/{source}": destination
            for destination, source in bundles.DEPENDENCY_FILES.items()
        }
    )
    for source, destination in copies.items():
        path = output / relative(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_file(ROOT, source), path)
    shutil.copytree(
        ROOT / "docs/agent/skills", output / "share/undercurrent/skills"
    )
    suffix = ".exe" if target.startswith("windows-") else ""
    subjects = {}
    for name in BINARIES:
        path = native / "bin" / (name + suffix)
        with path.open("rb") as stream:
            bundles.validate_binary_header(stream.read(65536), name)
        subjects[name + suffix] = digest(path)
    (sdk_wheel,) = (sdk_output / "wheelhouse").glob("*.whl")
    subjects[sdk_wheel.name] = digest(sdk_wheel)
    record = {
        "product": "undercurrent",
        "version": version,
        "target": target,
        "native_runtime": "external_daemon",
        "artifacts": subjects,
        "sdk_backend_constraints": (
            ROOT / "scripts/product-build-requirements.txt"
        ).read_text(),
        "rustc": run(
            "rustup", "run", "1.95.0", "rustc", "--version", capture=True
        ).strip(),
        "uv": run("uv", "--version", capture=True).strip(),
        "license_inventory_sha256": digest(ROOT / "licenses/inventory.json"),
        "reviewed_dependency_count": len(inventory["packages"]),
        "publication": "not-published",
    }
    (output / "share/undercurrent/product-installation.json").write_text(
        json.dumps(record, indent=2) + "\n", encoding="utf-8"
    )
    prepare(target)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--sdk", action="store_true")
    parser.add_argument("--native", type=Path)
    parser.add_argument("--sdk-output", type=Path)
    args = parser.parse_args()
    if args.target != host():
        parser.error("Build and verify product releases on the selected native target")
    if args.sdk:
        sdk(args.output)
    else:
        if not args.native or not args.sdk_output:
            parser.error("Resource assembly needs native and SDK component outputs")
        resources(args.output, args.target, args.native, args.sdk_output)
