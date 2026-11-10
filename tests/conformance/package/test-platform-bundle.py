#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Assemble, verify, install and exercise a native bundle without publishing it."""
import argparse
import hashlib
import json
import os
import shutil
import platform
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[3]
HELPER = ROOT / "scripts/release/release_bundle_qualification.py"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    artifacts = parser.add_mutually_exclusive_group()
    artifacts.add_argument("--bin-dir", type=Path, default=ROOT / "target/release",
                           help="existing release binaries (default: target/release)")
    artifacts.add_argument("--build", action="store_true",
                           help="build current source before qualifying its release binaries")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "target/platform-bundles")
    args = parser.parse_args()
    target = {("Darwin", "arm64"): "macos-arm64", ("Darwin", "x86_64"): "macos-x86_64",
              ("Linux", "x86_64"): "linux-x86_64-gnu",
              ("Windows", "AMD64"): "windows-x86_64-msvc"}.get((platform.system(), platform.machine()))
    assert target, "unsupported qualification host"
    if args.build:
        # Explicit target-dir keeps build output and qualification input paired,
        # even when the caller has configured CARGO_TARGET_DIR elsewhere.
        subprocess.run(["cargo", "build", "--locked", "--release", "--target-dir",
                        str(ROOT / "target"), "-p", "undercurrent-cli", "-p",
                        "undercurrent-daemon", "--bins"], cwd=ROOT, check=True)
    suffix = ".exe" if os.name == "nt" else ""
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parent = "/private/tmp" if sys.platform == "darwin" else None
    with tempfile.TemporaryDirectory(prefix="uc-bundle-", dir=parent) as tmp:
        base = Path(tmp)
        bundle, prefix = base / "bundle", base / "installed"
        def helper(command, *options, expected=True, target_override=None):
            result = subprocess.run([sys.executable, str(HELPER), command, "--platform", target_override or target,
                                     *map(str, options)], capture_output=True, text=True, timeout=60)
            assert (result.returncode == 0) == expected, result.stdout + result.stderr
            return json.loads(result.stdout) if expected else result.stderr
        inputs = {
            "package-manifest": "undercurrent-package.json", "package-schema": "schemas/undercurrent-package.schema.json",
            "config-schema": "schemas/config.schema.json", "license": "LICENSE",
            "foundation-notice": "crates/undercurrent-foundation/NOTICE", "vt100-license": "vendor/vt100/LICENSE",
            "vt100-provenance": "vendor/vt100/PROVENANCE.json", "vte-apache-license": "vendor/vte/LICENSE-APACHE",
            "vte-mit-license": "vendor/vte/LICENSE-MIT", "vte-provenance": "vendor/vte/PROVENANCE.json",
            "vte-unsafe-audit": "vendor/vte/UNSAFE-AUDIT.json", "install-document": "docs/getting-started/bundles.md",
        }
        options = [part for key, value in inputs.items() for part in ("--" + key, ROOT / value)]
        helper("assemble", "--artifact-dir", args.bin_dir.resolve(), "--output", bundle, *options)
        helper("verify", "--bundle", bundle)
        other = "macos-x86_64" if target != "macos-x86_64" else "macos-arm64"
        helper("verify", "--bundle", bundle, expected=False, target_override=other)
        # A changed binary must fail before installation or execution.
        executable = bundle / ("bin/uc" + suffix)
        original = executable.read_bytes()
        executable.write_bytes(original + b"tampered")
        helper("verify", "--bundle", bundle, expected=False)
        executable.write_bytes(original)
        transfer = base / "transfer.tar"
        helper("archive", "--bundle", bundle, "--output", transfer)
        unpacked = base / "unpacked"
        helper("unpack", "--archive", transfer, "--output", unpacked)
        bundle = unpacked
        helper("install", "--bundle", bundle, "--prefix", prefix, "--accept-license", "Apache-2.0")
        for name in ("uc", "uc-daemon", "uc-session-host"):
            subprocess.run([str(prefix / "bin" / (name + suffix)), "--version"], check=True, timeout=20)
        if os.name == "nt":
            pwsh = shutil.which("pwsh")
            assert pwsh, "PowerShell 7 is required for installed Windows qualification"
            subprocess.run([sys.executable, str(ROOT / "tests/e2e/platform/windows-native-smoke.py"),
                            "--skip-build", "--bin-dir", str(prefix / "bin"), "--powershell", pwsh], check=True, timeout=300)
        else:
            subprocess.run([sys.executable, str(ROOT / "tests/e2e/platform/test-platform-contract.py"),
                            str(prefix / "bin")], check=True, timeout=180)
        # Exercise the stop/backup/replace/restart path using the same version.
        # Cross-version schema compatibility needs an actual prior release fixture.
        env = {k: v for k, v in os.environ.items() if not k.startswith(("UC_", "UNDERCURRENT_", "XDG_"))}
        env.update(UNDERCURRENT_HOME=str(base / "product"), HOME=str(base))
        def uc(*parts, check=True):
            return subprocess.run([str(prefix / ("bin/uc" + suffix)), "--timeout", "10s", *parts],
                                  env=env, capture_output=True, text=True, check=check, timeout=30)
        try:
            uc("daemon", "start")
            if os.name == "nt":
                uc("start", "upgrade-state", "--", os.environ["COMSPEC"], "/d", "/q")
            else:
                uc("start", "upgrade-state", "--", "/bin/sleep", "30")
            uc("stop", "upgrade-state")
            uc("daemon", "stop")
            shutil.copytree(base / "product/state", base / "state-backup")
            staged = base / "staged"
            helper("install", "--bundle", bundle, "--prefix", staged, "--accept-license", "Apache-2.0")
            prefix.rename(base / "old-installation")
            staged.rename(prefix)
            uc("daemon", "start")
            assert "persisted: true" in uc("inspect", "upgrade-state").stdout
            print("PASS same-version replacement preserves durable session state", flush=True)
        finally:
            uc("daemon", "stop", check=False)
        archive = args.output_dir / f"undercurrent-0.1.0-beta-{target}.tar"
        result = helper("archive", "--bundle", bundle, "--output", archive)
        second = base / "second.tar"
        helper("archive", "--bundle", bundle, "--output", second)
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        assert digest == hashlib.sha256(second.read_bytes()).hexdigest(), "archive is not deterministic"
        report = {"platform": target, "status": "passed", "archive": str(archive), "sha256": digest,
                  "checks": ["verify", "wrong-architecture", "tamper", "private-archive-unpack", "install", "native-cli", "same-version-state-preserving-replacement", "deterministic-archive"],
                  "published": False, "signed_or_notarized": False}
        (args.output_dir / f"{target}.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
