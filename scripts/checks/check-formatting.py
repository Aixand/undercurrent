#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Check every first-party workspace package without traversing vendored source."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
import tomllib


PRODUCT_ROOT = Path(__file__).resolve().parents[2]
MONOREPO_ROOT = PRODUCT_ROOT
MONOREPO_MANIFEST = MONOREPO_ROOT / "Cargo.toml"
EXPECTED_WORKSPACE_PACKAGES = (
    "aixand-undercurrent",
    "aixand-undercurrent-foundation",
    "aixand-undercurrent-multiplex",
    "aixand-undercurrent-platform-windows",
    "aixand-undercurrent-protocol",
    "aixand-undercurrent-transport-core",
    "aixand-undercurrent-transport-local",
    "aixand-undercurrent-wire",
    "undercurrent-application",
    "undercurrent-cli",
    "undercurrent-config",
    "undercurrent-daemon",
    "undercurrent-diagnostics",
    "undercurrent-domain",
    "undercurrent-mcp",
    "undercurrent-platform-macos",
    "undercurrent-python-bridge",
    "undercurrent-runtime",
    "undercurrent-store-sqlite",
)
EXPECTED_WORKSPACE_EXCLUDES = (
    "tests/architecture/consumer",
    "vendor/vt100",
    "vendor/vte",
    "vendor/portable-pty",
)
class FormattingPolicyError(Exception):
    """Fail-closed formatting policy error."""


def validate_policy(
    workspace_packages: tuple[str, ...],
    workspace_excludes: tuple[str, ...],
) -> None:
    if tuple(sorted(workspace_packages)) != EXPECTED_WORKSPACE_PACKAGES:
        raise FormattingPolicyError(
            "workspace formatting package set is not the exact first-party set"
        )
    if workspace_excludes != EXPECTED_WORKSPACE_EXCLUDES:
        raise FormattingPolicyError(
            "workspace excludes must contain exactly the test consumers and approved vt100, vte and portable-pty"
        )
    if "vt100" in workspace_packages:
        raise FormattingPolicyError("vendored vt100 must not be a workspace member")
    if "vte" in workspace_packages:
        raise FormattingPolicyError("vendored vte must not be a workspace member")


def load_policy() -> tuple[tuple[str, ...], tuple[str, ...]]:
    completed = subprocess.run(
        [
            "cargo",
            "metadata",
            "--locked",
            "--offline",
            "--no-deps",
            "--format-version",
            "1",
            "--manifest-path",
            str(MONOREPO_MANIFEST),
        ],
        cwd=PRODUCT_ROOT,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise FormattingPolicyError("locked offline Cargo metadata failed")
    metadata = json.loads(completed.stdout)
    member_ids = set(metadata["workspace_members"])
    selected_packages = [
        package
        for package in metadata["packages"]
        if package["id"] in member_ids
        and package["name"] in EXPECTED_WORKSPACE_PACKAGES
    ]
    for package in selected_packages:
        manifest = Path(package["manifest_path"]).resolve()
        if not manifest.is_relative_to(PRODUCT_ROOT / "crates"):
            raise FormattingPolicyError(
                f"Undercurrent workspace package is outside the product: {package['name']}"
            )
    workspace_packages = tuple(sorted(package["name"] for package in selected_packages))
    with MONOREPO_MANIFEST.open("rb") as source:
        manifest = tomllib.load(source)
    product_prefix = Path(".")
    workspace_excludes = tuple(
        Path(entry).relative_to(product_prefix).as_posix()
        for entry in manifest["workspace"].get("exclude", [])
        if Path(entry).is_relative_to(product_prefix)
    )
    return workspace_packages, workspace_excludes


def run_checked(command: list[str]) -> None:
    completed = subprocess.run(
        command,
        cwd=PRODUCT_ROOT,
        stdin=subprocess.DEVNULL,
        check=False,
    )
    if completed.returncode != 0:
        raise FormattingPolicyError(f"formatting command failed: {' '.join(command)}")


def self_test() -> None:
    validate_policy(
        EXPECTED_WORKSPACE_PACKAGES,
        EXPECTED_WORKSPACE_EXCLUDES,
    )

    def expect_rejected(
        packages: tuple[str, ...],
        excludes: tuple[str, ...],
    ) -> None:
        try:
            validate_policy(packages, excludes)
        except FormattingPolicyError:
            return
        raise AssertionError("invalid formatting policy was accepted")

    for package in EXPECTED_WORKSPACE_PACKAGES:
        expect_rejected(
            tuple(item for item in EXPECTED_WORKSPACE_PACKAGES if item != package),
            EXPECTED_WORKSPACE_EXCLUDES,
        )
    expect_rejected(
        EXPECTED_WORKSPACE_PACKAGES,
        (*EXPECTED_WORKSPACE_EXCLUDES, "vendor/another-subtree"),
    )
    expect_rejected(
        (*EXPECTED_WORKSPACE_PACKAGES, "vt100"),
        EXPECTED_WORKSPACE_EXCLUDES,
    )
    expect_rejected(
        (*EXPECTED_WORKSPACE_PACKAGES, "vte"),
        EXPECTED_WORKSPACE_EXCLUDES,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    try:
        if args.self_test:
            self_test()
            print("formatting policy self-test passed: exact membership and exclusions")
            return 0
        workspace_packages, workspace_excludes = load_policy()
        validate_policy(workspace_packages, workspace_excludes)
        for package in workspace_packages:
            run_checked(
                [
                    "cargo",
                    "fmt",
                    "--manifest-path",
                    str(MONOREPO_MANIFEST),
                    "--package",
                    package,
                    "--",
                    "--check",
                ]
            )
    except (FormattingPolicyError, json.JSONDecodeError, OSError, tomllib.TOMLDecodeError) as error:
        print(f"formatting-policy: {error}", file=sys.stderr)
        return 1
    print(
        "formatting passed: "
        f"{len(EXPECTED_WORKSPACE_PACKAGES)} workspace packages; vendored source excluded"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
