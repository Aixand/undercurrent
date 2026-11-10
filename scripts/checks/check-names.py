#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Reject retired product namespaces and suite branding; preserve company names."""

from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[2]
ATTRIBUTION = "crates/undercurrent-foundation/NOTICE"
POLICY_FILES = {"scripts/checks/check-names.py", "tests/tooling/checks/naming_policy_test.py"}
# The shared installer and documentation checker retain their company-owned names.
RETIRED = re.compile(
    r"intessio|operation[./]undercurrent|"
    r"aixand_(?!(?:dev_|installer_cache\b|command(?:_names)?\b|docs_check\b))",
    re.IGNORECASE,
)
PRIVATE_PRODUCTS = re.compile(
    r"\baixand[ _-]suites?\b",
    re.IGNORECASE,
)


def violations(path, contents):
    """Return line numbers that introduce a retired namespace or private product."""
    if path == ATTRIBUTION or path in POLICY_FILES:
        return []
    return [number for number, line in enumerate(contents.splitlines(), 1)
            if RETIRED.search(line) or PRIVATE_PRODUCTS.search(line)]


def main():
    paths = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=ROOT
    ).decode().split("\0")
    failures = []
    for relative in sorted(set(filter(None, paths))):
        path = ROOT / relative
        if not path.is_file():
            continue
        if RETIRED.search(relative) or PRIVATE_PRODUCTS.search(relative):
            failures.append(f"{relative}: retired or private product name in path")
        try:
            contents = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        failures.extend(f"{relative}:{line}: retired namespace or private product reference"
                        for line in violations(relative, contents))
    if failures:
        raise SystemExit("\n".join(failures))
    print("naming policy passed: standalone product namespaces, company names, and source attribution")


if __name__ == "__main__":
    main()
