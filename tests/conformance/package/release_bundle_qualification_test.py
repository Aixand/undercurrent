#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Conformance gate for release-bundle fail-closed adversarial fixtures."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[3]
HELPER = ROOT / "scripts" / "release" / "release_bundle_qualification.py"
FIXTURE = Path(__file__).with_name("release-bundle-negative-cases.json")


def main() -> int:
    expected = json.loads(FIXTURE.read_text(encoding="utf-8"))
    if expected.get("schema") != "undercurrent.release-bundle-negative-cases/v0.1.0-beta":
        raise AssertionError("negative-case fixture schema is not exact 0.1.0-beta")
    expected_cases = {
        item["id"]: item["expected_error"] for item in expected.get("cases", [])
    }
    if len(expected_cases) != 14:
        raise AssertionError("negative-case fixture must contain fourteen unique cases")
    with tempfile.TemporaryDirectory(prefix="ucb-conformance-") as temporary:
        report = Path(temporary) / "adversarial.json"
        completed = subprocess.run(
            [sys.executable, str(HELPER), "adversarial", "--report", str(report)],
            cwd=ROOT,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=20,
            check=False,
        )
        if completed.returncode != 0:
            raise AssertionError(f"adversarial helper failed safely: {completed.stderr.strip()}")
        actual = json.loads(report.read_text(encoding="utf-8"))
    if actual.get("schema") != "undercurrent.release-bundle-adversarial/v0.1.0-beta":
        raise AssertionError("adversarial report schema is invalid")
    if actual.get("status") != "passed" or actual.get("valid_fixture") != "passed":
        raise AssertionError("valid synthetic fixture or adversarial suite did not pass")
    if actual.get("artifacts_executed") is not False:
        raise AssertionError("verification must never execute fixture artifacts")
    actual_cases = {
        item.get("id"): item.get("error_code")
        for item in actual.get("cases", [])
        if item.get("status") == "rejected"
    }
    if actual_cases != expected_cases:
        raise AssertionError(
            f"adversarial rejections differ: actual={actual_cases!r} expected={expected_cases!r}"
        )
    print(
        "release bundle adversarial conformance passed: "
        f"{len(actual_cases)} rejected, 0 executed"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
