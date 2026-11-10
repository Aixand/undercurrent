#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Focused positive and negative tests for the exact vendored-vte policy."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import shutil
import tempfile


ROOT = Path(__file__).resolve().parents[3]
CONTRACT_PATH = ROOT / "scripts/checks/vendor_policy.py"
SPEC = importlib.util.spec_from_file_location("validation_contract", CONTRACT_PATH)
if SPEC is None or SPEC.loader is None:
    raise AssertionError("validation contract import failed")
CONTRACT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONTRACT)


def expect_rejected(mutator) -> None:
    with tempfile.TemporaryDirectory(prefix="vte-policy-") as temporary:
        root = Path(temporary)
        destination = root / "vendor" / "vte"
        destination.parent.mkdir(parents=True)
        shutil.copytree(ROOT / "vendor" / "vte", destination)
        mutator(destination)
        try:
            CONTRACT.validate_vendored_vte(root)
        except Exception:
            return
        raise AssertionError("mutated vte vendor subtree was accepted")


def main() -> int:
    approved, unsafe_sites = CONTRACT.validate_vendored_vte(ROOT)
    expected = {
        Path("vendor/vte") / relative
        for relative in CONTRACT.VTE_VENDORED_FILES
        if relative.endswith(".rs")
    }
    if approved != expected or len(approved) != 6:
        raise AssertionError("approved dual-license source set is not exact")
    if unsafe_sites != {
        (Path("vendor/vte/src/definitions.rs"), 55),
        (Path("vendor/vte/src/lib.rs"), 317),
        (Path("vendor/vte/src/lib.rs"), 324),
    }:
        raise AssertionError("approved unsafe site set is not exact")

    expect_rejected(
        lambda root: (root / "LICENSE-APACHE").write_text(
            "tampered\n", encoding="utf-8"
        )
    )
    expect_rejected(
        lambda root: (root / "LICENSE-MIT").write_text(
            "tampered\n", encoding="utf-8"
        )
    )

    def tamper_provenance(root: Path) -> None:
        path = root / "PROVENANCE.json"
        value = json.loads(path.read_text(encoding="utf-8"))
        value["patch"]["sha256"] = "0" * 64
        path.write_text(json.dumps(value) + "\n", encoding="utf-8")

    expect_rejected(tamper_provenance)

    def tamper_tree_digest(root: Path) -> None:
        path = root / "PROVENANCE.json"
        value = json.loads(path.read_text(encoding="utf-8"))
        value["trees"]["vendored_candidate"]["sha256"] = "0" * 64
        path.write_text(json.dumps(value) + "\n", encoding="utf-8")

    expect_rejected(tamper_tree_digest)

    def tamper_unsafe_audit(root: Path) -> None:
        path = root / "UNSAFE-AUDIT.json"
        value = json.loads(path.read_text(encoding="utf-8"))
        value["sites"][0]["invariant"] = "tampered"
        path.write_text(json.dumps(value) + "\n", encoding="utf-8")

    expect_rejected(tamper_unsafe_audit)
    expect_rejected(
        lambda root: (
            root / "patches" / "vte-ground-ordered-noop.patch"
        ).write_text("tampered\n", encoding="utf-8")
    )
    expect_rejected(
        lambda root: (root / "src" / "unapproved.rs").write_text(
            "unsafe fn unapproved() {}\n", encoding="utf-8"
        )
    )
    print("vendored vte policy tests passed: 1 positive, 7 negative")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
