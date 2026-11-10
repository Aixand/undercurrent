#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Focused positive and negative tests for the exact vendored-vt100 policy."""

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
    with tempfile.TemporaryDirectory(prefix="vt100-policy-") as temporary:
        root = Path(temporary)
        destination = root / "vendor" / "vt100"
        destination.parent.mkdir(parents=True)
        shutil.copytree(ROOT / "vendor" / "vt100", destination)
        mutator(destination)
        try:
            CONTRACT.validate_vendored_vt100(root)
        except Exception:
            return
        raise AssertionError("mutated vt100 vendor subtree was accepted")


def main() -> int:
    approved = CONTRACT.validate_vendored_vt100(ROOT)
    expected = {
        Path("vendor/vt100") / relative
        for relative in CONTRACT.VT100_VENDORED_FILES
        if relative.startswith("src/") and relative.endswith(".rs")
    }
    if approved != expected or len(approved) != 8:
        raise AssertionError("approved MIT source set is not the exact eight upstream files")

    expect_rejected(lambda root: (root / "LICENSE").write_text("tampered\n", encoding="utf-8"))

    def tamper_provenance(root: Path) -> None:
        path = root / "PROVENANCE.json"
        value = json.loads(path.read_text(encoding="utf-8"))
        value["metadata_overlay"]["sha256"] = "0" * 64
        path.write_text(json.dumps(value) + "\n", encoding="utf-8")

    expect_rejected(tamper_provenance)

    def tamper_tree_digest(root: Path) -> None:
        path = root / "PROVENANCE.json"
        value = json.loads(path.read_text(encoding="utf-8"))
        value["trees"]["vendored_candidate"]["sha256"] = "0" * 64
        path.write_text(json.dumps(value) + "\n", encoding="utf-8")

    expect_rejected(tamper_tree_digest)
    expect_rejected(
        lambda root: (root / "src" / "unapproved.rs").write_text(
            "// SPDX-License-Identifier: MIT\n", encoding="utf-8"
        )
    )
    expect_rejected(
        lambda root: (root / "patches" / "vt100-exact-candidate.patch").write_text(
            "tampered\n", encoding="utf-8"
        )
    )
    expect_rejected(
        lambda root: (
            root / "patches" / "vt100-local-vte-dependency.patch"
        ).write_text("tampered\n", encoding="utf-8")
    )

    def tamper_dev_vte_source(root: Path) -> None:
        path = root / "Cargo.toml"
        text = path.read_text(encoding="utf-8")
        marker = (
            '[dev-dependencies.vte]\nversion = "=0.11.1"\npath = "../vte"'
        )
        if marker not in text:
            raise AssertionError("exact generated vt100 dev vte dependency is missing")
        path.write_text(
            text.replace(marker, marker.replace("../vte", "../vte-copy"), 1),
            encoding="utf-8",
        )

    expect_rejected(tamper_dev_vte_source)
    print("vendored vt100 policy tests passed: 1 positive, 7 negative")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
