#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Reject omitted notices, stale dependency inventories, and license-policy drift."""

import copy
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts/release"))
import third_party_licenses as licenses
import release_bundle_qualification as bundle


class InventoryTests(unittest.TestCase):
    def test_reviewed_inventory_matches_checkout(self):
        value = licenses.check()
        self.assertTrue(any(p["name"] == "ring" for p in value["packages"]))
        self.assertTrue(any(p["name"] == "libsqlite3-sys" for p in value["packages"]))

    def test_and_obligations_are_not_dropped(self):
        self.assertEqual(licenses.choice("(Apache-2.0 OR MIT) AND BSD-3-Clause"), "MIT AND BSD-3-Clause")
        self.assertEqual(licenses.choice("Apache-2.0 AND ISC"), "Apache-2.0 AND ISC")
        with self.assertRaises(ValueError):
            licenses.choice("MIT OR Unreviewed-License")

    def test_development_edges_are_excluded_but_build_edges_followed(self):
        roots = [{"id": name, "name": name} for name in licenses.ROOT_PACKAGES]
        metadata = {"packages": roots, "workspace_members": [p["id"] for p in roots],
                    "resolve": {"nodes": [
                        {"id": p["id"], "deps": [
                            {"pkg": "runtime", "dep_kinds": [{"kind": None}]},
                            {"pkg": "builder", "dep_kinds": [{"kind": "build"}]},
                            {"pkg": "test-only", "dep_kinds": [{"kind": "dev"}]},
                        ]} for p in roots
                    ] + [{"id": name, "deps": []} for name in ("runtime", "builder", "test-only")]}}
        self.assertEqual(licenses.reachable(metadata), {"runtime", "builder"})

    def test_empty_duplicate_and_unlicensed_records_rejected(self):
        value = json.loads((ROOT / "licenses/inventory.json").read_text())
        candidates = []
        altered = copy.deepcopy(value); altered["packages"] = []; candidates.append(altered)
        altered = copy.deepcopy(value); altered["packages"].append(altered["packages"][0]); candidates.append(altered)
        altered = copy.deepcopy(value); altered["packages"][0]["notice_ids"] = []; candidates.append(altered)
        for candidate in candidates:
            with self.subTest(), self.assertRaises(ValueError):
                licenses.validate_inventory(candidate)

    def test_dependency_changes_and_notice_tampering_require_regeneration(self):
        value = json.loads((ROOT / "licenses/inventory.json").read_text())
        with tempfile.TemporaryDirectory(prefix="uc-license-inputs-") as directory:
            root = Path(directory)
            for relative in value["input_hashes"]:
                dest = root / relative; dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(ROOT / relative, dest)
            for name in licenses.OUTPUTS:
                shutil.copyfile(ROOT / "licenses" / name, root / "licenses" / name)
            licenses.check(root)
            for relative in ("Cargo.lock", "crates/undercurrent-cli/Cargo.toml",
                             "crates/undercurrent-cli/LICENSE",
                             "licenses/THIRD-PARTY-NOTICES.txt"):
                path = root / relative; original = path.read_bytes()
                path.write_bytes(original + b"\nchanged\n")
                with self.subTest(relative=relative), self.assertRaises(ValueError):
                    licenses.check(root)
                path.write_bytes(original)
            extra = root / "crates/new-dependency/Cargo.toml"
            extra.parent.mkdir(parents=True); extra.write_text("new manifest\n")
            with self.assertRaises(ValueError):
                licenses.check(root)


class BundleTests(unittest.TestCase):
    def tearDown(self):
        bundle.configure_platform("linux-x86_64-gnu")

    def test_every_platform_carries_the_complete_collection(self):
        with tempfile.TemporaryDirectory(prefix="uc-license-platforms-") as directory:
            for platform in bundle.PLATFORMS:
                with self.subTest(platform=platform):
                    bundle.configure_platform(platform)
                    root = Path(directory) / platform
                    bundle.synthetic_bundle(root)
                    self.assertEqual(bundle.verify(root)["status"], "passed")
                    for relative, source in bundle.DEPENDENCY_FILES.items():
                        self.assertEqual((root / relative).read_bytes(), (ROOT / "licenses" / source).read_bytes())

    def test_missing_notice_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix="uc-license-missing-") as directory:
            root = Path(directory) / "bundle"; bundle.synthetic_bundle(root)
            (root / bundle.dependency_license_metadata()["notices"]).unlink()
            with self.assertRaises(bundle.QualificationError):
                bundle.verify(root)

    def test_notices_survive_archive_unpack_and_install(self):
        with tempfile.TemporaryDirectory(prefix="uc-license-install-") as directory:
            base = Path(directory); source = base / "bundle"
            bundle.synthetic_bundle(source)
            archive = base / "bundle.tar"
            bundle.archive_bundle(source, archive)
            unpacked = base / "unpacked"
            bundle.unpack_bundle(archive, unpacked)
            installed = base / "installed"
            bundle.install_bundle(unpacked, installed, "Apache-2.0")
            for relative, original in bundle.DEPENDENCY_FILES.items():
                self.assertEqual((installed / relative).read_bytes(), (ROOT / "licenses" / original).read_bytes())

    def test_rewritten_checksums_cannot_hide_modified_notices(self):
        with tempfile.TemporaryDirectory(prefix="uc-license-tamper-") as directory:
            root = Path(directory) / "bundle"; bundle.synthetic_bundle(root)
            bundle.write_bytes(root / bundle.dependency_license_metadata()["notices"], b"Incomplete notices\n")
            bundle.rewrite_checksums(root)
            with self.assertRaises(bundle.QualificationError) as rejected:
                bundle.verify(root)
            self.assertEqual(rejected.exception.code, "dependency_license_invalid")


if __name__ == "__main__":
    unittest.main()
