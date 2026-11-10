#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Exercise release rejection, artifact integrity, and publication recovery."""

import copy
import importlib.util
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
import zipfile

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("release_automation", ROOT / "scripts/release/automation.py")
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)
TAG = "v0.1.0-beta"
COMMIT = "a" * 40


def crate(commit=COMMIT, source=b"original", lock=b"lock"):
    stream = io.BytesIO()
    files = {".cargo_vcs_info.json": json.dumps({"git": {"sha1": commit}}).encode(),
             "src/lib.rs": source, "Cargo.lock": lock, "Cargo.toml": b"manifest"}
    with tarfile.open(fileobj=stream, mode="w:gz") as archive:
        for name, data in files.items():
            member = tarfile.TarInfo("example-0.1.0/" + name)
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
    return stream.getvalue()


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        for filename in ("Cargo.toml", "installation.toml", "undercurrent-package.json"):
            shutil.copyfile(ROOT / filename, self.root / filename)
        for name in release.CRATES:
            destination = self.root / "crates" / release.CRATE_DIRECTORIES[name]
            destination.mkdir(parents=True)
            shutil.copyfile(ROOT / "crates" / release.CRATE_DIRECTORIES[name] / "Cargo.toml", destination / "Cargo.toml")

    def test_versions_and_dependency_closure(self):
        self.assertEqual(release.validate_versions(TAG, self.root), "0.1.0-beta")
        manifest = self.root / "crates/undercurrent-sdk/Cargo.toml"
        manifest.write_text(manifest.read_text().replace('version = "=0.1.0-beta"', 'version = "=0.2.0"'))
        with self.assertRaisesRegex(ValueError, "dependency version"):
            release.validate_versions(TAG, self.root)

    def test_version_mismatch_and_invalid_tags(self):
        for tag in ("ci/windows-1", "v01.0.0", "v1.0.0-01", "v0.1.0-beta/extra", "v0.1.0\n"):
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                release.validate_versions(tag, self.root)
        with self.assertRaisesRegex(ValueError, "versions differ"):
            release.validate_versions("v0.2.0", self.root)

    def test_unpublished_target_dependency_rejected(self):
        manifest = self.root / "crates/undercurrent-transport-local/Cargo.toml"
        manifest.write_text(manifest.read_text().replace("../undercurrent-platform-windows", "../undercurrent-runtime")
                            .replace("undercurrent-platform-windows =", "undercurrent-runtime =")
                            .replace("aixand-undercurrent-platform-windows", "undercurrent-runtime"))
        with self.assertRaisesRegex(ValueError, "unpublished or unordered"):
            release.validate_versions(TAG, self.root)

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.root, stderr=subprocess.DEVNULL, text=True).strip()

    def test_git_tag_ancestry_and_clean_source(self):
        self.git("init", "-b", "main")
        self.git("config", "user.email", "release@example.invalid")
        self.git("config", "user.name", "Release test")
        self.git("config", "commit.gpgsign", "false")
        self.git("config", "tag.gpgsign", "false")
        self.git("config", "core.hooksPath", str(self.root / "no-hooks"))
        self.git("add", ".")
        self.git("commit", "-m", "fixture")
        self.git("tag", TAG)
        self.git("update-ref", "refs/remotes/origin/main", "HEAD")
        self.assertEqual(release.check(TAG, root=self.root)["commit"], self.git("rev-parse", "HEAD"))
        (self.root / "extra").write_text("dirty")
        with self.assertRaisesRegex(ValueError, "clean"):
            release.check(TAG, root=self.root)
        self.git("add", ".")
        self.git("commit", "-m", "not tagged")
        with self.assertRaisesRegex(ValueError, "Tag does not point"):
            release.check(TAG, root=self.root)
        self.git("tag", "-f", TAG)
        with self.assertRaises(subprocess.CalledProcessError):
            release.check(TAG, root=self.root)

    def test_complete_assets_required(self):
        for name in release.archive_names("0.1.0-beta"):
            (self.root / name).write_bytes(name.encode())
        # Keep only the download directory as input to the collector.
        assets = self.root / "assets"
        assets.mkdir()
        for name in release.archive_names("0.1.0-beta"):
            (self.root / name).rename(assets / name)
        missing = next(assets.iterdir())
        missing.unlink()
        with self.assertRaisesRegex(ValueError, "incomplete"):
            release.collect(TAG, assets)
        missing.write_bytes(b"download")
        release.collect(TAG, assets)
        self.assertEqual(len((assets / "SHA256SUMS").read_text().splitlines()), 8)

    def test_retry_checks_commit_and_source_bytes(self):
        release.verify_existing_crate(crate(), crate(lock=b"different registry lock"), COMMIT)
        with self.assertRaisesRegex(ValueError, "another commit"):
            release.verify_existing_crate(crate(), crate(commit="b" * 40), COMMIT)
        with self.assertRaisesRegex(ValueError, "content differs"):
            release.verify_existing_crate(crate(), crate(source=b"tampered"), COMMIT)

    def test_retry_publishes_only_remaining_crates(self):
        package = self.root / "target/package/tmp-crate"
        package.mkdir(parents=True)
        data = crate()
        (package / f"{release.CRATES[0]}-0.1.0-beta.crate").write_bytes(data)
        existing = {"yanked": False, "checksum": release.hashlib.sha256(data).hexdigest()}
        checked = {"version": "0.1.0-beta", "commit": COMMIT}
        with patch.object(release, "ROOT", self.root), \
             patch.object(release, "check", return_value=checked), \
             patch.dict(release.os.environ, {"CARGO_REGISTRY_TOKEN": "test-only"}), \
             patch.object(release, "registry_version", side_effect=[existing] + [None] * 7), \
             patch.object(release, "urlopen", return_value=io.BytesIO(data)), \
             patch.object(release.subprocess, "run") as command:
            release.crates(TAG, publish=True)
        calls = command.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertIn("--dry-run", calls[0].args[0])
        self.assertNotIn(release.CRATES[0], calls[1].args[0])
        self.assertIn(release.CRATES[-1], calls[1].args[0])
        self.assertNotIn("--dry-run", calls[1].args[0])

    def test_conflicting_crate_stops_before_any_new_upload(self):
        checked = {"version": "0.1.0-beta", "commit": COMMIT}
        with patch.object(release, "check", return_value=checked), \
             patch.dict(release.os.environ, {"CARGO_REGISTRY_TOKEN": "test-only"}), \
             patch.object(release, "registry_version", return_value={"yanked": True}), \
             patch.object(release.subprocess, "run") as command:
            with self.assertRaisesRegex(ValueError, "yanked"):
                release.crates(TAG, publish=True)
        self.assertEqual(command.call_count, 1)
        self.assertIn("--dry-run", command.call_args.args[0])

    def test_registry_outages_are_not_treated_as_missing_versions(self):
        for code in (403, 429, 503):
            with self.subTest(code=code), \
                 patch.object(release, "urlopen", side_effect=HTTPError("https://crates.io", code, "error", {}, None)), \
                 self.assertRaises(HTTPError):
                release.registry_version("example", "0.1.0")
        with patch.object(release, "urlopen", side_effect=HTTPError("https://crates.io", 404, "missing", {}, None)):
            self.assertIsNone(release.registry_version("example", "0.1.0"))

    def test_platform_archives_include_installer_and_evidence(self):
        for target in release.TARGETS:
            with self.subTest(target=target):
                dist = self.root / "dist"
                if dist.exists():
                    shutil.rmtree(dist)
                for folder in ("handoff/artifacts", "sdk", "bootstrap"):
                    (dist / folder).mkdir(parents=True)
                manifest = {"version": "0.1.0-beta", "target": target,
                            "sources": {"project": {"commit": COMMIT, "dirty": False}}}
                for path in (dist / "handoff/artifacts/release-manifest.json", dist / "sdk/release-manifest.json"):
                    path.write_text(json.dumps(manifest))
                digest = release.sha(dist / "sdk/release-manifest.json")
                proof = {"verified": True, "installation_tested": True, "target": target, "manifest_sha256": digest}
                (dist / "sdk-verification.json").write_text(json.dumps(proof))
                descriptor = {"version": "0.1.0-beta", "target": target, "manifest_sha256": digest,
                              "verification": {"runtime": True}}
                lifecycle = {"verified": True, "target": target, "manifest_sha256": digest,
                             "bootstrap_without_system_python": True, "upgrade_assembly_fixture_tested": True}
                (dist / "handoff/product-descriptor.json").write_text(json.dumps(descriptor))
                (dist / "lifecycle.json").write_text(json.dumps(lifecycle))
                (dist / "bootstrap/bootstrap.sh").write_text("installer")
                (dist / "sdk/sdk-file").write_text("sdk")
                (self.root / "licenses").mkdir(exist_ok=True)
                (self.root / "licenses/inventory.json").write_text("inventory")
                with patch.object(release, "run", return_value=COMMIT):
                    release.archive(TAG, self.root)
                self.assertEqual(len(list((dist / "assets").iterdir())), 2)
                for path in (dist / "assets").iterdir():
                    if path.suffix == ".zip":
                        with zipfile.ZipFile(path) as archive:
                            names = archive.namelist()
                    else:
                        with tarfile.open(path) as archive:
                            names = archive.getnames()
                    self.assertTrue(any(n.endswith("bootstrap/bootstrap.sh") for n in names))
                    self.assertTrue(any(n.endswith("release-metadata.json") for n in names))
                # An unrelated lifecycle report cannot qualify the payload.
                lifecycle["manifest_sha256"] = "another artifact"
                (dist / "lifecycle.json").write_text(json.dumps(lifecycle))
                with patch.object(release, "run", return_value=COMMIT), \
                     self.assertRaisesRegex(ValueError, "qualification"):
                    release.archive(TAG, self.root)

    def test_public_release_is_never_overwritten(self):
        assets = self.root / "assets"
        assets.mkdir()
        for name in release.archive_names("0.1.0-beta"):
            (assets / name).write_bytes(b"download")
        release.collect(TAG, assets)
        record = {"tag_name": TAG, "draft": False, "target_commitish": COMMIT}
        checked = {"version": "0.1.0-beta", "commit": COMMIT, "prerelease": True}
        with patch.object(release, "check", return_value=checked), \
             patch.dict(release.os.environ, {"GITHUB_REPOSITORY": "Aixand/undercurrent"}), \
             patch.object(release, "run", return_value=json.dumps([[record]])) as command:
            with self.assertRaisesRegex(ValueError, "already public"):
                release.github_release(TAG, assets)
            self.assertEqual(command.call_count, 1)
        altered = copy.deepcopy(record)
        altered.update(draft=True, target_commitish="b" * 40)
        with patch.object(release, "check", return_value=checked), \
             patch.dict(release.os.environ, {"GITHUB_REPOSITORY": "Aixand/undercurrent"}), \
             patch.object(release, "run", return_value=json.dumps([[altered]])):
            with self.assertRaisesRegex(ValueError, "another source"):
                release.github_release(TAG, assets)


if __name__ == "__main__":
    unittest.main()
