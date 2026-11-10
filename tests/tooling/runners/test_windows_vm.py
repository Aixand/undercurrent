# SPDX-License-Identifier: Apache-2.0
"""Verify that VM inputs include working-tree changes and preserve exact bytes."""

import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("windows_vm", ROOT / "scripts/ci/windows-vm.py")
VM = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VM)


class WindowsVmTests(unittest.TestCase):
    def test_snapshot_contains_exact_modified_and_untracked_source(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder) / "source"
            root.mkdir()

            def git(*args):
                subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)

            git("init", "--quiet")
            (root / ".gitignore").write_text("ignored.txt\n")
            (root / "edited.txt").write_bytes(b"committed\n")
            (root / "deleted.txt").write_bytes(b"removed\n")
            git("add", "--all")
            git("-c", "user.name=CI Test", "-c", "user.email=ci@localhost",
                "commit", "--quiet", "-m", "fixture")
            (root / "edited.txt").write_bytes(b"uncommitted\r\n")
            (root / "deleted.txt").unlink()
            (root / "untracked.txt").write_bytes(b"new\x00bytes")
            (root / "ignored.txt").write_bytes(b"excluded")
            archive = Path(folder) / "source.tar.gz"
            record = VM.snapshot(root, archive)
            with tarfile.open(archive) as snapshot:
                self.assertEqual(set(snapshot.getnames()), {
                    ".gitignore", "edited.txt", "untracked.txt", VM.SOURCE_RECORD,
                })
                self.assertEqual(snapshot.extractfile("edited.txt").read(), b"uncommitted\r\n")
                self.assertEqual(snapshot.extractfile("untracked.txt").read(), b"new\x00bytes")
                self.assertEqual(json.load(snapshot.extractfile(VM.SOURCE_RECORD)), record)
                for file in record["files"]:
                    self.assertEqual(hashlib.sha256(snapshot.extractfile(file["path"]).read()).hexdigest(),
                                     file["sha256"])
            (root / "edited.txt").write_bytes(b"another edit\n")
            changed = VM.snapshot(root, archive)
            self.assertNotEqual(changed["source_sha256"], record["source_sha256"])


if __name__ == "__main__":
    unittest.main()
