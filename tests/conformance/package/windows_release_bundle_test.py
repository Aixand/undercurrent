#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Native Windows ACL, path-kind and PE rejection tests; never executes fixtures."""
import ctypes as c
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import tarfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts/release"))
import release_bundle_qualification as bundle


class HeaderTests(unittest.TestCase):
    def setUp(self):
        bundle.configure_platform("windows-x86_64-msvc")

    def test_native_header_and_rejections(self):
        valid = bundle.synthetic_header()
        bundle.validate_binary_header(valid, "uc.exe")
        bad = [b"MZ", b"\x7fELF", valid[:87], valid[:200]]
        for offset, fmt, value in [(60, "I", 0xffffffff), (68, "H", 0xaa64),
                                   (70, "H", 0), (84, "H", 65535), (86, "H", 0x2002),
                                   (86, "H", 0), (88, "H", 0x10b), (156, "H", 2)]:
            header = bytearray(valid)
            struct.pack_into("<" + fmt, header, offset, value)
            bad.append(bytes(header))
        for header in bad:
            with self.subTest(header=header[:90]), self.assertRaises(bundle.QualificationError):
                bundle.validate_binary_header(header, "uc.exe")


@unittest.skipUnless(os.name == "nt", "native Windows ACLs required")
class SecurityTests(unittest.TestCase):
    def setUp(self):
        import windows_bundle_security as security
        self.security = security
        self.temp = tempfile.TemporaryDirectory(prefix="uc-bundle-security-")
        self.root = Path(self.temp.name)
        bundle.configure_platform("windows-x86_64-msvc")

    def tearDown(self):
        self.temp.cleanup()

    def test_conpty_runtime_is_pinned_even_when_checksums_are_rewritten(self):
        source = self.root / "bundle"
        bundle.synthetic_bundle(source)
        library = source / "bin/uc-conpty/conpty.dll"
        original = library.read_bytes()
        bundle.write_bytes(library, original + b"tampered")
        checksums = source / bundle.CHECKSUM_FILE
        lines = checksums.read_text(encoding="ascii").splitlines()
        bundle.write_bytes(checksums, ("\n".join(
            f"{bundle.digest_file(library)}  bin/uc-conpty/conpty.dll" if line.endswith("  bin/uc-conpty/conpty.dll") else line
            for line in lines) + "\n").encode("ascii"))
        with self.assertRaises(bundle.QualificationError) as rejected:
            bundle.verify(source)
        self.assertEqual(rejected.exception.code, "conpty_runtime_invalid")

    def test_missing_conpty_runtime_is_rejected(self):
        source = self.root / "bundle"
        bundle.synthetic_bundle(source)
        (source / "bin/uc-conpty/OpenConsole.exe").unlink()
        with self.assertRaises(bundle.QualificationError):
            bundle.verify(source)

    def test_private_round_trip_and_broad_acl_rejected_without_repair(self):
        s = self.security
        directory = self.root / "private"
        s.create_private_directory(directory)
        leaf = directory / "payload"
        with s.private_writer(leaf) as writer:
            writer.write(b"payload")
        s.restrict_private(leaf)
        s.verify_private(leaf)
        descriptor = s.P()
        s.check(s.ConvertDescriptor("D:P(A;;FA;;;WD)", 1, c.byref(descriptor), None))
        try:
            dacl, present, defaulted = s.P(), s.w.BOOL(), s.w.BOOL()
            s.check(s.GetDacl(descriptor, c.byref(present), c.byref(dacl), c.byref(defaulted)))
            with s.opened(leaf, True) as handle:
                self.assertEqual(s.SetSecurityInfo(handle, 1, 0x80000004, None, None, dacl, None), 0)
            for _ in range(2):
                with self.assertRaises(PermissionError):
                    s.verify_private(leaf)
            self.assertEqual(leaf.read_bytes(), b"payload")
        finally:
            s.LocalFree(descriptor)

    def test_hardlinks_and_junctions_are_rejected_before_acl_changes(self):
        s = self.security
        target = self.root / "target"
        s.create_private_directory(target)
        leaf = target / "payload"
        with s.private_writer(leaf) as writer:
            writer.write(b"payload")
        s.restrict_private(leaf)
        alias = target / "alias"
        os.link(leaf, alias)
        with self.assertRaises(PermissionError):
            s.restrict_private(alias)
        alias.unlink()
        s.verify_private(leaf)
        junction = self.root / "junction"
        subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-Command",
                        "$ErrorActionPreference='Stop'; $null=New-Item -ItemType Junction -Path $env:UC_LINK -Target $env:UC_TARGET"],
                       env={**os.environ, "UC_LINK": str(junction), "UC_TARGET": str(target)},
                       check=True, capture_output=True, timeout=20)
        try:
            with self.assertRaises(PermissionError):
                s.restrict_private(junction)
            with self.assertRaises(bundle.QualificationError) as rejected:
                bundle.relative_files(junction)
            self.assertEqual(rejected.exception.code, "bundle_not_directory")
            s.verify_private(target)
        finally:
            junction.rmdir()

    def test_private_writer_owns_nested_output_and_rejects_links_before_truncation(self):
        s = self.security
        leaf = self.root / "nested" / "child" / "payload"
        with s.private_writer(leaf) as writer:
            writer.write(b"original payload")
        for path in (leaf.parent.parent, leaf.parent, leaf):
            s.verify_private(path)
        with self.assertRaises(FileExistsError):
            with s.private_writer(leaf):
                self.fail("exclusive creation overwrote a file")
        alias = self.root / "outside-link"
        os.link(leaf, alias)
        with self.assertRaises(PermissionError):
            with s.private_writer(leaf, overwrite=True):
                self.fail("hardlink accepted")
        self.assertEqual(alias.read_bytes(), b"original payload")
        alias.unlink()
        with s.private_writer(leaf, overwrite=True) as writer:
            writer.write(b"new")
        self.assertEqual(leaf.read_bytes(), b"new")
        s.verify_private(leaf)

    def test_verified_copy_archive_and_wrong_platform(self):
        source, installed = self.root / "bundle", self.root / "installed"
        bundle.synthetic_bundle(source)
        bundle.verify(source)
        bundle.install_bundle(source, installed, "Apache-2.0")
        first, second = self.root / "first.tar", self.root / "second.tar"
        bundle.archive_bundle(installed, first)
        bundle.archive_bundle(installed, second)
        self.assertEqual(bundle.digest_file(first), bundle.digest_file(second))
        unpacked = self.root / "unpacked"
        bundle.unpack_bundle(first, unpacked)
        bundle.verify(unpacked)
        root_name = f"undercurrent-{bundle.VERSION}-{bundle.PLATFORM}"
        allowed = root_name + "/bin/uc.exe"
        for name, kind, size in (("../escaped", tarfile.REGTYPE, 0),
                                 (allowed, tarfile.SYMTYPE, 0),
                                 (allowed, tarfile.LNKTYPE, 0),
                                 (allowed, tarfile.REGTYPE, 513 * 1024 * 1024)):
            malicious = self.root / "malicious.tar"
            with tarfile.open(malicious, "w") as archive:
                entry = tarfile.TarInfo(name)
                entry.type = kind
                entry.linkname = "../escaped"
                entry.size = size
                if size:
                    # Deliberately truncated oversized header: rejection must
                    # happen before attempting to allocate or read its payload.
                    archive.fileobj.write(entry.tobuf())
                else:
                    archive.addfile(entry)
            rejected_output = self.root / "rejected"
            with self.assertRaises(bundle.QualificationError):
                bundle.unpack_bundle(malicious, rejected_output)
            self.assertFalse(rejected_output.exists())
            self.assertFalse((self.root / "escaped").exists())
        duplicate = self.root / "duplicate.tar"
        with tarfile.open(duplicate, "w") as archive:
            entry = tarfile.TarInfo(root_name)
            entry.type = tarfile.DIRTYPE
            archive.addfile(entry)
            archive.addfile(entry)
        with self.assertRaises(bundle.QualificationError):
            bundle.unpack_bundle(duplicate, self.root / "duplicate-output")
        bundle.configure_platform("macos-arm64")
        with self.assertRaises(bundle.QualificationError):
            bundle.verify(installed)


if __name__ == "__main__":
    unittest.main()
