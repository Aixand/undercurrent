#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Deterministic local release-bundle assembly and fail-closed verification."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import sys
import tarfile
import tempfile
from typing import Any

import third_party_licenses


BUNDLE_SCHEMA = "undercurrent.release-bundle/v0.1.0-beta"
PROVENANCE_SCHEMA = "undercurrent.local-provenance/v0.1.0-beta"
LICENSE_SCHEMA = "undercurrent.license-metadata/v0.1.0-beta"
ADVERSARIAL_SCHEMA = "undercurrent.release-bundle-adversarial/v0.1.0-beta"
COORDINATE = "undercurrent"
VERSION = "0.1.0-beta"
PLATFORM = "linux-x86_64-gnu"
PLATFORMS = (PLATFORM, "macos-arm64", "macos-x86_64", "windows-x86_64-msvc")
SHARE = "share/undercurrent"
DEPENDENCY_FILES = {
    f"{SHARE}/third-party/{name}": name for name in third_party_licenses.OUTPUTS
}
FOUNDATION_NOTICE_FILE = f"{SHARE}/NOTICES/undercurrent-foundation.txt"
FOUNDATION_NOTICE_SHA256 = "b2e4db7024801b8fadddf19fa3f10adaa3a142a4cd616826ed97b855d2f140d4"
VT100_LICENSE_FILE = f"{SHARE}/THIRD-PARTY-LICENSES/vt100-0.15.2-MIT.txt"
VT100_PROVENANCE_FILE = f"{SHARE}/third-party/vt100-0.15.2.provenance.json"
VT100_LICENSE_SHA256 = "53a04b0a11073f3d0183c31a4b7bdf9a3f68d453adeec136e59e5a9d9a554370"
VT100_PROVENANCE_SHA256 = "e7ffbfe85613d62787de2b0b52cff8cfb13d9dfaa50872fd6ccd48ba08b6cb29"
VT100_PATCH_SHA256 = "fd643c8c461990393895f69ed01f4a7d22b785f5a5e3619dc4f5ba615a2f13db"
VT100_METADATA_OVERLAY_SHA256 = (
    "6e555493a804daa4ae9746ac4d38d060115c14d15fe51e9d2295c6d5b6d9129e"
)
VTE_APACHE_LICENSE_FILE = f"{SHARE}/THIRD-PARTY-LICENSES/vte-0.11.1-Apache-2.0.txt"
VTE_MIT_LICENSE_FILE = f"{SHARE}/THIRD-PARTY-LICENSES/vte-0.11.1-MIT.txt"
VTE_PROVENANCE_FILE = f"{SHARE}/third-party/vte-0.11.1.provenance.json"
VTE_UNSAFE_AUDIT_FILE = f"{SHARE}/third-party/vte-0.11.1.unsafe-audit.json"
VTE_APACHE_LICENSE_SHA256 = "62c7a1e35f56406896d7aa7ca52d0cc0d272ac022b5d2796e7d6905db8a3636a"
VTE_MIT_LICENSE_SHA256 = "e4c9b06fa850cb9b540a5e400e9f6394cf15efcf4098144de477d1d3dae10150"
VTE_PROVENANCE_SHA256 = "0bc6e0dccb03dcb95027d788e98451017c8f6602671ab5dc9c46333eabf0ad63"
VTE_UNSAFE_AUDIT_SHA256 = "42a0e41e60242b706618127210a2af3005a0a7e60859e0713b158aa7b9997609"
VTE_PATCH_SHA256 = "3c5c222d1329464cad3f11d0f61f2b8fad7fd6351a6877580b3879b0d8681574"
PTY_LICENSE_FILE = f"{SHARE}/THIRD-PARTY-LICENSES/portable-pty-0.9.0-MIT.txt"
PTY_PROVENANCE_FILE = f"{SHARE}/third-party/portable-pty-0.9.0.provenance.json"
PTY_LICENSE_SHA256 = "89d78d84ebdf7c396981857e3cff221be2f93e7df2d1a26e5c4cd8ff059694cf"
PTY_PROVENANCE_SHA256 = "ae105522e62142547b5fa637f02a9940973abd17c5ad01e96e3b325e2ceb6705"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONPTY_ROOT = PROJECT_ROOT / "vendor/portable-pty/conpty"
CONPTY_PROVENANCE_FILE = f"{SHARE}/third-party/conpty.provenance.json"
CONPTY_PROVENANCE_SHA256 = "9df8dad1a3ed98714bc4173f31c51f984388aa2a68a568eec034a8ce12114684"
CONPTY_FILES = {
    "bin/uc-conpty/conpty.dll": "feeef341d891643c62d30b6b07800bc70f0bc148f44cb8c3bee84aa557ae805a",
    "bin/uc-conpty/OpenConsole.exe": "3d66b23d0a71bb8eed2b77edc8b9df9bf54ce6c8fb74c863a30e760997f80586",
    "bin/uc-conpty/LICENSE": "5d177f23ecfeb0ea8e050b6a5a16355e1ae9a0b286436ca8f83ed08b3795be6b",
}


def conpty_license_metadata() -> list[dict[str, str]]:
    return [{"name": "Microsoft.Windows.Console.ConPTY", "version": "1.25.260930003",
             "spdx": "MIT", "text": "bin/uc-conpty/LICENSE", "provenance": CONPTY_PROVENANCE_FILE}] if PLATFORM.startswith("windows-") else []


def copy_conpty_runtime(output: Path, artifact_dir: Path | None = None) -> None:
    if not PLATFORM.startswith("windows-"):
        return
    for relative, expected in CONPTY_FILES.items():
        name = PurePosixPath(relative).name
        source = (artifact_dir / "uc-conpty" / name) if artifact_dir else (
            CONPTY_ROOT / name if name == "LICENSE" else CONPTY_ROOT / "x64" / name)
        try:
            actual = digest_file(source)
        except OSError:
            fail("source_artifact_missing", f"required ConPTY runtime file is missing: {name}")
        if actual != expected:
            fail("conpty_runtime_invalid", f"ConPTY runtime file differs from the pinned Microsoft package: {name}")
        copy_regular(source, output / relative, 0o700 if relative in EXECUTABLE_FILES else 0o600)
    copy_regular(CONPTY_ROOT / "PROVENANCE.json", output / CONPTY_PROVENANCE_FILE, 0o600)

ARTIFACTS = (
    ("cli", "uc", "bin/uc"),
    ("daemon", "uc-daemon", "bin/uc-daemon"),
    ("session_host", "uc-session-host", "bin/uc-session-host"),
)
METADATA_FILES = (
    *DEPENDENCY_FILES,
    PTY_LICENSE_FILE,
    PTY_PROVENANCE_FILE,
    f"{SHARE}/undercurrent-package.json",
    f"{SHARE}/undercurrent-package.schema.json",
    f"{SHARE}/config.schema.json",
    f"{SHARE}/config.example.toml",
    f"{SHARE}/LICENSE",
    FOUNDATION_NOTICE_FILE,
    VT100_LICENSE_FILE,
    VTE_APACHE_LICENSE_FILE,
    VTE_MIT_LICENSE_FILE,
    f"{SHARE}/INSTALL.md",
    f"{SHARE}/license.json",
    f"{SHARE}/provenance.json",
    VT100_PROVENANCE_FILE,
    VTE_PROVENANCE_FILE,
    VTE_UNSAFE_AUDIT_FILE,
)
BUNDLE_MANIFEST = f"{SHARE}/bundle-manifest.json"
CHECKSUM_FILE = "SHA256SUMS"
EXPECTED_FILES = frozenset(
    [path for _role, _name, path in ARTIFACTS]
    + list(METADATA_FILES)
    + [BUNDLE_MANIFEST, CHECKSUM_FILE]
)
EXECUTABLE_FILES = frozenset(path for _role, _name, path in ARTIFACTS)
RETIRED_BINARIES = frozenset(
    {
        "uc-gateway",
        "uc-local",
        "uc-server",
        "undercurrent-gateway",
        "undercurrent-local",
        "undercurrent-server",
        "undercurrent-observability",
    }
)
IMPLEMENTATION_LIBRARY_SUFFIXES = (
    ".a",
    ".dylib",
    ".dll",
    ".lib",
    ".rlib",
    ".rmeta",
    ".so",
)
SOURCE_COMPONENTS = frozenset({".git", "crates", "src", "target"})
SOURCE_FILENAMES = frozenset({"Cargo.lock", "Cargo.toml", "build.rs"})
SECRET_PATH_PARTS = re.compile(
    r"(^|[._-])(credential|private[._-]?key|secret|token)([._-]|$)", re.IGNORECASE
)
PRIVATE_KEY_BLOCK = re.compile(
    rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"
    rb"[\r\n\t ]+[A-Za-z0-9+/=\r\n\t ]{32,}"
    rb"-----END (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"
)


class QualificationError(Exception):
    """Stable fail-closed bundle verification failure."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def configure_platform(platform: str) -> None:
    global PLATFORM, ARTIFACTS, EXPECTED_FILES, EXECUTABLE_FILES
    PLATFORM = platform
    suffix = ".exe" if platform.startswith("windows-") else ""
    ARTIFACTS = tuple((role, name, f"bin/{name}{suffix}") for role, name, _ in ARTIFACTS)
    EXECUTABLE_FILES = frozenset(path for _, _, path in ARTIFACTS)
    EXPECTED_FILES = frozenset(EXECUTABLE_FILES | set(METADATA_FILES) | {BUNDLE_MANIFEST, CHECKSUM_FILE})
    if PLATFORM.startswith("windows-"):
        EXECUTABLE_FILES |= {"bin/uc-conpty/OpenConsole.exe"}
        EXPECTED_FILES |= set(CONPTY_FILES) | {CONPTY_PROVENANCE_FILE}


def set_private(path: Path, mode: int) -> None:
    if os.name == "nt":
        from windows_bundle_security import restrict_private
        restrict_private(path)
    else:
        path.chmod(mode)


def dependency_license_metadata() -> dict[str, str]:
    return {
        "inventory": f"{SHARE}/third-party/inventory.json",
        "notices": f"{SHARE}/third-party/THIRD-PARTY-NOTICES.txt",
        "rust_standard_library": f"{SHARE}/third-party/RUST-STDLIB.html",
    }


def copy_dependency_notices(output: Path) -> None:
    try:
        third_party_licenses.check(PROJECT_ROOT)
    except (OSError, ValueError, KeyError, TypeError) as error:
        fail("dependency_license_invalid", f"dependency license collection requires review: {error}")
    for relative, source in DEPENDENCY_FILES.items():
        copy_regular(PROJECT_ROOT / "licenses" / source, output / relative, 0o600)


def dependency_license_material(root: Path) -> dict[str, str]:
    return {
        "coordinate": "undercurrent#dependency-licenses",
        "kind": "dependency_license_inventory",
        "sha256": digest_file(root / dependency_license_metadata()["inventory"]),
    }


def create_bundle_root(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if os.name == "nt":
        from windows_bundle_security import create_private_directory
        create_private_directory(path)
    else:
        path.mkdir(mode=0o700)


def check_private(path: Path, item: os.stat_result) -> None:
    if os.name == "nt":
        from windows_bundle_security import verify_private
        try:
            verify_private(path)
        except OSError:
            fail("owner_only_violation", "bundle requires current-user ownership and a protected owner-only ACL")
    elif item.st_mode & 0o077:
        fail("owner_only_violation", "bundle grants group or other access")


def is_reparse(path: Path) -> bool:
    return path.is_symlink() or bool(getattr(path.lstat(), "st_file_attributes", 0) & 0x400)


def fail(code: str, message: str) -> None:
    raise QualificationError(code, message)


def digest_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_bytes(path: Path, content: bytes, mode: int = 0o600) -> None:
    with payload_writer(path, overwrite=True) as writer:
        writer.write(content)
    set_private(path, mode)


def payload_writer(path: Path, overwrite: bool = False):
    if os.name == "nt":
        from windows_bundle_security import private_writer
        return private_writer(path, overwrite)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    return path.open("wb" if overwrite else "xb")


def write_json(path: Path, value: Any) -> None:
    write_bytes(
        path,
        (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8"),
    )


def normalize_bundle_modes(root: Path) -> None:
    """Apply the exact owner-only bundle modes before integrity calculation."""
    if is_reparse(root):
        fail("non_regular_entry", "cannot restrict a bundle reparse point")
    directories, files = [], []
    for base, names, leaves in os.walk(root, followlinks=False):
        for name in names + leaves:
            path = Path(base) / name
            if is_reparse(path) or (path.is_file() and path.stat().st_nlink != 1):
                fail("non_regular_entry", "cannot restrict bundle links")
            (directories if name in names else files).append(path)
    # Protect descendants before changing parents: Windows removes inherited
    # ACEs when the parent DACL stops supplying them. No existing child can lose
    # access during this transition because its own protected ACL is set first.
    for path in files:
        relative = path.relative_to(root).as_posix()
        set_private(path, 0o700 if relative in EXECUTABLE_FILES else 0o600)
    for path in sorted(directories, key=lambda p: len(p.parts), reverse=True):
        set_private(path, 0o700)
    set_private(root, 0o700)


def load_json(path: Path, code: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        fail(code, f"{path.name} is not valid UTF-8 JSON: {type(error).__name__}")


def require_object(value: Any, code: str, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(code, f"{name} must be a JSON object")
    return value


def copy_regular(source: Path, destination: Path, mode: int) -> None:
    try:
        source_stat = source.lstat()
    except OSError as error:
        fail("source_artifact_missing", f"required source artifact is unavailable: {error.filename}")
    if not stat.S_ISREG(source_stat.st_mode) or is_reparse(source):
        fail("source_artifact_not_regular", f"source artifact must be a regular file: {source.name}")
    with source.open("rb") as reader, payload_writer(destination) as writer:
        shutil.copyfileobj(reader, writer, 1024 * 1024)
    set_private(destination, mode)


def validate_package_contract(package: dict[str, Any]) -> None:
    expected = {
        "schema_version": "undercurrent.package/v0.1.0-beta",
        "coordinate": COORDINATE,
        "version": VERSION,
        "kind": "standalone_product",
    }
    for field, required in expected.items():
        if package.get(field) != required:
            fail("package_contract_mismatch", f"package field {field} must equal {required}")
    runtime = require_object(package.get("runtime"), "package_contract_mismatch", "runtime")
    if runtime.get("model") != "external_daemon":
        fail("package_contract_mismatch", "runtime model must be external_daemon")
    if runtime.get("semantic_transport") != "local_ipc":
        fail("package_contract_mismatch", "semantic transport must be local_ipc")
    if runtime.get("implicit_daemon_launch") is not True:
        fail("package_contract_mismatch", "CLI implicit daemon launch must be true")
    installation = require_object(
        package.get("installation"), "package_contract_mismatch", "installation"
    )
    for field, required in {
        "ordinary_mode": "verified_release_artifact",
        "monorepo_clone": False,
        "implicit_source_build": False,
        "verify_before_execution": True,
    }.items():
        if installation.get(field) != required:
            fail("package_contract_mismatch", f"installation field {field} is invalid")
    declared = package.get("artifacts")
    if not isinstance(declared, list) or len(declared) != len(ARTIFACTS):
        fail("package_contract_mismatch", "package must declare exactly three artifacts")
    actual_roles = {
        (item.get("role"), item.get("name"))
        for item in declared
        if isinstance(item, dict)
    }
    if PLATFORM != "linux-x86_64-gnu":
        if PLATFORM not in {entry.get("id") for entry in package.get("platforms", [])} or any(PLATFORM not in entry.get("platforms", []) for entry in declared):
            fail("package_contract_mismatch", "package does not declare the requested platform")
    expected_roles = {(role, name) for role, name, _path in ARTIFACTS}
    if actual_roles != expected_roles:
        fail("package_contract_mismatch", "package artifact roles do not match 0.1.0-beta")


def validate_binary_header(header: bytes, name: str) -> None:
    if PLATFORM == "linux-x86_64-gnu":
        if header[:4] != b"\x7fELF":
            fail("source_artifact_not_elf", f"{name} is not a Linux ELF artifact")
    elif PLATFORM == "windows-x86_64-msvc":
        import struct
        valid = False
        if len(header) >= 64 and header[:2] == b"MZ":
            pe = struct.unpack_from("<I", header, 60)[0]
            if 64 <= pe <= len(header) - 24 and header[pe:pe + 4] == b"PE\0\0":
                machine, sections = struct.unpack_from("<HH", header, pe + 4)
                optional_size, flags = struct.unpack_from("<HH", header, pe + 20)
                optional = pe + 24
                if 112 <= optional_size and optional + optional_size + 40 * sections <= len(header):
                    magic = struct.unpack_from("<H", header, optional)[0]
                    subsystem = struct.unpack_from("<H", header, optional + 68)[0]
                    valid = (machine == 0x8664 and 1 <= sections <= 96 and magic == 0x20b
                             and flags & 2 and not flags & 0x2000 and subsystem == 3)
        if not valid:
            fail("source_artifact_wrong_platform", f"{name} is not an AMD64 PE32+ console executable")
    else:
        import struct
        cpu = {"macos-arm64": 0x0100000c, "macos-x86_64": 0x01000007}[PLATFORM]
        if len(header) < 16 or header[:4] != b"\xcf\xfa\xed\xfe" or struct.unpack_from("<I", header, 4)[0] != cpu or struct.unpack_from("<I", header, 12)[0] != 2:
            fail("source_artifact_wrong_platform", f"{name} is not a native executable for {PLATFORM}")


def assemble(args: argparse.Namespace) -> dict[str, Any]:
    output = Path(args.output)
    artifact_dir = Path(args.artifact_dir)
    if output.exists() or output.is_symlink():
        fail("output_exists", "bundle output must not already exist")
    create_bundle_root(output)

    sources = {
        path: artifact_dir / PurePosixPath(path).name for _role, name, path in ARTIFACTS
    }
    for relative, source in sources.items():
        try:
            with source.open("rb") as handle:
                validate_binary_header(handle.read(65536), source.name)
        except OSError:
            fail("source_artifact_missing", f"required release artifact is missing: {source.name}")
        copy_regular(source, output / relative, 0o700)
    copy_conpty_runtime(output, artifact_dir)

    package_source = Path(args.package_manifest)
    package = require_object(
        load_json(package_source, "package_manifest_invalid"),
        "package_manifest_invalid",
        "package manifest",
    )
    validate_package_contract(package)
    copy_regular(package_source, output / f"{SHARE}/undercurrent-package.json", 0o600)
    copy_regular(
        Path(args.package_schema), output / f"{SHARE}/undercurrent-package.schema.json", 0o600
    )
    copy_regular(Path(args.config_schema), output / f"{SHARE}/config.schema.json", 0o600)
    copy_regular(Path(args.license), output / f"{SHARE}/LICENSE", 0o600)
    copy_regular(Path(args.foundation_notice), output / FOUNDATION_NOTICE_FILE, 0o600)
    copy_regular(Path(args.vt100_license), output / VT100_LICENSE_FILE, 0o600)
    copy_regular(Path(args.vt100_provenance), output / VT100_PROVENANCE_FILE, 0o600)
    copy_regular(Path(args.vte_apache_license), output / VTE_APACHE_LICENSE_FILE, 0o600)
    copy_regular(Path(args.vte_mit_license), output / VTE_MIT_LICENSE_FILE, 0o600)
    copy_regular(Path(args.vte_provenance), output / VTE_PROVENANCE_FILE, 0o600)
    copy_regular(Path(args.vte_unsafe_audit), output / VTE_UNSAFE_AUDIT_FILE, 0o600)
    copy_regular(Path(args.install_document), output / f"{SHARE}/INSTALL.md", 0o600)
    write_bytes(
        output / f"{SHARE}/config.example.toml",
        b'schema_version = "0.1.0-beta"\n\n[local]\n# endpoint defaults to <product-home>/run/control.sock\n',
    )

    copy_dependency_notices(output)
    copy_regular(PROJECT_ROOT / "vendor/portable-pty/LICENSE.md", output / PTY_LICENSE_FILE, 0o600)
    copy_regular(PROJECT_ROOT / "vendor/portable-pty/PROVENANCE.json", output / PTY_PROVENANCE_FILE, 0o600)

    subjects = [
        {
            "name": name,
            "role": role,
            "sha256": digest_file(output / path),
        }
        for role, name, path in ARTIFACTS
    ]
    write_json(
        output / f"{SHARE}/license.json",
        {
            "schema": LICENSE_SCHEMA,
            "coordinate": COORDINATE,
            "package_version": VERSION,
            "spdx": "Apache-2.0",
            "text": f"{SHARE}/LICENSE",
            "notices": [FOUNDATION_NOTICE_FILE],
            "acceptance_required_before_installation": True,
            "dependency_collection": dependency_license_metadata(),
            "third_party": [
            *conpty_license_metadata(),
            {"name": "portable-pty", "version": "0.9.0", "spdx": "MIT", "text": PTY_LICENSE_FILE, "provenance": PTY_PROVENANCE_FILE},
                {
                    "name": "vt100",
                    "version": "0.15.2",
                    "spdx": "MIT",
                    "text": VT100_LICENSE_FILE,
                    "provenance": VT100_PROVENANCE_FILE,
                },
                {
                    "name": "vte",
                    "version": "0.11.1",
                    "spdx": "Apache-2.0 OR MIT",
                    "texts": [VTE_APACHE_LICENSE_FILE, VTE_MIT_LICENSE_FILE],
                    "provenance": VTE_PROVENANCE_FILE,
                    "unsafe_audit": VTE_UNSAFE_AUDIT_FILE,
                },
            ],
        },
    )
    write_json(
        output / f"{SHARE}/provenance.json",
        {
            "schema": PROVENANCE_SCHEMA,
            "coordinate": COORDINATE,
            "package_version": VERSION,
            "platform": PLATFORM,
            "purpose": "local_release_qualification",
            "builder": {
                "id": "undercurrent-local-release-qualification",
                "external_network_used": False,
                "source_build_during_installation": False,
            },
            "materials": [
                {
                    "coordinate": COORDINATE,
                    "kind": "package_manifest",
                    "sha256": digest_file(output / f"{SHARE}/undercurrent-package.json"),
                },
                {
                    "coordinate": "crates.io/vt100@0.15.2",
                    "kind": "vendored_dependency",
                    "sha256": digest_file(output / VT100_PROVENANCE_FILE),
                },
                {
                    "coordinate": "crates.io/vte@0.11.1",
                    "kind": "vendored_dependency",
                    "sha256": digest_file(output / VTE_PROVENANCE_FILE),
                },
                {
                    "coordinate": "crates.io/vte@0.11.1#unsafe-audit",
                    "kind": "vendored_unsafe_audit",
                    "sha256": digest_file(output / VTE_UNSAFE_AUDIT_FILE),
                },
                {
                    "coordinate": "undercurrent-foundation@0.1.0-beta#attribution",
                    "kind": "source_attribution",
                    "sha256": digest_file(output / FOUNDATION_NOTICE_FILE),
                },
                {
                    "coordinate": "crates.io/portable-pty@0.9.0",
                    "kind": "vendored_dependency",
                    "sha256": digest_file(output / PTY_PROVENANCE_FILE),
                },
                dependency_license_material(output),
            ],
            "subjects": subjects,
            "publication_performed": False,
            "deployment_performed": False,
        },
    )
    write_json(
        output / BUNDLE_MANIFEST,
        {
            "schema": BUNDLE_SCHEMA,
            "coordinate": COORDINATE,
            "package_version": VERSION,
            "platform": PLATFORM,
            "layout": "operator_selected_prefix/0.1.0-beta",
            "artifacts": [
                {
                    "role": role,
                    "name": name,
                    "path": path,
                    "mode": "0700",
                    "sha256": digest_file(output / path),
                }
                for role, name, path in ARTIFACTS
            ],
            "metadata": list(METADATA_FILES),
            "installation": {
                "verification_required_before_execution": True,
                "source_build": False,
                "external_network": False,
                "activation": "explicit",
            },
        },
    )
    normalize_bundle_modes(output)
    rewrite_checksums(output)
    return verify(output)


def relative_files(root: Path) -> dict[str, Path]:
    files: dict[str, Path] = {}
    try:
        root_stat = root.lstat()
    except OSError:
        fail("bundle_missing", "bundle directory does not exist")
    if not stat.S_ISDIR(root_stat.st_mode) or is_reparse(root):
        fail("bundle_not_directory", "bundle root must be a real directory")
    check_private(root, root_stat)
    for directory, directory_names, file_names in os.walk(root, followlinks=False):
        base = Path(directory)
        for name in sorted(directory_names):
            path = base / name
            item = path.lstat()
            if not stat.S_ISDIR(item.st_mode) or is_reparse(path):
                fail("non_regular_entry", "bundle contains a non-directory path entry")
            check_private(path, item)
        for name in sorted(file_names):
            path = base / name
            item = path.lstat()
            if not stat.S_ISREG(item.st_mode) or is_reparse(path) or item.st_nlink != 1:
                fail("non_regular_entry", "bundle contains a link or non-regular file")
            check_private(path, item)
            relative = path.relative_to(root).as_posix()
            if PurePosixPath(relative).is_absolute() or ".." in PurePosixPath(relative).parts:
                fail("unsafe_bundle_path", "bundle contains an unsafe relative path")
            files[relative] = path
    return files


def scan_forbidden(files: dict[str, Path]) -> None:
    for relative, path in sorted(files.items()):
        pure = PurePosixPath(relative)
        lowered_name = pure.name.lower()
        if lowered_name.removesuffix(".exe") in RETIRED_BINARIES:
            fail("retired_binary_present", "bundle contains a retired executable")
        if lowered_name.endswith(IMPLEMENTATION_LIBRARY_SUFFIXES) and not (
            PLATFORM.startswith("windows-") and relative == "bin/uc-conpty/conpty.dll"
        ):
            fail("implementation_library_present", "bundle contains an implementation library")
        if any(part in SOURCE_COMPONENTS for part in pure.parts) or pure.name in SOURCE_FILENAMES:
            fail("source_tree_present", "bundle contains source-tree material")
        if relative not in EXPECTED_FILES and any(SECRET_PATH_PARTS.search(part) for part in pure.parts):
            fail("secret_material_detected", "bundle contains a secret-bearing path")
        if relative not in EXECUTABLE_FILES:
            try:
                content = path.read_bytes()
            except OSError:
                fail("bundle_read_failed", "bundle file could not be read")
            if PRIVATE_KEY_BLOCK.search(content):
                fail("secret_material_detected", "bundle contains private key material")


def parse_checksums(path: Path) -> dict[str, str]:
    try:
        lines = path.read_text(encoding="ascii").splitlines()
    except (OSError, UnicodeError):
        fail("checksum_manifest_invalid", "SHA256SUMS is not valid ASCII")
    entries: dict[str, str] = {}
    order: list[str] = []
    for line in lines:
        match = re.fullmatch(r"([0-9a-f]{64})  ([!-~]+)", line)
        if match is None:
            fail("checksum_manifest_invalid", "SHA256SUMS contains an invalid line")
        digest, relative = match.groups()
        pure = PurePosixPath(relative)
        if pure.is_absolute() or ".." in pure.parts or relative in entries:
            fail("checksum_manifest_invalid", "SHA256SUMS contains an unsafe or duplicate path")
        entries[relative] = digest
        order.append(relative)
    if order != sorted(order):
        fail("checksum_manifest_invalid", "SHA256SUMS entries must be bytewise sorted")
    return entries


def verify(root_value: Path | str) -> dict[str, Any]:
    root = Path(root_value)
    files = relative_files(root)
    scan_forbidden(files)
    missing = sorted(EXPECTED_FILES - files.keys())
    extra = sorted(files.keys() - EXPECTED_FILES)
    if missing:
        fail("missing_required_file", "bundle is missing a required file")
    if extra:
        fail("unexpected_bundle_file", "bundle contains a file outside the exact allowlist")

    for relative, path in files.items():
        actual_mode = stat.S_IMODE(path.stat().st_mode)
        expected_mode = 0o700 if relative in EXECUTABLE_FILES else 0o600
        if os.name != "nt" and actual_mode != expected_mode:
            fail("file_mode_mismatch", f"bundle mode is invalid for {relative}")
    checksums = parse_checksums(files[CHECKSUM_FILE])
    checksummed_files = EXPECTED_FILES - {CHECKSUM_FILE}
    if set(checksums) != checksummed_files:
        fail("checksum_file_set_mismatch", "SHA256SUMS does not cover the exact bundle payload")
    for relative in sorted(checksums):
        if digest_file(files[relative]) != checksums[relative]:
            fail("checksum_mismatch", f"SHA-256 mismatch for {relative}")

    package = require_object(
        load_json(files[f"{SHARE}/undercurrent-package.json"], "package_manifest_invalid"),
        "package_manifest_invalid",
        "package manifest",
    )
    validate_package_contract(package)
    if PLATFORM != "linux-x86_64-gnu":
        for _role, name, relative in ARTIFACTS:
            with files[relative].open("rb") as handle:
                validate_binary_header(handle.read(65536), name)
    manifest = require_object(
        load_json(files[BUNDLE_MANIFEST], "bundle_manifest_invalid"),
        "bundle_manifest_invalid",
        "bundle manifest",
    )
    if (
        manifest.get("schema") != BUNDLE_SCHEMA
        or manifest.get("coordinate") != COORDINATE
        or manifest.get("package_version") != VERSION
        or manifest.get("platform") != PLATFORM
    ):
        fail("bundle_manifest_mismatch", "bundle identity does not match the current package and platform")
    declared_artifacts = manifest.get("artifacts")
    if not isinstance(declared_artifacts, list) or len(declared_artifacts) != len(ARTIFACTS):
        fail("artifact_role_mismatch", "bundle must declare exactly three artifact roles")
    expected_by_path = {path: (role, name) for role, name, path in ARTIFACTS}
    observed_paths: set[str] = set()
    for item in declared_artifacts:
        if not isinstance(item, dict):
            fail("artifact_role_mismatch", "artifact declaration must be an object")
        relative = item.get("path")
        if relative not in expected_by_path or relative in observed_paths:
            fail("artifact_role_mismatch", "artifact path is missing, duplicate, or unknown")
        role, name = expected_by_path[relative]
        if item != {
            "role": role,
            "name": name,
            "path": relative,
            "mode": "0700",
            "sha256": checksums[relative],
        }:
            fail("artifact_role_mismatch", "artifact role, name, mode, or digest is invalid")
        observed_paths.add(relative)
    if observed_paths != set(expected_by_path):
        fail("artifact_role_mismatch", "artifact declarations are incomplete")
    if manifest.get("metadata") != list(METADATA_FILES):
        fail("bundle_manifest_mismatch", "bundle metadata roles are incomplete or reordered")
    installation = require_object(
        manifest.get("installation"), "bundle_manifest_mismatch", "bundle installation"
    )
    if installation != {
        "activation": "explicit",
        "external_network": False,
        "source_build": False,
        "verification_required_before_execution": True,
    }:
        fail("bundle_manifest_mismatch", "bundle installation policy is invalid")

    provenance = require_object(
        load_json(files[f"{SHARE}/provenance.json"], "provenance_invalid"),
        "provenance_invalid",
        "provenance",
    )
    if (
        provenance.get("schema") != PROVENANCE_SCHEMA
        or provenance.get("coordinate") != COORDINATE
        or provenance.get("package_version") != VERSION
        or provenance.get("platform") != PLATFORM
        or provenance.get("publication_performed") is not False
        or provenance.get("deployment_performed") is not False
    ):
        fail("provenance_invalid", "provenance identity or local-only posture is invalid")
    expected_subjects = [
        {"name": name, "role": role, "sha256": checksums[path]}
        for role, name, path in ARTIFACTS
    ]
    if provenance.get("subjects") != expected_subjects:
        fail("provenance_invalid", "provenance subjects do not bind all artifact roles")
    expected_materials = [
        {
            "coordinate": COORDINATE,
            "kind": "package_manifest",
            "sha256": checksums[f"{SHARE}/undercurrent-package.json"],
        },
        {
            "coordinate": "crates.io/vt100@0.15.2",
            "kind": "vendored_dependency",
            "sha256": checksums[VT100_PROVENANCE_FILE],
        },
        {
            "coordinate": "crates.io/vte@0.11.1",
            "kind": "vendored_dependency",
            "sha256": checksums[VTE_PROVENANCE_FILE],
        },
        {
            "coordinate": "crates.io/vte@0.11.1#unsafe-audit",
            "kind": "vendored_unsafe_audit",
            "sha256": checksums[VTE_UNSAFE_AUDIT_FILE],
        },
        {
            "coordinate": "undercurrent-foundation@0.1.0-beta#attribution",
            "kind": "source_attribution",
            "sha256": checksums[FOUNDATION_NOTICE_FILE],
        },
        {
            "coordinate": "crates.io/portable-pty@0.9.0",
            "kind": "vendored_dependency",
            "sha256": checksums[PTY_PROVENANCE_FILE],
        },
        dependency_license_material(root),
    ]
    if provenance.get("materials") != expected_materials:
        fail(
            "provenance_invalid",
            "provenance materials do not bind the package, dependency licenses, vendored inputs and Foundation attribution",
        )
    builder = require_object(provenance.get("builder"), "provenance_invalid", "builder")
    if builder != {
        "external_network_used": False,
        "id": "undercurrent-local-release-qualification",
        "source_build_during_installation": False,
    }:
        fail("provenance_invalid", "provenance builder policy is invalid")

    license_metadata = require_object(
        load_json(files[f"{SHARE}/license.json"], "license_metadata_invalid"),
        "license_metadata_invalid",
        "license metadata",
    )
    if license_metadata != {
        "acceptance_required_before_installation": True,
        "coordinate": COORDINATE,
        "package_version": VERSION,
        "schema": LICENSE_SCHEMA,
        "spdx": "Apache-2.0",
        "text": f"{SHARE}/LICENSE",
        "notices": [FOUNDATION_NOTICE_FILE],
        "dependency_collection": dependency_license_metadata(),
        "third_party": [
            *conpty_license_metadata(),
            {"name": "portable-pty", "version": "0.9.0", "spdx": "MIT", "text": PTY_LICENSE_FILE, "provenance": PTY_PROVENANCE_FILE},
            {
                "name": "vt100",
                "version": "0.15.2",
                "spdx": "MIT",
                "text": VT100_LICENSE_FILE,
                "provenance": VT100_PROVENANCE_FILE,
            },
            {
                "name": "vte",
                "version": "0.11.1",
                "spdx": "Apache-2.0 OR MIT",
                "texts": [VTE_APACHE_LICENSE_FILE, VTE_MIT_LICENSE_FILE],
                "provenance": VTE_PROVENANCE_FILE,
                "unsafe_audit": VTE_UNSAFE_AUDIT_FILE,
            },
        ],
    }:
        fail("license_metadata_invalid", "license metadata is incomplete or incompatible")
    # Compare with the reviewed collection, not just attacker-rewritten bundle checksums.
    try:
        third_party_licenses.check(PROJECT_ROOT)
    except (OSError, ValueError, KeyError, TypeError) as error:
        fail("dependency_license_invalid", f"dependency license collection requires review: {error}")
    for relative, source in DEPENDENCY_FILES.items():
        if digest_file(files[relative]) != digest_file(PROJECT_ROOT / "licenses" / source):
            fail("dependency_license_invalid", "dependency inventory or notices differ from the reviewed collection")
    if not files[f"{SHARE}/LICENSE"].read_bytes():
        fail("license_metadata_invalid", "license text is empty")
    if digest_file(files[PTY_LICENSE_FILE]) != PTY_LICENSE_SHA256 or digest_file(files[PTY_PROVENANCE_FILE]) != PTY_PROVENANCE_SHA256:
        fail("third_party_provenance_invalid", "portable-pty license or provenance is not exact")
    if PLATFORM.startswith("windows-"):
        for relative, expected in CONPTY_FILES.items():
            if digest_file(files[relative]) != expected:
                fail("conpty_runtime_invalid", "ConPTY runtime differs from the pinned Microsoft package")
        if digest_file(files[CONPTY_PROVENANCE_FILE]) != CONPTY_PROVENANCE_SHA256:
            fail("third_party_provenance_invalid", "ConPTY provenance is not exact")
    if digest_file(files[FOUNDATION_NOTICE_FILE]) != FOUNDATION_NOTICE_SHA256:
        fail("foundation_notice_invalid", "extracted Foundation attribution is not exact")
    if digest_file(files[VT100_LICENSE_FILE]) != VT100_LICENSE_SHA256:
        fail("third_party_license_invalid", "vt100 MIT license text is not exact")
    if digest_file(files[VT100_PROVENANCE_FILE]) != VT100_PROVENANCE_SHA256:
        fail("third_party_provenance_invalid", "vt100 provenance record is not exact")
    vt100_provenance = require_object(
        load_json(files[VT100_PROVENANCE_FILE], "third_party_provenance_invalid"),
        "third_party_provenance_invalid",
        "vt100 provenance",
    )
    if (
        vt100_provenance.get("schema") != "undercurrent.vendored-crate-provenance/v0.1.0-beta"
        or vt100_provenance.get("package")
        != {"name": "vt100", "version": "0.15.2", "license": "MIT"}
        or vt100_provenance.get("upstream", {}).get("crate_sha256")
        != "84cd863bf0db7e392ba3bd04994be3473491b31e66340672af5d11943c6274de"
        or vt100_provenance.get("patch", {}).get("sha256") != VT100_PATCH_SHA256
        or vt100_provenance.get("metadata_overlay", {}).get("sha256")
        != VT100_METADATA_OVERLAY_SHA256
    ):
        fail("third_party_provenance_invalid", "vt100 provenance identity is invalid")
    if (
        digest_file(files[VTE_APACHE_LICENSE_FILE]) != VTE_APACHE_LICENSE_SHA256
        or digest_file(files[VTE_MIT_LICENSE_FILE]) != VTE_MIT_LICENSE_SHA256
    ):
        fail("third_party_license_invalid", "vte dual-license texts are not exact")
    if digest_file(files[VTE_PROVENANCE_FILE]) != VTE_PROVENANCE_SHA256:
        fail("third_party_provenance_invalid", "vte provenance record is not exact")
    vte_provenance = require_object(
        load_json(files[VTE_PROVENANCE_FILE], "third_party_provenance_invalid"),
        "third_party_provenance_invalid",
        "vte provenance",
    )
    if (
        vte_provenance.get("schema") != "undercurrent.vendored-crate-provenance/v0.1.0-beta"
        or vte_provenance.get("package")
        != {
            "name": "vte",
            "version": "0.11.1",
            "license": "Apache-2.0 OR MIT",
        }
        or vte_provenance.get("upstream", {}).get("crate_sha256")
        != "f5022b5fbf9407086c180e9557be968742d839e68346af7792b8592489732197"
        or vte_provenance.get("patch", {}).get("sha256") != VTE_PATCH_SHA256
        or vte_provenance.get("unsafe_audit", {}).get("sha256")
        != VTE_UNSAFE_AUDIT_SHA256
    ):
        fail("third_party_provenance_invalid", "vte provenance identity is invalid")
    if digest_file(files[VTE_UNSAFE_AUDIT_FILE]) != VTE_UNSAFE_AUDIT_SHA256:
        fail("third_party_unsafe_audit_invalid", "vte unsafe audit is not exact")
    vte_unsafe_audit = require_object(
        load_json(files[VTE_UNSAFE_AUDIT_FILE], "third_party_unsafe_audit_invalid"),
        "third_party_unsafe_audit_invalid",
        "vte unsafe audit",
    )
    if (
        vte_unsafe_audit.get("schema")
        != "undercurrent.vendored-crate-unsafe-audit/v0.1.0-beta"
        or vte_unsafe_audit.get("package", {}).get("name") != "vte"
        or vte_unsafe_audit.get("package", {}).get("version") != "0.11.1"
        or vte_unsafe_audit.get("policy", {}).get("new_unsafe_allowed") is not False
        or vte_unsafe_audit.get("policy", {}).get("audited_site_count") != 3
        or len(vte_unsafe_audit.get("sites", [])) != 3
    ):
        fail("third_party_unsafe_audit_invalid", "vte unsafe audit identity is invalid")
    config_schema = require_object(
        load_json(files[f"{SHARE}/config.schema.json"], "config_schema_invalid"),
        "config_schema_invalid",
        "configuration schema",
    )
    if config_schema.get("$id") != "urn:undercurrent:schema:config:v0.1.0-beta":
        fail("config_schema_invalid", "configuration schema is not exact Undercurrent 0.1.0-beta")
    package_schema = require_object(
        load_json(files[f"{SHARE}/undercurrent-package.schema.json"], "package_schema_invalid"),
        "package_schema_invalid",
        "package schema",
    )
    if package_schema.get("$id") != "urn:undercurrent:schema:package:v0.1.0-beta":
        fail("package_schema_invalid", "package schema is not exact Undercurrent 0.1.0-beta")
    config_example = files[f"{SHARE}/config.example.toml"].read_text(encoding="utf-8")
    if (
        'schema_version = "0.1.0-beta"' not in config_example
        or "[local]" not in config_example
    ):
        fail("config_example_invalid", "configuration example is not local IPC generation 2")

    return {
        "schema": BUNDLE_SCHEMA,
        "status": "passed",
        "coordinate": COORDINATE,
        "package_version": VERSION,
        "platform": PLATFORM,
        "file_count": len(files),
        "bundle_manifest_sha256": checksums[BUNDLE_MANIFEST],
        "artifact_digests": {
            role: checksums[path] for role, _name, path in ARTIFACTS
        },
        "owner_only": True,
        "exact_roles": True,
        "source_tree_files": 0,
        "implementation_libraries": 0,
        "secret_material_files": 0,
    }


def rewrite_checksums(root: Path) -> None:
    paths = sorted(
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and not is_reparse(path) and path.relative_to(root).as_posix() != CHECKSUM_FILE
    )
    content = "".join(f"{digest_file(root / relative)}  {relative}\n" for relative in paths)
    write_bytes(root / CHECKSUM_FILE, content.encode("ascii"))


def archive_bundle(bundle: Path, output: Path) -> dict[str, Any]:
    verify(bundle)
    root_name = f"undercurrent-{VERSION}-{PLATFORM}"
    output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with payload_writer(output, overwrite=True) as writer, tarfile.open(fileobj=writer, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        directories = {PurePosixPath(relative).parent for relative in EXPECTED_FILES}
        expanded: set[PurePosixPath] = set()
        for directory in directories:
            while directory != PurePosixPath("."):
                expanded.add(directory)
                directory = directory.parent
        for relative in [PurePosixPath(".")] + sorted(expanded, key=lambda value: value.as_posix()):
            name = root_name if relative == PurePosixPath(".") else f"{root_name}/{relative.as_posix()}"
            info = tarfile.TarInfo(name)
            info.type = tarfile.DIRTYPE
            info.mode = 0o700
            info.uid = 0
            info.gid = 0
            info.uname = "root"
            info.gname = "root"
            info.mtime = 0
            archive.addfile(info)
        for relative in sorted(EXPECTED_FILES):
            path = bundle / relative
            info = tarfile.TarInfo(f"{root_name}/{relative}")
            info.mode = 0o700 if relative in EXECUTABLE_FILES else 0o600
            info.uid = 0
            info.gid = 0
            info.uname = "root"
            info.gname = "root"
            info.mtime = 0
            info.size = path.stat().st_size
            with path.open("rb") as handle:
                archive.addfile(info, handle)
    set_private(output, 0o600)
    return {"path": str(output), "sha256": digest_file(output), "bytes": output.stat().st_size}


def install_bundle(bundle: Path, prefix: Path, acceptance: str) -> dict[str, Any]:
    if acceptance != "Apache-2.0":
        fail("license_not_accepted", "Apache-2.0 must be explicitly accepted before installation")
    verification = verify(bundle)
    if prefix.exists() or prefix.is_symlink():
        fail("install_prefix_exists", "installation prefix must not already exist")
    create_bundle_root(prefix)
    try:
        for relative in sorted(EXPECTED_FILES):
            source = bundle / relative
            destination = prefix / relative
            copy_regular(
                source,
                destination,
                0o700 if relative in EXECUTABLE_FILES else 0o600,
            )
        normalize_bundle_modes(prefix)
        installed = verify(prefix)
    except BaseException:
        shutil.rmtree(prefix, ignore_errors=True)
        raise
    if installed["artifact_digests"] != verification["artifact_digests"]:
        shutil.rmtree(prefix, ignore_errors=True)
        fail("installed_digest_mismatch", "installed artifact digests differ from verified bundle")
    return {
        "status": "passed",
        "verified_before_copy": True,
        "verified_after_copy": True,
        "license_accepted": acceptance,
        "source_build": False,
        "external_network": False,
        "prefix": str(prefix),
    }


def unpack_bundle(archive_path: Path, output: Path) -> dict[str, Any]:
    """Extract only the exact payload into a newly created private directory."""
    output = output.absolute()
    if output.exists() or output.is_symlink():
        fail("output_exists", "unpack output must not already exist")
    root_name = f"undercurrent-{VERSION}-{PLATFORM}"
    directories = {root_name}
    for relative in EXPECTED_FILES:
        parent = PurePosixPath(relative).parent
        while parent != PurePosixPath("."):
            directories.add(f"{root_name}/{parent}")
            parent = parent.parent
    expected = {f"{root_name}/{relative}" for relative in EXPECTED_FILES} | directories
    create_bundle_root(output)
    try:
        with tarfile.open(archive_path, "r:") as archive:
            observed = set()
            for member in archive:
                if member.name not in expected or member.name in observed or member.pax_headers:
                    fail("unsafe_archive_entry", "archive contains an unexpected or duplicate member")
                observed.add(member.name)
                if member.name in directories:
                    if member.type != tarfile.DIRTYPE or member.size:
                        fail("unsafe_archive_entry", "archive directory has an invalid type or size")
                    continue
                relative = member.name[len(root_name) + 1:]
                maximum = 512 * 1024 * 1024 if relative in EXECUTABLE_FILES else 8 * 1024 * 1024
                if member.type != tarfile.REGTYPE or not 0 <= member.size <= maximum:
                    fail("unsafe_archive_entry", "archive file has an invalid type or size")
                destination = output / relative
                with archive.extractfile(member) as source, payload_writer(destination) as target:
                    shutil.copyfileobj(source, target, 1024 * 1024)
            if observed != expected:
                fail("missing_required_file", "archive does not contain the exact bundle payload")
        normalize_bundle_modes(output)
        result = verify(output)
    except BaseException:
        # Only the fresh output receives files, all from the exact allowlist;
        # archive links and caller-supplied existing destinations are rejected.
        shutil.rmtree(output, ignore_errors=True)
        raise
    return {"status": "passed", "output": str(output), "verification": result}


def synthetic_header() -> bytes:
    """Header-only fixture; adversarial validation never executes this content."""
    import struct
    if PLATFORM == "windows-x86_64-msvc":
        header = bytearray(512)
        header[:2] = b"MZ"
        struct.pack_into("<I", header, 60, 64)
        header[64:68] = b"PE\0\0"
        struct.pack_into("<HH", header, 68, 0x8664, 1)
        struct.pack_into("<HH", header, 84, 112, 2)
        struct.pack_into("<H", header, 88, 0x20b)
        struct.pack_into("<H", header, 156, 3)
        return bytes(header)
    if PLATFORM.startswith("macos-"):
        cpu = {"macos-arm64": 0x0100000c, "macos-x86_64": 0x01000007}[PLATFORM]
        return struct.pack("<IIII", 0xfeedfacf, cpu, 0, 2)
    return b"\x7fELF"


def synthetic_bundle(root: Path) -> None:
    create_bundle_root(root)
    for role, name, relative in ARTIFACTS:
        write_bytes(root / relative, synthetic_header() + b"synthetic-not-executable\n" + name.encode(), 0o700)
    copy_conpty_runtime(root)
    copy_dependency_notices(root)
    copy_regular(PROJECT_ROOT / "vendor/portable-pty/LICENSE.md", root / PTY_LICENSE_FILE, 0o600)
    copy_regular(PROJECT_ROOT / "vendor/portable-pty/PROVENANCE.json", root / PTY_PROVENANCE_FILE, 0o600)
    package = {
        "schema_version": "undercurrent.package/v0.1.0-beta",
        "coordinate": COORDINATE,
        "version": VERSION,
        "kind": "standalone_product",
        "runtime": {
            "model": "external_daemon",
            "semantic_transport": "local_ipc",
            "implicit_daemon_launch": True,
        },
        "installation": {
            "ordinary_mode": "verified_release_artifact",
            "monorepo_clone": False,
            "implicit_source_build": False,
            "verify_before_execution": True,
        },
        "artifacts": [
            {"role": role, "name": name, "platforms": [PLATFORM]} for role, name, _relative in ARTIFACTS
        ],
    }
    package["platforms"] = [{"id": PLATFORM}]
    write_json(root / f"{SHARE}/undercurrent-package.json", package)
    write_json(
        root / f"{SHARE}/undercurrent-package.schema.json",
        {"$id": "urn:undercurrent:schema:package:v0.1.0-beta"},
    )
    write_json(
        root / f"{SHARE}/config.schema.json",
        {"$id": "urn:undercurrent:schema:config:v0.1.0-beta"},
    )
    write_bytes(
        root / f"{SHARE}/config.example.toml",
        b'schema_version = "0.1.0-beta"\n[local]\n',
    )
    write_bytes(root / f"{SHARE}/LICENSE", b"Apache License 2.0 synthetic fixture\n")
    write_bytes(
        root / FOUNDATION_NOTICE_FILE,
        (PROJECT_ROOT / "crates/undercurrent-foundation/NOTICE").read_bytes(),
    )
    write_bytes(
        root / VT100_LICENSE_FILE,
        (PROJECT_ROOT / "vendor/vt100/LICENSE").read_bytes(),
    )
    write_bytes(
        root / VT100_PROVENANCE_FILE,
        (PROJECT_ROOT / "vendor/vt100/PROVENANCE.json").read_bytes(),
    )
    write_bytes(
        root / VTE_APACHE_LICENSE_FILE,
        (PROJECT_ROOT / "vendor/vte/LICENSE-APACHE").read_bytes(),
    )
    write_bytes(
        root / VTE_MIT_LICENSE_FILE,
        (PROJECT_ROOT / "vendor/vte/LICENSE-MIT").read_bytes(),
    )
    write_bytes(
        root / VTE_PROVENANCE_FILE,
        (PROJECT_ROOT / "vendor/vte/PROVENANCE.json").read_bytes(),
    )
    write_bytes(
        root / VTE_UNSAFE_AUDIT_FILE,
        (PROJECT_ROOT / "vendor/vte/UNSAFE-AUDIT.json").read_bytes(),
    )
    write_bytes(root / f"{SHARE}/INSTALL.md", b"Synthetic conformance fixture; never execute.\n")
    write_json(
        root / f"{SHARE}/license.json",
        {
            "schema": LICENSE_SCHEMA,
            "coordinate": COORDINATE,
            "package_version": VERSION,
            "spdx": "Apache-2.0",
            "text": f"{SHARE}/LICENSE",
            "notices": [FOUNDATION_NOTICE_FILE],
            "acceptance_required_before_installation": True,
            "dependency_collection": dependency_license_metadata(),
            "third_party": [
            *conpty_license_metadata(),
            {"name": "portable-pty", "version": "0.9.0", "spdx": "MIT", "text": PTY_LICENSE_FILE, "provenance": PTY_PROVENANCE_FILE},
                {
                    "name": "vt100",
                    "version": "0.15.2",
                    "spdx": "MIT",
                    "text": VT100_LICENSE_FILE,
                    "provenance": VT100_PROVENANCE_FILE,
                },
                {
                    "name": "vte",
                    "version": "0.11.1",
                    "spdx": "Apache-2.0 OR MIT",
                    "texts": [VTE_APACHE_LICENSE_FILE, VTE_MIT_LICENSE_FILE],
                    "provenance": VTE_PROVENANCE_FILE,
                    "unsafe_audit": VTE_UNSAFE_AUDIT_FILE,
                },
            ],
        },
    )
    subjects = [
        {"name": name, "role": role, "sha256": digest_file(root / relative)}
        for role, name, relative in ARTIFACTS
    ]
    write_json(
        root / f"{SHARE}/provenance.json",
        {
            "schema": PROVENANCE_SCHEMA,
            "coordinate": COORDINATE,
            "package_version": VERSION,
            "platform": PLATFORM,
            "purpose": "local_release_qualification",
            "builder": {
                "id": "undercurrent-local-release-qualification",
                "external_network_used": False,
                "source_build_during_installation": False,
            },
            "materials": [
                {
                    "coordinate": COORDINATE,
                    "kind": "package_manifest",
                    "sha256": digest_file(root / f"{SHARE}/undercurrent-package.json"),
                },
                {
                    "coordinate": "crates.io/vt100@0.15.2",
                    "kind": "vendored_dependency",
                    "sha256": digest_file(root / VT100_PROVENANCE_FILE),
                },
                {
                    "coordinate": "crates.io/vte@0.11.1",
                    "kind": "vendored_dependency",
                    "sha256": digest_file(root / VTE_PROVENANCE_FILE),
                },
                {
                    "coordinate": "crates.io/vte@0.11.1#unsafe-audit",
                    "kind": "vendored_unsafe_audit",
                    "sha256": digest_file(root / VTE_UNSAFE_AUDIT_FILE),
                },
                {
                    "coordinate": "undercurrent-foundation@0.1.0-beta#attribution",
                    "kind": "source_attribution",
                    "sha256": digest_file(root / FOUNDATION_NOTICE_FILE),
                },
                {
                    "coordinate": "crates.io/portable-pty@0.9.0",
                    "kind": "vendored_dependency",
                    "sha256": digest_file(root / PTY_PROVENANCE_FILE),
                },
                dependency_license_material(root),
            ],
            "subjects": subjects,
            "publication_performed": False,
            "deployment_performed": False,
        },
    )
    write_json(
        root / BUNDLE_MANIFEST,
        {
            "schema": BUNDLE_SCHEMA,
            "coordinate": COORDINATE,
            "package_version": VERSION,
            "platform": PLATFORM,
            "layout": "operator_selected_prefix/0.1.0-beta",
            "artifacts": [
                {
                    "role": role,
                    "name": name,
                    "path": relative,
                    "mode": "0700",
                    "sha256": digest_file(root / relative),
                }
                for role, name, relative in ARTIFACTS
            ],
            "metadata": list(METADATA_FILES),
            "installation": {
                "verification_required_before_execution": True,
                "source_build": False,
                "external_network": False,
                "activation": "explicit",
            },
        },
    )
    normalize_bundle_modes(root)
    rewrite_checksums(root)


def adversarial(report_path: Path | None) -> dict[str, Any]:
    cases = [
        ("missing_required_artifact", "missing_required_file"),
        ("tampered_artifact", "checksum_mismatch"),
        ("wrong_role", "artifact_role_mismatch"),
        ("extra_secret", "secret_material_detected"),
        ("retired_binary", "retired_binary_present"),
        ("implementation_library", "implementation_library_present"),
        ("source_tree", "source_tree_present"),
        ("tampered_third_party_notice", "third_party_license_invalid"),
        ("tampered_third_party_provenance", "third_party_provenance_invalid"),
        ("tampered_vte_license", "third_party_license_invalid"),
        ("tampered_vte_provenance", "third_party_provenance_invalid"),
        ("tampered_vte_unsafe_audit", "third_party_unsafe_audit_invalid"),
        ("missing_foundation_notice", "missing_required_file"),
        ("tampered_foundation_notice", "foundation_notice_invalid"),
    ]
    results: list[dict[str, str]] = []
    with tempfile.TemporaryDirectory(prefix="ucb-negative-") as temporary:
        # An elevated Windows token may give TemporaryDirectory a group owner.
        # Keep that container and create an explicitly account-owned fixture root.
        temporary_root = Path(temporary) / "fixtures"
        create_bundle_root(temporary_root)
        valid = temporary_root / "valid"
        synthetic_bundle(valid)
        verify(valid)
        for case_id, expected_code in cases:
            candidate = temporary_root / case_id
            install_bundle(valid, candidate, "Apache-2.0")
            for path in [candidate, *candidate.rglob("*")]:
                set_private(path, 0o700 if path.is_dir() or path.relative_to(candidate).as_posix() in EXECUTABLE_FILES else 0o600)
            if case_id == "missing_required_artifact":
                (candidate / ARTIFACTS[1][2]).unlink()
            elif case_id == "missing_foundation_notice":
                (candidate / FOUNDATION_NOTICE_FILE).unlink()
            elif case_id == "tampered_foundation_notice":
                notice_path = candidate / FOUNDATION_NOTICE_FILE
                with notice_path.open("ab") as handle:
                    handle.write(b"tampered")
                provenance_path = candidate / f"{SHARE}/provenance.json"
                provenance = load_json(provenance_path, "fixture_invalid")
                provenance["materials"][4]["sha256"] = digest_file(notice_path)
                write_json(provenance_path, provenance)
                rewrite_checksums(candidate)
            elif case_id == "tampered_artifact":
                with (candidate / ARTIFACTS[0][2]).open("ab") as handle:
                    handle.write(b"tampered")
            elif case_id == "wrong_role":
                manifest = load_json(candidate / BUNDLE_MANIFEST, "fixture_invalid")
                manifest["artifacts"][0]["role"] = "daemon"
                write_json(candidate / BUNDLE_MANIFEST, manifest)
                rewrite_checksums(candidate)
            elif case_id == "extra_secret":
                write_bytes(
                    candidate / "secrets/credential.pem",
                    b"".join((b"-----BEGIN ", b"PRIVATE KEY-----\n"))
                    + b"A" * 64
                    + b"\n-----END PRIVATE KEY-----\n",
                )
                rewrite_checksums(candidate)
            elif case_id == "retired_binary":
                retired = "bin/uc-gateway.exe" if PLATFORM.startswith("windows-") else "bin/uc-gateway"
                write_bytes(candidate / retired, b"\x7fELFretired\n", 0o700)
                rewrite_checksums(candidate)
            elif case_id == "implementation_library":
                write_bytes(candidate / f"{SHARE}/libundercurrent-runtime.so", b"ELF library\n")
                rewrite_checksums(candidate)
            elif case_id == "source_tree":
                write_bytes(candidate / "crates/undercurrent-runtime/src/lib.rs", b"pub fn leak() {}\n")
                rewrite_checksums(candidate)
            elif case_id == "tampered_third_party_notice":
                with (candidate / VT100_LICENSE_FILE).open("ab") as handle:
                    handle.write(b"tampered")
                rewrite_checksums(candidate)
            elif case_id == "tampered_third_party_provenance":
                provenance_path = candidate / VT100_PROVENANCE_FILE
                third_party = load_json(provenance_path, "fixture_invalid")
                third_party["metadata_overlay"]["sha256"] = "0" * 64
                write_json(provenance_path, third_party)
                bundle_provenance_path = candidate / f"{SHARE}/provenance.json"
                bundle_provenance = load_json(
                    bundle_provenance_path, "fixture_invalid"
                )
                bundle_provenance["materials"][1]["sha256"] = digest_file(
                    provenance_path
                )
                write_json(bundle_provenance_path, bundle_provenance)
                rewrite_checksums(candidate)
            elif case_id == "tampered_vte_license":
                with (candidate / VTE_APACHE_LICENSE_FILE).open("ab") as handle:
                    handle.write(b"tampered")
                rewrite_checksums(candidate)
            elif case_id == "tampered_vte_provenance":
                provenance_path = candidate / VTE_PROVENANCE_FILE
                third_party = load_json(provenance_path, "fixture_invalid")
                third_party["upstream"]["vcs_revision"] = "0" * 40
                write_json(provenance_path, third_party)
                bundle_provenance_path = candidate / f"{SHARE}/provenance.json"
                bundle_provenance = load_json(
                    bundle_provenance_path, "fixture_invalid"
                )
                bundle_provenance["materials"][2]["sha256"] = digest_file(
                    provenance_path
                )
                write_json(bundle_provenance_path, bundle_provenance)
                rewrite_checksums(candidate)
            elif case_id == "tampered_vte_unsafe_audit":
                audit_path = candidate / VTE_UNSAFE_AUDIT_FILE
                audit = load_json(audit_path, "fixture_invalid")
                audit["sites"][0]["invariant"] = "tampered"
                write_json(audit_path, audit)
                bundle_provenance_path = candidate / f"{SHARE}/provenance.json"
                bundle_provenance = load_json(
                    bundle_provenance_path, "fixture_invalid"
                )
                bundle_provenance["materials"][3]["sha256"] = digest_file(audit_path)
                write_json(bundle_provenance_path, bundle_provenance)
                rewrite_checksums(candidate)
            normalize_bundle_modes(candidate)
            try:
                verify(candidate)
            except QualificationError as error:
                if error.code != expected_code:
                    fail(
                        "adversarial_wrong_rejection",
                        f"{case_id} produced {error.code}, expected {expected_code}",
                    )
                results.append(
                    {"id": case_id, "status": "rejected", "error_code": error.code}
                )
            else:
                fail("adversarial_accepted", f"adversarial case {case_id} was accepted")
    report = {
        "schema": ADVERSARIAL_SCHEMA,
        "status": "passed",
        "valid_fixture": "passed",
        "artifacts_executed": False,
        "cases": results,
    }
    if report_path is not None:
        report_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        write_json(report_path, report)
    return report


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(description=__doc__)
    subcommands = command.add_subparsers(dest="command", required=True)
    assemble_command = subcommands.add_parser("assemble")
    assemble_command.add_argument("--artifact-dir", required=True)
    assemble_command.add_argument("--package-manifest", required=True)
    assemble_command.add_argument("--package-schema", required=True)
    assemble_command.add_argument("--config-schema", required=True)
    assemble_command.add_argument("--license", required=True)
    assemble_command.add_argument("--foundation-notice", required=True)
    assemble_command.add_argument("--vt100-license", required=True)
    assemble_command.add_argument("--vt100-provenance", required=True)
    assemble_command.add_argument("--vte-apache-license", required=True)
    assemble_command.add_argument("--vte-mit-license", required=True)
    assemble_command.add_argument("--vte-provenance", required=True)
    assemble_command.add_argument("--vte-unsafe-audit", required=True)
    assemble_command.add_argument("--install-document", required=True)
    assemble_command.add_argument("--output", required=True)
    verify_command = subcommands.add_parser("verify")
    verify_command.add_argument("--bundle", required=True)
    archive_command = subcommands.add_parser("archive")
    archive_command.add_argument("--bundle", required=True)
    archive_command.add_argument("--output", required=True)
    install_command = subcommands.add_parser("install")
    install_command.add_argument("--bundle", required=True)
    install_command.add_argument("--prefix", required=True)
    install_command.add_argument("--accept-license", required=True)
    unpack_command = subcommands.add_parser("unpack")
    unpack_command.add_argument("--archive", required=True)
    unpack_command.add_argument("--output", required=True)
    adversarial_command = subcommands.add_parser("adversarial")
    adversarial_command.add_argument("--report")
    for sub in (assemble_command, verify_command, archive_command, install_command, unpack_command, adversarial_command):
        sub.add_argument("--platform", choices=PLATFORMS, default=PLATFORM)
    return command


def main() -> int:
    global PLATFORM
    args = parser().parse_args()
    configure_platform(getattr(args, "platform", PLATFORM))
    try:
        if args.command == "assemble":
            result = assemble(args)
        elif args.command == "verify":
            result = verify(args.bundle)
        elif args.command == "archive":
            result = archive_bundle(Path(args.bundle), Path(args.output))
        elif args.command == "install":
            result = install_bundle(Path(args.bundle), Path(args.prefix), args.accept_license)
        elif args.command == "unpack":
            result = unpack_bundle(Path(args.archive), Path(args.output))
        else:
            result = adversarial(Path(args.report) if args.report else None)
    except QualificationError as error:
        print(f"release-bundle: {error.code}: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
