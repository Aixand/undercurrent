# SPDX-License-Identifier: Apache-2.0
"""Exact vendored terminal parser integrity and unsafe-code policy."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile


VT100_VENDOR_ROOT = Path("vendor/vt100")

VT100_PATCH_PATH = Path("patches/vt100-exact-candidate.patch")

VT100_PATCH_SHA256 = "fd643c8c461990393895f69ed01f4a7d22b785f5a5e3619dc4f5ba615a2f13db"

VT100_METADATA_OVERLAY_PATH = Path(
    "patches/vt100-local-vte-dependency.patch"
)

VT100_METADATA_OVERLAY_SHA256 = (
    "6e555493a804daa4ae9746ac4d38d060115c14d15fe51e9d2295c6d5b6d9129e"
)

VT100_PROVENANCE_PATH = Path("PROVENANCE.json")

VT100_LICENSE_SHA256 = "53a04b0a11073f3d0183c31a4b7bdf9a3f68d453adeec136e59e5a9d9a554370"

VT100_UPSTREAM_TREE_FILES = (
    ".cargo-ok",
    ".cargo_vcs_info.json",
    "CHANGELOG.md",
    "Cargo.lock",
    "Cargo.toml",
    "Cargo.toml.orig",
    "LICENSE",
    "README.md",
    "src/attrs.rs",
    "src/cell.rs",
    "src/grid.rs",
    "src/lib.rs",
    "src/parser.rs",
    "src/row.rs",
    "src/screen.rs",
    "src/term.rs",
)

VT100_PRISTINE_TREE_SHA256 = "94bf90c3740a620556100de3ee1d446b6edc1a4acbeb34d8ca3a4bf9b07010fa"

VT100_CANDIDATE_TREE_SHA256 = "1b97eb4afbbe8feabdb532c2f3c36be895a40d084151a915f1f8bd36c48429c2"

VT100_INTEGRATION_TREE_SHA256 = "3251612b222d95a997d8c5cbfbcaf67f5a4ba4ad70042cd61bf412191c79acd0"

VT100_VENDOR_TREE_SHA256 = "9d62b69f8a706203bfb1739b80d3fe63b564357863bdc5d8d9ebd7b5a1b0e27e"

VT100_VENDORED_FILES = {
    ".cargo_vcs_info.json": "59c8ab884e19eb7d0264622845d3a77674654bb4744fd1cca9e71bb0316e14a4",
    "CHANGELOG.md": "ba3d4706c8a4de9eaaeae41931d72a08a22e0c484feec052ccabaab8296702e6",
    "Cargo.toml": "e48a0e28834581ee9c7116caf92a4466194be56889fddbe43e492ad84721986b",
    "Cargo.toml.orig": "17041f82435e68b183393c113469b7542b4c117813f10c7ac7139598241da7cb",
    "LICENSE": "53a04b0a11073f3d0183c31a4b7bdf9a3f68d453adeec136e59e5a9d9a554370",
    "README.md": "01d073eaeefbd6a5ded71874cf9fe4738ee0e99a6f47772eb51254cc76c30dba",
    "src/attrs.rs": "4b8f6ba1ddb39a35db0406273f22b1292f65b29dd33b2656584d10ece8a22468",
    "src/cell.rs": "ceae7f620a35268e43128158f0a686068b6fcabafdcf274bffe33729936a52e9",
    "src/grid.rs": "b42f58aca9c77e2a87ee420f7bb33e67fb1e8ed1761da939a3f18b3ce9608e98",
    "src/lib.rs": "00b627e8529426741027ff7f27e00fcecb388e7d3a8a3b924ca2fc500a8dfa7f",
    "src/parser.rs": "6b080733b02fc6509183883fa2874d8b3a4cd26d0c7e63f0e6a3750cfcd45cb6",
    "src/row.rs": "2be8a7a63d17bdddde519e90c36a287fb46483443096ca3c482c55ed7b58d8f5",
    "src/screen.rs": "4d34a5122cfdaf544d9163f03921eef073a4d662450c426d50ebb57ab9643ada",
    "src/term.rs": "1c61bfc839026bd730fed5f71b6057286a6a33edede05df16693263cc00f7222"
}

VT100_CHANGED_FILES = [
    {
        "path": "src/cell.rs",
        "upstream_sha256": "5539f8bc03f1b20872b3a5a256b9a60c9e0c5d1983c574284f8fbb6ba6ee03c1",
        "candidate_sha256": "51ef40a250531d55980fdbb9b6dd9ccb536afafa7db232718bcd74d9b42408e8",
    },
    {
        "path": "src/grid.rs",
        "upstream_sha256": "84331d982dad8f276bb4017af468a81a29e2ff4519d54d2bbd2df61ad92cf5d7",
        "candidate_sha256": "493d863094d4694b79b9fd23711e05b24d74cd3d99c755dd29cb9658864afae9",
    },
    {
        "path": "src/parser.rs",
        "upstream_sha256": "0fcf35f2c553fe20059afcf259431789bc4c1e074b53b462746d038fa999aebb",
        "candidate_sha256": "2bf76ad43576576418e2837d179895a606d2007fc038176814f71dee89ba1e8f",
    },
    {
        "path": "src/row.rs",
        "upstream_sha256": "3d670ed3f9ba7a83990a88b7e01ddb2137b53993f49471ca78d6864044bce992",
        "candidate_sha256": "99633d0830d3b5ab7e8c127d52a44c4e94bdd51ce6970b0972ea40fc59925473",
    },
    {
        "path": "src/screen.rs",
        "upstream_sha256": "97b6ac1fe242017a5f1f2b7a866f738fdc41a1e7db4340b4d87cbe1785da8615",
        "candidate_sha256": "ebe95a371ad597c9299e87ca877ca889581f0934bc8c8ffe8cecc932df513487",
    },
]

VT100_METADATA_OVERLAY_CHANGED_FILES = [
    {
        "path": "Cargo.toml",
        "upstream_sha256": (
            "a38fcde696b9fe9458a6c09a600779888095718c3951eef3f5a30fba601a10a3"
        ),
        "candidate_sha256": VT100_VENDORED_FILES["Cargo.toml"],
    }
]

VT100_SCROLLBACK_OVERLAY = {
    "path": "patches/vt100-scrollback.patch",
    "sha256": "19cfa2e549d1d79f60e1ea0fed353f7e36ce038c267ec5b283e58e6a2d8ce772",
    "purpose": "Bound visible history rows to the viewport without offset underflow, and resize retained rows with the viewport. Retain rows scrolled off top-anchored primary-screen regions while excluding interior regions and alternate-screen history.",
    "changed_files": [
        {
            "path": "src/grid.rs",
            "input_sha256": "bd222e317a792c58545611fe6a822e3c00b57c894baf9239652c0e4d2e888b73",
            "output_sha256": "b42f58aca9c77e2a87ee420f7bb33e67fb1e8ed1761da939a3f18b3ce9608e98"
        }
    ]
}

VT100_TERMINAL_REPLIES_OVERLAY = {
    "path": "patches/vt100-terminal-replies.patch",
    "sha256": "917e6e08089f7fb8a6862d9640ec921c4ee71ad2076610b58a2c1b84369d6870",
    "purpose": "Bounded primary device attributes, operating status and cursor position replies, drained by the PTY owner without blocking its output reader. No unsafe code.",
    "changed_files": [
        {
            "path": "src/parser.rs",
            "input_sha256": "c5e4bf1eae32809a7a51397eda424c46ca5a8b37514821320d43438fed53423a",
            "output_sha256": "6b080733b02fc6509183883fa2874d8b3a4cd26d0c7e63f0e6a3750cfcd45cb6"
        },
        {
            "path": "src/screen.rs",
            "input_sha256": "3f65db2b2ffe259c76e044c11bab56afa948327ec697b58ad915cbd8d2e4c646",
            "output_sha256": "4d34a5122cfdaf544d9163f03921eef073a4d662450c426d50ebb57ab9643ada"
        }
    ]
}

VT100_FORMATTING_OVERLAY_FILES = [
    {
        "path": "src/attrs.rs",
        "input_sha256": "76893ab06f4aba9e21bbaec1430237a0214e7ae089655506fcbdeaf04270856f",
        "output_sha256": VT100_VENDORED_FILES["src/attrs.rs"],
    },
    {
        "path": "src/cell.rs",
        "input_sha256": "51ef40a250531d55980fdbb9b6dd9ccb536afafa7db232718bcd74d9b42408e8",
        "output_sha256": VT100_VENDORED_FILES["src/cell.rs"],
    },
    {
        "path": "src/grid.rs",
        "input_sha256": "493d863094d4694b79b9fd23711e05b24d74cd3d99c755dd29cb9658864afae9",
        "output_sha256": "bd222e317a792c58545611fe6a822e3c00b57c894baf9239652c0e4d2e888b73",
    },
    {
        "path": "src/parser.rs",
        "input_sha256": "2bf76ad43576576418e2837d179895a606d2007fc038176814f71dee89ba1e8f",
        "output_sha256": "c5e4bf1eae32809a7a51397eda424c46ca5a8b37514821320d43438fed53423a",
    },
    {
        "path": "src/row.rs",
        "input_sha256": "99633d0830d3b5ab7e8c127d52a44c4e94bdd51ce6970b0972ea40fc59925473",
        "output_sha256": VT100_VENDORED_FILES["src/row.rs"],
    },
    {
        "path": "src/screen.rs",
        "input_sha256": "ebe95a371ad597c9299e87ca877ca889581f0934bc8c8ffe8cecc932df513487",
        "output_sha256": "3f65db2b2ffe259c76e044c11bab56afa948327ec697b58ad915cbd8d2e4c646",
    },
    {
        "path": "src/term.rs",
        "input_sha256": "48b7fdad2ead1d6a148ae9b680c5afec0797a94907e7e0c542d896540d510601",
        "output_sha256": VT100_VENDORED_FILES["src/term.rs"],
    },
]

VTE_VENDOR_ROOT = Path("vendor/vte")

VTE_PATCH_PATH = Path("patches/vte-ground-ordered-noop.patch")

VTE_PATCH_SHA256 = "3c5c222d1329464cad3f11d0f61f2b8fad7fd6351a6877580b3879b0d8681574"

VTE_PROVENANCE_PATH = Path("PROVENANCE.json")

VTE_UNSAFE_AUDIT_PATH = Path("UNSAFE-AUDIT.json")

VTE_UNSAFE_AUDIT_SHA256 = "42a0e41e60242b706618127210a2af3005a0a7e60859e0713b158aa7b9997609"

VTE_APACHE_LICENSE_SHA256 = "62c7a1e35f56406896d7aa7ca52d0cc0d272ac022b5d2796e7d6905db8a3636a"

VTE_MIT_LICENSE_SHA256 = "e4c9b06fa850cb9b540a5e400e9f6394cf15efcf4098144de477d1d3dae10150"

VTE_UPSTREAM_TREE_FILES = (
    ".builds/linux.yml",
    ".cargo-ok",
    ".cargo_vcs_info.json",
    ".gitignore",
    "CHANGELOG.md",
    "Cargo.lock",
    "Cargo.toml",
    "Cargo.toml.orig",
    "LICENSE-APACHE",
    "LICENSE-MIT",
    "README.md",
    "examples/parselog.rs",
    "rustfmt.toml",
    "src/ansi.rs",
    "src/definitions.rs",
    "src/lib.rs",
    "src/params.rs",
    "src/table.rs",
    "tests/demo.vte",
)

VTE_PRISTINE_TREE_SHA256 = "182c0a6dac80b7a8de7b150a9a368d1c6bcc64a32ce7d5ad97f2f8f789c988e1"

VTE_CANDIDATE_TREE_SHA256 = "bc98e6aa9a73c4b839a798e334545dfd42553bc2a201f8f29d89e92aed370fcf"

VTE_VENDOR_TREE_SHA256 = "daa2bc1c76deba487d5fe3325b12dd7d027f8ca30ef37913b1a3e1d7b6e5deb4"

VTE_VENDORED_FILES = {
    ".builds/linux.yml": "d6b8e44b7843c312fba78a12901966521cb840a8c5a3a7b74256e3bfe6afb1b2",
    ".cargo_vcs_info.json": "7f180032d5740ea4ee946c34c14723806a802ba1559e5d67231ad06f9d8ccae5",
    ".gitignore": "f9b1ca6ae27d1c18215265024629a8960c31379f206d9ed20f64e0b2dcf79805",
    "CHANGELOG.md": "5cc90a0a0cd4ff2b27bceab5b4630b94554585af2ffda3e5516e4ebde51da3d2",
    "Cargo.toml": "6941bb40686e00cea50c30951ff3df1b962634cf74a175b18105ed89ca731b4a",
    "Cargo.toml.orig": "03cbe58d2614ad38fb2947a176e96bcccca86bf7384a3c06a57a038051675256",
    "LICENSE-APACHE": VTE_APACHE_LICENSE_SHA256,
    "LICENSE-MIT": VTE_MIT_LICENSE_SHA256,
    "README.md": "ff6200be737dfb4275b2cfbcd7742a7d7031f159908dd7799925733782341995",
    "examples/parselog.rs": "c4893136b07e87e613f05f63d6b784153393da82c01a106d4ed2178991ea8914",
    "rustfmt.toml": "815ceb8294049f74705470e17e7148a724f733088cf7f94ab3984e3e97114df2",
    "src/ansi.rs": "c99470854c3b7bea14a97e9e8c366fa475a117654729ae28600994500b7ce134",
    "src/definitions.rs": "c99f830c4ad27631c8b2e055ffef5c920fd8652ed2144c2999799ecf3fea8a30",
    "src/lib.rs": "f81721a41bff479b778d2e306868c50ac49afb8ea0d30b44ebaa9c0ba4e70b18",
    "src/params.rs": "fb79b5a035261378e0338f35e822d20640b37d057fb7f75d42e698a49befb545",
    "src/table.rs": "e8a343a1015e0683cc1f081717fdf5d518daae75667400fc42a405f6592a4a35",
    "tests/demo.vte": "1ac4e7fe28999e2aa632cd80f85d4f5dae776499164ffe0fa792151614ef80fe",
}

VTE_CHANGED_FILES = [
    {
        "path": "src/lib.rs",
        "upstream_sha256": "c32a92ae8989d37456ffca7e6d723f2957c15c429096aeeb8122de4ed88f321e",
        "candidate_sha256": VTE_VENDORED_FILES["src/lib.rs"],
    }
]

VTE_UNSAFE_SITES = [
    {
        "path": "src/definitions.rs",
        "source_file_sha256": VTE_VENDORED_FILES["src/definitions.rs"],
        "candidate_lines": [55, 62],
        "upstream_lines": [55, 62],
        "unsafe_line": 55,
        "context": (
            "Unpack the state-table byte into low-nibble State and high-nibble "
            "Action values."
        ),
        "context_sha256": "6603d5e89aacc1c1177313a78060b0462592ae00e5c7c8c9215275f4122778e9",
        "invariant": (
            "State and Action each remain exactly 16 tag-only variants with "
            "discriminants 0 through 15, so both masked nibbles are valid enum "
            "discriminants."
        ),
        "status": "unchanged_upstream",
    },
    {
        "path": "src/lib.rs",
        "source_file_sha256": VTE_VENDORED_FILES["src/lib.rs"],
        "candidate_lines": [316, 317],
        "upstream_lines": [203, 204],
        "unsafe_line": 317,
        "context": (
            "Construct the bounded OSC parameter scratch array with uninitialized "
            "MaybeUninit elements."
        ),
        "context_sha256": "45868417b194b4e63a0f4e80dffe2692b5864f8d0ada7422dfdfe17d114922f0",
        "invariant": (
            "Every element has type MaybeUninit<&[u8]>, so the array itself may "
            "begin uninitialized before the used prefix is populated."
        ),
        "status": "unchanged_upstream",
    },
    {
        "path": "src/lib.rs",
        "source_file_sha256": VTE_VENDORED_FILES["src/lib.rs"],
        "candidate_lines": [324, 328],
        "upstream_lines": [211, 215],
        "unsafe_line": 324,
        "context": (
            "Cast only the initialized OSC parameter prefix to borrowed byte "
            "slices for the synchronous callback."
        ),
        "context_sha256": "bb3e933a9f84908afdb0d6fc4943e78b8c7eda95f0f146d944c55160ecdfb6d6",
        "invariant": (
            "osc_num_params never exceeds MAX_OSC_PARAMS, every element in the "
            "selected prefix is initialized from a bounds-checked osc_raw range, "
            "and the borrowed slices do not escape osc_dispatch."
        ),
        "status": "unchanged_upstream",
    },
]

SOURCE_DIGEST_ALGORITHM = "sha256-tree-v1"

SOURCE_EXCLUDED_NAMES = {".git", "reports", "target", "__pycache__"}

SOURCE_TEMP_PREFIXES = (
    ".qualification-tmp.",
    "qualification-tmp.",
    ".undercurrent-qualification.",
    "undercurrent-qualification.",
)

class ContractViolation(Exception):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)

def reject_duplicate_members(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, child in pairs:
        require(key not in value, "JSON_DUPLICATE_KEY", f"duplicate JSON member {key!r}")
        value[key] = child
    return value

def load_json(path: Path) -> object:
    try:
        return json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=reject_duplicate_members,
        )
    except (OSError, json.JSONDecodeError) as error:
        raise ContractViolation("JSON_INVALID", f"cannot parse {path}: {error}") from error

def require(condition: bool, code: str, message: str) -> None:
    if not condition:
        raise ContractViolation(code, message)

def is_qualification_temporary(name: str) -> bool:
    return any(name.startswith(prefix) for prefix in SOURCE_TEMP_PREFIXES)

def is_source_path_excluded(relative: Path) -> bool:
    return any(
        part in SOURCE_EXCLUDED_NAMES or is_qualification_temporary(part)
        for part in relative.parts
    )

def source_digest(root: Path, *, regular_file_mode: int | None = None) -> str:
    root = root.resolve()
    require(root.is_dir(), "SOURCE_DIGEST", f"source root is not a directory: {root}")
    digest = hashlib.sha256()
    digest.update(f"{SOURCE_DIGEST_ALGORITHM}\0".encode())
    for directory, directory_names, file_names in os.walk(root, topdown=True, followlinks=False):
        directory_path = Path(directory)
        directory_names[:] = sorted(
            name
            for name in directory_names
            if name not in SOURCE_EXCLUDED_NAMES and not is_qualification_temporary(name)
        )
        for name in sorted(file_names):
            path = directory_path / name
            relative = path.relative_to(root).as_posix()
            if is_source_path_excluded(Path(relative)):
                continue
            stat_result = path.lstat()
            mode = stat_result.st_mode & 0o777
            if path.is_symlink():
                kind = "L"
                content = os.readlink(path).encode("utf-8", errors="surrogateescape")
            elif path.is_file():
                kind = "F"
                content = path.read_bytes()
                if regular_file_mode is not None:
                    mode = regular_file_mode
            else:
                continue
            digest.update(kind.encode())
            digest.update(b"\0")
            digest.update(relative.encode("utf-8", errors="surrogateescape"))
            digest.update(b"\0")
            digest.update(f"{mode:o}".encode())
            digest.update(b"\0")
            digest.update(str(len(content)).encode())
            digest.update(b"\0")
            digest.update(hashlib.sha256(content).digest())
            digest.update(b"\n")
    return f"sha256:{digest.hexdigest()}"

def selected_source_digest(root: Path, relative_paths: tuple[str, ...]) -> str:
    with tempfile.TemporaryDirectory(prefix="vt100-source-digest-") as temporary:
        selected_root = Path(temporary)
        for relative in relative_paths:
            source = root / relative
            require(
                source.is_file() and not source.is_symlink(),
                "PROVENANCE_INVALID",
                f"vt100 source-digest input is missing or not regular: {relative}",
            )
            destination = selected_root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
        # Every approved vt100/vte input is a non-executable source or metadata
        # file. Windows synthesizes permission bits instead of retaining POSIX
        # modes, so hash those inputs with their canonical 0644 mode there.
        return source_digest(
            selected_root, regular_file_mode=0o644 if os.name == "nt" else None
        )

def strip_rust_comments_and_strings(text: str) -> str:
    result: list[str] = []
    index = 0
    block_depth = 0
    state = "code"
    while index < len(text):
        pair = text[index : index + 2]
        char = text[index]
        if state == "code":
            if pair == "//":
                state = "line_comment"
                result.extend("  ")
                index += 2
            elif pair == "/*":
                state = "block_comment"
                block_depth = 1
                result.extend("  ")
                index += 2
            elif char == '"':
                state = "string"
                result.append(" ")
                index += 1
            else:
                result.append(char)
                index += 1
        elif state == "line_comment":
            result.append("\n" if char == "\n" else " ")
            if char == "\n":
                state = "code"
            index += 1
        elif state == "block_comment":
            if pair == "/*":
                block_depth += 1
                result.extend("  ")
                index += 2
            elif pair == "*/":
                block_depth -= 1
                result.extend("  ")
                index += 2
                if block_depth == 0:
                    state = "code"
            else:
                result.append("\n" if char == "\n" else " ")
                index += 1
        else:
            if char == "\\":
                result.extend("  ")
                index += 2
            elif char == '"':
                result.append(" ")
                index += 1
                state = "code"
            else:
                result.append("\n" if char == "\n" else " ")
                index += 1
    return "".join(result)

def validate_vendored_vt100(root: Path) -> set[Path]:
    vendor_root = root / VT100_VENDOR_ROOT
    require(
        vendor_root.is_dir() and not vendor_root.is_symlink(),
        "LICENSE_INVALID",
        "approved vt100 vendor root is missing or not a real directory",
    )
    for path in [vendor_root, *vendor_root.rglob("*")]:
        require(
            not path.is_symlink(),
            "PROVENANCE_INVALID",
            "approved vt100 vendor subtree contains a symbolic link",
        )

    expected_files = set(VT100_VENDORED_FILES) | {
        VT100_PROVENANCE_PATH.as_posix(),
        VT100_PATCH_PATH.as_posix(),
        VT100_METADATA_OVERLAY_PATH.as_posix(),
        VT100_SCROLLBACK_OVERLAY["path"],
        VT100_TERMINAL_REPLIES_OVERLAY["path"],
    }
    actual_files = {
        path.relative_to(vendor_root).as_posix()
        for path in vendor_root.rglob("*")
        if path.is_file()
    }
    require(
        actual_files == expected_files,
        "PROVENANCE_INVALID",
        "approved vt100 vendor file set is not exact",
    )

    provenance = load_json(vendor_root / VT100_PROVENANCE_PATH)
    require(
        set(provenance)
        == {
            "schema",
            "package",
            "upstream",
            "trees",
            "version_handling",
            "patch",
            "metadata_overlay",
            "formatting_overlay",
            "scrollback_overlay",
            "terminal_replies_overlay",
            "license",
            "integration",
            "vendored_files",
        },
        "PROVENANCE_INVALID",
        "vt100 provenance fields are not exact",
    )
    require(
        provenance["schema"] == "undercurrent.vendored-crate-provenance/v0.1.0-beta",
        "PROVENANCE_INVALID",
        "vt100 provenance schema is invalid",
    )
    require(
        provenance["package"]
        == {"name": "vt100", "version": "0.15.2", "license": "MIT"},
        "LICENSE_INVALID",
        "vt100 package identity or license is invalid",
    )
    require(
        provenance["upstream"]
        == {
            "registry": "crates.io",
            "crate_sha256": "84cd863bf0db7e392ba3bd04994be3473491b31e66340672af5d11943c6274de",
            "vcs_revision": "4c9e5da3de10d838d792929f1691024d6bf59c85",
        },
        "PROVENANCE_INVALID",
        "vt100 upstream provenance is invalid",
    )
    require(
        provenance["trees"]
        == {
            "algorithm": "sha256-tree-v1",
            "implementation": "scripts/checks/vendor_policy.py::source_digest",
            "upstream_pristine": {
                "file_count": 16,
                "paths": list(VT100_UPSTREAM_TREE_FILES),
                "sha256": VT100_PRISTINE_TREE_SHA256,
            },
            "qualified_candidate": {
                "file_count": 16,
                "manifest_metadata": "restored_upstream_0.15.2_pre_wiring",
                "paths": list(VT100_UPSTREAM_TREE_FILES),
                "sha256": VT100_CANDIDATE_TREE_SHA256,
            },
            "integration_candidate": {
                "file_count": 16,
                "manifest_metadata": (
                    "restored_upstream_0.15.2_direct_vte_path_overlay"
                ),
                "paths": list(VT100_UPSTREAM_TREE_FILES),
                "sha256": VT100_INTEGRATION_TREE_SHA256,
            },
            "vendored_candidate": {
                "file_count": 14,
                "paths": list(VT100_VENDORED_FILES),
                "sha256": VT100_VENDOR_TREE_SHA256,
            },
        },
        "PROVENANCE_INVALID",
        "vt100 source-tree provenance is invalid",
    )
    require(
        provenance["version_handling"]
        == {
            "qualified_manifest_versions_not_copied": [
                "0.15.2+width-recycle-reset-singlegrid",
                "0.15.2+width_recycle_reset",
            ],
            "vendored_version": "0.15.2",
        },
        "PROVENANCE_INVALID",
        "vt100 version normalization provenance is invalid",
    )
    require(
        provenance["patch"]
        == {
            "path": VT100_PATCH_PATH.as_posix(),
            "sha256": VT100_PATCH_SHA256,
            "purpose": (
                "Batch printable ASCII and valid UTF-8 terminal updates by row, "
                "with exact cached Unicode widths (including distinct absent and "
                "greater-than-two widths), a bounded non-semantic decoded-run "
                "cache that falls back to scalar rendering when cell copying "
                "cannot preserve multi-column semantics, dirty-range row "
                "recycling, and cached glyph copying into proven-pristine row "
                "suffixes, while preserving combining characters, wide-cell "
                "cleanup, wrapping, attributes, error accounting, clone "
                "semantics, and the vt100 0.15.2 public API."
            ),
            "changed_files": VT100_CHANGED_FILES,
        },
        "PROVENANCE_INVALID",
        "vt100 patch provenance is invalid",
    )
    require(
        provenance["metadata_overlay"]
        == {
            "path": VT100_METADATA_OVERLAY_PATH.as_posix(),
            "sha256": VT100_METADATA_OVERLAY_SHA256,
            "purpose": (
                "Bind vt100 0.15.2's generated normal and dev vte dependencies "
                "to the separately vendored vte 0.11.1 path while preserving "
                "Cargo.toml.orig as the exact upstream manifest."
            ),
            "changed_files": VT100_METADATA_OVERLAY_CHANGED_FILES,
        },
        "PROVENANCE_INVALID",
        "vt100 metadata-overlay provenance is invalid",
    )
    require(
        provenance["scrollback_overlay"] == VT100_SCROLLBACK_OVERLAY
        and hashlib.sha256(
            (vendor_root / VT100_SCROLLBACK_OVERLAY["path"]).read_bytes()
        ).hexdigest() == VT100_SCROLLBACK_OVERLAY["sha256"],
        "PROVENANCE_INVALID",
        "vt100 scrollback-overlay provenance or patch digest is invalid",
    )
    require(
        provenance["terminal_replies_overlay"] == VT100_TERMINAL_REPLIES_OVERLAY
        and hashlib.sha256(
            (vendor_root / VT100_TERMINAL_REPLIES_OVERLAY["path"]).read_bytes()
        ).hexdigest() == VT100_TERMINAL_REPLIES_OVERLAY["sha256"],
        "PROVENANCE_INVALID",
        "vt100 terminal-replies overlay provenance or patch digest is invalid",
    )
    require(
        provenance["formatting_overlay"]
        == {
            "tool": "rustfmt 1.9.0-stable (59807616e1 2026-04-14)",
            "edition": "2018",
            "configuration": "default",
            "behavioral_scope": "formatting_only",
            "files": VT100_FORMATTING_OVERLAY_FILES,
        },
        "PROVENANCE_INVALID",
        "vt100 formatting-overlay provenance is invalid",
    )
    require(
        provenance["license"]
        == {
            "spdx": "MIT",
            "path": "LICENSE",
            "sha256": VT100_LICENSE_SHA256,
        },
        "LICENSE_INVALID",
        "vt100 MIT license provenance is invalid",
    )
    require(
        provenance["integration"]
        == {
            "dependency": "workspace_path",
            "version_requirement": "=0.15.2",
            "vte_source": "path",
            "vte_path": "../vte",
            "vte_version_requirement": "=0.11.1",
            "vte_manifest_sections": [
                "dependencies.vte",
                "dev-dependencies.vte",
            ],
            "unsafe_code_added": False,
        },
        "PROVENANCE_INVALID",
        "vt100 integration provenance is invalid",
    )
    require(
        provenance["vendored_files"] == VT100_VENDORED_FILES,
        "PROVENANCE_INVALID",
        "vt100 vendored-file provenance is invalid",
    )

    for relative, expected_sha256 in VT100_VENDORED_FILES.items():
        path = vendor_root / relative
        require(
            path.is_file() and not path.is_symlink(),
            "PROVENANCE_INVALID",
            f"vt100 vendored file is missing: {relative}",
        )
        require(
            hashlib.sha256(path.read_bytes()).hexdigest() == expected_sha256,
            "PROVENANCE_INVALID",
            f"vt100 vendored file digest mismatch: {relative}",
        )
    require(
        hashlib.sha256((vendor_root / VT100_PATCH_PATH).read_bytes()).hexdigest()
        == VT100_PATCH_SHA256,
        "PROVENANCE_INVALID",
        "vt100 patch digest mismatch",
    )
    require(
        hashlib.sha256(
            (vendor_root / VT100_METADATA_OVERLAY_PATH).read_bytes()
        ).hexdigest()
        == VT100_METADATA_OVERLAY_SHA256,
        "PROVENANCE_INVALID",
        "vt100 metadata-overlay patch digest mismatch",
    )
    require(
        selected_source_digest(vendor_root, tuple(VT100_VENDORED_FILES))
        == f"sha256:{VT100_VENDOR_TREE_SHA256}",
        "PROVENANCE_INVALID",
        "vt100 selected source tree does not match sha256-tree-v1 provenance",
    )
    return {
        VT100_VENDOR_ROOT / relative
        for relative in VT100_VENDORED_FILES
        if relative.startswith("src/") and relative.endswith(".rs")
    }

def source_line_digest(path: Path, start: int, end: int) -> str:
    lines = path.read_bytes().splitlines(keepends=True)
    require(
        1 <= start <= end <= len(lines),
        "UNSAFE_RUST",
        f"unsafe audit line range is outside {path}",
    )
    return hashlib.sha256(b"".join(lines[start - 1 : end])).hexdigest()

def validate_vendored_vte(root: Path) -> tuple[set[Path], set[tuple[Path, int]]]:
    vendor_root = root / VTE_VENDOR_ROOT
    require(
        vendor_root.is_dir() and not vendor_root.is_symlink(),
        "LICENSE_INVALID",
        "approved vte vendor root is missing or not a real directory",
    )
    for path in [vendor_root, *vendor_root.rglob("*")]:
        require(
            not path.is_symlink(),
            "PROVENANCE_INVALID",
            "approved vte vendor subtree contains a symbolic link",
        )

    expected_files = set(VTE_VENDORED_FILES) | {
        VTE_PROVENANCE_PATH.as_posix(),
        VTE_UNSAFE_AUDIT_PATH.as_posix(),
        VTE_PATCH_PATH.as_posix(),
    }
    actual_files = {
        path.relative_to(vendor_root).as_posix()
        for path in vendor_root.rglob("*")
        if path.is_file()
    }
    require(
        actual_files == expected_files,
        "PROVENANCE_INVALID",
        "approved vte vendor file set is not exact",
    )

    unsafe_audit = load_json(vendor_root / VTE_UNSAFE_AUDIT_PATH)
    expected_unsafe_audit = {
        "schema": "undercurrent.vendored-crate-unsafe-audit/v0.1.0-beta",
        "package": {
            "name": "vte",
            "version": "0.11.1",
            "source_tree_sha256": VTE_VENDOR_TREE_SHA256,
        },
        "policy": {
            "new_unsafe_allowed": False,
            "audited_site_count": 3,
            "patch_sha256": VTE_PATCH_SHA256,
        },
        "sites": VTE_UNSAFE_SITES,
    }
    require(
        unsafe_audit == expected_unsafe_audit,
        "UNSAFE_RUST",
        "vte unsafe audit is not the exact approved three-site record",
    )

    provenance = load_json(vendor_root / VTE_PROVENANCE_PATH)
    expected_provenance = {
        "schema": "undercurrent.vendored-crate-provenance/v0.1.0-beta",
        "package": {
            "name": "vte",
            "version": "0.11.1",
            "license": "Apache-2.0 OR MIT",
        },
        "upstream": {
            "registry": "crates.io",
            "crate_sha256": (
                "f5022b5fbf9407086c180e9557be968742d839e68346af7792b8592489732197"
            ),
            "vcs_revision": "5e209bdccb44d7fbf3911629b4b9e771d8298b23",
        },
        "trees": {
            "algorithm": "sha256-tree-v1",
            "implementation": "scripts/checks/vendor_policy.py::source_digest",
            "upstream_pristine": {
                "file_count": 19,
                "paths": list(VTE_UPSTREAM_TREE_FILES),
                "sha256": VTE_PRISTINE_TREE_SHA256,
            },
            "qualified_candidate": {
                "file_count": 19,
                "manifest_metadata": "unchanged_upstream_0.11.1",
                "paths": list(VTE_UPSTREAM_TREE_FILES),
                "sha256": VTE_CANDIDATE_TREE_SHA256,
            },
            "vendored_candidate": {
                "file_count": 17,
                "paths": list(VTE_VENDORED_FILES),
                "sha256": VTE_VENDOR_TREE_SHA256,
            },
        },
        "version_handling": {
            "qualified_manifest_version": "0.11.1",
            "vendored_version": "0.11.1",
            "metadata_modified": False,
        },
        "patch": {
            "path": VTE_PATCH_PATH.as_posix(),
            "sha256": VTE_PATCH_SHA256,
            "purpose": (
                "Batch valid printable text and complete numeric CSI sequences "
                "in Ground state while preserving scalar callback ordering and "
                "fallback for controls, private/intermediate/subparameter CSI, "
                "invalid UTF-8, split input, parser state, and table behavior."
            ),
            "changed_files": VTE_CHANGED_FILES,
        },
        "licenses": [
            {
                "spdx": "Apache-2.0",
                "path": "LICENSE-APACHE",
                "sha256": VTE_APACHE_LICENSE_SHA256,
            },
            {
                "spdx": "MIT",
                "path": "LICENSE-MIT",
                "sha256": VTE_MIT_LICENSE_SHA256,
            },
        ],
        "unsafe_audit": {
            "path": VTE_UNSAFE_AUDIT_PATH.as_posix(),
            "sha256": VTE_UNSAFE_AUDIT_SHA256,
            "audited_site_count": 3,
            "new_unsafe_allowed": False,
        },
        "integration": {
            "dependency": "workspace_path",
            "workspace_relative_path": "vendor/vte",
            "version_requirement": "=0.11.1",
            "direct_test_consumer": "undercurrent-runtime",
            "transitive_runtime_consumer": "vt100",
            "registry_reference_alias": "vte-reference",
            "registry_reference_version": "=0.11.1",
            "unsafe_code_added": False,
        },
        "vendored_files": VTE_VENDORED_FILES,
    }
    require(
        provenance == expected_provenance,
        "PROVENANCE_INVALID",
        "vte provenance is not the exact approved record",
    )

    for relative, expected_sha256 in VTE_VENDORED_FILES.items():
        path = vendor_root / relative
        require(
            path.is_file() and not path.is_symlink(),
            "PROVENANCE_INVALID",
            f"vte vendored file is missing: {relative}",
        )
        require(
            hashlib.sha256(path.read_bytes()).hexdigest() == expected_sha256,
            "PROVENANCE_INVALID",
            f"vte vendored file digest mismatch: {relative}",
        )
    require(
        hashlib.sha256((vendor_root / VTE_PATCH_PATH).read_bytes()).hexdigest()
        == VTE_PATCH_SHA256,
        "PROVENANCE_INVALID",
        "vte patch digest mismatch",
    )
    require(
        hashlib.sha256((vendor_root / VTE_UNSAFE_AUDIT_PATH).read_bytes()).hexdigest()
        == VTE_UNSAFE_AUDIT_SHA256,
        "UNSAFE_RUST",
        "vte unsafe audit digest mismatch",
    )
    require(
        selected_source_digest(vendor_root, tuple(VTE_VENDORED_FILES))
        == f"sha256:{VTE_VENDOR_TREE_SHA256}",
        "PROVENANCE_INVALID",
        "vte selected source tree does not match sha256-tree-v1 provenance",
    )

    approved_unsafe_sites: set[tuple[Path, int]] = set()
    for site in VTE_UNSAFE_SITES:
        path = vendor_root / site["path"]
        start, end = site["candidate_lines"]
        require(
            source_line_digest(path, start, end) == site["context_sha256"],
            "UNSAFE_RUST",
            f"vte unsafe context digest mismatch: {site['path']}:{start}",
        )
        approved_unsafe_sites.add(
            (VTE_VENDOR_ROOT / site["path"], site["unsafe_line"])
        )

    unsafe_pattern = re.compile(r"\bunsafe\s+(?:extern|fn|impl|trait)|\bunsafe\s*\{")
    observed_unsafe_sites: set[tuple[Path, int]] = set()
    for relative in sorted(VTE_VENDORED_FILES):
        if not relative.endswith(".rs"):
            continue
        path = vendor_root / relative
        text = strip_rust_comments_and_strings(path.read_text(encoding="utf-8"))
        for match in unsafe_pattern.finditer(text):
            line = text.count("\n", 0, match.start()) + 1
            observed_unsafe_sites.add((VTE_VENDOR_ROOT / relative, line))
    require(
        observed_unsafe_sites == approved_unsafe_sites,
        "UNSAFE_RUST",
        "vte unsafe sites differ from the exact approved audit",
    )
    approved_sources = {
        VTE_VENDOR_ROOT / relative
        for relative in VTE_VENDORED_FILES
        if relative.endswith(".rs")
    }
    return approved_sources, approved_unsafe_sites
