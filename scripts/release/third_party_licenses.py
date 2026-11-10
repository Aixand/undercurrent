#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Collect reviewed dependency notices; check release inputs without network access."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tomllib

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = "undercurrent.dependency-licenses/v0.1.0-beta"
TARGETS = {
    "linux-x86_64-gnu": "x86_64-unknown-linux-gnu",
    "macos-arm64": "aarch64-apple-darwin",
    "macos-x86_64": "x86_64-apple-darwin",
    "windows-x86_64-msvc": "x86_64-pc-windows-msvc",
}
ROOT_PACKAGES = {"undercurrent-cli", "undercurrent-daemon", "undercurrent-python-bridge", "undercurrent-mcp",
                 "aixand-undercurrent", "aixand-undercurrent-protocol"}
OUTPUTS = ("inventory.json", "THIRD-PARTY-NOTICES.txt", "RUST-STDLIB.html")
LEGAL_NAME = re.compile(r"(^|[-_.])(licen[cs]es?|copying|copyright|notices?|authors)([-_.]|$)", re.I)
# Reviewed choices for the current graph. Unknown expressions require review.
CHOICES = {name: name for name in ("MIT", "Apache-2.0", "BSD-3-Clause", "BSD-2-Clause",
                                 "ISC", "Unicode-3.0", "Zlib", "CDLA-Permissive-2.0")}
CHOICES.update({
    "MIT OR Apache-2.0": "MIT", "Apache-2.0 OR MIT": "MIT", "Unlicense OR MIT": "MIT",
    "Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT": "MIT",
    "Apache-2.0 OR ISC OR MIT": "MIT", "Apache-2.0 OR BSL-1.0": "Apache-2.0",
    "BSD-2-Clause OR Apache-2.0": "Apache-2.0", "BSD-2-Clause OR Apache-2.0 OR MIT": "MIT",
    "(Apache-2.0 OR MIT) AND BSD-3-Clause": "MIT AND BSD-3-Clause",
    "(MIT OR Apache-2.0) AND Unicode-3.0": "MIT AND Unicode-3.0",
    "Apache-2.0 AND ISC": "Apache-2.0 AND ISC",
})


def digest(data):
    return hashlib.sha256(data).hexdigest()


def encoded(value):
    return (json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode()


def choice(expression):
    normalized = re.sub(r"\s*/\s*", " OR ", expression or "")
    if normalized not in CHOICES:
        raise ValueError(f"unreviewed license expression: {expression!r}")
    return CHOICES[normalized]


def reachable(metadata):
    packages = {p["id"]: p for p in metadata["packages"]}
    nodes = {n["id"]: n for n in metadata["resolve"]["nodes"]}
    roots = {p["name"] for p in packages.values()} & ROOT_PACKAGES
    if roots != ROOT_PACKAGES:
        raise ValueError("release package roots are missing from Cargo metadata")
    pending = [p["id"] for p in packages.values() if p["name"] in roots]
    visited = set()
    while pending:
        current = pending.pop()
        if current in visited:
            continue
        visited.add(current)
        pending.extend(d["pkg"] for d in nodes[current]["deps"]
                       if any(k["kind"] != "dev" for k in d["dep_kinds"]))
    return visited - set(metadata["workspace_members"])


def input_hashes(root=ROOT):
    canonical = (root / "LICENSE").read_bytes()
    for manifest in (root / "crates").glob("*/Cargo.toml"):
        license_path = manifest.parent / "LICENSE"
        if not license_path.is_file() or license_path.read_bytes() != canonical:
            raise ValueError(f"{license_path.relative_to(root)} must match the root LICENSE")
    names = {"Cargo.toml", "Cargo.lock", "LICENSE", "rust-toolchain.toml", "licenses/about.toml",
             "scripts/release/third_party_licenses.py", "python/pyproject.toml", "python/LICENSE"}
    for pattern in ("crates/*/Cargo.toml", "crates/*/LICENSE", "crates/*/NOTICE", "vendor/*/Cargo.toml",
                    "vendor/*/PROVENANCE.json", "vendor/*/conpty/PROVENANCE.json", "licenses/inputs/*"):
        names.update(p.relative_to(root).as_posix() for p in root.glob(pattern) if p.is_file())
    # Vendored sources can change without changing Cargo.lock.
    names.update(p.relative_to(root).as_posix() for p in (root / "vendor").rglob("*") if p.is_file())
    return {name: digest((root / name).read_bytes()) for name in sorted(names)}


def validate_inventory(value):
    if value.get("schema") != SCHEMA or value.get("platforms") != TARGETS:
        raise ValueError("invalid dependency inventory schema or platforms")
    packages = value.get("packages", [])
    keys = [(p["name"], p["version"], p["source"]) for p in packages]
    if not packages or len(keys) != len(set(keys)):
        raise ValueError("empty or duplicate dependency inventory")
    for p in packages:
        if p["selected_license"] != choice(p["declared_license"]) or not p["notice_ids"]:
            raise ValueError("dependency license choice or notices are missing")
    if set(value.get("output_hashes", {})) != set(OUTPUTS) - {"inventory.json"}:
        raise ValueError("license output coverage is incomplete")


def check(root=ROOT):
    folder = root / "licenses"
    value = json.loads((folder / "inventory.json").read_text())
    validate_inventory(value)
    if value["input_hashes"] != input_hashes(root):
        raise ValueError("dependency license inventory is stale; regenerate and review it")
    for name, expected in value["output_hashes"].items():
        if digest((folder / name).read_bytes()) != expected:
            raise ValueError(f"dependency notice content changed: {name}")
    return value


def generate(cargo_about):
    toolchain = tomllib.loads((ROOT / "rust-toolchain.toml").read_text())["toolchain"]["channel"]
    packages, platform_members = {}, {}
    for platform, triple in TARGETS.items():
        metadata = json.loads(subprocess.check_output([
            "cargo", f"+{toolchain}", "metadata", "--offline", "--locked", "--format-version", "1",
            "--filter-platform", triple,
        ], cwd=ROOT))
        members = reachable(metadata)
        platform_members[platform] = members
        packages.update((p["id"], p) for p in metadata["packages"] if p["id"] in members)
    version = subprocess.check_output([cargo_about, "--version"], text=True).strip()
    if version != "cargo-about 0.9.2":
        raise ValueError("use cargo-about 0.9.2, then review upgrades before regenerating")
    scan = json.loads(subprocess.check_output([
        cargo_about, "generate", "--offline", "--locked", "--fail", "--workspace",
        "--config", str(ROOT / "licenses/about.toml"), "--format", "json",
    ], cwd=ROOT, env=os.environ | {"RUSTUP_TOOLCHAIN": toolchain}))
    locked = {(p["name"], p["version"]): p for p in
              tomllib.loads((ROOT / "Cargo.lock").read_text())["package"]}
    notices = {}

    def add_notice(data, attribution):
        text = data.decode("utf-8").replace("\r\n", "\n").rstrip() + "\n"
        key = digest(text.encode())
        notice = notices.setdefault(key, {"text": text, "sources": set()})
        notice["sources"].add(attribution)
        return key

    records = []
    for package_id, package in sorted(packages.items(), key=lambda item: (item[1]["name"], item[1]["version"])):
        name, version = package["name"], package["version"]
        folder = Path(package["manifest_path"]).parent
        files = [p for p in folder.rglob("*") if p.is_file() and
                 (LEGAL_NAME.search(p.name) or any(LEGAL_NAME.fullmatch(part)
                                                  for part in p.relative_to(folder).parts[:-1]))]
        if not files:
            raise ValueError(f"{name} {version}: no upstream license files; manual review required")
        archive = None
        checksum = locked[(name, version)].get("checksum")
        if package["source"]:
            archive_path = folder.parents[2] / "cache" / folder.parent.name / f"{name}-{version}.crate"
            if not checksum or digest(archive_path.read_bytes()) != checksum:
                raise ValueError(f"{name}: crate archive does not match Cargo.lock")
            archive = tarfile.open(archive_path)

        def verified(path):
            data = path.read_bytes()
            if archive:
                member = archive.extractfile(f"{name}-{version}/{path.relative_to(folder).as_posix()}")
                if member is None or member.read() != data:
                    raise ValueError(f"{name}: extracted source differs from locked crate archive")
            return data

        ids, sources = set(), []
        try:
            if name == "libsqlite3-sys":
                sqlite_bytes = verified(folder / "sqlite3/sqlite3.c")
                sources.append({"path": "sqlite3/sqlite3.c", "sha256": digest(sqlite_bytes)})
            for path in sorted(files):
                data = verified(path)
                relative = path.relative_to(folder).as_posix()
                ids.add(add_notice(data, f"{name} {version}: {relative}"))
                sources.append({"path": relative, "sha256": digest(data)})
            # Retain source-header notices, notably ring's embedded ISC material.
            # Synthetic scanner fallback text is never substituted for upstream files.
            for license in scan["licenses"]:
                if not license.get("source_path") or not any(u["crate"]["id"] == package_id
                                                            for u in license["used_by"]):
                    continue
                path = Path(license["source_path"])
                if path.is_relative_to(folder):
                    data = verified(path)
                    ids.add(add_notice(license["text"].encode(),
                                       f"{name} {version}: extracted notice from {path.relative_to(folder).as_posix()}"))
                    sources.append({"path": path.relative_to(folder).as_posix(), "sha256": digest(data)})
        finally:
            if archive:
                archive.close()
        records.append({
            "name": name, "version": version, "declared_license": package["license"],
            "selected_license": choice(package["license"]),
            "source": package["source"] or "path:" + folder.relative_to(ROOT).as_posix(),
            "archive_sha256": checksum, "repository": package.get("repository"),
            "platforms": sorted(p for p, members in platform_members.items() if package_id in members),
            "notice_ids": sorted(ids), "source_files": sorted(sources, key=lambda s: (s["path"], s["sha256"])),
        })
    inputs = ROOT / "licenses/inputs"
    provenance = json.loads((inputs / "provenance.json").read_text())
    terminal = (inputs / "terminal-NOTICE.md").read_bytes()
    if digest(terminal) != provenance["terminal"]["sha256"] or provenance["rust"]["version"] != toolchain:
        raise ValueError("native notice inputs differ from their pinned provenance")
    add_notice(terminal, "Microsoft Terminal / ConPTY: upstream NOTICE.md (conservative superset)")
    for name, expected in provenance["rust"]["files"].items():
        data = (inputs / name).read_bytes()
        if digest(data) != expected:
            raise ValueError("Rust standard-library notice input changed")
        if name.endswith(".txt"):
            add_notice(data, f"Rust {toolchain}: {name}")
    sqlite = next(p for p in packages.values() if p["name"] == "libsqlite3-sys")
    sqlite_source = Path(sqlite["manifest_path"]).parent / "sqlite3/sqlite3.c"
    # Keep the public-domain dedication and exact bundled SQLite version visible.
    source = sqlite_source.read_text()
    dedication = re.search(r"/\*\n\*\* 2001 September 15.*?\*/", source, re.S)
    sqlite_version = re.search(r'#define SQLITE_VERSION\s+"([^"]+)"', source)
    if not dedication or not sqlite_version:
        raise ValueError("bundled SQLite dedication/version changed; review required")
    add_notice(dedication[0].encode(), f"SQLite {sqlite_version[1]}: bundled source public-domain dedication")
    introduction = (
        "Undercurrent third-party licenses and notices\n\n"
        "Generated from the locked supported-target dependency graphs. Includes build dependencies\n"
        "and conservative extra notices; this is not an assertion that every listed file is linked\n"
        "into every binary. Individual components retain their applicable licenses.\n"
        "See inventory.json for versions, selected license alternatives, and source hashes.\n"
        "See RUST-STDLIB.html for the pinned Rust standard library's complete upstream notices.\n\n"
    )
    chunks = [introduction]
    for key, notice in sorted(notices.items()):
        chunks.append(f"{'=' * 78}\nNotice {key}\n" + "\n".join(sorted(notice["sources"]))
                      + "\n\n" + notice["text"] + "\n")
    contents = {"THIRD-PARTY-NOTICES.txt": "".join(chunks).encode(),
                "RUST-STDLIB.html": (inputs / f"rust-{toolchain}-COPYRIGHT-library.html").read_bytes()}
    value = {
        "schema": SCHEMA, "platforms": TARGETS, "root_packages": sorted(ROOT_PACKAGES),
        "scope": "supported-target normal/build dependency closure; conservative workspace feature union",
        "generator": "cargo-about 0.9.2 plus verified original source notices",
        "rust_toolchain": toolchain, "input_hashes": input_hashes(), "packages": records,
        "native_components": {"sqlite": {"version": sqlite_version[1], "source_sha256": digest(sqlite_source.read_bytes()),
                                         "license": "public-domain dedication in bundled source"},
                              "conpty": json.loads((ROOT / "vendor/portable-pty/conpty/PROVENANCE.json").read_text())},
        "output_hashes": {name: digest(data) for name, data in contents.items()},
    }
    validate_inventory(value)
    contents["inventory.json"] = encoded(value)
    for name, data in contents.items():
        (ROOT / "licenses" / name).write_bytes(data)
    print(f"Collected {len(records)} Rust packages and {len(notices)} distinct notices.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("generate", "check"))
    parser.add_argument("--cargo-about", default="cargo-about")
    args = parser.parse_args()
    if args.command == "generate":
        generate(str(Path(args.cargo_about).resolve()) if "/" in args.cargo_about else args.cargo_about)
    value = check()
    print(f"Dependency license inputs and notice hashes passed ({len(value['packages'])} packages).")


if __name__ == "__main__":
    main()
