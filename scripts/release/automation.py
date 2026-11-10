#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate version tags, assemble downloads, and publish qualified releases."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile
import tomllib
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[2]
# Dependency order, including dependencies used only on Windows.
CRATE_DIRECTORIES = {
    "aixand-undercurrent-foundation": "undercurrent-foundation",
    "aixand-undercurrent-wire": "undercurrent-wire",
    "aixand-undercurrent-protocol": "undercurrent-protocol",
    "aixand-undercurrent-multiplex": "undercurrent-multiplex",
    "aixand-undercurrent-platform-windows": "undercurrent-platform-windows",
    "aixand-undercurrent-transport-core": "undercurrent-transport-core",
    "aixand-undercurrent-transport-local": "undercurrent-transport-local",
    "aixand-undercurrent": "undercurrent-sdk",
}
CRATES = tuple(CRATE_DIRECTORIES)

TARGETS = (
    "linux-x86_64-gnu",
    "macos-arm64",
    "macos-x86_64",
    "windows-x86_64-msvc",
)
VERSION = re.compile(
    r"v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-((?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)"
    r"(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*))?\Z"
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def run(argv, *, root=ROOT):
    return subprocess.check_output(argv, cwd=root, text=True).strip()


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def tag_version(tag):
    require(isinstance(tag, str) and VERSION.fullmatch(tag), "Expected a v<SemVer> tag")
    return tag[1:]


def dependencies(manifest):
    for kind in ("dependencies", "build-dependencies", "dev-dependencies"):
        yield from manifest.get(kind, {}).items()
    for target in manifest.get("target", {}).values():
        yield from dependencies(target)


def validate_versions(tag, root=ROOT):
    version = tag_version(tag)
    cargo = tomllib.loads((root / "Cargo.toml").read_text())
    installation = tomllib.loads((root / "installation.toml").read_text())
    versions = [cargo["workspace"]["package"]["version"], installation["workspace"]["version"]]
    versions.extend(p["version"] for p in installation["products"].values())
    versions.append(read(root / "undercurrent-package.json")["version"])
    require(all(v == version for v in versions), "Tag, Cargo, product, and package versions differ")
    seen = set()
    for name in CRATES:
        manifest = tomllib.loads((root / "crates" / CRATE_DIRECTORIES[name] / "Cargo.toml").read_text())
        package = manifest["package"]
        require(package["name"] == name, "Publication crate name differs from configured registry name")
        require(package.get("publish") is not False, f"{name} disables publishing")
        declared = package["version"]
        require(declared == version or declared == {"workspace": True}, f"{name} version differs")
        for dep_name, spec in dependencies(manifest):
            if not isinstance(spec, dict):
                continue
            if spec.get("workspace"):
                spec = cargo["workspace"]["dependencies"][dep_name]
            if "path" in spec:
                dependency = spec.get("package", dep_name)
                require(dependency in seen, f"{name}: unpublished or unordered dependency {dependency}")
                require(spec.get("version") == "=" + version, f"{name}: dependency version differs")
        seen.add(name)
    return version


def check(tag, *, root=ROOT):
    version = validate_versions(tag, root)
    commit = run(["git", "rev-parse", "HEAD"], root=root)
    require(run(["git", "rev-parse", f"refs/tags/{tag}^{{commit}}"], root=root) == commit,
            "Tag does not point to the checked-out commit")
    subprocess.run(["git", "merge-base", "--is-ancestor", commit, "refs/remotes/origin/main"],
                   cwd=root, check=True)
    require(not run(["git", "status", "--porcelain", "--untracked-files=normal"], root=root),
            "Release sources must be clean")
    return {"tag": tag, "version": version, "commit": commit, "prerelease": "-" in version}


def archive_names(version):
    return {
        f"undercurrent-{version}-{target}{suffix}.{('zip' if target.startswith('windows') else 'tar.gz')}"
        for target in TARGETS for suffix in ("", "-sdk")
    }


def archive(tag, root=ROOT):
    version = validate_versions(tag, root)
    dist = root / "dist"
    descriptor = read(dist / "handoff/product-descriptor.json")
    target = descriptor["target"]
    require(target in TARGETS and descriptor["version"] == version, "Wrong release artifact")
    commit = run(["git", "rev-parse", "HEAD"], root=root)
    for path in (dist / "handoff/artifacts/release-manifest.json", dist / "sdk/release-manifest.json"):
        manifest = read(path)
        source = manifest["sources"]["project"]
        require(manifest["target"] == target and manifest["version"] == version
                and source["commit"] == commit and source["dirty"] is False,
                "Artifact source differs from this clean release commit")
    require(sha(dist / "handoff/artifacts/release-manifest.json") == descriptor["manifest_sha256"],
            "Product manifest differs from its descriptor")
    sdk_proof = read(dist / "sdk-verification.json")
    require(sdk_proof["verified"] is True and sdk_proof["installation_tested"] is True
            and sdk_proof["target"] == target
            and sdk_proof["manifest_sha256"] == sha(dist / "sdk/release-manifest.json"),
            "SDK consumer verification does not match this artifact")
    lifecycle = read(dist / "lifecycle.json")
    require(lifecycle["verified"] is True and lifecycle["target"] == target
            and lifecycle["manifest_sha256"] == descriptor["manifest_sha256"]
            and lifecycle["bootstrap_without_system_python"] is True
            and lifecycle["upgrade_assembly_fixture_tested"] is True,
            "Missing matching bootstrap and upgrade qualification")
    require(descriptor["verification"]["runtime"] is True, "Artifact installation was not verified")
    destination = dist / "assets"
    destination.mkdir(exist_ok=False)
    metadata = {
        "tag": tag, "version": version, "target": target,
        "commit": commit,
        "compiler": run(["rustc", "--version"], root=root),
        "license_inventory_sha256": sha(root / "licenses/inventory.json"),
    }
    with tempfile.TemporaryDirectory() as temporary:
        for suffix in ("", "-sdk"):
            name = f"undercurrent-{version}-{target}{suffix}"
            stage = Path(temporary) / name
            stage.mkdir()
            if suffix:
                shutil.copytree(dist / "sdk", stage / "artifacts")
                shutil.copyfile(dist / "sdk-verification.json", stage / "verification-report.json")
            else:
                shutil.copytree(dist / "handoff", stage, dirs_exist_ok=True)
                shutil.copyfile(dist / "lifecycle.json", stage / "lifecycle.json")
            shutil.copytree(dist / "bootstrap", stage / "bootstrap")
            guide = f"https://github.com/Aixand/undercurrent/blob/{tag}/docs/getting-started/product-installer.md"
            product_instructions = (
                "Pass the artifacts directory to --artifact and an absolute installation store to --prefix.\n"
                "Start with the plan operation; read the license before accepting its digest.\n"
            )
            sdk_instructions = (
                "This is an SDK component for an application's Python environment, not a product store.\n"
                "Run bootstrap verification: verify --artifact artifacts --install\n"
                "Install the wheel under artifacts/component/wheelhouse/ using your environment's pip.\n"
                "The SDK connects to a separately installed Undercurrent runtime.\n"
            )
            (stage / "INSTALL.txt").write_text(
                "Undercurrent installation\n\n"
                "Keep this directory together after extracting the download.\n"
                "Use bootstrap/bootstrap.sh (Linux/macOS) or bootstrap/bootstrap.ps1 (Windows).\n"
                + (sdk_instructions if suffix else product_instructions)
                + "The bootstrap downloads its pinned helper interpreter.\n"
                f"Commands and license acceptance: {guide}\n"
            )
            (stage / "release-metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
            fmt = "zip" if target.startswith("windows") else "gztar"
            shutil.make_archive(str(destination / name), fmt, temporary, name)


def collect(tag, assets):
    version = tag_version(tag)
    files = list(assets.iterdir())
    require(all(p.is_file() and not p.is_symlink() for p in files), "Invalid release asset")
    require({p.name for p in files} == archive_names(version), "Release asset set is incomplete or unexpected")
    (assets / "SHA256SUMS").write_text("".join(f"{sha(p)}  {p.name}\n" for p in sorted(files)))


def crate_files(data):
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        result = {}
        for member in archive.getmembers():
            if member.isdir():
                continue
            require(member.isfile(), "Unexpected crate archive entry")
            name = member.name.partition("/")[2]
            require(name and name not in result, "Duplicate or invalid crate entry")
            result[name] = archive.extractfile(member).read()
        return result


def verify_existing_crate(local, remote, commit):
    left, right = crate_files(local), crate_files(remote)
    require(json.loads(right[".cargo_vcs_info.json"])["git"]["sha1"] == commit,
            "Existing crate was published from another commit")
    # Cargo's temporary registry in multi-package dry runs changes lockfile sources.
    # Every other packaged byte, including normalized manifests and VCS info, must match.
    left.pop("Cargo.lock", None)
    right.pop("Cargo.lock", None)
    require(left == right, "Existing crate content differs; use a new release version")


def registry_version(name, version):
    request = Request(f"https://crates.io/api/v1/crates/{name}/{version}",
                      headers={"User-Agent": "undercurrent-release (https://github.com/Aixand/undercurrent)"})
    try:
        with urlopen(request, timeout=60) as response:
            return json.load(response)["version"]
    except HTTPError as error:
        if error.code == 404:
            return None
        raise


def cargo_args(command, names):
    return ["cargo", "+1.95.0", command, "--registry", "crates-io", "--locked"] + [
        arg for name in names for arg in ("-p", name)
    ]


def crates(tag, *, publish=False):
    release = check(tag)
    # Cargo packages/verifies the selected graph together, including new dependencies.
    subprocess.run(cargo_args("publish", CRATES) + ["--dry-run"], cwd=ROOT, check=True)
    if not publish:
        return
    require(os.environ.get("CARGO_REGISTRY_TOKEN"), "Set the CARGO_REGISTRY_TOKEN Actions secret")
    pending = []
    for name in CRATES:
        existing = registry_version(name, release["version"])
        if existing is None:
            pending.append(name)
            continue
        require(not existing["yanked"], f"{name}: existing version is yanked")
        with urlopen(f"https://static.crates.io/crates/{name}/{name}-{release['version']}.crate", timeout=60) as response:
            remote = response.read()
        require(hashlib.sha256(remote).hexdigest() == existing["checksum"], "Registry checksum differs")
        local = (ROOT / "target/package" / f"{name}-{release['version']}.crate").read_bytes()
        verify_existing_crate(local, remote, release["commit"])
        print(f"Already published and source verified: {name}", flush=True)
    if pending:
        # Cargo orders the selected dependency graph and waits for index availability.
        subprocess.run(cargo_args("publish", pending), cwd=ROOT, check=True)


def github_release(tag, assets, *, publish=False):
    release = check(tag)
    expected = archive_names(release["version"]) | {"SHA256SUMS"}
    require({p.name for p in assets.iterdir()} == expected, "Unexpected release downloads")
    sums = "".join(f"{sha(p)}  {p.name}\n" for p in sorted(assets.iterdir()) if p.name != "SHA256SUMS")
    require((assets / "SHA256SUMS").read_text() == sums, "Release checksums differ")
    repository = os.environ["GITHUB_REPOSITORY"]
    require(repository == "Aixand/undercurrent", "Publication is restricted to Aixand/undercurrent")
    pages = json.loads(run(["gh", "api", "--paginate", "--slurp", f"repos/{repository}/releases?per_page=100"]))
    existing = next((item for page in pages for item in page if item["tag_name"] == tag), None)
    if existing:
        require(existing["draft"], "Release is already public; do not replace its assets")
        require(existing["target_commitish"] == release["commit"], "Draft belongs to another source commit")
    else:
        require(not publish, "Prepare the draft release first")
        argv = ["gh", "release", "create", tag, "--verify-tag", "--draft", "--target", release["commit"],
                "--title", tag, "--generate-notes"]
        if release["prerelease"]:
            argv.append("--prerelease")
        run(argv)
    if not publish:
        # Draft assets may be replaced on retries; public assets are never replaced.
        run(["gh", "release", "upload", tag, "--clobber", *map(str, sorted(assets.iterdir()))])
    else:
        # Confirm all draft assets still match the files prepared by this run.
        with tempfile.TemporaryDirectory() as temporary:
            run(["gh", "release", "download", tag, "--dir", temporary])
            downloaded = Path(temporary)
            require({p.name for p in downloaded.iterdir()} == expected, "Draft asset set differs")
            require(all(sha(downloaded / p.name) == sha(p) for p in assets.iterdir()), "Draft assets differ")
        argv = ["gh", "release", "edit", tag, "--draft=false"]
        if release["prerelease"]:
            argv += ["--prerelease", "--latest=false"]
        run(argv)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("check", "archive", "collect", "crates-check", "publish-crates", "draft", "publish-release"))
    parser.add_argument("--tag", default=os.environ.get("RELEASE_TAG"))
    parser.add_argument("--assets", type=Path, default=ROOT / "dist/assets")
    args = parser.parse_args()
    if args.operation == "check":
        print(json.dumps(check(args.tag)))
    elif args.operation == "archive":
        archive(args.tag)
    elif args.operation == "collect":
        collect(args.tag, args.assets)
    elif args.operation in {"crates-check", "publish-crates"}:
        crates(args.tag, publish=args.operation == "publish-crates")
    else:
        github_release(args.tag, args.assets, publish=args.operation == "publish-release")


if __name__ == "__main__":
    main()
