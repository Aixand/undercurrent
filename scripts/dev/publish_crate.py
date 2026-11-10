#!/usr/bin/env python3
"""Explicit crates.io adapter. Only run in an authorized release publisher job."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import tarfile
import tempfile
import time
from urllib.error import URLError
from urllib.parse import quote
from urllib.request import urlopen

from cli import dispatch
from core import Failure, Project, require
from packages import LIMIT, inspect_tar, validate


def inventory(path):
    inspect_tar(path)
    with tarfile.open(path) as archive:
        result = {}
        for entry in archive:
            if entry.isfile():
                with archive.extractfile(entry) as stream:
                    result[entry.name] = hashlib.file_digest(
                        stream, "sha256"
                    ).hexdigest()
        return result


def confirm(name, version, expected):
    url = f"https://crates.io/api/v1/crates/{quote(name, safe='')}/{quote(version, safe='')}/download"
    with tempfile.TemporaryDirectory(prefix="crate-confirm-") as temporary:
        destination = Path(temporary) / "published.crate"
        for attempt in range(5):
            try:
                with (
                    urlopen(url, timeout=30) as response,
                    destination.open("wb") as output,
                ):
                    size = 0
                    while block := response.read(1024 * 1024):
                        size += len(block)
                        require(size <= LIMIT, "Registry download exceeds size limit")
                        output.write(block)
                require(
                    inventory(destination) == expected,
                    "Published crate contents differ from the verified package",
                )
                return
            except URLError:
                if attempt == 4:
                    raise
                time.sleep(3)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", type=Path, required=True)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--registry", choices=["crates-io"], required=True)
    args = parser.parse_args()
    require(
        bool(os.environ.get("CARGO_REGISTRY_TOKEN")),
        "This explicit publisher requires the release job's short-lived CARGO_REGISTRY_TOKEN",
    )
    project = Project(args.project)
    require(
        project.profile == "rust-library",
        "v1 crate publisher is for selected library crates",
    )
    # Use the same checked source/tag preflight as local verification.
    from cli import parser as command_parser

    checked = dispatch(
        command_parser().parse_args(
            ["--project", str(project.root), "release-check", "--version", args.tag]
        )
    )
    artifact, manifest = validate(args.artifact, project)
    require(
        manifest["version"] == project.version
        and manifest["source"] == checked["source"],
        "Prepared crate source differs from the current release revision/inputs",
    )
    expected = inventory(artifact / manifest["primary"])
    features = ["--features", ",".join(project.features)] if project.features else []
    project.cargo("package", *features)
    metadata, _ = project.cargo_info()
    rebuilt = (
        Path(metadata["target_directory"])
        / "package"
        / f"{project.name}-{project.version}.crate"
    )
    require(
        inventory(rebuilt) == expected,
        "Publisher packaging differs from the verified crate contents",
    )
    project.cargo("publish", "--registry", args.registry, *features)
    require(
        inventory(rebuilt) == expected,
        "Cargo's published package differs; inspect the partial publication",
    )
    confirm(project.name, project.version, expected)
    print(
        json.dumps(
            {
                "published": project.name,
                "version": project.version,
                "registry_confirmed": True,
            }
        )
    )


if __name__ == "__main__":
    try:
        main()
    except (Failure, OSError, ValueError, tarfile.TarError) as error:
        import sys

        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
