"""Verified runtime archives, installed separately from replaceable products."""

from __future__ import annotations

import os
from pathlib import Path, PurePosixPath
import shutil
import tarfile
import tempfile
import urllib.parse
import urllib.request
import zipfile

from core import (
    digest,
    filesystem_path,
    read_json,
    relative,
    require,
    source_file,
    write_json,
)
from packages import LIMIT, descriptor


def download(url, destination, expected, *, allow_local=False, progress=None):
    parsed = urllib.parse.urlsplit(url)
    require(
        not parsed.username and not parsed.password and not parsed.fragment,
        "Runtime URL must not contain credentials or fragments",
    )
    require(
        parsed.scheme == "https"
        or (
            allow_local
            and parsed.scheme == "http"
            and parsed.hostname in {"localhost", "127.0.0.1", "::1"}
        ),
        "Runtime downloads require HTTPS (or explicit loopback testing)",
    )
    size = 0
    with (
        urllib.request.urlopen(url, timeout=60) as response,
        destination.open("xb") as output,
    ):
        final = urllib.parse.urlsplit(response.geturl())
        require(
            final.scheme == "https"
            or (
                allow_local
                and final.scheme == "http"
                and final.hostname in {"localhost", "127.0.0.1", "::1"}
            ),
            "Runtime redirect left the allowed transport",
        )
        length = response.headers.get("Content-Length")
        total = int(length) if length and length.isdecimal() else None
        if progress:
            progress(0, total)
        while chunk := response.read(1024 * 1024):
            size += len(chunk)
            require(size <= LIMIT, "Runtime download exceeds size limit")
            output.write(chunk)
            if progress:
                progress(size, total)
    require(digest(destination) == expected, "Runtime archive checksum mismatch")


def unpack(archive_path, destination, record):
    """Bounded extraction; resolve internal archive links without OS symlink privileges."""
    members, links = {}, {}
    total = 0
    strip = record.get("strip", 0)
    archive = (
        zipfile.ZipFile(archive_path)
        if record.get("format", "tar") == "zip"
        else tarfile.open(archive_path, "r:*")
    )
    try:
        entries = (
            archive.infolist()
            if isinstance(archive, zipfile.ZipFile)
            else archive.getmembers()
        )
        require(len(entries) <= 100000, "Too many runtime archive entries")
        for entry in entries:
            raw = entry.filename if isinstance(archive, zipfile.ZipFile) else entry.name
            raw = raw.rstrip("/")
            while raw.startswith("./"):
                raw = raw[2:]
            if not raw:
                continue
            relative(raw)
            parts = PurePosixPath(raw).parts
            if len(parts) <= strip:
                continue
            name = "/".join(parts[strip:])
            require(
                name not in members and name not in links,
                "Duplicate runtime archive entry",
            )
            if isinstance(archive, zipfile.ZipFile):
                mode = entry.external_attr >> 16
                require(
                    mode & 0o170000 != 0o120000, "Runtime zip symlinks are unsupported"
                )
                if entry.is_dir():
                    continue
                size = entry.file_size
            else:
                if entry.isdir():
                    continue
                if entry.issym() or entry.islnk():
                    target = PurePosixPath(entry.linkname)
                    require(
                        not target.is_absolute() and "\\" not in entry.linkname,
                        "Runtime archive link escapes root",
                    )
                    combined = (
                        PurePosixPath(name).parent / target
                        if entry.issym()
                        else PurePosixPath(*target.parts[strip:])
                    )
                    normalized = []
                    for part in combined.parts:
                        if part == "..":
                            require(normalized, "Runtime archive link escapes root")
                            normalized.pop()
                        elif part != ".":
                            normalized.append(part)
                    require(normalized, "Runtime archive link targets root")
                    links[name] = "/".join(normalized)
                    continue
                require(entry.isfile(), "Special runtime archive entry")
                size = entry.size
            total += size
            require(
                0 <= size and total <= LIMIT,
                "Runtime archive exceeds unpacked size limit",
            )
            members[name] = entry
        for name, entry in members.items():
            path = destination / relative(name)
            path.parent.mkdir(parents=True, exist_ok=True)
            source = (
                archive.open(entry)
                if isinstance(archive, zipfile.ZipFile)
                else archive.extractfile(entry)
            )
            with source, path.open("xb") as output:
                shutil.copyfileobj(source, output)
            mode = (
                entry.external_attr >> 16
                if isinstance(archive, zipfile.ZipFile)
                else entry.mode
            )
            path.chmod(0o755 if mode & 0o111 else 0o644)

        def resolve(name, visiting):
            require(name not in visiting, "Runtime archive link cycle")
            if name in links:
                return resolve(links[name], visiting | {name})
            path = destination / relative(name)
            require(path.exists(), "Runtime archive link target is missing")
            return path

        for name in links:
            path = destination / relative(name)
            require(not path.exists(), "Conflicting runtime link destination")
            source = resolve(name, set())
            path.parent.mkdir(parents=True, exist_ok=True)
            copied = (
                sum(p.stat().st_size for p in source.rglob("*") if p.is_file())
                if source.is_dir()
                else source.stat().st_size
            )
            total += copied
            require(total <= LIMIT, "Expanded runtime links exceed size limit")
            if source.is_dir():
                require(
                    not path.is_relative_to(source), "Recursive runtime directory link"
                )
                shutil.copytree(source, path)
            else:
                shutil.copyfile(source, path)
                path.chmod(source.stat().st_mode & 0o777)
    finally:
        archive.close()


def provision(store, record, artifact, *, allow_local=False, progress=None):
    executable_root = store / "runtimes" / record["sha256"]
    root = filesystem_path(executable_root)
    marker = root / "runtime-receipt.json"
    if root.exists():
        require(
            not root.is_symlink() and marker.is_file() and not marker.is_symlink(),
            "Unowned runtime directory",
        )
        receipt = read_json(marker)
        require(
            receipt.get("schema") == 2
            and receipt["archive_sha256"] == record["sha256"],
            "Runtime receipt mismatch",
        )
        actual = {
            p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()
        }
        require(
            actual == set(receipt["files"]) | {"runtime-receipt.json"},
            "Runtime inventory changed",
        )
        for name, info in receipt["files"].items():
            require(
                descriptor(source_file(root, name)) == info, "Installed runtime changed"
            )
    else:
        root.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix=".runtime-", dir=root.parent
        ) as temporary:
            temporary = Path(temporary)
            source = (
                source_file(artifact, record["artifact"])
                if record.get("artifact")
                else temporary / "download"
            )
            if not record.get("artifact"):
                download(
                    record["url"],
                    source,
                    record["sha256"],
                    allow_local=allow_local,
                    progress=progress,
                )
            require(
                digest(source) == record["sha256"], "Runtime archive checksum mismatch"
            )
            payload = temporary / "payload"
            payload.mkdir()
            unpack(source, payload, record)
            executable = source_file(payload, record["executable"])
            require(
                os.name == "nt" or os.access(executable, os.X_OK),
                "Runtime executable is not executable",
            )
            files = {
                p.relative_to(payload).as_posix(): descriptor(p)
                for p in sorted(payload.rglob("*"))
                if p.is_file()
            }
            write_json(
                payload / "runtime-receipt.json",
                {"schema": 2, "archive_sha256": record["sha256"], "files": files},
            )
            payload.rename(root)
    return executable_root / relative(record["executable"])
