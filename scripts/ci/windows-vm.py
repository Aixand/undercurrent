#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Copy the working tree to a prepared Windows Server VM and retrieve CI evidence."""

import argparse
import base64
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
import tarfile
import uuid

ROOT = Path(__file__).resolve().parents[2]
SOURCE_RECORD = ".undercurrent-ci-source.json"


def snapshot(root, archive):
    """Include edits and untracked source; exclude ignored files and deleted paths."""
    paths = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=root
    ).decode("utf-8").split("\0")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root).decode().strip()
    digest = hashlib.sha256()
    files = []
    with tarfile.open(archive, "w:gz", dereference=False) as output:
        for relative in sorted(set(filter(None, paths))):
            path = root / relative
            if relative == SOURCE_RECORD:
                raise ValueError(f"reserved snapshot metadata path: {relative}")
            if not path.exists() and not path.is_symlink():
                continue
            if not path.is_file() or path.is_symlink():
                raise ValueError(f"snapshot requires regular source files: {relative}")
            # Read once so the fingerprint describes the exact archived bytes.
            data = path.read_bytes()
            info = output.gettarinfo(str(path), arcname=relative)
            info.size = len(data)
            output.addfile(info, io.BytesIO(data))
            file_hash = hashlib.sha256(data).hexdigest()
            digest.update(relative.encode("utf-8") + b"\0" + file_hash.encode() + b"\0")
            files.append({"path": relative, "sha256": file_hash})
        record = {"revision": revision, "source_sha256": digest.hexdigest(), "files": files}
        data = (json.dumps(record, indent=2) + "\n").encode("utf-8")
        info = tarfile.TarInfo(SOURCE_RECORD)
        info.size = len(data)
        output.addfile(info, io.BytesIO(data))
    return record


def connection_options(identity, port, known_hosts, *, scp=False):
    command = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=15",
               "-o", "StrictHostKeyChecking=yes", "-P" if scp else "-p", str(port)]
    if identity:
        command += ["-i", str(identity)]
    if known_hosts:
        command += ["-o", f"UserKnownHostsFile={known_hosts}"]
    return command


def remote(target, script, *, identity=None, port=22, known_hosts=None, check=True):
    command = ["ssh", *connection_options(identity, port, known_hosts)]
    script = "$ProgressPreference = 'SilentlyContinue'\n" + script
    encoded = base64.b64encode(script.encode("utf-16-le")).decode("ascii")
    command += [target, "powershell.exe", "-NoProfile", "-NonInteractive", "-OutputFormat", "Text",
                "-EncodedCommand", encoded]
    return subprocess.run(command, check=check).returncode


def copy(source, destination, *, identity=None, port=22, known_hosts=None):
    command = ["scp", *connection_options(identity, port, known_hosts, scp=True)]
    subprocess.run([*command, str(source), str(destination)], check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True, help="SSH user@host, with the host key already verified")
    parser.add_argument("--identity", type=Path, help="SSH private key for this VM")
    parser.add_argument("--port", type=int, default=22)
    parser.add_argument("--known-hosts", type=Path, help="file containing the verified VM host key")
    parser.add_argument("--guest-root", default="C:/undercurrent-ci")
    parser.add_argument("--stage", default="all")
    parser.add_argument("--root", type=Path, default=ROOT, help="working tree to test, including local edits")
    parser.add_argument("--require-build", default="20348.5622", help="required Windows build and revision")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("port must be between 1 and 65535")
    connection = {"identity": args.identity, "port": args.port, "known_hosts": args.known_hosts}
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.@-]*", args.target):
        parser.error("target must be an SSH hostname/alias or user@hostname")
    if not re.fullmatch(r"[A-Za-z]:/[A-Za-z0-9_/-]+", args.guest_root) or ".." in args.guest_root:
        parser.error("guest root must be a simple absolute Windows path")
    if not re.fullmatch(r"[a-z-]+", args.stage) or not re.fullmatch(r"\d+\.\d+", args.require_build):
        parser.error("invalid stage or Windows build")
    run_id = uuid.uuid4().hex
    local = ROOT / "target/windows-vm" / run_id
    local.mkdir(parents=True)
    guest = f"{args.guest_root}/{run_id}"
    archive = local / "source.tar.gz"
    record = snapshot(args.root.resolve(), archive)
    (local / "source.json").write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    print(f"Source: {record['revision']} + working tree; fingerprint {record['source_sha256']}", flush=True)
    # Confirm the actual OS and token, not merely an SSH hostname or workflow label.
    remote(args.target, rf"""
$ErrorActionPreference = 'Stop'
$version = Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion'
$actual = "$($version.CurrentBuildNumber).$($version.UBR)"
if ($actual -ne '{args.require_build}') {{ throw "Expected Windows {args.require_build}; found $actual" }}
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = [Security.Principal.WindowsPrincipal]::new($identity)
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {{
    throw 'The VM test account must run elevated to reproduce the hosted runner'
}}
New-Item -ItemType Directory -Path '{guest}/source' | Out-Null
""", **connection)
    copy(archive, f"{args.target}:{guest}/source.tar.gz", **connection)
    test_status = 1
    evidence_status = 1
    try:
        test_status = remote(args.target, rf"""
$ErrorActionPreference = 'Stop'
$env:PATH = "$env:USERPROFILE\.cargo\bin;C:\Program Files\Git\cmd;C:\Program Files\PowerShell\7;C:\Python313;C:\Python313\Scripts;$env:PATH"
$env:CARGO_BUILD_JOBS = '2'
tar.exe -xzf '{guest}/source.tar.gz' -C '{guest}/source'
if ($LASTEXITCODE -ne 0) {{ exit $LASTEXITCODE }}
Set-Location '{guest}/source'
git init --quiet
if ($LASTEXITCODE -ne 0) {{ exit $LASTEXITCODE }}
git add --all
if ($LASTEXITCODE -ne 0) {{ exit $LASTEXITCODE }}
git -c user.name='Undercurrent CI' -c user.email='ci@localhost' commit --quiet -m 'Local validation snapshot'
if ($LASTEXITCODE -ne 0) {{ exit $LASTEXITCODE }}
python scripts/ci/windows-native.py --stage '{args.stage}'
exit $LASTEXITCODE
""", **connection, check=False)
    finally:
        evidence_status = remote(args.target, rf"""
$ErrorActionPreference = 'Stop'
Set-Location '{guest}/source'
$paths = @(Get-ChildItem -LiteralPath target -ErrorAction SilentlyContinue |
    Where-Object {{ $_.Name -eq 'windows-ci' -or $_.Name -like 'windows-native-smoke-*' -or
        $_.Name -like 'windows-attach-smoke-*' -or $_.Name -eq 'platform-bundles' -or
        $_.Name -eq 'windows-release-adversarial.json' }} |
    ForEach-Object {{ 'target/' + $_.Name }})
if ($paths.Count -eq 0) {{ throw 'No test evidence was produced' }}
tar.exe -czf '{guest}/evidence.tar.gz' @paths
exit $LASTEXITCODE
""", **connection, check=False)
        if evidence_status == 0:
            copy(f"{args.target}:{guest}/evidence.tar.gz", local / "evidence.tar.gz", **connection)
        (local / "result.json").write_text(json.dumps({
            "test_exit_code": test_status, "evidence_exit_code": evidence_status,
            "source_sha256": record["source_sha256"], "required_windows_build": args.require_build,
        }, indent=2) + "\n", encoding="utf-8")
        print(f"VM evidence: {local}", flush=True)
    return test_status or evidence_status


if __name__ == "__main__":
    raise SystemExit(main())
