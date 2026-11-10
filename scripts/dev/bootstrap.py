#!/usr/bin/env python3
"""Build a pinned, Python-free first-stage launcher for the portable artifact helper."""

from __future__ import annotations

import argparse
import base64
from pathlib import Path
import shlex
import sys
import tempfile
import urllib.parse
import zipapp

from catalog import TARGETS
from core import digest, read_json, require, write_json

SHELL = r"""#!/bin/sh
set -e
case "$(uname -s):$(uname -m)" in
    Linux:x86_64) actual=linux-x86_64-gnu ;;
    Linux:aarch64|Linux:arm64) actual=linux-arm64-gnu ;;
    Darwin:x86_64) actual=macos-x86_64 ;;
    Darwin:arm64) actual=macos-arm64 ;;
    *) printf '%s\n' 'Unsupported bootstrap host' >&2; exit 1 ;;
esac
expected=@@TARGET@@
[ "$actual" = "$expected" ] || { printf '%s\n' 'Bootstrap target differs from host' >&2; exit 1; }
cache="$HOME/.local/share/aixand/installer"
[ -z "$XDG_DATA_HOME" ] || cache="$XDG_DATA_HOME/aixand/installer"
[ -z "$AIXAND_INSTALLER_CACHE" ] || cache="$AIXAND_INSTALLER_CACHE"
case "$cache" in /*) ;; *) printf '%s\n' 'Installer cache must be absolute' >&2; exit 1 ;; esac
[ ! -L "$cache" ] || { printf '%s\n' 'Redirected installer cache' >&2; exit 1; }
mkdir -p "$cache"
runtime="$cache/@@SHA@@"
python="$runtime/@@EXECUTABLE@@"
checksum() {
    if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1"
    else shasum -a 256 "$1"
    fi
}
if [ ! -x "$python" ]; then
    lock="$cache/@@SHA@@.lock"
    mkdir "$lock" 2>/dev/null || { printf '%s\n' 'Another runtime bootstrap is running' >&2; exit 1; }
    temporary=$(mktemp -d "$cache/.prepare.XXXXXXXX")
    trap 'rm -rf "$temporary"; rmdir "$lock"' 0
    curl -fsSL @@PROTOCOL@@ @@URL@@ -o "$temporary/archive"
    actual=$(checksum "$temporary/archive")
    case "$actual" in @@SHA@@\ *) ;; *) printf '%s\n' 'Runtime checksum mismatch' >&2; exit 1 ;; esac
    mkdir "$temporary/payload"
    @@EXTRACT@@
    [ -x "$temporary/payload/@@EXECUTABLE@@" ] || { printf '%s\n' 'Missing helper interpreter' >&2; exit 1; }
    [ ! -e "$runtime" ] || { printf '%s\n' 'Incomplete cached runtime; use another cache directory' >&2; exit 1; }
    mv "$temporary/payload" "$runtime"
    rm -rf "$temporary"
    rmdir "$lock"
    trap - 0
fi
engine="$runtime/installer-@@ENGINE_SHA@@.pyz"
if [ ! -f "$engine" ]; then
    "$python" -I -c 'import base64,hashlib,os,sys,tempfile; data=base64.b64decode(sys.argv[2]); assert hashlib.sha256(data).hexdigest()==sys.argv[3]; fd,path=tempfile.mkstemp(dir=os.path.dirname(sys.argv[1])); stream=os.fdopen(fd,"wb"); stream.write(data); stream.close(); os.replace(path,sys.argv[1])' "$engine" '@@ENGINE@@' '@@ENGINE_SHA@@'
fi
actual=$(checksum "$engine")
case "$actual" in @@ENGINE_SHA@@\ *) ;; *) printf '%s\n' 'Helper checksum mismatch' >&2; exit 1 ;; esac
exec "$python" -I -B "$engine" "$@"
"""

POWERSHELL = r"""#requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$architecture = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString().ToLowerInvariant()
$expectedArchitecture = '@@ARCH@@'
if ($architecture -ne $expectedArchitecture) { throw 'Bootstrap target differs from host' }
$cache = Join-Path $env:LOCALAPPDATA 'Aixand\Installer'
if ($env:AIXAND_INSTALLER_CACHE) { $cache = $env:AIXAND_INSTALLER_CACHE }
if (-not [IO.Path]::IsPathRooted($cache)) { throw 'Installer cache must be absolute' }
[IO.Directory]::CreateDirectory($cache) | Out-Null
$runtime = Join-Path $cache '@@SHA@@'
$python = Join-Path $runtime '@@EXECUTABLE@@'
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    $lockPath = Join-Path $cache '@@SHA@@.lock'
    try { $lock = [IO.File]::Open($lockPath, 'OpenOrCreate', 'ReadWrite', 'None') }
    catch { throw 'Another runtime bootstrap is running' }
    $temporary = Join-Path $cache ('.prepare-' + [guid]::NewGuid().ToString('N'))
    [IO.Directory]::CreateDirectory($temporary) | Out-Null
    try {
        $archive = Join-Path $temporary 'runtime.zip'
        Invoke-WebRequest -Uri '@@URL@@' -OutFile $archive -UseBasicParsing
        if ((Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash.ToLowerInvariant() -ne '@@SHA@@') { throw 'Runtime checksum mismatch' }
        $payload = Join-Path $temporary 'payload'
        [IO.Directory]::CreateDirectory($payload) | Out-Null
        @@EXTRACT@@
        if (-not (Test-Path -LiteralPath (Join-Path $payload '@@EXECUTABLE@@') -PathType Leaf)) { throw 'Missing helper interpreter' }
        if (Test-Path -LiteralPath $runtime) { throw 'Incomplete cached runtime; use another cache directory' }
        [IO.Directory]::Move($payload, $runtime)
    } finally {
        if (Test-Path -LiteralPath $temporary) { Remove-Item -LiteralPath $temporary -Recurse -Force }
        $lock.Dispose()
    }
}
$engine = Join-Path $runtime 'installer-@@ENGINE_SHA@@.pyz'
if (-not (Test-Path -LiteralPath $engine -PathType Leaf)) {
    $data = [Convert]::FromBase64String('@@ENGINE@@')
    $temporary = $engine + '.' + [guid]::NewGuid().ToString('N')
    [IO.File]::WriteAllBytes($temporary, $data)
    [IO.File]::Move($temporary, $engine)
}
if ((Get-FileHash -LiteralPath $engine -Algorithm SHA256).Hash.ToLowerInvariant() -ne '@@ENGINE_SHA@@') { throw 'Helper checksum mismatch' }
& $python -I -B $engine @args
exit $LASTEXITCODE
"""


def generate(record, output, *, allow_local=False):
    from core import relative
    import re

    require(
        record.get("target") in TARGETS and record.get("role") in {"helper", "python"},
        "Bootstrap requires a declared Python/helper runtime target",
    )
    require(
        re.fullmatch(r"[0-9a-f]{64}", record.get("sha256", "")),
        "Bootstrap needs SHA-256",
    )
    relative(record["executable"])
    require(
        not any(c in record["executable"] for c in "'\"\r\n$")
        and chr(96) not in record["executable"],
        "Invalid bootstrap executable",
    )
    parsed = urllib.parse.urlsplit(record["url"])
    require(
        not parsed.username
        and not parsed.password
        and not parsed.fragment
        and (
            parsed.scheme == "https"
            or allow_local
            and parsed.scheme == "http"
            and parsed.hostname in {"localhost", "127.0.0.1", "::1"}
        ),
        "Bootstrap runtime requires HTTPS or explicit loopback testing",
    )
    destination = Path(output).absolute()
    require(not destination.exists(), "Bootstrap output already exists")
    require(
        record.get("format", "tar") in {"tar", "zip"},
        "Unknown bootstrap archive format",
    )
    require(
        type(record.get("strip", 0)) is int and 0 <= record.get("strip", 0) <= 8,
        "Invalid bootstrap strip count",
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".bootstrap-", dir=destination.parent
    ) as temporary:
        temporary = Path(temporary)
        source = temporary / "source"
        source.mkdir()
        for path in Path(__file__).parent.glob("*.py"):
            (source / path.name).write_bytes(path.read_bytes())
        (source / "__main__.py").write_text(
            "import runpy\nrunpy.run_module('consumer', run_name='__main__')\n"
        )
        staging = temporary / "result"
        staging.mkdir()
        engine = staging / "installer.pyz"
        zipapp.create_archive(source, engine, compressed=True)
        values = {
            "TARGET": record["target"],
            "SHA": record["sha256"],
            "EXECUTABLE": record["executable"],
            "ENGINE_SHA": digest(engine),
            "ENGINE": base64.b64encode(engine.read_bytes()).decode(),
            "URL": record["url"],
            "ARCH": "x64" if "x86_64" in record["target"] else "arm64",
        }
        windows = record["target"].startswith("windows-")
        if windows:
            require("'" not in record["url"], "Invalid PowerShell runtime URL")
            if record.get("format", "tar") == "zip":
                require(
                    not record.get("strip", 0),
                    "Windows ZIP runtime must have a root-level executable tree",
                )
                values["EXTRACT"] = (
                    "Expand-Archive -LiteralPath $archive -DestinationPath $payload"
                )
            else:
                values["EXTRACT"] = (
                    "& tar -xf $archive -C $payload --strip-components="
                    + str(record.get("strip", 0))
                    + "\n        if ($LASTEXITCODE -ne 0) { throw 'Runtime extraction failed' }"
                )
            script = POWERSHELL
        else:
            values["URL"] = shlex.quote(record["url"])
            values["PROTOCOL"] = (
                "" if allow_local else "--proto '=https' --proto-redir '=https'"
            )
            if record.get("format", "tar") == "zip":
                require(
                    not record.get("strip", 0),
                    "Unix ZIP bootstrap does not support stripping",
                )
                values["EXTRACT"] = (
                    'unzip -q "$temporary/archive" -d "$temporary/payload"'
                )
            else:
                values["EXTRACT"] = (
                    'tar -xf "$temporary/archive" -C "$temporary/payload" --strip-components='
                    + str(record.get("strip", 0))
                )
            script = SHELL
        for key, value in values.items():
            script = script.replace("@@" + key + "@@", value)
        require("@@" not in script, "Unresolved bootstrap placeholder")
        path = staging / ("bootstrap.ps1" if windows else "bootstrap.sh")
        path.write_text(script)
        path.chmod(0o755)
        write_json(
            staging / "bootstrap-descriptor.json",
            {
                "schema": 2,
                "runtime": record,
                "helper_sha256": digest(engine),
                "helper_revision": 2,
                "target": record["target"],
            },
        )
        staging.rename(destination)
    return {"directory": str(destination), "helper_sha256": values["ENGINE_SHA"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-descriptor", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--allow-http-localhost", action="store_true")
    args = parser.parse_args()
    try:
        generate(
            read_json(args.runtime_descriptor),
            args.output,
            allow_local=args.allow_http_localhost,
        )
    except (ValueError, OSError, KeyError) as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1)
