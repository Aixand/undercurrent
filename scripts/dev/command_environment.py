"""Inspect command conflicts and explicitly register a shared store in user PATH."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import os
import json
from pathlib import Path
import re
import subprocess

from core import read_json, require

USER_PATH_KEY = "Environment"


def path_key(value):
    return os.path.normcase(os.path.abspath(os.path.expandvars(value.strip('"'))))


def ordered_path(entries, directory, policy):
    require(policy in {"keep", "prefer"}, "Unknown command default policy")
    entries = [entry for entry in entries if entry]
    present = any(path_key(entry) == path_key(directory) for entry in entries)
    if policy == "keep" and present:
        return entries
    retained = [entry for entry in entries if path_key(entry) != path_key(directory)]
    return [directory, *retained] if policy == "prefer" else [*retained, directory]


def windows_paths():
    import winreg

    def value(hive, key):
        try:
            with winreg.OpenKey(hive, key) as handle:
                return winreg.QueryValueEx(handle, "Path")[0]
        except FileNotFoundError:
            return ""

    return (
        value(
            winreg.HKEY_LOCAL_MACHINE,
            r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment",
        ),
        value(winreg.HKEY_CURRENT_USER, USER_PATH_KEY),
    )


def search_path():
    if os.name == "nt":
        return os.path.expandvars(os.pathsep.join(windows_paths()))
    return os.environ.get("PATH", "")


def windows_candidates(names):
    script = r"""$ErrorActionPreference='Stop'
[Console]::OutputEncoding=New-Object Text.UTF8Encoding($false)
$rows=@()
foreach($name in ($env:AIXAND_COMMAND_NAMES | ConvertFrom-Json)) {
  $found=@(Get-Command -Name $name -All -CommandType Application,ExternalScript -ErrorAction SilentlyContinue)
  $rows+=@{name=$name;paths=@($found | ForEach-Object {$_.Source})}
}
ConvertTo-Json -InputObject @($rows) -Depth 4 -Compress
"""
    import base64

    executable = (
        Path(os.environ.get("SystemRoot", r"C:\Windows"))
        / "System32/WindowsPowerShell/v1.0/powershell.exe"
    )
    result = subprocess.run(
        [
            str(executable),
            "-NoProfile",
            "-NonInteractive",
            "-EncodedCommand",
            base64.b64encode(script.encode("utf-16le")).decode(),
        ],
        env={
            **os.environ,
            "PATH": search_path(),
            "AIXAND_COMMAND_NAMES": json.dumps(names),
        },
        capture_output=True,
        encoding="utf-8-sig",
        errors="replace",
        timeout=15,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
    require(result.returncode == 0, "Could not inspect Windows command precedence")
    return {row["name"]: row["paths"] for row in json.loads(result.stdout)}


def register(prefix, policy="keep"):
    """Only explicit CLI/UI choices mutate PATH; product lifecycle APIs never do."""
    require(os.name == "nt", "Automatic user PATH registration is available on Windows")
    require(
        (prefix / "installation-receipt.json").is_file(),
        "Install products before registering commands",
    )
    receipt = read_json(prefix / "installation-receipt.json")
    require(
        receipt.get("scope") == "shared" and receipt.get("owner") == "user",
        "Only user-owned shared products can register terminal commands",
    )
    import ctypes
    from ctypes import wintypes
    import winreg

    directory = str(prefix.absolute() / "bin")
    with winreg.CreateKey(winreg.HKEY_CURRENT_USER, USER_PATH_KEY) as handle:
        try:
            previous, kind = winreg.QueryValueEx(handle, "Path")
        except FileNotFoundError:
            previous, kind = "", winreg.REG_EXPAND_SZ
        require(
            kind in {winreg.REG_SZ, winreg.REG_EXPAND_SZ},
            "Unexpected user PATH registry type",
        )
        current = os.pathsep.join(
            ordered_path(previous.split(os.pathsep), directory, policy)
        )
        if current != previous:
            winreg.SetValueEx(handle, "Path", 0, kind, current)
    api = ctypes.WinDLL("user32", use_last_error=True)
    api.SendMessageTimeoutW.argtypes = [
        wintypes.HWND,
        wintypes.UINT,
        ctypes.c_size_t,
        wintypes.LPCWSTR,
        wintypes.UINT,
        wintypes.UINT,
        ctypes.POINTER(ctypes.c_size_t),
    ]
    api.SendMessageTimeoutW.restype = ctypes.c_ssize_t
    result = ctypes.c_size_t()
    api.SendMessageTimeoutW(
        0xFFFF, 0x1A, 0, "Environment", 2, 3000, ctypes.byref(result)
    )
    return {
        "registered": True,
        "changed": current != previous,
        "policy": policy,
        "directory": directory,
        "restartTerminal": True,
    }


def probe(path):
    try:
        result = subprocess.run(
            [str(path), "--version"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=3,
            encoding="utf-8",
            errors="replace",
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        output = result.stdout.strip()
        if result.returncode == 0 and len(output) <= 240:
            match = re.search(r"\b\d+\.\d+(?:\.\d+)?(?:[-.+][\w.-]+)?", output)
            if match:
                return match.group(0)
    except (OSError, subprocess.TimeoutExpired):
        pass
    return None


def inspect_commands(prefix, names=()):
    prefix = Path(prefix).absolute()
    receipt_file = prefix / "installation-receipt.json"
    receipt = read_json(receipt_file) if receipt_file.is_file() else {"products": {}}
    owned = {
        name: product["product"]["version"]
        for product in receipt["products"].values()
        for name in product["product"]["commands"]
    }
    names = sorted(set(names or owned))
    require(
        len(names) <= 64
        and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", name) for name in names),
        "Invalid command selection",
    )
    directories = search_path().split(os.pathsep)
    windows_found = windows_candidates(names) if os.name == "nt" else {}
    rows, probes = [], {}
    for name in names:
        installed = prefix / "bin" / (name + ".exe" if os.name == "nt" else name)
        candidates, seen = [], set()
        paths = []
        for directory in directories if os.name != "nt" else []:
            if not directory:
                continue
            directory = Path(os.path.expandvars(directory.strip('"')))
            if not directory.is_absolute():
                continue
            paths.append(directory / name)
        if os.name == "nt":
            paths = [Path(path) for path in windows_found.get(name, [])]
        for path in paths:
            key = path_key(str(path))
            if key in seen or not path.is_file() or not os.access(path, os.X_OK):
                continue
            seen.add(key)
            managed = name in owned and (
                key == path_key(str(installed))
                or installed.is_file()
                and path.samefile(installed)
            )
            candidate = {
                "path": str(path),
                "managed": managed,
                "version": owned[name] if managed else None,
            }
            candidates.append(candidate)
            # Never execute shell/batch scripts while discovering commands.
            if not managed and (os.name != "nt" or path.suffix.lower() == ".exe"):
                probes[key] = path
        rows.append(
            {
                "name": name,
                "installedPath": str(installed),
                "installedVersion": owned.get(name),
                "candidates": candidates,
            }
        )
    with ThreadPoolExecutor(max_workers=4) as pool:
        versions = dict(zip(probes, pool.map(probe, probes.values())))
    for row in rows:
        for candidate in row["candidates"]:
            if not candidate["managed"]:
                candidate["version"] = versions.get(path_key(candidate["path"]))
        row["default"] = row["candidates"][0] if row["candidates"] else None
        row["conflict"] = any(
            not candidate["managed"] for candidate in row["candidates"]
        )
    return {
        "commands": rows,
        "resolution": "PATH",
        "userPathSupported": os.name == "nt",
        "profileNotice": "PowerShell aliases, functions, and shell profiles can override PATH selection.",
    }
