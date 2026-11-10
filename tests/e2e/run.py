#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Discover and run explicitly selected E2E checks against already-built binaries."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
CATALOG = Path(__file__).with_name("catalog.json")
PLATFORMS = {"linux", "darwin", "win32"}
MODES = {"ci", "standalone", "manual", "helper"}
FIELDS = {"id", "path", "group", "platforms", "mode", "requires", "binaries", "command", "notes"}


def load_catalog(path=CATALOG, root=ROOT):
    value = json.loads(path.read_text())
    if set(value) != {"schema", "tests"} or value["schema"] != "undercurrent.e2e-catalog/v0.1.0-beta":
        raise ValueError("invalid E2E catalog schema")
    ids, paths = set(), set()
    for item in value["tests"]:
        if set(item) != FIELDS or item["id"] in ids or item["path"] in paths:
            raise ValueError(f"invalid or duplicate E2E entry: {item.get('id')}")
        relative = Path(item["path"])
        if relative.is_absolute() or ".." in relative.parts or not item["path"].startswith("tests/e2e/"):
            raise ValueError(f"invalid E2E path: {relative}")
        if not (root / relative).is_file():
            raise ValueError(f"missing E2E script: {relative}")
        if item["mode"] not in MODES or not item["platforms"] or not set(item["platforms"]) <= PLATFORMS:
            raise ValueError(f"invalid E2E mode or platforms: {item['id']}")
        if not isinstance(item["command"], list) or any(not isinstance(arg, str) for arg in item["command"]):
            raise ValueError(f"invalid E2E command: {item['id']}")
        if item["mode"] != "helper" and (not item["command"] or item["path"] not in item["command"]):
            raise ValueError(f"E2E command does not invoke its script: {item['id']}")
        ids.add(item["id"])
        paths.add(item["path"])
    physical = {p.relative_to(root).as_posix() for p in (root / "tests/e2e").rglob("*")
                if p.suffix in {".py", ".sh", ".ps1"} and p.name != "run.py"
                and "_support" not in p.parts and "__pycache__" not in p.parts}
    if paths != physical:
        raise ValueError(f"E2E catalog differs from scripts: {sorted(paths ^ physical)}")
    return value["tests"]


def select_tests(catalog, names, group, platform=sys.platform):
    known = {item["id"] for item in catalog}
    unknown = set(names) - known
    if unknown:
        raise ValueError(f"unknown E2E tests: {', '.join(sorted(unknown))}")
    selected = [item for item in catalog if item["id"] in names or
                (group == item["group"] and platform in item["platforms"]
                 and item["mode"] != "helper")]
    for item in selected:
        if platform not in item["platforms"]:
            raise ValueError(f"{item['id']} requires {', '.join(item['platforms'])}; host is {platform}")
        if item["mode"] == "helper":
            raise ValueError(f"{item['id']} is a supporting helper: {item['notes']}")
    if not selected:
        raise ValueError("no runnable tests selected for this platform")
    return selected


def command_for(item, bin_dir, report_dir):
    suffix = ".exe" if sys.platform == "win32" else ""
    variables = {"python": sys.executable, "bin_dir": str(bin_dir), "report_dir": str(report_dir),
                 "repo": str(ROOT), "exe": suffix}
    return [arg.format(**variables) for arg in item["command"]]


def preflight(selected, bin_dir):
    suffix = ".exe" if sys.platform == "win32" else ""
    for item in selected:
        for tool in item["requires"]:
            if not shutil.which(tool):
                raise ValueError(f"{item['id']}: missing prerequisite {tool}")
        for binary in item["binaries"]:
            path = bin_dir / (binary + suffix)
            if not path.is_file() or not os.access(path, os.X_OK):
                raise ValueError(f"{item['id']}: build the executable first: {path}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--list", action="store_true", help="List tests and their execution status (default)")
    selection.add_argument("--check", action="store_true", help="Validate catalog coverage without running tests")
    selection.add_argument("--test", action="append", default=[], help="Run a named test; may be repeated")
    selection.add_argument("--group", choices=["cli", "lifecycle", "configuration", "platform"],
                           help="Run a feature group supported by this host")
    parser.add_argument("--json", action="store_true", help="Print the catalog as JSON")
    parser.add_argument("--dry-run", action="store_true", help="Print selected commands without executing them")
    parser.add_argument("--bin-dir", type=Path, default=ROOT / "target/debug")
    parser.add_argument("--report-dir", type=Path, default=ROOT / "target/e2e")
    args = parser.parse_args(argv)
    try:
        catalog = load_catalog()
        if args.check:
            print(f"E2E catalog covers {len(catalog)} scripts")
            return 0
        if args.list or not (args.test or args.group):
            if args.json:
                print(json.dumps(catalog, indent=2))
            else:
                for item in catalog:
                    print(f"{item['id']:26} {item['mode']:14} {','.join(item['platforms']):18} {item['notes']}")
            return 0
        selected = select_tests(catalog, args.test, args.group)
        bin_dir, reports = args.bin_dir.resolve(), args.report_dir.resolve()
        if args.dry_run:
            for item in selected:
                print(json.dumps({"id": item["id"], "command": command_for(item, bin_dir, reports / item["id"]),
                                  "UC_E2E_BIN_DIR": str(bin_dir)}))
            return 0
        preflight(selected, bin_dir)
        reports.mkdir(parents=True, exist_ok=True)
        run = Path(tempfile.mkdtemp(prefix="run-", dir=reports))
        environment = os.environ | {"UC_E2E_BIN_DIR": str(bin_dir), "PYTHONDONTWRITEBYTECODE": "1"}
        results = []
        for item in selected:
            output = run / item["id"]
            output.mkdir()
            command = command_for(item, bin_dir, output)
            print(f"[e2e] {item['id']}", flush=True)
            result = subprocess.run(command, cwd=ROOT, env=environment, check=False)
            results.append({"id": item["id"], "exit_code": result.returncode})
            (run / "report.json").write_text(json.dumps({
                "schema": "undercurrent.e2e-result/v0.1.0-beta", "scope": "executed_tests_only", "tests": results,
            }, indent=2) + "\n")
            if result.returncode:
                print(f"E2E results: {run / 'report.json'}")
                return result.returncode if result.returncode > 0 else 128 - result.returncode
        print(f"E2E results: {run / 'report.json'}")
        return 0
    except (ValueError, OSError) as error:
        parser.exit(2, f"e2e: {error}\n")


if __name__ == "__main__":
    raise SystemExit(main())
