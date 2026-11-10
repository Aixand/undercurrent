#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Run selected project validation stages and report only executed stages."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib

from _support.processes import exit_code, handle_signals, run

ROOT = Path(__file__).resolve().parents[1]
STAGES = ("metadata", "formatting", "documentation", "tooling", "architecture", "local_identity", "clippy", "tests", "rustdoc",
          "package", "release", "configuration", "smoke", "recovery", "release_bundle")


def commands(stage, environment, logs):
    def python(path, *args):
        return [sys.executable, str(ROOT / path), *map(str, args)]
    release = Path(environment["CARGO_TARGET_DIR"]) / "release"
    return {
        "metadata": [python("scripts/checks/check-dependencies.py"),
                     python("scripts/checks/check-names.py")],
        "formatting": [python("scripts/checks/check-formatting.py")],
        "documentation": [python("tests/tooling/checks/docs_test.py"),
                          python("scripts/checks/check-docs.py", "--root", ROOT)],
        "tooling": [python("tests/tooling/run.py")],
        "architecture": [python("scripts/check-architecture.py", "--all")],
        "local_identity": [python("scripts/checks/check-local-ipc-security.py")],
        "clippy": [["cargo", "clippy", "--locked", "--workspace", "--all-targets", "--all-features", "--", "-D", "warnings"]],
        "tests": [["cargo", "build", "--locked", "--workspace", "--bins", "--all-features"],
                  ["cargo", "test", "--locked", "--workspace", "--all-targets", "--all-features"],
                  ["cargo", "test", "--locked", "--workspace", "--doc", "--all-features"]],
        "rustdoc": [["cargo", "doc", "--locked", "--workspace", "--all-features", "--no-deps"]],
        "package": [python("scripts/checks/validate-package-manifest.py"),
                    ["bash", str(ROOT / "tests/conformance/dev_installer_test.sh")],
                    python("tests/conformance/package/release_bundle_qualification_test.py")],
        "release": [["cargo", "build", "--locked", "--release", "--workspace", "--bins", "--all-features"]],
        "configuration": [python("tests/e2e/configuration/local-ipc.py", "--artifact-dir", release,
                                   "--report", logs / "configuration.json")],
        "smoke": [python("tests/e2e/cli/smoke.py", "--bin-dir", release)],
        "recovery": [python("tests/e2e/lifecycle/recovery.py", "--artifact-dir", release,
                            "--report", logs / "recovery.json")],
        "release_bundle": [python("tests/conformance/package/run-release-bundle-qualification.py", "--artifact-dir", release,
                                  "--report", logs / "release-bundle.json")],
    }[stage]


def validate(stages, report, environment):
    report.parent.mkdir(parents=True, exist_ok=True)
    logs = Path(tempfile.mkdtemp(prefix="run.", dir=report.parent))
    results, overall = [], 0
    try:
        for stage in stages:
            print(f"[standalone-validation] {stage}", flush=True)
            log = logs / f"{stage}.log"
            status = 0
            stage_env = environment | ({"RUSTDOCFLAGS": "-Dwarnings"} if stage == "rustdoc" else {})
            try:
                with log.open("wb") as output:
                    for command in commands(stage, environment, logs):
                        result = run(command, cwd=ROOT, env=stage_env, timeout=None, check=False,
                                     stdout=output, stderr=subprocess.STDOUT)
                        status = exit_code(result.returncode)
                        if status:
                            break
            except OSError as error:
                status = 127
                with log.open("a") as output:
                    output.write(f"{error}\n")
            except (SystemExit, KeyboardInterrupt) as error:
                status = error.code if isinstance(error, SystemExit) else 130
                raise
            finally:
                overall = status
                results.append({"stage": stage, "exit_code": status, "log": str(log)})
                with log.open("rb") as source:
                    while chunk := source.read(65536):
                        sys.stdout.buffer.write(chunk)
                sys.stdout.flush()
            if status:
                break
    finally:
        report.write_text(json.dumps({"schema": "undercurrent.standalone-validation/v0.1.0-beta",
            "status": "passed" if overall == 0 else "failed", "scope": "executed_stages_only",
            "stages": results}, indent=2) + "\n")
    return overall


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--all", action="store_true")
    selection.add_argument("--list", action="store_true")
    selection.add_argument("--stage", choices=STAGES)
    parser.add_argument("--report", type=Path, default=ROOT / "target/standalone-validation/report.json")
    args = parser.parse_args()
    if args.list:
        print("\n".join(STAGES))
        return 0
    environment = os.environ | {
        "RUSTUP_TOOLCHAIN": tomllib.loads((ROOT / "rust-toolchain.toml").read_text())["toolchain"]["channel"],
        "CARGO_NET_OFFLINE": "true", "CARGO_BUILD_JOBS": os.environ.get("CARGO_BUILD_JOBS", "6"),
        "CARGO_TARGET_DIR": str(Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target")).resolve()),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    with handle_signals():
        return validate([args.stage] if args.stage else STAGES, args.report.resolve(), environment)


if __name__ == "__main__":
    raise SystemExit(main())
