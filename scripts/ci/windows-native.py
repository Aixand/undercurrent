#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Run the Windows workflow locally or in CI, preserving logs and exact exit codes."""

import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
STAGES = (
    "toolchain", "formatting", "naming", "runner", "bundle-rejection", "build", "workspace",
    "capture", "architecture", "smoke", "installer", "lint", "install",
    "installed-smoke", "attach", "bundle", "uninstall",
)


def commands(stage):
    def python(path, *args):
        return [sys.executable, path, *args]

    install = str(ROOT / "target/windows-development-install")
    attach_environment = ROOT / "target/windows-ci-python"
    attach_python = str(attach_environment / "Scripts/python.exe")
    return {
        "toolchain": [["rustup", "show", "active-toolchain"], ["rustc", "-Vv"]],
        "formatting": [python("scripts/checks/check-formatting.py")],
        "naming": [python("scripts/checks/check-names.py")],
        "runner": [python("tests/tooling/runners/test_windows_ci.py"),
                   python("tests/tooling/runners/test_windows_vm.py")],
        "bundle-rejection": [
            python("scripts/checks/validate-package-manifest.py"),
            python("tests/conformance/package/release_bundle_qualification_test.py"),
            python("tests/conformance/package/windows_release_bundle_test.py"),
            python("scripts/release/release_bundle_qualification.py", "adversarial",
                   "--platform", "windows-x86_64-msvc", "--report",
                   "target/windows-release-adversarial.json"),
        ],
        "build": [["cargo", "build", "--locked", "-p", "undercurrent-cli",
                   "-p", "undercurrent-daemon", "--bins"]],
        "workspace": [["cargo", "test", "--locked", "--workspace", "--no-fail-fast"]],
        "capture": [["cargo", "test", "--locked", "-p", "undercurrent-runtime", "--lib",
                     "process::host::shell_environment::native_windows_tests::authenticated_native_capture_failures_preserve_snapshot_isolation",
                     "--", "--ignored", "--exact", "--test-threads=1", "--nocapture"]],
        "architecture": [
            ["cargo", "fetch", "--locked", "--manifest-path", "tests/architecture/consumer/Cargo.toml"],
            python("tests/architecture/architecture_gate.py", "--root", ".", "--mode", "all",
                   "--target-dir", "target"),
        ],
        "smoke": [python("tests/e2e/platform/windows-native-smoke.py", "--skip-build")],
        "installer": [["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
                       "tests/conformance/dev_installer_test.ps1"]],
        "lint": [["cargo", "clippy", "--locked", "-p", "aixand-undercurrent-platform-windows",
                  "-p", "undercurrent-runtime", "-p", "undercurrent-daemon", "-p", "undercurrent-cli",
                  "-p", "aixand-undercurrent-transport-local", "--all-targets", "--all-features",
                  "--", "-D", "warnings"]],
        "install": [["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
                     "dev.ps1", "install", "--prod"]],
        "installed-smoke": [python("tests/e2e/platform/windows-native-smoke.py", "--skip-build",
                                   "--bin-dir", install)],
        "attach": [
            python("-m", "venv", str(attach_environment)),
            [attach_python, "-m", "pip", "install", "--disable-pip-version-check", "pywinpty==3.0.5"],
            [attach_python, "tests/e2e/platform/windows-attach-smoke.py", "--bin-dir", "target/debug"],
            [attach_python, "tests/e2e/platform/windows-attach-smoke.py", "--bin-dir", install],
        ],
        "bundle": [python("tests/conformance/package/test-platform-bundle.py", "--bin-dir", install)],
        "uninstall": [["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
                       "dev.ps1", "uninstall"]],
    }[stage]


def run_stage(stage, environment, logs):
    log_path = logs / f"{stage}.log"
    started = time.monotonic()
    result = {"stage": stage, "exit_code": 1, "log": str(log_path), "commands": []}
    if stage == "capture":
        environment = environment | {"UC_WINDOWS_TEST_HOST": str(ROOT / "target/debug/uc-session-host.exe")}
    with log_path.open("w", encoding="utf-8") as log:
        try:
            for command in commands(stage):
                result["commands"].append(command)
                heading = f"[windows-ci:{stage}] {subprocess.list2cmdline(command)}\n"
                print(heading, end="", flush=True)
                log.write(heading)
                with subprocess.Popen(command, cwd=ROOT, env=environment, stdin=subprocess.DEVNULL,
                                      stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                      text=True, encoding="utf-8", errors="replace") as process:
                    try:
                        for line in process.stdout:
                            log.write(line)
                            log.flush()
                            print(line, end="", flush=True)
                        code = process.wait()
                    except BaseException:
                        process.kill()
                        process.wait()
                        raise
                result["exit_code"] = code
                if code:
                    break
        except OSError as error:
            result["exit_code"] = 127
            log.write(f"{error}\n")
            print(error, file=sys.stderr, flush=True)
    result["seconds"] = round(time.monotonic() - started, 3)
    return result


def source_record():
    manifest = ROOT / ".undercurrent-ci-source.json"
    supplied = json.loads(manifest.read_text(encoding="utf-8")) if manifest.exists() else None
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT).decode().strip()
    paths = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=ROOT
    ).decode("utf-8").split("\0")
    digest = hashlib.sha256()
    for relative in sorted(set(filter(None, paths))):
        if relative == ".undercurrent-ci-source.json":
            continue
        path = ROOT / relative
        if not path.exists():
            continue
        file_hash = hashlib.sha256(path.read_bytes()).hexdigest()
        digest.update(relative.encode("utf-8") + b"\0" + file_hash.encode() + b"\0")
    fingerprint = digest.hexdigest()
    if supplied and fingerprint != supplied["source_sha256"]:
        raise ValueError("VM source no longer matches the uploaded snapshot")
    return {"revision": supplied["revision"] if supplied else revision,
            "source_sha256": fingerprint}


def validate(stages, environment, logs):
    report = {
        "schema": "undercurrent.windows-ci/v0.1.0-beta", "status": "running",
        "scope": "full_workflow" if stages == list(STAGES) else "selected_stages_only",
        "platform": platform.platform(), "python": sys.version,
        "elevated": bool(ctypes.windll.shell32.IsUserAnAdmin()),
        "requested_stages": stages, "stages": [],
        "source": source_record(),
    }
    report_path = logs / "report.json"
    install_succeeded = False
    code = 1
    try:
        for stage in stages:
            result = run_stage(stage, environment, logs)
            report["stages"].append(result)
            code = result["exit_code"]
            if stage == "install":
                install_succeeded = code == 0
            if stage == "uninstall":
                install_succeeded = False
            report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
            if code:
                break
        report["status"] = "passed" if code == 0 else "failed"
    finally:
        # Match the workflow's always-run cleanup after successful installation.
        if install_succeeded and "uninstall" in stages:
            cleanup = run_stage("uninstall", environment, logs)
            report["stages"].append(cleanup)
            code = code or cleanup["exit_code"]
            if cleanup["exit_code"]:
                report["status"] = "failed"
        if report["status"] == "running":
            report["status"] = "interrupted"
        report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"[windows-ci] {report['status']}: {report_path}", flush=True)
    return code


def configure_output():
    # Redirected Windows output otherwise uses the active legacy code page.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="backslashreplace")


def main():
    configure_output()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=("all", *STAGES), default="all")
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()
    if args.list:
        print("\n".join(STAGES))
        return 0
    if os.name != "nt":
        parser.error("native Windows is required")
    if sys.version_info[:2] != (3, 13):
        parser.error("Python 3.13 is required to match the Windows workflow")
    log_root = ROOT / "target/windows-ci"
    log_root.mkdir(parents=True, exist_ok=True)
    logs = Path(tempfile.mkdtemp(prefix="run-", dir=log_root))
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith(("UC_", "UNDERCURRENT_"))}
    environment.update({
        "CARGO_TARGET_DIR": str(ROOT / "target"),
        "UNDERCURRENT_DEV_INSTALL_DIR": str(ROOT / "target/windows-development-install"),
        "PYTHONDONTWRITEBYTECODE": "1", "PYTHONIOENCODING": "utf-8",
        "PATH": str(Path(sys.executable).parent) + os.pathsep + environment.get("PATH", ""),
    })
    return validate(list(STAGES) if args.stage == "all" else [args.stage], environment, logs)


if __name__ == "__main__":
    raise SystemExit(main())
