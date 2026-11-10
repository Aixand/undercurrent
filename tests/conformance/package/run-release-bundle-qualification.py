#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Qualify existing Linux release binaries in a verified, isolated installation."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import uuid

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
from _support.processes import (assert_gone, exit_code, handle_signals, identities,
                                isolated_environment, run, single_identity, terminate_identity)

HELPER = ROOT / "scripts/release/release_bundle_qualification.py"
INPUTS = {
    "package-manifest": "undercurrent-package.json", "package-schema": "schemas/undercurrent-package.schema.json",
    "config-schema": "schemas/config.schema.json", "license": "LICENSE",
    "foundation-notice": "crates/undercurrent-foundation/NOTICE", "vt100-license": "vendor/vt100/LICENSE",
    "vt100-provenance": "vendor/vt100/PROVENANCE.json", "vte-apache-license": "vendor/vte/LICENSE-APACHE",
    "vte-mit-license": "vendor/vte/LICENSE-MIT", "vte-provenance": "vendor/vte/PROVENANCE.json",
    "vte-unsafe-audit": "vendor/vte/UNSAFE-AUDIT.json", "install-document": "docs/getting-started/bundles.md",
}


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def qualify(artifacts, work):
    bundle, prefix, runtime = work / "bundle", work / "prefix", work / "runtime"
    archive_one, archive_two = work / "bundle-one.tar", work / "bundle-two.tar"
    adversarial_path = work / "adversarial.json"

    def helper(*args, env=None):
        return run([sys.executable, HELPER, *args], env=env)

    helper("adversarial", "--report", adversarial_path)
    options = [part for key, value in INPUTS.items() for part in ("--" + key, ROOT / value)]
    helper("assemble", "--artifact-dir", artifacts, "--output", bundle, *options)
    helper("verify", "--bundle", bundle)
    helper("archive", "--bundle", bundle, "--output", archive_one)
    helper("archive", "--bundle", bundle, "--output", archive_two)
    archive = digest(archive_one)
    assert archive == digest(archive_two), "normalized release bundle is not deterministic"

    poison, tool_marker = work / "poison", work / "forbidden-tool-invoked"
    poison.mkdir(mode=0o700)
    for tool in ("cargo", "rustc", "git", "curl", "wget"):
        shim = poison / tool
        # Executable shell fixtures catch any attempted build or download.
        shim.write_text(f"#!/bin/sh\nprintf '%s\\n' '{tool}' >>'{tool_marker}'\nexit 97\n")
        shim.chmod(0o700)
    install_env = os.environ | {"PATH": f"{poison}:/usr/bin:/bin"}
    helper("install", "--bundle", bundle, "--prefix", prefix, "--accept-license", "Apache-2.0", env=install_env)
    assert not tool_marker.exists(), "installation invoked a source-build, VCS or download tool"

    home = runtime / "state/undercurrent"
    for directory in (runtime, runtime / "home", runtime / "state", runtime / "xdg-state",
                      runtime / "xdg-run", runtime / "tmp", home):
        directory.mkdir(mode=0o700)
    config = home / "config.toml"
    config.write_text("schema_version = \"0.1.0-beta\"\n\n[local]\n")
    config.chmod(0o600)
    marker = f"uc-release-bundle-{uuid.uuid4().hex}"
    environment = isolated_environment(home) | {
        "HOME": str(runtime / "home"), "XDG_STATE_HOME": str(runtime / "xdg-state"),
        "XDG_RUNTIME_DIR": str(runtime / "xdg-run"), "TMPDIR": str(runtime / "tmp"),
        "PATH": f"{poison}:{prefix}/bin:/usr/bin:/bin", "UC_RELEASE_QUALIFICATION_MARKER": marker,
    }
    uc, daemon, session_host = (prefix / "bin" / name for name in ("uc", "uc-daemon", "uc-session-host"))
    daemon_started = False

    def command(*args, check=True, timeout=20):
        return run([uc, *args], env=environment, cwd=runtime, check=check, timeout=timeout).stdout.decode().strip()

    try:
        for executable in (uc, daemon, session_host):
            assert run([executable, "--version"], env=environment, cwd=runtime).stdout.strip() == f"{executable.name} 0.1.0-beta".encode()
            assert f"Usage: {executable.name}".encode() in run([executable, "--help"], env=environment, cwd=runtime).stdout
        assert "configuration is valid" in command("config", "validate")
        assert not identities(daemon) and not identities(session_host), "configuration/help launched a runtime process"
        daemon_started = True
        command("ls", "--page-size", "1")
        daemon_identity = single_identity(daemon)
        host_identity = single_identity(session_host)
        assert not list(runtime.rglob("websocket.token")), "local bootstrap created a retired token"
        assert "epoch:" in command("daemon", "start")
        daemon_identity = single_identity(daemon)
        host_identity = single_identity(session_host)
        endpoint = home / "run/control.sock"
        assert endpoint.is_socket() and stat.S_IMODE(endpoint.stat().st_mode) == 0o600
        run([sys.executable, ROOT / "tests/e2e/platform/assert-no-tcp-listener.py", daemon_identity[0]])
        assert "epoch:" in command("daemon", "status")
        command("start", "release-bundle-probe", "--", "/bin/sh", "-c", "printf release-bundle-ready; sleep 60")
        detail = command("inspect", "release-bundle-probe", "--all")
        assert "release-bundle-probe" in detail and "running" in detail
        diagnostic = json.loads(command("--json", "debug", "bundle"))
        assert diagnostic["terminal_content_redacted"] is True and diagnostic.get("terminal_content") is None
        command("stop", "release-bundle-probe", "--force")
        command("clean", "release-bundle-probe", "--yes")
        command("daemon", "stop", "--graceful", "--drain-timeout", "5s")
        daemon_started = False
        assert_gone(host_identity, "session-host")
        assert_gone(daemon_identity, "daemon")
        assert not identities(marker=f"UC_RELEASE_QUALIFICATION_MARKER={marker}"), "qualification-owned processes remain"
        assert not tool_marker.exists(), "runtime invoked a source-build, VCS or download tool"
        assert (home / "state/undercurrent.db").is_file() and config.is_file()
        escaped = [path for path in runtime.rglob("*") if path.is_file() and not path.is_relative_to(home)]
        assert not escaped, f"runtime state escaped declared product roots: {escaped}"
        shutil.rmtree(prefix)
        assert not prefix.exists() and (home / "state/undercurrent.db").is_file() and config.is_file()
        artifact_lines = "".join(f"{digest(artifacts / name)}  {name}\n" for name in ("uc", "uc-daemon", "uc-session-host"))
        return make_report(json.loads(adversarial_path.read_text()), archive,
                           digest(bundle / "share/undercurrent/bundle-manifest.json"),
                           digest(ROOT / "undercurrent-package.json"), hashlib.sha256(artifact_lines.encode()).hexdigest())
    finally:
        if daemon_started:
            try:
                command("daemon", "stop", "--graceful", "--drain-timeout", "2s", check=False, timeout=5)
            except (OSError, subprocess.TimeoutExpired):
                pass
        for identity in identities(home=home):
            terminate_identity(identity)


def make_report(adversarial, archive, manifest, package, artifacts):
    return {
        "schema": "undercurrent.release-bundle-qualification/v0.1.0-beta",
        "status": "passed",
        "coordinate": "undercurrent",
        "package_version": "0.1.0-beta",
        "platform": "linux-x86_64-gnu",
        "source_artifacts": {
            "combined_sha256": f"sha256:{artifacts}",
            "package_manifest_sha256": f"sha256:{package}",
            "roles": ["cli", "daemon", "session_host"],
        },
        "bundle": {
            "archive_sha256": f"sha256:{archive}",
            "manifest_sha256": f"sha256:{manifest}",
            "deterministic": True,
            "owner_only": True,
            "exact_file_allowlist": True,
            "implementation_libraries": 0,
            "source_tree_files": 0,
            "secret_material_files": 0,
        },
        "installation": {
            "verified_before_execution": True,
            "verified_after_copy": True,
            "license_accepted": "Apache-2.0",
            "isolated_prefix": True,
            "monorepo_clone": False,
            "source_build": False,
            "external_network": False,
            "activation": "explicit",
        },
        "runtime": {
            "external_daemon": True,
            "semantic_transport": "local_ipc",
            "endpoint_kind": "unix_socket",
            "daemon_tcp_listeners": 0,
            "ordinary_command_process_spawns": 2,
            "automatic_local_bootstrap": "passed",
            "daemon_start": "passed",
            "status": "passed",
            "session_start_inspect_stop": "passed",
            "safe_diagnostic_bundle": "passed",
            "graceful_shutdown": "passed",
        },
        "uninstall": {
            "installed_prefix_removed": True,
            "default_product_state_preserved": True,
            "residual_owned_processes": 0,
            "residual_state_outside_declared_roots": 0,
            "qualification_scratch_removed_on_exit": True,
        },
        "adversarial": adversarial,
        "checks": [
            {"id": "artifact-integrity-and-roles", "status": "passed"},
            {"id": "metadata-license-provenance", "status": "passed"},
            {"id": "deterministic-owner-only-bundle", "status": "passed"},
            {"id": "offline-source-free-install", "status": "passed"},
            {"id": "installed-probes-and-config", "status": "passed"},
            {"id": "explicit-local-ipc-daemon-workflow", "status": "passed"},
            {"id": "ordinary-command-local-auto-bootstrap", "status": "passed"},
            {"id": "safe-diagnostic-default", "status": "passed"},
            {"id": "graceful-cleanup-and-uninstall", "status": "passed"},
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", type=Path, default=ROOT / "target/release")
    parser.add_argument("--report", type=Path, default=ROOT / "target/release-bundle-qualification/report.json")
    args = parser.parse_args()
    if (sys.platform, platform.machine()) != ("linux", "x86_64"):
        parser.error("release bundle qualification requires Linux x86_64")
    if not os.confstr("CS_GNU_LIBC_VERSION"):
        parser.error("release bundle qualification requires GNU libc")
    try:
        with handle_signals():
            with tempfile.TemporaryDirectory(prefix="ucr.", dir="/tmp") as directory:
                report = qualify(args.artifact_dir.resolve(), Path(directory))
            # Publish success only after temporary state has been removed.
            destination = args.report.resolve()
            destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            temporary = destination.with_name(destination.name + f".tmp.{os.getpid()}")
            try:
                temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
                temporary.chmod(0o600)
                temporary.replace(destination)
            finally:
                temporary.unlink(missing_ok=True)
    except subprocess.CalledProcessError as error:
        print(f"release bundle command failed: {shlex.join(error.cmd)}", file=sys.stderr)
        print((error.stderr or error.stdout or b"").decode(errors="replace"), file=sys.stderr)
        return exit_code(error.returncode)
    except subprocess.TimeoutExpired as error:
        print(f"release bundle command timed out: {error.cmd}", file=sys.stderr)
        return 124
    print(f"release bundle qualification passed: {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
