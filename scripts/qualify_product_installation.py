# SPDX-License-Identifier: Apache-2.0
"""Qualify source-independent shared/private native installations and data retention."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from qualify_connection_settings import qualify as qualify_connections  # noqa: E402

sys.path.insert(0, str(ROOT / "scripts/dev"))
from core import digest, write_json  # noqa: E402
from releases import MANIFEST, object_file, product_hash, seal, validate  # noqa: E402


def upgrade_fixture(artifact, base):
    """Derive a marked test assembly; native code and database schema stay current."""
    candidate = base / "upgrade-fixture"
    shutil.copytree(artifact, candidate)
    _, manifest = validate(candidate)
    marker = base / "installer-fixture.json"
    write_json(
        marker,
        {
            "purpose": "installer-lifecycle-test-only",
            "original_manifest_sha256": digest(artifact / MANIFEST),
        },
    )
    sources = {
        **manifest["sources"],
        "qualification-fixture": {
            "commit": None,
            "dirty": True,
            "source_sha256": digest(marker),
            "locks": {},
        },
    }
    product = manifest["products"]["undercurrent"]
    product["version"] += ".installer-test.1"
    product["files"]["share/undercurrent/installer-fixture.json"] = {
        **object_file(marker, candidate),
        "executable": False,
    }
    product["sources"] = sources
    product["release"] = product_hash(product)
    manifest.update(version=product["version"], sources=sources)
    seal(candidate, manifest)
    validate(candidate)
    return candidate


def qualify(artifact, report, bootstrap=None, exercise_upgrade=False):
    artifact, manifest = validate(artifact.resolve())
    license_sha = manifest["products"]["undercurrent"]["license_acceptance"]["sha256"]
    with tempfile.TemporaryDirectory(
        prefix="uct-", dir=None if os.name == "nt" else "/tmp"
    ) as temporary:
        base = Path(temporary).resolve()
        helper = base / "helper"
        shutil.copytree(
            ROOT / "scripts/dev",
            helper,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )
        env = {
            **os.environ,
            "PATH": ""
            if os.name != "nt"
            else str(Path(os.environ["SystemRoot"]) / "System32"),
            "PIP_NO_INDEX": "1",
            "UV_OFFLINE": "1",
            "HTTP_PROXY": "http://127.0.0.1:1",
            "HTTPS_PROXY": "http://127.0.0.1:1",
            "PYTHONHOME": str(base / "no-python"),
            "PYTHONPATH": str(base / "no-source"),
        }
        env = {
            k: v for k, v in env.items() if not k.startswith(("UNDERCURRENT_", "UC_"))
        }

        def run(*argv, ok=True, environment=None):
            result = subprocess.run(
                [str(v) for v in argv],
                cwd=base,
                env=environment or env,
                capture_output=True,
                text=True,
                timeout=240,
            )
            if ok != (result.returncode == 0):
                raise RuntimeError(
                    f"Unexpected result ({result.returncode}): {argv}\n{result.stdout}\n{result.stderr[-7000:]}"
                )
            return result

        def consumer(action, *args, ok=True):
            result = run(
                sys.executable, "-I", "-B", helper / "consumer.py", action, *args, ok=ok
            )
            if not ok:
                return result
            return (
                json.loads(result.stdout.splitlines()[-1])["result"]
                if "--events" in args
                else json.loads(result.stdout)
            )

        def entry(store):
            consumer("inspect", "--prefix", store)
            return json.loads((store / "installation-receipt.json").read_text(encoding="utf-8"))[
                "products"
            ]["undercurrent"]

        def command(store, name):
            return store / "bin" / (name + (".exe" if os.name == "nt" else ""))

        def smoke(store, home, action="full"):
            installed = entry(store)
            payload = store / installed["payload"]
            return json.loads(
                run(
                    installed["python"],
                    "-I",
                    "-B",
                    payload / "share/undercurrent/verify.py",
                    "--payload",
                    payload,
                    "--state-home",
                    home,
                    "--action",
                    action,
                ).stdout
            )

        shared, private = base / "shared tools 世界", base / "application runtime 世界"
        shared_home, private_home = base / "shared-home", base / "private-home"
        rejected = consumer(
            "install", "--artifact", artifact, "--prefix", shared, ok=False
        )
        assert "License acceptance required" in rejected.stderr and not shared.exists()
        expected = {}
        for store, scope, owner, home in (
            (shared, "shared", "user", shared_home),
            (private, "private", "example-app", private_home),
        ):
            options = (
                "--artifact",
                artifact,
                "--prefix",
                store,
                "--scope",
                scope,
                "--owner",
                owner,
                "--accept-license",
                license_sha,
                "--request-id",
                scope + "-setup",
                "--events",
                "jsonl",
            )
            consumer("install", *options)
            consumer("install", *options)
            run(command(store, "uc"), "--version")
            shim = store / "bin" / ("uc.aixand-command" if os.name == "nt" else "uc")
            shim_text = shim.read_text(encoding="utf-8")
            assert str(store / "runtimes") in shim_text, (store, shim_text)
            expected[scope] = smoke(store, home)["sessions"]
        assert (
            expected["shared"]
            and expected["private"]
            and set(expected["shared"]).isdisjoint(expected["private"])
        )
        connections = qualify_connections(Path(command(shared, "uc")))
        consumer(
            "install",
            "--artifact",
            artifact,
            "--prefix",
            private,
            "--scope",
            "private",
            "--owner",
            "another-app",
            "--accept-license",
            license_sha,
            ok=False,
        )

        # Exercise two live daemons and public bridge commands simultaneously.
        active = []
        try:
            for store, home in ((shared, shared_home), (private, private_home)):
                environment = {**env, "UNDERCURRENT_HOME": str(home)}
                run(command(store, "uc"), "daemon", "start", environment=environment)
                active.append((store, environment))
            code = "import asyncio,json,sys; from undercurrent import Client\nasync def check():\n async with Client(home=sys.argv[1],bridge=sys.argv[2]) as client:\n  print(json.dumps(sorted(v.id for v in await client.list())))\nasyncio.run(check())"
            for store, home, scope in (
                (shared, shared_home, "shared"),
                (private, private_home, "private"),
            ):
                ids = json.loads(
                    run(
                        entry(store)["python"],
                        "-I",
                        "-B",
                        "-c",
                        code,
                        home,
                        command(store, "uc-python-bridge"),
                    ).stdout
                )
                assert ids == expected[scope]
        finally:
            for store, environment in reversed(active):
                run(command(store, "uc"), "daemon", "stop", environment=environment)

        before = entry(shared)
        consumer("reinstall", "--artifact", artifact, "--prefix", shared)
        assert entry(shared)["payload"] != before["payload"]
        assert smoke(shared, shared_home, "inspect")["sessions"] == expected["shared"]
        consumer("rollback", "--prefix", shared, "--product", "undercurrent", ok=False)
        damaged = base / "damaged"
        shutil.copytree(artifact, damaged)
        (damaged / next(iter(manifest["files"]))).write_bytes(b"damaged")
        before_failed = entry(shared)["payload"]
        consumer("upgrade", "--artifact", damaged, "--prefix", shared, ok=False)
        assert entry(shared)["payload"] == before_failed

        # Both homes are quiesced. Back up the complete product-owned home and
        # verify copied bytes before replacing software; do not parse its store.
        backup = base / "backup"
        shutil.copytree(
            shared_home,
            backup,
            ignore=lambda directory, names: (
                set(names) & {"run", "logs", "verification-daemon.log"}
                if Path(directory) == shared_home
                else set()
            ),
        )
        backup_files = {
            p.relative_to(backup).as_posix(): digest(p)
            for p in backup.rglob("*")
            if p.is_file()
        }
        assert backup_files and all(
            digest(shared_home / name) == value for name, value in backup_files.items()
        )
        if exercise_upgrade:
            candidate = upgrade_fixture(artifact, base)
            consumer("upgrade", "--artifact", candidate, "--prefix", shared)
            assert entry(shared)["product"]["version"] != before["product"]["version"]
            assert (
                smoke(shared, shared_home, "inspect")["sessions"] == expected["shared"]
            )

        consumer(
            "uninstall", "--prefix", private, "--product", "undercurrent", ok=False
        )
        consumer(
            "uninstall", "--prefix", private, "--product", "undercurrent", "--stopped"
        )
        assert (private_home / "state/undercurrent.db").is_file()
        shutil.rmtree(private)
        run(command(shared, "uc"), "--version")
        assert smoke(shared, shared_home, "inspect")["sessions"] == expected["shared"]
        preserved = digest(shared_home / "state/undercurrent.db")
        consumer(
            "uninstall", "--prefix", shared, "--product", "undercurrent", "--stopped"
        )
        assert digest(shared_home / "state/undercurrent.db") == preserved
        assert not consumer("inspect", "--prefix", shared)["products"]

        bootstrap_tested = False
        if bootstrap:
            bootstrap_env = {
                **os.environ,
                "AIXAND_INSTALLER_CACHE": str(base / "bootstrap-cache"),
                "PIP_NO_INDEX": "1",
                "UV_OFFLINE": "1",
            }
            if os.name == "nt":
                launcher = [
                    shutil.which("pwsh") or shutil.which("powershell"),
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(bootstrap.resolve() / "bootstrap.ps1"),
                ]
                bootstrap_env["PATH"] = str(Path(os.environ["SystemRoot"]) / "System32")
            else:
                tools = base / "bootstrap-tools"
                tools.mkdir()
                for name in (
                    "sh",
                    "curl",
                    "tar",
                    "gzip",
                    "sha256sum",
                    "shasum",
                    "uname",
                    "mkdir",
                    "mktemp",
                    "mv",
                    "rm",
                    "rmdir",
                ):
                    if executable := shutil.which(name):
                        (tools / name).symlink_to(executable)
                launcher = ["/bin/sh", str(bootstrap.resolve() / "bootstrap.sh")]
                bootstrap_env["PATH"] = str(tools)
            store = base / "bootstrap-store"
            run(
                *launcher,
                "install",
                "--artifact",
                artifact,
                "--prefix",
                store,
                "--accept-license",
                license_sha,
                environment=bootstrap_env,
            )
            run(
                *launcher,
                "exec",
                "--prefix",
                store,
                "--command",
                "uc",
                "--",
                "--version",
                environment=bootstrap_env,
            )
            bootstrap_tested = True

        result = {
            "verified": True,
            "target": manifest["target"],
            "manifest_sha256": digest(artifact / MANIFEST),
            "license_acceptance_enforced": True,
            "shared_and_private": True,
            "simultaneous_daemons_isolated": True,
            "native_pty_and_sdk": True,
            "connection_profiles": connections,
            "offline_install": True,
            "empty_language_tool_path": True,
            "bootstrap_without_system_python": bootstrap_tested,
            "resume": True,
            "reinstall": True,
            "corrupt_upgrade_rejected": True,
            "upgrade_assembly_fixture_tested": exercise_upgrade,
            "quiesced_backup_verified": True,
            "stateful_rollback_rejected": True,
            "private_tree_removal_preserves_shared": True,
            "stopped_uninstall": True,
            "database_preserved": True,
        }
    if report:
        report.parent.mkdir(parents=True, exist_ok=True)
        with report.open("x", encoding="utf-8") as stream:
            stream.write(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--bootstrap", type=Path)
    parser.add_argument("--exercise-upgrade", action="store_true")
    args = parser.parse_args()
    qualify(args.artifact, args.report, args.bootstrap, args.exercise_upgrade)
