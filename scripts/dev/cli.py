#!/usr/bin/env python3
"""Language-independent entry point; project metadata selects the adapter."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import tarfile
import zipfile

if sys.version_info < (3, 11):
    print(
        "This developer helper requires Python 3.11+; select it with AIXAND_DEV_PYTHON",
        file=sys.stderr,
    )
    raise SystemExit(2)

import tomllib

from core import Failure, OPERATIONS, Project, Usage, run
from installs import install, prune, rollback, uninstall
from packages import build, package, validate, verify_install


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--project", type=Path, default=Path(__file__).resolve().parents[2]
    )
    result.add_argument(
        "operation",
        nargs="?",
        default="help",
        choices=sorted(OPERATIONS | {"release-check"}),
    )
    result.add_argument("--artifact", type=Path)
    result.add_argument("--output", type=Path)
    result.add_argument("--prefix", type=Path)
    result.add_argument("--environment", type=Path)
    result.add_argument("--python")
    result.add_argument("--target")
    result.add_argument(
        "--command", action="append", help="Command name to inspect (repeatable)"
    )
    result.add_argument(
        "--register-path",
        action="store_true",
        help="Register a shared installation in Windows user PATH",
    )
    result.add_argument(
        "--command-policy",
        choices=["keep", "prefer"],
        help="Keep current defaults or prefer installed commands in user PATH",
    )
    result.add_argument(
        "--report", type=Path, help="Write or consume a schema-2 verification report"
    )
    result.add_argument(
        "--product", help="Schema-2 product or hierarchical collection ID"
    )
    result.add_argument("--component", help="Schema-2 build component")
    result.add_argument("--variant", help="Declared schema-2 product variant")
    result.add_argument("--scope", choices=["shared", "private"])
    result.add_argument("--owner", help="Application owner for private installations")
    result.add_argument(
        "--accept-license",
        action="append",
        help="Accept the exact product license SHA-256 (repeat for a collection)",
    )
    result.add_argument("--expected-sha256", help="Expected schema-2 manifest SHA-256")
    result.add_argument(
        "--request-id", help="Idempotent installation request identifier"
    )
    result.add_argument(
        "--events", choices=["jsonl"], help="Stream versioned operation events"
    )
    result.add_argument(
        "--allow-http-localhost",
        action="store_true",
        help="Allow loopback runtime downloads for disposable tests",
    )
    result.add_argument(
        "--version", help="Expected release tag for release-check (v<package-version>)"
    )
    result.add_argument("--release", action="store_true")
    result.add_argument("--install", action="store_true", dest="install_proof")
    result.add_argument("--dry-run", action="store_true")
    result.add_argument(
        "--stopped",
        action="store_true",
        help="Explicitly assert users of removed software have stopped",
    )
    result.add_argument(
        "--json",
        action="store_true",
        help="Lifecycle results are JSON; subprocess logs use stderr",
    )
    return result


def check_options(args):
    common = {"project", "operation", "json"}
    allowed = {
        "help": set(),
        "doctor": set(),
        "develop": {"python"},
        "check": set(),
        "test": set(),
        "build": {"output", "release", "target", "python"},
        "package": {"output", "target", "python"},
        "verify": {"artifact", "install_proof", "python"},
        "install": {"artifact", "prefix", "python", "environment", "dry_run"},
        "reinstall": {"artifact", "prefix", "python", "dry_run"},
        "upgrade": {"artifact", "prefix", "python", "dry_run"},
        "rollback": {"prefix", "dry_run"},
        "prune": {"prefix", "dry_run", "stopped"},
        "uninstall": {"prefix", "dry_run", "stopped"},
        "publish": set(),
        "release-check": {"version", "artifact"},
    }[args.operation]
    unexpected = [
        key
        for key, value in vars(args).items()
        if value not in (None, False) and key not in common | allowed
    ]
    if unexpected:
        raise Usage("Options do not apply to this operation: " + ", ".join(unexpected))


def doctor(project):
    tools = (
        ["uv"]
        if project.python
        else (["cargo", "rustc", "rustup"] if project.rust else [])
    )
    tools += [
        project.expand(argv)[0]
        for batches in project.commands.values()
        for argv in batches
    ]
    tools = sorted(set(tools))
    found = {name: shutil.which(name) for name in tools}
    if project.rust:
        locks = list(project.root.glob("Cargo.lock"))
        pinned = (project.root / "rust-toolchain.toml").exists()
    else:
        locks = list(project.root.glob("uv.lock")) if project.python else []
        pinned = bool(project.python_request) if project.python else True
    runtime_ready = not (project.python or project.rust)
    if project.python and found.get("uv"):
        result = subprocess.run(
            ["uv", "python", "find", project.python_request, "--no-python-downloads"],
            cwd=project.root,
            capture_output=True,
        )
        runtime_ready = result.returncode == 0
    elif project.rust and found.get("rustup") and pinned:
        result = subprocess.run(
            project.rust_argv("rustc", "--version"),
            cwd=project.root,
            capture_output=True,
        )
        runtime_ready = result.returncode == 0
    ok = (
        all(found.values())
        and (bool(locks) or project.profile == "custom")
        and pinned
        and runtime_ready
    )
    capabilities = {"help", "doctor"}
    capabilities.update(project.commands)
    if project.profile != "custom":
        capabilities.update({"develop", "build", "package", "verify", "release-check"})
    if project.rust:
        capabilities.update({"check", "test"})
    if project.managed and os.name != "nt":
        capabilities.update({"install", "reinstall", "upgrade", "uninstall", "prune"})
        if project.install_config.get("stateful") is False:
            capabilities.add("rollback")
    return {
        "ready": ok,
        "profile": project.profile,
        "name": project.name,
        "version": project.version,
        "tools": found,
        "lock_present": bool(locks),
        "runtime_pin_present": pinned,
        "runtime_installed": runtime_ready,
        "managed_install_platform": "Unix only in v1",
        "checks": sorted(project.commands),
        "commands": project.exposed,
        "capabilities": sorted(capabilities),
        "hint": "Create missing locks intentionally during adoption; develop can provision selected runtimes but never updates locks.",
    }


def develop(project, python=None):
    if project.python:
        if not (project.root / "uv.lock").exists():
            raise Failure("Missing uv.lock; run uv lock intentionally during adoption")
        run(
            [
                "uv",
                "sync",
                "--locked",
                "--python",
                python or project.python_request,
                "--project",
                project.manifest.parent,
            ],
            project.root,
        )
    elif project.rust:
        if not (project.root / "Cargo.lock").exists():
            raise Failure(
                "Missing Cargo.lock; generate it intentionally during adoption"
            )
        if not (project.root / "rust-toolchain.toml").exists():
            raise Failure("Declare the tested toolchain in rust-toolchain.toml")
        config = tomllib.loads((project.root / "rust-toolchain.toml").read_text())[
            "toolchain"
        ]
        components = config.get("components", ["rustfmt", "clippy"])
        run(
            [
                "rustup",
                "toolchain",
                "install",
                project.rust_toolchain(),
                "--profile",
                config.get("profile", "minimal"),
                "--no-self-update",
                *[
                    arg
                    for component in components
                    for arg in ["--component", component]
                ],
            ],
            project.root,
        )
        project.cargo("fetch")
    else:
        project.configured("develop")
    return {"developed": True, "profile": project.profile}


def checks(project, operation):
    if operation in project.commands:
        project.configured(operation)
    elif project.rust and operation == "check":
        run(
            project.rust_argv(
                "cargo", "fmt", "--manifest-path", project.manifest, "--check"
            ),
            project.root,
        )
        features = (
            ["--features", ",".join(project.features)] if project.features else []
        )
        project.cargo("clippy", "--all-targets", *features, "--", "-D", "warnings")
    elif project.rust and operation == "test":
        features = (
            ["--features", ",".join(project.features)] if project.features else []
        )
        project.cargo("test", *features)
    else:
        raise Usage(f"{operation} requires an actual configured command")
    return {operation: "passed"}


def dispatch(args):
    if args.operation == "help":
        parser().print_help()
        return None
    from product_cli import handles, dispatch as product_dispatch

    if handles(args):
        return product_dispatch(args)
    check_options(args)
    project = Project(args.project)
    if args.target and not project.rust:
        raise Usage("--target applies only to Cargo adapters in v1")
    if args.python and not project.python:
        raise Usage("--python applies only to Python adapters")
    if args.output and args.operation == "build" and project.rust:
        raise Usage(
            "Cargo build outputs use CARGO_TARGET_DIR; --output applies to package and Python/custom build"
        )
    if (
        args.target
        and project.profile == "rust-library"
        and args.operation == "package"
    ):
        raise Usage(
            "v1 Cargo library packaging uses native verification; cross-target packaging requires another adapter"
        )
    operation = args.operation
    if operation == "doctor":
        result = doctor(project)
        if not result["ready"]:
            print(json.dumps(result, indent=2))
            raise Failure(
                "Doctor found missing prerequisites; no repairs were performed"
            )
        return result
    if operation == "develop":
        return develop(project, args.python)
    if operation in {"check", "test"}:
        return checks(project, operation)
    if operation == "build":
        return build(
            project,
            args.output,
            release=args.release,
            target=args.target,
            python=args.python,
        )
    if operation == "package":
        return package(project, args.output, target=args.target, python=args.python)
    if operation == "verify":
        if not args.artifact:
            raise Usage("verify requires --artifact")
        artifact, manifest = validate(args.artifact, project)
        if args.install_proof:
            return verify_install(project, artifact, manifest, args.python)
        return {
            "verified": True,
            "installation_tested": False,
            "version": manifest["version"],
        }
    if operation == "release-check":
        if project.profile == "custom":
            raise Usage("Custom release authority needs a project-specific adapter")
        identity = project.identity()
        if not args.version or args.version != "v" + project.version:
            raise Failure(
                "Release tag must equal v<package-version> from project metadata"
            )
        if not identity["commit"] or identity["dirty"]:
            raise Failure("Publication requires a clean committed release revision")
        tagged = run(
            ["git", "rev-parse", "--verify", "refs/tags/" + args.version + "^{commit}"],
            project.root,
            capture=True,
        ).strip()
        if tagged != identity["commit"]:
            raise Failure("Release tag does not identify the checked-out revision")
        if args.artifact:
            _, manifest = validate(args.artifact, project)
            if manifest["source"] != identity or manifest["version"] != project.version:
                raise Failure(
                    "Artifact was not built from this exact clean release input set"
                )
        return {
            "release_tag": args.version,
            "name": project.name,
            "version": project.version,
            "source": identity,
        }
    if operation == "publish":
        raise Usage(
            "No implicit publisher: adopt a profile-specific release workflow and configure its destination/authority"
        )
    if not project.managed:
        raise Usage(
            "This profile has no managed command installation; consume libraries using the package manager in a selected environment"
        )
    prefix = (
        args.prefix
        or Path(os.environ.get("XDG_DATA_HOME", str(Path.home() / ".local/share")))
        / "aixand/source"
        / project.name
    )
    if args.environment:
        raise Usage(
            "--environment is reserved for a future consumer-install adapter; use verify --install or the package manager"
        )
    if args.dry_run and operation != "prune":
        return {
            "planned_operation": operation,
            "prefix": str(prefix),
            "artifact": str(args.artifact) if args.artifact else "build from checkout",
            "executed": False,
        }
    if operation == "rollback":
        return rollback(project, prefix)
    if operation == "prune":
        return prune(project, prefix, dry_run=args.dry_run, stopped=args.stopped)
    if operation == "uninstall":
        return uninstall(project, prefix, stopped=args.stopped)
    if operation == "upgrade" and not args.artifact:
        raise Usage(
            "upgrade requires --artifact; v1 never fetches a moving latest release"
        )
    if args.artifact:
        return install(
            project,
            args.artifact,
            prefix,
            replace=operation != "install",
            python=args.python,
        )
    with tempfile.TemporaryDirectory(prefix="aixand-source-install-") as temporary:
        result = package(project, Path(temporary) / "package", python=args.python)
        return install(
            project,
            result["directory"],
            prefix,
            replace=operation != "install",
            python=args.python,
        )


def main(argv=None):
    try:
        args = parser().parse_args(argv)
        result = dispatch(args)
        if result is not None:
            if args.events == "jsonl":
                print(
                    json.dumps(
                        {"schema": 2, "stage": "result", "result": result},
                        sort_keys=True,
                    )
                )
            else:
                print(json.dumps(result, indent=2, sort_keys=True))
        return (
            1
            if args.operation == "doctor"
            and result is not None
            and not result.get("ready", False)
            else 0
        )
    except Usage as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    except (
        Failure,
        OSError,
        ValueError,
        KeyError,
        TypeError,
        StopIteration,
        tarfile.TarError,
        zipfile.BadZipFile,
    ) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
