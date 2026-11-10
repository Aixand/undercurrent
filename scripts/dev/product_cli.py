"""Workspace dispatch and public artifact operations, independent of source checkouts."""

from __future__ import annotations

import os
import platform
from pathlib import Path
import shutil
import sys
import tempfile
import tomllib

from catalog import Workspace, host, identity
from components import expand, version
from core import Usage, digest, read_json, require, run
import operations
import releases


OPTIONS = {
    "commands": {"prefix", "command", "register_path", "command_policy"},
    "help": set(),
    "doctor": set(),
    "develop": set(),
    "check": set(),
    "test": set(),
    "build": {"output", "target", "component", "product", "variant", "release"},
    "package": {"output", "target", "component", "product", "variant"},
    "verify": {"artifact", "install_proof", "python", "expected_sha256", "report"},
    "install": {
        "accept_license",
        "artifact",
        "prefix",
        "product",
        "scope",
        "owner",
        "python",
        "request_id",
        "events",
        "expected_sha256",
        "dry_run",
        "allow_http_localhost",
    },
    "reinstall": {
        "accept_license",
        "artifact",
        "prefix",
        "product",
        "scope",
        "owner",
        "python",
        "request_id",
        "events",
        "expected_sha256",
        "dry_run",
        "allow_http_localhost",
    },
    "upgrade": {
        "accept_license",
        "artifact",
        "prefix",
        "product",
        "scope",
        "owner",
        "python",
        "request_id",
        "events",
        "expected_sha256",
        "dry_run",
        "allow_http_localhost",
    },
    "inspect": {"prefix", "product"},
    "plan": {"artifact", "prefix", "product", "scope", "owner", "expected_sha256"},
    "export-release": {"artifact", "output", "expected_sha256", "report"},
    "release-check": {"artifact", "version", "product", "variant"},
    "rollback": {"prefix", "product", "dry_run"},
    "prune": {"prefix", "dry_run", "stopped"},
    "uninstall": {"prefix", "product", "dry_run", "stopped"},
    "publish": set(),
}

for _operation in ("install", "reinstall", "upgrade"):
    OPTIONS[_operation].update({"register_path", "command_policy"})


def command_result(args, result):
    if args.register_path:
        from command_environment import register, inspect_commands

        result["terminal"] = {
            **register(args.prefix, args.command_policy or "keep"),
            **inspect_commands(args.prefix),
        }
    return result


def handles(args):
    if (
        args.operation in {"inspect", "plan", "export-release", "commands"}
        or args.product
        or args.component
        or args.variant
    ):
        return True
    if args.artifact:
        path = Path(args.artifact)
        if path.is_dir():
            path /= releases.MANIFEST
        if path.is_file() and read_json(path).get("schema") == 2:
            return True
    config = args.project / "installation.toml"
    return config.is_file() and tomllib.loads(config.read_text()).get("schema") == 2


def dispatch(args):
    unexpected = [
        key
        for key, value in vars(args).items()
        if value not in (None, False)
        and key not in {"project", "operation", "json"} | OPTIONS[args.operation]
    ]
    if unexpected:
        raise Usage("Options do not apply: " + ", ".join(unexpected))
    operation = args.operation
    require(
        not args.command_policy or args.register_path,
        "--command-policy requires --register-path",
    )
    if operation == "commands":
        from command_environment import inspect_commands

        require(args.prefix, "Select an installation store with --prefix")
        registration = (
            command_result(args, {}).get("terminal", {}) if args.register_path else {}
        )
        return registration or inspect_commands(args.prefix, args.command or ())
    if operation in {"install", "reinstall", "upgrade", "plan"} and args.prefix is None:
        require(
            args.scope != "private",
            "Private installation requires an explicit --prefix",
        )
        if os.name == "nt":
            base = (
                Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData/Local"))
                / "Aixand"
            )
        elif sys.platform == "darwin":
            base = Path.home() / "Library/Application Support/Aixand"
        else:
            base = (
                Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local/share"))
                / "aixand"
            )
        args.prefix = base / (
            "source-products" if args.artifact is None else "products"
        )
    if operation in {"install", "reinstall"} and args.artifact is None:
        workspace = Workspace(args.project)
        require(
            not args.expected_sha256, "--expected-sha256 requires a prebuilt artifact"
        )
        selection = args.product or workspace.default
        selected = workspace.select(selection)
        require(args.prefix, "Select an installation store with --prefix")
        if args.dry_run:
            return {
                "schema": 2,
                "executed": False,
                "origin": "source",
                "selection": selection,
                "products": selected,
                "prefix": str(args.prefix),
                "verification_required": True,
            }
        with tempfile.TemporaryDirectory(prefix="source-product-install-") as temporary:
            artifact = releases.package(
                workspace, Path(temporary) / "release", product=selection
            )
            return command_result(
                args,
                operations.install(
                    artifact["directory"],
                    args.prefix,
                    product=selection,
                    operation=operation,
                    scope=args.scope or "shared",
                    owner=args.owner or "user",
                    python=args.python,
                    request=args.request_id,
                    events=args.events == "jsonl",
                    origin="source",
                    accepted_licenses=args.accept_license or [],
                    allow_local=args.allow_http_localhost,
                ),
            )
    if operation == "publish":
        raise Usage(
            "Configure an explicit destination adapter; export-release does not publish"
        )
    if operation in {"inspect", "uninstall", "rollback", "prune"}:
        require(args.prefix, "Select an installation store with --prefix")
        if operation == "inspect":
            result = operations.inspect(args.prefix)
            if args.product:
                require(
                    args.product in result["products"]
                    or args.product in result["groups"],
                    "Unknown installed product/group",
                )
            return result
        if operation == "prune":
            return operations.prune(
                args.prefix, dry_run=args.dry_run, stopped=args.stopped
            )
        require(args.product, "Select --product")
        if operation == "uninstall":
            return operations.remove(
                args.prefix, args.product, dry_run=args.dry_run, stopped=args.stopped
            )
        return operations.rollback(args.prefix, args.product, dry_run=args.dry_run)
    if operation in {
        "verify",
        "plan",
        "export-release",
        "install",
        "reinstall",
        "upgrade",
    }:
        require(
            args.artifact,
            "Artifact operations require --artifact; use package to build source explicitly",
        )
        artifact, manifest = releases.validate(
            args.artifact, expected=args.expected_sha256
        )
        if operation == "verify":
            if args.report:
                require(
                    not args.report.exists()
                    and not args.report.resolve().is_relative_to(artifact.resolve()),
                    "Verification report needs a new destination outside the immutable artifact",
                )
            component_proof = {}
            if args.install_proof:
                if manifest["kind"] == "component":
                    component_proof = operations.verify_component(
                        artifact, manifest, args.python
                    )
                else:
                    with tempfile.TemporaryDirectory(
                        prefix="product-install-proof-"
                    ) as temporary:
                        operations.install(
                            artifact,
                            Path(temporary) / "store",
                            python=args.python,
                            accepted_licenses=[
                                v["license_acceptance"]["sha256"]
                                for v in manifest["products"].values()
                                if "license_acceptance" in v
                            ],
                        )
            result = {
                "schema": 2,
                "kind": "installation-verification",
                "verified": True,
                "installation_tested": args.install_proof,
                "manifest_sha256": digest(artifact / releases.MANIFEST),
                "target": manifest["target"],
                "helper_python": platform.python_version(),
                "checks": {
                    p: len(v["verification"]) for p, v in manifest["products"].items()
                },
                **component_proof,
            }
            if args.report:
                from core import json_bytes

                args.report.parent.mkdir(parents=True, exist_ok=True)
                with args.report.open("xb") as stream:
                    stream.write(json_bytes(result))
            return result
        if operation == "export-release":
            require(args.output, "export-release requires --output")
            return releases.export(artifact, args.output, report=args.report)
        require(args.prefix, "Select an installation store with --prefix")
        values = {
            "product": args.product,
            "scope": args.scope or "shared",
            "owner": args.owner or "user",
            "expected": args.expected_sha256,
        }
        if operation == "plan" or args.dry_run:
            return operations.plan(
                artifact,
                args.prefix,
                operation="install" if operation == "plan" else operation,
                **values,
            )
        return command_result(
            args,
            operations.install(
                artifact,
                args.prefix,
                operation=operation,
                python=args.python,
                request=args.request_id,
                events=args.events == "jsonl",
                allow_local=args.allow_http_localhost,
                accepted_licenses=args.accept_license or [],
                **values,
            ),
        )
    workspace = Workspace(args.project)
    if operation == "doctor":
        tools = {}
        for component in workspace.components.values():
            for tool in {
                "python": ["uv"],
                "cargo": ["rustup"],
                "payload": [],
                "legacy": [],
            }[component["adapter"]]:
                tools[tool] = shutil.which(tool)
        return {
            "schema": 2,
            "ready": all(tools.values()),
            "tools": tools,
            "products": sorted(workspace.products),
            "groups": sorted(workspace.groups),
            "targets": workspace.targets,
            "helper_revision": 2,
            "capabilities": sorted(
                (OPTIONS.keys() - {"publish", "develop", "check", "test"})
                | workspace.commands.keys()
            ),
            "mutation_performed": False,
        }
    if operation in {"develop", "check", "test"}:
        if operation not in workspace.commands:
            raise Usage("Configure a real workspace command for " + operation)
        for argv in workspace.commands[operation]:
            run(
                expand(argv, {"root": workspace.root, "python": sys.executable}),
                workspace.root,
            )
        return {"operation": operation, "completed": True}
    if operation in {"build", "package"}:
        if operation == "build":
            from components import build

            selected = (
                workspace.select(args.product, args.variant)
                if not args.component
                else []
            )
            names = (
                [args.component]
                if args.component
                else [
                    c
                    for p in selected
                    for c in workspace.product(
                        p, args.variant if p == args.product else None
                    )["components"]
                ]
            )
            destination = Path(args.output or workspace.root / "dist/build").absolute()
            require(not destination.exists(), "Build output already exists")
            destination.mkdir(parents=True)
            target = args.target or host()
            require(target in workspace.targets, "Target is not declared")
            build(workspace, names, destination, target, release=args.release)
            return {"directory": str(destination), "release": args.release}
        return releases.package(
            workspace,
            args.output,
            product=args.product,
            component=args.component,
            variant=args.variant,
            target=args.target,
        )
    if operation == "release-check":
        product = args.product or workspace.default
        selected = workspace.select(product, args.variant)
        record = (
            workspace.groups[product]
            if product in workspace.groups
            else workspace.products[product]
        )
        release_version = (
            record.get("version", workspace.metadata["version"])
            if product in workspace.groups
            else version(workspace, product)
        )
        pattern = record.get(
            "release-tag", workspace.metadata.get("release-tag", "{product}/v{version}")
        )
        tag = pattern.replace("{product}", product).replace(
            "{version}", release_version
        )
        require(
            args.version == tag,
            "Release tag differs from selected product/version mapping",
        )
        sources = {name: identity(root) for name, root in workspace.sources.items()}
        require(
            all(value["commit"] and not value["dirty"] for value in sources.values()),
            "Release sources must be clean committed inputs",
        )
        tagged = run(
            ["git", "rev-parse", "--verify", "refs/tags/" + tag + "^{commit}"],
            workspace.root,
            capture=True,
        ).strip()
        require(
            tagged == sources["project"]["commit"],
            "Tag differs from selected project revision",
        )
        if args.artifact:
            _, manifest = releases.validate(args.artifact)
            require(
                manifest["sources"] == sources
                and manifest["selection"] == product
                and manifest["variant"] == args.variant
                and manifest["version"] == release_version
                and all(
                    record["version"] == version(workspace, name)
                    for name, record in manifest["products"].items()
                ),
                "Artifact differs from selected clean source/product",
            )
            require(
                set(selected) == set(manifest["products"]),
                "Artifact product set differs",
            )
        return {
            "release_tag": tag,
            "version": release_version,
            "products": selected,
            "sources": sources,
        }
    raise Usage("Unsupported schema-2 operation: " + operation)
