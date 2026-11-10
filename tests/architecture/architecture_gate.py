#!/usr/bin/env python3

import argparse
import copy
import json
import os
import re
import subprocess
import sys
import tomllib


UNDERCURRENT_PACKAGES = (
    "aixand-undercurrent",
    "aixand-undercurrent-foundation",
    "aixand-undercurrent-multiplex",
    "aixand-undercurrent-platform-windows",
    "aixand-undercurrent-protocol",
    "aixand-undercurrent-transport-core",
    "aixand-undercurrent-transport-local",
    "aixand-undercurrent-wire",
    "undercurrent-application",
    "undercurrent-cli",
    "undercurrent-config",
    "undercurrent-daemon",
    "undercurrent-diagnostics",
    "undercurrent-domain",
    "undercurrent-mcp",
    "undercurrent-platform-macos",
    "undercurrent-python-bridge",
    "undercurrent-runtime",
    "undercurrent-store-sqlite",
)
from pathlib import Path


class Violation(Exception):
    def __init__(self, rule, detail):
        self.rule = rule
        self.detail = detail
        super().__init__(f"{rule}: {detail}")


def fail(rule, detail):
    raise Violation(rule, detail)


def normalize_vte_resolved_edges(edges):
    unique = {
        json.dumps(edge, sort_keys=True, separators=(",", ":")): edge
        for edge in edges
    }
    return sorted(
        unique.values(),
        key=lambda edge: (
            edge["consumer"]["package_id"],
            edge["dependency"]["package_id"],
            edge["dependency_name"],
            edge["kind"],
        ),
    )


def load_json(path):
    try:
        with path.open("r", encoding="utf-8") as source:
            return json.load(source)
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit(f"error: cannot parse {path}: {error}") from error


def run_json(command, cwd):
    completed = subprocess.run(
        command,
        cwd=cwd,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if completed.returncode != 0:
        sys.stderr.write(completed.stderr)
        raise SystemExit(f"error: command failed ({completed.returncode}): {' '.join(command)}")
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise SystemExit(f"error: command returned invalid JSON: {' '.join(command)}: {error}") from error


def run_text(command, cwd, env=None):
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if completed.returncode != 0:
        sys.stderr.write(completed.stdout)
        sys.stderr.write(completed.stderr)
        raise SystemExit(f"error: command failed ({completed.returncode}): {' '.join(command)}")
    return completed.stdout


def validate_allowed_graph(allowed):
    expected_keys = {
        "schema_version",
        "workspace_packages",
        "public_external_surfaces",
        "allowed_external_path_packages",
        "allowed_vendored_path_dependencies",
        "allowed_registry_reference_dependencies",
        "allowed_crates_io_patches",
        "allowed_vte_resolved_edges",
        "domain_forbidden_imports",
        "application_forbidden_imports",
        "cli_forbidden_imports",
        "cli_daemon_lifecycle_process_files",
        "cli_private_reset_module",
        "consumer_direct_dependencies",
        "consumer_forbidden_dependencies",
        "consumer_forbidden_symbol_fragments",
        "semantic_transport",
        "private_unix_packages",
    }
    if set(allowed) != expected_keys:
        raise SystemExit("error: allowed-graph.json has missing or unknown top-level fields")
    if allowed["schema_version"] != "undercurrent.architecture/v0.1.0-beta":
        raise SystemExit("error: unsupported architecture graph schema")
    if allowed["cli_private_reset_module"] != "crates/undercurrent-cli/src/reset.rs":
        raise SystemExit("error: private CLI Unix probing is restricted to the owned reset module")
    if set(allowed["public_external_surfaces"]) != {
        "aixand-undercurrent-protocol",
        "aixand-undercurrent",
    }:
        raise SystemExit("error: protocol and SDK must be the only public product surfaces")
    for package, disposition in allowed["workspace_packages"].items():
        if set(disposition) != {"disposition", "allowed_internal_dependencies"}:
            raise SystemExit(f"error: invalid disposition record for {package}")
        if disposition["allowed_internal_dependencies"] != sorted(
            disposition["allowed_internal_dependencies"]
        ):
            raise SystemExit(f"error: internal dependencies for {package} must be sorted")
    vendored = allowed["allowed_vendored_path_dependencies"]
    if not isinstance(vendored, list) or vendored != sorted(
        vendored,
        key=lambda item: (
            item["package"],
            item["name"],
            item["path"],
            item["kind"],
        ),
    ):
        raise SystemExit("error: vendored path dependencies must be a sorted list")
    for dependency in vendored:
        if set(dependency) != {"package", "name", "path", "kind"}:
            raise SystemExit("error: vendored path dependency fields are not exact")
        if dependency["kind"] not in {"normal", "build", "dev"}:
            raise SystemExit("error: vendored dependency kind is invalid")
        path = Path(dependency["path"])
        if path.is_absolute() or ".." in path.parts:
            raise SystemExit("error: vendored dependency path must be workspace-relative")
    references = allowed["allowed_registry_reference_dependencies"]
    reference_sort_key = lambda item: (
        item["package"],
        item["name"],
        item["rename"],
        item["requirement"],
        item["kind"],
        item["source"],
    )
    if not isinstance(references, list) or references != sorted(
        references, key=reference_sort_key
    ):
        raise SystemExit("error: registry reference dependencies must be a sorted list")
    for dependency in references:
        if set(dependency) != {
            "package",
            "name",
            "rename",
            "requirement",
            "kind",
            "source",
        }:
            raise SystemExit("error: registry reference dependency fields are not exact")
        if dependency["kind"] != "dev":
            raise SystemExit("error: registry reference dependency must be dev-only")
    patches = allowed["allowed_crates_io_patches"]
    if not isinstance(patches, list) or patches != sorted(
        patches, key=lambda item: (item["name"], item["path"], item["version"])
    ):
        raise SystemExit("error: crates.io patches must be a sorted list")
    for patch in patches:
        if set(patch) != {
            "name",
            "path",
            "resolved_consumers",
            "source",
            "version",
            "workspace_dependencies",
            "workspace_member",
        }:
            raise SystemExit("error: crates.io patch fields are not exact")
        path = Path(patch["path"])
        if path.is_absolute() or ".." in path.parts:
            raise SystemExit("error: crates.io patch path must be workspace-relative")
        if patch["resolved_consumers"] != sorted(patch["resolved_consumers"]):
            raise SystemExit("error: crates.io patch consumers must be sorted")
        if patch["workspace_dependencies"] != sorted(
            patch["workspace_dependencies"],
            key=lambda item: (item["package"], item["kind"], item["requirement"]),
        ):
            raise SystemExit("error: crates.io patch workspace dependencies must be sorted")
    vte_edges = allowed["allowed_vte_resolved_edges"]
    if not isinstance(vte_edges, list) or vte_edges != normalize_vte_resolved_edges(
        vte_edges
    ):
        raise SystemExit("error: resolved vte edges must be exact, unique, and sorted")
    identity_fields = {"name", "version", "source", "path", "package_id"}
    for edge in vte_edges:
        if set(edge) != {"consumer", "dependency", "dependency_name", "kind"}:
            raise SystemExit("error: resolved vte edge fields are not exact")
        for role in ("consumer", "dependency"):
            identity = edge[role]
            if set(identity) != identity_fields:
                raise SystemExit("error: resolved vte package identity fields are not exact")
            if (identity["source"] is None) == (identity["path"] is None):
                raise SystemExit("error: resolved vte identity must have exactly one source or path")
        if edge["dependency"]["name"] != "vte":
            raise SystemExit("error: resolved vte edge target must be vte")
        if edge["kind"] not in {"normal", "dev"}:
            raise SystemExit("error: resolved vte edge kind is invalid")
    if vte_edges:
        sample = vte_edges[0]
        if normalize_vte_resolved_edges([sample, sample]) != [sample]:
            raise SystemExit("error: duplicate resolved vte edges are not normalized")
        unexpected = copy.deepcopy(sample)
        unexpected["consumer"] = dict(unexpected["consumer"])
        unexpected["consumer"]["name"] = "undercurrent-daemon"
        unexpected["consumer"]["package_id"] = "path:crates/undercurrent-daemon#0.1.0-beta"
        if len(normalize_vte_resolved_edges([sample, unexpected])) != 2:
            raise SystemExit("error: distinct resolved vte consumers were collapsed")


def base_model(allowed):
    packages = sorted(allowed["workspace_packages"])
    edges = {
        package: list(record["allowed_internal_dependencies"])
        for package, record in allowed["workspace_packages"].items()
    }
    return {
        "packages": packages,
        "edges": edges,
        "imports": {package: [] for package in packages},
        "features": {package: [] for package in packages},
        "path_dependencies": [],
        "registry_reference_dependencies": [
            dict(dependency)
            for dependency in allowed["allowed_registry_reference_dependencies"]
        ],
        "crates_io_patches": copy.deepcopy(allowed["allowed_crates_io_patches"]),
        "vte_resolved_edges": copy.deepcopy(allowed["allowed_vte_resolved_edges"]),
        "consumer_dependencies": list(allowed["consumer_direct_dependencies"]),
        "consumer_symbols": [],
        "transports": [allowed["semantic_transport"]["package"]],
        "unix_uses": [],
    }


def validate_model(model, allowed):
    packages = set(model["packages"])

    transports = model["transports"]
    expected_transport = allowed["semantic_transport"]["package"]
    if transports != [expected_transport]:
        fail(
            "ARCH_LOCAL_TRANSPORT_COUNT",
            f"expected only {expected_transport}, found {transports}",
        )

    edges = {package: set(dependencies) for package, dependencies in model["edges"].items()}
    if edges.get("undercurrent-domain", set()) - {"aixand-undercurrent-foundation"}:
        fail("ARCH_DOMAIN_INWARD_PURITY", "domain has an internal outward dependency")
    application_dependencies = edges.get("undercurrent-application", set())
    if application_dependencies != {"undercurrent-domain"}:
        fail(
            "ARCH_APPLICATION_INWARD_PURITY",
            f"application dependencies are {sorted(application_dependencies)}",
        )
    runtime_dependencies = edges.get("undercurrent-runtime", set())
    if "undercurrent-store-sqlite" in runtime_dependencies:
        fail(
            "ARCH_RUNTIME_STORE_IMPLEMENTATION",
            "undercurrent-runtime depends on the concrete SQLite store adapter",
        )
    cli_dependencies = {
        dependency
        for dependency in edges.get("undercurrent-cli", set())
        if dependency.startswith(("undercurrent-", "aixand-undercurrent"))
    }
    # Windows supplies native terminal and exact-file reset primitives here;
    # workload runtime and store implementations remain forbidden CLI edges.
    if cli_dependencies != {
        "undercurrent-config", "undercurrent-diagnostics", "aixand-undercurrent-platform-windows",
        "aixand-undercurrent-protocol", "aixand-undercurrent",
    }:
        fail(
            "ARCH_CLI_PRODUCT_DEPENDENCIES",
            f"CLI product dependencies are {sorted(cli_dependencies)}",
        )

    imports = model["imports"]
    for symbol in imports.get("undercurrent-domain", []):
        if symbol in allowed["domain_forbidden_imports"]:
            fail("ARCH_DOMAIN_FORBIDDEN_IMPORT", symbol)
    for symbol in imports.get("undercurrent-application", []):
        if symbol in allowed["application_forbidden_imports"]:
            fail("ARCH_APPLICATION_FORBIDDEN_IMPORT", symbol)
    for symbol in imports.get("undercurrent-cli", []):
        if symbol in allowed["cli_forbidden_imports"]:
            fail("ARCH_CLI_LOCAL_EXECUTION", symbol)

    for package, features in model["features"].items():
        embedded = [feature for feature in features if "embedded" in feature.lower()]
        if embedded:
            fail("ARCH_EMBEDDED_RUNTIME", f"{package}: {embedded}")

    allowed_vendored = {
        (
            dependency["package"],
            dependency["name"],
            dependency["path"],
            dependency["kind"],
        )
        for dependency in allowed["allowed_vendored_path_dependencies"]
    }
    vendored_names = {dependency[1] for dependency in allowed_vendored}
    for dependency in model["path_dependencies"]:
        name = dependency["name"]
        if name in allowed["allowed_external_path_packages"]:
            continue
        dependency_identity = (
            dependency["package"],
            name,
            dependency["path"],
            dependency["kind"],
        )
        if name in vendored_names:
            if dependency_identity not in allowed_vendored:
                fail(
                    "ARCH_VENDORED_PATH_MISMATCH",
                    (
                        f"{dependency['package']} -> {name} "
                        f"({dependency['path']}, {dependency['kind']})"
                    ),
                )
            continue
        if name not in allowed["allowed_external_path_packages"]:
            fail(
                "ARCH_FOREIGN_PRODUCT_IMPLEMENTATION",
                f"{dependency['package']} -> {name} ({dependency['path']})",
            )

    reference_fields = (
        "package",
        "name",
        "rename",
        "requirement",
        "kind",
        "source",
    )
    def reference_identity(dependency):
        return tuple(
            (dependency[field] or "") if field == "rename" else dependency[field]
            for field in reference_fields
        )

    allowed_references = {
        reference_identity(dependency)
        for dependency in allowed["allowed_registry_reference_dependencies"]
    }
    observed_references = {
        reference_identity(dependency)
        for dependency in model["registry_reference_dependencies"]
    }
    for name, rule in (
        ("vt100", "ARCH_VT100_REFERENCE_MISMATCH"),
        ("vte", "ARCH_VTE_IDENTITY_MISMATCH"),
    ):
        observed = {identity for identity in observed_references if identity[1] == name}
        expected = {identity for identity in allowed_references if identity[1] == name}
        if observed != expected:
            fail(
                rule,
                (
                    f"registry {name} references differ: observed={sorted(observed)}, "
                    f"allowed={sorted(expected)}"
                ),
            )

    if model["crates_io_patches"] != allowed["allowed_crates_io_patches"]:
        fail(
            "ARCH_VTE_IDENTITY_MISMATCH",
            (
                "crates.io patches differ: "
                f"observed={model['crates_io_patches']}, "
                f"allowed={allowed['allowed_crates_io_patches']}"
            ),
        )
    if model["vte_resolved_edges"] != allowed["allowed_vte_resolved_edges"]:
        fail(
            "ARCH_VTE_IDENTITY_MISMATCH",
            (
                "resolved vte package identities differ: "
                f"observed={model['vte_resolved_edges']}, "
                f"allowed={allowed['allowed_vte_resolved_edges']}"
            ),
        )

    find_cycle(edges)

    consumer_dependencies = set(model["consumer_dependencies"])
    forbidden_consumer = consumer_dependencies.intersection(
        allowed["consumer_forbidden_dependencies"]
    )
    if forbidden_consumer:
        fail(
            "ARCH_CONSUMER_IMPLEMENTATION_EDGE",
            f"consumer reaches {sorted(forbidden_consumer)}",
        )
    if consumer_dependencies != set(allowed["consumer_direct_dependencies"]):
        fail(
            "ARCH_CONSUMER_PUBLIC_SURFACE",
            f"consumer direct dependencies are {sorted(consumer_dependencies)}",
        )
    for symbol in model["consumer_symbols"]:
        if any(fragment in symbol for fragment in allowed["consumer_forbidden_symbol_fragments"]):
            fail("ARCH_CONSUMER_SYMBOL_LEAK", symbol)

    private_unix_packages = set(allowed["private_unix_packages"])
    for use in model["unix_uses"]:
        if use["package"] not in private_unix_packages and not private_reset_use(
            use["package"], use["path"], use["symbol"], allowed
        ):
            fail(
                "ARCH_PUBLIC_UNIX_TRANSPORT",
                f"{use['package']}:{use['path']}:{use['symbol']}",
            )

    expected_packages = set(allowed["workspace_packages"])
    if packages != expected_packages:
        fail(
            "ARCH_WORKSPACE_DISPOSITION",
            f"missing={sorted(expected_packages - packages)} extra={sorted(packages - expected_packages)}",
        )
    for package, record in allowed["workspace_packages"].items():
        expected = set(record["allowed_internal_dependencies"])
        actual = edges.get(package, set())
        if actual != expected:
            fail(
                "ARCH_GRAPH_MISMATCH",
                f"{package}: expected {sorted(expected)}, found {sorted(actual)}",
            )


def find_cycle(edges):
    visiting = set()
    visited = set()

    def visit(package, path):
        if package in visiting:
            cycle = path[path.index(package) :] + [package]
            fail("ARCH_CYCLIC_TARGET_GRAPH", " -> ".join(cycle))
        if package in visited:
            return
        visiting.add(package)
        for dependency in sorted(edges.get(package, set())):
            if dependency in edges:
                visit(dependency, path + [dependency])
        visiting.remove(package)
        visited.add(package)

    for package in sorted(edges):
        visit(package, [package])


def production_sources(package):
    source_root = Path(package["manifest_path"]).parent / "src"
    return sorted(source_root.rglob("*.rs")) if source_root.is_dir() else []


def private_reset_use(package, path, symbol, allowed):
    # Local reset checks only whether its owned session-host socket is active.
    # Its native refusal test owns a listener fixture in the same module. Neither
    # is a public semantic transport; other CLI files and socket APIs stay denied.
    return (
        package == "undercurrent-cli"
        and path == allowed["cli_private_reset_module"]
        and symbol in {"std::os::unix::net::UnixStream", "std::os::unix::net::UnixListener"}
    )


def source_contains(text, symbol):
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    text = "\n".join(line.split("//", 1)[0] for line in text.splitlines())
    escaped = re.escape(symbol)
    if "::" in symbol:
        pattern = r"(?<![A-Za-z0-9_])" + escaped + r"(?![A-Za-z0-9_])"
    else:
        pattern = (
            r"(?:^|\n)\s*(?:pub\s+)?(?:use|extern\s+crate)\s+(?:::)?"
            + escaped
            + r"(?:\b|::)|(?<![A-Za-z0-9_])"
            + escaped
            + r"::"
        )
    return re.search(pattern, text) is not None


def validate_persisted_dispatch_decode_boundary(root):
    symbols = (
        "RuntimeDispatchBinding::legacy_without_request_digest",
        "RuntimeDispatchBinding::legacy_without_policy_and_request_digest",
    )
    allowed = set()
    observed = set()
    for source in sorted((Path(root) / "crates").glob("*/src/**/*.rs")):
        if source.name == "tests.rs" or source.stem.endswith("_tests"):
            continue
        text = source.read_text(encoding="utf-8")
        if any(source_contains(text, symbol) for symbol in symbols):
            observed.add(source.relative_to(root).as_posix())
    if observed != allowed:
        fail(
            "ARCH_PERSISTED_DISPATCH_DECODE_BOUNDARY",
            f"production decode sites differ: observed={sorted(observed)}, allowed={sorted(allowed)}",
        )


def resolved_package_identity(package, root):
    source = package.get("source")
    version = package["version"]
    name = package["name"]
    if source is None:
        directory = Path(package["manifest_path"]).parent.resolve()
        try:
            path = directory.relative_to(Path(root).resolve()).as_posix()
        except ValueError:
            path = str(directory)
        expected_package_id = f"path+{directory.as_uri()}#{version}"
        if package["id"] != expected_package_id:
            fail(
                "ARCH_VTE_IDENTITY_MISMATCH",
                f"unexpected Cargo path package ID {package['id']!r}",
            )
        package_id = f"path:{path}#{version}"
    else:
        path = None
        expected_package_id = f"{source}#{name}@{version}"
        if package["id"] != expected_package_id:
            fail(
                "ARCH_VTE_IDENTITY_MISMATCH",
                f"unexpected Cargo registry package ID {package['id']!r}",
            )
        package_id = package["id"]
    return {
        "name": name,
        "version": version,
        "source": source,
        "path": path,
        "package_id": package_id,
    }


def model_from_workspace(root, allowed):
    workspace_root = Path(root).resolve()
    workspace_manifest = workspace_root / "Cargo.toml"
    metadata = run_json(
        [
            "cargo",
            "metadata",
            "--manifest-path",
            str(workspace_manifest),
            "--locked",
            "--all-features",
            "--format-version",
            "1",
        ],
        root,
    )
    packages_by_id = {package["id"]: package for package in metadata["packages"]}
    all_members = set(metadata["workspace_members"])
    members = all_members
    for package in metadata["packages"]:
        if package.get("source") is None and not Path(package["manifest_path"]).resolve().is_relative_to(Path(root).resolve()):
            fail("ARCH_EXTERNAL_PATH_DEPENDENCY", f"local dependency escapes project: {package['name']}")
    selected_names = [packages_by_id[package_id]["name"] for package_id in members]
    if sorted(selected_names) != list(UNDERCURRENT_PACKAGES):
        fail(
            "ARCH_WORKSPACE_DISPOSITION",
            f"root workspace Undercurrent selection differs: {sorted(selected_names)}",
        )
    resolve_nodes = {
        node["id"]: node for node in metadata["resolve"]["nodes"]
    }
    reachable = set(members)
    pending = list(members)
    while pending:
        package_id = pending.pop()
        for dependency in resolve_nodes[package_id]["deps"]:
            target = dependency["pkg"]
            if target not in resolve_nodes:
                fail(
                    "ARCH_TREE_METADATA_MISMATCH",
                    f"resolve edge targets missing package {target}",
                )
            if target not in reachable:
                reachable.add(target)
                pending.append(target)
    workspace_packages = {packages_by_id[package_id]["name"]: packages_by_id[package_id] for package_id in members}
    model = base_model(allowed)
    model["packages"] = sorted(workspace_packages)
    model["edges"] = {package: [] for package in workspace_packages}
    for node in metadata["resolve"]["nodes"]:
        if node["id"] not in members:
            continue
        package = packages_by_id[node["id"]]["name"]
        model["edges"][package] = sorted(
            packages_by_id[dependency["pkg"]]["name"]
            for dependency in node["deps"]
            if dependency["pkg"] in members
            and any(kind["kind"] in (None, "normal", "build") for kind in dependency["dep_kinds"])
        )

    model["features"] = {
        name: sorted(package.get("features", {})) for name, package in workspace_packages.items()
    }
    model["path_dependencies"] = []
    model["registry_reference_dependencies"] = []
    registry_reference_names = {
        dependency["name"]
        for dependency in allowed["allowed_registry_reference_dependencies"]
    }
    for package_name, package in workspace_packages.items():
        for dependency in package["dependencies"]:
            if dependency["name"] in workspace_packages:
                continue
            path = dependency.get("path")
            if (
                dependency["name"] in registry_reference_names
                and path is None
                and dependency.get("source") is not None
            ):
                model["registry_reference_dependencies"].append(
                    {
                        "package": package_name,
                        "name": dependency["name"],
                        "rename": dependency.get("rename"),
                        "requirement": dependency["req"],
                        "kind": dependency.get("kind") or "normal",
                        "source": dependency["source"],
                    }
                )
            if path is not None:
                dependency_path = Path(path).resolve()
                try:
                    normalized_path = dependency_path.relative_to(Path(root).resolve()).as_posix()
                except ValueError:
                    normalized_path = str(dependency_path)
                model["path_dependencies"].append(
                    {
                        "package": package_name,
                        "name": dependency["name"],
                        "path": normalized_path,
                        "kind": dependency.get("kind") or "normal",
                    }
                )
    model["registry_reference_dependencies"].sort(
        key=lambda item: (
            item["package"],
            item["name"],
            item["rename"] or "",
            item["requirement"],
            item["kind"],
            item["source"],
        )
    )
    with workspace_manifest.open("rb") as source:
        root_manifest = tomllib.load(source)
    patch_declarations = root_manifest.get("patch", {}).get("crates-io", {})
    model["crates_io_patches"] = []
    for name, declaration in sorted(patch_declarations.items()):
        declared_path = declaration.get("path") if isinstance(declaration, dict) else None
        declared_absolute = (workspace_root / str(declared_path)).resolve()
        try:
            product_declared_path = declared_absolute.relative_to(Path(root).resolve()).as_posix()
        except ValueError:
            product_declared_path = str(declared_path)
        package_candidates = [
            package
            for package in metadata["packages"]
            if package["name"] == name
            and package.get("source") is None
            and Path(package["manifest_path"]).parent.resolve()
            == declared_absolute
        ]
        package = package_candidates[0] if len(package_candidates) == 1 else None
        package_id = package["id"] if package is not None else None
        workspace_dependencies = []
        for package_name, workspace_package in workspace_packages.items():
            for dependency in workspace_package["dependencies"]:
                if dependency["name"] == name:
                    workspace_dependencies.append(
                        {
                            "package": package_name,
                            "kind": dependency.get("kind") or "normal",
                            "requirement": dependency["req"],
                        }
                    )
        resolved_consumers = sorted(
            {
                packages_by_id[node["id"]]["name"]
                for node in metadata["resolve"]["nodes"]
                if node["id"] in members
                if package_id is not None
                and any(dependency["pkg"] == package_id for dependency in node["deps"])
            }
        )
        model["crates_io_patches"].append(
            {
                "name": name,
                "path": product_declared_path,
                "resolved_consumers": resolved_consumers,
                "source": package.get("source") if package is not None else "missing",
                "version": package["version"] if package is not None else "missing",
                "workspace_dependencies": sorted(
                    workspace_dependencies,
                    key=lambda item: (
                        item["package"],
                        item["kind"],
                        item["requirement"],
                    ),
                ),
                "workspace_member": package_id in all_members,
            }
        )

    resolved_vte_edges = []
    for node in metadata["resolve"]["nodes"]:
        if node["id"] not in reachable:
            continue
        consumer = packages_by_id[node["id"]]
        for dependency in node["deps"]:
            target = packages_by_id[dependency["pkg"]]
            if target["name"] != "vte":
                continue
            for dependency_kind in dependency["dep_kinds"]:
                resolved_vte_edges.append(
                    {
                        "consumer": resolved_package_identity(consumer, root),
                        "dependency": resolved_package_identity(target, root),
                        "dependency_name": dependency["name"].replace("_", "-"),
                        "kind": dependency_kind["kind"] or "normal",
                    }
                )
    model["vte_resolved_edges"] = normalize_vte_resolved_edges(resolved_vte_edges)

    model["imports"] = {package: [] for package in workspace_packages}
    model["unix_uses"] = []
    unix_symbols = [
        "std::os::unix::net::UnixListener",
        "std::os::unix::net::UnixStream",
        "tokio::net::UnixListener",
        "tokio::net::UnixStream",
        "libc::AF_UNIX",
        "Domain::UNIX",
    ]
    for package_name, package in workspace_packages.items():
        scan_symbols = set()
        if package_name == "undercurrent-domain":
            scan_symbols.update(allowed["domain_forbidden_imports"])
        if package_name == "undercurrent-application":
            scan_symbols.update(allowed["application_forbidden_imports"])
        if package_name == "undercurrent-cli":
            scan_symbols.update(allowed["cli_forbidden_imports"])
        for source in production_sources(package):
            text = source.read_text(encoding="utf-8")
            for symbol in sorted(scan_symbols):
                relative_source = source.relative_to(Path(package["manifest_path"]).parent).as_posix()
                lifecycle_process_use = (
                    package_name == "undercurrent-cli"
                    and symbol in {"std::process::Command", "crate::process_launch"}
                    and relative_source in allowed["cli_daemon_lifecycle_process_files"]
                )
                reset_probe_use = private_reset_use(
                    package_name, source.relative_to(root).as_posix(), symbol, allowed
                )
                if source_contains(text, symbol) and not (lifecycle_process_use or reset_probe_use):
                    model["imports"][package_name].append(symbol)
            for symbol in unix_symbols:
                if source_contains(text, symbol):
                    model["unix_uses"].append(
                        {
                            "package": package_name,
                            "path": source.relative_to(root).as_posix(),
                            "symbol": symbol,
                        }
                    )

    transport_packages = sorted(
        package
        for package in workspace_packages
        if package.startswith("aixand-undercurrent-transport-") and package != "aixand-undercurrent-transport-core"
    )
    model["transports"] = transport_packages
    validate_tree_crosscheck(workspace_root, members, packages_by_id, model["edges"])
    return model


def validate_tree_crosscheck(root, members, packages_by_id, metadata_edges):
    workspace_names = {packages_by_id[package_id]["name"] for package_id in members}
    output = run_text(
        [
            "cargo",
            "tree",
            # Manifest metadata includes conditional dependencies for every OS.
            # Inspect that same graph without compiling or running other targets.
            "--target",
            "all",
            "--manifest-path",
            str(Path(root) / "Cargo.toml"),
            "--locked",
            "--all-features",
            *[
                argument
                for package in sorted(workspace_names)
                for argument in ("-p", package)
            ],
            "--edges",
            "normal,build",
            "--prefix",
            "depth",
            "--no-dedupe",
            "--format",
            "{p}",
        ],
        root,
    )
    seen_roots = set()
    tree_edges = set()
    stack = []
    for line in output.splitlines():
        match = re.match(r"^(\d+)([A-Za-z0-9_-]+)\s+v", line)
        if match is None:
            continue
        depth = int(match.group(1))
        package = match.group(2)
        if depth == 0:
            seen_roots.add(package)
        if depth < len(stack):
            stack = stack[:depth]
        if depth > 0 and depth - 1 < len(stack):
            parent = stack[depth - 1]
            if parent in workspace_names and package in workspace_names:
                tree_edges.add((parent, package))
        if depth == len(stack):
            stack.append(package)
        else:
            stack[depth] = package
    if seen_roots != workspace_names:
        fail(
            "ARCH_TREE_METADATA_MISMATCH",
            f"tree roots differ from metadata: {sorted(seen_roots)}",
        )
    expected_edges = {
        (package, dependency)
        for package, dependencies in metadata_edges.items()
        for dependency in dependencies
    }
    if not expected_edges.issubset(tree_edges):
        fail(
            "ARCH_TREE_METADATA_MISMATCH",
            f"tree omitted edges {sorted(expected_edges - tree_edges)}",
        )


def validate_fixture_shape(fixture):
    if set(fixture) != {
        "schema_version",
        "name",
        "description",
        "mutation",
        "expected_rule",
    }:
        raise SystemExit("error: negative fixture has missing or unknown fields")
    if fixture["schema_version"] != "undercurrent.architecture-negative/v0.1.0-beta":
        raise SystemExit("error: unsupported negative fixture schema")
    mutation = fixture["mutation"]
    if not isinstance(mutation, dict) or "kind" not in mutation:
        raise SystemExit("error: negative fixture mutation is invalid")


def apply_mutation(model, mutation):
    kind = mutation["kind"]
    if kind == "add_edge":
        model["edges"].setdefault(mutation["from"], []).append(mutation["to"])
    elif kind == "add_import":
        model["imports"].setdefault(mutation["package"], []).append(mutation["symbol"])
    elif kind == "add_feature":
        model["features"].setdefault(mutation["package"], []).append(mutation["feature"])
    elif kind == "add_path_dependency":
        model["path_dependencies"].append(
            {
                "package": mutation["package"],
                "name": mutation["name"],
                "path": mutation["path"],
                "kind": mutation.get("dependency_kind", "normal"),
            }
        )
    elif kind == "add_registry_reference_dependency":
        model["registry_reference_dependencies"].append(
            {
                "package": mutation["package"],
                "name": mutation["name"],
                "rename": mutation["rename"],
                "requirement": mutation["requirement"],
                "kind": mutation["dependency_kind"],
                "source": mutation["source"],
            }
        )
    elif kind == "set_crates_io_patch_path":
        model["crates_io_patches"][0]["path"] = mutation["path"]
    elif kind == "set_crates_io_patch_name":
        model["crates_io_patches"][0]["name"] = mutation["name"]
    elif kind == "add_crates_io_patch_consumer":
        model["crates_io_patches"][0]["resolved_consumers"].append(
            mutation["package"]
        )
        model["crates_io_patches"][0]["resolved_consumers"].sort()
    elif kind == "add_vte_resolved_edge":
        model["vte_resolved_edges"].append(copy.deepcopy(mutation["edge"]))
        model["vte_resolved_edges"] = normalize_vte_resolved_edges(
            model["vte_resolved_edges"]
        )
    elif kind == "add_consumer_dependency":
        model["consumer_dependencies"].append(mutation["package"])
    elif kind == "add_consumer_symbol":
        model["consumer_symbols"].append(mutation["symbol"])
    elif kind == "add_transport":
        model["transports"].append(mutation["package"])
    elif kind == "add_package":
        model["packages"].append(mutation["package"])
        model["edges"][mutation["package"]] = []
        model["imports"][mutation["package"]] = []
        model["features"][mutation["package"]] = []
    elif kind == "add_unix_use":
        model["unix_uses"].append(
            {
                "package": mutation["package"],
                "path": mutation["path"],
                "symbol": mutation["symbol"],
            }
        )
    else:
        raise SystemExit(f"error: unsupported negative mutation kind {kind!r}")


def run_negative_fixture(path, allowed):
    fixture = load_json(path)
    validate_fixture_shape(fixture)
    model = base_model(allowed)
    apply_mutation(model, fixture["mutation"])
    try:
        validate_model(model, allowed)
    except Violation as error:
        if error.rule != fixture["expected_rule"]:
            raise SystemExit(
                f"error: {path.name} raised {error.rule}, expected {fixture['expected_rule']}: {error.detail}"
            ) from error
        print(f"negative fixture passed: {fixture['name']} -> {error.rule}")
        return
    raise SystemExit(f"error: negative fixture unexpectedly passed: {path.name}")


def consumer_model(root, allowed, target_dir):
    target_dir = target_dir.resolve()
    manifest = root / "tests/architecture/consumer/Cargo.toml"
    metadata = run_json(
        [
            "cargo",
            "metadata",
            "--manifest-path",
            str(manifest),
            "--locked",
            "--offline",
            "--all-features",
            "--format-version",
            "1",
        ],
        root,
    )
    members = set(metadata["workspace_members"])
    packages_by_id = {package["id"]: package for package in metadata["packages"]}
    if len(members) != 1:
        fail("ARCH_CONSUMER_PUBLIC_SURFACE", "consumer fixture must be its own one-package workspace")
    consumer = packages_by_id[next(iter(members))]
    direct = sorted(
        dependency["name"]
        for dependency in consumer["dependencies"]
        if dependency.get("kind") in (None, "normal")
    )
    tree = run_text(
        [
            "cargo",
            "tree",
            "--manifest-path",
            str(manifest),
            "--locked",
            "--offline",
            "--all-features",
            "--edges",
            "normal,build",
            "--prefix",
            "depth",
            "--no-dedupe",
            "--format",
            "{p}",
        ],
        root,
    )
    tree_packages = {
        match.group(2)
        for line in tree.splitlines()
        if (match := re.match(r"^(\d+)([A-Za-z0-9_-]+)\s+v", line)) is not None
    }
    forbidden_tree = tree_packages.intersection(allowed["consumer_forbidden_dependencies"])
    if forbidden_tree:
        fail(
            "ARCH_CONSUMER_IMPLEMENTATION_EDGE",
            f"consumer dependency tree reaches {sorted(forbidden_tree)}",
        )
    environment = os.environ.copy()
    environment["CARGO_TARGET_DIR"] = str(target_dir)
    build_command = [
            "cargo",
            "rustc" if os.name == "nt" else "build",
            "--manifest-path",
            str(manifest),
            "--locked",
            "--offline",
            "--all-features",
        ]
    if os.name == "nt":
        # MSVC executables put debug symbols in a PDB, not an ELF symbol table.
        # A linker map records the linked Rust symbols without needing Unix nm.
        # Their crate-name fragments remain visible in Rust's mangled names.
        symbol_map = target_dir / "consumer-symbols.map"
        build_command += ["--bin", "operation-boundary-consumer", "--",
                          "-C", f"link-arg=/MAP:{symbol_map}"]
    run_text(
        build_command,
        root,
        environment,
    )
    if os.name == "nt":
        symbols = symbol_map.read_text(encoding="utf-8", errors="replace")
        if "operation_boundary_consumer" not in symbols:
            fail("ARCH_CONSUMER_PUBLIC_SURFACE", "MSVC map omitted consumer symbols")
    else:
        binary = target_dir / "debug/operation-boundary-consumer"
        symbols = run_text(["nm", "-C", "--defined-only", str(binary)], root)
    model = base_model(allowed)
    model["consumer_dependencies"] = direct
    model["consumer_symbols"] = [
        line
        for line in symbols.splitlines()
        if any(
            fragment in line for fragment in allowed["consumer_forbidden_symbol_fragments"]
        )
    ]
    return model


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument(
        "--mode", required=True, choices=("all", "positive", "negative", "consumer")
    )
    parser.add_argument("--fixture", type=Path)
    parser.add_argument("--target-dir", type=Path)
    arguments = parser.parse_args()
    root = arguments.root.resolve()
    allowed = load_json(root / "tests/architecture/allowed-graph.json")
    validate_allowed_graph(allowed)

    if arguments.mode in ("all", "positive"):
        model = model_from_workspace(root, allowed)
        validate_model(model, allowed)
        validate_persisted_dispatch_decode_boundary(root)
        print(
            "positive architecture gate passed: "
            f"{len(model['packages'])} workspace packages, "
            f"{sum(len(edges) for edges in model['edges'].values())} internal edges"
        )

    if arguments.mode == "negative":
        if arguments.fixture is None:
            raise SystemExit("error: --negative requires --fixture")
        run_negative_fixture(arguments.fixture, allowed)
    elif arguments.mode == "all":
        negative_root = root / "tests/architecture/negative"
        fixtures = sorted(negative_root.glob("*.json"))
        if len(fixtures) < 15:
            raise SystemExit("error: architecture gate requires at least 15 negative fixtures")
        for fixture in fixtures:
            run_negative_fixture(fixture, allowed)

    if arguments.mode in ("all", "consumer"):
        if arguments.target_dir is None:
            raise SystemExit("error: consumer mode requires --target-dir")
        model = consumer_model(root, allowed, arguments.target_dir)
        validate_model(model, allowed)
        print("consumer boundary passed: Undercurrent protocol/SDK only")


if __name__ == "__main__":
    main()
