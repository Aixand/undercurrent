"""Trusted source-side adapters. Artifact installation never executes these hooks."""

from __future__ import annotations

from pathlib import Path
import shutil
import sys
import tomllib
import re

from core import Project, relative, require, run, source_file
from packages import package as legacy_package


def expand(argv, values):
    result = []
    for arg in argv:
        for name, value in values.items():
            arg = arg.replace("{" + name + "}", str(value))
        require(
            not re.search(r"\{[A-Za-z][A-Za-z0-9:_-]*\}", arg),
            "Unknown command placeholder",
        )
        result.append(arg)
    return result


def payload(workspace, name, output, target, built, release=True):
    item = workspace.components[name]
    root = workspace.component_root(name)
    values = {
        "root": workspace.root,
        "source": root,
        "output": output,
        "target": target,
        "python": sys.executable,
        "profile": "release" if release else "debug",
    }
    values.update({f"component:{k}": v for k, v in built.items()})
    for argv in item.get("build", []):
        run(expand(argv, values), root)
    for spec in item.get("files", []):
        source = source_file(root, spec["source"])
        destination = output / relative(spec["destination"])
        require(not destination.exists(), "Duplicate component destination")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
        destination.chmod(0o755 if spec.get("executable", False) else 0o644)


def python(workspace, name, output, target, built, release=True):
    from catalog import host

    require(
        target == host(),
        "Python components must be built on the selected native target",
    )
    item = workspace.components[name]
    root = workspace.component_root(name)
    require(item.get("python"), "Python component must select its build interpreter")
    args = [
        "uv",
        "build",
        "--wheel",
        "--no-sources",
        "--python",
        item["python"],
        "--out-dir",
        output / "wheelhouse",
        root,
    ]
    run(args, root)
    for path in item.get("wheels", []):
        source = source_file(root, path)
        require(source.suffix == ".whl", "Dependency wheel must be a wheel")
        destination = output / "wheelhouse" / source.name
        require(not destination.exists(), "Duplicate wheel filename")
        shutil.copyfile(source, destination)
    require(
        not item.get("extras"), "Resolve extras into the declared dependency wheel set"
    )


def cargo(workspace, name, output, target, built, release=True):
    item = workspace.components[name]
    root = workspace.component_root(name)
    manifest = root / relative(item.get("manifest", "Cargo.toml"))
    triple = {
        "linux-x86_64-gnu": "x86_64-unknown-linux-gnu",
        "linux-arm64-gnu": "aarch64-unknown-linux-gnu",
        "macos-x86_64": "x86_64-apple-darwin",
        "macos-arm64": "aarch64-apple-darwin",
        "windows-x86_64-msvc": "x86_64-pc-windows-msvc",
        "windows-arm64-msvc": "aarch64-pc-windows-msvc",
    }[target]
    toolchain = item.get("toolchain")
    require(toolchain, "Cargo component requires an exact toolchain")
    import re

    require(re.fullmatch(r"\d+\.\d+\.\d+", toolchain), "Pin an exact Cargo toolchain")
    command = ["rustup", "run", toolchain, "cargo"]
    args = [
        "build",
        "--locked",
        "--manifest-path",
        manifest,
        "--target",
        triple,
    ]
    if release:
        args.append("--release")
    if item.get("features"):
        args += ["--features", ",".join(item["features"])]
    run([*command, *args], root)
    import json

    info = json.loads(
        run(
            [
                *command,
                "metadata",
                "--locked",
                "--offline",
                "--no-deps",
                "--format-version=1",
                "--manifest-path",
                manifest,
            ],
            root,
            capture=True,
        )
    )
    for package in info["packages"]:
        for dependency in package["dependencies"]:
            path = dependency.get("path")
            require(
                not path
                or any(
                    Path(path).resolve().is_relative_to(source)
                    for source in workspace.sources.values()
                ),
                "Cargo dependency is outside declared sources",
            )
    for binary in item.get("binaries", []):
        require("/" not in binary and "\\" not in binary, "Invalid Cargo binary")
        filename = binary + (".exe" if target.startswith("windows-") else "")
        source = (
            Path(info["target_directory"])
            / triple
            / ("release" if release else "debug")
            / filename
        )
        require(source.is_file(), f"Missing Cargo binary: {filename}")
        destination = output / "bin" / filename
        destination.parent.mkdir(exist_ok=True)
        shutil.copyfile(source, destination)
        destination.chmod(0o755)
    require(item.get("binaries"), "Cargo component must select binaries")
    payload(workspace, name, output, target, built, release)


def legacy(workspace, name, output, target, built, release=True):
    project = Project(workspace.component_root(name))
    result = legacy_package(project, output / "package")
    return Path(result["directory"])


ADAPTERS = {"payload": payload, "python": python, "cargo": cargo, "legacy": legacy}


def build(workspace, names, output, target, *, release=True):
    built = {}
    workspace.build_records = {}
    for name in workspace.component_order(names):
        destination = output / name
        destination.mkdir()
        result = ADAPTERS[workspace.components[name]["adapter"]](
            workspace, name, destination, target, built, release
        )
        built[name] = result or destination
        item = workspace.components[name]
        record = {
            "adapter": item["adapter"],
            "profile": "release" if release else "debug",
            "helper_python": sys.version.split()[0],
        }
        if item["adapter"] == "cargo":
            record["rustc"] = run(
                ["rustup", "run", item["toolchain"], "rustc", "--version"],
                workspace.component_root(name),
                capture=True,
            ).strip()
            record["cargo"] = run(
                ["rustup", "run", item["toolchain"], "cargo", "--version"],
                workspace.component_root(name),
                capture=True,
            ).strip()
        elif item["adapter"] == "python":
            record["uv"] = run(
                ["uv", "--version"], workspace.root, capture=True
            ).strip()
            executable = run(
                ["uv", "python", "find", item["python"], "--no-python-downloads"],
                workspace.root,
                capture=True,
            ).strip()
            record["python"] = run(
                [
                    executable,
                    "-I",
                    "-c",
                    "import platform;print(platform.python_version())",
                ],
                workspace.root,
                capture=True,
            ).strip()
        workspace.build_records[name] = record
    return built


def version(workspace, product):
    item = workspace.products[product]
    if item.get("version-from"):
        component = workspace.components[item["version-from"]]
        root = workspace.component_root(item["version-from"])
        manifest = root / relative(
            component.get(
                "manifest",
                "pyproject.toml" if component["adapter"] == "python" else "Cargo.toml",
            )
        )
        metadata = tomllib.loads(manifest.read_text())
        value = metadata.get("project", metadata.get("package", {})).get("version")
        require(
            isinstance(value, str), "version-from requires static ecosystem metadata"
        )
        return value
    return item.get("version", workspace.metadata["version"])
