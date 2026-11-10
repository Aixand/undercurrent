"""Versioned, vendored developer helper. Requires Python 3.11+, no Python dependencies."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tomllib

HELPER_REVISION = 1
PROFILES = {"python-library", "python-tool", "rust-library", "rust-cli", "custom"}
OPERATIONS = {
    "help",
    "doctor",
    "develop",
    "check",
    "test",
    "build",
    "package",
    "verify",
    "install",
    "reinstall",
    "upgrade",
    "rollback",
    "prune",
    "uninstall",
    "publish",
    "inspect",
    "plan",
    "export-release",
    "commands",
}
CONFIG_KEYS = {
    "schema",
    "profile",
    "manifest",
    "resources",
    "features",
    "commands",
    "verification",
    "install",
    "python",
    "custom",
}
NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]*\Z")
IGNORED = {
    ".git",
    ".venv",
    "target",
    "dist",
    "build",
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    "node_modules",
    ".mypy_cache",
}


class Failure(Exception):
    pass


class Usage(Failure):
    pass


def require(condition, message):
    if not condition:
        raise Failure(message)


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def json_bytes(value):
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def write_json(path, value):
    Path(path).write_bytes(json_bytes(value))


def read_json(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"Duplicate JSON key: {key}")
            result[key] = value
        return result

    require(Path(path).stat().st_size <= 8 * 1024 * 1024, "JSON metadata exceeds 8 MiB")
    return json.loads(Path(path).read_text(), object_pairs_hook=unique)


def run(argv, cwd, *, capture=False, env=None):
    argv = [str(arg) for arg in argv]
    require(
        bool(shutil.which(argv[0]) or Path(argv[0]).is_file()),
        f"Missing tool: {argv[0]}",
    )
    result = subprocess.run(
        argv,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE if capture else sys.stderr,
        stderr=subprocess.PIPE if capture else sys.stderr,
    )
    if result.returncode:
        # Tool diagnostics go to stderr; never dump the process environment.
        if capture and result.stderr:
            print(result.stderr, file=sys.stderr, end="")
        raise Failure(f"{Path(argv[0]).name} failed (exit {result.returncode})")
    return result.stdout if capture else None


def filesystem_path(path):
    """Use Windows long paths for file access while retaining normal process paths."""
    path = Path(path)
    if os.name != "nt":
        return path
    value = str(path.absolute())
    if not value.startswith("\\\\?\\"):
        value = (
            "\\\\?\\UNC\\" + value[2:]
            if value.startswith("\\\\")
            else "\\\\?\\" + value
        )
    return Path(value)


def relative(value):
    require(
        isinstance(value, str) and value and "\\" not in value,
        "Expected a relative POSIX path",
    )
    path = Path(value)
    require(
        not path.is_absolute()
        and all(p not in {"..", ".", ""} for p in value.split("/")),
        f"Unsafe relative path: {value}",
    )
    require(not any(c in value for c in "\n\r\x00:"), f"Unsafe relative path: {value}")
    return path


def source_file(root, value):
    path = root / relative(value)
    require(
        path.is_file() and not path.is_symlink(),
        f"Missing regular source file: {value}",
    )
    require(
        path.resolve().is_relative_to(root.resolve()),
        f"Source escapes project: {value}",
    )
    return path


def validate_keys(value, keys, label):
    require(isinstance(value, dict), f"{label} must be a table/object")
    require(not set(value) - keys, f"Unknown {label} keys: {sorted(set(value) - keys)}")


class Project:
    def __init__(self, root):
        self.root = Path(root).resolve()
        config_path = self.root / "installation.toml"
        require(
            config_path.is_file(),
            "Missing installation.toml; see the template SETUP.md",
        )
        self.config = tomllib.loads(config_path.read_text())
        validate_keys(self.config, CONFIG_KEYS, "configuration")
        require(
            type(self.config.get("schema")) is int and self.config["schema"] == 1,
            "Unsupported installation schema",
        )
        self.profile = self.config.get("profile")
        require(self.profile in PROFILES, f"Unsupported profile: {self.profile}")
        self.python = self.profile.startswith("python-")
        self.rust = self.profile.startswith("rust-")
        self.manifest = self.root / relative(
            self.config.get(
                "manifest", "pyproject.toml" if self.python else "Cargo.toml"
            )
        )
        self.commands = self.config.get("commands", {})
        validate_keys(
            self.commands, {"develop", "check", "test", "build", "package"}, "commands"
        )
        for operation, batches in self.commands.items():
            require(
                self.profile == "custom" or operation in {"check", "test"},
                f"{operation} is adapter-owned; use the custom profile for custom build/develop/package operations",
            )
            require(
                isinstance(batches, list) and bool(batches),
                f"{operation} needs a nonempty list of argv arrays",
            )
            for argv in batches:
                require(
                    isinstance(argv, list)
                    and argv
                    and all(isinstance(a, str) and a for a in argv),
                    f"Invalid argv for {operation}",
                )
        self.resources = self.config.get("resources", [])
        require(
            isinstance(self.resources, list)
            and all(isinstance(p, str) for p in self.resources),
            "resources must be paths",
        )
        for item in self.resources:
            relative(item)
        require(
            self.profile in {"rust-cli", "custom"} or not self.resources,
            "Python/library resources belong in ecosystem package configuration; resources applies to native CLI payloads",
        )
        self.features = self.config.get("features", [])
        require(
            isinstance(self.features, list)
            and all(isinstance(x, str) and NAME.fullmatch(x) for x in self.features),
            "features must be simple Cargo feature names",
        )
        require(
            self.rust or not self.features, "features applies only to Cargo profiles"
        )
        self.verification = self.config.get("verification", {})
        validate_keys(self.verification, {"commands", "consumer"}, "verification")
        for argv in self.verification.get("commands", []):
            require(
                isinstance(argv, list)
                and argv
                and all(isinstance(a, str) and a for a in argv),
                "Invalid verification command",
            )
        require(
            isinstance(self.verification.get("commands", []), list),
            "verification.commands must be an array",
        )
        require(
            isinstance(self.verification.get("consumer", ""), str),
            "verification.consumer must be Rust source text",
        )
        self.install_config = self.config.get("install", {})
        validate_keys(self.install_config, {"stateful"}, "install")
        require(
            type(self.install_config.get("stateful", True)) is bool,
            "install.stateful must be boolean",
        )
        py = self.config.get("python", {})
        validate_keys(py, {"version"}, "python")
        self.python_request = py.get("version")
        require(
            not self.python_request
            or (
                isinstance(self.python_request, str)
                and re.fullmatch(r"3\.\d+(?:\.\d+)?", self.python_request)
            ),
            "python.version must be a specific Python 3 minor/patch version",
        )
        if self.python:
            require(
                self.python_request, "Python profiles require a tested python.version"
            )
            self.metadata = tomllib.loads(self.manifest.read_text())
            package = self.metadata.get("project", {})
            require(
                not package.get("dynamic"), "v1 requires static Python package metadata"
            )
            self.name, self.version = package.get("name"), package.get("version")
            self.exposed = (
                sorted(package.get("scripts", {}))
                if self.profile == "python-tool"
                else []
            )
            require(
                self.profile != "python-tool" or self.exposed,
                "Python tool has no project.scripts",
            )
        elif self.rust:
            self.metadata = tomllib.loads(self.manifest.read_text())
            package = self.metadata.get("package", {})
            require(
                package, "Select a Cargo package manifest, not a virtual workspace root"
            )
            workspace = {}
            for parent in [self.manifest.parent, *self.manifest.parent.parents]:
                candidate = parent / "Cargo.toml"
                if candidate.exists():
                    data = tomllib.loads(candidate.read_text())
                    if "workspace" in data:
                        workspace = data["workspace"].get("package", {})
                        break
            self.name = package.get("name")
            self.version = package.get("version")
            if self.version == {"workspace": True}:
                self.version = workspace.get("version")
            bins = self.metadata.get("bin", [])
            self.exposed = (
                [b["name"] for b in bins]
                if bins
                else (
                    [self.name]
                    if (self.manifest.parent / "src/main.rs").exists()
                    else []
                )
            )
            if self.profile == "rust-cli":
                require(
                    len(self.exposed) == 1,
                    "v1 rust-cli selects exactly one binary; multi-binary bundles are a later profile",
                )
            else:
                self.exposed = []
        else:
            custom = self.config.get("custom", {})
            validate_keys(custom, {"name", "version", "artifacts"}, "custom")
            self.name, self.version = custom.get("name"), custom.get("version")
            self.exposed = []
        require(
            isinstance(self.name, str) and NAME.fullmatch(self.name),
            "Invalid package name",
        )
        require(
            isinstance(self.version, str) and VERSION.fullmatch(self.version),
            "Invalid/static package version required",
        )
        require(
            all(NAME.fullmatch(name) for name in self.exposed), "Unsafe command name"
        )

    @property
    def managed(self):
        return self.profile in {"python-tool", "rust-cli"}

    def runtime_python(self, override=None):
        if override:
            return run(
                ["uv", "python", "find", override, "--no-python-downloads"],
                self.root,
                capture=True,
            ).strip()
        return run(
            [
                "uv",
                "python",
                "find",
                self.python_request or sys.executable,
                "--no-python-downloads",
            ],
            self.root,
            capture=True,
        ).strip()

    def expand(self, argv, **extra):
        values = {
            "{root}": str(self.root),
            "{python}": sys.executable,
            "{venv-python}": str(
                self.root
                / ".venv"
                / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
            ),
            **extra,
        }
        result = []
        for arg in argv:
            for key, value in values.items():
                arg = arg.replace(key, str(value))
            result.append(arg)
        return result

    def configured(self, operation, **extra):
        if operation not in self.commands:
            raise Usage(f"{operation} is unsupported: configure a real project command")
        for argv in self.commands[operation]:
            run(self.expand(argv, **extra), self.root)

    def cargo(self, operation, *extra, capture=False):
        return run(
            self.rust_argv(
                "cargo", operation, "--locked", "--manifest-path", self.manifest, *extra
            ),
            self.root,
            capture=capture,
        )

    def rust_toolchain(self):
        path = self.root / "rust-toolchain.toml"
        require(
            path.is_file(),
            "Declare a tested rust-toolchain.toml at the selected project root",
        )
        channel = tomllib.loads(path.read_text()).get("toolchain", {}).get("channel")
        require(
            isinstance(channel, str) and re.fullmatch(r"\d+\.\d+\.\d+", channel),
            "v1 requires an exact stable Rust toolchain version",
        )
        return channel

    def rust_argv(self, *argv):
        # Explicit selection also defeats an inherited RUSTUP_TOOLCHAIN override.
        return ["rustup", "run", self.rust_toolchain(), *map(str, argv)]

    def cargo_info(self):
        data = json.loads(
            self.cargo(
                "metadata", "--no-deps", "--offline", "--format-version=1", capture=True
            )
        )
        for package in data["packages"]:
            for dependency in package["dependencies"]:
                path = dependency.get("path")
                require(
                    not path or Path(path).resolve().is_relative_to(self.root),
                    "v1 Cargo packaging requires path dependencies inside the declared project root; coordinated external sources need another adapter",
                )
        selected = next(
            p
            for p in data["packages"]
            if Path(p["manifest_path"]).resolve() == self.manifest.resolve()
        )
        return data, selected

    def identity(self):
        paths = []
        commit = None
        dirty = True
        if shutil.which("git"):
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=self.root,
                text=True,
                capture_output=True,
            )
            if result.returncode == 0:
                commit = result.stdout.strip()
                status = subprocess.run(
                    ["git", "status", "--porcelain", "--", "."],
                    cwd=self.root,
                    text=True,
                    capture_output=True,
                )
                dirty = status.returncode != 0 or bool(status.stdout)
        if commit:
            selected = subprocess.run(
                [
                    "git",
                    "ls-files",
                    "--cached",
                    "--others",
                    "--exclude-standard",
                    "-z",
                    "--",
                    ".",
                ],
                cwd=self.root,
                capture_output=True,
                check=True,
            )
            candidates = [
                self.root / os.fsdecode(name)
                for name in selected.stdout.split(b"\0")
                if name
            ]
        else:
            candidates = []
            for base, dirs, files in os.walk(self.root):
                dirs[:] = sorted(
                    d
                    for d in dirs
                    if d not in IGNORED
                    and not d.endswith(".egg-info")
                    and not (Path(base) / d).is_symlink()
                )
                candidates.extend(Path(base) / name for name in files)
        for path in sorted(candidates):
            if path.is_symlink():
                require(
                    path.resolve().is_relative_to(self.root) and path.is_file(),
                    "Selected source symlinks must point to a regular file inside the project root",
                )
                paths.append(
                    (
                        path.relative_to(self.root).as_posix(),
                        os.readlink(path),
                        digest(path),
                    )
                )
            elif path.is_file() and not path.name.endswith((".pyc", ".pyo")):
                paths.append((path.relative_to(self.root).as_posix(), digest(path)))
        source_hash = hashlib.sha256(json_bytes(paths)).hexdigest()
        locks = {
            p.name: digest(p)
            for p in [self.root / "uv.lock", self.root / "Cargo.lock"]
            if p.is_file()
        }
        return {
            "commit": commit,
            "dirty": dirty,
            "source_sha256": source_hash,
            "locks": locks,
        }


def host_target(project):
    return (
        run(project.rust_argv("rustc", "-vV"), project.root, capture=True)
        .split("host: ", 1)[1]
        .splitlines()[0]
    )
