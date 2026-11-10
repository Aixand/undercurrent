"""Package adapters and bounded artifact verification."""

from __future__ import annotations

from email.parser import BytesParser
import hashlib
import os
from pathlib import Path
import platform
import re
import shutil
import tarfile
import tempfile
import zipfile

from core import (
    HELPER_REVISION,
    NAME,
    VERSION,
    digest,
    host_target,
    read_json,
    relative,
    require,
    run,
    source_file,
    validate_keys,
    write_json,
)

LIMIT = 2 * 1024**3


def descriptor(path):
    return {"sha256": digest(path), "size": path.stat().st_size}


def canonical(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def wheel_info(path):
    with zipfile.ZipFile(path) as archive:
        entries = archive.infolist()
        require(
            sum(e.file_size for e in entries) <= LIMIT,
            "Wheel exceeds unpacked size limit",
        )
        names = [e.filename for e in entries]
        require(len(names) == len(set(names)), "Duplicate wheel entry")
        for entry in entries:
            relative(entry.filename.rstrip("/"))
            require(
                (entry.external_attr >> 16) & 0o170000 != 0o120000,
                "Wheel contains a symlink",
            )
        metadata = [name for name in names if name.endswith(".dist-info/METADATA")]
        require(len(metadata) == 1, "Wheel needs one METADATA file")
        info = BytesParser().parsebytes(archive.read(metadata[0]))
        require(info.get("Name") and info.get("Version"), "Wheel has no identity")
        for dependency in info.get_all("Requires-Dist", []):
            require(
                "@" not in dependency,
                "Release wheels cannot contain direct URL dependencies",
            )
        return info


def python_identity(project, executable):
    import json

    return json.loads(
        run(
            [
                executable,
                "-I",
                "-c",
                "import json,sys,sysconfig;print(json.dumps({'implementation':sys.implementation.name,"
                "'minor':f'{sys.version_info.major}.{sys.version_info.minor}',"
                "'platform':sysconfig.get_platform(),'soabi':sysconfig.get_config_var('SOABI') or ''}))",
            ],
            project.root,
            capture=True,
        )
    )


def build(project, output=None, *, release=False, target=None, python=None):
    if project.python:
        destination = Path(output or project.root / "dist/build").absolute()
        run(
            [
                "uv",
                "build",
                "--no-sources",
                "--no-build-logs",
                "--python",
                python or project.python_request,
                "--out-dir",
                destination,
                project.manifest.parent,
            ],
            project.root,
        )
        return {"directory": str(destination)}
    if project.rust:
        args = ["--release"] if release else []
        if target:
            args += ["--target", target]
        if project.features:
            args += ["--features", ",".join(project.features)]
        project.cargo("build", *args)
        data, _ = project.cargo_info()
        return {"directory": data["target_directory"]}
    project.configured(
        "build", **{"{output}": str(output or project.root / "dist/build")}
    )
    return {"directory": str(output or project.root / "dist/build")}


def python_package(project, output, python):
    executable = project.runtime_python(python)
    # Index-ready artifacts must not accidentally resolve changed siblings.
    sources = project.metadata.get("tool", {}).get("uv", {}).get("sources", {})
    require(
        not sources,
        "v1 packaging requires index-ready dependencies; local/Git source overrides need a later coordinated-dependency adapter",
    )
    dist = output / "python"
    build(project, dist, release=True, python=executable)
    wheels = list(dist.glob("*.whl"))
    sdists = list(dist.glob("*.tar.gz"))
    require(
        len(wheels) == len(sdists) == 1,
        "Expected one project wheel and one source distribution",
    )
    info = wheel_info(wheels[0])
    require(
        canonical(info["Name"]) == canonical(project.name)
        and info["Version"] == project.version,
        "Wheel identity differs from static project metadata; use a canonical Python version",
    )
    require(
        wheels[0].name.endswith("-py3-none-any.whl"),
        "v1 Python profiles package pure-Python project wheels; native extension wheels need another adapter",
    )
    requirements = output / "requirements.txt"
    run(
        [
            "uv",
            "export",
            "--locked",
            "--no-dev",
            "--no-default-groups",
            "--no-emit-project",
            "--no-editable",
            "--no-annotate",
            "--no-header",
            "--output-file",
            requirements,
            "--project",
            project.manifest.parent,
        ],
        project.root,
    )
    text = requirements.read_text()
    for line in text.splitlines():
        stripped = line.strip()
        require(
            not stripped
            or stripped.startswith(("#", "--hash="))
            or (
                not stripped.startswith(("-", "/", "."))
                and "==" in stripped
                and " @ " not in stripped
                and "://" not in stripped
            ),
            "Dependency export contains non-registry, non-pinned, or unsupported requirements",
        )
    wheelhouse = output / "wheelhouse"
    wheelhouse.mkdir()
    if any(
        line.strip() and not line.lstrip().startswith("#") for line in text.splitlines()
    ):
        run(
            [
                "uv",
                "tool",
                "run",
                "--isolated",
                "--python",
                executable,
                "--from",
                "pip==25.3",
                "pip",
                "--isolated",
                "download",
                "--require-hashes",
                "--only-binary=:all:",
                "--no-deps",
                "--dest",
                wheelhouse,
                "--requirement",
                requirements,
            ],
            project.root,
        )
    packages = {}
    for path in wheelhouse.glob("*.whl"):
        name = canonical(wheel_info(path)["Name"])
        require(
            name != canonical(project.name) and name not in packages,
            "Duplicate dependency wheel identity",
        )
        packages[name] = path.name
    return {
        "primary": wheels[0].relative_to(output).as_posix(),
        "runtime": python_identity(project, executable),
        "target": None,
        "payload": {},
    }


def add_resources(project, payload):
    for item in project.resources:
        source = source_file(project.root, item)
        destination = payload / "share" / item
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)


def rust_package(project, output, target):
    data, selected = project.cargo_info()
    if project.profile == "rust-library":
        require(selected["publish"] != [], "Cargo package has publish=false")
        args = ["--features", ",".join(project.features)] if project.features else []
        project.cargo("package", *args)
        archive = (
            Path(data["target_directory"])
            / "package"
            / f"{project.name}-{project.version}.crate"
        )
        shutil.copyfile(archive, output / archive.name)
        return {"primary": archive.name, "target": None, "runtime": None, "payload": {}}
    target = target or host_target(project)
    build(project, release=True, target=target)
    name = project.exposed[0] + (".exe" if "windows" in target else "")
    binary = Path(data["target_directory"]) / target / "release" / name
    require(binary.is_file(), f"Missing built binary: {name}")
    with tempfile.TemporaryDirectory(prefix="aixand-payload-") as temporary:
        payload = Path(temporary)
        (payload / "bin").mkdir()
        shutil.copyfile(binary, payload / "bin" / name)
        (payload / "bin" / name).chmod(0o755)
        add_resources(project, payload)
        files = {
            p.relative_to(payload).as_posix(): {
                **descriptor(p),
                "executable": bool(p.stat().st_mode & 0o111),
            }
            for p in sorted(payload.rglob("*"))
            if p.is_file()
        }
        archive_name = f"{project.name}-{project.version}-{target}.tar.gz"
        with tarfile.open(output / archive_name, "w:gz") as archive:
            for name in files:
                archive.add(payload / name, arcname=name, recursive=False)
    return {
        "primary": archive_name,
        "target": target,
        "runtime": None,
        "payload": files,
    }


def package(project, output=None, *, target=None, python=None):
    destination = Path(output or project.root / "dist/package").absolute()
    require(
        not destination.exists() and not destination.is_symlink(),
        f"Output already exists: {destination}; select a new --output directory",
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".package-", dir=destination.parent
    ) as temporary:
        staging = Path(temporary) / "result"
        staging.mkdir()
        if project.python:
            details = python_package(project, staging, python)
        elif project.rust:
            details = rust_package(project, staging, target)
        else:
            project.configured("package", **{"{output}": str(staging)})
            artifacts = project.config.get("custom", {}).get("artifacts", [])
            require(
                artifacts,
                "Custom packaging must declare custom.artifacts relative to {output}",
            )
            for name in artifacts:
                source_file(staging, name)
            details = {
                "primary": artifacts[0],
                "target": None,
                "runtime": None,
                "payload": {},
            }
        files = {}
        for path in sorted(staging.rglob("*")):
            require(not path.is_symlink(), "Package output cannot contain symlinks")
            if path.is_file():
                files[path.relative_to(staging).as_posix()] = descriptor(path)
        toolchain = {"helper_python": platform.python_version()}
        if project.python:
            toolchain["uv"] = run(
                ["uv", "--version"], project.root, capture=True
            ).strip()
            toolchain["wheelhouse_pip"] = "25.3"
            with zipfile.ZipFile(staging / details["primary"]) as wheel:
                wheel_metadata = next(
                    name
                    for name in wheel.namelist()
                    if name.endswith(".dist-info/WHEEL")
                )
                toolchain["wheel_generator"] = (
                    BytesParser()
                    .parsebytes(wheel.read(wheel_metadata))
                    .get("Generator", "not reported")
                )
        elif project.rust:
            for tool in ["cargo", "rustc"]:
                toolchain[tool] = run(
                    project.rust_argv(tool, "--version"), project.root, capture=True
                ).strip()
        manifest = {
            "schema": 1,
            "helper_revision": HELPER_REVISION,
            "name": project.name,
            "version": project.version,
            "profile": project.profile,
            "commands": project.exposed,
            "features": project.features,
            "source": project.identity(),
            "toolchain": toolchain,
            "files": files,
            **details,
        }
        write_json(staging / "release-manifest.json", manifest)
        sums = {
            **files,
            "release-manifest.json": descriptor(staging / "release-manifest.json"),
        }
        (staging / "SHA256SUMS").write_text(
            "".join(
                f"{value['sha256']}  {name}\n" for name, value in sorted(sums.items())
            )
        )
        validate(staging, project)
        staging.rename(destination)
    return {
        "directory": str(destination),
        "manifest": str(destination / "release-manifest.json"),
    }


def validate(artifact, project=None):
    root = Path(artifact).absolute()
    if root.name == "release-manifest.json":
        root = root.parent
    require(
        root.is_dir() and not root.is_symlink(),
        "--artifact must be a package directory or its release-manifest.json",
    )
    require(
        (root / "release-manifest.json").is_file()
        and not (root / "release-manifest.json").is_symlink(),
        "Missing regular release manifest",
    )
    manifest = read_json(root / "release-manifest.json")
    keys = {
        "schema",
        "helper_revision",
        "name",
        "version",
        "profile",
        "commands",
        "features",
        "source",
        "toolchain",
        "files",
        "primary",
        "target",
        "runtime",
        "payload",
    }
    validate_keys(manifest, keys, "release manifest")
    require(
        set(manifest) == keys
        and type(manifest["schema"]) is int
        and manifest["schema"] == 1,
        "Unsupported/incomplete release manifest",
    )
    require(
        type(manifest["helper_revision"]) is int
        and manifest["helper_revision"] == HELPER_REVISION,
        "Unsupported helper revision",
    )
    require(
        isinstance(manifest["name"], str) and NAME.fullmatch(manifest["name"]),
        "Invalid artifact name",
    )
    require(
        isinstance(manifest["version"], str) and VERSION.fullmatch(manifest["version"]),
        "Invalid artifact version",
    )
    if project:
        require(
            manifest["name"] == project.name and manifest["profile"] == project.profile,
            "Artifact belongs to another project/profile",
        )
        require(
            manifest["commands"] == project.exposed
            and manifest["features"] == project.features,
            "Artifact commands/features differ from selected project",
        )
    require(
        isinstance(manifest["files"], dict) and manifest["files"],
        "Missing artifact file set",
    )
    expected = set(manifest["files"]) | {"release-manifest.json", "SHA256SUMS"}
    actual = set()
    for path in root.rglob("*"):
        require(not path.is_symlink(), "Symlink in package directory")
        if path.is_file():
            actual.add(path.relative_to(root).as_posix())
    require(actual == expected, "Package contains missing or unexpected files")
    total = 0
    for name, info in manifest["files"].items():
        path = source_file(root, name)
        validate_keys(info, {"sha256", "size"}, "artifact descriptor")
        require(
            type(info.get("size")) is int and 0 <= info["size"] <= LIMIT,
            "Invalid artifact size",
        )
        total += info["size"]
        require(info == descriptor(path), f"Artifact checksum/size mismatch: {name}")
    require(total <= LIMIT, "Package exceeds size limit")
    require(manifest["primary"] in manifest["files"], "Missing primary artifact")
    expected_sums = {
        **manifest["files"],
        "release-manifest.json": descriptor(root / "release-manifest.json"),
    }
    sums = "".join(
        f"{value['sha256']}  {name}\n" for name, value in sorted(expected_sums.items())
    )
    require(
        (root / "SHA256SUMS").read_text() == sums,
        "SHA256SUMS differs from manifest/files",
    )
    if manifest["profile"].startswith("python-"):
        info = wheel_info(root / manifest["primary"])
        require(
            canonical(info["Name"]) == canonical(manifest["name"])
            and info["Version"] == manifest["version"],
            "Project wheel identity mismatch",
        )
        for wheel in (root / "wheelhouse").glob("*.whl"):
            wheel_info(wheel)
    elif manifest["profile"] == "rust-cli":
        require(
            isinstance(manifest["payload"], dict) and manifest["payload"],
            "Missing native payload inventory",
        )
        inspect_tar(root / manifest["primary"], expected=manifest["payload"])
    elif manifest["profile"] == "rust-library":
        inspect_tar(root / manifest["primary"])
    return root, manifest


def inspect_tar(path, *, expected=None, destination=None):
    with tarfile.open(path, "r:*") as archive:
        entries = []
        for entry in archive:
            require(len(entries) < 10000, "Too many archive entries")
            entries.append(entry)
        names = set()
        size = 0
        for entry in entries:
            relative(entry.name.rstrip("/"))
            require(entry.name not in names, "Duplicate archive entry")
            names.add(entry.name)
            require(
                entry.isfile() or entry.isdir(),
                "Archive links and special files are forbidden",
            )
            size += entry.size
            require(
                size <= LIMIT and entry.size >= 0, "Archive exceeds unpacked size limit"
            )
        regular = {entry.name for entry in entries if entry.isfile()}
        if expected is not None:
            require(
                regular == set(expected),
                "Native archive has missing or unexpected payload files",
            )
        for entry in entries:
            if entry.isdir():
                continue
            with archive.extractfile(entry) as stream:
                actual_hash = hashlib.file_digest(stream, "sha256").hexdigest()
            if expected is not None:
                info = expected[entry.name]
                validate_keys(
                    info, {"sha256", "size", "executable"}, "payload descriptor"
                )
                require(
                    type(info.get("executable")) is bool
                    and type(info.get("size")) is int,
                    "Invalid payload descriptor",
                )
                require(
                    info["sha256"] == actual_hash and info["size"] == entry.size,
                    "Native payload checksum mismatch",
                )
                require(
                    bool(entry.mode & 0o111) == info["executable"],
                    "Native payload executable bit differs",
                )
            if destination:
                output = Path(destination) / relative(entry.name)
                output.parent.mkdir(parents=True, exist_ok=True)
                with archive.extractfile(entry) as source, output.open("xb") as target:
                    shutil.copyfileobj(source, target)
                output.chmod(
                    0o755
                    if expected is not None and expected[entry.name]["executable"]
                    else 0o644
                )


def smoke(project, manifest, payload, *, environment=None):
    require(
        project.verification.get("commands"),
        "Configure meaningful verification.commands before installation",
    )
    replacements = {"{payload}": str(payload)}
    if environment:
        replacements["{python}"] = str(
            environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        )
    for name in manifest["commands"]:
        suffix = ".exe" if os.name == "nt" else ""
        replacements["{command:" + name + "}"] = str(payload / "bin" / (name + suffix))
    with tempfile.TemporaryDirectory(prefix="aixand-smoke-") as temporary:
        for argv in project.verification["commands"]:
            expanded = project.expand(argv, **replacements)
            require(
                not any("{command:" in arg for arg in expanded),
                "Unknown smoke command placeholder",
            )
            run(expanded, temporary)


def fill_payload(project, artifact, manifest, payload, python=None):
    if manifest["profile"] == "rust-cli":
        machine = {"AMD64": "x86_64", "arm64": "aarch64"}.get(
            platform.machine(), platform.machine()
        )
        system = platform.system()
        suffix = {
            "Linux": "unknown-linux-gnu",
            "Darwin": "apple-darwin",
            "Windows": "pc-windows-msvc",
        }.get(system)
        require(
            suffix and manifest["target"] == machine + "-" + suffix,
            "Native artifact target differs from current host; runtime verification cannot execute it",
        )
        require(
            system != "Linux" or platform.libc_ver()[0] == "glibc",
            "This native profile targets GNU libc",
        )
        inspect_tar(
            artifact / manifest["primary"],
            expected=manifest["payload"],
            destination=payload,
        )
        smoke(project, manifest, payload)
        return
    require(
        manifest["profile"].startswith("python-"),
        "This profile has no managed executable payload",
    )
    executable = project.runtime_python(python)
    require(
        python_identity(project, executable) == manifest["runtime"],
        "Python implementation/minor/platform differs from packaged wheelhouse",
    )
    environment = payload / "env"
    run(["uv", "venv", "--python", executable, environment], project.root)
    interpreter = environment / (
        "Scripts/python.exe" if os.name == "nt" else "bin/python"
    )
    wheels = [
        artifact / manifest["primary"],
        *sorted((artifact / "wheelhouse").glob("*.whl")),
    ]
    # Install only enumerated, hashed wheels. A cached registry wheel must not
    # satisfy a dependency accidentally omitted from the release wheelhouse.
    run(
        [
            "uv",
            "pip",
            "install",
            "--offline",
            "--no-index",
            "--no-deps",
            "--python",
            interpreter,
            *wheels,
        ],
        project.root,
    )
    run(["uv", "pip", "check", "--python", interpreter], project.root)
    (payload / "bin").mkdir(exist_ok=True)
    for name in manifest["commands"]:
        if os.name == "nt":
            shutil.copyfile(
                environment / "Scripts" / (name + ".exe"),
                payload / "bin" / (name + ".exe"),
            )
        else:
            (payload / "bin" / name).symlink_to(Path("../env/bin") / name)
    smoke(project, manifest, payload, environment=environment)


def verify_install(project, artifact, manifest, python=None):
    with tempfile.TemporaryDirectory(prefix="aixand-install-proof-") as temporary:
        payload = Path(temporary) / "payload"
        payload.mkdir()
        if project.profile == "rust-library":
            inspect_tar(artifact / manifest["primary"], destination=payload)
            crate = payload / f"{manifest['name']}-{manifest['version']}"
            require(crate.is_dir(), "Crate archive has unexpected root")
            consumer = project.verification.get("consumer")
            require(consumer, "Rust library requires verification.consumer")
            app = Path(temporary) / "consumer"
            (app / "src").mkdir(parents=True)
            import json

            (app / "Cargo.toml").write_text(
                '[package]\nname="installation-consumer"\nversion="0.0.0"\nedition="2021"\n'
                f"[dependencies]\n{manifest['name']} = {{path={json.dumps(str(crate))}, features={json.dumps(manifest['features'])}}}\n"
            )
            (app / "src/main.rs").write_text(consumer)
            run(
                project.rust_argv(
                    "cargo", "run", "--offline", "--manifest-path", app / "Cargo.toml"
                ),
                app,
            )
        else:
            fill_payload(project, artifact, manifest, payload, python)
    return {"verified": True, "installation_tested": True, "profile": project.profile}
