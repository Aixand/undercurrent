"""Source-independent product lifecycle with per-product activation and batch journals."""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import uuid

from catalog import PRODUCT_ID, host, check_baseline
from core import (
    Failure,
    digest,
    filesystem_path,
    json_bytes,
    read_json,
    relative,
    require,
    run,
    source_file,
    write_json,
)
from packages import canonical, descriptor, wheel_info
from releases import MANIFEST, ordered, validate
from runtime_support import provision

STATE = "installation-receipt.json"
MARKER = ".aixand-store.json"


def redirected(path):
    return path.is_symlink() or (
        path.exists() and bool(getattr(path.lstat(), "st_file_attributes", 0) & 0x400)
    )


def atomic(path, data):
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.chmod(0o600)
        deadline = time.monotonic() + 3
        delay = 0.01
        while True:
            try:
                os.replace(temporary, path)
                break
            except OSError as error:
                # Windows readers and scanners can briefly deny replacement.
                # Keep the original intact and bound retries for real failures.
                if (
                    os.name != "nt"
                    or getattr(error, "winerror", None) not in (5, 32, 33)
                    or time.monotonic() >= deadline
                ):
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 0.1)
    finally:
        temporary.unlink(missing_ok=True)


def safe_store(path):
    root = Path(path).expanduser().absolute()
    require(
        root != Path(root.anchor), "Cannot use filesystem root as installation store"
    )
    require(not redirected(root), "Installation store was redirected")
    # Canonicalize system aliases such as macOS /var before recording ownership.
    return root.parent.resolve() / root.name


def empty_state(scope, owner):
    return {
        "schema": 2,
        "origin": "artifact-helper",
        "scope": scope,
        "owner": owner,
        "products": {},
        "groups": {},
        "requests": {},
        "shims": {},
    }


def state_of(root, *, scope=None, owner=None):
    if not root.exists():
        return empty_state(scope or "shared", owner or "user")
    marker = source_file(root, MARKER)
    identity = read_json(marker)
    for name in ("bin", "helpers", "payloads", "runtimes"):
        require(not redirected(root / name), "Owned store directory was redirected")
    require(
        set(identity) == {"schema", "origin", "scope", "owner"}
        and identity["schema"] == 2
        and identity["origin"] == "artifact-helper",
        "Unknown installation store owner",
    )
    require(
        identity["scope"] in {"shared", "private"}
        and isinstance(identity["owner"], str),
        "Invalid store scope",
    )
    require(
        scope is None or identity["scope"] == scope,
        "Installation scope differs from store",
    )
    require(
        owner is None or identity["owner"] == owner,
        "Installation owner differs from store",
    )
    if not (root / STATE).exists():
        return empty_state(identity["scope"], identity["owner"])
    state = read_json(source_file(root, STATE))
    require(
        set(state) == set(empty_state("", ""))
        and all(
            state[k] == identity[k] for k in ("schema", "origin", "scope", "owner")
        ),
        "Installation receipt ownership differs",
    )
    require(
        all(
            isinstance(state[k], dict)
            for k in ("products", "groups", "requests", "shims")
        ),
        "Invalid installation receipt tables",
    )
    for product, entry in state["products"].items():
        require(
            PRODUCT_ID.fullmatch(product)
            and entry["product"]["id"] == product
            and re.fullmatch(r"[0-9a-f]{64}", entry["product"]["release"]),
            "Invalid installed product",
        )
        path = relative(entry["payload"])
        require(
            path.parts[:1] == ("payloads",) and path.parts[1:-1] == Path(product).parts,
            "Payload path differs from product identity",
        )
        require(
            (root / path).resolve().is_relative_to(root.resolve()),
            "Payload escapes installation store",
        )
        require(
            not any(
                redirected(p)
                for p in [root / path, *(root / path).parents]
                if p.is_relative_to(root)
            ),
            "Payload path was redirected",
        )
    return state


def shim_bytes(root, helper, python, command):
    consumer = helper / "consumer.py"
    args = [
        str(python),
        "-I",
        "-B",
        str(consumer),
        "exec",
        "--prefix",
        str(root),
        "--command",
        command,
        "--",
    ]
    return (
        "#!/bin/sh\nexec " + " ".join(shlex.quote(x) for x in args) + ' "$@"\n'
    ).encode()


def helper_copy(root, launcher=None):
    source = Path(__file__).parent
    if source.is_file():
        import zipfile

        with zipfile.ZipFile(source) as archive:
            files = {
                n: archive.read(n)
                for n in archive.namelist()
                if "/" not in n and n.endswith(".py") and n != "__main__.py"
            }
    else:
        files = {p.name: p.read_bytes() for p in source.glob("*.py")}
    require(files and "consumer.py" in files, "Missing portable helper source")
    if os.name == "nt":
        if launcher:
            files["command-launcher.exe"] = Path(launcher).read_bytes()
        else:
            native = next(
                (
                    value
                    for value in state_of(root)["shims"].values()
                    if isinstance(value, dict)
                ),
                None,
            )
            require(native is not None, "Missing owned native command launcher")
            files["command-launcher.exe"] = shim_data(native, root)
    identifier = hashlib.sha256(
        b"".join(k.encode() + files[k] for k in sorted(files))
    ).hexdigest()
    destination = root / "helpers" / identifier
    if not destination.exists():
        destination.mkdir(parents=True)
        for name, content in files.items():
            (destination / name).write_bytes(content)
    require(not destination.is_symlink(), "Redirected helper directory")
    for name, content in files.items():
        require(
            source_file(destination, name).read_bytes() == content,
            "Installed helper changed",
        )
    return destination


def shim_inventory(root, state, helper=None, python=None):
    if helper is None:
        return state["shims"]
    commands = {}
    for product, entry in state["products"].items():
        for name in entry["product"]["commands"]:
            key = name.casefold() if os.name == "nt" else name
            require(
                key not in commands, f"Command is provided by multiple products: {name}"
            )
            commands[key] = name
    if os.name == "nt":
        launcher_path = source_file(helper, "command-launcher.exe")
        result = {}
        for name in commands.values():
            from windows_metadata import launcher_bytes

            product = next(
                entry["product"]
                for entry in state["products"].values()
                if name in entry["product"]["commands"]
            )
            content = launcher_bytes(
                launcher_path, product["version"], product["id"], name
            )
            launchers = helper / "launchers"
            require(
                not redirected(launchers), "Versioned launcher directory was redirected"
            )
            launchers.mkdir(exist_ok=True)
            versioned = launchers / (hashlib.sha256(content).hexdigest()[:20] + ".exe")
            require(not redirected(versioned), "Versioned launcher was redirected")
            if versioned.exists():
                require(
                    versioned.read_bytes() == content,
                    "Versioned command launcher changed",
                )
            else:
                versioned.write_bytes(content)
            launcher_record = {
                "file": versioned.relative_to(root).as_posix(),
                **descriptor(versioned),
            }
            fields = [
                "aixand-command/2",
                str(python),
                str(helper / "consumer.py"),
                str(root),
                name,
            ]
            require(
                not any("\n" in x or "\r" in x for x in fields),
                "Invalid native command path",
            )
            result[name + ".exe"] = launcher_record
            result[name + ".aixand-command"] = "\n".join(fields) + "\n"
        return result
    return {
        name + (".cmd" if os.name == "nt" else ""): shim_bytes(
            root, helper, python, name
        ).decode("utf-8")
        for name in commands.values()
    }


def shim_data(value, root):
    if isinstance(value, str):
        return value.encode("utf-8")
    require(
        isinstance(value, dict) and set(value) == {"file", "sha256", "size"},
        "Invalid native shim",
    )
    path = source_file(root, value["file"])
    require(
        descriptor(path) == {k: value[k] for k in ("sha256", "size")},
        "Native launcher changed",
    )
    return path.read_bytes()


def check_shims(root, state):
    directory = root / "bin"
    require(not directory.is_symlink(), "Command directory was redirected")
    actual = {p.name for p in directory.iterdir()} if directory.exists() else set()
    require(
        actual == set(state["shims"]),
        "Command directory contains unowned/missing entries",
    )
    for name, content in state["shims"].items():
        require(
            source_file(directory, name).read_bytes() == shim_data(content, root),
            "Owned command changed",
        )


def write_shims(root, previous, state):
    directory = root / "bin"
    directory.mkdir(exist_ok=True)
    for name in set(previous["shims"]) - set(state["shims"]):
        (directory / name).unlink(missing_ok=True)
    for name, content in state["shims"].items():
        path = directory / name
        atomic(path, shim_data(content, root))
        path.chmod(0o755)


def recover(root):
    journal = root / ".activation.json"
    if journal.exists():
        entry = read_json(source_file(root, ".activation.json"))
        require(
            set(entry) == {"schema", "store", "old", "new"}
            and entry["schema"] == 2
            and entry["store"] == str(root),
            "Unknown/foreign activation journal",
        )
        # Recovery only replaces files whose bytes are one of the journal's owned values.
        for name in set(entry["old"]["shims"]) | set(entry["new"]["shims"]):
            relative(name)
            path = root / "bin" / name
            if path.exists():
                actual = source_file(root / "bin", name).read_bytes()
                require(
                    actual
                    in {
                        shim_data(x["shims"][name], root)
                        for x in (entry["old"], entry["new"])
                        if name in x["shims"]
                    },
                    "Command changed during interrupted activation",
                )
        write_shims(root, entry["new"], entry["old"])
        atomic(root / STATE, json_bytes(entry["old"]))
        journal.unlink()


@contextmanager
def locked(path, *, scope="shared", owner="user", create=False):
    root = safe_store(path)
    if not root.exists():
        require(create, "No installation store")
        root.mkdir(parents=True, mode=0o700)
    marker = root / MARKER
    if not marker.exists():
        require(
            create and not list(root.iterdir()), "Refusing nonempty unmanaged store"
        )
        with marker.open("xb") as stream:
            stream.write(
                json_bytes(
                    {
                        k: empty_state(scope, owner)[k]
                        for k in ("schema", "origin", "scope", "owner")
                    }
                )
            )
    state_of(root, scope=scope, owner=owner)
    path = root / ".install.lock"
    require(not path.is_symlink(), "Redirected installation lock")
    with path.open("a+b") as stream:
        try:
            if os.name == "nt":
                import msvcrt

                if path.stat().st_size == 0:
                    stream.write(b"\0")
                    stream.flush()
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            raise Failure("Another installation operation is running") from error
        try:
            recover(root)
            yield root
        finally:
            if os.name == "nt":
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)


def activate(root, old, new, helper):
    check_shims(root, old)
    # Keep installed commands independent of a development/desktop interpreter.
    # Runtime directories outlive product payloads, including previous selections.
    interpreter = next(
        (
            executable
            for entry in new["products"].values()
            for name, executable in entry["runtimes"].items()
            if entry["product"]["runtime_records"][name]["role"] == "python"
            and (
                match := re.match(
                    r"3\.(\d+)(?:\.|$)",
                    entry["product"]["runtime_records"][name].get("version", ""),
                )
            )
            and int(match[1]) >= 11
        ),
        sys.executable,
    )
    new["shims"] = shim_inventory(root, new, helper, interpreter)
    atomic(
        root / ".activation.json",
        json_bytes({"schema": 2, "store": str(root), "old": old, "new": new}),
    )
    try:
        write_shims(root, old, new)
        atomic(root / STATE, json_bytes(new))
        (root / ".activation.json").unlink()
    except BaseException:
        recover(root)
        raise


def payload_inventory(payload):
    payload = filesystem_path(payload)
    result = {}
    for path in sorted(payload.rglob("*")):
        name = path.relative_to(payload).as_posix()
        if name == "payload-receipt.json":
            continue
        if path.is_symlink():
            result[name] = {"link": os.readlink(path)}
        elif path.is_file():
            result[name] = descriptor(path)
    return result


def probe(root, entry):
    payload = root / relative(entry["payload"])
    require(not payload.is_symlink(), "Redirected product payload")
    receipt = read_json(source_file(payload, "payload-receipt.json"))
    require(
        receipt["schema"] == 2
        and receipt["release"] == entry["product"]["release"]
        and receipt["product"] == entry["product"]["id"],
        "Payload receipt differs",
    )
    require(
        payload_inventory(payload) == receipt["files"],
        "Installed payload changed; use explicit reinstall",
    )
    return payload


def inspect(prefix):
    root = safe_store(prefix)
    if not root.exists():
        return {"schema": 2, "installed": False, "products": {}, "groups": {}}
    state = state_of(root)
    check_shims(root, state)
    products = {}
    for name, entry in state["products"].items():
        probe(root, entry)
        products[name] = {
            "version": entry["product"]["version"],
            "release": entry["product"]["release"],
            "payload": str(root / entry["payload"]),
            "explicit": entry["explicit"],
            "commands": sorted(entry["product"]["commands"]),
        }
    return {
        "schema": 2,
        "installed": bool(products),
        "products": products,
        "groups": state["groups"],
        "scope": state["scope"],
        "owner": state["owner"],
        "cli_directory": str(root / "bin"),
        "recovery_required": (root / ".activation.json").exists(),
    }


def plan(
    artifact,
    prefix,
    *,
    product=None,
    operation="install",
    scope="shared",
    owner="user",
    expected=None,
    origin="artifact",
):
    require(
        scope in {"shared", "private"} and PRODUCT_ID.fullmatch(owner),
        "Invalid installation scope/owner",
    )
    require(
        scope != "private" or owner != "user",
        "Private installations require an application owner",
    )
    _, manifest = validate(artifact, expected=expected)
    require(manifest["target"] == host(), "Release target differs from host")
    check_baseline(manifest["baseline"])
    selected = product or manifest["selection"]
    order = ordered(manifest, selected)
    root = safe_store(prefix)
    state = state_of(root, scope=scope, owner=owner)
    if root.exists():
        check_shims(root, state)
    requested = set(order)
    for name in requested & set(state["products"]):
        require(
            state["products"][name].get("origin") == origin,
            "Existing product belongs to another installation route",
        )
    replacing = {
        p
        for p in order
        if p in state["products"]
        and state["products"][p]["product"]["release"]
        != manifest["products"][p]["release"]
    }
    require(
        operation in {"install", "reinstall", "upgrade"},
        "Unknown installation operation",
    )
    require(
        operation != "install" or not replacing,
        "Version differs; use explicit upgrade or reinstall",
    )
    if operation in {"upgrade", "reinstall"}:
        require(
            selected in state["products"] or selected in state["groups"],
            "Selected installation is absent",
        )
    for group, record in state["groups"].items():
        if group == selected:
            continue
        require(
            not any(
                p in replacing
                and record["releases"].get(p) != manifest["products"][p]["release"]
                for p in replacing
                if p in record["releases"]
            ),
            f"Replacement conflicts with installed collection: {group}",
        )
    for name, entry in state["products"].items():
        if name not in requested:
            require(
                not any(
                    p in replacing and release != manifest["products"][p]["release"]
                    for p, release in entry["dependency_releases"].items()
                ),
                f"Replacement conflicts with installed consumer: {name}",
            )
    if operation == "upgrade":
        for name in replacing:
            require(
                state["products"][name]["product"]["version"]
                != manifest["products"][name]["version"],
                "Immutable version changed; use a new release version or explicit source reinstall",
            )
    commands = {}
    for name in set(state["products"]) | requested:
        item = (
            manifest["products"][name]
            if name in requested
            else state["products"][name]["product"]
        )
        for command in item["commands"]:
            key = command.casefold() if os.name == "nt" else command
            require(key not in commands, f"Command conflict: {command}")
            commands[key] = name
    return {
        "schema": 2,
        "selection": selected,
        "operation": operation,
        "target": manifest["target"],
        "scope": scope,
        "owner": owner,
        "prefix": str(root),
        "executed": False,
        "licenses": [
            {
                "product": p,
                **manifest["products"][p]["license_acceptance"],
                "accepted": state["products"]
                .get(p, {})
                .get("product", {})
                .get("license_acceptance")
                == manifest["products"][p]["license_acceptance"],
            }
            for p in order
            if "license_acceptance" in manifest["products"][p]
        ],
        "products": [
            {
                "id": p,
                "version": manifest["products"][p]["version"],
                "action": "reuse"
                if p in state["products"]
                and p not in replacing
                and operation != "reinstall"
                else "replace"
                if p in state["products"]
                else "install",
            }
            for p in order
        ],
    }


def resolve_command(root, entry, name):
    payload = root / relative(entry["payload"])
    spec = entry["product"]["commands"][name]
    program = spec["program"].replace("{exe}", ".exe" if os.name == "nt" else "")
    if entry["product"]["adapter"] == "python":
        program = "env/" + (
            "Scripts/" + name + ".exe" if os.name == "nt" else "bin/" + name
        )
    program_path = source_file(payload, program)
    values = {"payload": payload, "python": entry["python"] or ""}
    values.update(
        {
            key: entry.get(key.replace("-", "_"), "")
            for key in ("python-home", "python-library", "site-packages")
        }
    )
    values.update({f"runtime:{k}": v for k, v in entry["runtimes"].items()})
    for runtime, executable in entry["runtimes"].items():
        path = Path(executable)
        for _ in Path(entry["product"]["runtime_records"][runtime]["executable"]).parts:
            path = path.parent
        values["runtime-root:" + runtime] = path

    def expand(arg):
        for key, value in values.items():
            arg = arg.replace("{" + key + "}", str(value))
        require(
            not re.search(r"\{[A-Za-z][A-Za-z0-9:_-]*\}", arg),
            "Unknown installed command placeholder",
        )
        return arg

    argv = [str(program_path), *map(expand, spec["args"])]
    if spec["runtime"]:
        runtime = entry["runtimes"].get(spec["runtime"])
        if (
            runtime
            and entry["product"]["runtime_records"][spec["runtime"]]["role"] == "python"
        ):
            runtime = entry["python"]
        if spec["runtime"] == "python" and not runtime:
            runtime = entry["python"]
        require(runtime, "Required command runtime is absent")
        if (
            spec["runtime"] == "python"
            or entry["product"]["runtime_records"].get(spec["runtime"], {}).get("role")
            == "python"
        ):
            argv.insert(0, "-B")
        argv.insert(0, str(runtime))
    env = {
        k: v
        for k, v in os.environ.items()
        if k not in {"PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV"}
    }
    env.update({"PYTHONDONTWRITEBYTECODE": "1", "PYTHONNOUSERSITE": "1"})
    env.update({k: expand(v) for k, v in spec["env"].items()})
    return argv, env


def verify_payload(root, entry):
    payload = root / relative(entry["payload"])
    replacements = {
        "{payload}": str(payload),
        "{python}": entry["python"] or sys.executable,
    }
    commands = {}
    for name in entry["product"]["commands"]:
        commands["{command:" + name + "}"] = resolve_command(root, entry, name)
    with tempfile.TemporaryDirectory(prefix="aixand-product-proof-") as temporary:
        for spec in entry["product"]["verification"]:
            require(
                spec and all(isinstance(x, str) for x in spec),
                "Invalid verification argv",
            )
            if spec[0] in commands:
                argv, env = commands[spec[0]]
                argv = [*argv, *spec[1:]]
            else:
                argv = list(spec)
                env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
            for key, value in replacements.items():
                argv = [arg.replace(key, value) for arg in argv]
            require(
                not any("{command:" in arg for arg in argv),
                "Unknown verification command",
            )
            if argv[0] == replacements["{python}"]:
                argv.insert(1, "-B")
            run(argv, temporary, env=env)


def prepare(root, artifact, product, runtimes, python, origin="artifact"):
    # Leave room for third-party resource paths read by normal Windows Python.
    # The receipt retains the full release hash and the unique suffix is unchanged.
    release_prefix = product["release"][: 8 if os.name == "nt" else 16]
    path = (
        Path("payloads")
        / product["id"]
        / (release_prefix + "-" + uuid.uuid4().hex[:12])
    )
    payload = root / path
    payload.mkdir(parents=True)
    try:
        for name, record in product["files"].items():
            destination = payload / relative(name)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source_file(artifact, record["artifact"]), destination)
            destination.chmod(0o755 if record["executable"] else 0o644)
        entry = {
            "product": product,
            "payload": path.as_posix(),
            "runtimes": runtimes,
            "python": python,
            "explicit": False,
            "previous": None,
            "dependency_releases": {},
            "python_home": "",
            "python_library": "",
            "site_packages": "",
            "origin": origin,
        }
        if product["python"]:
            selected = next(
                (
                    runtimes[k]
                    for k, r in product["runtime_records"].items()
                    if r["role"] == "python"
                ),
                python or sys.executable,
            )
            version = run(
                [
                    selected,
                    "-I",
                    "-B",
                    "-c",
                    "import platform;print(platform.python_version())",
                ],
                payload,
                capture=True,
            ).strip()
            require(
                version == product["python"]
                or version.startswith(product["python"] + "."),
                "Selected Python version differs from product requirement",
            )
            entry["python"] = selected
            info = json.loads(
                run(
                    [
                        selected,
                        "-I",
                        "-B",
                        "-c",
                        "import json,sys,sysconfig,pathlib; p=pathlib.Path(sys.base_prefix);"
                        "c=[p/('python'+str(sys.version_info.major)+str(sys.version_info.minor)+'.dll'),"
                        "pathlib.Path(sysconfig.get_config_var('LIBDIR') or p)/(sysconfig.get_config_var('LDLIBRARY') or ''),"
                        "p/'Python'];"
                        "print(json.dumps({'home':str(p),'library':next((str(v) for v in c if v.is_file()),'')}))",
                    ],
                    payload,
                    capture=True,
                )
            )
            entry.update(
                {"python_home": info["home"], "python_library": info["library"]}
            )
        if any(
            name.startswith("wheelhouse/") and name.endswith(".whl")
            for name in product["files"]
        ):
            require(
                entry["python"], "Python product needs a declared interpreter version"
            )
            environment = payload / "env"
            uv = next(
                (
                    runtimes[k]
                    for k, r in product["runtime_records"].items()
                    if r["role"] == "uv"
                ),
                None,
            )
            wheels = sorted((payload / "wheelhouse").glob("*.whl"))
            require(wheels, "Python product has no prepared wheels")
            identities = [canonical(wheel_info(p)["Name"]) for p in wheels]
            require(
                len(identities) == len(set(identities)), "Conflicting wheel identities"
            )
            extras = {canonical(k): v for k, v in product["extras"].items()}
            require(set(extras) <= set(identities), "Extras refer to a missing wheel")
            wheel_specs = [
                str(path)
                + ("[" + ",".join(extras[name]) + "]" if name in extras else "")
                for path, name in zip(wheels, identities)
            ]
            if uv:
                run([uv, "venv", "--python", entry["python"], environment], payload)
            else:
                run(
                    [entry["python"], "-I", "-B", "-m", "venv", environment],
                    payload,
                    env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                )
            executable = environment / (
                "Scripts/python.exe" if os.name == "nt" else "bin/python"
            )
            if uv:
                run(
                    [
                        uv,
                        "pip",
                        "install",
                        "--offline",
                        "--no-index",
                        "--find-links",
                        payload / "wheelhouse",
                        "--python",
                        executable,
                        *wheel_specs,
                    ],
                    payload,
                )
                run([uv, "pip", "check", "--python", executable], payload)
            else:
                run(
                    [
                        executable,
                        "-I",
                        "-B",
                        "-Xutf8",
                        "-m",
                        "pip",
                        "--isolated",
                        "--disable-pip-version-check",
                        "install",
                        *(
                            ["--prefix", filesystem_path(environment)]
                            if os.name == "nt"
                            else []
                        ),
                        "--no-index",
                        "--find-links",
                        payload / "wheelhouse",
                        "--only-binary=:all:",
                        *wheel_specs,
                    ],
                    payload,
                )
                run(
                    [
                        executable,
                        "-I",
                        "-B",
                        "-Xutf8",
                        "-m",
                        "pip",
                        "--isolated",
                        "check",
                    ],
                    payload,
                )
            entry["python"] = str(executable)
            # JSON keeps paths lossless across the isolated interpreter's
            # Windows code page and the installer's captured text stream.
            entry["site_packages"] = json.loads(
                run(
                    [
                        executable,
                        "-I",
                        "-B",
                        "-c",
                        "import json,sysconfig;print(json.dumps(sysconfig.get_path('purelib')))",
                    ],
                    payload,
                    capture=True,
                )
            )
        require(
            product["adapter"] != "python" or entry["site_packages"],
            "Python product has no prepared wheel set",
        )
        verify_payload(root, entry)
        write_json(
            payload / "payload-receipt.json",
            {
                "schema": 2,
                "release": product["release"],
                "product": product["id"],
                "files": payload_inventory(payload),
            },
        )
        return entry
    except BaseException:
        shutil.rmtree(filesystem_path(payload))
        raise


class Events:
    def __init__(self, request=None, enabled=False):
        self.request = request or uuid.uuid4().hex
        require(
            re.fullmatch(r"[A-Za-z0-9_-]{1,128}", self.request), "Invalid request ID"
        )
        self.enabled = enabled

    def emit(self, stage, product=None, **details):
        if self.enabled:
            print(
                json.dumps(
                    {
                        "schema": 2,
                        "request": self.request,
                        "stage": stage,
                        "product": product,
                        **details,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )


def verify_component(artifact, manifest, python=None):
    require(manifest["target"] == host(), "Component target differs from host")
    check_baseline(manifest["baseline"])
    component = manifest["component"]
    require(
        component and component["verification"],
        "Declare component verification before consumer proof",
    )
    require(
        component["adapter"] != "legacy",
        "Verify legacy libraries through their schema-1 ecosystem adapter",
    )
    files = {}
    for name, record in manifest["files"].items():
        require(name.startswith("component/"), "Component file escapes component root")
        logical = name.removeprefix("component/")
        files[logical] = {
            "artifact": name,
            **record,
            "executable": bool((artifact / name).stat().st_mode & 0o111),
        }
    product = {
        "id": "component-proof",
        "release": digest(artifact / MANIFEST),
        "adapter": "hybrid",
        "files": files,
        "python": component["python"],
        "commands": {},
        "verification": component["verification"],
        "runtime_records": {},
        "extras": {},
    }
    with tempfile.TemporaryDirectory(prefix="component-consumer-") as temporary:
        prepare(Path(temporary), artifact, product, {}, python)
    return {
        "schema": 2,
        "verified": True,
        "installation_tested": True,
        "component": component["id"],
        "global_commands_created": False,
    }


def install(
    artifact,
    prefix,
    *,
    product=None,
    operation="install",
    scope="shared",
    owner="user",
    python=None,
    request=None,
    events=False,
    expected=None,
    allow_local=False,
    origin="artifact",
    accepted_licenses=(),
):
    require(
        scope in {"private", "shared"} and PRODUCT_ID.fullmatch(owner),
        "Invalid installation scope/owner",
    )
    require(
        scope != "private" or owner != "user",
        "Private installations require an application owner",
    )
    event = Events(request, events)
    artifact, manifest = validate(artifact, expected=expected)
    require(
        manifest["kind"] != "component",
        "Components use an ecosystem consumer, not a product store",
    )
    require(manifest["target"] == host(), "Release target differs from host")
    check_baseline(manifest["baseline"])
    selected = product or manifest["selection"]
    require(
        all(re.fullmatch(r"[0-9a-f]{64}", value) for value in accepted_licenses),
        "License acceptance requires SHA-256 digests",
    )

    def check_licenses(state):
        for name in ordered(manifest, selected):
            license_record = manifest["products"][name].get("license_acceptance")
            existing = (
                state["products"]
                .get(name, {})
                .get("product", {})
                .get("license_acceptance")
            )
            require(
                not license_record
                or license_record == existing
                or license_record["sha256"] in accepted_licenses,
                f"License acceptance required for {name}; use --accept-license "
                + (license_record["sha256"] if license_record else ""),
            )

    # Refuse missing consent before creating a store, then recheck under its lock.
    plan(
        artifact,
        prefix,
        product=selected,
        operation=operation,
        scope=scope,
        owner=owner,
        origin=origin,
    )
    check_licenses(state_of(safe_store(prefix)))
    fingerprint = hashlib.sha256(
        json_bytes(
            {
                "manifest": digest(artifact / MANIFEST),
                "selection": selected,
                "operation": operation,
                "origin": origin,
            }
        )
    ).hexdigest()
    with locked(prefix, scope=scope, owner=owner, create=True) as root:
        planned = plan(
            artifact,
            root,
            product=selected,
            operation=operation,
            scope=scope,
            owner=owner,
            origin=origin,
        )
        state = state_of(root)
        check_licenses(state)
        previous_request = state["requests"].get(event.request)
        require(
            not previous_request or previous_request["input"] == fingerprint,
            "Request ID was already used with different inputs",
        )
        completed = previous_request.get("completed", []) if previous_request else []
        state["requests"][event.request] = {
            "input": fingerprint,
            "status": "running",
            "completed": completed,
        }
        atomic(root / STATE, json_bytes(state))
        launcher = manifest["helpers"].get("windows_launcher")
        helper = helper_copy(
            root, artifact / launcher["artifact"] if launcher else None
        )
        try:
            for item in planned["products"]:
                name = item["id"]
                product_record = manifest["products"][name]
                event.emit("checking", name, version=product_record["version"])
                old = state["products"].get(name)
                if (
                    old
                    and old["product"]["release"] == product_record["release"]
                    and (operation != "reinstall" or name in completed)
                ):
                    probe(root, old)
                    entry = old
                    event.emit("reused", name)
                else:
                    runtime_paths = {}
                    for runtime in product_record["runtimes"]:
                        event.emit("preparing-runtime", name, runtime=runtime)
                        runtime_paths[runtime] = str(
                            provision(
                                root,
                                manifest["runtimes"][runtime],
                                artifact,
                                allow_local=allow_local,
                                progress=lambda size, total: event.emit(
                                    "downloading",
                                    name,
                                    runtime=runtime,
                                    bytes=size,
                                    total=total,
                                ),
                            )
                        )
                    event.emit("installing", name)
                    entry = prepare(
                        root, artifact, product_record, runtime_paths, python, origin
                    )
                    event.emit("verified", name)
                entry = {
                    **entry,
                    "explicit": entry["explicit"]
                    or name == selected
                    and selected in manifest["products"],
                    "dependency_releases": {
                        p: manifest["products"][p]["release"]
                        for p in product_record["dependencies"]
                    },
                }
                if old and entry["payload"] != old["payload"]:
                    entry["previous"] = {**old, "previous": None}
                new = json.loads(json.dumps(state))
                new["products"][name] = entry
                if name not in completed:
                    completed.append(name)
                new["requests"][event.request]["completed"] = list(completed)
                activate(root, state, new, helper)
                state = new
                event.emit("activated", name)
            new = json.loads(json.dumps(state))
            if selected in manifest["groups"]:
                new["groups"][selected] = manifest["groups"][selected]
            new["requests"][event.request]["status"] = "complete"
            activate(root, state, new, helper)
        except BaseException:
            failed = state_of(root)
            failed["requests"][event.request]["status"] = "interrupted"
            atomic(root / STATE, json_bytes(failed))
            event.emit("interrupted", completed=completed)
            raise
    event.emit("complete", completed=completed)
    return {
        **inspect(prefix),
        "request": event.request,
        "selection": selected,
        "restart_required": any(x["action"] == "replace" for x in planned["products"]),
        "path_changed": False,
    }


def execute(prefix, command, args):
    root = safe_store(prefix)
    state = state_of(root)
    require(
        not (root / ".activation.json").exists(), "Installation recovery is required"
    )
    entries = [
        x for x in state["products"].values() if command in x["product"]["commands"]
    ]
    require(len(entries) == 1, "Installed command is absent or ambiguous")
    argv, env = resolve_command(root, entries[0], command)
    if os.name != "nt":
        # The installed command owns this process. Preserve its PID and let the
        # product handle termination and terminal signals directly.
        os.execvpe(argv[0], [*argv, *args], env)
        raise AssertionError("exec returned unexpectedly")
    # Console events reach the entire Windows process group. Keep the Python
    # wrapper alive while the product handles the same event and cleans up.
    import signal

    signals = (signal.SIGINT, signal.SIGBREAK)
    previous = {value: signal.getsignal(value) for value in signals}
    try:
        for value in signals:
            signal.signal(value, lambda signum, frame: None)
        return subprocess.run([*argv, *args], env=env).returncode
    finally:
        for value, handler in previous.items():
            signal.signal(value, handler)


def remove(prefix, product, *, stopped=False, dry_run=False):
    root = safe_store(prefix)
    if not root.exists():
        return {"already_absent": True}
    initial = state_of(root)
    require(PRODUCT_ID.fullmatch(product), "Invalid product ID")
    if product not in initial["products"] and product not in initial["groups"]:
        return {"already_absent": True, "product": product}
    require(stopped or dry_run, "Uninstall requires --stopped")
    candidates = (
        set(initial["groups"][product]["releases"])
        if product in initial["groups"]
        else {product}
    )
    remaining_groups = {k: v for k, v in initial["groups"].items() if k != product}
    if product in initial["groups"]:
        candidates = {p for p in candidates if not initial["products"][p]["explicit"]}
    protected = {p for g in remaining_groups.values() for p in g["releases"]}
    protected.update(
        p
        for k, v in initial["products"].items()
        if k not in candidates
        for p in v["dependency_releases"]
    )
    if product in initial["products"]:
        require(
            product not in protected,
            "Product is referenced by another product or collection",
        )
    candidates -= protected
    # Retained dependents retain their own dependency closure.
    while True:
        retained = {
            p
            for k, v in initial["products"].items()
            if k not in candidates
            for p in v["dependency_releases"]
        }
        next_candidates = candidates - retained
        if next_candidates == candidates:
            break
        candidates = next_candidates
    for name in candidates:
        probe(root, initial["products"][name])
    owned_payloads = []
    for name in candidates:
        directory = root / "payloads" / relative(name)
        for payload in directory.iterdir():
            require(
                payload.is_dir() and not redirected(payload), "Unowned retired payload"
            )
            receipt = read_json(source_file(payload, "payload-receipt.json"))
            require(
                receipt.get("schema") == 2
                and receipt.get("product") == name
                and payload_inventory(payload) == receipt["files"],
                "Retired payload changed",
            )
            owned_payloads.append(payload)
    if dry_run:
        return {"removed": False, "products": sorted(candidates), "group": product}
    with locked(root, scope=initial["scope"], owner=initial["owner"]) as root:
        state = state_of(root)
        require(state == initial, "Installation changed; plan removal again")
        new = json.loads(json.dumps(state))
        new["groups"] = remaining_groups
        for name in candidates:
            del new["products"][name]
        activate(root, state, new, helper_copy(root))
        for payload in owned_payloads:
            shutil.rmtree(filesystem_path(payload))
    return {
        "removed": True,
        "products": sorted(candidates),
        "user_data_preserved": True,
        "runtime_cleanup": "explicit only",
    }


def rollback(prefix, product, *, dry_run=False):
    root = safe_store(prefix)
    state = state_of(root)
    require(product in state["products"], "Rollback selects an individual product")
    current = state["products"][product]
    require(
        not current["product"]["stateful"],
        "Stateful rollback requires a migration-aware adapter",
    )
    previous = current["previous"]
    require(
        previous and not previous["product"]["stateful"],
        "No stateless previous payload",
    )
    for group in state["groups"].values():
        require(
            product not in group["releases"]
            or group["releases"][product] == previous["product"]["release"],
            "Rollback conflicts with collection pin",
        )
    for name, entry in state["products"].items():
        if name != product:
            require(
                product not in entry["dependency_releases"]
                or entry["dependency_releases"][product]
                == previous["product"]["release"],
                "Rollback conflicts with installed consumer",
            )
    for name, release in previous["dependency_releases"].items():
        require(
            name in state["products"]
            and state["products"][name]["product"]["release"] == release,
            "Previous payload needs a different dependency release",
        )
    probe(root, current)
    probe(root, previous)
    if dry_run:
        return {"executed": False, "version": previous["product"]["version"]}
    with locked(root, scope=state["scope"], owner=state["owner"]):
        require(state_of(root) == state, "Installation changed; plan rollback again")
        new = json.loads(json.dumps(state))
        new["products"][product] = {
            **previous,
            "previous": {**current, "previous": None},
        }
        activate(root, state, new, helper_copy(root))
    return {
        "executed": True,
        "version": previous["product"]["version"],
        "restart_required": True,
    }


def prune(prefix, *, stopped=False, dry_run=False):
    root = safe_store(prefix)
    state = state_of(root)
    retained = set()
    for entry in state["products"].values():
        retained.add(entry["payload"])
        if entry["previous"]:
            retained.add(entry["previous"]["payload"])
    candidates = []
    directory = root / "payloads"
    require(not directory.is_symlink(), "Redirected payload root")
    for marker in directory.rglob("payload-receipt.json") if directory.exists() else []:
        payload = marker.parent
        require(
            not any(
                redirected(p)
                for p in [payload, *payload.parents]
                if p.is_relative_to(root)
            ),
            "Redirected retired payload",
        )
        receipt = read_json(source_file(payload, "payload-receipt.json"))
        require(
            receipt["schema"] == 2 and payload_inventory(payload) == receipt["files"],
            "Retired payload changed",
        )
        require(
            Path(receipt["product"]).parts
            == payload.relative_to(root / "payloads").parts[:-1],
            "Retired payload ownership differs",
        )
        if payload.relative_to(root).as_posix() not in retained:
            candidates.append(payload)
    require(dry_run or stopped, "Prune requires --stopped")
    if not dry_run:
        with locked(root, scope=state["scope"], owner=state["owner"]):
            require(state_of(root) == state, "Installation changed; plan pruning again")
            for path in candidates:
                shutil.rmtree(filesystem_path(path))
    return {
        "removed": not dry_run,
        "payloads": [p.relative_to(root).as_posix() for p in candidates],
        "retained": "current, previous, helper runtimes, and user data",
    }
