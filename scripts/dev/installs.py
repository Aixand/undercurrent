"""Owned-prefix installation, recoverable activation, and explicit retention."""

from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path
import re
import shutil
import uuid

from core import digest, json_bytes, read_json, require, write_json
from packages import fill_payload, smoke, validate

MARKER = ".installation-template.json"
ID = re.compile(r"[0-9a-f]{64}\Z")


def atomic_json(path, value):
    temporary = path.with_name(".write-" + uuid.uuid4().hex)
    try:
        with temporary.open("xb") as stream:
            stream.write(json_bytes(value))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def reference(prefix, identifier):
    target = prefix / "current"
    if identifier is None:
        target.unlink(missing_ok=True)
        return
    require(ID.fullmatch(identifier), "Unsafe payload identity")
    temporary = prefix / (".current-" + uuid.uuid4().hex)
    try:
        temporary.symlink_to("payloads/" + identifier, target_is_directory=True)
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


def recover(prefix):
    journal = prefix / ".activation.json"
    if not journal.exists():
        return
    require(not journal.is_symlink(), "Unsafe activation journal")
    transaction = read_json(journal)
    require(set(transaction) == {"old", "new"}, "Invalid activation journal")
    old, new = transaction["old"], transaction["new"]
    for state in [old, new]:
        if state is not None:
            validate_state_shape(state)
    pointer = prefix / "current"
    require(not pointer.exists() or pointer.is_symlink(), "Unsafe active pointer")
    selected = os.readlink(pointer) if pointer.is_symlink() else None
    expected = {
        None if state is None else "payloads/" + state["current"]
        for state in [old, new]
    }
    require(
        selected in expected,
        "Activation journal conflicts with current pointer; preserve prefix for inspection",
    )
    state_path = prefix / "installation-receipt.json"
    require(not state_path.is_symlink(), "Unsafe installation receipt")
    recorded = read_json(state_path) if state_path.exists() else None
    require(recorded in [old, new], "Activation journal conflicts with receipt")
    # A fully committed selection wins; otherwise restore the previous selection.
    chosen = (
        new if recorded == new and selected == "payloads/" + new["current"] else old
    )
    reference(prefix, chosen["current"] if chosen else None)
    if chosen is None:
        state_path.unlink(missing_ok=True)
    else:
        atomic_json(state_path, chosen)
    journal.unlink()


def validate_state_shape(state):
    require(
        isinstance(state, dict)
        and set(state) == {"schema", "current", "previous", "commands"},
        "Invalid installation receipt",
    )
    require(
        type(state["schema"]) is int and state["schema"] == 1,
        "Unsupported receipt schema",
    )
    require(
        isinstance(state["current"], str) and ID.fullmatch(state["current"]),
        "Invalid current payload identity",
    )
    require(
        state["previous"] is None
        or (isinstance(state["previous"], str) and ID.fullmatch(state["previous"])),
        "Invalid previous payload identity",
    )
    require(isinstance(state["commands"], list), "Invalid receipt commands")


def state_of(prefix, project):
    path = prefix / "installation-receipt.json"
    require(not path.is_symlink(), "Unsafe receipt")
    state = read_json(path) if path.exists() else None
    if state:
        validate_state_shape(state)
        require(
            state["commands"] == project.exposed,
            "Command set changed; use a new prefix and an explicit migration",
        )
    current = prefix / "current"
    require(
        not current.exists() or current.is_symlink(),
        "Current selection is not a managed symlink",
    )
    require(
        not current.is_symlink()
        if state is None
        else current.is_symlink()
        and os.readlink(current) == "payloads/" + state["current"],
        "Active pointer differs from receipt",
    )
    bin_dir = prefix / "bin"
    require(not bin_dir.is_symlink(), "Command directory cannot be a symlink")
    if bin_dir.exists():
        require(bin_dir.is_dir(), "Invalid command directory")
        require(
            set(p.name for p in bin_dir.iterdir()) <= set(project.exposed),
            "Unrelated command in managed prefix",
        )
        for path in bin_dir.iterdir():
            require(
                path.is_symlink()
                and os.readlink(path) == "../current/bin/" + path.name,
                "Refusing changed/unowned command",
            )
    return state


@contextmanager
def locked(prefix, project, *, create=False):
    require(
        os.name != "nt",
        "Managed prefix activation is Unix-only in v1; Windows wrappers/builds need native CI qualification",
    )
    prefix = Path(prefix).expanduser().absolute()
    require(
        prefix != Path(prefix.anchor) and not prefix.is_symlink(),
        "Unsafe installation prefix",
    )
    if not prefix.exists():
        require(create, "No managed installation at selected prefix")
        prefix.mkdir(parents=True, mode=0o700)
    require(prefix.is_dir(), "Prefix is not a directory")
    marker = prefix / MARKER
    expected = {
        "schema": 1,
        "name": project.name,
        "profile": project.profile,
        "origin": "source-helper",
    }
    if not marker.exists():
        require(
            create and not list(prefix.iterdir()),
            "Refusing nonempty unmanaged installation prefix",
        )
        with marker.open("x") as stream:
            stream.write(json_bytes(expected).decode())
    require(
        not marker.is_symlink() and read_json(marker) == expected,
        "Prefix ownership/origin mismatch",
    )
    lock_path = prefix / ".install.lock"
    require(not lock_path.is_symlink(), "Unsafe installation lock")
    descriptor = os.open(
        lock_path, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600
    )
    import fcntl

    with os.fdopen(descriptor, "a+b") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            from core import Failure

            raise Failure("Another installation operation is running") from error
        recover(prefix)
        yield prefix


def receipt(prefix, identifier, project):
    require(ID.fullmatch(identifier), "Unsafe payload identity")
    payload = prefix / "payloads" / identifier
    require(
        not (prefix / "payloads").is_symlink()
        and payload.is_dir()
        and not payload.is_symlink(),
        "Unsafe payload directory",
    )
    path = payload / "payload-receipt.json"
    manifest_path = payload / "release-manifest.json"
    require(
        not path.is_symlink() and not manifest_path.is_symlink(),
        "Unsafe payload metadata",
    )
    result = read_json(path)
    require(
        result
        == {
            "schema": 1,
            "manifest_sha256": identifier,
            "name": project.name,
            "profile": project.profile,
        },
        "Payload receipt ownership mismatch",
    )
    require(digest(manifest_path) == identifier, "Installed manifest identity changed")
    manifest = read_json(manifest_path)
    require(
        manifest["name"] == project.name
        and manifest["profile"] == project.profile
        and manifest["commands"] == project.exposed,
        "Installed payload metadata differs",
    )
    return payload, manifest


def probe(prefix, identifier, project):
    payload, manifest = receipt(prefix, identifier, project)
    if project.profile == "rust-cli":
        for name, info in manifest["payload"].items():
            path = payload / name
            require(
                path.is_file()
                and not path.is_symlink()
                and digest(path) == info["sha256"],
                "Installed native payload changed",
            )
        smoke(project, manifest, payload)
    else:
        for name in project.exposed:
            path = payload / "bin" / name
            require(
                path.is_symlink() and os.readlink(path) == "../env/bin/" + name,
                "Installed Python launcher changed",
            )
        smoke(project, manifest, payload, environment=payload / "env")
    return payload, manifest


def activate(prefix, state, previous):
    journal = prefix / ".activation.json"
    atomic_json(journal, {"old": previous, "new": state})
    try:
        reference(prefix, state["current"])
        atomic_json(prefix / "installation-receipt.json", state)
        journal.unlink()
    except BaseException:
        recover(prefix)
        raise


def install(project, artifact, prefix, *, replace=False, python=None):
    require(
        project.managed, "Managed command installation is unsupported for this profile"
    )
    artifact, manifest = validate(artifact, project)
    identifier = digest(artifact / "release-manifest.json")
    with locked(prefix, project, create=not replace) as root:
        previous = state_of(root, project)
        require(
            not previous or replace or previous["current"] == identifier,
            "Already installed; use reinstall or upgrade --artifact",
        )
        require(not replace or previous, "No owned installation to replace")
        if previous and previous["current"] == identifier:
            probe(root, identifier, project)
            return {
                "prefix": str(root),
                "version": manifest["version"],
                "unchanged": True,
            }
        if previous:
            probe(root, previous["current"], project)
        payloads = root / "payloads"
        require(not payloads.is_symlink(), "Unsafe payload root")
        payloads.mkdir(exist_ok=True)
        payload = payloads / identifier
        created = False
        try:
            if payload.exists() or payload.is_symlink():
                probe(root, identifier, project)
            else:
                payload.mkdir(mode=0o700)
                created = True
                # A Python venv is created at its final path, never moved later.
                fill_payload(project, artifact, manifest, payload, python)
                shutil.copyfile(
                    artifact / "release-manifest.json",
                    payload / "release-manifest.json",
                )
                write_json(
                    payload / "payload-receipt.json",
                    {
                        "schema": 1,
                        "manifest_sha256": identifier,
                        "name": project.name,
                        "profile": project.profile,
                    },
                )
            bin_dir = root / "bin"
            bin_dir.mkdir(exist_ok=True)
            for name in project.exposed:
                command = bin_dir / name
                if not command.is_symlink():
                    require(
                        not command.exists(), "Command appeared during installation"
                    )
                    command.symlink_to("../current/bin/" + name)
            state = {
                "schema": 1,
                "current": identifier,
                "previous": previous["current"] if previous else None,
                "commands": project.exposed,
            }
            activate(root, state, previous)
        except BaseException:
            # Only remove a payload created by this attempt, and never an active one.
            current = root / "current"
            active = os.readlink(current) if current.is_symlink() else None
            if created and active != "payloads/" + identifier:
                shutil.rmtree(payload)
            raise
    return {
        "prefix": str(root),
        "version": manifest["version"],
        "commands": str(root / "bin"),
        "restart_required": True,
        "runtime_cleanup": "explicit only",
    }


def rollback(project, prefix):
    require(
        project.install_config.get("stateful") is False,
        "Rollback requires install.stateful=false; stateful products need a migration-aware adapter",
    )
    with locked(prefix, project) as root:
        previous = state_of(root, project)
        require(previous and previous["previous"], "No retained previous selection")
        _, manifest = probe(root, previous["previous"], project)
        state = {
            **previous,
            "current": previous["previous"],
            "previous": previous["current"],
        }
        activate(root, state, previous)
    return {
        "prefix": str(root),
        "version": manifest["version"],
        "restart_required": True,
    }


def retired(prefix, state, project):
    payloads = prefix / "payloads"
    require(not payloads.is_symlink(), "Unsafe payload root")
    retained = {state["current"], state["previous"]} if state else set()
    result = []
    for path in sorted(payloads.iterdir()) if payloads.exists() else []:
        receipt(prefix, path.name, project)
        if path.name not in retained:
            result.append(path)
    return result


def prune(project, prefix, *, dry_run=False, stopped=False):
    with locked(prefix, project) as root:
        state = state_of(root, project)
        paths = retired(root, state, project)
        require(
            dry_run or stopped,
            "prune requires --stopped: attest that processes using retired payloads have stopped; use --dry-run to inspect first",
        )
        if not dry_run:
            for path in paths:
                shutil.rmtree(path)
    return {
        "retired_payloads": [p.name for p in paths],
        "removed": not dry_run,
        "retained": "current and previous; shared runtimes and user data untouched",
    }


def uninstall(project, prefix, *, stopped=False):
    prefix = Path(prefix).expanduser().absolute()
    if not prefix.exists() and not prefix.is_symlink():
        return {"already_absent": True, "prefix": str(prefix)}
    require(
        stopped,
        "uninstall requires --stopped after stopping processes using this installation",
    )
    with locked(prefix, project) as root:
        state = state_of(root, project)
        owned = retired(root, None, project)
        if state is None and not owned and not (root / "bin").exists():
            return {"already_absent": True, "prefix": str(root)}
        if state:
            probe(root, state["current"], project)
        for command in (root / "bin").iterdir() if (root / "bin").exists() else []:
            command.unlink()
        (root / "current").unlink(missing_ok=True)
        (root / "installation-receipt.json").unlink(missing_ok=True)
        for path in owned:
            shutil.rmtree(path)
        for directory in [root / "bin", root / "payloads"]:
            if directory.exists():
                directory.rmdir()
        # Marker and lock remain so repeated operations retain ownership history.
    return {"prefix": str(root), "removed": True, "user_data_preserved": True}
