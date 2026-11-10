"""Portable schema-2 artifact assembly and source-independent structural verification."""

from __future__ import annotations

import hashlib
from pathlib import Path
import re
import shutil
import tempfile

from catalog import (
    COMMAND,
    PRODUCT_ID,
    REVISION,
    TARGETS,
    command_record,
    argv_list,
    strings,
    host,
    host_baseline,
    identity,
    validate_baseline,
)
from components import build as build_components, version
from core import (
    digest,
    json_bytes,
    read_json,
    relative,
    require,
    source_file,
    validate_keys,
    write_json,
)
from packages import (
    LIMIT,
    descriptor,
    inspect_tar,
    validate as legacy_validate,
    wheel_info,
)

MANIFEST = "release-manifest.json"


def object_file(source, output):
    info = descriptor(source)
    path = "objects/" + info["sha256"]
    destination = output / path
    destination.parent.mkdir(exist_ok=True)
    if not destination.exists():
        shutil.copyfile(source, destination)
    return {"artifact": path, **info}


def product_hash(record):
    return hashlib.sha256(
        json_bytes({k: v for k, v in record.items() if k not in {"release", "sources"}})
    ).hexdigest()


def seal(root, manifest):
    manifest["files"] = {
        p.relative_to(root).as_posix(): descriptor(p)
        for p in sorted(root.rglob("*"))
        if p.is_file() and p.name not in {MANIFEST, "SHA256SUMS"}
    }
    write_json(root / MANIFEST, manifest)
    files = {**manifest["files"], MANIFEST: descriptor(root / MANIFEST)}
    (root / "SHA256SUMS").write_text(
        "".join(f"{v['sha256']}  {k}\n" for k, v in sorted(files.items()))
    )


def native_helpers(target, temporary, staging):
    if not target.startswith("windows-"):
        return {}
    from core import run

    executable = temporary / "command-launcher.exe"
    triple = ("x86_64" if "x86_64" in target else "aarch64") + "-pc-windows-msvc"
    run(
        [
            "rustup",
            "run",
            "1.98.0",
            "rustc",
            "--edition=2021",
            "-O",
            "--crate-name",
            "aixand_command",
            "--target",
            triple,
            Path(__file__).with_name("command_launcher.rs"),
            "-o",
            executable,
        ],
        temporary,
    )
    from windows_metadata import stamp

    stamp(
        executable,
        "2.0.0",
        "Aixand installation engine",
        "aixand-command",
        launcher=True,
    )
    return {"windows_launcher": object_file(executable, staging)}


def package(
    workspace, output, *, product=None, variant=None, target=None, component=None
):
    destination = Path(output or workspace.root / "dist/package").absolute()
    require(
        not destination.exists() and not destination.is_symlink(),
        "Package output already exists",
    )
    target = target or host()
    require(target in workspace.targets, "Target is not declared by this workspace")
    baseline = workspace.metadata.get("baselines", {}).get(target)
    if baseline is None:
        require(target == host(), "Cross-target packages require an explicit baseline")
        baseline = host_baseline()
    require(
        not component or not product and not variant,
        "Component selection cannot select a product/variant",
    )
    selected = workspace.select(product, variant) if not component else []
    selection = component or product or workspace.default
    names = (
        [component]
        if component
        else list(
            dict.fromkeys(
                name
                for p in selected
                for name in workspace.product(p, variant if p == selection else None)[
                    "components"
                ]
            )
        )
    )
    source = {name: identity(root) for name, root in workspace.sources.items()}
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".package-", dir=destination.parent
    ) as temporary:
        temporary = Path(temporary)
        staging = temporary / "result"
        staging.mkdir()
        build_dir = temporary / "components"
        build_dir.mkdir()
        built = build_components(workspace, names, build_dir, target)
        if component:
            shutil.copytree(built[component], staging / "component")
            manifest = {
                "schema": 2,
                "helper_revision": REVISION,
                "kind": "component",
                "name": workspace.metadata["name"],
                "version": workspace.metadata["version"],
                "selection": selection,
                "variant": None,
                "target": target,
                "sources": source,
                "products": {},
                "groups": {},
                "runtimes": {},
            }
        else:
            products, runtimes = {}, {}
            for name in selected:
                config = workspace.product(name, variant if name == selection else None)
                require(
                    target in config.get("targets", workspace.targets),
                    f"Product does not support target: {name}",
                )
                files = {}
                for comp in workspace.component_order(config["components"]):
                    root = built[comp]
                    if workspace.components[comp]["adapter"] == "legacy":
                        _, legacy = legacy_validate(root)
                        if legacy["profile"] == "rust-cli":
                            extracted = temporary / ("legacy-" + comp)
                            extracted.mkdir()
                            inspect_tar(
                                root / legacy["primary"],
                                expected=legacy["payload"],
                                destination=extracted,
                            )
                            root = extracted
                        elif legacy["profile"].startswith("python-"):
                            for filename in legacy["files"]:
                                if filename.endswith(".whl"):
                                    logical = "wheelhouse/" + Path(filename).name
                                    entry = {
                                        **object_file(root / filename, staging),
                                        "executable": False,
                                    }
                                    require(
                                        logical not in files or files[logical] == entry,
                                        "Conflicting product wheels",
                                    )
                                    files[logical] = entry
                            continue
                        else:
                            raise ValueError(
                                "Libraries are exported as components, not executable products"
                            )
                    for path in sorted(root.rglob("*")):
                        require(
                            not path.is_symlink(),
                            "Component outputs must contain regular files",
                        )
                        if path.is_file():
                            logical = path.relative_to(root).as_posix()
                            entry = {
                                **object_file(path, staging),
                                "executable": bool(path.stat().st_mode & 0o111),
                            }
                            require(
                                logical not in files or files[logical] == entry,
                                "Conflicting component files",
                            )
                            files[logical] = entry
                notices = []
                notice_sources = []
                for path in ("LICENSE", "NOTICE", *config.get("notices", [])):
                    if (workspace.root / path).is_file():
                        notice_sources.append((workspace.root, path, "project"))
                    else:
                        require(
                            path not in config.get("notices", []),
                            "Declared notice is missing",
                        )
                for comp in workspace.component_order(config["components"]):
                    comp_root = workspace.component_root(comp)
                    if comp_root != workspace.root:
                        notice_sources.extend(
                            (comp_root, path, comp)
                            for path in ("LICENSE", "NOTICE")
                            if (comp_root / path).is_file()
                        )
                for notice_root, path, owner in notice_sources:
                    logical = "share/notices/" + owner + "/" + path
                    files[logical] = {
                        **object_file(source_file(notice_root, path), staging),
                        "executable": False,
                    }
                    if logical not in notices:
                        notices.append(logical)
                require(files, "Product has an empty payload")
                for runtime in config.get("runtimes", []):
                    record = workspace.runtime(runtime, target)
                    require(
                        record["target"] == target,
                        "Runtime target differs from selected product target",
                    )
                    if "path" in record:
                        archive = source_file(workspace.root, record.pop("path"))
                        require(
                            digest(archive) == record["sha256"],
                            "Configured runtime archive checksum mismatch",
                        )
                        record["artifact"] = object_file(archive, staging)["artifact"]
                    runtimes[runtime] = record
                commands = {k: command_record(v) for k, v in config["commands"].items()}
                if target.startswith("windows-"):
                    from windows_metadata import stamp

                    for command, record in commands.items():
                        logical = record["program"].replace("{exe}", ".exe")
                        if (
                            record["runtime"]
                            or not logical.endswith(".exe")
                            or logical not in files
                        ):
                            continue
                        entry = files[logical]
                        versioned = temporary / ("versioned-" + command + ".exe")
                        shutil.copyfile(staging / entry["artifact"], versioned)
                        stamp(versioned, version(workspace, name), name, command)
                        files[logical] = {
                            **object_file(versioned, staging),
                            "executable": True,
                        }
                product_record = {
                    "id": name,
                    "version": version(workspace, name),
                    "adapter": config["adapter"],
                    "target": target,
                    "baseline": baseline,
                    "variant": variant if name == selection else None,
                    "dependencies": config.get("dependencies", []),
                    "runtimes": config.get("runtimes", []),
                    "stateful": config.get("stateful", True),
                    "commands": commands,
                    "verification": config["verification"],
                    "python": config.get("python"),
                    "extras": config.get("extras", {}),
                    "notices": notices,
                    "files": files,
                    "sources": source,
                    "runtime_records": {
                        r: runtimes[r] for r in config.get("runtimes", [])
                    },
                }
                product_record["release"] = product_hash(product_record)
                if config.get("license-acceptance"):
                    path = config["license-acceptance"]
                    logical = "share/notices/project/" + path
                    license_file = source_file(workspace.root, path)
                    product_record["files"][logical] = {
                        **object_file(license_file, staging),
                        "executable": False,
                    }
                    if logical not in product_record["notices"]:
                        product_record["notices"].append(logical)
                    product_record["license_acceptance"] = {
                        "path": logical,
                        "sha256": digest(license_file),
                    }
                    product_record["release"] = product_hash(product_record)
                products[name] = product_record
            groups = {}
            if selection in workspace.groups:
                group = workspace.groups[selection]
                groups[selection] = {
                    "version": group.get("version", workspace.metadata["version"]),
                    "products": group["products"],
                    "releases": {p: products[p]["release"] for p in selected},
                }
            manifest = {
                "schema": 2,
                "helper_revision": REVISION,
                "kind": "group" if groups else "product",
                "name": workspace.metadata["name"],
                "version": groups[selection]["version"]
                if groups
                else products[selection]["version"],
                "selection": selection,
                "variant": variant,
                "target": target,
                "sources": source,
                "products": products,
                "groups": groups,
                "runtimes": runtimes,
            }
        manifest["builds"] = workspace.build_records
        manifest["baseline"] = baseline
        manifest["component"] = (
            {
                "id": component,
                "adapter": workspace.components[component]["adapter"],
                "python": workspace.components[component].get("python"),
                "verification": workspace.components[component].get("verification", []),
            }
            if component
            else None
        )
        manifest["helpers"] = (
            native_helpers(target, temporary, staging) if selected else {}
        )
        if selected:
            referenced = {
                entry["artifact"]
                for product in products.values()
                for entry in product["files"].values()
            }
            referenced.update(
                record["artifact"]
                for record in runtimes.values()
                if "artifact" in record
            )
            referenced.update(
                record["artifact"] for record in manifest["helpers"].values()
            )
            # Native version stamping replaces an object; retain only final release bytes.
            for path in (staging / "objects").iterdir():
                if path.relative_to(staging).as_posix() not in referenced:
                    path.unlink()
        require(
            {name: identity(root) for name, root in workspace.sources.items()}
            == source,
            "Source inputs changed during packaging",
        )
        seal(staging, manifest)
        validate(staging)
        staging.rename(destination)
    return {
        "directory": str(destination),
        "manifest": str(destination / MANIFEST),
        "selection": selection,
        "target": target,
    }


def ordered(manifest, selected=None):
    name = selected or manifest["selection"]
    products, groups = manifest["products"], manifest["groups"]
    require(
        name in products or name in groups,
        "Selected product/group is not in this artifact",
    )
    result, visiting = [], set()

    def visit(product):
        require(
            product in products and product not in visiting,
            "Missing/cyclic product dependency",
        )
        if product in result:
            return
        visiting.add(product)
        for child in products[product]["dependencies"]:
            visit(child)
        visiting.remove(product)
        result.append(product)

    for member in groups[name]["products"] if name in groups else [name]:
        visit(member)
    return result


def validate(artifact, *, expected=None):
    root = Path(artifact).absolute()
    if root.name == MANIFEST:
        root = root.parent
    manifest_file = source_file(root, MANIFEST)
    if expected:
        require(
            digest(manifest_file) == expected,
            "Expected release manifest checksum differs",
        )
    manifest = read_json(manifest_file)
    keys = {
        "schema",
        "helper_revision",
        "kind",
        "name",
        "version",
        "selection",
        "variant",
        "target",
        "sources",
        "products",
        "groups",
        "runtimes",
        "files",
        "helpers",
        "component",
        "builds",
        "baseline",
    }
    require(
        set(manifest) == keys
        and manifest["schema"] == 2
        and manifest["helper_revision"] == REVISION,
        "Unsupported/incomplete schema-2 release",
    )
    require(
        manifest["kind"] in {"component", "product", "group"}
        and manifest["target"] in TARGETS,
        "Invalid release kind/target",
    )
    require(
        PRODUCT_ID.fullmatch(manifest["name"])
        and PRODUCT_ID.fullmatch(manifest["selection"]),
        "Invalid release identity",
    )
    require(
        isinstance(manifest["version"], str) and manifest["version"],
        "Invalid release version",
    )
    validate_baseline(manifest["baseline"])
    require(
        isinstance(manifest["builds"], dict) and manifest["builds"],
        "Missing build evidence",
    )
    for name, record in manifest["builds"].items():
        require(
            COMMAND.fullmatch(name)
            and isinstance(record, dict)
            and all(
                isinstance(k, str) and isinstance(v, str) for k, v in record.items()
            ),
            "Invalid build evidence",
        )
    component = manifest["component"]
    if manifest["kind"] == "component":
        require(
            isinstance(component, dict)
            and set(component) == {"id", "adapter", "python", "verification"},
            "Invalid component descriptor",
        )
        require(component["id"] == manifest["selection"], "Component selection differs")
        argv_list(component["verification"], "component verification")
    else:
        require(component is None, "Product release cannot contain component selection")
    files = manifest["files"]
    require(isinstance(files, dict) and files, "Missing artifact inventory")
    actual = set()
    for path in root.rglob("*"):
        require(not path.is_symlink(), "Artifact symlinks are forbidden")
        if path.is_file():
            actual.add(path.relative_to(root).as_posix())
    require(
        actual == set(files) | {MANIFEST, "SHA256SUMS"},
        "Missing/unexpected artifact files",
    )
    total = 0
    for name, info in files.items():
        relative(name)
        require(
            set(info) == {"sha256", "size"}
            and type(info["size"]) is int
            and info["size"] >= 0
            and re.fullmatch(r"[0-9a-f]{64}", info["sha256"]),
            "Invalid artifact descriptor",
        )
        total += info["size"]
        require(
            total <= LIMIT and descriptor(source_file(root, name)) == info,
            "Artifact checksum/size mismatch",
        )
        if name.endswith(".whl"):
            wheel_info(root / name)
    sums = {**files, MANIFEST: descriptor(manifest_file)}
    require(
        (root / "SHA256SUMS").read_text()
        == "".join(f"{v['sha256']}  {k}\n" for k, v in sorted(sums.items())),
        "Checksum inventory differs",
    )
    referenced = set()
    for helper, info in manifest["helpers"].items():
        require(
            helper == "windows_launcher"
            and set(info) == {"artifact", "sha256", "size"},
            "Unknown native helper",
        )
        require(
            info["artifact"] in files
            and files[info["artifact"]] == {k: info[k] for k in ("sha256", "size")},
            "Native helper is not bound to inventory",
        )
        referenced.add(info["artifact"])
    require(
        not manifest["target"].startswith("windows-")
        or manifest["kind"] == "component"
        or "windows_launcher" in manifest["helpers"],
        "Windows release needs a native command launcher",
    )
    product_keys = {
        "id",
        "version",
        "adapter",
        "target",
        "variant",
        "dependencies",
        "runtimes",
        "stateful",
        "commands",
        "verification",
        "python",
        "files",
        "sources",
        "runtime_records",
        "release",
        "extras",
        "notices",
        "baseline",
    }
    for name, product in manifest["products"].items():
        require(
            PRODUCT_ID.fullmatch(name)
            and product_keys <= set(product) <= product_keys | {"license_acceptance"}
            and product["id"] == name,
            "Invalid product descriptor",
        )
        require(
            product_hash(product) == product["release"],
            "Product release identity mismatch",
        )
        require(
            isinstance(product["version"], str) and product["version"],
            "Invalid product version",
        )
        argv_list(product["verification"], "product verification")
        strings(product["notices"], "product notices")
        if "license_acceptance" in product:
            license_record = product["license_acceptance"]
            require(
                isinstance(license_record, dict)
                and set(license_record) == {"path", "sha256"}
                and license_record["path"] in product["notices"]
                and re.fullmatch(r"[0-9a-f]{64}", license_record["sha256"])
                and product["files"].get(license_record["path"], {}).get("sha256")
                == license_record["sha256"],
                "Invalid license acceptance record",
            )
        require(
            set(product["notices"]) <= set(product["files"]),
            "Missing declared product notice",
        )
        strings(product["dependencies"], "product dependencies")
        strings(product["runtimes"], "product runtimes")
        require(isinstance(product["extras"], dict), "Invalid extras map")
        for package_name, extras in product["extras"].items():
            require(COMMAND.fullmatch(package_name), "Invalid extras package")
            strings(extras, "product extras", nonempty=True)
            require(
                all(COMMAND.fullmatch(value) for value in extras),
                "Invalid Python extra",
            )
        require(
            product["python"] is None
            or isinstance(product["python"], str)
            and re.fullmatch(r"3\.\d+(?:\.\d+)?", product["python"]),
            "Invalid Python runtime requirement",
        )
        require(
            product["adapter"] in {"native", "python", "hybrid", "application"}
            and product["target"] == manifest["target"]
            and product["baseline"] == manifest["baseline"]
            and type(product["stateful"]) is bool,
            "Invalid product adapter/target/state",
        )
        require(
            isinstance(product["dependencies"], list)
            and len(set(product["dependencies"])) == len(product["dependencies"]),
            "Invalid product dependencies",
        )
        require(
            product["commands"] and product["verification"] and product["files"],
            "Incomplete product payload",
        )
        for command, value in product["commands"].items():
            require(
                COMMAND.fullmatch(command) and command_record(value) == value,
                "Invalid product command",
            )
            require(
                value["runtime"] is None
                or value["runtime"] in product["runtimes"]
                or value["runtime"] == "python"
                and product["python"],
                "Unknown command runtime",
            )
            logical = value["program"].replace(
                "{exe}", ".exe" if manifest["target"].startswith("windows-") else ""
            )
            require(
                product["adapter"] == "python" or logical in product["files"],
                "Command program is absent",
            )
        for logical, info in product["files"].items():
            relative(logical)
            require(
                set(info) == {"artifact", "sha256", "size", "executable"}
                and type(info["executable"]) is bool,
                "Invalid product file",
            )
            require(
                info["artifact"] in files
                and files[info["artifact"]] == {k: info[k] for k in ("sha256", "size")},
                "Product file is not bound to inventory",
            )
            referenced.add(info["artifact"])
            if logical.endswith(".whl"):
                wheel_info(root / info["artifact"])
        require(
            set(product["runtime_records"]) == set(product["runtimes"]),
            "Runtime record set differs",
        )
        for runtime in product["runtimes"]:
            require(
                runtime in manifest["runtimes"]
                and product["runtime_records"][runtime]
                == manifest["runtimes"][runtime],
                "Product/runtime mapping differs",
            )
        ordered(manifest, name)
    for name, group in manifest["groups"].items():
        strings(group["products"], "group products", nonempty=True)
        require(
            PRODUCT_ID.fullmatch(name)
            and name not in manifest["products"]
            and set(group) == {"version", "products", "releases"},
            "Invalid group",
        )
        selected = ordered(manifest, name)
        require(
            group["releases"]
            == {p: manifest["products"][p]["release"] for p in selected},
            "Group release set differs",
        )
    for name, record in manifest["runtimes"].items():
        require(COMMAND.fullmatch(name), "Invalid runtime ID")
        validate_keys(
            record,
            {
                "artifact",
                "url",
                "sha256",
                "executable",
                "role",
                "version",
                "target",
                "format",
                "strip",
            },
            "runtime descriptor",
        )
        require(
            ("artifact" in record) != ("url" in record)
            and record["target"] == manifest["target"]
            and re.fullmatch(r"[0-9a-f]{64}", record["sha256"]),
            "Invalid runtime descriptor",
        )
        relative(record["executable"])
        require(
            record["role"] in {"python", "node", "uv", "helper", "other"}
            and record.get("format", "tar") in {"tar", "zip"}
            and type(record.get("strip", 0)) is int
            and 0 <= record.get("strip", 0) <= 8,
            "Invalid runtime role/format/strip",
        )
        if record.get("artifact"):
            require(
                record["artifact"] in files
                and files[record["artifact"]]["sha256"] == record["sha256"],
                "Runtime is not bound to artifact inventory",
            )
            referenced.add(record["artifact"])
    if manifest["kind"] != "component":
        ordered(manifest)
        require(referenced == set(files), "Artifact contains unreferenced files")
    return root, manifest


def export(artifact, output, *, report=None):
    root, manifest = validate(artifact)
    evidence = None
    if report:
        evidence = read_json(report)
        require(
            evidence.get("schema") == 2
            and evidence.get("kind") == "installation-verification"
            and evidence.get("verified") is True
            and evidence.get("manifest_sha256") == digest(root / MANIFEST)
            and evidence.get("target") == manifest["target"]
            and type(evidence.get("installation_tested")) is bool,
            "Verification report does not match this exact artifact",
        )
    destination = Path(output).absolute()
    require(
        not destination.exists() and not destination.is_symlink(),
        "Export destination already exists",
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".export-", dir=destination.parent
    ) as temporary:
        staging = Path(temporary) / "release"
        shutil.copytree(root, staging / "artifacts")
        descriptor_record = {
            "schema": 2,
            "kind": "release-export",
            "name": manifest["name"],
            "version": manifest["version"],
            "selection": manifest["selection"],
            "target": manifest["target"],
            "manifest": "artifacts/" + MANIFEST,
            "manifest_sha256": digest(root / MANIFEST),
            "products": {
                p: {
                    "version": v["version"],
                    "release": v["release"],
                    "commands": sorted(v["commands"]),
                }
                for p, v in manifest["products"].items()
            },
            "groups": manifest["groups"],
            "publication": "not-published",
            "verification": {
                "static": True,
                "runtime": bool(evidence and evidence["installation_tested"]),
                "report": "verification-report.json" if evidence else None,
            },
        }
        if evidence:
            write_json(staging / "verification-report.json", evidence)
        write_json(staging / "product-descriptor.json", descriptor_record)
        staging.rename(destination)
    return {
        "directory": str(destination),
        "descriptor": str(destination / "product-descriptor.json"),
        "published": False,
    }
