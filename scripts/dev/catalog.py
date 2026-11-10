"""Schema-2 workspaces: build components, independent products, and collections."""

from __future__ import annotations

from pathlib import Path
from itertools import zip_longest
import platform
import re
from types import SimpleNamespace
import tomllib

from core import Project, relative, require, validate_keys

REVISION = 2
PRODUCT_ID = re.compile(r"[a-z0-9][a-z0-9_-]*(?:/[a-z0-9][a-z0-9_-]*)*\Z")
COMMAND = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
TARGETS = {
    "linux-x86_64-gnu",
    "linux-arm64-gnu",
    "macos-x86_64",
    "macos-arm64",
    "windows-x86_64-msvc",
    "windows-arm64-msvc",
}


def identity(root):
    return Project.identity(SimpleNamespace(root=Path(root).resolve()))


def host():
    machine = platform.machine().lower()
    arch = {"amd64": "x86_64", "aarch64": "arm64"}.get(machine, machine)
    system = platform.system()
    target = {
        "Linux": f"linux-{arch}-gnu",
        "Darwin": f"macos-{arch}",
        "Windows": f"windows-{arch}-msvc",
    }.get(system)
    require(target in TARGETS, "Unsupported host target")
    require(
        system != "Linux" or platform.libc_ver()[0] == "glibc",
        "Linux product installation currently requires GNU libc",
    )
    return target


def host_baseline():
    if platform.system() == "Linux":
        return {"libc": platform.libc_ver()[1]}
    if platform.system() == "Darwin":
        return {"os-version": platform.mac_ver()[0]}
    return {"os-version": platform.win32_ver()[1] or platform.version()}


def validate_baseline(value):
    validate_keys(value, {"libc", "os-version"}, "platform baseline")
    require(value, "A platform baseline cannot be empty")
    for required in value.values():
        require(
            isinstance(required, str) and re.fullmatch(r"\d+(?:\.\d+)*", required),
            "Baseline versions must be numeric dotted versions",
        )


def check_baseline(value):
    validate_baseline(value)
    current = host_baseline()
    for key, required in value.items():
        require(key in current, "Baseline does not apply to this host")
        versions = list(
            zip_longest(
                map(int, current[key].split(".")),
                map(int, required.split(".")),
                fillvalue=0,
            )
        )
        require(
            tuple(v[0] for v in versions) >= tuple(v[1] for v in versions),
            "Host is older than the declared platform baseline",
        )


def strings(value, label, *, nonempty=False):
    require(
        isinstance(value, list)
        and all(isinstance(x, str) and x for x in value)
        and len(value) == len(set(value))
        and (value or not nonempty),
        f"{label} must be an array of distinct nonempty strings",
    )
    return value


def argv_list(value, label):
    require(isinstance(value, list), f"{label} must be an array")
    for argv in value:
        require(
            isinstance(argv, list)
            and argv
            and all(isinstance(x, str) and "\0" not in x for x in argv),
            f"{label} must contain argument arrays",
        )
    return value


def command_record(value):
    validate_keys(value, {"program", "args", "env", "runtime"}, "command")
    relative(value["program"])
    args = value.get("args", [])
    require(
        isinstance(args, list)
        and all(isinstance(x, str) and "\0" not in x for x in args),
        "Command args must be strings",
    )
    env = value.get("env", {})
    require(
        isinstance(env, dict)
        and all(
            re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", k)
            and isinstance(v, str)
            and "\0" not in v
            for k, v in env.items()
        ),
        "Invalid command environment",
    )
    return {
        "program": value["program"],
        "args": args,
        "env": env,
        "runtime": value.get("runtime"),
    }


class Workspace:
    def __init__(self, root):
        self.root = Path(root).resolve()
        config = tomllib.loads((self.root / "installation.toml").read_text())
        validate_keys(
            config,
            {
                "schema",
                "profile",
                "workspace",
                "sources",
                "components",
                "products",
                "groups",
                "runtimes",
                "commands",
            },
            "workspace",
        )
        require(
            config.get("schema") == 2 and config.get("profile") == "workspace",
            "Expected schema=2, profile=workspace",
        )
        self.config = config
        self.metadata = config.get("workspace", {})
        validate_keys(
            self.metadata,
            {
                "name",
                "version",
                "default-product",
                "targets",
                "release-tag",
                "baselines",
            },
            "workspace metadata",
        )
        require(
            PRODUCT_ID.fullmatch(self.metadata.get("name", "")),
            "Invalid workspace name",
        )
        require(
            isinstance(self.metadata.get("version"), str) and self.metadata["version"],
            "Workspace release version is required",
        )
        self.targets = strings(
            self.metadata.get("targets", []), "workspace.targets", nonempty=True
        )
        require(set(self.targets) <= TARGETS, "Unknown workspace target")
        baselines = self.metadata.get("baselines", {})
        require(
            isinstance(baselines, dict) and set(baselines) <= set(self.targets),
            "Unknown baseline target",
        )
        for baseline in baselines.values():
            validate_baseline(baseline)
        self.commands = config.get("commands", {})
        validate_keys(self.commands, {"develop", "check", "test"}, "workspace commands")
        for name, value in self.commands.items():
            argv_list(value, name)
        self.sources = {"project": self.root}
        for name, value in config.get("sources", {}).items():
            require(COMMAND.fullmatch(name) and name != "project", "Invalid source ID")
            validate_keys(value, {"path"}, "source")
            path = Path(value["path"])
            if not path.is_absolute():
                path = self.root / path
            require(
                path.is_dir() and not path.is_symlink(),
                "Source must be an existing regular directory",
            )
            self.sources[name] = path.resolve()
        self.components = config.get("components", {})
        require(isinstance(self.components, dict), "components must be a table")
        for name, value in self.components.items():
            require(COMMAND.fullmatch(name), "Invalid component ID")
            validate_keys(
                value,
                {
                    "adapter",
                    "source",
                    "path",
                    "build",
                    "files",
                    "manifest",
                    "binaries",
                    "features",
                    "dependencies",
                    "python",
                    "extras",
                    "notices",
                    "wheels",
                    "toolchain",
                    "verification",
                },
                "component",
            )
            require(
                value.get("adapter") in {"payload", "python", "cargo", "legacy"},
                "Unknown component adapter",
            )
            require(
                value.get("source", "project") in self.sources,
                "Unknown component source",
            )
            relative(value.get("path", "component")) if value.get("path") else None
            argv_list(value.get("build", []), "component.build")
            argv_list(value.get("verification", []), "component.verification")
            for item in value.get("files", []):
                validate_keys(
                    item, {"source", "destination", "executable"}, "component file"
                )
                relative(item["source"])
                relative(item["destination"])
                require(
                    type(item.get("executable", False)) is bool,
                    "executable must be boolean",
                )
            strings(value.get("dependencies", []), "component.dependencies")
        self.runtimes = config.get("runtimes", {})
        require(isinstance(self.runtimes, dict), "runtimes must be a table")
        self.runtime_matrix = {}
        runtime_keys = {
            "path",
            "url",
            "sha256",
            "executable",
            "role",
            "version",
            "target",
            "format",
            "strip",
        }
        for name, value in self.runtimes.items():
            require(COMMAND.fullmatch(name), "Invalid runtime ID")
            if "targets" in value:
                validate_keys(value, {"role", "version", "targets"}, "runtime matrix")
                require(
                    isinstance(value["targets"], dict) and value["targets"],
                    "Empty runtime matrix",
                )
                records = {}
                for target, child in value["targets"].items():
                    validate_keys(
                        child, runtime_keys - {"role", "target"}, "runtime target"
                    )
                    records[target] = {k: v for k, v in value.items() if k != "targets"}
                    records[target].update(child)
                    records[target]["target"] = target
            else:
                records = {value.get("target"): value}
            self.runtime_matrix[name] = records
            for target, record in records.items():
                validate_keys(record, runtime_keys, "runtime")
                require(
                    ("path" in record) != ("url" in record),
                    "Runtime needs exactly one path or URL",
                )
                require(
                    re.fullmatch(r"[0-9a-f]{64}", record.get("sha256", "")),
                    "Runtime needs SHA-256",
                )
                require(
                    record.get("role") in {"python", "node", "uv", "helper", "other"},
                    "Unknown runtime role",
                )
                require(target in self.targets, "Runtime target is not declared")
                relative(record["executable"])
                require(
                    record.get("format", "tar") in {"tar", "zip"},
                    "Unknown runtime format",
                )
                require(
                    type(record.get("strip", 0)) is int
                    and 0 <= record.get("strip", 0) <= 8,
                    "Invalid runtime strip count",
                )
        self.products = config.get("products", {})
        require(
            isinstance(self.products, dict) and self.products,
            "Declare installable products",
        )
        for name, value in self.products.items():
            require(PRODUCT_ID.fullmatch(name), "Invalid hierarchical product ID")
            validate_keys(
                value,
                {
                    "version",
                    "version-from",
                    "adapter",
                    "components",
                    "dependencies",
                    "commands",
                    "verification",
                    "runtimes",
                    "stateful",
                    "variants",
                    "targets",
                    "python",
                    "release-tag",
                    "extras",
                    "license-acceptance",
                },
                "product",
            )
            require(
                value.get("adapter") in {"native", "python", "hybrid", "application"},
                "Unknown product adapter",
            )
            strings(value.get("components", []), "product.components", nonempty=True)
            strings(value.get("dependencies", []), "product.dependencies")
            strings(value.get("runtimes", []), "product.runtimes")
            if "license-acceptance" in value:
                relative(value["license-acceptance"])
            for path in strings(value.get("notices", []), "product.notices"):
                relative(path)
            require(
                isinstance(value.get("extras", {}), dict),
                "Product extras must be a package-to-extras table",
            )
            for package_name, extras in value.get("extras", {}).items():
                require(COMMAND.fullmatch(package_name), "Invalid extras package")
                strings(extras, "product.extras", nonempty=True)
            require(
                not value.get("python")
                or isinstance(value["python"], str)
                and re.fullmatch(r"3\.\d+(?:\.\d+)?", value["python"]),
                "Invalid product Python version",
            )
            require(
                type(value.get("stateful", True)) is bool, "stateful must be boolean"
            )
            require(
                set(value.get("targets", self.targets)) <= set(self.targets),
                "Unknown product target",
            )
            commands = value.get("commands", {})
            require(isinstance(commands, dict) and commands, "Product needs commands")
            for command, record in commands.items():
                require(
                    COMMAND.fullmatch(command)
                    and not command.lower().endswith((".cmd", ".exe", ".bat")),
                    "Command names must omit platform suffixes",
                )
                command_record(record)
            verification = value.get("verification", [])
            argv_list(verification, "product.verification")
            require(verification, "Product needs meaningful verification")
            for variant, changes in value.get("variants", {}).items():
                require(COMMAND.fullmatch(variant), "Invalid variant")
                validate_keys(
                    changes,
                    {"components", "runtimes", "dependencies", "commands", "extras"},
                    "variant",
                )
        self.groups = config.get("groups", {})
        for name, value in self.groups.items():
            require(
                PRODUCT_ID.fullmatch(name) and name not in self.products,
                "Invalid/conflicting group ID",
            )
            validate_keys(value, {"version", "products", "release-tag"}, "group")
            strings(value.get("products", []), "group.products", nonempty=True)
            require(
                set(value["products"]) <= set(self.products),
                "Group contains unknown products",
            )
        for name in self.products:
            self.select(name)
        self.default = self.metadata.get("default-product")
        require(
            self.default in self.products or self.default in self.groups,
            "Declare a default product/group",
        )
        self.component_order(list(self.components))

    def runtime(self, name, target):
        require(
            name in self.runtime_matrix and target in self.runtime_matrix[name],
            "Runtime has no declared artifact for selected target",
        )
        return dict(self.runtime_matrix[name][target])

    def component_root(self, name):
        item = self.components[name]
        root = self.sources[item.get("source", "project")]
        path = root / relative(item["path"]) if item.get("path") else root
        require(
            path.is_dir() and path.resolve().is_relative_to(root),
            "Component escapes declared source",
        )
        return path.resolve()

    def component_order(self, names):
        result, visiting = [], set()

        def visit(name):
            require(name in self.components, f"Unknown component: {name}")
            require(name not in visiting, "Component dependency cycle")
            if name in result:
                return
            visiting.add(name)
            for child in self.components[name].get("dependencies", []):
                visit(child)
            visiting.remove(name)
            result.append(name)

        for name in names:
            visit(name)
        return result

    def product(self, name, variant=None):
        require(name in self.products, f"Unknown product: {name}")
        value = dict(self.products[name])
        if variant:
            require(
                variant in value.get("variants", {}),
                f"Unknown variant for {name}: {variant}",
            )
            value.update(value["variants"][variant])
        require(
            set(value["components"]) <= set(self.components),
            "Product has unknown components",
        )
        require(
            set(value.get("runtimes", [])) <= set(self.runtimes),
            "Product has unknown runtimes",
        )
        return value

    def select(self, name=None, variant=None):
        name = name or self.default
        require(
            name in self.products or name in self.groups,
            f"Unknown product/group: {name}",
        )
        require(
            not variant or name in self.products,
            "Select variants on individual products",
        )
        result, visiting = [], set()

        def visit(product):
            require(product not in visiting, "Product dependency cycle")
            if product in result:
                return
            item = self.product(product, variant if product == name else None)
            visiting.add(product)
            for child in item.get("dependencies", []):
                visit(child)
            visiting.remove(product)
            result.append(product)

        for member in self.groups[name]["products"] if name in self.groups else [name]:
            visit(member)
        return result
