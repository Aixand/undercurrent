// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

const PRODUCT_PREFIXES: [&str; 2] = ["undercurrent-", "aixand-undercurrent"];

fn permitted_product_dependencies() -> BTreeMap<&'static str, BTreeSet<&'static str>> {
    BTreeMap::from([
        (
            "undercurrent-application",
            BTreeSet::from(["undercurrent-domain"]),
        ),
        (
            "undercurrent-cli",
            BTreeSet::from([
                "undercurrent-config",
                "undercurrent-diagnostics",
                "aixand-undercurrent-platform-windows",
                "aixand-undercurrent-protocol",
                "aixand-undercurrent",
            ]),
        ),
        (
            "undercurrent-config",
            BTreeSet::from(["aixand-undercurrent-foundation"]),
        ),
        ("undercurrent-platform-macos", BTreeSet::from([])),
        ("aixand-undercurrent-platform-windows", BTreeSet::from([])),
        (
            "undercurrent-daemon",
            BTreeSet::from([
                "undercurrent-application",
                "undercurrent-config",
                "undercurrent-diagnostics",
                "undercurrent-domain",
                "aixand-undercurrent-platform-windows",
                "aixand-undercurrent-multiplex",
                "aixand-undercurrent-protocol",
                "undercurrent-runtime",
                "aixand-undercurrent",
                "undercurrent-store-sqlite",
                "aixand-undercurrent-wire",
                "aixand-undercurrent-transport-local",
                "aixand-undercurrent-transport-core",
            ]),
        ),
        (
            "undercurrent-diagnostics",
            BTreeSet::from(["aixand-undercurrent-platform-windows"]),
        ),
        (
            "undercurrent-domain",
            BTreeSet::from(["aixand-undercurrent-foundation"]),
        ),
        ("aixand-undercurrent-foundation", BTreeSet::from([])),
        (
            "undercurrent-mcp",
            BTreeSet::from([
                "undercurrent-config",
                "aixand-undercurrent-protocol",
                "aixand-undercurrent",
            ]),
        ),
        (
            "aixand-undercurrent-multiplex",
            BTreeSet::from(["aixand-undercurrent-wire"]),
        ),
        (
            "aixand-undercurrent-protocol",
            BTreeSet::from(["aixand-undercurrent-foundation"]),
        ),
        (
            "undercurrent-python-bridge",
            BTreeSet::from(["aixand-undercurrent-protocol", "aixand-undercurrent"]),
        ),
        (
            "undercurrent-runtime",
            BTreeSet::from([
                "aixand-undercurrent-foundation",
                "undercurrent-application",
                "undercurrent-domain",
                "undercurrent-platform-macos",
                "aixand-undercurrent-platform-windows",
            ]),
        ),
        (
            "aixand-undercurrent",
            BTreeSet::from([
                "aixand-undercurrent-multiplex",
                "aixand-undercurrent-protocol",
                "aixand-undercurrent-wire",
                "aixand-undercurrent-transport-local",
            ]),
        ),
        (
            "undercurrent-store-sqlite",
            BTreeSet::from([
                "undercurrent-application",
                "undercurrent-domain",
                "aixand-undercurrent-foundation",
                // The leaf adapter scopes native file ownership for SQLite;
                // it has no dependencies on product policy or runtime services.
                "aixand-undercurrent-platform-windows",
            ]),
        ),
        (
            "aixand-undercurrent-transport-core",
            BTreeSet::from([
                "aixand-undercurrent-multiplex",
                "aixand-undercurrent-protocol",
                "aixand-undercurrent-wire",
            ]),
        ),
        (
            "aixand-undercurrent-transport-local",
            BTreeSet::from([
                "aixand-undercurrent-multiplex",
                "aixand-undercurrent-platform-windows",
                "aixand-undercurrent-protocol",
                "aixand-undercurrent-transport-core",
                "aixand-undercurrent-wire",
            ]),
        ),
        (
            "aixand-undercurrent-wire",
            BTreeSet::from(["aixand-undercurrent-foundation"]),
        ),
    ])
}

#[test]
fn daemon_entry_and_composition_are_thin_non_semantic_facades() {
    let root = product_root();
    let entry = std::fs::read_to_string(root.join("crates/undercurrent-daemon/src/main.rs"))
        .expect("daemon entry source is readable");
    let entry = entry
        .split("\n#[cfg(test)]")
        .next()
        .expect("production entry source exists");
    let composition =
        std::fs::read_to_string(root.join("crates/undercurrent-daemon/src/composition.rs"))
            .expect("daemon composition source is readable");
    let composition = composition
        .split("\n#[cfg(test)]")
        .next()
        .expect("production composition source exists")
        .split("\n#[cfg(all(test,")
        .next()
        .expect("production composition excludes platform-specific test modules");

    assert_eq!(
        public_facade_declarations(entry),
        Vec::<String>::new(),
        "the process entry must not expose reusable behavior"
    );
    assert!(
        !entry
            .lines()
            .any(|line| line.trim_start().starts_with("mod ")),
        "the process entry must delegate through the daemon facade instead of owning modules"
    );
    assert_eq!(
        entry
            .matches("DaemonProcess::compose_with_log_pressure")
            .count(),
        1,
        "the process entry must construct the daemon through exactly one composition call"
    );
    for forbidden_crate in [
        "undercurrent_application",
        "undercurrent_domain",
        "undercurrent_multiplex",
        "undercurrent_protocol",
        "undercurrent_runtime",
        "undercurrent_sdk",
        "undercurrent_store_sqlite",
        "undercurrent_transport_local",
        "undercurrent_wire",
    ] {
        assert!(
            !entry.contains(forbidden_crate),
            "the process entry directly references forbidden crate {forbidden_crate}"
        );
    }

    let mut expected_facade = vec![
        "const fn epoch".to_owned(),
        "const fn started_at_millis".to_owned(),
        "fn compose".to_owned(),
        "fn compose_with_external_adapters".to_owned(),
        "fn compose_with_log_pressure".to_owned(),
        "fn compose_with_runtime_cancellation_authorizer".to_owned(),
        "fn compose_with_secret_resolver".to_owned(),
        "struct DaemonProcess".to_owned(),
        "use error::*".to_owned(),
        "use identity::{DaemonInstanceState, daemon_instance_state}".to_owned(),
        "use lifecycle::*".to_owned(),
    ];
    expected_facade.sort_unstable();
    assert_eq!(
        public_facade_declarations(composition),
        expected_facade,
        "composition.rs may expose only the daemon composition/lifecycle facade"
    );

    for forbidden_semantic_construct in [
        "match request",
        "match command",
        "match operation",
        "match session",
        "match status",
        "ProductRequest::",
        "ProductResponse::",
        "ProductEvent::",
        "WorkStatus::",
        "SessionStatus::",
        "ForegroundAxis::",
        "ForegroundState::",
        "HealthStatus::",
        ".get(",
        ".list(",
        ".find(",
        ".query(",
        ".upsert(",
        ".update(",
        ".delete(",
        "SELECT ",
        "INSERT ",
        "UPDATE ",
        "DELETE ",
    ] {
        assert!(
            !composition.contains(forbidden_semantic_construct),
            "composition.rs contains forbidden semantic/store-query construct {forbidden_semantic_construct:?}"
        );
    }
}

fn public_facade_declarations(source: &str) -> Vec<String> {
    let mut declarations = source
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if let Some(item) = line.strip_prefix("pub use ") {
                return Some(format!("use {}", item.trim_end_matches(';')));
            }
            if let Some(item) = line.strip_prefix("pub struct ") {
                let name = item.split_whitespace().next().expect("public struct name");
                return Some(format!("struct {name}"));
            }
            if let Some(item) = line.strip_prefix("pub const fn ") {
                let name = item.split('(').next().expect("public const function name");
                return Some(format!("const fn {name}"));
            }
            if let Some(item) = line.strip_prefix("pub fn ") {
                let name = item.split('(').next().expect("public function name");
                return Some(format!("fn {name}"));
            }
            line.starts_with("pub ").then(|| line.to_owned())
        })
        .collect::<Vec<_>>();
    declarations.sort_unstable();
    declarations
}

#[test]
fn production_crate_graph_obeys_the_architecture_contract() {
    let root = product_root();
    let expected = permitted_product_dependencies();
    let workspace = manifest(&root.join("Cargo.toml"));
    let workspace_dependencies = &workspace["workspace"]["dependencies"];
    let mut actual_members = BTreeSet::new();

    for entry in fs::read_dir(root.join("crates")).expect("workspace crates are readable") {
        let path = entry.expect("crate directory is readable").path();
        if !path.join("Cargo.toml").is_file() {
            continue;
        }
        let package = manifest(&path.join("Cargo.toml"));
        let package_name = package
            .get("package")
            .and_then(|value| value.get("name"))
            .and_then(toml::Value::as_str)
            .expect("workspace package must have a string name");
        actual_members.insert(package_name.to_owned());

        let actual_dependencies = production_product_dependencies(&package, workspace_dependencies);
        let permitted = expected
            .get(package_name)
            .unwrap_or_else(|| panic!("{package_name} is missing from the architecture contract"));
        assert_eq!(
            actual_dependencies,
            permitted.iter().copied().map(str::to_owned).collect(),
            "production dependencies for {package_name} changed; update the contract only after reviewing the dependency direction",
        );
    }

    let expected_members = expected
        .keys()
        .copied()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        actual_members, expected_members,
        "the workspace and architecture contract must name the same product crates"
    );
}

fn product_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("daemon manifest must be nested under workspace/crates")
        .to_owned()
}

fn manifest(path: &Path) -> toml::Value {
    let source = fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    toml::from_str(&source)
        .unwrap_or_else(|error| panic!("failed to parse {}: {error}", path.display()))
}

fn production_product_dependencies(
    manifest: &toml::Value,
    workspace_dependencies: &toml::Value,
) -> BTreeSet<String> {
    let mut dependencies = BTreeSet::new();
    collect_dependency_table(
        manifest.get("dependencies"),
        workspace_dependencies,
        &mut dependencies,
    );
    collect_dependency_table(
        manifest.get("build-dependencies"),
        workspace_dependencies,
        &mut dependencies,
    );

    if let Some(targets) = manifest.get("target").and_then(toml::Value::as_table) {
        for target in targets.values() {
            collect_dependency_table(
                target.get("dependencies"),
                workspace_dependencies,
                &mut dependencies,
            );
            collect_dependency_table(
                target.get("build-dependencies"),
                workspace_dependencies,
                &mut dependencies,
            );
        }
    }

    dependencies
}

fn collect_dependency_table(
    value: Option<&toml::Value>,
    workspace_dependencies: &toml::Value,
    dependencies: &mut BTreeSet<String>,
) {
    let Some(table) = value.and_then(toml::Value::as_table) else {
        return;
    };
    for (alias, specification) in table {
        let specification = if specification
            .get("workspace")
            .and_then(toml::Value::as_bool)
            == Some(true)
        {
            workspace_dependencies
                .get(alias)
                .expect("workspace dependency exists")
        } else {
            specification
        };
        let name = specification
            .get("package")
            .and_then(toml::Value::as_str)
            .unwrap_or(alias);
        if PRODUCT_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix))
        {
            dependencies.insert(name.to_owned());
        }
    }
}
