// SPDX-License-Identifier: Apache-2.0

use std::fs;
use std::path::{Path, PathBuf};

const FR_MUX_002_APPLICABILITY: &str = "not_applicable_no_browser_frontend";

#[test]
fn fr_mux_002_is_conditional_because_no_browser_frontend_exists_in_this_product() {
    assert_eq!(
        FR_MUX_002_APPLICABILITY,
        "not_applicable_no_browser_frontend"
    );
    let project = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("Undercurrent project root");
    let browser_sources = browser_sources(project);
    assert!(
        browser_sources.is_empty(),
        "FR-MUX-002 becomes applicable when a browser frontend exists; found: {browser_sources:?}"
    );
}

fn browser_sources(project: &Path) -> Vec<PathBuf> {
    let mut pending = vec![project.to_path_buf()];
    let mut browser_sources = Vec::new();

    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory).expect("read project source directory") {
            let entry = entry.expect("read project source entry");
            let path = entry.path();
            if path.is_dir() {
                // The documentation guide installs its Mermaid renderer here.
                // Exclude this tool directory, not other files under build/.
                if path != project.join("build/docs-tools")
                    && !is_generated_or_evidence_directory(&path)
                {
                    pending.push(path);
                }
                continue;
            }
            let relative = path.strip_prefix(project).unwrap_or(&path);
            // The terminal demo runs a Node JSON API, not a browser client.
            // Match individual files so a frontend added beside the workload
            // is still detected.
            if [
                "licenses/RUST-STDLIB.html",
                "licenses/inputs/rust-1.95.0-COPYRIGHT-library.html",
                "docs/assets/demos/fixtures/server/package.json",
                "docs/assets/demos/fixtures/server/server.mjs",
            ]
            .iter()
            .any(|exception| relative == Path::new(exception))
            {
                continue;
            }
            if is_browser_source_marker(&path) {
                browser_sources.push(relative.to_path_buf());
            }
        }
    }

    browser_sources.sort();
    browser_sources
}

#[test]
fn documentation_previews_do_not_hide_product_browser_sources() {
    let project = tempfile::tempdir().expect("temporary project");
    let root = project.path();
    fs::create_dir_all(root.join("scripts/docs")).unwrap();
    fs::create_dir_all(root.join(".documentation-task/pages")).unwrap();
    fs::write(root.join(".documentation-task/pages/index.html"), "preview").unwrap();
    fs::create_dir_all(root.join("build/docs-tools/node_modules/renderer")).unwrap();
    fs::write(
        root.join("build/docs-tools/node_modules/renderer/package.json"),
        "{}",
    )
    .unwrap();
    fs::create_dir_all(root.join("docs/assets/demos/fixtures/server")).unwrap();
    fs::write(
        root.join("docs/assets/demos/fixtures/server/package.json"),
        r#"{"scripts":{"start":"node server.mjs"}}"#,
    )
    .unwrap();
    fs::write(
        root.join("docs/assets/demos/fixtures/server/server.mjs"),
        "// Node API workload used by terminal recordings",
    )
    .unwrap();
    assert!(browser_sources(root).is_empty());

    fs::create_dir_all(root.join("web")).unwrap();
    fs::write(root.join("build/client.js"), "// must be detected").unwrap();
    fs::write(root.join("web/index.html"), "product frontend").unwrap();
    fs::write(root.join("scripts/docs/another.mjs"), "// must be detected").unwrap();
    fs::write(
        root.join("docs/assets/demos/fixtures/server/index.html"),
        "browser UI beside the Node workload must be detected",
    )
    .unwrap();
    fs::write(
        root.join("docs/assets/demos/fixtures/server/client.mjs"),
        "// browser client beside the Node workload must be detected",
    )
    .unwrap();
    assert_eq!(
        browser_sources(root),
        vec![
            PathBuf::from("build/client.js"),
            PathBuf::from("docs/assets/demos/fixtures/server/client.mjs"),
            PathBuf::from("docs/assets/demos/fixtures/server/index.html"),
            PathBuf::from("scripts/docs/another.mjs"),
            PathBuf::from("web/index.html"),
        ]
    );
}

fn is_generated_or_evidence_directory(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            matches!(
                name,
                ".git"
                    | ".agents"
                    | ".codex"
                    | ".documentation-task"
                    | "target"
                    | "reports"
                    | "vendor"
            )
        })
}

fn is_browser_source_marker(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    if matches!(
        name,
        "angular.json"
            | "package.json"
            | "index.html"
            | "manifest.webmanifest"
            | "browserconfig.xml"
            | "service-worker.js"
            | "service-worker.mjs"
            | "service-worker.cjs"
    ) || name.starts_with("vite.config.")
        || name.starts_with("webpack.config.")
        || name.starts_with("next.config.")
        || name.starts_with("nuxt.config.")
        || name.starts_with("svelte.config.")
        || name.starts_with("astro.config.")
    {
        return true;
    }
    matches!(
        path.extension().and_then(|extension| extension.to_str()),
        Some("js" | "mjs" | "cjs" | "html" | "htm" | "ts" | "tsx" | "jsx" | "vue" | "svelte")
    ) || rust_manifest_declares_browser_target(path)
}

fn rust_manifest_declares_browser_target(path: &Path) -> bool {
    if path.file_name().and_then(|name| name.to_str()) != Some("Cargo.toml") {
        return false;
    }
    let Ok(source) = fs::read_to_string(path) else {
        return false;
    };
    ["wasm-bindgen", "wasm-bindgen-futures", "web-sys", "js-sys"]
        .iter()
        .any(|dependency| source.contains(dependency))
}
