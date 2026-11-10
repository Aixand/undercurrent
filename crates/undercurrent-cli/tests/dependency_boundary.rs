// SPDX-License-Identifier: Apache-2.0

//! Compile-independent dependency guards for the external-daemon boundary.

use std::fs;
use std::path::{Path, PathBuf};

const CLI_FORBIDDEN_DEPENDENCIES: &[&str] = &[
    "undercurrent-application",
    "undercurrent-daemon",
    "undercurrent-domain",
    "undercurrent-multiplex",
    "undercurrent-runtime",
    "undercurrent-store-sqlite",
    "undercurrent-wire",
];

#[test]
fn cli_links_only_public_product_protocol_and_sdk_boundaries() {
    let manifest = read(&crates_root().join("undercurrent-cli/Cargo.toml"));
    for dependency in CLI_FORBIDDEN_DEPENDENCIES {
        assert!(
            !manifest
                .lines()
                .any(|line| line.trim_start().starts_with(&format!("{dependency} ="))),
            "CLI must not link {dependency}"
        );
    }
}

fn crates_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("CLI crate parent")
        .to_path_buf()
}

fn read(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|error| panic!("could not read {path:?}: {error}"))
}
