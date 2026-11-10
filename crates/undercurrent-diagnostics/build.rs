// SPDX-License-Identifier: Apache-2.0

use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=UNDERCURRENT_BUILD_REVISION");
    let revision = std::env::var("UNDERCURRENT_BUILD_REVISION")
        .ok()
        .or_else(|| {
            let output = Command::new("git")
                .args(["rev-parse", "HEAD"])
                .output()
                .ok()?;
            output
                .status
                .success()
                .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        });
    if let Some(revision) = revision
        .filter(|s| !s.is_empty() && s.len() <= 64 && s.bytes().all(|c| c.is_ascii_hexdigit()))
    {
        println!("cargo:rustc-env=UC_BUILD_REVISION={revision}");
    }
    // Git refs can move without changing Rust sources.
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/refs");
}
