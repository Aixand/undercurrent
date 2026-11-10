// SPDX-License-Identifier: Apache-2.0

use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn ordinary_commands_have_no_process_launch_capability() {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut sources = Vec::new();
    collect_rust_sources(&source_root, &mut sources);

    for path in sources {
        let source = fs::read_to_string(&path).expect("read CLI source");
        // Clap constructs parser commands, not operating-system processes.
        let launch_source = source.replace("clap::Command::new(", "clap_parser_builder(");
        let relative_path = path.strip_prefix(&source_root).expect("CLI source path");
        let lifecycle_owned = relative_path == Path::new("daemon_lifecycle.rs")
            || relative_path == Path::new("daemon_lifecycle/process_launch.rs");
        for process_launch_marker in [
            "use std::process::Command",
            "use std::process::{Command",
            "Command::new(",
            "isolate_background_process",
            "launch_daemon(",
        ] {
            assert!(
                lifecycle_owned || !launch_source.contains(process_launch_marker),
                "ordinary CLI source {} must not contain process-launch marker {process_launch_marker}",
                path.display()
            );
        }
    }

    let client_source = fs::read_to_string(source_root.join("client.rs"))
        .expect("read native product client source");
    assert!(!client_source.contains("launch_resolved"));
    assert!(!client_source.contains("spawn()"));
    assert!(
        !source_root.join("process_launch.rs").exists(),
        "process launch capability must remain private beneath daemon_lifecycle"
    );
}

fn collect_rust_sources(directory: &Path, output: &mut Vec<PathBuf>) {
    let mut entries = fs::read_dir(directory)
        .expect("read CLI source directory")
        .map(|entry| entry.expect("read CLI source entry").path())
        .collect::<Vec<_>>();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            collect_rust_sources(&path, output);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            output.push(path);
        }
    }
}
