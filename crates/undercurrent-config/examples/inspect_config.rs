// SPDX-License-Identifier: Apache-2.0
//! Print the effective configuration of an explicit absolute home without writes.
//!
//! Run from the workspace root:
//! `cargo run --locked -p undercurrent-config --example inspect_config -- /absolute/home`
//!
//! Pass `--sample` instead of a home to print the walkthrough's sample YAML.
//! No daemon is needed. Retired configuration environment variables must be unset.

use std::path::PathBuf;
use std::process::ExitCode;

use undercurrent_config::{ConfigDiscovery, PathEnvironment, load_config};

const SAMPLE_CONFIG: &str = r#"schema_version: 0.1.0-beta
cli:
  command_timeout_millis: 45000
  list_page_size: 17
shortcuts:
  prefix: ctrl-a
terminal:
  scrollback:
    memory_lines: 0
"#;

fn main() -> ExitCode {
    let mut arguments = std::env::args_os().skip(1);
    let Some(home) = arguments.next() else {
        eprintln!("usage: inspect_config ABSOLUTE_UNDERCURRENT_HOME | --sample");
        return ExitCode::from(2);
    };
    if arguments.next().is_some() {
        eprintln!("usage: inspect_config ABSOLUTE_UNDERCURRENT_HOME | --sample");
        return ExitCode::from(2);
    }
    if home.to_str() == Some("--sample") {
        print!("{SAMPLE_CONFIG}");
        return ExitCode::SUCCESS;
    }
    let discovery = ConfigDiscovery::from_path_env(
        PathEnvironment::default().with_undercurrent_home(PathBuf::from(home)),
    );
    match load_config(&discovery) {
        Ok(loaded) => match serde_json::to_string_pretty(&loaded.redacted_report()) {
            Ok(report) => {
                println!("{report}");
                ExitCode::SUCCESS
            }
            Err(_) => {
                eprintln!("could not encode configuration report");
                ExitCode::FAILURE
            }
        },
        Err(error) => {
            // Display deliberately avoids the paths carried by Debug/typed errors.
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
