// SPDX-License-Identifier: Apache-2.0

//! Native `uc` process entrypoint.

fn main() -> std::process::ExitCode {
    std::process::ExitCode::from(undercurrent_cli::run_native_cli_process().code())
}
