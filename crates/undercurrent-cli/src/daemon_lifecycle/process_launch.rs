// SPDX-License-Identifier: Apache-2.0

//! Native background-process launch policy private to daemon lifecycle control.

use std::process::Command;

/// Isolates a long-lived child from signals targeting the invoking CLI's
/// process group.
///
/// The caller still owns stdio redirection and process-specific arguments.
/// This function performs no IO and does not panic.
pub(super) fn isolate_background_process(command: &mut Command) {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000); // CREATE_NO_WINDOW
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;

        command.process_group(0);
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn child_owns_a_process_group_distinct_from_the_invoking_cli() {
        let mut command = Command::new("sh");
        command.arg("-c").arg("kill -0 -$$");
        isolate_background_process(&mut command);

        assert!(command.status().expect("isolated shell").success());
    }
}
