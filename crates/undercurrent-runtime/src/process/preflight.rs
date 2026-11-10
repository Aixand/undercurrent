// SPDX-License-Identifier: Apache-2.0

//! Pre-acceptance launch resolution and spawn-time object-handle pinning.
//!
//! Preparation resolves environment and paths into a mode-bound receipt.
//! Verification retains the handles needed by the later spawn. Native mode
//! permits installed updates at the accepted executable path after safety
//! checks; it does not promise immutable Unix executable bytes. Linux sealed
//! mode stages verified bytes in a sealed memory file and rehashes after sealing.
//! Working-directory identities are checked independently of executable content.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::collections::BTreeMap;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::fs::File;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::sync::{Arc, Mutex, Weak};

use undercurrent_application::PortErrorKind;
use undercurrent_domain::LaunchSpec;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use undercurrent_domain::PayloadDigest;

use super::NativeProcessRuntimeLimits;

#[cfg(windows)]
#[path = "windows_preflight.rs"]
mod windows;

/// Payload-safe launch preparation or verification failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LaunchPreparationError {
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    PlatformUnsupported,
    #[cfg(any(target_os = "macos", windows))]
    SealedLaunchUnsupported,
    Environment,
    MissingEnvironmentBinding,
    InvalidEnvironmentBinding,
    #[cfg(target_os = "linux")]
    SecurePathResolutionUnavailable,
    WorkingDirectoryOutsideBinding,
    WorkingDirectoryUnavailable,
    MissingBindingRootIdentity,
    MissingWorkingDirectoryIdentity,
    StaleBindingRootIdentity,
    StaleWorkingDirectoryIdentity,
    ExecutableReference,
    ExecutableUnavailable,
    ExecutableNotRegular,
    ExecutableNotRunnable,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    ExecutablePrivileged,
    #[cfg(target_os = "linux")]
    ExecutableCapabilities,
    #[cfg(target_os = "linux")]
    ExecutableCapabilityInspection,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    ExecutableScriptUnsupported,
    ExecutableTooLarge,
    ExecutableRead,
    ExecutableDigestMismatch,
    ExecutableIdentityUnavailable,
    MissingExecutableReceipt,
    MissingExecutableIdentity,
    StaleExecutableReceipt,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    ImmutableExecutableUnavailable,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    ProcessHandleFilesystemUnavailable,
    Domain,
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt as _;
    use std::process::Command;

    use tempfile::tempdir;
    use undercurrent_domain::{
        CommandSpec, EnvironmentVariables, ExecutionEnvironmentBinding, ExecutionLimits,
        LaunchSpec, PathReference, PayloadDigest, TerminalSize,
    };

    use super::*;

    fn limits() -> NativeProcessRuntimeLimits {
        NativeProcessRuntimeLimits {
            max_processes: 2,
            spawn_workers: 1,
            spawn_queue_capacity: 2,
            input_queue_sequences: 2,
            input_queue_bytes_per_process: 128,
            output_read_bytes: 128,
            screen_snapshot_bytes: 4_096,
            environment_file_bytes: 1_024,
            environment_entries: 16,
            environment_bytes: 4_096,
            worker_thread_stack_bytes: 64 * 1024,
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn pinned_pty_cwd_survives_rename_and_symlink_replacement() {
        use portable_pty::{CommandBuilder, PtySize, native_pty_system};
        let temp = tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let selected = root.join("selected");
        let replacement = root.join("replacement");
        fs::create_dir(&selected).unwrap();
        fs::create_dir(&replacement).unwrap();
        fs::write(selected.join("marker"), "accepted").unwrap();
        fs::write(replacement.join("marker"), "wrong").unwrap();
        let requested = launch(Path::new("/bin/sh"), &root, &selected);
        let command = requested
            .command()
            .clone()
            .with_launch_mode(undercurrent_domain::LaunchMode::Native);
        let launch = prepare_launch(&requested.with_command(command), limits()).unwrap();
        let handles = verify_prepared_launch(&launch).unwrap();
        let mut command = CommandBuilder::new("/bin/sh");
        command.args(["-c", "cat marker > \"$1\"", "sh"]);
        let output = root.join("output");
        command.arg(&output);
        command
            .cwd_handle(handles.working_directory_handle().unwrap())
            .unwrap();
        drop(handles);
        fs::rename(&selected, root.join("moved")).unwrap();
        std::os::unix::fs::symlink(&replacement, &selected).unwrap();
        let parent_cwd = std::env::current_dir().unwrap();
        let pair = native_pty_system().openpty(PtySize::default()).unwrap();
        let mut child = pair.slave.spawn_command(command).unwrap();
        assert!(child.wait().unwrap().success());
        assert_eq!(fs::read_to_string(output).unwrap(), "accepted");
        assert_eq!(std::env::current_dir().unwrap(), parent_cwd);
    }

    fn domain_limits() -> ExecutionLimits {
        ExecutionLimits {
            max_arguments: 4,
            max_command_bytes: 8_192,
            max_environment_entries: 16,
            max_environment_bytes: 4_096,
            max_path_bytes: 4_096,
            max_health_rules: 1,
            max_health_rule_bytes: 1,
        }
    }

    fn launch(program: &Path, root: &Path, cwd: &Path) -> LaunchSpec {
        LaunchSpec::new(
            CommandSpec::new(program.to_string_lossy(), Vec::new(), domain_limits())
                .expect("command"),
            EnvironmentVariables::new(Vec::new(), domain_limits()).expect("environment"),
            None,
            Some(PathReference::new(cwd.to_string_lossy(), 4_096).expect("cwd")),
            TerminalSize::new(80, 24, 200, 100).expect("terminal"),
        )
        .with_environment_binding(Some(ExecutionEnvironmentBinding::new(
            PayloadDigest::of_bytes(b"binding"),
            PathReference::new(root.to_string_lossy(), 4_096).expect("root"),
        )))
    }

    fn executable(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).expect("write executable");
        let mut permissions = fs::metadata(path).expect("metadata").permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(path, permissions).expect("permissions");
    }

    #[cfg(target_os = "linux")]
    fn copy_binary(source: &Path, destination: &Path) {
        fs::copy(source, destination).expect("copy executable");
        let mut permissions = fs::metadata(destination).expect("metadata").permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(destination, permissions).expect("permissions");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn preparation_pins_executable_and_opened_directory_identities() {
        let directory = tempdir().expect("temporary directory");
        let workspace = directory.path().join("workspace");
        let child = workspace.join("child");
        fs::create_dir_all(&child).expect("workspace");
        let program = directory.path().join("example-tool");
        executable(&program, b"stable executable");

        let prepared = prepare_launch(&launch(&program, &workspace, &child), limits())
            .expect("launch prepares");

        let resolution = prepared
            .command()
            .executable_resolution()
            .expect("executable receipt");
        assert!(resolution.object_identity().is_some());
        assert!(prepared.working_directory_identity().is_some());
        assert!(
            prepared
                .environment_binding()
                .and_then(ExecutionEnvironmentBinding::root_identity)
                .is_some()
        );
        verify_prepared_launch(&prepared).expect("receipt verifies and stages");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn environment_binding_root_becomes_the_default_working_directory() {
        let directory = tempdir().expect("temporary directory");
        let workspace = directory.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace");
        let program = directory.path().join("example-tool");
        executable(&program, b"stable executable");
        let launch = LaunchSpec::new(
            CommandSpec::new(program.to_string_lossy(), Vec::new(), domain_limits())
                .expect("command"),
            EnvironmentVariables::new(Vec::new(), domain_limits()).expect("environment"),
            None,
            None,
            TerminalSize::new(80, 24, 200, 100).expect("terminal"),
        )
        .with_environment_binding(Some(ExecutionEnvironmentBinding::new(
            PayloadDigest::of_bytes(b"binding"),
            PathReference::new(workspace.to_string_lossy(), 4_096).expect("root"),
        )));

        let prepared = prepare_launch(&launch, limits()).expect("launch prepares");
        assert_eq!(
            prepared.working_directory().map(PathReference::as_str),
            workspace.to_str()
        );
        verify_prepared_launch(&prepared).expect("receipt verifies and stages");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn preparation_rejects_directory_escape_and_symlink_escape() {
        let directory = tempdir().expect("temporary directory");
        let workspace = directory.path().join("workspace");
        let outside = directory.path().join("outside");
        fs::create_dir_all(&workspace).expect("workspace");
        fs::create_dir_all(&outside).expect("outside");
        let program = directory.path().join("example-tool");
        executable(&program, b"stable executable");
        assert_eq!(
            prepare_launch(&launch(&program, &workspace, &outside), limits()).unwrap_err(),
            LaunchPreparationError::WorkingDirectoryOutsideBinding
        );

        std::os::unix::fs::symlink(&outside, workspace.join("escape")).expect("escape symlink");
        assert!(matches!(
            prepare_launch(
                &launch(&program, &workspace, &workspace.join("escape")),
                limits()
            ),
            Err(LaunchPreparationError::WorkingDirectoryOutsideBinding
                | LaunchPreparationError::WorkingDirectoryUnavailable)
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn preparation_rejects_digest_mismatch_scripts_and_privileged_files() {
        let directory = tempdir().expect("temporary directory");
        let workspace = directory.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace");
        let program = directory.path().join("example-tool");
        executable(&program, b"stable executable");
        let mismatched = launch(&program, &workspace, &workspace).with_command(
            CommandSpec::new(program.to_string_lossy(), Vec::new(), domain_limits())
                .expect("command")
                .with_expected_executable_digest(Some(PayloadDigest::of_bytes(b"other"))),
        );
        assert_eq!(
            prepare_launch(&mismatched, limits()).unwrap_err(),
            LaunchPreparationError::ExecutableDigestMismatch
        );

        executable(&program, b"#!/bin/sh\nexit 0\n");
        assert_eq!(
            prepare_launch(&launch(&program, &workspace, &workspace), limits()).unwrap_err(),
            LaunchPreparationError::ExecutableScriptUnsupported
        );

        executable(&program, b"native bytes");
        let mut permissions = fs::metadata(&program).expect("metadata").permissions();
        permissions.set_mode(0o4700);
        fs::set_permissions(&program, permissions).expect("setuid mode");
        assert_eq!(
            prepare_launch(&launch(&program, &workspace, &workspace), limits()).unwrap_err(),
            LaunchPreparationError::ExecutablePrivileged
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn same_inode_mutation_after_acceptance_fails_closed() {
        let directory = tempdir().expect("temporary directory");
        let workspace = directory.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace");
        let program = directory.path().join("example-tool");
        executable(&program, b"first executable");
        let prepared = prepare_launch(&launch(&program, &workspace, &workspace), limits())
            .expect("launch prepares");

        executable(&program, b"changed on the same inode");

        assert_eq!(
            verify_prepared_launch(&prepared).err(),
            Some(LaunchPreparationError::StaleExecutableReceipt)
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn sealed_executable_survives_path_replacement_after_verification() {
        let directory = tempdir().expect("temporary directory");
        let workspace = directory.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace");
        let program = directory.path().join("example-tool");
        copy_binary(Path::new("/bin/echo"), &program);
        let prepared = prepare_launch(&launch(&program, &workspace, &workspace), limits())
            .expect("launch prepares");
        let handles = verify_prepared_launch(&prepared).expect("launch handles");

        fs::rename(&program, directory.path().join("accepted-tool")).expect("move accepted path");
        copy_binary(Path::new("/bin/false"), &program);

        let output = Command::new(handles.program_path())
            .arg("sealed")
            .output()
            .expect("execute sealed memfd");
        assert!(output.status.success());
        assert_eq!(output.stdout, b"sealed\n");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn identical_verified_launches_share_runtime_local_object_handles() {
        let directory = tempdir().expect("temporary directory");
        let workspace = directory.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace");
        let program = directory.path().join("example-tool");
        copy_binary(Path::new("/bin/true"), &program);
        let prepared = prepare_launch(&launch(&program, &workspace, &workspace), limits())
            .expect("launch prepares");
        let executable_cache = SealedExecutableCache::new();
        let directory_cache = PinnedDirectoryCache::new();

        let first = verify_prepared_launch_cached(&prepared, &executable_cache, &directory_cache)
            .expect("first handles");
        let second = verify_prepared_launch_cached(&prepared, &executable_cache, &directory_cache)
            .expect("second handles");

        assert_eq!(first.program_path(), second.program_path());
        assert!(first.binding_root_is_working_directory());
        assert_eq!(
            first.working_directory_path(),
            second.working_directory_path()
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cwd_handle_survives_path_replacement_after_verification() {
        let directory = tempdir().expect("temporary directory");
        let workspace = directory.path().join("workspace");
        let child = workspace.join("child");
        fs::create_dir_all(&child).expect("workspace");
        fs::write(child.join("accepted-marker"), b"accepted").expect("marker");
        let program = directory.path().join("example-tool");
        executable(&program, b"stable executable");
        let prepared = prepare_launch(&launch(&program, &workspace, &child), limits())
            .expect("launch prepares");
        let handles = verify_prepared_launch(&prepared).expect("launch handles");

        fs::rename(&child, workspace.join("accepted-child")).expect("move accepted directory");
        fs::create_dir(&child).expect("replacement directory");
        fs::write(child.join("replacement-marker"), b"replacement").expect("replacement marker");

        let pinned = handles
            .working_directory_path()
            .expect("working directory handle");
        assert_eq!(
            fs::read(pinned.join("accepted-marker")).expect("read accepted directory"),
            b"accepted"
        );
        assert!(!pinned.join("replacement-marker").exists());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cwd_path_swap_between_acceptance_and_spawn_fails_closed() {
        let directory = tempdir().expect("temporary directory");
        let workspace = directory.path().join("workspace");
        let child = workspace.join("child");
        fs::create_dir_all(&child).expect("workspace");
        let program = directory.path().join("example-tool");
        executable(&program, b"stable executable");
        let prepared = prepare_launch(&launch(&program, &workspace, &child), limits())
            .expect("launch prepares");

        fs::rename(&child, workspace.join("accepted-child")).expect("move accepted directory");
        fs::create_dir(&child).expect("replacement directory");

        assert_eq!(
            verify_prepared_launch(&prepared).err(),
            Some(LaunchPreparationError::StaleWorkingDirectoryIdentity)
        );
    }

    fn native_launch(program: &Path, root: &Path) -> LaunchSpec {
        let launch = launch(program, root, root);
        let command = launch
            .command()
            .clone()
            .with_launch_mode(undercurrent_domain::LaunchMode::Native);
        launch.with_command(command)
    }

    #[test]
    fn native_preserves_symlink_invocation_and_runs_script_updates() {
        let directory = tempdir().expect("directory");
        let script = directory.path().join("script");
        let invocation = directory.path().join("entrypoint");
        executable(&script, b"#!/bin/sh\nprintf first");
        std::os::unix::fs::symlink(&script, &invocation).expect("symlink");
        let prepared = prepare_launch(&native_launch(&invocation, directory.path()), limits())
            .expect("prepare native script");
        assert_eq!(
            prepared.command().program(),
            invocation.to_str().expect("path")
        );
        let handles = verify_prepared_launch(&prepared).expect("native handles");
        assert_eq!(
            Command::new(handles.program_path())
                .output()
                .expect("run")
                .stdout,
            b"first"
        );
        fs::rename(&script, directory.path().join("old-script")).expect("replace script");
        executable(&script, b"#!/bin/sh\nprintf updated");
        let restarted = verify_prepared_launch(&prepared).expect("native restart accepts update");
        assert_eq!(
            Command::new(restarted.program_path())
                .output()
                .expect("run update")
                .stdout,
            b"updated"
        );
        executable(&script, b"#!/bin/sh\nprintf privileged");
        fs::set_permissions(&script, fs::Permissions::from_mode(0o4700)).expect("setuid");
        assert_eq!(
            verify_prepared_launch(&prepared).err(),
            Some(LaunchPreparationError::ExecutablePrivileged)
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn launch_receipts_cannot_be_reinterpreted_in_another_mode() {
        let directory = tempdir().expect("directory");
        let program = directory.path().join("example-tool");
        copy_binary(Path::new("/bin/echo"), &program);
        for mode in [
            undercurrent_domain::LaunchMode::Native,
            undercurrent_domain::LaunchMode::Sealed,
        ] {
            let base = launch(&program, directory.path(), directory.path());
            let command = base.command().clone().with_launch_mode(mode);
            let prepared = prepare_launch(&base.with_command(command), limits()).expect("prepare");
            let other = match mode {
                undercurrent_domain::LaunchMode::Native => undercurrent_domain::LaunchMode::Sealed,
                undercurrent_domain::LaunchMode::Sealed => undercurrent_domain::LaunchMode::Native,
            };
            let changed = prepared.command().clone().with_launch_mode(other);
            assert_eq!(
                verify_prepared_launch(&prepared.with_command(changed)).err(),
                Some(LaunchPreparationError::StaleExecutableReceipt)
            );
        }
        let base = native_launch(&program, directory.path());
        let command = base
            .command()
            .clone()
            .with_expected_executable_digest(Some(PayloadDigest::of_bytes(b"pin")));
        assert_eq!(
            prepare_launch(&base.with_command(command), limits()).err(),
            Some(LaunchPreparationError::ExecutableDigestMismatch)
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn legacy_receipt_without_identities_fails_closed() {
        let directory = tempdir().expect("temporary directory");
        let workspace = directory.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace");
        let program = directory.path().join("example-tool");
        executable(&program, b"stable executable");
        let digest = PayloadDigest::of_bytes(b"stable executable");
        let command = CommandSpec::new(program.to_string_lossy(), Vec::new(), domain_limits())
            .expect("command")
            .pin_executable(
                program.to_string_lossy(),
                digest,
                PayloadDigest::of_bytes(b"legacy receipt"),
                domain_limits(),
            )
            .expect("legacy pin");
        let legacy = launch(&program, &workspace, &workspace).with_command(command);

        assert_eq!(
            verify_prepared_launch(&legacy).err(),
            Some(LaunchPreparationError::MissingExecutableIdentity)
        );
    }
    #[cfg(target_os = "macos")]
    #[test]
    fn macos_native_rejects_sealed_escape_and_replaced_directory() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("root");
        let cwd = root.join("child");
        fs::create_dir_all(&cwd).unwrap();
        let script = directory.path().join("script");
        executable(&script, b"#!/bin/sh\nprintf ok");
        let native = |cwd: &Path| {
            let spec = launch(&script, &root, cwd);
            let command = spec
                .command()
                .clone()
                .with_launch_mode(undercurrent_domain::LaunchMode::Native);
            spec.with_command(command)
        };
        assert_eq!(
            prepare_launch(&launch(&script, &root, &cwd), limits()).err(),
            Some(LaunchPreparationError::SealedLaunchUnsupported)
        );
        assert_eq!(
            prepare_launch(&native(directory.path()), limits()).err(),
            Some(LaunchPreparationError::WorkingDirectoryOutsideBinding)
        );
        let alias = root.join("alias");
        std::os::unix::fs::symlink(directory.path(), &alias).unwrap();
        assert!(prepare_launch(&native(&alias), limits()).is_err());
        let prepared = prepare_launch(&native(&cwd), limits()).unwrap();
        verify_prepared_launch(&prepared).unwrap();
        fs::rename(&cwd, root.join("old-child")).unwrap();
        fs::create_dir(&cwd).unwrap();
        assert_eq!(
            verify_prepared_launch(&prepared).err(),
            Some(LaunchPreparationError::StaleWorkingDirectoryIdentity)
        );
    }
}

impl LaunchPreparationError {
    /// Maps adapter detail into the stable application error taxonomy.
    pub(super) const fn port_kind(self) -> PortErrorKind {
        match self {
            #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
            Self::PlatformUnsupported => PortErrorKind::Unsupported,
            #[cfg(any(target_os = "macos", windows))]
            Self::SealedLaunchUnsupported => PortErrorKind::Unsupported,
            #[cfg(target_os = "linux")]
            Self::SecurePathResolutionUnavailable => PortErrorKind::Unsupported,
            #[cfg(target_os = "linux")]
            Self::ExecutableCapabilities | Self::ExecutableCapabilityInspection => {
                PortErrorKind::InvalidData
            }
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            Self::ImmutableExecutableUnavailable | Self::ProcessHandleFilesystemUnavailable => {
                PortErrorKind::Unavailable
            }
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            Self::ExecutablePrivileged | Self::ExecutableScriptUnsupported => {
                PortErrorKind::InvalidData
            }
            Self::Environment
            | Self::MissingEnvironmentBinding
            | Self::InvalidEnvironmentBinding
            | Self::WorkingDirectoryOutsideBinding
            | Self::WorkingDirectoryUnavailable
            | Self::MissingBindingRootIdentity
            | Self::MissingWorkingDirectoryIdentity
            | Self::StaleBindingRootIdentity
            | Self::StaleWorkingDirectoryIdentity
            | Self::ExecutableReference
            | Self::ExecutableUnavailable
            | Self::ExecutableNotRegular
            | Self::ExecutableNotRunnable
            | Self::ExecutableTooLarge
            | Self::ExecutableRead
            | Self::ExecutableDigestMismatch
            | Self::ExecutableIdentityUnavailable
            | Self::MissingExecutableReceipt
            | Self::MissingExecutableIdentity
            | Self::StaleExecutableReceipt
            | Self::Domain => PortErrorKind::InvalidData,
        }
    }
}

impl Display for LaunchPreparationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
            Self::PlatformUnsupported => {
                "secure native launch preparation is unsupported on this platform"
            }
            #[cfg(any(target_os = "macos", windows))]
            Self::SealedLaunchUnsupported => {
                if cfg!(windows) {
                    "sealed executable launches are unsupported on Windows; use native launch mode"
                } else {
                    "sealed executable launches are unsupported on macOS; use native launch mode"
                }
            }
            Self::Environment => "process environment could not be resolved safely",
            Self::MissingEnvironmentBinding => {
                "working directory requires a supplied environment binding"
            }
            Self::InvalidEnvironmentBinding => "environment binding root is invalid",
            #[cfg(target_os = "linux")]
            Self::SecurePathResolutionUnavailable => {
                "kernel-enforced beneath-root path resolution is unavailable"
            }
            Self::WorkingDirectoryOutsideBinding => {
                "working directory is outside the supplied environment binding"
            }
            Self::WorkingDirectoryUnavailable => {
                if cfg!(target_os = "macos") {
                    "working directory is unavailable; use an accessible directory within the workspace root without directory symlinks below that root"
                } else {
                    "working directory is unavailable"
                }
            },
            Self::MissingBindingRootIdentity => {
                "prepared environment binding lacks an object identity"
            }
            Self::MissingWorkingDirectoryIdentity => {
                "prepared working directory lacks an object identity"
            }
            Self::StaleBindingRootIdentity => {
                "environment binding root changed after launch acceptance"
            }
            Self::StaleWorkingDirectoryIdentity => {
                "working directory changed after launch acceptance"
            }
            Self::ExecutableReference => "executable reference is invalid or unsupported",
            Self::ExecutableUnavailable => "executable could not be resolved",
            Self::ExecutableNotRegular => "resolved executable is not a regular file",
            Self::ExecutableNotRunnable => "resolved executable is not runnable",
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            Self::ExecutablePrivileged => {
                "set-user-ID or set-group-ID executables are not permitted"
            }
            #[cfg(target_os = "linux")]
            Self::ExecutableCapabilities => "file-capability executables are not permitted",
            #[cfg(target_os = "linux")]
            Self::ExecutableCapabilityInspection => {
                "executable file capabilities could not be inspected safely"
            }
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            Self::ExecutableScriptUnsupported => {
                "script executables require an explicitly pinned interpreter contract"
            }
            Self::ExecutableTooLarge => "resolved executable exceeds the verification limit",
            Self::ExecutableRead => "resolved executable could not be read for verification",
            Self::ExecutableDigestMismatch => {
                "resolved executable does not match its supplied content pin"
            }
            Self::ExecutableIdentityUnavailable => {
                "resolved executable object identity could not be established"
            }
            Self::MissingExecutableReceipt => {
                "process launch lacks a pre-acceptance executable resolution receipt"
            }
            Self::MissingExecutableIdentity => {
                "prepared executable lacks a filesystem object identity"
            }
            Self::StaleExecutableReceipt => {
                "executable changed after its pre-acceptance resolution receipt"
            }
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            Self::ImmutableExecutableUnavailable => {
                "verified executable could not be staged as an immutable object"
            }
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            Self::ProcessHandleFilesystemUnavailable => {
                "parent process object handles are unavailable to child launch"
            }
            Self::Domain => "prepared launch violates the bounded domain contract",
        })
    }
}

impl Error for LaunchPreparationError {}

/// Selected executable path and exact directory handles retained for a managed
/// process lifetime. Sealed launches additionally retain immutable executable bytes.
pub(super) struct PreparedLaunchHandles {
    program_path: PathBuf,
    working_directory_path: Option<PathBuf>,
    #[cfg(windows)]
    _windows_paths: Vec<undercurrent_platform_windows::PinnedPath>,
    #[cfg(windows)]
    windows_batch_script: Option<PathBuf>,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    _executable: Option<Arc<File>>,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    _binding_root: Option<Arc<File>>,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    _working_directory: Option<Arc<File>>,
}

/// Runtime-local weak registry allowing processes with identical verified
/// executable bytes to retain one shared sealed object instead of consuming a
/// distinct descriptor per process.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(super) struct SealedExecutableCache {
    // macOS shares the verification interface but rejects sealed launches, so
    // only Linux needs storage for sealed executable objects.
    #[cfg(target_os = "linux")]
    entries: Mutex<BTreeMap<PayloadDigest, Weak<File>>>,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl SealedExecutableCache {
    pub(super) fn new() -> Self {
        Self {
            #[cfg(target_os = "linux")]
            entries: Mutex::new(BTreeMap::new()),
        }
    }

    #[cfg(target_os = "linux")]
    fn retain(&self, digest: PayloadDigest, executable: File) -> Arc<File> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        entries.retain(|_, executable| executable.strong_count() != 0);
        if let Some(retained) = entries.get(&digest).and_then(Weak::upgrade) {
            return retained;
        }
        let retained = Arc::new(executable);
        entries.insert(digest, Arc::downgrade(&retained));
        retained
    }
}

/// Runtime-local weak registry for verified binding-root and working-directory
/// capabilities. Sharing the same open directory object keeps descriptor use
/// proportional to distinct live filesystem objects rather than process count.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(super) struct PinnedDirectoryCache {
    entries: Mutex<BTreeMap<PayloadDigest, Weak<File>>>,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl PinnedDirectoryCache {
    pub(super) fn new() -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
        }
    }

    fn retain(&self, identity: PayloadDigest, directory: File) -> Arc<File> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        entries.retain(|_, directory| directory.strong_count() != 0);
        if let Some(retained) = entries.get(&identity).and_then(Weak::upgrade) {
            return retained;
        }
        let retained = Arc::new(directory);
        entries.insert(identity, Arc::downgrade(&retained));
        retained
    }
}

impl PreparedLaunchHandles {
    #[cfg(windows)]
    pub(super) fn windows_batch_script(&self) -> Option<&std::ffi::OsStr> {
        self.windows_batch_script
            .as_ref()
            .map(|path| path.as_os_str())
    }
    /// Returns the native invocation path or sealed parent-host executable reference.
    pub(super) fn program_path(&self) -> &Path {
        &self.program_path
    }

    /// Returns the parent-host exact-directory handle reference when one was
    /// configured.
    #[cfg(target_os = "macos")]
    pub(super) fn working_directory_handle(&self) -> Option<Arc<File>> {
        self._working_directory.clone()
    }

    pub(super) fn working_directory_path(&self) -> Option<&Path> {
        self.working_directory_path.as_deref()
    }

    #[cfg(all(test, target_os = "linux"))]
    fn binding_root_is_working_directory(&self) -> bool {
        match (&self._binding_root, &self._working_directory) {
            (Some(root), Some(directory)) => Arc::ptr_eq(root, directory),
            _ => false,
        }
    }
}

/// Resolves launch inputs and records mode-bound acceptance evidence before persistence.
pub(super) fn prepare_launch(
    launch: &LaunchSpec,
    runtime_limits: NativeProcessRuntimeLimits,
) -> Result<LaunchSpec, LaunchPreparationError> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        native::prepare_launch(launch, runtime_limits)
    }
    #[cfg(windows)]
    {
        windows::prepare_launch(launch, runtime_limits)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    {
        let _ = (launch, runtime_limits);
        Err(LaunchPreparationError::PlatformUnsupported)
    }
}

/// Revalidates prepared directories and applies the saved executable launch mode
/// immediately before process creation.
#[cfg(any(test, not(any(target_os = "linux", target_os = "macos"))))]
pub(super) fn verify_prepared_launch(
    launch: &LaunchSpec,
) -> Result<PreparedLaunchHandles, LaunchPreparationError> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        native::verify_prepared_launch(
            launch,
            &SealedExecutableCache::new(),
            &PinnedDirectoryCache::new(),
        )
    }
    #[cfg(windows)]
    {
        windows::verify_prepared_launch(launch)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    {
        let _ = launch;
        Err(LaunchPreparationError::PlatformUnsupported)
    }
}

/// Spawn-path verification using the runtime's descriptor-bounded sealed
/// executable registry.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(super) fn verify_prepared_launch_cached(
    launch: &LaunchSpec,
    executable_cache: &SealedExecutableCache,
    directory_cache: &PinnedDirectoryCache,
) -> Result<PreparedLaunchHandles, LaunchPreparationError> {
    native::verify_prepared_launch(launch, executable_cache, directory_cache)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod native {
    use std::collections::BTreeMap;
    use std::env;
    use std::ffi::OsString;
    use std::fs::{self, File};
    use std::io::Seek;
    use std::io::{Read, Write};
    #[cfg(target_os = "linux")]
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::PermissionsExt as _;
    use std::path::{Component, Path, PathBuf};

    #[cfg(target_os = "linux")]
    use rustix::fs::{
        AtFlags, MemfdFlags, ResolveFlags, SealFlags, StatxFlags, fchmod, fcntl_add_seals,
        fcntl_get_seals, fgetxattr, memfd_create, openat2, statx,
    };
    use rustix::fs::{Mode, OFlags, open};
    #[cfg(target_os = "linux")]
    use rustix::io::Errno;
    use sha2::{Digest as _, Sha256};
    use undercurrent_domain::{
        EnvironmentVariables, ExecutionEnvironmentBinding, ExecutionLimits, LaunchSpec,
        PathReference, PayloadDigest,
    };

    use super::{
        LaunchPreparationError, NativeProcessRuntimeLimits, PinnedDirectoryCache,
        PreparedLaunchHandles, SealedExecutableCache,
    };
    use crate::process::environment::load_environment;

    const MAX_RESOLVED_PATH_BYTES: usize = 4_096;
    const MAX_EXECUTABLE_BYTES: u64 = 512 * 1024 * 1024;
    const HASH_BUFFER_BYTES: usize = 64 * 1024;
    const RECEIPT_DOMAIN: &[u8] = b"undercurrent.executable-resolution.v0.1.0-beta\0";
    const EXECUTABLE_IDENTITY_DOMAIN: &[u8] = b"undercurrent.fs-object.executable.v0.1.0-beta\0";
    const DIRECTORY_IDENTITY_DOMAIN: &[u8] = b"undercurrent.fs-object.directory.v0.1.0-beta\0";
    #[cfg(target_os = "linux")]
    const REQUIRED_SEALS: SealFlags = SealFlags::WRITE
        .union(SealFlags::GROW)
        .union(SealFlags::SHRINK)
        .union(SealFlags::SEAL);

    struct ResolvedWorkingDirectory {
        working_directory: Option<PathReference>,
        working_directory_identity: Option<PayloadDigest>,
        environment_binding: Option<ExecutionEnvironmentBinding>,
    }

    struct OpenedWorkingDirectory {
        binding_root: Option<File>,
        working_directory: Option<File>,
    }

    struct ResolvedExecutable {
        path: String,
        content_digest: PayloadDigest,
        object_identity: PayloadDigest,
    }

    pub(super) fn prepare_launch(
        launch: &LaunchSpec,
        runtime_limits: NativeProcessRuntimeLimits,
    ) -> Result<LaunchSpec, LaunchPreparationError> {
        #[cfg(target_os = "macos")]
        if launch.command().launch_mode() != undercurrent_domain::LaunchMode::Native {
            return Err(LaunchPreparationError::SealedLaunchUnsupported);
        }
        if launch.command().launch_mode() == undercurrent_domain::LaunchMode::Native
            && launch.command().expected_executable_digest().is_some()
        {
            return Err(LaunchPreparationError::ExecutableDigestMismatch);
        }
        let merged_environment = load_environment(launch, runtime_limits)
            .map_err(|_| LaunchPreparationError::Environment)?;
        let resolved_directory = resolve_working_directory(launch)?;
        let resolved_program = resolve_executable(
            launch.command().program(),
            resolved_directory.working_directory.as_ref(),
            &merged_environment,
            launch.command().launch_mode(),
        )?;
        if launch
            .command()
            .expected_executable_digest()
            .is_some_and(|expected| expected != resolved_program.content_digest)
        {
            return Err(LaunchPreparationError::ExecutableDigestMismatch);
        }
        let binding_digest = resolved_directory
            .environment_binding
            .as_ref()
            .map(ExecutionEnvironmentBinding::digest);
        let root_identity = resolved_directory
            .environment_binding
            .as_ref()
            .and_then(ExecutionEnvironmentBinding::root_identity);
        let receipt_digest = resolution_receipt(
            launch.command().launch_mode(),
            launch.command().program(),
            &resolved_program.path,
            resolved_program.content_digest,
            resolved_program.object_identity,
            binding_digest,
            root_identity,
            resolved_directory.working_directory_identity,
        );
        let limits = execution_limits(
            launch,
            resolved_program.path.len(),
            &merged_environment,
            runtime_limits,
        )?;
        let command = launch
            .command()
            .clone()
            .pin_executable_with_identity(
                resolved_program.path,
                resolved_program.content_digest,
                resolved_program.object_identity,
                receipt_digest,
                limits,
            )
            .map_err(|_| LaunchPreparationError::Domain)?;
        let environment = EnvironmentVariables::new(merged_environment, limits)
            .map_err(|_| LaunchPreparationError::Domain)?;
        let launch = launch
            .clone()
            .with_command(command)
            .with_resolved_environment(environment, limits)
            .map_err(|_| LaunchPreparationError::Domain)?;
        Ok(launch
            .with_resolved_paths(
                resolved_directory.working_directory,
                resolved_directory.environment_binding,
            )
            .with_working_directory_identity(resolved_directory.working_directory_identity))
    }

    pub(super) fn verify_prepared_launch(
        launch: &LaunchSpec,
        executable_cache: &SealedExecutableCache,
        directory_cache: &PinnedDirectoryCache,
    ) -> Result<PreparedLaunchHandles, LaunchPreparationError> {
        let resolution = launch
            .command()
            .executable_resolution()
            .ok_or(LaunchPreparationError::MissingExecutableReceipt)?;
        let expected_executable_identity = resolution
            .object_identity()
            .ok_or(LaunchPreparationError::MissingExecutableIdentity)?;
        if launch.environment_file().is_some() {
            return Err(LaunchPreparationError::Environment);
        }
        let opened_directory = open_prepared_working_directory(launch)?;
        let mode = launch.command().launch_mode();
        if mode == undercurrent_domain::LaunchMode::Native
            && launch.command().expected_executable_digest().is_some()
        {
            return Err(LaunchPreparationError::ExecutableDigestMismatch);
        }
        let binding_digest = launch
            .environment_binding()
            .map(ExecutionEnvironmentBinding::digest);
        let root_identity = launch
            .environment_binding()
            .and_then(ExecutionEnvironmentBinding::root_identity);
        let expected_receipt = resolution_receipt(
            launch.command().launch_mode(),
            resolution.requested_reference(),
            launch.command().program(),
            resolution.content_digest(),
            expected_executable_identity,
            binding_digest,
            root_identity,
            launch.working_directory_identity(),
        );
        if expected_receipt != resolution.receipt_digest() {
            return Err(LaunchPreparationError::StaleExecutableReceipt);
        }
        let (program_path, staged_executable) = match mode {
            #[cfg(target_os = "macos")]
            undercurrent_domain::LaunchMode::Sealed => {
                return Err(LaunchPreparationError::SealedLaunchUnsupported);
            }
            #[cfg(target_os = "linux")]
            undercurrent_domain::LaunchMode::Sealed => {
                let staged = stage_prepared_executable(
                    Path::new(launch.command().program()),
                    resolution.content_digest(),
                    expected_executable_identity,
                )?;
                let retained = executable_cache.retain(resolution.content_digest(), staged);
                let path = parent_fd_path(retained.as_raw_fd());
                verify_proc_reference(&path)?;
                (path, Some(retained))
            }
            undercurrent_domain::LaunchMode::Native => {
                // Revalidate the saved invocation path, allowing an installed update.
                // Native execution deliberately does not claim race-free byte pinning.
                let path = Path::new(launch.command().program());
                if !path.is_absolute() {
                    return Err(LaunchPreparationError::StaleExecutableReceipt);
                }
                open_and_hash_executable(path, mode)?;
                (path.to_owned(), None)
            }
        };
        let binding_root = match opened_directory.binding_root {
            Some(root) => Some(directory_cache.retain(
                root_identity.ok_or(LaunchPreparationError::MissingBindingRootIdentity)?,
                root,
            )),
            None => None,
        };
        let working_directory = match opened_directory.working_directory {
            Some(directory) => Some(
                directory_cache.retain(
                    launch
                        .working_directory_identity()
                        .ok_or(LaunchPreparationError::MissingWorkingDirectoryIdentity)?,
                    directory,
                ),
            ),
            None => None,
        };
        #[cfg(target_os = "linux")]
        let working_directory_path = working_directory
            .as_ref()
            .map(|directory| parent_fd_path(directory.as_raw_fd()));
        #[cfg(target_os = "macos")]
        let working_directory_path = working_directory
            .as_ref()
            .map(|directory| {
                fd_path(
                    directory,
                    LaunchPreparationError::WorkingDirectoryUnavailable,
                )
            })
            .transpose()?;
        #[cfg(target_os = "macos")]
        let _ = executable_cache;
        if let Some(path) = &working_directory_path {
            verify_proc_reference(path)?;
        }
        Ok(PreparedLaunchHandles {
            program_path,
            working_directory_path,
            _executable: staged_executable,
            _binding_root: binding_root,
            _working_directory: working_directory,
        })
    }

    fn resolve_working_directory(
        launch: &LaunchSpec,
    ) -> Result<ResolvedWorkingDirectory, LaunchPreparationError> {
        let Some(binding) = launch.environment_binding() else {
            if launch.working_directory().is_some() {
                return Err(LaunchPreparationError::MissingEnvironmentBinding);
            }
            return Ok(ResolvedWorkingDirectory {
                working_directory: None,
                working_directory_identity: None,
                environment_binding: None,
            });
        };
        let requested_root = Path::new(binding.working_directory_root().as_str());
        if !requested_root.is_absolute() {
            return Err(LaunchPreparationError::InvalidEnvironmentBinding);
        }
        let root = open_directory_root(requested_root)?;
        let root_path = fd_path(&root, LaunchPreparationError::InvalidEnvironmentBinding)?;
        let root_text = path_text(
            &root_path,
            LaunchPreparationError::InvalidEnvironmentBinding,
        )?;
        let root_reference = PathReference::new(root_text, MAX_RESOLVED_PATH_BYTES)
            .map_err(|_| LaunchPreparationError::InvalidEnvironmentBinding)?;
        let root_identity = object_identity(
            &root,
            DIRECTORY_IDENTITY_DOMAIN,
            LaunchPreparationError::InvalidEnvironmentBinding,
        )?;
        let relative =
            requested_working_directory(launch.working_directory(), requested_root, &root_path)?;
        let working_directory = open_beneath(&root, &relative)?;
        let working_directory_path = fd_path(
            &working_directory,
            LaunchPreparationError::WorkingDirectoryUnavailable,
        )?;
        let working_directory_text = path_text(
            &working_directory_path,
            LaunchPreparationError::WorkingDirectoryUnavailable,
        )?;
        let working_directory_reference =
            PathReference::new(working_directory_text, MAX_RESOLVED_PATH_BYTES)
                .map_err(|_| LaunchPreparationError::WorkingDirectoryUnavailable)?;
        let working_directory_identity = object_identity(
            &working_directory,
            DIRECTORY_IDENTITY_DOMAIN,
            LaunchPreparationError::WorkingDirectoryUnavailable,
        )?;
        Ok(ResolvedWorkingDirectory {
            working_directory: Some(working_directory_reference),
            working_directory_identity: Some(working_directory_identity),
            environment_binding: Some(
                ExecutionEnvironmentBinding::new(binding.digest(), root_reference)
                    .with_root_identity(Some(root_identity)),
            ),
        })
    }

    fn open_prepared_working_directory(
        launch: &LaunchSpec,
    ) -> Result<OpenedWorkingDirectory, LaunchPreparationError> {
        let Some(binding) = launch.environment_binding() else {
            if launch.working_directory().is_some() || launch.working_directory_identity().is_some()
            {
                return Err(LaunchPreparationError::MissingEnvironmentBinding);
            }
            return Ok(OpenedWorkingDirectory {
                binding_root: None,
                working_directory: None,
            });
        };
        let expected_root_identity = binding
            .root_identity()
            .ok_or(LaunchPreparationError::MissingBindingRootIdentity)?;
        let expected_working_directory_identity = launch
            .working_directory_identity()
            .ok_or(LaunchPreparationError::MissingWorkingDirectoryIdentity)?;
        let requested_working_directory = launch
            .working_directory()
            .ok_or(LaunchPreparationError::MissingWorkingDirectoryIdentity)?;
        let root_path = Path::new(binding.working_directory_root().as_str());
        if !root_path.is_absolute() {
            return Err(LaunchPreparationError::InvalidEnvironmentBinding);
        }
        let root = open_directory_root(root_path)?;
        let root_identity = object_identity(
            &root,
            DIRECTORY_IDENTITY_DOMAIN,
            LaunchPreparationError::StaleBindingRootIdentity,
        )?;
        if root_identity != expected_root_identity {
            return Err(LaunchPreparationError::StaleBindingRootIdentity);
        }
        let relative =
            relative_under_root(Path::new(requested_working_directory.as_str()), root_path)?;
        let working_directory = open_beneath(&root, &relative)?;
        let working_directory_identity = object_identity(
            &working_directory,
            DIRECTORY_IDENTITY_DOMAIN,
            LaunchPreparationError::StaleWorkingDirectoryIdentity,
        )?;
        if working_directory_identity != expected_working_directory_identity {
            return Err(LaunchPreparationError::StaleWorkingDirectoryIdentity);
        }
        #[cfg(target_os = "linux")]
        let working_directory_path = parent_fd_path(working_directory.as_raw_fd());
        #[cfg(target_os = "macos")]
        let working_directory_path = fd_path(
            &working_directory,
            LaunchPreparationError::WorkingDirectoryUnavailable,
        )?;
        verify_proc_reference(&working_directory_path)?;
        Ok(OpenedWorkingDirectory {
            binding_root: Some(root),
            working_directory: Some(working_directory),
        })
    }

    #[cfg(target_os = "linux")]
    const DIRECTORY_ACCESS: OFlags = OFlags::PATH;
    #[cfg(target_os = "macos")]
    const DIRECTORY_ACCESS: OFlags = OFlags::RDONLY;

    fn open_directory_root(path: &Path) -> Result<File, LaunchPreparationError> {
        let fd = open(
            path,
            DIRECTORY_ACCESS | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| LaunchPreparationError::InvalidEnvironmentBinding)?;
        let file = File::from(fd);
        if !file
            .metadata()
            .map_err(|_| LaunchPreparationError::InvalidEnvironmentBinding)?
            .is_dir()
        {
            return Err(LaunchPreparationError::InvalidEnvironmentBinding);
        }
        Ok(file)
    }

    #[cfg(target_os = "linux")]
    fn open_beneath(root: &File, relative: &Path) -> Result<File, LaunchPreparationError> {
        openat2(
            root,
            relative,
            DIRECTORY_ACCESS | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
            ResolveFlags::BENEATH | ResolveFlags::NO_MAGICLINKS,
        )
        .map(File::from)
        .map_err(|error| {
            if error == Errno::NOSYS {
                LaunchPreparationError::SecurePathResolutionUnavailable
            } else if error == Errno::XDEV {
                LaunchPreparationError::WorkingDirectoryOutsideBinding
            } else {
                LaunchPreparationError::WorkingDirectoryUnavailable
            }
        })
    }

    #[cfg(target_os = "macos")]
    fn open_beneath(root: &File, relative: &Path) -> Result<File, LaunchPreparationError> {
        let relative = normalized_relative(relative)?;
        let mut directory = root
            .try_clone()
            .map_err(|_| LaunchPreparationError::WorkingDirectoryUnavailable)?;
        // Darwin has no openat2: walk each component with a retained directory
        // descriptor and reject symlinks and parent traversal.
        for component in relative.components() {
            if let Component::Normal(name) = component {
                directory = rustix::fs::openat(
                    &directory,
                    name,
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::empty(),
                )
                .map(File::from)
                .map_err(|_| LaunchPreparationError::WorkingDirectoryUnavailable)?;
            }
        }
        Ok(directory)
    }

    fn requested_working_directory(
        requested: Option<&PathReference>,
        requested_root: &Path,
        opened_root: &Path,
    ) -> Result<PathBuf, LaunchPreparationError> {
        let Some(requested) = requested else {
            return Ok(PathBuf::from("."));
        };
        let requested = Path::new(requested.as_str());
        if requested.is_absolute() {
            if let Ok(relative) = requested.strip_prefix(requested_root) {
                return normalized_relative(relative);
            }
            if let Ok(relative) = requested.strip_prefix(opened_root) {
                return normalized_relative(relative);
            }
            return Err(LaunchPreparationError::WorkingDirectoryOutsideBinding);
        }
        normalized_relative(requested)
    }

    fn relative_under_root(
        requested: &Path,
        root: &Path,
    ) -> Result<PathBuf, LaunchPreparationError> {
        if !requested.is_absolute() {
            return Err(LaunchPreparationError::WorkingDirectoryUnavailable);
        }
        let relative = requested
            .strip_prefix(root)
            .map_err(|_| LaunchPreparationError::WorkingDirectoryOutsideBinding)?;
        normalized_relative(relative)
    }

    fn normalized_relative(path: &Path) -> Result<PathBuf, LaunchPreparationError> {
        let mut normalized = PathBuf::new();
        for component in path.components() {
            match component {
                Component::Normal(value) => normalized.push(value),
                Component::CurDir => {}
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    return Err(LaunchPreparationError::WorkingDirectoryOutsideBinding);
                }
            }
        }
        if normalized.as_os_str().is_empty() {
            normalized.push(".");
        }
        Ok(normalized)
    }

    fn resolve_executable(
        reference: &str,
        working_directory: Option<&PathReference>,
        environment: &BTreeMap<String, String>,
        mode: undercurrent_domain::LaunchMode,
    ) -> Result<ResolvedExecutable, LaunchPreparationError> {
        if let Some(tool) = reference.strip_prefix("tool://") {
            let (name, version) = tool
                .split_once('/')
                .ok_or(LaunchPreparationError::ExecutableReference)?;
            if name.is_empty()
                || version.is_empty()
                || name.contains(['/', '\\'])
                || version.contains(['/', '\\'])
            {
                return Err(LaunchPreparationError::ExecutableReference);
            }
            return search_path(name, environment, mode);
        }
        if reference.contains("://") {
            return Err(LaunchPreparationError::ExecutableReference);
        }
        let path = PathBuf::from(reference);
        if path.is_absolute() {
            return open_and_hash_executable(&path, mode);
        }
        if path.components().count() > 1 {
            let root =
                working_directory.ok_or(LaunchPreparationError::MissingEnvironmentBinding)?;
            return open_and_hash_executable(&PathBuf::from(root.as_str()).join(path), mode);
        }
        search_path(reference, environment, mode)
    }

    fn search_path(
        program: &str,
        environment: &BTreeMap<String, String>,
        mode: undercurrent_domain::LaunchMode,
    ) -> Result<ResolvedExecutable, LaunchPreparationError> {
        if program.is_empty() || program.contains(['/', '\\']) {
            return Err(LaunchPreparationError::ExecutableReference);
        }
        let search = environment
            .get("PATH")
            .map(OsString::from)
            .or_else(|| env::var_os("PATH"))
            .ok_or(LaunchPreparationError::ExecutableUnavailable)?;
        for directory in env::split_paths(&search) {
            if !directory.is_absolute() {
                continue;
            }
            match open_and_hash_executable(&directory.join(program), mode) {
                Ok(executable) => return Ok(executable),
                Err(
                    LaunchPreparationError::ExecutableUnavailable
                    | LaunchPreparationError::ExecutableNotRegular
                    | LaunchPreparationError::ExecutableNotRunnable,
                ) => {}
                Err(error) => return Err(error),
            }
        }
        Err(LaunchPreparationError::ExecutableUnavailable)
    }

    fn open_and_hash_executable(
        path: &Path,
        mode: undercurrent_domain::LaunchMode,
    ) -> Result<ResolvedExecutable, LaunchPreparationError> {
        // A caller reference may itself be a symlink (for example `/bin/sh`).
        // Resolve that reference first, then require the object-bearing open to
        // use `O_NOFOLLOW`. Whatever object wins any resolution race is the
        // exact fd subsequently inspected, hashed, identified, and receipted.
        let resolved =
            fs::canonicalize(path).map_err(|_| LaunchPreparationError::ExecutableUnavailable)?;
        let mut file = open_executable(&resolved)?;
        let identity = validate_open_executable(&file)?;
        let digest = hash_executable_with_scripts(
            &mut file,
            None,
            mode == undercurrent_domain::LaunchMode::Native,
        )?;
        let after_identity = validate_open_executable(&file)?;
        if after_identity != identity {
            return Err(LaunchPreparationError::ExecutableRead);
        }
        let actual = fd_path(&file, LaunchPreparationError::ExecutableUnavailable)?;
        let selected = if mode == undercurrent_domain::LaunchMode::Native {
            path
        } else {
            &actual
        };
        let path = path_text(selected, LaunchPreparationError::ExecutableReference)?;
        if path.len() > MAX_RESOLVED_PATH_BYTES {
            return Err(LaunchPreparationError::ExecutableReference);
        }
        Ok(ResolvedExecutable {
            path: path.to_owned(),
            content_digest: digest,
            object_identity: identity,
        })
    }

    fn open_executable(path: &Path) -> Result<File, LaunchPreparationError> {
        open(
            path,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map(File::from)
        .map_err(|_| LaunchPreparationError::ExecutableUnavailable)
    }

    fn validate_open_executable(file: &File) -> Result<PayloadDigest, LaunchPreparationError> {
        let metadata = file
            .metadata()
            .map_err(|_| LaunchPreparationError::ExecutableUnavailable)?;
        if !metadata.is_file() {
            return Err(LaunchPreparationError::ExecutableNotRegular);
        }
        if metadata.len() > MAX_EXECUTABLE_BYTES {
            return Err(LaunchPreparationError::ExecutableTooLarge);
        }
        let mode = metadata.permissions().mode();
        if mode & 0o111 == 0 {
            return Err(LaunchPreparationError::ExecutableNotRunnable);
        }
        if mode & 0o6000 != 0 {
            return Err(LaunchPreparationError::ExecutablePrivileged);
        }
        #[cfg(target_os = "linux")]
        reject_file_capabilities(file)?;
        object_identity(
            file,
            EXECUTABLE_IDENTITY_DOMAIN,
            LaunchPreparationError::ExecutableIdentityUnavailable,
        )
    }

    #[cfg(target_os = "linux")]
    fn reject_file_capabilities(file: &File) -> Result<(), LaunchPreparationError> {
        let mut value: Vec<u8> = Vec::new();
        match fgetxattr(file, "security.capability", &mut value) {
            Ok(_) => Err(LaunchPreparationError::ExecutableCapabilities),
            Err(error) if error == Errno::NODATA => Ok(()),
            Err(error) if error == Errno::OPNOTSUPP || error == Errno::NOTSUP => Ok(()),
            Err(_) => Err(LaunchPreparationError::ExecutableCapabilityInspection),
        }
    }

    #[cfg(target_os = "linux")]
    fn hash_executable(
        source: &mut File,
        destination: Option<&mut File>,
    ) -> Result<PayloadDigest, LaunchPreparationError> {
        hash_executable_with_scripts(source, destination, false)
    }

    fn hash_executable_with_scripts(
        source: &mut File,
        mut destination: Option<&mut File>,
        allow_scripts: bool,
    ) -> Result<PayloadDigest, LaunchPreparationError> {
        source
            .rewind()
            .map_err(|_| LaunchPreparationError::ExecutableRead)?;
        let mut digest = Sha256::new();
        let mut buffer = vec![0_u8; HASH_BUFFER_BYTES];
        let mut total = 0_u64;
        let mut prefix = [0_u8; 2];
        let mut prefix_bytes = 0_usize;
        loop {
            let read = source
                .read(&mut buffer)
                .map_err(|_| LaunchPreparationError::ExecutableRead)?;
            if read == 0 {
                break;
            }
            total = total
                .checked_add(u64::try_from(read).unwrap_or(u64::MAX))
                .ok_or(LaunchPreparationError::ExecutableTooLarge)?;
            if total > MAX_EXECUTABLE_BYTES {
                return Err(LaunchPreparationError::ExecutableTooLarge);
            }
            if prefix_bytes < prefix.len() {
                let copy = (prefix.len() - prefix_bytes).min(read);
                prefix[prefix_bytes..prefix_bytes + copy].copy_from_slice(&buffer[..copy]);
                prefix_bytes += copy;
            }
            digest.update(&buffer[..read]);
            if let Some(destination) = destination.as_deref_mut() {
                destination
                    .write_all(&buffer[..read])
                    .map_err(|_| LaunchPreparationError::ImmutableExecutableUnavailable)?;
            }
        }
        if !allow_scripts && prefix_bytes == 2 && prefix == *b"#!" {
            return Err(LaunchPreparationError::ExecutableScriptUnsupported);
        }
        payload_digest(digest, LaunchPreparationError::Domain)
    }

    #[cfg(target_os = "linux")]
    fn stage_prepared_executable(
        path: &Path,
        expected_digest: PayloadDigest,
        expected_identity: PayloadDigest,
    ) -> Result<File, LaunchPreparationError> {
        if !path.is_absolute() {
            return Err(LaunchPreparationError::StaleExecutableReceipt);
        }
        let mut source =
            open_executable(path).map_err(|_| LaunchPreparationError::StaleExecutableReceipt)?;
        let identity = validate_open_executable(&source)
            .map_err(|_| LaunchPreparationError::StaleExecutableReceipt)?;
        if identity != expected_identity {
            return Err(LaunchPreparationError::StaleExecutableReceipt);
        }
        let writable_fd = match memfd_create(
            "undercurrent-executable",
            MemfdFlags::CLOEXEC | MemfdFlags::ALLOW_SEALING | MemfdFlags::EXEC,
        ) {
            Ok(fd) => fd,
            Err(error) if error == Errno::INVAL => memfd_create(
                "undercurrent-executable",
                MemfdFlags::CLOEXEC | MemfdFlags::ALLOW_SEALING,
            )
            .map_err(|_| LaunchPreparationError::ImmutableExecutableUnavailable)?,
            Err(_) => return Err(LaunchPreparationError::ImmutableExecutableUnavailable),
        };
        let mut writable = File::from(writable_fd);
        let digest = hash_executable(&mut source, Some(&mut writable))?;
        if digest != expected_digest {
            return Err(LaunchPreparationError::StaleExecutableReceipt);
        }
        let after_identity = validate_open_executable(&source)
            .map_err(|_| LaunchPreparationError::StaleExecutableReceipt)?;
        if after_identity != expected_identity {
            return Err(LaunchPreparationError::StaleExecutableReceipt);
        }
        writable
            .flush()
            .map_err(|_| LaunchPreparationError::ImmutableExecutableUnavailable)?;
        writable
            .rewind()
            .map_err(|_| LaunchPreparationError::ImmutableExecutableUnavailable)?;
        fchmod(&writable, Mode::from_raw_mode(0o500))
            .map_err(|_| LaunchPreparationError::ImmutableExecutableUnavailable)?;
        fcntl_add_seals(&writable, REQUIRED_SEALS)
            .map_err(|_| LaunchPreparationError::ImmutableExecutableUnavailable)?;
        let seals = fcntl_get_seals(&writable)
            .map_err(|_| LaunchPreparationError::ImmutableExecutableUnavailable)?;
        if !seals.contains(REQUIRED_SEALS) {
            return Err(LaunchPreparationError::ImmutableExecutableUnavailable);
        }
        let writable_path = self_fd_path(writable.as_raw_fd());
        let mut retained = File::open(&writable_path)
            .map_err(|_| LaunchPreparationError::ProcessHandleFilesystemUnavailable)?;
        drop(writable);
        let retained_seals = fcntl_get_seals(&retained)
            .map_err(|_| LaunchPreparationError::ImmutableExecutableUnavailable)?;
        if !retained_seals.contains(REQUIRED_SEALS) {
            return Err(LaunchPreparationError::ImmutableExecutableUnavailable);
        }
        // A same-identity peer may be able to inspect this process's fd table.
        // Rehash only after the write-preventing seals are installed so the
        // executable bytes proven here are exactly the immutable bytes later
        // reached through the retained parent-host fd reference.
        let sealed_digest = hash_executable(&mut retained, None)
            .map_err(|_| LaunchPreparationError::ImmutableExecutableUnavailable)?;
        if sealed_digest != expected_digest {
            return Err(LaunchPreparationError::ImmutableExecutableUnavailable);
        }
        Ok(retained)
    }

    #[cfg(target_os = "linux")]
    fn object_identity(
        file: &File,
        domain: &[u8],
        failure: LaunchPreparationError,
    ) -> Result<PayloadDigest, LaunchPreparationError> {
        let stat = statx(
            file,
            "",
            AtFlags::EMPTY_PATH,
            StatxFlags::BASIC_STATS | StatxFlags::BTIME,
        )
        .map_err(|_| failure)?;
        let returned = StatxFlags::from_bits_retain(stat.stx_mask);
        let required = StatxFlags::TYPE
            | StatxFlags::MODE
            | StatxFlags::UID
            | StatxFlags::GID
            | StatxFlags::INO;
        if !returned.contains(required) {
            return Err(failure);
        }
        let mut digest = Sha256::new();
        digest.update(domain);
        digest.update(stat.stx_dev_major.to_le_bytes());
        digest.update(stat.stx_dev_minor.to_le_bytes());
        digest.update(stat.stx_ino.to_le_bytes());
        digest.update(stat.stx_mode.to_le_bytes());
        digest.update(stat.stx_uid.to_le_bytes());
        digest.update(stat.stx_gid.to_le_bytes());
        digest.update(stat.stx_rdev_major.to_le_bytes());
        digest.update(stat.stx_rdev_minor.to_le_bytes());
        digest.update([u8::from(returned.contains(StatxFlags::BTIME))]);
        if returned.contains(StatxFlags::BTIME) {
            digest.update(stat.stx_btime.tv_sec.to_le_bytes());
            digest.update(stat.stx_btime.tv_nsec.to_le_bytes());
        }
        payload_digest(digest, failure)
    }

    #[cfg(target_os = "macos")]
    fn object_identity(
        file: &File,
        domain: &[u8],
        failure: LaunchPreparationError,
    ) -> Result<PayloadDigest, LaunchPreparationError> {
        use std::os::macos::fs::MetadataExt as _;
        let metadata = file.metadata().map_err(|_| failure)?;
        let mut digest = Sha256::new();
        digest.update(domain);
        digest.update(metadata.st_dev().to_le_bytes());
        digest.update(metadata.st_ino().to_le_bytes());
        digest.update(metadata.st_mode().to_le_bytes());
        digest.update(metadata.st_uid().to_le_bytes());
        digest.update(metadata.st_gid().to_le_bytes());
        digest.update(metadata.st_birthtime().to_le_bytes());
        digest.update(metadata.st_birthtime_nsec().to_le_bytes());
        payload_digest(digest, failure)
    }

    #[allow(clippy::too_many_arguments)]
    fn resolution_receipt(
        mode: undercurrent_domain::LaunchMode,
        requested_reference: &str,
        resolved_program: &str,
        content_digest: PayloadDigest,
        executable_identity: PayloadDigest,
        environment_binding_digest: Option<PayloadDigest>,
        binding_root_identity: Option<PayloadDigest>,
        working_directory_identity: Option<PayloadDigest>,
    ) -> PayloadDigest {
        let mut digest = Sha256::new();
        digest.update(RECEIPT_DOMAIN);
        // Keep legacy sealed receipts byte-for-byte stable; bind native explicitly.
        if mode == undercurrent_domain::LaunchMode::Native {
            update_field(&mut digest, b"native.v0.1.0-beta");
        }
        update_field(&mut digest, requested_reference.as_bytes());
        update_field(&mut digest, resolved_program.as_bytes());
        update_field(&mut digest, content_digest.as_bytes());
        update_field(&mut digest, executable_identity.as_bytes());
        update_optional_digest(&mut digest, environment_binding_digest);
        update_optional_digest(&mut digest, binding_root_identity);
        update_optional_digest(&mut digest, working_directory_identity);
        payload_digest(digest, LaunchPreparationError::Domain)
            .expect("SHA-256 output is always a valid payload digest")
    }

    fn update_field(digest: &mut Sha256, value: &[u8]) {
        digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_le_bytes());
        digest.update(value);
    }

    fn update_optional_digest(digest: &mut Sha256, value: Option<PayloadDigest>) {
        match value {
            Some(value) => {
                digest.update([1]);
                digest.update(value.as_bytes());
            }
            None => digest.update([0]),
        }
    }

    fn execution_limits(
        launch: &LaunchSpec,
        resolved_program_bytes: usize,
        environment: &BTreeMap<String, String>,
        runtime_limits: NativeProcessRuntimeLimits,
    ) -> Result<ExecutionLimits, LaunchPreparationError> {
        if resolved_program_bytes > MAX_RESOLVED_PATH_BYTES {
            return Err(LaunchPreparationError::ExecutableReference);
        }
        let command_bytes = launch
            .command()
            .arguments()
            .iter()
            .try_fold(resolved_program_bytes, |total, argument| {
                total.checked_add(argument.len())
            })
            .ok_or(LaunchPreparationError::Domain)?;
        let environment_bytes = environment.iter().try_fold(0_usize, |total, (key, value)| {
            total.checked_add(key.len())?.checked_add(value.len())
        });
        if environment.len() > runtime_limits.environment_entries
            || environment_bytes.is_none_or(|bytes| bytes > runtime_limits.environment_bytes)
        {
            return Err(LaunchPreparationError::Environment);
        }
        Ok(ExecutionLimits {
            max_arguments: launch.command().arguments().len().max(1),
            max_command_bytes: command_bytes.max(1),
            max_environment_entries: runtime_limits.environment_entries,
            max_environment_bytes: runtime_limits.environment_bytes,
            max_path_bytes: MAX_RESOLVED_PATH_BYTES,
            max_health_rules: 1,
            max_health_rule_bytes: 1,
        })
    }

    fn fd_path(
        file: &File,
        failure: LaunchPreparationError,
    ) -> Result<PathBuf, LaunchPreparationError> {
        #[cfg(target_os = "linux")]
        let path = fs::read_link(self_fd_path(file.as_raw_fd())).map_err(|_| failure)?;
        #[cfg(target_os = "macos")]
        let path = {
            let mut path = PathBuf::new();
            nix::fcntl::fcntl(file, nix::fcntl::FcntlArg::F_GETPATH(&mut path))
                .map_err(|_| failure)?;
            // Revalidate the path returned by the kernel before native spawn.
            use std::os::unix::fs::MetadataExt as _;
            let actual = fs::metadata(&path).map_err(|_| failure)?;
            let expected = file.metadata().map_err(|_| failure)?;
            if actual.dev() != expected.dev() || actual.ino() != expected.ino() {
                return Err(failure);
            }
            path
        };
        if !path.is_absolute() || path.to_string_lossy().ends_with(" (deleted)") {
            return Err(failure);
        }
        Ok(path)
    }

    #[cfg(target_os = "linux")]
    fn self_fd_path(fd: i32) -> PathBuf {
        PathBuf::from(format!("/proc/self/fd/{fd}"))
    }

    #[cfg(target_os = "linux")]
    fn parent_fd_path(fd: i32) -> PathBuf {
        PathBuf::from(format!("/proc/{}/fd/{fd}", std::process::id()))
    }

    fn verify_proc_reference(path: &Path) -> Result<(), LaunchPreparationError> {
        fs::metadata(path)
            .map(|_| ())
            .map_err(|_| LaunchPreparationError::ProcessHandleFilesystemUnavailable)
    }

    fn payload_digest(
        digest: Sha256,
        failure: LaunchPreparationError,
    ) -> Result<PayloadDigest, LaunchPreparationError> {
        PayloadDigest::from_hex(&format!("{:x}", digest.finalize())).map_err(|_| failure)
    }

    fn path_text(
        path: &Path,
        error: LaunchPreparationError,
    ) -> Result<&str, LaunchPreparationError> {
        path.to_str().ok_or(error)
    }
}
