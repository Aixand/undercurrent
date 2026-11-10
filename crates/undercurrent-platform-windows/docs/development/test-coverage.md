# Find native test coverage and its limits

Use this reference when choosing regression tests or assessing what a passing
check proves. The tests are beside the owning source linked below. They run
only on Windows; use the [native verification procedure](contributing.md#choose-checks-for-a-change)
for commands and prerequisites. They exercise temporary state, local pipes,
loopback sockets, and test-owned children without a daemon.

## Pipe and console workers

### [Private pipes](../../src/pipe.rs)

`kernel_pipe_has_protected_single_account_acl_and_noninheritable_handle`: descriptor protection, one access-control entry (ACE), and handle inheritance flag. Does not launch a second-account client.

### [Pipe account identification](../../src/security.rs)

`pipe_identification_restores_empty_and_existing_thread_tokens` compares the
kernel token ID before and after successful identification, including nested
file-owner scopes. `anonymous_identification_fails_without_leaking_a_thread_token`
checks both thread contexts after anonymous rejection.
`unconnected_identification_fails_and_restores_the_thread_context` checks a
disconnected handle. These tests do not force token-restoration failure/abort
or process access denial across Windows logon contexts.

The opt-in [peer-context qualification](contributing.md#qualify-peer-logon-contexts)
uses real desktop and SSH processes with the production pipe implementation.
It additionally checks CLI and Python-bridge queries against a regular desktop
daemon, with a real test session. This requires an elevated SSH coordinator and
an interactive desktop login; ordinary headless CI cannot supply that evidence.

### [Blocking pipes](../../src/blocking_pipe.rs)

`nonblocking_accept_and_kernel_peer_identity`: initial and successor `WouldBlock`, connected peer PID. `private_pipe_timeout_recovery_and_clone_shutdown`: timeout reuse, wakeup, and EOF.

### [Console](../../src/console.rs)

`closing_registration_wakes_idle_worker_and_is_terminal`: close wakes a small-stack worker and repeated `next` ends. Does not synthesize real Ctrl-C/Ctrl-Break events.

## Files, paths, and account ownership

### [Private files and pins](../../src/filesystem.rs)

`restricting_a_directory_does_not_rewrite_existing_descendant_acls`, `junctions_are_rejected_without_mutating_the_target`, and `private_files_are_readable_after_reopen_but_hardlinks_are_rejected`: permission and alias boundaries.

### [Launch paths](../../src/filesystem.rs)

`pinned_ancestors_cannot_be_renamed_and_identity_survives_reopen`, `launch_names_preserve_case_and_reject_aliases`, `launch_leaf_blocks_content_replacement_and_ancestor_rename_ex`: identity and rename protection.

### [State lifecycle](../../src/filesystem.rs)

`exclusive_staging_replaces_an_existing_private_record`, `private_locks_share_one_object_and_release_with_the_owner`, `removal_pins_the_exact_file_and_rejects_replacement_and_hardlinks`: staged publication, contention, and exact deletion. Independent pins and traversal/device-path rejection have separate tests.

### [Default file owner](../../src/file_owner.rs)

`library_created_files_have_exact_user_ownership`, `nested_callbacks_and_unwinding_restore_the_thread_token`: file compatibility, nested restoration, panic unwind, thread isolation. Does not force token-restoration failure/abort.

## Processes and observations

### [Process identity](../../src/process.rs)

`identity_is_stable_and_retained_handle_observes_exit`, `invalid_pid_fails_closed`: identity, liveness, parent, image, owner, and rejected IDs. Does not deterministically force PID reuse.

### [Jobs](../../src/job.rs)

`piped_command_runs_and_drop_terminates_retained_process`, `command_exit_status_is_preserved`, `observer_retains_exact_job_members_and_detects_tree_changes`: launch, root status, containment cleanup, membership change.

### [Job observer](../../src/job_observation.rs)

`a_non_job_handle_cannot_produce_membership_evidence`: delayed kernel validation rejects a file handle.

### [TCP tables](../../src/listeners.rs)

Layout matching and timestamp tests, `growth_errors_and_row_limits_never_claim_absence`, `incomplete_families_preserve_positive_evidence_but_not_negative`, real IPv4/IPv6 ownership, and exited/unrelated process tests.

## Limits and caller checks

These tests cover specific schedules and fixtures. They do not prove power-loss
durability, every possible race, or cross-account resistance. A successful
non-Windows build removes the implementation and its unit tests via
`#![cfg(windows)]`.

For changes that affect session outcomes, output, storage, or daemon ownership,
run the relevant [caller checks](contributing.md#check-the-affected-caller).
Full platform qualification uses the [native workflow and smoke checks](contributing.md#qualify-the-platform).

[Crate documentation](../README.md)
