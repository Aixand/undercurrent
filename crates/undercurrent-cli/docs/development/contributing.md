# Make a change and choose verification

Run commands from the repository root with the pinned toolchain and dependencies
available; see [development setup](../../../../docs/development/setup.md). Use the [source map](../internals/code-map.md) to locate the affected owner.

## Choose the affected contracts

| Change | Update and verify |
| --- | --- |
| Command or option | Clap grammar, command registry, output/TTY policy, preparation, native/stream dispatch, help examples, and user reference; test routing and invalid-input boundary |
| New product behavior | Add/change the owning protocol and daemon contract first; keep CLI requests typed, preserve exact name-to-ID selection, and document acceptance versus completion |
| Bulk operation | Preserve full selection before mutation, cursor/duplicate/capacity guards, shared deadline, bounded worker count, and deterministic reports for partial success |
| Stream or display | Test seed/live shapes, overlap/gaps, same-ID recovery, writer failure, cancellation, terminal restoration, and bounded query membership |
| Interactive change | Exercise resize, narrow displays, Unicode cells, copy/history, read-only input, local focus, and child escape confinement |
| Group mutation | Test durable pending intent before send, response loss, known-absent rejection, concurrent metadata changes, restoration classification, and deletion resumption |
| Lifecycle or reset | Use injected launch/connection/filesystem seams; preserve private-path identity and ownership checks; review Unix and Windows paths separately |
| Shell hook | Keep one embedded document per shell; check idempotent install, existing bytes/modes, status preservation, optional interpreter skips, and native PowerShell |
| Rust docs or examples | Run warning-denied Rustdoc and applicable doctests through the existing validation workflow |

## Run focused checks

Choose the commands relevant to your change in a prepared development environment:

```sh
cargo test --locked -p undercurrent-cli
cargo test --locked -p undercurrent-cli --doc
RUSTDOCFLAGS=-Dwarnings cargo doc --locked -p undercurrent-cli --no-deps
```

The repository integration workflow is
[scripts/validate.py](../../../../scripts/validate.py).
Use its `--stage rustdoc` for the warning-denied workspace Rustdoc check and
the affected architecture, formatting, tests, and platform stages during
integration. The runner is offline by default, so fetch the locked dependencies
through the project's normal setup before using it. Scope a local check to the
change; do not claim a focused check covers native platforms it did not run.

## Choose integration evidence

| Evidence | What it exercises |
| --- | --- |
| Inline tests in executor, preparation, streams, and presentation | DTO preparation, deadlines, batch selection, wait conditions, cancellation, byte handling, and human/JSON formatting |
| [machine_output_golden](../../tests/machine_output_golden.rs) | Presentation/failure taxonomy fixtures; generic fixtures are not every command's actual DTO schema |
| [automatic_daemon_startup](../../tests/automatic_daemon_startup.rs) | Unix fake launcher, eligible routes, local-only routes, and owned-but-unreachable protection |
| [explicit_daemon_lifecycle](../../tests/explicit_daemon_lifecycle.rs) | Process-launch source boundary; it does not forbid registered automatic startup |
| [config_yaml](../../tests/config_yaml.rs) | Configuration validation without daemon state creation |
| [session_context](../../tests/session_context.rs), [PowerShell](../../tests/powershell_integration.rs), [Windows home](../../tests/windows_home.rs) | Local context behavior, optional shell fixtures, native profile/hook behavior, and USERPROFILE resolution |
| Attach/group/renderer inline tests | Input order, stream cleanup, copy confinement, bounds, metadata preservation, and restoration decisions |
| [attach modes](../../../../tests/e2e/cli/attach-modes.py), [attach/stop pickers](../../../../tests/e2e/cli/attach-picker.py) | Real POSIX PTY behavior with isolated homes and built debug binaries; stop cancellation, stable IDs, force, and pending restarts |
| [workspace](../../../../tests/e2e/cli/workspace.py) | Real POSIX PTY attach/return, process survival, read-only previews, creation, groups, labels, confirmation, resize, and signal cleanup |
| [group persistence](../../../../tests/e2e/cli/group-persistence.py) | Linux-specific process/daemon failure, directory retention, stable slots, intentional closure, and resumable deletion |
| [CLI smoke](../../../../tests/e2e/cli/smoke.py), [execution environment smoke](../../../../tests/e2e/cli/exec-environment.py) | Real daemon/product flows and live execution context |
| [Windows job in Native platform CI](../../../../.github/workflows/platform-contracts.yml), [Windows attach smoke](../../../../tests/e2e/platform/windows-attach-smoke.py) | Windows native platform evidence |

Real-daemon tests require their documented binaries, interpreters, platform,
and isolated state. Some shell integration tests skip an unavailable interpreter;
read the output before claiming that shell was verified. Use [isolated runtime state](../../../../docs/development/setup.md#use-isolated-runtime-state)
for lifecycle checks, and follow each test's cleanup procedure. Record the
commands actually run and any platform or interpreter coverage still missing.

[CLI maintainer index](../README.md)
