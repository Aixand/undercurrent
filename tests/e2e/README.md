# Run a live product regression

Build the needed binaries first. Run from the repository root:

```sh
python3 tests/e2e/run.py --list
python3 tests/e2e/run.py --check
python3 tests/e2e/run.py --test attach-picker --dry-run
python3 tests/e2e/run.py --test attach-picker --bin-dir target/debug
python3 tests/e2e/run.py --group lifecycle --bin-dir target/debug
```

`--list --json` exposes the catalog for tooling. `--check` verifies every E2E
script has an entry and every entry points to an existing script. `--dry-run`
prints argument arrays without executing commands or creating report folders.
Use repeated `--test NAME` options for a selected sequence.

| Group | Coverage |
| --- | --- |
| `cli` | Attach, groups, input limits, support collection and live environment. |
| `lifecycle` | Recovery, fast stop/restart and reset/shutdown. |
| `configuration` | Home layout and isolation, IPC settings, launch modes and profiles. |
| `platform` | Shared Linux/macOS contracts and detection, Windows native/ConPTY behavior. |

The runner checks the host platform, command prerequisites and built binaries
before executing any selected test. It stops on the first failure and records
only executed tests under a fresh `target/e2e/run-*/report.json`. A passed subset
is not evidence that every E2E test or platform passed. Individual tests own
their bounded operations and isolated-state cleanup.

The catalog records `ci`, `standalone`, `manual`, and supporting `helper` status.
CI and standalone invoke their own explicitly selected checks. Feature groups
include manual tests supported by the current host and exclude supporting helpers.

The live-PID listener assertion is a release-bundle helper, not a test that the
runner can launch on its own. Its catalog entry names that prerequisite.

The Windows peer-context qualification also requires an external session setup.
Run it manually from elevated PowerShell over SSH as the user signed in to the
desktop. Build matching native binaries, including `uc-python-bridge`, and the
[`pipe_peer_probe` example](../../crates/undercurrent-platform-windows/examples/pipe_peer_probe.rs)
from `undercurrent-platform-windows` first. From the
repository root, with an unused evidence directory:

```powershell
python tests/e2e/platform/windows-peer-contexts.py `
  --probe target/debug/examples/pipe_peer_probe.exe `
  --uc target/debug/uc.exe --work C:\Temp\uc-peer-context-proof
```

The script creates only test-owned tasks, processes, and runtime homes. Success
prints a report with `passed: true`; evidence stays under the selected directory.
Inspect `report.json`, `daemonCleanup`, and `taskCleanupErrors` if it fails.
Retain uncertain state until owned tasks and processes are confirmed stopped.
The generic runner lists this probe as a helper and excludes it from group runs.

Shared interactive tests import `Terminal` from `_support/terminal.py`.
`UC_E2E_BIN_DIR` is a test-harness override used by the runner for scripts whose
default is `target/debug`; product child environments still use isolated homes.
