# Verify a runtime documentation or adapter change

Use this guide to choose focused checks and inspect failures without attaching
to a user's daemon. Run commands from the **repository root**, which contains
the workspace `Cargo.toml` and `scripts/`. The pinned toolchain is Rust 1.95.0
with Rustfmt and Clippy; dependencies must already be available or obtainable
through the repository's normal setup. Python 3.11+ is needed by validation
scripts. A native linker and the target OS facilities are required for process
tests. This crate has no independent workspace or installable user CLI.

A passing target-gated suite on one platform does not establish support on
another.

## Choose a focused check

1. Select the section for your change: [pure adapters](#start-with-an-isolated-pure-adapter-check),
   [native processes or host receipts](#run-process-tests-on-the-owning-os), or
   [Rustdoc and Markdown](#check-rustdoc-and-integration-through-the-existing-runner).
2. Run the selected commands from the repository root. A target-gated suite must
   execute cases on its owning OS; zero tests is not platform validation.
3. Check the exit status and selected test names. Use the [failure table](#diagnose-failures-using-the-owning-evidence)
   to locate evidence, then follow the cleanup notes beside the selected suite.

## Start with an isolated pure-adapter check

For coordination, in-memory repository, and event changes, this complete
command uses a separate build output directory and runs only named test targets:

```console
CARGO_TARGET_DIR=target/runtime-contributor-checks \
  cargo test --locked -p undercurrent-runtime \
  --test runtime_contract --test event_journal --test event_feed
```

Expected result: Cargo exits zero with the selected tests passing. There is no
daemon to start or stop for these targets. The fixtures construct adapters in
memory. Preserve this distinction when adding a test: real process behavior
belongs in process fixtures, not in a supposedly pure queue test.

### Terminal, profiles, and health

Terminal and evidence changes have separate targets:

```console
CARGO_TARGET_DIR=target/runtime-contributor-checks \
  cargo test --locked -p undercurrent-runtime \
  --test terminal_screen --test vt100_regressions --test vte_regressions \
  --test foreground_evidence --test profile_catalog --test health_checks
```

Health tests include local test-server I/O. Parser regression targets compare
the workspace's vendored implementation with pinned registry reference crates;
they protect observable semantics across optimized decoder paths. The
[terminal benchmark](../../benches/terminal_decoder.rs) is useful for performance
investigation after correctness checks, but a benchmark is not a replacement
for parser equivalence.

## Run process tests on the owning OS

On Linux or macOS, use the native process target:

```console
CARGO_TARGET_DIR=target/runtime-contributor-checks \
  cargo test --locked -p undercurrent-runtime \
  --test native_process_runtime
```

These fixtures launch their own children and PTYs. Linux sealed checks need
the relevant kernel facilities and `/proc` descriptor access; macOS coverage
uses native mode and Darwin adapters. Read the target's `cfg` guards to see
which cases actually ran. Do not point fixtures at an existing user's socket
or process IDs.

On macOS, the 300-process capacity fixture runs in a separate test subprocess
with its file-descriptor soft limit raised to 2048. This preserves the full
workload despite the common 256-descriptor shell limit, without changing the
parent test runner's limits. The runner must permit that increase and have
enough available PTYs; a setup failure is reported as a failed test.

### Linux host receipts

For private mutation receipts on Linux, the module runs a real host and SQLite
under temporary directories and its own test-subprocess entry point:

```console
CARGO_TARGET_DIR=target/runtime-contributor-checks \
  cargo test --locked -p undercurrent-runtime \
  --lib process::host_receipt_tests:: -- --test-threads=1
```

No installed daemon is required by this module; its wrapper launches the test
binary. The scenarios deliberately kill their own recorded host/process
incarnations and reconcile receipts. Expect slower tests and platform process
dependencies such as `/bin/sh`, `awk`, `sleep`, and `/proc`. A crash or timeout
is a failed check to investigate, not permission to run broad process-kill
commands. Retain fixture artifacts when diagnosing a failure and restrict
cleanup to their proven owners. The tests contain cleanup paths, but interruption
or assertion failure can bypass those paths.

### Windows process and execution tests

On Windows, use PowerShell from the repository root:

```powershell
$previousRuntimeTarget = $env:CARGO_TARGET_DIR
try {
    $env:CARGO_TARGET_DIR = 'target/runtime-contributor-checks'
    cargo test --locked -p undercurrent-runtime --test windows_native_runtime
    if ($LASTEXITCODE -ne 0) { throw 'Native runtime tests failed' }
    cargo test --locked -p undercurrent-runtime `
      --lib process::execute_host::windows_tests::
    if ($LASTEXITCODE -ne 0) { throw 'Windows execution tests failed' }
} finally {
    $env:CARGO_TARGET_DIR = $previousRuntimeTarget
}
```

The block restores your previous build directory even if a test fails.
These tests require Windows process/job, named-pipe, and ConPTY facilities plus
standard command tools used by the fixtures. The test target covers command
input/resize, batch argument handling, native updates, managed-tree listener
evidence, stop escalation, and directory replacement. A non-Windows build can
report zero tests for this target; that is not Windows validation.

### Clean up fixture build output

When all relevant tests have stopped, `target/runtime-contributor-checks` is
disposable build output and can be removed if no longer needed. It is separate
from product runtime state. Do not delete live host journals as test cleanup.

## Check Rustdoc and integration through the existing runner

For local Rust API comment or example edits, use focused checks first:

```console
CARGO_TARGET_DIR=target/runtime-contributor-checks \
  cargo test --locked -p undercurrent-runtime --doc
RUSTDOCFLAGS=-Dwarnings \
  CARGO_TARGET_DIR=target/runtime-contributor-checks \
  cargo doc --locked -p undercurrent-runtime --no-deps
```

Successful Rustdoc exits zero and writes
`target/runtime-contributor-checks/doc/undercurrent_runtime/index.html`.
The maintained tests are executable maintainer fixtures; external consumers
should use the [SDK](../../../undercurrent-sdk/README.md).

The [project validation runner](../../../../scripts/validate.py)
remains the integration authority, including warning-denied workspace Rustdoc:

```console
python3 scripts/validate.py --stage rustdoc \
  --report target/runtime-rustdoc-validation/report.json
```

That stage builds workspace documentation and runs offline with the pinned
toolchain. Missing cached dependencies are a setup failure, not a reason to
remove the check. A passed report covers only the executed stage. Use the
runner's `--list` to inspect other stages and select them according to the
owning change; release, smoke, and recovery stages have additional prerequisites
and should not be substituted for a focused unit check.

### Markdown and diagrams

For Markdown, verify relative paths, anchors, navigation, contract agreement,
and the complete diff. Render every changed Mermaid block and visually inspect
labels, relationships, clipping, and readability. Follow the shared
[verification guide](../../../../docs/development/verification.md) for compiler,
link, and diagram checks.
Parsing or finding `accTitle`/`accDescr` alone is not rendered validation.

## Diagnose failures using the owning evidence

| Symptom | First investigation | Avoid concluding |
| --- | --- | --- |
| Launch rejected before a child appears | [Preflight errors, mode, receipt and binding](../internals/processes/launch.md) | A different `PATH` lookup is a safe retry of the accepted launch |
| Exit not delivered | [Arming and output-completion gates](../internals/processes/native-processes.md), then [host output checkpoint](../internals/host/recovery.md) | Process exit means durable application exit settlement |
| Input returns an error after write began | [Input worker partial effects](../internals/processes/native-processes.md) and [attempt receipt](../internals/host/recovery.md) | No bytes reached the child |
| Missing raw output but visible screen | [Retention gaps and snapshot recovery](../internals/host/recovery.md) | The snapshot restored historical logs |
| Mutation or execution outcome unknown | Exact attempt/job, host journal availability, application settlement | Reconnection authorizes a new mutation |
| Process/receipt capacity exhausted | Occupied native slots, unarmed exits, pending receipts/jobs and ACK progress | All capacity is consumed by running children |
| Foreground state never becomes ready | [Freshness, completeness, profile match and health sampling](../internals/evidence.md) | Missing or truncated evidence proves busy or ready |
| Host request never resolves | Socket write/read path, pending map, worker queue and blocking effect | `io_timeout` is a general reply deadline |

Report the exact target, commands, exit status, observed failures, and unverified
platforms. Link the relevant source/test and keep command/environment/terminal
payloads out of diagnostic reports unless deliberately needed and redacted.

[Crate documentation](../README.md)
