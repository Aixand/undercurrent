# Locate Windows responsibilities and boundaries

Use this source map to choose where a Windows change belongs. It covers
`aixand-undercurrent-platform-windows` 0.1.0-beta in this checkout; all exports are
Windows-only. Begin with [native setup](../development/contributing.md#prepare-a-native-checkout)
if you need to run the code.

For the wider model, use the shared [process and state diagram](../../../../docs/architecture/README.md#process-boundaries),
[workspace dependency map](../../../../docs/architecture/dependencies.md#manifest-map),
and [creation, output, and recovery walkthroughs](../../../../docs/architecture/flows.md).
The adapter is linked into its callers' processes. It is not a separate service.

## What the adapter owns

The adapter wraps Win32 calls with Rust ownership, input validation, bounded
queries, and `io::Result` failures. `OwnedHandle` and `File` close retained OS
objects on drop. Unsafe code is denied by default in the
[manifest](../../Cargo.toml); narrow `#[allow(unsafe_code)]` blocks carry local
safety explanations. The crate has no Undercurrent dependencies and cannot
decide product policy by importing application or protocol types.

Named pipes provide interprocess communication (IPC); ConPTY is Windows'
pseudoconsole facility. A Job Object is a kernel object used here to contain or
observe a set of processes.

The [manifest](../../Cargo.toml) declares Win32 bindings, Tokio pipe/console
support, and endpoint hashing libraries. `tempfile` is used by tests and the
example. Cargo requirements and lockfile resolutions are distinct; package
version 0.1.0-beta does not declare a wire or product contract version.

## Code map and investigation paths

| Start in this source | Follow the responsibility |
| --- | --- |
| [lib.rs](../../src/lib.rs) | Windows gate and sibling-crate exports. |
| [pipe.rs](../../src/pipe.rs) | Endpoint hashing, restricted pipe handles, kernel peer account checks. Used by [public local transport](../../../undercurrent-transport-local/src/windows.rs). |
| [blocking_pipe.rs](../../src/blocking_pipe.rs) | Blocking byte streams for the [session host](../../../undercurrent-runtime/src/process/host.rs) and [agent gateway](../../../undercurrent-daemon/src/agent_bridge_gateway.rs). |
| [console.rs](../../src/console.rs) | Console notification workers and standard-handle inheritance. Used by [CLI signals](../../../undercurrent-cli/src/signals.rs) and [daemon main](../../../undercurrent-daemon/src/main.rs). |
| [security.rs](../../src/security.rs) and [file_owner.rs](../../src/file_owner.rs) | Process-token account queries and synchronous default file ownership. The [SQLite wrapper](../../../undercurrent-store-sqlite/src/sqlite/mod.rs) supplies library-created file ownership. |
| [filesystem.rs](../../src/filesystem.rs) | Pins, private ACLs, publication, locks, exact removal. Callers include [host files](../../../undercurrent-runtime/src/process/host_files.rs), [daemon instance ownership](../../../undercurrent-daemon/src/process_instance_windows.rs), and [CLI reset](../../../undercurrent-cli/src/reset/windows.rs). |
| [process.rs](../../src/process.rs) | Retained identity, ancestry, system paths. Used by [agent ancestry validation](../../../undercurrent-runtime/src/process/hosted.rs). |
| [job.rs](../../src/job.rs) | Assign-before-resume launch and whole-job termination for [one-shot supervisors](../../../undercurrent-runtime/src/process/execute_host.rs). |
| [job_observation.rs](../../src/job_observation.rs) and [listeners.rs](../../src/listeners.rs) | Bounded membership and host TCP evidence. The [managed PTY](../../../undercurrent-runtime/src/process/managed.rs) supplies the observer; [runtime observation](../../../undercurrent-runtime/src/process/mod.rs) interprets the result. |

Use the [mechanism index](../README.md#understand-a-mechanism) for local explanations
and diagrams. This map identifies source owners rather than listing every API.

## Follow representative flows

The shared walkthroughs own end-to-end ordering. At this crate's boundary,
follow these entry points:

### [Session creation](../../../../docs/architecture/flows.md#start-a-session)

[Windows preflight](../../../undercurrent-runtime/src/process/windows_preflight.rs) retains `PinnedPath` objects for executable and working directory checks. Runtime owns content hashes, executable receipts, environment binding, batch handling, and typed launch errors. Runtime and `portable-pty` create ConPTYs. See [native runtime tests](../../../undercurrent-runtime/tests/windows_native_runtime.rs) for launch, I/O, resize, sealed-request rejection, directory replacement, and restart.

### [Terminal output](../../../../docs/architecture/flows.md#consume-terminal-bytes)

ConPTY bytes pass through [managed runtime](../../../undercurrent-runtime/src/process/managed.rs) and [host routing](../../../undercurrent-runtime/src/process/host.rs). Private host frames use `BlockingPipeStream`; public streams use asynchronous local transport. Runtime owns history, cursors, replay, and retention gaps. One-shot stdout/stderr use the separate [job cleanup flow](processes.md#launch-a-contained-one-shot-process).

### [Recovery](../../../../docs/architecture/flows.md#recover-after-daemon-replacement)

Retained process and file identities support [hosted recovery](../../../undercurrent-runtime/src/process/hosted.rs). The [execution registry](../../../undercurrent-runtime/src/process/execute_host.rs), [attempt receipts](../../../undercurrent-runtime/src/process/attempt_receipts.rs), and [SQLite repository](../../../undercurrent-store-sqlite/src/sqlite/mod.rs) own journals, results, fences, and deduplication. Reopening a pipe supplies a byte connection; replay follows the owning receipt/probe contract.

The daemon owns repository access. The separate session host owns PTYs and
managed jobs, and retains control of one-shot supervisors; each supervisor
retains its workload job. Those resource lifetimes do not establish a durable
session or operation result. Keep the shared process diagram as the authority
for process/state ownership.

## Preserve these boundaries when extending the crate

Keep new wrappers small enough to describe their handle rights, ownership
transfer, input bounds, and failure paths locally. Explain every unsafe call's
pointer lifetime and output validation. Prefer retained objects over reopening
numeric PIDs or mutable pathnames, and preserve `Truncated` or errors where a
bounded query cannot prove absence.

Keep new policy in its owner: product DTOs and wire versions in
[protocol](../../../undercurrent-protocol/README.md), native launch and host recovery
in [runtime](../../../undercurrent-runtime/README.md), request framing in
[local transport](../../../undercurrent-transport-local/README.md), and transaction
semantics in [SQLite](../../../undercurrent-store-sqlite/README.md). An extra Windows
primitive does not authorize another public transport or an external library API.

Use [development and verification](../development/contributing.md#choose-checks-for-a-change)
to select tests that cover the affected boundary.

[Crate documentation](../README.md)
