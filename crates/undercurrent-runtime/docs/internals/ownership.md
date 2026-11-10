# Find the owner of a runtime change

Use this maintainer map to trace application work into runtime mechanisms and
choose the correct extension point. It covers `undercurrent-runtime` 0.1.0-beta in
the current workspace. The [architecture policy](../../../../tests/architecture/allowed-graph.json)
assigns this crate the `daemon_private_runtime_uchs` role. The supported external
Rust library surfaces are protocol and SDK; runtime's public Rust visibility is
for first-party composition.

## Keep dependency and process boundaries separate

Use the project [dependency map](../../../../docs/architecture/dependencies.md#manifest-map)
for crate relationships and the [session-host ownership view](host/session-host.md#locate-processes-and-state)
for runtime processes. This page maps mechanisms to their owning source and
extension guide rather than maintaining those diagrams again.

Runtime implements application ports using domain values and target-specific
macOS/Windows primitives. Its [manifest](../../Cargo.toml) also selects PTY, terminal
parser, channel, regex, HTTP, serialization, hashing, and OS libraries. SQLite is
a development dependency; production persistence arrives through application
ports. Vendored parser/PTY code remains outside this crate's ownership.

## Trace session creation through the boundary

Start with the shared [session-creation walkthrough](../../../../docs/architecture/flows.md#start-a-session).
Application owns admission, activation ordering, compensation, and durable
settlement. Runtime supplies [launch preparation](processes/launch.md),
[native process ownership](processes/native-processes.md), and [private-host delivery](host/session-host.md).
These local guides explain the mechanisms behind each runtime call.

The [application's dispatched-start contract](../../../undercurrent-application/docs/internals/execution/attempts.md#dispatched-spawn-is-settled-by-the-start-service)
owns deferred settlement and post-commit receipt acknowledgement. The durable
wrapper forwards acknowledgement to the runtime; failure preserves the live
session so an exact replay can retry receipt release. Semantic settlement and
receipt release remain separate operations. Runtime [recovery](host/recovery.md)
explains retained host evidence and exact-process reconciliation.

## Navigate by responsibility

This map groups behavioral owners; generated Rustdoc remains the symbol index.
Each explanation links its representative tests and invariants.

| Responsibility | Owning modules | Read next |
| --- | --- | --- |
| Crate surface, common errors, platform declaration | [lib](../../src/lib.rs), [error](../../src/error.rs), [platform](../../src/platform.rs) | This page and [verification](../development/verification.md) |
| Clock and ID generation | [clock](../../src/clock.rs) | [Coordination and telemetry](coordination/scheduling.md) |
| Session mutation leases and exact process bindings | [coordinator](../../src/coordinator.rs), [directory](../../src/directory.rs) | [Coordination](coordination/scheduling.md) |
| Scheduled observations, restarts, input reservations | [observations](../../src/observations.rs), [restarts](../../src/restarts.rs), [deliveries](../../src/deliveries.rs) | [Queue revision and deduplication boundaries](coordination/scheduling.md) |
| In-memory repository and journal/feed | [repository](../../src/repository.rs), [events](../../src/events.rs) | [Commit, publication, replay, and ACK](coordination/events.md) |
| Telemetry capture and aggregation | [telemetry](../../src/telemetry.rs) | [Bounded telemetry](coordination/scheduling.md) |
| Foreground evidence and shell signals | [foreground](../../src/foreground.rs), [cooperative](../../src/cooperative.rs) | [Evidence](evidence.md) |
| Profile loading, bounded regex, health workers | [profiles](../../src/profiles.rs), [patterns](../../src/patterns.rs), [health_checks](../../src/health_checks.rs) | [Profile and health interpretation](evidence.md) |
| Input encoding and terminal projection | [terminal_input](../../src/terminal_input.rs), [terminal_screen](../../src/terminal_screen.rs), [health samples](../../src/terminal_screen/health.rs), [profile samples](../../src/terminal_screen/profile.rs) | [Terminal model](terminal.md) |
| Native runtime ownership, spawn, managed process, waits | [process/mod](../../src/process/mod.rs), [spawn](../../src/process/spawn.rs), [managed](../../src/process/managed.rs), [reaper](../../src/process/reaper.rs), [process/platform](../../src/process/platform.rs) | [Native lifecycle](processes/native-processes.md) |
| Launch preparation and environment | [preparation](../../src/process/preparation.rs), [environment](../../src/process/environment.rs), [preflight](../../src/process/preflight.rs), [Windows preflight](../../src/process/windows_preflight.rs) | [Launch guarantees](processes/launch.md) |
| Kernel foreground evidence | [Linux observation](../../src/process/linux_observation.rs), [macOS observation](../../src/process/macos_observation.rs) | [OS evidence and completeness](evidence.md) |
| Private host and daemon client | [host](../../src/process/host.rs), [hosted](../../src/process/hosted.rs), [protocol](../../src/process_host_protocol.rs) | [Host ownership](host/session-host.md), [wire reference](../reference/host-protocol.md) |
| Captured shell context | [shell_environment](../../src/process/shell_environment.rs) | [Peer verification](host/session-host.md#verify-shell-and-agent-peers), [execution](processes/execution.md) |
| Host files, receipts, and exact exit proof | [host_files](../../src/process/host_files.rs), [attempt_receipts](../../src/process/attempt_receipts.rs), [terminal_exits](../../src/process/terminal_exits.rs), [attempt_observer](../../src/process/attempt_observer.rs) | [Recovery](host/recovery.md) |
| One-shot command adapter and supervision | [session_command](../../src/process/session_command.rs), [execute_host](../../src/process/execute_host.rs) | [Execution](processes/execution.md) |
| Real host and target-specific qualification | [host_receipt_tests](../../src/process/host_receipt_tests.rs), [windows_execution_tests](../../src/process/windows_execution_tests.rs) | [Verification tasks](../development/verification.md) |

## Choose an extension boundary

| Intended change | Start with | Preserve and verify |
| --- | --- | --- |
| New process mutation or changed effect meaning | Application/domain contract, then runtime adapter and host protocol | Admission before effect, certainty translation, result digest, durable receipt compatibility, post-settlement ACK; test response loss and host death |
| New OS mechanism | Native/platform adapter behind existing port | Exact process creation identity, single waiter ownership, platform-specific failure evidence; execute tests on the actual target |
| New terminal capability | Input encoder or screen owner | Binary offsets, parser consistency, query-response routing, opaque mode; compare parser behavior against maintained oracle tests |
| New detection rule | Domain rule/profile schema and evidence collector | Bounded compilation/sampling, completeness and freshness, process/revision revalidation, atomic profile reload |
| New queue or scheduler behavior | Owning in-memory adapter and application port | Capacity scope, fair ordering where promised, stale revision rejection, wake/cancel semantics and pressure diagnostics |
| New persisted host field | Owning journal schema and compatibility loader | Explicit version/default semantics, byte limits, private files, fail-closed corruption handling, retained replay after restart |

Keep authorization, command admission policy, session lifecycle transitions,
and durable product state in their existing inner owners. Runtime may report
that bytes were written, a process exited, or a receipt was committed; it cannot
infer that the application running inside a terminal completed a requested task.

## Check platform declarations against implementations

[RuntimePlatformSupport](../../src/platform.rs) reports PTYs and hosted sessions
for Unix and Windows, and supervised commands for Linux, macOS, and Windows.
Its foreground-inspection flag describes Unix-style foreground inspection on
Linux/macOS; Windows instead supplies managed job/process evidence. These
constants identify adapters and do not establish target-native qualification.

Use the specific [launch limits](processes/launch.md),
[execution supervision](processes/execution.md), and [evidence limitations](evidence.md)
when investigating behavior. Linux supports sealed preparation; Darwin and
Windows preparation require native mode. Windows managed-process/tree listener
evidence does not imply Unix-style foreground process-group inspection.
Release/platform qualification belongs to the repository's platform owners.

[Crate documentation](../README.md)
