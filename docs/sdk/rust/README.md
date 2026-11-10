# Integrate with the blocking Rust client

This reading path points to the maintained Rust SDK guides. Rust-specific
usage and recovery guides live here, while maintained examples and client
implementation remain in the crate. Shared
[SDK choices](../README.md) compares languages and supported boundaries.

## Use the maintained consumer examples

Begin with [connect and query](connect-and-query.md).
It includes prerequisites, dependency declarations, endpoint selection, complete
commands, expected results, and cleanup against an already-running daemon.

| Next task | Complete guide and maintained source |
| --- | --- |
| Query status and open a watch | [Connection tutorial](connect-and-query.md), [query_watch.rs](../../../crates/undercurrent-sdk/examples/query_watch.rs) |
| Construct and validate a protocol payload | [First payload tutorial](first-payload.md) |
| Create and stop a disposable session | [Operation guide](operations.md#create-and-clean-up-a-disposable-session), [create_and_stop.rs](../../../crates/undercurrent-sdk/examples/create_and_stop.rs) |
| Read and observe terminal output | [Output guide](streams-and-recovery.md#observe-terminal-output) |

## Share and order work

Read [deadlines and ownership](streams-and-recovery.md#deadlines-and-ownership)
for caller timeouts, sharing, and cancellation. Maintainers can trace the
[worker ownership model](../../../crates/undercurrent-sdk/docs/internals/client-runtime.md#runtime-ownership-and-concurrency).

## Use exact contracts and bounded operations

Use the [operation task map](operations.md#choose-the-right-operation)
for requests and mutation reconciliation. [Stream recovery](streams-and-recovery.md)
owns typed streams and discontinuities; [output concepts](../../architecture/observations-and-output.md)
defines the distinct byte offsets, stream sequences, and event cursors.

## Budget, diagnose, and release

[Limits and diagnostics](limits-and-diagnostics.md)
owns profiles, bounded telemetry, and error interpretation. Read the shared
[recovery guide](../recovery.md) for operation outcome uncertainty and the
[SDK implementation guide](../../../crates/undercurrent-sdk/docs/development/extending-client.md#verification)
for maintainer verification.

[SDK choices](../README.md) · [Protocol reference](../../reference/protocol/README.md)
