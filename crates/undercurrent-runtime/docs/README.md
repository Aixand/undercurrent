# undercurrent-runtime maintainer documentation

Runtime implements process, terminal, and coordination ports. Application owns
effect ordering, SQLite owns durable session transactions, and daemon wires
the private session host. Use the [ownership and source map](internals/ownership.md)
to locate an adapter and the traces below to identify its boundary. Start with
[isolated adapter checks](development/verification.md#start-with-an-isolated-pure-adapter-check)
for in-memory behavior or [native process tests](development/verification.md#run-process-tests-on-the-owning-os)
for OS effects.

## Package and reader scope

This daemon-private crate uses [domain values](../../undercurrent-domain/docs/README.md)
to implement [application ports](../../undercurrent-application/docs/README.md).
Its `pub` items support composition. Public transport, SDK behavior,
authorization, and application-level success belong to other layers.
Package version `0.1.0-beta` identifies the crate. Private host wire and receipt
formats have independent scope. Process-host receipts have narrower effects and
retention than durable session transactions. Use the [session-host ownership view](internals/host/session-host.md#locate-processes-and-state)
for process boundaries, stores, journals, and one-shot execution.

The adapters cover launch verification, terminal delivery and screens, foreground
observation, health checks, profile matching, coordination, events, restart, and
telemetry. See the [manifest](../Cargo.toml) for platform dependencies.
The workspace requires Rust 1.95. Native process tests use OS-specific fixtures.

## Internals

- [Find the owner of a runtime change](internals/ownership.md).
- [Keep terminal bytes, screens, and input consistent](internals/terminal.md).
- [Interpret foreground evidence and health checks](internals/evidence.md).

| Investigate | Trace |
| --- | --- |
| Launch selection or verification failed | [Prepare and verify the launch](internals/processes/launch.md) |
| A native child failed to start or exit cleanly | [Trace native process ownership](internals/processes/native-processes.md) |
| A one-shot command lost its result | [Trace execution, cancellation, and retained results](internals/processes/execution.md) |
| Daemon lost the private host connection | [Locate host state](internals/host/session-host.md), then [reconcile uncertain effects](internals/host/recovery.md) |
| Work is queued or an event is missing | [Check coordination](internals/coordination/scheduling.md) or [trace event publication and replay](internals/coordination/events.md) |

## Reference

- [Look up the private session-host protocol](reference/host-protocol.md).

## Development

- [Verify a runtime documentation or adapter change](development/verification.md).

[System architecture](../../../docs/architecture/README.md) · [Workspace code map](../../../docs/architecture/dependencies.md) · [Product documentation](../../../docs/README.md)
