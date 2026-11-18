# undercurrent-transport-local maintainer documentation

Use these pages to change or diagnose the protected local adapter. For package
scope, supported platforms, and consumer entry points, start with the
[crate overview](../README.md). This adapter owns local peer verification and
framed I/O. Transport core owns the request and stream engine.

## Package and reader scope

The [manifest](../Cargo.toml) inherits version `0.1.0-beta`, Rust 1.95, and
edition 2024. It declares no crate feature flags. Native tests exercise Unix
domain sockets on Linux and macOS and local named pipes on Windows separately.
TCP, TLS, WebSocket, token-file, and browser transports are outside this
adapter. The local OS account is the trust boundary. It provides no isolation
between processes under that account or protection from a host administrator.

The SDK supplies client I/O. The daemon gives `LocalServer` an authenticated
dispatcher. This crate and its transport-core re-exports support internal
composition under the `protected_local_ipc` role in the
[architecture policy](../../../tests/architecture/allowed-graph.json).
Use generated Rustdoc for symbol lookup. [Wire](../../undercurrent-wire/docs/README.md)
owns codecs and [multiplex](../../undercurrent-multiplex/docs/README.md) supplies
scheduling and replay. Product effects and private host or agent channels have
separate owners. See [client boundaries](internals/ownership.md#client-boundaries)
for supported integration paths.

## Choose a task

| I need to… | Start here |
| --- | --- |
| Reproduce behavior or check a change | [Verify and extend the local transport](development/contributing.md) — in-memory example, native tests, and change ownership. See also [test selection by behavior](development/contributing.md#find-the-relevant-test-suite). |
| Diagnose a rejected connection or a drain that will not finish | [Local connection troubleshooting](development/troubleshooting.md) — safe investigation and recovery checks. |
| Find the layer that owns a behavior | [Transport boundary and source map](internals/ownership.md) — adapter, core, and product responsibilities. |

## Understand a mechanism

- [Endpoint protection and lifetime](internals/endpoints.md): address validation,
  Unix stale recovery, and Windows pipe ownership.
- [Connection lifecycle](internals/server.md): admission, dispatch, writer
  handoff, and cooperative shutdown.

## Look up a contract

- [Bounded framing](reference/framing.md): header checks, partial I/O,
  cancellation, and capacity termination bytes.
- [Server limits and deadlines](reference/server-limits.md): constructor
  constraints, scheduler budgets, and the scope of each timer.
- [Errors and diagnostics](reference/diagnostics.md): emitted error mappings,
  safe logging, pressure samples, and run report counters.

For application effects and consumer recovery, use the shared
[system flows](../../../docs/architecture/flows.md) and
[SDK recovery guide](../../../docs/sdk/recovery.md).
[System architecture](../../../docs/architecture/README.md) ·
[Workspace code map](../../../docs/architecture/dependencies.md) ·
[Product documentation](../../../docs/README.md)
