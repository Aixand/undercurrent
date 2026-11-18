# Transport core maintainer documentation

Use these pages to change or diagnose the internal server engine. Start with
[connection ownership](internals/ownership.md) if you are new to the crate;
start with [focused checks](development/contributing.md#run-focused-checks) to
reproduce a regression without a daemon. The core owns server admission,
correlation, scheduling, and bounded result retention. The local adapter owns
physical I/O and the daemon owns product effects.

## Package and reader scope

This internal server engine has no standalone service or installation.
The [architecture policy](../../../tests/architecture/allowed-graph.json)
assigns it `transport_independent_server_engine`. Its Rust exports support
internal composition. The [SDKs](../../../docs/sdk/README.md) and the
[product protocol](../../../docs/reference/protocol/README.md) are the client
integration paths. Package version `0.1.0-beta` uses workspace Rust 1.95.0 and
declares no crate feature flags. The [composition prerequisites](internals/ownership.md#composition-prerequisites)
distinguish wire and product contract versions. The local adapter listens on
sockets and verifies OS peers. Product dispatch decodes operations and delegates
process effects and durable commits to their owners.

## Find a mechanism

| Question | Explanation |
| --- | --- |
| Who closes the transport session, releases its lease, and owns shared state? | [Connection ownership and composition](internals/ownership.md) |
| Which source owns admission, dispatch, or retention? | [Core source map](internals/ownership.md#code-map) |
| Why is a request ID consumed, and when is a response settled? | [Request admission, settlement and cancellation](internals/request-lifecycle.md) |
| Can a lost mutation result be replayed safely? | [Exact result retention and its limits](internals/idempotency.md) |
| Why is a stream opening, stalled, or missing history? | [Stream activation, write confirmation and recovery](internals/streams-and-recovery.md) |

## Change or diagnose the engine

- [Development procedure and focused test selection](development/contributing.md).
- [Pressure, pagination and telemetry reference](reference/diagnostics.md).
- [Error categories and overload reference](reference/errors.md).
- [Run the retained-result example](internals/idempotency.md#run-a-daemon-free-retention-example).

[Crate scope and supported consumer entry points](../README.md) ·
[System architecture](../../../docs/architecture/README.md) ·
[Workspace code map](../../../docs/architecture/dependencies.md) ·
[Product documentation](../../../docs/README.md)
