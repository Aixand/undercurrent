# undercurrent-multiplex maintainer documentation

Start with the [bounded delivery exercise](development/contributing.md#run-the-local-exercise)
to see queue pressure, credit return, and reconnect replay without a daemon.
The [crate overview](../README.md) identifies the internal boundary and supported
SDK entry points.

Multiplex owns routing, scheduling, and replay state. The SDK and server
adapters drive connection I/O and recovery. Use the
[verification matrix](development/contributing.md#choose-verification-for-the-change)
to select checks for the boundary you changed.

## Package and reader scope

The [architecture policy](../../../tests/architecture/allowed-graph.json)
classifies this internal crate as `internal_ucmp_scheduling`. It starts no
threads and owns no session or database state. Package version `0.1.0-beta`
varies independently of the [UCMP wire version](../../undercurrent-wire/src/version.rs).

The [bounded delivery example](../examples/bounded_delivery.rs) exercises
admission, credit exhaustion, consumption, and replay. The
[contract suite](../tests/multiplex_contract.rs) supplies regression evidence.

## Internals

| Maintainer question | Explanation |
| --- | --- |
| Which object owns this state, and which adapter drives it? | [Ownership and source map](internals/ownership.md) |
| What survives a timeout or disconnect? Which checkpoint resumes? | [Request waits and stream consumption](internals/routing.md) |
| Why was a frame rejected, replaced, dropped, or left queued? | [Admission, selection, and credit](internals/scheduling.md) |
| When is a written frame retained? Why did resume return a gap? | [Write confirmation and replay lifetime](internals/replay.md) |

## Reference

- [Interpret multiplex failures and pressure](reference/diagnostics.md).

## Development

- [Exercise and change multiplex behavior](development/contributing.md).
- [Diagnose a stalled or disconnected stream](development/diagnose-delivery.md).

[System architecture](../../../docs/architecture/README.md) · [Workspace code map](../../../docs/architecture/dependencies.md) · [Product documentation](../../../docs/README.md)
