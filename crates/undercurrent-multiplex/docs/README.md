# undercurrent-multiplex maintainer documentation

Start with the [bounded delivery exercise](development/contributing.md#run-the-local-exercise)
to see queue pressure, credit return, and reconnect replay without a daemon.
The [crate overview](../README.md) identifies the internal boundary and supported
SDK entry points.

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
