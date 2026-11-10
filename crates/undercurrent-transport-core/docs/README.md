# Transport core maintainer documentation

Use these pages to change or diagnose the internal server engine. Start with
[connection ownership](internals/ownership.md) if you are new to the crate;
start with [focused checks](development/contributing.md#run-focused-checks) to
reproduce a regression without a daemon.

## Find a mechanism

| Question | Explanation |
| --- | --- |
| Who closes the session, releases its lease, and owns shared state? | [Connection ownership and composition](internals/ownership.md) |
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
