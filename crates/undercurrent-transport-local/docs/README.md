# undercurrent-transport-local maintainer documentation

Use these pages to change or diagnose the protected local adapter. For package
scope, supported platforms, and consumer entry points, start with the
[crate overview](../README.md).

## Choose a task

| I need to… | Start here |
| --- | --- |
| Reproduce behavior or check a change | [Verify and extend the local transport](development/contributing.md) — in-memory example, native tests, and change ownership. |
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
