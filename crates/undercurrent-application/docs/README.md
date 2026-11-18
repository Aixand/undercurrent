# undercurrent-application maintainer documentation

Application owns use-case ordering and [port contracts](../src/ports.rs).
Domain owns the decisions inside a transition, runtime and SQLite implement
the effects, and daemon composes the adapters. Start with the
[ownership and source map](internals/ownership.md), trace the relevant workflow
below, then use the [fake-port verification loop](development/contributing.md#run-a-focused-verification-loop)
to check ordering and failure handling.

## Package and reader scope

The [crate root](../src/lib.rs) exposes modules and re-exports.
Application depends only on [domain](../../undercurrent-domain/docs/README.md)
among workspace crates. Its `pub` items support internal composition.
The [architecture policy](../../../tests/architecture/allowed-graph.json)
identifies the SDK and protocol as the external Rust library surfaces.
For an output retention regression, run the [in-memory gap example](internals/sessions/terminal-io.md#run-an-in-memory-retention-example).
Lifecycle tests exercise ordering without a daemon.

## Internals

- [Find the owner of an application change](internals/ownership.md).

### Sessions and terminal I/O

- [Follow a session from creation through restart](internals/sessions/lifecycle.md).
- [Understand list queries, watches, and private detail](internals/sessions/read-models.md).
- [Follow input delivery and recover terminal output](internals/sessions/terminal-io.md).

### Execution and runtime controls

- [Track process effects across lost responses](internals/execution/attempts.md).
- [Repair effects, cancel an attached runtime, and shut down the host](internals/execution/runtime-controls.md).
- [Execute a bounded command in a session context](internals/execution/commands-and-secrets.md).
- [Trace an agent call through the durable queue](internals/execution/agent-channel.md).

### Observation and profiles

- [Trace observations without blocking session mutations](internals/observation/observations.md).
- [Investigate profile selection and rule evaluation](internals/observation/profiles.md).

### Recovery and event publication

- [Reconcile durable sessions with process truth](internals/recovery/startup.md).
- [Trace a committed change into the event feed](internals/recovery/events-and-outbox.md).

## Diagnostics

- [Diagnose application failures and authorize diagnostic content](internals/diagnostics.md).

## Development

- [Investigate and verify an application-layer change](development/contributing.md).

[System architecture](../../../docs/architecture/README.md) · [Workspace code map](../../../docs/architecture/dependencies.md) · [Product documentation](../../../docs/README.md)
