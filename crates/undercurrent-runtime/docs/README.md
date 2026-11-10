# undercurrent-runtime maintainer documentation

Choose the mechanism or contract you need to investigate below. The
[crate overview](../README.md) describes its scope and starting checks.

## Internals

- [Find the owner of a runtime change](internals/ownership.md).
- [Keep terminal bytes, screens, and input consistent](internals/terminal.md).
- [Interpret foreground evidence and health checks](internals/evidence.md).

### Process preparation and execution

- [Understand launch preparation and verification](internals/processes/launch.md).
- [Trace a native process from admission to exit](internals/processes/native-processes.md).
- [Trace a command executed in a session's context](internals/processes/execution.md).

### Session host and recovery

- [Understand the daemon-independent session host](internals/host/session-host.md).
- [Reconcile host disconnection and uncertain effects](internals/host/recovery.md).

### Coordination and event delivery

- [Coordinate bounded runtime work](internals/coordination/scheduling.md).
- [Follow a committed change into event replay](internals/coordination/events.md).

## Reference

- [Look up the private session-host protocol](reference/host-protocol.md).

## Development

- [Verify a runtime documentation or adapter change](development/verification.md).

[System architecture](../../../docs/architecture/README.md) · [Workspace code map](../../../docs/architecture/dependencies.md) · [Product documentation](../../../docs/README.md)
