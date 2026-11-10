# undercurrent-daemon maintainer documentation

Choose the mechanism or contract you need to investigate below. The
[crate overview](../README.md) describes its scope and starting checks.
For operator tasks, use the [shared daemon guides](../../../docs/guides/daemon/README.md).

## Internals

- [Understand daemon composition and ownership](internals/composition.md).
- [Read output and recover subscriptions](internals/output-and-streams.md).
- [Extend diagnostic collection and telemetry](internals/diagnostics.md).

### Service lifecycle and background work

- [Trace startup, recovery, and shutdown](internals/lifecycle/startup-and-shutdown.md).
- [Understand background work and wake ordering](internals/lifecycle/background-work.md).

### Requests and effect evidence

- [Follow a request from admission to a durable result](internals/requests/request-flow.md).
- [Reconcile uncertain mutation outcomes](internals/requests/effects-and-recovery.md).
- [Trace an attached agent call and durable reply](internals/requests/agent-channel.md).

### Configuration and profile activation

- [Configure daemon resource bounds](internals/configuration/resource-bounds.md).
- [Understand atomic profile activation](internals/configuration/profile-reload.md).

## Development

- [Navigate, change, and verify the daemon](development/contributing.md).

[System architecture](../../../docs/architecture/README.md) · [Workspace code map](../../../docs/architecture/dependencies.md) · [Product documentation](../../../docs/README.md)
