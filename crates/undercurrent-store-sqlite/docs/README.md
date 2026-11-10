# undercurrent-store-sqlite maintainer documentation

Choose the mechanism or contract you need to investigate below. The
[crate overview](../README.md) describes its scope and starting checks.

## Internals

- [Understand repository ownership and worker behavior](internals/ownership.md).

### Durable transactions

- [Trace session transactions and outbox delivery](internals/transactions/sessions-and-outbox.md).
- [Reconcile operation attempts without repeating unknown effects](internals/transactions/attempts.md).
- [Follow durable runtime cancellation through restart](internals/transactions/runtime-cancellation.md).
- [Understand durable agent events and replies](internals/transactions/agent-channel.md).

## Reference

- [Inspect storage format and compatibility rules](reference/storage-format.md).

## Development

- [Exercise, verify, and recover a SQLite repository](development/maintenance.md).
- [Verify and extend the SQLite adapter](development/contributing.md).

[System architecture](../../../docs/architecture/README.md) · [Workspace code map](../../../docs/architecture/dependencies.md) · [Product documentation](../../../docs/README.md)
