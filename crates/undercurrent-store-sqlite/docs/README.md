# undercurrent-store-sqlite maintainer documentation

SQLite owns transaction and worker mechanics. Application defines persistence
ports and controls process effects. Use the [source map](internals/ownership.md#code-map)
to locate the SQL, codec, or worker, then follow the relevant transaction below.
Start with the [temporary repository reproduction](development/contributing.md#run-an-isolated-reproduction)
or select [focused tests](development/contributing.md#choose-the-relevant-tests)
for the contract you change.

## Package and reader scope

This internal workspace crate is version `0.1.0-beta`, sets `publish = false`,
and uses bundled SQLite without optional crate features. The
[architecture policy](../../../tests/architecture/allowed-graph.json) assigns it
`daemon_private_store_adapter`. The public SDKs reach storage through the daemon.

A bounded worker owns each connection. The repository atomically writes session
aggregates, outbox events, and event snapshots. It retains fenced attempts,
cancellation controls, and agent events and replies. Separate maintenance
functions verify, back up, and restore databases. With the pinned Rust toolchain
and locked dependencies, the temporary repository example covers reopen,
backup, restore, and cleanup without a daemon or child process.

## Internals

- [Understand repository ownership and worker behavior](internals/ownership.md).

| Investigate | Trace |
| --- | --- |
| A session update conflicted or an event went missing | [Session transactions and outbox](internals/transactions/sessions-and-outbox.md) |
| A process effect is uncertain | [Attempt identity, fences, and settlement](internals/transactions/attempts.md) |
| A runtime cancellation is pending after restart | [Durable cancellation controls](internals/transactions/runtime-cancellation.md) |
| An agent call or reply was duplicated, rejected, or is still pending | [Agent event and reply transactions](internals/transactions/agent-channel.md) |

## Reference

- [Inspect storage format and compatibility rules](reference/storage-format.md).

## Development

- [Exercise, verify, and recover a SQLite repository](development/maintenance.md).
- [Verify and extend the SQLite adapter](development/contributing.md).

[System architecture](../../../docs/architecture/README.md) · [Workspace code map](../../../docs/architecture/dependencies.md) · [Product documentation](../../../docs/README.md)
