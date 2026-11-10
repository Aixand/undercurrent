# undercurrent-store-sqlite

Understand and maintain the daemon's durable session and operation evidence.
This internal adapter implements application persistence ports with SQLite;
it does not run processes or serve client connections. Its Rust `pub` items
support daemon composition and repository tests, not an external SDK contract.
The [architecture policy](../../tests/architecture/allowed-graph.json) names its
role `daemon_private_store_adapter`. The package is version 0.1.0-beta and sets
`publish = false`; it is maintained as an internal workspace crate.

The repository atomically writes session aggregates, outbox events, and event
snapshots. It also retains fenced operation attempts, cancellation controls,
and agent events/replies. A bounded worker owns each connection. Separate
maintenance functions verify, back up, and restore database files.

Choose a starting point:

| Reader or task | Start here |
| --- | --- |
| Product user | [Undercurrent overview](../../README.md) and [CLI](../undercurrent-cli/README.md) |
| SDK integrator | [Rust SDK](../undercurrent-sdk/README.md) or [Python SDK](../../python/README.md); neither accesses SQLite directly |
| Maintainer | [Implementation, contracts, and development guide](docs/README.md) |

From the workspace root, with the pinned Rust 1.95.0 toolchain and dependencies
available, run the isolated example:

```console
cargo run --locked -p undercurrent-store-sqlite --example repository_roundtrip
```

It creates, changes, reopens, backs up, and restores a temporary repository,
checks the results, then removes its files. No daemon or child process is needed.
See the [example instructions](docs/development/maintenance.md#exercise-a-temporary-repository)
for expected output and failure handling. The crate uses bundled SQLite and
declares no optional features; platform details are in the
[maintenance guide](docs/development/maintenance.md#filesystem-and-platform-behavior).

The [code map](docs/internals/ownership.md#code-map) links each owned module and its
tests. Contract ownership remains in
[application](../undercurrent-application/README.md) and
[domain](../undercurrent-domain/README.md); the
[daemon](../undercurrent-daemon/README.md) owns composition and configuration.
