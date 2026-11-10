# aixand-undercurrent-wire

Use this crate to maintain the bounded frames exchanged between the Undercurrent
client and daemon. It owns UCMP binary messages, UCHS connection-handshake
encoding, negotiation values, identifiers, trace context, and ordering scalars.
It performs no transport I/O and owns no sessions, queues, replay journals, or
durable state.

This is an **internal** crate (`internal_ucmp_uchs_primitives` in the
[architecture policy](../../tests/architecture/allowed-graph.json)). Its Rust
`pub` exports support workspace composition; they are not a supported external
library surface. See the [version reference](docs/reference/scalars.md#versions)
when checking package, connection, or encoding compatibility.

| Reader | Starting point |
| --- | --- |
| Users operating the daemon | [Product overview and CLI tasks](../../README.md) |
| SDK integrators | [Supported Rust SDK](../undercurrent-sdk/README.md) and [Python SDK](../../python/README.md) |
| Maintainer | [Implementation, contracts, and development guide](docs/README.md) |

## Build and verify

[Run the in-memory codec example](docs/development/contributing.md#run-the-example)
to exercise negotiation, fragmented input, and binary stream credit without a
daemon. The guide includes setup, expected output, diagnosis, and focused tests.

Use the [maintainer documentation](docs/README.md) to find the owning source,
exact frame and scalar contracts, and local mechanisms.

[Undercurrent overview](../../README.md)
