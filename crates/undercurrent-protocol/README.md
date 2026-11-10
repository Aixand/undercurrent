# aixand-undercurrent-protocol

Use this crate to construct and interpret Undercurrent's public operations,
results, errors, and streams. It owns product DTOs, registries, contract checks,
bounded codecs, and canonical digests. It performs no connection, process, or
storage IO. The [Rust SDK](../undercurrent-sdk/README.md) carries these values to
the daemon; the daemon validates and translates them into application commands.

This is a supported external Rust surface alongside `aixand-undercurrent`, as
specified by [architecture policy](../../tests/architecture/allowed-graph.json).
The crate is `0.1.0-beta`; the current product contract is **exactly `0.1.0-beta`**, schema
generation **`1`**. Neither is the UCMP transport version.

| Reader | Start here |
| --- | --- |
| Users operating sessions | [Product overview](../../README.md) and [Python setup and tasks](../../python/README.md); use [recovery guidance](../../docs/reference/protocol/recovery.md) to interpret uncertain results. |
| SDK integrators | [Encode and inspect a payload](../../docs/sdk/rust/first-payload.md), then [encoding and validation](../../docs/reference/protocol/encoding.md), [operation reference](../../docs/reference/protocol/operations.md), and [streams and output](../../docs/reference/protocol/streams.md). |
| Maintainer | [Implementation, contracts, and development guide](docs/README.md) |
| Attached-runtime integrators | [Attempt status and repair](../../docs/reference/protocol/recovery.md), [exact cancellation](../../docs/reference/protocol/cancellation.md), and [agent event exchange](../../docs/reference/protocol/agent-events.md). |

The [workspace dependency map](../../docs/architecture/dependencies.md) shows
why consumers can use these types without importing daemon internals. Public
agent-bridge DTOs describe a [private bridge](../../docs/reference/protocol/agent-events.md);
they do not make it a consumer transport.

For symbol-level contracts, use the [Rustdoc generation instructions](../../docs/reference/protocol/README.md#generate-symbol-level-documentation).
For ownership, source entry points, and relevant tests, start at the
[maintainer documentation](docs/README.md).
