# aixand-undercurrent-transport-core

Understand and change the daemon's transport-independent connection engine here.
This internal crate owns bounded request and stream admission, response
correlation, outbound scheduling integration, replay integration, active
connection diagnostics, and an in-memory mutation-result cache. It does not
listen on sockets, verify OS peers, decode product operations, execute processes,
or commit application state.

The [connection ownership diagram](docs/internals/ownership.md#runtime-ownership)
shows how the adapter, core session and shared replay journal fit together.

| Reader | Start here |
| --- | --- |
| Maintainer | [Implementation, contracts, and development guide](docs/README.md) |
| SDK integrators | Use the supported [Rust SDK](../undercurrent-sdk/README.md) and [product protocol](../undercurrent-protocol/README.md), or the [Python SDK](../../python/README.md). Core's `pub` items are internal composition seams. |
| Users | Use the [product entry point](../../README.md) for CLI and daemon tasks. This crate has no standalone service or installation step. |

Scope: crate version **0.1.0-beta**, workspace Rust **1.95.0**, no crate feature flags.
The Undercurrent wire version and product contract version are separate; see
[composition prerequisites](docs/internals/ownership.md#composition-prerequisites).
The [architecture policy](../../tests/architecture/allowed-graph.json) names this
crate `transport_independent_server_engine` and lists only protocol and SDK as
supported external Rust library surfaces.
