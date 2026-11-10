# undercurrent-application

Coordinates session use cases across pure domain decisions, durable product
state, and process/runtime effects. Maintainers use this crate to change
command ordering, recovery, prepared reads, observations, and the ports that
infrastructure implements.

This is an **internal application layer**, version 0.1.0-beta. Rust `pub` visibility
supports workspace composition; the
[architecture policy](../../tests/architecture/allowed-graph.json) designates
only the SDK and protocol crates as external Rust library surfaces.

| Reader | Start here |
| --- | --- |
| Users operating Undercurrent | [Product overview and first use](../../README.md), [CLI orientation](../undercurrent-cli/README.md). |
| SDK integrators | [Rust SDK](../undercurrent-sdk/README.md), [Python package](../../python/README.md), [product protocol](../undercurrent-protocol/README.md). |
| Maintainer | [Implementation, contracts, and development guide](docs/README.md) |

## Maintainer reading paths

The [local documentation index](docs/README.md) groups session behavior,
execution and runtime controls, observations, and recovery. Use its
[diagnostics](docs/README.md#diagnostics) and
[development](docs/README.md#development) sections to investigate failures and
choose checks.

The [source root](src/lib.rs) exposes modules and re-exports;
[ports.rs](src/ports.rs) defines the central adapter contracts. The crate depends
only on [domain](../undercurrent-domain/README.md).
[Runtime](../undercurrent-runtime/README.md) and
[SQLite storage](../undercurrent-store-sqlite/README.md) implement ports;
[daemon composition](../undercurrent-daemon/README.md) selects adapters and
connects requests and background workers. See [application ownership](docs/internals/ownership.md) for local port boundaries
and links to the shared dependency and process diagrams.

## First maintainer check

Start with the [focused verification loop](docs/development/contributing.md#run-a-focused-verification-loop).
It gives repository-root commands, prerequisites, expected results, and the
limits of fake-port tests. Lifecycle tests exercise ordering without a daemon;
the [output-gap example](docs/internals/sessions/terminal-io.md#run-an-in-memory-retention-example)
shows retention recovery using synthetic bytes.
