# undercurrent-domain

Use this crate to understand and change the invariants of a managed session.
It owns validated values, session and attempt transitions, restart decisions,
foreground evidence selection, health aggregation, profiles, and declared work.
Callers supply time, identities, limits, and evidence; the crate performs no I/O.

This is an **internal maintainer surface**, despite its Rust `pub` exports.
The [architecture policy](../../tests/architecture/allowed-graph.json) supports
external Rust consumers through `aixand-undercurrent` and `aixand-undercurrent-protocol`.
These pages describe domain package **0.1.0-beta** at this checkout; the detection
profile schema is version **3**, a separate version from product and wire contracts.

Among workspace crates, domain depends only on
[foundation](../undercurrent-foundation/README.md). The
[workspace dependency map](../../docs/architecture/dependencies.md) shows its
callers and their other dependencies. Domain owns no transport, database, or OS
adapter.

Maintainers can start with the
[in-memory model walkthrough](docs/development/contributing.md#run-the-model-without-a-daemon)
using the pinned Rust toolchain; it needs no running daemon.

| Reader goal | Start here |
| --- | --- |
| Use Undercurrent as an operator | [Product overview and CLI entry points](../../README.md) |
| Integrate a client | [Rust SDK](../undercurrent-sdk/README.md), [Python package](../../python/README.md), and [public protocol](../undercurrent-protocol/README.md) |
| Maintainer | [Implementation, contracts, and development guide](docs/README.md) |

The [source exports](src/lib.rs) lead to symbol-level contracts. The
[contract tests](tests/domain_contract.rs) and focused suites listed in the
maintainer guide are the primary local regression evidence.
