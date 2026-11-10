# aixand-undercurrent-multiplex

Understand and maintain the bounded queues that let finite requests and logical
streams share an Undercurrent connection. This internal crate owns client
correlation, server scheduling, byte credit, and in-memory stream replay. It
performs no socket I/O, starts no threads, and owns no session or database state.

The [architecture policy](../../tests/architecture/allowed-graph.json) classifies
it as `internal_ucmp_scheduling`. Its Rust exports support first-party adapters;
they are not a supported external SDK. Package version `0.1.0-beta` is distinct from
the [UCMP wire version](../undercurrent-wire/src/version.rs).

| Reader | Start here |
| --- | --- |
| Users operating sessions | [Product overview and first use](../../README.md) |
| SDK integrators | [Rust SDK](../undercurrent-sdk/README.md) or [Python package](../../python/README.md) |
| Maintainer | [Implementation, contracts, and development guide](docs/README.md) |

Start local development with the
[daemon-free bounded delivery exercise](docs/development/contributing.md#run-the-local-exercise).
It includes setup, a complete command, expected output, and cleanup for the
[maintained example](examples/bounded_delivery.rs), which checks admission,
credit exhaustion, consumption, and replay.
The [contract tests](tests/multiplex_contract.rs) are the main regression suite.
