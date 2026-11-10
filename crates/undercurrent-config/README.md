# undercurrent-config

Understand and change the configuration contract shared by the standalone CLI
and daemon. This crate owns typed settings, bounded YAML/TOML loading, shared
validation, and home-derived path discovery. Component adapters remain in the
CLI and daemon; loading configuration does not start either process.

This is an **internal shared-configuration crate**, version 0.1.0-beta, using
configuration schema 0.1.0-beta. Its public Rust items support workspace
callers. The [architecture policy](../../tests/architecture/allowed-graph.json)
designates `aixand-undercurrent` and `aixand-undercurrent-protocol` as the supported
external Rust surfaces. Schema generation, package version, and wire version
are separate identifiers.

| Reader and goal | Start here |
| --- | --- |
| User: validate a file or diagnose a rejected setting | [Inspect configuration without starting a daemon](../../docs/guides/configuration/diagnose.md) |
| SDK integrator: find the supported client boundary | [Client configuration boundary](docs/internals/loading.md#client-configuration-boundary), [Rust SDK sources](../undercurrent-sdk/src/lib.rs), and [Python package](../../python/README.md) |
| Maintainer | [Implementation, contracts, and development guide](docs/README.md) |

The small [inspection example](examples/inspect_config.rs) loads an explicitly
selected home and prints the redacted effective document. The
[walkthrough](../../docs/guides/configuration/diagnose.md#inspect-an-isolated-home) supplies
setup, expected results, and cleanup. It uses existing dependencies and needs
no daemon.

The [crate manifest](Cargo.toml) has no optional features or internal crate
dependencies. Build from the workspace root with the pinned
[Rust toolchain](../../rust-toolchain.toml). See the
[verification guide](docs/development/contributing.md#verification) for focused commands,
the warning-denied Rustdoc gate, and generated API documentation. Defaults and
field meanings live in [document.rs](src/document.rs); numeric schema ranges
live in [config.schema.json](../../schemas/config.schema.json).
