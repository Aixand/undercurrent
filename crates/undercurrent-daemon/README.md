# undercurrent-daemon

Run and maintain the external Undercurrent service that connects durable session
state, process supervision, and authenticated local clients. This crate composes
application services with the runtime, SQLite, and transport adapters; it does
not own workflow scheduling or interpret a process's output as a workflow result.

The package builds `uc-daemon` and the required private helper `uc-session-host`.
Keep both from the same build together. The daemon library is an internal
composition surface, even where Rust symbols are `pub`. The
[architecture policy](../../tests/architecture/allowed-graph.json) identifies
only the [SDK](../undercurrent-sdk/README.md) and
[protocol](../undercurrent-protocol/README.md) as supported external Rust surfaces.

| Reader | Start here |
| --- | --- |
| Operator | [Daemon operating guides](../../docs/guides/daemon/README.md): start, inspect, diagnose, and stop one installation. |
| SDK integrator | [Choose a supported SDK](../../docs/sdk/README.md) and [recover after a lost response](../../docs/sdk/recovery.md). |
| Maintainer | [Daemon documentation](docs/README.md): find local mechanisms, ownership, and focused checks. |

For process boundaries and persisted state, read the [system architecture](../../docs/architecture/README.md).
The public client channel and private session-host channel have separate contracts.

Implementation entry points are [composition](src/composition.rs),
[configuration](src/config.rs), and [product adapters](src/application_adapter/mod.rs).
Focused evidence includes [startup recovery](tests/startup_recovery.rs),
[hosted-process recovery](tests/hosted_process_recovery.rs), and
[product dispatch](tests/product_dispatch.rs). Package version is `0.1.0-beta`;
configuration, product schema, and wire versions are separate contracts.

[Product overview](../../README.md)
