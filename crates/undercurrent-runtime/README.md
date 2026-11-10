# undercurrent-runtime

Connects Undercurrent's application services to processes, pseudoterminals
(PTYs), clocks, and operating-system observations. Maintainers use this crate
to implement process mechanics and the private `uc-session-host` helper.

Its main responsibilities are:

- Preparing and verifying launches, spawning processes, and managing process hosts.
- Delivering terminal input, reading output, and maintaining terminal screens.
- Observing foreground processes, evaluating health checks, and matching profiles.
- Providing bounded coordination, event, restart, and telemetry adapters.

This crate implements [application ports](../undercurrent-application/README.md)
using [domain values](../undercurrent-domain/README.md). The
[daemon](../undercurrent-daemon/README.md) composes these adapters and packages the
private `uc-session-host` helper. Runtime owns process and PTY mechanics; SQLite
owns durable storage, and public clients reach the system through the daemon.

This is a **daemon-private** crate, not a supported consumer library, even where
Rust symbols are `pub`. The [architecture policy](../../tests/architecture/allowed-graph.json)
permits external Rust consumers to depend on the protocol and SDK instead.
Package version 0.1.0-beta does not identify the private host wire or receipt format.

See the [session-host ownership view](docs/internals/host/session-host.md#locate-processes-and-state)
for process boundaries, stores, journals, and one-shot execution, and the
[system architecture](../../docs/architecture/README.md) for the product model.

| Reader | Starting point |
| --- | --- |
| Users operating sessions | [Product setup and CLI tasks](../../README.md). |
| SDK integrators | [Rust SDK](../undercurrent-sdk/README.md) or [Python SDK](../../python/README.md). |
| Maintainer | [Implementation, contracts, and development guide](docs/README.md) |

## Contribute a runtime change

Start with the [local documentation index](docs/README.md) to find the owning
mechanism, its contract, and focused verification. Keep implementation changes
inside the application-port boundary described above.

The [crate manifest](Cargo.toml) declares the current platform dependencies and
the workspace requires Rust 1.95. Native process tests use OS-specific fixtures;
pure adapter tests are separate. Runtime mechanisms do not supply authorization,
public transport, SDK behavior, or application-level success guarantees. Durable
session transactions belong to the SQLite adapter; process-host receipts have
their own narrower effects and retention contracts.

[Undercurrent overview](../../README.md)
