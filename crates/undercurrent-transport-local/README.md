# aixand-undercurrent-transport-local

This internal crate connects the daemon's public client boundary to protected
local OS byte streams. Maintainers use it to maintain endpoint security,
bounded framing, connection workers, and listener shutdown. The architecture
policy assigns it the `protected_local_ipc` role; Rust `pub` items and transport
core re-exports are internal composition surfaces, not supported external SDK APIs.

| Reader | Start here |
| --- | --- |
| Product users | [Product setup and CLI](../../README.md); [operator troubleshooting](../../docs/guides/troubleshooting.md) |
| SDK integrators | [Rust SDK](../undercurrent-sdk/README.md) or [Python async package](../../python/README.md); [boundary distinctions](docs/internals/ownership.md#client-boundaries) |
| Maintainer | [Implementation, contracts, and development guide](docs/README.md) |

The current package is version **0.1.0-beta**, using workspace Rust **1.95** and
edition **2024**. Linux and macOS use Unix domain sockets; Windows uses local
named pipes. These are implemented adapters, with separate native test
requirements. There are no crate-specific feature flags and no TCP, TLS,
WebSocket, token-file, or browser fallback. The local OS account is the trust
boundary; this crate does not isolate mutually untrusted processes under that
account or protect against an administrator controlling the machine.

The SDK supplies client I/O and the daemon supplies an authenticated dispatcher
to `LocalServer`. Connection semantics belong to
[transport core](../undercurrent-transport-core/README.md), wire codecs to
[wire](../undercurrent-wire/README.md), and scheduling/replay to
[multiplex](../undercurrent-multiplex/README.md). Product execution, durable
session state, and private session-host or agent IPC are outside this crate.

The [manifest](Cargo.toml) and
[architecture policy](../../tests/architecture/allowed-graph.json) define the
package boundary. Generated Rustdoc provides symbol lookup; the linked pages
explain the behavior and its limitations.
