# aixand-undercurrent-platform-windows

Use this crate when maintaining Windows OS primitives shared by Undercurrent's
CLI, daemon, runtime, local transport, and SQLite adapter. It contains checked
Win32 handle operations, private filesystem access, account-restricted named
pipes, process observation, and Job Object control.

This is an internal workspace library, version **0.1.0-beta**, compiled only under
`cfg(windows)`. Rust `pub` items allow sibling crates to use these primitives;
they do not make this a supported external SDK. The
[architecture policy](../../tests/architecture/allowed-graph.json) assigns it
`shared_native_os_primitives`, permits no internal dependencies, and identifies
`aixand-undercurrent` and `aixand-undercurrent-protocol` as the public Rust surfaces.

Choose a starting point:

| Reader or task | Start here |
| --- | --- |
| Product user: install and operate Undercurrent | [Product overview and first use](../../README.md) |
| SDK integrator: control an external daemon | [Rust SDK](../undercurrent-sdk/README.md) or [Python SDK](../../python/README.md) |
| Maintainer | [Implementation, contracts, and development guide](docs/README.md) |

For your first local change, follow [native setup and the private-record
example](docs/development/contributing.md). It publishes, checks, and removes
temporary state without a daemon, then points to focused tests. Windows behavior
must be checked on **native Windows x86_64 MSVC**; a successful non-Windows build
does not exercise this crate's implementation.

Start code investigation at [the crate exports](src/lib.rs); use generated
Rustdoc for individual items. Product lifecycle, request deduplication, session
recovery, protocol framing, ConPTY, and durable operation receipts belong to
the calling components, as mapped in [the ownership guide](docs/internals/ownership.md).
