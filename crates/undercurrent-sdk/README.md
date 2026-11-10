# aixand-undercurrent

Use this crate to control and observe a running Undercurrent daemon from Rust.
The crates.io package name is `aixand-undercurrent`. After the version is published,
you can use a short dependency alias:

```toml
[dependencies]
undercurrent = { package = "aixand-undercurrent", version = "0.1.0-beta" }
```

This alias makes the API available as `undercurrent::`. The source directory and
internal library name remain `undercurrent-sdk` and `undercurrent_sdk`.

`ProductClient` is a blocking, cheaply clonable client: requests and subscriptions
share one local connection and one recovery supervisor. The `uc` CLI and Python
bridge use this same Rust boundary.

Its main responsibilities are:

- Typed product calls and stream subscriptions.
- Authenticated local IPC connections with mutual OS peer verification.
- Sharing one connection across requests and logical streams.
- Reconnection, recovery support, trace context, and client error reporting.

The SDK exposes [protocol](../undercurrent-protocol/README.md) data through its
client API and uses [wire](../undercurrent-wire/README.md) and
[multiplex](../undercurrent-multiplex/README.md) internally. Its concrete local IPC
client connector lives in this crate. It connects to a running external daemon;
there is no embedded runtime or implicit daemon launch.

SDK/package version **0.1.0-beta** targets Linux, macOS, and Windows. Building this
checkout requires Rust **1.95**. There is no network fallback, embedded daemon,
automatic mutation retry, or Rust async facade. The separately packaged
[Python API](../../python/README.md) is asynchronous.

| Reader and goal | Start here |
| --- | --- |
| User integrating an existing daemon | [Connect, query, and watch](../../docs/sdk/rust/connect-and-query.md) |
| SDK integrator implementing tasks | [Product operations](../../docs/sdk/rust/operations.md), [streams and recovery](../../docs/sdk/rust/streams-and-recovery.md) |
| SDK integrator checking a contract | [Public 0.1.0-beta boundary](PUBLIC_API.md), [limits and diagnostics](../../docs/sdk/rust/limits-and-diagnostics.md) |
| Maintainer | [Maintainer documentation](docs/README.md) |

For the wider process and state model, see the shared
[system architecture](../../docs/architecture/README.md). For ownership inside
the SDK, see the [client ownership diagram](docs/internals/client-runtime.md#runtime-ownership-and-concurrency).

Maintained examples are linked from their task guides. Behavioral checks live in
[client contract tests](tests/client_contract.rs),
[typed product tests](tests/product_client.rs), and
[real local transport tests](tests/local_boundary.rs).

[Undercurrent overview](../../README.md)
