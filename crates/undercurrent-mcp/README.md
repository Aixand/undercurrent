# undercurrent-mcp

`uc-mcp` is a local stdio MCP server using the blocking public Rust SDK. It owns
tool presentation, one lazily connected client, bounded request/wait admission,
and finite observations. It never starts a daemon or directly accesses runtime
storage. See [setup and contracts](../../docs/agent/mcp.md).

`params.rs` owns assistant-facing schemas; `server.rs` owns MCP dispatch and
limits; `operations.rs` translates typed operations and manages watch lifetimes;
`client.rs` owns connection setup. No finite mutation is automatically retried.
Each tool can work without an installed skill. The optional skills own higher
level process and worker workflows.

Validate with `cargo test --locked -p undercurrent-mcp` and, after building the
native binaries, `python3 tests/conformance/mcp_test.py`. Native installation
verification also exercises this adapter against its own disposable daemon.
