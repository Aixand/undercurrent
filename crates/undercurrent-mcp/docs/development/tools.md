# Change and verify an MCP tool

Use this guide to change how `uc-mcp` maps a tool call onto the blocking SDK.
For caller-visible schemas, stop modes, and recovery guidance, start with the
[MCP consumer guide](../../../../docs/agent/mcp.md).

## Trace a tool call

| Phase | Source | What to preserve |
| --- | --- | --- |
| Start stdio | [main.rs](../../src/main.rs) | Resolve the configured home and request budget. Create the connection holder without dialing. Keep stdout reserved for MCP messages. |
| Discover tools | [server.rs](../../src/server.rs), [params.rs](../../src/params.rs) | Generate input schemas from argument types. Keep output schemas, annotations, and the same filtered registry for listing and lookup. Discovery works without a daemon. |
| Admit a call | [server.rs](../../src/server.rs) | Reject disabled names and oversized argument objects. Acquire the appropriate permit before dispatching onto a blocking worker. Keep the permit until the worker exits. |
| Translate and connect | [operations.rs](../../src/operations.rs), [client.rs](../../src/client.rs) | Deserialize arguments and validate adapter constraints before dialing. Lazily connect one shared SDK client for the local home. Preserve native endpoint spelling for Windows pipe identity. |
| Return a result | [operations.rs](../../src/operations.rs), [server.rs](../../src/server.rs), [client.rs](../../src/client.rs) | Match the typed SDK response, project data, and preserve available product error category, disposition, and effect certainty. Bound the encoded result and retain safe target identifiers for reconciliation. |

`get_status` probes the daemon and can return `connected: false`. Discovery
only reports the adapter's tool inventory. Server teardown closes the shared SDK connection. Managed sessions continue
under the daemon.

## Admission and cancellation

| Guard | Scope |
| --- | --- |
| Stdio line and result size | The input line limit is 262,144 bytes. The result check reserves 4,096 bytes below that limit and counts the structured result and its compatibility text together. An oversized result can replace an already settled mutation response. |
| Tool arguments | Encoded argument objects above 65,536 bytes fail before worker admission. |
| Worker capacity | Eight ordinary calls and four waits use separate semaphores. Saturation rejects the call before SDK submission. |
| SDK timing | The SDK profile sets a 3-second local connect timeout and a 3-second handshake response timeout. A finite SDK call receives a 10-second response budget. Connection acquisition has its own timing. |
| State wait | The 1–60 second observation budget begins after connection acquisition. Subscription opening uses the remaining budget capped at 10 seconds. Once open, receives poll at most 100 milliseconds at a time. |

The request context supplies a cancellation token. Dropping the handler also
signals the blocking worker through a token guard. The mutation path checks
cancellation before connection acquisition and again before calling the SDK.
Once submitted, a mutation can take effect even if the caller loses the reply.
Reconcile using the target and any available effect certainty before retrying.
The adapter leaves finite mutations unreplayed.

A wait opens one session watch and closes it on exit through the `Watch` drop
guard. The worker checks cancellation around receives after activation. It
returns an observation error on a gap because continuity was lost. Closing
that watch leaves the process running. Connection acquisition and subscription
activation can delay cancellation handling.

## Extend the adapter

1. Update the typed arguments and the tool registration together. Check that
   read-only discovery and direct lookup both hide any new mutation.
2. Map the tool to a typed SDK operation. Validate adapter-specific constraints
   before `Connection::get`. On Windows, default or explicit `graceful_only`
   stop is rejected before connection with `unsupported_mode` and known-absent
   effect. Use the [consumer stop modes](../../../../docs/agent/skills/undercurrent/references/mcp-control.md)
   when changing this mapping.
3. Update the output schema and result projection together. For output pages,
   preserve byte continuity at `to_offset`. The `next_offset` field is the producer head.
   Keep request bodies and private payloads out of error target metadata.
4. Check the relevant behavior below and update the consumer contract if
   caller-visible behavior changes.

## Verify a change

Run from the repository root with the [workspace Rust toolchain](../../../../rust-toolchain.toml)
and a native compiler. The unit tests use local fixtures and require no running daemon.

```sh
cargo test --locked -p undercurrent-mcp
```

| Change | Evidence to inspect |
| --- | --- |
| Discovery, read-only mode, or encoded size | [Server tests](../../src/server.rs) check mutation lookup and the actual encoded result bound. |
| Arguments, cancellation, or projection | [Operation tests](../../src/operations.rs) check validation and cancellation before submission, output byte continuity with split UTF-8, and native Windows graceful-stop rejection. |
| Native endpoint selection | [Connection test](../../src/client.rs) compares the exact native endpoint spelling with shared config paths. |

For changes that cross stdio, local IPC, or the daemon boundary, use Python
3 with the matching native binaries. The conformance probe creates a temporary
runtime home and starts and stops its own daemon. Replace `target/debug` if
Cargo uses a different target directory.

```sh
cargo build --locked -p undercurrent-mcp -p undercurrent-cli -p undercurrent-daemon
python3 tests/conformance/mcp_test.py --binary-dir target/debug
```

Expect passing unit tests and a JSON summary from the native probe. If the
probe cannot connect, check the binary directory and the daemon startup output
reported by the harness. The [conformance harness](../../../../tests/conformance/mcp_test.py)
and [native workflow](../../../../.github/workflows/platform-contracts.yml)
define the test scope. Record results for the target and revision exercised.

[Adapter documentation](../README.md)
