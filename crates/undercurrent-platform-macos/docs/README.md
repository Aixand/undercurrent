# undercurrent-platform-macos maintainer documentation

Use these pages to change or debug the macOS process adapter. The
[crate overview](../README.md) describes its scope. This internal adapter
supplies bounded Darwin observations to the runtime. Verify native queries
on macOS and interpret their results as process evidence.

| Maintainer task | Reading path |
| --- | --- |
| Verify a change or add a bounded native query | [Work on the adapter](development/contributing.md) (task guide) |
| Find why an observation is missing, incomplete, or rejected | [Diagnose process evidence](development/diagnosing-observations.md) (task guide) |
| Locate the Rust wrappers, C listener queries, and runtime caller | [Source map](internals/runtime-integration.md#dependencies-and-source-map) |
| Trace query results into a foreground observation | [Follow evidence into the runtime](internals/runtime-integration.md) (explanation) |
| Check accepted native replies, bounds, errors, or buffer lifetime | [Darwin query contracts](reference/query-contracts.md) (reference) |

For a first change, run the self-query check in the development guide, read the
contract for the affected query, then trace its caller in the runtime guide.
For system context, use the shared [process ownership diagram](../../../docs/architecture/README.md#process-boundaries)
and [workspace code map](../../../docs/architecture/dependencies.md).

[Product documentation](../../../docs/README.md)

## Package and reader scope

Only `undercurrent-runtime` may depend on this crate under the
[architecture policy](../../../tests/architecture/allowed-graph.json).
The adapter queries process identity, group membership, argument bytes,
working directory, executable path, and TCP listeners. In hosted foreground
sampling, the session host runtime owns the sampling cache, process, and PTY.
The adapter owns temporary buffers. Queries use bounded storage and iteration
and block until the OS returns, without a wall-clock deadline or cancellation.
A listener observation alone does not establish readiness.

Native verification requires macOS, the pinned Rust toolchain, and a macOS
SDK/C compiler. Linux and Windows builds cannot exercise Darwin queries.
The daemon-free self-query leaves no persistent runtime state. Package version
`0.1.0-beta` identifies the Cargo package. Wire and product contracts have
separate versions.
