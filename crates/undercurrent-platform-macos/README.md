# undercurrent-platform-macos

This daemon-private adapter supplies bounded, read-only Darwin process evidence:
identity, process-group membership, argument bytes, working directory,
executable path, and TCP listeners. Runtime maintainers use it to keep native
pointers and SDK structures inside one small boundary.

The [architecture policy](../../tests/architecture/allowed-graph.json) permits
only `undercurrent-runtime` to depend on it. Rust `pub` enables those internal
calls; supported external Rust libraries are `aixand-undercurrent` and
`aixand-undercurrent-protocol`.

## Choose a starting point

| Reader and goal | Start here |
| --- | --- |
| Maintainer: change or debug the adapter | [Maintainer documentation](docs/README.md) |
| User: operate sessions through the product | [Undercurrent overview](../../README.md), [CLI entry point](../undercurrent-cli/README.md) |
| SDK integrator: control an external daemon | [Rust SDK](../undercurrent-sdk/README.md), [Python SDK and its platform scope](../../python/README.md) |

## Where the queries run

The adapter runs inside its caller. In hosted foreground observation, the
session host's native runtime calls it and owns the sampling cache, process,
and PTY. The adapter owns only temporary query buffers. See the
[system process diagram](../../docs/architecture/README.md#process-boundaries)
and the [local sampling flow](docs/internals/runtime-integration.md#collect-one-foreground-observation).

## Platform and first verification

The library and its tests require **macOS**, the pinned Rust toolchain, and a
macOS SDK/C compiler. Package `0.1.0-beta` is a Cargo version, not a wire or
product-contract version. A Linux or Windows build does not exercise Darwin
queries.

Start with the [self-query check](docs/development/contributing.md#2-run-the-self-query-test);
it needs no daemon and creates no persistent runtime state. The same guide
covers build prerequisites, native test coverage, and cleanup.

Queries block until the OS returns. Storage and iteration are bounded, but
there is no wall-clock deadline or cancellation mechanism. Read the
[query contracts](docs/reference/query-contracts.md) before interpreting partial
or unavailable evidence, or treating a listener as readiness.
