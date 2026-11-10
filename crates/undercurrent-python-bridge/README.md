# undercurrent-python-bridge

`uc-python-bridge` connects the async Python package to an external Undercurrent
daemon through the public, blocking Rust SDK. Maintainers use this crate to
maintain the subprocess boundary: JSON-lines framing, request normalization,
bounded request workers, and acknowledged session streams.

This binary package has no public Rust library or feature switches. Its private
stdio protocol belongs to the matching Python package and bridge revision.
The [architecture policy](../../tests/architecture/allowed-graph.json) classifies
it as `python_public_client_adapter`; supported external Rust library surfaces
are `aixand-undercurrent` and `aixand-undercurrent-protocol`.
The bridge does not start the daemon, own sessions, open its database, or speak
private session-host or agent-bridge IPC.

See the [Python process-boundary diagram](../../python/README.md) for how the
subprocess reaches the daemon, and [local operation flow](docs/internals/operation-flow.md)
for reader and worker ordering.

## Choose a starting point

| Reader | Start here |
| --- | --- |
| Maintainer | [Build, trace, and debug the adapter](docs/README.md) |
| Python user | [Python setup and operations](../../python/README.md) |
| SDK integrator | [Python SDK guides](../../docs/sdk/python/README.md) and [Rust public SDK](../undercurrent-sdk/README.md) |

## First connection check

Follow [the build and connection check](docs/development/contributing.md#prepare-the-executable-and-python-import)
from the repository root. It uses the maintained
[`inspect_runtime.py`](examples/inspect_runtime.py) example with a matching,
already running daemon. A successful check lists sessions, including an empty
list, and closes the bridge while leaving those sessions running. The guide
includes prerequisites, runtime-home selection, expected failures, and cleanup.

## Source and verification

All Rust behavior is in [src/main.rs](src/main.rs). Its caller is
[Python's private transport](../../python/src/undercurrent/_transport.py);
[ProductClient](../undercurrent-sdk/src/product.rs) owns typed SDK interaction.
There are currently no bridge-local Rust tests. The
[verification map](docs/development/contributing.md#verify-a-change) distinguishes Python
mock tests, SDK and daemon tests, and remaining bridge integration coverage.

[Repository overview](../../README.md)
