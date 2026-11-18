# undercurrent-python-bridge maintainer documentation

The bridge owns the private JSON-lines subprocess boundary. Python owns caller
correlation and buffering, and the Rust SDK owns authenticated local IPC and
reconnection. The [crate overview](../README.md) links to a first connection check.

## Package and reader scope

The binary declares no public Rust library or crate features. Use matching
Python and bridge revisions for the private stdio protocol. The
[architecture policy](../../../tests/architecture/allowed-graph.json) assigns
the bridge `python_public_client_adapter`. The bridge leaves daemon startup,
session ownership, database access, and private session-host and agent IPC to
their respective owners. See the [Python process view](../../../python/docs/README.md#connection-ownership).

The [connection check](development/contributing.md#prepare-the-executable-and-python-import)
uses [inspect_runtime.py](../examples/inspect_runtime.py) with a matching daemon
already running. It lists sessions, including an empty list, and closes the
bridge while leaving sessions running. [Bridge main](../src/main.rs) owns
startup, dispatch, workers, and shutdown. [Envelope output](../src/output.rs)
serializes stdout and guards terminal frames. [Python transport](../../../python/src/undercurrent/_transport.py)
owns the subprocess.

## Internals

- [Locate command handling and envelope output](internals/operation-flow.md#code-map).
- [Trace a Python operation through the bridge](internals/operation-flow.md).
- [Diagnose failures and reconcile uncertain effects](internals/recovery.md).

## Reference

- [Look up the private stdio protocol](reference/stdio-protocol.md).

## Development

- [Choose the owner of a change](development/contributing.md#extend-the-correct-owner).
- [Select bridge unit tests and cross-boundary checks](development/contributing.md#verify-a-change).
- [Work on the Python SDK](../../../python/docs/README.md).

[System architecture](../../../docs/architecture/README.md) · [Workspace code map](../../../docs/architecture/dependencies.md) · [Product documentation](../../../docs/README.md)
