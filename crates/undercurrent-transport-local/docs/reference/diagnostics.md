# Interpret local server errors and diagnostics

Use this reference to interpret a failure or a listener report without
exposing endpoint or payload details. For the investigation procedure, return
to [troubleshooting](../development/troubleshooting.md).

## Error categories and emitted mappings

[error.rs](../../src/error.rs) owns `LocalServerErrorCategory`, `LocalServerStage`,
and `LocalServerError`. The category set is `Configuration`, `Authentication`,
`Security`, `Protocol`, `Timeout`, `Capacity`, `Transport`, and `Internal`.
Stages are `Listener`, `Authenticate`, `Negotiate`, `ComposeConnection`,
`Dispatch`, `Write`, `Shutdown`, and `SpawnWorker`. These are available labels,
not a promise that each label is emitted by every relevant failure.

Current mappings in [server.rs](../../src/server.rs) matter when debugging:

| Failure | Observed mapping in the code |
| --- | --- |
| Native bind / accept / Tokio listener conversion | `Transport` at `Listener`; bind/accept/conversion retain I/O kind. Other wrappers do not consistently populate it. |
| Accepted peer verification | Counts as `Rejected` with no retained issue or authentication error detail. |
| First-hello read deadline | `Timeout` at `Negotiate`. |
| Handshake response write or close timeout | `Transport` at `Negotiate`, with timeout source; not `Timeout`. |
| UCMP connection error | Core configuration/protocol/overloaded/unavailable/invariant categories map to configuration/protocol/capacity/transport/internal at `Dispatch`. |
| Idle timeout / active send timeout | `Timeout` at `Dispatch` / `Write`. |
| Active read / send I/O failure | `Transport` at `Dispatch` / `Write`. |
| Replay confirmation failure after send | `Internal` at `Write`. Bytes may already be visible to the peer. |
| Writer/cleanup-supervisor creation, cleanup panic, or writer/worker join failure | `Internal` at `SpawnWorker`; task join failure uses ordinal `u64::MAX` in the report. |

## Safe logging

`Display` exposes category and stage; `Debug` also shows optional I/O kind and
only a redacted marker for a retained source. `Error::source()` still exposes
the concrete error to trusted diagnostics and can contain private text. Do
not recursively format that chain into public logs. Equality compares category,
stage, and I/O kind, not source identity or source text.

## Live pressure and diagnostic pages

`pressure()` samples registry pressure, worker/cleanup ownership count, replay pressure,
and admission flag. These readings are not an atomic system snapshot.
`connection_workers` includes connections transferred to native cleanup and
drops only after that ownership is released. Registered clients remain reserved
through application cleanup and writer join. `accepting` can be true before
`run` and does not establish liveness.
`connection_registry()` provides transport-core bounded diagnostic pages;
concurrent changes can appear between pages. Product diagnostics consume these
through [daemon runtime diagnostics](../../../undercurrent-daemon/src/runtime_diagnostics.rs).

## Run report counters

On a normal `run` return, `LocalServerRunReport` contains saturating counters:

| Field | Interpretation |
| --- | --- |
| `accepted` | Native streams accepted, including those immediately rejected before a worker starts. |
| `completed` | Workers returning `Completed`, including shutdown/recycle during a silent handshake. It is not a count of executed operations or necessarily negotiated clients. |
| `rejected` | Semaphore, peer identity, or successfully sent setup rejection outcomes. A failed rejection write counts as failure instead. |
| `failed` / `issue_count` | Recorded failed worker outcomes, including task join failures. Both increment together on these paths. Fatal listener errors drain connection cleanup, then return the original `Err` rather than a partial run report. |
| `issues` | First failures observed up to `maximum_retained_issues`, with only connection ordinal, category, and stage. Completion order can differ from accept order. |

[Crate documentation](../README.md)
