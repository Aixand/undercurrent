# Server limits and deadlines

Use this reference to validate a server configuration or identify which wait
can expire. The authority is `LocalServerLimits::new` and the timer placement
in [server.rs](../../src/server.rs); there are no adapter defaults.

## Constructor constraints

The daemon maps configured values in
[config_transport.rs](../../../undercurrent-daemon/src/config_transport.rs).
Nested scheduler, replay, diagnostic, and policy constructors own their own
validation. Keep the limits paired with the policy used to construct them:
`bind` rechecks only the policy frame maximum against `maximum_message_bytes`.

| Input or relationship | Validation / role |
| --- | --- |
| `handshake_frame_bytes` | Must construct a `HandshakeCodec`; complete-frame allocation bound before negotiation. Failure: `InvalidHandshakeMaximum`. |
| `handshake_timeout`, `write_timeout` | Nonzero or `ZeroDuration { field }`. Exact timer scopes are below. |
| `maximum_message_bytes` | Nonzero adapter policy ceiling, otherwise `ZeroLimit { field }`. Must also contain the maxima below. |
| `maximum_connections` | Nonzero physical-worker and registry bound, otherwise `ZeroLimit { field }`. Includes handshakes and connections still owned by cleanup. |
| `maximum_retained_issues` | Nonzero run-issue retention bound, otherwise `ZeroLimit { field }`. |
| `writer_queue_messages` | Nonzero message count for the bounded writer bridge channel, otherwise `ZeroLimit { field }`. |
| Message bound versus handshake / policy | Must contain both maxima or `MessageBelowHandshake` / `MessageBelowPolicy`. |
| Offered connection queue bytes | At least four maximum frames or `ConnectionQueueBelowClassReservations`. Reserves a frame for each traffic class. |
| Offered stream queue bytes | At least one maximum frame or `StreamQueueBelowFrame`. |
| Replay maximum item bytes | At least one maximum frame or `ReplayItemBelowFrame`. |
| `scheduler_messages` | Explicit message-count limits accompany byte bounds. |
| `replay` | Bounds stream journals, bytes, and retained item size in memory. |
| `diagnostics` | Bounds connection and stream diagnostic pages. |

## Scheduler byte budgets

For negotiated frame size `F` and connection queue `Q`, `scheduler_limits`
requires `Q >= 4F` and stream queue `>= F`. It computes
`u = floor((Q - 4F) / 8)` and class budgets
`[F + 2u, F + 2u, F + u, F + 3u]` in control, lifecycle, state, bulk order.

The base `F` reservation gives each class room for one maximum frame. The 2:2:1:3
remainder split allocates queue bytes; it is separate from the multiplex
[8:4:2:1 frame-selection schedule](../../../undercurrent-multiplex/docs/internals/scheduling.md#selection-ordering-and-fairness).
Neither ratio establishes byte throughput or latency. The exact remainder and
burst constants are current adapter policy, not negotiated guarantees.
The 2026-10-05 decision retains them as heuristics: keep a frame available in
every class, give bulk more queue space for its larger payloads, and constrain
one stream's burst while allowing it to proceed when no peer is writable.
The [scheduler workload probe](../../../undercurrent-multiplex/docs/development/contributing.md#measure-scheduler-policy)
records the current baseline; it does not establish historical calibration or
an end-to-end latency promise.

Integer remainder is unassigned. The stream burst is
`max(F, min(4F, stream_queue_bytes))`. This reserves space for all traffic
classes while leaving the scheduling and fairness rules to
[multiplex](../../../undercurrent-multiplex/src/scheduler.rs).

## Timer scopes

### Handshake read

`handshake_timeout` wraps the first `local.next()`, after peer verification. It is not a single deadline for the entire exchange. Shutdown and recycle can interrupt this read.

### Handshake response

`write_timeout` wraps `send`; rejection then gets a separate `write_timeout` for close. These waits do not select on shutdown/recycle.

### Active connection idle

A fresh timer using negotiated heartbeat timeout is created each main-loop iteration. Either inbound or outbound progress can restart it; it is not a strict elapsed-since-last-peer-byte deadline. It does not run while an inner write or synchronous dispatch call is executing.

### Active frame write

`write_timeout` bounds `local.send`; shutdown/recycle can interrupt it. Only one write is active per connection.

### Final close

Best-effort `write_timeout`, only for a successful loop outcome without an interrupted write. Failure of this close is ignored.

### Draining workers

No aggregate deadline in `LocalServer`. Its caller must choose when to request
shutdown. `run` starts one native cleanup supervisor; queued and retiring jobs
retain their connection permits, so their count remains bounded by
`maximum_connections`. Cleanup callbacks must return promptly; see
[cleanup ownership](../internals/server.md#failure-and-external-cancellation).

There is no per-product-request deadline here. Operation deadlines and
cooperative cancellation belong to the SDK, dispatcher, and executor.
The adapter does not generate heartbeat pings; transport-core semantics handle
received ping messages, and the client owns its heartbeat behavior.

See the [connection lifecycle](../internals/server.md) for ordering and
[diagnostics](diagnostics.md) for timeout error mappings.

[Crate documentation](../README.md)
