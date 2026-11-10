# Interpret pressure and diagnostic pages

Use this reference when reading internal connection diagnostics or adding a
field. [connections.rs](../../src/engine/connections.rs) owns registration and
connection pages; [session.rs](../../src/engine/session.rs) owns session pressure
and stream pages. These APIs inspect in-memory state without transport IO or
product payloads.

## Pressure and lifetime

| Observation | Meaning |
| --- | --- |
| `pending_requests` | Pending finite requests, including stream activation requests. |
| `request_ids_used` | All IDs retained in the connection epoch, including settled or rejected work. It can grow while pending work stays near zero. See the [request ledger](../internals/request-lifecycle.md#request-ids-survive-settlement). |
| `logical_streams` | Opening reservations and active streams. |
| Scheduler pressure | Encoded queued bytes, message counts and credit. Already-dequeued writes held by the adapter are excluded. |
| `pressure: None` in a connection row | The weak session cannot be upgraded, or registration used no session. |
| `Some` pressure with `closed = true` | A live session whose connection-local teardown has begun. |
| Registry stream lookup returns `None` | No registration or no available session. A live closed session can instead return an empty page. |

Dropping the last session does not remove a retained registration lease. Such a
lease still consumes connection capacity. See [ownership and teardown](../internals/ownership.md#runtime-ownership).

## Pagination and consistency

| Property | Connection pages | Stream pages |
| --- | --- | --- |
| Order | Ascending `ClientId` | Ascending `StreamId` |
| Cursor | Exclusive `after`; `next_after` exists only if a look-ahead row exists | Same rule, using stream IDs |
| Selection bound | At most `page_size + 1` registry entries | At most `page_size + 1` stream entries |
| Sampling bound | Only returned live sessions | Only returned active scheduler streams |
| Cost evidence | `registry_entries_examined`, `sessions_sampled` | `stream_entries_examined`, `active_streams_sampled` |
| Row data | Negotiated metadata and optional session pressure | Opening request ID, or active configuration and pressure; exact enforced queue byte/message limits |

[`ServerDiagnosticLimits`](../../src/engine/registry.rs) requires nonzero
connection and stream page maxima. A requested page size must be nonzero and
within its configured maximum. A page-size error is an input error, not evidence
of daemon saturation.

Connection pages copy selected metadata and aggregate registry counts under the
registry lock, then release it before sampling session pressure. Row pressure
and aggregate counts can therefore describe different instants. Concurrent
registrations and removals can change later pages; this is an operational view.

Stream pages hold the session lock for selection and connection pressure. Active
rows require scheduler state with the same stream kind; disagreement raises
`StreamRegistryDiverged`. Diagnostics do not create replay journals or extend
reconnect deadlines. The [connection contract tests](../../tests/connection_contract.rs)
cover page bounds, weak ownership, queue limits and unchanged replay retention.

## Telemetry and redaction

The [diagnostic-name registry](../../src/engine/registry.rs) supplies twelve stable
names through `ALL` and `as_str`. It does not publish events. Calling a session
or cache method does not itself emit the corresponding `write_confirmed` or
`idempotency_reserved` event.

Core session telemetry records pending counts, replayed items, reported gaps and
adapter-supplied write duration, and shares its telemetry port with the scheduler.
The daemon has a separate [product-dispatch event registry](../../../undercurrent-daemon/src/dispatch.rs).
See [callback constraints](../internals/ownership.md#synchronization-and-lifetimes)
before installing a telemetry sink or queue-drain callback.

`ServerDispatchError` hides its wrapped source in `Display` and `Debug` while
`Error::source` retains it for controlled investigation. This protection is
specific to that wrapper: `ServerAction` contains opaque bytes and derives
`Debug`. Do not log raw actions, product payloads, terminal output or arbitrary
error-source chains as routine connection telemetry. The
[dispatch contract test](../../tests/dispatch_contract.rs) checks redaction and source retention.

[Crate documentation](../README.md)
