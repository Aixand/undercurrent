# Interpret multiplex failures and pressure

Look up error categories, overload fields, and pressure observations here. For
a stalled stream, start with the [diagnostic procedure](../development/diagnose-delivery.md). It covers internal Rust types in
[error.rs](../../src/error.rs), [registry.rs](../../src/registry.rs), and the local
router/scheduler/journal. Product errors and transport I/O failures retain their
own contracts in [SDK errors](../../../undercurrent-sdk/src/error.rs) and
[server errors](../../../undercurrent-transport-core/src/engine/error.rs).

## Typed failure reference

`MultiplexError::category` and `overload` own the following mapping. Use structured
values; `Display` strings are human explanations and may contain request/stream
IDs. The overload projection deliberately excludes IDs, payloads, and credentials.

| Variant and category | What the caller can conclude |
| --- | --- |
| `InvalidLimit`, `InvalidFrameSize`<br>Category: `InvalidArgument` | Fix the named bound or encoded length. Constructors and admission perform no transport write. |
| `StreamSequenceOutOfOrder`, `FutureResumeSequence`<br>Category: `InvalidArgument` | Check stream identity, epoch, and checkpoints. Increasing sequence validation is not mutation deduplication. |
| `RequestIdInUse`, `StreamIdInUse`<br>Category: `Conflict` | An identity still belongs to existing state; investigate ownership before allocating it again. |
| `UnknownRequest`, `UnknownStream`<br>Category: `Conflict` | The route/key is absent or has been removed. A late response is not evidence of application rollback. |
| `CapacityExceeded`<br>Category: `Overloaded` | Wait for an owned slot/journal entry to be released (`after_capacity_available`). |
| `QueueSaturated`, `QueueMessageSaturated`<br>Category: `Overloaded` | Local queue admission failed; retry after observed progress (`after_queue_drain`) only if the item can fit that budget. |
| `CreditExhausted`<br>Category: `Overloaded` | Wait for returned credit (`after_window_update`). Scheduler selection handles this internally and returns `Ok(None)`. |
| `CreditUpdateExceeded`<br>Category: `Overloaded` | Proposed window exceeds its maximum; `do_not_retry_unchanged`. Inspect duplicate credit returns and negotiated limits. |
| `ConnectionClosed`<br>Category: `ConnectionClosed` | This object cannot be reopened; teardown is terminal for its connection-local state. |
| `CounterExhausted`<br>Category: `InvariantViolation` | Checked ordinal exhaustion or inconsistent internal queue/index state; preserve diagnostics and investigate. |

`ReplayUnavailable` exists in the category registry, but no current
`MultiplexError` variant maps to it. Ordinary unavailable/evicted replay is a
successful `ReplayResult::Gap` or `ReplayResult::GapWithItems`, not that error category. Do not use an unused
category as evidence that replay threw an error.

### Overload fields and retry guidance

`MultiplexOverload` exposes only the following bounded context. `resource` names
the slot pool, queue, or credit window. An absent value is `None`.

| Error | `limit` | `current` | `requested` |
| --- | --- | --- | --- |
| `CapacityExceeded` | Full slot count | Full slot count | `1` |
| `QueueSaturated` | Byte cap | Queued bytes | Incoming bytes |
| `QueueMessageSaturated` | Message cap | Queued messages | `1` |
| `CreditExhausted` | Absent | Available bytes | Required bytes |
| `CreditUpdateExceeded` | Maximum window | Absent | Proposed window |

The retry hints in the category table describe a local rejection. They do not establish whether an upstream
mutation is safe to repeat. Queue drain cannot help an item larger than the empty
queue's byte cap. **Mailbox message overflow also uses `QueueSaturated`**, so its
byte fields alone may not explain the failure; inspect the mailbox message limit.

### Constructor and wait errors

Construction errors are separate types: `SchedulerLimitError` identifies zero
values, excess class bytes/messages, excess stream messages, excess initial
connection credit, and burst below frame maximum. `ReplayLimitError` identifies
zero values or an item/stream budget exceeding its parent. The
[limit explanations](../internals/scheduling.md#limits-are-independent-constraints) and
[replay bounds](../internals/replay.md#retention-and-identity) describe the relationships.

**`PendingResponseError`:** `Cancelled`, `ConnectionClosed`, `Timeout`, and
`RouteDisconnected` (the one-shot sender disappeared unexpectedly). A wait
consumes the pending handle.

**`InboundStreamError`:** `Cancelled`, `Unsubscribed`, `ConnectionClosed`, and
`Timeout`. A timeout leaves the borrowed stream handle usable.

## Pressure and telemetry ownership

| Scope | Method, measurements, and limits |
| --- | --- |
| All scheduler queues | `pressure`: total and per-class queued bytes/messages, active streams, connection credit, cumulative coalesces/drops; captured under one scheduler lock. |
| One scheduler stream | `stream_pressure`: kind, bytes/messages, credit, accepted sequence including drops, pending gap, cumulative coalesces/drops. Unknown after stream removal. |
| Router | `pressure`: pending requests, registered streams, and mailbox bytes. The registry stays locked while mailboxes are sampled separately; concurrent consumption prevents an atomic sum. Responses already handed to a receiver are excluded. |
| One client mailbox | Handle `queued_bytes` or router `stream_queued_bytes`: flow-controlled byte charge for the selected mailbox; gaps have zero charge but occupy message slots. |
| Journal | `pressure`: retained streams/items/bytes, active owners and indexed expirations, after pruning due inactive journals. Returned replay clones are outside this accounting. |

The stable metric and diagnostic name inventories live in
[registry.rs](../../src/registry.rs), with complete `ALL` arrays and snake-case
`as_str` mappings. Keep symbol lookup there and in generated Rustdoc; the grouping
below explains emission rather than defining another registry.

| Metric group | Producer and interpretation |
| --- | --- |
| `queued_bytes`, `queued_messages` | Scheduler emits selected class snapshots on enqueue/dequeue and all classes on removal/close. |
| `logical_streams`, `connection_credit_bytes` | Scheduler reports counts/credit after relevant changes; initial construction itself emits no observations. |
| `coalesced_items`, `dropped_items`, `reported_gaps` | Scheduler counter increments. Source gaps increment drops without a class; a pending live gap increments reported gaps when selected, before physical write success. |
| `stream_lag_items` | A sample of one stream's queued frames plus all pending loss intervals; no stream-ID label. It is not a connection total or uniquely identifiable stream time series. |
| `physical_connections`, `pending_requests`, `replayed_items`, `connection_write_duration_seconds` | Outer transport components supply these; the standalone scheduler cannot observe them. Server replay count refers to scheduled retained items, not confirmed receipt. |

### Callback and aggregation limits

`MultiplexTelemetry: Send + Sync` has only `record_metric`. Implementations must
return promptly; the trait does not enforce nonblocking execution. The scheduler
calls it after unlocking. A slow or panicking custom callback can still affect
the calling thread after state has changed. `NoopMultiplexTelemetry` is the
default.

`MultiplexDiagnosticName` defines names but has no diagnostic-emission
method in this trait; listing a name such as `CreditBlocked` does not prove an
event was emitted.

The [daemon telemetry adapter](../../../undercurrent-daemon/src/multiplex_telemetry.rs)
aggregates fixed-cardinality metric/class series, rejecting nonfinite values and
dropping updates on contention. Samples from shared connections can interleave;
an aggregate's latest gauge value is not automatically a fleet-wide sum.

## Diagnose a stalled or disconnected stream

The procedure now lives in [Diagnose delivery](../development/diagnose-delivery.md),
including prerequisites, queue and credit checks, recovery verification, and cleanup.

[Crate documentation](../README.md)
