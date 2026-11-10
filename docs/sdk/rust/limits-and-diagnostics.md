# Look up limits, errors, and diagnostic evidence

Use this reference to size a Rust SDK 0.1.0-beta client and diagnose failures without
logging private payloads. The authoritative defaults are in
[profile.rs](../../../crates/undercurrent-sdk/src/profile.rs); validation belongs to
[boundary.rs](../../../crates/undercurrent-sdk/src/boundary.rs), [client.rs](../../../crates/undercurrent-sdk/src/client.rs),
[local.rs](../../../crates/undercurrent-sdk/src/local.rs), and [product.rs](../../../crates/undercurrent-sdk/src/product.rs).
The [public API reference](../../../crates/undercurrent-sdk/PUBLIC_API.md) owns supported version/surface
policy, and [stream recovery](streams-and-recovery.md) owns recovery behavior.

To connect a support case to daemon logs, call `ProductClient::with_correlation_id`
on the client before issuing requests or creating its clones. Each request and
stream still receives its own generated trace/span identity. Explicit
`request_with_trace_context` and `open_subscription_with_trace_context` calls
take precedence; reconnecting a stream preserves its original context. The
reference is diagnostic metadata and supplies no retry or mutation authority.
The native CLI uses this mechanism for its printed `UC-` error references; see
[support report collection](../../guides/daemon/diagnostics.md#collect-a-support-report).

| Need | Section |
| --- | --- |
| Size a client | [Standard profile](#standard-profile) and [custom limits](#profile-variants-and-custom-limits) |
| Interpret a failure | [Error boundaries](#error-boundaries) |
| Investigate pressure or missing events | [Bounded telemetry](#inspect-pressure-and-bounded-telemetry) |
| Decide what is safe to log | [Privacy and trace context](#privacy-and-trace-context) |

## Standard profile

`StandaloneClientProfile::standard(client_id)` creates compatible offers, local
transport bounds, product codecs, and recovery settings. It performs no IO.
These are **offers and local limits**, not a promise that the daemon accepts all
of them. Read the installed `agreement()` to see selected limits, heartbeat terms,
capabilities, server epoch, and optional replay retention.

| Layer / field | Standard value | Meaning |
| --- | --- | --- |
| Connection `max_frame_bytes` | 33,554,432 bytes | Complete UCMP frame bound |
| Connection `max_logical_streams` | 1,024 | Active logical stream offer |
| Connection `max_pending_requests` | 256 | Correlated finite request slots, including opens/resumes |
| Connection `connection_queue_bytes` | 134,217,728 | Negotiated connection queue budget |
| Connection `stream_queue_bytes` | 33,554,432 | Negotiated per-stream queue budget |
| Connection initial stream credit | 262,144 bytes | `initial_stream_window_bytes`; per-stream flow-control credit |
| Local handshake frame | 65,536 bytes | Complete UCHS message maximum |
| Local connect / handshake timeouts | 5 seconds each | Per-attempt dial and handshake bounds |
| Local maximum message | 33,554,432 bytes | Adapter message capacity |
| Local outbound / inbound queues | 1,024 / 64 messages | Encoded outbound / decoded inbound adapter buffers |
| SDK payload | 33,554,432 minus maximum client-frame envelope | Space reserved for framing, IDs, and trace context |
| SDK writer queue | 1,024 messages | Semantic commands awaiting the sole writer |
| SDK stream mailbox | 16,448 items and 262,144 bytes | Item slots cover initial byte credit even for tiny frames, plus gap headroom; both limits apply |
| Product request / response codec | 33,552,604 / 5,242,880 bytes | Bounded finite JSON payload classes |
| Product stream spec / JSON item codec | 65,536 / 131,072 bytes | Subscription specification and JSON delivery |
| Terminal data codec | 131,072 bytes | Binary data bound; header overhead is additional |
| Resume timeout | 5 seconds | Each finite stream resume response wait |
| Recovery | 8 attempts; 8 parallel resumes | Per-pass physical attempts and resume-worker bound |
| Recovery delays | 50 ms initial, 2 seconds maximum | 20,000 basis points = 2× exponential multiplier |
| Recovery jitter | Up to 2,000 basis points | Additive 0–20%, clamped by the 2-second ceiling |
| Default telemetry diagnostics | 128 entries | Oldest entries evicted; metrics have fixed registry cardinality |

The exact envelope reservation comes from the
[wire codec](../../../crates/undercurrent-wire/src/codec.rs), not a product payload constant.
Do not add that private crate as a consumer dependency to calculate a new offer;
prefer the standard profile or use conservative explicit SDK bounds.

## Profile variants and custom limits

Use `StandaloneClientProfile::with_request_limit(client_id, request_bytes)` to
configure the **complete JSON request** bound. The profile computes frame and
transport queue offers, including four frame reservations in the connection
queue. Even a small request limit keeps enough frame capacity for the 5 MiB
response allowance, which covers JSON-encoded output and screen history.
The daemon's negotiated and service limits remain authoritative; this
option does not change its input allowance. The CLI supplies its loaded
`requests.request_bytes`; standalone Rust clients do not read that config file.
See the [input-size decision](../../architecture/decisions/0004-input-size-limits.md)
for the default's basis and separate memory/retry limitations.


| Use case | Constructor and change from standard |
| --- | --- |
| Initial probe | `single_connection_attempt` allows one recovery attempt. Supply its policy as the **initial** policy to `connect_with_initial_policy` for an externally bounded probe. |
| Retained recovery qualification | `retained_recovery_qualification` reserves at least an 8,388,608-byte per-stream queue. The current 32 MiB frame requires 32 MiB in both profiles; SDK mailboxes stay unchanged. |
| Planned daemon-restart qualification | `planned_restart_qualification` allows 20 recovery attempts. All other settings stay unchanged. |

Passing the entire single-attempt profile runtime to ordinary `connect` limits
**both initial and subsequent** recovery to one attempt. To shorten startup while
retaining normal recovery, pass the standard runtime plus the single-attempt
policy to `ProductClient::connect_with_initial_policy`. The
[CLI factory](../../../crates/undercurrent-cli/src/client.rs) follows that pattern and adjusts
local transport deadlines separately. Profile attempts are not an overall
wall-clock deadline.

Custom configurations are validated in layers:

1. `ClientConnectionLimits` delegates nonzero and relationship checks to the wire
   contract. `ClientConnectionHello` rejects protocol versions other than 0.1.0-beta and
   validates identity/capability names.
2. `LocalClientLimits` rejects zero durations/queues/message size and a message
   maximum below the handshake bound. `LocalClientConfig` rejects an offered
   UCMP frame larger than the adapter's message bound.
3. `ClientRuntimeLimits` rejects zero bounds. `Client::connect` reduces payload
   and mailbox ceilings to fit the selected frame and stream queue. Frames larger
   than the maximum client envelope reserve that envelope before admitting a
   payload. Very small manually constructed frames still require caller-managed
   overhead and a payload bound fitting the selected frame. The original product
   codec remains a local bound; an oversized call can fail at physical admission
   or at the daemon without making a smaller call invalid.
4. `ProductClientRuntime` rejects zero resume timeout and product payload limits
   above the SDK payload bound. Terminal validation includes the larger screen
   header. This checks relationships, not whether all future request values will
   encode successfully.
5. `ClientRecoveryPolicy` requires nonzero initial/maximum delay, attempts and
   parallel resumes, maximum delay at least initial delay, multiplier at least
   10,000 basis points, and jitter in `1..=10,000`. A jitter sampler may return
   zero; values above the configured bound are clamped.

The official local adapter uses the configured handshake timeout for writes after
negotiation too. A saturated outbound adapter queue returns a typed capacity error. A full inbound
adapter queue pauses reads with one retained frame and resumes on reader notification;
outbound traffic and shutdown remain active. A full inbound queue retains its first
terminal error out of band, so it can be reported after queued frames drain.

## Error boundaries

**Config**

`ClientBoundaryError::field`, `LocalClientConfigError`, profile component and limit errors identify local invalid settings; correct them before connecting.

**Physical setup**

`ProductClientError::Connect` retains `ResilientClientConnectError` and its `ClientRecoveryError` source; distinguish exhausted connector attempts from SDK/supervisor/resume-worker startup failure.

**Transport**

`ClientTransportError` identifies `Connect`, `Send`, `Receive`, or `Close`, optional overload, and retained source; `LocalTransportError` adds its stage.

**Finite runtime**

`ClientError` distinguishes invalid payload, writer saturation, router/pending failure, retained transport failure, and trace generation failure.

**Persistent phase**

`Recovering`, `RecoveryFailed(reason)`, and `Closed` describe the client; they are not server product errors.

**Data**

`Codec`, `OperationMismatch`, `StreamKindMismatch`, `StreamItemMismatch`, and `MissingCapability` distinguish incompatible data from a product rejection.

**Product rejection**

`Server(ProductError)` or initial `StreamRejected(ProductStreamError)` preserves exact category, optional disposition, retry timing/limit, and resynchronization; finite errors may include effect certainty.

**Stream receive**

Timeout, independent unsubscribe, client close, terminal recovery failure, or SDK-level resume rejection; see the [resume limitation](streams-and-recovery.md#pressure-and-current-recovery-limitations).

### Distinguish presentation categories from server evidence

`ProductClientError::category()` maps failures to seven presentation categories:
invalid argument, unavailable, overloaded, timeout, cancelled, server rejected,
and protocol violation. Exact mapping is owned by
[product/error.rs](../../../crates/undercurrent-sdk/src/product/error.rs).
These distinctions matter when choosing a response:

| Failure | Presentation category |
| --- | --- |
| Initial connection fails, including nested connector capacity errors | `Unavailable` |
| Local response wait times out | `Timeout` |
| Server rejects with a timeout | `ServerRejected` |
| Request or stream-spec codec error | `InvalidArgument` |
| Response, stream-item, or terminal decode error | `ProtocolViolation` |
| Server reports cancellation | `Cancelled` |

`product_error_category()` and `product_error_disposition()` return only actual
server fields, never a guessed category for local failures. Preserve `None` for
absent disposition. Finite `effect_certainty` is accessible by matching
`ProductClientError::Server`; there is no SDK accessor that invents it. Missing
metadata is not evidence of absent effects. Use
[mutation reconciliation](operations.md#reconcile-a-mutation) before retrying
ambiguous operations.

### Interpret overload guidance

`ClientError::overload()` and `ClientTransportError::overload()` expose a resource
name, optional limit/current/requested quantities, and `ClientRetryGuidance`.
The guidance distinguishes capacity release, queue drain, returned credit,
bounded backoff, and “do not retry unchanged.” It describes resource recovery,
not blanket permission to replay a mutation. Direct `Subscription` receive maps
a connection's retained overload to `ClientStreamReceiveError::Overloaded`.
The resilient wrapper instead treats physical closure as a recovery trigger;
it does not expose that same direct receive error variant.

## Inspect pressure and bounded telemetry

`ProductClient::pressure()` reports phase, installed generation, physical attempt
accounting, and registered persistent subscriptions. `Failed(reason)` is terminal;
check whether the reason is limits, checkpoints, incompatible agreement, exhausted
attempts, client runtime, or resume runtime. The lower `ResilientClient` also
exposes `last_recovery_error()` with the retained source. Keep a clone when wrapping
it with `ProductClient::from_resilient` if you need that diagnostic access.

Attempt accounting includes successful installs and exhausted-attempt errors.
Some failures after a connector succeeds (for example invalid resume checkpoints
or worker setup) do not carry an attempt count, and the current failure path adds
zero for them. Do not treat this field as an exact audit log of every dial under
all failures. The subscription count can include a terminal failed handle until
that handle is dropped or cancelled.

### Read the default recorder

Default clients expose `telemetry_diagnostics`, `telemetry_metric_series`, and
`telemetry_pressure`. Check dropped counts before drawing conclusions from
missing events.

| Recorder behavior | Consequence for readers |
| --- | --- |
| Diagnostics contain stable names, oldest first | No payload labels or timestamps are recorded |
| Writes use non-waiting locks | Contended observations are dropped |
| Diagnostic retention reaches capacity | The oldest entry is evicted |
| A metric value is non-finite | The update is rejected |
| A caller reads a snapshot | The read takes ordinary locks |

The [diagnostic and metric registries](../../../crates/undercurrent-sdk/src/registry.rs) own stable names and
complete `ALL` lists. Use them in external adapters rather than inventing parallel
spelling tables. Useful diagnostic sequences include request queued → response
routed, late response ignored, slow consumer detected, credit deferred, writer
saturated, and recovery started → succeeded/failed. Resume failure is recorded
per affected SDK-level stream result.

| Metric family | Interpretation |
| --- | --- |
| Physical connections, pending requests, logical subscriptions, writer queued messages | Observations of physical runtime gauges, including resets on teardown |
| Stream mailbox bytes, deferred credit bytes | Per-stream observations without stream labels; `latest` is not the total across all subscriptions |
| Request duration seconds | Handle lifetime from request start through wait/drop; also covers finite opens/resumes, not daemon-only service time |
| Reconnect attempts, resume attempts/succeeded/failed, overload rejections | Recorded increments or pass-sized observations; inspect `total` with the recorder-loss caveat |

Every series retains samples, finite saturating total, minimum, maximum, and latest.
Summing gauge samples does not yield current occupancy. Metrics are fixed-cardinality
by registry name, so multiple generations/streams share those series.

### Supply a custom sink

To supply a custom sink, implement non-blocking, payload-safe `ClientTelemetry`,
construct `ResilientClient::connect_with_telemetry`, then wrap it with
`ProductClient::from_resilient`. `Client::connect_with_telemetry` and manual recovery
also accept a sink. Default-recorder accessors return `None` for a custom sink;
retain and query the sink yourself. `NoopClientTelemetry` explicitly disables
recording. A sink must avoid blocking, re-entering client operations, or panicking
on SDK worker threads.

## Privacy and trace context

`ClientTraceContext` validates caller-supplied trace/span identities and optional
parent/correlation values. Ordinary requests generate context from one OS-entropy
seed per physical client and a checked monotonic counter. Generation requires no
further entropy IO after construction. Counter exhaustion fails rather than
wrapping; entropy failure prevents construction. Logical subscriptions retain the
original context across resumes. Traces correlate work; they do not authenticate
it or provide durable deduplication.

`ClientTransportError`, `LocalTransportError`, and `ClientBoundaryError` redact
concrete source text from their outer formatting, while retaining `Error::source`
for controlled debugging. Product subscription item `Debug` prints kind/sequence,
not terminal bytes or JSON content. That does **not** make every SDK value safe
to log: `ClientTransportCommand`, outbound transport `Debug`, and raw
`ClientStreamItem::Data` include payloads. Checkpoints omit payload content from
`Debug` but still show identifiers. Protocol DTOs have their own redaction rules.
Never walk source chains or serialize all request/response values into diagnostics
without an explicit data-handling decision.

[Connection troubleshooting](connect-and-query.md#diagnose-setup-failures) · [Maintainer verification](../../../crates/undercurrent-sdk/docs/README.md#verification) · [SDK entry point](../../../crates/undercurrent-sdk/README.md)
