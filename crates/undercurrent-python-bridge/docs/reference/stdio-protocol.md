# Look up the private stdio protocol

Use this reference when changing the paired Python transport and Rust bridge.
It describes package `0.1.0-beta` and ready protocol `0.1.0-beta` at this revision; it is not an
independent public API. The owning definitions are [`Command` and `run`](../../src/main.rs)
and the [Python `Bridge`](../../../../python/src/undercurrent/_transport.py).
Product payload definitions belong to the [protocol DTOs](../../../undercurrent-protocol/src/dto.rs),
[operation registry](../../../undercurrent-protocol/src/registry.rs), and
[contract validation](../../../undercurrent-protocol/src/compatibility.rs).

[Crate entry point](../../README.md) · [Implementation flows](../internals/operation-flow.md) ·
[Recovery](../internals/recovery.md)

## Version identifiers

Keep package releases, private stdio readiness, and product compatibility separate:

| Identifier | Current source | Meaning |
| --- | --- | --- |
| Bridge / Python package | `0.1.0-beta` / canonical Python `0.1.0b0` in [bridge manifest](../../Cargo.toml) / [Python manifest](../../../../python/pyproject.toml) | Package release versions; build both from the same revision |
| Private ready protocol | `0.1.0-beta` in [`main.rs`](../../src/main.rs) and [Python transport](../../../../python/src/undercurrent/_transport.py) | Parent/subprocess handshake marker; not negotiated product capabilities |
| Rust SDK package | `0.1.0-beta` in [SDK manifest](../../../undercurrent-sdk/Cargo.toml) | Supported Rust API generation |
| Product contract / schema | `0.1.0-beta` / `0.1.0-beta` in the [protocol contract constants and DTOs](../../../undercurrent-protocol/src/contract.rs) | Current product payload compatibility, supplied by `MutationContext::current` |
| Client wire version | `ClientProtocolVersion::CURRENT = 0.1.0-beta` in [SDK boundary](../../../undercurrent-sdk/src/boundary.rs) | Public SDK's local wire negotiation, independent of private stdio protocol |

There are no optional Cargo features in this bridge's manifest. Matching `0.1.0-beta`
package numbers do not establish compatibility across different source revisions.

## Framing and startup

The parent launches one bridge with piped stdin/stdout. Input is UTF-8 JSON, one
command per line; a final nonempty line at EOF can omit its newline. Output is
compact JSON plus a newline, explicitly flushed. Normal replies, stream workers,
and `main` fatal errors share one output owner. Its mutex spans serialization,
newline, and flush without imposing input-order completion. A fatal emission also
marks output terminal under that lock: already-started frames finish first and
later worker frames are suppressed before process exit. Broken stdout or process
termination can still leave incomplete bytes; treat those as bridge failure,
not a complete response.

The bridge ignores command-line arguments: it has no `--help`, socket flag, or
CLI parser. Configuration comes from the environment:

| Environment variable | Rust bridge behavior | Python caller behavior |
| --- | --- | --- |
| `UNDERCURRENT_HOME` | Used as-is; appends `/run/control.sock` | `Client(home=...)` takes precedence, then environment, then `~/.aixand/undercurrent`; expands user and resolves absolute path before spawning |
| `HOME` | Fallback is `$HOME/.aixand/undercurrent`; absent/non-Unicode `HOME` becomes an empty prefix | Python normally supplies resolved `UNDERCURRENT_HOME` explicitly |
| `UNDERCURRENT_PYTHON_BRIDGE` | Not read by the Rust process | `Client(bridge=...)` takes precedence, then environment, then `uc-python-bridge` on PATH; resolved with `shutil.which` |

Rust creates a `StandaloneClientProfile::standard` with a fresh
`python-<UUIDv7>` client identity, constructs `LocalClientConfig`, and connects
with deterministic recovery jitter seeded from the low 64 bits of that fresh
UUID, which include per-instance random material. Tests can reproduce samples
from a fixed identity; production clients no longer all start with seed 1.
Independent samples reduce synchronized retry scheduling without guaranteeing
that retries never coincide. After successful SDK setup, it emits exactly
this startup shape:

```json
{"type":"ready","protocol":"0.1.0-beta"}
```

The Python parent requires both fields. This is connection readiness, not a
session's application readiness. The bridge has no startup timeout option;
SDK connection/profile limits apply. Python separately bounds its initial wait
by `request_timeout`.

## Command envelope

| Envelope field | Shape and default | Meaning |
| --- | --- | --- |
| `id` | Required string | Parent-selected correlation key; requests are not deduplicated by this ID |
| `op` | Required string | One of the operations below |
| `body` | Any JSON value, default `null` | Product request or stream specification where required |
| `timeout` | Number, default `30.0` seconds | Must be finite and in `(0, 300]`; validated even for `ack` and `unsubscribe` |

Unknown envelope fields are ignored by `Command` deserialization. Missing or
wrongly typed required fields, malformed JSON, invalid UTF-8, and blank lines
are fatal reader failures. A well-typed out-of-range timeout produces an
ID-correlated `InvalidArgument` error and keeps the loop alive.

| Command operation | Body | Execution and response |
| --- | --- | --- |
| `request` | Any deserializable `ProductRequest` | Normalize selected mutations, check finite-request capacity, spawn worker; one `result` or `error` on normal worker completion |
| `subscribe` | `session_watch` or `session_events` product stream spec | Open synchronously on the stdin reader; `result: null` precedes stream items; retain entry keyed by `id` |
| `ack` | Ignored | Nonblocking send to the stream's one-slot acknowledgment channel; unknown IDs and full/disconnected channels are silently ignored; no response |
| `unsubscribe` | Ignored | Remove entry, cancel SDK subscription, drop acknowledgment sender, join worker; unknown IDs are no-ops; no response |

An unknown `op` with a valid timeout is fatal; timeout validation runs first. Invalid product-request JSON yields `InvalidArgument`
with `Invalid product request`. Invalid or unsupported stream specs yield
`InvalidArgument` with `Expected session watch or events specification`.
`terminal_output` and `agent_events` specs are rejected even though the public
Rust SDK supports them. A finite `request` is not restricted to the operations
exposed as Python convenience methods.

The parent generates fresh UUIDv4 hex IDs. The Rust bridge only checks duplicate
IDs among active subscriptions; it has no uniqueness check for finite requests
or across request and subscription namespaces. Do not reuse IDs for simultaneous
calls: the Python pending-future map could otherwise correlate the wrong result.

## Mutation normalization

`prepare` is the entire bridge-specific product transformation:

| Request | Change before SDK encoding |
| --- | --- |
| `start_session` | If environment binding is absent and working directory is present, set its root to that exact directory string and derive its digest with `standalone_environment_binding_digest_sha256` |
| `start_session`, `send_terminal_input`, `stop_session`, `resize_terminal`, `rename_session` | Replace mutation metadata with `MutationContext::current(operation)`, retaining only the previous `idempotency_key` |
| All other requests | Pass through unchanged |

An existing environment binding is preserved. The bridge does not canonicalize
the directory or validate its existence; Python `Client.start` resolves its
working directory, and daemon validation owns launch constraints. Supplied
mutation `attempt`, `contract`, and compatible unknown fields are discarded for
the five normalized operations. This is a standalone convenience path, not a
way to preserve caller-fenced ecosystem attempts. Other mutations need their
own valid current metadata. The bridge neither invents idempotency keys nor
adds durable deduplication.

## Output envelopes

| Envelope type | Fields besides `type` | Meaning |
| --- | --- | --- |
| `ready` | `protocol: "0.1.0-beta"` | Initial SDK connection installed |
| `result` | `id`, `result` | Serialized `ProductSuccess`, or `null` for subscription activation |
| `error` | `id`, `error` | Command validation/capacity failure, SDK request failure, or stream-open failure |
| `item` | `id`, `item` | Serialized product JSON item, or the bridge gap projection below |
| `stream_error` | `id`, `error` | Worker receive failure other than its polling timeout; worker exits |
| `fatal` | `error` | Startup or reader failure reported by `main`, followed by exit status 1 if stdout remains writable |

Product successes retain `{"operation": ..., "result": ...}` and product JSON
items retain `{"item": ..., "value": ...}`. The bridge discards stream-open
metadata and stream-local sequence numbers. Durable session-event sequence
fields inside a product item remain present.

For an SDK gap, the only forwarded fields are:

```json
{"type":"item","id":"watch-1","item":{"item":"gap","value":{"reason":"replay_evicted","recovery":"restart_query"}}}
```

This is an illustrative envelope, not captured output. The bridge drops the SDK
gap's stream ID and missing-sequence range. Exact reason/recovery spellings are
owned by [product subscription types](../../../undercurrent-sdk/src/product/subscription.rs).
Every forwarded item, including a gap, waits for an acknowledgment before the
worker receives another. The unexpected `Terminal` variant exits the worker
silently; current admission makes that branch unreachable in a valid stream.
There is no separate end-of-stream envelope.

## Error projection

[`error_value`](../../src/main.rs) projects SDK failures as follows. Optional
values serialize as `null`; local validation/capacity errors and `fatal` contain
only `message` and `category`.

| Field | Source and interpretation |
| --- | --- |
| `message` | SDK Display text, or a bridge-generated explanation |
| `category` | Rust Debug spelling shown below; differs from `as_str()` |
| `product_category` | Typed finite or stream-open rejection category, when supplied |
| `effect_certainty` | Copied only from `ProductClientError::Server` |
| `disposition` | Typed finite or stream-open rejection disposition, when supplied |

### Category lookup

| Bridge category | Typical source |
| --- | --- |
| `InvalidArgument` | Local command validation, request/spec codec validation |
| `Unavailable` | Connection/recovery failure, closed client, or `fatal` |
| `Overloaded` | Bridge admission or SDK bounded-resource saturation |
| `Timeout` | SDK caller deadline; stream poll timeouts are suppressed |
| `Cancelled` | Closed subscription or daemon cancellation result |
| `ServerRejected` | Typed daemon rejection other than cancelled |
| `ProtocolViolation` | Capability, response correlation, or response/item decoding failure |

The SDK's [error mapping](../../../undercurrent-sdk/src/product/error.rs) owns the
full classification. The projection omits operation, session ID, retryable flag,
retry delay, resource limit, resynchronization detail, and Rust source chains.

Python constructs [`UndercurrentError`](../../../../python/src/undercurrent/errors.py)
from these fields. A wire `Timeout` remains an `UndercurrentError`; expiry of a
Python `asyncio.timeout` raises Python's `TimeoutError`. Neither an absent
certainty field nor a category alone establishes safe mutation retry.

## Limits and timing

| Bound | Current implementation | Consequence |
| --- | --- | --- |
| Input line | `max(4 MiB, request_bytes + 65,536)`, including newline when present; Rust reads at most one extra byte to detect overflow | Overflow is fatal; Python rejects its own oversized writes first |
| Rust stdout | No separate encoded-line size guard | Product/SDK codecs bound source values; Python's reader uses the same calculated line limit |
| Finite request workers | 16 admitted until completion, including result emission | Excess receives `Overloaded`; blocked stdout keeps workers counted |
| Subscription entries | 64; duplicate active ID also rejected as `Overloaded` | Entries count until unsubscribe/EOF, even after worker exit |
| Acknowledgment channel | One queued token per stream | Intended caller sends one acknowledgment per consumed item; no sequence validation prevents a raw client from pre-acking |
| Python pending calls | Semaphore of 16 across requests and subscription opens | Slot waiting occurs before its response timeout; Rust still checks its own capacity |
| Python stream queue | Two entries per stream | Parent acknowledges before yielding; one item can already be in user code while the next arrives |

### Timeout phases

These waits start at different phases; none is a complete end-to-end deadline.

| Wait | Current implementation | Consequence |
| --- | --- | --- |
| Command timeout | Default 30 s, allowed `(0, 300]` | Passed to SDK finite wait or subscription activation; not a stream lifetime |
| Stream receive poll | 250 ms | Timeout loops; idle streams do not expire after 250 ms |
| Python response wait | `request_timeout + 1` s after write/drain | Excludes connection, slot wait, and write-lock/drain time; not a complete end-to-end deadline |
| Python close wait | 2 s before killing a lingering bridge | Bounds the parent's graceful wait, not daemon effects |

Lower product payload and transport bounds still apply. The private optional
`--request-bytes N` argument selects the SDK request limit; omission uses
`DEFAULT_REQUEST_BYTES` (33,552,604). Python passes its `Client(request_bytes=...)`
option and writes UTF-8 JSON without ASCII-only escaping. Both sides calculate
the same line bound. Invalid, zero, overflowing or extra arguments fail startup.
The [SDK profile](../../../undercurrent-sdk/src/profile.rs) derives frame and
transport queue offers from this bound; other profile settings retain their defaults. See [ordering and acknowledgment flow](../internals/operation-flow.md#stream-delivery-and-backpressure)
and [shutdown](../internals/recovery.md#close-and-cancellation-are-different-actions) before
changing these bounds.

[Crate documentation](../README.md)
