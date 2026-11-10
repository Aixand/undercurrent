# Extend and verify the SDK boundary

Use this guide to place a change in its owning component and select the focused
SDK checks. Consumer setup is in the [connection tutorial](../../../../docs/sdk/rust/connect-and-query.md);
public compatibility is defined by the [0.1.0-beta boundary](../../PUBLIC_API.md).

## Place an extension at its owner

### Add an operation or stream

For a new operation or stream kind, first update the owning protocol DTO,
registry, codec, and product contract/schema. The daemon owns authorization,
execution, retention, and stream scheduling. The SDK's generic request path may
already carry a new operation; add a typed convenience only when it improves
consumer use. If a stream kind changes, update `stream_item_kind`, stream-open
correlation, gap mapping when applicable, and codec bounds. Check CLI and Python
bridge consumers independently: Python's current bridge accepts watch/events
subscriptions and uses a private stdio protocol, not the entire Rust surface.

### Change transport behavior

For transport changes, preserve SDK-owned values and bounded encode/decode,
source redaction, authentication-before-handshake, and `close` unblocking IO.
Extend [transport contracts](../../tests/client_contract.rs) and
[real local boundary tests](../../tests/local_boundary.rs); test silent peers,
capacity, and cancellation, not just successful requests. New network fallback or
private-runtime coupling would change architecture policy and supported behavior.

### Change recovery behavior

For recovery changes, test races among receive, detach, close, and replacement
installation. Preserve caller deadlines and exact stream IDs. Test one failing
stream alongside successful peers, terminal recovery exhaustion, and new opens
after reconnect. Changes to retryability require an operation-specific effect
contract, not only a transport test.

### Change diagnostics or profiles

For diagnostics, update central registries, preserve stable spelling and bounded
labels, check drop accounting, and test redaction of arbitrary source text. For
profile changes, update the [limits reference](../../../../docs/sdk/rust/limits-and-diagnostics.md), validate
negotiated relationships, and retain the tests that qualification variants change
only their intended limits.

## Verification

From the repository root, use the pinned toolchain and locked dependencies. The checkout must
have its dependencies available. Each command should exit successfully; tests
should report no failures and Rustdoc should emit no warnings.
The local boundary tests create their own temporary endpoints and test servers;
they do not require a real user daemon.

```sh
cargo test --locked -p aixand-undercurrent --lib --tests
cargo check --locked -p aixand-undercurrent --examples
cargo test --locked -p aixand-undercurrent --doc
RUSTDOCFLAGS='-D warnings' cargo doc --locked -p aixand-undercurrent --no-deps
```

On PowerShell, set `$env:RUSTDOCFLAGS = '-D warnings'` before the last Cargo command
instead of the POSIX inline assignment. Generated API documentation is under
`target/doc/undercurrent_sdk/index.html` with the default Cargo target directory.
For integration validation, the existing
[validation runner](../../../../scripts/validate.py) owns the full checks,
including its warning-denied Rustdoc stage:

```sh
scripts/validate.py --stage rustdoc
```

That stage checks the entire workspace. Use it as an integration check after
focused SDK checks. Mermaid changes also require rendering and visual inspection;
syntax or metadata checks alone do not establish layout/readability. Runtime examples
need a deliberately selected daemon and platform-appropriate executable; compiling
them does not prove their live behavior.

| Evidence | Significant cases and limits |
| --- | --- |
| [Public boundary](../../tests/public_boundary.rs), [consumer fixture](../../tests/public_consumer.rs) | Public declarations do not expose private wire/router types; consumer fixture and opaque boundary; source scanner is not a full Rust type-system proof |
| [Client contracts](../../tests/client_contract.rs) | Out-of-order concurrent responses; 300 independent/resumed streams on one transport; writer saturation; close/admission races; heartbeat liveness/silence; slow consumer retains consumed checkpoint; retained source redaction; cancellable backoff; separate initial policy; repeated generations; exhaustion wakes handles |
| [Typed product](../../tests/product_client.rs) | Required optional-capability preflight for opaque/guarded operations; correlated cancellation race; exact error disposition; binary terminal and finite RPC sharing; malformed terminal item cancels stream; retained-recovery profile |
| [Local IPC](../../tests/local_boundary.rs) | Real local IPC bytes and 300 streams; admission overload; pre-cancelled and silent setup; negotiated limits; physical recycle; daemon epoch gap; slow reader/shutdown behavior |
| Inline module tests | Profile variant differences, payload-class limit failure, every gap mapping, trace width/uniqueness within a generator, bounded diagnostic eviction and invalid metrics, adapter terminal overload and wire error redaction |
| [Architecture consumer](../../../../tests/architecture/consumer/Cargo.toml) | External build graph restricted to SDK/protocol surfaces |

Do not use passing memory-transport tests to claim OS authentication coverage, or
Linux execution to claim macOS/Windows qualification. Product-resume validation
and writer-owned credit retry have deterministic transport tests; these do not
eliminate physically blocked transports or extreme/concurrent receive timeout
limitations.
When debugging, begin at the typed error/phase, then inspect bounded telemetry and
the owning path from the [code map](#code-map). Keep exact request identities for mutation
reconciliation and avoid logging payloads in transport traces.


## Invariants and tradeoffs to preserve

| Invariant | Why it exists / consequence |
| --- | --- |
| One reader and writer per physical client | Correlation and stream routing remain centralized; a reader per request could steal another response |
| Register routes before enqueueing | Fast responses and early stream data have a destination; unwind provisional registrations on failure |
| Bounded bytes **and** message counts | Many small items and a few large items can each exhaust memory; a byte bound alone is insufficient |
| Caller-consumed cursor, explicit gaps | Queued-but-undelivered bytes must not silently disappear on resume; complete history is still retention-dependent |
| Isolate mailbox overload; physical teardown if remote close cannot be queued | Reports one failed logical stream without skipping its application checkpoint; peers recover only when orderly cleanup is impossible |
| No finite-request auto-retry | Request loss cannot prove whether process or durable effects happened |
| Stable stream identity/trace across generations | Resume refers to the original logical interest, independently of new physical request IDs |
| SDK-owned types at the external boundary | Internal wire/router changes should not force consumers to depend on daemon-private structures |
| Non-blocking payload-free telemetry writes | Instrumentation must not stall the sole reader/writer or expand high-cardinality state |

These explain existing implementation choices, not new design decisions. Concrete
limitations needing behavior review are documented in the
[recovery guide](../../../../docs/sdk/rust/streams-and-recovery.md#pressure-and-current-recovery-limitations)
and [diagnostic reference](../../../../docs/sdk/rust/limits-and-diagnostics.md#inspect-pressure-and-bounded-telemetry).
In particular, do not “fix” an uncertain mutation by retrying it, infer typed resume
success from encoded response arrival, or label raw outbound `Debug` as redacted.

## Code map

| Owned source | Responsibility and investigation entry points |
| --- | --- |
| [lib.rs](../../src/lib.rs) | Private modules, root re-exports, target gating; public consumer entry point |
| [boundary.rs](../../src/boundary.rs) | SDK-owned opaque identifiers, offers/agreements, trace context, gap mapping, exact-frame encode/decode and custom adapter values |
| [product.rs](../../src/product.rs) | Required/optional capabilities, runtime/codec validation, cancellable finite calls, convenience operations, typed initial stream activation |
| [product / error.rs](../../src/product/error.rs) | Typed product failures and presentation categories; exact server metadata access |
| [product / subscription.rs](../../src/product/subscription.rs) | Product stream handle, binary/JSON decode, kind checks, gap mapping, cancellation on malformed items |
| [client.rs](../../src/client.rs) | One physical runtime, reader/writer/heartbeat threads, IDs, pending request lifetime, stream mailboxes, flow credit, close admission and retained failures |
| [transport.rs](../../src/transport.rs) | Send/receive/close trait and source-retaining redacted transport errors/overload context |
| [local.rs](../../src/local.rs) | Config/limit validation, OS-local connector, worker runtime, handshake, framed adapter queues and shutdown signaling |
| [reconnect.rs](../../src/reconnect.rs) | Backoff/jitter/cancellation, connector trait, manual checkpoints, bounded parallel resume pass and result ordering |
| [resilient.rs](../../src/resilient.rs) | Persistent ownership, generation-aware stream slots, weak registration, supervisor, failure phases and teardown |
| [profile.rs](../../src/profile.rs) | Standard and qualification profiles; compatible codec/queue/attempt defaults |
| [error.rs](../../src/error.rs) | Generic runtime categories, private-router/pending projection, overload and retry guidance |
| [trace.rs](../../src/trace.rs) | Entropy-seeded per-physical-client trace sequence and checked exhaustion |
| [registry.rs](../../src/registry.rs) | Central stable SDK diagnostic/metric names and complete registries |
| [telemetry.rs](../../src/telemetry.rs) | Sink trait, no-op sink, bounded recorder, contention/loss accounting and finite series statistics |

[SDK maintainer documentation](../README.md)
