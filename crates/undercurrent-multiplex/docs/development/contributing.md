# Exercise and change multiplex behavior

Use this guide to reproduce a bounded delivery flow and select checks for an
internal change. Work from the repository root containing the workspace
[Cargo.toml](../../../../Cargo.toml). The pinned
[toolchain](../../../../rust-toolchain.toml) is Rust `1.95.0`; the workspace uses
edition 2024 and declares Rust `1.95` as its minimum. This crate has no feature
flags and only the existing `aixand-undercurrent-wire` dependency.

The example and multiplex contract suite need no daemon, sockets, session host,
credentials, or user processes. The crate has no OS-specific branches; native
transport/platform qualification belongs to the owning adapters. A focused
in-memory test is not evidence that the complete product works on every OS.

## Run the local exercise

1. In the repository root, check that `rustc --version` reports the pinned
   `1.95.0` toolchain. Follow [maintainer setup](../../../../docs/development/setup.md)
   if it is unavailable. An environment override such as `RUSTUP_TOOLCHAIN` can
   take precedence over `rust-toolchain.toml`.
2. Run the maintained [bounded_delivery.rs](../../examples/bounded_delivery.rs):

   ```sh
   cargo run --locked -p aixand-undercurrent-multiplex --example bounded_delivery
   ```

3. Verify exit code zero and these four lines (expected output):

   ```text
   second frame waits for consumed credit
   consuming sequence 1 returns credit
   resume after consumed sequence 1 delivers sequence 2
   all connection and journal entries released
   ```

   Assertions also check the routed/consumed distinction and the released state.
   The example closes both routers and the scheduler, then removes the journal
   entry. It leaves only ordinary Cargo build artifacts; no daemon stop or
   runtime cleanup is required.

### What the exercise demonstrates

The example encodes two real UCMP data frames and starts with credit for one.
Consuming sequence 1 returns its full encoded byte charge, allowing sequence 2
to be selected. The simulated connection closes before sequence 2 is consumed,
so resume after checkpoint 1 selects it again.

Transport success is simulated with an in-memory byte buffer. This exercises
multiplex mechanisms; physical transport and SDK integration need their own
checks. Follow the [write boundary](../internals/replay.md#the-write-boundary)
and [consumed checkpoint](../internals/routing.md#stream-mailboxes-and-checkpoints)
explanations to trace the assertions.

### If the exercise fails

| Failure | Next action |
| --- | --- |
| Pinned toolchain unavailable | Provision it through the repository development setup process. |
| Locked dependencies missing while offline | Populate the approved dependency cache, then retry. Preserve the lockfile. |
| Typed limit or sequence error after an edit | Compare the change with [admission rules](../internals/scheduling.md) and [consumed checkpoints](../internals/routing.md). |
| Receive timeout | Trace the missing local route; the example uses finite waits to expose it. For a real adapter, use [delivery diagnosis](diagnose-delivery.md). |

## Put the change in its owning layer

| Intended change | Owning code and contracts to review |
| --- | --- |
| Wire bytes, IDs, gap encoding, protocol version or negotiated limits | [Wire](../../../undercurrent-wire/src/lib.rs); update codec/contract tests before changing scheduler assumptions. |
| A new scheduling class or different fairness | [frame.rs](../../src/frame.rs) and [scheduler.rs](../../src/scheduler.rs); update class registry, array/index order, weights, all byte/message limits, and telemetry consumers. |
| A product subscription or recovery policy | [Protocol registry](../../../undercurrent-protocol/src/registry.rs) and [daemon stream policy](../../../undercurrent-daemon/src/stream.rs); an internal `StreamKind` alone does not expose a product operation. |
| Queue pressure, coalescing, source gaps | Scheduler and daemon publisher/pump; preserve reliable rejection checkpoints, exact accounting, and writer wakeups. |
| Client route or wait lifetime | [router.rs](../../src/router.rs), [SDK client](../../../undercurrent-sdk/src/client.rs), and resilient recovery; distinguish consuming a pending handle from borrowing a stream handle. |
| Reconnect retention or expiration | [replay.rs](../../src/replay.rs), [server session](../../../undercurrent-transport-core/src/engine/session.rs), and [local server sweep](../../../undercurrent-transport-local/src/server.rs). Preserve activation/rollback ownership. |
| Error presentation or observations | [error.rs](../../src/error.rs), [registry.rs](../../src/registry.rs), SDK/server conversions, and [daemon telemetry](../../../undercurrent-daemon/src/multiplex_telemetry.rs). Keep payloads/IDs out of overload and metric labels. |
| Request deduplication, durable effects, session creation or cancellation | [Daemon dispatch](../../../undercurrent-daemon/src/dispatch.rs) and its application/store/runtime owners; replay journals only retain stream frames. |

Before extending a policy, write down the proposed invariant and the state that
owns it. In particular, preserve class reservations so bulk cannot consume control
capacity, independent message bounds so tiny frames cannot grow metadata without
limit, and separate routed/consumed checkpoints so unread data is not skipped on
resume. Keep transport work outside multiplex locks. Exactly one physical writer
must preserve selection and confirmation order; thread-safe methods do not make
multiple concurrent writers safe.

Adding an operation or policy also requires a recovery story: how overload is
reported, whether an item is coalescible, which gap recovery is valid, how an
activation failure releases state, and how the consumer knows it succeeded.
Review the [ordering limitations](../internals/scheduling.md#selection-ordering-and-fairness)
before introducing cross-class stream frames or nonconsecutive droppable loss.
Record significant changed design decisions in the repository's maintainer
decision location through its owner, and update these explanations and diagrams
with the implementation.

## Choose verification for the change

Run focused checks from the repository root:

```sh
cargo test --locked -p aixand-undercurrent-multiplex --test multiplex_contract
cargo test --locked -p aixand-undercurrent-multiplex --doc
cargo check --locked -p aixand-undercurrent-multiplex --examples
RUSTDOCFLAGS=-Dwarnings cargo doc --locked -p aixand-undercurrent-multiplex --no-deps
```

Success means all commands exit zero, the contract tests pass, and Rustdoc emits
no warnings. The crate currently has no doctest cases; a successful doctest
command alone does not exercise the example. Run the exercise above as well.

The [multiplex contract suite](../../tests/multiplex_contract.rs) is the primary
daemon-free regression source. Use this map to select the assertions a change
must preserve; the source holds the individual test names.

| Changed mechanism | Existing assertions to preserve |
| --- | --- |
| Frame and registry limits | Negotiated maximum checked before queueing; 300 streams supported; active IDs rejected. |
| Admission under pressure | Bulk leaves control reservation available; slow producers stay bounded and fair to lifecycle and peer streams; tiny reliable frames respect message bounds. |
| Loss and replacement | Separate losses preserve intervening data; only adjacent compatible gaps coalesce; gap exhaustion preserves the checkpoint; repeated replacement keeps route metadata bounded. |
| Selection and credit | Weighted control schedule; control bypasses exhausted data credit; consumed credit updates both windows atomically; a sole writable stream does not stall at its burst cap. |
| Request routes | Out-of-order responses reach the exact caller; product error disposition bytes stay exact; response rollback leaves streams alone. |
| Stream routes and teardown | Independent request/stream namespaces; per-stream increasing sequences; teardown preserves consumed sequence and releases queues; churn leaves no stale state. |
| Replay | Lifecycle transitions replay or report gaps; eviction and epoch gaps are explicit; stable clients isolate equal stream IDs; churn keeps bookkeeping bounded. |
| Expiration and activation | Admission prunes unrelated expired capacity; state survives just before the deadline; due journals cannot reactivate; pressure preserves active/future state. |
| Errors and telemetry | Overload fields are redacted with local retry guidance; registries are complete, unique, and stable; class-bounded metrics emit after unlocking. |

Open `target/doc/undercurrent_multiplex/index.html` after Rustdoc succeeds (or the
equivalent under your `CARGO_TARGET_DIR`). Rustdoc is the symbol reference; these
pages explain composition and invariants. The [project validation runner](../../../../scripts/validate.py)
adds workspace integration checks. Its `--stage rustdoc` runs warning-denied
workspace Rustdoc; `--stage tests` includes workspace tests and doctests. Those
are broader integration gates, not required daemon setup for this example.

For changes across the boundary, inspect and run the relevant
[server connection tests](../../../undercurrent-transport-core/tests/connection_contract.rs)
(confirmation, window updates, disconnect/expiry) and
[SDK tests](../../../undercurrent-sdk/tests/client_contract.rs)
(slow consumer closes connection, consumed checkpoint, reconnect).
Check [daemon policy tests](../../../undercurrent-daemon/src/stream.rs) when changing
delivery or terminal representation. Python async behavior requires its own
package checks; it is not established by these Rust router tests.

The existing tests demonstrate bounded mechanisms under their configured loads;
they are not throughput benchmarks, exhaustive race exploration, or proof of
cross-class ordering. Activation rollback is traced in the server implementation;
do not infer complete rollback-path coverage from the confirmation tests.
Registry uniqueness tests also do not prove that every
diagnostic name has a live emitter. Keep those limitations visible in a review.
For documentation edits, check links, contracts, and Mermaid accessibility;
render and visually inspect diagrams in the integration validation environment.

[Crate documentation](../README.md)

## Measure scheduler policy

From the repository root with the pinned toolchain and locked dependencies:

```sh
RUSTUP_TOOLCHAIN=1.95.0 cargo bench --locked -p aixand-undercurrent-multiplex --bench scheduler_policy
```

The [probe](../../benches/scheduler_policy.rs) holds eight queued frames per peer,
two peers per class, over 150,000 selections. It uses a 4 KiB maximum frame,
512 KiB connection queue, 64 KiB stream queues, 8 KiB starting stream credit,
the adapter's 2:2:1:3 remainder allocation, and four-frame burst cap. Payloads
are shared pre-encoded buffers. Uniform frames are 512 bytes; mixed classes use
64/256/1,024/4,096 bytes. The stalled run returns one bulk peer's consumed credit
only every 1,000 selections. Other peers return credit immediately.

A release-mode Linux x86_64 run on 2026-10-05 with Rust 1.95.0 measured:

| Workload | Control/lifecycle/state/bulk frames | Control queue p99 | Bulk queue p99 | Bulk peer counts |
| --- | --- | --- | --- | --- |
| Uniform | 80,000 / 40,000 / 20,000 / 10,000 | 10.9 µs | 74.7 µs | 5,000 / 5,000 |
| Mixed | 80,000 / 40,000 / 20,000 / 10,000 | 9.9 µs | 72.8 µs | 5,000 / 5,000 |
| Mixed with returned-credit stall | 80,000 / 40,000 / 20,000 / 10,000 | 10.2 µs | 931.8 µs | 9,699 / 301 |

All writable peers progressed. Uniform byte service followed frame frequency;
mixed bulk traffic carried 40.96 MB versus control's 5.12 MB, demonstrating why
frame weights are not byte weights. In the stalled run the eligible bulk peer
continued, and the stalled peer resumed when credit returned. These are observed
scheduler-only queue times on this machine, not socket or end-to-end service
objectives. The run processed roughly 4.2 million selections per second.

Before tuning, preserve non-starvation and the one-frame-per-class capacity
floor, compare the same workloads and larger/smaller frames, and record regressions
in control/lifecycle queue time, bulk byte throughput, and peer service gaps.
Deployment-specific end-to-end latency targets require transport, consumer, and
hardware measurements; this microbenchmark cannot supply them. The accepted
policy retains current values pending evidence that another allocation improves
those measured tradeoffs.
