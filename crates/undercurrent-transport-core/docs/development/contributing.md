# Change and diagnose the server engine

Use this guide to reproduce a core invariant without starting a daemon, select
its owning implementation, and choose follow-up checks. It assumes a checkout
of this workspace, its [pinned Rust 1.95.0 toolchain](../../../../rust-toolchain.toml),
and available locked dependencies. Run all commands below from the repository
root. Core's package is internal; these are maintainer commands, not a public
SDK setup recipe.

## Run focused checks

1. Run the three maintained integration-test targets. The crate has no feature flags.
   They use in-memory fixtures and, where necessary, threads; no daemon, OS endpoint,
   real session process or runtime home is required.

   ```sh
   cargo test --locked -p aixand-undercurrent-transport-core --test connection_contract
   cargo test --locked -p aixand-undercurrent-transport-core --test idempotency_contract
   cargo test --locked -p aixand-undercurrent-transport-core --test dispatch_contract
   ```

   Expected result: each selected test binary reports success. A test failing before
   compilation because the pinned toolchain or a dependency is unavailable means
   setup is incomplete, not that the contract passed. The standalone validation
   runner uses offline Cargo, so its dependencies must already be cached. Do not
   change manifests or the lockfile to conceal a local setup failure.

2. To isolate replay confirmation or pending mutation ambiguity, use an exact filter:

   ```sh
   cargo test --locked -p aixand-undercurrent-transport-core --test connection_contract successful_physical_write_is_the_replay_commit_boundary -- --exact
   cargo test --locked -p aixand-undercurrent-transport-core --test idempotency_contract pending_ambiguity_never_expires_silently_and_requires_explicit_abandonment -- --exact
   ```

   Verify that the output reports **one** executed test for each exact filter; a
   misspelled filter can yield zero tests and a successful exit. For a complete
   cache-use example, follow [retained results](../internals/idempotency.md#run-a-daemon-free-retention-example).

3. After changing API comments or examples, run:

   ```sh
   RUSTDOCFLAGS='-D warnings' cargo doc --locked -p aixand-undercurrent-transport-core --no-deps
   cargo test --locked -p aixand-undercurrent-transport-core --doc
   cargo check --locked -p aixand-undercurrent-transport-core --examples
   cargo run --locked -p aixand-undercurrent-transport-core --example retained_result
   ```

   Generated API documentation starts at
   `target/doc/undercurrent_transport_core/index.html` under the default Cargo target
   directory. A generated artifact is not a tracked navigation dependency. There are
   no daemon processes or persistent runtime resources to clean up after these
   checks; Cargo's build artifacts remain in its configured target directory.


The repository-wide authority is
[`scripts/validate.py`](../../../../scripts/validate.py).
Its `tests` stage includes doctests, and its `rustdoc` stage uses warning-denied
Rustdoc. After a change spanning crate boundaries, the repository checks include:

```sh
python3 scripts/validate.py --stage tests
python3 scripts/validate.py --stage rustdoc
```

They cover more than this crate and can require platform/runtime prerequisites.
A crate-only success does not establish their success.

## Select evidence for the behavior you changed

Use this map to select a regression test and identify checks in adjacent crates.

| Behavior | Focused evidence |
| --- | --- |
| Writer wakes and pressure | Connection tests: [idle wait and close](../../tests/connection_contract.rs#L28), [credit wait](../../tests/connection_contract.rs#L77), [dequeue wake](../../tests/connection_contract.rs#L139). |
| Request IDs and cancellation | [Bounded, out-of-order completion](../../tests/connection_contract.rs#L156); [one terminal cancellation with the right codec family](../../tests/connection_contract.rs#L232). |
| Stream scale and disconnect | [300 selective streams](../../tests/connection_contract.rs#L359), [disconnect and expiry](../../tests/connection_contract.rs#L320), [owned-state release](../../tests/connection_contract.rs#L976). |
| Replay boundary | [Successful write](../../tests/connection_contract.rs#L542), [unconfirmed drop](../../tests/connection_contract.rs#L634), [missing replay becomes a retained gap](../../tests/connection_contract.rs#L743). |
| Source loss and priority | [Source gap ordering](../../tests/connection_contract.rs#L676), [heartbeat priority](../../tests/connection_contract.rs#L791), [fairness under terminal flood](../../tests/connection_contract.rs#L834). |
| Diagnostic work and limits | [Opening rows](../../tests/connection_contract.rs#L417), [exact active queue bounds](../../tests/connection_contract.rs#L451), [bounded 300-session page](../../tests/connection_contract.rs#L1084), [stream delegation](../../tests/connection_contract.rs#L1171), [page-size bound](../../tests/connection_contract.rs#L1207). |
| Diagnostic retention and registration | [Unchanged replay deadlines](../../tests/connection_contract.rs#L279), [one connection per client and lease drop](../../tests/connection_contract.rs#L1038), [weak ownership](../../tests/connection_contract.rs#L1132), [stable diagnostic names](../../tests/connection_contract.rs#L1019). |
| Cache identity and expiry | Idempotency tests: [exact result and redaction](../../tests/idempotency_contract.rs#L14), [exact product error](../../tests/idempotency_contract.rs#L76), [pending ambiguity](../../tests/idempotency_contract.rs#L102), [indexed expiry and released bytes](../../tests/idempotency_contract.rs#L126). |
| Cache isolation and reservation | [Client retirement](../../tests/idempotency_contract.rs#L146); [reserve maximum response space before execution](../../tests/idempotency_contract.rs#L171). |
| Dispatch failure presentation | [Redacted error with inspectable source](../../tests/dispatch_contract.rs#L9). |
| Product integration | [Daemon product dispatch tests](../../../undercurrent-daemon/tests/product_dispatch.rs): response-loss replay across capability changes, reliable-publisher backpressure without sequence loss, and runtime-cancellation reauthorization. |
| Scheduler, replay and wire algorithms | [Multiplex tests](../../../undercurrent-multiplex/tests/multiplex_contract.rs), [wire tests](../../../undercurrent-wire/tests/wire_contract.rs), [handshake tests](../../../undercurrent-wire/tests/handshake_contract.rs). |

The 300-stream tests establish bounded in-memory behavior for their configured
fixtures, not a production throughput qualification. Core's tests explicitly
simulate write confirmation and cannot prove native peer verification, physical
write deadlines, durable commits or crash recovery. Those belong to adapter,
daemon, application and store checks. Existing core tests do not exhaustively
exercise every activation rollback branch, ledger-capacity boundary, or every
cross-thread ordering; add targeted coverage when changing those paths rather
than inferring coverage from an adjacent happy path.

## Inspect pressure without payloads

Use the [diagnostics reference](../reference/diagnostics.md) to interpret pending
work, retained request IDs, queue pressure and weak session references. For a
stalled writer or a gap, follow the
[recovery decision tree](../internals/streams-and-recovery.md#diagnose-loss-before-retrying).

## Interpret failures at the correct boundary

Use the [error reference](../reference/errors.md) for category mappings and
overload hints. Preserve operation-specific outcome evidence before deciding
whether an uncertain mutation can be retried.

## Choose the owning layer

| Intended change | Owner and required follow-through |
| --- | --- |
| New product operation or authorization rule | Protocol registry/contract and daemon/application executor. Core normally keeps the bytes opaque. Check product binding, errors, effect certainty and operation-specific retry evidence. |
| New connection message | Wire codec and multiplex message contract, then core `accept`, local adapter correlation and SDK routing. Check unknown IDs, bounds, cancellation and compatibility. |
| New stream kind/policy | Product stream registry and daemon `ProductStreamPolicy`, with multiplex scheduler/recovery semantics as needed. Check source gaps, credit stalls, saturation, replay activation and last-sequence handoff. |
| Queue, credit or retention algorithm | Multiplex owner. Core integrates activation, publication and confirmation; preserve failed-activation rollback and distinguish queue admission from write success. |
| Authentication or physical timeout | Local adapter; never trust an arbitrary logical client ID as authenticated identity. Keep credentials outside the core context. |
| New diagnostic field | Connection/session diagnostic types and daemon projection. Maintain bounded pagination, payload exclusion and weak ownership. Add a stable registry name only when its meaning and emission site are defined. |
| New mutation-cache behavior | Core idempotency store plus daemon cache selection/projection. Preserve maximum-response reservation, exact identity, no silent pending expiry and operation-specific bypasses. |

1. Locate the implementation in the [source map](../internals/ownership.md#code-map)
   and trace the affected [request](../internals/request-lifecycle.md) or
   [stream](../internals/streams-and-recovery.md) path.
2. Change the owning layer and add a regression at the boundary you changed.
   Exercise rejection or rollback as well as successful admission.
3. Run the [focused checks](#run-focused-checks) and use the test map above for
   adjacent coverage. Confirm exact test filters execute a test.
4. For dependency or composition changes, also use the
   [architecture gate](../../../../docs/development/verification.md).
   Keep durable store/runtime decisions with their owning crates.

[Crate documentation](../README.md)
