# Verify and extend the SQLite adapter

Use this guide to choose the right owner, reproduce a storage issue, and select
checks that establish the behavior you changed. Prerequisites are the workspace
checkout, pinned Rust 1.95.0 toolchain, locked dependencies, native build tools for
bundled SQLite, and writable temporary storage. Run commands below from the
workspace root. No daemon is required for the crate's tests or example.
[Back to the crate guide](../../README.md).

Start with the [code map](../internals/ownership.md#code-map) and the page for the affected
behavior. Public visibility within this internal crate does not make its types a
supported consumer API. For supported clients use
[the Rust SDK](../../../undercurrent-sdk/README.md) or
[the Python SDK](../../../../python/README.md).

## Run an isolated reproduction

1. Follow [the temporary repository exercise](maintenance.md#exercise-a-temporary-repository)
   to reproduce session, outbox, reopen, and backup/restore behavior. That procedure
   includes the command, expected output, failure handling, and cleanup.
2. For richer state, choose a fixture from [the test map below](#choose-the-relevant-tests).
   Keep corruption injection and raw SQL edits in disposable test databases.
   Do not connect a fixture to a user's runtime home. For example:

   ```console
   cargo test --locked -p undercurrent-store-sqlite --test sqlite_repository
   ```

3. When the change is ready, run the crate suite. It includes the library tests
   and integration suites:

   ```console
   cargo test --locked -p undercurrent-store-sqlite --all-targets
   ```

4. Check Rust API comments and examples when those change:

   ```console
   cargo test --locked -p undercurrent-store-sqlite --doc
   RUSTDOCFLAGS='-D warnings' cargo doc --locked -p undercurrent-store-sqlite --no-deps
   cargo check --locked -p undercurrent-store-sqlite --examples
   ```

Passing tests should exit successfully with no failed cases. Rustdoc should
complete without warnings and generate the crate entry in
`target/doc/undercurrent_store_sqlite/index.html` unless `CARGO_TARGET_DIR` is set.
The example check compiles examples; it does not run their assertions. Successful
tests remove their `tempfile` databases after handles are dropped; Cargo artifacts
remain under the configured target directory. Cache or toolchain failures do
not establish a test result. Resolve them through the workspace development
setup rather than changing the lockfile for a documentation check.

The repository-wide owner uses
[scripts/validate.py](../../../../scripts/validate.py), including
its warning-denied Rustdoc stage:

```console
scripts/validate.py --stage rustdoc
```

That stage builds workspace documentation, uses the pinned toolchain and offline
Cargo, and requires the workspace dependency cache. It belongs to coordinated
repository validation. Run the focused crate checks above during local
development and coordinate the workspace stage with repository validation.

## Choose the relevant tests

Choose the suite matching the changed behavior. Use `--lib` for the in-source
tests, or `--test` followed by the integration suite name without `.rs`. Append
a test-name substring to either invocation to focus it further. The following sections
describe test coverage, not execution evidence for every platform. Use `-- --nocapture` only for controlled diagnostics that
do not print private session or agent payloads.

### [sqlite_repository.rs](../../tests/sqlite_repository.rs)

Complete private aggregate/outbox reopen; secret-reference retention; encoded/session/outbox capacity; known-absent initial refusal; independent-instance revision conflict; terminal deletion; 300-session keyset recovery; delayed corrupt/oversize read rejection; telemetry; Unix file mode; snapshot preservation and exact-release rejection.

### [crash_boundaries.rs](../../tests/crash_boundaries.rs)

Insert/commit/delete after outbox, aggregate, counters and before commit; acknowledgement after delete/counters and before commit; reopen confirms rollback of all related state.

### [durability.rs](../../tests/durability.rs)

Local fence allocation, cell-scoped dispatched fences, shutdown cancellation exception, policy/digest provenance, adversarial schema/ledger rejection before mutation, scan exclusion/count bounds, WAL backup/restore rollback, initial schema/attempt failures, audited repair, current schema identity; Windows account-owned files/sidecars.

### [runtime_attempt_status.rs](../../tests/runtime_attempt_status.rs)

Every retained status state across reopen; byte-for-byte unchanged typed attempts/sessions/outbox after queries; digest/provenance preflight before session lookup.

### [runtime_cancellation.rs](../../tests/runtime_cancellation.rs)

Atomic admission/replay, stale revision rejection, exact retries before marker, terminal commit races, in-flight unknown across restarts, monotonic state, all-controls recovery claim, corrupt discriminators/identity grammars/pins and timestamp overflow.

### [agent_channel.rs](../../tests/agent_channel.rs)

Current target resolution, pending/replied replay across reopen, conflicting calls, exact replies, stale fence/expiry, digest and 32-unsettled-event limits.

### [sqlite/tests.rs](../../src/sqlite/tests.rs)

Prepared compensation rollback, exact intent survival, terminal audit, duplicate/conflict rules, preservation of higher attempt outcomes and readiness clearing.

### [codec/mod.rs](../../src/sqlite/codec/mod.rs)

Row/envelope versions, pre-parse byte limits, lifecycle revision, profile binding, capture modes, executable/environment provenance and required-field rejection.

### [database.rs unit tests](../../src/sqlite/database.rs)

SQL normalization preserves token boundaries; policy debug omits private references/digest; policy version/retention and retired callback validation.

Useful focused commands for recovery work:

```console
cargo test --locked -p undercurrent-store-sqlite --test crash_boundaries
cargo test --locked -p undercurrent-store-sqlite --test durability wal_backup_is_verified_and_restore_rolls_back_on_interruption
cargo test --locked -p undercurrent-store-sqlite --test runtime_cancellation
cargo test --locked -p undercurrent-store-sqlite --test agent_channel
```

`SqliteCrashInjector` is a `Send + Sync` predicate supplied through explicit
test opens/options; ordinary opens use `NoopSqliteCrashInjector`.
[crash.rs](../../src/sqlite/crash.rs) lists stable points for schema creation,
session writes, acknowledgements, attempt accept/settle/reconcile/repair,
compensation prepare/complete, and restore after installation before verification.
The transaction hooks return errors and let transactions drop; they do not kill
the process or simulate storage hardware failure. The restore hook also returns
an error, exercising the rollback handler rather than bypassing it. There are
no dedicated cancellation/agent-channel crash points in this enum.

These tests prove the exercised returned-error and reopen paths. They do not
establish arbitrary power-loss atomicity, exactly-once external process effects,
bounded total database growth, exhaustive session-payload verification at open,
or identical Windows/Unix directory synchronization. Application/runtime tests
are also needed for orchestration changes; SQLite tests alone cannot prove
transport response-loss, authorization, or process receipt behavior.

## Extend the right layer

| Proposed change | Owning work and invariants to preserve | Relevant validation |
| --- | --- | --- |
| New session field or domain invariant | Domain constructors first; aggregate/state/0.1.0-beta conversions as appropriate; explicit missing-field policy; rehydrate complete state and preserve private values without exposing them | Codec roundtrip/malformed/unsupported-version cases and complete-session reopen |
| New domain event kind | Domain/application contract, then both SQL encode/decode mappings; monotonic revision suffix and atomic snapshot/outbox/counters | Session transaction, snapshot and acknowledgement tests; publisher/presentation tests upstream |
| New persistence port operation | Application owns the port; worker request/reply and implementation belong here; clone request data and preserve admission/cancellation semantics | Queue failure classification, conflict and reopen evidence; no process I/O on the DB thread |
| Schema change | Complete initial SQL schema, current fingerprint, identity evidence and validators; reject other layouts | Fresh/current opens, fault rollback, tampered layout rejection, maintenance verification/backup/restore |
| New attempt state or scope | Domain transition rules, row discriminator/constraints, decoder, unresolved scan, status, audit and recovery callers | Fencing, duplicates/conflicts, readiness, compensation and repair; runtime evidence integration |
| Cancellation behavior | Application orchestration plus raw 0/4/3 distinction, state monotonicity and terminal commit coupling | Retry-before-marker, marker-before-I/O, repeated restart and concurrent terminal commit tests |
| Agent binding or payload | Domain/application contract, row bounds/digests, target validation, retention and replay | Exact bytes/identity replay, expiry/fence, capacity, reopen and upstream authentication |
| Maintenance change | Independent connections, source/target exclusion, stage errors, sidecars, sync and rollback branches | WAL-aware backup, returned install failure, cleanup/rollback errors, platform ownership checks |

Keep each format definition in its owner. Do not derive a schema change from a
README example or silently accept a historical prerelease because it shares a
version number. Retain the reason for each boundary: revision checks prevent lost
updates; outbox transactions prevent lost publication intent; fences prevent
stale effects; immutable provenance prevents fabricated authority; the cancellation
marker prevents blind redrive; cleanup intent pins exactly what may be destroyed.

Before changing retries, read [attempts](../internals/transactions/attempts.md),
[runtime cancellation](../internals/transactions/runtime-cancellation.md), and
[the worker lifetime](../internals/ownership.md#open-share-and-close-a-repository).
Any mutation admitted to the worker can finish after the caller disappears.
An internal port failure and a public transport failure are different layers;
neither is blanket permission to resubmit a mutation.

## Review documentation and diagrams

Check relative links and anchors from each owning page, code/test paths, page
navigation, and facts against current SQL/codec/port definitions. Use
`git diff --check` and inspect all changes including untracked pages/examples.
Rust source changes in a documentation-only assignment must be documentation
comments only. Do not regenerate production code, change manifests/lockfiles,
or alter tests to hide a failing documentation check.

Mermaid source remains beside each explanation. Render changed diagrams with the
repository's available renderer and visually inspect labels, branch ordering,
clipping and readability; parsing/accessibility metadata alone is not sufficient.
Follow the shared [verification guide](../../../../docs/development/verification.md)
and record which checks actually ran and which still need platform evidence.

Cross-crate walkthroughs belong in shared maintainer documentation. Link to the
local guides rather than copying their diagrams or format tables. Coordinate
changes affecting [application](../../../undercurrent-application/README.md),
[runtime](../../../undercurrent-runtime/README.md), and
[daemon composition](../../../undercurrent-daemon/README.md); this crate does not
own consumer transports or process lifecycle policy.

[Crate documentation](../README.md)
