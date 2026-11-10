# Find the owner of a foundational mechanism

Foundation keeps representation and validation consistent without acquiring
product policy or infrastructure dependencies. This explanation is for
maintainers to package 0.1.0-beta; start at the [crate README](../../README.md) for
audience navigation. The [workspace manifest](../../../../Cargo.toml),
[crate manifest](../../Cargo.toml), and
[architecture policy](../../../../tests/architecture/allowed-graph.json) define
its placement as `shared_foundational_mechanisms`.

## Dependency boundary

Foundation has no first-party production dependency. Its direct callers are
Domain, Protocol, Configuration, Wire, Runtime, and the SQLite adapter; those callers have their own
dependencies. See the
[workspace dependency map](../../../../docs/architecture/dependencies.md#manifest-map)
for consumers and [Cargo.toml](../../Cargo.toml) for exact dependency declarations.

Serialization, canonical JSON, hashing, and constant-time comparison use the
libraries selected by that manifest; their behavioral contracts are explained
in [value mechanisms](../reference/values.md). `jsonschema` and `proptest` are development
dependencies for schema checks and canonicalization properties. Changing the
inward dependency boundary requires architecture review.

## Code map

All five modules are private, with selected names re-exported at the crate root.
Generated Rustdoc is the symbol reference; the following map groups behavior.

| Source and responsibility | Investigation path |
| --- | --- |
| [Crate root](../../src/lib.rs): Root exports and `deny(missing_docs)`; no runtime initialization. | [Supported surfaces](../../../../tests/architecture/allowed-graph.json) and [architecture gate](../../../../tests/architecture/architecture_gate.py). |
| [Bounded strings](../../src/bounded.rs): Check decoded UTF-8 byte length before making the wrapper's own copy. | Inline tests cover plain/escaped input and oversized scalar deserialization; [scalar limits](../reference/values.md#bound-decoded-strings). |
| [Canonical JSON](../../src/canonical.rs): Preflight decoded JSON, then bound canonical output. | Inline golden, limit, and property tests; [canonicalization contract](../reference/values.md#canonicalize-decoded-json). |
| [Digests](../../src/digest.rs): Hash exact bytes, preserve one wire form, explicitly compare digest bytes in constant time. | Inline SHA-256 and schema tests; [digest contract](../reference/values.md#identify-and-verify-bytes). |
| [Privacy](../../src/privacy.rs): Stable classification labels and a bounded-depth conservative scanner. | Inline normalization, classification, and scanner tests; [privacy policy](privacy.md). |
| [Opaque URIs](../../src/uri.rs): Preserve an ASCII opaque reference after structural and selected credential checks. | Inline URI and schema tests; [URI contract and schema limits](../reference/values.md#validate-an-opaque-reference). |

## From a session request to an integrity value

To investigate a session fingerprint, start at protocol's
[`canonical_product_request_digest`](../../../undercurrent-protocol/src/canonical.rs).
It serializes the typed request to a `Value`, removes `input.mutation` when
`input` is an object, and calls Foundation with `CanonicalJsonLimits::DEFAULT`.
It hashes the returned canonical bytes with `Digest::sha256`. Serialization or
canonicalization failure becomes `InvalidCanonicalPayload`.

Field selection belongs to protocol. Foundation leaves the supplied value
unchanged; its [preflight and bounded writer](../reference/values.md#preflight-and-allocation)
only govern canonicalization. See the shared
[semantic fingerprint contract](../../../../docs/reference/protocol/encoding.md#semantic-fingerprints-and-output-digests)
for metadata exclusions, and the
[retry-metadata test](../../../undercurrent-protocol/tests/refactoring_contract.rs)
and [dispatch tests](../../../undercurrent-protocol/tests/dispatch_binding.rs)
for changes that preserve or alter a fingerprint.

The [daemon boundary](../../../undercurrent-daemon/src/boundary.rs) compares an
attached start's claimed digest with that fingerprint before constructing its
domain dispatch binding. Digest construction has no durable commit or process
effect and does not accept, execute, or deduplicate a mutation. Continue with
the shared [session creation flow](../../../../docs/architecture/flows.md)
for admission, persistence, and launch.

## Other callers and ownership limits

Use this map to continue past Foundation into the owner of a product rule.

| Caller and use of Foundation | What stays with the caller |
| --- | --- |
| [Agent protocol helpers](../../../undercurrent-protocol/src/agent.rs): Canonicalize bindings, schemas, calls, and outcomes with document-specific limits; hash canonical schema bytes. | Which document is canonicalized, frame limits, and admission/reply semantics. |
| [Runtime cancellation protocol](../../../undercurrent-protocol/src/runtime_cancellation.rs): Canonicalize the selected command fields, then hash. | Digest domain, field selection, target identity, deadlines, authorization, and cancellation lifecycle. |
| [Execution output helpers](../../../undercurrent-protocol/src/execution_output_digest.rs): Hash exact payload bytes and verify a digest after checking its syntax. | DTOs use 64 lowercase hex characters, omitting Foundation's `sha256:` prefix. |
| [Terminal output](../../../undercurrent-daemon/src/presentation/feed.rs): Hash a returned data page through the protocol helper. | Replay offsets, gaps, snapshot recovery, and retention are modeled by the replay owner. A matching digest does not prove gap-free output. |
| [Domain attempts](../../../undercurrent-domain/src/attempt.rs): Wrap content digests and opaque policy/callback references. | Fencing, attempt transitions, callback-specific shape, and unknown-effect reconciliation. |
| [Environment](../../../undercurrent-protocol/src/dto.rs): Scan raw environment variables and reject sensitive values during serialization. | Secret-reference resolution, persistence validation, and errors exposed to clients. See [the privacy boundary](privacy.md#use-the-result-at-a-boundary). |
| [SQLite records](../../../undercurrent-store-sqlite/src/sqlite/database.rs): Retain Foundation classification labels in dispatch output contracts. | Transactions, commits, schema admission, and persisted record formats. |

## Execution and resource ownership

Foundation functions are synchronous computations on caller-provided memory.
They create no threads, locks, subscriptions, sockets, files, or background work.
Returned strings/vectors belong to the caller; borrows remain tied to the input
or wrapper. The detector keeps a private field-name set that is read through
`&self`. Independent calls share no mutable global state. Dropping values
releases ordinary allocations; there is no shutdown protocol or zeroization
guarantee.

There are no deadlines, cancellation points, retry timers, or transport errors
in this crate. Size/depth checks bound selected work, not wall-clock execution
time or total process memory. Errors carry structural reasons, not raw input;
Foundation emits no logs or metrics. Callers own diagnostic policy, including
[safe diagnostic context](privacy.md#use-the-result-at-a-boundary).

Re-running a failed local validation with unchanged inputs does not recover a
remote effect. After response loss, use the owning
operation's [status and reconciliation contract](../../../../docs/reference/protocol/recovery.md);
Foundation supplies no basis for automatically retrying a process-affecting
mutation.

Continue with [value contracts](../reference/values.md), [privacy policy](privacy.md), or
[focused verification and extension guidance](../development/contributing.md).

[Crate documentation](../README.md)
