# Choose the boundary for an extension

Use this guide before adding an operation, adapter, state field, or client
convenience. Start from the [code map](../architecture/dependencies.md) and the owning contract.
Rust visibility alone is not permission to add a new supported consumer surface.

| Change | Owners to update | Invariants and verification |
| --- | --- | --- |
| Product operation or error | Protocol DTOs/registries/fixtures, daemon boundary/adapter, SDK, relevant Python surface | Capability preflight, validation, authorization, effect certainty, exact contract version; protocol conformance and SDK product-client tests |
| Session rule or lifecycle transition | Domain, application command, affected store codec and presentation | Pure transition guards, revision and identity checks, event order, backward record compatibility; domain and command failure-window tests |
| New process side effect | Application port/service, durable attempt model, runtime/host and daemon composition | Admission before IO, exact semantic identity, settlement and receipt ACK, response-loss recovery, compensation preserving replacements |
| Query or projection | Application query/read model, daemon presentation, protocol/SDK | Stable cursor semantics, bounded page size, current snapshot versus historical event meaning; projection/recovery and client tests |
| Terminal output or screen | Runtime/host, application output store, presentation and stream adapters | Byte offsets versus frame sequences, delivered interval versus producer head, explicit retention gaps, digest and bounded memory |
| Scheduling or flow control | Multiplex, transport core/local, SDK recovery | Per-stream ordering, capacity ownership, write-confirmed versus consumed checkpoint, teardown and generation races |
| Configuration option | Config type/default/validator/sample, JSON schema, daemon/CLI projections | Omitted versus empty values, coupled bounds, unknown-field rejection, offline versus live behavior; parity plus boundary cases |
| Persisted field | Domain/record codec, SQLite migrations/verification, recovery, manifest if applicable | Exact record bytes and digest semantics, forward rejection, historical fixture compatibility, crash windows |
| OS primitive | Runtime and target platform adapter | Exact native identity and ownership, target error mapping, cleanup, native test execution |
| Detection profile or rule | Domain profile contract, runtime evidence, daemon reload/catalog | Bounded matching, freshness and completeness, revision revalidation, all-or-nothing candidate activation |
| Python convenience | Python client/models/state/transport and bridge when necessary | One-loop lifetime, cancellation/correlation cleanup, bounded queues and ACKs, preserved error metadata, mock and live evidence kept separate |

## Define the failure contract before adding retries

A new mutation must say how a caller distinguishes rejection before effects,
confirmed effects, and an unknown result. Define identity scope, duplicate
semantics, retained evidence, authorization on replay, and expiration. Then test
response loss after each durable commit and side effect. A digest or idempotency
key without those rules does not make retry safe.

For runtime changes, trace [creation and recovery](../architecture/flows.md) and the detailed
[attempt contract](../../crates/undercurrent-application/docs/internals/execution/attempts.md).
Do not widen a retry loop around an uncertain mutation to make a happy-path
test pass. A reconnect policy restores a connection; a stream resume policy
restores an observation; neither answers whether the mutation already occurred.

## Preserve ownership and observability

Keep pure decisions in the domain and effects behind application ports. Compose
concrete owners in the daemon. Share a queue through its read and write ports,
make shutdown interrupt blocking work, and avoid strong-reference cycles through
diagnostics. Native process identity includes creation evidence, not just a PID.
Retain typed partial failure details instead of logging arbitrary private payloads.

Update the [authoritative reference](../reference/protocol/README.md), local explanation,
user or SDK task, and affected diagrams together. Add an ADR for a significant
boundary decision; [current decisions](../architecture/decisions/README.md) explain the format.
Run the [appropriate checks](verification.md) and state platform and renderer
limitations explicitly in review.

[Documentation index](../README.md)
