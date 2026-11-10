# Interpret core errors

Use the category to locate the failing boundary. It does not establish whether
a product effect occurred. This reference is derived from [error.rs](../../src/engine/error.rs); the owning
wire and multiplex errors retain their concrete causes.

## Connection error categories

### `configuration`

Zero diagnostic/connection limits, missing required capability or inconsistent
composition. Fix negotiated/composed settings; retries do not repair them.

### `protocol`

Request/stream-kind mismatches, duplicate connection/client IDs, pagination
errors; multiplex invalid-argument, conflict or replay-unavailable errors;
invalid or oversized wire frames. Inspect identity, cursor and framing at the
owning boundary.

### `overloaded`

Multiplex resource saturation, queue bounds and credit failures. Inspect
`overload()` for the payload-free resource, limit/current/requested values and
guidance.

### `unavailable`

Multiplex connection closed. Replace the connection after preserving unresolved
mutation outcomes.

### `invariant_violation`

Stream/scheduler divergence, multiplex counter invariants or wire invariant
failure. Investigate state composition and ordering before retrying unchanged input.

## Overload hints and effect evidence

`overload()` is available only for wrapped multiplex overloads. It distinguishes
capacity availability, queue drainage, window updates and input that should not
be retried unchanged. Consult [admission](../internals/request-lifecycle.md#request-ids-survive-settlement)
for the non-evicting request-ledger exception. The core category does not decide
whether a product effect occurred. Typed product rejection, dispatch-admission
failure, framing/IO failure and response loss are different evidence.

## Idempotency errors

The idempotency store has its own error domain, not a `ServerConnectionError`
category: invalid request/response size, record capacity, byte capacity, or missing
reservation. Conflicting bytes are result variants, not thrown errors. Limit
construction separately distinguishes zero bounds from total bytes too small
for one maximum record. Use [cache rules](../internals/idempotency.md#identity-and-bounded-retention)
when diagnosing retained-byte pressure or settlement failure.

[Crate documentation](../README.md)
