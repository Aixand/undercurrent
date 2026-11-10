# Interpret errors without inventing effect certainty

Use structured error fields to distinguish the failed boundary, its reason,
and what is known about an effect. A timeout, unavailable transport, or typed
server rejection does not by itself prove that no process or durable mutation
occurred. See [recovery](../sdk/recovery.md) for the decision procedure.

Native CLI failures also include an `incident_id` support reference when process
capture is available. It links retained diagnostic evidence and does not change
error disposition, exit status, or effect certainty. Use
[collecting a support report](../guides/daemon/diagnostics.md#collect-a-support-report)
to export available evidence, including when the daemon is stopped.

## Product categories

The exact 19-category inventory belongs to
[registry.rs](../../crates/undercurrent-protocol/src/registry.rs) and
[its fixture](../../crates/undercurrent-protocol/tests/fixtures/registry_errors.json).
The entries group lookup values without changing their distinctions. Ordinary
dispositions come from `ProductErrorDisposition::default_for_category` in that
registry. A producer can override them with stronger evidence. **Consumers must
not use these defaults to fill absent metadata.**

**`invalid_request`, `invalid_configuration`**

Default disposition: `semantic_rejection`. Check the rejected shape/field, exact contract, and daemon configuration. Check whether rejection is explicitly pre-effect.

**`not_found`, `name_conflict`**

Default disposition: `semantic_rejection`. Verify stable identity and name ownership; do not redirect uncertain work to a replacement session or create a duplicate to hide prior effects.

**`failed_precondition`**

Default disposition: `semantic_rejection`. Inspect current lifecycle, fence, pending attempt, or retained binding. This broad category is not proof of absence.

**`revision_precondition_failed`**

Default disposition: `semantic_rejection`. The explicit stop/input revision check failed under the lease before effects; refresh state before deciding on a new action. Do not silently discard the guard.

**`unauthenticated`, `permission_denied`**

Default disposition: `access_rejected`. Restore authenticated authority; do not probe other identities to evade scope.

**`cancelled`**

Default disposition: `cancellation`. Inspect correlated terminal response and effect evidence.

**`timed_out`**

Default disposition: `timeout`. Reconcile any possible mutation effect before another command.

**`capacity_exceeded`**

Default disposition: `capacity_rejected`. Inspect the resource, limit/delay, and retained pending work; reduce request size or pressure while preserving attempt identity.

**`replay_unavailable`, `cursor_expired`**

Default disposition: `partial`. Follow resynchronization: snapshot, query restart, or available output offset.

**`protocol_mismatch`**

Default disposition: `protocol_failure`. Check versions/capabilities and adapter correlation.

**`process`, `terminal`**

Default disposition: `runtime_failure`. Inspect durable lifecycle/attempt and runtime evidence.

**`persistence`, `internal`**

Default disposition: `infrastructure_failure`. Inspect safe daemon diagnostics; a storage error can occur after a process effect.

**`transport`**

Default disposition: `transport_failure`. Reconnect as needed; retain unresolved mutation identity.

`unknown_inconclusive` has no ordinary category mapping. All categories locate a
failure; inspect the stage, disposition, certainty, and diagnostics before
concluding that an earlier effect was absent.

## Read the evidence fields independently

The owning shape is `ProductError` in [dto.rs](../../crates/undercurrent-protocol/src/dto.rs),
with validation in [contract.rs](../../crates/undercurrent-protocol/src/contract.rs).
Use the fields together; category alone does not establish effect certainty.

| Field | Meaning and use |
| --- | --- |
| `operation`, `session_id` | Optional operation and exact target context. `operation` can be absent when request decoding failed first. |
| `category`, `message` | Product failure category and its presented explanation. |
| `disposition` | Optional high-level outcome, emitted when its capability is negotiated. See the distinctions below. |
| `effect_certainty` | Optional `known_absent`, `known_applied`, or `unknown`, emitted when its capability is negotiated. Missing evidence must remain missing. |
| `retryable`, `retry_after_millis` | Retry hint plus optional delay. Apply the exact operation's retained-identity and replay contract before retrying. |
| `limit` | Optional resource-limit detail. |
| `resynchronization` | Optional required action: `request_snapshot`, `restart_query`, or `restart_from_available_offset`. |

`ProductErrorDisposition` has its own [registry](../../crates/undercurrent-protocol/src/registry.rs):

| Disposition | Distinction |
| --- | --- |
| `semantic_rejection` | Request or current semantic state rejected the operation. |
| `access_rejected` | Authentication or authorization rejected access. |
| `capacity_rejected` | A capacity or rate limit rejected work. |
| `runtime_failure` | A supervised process or terminal runtime failed. |
| `infrastructure_failure` | A durable or internal infrastructure boundary failed. |
| `transport_failure` | Physical connection or transport failed. |
| `protocol_failure` | Negotiation, decoding, or correlation failed. |
| `timeout` | A bounded deadline elapsed. |
| `cancellation` | Cancellation interrupted the operation. |
| `partial` | Some requested data or work remains unavailable or incomplete. |
| `unknown_inconclusive` | Effect or reconciliation evidence remains unresolved. |

These values describe the disposition of the failure. Read `effect_certainty`
separately: neither a disposition nor a category supplies a missing certainty.

## Presentation by client

### Rust

`ProductClientError` distinguishes connection, capability, codec, typed
server/stream rejection, operation/kind mismatch, and subscription failure.
The [error implementation](../../crates/undercurrent-sdk/src/product/error.rs) owns these variants.

Its presentation categories are `invalid_argument`, `unavailable`, `overloaded`,
`timeout`, `cancelled`, `server_rejected`, and `protocol_violation`. These are
SDK presentation categories, separate from the product inventory above.

### Python

`UndercurrentError` carries optional `category`, `product_category`,
`disposition`, and `effect_certainty`. The bridge category uses Rust Debug
spelling such as `ServerRejected`. Ordinary validation, IO, timeout, and
cancellation exceptions can also occur; see the [Python error reference](../sdk/python/recovery.md#classify-the-failure).

### CLI

The stderr failure report includes a CLI category and optional product category,
disposition, and certainty. See the [CLI exit/output reference](cli-output.md#exit-categories-and-machine-errors).

The [CLI exit-code table](cli-output.md#exit-categories-and-machine-errors)
owns the numeric values and the `exec` child-exit exception. Finite output gaps
and invalid profile lint reports can be successful requests whose payload still
needs interpretation.

### Projection limits

Python `OutputGap` exposes only the earliest available offset;
`SessionEnded` retains its terminal snapshot; `SessionRemoved` represents a
watch's exact-ID disappearance. Not every product `not_found` is converted to
that subclass. Rust preserves richer typed metadata, while the CLI and Python
wrappers intentionally expose smaller projections.

## Preserve useful diagnostics

Record safe categories, intended operation, timestamps, and exact identities
you already own. Normal session detail, terminal output, environment context,
and nested error `Debug` values can contain private content. Payload-safe
operational telemetry does not imply every return value is redacted. Use
[daemon diagnostics](../guides/daemon/diagnostics.md)
and [SDK telemetry](../sdk/rust/limits-and-diagnostics.md)
for supported bounded exports.

[Operator troubleshooting](../guides/troubleshooting.md) · [Documentation index](../README.md)
