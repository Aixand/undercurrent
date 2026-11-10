# Locate runtime-dispatch policy and its enforcement limits

This overview locates the current dispatch-policy contract and its enforcement
boundaries. [Compatibility](../reference/compatibility.md) distinguishes package,
product, wire, and schema versions.

The [domain policy explanation](../../crates/undercurrent-domain/docs/internals/attempts-and-recovery.md)
owns dispatch identities, fences, policy evidence and agent bindings.
[Application attempts](../../crates/undercurrent-application/docs/internals/execution/attempts.md)
owns admission, settlement, replay, reconciliation and durable readiness.
The [protocol operation reference](../reference/protocol/README.md) links public DTOs,
registries and compatibility rules.

Current start checks require retained policy evidence on dispatched requests
and enforce freshness at specific admission boundaries. An exact already-applied
replay can use retained result evidence rather than spawn again. Policy resource
ceilings being represented, validated and persisted does **not** establish that
the native runtime enforces every requested OS limit.

Cancellation needs a separate authorized control bound to the exact target
start attempt. Transport ownership or well-formed caller references alone do
not grant semantic cancellation authority. See
[runtime controls](../../crates/undercurrent-application/docs/internals/execution/runtime-controls.md)
and [cancellation DTO contract](../reference/protocol/cancellation.md).
Graceful cancellation follows stop policy and may escalate; it is distinct
from the no-escalation `GracefulOnly` stop mode.

Storage decoding and exact reserialization/digest checks also constrain policy
compatibility. A structurally permitted old field omission is not proof that
every historical byte fixture is accepted. Use the
[storage-format reference](../../crates/undercurrent-store-sqlite/docs/reference/storage-format.md)
and actual fixtures for a compatibility change.

[SDK recovery](../sdk/recovery.md) · [Documentation index](../README.md)
