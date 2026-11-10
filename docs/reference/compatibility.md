# Distinguish versions, support declarations, and implementation evidence

Use this reference when matching clients, configuring a runtime, or opening
existing state. The versions below describe this source checkout. For installed
binaries, check their version output and the accompanying release metadata.

## Development version

This checkout's development version is **v0.1.0-beta**. The product, all 19 Rust crates,
public SDK, product contracts, configuration, profiles, environment payloads,
wire formats, private journals, and report schemas belong to that release.
Cargo metadata spells the version `0.1.0-beta`; Python packaging uses its canonical
beta spelling `0.1.0b0`.

Clients, daemon, session host, and Python bridge must come from matching builds.
Readers accept the exact current release identity and reject other versions.
There are no supported legacy contracts, storage migrations, or compatibility
windows. Schema identities retain their namespace, for example
`undercurrent.package/v0.1.0-beta`.

SQLite requires numeric `PRAGMA user_version = 1` and numeric row format markers.
Its `schema_identity` table separately records `0.1.0-beta`, the complete initial
layout, and its checksum. Opening an existing database requires that exact
identity and layout fingerprint before writable access; it never upgrades state.

## Known contract and implementation differences

These are source-backed limitations requiring owner review, not new guarantees.

| Difference | Reader implication and evidence |
| --- | --- |
| Package schema identity alone does not prove database compatibility | Let the storage adapter validate exact layouts; require the current release identity and full SQL fingerprint. [Storage format](../../crates/undercurrent-store-sqlite/docs/reference/storage-format.md). |
| Semantic delivery reservations persist in the current SQLite schema | New reservations survive daemon replacement; indeterminate outcomes never authorize another write. [Python recovery](../sdk/python/recovery.md#reconcile-a-mutation). |
| Scheduler port describes durable accepted work; queue is in memory | Durable session intent supports startup reconstruction, but an enqueue failure is not automatically repaired by every retry. [Application recovery](../../crates/undercurrent-application/docs/internals/recovery/startup.md). |
| Output `next_offset` is producer head, while Python omits `to_offset` | Page by actual delivered interval; fixed examples reject malformed/non-progressing pages. CLI follow continues at the delivered seed end. [Output guide](../guides/cli/status-and-output.md). |
| Runtime configuration validation is stricter than structural schema validation | Some numeric runtime constraints are stricter; `default_profile` accepts string or null and applies to CLI starts without an explicit profile. [Configuration reference](configuration-fields.md). |
| Runtime-dispatch policy retains resource ceilings | Retention/validation does not claim OS enforcement of those ceilings. [Policy model](../../crates/undercurrent-domain/docs/internals/attempts-and-recovery.md#dispatch-policy-and-agent-bindings). |
| Saturated stream-close queues can affect peer subscriptions | Product recovery validates activation responses, and the writer retries deferred credit. A close that cannot enter the writer still closes the shared physical connection. [SDK stream limitations](../sdk/rust/streams-and-recovery.md#pressure-and-current-recovery-limitations). |
| Graceful runtime cancellation and graceful-only stop are distinct | Graceful cancellation follows the session stop policy and may escalate; do not infer a no-force guarantee from its name. [Application control guide](../../crates/undercurrent-application/docs/internals/execution/runtime-controls.md). |
| Private host recovery has epoch, frame-limit and retry limitations | Host replacement within one adapter can reuse its epoch, environment frame-bound calculations differ, and failed replay workers are not perpetual retries. [Runtime recovery](../../crates/undercurrent-runtime/docs/internals/host/recovery.md), [host framing](../../crates/undercurrent-runtime/docs/reference/host-protocol.md). |

## Matching state and builds

Use a fresh `UNDERCURRENT_HOME` for the first beta baseline. Existing development
state belongs to the build that created it and is not converted. Reconcile and
stop work before replacing binaries. Current cancellation hashes, receipt
references, and process identities use the beta namespace; changing their
namespace changes their identity.

## Platform support and installation

[Platform support](platforms.md) distinguishes native adapters,
declared targets and current validation entry points. Source presence alone
does not establish a qualified artifact or downloadable release.

For replacement and recovery use [installation](../getting-started/installation.md),
[configuration diagnosis](../guides/configuration/diagnose.md), and
[storage recovery](../guides/daemon/storage.md). Preserve a compatible backup
and reject unsupported formats rather than silently reinterpreting them.

[Documentation index](../README.md)
