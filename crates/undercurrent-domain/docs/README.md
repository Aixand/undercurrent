# undercurrent-domain maintainer documentation

Domain owns pure session invariants. Application supplies evidence, commits
transitions, and invokes adapters. Use the [model and source map](internals/model.md#code-map-by-question)
to locate a rule, then follow its trace below. For a local reproduction, start
with the [in-memory model](development/contributing.md#run-the-model-without-a-daemon)
and select [regression evidence](development/contributing.md#choose-regression-evidence-for-the-change)
for the invariant you change.

## Package and reader scope

This internal crate performs no I/O. Callers supply time, identities, limits,
and evidence. Domain depends only on [Foundation](../../undercurrent-foundation/docs/README.md)
among workspace crates. The package version is `0.1.0-beta`; detection profile
schema version `3` varies independently of the product and wire contracts.
Start symbol lookup at the [crate exports](../src/lib.rs) and see the
[domain contract tests](../tests/domain_contract.rs) for regression evidence.
The [SDKs](../../../docs/sdk/README.md) and [public protocol](../../../docs/reference/protocol/README.md)
are the client integration paths.

## Internals

- [Find the owner of a domain behavior](internals/model.md).
- [Follow a session from creation through recovery](internals/session-lifecycle.md).
- [Preserve effect certainty during retries and recovery](internals/attempts-and-recovery.md).
- [Explain foreground and health without inventing completion](internals/observations-and-profiles.md).
- [Keep declared work separate from prepared session queries](internals/work-and-queries.md).

## Reference

- [Validate values at I/O boundaries](reference/values-and-io.md).

## Development

- [Change and verify a domain invariant](development/contributing.md).

[System architecture](../../../docs/architecture/README.md) · [Workspace code map](../../../docs/architecture/dependencies.md) · [Product documentation](../../../docs/README.md)
