# Investigate and verify an application-layer change

Use this guide to place a change, reproduce it with focused tests, and select
its integration checks. It applies to internal `undercurrent-application` 0.1.0-beta.
Run commands from the repository root with the Rust 1.95.0 toolchain pinned by
[rust-toolchain.toml](../../../../rust-toolchain.toml) and locked dependencies
available. The crate has no feature flags and depends only on domain. Its focused
fake-port tests need no daemon, PTY, or managed process.

1. Find the owner in the [application source map](../internals/ownership.md#start-a-source-investigation-by-task)
   and the [change table below](#place-a-change-and-verify-its-boundaries).
2. Run the closest existing test before editing. The commands below identify
   focused entry points; mechanism pages link their remaining suites.
3. Implement the change at its owning boundary. Preserve typed failures,
   retained identities, and the evidence needed after a lost response.
4. Rerun the focused cases, including failures at the changed boundary. Update
   the affected explanation and diagram, then select wider checks according to
   the change's scope.

## Run a focused verification loop

Choose tests by the contract being changed. Run these commands from the
repository root.

| Change | Focused command and source evidence |
| --- | --- |
| Lifecycle and command ordering | `cargo test --locked -p undercurrent-application --lib commands::command_tests`; [command fixtures](../../src/commands/command_tests.rs) cover admission, compensation, stop, restart, and recovery. |
| Layer boundaries and API shapes | `cargo test --locked -p undercurrent-application --test refactoring_contract`; [contract tests](../../tests/refactoring_contract.rs). |
| Typed errors, telemetry names, and safe formatting | `cargo test --locked -p undercurrent-application --test ports_and_observability --test port_operation_registry`; [observability tests](../../tests/ports_and_observability.rs) and [registry coverage](../../tests/port_operation_registry.rs). |
| Bundle permissions | `cargo test --locked -p undercurrent-application --lib diagnostic_bundle`; [authorization tests](../../src/diagnostic_bundle.rs). |
| Read-only output-gap example | `cargo run --locked -p undercurrent-application --example output_gap`; [example and expected result](../internals/sessions/terminal-io.md#run-an-in-memory-retention-example). |

Expected result: the selected assertions pass or the example prints the documented
gap and retained suffix. These commands create normal Cargo artifacts but no
daemon or persistent session state; there is no process cleanup. A missing
toolchain, dependency cache, or compiler prerequisite is a setup failure, not
evidence against the contract. Use the linked topic pages for attempt, profile,
observation, event, stream, and storage-specific tests rather than running
unrelated tests to obscure a focused failure.

For a cross-cutting application change, run the package tests and doctests:

```sh
cargo test --locked -p undercurrent-application
cargo test --locked -p undercurrent-application --doc
```

The integrated workflow is [scripts/validate.py](../../../../scripts/validate.py).
It derives the pinned toolchain, sets offline Cargo mode, and records stage logs
and a JSON report. Its Rustdoc stage is:

```sh
python3 scripts/validate.py --stage rustdoc
```

Expected result: warning-denied workspace API documentation builds successfully.
This is an integrating check, not a package-only build. Its tests stage also
builds workspace binaries and runs workspace tests/doctests, while later stages
perform runtime qualification. Use an isolated runtime environment for those
stages as required by their owning scripts; the fake-port commands above do not
substitute for platform qualification. After documentation edits, additionally
check relative paths/anchors and render every changed Mermaid diagram, inspecting
labels, boundaries, clipping, and readability. Parsing or checking `accTitle`
alone does not establish a visually usable diagram.

## Diagnose by stage without logging private content

Follow [typed diagnostics and failure investigation](../internals/diagnostics.md#diagnose-by-stage-without-logging-private-content)
to distinguish rejection, a committed effect, and an unresolved result without
logging private payloads.

## Authorize diagnostic content explicitly

Use the [diagnostic-content authorization contract](../internals/diagnostics.md#authorize-diagnostic-content-explicitly)
when adding a bundle section. It owns safe defaults, permission ordering,
request-scoped proof, and collection boundaries.

## Place a change and verify its boundaries

| Intended change | Owner and invariants to preserve | Evidence to extend |
| --- | --- | --- |
| New lifecycle or metadata rule | Domain decides valid state; application coordinates lease, optimistic revision, aggregate/outbox commit, and post-commit projection. | [Lifecycle tests and failure stages](../internals/sessions/lifecycle.md#investigate-or-extend-a-command), plus store atomicity for new persisted fields. |
| New process effect | Add admission semantics, canonical payload identity, fencing, retained runtime receipt, settlement, and operation-specific reconciliation. Raw port availability is insufficient. | [Attempt tests](../internals/execution/attempts.md#verification-and-extension-points) and concrete runtime response-loss cases. |
| New adapter operation/error/action | Update owning enums, stable mappings/ALL registries, presentation mapping, and explicit authorization at the actual entry point. | Registry tests and authorization-before-IO tests, including denial that must not reveal existence. |
| New prepared field or watch filter | Maintain indexes and projection revisions; route mutations through the subscription hub; account for page extraction cost and cursor invalidation. | [Read model and hub tests](../internals/sessions/read-models.md). |
| New observation or profile rule | Preserve lease release around expensive IO, exact revision/process/profile revalidation, bounded scheduling, and incomplete-evidence semantics. | [Observation](../internals/observation/observations.md) and [profile](../internals/observation/profiles.md) cases. |
| New event or output path | Respect outbox prefix acknowledgements or byte-offset retention; keep publication, durable commit, and process output separate. | [Outbox tests](../internals/recovery/events-and-outbox.md), [terminal tests](../internals/sessions/terminal-io.md), and slow-consumer/gap cases. |
| New agent field or diagnostic content | Protocol validates payloads, store enforces transactions, boundary enforces authority, and telemetry remains content-free. | [Agent transaction tests](../internals/execution/agent-channel.md) or explicit diagnostic-content authorization tests. |

Before finishing, state the durable point, externally visible effect, retained
identity, ambiguous outcome, and recovery path in the affected explanation and
diagram. Test failures at the boundaries, not only the happy path. If an
implementation disagrees with a port's intended guarantee, record both and its
reader impact as the [current architecture limitations](../internals/ownership.md#port-contracts-versus-the-current-composition)
do. A documentation edit must not silently promote that deviation into a new
contract. Significant design changes need a decision record in the shared
maintainer documentation, with context, consequences, and status.

[Crate documentation](../README.md)
