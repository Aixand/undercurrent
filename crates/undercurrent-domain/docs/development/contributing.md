# Change and verify a domain invariant

Use this how-to after finding the relevant behavior in the [code map](../internals/model.md).
Work from the repository root with the installed toolchain selected by
[rust-toolchain.toml](../../../../rust-toolchain.toml), currently Rust 1.95.0,
and available locked dependencies. The [workspace manifest](../../../../Cargo.toml)
sets edition 2024; the [crate manifest](../../Cargo.toml) has no feature switches
and depends only on foundation. Pure domain checks need no daemon, PTY, database,
network service, or platform-specific process setup. OS qualification belongs
to runtime/daemon tests, not the model example.

## Run the model without a daemon

Run the maintained [model walkthrough](../../examples/model_walkthrough.rs) before
changing a domain invariant. It uses explicit synthetic identities, times, and
evidence, and never executes the launch specification's command.

1. From the repository root, run:

   ```console
   cargo run --locked -p undercurrent-domain --example model_walkthrough
   ```

2. Check for a successful exit and these three stdout lines (expected output):

   ```text
   session=example-session lifecycle=running revision=4
   attempt_reconciliation_required=false
   output_verified=true truncated=true
   ```

3. Follow the example's calls: create a session, record a synthetic process,
   enter Running, commit an available-input observation, round-trip typed state,
   reconcile a synthetic unknown input effect, and verify truncated output.
   Then use [the change workflow](#put-the-change-at-the-right-boundary) below.

The assertions verify round-trip equality, revision separation, non-actionable
availability, and the transition from unknown to known effect. A successful
exit plus those three lines verifies the example. It does not prove a process
was launched, a write was delivered, or any adapter transaction committed.
There is no runtime state to clean up; Cargo leaves its normal build artifacts.
The linked example is the maintained source for this procedure.

### If the walkthrough fails

If Cargo cannot find the package, run from the root containing `Cargo.toml`.
Toolchain-selection or dependency-resolution failures are setup failures; use
the repository's supported toolchain and dependency preparation path before
interpreting them as domain regressions. A domain error or assertion failure
after editing the example points to the constructor/transition named in its
source; compare with [lifecycle invariants](../internals/session-lifecycle.md) and
[attempt guards](../internals/attempts-and-recovery.md#settlement-and-compensation-guards).
Do not start a daemon to make this example pass.

## Put the change at the right boundary

1. Locate the defining source and its current callers. Keep pure validation,
   deterministic selection, and aggregate invariants in domain. Put process
   inspection, secret resolution, regex compilation, persistence, scheduling,
   and publication in their adapters/application ports.
2. State the invariant and why it is needed. Identify whether it applies to
   new input, reconstructed persisted state, or both. Check whether constructors
   actually enforce it; a field named “canonical” is not a canonicalizer.
3. Preserve mutation ordering. Validate fallible inputs and obtain the next
   revision before changing state. Check stale timestamps, exact no-op behavior,
   revision exhaustion, and reconstruction of the resulting snapshot. Ensure
   application callers commit the matching outbox change under their existing
   ordering and optimistic-revision rules.
4. Inspect contract impact. New fields or variants can require updates to the
   SQLite codec, protocol DTO/registry, daemon translation, and supported SDKs.
   Public Rust visibility here does not authorize exposing a new consumer API.
   Keep detection schema, storage compatibility, product contract, and wire
   versions separate.
5. Add meaningful regression coverage at the owning level, update the relevant
   local explanation and diagrams, then run the focused checks below. Significant
   new design decisions also need a maintainer decision record in shared docs;
   preserve the existing ownership rules when assigning that documentation.

| Change | Sources and consequences to inspect |
| --- | --- |
| Lifecycle, ownership, or restart | [Session and rehydration](../../src/session.rs), [process activation](../../../undercurrent-application/src/commands/activation.rs), [exit](../../../undercurrent-application/src/commands/process_exit.rs), [startup reconciliation](../../../undercurrent-application/src/commands/startup_reconciliation.rs), and [SQLite aggregate codec](../../../undercurrent-store-sqlite/src/sqlite/codec/aggregate.rs). |
| Event kind or error | [Domain event registry](../../src/event.rs), [domain error categories](../../src/error.rs), [protocol registry](../../../undercurrent-protocol/src/registry.rs), and daemon translation. Review observation-plan invalidation and effect certainty rather than only serialized names. |
| Attempts and bindings | [Attempt invariants](../../src/attempt.rs), [agent bounds](../../src/agent.rs), [attempt store port](../../../undercurrent-application/src/attempts.rs), [durable runtime](../../../undercurrent-application/src/durable_process.rs), [protocol contract](../../../undercurrent-protocol/src/contract.rs), and storage compatibility. |
| Foreground or profile | [Decision policy](../../src/foreground_decision.rs), [profile schema](../../src/profile.rs), [application matching](../../../undercurrent-application/src/profiles.rs), [runtime evidence](../../../undercurrent-runtime/tests/foreground_evidence.rs), and prepared-profile epoch invalidation. Never turn weak availability into a requested interaction. |
| Input or output | [Terminal values](../../src/terminal.rs), [terminal commands](../../../undercurrent-application/src/commands/terminal.rs), [output store](../../../undercurrent-application/src/terminal_output.rs), and [runtime screen tests](../../../undercurrent-runtime/tests/terminal_screen.rs). Recheck byte estimates, process identity, concurrent resize, partial delivery, and retention gaps. |
| Work or query | [WorkContext](../../src/work_context.rs), [filter predicate](../../src/query.rs), [prepared read model](../../../undercurrent-application/src/read_model.rs), and [query services](../../../undercurrent-application/src/queries.rs). Preserve owner-scoped identity and deterministic summary membership. |
| Private launch values | [Execution](../../src/execution.rs), [secret classification](../../src/secret.rs), persistence admission, and diagnostic canary tests. Redacted `Debug` does not authorize exposing getters or storing raw credentials. |

## Choose regression evidence for the change

| Suite | Important coverage |
| --- | --- |
| [Domain](../../tests/domain_contract.rs) | Bounds and privacy, owner-scoped work, lifecycle/ownership, stale timestamps, revision exhaustion, restart backoff/disposition, full state round trips, corrupt reconstruction, terminal input/resize, query membership, event registries, and group restoration. |
| [Evidence](../../tests/observation_contract.rs) | Independent axes; priority/confidence ties; stale, future, unavailable and weak evidence; source-axis uniqueness; rationale reconstruction; partial/all-pass/failing health; evidence-only session events. |
| [Profiles](../../tests/profile_contract.rs) | Non-backtracking wildcards, literal/Windows matching, environment-name requirements, signature boundaries, semantic shape, regions/background rules, final-boundary limits, input pacing, and sample-text privacy. |
| [Attempts and output](../../tests/refactoring_contract.rs) | Wrong-fence rejection, exact settlement duplicate, unknown-effect reconciliation, output tampering and truncation. |
| [Dispatch](../../tests/dispatch_binding.rs) | Canonical callback grammar, immutable request/policy binding, positive limits, expiry boundary, schema length, Secret output rejection, and reference redaction. |
| [Privacy](../../tests/environment_privacy.rs) | Raw credential rejection, ordinary/secret-name disjointness, bounds, and redacted diagnostics. |
| Inline module tests | Cooperative registries, executable pins/capture defaults, semantic IDs/actionability, and per-axis evidence selection. |

From the repository root, the focused crate checks are:

```console
cargo test --locked -p undercurrent-domain
cargo test --locked -p undercurrent-domain --doc
cargo check --locked -p undercurrent-domain --examples
RUSTDOCFLAGS="-D warnings" cargo doc --locked -p undercurrent-domain --no-deps
```

Expected results are successful exits, passing tests, a compilable example, and
warning-free documentation. Browse the generated crate index under
`target/doc/undercurrent_domain/index.html` with the default Cargo target
directory. Symbol inventories belong there; these Markdown pages explain how
the contracts fit together.

The project-level runner is
[scripts/validate.py](../../../../scripts/validate.py). Its
`--stage rustdoc` runs warning-denied workspace Rustdoc; its `tests` stage also
includes workspace doctests. For a broader authorized integration change, use
those stages and [architecture checks](../../../../tests/architecture/README.md)
alongside affected caller suites. The runner sets Cargo offline mode, so its
dependencies must already be cached. These commands describe verification to
perform for a change; they are not a qualification record for every platform.

Domain tests cannot establish transaction atomicity, OS process identity,
cross-process recovery, SDK behavior, or stream retention. Use
[application command tests](../../../undercurrent-application/src/commands/command_tests.rs),
[durable process tests](../../../undercurrent-application/tests/durable_process_attempts.rs),
[output tests](../../../undercurrent-application/tests/terminal_output.rs), and
[hosted recovery tests](../../../undercurrent-daemon/tests/hosted_process_recovery.rs)
when those boundaries change. Platform requirements and runtime isolation for
those suites belong to their owning crates.

For Markdown changes, validate relative paths/anchors and navigation, inspect
the diff for unrelated changes, and render every changed Mermaid diagram.
Visually inspect labels, connections, clipping, and readability in supported
themes; successful parsing alone is insufficient. See the
[documentation workflow](../../../../docs/development/documentation.md) and
[verification guide](../../../../docs/development/verification.md) for the review checks.

[Crate documentation](../README.md)
