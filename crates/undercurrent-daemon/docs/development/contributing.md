# Navigate, change, and verify the daemon

Use this guide to locate the owner of a daemon change and select the evidence
needed to review it. Work from the repository root with the
[pinned Rust toolchain](../../../../rust-toolchain.toml), Python 3.11 or later for
validation scripts, and the native compiler/linker requirements of the runtime
and bundled SQLite dependencies. The [workspace manifest](../../../../Cargo.toml)
defines the build root; do not extract this crate into a separate workspace.

Read [composition and ownership](../internals/composition.md) before extending the daemon.
It is the `external_daemon_composition` layer under the
[architectural policy](../../../../tests/architecture/allowed-graph.json). Keep
application services transport-independent and put consumer code on the supported
SDK/protocol surfaces. Rust `pub` alone does not promise external support.

## Locate the behavior, then follow its owner

This is a responsibility map, not an inventory of public symbols. Generated
Rustdoc supplies symbol-level lookup. The linked explanations connect these
modules to their inward-facing contracts and tests.

### Composition and resource owners

| Responsibility | Implementation entry points | Read before changing it |
| --- | --- | --- |
| Package and executable boundaries | [lib](../../src/lib.rs), [main](../../src/main.rs), [private helper entry point](../../src/session_host_main.rs) | [Architecture](../internals/composition.md), [operation](../../../../docs/guides/daemon/operations.md) |
| Strong owners, epochs, staged errors, teardown | [composition](../../src/composition.rs), [identity](../../src/composition/identity.rs), [stage errors](../../src/composition/error.rs), [lifecycle](../../src/composition/lifecycle.rs) | [Lifecycle](../internals/lifecycle/startup-and-shutdown.md) |
| Pinned installation identity and private paths | [instance scope](../../src/process_instance.rs), [Windows scope](../../src/process_instance_windows.rs) | [Configuration](../internals/configuration/resource-bounds.md), [coupled persistence](../internals/lifecycle/startup-and-shutdown.md#identity-and-coupled-persistence) |
| Shared settings projected into concrete bounds | [config](../../src/config.rs), [bootstrap configuration](../../src/bootstrap_config.rs), [bootstrap paths](../../src/bootstrap_paths.rs), [client](../../src/config_client.rs), [services](../../src/config_services.rs), [transport](../../src/config_transport.rs) | [Configuration relationships](../internals/configuration/resource-bounds.md) |

### Requests and observations

| Responsibility | Implementation entry points | Read before changing it |
| --- | --- | --- |
| Finite admission and boundary conversion | [registry](../../src/registry.rs), [dispatch](../../src/dispatch.rs), [boundary](../../src/boundary.rs), [authorization](../../src/authorization.rs) | [Request flow](../internals/requests/request-flow.md) |
| Application port wiring and finite commands | [adapter owner](../../src/application_adapter/mod.rs), [finite operations](../../src/application_adapter/finite.rs), [reads](../../src/application_adapter/reads.rs), [daemon operations adapter](../../src/application_adapter/operations.rs) | [Request flow](../internals/requests/request-flow.md), [effect evidence](../internals/requests/effects-and-recovery.md) |
| Safe errors and complete-command evidence | [product errors](../../src/error.rs), [dispatch evidence](../../src/dispatch_effect_evidence.rs), [adapter errors](../../src/application_adapter/error.rs), [adapter evidence](../../src/application_adapter/error_effect_evidence.rs) | [Effects and recovery](../internals/requests/effects-and-recovery.md) |
| Accepted execution and bounded live attempt tracking | [attempt tracking](../../src/attempt_tracking.rs), [execution output store](../../src/execution_output_store.rs) | [Effects and recovery](../internals/requests/effects-and-recovery.md), [output](../internals/output-and-streams.md) |
| Background lifecycle, wakes, and cooperative cancellation | [background runtime](../../src/background.rs), [worker owner](../../src/workers.rs), [blocking future bridge](../../src/blocking.rs), [request cancellation](../../src/cancellation.rs) | [Background work](../internals/lifecycle/background-work.md) |
| Subscription reservation, pump scheduling, checkpoints | [stream contracts](../../src/stream.rs), [pump](../../src/stream/pump.rs), [stream adapters](../../src/application_adapter/streams.rs), [cursor store](../../src/cursor.rs), [screen checkpoints](../../src/screen_checkpoint.rs) | [Output and streams](../internals/output-and-streams.md) |
| Application-to-product projection | [presentation](../../src/presentation.rs), [detail](../../src/presentation/detail.rs), [feed](../../src/presentation/feed.rs), [profile](../../src/presentation/profile.rs), [event kinds](../../src/event_kind.rs) | [Request flow](../internals/requests/request-flow.md), [output](../internals/output-and-streams.md), [profiles](../../../../docs/guides/configuration/profiles.md) |

### Configuration and diagnostics

| Responsibility | Implementation entry points | Read before changing it |
| --- | --- | --- |
| Profile file parsing, seeding, and live activation | [file catalog](../../src/profile_files.rs), [YAML boundary](../../src/profile_yaml.rs), [built-ins](../../src/profile_builtins.rs), [profile adapter](../../src/application_adapter/profiles.rs) | [Profile tasks](../../../../docs/guides/configuration/profiles.md), [reload ordering](../internals/configuration/profile-reload.md) |
| Status, retained diagnostic pages, product shutdown | [operations](../../src/operations.rs), [runtime status](../../src/runtime_status.rs) | [Lifecycle](../internals/lifecycle/startup-and-shutdown.md), [diagnostics](../../../../docs/guides/daemon/diagnostics.md) |
| Bounded runtime diagnostic projections | [diagnostic owner](../../src/runtime_diagnostics.rs), [typed rows](../../src/runtime_diagnostics/rows.rs), [services](../../src/runtime_diagnostics/services.rs), [streams](../../src/runtime_diagnostics/streams.rs) | [Diagnostic collection](../internals/diagnostics.md) |
| Logging and typed telemetry | [logging bridge](../../src/logging.rs), [rotating writer](../../src/structured_logging.rs), [dispatch telemetry](../../src/dispatch_telemetry.rs), [multiplex telemetry](../../src/multiplex_telemetry.rs) | [Telemetry emission](../internals/diagnostics.md#emit-bounded-telemetry) |
| Protected attached-agent ingress and durable-reply wakes | [gateway](../../src/agent_bridge_gateway.rs), [notification hub](../../src/agent_event_notifications.rs) | [Attached agent calls](../internals/requests/agent-channel.md) |

## Trace representative changes

Follow the owning walkthrough and test its failure boundaries:

| Change | Start with | Review focus |
| --- | --- | --- |
| Session creation | [Daemon request boundary](../internals/requests/request-flow.md#trace-session-creation), then [cross-crate activation](../../../../docs/architecture/flows.md#start-a-session) | Separate initial insert, runtime effect, settlement, acknowledgement, and response. |
| Terminal output | [Stream preparation and pump](../internals/output-and-streams.md#follow-terminal-bytes-across-the-process-boundary) | Preserve byte offsets, event sequences, screen checkpoints, process incarnation, and pending reliable items. |
| Recovery | [Startup gates](../internals/lifecycle/startup-and-shutdown.md#two-kinds-of-readiness), then [effect repair](../internals/requests/effects-and-recovery.md) | Commit evidence before acknowledgement; listener reachability and request cancellation do not prove effect absence. |

## Extend contracts without hiding failure windows

1. Identify the semantic owner. Add domain values and application ports/use cases
   inward of this crate; implement concrete IO in its adapter owner. For a new
   product capability, update the protocol registry, validation, request/response
   projection, and both supported SDK surfaces as applicable. Do not hand-edit a
   second operation inventory in prose.
2. Specify authorization, bounds, ordering, cancellation, effect evidence, and
   recovery before adding dispatch. Check readiness and shutdown admission
   separately. A repair/status route may need access while ordinary mutations
   remain gated; granting it broad mutation bypass would change the contract.
3. At the boundary, reject invalid typed combinations without echoing raw
   command/env/terminal data. Classify KnownAbsent only when the owner proves the
   **whole command** had no externally visible effect. A convenient low-level
   port category or `retryable` flag is not such proof.
4. Give queues, cursors, checkpoints, telemetry, and background cycles explicit
   capacities. Declare what happens on exhaustion: reject, evict, coalesce, retain
   a pending reliable item, or report a gap. Preserve reservation cleanup on every
   failed stream-open path and release per-client resources on close.
5. Add failure-window and ownership checks beside the existing tests. Update the
   authoritative contract and affected explanation/diagram; generated API docs
   should remain usable with warnings denied.

Configuration changes start in shared configuration, then daemon projections and
cross-component validation. Keep IO after validation. Profile changes must preserve
stable-snapshot checking, live-reference validation, and wake/catalog ordering.
Background additions need a bounded cycle, race-safe wake, interruptible stop,
and explicit strong owner. Read the corresponding guide before changing those
mechanisms: they are not interchangeable general-purpose thread pools.

## Run the relevant checks

Run from the repository root. The project validation runner
pins the repository toolchain, enables offline Cargo, records stage logs, and
stops after a failing stage. Dependencies must already be present in the Cargo
cache for its offline stages. Its report covers only executed stages.

```sh
python3 scripts/validate.py --list
python3 scripts/validate.py --stage architecture
python3 scripts/validate.py --stage rustdoc
```

The last stage is the required warning-denied Rustdoc check; it runs
`RUSTDOCFLAGS=-Dwarnings cargo doc --locked --workspace --all-features --no-deps`.
Generated daemon API docs are under `target/doc/undercurrent_daemon/index.html`
unless `CARGO_TARGET_DIR` changes the root. These generated files are outputs,
not authored Markdown. The runner's `tests` stage also includes workspace
doctests. See the [runner](../../../../scripts/validate.py) for complete
release, configuration, recovery, and platform-oriented stage scope.

For a focused daemon change, select the test targets that exercise its invariants:

```sh
cargo test --locked -p undercurrent-daemon --lib
cargo test --locked -p undercurrent-daemon --test product_boundary --test product_dispatch
cargo test --locked -p undercurrent-daemon --test daemon_operations
cargo test --locked -p undercurrent-daemon --doc
```

An exit code of zero establishes only those selected tests. A missing dependency,
helper, platform facility, or compiler is an unverified check until resolved, not
a test pass. Unit tests for structured logging isolate global subscribers where
needed; do not remove that isolation to make a test order happen to work.

| Changed behavior | Focused evidence |
| --- | --- |
| Layer ownership and configuration parity | [architecture_contract](../../tests/architecture_contract.rs), [config_validation_parity](../../tests/config_validation_parity.rs), in-module config tests |
| Boundary validation, error privacy, request concurrency or effect evidence | [product_boundary](../../tests/product_boundary.rs), [product_dispatch](../../tests/product_dispatch.rs), [effect_evidence](../../tests/product_dispatch/effect_evidence.rs), [application_executor](../../tests/application_executor.rs) |
| Projection, offset/cursor expiry, screen resume | [product_presentation](../../tests/product_presentation.rs), [cursor_store](../../tests/cursor_store.rs), [screen_checkpoint](../../tests/screen_checkpoint.rs), stream-pump unit tests |
| Wake races, isolated failures, complete startup recovery | [background_workers](../../tests/background_workers.rs), [application_runtime](../../tests/application_runtime.rs), [startup_recovery](../../tests/startup_recovery.rs) |
| Mutation drain, durable host shutdown, retained diagnostics | [daemon_operations](../../tests/daemon_operations.rs) and in-module runtime diagnostic tests |
| Host reconnect, output/screen recovery, unavailable-daemon exits | [hosted_process_recovery](../../tests/hosted_process_recovery.rs); this launches controlled helper/process fixtures and needs a matching `uc-session-host` artifact |
| Profile reload, logging, process locks, agent wakes | Tests beside the owning modules listed above; platform-specific paths require their actual target OS |

Tests that launch processes must use their temporary isolated runtime state and
matching helper binaries. Build the required helper from this checkout before a
hosted-process test if the normal test runner has not supplied it:

```sh
cargo build --locked -p undercurrent-daemon --bins
cargo test --locked -p undercurrent-daemon --test hosted_process_recovery
```

Do not aim fixtures at an operator's live home. A passing Linux test does not
validate macOS peer resolution or Windows named pipes, ACLs, process identity,
and console events. Full qualification belongs to the project's platform gates.

Markdown changes need relative-link/anchor and referenced-path checks. Changed
Mermaid needs parsing **and rendering with visual inspection** of labels,
connections, clipping, and supported themes. A text check for `accTitle` and
`accDescr` alone cannot establish that a diagram renders correctly.

## Examples and deployment fixtures

The [retained-output Python example](../../examples/read_retained_output.py) is a
read-only SDK example with a bounded pagination loop. Its complete setup, command,
expected exit behavior, and partial-file cleanup are in
[the output guide](../internals/output-and-streams.md). It requires a running daemon and an
existing stable session ID; it does not create a session or stop the daemon.

The existing Rust fixtures have a different purpose:

| Fixture | Scope and limits |
| --- | --- |
| [development_daemon](../../examples/development_daemon.rs) | Requires one explicit configuration path and a validated absolute existing directory in `UNDERCURRENT_DEVELOPMENT_CODEX_HOME`. Resolves only the exact development credential handle. Semantic cancellation/status authorization remains denied. |
| [integration_daemon](../../examples/integration_daemon.rs) | Used by the integration gate with a narrow mock authority and fixed tenant/product scope. Permits only the fixture's matching graceful cancellation, never force. Its optional live credential mode is deployment-specific. It is not a production Trust implementation. |

To check that those Rust fixtures still compile without exercising credentials
or starting a daemon:

```sh
cargo check --locked -p undercurrent-daemon --examples
```

Both fixtures compose real daemon infrastructure when executed and intentionally
use a configuration-path entry point different from ordinary `uc-daemon`.
Compilation alone does not validate their deployment integration. Keep secrets
and authority adapters explicitly injected; do not weaken the production
fail-closed defaults to make a fixture work.

[Documentation index](../README.md) · [Crate entry point](../../README.md)
