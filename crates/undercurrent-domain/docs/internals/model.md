# Find the owner of a domain behavior

Use this map before changing a session invariant or adding a new kind of
evidence. It covers the internal Rust model in this checkout. Start with the
[crate README](../../README.md) for consumer entry points; a domain constructor is
not a client request or an authorization check.

## Ownership and effects

Domain is a library of deterministic decisions and owned values. Its only
workspace dependency is [foundation](../../../undercurrent-foundation/README.md),
used for SHA-256 content digests, opaque URI validation, and classification.
It creates no threads, tasks, locks, processes, sockets, timers, or telemetry.
It has no persistence codec and does not read the clock. Ordinary Rust ownership
controls value lifetimes; mutable methods need an exclusive borrow, which does
not coordinate separate clones of an aggregate.

A `DomainChange` describes an in-memory mutation. Application services commit
the aggregate and its outbox event through a repository; runtime ports perform
process effects. Private session-host IPC is separate from the public client
protocol. See the [system process and state view](../../../../docs/architecture/README.md#process-boundaries)
for those owners and the [cross-crate flows](../../../../docs/architecture/flows.md)
for ordering. This page maps the local decisions to their source and guide.

## Code map by question

The table maps behavior to its source of truth and local explanation. Use
generated Rustdoc for the complete symbol inventory.

| Question | Owning modules | Explanation |
| --- | --- | --- |
| What can internal callers import? | [lib.rs](../../src/lib.rs) | Module exports and foundation `Classification` re-export; supported external surfaces remain defined by [architecture policy](../../../../tests/architecture/allowed-graph.json). |
| What makes a session and process identity valid? | [identity.rs](../../src/identity.rs) | [Value boundaries](../reference/values-and-io.md#names-time-and-revisions) |
| Which session changes are legal and revisioned? | [session.rs](../../src/session.rs), [lifecycle.rs](../../src/lifecycle.rs) | [Lifecycle and reconstruction](session-lifecycle.md) |
| When should another process incarnation start? | [restart.rs](../../src/restart.rs) | [Restart decisions](session-lifecycle.md#restart-intent-survives-the-process) |
| What does an accepted operation prove? | [attempt.rs](../../src/attempt.rs) | [Attempts, fencing, and recovery](attempts-and-recovery.md) |
| What binds an agent runtime to its caller? | [agent.rs](../../src/agent.rs) | [Dispatch and agent bindings](attempts-and-recovery.md#dispatch-policy-and-agent-bindings) |
| What can be launched and persisted? | [execution.rs](../../src/execution.rs), [secret.rs](../../src/secret.rs) | [Launch preparation and privacy](../reference/values-and-io.md#launch-values-and-preparation) |
| How are labels and private metadata bounded? | [attributes.rs](../../src/attributes.rs) | [Attribute boundaries](../reference/values-and-io.md#labels-metadata-and-diagnostics) |
| What does a readiness observation mean? | [foreground.rs](../../src/foreground.rs), [decision policy](../../src/foreground_decision.rs) | [Independent axes and evidence selection](observations-and-profiles.md) |
| Which evidence and health results support a summary? | [observation.rs](../../src/observation.rs), [health.rs](../../src/health.rs) | [Health aggregation](observations-and-profiles.md#health-is-an-independent-evaluation) |
| How do profiles describe matching and input policy? | [profile.rs](../../src/profile.rs) | [Profile rules](observations-and-profiles.md#profiles-describe-evidence-and-input-mechanics) |
| Which profile applies to this exact process? | [profile snapshot](../../src/effective_profile.rs) | [Prepared profile epochs](observations-and-profiles.md#base-profile-and-effective-profile-have-different-lifetimes) |
| What do cooperative shell signals claim? | [cooperative.rs](../../src/cooperative.rs) | [Cooperative contract](observations-and-profiles.md#cooperative-shell-evidence) |
| What can a terminal input sequence contain? | [terminal.rs](../../src/terminal.rs) | [Input and resize](../reference/values-and-io.md#terminal-input-and-resize) |
| What does an output digest verify? | [output.rs](../../src/output.rs) | [Output integrity and retention](../reference/values-and-io.md#output-integrity-does-not-prove-complete-history) |
| Who declares work and owns its result? | [work context](../../src/work_context.rs) | [WorkContext](work-and-queries.md) |
| How is summary membership determined? | [query.rs](../../src/query.rs) | [Prepared queries](work-and-queries.md#query-membership-without-detail-hydration) |
| What do monotonic values and timestamps guarantee? | [revision.rs](../../src/revision.rs), [time.rs](../../src/time.rs) | [Time and revisions](../reference/values-and-io.md#names-time-and-revisions) |
| Which failures and events cross the domain boundary? | [error.rs](../../src/error.rs), [event.rs](../../src/event.rs) | [Error handling](../reference/values-and-io.md#typed-rejections-and-outer-failures), [event invalidation](session-lifecycle.md#changes-events-and-projections) |

## Design constraints to preserve

Validated leaf values prevent malformed input from reaching aggregate decisions.
Aggregate methods additionally enforce relationships between valid fields:
`Running` needs a process owner, and persisted health must match configured
checks. Reconstruction is a validation boundary, not a struct-copy shortcut.

The independent lifecycle, foreground, health, and work models prevent an
observation or declaration from rewriting OS supervision truth. Likewise,
`KnownApplied` describes an operation effect, not completion of caller-owned
work. Durable revisions and runtime profile epochs serve different lifetimes.

Bounds are generally explicit constructor arguments. Do not silently replace
caller limits with domain defaults or assume every public value enforces all
upstream bounds. For example, profile matching evidence and agent JSON bytes
arrive from already bounded/canonicalized boundaries; the domain does not
parse or authenticate them. See the focused pages for these trust boundaries.

Source names such as “commit” and “durable” describe the intended persisted
model. Only the repository transaction and adapter evidence establish that a
real effect occurred. [The maintainer workflow](../development/contributing.md) explains how
to verify a change across those boundaries without introducing I/O here.

[Crate documentation](../README.md)
