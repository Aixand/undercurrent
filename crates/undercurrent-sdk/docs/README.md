# SDK maintainer documentation

Find the SDK mechanism or verification task you need. These pages describe the
Rust SDK 0.1.0-beta implementation; integrators should start with the
[Rust consumer guides](../../../docs/sdk/rust/README.md) and
[public 0.1.0-beta boundary](reference/public-api.md).

## Client dependencies and evidence

The SDK exposes product data from the [protocol crate](../../undercurrent-protocol/docs/README.md)
and uses [wire](../../undercurrent-wire/docs/README.md) and
[multiplex](../../undercurrent-multiplex/docs/README.md) internally. The concrete
local client connector lives here. The [public reference](reference/public-api.md)
owns platform, connection, adapter, and mutation guarantees. The SDK targets
Linux, macOS, and Windows with workspace Rust 1.95. The local connector
performs mutual OS peer verification. The SDK provides blocking Rust calls,
with no embedded daemon, implicit daemon launch, or Rust async facade. The asynchronous
[Python SDK](../../../python/docs/README.md) has a separate presentation API.

Behavioral evidence includes [client contract tests](../tests/client_contract.rs),
[typed product tests](../tests/product_client.rs), and
[native local transport tests](../tests/local_boundary.rs).
The [Rust consumer guides](../../../docs/sdk/rust/README.md) link to maintained examples.

## Responsibility and dependency boundary

The SDK owns blocking client calls, its local connector, frame adaptation,
product decoding, subscription ownership, recovery, and telemetry. The external
daemon owns durable session state, authorization, process effects, profiles,
output retention, and daemon startup. Changes here also affect the CLI and
Python bridge, whose presentation APIs differ.

[Cargo.toml](../Cargo.toml) owns exact dependencies. The
[workspace dependency map](../../../docs/architecture/dependencies.md#dependency-direction)
explains their direction. Native endpoints and peer checks belong to
[transport-local](../../undercurrent-transport-local/README.md); private host and
agent channels are outside the public client transport.

## Find a mechanism or task

| Question | Read |
| --- | --- |
| Who retains a connection, and how do requests share it? | [Client ownership and request admission](internals/client-runtime.md) |
| How do stream activation, credit, and replacement work? | [Subscription recovery](internals/subscription-recovery.md) |
| Where should a change go, and how do I verify it? | [Extend and verify the boundary](development/extending-client.md) |
| Which source owns a particular mechanism? | [Code map](development/extending-client.md#code-map) |
| Which tests cover the behavior I changed? | [Focused checks and evidence](development/extending-client.md#verification) |

## Code map

Use the [source responsibility table](development/extending-client.md#code-map).

## Runtime ownership and concurrency

See [shared owners, workers, correlation, and teardown](internals/client-runtime.md#runtime-ownership-and-concurrency).

## Session creation

Trace [SDK request admission](internals/client-runtime.md#session-creation), then
the shared [daemon start flow](../../../docs/architecture/flows.md#start-a-session).

## Terminal output

Trace [stream activation](internals/subscription-recovery.md#terminal-output) and
[delivery checkpoints](internals/subscription-recovery.md#delivery-checkpoints-and-credit).

## Recovery ordering

See [checkpoint capture and replacement installation](internals/subscription-recovery.md#recovery-ordering).

## Invariants and tradeoffs to preserve

Use the [invariant table](development/extending-client.md#invariants-and-tradeoffs-to-preserve).

## Place an extension at its owner

Choose the [operation, transport, recovery, or diagnostic workflow](development/extending-client.md#place-an-extension-at-its-owner).

## Verification

Use the [focused commands and evidence map](development/extending-client.md#verification).

[SDK crate](../README.md)
