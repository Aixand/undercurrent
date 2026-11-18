# Contribute to the product protocol

Find the source, owner, and checks for a protocol change. This crate owns DTOs,
registries, bounded codecs, validators, and canonical digests. Its values describe
operations; the daemon, application, store, and runtime own their effects.

| Task | Start here |
| --- | --- |
| Find a message, validator, digest, or regression test | [Contract source map](internals/contract-boundary.md#source-map) |
| Choose registry, validation, and adapter tests | [Focused verification](development/extend-contract.md#verification) |
| Change a contract and check its consumers | [Extension procedure](development/extend-contract.md) |
| Construct or interpret a public payload | [Consumer protocol reference](../../../docs/reference/protocol/README.md) |
| Follow a request through the system | [Shared walkthroughs](../../../docs/architecture/flows.md) |

## Package and reader scope

The [architecture policy](../../../tests/architecture/allowed-graph.json)
supports this package and `aixand-undercurrent` for external Rust consumers.
The crate and exact product contract are `0.1.0-beta`. The schema generation is
`1`. The UCMP transport version is independent. Public agent-bridge DTOs
describe a private channel. The protocol performs no connection, process, or
storage I/O. The SDK carries these values, and the daemon validates them and
translates them into application commands.

For attached-runtime integration, see [attempt status and repair](../../../docs/reference/protocol/recovery.md),
[exact cancellation](../../../docs/reference/protocol/cancellation.md), and
[agent events](../../../docs/reference/protocol/agent-events.md).
Use [generated Rustdoc](../../../docs/reference/protocol/README.md#generate-symbol-level-documentation)
for symbol lookup.

## Runtime ownership

The [system process/state view](../../../docs/architecture/README.md#process-boundaries)
locates communicating processes; the [workspace dependency map](../../../docs/architecture/dependencies.md)
locates Rust dependencies. The protocol owns no process. Public SDK traffic uses
local IPC; the [session-host protocol](../../undercurrent-runtime/docs/reference/host-protocol.md)
and [agent bridge](../../../docs/reference/protocol/agent-events.md) are private boundaries.
For supported platforms, distinguish [SDK adapter availability](../../undercurrent-sdk/src/local.rs)
from [platform qualification](../../../docs/development/platform-testing.md).

## Source map

[Contract ownership and tests](internals/contract-boundary.md#source-map) maps the
crate's modules to their contracts and verification evidence.

## Trace session creation

Start with the [shared creation sequence](../../../docs/architecture/flows.md#start-a-session),
then use [daemon request conversion and settlement](../../undercurrent-daemon/docs/internals/requests/request-flow.md#trace-session-creation)
for admission, validation, and attached-dispatch response loss. The
[application attempt guide](../../undercurrent-application/docs/internals/execution/attempts.md#dispatched-spawn-is-settled-by-the-start-service)
records the standard wrapper's no-op acknowledgement limitation.

For other flows, follow [terminal screen seeding](../../../docs/reference/protocol/streams.md#seed-a-live-terminal-view)
and [evidence-backed repair](../../../docs/reference/protocol/recovery.md#repair-an-unknown-attempt).

## Design constraints and tradeoffs

[Protocol boundary constraints](internals/contract-boundary.md#design-constraints-and-tradeoffs)
explain the separation of values, effects, observations, and replay evidence.

## Extend a contract

Follow the [extension checklist](development/extend-contract.md#extend-a-contract)
from registry/DTO changes through validation, consumer wiring, and owner tests.

## Verification

Use the [focused commands and expected results](development/extend-contract.md#verification).
That guide distinguishes package checks from the workspace Rustdoc gate.

[Crate orientation](../README.md) · [Consumer protocol reference](../../../docs/reference/protocol/README.md)
