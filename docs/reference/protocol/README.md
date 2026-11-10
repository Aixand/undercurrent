# Find product contracts and generated API documentation

Use this map to locate exact fields, operations, error categories, and version
owners. The supported product contract is **0.1.0-beta**, schema **0.1.0-beta**;
public transport uses **connection protocol 0.1.0-beta with UCMP framing 0.1.0-beta**. Package versions and private IPC formats are
separate axes. [Compatibility](../compatibility.md) records each owner.

## Public product contract

Start with the task you need to complete. These references describe the current
Rust product boundary; the [SDK guides](../../sdk/README.md) provide connection
setup and maintained client examples.

| Task | Reference |
| --- | --- |
| Build or decode a payload; check versions and validation | [Encoding and validation](encoding.md) |
| Choose an operation; interpret its successful result | [Finite operations](operations.md) |
| Page output, maintain a watched view, or recover a stream gap | [Streams and retained output](streams.md) |
| Resolve an error or lost mutation response | [Effect evidence and attempt recovery](recovery.md) |
| Stop one attached attempt and reconcile its control | [Exact runtime cancellation](cancellation.md) |
| Poll or reply to an attached agent call | [Agent event exchange](agent-events.md) |
| Change a contract or find its tests | [Protocol maintainer entry point](../../../crates/undercurrent-protocol/docs/README.md) |

The [registry and fixtures](../../../crates/undercurrent-protocol/src/registry.rs)
define operation, stream, event, and error names. Each topic above links its owning
Rust/Serde definitions, which define the payload shapes used by the current
protocol. The [package contract](../../../undercurrent-package.json) records
the supported versions and registries.

A schema-valid request does not prove target existence, authority, or runtime
support. Local OS authentication establishes the peer account; attached controls
also need [deployment-injected semantic authority](../../../docs/architecture/credentials.md).
The standard transport-only authorizer denies controls that require it.

## Transport and private boundaries

The [wire reference](../../../crates/undercurrent-wire/docs/reference/wire-format.md) owns
UCMP data/control framing and the public client's UCHS negotiation. Public local
transport adapters perform OS peer verification before that handshake. The
[local transport guide](../../../crates/undercurrent-transport-local/docs/internals/ownership.md)
owns endpoint and connection security behavior.

The runtime's private host protocol also uses the name UCHS and version byte 1,
but it is a **different format**: its header is 10 bytes, while public wire
handshake framing uses 12 bytes. It has private credentials, messages, receipts,
and process semantics. Do not send SDK traffic to a host endpoint or expose it
as public configuration. Python's private JSON-lines bridge is another adapter
boundary, not a second public semantic transport.

## Generate symbol-level documentation

From the repository root, after preparing the pinned toolchain/dependencies:

```sh
cargo doc --locked -p aixand-undercurrent -p aixand-undercurrent-protocol --no-deps
```

Open `target/doc/undercurrent_sdk/index.html` and
`target/doc/undercurrent_protocol/index.html` (under `CARGO_TARGET_DIR` if set).
These paths are build outputs, not pre-existing published sites. Symbol-level
contracts and signatures belong there. Use the current warning-denied
`python3 scripts/validate.py --stage rustdoc` stage for repository
verification; see [verification prerequisites](../../development/verification.md).

The [public 0.1.0-beta boundary](../../../crates/undercurrent-sdk/PUBLIC_API.md) distinguishes
supported SDK/protocol types from internal public Rust symbols. The retained
[0.1.0-beta pointer](../../../crates/undercurrent-sdk/PUBLIC_API.md) marks retirement;
it is not a compatibility negotiation option.

[SDK setup](../../sdk/README.md) · [Error lookup](../errors.md) · [Documentation index](../../README.md)
