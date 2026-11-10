# Undercurrent SDK 0.1.0-beta public API

This reference defines the local IPC public SDK boundary for Rust integrators.
Start with [connection setup](../../docs/sdk/rust/connect-and-query.md) for a runnable example.
The owning declarations are [lib.rs](src/lib.rs), [boundary.rs](src/boundary.rs),
[product.rs](src/product.rs), and the [architecture policy](../../tests/architecture/allowed-graph.json).
That policy names `aixand-undercurrent` and `aixand-undercurrent-protocol` as the supported
external Rust surfaces. A `pub` item in a daemon, wire, or multiplex crate does
not make it a supported consumer dependency.

## Version and platform scope

| Version axis | Current value | Owning source |
| --- | --- | --- |
| SDK, contract, schema and connection | `0.1.0-beta` exactly | Workspace version, product contract and SDK boundary constants |
| Minimum Rust/toolchain | Rust `1.95`, pinned `1.95.0` | Workspace manifest and toolchain |

`MutationContext::current(operation)` supplies current mutation metadata.
`MutationContext::default()` does not. Only the exact current release is accepted;
there are no earlier SDK connection modes or product contract readers.

Linux and macOS use Unix-domain sockets; Windows uses a local named pipe derived
from the configured absolute endpoint path and owner identity. Other targets
fail at compilation. No crate feature selects a network transport. There are no
SDK feature flags. [Local adapter requirements](../../docs/sdk/rust/connect-and-query.md#prerequisites)
apply before product negotiation; this is not private session-host IPC or an
agent bridge protocol.

## Supported layers

| Concern | Consumer boundary and responsibility |
| --- | --- |
| Typed product use | `ProductClient`, `PendingProductRequest`, `ProductSubscription`, protocol-owned request/result DTOs |
| Persistent encoded use | `ResilientClient`, `ResilientSubscription`; caller owns payload encoding and semantic validation |
| One physical connection | `Client`, `Subscription`; no automatic replacement |
| Local connection setup | `StandaloneClientProfile`, `LocalClientConfig`, `LocalClientConnector` |
| Custom/test adapter | `ClientConnector`, `NegotiatedClientTransport`, `ClientTransport` and SDK-owned inbound/outbound values |
| Manual recovery | `SubscriptionCheckpoint`, `recover_client_connection` and its telemetry variant |
| Operational visibility | Typed errors, `pressure()`, bounded telemetry and trace-context APIs |

Opaque values expose primitives such as `as_str()` and `value()`. They provide
no public conversion to private wire or multiplex types. Connection offers and
agreements, trace context, stream IDs, epochs, sequences, and delivery/error
values belong to the SDK. Product request, success, error, and stream-specification
DTOs belong to `aixand-undercurrent-protocol`. Keep symbol lookup in generated Rustdoc;
the [maintainer guide](docs/README.md#verification) gives its build path.

## Connection and adapter contract

Construct `LocalClientConfig::new(endpoint, hello, limits)` and then
`LocalClientConnector::new(config)`. Neither constructor connects or launches a
daemon. `ProductClient::connect` blocks while establishing the connection.
`StandaloneClientProfile` supplies compatible defaults; actual negotiated limits
and capabilities are available through `agreement()`. Use
`StandaloneClientProfile::with_request_limit(client_id, request_bytes)` to size
complete finite JSON requests. Its default is exposed as `DEFAULT_REQUEST_BYTES`;
this does not configure the daemon's terminal-input or aggregate resource limits.

For custom adapters:

- `ClientConnector::connect` performs one bounded, cancellation-aware dial and
  handshake and returns an agreement with its physical transport. Implement
  timeouts in the adapter; the retry loop cannot interrupt an uncooperative
  blocking connector.
- Exactly one SDK reader calls `receive`; exactly one SDK writer serializes
  `send`. `ClientTransport` must be `Send + Sync`, and `close` must unblock both.
- `ClientTransportOutbound::encode(maximum_frame_bytes)` encodes a complete
  frame. `ClientTransportInbound::decode` requires exactly one complete frame,
  rejecting incomplete input and trailing bytes. Frame assembly belongs to the
  byte adapter. `ClientConnectionAgreement::decode_accepted` likewise accepts
  exactly one accepted bounded handshake response, not a rejected response.
- `command()` supplies SDK-owned semantic values for test ports. Those values
  include payload bytes; their `Debug` output is **not** a safe diagnostic sink.
  See [redaction limits](../../docs/sdk/rust/limits-and-diagnostics.md#privacy-and-trace-context).
- The adapter owns authentication and handshake integrity. Decoding an accepted
  agreement is not itself peer authentication or proof that an offer was honored.

The generic `Client` requires multiplexing, flow control, and trace context.
The typed `ProductClient` additionally requires binary payloads and stream resume.
The authoritative capability lists are `PRODUCT_CLIENT_REQUIRED_CAPABILITIES`
and `PRODUCT_CLIENT_OPTIONAL_CAPABILITIES` in [product.rs](src/product.rs).
Optional product capabilities enable error disposition, error effect certainty,
opaque output capture, and session revision preconditions. Opaque start and
guarded input/stop preflight their capability before encoding/sending; missing
support fails locally. Do not silently remove a guard to make a request succeed.

`ProductClient::from_resilient` validates the five required capabilities and
closes the shared client on failure. On success it reuses the supplied runtime's
codecs; it does not rebuild or reconfigure the existing physical client. The
caller must supply codecs/limits consistent with that client's construction.

## Calls, lifetimes, and effects

Rust connection, request, stream-open, and receive methods are blocking. Cloning
a client shares the physical connection and supervisor, rather than opening a
new connection. In async Rust code, use a bounded blocking worker facility; do
not block an executor thread with these methods. Python uses a separate async
stdio bridge and has different cancellation and buffering behavior.

`request()` validates the correlated product operation. Stream opening validates
the initial activation kind; later JSON items must match that kind. Binary
terminal items have their own codec. Malformed typed stream data cancels that
logical stream. [Pressure exceptions and resume limitations](../../docs/sdk/rust/streams-and-recovery.md)
qualify isolation and recovery.

Requests may finish out of order. Successful local queue admission is not remote
acceptance, a durable commit, or completion. Finite mutations are not retried by
the SDK. A timeout, close, or lost response may leave their effects unknown.
[Operation reconciliation](../../docs/sdk/rust/operations.md#reconcile-a-mutation) is the caller's
responsibility.

`PendingProductRequest::cancel()` queues cancellation on the original connection;
`wait()` is still needed to observe its correlated normal or cancellation result.
Dropping a pending handle sends best-effort cancellation. Cancelling/dropping a
subscription removes that interest. `ProductClient::close()` stops recovery and
closes shared resources for **all** clones and subscriptions; it does not stop
managed sessions or shut down the daemon. See [deadline and lifetime details](../../docs/sdk/rust/streams-and-recovery.md#deadlines-and-ownership).

Error categories are presentation aids. Preserve typed server fields and optional
effect/disposition evidence; never infer mutation safety from a broad category.
[Errors and diagnostics](../../docs/sdk/rust/limits-and-diagnostics.md#error-boundaries) explains
the separate configuration, transport, recovery, codec, and product failures.

[SDK entry point](README.md)
