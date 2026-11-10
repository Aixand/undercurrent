# Exercise, diagnose, and change the codecs

Use this guide for a local wire change. Run commands from the repository root,
the directory containing the [workspace Cargo.toml](../../../../Cargo.toml).
You need Cargo and the pinned **Rust 1.95.0** toolchain from
[rust-toolchain.toml](../../../../rust-toolchain.toml), the checked-in lockfile,
checked-in compatibility fixtures, and available dependency sources. No daemon,
credentials, local endpoint, database, or shell fixture is needed for this
crate's example and tests. The crate declares no optional Cargo features and
has no platform-specific code; that does not qualify the complete product for
every Rust target. The local SDK adapter has its own Linux/macOS/Windows scope.

## Run the example

1. From the repository root, run the maintained
   [inspect_frames.rs](../../examples/inspect_frames.rs) example:

   ```sh
   cargo run --locked -p aixand-undercurrent-wire --example inspect_frames
   ```

   It runs entirely in memory using existing crate dependencies. Its opaque
   binary payloads are codec samples, not product operations to send to a daemon.
   If Cargo reports a missing pinned compiler or unavailable dependencies,
   follow [maintainer setup](../../../../docs/development/README.md) before retrying.
   Keep `--locked` so the exercise cannot silently update the lockfile.

2. Check for a zero exit status and these three application lines, excluding
   Cargo progress messages. This is expected output:

   ```text
   negotiated protocol 0.1.0-beta; maximum frame 4096 bytes
   decoded fragmented request and following ping
   binary payload preserved; credit includes the full frame
   ```

   The assertions check hello/acceptance framing, `NeedMore` on a seven-byte
   prefix, consumed lengths across coalesced request/ping frames, and binary
   payload/credit behavior. A nonzero exit or assertion failure means the
   exercise did not complete; use the failure lookup below.

3. Follow [the decoding walkthrough](../internals/flows.md#decode-one-frame-from-a-caller-buffer)
   to relate those results to the buffer contract, then choose the owning
   layer before making a change. No process or runtime-state cleanup is needed;
   only normal Cargo artifacts remain under the selected target directory.

When adapting the example, leave room for the full frame envelope, not just
payload. Empty request and stream-data payloads are invalid. Check the
[message layouts](../reference/wire-format.md#ucmp-messages) and
[envelope reservation](../reference/wire-format.md#envelope-reservation) if a
custom value fails to encode.

## Diagnose a frame or negotiation failure

Start from the typed error and the connection stage, not a raw payload dump.
Use [the ownership map](../internals/ownership.md#runtime-ownership) to establish whether
the input is client UCHS, UCMP, private host IPC, or an adapter control frame.
The local adapter's `UCLC` capacity-close frame belongs to
[framed.rs](../../../undercurrent-transport-local/src/framed.rs), not either codec.

| Symptom | Check and recovery |
| --- | --- |
| `NeedMore` repeats | Retain the entire prefix; compare buffer length with `minimum_total`. Wait under the transport's deadline. At EOF inside a frame, report truncation instead of retrying the same unchanged slice. |
| Invalid magic, version, kind, or flags | Check endpoint/protocol and read direction against the [header table](../reference/wire-format.md#common-header). Do not skip bytes looking for another magic sequence; the codec provides no resynchronization contract. |
| `FrameTooLarge` | Compare declared full length with the setup or negotiated maximum. Do not allocate the rejected body to diagnose it. Fix legitimate limit/envelope mismatch at its owner. |
| Invalid ID, short field, empty payload, or trailing bytes | Compare field order and presence tags with the kind's layout. Do not treat a complete malformed body as an incomplete transport read. |
| UCHS `InvalidPayload` | Verify hello/agreement fields and constructor invariants. Prefer the redacted outer error in ordinary logs; parser source chains have a different disclosure boundary. |
| `IncompatibleVersions` or typed version rejection | Both offers must be protocol `0.1.0-beta`; package version or product contract naming is not the selected connection version. |
| Missing required capability | Check exact names in the offer and server policy; do not downgrade a required feature to optional unless the client can operate without it. |
| Retryable setup rejection | Apply the SDK's bounded reconnect/backoff policy. The flag says nothing about previously issued mutations. |
| Duplicate request/stream ID or bad credit | Inspect transport-core/router/scheduler state; a successful wire decode validates representation only. |
| Product error, lost response, or stream gap | Follow [the recovery paths](../internals/flows.md#recover-after-disconnection-or-a-gap); identify effect certainty or stream recovery action before issuing more work. |

`WireError` never wraps a socket error. An adapter timeout, EOF, partial write,
or peer-verification failure must be investigated in the adapter even if the
last complete frame was valid. Conversely, a product rejection in a successfully
decoded response is not a wire framing error.

## Choose the owning layer

| Proposed change | Owner and affected contracts |
| --- | --- |
| Header, kind, field order, binary gap code | wire codec/registry plus the product-owned registry and fixtures; both directions and all callers need review. |
| Limits, capability negotiation, or heartbeat value invariants | wire connection values, handshake serialization, then adapter/SDK policy validation. |
| Product operation, payload schema, effect certainty, terminal offset | protocol and daemon/application; preserve opaque UCMP payload handling. |
| Fairness, priority, credit, queue eviction, or replay retention | multiplex and transport-core; wire only represents the resulting control/data/gap message. |
| Local endpoint, authentication, fragmented reads, cancellation-safe writes | transport-local and the SDK adapter. |
| Private session-host protocol | runtime; do not reuse this crate's UCHS decoder. |
| Retry or persistent subscription policy | SDK and operation-specific product contracts. |

For a new message or field:

1. Establish its owning contract and compatibility/version policy. Existing
   decoders reject unknown codes, nonzero flags, and other encoding-version
   bytes; negotiation separately requires the exact current protocol version.
   An additive enum variant is not automatically interoperable with old peers.
2. Update semantic types, code/name/direction registries, both encode and decode
   paths, and any gap mapping or envelope reservation affected. Preserve stable
   codes; do not derive codes from enum or array positions.
3. Check constructors **and** Serde paths for the same intended invariants.
   Distinguish representation checks from offer-relative or stateful checks.
   Do not add protocol/SDK dependencies to wire to validate product semantics.
4. Extend contract tests for both directions, exact boundaries, malformed
   values, fragmentation/coalescing, and the relevant caller behavior. Compare
   against the fixed UCMP/UCHS JSON and exact-byte fixtures in `tests/fixtures`.
   Update those expectations only for an intentional protocol change; ordinary
   refactors must preserve their values.
5. Update the authoritative reference, example if relevant, Rustdoc, and
   affected diagrams. Check links and render the diagrams, then visually
   inspect labels, relationships, clipping, and theme readability.

## Verify a change

Run the focused checks from the repository root after changing the relevant
code, contracts, or example:

```sh
cargo test --locked -p aixand-undercurrent-wire --lib --tests
cargo check --locked -p aixand-undercurrent-wire --examples
cargo test --locked -p aixand-undercurrent-wire --doc
RUSTDOCFLAGS=-Dwarnings cargo doc --locked -p aixand-undercurrent-wire --no-deps
git diff --check -- crates/undercurrent-wire
```

Successful tests report no failures; the example check and warning-denied
Rustdoc must exit zero. Generated API documentation is at
`target/doc/undercurrent_wire/index.html` under the default target directory.
Set `RUSTDOCFLAGS` with the equivalent environment syntax on PowerShell.
The repository's full validation runner owns the warning-denied workspace
Rustdoc stage:

```sh
python3 scripts/validate.py --stage rustdoc
```

That stage is broader than the crate checks and expects the full workspace's
dependencies. The [runner](../../../../scripts/validate.py) uses offline
Cargo and the pinned toolchain.

### What the existing tests prove

**Codec and value tests**

- [UCMP contracts](../../tests/wire_contract.rs): every client/server kind, raw
  binary bytes, all gap reasons, fragmented/coalesced input, early oversize
  rejection, direction/format/flags/ID/payload/gap validation, maximum trace
  envelope, and registry/header parity.
- [UCHS contracts](../../tests/handshake_contract.rs): incremental hello,
  accepted/rejected round trips, retryable capacity rejection, declared/encoded
  bounds, direction/format/flags, validated JSON and source redaction, and
  registry/header parity.
- [Connection values](../../src/connection.rs): exact capability registry,
  intersection/minimum selection, required-capability and future-version
  rejection, and deserialized numeric invariants.
- [Scalar tests](../reference/scalars.md#verification-evidence): ID/name grammar,
  counter overflow, page size, cursor limits/redaction, and current-version checks.

**Caller tests**

- [Local framing](../../../undercurrent-transport-local/tests/framing.rs): partial
  input survives cancelled receive, oversize headers fail before body allocation,
  EOF truncation, and writes through a tiny pipe.
- [Core connections](../../../undercurrent-transport-core/tests/connection_contract.rs):
  out-of-order finite completion, cancellation, replay after confirmed writes,
  gaps, priority, independent unsubscribe, and admission lifetimes.
- [Multiplex](../../../undercurrent-multiplex/tests/multiplex_contract.rs): bounded
  queues/mailboxes, isolated slow consumers, replay client scope, epoch/eviction
  gaps, and retention deadlines.

The two [fixtures](../../tests/fixtures/) freeze a ping and a capacity rejection;
they do not freeze every possible message body. The local tests do not provide
an exhaustive malformed-byte or Unicode corpus, global allocator bounds, or
end-to-end guarantees for runtime side effects. When a change touches those
areas, add focused evidence at the owning boundary rather than interpreting
codec round trips as full daemon qualification.

[Back to crate](../../README.md) · [Code map](../internals/ownership.md#code-map)

[Crate documentation](../README.md)
