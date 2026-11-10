# Look up UCMP and UCHS frame layouts

This reference describes the current client/daemon codecs. Exact field order is
owned by [codec.rs](../../src/codec.rs) and [handshake.rs](../../src/handshake.rs);
UCMP kind names and codes are owned by [registry.rs](../../src/registry.rs).
[Contract tests](../../tests/wire_contract.rs) compare markers and registries with
the fixed [UCMP fixture](../../tests/fixtures/ucmp-contract.json).
[Handshake tests](../../tests/handshake_contract.rs) pin setup markers, message
codes, and rejection categories to the
[UCHS fixture](../../tests/fixtures/uchs-contract.json). Exact-byte fixtures
also check encoded frames; these expectations are independent of the implementation.
This is maintainer material for an internal protocol, not a supported custom
client API. The runtime's [separate UCHS protocol](../internals/ownership.md#runtime-ownership)
does not use these layouts.

## Common header

All multibyte integers here are unsigned and big-endian. The full frame is the
22-byte header plus exactly `body_length` bytes. The configured maximum applies
to the **full frame**, inclusively.

| Byte offsets | Width | UCMP value | Client-handshake UCHS value |
| --- | --- | --- | --- |
| 0–3 | 4 | ASCII `UCMP` | ASCII `UCHS` |
| 4 | 1 | Release marker length `10` | Release marker length `10` |
| 5 | 1 | Direction-specific kind below | Hello `1`, Accepted `2`, Rejected `3` |
| 6–7 | 2 | Reserved flags, must be zero | Reserved flags, must be zero |
| 8–11 | 4 | Body length, excluding header | JSON body length, excluding header |
| 12–21 | 10 | ASCII `0.1.0-beta` | ASCII `0.1.0-beta` |

Neither header has a checksum, compression flag, authentication token, or
request deadline. Payload integrity, product contracts, peer authentication,
and deadlines are separate concerns. The [ping fixture](../../tests/fixtures/ucmp-ping.hex)
is the complete 22-byte frame `55434d500a06000000000000302e312e302d62657461`.

## UCMP field notation

| Notation | Binary representation |
| --- | --- |
| `id` | `u16` UTF-8 byte length followed by validated identifier bytes; see [identifier rules](scalars.md#identifiers). |
| `optional id` | One presence byte: `0` means absent; `1` is followed by `id`; any other byte is invalid. |
| `trace` | Trace ID, span ID, optional parent span ID, optional correlation ID, in that order. |
| `u64` | Eight bytes, big-endian; zero is representable. |
| `payload+` | All remaining body bytes; at least one byte required. |
| `payload*` | All remaining body bytes; empty is allowed. |

Payload-bearing bodies have no additional payload length or terminator. The
codec treats their remainder as opaque bytes, so extra bytes there become part
of the payload. Fixed-layout control and gap bodies reject any trailing bytes.
Trace fields are mandatory for request, open, and resume regardless of the
capability set passed elsewhere; the codec has no capability state.

## UCMP messages

These tables are in numeric code order. `WireMessageKind::ALL` is a complete
registry in semantic order and places resume (`8`) before close-stream (`4`);
do not infer a kind's code from its array index.

**Client → server** (`decode_client`):

| Code | Diagnostic name | Body, in order |
| --- | --- | --- |
| 1 | `request` | request ID, trace, payload+ |
| 2 | `cancel_request` | request ID |
| 3 | `open_stream` | request ID, stream ID, trace, payload+ |
| 4 | `close_stream` | stream ID |
| 5 | `window_update` | stream ID, additional bytes (`u64`) |
| 6 | `ping` | Empty |
| 7 | `client_close` | Empty |
| 8 | `resume_stream` | request ID, stream ID, server epoch ID, after (`u64`), trace, payload+ |

**Server → client** (`decode_server`):

| Code | Diagnostic name | Body, in order |
| --- | --- | --- |
| 128 | `response` | request ID, payload* |
| 129 | `stream_data` | stream ID, sequence (`u64`), payload+ |
| 130 | `stream_gap` | stream ID, first missing (`u64`), last missing (`u64`), reason (`u8`), recovery (`u8`) |
| 131 | `pong` | Empty |
| 132 | `server_close` | Empty |

`decode_client` rejects server codes; `decode_server` rejects client codes.
Unknown codes fail closed. Successful decode does not establish that a request
ID is unique, a stream exists, a resume is permitted, or returned credit is
valid. Those stateful checks belong to transport-core and multiplex.

For `StreamData`, encoding ignores the in-memory `flow_controlled_bytes` field;
decoding sets it to the **full consumed frame length**. `InboundStreamItem`
reports that charge through `encoded_len()`. A gap reports zero charge and its
`sequence()` is `last_missing`. This accounts for a discontinuity without
pretending that gap metadata is delivered data. Sequence continuity and credit
enforcement are caller responsibilities.

### Gap codes

The gap-code mappings live in [codec.rs](../../src/codec.rs); their semantic enums
live in [message.rs](../../src/message.rs). Missing ranges are inclusive and require
`first_missing <= last_missing`. Encoding also checks that the gap's stream ID
matches the enclosing `ServerMessage::StreamItem` ID. Either violation returns
`InvalidGapRange`. No reason-to-recovery pairing is enforced by the codec.

| Reason code | Value | Meaning |
| --- | --- | --- |
| 1 | `LiveQueueOverflow` | Droppable live data exceeded a bounded queue. |
| 2 | `ReplayEvicted` | Requested stream sequence fell out of replay retention. |
| 3 | `ReplayUnavailable` | No retained journal for the stream. |
| 4 | `ServerEpochChanged` | A different server process epoch invalidates replay. |
| 5 | `SourceRetentionEvicted` | Application source evicted data before consumption. |

| Recovery code | Value | Reader action |
| --- | --- | --- |
| 1 | `RequestSnapshot` | Obtain authoritative state or screen. |
| 2 | `RestartQuery` | Start a new filtered query snapshot. |
| 3 | `RestartFromAvailableOffset` | Select an available terminal byte offset. |

See [recovery](../internals/flows.md#recover-after-disconnection-or-a-gap) for the distinction
between these instructions and retrying a mutation.

### Envelope reservation

`MAX_CLIENT_FRAME_ENVELOPE_BYTES` is **1,838**: the header, seven maximum-sized
length-prefixed IDs, two optional-presence bytes, and one `u64`. Resume with
both optional trace fields is the largest client envelope. Reserve this much
in addition to an advertised product payload maximum if all valid IDs must fit.
The [maximum-trace test](../../tests/wire_contract.rs) checks exact fit and a frame
one byte too small. This reservation is not a universal server-frame or memory
overhead constant.

Current consumers include [SDK profile limits](../../../undercurrent-sdk/src/profile.rs)
and [daemon transport configuration](../../../undercurrent-daemon/src/config_transport.rs).

## UCHS bodies

UCHS uses UTF-8 JSON for its body. The discriminator selects the semantic value;
there is no JSON wrapper for the Rust `HandshakeResponse` enum.

| Code | Decoder | Body type |
| --- | --- | --- |
| 1 | `decode_hello` | `ConnectionHello` |
| 2 | `decode_response` | `ConnectionAgreement` |
| 3 | `decode_response` | `HandshakeRejection`: `category` and `retryable` |

See [negotiation values](../internals/negotiation.md#negotiation-values) for hello and agreement
fields and validation. The current Serde structs do not use
`deny_unknown_fields`; do not describe them as rejecting every unknown JSON
field. Typed field validation still runs. Unknown enum values, invalid types,
missing required fields, malformed JSON, and trailing non-whitespace JSON
content fail. Ordinary JSON whitespace is accepted.

The [capacity-rejection fixture](../../tests/fixtures/uchs-capacity-rejection.hex)
encodes `{"category":"capacity_exceeded","retryable":true}`. Fixture bytes
freeze current encoder output; decoders do not require that field ordering.
The bounded JSON writer stops at the first write that would exceed its body
limit. Its `FrameTooLarge.actual` is the attempted size at that point, not a
promise that the entire hypothetical JSON was serialized and measured.

## Incremental decoding

Both codecs return one message with `consumed`, or `NeedMore` with a **total**
buffer requirement. Validation follows these stages in
[codec.rs](../../src/codec.rs) and [handshake.rs](../../src/handshake.rs):

| Available input | UCMP | Client UCHS |
| --- | --- | --- |
| Fewer than 22 header bytes | `NeedMore { minimum_total: 22 }`; magic is not checked yet. | Same. |
| Complete header | Check magic, version, kind **and direction**, flags, then total size. Reject an oversized frame before waiting for its body. | Check magic, format, known kind, flags, then total size. A known wrong-side kind is checked later. |
| Valid header, incomplete declared body | `NeedMore` for header plus declared body length. | Same. |
| Complete declared body | Parse fields and validate payload/trailing bytes. A short required field is `TruncatedField`, not `NeedMore`. | Check direction, then deserialize JSON with typed value validation. |
| Complete valid frame, possibly followed by more bytes | Return one owned message; `consumed` is exactly this frame's length. | Same. |

For buffer advancement, EOF, and transport deadlines, follow the
[caller-buffer walkthrough](../internals/flows.md#decode-one-frame-from-a-caller-buffer).
Neither codec retains partial input or receives EOF/timeout signals.

## Error reference and safe diagnostics

| UCMP `WireError` | Trigger / category |
| --- | --- |
| `MaximumBelowHeader` | Codec maximum below 22; `InvalidFrame` category. |
| `InvalidMagic`, `UnsupportedVersion` | Wrong protocol or encoding byte; `InvalidFrame`. |
| `InvalidMessageKind`, `UnsupportedFlags` | Unknown/wrong-direction kind or nonzero flags; `InvalidFrame`. |
| `FrameTooLarge` | Declared or encoded full frame exceeds maximum; `FrameTooLarge`. |
| `TruncatedField`, `InvalidIdentifier` | Short field or invalid UTF-8/ID grammar; `InvalidFrame`. |
| `InvalidFieldCode` | Optional ID presence byte is neither 0 nor 1; `InvalidFrame`. |
| `EmptyPayload`, `TrailingBytes` | Empty required payload or surplus fixed-layout bytes; `InvalidFrame`. |
| `InvalidGapRange`, `InvalidGapCode` | Invalid interval/stream match or unknown reason/recovery; `InvalidFrame`. |
| `SizeOverflow` | Checked size arithmetic cannot be represented; `InvariantViolation`. |

| UCHS `HandshakeCodecError` | Category |
| --- | --- |
| `MaximumBelowHeader` | `Configuration` |
| `InvalidMagic` | `InvalidFrame` |
| `UnsupportedFormat` | `InvalidFrame` |
| `InvalidMessageKind` | `InvalidFrame` |
| `UnsupportedFlags` | `InvalidFrame` |
| `InvalidPayload` | `InvalidFrame` |
| `FrameTooLarge` | `FrameTooLarge` |
| `SizeOverflow` | `InvariantViolation` |

These mappings are owned by [error.rs](../../src/error.rs) and
[handshake.rs](../../src/handshake.rs). Codec errors report field names, codes,
lengths, and categories without retaining UCMP payload bytes. UCHS
`InvalidPayload` retains a `serde_json::Error` through `Error::source()` but omits
it from `Display` and custom `Debug`. Source-chain logging is not covered by
that redaction. Message enums derive `Debug` and may expose payloads; identifier
and name parse errors also retain rejected values. Use codec categories and
safe structural fields for diagnostics instead of dumping frames or arbitrary
source chains.

[Back to crate](../../README.md) · [Diagnose a failure](../development/contributing.md#diagnose-a-frame-or-negotiation-failure)

[Crate documentation](../README.md)
