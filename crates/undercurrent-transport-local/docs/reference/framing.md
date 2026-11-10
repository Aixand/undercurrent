# Look up the bounded local frame contract

Use this reference when implementing or debugging the internal byte-stream
adapter. The owning implementation is [framed.rs](../../src/framed.rs), tested by
[framing.rs](../../tests/framing.rs). Native authentication is a separate layer;
`LocalFrameStream<S>` also works over an in-memory Tokio duplex stream.

## Framing versus semantic decoding

| Layer | Owner | Responsibility |
| --- | --- | --- |
| Native stream | [Endpoint adapters](../internals/endpoints.md) | Protected address, OS peer verification, byte I/O. |
| `LocalFrameStream` | This crate | Read a 22-byte header, validate magic and bounded total size, return exactly one complete frame. |
| UCHS | [Handshake codec](../../../undercurrent-wire/src/handshake.rs) | Validate handshake format, kind, payload, and negotiation response. |
| UCMP | [Wire codec](../../../undercurrent-wire/src/codec.rs) and transport core | Validate message format and connection semantics. |
| UCLC | `CAPACITY_CLOSE_FRAME` in this crate; [SDK receiver](../../../undercurrent-sdk/src/local.rs) | One exact server-to-client capacity termination control. |

The local framer recognizes `UCHS`, `UCMP`, and `UCLC` magic at bytes 0–3.
Bytes 8–11 are an unsigned big-endian `u32` body length. It checks `12 + body`
with overflow detection against `maximum` **before** resizing the body buffer.
The bound includes the header. The framer does not validate the meaning of
bytes 4–7, message direction, or the body. UCHS/UCMP decoders must consume the
whole returned frame; both SDK and server check that condition.

UCHS and UCMP both carry the current `0.1.0-beta` release marker. Their formats
are owned by wire;
[CURRENT_PROTOCOL_VERSION](../../../undercurrent-wire/src/version.rs) is 0.1.0-beta.
The local termination control, product payload schemas, and Python bridge
use the same release identity with their respective encodings.

## Buffer and cancellation behavior

`new(io, maximum)` requires at least 12 bytes or returns `InvalidData`.
`set_maximum` also rejects a value below 12, below the current allocated input
frame, or below a pending output frame. The server starts with the handshake
bound and changes to the negotiated UCMP full-frame bound after sending the
acceptance. It does not reset partial I/O when the bound changes.

| Event | Behavior |
| --- | --- |
| Fragmented read or cancelled `next()` future | The same `LocalFrameStream` keeps the input bytes and filled offset. Poll it again to continue. |
| Coalesced frames | Each item returns one complete frame; the next poll reads the next frame. |
| Clean EOF at a frame boundary | Returns `None`, and stays ended. |
| EOF inside header or body | Returns `UnexpectedEof` once, then `None`. |
| Invalid magic, oversized/overflowing length, or underlying read error | Returns an error once and ends the read stream. There is no resynchronization scan. |
| `start_send` with an existing pending frame | Returns `InvalidData` (`write pending`). |
| Outbound frame shorter than 22 or larger than the bound | Returns `InvalidData`; outbound magic, header length, and payload are not validated by the sink. Use the owning codec. |
| Partial write | Stores one `Arc<[u8]>` and an offset; each successful write advances the offset. |
| Zero-length write progress | Returns `WriteZero`. |
| Flush/close | `poll_ready` first flushes the previous frame; `poll_close` flushes and then calls underlying shutdown. |

Cancellation safety means retaining the **same value** preserves progress. It
does not mean replaying a cancelled `send` is safe: the sink may already own
some or all of that frame. Resume flushing the existing sink, or close the
connection under its owner's failure policy. Dropping the stream discards its
buffers and closes the owned I/O handle. Write errors do not automatically
mark the read half ended or reset the write state; the server treats them as
connection failures. Do not use this framing property as a mutation retry rule.

The input frame and one output frame are individually bounded, not a promise
that the entire process uses at most `maximum` bytes. Completed frames handed
to callers, native buffers, scheduler queues, replay, and writer queues have
separate owners and bounds. There is no timer inside `LocalFrameStream`.

## Capacity termination control

The authoritative bytes are `CAPACITY_CLOSE_FRAME` in
[framed.rs](../../src/framed.rs). This is a local adapter control, not a UCMP
product error or UCHS rejection.

| Byte range | Encoding | Value |
| --- | --- | --- |
| 0–3 | ASCII magic | `UCLC` |
| 4–5 | Big-endian `u16` release marker length | `10` |
| 6–7 | Big-endian `u16` kind | `1` (capacity exceeded) |
| 8–11 | Big-endian `u32` body length | `0` |
| 12–21 | ASCII release identity | `0.1.0-beta` |

The SDK recognizes only this exact 22-byte frame after negotiation and reports
`server_connection_capacity` with backoff guidance. Other UCLC values can pass
the shallow framer but fail semantic decoding; they do not gain a fallback
meaning. The server attempts this control after an active connection fails
with the `Capacity` category. Sending it is best effort and bounded by the
write timeout; delivery is not guaranteed. Physical connection-limit rejection
before negotiation simply drops the accepted stream and does not send UCLC.

Run the [in-memory example](../development/contributing.md#exercise-framing-without-a-daemon)
to see complete-frame handling without a native endpoint. For ordering and
failure semantics, continue to [the server lifecycle](../internals/server.md).

[Crate entry point](../../README.md)

[Crate documentation](../README.md)
