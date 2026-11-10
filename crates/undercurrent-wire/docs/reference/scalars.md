# Look up scalar validation and version scope

Use this reference when choosing a value type or reviewing deserialization.
It describes the types exported by this internal crate, not equivalent-looking
types owned by foundation, domain, protocol, or SDK. The source files linked
below own these contracts. Generated Rustdoc supplies the complete symbol
inventory; [the maintainer guide](../development/contributing.md#verify-a-change) shows how to
build it.

## Identifiers

[identifiers.rs](../../src/identifiers.rs) defines distinct string-backed types for
message, idempotency, correlation, causation, event, run, actor, authority,
project, resource, source, tenant, trace, span, physical connection, logical
client, request, stream, and server-epoch identities. The shared macro gives
each type the same validation, comparison, hashing, `FromStr`, display, and
string Serde representation. `into_string()` transfers the owned value.

| Rule | Exact behavior |
| --- | --- |
| Nonempty | Empty input returns `IdentifierParseErrorReason::Empty`. |
| Byte limit | At most `MAX_IDENTIFIER_LEN` = 256 UTF-8 bytes; checked before characters. |
| Characters | Rejects `char::is_ascii_control()` or `char::is_whitespace()`; reports the first rejected character's byte index. |
| Accepted form | No UUID, ASCII-only, hexadecimal, namespace, case, or punctuation requirement. Non-whitespace Unicode can be accepted. |
| Serde | Reads a string and calls the same constructor. |

An ID is a validated label, not proof of uniqueness, authority, existence,
randomness, or ownership. The codec checks only its grammar. In particular,
`IdempotencyKey` is a value type and does not store results or deduplicate work.
`RequestId` is used for connection-local correlation; a product retry contract
must supply its own identity and scope. `ClientId` and `ServerEpoch` take on
replay meaning in [the owning components](../internals/flows.md#recover-after-disconnection-or-a-gap).

`IdentifierParseError` retains the full rejected string for `value()`, and its
derived `Debug` exposes it. `Display` prints a reason, which may include the
rejected character. The binary decoder maps parse errors to
`WireError::InvalidIdentifier { field }`, discarding the rejected value.

## Names

[name.rs](../../src/name.rs) owns `NamespacedName`.

| Rule | Exact behavior |
| --- | --- |
| Length | 1–192 bytes (`MAX_NAME_LEN`). |
| Alphabet | Lowercase ASCII letters, digits, `_`, `-`, and `.`. No normalization. |
| Segments | Dot-separated segments must be nonempty. A dot is optional; a segment may start with a digit, underscore, or hyphen. |
| Validation order | Empty input, length, empty segments, then characters. |
| Character errors | Uppercase ASCII produces `NotLowercase`; other disallowed characters produce `InvalidCharacter`. Errors include byte indices where applicable. |
| Serde | Reads a string and reuses validation. |

A syntactically valid capability name need not be registered or supported.

Like identifier errors, `NameParseError` retains the rejected value and exposes
it through `Debug` and `value()`. Avoid indiscriminate parse-error dumps.

## Ordering and pagination

[ordering.rs](../../src/ordering.rs) supplies reusable scalar mechanics.

### Revision and StreamSequence

Both are transparent `u64` counters, initially/default zero. `new` accepts any
`u64`; `next()` returns a new value or `CounterExhausted` without mutation or
wrapping. The error's `counter()` identifies the exhausted type:

| Type | Exhaustion name | Ordering scope supplied by the caller |
| --- | --- | --- |
| `Revision` | `revision` | Entity revision and durable commit ordering. |
| `StreamSequence` | `stream_sequence` | Stream-local delivery, replay, and gaps. |

Construction and Serde accept zero, repeated values, and arbitrary large values;
they cannot check monotonicity against previously seen items. The caller owns
persistence and ordering. Neither type is a synchronization primitive or a
global sequence allocator, and a stream sequence is not a terminal byte offset.

### PageSize

`PageSize` is a transparent `u32`. `new(value, maximum)` rejects zero for either
argument and rejects a value above the maximum. Deserialization validates
against `u32::MAX` only: callers must reapply the product's maximum after decoding.

### OpaqueCursor

`OpaqueCursor` accepts a nonempty string of at most 4,096 bytes
(`MAX_OPAQUE_CURSOR_BYTES`) and rejects Unicode control characters using
`char::is_control()`. Unlike identifiers, cursors allow ordinary spaces.
Product owners validate content, signatures, query binding, expiration, and
pagination consistency; wire performs no base64 decoding or signing.

`Debug` prints only byte length. `Display`, `as_str()`, `into_string()`, and
serialization reveal the actual value. `PaginationValueError` distinguishes
zero page size, exceeded product size, empty/oversized cursor, and control
character by byte index.

## Trace context

[trace.rs](../../src/trace.rs) carries a required `TraceId` and `SpanId`, with
optional parent `SpanId` and `CorrelationId`. `new` starts both optional fields
absent; builder methods add them. Serde omits absent optionals; binary UCMP
uses the explicit presence bytes in the [frame reference](wire-format.md#ucmp-field-notation).
Requests, stream open, and stream resume carry context; responses correlate
by request ID and stream data by stream ID.

Trace and span IDs use generic identifier validation. They do not enforce W3C
hexadecimal widths or nonzero trace values. Context holds no sampling flag,
timestamps, baggage, or exported span, and it does not generate IDs. The
[SDK trace provider](../../../undercurrent-sdk/src/trace.rs) and
[daemon dispatch telemetry](../../../undercurrent-daemon/src/dispatch_telemetry.rs)
own generation and instrumentation. A resumed stream can carry new activation
context; that alone does not establish a new product operation.

## Versions

All version identities in this release use `0.1.0-beta`; each format owns its
encoding and exact-current validation.

| Axis | Current source and value | Consequence |
| --- | --- | --- |
| Cargo package | [Cargo.toml](../../Cargo.toml): `0.1.0-beta` | Shared workspace release metadata |
| Connection protocol | [version.rs](../../src/version.rs): full `0.1.0-beta` string | Hello/agreement require exact-current acceptance |
| UCMP encoding | [codec.rs](../../src/codec.rs): marker length `10`, ASCII `0.1.0-beta` | Header rejects every other release marker |
| Client UCHS encoding | [handshake.rs](../../src/handshake.rs): marker length `10`, ASCII `0.1.0-beta` | Header rejects every other release marker |
| Product contract and schema | [protocol contract](../../../undercurrent-protocol/src/contract.rs) and [compatibility policy](../../../undercurrent-protocol/src/compatibility.rs): `0.1.0-beta` | Product validation requires exact current metadata |

`ProtocolVersion` carries major/minor/patch and the beta marker. Display and
Serde use a canonical full release string. `reader.can_read(produced)` is true
only when both values equal `CURRENT_PROTOCOL_VERSION`. Equal future versions,
stable `0.1.0`, and older releases do not pass.
Negotiation uses the same sole-current rule. Adding a
version requires an explicit compatibility design; there is no range or
highest-common-version selection in this implementation.

## Verification evidence

Inline tests in [identifiers.rs](../../src/identifiers.rs) and
[name.rs](../../src/name.rs) cover accepted forms and representative invalid
construction/deserialization. [ordering.rs](../../src/ordering.rs) checks counter
exhaustion, product page-size construction, zero on the wire, and cursor
redaction/limits. [version.rs](../../src/version.rs) tests sole-current acceptance.
The [wire contract tests](../../tests/wire_contract.rs) cover trace round trips
including both optional fields and maximum-size envelope accounting. These
are evidence for the cases tested, not an exhaustive Unicode or fuzzing suite.

[Back to crate](../../README.md) · [Frame reference](wire-format.md)

[Crate documentation](../README.md)
