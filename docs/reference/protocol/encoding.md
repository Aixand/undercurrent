# Encode and validate the current contract

Use this reference when constructing DTOs or implementing a protocol adapter.
The authoritative message shapes are [dto.rs](../../../crates/undercurrent-protocol/src/dto.rs) and the dedicated
[contract](../../../crates/undercurrent-protocol/src/contract.rs), [repair](../../../crates/undercurrent-protocol/src/repair_attempt.rs),
[status](../../../crates/undercurrent-protocol/src/runtime_attempt_status.rs), [cancellation](../../../crates/undercurrent-protocol/src/runtime_cancellation.rs),
and [agent](../../../crates/undercurrent-protocol/src/agent.rs) modules. Stable registry values and product version
constants are owned by the [protocol registry](../../../crates/undercurrent-protocol/src/registry.rs)
and [contract metadata](../../../crates/undercurrent-protocol/src/contract.rs).
Their serialized values are pinned by the
[compatibility fixtures](../../../crates/undercurrent-protocol/tests/refactoring_contract.rs).
These Rust/Serde definitions are the product codec's schema; this codec does not
run a separate JSON Schema validator.

## Versions and envelopes

| Version or discriminator | Current value and scope |
| --- | --- |
| Protocol Cargo package | `0.1.0-beta`, in [Cargo.toml](../../../crates/undercurrent-protocol/Cargo.toml). |
| Product contract | Exactly `0.1.0-beta`; capability must equal `ProductOperation::as_str()`. There is no range or minor-version compatibility check. |
| Product schema generation | String `"0.1.0-beta"`; request schema `undercurrent.request`, response schema `undercurrent.response`. |
| Public connection protocol | Exactly `0.1.0-beta`, owned by [wire version negotiation](../../../crates/undercurrent-wire/src/version.rs); negotiated before product decoding. UCMP/UCHS frames carry the full `0.1.0-beta` marker. |
| Agent bridge | String `"0.1.0-beta"`, the current private bridge identity. |
| Environment persistence | `EnvironmentSchemaVersionDto::Current`, serialized `"0.1.0-beta"`; independent of product contract retirement. |

`ProductContractDto::current(operation, payload_kind)` creates the descriptor.
`validate_product_contract` checks capability, schema, schema generation, then
contract version, in that order. Fixtures with unsupported release identities
fail validation. Only the current release contract is accepted.

| Payload | Serde envelope |
| --- | --- |
| Finite request | `{"operation":"…","input":{…}}`; unit `get_daemon_status` has no input payload. |
| Finite success | `{"outcome":"success","value":{"operation":"…","result":{…}}}`. |
| Finite error | `{"outcome":"error","value":{…}}`. |
| Stream specification | `{"stream":"…","input":{…}}`. |
| Stream activation | `{"outcome":"opened","value":{…}}` or `outcome: "error"`. |
| Nonterminal-stream JSON item | `{"item":"…","value":{…}}`. Here “nonterminal” means a stream other than terminal output, not a lifecycle state. |
| Terminal stream item | [UCTO binary layout](streams.md#binary-terminal-layout), never this JSON item envelope. |

Two successful result tags differ from their request operation spelling:
`runtime_attempt_status` maps to `get_runtime_attempt_status`, and
`runtime_cancellation_status` maps to `get_runtime_cancellation_status` through
`ProductSuccess::operation()`. Correlate using that method. Neither the product
JSON envelope nor its canonical digest carries the SDK's request ID, stream ID,
or trace context; those belong to surrounding SDK/transport framing.

### Optional product capabilities

The [capability registry](../../../crates/undercurrent-protocol/src/registry.rs) defines these names for the current
product contract. The SDK offers them opportunistically; product version equality
alone does not imply that a capability was negotiated.

| Connection capability | Effect |
| --- | --- |
| `undercurrent.error-disposition` | Optional exact high-level disposition on finite/stream-open errors. |
| `undercurrent.error-effect-certainty` | Optional whole-operation certainty on finite errors. |
| `undercurrent.opaque-output-capture` | Explicit opaque output mode on start and configuration detail. |
| `undercurrent.session-revision-precondition` | Atomic expected-revision checks for stop/input and their distinct pre-effect failure category. |

The SDK rejects requested opaque capture or explicit revision preconditions
locally when the required capability is absent; daemon dispatch checks them too.
The codec has no connection state and cannot enforce those agreements itself.

## Bounds and validation layers

[ProductCodec](../../../crates/undercurrent-protocol/src/codec.rs) is stateless and cheaply copyable. All four
`ProductCodecLimits` must be nonzero: request, response, stream specification,
and JSON stream item bytes. Stream-open responses use the **response** limit.
The constructor does not check a negotiated frame budget. The SDK's
[ProductClientRuntime](../../../crates/undercurrent-sdk/src/product.rs) checks that all
product payload limits, including binary terminal headers, fit its client limit.

### Byte limits and allocation

**Decoding:** reject empty input, check the byte limit **before parsing**, then
decode with Serde JSON.

**Encoding:** serialize into a `Vec`, then check its size. **The output limit is
not a preallocation or peak-memory guarantee.**

A payload exactly at the limit is permitted. There is no universal default limit
in this crate: select limits through the connection profile or your bounded adapter.

### Validation responsibilities

| Layer | What it establishes | What still needs checking |
| --- | --- | --- |
| Serde plus `ProductCodec` | Recognized enum variants, represented field types, explicit unknown-field policies, total JSON byte bound. | Domain values, authorization, version descriptors, most digest/identity invariants, per-operation numeric bounds. |
| `validate_product_request_contract` | Current metadata on mutations; repair and both status requests use their direct contract fields; ordinary read-only operations need no descriptor. | Dedicated DTO validation, domain checks, effect/retry semantics. |
| Dedicated `.validate()` methods | Repair/status/cancellation/result consistency and diagnostic bundle bounds. | Current authority, retained identity match, runtime availability, current revisions/deadlines. |
| Daemon boundary and application | Domain conversion, admission and authorization, durable state and effects. | Client interpretation of returned effects and recovery. |

The [SDK decoder](../../../crates/undercurrent-sdk/src/product.rs) checks operation
correlation; it does not automatically call every DTO's `.validate()` method.
An integrator that consumes the strict repair, runtime-status, or cancellation
contracts should invoke their validation methods and compare the returned
identity/binding with the saved request. Constructors enforce local invariants,
but derived deserialization can bypass those constructors. See the tests for
[decoded cancellation revision regression](../../../crates/undercurrent-protocol/src/runtime_cancellation.rs).

### Unknown fields and mutation metadata

Most structs use `deny_unknown_fields`, but this is **not a blanket property**
of all envelopes and internally tagged enums. Read the owning Serde definition.
The explicit additive extension point is `MutationContext.compatible_unknown`,
a flattened sorted map retained for re-encoding.

| Extension-map rule | Exact scope |
| --- | --- |
| Reserved names | Reject case-insensitive names equal to `auth`, `authority`, `credential`, `permission`, `secret`, `security`, or `tenant`. |
| Reserved prefixes | Also reject those names followed by `.`, `_`, or `-`. |
| Inspection boundary | Check top-level names, not arbitrary nested values. This is not an authorization mechanism. |
| Cancellation | Require an empty map, even for otherwise compatible names. |

`MutationContext::default()` is useful for construction but omits the required
current mutation contract. Prefer `MutationContext::current(operation)` and
fill operation-specific retry metadata deliberately. Repair is mutating yet
has **no** `MutationContext`: the peer cannot supply its fence or effect claim.

## Semantic fingerprints and output digests

[canonical.rs](../../../crates/undercurrent-protocol/src/canonical.rs) serializes the typed `ProductRequest`, removes
`input.mutation`, canonicalizes the remaining envelope, and returns a
Foundation `Digest`. Cancellation 0.1.0-beta additionally uses its versioned integer
representation and fingerprint domain, as described below. Thus all mutation metadata—including idempotency key,
attempt, contract, and compatible fields—is excluded.

The operation tag and all
remaining serialized input participate. Defaults omitted by Serde remain omitted;
this is not the hash of the original incoming JSON bytes.

The [Foundation canonicalizer](../../../crates/undercurrent-foundation/src/canonical.rs)
uses bounded canonical JSON. Its default document bound is 1 MiB and depth bound
64, with additional collection/string limits in that source. Canonicalization
can fail even when the product codec byte bound would permit the JSON. The
protocol reports `CanonicalPayloadError::InvalidCanonicalPayload` without
revealing private content.

The fingerprint binds the canonicalized semantics. It cannot distinguish every
large integer: the [binary64 numeric profile](../../../crates/undercurrent-foundation/docs/reference/values.md#numeric-identity)
can merge distinct integer values. Cancellation 0.1.0-beta is an explicit exception: its
four pins and deadline are decimal strings in a separately versioned fingerprint
envelope. Other cancellation digest domains are rejected; see
[current identity rules](cancellation.md#namespace-cutover-and-retained-state).
Other operations retain the generic numeric contract. The fingerprint also does not make different
mutation metadata interchangeable for replay: the daemon's request cache compares retained request bytes, and durable
attempt services enforce their own identities and fences. Cancellation has a
separate [domain-separated command digest](cancellation.md#two-different-digests).

[Output digest helpers](../../../crates/undercurrent-protocol/src/execution_output_digest.rs) hash **exact bytes**,
not text, JSON, or an entire result DTO.

| Output digest property | Rule |
| --- | --- |
| Public `payload_digest_sha256` form | 64 lowercase hexadecimal characters without `sha256:`. |
| Verification order | Reject uppercase, prefixes, wrong length and nonhex characters, then delegate to Foundation's constant-time digest verification. |
| Foundation `Digest` display | Includes a prefix; do not copy its display string into an unprefixed DTO field. |

## Identity, environment, and privacy

### Authenticated identity and secret references

[AuthenticatedRequestContext](../../../crates/undercurrent-protocol/src/auth.rs) holds principal, optional tenant,
and authority **references**, each 1–256 bytes without control characters. Their
Debug output reveals lengths, not values. Serde uses their checked constructors.
This context is established by transport authentication and passed separately
from `ProductRequest`; building it in a client does not grant authority.

`SecretReferenceDto` is an opaque 1–512-byte reference without control characters.
It serializes the reference, never resolved material, and redacts Debug to a
length. It does not validate a URI scheme or resolve a secret.

### Environment persistence checks

Raw-map persistence and serialization use Foundation's [private environment text policy](../../../crates/undercurrent-foundation/docs/internals/privacy.md#text-policy).
Rejection means that detector policy matched, not that the value was proven to
be a secret. The domain's [environment validator](../../../crates/undercurrent-domain/docs/reference/values-and-io.md#ordinary-environment-and-secret-references)
has separate rules; passing either boundary alone does not establish the other.

`EnvironmentSpecDto` stores explicitly non-secret raw variables separately from
secret references. Call `validate_for_persistence()` before treating it as safe
to persist. These checks have different scopes:

| Boundary | Check performed |
| --- | --- |
| Persistence validation | Reject keys present in both maps and credential-classified raw material. |
| Raw-variable serialization | Reject sensitive field names and credential-like environment content; ordinary Unicode and multiline values are permitted. This check alone does **not** check overlap. |
| Deserialization | Does **not** run the persistence check. |

The daemon performs the authoritative boundary validation.

### Environment binding and private results

The optional environment binding carries a digest and working-directory root;
resolution/confinement belongs to the runtime. The standalone helper
`standalone_environment_binding_digest_sha256` hashes the prefix
`undercurrent.standalone-environment-binding.v0.1.0-beta` followed by a NUL byte and the
root's exact bytes. It does not normalize or verify the path, and ecosystem
callers must retain their Environment product's binding digest instead.

`ExecutionContextDto` intentionally carries merged environment values for an
authorized execution adapter. It must never become a list projection or log
payload. Commands, output, terminal input, agent results, and serialized requests
are also private. Redacted envelope Debug implementations do not make arbitrary
nested DTOs safe to log.

## Codec errors and safe diagnostics

[ProductCodecError](../../../crates/undercurrent-protocol/src/error.rs) exposes a stable stage (`Request`,
`Response`, `StreamSpec`, `StreamItem`, `TerminalOutput`) and category
(`Capacity`, `Malformed`, `UnsupportedKind`). Empty input, malformed JSON,
invalid binary lengths/geometry, and offset overflow are malformed; unknown
terminal discriminators are unsupported. Inspect `limit`, `observed_bytes`,
`declared_bytes`, and `unsupported_kind_code` when present.

Its own Display and Debug redact payloads. `Error::source()` can retain the
underlying Serde error; source-chain formatting is not covered by that redaction
promise. Log the stable accessors instead of raw input or arbitrary error sources.
These are local codec errors, distinct from [server errors and effect evidence](recovery.md).

[observability.rs](../../../crates/undercurrent-protocol/src/observability.rs) translates stable application strings
without importing application types. `invalid_argument` becomes `InvalidInput`;
`unauthenticated` and `failed_precondition` become `Rejected`; permission,
not-found, conflict, capacity, rate-limit, cancellation, timeout, interruption,
unavailability, replay, stale cursor, protocol, and unsupported strings preserve
their distinct boundary statuses. Unrecognized strings map to `Failed`.
This status vocabulary is separate from `ProductErrorCategory` and does not
establish effect certainty.

`qualified_observability_name(kind, local_name)` accepts nonempty lowercase ASCII
letters, digits, and underscores and returns
`undercurrent.<kind>.<local_name>`. It validates spelling, not membership
in a registry. Supply stable registry strings, not user input, identities, or
payload content.

See [protocol contract tests](../../../crates/undercurrent-protocol/tests/protocol_contract.rs),
[version and canonicalization tests](../../../crates/undercurrent-protocol/tests/refactoring_contract.rs), and the
[extension checklist](../../../crates/undercurrent-protocol/docs/README.md#extend-a-contract) when changing this boundary.
