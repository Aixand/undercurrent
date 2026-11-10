# Run and change Foundation primitives

Use this guide to reproduce a value check, locate a failing invariant, and
choose verification for a Foundation change. It covers package 0.1.0-beta in this
standalone workspace. Read [ownership](../internals/ownership.md) first if the change
also affects product requests, attempts, or persistence.

## Prerequisites

Run commands from the repository root, beside [Cargo.toml](../../../../Cargo.toml).
Use the pinned [Rust 1.95.0 toolchain](../../../../rust-toolchain.toml) with
`rustfmt`, Clippy, and the locked dependencies. These checks use synthetic,
in-memory data and require no daemon, credentials, environment configuration,
sockets, SQLite, PTY, or session state.

Foundation has no feature switches or platform-specific source branches.
Complete-product platform support is covered by the
[product setup documentation](../../../../README.md).

## Run the example

1. From the repository root, run the maintained
   [foundation_values.rs example](../../examples/foundation_values.rs):

   ```console
   cargo run --locked -p aixand-undercurrent-foundation --example foundation_values
   ```

   Cargo may fetch missing locked dependencies. For a prepared offline checkout,
   add `--offline`. A missing toolchain or cached dependency is a setup failure;
   use the repository setup process. Keep the prescribed manifests and lockfile:
   `--locked` rejects resolution that would change `Cargo.lock`.

2. Check for exit status zero and these five expected application lines
   (Cargo build messages are omitted):

   ```text
   canonical: {"a":1,"b":2}
   digest: sha256:43258cff783fe7036d8a43033f830adfc60ec037382473548ac742b888292777
   bounded string: ready
   opaque URI: trust://passport/subjects/sub_01
   privacy: safe value accepted; sensitive value rejected
   ```

   The example checks canonical JSON and digest round trips, bounded Serde
   decoding, URI validation, and sensitive-material findings. Its assertions
   also reject changed digest content, an oversized string, and a credential
   query. Keep those assertions intact when checking success.

3. If the build fails, check the working directory, pinned toolchain, and
   dependency availability. If an assertion fails after a code change, use
   [local diagnosis](#diagnose-a-local-rejection) to find the owning contract
   before changing a fixture.

The example creates no external state or processes to clean up. Cargo keeps
build artifacts in `target/` or `CARGO_TARGET_DIR`; retain them for subsequent
checks. Continue with [extension steps](#extend-the-correct-layer) to make a
change, or select its verification below.

## Choose the right checks

### Primitive behavior or example changes

Run the unit tests and compile the examples:

```console
cargo test --locked -p aixand-undercurrent-foundation --lib
cargo check --locked -p aixand-undercurrent-foundation --examples
```

Both commands should exit zero. Then [run the example](#run-the-example) to
check its assertions and output.

### Rust API documentation changes

Run doctests and warning-denied Rustdoc:

```console
cargo test --locked -p aixand-undercurrent-foundation --doc
RUSTDOCFLAGS='-D warnings' cargo doc --locked -p aixand-undercurrent-foundation --no-deps
```

The environment assignment in the last line is POSIX shell syntax; on another
shell set `RUSTDOCFLAGS` to `-D warnings` using its environment syntax first.
With Cargo's default target location the resulting symbol index is
`target/doc/undercurrent_foundation/index.html`. The source denies missing docs;
Rustdoc checks complement link and behavioral review, not replace them.

### Integration checks

The repository's [validation runner](../../../../scripts/validate.py)
is the authoritative integrated workflow. `python3 scripts/validate.py
--stage rustdoc` runs its warning-denied **workspace** Rustdoc stage. Its `tests`
stage builds workspace binaries and runs all-target tests plus doctests, and its
`architecture` stage checks dependencies and public surfaces. Run these broader
stages in the repository's integration workflow after focused package checks.
The runner pins the repository toolchain and sets
`CARGO_NET_OFFLINE=true`, so prepare dependencies first.

### Find the regression test

| Change or symptom | Existing verification and what it demonstrates |
| --- | --- |
| Decoded string size | [bounded.rs tests](../../src/bounded.rs): plain/escaped oversize and scalar pre-parser bounds. Add multibyte and exact-bound cases if changing units. |
| Digest format and comparison | [digest.rs tests](../../src/digest.rs): public SHA-256 `abc` vector, mismatch rejection, canonical parser forms, generated schema. These are functional checks, not timing measurements. |
| Canonical bytes | [canonical.rs tests](../../src/canonical.rs): number/Unicode/key-order fixture, exact golden digest, and object insertion-order property test. |
| Canonical resource bounds | Same module: every zero/excessive profile field, expanded profiles, depth/string/key/array/object rejection, aggregate width, and exact escaped-byte output bound. No allocator-usage or throughput guarantee is measured. |
| Classification compatibility | [privacy.rs tests](../../src/privacy.rs): exact lowercase stored values, round trips, ordering, and `max`. |
| Sensitive-material decisions | Same module: normalization, size guards, nested secrets, representative credential prefixes, control/non-ASCII text, `Secret`, and excessive nesting. These do not exhaust every text heuristic or possible secret form. |
| URI parsing and schema | [uri.rs tests](../../src/uri.rs): safe references, userinfo, signed/case-varied/encoded query names, and a differential corpus across component positions, ASCII/Unicode, and length boundaries. See [schema checks and the remaining grammar question](../reference/values.md#schema-and-parser-checks). |
| Request fingerprint changes | [Protocol dispatch tests](../../../undercurrent-protocol/tests/dispatch_binding.rs) and [refactoring tests](../../../undercurrent-protocol/tests/refactoring_contract.rs): semantic changes affect the digest; retry metadata does not. |
| Output digest format | [Protocol execution output tests](../../../undercurrent-protocol/src/execution_output_digest.rs): golden payload, changed bytes, uppercase hex, and unwanted prefix fail appropriately. |
| Callback or policy changes | [Domain dispatch tests](../../../undercurrent-domain/tests/dispatch_binding.rs): narrower product rules around Foundation values. |
| Dependencies or exports | [Architecture policy](../../../../tests/architecture/allowed-graph.json), [gate](../../../../tests/architecture/architecture_gate.py), and [consumer fixture](../../../../tests/architecture/consumer/Cargo.toml): Foundation stays below the supported external SDK/protocol boundary. |

Use a test name to narrow a reproduction, for example:

```console
cargo test --locked -p aixand-undercurrent-foundation decoded_values_are_preflighted_before_bounded_canonical_output
cargo test --locked -p aixand-undercurrent-protocol --test dispatch_binding
```

The second command is relevant when a shared encoding change affects request
fingerprints; it is not necessary for every prose edit.

For Markdown-only edits, check links, anchors, navigation, and referenced paths;
compile examples or run Rustdoc when those artifacts change. Render changed
Mermaid diagrams and visually inspect
their labels, edges, clipping, and readability in the supported display themes;
parsing or presence of accessibility metadata alone is insufficient.

## Diagnose a local rejection

These failures are local computations and have no remote effect to reconcile.
Use the returned typed error when calling a parser/canonicalizer directly;
Serde and product boundaries may translate it into a broader error.

| Observation | First investigation | Recovery |
| --- | --- | --- |
| Unexpected string length error | Decoded UTF-8 bytes versus escaped input length; `BoundedString` visitor. | Correct the input or its owning bound before decoding. |
| Canonical document over budget | `preflight_value`, escaping cost, root depth, and `CanonicalValueBudget`. | Reduce input or review the caller-specific profile; `actual` may only be a partial budget count. |
| Canonicalization suddenly changes a digest | Selected fields, number representation, key canonicalization, serializer version, then hash formatting. | Preserve the golden contract or coordinate a versioned compatibility change. |
| Digest parsing or verification fails | Prefix versus plain-hex DTO field; uppercase; exact payload bytes. | Use the appropriate protocol helper or correct the source bytes; retain the trusted expected digest. |
| Ordinary text flagged as sensitive | Non-ASCII/control/length guards, field normalization, then credential patterns. | Use safe structured diagnostics or the caller's secret-reference path. Do not dump rejected content. |
| Scanner depth error | Root-inclusive nesting and short-circuit behavior. | Reject or simplify the structure; never treat an error as `false`. |
| URI accepted by schema but rejected by Rust | Parser's ASCII, userinfo, and query rules. | Use parser validation at construction and investigate schema drift. |
| URI rejected despite no actual secret | Percent-encoded query name or exact denylisted name. | Use the owning reference format; do not normalize identities blindly. |

Instrument the caller using the [safe diagnostic context](../internals/privacy.md#use-the-result-at-a-boundary)
described in the privacy guide. Debugging a lost response, retention gap,
deadline, or cancellation requires the
[owning runtime/product layer](../internals/ownership.md#other-callers-and-ownership-limits),
not retrying a digest or assuming a local success proves remote completion.

## Extend the correct layer

1. **Choose the owner.** Decide whether the rule is universal to the value or
   specific to a product operation. Put universal representation/resource rules
   here; keep field
   selection, digest domains, authorization, callbacks, retries, and durable
   lifecycle rules in their existing owners. Foundation must not depend on an
   Undercurrent product layer.
2. **Check compatibility consumers before changing a representation.** Digest
   bytes, canonical JSON, classification labels, and accepted URI identities
   can already participate in stored records and request fingerprints. New
   normalization can merge distinct identities; new canonical bytes can break
   comparisons with stored digests. Coordinate migration/version decisions with
   protocol/domain/store owners rather than silently accepting both forms.
3. **Keep representations aligned.** Update the parser, Serde behavior, schema
   (where supplied), and relevant fixtures together. For bounded decoding test
   direct parsing and Serde
   separately, since Serde can reject earlier. For schema changes add
   accepted/rejected differential cases, including the documented URI gaps.
   Retain constant-time verification through `subtle` rather than substituting
   derived equality in verification code.
4. **Preserve resource and failure rules.** For canonicalization changes keep
   [preflight-before-output ordering](../reference/values.md#preflight-and-allocation),
   root-inclusive depth, escaped-byte accounting, the aggregate budget, and
   the writer's backstop. Test exact bounds as well as oversize data. For
   privacy changes test benign false positives, nested detection, errors, and
   the caller's fail-closed policy. Do not expand limits merely to silence a
   failure without reviewing the owning format's resource contract.
5. **Verify and document the change.** Update module/symbol comments, these
   guides, and affected caller docs; run focused checks and the appropriate integrated stages. Preserve
   [NOTICE](../../NOTICE) provenance. A significant compatibility or boundary
   change also needs a shared maintainer decision record owned by the
   integration documentation workflow.

Continue with the [code map](../internals/ownership.md#code-map) to choose the owning file.

[Crate documentation](../README.md)
