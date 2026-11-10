# Extend and verify a product contract

Use this guide for a protocol behavior change, after identifying the affected
operation, stream, or value in the [source map](../internals/contract-boundary.md#source-map).
Work from the repository root with the pinned toolchain and dependencies from
[maintainer setup](../../../../docs/development/README.md). Protocol examples
below run in memory and need no daemon or credentials.

## Extend a contract

1. Identify the reader-visible operation or stream and its owning contract.
   Use [Cargo manifests](../../../../Cargo.toml) and architecture policy to keep
   consumer surfaces limited to protocol and SDK. Cross-crate runtime behavior
   belongs in shared maintainer docs, linked to this local contract explanation.
2. For a new operation/value, update the protocol-owned
   [registry](../../src/registry.rs), its `ALL` lists and `as_str()` mappings,
   DTO request/success mappings, operation/session-context helpers, and golden
   fixtures together. Product schema/version constants live in
   [contract metadata](../../src/contract.rs). Fixtures are explicit compatibility
   expectations; change them only for an intentional contract change.
   Distinguish stored schema generations from product or wire versions.
3. Decide strict versus compatible fields deliberately. Specify defaults,
   omitted/null differences, serialized tags, contract/capability checks, and
   fingerprint participation. For mutations, document acceptance, durable
   effect/settlement, response loss, authorization, and exact retry/reconciliation
   before adding a convenience API.
4. Implement authoritative checks at the daemon/domain/application boundary.
   Constructors and Serde shape are not sufficient validation. For a decoded
   recovery result, add coherence checks and exercise `.validate()` in consumers.
   Keep private diagnostic material out of Debug/Display and sources used for
   outward logs.
5. Wire daemon dispatch, boundary conversion, presentation, authorization,
   durability, diagnostics and the SDK as appropriate. Stream changes must
   cover item-kind correlation, replay checkpoints, gaps, and resource release.
   Agent changes may affect the protected bridge as well as public polling.
6. Add meaningful contract/fixture and owner-layer tests, update the affected
   pages and Mermaid diagrams, and run the corresponding checks below. Significant
   new design decisions belong in the shared maintainer decisions area with
   context, consequences, and status; do not recast a proposal as current behavior.

## Verification

Run these focused checks from the repository root with the pinned Rust `1.95.0`
toolchain and locked dependencies available:

```sh
cargo test --locked -p aixand-undercurrent-protocol
cargo test --locked -p aixand-undercurrent-protocol --doc
cargo check --locked -p aixand-undercurrent-protocol --examples
cargo run --locked -p aixand-undercurrent-protocol --example inspect_payload
cargo run --locked -p aixand-undercurrent-protocol --example inspect_cancellation
RUSTDOCFLAGS=-Dwarnings cargo doc --locked -p aixand-undercurrent-protocol --no-deps
```

Protocol tests and these examples require no daemon and create no live runtime
sessions. Success means tests/assertions pass, the examples print their expected
summaries, and Rustdoc emits no warnings. Compile-only checks do not prove an
example ran. Build output remains under the configured Cargo target directory.

The current repository validation runner also owns the warning-denied workspace
Rustdoc gate:

```sh
scripts/validate.py --stage rustdoc
```

That is a **workspace** check, not a focused protocol command. Its runner sets the
pinned toolchain and offline dependency mode. Follow the
[validation script](../../../../scripts/validate.py) for repository-wide
changes; focused commands do not replace this gate when repository validation
requires it.
Owner-layer effect tests may need their package's runtime or isolated state;
review those test fixtures before running integration tests.

For Markdown-only changes, verify relative files/anchors and navigation. For
Mermaid changes, parse **and render**, then inspect labels, ordering, boundaries,
clipping and readability in supported themes. `accTitle`/`accDescr` and parsing
alone do not prove visual quality. Record rendered artifacts and an explicit
visual review separately from source checks.

Debug from the narrowest boundary: codec stage/category for malformed data;
contract validator for version issues; daemon boundary for value rejection;
application/ledger evidence for uncertain effects; source retention versus
transport gap for missing output. Inspect the significant tests in the source
map instead of treating a clean Rustdoc build as behavioral coverage.

[Protocol documentation](../README.md)
