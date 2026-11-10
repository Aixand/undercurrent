# Change and verify the configuration contract

Use this guide when adding a setting, changing validation, or debugging a
configuration discrepancy between binaries. It targets maintainers to the
current workspace. Read the [load flow](../internals/loading.md) first; field defaults,
ranges, and shared relationships are described in the
[configuration reference](../../../../docs/reference/configuration-fields.md).

## Change a setting without splitting the contract

Work from the workspace root with the pinned Rust toolchain and dependencies
available; see [maintainer setup](../../../../docs/development/setup.md) for build prerequisites.
Follow the settings through their receiving component as well as this crate.

1. **Choose the owner.** Establish the receiving component's contract and whether the setting belongs
   to configuration, an explicit operation, or a client API. Do not add runtime
   imports to this crate; its allowed internal dependency list is empty.
2. **Define the field.** Add or revise the Serde field, its units and meaning, and its default in
   [document.rs](../../src/document.rs). All product structs deny unknown fields
   and default omitted fields. Review bootstrap separately: the whole-section
   and field-level telemetry defaults differ. Keep arbitrary values out of
   diagnostic errors.
3. **Update the schema.** Update the owning [schema](../../../../schemas/config.schema.json). The loader
   embeds this file at compile time and traverses numeric `minimum`/`maximum`
   through local `$ref` and `properties`. It does not interpret arbitrary
   schema keywords. A new string pattern, union, array, or cross-field rule
   needs deliberate shared validation and shape-check review, not just a
   schema edit. Rebuild to pick up schema changes.
4. **Check coupled bounds.** Add shared relationships in [validate.rs](../../src/validate.rs) to reflect
   component constructors, including overflow checks and coupled producer/
   consumer budgets. Explain why the bound exists. Review default settings
   against every new rule: omitted fields are checked too.
5. **Update projections.** Update the receiving CLI/daemon projection and any runtime owner with its
   own invariants. Projections retain their validation so directly constructed
   settings cannot evade their contracts. Do not assume an external SDK
   consumes the new field: the CLI uses a compiled connection profile, and
   Python has separate async connection options.
6. **Test the boundaries.** Add focused cases at relevant boundaries: omission/default, accepted and
   rejected endpoints of a range, invalid type, unknown key, related-capacity
   failure, and private-value non-disclosure. Test YAML and TOML equivalence
   where representation matters. Keep process-environment mutations out of
   concurrently running tests; supplied `PathEnvironment` isolates discovery
   inputs but does not bypass actual retired-variable guards.
7. **Review diagnostics and documentation.** Review [daemon parity tests](../../../undercurrent-daemon/tests/config_validation_parity.rs),
   the [field reference](../../../../docs/reference/configuration-fields.md), the
   inspection sample, and reports. A new user-controlled
   string needs explicit redaction review: the report serializes product
   settings by default. For protocol-envelope changes, compare shared
   reservations to the wire codec and retain boundary tests.

Successful verification means shared loading rejects the intended invalid
documents, accepts valid defaults/overrides, and still agrees with component
constructors for the tested domain. Add targeted cases where a broad sampling
test cannot establish that behavior. The rationale for duplicate validation
is early offline feedback without weakening component-owned contracts.

## Code map

| Module or boundary | Responsibility and investigation entry point | Relevant evidence |
| --- | --- | --- |
| [lib.rs](../../src/lib.rs) | Private modules, public document/load/path re-exports; shared validator stays private | [Architecture policy](../../../../tests/architecture/allowed-graph.json) and [crate manifest](../../Cargo.toml) |
| [document.rs](../../src/document.rs) | All product sections, Serde shape, defaults, scrollback cell cap; no component construction | [Default/override parity tests](../../tests/config_loading.rs) and [daemon component parity](../../../undercurrent-daemon/tests/config_validation_parity.rs) |
| [load.rs](../../src/load.rs) | Bootstrap defaults, discovery, bounded readers, YAML pre-scan, main-file shortcut decoding, typed errors, reports, projections; path-candidate precedence helper | Inline loader tests and [file-loading integration tests](../../tests/config_loading.rs) |
| [paths.rs](../../src/paths.rs) | Native environment capture, lexical home paths, legacy marker probes, path errors, base joining | Inline default/custom/XDG/legacy/initialized-home tests |
| [validate.rs](../../src/validate.rs) | Safe field/type checking, embedded-schema numeric ranges, prefix grammar, overflow and component relationships | [Strict loading tests](../../tests/config_loading.rs), [component and wire-envelope parity](../../../undercurrent-daemon/tests/config_validation_parity.rs) |
| [CLI runtime config](../../../undercurrent-cli/src/runtime_config.rs), [local commands](../../../undercurrent-cli/src/local.rs) | Endpoint/path validation, CLI projection, offline show/validate | [CLI YAML tests](../../../undercurrent-cli/tests/config_yaml.rs) |
| [Daemon configuration](../../../undercurrent-daemon/src/config.rs) and its [transport](../../../undercurrent-daemon/src/config_transport.rs), [client](../../../undercurrent-daemon/src/config_client.rs), [service](../../../undercurrent-daemon/src/config_services.rs) projections | Convert numbers into validated component types; preserve private/public boundaries | Their local tests and daemon parity tests |

For the full symbol inventory, use generated Rustdoc. The module map gives
ownership and test entry points rather than repeating field declarations.

## Significant tests and their limits

| Check and evidence | Not established by this check |
| --- | --- |
| **[YAML/TOML parity](../../tests/config_loading.rs)**<br>Both formats yield the same sample defaults/overrides, including zero scrollback | All possible YAML/TOML values are equivalent |
| **[Main-file discovery](../../tests/config_loading.rs)**<br>Defaults, all three active names, ambiguity, malformed-file rejection, ignored backup name, no state creation | Every IO/symlink race or permission case |
| **[Shortcut file selection](../../tests/config_loading.rs)**<br>Main-file shortcuts and ignored separate shortcut files | Main-file snapshot consistency |
| **[Strict document loading](../../tests/config_loading.rs)**<br>Duplicate keys, multiple documents, anchors/aliases/tags, bad shape/types, generation and path rejection | Exhaustion of every parser budget boundary |
| **[Diagnostic redaction](../../tests/config_loading.rs)**<br>Private-value sentinel is absent from errors/report and home is absent from report | Blanket safety of derived `Debug` or future fields |
| **[File and history bounds](../../tests/config_loading.rs)**<br>Main-file overflow and 10,000/10,001 rows at default 500 columns | Every width, machine word size, or parser event/node limit |
| **Inline load tests**<br>Retired path fields/environment input, explicit/home discovery, ignored older wrapper location, helper precedence | `invalid_or_missing_explicit_documents_fail_closed` uses extensionless filenames for several cases, so it is not specific proof of each schema/parse branch |
| **Inline path tests**<br>Native default joins, XDG nonselection, custom-home isolation, invalid home errors, current-layout bypass, lexical joining | Database contents/schema validity or full historical migration coverage |
| **[Component bound sampling](../../../undercurrent-daemon/tests/config_validation_parity.rs)**<br>Mutates each numeric leaf to 0, 1, 2, 65,536, `u32::MAX`, `u64::MAX`; flags a shared acceptance rejected by daemon projection | Full combinations of fields, arbitrary numbers, cross-platform proof, or successful runtime startup |
| **[Wire-envelope parity](../../../undercurrent-daemon/tests/config_validation_parity.rs)**<br>Exact-fit and one-byte-over request envelope; handshake sizes 11/12 against codec | Every product/terminal payload or every future codec variant |
| **[CLI offline inspection](../../../undercurrent-cli/tests/config_yaml.rs)**<br>Offline command success/failure and no `run` creation for its cases | A live daemon has applied the configuration |

Inline cases are in [load.rs](../../src/load.rs) and [paths.rs](../../src/paths.rs).
The linked test files contain the exact test names for filtering a focused run.

## Verification

Run the relevant checks below from the workspace root. They use
temporary homes in their tests and do not require a running daemon. Ensure
retired environment variables listed in the [reference](../../../../docs/reference/configuration-fields.md#retired-inputs)
are unset; otherwise the loader deliberately rejects the test process inputs.
Existing Cargo target artifacts need no runtime cleanup.

| Change to verify | Command |
| --- | --- |
| Shared discovery, formats, defaults, validation, or paths | `cargo test --locked -p undercurrent-config` |
| Component bounds or projections | `cargo test --locked -p undercurrent-daemon --test config_validation_parity` |
| CLI preferences or offline report behavior | `cargo test --locked -p undercurrent-cli --test config_yaml` |
| Rust API comments | `RUSTDOCFLAGS='-D warnings' cargo doc --locked -p undercurrent-config --no-deps`<br>`cargo test --locked -p undercurrent-config --doc` |
| Maintained Rust example | `cargo check --locked -p undercurrent-config --examples`, then run the [isolated walkthrough](../../../../docs/guides/configuration/diagnose.md#inspect-an-isolated-home) |
| Any patch | `git diff --check` |

Expected result is exit status 0 for each applicable command. Passing the
crate tests does not substitute for daemon parity or CLI command checks when
their projections change. Run the [inspection walkthrough](../../../../docs/guides/configuration/diagnose.md#inspect-an-isolated-home)
to exercise the maintained example. If dependency resolution or a platform
adapter prevents a check, report that check as unverified rather than treating
source review as execution evidence.

The workspace's official [validation runner](../../../../scripts/validate.py)
has a warning-denied Rustdoc stage:

```sh
python3 scripts/validate.py --stage rustdoc
```

This integration stage builds workspace-wide documentation with all features.
It sets the pinned toolchain and offline Cargo mode, so dependencies must
already be available. Use the focused command above while iterating on this
crate, and the runner for the integration Rustdoc gate.

After a focused `cargo doc`, open
`target/doc/undercurrent_config/index.html` (or the corresponding
`CARGO_TARGET_DIR` location) for the generated API reference. For documentation
changes, check relative links/anchors, compare field claims to current source,
and render every changed Mermaid diagram and visually inspect labels,
connections, clipping, and readability in supported themes. Parsing alone is
insufficient.

## Debug a disagreement

If schema tooling accepts a document that `uc config validate` rejects, inspect
the shared cross-field checks and typed integer bounds first, then the CLI
adapter. Schema validation does not simulate component construction. If the
shared loader accepts a value rejected by a component, preserve a minimal
isolated fixture and investigate that constructor with the parity test; do not
relax the component just to obtain a green check.

If a report seems different from runtime behavior, verify which boundary reads
the field and whether the daemon has restarted. If a valid file still fails
to load, inspect retired process variables, duplicate active entries, legacy
markers, and filesystem access before changing the document. Use
[typed errors and safe diagnostics](../../../../docs/guides/configuration/diagnose.md#diagnose-a-rejection).

For storage and transport rejections, use [storage diagnosis](../../../../docs/guides/daemon/storage.md)
and [configuration diagnosis](../../../../docs/guides/configuration/diagnose.md). Check
[bootstrap compatibility](../../../../docs/reference/configuration-fields.md#document-structure-and-defaults)
when investigating default-profile precedence. A schema/loader mismatch does
not authorize silently importing old state or adding a fallback.

[Crate documentation](../README.md)
