# Find configuration fields, defaults, and validation rules

Find the reference or implementation that owns your question. This index
covers configuration release **0.1.0-beta**, matching the current product
release. The shipped document schema is
[config.schema.json](../../schemas/config.schema.json). Runtime loading and
cross-field validation also depend on the owning Rust sources below; schema-only
validation is not sufficient for all constraints.

| Lookup | Authoritative source or maintained explanation |
| --- | --- |
| Every typed field and default | [document.rs](../../crates/undercurrent-config/src/document.rs) |
| Schema numeric ranges and strict object shapes | [JSON Schema](../../schemas/config.schema.json) |
| File selection, bootstrap values, YAML/TOML budgets | [load.rs](../../crates/undercurrent-config/src/load.rs), [loader explanation](../../crates/undercurrent-config/docs/internals/loading.md) |
| Home derivation and legacy markers | [paths.rs](../../crates/undercurrent-config/src/paths.rs), [path reference](../../crates/undercurrent-config/docs/internals/paths.md) |
| All section meanings and receiving owners | [section reference](configuration-fields.md#sections-and-component-boundaries) |
| Scalar bounds and type constraints | [scalar reference](configuration-fields.md#scalar-constraints) |
| Coupled capacities and frame overhead | [shared constraints](configuration-fields.md#relationships-between-settings), [validate.rs](../../crates/undercurrent-config/src/validate.rs) |
| Retired environment/document inputs | [rejected inputs](configuration-fields.md#retired-inputs) |
| CLI application of defaults | [runtime_config.rs](../../crates/undercurrent-cli/src/runtime_config.rs) |
| Daemon projections and adapter validation | [daemon configuration](../../crates/undercurrent-daemon/docs/internals/configuration/resource-bounds.md) |
| Schema/loader parity coverage and limits | [configuration maintainer guide](../../crates/undercurrent-config/docs/development/contributing.md) |

For document layout, omitted-field behavior, and current bootstrap
discrepancies, start with [structure and defaults](configuration-fields.md#document-structure-and-defaults).
For the limits of capacity settings, see [relationships between settings](configuration-fields.md#relationships-between-settings),
[observations and output](../architecture/observations-and-output.md), and
[client recovery](../sdk/recovery.md).

For a complete edit/check/apply workflow, use [configure an installation](../guides/configuration/README.md).
For current schema/implementation mismatches, see [compatibility](compatibility.md).

[Documentation index](../README.md)
