# undercurrent-config maintainer documentation

Choose the mechanism or contract you need to investigate below. The
[crate overview](../README.md) describes its scope and starting checks.

## Package and reader scope

Configuration owns typed settings, bounded YAML and TOML loading, shared
validation, and home-derived paths. Its `pub` items support workspace callers.
For external integration, use the [SDK guides](../../../docs/sdk/README.md)
and [protocol reference](../../../docs/reference/protocol/README.md).
The CLI and daemon own their component
adapters. Loading configuration starts neither process.

The internal crate and configuration schema are both at `0.1.0-beta`. Schema,
package, and wire versions vary independently. [Defaults and field meanings](../src/document.rs)
and [numeric ranges](../../../schemas/config.schema.json) have separate sources.
The [manifest](../Cargo.toml) declares no optional features and lists
[Foundation](../../undercurrent-foundation/docs/README.md) as its sole workspace
dependency.

For a daemon-free reproduction, run the [isolated home walkthrough](../../../docs/guides/configuration/diagnose.md#inspect-an-isolated-home).
The [example](../examples/inspect_config.rs) prints the redacted effective
document. For SDK integration, see the [client configuration boundary](internals/loading.md#client-configuration-boundary).
Use the [verification guide](development/contributing.md#verification) for the
pinned toolchain, focused checks, warning-denied Rustdoc, and generated API docs.

## Development

- [Change and verify the configuration contract](development/contributing.md).

[System architecture](../../../docs/architecture/README.md) · [Workspace code map](../../../docs/architecture/dependencies.md) · [Product documentation](../../../docs/README.md)
