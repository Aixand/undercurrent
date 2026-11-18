# undercurrent-foundation maintainer documentation

Foundation owns bounded values, content identity, and privacy detection.
Callers choose limits, disclosure policy, and what bytes to hash. Use these
paths to locate or change a primitive. The [crate overview](../README.md)
identifies supported consumer entry points.

| I need to… | Start here |
| --- | --- |
| Reproduce a value check locally | [Run the maintained example](development/contributing.md#run-the-example), then [choose focused checks](development/contributing.md#choose-the-right-checks). |
| Decide where a change belongs | [Code owners and caller boundaries](internals/ownership.md#code-map), then [extension steps](development/contributing.md#extend-the-correct-layer). |
| Explain a rejection or changed digest | [Local diagnosis](development/contributing.md#diagnose-a-local-rejection), then the exact [value contract](reference/values.md). |
| Interpret a sensitive-material finding | [Boundary decisions](internals/privacy.md#use-the-result-at-a-boundary), then [field](internals/privacy.md#field-name-policy) and [text](internals/privacy.md#text-policy) rules. |

## Package and reader scope

These in-memory primitives serve domain, protocol, and storage. Foundation
supplies the shared `0.1.0-beta` release marker. Each codec owns its encoding.
The [manifest](../Cargo.toml) inherits Rust 1.95 and edition 2024 from the
workspace and declares no crate features. Its Rust exports serve internal
composition. For supported endpoints, use the [SDK guides](../../../docs/sdk/README.md)
and [protocol reference](../../../docs/reference/protocol/README.md).
See [NOTICE](../NOTICE) for original source attribution and intentionally
excluded APIs.

## Internals

- [Find the owner of a foundational mechanism](internals/ownership.md).
- [Decide whether structured data can cross a diagnostic boundary](internals/privacy.md).

## Reference

- [Check value boundaries and content identity](reference/values.md).

## Development

- [Run and change Foundation primitives](development/contributing.md).

[System architecture](../../../docs/architecture/README.md) · [Workspace code map](../../../docs/architecture/dependencies.md) · [Product documentation](../../../docs/README.md)
