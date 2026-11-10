# undercurrent-foundation maintainer documentation

Start with the [crate overview](../README.md) for scope and supported consumer
entry points. Use these paths to investigate or change a Foundation primitive.

| I need to… | Start here |
| --- | --- |
| Reproduce a value check locally | [Run the maintained example](development/contributing.md#run-the-example). |
| Decide where a change belongs | [Code owners and caller boundaries](internals/ownership.md#code-map), then [extension steps](development/contributing.md#extend-the-correct-layer). |
| Explain a rejection or changed digest | [Local diagnosis](development/contributing.md#diagnose-a-local-rejection), then the exact [value contract](reference/values.md). |
| Interpret a sensitive-material finding | [Boundary decisions](internals/privacy.md#use-the-result-at-a-boundary), then [field](internals/privacy.md#field-name-policy) and [text](internals/privacy.md#text-policy) rules. |

## Internals

- [Find the owner of a foundational mechanism](internals/ownership.md).
- [Decide whether structured data can cross a diagnostic boundary](internals/privacy.md).

## Reference

- [Check value boundaries and content identity](reference/values.md).

## Development

- [Run and change Foundation primitives](development/contributing.md).

[System architecture](../../../docs/architecture/README.md) · [Workspace code map](../../../docs/architecture/dependencies.md) · [Product documentation](../../../docs/README.md)
