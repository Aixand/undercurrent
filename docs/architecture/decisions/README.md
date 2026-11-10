# Understand current design decisions

These records explain consequential choices in the current source. Each record
states its status and whether it documents an existing design retrospectively
or records an accepted change. Implementation does not establish release status.

| Record | Status | Question |
| --- | --- | --- |
| [Keep effects outside the aggregate transaction](0001-effect-and-observation-boundaries.md) | Implemented, with stated adapter limits | Why are durable intent, process effects and observations separate? |
| [Follow foreground processes by default in the CLI](0002-cli-foreground-default.md) | Accepted and implemented | Why should direct launches discover applications behind wrappers? |
| [Authority for graceful runtime cancellation](0003-runtime-cancellation-graceful-authority.md) | Accepted and implemented | Why does Graceful require a separate Force command? |
| [Configure finite input independently from resource budgets](0004-input-size-limits.md) | Accepted and implemented | Why default to 5 MiB, and which other limits must accommodate it? |

For a new significant decision, record its status, context, decision,
consequences, alternatives or tradeoffs, source evidence and validation. Mark a
superseded record explicitly and link its replacement. Temporary migration
checklists belong in dated plans or issues, not these lasting authoring rules.

[Architecture](../README.md) · [Documentation index](../../README.md)
