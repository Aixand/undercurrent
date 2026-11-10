# undercurrent-platform-windows maintainer documentation

Use this index to find the owner of a Windows change, understand its native
invariants, and choose checks. These pages are for maintainers to the internal
Windows adapter. For product and SDK entry points, see the [crate overview](../README.md).

## Start with a task

| I need to… | Start here |
| --- | --- |
| Run my first native check | [Setup and private-record example](development/contributing.md#prepare-a-native-checkout) |
| Decide which crate owns a change | [Windows responsibilities](internals/ownership.md) |
| Find regression cases and known gaps | [Test coverage and limits](development/test-coverage.md) |
| Select tests or investigate a failure | [Verification](development/contributing.md#choose-checks-for-a-change) · [Diagnosis](development/contributing.md#diagnose-a-windows-failure) |

## Understand a mechanism

| Explanation | Questions answered |
| --- | --- |
| [Private named pipes and console workers](internals/private-ipc.md) | How are peers checked? Which calls block? What wakes a worker during shutdown? |
| [Process identity, jobs, and TCP evidence](internals/processes.md) | When may a child run? What does termination guarantee? When is a negative observation complete? |
| [Private files and exact identity](internals/filesystem-security.md) | Which pin should I use? When can ACLs change? Who flushes, locks, publishes, and removes state? |

The focused diagrams live beside those mechanisms. For individual exported
items, use [generated Rustdoc](development/contributing.md#check-documentation-comments-and-examples).

[System architecture](../../../docs/architecture/README.md) · [Workspace code map](../../../docs/architecture/dependencies.md) · [Product documentation](../../../docs/README.md)
