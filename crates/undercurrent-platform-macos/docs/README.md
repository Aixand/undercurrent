# undercurrent-platform-macos maintainer documentation

Use these pages to change or debug the macOS process adapter. The
[crate overview](../README.md) describes its scope and supported audience.

| Maintainer task | Reading path |
| --- | --- |
| Verify a change or add a bounded native query | [Work on the adapter](development/contributing.md) (task guide) |
| Find why an observation is missing, incomplete, or rejected | [Diagnose process evidence](development/diagnosing-observations.md) (task guide) |
| Trace query results into a foreground observation | [Follow evidence into the runtime](internals/runtime-integration.md) (explanation) |
| Check accepted native replies, bounds, errors, or buffer lifetime | [Darwin query contracts](reference/query-contracts.md) (reference) |

For a first change, run the self-query check in the development guide, read the
contract for the affected query, then trace its caller in the runtime guide.
For system context, use the shared [process ownership diagram](../../../docs/architecture/README.md#process-boundaries)
and [workspace code map](../../../docs/architecture/dependencies.md).

[Product documentation](../../../docs/README.md)
