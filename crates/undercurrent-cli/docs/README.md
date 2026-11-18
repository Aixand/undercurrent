# undercurrent-cli maintainer documentation

Use these guides to locate and change the CLI implementation. They cover the
current 0.1.0-beta crate in the root workspace. Start with ownership and the source
map, then follow the mechanism affected by your change. The CLI owns command
routing, local state, and presentation. Daemon-backed commands use the Rust SDK
for product requests.

## Package and reader scope

The CLI translates arguments, formats human and JSON output, manages
bounded subscriptions and terminal views, and guards local lifecycle actions.
Its exported Rust adapters support internal orchestration. For supported
consumer APIs, use the [SDK guides](../../../docs/sdk/README.md) and
[protocol reference](../../../docs/reference/protocol/README.md).

Install matching `uc`, `uc-daemon`, and `uc-session-host` binaries before running
the first-session tutorial. Some commands can start an absent daemon. Consult
the [startup rules](../../../docs/guides/daemon/cli-lifecycle.md#start-and-verify-readiness)
when scripting. For interactive tasks, see the [workspace TUI](../../../docs/guides/cli/workspace.md),
[attach and splits](../../../docs/guides/cli/interactive.md),
[groups](../../../docs/guides/cli/groups.md), and
[shell hooks](../../../docs/guides/cli/shell-integration.md).

## Internals

| Topic | Guide |
| --- | --- |
| CLI, SDK, daemon, and local-state boundaries | [Ownership](internals/ownership.md) |
| Routing, arguments, commands, UI, groups, and lifecycle source | [Code map](internals/code-map.md) |
| Preparation, request submission, start results, and response loss | [Session startup](internals/session-start.md) |
| Output subscriptions, attach rendering, gaps, cancellation, and cleanup | [Terminal and streams](internals/terminal-and-streams.md) |
| Registry publication, pending mutations, restoration, and reconciliation | [Groups and recovery](internals/groups-and-recovery.md) |

## Development

[Making and verifying a change](development/contributing.md) maps change types
to affected contracts, focused tests, and native integration checks. Commands
run from the repository root. Use the [integration evidence map](development/contributing.md#choose-integration-evidence)
to choose checks for output, startup, reset, or terminal changes.

## Using the CLI

For commands and operator procedures, use the [first-session tutorial](../../../docs/getting-started/first-session.md),
[CLI task guides](../../../docs/guides/README.md), [command reference](../../../docs/reference/cli-commands.md),
and [output contracts](../../../docs/reference/cli-output.md).

[Crate overview](../README.md) · [System architecture](../../../docs/architecture/README.md) · [Documentation index](../../../docs/README.md)
