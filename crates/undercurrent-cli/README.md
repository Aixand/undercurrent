# undercurrent-cli

Use `uc` to start, inspect, control, and attach to local Undercurrent sessions.
This crate supplies the native CLI; the daemon and session host run separately.
Package version 0.1.0-beta belongs to the root Cargo workspace.

| Reader | Start here |
| --- | --- |
| CLI user | [Run your first session](../../docs/getting-started/first-session.md), then [choose commands](../../docs/reference/cli-commands.md) |
| Operator | [Configure and manage the daemon](../../docs/guides/daemon/cli-lifecycle.md), [interpret output and recover failures](../../docs/reference/cli-output.md) |
| Interactive user | [Workspace TUI](../../docs/guides/cli/workspace.md), [attach and split panes](../../docs/guides/cli/interactive.md), [restore groups](../../docs/guides/cli/groups.md), [install shell hooks](../../docs/guides/cli/shell-integration.md) |
| SDK integrator | [Blocking Rust SDK](../undercurrent-sdk/README.md), [asynchronous Python package](../../python/README.md), and [product protocol](../undercurrent-protocol/README.md) |
| Maintainer | [Implementation, contracts, and development guide](docs/README.md) |

Follow [installation](../../docs/getting-started/installation.md) to prepare
matching `uc`, `uc-daemon`, and `uc-session-host` binaries, then complete the
[first-session tutorial](../../docs/getting-started/first-session.md).
Some commands can start an
absent daemon; consult [startup rules](../../docs/guides/daemon/cli-lifecycle.md#start-and-verify-readiness)
when scripting.

The [system process view](../../docs/architecture/README.md#process-boundaries)
shows public SDK communication and the separate private session host. The
[workspace dependency map](../../docs/architecture/dependencies.md) shows which
crates may depend on each other. For this crate's local responsibilities, read
[CLI ownership](docs/internals/ownership.md).

The CLI owns argument translation, human/JSON presentation, bounded subscriptions,
terminal views, and guarded local lifecycle actions. Its exported Rust adapters
are internal orchestration seams; supported consumer APIs live in the SDK and
protocol crates. Relevant tests are mapped in the
[maintainer guide](docs/development/contributing.md#make-a-change-and-choose-verification).

[Undercurrent product overview](../../README.md)
