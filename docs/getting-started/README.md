# Get started with sessions and workflows

These guides assume `uc` is installed and available in your terminal.
Start with a short command, inspect its output, then try interactive work.
These paths use standalone Undercurrent through its CLI, MCP server, or SDKs.

Choosing a tool first? [Compare Undercurrent and tmux](undercurrent-vs-tmux.md)
for terminal workflows, process observation, service health, and automation.

1. [Initialize your shell](installation.md#initialize-your-shell) with `uc init bash`,
   `uc init zsh`, `uc init fish`, or `uc init powershell --profile "$PROFILE"`,
   choosing the shell you will use. Open a new managed shell to load its hooks.
2. Run your first session on [Linux or macOS](first-session.md), or in
   [PowerShell](first-session-windows.md).
3. [Open an interactive shell](../guides/cli/interactive.md) and detach while it keeps running.
4. [Choose a workflow](#choose-a-workflow) below, or continue with
   [task guides](../guides/README.md) and [SDK integration](../sdk/README.md).

Need to install `uc`? See [installation](installation.md). Source builds belong
in [development setup](../development/setup.md); separately supplied archives
are covered by [bundle verification](bundles.md).

## Choose a workflow

| Goal | Guide | Start with |
| --- | --- | --- |
| Keep a process running and return to it | [Session control](../guides/cli/sessions.md) and [interactive terminals](../guides/cli/interactive.md). | The CLI and a foreground command or shell. |
| Run an authenticated agent in a session | [Agent skills and prerequisites](../agent/README.md). | A separately installed agent CLI and its normal authentication. |
| Coordinate independent agent tasks | [Orchestration skill](../agent/skills/undercurrent-orchestrate/SKILL.md). | An orchestrating agent, worker CLI, bounded tasks, and result checks. |
| Observe server readiness | [Detection profiles and health checks](../guides/configuration/profiles.md). | A running service with an explicit readiness signal. |
| Run jobs and a dependent report | [Python coordinator example](../assets/demos/fixtures/orchestrate.py) and [recording walkthrough](../assets/demos/README.md#coordinate-jobs). | The Python SDK, matching bridge, and a running daemon. |

## Run independent agent tasks

Install the [agent skills](../agent/README.md) and choose CLI, MCP, or automatic
interface selection. The orchestration skill launches workers directly as
Undercurrent sessions and records the task, workspace, session identity, and
verification needed for each result.

Give each worker one bounded task. Use separate worktrees for concurrent edits;
read-only reviews can share a checkout. Workers need explicit context because
they do not inherit their parent's conversation. Inspect the reported files and
run the appropriate checks before accepting their work.

An observed input prompt indicates an interaction surface. It does not prove
the requested task succeeded. Preserve the session and evidence if a timeout
leaves the outcome uncertain; use the [recovery guide](../guides/troubleshooting.md)
before repeating a mutation. Stop and clean only sessions created for the task.

## Automate through an SDK

Follow the [Python input round trip](../sdk/python/quickstart.md) or
[Rust reading path](../sdk/rust/README.md). Both connect to an external daemon.
Your coordinator owns workflow ordering, application checkpoints, and result
validation. Undercurrent supplies session state, input/output, and observations.

[Documentation index](../README.md)
