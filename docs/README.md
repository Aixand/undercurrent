# Find the right Undercurrent guide

These guides describe the `main` development branch for `0.1.0-beta`.
Published artifacts must be matched to their own release metadata.
Documentation standard: 0.1.0. Maintainers follow the
[shared standard](_standards/standard.md) and
[project documentation workflow](development/documentation.md).

The usage guides assume `uc` is installed and available in your terminal.
Start with [your first session](getting-started/first-session.md), or choose a
topic below. Setup instructions live in [installation](getting-started/installation.md).
[Compatibility](reference/compatibility.md) identifies package
availability and platform qualification.

Use the [local demos and fixtures](assets/demos/README.md) to explore recorded workflows.
Use the [standalone workflow index](getting-started/README.md#choose-a-workflow)
for persistent development servers, interactive processes, and SDK coordination.

For current availability, platform scope, and known limitations, see
[compatibility](reference/compatibility.md). Report unclear or incorrect
instructions through the [documentation issue form](https://github.com/Aixand/undercurrent/issues/new?template=02-documentation.yml).

| Section | Find here |
| --- | --- |
| [Getting started](getting-started/README.md) | Your first session, [session workflows](getting-started/README.md#choose-a-workflow), and [Undercurrent and tmux](getting-started/undercurrent-vs-tmux.md). |
| [Task guides](guides/README.md) | CLI workflows, configuration, daemon operation, storage recovery, and troubleshooting. |
| [SDKs](sdk/README.md) | [Rust](sdk/rust/README.md) and [Python](sdk/python/README.md) consumer guides, examples, and recovery. |
| [Agent skills](agent/README.md) | Installable skills for session control and subagent orchestration through `uc`. |
| [Architecture](architecture/README.md) | Processes, state ownership, crate dependencies, lifecycle, flows, and design decisions. |
| [Reference](reference/README.md) | Exact CLI, configuration, protocol, error, compatibility, and platform contracts. |
| [Development](development/README.md) | Source setup, extension points, verification, and release procedures. |

## Use and operate sessions

1. Run your first session on [Linux or macOS](getting-started/first-session.md)
   or [Windows](getting-started/first-session-windows.md).
2. [Control sessions](guides/cli/sessions.md) and [read status and output](guides/cli/status-and-output.md).
3. [Attach an interactive shell](guides/cli/interactive.md),
   [save and restore groups](guides/cli/groups.md), or choose another task from
   [operation and troubleshooting guides](guides/README.md).

## Integrate through an SDK

1. [Choose the supported boundary and language](sdk/README.md).
2. Follow [blocking Rust integration](sdk/rust/README.md) or [async Python integration](sdk/python/README.md).
3. Understand [lifecycle](architecture/lifecycle.md), [output](architecture/observations-and-output.md),
   and [recovery](sdk/recovery.md); use [reference](reference/README.md) for exact contracts.

## Orchestrate agents

Install the [agent skills](agent/README.md) to let your coding agent manage
sessions or delegate work to worker agents through the [local MCP server](agent/mcp.md)
or the CLI. Choose an explicit interface, or use automatic selection to prefer
compatible MCP for the selected home when configured.

1. Follow the [standalone workflow index](getting-started/README.md#choose-a-workflow)
   to select session control, direct worker orchestration, or SDK coordination.
2. Give each worker an explicit task and verify its reported artifacts; an input
   prompt or process exit alone does not establish workflow success.

## Explore the implementation

The development guides support maintainers and readers building their own
checkouts. We welcome issues, but are not accepting pull requests; see the
[participation policy](../CONTRIBUTING.md). Report vulnerabilities through the
[private security route](../SECURITY.md).

1. [Prepare development tools](development/setup.md).
2. Read [runtime architecture and state ownership](architecture/README.md)
   and the separate [crate dependency and code map](architecture/dependencies.md).
3. Trace [session creation, output, and recovery](architecture/flows.md).
4. Follow [development guidance](development/README.md) for changes and verification.

The code map links every first-party package. Consumer guides live in this
shared tree; package READMEs link local implementation details and maintained examples.

## Look up contracts and diagnose an installation

Use [reference](reference/README.md) for exact contracts,
[configuration diagnosis](guides/configuration/diagnose.md) for rejected settings,
and [storage recovery](guides/daemon/storage.md) for durable-state failures.

[Product overview](../README.md)
