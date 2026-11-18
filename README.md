# Undercurrent

Run, observe and control terminal processes that outlive a client connection.
Undercurrent provides a local CLI, blocking Rust SDK, async Python SDK and MCP
adapter. The daemon and private process host own execution; applications decide
whether the work succeeded.

## Installation

Use [installation](docs/getting-started/installation.md) for published tools and
source builds. Match the CLI, daemon, session host and bridge versions.

### Initialize your shell

Follow [shell initialization](docs/getting-started/installation.md#initialize-your-shell)
for your chosen shell. Existing managed shells retain their loaded hooks.

## Quick start

With `uc` installed, run from your project directory in a POSIX shell. Choose an
unused session name:

```sh
uc start uc-readme-demo -- /bin/sh -c 'printf "session complete\n"'
uc wait uc-readme-demo --lifecycle exited --lifecycle failed --lifecycle stopped --timeout 15s
uc inspect uc-readme-demo
uc logs uc-readme-demo --max-bytes 4096
```

Check for exit code zero and `session complete`. After inspecting the result,
remove only this finished example record with `uc clean uc-readme-demo --yes`.
Windows and interactive workflows start in [your first session](docs/getting-started/README.md).

### Keep a session running

Follow [interactive terminals](docs/guides/cli/interactive.md) for attach/detach
and [session workflows](docs/guides/cli/sessions.md) for lifecycle and cleanup.

### Orchestrate two agents

Use the independently installable [agent skills](docs/agent/README.md). The
parent verifies task-specific results; an available prompt alone proves no completion.

## See it in action

[Real runtime workflows](docs/examples/runtime/README.md),
[crate exercises](docs/examples/crates/README.md) and
[recordings and fixtures](docs/assets/demos/README.md) show checked results.

## What you can do

Keep servers and shells running, inspect retained output, observe health and
interaction, deliver input and coordinate work using explicit application checks.
See the [workflow index](docs/getting-started/README.md#choose-a-workflow).

## Build with Undercurrent

Choose the [Rust or Python SDK](docs/sdk/README.md). Closing an SDK client leaves
managed sessions running. [Supported Rust contracts](crates/undercurrent-sdk/docs/reference/public-api.md)
are separate from workspace-internal APIs.

## Architecture

[Architecture](docs/architecture/README.md) explains process and state ownership.
The [development map](docs/development/workspace-guide.md) locates all 19 crates
and the separate Python package.

## Documentation

[Project site](https://projects.aixand.org/undercurrent/docs/) ·
[Source reading paths](docs/README.md) · [Examples](docs/examples/runtime/README.md) ·
[Changelog](CHANGELOG.md)

## AI-assisted development

We use AI tools and remain responsible for validation and attribution. Read the
[project disclosure](docs/project/about.md#ai-assisted-development).

## Issues and pull requests

[Participation](CONTRIBUTING.md) · [Support](SUPPORT.md) · [Security](SECURITY.md) ·
[Conduct](CODE_OF_CONDUCT.md) · [Apache-2.0](LICENSE)
