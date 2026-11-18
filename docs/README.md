# Find the right Undercurrent documentation

These guides describe the current development branch at `0.1.0-beta`. Match
published artifacts to their own release metadata. Documentation standard: 0.1.0.
The project site imports the same maintained sources; no substantive first-party
guide is intentionally hidden from publication.

## Choose a reader task

| Task | Start here |
| --- | --- |
| Install and run processes | [Getting started](getting-started/README.md) |
| Operate and diagnose sessions | [Task guides](guides/README.md) |
| Integrate from Rust or Python | [SDK guides](sdk/README.md) |
| Understand decisions and boundaries | [Architecture](architecture/README.md) and [design decisions](architecture/decisions/README.md) |
| Develop a crate or SDK | [Workspace map](development/workspace-guide.md) |
| Verify, package or maintain | [Development](development/README.md) |
| Control processes with an agent | [Agent skills and workflows](agent/README.md) |
| Read exact contracts | [Reference](reference/README.md) |
| Find runnable examples | [Runtime workflows](examples/runtime/README.md) and [crate mechanisms](examples/crates/README.md) |
| Find policies, support and notices | [Project](project/README.md) |

## Follow the folder owners

```text
docs/
  getting-started/       installation and first use
  guides/                operating tasks and diagnosis
  sdk/                   consumer Rust and Python guides
  architecture/          cross-crate behavior and decisions/
  development/           setup, verification, distribution and maintenance
  agent/                 workflows, shared instructions and portable skills/
  project/               project information, policies and dependencies
  examples/              runnable workflows and crate exercises
  assets/                recording inputs, media and branding
  _standards/            versioned documentation authoring contract
```

Crate implementation guides live in `crates/undercurrent-*/docs/`; Python's
implementation guides live in `python/docs/`. Their READMEs are short repository
entry points to those published owners. The [publication guide](publication.md)
and [coverage inventory](coverage.json) identify every retained document's role.

[Product overview](../README.md) · [Support](../SUPPORT.md)
