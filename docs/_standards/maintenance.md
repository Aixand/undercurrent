# Keep documentation aligned with behavior

Update affected documentation with the change that alters behavior. Review the
reader outcome again when installation, CLI options, schemas, APIs, defaults,
platform support, or operational recovery changes.

## Give each subject an owner

Each subject should have one owning page. Link to its contract rather than
maintaining competing copies. Keep generated symbol inventories with their code
or schema source. Keep explanation and workflows in authored documentation.

For suites, keep a small ownership map listing real project paths, documentation
entry points, integration owners, and verification commands. Discover project
boundaries from manifests and responsibilities. A source directory is not
necessarily an independent project.

Keep project-specific terminology and exceptions outside the copied standard.
Record the adopted standard version in the documentation index. Upgrade copied
standards deliberately; changes in a shared checkout do not update them.

## Declare the version scope

State whether a doc set describes a released version or the development branch.
Call out pages with a narrower platform or version scope. Keep planned behavior
clearly identified. Archive obsolete guidance only when it remains useful for a
supported older version; otherwise remove it and repair incoming links.

Do not change a review date without reviewing the content. Evidence should name
the source revision or checkout scope, commands executed, target platform, and
outcomes. Keep transient logs and exhaustive execution records in review reports
or build artifacts, rather than in every user-facing page.

## Resolve conflicting evidence

A schema, implementation, test, and existing guide can disagree. Identify each
source and the affected reader claim. Describe observed behavior and its scope;
record the unresolved contract conflict. Do not silently turn a bug into a
promised interface or present an intended fix as implemented.

## Review a documentation update

| Change | Review |
| --- | --- |
| Page move or removal | Incoming links, anchors, index, published redirects if relevant |
| Command or field change | Owning definition, examples, reference, affected guides |
| Architecture change | Boundaries, diagrams, local contributor docs, decisions |
| Platform or release change | Available artifacts, prerequisites, tested target and version |
| Example change | Runnable source, expected result, cleanup, derived excerpts |
| Shared standard change | Version, changelog, templates, examples, regression checks |

Existing project review cadence can include documentation. This template creates
no schedules, monitors, repository registrations, or automatic project adoption.

[Acceptance rules](standard.md#accept-a-documentation-change) · [Agent workflow](agent-workflow.md)
