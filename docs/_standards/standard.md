# Write documentation that helps a reader succeed

Documentation standard: **0.1.0**. This standard applies to user, operator,
integrator, and contributor documentation. Required rules use **must**;
recommended practices use **should**. Explain material exceptions in the review.

## Start with the reader's need

A page must identify one main reader outcome. Put its purpose and relevant scope
at the start. State platform, version, or development-branch scope when these
change the instructions or meaning.

| Reader need | Page type | Required content |
| --- | --- | --- |
| Learn through a first successful example | Tutorial / getting started | Prerequisites, one working path, visible result, verification, cleanup |
| Complete a known task | Task guide | When to use it, setup, actions, result, relevant recovery |
| Look up an exact contract | Reference | Definitions, inputs, defaults, units, constraints, outputs, errors as applicable |
| Understand responsibilities or reasons | Architecture / concept | Scope, appropriate visual, responsibilities, constraints, rationale, owning sources |
| Diagnose or restore operation | Troubleshooting / runbook | Symptom, checks, interpretation, corrective action, verification |
| Change or verify the implementation | Development | Setup, ownership, extension points, focused checks |
| Preserve a design decision | Decision record | Status, context, decision, consequences, superseding decision if applicable |

Do not mix a long design explanation into a procedure. Link to the owning
explanation or reference. Split a page when it serves independent reader needs.
A page has no minimum word count or diagram quota.

## Choose the smallest useful structure

Every project must offer a clear entry point. Its README must explain its
purpose, show an appropriate first use or integration path, and link to deeper
material. A multi-page doc set must have a navigable index.

Create sections only when they contain useful content. Keep effective existing
paths. For larger projects, use `getting-started/`, `guides/`, `reference/`,
`architecture/`, and `development/`. Add SDK and operational pages as needed.

A suite must map its real child projects and explain shared setup and integration.
Child projects own their contracts and implementation guides. A package can have
a concise README and local contributor docs without a complete product manual.

## Ground technical claims

Use definitions, schemas, registries, implementation, and meaningful tests to
establish what the current system does. Link to authoritative source locations
from technical reference and architecture pages. Record intended behavior that
conflicts with implementation as a discrepancy; do not silently redefine it.

A development build, published release, and planned feature must remain distinct.
Do not invent commands, default values, support guarantees, releases, or test
results. Define units, precedence, timeouts, limits, and failure effects where they
affect the reader. Distinguish acceptance, readiness, and successful completion.

A source link helps a reviewer verify a claim. Routine users should still have
enough information in the page to complete their task.

## Make procedures reproducible

Commands must identify their working directory, prerequisites, shell or platform
when relevant, and the values that readers replace. Use realistic sanitized
examples. Identify incomplete excerpts and link to runnable source when available.

Describe observable success and the next diagnostic action for likely failure.
Explain cleanup when examples create resources. Explain destructive effects
before the relevant action. Keep necessary checks close to the action they verify.

## Use visuals and concise language

Follow the [style guide](style-guide.md) and [diagram rules](diagrams.md).
Use diagrams for relationships, interactions, and transitions. Use tables for
exact fields and comparisons; use code for executable actions.

Prose beside a diagram must add constraints, rationale, exceptions, or operating
guidance. Do not restate every arrow. Supply accessible descriptions of essential
relationships and readable alternatives for nonvisual readers.

## Accept a documentation change

A completed change must satisfy these checks:

1. Required reader paths are covered and reachable from an entry point.
2. Claims agree with owning definitions or identified discrepancies.
3. Commands and examples have relevant verification evidence.
4. Local links, anchors, and referenced paths resolve.
5. Changed diagrams render and have been visually inspected.
6. Terminology is consistent and redundant explanation is removed.
7. The report separates passed, failed, and unverified checks with their scope.

Formatting checks cannot prove behavior. An unresolved material claim must remain
recorded as a gap. A project must not be reported as fully documented while such
required coverage is missing.

[Maintain the standard](maintenance.md) · [Apply the agent workflow](agent-workflow.md)
