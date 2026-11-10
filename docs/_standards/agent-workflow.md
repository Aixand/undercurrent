# Perform and review documentation work

This is the canonical documentation agent contract for standard **0.1.0**.
Read the [standard](standard.md), [style guide](style-guide.md), and
[diagram guide](diagrams.md) before writing. Read [maintenance](maintenance.md)
when correcting contracts, moving pages, or updating a copied standard.

User-authorized scope and applicable project instructions control the work.
Do not launch agents, change application code, or adopt templates across projects
merely because this workflow describes those capabilities.

## Inspect before writing

1. Read applicable project instructions and existing documentation entry points.
2. Identify real project/package boundaries and the assigned file scope.
3. Inspect manifests, definitions, schemas, implementation, and meaningful tests.
4. Map reader tasks to existing owning pages; identify gaps and contradictions.
5. Correct useful existing material and add pages only for unmet reader needs.

Do not invent behavior, commands, defaults, support claims, releases, or check
results. Preserve exact terminology. Keep source links in technical pages and
record evidence in the report. Report conflicting contracts instead of silently
choosing an unsupported guarantee.

## Write within the assignment

Use the appropriate page template. Keep one reader outcome per page. Use Mermaid
for structural explanations and avoid repeating its arrows in prose. Provide
prerequisites, working directory, expected result, and relevant recovery for tasks.

Preserve useful paths, unrelated changes, and other workers' edits. Documentation
work does not authorize application changes to make an example pass. Report an
implementation defect and its reader impact under the existing mandate.

## Supervise a suite when delegation is authorized

A suite supervisor owns shared setup, project maps, and integration explanations.
Assign a worker to each actual child project with a self-contained task, exact
working directory, allowed paths, shared-standard location/version, and checks.
Do not create a worker for every ordinary folder or third-party dependency.

Give every file one editing owner. Child workers own local contracts and guides;
the supervisor reconciles cross-project claims and links. Review worker evidence
against source and the reader outcome before accepting the suite result.

## Verify and report

Run focused checks appropriate to the change. Render and visually inspect changed
diagrams. Execute examples in an appropriate isolated environment when authorized.
Do not treat syntax checks, a process launch, or another worker's assertion as
proof of an end-to-end result.

Report corrected claims, covered tasks, consolidated duplication, exact checks
and scope, inspected diagrams/themes, and unresolved gaps. Mark checks as passed,
failed, or unverified. Do not claim a project complete while required reader paths
or material claims remain unresolved.

## Load this contract from a project

Keep a short local `AGENTS.md` that explicitly points here and preserves the
project's other instructions. A shared sibling folder is not automatically in a
project's instruction chain. With copied standards, point to the local
`docs/_standards/` directory. With a shared checkout, verify the path is accessible.
Follow the repository's tracked-file policy; this template keeps local assistant
entry points ignored and the canonical contract in ordinary versioned docs.

[Required standard](standard.md) · [Source references](references.md)
