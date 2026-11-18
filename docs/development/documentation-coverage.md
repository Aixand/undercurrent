# Keep every document accounted for

Each substantive first-party document has a project-site page. A retained
repository entrypoint links to its published owner. Dependency records remain
available through the published dependency hub and byte-preserving downloads.

## Choose one role

| Role | Required evidence |
| --- | --- |
| Published guide or reference | An exact source entry in a publication manifest |
| Repository entrypoint | A concise summary, published destination and direct owner link |
| Source redirect | A short stable pointer to a published canonical owner |
| Dependency record | An explicit inventory entry, published hub and copied original attachment |

The [coverage inventory](../coverage.json) lists every explicit exception by file.
There is no wildcard exemption for a folder. Generated skill reference views are
published too; generation does not make their behavior invisible.

## Run the checks

From the repository root with Python 3.11+:

```sh
python3 scripts/checks/check-doc-coverage.py --root .
python3 scripts/docs/generate_skill_references.py --check
python3 scripts/docs/package_skills.py --check
python3 scripts/checks/check-docs.py --root .
```

Each command must exit zero. Coverage discovers tracked files and unignored new
Markdown, validates publication sources, rejects stale classifications and checks
that entrypoints remain short and link to published owners. Source archives use
a directory scan excluding build outputs when Git metadata is absent.

A new substantive page belongs in its owning publication manifest. If a README
grows beyond its declared summary budget, move the explanation into the owning
guide and retain a link. An output labeled dependency must preserve an original
attachment; it cannot hide an ordinary first-party guide.

## Update generated portable files

Edit the [shared MCP source](../agent/shared/mcp-control.md) and any skill-specific
additions in its generator, then run:

```sh
python3 scripts/docs/generate_skill_references.py
python3 scripts/docs/package_skills.py
```

Both skills retain their local references and can be installed independently.
The archives include the root license and use fixed names, ordering and timestamps.
Check mode never writes. Review the exact diff and run the documentation tests
before importing a new pinned revision into the site.

The [documentation workflow](documentation.md) owns authoring and visual checks;
[publication](../publication.md) owns source hashes, routes and asset imports.
