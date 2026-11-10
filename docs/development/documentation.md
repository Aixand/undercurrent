# Write and verify Undercurrent documentation

Use this guide when editing user, SDK, architecture, or maintainer documentation.
Run commands from the repository root with Python 3.11 or newer.
The documentation describes `main` at `0.1.0-beta`; a published release has its
own artifact metadata and qualification evidence.

## Follow the shared standard

Undercurrent adopts documentation standard **0.1.0** from the documentation
template. The copied [standard](../_standards/standard.md),
[style guide](../_standards/style-guide.md), [diagram guide](../_standards/diagrams.md),
and [maintenance rules](../_standards/maintenance.md) govern documentation work.
The [agent workflow](../_standards/agent-workflow.md) is the canonical assistant
contract; local entry points follow the [repository file policy](../../REPOSITORY_FILES.md).

Keep the existing reader paths. Product tutorials, operation guides, SDK usage,
architecture, and reference belong under `docs/`. Crate READMEs and local docs
own implementation details. The [code map](../architecture/dependencies.md)
locates those owners. Preserve useful content when adopting a template update.

Establish commands and claims from the owning source and meaningful tests.
Distinguish a source build, a supplied artifact, a published download, and a
planned hosting route. Record discrepancies in the
[compatibility reference](../reference/compatibility.md) instead of promising
behavior that the implementation does not establish.

## Describe processes and sessions

Describe core behavior in terms of processes and managed sessions. A session is
Undercurrent's managed object; a process is the program running within it.

Use agent terminology in guides specifically about agents or clearly identified
agent examples. Keep concrete names such as Codex in those examples. Preserve
exact command names, API fields, protocol terms, and output labels.

## Run offline checks

```sh
python3 tests/tooling/checks/docs_test.py
python3 scripts/checks/check-docs.py --root .
git diff --check
```

Each command should exit zero. The [configuration](../../docs-checks.json)
covers community files, shared guides, crate documentation, Python docs, and
test/tooling indexes. Correct reported paths, heading anchors, fence languages,
heading structure, unresolved tokens, and missing diagram descriptions.
The checker does not fetch hosted URLs, inspect raw HTML links, run examples,
or prove technical accuracy. Review those separately.

The same offline checks run in the **Documentation checks** CI job.
`./dev.sh check` and `python3 scripts/validate.py --stage documentation` also
run documentation validation. The tooling test runner discovers the checker's
regression suite automatically.

## Verify procedures and diagrams

Run changed examples in isolated runtime homes and preserve their failure
evidence. See [verification](verification.md) for selecting product tests.
Installation examples use the [product guide](../getting-started/product-installer.md)
and [installer qualification](installer-validation.md).

Render changed Mermaid blocks with Mermaid CLI 12, inspect both default and
dark themes, and preview them in the publishing renderer.
For a checker extraction without rendering, choose a new output directory:

```sh
python3 scripts/checks/check-docs.py --root . --extract-mermaid build/docs-mermaid
```

For local rendering, install Node 22.13+ and npm. From the repository root,
install the pinned renderer into the ignored `build/` directory:

```sh
npm install --prefix build/docs-tools --save-exact --ignore-scripts @mermaid-js/mermaid-cli@12.2.0
npm exec --prefix build/docs-tools -- puppeteer browsers install chrome
python3 scripts/checks/check-docs.py --root . \
  --render-mermaid build/docs-diagrams --mmdc build/docs-tools/node_modules/.bin/mmdc
```

On Windows, use `build/docs-tools/node_modules/.bin/mmdc.cmd`. The first two
commands need network access unless their packages and browser are cached.
Browser installation is explicit, so it also works when npm blocks dependency
install scripts. Keep the generated npm lockfile with your local tools. Use
`npm ci --prefix build/docs-tools --ignore-scripts` to recreate the same packages,
then repeat the browser installation command.

Use an unused output directory. Rendering needs Chromium and any required native
browser libraries. Keep previews under ignored `build/`.
Use `--puppeteer-config` for an explicit browser configuration when necessary.
The checker does not disable the browser sandbox automatically.

## Report the result

Describe corrected claims and affected reader tasks. Record exact checks,
executed examples, inspected diagram themes, and remaining gaps. A link-check
pass alone does not establish a successful installation or supported platform.

[Documentation index](../README.md) · [Development guides](README.md)
