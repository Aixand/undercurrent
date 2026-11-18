# Publish source-owned Undercurrent documentation

General owns setup, usage, consumer SDKs and cross-crate explanations. Each of
the 19 crate doc trees owns its implementation and verification path. Python has
a separate maintainer category because the SDK package and Rust stdio bridge
have distinct owners. All substantive first-party guides, decisions, release procedures and policies
have project-site views. Conventional README and policy locations remain
available for repository and packaging tools. The explicit coverage inventory
records short entrypoints, source redirects and original dependency attachments.

## Select a reading path

| Owner | Manifest |
| --- | --- |
| General | `docs/publication.json` |
| Each Rust crate | `crates/undercurrent-NAME/docs/publication.json` |
| Python SDK | `python/docs/publication.json` |

The Rust SDK uses site category `rust-sdk` to preserve existing General `sdk/`
URLs. All other crate categories use their directory suffix. General has no
`component` field. A manifest selects exact pages and declares recording assets.
The site imports content and assets from one full source commit and records hashes.

## Check before committing

From this checkout with the pinned dependencies available:

```sh
python3 tests/tooling/checks/docs_test.py
python3 scripts/checks/check-docs.py --root .
python3 scripts/docs/check_example_snippets.py
python3 scripts/docs/verify_crate_examples.py \
  --target-dir /tmp/uc-doc-target --evidence-dir /tmp/uc-crate-evidence
```

Use fresh evidence directories. The Rust exercises use internal workspace APIs;
their successful execution does not widen supported external dependencies.
Execute the [runtime workflows](examples/runtime/README.md) with matching native
tools and an isolated SDK environment. Check actual results and cleanup.
Render changed Mermaid in light and dark themes and inspect the publishing site.
Native macOS and Windows behavior requires its own target evidence.

## Record actual execution

Existing CLI and parallel-job tapes use `scripts/docs/render_demos.py`. The
[source demo guide](assets/demos/README.md) owns its prerequisites and cleanup.
Site-ready MP4s and their effective tapes/capture records live in
`docs/examples/recordings/`. The runtime SDK recorder is
`docs/examples/runtime/recordings/record.py --runtime-bin /absolute/path/to/bin`.
It needs VHS, ttyd, ffmpeg with libopenh264, ffprobe, Chromium and Noto Sans Mono.

Keep captured output real. Capture records identify executed source dependencies,
media dimensions and verification scope. Do not publish credentials, runtime
stores, private process logs or arbitrary session dumps. Root historical GIFs
remain source examples; refreshed site MP4s have their own capture records.

## Import one exact revision

After committing, run from the sibling project-site checkout:

```sh
npm run docs:sync -- --source ../undercurrent \
  --repository https://github.com/Aixand/undercurrent \
  --manifest docs/publication.json --revision SOURCE_SHA --output .
```

Repeat for all crate manifests and Python using the same 40-character commit.
Run the site build, component/browser tests, recording playback and local hosting
checks. Push the source revision before public deployment so pinned source links
resolve. Do not edit generated site snapshots directly.

## Keep one authored owner

General's manifest uses `sourceRoot: "repository"`, allowing root policies and
source notices to publish directly without authored copies under `docs/`. Skill
entrypoints use `format: "agent-skill"`: publication removes their packaging
metadata, preserves the instruction body and hashes the complete original file.
Both skill folders remain independently installable. Shared MCP text is authored
once; generated portable copies and fixed-layout ZIP downloads have drift checks.

The coverage gate checks all tracked and unignored Markdown. Every substantive
first-party source is selected by a publication manifest. Short entrypoints and
legacy source redirects must link to a published owner; dependency records must
have an imported original attachment and a published dependency hub. No wildcard
folder exclusion hides first-party explanations. The documentation CI job and
local validation stage run coverage and generation checks.

Source links to moved guides point directly at canonical owners. Compatibility
files retain old source paths; existing site slugs remain stable. Original
licenses and provenance files keep their packaging paths and bytes.
