"""Regenerate the portable MCP references from docs/agent/shared/mcp-control.md.

Run from the repository root after editing the shared Markdown or skill-specific
additions below. Keep the two insertion markers in the shared Markdown.
Pass --check to detect missing or stale copies without writing.
"""

import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE = Path("docs/agent/shared/mcp-control.md")
SKILLS = Path("docs/agent/skills")
LAUNCH_MARKER = "<!-- skill-launch -->"
CLEANUP_MARKER = "<!-- skill-cleanup -->"
SERVER_EXAMPLE = """

For a project with an npm `dev` script, a start's arguments could be:

```json
{
  "name": "UNIQUE_SERVER_NAME",
  "program": "npm",
  "arguments": ["run", "dev"],
  "cwd": "/absolute/project"
}
```

Replace all example names, paths, and programs with the actual task's values.
Verify readiness from this application's output or real endpoint. MCP start has
no custom health-rule argument; use observed configured health only when present,
or check readiness with available application tools. Do not invent CLI flags as
tool arguments. Keep a requested persistent server running after verification."""
WORKER_LINKS = """ Read [MCP worker launches](launching-workers-mcp.md)
for finite and interactive examples after [preparing the task](launching-workers.md)."""
STOPPED_RECORDS = """ Stopped records may remain in MCP mode after
their outputs and worker results have been preserved."""


def render(common: str) -> dict[Path, str]:
    for marker in (LAUNCH_MARKER, CLEANUP_MARKER):
        if common.count(marker) != 1:
            raise ValueError(f"Expected exactly one insertion marker {marker}")

    return {
        SKILLS / "undercurrent/references/mcp-control.md": (
            common.replace(LAUNCH_MARKER, SERVER_EXAMPLE).replace(CLEANUP_MARKER, "")
        ),
        SKILLS / "undercurrent-orchestrate/references/mcp-control.md": (
            common.replace(LAUNCH_MARKER, WORKER_LINKS).replace(CLEANUP_MARKER, STOPPED_RECORDS)
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Fail if a generated copy differs.")
    args = parser.parse_args()

    try:
        references = render((ROOT / SOURCE).read_bytes().decode("utf-8"))
    except (OSError, ValueError) as error:
        parser.exit(1, f"{SOURCE}: {error}\n")

    stale = []
    for relative, content in references.items():
        path = ROOT / relative
        expected = content.encode("utf-8")
        if args.check:
            if not path.is_file() or path.read_bytes() != expected:
                stale.append(relative)
        else:
            path.write_bytes(expected)

    if stale:
        for path in stale:
            print(f"Missing or stale generated reference: {path}")
        print("Run python3 scripts/docs/generate_skill_references.py to regenerate.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
