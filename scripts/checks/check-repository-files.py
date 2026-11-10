#!/usr/bin/env python3
"""Reject coding-assistant repository instructions in Git's proposed file tree."""

import argparse
import fnmatch
from pathlib import Path
import re
import subprocess
import sys

# Case-insensitive path policy. Do not ban product assets such as SKILL.md,
# ordinary prompt packs, application code, or documentation discussing AI.
FILE_PATTERNS = (
    "agent.md",
    "agents.md",
    "agents.*.md",
    "agents.md.*",
    "agent_instructions.md",
    "agent-instructions.md",
    "claude.md",
    "claude.*.md",
    "claude.md.*",
    "codex.md",
    "codex.*.md",
    "codex.md.*",
    "gemini.md",
    "gemini.*.md",
    "gemini.md.*",
    "qwen.md",
    "amp.md",
    "warp.md",
    "crush.md",
    "opencode.md",
    "ai.md",
    "ai_context.md",
    "ai-context.md",
    "ai-instructions.md",
    "copilot-instructions.md",
    "copilot-instructions.md.*",
    ".cursorrules",
    ".cursorignore",
    ".cursorindexingignore",
    ".windsurfrules",
    ".clinerules",
    ".clinerules.*",
    ".roorules",
    ".roorules-*",
    ".copilotignore",
    ".goosehints",
    ".gooseignore",
    ".aider*",
    "opencode.json",
    "opencode.jsonc",
    "opencode.local.json",
    "opencode.local.jsonc",
)
DIRECTORY_NAMES = {
    ".agent",
    ".agents",
    ".claude",
    ".codex",
    ".cursor",
    ".windsurf",
    ".cline",
    ".clinerules",
    ".roo",
    ".roorules",
    ".continue",
    ".gemini",
    ".qwen",
    ".opencode",
    ".junie",
    ".aider",
    ".goose",
}
GITHUB_DIRECTORIES = {"agents", "instructions", "prompts"}


def forbidden(path):
    parts = path.casefold().split("/")
    if any(part in DIRECTORY_NAMES for part in parts):
        return True
    if any(fnmatch.fnmatchcase(parts[-1], pattern) for pattern in FILE_PATTERNS):
        return True
    return any(
        first == ".github" and second in GITHUB_DIRECTORIES
        for first, second in zip(parts, parts[1:])
    )


def git(root, *args):
    result = subprocess.run(
        ["git", "-C", str(root), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.decode(errors="replace").strip())
    return result.stdout


def commit(root, revision):
    value = git(
        root, "rev-parse", "--verify", "--end-of-options", revision + "^{commit}"
    )
    value = value.decode("ascii").strip()
    if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", value):
        raise RuntimeError("Git returned an invalid commit identity")
    return value


def paths(root, revision=None):
    data = (
        git(root, "ls-files", "--cached", "-z")
        if revision is None
        else git(root, "ls-tree", "-r", "--name-only", "-z", revision)
    )
    return sorted(
        {p.decode("utf-8", errors="surrogateescape") for p in data.split(b"\0") if p}
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", type=Path, default=Path.cwd())
    parser.add_argument("--base", help="Check every commit introduced after this base")
    parser.add_argument("--head", help="Head revision, used together with --base")
    parser.add_argument(
        "--all-history", action="store_true", help="Check every commit reachable from HEAD"
    )
    args = parser.parse_args(argv)
    if args.all_history and (args.base is not None or args.head is not None):
        parser.error("--all-history cannot be combined with --base or --head")
    if (args.base is None) != (args.head is None):
        parser.error("--base and --head must be supplied together")
    try:
        root = Path(git(args.project, "rev-parse", "--show-toplevel").decode().strip())
        if args.all_history:
            head = commit(root, "HEAD")
            revisions = git(root, "rev-list", "--reverse", head, "--").decode().splitlines()
        elif args.base is not None:
            base, head = commit(root, args.base), commit(root, args.head)
            revisions = (
                git(root, "rev-list", "--reverse", base + ".." + head, "--")
                .decode()
                .splitlines()
            )
            # Also check the head when the range is empty or histories diverge.
            revisions = list(dict.fromkeys([*revisions, head]))
        else:
            revisions = [None]
        violations = []
        for revision in revisions:
            violations.extend(
                (revision, p) for p in paths(root, revision) if forbidden(p)
            )
        if violations:
            print(
                "Coding-assistant instruction/configuration files cannot be committed:",
                file=sys.stderr,
            )
            for revision, path in violations:
                label = revision[:12] if revision else "index"
                print(f"  {label}: {path!r}", file=sys.stderr)
            print(
                "Remove these paths from version control. See REPOSITORY_FILES.md.",
                file=sys.stderr,
            )
            return 1
        print("Repository file policy passed")
        return 0
    except (OSError, RuntimeError) as error:
        print(f"Repository file policy could not run: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
