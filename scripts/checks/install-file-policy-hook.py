#!/usr/bin/env python3
"""Install only this clone's pre-commit file-policy hook, preserving other managers."""

import argparse
from pathlib import Path
import shlex
import subprocess
import sys

MARKER = "# Managed by install-file-policy-hook.py (repository file policy v1)"


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", type=Path, default=Path.cwd())
    args = parser.parse_args()
    try:
        root = Path(git(args.project, "rev-parse", "--show-toplevel"))
        git_dir = Path(git(root, "rev-parse", "--absolute-git-dir")).resolve()
        common = Path(git(root, "rev-parse", "--git-common-dir"))
        common = (
            (root / common).resolve() if not common.is_absolute() else common.resolve()
        )
        if git_dir != common:
            raise RuntimeError(
                "Linked worktrees share Git hooks; integrate through a per-worktree hook manager to keep installation limited to this clone"
            )
        existing = subprocess.run(
            [
                "git",
                "-C",
                str(root),
                "config",
                "--show-origin",
                "--get-all",
                "core.hooksPath",
            ],
            capture_output=True,
            text=True,
        )
        if existing.returncode not in {0, 1}:
            raise RuntimeError("Cannot inspect core.hooksPath")
        if existing.returncode == 0:
            raise RuntimeError(
                "core.hooksPath is already configured; integrate with that hook manager as described in REPOSITORY_FILES.md"
            )
        if any(
            (root / name).exists()
            for name in [
                ".pre-commit-config.yaml",
                "lefthook.yml",
                "lefthook.yaml",
                ".lefthook.yml",
                ".husky",
            ]
        ):
            raise RuntimeError(
                "An existing hook-manager configuration was preserved; integrate using REPOSITORY_FILES.md"
            )
        checker = root / "scripts/checks/check-repository-files.py"
        if not checker.is_file():
            raise RuntimeError("Missing scripts/checks/check-repository-files.py")
        hooks = Path(git(root, "rev-parse", "--git-path", "hooks"))
        if not hooks.is_absolute():
            hooks = root / hooks
        hooks.mkdir(parents=True, exist_ok=True)
        hook = hooks / "pre-commit"
        if hook.is_symlink() or (
            hook.exists() and MARKER.encode() not in hook.read_bytes()
        ):
            raise RuntimeError(
                "An existing pre-commit hook was preserved; integrate the check with it using REPOSITORY_FILES.md"
            )
        body = (
            "#!/bin/sh\n"
            + MARKER
            + "\n"
            + "exec "
            + shlex.quote(sys.executable)
            + " scripts/checks/check-repository-files.py\n"
        )
        hook.write_text(body)
        hook.chmod(hook.stat().st_mode | 0o111)
        print("Installed repository file policy in this clone's pre-commit hook")
        return 0
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"Hook installation stopped: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
