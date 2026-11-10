#!/usr/bin/env python3
"""Exercise the file policy against real indexes, commits, ranges, and hooks."""

import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

SOURCE = Path(__file__).resolve().parents[3]
CHECKER = SOURCE / "scripts/checks/check-repository-files.py"
INSTALLER = SOURCE / "scripts/checks/install-file-policy-hook.py"
spec = importlib.util.spec_from_file_location("repository_file_policy", CHECKER)
policy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(policy)


class PolicyNamesTests(unittest.TestCase):
    def test_recognized_nested_case_and_assistant_configurations(self):
        for path in [
            "AGENTS.md",
            "docs/aGeNtS.Md",
            "nested/AGENTS.override.md",
            "CLAUDE.local.md",
            "doc/Claude.md.example",
            "CODEX.md",
            "GEMINI.md",
            "QWEN.md",
            "AMP.md",
            "WARP.md",
            "CRUSH.md",
            "OPENCODE.md",
            "AI_CONTEXT.md",
            "agent-instructions.md",
            "opencode.jsonc",
            ".cursor/rules/api.mdc",
            "nested/.Claude/settings.json",
            "x/.codex/config.toml",
            ".continue/config.yaml",
            ".junie/guidelines.md",
            ".cursorrules",
            ".windsurfrules",
            ".clinerules/01.md",
            ".roorules-code",
            ".aider.conf.yml",
            ".goosehints",
            ".github/copilot-instructions.md",
            ".github/instructions/python.instructions.md",
            ".github/agents/review.agent.md",
            "nested/.GITHUB/prompts/review.prompt.md",
        ]:
            with self.subTest(path=path):
                self.assertTrue(policy.forbidden(path), path)

    def test_product_skills_prompts_source_and_general_docs_are_allowed(self):
        for path in [
            "example-app/examples/csv-audit/SKILL.md",
            "packs/agents/pack.json",
            "example_app/core.py",
            "docs/agents-overview.md",
            "docs/ai-policy.md",
            "REPOSITORY_FILES.md",
            ".github/workflows/ci.yml",
            "docs/space name.md",
        ]:
            with self.subTest(path=path):
                self.assertFalse(policy.forbidden(path), path)


class PolicyGitTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="repository-policy-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = dict(os.environ)
        for name in list(self.env):
            if name.startswith("GIT_"):
                self.env.pop(name)
        self.env.update(
            {
                "GIT_CONFIG_GLOBAL": os.devnull,
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_AUTHOR_NAME": "Policy test fixture",
                "GIT_COMMITTER_NAME": "Policy test fixture",
                "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
                "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
                "PYTHONDONTWRITEBYTECODE": "1",
            }
        )
        self.git("init", "-q", "-b", "main")
        target = self.root / "scripts/checks"
        target.mkdir(parents=True)
        shutil.copy2(CHECKER, target / CHECKER.name)
        shutil.copy2(INSTALLER, target / INSTALLER.name)
        self.write("README.md", "Fixture\n")
        self.git("add", ".")
        self.git("-c", "commit.gpgsign=false", "commit", "-qm", "test: seed fixture")
        self.base = self.git("rev-parse", "HEAD").stdout.strip()

    def git(self, *argv, ok=True):
        result = subprocess.run(
            ["git", *argv],
            cwd=self.root,
            env=self.env,
            text=True,
            capture_output=True,
            timeout=20,
        )
        if ok:
            self.assertEqual(result.returncode, 0, result.stderr)
        return result

    def write(self, name, content="fixture\n"):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        return path

    def check(self, *args, ok=True):
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                str(self.root / "scripts/checks" / CHECKER.name),
                *args,
            ],
            cwd=self.root,
            env=self.env,
            text=True,
            capture_output=True,
            timeout=20,
        )
        self.assertEqual(result.returncode, 0 if ok else 1, result.stderr)
        return result

    def install(self, ok=True, project=None):
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                str(self.root / "scripts/checks" / INSTALLER.name),
                "--project",
                str(project or self.root),
            ],
            cwd=self.root,
            env=self.env,
            text=True,
            capture_output=True,
            timeout=20,
        )
        self.assertEqual(result.returncode, 0 if ok else 1, result.stderr)
        return result

    def test_clean_index_and_ignored_untracked_personal_file(self):
        self.write(".gitignore", "AGENTS.md\n")
        self.write("AGENTS.md")
        self.git("add", ".gitignore")
        self.check()
        self.git("add", "-f", "AGENTS.md")
        self.assertIn("AGENTS.md", self.check(ok=False).stderr)

    def test_all_history_checks_rewritten_root_without_a_previous_base(self):
        self.git("checkout", "--orphan", "rewritten")
        self.write("README.md", "Replacement history\n")
        self.git("add", ".")
        self.git("-c", "commit.gpgsign=false", "commit", "-qm", "test: replacement root")
        self.check("--all-history")

    def test_all_history_rejects_a_forbidden_file_removed_before_head(self):
        self.write("AGENTS.md")
        self.git("add", "AGENTS.md")
        self.git("-c", "commit.gpgsign=false", "commit", "-qm", "test: forbidden history")
        self.git("rm", "AGENTS.md")
        self.git("-c", "commit.gpgsign=false", "commit", "-qm", "test: clean head")
        self.check()
        self.assertIn("AGENTS.md", self.check("--all-history", ok=False).stderr)

    def test_index_catches_deleted_worktree_file_and_all_preexisting_entries(self):
        path = self.write("nested/aGeNtS.Md")
        self.git("add", ".")
        path.unlink()
        self.assertIn("nested/aGeNtS.Md", self.check(ok=False).stderr)
        self.git("rm", "--cached", "nested/aGeNtS.Md")
        self.check()

    @unittest.skipIf(os.name == "nt", "symlink setup needs native permissions")
    def test_symlinks_are_checked_by_their_tracked_names(self):
        (self.root / "CLAUDE.md").symlink_to("README.md")
        self.git("add", "CLAUDE.md")
        self.check(ok=False)

    def test_staged_legacy_removal_and_first_compliant_commit(self):
        self.write("AGENTS.md")
        self.git("add", "AGENTS.md")
        self.git("-c", "commit.gpgsign=false", "commit", "-qm", "test: legacy file")
        legacy = self.git("rev-parse", "HEAD").stdout.strip()
        self.install()
        self.git("rm", "AGENTS.md")
        self.git(
            "-c", "commit.gpgsign=false", "commit", "-qm", "test: remove legacy file"
        )
        self.check("--base", legacy, "--head", "HEAD")

    def test_actual_commit_hook_rejects_forced_addition_and_allows_retry(self):
        self.install()
        self.install()  # Managed installation is idempotent.
        self.write(".gitignore", "CLAUDE.md\n")
        self.write("CLAUDE.md")
        self.git("add", ".gitignore")
        self.git("add", "-f", "CLAUDE.md")
        failed = self.git(
            "-c", "commit.gpgsign=false", "commit", "-qm", "test: reject", ok=False
        )
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("CLAUDE.md", failed.stderr)
        self.assertEqual(self.git("rev-parse", "HEAD").stdout.strip(), self.base)
        self.git("rm", "--cached", "CLAUDE.md")
        self.git(
            "-c", "commit.gpgsign=false", "commit", "-qm", "test: retry clean input"
        )
        self.check()

    def test_partial_commit_uses_gits_proposed_index(self):
        self.install()
        self.write("AGENTS.md")
        self.git("add", "AGENTS.md")
        self.write("README.md", "changed\n")
        self.git(
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--only",
            "README.md",
            "-qm",
            "test: partial commit",
        )
        self.assertNotIn(
            "AGENTS.md", self.git("ls-tree", "-r", "--name-only", "HEAD").stdout
        )
        self.check(ok=False)  # The other staged file remains for the next commit.

    def test_range_catches_added_then_deleted_intermediate_file(self):
        self.write("CLAUDE.md")
        self.git("add", "CLAUDE.md")
        self.git(
            "-c", "commit.gpgsign=false", "commit", "-qm", "test: bypassed local hook"
        )
        bad = self.git("rev-parse", "HEAD").stdout.strip()
        self.git("rm", "CLAUDE.md")
        self.git("-c", "commit.gpgsign=false", "commit", "-qm", "test: remove file")
        self.check()
        failure = self.check("--base", self.base, "--head", "HEAD", ok=False)
        self.assertIn(bad[:12], failure.stderr)
        self.assertIn("CLAUDE.md", failure.stderr)

    def test_empty_range_still_checks_the_head(self):
        self.write("AGENTS.md")
        self.git("add", "AGENTS.md")
        self.git("-c", "commit.gpgsign=false", "commit", "-qm", "test: bad head")
        self.check("--base", "HEAD", "--head", "HEAD", ok=False)

    def test_existing_hook_manager_and_custom_hook_are_preserved(self):
        self.git("config", "core.hooksPath", ".custom-hooks")
        self.assertIn("core.hooksPath", self.install(ok=False).stderr)
        self.assertEqual(
            self.git("config", "core.hooksPath").stdout.strip(), ".custom-hooks"
        )
        self.git("config", "--unset", "core.hooksPath")
        hook = self.root / ".git/hooks/pre-commit"
        hook.write_bytes(b"#!/bin/sh\n# existing\xff\nexit 0\n")
        before = hook.read_bytes()
        self.install(ok=False)
        self.assertEqual(hook.read_bytes(), before)
        hook.unlink()
        self.write(".pre-commit-config.yaml", "repos: []\n")
        self.install(ok=False)
        self.assertFalse(hook.exists())

    def test_linked_worktree_does_not_modify_shared_hooks(self):
        worktree = self.root / "linked-tree"
        self.git("worktree", "add", "--detach", str(worktree), "HEAD")
        self.assertIn(
            "Linked worktrees", self.install(ok=False, project=worktree).stderr
        )
        self.assertFalse((self.root / ".git/hooks/pre-commit").exists())


if __name__ == "__main__":
    unittest.main()
